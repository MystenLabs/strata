use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use strata_core::{
    SegmentGcLifetimeRange, SegmentGcLifetimeUpdate, SegmentGcOverlay, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentId,
};
use typed_store::{
    Map,
    rocks::{DBBatch, default_db_options},
};

use crate::{Error, Result};

use super::super::StrataIndex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EncodedSegmentGcOverlayMergeOperand {
    Op(SegmentGcOverlayMergeOp),
    Ops(Vec<SegmentGcOverlayMergeOp>),
}

impl EncodedSegmentGcOverlayMergeOperand {
    fn into_ops(self) -> Vec<SegmentGcOverlayMergeOp> {
        match self {
            Self::Op(op) => vec![op],
            Self::Ops(ops) => ops,
        }
    }
}

impl StrataIndex {
    pub fn get_segment_gc_overlay(
        &self,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentGcOverlay>> {
        Ok(self.segment_gc_overlay.get(&segment_id)?)
    }

    pub fn merge_segment_gc_overlay_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        ops: Vec<SegmentGcOverlayMergeOp>,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Use a RocksDB merge operand rather than read-modify-write. GC overlay updates are
        // published beside manifest/accounting rows, and several independent compactions can append
        // range facts for the same segment. The merge operator is the physical serialization point
        // that turns those append-only facts into one canonical per-segment overlay.
        let operand =
            encode_segment_gc_overlay_merge_operand(EncodedSegmentGcOverlayMergeOperand::Ops(ops))?;
        batch.partial_merge_batch(self.segment_gc_overlay(), [(&segment_id, operand)])?;
        Ok(())
    }
}

pub(crate) fn segment_gc_overlay_cf_options() -> rocksdb::Options {
    let mut options = default_db_options().options;
    options.set_merge_operator(
        "strata-segment-gc-overlay-merge",
        move |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
            full_merge_segment_gc_overlay(existing_value, operands)
        },
        move |_key: &[u8], _existing_value: Option<&[u8]>, operands: &MergeOperands| {
            partial_merge_segment_gc_overlay(operands)
        },
    );
    options
}

fn full_merge_segment_gc_overlay(
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // RocksDB may compact many delayed operands into an existing materialized overlay. The operands
    // are physical ref transitions in commit order; the stored value is just the latest normalized
    // summary. Keeping the fold deterministic matters because dead ranges and lifetime hints are not
    // independent sets: retiring a range removes any hint for that range, while a later lifetime
    // update can carve a subrange back out of stale dead knowledge.
    let mut state = match existing_value {
        Some(value) => bcs::from_bytes::<SegmentGcOverlay>(value).ok()?,
        None => SegmentGcOverlay::default(),
    };

    for operand in operands {
        let operand = decode_segment_gc_overlay_merge_operand(operand).ok()?;
        for op in operand.into_ops() {
            apply_segment_gc_overlay_merge_op(&mut state, op);
        }
    }

    normalize_segment_gc_overlay(&mut state);
    bcs::to_bytes(&state).ok()
}

fn apply_segment_gc_overlay_merge_op(state: &mut SegmentGcOverlay, op: SegmentGcOverlayMergeOp) {
    match op {
        SegmentGcOverlayMergeOp::RetireBatch { ranges } => {
            // RetireBatch is definitive negative information for GC copy planning. A retired range
            // is known not to protect live user data, so any lifetime hint overlapping it must be
            // removed; GC should skip the bytes instead of routing them by expiry.
            for range in ranges {
                if is_empty_range(range) {
                    continue;
                }
                state.dead.push(range);
                remove_lifetimes_overlapping(&mut state.lifetimes, range);
            }
            coalesce_ranges(&mut state.dead);
        }
        SegmentGcOverlayMergeOp::LifetimeBatch { updates } => {
            // LifetimeBatch is weaker than a retire: it annotates copy eligible bytes with routing
            // metadata. The update may also clear an existing hint (`lifecycle = None`) while still
            // saying the range should no longer be considered dead by this stale-tolerant overlay.
            for update in updates {
                apply_lifetime_update(state, update);
            }
        }
    }
}

fn apply_lifetime_update(state: &mut SegmentGcOverlay, update: SegmentGcLifetimeUpdate) {
    if is_empty_range(update.range) {
        return;
    }

    // A later lifetime update can revive a range that the overlay had marked dead due to expiry.
    // This is a storage level correction, not a claim that the whole segment is live: the updated
    // range becomes copy-eligible again, and any previous hint over the same bytes is replaced.
    subtract_dead_range(&mut state.dead, update.range);
    remove_lifetimes_overlapping(&mut state.lifetimes, update.range);

    if let Some(lifecycle) = update.lifecycle {
        state.lifetimes.push(SegmentGcLifetimeRange {
            range: update.range,
            lifecycle,
        });
        state
            .lifetimes
            .sort_by_key(|entry| (entry.range.offset, entry.range.len));
    }
}

fn normalize_segment_gc_overlay(state: &mut SegmentGcOverlay) {
    // Canonical form keeps `dead` and `lifetimes` disjoint. Readers can then treat `dead` as the
    // first filter and use `lifetimes` only for ranges that remain eligible to copy. That avoids
    // asking GC to reconcile contradictory instructions during segment scanning.
    coalesce_ranges(&mut state.dead);
    state.lifetimes.retain(|entry| {
        !is_empty_range(entry.range) && !range_overlaps_any(entry.range, &state.dead)
    });
    state
        .lifetimes
        .sort_by_key(|entry| (entry.range.offset, entry.range.len));
}

fn coalesce_ranges(ranges: &mut Vec<SegmentGcRecordRange>) {
    // Overlay ranges are segment-local byte intervals. Coalescing overlapping and adjacent ranges
    // bounds the stored overlay size without changing copy semantics: one larger dead interval and
    // two touching dead intervals tell GC to skip the same physical bytes.
    ranges.retain(|range| !is_empty_range(*range));
    ranges.sort_by_key(|range| (range.offset, range.len));

    let mut coalesced: Vec<SegmentGcRecordRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        let Some(last) = coalesced.last_mut() else {
            coalesced.push(range);
            continue;
        };

        let last_end = range_end(*last);
        let range_end = range_end(range);
        if range.offset <= last_end {
            let new_end = last_end.max(range_end);
            last.len = new_end.saturating_sub(last.offset);
        } else {
            coalesced.push(range);
        }
    }

    *ranges = coalesced;
}

fn subtract_dead_range(dead: &mut Vec<SegmentGcRecordRange>, live: SegmentGcRecordRange) {
    if is_empty_range(live) {
        return;
    }

    // Removing a live/copy-eligible subrange can split one dead interval into two. This is the
    // operation that lets a lifecycle extension undo only the bytes it touched instead of clearing
    // all stale-dead knowledge for the segment.
    let live_end = range_end(live);
    let mut remaining = Vec::with_capacity(dead.len().saturating_add(1));
    for range in dead.drain(..) {
        let range_end = range_end(range);
        if range_end <= live.offset || range.offset >= live_end {
            remaining.push(range);
            continue;
        }

        if range.offset < live.offset {
            remaining.push(SegmentGcRecordRange {
                offset: range.offset,
                len: live.offset.saturating_sub(range.offset),
            });
        }
        if range_end > live_end {
            remaining.push(SegmentGcRecordRange {
                offset: live_end,
                len: range_end.saturating_sub(live_end),
            });
        }
    }

    *dead = remaining;
}

fn remove_lifetimes_overlapping(
    lifetimes: &mut Vec<SegmentGcLifetimeRange>,
    range: SegmentGcRecordRange,
) {
    // This is exact for today's producers because overlay facts are emitted for whole record refs:
    // if a retire range overlaps a lifetime range, it is the same physical record range and the
    // retire fact wins. If future callers emit partial record ranges or coalesced lifetime ranges,
    // this must split the lifetime interval around `range` instead of dropping the whole hint.
    lifetimes.retain(|entry| !ranges_overlap(entry.range, range));
}

fn range_overlaps_any(range: SegmentGcRecordRange, ranges: &[SegmentGcRecordRange]) -> bool {
    ranges
        .iter()
        .any(|candidate| ranges_overlap(range, *candidate))
}

fn ranges_overlap(left: SegmentGcRecordRange, right: SegmentGcRecordRange) -> bool {
    !is_empty_range(left)
        && !is_empty_range(right)
        && left.offset < range_end(right)
        && right.offset < range_end(left)
}

fn is_empty_range(range: SegmentGcRecordRange) -> bool {
    range.len == 0
}

fn range_end(range: SegmentGcRecordRange) -> u64 {
    range.offset.saturating_add(range.len)
}

pub(crate) fn partial_merge_segment_gc_overlay(operands: &MergeOperands) -> Option<Vec<u8>> {
    // Partial merge cannot inspect the existing overlay value, so it must not try to resolve
    // retire-vs-lifetime conflicts. It only batches operands into one encoded list; full merge is
    // the point where current state and all pending operands are folded together.
    let mut ops = Vec::new();
    for operand in operands {
        let operand = decode_segment_gc_overlay_merge_operand(operand).ok()?;
        ops.extend(operand.into_ops());
    }
    bcs::to_bytes(&EncodedSegmentGcOverlayMergeOperand::Ops(ops)).ok()
}

pub(crate) fn encode_segment_gc_overlay_merge_operand(
    operand: EncodedSegmentGcOverlayMergeOperand,
) -> Result<Vec<u8>> {
    bcs::to_bytes(&operand).map_err(|err| Error::Serialization(err.to_string()))
}

fn decode_segment_gc_overlay_merge_operand(
    data: &[u8],
) -> Result<EncodedSegmentGcOverlayMergeOperand> {
    bcs::from_bytes(data).map_err(|err| Error::Serialization(err.to_string()))
}
