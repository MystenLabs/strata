use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strata_core::{
    BlobKey, BlobLifecycle, RecordRef, SegmentGcLifetimeUpdate, SegmentGcLiveRecord,
    SegmentGcOverlayMergeOp, SegmentGcRecordRange, SegmentId, ShardKey, StrataLsn,
};

use crate::{PartitionId, RunId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetireReason {
    Overwritten,
    Tombstoned,
    Mapped,
    ShardDropped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefEvent {
    Live {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    },
    Retired {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
        reason: RetireReason,
    },
    LifecycleChanged {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        old: Option<BlobLifecycle>,
        new: Option<BlobLifecycle>,
    },
    Mapped {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        from: RecordRef,
        to: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    },
}

/// Signed per-segment GC summary changes produced by materializing update history.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentGcSummaryDelta {
    pub total_bytes: i128,
    pub live_bytes: i128,
    pub retired_bytes: i128,
    pub live_ref_count: i128,
}

impl SegmentGcSummaryDelta {
    fn apply_live(&mut self, len: u64) {
        let len = i128::from(len);
        self.total_bytes += len;
        self.live_bytes += len;
        self.live_ref_count += 1;
    }

    fn apply_retired(&mut self, len: u64) {
        let len = i128::from(len);
        self.live_bytes -= len;
        self.retired_bytes += len;
        self.live_ref_count -= 1;
    }
}

/// Transient durable-write set produced while compacting one partition.
///
/// This batch is in-memory until the owner publishes it, but its contents are intended to become
/// durable database rows in the same RocksDB batch as the prepared manifest. It is born during
/// `prepare_delta_compaction` or `prepare_major_compact_partition` and retired after the owner has
/// persisted the rows and applied the matching prepared manifest.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CompactionEventBatch {
    /// The batch is scoped to one partition because compaction never mixes hash ranges. This lets the
    /// owner route the event rows and reason about publication independently per partition.
    pub partition: PartitionId,
    /// Input run ids record the physical lineage that was consumed to produce these rows. They make
    /// the event batch auditable and tie the logical GC/accounting deltas to the exact run files that
    /// become obsolete after publish.
    pub input_run_ids: Vec<RunId>,
    /// The replacement run id links the derived rows back to the new physical file. It is absent for
    /// no-op compactions where the prepared manifest is just the current root.
    pub output_run_id: Option<RunId>,
    /// The batch watermark tells the owner how far this compaction's derived accounting rows reach
    /// without replaying the run files again. Major compaction uses it to advance the partition's
    /// materialization watermark.
    pub max_lsn: StrataLsn,
    /// Logical ref transitions are kept as an audit trail and as the source from which the physical
    /// summary and overlay mutations were derived. They are not replayed by future index reads;
    /// future reads follow the manifest's run stack.
    pub events: Vec<RefEvent>,
    /// Summary deltas are retained for callers that inspect prepared compaction output directly. The
    /// store's durable GC summary is folded into `SegmentGcOverlay`.
    pub segment_summary: BTreeMap<SegmentId, SegmentGcSummaryDelta>,
    /// GC overlay operands are already grouped by physical segment because RocksDB merge application
    /// is the lifecycle boundary for collectability. Publishing these operands with the manifest
    /// prevents GC from seeing a segment range as retired or lifetime-adjusted without the matching
    /// accounting-index root.
    pub segment_gc_overlay_ops: BTreeMap<SegmentId, Vec<SegmentGcOverlayMergeOp>>,
}

impl CompactionEventBatch {
    pub(crate) fn record_event(&mut self, event: RefEvent) {
        // RefEvents are the logical history; signed summary deltas and GC-overlay merge operands
        // are physical consequences that can be published beside that history. The store treats the
        // overlay summary as the canonical GC accounting view, but these deltas are still retained
        // for tests and for callers that inspect prepared compaction output directly.
        match &event {
            RefEvent::Live { record_ref, .. } => {
                self.segment_summary
                    .entry(record_ref.segment_id)
                    .or_default()
                    .apply_live(record_ref.len);
            }
            RefEvent::Retired { record_ref, .. } => {
                self.segment_summary
                    .entry(record_ref.segment_id)
                    .or_default()
                    .apply_retired(record_ref.len);
            }
            RefEvent::Mapped { from, to, .. } => {
                // The logical key did not change, but the protected bytes did. Summary deltas see a
                // retire on the old segment and a live allocation on the new segment; the matching
                // GC overlay update is emitted below from the same RefEvent.
                self.segment_summary
                    .entry(from.segment_id)
                    .or_default()
                    .apply_retired(from.len);
                self.segment_summary
                    .entry(to.segment_id)
                    .or_default()
                    .apply_live(to.len);
            }
            RefEvent::LifecycleChanged { .. } => {}
        }
        self.record_gc_overlay_event(&event);
        self.events.push(event);
    }

    fn record_gc_overlay_event(&mut self, event: &RefEvent) {
        // The GC overlay is keyed by physical segment, not by blob key. These operands therefore
        // translate each logical ref transition into range-scoped segment updates.
        match event {
            RefEvent::Live {
                record_ref,
                lifecycle,
                ..
            } => {
                self.push_gc_overlay_op(
                    record_ref.segment_id,
                    SegmentGcOverlayMergeOp::AddLiveBatch {
                        records: vec![SegmentGcLiveRecord {
                            range: SegmentGcRecordRange::from(*record_ref),
                            lifecycle: *lifecycle,
                        }],
                    },
                );
            }
            RefEvent::Retired { record_ref, .. } => {
                self.push_gc_overlay_op(
                    record_ref.segment_id,
                    SegmentGcOverlayMergeOp::RetireBatch {
                        ranges: vec![SegmentGcRecordRange::from(*record_ref)],
                    },
                );
            }
            RefEvent::LifecycleChanged {
                record_ref, new, ..
            } => {
                // Even `new = None` must be written as an overlay update. Absence of an operand would
                // mean "keep whatever hint was already stored", while this transition means "the
                // range is still copy-eligible, but no longer has a lifetime hint."
                self.push_gc_overlay_op(
                    record_ref.segment_id,
                    SegmentGcOverlayMergeOp::LifetimeBatch {
                        updates: vec![SegmentGcLifetimeUpdate {
                            range: SegmentGcRecordRange::from(*record_ref),
                            lifecycle: *new,
                        }],
                    },
                );
            }
            RefEvent::Mapped {
                from,
                to,
                lifecycle,
                ..
            } => {
                // Mapping is a physical move: the old range becomes collectable and the new range
                // inherits the key's lifecycle, if any.
                self.push_gc_overlay_op(
                    from.segment_id,
                    SegmentGcOverlayMergeOp::RetireBatch {
                        ranges: vec![SegmentGcRecordRange::from(*from)],
                    },
                );
                self.push_gc_overlay_op(
                    to.segment_id,
                    SegmentGcOverlayMergeOp::AddLiveBatch {
                        records: vec![SegmentGcLiveRecord {
                            range: SegmentGcRecordRange::from(*to),
                            lifecycle: *lifecycle,
                        }],
                    },
                );
            }
        }
    }

    fn push_gc_overlay_op(&mut self, segment_id: SegmentId, op: SegmentGcOverlayMergeOp) {
        self.segment_gc_overlay_ops
            .entry(segment_id)
            .or_default()
            .push(op);
    }
}
