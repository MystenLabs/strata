//! Segment-state row construction and publication primitives shared by the open,
//! recovery and write paths.

use core_types::{
    PlacementClass, SegmentFileState, SegmentId, SegmentOwner, SegmentState, StrataLsn,
};
use index::StrataIndex;
use segment::SegmentWriter;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    Error, Result, StrataStoreConfig,
    layout::{relative_segment_path, segment_path},
};

/// In-memory allocation publication state shared by the active writer and captured durability
/// checkpoints.
///
/// Each checkpoint carries cumulative byte and record counts. This tracker turns them into a
/// delta so the GC allocation baseline is applied exactly once.
#[derive(Debug, Default)]
pub(crate) struct SegmentAllocationTracker {
    published_bytes: AtomicU64,
    published_records: AtomicU64,
}

impl SegmentAllocationTracker {
    pub(crate) fn with_published_bytes(published_bytes: u64) -> Self {
        Self {
            published_bytes: AtomicU64::new(published_bytes),
            published_records: AtomicU64::new(0),
        }
    }

    pub(crate) fn unpublished_allocation(
        &self,
        captured_bytes: u64,
        captured_records: u64,
    ) -> Result<(u64, u64)> {
        let published_bytes = self.published_bytes.load(Ordering::Acquire);
        let published_records = self.published_records.load(Ordering::Acquire);
        // A later full-segment seal can win the publication race against an older durability
        // snapshot. In that case the captured prefix is already covered and contributes no delta.
        let bytes = captured_bytes.saturating_sub(published_bytes);
        let records = captured_records.saturating_sub(published_records);
        if (bytes == 0) != (records == 0) {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "segment allocation advances by {bytes} bytes and {records} records"
                ),
            });
        }
        Ok((bytes, records))
    }

    pub(crate) fn mark_published(&self, captured_bytes: u64, captured_records: u64) {
        self.published_bytes
            .fetch_max(captured_bytes, Ordering::Release);
        self.published_records
            .fetch_max(captured_records, Ordering::Release);
    }
}

/// Makes the active segment visible in the index at open time, before any write happens. This is
/// what keeps a brand new (or just recovered) segment from looking like an orphan to the next
/// crash recovery. Invoked after restart on the active segment.
pub(crate) fn publish_active_segment_state(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
    next_lsn: StrataLsn,
) -> Result<SegmentState> {
    let existing = index.get_segment_state(active_writer.segment_id())?;
    let state = active_segment_state_with_lsn(
        config,
        owner,
        active_writer,
        durable_offset,
        existing.as_ref(),
        None,
    );
    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &state)?;
    if existing.is_none() {
        index.put_segment_published_at_lsn_batch(
            &mut batch,
            active_writer.segment_id(),
            next_lsn,
        )?;
    }
    batch.write()?;
    Ok(state)
}

/// Builds the normal open segment state row for the current writer.
///
/// All open ingest rows should use the same relative path and explicit store owner.
/// Hand building this in multiple places risks one path being absolute, so a later move of the
/// store root would make that segment unreadable while others still resolve correctly.
#[cfg(test)]
pub(crate) fn active_segment_state(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> SegmentState {
    active_segment_state_with_lsn(config, owner, active_writer, durable_offset, None, None)
}

/// Builds the segment state row for the active writer. Fields the writer doesn't own
/// (volume, placement class, LSN bounds) are carried over from the existing row so a routine
/// state update can't clobber what background reorganization or recovery set. min/max LSN are
/// maintained per segment so the durable-frontier walk and GC can reason about which LSNs a
/// segment covers without scanning it.
fn active_segment_state_with_lsn(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
    existing: Option<&SegmentState>,
    appended_lsn: Option<StrataLsn>,
) -> SegmentState {
    let mut state = active_segment_state_from_path(
        config,
        owner,
        active_writer.segment_id(),
        active_writer.write_offset(),
        durable_offset,
    );
    if let Some(existing) = existing {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.min_lsn = existing.min_lsn;
        state.max_lsn = existing.max_lsn;
        state.sealed_before_lsn = existing.sealed_before_lsn;
    }
    if let Some(lsn) = appended_lsn {
        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }
    state
}

/// Creates a fresh segment state row from an on disk path.
///
/// New rows start with no sealed checksum or LSN bounds. Accidentally
/// carrying those fields from a previous segment id would make recovery think an open segment is
/// sealed or make GC believe it contains LSNs it never wrote.
pub(crate) fn active_segment_state_from_path(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    segment_id: SegmentId,
    write_offset: u64,
    durable_offset: u64,
) -> SegmentState {
    let path = segment_path(config, segment_id);
    SegmentState {
        owner,
        segment_id,
        volume_id: 0,
        path: relative_segment_path(config, path),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset,
        durable_offset,
        min_lsn: None,
        max_lsn: None,
        sealed_before_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

pub(crate) fn publish_segment_allocation_delta(
    index: &StrataIndex,
    batch: &mut index::port::map::IndexBatch,
    segment_id: SegmentId,
    allocation_bytes: u64,
    allocation_records: u64,
) -> Result<bool> {
    if allocation_bytes == 0 && allocation_records == 0 {
        return Ok(false);
    }
    if (allocation_bytes == 0) != (allocation_records == 0) {
        return Err(Error::InvariantViolation {
            reason: format!(
                "segment {segment_id} GC baseline advances by {allocation_bytes} bytes and {allocation_records} records"
            ),
        });
    }

    index.merge_segment_gc_summary_batch(
        batch,
        segment_id,
        &core_types::SegmentGcSummaryDelta {
            total_bytes: i128::from(allocation_bytes),
            live_bytes: i128::from(allocation_bytes),
            live_ref_count: i128::from(allocation_records),
            unknown_lifetime_bytes: i128::from(allocation_bytes),
            unknown_lifetime_ref_count: i128::from(allocation_records),
            ..Default::default()
        },
    )?;
    Ok(true)
}

/// Returns unsealed ingest segments in write order.
///
/// Recovery must scan low segment ids first. If segment 3 is recovered before
/// segment 2 and segment 2 then turns out to have lost LSN 40, keeping segment 3's later LSNs would
/// create a non-contiguous history.
pub(crate) fn unsealed_ingest_segment_ids(index: &StrataIndex) -> Result<Vec<SegmentId>> {
    let mut segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && is_unsealed_state(state.state)
        })
        .map(|(segment_id, _)| segment_id)
        .collect::<Vec<_>>();
    segment_ids.sort_unstable();
    Ok(segment_ids)
}

pub(crate) fn unsealed_ingest_segment_count(index: &StrataIndex) -> Result<usize> {
    Ok(unsealed_ingest_segment_ids(index)?.len())
}

fn is_unsealed_state(state: SegmentFileState) -> bool {
    matches!(state, SegmentFileState::Open | SegmentFileState::Sealing)
}
