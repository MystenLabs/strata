use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use crate::port::map::IndexBatch;
use core_types::{
    GarbageEvent, SegmentFileState, SegmentGcLifetimeUpdate, SegmentGcOverlay,
    SegmentGcOverlayMergeOp, SegmentGcRecordRange, SegmentGcSummary, SegmentGcSummaryDelta,
    SegmentId,
};
use lsm::{
    GarbageLog, GarbageRecord, SegmentGarbageLog, fold_segment_garbage, read_segment_garbage,
    sync_segment_garbage_logs,
};

use crate::{Error, Result, StrataIndex, segment::gc_summary::apply_segment_gc_summary_delta};

const MAX_SWEEP_FRAMES: usize = 256;
/// Heap budget for folded overlays kept between sweeps (see `overlay_cache`). A mature
/// one-gigabyte segment of 16 KiB records folds to about two megabytes, so this holds the
/// overlays of several hundred such segments.
pub(crate) const OVERLAY_CACHE_BYTES: usize = 1 << 30;

impl StrataIndex {
    pub fn get_segment_garbage_log_position(&self, segment_id: SegmentId) -> Result<Option<u64>> {
        self.segment_garbage_log_positions.get(&segment_id)
    }

    pub fn put_segment_garbage_log_position_batch(
        &self,
        batch: &mut IndexBatch,
        segment_id: SegmentId,
        position: u64,
    ) -> Result<()> {
        batch.insert_batch(
            self.segment_garbage_log_positions(),
            [(segment_id, position)],
        )?;
        Ok(())
    }

    /// Reads a segment's summary and committed local garbage prefix from the same RocksDB view.
    pub fn read_segment_garbage_overlay(
        &self,
        namespace_dir: impl AsRef<Path>,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentGcOverlay>> {
        let snapshot = self.db.snapshot()?;
        let Some(state) = self
            .segment_states
            .get_with_snapshot(snapshot.as_ref(), &segment_id)?
        else {
            return Ok(None);
        };
        let summary = self
            .segment_gc_summaries
            .get_with_snapshot(snapshot.as_ref(), &segment_id)?;
        let committed = self
            .segment_garbage_log_positions
            .get_with_snapshot(snapshot.as_ref(), &segment_id)?;
        let summary = summary.unwrap_or_default();
        let committed = committed.unwrap_or_default();
        let ops = if committed == 0 {
            Vec::new()
        } else {
            let path = segment_garbage_log_path(namespace_dir.as_ref(), &state.path);
            read_segment_garbage(path, committed)?
        };
        Ok(Some(fold_segment_garbage(ops, summary)?))
    }

    /// Reads the committed detail records for one segment.
    pub fn read_segment_garbage_records(
        &self,
        namespace_dir: impl AsRef<Path>,
        segment_id: SegmentId,
    ) -> Result<Vec<GarbageRecord>> {
        let snapshot = self.db.snapshot()?;
        let Some(state) = self
            .segment_states
            .get_with_snapshot(snapshot.as_ref(), &segment_id)?
        else {
            return Ok(Vec::new());
        };
        let committed = self
            .segment_garbage_log_positions
            .get_with_snapshot(snapshot.as_ref(), &segment_id)?
            .unwrap_or_default();
        if committed == 0 {
            return Ok(Vec::new());
        }
        let path = segment_garbage_log_path(namespace_dir.as_ref(), &state.path);
        Ok(read_segment_garbage(path, committed)?)
    }

    /// Sweeps a bounded batch of committed global frames into segment-local files.
    ///
    /// Each touched local file is synced once before its position, additive summary delta, and the
    /// sweep cursor become visible in one RocksDB batch. Calls for the same log must be serialized
    /// by the owner.
    pub fn sweep_garbage_log(
        &self,
        global_log_dir: impl AsRef<Path>,
        namespace_dir: impl AsRef<Path>,
        head_name: &str,
        cursor_name: &str,
    ) -> Result<bool> {
        if head_name == cursor_name {
            return Err(Error::InvalidGarbageSweep(
                "head and sweep cursor names must differ".to_owned(),
            ));
        }
        let global_log_dir = global_log_dir.as_ref();
        let head_name = head_name.to_owned();
        let cursor_name = cursor_name.to_owned();
        let mut batch = self.indexed_batch()?;
        let Some(head) = batch.get(self.garbage_log_positions(), &head_name)? else {
            return Ok(false);
        };
        let cursor = batch
            .get(self.garbage_log_positions(), &cursor_name)?
            .unwrap_or_default();
        GarbageLog::reclaim_before(global_log_dir, cursor)?;

        let mut by_segment = BTreeMap::<SegmentId, Vec<GarbageRecord>>::new();
        let mut next_cursor = cursor;
        for _ in 0..MAX_SWEEP_FRAMES {
            let Some((records, position)) =
                GarbageLog::read_next(global_log_dir, next_cursor, head)?
            else {
                break;
            };
            for record in records {
                let segment_id = record.key.segment_id;
                by_segment.entry(segment_id).or_default().push(record);
            }
            next_cursor = position;
        }
        if next_cursor == cursor {
            return Ok(false);
        }

        let mut states = BTreeMap::new();
        let mut summaries = BTreeMap::new();
        let mut deleted_segments = Vec::new();
        for segment_id in by_segment.keys() {
            let state = batch
                .get(self.segment_states(), segment_id)?
                .ok_or_else(|| {
                    Error::InvalidGarbageSweep(format!("segment {segment_id} has no state"))
                })?;
            if state.state == SegmentFileState::Deleted {
                deleted_segments.push(*segment_id);
                continue;
            }
            // Leave the frame pending until Store durability publication installs the segment's
            // allocation baseline. A summary row alone is insufficient: active segments publish
            // several durable prefixes, and a garbage event can name a record beyond the prefix
            // currently accounted by the row. Applying its negative live-byte delta first would
            // make the summary depend on whether the sweeper or the next durability publication
            // won the race.
            let Some(summary) = batch.get(self.segment_gc_summaries(), segment_id)? else {
                return Ok(false);
            };
            let required_bytes = by_segment[segment_id]
                .iter()
                .map(|record| {
                    let range = record.event.record();
                    range.offset.checked_add(range.len).ok_or_else(|| {
                        Error::InvalidGarbageSweep(format!(
                            "segment {segment_id} garbage range overflows"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max()
                .unwrap_or_default();
            if summary.total_bytes < required_bytes {
                return Ok(false);
            }
            states.insert(*segment_id, state);
            summaries.insert(*segment_id, summary);
        }
        for segment_id in deleted_segments {
            by_segment.remove(&segment_id);
        }

        let mut appended = Vec::new();
        for (segment_id, records) in by_segment {
            let state = states
                .remove(&segment_id)
                .expect("states were resolved above");
            let committed = batch
                .get(self.segment_garbage_log_positions(), &segment_id)?
                .unwrap_or_default();
            let path = segment_garbage_log_path(namespace_dir.as_ref(), &state.path);
            let row = summaries
                .remove(&segment_id)
                .expect("summaries were resolved above");
            let cached = self
                .overlay_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take(segment_id, committed);
            let mut overlay = match cached {
                Some((expected_row, mut overlay)) => {
                    // Other publishers may have moved the row since this overlay was folded;
                    // carry that movement into the cached summary so it matches a fresh fold.
                    let drift = summary_delta_between(&expected_row, &row);
                    apply_segment_gc_summary_delta(&mut overlay.summary, &drift).ok_or_else(
                        || {
                            Error::InvalidGarbageSweep(format!(
                                "segment {segment_id} summary drift does not apply"
                            ))
                        },
                    )?;
                    overlay
                }
                None => {
                    let existing = if committed == 0 {
                        Vec::new()
                    } else {
                        read_segment_garbage(&path, committed)?
                    };
                    fold_segment_garbage(existing, row.clone())?
                }
            };
            let before_summary = overlay.summary.clone();
            overlay.apply_merge_ops(records.iter().map(garbage_merge_op));
            // Garbage events describe idempotent state transitions, while their serialized
            // summary deltas describe the transition from the compactor's input view. A later
            // relocation-aware compaction may legitimately restate an event already present in
            // the segment log. Reapplying that raw delta would subtract the old state twice.
            // Derive the merge operand from the overlay transition instead, preserving both
            // idempotency and commutativity with allocation-baseline merge operands.
            let summary_delta = summary_delta_between(&before_summary, &overlay.summary);
            let mut file = SegmentGarbageLog::open(path, committed)?;
            let position = file.append_unsynced(&records)?;
            appended.push(file);
            batch.put(self.segment_garbage_log_positions(), &segment_id, &position)?;
            self.merge_segment_gc_summary_batch(batch.raw_batch_mut(), segment_id, &summary_delta)?;
            let mut expected_row = row;
            if apply_segment_gc_summary_delta(&mut expected_row, &summary_delta).is_some() {
                self.overlay_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(segment_id, position, expected_row, overlay);
            }
        }
        // One filesystem sync covers every appended log; it must land before the positions
        // that make those appends visible are committed below.
        sync_segment_garbage_logs(namespace_dir.as_ref(), &appended)?;
        batch.put(self.garbage_log_positions(), &cursor_name, &next_cursor)?;
        batch.write_with_sync(true)?;
        GarbageLog::reclaim_before(global_log_dir, next_cursor)?;
        Ok(true)
    }
}

fn summary_delta_between(
    before: &SegmentGcSummary,
    after: &SegmentGcSummary,
) -> SegmentGcSummaryDelta {
    let mut delta = SegmentGcSummaryDelta {
        total_bytes: difference(after.total_bytes, before.total_bytes),
        live_bytes: difference(after.live_bytes, before.live_bytes),
        retired_bytes: difference(after.retired_bytes, before.retired_bytes),
        expired_bytes: difference(after.expired_bytes, before.expired_bytes),
        live_ref_count: difference(after.live_ref_count, before.live_ref_count),
        unknown_lifetime_bytes: difference(
            after.unknown_lifetime_bytes,
            before.unknown_lifetime_bytes,
        ),
        unknown_lifetime_ref_count: difference(
            after.unknown_lifetime_ref_count,
            before.unknown_lifetime_ref_count,
        ),
        ..Default::default()
    };

    for (&epoch, bucket) in &before.future_epoch_histogram {
        delta.epoch_bytes.insert(epoch, -i128::from(bucket.bytes));
        delta.epoch_refs.insert(epoch, -i128::from(bucket.refs));
    }
    for (&epoch, bucket) in &after.future_epoch_histogram {
        *delta.epoch_bytes.entry(epoch).or_default() += i128::from(bucket.bytes);
        *delta.epoch_refs.entry(epoch).or_default() += i128::from(bucket.refs);
    }
    delta.epoch_bytes.retain(|_, change| *change != 0);
    delta.epoch_refs.retain(|_, change| *change != 0);

    for (&extension_count, &count) in &before.extension_count_histogram {
        delta
            .extension_counts
            .insert(extension_count, -i128::from(count));
    }
    for (&extension_count, &count) in &after.extension_count_histogram {
        *delta.extension_counts.entry(extension_count).or_default() += i128::from(count);
    }
    delta.extension_counts.retain(|_, change| *change != 0);
    delta
}

fn difference(after: u64, before: u64) -> i128 {
    i128::from(after) - i128::from(before)
}

fn garbage_merge_op(record: &GarbageRecord) -> SegmentGcOverlayMergeOp {
    let range = SegmentGcRecordRange::from(record.event.record());
    match record.event {
        GarbageEvent::Retired { .. } => SegmentGcOverlayMergeOp::RetireBatch {
            ranges: vec![range],
        },
        GarbageEvent::Expired { .. } => SegmentGcOverlayMergeOp::ExpireBatch {
            ranges: vec![range],
        },
        GarbageEvent::SetLifecycle { lifecycle, .. } => SegmentGcOverlayMergeOp::LifetimeBatch {
            updates: vec![SegmentGcLifetimeUpdate { range, lifecycle }],
        },
    }
}

fn segment_garbage_log_path(namespace_dir: &Path, segment_path: &str) -> PathBuf {
    let path = PathBuf::from(segment_path);
    let mut path = if path.is_absolute() {
        path
    } else {
        namespace_dir.join(path)
    };
    path.set_extension("glog");
    path
}
