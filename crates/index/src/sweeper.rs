use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
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
const MAX_SWEEP_RECORDS: usize = 32_768;
const SWEEP_WORKERS: usize = 4;
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
        self.sweep_garbage_log_with_workers(
            global_log_dir.as_ref(),
            namespace_dir.as_ref(),
            head_name,
            cursor_name,
            SWEEP_WORKERS,
        )
    }

    fn sweep_garbage_log_with_workers(
        &self,
        global_log_dir: &Path,
        namespace_dir: &Path,
        head_name: &str,
        cursor_name: &str,
        worker_count: usize,
    ) -> Result<bool> {
        if head_name == cursor_name {
            return Err(Error::InvalidGarbageSweep(
                "head and sweep cursor names must differ".to_owned(),
            ));
        }
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
        let mut forwarding = BTreeMap::new();
        let mut next_cursor = cursor;
        for (records, position) in GarbageLog::read_batch(
            global_log_dir,
            cursor,
            head,
            MAX_SWEEP_FRAMES,
            MAX_SWEEP_RECORDS,
            worker_count,
        )? {
            for record in records {
                self.route_garbage_record(record, position, &mut forwarding, &mut by_segment)?;
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

        // Map in log order, then give each segment to exactly one reducer. Workers never
        // publish metadata: an error in any reducer leaves the entire cursor uncommitted.
        let mut jobs = Vec::with_capacity(by_segment.len());
        for (segment_id, records) in by_segment {
            let state = states.remove(&segment_id).expect("state resolved above");
            let row = summaries
                .remove(&segment_id)
                .expect("summary resolved above");
            let committed = batch
                .get(self.segment_garbage_log_positions(), &segment_id)?
                .unwrap_or_default();
            jobs.push(SweepJob {
                segment_id,
                records,
                path: segment_garbage_log_path(namespace_dir, &state.path),
                committed,
                row,
            });
        }
        let workers = worker_count.min(jobs.len()).max(1);
        let pending = Mutex::new(jobs.into_iter());
        let mut reduced = std::thread::scope(|scope| {
            let handles = (0..workers)
                .map(|_| {
                    let pending = &pending;
                    scope.spawn(move || -> Result<Vec<SweepOutput>> {
                        let mut outputs = Vec::new();
                        let mut retained_bytes = 0usize;
                        loop {
                            let Some(job) = pending.lock().expect("sweep queue poisoned").next()
                            else {
                                break;
                            };
                            let mut output = self.reduce_segment_garbage(job)?;
                            let bytes = crate::overlay_cache::overlay_bytes(&output.overlay);
                            // Bound retained overlays across all reducers, independently of
                            // scheduling. An oversized overlay is dropped after its append.
                            if retained_bytes.saturating_add(bytes) <= OVERLAY_CACHE_BYTES / workers
                            {
                                retained_bytes += bytes;
                            } else {
                                output.retain_overlay = false;
                                output.overlay = SegmentGcOverlay::default();
                            }
                            outputs.push(output);
                        }
                        Ok(outputs)
                    })
                })
                .collect::<Vec<_>>();
            let mut all = Vec::new();
            let mut error = None;
            for handle in handles {
                match handle.join().expect("garbage sweep worker panicked") {
                    Ok(outputs) => all.extend(outputs),
                    Err(err) => {
                        error.get_or_insert(err);
                    }
                }
            }
            match error {
                Some(err) => Err(err),
                None => Ok(all),
            }
        })?;
        reduced.sort_unstable_by_key(|output| output.segment_id);
        let mut appended = Vec::with_capacity(reduced.len());
        let mut updated_overlays = Vec::with_capacity(reduced.len());
        for output in reduced {
            batch.put(
                self.segment_garbage_log_positions(),
                &output.segment_id,
                &output.position,
            )?;
            self.merge_segment_gc_summary_batch(
                batch.raw_batch_mut(),
                output.segment_id,
                &output.delta,
            )?;
            appended.push(output.file);
            updated_overlays.push(output.retain_overlay.then_some((
                output.segment_id,
                output.position,
                output.expected_row,
                output.overlay,
            )));
        }
        // One filesystem sync covers every appended log; it must land before the positions
        // that make those appends visible are committed below.
        sync_segment_garbage_logs(namespace_dir, &appended)?;
        for (source, route) in forwarding {
            if route.is_some_and(|route| {
                crate::garbage_forwarding::at_or_before(route.through, next_cursor)
            }) {
                batch
                    .raw_batch_mut()
                    .delete_batch(&self.garbage_forwarding, [source])?;
            }
        }
        batch.put(self.garbage_log_positions(), &cursor_name, &next_cursor)?;
        batch.write_with_sync(true)?;
        // Keep validated writers only after publication succeeds. Any earlier error drops
        // the touched writers and overlays, so a retry recovers from the committed positions.
        {
            let mut cache = self
                .overlay_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for (file, entry) in appended.into_iter().zip(updated_overlays) {
                if let Some((segment_id, position, expected_row, overlay)) = entry {
                    cache.insert(segment_id, position, expected_row, overlay, file);
                }
            }
        }
        GarbageLog::reclaim_before(global_log_dir, next_cursor)?;
        Ok(true)
    }
    fn reduce_segment_garbage(&self, job: SweepJob) -> Result<SweepOutput> {
        let SweepJob {
            segment_id,
            records,
            path,
            committed,
            row,
        } = job;
        let cached = self
            .overlay_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take(segment_id, committed, &path)?;
        let (mut overlay, mut file) = match cached {
            Some((expected_row, mut overlay, file)) => {
                // Other publishers may have moved the row since this overlay was folded;
                // carry that movement into the cached summary so it matches a fresh fold.
                let drift = summary_delta_between(&expected_row, &row);
                apply_segment_gc_summary_delta(&mut overlay.summary, &drift).ok_or_else(|| {
                    Error::InvalidGarbageSweep(format!(
                        "segment {segment_id} summary drift does not apply"
                    ))
                })?;
                (overlay, file)
            }
            None => {
                let existing = if committed == 0 {
                    Vec::new()
                } else {
                    read_segment_garbage(&path, committed)?
                };
                (
                    fold_segment_garbage(existing, row.clone())?,
                    SegmentGarbageLog::open(&path, committed)?,
                )
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
        let position = file.append_unsynced(&records)?;
        let mut expected_row = row;
        let retain_overlay =
            apply_segment_gc_summary_delta(&mut expected_row, &summary_delta).is_some();
        Ok(SweepOutput {
            segment_id,
            position,
            file,
            expected_row,
            overlay,
            delta: summary_delta,
            retain_overlay,
        })
    }
}

struct SweepJob {
    segment_id: SegmentId,
    records: Vec<GarbageRecord>,
    path: PathBuf,
    committed: u64,
    row: SegmentGcSummary,
}

struct SweepOutput {
    segment_id: SegmentId,
    position: u64,
    file: SegmentGarbageLog,
    expected_row: SegmentGcSummary,
    overlay: SegmentGcOverlay,
    delta: SegmentGcSummaryDelta,
    retain_overlay: bool,
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

#[cfg(test)]
mod parallel_benchmark {
    use super::*;
    use core_types::{
        BlobKey, GarbageEvent, PlacementClass, RecordRef, SegmentOwner, SegmentState,
    };
    use std::time::Instant;

    /// Run manually with --ignored --nocapture. Both runs use the same pipeline; only reducer
    /// parallelism differs. Cold logs exercise validation/folding, warm logs exercise cached work.
    #[test]
    #[ignore = "manual sweep throughput benchmark"]
    fn sweep_reducer_throughput() {
        for workers in [1, 4] {
            let dir = tempfile::tempdir().unwrap();
            let index =
                StrataIndex::open_path(dir.path().join("index"), "strata", "bench").unwrap();
            let namespace = dir.path().join("namespace");
            let global = dir.path().join("global");
            let mut incoming = Vec::new();
            for id in 1..=32 {
                let state = SegmentState {
                    segment_id: id,
                    owner: SegmentOwner::Store,
                    path: format!("{id}.seg"),
                    state: SegmentFileState::Sealed,
                    write_offset: 100,
                    durable_offset: 100,
                    min_lsn: Some(1),
                    max_lsn: Some(1),
                    placement_class: PlacementClass::Ingest,
                    volume_id: 0,
                    sealed_before_lsn: None,
                    sealed_len: Some(100),
                    sealed_sha256: None,
                };
                index.put_segment_state(&state).unwrap();
                let mut batch = index.batch();
                index
                    .put_segment_gc_summary_batch(
                        &mut batch,
                        id,
                        &SegmentGcSummary {
                            total_bytes: 100,
                            live_bytes: 100,
                            live_ref_count: 1,
                            unknown_lifetime_bytes: 100,
                            unknown_lifetime_ref_count: 1,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                let record = |lsn| GarbageRecord {
                    key: core_types::SegmentKey {
                        segment_id: id,
                        blob_key: BlobKey::new(b"k".as_slice()).unwrap(),
                    },
                    lsn,
                    event: GarbageEvent::SetLifecycle {
                        record: RecordRef {
                            segment_id: id,
                            offset: 0,
                            len: 100,
                        },
                        lifecycle: None,
                    },
                    summary_delta: Default::default(),
                };
                let history = (1..=20_000).map(record).collect::<Vec<_>>();
                let position =
                    SegmentGarbageLog::open(segment_garbage_log_path(&namespace, &state.path), 0)
                        .unwrap()
                        .append(&history)
                        .unwrap();
                index
                    .put_segment_garbage_log_position_batch(&mut batch, id, position)
                    .unwrap();
                batch.write_with_sync(true).unwrap();
                incoming.push(record(20_001));
            }
            let mut log = GarbageLog::open(&global, 1 << 30, Default::default()).unwrap();
            for phase in ["cold", "warm"] {
                let mut head = Default::default();
                for frame in 0..32 {
                    let records = incoming
                        .iter()
                        .flat_map(|record| {
                            (0..32).map(move |offset| {
                                let mut record = record.clone();
                                record.lsn += frame * 32 + offset;
                                record
                            })
                        })
                        .collect::<Vec<_>>();
                    head = log.append(&records).unwrap();
                }
                let mut batch = index.batch();
                index
                    .put_garbage_log_position_batch(&mut batch, "head", head)
                    .unwrap();
                batch.write_with_sync(true).unwrap();
                let now = Instant::now();
                assert!(
                    index
                        .sweep_garbage_log_with_workers(
                            &global, &namespace, "head", "cursor", workers
                        )
                        .unwrap()
                );
                eprintln!(
                    "sweep workers={workers} phase={phase} elapsed_ms={:.2}",
                    now.elapsed().as_secs_f64() * 1000.0
                );
                for record in &mut incoming {
                    record.lsn += 1024;
                }
            }
        }
    }
}
