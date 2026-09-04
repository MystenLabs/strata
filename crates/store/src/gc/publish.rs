//! GC publication outside the foreground write coordinator.
//!
//! The executor revalidates staged copies, writes their relocations directly to a synced immutable
//! L0 table, then activates that table and the related segment metadata in one RocksDB batch. The
//! GC thread syncs that activation itself before source deletion becomes eligible; foreground
//! writer durability is not part of the relocation protocol.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::output::{
    GcPublishCommitError, GcSkippedCopiedRecord, GcSkippedCopiedRecordKind,
    PlannedGcOutputSegments, apply_gc_output_lsn_bounds, assign_gc_publish_fence,
    gc_output_bytes_by_source, initial_gc_output_metadata, skipped_gc_output_ranges,
};
use crate::{
    Error, GARBAGE_LOG_HEAD, GARBAGE_LOG_SWEEP_CURSOR, GcPublishResult, GcStagedCopiedRecord,
    LSM_GARBAGE_LOG_MAX_BYTES, RELOCATION_LSM_MANIFEST, Result,
    blob_lsm::terminal_garbage_record,
    fs_util::{segment_garbage_log_path, unlink_gc_segment_files},
    gc::{
        GcExecutor, GcPrepublishedCopy, GcPrepublishedOutputSegment, OverlayRecordClassifier,
        OverlayRecordState,
    },
    layout::segment_state_path,
    maintenance::{garbage_log_dir, read_relocation_lsm_manifest},
    metrics::GcKnownDelta,
    relocation::RelocationEntry,
    shard_gc::shard_generation_is_obsolete,
};
use core_types::{
    GarbageEvent, PlacementClass, SegmentFileState, SegmentGcRecordRange, SegmentId, SegmentState,
};
use gc_planner::{GcAction, GcScenario};
use lsm::{GarbageLog, SegmentGarbageLog};

#[derive(Debug)]
struct GcPreparedPublish {
    copy: GcPrepublishedCopy,
    reconciled_lsn: core_types::StrataLsn,
    skipped_records: Vec<GcSkippedCopiedRecord>,
}

impl GcExecutor {
    /// Reconciles and publishes one prepared copy while excluding blob compaction. Foreground
    /// writes do not take this lock and do not enter this call path.
    pub(crate) fn submit_gc_publish(&self, copy: GcPrepublishedCopy) -> Result<GcPublishResult> {
        let admission_lock = Arc::clone(&self.compaction_admission_lock);
        let _admission_guard = admission_lock
            .write()
            .expect("compaction admission lock poisoned");
        self.drain_gc_reconciliation_log()?;
        let publish = self.prepare_gc_publish(copy)?;
        self.commit_gc_publish(publish)
    }

    /// Folds all committed garbage into the per-segment overlays used for revalidation. Each
    /// bounded sweep owns the publication lock only for its own synced metadata batch. That sync
    /// also makes any earlier relocation activation durable.
    fn drain_gc_reconciliation_log(&self) -> Result<()> {
        let garbage_publish_lock = Arc::clone(&self.garbage_publish_lock);
        loop {
            let (swept, relocation_lsn) = {
                let _publish_guard = garbage_publish_lock
                    .lock()
                    .expect("garbage publication lock poisoned");
                let relocation_lsn = self.relocations.lsm().last_lsn()?.unwrap_or_default();
                let swept = self.index.sweep_garbage_log(
                    garbage_log_dir(&self.config),
                    self.config.namespace_dir(),
                    GARBAGE_LOG_HEAD,
                    GARBAGE_LOG_SWEEP_CURSOR,
                )?;
                (swept, relocation_lsn)
            };
            if !swept {
                break;
            }
            self.durable_relocation_lsn
                .fetch_max(relocation_lsn, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// Revalidates copied ranges against committed segment-local garbage state.
    ///
    /// Decides, for each copied record: is it still worth publishing?
    /// `reconciled_lsn` is the latest visible foreground sequence observed for this pass.
    /// Metadata actions skip the line:
    /// DeleteSegment(s) / ReclassifySegment copied nothing, so there's nothing to revalidate - return immediately.
    /// Load an overlay + classifier per distinct source segment. A missing overlay means the source vanished → GcMissingSourceSegment.
    /// Sort all records by (segment_id, offset, len, payload_lsn, shard, key). This is not cosmetic — it's correctness critical.
    /// The OverlayRecordClassifier is a forward-only cursor merging against the overlay's sorted range lists; it never rewinds.
    /// The sort also makes survivor order and table construction deterministic.
    /// Classify each record, in priority order:
    /// Shard generation fenced? (shard_generation_is_obsolete) — the whole shard was dropped/recreated, so everything from the old generation is dead → skip as Retired.
    /// Overlay says Retired (a newer write replaced it) → skip.
    /// Overlay says Expired (explicit terminal expiry event) → skip.
    /// CopyEligible { lifecycle } → survivor.
    /// Bonus: the record's lifecycle is refreshed from the overlay, since it may know a newer routing hint than the planner did.
    /// An overlay range must either fully contain a record or not touch it at all — partial overlap is an error,
    /// because GC moves whole encoded records and can't split a payload's liveness.
    /// Example: A and D come out as survivors; B is skipped(Retired), C is skipped(Expired).
    fn prepare_gc_publish(&self, mut copy: GcPrepublishedCopy) -> Result<GcPreparedPublish> {
        let reconciled_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if matches!(
            &copy.plan.action,
            GcAction::DeleteSegment { .. }
                | GcAction::DeleteSegments { .. }
                | GcAction::ReclassifySegment { .. }
        ) {
            return Ok(GcPreparedPublish {
                copy,
                reconciled_lsn,
                skipped_records: Vec::new(),
            });
        }

        let source_segment_ids = copy
            .copied_records
            .iter()
            .map(|record| record.source.from.segment_id)
            .collect::<BTreeSet<_>>();
        let overlays = source_segment_ids
            .into_iter()
            .map(|segment_id| {
                self.index
                    .read_segment_garbage_overlay(self.config.namespace_dir(), segment_id)?
                    .map(|overlay| (segment_id, overlay))
                    .ok_or(Error::GcMissingSourceSegment { segment_id })
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let clock_expiry_epoch = self.index.clock_expiry_epoch()?;
        let mut classifiers = overlays
            .iter()
            .map(|(&segment_id, overlay)| {
                (
                    segment_id,
                    OverlayRecordClassifier::new(segment_id, overlay, clock_expiry_epoch),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let mut records = std::mem::take(&mut copy.copied_records);
        records.sort_by_key(|record| {
            (
                record.source.from.segment_id,
                record.source.from.offset,
                record.source.from.len,
                record.source.payload_lsn,
                record.source.shard,
                record.source.key.clone(),
            )
        });
        let mut survivors = Vec::with_capacity(records.len());
        let mut skipped_records = Vec::new();
        for mut record in records {
            let skipped = if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                Some(GcSkippedCopiedRecordKind::Retired)
            } else {
                let classifier = classifiers
                    .get_mut(&record.source.from.segment_id)
                    .expect("classifier was initialized for every copied source segment");
                match classifier.classify(SegmentGcRecordRange::from(record.source.from))? {
                    OverlayRecordState::Retired => Some(GcSkippedCopiedRecordKind::Retired),
                    OverlayRecordState::Expired => Some(GcSkippedCopiedRecordKind::Expired),
                    OverlayRecordState::CopyEligible { lifecycle } => {
                        record.source.lifecycle = lifecycle;
                        None
                    }
                }
            };
            if let Some(kind) = skipped {
                skipped_records.push(GcSkippedCopiedRecord { record, kind });
            } else {
                survivors.push(record);
            }
        }
        copy.copied_records = survivors;
        Ok(GcPreparedPublish {
            copy,
            reconciled_lsn,
            skipped_records,
        })
    }

    /// Publishes surviving copies with a two-part protocol:
    ///
    /// 1. Write, fsync, and rename one relocation patch SST. It has an independent relocation-LSM
    ///    sequence; GC does not reserve foreground LSNs or touch the main WAL.
    /// 2. Under the garbage-publication lock, atomically add that table to the relocation manifest
    ///    and publish output/source/garbage metadata with a synced batch.
    ///
    /// A crash before activation leaves an orphan SST that startup removes. A crash after activation
    /// can recover it because the SST was durable first. The activation batch itself advances
    /// `durable_relocation_lsn`, so source deletion never depends on foreground writer durability.
    ///
    /// The rest of this comment walks the function in code order, reusing the cast from
    /// prepare_gc_publish: source segment S7 (sealed, 100 MB) whose records A, B, C, D were copied
    /// into staging file T900, pre-assigned durable id S42. B was retired and C expired mid-copy,
    /// so A and D are the survivors. Say the durable blob frontier sits at LSN 1000 and the
    /// relocation LSM's last table sequence is 86.
    ///
    /// Opening moves — the exits that need no protocol. A metadata action (DeleteSegment(s) /
    /// ReclassifySegment) must arrive with zero staged outputs and zero copied records; anything
    /// else is a GcInvalidPlan. Valid ones route to apply_gc_metadata_action and return an empty
    /// result — nothing below concerns them. For copy actions, every survivor's shard generation
    /// is checked one more time. This is not paranoia: shard drops run on the foreground writer
    /// lane, which this executor does not block, so a drop can land between prepare's
    /// classification and this moment — the re-check catches that race and aborts cleanly before
    /// anything durable happens. And if reconciliation left no survivors at all, the staging files
    /// are pure waste: each output's row flips to Deleted in one small unsynced batch and the
    /// function returns the skipped records. The files are unlinked later through the delete
    /// path, and losing that batch to a crash is fine — the rows would still read
    /// PendingGcOutput, which startup cleanup deletes anyway.
    ///
    /// Planning on paper. plan_gc_output_segments decides which staging files deserve to exist:
    /// T900 held survivors, so it maps to durable id S42 and gets a drafted Sealed row; a staging
    /// file whose every copy died is left out and receives a Deleted row in the final batch
    /// instead. Then the byte audit: the sealed lengths of the promoted outputs (all of T900 —
    /// A+B+C+D bytes, because dead records still occupy a sealed file) are totalled twice, once
    /// directly and once attributed back to source segments by gc_output_bytes_by_source. If the
    /// totals disagree the plan is internally inconsistent and publication aborts with
    /// InvariantViolation. skipped_gc_output_ranges then regroups the stale copies by final
    /// output and terminal kind (B → (S42, Retired), C → (S42, Expired)) for the birth metadata
    /// below, and plan_gc_relocating_source_states drafts S7's Sealed → GcRelocating row so the
    /// source gets fenced in the same atomic batch that publishes its relocations.
    ///
    /// The fence, not a sequence. Here is the biggest difference from a foreground write: a
    /// relocation is a physical publication, not a logical blob mutation, so GC allocates no new
    /// foreground LSNs. Instead the current durable blob frontier — get_committed_lsn(), 1000 in
    /// the example — becomes the shared publish_lsn "fence" on every row, and
    /// assign_gc_publish_fence stamps each survivor with it while swapping T900 for S42 in its
    /// destination ref (payload_lsn, the LSN of the original write, is untouched forever).
    /// apply_gc_output_lsn_bounds then gives S42 min_lsn = max_lsn = 1000. Ordering between
    /// individual relocations is owned instead by the relocation LSM's private table sequence,
    /// assigned in part one below.
    ///
    /// Everything that can be built outside the lock, is. Three artifacts are prepared up front:
    /// the relocation entries themselves ((key, shard, payload_lsn) → fence + S42 ref); the
    /// terminal garbage for the sources — one Retired event each for A@S7 and D@S7 at the fence
    /// LSN, sorted by (key, lsn, offset, len) because the garbage log appends exactly one sorted
    /// frame; and S42's birth metadata from initial_gc_output_metadata — a summary accounting
    /// every sealed byte (A and D live with unknown lifetime, B retired at birth, C expired at
    /// birth) plus B's and C's terminal events for S42's own segment garbage log, stamped with
    /// their original payload LSNs since a skipped record never earns a publication. Those
    /// per-output log files are even created and appended here, before the lock — they sit beside
    /// output files that nothing references yet, so they are exactly as invisible as the outputs
    /// themselves.
    ///
    /// Part one: the durable half. prepare_l0 partitions the relocation entries and turns them into
    /// immutable patch SSTs. It claims consecutive manifest table ids, takes the relocation LSM's
    /// next sequence — 87, which becomes the shared activation sequence — sorts each partition by
    /// key identity, rejects duplicate identities, and writes, fsyncs, and renames the files into
    /// the table directory. The manifest edit that would add these tables is still just a value in
    /// memory.
    /// This is the trick the whole protocol stands on: the expensive durable artifact is created
    /// first, but it is inert — no manifest references it, no reader can see it, and a crash right
    /// now leaves an orphan that remove_orphan_tables deletes at the next open.
    ///
    /// Part two: activation. Under the garbage-publication lock, the global garbage log is opened
    /// at its last committed position and the retirement frame is appended and synced. One
    /// RocksDB batch then assembles the entire publication: the manifest merge that adds the
    /// patch SST to the relocation manifest (the activation itself); S42's Sealed row, birth
    /// summary, segment-garbage-log position, and published_at_lsn of 1000 (what snapshot
    /// protection compares against); S7's GcRelocating row; the new global garbage-log head;
    /// reclaim-pending rows keyed (S7, activation sequence 87) → S42's byte total, which later
    /// both gates S7's deletion on durability and prices its net reclamation; Deleted rows for
    /// staging files that published nothing; and, tallied alongside, the metric delta for B's and
    /// C's newborn garbage. GC writes this batch with sync, reads the merged manifest back, and
    /// installs it into the in-memory relocation LSM. This is the instant readers start resolving
    /// A and D to S42. The durable frontier then advances through sequence 87, so deletion of S7
    /// is safe without waiting for any foreground writer sync.
    ///
    /// Aftermath. On success the relocation cache is warmed with the new destinations, metrics
    /// absorb the known-garbage delta, the relocating-segment count, and the published byte
    /// total, and the LSM compactor gets a best-effort nudge (the inline comment explains why a
    /// compaction pass is wanted at all). The two failure arms fall on either side of the batch
    /// write. BeforeIndexBatch means activation never happened: the orphan patch SSTs are unlinked
    /// best-effort (startup would remove it anyway) and the error returns cleanly - no halt, the
    /// plan is simply retryable. IndexCommit means the batch write, or the manifest read-back and
    /// install after it, failed - the durable and in-memory views can no longer be trusted to
    /// agree, and that halts the store.
    ///
    /// S7 is still deleted through the ordinary guarded cleanup path, but its activation is already
    /// durable when this function returns. The durable frontier check remains the deletion fence;
    /// it now passes immediately instead of waiting for unrelated foreground work.
    fn commit_gc_publish(&self, publish: GcPreparedPublish) -> Result<GcPublishResult> {
        let GcPreparedPublish {
            copy,
            reconciled_lsn,
            skipped_records,
        } = publish;
        match &copy.plan.action {
            GcAction::DeleteSegment { .. }
            | GcAction::DeleteSegments { .. }
            | GcAction::ReclassifySegment { .. } => {
                if !copy.outputs.is_empty() || !copy.copied_records.is_empty() {
                    return Err(Error::GcInvalidPlan(
                        "metadata action cannot include staged outputs or copied records",
                    ));
                }
                self.apply_gc_metadata_action(copy.plan.scenario, &copy.plan.action)?;
                return Ok(GcPublishResult {
                    reconciled_lsn,
                    output_segments: Vec::new(),
                    published_records: Vec::new(),
                    skipped_records: Vec::new(),
                });
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveLiveBytesFromSources { .. } => {}
        }

        let survivors = copy.copied_records;
        for record in &survivors {
            if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                return Err(Error::GcInvalidPlan(
                    "copied shard generation became obsolete before publish",
                ));
            }
        }

        if survivors.is_empty() {
            let mut batch = self.index.batch();
            for output in &copy.outputs {
                self.index
                    .put_segment_state_batch(&mut batch, &output.deleted_state(&self.config))?;
            }
            batch.write().map_err(index::Error::from)?;
            return Ok(GcPublishResult {
                reconciled_lsn,
                output_segments: Vec::new(),
                published_records: Vec::new(),
                skipped_records: skipped_records
                    .into_iter()
                    .map(|skipped| skipped.record)
                    .collect(),
            });
        }

        let mut output_plan = self.plan_gc_output_segments(&copy.outputs, &survivors)?;
        let published_output_bytes =
            output_plan
                .published_outputs
                .iter()
                .try_fold(0_u64, |total, output| {
                    total
                        .checked_add(output.sealed_len)
                        .ok_or(segment::Error::RangeOverflow)
                })?;
        let output_bytes_by_source =
            gc_output_bytes_by_source(&survivors, &skipped_records, &output_plan.used_staged_ids)?;
        let attributed_output_bytes =
            output_bytes_by_source
                .values()
                .try_fold(0_u64, |total, bytes| {
                    total
                        .checked_add(*bytes)
                        .ok_or(segment::Error::RangeOverflow)
                })?;
        if attributed_output_bytes != published_output_bytes {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published GC output bytes {published_output_bytes} do not match source-attributed bytes {attributed_output_bytes}"
                ),
            });
        }
        let skipped_output_ranges =
            skipped_gc_output_ranges(&skipped_records, &output_plan.staged_to_final_segment_id);
        let relocating_source_states = self.plan_gc_relocating_source_states(
            survivors.iter().map(|record| record.source.from.segment_id),
        )?;
        // Relocation is a physical publication, not a logical blob mutation. All rows in this L0
        // share the latest durable blob frontier as their logical ordering fence; the relocation
        // LSM assigns its own sequence to the immutable table below.
        let publish_lsn = self.index.get_committed_lsn()?;
        let published_records = match assign_gc_publish_fence(
            publish_lsn,
            &survivors,
            &output_plan.staged_to_final_segment_id,
        ) {
            Ok(records) => records,
            Err(error) => return Err(error),
        };
        apply_gc_output_lsn_bounds(&mut output_plan.segment_states, &published_records);
        let relocation_entries = published_records
            .iter()
            .map(|record| RelocationEntry {
                key: record.source.key.clone(),
                shard: record.source.shard,
                payload_lsn: record.source.payload_lsn,
                publish_lsn: record.publish_lsn,
                to: record.to,
            })
            .collect::<Vec<_>>();
        let mut relocation_garbage = published_records
            .iter()
            .map(|record| {
                terminal_garbage_record(
                    record.source.key.as_bytes(),
                    record.publish_lsn,
                    record.source.from,
                    record.source.lifecycle,
                    GarbageEvent::Retired {
                        record: record.source.from,
                    },
                )
                .map_err(Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        relocation_garbage.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.lsn.cmp(&right.lsn))
                .then_with(|| left.event.record().offset.cmp(&right.event.record().offset))
                .then_with(|| left.event.record().len.cmp(&right.event.record().len))
        });
        let (output_summaries, output_garbage) = initial_gc_output_metadata(
            &output_plan.segment_states,
            &published_records,
            &skipped_records,
            &output_plan.staged_to_final_segment_id,
        )?;
        let mut output_garbage_positions = BTreeMap::new();
        for state in &output_plan.segment_states {
            let Some(records) = output_garbage.get(&state.segment_id) else {
                continue;
            };
            let path = segment_garbage_log_path(segment_state_path(&self.config, state));
            let position = SegmentGarbageLog::open(path, 0)
                .map_err(Error::from)?
                .append(records)
                .map_err(Error::from)?;
            output_garbage_positions.insert(state.segment_id, position);
        }
        let (activation_sequence, relocation_edit) =
            self.relocations.prepare_l0(&relocation_entries)?;
        let garbage_publish_lock = Arc::clone(&self.garbage_publish_lock);
        let mut skipped_output_delta = GcKnownDelta::default();
        let commit_result = (|| {
            let _garbage_publish_guard = garbage_publish_lock
                .lock()
                .expect("garbage publication lock poisoned");
            let committed_garbage = self
                .index
                .get_garbage_log_position(GARBAGE_LOG_HEAD)?
                .unwrap_or_default();
            let mut garbage_log = GarbageLog::open(
                garbage_log_dir(&self.config),
                LSM_GARBAGE_LOG_MAX_BYTES,
                committed_garbage,
            )
            .map_err(Error::from)?;
            let garbage_position = garbage_log
                .append(&relocation_garbage)
                .map_err(Error::from)?;
            let mut batch = self.index.batch();
            self.index.merge_lsm_manifest_batch(
                &mut batch,
                RELOCATION_LSM_MANIFEST,
                &relocation_edit,
            )?;
            for state in &output_plan.segment_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
                self.index.put_segment_gc_summary_batch(
                    &mut batch,
                    state.segment_id,
                    output_summaries
                        .get(&state.segment_id)
                        .expect("every output has an initial summary"),
                )?;
                if let Some(position) = output_garbage_positions.get(&state.segment_id) {
                    self.index.put_segment_garbage_log_position_batch(
                        &mut batch,
                        state.segment_id,
                        *position,
                    )?;
                }
                self.index.put_segment_published_at_lsn_batch(
                    &mut batch,
                    state.segment_id,
                    publish_lsn,
                )?;
            }
            for state in &relocating_source_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
            }
            self.index.put_garbage_log_position_batch(
                &mut batch,
                GARBAGE_LOG_HEAD,
                garbage_position,
            )?;
            for (source_segment_id, output_bytes) in &output_bytes_by_source {
                self.index.put_gc_reclaim_pending_batch(
                    &mut batch,
                    *source_segment_id,
                    activation_sequence,
                    *output_bytes,
                    copy.plan.scenario.metric_label(),
                )?;
            }
            for output in &copy.outputs {
                if !output_plan
                    .used_staged_ids
                    .contains(&output.staged_segment_id)
                {
                    self.index
                        .put_segment_state_batch(&mut batch, &output.deleted_state(&self.config))?;
                }
            }
            for ((_segment_id, kind), ranges) in skipped_output_ranges {
                let bytes = ranges.iter().map(|range| range.len).sum::<u64>();
                skipped_output_delta.total_bytes += i128::from(bytes);
                match kind {
                    GcSkippedCopiedRecordKind::Retired => {
                        skipped_output_delta.retired_bytes += i128::from(bytes);
                    }
                    GcSkippedCopiedRecordKind::Expired => {
                        skipped_output_delta.expired_bytes += i128::from(bytes);
                    }
                }
            }

            if let Err(error) = batch
                .write_with_sync(true)
                .map_err(index::Error::from)
                .map_err(Error::from)
            {
                return Err(GcPublishCommitError::IndexCommit(error));
            }
            self.relocations
                .lsm()
                .reload_manifest(|| read_relocation_lsm_manifest(&self.index))
                .map_err(Error::from)
                .map_err(GcPublishCommitError::IndexCommit)?;
            self.durable_relocation_lsn
                .fetch_max(activation_sequence, std::sync::atomic::Ordering::Release);
            Ok::<(), GcPublishCommitError>(())
        })();

        match commit_result {
            Ok(()) => {
                for record in &published_records {
                    self.relocation_cache.insert(
                        record.source.key.clone(),
                        record.source.shard,
                        record.source.payload_lsn,
                        record.to,
                    );
                }
                self.metrics.apply_gc_known_delta(skipped_output_delta);
                self.metrics
                    .add_gc_relocating_segments(relocating_source_states.len());
                self.metrics.record_gc_output_published(
                    copy.plan.scenario.metric_label(),
                    published_output_bytes,
                );
                // A foreground mutation may have stayed copy-eligible because it had not reached
                // the garbage log before this publication. Prompt a relocation-aware compaction
                // to project that mutation (or the current lifecycle) onto the new destination.
                let _ = self.lsm_compact_tx.send(());
                Ok(GcPublishResult {
                    reconciled_lsn,
                    output_segments: output_plan.published_outputs,
                    published_records,
                    skipped_records: skipped_records
                        .into_iter()
                        .map(|skipped| skipped.record)
                        .collect(),
                })
            }
            Err(GcPublishCommitError::BeforeIndexBatch(error)) => {
                for table in &relocation_edit.add_patches {
                    let _ = std::fs::remove_file(
                        self.relocations
                            .lsm()
                            .table_store()
                            .root()
                            .join(&table.relative_path),
                    );
                }
                Err(error)
            }
            Err(GcPublishCommitError::IndexCommit(error)) => {
                self.store_halt
                    .halt(format!("fatal GC relocation activation error: {error}"));
                Err(error)
            }
        }
    }

    /// Dispatcher for the byte-free plans: DeleteSegment(s) fall through to
    /// delete_empty_gc_segments, ReclassifySegment to reclassify_gc_segment. The last arm is the
    /// mirror image of the guard in commit_gc_publish: a copy action arriving here is exactly as
    /// invalid as a metadata action arriving with staged outputs, and both fail the same way.
    fn apply_gc_metadata_action(&self, scenario: GcScenario, action: &GcAction) -> Result<()> {
        match action {
            GcAction::DeleteSegment { segment_id } => {
                self.delete_empty_gc_segments(&[*segment_id], scenario)?;
            }
            GcAction::DeleteSegments { segment_ids } => {
                self.delete_empty_gc_segments(segment_ids, scenario)?;
            }
            GcAction::ReclassifySegment {
                segment_id,
                placement_class,
            } => {
                self.reclassify_gc_segment(*segment_id, *placement_class)?;
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveLiveBytesFromSources { .. } => {
                return Err(Error::GcInvalidPlan(
                    "copy action must use the copy publish path",
                ));
            }
        }
        Ok(())
    }

    /// Fences every source that published at least one surviving relocation.
    ///
    /// In the example: S7's row is rewritten Sealed → GcRelocating. Only Sealed sources qualify —
    /// finding anything else here means the claim guard failed to keep the planner away and the
    /// publish aborts with GcSourceSegmentNotSealed before touching durable state.
    ///
    /// The state transition shares the same RocksDB batch as the relocation-table descriptors.
    /// A planner therefore cannot observe a committed relocation without also observing that its
    /// source is ineligible for another copy plan.
    fn plan_gc_relocating_source_states(
        &self,
        source_ids: impl IntoIterator<Item = SegmentId>,
    ) -> Result<Vec<SegmentState>> {
        let source_ids = source_ids.into_iter().collect::<BTreeSet<_>>();
        let mut states = Vec::with_capacity(source_ids.len());
        for segment_id in source_ids {
            let mut state = self
                .index
                .get_segment_state(segment_id)?
                .ok_or(Error::GcMissingSourceSegment { segment_id })?;
            if state.state != SegmentFileState::Sealed {
                return Err(Error::GcSourceSegmentNotSealed {
                    segment_id,
                    state: state.state,
                });
            }
            state.state = SegmentFileState::GcRelocating;
            states.push(state);
        }
        Ok(states)
    }

    /// The end of a source segment's life. After the publish above, S7 sits in GcRelocating while
    /// the sweeper folds its retirement events into its overlay and summary; once the summary
    /// proves nothing live remains, a DeleteSegment plan lands here.
    ///
    /// Every segment must pass four gates before its state flips to Deleted:
    /// 1. It must exist in the index — a vanished id is GcMissingSourceSegment.
    /// 2. Already-Deleted segments skip the gates and go straight into the unlink list. That is
    ///    the retry path: a crash after the state flip but before the unlink leaves a Deleted row
    ///    with a file still on disk, and re-running the plan finishes the job instead of erroring.
    /// 3. It must be Sealed or GcRelocating — deleting an active or half-sealed segment is a
    ///    planner bug (GcSourceSegmentNotSealed).
    /// 4. Its GC summary must show live_ref_count == 0 (GcSourceSegmentNotEmpty otherwise). The
    ///    summary only advances when the garbage log is swept, which is why publication drains the
    ///    log first: for S7 this gate cannot pass until the relocation retirement events have
    ///    been folded in.
    /// Plus one silent gate: if a live snapshot is pinned at an LSN >= the segment's
    /// published_at_lsn, the snapshot could still read these bytes, so the segment is quietly
    /// skipped this round — no error, the planner simply proposes it again after the snapshot
    /// closes.
    ///
    /// A relocating source has one additional gate: a later RocksDB sync must have made its
    /// relocation manifest activation durable. Then Deleted rows commit first in one synced
    /// batch (with the known-garbage summaries and the relocating-segment gauge retired from
    /// metrics), reader-cache entries are evicted, and only then are files unlinked. Last, the
    /// reclaim-pending rows written at publish time are consumed and net reclamation is recorded:
    /// for S7 that is its 100 MB of file freed, offset by the S42 bytes GC created on its behalf —
    /// so the metric reports what GC actually gave back, not just what it unlinked.
    fn delete_empty_gc_segments(
        &self,
        segment_ids: &[SegmentId],
        fallback_scenario: GcScenario,
    ) -> Result<()> {
        let mut states = Vec::with_capacity(segment_ids.len());
        let mut states_to_commit = Vec::new();
        let mut summaries_to_remove = Vec::new();
        let mut relocating_segments_to_remove = 0;
        // Read the clock bound before the summaries: it only advances, so a summary read later
        // can only have fewer clock-live records than this bound admits.
        let clock_expiry_epoch = self.index.clock_expiry_epoch()?;
        for segment_id in segment_ids {
            let mut state = self.index.get_segment_state(*segment_id)?.ok_or(
                Error::GcMissingSourceSegment {
                    segment_id: *segment_id,
                },
            )?;
            if state.state == SegmentFileState::Deleted {
                states.push(state);
                continue;
            }
            if !matches!(
                state.state,
                SegmentFileState::Sealed | SegmentFileState::GcRelocating
            ) {
                return Err(Error::GcSourceSegmentNotSealed {
                    segment_id: *segment_id,
                    state: state.state,
                });
            }
            let summary = self
                .index
                .get_segment_gc_summary(*segment_id)?
                .unwrap_or_default();
            let live_ref_count = match clock_expiry_epoch {
                Some(epoch) => summary.live_after_epoch(epoch).refs,
                None => summary.live_ref_count,
            };
            if live_ref_count != 0 {
                return Err(Error::GcSourceSegmentNotEmpty {
                    segment_id: *segment_id,
                    live_ref_count,
                });
            }
            if state.state == SegmentFileState::GcRelocating
                && !self.relocation_activation_is_durable(state.segment_id)?
            {
                continue;
            }
            let published_at_lsn = self.index.get_segment_published_at_lsn(state.segment_id)?;
            if self.live_snapshots.protects(published_at_lsn) {
                continue;
            }

            if state.state == SegmentFileState::GcRelocating {
                relocating_segments_to_remove += 1;
            }
            state.state = SegmentFileState::Deleted;
            summaries_to_remove.push(summary);
            states_to_commit.push(state.clone());
            states.push(state);
        }

        if !states_to_commit.is_empty() {
            let mut batch = self.index.batch();
            for state in &states_to_commit {
                self.index.put_segment_state_batch(&mut batch, state)?;
            }
            batch.write_with_sync(true).map_err(index::Error::from)?;
            for summary in &summaries_to_remove {
                self.metrics.remove_gc_known_summary(summary);
            }
            self.metrics
                .remove_gc_relocating_segments(relocating_segments_to_remove);
        }

        for state in &states {
            self.reader_cache.evict(state.segment_id);
            self.metrics.record_reader_cache_eviction();
        }
        let unlinked_segments = unlink_gc_segment_files(&self.config, &states)?;
        if !unlinked_segments.is_empty() {
            let source_segment_ids = unlinked_segments
                .iter()
                .map(|(segment_id, _)| *segment_id)
                .collect::<Vec<_>>();
            let mut batch = self.index.batch();
            let attribution_by_source = self
                .index
                .remove_gc_reclaim_pending_for_sources_batch(&mut batch, &source_segment_ids)?;
            batch.write().map_err(index::Error::from)?;
            for (segment_id, source_bytes) in unlinked_segments {
                let attribution = attribution_by_source.get(&segment_id).cloned().unwrap_or(
                    index::GcReclaimAttribution {
                        output_bytes: 0,
                        strategy: None,
                    },
                );
                let strategy = match attribution.strategy.as_deref() {
                    Some(strategy) => strategy,
                    None if attribution.output_bytes == 0 => fallback_scenario.metric_label(),
                    None => "unknown",
                };
                self.metrics.record_gc_source_deleted(
                    strategy,
                    source_bytes,
                    attribution.output_bytes,
                );
            }
        }
        Ok(())
    }

    /// Answers one question for a GcRelocating source: can this segment's file be deleted yet, or
    /// could a crash still lose the pointers that replaced it?
    ///
    /// Two different sequence spaces are easy to confuse around GC, and this function compares
    /// entirely within one of them, so first the vocabulary:
    ///
    /// - Foreground store LSNs — payload LSNs, the publish fence (1000 in the running example) —
    ///   never appear in this function at all.
    /// - The *relocation sequence* is the relocation LSM's own private counter, unrelated to
    ///   store LSNs. Every patch table GC publishes takes the next value; prepare_l0 stamped the
    ///   example's table (all entries in that patch file) with sequence 87. It orders relocation tables and nothing else.
    /// - The *activation LSN* of a source segment is a bookmark into that sequence: the sequence
    ///   number 87 (the example's table) of the relocation lsm table whose activation published this source's replacement pointers. It is
    ///   recorded at publish time as the reclaim pending row (S7, 87) → bytes, in the same synced
    ///   RocksDB batch as S7's GcRelocating flip and the manifest merge itself. That shared batch
    ///   makes the state, bookmark, and relocation manifest atomic and durable together.
    ///   get_gc_reclaim_activation_lsn returns the *max* across the source's rows - a source
    ///   published more than once must wait for its latest activation, and since the frontier
    ///   below only moves forward, covering the max covers them all.
    /// - `durable_relocation_lsn` is the store-wide frontier meaning "every activation at or
    ///   below this sequence is provably on disk." It is seeded at open from the recovered
    ///   manifest (whatever the durable manifest already references is durable by definition) and
    ///   advances as part of GC activation after its own synced RocksDB write. Recovery seeds it
    ///   from the durable manifest, while sweeps and relocation compaction may conservatively
    ///   reaffirm an already-durable frontier.
    ///
    /// So the predicate is simply: activation 87 <= durable frontier. Activation cannot become
    /// visible before it is durable: the synced batch commits before the manifest is installed in
    /// memory and before this frontier advances. The predicate remains a defensive deletion fence
    /// for recovery and legacy relocation paths.
    ///
    /// A missing activation row answers false for the same reason: with no record of which
    /// sequence to wait for, durability cannot be proven, so the deletion stays blocked rather
    /// than guessed at.
    pub(crate) fn relocation_activation_is_durable(
        &self,
        source_segment_id: SegmentId,
    ) -> Result<bool> {
        let Some(activation_lsn) = self
            .index
            .get_gc_reclaim_activation_lsn(source_segment_id)?
        else {
            return Ok(false);
        };
        Ok(activation_lsn
            <= self
                .durable_relocation_lsn
                .load(std::sync::atomic::Ordering::Acquire))
    }

    /// Rewrites a sealed segment's placement class without moving a single byte.
    ///
    /// Two cases use this: an unknown-heavy ingest segment that would gain little from being
    /// rewritten, and an ExactEpoch(50) segment whose records were extended and remain too live to
    /// copy economically. The planner labels either one Spillover so fragmentation-driven plans
    /// can handle it later. Only the durable label steering future planner decisions changes; the
    /// file, its id, owner, path, and bytes stay put. Avoiding a physical rename also avoids a
    /// second crash-consistency protocol for moving a file beside its metadata row. Only Sealed
    /// segments qualify, a matching class is a no-op, and the single-row change commits synced.
    fn reclassify_gc_segment(
        &self,
        segment_id: SegmentId,
        placement_class: PlacementClass,
    ) -> Result<()> {
        let mut state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        if state.state != SegmentFileState::Sealed {
            return Err(Error::GcSourceSegmentNotSealed {
                segment_id,
                state: state.state,
            });
        }
        if state.placement_class == placement_class {
            return Ok(());
        }

        state.placement_class = placement_class;
        let mut batch = self.index.batch();
        self.index.put_segment_state_batch(&mut batch, &state)?;
        batch.write_with_sync(true).map_err(index::Error::from)?;
        Ok(())
    }

    /// Selects the prepublished output segments that still contain surviving copied records.
    ///
    /// Only outputs that contain surviving copied records are finalized as `Sealed`. The returned
    /// map translates temporary staged segment ids to final durable segment ids. In the example:
    /// A and D survived in T900, so T900 lands in `used_staged_ids`, maps to S42, and contributes
    /// S42's drafted Sealed row. Had B and C been T900's only records, T900 would be absent from
    /// everything returned here and commit_gc_publish would write its Deleted row instead — an
    /// entire output demoted from "publish" to "cleanup" by this one filter. A survivor pointing
    /// at a staged id with no registered output row is GcMissingStagedOutput: the executor handed
    /// over a copy without its container. Nothing durable has happened at this point, so that
    /// abort is clean. (Field-by-field semantics of the returned plan are documented on
    /// PlannedGcOutputSegments in output.rs.)
    fn plan_gc_output_segments(
        &self,
        outputs: &[GcPrepublishedOutputSegment],
        survivors: &[GcStagedCopiedRecord],
    ) -> Result<PlannedGcOutputSegments> {
        let used_staged_ids = survivors
            .iter()
            .map(|record| record.staged.segment_id)
            .collect::<BTreeSet<_>>();
        let mut outputs_by_staged_id = outputs
            .iter()
            .map(|output| (output.staged_segment_id, output))
            .collect::<BTreeMap<_, _>>();
        let mut staged_to_final_segment_id = BTreeMap::new();
        let mut published_outputs = Vec::new();
        let mut segment_states = Vec::new();

        for staged_segment_id in &used_staged_ids {
            let output = match outputs_by_staged_id.remove(staged_segment_id) {
                Some(output) => output,
                None => {
                    return Err(Error::GcMissingStagedOutput {
                        staged_segment_id: *staged_segment_id,
                    });
                }
            };
            let final_segment_id = output.segment_id;
            let published_output = output.published_output();
            published_outputs.push(published_output);
            staged_to_final_segment_id.insert(output.staged_segment_id, final_segment_id);
            segment_states.push(output.sealed_state(&self.config));
        }

        Ok(PlannedGcOutputSegments {
            staged_to_final_segment_id,
            used_staged_ids,
            published_outputs,
            segment_states,
        })
    }
}
