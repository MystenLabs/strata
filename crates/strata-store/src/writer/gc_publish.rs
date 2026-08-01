//! Ordered GC publication: reconciles prepared copies against current blob
//! state and commits output metadata in the writer lane.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use strata_core::{GarbageEvent, PlacementClass, SegmentFileState, SegmentId, SegmentState};
use strata_gc::GcAction;
use strata_lsm::{GarbageLog, SegmentGarbageLog, StoredValue, decode_value};
use strata_relocation::RelocationEntry;

use super::gc_output::{
    GcPublishCommitError, GcSkippedCopiedRecord, GcSkippedCopiedRecordKind,
    PlannedGcOutputSegments, apply_gc_output_lsn_bounds, assign_gc_publish_lsns,
    gc_output_bytes_by_source, initial_gc_output_metadata, skipped_gc_output_ranges,
};
use crate::{
    Error, GARBAGE_LOG_HEAD, GcPreparedPublish, GcPublishResult, GcStagedCopiedRecord,
    LSM_GARBAGE_LOG_MAX_BYTES, Result, WriteCoordinator,
    blob_lsm::{BlobMerge, BlobState as LsmBlobState, terminal_garbage_record},
    fs_util::{segment_garbage_log_path, unlink_gc_segment_files},
    gc::{GcPrepublishedCopy, GcPrepublishedOutputSegment},
    layout::segment_state_path,
    maintenance::{flush_relocation_lsm, garbage_log_dir, publish_blob_lsm_edit},
    metrics::GcKnownDelta,
    shard_gc::shard_generation_is_obsolete,
    wal::WalEntry,
    wal_format::StoreWalMutation,
};

impl WriteCoordinator {
    /// Publishes copied GC outputs after revalidating them in writer order.
    ///
    /// TODO: publish large GC copies in bounded chunks. File construction and fsync are outside
    /// this path, but a large copy still commits one RocksDB batch of relocation and segment
    /// metadata while foreground writes wait behind it.
    pub(crate) fn submit_gc_publish(
        &mut self,
        copy: GcPrepublishedCopy,
    ) -> Result<GcPublishResult> {
        let publish = self.prepare_gc_publish(copy)?;
        self.commit_gc_publish(publish)
    }

    /// Revalidates copied refs against the current LSM while holding the writer ordering lane.
    ///
    /// Every foreground mutation already ahead of this request is visible here, and no later
    /// mutation can enter until the matching relocation metadata batch is appended below.
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

        let current_epoch = self
            .index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)?;
        let mut survivors = Vec::with_capacity(copy.copied_records.len());
        let mut skipped_records = Vec::new();
        for mut record in std::mem::take(&mut copy.copied_records) {
            let skipped = if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                Some(GcSkippedCopiedRecordKind::Retired)
            } else {
                let state = match self.lsm.get(0, record.source.key.as_bytes(), &BlobMerge)? {
                    Some(encoded) => match decode_value(&encoded)? {
                        StoredValue::Inline(bytes) => Some(LsmBlobState::decode(bytes)?),
                        StoredValue::Blob { .. } => {
                            return Err(Error::InvariantViolation {
                                reason: format!(
                                    "materialized blob state for {:?} is segment-backed",
                                    record.source.key
                                ),
                            });
                        }
                    },
                    None => None,
                };
                match state {
                    Some(state)
                        if state
                            .versions
                            .get(&record.source.shard)
                            .is_some_and(|version| {
                                version.lsn == record.source.payload_lsn
                                    && version.record_ref == record.source.from
                            }) =>
                    {
                        match state.resolve(record.source.shard, current_epoch) {
                            Some((_, lifecycle)) => {
                                record.source.lifecycle = lifecycle;
                                None
                            }
                            None => Some(GcSkippedCopiedRecordKind::Expired),
                        }
                    }
                    _ => Some(GcSkippedCopiedRecordKind::Retired),
                }
            };
            if let Some(kind) = skipped {
                skipped_records.push(GcSkippedCopiedRecord { record, kind });
            } else {
                survivors.push(record);
            }
        }
        survivors.sort_by_key(|record| {
            (
                record.source.from.segment_id,
                record.source.from.offset,
                record.source.from.len,
                record.source.payload_lsn,
                record.source.shard,
                record.source.key.clone(),
            )
        });

        copy.copied_records = survivors;
        Ok(GcPreparedPublish {
            copy,
            reconciled_lsn,
            skipped_records,
        })
    }

    fn commit_gc_publish(&mut self, publish: GcPreparedPublish) -> Result<GcPublishResult> {
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
                self.apply_gc_metadata_action(&copy.plan.action)?;
                return Ok(GcPublishResult {
                    reconciled_lsn,
                    output_segments: Vec::new(),
                    published_records: Vec::new(),
                    skipped_records: Vec::new(),
                });
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
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
            batch.write().map_err(strata_index::Error::from)?;
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
                        .ok_or(strata_segment::Error::RangeOverflow)
                })?;
        let output_bytes_by_source =
            gc_output_bytes_by_source(&survivors, &skipped_records, &output_plan.used_staged_ids)?;
        let attributed_output_bytes =
            output_bytes_by_source
                .values()
                .try_fold(0_u64, |total, bytes| {
                    total
                        .checked_add(*bytes)
                        .ok_or(strata_segment::Error::RangeOverflow)
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
        let first_publish_lsn = self.index.get_next_lsn()?;
        let publish_lsns = (0..survivors.len())
            .map(|offset| {
                first_publish_lsn
                    .checked_add(
                        u64::try_from(offset)
                            .map_err(|_| Error::from(strata_segment::Error::RangeOverflow))?,
                    )
                    .ok_or_else(|| Error::from(strata_segment::Error::RangeOverflow))
            })
            .collect::<Result<Vec<_>>>()?;
        let published_records = match assign_gc_publish_lsns(
            &publish_lsns,
            &survivors,
            &output_plan.staged_to_final_segment_id,
        ) {
            Ok(records) => records,
            Err(error) => {
                self.halt_writer_error("apply GC LSM lsns", &error);
                return Err(error);
            }
        };
        let reclaim_publish_lsn = published_records
            .first()
            .expect("surviving GC records are non-empty")
            .publish_lsn;
        let last_publish_lsn = published_records
            .last()
            .expect("surviving GC records are non-empty")
            .publish_lsn;
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
        let wal_entries = relocation_entries
            .iter()
            .map(|entry| {
                Ok(WalEntry {
                    lsn: entry.publish_lsn,
                    payload: StoreWalMutation::Relocation(entry.clone()).encode()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if let Err(error) = self.wal.append(&wal_entries).map_err(Error::from) {
            self.halt_writer_error("GC store WAL append", &error);
            return Err(error);
        }
        let relocation_write = match self.relocations.write_batch(0, &relocation_entries) {
            Ok(write) => write,
            Err(error) => {
                let error = Error::from(error);
                self.halt_writer_error("GC relocation LSM write", &error);
                return Err(error);
            }
        };
        if relocation_write.lsns.len() != relocation_entries.len() {
            let error = Error::InvariantViolation {
                reason: "relocation LSM returned an unexpected LSN count".to_owned(),
            };
            self.halt_writer_error("GC relocation LSM write result", &error);
            return Err(error);
        }
        let store_checkpoint = match self.sync_store_files() {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.halt_writer_error("GC store durability checkpoint", &error);
                return Err(error);
            }
        };
        if self.wal.last_lsn() != Some(last_publish_lsn) {
            let error = Error::InvariantViolation {
                reason: format!(
                    "GC store WAL ends at {:?}, expected {last_publish_lsn}",
                    self.wal.last_lsn()
                ),
            };
            self.halt_writer_error("GC store durability frontier", &error);
            return Err(error);
        }
        let rolled_relocation_memtable = !relocation_write.rolled_memtables.is_empty();

        let pending_rollovers = self.take_pending_rollovers();
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
        let durability_publish_lock = Arc::clone(&self.durability_publish_lock);
        let mut skipped_output_delta = GcKnownDelta::default();
        let commit_result = (|| {
            let _publish_guard = durability_publish_lock
                .lock()
                .expect("durability publication lock poisoned");
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
            let (output_summaries, output_garbage) = initial_gc_output_metadata(
                &output_plan.segment_states,
                &published_records,
                &skipped_records,
                &output_plan.staged_to_final_segment_id,
                self.index
                    .get_current_epoch()?
                    .ok_or(Error::EpochNotInitialized)?,
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

            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
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
                    reclaim_publish_lsn,
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
                    reclaim_publish_lsn,
                    *output_bytes,
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

            let next_lsn = published_records
                .last()
                .and_then(|record| record.publish_lsn.checked_add(1))
                .ok_or(strata_segment::Error::RangeOverflow)?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            self.index
                .put_published_lsn_batch(&mut batch, last_publish_lsn)?;
            self.index
                .put_store_checkpoint_batch(&mut batch, store_checkpoint)?;
            if let Err(error) = batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)
                .map_err(Error::from)
            {
                return Err(GcPublishCommitError::IndexCommit(error));
            }
            Ok::<(), GcPublishCommitError>(())
        })();

        match commit_result {
            Ok(()) => {
                self.last_durability_publish_at = Instant::now();
                if let Err(error) = self.lsm.materialize_through(last_publish_lsn, |edit| {
                    publish_blob_lsm_edit(&self.index, edit)
                }) {
                    let error = Error::from(error);
                    self.halt_writer_error("GC metadata materialization", &error);
                    return Err(error);
                }
                if rolled_relocation_memtable {
                    flush_relocation_lsm(
                        &self.index,
                        &self.relocations,
                        &self.relocation_cache,
                        &self.metrics,
                    )?;
                }
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
                self.metrics
                    .record_gc_output_published(published_output_bytes);
                self.run_rollover_post_commit(pending_rollovers);
                let next_lsn = published_records
                    .last()
                    .and_then(|record| record.publish_lsn.checked_add(1))
                    .expect("published records are non-empty and checked above");
                self.metrics.set_next_lsn(next_lsn);
                self.metrics.set_published_lsn(last_publish_lsn);
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
                self.restore_pending_rollovers(pending_rollovers);
                self.halt_writer_error("gc publish before index batch", &error);
                Err(error)
            }
            Err(GcPublishCommitError::IndexCommit(error)) => {
                self.halt_writer_error("gc publish index batch commit", &error);
                Err(error)
            }
        }
    }

    fn apply_gc_metadata_action(&self, action: &GcAction) -> Result<()> {
        match action {
            GcAction::DeleteSegment { segment_id } => {
                self.delete_empty_gc_segments(&[*segment_id])?;
            }
            GcAction::DeleteSegments { segment_ids } => {
                self.delete_empty_gc_segments(segment_ids)?;
            }
            GcAction::ReclassifySegment {
                segment_id,
                placement_class,
            } => {
                self.reclassify_gc_segment(*segment_id, *placement_class)?;
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {
                return Err(Error::GcInvalidPlan(
                    "copy action must use the copy publish path",
                ));
            }
        }
        Ok(())
    }

    /// Fences every source that published at least one surviving relocation.
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

    fn delete_empty_gc_segments(&self, segment_ids: &[SegmentId]) -> Result<()> {
        let mut states = Vec::with_capacity(segment_ids.len());
        let mut states_to_commit = Vec::new();
        let mut summaries_to_remove = Vec::new();
        let mut relocating_segments_to_remove = 0;
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
            if summary.live_ref_count != 0 {
                return Err(Error::GcSourceSegmentNotEmpty {
                    segment_id: *segment_id,
                    live_ref_count: summary.live_ref_count,
                });
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
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
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
            let output_bytes_by_source = self
                .index
                .remove_gc_reclaim_pending_for_sources_batch(&mut batch, &source_segment_ids)?;
            batch.write().map_err(strata_index::Error::from)?;
            for (segment_id, source_bytes) in unlinked_segments {
                let output_bytes = output_bytes_by_source
                    .get(&segment_id)
                    .copied()
                    .unwrap_or(0);
                self.metrics
                    .record_gc_source_deleted(source_bytes, output_bytes);
            }
        }
        Ok(())
    }

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
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        Ok(())
    }

    /// Selects the prepublished output segments that still contain surviving copied records.
    ///
    /// Only outputs that contain surviving copied records are finalized as `Sealed`. The returned
    /// map translates temporary staged segment ids to final durable segment ids.
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
