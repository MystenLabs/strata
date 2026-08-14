//! Store-local GC execution: planning snapshots, staging copies, prepublication, and cleanup.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use strata_core::{
    BlobLifecycle, DecodedRecord, FIXED_RECORD_HEADER_LEN, PlacementClass, RecordRef,
    SegmentFileState, SegmentGcOverlay, SegmentGcRecordRange, SegmentGcSummary, SegmentId,
    SegmentState, ShardCleanupState, ShardKey,
};
use strata_gc::{DestinationClass, GcAction, GcCopyRecord, GcCopySelector, GcPlan, GcPlanner};
use strata_index::StrataIndex;
use strata_segment::{SegmentIoObserver, SegmentWriter};

use super::{
    GcExecutor, GcPrepublishedCopy, GcPrepublishedOutputSegment, GcPublishResult,
    GcPublishedOutputSegment, GcStagedCopiedRecord, GcStagedOutputSegment, PreparedGcCopy,
    PreparedGcPlan,
};
use crate::{
    Error, GcIoLimiter, Result,
    layout::{retention_segment_path, segment_path, segment_state_path},
    prune_empty_retention_dirs,
    seal::sha256_file_prefix,
    segment_garbage_log_path,
    shard_gc::{remove_shard_retention_generation, shard_generation_is_obsolete},
    sync_parent_dir,
};

impl GcExecutor {
    /// Runs one complete GC attempt: prepare, copy, then publish.
    ///
    /// `Ok(None)` means no eligible plan was admitted or selected. Errors are operational failures
    /// from file I/O, index access, or writer publication.
    pub(crate) fn run_once(&self, planner: &GcPlanner) -> Result<Option<GcPublishResult>> {
        self.gc_io_limiter
            .set_bytes_per_sec(self.gc_concurrency.active_io_bytes_per_sec());
        let Some(prepared) = self.prepare_gc_plan(planner)? else {
            return Ok(None);
        };
        let copy = self.copy_prepared_gc_plan(prepared)?;
        self.publish_prepared_gc_copy(copy).map(Some)
    }

    /// Deletes every segment of a dropped shard generation as one unit, no per-record accounting.
    ///
    /// When a shard is dropped, the writer publishes a generation fence and a cleanup job in
    /// state ReadyForGc (see mark_shard_dropped). From that moment new plans no longer see the
    /// generation's segments, but the files are still on disk. This function is the other end:
    /// it erases the generation's metadata and its whole retention directory in one sweep —
    /// that wholesale erasure is the entire point of the fence, and why none of these segments
    /// need their bytes individually retired through the garbage log.
    ///
    /// The gates, in code order:
    /// - The global garbage log must be fully swept (head == sweep cursor). Events still in the
    ///   log may reference segments this cleanup is about to erase; draining first means the
    ///   sweeper never has to resolve an event against metadata that no longer exists.
    /// - The job's drop_lsn must be at or below published_lsn. The drop itself has to be durable
    ///   before the files vanish — a crash must not resurrect a generation whose directory is
    ///   already gone.
    /// - Every owned segment must be claimable (claim_when_available). This is the drain: the
    ///   `draining` reservation blocks *new* GC claims on these segments immediately, while
    ///   in-flight jobs that claimed them from an older snapshot are allowed to finish. A timeout
    ///   skips the job for this wake rather than stalling the worker; the next wake retries.
    /// - Both the publish/cleanup lock and the durability-publication lock are held for the
    ///   removal itself: the first keeps GC prepublish from renaming a new output file into the
    ///   directory being removed, the second keeps garbage publication and sweeping from running
    ///   mid-erasure. The job is re-read under the locks — another worker may have finished it.
    /// - Any GcRelocating segment in the generation must have its relocation activation durable
    ///   (relocation_activation_is_durable), the same rule delete_empty_gc_segments applies: a
    ///   source file must never disappear while the only pointers to its copied bytes could
    ///   still be lost to a crash.
    ///
    /// Then the removal: aggregate the segments' GC summaries (to retire them from the
    /// known-garbage metrics in one subtraction), evict reader-cache entries, remove the
    /// generation's keyed metadata rows in one synced batch, delete the retention directory tree,
    /// and finally flip the job to ShardOwnedReclaimed in its own synced batch. Blob rows from
    /// mixed ingest segments that referenced this generation are not handled here — ordinary
    /// blob-LSM compaction fences those against the shard registry as it encounters them.
    pub(crate) fn cleanup_ready_shard_generations(&self) -> Result<usize> {
        let garbage_head = self
            .index
            .get_garbage_log_position(crate::GARBAGE_LOG_HEAD)?
            .unwrap_or_default();
        let garbage_swept = self
            .index
            .get_garbage_log_position(crate::GARBAGE_LOG_SWEEP_CURSOR)?
            .unwrap_or_default();
        if garbage_head != garbage_swept {
            return Ok(0);
        }
        let published_lsn = self.index.get_committed_lsn()?;
        let jobs = self.index.iter_shard_cleanup_jobs()?;
        let mut cleaned = 0;
        for job in jobs.into_iter().filter(|job| {
            job.state == ShardCleanupState::ReadyForGc && job.drop_lsn <= published_lsn
        }) {
            let owned_segments = self
                .index
                .iter_segment_states_for_shard(job.shard)?
                .into_iter()
                .map(|(segment_id, _)| segment_id)
                .collect::<BTreeSet<_>>();
            let Some(_claim) = self.claims.claim_when_available(
                owned_segments.clone(),
                self.config.shard_drop_gc_drain_timeout,
            ) else {
                continue;
            };
            let _publish_cleanup_guard = self
                .publish_cleanup_lock
                .lock()
                .expect("gc publish/cleanup lock poisoned");
            let _durability_publish_guard = self
                .durability_publish_lock
                .lock()
                .expect("durability publication lock poisoned");
            let Some(current_job) = self.index.get_shard_cleanup_job(job.shard)? else {
                continue;
            };
            if current_job.state != ShardCleanupState::ReadyForGc {
                continue;
            }
            let mut activations_durable = true;
            for segment_id in &owned_segments {
                if self
                    .index
                    .get_segment_state(*segment_id)?
                    .is_some_and(|state| state.state == SegmentFileState::GcRelocating)
                    && !self.relocation_activation_is_durable(*segment_id)?
                {
                    activations_durable = false;
                    break;
                }
            }
            if !activations_durable {
                continue;
            }
            let mut removed_summary = SegmentGcSummary::default();
            let mut removed_relocating_segments = 0;
            for segment_id in &owned_segments {
                if self
                    .index
                    .get_segment_state(*segment_id)?
                    .is_some_and(|state| state.state == SegmentFileState::GcRelocating)
                {
                    removed_relocating_segments += 1;
                }
                if let Some(summary) = self.index.get_segment_gc_summary(*segment_id)? {
                    removed_summary.total_bytes = removed_summary
                        .total_bytes
                        .saturating_add(summary.total_bytes);
                    removed_summary.live_bytes = removed_summary
                        .live_bytes
                        .saturating_add(summary.live_bytes);
                    removed_summary.retired_bytes = removed_summary
                        .retired_bytes
                        .saturating_add(summary.retired_bytes);
                    removed_summary.expired_bytes = removed_summary
                        .expired_bytes
                        .saturating_add(summary.expired_bytes);
                    removed_summary.live_ref_count = removed_summary
                        .live_ref_count
                        .saturating_add(summary.live_ref_count);
                }
                self.reader_cache.evict(*segment_id);
                self.metrics.record_reader_cache_eviction();
            }
            remove_shard_retention_generation(&self.config, &self.index, job.shard)?;
            self.metrics.remove_gc_known_summary(&removed_summary);
            self.metrics
                .remove_gc_relocating_segments(removed_relocating_segments);
            let mut batch = self.index.batch();
            let mut completed = current_job;
            completed.state = ShardCleanupState::ShardOwnedReclaimed;
            self.index
                .put_shard_cleanup_job_batch(&mut batch, completed)?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
            cleaned += 1;
        }
        Ok(cleaned)
    }

    /// Prepares one GC plan from published summaries and segment-local garbage logs.
    ///
    /// Stage one of the pipeline. build_gc_snapshot pins one RocksDB snapshot and reads every
    /// GC-facing row from it (None means no current epoch is published yet, so expiry-sensitive
    /// planning cannot run). Claimed and draining segments are marked so the pure planner skips
    /// them instead of proposing work that would immediately fail to claim. The planner then
    /// yields plans in ranked order and this function takes the first one whose sources it can
    /// claim — all-or-nothing, so losing a race for even one source (another worker claimed it
    /// between the mark and the try) just means falling through to the next-best plan.
    ///
    /// For a copy plan the sources' garbage overlays are loaded now; the copy stage classifies
    /// every record against them. A source that vanished by this point is reachable through one
    /// race — a competing worker's delete plan finished after this snapshot was built but before
    /// this claim was taken — and surfaces as an error: the attempt aborts cleanly (nothing has
    /// been written), the worker logs it and backs off, and the next attempt plans from a fresh
    /// snapshot that no longer lists the segment.
    pub(crate) fn prepare_gc_plan(&self, planner: &GcPlanner) -> Result<Option<PreparedGcPlan>> {
        let Some(mut snapshot) = self.index.build_gc_snapshot()? else {
            return Ok(None);
        };
        self.claims.mark_snapshot(&mut snapshot);
        for plan in planner.plans(&snapshot) {
            let source_segments = gc_plan_source_segment_ids(&plan);
            let Some(claim) = self.claims.try_claim(source_segments) else {
                continue;
            };
            let source_overlays = copy_source_segment_ids(&plan)
                .into_iter()
                .map(|segment_id| {
                    self.index
                        .read_segment_garbage_overlay(self.config.namespace_dir(), segment_id)?
                        .map(|overlay| (segment_id, overlay))
                        .ok_or_else(|| Error::InvariantViolation {
                            reason: format!("GC selected missing source segment {segment_id}"),
                        })
                })
                .collect::<Result<BTreeMap<_, _>>>()?;

            return Ok(Some(PreparedGcPlan {
                plan,
                source_overlays,
                claim: Some(claim),
            }));
        }
        Ok(None)
    }

    /// Copies selected GC records into sealed staging files.
    ///
    /// This consumes a `PreparedGcPlan` and preserves its source claims through publication.
    ///
    /// Stage two. Metadata-only plans (delete/reclassify) pass straight through with no outputs —
    /// they have nothing to copy and publication routes them to their own handlers. Copy plans
    /// get a fresh per-attempt staging directory (named by pid, wall-clock seed, and a retry
    /// counter; deliberately absent from all durable metadata, so a crashed attempt's directory
    /// is inert garbage any later cleanup can discard). If the copy fails at any point the whole
    /// directory is removed best-effort and the error surfaces; nothing else has happened yet.
    /// In the example: this is where A, B, C, D are read out of S7 and written into T900 — all
    /// four, because the overlay this attempt planned from still called all four live.
    pub fn copy_prepared_gc_plan(&self, prepared: PreparedGcPlan) -> Result<PreparedGcCopy> {
        let PreparedGcPlan {
            plan,
            source_overlays,
            claim,
        } = prepared;
        if !plan_has_copy_action(&plan) {
            return Ok(PreparedGcCopy {
                plan,
                outputs: Vec::new(),
                copied_records: Vec::new(),
                claim,
            });
        }

        let staging_dir = create_gc_staging_dir(&self.config)?;
        let copy_result = self.copy_gc_plan_to_staging(&staging_dir, &plan, &source_overlays);
        let (outputs, copied_records) = match copy_result {
            Ok(copy) => copy,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging_dir);
                return Err(error);
            }
        };

        Ok(PreparedGcCopy {
            plan,
            outputs,
            copied_records,
            claim,
        })
    }

    /// Publishes staged GC copies through the independent relocation path.
    ///
    /// Output files are first renamed into final segment paths and protected by pending segment
    /// rows before GC reconciles copied refs and activates a durable relocation L0.
    ///
    /// Stages three and four under one umbrella, with the cleanup lock held across both (the
    /// inline comment below explains the shard-drop ordering it buys). prepublish_gc_outputs
    /// turns T900 into S42-on-disk with a PendingGcOutput row; submit_gc_publish (`publish.rs`)
    /// does the reconciliation and activation. Then the two endings:
    ///
    /// On success, some prepublished outputs may still have published nothing — every record they
    /// held went stale during reconciliation. Publication already wrote their Deleted rows; this
    /// function removes their files and prunes newly empty retention directories, so the
    /// filesystem catches up with the metadata.
    ///
    /// On failure, the response depends on whether the store halted. A clean failure (a
    /// revalidation error, an invalid plan) abandons the prepublished outputs: their rows flip to
    /// Deleted in one synced batch and their files are removed — the attempt leaves nothing
    /// behind but its claim, which the guard releases on drop. But if the store halted mid-
    /// publish, nothing is touched: the on-disk state is exactly the evidence recovery will use,
    /// and "cleaning up" here could destroy files a half-committed activation still references.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        // Keep shard cleanup ordered with every phase that can create or remove a retention path.
        // The writer may fence a shard while this guard is held; drop cleanup waits until publish
        // has reconciled that fence and removed any now-unpublished output.
        let _publish_cleanup_guard = self
            .publish_cleanup_lock
            .lock()
            .expect("gc publish/cleanup lock poisoned");
        let copy = self.prepublish_gc_outputs(copy)?;
        let prepublished_outputs = copy.outputs.clone();
        let result = self.publish_prepublished_gc_copy(copy);

        match result {
            Ok(result) => {
                remove_unpublished_prepublished_outputs(
                    &self.config,
                    &prepublished_outputs,
                    &result.output_segments,
                )?;
                Ok(result)
            }
            Err(error) => {
                if self.store_halt.error().is_none() {
                    let _ = abandon_prepublished_outputs(
                        &self.config,
                        &self.index,
                        &prepublished_outputs,
                    );
                }
                Err(error)
            }
        }
    }

    fn publish_prepublished_gc_copy(&self, publish: GcPrepublishedCopy) -> Result<GcPublishResult> {
        self.store_halt.check()?;
        self.submit_gc_publish(publish)
    }

    /// Filters out outputs whose shard died mid-copy, then promotes the rest to prepublished.
    ///
    /// A shard drop can land while the copy stage was reading S7 — the executor holds no lock
    /// against the writer lane. Outputs belonging to a now-obsolete generation are removed here,
    /// files and all, before they ever receive durable ids: publishing them would recreate
    /// retention paths for a generation whose directory teardown may already be queued. Their
    /// copied records stay in the bundle and are harmless — reconciliation skips them via the
    /// same shard-generation fence, so no survivor will ever reference the missing output. Any
    /// error in the middle removes every original staging file, obsolete or not.
    fn prepublish_gc_outputs(&self, copy: PreparedGcCopy) -> Result<GcPrepublishedCopy> {
        let PreparedGcCopy {
            plan,
            outputs,
            copied_records,
            claim,
        } = copy;
        let original_outputs = outputs.clone();
        let outputs = (|| {
            let mut publishable = Vec::with_capacity(outputs.len());
            let mut obsolete = Vec::new();
            for output in outputs {
                if shard_generation_is_obsolete(&self.index, output.shard)? {
                    obsolete.push(output);
                } else {
                    publishable.push(output);
                }
            }
            remove_gc_staging_output_files(&obsolete)?;
            self.prepublish_gc_output_segments(publishable)
        })();
        let outputs = match outputs {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = remove_gc_staging_output_files(&original_outputs);
                return Err(error);
            }
        };
        Ok(GcPrepublishedCopy {
            plan,
            outputs,
            copied_records,
            _claim: claim,
        })
    }

    /// Gives each staging file a durable identity: a real segment id, its final retention path,
    /// and a PendingGcOutput row.
    ///
    /// For each output, in order: allocate the next id from the shared monotonic allocator (this
    /// is where T900 becomes S42); build the retention path from shard, placement class, and id,
    /// creating directories as needed; then handle the one legal collision — a file already at
    /// that path with *no* index row is debris from a crash after rename but before the batch
    /// below, safe to remove and overwrite, while a file with a row means the allocator handed
    /// out a live id and the attempt aborts (GcOutputSegmentExists) rather than clobber it.
    /// Rename the staging file into place and fsync both parent directories so the rename itself
    /// survives a crash.
    ///
    /// Only after every file is in place does one synced batch write all the PendingGcOutput
    /// rows. That row is the crash contract for everything that follows: from this moment until
    /// activation flips it to Sealed, startup treats the file as deletable leftovers
    /// (cleanup_pending_gc_outputs). The rows must not be written before the files exist — a row
    /// pointing at nothing would make startup's unlink a no-op while recovery still believes the
    /// id was consumed. On any error the whole batch of files — renamed and not-yet-renamed — is
    /// removed best-effort and the attempt aborts.
    fn prepublish_gc_output_segments(
        &self,
        outputs: Vec<GcStagedOutputSegment>,
    ) -> Result<Vec<GcPrepublishedOutputSegment>> {
        if outputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut prepublished = Vec::with_capacity(outputs.len());
        let result = (|| {
            for output in &outputs {
                let segment_id = self.segment_ids.allocate()?;
                let final_path = retention_segment_path(
                    &self.config,
                    output.shard,
                    output.placement_class,
                    segment_id,
                );
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).map_err(|source| Error::Io {
                        path: parent.to_path_buf(),
                        source,
                    })?;
                }
                if final_path.exists() {
                    if self.index.get_segment_state(segment_id)?.is_none() {
                        fs::remove_file(&final_path).map_err(|source| Error::Io {
                            path: final_path.clone(),
                            source,
                        })?;
                    } else {
                        return Err(Error::GcOutputSegmentExists {
                            segment_id,
                            path: final_path,
                        });
                    }
                }
                fs::rename(&output.path, &final_path).map_err(|source| Error::Io {
                    path: final_path.clone(),
                    source,
                })?;
                sync_parent_dir(&final_path)?;
                if output.path.parent() != final_path.parent() {
                    sync_parent_dir(&output.path)?;
                }
                prepublished.push(GcPrepublishedOutputSegment {
                    staged_segment_id: output.staged_segment_id,
                    segment_id,
                    shard: output.shard,
                    path: final_path,
                    placement_class: output.placement_class,
                    sealed_len: output.sealed_len,
                    sealed_sha256: output.sealed_sha256,
                });
            }

            let mut batch = self.index.batch();
            for output in &prepublished {
                self.index
                    .put_segment_state_batch(&mut batch, &output.pending_state(&self.config))?;
            }
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
            Ok::<_, Error>(())
        })();

        match result {
            Ok(()) => Ok(prepublished),
            Err(error) => {
                let _ = remove_gc_prepublished_output_files(&self.config, &prepublished);
                let _ = remove_gc_staging_output_files(&outputs);
                Err(error)
            }
        }
    }

    /// Copies selected records into temporary sealed segment files grouped by destination class.
    ///
    /// Staging output uses local segment ids starting at one. Publish later assigns durable segment
    /// ids and translates staged refs into final refs.
    ///
    /// The selector is the plan's routing table (which lifecycle buckets go to which destination
    /// class); the copier owns the staging files. After every source is scanned,
    /// validate_copied_bytes compares what was actually copied against what the plan predicted —
    /// a mismatch means the view the planner ranked this plan on has drifted too far to trust,
    /// and the attempt aborts before wasting prepublish and publication work on it.
    fn copy_gc_plan_to_staging(
        &self,
        staging_dir: &Path,
        plan: &GcPlan,
        source_overlays: &BTreeMap<SegmentId, SegmentGcOverlay>,
    ) -> Result<(Vec<GcStagedOutputSegment>, Vec<GcStagedCopiedRecord>)> {
        let selector = GcCopySelector::new(plan)?;
        let mut copier = GcStagingCopier::new(
            &self.gc_io_limiter,
            staging_dir,
            self.config.segment_max_bytes,
            Arc::new(self.metrics.clone()),
        );

        for segment_id in copy_source_segment_ids(plan) {
            let overlay =
                source_overlays
                    .get(&segment_id)
                    .ok_or_else(|| Error::InvariantViolation {
                        reason: format!(
                            "GC copy plan omitted snapshot overlay for source segment {segment_id}"
                        ),
                    })?;
            self.copy_gc_source_segment_to_staging(segment_id, overlay, &selector, &mut copier)?;
        }

        selector.validate_copied_bytes(copier.copied_bytes())?;
        copier.finish()
    }

    /// Scans one source segment and copies records selected by the aggregate plan routes.
    ///
    /// The scan is offset ordered, so overlay classification advances monotonically through the
    /// segment-local overlay. Expired and retired ranges are skipped. Copy eligible records are
    /// decoded only when their lifecycle bucket is named by the plan.
    ///
    /// In detail: the source must still be Sealed with a recorded sealed length, and the file on
    /// disk must be exactly that long — anything else is a corrupt or torn prefix and the copy
    /// aborts. Then a single forward walk over S7: read each fixed record header (every read
    /// charges the shared I/O limiter first, which is how the tuner's byte budget actually slows
    /// GC down), classify the record's range against the overlay. Dead ranges are the payoff of
    /// the ordered walk — B's bytes, had the overlay already known B was dead, would be skipped
    /// here without ever reading the body. A live record is routed by its lifecycle: if the plan
    /// does not name that bucket (a MoveEpochBytes plan moving only epoch-50 bytes leaves other
    /// lifecycles in place), it is skipped and simply stays in S7 for a later plan. Otherwise the
    /// body is read, decoded, and appended to the staging copier with its source ref, payload
    /// LSN, and refreshed lifecycle riding along for publication.
    fn copy_gc_source_segment_to_staging(
        &self,
        segment_id: SegmentId,
        overlay: &SegmentGcOverlay,
        selector: &GcCopySelector,
        copier: &mut GcStagingCopier<'_>,
    ) -> Result<()> {
        let state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        if state.state != SegmentFileState::Sealed {
            return Err(Error::GcSourceSegmentNotSealed {
                segment_id,
                state: state.state,
            });
        }
        let sealed_len = state
            .sealed_len
            .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
        let path = gc_source_segment_path(&self.config, &state);
        let file_len = fs::metadata(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?
            .len();
        if file_len != sealed_len {
            return Err(Error::GcSourceSegmentInvalidPrefix {
                segment_id,
                path,
                expected_len: sealed_len,
                valid_len: file_len,
            });
        }

        let mut file = File::open(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let mut classifier = OverlayRecordClassifier::new(segment_id, overlay);
        let mut offset = 0_u64;

        while offset < sealed_len {
            let remaining = sealed_len - offset;
            if remaining < FIXED_RECORD_HEADER_LEN as u64 {
                return Err(Error::GcSourceSegmentInvalidPrefix {
                    segment_id,
                    path,
                    expected_len: sealed_len,
                    valid_len: offset,
                });
            }

            file.seek(SeekFrom::Start(offset))
                .map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            let mut fixed = [0; FIXED_RECORD_HEADER_LEN];
            self.gc_io_limiter.acquire(FIXED_RECORD_HEADER_LEN as u64);
            file.read_exact(&mut fixed).map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
            self.metrics
                .record_segment_file_read(FIXED_RECORD_HEADER_LEN as u64);

            let header =
                DecodedRecord::peek_fixed_header(&fixed).map_err(strata_segment::Error::from)?;
            let record_len = header
                .encoded_record_len()
                .map_err(strata_segment::Error::from)?;
            if remaining < record_len {
                return Err(Error::GcSourceSegmentInvalidPrefix {
                    segment_id,
                    path,
                    expected_len: sealed_len,
                    valid_len: offset,
                });
            }

            let record_ref = RecordRef {
                segment_id,
                offset,
                len: record_len,
            };
            let range = SegmentGcRecordRange::from(record_ref);
            let lifecycle = match classifier.classify(range)? {
                OverlayRecordState::Retired | OverlayRecordState::Expired => {
                    offset = offset
                        .checked_add(record_len)
                        .ok_or(strata_segment::Error::RangeOverflow)?;
                    continue;
                }
                OverlayRecordState::CopyEligible { lifecycle } => lifecycle,
            };

            let Some(destination_class) = selector.destination_for(segment_id, lifecycle) else {
                offset = offset
                    .checked_add(record_len)
                    .ok_or(strata_segment::Error::RangeOverflow)?;
                continue;
            };

            let payload_len = usize::try_from(header.payload_len)
                .map_err(|_| strata_segment::Error::RangeOverflow)?;
            let key_len = header.key_len as usize;
            let body_len = payload_len
                .checked_add(key_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            let mut body = vec![0; body_len];
            self.gc_io_limiter.acquire(body_len as u64);
            if !body.is_empty() {
                file.read_exact(&mut body).map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
                self.metrics.record_segment_file_read(body.len() as u64);
            }
            let key = body.split_off(payload_len);
            let payload = body;
            let decoded = DecodedRecord::from_parts(header, &fixed, payload, key)
                .map_err(strata_segment::Error::from)?;
            let payload = decoded.payload;
            let record = GcCopyRecord {
                key: decoded.key,
                shard: decoded.header.shard,
                payload_lsn: decoded.header.generation,
                from: record_ref,
                lifecycle,
                destination_class,
            };
            copier.append(record, &payload)?;

            offset = offset
                .checked_add(record_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
        }

        Ok(())
    }
}

/// Open GC staging state shared across source segment scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct DestinationPlacement {
    shard: ShardKey,
    class: DestinationClass,
}

/// Owns the staging files for one copy attempt and routes each record to the right one.
///
/// Records are grouped by (shard, destination class): one open file per group, so a plan that
/// routes epoch-50 bytes and spillover bytes out of the same source produces two staging files.
/// When an open file fills up it is sealed and replaced, so one logical group can span several
/// staged files. Staged ids are process-local (1, 2, 3, ...) and mean nothing outside this
/// attempt; prepublish maps them to durable ids later. `copied_bytes` counts *source* bytes so
/// the plan's prediction can be audited by validate_copied_bytes.
struct GcStagingCopier<'a> {
    io_limiter: &'a GcIoLimiter,
    io_observer: Arc<dyn SegmentIoObserver>,
    staging_dir: &'a Path,
    segment_max_bytes: u64,
    outputs: Vec<GcStagedOutputSegment>,
    open_outputs: BTreeMap<DestinationPlacement, OpenStagedOutput>,
    copied_records: Vec<GcStagedCopiedRecord>,
    copied_bytes: u64,
    next_staged_segment_id: SegmentId,
}

impl<'a> GcStagingCopier<'a> {
    fn new(
        io_limiter: &'a GcIoLimiter,
        staging_dir: &'a Path,
        segment_max_bytes: u64,
        io_observer: Arc<dyn SegmentIoObserver>,
    ) -> Self {
        Self {
            io_limiter,
            io_observer,
            staging_dir,
            segment_max_bytes,
            outputs: Vec::new(),
            open_outputs: BTreeMap::new(),
            copied_records: Vec::new(),
            copied_bytes: 0,
            next_staged_segment_id: 1,
        }
    }

    fn copied_bytes(&self) -> u64 {
        self.copied_bytes
    }

    fn append(&mut self, record: GcCopyRecord, payload: &[u8]) -> Result<()> {
        let destination = DestinationPlacement {
            shard: record.shard,
            class: record.destination_class,
        };
        if !self.open_outputs.contains_key(&destination) {
            let output = create_staged_output(
                self.staging_dir,
                self.next_staged_segment_id,
                destination,
                self.segment_max_bytes,
                Arc::clone(&self.io_observer),
            )?;
            self.next_staged_segment_id = output.next_staged_segment_id;
            self.open_outputs.insert(destination, output);
        }

        let output = self
            .open_outputs
            .get_mut(&destination)
            .expect("staged output inserted above");
        let mut append_context = GcStagedOutputAppendContext {
            io_limiter: self.io_limiter,
            finished_outputs: &mut self.outputs,
            staging_dir: self.staging_dir,
            next_staged_segment_id: &mut self.next_staged_segment_id,
            segment_max_bytes: self.segment_max_bytes,
            io_observer: Arc::clone(&self.io_observer),
        };
        let staged =
            append_gc_record_to_staged_output(&mut append_context, output, &record, payload)?;
        self.copied_bytes = self
            .copied_bytes
            .checked_add(record.from.len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        self.copied_records.push(GcStagedCopiedRecord {
            source: record,
            staged,
        });
        Ok(())
    }

    /// Seals every still-open staging file (computing the digest publication will record) and
    /// returns outputs and records in deterministic order — outputs by staged id, records by
    /// source position — so every attempt over the same inputs hands publication the same shape.
    fn finish(mut self) -> Result<(Vec<GcStagedOutputSegment>, Vec<GcStagedCopiedRecord>)> {
        for (_, output) in self.open_outputs {
            self.outputs.push(output.finish(self.io_limiter)?);
        }
        self.outputs.sort_by_key(|output| output.staged_segment_id);
        self.copied_records
            .sort_by_key(|record| (record.source.from.segment_id, record.source.from.offset));
        Ok((self.outputs, self.copied_records))
    }
}

/// Writable GC output segment that has not been sealed yet.
#[derive(Debug)]
struct OpenStagedOutput {
    /// Segment writer for the temporary staging file.
    writer: SegmentWriter,
    io_observer: Arc<dyn SegmentIoObserver>,
    /// Logical shard and destination class this output accepts.
    destination: DestinationPlacement,
    /// Final placement class to record if this output is published.
    placement_class: PlacementClass,
    /// Next local staging id to allocate after this output.
    next_staged_segment_id: SegmentId,
}

impl OpenStagedOutput {
    /// Seals the temporary file and records the digest needed for final segment metadata.
    fn finish(mut self, io_limiter: &GcIoLimiter) -> Result<GcStagedOutputSegment> {
        let sealed_len = self.writer.seal()?;
        let path = self.writer.path().to_path_buf();
        io_limiter.acquire(sealed_len);
        let sealed_sha256 = sha256_file_prefix(&path, sealed_len)?;
        self.io_observer.record_read(sealed_len);
        Ok(GcStagedOutputSegment {
            staged_segment_id: self.writer.segment_id(),
            shard: self.destination.shard,
            destination_class: self.destination.class,
            placement_class: self.placement_class,
            path,
            sealed_len,
            sealed_sha256,
        })
    }
}

/// Creates one open staging segment for a destination class.
fn create_staged_output(
    staging_dir: &std::path::Path,
    staged_segment_id: SegmentId,
    destination: DestinationPlacement,
    segment_max_bytes: u64,
    io_observer: Arc<dyn SegmentIoObserver>,
) -> Result<OpenStagedOutput> {
    let placement_class = placement_class_for_destination(destination.class);
    let path = staging_dir.join(format!("{staged_segment_id:012}.data"));
    let writer = SegmentWriter::create_with_io_observer(
        &path,
        staged_segment_id,
        placement_class,
        segment_max_bytes,
        Arc::clone(&io_observer),
    )?;
    Ok(OpenStagedOutput {
        writer,
        io_observer,
        destination,
        placement_class,
        next_staged_segment_id: staged_segment_id.saturating_add(1),
    })
}

/// Appends one copied record to an open staging output.
///
/// If the current output is full, it is sealed and pushed into `finished_outputs`, then a
/// replacement output for the same destination class is opened before retrying the append.
struct GcStagedOutputAppendContext<'a> {
    io_limiter: &'a GcIoLimiter,
    finished_outputs: &'a mut Vec<GcStagedOutputSegment>,
    staging_dir: &'a Path,
    next_staged_segment_id: &'a mut SegmentId,
    segment_max_bytes: u64,
    io_observer: Arc<dyn SegmentIoObserver>,
}

fn append_gc_record_to_staged_output(
    context: &mut GcStagedOutputAppendContext<'_>,
    output: &mut OpenStagedOutput,
    record: &GcCopyRecord,
    payload: &[u8],
) -> Result<RecordRef> {
    context.io_limiter.acquire(record.from.len);
    match output
        .writer
        .append_for_shard(&record.key, record.payload_lsn, record.shard, payload)
    {
        Ok(outcome) => Ok(outcome.record_ref),
        Err(strata_segment::Error::SegmentFull { .. }) => {
            let replacement = create_staged_output(
                context.staging_dir,
                *context.next_staged_segment_id,
                output.destination,
                context.segment_max_bytes,
                Arc::clone(&context.io_observer),
            )?;
            let finished = std::mem::replace(output, replacement).finish(context.io_limiter)?;
            context.finished_outputs.push(finished);
            *context.next_staged_segment_id = output.next_staged_segment_id;
            let outcome = output.writer.append_for_shard(
                &record.key,
                record.payload_lsn,
                record.shard,
                payload,
            )?;
            Ok(outcome.record_ref)
        }
        Err(error) => Err(error.into()),
    }
}

/// Allocates a unique per-attempt staging directory under the store namespace.
///
/// The directory name includes process id, wall-clock seed, and retry counter. It is intentionally
/// not durable metadata; failed copy attempts remove it best-effort, and recovery can discard stale
/// staging directories because they are not referenced by segment state.
fn create_gc_staging_dir(config: &crate::StrataStoreConfig) -> Result<PathBuf> {
    let root = config.namespace_dir().join("gc-staging");
    fs::create_dir_all(&root).map_err(|source| Error::Io {
        path: root.clone(),
        source,
    })?;
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    for attempt in 0..1024_u64 {
        let path = root.join(format!("{}-{}-{attempt}", std::process::id(), seed));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(Error::Io { path, source }),
        }
    }
    Err(Error::InvalidConfig(
        "could not allocate gc staging directory",
    ))
}

/// After a successful publish, unlinks the prepublished outputs that published nothing.
///
/// Publication only finalizes outputs holding at least one surviving record; an output whose
/// every copy went stale got a Deleted row instead. This removes those files (and their sidecar
/// garbage logs) and prunes any retention directories the removal emptied. Metadata first, files
/// second — the reverse order could unlink a file whose row still says PendingGcOutput.
fn remove_unpublished_prepublished_outputs(
    config: &crate::StrataStoreConfig,
    prepublished: &[GcPrepublishedOutputSegment],
    published: &[GcPublishedOutputSegment],
) -> Result<()> {
    let published_ids = published
        .iter()
        .map(|output| output.segment_id)
        .collect::<BTreeSet<_>>();
    let unpublished = prepublished
        .iter()
        .filter(|output| !published_ids.contains(&output.segment_id))
        .cloned()
        .collect::<Vec<_>>();
    if unpublished.is_empty() {
        return Ok(());
    }
    remove_gc_prepublished_output_files(config, &unpublished)
}

/// After a failed (but not halted) publish, retires every prepublished output.
///
/// The rows flip PendingGcOutput → Deleted in one synced batch, then the files are removed. The
/// synced flip matters: once the files are gone, no durable state may still describe them as a
/// pending publication waiting to be recovered. The caller skips this entirely when the store
/// halted — see publish_prepared_gc_copy for why a halted store's files must be left alone.
fn abandon_prepublished_outputs(
    config: &crate::StrataStoreConfig,
    index: &StrataIndex,
    outputs: &[GcPrepublishedOutputSegment],
) -> Result<()> {
    if outputs.is_empty() {
        return Ok(());
    }

    let mut batch = index.batch();
    for output in outputs {
        index.put_segment_state_batch(&mut batch, &output.deleted_state(config))?;
    }
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    remove_gc_prepublished_output_files(config, outputs)
}

fn remove_gc_staging_output_files(outputs: &[GcStagedOutputSegment]) -> Result<()> {
    for output in outputs {
        remove_gc_output_file(&output.path)?;
    }
    Ok(())
}

fn remove_gc_prepublished_output_files(
    config: &crate::StrataStoreConfig,
    outputs: &[GcPrepublishedOutputSegment],
) -> Result<()> {
    for output in outputs {
        remove_gc_output_file(&output.path)?;
        if let Some(parent) = output.path.parent() {
            prune_empty_retention_dirs(config, parent.to_path_buf())?;
        }
    }
    Ok(())
}

fn remove_gc_output_file(path: &std::path::Path) -> Result<()> {
    let mut removed = false;
    for path in [
        path.to_path_buf(),
        segment_garbage_log_path(path.to_path_buf()),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
    }
    if removed {
        sync_parent_dir(path)
    } else {
        Ok(())
    }
}

/// Converts a planner destination class into the physical placement class used by segment state.
fn placement_class_for_destination(destination_class: DestinationClass) -> PlacementClass {
    match destination_class {
        DestinationClass::ExactEpoch(epoch) => PlacementClass::ExactEpoch(epoch),
        DestinationClass::Spillover => PlacementClass::Spillover,
    }
}

/// Overlay-derived copy disposition for a single source record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverlayRecordState {
    /// The record is permanently retired and must not be copied.
    Retired,
    /// The record has an explicit terminal expiry event and must not be copied.
    Expired,
    /// The record is live enough to copy, with an optional known lifecycle.
    CopyEligible { lifecycle: Option<BlobLifecycle> },
}

/// Offset ordered classifier for records in one segment GC overlay.
///
/// Overlay ranges must either fully contain a record or not overlap it. Partial overlap is rejected
/// because GC only rewrites whole encoded records and cannot split payload liveness.
pub(crate) struct OverlayRecordClassifier<'a> {
    segment_id: SegmentId,
    overlay: &'a SegmentGcOverlay,
    expired_index: usize,
    retired_index: usize,
    lifetime_index: usize,
}

impl<'a> OverlayRecordClassifier<'a> {
    pub(crate) fn new(segment_id: SegmentId, overlay: &'a SegmentGcOverlay) -> Self {
        Self {
            segment_id,
            overlay,
            expired_index: 0,
            retired_index: 0,
            lifetime_index: 0,
        }
    }

    pub(crate) fn classify(&mut self, record: SegmentGcRecordRange) -> Result<OverlayRecordState> {
        if classify_skipped_range(
            self.segment_id,
            &self.overlay.retired,
            &mut self.retired_index,
            record,
        )? {
            return Ok(OverlayRecordState::Retired);
        }
        if classify_skipped_range(
            self.segment_id,
            &self.overlay.expired,
            &mut self.expired_index,
            record,
        )? {
            return Ok(OverlayRecordState::Expired);
        }

        let lifecycle = self.lifecycle_for_record(record)?;
        Ok(OverlayRecordState::CopyEligible { lifecycle })
    }

    fn lifecycle_for_record(
        &mut self,
        record: SegmentGcRecordRange,
    ) -> Result<Option<BlobLifecycle>> {
        advance_range_cursor(
            &self.overlay.lifetimes,
            &mut self.lifetime_index,
            record.offset,
            |entry| entry.range,
        );
        let Some(lifetime) = self.overlay.lifetimes.get(self.lifetime_index) else {
            return Ok(None);
        };
        if range_contains(lifetime.range, record) {
            return Ok(Some(lifetime.lifecycle));
        }
        if ranges_overlap(lifetime.range, record) {
            return Err(partial_overlay_error(self.segment_id, record));
        }
        Ok(None)
    }
}

fn classify_skipped_range(
    segment_id: SegmentId,
    ranges: &[SegmentGcRecordRange],
    index: &mut usize,
    record: SegmentGcRecordRange,
) -> Result<bool> {
    advance_range_cursor(ranges, index, record.offset, |range| *range);
    let Some(range) = ranges.get(*index).copied() else {
        return Ok(false);
    };
    if range_contains(range, record) {
        return Ok(true);
    }
    if ranges_overlap(range, record) {
        return Err(partial_overlay_error(segment_id, record));
    }
    Ok(false)
}

fn advance_range_cursor<T>(
    ranges: &[T],
    index: &mut usize,
    record_offset: u64,
    range_for: impl Fn(&T) -> SegmentGcRecordRange,
) {
    while let Some(range) = ranges.get(*index).map(&range_for) {
        if range_end(range) > record_offset {
            break;
        }
        *index += 1;
    }
}

/// Builds the error returned when an overlay range cuts through a record boundary.
fn partial_overlay_error(segment_id: SegmentId, record: SegmentGcRecordRange) -> Error {
    Error::GcOverlayPartialRecordRange {
        segment_id,
        offset: record.offset,
        len: record.len,
    }
}

/// Returns true when `container` fully covers `contained`.
fn range_contains(container: SegmentGcRecordRange, contained: SegmentGcRecordRange) -> bool {
    container.offset <= contained.offset && range_end(container) >= range_end(contained)
}

/// Returns true when two half-open byte ranges overlap.
fn ranges_overlap(left: SegmentGcRecordRange, right: SegmentGcRecordRange) -> bool {
    left.offset < range_end(right) && right.offset < range_end(left)
}

/// Computes the exclusive end offset for a record range.
fn range_end(range: SegmentGcRecordRange) -> u64 {
    range.offset.saturating_add(range.len)
}

/// Resolves the on-disk path for a GC source segment.
///
/// Older or synthetic test states may not carry `SegmentState.path`; in that case the ingest layout
/// path is derived from the segment id.
fn gc_source_segment_path(
    config: &crate::StrataStoreConfig,
    state: &SegmentState,
) -> std::path::PathBuf {
    if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    }
}

/// Returns true if any plan action requires source record copying.
fn plan_has_copy_action(plan: &GcPlan) -> bool {
    matches!(
        &plan.action,
        GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. }
    )
}

/// Returns source segment ids that must be scanned and copied for a plan.
fn copy_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = BTreeSet::new();
    match &plan.action {
        GcAction::MoveLiveBytes {
            source_segment_id, ..
        } => {
            source_ids.insert(*source_segment_id);
        }
        GcAction::MoveEpochBytes { routes, .. } => {
            source_ids.extend(routes.iter().map(|route| route.source_segment_id));
        }
        GcAction::DeleteSegment { .. }
        | GcAction::DeleteSegments { .. }
        | GcAction::ReclassifySegment { .. } => {}
    }
    source_ids
}

/// Returns every source segment id a plan must claim before execution.
///
/// This includes metadata-only actions such as delete and reclassify, not just copy sources.
fn gc_plan_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = copy_source_segment_ids(plan);
    match &plan.action {
        GcAction::DeleteSegment { segment_id } => {
            source_ids.insert(*segment_id);
        }
        GcAction::DeleteSegments { segment_ids } => {
            source_ids.extend(segment_ids.iter().copied());
        }
        GcAction::ReclassifySegment { segment_id, .. } => {
            source_ids.insert(*segment_id);
        }
        GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
    }
    source_ids
}
