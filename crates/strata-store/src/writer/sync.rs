//! Asynchronous durability publication.
//!
//! The writer captures an immutable LSN/file boundary and queues the expensive file syncs, then
//! immediately resumes foreground commits. File-sync callbacks count into one shared barrier; the
//! last callback sends an internal writer command. The writer performs only the final state merge
//! and synced RocksDB publication, which prevents a background publisher from overwriting segment
//! state committed by newer writes.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use strata_core::StoreCheckpoint;

use crate::{
    DURABILITY_PUBLISH_INTERVAL, DURABILITY_PUBLISH_SEGMENT_BYTES, DURABILITY_PUBLISH_WAL_BYTES,
    DurabilityPublish, Error, PendingSyncRequest, Result, SegmentSync, StoreSyncProfile,
    SyncRequest, WriteCommand, WriteCoordinator, file_sync::FileSyncTask,
    maintenance::publish_blob_lsm_edit, profile_phase, publish_segment_allocation_baseline,
};

impl DurabilityPublish {
    /// Called once by every captured segment sync and once by the WAL sync. The last callback
    /// hands this same publication object back to the writer; no separate barrier/snapshot/ready
    /// types are needed.
    fn sync_finished(self: &Arc<Self>, result: Result<()>) {
        if let Err(error) = result {
            let mut final_result = self
                .file_sync_result
                .lock()
                .expect("durability file-sync result lock poisoned");
            if final_result.is_none() {
                *final_result = Some(Err(error));
            }
        }
        let previous = self.remaining_syncs.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "durability sync completed too often");
        if previous != 1 {
            return;
        }
        {
            let mut final_result = self
                .file_sync_result
                .lock()
                .expect("durability file-sync result lock poisoned");
            if final_result.is_none() {
                *final_result = Some(Ok(self.file_sync_started.elapsed()));
            }
        }
        let _ = self
            .ready_tx
            .send(WriteCommand::DurabilityReady(Arc::clone(self)));
    }
}

impl WriteCoordinator {
    /// Starts one durability publication without waiting for physical I/O.
    ///
    /// Capturing happens on the writer thread, so `target_lsn`, the WAL position, segment offsets,
    /// and allocation counters describe one coherent prefix. Later writes may append to the same
    /// active files; syncing more bytes than the captured offsets is harmless because the metadata
    /// publication claims only this snapshot.
    pub(crate) fn start_durability_publish(&mut self, force: bool) -> Result<Option<u64>> {
        if let Some(target) = self.durability_in_flight_lsn {
            return Ok(Some(target));
        }

        let target_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        let published_lsn = self.index.get_published_lsn()?;
        if published_lsn > target_lsn {
            return Err(Error::InvariantViolation {
                reason: format!("published LSN {published_lsn} follows committed LSN {target_lsn}"),
            });
        }
        if published_lsn == target_lsn && !force {
            self.last_durability_publish_at = Instant::now();
            return Ok(None);
        }

        let started = Instant::now();
        let mut segments = std::mem::take(&mut self.pending_segment_syncs);

        let active_segment_id = self.segment.segment_id();
        let active_segment_offset = self.segment.write_offset();
        let active_path = self.segment.path().to_path_buf();
        let active_file = self.segment.clone_file_for_sync()?;
        let active_completion = crate::file_sync::FileSyncCompletion::new();
        segments.push(SegmentSync {
            segment_id: active_segment_id,
            durable_offset: active_segment_offset,
            allocation_records: self.active_allocation_records,
            allocation_tracker: Arc::clone(&self.active_allocation_tracker),
            completion: active_completion.clone(),
        });
        let wal_position = self.wal.position();
        let wal_bytes = self.wal.pending_bytes();
        let segment_bytes = self.pending_segment_bytes;
        let remaining_syncs = segments.len() + 1;
        let publish = Arc::new(DurabilityPublish {
            target_lsn,
            wal_position,
            checkpoint_segment_id: active_segment_id,
            checkpoint_segment_offset: active_segment_offset,
            segments,
            wal_bytes,
            segment_bytes,
            started,
            file_sync_started: Instant::now(),
            remaining_syncs: std::sync::atomic::AtomicUsize::new(remaining_syncs),
            file_sync_result: std::sync::Mutex::new(None),
            ready_tx: self.internal_write_tx.clone(),
        });

        // Rolled segment syncs may already have completed. Their completion object delivers the
        // stored result immediately; the active segment and WAL callbacks below remain outstanding,
        // so the final notification still comes from a file-sync worker.
        for segment in &publish.segments {
            let publish = Arc::clone(&publish);
            segment
                .completion
                .notify(move |result| publish.sync_finished(result));
        }

        let wal_publish = Arc::clone(&publish);
        let queued_wal_position = self
            .wal
            .sync_with_notification(move |result| wal_publish.sync_finished(result))?;
        if queued_wal_position != wal_position {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "queued WAL position {queued_wal_position:?} changed from captured position {wal_position:?}"
                ),
            });
        }
        let worker_completion = active_completion;
        self.segment_sync_tx
            .send(FileSyncTask::new(active_path, active_file, move |result| {
                worker_completion.complete(result);
            }))
            .map_err(|_| Error::FileSyncQueueClosed)?;

        self.wal.mark_pending_bytes_captured();
        self.pending_segment_bytes = 0;
        self.oldest_unpublished_at = None;
        self.durability_in_flight_lsn = Some(target_lsn);
        self.metrics.set_durability_pending(0, 0, true);
        Ok(Some(target_lsn))
    }

    pub(crate) fn maybe_start_durability_publish(&mut self, force: bool) -> Result<bool> {
        if self.durability_in_flight_lsn.is_some() {
            return Ok(false);
        }
        let age_due = self
            .oldest_unpublished_at
            .is_some_and(|started| started.elapsed() >= DURABILITY_PUBLISH_INTERVAL);
        let pressure_due = self.wal.pending_bytes() >= DURABILITY_PUBLISH_WAL_BYTES
            || self.pending_segment_bytes >= DURABILITY_PUBLISH_SEGMENT_BYTES;
        if !force && !age_due && !pressure_due {
            return Ok(false);
        }
        Ok(self.start_durability_publish(force)?.is_some())
    }

    pub(crate) fn note_committed_write(&mut self, segment_bytes: u64) -> Result<()> {
        self.pending_segment_bytes = self.pending_segment_bytes.saturating_add(segment_bytes);
        self.oldest_unpublished_at.get_or_insert_with(Instant::now);
        self.metrics.set_durability_pending(
            self.wal.pending_bytes(),
            self.pending_segment_bytes,
            self.durability_in_flight_lsn.is_some(),
        );
        self.maybe_start_durability_publish(false)?;
        Ok(())
    }

    /// Finishes the metadata half of a completed file sync on the serialized writer thread.
    pub(crate) fn finish_durability_publish(
        &mut self,
        publish: Arc<DurabilityPublish>,
    ) -> Result<(u64, StoreSyncProfile)> {
        let expected = self.durability_in_flight_lsn.take();
        if expected != Some(publish.target_lsn) {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "durability completion for LSN {} does not match in-flight target {expected:?}",
                    publish.target_lsn
                ),
            });
        }

        let file_sync_result = publish
            .file_sync_result
            .lock()
            .expect("durability file-sync result lock poisoned")
            .take()
            .expect("ready durability publication has no file-sync result");
        let mut phases = StoreSyncProfile {
            segment_sync: file_sync_result?,
            ..StoreSyncProfile::default()
        };
        let publish_started = Instant::now();
        let _publish_guard = self
            .durability_publish_lock
            .lock()
            .expect("durability publish lock poisoned");
        // The relocation frontier must be sampled while holding the same lock used by GC
        // activation. The synced RocksDB write below then proves that every sampled manifest edit
        // and activation row reached disk together.
        let durable_relocation_lsn = self.relocations.lsm().last_lsn()?.unwrap_or_default();

        let current_published_lsn = self.index.get_published_lsn()?;
        if current_published_lsn > publish.target_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published LSN {current_published_lsn} follows completed durability target {}",
                    publish.target_lsn
                ),
            });
        }
        let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if publish.target_lsn > committed_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "durability target {} follows committed LSN {committed_lsn}",
                    publish.target_lsn
                ),
            });
        }

        let (batch, states, allocation_marks) = profile_phase(
            Some(&mut phases),
            |profile, elapsed| profile.published_lsn_compute += elapsed,
            || {
                let mut batch = self.index.batch();
                let mut states = Vec::with_capacity(publish.segments.len());
                let mut allocation_marks = Vec::with_capacity(publish.segments.len());
                for segment in &publish.segments {
                    let mut state = self
                        .index
                        .get_segment_state(segment.segment_id)?
                        .ok_or_else(|| Error::InvariantViolation {
                            reason: format!(
                                "durability target references missing segment {}",
                                segment.segment_id
                            ),
                        })?;
                    if state.write_offset < segment.durable_offset {
                        return Err(Error::InvariantViolation {
                            reason: format!(
                                "segment {} write offset {} precedes durability target {}",
                                segment.segment_id, state.write_offset, segment.durable_offset
                            ),
                        });
                    }
                    state.durable_offset = state.durable_offset.max(segment.durable_offset);
                    self.index.put_segment_state_batch(&mut batch, &state)?;

                    let allocation_records = segment
                        .allocation_tracker
                        .unpublished_records(segment.allocation_records)?;
                    publish_segment_allocation_baseline(
                        &self.index,
                        &mut batch,
                        segment.segment_id,
                        segment.durable_offset,
                        allocation_records,
                    )?;
                    allocation_marks.push((
                        Arc::clone(&segment.allocation_tracker),
                        segment.allocation_records,
                    ));
                    states.push(state);
                }
                self.index
                    .put_published_lsn_batch(&mut batch, publish.target_lsn)?;
                self.index.put_store_checkpoint_batch(
                    &mut batch,
                    StoreCheckpoint {
                        wal_position: publish.wal_position,
                        active_segment_id: publish.checkpoint_segment_id,
                        active_segment_offset: publish.checkpoint_segment_offset,
                    },
                )?;
                Ok::<_, Error>((batch, states, allocation_marks))
            },
        )?;

        profile_phase(
            Some(&mut phases),
            |profile, elapsed| profile.index_batch_commit += elapsed,
            || {
                batch
                    .write_with_sync(true)
                    .map_err(strata_index::Error::from)
            },
        )?;
        for (tracker, records) in allocation_marks {
            tracker.mark_published(records);
        }

        profile_phase(
            Some(&mut phases),
            |profile, elapsed| profile.state_update += elapsed,
            || {
                if let Some(state) = states
                    .iter()
                    .find(|state| state.segment_id == self.active_segment_state.segment_id)
                {
                    self.durable_offset = self.durable_offset.max(state.durable_offset);
                    self.active_segment_state.durable_offset = self.durable_offset;
                }
                self.durable_relocation_lsn
                    .fetch_max(durable_relocation_lsn, std::sync::atomic::Ordering::Release);
                self.last_durability_publish_at = Instant::now();
                self.metrics.set_active_segment(
                    self.active_segment_state.segment_id,
                    self.active_segment_state.write_offset,
                    self.durable_offset,
                );
                self.metrics.set_published_lsn(publish.target_lsn);
            },
        );
        drop(_publish_guard);

        self.reclaim_store_wal(publish.target_lsn)?;
        let elapsed = publish.started.elapsed();
        self.metrics.record_sync(Ok(publish.segment_bytes), elapsed);
        self.metrics.record_durability_wal_bytes(publish.wal_bytes);
        self.metrics.set_durability_pending(
            self.wal.pending_bytes(),
            self.pending_segment_bytes,
            false,
        );
        self.gc_concurrency
            .observe_sync(elapsed, publish.segment_bytes);
        self.request_lsm_compaction();
        phases.writer_total = publish_started.elapsed();
        Ok((publish.target_lsn, phases))
    }

    pub(crate) fn enqueue_sync_request(&mut self, request: SyncRequest) {
        let SyncRequest {
            response_tx,
            profile: profile_request,
        } = request;
        let started = Instant::now();
        let profile = profile_request.begin(started);
        let target_lsn = match self.index.get_next_lsn() {
            Ok(next_lsn) => next_lsn.saturating_sub(1),
            Err(error) => {
                let _ = response_tx.send(Err(error.into()));
                return;
            }
        };
        match self.index.get_published_lsn() {
            Ok(published) if published > target_lsn => {
                let _ = response_tx.send(Err(Error::InvariantViolation {
                    reason: format!("published LSN {published} follows committed LSN {target_lsn}"),
                }));
                return;
            }
            Ok(_) => {}
            Err(error) => {
                let _ = response_tx.send(Err(error.into()));
                return;
            }
        }
        let needs_follow_up = self.durability_in_flight_lsn.is_some();
        self.pending_sync_requests.push(PendingSyncRequest {
            target_lsn,
            needs_follow_up,
            response_tx,
            profile_request,
            profile,
            started,
        });
        if self.durability_in_flight_lsn.is_none()
            && let Err(error) = self.maybe_start_durability_publish(true)
        {
            self.halt_writer_error("start explicit durability publication", &error);
            self.fail_pending_sync_requests();
        }
    }

    pub(crate) fn complete_sync_requests(&mut self, published_lsn: u64, phases: &StoreSyncProfile) {
        let mut retained = Vec::new();
        for mut pending in std::mem::take(&mut self.pending_sync_requests) {
            if pending.target_lsn > published_lsn || pending.needs_follow_up {
                // The completed snapshot satisfies the ordering fence. Any request that arrived
                // after it was captured may attach to the follow-up publication started below.
                pending.needs_follow_up = false;
                retained.push(pending);
                continue;
            }
            if let Some(profile) = pending.profile.as_mut() {
                let queue_wait = profile.queue_wait;
                *profile = StoreSyncProfile {
                    queue_wait,
                    segment_sync: phases.segment_sync,
                    published_lsn_compute: phases.published_lsn_compute,
                    index_batch_commit: phases.index_batch_commit,
                    state_update: phases.state_update,
                    writer_total: pending.started.elapsed(),
                    ..StoreSyncProfile::default()
                };
            }
            let response_started = Instant::now();
            let _ = pending.response_tx.send(Ok(()));
            if let Some(mut profile) = pending.profile {
                profile.response_send = response_started.elapsed();
                pending.profile_request.send(profile);
            }
        }
        self.pending_sync_requests = retained;
    }

    pub(crate) fn fail_pending_sync_requests(&mut self) {
        let reason = self
            .store_halt
            .error()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "durability publication failed".to_owned());
        for pending in std::mem::take(&mut self.pending_sync_requests) {
            let _ = pending.response_tx.send(Err(Error::StoreHalted {
                reason: reason.clone(),
            }));
            if let Some(mut profile) = pending.profile {
                profile.writer_total = pending.started.elapsed();
                pending.profile_request.send(profile);
            }
        }
    }

    /// Advances the blob projection and reclaims complete WAL files no longer needed for replay.
    fn reclaim_store_wal(&mut self, published_lsn: u64) -> Result<()> {
        self.lsm.materialize_through(published_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        let reclaim_through = self.lsm.manifest().materialized_through.unwrap_or_default();
        if reclaim_through == 0 {
            return Ok(());
        }

        let retained_from = self.wal.retained_from_after(reclaim_through)?;
        let persisted = self.index.get_store_wal_retained_from()?;
        let current = persisted.unwrap_or(self.lsm.manifest().wal_retained_from);
        if retained_from > current || persisted.is_none() {
            let mut batch = self.index.batch();
            self.index
                .put_store_wal_retained_from_batch(&mut batch, retained_from.max(current))?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
        }
        self.wal.reclaim_through(reclaim_through)?;
        Ok(())
    }
}
