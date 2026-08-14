//! Asynchronous sync and commit workflow.
//!
//! The writer captures an immutable LSN and file boundary and queues the expensive file syncs, then
//! immediately resumes foreground commits. The last segment sync callback
//! queues the captured WAL sync, and WAL sync completion hands the snapshot back to the writer. The
//! writer performs only the final state merge and synced RocksDB publication, which prevents a
//! background publisher from overwriting segment state committed by newer writes.

use std::{
    fs,
    sync::{Arc, Mutex},
    time::Instant,
};

use strata_core::{SegmentFileState, StoreCheckpoint};

use crate::{
    Error, PendingSyncRequest, Result, SYNC_AND_COMMIT_INTERVAL, SYNC_AND_COMMIT_SEGMENT_BYTES,
    SYNC_AND_COMMIT_WAL_BYTES, SegmentSync, StoreSyncProfile, SyncAndCommit, SyncRequest,
    WriteCommand, WriteCoordinator,
    file_sync::{FileSyncSender, FileSyncTask},
    maintenance::publish_blob_lsm_edit,
    profile_phase, publish_segment_allocation_baseline,
    seal::prepare_synced_seal,
    unsealed_ingest_segment_count,
};

struct PendingWalSync {
    remaining_segments: usize,
    task: Option<FileSyncTask>,
}

impl SyncAndCommit {
    fn record_sync_error(&self, result: Result<()>) {
        if let Err(error) = result {
            let mut final_result = self
                .file_sync_result
                .lock()
                .expect("durability file-sync result lock poisoned");
            if final_result.is_none() {
                *final_result = Some(Err(error));
            }
        }
    }

    /// The last segment completion submits the WAL sync. A failed segment cancels the prepared
    /// WAL task instead, so this durability cycle never syncs references to a failed segment.
    fn on_segment_sync_finished(
        self: &Arc<Self>,
        result: Result<()>,
        pending_wal_sync: &Mutex<PendingWalSync>,
        wal_sync_tx: &FileSyncSender,
    ) {
        self.record_sync_error(result);
        let wal_sync = {
            let mut pending = pending_wal_sync
                .lock()
                .expect("pending WAL sync lock poisoned");
            assert!(
                pending.remaining_segments > 0,
                "segment sync completed too often"
            );
            pending.remaining_segments -= 1;
            if pending.remaining_segments != 0 {
                return;
            }
            pending
                .task
                .take()
                .expect("last segment completion has no WAL sync task")
        };
        if self
            .file_sync_result
            .lock()
            .expect("durability file-sync result lock poisoned")
            .is_some()
        {
            wal_sync.complete(Err(Error::InvariantViolation {
                reason: "WAL sync cancelled after a segment sync failure".to_owned(),
            }));
            return;
        }
        if let Err(error) = wal_sync_tx.send(wal_sync) {
            error.0.complete(Err(Error::FileSyncQueueClosed));
        }
    }

    fn on_wal_sync_finished(self: &Arc<Self>, result: Result<()>) {
        self.record_sync_error(result);
        {
            let mut final_result = self
                .file_sync_result
                .lock()
                .expect("durability file-sync result lock poisoned");
            if final_result.is_none() {
                *final_result = Some(Ok(self.file_sync_started.elapsed()));
            }
        }
        if self.sync_done_tx.send(Arc::clone(self)).is_ok() {
            let _ = self.wake_tx.try_send(WriteCommand::SyncDone);
        }
    }
}

impl WriteCoordinator {
    /// Starts one sync and commit without waiting for physical I/O.
    ///
    /// Capturing happens on the writer thread, so `target_lsn`, the WAL position, segment offsets,
    /// and allocation counters describe one coherent prefix. Later writes may append to the same
    /// active files; syncing more bytes than the captured offsets is harmless because the metadata
    /// publication claims only this snapshot.
    pub(crate) fn start_sync_and_commit(&mut self, force: bool) -> Result<Option<u64>> {
        if let Some(target) = self.sync_and_commit_in_flight {
            return Ok(Some(target));
        }

        let current_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        let committed_lsn = self.index.get_committed_lsn()?;
        if committed_lsn > current_lsn {
            return Err(Error::InvariantViolation {
                reason: format!("committed LSN {committed_lsn} follows target LSN {current_lsn}"),
            });
        }
        if committed_lsn == current_lsn && !force {
            self.last_committed_at = Instant::now();
            return Ok(None);
        }

        let started = Instant::now();
        let mut segments = std::mem::take(&mut self.pending_segment_syncs);

        let active_segment_id = self.segment.segment_id();
        let active_segment_offset = self.segment.write_offset();
        let active_path = self.segment.path().to_path_buf();
        segments.push(SegmentSync {
            segment_id: active_segment_id,
            path: active_path,
            durable_offset: active_segment_offset,
            sealed_before_lsn: None,
            sealed_sha256: Arc::new(std::sync::Mutex::new(None)),
            allocation_records: self.active_allocation_records,
            allocation_tracker: Arc::clone(&self.active_allocation_tracker),
        });
        let files = segments
            .iter()
            .map(|segment| {
                if segment.segment_id == active_segment_id {
                    return self.segment.clone_file_for_sync().map_err(Error::from);
                }
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&segment.path)
                    .map_err(|source| Error::Io {
                        path: segment.path.clone(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let wal_position = self.wal.position();
        let wal_bytes = self.wal.pending_bytes();
        let segment_bytes = self.pending_segment_bytes;
        let segment_count = segments.len();
        let sync_and_commit = Arc::new(SyncAndCommit {
            target_lsn: current_lsn,
            wal_position,
            checkpoint_segment_id: active_segment_id,
            checkpoint_segment_offset: active_segment_offset,
            segments,
            wal_bytes,
            segment_bytes,
            started,
            file_sync_started: Instant::now(),
            file_sync_result: std::sync::Mutex::new(None),
            sync_done_tx: self.sync_done_tx.clone(),
            wake_tx: self.internal_write_tx.clone(),
        });

        let wal_publish = Arc::clone(&sync_and_commit);
        let (captured_wal_position, wal_sync_task) = self
            .wal
            .sync_data(move |result| wal_publish.on_wal_sync_finished(result))?;
        if captured_wal_position != wal_position {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "synced WAL position {captured_wal_position:?} changed from captured position {wal_position:?}"
                ),
            });
        }
        let pending_wal_sync = Arc::new(Mutex::new(PendingWalSync {
            remaining_segments: segment_count,
            task: Some(wal_sync_task),
        }));
        let wal_sync_tx = self.wal.file_sync_sender();
        for (segment, file) in sync_and_commit.segments.iter().zip(files) {
            let sync_and_commit = Arc::clone(&sync_and_commit);
            let pending_wal_sync = Arc::clone(&pending_wal_sync);
            let wal_sync_tx = wal_sync_tx.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();
            let segment_id = segment.segment_id;
            let sealed_len = segment.durable_offset;
            let sealed_before_lsn = segment.sealed_before_lsn;
            let sealed_sha256 = Arc::clone(&segment.sealed_sha256);
            let path = segment.path.clone();
            let completion_path = path.clone();
            self.segment_sync_tx
                .send(FileSyncTask::new(path, file, move |result| {
                    let result = result.and_then(|()| {
                        if sealed_before_lsn.is_none() {
                            return Ok(());
                        }
                        match prepare_synced_seal(&config, segment_id, &completion_path, sealed_len)
                        {
                            Ok(checksum) => {
                                *sealed_sha256.lock().expect("seal checksum lock poisoned") =
                                    checksum;
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    });
                    if sealed_before_lsn.is_some() && result.is_err() {
                        metrics.record_seal_error();
                    }
                    sync_and_commit.on_segment_sync_finished(
                        result,
                        &pending_wal_sync,
                        &wal_sync_tx,
                    );
                }))
                .map_err(|_| Error::FileSyncQueueClosed)?;
        }

        self.wal.mark_pending_bytes_captured();
        self.pending_segment_bytes = 0;
        self.oldest_uncommitted_at = None;
        self.sync_and_commit_in_flight = Some(current_lsn);
        self.metrics.set_durability_pending(0, 0, true);
        Ok(Some(current_lsn))
    }

    pub(crate) fn maybe_start_sync_and_commit(&mut self, force: bool) -> Result<bool> {
        if self.sync_and_commit_in_flight.is_some() {
            return Ok(false);
        }
        let age_due = self
            .oldest_uncommitted_at
            .is_some_and(|started| started.elapsed() >= SYNC_AND_COMMIT_INTERVAL);
        let segment_pressure_due = self.pending_segment_bytes >= SYNC_AND_COMMIT_SEGMENT_BYTES;
        let pressure_due =
            self.wal.pending_bytes() >= SYNC_AND_COMMIT_WAL_BYTES || segment_pressure_due;
        if !force && !age_due && !pressure_due {
            return Ok(false);
        }
        Ok(self.start_sync_and_commit(force)?.is_some())
    }

    pub(crate) fn note_uncommitted_write(&mut self, segment_bytes: u64) -> Result<()> {
        self.pending_segment_bytes = self.pending_segment_bytes.saturating_add(segment_bytes);
        self.oldest_uncommitted_at.get_or_insert_with(Instant::now);
        self.metrics.set_durability_pending(
            self.wal.pending_bytes(),
            self.pending_segment_bytes,
            self.sync_and_commit_in_flight.is_some(),
        );
        self.maybe_start_sync_and_commit(false)?;
        Ok(())
    }

    /// Finishes the metadata half of a completed file sync on the serialized writer thread.
    pub(crate) fn commit_after_sync(
        &mut self,
        commit: Arc<SyncAndCommit>,
    ) -> Result<(u64, StoreSyncProfile)> {
        let expected = self.sync_and_commit_in_flight.take();
        if expected != Some(commit.target_lsn) {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "durability completion for LSN {} does not match in-flight target {expected:?}",
                    commit.target_lsn
                ),
            });
        }

        let file_sync_result = commit
            .file_sync_result
            .lock()
            .expect("durability file sync result lock poisoned")
            .take()
            .expect("ready durability publication has no file-sync result");
        let mut phases = StoreSyncProfile {
            segment_sync: file_sync_result?,
            ..StoreSyncProfile::default()
        };
        let commit_started = Instant::now();
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("durability publish lock poisoned");
        // The relocation frontier must be sampled while holding the same lock used by GC
        // activation. The synced RocksDB write below then proves that every sampled manifest edit
        // and activation row reached disk together.
        let durable_relocation_lsn = self.relocations.lsm().last_lsn()?.unwrap_or_default();

        let current_committed_lsn = self.index.get_committed_lsn()?;
        if current_committed_lsn > commit.target_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published LSN {current_committed_lsn} follows completed durability target {}",
                    commit.target_lsn
                ),
            });
        }
        let current_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if commit.target_lsn > current_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "durability target {} follows current LSN {current_lsn}",
                    commit.target_lsn
                ),
            });
        }

        let (batch, states, allocation_marks) = profile_phase(
            Some(&mut phases),
            |profile, elapsed| profile.published_lsn_compute += elapsed,
            || {
                let mut batch = self.index.batch();
                let mut states = Vec::with_capacity(commit.segments.len());
                let mut allocation_marks = Vec::with_capacity(commit.segments.len());
                for segment in &commit.segments {
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
                    if let Some(sealed_before_lsn) = segment.sealed_before_lsn {
                        if state.state != SegmentFileState::Sealing {
                            return Err(Error::InvariantViolation {
                                reason: format!(
                                    "segment {} was captured for sealing but is {:?}",
                                    segment.segment_id, state.state
                                ),
                            });
                        }
                        state.state = SegmentFileState::Sealed;
                        state.sealed_before_lsn = Some(sealed_before_lsn);
                        state.sealed_len = Some(segment.durable_offset);
                        state.sealed_sha256 = *segment
                            .sealed_sha256
                            .lock()
                            .expect("seal checksum lock poisoned");
                    }
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
                    .put_commit_lsn_batch(&mut batch, commit.target_lsn)?;
                self.index.put_store_checkpoint_batch(
                    &mut batch,
                    StoreCheckpoint {
                        wal_position: commit.wal_position,
                        active_segment_id: commit.checkpoint_segment_id,
                        active_segment_offset: commit.checkpoint_segment_offset,
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
        for state in &states {
            if state.state == SegmentFileState::Sealed {
                self.metrics.record_segment_sealed();
            }
        }
        self.metrics
            .set_unsealed_segments(unsealed_ingest_segment_count(&self.index)?);

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
                self.last_committed_at = Instant::now();
                self.metrics.set_active_segment(
                    self.active_segment_state.segment_id,
                    self.active_segment_state.write_offset,
                    self.durable_offset,
                );
                self.metrics.set_published_lsn(commit.target_lsn);
            },
        );
        drop(_commit_guard);

        self.reclaim_store_wal(commit.target_lsn)?;
        let elapsed = commit.started.elapsed();
        self.metrics.record_sync(Ok(commit.segment_bytes), elapsed);
        self.metrics.record_durability_wal_bytes(commit.wal_bytes);
        self.metrics.set_durability_pending(
            self.wal.pending_bytes(),
            self.pending_segment_bytes,
            false,
        );
        self.gc_concurrency
            .observe_sync(elapsed, commit.segment_bytes);
        self.request_lsm_compaction();
        phases.writer_total = commit_started.elapsed();
        Ok((commit.target_lsn, phases))
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
        match self.index.get_committed_lsn() {
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
        let needs_follow_up = self.sync_and_commit_in_flight.is_some();
        self.pending_sync_requests.push(PendingSyncRequest {
            target_lsn,
            needs_follow_up,
            response_tx,
            profile_request,
            profile,
            started,
        });
        if self.sync_and_commit_in_flight.is_none()
            && let Err(error) = self.start_sync_and_commit(true)
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
    fn reclaim_store_wal(&mut self, committed_lsn: u64) -> Result<()> {
        self.lsm.publish_pending_through(committed_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        self.lsm.materialize_through(committed_lsn, |edit| {
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

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        sync::{Arc, Mutex, mpsc},
        time::{Duration, Instant},
    };

    use strata_core::WalPosition;
    use tempfile::tempdir;

    use super::{FileSyncTask, PendingWalSync};
    use crate::{SyncAndCommit, WriteCommand};

    #[test]
    fn wal_sync_waits_for_all_segment_syncs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        let (wal_sync_tx, wal_sync_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (wake_tx, wake_rx) = mpsc::sync_channel(1);
        let publish = Arc::new(SyncAndCommit {
            target_lsn: 1,
            wal_position: WalPosition::default(),
            checkpoint_segment_id: 1,
            checkpoint_segment_offset: 0,
            segments: Vec::new(),
            wal_bytes: 0,
            segment_bytes: 0,
            started: Instant::now(),
            file_sync_started: Instant::now(),
            file_sync_result: Mutex::new(None),
            sync_done_tx: ready_tx,
            wake_tx,
        });
        let wal_publish = Arc::clone(&publish);
        let pending_wal_sync = Mutex::new(PendingWalSync {
            remaining_segments: 2,
            task: Some(FileSyncTask::new(path, file, move |result| {
                wal_publish.on_wal_sync_finished(result);
            })),
        });

        publish.on_segment_sync_finished(Ok(()), &pending_wal_sync, &wal_sync_tx);
        assert!(matches!(
            wal_sync_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        publish.on_segment_sync_finished(Ok(()), &pending_wal_sync, &wal_sync_tx);
        let wal_sync = wal_sync_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            ready_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        wal_sync.complete(Ok(()));
        let ready = ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(Arc::ptr_eq(&ready, &publish));
        assert!(matches!(
            wake_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteCommand::SyncDone
        ));
    }
}
