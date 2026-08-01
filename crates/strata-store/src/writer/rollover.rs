//! Active-segment rollover and seal-backlog scheduling.

use std::{thread, time::Instant};

use strata_core::{SegmentFileState, StrataLsn};

use crate::{
    Error, PendingRollover, Result, SEAL_BACKLOG_WAIT, WriteCoordinator,
    active_segment_state_from_path, seal::SegmentSealTask, unsealed_ingest_segment_count,
};

impl WriteCoordinator {
    /// Runs all rollover side effects whose metadata was just committed.
    ///
    /// Failure mode avoided: the sealer queue is outside RocksDB and cannot be rolled back. Running
    /// this only after commit means a crash before commit has no queued seal for an index-invisible
    /// segment.
    pub(crate) fn run_rollover_post_commit(&self, pending_rollovers: Vec<PendingRollover>) {
        for rollover in pending_rollovers {
            rollover.run_post_commit(self.seal_tx.clone(), self.metrics.clone());
        }
    }

    pub(crate) fn request_lsm_flush(&self, rolled_memtable: bool) {
        if rolled_memtable && self.lsm_flush_tx.send(()).is_err() {
            let reason = "LSM memtable flusher stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    pub(crate) fn request_lsm_compaction(&self) {
        if self.lsm_compact_tx.send(()).is_err() {
            let reason = "LSM compactor stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    /// Rolls a non-empty payload segment on its own cadence.
    ///
    /// This is not a durability publication: it syncs the segment being closed, but it neither
    /// syncs the store WAL nor advances `PublishedLsn`. `sync_data` owns that separate boundary.
    pub(crate) fn process_segment_rollover(&mut self) -> Result<()> {
        self.last_segment_rollover_at = Instant::now();
        let sealed_before_lsn = self.index.get_next_lsn()?;
        if sealed_before_lsn <= self.last_segment_rollover_next_lsn {
            return Ok(());
        }

        self.rollover_active_segment(sealed_before_lsn)?;
        let pending_rollovers = self.take_pending_rollovers();
        let commit_result = (|| {
            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            batch.write().map_err(strata_index::Error::from)?;
            Ok::<(), Error>(())
        })();

        match commit_result {
            Ok(()) => {
                self.run_rollover_post_commit(pending_rollovers);
                Ok(())
            }
            Err(error) => {
                self.restore_pending_rollovers(pending_rollovers);
                Err(error)
            }
        }
    }

    /// Rolls the store-owned payload segment and stages its RocksDB metadata.
    ///
    /// Segment rollover is intentionally here, beside WAL ownership. The LSM sees only the
    /// `RecordRef` produced after this method installs the next segment.
    pub(crate) fn rollover_active_segment(&mut self, sealed_before_lsn: StrataLsn) -> Result<()> {
        self.wait_for_seal_backlog_capacity()?;
        let old_segment_id = self.active_segment_state.segment_id;
        let sealed_length = self.segment.write_offset();
        if self.segment.segment_id() != old_segment_id
            || sealed_length != self.active_segment_state.write_offset
        {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "payload writer is segment {} at {sealed_length}, expected {old_segment_id} at {}",
                    self.segment.segment_id(),
                    self.active_segment_state.write_offset
                ),
            });
        }
        // A rollover is rare and already enters the sealing path, so synchronously closing this
        // segment keeps the hand-off obvious without adding another pending-sync state machine.
        self.segment.sync_data()?;
        let next = self.segment_factory.create()?;
        let new_segment_id = next.segment_id();
        if new_segment_id <= old_segment_id {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "replacement segment {new_segment_id} must follow active segment {old_segment_id}"
                ),
            });
        }
        let new_state =
            active_segment_state_from_path(&self.config, self.ingest_owner, new_segment_id, 0, 0);
        let mut old_state = self.active_segment_state.clone();
        old_state.write_offset = sealed_length;
        old_state.durable_offset = self.durable_offset;
        old_state.state = SegmentFileState::Sealing;
        old_state.sealed_before_lsn = Some(sealed_before_lsn);

        self.pending_rollovers.push(PendingRollover {
            old_segment_state: old_state,
            new_segment_state: new_state.clone(),
            new_segment_published_at_lsn: sealed_before_lsn,
            seal_task: SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: sealed_length,
                sealed_before_lsn,
                allocation_records: self.pending_allocation_records,
            },
        });
        self.segment = next;
        self.pending_allocation_records = 0;
        self.active_segment_state = new_state;
        self.durable_offset = 0;
        self.last_segment_rollover_at = Instant::now();
        self.last_segment_rollover_next_lsn = sealed_before_lsn;
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
            self.durable_offset,
        );
        Ok(())
    }

    /// Backpressure: if the sealer can't keep up, writes eventually block here instead of
    /// accumulating unbounded unsealed segments. Unsealed segments are the expensive thing at
    /// restart (each one gets a full recovery scan), so the cap directly bounds worst-case
    /// recovery time.
    pub(crate) fn wait_for_seal_backlog_capacity(&self) -> Result<()> {
        let started = Instant::now();
        let mut waiting = false;
        loop {
            if unsealed_ingest_segment_count(&self.index)? < self.config.max_unsealed_segments {
                if waiting {
                    self.metrics
                        .finish_seal_backpressure_wait(started.elapsed());
                    self.gc_concurrency.set_seal_backpressure(false);
                }
                return Ok(());
            }
            if !waiting {
                waiting = true;
                self.metrics.start_seal_backpressure_wait();
                self.gc_concurrency.set_seal_backpressure(true);
            }
            thread::sleep(SEAL_BACKLOG_WAIT);
        }
    }
}
