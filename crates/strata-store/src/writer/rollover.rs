//! Active-segment rollover and unsealed segment backpressure.
//!
//! Rollover installs a fresh active file and stages the old segment as `Sealing`. The next
//! durability publication syncs the old file, verifies its final length, optionally hashes it,
//! and publishes `Sealed` with the durable LSN frontier in one RocksDB batch.

use std::{sync::Arc, thread, time::Instant};

use strata_core::{SegmentFileState, StrataLsn};

use crate::{
    Error, PendingRollover, Result, SEAL_BACKLOG_WAIT, SegmentSync, WriteCoordinator,
    active_segment_state_from_path, segment_state::SegmentAllocationTracker,
};

impl WriteCoordinator {
    /// Nudges the background memtable flusher after a commit whose blob-LSM write rolled a
    /// memtable. The caller (the foreground batch commit) passes `rolled_memtable`, so a commit
    /// that stayed inside the active memtable costs nothing here.
    ///
    /// The send only fails when the flusher thread is gone, and that thread only exits at shutdown
    /// or after a fatal error of its own. A store that can no longer flush memtables stops
    /// draining frozen memtables, which both holds memory and blocks store-WAL reclamation — so a
    /// dead flusher halts the store and the LSM instead of being quietly ignored.
    pub(crate) fn request_lsm_flush(&self, rolled_memtable: bool) {
        if rolled_memtable && self.lsm_flush_tx.send(()).is_err() {
            let reason = "LSM memtable flusher stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    /// Same shape as request_lsm_flush but for the background compactor, and unconditional — the
    /// callers have already decided a compaction pass is warranted. A dead compactor is treated
    /// exactly like a dead
    /// flusher: the store cannot stay healthy without it, so everything halts.
    pub(crate) fn request_lsm_compaction(&self) {
        if self.lsm_compact_tx.send(()).is_err() {
            let reason = "LSM compactor stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    /// Swaps the physical writer to a fresh segment and stages every metadata consequence in
    /// memory. It does not wait for durability or perform file I/O beyond creating the new file.
    ///
    /// Segment rollover is intentionally here, beside WAL ownership. The LSM sees only the
    /// `RecordRef` produced after this method installs the next segment.
    ///
    /// The segment-full path (commit.rs) calls this mid-batch with the LSN of the put that did not
    /// fit, and that put becomes the first record of the new segment. Therefore
    /// `sealed_before_lsn` promises that every record in the closing segment has a lower LSN and
    /// everything at or above it lives in a later segment.
    ///
    /// The writer first waits for unsealed-segment capacity, verifies its physical offset against
    /// metadata, creates a strictly newer segment, and stages both metadata rows. It also records
    /// the closed file and final allocation counters for the next durability snapshot.
    ///
    /// From this point the very next put lands in S31, while S30's rows sit in pending_rollovers
    /// waiting for whichever index batch commits next. apply_batch (batch.rs) explains why they
    /// must ride that batch atomically: no metadata referencing records in S31 may become visible
    /// before S31 itself is registered.
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
        let pending_segment_sync = SegmentSync {
            segment_id: old_segment_id,
            path: self.segment.path().to_path_buf(),
            durable_offset: sealed_length,
            sealed_before_lsn: Some(sealed_before_lsn),
            sealed_sha256: Arc::new(std::sync::Mutex::new(None)),
            allocation_records: self.active_allocation_records,
            allocation_tracker: Arc::clone(&self.active_allocation_tracker),
        };
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
            new_segment_id,
        });
        self.pending_segment_syncs.push(pending_segment_sync);
        self.segment = next;
        self.active_allocation_records = 0;
        self.active_allocation_tracker = Arc::new(SegmentAllocationTracker::default());
        self.active_segment_state = new_state;
        self.durable_offset = 0;
        self.unsealed_segments += 1;
        self.metrics.set_unsealed_segments(self.unsealed_segments);
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
            self.durable_offset,
        );
        Ok(())
    }

    /// Backpressure: if durability publication can't keep up, writes eventually block here
    /// instead of accumulating unbounded unsealed segments. Unsealed segments are the expensive
    /// thing at restart, so the cap directly bounds worst-case recovery time.
    ///
    /// The writer maintains the number of ingest segments still Open or Sealing. A count under
    /// config.max_unsealed_segments means there is room for one more and the wait (if any) ends.
    /// Otherwise the writer naps SEAL_BACKLOG_WAIT (10 ms — short on purpose, because this sleep
    /// can sit directly on a foreground put that triggered a segment-full rollover) until a
    /// durability completion seals a rolled segment.
    ///
    /// The first blocked iteration raises two flags exactly once: a metrics timer measuring how
    /// long the writer stays blocked, and the GC tuner's seal-backpressure bit. Foreground writes
    /// stalling on durability is foreground pressure, and the tuner reacts by backing GC off.
    /// Both flags are cleared on the way out, and the wait duration is recorded.
    pub(crate) fn wait_for_seal_backlog_capacity(&mut self) -> Result<()> {
        let started = Instant::now();
        let mut waiting = false;
        loop {
            self.process_sync_done();
            self.store_halt.check()?;
            if self.unsealed_segments < self.config.max_unsealed_segments {
                if waiting {
                    self.metrics
                        .finish_seal_backpressure_wait(started.elapsed());
                    self.gc_concurrency.set_seal_backpressure(false);
                }
                return Ok(());
            }
            self.maybe_start_sync_and_commit(false)?;
            if !waiting {
                waiting = true;
                self.metrics.start_seal_backpressure_wait();
                self.gc_concurrency.set_seal_backpressure(true);
            }
            thread::sleep(SEAL_BACKLOG_WAIT);
        }
    }
}
