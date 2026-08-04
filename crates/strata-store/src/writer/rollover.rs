//! Active-segment rollover and seal-backlog scheduling.
//!
//! The writer appends every foreground payload into one active ingest segment. That file cannot
//! grow forever: sealed segments are what retention organization and GC operate on, and unsealed
//! segments are what recovery has to rescan at startup. Rollover is the hand-off between those two
//! worlds - close the current file, open a fresh one, and let the background sealer finish the old
//! one.
//!
//! Running example for everything below: the active segment is S30 with 900 MB written, and the
//! store's next_lsn is 4200 at the moment we roll. Rollover creates S31 and makes it the active
//! segment, flips S30's row to Sealing with sealed_before_lsn 4200 (every record inside S30 has an
//! LSN below 4200), and queues S30 for the sealer. The sealer later verifies the file length,
//! hashes the bytes (integrity policy permitting), and republishes S30 as Sealed — only then does
//! GC start considering it.
//!
//! There are two triggers. A foreground put that no longer fits in the active segment rolls
//! immediately, mid-batch (commit.rs calls rollover_active_segment directly). And a timer rolls a
//! quiet-but-growing segment every SEGMENT_ROLLOVER_INTERVAL (20 minutes), so a slow trickle of
//! writes still produces sealed files on a bounded cadence; administrative tools and benchmarks
//! can fire the same path on demand through the RolloverSegment command.
//!
//! The load-bearing design decision is that a rollover happens in three phases. Phase one
//! (rollover_active_segment) swaps the in-memory writer to the new file and only *stages* the
//! metadata rows in pending_rollovers. Phase two commits those rows inside whichever index batch
//! happens next — the same foreground batch in the segment-full case or a small dedicated batch in
//! the timer case. Phase three (run_rollover_post_commit) queues the seal task strictly after the
//! commit. Each function below explains what breaks if its phase ran in a different order.

use std::{sync::mpsc, thread, time::Instant};

use strata_core::{SegmentFileState, StrataLsn};

use crate::{
    Error, PendingRollover, PendingSegmentSync, Result, SEAL_BACKLOG_WAIT, WriteCoordinator,
    active_segment_state_from_path, file_sync::FileSyncTask, seal::SegmentSealTask,
    unsealed_ingest_segment_count,
};

impl WriteCoordinator {
    /// Phase three of a rollover: runs the side effects whose metadata was just committed.
    ///
    /// Concretely, one SegmentSealTask per rollover is handed to the background sealer — for the
    /// example, "seal S30: 900 MB, everything below LSN 4200". Every commit path that can carry
    /// rollover rows calls this after its batch write succeeds: the foreground batch commit and
    /// process_segment_rollover below.
    ///
    /// Failure mode avoided: the sealer queue is outside RocksDB and cannot be rolled back. Running
    /// this only after commit means a crash before commit has no queued seal for an index-invisible
    /// segment.
    pub(crate) fn run_rollover_post_commit(&self, pending_rollovers: Vec<PendingRollover>) {
        for rollover in pending_rollovers {
            rollover.run_post_commit(self.seal_tx.clone(), self.metrics.clone());
        }
    }

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

    /// The timer-path entry point: rolls a non-empty payload segment on its own cadence.
    ///
    /// The writer loop lands here when SEGMENT_ROLLOVER_INTERVAL expires, and the public
    /// rollover_active_segment_for_sealing API arrives here too via the RolloverSegment command.
    /// The cadence clock is reset before anything else, so even a failed attempt waits out a full
    /// interval instead of retrying hot.
    ///
    /// Then the emptiness guard. next_lsn is read (it will become sealed_before_lsn if we do roll)
    /// and compared against the value recorded at the end of the previous rollover. Bytes never
    /// enter a segment without consuming an LSN, so an unchanged next_lsn proves the active
    /// segment is still empty and the roll is skipped — without this, a completely idle store
    /// would mint a fresh segment file every 20 minutes forever. The check is conservative in the
    /// other direction: LSNs also advance for byte-free operations like tombstones and epoch
    /// changes, so "next_lsn moved" does not strictly guarantee the segment grew, and that is
    /// fine — the cost is an occasional near-empty rollover, not a correctness problem.
    ///
    /// A real roll performs the three phases from the module doc back to back:
    /// rollover_active_segment stages the swap (S30 out, S31 in, rows parked in
    /// pending_rollovers); the staged rows are drained into a small dedicated index batch and
    /// committed, because unlike the segment-full path there is no foreground batch coming to
    /// carry them; and on success run_rollover_post_commit queues the seal task. If the commit
    /// fails instead, the rows are put back with restore_pending_rollovers — the in-memory writer
    /// has already moved on to S31 and that swap cannot be undone, so the rows must survive to
    /// ride whichever commit happens next rather than be dropped on the floor.
    ///
    /// The batch is deliberately written without sync, and this is not a durability publication:
    /// rollover starts syncing the segment being closed, but it neither waits for that sync nor
    /// syncs the store WAL or advances `PublishedLsn`. `sync_data` owns that separate boundary,
    /// waits for the rollover sync, and makes these rows crash-durable with its synced index write.
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

    /// Phase one of a rollover: starts syncing the closing segment in the background, swaps the
    /// physical writer to a fresh segment, and stages every metadata consequence in memory. It does
    /// not wait for durability or publish any durable metadata.
    ///
    /// Segment rollover is intentionally here, beside WAL ownership. The LSM sees only the
    /// `RecordRef` produced after this method installs the next segment.
    ///
    /// Two callers, one meaning for the boundary LSN. The segment-full path (commit.rs) calls this
    /// mid-batch with the LSN of the put that did not fit, and that put becomes the first record
    /// of the new segment. The timer path passes the current next_lsn, which no record has used
    /// yet. Either way `sealed_before_lsn` promises the same thing: every record in the closing
    /// segment has an LSN strictly below it, and everything at or above it lives in a later
    /// segment. In the example, S30 gets sealed_before_lsn 4200.
    ///
    /// The flow, in order:
    /// 1. Block until the sealer has capacity (wait_for_seal_backlog_capacity below). This is the
    ///    one place a rollover can stall the writer thread.
    /// 2. Cross-check the physical writer against the tracked segment row: same segment id, same
    ///    write offset. A mismatch means bytes went somewhere the metadata did not follow, the
    ///    seal would record the wrong length, and the only honest answer is InvariantViolation.
    /// 3. Clone the closing file descriptor and queue its fsync. The completion is retained until
    ///    sync_data waits for it, keeping the rollover-triggering put off the fsync critical path.
    /// 4. Create the replacement file and require its id to be strictly larger. Segment ids are
    ///    how "later" is spelled on disk; a reused or non-monotonic id would corrupt every
    ///    ordering assumption recovery and GC make.
    /// 5. Build the two rows this rollover will eventually commit: S30's row with its final
    ///    write_offset (the 900 MB), the durable offset as of the last publication, state
    ///    Sealing, and sealed_before_lsn 4200; and a clean Open row for S31 at offset zero with
    ///    no sealed fields (active_segment_state_from_path explains why stale sealed fields would
    ///    be dangerous).
    /// 6. Stage a PendingRollover carrying both rows, S31's published_at_lsn (4200 — the stamp
    ///    snapshot protection later compares against), and the SegmentSealTask (S30, its 900 MB,
    ///    the 4200 boundary, and the count of records appended since the last allocation
    ///    baseline, which the sealer republishes for GC accounting).
    /// 7. Swap the in-memory world: the writer now appends to S31, the allocation-record counter
    ///    and durable offset reset to zero, last_segment_rollover_next_lsn records 4200 for the
    ///    next emptiness check, and the active-segment metrics gauge moves over.
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
        let path = self.segment.path().to_path_buf();
        let file = self.segment.clone_file_for_sync()?;
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        self.segment_sync_tx
            .send(FileSyncTask::new(path, file, move |result| {
                let _ = completion_tx.send(result);
            }))
            .map_err(|_| Error::FileSyncQueueClosed)?;
        let pending_segment_sync = PendingSegmentSync {
            segment_id: old_segment_id,
            completion_rx,
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
            new_segment_state: new_state.clone(),
            new_segment_published_at_lsn: sealed_before_lsn,
            seal_task: SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: sealed_length,
                sealed_before_lsn,
                allocation_records: self.pending_allocation_records,
            },
        });
        self.pending_segment_syncs.push(pending_segment_sync);
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
    ///
    /// The mechanism is plain polling. Each iteration counts the ingest segments still Open or
    /// Sealing in the index; a count under config.max_unsealed_segments means there is room for
    /// one more and the wait (if any) ends. Otherwise the writer naps SEAL_BACKLOG_WAIT (10 ms —
    /// short on purpose, because this sleep can sit directly on a foreground put that triggered a
    /// segment-full rollover) and counts again, for as long as the sealer needs.
    ///
    /// The first blocked iteration raises two flags exactly once: a metrics timer measuring how
    /// long the writer stays blocked, and the GC tuner's seal-backpressure bit. Foreground writes
    /// stalling on the sealer is foreground pressure, and the tuner reacts by backing GC off so
    /// its IO goes to the sealer instead of competing with it. Both flags are cleared on the way
    /// out, and the wait duration is recorded.
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
