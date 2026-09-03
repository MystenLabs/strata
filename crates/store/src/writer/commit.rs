//! Foreground batch commit path: batch preparation, segment append, and the
//! atomic index metadata commit.
//! One writer thread, one LSN sequence: prepare_batch reserves a contiguous range, so a batch's ops can never interleave with another writer's.
//! Fixed publication order — segment bytes → store WAL → LSM memtable → RocksDB batch — and the RocksDB batch is the commit point.
//! Committed ≠ durable: when the caller gets its LSN back, the write is visible and ordered, but only a later sync (or the periodic durability publish) makes it crash-proof. Callers needing durability gate on published_lsn() >= lsn
//!
//! This is a *group* commit path. The writer loop, on picking up one Batch command, greedily
//! drains every further Batch already sitting in the queue (up to the grouped-operation and
//! grouped-payload caps in mod.rs) and hands the whole vector here. The point is amortization:
//! N waiting callers cost one store-WAL append, one blob-LSM write, and one RocksDB batch —
//! instead of N of each — while each caller still gets its own response, its own LSNs, and its
//! own metrics.
//!
//! Running example for everything below, two callers grouped together: batch X carries
//! Put("foo") and IncrementEpoch, batch Y carries Put("bar"). next_lsn starts at 100, so X gets
//! LSNs 100 and 101, Y gets 102, and the group commits next_lsn = 103. "foo" and "bar" land as
//! bytes in the active segment; the epoch change is the one op kind that writes no payload.
//!
//! The failure philosophy has two tiers, split by whether physical work has started. During
//! preparation a batch fails *alone*: its caller gets the error, the LSN cursor does not move,
//! and the rest of the group proceeds as if it never existed. From the first segment append
//! onward the group is one fate: any failure halts the writer (bytes and WAL entries may exist
//! that metadata will never acknowledge — only recovery can reconcile that) and every caller in
//! the group receives the halt.

use std::time::Instant;

use core_types::{Epoch, ShardId, ShardKey, StrataLsn, encoded_record_len};

use crate::{
    BatchOp, BatchWriteRequest, BatchWriteResult, Error, PendingRollover, PreparedBatch,
    PreparedBatchOp, Result, StoreWriteProfile, WriteCoordinator, metrics::PutMetric,
    profile_phase, wal::WalEntry, wal_format::StoreWalMutation,
};

impl WriteCoordinator {
    /// Commits queued foreground batches together in segment, WAL, LSM, then RocksDB order.
    ///
    /// Preparation assigns a contiguous LSN range. Payload operations append to the active segment,
    /// rolling it first when needed. Every operation is appended to the store WAL, keyed mutations
    /// enter the LSM, and the RocksDB batch atomically publishes the logical metadata plus any
    /// staged rollover rows. After physical writes begin, a failure halts the writer for recovery.
    ///
    /// The walk through, in code order:
    ///
    /// Phase 1 — prepare, per batch, in arrival order. Empty batches are answered immediately with
    /// a default result and never join the group. The first real batch reads next_lsn and
    /// current_epoch from RocksDB once; every later batch threads the in-memory cursor forward, so
    /// X consumes 100-101 and Y starts at 102 without touching the index again. A batch that fails
    /// validation is rejected *individually* — its caller gets the error and its put/delete
    /// metrics record failure — and, per the inline comment below, neither cursor advances, so the
    /// surviving batches still receive a gap-free range. Nothing physical has happened yet, which
    /// is what makes this per-batch isolation possible at all.
    ///
    /// Phase 2 — physical writes, one pass over every op of every surviving batch. A Put first
    /// asks the active segment for capacity and, if the record does not fit, rolls the segment
    /// mid-group with the put's own LSN as the boundary — the put then becomes the first record of
    /// the new segment. The append is length-checked against what prepare_batch predicted (a
    /// mismatch is an InvariantViolation: the encoder and the writer disagree about bytes already
    /// on disk), the returned record_ref is backfilled into the prepared op for the LSM mutation
    /// to reference, and the in-memory active-segment row advances: write_offset, and the min/max
    /// LSN bounds GC later uses to reason about the segment without scanning it. Then, for every
    /// op kind: exactly one store-WAL entry at its LSN, and for keyed ops (Put, SetBlobLifetime,
    /// Tombstone) one blob-LSM mutation routed to its hash partition (partition_for_key over the
    /// raw key — the same routing the relocation LSM must mirror). IncrementEpoch is the one
    /// RocksDB-only op: a bare Epoch WAL entry, no LSM row. The phase ends with the two amortized
    /// writes — one WAL append and one LSM write_batch for the entire group — and notes whether
    /// the LSM rolled a memtable. `failure_context` is threaded through every step so a halt names
    /// the exact sub-step that failed. Any error here is tier two: orphaned-byte accounting, halt,
    /// and fail_batch_group for everyone.
    ///
    /// Phase 3 — the commit point. Pending rollovers are taken (a roll staged in phase 2 commits
    /// exactly once, atomically with the records that already reference the new segment) and
    /// commit_write_group writes the single RocksDB batch. Success makes every LSN in the group
    /// visible at once; failure is fatal exactly like phase 2, because the WAL and LSM now hold
    /// state the index refused to acknowledge.
    ///
    /// Phase 4 — post-commit bookkeeping, no failures left that can reject a caller. The LSM
    /// flusher is nudged if a memtable rolled; gauges update (active segment, next_lsn = 103, and
    /// the current epoch from the *last* batch that changed it). Only now are per-put and
    /// per-tombstone success metrics recorded — recording during the append would count a write
    /// as successful before the commit that makes it visible — and each caller receives its own
    /// result with its own op LSNs. note_committed_write then accounts the group's segment bytes
    /// as durability-pending and may immediately start an asynchronous durability publish if byte
    /// or age pressure crossed a threshold: this is how "committed" writes stop being merely
    /// visible and start becoming crash-proof without any caller asking. Profiles go out last,
    /// stamped with the full writer-side elapsed time.
    pub(crate) fn process_batch_group(&mut self, requests: Vec<BatchWriteRequest>) {
        let started = Instant::now();
        let mut next_lsn = None;
        let mut current_epoch = None;
        let mut prepared_batches = Vec::with_capacity(requests.len());

        // Validate everything before writing bytes. A rejected batch does not advance either
        // cursor, so later valid batches still receive a gap-free LSN range.
        for mut request in requests {
            let put_count = request
                .ops
                .iter()
                .filter(|op| matches!(op, BatchOp::Put { .. }))
                .count();
            let delete_count = request
                .ops
                .iter()
                .filter(|op| matches!(op, BatchOp::Tombstone { .. }))
                .count();
            let mut profile = request.profile.begin(started);
            if request.ops.is_empty() {
                let _ = profile_phase(
                    profile.as_mut(),
                    |profile, elapsed| profile.response_send += elapsed,
                    || request.response_tx.send(Ok(BatchWriteResult::default())),
                );
                if let Some(mut profile) = profile {
                    profile.writer_total = started.elapsed();
                    request.profile.send(profile);
                }
                continue;
            }

            let ops = std::mem::take(&mut request.ops);
            let result = profile_phase(
                profile.as_mut(),
                |profile, elapsed| profile.prepare_batch += elapsed,
                || {
                    let batch_next_lsn = match next_lsn {
                        Some(next_lsn) => next_lsn,
                        None => self.index.get_next_lsn()?,
                    };
                    let batch_epoch = match next_lsn {
                        Some(_) => current_epoch,
                        None => self.index.get_current_epoch()?,
                    };
                    self.prepare_batch(ops, batch_next_lsn, batch_epoch)
                },
            );
            match result {
                Ok((prepared, batch_next_lsn, batch_epoch)) => {
                    next_lsn = Some(batch_next_lsn);
                    current_epoch = batch_epoch;
                    prepared_batches.push((request, prepared, profile));
                }
                Err(error) => {
                    let _ = profile_phase(
                        profile.as_mut(),
                        |profile, elapsed| profile.response_send += elapsed,
                        || request.response_tx.send(Err(error)),
                    );
                    for _ in 0..put_count {
                        self.metrics.record_put(Err(()), started.elapsed());
                    }
                    for _ in 0..delete_count {
                        self.metrics.record_delete(false, started.elapsed());
                    }
                    if let Some(mut profile) = profile {
                        profile.writer_total = started.elapsed();
                        request.profile.send(profile);
                    }
                }
            }
        }

        if prepared_batches.is_empty() {
            return;
        }

        let mut appended_records = 0_u64;
        let mut appended_bytes = 0_u64;
        let mut put_metrics = vec![Vec::new(); prepared_batches.len()];
        let mut lsm_writes = Vec::new();
        let mut wal_entries = Vec::new();
        let mut failure_context = "grouped batch commit";
        let write_result = (|| -> Result<bool> {
            for (batch_index, (_, prepared, profile)) in prepared_batches.iter_mut().enumerate() {
                for op in &mut prepared.ops {
                    let lsn = op.lsn();
                    if let PreparedBatchOp::Put {
                        shard,
                        key,
                        payload,
                        record_ref,
                        record_bytes,
                        ..
                    } = op
                    {
                        failure_context = "payload segment append";
                        let written = profile_phase(
                            profile.as_mut(),
                            |profile, elapsed| profile.segment_append += elapsed,
                            || {
                                if self.segment.ensure_capacity(*record_bytes).is_err() {
                                    self.rollover_active_segment(lsn)?;
                                }
                                self.segment
                                    .append_for_shard(key, lsn, *shard, payload)
                                    .map_err(Error::from)
                            },
                        )?;
                        if written.record_len != *record_bytes {
                            failure_context = "payload segment length";
                            return Err(Error::InvariantViolation {
                                reason: format!(
                                    "segment put at LSN {lsn} wrote {}, expected {record_bytes}",
                                    written.record_len
                                ),
                            });
                        }
                        *record_ref = Some(written.record_ref);
                        failure_context = "advance active segment offset";
                        let write_offset = written.record_ref.end_offset().ok_or_else(|| {
                            Error::InvariantViolation {
                                reason: format!(
                                    "record reference at LSN {lsn} overflows its segment"
                                ),
                            }
                        })?;
                        appended_records = appended_records.saturating_add(1);
                        appended_bytes = appended_bytes.saturating_add(written.record_ref.len);
                        self.active_allocation_records =
                            self.active_allocation_records.saturating_add(1);
                        put_metrics[batch_index].push(PutMetric {
                            payload_bytes: payload.len() as u64,
                            record_bytes: *record_bytes,
                        });
                        self.active_segment_state.write_offset = write_offset;
                        self.active_segment_state.min_lsn = Some(
                            self.active_segment_state
                                .min_lsn
                                .map_or(lsn, |first| first.min(lsn)),
                        );
                        self.active_segment_state.max_lsn = Some(
                            self.active_segment_state
                                .max_lsn
                                .map_or(lsn, |last| last.max(lsn)),
                        );
                    }

                    failure_context = "encode store WAL mutation";
                    let store_mutation = match op.blob_mutation(self.config.lsm_partition_count)? {
                        Some(mutation) => {
                            lsm_writes.push((lsn, mutation.clone()));
                            StoreWalMutation::Blob(mutation)
                        }
                        None => match op {
                            PreparedBatchOp::EpochChange { epoch, .. } => {
                                StoreWalMutation::Epoch { epoch: *epoch }
                            }
                            _ => {
                                unreachable!("only epoch changes are RocksDB-only batch operations")
                            }
                        },
                    };
                    failure_context = "encode store WAL record";
                    wal_entries.push(WalEntry {
                        lsn,
                        payload: store_mutation.encode()?,
                    });
                    prepared.result.op_lsns.push(lsn);
                }
            }

            failure_context = "store WAL append";
            self.wal.append(&wal_entries)?;
            failure_context = "blob LSM apply";
            let result = self.lsm.write_batch(lsm_writes)?;
            Ok(!result.rolled_memtables.is_empty())
        })();

        let rolled_memtable = match write_result {
            Ok(rolled_memtable) => rolled_memtable,
            Err(error) => {
                self.halt_submit_batch_failure(
                    failure_context,
                    &error,
                    appended_records,
                    appended_bytes,
                );
                self.fail_batch_group(prepared_batches, started);
                return;
            }
        };

        let pending_rollovers = self.take_pending_rollovers();
        let commit_started = Instant::now();
        let commit_result = self.commit_write_group(
            &pending_rollovers,
            prepared_batches.iter().map(|(_, prepared, _)| prepared),
            next_lsn.expect("a non-empty prepared group has a next LSN"),
        );
        let commit_elapsed = commit_started.elapsed();
        for (_, _, profile) in &mut prepared_batches {
            if let Some(profile) = profile {
                profile.index_batch_commit += commit_elapsed;
            }
        }
        if let Err(error) = commit_result {
            self.halt_submit_batch_failure(
                "index batch commit",
                &error,
                appended_records,
                appended_bytes,
            );
            self.fail_batch_group(prepared_batches, started);
            return;
        }
        self.metrics.record_commit_group(prepared_batches.len());

        self.request_lsm_flush(rolled_memtable);
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
            self.durable_offset,
        );
        self.metrics
            .set_next_lsn(next_lsn.expect("a non-empty prepared group has a next LSN"));
        if let Some(epoch) = prepared_batches
            .iter()
            .rev()
            .find_map(|(_, prepared, _)| prepared.result.last_epoch())
        {
            self.metrics.set_current_epoch(epoch);
        }

        let mut segment_bytes = 0_u64;
        for ((request, prepared, profile), batch_put_metrics) in
            prepared_batches.iter_mut().zip(put_metrics)
        {
            for metric in batch_put_metrics {
                segment_bytes = segment_bytes.saturating_add(metric.record_bytes);
                self.metrics.record_put(Ok(metric), started.elapsed());
            }
            for _ in prepared
                .ops
                .iter()
                .filter(|op| matches!(op, PreparedBatchOp::Tombstone { .. }))
            {
                self.metrics.record_delete(true, started.elapsed());
            }
            let _ = profile_phase(
                profile.as_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || request.response_tx.send(Ok(prepared.result.clone())),
            );
        }
        if let Err(error) = self.note_uncommitted_write(segment_bytes) {
            self.halt_writer_error("schedule grouped batch durability", &error);
        }
        for (request, _, profile) in prepared_batches {
            if let Some(mut profile) = profile {
                profile.writer_total = started.elapsed();
                request.profile.send(profile);
            }
        }
    }

    /// Delivers the collective failure: every caller whose batch survived preparation gets the
    /// same answer.
    ///
    /// By the time this runs the writer has already halted, so each response carries the stored
    /// halt reason — the context string naming the exact sub-step that failed — rather than a
    /// per-batch error; a generic StoreHalted stands in if the reason is somehow missing. Put and
    /// tombstone metrics record failure for every op that had been promised, and profiles are
    /// still delivered: the slow path stays observable precisely when observability matters most.
    fn fail_batch_group(
        &self,
        prepared_batches: Vec<(BatchWriteRequest, PreparedBatch, Option<StoreWriteProfile>)>,
        started: Instant,
    ) {
        for (request, prepared, mut profile) in prepared_batches {
            let error = self
                .store_halt
                .error()
                .unwrap_or_else(|| Error::StoreHalted {
                    reason: "writer failed during grouped batch commit".to_owned(),
                });
            let _ = profile_phase(
                profile.as_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || request.response_tx.send(Err(error)),
            );
            for _ in prepared
                .ops
                .iter()
                .filter(|op| matches!(op, PreparedBatchOp::Put { .. }))
            {
                self.metrics.record_put(Err(()), started.elapsed());
            }
            for _ in prepared
                .ops
                .iter()
                .filter(|op| matches!(op, PreparedBatchOp::Tombstone { .. }))
            {
                self.metrics.record_delete(false, started.elapsed());
            }
            if let Some(mut profile) = profile {
                profile.writer_total = started.elapsed();
                request.profile.send(profile);
            }
        }
    }

    /// Halts after a mid-group failure, first recording how many bytes were left orphaned.
    ///
    /// "Orphaned" means appended to the active segment during this group but never acknowledged
    /// by a committed RocksDB batch — the index still ends before them. They are not a
    /// correctness problem (recovery's unsealed-segment scan re-derives the true frontier and
    /// discards them), but they are disk and recovery-time exposure, and the metric sizes exactly
    /// that.
    fn halt_submit_batch_failure(
        &self,
        context: &str,
        error: &Error,
        appended_records: u64,
        appended_bytes: u64,
    ) {
        self.metrics
            .record_orphaned_segment_bytes(appended_records, appended_bytes);
        self.halt_writer_error(context, error);
    }

    /// Marks the store terminally failed, stamping the failing sub step into the reason every
    /// later caller will see. There is no un-halt: the writer's in-memory view can no longer be
    /// trusted to match disk, and only a restart's recovery may reconcile the two.
    pub(crate) fn halt_writer_error(&self, context: &str, error: &Error) {
        self.store_halt.halt(format!(
            "fatal strata writer error during {context}: {error}"
        ));
    }

    /// Validates a client batch and assigns its contiguous store-owned LSN range.
    /// This translates each `BatchOp` into a `PreparedBatchOp`, it touches no physical state.
    /// `Put`: resolves the shard id to its active generation via `openable_shard_key`, computes
    /// the exact encoded size upfront and rejects the payloads that could never fit in any single segment.
    /// It also stamps the op with the current epoch of the system.
    /// `SetBlobLifetime`: requires the target epoch to be strictly in future. `logical_end_epoch <= current` is
    /// rejected as `InvalidBlobLifetime` error.
    /// `Tombstone`: just needs a shard resolved.
    /// `IncrementEpoch`: bumps the epoch in the current batch which means later ops in the same batch
    /// will see the new epoch.
    ///
    /// Group threading: the caller passes the group's running (next_lsn, current_epoch) cursor and
    /// receives the advanced pair back — in the example, batch X takes (100, e) and returns
    /// (102, e+1), which is exactly what batch Y is prepared with, so Y's ops see X's epoch bump
    /// without any RocksDB read between them. Because this function touches no physical state, a
    /// rejection costs nothing: the caller simply keeps its previous cursor values and the failed
    /// batch leaves no trace in the LSN sequence.
    fn prepare_batch(
        &self,
        ops: Vec<BatchOp>,
        mut next_lsn: StrataLsn,
        mut current_epoch: Option<Epoch>,
    ) -> Result<(PreparedBatch, StrataLsn, Option<Epoch>)> {
        let mut prepared_ops = Vec::with_capacity(ops.len());
        let mut op_epochs = Vec::with_capacity(ops.len());

        for op in ops {
            let lsn = next_lsn;
            next_lsn = next_lsn
                .checked_add(1)
                .ok_or(segment::Error::RangeOverflow)?;
            match op {
                BatchOp::Put {
                    shard_id,
                    key,
                    payload,
                } => {
                    // Why do we care about the current epoch here?
                    // The reason to remember the epoch at which the put was submitted is to
                    // ensure that this put's visibility can be judged by LSM compaction later on.
                    // Imagine if this was the sequence:
                    // current_epoch = 10
                    // LSN 100: SetLifetime { logical_end_epoch: 50 }
                    // LSN 101: Put { key: "foo", payload: "bar" }
                    // LSN 102: Put { key: "foo", payload: "baz" }
                    // LSN 103: ChangeEpoch { epoch: 50 }
                    // LSN 104: Put { key: "baz", payload: "qux"}
                    // The first Put has no explicit lifecycle, but the key already has an explicit
                    // lifecycle ending at epoch 50. Since 50 > current_epoch(10), compaction lets
                    // the new physical record inherit that lifecycle. GC then knows the bytes
                    // for "bar" belong in the "expires at 50" segment.
                    // Later when the second Put comes along at epoch < 50, compaction lets it
                    // inherit the lifecycle of the key, and the bytes for "baz" belong in the
                    // "expires at 50" segment. The bytes for "bar" at this point are eligible for
                    // garbage collection since the key is overwritten.
                    // Subsequently epoch advances to 50 and the final Put at LSN 104 happens and
                    // if do not record the current epoch at which this put was submitted, then
                    // compaction would not know that the bytes for "qux" should not inherit
                    // an expired lifetime. It would think that the bytes for "qux" should belong
                    // in the "expires at 50" segment (Important thing to know is that compaction
                    // does not know about the epoch change as it is not a key based operation)
                    let current_epoch = current_epoch.ok_or(Error::EpochNotInitialized)?;
                    let shard = self.openable_shard_key(shard_id)?;
                    let record_bytes =
                        encoded_record_len(&key, payload.len()).map_err(segment::Error::from)?;
                    if record_bytes > self.config.segment_max_bytes {
                        return Err(segment::Error::SegmentFull {
                            max_size: self.config.segment_max_bytes,
                            attempted_size: record_bytes,
                        }
                        .into());
                    }
                    prepared_ops.push(PreparedBatchOp::Put {
                        shard,
                        key,
                        payload,
                        lsn,
                        current_epoch,
                        record_ref: None,
                        record_bytes,
                    });
                    op_epochs.push(None);
                }
                BatchOp::SetBlobLifetime {
                    key,
                    logical_end_epoch,
                } => {
                    let epoch = current_epoch.ok_or(Error::EpochNotInitialized)?;
                    if logical_end_epoch <= epoch {
                        return Err(Error::InvalidBlobLifetime {
                            logical_end_epoch,
                            current_epoch: epoch,
                        });
                    }
                    prepared_ops.push(PreparedBatchOp::Lifecycle {
                        key,
                        lsn,
                        logical_end_epoch,
                        current_epoch: epoch,
                    });
                    op_epochs.push(None);
                }
                BatchOp::Tombstone { shard_id, key } => {
                    let shard = self.openable_shard_key(shard_id)?;
                    prepared_ops.push(PreparedBatchOp::Tombstone { shard, key, lsn });
                    op_epochs.push(None);
                }
                BatchOp::Relocate {
                    key,
                    shard,
                    payload_lsn,
                    to,
                } => {
                    // GC validated the shard generation at publish, and the mutation is
                    // conditional on the exact payload version, so a stale relocation folds to a
                    // no-op rather than failing the batch.
                    prepared_ops.push(PreparedBatchOp::Relocate {
                        key,
                        shard,
                        payload_lsn,
                        to,
                        lsn,
                    });
                    op_epochs.push(None);
                }
                BatchOp::IncrementEpoch => {
                    let next_epoch = current_epoch
                        .ok_or(Error::EpochNotInitialized)?
                        .checked_add(1)
                        .ok_or(segment::Error::RangeOverflow)?;
                    current_epoch = Some(next_epoch);
                    prepared_ops.push(PreparedBatchOp::EpochChange {
                        lsn,
                        epoch: next_epoch,
                    });
                    op_epochs.push(Some(next_epoch));
                }
            }
        }

        Ok((
            PreparedBatch {
                result: BatchWriteResult {
                    op_lsns: Vec::with_capacity(prepared_ops.len()),
                    op_epochs,
                },
                ops: prepared_ops,
            },
            next_lsn,
            current_epoch,
        ))
    }

    /// Temporarily removes staged rollover metadata so it can be included in the current durable
    /// index batch exactly once.
    pub(crate) fn take_pending_rollovers(&mut self) -> Vec<PendingRollover> {
        std::mem::take(&mut self.pending_rollovers)
    }

    /// Commits the index side of a group of prepared batches.
    ///
    /// This assembles one atomic RocksDB write:
    ///
    /// 1. Any pending rollover rows are part of the batch i.e. old segments flipped to `Sealing` state
    ///    and new segments are registered and published at LSN.
    /// 2. For epoch change, the `epoch_changes[lsn]` history row and the `current_epoch` pointer.
    /// 3. The updated active segment state row but only if op actually wrote payload bytes
    /// 4. `next_lsn = last committed lsn + 1`
    ///
    /// A rollover during this write stages its metadata in `self.pending_rollovers`, so the active
    /// segment change commits atomically with the records that reference it.
    ///
    /// Two details are easy to miss. The active-segment row is written only when some op actually
    /// appended payload bytes (`wrote_payload`) — a group of pure metadata ops (epoch changes,
    /// tombstones, lifetimes) must not churn the segment row, and in particular must not
    /// re-publish min/max LSN bounds that did not move. And the batch is written *without* sync:
    /// this is the commit point for visibility and ordering, not for durability — per the module
    /// doc, crash-proofness arrives later via the pressure-driven durability publish, and callers
    /// that need it gate on published_lsn. One batch for the whole group also means the group is
    /// all-or-nothing at the metadata level: either every caller's LSNs exist in the index or
    /// none do.
    fn commit_write_group<'a>(
        &self,
        pending_rollovers: &[PendingRollover],
        prepared_batches: impl IntoIterator<Item = &'a PreparedBatch>,
        next_lsn: StrataLsn,
    ) -> Result<()> {
        let mut batch = self.index.batch();
        for rollover in pending_rollovers {
            rollover.apply_batch(&self.index, &mut batch)?;
        }

        let mut wrote_payload = false;
        for prepared in prepared_batches {
            for op in &prepared.ops {
                match op {
                    PreparedBatchOp::Put { .. } => {
                        wrote_payload = true;
                    }
                    PreparedBatchOp::Lifecycle { .. }
                    | PreparedBatchOp::Tombstone { .. }
                    | PreparedBatchOp::Relocate { .. } => {}
                    PreparedBatchOp::EpochChange { lsn, epoch } => {
                        self.index
                            .put_epoch_change_batch(&mut batch, *lsn, *epoch)?;
                        self.index.put_current_epoch_batch(&mut batch, *epoch)?;
                    }
                }
            }
        }
        if wrote_payload {
            self.index
                .put_segment_state_batch(&mut batch, &self.active_segment_state)?;
        }
        self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
        batch.write().map_err(index::Error::from)?;
        Ok(())
    }

    /// Returns the active generation key for a shard that can accept writes.
    ///
    /// A stale writer that only knows `shard_id` must not write into a shard
    /// after it has been dropped and recreated. This forces every put to use the current generation
    /// stored in the registry.
    fn openable_shard_key(&self, shard_id: ShardId) -> Result<ShardKey> {
        match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => Ok(info.key(shard_id)),
            Some(info) => Err(Error::ShardUnavailable {
                shard_id,
                generation: info.current_generation,
                current_generation: info.current_generation,
                state: info.state,
            }),
            None => Err(Error::ShardNotFound { shard_id }),
        }
    }
}
