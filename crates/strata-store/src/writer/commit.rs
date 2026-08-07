//! Foreground batch commit path: batch preparation, segment append, and the
//! atomic index metadata commit.
//! One writer thread, one LSN sequence: prepare_batch reserves a contiguous range, so a batch's ops can never interleave with another writer's.
//! Fixed publication order — segment bytes → store WAL → LSM memtable → RocksDB batch — and the RocksDB batch is the commit point.
//! Committed ≠ durable: when the caller gets its LSN back, the write is visible and ordered, but only a later sync (or the periodic durability publish) makes it crash-proof. Callers needing durability gate on published_lsn() >= lsn

use std::time::Instant;

use strata_core::{Epoch, ShardId, ShardKey, StrataLsn, encoded_record_len};

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
        if let Err(error) = self.note_committed_write(segment_bytes) {
            self.halt_writer_error("schedule grouped batch durability", &error);
        }
        for (request, _, profile) in prepared_batches {
            if let Some(mut profile) = profile {
                profile.writer_total = started.elapsed();
                request.profile.send(profile);
            }
        }
    }

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
                .ok_or(strata_segment::Error::RangeOverflow)?;
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
                    let record_bytes = encoded_record_len(&key, payload.len())
                        .map_err(strata_segment::Error::from)?;
                    if record_bytes > self.config.segment_max_bytes {
                        return Err(strata_segment::Error::SegmentFull {
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
                BatchOp::IncrementEpoch => {
                    let next_epoch = current_epoch
                        .ok_or(Error::EpochNotInitialized)?
                        .checked_add(1)
                        .ok_or(strata_segment::Error::RangeOverflow)?;
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

    /// Restores rollover metadata when a non-foreground metadata publish fails before committing.
    ///
    /// Foreground batch failures after physical writer work starts are fatal instead.
    pub(crate) fn restore_pending_rollovers(&mut self, pending_rollovers: Vec<PendingRollover>) {
        self.pending_rollovers = pending_rollovers;
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
                    PreparedBatchOp::Lifecycle { .. } | PreparedBatchOp::Tombstone { .. } => {}
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
        batch.write().map_err(strata_index::Error::from)?;
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
