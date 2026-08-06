//! Foreground batch commit path: batch preparation, segment append, and the
//! atomic index metadata commit.
//! One writer thread, one LSN sequence: prepare_batch reserves a contiguous range, so a batch's ops can never interleave with another writer's.
//! Fixed publication order — segment bytes → store WAL → LSM memtable → RocksDB batch — and the RocksDB batch is the commit point.
//! Committed ≠ durable: when the caller gets its LSN back, the write is visible and ordered, but only a later sync (or the periodic durability publish) makes it crash-proof. Callers needing durability gate on published_lsn() >= lsn

use std::sync::mpsc;

use strata_core::{ShardId, ShardKey, encoded_record_len};

use crate::{
    BatchOp, BatchWriteResult, Error, PendingRollover, PreparedBatch, PreparedBatchOp, Result,
    StoreWriteProfile, WriteCoordinator, metrics::PutMetric, profile_phase, wal::WalEntry,
    wal_format::StoreWalMutation,
};

impl WriteCoordinator {
    /// Commits one foreground batch in segment, WAL, LSM, then RocksDB order.
    ///
    /// Preparation assigns a contiguous LSN range. Payload operations append to the active segment,
    /// rolling it first when needed. Every operation is appended to the store WAL, keyed mutations
    /// enter the LSM, and the RocksDB batch atomically publishes the logical metadata plus any
    /// staged rollover rows. After physical writes begin, a failure halts the writer for recovery.
    pub(crate) fn submit_batch(
        &mut self,
        ops: Vec<BatchOp>,
        response_tx: mpsc::Sender<Result<BatchWriteResult>>,
        mut profile: Option<&mut StoreWriteProfile>,
    ) -> std::result::Result<(BatchWriteResult, Vec<PutMetric>), ()> {
        if ops.is_empty() {
            let result = BatchWriteResult::default();
            let _ = profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || response_tx.send(Ok(result.clone())),
            );
            return Ok((result, Vec::new()));
        }

        let mut prepared = match profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.prepare_batch += elapsed,
            || self.prepare_batch(ops),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.response_send += elapsed,
                    || response_tx.send(Err(error)),
                );
                return Err(());
            }
        };

        let mut appended_records = 0_u64;
        let mut appended_bytes = 0_u64;
        let mut put_metrics = Vec::new();
        let mut lsm_writes = Vec::new();
        let mut wal_entries = Vec::with_capacity(prepared.ops.len());
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
                let append = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.segment_append += elapsed,
                    || {
                        if self.segment.ensure_capacity(*record_bytes).is_err() {
                            self.rollover_active_segment(lsn)?;
                        }
                        self.segment
                            .append_for_shard(key, lsn, *shard, payload)
                            .map_err(Error::from)
                    },
                );
                let written = match append {
                    Ok(written) => written,
                    Err(error) => {
                        self.halt_submit_batch_failure(
                            "payload segment append",
                            &error,
                            appended_records,
                            appended_bytes,
                        );
                        let _ = response_tx.send(Err(error));
                        return Err(());
                    }
                };
                if written.record_len != *record_bytes {
                    let error = Error::InvariantViolation {
                        reason: format!(
                            "segment put at LSN {lsn} wrote {}, expected {record_bytes}",
                            written.record_len
                        ),
                    };
                    self.halt_submit_batch_failure(
                        "payload segment length",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
                *record_ref = Some(written.record_ref);
                let Some(write_offset) = written.record_ref.end_offset() else {
                    let error = Error::InvariantViolation {
                        reason: format!("record reference at LSN {lsn} overflows its segment"),
                    };
                    self.halt_submit_batch_failure(
                        "advance active segment offset",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                };
                appended_records = appended_records.saturating_add(1);
                appended_bytes = appended_bytes.saturating_add(written.record_ref.len);
                self.active_allocation_records = self.active_allocation_records.saturating_add(1);
                put_metrics.push(PutMetric {
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

            let store_mutation = match op.blob_mutation(self.config.lsm_partition_count) {
                Ok(Some(mutation)) => {
                    lsm_writes.push((lsn, mutation.clone()));
                    StoreWalMutation::Blob(mutation)
                }
                Ok(None) => match op {
                    PreparedBatchOp::EpochChange { epoch, .. } => {
                        StoreWalMutation::Epoch { epoch: *epoch }
                    }
                    _ => unreachable!("only epoch changes are RocksDB-only batch operations"),
                },
                Err(error) => {
                    self.halt_submit_batch_failure(
                        "encode store WAL mutation",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
            };
            let payload = match store_mutation.encode() {
                Ok(payload) => payload,
                Err(error) => {
                    self.halt_submit_batch_failure(
                        "encode store WAL record",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
            };
            wal_entries.push(WalEntry { lsn, payload });
            prepared.result.op_lsns.push(lsn);
        }

        if let Err(error) = self.wal.append(&wal_entries) {
            self.halt_submit_batch_failure(
                "store WAL append",
                &error,
                appended_records,
                appended_bytes,
            );
            let _ = response_tx.send(Err(error));
            return Err(());
        }
        let lsm_write = match self.lsm.write_batch(lsm_writes) {
            Ok(result) => result,
            Err(error) => {
                let error = Error::from(error);
                self.halt_submit_batch_failure(
                    "blob LSM apply",
                    &error,
                    appended_records,
                    appended_bytes,
                );
                let _ = response_tx.send(Err(error));
                return Err(());
            }
        };
        let rolled_memtable = !lsm_write.rolled_memtables.is_empty();

        let pending_rollovers = self.take_pending_rollovers();
        let commit_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.index_batch_commit += elapsed,
            || self.commit_write_batch(&pending_rollovers, &prepared),
        );
        if let Err(error) = commit_result {
            self.halt_submit_batch_failure(
                "index batch commit",
                &error,
                appended_records,
                appended_bytes,
            );
            let _ = profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || response_tx.send(Err(error)),
            );
            return Err(());
        }
        self.request_lsm_flush(rolled_memtable);
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
            self.durable_offset,
        );
        let result = prepared.result;
        let _ = profile_phase(
            profile,
            |profile, elapsed| profile.response_send += elapsed,
            || response_tx.send(Ok(result.clone())),
        );
        Ok((result, put_metrics))
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
    fn prepare_batch(&self, ops: Vec<BatchOp>) -> Result<PreparedBatch> {
        let mut prepared_ops = Vec::with_capacity(ops.len());
        let mut op_epochs = Vec::with_capacity(ops.len());
        let mut current_epoch = self.index.get_current_epoch()?;
        let mut next_lsn = self.index.get_next_lsn()?;

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

        Ok(PreparedBatch {
            result: BatchWriteResult {
                op_lsns: Vec::with_capacity(prepared_ops.len()),
                op_epochs,
            },
            ops: prepared_ops,
        })
    }

    /// Temporarily removes staged rollover metadata so it can be included in the current durable
    /// index batch exactly once.
    pub(crate) fn take_pending_rollovers(&mut self) -> Vec<PendingRollover> {
        std::mem::take(&mut self.pending_rollovers)
    }

    /// Restores rollover metadata when a non-foreground metadata publish fails before committing.
    ///
    /// Foreground `submit_batch` failures after physical writer work starts are fatal instead.
    pub(crate) fn restore_pending_rollovers(&mut self, pending_rollovers: Vec<PendingRollover>) {
        self.pending_rollovers = pending_rollovers;
    }

    /// Commits the index side of a prepared batch.
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
    fn commit_write_batch(
        &self,
        pending_rollovers: &[PendingRollover],
        prepared: &PreparedBatch,
    ) -> Result<()> {
        let mut batch = self.index.batch();
        for rollover in pending_rollovers {
            rollover.apply_batch(&self.index, &mut batch)?;
        }

        let mut wrote_payload = false;
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
        if wrote_payload {
            self.index
                .put_segment_state_batch(&mut batch, &self.active_segment_state)?;
        }
        let next_lsn = prepared
            .result
            .last_lsn()
            .and_then(|lsn| lsn.checked_add(1))
            .ok_or(strata_segment::Error::RangeOverflow)?;
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
