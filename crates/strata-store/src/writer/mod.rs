//! The single-threaded write coordinator loop: command dispatch, scheduled
//! maintenance, and shard add/drop. The heavier commit paths live in sibling
//! files, each holding one `impl WriteCoordinator` block split by concern.

use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

use strata_core::{ShardCleanupJob, ShardCleanupState, ShardId, ShardInfo, ShardKey, ShardState};

use crate::{
    AddShardRequest, BatchOp, BatchWriteRequest, DURABILITY_PUBLISH_INTERVAL, DropShardRequest,
    Error, Result, SEGMENT_ROLLOVER_INTERVAL, SyncRequest, WriteCommand, WriteCoordinator,
    wal::WalEntry, wal_format::StoreWalMutation,
};

mod commit;
mod rollover;
mod sync;

impl WriteCoordinator {
    /// Main compatibility loop for store metadata publication and administrative operations.
    pub(crate) fn run(mut self) {
        loop {
            if self.next_maintenance_timeout().is_zero()
                && let Err(error) = self.process_scheduled_maintenance()
            {
                self.halt_writer_error("scheduled writer maintenance", &error);
            }
            let timeout = self.next_maintenance_timeout();
            let command = match self.write_rx.recv_timeout(timeout) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.process_scheduled_maintenance() {
                        self.halt_writer_error("scheduled writer maintenance", &error);
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            if matches!(command, WriteCommand::Shutdown) {
                break;
            }
            if !matches!(command, WriteCommand::DurabilityReady(_)) {
                self.metrics.dequeue_write_command();
            }
            if let Some(error) = self.store_halt.error()
                && !matches!(command, WriteCommand::DurabilityReady(_))
            {
                Self::send_command_error(command, error);
                continue;
            }
            match command {
                WriteCommand::AddShard(request) => {
                    self.process_add_shard(request);
                }
                WriteCommand::Batch(request) => {
                    self.process_batch(request);
                }
                WriteCommand::DropShard(request) => {
                    self.process_drop_shard(request);
                }
                WriteCommand::RolloverSegment(request) => {
                    let result = self.process_segment_rollover();
                    let _ = request.response_tx.send(result);
                }
                WriteCommand::Sync(request) => {
                    self.process_sync(request);
                }
                WriteCommand::DurabilityReady(ready) => {
                    match self.finish_durability_publish(ready) {
                        Ok((published_lsn, phases)) => {
                            self.complete_sync_requests(published_lsn, &phases);
                            let force = !self.pending_sync_requests.is_empty();
                            if let Err(error) = self.maybe_start_durability_publish(force) {
                                self.halt_writer_error(
                                    "start follow-up durability publication",
                                    &error,
                                );
                                self.fail_pending_sync_requests();
                            }
                        }
                        Err(error) => {
                            self.metrics.record_sync(Err(()), Duration::ZERO);
                            self.metrics.set_durability_pending(
                                self.wal.pending_bytes(),
                                self.pending_segment_bytes,
                                false,
                            );
                            self.halt_writer_error("finish durability publication", &error);
                            self.fail_pending_sync_requests();
                        }
                    }
                }
                WriteCommand::Shutdown => unreachable!("shutdown is handled before dispatch"),
            }
        }
        // A completion callback may be waiting for capacity in the bounded public command queue.
        // Dropping the receiver releases it immediately during shutdown.
        drop(self.write_rx);
    }

    fn next_maintenance_timeout(&self) -> Duration {
        self.next_durability_publish_timeout()
            .min(self.next_segment_rollover_timeout())
    }

    fn next_durability_publish_timeout(&self) -> Duration {
        if self.durability_in_flight_lsn.is_some() {
            return DURABILITY_PUBLISH_INTERVAL;
        }
        self.oldest_unpublished_at
            .map_or(DURABILITY_PUBLISH_INTERVAL, |started| {
                DURABILITY_PUBLISH_INTERVAL.saturating_sub(started.elapsed())
            })
    }

    fn next_segment_rollover_timeout(&self) -> Duration {
        SEGMENT_ROLLOVER_INTERVAL.saturating_sub(self.last_segment_rollover_at.elapsed())
    }

    fn process_scheduled_maintenance(&mut self) -> Result<()> {
        // Rollover first when both clocks expire together. The following publication then fsyncs
        // the store WAL and the RocksDB metadata that installed the replacement active segment.
        if self.next_segment_rollover_timeout().is_zero() {
            self.process_segment_rollover()?;
        }
        if self.next_durability_publish_timeout().is_zero() {
            self.process_scheduled_durability_publish()?;
        }
        Ok(())
    }

    fn process_scheduled_durability_publish(&mut self) -> Result<()> {
        let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        let published_lsn = self.index.get_published_lsn()?;
        if published_lsn > committed_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published LSN {published_lsn} follows committed LSN {committed_lsn}"
                ),
            });
        }
        if published_lsn == committed_lsn {
            self.last_durability_publish_at = Instant::now();
            return Ok(());
        }
        self.maybe_start_durability_publish(true).map(|_| ())
    }

    fn send_command_error(command: WriteCommand, error: Error) {
        match command {
            WriteCommand::AddShard(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::Batch(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::DropShard(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::RolloverSegment(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::Sync(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::DurabilityReady(_) => {}
            WriteCommand::Shutdown => {}
        }
    }

    fn process_add_shard(&mut self, request: AddShardRequest) {
        let result = self.submit_add_shard(request.shard_id);
        let _ = request.response_tx.send(result);
    }

    /// Creates or reactivates a shard generation through the writer queue.
    ///
    /// Drop/re-add must bump generation exactly once. Without this serialized
    /// registry update, one thread could resurrect generation 0 while another has already dropped
    /// it and started generation 1, making old writes visible in the new namespace.
    fn submit_add_shard(&mut self, shard_id: ShardId) -> Result<ShardKey> {
        let info = match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => return Ok(info.key(shard_id)),
            Some(info) if info.is_dropped() => {
                ShardInfo::active(info.current_generation.checked_add(1).ok_or(
                    Error::ShardGenerationOverflow {
                        shard_id,
                        current_generation: info.current_generation,
                    },
                )?)
            }
            Some(info) => {
                return Err(Error::ShardUnavailable {
                    shard_id,
                    generation: info.current_generation,
                    current_generation: info.current_generation,
                    state: info.state,
                });
            }
            None => ShardInfo::active(0),
        };

        let mut batch = self.index.batch();
        self.index
            .put_shard_info_batch(&mut batch, shard_id, info)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        Ok(info.key(shard_id))
    }

    fn process_drop_shard(&mut self, request: DropShardRequest) {
        let pending_wal_bytes = self.wal.pending_bytes();
        let result = self.submit_drop_shard(request.shard_id);
        let committed = result.is_ok();
        let _ = request.response_tx.send(result);
        if committed
            && self.wal.pending_bytes() > pending_wal_bytes
            && let Err(error) = self.note_committed_write(0)
        {
            self.halt_writer_error("schedule shard-drop durability", &error);
        }
    }

    /// Validates that a shard can be dropped and appends the asynchronous registry update.
    ///
    /// Treating "already dropped" as success makes retries idempotent after
    /// caller timeouts. Treating missing shards as success would hide bugs where a caller thinks it
    /// deleted tenant 42 but that tenant was never registered.
    fn submit_drop_shard(&mut self, shard_id: ShardId) -> Result<ShardKey> {
        let Some(info) = self.index.get_shard_info(shard_id)? else {
            return Err(Error::ShardNotFound { shard_id });
        };
        if info.state == ShardState::Dropped {
            return Ok(info.key(shard_id));
        }

        // Writer serialization gives the drop an LSN after every preceding payload transition.
        // It becomes crash-durable at the next ordinary `sync()`; cleanup is gated by that
        // published frontier below.
        let shard = info.key(shard_id);
        self.mark_shard_dropped(shard_id, shard)?;
        Ok(shard)
    }

    /// Stores the drop in the store WAL and RocksDB. No fake LSM row is created.
    fn mark_shard_dropped(&mut self, shard_id: ShardId, shard: ShardKey) -> Result<()> {
        let dropped_info = ShardInfo {
            current_generation: shard.generation,
            state: ShardState::Dropped,
        };
        let drop_lsn = self.index.get_next_lsn()?;
        let next_lsn = drop_lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        self.wal.append(&[WalEntry {
            lsn: drop_lsn,
            payload: StoreWalMutation::ShardDrop { shard }.encode()?,
        }])?;

        let commit_result = (|| {
            let mut batch = self.index.batch();
            self.index
                .put_shard_info_batch(&mut batch, shard_id, dropped_info)?;
            self.index.put_shard_cleanup_job_batch(
                &mut batch,
                ShardCleanupJob {
                    shard,
                    drop_lsn,
                    // Shard-owned files can be removed as a unit as soon as the durable generation
                    // fence is published. Mixed ingest refs are retired later by ordinary blob-LSM
                    // compaction and do not block this bulk cleanup.
                    state: ShardCleanupState::ReadyForGc,
                },
            )?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            batch.write().map_err(strata_index::Error::from)?;
            Ok::<(), Error>(())
        })();

        if let Err(error) = commit_result {
            self.halt_writer_error("shard drop after store-WAL append", &error);
            return Err(error);
        }

        self.metrics.set_next_lsn(next_lsn);
        Ok(())
    }

    /// Handles one client batch and records user visible put metrics.
    ///
    /// Metrics are recorded once per submitted put after the writer knows
    /// whether the batch committed or was rejected during validation. Recording during append would
    /// count a write as successful before the index commit that makes it visible.
    fn process_batch(&mut self, request: BatchWriteRequest) {
        let BatchWriteRequest {
            ops,
            response_tx,
            profile: profile_request,
        } = request;
        let started = Instant::now();
        let put_count = ops
            .iter()
            .filter(|op| matches!(op, BatchOp::Put { .. }))
            .count();
        let mut profile = profile_request.begin(started);
        match self.submit_batch(ops, response_tx, profile.as_mut()) {
            Ok((result, put_metrics)) => {
                let segment_bytes = put_metrics.iter().fold(0_u64, |total, metric| {
                    total.saturating_add(metric.record_bytes)
                });
                for metric in put_metrics {
                    self.metrics.record_put(Ok(metric), started.elapsed());
                }
                if let Some(last_lsn) = result.last_lsn() {
                    self.metrics.set_next_lsn(last_lsn.saturating_add(1));
                }
                if let Some(epoch) = result.last_epoch() {
                    self.metrics.set_current_epoch(epoch);
                }
                if result.last_lsn().is_some()
                    && let Err(error) = self.note_committed_write(segment_bytes)
                {
                    self.halt_writer_error("schedule batch durability", &error);
                }
            }
            Err(()) => {
                for _ in 0..put_count {
                    self.metrics.record_put(Err(()), started.elapsed());
                }
            }
        }
        if let Some(mut profile) = profile {
            profile.writer_total = started.elapsed();
            profile_request.send(profile);
        }
    }

    fn process_sync(&mut self, request: SyncRequest) {
        self.enqueue_sync_request(request);
    }
}
