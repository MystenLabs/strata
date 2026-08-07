//! The single-threaded write coordinator loop: command dispatch, scheduled
//! maintenance, and shard add/drop. The heavier commit paths live in sibling
//! files, each holding one `impl WriteCoordinator` block split by concern.

use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

use strata_core::{ShardCleanupJob, ShardCleanupState, ShardId, ShardInfo, ShardKey, ShardState};

use crate::{
    AddShardRequest, BatchOp, DURABILITY_PUBLISH_INTERVAL, DropShardRequest, Error, Result,
    SEGMENT_ROLLOVER_INTERVAL, SyncRequest, WriteCommand, WriteCoordinator, wal::WalEntry,
    wal_format::StoreWalMutation,
};

mod commit;
mod rollover;
mod sync;

const MAX_GROUPED_BATCHES: usize = 16;
const MAX_GROUPED_OPERATIONS: usize = 256;
const MAX_GROUPED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

impl WriteCoordinator {
    /// Main compatibility loop for store metadata publication and administrative operations.
    pub(crate) fn run(mut self) {
        let mut deferred = None;
        loop {
            self.process_ready_durability();
            if self.next_maintenance_timeout().is_zero()
                && let Err(error) = self.process_scheduled_maintenance()
            {
                self.halt_writer_error("scheduled writer maintenance", &error);
            }
            let timeout = self.next_maintenance_timeout();
            let command = match deferred.take() {
                Some(command) => command,
                None => match self.write_rx.recv_timeout(timeout) {
                    Ok(command) => {
                        if !matches!(command, WriteCommand::DurabilityReady) {
                            self.metrics.dequeue_write_command();
                        }
                        command
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = self.process_scheduled_maintenance() {
                            self.halt_writer_error("scheduled writer maintenance", &error);
                        }
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                },
            };
            if matches!(command, WriteCommand::Shutdown) {
                break;
            }
            if let Some(error) = self.store_halt.error()
                && !matches!(command, WriteCommand::DurabilityReady)
            {
                Self::send_command_error(command, error);
                continue;
            }
            match command {
                WriteCommand::AddShard(request) => {
                    self.process_add_shard(request);
                }
                WriteCommand::Batch(first) => {
                    let mut operation_count = first.ops.len();
                    let mut payload_bytes = first
                        .ops
                        .iter()
                        .filter_map(|op| match op {
                            BatchOp::Put { payload, .. } => Some(payload.len()),
                            _ => None,
                        })
                        .sum::<usize>();
                    let mut requests = vec![first];

                    while requests.len() < MAX_GROUPED_BATCHES {
                        match self.write_rx.try_recv() {
                            Ok(command) => {
                                if !matches!(command, WriteCommand::DurabilityReady) {
                                    self.metrics.dequeue_write_command();
                                }
                                match command {
                                    WriteCommand::Batch(request) => {
                                        let next_operations = request.ops.len();
                                        let next_payload_bytes = request
                                            .ops
                                            .iter()
                                            .filter_map(|op| match op {
                                                BatchOp::Put { payload, .. } => Some(payload.len()),
                                                _ => None,
                                            })
                                            .sum::<usize>();
                                        if operation_count.saturating_add(next_operations)
                                            > MAX_GROUPED_OPERATIONS
                                            || payload_bytes.saturating_add(next_payload_bytes)
                                                > MAX_GROUPED_PAYLOAD_BYTES
                                        {
                                            deferred = Some(WriteCommand::Batch(request));
                                            break;
                                        }
                                        operation_count += next_operations;
                                        payload_bytes += next_payload_bytes;
                                        requests.push(request);
                                    }
                                    command => {
                                        deferred = Some(command);
                                        break;
                                    }
                                }
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    self.process_batch_group(requests);
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
                WriteCommand::DurabilityReady => self.process_ready_durability(),
                WriteCommand::Shutdown => unreachable!("shutdown is handled before dispatch"),
            }
        }
        drop(self.write_rx);
    }

    fn process_ready_durability(&mut self) {
        let Ok(ready) = self.durability_ready_rx.try_recv() else {
            return;
        };
        match self.finish_durability_publish(ready) {
            Ok((published_lsn, phases)) => {
                self.complete_sync_requests(published_lsn, &phases);
                let result = if self.pending_sync_requests.is_empty() {
                    self.maybe_start_durability_publish(false).map(|_| ())
                } else {
                    self.start_durability_publish(true).map(|_| ())
                };
                if let Err(error) = result {
                    self.halt_writer_error("start follow-up durability publication", &error);
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
            WriteCommand::DurabilityReady => {}
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

    fn process_sync(&mut self, request: SyncRequest) {
        self.enqueue_sync_request(request);
    }
}
