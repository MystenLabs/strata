//! The write protocol between `StrataStore` and the writer thread: command and
//! request types, the public batch builder and its result/profile types, and the
//! prepared-batch forms the writer commits.

use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use core_types::{
    BlobKey, Epoch, RecordRef, SegmentId, SegmentState, ShardId, ShardKey, StrataLsn,
};
use index::StrataIndex;
use lsm::Mutation as LsmMutation;

use crate::{Error, Result, StrataStore, blob_lsm::BlobMutation, partition::partition_for_key};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum WriteCommand {
    AddShard(AddShardRequest),
    Batch(BatchWriteRequest),
    DropShard(DropShardRequest),
    Sync(SyncRequest),
    SyncDone,
    Shutdown,
}

#[derive(Debug)]
pub(crate) struct AddShardRequest {
    pub(crate) shard_id: ShardId,
    pub(crate) response_tx: mpsc::Sender<Result<ShardKey>>,
}

#[derive(Debug)]
pub(crate) struct ProfileRequest<P> {
    enqueued_at: Option<Instant>,
    tx: Option<mpsc::Sender<P>>,
}

impl<P> Default for ProfileRequest<P> {
    fn default() -> Self {
        Self {
            enqueued_at: None,
            tx: None,
        }
    }
}

impl<P> ProfileRequest<P> {
    pub(crate) fn enabled(tx: mpsc::Sender<P>) -> Self {
        Self {
            enqueued_at: Some(Instant::now()),
            tx: Some(tx),
        }
    }

    fn queue_wait(&self, started: Instant) -> Option<Duration> {
        self.tx
            .as_ref()
            .map(|_| started.saturating_duration_since(self.enqueued_at.unwrap_or(started)))
    }

    pub(crate) fn send(self, profile: P) {
        if let Some(tx) = self.tx {
            let _ = tx.send(profile);
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchWriteRequest {
    pub(crate) ops: Vec<BatchOp>,
    pub(crate) response_tx: mpsc::Sender<Result<BatchWriteResult>>,
    pub(crate) profile: ProfileRequest<StoreWriteProfile>,
}

#[derive(Debug)]
pub(crate) struct DropShardRequest {
    pub(crate) shard_id: ShardId,
    pub(crate) response_tx: mpsc::Sender<Result<ShardKey>>,
}

#[derive(Debug)]
pub(crate) struct SyncRequest {
    pub(crate) response_tx: mpsc::Sender<Result<()>>,
    pub(crate) profile: ProfileRequest<StoreSyncProfile>,
}

#[derive(Debug)]
pub(crate) enum BatchOp {
    Put {
        shard_id: ShardId,
        key: BlobKey,
        payload: Arc<[u8]>,
    },
    SetBlobLifetime {
        key: BlobKey,
        logical_end_epoch: Epoch,
    },
    Tombstone {
        shard_id: ShardId,
        key: BlobKey,
    },
    IncrementEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchWriteResult {
    pub(crate) op_lsns: Vec<StrataLsn>,
    pub(crate) op_epochs: Vec<Option<Epoch>>,
}

impl BatchWriteResult {
    /// LSNs in the same order as the submitted operations.
    ///
    /// Callers should not infer "the next operation is previous + 1" after
    /// a failed or empty batch. The writer is the source of truth for what actually committed.
    pub fn op_lsns(&self) -> &[StrataLsn] {
        &self.op_lsns
    }

    /// Epoch outputs in operation order; non-epoch operations have `None`.
    ///
    /// Mixed batches need to know which op advanced the epoch. Returning a
    /// single final epoch would make `put, increment, put` ambiguous to callers recording fences.
    pub fn op_epochs(&self) -> &[Option<Epoch>] {
        &self.op_epochs
    }

    pub fn epoch_for_op(&self, index: usize) -> Option<Epoch> {
        self.op_epochs.get(index).copied().flatten()
    }

    pub fn first_lsn(&self) -> Option<StrataLsn> {
        self.op_lsns.first().copied()
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.op_lsns.last().copied()
    }

    pub fn last_epoch(&self) -> Option<Epoch> {
        self.op_epochs.iter().rev().find_map(|epoch| *epoch)
    }
}

/// Temporary write-path timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreWriteProfile {
    /// Time spent blocked in the client-side bounded queue send.
    pub queue_send: Duration,
    /// Time from client submission until the writer starts the command, excluding `queue_send`.
    pub queue_wait: Duration,
    pub prepare_batch: Duration,
    pub segment_capacity: Duration,
    pub segment_append: Duration,
    pub index_batch_commit: Duration,
    pub response_send: Duration,
    pub writer_total: Duration,
}

/// Temporary sync-path timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreSyncProfile {
    /// Time spent blocked in the client-side bounded queue send.
    pub queue_send: Duration,
    /// Time from client submission until the writer starts the command, excluding `queue_send`.
    pub queue_wait: Duration,
    pub segment_sync: Duration,
    /// Includes durable segment state assembly, published LSN computation, and batch construction.
    pub published_lsn_compute: Duration,
    /// RocksDB batch commit with synchronous WAL durability.
    pub index_batch_commit: Duration,
    pub state_update: Duration,
    pub response_send: Duration,
    pub writer_total: Duration,
}

#[cfg(feature = "internal-profiling")]
pub trait StoreProfileSink: Send + Sync + std::fmt::Debug {
    fn record_write(&self, profile: StoreWriteProfile);
    fn record_sync(&self, profile: StoreSyncProfile);
}

impl ProfileRequest<StoreWriteProfile> {
    pub(crate) fn begin(&self, started: Instant) -> Option<StoreWriteProfile> {
        self.queue_wait(started)
            .map(|queue_wait| StoreWriteProfile {
                queue_wait,
                ..StoreWriteProfile::default()
            })
    }
}

impl ProfileRequest<StoreSyncProfile> {
    pub(crate) fn begin(&self, started: Instant) -> Option<StoreSyncProfile> {
        self.queue_wait(started).map(|queue_wait| StoreSyncProfile {
            queue_wait,
            ..StoreSyncProfile::default()
        })
    }
}

pub(crate) fn profile_phase<P, T>(
    profile: Option<&mut P>,
    record_elapsed: impl FnOnce(&mut P, Duration),
    action: impl FnOnce() -> T,
) -> T {
    let Some(profile) = profile else {
        return action();
    };

    let started = Instant::now();
    let result = action();
    record_elapsed(profile, started.elapsed());
    result
}

#[derive(Debug)]
pub struct StrataBatch<'a> {
    pub(crate) store: &'a StrataStore,
    pub(crate) ops: Vec<BatchOp>,
}

impl<'a> StrataBatch<'a> {
    /// Adds a payload write to this batch.
    ///
    /// Batching submits all operations as one writer command. That keeps
    /// `put, tombstone` in one batch from being interleaved by another writer between the two
    /// operations.
    pub fn put(
        &mut self,
        shard_id: ShardId,
        key: BlobKey,
        payload: impl Into<Arc<[u8]>>,
    ) -> &mut Self {
        self.ops.push(BatchOp::Put {
            shard_id,
            key,
            payload: payload.into(),
        });
        self
    }

    /// Adds a metadata only lifetime update to this batch.
    ///
    /// When a lifetime change is batched with other ops, it shares the same
    /// contiguous LSN reservation. Otherwise a concurrent tombstone could slip between the caller's
    /// payload write and its lifetime update.
    pub fn set_blob_lifetime(&mut self, key: BlobKey, logical_end_epoch: Epoch) -> &mut Self {
        self.ops.push(BatchOp::SetBlobLifetime {
            key,
            logical_end_epoch,
        });
        self
    }

    /// Adds a shard-scoped tombstone to this batch.
    ///
    /// Tombstones remain ordered relative to any preceding puts in the same
    /// batch. Without this, deleting a key after writing a replacement could race with another put
    /// and hide the wrong version.
    pub fn tombstone(&mut self, shard_id: ShardId, key: BlobKey) -> &mut Self {
        self.ops.push(BatchOp::Tombstone { shard_id, key });
        self
    }

    /// Adds an epoch increment to this batch.
    ///
    /// Epoch changes are treated like logical operations. A batch such as
    /// `put A, increment epoch, put B` must replay exactly that order after crash recovery so A and
    /// B do not end up in the same logical epoch.
    pub fn increment_epoch(&mut self) -> &mut Self {
        self.ops.push(BatchOp::IncrementEpoch);
        self
    }

    /// Submits the accumulated operations to the writer.
    ///
    /// The batch is consumed on write, so callers cannot accidentally submit
    /// the same prepared operations twice and create duplicate records with new LSNs.
    pub fn write(self) -> Result<BatchWriteResult> {
        self.store.write_batch(self.ops)
    }
}

#[derive(Debug)]
pub(crate) struct PreparedBatch {
    pub(crate) result: BatchWriteResult,
    pub(crate) ops: Vec<PreparedBatchOp>,
}

#[derive(Debug)]
pub(crate) enum PreparedBatchOp {
    Put {
        shard: ShardKey,
        key: BlobKey,
        payload: Arc<[u8]>,
        lsn: StrataLsn,
        current_epoch: Epoch,
        record_ref: Option<RecordRef>,
        record_bytes: u64,
    },
    Lifecycle {
        key: BlobKey,
        lsn: StrataLsn,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    Tombstone {
        shard: ShardKey,
        key: BlobKey,
        lsn: StrataLsn,
    },
    EpochChange {
        lsn: StrataLsn,
        epoch: Epoch,
    },
}

impl PreparedBatchOp {
    pub(crate) fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Lifecycle { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::EpochChange { lsn, .. } => *lsn,
        }
    }

    pub(crate) fn blob_mutation(&self, partition_count: u32) -> Result<Option<LsmMutation>> {
        match self {
            Self::Put {
                shard,
                key,
                current_epoch,
                record_ref,
                ..
            } => Ok(Some(LsmMutation::PutBlob {
                partition: partition_for_key(key.as_bytes(), partition_count),
                key: key.as_bytes().to_vec(),
                metadata: BlobMutation::encode_put_metadata(*shard, *current_epoch),
                record_ref: record_ref.ok_or_else(|| Error::InvariantViolation {
                    reason: format!("payload segment reference is missing at LSN {}", self.lsn()),
                })?,
            })),
            Self::Lifecycle {
                key,
                logical_end_epoch,
                current_epoch,
                ..
            } => Ok(Some(LsmMutation::Put {
                partition: partition_for_key(key.as_bytes(), partition_count),
                key: key.as_bytes().to_vec(),
                value: BlobMutation::SetLifetime {
                    logical_end_epoch: *logical_end_epoch,
                    current_epoch: *current_epoch,
                }
                .encode_inline()?,
            })),
            Self::Tombstone { shard, key, .. } => Ok(Some(LsmMutation::Put {
                partition: partition_for_key(key.as_bytes(), partition_count),
                key: key.as_bytes().to_vec(),
                value: BlobMutation::Tombstone { shard: *shard }.encode_inline()?,
            })),
            Self::EpochChange { .. } => Ok(None),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingRollover {
    pub(crate) old_segment_state: SegmentState,
    pub(crate) new_segment_id: SegmentId,
}

impl PendingRollover {
    /// Adds the old-segment `Sealing` row and the new segment's publication marker to a write
    /// batch. The same batch writes the final active segment state; if another rollover followed
    /// this one, that rollover writes this segment's `Sealing` state instead.
    ///
    /// Rollover metadata must commit atomically with the writer metadata batch that first publishes
    /// later segment or LSN state.
    pub(crate) fn apply_batch(
        &self,
        index: &StrataIndex,
        batch: &mut typed_store::rocks::DBBatch,
    ) -> Result<()> {
        let published_at_lsn =
            self.old_segment_state
                .sealed_before_lsn
                .ok_or_else(|| Error::InvariantViolation {
                    reason: format!(
                        "rollover from segment {} has no sealed-before LSN",
                        self.old_segment_state.segment_id
                    ),
                })?;
        index.put_segment_state_batch(batch, &self.old_segment_state)?;
        index.put_segment_published_at_lsn_batch(batch, self.new_segment_id, published_at_lsn)?;
        Ok(())
    }
}
