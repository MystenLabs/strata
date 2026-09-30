//! Per-blob lifecycle work stored alongside application metadata in RocksDB.
//!
//! A producer stages metadata and queue changes in one batch. A worker takes a snapshot, syncs
//! RocksDB, then processes only that snapshot. Registration cancels earlier ordinary deletes;
//! acknowledgements remove only completed event indexes. Epoch barriers order work across blobs.
//!
//! This module does not apply operations to Strata. The worker must recheck cancellation under
//! the shared blob lock, durably apply effects with replay identities, and drain all preceding
//! work through a barrier's event index before advancing the epoch. Event indexes come from the
//! application; they are not Strata LSNs or a global applied watermark. The application owns
//! reference checks, event ordering and replay filtering, pool fan-out and foreground put coordination.

mod model;
pub use model::*;

use crate::{
    Result,
    port::{
        IndexDb, IndexSnapshot, IndexWriteBatch, TypedMap,
        codec::{encode_key, encode_value},
    },
};
use std::sync::Arc;

pub const PENDING_BLOBS_CF: &str = "strata_pending_blob_ops";
pub const EPOCH_BARRIERS_CF: &str = "strata_pending_epoch_barriers";

/// Open these families with the application database, registering the merge operator on reopen.
pub fn cf_options(mut standard: rocksdb::Options) -> [(&'static str, rocksdb::Options); 2] {
    let barriers = standard.clone();
    standard.set_merge_operator(
        "strata_pending_blob_ops_v1",
        |_, existing, operands| merge_pending(existing, operands).ok(),
        // Cancellation and acknowledgement need the base row. Never partially fold them away.
        |_, _, _| None,
    );
    [(PENDING_BLOBS_CF, standard), (EPOCH_BARRIERS_CF, barriers)]
}

/// Pending work keyed by blob, with epoch barriers keyed by the application's event index.
/// Producers preserve event order for each blob and finish enqueueing all work through a barrier's
/// index before publishing that barrier. Different blobs may be enqueued concurrently.
#[derive(Debug, Clone)]
pub struct PendingQueue {
    db: Arc<dyn IndexDb>,
    blobs: TypedMap<Vec<u8>, PendingBlobOps>,
    barriers: TypedMap<u64, EpochBarrier>,
}

impl PendingQueue {
    /// The caller must open the queue families using [`cf_options`]. All queue and application
    /// metadata writes must keep the WAL enabled.
    pub fn new(db: Arc<dyn IndexDb>) -> Self {
        Self {
            blobs: TypedMap::new(Arc::clone(&db), PENDING_BLOBS_CF),
            barriers: TypedMap::new(Arc::clone(&db), EPOCH_BARRIERS_CF),
            db,
        }
    }

    /// Stage application metadata through `batch.metadata()`. Propagate staging errors and do
    /// not enqueue an already-handled event: persist the application's event progress in this
    /// same batch so recovery can skip it. Callback failure discards the whole batch.
    /// Success means committed, not synced; uncertain commit errors require stopping and recovery.
    pub fn write_batch<T>(&self, update: impl FnOnce(&mut PendingBatch) -> Result<T>) -> Result<T> {
        let mut batch = PendingBatch {
            write: self.db.write_batch(),
        };
        let result = update(&mut batch)?;
        batch.write.write(false)?;
        Ok(result)
    }

    /// Capture BEFORE syncing: newer visible writes must wait for the next pass. A sync failure
    /// returns no usable view and requires stopping and recovery. A snapshot can become stale
    /// through registration, so workers still revalidate under the blob lock before deleting.
    /// Use this same snapshot for both [`Self::blobs`] and [`Self::barriers`].
    pub fn durable_snapshot(&self) -> Result<Box<dyn IndexSnapshot + '_>> {
        let snapshot = self.db.snapshot()?;
        self.db.flush_wal(true)?;
        Ok(snapshot)
    }

    /// Stream blob operations from the snapshot returned by [`Self::durable_snapshot`].
    pub fn blobs<'a>(
        &'a self,
        snapshot: &'a dyn IndexSnapshot,
    ) -> Result<impl Iterator<Item = Result<(Vec<u8>, PendingBlobOps)>> + 'a> {
        self.blobs.safe_iter_with_snapshot(snapshot)
    }

    /// Stream epoch barriers from the same snapshot used for [`Self::blobs`].
    pub fn barriers<'a>(
        &'a self,
        snapshot: &'a dyn IndexSnapshot,
    ) -> Result<impl Iterator<Item = Result<(u64, EpochBarrier)>> + 'a> {
        self.barriers.safe_iter_with_snapshot(snapshot)
    }
}

/// Queue edits and application metadata in one atomic batch.
pub struct PendingBatch {
    write: Box<dyn IndexWriteBatch>,
}

impl PendingBatch {
    pub fn metadata(&mut self) -> &mut dyn IndexWriteBatch {
        self.write.as_mut()
    }

    /// Enqueue at most one command per blob per event, in increasing event order. The caller
    /// coalesces work for the same (blob, event) and filters retries using its metadata. Zero is
    /// a valid event index; indexes need not be contiguous or unique across different blobs.
    pub fn append(
        &mut self,
        key: &[u8],
        event_index: u64,
        operation: BlobOperation,
        source: Vec<u8>,
    ) -> Result<()> {
        let operand = BlobOperand::V1(BlobEdit::Append(BlobCommand {
            event_index,
            source,
            operation,
        }));
        self.write
            .merge(PENDING_BLOBS_CF, &encode_key(key)?, &operand.encode()?)
    }

    /// Add/validate the live reference in this same batch under the shared blob lock. An old
    /// in-flight put must not call this method; permanent invalidation deletes are never cancelled.
    /// The same event-index contract as [`Self::append`] applies.
    pub fn register(
        &mut self,
        key: &[u8],
        event_index: u64,
        end_epoch: u64,
        source: Vec<u8>,
    ) -> Result<()> {
        let operand = BlobOperand::V1(BlobEdit::Register(BlobCommand {
            event_index,
            source,
            operation: BlobOperation::SetLifetime { end_epoch },
        }));
        self.write
            .merge(PENDING_BLOBS_CF, &encode_key(key)?, &operand.encode()?)
    }

    /// Publish only after all blob work through this event index has been enqueued, including any
    /// pool fan-out. The worker finishes that work before advancing to this absolute epoch and
    /// defers higher event indexes until afterward. One barrier is allowed per event.
    pub fn advance_epoch(&mut self, event_index: u64, epoch: u64, source: Vec<u8>) -> Result<()> {
        self.write.put(
            EPOCH_BARRIERS_CF,
            &encode_key(&event_index)?,
            &encode_value(&EpochBarrier::V1 { epoch, source })?,
        )
    }
}

#[cfg(test)]
mod tests;
