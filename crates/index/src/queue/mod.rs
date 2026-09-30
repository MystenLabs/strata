//! Per-blob lifecycle work stored alongside application metadata in RocksDB.
//!
//! A producer stages metadata and queue changes in one batch. A worker takes a snapshot, syncs
//! RocksDB, then processes only that snapshot. Registration cancels earlier ordinary deletes;
//! acknowledgements remove only completed revisions. Epoch barriers order work across blobs.
//!
//! This module does not apply operations to Strata. The worker must recheck cancellation under
//! the shared blob lock, durably apply effects with replay identities, and drain all preceding
//! work before an epoch barrier. Revisions are command identities, not LSNs or an applied watermark.
//! The application owns reference checks, pool fan-out and foreground put coordination.

mod model;
pub use model::*;

use crate::{
    Error, Result,
    port::{
        IndexDb, IndexSnapshot, IndexWriteBatch, TypedMap,
        codec::{decode_value, encode_key, encode_value},
    },
};
use std::sync::{Arc, Mutex};

pub const PENDING_BLOBS_CF: &str = "strata_pending_blob_ops";
pub const EPOCH_BARRIERS_CF: &str = "strata_pending_epoch_barriers";
pub const LAST_REVISION_CF: &str = "strata_pending_last_revision";

/// Open these families with the application database, registering the merge operator on reopen.
pub fn cf_options(mut standard: rocksdb::Options) -> [(&'static str, rocksdb::Options); 3] {
    let barriers = standard.clone();
    let revision = standard.clone();
    standard.set_merge_operator(
        "strata_pending_blob_ops_v1",
        |_, existing, operands| merge_pending(existing, operands).ok(),
        // Cancellation and acknowledgement need the base row. Never partially fold them away.
        |_, _, _| None,
    );
    [
        (PENDING_BLOBS_CF, standard),
        (EPOCH_BARRIERS_CF, barriers),
        (LAST_REVISION_CF, revision),
    ]
}

/// Open once per application database; clones share the short producer lock.
#[derive(Debug, Clone)]
pub struct PendingQueue {
    db: Arc<dyn IndexDb>,
    producer_lock: Arc<Mutex<()>>,
    blobs: TypedMap<Vec<u8>, PendingBlobOps>,
    barriers: TypedMap<Revision, EpochBarrier>,
}

impl PendingQueue {
    /// The caller must open the queue families using [`cf_options`]. All queue and application
    /// metadata writes must keep the WAL enabled.
    pub fn new(db: Arc<dyn IndexDb>) -> Self {
        Self {
            blobs: TypedMap::new(Arc::clone(&db), PENDING_BLOBS_CF),
            barriers: TypedMap::new(Arc::clone(&db), EPOCH_BARRIERS_CF),
            db,
            producer_lock: Arc::default(),
        }
    }

    /// Stage application metadata through `batch.metadata()`. Propagate staging errors and do
    /// not reenter the queue. Callback failure discards the batch and its revision allocation.
    /// Success means committed, not synced; uncertain commit errors require stopping and recovery.
    pub fn write_batch<T>(&self, update: impl FnOnce(&mut PendingBatch) -> Result<T>) -> Result<T> {
        let _guard = self
            .producer_lock
            .lock()
            .map_err(|_| Error::InvalidPendingOperation("producer lock poisoned".into()))?;
        let last_revision = self
            .db
            .get(LAST_REVISION_CF, &[])?
            .as_deref()
            .map(decode_value)
            .transpose()?
            .unwrap_or_default();
        let mut batch = PendingBatch {
            write: self.db.write_batch(),
            last_revision,
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
    ) -> Result<impl Iterator<Item = Result<(Revision, EpochBarrier)>> + 'a> {
        self.barriers.safe_iter_with_snapshot(snapshot)
    }
}

/// The application's atomic batch plus queue revision allocation.
pub struct PendingBatch {
    write: Box<dyn IndexWriteBatch>,
    last_revision: Revision,
}

impl PendingBatch {
    pub fn metadata(&mut self) -> &mut dyn IndexWriteBatch {
        self.write.as_mut()
    }

    fn allocate(&mut self) -> Result<Revision> {
        let revision = self.last_revision.next()?;
        self.write
            .put(LAST_REVISION_CF, &[], &encode_value(&revision)?)?;
        self.last_revision = revision;
        Ok(revision)
    }

    pub fn append(
        &mut self,
        key: &[u8],
        operation: BlobOperation,
        source: Vec<u8>,
    ) -> Result<Revision> {
        let revision = self.allocate()?;
        let operand = BlobOperand::V1(BlobEdit::Append(BlobCommand {
            revision,
            source,
            operation,
        }));
        self.write
            .merge(PENDING_BLOBS_CF, &encode_key(key)?, &operand.encode()?)?;
        Ok(revision)
    }

    /// Add/validate the live reference in this same batch under the shared blob lock. An old
    /// in-flight put must not call this method; permanent invalidation deletes are never cancelled.
    pub fn register(&mut self, key: &[u8], end_epoch: u64, source: Vec<u8>) -> Result<Revision> {
        let revision = self.allocate()?;
        let operand = BlobOperand::V1(BlobEdit::Register(BlobCommand {
            revision,
            source,
            operation: BlobOperation::SetLifetime { end_epoch },
        }));
        self.write
            .merge(PENDING_BLOBS_CF, &encode_key(key)?, &operand.encode()?)?;
        Ok(revision)
    }

    /// Enqueue in event order, after any preceding pool fan-out. The worker must finish earlier
    /// revisions before advancing to this absolute epoch, and defer later revisions until after it.
    pub fn advance_epoch(&mut self, epoch: u64, source: Vec<u8>) -> Result<Revision> {
        let revision = self.allocate()?;
        self.write.put(
            EPOCH_BARRIERS_CF,
            &encode_key(&revision)?,
            &encode_value(&EpochBarrier::V1 { epoch, source })?,
        )?;
        Ok(revision)
    }
}

#[cfg(test)]
mod tests;
