//! Pending lifecycle work in the application's RocksDB database.
//!
//! One mergeable row holds each blob's pending operations. Epoch barriers live in a separate
//! ordered table. Revisions order both kinds of work, but are not Strata LSNs or proof of an
//! applied prefix: different blobs can complete out of order.
//!
//! The application supplies a storage adapter and stages its metadata in the same batch as
//! queue changes. This crate owns the schema, merge semantics, allocation and snapshot-before-sync
//! ordering. It does not run a worker or mutate Strata. Payload puts keep their separate
//! data-first durability path.
//!
//! A future worker must coordinate with registrations and puts under shared blob locks, revalidate
//! cancellations, durably apply operations with replay identities, and acknowledge only completed
//! revisions. It must finish all work before an epoch barrier before advancing the epoch, and must
//! not apply work after that barrier prematurely. Pool fan-out must finish enqueueing before the
//! application publishes the subsequent barrier. An event ID is opaque provenance, not an order.

mod model;
pub use model::*;

use std::sync::{Arc, Mutex};

pub const PENDING_BLOBS_CF: &str = "strata_pending_blob_ops";
pub const EPOCH_BARRIERS_CF: &str = "strata_pending_epoch_barriers";
pub const LAST_REVISION_CF: &str = "strata_pending_last_revision";
pub const MERGE_OPERATOR_NAME: &str = "strata_pending_blob_ops_v1";

/// Storage operations staged in a caller-owned atomic batch. All must use the same database.
/// Dropping a batch without committing must discard every staged change.
pub trait QueueWrite {
    type Error: From<Error>;
    type Metadata;
    fn metadata(&mut self) -> &mut Self::Metadata;
    fn merge_blob(&mut self, key: &[u8], operand: &[u8]) -> Result<(), Self::Error>;
    fn put_barrier(
        &mut self,
        revision: Revision,
        barrier: &EpochBarrier,
    ) -> Result<(), Self::Error>;
    fn set_last_revision(&mut self, revision: Revision) -> Result<(), Self::Error>;
}

/// Snapshot reads must share one RocksDB snapshot across all queue tables.
pub trait QueueSnapshot {
    type Error;
    /// Page in the adapter's key order, strictly after the supplied key. Include empty rows so
    /// callers can make bounded progress even when acknowledgements have emptied many rows.
    fn blobs(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingBlobOps)>, Self::Error>;
    /// Page strictly after `after`, in increasing revision order.
    fn barriers(
        &self,
        after: Option<Revision>,
        limit: usize,
    ) -> Result<Vec<(Revision, EpochBarrier)>, Self::Error>;
}

/// The embedding application's RocksDB adapter. WAL must be enabled for every queue/metadata
/// commit; `sync` must make all commits visible before its invocation durable.
pub trait QueueStorage {
    type Error: From<Error>;
    type Write: QueueWrite<Error = Self::Error>;
    type Snapshot<'a>: QueueSnapshot<Error = Self::Error>
    where
        Self: 'a;
    fn last_revision(&self) -> Result<Revision, Self::Error>;
    fn batch(&self) -> Self::Write;
    fn commit(&self, batch: Self::Write) -> Result<(), Self::Error>;
    fn snapshot(&self) -> Result<Self::Snapshot<'_>, Self::Error>;
    fn sync(&self) -> Result<(), Self::Error>;
}

/// Construct once per queue namespace; clones share the allocator lock. This lock orders
/// commits, but does not replace the application's reference checks or shared blob locks.
#[derive(Debug)]
pub struct PendingQueue<S> {
    storage: Arc<S>,
    producer_lock: Arc<Mutex<()>>,
}

impl<S> Clone for PendingQueue<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            producer_lock: Arc::clone(&self.producer_lock),
        }
    }
}

impl<S: QueueStorage> PendingQueue<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
            producer_lock: Arc::default(),
        }
    }

    /// Commit metadata, merge operands and revision allocation atomically. Callback errors abort
    /// the whole batch. Propagate staging errors; only stage metadata in the supplied write and
    /// never reenter this queue.
    /// Success means visible, not synced. An uncertain commit requires stopping and recovery.
    pub fn write_batch<T>(
        &self,
        update: impl FnOnce(&mut PendingBatch<S::Write>) -> Result<T, S::Error>,
    ) -> Result<T, S::Error> {
        let _guard = self.producer_lock.lock().map_err(|_| Error::Poisoned)?;
        let mut batch = PendingBatch {
            write: self.storage.batch(),
            last_revision: self.storage.last_revision()?,
        };
        let result = update(&mut batch)?;
        self.storage.commit(batch.write)?;
        Ok(result)
    }

    /// Capture first, then sync. Later commits cannot enter this view, even if they are visible
    /// by the time the sync returns. A failed sync returns no view; stop and recover on I/O errors.
    /// Durability alone does not authorize a stale delete: the worker still checks cancellation
    /// under the shared blob lock before applying each snapshot entry.
    pub fn durable_snapshot(&self) -> Result<DurableSnapshot<S::Snapshot<'_>>, S::Error> {
        let snapshot = self.storage.snapshot()?;
        self.storage.sync()?;
        Ok(DurableSnapshot { snapshot })
    }
}

/// A snapshot covered by a successful RocksDB WAL sync, not a view of newer visible writes.
pub struct DurableSnapshot<S> {
    snapshot: S,
}

impl<S: QueueSnapshot> DurableSnapshot<S> {
    pub fn blobs(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingBlobOps)>, S::Error> {
        self.snapshot.blobs(after, limit)
    }

    pub fn barriers(
        &self,
        after: Option<Revision>,
        limit: usize,
    ) -> Result<Vec<(Revision, EpochBarrier)>, S::Error> {
        self.snapshot.barriers(after, limit)
    }
}

pub struct PendingBatch<W> {
    write: W,
    last_revision: Revision,
}

impl<W: QueueWrite> PendingBatch<W> {
    /// Application metadata must be staged here, never committed independently.
    pub fn metadata(&mut self) -> &mut W::Metadata {
        self.write.metadata()
    }

    fn allocate(&mut self) -> Result<Revision, W::Error> {
        let revision = self.last_revision.next()?;
        self.write.set_last_revision(revision)?;
        self.last_revision = revision;
        Ok(revision)
    }

    /// Append without reading or rewriting the blob's pending list.
    pub fn append(
        &mut self,
        key: &[u8],
        operation: BlobOperation,
        source: Vec<u8>,
    ) -> Result<Revision, W::Error> {
        let revision = self.allocate()?;
        let operand = BlobOperand::V1(BlobEdit::Append(BlobCommand {
            revision,
            source,
            operation,
        }));
        self.write.merge_blob(key, &operand.encode()?)?;
        Ok(revision)
    }

    /// Atomically cancel earlier cancellable deletes and append lifetime initialization.
    /// The application must add/validate the live reference in this same batch and coordinate
    /// with the worker's blob lock. An old in-flight put is not a registration and must not call
    /// this helper. Permanent invalidation deletes are never cancelled.
    pub fn register(
        &mut self,
        key: &[u8],
        end_epoch: u64,
        source: Vec<u8>,
    ) -> Result<Revision, W::Error> {
        let revision = self.allocate()?;
        let operand = BlobOperand::V1(BlobEdit::Register(BlobCommand {
            revision,
            source,
            operation: BlobOperation::SetLifetime { end_epoch },
        }));
        self.write.merge_blob(key, &operand.encode()?)?;
        Ok(revision)
    }

    /// An absolute target, not an increment. Producers must enqueue in application event order.
    pub fn advance_epoch(&mut self, epoch: u64, source: Vec<u8>) -> Result<Revision, W::Error> {
        let revision = self.allocate()?;
        self.write
            .put_barrier(revision, &EpochBarrier::V1 { epoch, source })?;
        Ok(revision)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Strata queue revision exhausted")]
    Exhausted,
    #[error("Strata queue producer lock poisoned")]
    Poisoned,
    #[error("invalid Strata pending operation: {0}")]
    Codec(#[from] bcs::Error),
    #[error("Strata pending revisions must increase and start above zero")]
    RevisionOrder,
    #[error("registration must initialize a lifetime")]
    RegistrationOperation,
}

#[cfg(test)]
mod tests;
