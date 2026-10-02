//! Application replay bookkeeping. The engine sees only opaque batch keys and LSNs.
//! Submission methods target Strata (which records the LSN in RocksDB); acknowledgement methods
//! update only RocksDB after checking Strata durability.

use index::port::codec::encode_key;
use store::{StrataBatch, StrataStore};

use super::*;

impl PendingQueue {
    /// Submit the first pending command for a blob, or return its previously submitted LSN.
    /// The caller builds `batch` with the command's actual operations, including all of its
    /// physical keys/shards. If an LSN binding already exists, the batch is dropped without writing.
    ///
    /// The caller must select work from a durable snapshot and hold the shared blob/lifecycle
    /// locks from revalidation through durable acknowledgement. This method rechecks that the
    /// command is still first, but it does not make newly enqueued work durable or acquire locks.
    /// Shard generations must be validated under those locks before building tombstones.
    /// Submissions for the same blob must be serialized, including the saved-LSN check. Different
    /// blobs may be submitted concurrently; the writer does not check for duplicate batch keys.
    ///
    /// Success returns a *submitted* LSN. Batch several submissions before waiting for Strata
    /// durability, then call `acknowledge_blobs_rocksdb`. An uncertain write/sync error requires
    /// stopping and recovery, without admitting conflicting writes. Recovery has already removed
    /// bindings for discarded writes before the store opens, so a surviving binding identifies
    /// this exact submission even when earlier attempts' LSNs have been reused.
    pub fn submit_blob_strata(
        &self,
        key: &[u8],
        event_index: u64,
        batch: StrataBatch<'_>,
    ) -> Result<u64> {
        self.check_shared_rocksdb(batch.store())?;
        let pending = self.blobs.get(&key.to_vec())?.unwrap_or_default();
        if pending.commands().first().map(|c| c.event_index) != Some(event_index) {
            return Err(Error::InvalidPendingOperation(
                "command was cancelled, acknowledged, or is not first in its blob queue".into(),
            ));
        }
        self.submit_strata(blob_lsn_key_rocksdb(key, event_index)?, batch)
    }

    /// Retire completed commands and their LSN bindings atomically, sharing one RocksDB WAL sync.
    /// Release the shared blob locks only after success. Newer queue appends are preserved.
    pub fn acknowledge_blobs_rocksdb(
        &self,
        store: &StrataStore,
        commands: &[(&[u8], u64)],
    ) -> Result<()> {
        self.check_shared_rocksdb(store)?;
        let mut batch = store.index().batch();
        for &(key, event_index) in commands {
            let lsn_key = blob_lsn_key_rocksdb(key, event_index)?;
            self.check_durable_strata(store, &lsn_key)?;
            let operand = BlobOperand::V1(BlobEdit::Acknowledge {
                through_event_index: event_index,
            });
            batch.partial_merge_batch(&self.blobs, [(key.to_vec(), operand.encode()?)])?;
            batch.delete_batch(store.index().submitted_batch_lsns(), [&lsn_key])?;
        }
        batch.write_with_sync(true)?;
        Ok(())
    }

    /// Submit an absolute epoch target. The caller must first drain all preceding blob commands
    /// and prevent later work from passing this barrier until its acknowledgement is durable.
    /// Serialize barrier submissions, including retries, under the shared lifecycle lock.
    pub fn submit_epoch_strata(&self, store: &StrataStore, event_index: u64) -> Result<u64> {
        self.check_shared_rocksdb(store)?;
        let Some(EpochBarrier::V1 { epoch, .. }) = self.barriers.get(&event_index)? else {
            return Err(Error::InvalidPendingOperation(
                "missing epoch barrier".into(),
            ));
        };
        let mut batch = store.batch();
        batch.advance_epoch_to(epoch);
        self.submit_strata(epoch_lsn_key_rocksdb(event_index)?, batch)
    }

    /// Durably retire a completed epoch barrier and its LSN binding in one RocksDB batch.
    pub fn acknowledge_epoch_rocksdb(&self, store: &StrataStore, event_index: u64) -> Result<()> {
        let lsn_key = epoch_lsn_key_rocksdb(event_index)?;
        self.check_durable_strata(store, &lsn_key)?;
        let mut batch = store.index().batch();
        batch.delete_batch(&self.barriers, [event_index])?;
        batch.delete_batch(store.index().submitted_batch_lsns(), [&lsn_key])?;
        batch.write_with_sync(true)?;
        Ok(())
    }

    // The caller holds the blob/lifecycle lock across this lookup and submission.
    fn submit_strata(&self, lsn_key: Vec<u8>, batch: StrataBatch<'_>) -> Result<u64> {
        if let Some(lsn) = batch.store().index().submitted_batch_lsns().get(&lsn_key)? {
            return Ok(lsn);
        }
        let result = batch.write_with_lsn(lsn_key)?;
        Ok(result.last_lsn().expect("tracked batches are nonempty"))
    }

    fn check_shared_rocksdb(&self, store: &StrataStore) -> Result<()> {
        if !Arc::ptr_eq(&self.db, store.index().db()) {
            return Err(Error::InvalidPendingOperation(
                "queue and store must share the same IndexDb handle".into(),
            ));
        }
        Ok(())
    }

    fn check_durable_strata(&self, store: &StrataStore, lsn_key: &Vec<u8>) -> Result<()> {
        self.check_shared_rocksdb(store)?;
        let Some(lsn) = store.index().submitted_batch_lsns().get(lsn_key)? else {
            return Err(Error::InvalidPendingOperation(
                "cannot acknowledge work without a submitted Strata batch".into(),
            ));
        };
        // Trust the notification issued AFTER a successful sync, not a RocksDB read that
        // could expose a checkpoint whose fsync is still in flight or has failed.
        let durability = store.subscribe_durability_progress();
        let progress = durability.borrow();
        if let Some(reason) = &progress.halt_reason {
            return Err(store::Error::StoreHalted {
                reason: reason.clone(),
            }
            .into());
        }
        if lsn > progress.published_lsn {
            return Err(Error::InvalidPendingOperation(
                "Strata batch is not durable".into(),
            ));
        }
        Ok(())
    }
}

// Versioned, disjoint opaque key spaces. Only this layer knows these contain event indexes.
fn blob_lsn_key_rocksdb(key: &[u8], event_index: u64) -> Result<Vec<u8>> {
    Ok(encode_key(&(
        b"queue/blob/v1".as_slice(),
        key,
        event_index,
    ))?)
}

fn epoch_lsn_key_rocksdb(event_index: u64) -> Result<Vec<u8>> {
    Ok(encode_key(&(b"queue/epoch/v1".as_slice(), event_index))?)
}
