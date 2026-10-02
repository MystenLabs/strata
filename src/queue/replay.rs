//! Application replay bookkeeping. The engine sees only opaque batch keys and LSNs.

use index::port::codec::encode_key;
use store::{StrataBatch, StrataStore};

use super::*;

impl PendingQueue {
    /// Submit the first pending command for a blob, or return its previously submitted LSN.
    /// `prepare` translates that command into ordinary Strata operations (including all of its
    /// physical keys/shards); it is not called again while the LSN binding exists.
    ///
    /// The caller must select work from a durable snapshot and hold the shared blob/lifecycle
    /// locks from revalidation through durable acknowledgement. This method rechecks that the
    /// command is still first, but it does not make newly enqueued work durable or acquire locks.
    /// Shard generations must be validated under those locks before building tombstones.
    ///
    /// Success returns a *submitted* LSN. Batch several submissions before waiting for Strata
    /// durability, then call `acknowledge_blobs`. An uncertain write/sync error requires stopping
    /// and recovery, without admitting conflicting writes. Recovery has already removed bindings
    /// for discarded writes before the store opens, so a surviving binding identifies this exact
    /// submission even when earlier attempts' LSNs have been reused.
    pub fn submit_blob(
        &self,
        store: &StrataStore,
        key: &[u8],
        event_index: u64,
        prepare: impl FnOnce(&mut StrataBatch<'_>) -> store::Result<()>,
    ) -> Result<u64> {
        self.check_store(store)?;
        let pending = self.blobs.get(&key.to_vec())?.unwrap_or_default();
        if pending.commands().first().map(|c| c.event_index) != Some(event_index) {
            return Err(Error::InvalidPendingOperation(
                "command was cancelled, acknowledged, or is not first in its blob queue".into(),
            ));
        }
        self.submit(store, blob_lsn_key(key, event_index)?, prepare)
    }

    /// Retire completed commands and their LSN bindings atomically, sharing one RocksDB WAL sync.
    /// Release the shared blob locks only after success. Newer queue appends are preserved.
    pub fn acknowledge_blobs(&self, store: &StrataStore, commands: &[(&[u8], u64)]) -> Result<()> {
        self.check_store(store)?;
        let mut batch = store.index().batch();
        for &(key, event_index) in commands {
            let lsn_key = blob_lsn_key(key, event_index)?;
            self.check_durable(store, &lsn_key)?;
            let operand = BlobOperand::V1(BlobEdit::Acknowledge {
                through_event_index: event_index,
            });
            batch.partial_merge_batch(&self.blobs, [(key.to_vec(), operand.encode()?)])?;
            batch.delete_batch(store.index().batch_lsns(), [&lsn_key])?;
        }
        batch.write_with_sync(true)?;
        Ok(())
    }

    /// Submit an absolute epoch target. The caller must first drain all preceding blob commands
    /// and prevent later work from passing this barrier until its acknowledgement is durable.
    pub fn submit_epoch(&self, store: &StrataStore, event_index: u64) -> Result<u64> {
        self.check_store(store)?;
        let Some(EpochBarrier::V1 { epoch, .. }) = self.barriers.get(&event_index)? else {
            return Err(Error::InvalidPendingOperation(
                "missing epoch barrier".into(),
            ));
        };
        self.submit(store, epoch_lsn_key(event_index)?, |batch| {
            batch.advance_epoch_to(epoch);
            Ok(())
        })
    }

    /// Durably retire a completed epoch barrier and its LSN binding in one RocksDB batch.
    pub fn acknowledge_epoch(&self, store: &StrataStore, event_index: u64) -> Result<()> {
        let lsn_key = epoch_lsn_key(event_index)?;
        self.check_durable(store, &lsn_key)?;
        let mut batch = store.index().batch();
        batch.delete_batch(&self.barriers, [event_index])?;
        batch.delete_batch(store.index().batch_lsns(), [&lsn_key])?;
        batch.write_with_sync(true)?;
        Ok(())
    }

    fn submit(
        &self,
        store: &StrataStore,
        lsn_key: Vec<u8>,
        prepare: impl FnOnce(&mut StrataBatch<'_>) -> store::Result<()>,
    ) -> Result<u64> {
        if let Some(lsn) = store.index().batch_lsns().get(&lsn_key)? {
            return Ok(lsn);
        }
        let mut batch = store.batch();
        prepare(&mut batch)?;
        let result = batch.write_with_lsn(lsn_key)?;
        Ok(result.last_lsn().expect("tracked batches are nonempty"))
    }

    fn check_store(&self, store: &StrataStore) -> Result<()> {
        if !Arc::ptr_eq(&self.db, store.index().db()) {
            return Err(Error::InvalidPendingOperation(
                "queue and store must share the same IndexDb handle".into(),
            ));
        }
        Ok(())
    }

    fn check_durable(&self, store: &StrataStore, lsn_key: &Vec<u8>) -> Result<()> {
        self.check_store(store)?;
        let Some(lsn) = store.index().batch_lsns().get(lsn_key)? else {
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
fn blob_lsn_key(key: &[u8], event_index: u64) -> Result<Vec<u8>> {
    Ok(encode_key(&(
        b"queue/blob/v1".as_slice(),
        key,
        event_index,
    ))?)
}

fn epoch_lsn_key(event_index: u64) -> Result<Vec<u8>> {
    Ok(encode_key(&(b"queue/epoch/v1".as_slice(), event_index))?)
}
