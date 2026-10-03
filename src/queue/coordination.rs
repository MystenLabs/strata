//! Shared admission for blob work and shard/epoch lifecycle changes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use super::{Error, PendingQueue, Result};

#[derive(Debug, Default)]
pub(super) struct Coordination {
    lifecycle: Arc<RwLock<()>>,
    blobs: Mutex<HashMap<Vec<u8>, Weak<BlobLock>>>,
}

#[derive(Debug)]
struct BlobLock {
    key: Vec<u8>,
    mutex: Arc<AsyncMutex<()>>,
    owner: Weak<Coordination>,
}

impl Drop for BlobLock {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut blobs = owner.blobs.lock().expect("blob lock registry poisoned");
            // A new acquisition may already have replaced our expired weak entry.
            if blobs
                .get(&self.key)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                blobs.remove(&self.key);
            }
        }
    }
}

/// Exclusive access to the selected blobs, with shard/epoch changes held back.
/// Foreground puts, registrations and queued work must share these guards. Recheck application
/// references after acquiring them. Hold through Strata durability and RocksDB acknowledgement;
/// once a submission starts, task cancellation or an uncertain write/sync failure is fail-stop.
#[derive(Debug)]
#[must_use = "hold this guard through the protected blob work and durable acknowledgement"]
pub struct LockedBlobs {
    // Drop mutex guards before releasing their registry entries, and the lifecycle guard last.
    _guards: Vec<OwnedMutexGuard<()>>,
    entries: Vec<Arc<BlobLock>>,
    coordination: Arc<Coordination>,
    _lifecycle: OwnedRwLockReadGuard<()>,
}

/// Exclusive access for shard creation/removal and epoch barriers. Hold until their Strata
/// changes and associated RocksDB metadata are durable. This guard does not select event order.
#[derive(Debug)]
#[must_use = "hold this guard through the shard or epoch change and its durability"]
pub struct LifecycleGuard {
    coordination: Arc<Coordination>,
    _guard: OwnedRwLockWriteGuard<()>,
}

impl PendingQueue {
    /// Lock all blobs needed by one operation. Keys are sorted and deduplicated so overlapping
    /// groups cannot deadlock by requesting opposite orders. Different blobs remain independent.
    /// Acquire the entire group in one call: never acquire another guard while holding one from
    /// this queue, or upgrade blob guards to a lifecycle guard. Do not wait for queued work that
    /// needs these same locks while holding the guard; finish prerequisites first, then recheck.
    pub async fn lock_blobs(&self, keys: &[&[u8]]) -> LockedBlobs {
        // Blob locks alone cannot exclude shard recreation or epoch changes: those affect many
        // blobs and take the lifecycle lock exclusively. For example, a delete could validate
        // shard 7 generation 0, then a drop/recreate could make its tombstone target generation 1.
        // Take the shared lifecycle guard first and retain it in LockedBlobs through Strata
        // durability and RocksDB acknowledgement. This also makes epoch advancement wait for
        // an in-flight lifetime extension to finish. Shared mode allows unrelated blobs to run
        // concurrently; exclusive mode would serialize all blob work. The worker must still
        // order queued events so required extensions are processed before an epoch advance.
        let lifecycle = self.coordination.lifecycle.clone().read_owned().await;
        let mut keys: Vec<_> = keys.iter().map(|key| key.to_vec()).collect();
        keys.sort_unstable();
        keys.dedup();
        let entries: Vec<_> = {
            let mut blobs = self
                .coordination
                .blobs
                .lock()
                .expect("blob lock registry poisoned");
            keys.into_iter()
                .map(|key| {
                    if let Some(entry) = blobs.get(&key).and_then(Weak::upgrade) {
                        return entry;
                    }
                    let entry = Arc::new(BlobLock {
                        key: key.clone(),
                        mutex: Arc::default(),
                        owner: Arc::downgrade(&self.coordination),
                    });
                    blobs.insert(key, Arc::downgrade(&entry));
                    entry
                })
                .collect()
        };
        // Reservations stay alive while waiting. Cancelling an acquisition releases both the
        // already-acquired guards and the registry entries; there is no permanent per-blob map.
        let mut guards = Vec::with_capacity(entries.len());
        for entry in &entries {
            guards.push(entry.mutex.clone().lock_owned().await);
        }
        LockedBlobs {
            _guards: guards,
            entries,
            coordination: self.coordination.clone(),
            _lifecycle: lifecycle,
        }
    }

    /// Drain admitted blob work and prevent new work during an epoch or shard change.
    /// Do not call while holding any blob or lifecycle guard from this queue.
    pub async fn lock_lifecycle(&self) -> LifecycleGuard {
        LifecycleGuard {
            coordination: self.coordination.clone(),
            _guard: self.coordination.lifecycle.clone().write_owned().await,
        }
    }

    pub(super) fn check_blob_lock(&self, guard: &LockedBlobs, key: &[u8]) -> Result<()> {
        if !Arc::ptr_eq(&self.coordination, &guard.coordination)
            || guard
                .entries
                .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                .is_err()
        {
            return Err(Error::InvalidPendingOperation(
                "blob guard belongs to another queue or does not cover this blob".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn check_lifecycle_lock(&self, guard: &LifecycleGuard) -> Result<()> {
        if !Arc::ptr_eq(&self.coordination, &guard.coordination) {
            return Err(Error::InvalidPendingOperation(
                "lifecycle guard belongs to another queue".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "coordination_tests.rs"]
mod tests;
