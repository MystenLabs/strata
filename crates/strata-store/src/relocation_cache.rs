//! Physical relocation cache, consulted only after the main LSM resolves a live payload whose
//! recorded source segment has been deleted. It does not cache folded main-LSM rows or lifecycle.

use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};

use strata_core::{BlobKey, RecordRef, ShardKey, StrataLsn};
use strata_relocation::RelocationEntry;

pub const DEFAULT_RELOCATION_CACHE_ENTRIES: usize = 100_000;

#[derive(Debug)]
struct CachedRelocation {
    payload_lsn: StrataLsn,
    to: RecordRef,
    referenced: bool,
}

#[derive(Debug, Default)]
struct State {
    rows: HashMap<BlobKey, HashMap<ShardKey, CachedRelocation>>,
    clock: VecDeque<(BlobKey, ShardKey)>,
    entries: usize,
}

#[derive(Debug)]
pub(crate) struct RelocationCache {
    capacity: usize,
    state: Mutex<State>,
}

impl RelocationCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn get(
        &self,
        key: &BlobKey,
        shard: ShardKey,
        payload_lsn: StrataLsn,
    ) -> Option<RecordRef> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let matches = state
            .rows
            .get(key)
            .and_then(|shards| shards.get(&shard))
            .is_some_and(|entry| entry.payload_lsn == payload_lsn);
        if !matches {
            return None;
        }
        let entry = state
            .rows
            .get_mut(key)
            .and_then(|shards| shards.get_mut(&shard))
            .expect("checked relocation row");
        entry.referenced = true;
        Some(entry.to)
    }

    pub(crate) fn insert(
        &self,
        key: BlobKey,
        shard: ShardKey,
        payload_lsn: StrataLsn,
        to: RecordRef,
    ) {
        if self.capacity == 0 {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = state
            .rows
            .get_mut(&key)
            .and_then(|shards| shards.get_mut(&shard))
        {
            entry.payload_lsn = payload_lsn;
            entry.to = to;
            entry.referenced = true;
            return;
        }

        state.rows.entry(key.clone()).or_default().insert(
            shard,
            CachedRelocation {
                payload_lsn,
                to,
                referenced: true,
            },
        );
        state.clock.push_back((key, shard));
        state.entries += 1;

        while state.entries > self.capacity {
            let Some((candidate_key, candidate_shard)) = state.clock.pop_front() else {
                break;
            };
            let Some(entry) = state
                .rows
                .get_mut(&candidate_key)
                .and_then(|shards| shards.get_mut(&candidate_shard))
            else {
                continue;
            };
            if entry.referenced {
                entry.referenced = false;
                state.clock.push_back((candidate_key, candidate_shard));
                continue;
            }
            let shards = state
                .rows
                .get_mut(&candidate_key)
                .expect("clock candidate key exists");
            shards.remove(&candidate_shard);
            if shards.is_empty() {
                state.rows.remove(&candidate_key);
            }
            state.entries -= 1;
        }
    }

    pub(crate) fn remove_dropped(&self, dropped: &[RelocationEntry]) -> usize {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut removed = 0;
        for dropped in dropped {
            let matches = state
                .rows
                .get(&dropped.key)
                .and_then(|shards| shards.get(&dropped.shard))
                .is_some_and(|cached| {
                    cached.payload_lsn == dropped.payload_lsn && cached.to == dropped.to
                });
            if !matches {
                continue;
            }
            let shards = state
                .rows
                .get_mut(&dropped.key)
                .expect("checked relocation cache key");
            shards.remove(&dropped.shard);
            if shards.is_empty() {
                state.rows.remove(&dropped.key);
            }
            state.entries -= 1;
            removed += 1;
        }
        if removed > 0 {
            let mut clock = std::mem::take(&mut state.clock);
            clock.retain(|(key, shard)| {
                state
                    .rows
                    .get(key)
                    .is_some_and(|shards| shards.contains_key(shard))
            });
            state.clock = clock;
        }
        removed
    }

    pub(crate) fn clear(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = State::default();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &[u8]) -> BlobKey {
        BlobKey::new(name.to_vec()).unwrap()
    }

    fn reference(segment_id: u64) -> RecordRef {
        RecordRef {
            segment_id,
            offset: 10,
            len: 20,
        }
    }

    #[test]
    fn replacement_and_payload_identity_cannot_return_an_old_pointer() {
        let cache = RelocationCache::new(2);
        let blob = key(b"blob");
        let shard = ShardKey {
            id: 1,
            generation: 2,
        };

        cache.insert(blob.clone(), shard, 7, reference(2));
        assert_eq!(cache.get(&blob, shard, 7), Some(reference(2)));
        assert_eq!(cache.get(&blob, shard, 8), None);

        cache.insert(blob.clone(), shard, 7, reference(3));
        assert_eq!(cache.get(&blob, shard, 7), Some(reference(3)));
    }

    #[test]
    fn clock_eviction_keeps_the_cache_bounded() {
        let cache = RelocationCache::new(1);
        let shard = ShardKey {
            id: 1,
            generation: 0,
        };
        let first = key(b"first");
        let second = key(b"second");

        cache.insert(first.clone(), shard, 1, reference(1));
        cache.insert(second.clone(), shard, 2, reference(2));

        assert_eq!(cache.len(), 1);
        assert!(cache.get(&first, shard, 1).is_none() || cache.get(&second, shard, 2).is_none());
    }

    #[test]
    fn compaction_drop_removes_only_the_exact_cached_destination() {
        let cache = RelocationCache::new(2);
        let blob = key(b"blob");
        let shard = ShardKey {
            id: 1,
            generation: 0,
        };
        let dropped = RelocationEntry {
            key: blob.clone(),
            shard,
            payload_lsn: 7,
            publish_lsn: 8,
            to: reference(2),
        };

        cache.insert(blob.clone(), shard, 7, reference(2));
        assert_eq!(cache.remove_dropped(std::slice::from_ref(&dropped)), 1);
        assert_eq!(cache.len(), 0);

        cache.insert(blob.clone(), shard, 7, reference(3));
        assert_eq!(cache.remove_dropped(&[dropped]), 0);
        assert_eq!(cache.get(&blob, shard, 7), Some(reference(3)));
    }
}
