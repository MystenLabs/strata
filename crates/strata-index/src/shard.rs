use std::collections::BTreeMap;

use strata_core::{ShardId, ShardInfo, ShardKey, ShardState};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    /// Loads the shard registry from RocksDB into the process local durable view.
    ///
    /// Blob version merge operators and read paths need shard generation decisions without doing
    /// a RocksDB lookup inside every fold. Loading the registry into this cache gives them a
    /// process local view of which physical writer generations are still valid.
    /// There is another important reason to load the shard registry into the process local durable view:
    /// the process local durable view of shard generation is only updated after wal sync which prevents
    /// compaction and merge operators from discardign data too soon in case of a crash.
    pub(crate) fn load_shard_infos(&self) -> Result<()> {
        let shard_infos = self
            .shards
            .safe_iter()?
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()
            .map_err(Error::from)?;
        *self
            .shard_infos
            .write()
            .expect("shard info cache lock poisoned") = shard_infos;
        Ok(())
    }

    /// Publishes a shard registry update to the process local durable view.
    ///
    /// This must only be called after the matching RocksDB batch is written and the RocksDB WAL has
    /// been synced. The cache is consumed by blob version merge operators, compaction filters, and
    /// read resolution to decide whether a shard generation is obsolete. Those decisions can be
    /// destructive: compaction may remove packed blob version entries for an obsolete generation.
    ///
    /// A direct RocksDB read is too early for that purpose. After `batch.write()` but before
    /// `flush_wal(true)`, the new shard row is visible to reads but may still be lost by crash
    /// recovery. For example, suppose the durable state is `shard 7 -> Active generation 0`, and a
    /// drop writes `shard 7 -> Dropped generation 0` but crashes before the WAL sync. If compaction
    /// used the visible but not durable row, it could prune generation-0 blob metadata. Recovery may
    /// then restore `Active generation 0`, leaving the shard current but its blob metadata removed.
    ///
    /// Keeping this cache behind the WAL sync frontier gives pruning logic a view that should still
    /// be true after recovery.
    pub fn set_cached_shard_info(&self, shard_id: ShardId, info: ShardInfo) {
        self.shard_infos
            .write()
            .expect("shard info cache lock poisoned")
            .insert(shard_id, info);
    }

    pub(crate) fn shard_generation_is_cached_obsolete(&self, shard: ShardKey) -> bool {
        let shard_infos = self
            .shard_infos
            .read()
            .expect("shard info cache lock poisoned");
        shard_generation_is_obsolete(shard, &shard_infos)
    }

    pub fn get_shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.shards.get(&shard_id)?)
    }

    pub fn put_shard_info(&self, shard_id: ShardId, info: ShardInfo) -> Result<()> {
        let mut batch = self.batch();
        self.put_shard_info_batch(&mut batch, shard_id, info)?;
        batch.write()?;
        self.flush_wal(true)?;
        // Update the cache only after the durable registry write succeeds. A cache entry that races
        // ahead of RocksDB could make this process prune or hide a generation that recovery would
        // still consider current after a crash.
        self.set_cached_shard_info(shard_id, info);
        Ok(())
    }

    pub fn put_shard_info_batch(
        &self,
        batch: &mut DBBatch,
        shard_id: ShardId,
        info: ShardInfo,
    ) -> Result<()> {
        batch
            .insert_batch(self.shards(), [(&shard_id, &info)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_shards(&self) -> Result<Vec<(ShardId, ShardInfo)>> {
        self.shards
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn remove_shard_keyed_metadata_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
    ) -> Result<()> {
        // Dropping a shard generation removes metadata keyed by the full physical generation, not
        // just the logical shard id. That keeps a later reincarnation of the same shard id from
        // inheriting segment manifests or stats that belonged to the old writer.
        let segment_state_keys = self
            .segment_states
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.shard == shard => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        batch.delete_batch(&self.segment_states, segment_state_keys)?;

        let segment_stats_keys = self
            .segment_stats
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.shard == shard => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        batch.delete_batch(&self.segment_stats, segment_stats_keys)?;

        Ok(())
    }
}

pub(crate) fn shard_generation_is_obsolete(
    shard: ShardKey,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> bool {
    // Generation is the stale-write guard. Older generations are always obsolete, and the current
    // generation becomes obsolete once the shard is explicitly dropped. A missing registry entry is
    // treated as not obsolete so bootstrap/recovery can rebuild state without speculative pruning.
    match shard_infos.get(&shard.id) {
        Some(info) if shard.generation < info.current_generation => true,
        Some(info)
            if shard.generation == info.current_generation && info.state == ShardState::Dropped =>
        {
            true
        }
        _ => false,
    }
}
