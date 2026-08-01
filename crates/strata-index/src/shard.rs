use std::collections::BTreeMap;

use strata_core::{SegmentFileState, SegmentOwner, ShardId, ShardInfo, ShardKey, ShardState};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    pub fn get_shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.shards.get(&shard_id)?)
    }

    pub fn put_shard_info(&self, shard_id: ShardId, info: ShardInfo) -> Result<()> {
        let mut batch = self.batch();
        self.put_shard_info_batch(&mut batch, shard_id, info)?;
        batch.write_with_sync(true)?;
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
        // Keep a small Deleted state tombstone after removing the shard generation's files.
        // Snapshot-driven blob-LSM compaction uses the owner on that row to distinguish
        // bulk-reclaimed records from records in mixed ingest segments, and the garbage sweeper
        // uses Deleted to discard any terminal event that raced with whole-file cleanup.
        let mut segment_states = self
            .segment_states
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((segment_id, state)) if state.owner == SegmentOwner::Shard(shard) => {
                    Some(Ok((segment_id, state)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()
            .map_err(Error::from)?;
        for state in segment_states.values_mut() {
            state.state = SegmentFileState::Deleted;
            self.put_segment_state_batch(batch, state)?;
        }
        let segment_ids = segment_states.keys().copied().collect::<Vec<_>>();

        if !segment_ids.is_empty() {
            batch.delete_batch(&self.segment_publication_lsns, segment_ids.iter().copied())?;
            batch.delete_batch(&self.segment_gc_summaries, segment_ids.iter().copied())?;
            batch.delete_batch(
                &self.segment_garbage_log_positions,
                segment_ids.iter().copied(),
            )?;
            let reclaim_pending_keys = self
                .gc_reclaim_pending
                .safe_iter()?
                .filter_map(|result| match result {
                    Ok((key, _)) if segment_ids.contains(&key.0) => Some(Ok(key)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::from)?;
            batch.delete_batch(&self.gc_reclaim_pending, reclaim_pending_keys)?;
        }

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
