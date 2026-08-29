use std::sync::Arc;

use core_types::{
    Epoch, SegmentGcSummary, SegmentId, SegmentState, ShardCleanupJob, ShardId, ShardInfo,
    ShardKey, StoreStateKey, StrataLsn,
};
use lsm::{GarbageLogPosition, Manifest};
use typed_store::rocks::{DBBatch, DBMap, RocksDB};

use crate::{Error, Result};

use super::{StrataIndex, StrataIndexCfNames};

impl StrataIndex {
    pub fn db(&self) -> &Arc<RocksDB> {
        &self.db
    }

    pub fn cf_names(&self) -> &StrataIndexCfNames {
        &self.cf_names
    }

    pub fn batch(&self) -> DBBatch {
        self.store_state.batch()
    }

    pub fn segment_states(&self) -> &DBMap<SegmentId, SegmentState> {
        &self.segment_states
    }

    pub fn segment_publication_lsns(&self) -> &DBMap<SegmentId, StrataLsn> {
        &self.segment_publication_lsns
    }

    pub fn segment_gc_summaries(&self) -> &DBMap<SegmentId, SegmentGcSummary> {
        &self.segment_gc_summaries
    }

    pub fn gc_reclaim_pending(&self) -> &DBMap<(SegmentId, StrataLsn), u64> {
        &self.gc_reclaim_pending
    }

    pub fn gc_reclaim_strategies(&self) -> &DBMap<(SegmentId, StrataLsn), String> {
        &self.gc_reclaim_strategies
    }

    pub fn shards(&self) -> &DBMap<ShardId, ShardInfo> {
        &self.shards
    }

    pub fn shard_cleanup_jobs(&self) -> &DBMap<ShardKey, ShardCleanupJob> {
        &self.shard_cleanup_jobs
    }

    pub fn store_state(&self) -> &DBMap<StoreStateKey, StrataLsn> {
        &self.store_state
    }

    pub fn epoch_changes(&self) -> &DBMap<StrataLsn, Epoch> {
        &self.epoch_changes
    }

    pub fn lsm_manifests(&self) -> &DBMap<String, Manifest> {
        &self.lsm_manifests
    }

    pub fn garbage_log_positions(&self) -> &DBMap<String, GarbageLogPosition> {
        &self.garbage_log_positions
    }

    pub fn segment_garbage_log_positions(&self) -> &DBMap<SegmentId, u64> {
        &self.segment_garbage_log_positions
    }

    pub fn flush_wal(&self, sync: bool) -> Result<()> {
        match self.db.as_ref() {
            RocksDB::DB(db) => db
                .underlying
                .flush_wal(sync)
                .map_err(|error| Error::RocksDb(error.into_string())),
            RocksDB::OptimisticTransactionDB(_) => Err(Error::RocksDb(
                "flush_wal is not supported for optimistic transaction RocksDB".to_owned(),
            )),
        }
    }
}
