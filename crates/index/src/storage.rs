use std::sync::Arc;

use crate::port::{IndexDb, TypedMap, map::IndexBatch};
use core_types::{
    Epoch, SegmentGcSummary, SegmentId, SegmentState, ShardCleanupJob, ShardId, ShardInfo,
    ShardKey, StoreStateKey, StrataLsn,
};
use lsm::{GarbageLogPosition, Manifest};

use crate::Result;

use super::{StrataIndex, StrataIndexCfNames};

impl StrataIndex {
    pub fn db(&self) -> &Arc<dyn IndexDb> {
        &self.db
    }

    pub fn cf_names(&self) -> &StrataIndexCfNames {
        &self.cf_names
    }

    pub fn batch(&self) -> IndexBatch {
        self.store_state.batch()
    }

    pub fn segment_states(&self) -> &TypedMap<SegmentId, SegmentState> {
        &self.segment_states
    }

    pub fn segment_publication_lsns(&self) -> &TypedMap<SegmentId, StrataLsn> {
        &self.segment_publication_lsns
    }

    pub fn segment_gc_summaries(&self) -> &TypedMap<SegmentId, SegmentGcSummary> {
        &self.segment_gc_summaries
    }

    pub fn gc_reclaim_pending(&self) -> &TypedMap<(SegmentId, StrataLsn), u64> {
        &self.gc_reclaim_pending
    }

    pub fn gc_reclaim_strategies(&self) -> &TypedMap<(SegmentId, StrataLsn), String> {
        &self.gc_reclaim_strategies
    }

    pub fn shards(&self) -> &TypedMap<ShardId, ShardInfo> {
        &self.shards
    }

    pub fn shard_cleanup_jobs(&self) -> &TypedMap<ShardKey, ShardCleanupJob> {
        &self.shard_cleanup_jobs
    }

    pub fn store_state(&self) -> &TypedMap<StoreStateKey, StrataLsn> {
        &self.store_state
    }

    pub fn epoch_changes(&self) -> &TypedMap<StrataLsn, Epoch> {
        &self.epoch_changes
    }

    pub fn lsm_manifests(&self) -> &TypedMap<String, Manifest> {
        &self.lsm_manifests
    }

    pub fn garbage_log_positions(&self) -> &TypedMap<String, GarbageLogPosition> {
        &self.garbage_log_positions
    }

    pub fn segment_garbage_log_positions(&self) -> &TypedMap<SegmentId, u64> {
        &self.segment_garbage_log_positions
    }

    pub fn flush_wal(&self, sync: bool) -> Result<()> {
        self.db.flush_wal(sync)
    }
}
