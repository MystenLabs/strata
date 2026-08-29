pub(crate) const SEGMENT_STATES_CF: &str = "segment_states";
pub(crate) const SEGMENT_PUBLICATION_LSNS_CF: &str = "segment_publication_lsns";
pub(crate) const SEGMENT_GC_SUMMARIES_CF: &str = "segment_gc_summaries";
pub(crate) const GC_RECLAIM_PENDING_CF: &str = "gc_reclaim_pending";
pub(crate) const GC_RECLAIM_STRATEGIES_CF: &str = "gc_reclaim_strategies";
pub(crate) const SHARDS_CF: &str = "shards";
pub(crate) const SHARD_CLEANUP_JOBS_CF: &str = "shard_cleanup_jobs";
pub(crate) const STORE_STATE_CF: &str = "store_state";
pub(crate) const EPOCH_CHANGES_CF: &str = "epoch_changes";
pub(crate) const LSM_MANIFESTS_CF: &str = "lsm_manifests";
pub(crate) const GARBAGE_LOG_POSITIONS_CF: &str = "garbage_log_positions";
pub(crate) const SEGMENT_GARBAGE_LOG_POSITIONS_CF: &str = "segment_garbage_log_positions";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrataIndexCfNames {
    pub segment_states: String,
    pub segment_publication_lsns: String,
    pub segment_gc_summaries: String,
    pub gc_reclaim_pending: String,
    pub gc_reclaim_strategies: String,
    pub shards: String,
    pub shard_cleanup_jobs: String,
    pub store_state: String,
    pub epoch_changes: String,
    pub lsm_manifests: String,
    pub garbage_log_positions: String,
    pub segment_garbage_log_positions: String,
}

impl StrataIndexCfNames {
    pub fn new(prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref().trim_matches('/');
        let with_prefix = |name: &str| {
            if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            }
        };

        Self {
            segment_states: with_prefix(SEGMENT_STATES_CF),
            segment_publication_lsns: with_prefix(SEGMENT_PUBLICATION_LSNS_CF),
            segment_gc_summaries: with_prefix(SEGMENT_GC_SUMMARIES_CF),
            gc_reclaim_pending: with_prefix(GC_RECLAIM_PENDING_CF),
            gc_reclaim_strategies: with_prefix(GC_RECLAIM_STRATEGIES_CF),
            shards: with_prefix(SHARDS_CF),
            shard_cleanup_jobs: with_prefix(SHARD_CLEANUP_JOBS_CF),
            store_state: with_prefix(STORE_STATE_CF),
            epoch_changes: with_prefix(EPOCH_CHANGES_CF),
            lsm_manifests: with_prefix(LSM_MANIFESTS_CF),
            garbage_log_positions: with_prefix(GARBAGE_LOG_POSITIONS_CF),
            segment_garbage_log_positions: with_prefix(SEGMENT_GARBAGE_LOG_POSITIONS_CF),
        }
    }

    pub(crate) fn as_strs(&self) -> [&str; 12] {
        [
            self.segment_states.as_str(),
            self.segment_publication_lsns.as_str(),
            self.segment_gc_summaries.as_str(),
            self.gc_reclaim_pending.as_str(),
            self.gc_reclaim_strategies.as_str(),
            self.shards.as_str(),
            self.shard_cleanup_jobs.as_str(),
            self.store_state.as_str(),
            self.epoch_changes.as_str(),
            self.lsm_manifests.as_str(),
            self.garbage_log_positions.as_str(),
            self.segment_garbage_log_positions.as_str(),
        ]
    }
}
