pub(crate) const BLOB_VERSIONS_CF: &str = "blob_versions";
pub(crate) const SEGMENT_STATES_CF: &str = "segment_states";
pub(crate) const SEGMENT_STATS_CF: &str = "segment_stats";
pub(crate) const SEGMENT_REF_STATE_CF: &str = "segment_ref_state";
pub(crate) const SEGMENT_REF_EVENTS_CF: &str = "segment_ref_events";
pub(crate) const SEGMENT_GC_OVERLAY_CF: &str = "segment_gc_overlay";
pub(crate) const SHARDS_CF: &str = "shards";
pub(crate) const STORE_STATE_CF: &str = "store_state";
pub(crate) const EPOCH_CHANGES_CF: &str = "epoch_changes";
pub(crate) const UNACCOUNTED_LSN_OPS_CF: &str = "unaccounted_lsn_ops";
pub(crate) const ACCOUNTING_INDEX_CF: &str = "accounting_index";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrataIndexCfNames {
    pub blob_versions: String,
    pub segment_states: String,
    pub segment_stats: String,
    pub segment_ref_state: String,
    pub segment_ref_events: String,
    pub segment_gc_overlay: String,
    pub shards: String,
    pub store_state: String,
    pub epoch_changes: String,
    pub unaccounted_lsn_ops: String,
    pub accounting_index: String,
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
            blob_versions: with_prefix(BLOB_VERSIONS_CF),
            segment_states: with_prefix(SEGMENT_STATES_CF),
            segment_stats: with_prefix(SEGMENT_STATS_CF),
            segment_ref_state: with_prefix(SEGMENT_REF_STATE_CF),
            segment_ref_events: with_prefix(SEGMENT_REF_EVENTS_CF),
            segment_gc_overlay: with_prefix(SEGMENT_GC_OVERLAY_CF),
            shards: with_prefix(SHARDS_CF),
            store_state: with_prefix(STORE_STATE_CF),
            epoch_changes: with_prefix(EPOCH_CHANGES_CF),
            unaccounted_lsn_ops: with_prefix(UNACCOUNTED_LSN_OPS_CF),
            accounting_index: with_prefix(ACCOUNTING_INDEX_CF),
        }
    }

    pub(crate) fn as_strs(&self) -> [&str; 11] {
        [
            self.blob_versions.as_str(),
            self.segment_states.as_str(),
            self.segment_stats.as_str(),
            self.segment_ref_state.as_str(),
            self.segment_ref_events.as_str(),
            self.segment_gc_overlay.as_str(),
            self.shards.as_str(),
            self.store_state.as_str(),
            self.epoch_changes.as_str(),
            self.unaccounted_lsn_ops.as_str(),
            self.accounting_index.as_str(),
        ]
    }
}
