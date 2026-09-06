use std::{collections::BTreeMap, path::Path, sync::Arc};
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, default_db_options, open_cf_opts},
};

use crate::Result;

use super::cf::{
    EPOCH_CHANGES_CF, GARBAGE_LOG_POSITIONS_CF, GC_RECLAIM_PENDING_CF, GC_RECLAIM_STRATEGIES_CF,
    LSM_MANIFESTS_CF, SEGMENT_GARBAGE_LOG_POSITIONS_CF, SEGMENT_GC_SUMMARIES_CF,
    SEGMENT_PUBLICATION_LSNS_CF, SEGMENT_STATES_CF, SHARD_CLEANUP_JOBS_CF, SHARDS_CF,
    STORE_STATE_CF,
};
use super::migration::migrate_and_drop_retired_cfs;
use super::options::cf_options;
use super::{StrataIndex, StrataIndexCfNames};

impl StrataIndex {
    /// Opens a dedicated typed-store RocksDB instance at `path`.
    ///
    /// `metric_suffix` is appended to the `strata_index` RocksDB metric label. Use a stable,
    /// low-cardinality index identifier such as a namespace or configured instance name.
    pub fn open_path(
        path: impl AsRef<Path>,
        cf_prefix: impl AsRef<str>,
        metric_suffix: impl AsRef<str>,
    ) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cf_options = cf_options(&cf_names);
        let cf_options = cf_options
            .iter()
            .map(|(name, options)| (name.as_str(), options.clone()))
            .collect::<Vec<_>>();
        let db = open_cf_opts(
            path,
            Some(default_db_options().options),
            metric_conf_with_suffix("strata_index", metric_suffix),
            &cf_options,
        )?;
        Self::from_db_with_cf_names(db, cf_names)
    }

    /// Attaches Strata index column families to an already-open typed-store RocksDB instance.
    ///
    /// Missing column families are created using typed-store default RocksDB options.
    pub fn from_db(db: Arc<RocksDB>, cf_prefix: impl AsRef<str>) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cf_options = cf_options(&cf_names)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for cf in cf_names.as_strs() {
            if db.cf_handle(cf).is_none() {
                db.create_cf(cf, &cf_options[cf])
                    .map_err(|err| TypedStoreError::RocksDBError(err.into_string()))?;
            }
        }
        Self::from_db_with_cf_names(db, cf_names)
    }

    fn from_db_with_cf_names(db: Arc<RocksDB>, cf_names: StrataIndexCfNames) -> Result<Self> {
        let rw_options = ReadWriteOptions::default();
        let segment_states = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_states),
            Some(SEGMENT_STATES_CF),
            &rw_options,
            true,
        )?;
        let segment_publication_lsns = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_publication_lsns),
            Some(SEGMENT_PUBLICATION_LSNS_CF),
            &rw_options,
            true,
        )?;
        let segment_gc_summaries = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_gc_summaries),
            Some(SEGMENT_GC_SUMMARIES_CF),
            &rw_options,
            true,
        )?;
        let gc_reclaim_pending = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.gc_reclaim_pending),
            Some(GC_RECLAIM_PENDING_CF),
            &rw_options,
            true,
        )?;
        let gc_reclaim_strategies = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.gc_reclaim_strategies),
            Some(GC_RECLAIM_STRATEGIES_CF),
            &rw_options,
            true,
        )?;
        let shards = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.shards),
            Some(SHARDS_CF),
            &rw_options,
            true,
        )?;
        let shard_cleanup_jobs = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.shard_cleanup_jobs),
            Some(SHARD_CLEANUP_JOBS_CF),
            &rw_options,
            true,
        )?;
        let store_state = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.store_state),
            Some(STORE_STATE_CF),
            &rw_options,
            true,
        )?;
        let epoch_changes = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.epoch_changes),
            Some(EPOCH_CHANGES_CF),
            &rw_options,
            true,
        )?;
        let lsm_manifests = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.lsm_manifests),
            Some(LSM_MANIFESTS_CF),
            &rw_options,
            true,
        )?;
        let garbage_log_positions = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.garbage_log_positions),
            Some(GARBAGE_LOG_POSITIONS_CF),
            &rw_options,
            true,
        )?;
        let segment_garbage_log_positions = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_garbage_log_positions),
            Some(SEGMENT_GARBAGE_LOG_POSITIONS_CF),
            &rw_options,
            true,
        )?;
        migrate_and_drop_retired_cfs(&db, &cf_names, &shard_cleanup_jobs)?;
        Ok(Self {
            db,
            cf_names,
            segment_states,
            segment_publication_lsns,
            segment_gc_summaries,
            gc_reclaim_pending,
            gc_reclaim_strategies,
            shards,
            shard_cleanup_jobs,
            store_state,
            epoch_changes,
            lsm_manifests,
            manifest_publish_lock: Arc::new(std::sync::Mutex::new(())),
            garbage_log_positions,
            segment_garbage_log_positions,
            overlay_cache: Arc::new(std::sync::Mutex::new(
                crate::overlay_cache::OverlayCache::new(crate::sweeper::OVERLAY_CACHE_BYTES),
            )),
        })
    }
}

pub(crate) fn metric_conf_with_suffix(base: &str, suffix: impl AsRef<str>) -> MetricConf {
    let suffix = suffix.as_ref();
    if suffix.is_empty() {
        MetricConf::new(base)
    } else {
        MetricConf::new(&format!("{base}_{suffix}"))
    }
}
