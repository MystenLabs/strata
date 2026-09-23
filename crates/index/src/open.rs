use std::{collections::BTreeMap, path::Path, sync::Arc};

use crate::Result;
use crate::port::{IndexDb, RocksBackend, TypedMap, options::default_db_options};

use super::migration::migrate_and_drop_retired_cfs;
use super::options::cf_options;
use super::{StrataIndex, StrataIndexCfNames};

impl StrataIndex {
    /// Opens a dedicated RocksDB instance at `path`.
    pub fn open_path(
        path: impl AsRef<Path>,
        cf_prefix: impl AsRef<str>,
        _metric_suffix: impl AsRef<str>,
    ) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cf_options = cf_options(&cf_names);
        let db = RocksBackend::open(path, Some(default_db_options()), &cf_options)?;
        Self::from_db_with_cf_names(Arc::new(db), cf_names)
    }

    /// Attaches Strata index column families to a storage handle the caller already opened.
    ///
    /// This is the embedding seam: a host that runs its own RocksDB implements [`IndexDb`] over
    /// it and Strata's families live in that instance, without Strata depending on the host's
    /// RocksDB wrapper.
    ///
    /// Missing column families are created using Strata's own options for that family.
    pub fn from_db(db: Arc<dyn IndexDb>, cf_prefix: impl AsRef<str>) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cf_options = cf_options(&cf_names)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for cf in cf_names.as_strs() {
            if !db.cf_exists(cf) {
                db.create_cf(cf, &cf_options[cf])?;
            }
        }
        Self::from_db_with_cf_names(db, cf_names)
    }

    fn from_db_with_cf_names(db: Arc<dyn IndexDb>, cf_names: StrataIndexCfNames) -> Result<Self> {
        let segment_states = TypedMap::new(Arc::clone(&db), &cf_names.segment_states);
        let segment_publication_lsns =
            TypedMap::new(Arc::clone(&db), &cf_names.segment_publication_lsns);
        let segment_gc_summaries = TypedMap::new(Arc::clone(&db), &cf_names.segment_gc_summaries);
        let gc_reclaim_pending = TypedMap::new(Arc::clone(&db), &cf_names.gc_reclaim_pending);
        let gc_reclaim_strategies = TypedMap::new(Arc::clone(&db), &cf_names.gc_reclaim_strategies);
        let shards = TypedMap::new(Arc::clone(&db), &cf_names.shards);
        let shard_cleanup_jobs = TypedMap::new(Arc::clone(&db), &cf_names.shard_cleanup_jobs);
        let store_state = TypedMap::new(Arc::clone(&db), &cf_names.store_state);
        let epoch_changes = TypedMap::new(Arc::clone(&db), &cf_names.epoch_changes);
        let lsm_manifests = TypedMap::new(Arc::clone(&db), &cf_names.lsm_manifests);
        let garbage_log_positions = TypedMap::new(Arc::clone(&db), &cf_names.garbage_log_positions);
        let segment_garbage_log_positions =
            TypedMap::new(Arc::clone(&db), &cf_names.segment_garbage_log_positions);
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
