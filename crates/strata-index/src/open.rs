use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use strata_core::{ShardId, ShardInfo, StrataLsn};
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, default_db_options, open_cf_opts},
};

use crate::Result;

use super::cf::{
    ACCOUNTING_INDEX_CF, BLOB_VERSIONS_CF, EPOCH_CHANGES_CF, GC_RECLAIM_PENDING_CF,
    GC_RELOCATIONS_CF, SEGMENT_GC_OVERLAY_CF, SEGMENT_REF_EVENTS_CF, SEGMENT_STATES_CF, SHARDS_CF,
    STORE_STATE_CF, UNACCOUNTED_LSN_OPS_CF,
};
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
        let compact_safe_lsn = Arc::new(RwLock::new(0));
        let shard_infos = Arc::new(RwLock::new(BTreeMap::new()));
        let cf_options = cf_options(
            &cf_names,
            Arc::clone(&compact_safe_lsn),
            Arc::clone(&shard_infos),
        );
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
        Self::from_db_with_cf_names(db, cf_names, compact_safe_lsn, shard_infos)
    }

    /// Attaches Strata index column families to an already-open typed-store RocksDB instance.
    ///
    /// Missing column families are created using typed-store default RocksDB options.
    pub fn from_db(db: Arc<RocksDB>, cf_prefix: impl AsRef<str>) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let compact_safe_lsn = Arc::new(RwLock::new(0));
        let shard_infos = Arc::new(RwLock::new(BTreeMap::new()));
        let cf_options = cf_options(
            &cf_names,
            Arc::clone(&compact_safe_lsn),
            Arc::clone(&shard_infos),
        )
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        for cf in cf_names.as_strs() {
            if db.cf_handle(cf).is_none() {
                db.create_cf(cf, &cf_options[cf])
                    .map_err(|err| TypedStoreError::RocksDBError(err.into_string()))?;
            }
        }
        Self::from_db_with_cf_names(db, cf_names, compact_safe_lsn, shard_infos)
    }

    fn from_db_with_cf_names(
        db: Arc<RocksDB>,
        cf_names: StrataIndexCfNames,
        compact_safe_lsn: Arc<RwLock<StrataLsn>>,
        shard_infos_cache: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    ) -> Result<Self> {
        let rw_options = ReadWriteOptions::default();
        let blob_versions = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.blob_versions),
            Some(BLOB_VERSIONS_CF),
            &rw_options,
            true,
        )?;
        let segment_states = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_states),
            Some(SEGMENT_STATES_CF),
            &rw_options,
            true,
        )?;
        let segment_ref_events = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_ref_events),
            Some(SEGMENT_REF_EVENTS_CF),
            &rw_options,
            true,
        )?;
        let segment_gc_overlay = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_gc_overlay),
            Some(SEGMENT_GC_OVERLAY_CF),
            &rw_options,
            true,
        )?;
        let gc_relocations = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.gc_relocations),
            Some(GC_RELOCATIONS_CF),
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
        let shards = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.shards),
            Some(SHARDS_CF),
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
        let unaccounted_lsn_ops = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.unaccounted_lsn_ops),
            Some(UNACCOUNTED_LSN_OPS_CF),
            &rw_options,
            true,
        )?;
        let accounting_index = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.accounting_index),
            Some(ACCOUNTING_INDEX_CF),
            &rw_options,
            true,
        )?;
        let index = Self {
            db,
            cf_names,
            blob_compact_safe_lsn: compact_safe_lsn,
            shard_infos: shard_infos_cache,
            accounting_snapshot_pins: Arc::new(Mutex::new(Default::default())),
            blob_versions,
            segment_states,
            segment_ref_events,
            segment_gc_overlay,
            gc_relocations,
            gc_reclaim_pending,
            shards,
            store_state,
            epoch_changes,
            unaccounted_lsn_ops,
            accounting_index,
        };
        index.load_shard_infos()?;
        index.load_blob_compact_safe_lsn()?;
        Ok(index)
    }
}

impl StrataIndex {
    pub(crate) fn load_blob_compact_safe_lsn(&self) -> Result<()> {
        *self
            .blob_compact_safe_lsn
            .write()
            .expect("blob version compaction frontier lock poisoned") = self.get_durable_lsn()?;
        Ok(())
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
