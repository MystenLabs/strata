//! RocksDB-backed Strata metadata indexes using Walrus typed-store.
//!
//! This crate stores durable metadata in RocksDB using Walrus typed-store.
//!
//! Column families:
//!
//! ```text
//! StrataIndex
//! +-----------------+-----------------------------------------------+
//! | blob_versions   | (BlobKey, LSN) -> BlobEntry version/delta     |
//! | segment_states  | SegmentId -> SegmentState                     |
//! | segment_stats   | SegmentId -> SegmentStats                     |
//! | store_state     | StoreStateKey -> u64                         |
//! | epoch_changes       | LSN -> current Epoch                      |
//! | unaccounted_lsn_ops | LSN -> BlobKey                            |
//! +-----------------+-----------------------------------------------+
//! ```
//!
//! The `blob_versions` table is append-only from the store's point of view: puts, snapshots,
//! extension deltas, and tombstones create newer versions instead of mutating old rows. Extension
//! deltas carry the latest logical end epoch without repeating the payload reference. The store
//! resolves them by walking backward to the latest payload-bearing version.
//! `unaccounted_lsn_ops` is the LSN-to-blob-key index used by durability, recovery, and later
//! accounting. Rows are written with blob ops, retained after durability, removed on rollback if
//! lost, and will be removed by accounting once consumed.
//!
//! Namespacing is handled by prefixing column family names:
//!
//! ```text
//! strata/<namespace>/blob_versions
//! strata/<namespace>/segment_states
//! ...
//! ```

mod error;

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use strata_core::{
    BlobEntry, BlobKey, BlobVersionKey, Epoch, SegmentId, SegmentState, SegmentStats,
    StoreStateKey, StrataLsn, StrataStoreState,
};
use typed_store::{
    Map, TypedStoreError,
    rocks::{DBBatch, DBMap, MetricConf, ReadWriteOptions, RocksDB, default_db_options, open_cf},
};

pub use error::{Error, Result};

const BLOB_VERSIONS_CF: &str = "blob_versions";
const SEGMENT_STATES_CF: &str = "segment_states";
const SEGMENT_STATS_CF: &str = "segment_stats";
const STORE_STATE_CF: &str = "store_state";
const EPOCH_CHANGES_CF: &str = "epoch_changes";
const UNACCOUNTED_LSN_OPS_CF: &str = "unaccounted_lsn_ops";
static NEXT_METRIC_ID: AtomicU64 = AtomicU64::new(0);

/// Typed-store backed Strata metadata index.
#[derive(Clone, Debug)]
pub struct StrataIndex {
    /// Shared typed-store RocksDB handle used for all Strata metadata column families.
    db: Arc<RocksDB>,
    /// Fully-qualified column family names, including the caller's namespace prefix.
    cf_names: StrataIndexCfNames,
    /// Version table: append-only blob versions keyed by `(blob key, LSN)`.
    blob_versions: DBMap<BlobVersionKey, BlobEntry>,
    /// Durable manifest for each segment: path, state, offsets, placement, LSN bounds, and digest.
    segment_states: DBMap<SegmentId, SegmentState>,
    /// Segment-level accounting used by cleanup planning without scanning payload files.
    segment_stats: DBMap<SegmentId, SegmentStats>,
    /// Store cursor fields: next assigned, highest durable, and highest accounted LSNs.
    store_state: DBMap<StoreStateKey, StrataLsn>,
    /// Epoch timeline. LSN 0 is the genesis epoch; later rows are explicit epoch increments.
    epoch_changes: DBMap<StrataLsn, Epoch>,
    /// Blob-key operations keyed by LSN, retained until the accounting worker consumes them.
    unaccounted_lsn_ops: DBMap<StrataLsn, BlobKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrataIndexCfNames {
    pub blob_versions: String,
    pub segment_states: String,
    pub segment_stats: String,
    pub store_state: String,
    pub epoch_changes: String,
    pub unaccounted_lsn_ops: String,
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
            store_state: with_prefix(STORE_STATE_CF),
            epoch_changes: with_prefix(EPOCH_CHANGES_CF),
            unaccounted_lsn_ops: with_prefix(UNACCOUNTED_LSN_OPS_CF),
        }
    }

    fn as_strs(&self) -> [&str; 6] {
        [
            self.blob_versions.as_str(),
            self.segment_states.as_str(),
            self.segment_stats.as_str(),
            self.store_state.as_str(),
            self.epoch_changes.as_str(),
            self.unaccounted_lsn_ops.as_str(),
        ]
    }
}

impl StrataIndex {
    /// Opens a dedicated typed-store RocksDB instance at `path`.
    pub fn open_path(path: impl AsRef<Path>, cf_prefix: impl AsRef<str>) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cfs = cf_names.as_strs();
        let db = open_cf(path, None, unique_metric_conf("strata_index"), &cfs)?;
        Self::from_db_with_cf_names(db, cf_names)
    }

    /// Attaches Strata index column families to an already-open typed-store RocksDB instance.
    ///
    /// Missing column families are created using typed-store default RocksDB options.
    pub fn from_db(db: Arc<RocksDB>, cf_prefix: impl AsRef<str>) -> Result<Self> {
        let cf_names = StrataIndexCfNames::new(cf_prefix);
        let cf_options = default_db_options().options;
        for cf in cf_names.as_strs() {
            if db.cf_handle(cf).is_none() {
                db.create_cf(cf, &cf_options)
                    .map_err(|err| TypedStoreError::RocksDBError(err.into_string()))?;
            }
        }
        Self::from_db_with_cf_names(db, cf_names)
    }

    fn from_db_with_cf_names(db: Arc<RocksDB>, cf_names: StrataIndexCfNames) -> Result<Self> {
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
        let segment_stats = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_stats),
            Some(SEGMENT_STATS_CF),
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

        Ok(Self {
            db,
            cf_names,
            blob_versions,
            segment_states,
            segment_stats,
            store_state,
            epoch_changes,
            unaccounted_lsn_ops,
        })
    }

    pub fn db(&self) -> &Arc<RocksDB> {
        &self.db
    }

    pub fn cf_names(&self) -> &StrataIndexCfNames {
        &self.cf_names
    }

    pub fn batch(&self) -> DBBatch {
        self.blob_versions.batch()
    }

    pub fn blob_versions(&self) -> &DBMap<BlobVersionKey, BlobEntry> {
        &self.blob_versions
    }

    pub fn segment_states(&self) -> &DBMap<SegmentId, SegmentState> {
        &self.segment_states
    }

    pub fn segment_stats(&self) -> &DBMap<SegmentId, SegmentStats> {
        &self.segment_stats
    }

    pub fn store_state(&self) -> &DBMap<StoreStateKey, StrataLsn> {
        &self.store_state
    }

    pub fn epoch_changes(&self) -> &DBMap<StrataLsn, Epoch> {
        &self.epoch_changes
    }

    pub fn unaccounted_lsn_ops(&self) -> &DBMap<StrataLsn, BlobKey> {
        &self.unaccounted_lsn_ops
    }

    pub fn get_blob_entry(&self, key: &BlobKey) -> Result<Option<BlobEntry>> {
        Ok(self.latest_blob_version(key)?.map(|(_, entry)| entry))
    }

    pub fn contains_blob(&self, key: &BlobKey) -> Result<bool> {
        Ok(self.latest_blob_version(key)?.is_some())
    }

    pub fn put_blob_entry(&self, key: &BlobKey, entry: &BlobEntry) -> Result<()> {
        let mut batch = self.batch();
        self.put_blob_entry_batch(&mut batch, key, entry)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_blob_entry_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        self.put_blob_version_batch(batch, key, entry)
    }

    pub fn put_blob_version_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        batch.insert_batch(
            &self.blob_versions,
            [(
                &BlobVersionKey {
                    key: key.clone(),
                    lsn: entry.lsn,
                },
                entry,
            )],
        )?;
        Ok(())
    }

    pub fn latest_blob_version(
        &self,
        key: &BlobKey,
    ) -> Result<Option<(BlobVersionKey, BlobEntry)>> {
        let lower = BlobVersionKey {
            key: key.clone(),
            lsn: 0,
        };
        let upper = BlobVersionKey {
            key: key.clone(),
            lsn: StrataLsn::MAX,
        };

        for result in self
            .blob_versions
            .reversed_safe_iter_with_bounds(Some(lower), Some(upper))?
        {
            let (version_key, entry) = result?;
            if version_key.key == *key {
                return Ok(Some((version_key, entry)));
            }
        }

        Ok(None)
    }

    pub fn reversed_blob_versions(
        &self,
        key: &BlobKey,
        max_lsn: StrataLsn,
    ) -> Result<Vec<(BlobVersionKey, BlobEntry)>> {
        let lower = BlobVersionKey {
            key: key.clone(),
            lsn: 0,
        };
        let upper = BlobVersionKey {
            key: key.clone(),
            lsn: max_lsn,
        };
        let mut versions = Vec::new();

        for result in self
            .blob_versions
            .reversed_safe_iter_with_bounds(Some(lower), Some(upper))?
        {
            let (version_key, entry) = result?;
            if version_key.key == *key {
                versions.push((version_key, entry));
            }
        }

        Ok(versions)
    }

    pub fn get_blob_version(&self, key: &BlobVersionKey) -> Result<Option<BlobEntry>> {
        Ok(self.blob_versions.get(key)?)
    }

    pub fn remove_blob_versions_batch(
        &self,
        batch: &mut DBBatch,
        hidden_versions: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        for (key, lsn) in hidden_versions {
            batch.delete_batch(
                &self.blob_versions,
                [BlobVersionKey {
                    key: key.clone(),
                    lsn: *lsn,
                }],
            )?;
        }

        Ok(())
    }

    pub fn get_segment_state(&self, segment_id: SegmentId) -> Result<Option<SegmentState>> {
        Ok(self.segment_states.get(&segment_id)?)
    }

    pub fn put_segment_state(&self, state: &SegmentState) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_state_batch(&mut batch, state)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_state_batch(&self, batch: &mut DBBatch, state: &SegmentState) -> Result<()> {
        batch
            .insert_batch(self.segment_states(), [(&state.segment_id, state)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_segment_stats(&self, segment_id: SegmentId) -> Result<Option<SegmentStats>> {
        Ok(self.segment_stats.get(&segment_id)?)
    }

    pub fn put_segment_stats(&self, segment_id: SegmentId, stats: &SegmentStats) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_stats_batch(&mut batch, segment_id, stats)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_stats_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        stats: &SegmentStats,
    ) -> Result<()> {
        batch
            .insert_batch(self.segment_stats(), [(&segment_id, stats)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_next_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::NextLsn)?
            .unwrap_or_else(|| StrataStoreState::default().next_lsn))
    }

    pub fn get_durable_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::DurableLsn)?
            .unwrap_or_else(|| StrataStoreState::default().durable_lsn))
    }

    pub fn get_accounted_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::AccountedLsn)?
            .unwrap_or_else(|| StrataStoreState::default().accounted_lsn))
    }

    pub fn get_current_epoch(&self) -> Result<Option<Epoch>> {
        Ok(self.store_state.get(&StoreStateKey::CurrentEpoch)?)
    }

    pub fn get_store_state(&self) -> Result<Option<StrataStoreState>> {
        Ok(Some(StrataStoreState {
            next_lsn: self.get_next_lsn()?,
            durable_lsn: self.get_durable_lsn()?,
            accounted_lsn: self.get_accounted_lsn()?,
        }))
    }

    pub fn put_next_lsn_batch(&self, batch: &mut DBBatch, next_lsn: StrataLsn) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::NextLsn, &next_lsn)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_durable_lsn_batch(&self, batch: &mut DBBatch, durable_lsn: StrataLsn) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(&StoreStateKey::DurableLsn, &durable_lsn)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_accounted_lsn_batch(
        &self,
        batch: &mut DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(&StoreStateKey::AccountedLsn, &accounted_lsn)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_current_epoch_batch(&self, batch: &mut DBBatch, epoch: Epoch) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::CurrentEpoch, &epoch)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_store_state_batch(
        &self,
        batch: &mut DBBatch,
        state: &StrataStoreState,
    ) -> Result<()> {
        self.put_next_lsn_batch(batch, state.next_lsn)?;
        self.put_durable_lsn_batch(batch, state.durable_lsn)?;
        self.put_accounted_lsn_batch(batch, state.accounted_lsn)
    }

    pub fn put_epoch_change_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        epoch: Epoch,
    ) -> Result<()> {
        batch
            .insert_batch(self.epoch_changes(), [(&lsn, &epoch)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_epoch_change(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        Ok(self.epoch_changes.get(&lsn)?)
    }

    pub fn latest_epoch_at_lsn(&self, max_lsn: StrataLsn) -> Result<Option<(StrataLsn, Epoch)>> {
        for result in self
            .epoch_changes
            .reversed_safe_iter_with_bounds(None, Some(max_lsn))?
        {
            return result.map(Some).map_err(Error::from);
        }
        Ok(None)
    }

    pub fn iter_epoch_changes_from(&self, min_lsn: StrataLsn) -> Result<Vec<(StrataLsn, Epoch)>> {
        self.epoch_changes
            .safe_iter_with_bounds(Some(min_lsn), None)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn remove_epoch_changes_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        for lsn in lsns {
            batch.delete_batch(&self.epoch_changes, [lsn])?;
        }
        Ok(())
    }

    pub fn iter_unaccounted_lsn_ops(&self) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.unaccounted_lsn_ops
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn put_unaccounted_lsn_op_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        key: &BlobKey,
    ) -> Result<()> {
        batch
            .insert_batch(self.unaccounted_lsn_ops(), [(&lsn, key)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_unaccounted_lsn_ops_from(
        &self,
        min_lsn: StrataLsn,
    ) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.unaccounted_lsn_ops
            .safe_iter_with_bounds(Some(min_lsn), None)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn remove_unaccounted_lsn_ops_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        for lsn in lsns {
            batch.delete_batch(self.unaccounted_lsn_ops(), [lsn])?;
        }
        Ok(())
    }

    pub fn iter_segment_states(&self) -> Result<Vec<(SegmentId, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
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

fn unique_metric_conf(base: &str) -> MetricConf {
    let metric_id = NEXT_METRIC_ID.fetch_add(1, Ordering::Relaxed);
    MetricConf::new(&format!("{base}_{metric_id}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use strata_core::{BlobLifecycle, BlobState, PlacementClass, RecordRef, SegmentFileState};
    use tempfile::tempdir;
    use typed_store::{DBMetrics, rocks::open_cf};

    use super::*;

    static INIT_TYPED_STORE_METRICS: Once = Once::new();

    fn init_typed_store_metrics() {
        INIT_TYPED_STORE_METRICS.call_once(|| {
            DBMetrics::get();
        });
    }

    fn blob_entry(segment_id: SegmentId, offset: u64) -> BlobEntry {
        BlobEntry {
            record_ref: Some(RecordRef {
                segment_id,
                offset,
                len: 1,
            }),
            lsn: 1,
            generation: 1,
            state: BlobState::Live,
            lifecycle: BlobLifecycle::new(42),
        }
    }

    fn version_key(key: &BlobKey, lsn: strata_core::StrataLsn) -> BlobVersionKey {
        BlobVersionKey {
            key: key.clone(),
            lsn,
        }
    }

    fn segment_state(segment_id: SegmentId) -> SegmentState {
        SegmentState {
            segment_id,
            volume_id: 0,
            path: format!("{segment_id:06}.data"),
            placement_class: PlacementClass::Ingest,
            state: SegmentFileState::Open,
            write_offset: 128,
            durable_offset: 64,
            min_lsn: Some(1),
            max_lsn: Some(3),
            sealed_len: None,
            sealed_sha256: None,
        }
    }

    #[tokio::test]
    async fn open_path_persists_blob_entry_across_reopen() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let entry = blob_entry(7, 128);

        {
            let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
            index.put_blob_entry(&key, &entry).unwrap();
        }

        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
    }

    #[tokio::test]
    async fn from_db_creates_missing_cfs() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let db = open_cf(
            dir.path(),
            None,
            unique_metric_conf("strata_index_test"),
            &["existing"],
        )
        .unwrap();

        let index = StrataIndex::from_db(db, "embedded").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let entry = blob_entry(1, 0);
        index.put_blob_entry(&key, &entry).unwrap();

        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
    }

    #[tokio::test]
    async fn batch_writes_across_index_cfs_atomically() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let entry = blob_entry(9, 256);
        let state = segment_state(9);
        let stats = SegmentStats {
            total_bytes: 1024,
            live_bytes: 256,
            live_ref_count: 1,
            ..Default::default()
        };

        let mut batch = index.batch();
        index
            .put_blob_entry_batch(&mut batch, &key, &entry)
            .unwrap();
        index.put_segment_state_batch(&mut batch, &state).unwrap();
        index
            .put_segment_stats_batch(&mut batch, state.segment_id, &stats)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, entry.lsn, &key)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
        assert_eq!(
            index.get_segment_state(state.segment_id).unwrap(),
            Some(state)
        );
        assert_eq!(index.get_segment_stats(9).unwrap(), Some(stats));
        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![(entry.lsn, key.clone())]
        );
    }

    #[tokio::test]
    async fn latest_blob_version_returns_highest_lsn_for_key() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let mut first = blob_entry(7, 128);
        first.lsn = 1;
        let mut second = blob_entry(7, 256);
        second.lsn = 2;
        index.put_blob_entry(&key, &first).unwrap();
        index.put_blob_entry(&key, &second).unwrap();

        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(second));
    }

    #[tokio::test]
    async fn removing_blob_versions_deletes_exact_rows_and_exposes_previous_version() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let mut first = blob_entry(7, 128);
        first.lsn = 1;
        let mut second = blob_entry(7, 256);
        second.lsn = 2;
        let mut third = blob_entry(7, 512);
        third.lsn = 3;
        index.put_blob_entry(&key, &first).unwrap();
        index.put_blob_entry(&key, &second).unwrap();
        index.put_blob_entry(&key, &third).unwrap();

        let mut batch = index.batch();
        index
            .remove_blob_versions_batch(&mut batch, &[(key.clone(), 2)])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_blob_version(&version_key(&key, 2)).unwrap(), None);
        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(third.clone()));

        let mut batch = index.batch();
        index
            .remove_blob_versions_batch(&mut batch, &[(key.clone(), 3)])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_blob_version(&version_key(&key, 3)).unwrap(), None);
        assert_eq!(index.get_blob_entry(&key).unwrap(), Some(first));
    }

    #[tokio::test]
    async fn iterates_segment_states() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let state_1 = segment_state(1);
        let state_2 = segment_state(2);

        index.put_segment_state(&state_2).unwrap();
        index.put_segment_state(&state_1).unwrap();

        let states = index.iter_segment_states().unwrap();
        assert_eq!(states, vec![(1, state_1), (2, state_2)]);
    }

    #[tokio::test]
    async fn store_state_fields_update_independently() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();

        assert_eq!(
            index.get_store_state().unwrap(),
            Some(StrataStoreState::default())
        );
        assert_eq!(index.get_current_epoch().unwrap(), None);

        let mut batch = index.batch();
        index.put_next_lsn_batch(&mut batch, 42).unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_next_lsn().unwrap(), 42);
        assert_eq!(index.get_durable_lsn().unwrap(), 0);
        assert_eq!(index.get_accounted_lsn().unwrap(), 0);

        let mut batch = index.batch();
        index.put_durable_lsn_batch(&mut batch, 41).unwrap();
        index.put_accounted_lsn_batch(&mut batch, 40).unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.get_store_state().unwrap(),
            Some(StrataStoreState {
                next_lsn: 42,
                durable_lsn: 41,
                accounted_lsn: 40,
            })
        );
    }

    #[tokio::test]
    async fn epoch_changes_track_genesis_and_lsn_ordered_updates() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();

        assert_eq!(index.latest_epoch_at_lsn(StrataLsn::MAX).unwrap(), None);

        let mut batch = index.batch();
        index.put_epoch_change_batch(&mut batch, 0, 42).unwrap();
        index.put_current_epoch_batch(&mut batch, 42).unwrap();
        index.put_epoch_change_batch(&mut batch, 5, 43).unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_current_epoch().unwrap(), Some(42));
        assert_eq!(index.latest_epoch_at_lsn(0).unwrap(), Some((0, 42)));
        assert_eq!(index.latest_epoch_at_lsn(4).unwrap(), Some((0, 42)));
        assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((5, 43)));
        assert_eq!(index.iter_epoch_changes_from(1).unwrap(), vec![(5, 43)]);

        let mut batch = index.batch();
        index.remove_epoch_changes_batch(&mut batch, &[5]).unwrap();
        batch.write().unwrap();

        assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((0, 42)));
    }

    #[tokio::test]
    async fn iterates_unaccounted_lsn_ops_in_order() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();

        let mut batch = index.batch();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 2, &key_2)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 3, &key_3)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 1, &key_1)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![(1, key_1.clone()), (2, key_2), (3, key_3.clone())]
        );

        let mut batch = index.batch();
        index
            .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![(1, key_1), (3, key_3)]
        );
    }

    #[tokio::test]
    async fn unaccounted_lsn_ops_are_retained_until_explicitly_removed() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let entry_1 = blob_entry(1, 0);
        let mut entry_2 = blob_entry(1, 1);
        entry_2.lsn = 2;
        entry_2.generation = 2;

        let mut batch = index.batch();
        index
            .put_blob_version_batch(&mut batch, &key_1, &entry_1)
            .unwrap();
        index
            .put_blob_version_batch(&mut batch, &key_2, &entry_2)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, entry_1.lsn, &key_1)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, entry_2.lsn, &key_2)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops_from(2).unwrap(),
            vec![(2, key_2.clone())]
        );

        let mut batch = index.batch();
        index
            .remove_blob_versions_batch(&mut batch, &[(key_2.clone(), 2)])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![(1, key_1.clone()), (2, key_2.clone())]
        );

        let mut batch = index.batch();
        index
            .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.iter_unaccounted_lsn_ops().unwrap(), vec![(1, key_1)]);
    }
}
