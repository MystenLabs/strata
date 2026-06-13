//! RocksDB-backed Strata metadata indexes using Walrus typed-store.
//!
//! This crate stores durable metadata in RocksDB using Walrus typed-store.
//!
//! Column families:
//!
//! ```text
//! StrataIndex
//! +-----------------+-----------------------------------------------+
//! | blob_versions   | BlobKey -> packed BlobVersionState            |
//! | segment_states  | SegmentKey -> SegmentState                    |
//! | segment_stats   | SegmentKey -> SegmentStats                    |
//! | shards          | ShardId -> ShardInfo                          |
//! | store_state     | ShardStoreStateKey -> u64                    |
//! | epoch_changes   | ShardLsnKey -> current Epoch                 |
//! | unaccounted_lsn_ops | ShardLsnKey -> BlobKey                     |
//! +-----------------+-----------------------------------------------+
//! ```
//!
//! The `blob_versions` table is merge-only from the store's point of view: puts and snapshots
//! append shard-local payload ops to a packed value keyed by blob. The same packed value also stores
//! blob-level lifetime and tombstone metadata in the same LSN order.
//! `store_state`, `epoch_changes`, and `unaccounted_lsn_ops` are scoped by shard generation. A
//! store can use that namespace as one global LSN domain, while blob-version heads can still be
//! isolated by logical shard. `unaccounted_lsn_ops` is the LSN-to-blob-key index used by
//! durability, recovery, and later accounting. Rows are written with blob ops, retained after
//! durability, removed on rollback if lost, and will be removed by accounting once consumed.
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
    collections::{BTreeMap, BTreeSet},
    ffi::{CStr, CString},
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use rocksdb::{
    MergeOperands,
    compaction_filter::{CompactionFilter, Decision},
    compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
};
use serde::{Deserialize, Serialize};
use strata_core::{
    BlobEntry, BlobKey, BlobLifecycleHead, BlobLifecycleMergeOp, BlobLifecycleOp,
    BlobLifecycleState, BlobVersionKey, BlobVersionState, Epoch, SegmentId, SegmentKey,
    SegmentState, SegmentStats, ShardHead, ShardId, ShardInfo, ShardKey, ShardLsnKey, ShardState,
    ShardStoreStateKey, StoreStateKey, StrataLsn, StrataStoreState, VersionMergeOp, VersionOp,
    VersionState,
};
use typed_store::{
    Map, TypedStoreError,
    rocks::{
        DBBatch, DBMap, MetricConf, ReadWriteOptions, RocksDB, default_db_options, open_cf_opts,
    },
};

pub use error::{Error, Result};

const BLOB_VERSIONS_CF: &str = "blob_versions";
const SEGMENT_STATES_CF: &str = "segment_states";
const SEGMENT_STATS_CF: &str = "segment_stats";
const SHARDS_CF: &str = "shards";
const STORE_STATE_CF: &str = "store_state";
const EPOCH_CHANGES_CF: &str = "epoch_changes";
const UNACCOUNTED_LSN_OPS_CF: &str = "unaccounted_lsn_ops";
static NEXT_METRIC_ID: AtomicU64 = AtomicU64::new(0);
const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum BlobVersionMergeOp {
    Version(VersionMergeOp),
    Lifecycle(BlobLifecycleMergeOp),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum EncodedBlobVersionMergeOperand {
    Op(BlobVersionMergeOp),
    Ops(Vec<BlobVersionMergeOp>),
}

impl EncodedBlobVersionMergeOperand {
    fn into_ops(self) -> Vec<BlobVersionMergeOp> {
        match self {
            Self::Op(op) => vec![op],
            Self::Ops(ops) => ops,
        }
    }
}

/// Typed-store backed Strata metadata index.
#[derive(Clone, Debug)]
pub struct StrataIndex {
    /// Shared typed-store RocksDB handle used for all Strata metadata column families.
    db: Arc<RocksDB>,
    /// Fully-qualified column family names, including the caller's namespace prefix.
    cf_names: StrataIndexCfNames,
    /// Store-global safe compaction frontier observed by blob metadata merge operators.
    /// Equals the persisted accounted LSN; ops at or below it may be folded into heads.
    blob_compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    /// Cached shard registry used by blob-version merge and compaction cleanup.
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    /// Packed payload version and lifecycle state keyed by blob key.
    blob_versions: DBMap<BlobKey, BlobVersionState>,
    /// Durable manifest for each segment: path, state, offsets, placement, LSN bounds, and digest.
    segment_states: DBMap<SegmentKey, SegmentState>,
    /// Segment-level accounting used by cleanup planning without scanning payload files.
    segment_stats: DBMap<SegmentKey, SegmentStats>,
    /// Shard registry used to resolve the current internal generation for each logical shard.
    shards: DBMap<ShardId, ShardInfo>,
    /// Store cursors scoped by shard generation.
    store_state: DBMap<ShardStoreStateKey, StrataLsn>,
    /// Epoch timeline scoped by shard generation. LSN 0 is the genesis epoch for that shard.
    epoch_changes: DBMap<ShardLsnKey, Epoch>,
    /// Blob-key operations keyed by store-global LSN, retained until accounting consumes them.
    unaccounted_lsn_ops: DBMap<ShardLsnKey, BlobKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrataIndexCfNames {
    pub blob_versions: String,
    pub segment_states: String,
    pub segment_stats: String,
    pub shards: String,
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
            shards: with_prefix(SHARDS_CF),
            store_state: with_prefix(STORE_STATE_CF),
            epoch_changes: with_prefix(EPOCH_CHANGES_CF),
            unaccounted_lsn_ops: with_prefix(UNACCOUNTED_LSN_OPS_CF),
        }
    }

    fn as_strs(&self) -> [&str; 7] {
        [
            self.blob_versions.as_str(),
            self.segment_states.as_str(),
            self.segment_stats.as_str(),
            self.shards.as_str(),
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
            unique_metric_conf("strata_index"),
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
        let segment_stats = DBMap::reopen_with_class(
            &db,
            Some(&cf_names.segment_stats),
            Some(SEGMENT_STATS_CF),
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

        let index = Self {
            db,
            cf_names,
            blob_compact_safe_lsn: compact_safe_lsn,
            shard_infos: shard_infos_cache,
            blob_versions,
            segment_states,
            segment_stats,
            shards,
            store_state,
            epoch_changes,
            unaccounted_lsn_ops,
        };
        index.load_shard_infos()?;
        index.load_blob_compact_safe_lsn()?;
        Ok(index)
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

    pub fn blob_versions(&self) -> &DBMap<BlobKey, BlobVersionState> {
        &self.blob_versions
    }

    pub fn segment_states(&self) -> &DBMap<SegmentKey, SegmentState> {
        &self.segment_states
    }

    pub fn segment_stats(&self) -> &DBMap<SegmentKey, SegmentStats> {
        &self.segment_stats
    }

    pub fn shards(&self) -> &DBMap<ShardId, ShardInfo> {
        &self.shards
    }

    pub fn store_state(&self) -> &DBMap<ShardStoreStateKey, StrataLsn> {
        &self.store_state
    }

    pub fn epoch_changes(&self) -> &DBMap<ShardLsnKey, Epoch> {
        &self.epoch_changes
    }

    pub fn unaccounted_lsn_ops(&self) -> &DBMap<ShardLsnKey, BlobKey> {
        &self.unaccounted_lsn_ops
    }

    /// Monotonically raises the store-global compaction frontier. Merge operators may fold any
    /// blob op with `lsn <= frontier` into its head, so callers must only pass LSNs that are
    /// durably accounted — folding is irreversible while rollback only edits the tail.
    pub fn set_blob_compact_safe_lsn(&self, lsn: StrataLsn) {
        let mut frontier = self
            .blob_compact_safe_lsn
            .write()
            .expect("blob version compaction frontier lock poisoned");
        *frontier = (*frontier).max(lsn);
    }

    fn load_shard_infos(&self) -> Result<()> {
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

    pub fn set_cached_shard_info(&self, shard_id: ShardId, info: ShardInfo) {
        self.shard_infos
            .write()
            .expect("shard info cache lock poisoned")
            .insert(shard_id, info);
    }

    fn load_blob_compact_safe_lsn(&self) -> Result<()> {
        *self
            .blob_compact_safe_lsn
            .write()
            .expect("blob version compaction frontier lock poisoned") = self.get_accounted_lsn()?;
        Ok(())
    }

    pub fn get_blob_state(&self, key: &BlobKey) -> Result<Option<BlobVersionState>> {
        Ok(self.blob_versions.get(key)?)
    }

    pub fn get_blob_version_state(&self, key: &BlobKey) -> Result<Option<VersionState>> {
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| (!state.versions.is_empty()).then_some(state.versions)))
    }

    pub fn get_blob_lifecycle_state(&self, key: &BlobKey) -> Result<Option<BlobLifecycleState>> {
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| (!state.lifecycle.is_empty()).then_some(state.lifecycle)))
    }

    pub fn resolve_blob_lifecycle_at(
        &self,
        key: &BlobKey,
        max_lsn: StrataLsn,
    ) -> Result<BlobLifecycleHead> {
        Ok(self
            .get_blob_state(key)?
            .map_or_else(BlobLifecycleHead::default, |state| {
                state.lifecycle.resolve_at(max_lsn)
            }))
    }

    pub fn resolve_blob_head(&self, key: &BlobKey, shard: ShardKey) -> Result<Option<ShardHead>> {
        if self.shard_generation_is_cached_obsolete(shard) {
            return Ok(None);
        }
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| state.versions.resolve_head(shard)))
    }

    fn shard_generation_is_cached_obsolete(&self, shard: ShardKey) -> bool {
        let shard_infos = self
            .shard_infos
            .read()
            .expect("shard info cache lock poisoned");
        shard_generation_is_obsolete(shard, &shard_infos)
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

    fn put_blob_state_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        state: &BlobVersionState,
    ) -> Result<()> {
        if state.is_empty() {
            batch.delete_batch(&self.blob_versions, [key.clone()])?;
        } else {
            batch.insert_batch(&self.blob_versions, [(key, state)])?;
        }
        Ok(())
    }

    pub fn put_blob_version_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        self.merge_blob_version_batch(batch, key, STANDALONE_SHARD, entry)
    }

    pub fn merge_blob_version_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        shard: ShardKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        let op = VersionMergeOp::Append(VersionOp {
            shard,
            entry: entry.clone(),
        });
        self.apply_blob_version_merge_op_batch(batch, key, op)
    }

    pub fn apply_blob_version_merge_op_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        op: VersionMergeOp,
    ) -> Result<()> {
        let operand = encode_blob_version_merge_operand(EncodedBlobVersionMergeOperand::Op(
            BlobVersionMergeOp::Version(op),
        ))?;
        batch.partial_merge_batch(&self.blob_versions, [(key, operand)])?;
        Ok(())
    }

    pub fn apply_blob_lifecycle_merge_op_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        op: BlobLifecycleMergeOp,
    ) -> Result<()> {
        let operand = encode_blob_version_merge_operand(EncodedBlobVersionMergeOperand::Op(
            BlobVersionMergeOp::Lifecycle(op),
        ))?;
        batch.partial_merge_batch(&self.blob_versions, [(key, operand)])?;
        Ok(())
    }

    pub fn blob_version_ops_at_lsn(&self, key: &BlobKey, lsn: StrataLsn) -> Result<Vec<VersionOp>> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok(Vec::new());
        };

        Ok(state.versions.ops_at_lsn(lsn))
    }

    pub fn blob_ops_at_lsn(
        &self,
        key: &BlobKey,
        lsn: StrataLsn,
    ) -> Result<(Vec<VersionOp>, Vec<BlobLifecycleOp>)> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok((Vec::new(), Vec::new()));
        };

        Ok((
            state.versions.ops_at_lsn(lsn),
            state.lifecycle.ops_at_lsn(lsn),
        ))
    }

    pub fn blob_lifecycle_ops_at_lsn(
        &self,
        key: &BlobKey,
        lsn: StrataLsn,
    ) -> Result<Vec<BlobLifecycleOp>> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok(Vec::new());
        };

        Ok(state.lifecycle.ops_at_lsn(lsn))
    }

    pub fn latest_blob_version(
        &self,
        key: &BlobKey,
    ) -> Result<Option<(BlobVersionKey, BlobEntry)>> {
        let Some(state) = self.get_blob_version_state(key)? else {
            return Ok(None);
        };

        let mut latest = state
            .heads
            .get(&STANDALONE_SHARD)
            .map(|head| (head.head_lsn, head.entry.clone()));

        for op in state.tail.iter().filter(|op| op.shard == STANDALONE_SHARD) {
            if latest
                .as_ref()
                .is_none_or(|(latest_lsn, _)| op.lsn() > *latest_lsn)
            {
                latest = Some((op.lsn(), op.entry.clone()));
            }
        }

        Ok(latest.map(|(lsn, entry)| {
            (
                BlobVersionKey {
                    key: key.clone(),
                    lsn,
                },
                entry,
            )
        }))
    }

    pub fn get_blob_version(&self, key: &BlobVersionKey) -> Result<Option<BlobEntry>> {
        self.get_blob_version_for_shard(key, STANDALONE_SHARD)
    }

    pub fn get_blob_version_for_shard(
        &self,
        key: &BlobVersionKey,
        shard: ShardKey,
    ) -> Result<Option<BlobEntry>> {
        let Some(state) = self.get_blob_version_state(&key.key)? else {
            return Ok(None);
        };

        if let Some(op) = state
            .tail
            .iter()
            .rev()
            .find(|op| op.shard == shard && op.lsn() == key.lsn)
        {
            return Ok(Some(op.entry.clone()));
        }

        Ok(state
            .heads
            .get(&shard)
            .into_iter()
            .find(|head| head.head_lsn == key.lsn)
            .map(|head| head.entry.clone()))
    }

    pub fn remove_blob_versions_batch(
        &self,
        batch: &mut DBBatch,
        hidden_versions: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        self.remove_blob_versions_for_shard_batch(batch, STANDALONE_SHARD, hidden_versions)
    }

    pub fn remove_blob_versions_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        hidden_versions: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        let mut by_key = BTreeMap::<BlobKey, BTreeSet<StrataLsn>>::new();
        for (key, lsn) in hidden_versions {
            by_key.entry(key.clone()).or_default().insert(*lsn);
        }

        for (key, lsns) in by_key {
            let Some(mut state) = self.get_blob_state(&key)? else {
                continue;
            };
            state
                .versions
                .tail
                .retain(|op| op.shard != shard || !lsns.contains(&op.lsn()));
            state
                .versions
                .heads
                .retain(|candidate, head| *candidate != shard || !lsns.contains(&head.head_lsn));
            self.put_blob_state_batch(batch, &key, &state)?;
        }

        Ok(())
    }

    pub fn remove_blob_ops_at_lsns_batch(
        &self,
        batch: &mut DBBatch,
        hidden_ops: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        let mut by_key = BTreeMap::<BlobKey, BTreeSet<StrataLsn>>::new();
        for (key, lsn) in hidden_ops {
            by_key.entry(key.clone()).or_default().insert(*lsn);
        }

        for (key, lsns) in by_key {
            let Some(mut state) = self.get_blob_state(&key)? else {
                continue;
            };
            remove_version_lsns_from_state(&mut state.versions, &lsns);
            remove_lifecycle_lsns_from_state(&mut state.lifecycle, &lsns);
            self.put_blob_state_batch(batch, &key, &state)?;
        }

        Ok(())
    }

    pub fn iter_blob_versions(&self) -> Result<Vec<(BlobVersionKey, BlobEntry)>> {
        let mut versions = Vec::new();
        for result in self.blob_versions.safe_iter()? {
            let (key, state) = result?;
            for head in state.versions.heads.values() {
                versions.push((
                    BlobVersionKey {
                        key: key.clone(),
                        lsn: head.head_lsn,
                    },
                    head.entry.clone(),
                ));
            }
            for op in state.versions.tail {
                versions.push((
                    BlobVersionKey {
                        key: key.clone(),
                        lsn: op.lsn(),
                    },
                    op.entry.clone(),
                ));
            }
        }
        versions.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(versions)
    }

    pub fn get_segment_state(&self, segment_id: SegmentId) -> Result<Option<SegmentState>> {
        self.get_segment_state_for_shard(STANDALONE_SHARD, segment_id)
    }

    pub fn get_segment_state_for_shard(
        &self,
        shard: ShardKey,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentState>> {
        Ok(self.segment_states.get(&SegmentKey { shard, segment_id })?)
    }

    pub fn put_segment_state(&self, state: &SegmentState) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_state_batch(&mut batch, state)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_state_batch(&self, batch: &mut DBBatch, state: &SegmentState) -> Result<()> {
        batch
            .insert_batch(
                self.segment_states(),
                [(
                    &SegmentKey {
                        shard: state.shard,
                        segment_id: state.segment_id,
                    },
                    state,
                )],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_segment_stats(&self, segment_id: SegmentId) -> Result<Option<SegmentStats>> {
        self.get_segment_stats_for_shard(STANDALONE_SHARD, segment_id)
    }

    pub fn get_segment_stats_for_shard(
        &self,
        shard: ShardKey,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentStats>> {
        Ok(self.segment_stats.get(&SegmentKey { shard, segment_id })?)
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
        self.put_segment_stats_for_shard_batch(batch, STANDALONE_SHARD, segment_id, stats)
    }

    pub fn put_segment_stats_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        segment_id: SegmentId,
        stats: &SegmentStats,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.segment_stats(),
                [(&SegmentKey { shard, segment_id }, stats)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_segment_stats_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(SegmentId, SegmentStats)>> {
        self.segment_stats
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, stats)) if key.shard == shard => Some(Ok((key.segment_id, stats))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn get_shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.shards.get(&shard_id)?)
    }

    pub fn put_shard_info(&self, shard_id: ShardId, info: ShardInfo) -> Result<()> {
        let mut batch = self.batch();
        self.put_shard_info_batch(&mut batch, shard_id, info)?;
        batch.write()?;
        self.flush_wal(true)?;
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

        let unaccounted_lsn_op_keys = self
            .unaccounted_lsn_ops
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.shard == shard => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        batch.delete_batch(&self.unaccounted_lsn_ops, unaccounted_lsn_op_keys)?;

        let epoch_change_keys = self
            .epoch_changes
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.shard == shard => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        batch.delete_batch(&self.epoch_changes, epoch_change_keys)?;

        let store_state_keys = self
            .store_state
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.shard == shard => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        batch.delete_batch(&self.store_state, store_state_keys)?;

        Ok(())
    }

    pub fn get_next_lsn(&self) -> Result<StrataLsn> {
        self.get_next_lsn_for_shard(STANDALONE_SHARD)
    }

    pub fn get_next_lsn_for_shard(&self, shard: ShardKey) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&ShardStoreStateKey {
                shard,
                field: StoreStateKey::NextLsn,
            })?
            .unwrap_or_else(|| StrataStoreState::default().next_lsn))
    }

    pub fn get_durable_lsn(&self) -> Result<StrataLsn> {
        self.get_durable_lsn_for_shard(STANDALONE_SHARD)
    }

    pub fn get_durable_lsn_for_shard(&self, shard: ShardKey) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&ShardStoreStateKey {
                shard,
                field: StoreStateKey::DurableLsn,
            })?
            .unwrap_or_else(|| StrataStoreState::default().durable_lsn))
    }

    pub fn get_accounted_lsn(&self) -> Result<StrataLsn> {
        self.get_accounted_lsn_for_shard(STANDALONE_SHARD)
    }

    pub fn get_accounted_lsn_for_shard(&self, shard: ShardKey) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&ShardStoreStateKey {
                shard,
                field: StoreStateKey::AccountedLsn,
            })?
            .unwrap_or_else(|| StrataStoreState::default().accounted_lsn))
    }

    pub fn get_current_epoch(&self) -> Result<Option<Epoch>> {
        self.get_current_epoch_for_shard(STANDALONE_SHARD)
    }

    pub fn get_current_epoch_for_shard(&self, shard: ShardKey) -> Result<Option<Epoch>> {
        Ok(self.store_state.get(&ShardStoreStateKey {
            shard,
            field: StoreStateKey::CurrentEpoch,
        })?)
    }

    pub fn get_store_state(&self) -> Result<Option<StrataStoreState>> {
        self.get_store_state_for_shard(STANDALONE_SHARD)
    }

    pub fn get_store_state_for_shard(&self, shard: ShardKey) -> Result<Option<StrataStoreState>> {
        Ok(Some(StrataStoreState {
            next_lsn: self.get_next_lsn_for_shard(shard)?,
            durable_lsn: self.get_durable_lsn_for_shard(shard)?,
            accounted_lsn: self.get_accounted_lsn_for_shard(shard)?,
        }))
    }

    pub fn put_next_lsn_batch(&self, batch: &mut DBBatch, next_lsn: StrataLsn) -> Result<()> {
        self.put_next_lsn_for_shard_batch(batch, STANDALONE_SHARD, next_lsn)
    }

    pub fn put_next_lsn_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        next_lsn: StrataLsn,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(
                    &ShardStoreStateKey {
                        shard,
                        field: StoreStateKey::NextLsn,
                    },
                    &next_lsn,
                )],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_durable_lsn_batch(&self, batch: &mut DBBatch, durable_lsn: StrataLsn) -> Result<()> {
        self.put_durable_lsn_for_shard_batch(batch, STANDALONE_SHARD, durable_lsn)
    }

    pub fn put_durable_lsn_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        durable_lsn: StrataLsn,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(
                    &ShardStoreStateKey {
                        shard,
                        field: StoreStateKey::DurableLsn,
                    },
                    &durable_lsn,
                )],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_accounted_lsn_batch(
        &self,
        batch: &mut DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        self.put_accounted_lsn_for_shard_batch(batch, STANDALONE_SHARD, accounted_lsn)
    }

    pub fn put_accounted_lsn_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(
                    &ShardStoreStateKey {
                        shard,
                        field: StoreStateKey::AccountedLsn,
                    },
                    &accounted_lsn,
                )],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    /// Commits accounting results and only then publishes the new compaction frontier — the
    /// merge operator must never fold ops whose accounting could still be lost.
    pub fn commit_accounting_batch(
        &self,
        mut batch: DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        self.put_accounted_lsn_batch(&mut batch, accounted_lsn)?;
        batch.write()?;
        self.flush_wal(true)?;
        self.set_blob_compact_safe_lsn(accounted_lsn);
        Ok(())
    }

    pub fn put_current_epoch_batch(&self, batch: &mut DBBatch, epoch: Epoch) -> Result<()> {
        self.put_current_epoch_for_shard_batch(batch, STANDALONE_SHARD, epoch)
    }

    pub fn put_current_epoch_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        epoch: Epoch,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.store_state(),
                [(
                    &ShardStoreStateKey {
                        shard,
                        field: StoreStateKey::CurrentEpoch,
                    },
                    &epoch,
                )],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_store_state_batch(
        &self,
        batch: &mut DBBatch,
        state: &StrataStoreState,
    ) -> Result<()> {
        self.put_store_state_for_shard_batch(batch, STANDALONE_SHARD, state)
    }

    pub fn put_store_state_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        state: &StrataStoreState,
    ) -> Result<()> {
        self.put_next_lsn_for_shard_batch(batch, shard, state.next_lsn)?;
        self.put_durable_lsn_for_shard_batch(batch, shard, state.durable_lsn)?;
        self.put_accounted_lsn_for_shard_batch(batch, shard, state.accounted_lsn)
    }

    pub fn put_epoch_change_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        epoch: Epoch,
    ) -> Result<()> {
        self.put_epoch_change_for_shard_batch(batch, STANDALONE_SHARD, lsn, epoch)
    }

    pub fn put_epoch_change_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        lsn: StrataLsn,
        epoch: Epoch,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.epoch_changes(),
                [(&ShardLsnKey { shard, lsn }, &epoch)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_epoch_change(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        self.get_epoch_change_for_shard(STANDALONE_SHARD, lsn)
    }

    pub fn get_epoch_change_for_shard(
        &self,
        shard: ShardKey,
        lsn: StrataLsn,
    ) -> Result<Option<Epoch>> {
        Ok(self.epoch_changes.get(&ShardLsnKey { shard, lsn })?)
    }

    pub fn latest_epoch_at_lsn(&self, max_lsn: StrataLsn) -> Result<Option<(StrataLsn, Epoch)>> {
        self.latest_epoch_at_lsn_for_shard(STANDALONE_SHARD, max_lsn)
    }

    pub fn latest_epoch_at_lsn_for_shard(
        &self,
        shard: ShardKey,
        max_lsn: StrataLsn,
    ) -> Result<Option<(StrataLsn, Epoch)>> {
        let mut latest = None;
        for result in self.epoch_changes.safe_iter()? {
            let (key, epoch) = result?;
            let lsn = key.lsn;
            if key.shard != shard {
                continue;
            }
            if lsn <= max_lsn && latest.is_none_or(|(latest_lsn, _)| lsn > latest_lsn) {
                latest = Some((lsn, epoch));
            }
        }
        Ok(latest)
    }

    pub fn iter_epoch_changes_from(&self, min_lsn: StrataLsn) -> Result<Vec<(StrataLsn, Epoch)>> {
        self.iter_epoch_changes_for_shard_from(STANDALONE_SHARD, min_lsn)
    }

    pub fn iter_epoch_changes_for_shard_from(
        &self,
        shard: ShardKey,
        min_lsn: StrataLsn,
    ) -> Result<Vec<(StrataLsn, Epoch)>> {
        let mut changes = self
            .epoch_changes
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, epoch)) if key.shard == shard && key.lsn >= min_lsn => {
                    Some(Ok((key.lsn, epoch)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        changes.sort_by_key(|(lsn, _)| *lsn);
        Ok(changes)
    }

    pub fn remove_epoch_changes_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        self.remove_epoch_changes_for_shard_batch(batch, STANDALONE_SHARD, lsns)
    }

    pub fn remove_epoch_changes_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        for lsn in lsns {
            batch.delete_batch(&self.epoch_changes, [ShardLsnKey { shard, lsn: *lsn }])?;
        }
        Ok(())
    }

    pub fn put_unaccounted_lsn_op_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        shard: ShardKey,
        key: &BlobKey,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.unaccounted_lsn_ops(),
                [(&ShardLsnKey { shard, lsn }, key)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_blob_unaccounted_lsn_op_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        key: &BlobKey,
    ) -> Result<()> {
        self.put_unaccounted_lsn_op_batch(batch, lsn, STANDALONE_SHARD, key)
    }

    pub fn get_unaccounted_lsn_op(&self, lsn: StrataLsn) -> Result<Option<BlobKey>> {
        self.get_unaccounted_lsn_op_for_shard(STANDALONE_SHARD, lsn)
    }

    pub fn get_unaccounted_lsn_op_for_shard(
        &self,
        shard: ShardKey,
        lsn: StrataLsn,
    ) -> Result<Option<BlobKey>> {
        Ok(self.unaccounted_lsn_ops.get(&ShardLsnKey { shard, lsn })?)
    }

    pub fn iter_unaccounted_lsn_ops(&self) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.iter_unaccounted_lsn_ops_from(0)
    }

    pub fn iter_unaccounted_lsn_ops_from(
        &self,
        min_lsn: StrataLsn,
    ) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.iter_unaccounted_lsn_ops_for_shard_from(STANDALONE_SHARD, min_lsn)
    }

    pub fn iter_unaccounted_lsn_ops_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.iter_unaccounted_lsn_ops_for_shard_from(shard, 0)
    }

    pub fn iter_unaccounted_lsn_ops_for_shard_from(
        &self,
        shard: ShardKey,
        min_lsn: StrataLsn,
    ) -> Result<Vec<(StrataLsn, BlobKey)>> {
        let mut ops = self
            .unaccounted_lsn_ops
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, blob_key)) if key.shard == shard && key.lsn >= min_lsn => {
                    Some(Ok((key.lsn, blob_key)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        ops.sort_by_key(|(lsn, _)| *lsn);
        Ok(ops)
    }

    pub fn remove_unaccounted_lsn_ops_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        self.remove_unaccounted_lsn_ops_for_shard_batch(batch, STANDALONE_SHARD, lsns)
    }

    pub fn remove_unaccounted_lsn_ops_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        for lsn in lsns {
            batch.delete_batch(
                self.unaccounted_lsn_ops(),
                [ShardLsnKey { shard, lsn: *lsn }],
            )?;
        }
        Ok(())
    }

    pub fn iter_segment_states(&self) -> Result<Vec<(SegmentId, SegmentState)>> {
        self.iter_segment_states_for_shard(STANDALONE_SHARD)
    }

    pub fn iter_segment_states_by_key(&self) -> Result<Vec<(SegmentKey, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn iter_segment_states_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(SegmentId, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, state)) if key.shard == shard => Some(Ok((key.segment_id, state))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
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

fn cf_options(
    cf_names: &StrataIndexCfNames,
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
) -> Vec<(String, rocksdb::Options)> {
    let mut options = Vec::new();
    for cf in cf_names.as_strs() {
        let cf_options = if cf == cf_names.blob_versions {
            blob_versions_cf_options(Arc::clone(&compact_safe_lsn), Arc::clone(&shard_infos))
        } else {
            default_db_options().options
        };
        options.push((cf.to_owned(), cf_options));
    }
    options
}

fn blob_versions_cf_options(
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
) -> rocksdb::Options {
    let mut options = default_db_options().options;
    let merge_shard_infos = Arc::clone(&shard_infos);
    options.set_merge_operator(
        "strata-blob-versions-merge",
        move |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
            let compact_safe_lsn = *compact_safe_lsn
                .read()
                .expect("blob version compaction frontier lock poisoned");
            let shard_infos = merge_shard_infos
                .read()
                .expect("shard info cache lock poisoned")
                .clone();
            full_merge_blob_versions(existing_value, operands, compact_safe_lsn, &shard_infos)
        },
        move |_key: &[u8], _existing_value: Option<&[u8]>, operands: &MergeOperands| {
            partial_merge_blob_versions(operands)
        },
    );
    options.set_compaction_filter_factory(BlobVersionsCompactionFilterFactory {
        shard_infos,
        name: CString::new("strata-blob-versions-compaction-filter").unwrap(),
    });
    options
}

fn full_merge_blob_versions(
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
    compact_safe_lsn: StrataLsn,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> Option<Vec<u8>> {
    let mut state = match existing_value {
        Some(value) => bcs::from_bytes::<BlobVersionState>(value).ok()?,
        None => BlobVersionState::default(),
    };

    for operand in operands {
        let operand = decode_blob_version_merge_operand(operand).ok()?;
        for op in operand.into_ops() {
            apply_blob_version_merge_op(&mut state, op);
        }
    }

    state.versions.compact_through(compact_safe_lsn);
    state.lifecycle.compact_through(compact_safe_lsn);
    prune_obsolete_shard_versions(&mut state.versions, shard_infos);
    bcs::to_bytes(&state).ok()
}

fn apply_blob_version_merge_op(state: &mut BlobVersionState, op: BlobVersionMergeOp) {
    match op {
        BlobVersionMergeOp::Version(op) => state.versions.apply_merge_op(op),
        BlobVersionMergeOp::Lifecycle(op) => state.lifecycle.apply_merge_op(op),
    }
}

#[derive(Clone)]
struct BlobVersionsCompactionFilterFactory {
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    name: CString,
}

impl CompactionFilterFactory for BlobVersionsCompactionFilterFactory {
    type Filter = BlobVersionsCompactionFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        BlobVersionsCompactionFilter {
            shard_infos: Arc::clone(&self.shard_infos),
            scratch: Vec::new(),
            name: CString::new("strata-blob-versions-compaction-filter").unwrap(),
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

struct BlobVersionsCompactionFilter {
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    scratch: Vec<u8>,
    name: CString,
}

impl CompactionFilter for BlobVersionsCompactionFilter {
    fn filter<'a>(&'a mut self, _level: u32, _key: &[u8], value: &[u8]) -> Decision<'a> {
        let Ok(mut state) = bcs::from_bytes::<BlobVersionState>(value) else {
            return Decision::Keep;
        };
        let shard_infos = self
            .shard_infos
            .read()
            .expect("shard info cache lock poisoned")
            .clone();
        if !prune_obsolete_shard_versions(&mut state.versions, &shard_infos) {
            return Decision::Keep;
        }
        if state.is_empty() {
            return Decision::Remove;
        }
        match bcs::to_bytes(&state) {
            Ok(bytes) => {
                self.scratch = bytes;
                Decision::Change(self.scratch.as_slice())
            }
            Err(_) => Decision::Keep,
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

fn prune_obsolete_shard_versions(
    state: &mut VersionState,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> bool {
    let initial_heads = state.heads.len();
    let initial_tail = state.tail.len();
    state
        .heads
        .retain(|shard, _| !shard_generation_is_obsolete(*shard, shard_infos));
    state
        .tail
        .retain(|op| !shard_generation_is_obsolete(op.shard, shard_infos));
    state.heads.len() != initial_heads || state.tail.len() != initial_tail
}

fn shard_generation_is_obsolete(
    shard: ShardKey,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> bool {
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

fn remove_version_lsns_from_state(state: &mut VersionState, lsns: &BTreeSet<StrataLsn>) {
    state.tail.retain(|op| !lsns.contains(&op.lsn()));
    state.heads.retain(|_, head| !lsns.contains(&head.head_lsn));
}

fn remove_lifecycle_lsns_from_state(state: &mut BlobLifecycleState, lsns: &BTreeSet<StrataLsn>) {
    state.tail.retain(|op| !lsns.contains(&op.lsn()));
    if let Some(lifetime) = &state.head.lifetime
        && lsns.contains(&lifetime.lsn)
    {
        state.head.lifetime = None;
    }
    if state
        .head
        .tombstone_lsn
        .is_some_and(|lsn| lsns.contains(&lsn))
    {
        state.head.tombstone_lsn = None;
    }
}

fn partial_merge_blob_versions(operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut ops = Vec::new();
    for operand in operands {
        let operand = decode_blob_version_merge_operand(operand).ok()?;
        ops.extend(operand.into_ops());
    }
    bcs::to_bytes(&EncodedBlobVersionMergeOperand::Ops(ops)).ok()
}

fn encode_blob_version_merge_operand(operand: EncodedBlobVersionMergeOperand) -> Result<Vec<u8>> {
    bcs::to_bytes(&operand).map_err(|err| Error::Serialization(err.to_string()))
}

fn decode_blob_version_merge_operand(data: &[u8]) -> Result<EncodedBlobVersionMergeOperand> {
    bcs::from_bytes(data).map_err(|err| Error::Serialization(err.to_string()))
}

fn unique_metric_conf(base: &str) -> MetricConf {
    let metric_id = NEXT_METRIC_ID.fetch_add(1, Ordering::Relaxed);
    MetricConf::new(&format!("{base}_{metric_id}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use strata_core::{
        BlobLifecycleAction, BlobLifecycleMergeOp, BlobLifecycleOp, BlobState, PlacementClass,
        RecordRef, SegmentFileState, ShardInfo, ShardKey, ShardState,
    };
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
        }
    }

    fn version_key(key: &BlobKey, lsn: strata_core::StrataLsn) -> BlobVersionKey {
        BlobVersionKey {
            key: key.clone(),
            lsn,
        }
    }

    fn segment_state(segment_id: SegmentId) -> SegmentState {
        segment_state_for_shard(STANDALONE_SHARD, segment_id)
    }

    fn segment_state_for_shard(shard: ShardKey, segment_id: SegmentId) -> SegmentState {
        SegmentState {
            shard,
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

    fn unaccounted(_shard: ShardKey, key: &BlobKey) -> BlobKey {
        key.clone()
    }

    fn put_version_state(index: &StrataIndex, key: &BlobKey, state: &VersionState) {
        let blob_state = BlobVersionState {
            versions: state.clone(),
            lifecycle: BlobLifecycleState::default(),
        };
        let mut batch = index.batch();
        batch
            .insert_batch(index.blob_versions(), [(key, &blob_state)])
            .unwrap();
        batch.write().unwrap();
    }

    fn compact_blob_versions(index: &StrataIndex) {
        index.blob_versions.flush().unwrap();
        let cf = index.blob_versions.cf().unwrap();
        index.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
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
    async fn shard_info_persists_across_reopen() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let shard_id = 17;
        let info = ShardInfo {
            current_generation: 4,
            state: ShardState::Dropping,
        };

        {
            let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
            index.put_shard_info(shard_id, info).unwrap();
        }

        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        assert_eq!(index.get_shard_info(shard_id).unwrap(), Some(info));
        assert_eq!(index.iter_shards().unwrap(), vec![(shard_id, info)]);
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
        let shard_id = 11;
        let shard_info = ShardInfo::active(2);

        let mut batch = index.batch();
        index
            .put_blob_entry_batch(&mut batch, &key, &entry)
            .unwrap();
        index.put_segment_state_batch(&mut batch, &state).unwrap();
        index
            .put_segment_stats_batch(&mut batch, state.segment_id, &stats)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, entry.lsn, STANDALONE_SHARD, &key)
            .unwrap();
        index
            .put_shard_info_batch(&mut batch, shard_id, shard_info)
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
            vec![(entry.lsn, unaccounted(STANDALONE_SHARD, &key))]
        );
        assert_eq!(index.get_shard_info(shard_id).unwrap(), Some(shard_info));
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
    async fn blob_versions_are_stored_as_packed_state() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let mut first = blob_entry(7, 128);
        first.lsn = 1;
        let mut second = blob_entry(7, 256);
        second.lsn = 2;

        let mut batch = index.batch();
        index
            .put_blob_version_batch(&mut batch, &key, &first)
            .unwrap();
        index
            .put_blob_version_batch(&mut batch, &key, &second)
            .unwrap();
        batch.write().unwrap();

        let state = index.get_blob_version_state(&key).unwrap().unwrap();
        assert_eq!(state.heads.len(), 0);
        assert_eq!(state.tail.len(), 2);
        assert_eq!(
            index.latest_blob_version(&key).unwrap(),
            Some((version_key(&key, 2), second.clone()))
        );
        assert_eq!(
            index.iter_blob_versions().unwrap(),
            vec![
                (version_key(&key, 1), first),
                (version_key(&key, 2), second)
            ]
        );
    }

    #[tokio::test]
    async fn blob_versions_pack_payload_and_lifecycle_state_together() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let entry = blob_entry(7, 128);

        let mut batch = index.batch();
        index
            .put_blob_version_batch(&mut batch, &key, &entry)
            .unwrap();
        index
            .apply_blob_lifecycle_merge_op_batch(
                &mut batch,
                &key,
                BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                    lsn: 2,
                    action: BlobLifecycleAction::SetLifetime {
                        logical_end_epoch: 50,
                    },
                }),
            )
            .unwrap();
        batch.write().unwrap();

        let state = index.get_blob_state(&key).unwrap().unwrap();
        assert_eq!(state.versions.tail.len(), 1);
        assert_eq!(state.lifecycle.tail.len(), 1);
        assert_eq!(
            index
                .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
                .unwrap()
                .lifetime
                .unwrap()
                .lifecycle
                .logical_end_epoch,
            50
        );
        assert_eq!(
            index.blob_ops_at_lsn(&key, 2).unwrap(),
            (
                Vec::new(),
                vec![BlobLifecycleOp {
                    lsn: 2,
                    action: BlobLifecycleAction::SetLifetime {
                        logical_end_epoch: 50,
                    },
                }]
            )
        );
    }

    #[tokio::test]
    async fn accounting_commit_advances_global_blob_version_compaction_frontier() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let shard = ShardKey {
            id: 42,
            generation: 7,
        };
        let mut first = blob_entry(7, 128);
        first.lsn = 1;
        let mut second = blob_entry(7, 256);
        second.lsn = 2;

        let mut batch = index.batch();
        index
            .merge_blob_version_batch(&mut batch, &key, shard, &first)
            .unwrap();
        index
            .merge_blob_version_batch(&mut batch, &key, shard, &second)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, first.lsn, shard, &key)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, second.lsn, shard, &key)
            .unwrap();
        batch.write().unwrap();

        let mut batch = index.batch();
        index
            .remove_unaccounted_lsn_ops_for_shard_batch(&mut batch, shard, &[1, 2])
            .unwrap();
        index.commit_accounting_batch(batch, 2).unwrap();
        assert_eq!(index.get_accounted_lsn().unwrap(), 2);
        assert_eq!(
            index.iter_unaccounted_lsn_ops_for_shard(shard).unwrap(),
            Vec::new()
        );
        let state = index.get_blob_version_state(&key).unwrap().unwrap();
        assert_eq!(state.tail, Vec::new());
        let head = state.heads.get(&shard).unwrap();
        assert_eq!(head.head_lsn, 2);
        assert_eq!(head.payload_lsn, Some(2));
        assert_eq!(head.entry.record_ref, second.record_ref);

        assert_eq!(
            index.resolve_blob_head(&key, shard).unwrap().unwrap(),
            head.clone()
        );
    }

    #[tokio::test]
    async fn blob_versions_compaction_filter_removes_dropped_shard_generation() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let dropped_shard = ShardKey {
            id: 5,
            generation: 0,
        };
        let kept_shard = ShardKey {
            id: 6,
            generation: 0,
        };
        let mixed_key = BlobKey::new(b"mixed-blob".to_vec()).unwrap();
        let dropped_only_key = BlobKey::new(b"dropped-only-blob".to_vec()).unwrap();
        let mut dropped_entry = blob_entry(1, 128);
        dropped_entry.lsn = 1;
        let mut kept_entry = blob_entry(2, 256);
        kept_entry.lsn = 1;

        index
            .put_shard_info(dropped_shard.id, ShardInfo::active(0))
            .unwrap();
        index
            .put_shard_info(kept_shard.id, ShardInfo::active(0))
            .unwrap();

        let mut mixed_state = VersionState::default();
        mixed_state.append_op(VersionOp {
            shard: dropped_shard,
            entry: dropped_entry.clone(),
        });
        mixed_state.append_op(VersionOp {
            shard: kept_shard,
            entry: kept_entry.clone(),
        });
        put_version_state(&index, &mixed_key, &mixed_state);

        let mut dropped_only_state = VersionState::default();
        dropped_only_state.append_op(VersionOp {
            shard: dropped_shard,
            entry: dropped_entry,
        });
        put_version_state(&index, &dropped_only_key, &dropped_only_state);

        index
            .put_shard_info(
                dropped_shard.id,
                ShardInfo {
                    current_generation: dropped_shard.generation,
                    state: ShardState::Dropped,
                },
            )
            .unwrap();
        compact_blob_versions(&index);

        assert_eq!(
            index.resolve_blob_head(&mixed_key, dropped_shard).unwrap(),
            None
        );
        assert_eq!(
            index
                .resolve_blob_head(&mixed_key, kept_shard)
                .unwrap()
                .unwrap()
                .entry,
            kept_entry
        );
        assert!(
            index
                .get_blob_version_state(&dropped_only_key)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn blob_versions_compaction_filter_removes_stale_shard_generation() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let stale_shard = ShardKey {
            id: 5,
            generation: 0,
        };
        let current_shard = ShardKey {
            id: 5,
            generation: 1,
        };
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let mut stale_entry = blob_entry(1, 128);
        stale_entry.lsn = 7;
        let mut current_entry = blob_entry(2, 256);
        current_entry.lsn = 1;

        index
            .put_shard_info(
                current_shard.id,
                ShardInfo::active(current_shard.generation),
            )
            .unwrap();

        let mut state = VersionState::default();
        state.heads.insert(
            stale_shard,
            ShardHead {
                head_lsn: stale_entry.lsn,
                payload_lsn: Some(stale_entry.lsn),
                entry: stale_entry,
            },
        );
        state.append_op(VersionOp {
            shard: current_shard,
            entry: current_entry.clone(),
        });
        put_version_state(&index, &key, &state);
        compact_blob_versions(&index);

        assert_eq!(index.resolve_blob_head(&key, stale_shard).unwrap(), None);
        assert_eq!(
            index
                .resolve_blob_head(&key, current_shard)
                .unwrap()
                .unwrap()
                .entry,
            current_entry
        );
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
    async fn removing_blob_versions_is_scoped_to_shard() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let shard = ShardKey {
            id: 9,
            generation: 3,
        };
        let mut standalone = blob_entry(7, 128);
        standalone.lsn = 1;
        let mut shard_entry = blob_entry(8, 256);
        shard_entry.lsn = 1;
        shard_entry.generation = 2;

        let mut batch = index.batch();
        index
            .put_blob_version_batch(&mut batch, &key, &standalone)
            .unwrap();
        index
            .merge_blob_version_batch(&mut batch, &key, shard, &shard_entry)
            .unwrap();
        batch.write().unwrap();

        let mut batch = index.batch();
        index
            .remove_blob_versions_for_shard_batch(&mut batch, shard, &[(key.clone(), 1)])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.get_blob_version(&version_key(&key, 1)).unwrap(),
            Some(standalone)
        );
        assert_eq!(
            index
                .get_blob_version_for_shard(&version_key(&key, 1), shard)
                .unwrap(),
            None
        );
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
    async fn segment_state_and_stats_are_keyed_by_shard() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let shard = ShardKey {
            id: 5,
            generation: 2,
        };
        let standalone_state = segment_state(1);
        let shard_state = segment_state_for_shard(shard, 1);
        let standalone_stats = SegmentStats {
            total_bytes: 11,
            ..Default::default()
        };
        let shard_stats = SegmentStats {
            total_bytes: 22,
            ..Default::default()
        };

        let mut batch = index.batch();
        index
            .put_segment_state_batch(&mut batch, &standalone_state)
            .unwrap();
        index
            .put_segment_state_batch(&mut batch, &shard_state)
            .unwrap();
        index
            .put_segment_stats_batch(&mut batch, 1, &standalone_stats)
            .unwrap();
        index
            .put_segment_stats_for_shard_batch(&mut batch, shard, 1, &shard_stats)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_segment_state(1).unwrap(), Some(standalone_state));
        assert_eq!(
            index.get_segment_state_for_shard(shard, 1).unwrap(),
            Some(shard_state)
        );
        assert_eq!(index.get_segment_stats(1).unwrap(), Some(standalone_stats));
        assert_eq!(
            index.get_segment_stats_for_shard(shard, 1).unwrap(),
            Some(shard_stats)
        );
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
    async fn lsn_keyed_tables_are_shard_local() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let shard = ShardKey {
            id: 5,
            generation: 2,
        };
        let other_shard = ShardKey {
            id: 6,
            generation: 0,
        };
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

        // Each shard owns its own LSN sequence and epoch timeline. Different shards can use the
        // same numeric LSN without colliding because the shard is part of the key.
        let mut batch = index.batch();
        index
            .put_next_lsn_for_shard_batch(&mut batch, shard, 2)
            .unwrap();
        index
            .put_next_lsn_for_shard_batch(&mut batch, other_shard, 2)
            .unwrap();
        index
            .put_epoch_change_for_shard_batch(&mut batch, shard, 1, 42)
            .unwrap();
        index
            .put_epoch_change_for_shard_batch(&mut batch, other_shard, 1, 43)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 1, shard, &key_a)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 1, other_shard, &key_b)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(index.get_next_lsn_for_shard(shard).unwrap(), 2);
        assert_eq!(index.get_next_lsn_for_shard(other_shard).unwrap(), 2);
        assert_eq!(
            index.latest_epoch_at_lsn_for_shard(shard, 1).unwrap(),
            Some((1, 42))
        );
        assert_eq!(
            index.latest_epoch_at_lsn_for_shard(other_shard, 1).unwrap(),
            Some((1, 43))
        );
        assert_eq!(
            index.iter_unaccounted_lsn_ops_for_shard(shard).unwrap(),
            vec![(1, unaccounted(shard, &key_a))]
        );
        assert_eq!(
            index
                .iter_unaccounted_lsn_ops_for_shard(other_shard)
                .unwrap(),
            vec![(1, unaccounted(other_shard, &key_b))]
        );
        assert_eq!(
            index
                .get_unaccounted_lsn_op_for_shard(other_shard, 1)
                .unwrap(),
            Some(unaccounted(other_shard, &key_b))
        );
    }

    #[tokio::test]
    async fn removing_shard_keyed_metadata_skips_blob_versions() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        let shard = ShardKey {
            id: 5,
            generation: 2,
        };
        let other_shard = ShardKey {
            id: 6,
            generation: 1,
        };
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let other_key = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let mut entry = blob_entry(1, 128);
        entry.lsn = 1;
        let shard_state = segment_state_for_shard(shard, 1);
        let other_state = segment_state_for_shard(other_shard, 1);
        let shard_stats = SegmentStats {
            total_bytes: 11,
            ..Default::default()
        };
        let other_stats = SegmentStats {
            total_bytes: 22,
            ..Default::default()
        };

        let mut batch = index.batch();
        index
            .merge_blob_version_batch(&mut batch, &key, shard, &entry)
            .unwrap();
        index
            .put_segment_state_batch(&mut batch, &shard_state)
            .unwrap();
        index
            .put_segment_state_batch(&mut batch, &other_state)
            .unwrap();
        index
            .put_segment_stats_for_shard_batch(&mut batch, shard, 1, &shard_stats)
            .unwrap();
        index
            .put_segment_stats_for_shard_batch(&mut batch, other_shard, 1, &other_stats)
            .unwrap();
        index
            .put_next_lsn_for_shard_batch(&mut batch, shard, 3)
            .unwrap();
        index
            .put_durable_lsn_for_shard_batch(&mut batch, shard, 2)
            .unwrap();
        index
            .put_accounted_lsn_for_shard_batch(&mut batch, shard, 1)
            .unwrap();
        index
            .put_current_epoch_for_shard_batch(&mut batch, shard, 7)
            .unwrap();
        index
            .put_epoch_change_for_shard_batch(&mut batch, shard, 1, 7)
            .unwrap();
        index
            .put_next_lsn_for_shard_batch(&mut batch, other_shard, 5)
            .unwrap();
        index
            .put_durable_lsn_for_shard_batch(&mut batch, other_shard, 4)
            .unwrap();
        index
            .put_accounted_lsn_for_shard_batch(&mut batch, other_shard, 3)
            .unwrap();
        index
            .put_current_epoch_for_shard_batch(&mut batch, other_shard, 8)
            .unwrap();
        index
            .put_epoch_change_for_shard_batch(&mut batch, other_shard, 2, 8)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 1, shard, &key)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 2, other_shard, &other_key)
            .unwrap();
        batch.write().unwrap();

        let mut batch = index.batch();
        index
            .remove_shard_keyed_metadata_batch(&mut batch, shard)
            .unwrap();
        batch.write().unwrap();

        assert!(
            index
                .get_segment_state_for_shard(shard, 1)
                .unwrap()
                .is_none()
        );
        assert!(
            index
                .get_segment_stats_for_shard(shard, 1)
                .unwrap()
                .is_none()
        );
        assert_eq!(index.get_next_lsn_for_shard(shard).unwrap(), 1);
        assert_eq!(index.get_durable_lsn_for_shard(shard).unwrap(), 0);
        assert_eq!(index.get_accounted_lsn_for_shard(shard).unwrap(), 0);
        assert_eq!(index.get_current_epoch_for_shard(shard).unwrap(), None);
        assert_eq!(
            index.iter_epoch_changes_for_shard_from(shard, 0).unwrap(),
            Vec::new()
        );
        assert_eq!(index.get_next_lsn_for_shard(other_shard).unwrap(), 5);
        assert_eq!(index.get_durable_lsn_for_shard(other_shard).unwrap(), 4);
        assert_eq!(index.get_accounted_lsn_for_shard(other_shard).unwrap(), 3);
        assert_eq!(
            index.get_current_epoch_for_shard(other_shard).unwrap(),
            Some(8)
        );
        assert_eq!(
            index
                .iter_epoch_changes_for_shard_from(other_shard, 0)
                .unwrap(),
            vec![(2, 8)]
        );
        assert_eq!(
            index.iter_unaccounted_lsn_ops_for_shard(shard).unwrap(),
            Vec::new()
        );
        assert_eq!(
            index
                .iter_unaccounted_lsn_ops_for_shard(other_shard)
                .unwrap(),
            vec![(2, unaccounted(other_shard, &other_key))]
        );
        assert!(index.resolve_blob_head(&key, shard).unwrap().is_some());
        assert_eq!(
            index.get_segment_state_for_shard(other_shard, 1).unwrap(),
            Some(other_state)
        );
        assert_eq!(
            index.get_segment_stats_for_shard(other_shard, 1).unwrap(),
            Some(other_stats)
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
            .put_unaccounted_lsn_op_batch(&mut batch, 2, STANDALONE_SHARD, &key_2)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 3, STANDALONE_SHARD, &key_3)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, 1, STANDALONE_SHARD, &key_1)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![
                (1, unaccounted(STANDALONE_SHARD, &key_1)),
                (2, unaccounted(STANDALONE_SHARD, &key_2)),
                (3, unaccounted(STANDALONE_SHARD, &key_3))
            ]
        );

        let mut batch = index.batch();
        index
            .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![
                (1, unaccounted(STANDALONE_SHARD, &key_1)),
                (3, unaccounted(STANDALONE_SHARD, &key_3))
            ]
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
            .put_unaccounted_lsn_op_batch(&mut batch, entry_1.lsn, STANDALONE_SHARD, &key_1)
            .unwrap();
        index
            .put_unaccounted_lsn_op_batch(&mut batch, entry_2.lsn, STANDALONE_SHARD, &key_2)
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops_from(2).unwrap(),
            vec![(2, unaccounted(STANDALONE_SHARD, &key_2))]
        );

        let mut batch = index.batch();
        index
            .remove_blob_versions_batch(&mut batch, &[(key_2.clone(), 2)])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![
                (1, unaccounted(STANDALONE_SHARD, &key_1)),
                (2, unaccounted(STANDALONE_SHARD, &key_2))
            ]
        );

        let mut batch = index.batch();
        index
            .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
            .unwrap();
        batch.write().unwrap();

        assert_eq!(
            index.iter_unaccounted_lsn_ops().unwrap(),
            vec![(1, unaccounted(STANDALONE_SHARD, &key_1))]
        );
    }
}
