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
//! | segment_states  | SegmentId -> SegmentState                     |
//! | segment_ref_events | SegmentRefEventKey -> SegmentRefEvent       |
//! | segment_gc_overlay | SegmentId -> SegmentGcOverlay + summary     |
//! | gc_relocations | RecordRef -> GcRelocation                      |
//! | shards          | ShardId -> ShardInfo                          |
//! | store_state     | StoreStateKey -> u64                         |
//! | epoch_changes   | StrataLsn -> current Epoch                   |
//! | unaccounted_lsn_ops | StrataLsn -> BlobKey                        |
//! | accounting_index  | AccountingIndexKey -> AccountingIndexValue     |
//! +-----------------+-----------------------------------------------+
//! ```
//!
//! The `blob_versions` table is merge-only from the store's point of view: puts and snapshots
//! append shard-local payload ops to a packed value keyed by blob. The same packed value also stores
//! blob-level lifetime and tombstone metadata in the same LSN order.
//! `store_state`, `epoch_changes`, and `unaccounted_lsn_ops` are store-global because Strata now
//! has one LSN domain across all logical shards. Blob-version heads are still isolated by logical
//! shard. `unaccounted_lsn_ops` is the LSN-to-blob-key index used by durability, recovery, and
//! later accounting. Rows are written with blob ops, retained after durability, removed on rollback
//! if lost, and will be removed by accounting once consumed.
//!
//! Namespacing is handled by prefixing column family names:
//!
//! ```text
//! strata/<namespace>/blob_versions
//! strata/<namespace>/segment_states
//! ...
//! ```

mod accounting;
mod blob;
mod cf;
mod error;
mod gc;
mod global;
mod open;
mod options;
mod segment;
mod shard;
mod storage;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
};

pub use accounting::{
    ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_CONSUMED_CURSOR_KEY,
    ACCOUNTING_INDEX_LOG_DURABLE_POSITION_KEY, AccountingIndexKey, AccountingIndexValue,
    AccountingRefEvent, AccountingSnapshot, AccountingSnapshotGuard,
};
pub use cf::StrataIndexCfNames;
pub use error::{Error, Result};

#[cfg(test)]
pub(crate) use open::metric_conf_with_suffix;

use strata_core::{
    BlobKey, BlobVersionState, Epoch, GcRelocation, RecordRef, SegmentGcOverlay, SegmentId,
    SegmentRefEvent, SegmentRefEventKey, SegmentState, ShardId, ShardInfo, ShardKey, StoreStateKey,
    StrataLsn,
};
#[cfg(test)]
use strata_core::{
    BlobLifecycleState, BlobVersionKey, PutEntry, PutHead, PutOp, PutState,
    SegmentGcLifetimeUpdate, SegmentGcOverlayMergeOp, SegmentGcRecordRange, StrataStoreState,
};
use typed_store::rocks::{DBMap, RocksDB};

pub(crate) const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};

/// Typed-store backed Strata metadata index.
#[derive(Clone, Debug)]
pub struct StrataIndex {
    /// Shared typed-store RocksDB handle used for all Strata metadata column families.
    db: Arc<RocksDB>,
    /// Fully-qualified column family names, including the caller's namespace prefix.
    cf_names: StrataIndexCfNames,
    /// Store-global compaction frontier observed by blob metadata merge operators.
    /// Currently loaded from the persisted accounted LSN.
    blob_compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    /// Cached shard registry used by blob-version merge and compaction cleanup.
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    /// In-memory accounting frontiers currently pinning segment ref events for GC.
    accounting_snapshot_pins: Arc<Mutex<accounting::AccountingSnapshotPins>>,
    /// Packed payload version and lifecycle state keyed by blob key.
    blob_versions: DBMap<BlobKey, BlobVersionState>,
    /// Durable manifest for each segment: path, state, offsets, placement, LSN bounds, and digest.
    segment_states: DBMap<SegmentId, SegmentState>,
    /// Precise ref changes used to reconcile records copied while accounting was running.
    segment_ref_events: DBMap<SegmentRefEventKey, SegmentRefEvent>,
    /// Stale-tolerant segment-local GC view: summary counters, expired/retired ranges, and hints.
    segment_gc_overlay: DBMap<SegmentId, SegmentGcOverlay>,
    /// Active GC publish forwarding rows used while accounting replays pre-publish events.
    gc_relocations: DBMap<RecordRef, GcRelocation>,
    /// Shard registry used to resolve the current internal generation for each logical shard.
    shards: DBMap<ShardId, ShardInfo>,
    /// Store-global cursors.
    store_state: DBMap<StoreStateKey, StrataLsn>,
    /// Store-global epoch timeline. LSN 0 is the genesis epoch for the namespace.
    epoch_changes: DBMap<StrataLsn, Epoch>,
    /// Blob-key operations keyed by store-global LSN, retained until accounting consumes them.
    /// Unaccounted lsn ops need the blob key to partition delta log operations.
    unaccounted_lsn_ops: DBMap<StrataLsn, BlobKey>,
    /// Accounting-index metadata committed atomically with GC state.
    accounting_index: DBMap<AccountingIndexKey, AccountingIndexValue>,
}

#[cfg(test)]
mod tests;
