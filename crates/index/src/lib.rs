//! RocksDB-backed Strata metadata indexes.
//!
//! This crate stores durable metadata in RocksDB through the [`port`] storage trait, so it
//! does not depend on any particular RocksDB wrapper crate.
//!
//! Column families:
//!
//! ```text
//! StrataIndex
//! +-----------------+-----------------------------------------------+
//! | segment_states  | SegmentId -> SegmentState                     |
//! | segment_publication_lsns | SegmentId -> first visible Store LSN |
//! | segment_gc_summaries | SegmentId -> SegmentGcSummary              |
//! | gc_reclaim_pending | (SegmentId, StrataLsn) -> output bytes      |
//! | gc_reclaim_strategies | (SegmentId, StrataLsn) -> strategy label  |
//! | shards          | ShardId -> ShardInfo                          |
//! | shard_cleanup_jobs | ShardKey -> ShardCleanupJob                 |
//! | store_state     | StoreStateKey -> u64                         |
//! | epoch_changes   | StrataLsn -> current Epoch                   |
//! | lsm_manifests     | String -> lsm::Manifest                 |
//! | garbage_log_positions | String -> lsm::GarbageLogPosition       |
//! | segment_garbage_log_positions | SegmentId -> committed local file offset   |
//! +-----------------+-----------------------------------------------+
//! ```
//!
//! `store_state` and `epoch_changes` are store-global because Strata has one LSN domain across all
//! logical shards. Blob state itself lives in `lsm`; this crate owns only store metadata.
//!
//! Namespacing is handled by prefixing column family names:
//!
//! ```text
//! strata/<namespace>/segment_states
//! strata/<namespace>/segment_states
//! ...
//! ```

mod cf;
mod cleanup;
mod error;
mod gc;
mod global;
mod indexed_batch;
mod manifest;
mod migration;
mod open;
mod options;
mod overlay_cache;
pub mod port;
mod publication;
mod segment;
mod shard;
mod storage;
mod sweeper;

use std::sync::Arc;

pub use cf::StrataIndexCfNames;
pub use error::{Error, Result};
pub use gc::GcReclaimAttribution;
pub use indexed_batch::IndexedBatch;

/// Column-family names and options for one index prefix, for tools that open the same RocksDB
/// read-only (for example as a secondary instance) and must register the same merge operators.
pub fn cf_options_for_prefix(cf_prefix: &str) -> Vec<(String, rocksdb::Options)> {
    options::cf_options(&cf::StrataIndexCfNames::new(cf_prefix))
}

use core_types::{
    Epoch, SegmentGcSummary, SegmentId, SegmentState, ShardCleanupJob, ShardId, ShardInfo,
    ShardKey, StoreStateKey, StrataLsn,
};
use lsm::{GarbageLogPosition, Manifest};
use port::{IndexDb, TypedMap};

/// Strata metadata index, backed by any [`IndexDb`].
#[derive(Clone, Debug)]
pub struct StrataIndex {
    /// Shared storage handle used for all Strata metadata column families.
    db: Arc<dyn IndexDb>,
    /// Fully-qualified column family names, including the caller's namespace prefix.
    cf_names: StrataIndexCfNames,
    /// Durable manifest for each segment: path, state, offsets, placement, LSN bounds, and digest.
    segment_states: TypedMap<SegmentId, SegmentState>,
    /// First Store LSN at which each physical segment became visible.
    ///
    /// Missing rows are legacy segments and conservatively mean LSN 0.
    segment_publication_lsns: TypedMap<SegmentId, StrataLsn>,
    /// Summary-only GC planning state published after segment-local garbage is synced.
    segment_gc_summaries: TypedMap<SegmentId, SegmentGcSummary>,
    /// GC output bytes attributed to each source until that source file is unlinked.
    gc_reclaim_pending: TypedMap<(SegmentId, StrataLsn), u64>,
    /// Originating GC strategy retained until the corresponding source file is unlinked.
    gc_reclaim_strategies: TypedMap<(SegmentId, StrataLsn), String>,
    /// Shard registry used to resolve the current internal generation for each logical shard.
    shards: TypedMap<ShardId, ShardInfo>,
    /// Resumable whole-generation cleanup work created by shard drop.
    shard_cleanup_jobs: TypedMap<ShardKey, ShardCleanupJob>,
    /// Store-global cursors.
    store_state: TypedMap<StoreStateKey, StrataLsn>,
    /// Store-global epoch timeline. LSN 0 is the genesis epoch for the namespace.
    epoch_changes: TypedMap<StrataLsn, Epoch>,
    /// Materialized file set for each LSM, updated through RocksDB merge operands.
    lsm_manifests: TypedMap<String, Manifest>,
    /// Serializes read-modify-write publication of the LSM manifests; see
    /// [`StrataIndex::lock_lsm_manifests`].
    manifest_publish_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// Last checksummed garbage-log frame committed by each named producer.
    garbage_log_positions: TypedMap<String, GarbageLogPosition>,
    /// Committed prefix of each segment-local `.glog` file.
    segment_garbage_log_positions: TypedMap<SegmentId, u64>,
    /// Folded overlays the sweeper may reuse instead of re-reading a segment's whole log.
    overlay_cache: std::sync::Arc<std::sync::Mutex<overlay_cache::OverlayCache>>,
}

#[cfg(test)]
mod tests;
