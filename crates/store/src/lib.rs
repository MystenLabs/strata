//! High-level Strata blob store composed from segment files and the Strata index.
//!
//! This crate coordinates the storage protocol. Segment files hold bytes, the index holds
//! durable metadata, and the store is responsible for keeping both sides consistent across
//! writes, syncs, crashes, recovery, and sealing.
//!
//! Write path:
//!
//! ```text
//! StrataStore::put
//!   -> enqueue write command
//!   -> store writer assigns the next store-global LSN
//!   -> append segment bytes and a routed store-WAL entry
//!   -> pass (LSN, keyed mutation) to the unlogged LSM
//!   -> commit an atomic index batch:
//!        segment_states[(store, segment_id)].write_offset = end_of_record
//!        store_state[(store, NextLsn)] = lsn + 1
//!   -> return to caller only after the batch commits
//! ```
//!
//! Epoch path:
//!
//! ```text
//! open new namespace
//!   -> epoch_changes[0] = starting_epoch
//!   -> store_state[CurrentEpoch] = starting_epoch
//!
//! StrataStore::increment_epoch
//!   -> submit a one-op batch containing BatchOp::IncrementEpoch
//!   -> reserve one store-global LSN
//!   -> commit the epoch row with the rest of the batch:
//!   -> epoch_changes[(store, lsn)] = current_epoch + 1
//!   -> store_state[(store, CurrentEpoch)] = current_epoch + 1
//!   -> store_state[(store, NextLsn)] = lsn + 1
//! ```
//!
//! Sync path:
//!
//! ```text
//! StrataStore::sync
//!   -> store waits for rolled segment fsyncs, then fsyncs the active segment and its WAL
//!   -> advance segment_states[active].durable_offset
//!   -> publish store_state[PublishedLsn] and the store checkpoint together
//!   -> fsync RocksDB WAL
//! ```
//!
//! Startup path:
//!
//! ```text
//! open
//!   -> validate config and create ingest directory
//!   -> discard stale GC staging directories
//!   -> discard or reject orphan segment files with no index state
//!   -> recover unsealed segments and validate the exact store-WAL prefix
//!   -> verify sealed segment files according to SealedSegmentIntegrityPolicy
//!   -> load both LSM manifests, remove unpublished SSTs, and route the remaining store-WAL suffix
//!   -> choose active segment
//!   -> synchronously seal recovered rolled segments
//!   -> start memtable flush/compaction, garbage sweep, writer, and GC
//! ```
//!
//! Crash model:
//!
//! - Unsealed segments are scanned from offset 0. The store keeps the longest valid prefix that
//!   is compatible with the recovery policy.
//! - Orphan segment files without index state are ignored by point-in-time recovery by deleting
//!   the file before any active writer is opened.
//! - A complete committed store-WAL tail is promoted. If an incomplete tail is newer than
//!   `published_lsn`, its logical operations and segment bytes are rolled back together.
//! - Sealed segments are expected to be stable. On open, their files must exist and match
//!   indexed length; optional checksum verification recomputes the sealed SHA-256 digest.
//! - `published_lsn` means every logical operation up to that store-global LSN is recoverable after
//!   restart.
//!
//! Read path:
//!
//! ```text
//! get_blob
//!   -> merge the blob's LSM operands into its current state
//!   -> SegmentReader::read_record
//!   -> verify record key
//!   -> verify full-record checksum unless ReadOptions disables it
//!
//! stream_blob
//!   -> merge the blob's LSM operands into its current state
//!   -> read record header and key trailer
//!   -> validate requested payload range
//!   -> return a blocking file-range stream
//! ```
//!
//! Blob lifetime path:
//!
//! ```text
//! StrataStore::set_blob_lifetime
//!   -> append a metadata-only BlobMutation to the LSM
//!   -> do not read segment state
//!   -> do not update GC overlay summary/ranges
//! ```
//!
//! The Store-owned LSM merge operator materializes shard versions, lifetime changes, and
//! tombstones. GC moves live in the relocation LSM and are folded into main rows by the streaming
//! compaction join.
//!
//! Blob-LSM compaction emits terminal transitions into the global garbage log; the sweeper folds
//! them into per-segment summaries and local garbage logs. GC plans and copies from that state and
//! revalidates every copied record against the current blob LSM before publication.
//!
//! # Module map
//!
//! This file keeps the crate's data model — the `StrataStore` handle, the `WriteCoordinator`
//! state, `StoreHalt`, and the tuning constants — so private fields stay visible to every
//! submodule. Behavior lives in the modules:
//!
//! - [`store`]: runtime API of `StrataStore` (writes, shards, epochs, sync, shutdown)
//! - [`open`] / [`recovery`]: `StrataStore::open`, config validation, crash recovery
//! - [`batch`]: the write protocol between `StrataStore` and the writer thread
//! - [`writer`]: the foreground commit, rollover, and sync coordinator
//! - [`gc`]: GC worker admission, copy execution, publication, and output accounting
//! - [`maintenance`]: background workers (garbage-log sweeper, LSM flusher/compactor)
//! - [`read`]: point reads and blob streaming
//! - [`seal`]: segment sealing; [`wal`] / [`wal_format`]: the store WAL
//! - [`segment_state`] / [`fs_util`] / [`layout`]: shared segment-row and filesystem helpers
mod batch;
pub mod blob_lsm;
mod config;
mod error;
mod file_sync;
mod fs_util;
mod gc;
mod gc_rate_limiter;
mod layout;
mod maintenance;
mod metrics;
mod open;
mod partition;
mod read;
mod reader_cache;
mod recovery;
mod relocation;
mod seal;
mod segment_state;
mod shard_gc;
mod store;
mod wal;
mod wal_format;
mod writer;

use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, Mutex, Weak, atomic::AtomicU64, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(test)]
use core_types::{
    BlobKey, Epoch, PlacementClass, RecordRef, SegmentFileState, ShardCleanupState, ShardInfo,
    ShardState,
};
use core_types::{SegmentId, SegmentOwner, SegmentState, ShardKey, StrataLsn, WalPosition};
use file_sync::FileSyncSender;
#[cfg(test)]
use file_sync::file_sync_channel;
#[cfg(test)]
use lsm::ManifestEdit;
use lsm::{LiveSnapshots, Lsm};
use segment::{SegmentFactory, SegmentIdAllocator, SegmentWriter};
#[cfg(test)]
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};
use tokio::sync::watch;
use wal::Wal;
#[cfg(test)]
use wal::WalEntry;
#[cfg(test)]
use wal_format::StoreWalMutation;

pub use config::{
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MIN_IO_BYTES_PER_SEC, DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
    DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT, DEFAULT_LSM_COMPACTION_PATCH_BYTES,
    DEFAULT_LSM_MEMTABLE_MAX_AGE, DEFAULT_LSM_PARTITION_COUNT, DEFAULT_SEGMENT_MAX_BYTES,
    DEFAULT_SEGMENT_READER_CACHE_CAPACITY, DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
    SealedSegmentIntegrityPolicy, StrataRecoveryPolicy, StrataStoreConfig,
};
pub use error::{Error, Result};
/// The metadata index and its storage port.
///
/// Re-exported so an embedder can reach [`StrataStore::from_index`] and, if it wants Strata's
/// column families inside a RocksDB instance it already runs, implement
/// [`index::port::IndexDb`] over that instance without depending on this workspace's internals.
pub use index::{StrataIndex, port as index_port};

#[cfg(test)]
use gc::GcConcurrencyConfig;
use gc::{GcCommand, GcConcurrencyController, GcSourceClaims};
pub use gc::{
    GcPublishResult, GcPublishedOutputSegment, GcPublishedRecord, GcStagedCopiedRecord,
    GcStagedOutputSegment, PreparedGcCopy, PreparedGcPlan,
};
pub use gc_planner::{GcPlanner, GcPlannerConfig};
use gc_rate_limiter::GcIoLimiter;
#[cfg(test)]
use layout::{relative_segment_path, segment_path, segment_state_path};
pub use metrics::StrataStoreMetrics;
pub use read::{ReadOptions, StoreGetProfile};
use reader_cache::SegmentReaderCache;
pub use relocation::DEFAULT_RELOCATION_CACHE_ENTRIES;
use relocation::{RelocationCache, RelocationStore};

#[cfg(feature = "internal-profiling")]
pub use batch::StoreProfileSink;
use batch::{
    AddShardRequest, BatchOp, BatchWriteRequest, DropShardRequest, PendingRollover, PreparedBatch,
    PreparedBatchOp, ProfileRequest, SyncRequest, WriteCommand, profile_phase,
};
pub use batch::{BatchWriteResult, StoreSyncProfile, StoreWriteProfile, StrataBatch};
use fs_util::{prune_empty_retention_dirs, segment_garbage_log_path, sync_parent_dir};
#[cfg(test)]
use gc::output::{GcSkippedCopiedRecord, GcSkippedCopiedRecordKind, gc_output_bytes_by_source};
#[cfg(test)]
use maintenance::{compact_relocation_lsm, flush_relocation_lsm, garbage_log_dir};
#[cfg(test)]
use segment_state::active_segment_state;
use segment_state::{
    SegmentAllocationTracker, active_segment_state_from_path, publish_segment_allocation_delta,
    unsealed_ingest_segment_count, unsealed_ingest_segment_ids,
};

const FIRST_SEGMENT_ID: SegmentId = 1;
/// How long the writer naps while waiting for durability publication to drain rolled segments.
/// Short, because this sleep sits on the foreground put path during rollover backpressure.
const SEAL_BACKLOG_WAIT: Duration = Duration::from_millis(10);
const SYNC_AND_COMMIT_INTERVAL: Duration = Duration::from_secs(1);
const SYNC_AND_COMMIT_WAL_BYTES: u64 = 64 * 1024 * 1024;
const SYNC_AND_COMMIT_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;
const GARBAGE_LOG_HEAD: &str = "lsm-garbage";
const GARBAGE_LOG_SWEEP_CURSOR: &str = "lsm-garbage-sweep";
const GARBAGE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const LSM_MEMTABLE_MAX_AGE: Duration = Duration::from_secs(1);
const LSM_MEMTABLE_MAX_KEYS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();
const LSM_FILE_SYNC_WORKERS: usize = 2;
const LSM_COMPACTION_PATCH_COUNT: usize = 8;
const LSM_COMPACTION_PATCH_BYTES: u64 = 64 * 1024 * 1024;
const LSM_COMPACTION_TARGET_BYTES: u64 = 64 * 1024 * 1024;
/// The blob patch tier is folded into the base only once it reaches this fraction of the base, so
/// base bytes rewritten per ingested byte stay a constant instead of growing with the base.
const LSM_FULL_COMPACTION_BASE_DIVISOR: u64 = 10;
/// Consecutive blob patches form one size tier while the largest is at most this many times the
/// smallest, which keeps each byte's number of tier rewrites logarithmic in the tier size.
const LSM_PATCH_TIER_SIZE_RATIO: u64 = 4;
/// A tier merge needs at least this many comparable consecutive patches.
const LSM_PATCH_TIER_FANOUT: usize = 4;
/// Upper bound on the patches one tier merge reads.
const LSM_PATCH_TIER_MAX_INPUTS: usize = 16;
/// Once a partition carries this many patches, a tier merge accepts any run of two.
const LSM_PATCH_TIER_MAX_PATCHES: usize = 32;
/// Cold base tables one partition re-reads each time a newer epoch transition becomes applicable
/// without reading patches.
const LSM_SWEEP_TABLES_PER_EPOCH: usize = 1;
const LSM_GARBAGE_LOG_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const LSM_OBSOLETE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const LSM_BASE_FORMAT: &str = "store-base-v2";
const LSM_PATCH_FORMAT: &str = "store-patch-v2";
const BLOB_LSM_MANIFEST: &str = "blob";
const RELOCATION_LSM_BASE_FORMAT: &str = "relocation-base-v1";
const RELOCATION_LSM_PATCH_FORMAT: &str = "relocation-patch-v1";
const RELOCATION_LSM_MANIFEST: &str = "relocation";
/// Default logical shard used by the standalone convenience APIs.
pub(crate) const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};
/// Explicit owner used by mixed ingest segment metadata.
pub(crate) const INGEST_SEGMENT_OWNER: SegmentOwner = SegmentOwner::Store;

#[cfg(test)]
use open::{
    ensure_epoch_initialized, ensure_ingest_dir, load_blob_lsm_manifest,
    load_relocation_lsm_manifest, open_lsm, open_store_wal, store_wal_recovery_state,
};
#[cfg(test)]
use read::{ResolvedBlobVersion, resolve_blob_version};

/// Single-namespace Strata store.
#[derive(Debug)]
pub struct StrataStore {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    lsm: Weak<Lsm>,
    pub(crate) write_tx: Option<mpsc::SyncSender<WriteCommand>>,
    writer_handle: Option<JoinHandle<()>>,
    lsm_flush_tx: Option<mpsc::Sender<()>>,
    lsm_compact_tx: Option<mpsc::Sender<()>>,
    wal_reclaim_tx: Option<mpsc::SyncSender<()>>,
    lsm_flush_handle: Option<JoinHandle<()>>,
    lsm_compact_handle: Option<JoinHandle<()>>,
    wal_reclaim_handle: Option<JoinHandle<()>>,
    lsm_sync_handles: Vec<JoinHandle<()>>,
    garbage_sweep_tx: Option<mpsc::Sender<()>>,
    garbage_sweep_handle: Option<JoinHandle<()>>,
    pub(crate) gc_txs: Vec<mpsc::Sender<GcCommand>>,
    gc_handles: Vec<JoinHandle<()>>,
    pub(crate) gc_publish_cleanup_lock: Arc<Mutex<()>>,
    pub(crate) garbage_publish_lock: Arc<Mutex<()>>,
    /// Held shared by every blob-LSM compaction pass. Nothing in the store takes it exclusively;
    /// tests do, to hold the compactor still while they stage a scenario. parking_lot's lock so a
    /// waiting writer is admitted after the pass in progress rather than losing the wake-up race
    /// to the compactor's immediate re-acquire at the next partition boundary.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) compaction_pause_lock: Arc<parking_lot::RwLock<()>>,
    /// Relocations GC activated while a compaction pass was in flight; see
    /// `relocation::RelocationActivations`.
    pub(crate) relocation_activations: Arc<relocation::RelocationActivations>,
    pub(crate) durable_relocation_lsn: Arc<AtomicU64>,
    pub(crate) gc_claims: Arc<GcSourceClaims>,
    pub(crate) gc_concurrency: Arc<GcConcurrencyController>,
    pub(crate) gc_io_limiter: Arc<GcIoLimiter>,
    pub(crate) segment_ids: SegmentIdAllocator,
    pub(crate) reader_cache: Arc<SegmentReaderCache>,
    pub(crate) relocations: Arc<RelocationStore>,
    pub(crate) relocation_cache: Arc<RelocationCache>,
    live_snapshots: LiveSnapshots,
    pub(crate) store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
}

/// Latest crash-durable store LSN, or a terminal failure that prevents further publication.
///
/// Subscribers receive the current value immediately and are notified when it changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityProgress {
    /// Every operation through this LSN survives a crash.
    pub published_lsn: StrataLsn,
    /// Set when this store instance can no longer accept or durably publish new writes.
    pub halt_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StoreHalt {
    reason: Arc<Mutex<Option<String>>>,
    durability_tx: watch::Sender<DurabilityProgress>,
}

impl Default for StoreHalt {
    fn default() -> Self {
        Self::new(0)
    }
}

impl StoreHalt {
    fn new(published_lsn: StrataLsn) -> Self {
        let (durability_tx, _) = watch::channel(DurabilityProgress {
            published_lsn,
            halt_reason: None,
        });
        Self {
            reason: Arc::new(Mutex::new(None)),
            durability_tx,
        }
    }

    fn subscribe_durability_progress(&self) -> watch::Receiver<DurabilityProgress> {
        self.durability_tx.subscribe()
    }

    fn publish_lsn(&self, published_lsn: StrataLsn) {
        self.durability_tx.send_if_modified(|progress| {
            if published_lsn <= progress.published_lsn {
                return false;
            }
            progress.published_lsn = published_lsn;
            true
        });
    }

    fn halt(&self, reason: impl Into<String>) {
        let mut guard = self.reason.lock().expect("store halt lock poisoned");
        if guard.is_none() {
            let reason = reason.into();
            *guard = Some(reason.clone());
            self.durability_tx.send_modify(|progress| {
                progress.halt_reason = Some(reason);
            });
        }
    }

    fn error(&self) -> Option<Error> {
        self.reason
            .lock()
            .expect("store halt lock poisoned")
            .clone()
            .map(|reason| Error::StoreHalted { reason })
    }

    fn check(&self) -> Result<()> {
        match self.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Owner of the global sequence, payload segment, and store WAL.
///
/// There is exactly one routing decision here: every LSN is appended to `wal`, then keyed records
/// go to an LSM while epoch/shard records go to RocksDB. Neither LSM allocates LSNs or performs
/// durability I/O.
struct WriteCoordinator {
    config: StrataStoreConfig,
    index: StrataIndex,
    lsm: Arc<Lsm>,
    wal: Wal,
    segment: SegmentWriter,
    segment_factory: SegmentFactory,
    segment_sync_tx: FileSyncSender,
    pending_segment_syncs: Vec<SegmentSync>,
    internal_write_tx: mpsc::SyncSender<WriteCommand>,
    sync_done_tx: mpsc::Sender<Arc<SyncAndCommit>>,
    sync_done_rx: mpsc::Receiver<Arc<SyncAndCommit>>,
    sync_and_commit_in_flight: Option<StrataLsn>,
    pending_sync_requests: Vec<PendingSyncRequest>,
    active_segment_state: SegmentState,
    durable_offset: u64,
    active_allocation_records: u64,
    active_allocation_tracker: Arc<SegmentAllocationTracker>,
    pending_segment_bytes: u64,
    unsealed_segments: usize,
    oldest_uncommitted_at: Option<Instant>,
    last_committed_at: Instant,
    pending_rollovers: Vec<PendingRollover>,
    lsm_flush_tx: mpsc::Sender<()>,
    lsm_compact_tx: mpsc::Sender<()>,
    wal_reclaim_tx: mpsc::SyncSender<()>,
    write_rx: mpsc::Receiver<WriteCommand>,
    ingest_owner: SegmentOwner,
    gc_concurrency: Arc<GcConcurrencyController>,
    store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
}

#[derive(Clone, Debug)]
struct SegmentSync {
    segment_id: SegmentId,
    path: PathBuf,
    durable_offset: u64,
    // Some vs None: Some means the segment is rolled over whereas active segment has no sealed_before_lsn.
    // Other than that, this field has no special usage.
    sealed_before_lsn: Option<StrataLsn>,
    sealed_sha256: Arc<Mutex<Option<[u8; 32]>>>,
    allocation_records: u64,
    allocation_tracker: Arc<SegmentAllocationTracker>,
}

struct PendingSyncRequest {
    target_lsn: StrataLsn,
    /// A request received after the current snapshot was captured must wait for the next one,
    /// even when both snapshots have the same foreground LSN. GC may have published unsynced
    /// relocation metadata between them.
    needs_follow_up: bool,
    response_tx: mpsc::Sender<Result<()>>,
    profile_request: ProfileRequest<StoreSyncProfile>,
    profile: Option<StoreSyncProfile>,
    started: Instant,
}

#[derive(Clone, Copy, Debug)]
struct FileSyncProfile {
    segment_files: Duration,
    wal: Duration,
    total: Duration,
    completed_at: Instant,
}

#[derive(Debug, Default)]
struct FileSyncTimings {
    segment_files: Option<Duration>,
    wal_started_at: Option<Instant>,
}

#[derive(Debug)]
struct SyncAndCommit {
    target_lsn: StrataLsn,
    wal_position: WalPosition,
    checkpoint_segment_id: SegmentId,
    checkpoint_segment_offset: u64,
    segments: Vec<SegmentSync>,
    wal_bytes: u64,
    segment_bytes: u64,
    started: Instant,
    file_sync_started: Instant,
    file_sync_timings: Mutex<FileSyncTimings>,
    ///None: still syncing
    ///Some(Err(error)): a sync failed
    ///Some(Ok(profile)): all segment and WAL syncs completed with these timings
    file_sync_result: Mutex<Option<Result<FileSyncProfile>>>,
    sync_done_tx: mpsc::Sender<Arc<SyncAndCommit>>,
    wake_tx: mpsc::SyncSender<WriteCommand>,
}

#[cfg(test)]
mod tests;
