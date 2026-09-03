use std::{path::PathBuf, time::Duration};

use core_types::Epoch;
use gc_planner::GcPlannerConfig;

const INGEST_DIR: &str = "ingest";
const INDEX_DIR: &str = "index";
const RELOCATION_DIR: &str = "relocations";

pub const DEFAULT_SEGMENT_READER_CACHE_CAPACITY: usize = 64_000;
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_GC_WORKER_COUNT: usize = 1;
pub const DEFAULT_GC_INITIAL_WORKER_COUNT: usize = 1;
pub const DEFAULT_GC_TUNING_WINDOW_CYCLES: u64 = 8;
pub const DEFAULT_GC_SYNC_IMPACT_THRESHOLD: Duration = Duration::from_millis(250);
pub const DEFAULT_GC_IO_BYTES_PER_SEC: u64 = 32 * 1024 * 1024;
pub const DEFAULT_GC_MIN_IO_BYTES_PER_SEC: u64 = 4 * 1024 * 1024;
pub const DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_LSM_PARTITION_COUNT: u32 = 1;
pub const DEFAULT_LSM_COMPACTION_PATCH_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_LSM_MEMTABLE_MAX_AGE: Duration = Duration::from_secs(1);

/// Runtime configuration for one Strata store namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrataStoreConfig {
    pub root_dir: PathBuf,
    pub namespace: String,
    pub segment_max_bytes: u64,
    pub write_queue_capacity: usize,
    pub max_unsealed_segments: usize,
    pub segment_reader_cache_capacity: usize,
    /// Number of hash partitions shared by the main and relocation LSMs.
    ///
    /// This is an on-disk compatibility setting. Reopening an existing namespace with a
    /// different value is rejected rather than silently routing keys to different tables.
    pub lsm_partition_count: u32,
    /// Patch bytes per blob-LSM partition that trigger a full pass while the base is small, and
    /// the size under which a whole partition is folded on the periodic tick. A larger base goes
    /// full only once its patch tier reaches a fixed fraction of the base, so base bytes rewritten
    /// per ingested byte stay bounded as the base grows.
    pub lsm_compaction_patch_bytes: u64,
    /// Oldest an LSM memtable may grow before it is frozen and flushed into a patch SST. Every
    /// partition of both LSMs flushes on this cadence, so with many partitions a longer age keeps
    /// the patch count, and the fixed cost per compaction pass, in check; the writes-merged
    /// frontier that gates clock expiry lags by at most this long.
    pub lsm_memtable_max_age: Duration,
    pub recovery_policy: StrataRecoveryPolicy,
    pub sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy,
    /// Whether background GC workers are started.
    pub gc_workers_enabled: bool,
    /// Background GC cadence for one planning/copy/publish attempt.
    pub gc_interval: Duration,
    /// Maximum number of background GC workers that may plan/copy disjoint source segments
    /// concurrently.
    pub gc_worker_count: usize,
    /// Initial number of GC workers admitted by the runtime concurrency tuner.
    pub gc_initial_worker_count: usize,
    /// Completed admitted GC attempts per normal tuner decision window.
    pub gc_tuning_window_cycles: u64,
    /// Foreground sync latency above which GC concurrency starts being treated as suspicious.
    ///
    /// The tuner also compares against the best observed baseline, so this is a lower bound for
    /// pressure detection rather than the only signal.
    pub gc_sync_impact_threshold: Duration,
    /// Store-wide byte budget for background GC disk reads and writes.
    ///
    /// This budget is shared by all GC workers. It limits physical copy/scan/checksum I/O, while
    /// `gc_worker_count` only controls how many workers may be active.
    pub gc_io_bytes_per_sec: u64,
    /// Lower bound for the runtime GC I/O budget under foreground latency pressure.
    pub gc_min_io_bytes_per_sec: u64,
    /// Policy knobs used by the background GC planner.
    pub gc_planner_config: GcPlannerConfig,
    /// Maximum time one background shard-cleanup attempt waits for retention-segment GC readers.
    ///
    /// New overlapping claims are fenced immediately. A timeout leaves the persisted cleanup job
    /// in place so a later GC wakeup can retry without delaying the `drop_shard` caller.
    pub shard_drop_gc_drain_timeout: Duration,
    /// Initial epoch used only when creating a namespace without persisted epoch metadata.
    pub starting_epoch: Epoch,
    /// Evaluation mode: push every published GC relocation through the foreground writer as a
    /// conditional main-LSM mutation, in batches of this many records. `None` leaves healing to
    /// compaction.
    ///
    /// The relocation LSM stays the durable forwarding view and the source-deletion fence either
    /// way. Write-back only adds the per-record foreground metadata path that compaction-coupled
    /// designs require, so its cost can be measured against lazy healing.
    pub relocation_writeback_chunk: Option<usize>,
}

/// Policy used when recovering unsealed ingest segments after a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrataRecoveryPolicy {
    /// Recover the longest per-shard ordered prefix of unsealed segment data.
    PointInTime,
    /// Fail store open if unsealed segment files do not exactly match indexed offsets.
    AbsoluteConsistency,
}

/// Policy used when verifying sealed segment files during store open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedSegmentIntegrityPolicy {
    /// Verify each sealed segment file exists and has the indexed sealed length.
    MetadataOnly,
    /// Verify metadata and recompute the sealed segment SHA-256 digest.
    Checksum,
}

impl StrataStoreConfig {
    /// Creates a store configuration with production defaults for a namespace under `root_dir`.
    ///
    /// Callers must supply the directory and namespace because these determine where durable data
    /// lives; the remaining settings can be overridden on the returned configuration.
    pub fn new(root_dir: impl Into<PathBuf>, namespace: impl Into<String>) -> Self {
        Self {
            root_dir: root_dir.into(),
            namespace: namespace.into(),
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            write_queue_capacity: 1024,
            max_unsealed_segments: 8,
            segment_reader_cache_capacity: DEFAULT_SEGMENT_READER_CACHE_CAPACITY,
            lsm_partition_count: DEFAULT_LSM_PARTITION_COUNT,
            lsm_compaction_patch_bytes: DEFAULT_LSM_COMPACTION_PATCH_BYTES,
            lsm_memtable_max_age: DEFAULT_LSM_MEMTABLE_MAX_AGE,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            gc_workers_enabled: true,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config: GcPlannerConfig::default(),
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: 0,
            relocation_writeback_chunk: None,
        }
    }

    pub fn namespace_dir(&self) -> PathBuf {
        self.root_dir.join(&self.namespace)
    }

    pub fn ingest_dir(&self) -> PathBuf {
        self.namespace_dir().join(INGEST_DIR)
    }

    pub fn standalone_index_dir(&self) -> PathBuf {
        self.namespace_dir().join(INDEX_DIR)
    }

    pub fn relocation_dir(&self) -> PathBuf {
        self.namespace_dir().join(RELOCATION_DIR)
    }

    pub fn index_cf_prefix(&self) -> String {
        format!("strata/{}", self.namespace)
    }
}
