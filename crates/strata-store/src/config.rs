use std::{path::PathBuf, time::Duration};

use strata_core::Epoch;
use strata_gc::GcPlannerConfig;

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
pub const DEFAULT_SEAL_WORKER_COUNT: usize = 1;
pub const DEFAULT_LSM_PARTITION_COUNT: u32 = 1;

/// Runtime configuration for one Strata store namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrataStoreConfig {
    pub root_dir: PathBuf,
    pub namespace: String,
    pub segment_max_bytes: u64,
    pub write_queue_capacity: usize,
    pub max_unsealed_segments: usize,
    pub seal_worker_count: usize,
    pub segment_reader_cache_capacity: usize,
    /// Number of hash partitions shared by the main and relocation LSMs.
    ///
    /// This is an on-disk compatibility setting. Reopening an existing namespace with a
    /// different value is rejected rather than silently routing keys to different tables.
    pub lsm_partition_count: u32,
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
