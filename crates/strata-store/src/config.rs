use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use strata_core::{Epoch, StrataLsn};
use strata_gc::GcPlannerConfig;

const INGEST_DIR: &str = "ingest";
const INDEX_DIR: &str = "index";
const ACCOUNTING_INDEX_DIR: &str = "accounting-index";

pub const DEFAULT_SEGMENT_READER_CACHE_CAPACITY: usize = 64;
pub const DEFAULT_ACCOUNTING_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD: usize = 1024;
pub const DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT: u32 = 64;
pub const DEFAULT_ACCOUNTING_SIDECAR_INTERVAL: Duration = Duration::from_secs(20 * 60);
pub const DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD: usize = 4096;
pub const DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD: usize = 8;
pub const DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD: u64 = 64 * 1024 * 1024;
pub const DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD: usize = 8;
pub const DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD: u64 = 256 * 1024 * 1024;
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_GC_WORKER_COUNT: usize = 1;
pub const DEFAULT_GC_INITIAL_WORKER_COUNT: usize = 1;
pub const DEFAULT_GC_TUNING_WINDOW_CYCLES: u64 = 8;
pub const DEFAULT_GC_SYNC_IMPACT_THRESHOLD: Duration = Duration::from_millis(250);
pub const DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN: Option<StrataLsn> = None;

/// Runtime configuration for one Strata store namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrataStoreConfig {
    pub root_dir: PathBuf,
    pub namespace: String,
    pub segment_max_bytes: u64,
    pub write_queue_capacity: usize,
    pub max_unsealed_segments: usize,
    pub segment_reader_cache_capacity: usize,
    pub recovery_policy: StrataRecoveryPolicy,
    pub sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy,
    pub accounting_interval: Duration,
    pub accounting_unaccounted_threshold: usize,
    pub accounting_sidecar_partition_count: u32,
    pub accounting_sidecar_interval: Duration,
    pub accounting_sidecar_ingest_record_threshold: usize,
    pub accounting_sidecar_delta_run_count_threshold: usize,
    pub accounting_sidecar_delta_run_bytes_threshold: u64,
    pub accounting_sidecar_major_patch_count_threshold: usize,
    pub accounting_sidecar_major_patch_bytes_threshold: u64,
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
    /// Policy knobs used by the background GC planner.
    pub gc_planner_config: GcPlannerConfig,
    /// Optional GC admission limit measured as `durable_lsn - accounted_lsn`.
    ///
    /// This is an efficiency gate, not a correctness barrier. If set, new GC planning/copy work is
    /// skipped while accounting is too far behind because the planner's liveness view is likely
    /// stale. GC publish still uses relocation forwarding and does not require accounting to catch
    /// all the way up to the durable LSN.
    pub gc_max_accounting_lag_lsn: Option<StrataLsn>,
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

    pub fn accounting_index_dir(&self) -> PathBuf {
        self.namespace_dir().join(ACCOUNTING_INDEX_DIR)
    }

    pub(crate) fn accounting_sidecar_partition_count(&self) -> NonZeroU32 {
        NonZeroU32::new(self.accounting_sidecar_partition_count)
            .expect("accounting sidecar partition count is validated before use")
    }

    pub fn index_cf_prefix(&self) -> String {
        format!("strata/{}", self.namespace)
    }
}
