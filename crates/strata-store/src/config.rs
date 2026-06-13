use std::{path::PathBuf, time::Duration};

use strata_core::Epoch;

const INGEST_DIR: &str = "ingest";
const INDEX_DIR: &str = "index";

pub const DEFAULT_SEGMENT_READER_CACHE_CAPACITY: usize = 64;
pub const DEFAULT_ACCOUNTING_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD: usize = 1024;

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

    pub fn index_cf_prefix(&self) -> String {
        format!("strata/{}", self.namespace)
    }
}
