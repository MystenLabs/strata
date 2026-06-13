use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Epoch, ShardKey, StrataLsn};

pub type SegmentId = u64;
pub type VolumeId = u32;

/// Index key for a segment local to a shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentKey {
    pub shard: ShardKey,
    pub segment_id: SegmentId,
}

/// Physical placement class for a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementClass {
    Ingest,
    ExactEpoch(Epoch),
    Spillover,
}

/// Durable segment file lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentFileState {
    Open,
    Sealing,
    Sealed,
    SealFailed,
    Deleting,
    Deleted,
}

/// Durable metadata for one segment file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentState {
    pub shard: ShardKey,
    pub segment_id: SegmentId,
    pub volume_id: VolumeId,
    pub path: String,
    pub placement_class: PlacementClass,
    pub state: SegmentFileState,
    pub write_offset: u64,
    pub durable_offset: u64,
    pub min_lsn: Option<StrataLsn>,
    pub max_lsn: Option<StrataLsn>,
    pub sealed_len: Option<u64>,
    /// SHA-256 digest of the sealed bytes, present only after the segment is finalized.
    pub sealed_sha256: Option<[u8; 32]>,
}

/// Live refs and bytes in one segment that expire at one logical end epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EpochBucket {
    pub refs: u64,
    pub bytes: u64,
}

/// Cheap accounting used by GC planning.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentStats {
    pub total_bytes: u64,
    pub live_bytes: u64,
    pub tombstoned_bytes: u64,
    pub expired_bytes: u64,
    pub pinned_bytes: u64,
    pub live_ref_count: u64,
    pub unknown_lifetime_bytes: u64,
    pub unknown_lifetime_ref_count: u64,
    pub min_live_end_epoch: Option<Epoch>,
    pub max_live_end_epoch: Option<Epoch>,
    pub future_epoch_histogram: BTreeMap<Epoch, EpochBucket>,
    /// Extension counts of refs added live to this segment. Refs stay in their bucket after they
    /// expire: per-epoch extension counts are not tracked, so expiry sweeps cannot remove them.
    pub extension_count_histogram: BTreeMap<u32, u64>,
}

impl SegmentStats {
    pub fn garbage_bytes(&self) -> u64 {
        self.tombstoned_bytes.saturating_add(self.expired_bytes)
    }

    pub fn garbage_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.garbage_bytes() as f64 / self.total_bytes as f64
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live_ref_count == 0
    }
}
