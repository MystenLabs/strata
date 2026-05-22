use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Epoch, StrataLsn};

pub type SegmentId = u64;
pub type VolumeId = u32;

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

/// Cheap accounting used by GC planning.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentStats {
    pub total_bytes: u64,
    pub live_bytes: u64,
    pub tombstoned_bytes: u64,
    pub expired_bytes: u64,
    pub pinned_bytes: u64,
    pub live_ref_count: u64,
    pub min_live_end_epoch: Option<Epoch>,
    pub max_live_end_epoch: Option<Epoch>,
    pub future_epoch_histogram: BTreeMap<Epoch, u64>,
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
