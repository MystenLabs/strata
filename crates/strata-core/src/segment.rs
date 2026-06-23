use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlobLifecycle, Epoch, ShardKey, StrataLsn};

pub type SegmentId = u64;
pub type VolumeId = u32;

/// Index key for a segment local to a shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentKey {
    pub shard: ShardKey,
    pub segment_id: SegmentId,
}

/// Offset-keyed accounting state for one physical record in an ingest segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentRefKey {
    pub segment_id: SegmentId,
    pub offset: u64,
}

/// LSN-ordered accounting event for one physical record in an ingest segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentRefEventKey {
    pub segment_id: SegmentId,
    pub lsn: StrataLsn,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentRefStatus {
    Live,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRefState {
    pub status: SegmentRefStatus,
    pub lifecycle: Option<BlobLifecycle>,
    pub last_accounted_lsn: StrataLsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentRefEvent {
    Retired,
    LifecycleChanged { lifecycle: Option<BlobLifecycle> },
}

/// Segment-local byte range for one encoded record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentGcRecordRange {
    pub offset: u64,
    pub len: u64,
}

impl SegmentGcRecordRange {
    pub fn end_offset(self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }
}

impl From<crate::RecordRef> for SegmentGcRecordRange {
    fn from(record_ref: crate::RecordRef) -> Self {
        Self {
            offset: record_ref.offset,
            len: record_ref.len,
        }
    }
}

/// Known lifetime for a segment-local live or copy eligible record range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGcLifetimeRange {
    pub range: SegmentGcRecordRange,
    pub lifecycle: BlobLifecycle,
}

/// Lifetime update folded into a segment-local GC overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGcLifetimeUpdate {
    pub range: SegmentGcRecordRange,
    pub lifecycle: Option<BlobLifecycle>,
}

/// Stale tolerant segment local overlay used by GC copy planning.
///
/// `dead` ranges are definitely skippable. Ranges absent from `dead` are eligible to copy, not
/// necessarily proven live in the freshest blob-version view. `lifetimes` contains routing hints
/// for eligible ranges with known expiry; absent lifetime means spillover/unknown routing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentGcOverlay {
    pub dead: Vec<SegmentGcRecordRange>,
    pub lifetimes: Vec<SegmentGcLifetimeRange>,
}

/// Merge operand for `SegmentGcOverlay`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentGcOverlayMergeOp {
    /// Marks segment-local ranges as definitely not protecting live data. This is stronger than a
    /// lifetime hint and removes overlapping lifecycle overlay state when folded.
    RetireBatch { ranges: Vec<SegmentGcRecordRange> },
    /// Installs or clears lifecycle routing for ranges that remain copy-eligible. A `None`
    /// lifecycle is an explicit clear, not the absence of an update.
    LifetimeBatch {
        updates: Vec<SegmentGcLifetimeUpdate>,
    },
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
