use serde::{Deserialize, Serialize};

use crate::SegmentId;

pub type Epoch = u64;
pub type Generation = u64;
pub type StrataLsn = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLifecycle {
    pub logical_end_epoch: Epoch,
    pub extension_count: u32,
}

impl BlobLifecycle {
    pub fn new(logical_end_epoch: Epoch) -> Self {
        Self {
            logical_end_epoch,
            extension_count: 0,
        }
    }

    pub fn extend_to(&mut self, new_end_epoch: Epoch) {
        if new_end_epoch > self.logical_end_epoch {
            self.logical_end_epoch = new_end_epoch;
        }
        self.extension_count = self.extension_count.saturating_add(1);
    }
}

/// Physical record location in a segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordRef {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub len: u64,
}

impl RecordRef {
    pub fn end_offset(self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobState {
    Live,
    Tombstoned,
}

/// Metadata entry stored in the Strata blob version index.
///
/// A live entry with a record reference is a payload-bearing put or snapshot. A live entry without
/// a record reference is an extension delta: it carries the current logical end epoch, while the
/// read path walks backward to find the latest payload-bearing version. A tombstoned entry ends the
/// live chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobEntry {
    pub record_ref: Option<RecordRef>,
    pub lsn: StrataLsn,
    pub generation: Generation,
    pub state: BlobState,
    pub lifecycle: BlobLifecycle,
}

/// Versioned blob-index key. Versions are append-only and ordered by logical LSN.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobVersionKey {
    pub key: crate::BlobKey,
    pub lsn: StrataLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrataStoreState {
    pub next_lsn: StrataLsn,
    pub durable_lsn: StrataLsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StoreStateKey {
    NextLsn,
    DurableLsn,
    CurrentEpoch,
}

impl Default for StrataStoreState {
    fn default() -> Self {
        Self {
            next_lsn: 1,
            durable_lsn: 0,
        }
    }
}

impl BlobEntry {
    pub fn is_live(&self) -> bool {
        self.state == BlobState::Live
    }

    pub fn is_tombstone(&self) -> bool {
        self.state == BlobState::Tombstoned
    }

    pub fn is_extension_delta(&self) -> bool {
        self.state == BlobState::Live && self.record_ref.is_none()
    }

    pub fn has_payload_ref(&self) -> bool {
        self.state == BlobState::Live && self.record_ref.is_some()
    }
}
