use serde::{Deserialize, Serialize};

use crate::SegmentId;

pub type Epoch = u64;
pub type Generation = u64;
pub type ShardGeneration = u64;
pub type ShardId = u32;
pub type StrataLsn = u64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardState {
    Active,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardInfo {
    pub current_generation: ShardGeneration,
    pub state: ShardState,
}

impl ShardInfo {
    pub fn active(current_generation: ShardGeneration) -> Self {
        Self {
            current_generation,
            state: ShardState::Active,
        }
    }

    pub fn key(self, id: ShardId) -> ShardKey {
        ShardKey {
            id,
            generation: self.current_generation,
        }
    }

    pub fn is_active(self) -> bool {
        self.state == ShardState::Active
    }

    pub fn is_dropped(self) -> bool {
        self.state == ShardState::Dropped
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    pub id: ShardId,
    pub generation: ShardGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardCleanupState {
    PendingAccounting,
    ReadyForGc,
}

/// Durable progress for asynchronously reclaiming one dropped shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCleanupJob {
    pub shard: ShardKey,
    pub drop_lsn: StrataLsn,
    pub state: ShardCleanupState,
}
