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
    #[serde(rename = "g")]
    pub current_generation: ShardGeneration,
    #[serde(rename = "s")]
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
    #[serde(rename = "i")]
    pub id: ShardId,
    #[serde(rename = "g")]
    pub generation: ShardGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardCleanupState {
    ReadyForGc,
    /// Shard-owned files and their GC metadata have been removed. The job remains as the durable
    /// generation tombstone used by later snapshot-driven blob-LSM compactions.
    ShardOwnedReclaimed,
}

/// Durable progress for asynchronously reclaiming one dropped shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCleanupJob {
    #[serde(rename = "sh")]
    pub shard: ShardKey,
    #[serde(rename = "dl")]
    pub drop_lsn: StrataLsn,
    #[serde(rename = "st")]
    pub state: ShardCleanupState,
}
