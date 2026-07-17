use serde::{Deserialize, Serialize};
use strata_core::{BlobKey, BlobLifecycle, Epoch, RecordRef, ShardKey, StrataLsn};

use super::{BlobUpdate, MaterializedBlobState};

/// Keyed state row stored inside base runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StateRecord {
    pub(crate) key: BlobKey,
    pub(crate) state: MaterializedBlobState,
}

/// Keyed update summary stored inside patch runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PatchRecord {
    pub(crate) key: BlobKey,
    pub(crate) updates: Vec<PatchUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PatchUpdate {
    Put {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        current_epoch: Epoch,
        lifecycle: Option<BlobLifecycle>,
    },
    Tombstone {
        lsn: StrataLsn,
        key: BlobKey,
    },
    SetLifetime {
        lsn: StrataLsn,
        key: BlobKey,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    MapRef {
        lsn: StrataLsn,
        key: BlobKey,
        from: RecordRef,
        to: RecordRef,
    },
}

impl PatchUpdate {
    pub(crate) fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::SetLifetime { lsn, .. }
            | Self::MapRef { lsn, .. } => *lsn,
        }
    }
}

impl From<BlobUpdate> for PatchUpdate {
    fn from(update: BlobUpdate) -> Self {
        match update {
            BlobUpdate::Put {
                lsn,
                key,
                shard,
                record_ref,
                current_epoch,
                lifecycle,
            } => Self::Put {
                lsn,
                key,
                shard,
                record_ref,
                current_epoch,
                lifecycle,
            },
            BlobUpdate::Tombstone { lsn, key } => Self::Tombstone { lsn, key },
            BlobUpdate::SetLifetime {
                lsn,
                key,
                logical_end_epoch,
                current_epoch,
            } => Self::SetLifetime {
                lsn,
                key,
                logical_end_epoch,
                current_epoch,
            },
            BlobUpdate::MapRef { lsn, key, from, to } => Self::MapRef { lsn, key, from, to },
        }
    }
}

pub(crate) trait RecordLsn {
    fn record_lsn(&self) -> StrataLsn;
}

impl RecordLsn for StateRecord {
    fn record_lsn(&self) -> StrataLsn {
        self.state.head_lsn
    }
}

impl RecordLsn for PatchRecord {
    fn record_lsn(&self) -> StrataLsn {
        self.updates
            .iter()
            .map(PatchUpdate::lsn)
            .max()
            .unwrap_or_default()
    }
}

impl RecordLsn for PatchUpdate {
    fn record_lsn(&self) -> StrataLsn {
        self.lsn()
    }
}

impl RecordLsn for BlobUpdate {
    fn record_lsn(&self) -> StrataLsn {
        self.lsn()
    }
}

impl<T> RecordLsn for &T
where
    T: RecordLsn + ?Sized,
{
    fn record_lsn(&self) -> StrataLsn {
        (*self).record_lsn()
    }
}

pub(crate) fn sort_updates(updates: &mut [BlobUpdate]) {
    // Run readers and k-way mergers rely on this physical ordering. The reducer itself is per-key,
    // but compaction needs all updates for a key to arrive as one contiguous, LSN-ordered slice.
    updates.sort_by(|left, right| {
        left.key()
            .cmp(right.key())
            .then_with(|| left.lsn().cmp(&right.lsn()))
    });
}
