//! Global compaction facts captured once per blob-LSM compaction.

use std::collections::BTreeMap;

use core_types::{Epoch, SegmentId, ShardId, ShardInfo, ShardKey, ShardState};
use lsm::StrataLsn;

/// Durable global facts captured once before one blob-LSM compaction.
///
/// The merge operator never performs an index lookup for an individual blob. Epoch transitions,
/// shard generation fences, and bulk-reclaimed shard segments are all resolved from this snapshot.
#[derive(Debug, Clone, Default)]
pub(crate) struct BlobCompactionSnapshot {
    /// Complete manifest frontier bounding every global fact this compaction may apply.
    pub(crate) materialized_through_lsn: StrataLsn,
    pub(crate) emit_garbage_from_lsn: StrataLsn,
    pub(crate) epoch_changes: Vec<(StrataLsn, Epoch)>,
    pub(crate) shard_infos: BTreeMap<ShardId, ShardInfo>,
    pub(crate) shard_drop_lsns: BTreeMap<ShardKey, StrataLsn>,
    pub(crate) reclaimed_shard_segments: BTreeMap<SegmentId, ShardKey>,
}

/// One global transition that terminally ends blob versions.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TerminalTransition {
    pub(crate) lsn: StrataLsn,
    pub(crate) emit_garbage: bool,
}

impl BlobCompactionSnapshot {
    pub(crate) fn expiry(&self, logical_end_epoch: Epoch) -> Option<TerminalTransition> {
        let index = self
            .epoch_changes
            .partition_point(|(_, epoch)| *epoch < logical_end_epoch);
        let &(lsn, _) = self.epoch_changes.get(index)?;
        if lsn > self.materialized_through_lsn {
            return None;
        }
        Some(TerminalTransition {
            lsn,
            emit_garbage: lsn >= self.emit_garbage_from_lsn,
        })
    }

    pub(crate) fn shard_retirement(&self, shard: ShardKey) -> Option<TerminalTransition> {
        if let Some(&drop_lsn) = self.shard_drop_lsns.get(&shard) {
            if drop_lsn > self.materialized_through_lsn {
                return None;
            }
            return Some(TerminalTransition {
                lsn: drop_lsn,
                emit_garbage: drop_lsn >= self.emit_garbage_from_lsn,
            });
        }

        let obsolete = match self.shard_infos.get(&shard.id) {
            Some(info) if shard.generation < info.current_generation => true,
            Some(info)
                if shard.generation == info.current_generation
                    && info.state == ShardState::Dropped =>
            {
                true
            }
            _ => false,
        };
        // A missing drop tombstone identifies a generation handled before snapshot compaction was
        // introduced. Its physical retirement was already projected by the pre-cutover path.
        obsolete.then_some(TerminalTransition {
            lsn: self.materialized_through_lsn,
            emit_garbage: false,
        })
    }
}
