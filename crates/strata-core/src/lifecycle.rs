use serde::{Deserialize, Serialize};

use crate::{Epoch, SegmentId, ShardKey, StrataLsn};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobLifecycleAction {
    SetLifetime {
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLifecycleOp {
    pub lsn: StrataLsn,
    pub action: BlobLifecycleAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobLifecycleMergeOp {
    Append(BlobLifecycleOp),
    RollbackFrom { lsn: StrataLsn },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLifetimeHead {
    pub lsn: StrataLsn,
    pub current_epoch: Epoch,
    pub lifecycle: BlobLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlobLifecycleHead {
    pub lifetime: Option<BlobLifetimeHead>,
    pub expiry_lsn: Option<StrataLsn>,
    pub tombstone_lsn: Option<StrataLsn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlobLifecycleState {
    pub head: BlobLifecycleHead,
    pub tail: Vec<BlobLifecycleOp>,
}

impl BlobLifecycleOp {
    pub fn lsn(&self) -> StrataLsn {
        self.lsn
    }
}

impl BlobLifecycleHead {
    fn apply_op(&mut self, op: &BlobLifecycleOp) {
        match op.action {
            BlobLifecycleAction::SetLifetime {
                logical_end_epoch,
                current_epoch,
            } => {
                let previous = self.lifetime.as_ref().filter(|head| op.lsn > head.lsn);
                let previous_expired =
                    previous.is_some_and(|head| head.lifecycle.logical_end_epoch <= current_epoch);
                if previous_expired && self.expiry_lsn.is_none_or(|expiry_lsn| op.lsn > expiry_lsn)
                {
                    self.expiry_lsn = Some(op.lsn);
                }
                let extension_count = if previous_expired {
                    0
                } else {
                    previous.map_or(0, |head| head.lifecycle.extension_count.saturating_add(1))
                };
                self.lifetime = Some(BlobLifetimeHead {
                    lsn: op.lsn,
                    current_epoch,
                    lifecycle: BlobLifecycle {
                        logical_end_epoch,
                        extension_count,
                    },
                });
            }
            BlobLifecycleAction::Tombstone => {
                if self
                    .tombstone_lsn
                    .is_none_or(|tombstone_lsn| op.lsn > tombstone_lsn)
                {
                    self.tombstone_lsn = Some(op.lsn);
                }
            }
        }
    }
}

impl BlobLifecycleState {
    pub fn is_empty(&self) -> bool {
        self.head.lifetime.is_none()
            && self.head.expiry_lsn.is_none()
            && self.head.tombstone_lsn.is_none()
            && self.tail.is_empty()
    }

    pub fn append_op(&mut self, op: BlobLifecycleOp) {
        self.tail.push(op);
    }

    pub fn apply_merge_op(&mut self, op: BlobLifecycleMergeOp) {
        match op {
            BlobLifecycleMergeOp::Append(op) => self.append_op(op),
            BlobLifecycleMergeOp::RollbackFrom { lsn } => self.rollback_from(lsn),
        }
    }

    pub fn apply_merge_ops(&mut self, ops: impl IntoIterator<Item = BlobLifecycleMergeOp>) {
        for op in ops {
            self.apply_merge_op(op);
        }
    }

    pub fn rollback_from(&mut self, rollback_from: StrataLsn) {
        self.tail.retain(|op| op.lsn() < rollback_from);
    }

    pub fn resolve_at(&self, max_lsn: StrataLsn) -> BlobLifecycleHead {
        let mut head = BlobLifecycleHead {
            lifetime: self
                .head
                .lifetime
                .as_ref()
                .filter(|lifetime| lifetime.lsn <= max_lsn)
                .cloned(),
            expiry_lsn: self
                .head
                .expiry_lsn
                .filter(|expiry_lsn| *expiry_lsn <= max_lsn),
            tombstone_lsn: self
                .head
                .tombstone_lsn
                .filter(|tombstone_lsn| *tombstone_lsn <= max_lsn),
        };
        let mut tail = self
            .tail
            .iter()
            .filter(|op| op.lsn() <= max_lsn)
            .collect::<Vec<_>>();
        tail.sort_by_key(|op| op.lsn());

        for op in tail {
            head.apply_op(op);
        }

        head
    }

    pub fn op_at_lsn(&self, lsn: StrataLsn) -> Option<BlobLifecycleOp> {
        let lifetime_op = self
            .head
            .lifetime
            .as_ref()
            .filter(|lifetime| lifetime.lsn == lsn)
            .map(|lifetime| BlobLifecycleOp {
                lsn,
                action: BlobLifecycleAction::SetLifetime {
                    logical_end_epoch: lifetime.lifecycle.logical_end_epoch,
                    current_epoch: lifetime.current_epoch,
                },
            });
        let tombstone_op = (self.head.tombstone_lsn == Some(lsn)).then_some(BlobLifecycleOp {
            lsn,
            action: BlobLifecycleAction::Tombstone,
        });
        let mut ops = lifetime_op
            .into_iter()
            .chain(tombstone_op)
            .chain(self.tail.iter().filter(|op| op.lsn() == lsn).cloned());
        let op = ops.next();
        debug_assert!(
            ops.next().is_none(),
            "multiple lifecycle ops found for one LSN"
        );
        op
    }

    pub fn compact_through(&mut self, compact_safe_lsn: StrataLsn) {
        let mut tail = std::mem::take(&mut self.tail);
        tail.sort_by_key(|op| op.lsn());

        for op in tail {
            if op.lsn() <= compact_safe_lsn {
                self.head.apply_op(&op);
            } else {
                self.tail.push(op);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StoreStateKey {
    NextLsn,
    PublishedLsn,
    CurrentEpoch,
    /// Reserved legacy key. Kept in this position so persisted enum discriminants remain stable.
    LegacyProjectionFrontier,
    /// Reserved legacy copy of `PublishedLsn`. New checkpoints store only physical coordinates;
    /// this key remains here so persisted enum discriminants stay stable.
    LsmDurableLsn,
    /// Store-checkpoint fields. The `Lsm` names are retained only because this enum's serialized
    /// discriminants are already on disk.
    LsmWalLogId,
    LsmWalOffset,
    LsmActiveSegmentId,
    LsmActiveSegmentOffset,
    /// Reserved fields from the removed relocation-LSM WAL. Do not reuse these discriminants.
    RelocationLsmDurableLsn,
    RelocationLsmWalLogId,
    RelocationLsmWalOffset,
    /// First LSN whose epoch/shard terminal garbage is emitted by blob-LSM compaction.
    ///
    /// Earlier transitions may already have terminal garbage materialized by an older release.
    /// Kept in its original enum position so the persisted discriminant remains stable.
    BlobCompactionGarbageFromLsn,
    /// First store-WAL file that recovery must retain and validate.
    /// Appended here so every preceding persisted discriminant remains stable.
    StoreWalRetainedFrom,
}

/// Exclusive end of a store-WAL prefix made durable by a completed file sync.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WalPosition {
    pub log_id: u64,
    pub offset: u64,
}

/// Physical coordinates associated atomically with RocksDB's `PublishedLsn`.
///
/// For example, when `PublishedLsn = 42`, `wal_position` is the exact WAL prefix through LSN 42,
/// while `active_segment_id` and `active_segment_offset` identify the active payload prefix synced
/// by that publication. The checkpoint deliberately has no LSN of its own: `PublishedLsn` is the
/// store's single logical durability frontier. It says nothing about whether blob or relocation
/// LSM memtables have been flushed to SSTs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCheckpoint {
    pub wal_position: WalPosition,
    pub active_segment_id: SegmentId,
    pub active_segment_offset: u64,
}

/// Store-state field scoped to one shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardStoreStateKey {
    pub shard: ShardKey,
    pub field: StoreStateKey,
}

/// LSN-keyed metadata row scoped to one shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardLsnKey {
    pub shard: ShardKey,
    pub lsn: StrataLsn,
}
