use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::SegmentId;

pub type Epoch = u64;
pub type Generation = u64;
pub type ShardGeneration = u64;
pub type ShardId = u32;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardState {
    Active,
    Dropping,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    pub id: ShardId,
    pub generation: ShardGeneration,
}

/// Payload entry stored in the Strata blob version index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobEntry {
    pub record_ref: Option<RecordRef>,
    pub lsn: StrataLsn,
    pub generation: Generation,
    pub state: BlobState,
}

/// Resolved latest state for one shard after accounted version ops have been compacted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardHead {
    pub head_lsn: StrataLsn,
    pub payload_lsn: Option<StrataLsn>,
    pub entry: BlobEntry,
}

/// One unaccounted version op retained in a packed blob version value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionOp {
    pub shard: ShardKey,
    pub entry: BlobEntry,
}

/// Merge operand applied to a packed blob version value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionMergeOp {
    Append(VersionOp),
    MapRef {
        shard: ShardKey,
        payload_lsn: StrataLsn,
        from: RecordRef,
        to: RecordRef,
    },
    RollbackFrom {
        shard: ShardKey,
        lsn: StrataLsn,
    },
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

/// Packed metadata state for one blob key.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlobVersionState {
    pub versions: VersionState,
    pub lifecycle: BlobLifecycleState,
}

impl VersionOp {
    pub fn lsn(&self) -> StrataLsn {
        self.entry.lsn
    }
}

/// Packed version state for one blob key.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VersionState {
    pub heads: BTreeMap<ShardKey, ShardHead>,
    pub tail: Vec<VersionOp>,
}

impl ShardHead {
    fn from_op(op: &VersionOp) -> Self {
        Self {
            head_lsn: op.lsn(),
            payload_lsn: op.entry.has_payload_ref().then_some(op.lsn()),
            entry: op.entry.clone(),
        }
    }

    fn apply_op(&mut self, op: &VersionOp) {
        if op.lsn() <= self.head_lsn {
            return;
        }

        *self = Self::from_op(op);
    }
}

impl VersionState {
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty() && self.tail.is_empty()
    }

    pub fn append_op(&mut self, op: VersionOp) {
        self.tail.push(op);
    }

    pub fn apply_merge_op(&mut self, op: VersionMergeOp) {
        match op {
            VersionMergeOp::Append(op) => {
                // Keep new ops in the exact per-LSN tail until the compaction frontier crosses them.
                // Readers call `resolve_head()` to project `heads + tail`; only
                // `compact_through(compact_safe_lsn)` may fold tail history into compact heads.
                self.append_op(op)
            }
            VersionMergeOp::MapRef {
                shard,
                payload_lsn,
                from,
                to,
            } => self.map_ref(shard, payload_lsn, from, to),
            VersionMergeOp::RollbackFrom { shard, lsn } => self.rollback_from(shard, lsn),
        }
    }

    pub fn apply_merge_ops(&mut self, ops: impl IntoIterator<Item = VersionMergeOp>) {
        for op in ops {
            self.apply_merge_op(op);
        }
    }

    pub fn rollback_from(&mut self, shard: ShardKey, rollback_from: StrataLsn) {
        self.tail
            .retain(|op| op.shard != shard || op.lsn() < rollback_from);
    }

    pub fn map_ref(
        &mut self,
        shard: ShardKey,
        payload_lsn: StrataLsn,
        from: RecordRef,
        to: RecordRef,
    ) {
        // `payload_lsn` identifies the exact payload version being relocated. Matching only `from`
        // would corrupt a later version that reused the same physical ref: for example
        // `put@7 -> A`, `put@9 -> A`, then `MapRef(payload_lsn=7, A -> B)` must leave `put@9`
        // pointing at A. The same identity check works whether that payload is still in the tail or
        // has already been folded into a compact head.
        for op in self
            .tail
            .iter_mut()
            .filter(|op| op.shard == shard && op.lsn() == payload_lsn)
        {
            map_entry_ref(&mut op.entry, from, to);
        }

        if let Some(head) = self
            .heads
            .get_mut(&shard)
            .filter(|head| head.payload_lsn == Some(payload_lsn))
        {
            map_entry_ref(&mut head.entry, from, to);
        }
    }

    pub fn resolve_head(&self, shard: ShardKey) -> Option<ShardHead> {
        let mut head = self.heads.get(&shard).cloned();
        let mut tail = self
            .tail
            .iter()
            .filter(|op| op.shard == shard)
            .collect::<Vec<_>>();
        tail.sort_by_key(|op| op.lsn());

        for op in tail {
            match &mut head {
                Some(head) => head.apply_op(op),
                None => head = Some(ShardHead::from_op(op)),
            }
        }

        head
    }

    pub fn ops_at_lsn(&self, lsn: StrataLsn) -> Vec<VersionOp> {
        let mut ops = Vec::new();
        for (shard, head) in &self.heads {
            if head.head_lsn == lsn {
                ops.push(VersionOp {
                    shard: *shard,
                    entry: head.entry.clone(),
                });
            }
        }
        ops.extend(self.tail.iter().filter(|op| op.lsn() == lsn).cloned());
        ops.sort_by_key(|op| op.shard);
        ops
    }

    pub fn compact_through(&mut self, compact_safe_lsn: StrataLsn) {
        let compact_safe_lsns = self
            .tail
            .iter()
            .map(|op| (op.shard, compact_safe_lsn))
            .collect::<BTreeMap<_, _>>();
        self.compact_through_shards(&compact_safe_lsns);
    }

    pub fn compact_through_shards(&mut self, compact_safe_lsns: &BTreeMap<ShardKey, StrataLsn>) {
        let mut tail = std::mem::take(&mut self.tail);
        tail.sort_by_key(|op| op.lsn());

        for op in tail {
            if compact_safe_lsns
                .get(&op.shard)
                .is_some_and(|compact_safe_lsn| op.lsn() <= *compact_safe_lsn)
            {
                self.apply_accounted_op(&op);
            } else {
                self.tail.push(op);
            }
        }
    }

    fn apply_accounted_op(&mut self, op: &VersionOp) {
        match self.heads.get_mut(&op.shard) {
            Some(head) => head.apply_op(op),
            None => {
                self.heads.insert(op.shard, ShardHead::from_op(op));
            }
        }
    }
}

fn map_entry_ref(entry: &mut BlobEntry, from: RecordRef, to: RecordRef) {
    if entry.is_live() && entry.record_ref == Some(from) {
        entry.record_ref = Some(to);
    }
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

    pub fn ops_at_lsn(&self, lsn: StrataLsn) -> Vec<BlobLifecycleOp> {
        let mut ops = Vec::new();
        if let Some(lifetime) = &self.head.lifetime
            && lifetime.lsn == lsn
        {
            ops.push(BlobLifecycleOp {
                lsn,
                action: BlobLifecycleAction::SetLifetime {
                    logical_end_epoch: lifetime.lifecycle.logical_end_epoch,
                    current_epoch: lifetime.current_epoch,
                },
            });
        }
        if self.head.tombstone_lsn == Some(lsn) {
            ops.push(BlobLifecycleOp {
                lsn,
                action: BlobLifecycleAction::Tombstone,
            });
        }
        ops.extend(self.tail.iter().filter(|op| op.lsn() == lsn).cloned());
        ops
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

impl BlobVersionState {
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty() && self.lifecycle.is_empty()
    }
}

/// Compatibility view key for a blob version identified by logical LSN.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobVersionKey {
    pub key: crate::BlobKey,
    pub lsn: StrataLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrataStoreState {
    pub next_lsn: StrataLsn,
    pub durable_lsn: StrataLsn,
    pub accounted_lsn: StrataLsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StoreStateKey {
    NextLsn,
    DurableLsn,
    CurrentEpoch,
    AccountedLsn,
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

impl Default for StrataStoreState {
    fn default() -> Self {
        Self {
            next_lsn: 1,
            durable_lsn: 0,
            accounted_lsn: 0,
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

    pub fn has_payload_ref(&self) -> bool {
        self.state == BlobState::Live && self.record_ref.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARD: ShardKey = ShardKey {
        id: 7,
        generation: 3,
    };

    const OTHER_SHARD: ShardKey = ShardKey {
        id: 8,
        generation: 3,
    };

    fn record_ref(segment_id: SegmentId, offset: u64) -> RecordRef {
        RecordRef {
            segment_id,
            offset,
            len: 10,
        }
    }

    fn put_entry(lsn: StrataLsn, record_ref: RecordRef) -> BlobEntry {
        BlobEntry {
            record_ref: Some(record_ref),
            lsn,
            generation: lsn,
            state: BlobState::Live,
        }
    }

    fn tombstone_entry(lsn: StrataLsn) -> BlobEntry {
        BlobEntry {
            record_ref: None,
            lsn,
            generation: lsn,
            state: BlobState::Tombstoned,
        }
    }

    fn op(shard: ShardKey, entry: BlobEntry) -> VersionOp {
        VersionOp { shard, entry }
    }

    #[test]
    fn resolve_head_applies_tail_for_requested_shard_only() {
        let live_ref = record_ref(1, 10);
        let other_ref = record_ref(2, 20);
        let mut state = VersionState::default();
        state.append_op(op(OTHER_SHARD, put_entry(1, other_ref)));
        state.append_op(op(SHARD, put_entry(2, live_ref)));

        let head = state.resolve_head(SHARD).unwrap();

        assert_eq!(head.head_lsn, 2);
        assert_eq!(head.payload_lsn, Some(2));
        assert_eq!(head.entry.record_ref, Some(live_ref));
    }

    #[test]
    fn compact_through_folds_latest_payload_head() {
        let live_ref = record_ref(1, 10);
        let newer_ref = record_ref(1, 20);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(1, live_ref)));
        state.append_op(op(SHARD, put_entry(2, newer_ref)));

        state.compact_through(2);

        assert!(state.tail.is_empty());
        let head = state.heads.get(&SHARD).unwrap();
        assert_eq!(head.head_lsn, 2);
        assert_eq!(head.payload_lsn, Some(2));
        assert_eq!(head.entry.lsn, 2);
        assert_eq!(head.entry.generation, 2);
        assert_eq!(head.entry.record_ref, Some(newer_ref));
    }

    #[test]
    fn compact_through_tombstone_clears_payload_ref() {
        let live_ref = record_ref(1, 10);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(1, live_ref)));
        state.append_op(op(SHARD, tombstone_entry(2)));

        state.compact_through(2);

        let head = state.heads.get(&SHARD).unwrap();
        assert_eq!(head.head_lsn, 2);
        assert_eq!(head.payload_lsn, None);
        assert_eq!(head.entry.state, BlobState::Tombstoned);
        assert_eq!(head.entry.record_ref, None);
    }

    #[test]
    fn compact_through_retains_newer_tail_but_resolve_applies_it() {
        let live_ref = record_ref(1, 10);
        let newer_ref = record_ref(1, 20);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(1, live_ref)));
        state.append_op(op(SHARD, put_entry(2, newer_ref)));

        state.compact_through(1);

        assert_eq!(state.tail.len(), 1);
        assert_eq!(state.tail[0].lsn(), 2);
        let compacted_head = state.heads.get(&SHARD).unwrap();
        assert_eq!(compacted_head.entry.record_ref, Some(live_ref));

        let resolved = state.resolve_head(SHARD).unwrap();
        assert_eq!(resolved.head_lsn, 2);
        assert_eq!(resolved.payload_lsn, Some(2));
        assert_eq!(resolved.entry.record_ref, Some(newer_ref));
    }

    #[test]
    fn rollback_from_drops_unaccounted_tail_at_or_after_lsn() {
        let live_ref = record_ref(1, 10);
        let newer_ref = record_ref(1, 20);
        let mut state = VersionState::default();
        state.apply_merge_ops([
            VersionMergeOp::Append(op(SHARD, put_entry(1, live_ref))),
            VersionMergeOp::Append(op(SHARD, put_entry(2, newer_ref))),
            VersionMergeOp::Append(op(SHARD, tombstone_entry(3))),
            VersionMergeOp::RollbackFrom {
                shard: SHARD,
                lsn: 2,
            },
        ]);

        assert_eq!(state.tail.len(), 1);
        assert_eq!(state.tail[0].lsn(), 1);
        let head = state.resolve_head(SHARD).unwrap();
        assert_eq!(head.head_lsn, 1);
        assert_eq!(head.entry.record_ref, Some(live_ref));
    }

    #[test]
    fn rollback_from_does_not_remove_compacted_heads() {
        let live_ref = record_ref(1, 10);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(1, live_ref)));
        state.compact_through(1);

        state.apply_merge_op(VersionMergeOp::RollbackFrom {
            shard: SHARD,
            lsn: 1,
        });

        assert!(state.tail.is_empty());
        let head = state.resolve_head(SHARD).unwrap();
        assert_eq!(head.head_lsn, 1);
        assert_eq!(head.entry.record_ref, Some(live_ref));
    }

    #[test]
    fn rollback_from_only_drops_tail_for_matching_shard() {
        let live_ref = record_ref(1, 10);
        let other_ref = record_ref(2, 20);
        let mut state = VersionState::default();
        state.apply_merge_ops([
            VersionMergeOp::Append(op(SHARD, put_entry(1, live_ref))),
            VersionMergeOp::Append(op(OTHER_SHARD, put_entry(1, other_ref))),
            VersionMergeOp::RollbackFrom {
                shard: SHARD,
                lsn: 1,
            },
        ]);

        assert_eq!(state.resolve_head(SHARD), None);
        let other_head = state.resolve_head(OTHER_SHARD).unwrap();
        assert_eq!(other_head.head_lsn, 1);
        assert_eq!(other_head.entry.record_ref, Some(other_ref));
    }

    #[test]
    fn map_ref_rewrites_exact_tail_payload_ref() {
        let source = record_ref(1, 10);
        let destination = record_ref(2, 20);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(7, source)));

        state.apply_merge_op(VersionMergeOp::MapRef {
            shard: SHARD,
            payload_lsn: 7,
            from: source,
            to: destination,
        });

        let head = state.resolve_head(SHARD).unwrap();
        assert_eq!(head.head_lsn, 7);
        assert_eq!(head.entry.record_ref, Some(destination));
    }

    #[test]
    fn map_ref_rewrites_exact_compacted_head_payload_ref() {
        let source = record_ref(1, 10);
        let destination = record_ref(2, 20);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(7, source)));
        state.compact_through(7);

        state.apply_merge_op(VersionMergeOp::MapRef {
            shard: SHARD,
            payload_lsn: 7,
            from: source,
            to: destination,
        });

        let head = state.resolve_head(SHARD).unwrap();
        assert_eq!(head.entry.record_ref, Some(destination));
    }

    #[test]
    fn map_ref_does_not_rewrite_mismatched_lsn_or_ref() {
        let source = record_ref(1, 10);
        let destination = record_ref(2, 20);
        let mut state = VersionState::default();
        state.append_op(op(SHARD, put_entry(7, source)));

        state.apply_merge_ops([
            VersionMergeOp::MapRef {
                shard: SHARD,
                payload_lsn: 6,
                from: source,
                to: destination,
            },
            VersionMergeOp::MapRef {
                shard: SHARD,
                payload_lsn: 7,
                from: record_ref(9, 90),
                to: destination,
            },
        ]);

        let head = state.resolve_head(SHARD).unwrap();
        assert_eq!(head.entry.record_ref, Some(source));
    }
}
