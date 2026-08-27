use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlobState, Generation, RecordRef, ShardKey, StrataLsn};

/// Payload entry stored in the Strata blob version index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutEntry {
    pub record_ref: Option<RecordRef>,
    pub lsn: StrataLsn,
    pub generation: Generation,
    pub state: BlobState,
}

impl PutEntry {
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

/// One pending version op retained in a packed blob value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutOp {
    pub shard: ShardKey,
    pub entry: PutEntry,
}

impl PutOp {
    pub fn lsn(&self) -> StrataLsn {
        self.entry.lsn
    }
}

/// Merge operand applied to a packed blob version value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutMergeOp {
    Append(PutOp),
    RollbackFrom { lsn: StrataLsn },
}

/// Resolved latest state for one shard after pending version ops have been compacted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutHead {
    pub head_lsn: StrataLsn,
    pub payload_lsn: Option<StrataLsn>,
    pub entry: PutEntry,
}

impl PutHead {
    fn from_op(op: &PutOp) -> Self {
        Self {
            head_lsn: op.lsn(),
            payload_lsn: op.entry.has_payload_ref().then_some(op.lsn()),
            entry: op.entry.clone(),
        }
    }

    fn apply_op(&mut self, op: &PutOp) {
        if op.lsn() <= self.head_lsn {
            return;
        }

        *self = Self::from_op(op);
    }
}

/// Packed version state for one blob key.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PutState {
    pub heads: BTreeMap<ShardKey, PutHead>,
    pub tail: Vec<PutOp>,
}

impl PutState {
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty() && self.tail.is_empty()
    }

    pub fn apply_merge_op(&mut self, op: PutMergeOp) {
        match op {
            PutMergeOp::Append(op) => {
                // Keep new ops in the exact per-LSN tail until the compaction frontier crosses them.
                // Readers call `resolve_head()` to project `heads + tail`; only
                // `compact_through(compact_safe_lsn)` may fold tail history into compact heads.
                self.tail.push(op);
            }
            PutMergeOp::RollbackFrom { lsn } => {
                self.tail.retain(|op| op.lsn() < lsn);
            }
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn apply_merge_ops(&mut self, ops: impl IntoIterator<Item = PutMergeOp>) {
        for op in ops {
            self.apply_merge_op(op);
        }
    }

    pub fn resolve_head(&self, shard: ShardKey) -> Option<PutHead> {
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
                None => head = Some(PutHead::from_op(op)),
            }
        }

        head
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn entry_at_lsn(&self, shard: ShardKey, lsn: StrataLsn) -> Option<PutEntry> {
        self.tail
            .iter()
            .rev()
            .find(|op| op.shard == shard && op.lsn() == lsn)
            .map(|op| op.entry.clone())
            .or_else(|| {
                self.heads
                    .get(&shard)
                    .filter(|head| head.head_lsn == lsn)
                    .map(|head| head.entry.clone())
            })
    }

    pub fn op_at_lsn(&self, lsn: StrataLsn) -> Option<PutOp> {
        let mut ops = self
            .heads
            .iter()
            .filter(|(_, head)| head.head_lsn == lsn)
            .map(|(shard, head)| PutOp {
                shard: *shard,
                entry: head.entry.clone(),
            })
            .chain(self.tail.iter().filter(|op| op.lsn() == lsn).cloned());
        let op = ops.next();
        debug_assert!(ops.next().is_none(), "multiple put ops found for one LSN");
        op
    }

    pub fn compact_through(&mut self, compact_safe_lsn: StrataLsn) {
        let compact_safe_lsns = self
            .tail
            .iter()
            .map(|op| op.shard)
            .map(|shard| (shard, compact_safe_lsn))
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
                self.apply_compacted_op(&op);
            } else {
                self.tail.push(op);
            }
        }
    }

    fn apply_compacted_op(&mut self, op: &PutOp) {
        match self.heads.get_mut(&op.shard) {
            Some(head) => head.apply_op(op),
            None => {
                self.heads.insert(op.shard, PutHead::from_op(op));
            }
        }
    }
}
