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

/// One unaccounted version op retained in a packed blob version value.
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

/// One rollbackable physical relocation for an existing payload version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapRefOp {
    /// LSN assigned to the GC publish operation.
    pub publish_lsn: StrataLsn,
    /// Shard whose physical payload ref is being rewritten.
    pub shard: ShardKey,
    /// Original payload version LSN being relocated.
    pub payload_lsn: StrataLsn,
    /// Source range that was copied by GC.
    pub from: RecordRef,
    /// Replacement range in the GC output segment.
    pub to: RecordRef,
}

/// Merge operand applied to a packed blob version value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutMergeOp {
    Append(PutOp),
    MapRef {
        publish_lsn: StrataLsn,
        shard: ShardKey,
        payload_lsn: StrataLsn,
        from: RecordRef,
        to: RecordRef,
    },
    RollbackFrom {
        lsn: StrataLsn,
    },
}

/// Resolved latest state for one shard after accounted version ops have been compacted.
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
    pub maps: Vec<MapRefOp>,
}

impl PutState {
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty() && self.tail.is_empty() && self.maps.is_empty()
    }

    pub fn apply_merge_op(&mut self, op: PutMergeOp) {
        match op {
            PutMergeOp::Append(op) => {
                // Keep new ops in the exact per-LSN tail until the compaction frontier crosses them.
                // Readers call `resolve_head()` to project `heads + tail`; only
                // `compact_through(compact_safe_lsn)` may fold tail history into compact heads.
                self.tail.push(op);
            }
            PutMergeOp::MapRef {
                publish_lsn,
                shard,
                payload_lsn,
                from,
                to,
            } => {
                // `payload_lsn` identifies the exact payload version being relocated. Matching only `from`
                // would corrupt a later version that reused the same physical ref: for example
                // `put@7 -> A`, `put@9 -> A`, then `MapRef(payload_lsn=7, A -> B)` must leave `put@9`
                // pointing at A. The same identity check is applied lazily during reads so the relocation
                // remains rollbackable by its own publish LSN until it is compacted through the durable
                // frontier.
                self.maps.push(MapRefOp {
                    publish_lsn,
                    shard,
                    payload_lsn,
                    from,
                    to,
                });
            }
            PutMergeOp::RollbackFrom { lsn } => {
                self.tail.retain(|op| op.lsn() < lsn);
                self.maps.retain(|op| op.publish_lsn < lsn);
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

    fn apply_map_ref_to_materialized(&mut self, map: &MapRefOp) {
        for op in self
            .tail
            .iter_mut()
            .filter(|op| op.shard == map.shard && op.lsn() == map.payload_lsn)
        {
            map_entry_ref(&mut op.entry, map.from, map.to);
        }

        if let Some(head) = self
            .heads
            .get_mut(&map.shard)
            .filter(|head| head.payload_lsn == Some(map.payload_lsn))
        {
            map_entry_ref(&mut head.entry, map.from, map.to);
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

        if let Some(head) = &mut head {
            let mut maps = self
                .maps
                .iter()
                .filter(|op| op.shard == shard && head.payload_lsn == Some(op.payload_lsn))
                .collect::<Vec<_>>();
            maps.sort_by_key(|op| op.publish_lsn);
            for map in maps {
                map_entry_ref(&mut head.entry, map.from, map.to);
            }
        }

        head
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn entry_at_lsn(&self, shard: ShardKey, lsn: StrataLsn) -> Option<PutEntry> {
        let mut entry = self
            .tail
            .iter()
            .rev()
            .find(|op| op.shard == shard && op.lsn() == lsn)
            .map(|op| op.entry.clone())
            .or_else(|| {
                self.heads
                    .get(&shard)
                    .filter(|head| head.head_lsn == lsn)
                    .map(|head| head.entry.clone())
            })?;

        let mut maps = self
            .maps
            .iter()
            .filter(|op| op.shard == shard && op.payload_lsn == lsn)
            .collect::<Vec<_>>();
        maps.sort_by_key(|op| op.publish_lsn);
        for map in maps {
            map_entry_ref(&mut entry, map.from, map.to);
        }

        Some(entry)
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

    pub fn map_ref_at_lsn(&self, lsn: StrataLsn) -> Option<MapRefOp> {
        let mut ops = self.maps.iter().filter(|op| op.publish_lsn == lsn).copied();
        let op = ops.next();
        debug_assert!(ops.next().is_none(), "multiple map refs found for one LSN");
        op
    }

    pub fn compact_through(&mut self, compact_safe_lsn: StrataLsn) {
        let compact_safe_lsns = self
            .tail
            .iter()
            .map(|op| op.shard)
            .chain(self.maps.iter().map(|op| op.shard))
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
                self.apply_accounted_op(&op);
            } else {
                self.tail.push(op);
            }
        }

        let mut maps = std::mem::take(&mut self.maps);
        maps.sort_by_key(|op| op.publish_lsn);
        for op in maps {
            if compact_safe_lsns
                .get(&op.shard)
                .is_some_and(|compact_safe_lsn| {
                    op.publish_lsn <= *compact_safe_lsn && op.payload_lsn <= *compact_safe_lsn
                })
            {
                self.apply_map_ref_to_materialized(&op);
            } else {
                self.maps.push(op);
            }
        }
    }

    fn apply_accounted_op(&mut self, op: &PutOp) {
        match self.heads.get_mut(&op.shard) {
            Some(head) => head.apply_op(op),
            None => {
                self.heads.insert(op.shard, PutHead::from_op(op));
            }
        }
    }
}

fn map_entry_ref(entry: &mut PutEntry, from: RecordRef, to: RecordRef) {
    if entry.is_live() && entry.record_ref == Some(from) {
        entry.record_ref = Some(to);
    }
}
