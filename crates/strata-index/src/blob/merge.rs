use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use strata_core::{
    BlobLifecycleMergeOp, BlobLifecycleState, BlobVersionState, PutMergeOp, PutState, ShardId,
    ShardInfo, StrataLsn,
};

use crate::{Error, Result};

use super::super::shard::shard_generation_is_obsolete;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BlobVersionMergeOp {
    Version(PutMergeOp),
    Lifecycle(BlobLifecycleMergeOp),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EncodedBlobVersionMergeOperand {
    Op(BlobVersionMergeOp),
    Ops(Vec<BlobVersionMergeOp>),
}

impl EncodedBlobVersionMergeOperand {
    fn into_ops(self) -> Vec<BlobVersionMergeOp> {
        match self {
            Self::Op(op) => vec![op],
            Self::Ops(ops) => ops,
        }
    }
}

pub(crate) fn merge_blob_versions(
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
    compact_safe_lsn: StrataLsn,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> Option<Vec<u8>> {
    // Each RocksDB merge operand appends either a payload version operation or a lifecycle operation.
    // Full merge applies those operands, then folds only ops at or below the caller provided compaction
    // frontier into compact heads. Newer ops stay in the tail so `resolve_head()` and exact per LSN
    // lookups can still see them.
    let mut state = match existing_value {
        Some(value) => bcs::from_bytes::<BlobVersionState>(value).ok()?,
        None => BlobVersionState::default(),
    };

    for operand in operands {
        let operand = decode_blob_version_merge_operand(operand).ok()?;
        for op in operand.into_ops() {
            apply_blob_version_merge_op(&mut state, op);
        }
    }

    state.versions.compact_through(compact_safe_lsn);
    state.lifecycle.compact_through(compact_safe_lsn);
    // Shard generations are physical write fences. A logical shard id may be dropped and recreated,
    // so any version rows from an older generation must disappear from the packed value before a
    // later reader can accidentally resolve a stale payload as current.
    prune_obsolete_shard_versions(&mut state.versions, shard_infos);
    bcs::to_bytes(&state).ok()
}

fn apply_blob_version_merge_op(state: &mut BlobVersionState, op: BlobVersionMergeOp) {
    // Payload and lifecycle histories share one RocksDB key so a single merge fold preserves their
    // relative LSN order for rollback/accounting queries. The two reducers remain separate because
    // payload visibility and lifecycle/tombstone metadata compact under different invariants.
    match op {
        BlobVersionMergeOp::Version(op) => state.versions.apply_merge_op(op),
        BlobVersionMergeOp::Lifecycle(op) => state.lifecycle.apply_merge_op(op),
    }
}

pub(crate) fn prune_obsolete_shard_versions(
    state: &mut PutState,
    shard_infos: &BTreeMap<ShardId, ShardInfo>,
) -> bool {
    // This is not ordinary history compaction. Obsolete generations name physical writers that are
    // no longer allowed to publish records for the logical shard. Keeping those heads or tails would
    // let stale handles survive a drop/recreate boundary.
    let initial_heads = state.heads.len();
    let initial_tail = state.tail.len();
    let initial_maps = state.maps.len();
    state
        .heads
        .retain(|shard, _| !shard_generation_is_obsolete(*shard, shard_infos));
    state
        .tail
        .retain(|op| !shard_generation_is_obsolete(op.shard, shard_infos));
    state
        .maps
        .retain(|op| !shard_generation_is_obsolete(op.shard, shard_infos));
    state.heads.len() != initial_heads
        || state.tail.len() != initial_tail
        || state.maps.len() != initial_maps
}

pub(crate) fn remove_version_lsns_from_state(state: &mut PutState, lsns: &BTreeSet<StrataLsn>) {
    // Recovery rollback removes by exact LSN instead of compacting by frontier. These ops belonged
    // to writes whose durable record was lost, so both unresolved tails and already-folded heads must
    // be stripped without disturbing neighboring committed history.
    state.tail.retain(|op| !lsns.contains(&op.lsn()));
    state.maps.retain(|op| !lsns.contains(&op.publish_lsn));
    state.heads.retain(|_, head| !lsns.contains(&head.head_lsn));
}

pub(crate) fn remove_lifecycle_lsns_from_state(
    state: &mut BlobLifecycleState,
    lsns: &BTreeSet<StrataLsn>,
) {
    // Lifecycle rollback has the same exact-LSN shape as payload rollback, but the compacted head
    // can contain independent lifetime and tombstone facts. Each compacted fact is removed only if
    // its own LSN was lost.
    state.tail.retain(|op| !lsns.contains(&op.lsn()));
    if let Some(lifetime) = &state.head.lifetime
        && lsns.contains(&lifetime.lsn)
    {
        state.head.lifetime = None;
    }
    if state.head.expiry_lsn.is_some_and(|lsn| lsns.contains(&lsn)) {
        state.head.expiry_lsn = None;
    }
    if state
        .head
        .tombstone_lsn
        .is_some_and(|lsn| lsns.contains(&lsn))
    {
        state.head.tombstone_lsn = None;
    }
}

pub(crate) fn full_merge_blob_versions(
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    merge_shard_infos: Arc<RwLock<BTreeMap<u32, ShardInfo>>>,
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let compact_safe_lsn = *compact_safe_lsn
        .read()
        .expect("blob version compaction frontier lock poisoned");
    let shard_infos = merge_shard_infos
        .read()
        .expect("shard info cache lock poisoned")
        .clone();
    merge_blob_versions(existing_value, operands, compact_safe_lsn, &shard_infos)
}

pub(crate) fn partial_merge_blob_versions(operands: &MergeOperands) -> Option<Vec<u8>> {
    // Partial merge has no access to the existing packed value or the current compaction frontier,
    // so it must not fold history. It only concatenates operands to reduce RocksDB merge fan-out;
    // full merge decides what can be materialized or pruned.
    let mut ops = Vec::new();
    for operand in operands {
        let operand = decode_blob_version_merge_operand(operand).ok()?;
        ops.extend(operand.into_ops());
    }
    bcs::to_bytes(&EncodedBlobVersionMergeOperand::Ops(ops)).ok()
}

pub(crate) fn encode_blob_version_merge_operand(
    operand: EncodedBlobVersionMergeOperand,
) -> Result<Vec<u8>> {
    bcs::to_bytes(&operand).map_err(|err| Error::Serialization(err.to_string()))
}

fn decode_blob_version_merge_operand(data: &[u8]) -> Result<EncodedBlobVersionMergeOperand> {
    bcs::from_bytes(data).map_err(|err| Error::Serialization(err.to_string()))
}
