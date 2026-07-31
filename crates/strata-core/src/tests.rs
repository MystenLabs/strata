use super::{PutMergeOp, PutOp, PutState};
use crate::{BlobState, PutEntry, RecordRef, SegmentId, ShardKey, StrataLsn};

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

fn put_entry(lsn: StrataLsn, record_ref: RecordRef) -> PutEntry {
    PutEntry {
        record_ref: Some(record_ref),
        lsn,
        generation: lsn,
        state: BlobState::Live,
    }
}

fn tombstone_entry(lsn: StrataLsn) -> PutEntry {
    PutEntry {
        record_ref: None,
        lsn,
        generation: lsn,
        state: BlobState::Tombstoned,
    }
}

fn op(shard: ShardKey, entry: PutEntry) -> PutOp {
    PutOp { shard, entry }
}

fn append_put(state: &mut PutState, shard: ShardKey, entry: PutEntry) {
    state.apply_merge_op(PutMergeOp::Append(op(shard, entry)));
}

#[test]
fn resolve_head_applies_tail_for_requested_shard_only() {
    let live_ref = record_ref(1, 10);
    let other_ref = record_ref(2, 20);
    let mut state = PutState::default();
    append_put(&mut state, OTHER_SHARD, put_entry(1, other_ref));
    append_put(&mut state, SHARD, put_entry(2, live_ref));

    let head = state.resolve_head(SHARD).unwrap();

    assert_eq!(head.head_lsn, 2);
    assert_eq!(head.payload_lsn, Some(2));
    assert_eq!(head.entry.record_ref, Some(live_ref));
}

#[test]
fn compact_through_folds_latest_payload_head() {
    let live_ref = record_ref(1, 10);
    let newer_ref = record_ref(1, 20);
    let mut state = PutState::default();
    append_put(&mut state, SHARD, put_entry(1, live_ref));
    append_put(&mut state, SHARD, put_entry(2, newer_ref));

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
    let mut state = PutState::default();
    append_put(&mut state, SHARD, put_entry(1, live_ref));
    append_put(&mut state, SHARD, tombstone_entry(2));

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
    let mut state = PutState::default();
    append_put(&mut state, SHARD, put_entry(1, live_ref));
    append_put(&mut state, SHARD, put_entry(2, newer_ref));

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
fn rollback_from_drops_pending_tail_at_or_after_lsn() {
    let live_ref = record_ref(1, 10);
    let newer_ref = record_ref(1, 20);
    let mut state = PutState::default();
    state.apply_merge_ops([
        PutMergeOp::Append(op(SHARD, put_entry(1, live_ref))),
        PutMergeOp::Append(op(SHARD, put_entry(2, newer_ref))),
        PutMergeOp::Append(op(SHARD, tombstone_entry(3))),
        PutMergeOp::RollbackFrom { lsn: 2 },
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
    let mut state = PutState::default();
    append_put(&mut state, SHARD, put_entry(1, live_ref));
    state.compact_through(1);

    state.apply_merge_op(PutMergeOp::RollbackFrom { lsn: 1 });

    assert!(state.tail.is_empty());
    let head = state.resolve_head(SHARD).unwrap();
    assert_eq!(head.head_lsn, 1);
    assert_eq!(head.entry.record_ref, Some(live_ref));
}

#[test]
fn rollback_from_drops_tail_across_shards_at_or_after_lsn() {
    let live_ref = record_ref(1, 10);
    let other_ref = record_ref(2, 20);
    let mut state = PutState::default();
    state.apply_merge_ops([
        PutMergeOp::Append(op(SHARD, put_entry(1, live_ref))),
        PutMergeOp::Append(op(OTHER_SHARD, put_entry(2, other_ref))),
        PutMergeOp::RollbackFrom { lsn: 2 },
    ]);

    let head = state.resolve_head(SHARD).unwrap();
    assert_eq!(head.head_lsn, 1);
    assert_eq!(head.entry.record_ref, Some(live_ref));
    assert_eq!(state.resolve_head(OTHER_SHARD), None);
}
