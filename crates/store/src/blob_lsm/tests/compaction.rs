//! Snapshot-compaction pruning: expiry frontiers, shard retirement, and bulk reclamation.

use std::collections::BTreeMap;

use core_types::{BlobLifecycle, GarbageEvent, ShardInfo, ShardState};
use lsm::{MergeOperator, StoredValue, decode_value, encode_inline_value};

use crate::blob_lsm::{
    BlobCompactionSnapshot, BlobLifetime, BlobMergeWithRelocations, BlobMutation, BlobState,
    BlobVersion,
};

use super::{compact_state, inline_patch, record, shard};

#[test]
fn snapshot_compaction_waits_for_the_materialized_epoch_frontier() {
    let shard = shard(1, 1);
    let record = record(7, 10);
    let state = BlobState {
        versions: BTreeMap::from([(
            shard,
            BlobVersion {
                lsn: 2,
                write_epoch: 5,
                record_ref: record,
            },
        )]),
        lifetime: Some(BlobLifetime {
            lsn: 1,
            lifecycle: BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            },
        }),
    };
    let epoch_changes = vec![(0, 5), (4, 10)];

    // LSN 3 may still contain a pre-expiry extension in a memtable. The index knows that
    // epoch 10 was reached at LSN 4, but an SST-only compaction is complete only through LSN 2.
    let (before_extension, garbage) = compact_state(
        state.clone(),
        BlobCompactionSnapshot {
            materialized_through_lsn: 2,
            emit_garbage_from_lsn: 0,
            epoch_changes: epoch_changes.clone(),
            ..BlobCompactionSnapshot::default()
        },
    );
    assert_eq!(
        before_extension.unwrap().versions[&shard].record_ref,
        record
    );
    assert!(garbage.is_empty());

    let encoded_base = encode_inline_value(&state.encode().unwrap());
    let extension = inline_patch(BlobMutation::SetLifetime {
        logical_end_epoch: 20,
        current_epoch: 9,
    });
    let merge = BlobMergeWithRelocations::new(
        None,
        BlobCompactionSnapshot {
            materialized_through_lsn: 4,
            emit_garbage_from_lsn: 0,
            epoch_changes,
            ..BlobCompactionSnapshot::default()
        },
    );
    let mut garbage = Vec::new();
    let merged = merge
        .merge(
            b"blob",
            Some(&encoded_base),
            &[(3, extension.as_slice())],
            &mut |record| {
                garbage.push(record);
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
    let StoredValue::Inline(bytes) = decode_value(&merged).unwrap() else {
        panic!("full merge must materialize inline state");
    };
    let state = BlobState::decode(bytes).unwrap();

    assert_eq!(state.resolve(shard, 10).unwrap().0.record_ref, record);
    assert_eq!(
        state
            .resolve(shard, 10)
            .unwrap()
            .1
            .unwrap()
            .logical_end_epoch,
        20
    );
    assert!(
        garbage
            .iter()
            .all(|record| !matches!(record.event, GarbageEvent::Expired { .. }))
    );
}

#[test]
fn snapshot_compaction_expires_versions_at_the_first_reaching_epoch() {
    let shard = shard(1, 1);
    let record = record(7, 10);
    let state = BlobState {
        versions: BTreeMap::from([(
            shard,
            BlobVersion {
                lsn: 3,
                write_epoch: 5,
                record_ref: record,
            },
        )]),
        lifetime: Some(BlobLifetime {
            lsn: 4,
            lifecycle: BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            },
        }),
    };
    let snapshot = BlobCompactionSnapshot {
        materialized_through_lsn: 20,
        emit_garbage_from_lsn: 8,
        epoch_changes: vec![(0, 5), (9, 8), (12, 10), (18, 11)],
        shard_infos: BTreeMap::from([(1, ShardInfo::active(1))]),
        ..BlobCompactionSnapshot::default()
    };

    let (state, garbage) = compact_state(state, snapshot);

    assert!(state.unwrap().versions.is_empty());
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].lsn, 12);
    assert_eq!(garbage[0].event, GarbageEvent::Expired { record });
}

#[test]
fn snapshot_compaction_silently_prunes_legacy_global_expiry() {
    let shard = shard(1, 1);
    let record = record(7, 10);
    let state = BlobState {
        versions: BTreeMap::from([(
            shard,
            BlobVersion {
                lsn: 3,
                write_epoch: 5,
                record_ref: record,
            },
        )]),
        lifetime: Some(BlobLifetime {
            lsn: 4,
            lifecycle: BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            },
        }),
    };
    let snapshot = BlobCompactionSnapshot {
        materialized_through_lsn: 20,
        emit_garbage_from_lsn: 13,
        epoch_changes: vec![(0, 5), (12, 10)],
        shard_infos: BTreeMap::from([(1, ShardInfo::active(1))]),
        ..BlobCompactionSnapshot::default()
    };

    let (state, garbage) = compact_state(state, snapshot);

    assert!(state.unwrap().versions.is_empty());
    assert!(garbage.is_empty());
}

#[test]
fn snapshot_compaction_retires_mixed_refs_but_silently_prunes_bulk_reclaimed_refs() {
    let dropped = shard(1, 4);
    let mixed = record(7, 10);
    let owned = record(8, 20);
    let state = BlobState {
        versions: BTreeMap::from([
            (
                dropped,
                BlobVersion {
                    lsn: 3,
                    write_epoch: 5,
                    record_ref: mixed,
                },
            ),
            (
                shard(2, 4),
                BlobVersion {
                    lsn: 4,
                    write_epoch: 5,
                    record_ref: owned,
                },
            ),
        ]),
        lifetime: None,
    };
    let second_dropped = shard(2, 4);
    let snapshot = BlobCompactionSnapshot {
        materialized_through_lsn: 20,
        emit_garbage_from_lsn: 8,
        shard_infos: BTreeMap::from([
            (
                1,
                ShardInfo {
                    current_generation: 4,
                    state: ShardState::Dropped,
                },
            ),
            (
                2,
                ShardInfo {
                    current_generation: 4,
                    state: ShardState::Dropped,
                },
            ),
        ]),
        shard_drop_lsns: BTreeMap::from([(dropped, 11), (second_dropped, 12)]),
        reclaimed_shard_segments: BTreeMap::from([(owned.segment_id, second_dropped)]),
        ..BlobCompactionSnapshot::default()
    };

    let (state, garbage) = compact_state(state, snapshot);

    assert!(state.is_none());
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].lsn, 11);
    assert_eq!(garbage[0].event, GarbageEvent::Retired { record: mixed });
}

#[test]
fn snapshot_compaction_silently_prunes_shards_owned_by_the_pre_cutover_path() {
    let pre_cutover_drop = shard(1, 4);
    let legacy_without_job = shard(2, 3);
    let state = BlobState {
        versions: BTreeMap::from([
            (
                pre_cutover_drop,
                BlobVersion {
                    lsn: 3,
                    write_epoch: 5,
                    record_ref: record(7, 10),
                },
            ),
            (
                legacy_without_job,
                BlobVersion {
                    lsn: 4,
                    write_epoch: 5,
                    record_ref: record(8, 20),
                },
            ),
        ]),
        lifetime: None,
    };
    let snapshot = BlobCompactionSnapshot {
        materialized_through_lsn: 20,
        emit_garbage_from_lsn: 12,
        shard_infos: BTreeMap::from([
            (
                1,
                ShardInfo {
                    current_generation: 4,
                    state: ShardState::Dropped,
                },
            ),
            (2, ShardInfo::active(4)),
        ]),
        shard_drop_lsns: BTreeMap::from([(pre_cutover_drop, 11)]),
        ..BlobCompactionSnapshot::default()
    };

    let (state, garbage) = compact_state(state, snapshot);

    assert!(state.is_none());
    assert!(garbage.is_empty());
}
