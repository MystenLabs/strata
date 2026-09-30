use super::*;
use crate::blob_lsm::format::{BlobMutationWithLSN, LifecycleMutation, global_operand_floor};

fn event(event_index: u64, operation: LifecycleMutation) -> BlobMutation {
    BlobMutation::ApplyEvent {
        event_index,
        current_epoch: 5,
        operation,
    }
}

#[test]
fn lifecycle_event_codecs_preserve_zero_and_max_indexes_and_all_generations() {
    let mutations = vec![
        BlobMutationWithLSN {
            lsn: 1,
            mutation: event(
                0,
                LifecycleMutation::SetLifetime {
                    logical_end_epoch: 10,
                },
            ),
        },
        BlobMutationWithLSN {
            lsn: 2,
            mutation: event(
                u64::MAX,
                LifecycleMutation::Tombstone {
                    shards: vec![shard(1, 2), shard(3, 4)],
                },
            ),
        },
    ];
    for mutation in &mutations {
        assert_eq!(
            BlobMutationWithLSN::decode_inline(
                mutation.lsn,
                &mutation.mutation.encode_inline().unwrap()
            )
            .unwrap(),
            vec![mutation.clone()]
        );
    }
    let encoded = BlobMutationWithLSN::encode_batch(&mutations).unwrap();
    assert_eq!(
        BlobMutationWithLSN::decode_inline(2, &encoded).unwrap(),
        mutations
    );
    for prefix in 0..encoded.len() {
        assert!(BlobMutationWithLSN::decode_inline(2, &encoded[..prefix]).is_err());
    }
    for last_event_index in [None, Some(0), Some(u64::MAX)] {
        let state = BlobState {
            last_event_index,
            ..BlobState::default()
        };
        assert_eq!(BlobState::decode(&state.encode().unwrap()).unwrap(), state);
    }
}

#[test]
fn legacy_state_and_patches_decode_without_an_event_marker() {
    // v3 empty base: version, u32 shard count, absent lifetime. No event field existed.
    let legacy = [3, 0, 0, 0, 0, 0];
    assert_eq!(BlobState::decode(&legacy).unwrap(), BlobState::default());
    let mut lifetime = vec![3, 2];
    lifetime.extend_from_slice(&10_u64.to_le_bytes());
    lifetime.extend_from_slice(&5_u64.to_le_bytes());
    assert_eq!(
        BlobMutationWithLSN::decode_inline(1, &lifetime).unwrap(),
        vec![BlobMutationWithLSN {
            lsn: 1,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5
            },
        }]
    );
    // v4 requires the marker field, even when it is absent.
    let mut truncated = legacy;
    truncated[0] = 4;
    assert!(BlobState::decode(&truncated).is_err());
}

#[test]
fn compaction_keeps_replay_marker_after_deleting_the_last_version() {
    let (state, garbage) = merge(&[
        (1, put_patch(shard(1, 0), 5, record(1, 0))),
        (
            2,
            inline_patch(event(
                0,
                LifecycleMutation::Tombstone {
                    shards: vec![shard(1, 0)],
                },
            )),
        ),
    ]);
    assert!(state.versions.is_empty());
    assert_eq!(garbage.len(), 1);
    let (state, garbage) = compact_state(state, BlobCompactionSnapshot::default());
    assert_eq!(state.unwrap().last_event_index, Some(0));
    assert!(garbage.is_empty());
}

#[test]
fn partial_merge_does_not_apply_a_replayed_delete_to_a_newer_put() {
    let base = encode_inline_value(
        &BlobState {
            last_event_index: Some(10),
            ..BlobState::default()
        }
        .encode()
        .unwrap(),
    );
    let patches = vec![
        (20, put_patch(shard(1, 0), 5, record(1, 0))),
        (
            21,
            inline_patch(event(
                10,
                LifecycleMutation::Tombstone {
                    shards: vec![shard(1, 0)],
                },
            )),
        ),
        (
            22,
            inline_patch(event(
                9,
                LifecycleMutation::SetLifetime {
                    logical_end_epoch: 6,
                },
            )),
        ),
    ];
    // Even with epoch 6 visible, neither the replayed delete nor the old lifetime can make
    // this put garbage. Only the base proves those operations have already been applied.
    let (partial, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 30,
            epoch_changes: vec![(0, 5), (23, 6)],
            ..BlobCompactionSnapshot::default()
        },
    );
    assert!(garbage.is_empty());
    let mut garbage = Vec::new();
    let merged = BlobMerge
        .merge(b"blob", Some(&base), &[(22, &partial)], &mut |r| {
            garbage.push(r);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let StoredValue::Inline(bytes) = decode_value(&merged).unwrap() else {
        panic!()
    };
    let state = BlobState::decode(bytes).unwrap();
    assert_eq!(state.last_event_index, Some(10));
    assert_eq!(state.versions[&shard(1, 0)].record_ref, record(1, 0));
    assert!(state.lifetime.is_none());
    assert!(garbage.is_empty());
}

#[test]
fn event_lifetimes_hold_back_the_epoch_compaction_frontier() {
    let lifetime = inline_patch(event(
        3,
        LifecycleMutation::SetLifetime {
            logical_end_epoch: 10,
        },
    ));
    let delete = inline_patch(event(
        4,
        LifecycleMutation::Tombstone {
            shards: vec![shard(1, 0)],
        },
    ));
    assert_eq!(
        global_operand_floor(&[(7, &lifetime), (8, &delete)]).unwrap(),
        Some(7)
    );
    let put = put_patch(shard(2, 0), 5, record(1, 0));
    assert_eq!(
        global_operand_floor(&[(6, &put), (7, &lifetime)]).unwrap(),
        Some(7)
    );
    assert_eq!(
        global_operand_floor(&[(7, &lifetime), (8, &put)]).unwrap(),
        Some(7)
    );
    assert_eq!(global_operand_floor(&[(8, &delete)]).unwrap(), None);
}
