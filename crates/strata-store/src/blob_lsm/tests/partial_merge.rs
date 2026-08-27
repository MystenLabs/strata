//! Patch-reduction semantics: partial merges must stay terminally equivalent to full merges.

use std::collections::BTreeMap;

use strata_core::{BlobLifecycle, GarbageEvent};
use strata_lsm::{MergeOperator, StoredValue, decode_value, encode_inline_value};

use crate::blob_lsm::format::BlobMutationWithLSN;
use crate::blob_lsm::merge::decode_patches;
use crate::blob_lsm::{
    BlobCompactionSnapshot, BlobLifetime, BlobMerge, BlobMergeWithRelocations, BlobMutation,
    BlobState, BlobVersion,
};

use super::{
    aggregate_garbage_deltas, assert_functionally_equivalent_garbage, inline_patch, merge,
    partial_merge_with_snapshot, put_patch, record, shard,
};

#[test]
fn partial_merge_batches_operations_without_changing_full_merge() {
    let shard_a = shard(1, 1);
    let shard_b = shard(2, 1);
    let first_a = record(1, 10);
    let first_b = record(2, 20);
    let second_a = record(3, 30);
    let final_a = record(4, 40);
    let patches = [
        (1, put_patch(shard_a, 5, first_a)),
        (2, put_patch(shard_b, 5, first_b)),
        (
            3,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (4, put_patch(shard_a, 6, second_a)),
        (5, inline_patch(BlobMutation::Tombstone { shard: shard_b })),
        (
            6,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 20,
                current_epoch: 10,
            }),
        ),
        (7, put_patch(shard_a, 10, final_a)),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut partial_garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            partial_garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();

    let direct = merge(&patches);
    let (from_batch_state, mut from_batch_garbage) = merge(&[(7, batched)]);
    from_batch_garbage.extend(partial_garbage);

    assert_eq!(from_batch_state, direct.0);
    assert_functionally_equivalent_garbage(&from_batch_garbage, &direct.1);
}

#[test]
fn partial_merge_preserves_lifetimes_without_expiring_puts() {
    let shard = shard(1, 1);
    let record = record(2, 20);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
        (
            3,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 20,
                current_epoch: 9,
            }),
        ),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut garbage = Vec::new();
    let batch = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert!(garbage.is_empty());
    let decoded = decode_patches(&[(3, batch.as_slice())]).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        decoded[0].mutation,
        BlobMutation::SetLifetime {
            logical_end_epoch: 10,
            current_epoch: 5,
        }
    );
    assert!(matches!(
        decoded[1].mutation,
        BlobMutation::Put { record_ref, .. } if record_ref == record
    ));
    assert_eq!(
        decoded[2].mutation,
        BlobMutation::SetLifetime {
            logical_end_epoch: 20,
            current_epoch: 9,
        }
    );
}

#[test]
fn partial_merge_publishes_patch_local_lifetime_for_surviving_put() {
    let shard = shard(1, 1);
    let record = record(2, 20);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
    ];

    let (batched, partial_garbage) =
        partial_merge_with_snapshot(&patches, BlobCompactionSnapshot::default());

    assert_eq!(partial_garbage.len(), 1);
    assert_eq!(partial_garbage[0].lsn, 1);
    assert_eq!(
        partial_garbage[0].event,
        GarbageEvent::SetLifecycle {
            record,
            lifecycle: Some(BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            }),
        }
    );

    let direct = merge(&patches);
    let (from_batch_state, mut from_batch_garbage) = merge(&[(2, batched)]);
    from_batch_garbage.extend(partial_garbage);
    assert_eq!(from_batch_state, direct.0);
    assert_functionally_equivalent_garbage(&from_batch_garbage, &direct.1);
}

#[test]
fn partial_merge_expires_patch_puts_before_and_after_a_lifetime() {
    let shard_a = shard(1, 1);
    let shard_b = shard(2, 1);
    let before = record(1, 10);
    let after = record(2, 20);
    let patches = [
        (1, put_patch(shard_a, 5, before)),
        (
            2,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (3, put_patch(shard_b, 5, after)),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 4,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (4, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            },
        }]
    );
    for record in [before, after] {
        assert!(garbage.iter().any(|garbage| {
            garbage.lsn == 4 && garbage.event == GarbageEvent::Expired { record }
        }));
    }
}

#[test]
fn partial_merge_starts_a_new_bucket_after_lifetime_expiry() {
    let shard = shard(1, 1);
    let expired = record(1, 10);
    let surviving = record(2, 20);
    let patches = [
        (1, put_patch(shard, 5, expired)),
        (
            2,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 8,
                current_epoch: 5,
            }),
        ),
        (
            4,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 8,
            }),
        ),
        (5, put_patch(shard, 8, surviving)),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 5,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (3, 8)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(5, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 3);
    assert!(matches!(
        decoded[0],
        BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 8,
                ..
            },
        }
    ));
    assert!(matches!(
        decoded[1],
        BlobMutationWithLSN {
            lsn: 4,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                ..
            },
        }
    ));
    assert!(matches!(
        decoded[2],
        BlobMutationWithLSN {
            lsn: 5,
            mutation: BlobMutation::Put { record_ref, .. },
        } if record_ref == surviving
    ));
    assert!(garbage.iter().any(|record| {
        record.lsn == 3 && record.event == GarbageEvent::Expired { record: expired }
    }));
    assert!(garbage.iter().any(|record| {
        record.lsn == 4
            && record.event
                == GarbageEvent::SetLifecycle {
                    record: surviving,
                    lifecycle: Some(BlobLifecycle {
                        logical_end_epoch: 10,
                        extension_count: 0,
                    }),
                }
    }));
}

#[test]
fn partial_merge_epoch_expiry_drops_a_tombstone_bucket() {
    let shard = shard(1, 1);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 8,
                current_epoch: 5,
            }),
        ),
        (2, inline_patch(BlobMutation::Tombstone { shard })),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 3,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (3, 8)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![BlobMutationWithLSN {
            lsn: 1,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 8,
                current_epoch: 5,
            },
        }]
    );
    assert!(garbage.is_empty());
}

#[test]
fn partial_merge_lifetime_survives_a_tombstone_for_a_future_put() {
    let shard = shard(1, 1);
    let first = record(1, 10);
    let second = record(2, 20);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, first)),
        (3, inline_patch(BlobMutation::Tombstone { shard })),
        (4, put_patch(shard, 5, second)),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 5,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (5, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(4, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![BlobMutationWithLSN {
            lsn: 1,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            },
        }]
    );
    assert!(garbage.iter().any(|garbage| {
        garbage.lsn == 3 && garbage.event == GarbageEvent::Retired { record: first }
    }));
    assert!(garbage.iter().any(|garbage| {
        garbage.lsn == 5 && garbage.event == GarbageEvent::Expired { record: second }
    }));
}

#[test]
fn partial_merge_later_lifetime_prevents_the_old_epoch_from_expiring_a_put() {
    let shard = shard(1, 1);
    let record = record(1, 10);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
        (
            3,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 20,
                current_epoch: 9,
            }),
        ),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 4,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (4, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 3);
    assert!(matches!(
        decoded[1],
        BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::Put { record_ref, .. },
        } if record_ref == record
    ));
    assert!(garbage.is_empty());
}

#[test]
fn partial_merge_does_not_expire_beyond_the_materialized_frontier() {
    let shard = shard(1, 1);
    let record = record(1, 10);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 4,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (5, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 2);
    assert!(matches!(
        decoded[1],
        BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::Put { record_ref, .. },
        } if record_ref == record
    ));
    assert!(
        garbage
            .iter()
            .all(|record| !matches!(record.event, GarbageEvent::Expired { .. }))
    );
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].lsn, 1);
    assert_eq!(
        garbage[0].event,
        GarbageEvent::SetLifecycle {
            record,
            lifecycle: Some(BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            }),
        }
    );
}

#[test]
fn partial_merge_does_not_expire_a_put_with_only_an_unknown_base_lifetime() {
    let shard = shard(1, 1);
    let record = record(1, 10);
    let patch = put_patch(shard, 5, record);
    let patches = [(2, patch.as_slice())];
    let merge = BlobMergeWithRelocations::new(
        None,
        BlobCompactionSnapshot {
            materialized_through_lsn: 5,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (5, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let mut garbage = Vec::new();

    let merged = merge
        .partial_merge(b"blob", &patches, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap();

    assert!(merged.is_none());
    assert!(garbage.is_empty());
}

#[test]
fn partial_merge_silently_prunes_expiry_before_the_materialization_cutover() {
    let shard = shard(1, 1);
    let record = record(1, 10);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 3,
            emit_garbage_from_lsn: 4,
            epoch_changes: vec![(0, 5), (3, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![BlobMutationWithLSN {
            lsn: 1,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            },
        }]
    );
    assert!(garbage.is_empty());
}

#[test]
fn partial_merge_expiry_precedes_a_later_tombstone() {
    let shard = shard(1, 1);
    let record = record(1, 10);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
        (4, inline_patch(BlobMutation::Tombstone { shard })),
    ];
    let (batched, garbage) = partial_merge_with_snapshot(
        &patches,
        BlobCompactionSnapshot {
            materialized_through_lsn: 4,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (3, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let decoded = decode_patches(&[(4, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![
            BlobMutationWithLSN {
                lsn: 1,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            },
            BlobMutationWithLSN {
                lsn: 4,
                mutation: BlobMutation::Tombstone { shard },
            },
        ]
    );
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].lsn, 3);
    assert_eq!(garbage[0].event, GarbageEvent::Expired { record });
}

#[test]
fn partial_merge_reduces_shard_history_after_a_lifetime_barrier() {
    let shard = shard(1, 1);
    let record = record(2, 20);
    let patches = [
        (
            1,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (2, put_patch(shard, 5, record)),
        (3, inline_patch(BlobMutation::Tombstone { shard })),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut partial_garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            partial_garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded[0],
        BlobMutationWithLSN {
            lsn: 1,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            },
        }
    );
    assert_eq!(
        decoded[1],
        BlobMutationWithLSN {
            lsn: 3,
            mutation: BlobMutation::Tombstone { shard },
        }
    );
    assert_eq!(partial_garbage.len(), 1);
    assert_eq!(partial_garbage[0].lsn, 3);
    assert_eq!(partial_garbage[0].event, GarbageEvent::Retired { record });

    let direct = merge(&patches);
    let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
    from_batch_garbage.extend(partial_garbage);

    assert_eq!(from_batch_state, direct.0);
    assert_eq!(
        aggregate_garbage_deltas(&from_batch_garbage),
        aggregate_garbage_deltas(&direct.1)
    );
}

#[test]
fn partial_merge_terminal_event_may_subsume_expiry_from_a_base_lifetime() {
    let shard = shard(1, 1);
    let base_record = record(1, 10);
    let patch_record = record(2, 20);
    let base_state = BlobState {
        versions: BTreeMap::from([(
            shard,
            BlobVersion {
                lsn: 2,
                write_epoch: 5,
                record_ref: base_record,
            },
        )]),
        lifetime: Some(BlobLifetime {
            lsn: 3,
            lifecycle: BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 0,
            },
        }),
    };
    let patches = [
        (4, put_patch(shard, 5, patch_record)),
        (6, inline_patch(BlobMutation::Tombstone { shard })),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let decoded = decode_patches(&[(6, batched.as_slice())]).unwrap();

    assert_eq!(
        decoded,
        vec![BlobMutationWithLSN {
            lsn: 6,
            mutation: BlobMutation::Tombstone { shard },
        }]
    );

    // Epoch 10 is reached between the patch-local put and tombstone. The partial merger
    // cannot see either this transition or the lifetime in the base, so the tombstone
    // terminally accounts for the patch record as Retired rather than Expired.
    assert_eq!(
        garbage
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    GarbageEvent::Retired { record } | GarbageEvent::Expired { record }
                        if record == patch_record
                )
            })
            .count(),
        1
    );

    let base = encode_inline_value(&base_state.encode().unwrap());
    let patches = [(6, batched.as_slice())];
    let merge = BlobMergeWithRelocations::new(
        None,
        BlobCompactionSnapshot {
            materialized_through_lsn: 6,
            emit_garbage_from_lsn: 0,
            epoch_changes: vec![(0, 5), (5, 10)],
            ..BlobCompactionSnapshot::default()
        },
    );
    let encoded = merge
        .merge(b"blob", Some(&base), &patches, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
        panic!("blob compaction must materialize inline state");
    };
    let state = BlobState::decode(bytes).unwrap();

    assert!(state.resolve(shard, 10).is_none());
    assert_eq!(
        garbage
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    GarbageEvent::Retired { record } | GarbageEvent::Expired { record }
                        if record == patch_record
                )
            })
            .count(),
        1
    );
}

#[test]
fn partial_merge_reduces_terminal_puts_across_a_retained_lifetime() {
    let shard = shard(1, 1);
    let record = record(2, 20);
    let patches = [
        (1, put_patch(shard, 5, record)),
        (
            2,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (3, inline_patch(BlobMutation::Tombstone { shard })),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut partial_garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            partial_garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded[0],
        BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            },
        }
    );
    assert_eq!(
        decoded[1],
        BlobMutationWithLSN {
            lsn: 3,
            mutation: BlobMutation::Tombstone { shard },
        }
    );
    assert_eq!(partial_garbage.len(), 1);
    assert_eq!(partial_garbage[0].lsn, 3);
    assert_eq!(partial_garbage[0].event, GarbageEvent::Retired { record });

    let direct = merge(&patches);
    let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
    from_batch_garbage.extend(partial_garbage);

    assert_eq!(from_batch_state, direct.0);
    assert_eq!(
        aggregate_garbage_deltas(&from_batch_garbage),
        aggregate_garbage_deltas(&direct.1)
    );
}

#[test]
fn partial_merge_reduces_shard_history_and_emits_terminal_garbage() {
    let shard = shard(1, 1);
    let first = record(1, 10);
    let second = record(2, 20);
    let third = record(3, 30);
    let patches = [
        (1, put_patch(shard, 5, first)),
        (2, put_patch(shard, 6, second)),
        (3, put_patch(shard, 7, third)),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut partial_garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            partial_garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

    assert_eq!(decoded.len(), 1);
    assert!(matches!(
        decoded[0],
        BlobMutationWithLSN {
            lsn: 3,
            mutation: BlobMutation::Put { record_ref, .. },
        } if record_ref == third
    ));

    let direct = merge(&patches);
    let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
    from_batch_garbage.extend(partial_garbage);

    assert_eq!(from_batch_state, direct.0);
    assert_eq!(from_batch_garbage, direct.1);
}

#[test]
fn partial_merge_final_put_retires_the_unknown_base_without_a_synthetic_tombstone() {
    let shard = shard(1, 1);
    let base_record = record(1, 10);
    let first_patch_record = record(2, 20);
    let final_patch_record = record(3, 30);
    let base_state = BlobState {
        versions: BTreeMap::from([(
            shard,
            BlobVersion {
                lsn: 0,
                write_epoch: 5,
                record_ref: base_record,
            },
        )]),
        lifetime: None,
    };
    let patches = [
        (1, put_patch(shard, 5, first_patch_record)),
        (2, put_patch(shard, 5, final_patch_record)),
    ];
    let borrowed = patches
        .iter()
        .map(|(lsn, value)| (*lsn, value.as_slice()))
        .collect::<Vec<_>>();
    let mut garbage = Vec::new();
    let batched = BlobMerge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

    assert!(matches!(
        decoded.as_slice(),
        [BlobMutationWithLSN {
            lsn: 2,
            mutation: BlobMutation::Put { record_ref, .. },
        }] if *record_ref == final_patch_record
    ));

    let base = encode_inline_value(&base_state.encode().unwrap());
    let patches = [(2, batched.as_slice())];
    let encoded = BlobMerge
        .merge(b"blob", Some(&base), &patches, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
        panic!("blob merge must materialize inline state");
    };
    let state = BlobState::decode(bytes).unwrap();

    let version = state.resolve(shard, 5).unwrap().0;
    assert_eq!(version.lsn, 2);
    assert_eq!(version.record_ref, final_patch_record);
    assert!(garbage.iter().any(|record| {
        record.lsn == 2
            && record.event
                == GarbageEvent::Retired {
                    record: first_patch_record,
                }
    }));
    assert!(garbage.iter().any(|record| {
        record.lsn == 2
            && record.event
                == GarbageEvent::Retired {
                    record: base_record,
                }
    }));
}
