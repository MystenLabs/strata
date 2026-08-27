//! Full-merge state-machine semantics: overwrites, tombstones, and read visibility.

use core_types::{BlobLifecycle, GarbageEvent};

use crate::blob_lsm::BlobMutation;

use super::{inline_patch, merge, put_patch, record, shard};

#[test]
fn merge_keeps_one_version_per_shard_and_retires_overwrites() {
    let first = record(1, 10);
    let other = record(2, 20);
    let replacement = record(3, 30);
    let shard_a = shard(1, 1);
    let shard_b = shard(1, 2);
    let patches = [
        (1, put_patch(shard_a, 5, first)),
        (2, put_patch(shard_b, 5, other)),
        (3, put_patch(shard_a, 6, replacement)),
    ];

    let (state, garbage) = merge(&patches);

    assert_eq!(state.resolve(shard_a, 6).unwrap().0.record_ref, replacement);
    assert_eq!(state.resolve(shard_b, 6).unwrap().0.record_ref, other);
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].event, GarbageEvent::Retired { record: first });
    assert_eq!(garbage[0].lsn, 3);
}

#[test]
fn tombstone_only_retires_its_shard() {
    let shard_a = shard(1, 1);
    let shard_b = shard(2, 1);
    let record_a = record(1, 10);
    let record_b = record(2, 20);
    let patches = [
        (1, put_patch(shard_a, 5, record_a)),
        (2, put_patch(shard_b, 5, record_b)),
        (3, inline_patch(BlobMutation::Tombstone { shard: shard_a })),
    ];

    let (state, garbage) = merge(&patches);

    assert!(state.resolve(shard_a, 5).is_none());
    assert_eq!(state.resolve(shard_b, 5).unwrap().0.record_ref, record_b);
    assert_eq!(garbage.len(), 1);
    assert_eq!(garbage[0].event, GarbageEvent::Retired { record: record_a });
    assert_eq!(garbage[0].lsn, 3);
}

#[test]
fn lifetime_expiry_and_tombstone_match_store_visibility() {
    let shard = shard(1, 1);
    let first = record(1, 10);
    let after_expiry = record(2, 20);
    let patches = [
        (1, put_patch(shard, 5, first)),
        (
            2,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }),
        ),
        (
            3,
            inline_patch(BlobMutation::SetLifetime {
                logical_end_epoch: 20,
                current_epoch: 10,
            }),
        ),
        (4, put_patch(shard, 10, after_expiry)),
    ];
    let (state, garbage) = merge(&patches);

    let (version, lifecycle) = state.resolve(shard, 10).unwrap();
    assert_eq!(version.record_ref, after_expiry);
    assert_eq!(lifecycle.unwrap().logical_end_epoch, 20);
    assert!(
        garbage
            .iter()
            .any(|record| record.event == GarbageEvent::Expired { record: first })
    );
    assert!(garbage.iter().any(|record| {
        record.event
            == GarbageEvent::SetLifecycle {
                record: after_expiry,
                lifecycle: Some(BlobLifecycle {
                    logical_end_epoch: 20,
                    extension_count: 0,
                }),
            }
    }));

    let tombstone = (5, inline_patch(BlobMutation::Tombstone { shard }));
    let mut all = patches.to_vec();
    all.push(tombstone);
    let (state, garbage) = merge(&all);
    assert!(state.resolve(shard, 10).is_none());
    assert!(garbage.iter().any(|record| record.event
        == GarbageEvent::Retired {
            record: after_expiry
        }));
}
