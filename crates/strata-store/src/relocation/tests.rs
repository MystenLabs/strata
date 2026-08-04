use std::{num::NonZeroU32, sync::Arc};

use strata_core::{BlobKey, RecordRef, ShardKey};
use strata_lsm::{Lsm, LsmOptions, Manifest};
use tempfile::TempDir;

use super::{RelocationEntry, RelocationStore};
use crate::partition::partition_for_key;

fn record(segment_id: u64, offset: u64) -> RecordRef {
    RecordRef {
        segment_id,
        offset,
        len: 17,
    }
}

fn entry(
    key: &[u8],
    shard: ShardKey,
    payload_lsn: u64,
    publish_lsn: u64,
    to: RecordRef,
) -> RelocationEntry {
    RelocationEntry {
        key: BlobKey::new(key.to_vec()).unwrap(),
        shard,
        payload_lsn,
        publish_lsn,
        to,
    }
}

fn open(directory: &TempDir, recovered: Vec<RelocationEntry>) -> RelocationStore {
    open_partitioned(directory, recovered, 1)
}

fn open_partitioned(
    directory: &TempDir,
    recovered: Vec<RelocationEntry>,
    partition_count: u32,
) -> RelocationStore {
    let root = directory.path();
    let manifest = Arc::new(Manifest::empty(
        "relocation-base-v1",
        "relocation-patch-v1",
        NonZeroU32::new(partition_count).unwrap(),
    ));
    let last_lsn = recovered.last().map(|entry| entry.publish_lsn);
    let recovered = recovered
        .iter()
        .map(|entry| {
            (
                entry.publish_lsn,
                RelocationStore::lsm_mutation(partition_count, entry),
            )
        })
        .collect();
    let lsm = Lsm::from_parts(
        root.join("tables"),
        manifest,
        recovered,
        last_lsn,
        LsmOptions::default(),
    )
    .unwrap();
    RelocationStore::new(Arc::new(lsm))
}

fn key_for_partition(partition: u32, partition_count: u32) -> BlobKey {
    (0..)
        .map(|candidate| BlobKey::new(format!("partition-key-{candidate}").into_bytes()).unwrap())
        .find(|key| partition_for_key(key.as_bytes(), partition_count) == partition)
        .unwrap()
}

fn finish(store: RelocationStore) {
    drop(store);
}

#[test]
fn resolves_one_logical_payload() {
    let directory = TempDir::new().unwrap();
    let store = open(&directory, Vec::new());
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();

    assert_eq!(
        store
            .lookup(&BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
            .unwrap()
            .unwrap()
            .to,
        record(7, 20)
    );
    finish(store);
}

#[test]
fn repeated_move_replaces_the_current_location() {
    let directory = TempDir::new().unwrap();
    let store = open(&directory, Vec::new());
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 15, record(12, 60))])
        .unwrap();

    let relocation = store
        .lookup(&BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
        .unwrap()
        .unwrap();
    assert_eq!(relocation.publish_lsn, 15);
    assert_eq!(relocation.to, record(12, 60));
    finish(store);
}

#[test]
fn streams_current_locations_for_the_requested_blob_range() {
    let directory = TempDir::new().unwrap();
    let store = open(&directory, Vec::new());
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(
            0,
            &[
                entry(b"blob-a", shard, 9, 11, record(7, 20)),
                entry(b"blob-b", shard, 10, 12, record(8, 30)),
                entry(b"blob-a", shard, 9, 13, record(9, 40)),
            ],
        )
        .unwrap();

    let mut scan = store.scan(0, b"blob-a", b"blob-a", u64::MAX).unwrap();
    let current = scan.current().unwrap();
    assert_eq!(current.key.as_bytes(), b"blob-a");
    assert_eq!(current.payload_lsn, 9);
    assert_eq!(current.publish_lsn, 13);
    assert_eq!(current.to, record(9, 40));
    scan.advance().unwrap();
    assert!(scan.current().is_none());
    finish(store);
}

#[test]
fn shard_and_payload_lsn_are_part_of_the_key() {
    let directory = TempDir::new().unwrap();
    let store = open(&directory, Vec::new());
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();

    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    assert!(store.lookup(&key, shard, 8).unwrap().is_none());
    assert!(
        store
            .lookup(
                &key,
                ShardKey {
                    id: 5,
                    generation: 2,
                },
                9,
            )
            .unwrap()
            .is_none()
    );
    finish(store);
}

#[test]
fn decoded_store_wal_record_reopens_the_current_location() {
    let directory = TempDir::new().unwrap();
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    let relocation = entry(b"blob", shard, 9, 11, record(7, 20));
    let store = open(&directory, vec![relocation]);
    assert_eq!(
        store
            .lookup(&BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
            .unwrap()
            .unwrap()
            .to,
        record(7, 20)
    );
    finish(store);
}

#[test]
fn direct_l0_publication_writes_one_table_per_touched_partition() {
    let directory = TempDir::new().unwrap();
    let partition_count = 4;
    let store = open_partitioned(&directory, Vec::new(), partition_count);
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    let entries = (0..partition_count)
        .map(|partition| RelocationEntry {
            key: key_for_partition(partition, partition_count),
            shard,
            payload_lsn: 10 + u64::from(partition),
            publish_lsn: 20 + u64::from(partition),
            to: record(100 + u64::from(partition), 0),
        })
        .collect::<Vec<_>>();

    let (_sequence, edit) = store.prepare_l0(&entries).unwrap();
    assert_eq!(edit.add_patches.len(), partition_count as usize);
    assert_eq!(
        edit.add_patches
            .iter()
            .map(|table| table.partition)
            .collect::<Vec<_>>(),
        (0..partition_count).collect::<Vec<_>>()
    );
    assert_eq!(
        edit.add_patches
            .iter()
            .map(|table| table.id)
            .collect::<Vec<_>>(),
        (1..=u64::from(partition_count)).collect::<Vec<_>>()
    );

    let mut published = store.lsm().manifest().as_ref().clone();
    published.apply(&edit).unwrap();
    store.lsm().install_manifest(published).unwrap();
    for entry in &entries {
        assert_eq!(
            store
                .lookup(&entry.key, entry.shard, entry.payload_lsn)
                .unwrap()
                .unwrap()
                .to,
            entry.to
        );
    }
    finish(store);
}
