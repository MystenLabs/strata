use std::{num::NonZeroU32, sync::Arc, thread};

use strata_core::{BlobKey, PlacementClass, RecordRef, ShardKey};
use strata_lsm::segment::{SegmentFactory, SegmentIdAllocator, SegmentWriter, segment_path};
use strata_lsm::{Lsm, LsmCheckpoint, LsmOptions, Manifest, Wal, WalPosition, file_sync_channel};
use strata_relocation::{RelocationEntry, RelocationStore};
use tempfile::TempDir;

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

fn open(
    directory: &TempDir,
    checkpoint: Option<LsmCheckpoint>,
) -> (RelocationStore, Vec<thread::JoinHandle<()>>) {
    let root = directory.path();
    let segment_dir = root.join("segment");
    std::fs::create_dir_all(&segment_dir).unwrap();
    let segment_path = segment_path(&segment_dir, 1);
    let segment = if segment_path.exists() {
        SegmentWriter::open_existing(&segment_path, 1, PlacementClass::Ingest, 1 << 20).unwrap()
    } else {
        SegmentWriter::create(&segment_path, 1, PlacementClass::Ingest, 1 << 20).unwrap()
    };
    let (sync_tx, syncer) = file_sync_channel(8);
    let worker = thread::spawn(move || syncer.run());
    let manifest = Arc::new(Manifest::empty(
        "relocation-base-v1",
        "relocation-patch-v1",
        NonZeroU32::MIN,
    ));
    let last_lsn = checkpoint.and_then(|checkpoint| checkpoint.durable_lsn);
    let wal = Wal::recover(
        root.join("wal"),
        1 << 20,
        checkpoint.map_or(WalPosition::default(), |checkpoint| checkpoint.wal_position),
        last_lsn,
        None,
        1,
        last_lsn,
        sync_tx,
    )
    .unwrap();
    let lsm = Lsm::from_parts(
        root.join("tables"),
        manifest,
        segment,
        SegmentFactory::new(
            segment_dir,
            SegmentIdAllocator::new(2),
            PlacementClass::Ingest,
            1 << 20,
        ),
        wal,
        last_lsn,
        LsmOptions::default(),
    )
    .unwrap();
    (RelocationStore::new(Arc::new(lsm)), vec![worker])
}

fn finish(store: RelocationStore, workers: Vec<thread::JoinHandle<()>>) {
    drop(store);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn resolves_one_logical_payload() {
    let directory = TempDir::new().unwrap();
    let (store, workers) = open(&directory, None);
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();

    assert_eq!(
        store
            .lookup(0, &BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
            .unwrap()
            .unwrap()
            .to,
        record(7, 20)
    );
    finish(store, workers);
}

#[test]
fn repeated_move_replaces_the_current_location() {
    let directory = TempDir::new().unwrap();
    let (store, workers) = open(&directory, None);
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
        .lookup(0, &BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
        .unwrap()
        .unwrap();
    assert_eq!(relocation.publish_lsn, 15);
    assert_eq!(relocation.to, record(12, 60));
    finish(store, workers);
}

#[test]
fn streams_current_locations_for_the_requested_blob_range() {
    let directory = TempDir::new().unwrap();
    let (store, workers) = open(&directory, None);
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
    finish(store, workers);
}

#[test]
fn shard_and_payload_lsn_are_part_of_the_key() {
    let directory = TempDir::new().unwrap();
    let (store, workers) = open(&directory, None);
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();

    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    assert!(store.lookup(0, &key, shard, 8).unwrap().is_none());
    assert!(
        store
            .lookup(
                0,
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
    finish(store, workers);
}

#[test]
fn synced_wal_reopens_the_current_location() {
    let directory = TempDir::new().unwrap();
    let shard = ShardKey {
        id: 4,
        generation: 2,
    };
    let (store, workers) = open(&directory, None);
    store
        .write_batch(0, &[entry(b"blob", shard, 9, 11, record(7, 20))])
        .unwrap();
    let checkpoint = store.lsm().sync().unwrap();
    finish(store, workers);

    let (store, workers) = open(&directory, Some(checkpoint));
    assert_eq!(
        store
            .lookup(0, &BlobKey::new(b"blob".to_vec()).unwrap(), shard, 9)
            .unwrap()
            .unwrap()
            .to,
        record(7, 20)
    );
    finish(store, workers);
}
