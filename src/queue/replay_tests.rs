use super::*;
use crate::{BlobKey, StrataStore, StrataStoreConfig, StrataStoreMetrics};
use index::{StrataIndex, port::RocksBackend};
use std::{fs::OpenOptions, path::Path, time::Duration};
use tempfile::tempdir;

pub(super) fn open_store(root: &Path) -> (StrataStore, PendingQueue) {
    let mut cfg = StrataStoreConfig::new(root, "test");
    cfg.starting_epoch = 42;
    cfg.gc_workers_enabled = false;
    cfg.lsm_memtable_max_age = Duration::from_secs(3600);
    let options = cf_options(rocksdb::Options::default())
        .into_iter()
        .map(|(name, options)| (name.to_owned(), options))
        .chain(index::cf_options_for_prefix(&cfg.index_cf_prefix()))
        .collect::<Vec<_>>();
    let db = Arc::new(RocksBackend::open(root.join("db"), None, &options).unwrap());
    let index = StrataIndex::from_db(db.clone(), cfg.index_cf_prefix()).unwrap();
    let store = StrataStore::from_index(cfg, index, StrataStoreMetrics::default()).unwrap();
    (store, PendingQueue::new(db))
}

pub(super) fn key(name: &[u8]) -> BlobKey {
    BlobKey::new(name.to_vec()).unwrap()
}

fn delete() -> BlobOperation {
    BlobOperation::Delete {
        shards: vec![ShardGeneration {
            shard: 7,
            generation: 0,
        }],
        cancellable: true,
    }
}

#[tokio::test]
async fn recovered_delete_is_acknowledged_without_resubmission() -> Result<()> {
    let dir = tempdir().unwrap();
    let physical_keys = [key(b"primary"), key(b"secondary")];
    let submitted_lsn;
    {
        let (store, queue) = open_store(dir.path());
        store.add_shard(7)?;
        let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
        for key in &physical_keys {
            store.put(7, key, b"old")?;
        }
        store.sync()?;
        queue.write_batch(|batch| batch.append(b"blob", 100, delete(), vec![]))?;
        drop(queue.durable_snapshot()?);
        let mut batch = store.batch();
        for key in &physical_keys {
            batch.tombstone(7, key.clone());
        }
        submitted_lsn = queue.submit_blob_strata(&guard, b"blob", 100, batch)?;
        assert!(store.published_lsn()? < submitted_lsn);
        assert!(
            queue
                .acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 100)])
                .is_err()
        );
        // A RocksDB checkpoint can be visible before its sync completes. The queue must use
        // the store's successful-durability notification rather than trusting that visible row.
        let published = store.published_lsn()?;
        let mut metadata = store.index().batch();
        store
            .index()
            .put_commit_lsn_batch(&mut metadata, submitted_lsn)?;
        metadata.write()?;
        assert!(
            queue
                .acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 100)])
                .is_err()
        );
        let mut metadata = store.index().batch();
        store
            .index()
            .put_commit_lsn_batch(&mut metadata, published)?;
        metadata.write()?;
        store.sync()?;
        // Crash before acknowledging the queue row. Both slivers and their recorded LSN survive.
    }
    {
        let (store, queue) = open_store(dir.path());
        let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
        let before = store.index().get_next_lsn()?;
        let mut batch = store.batch();
        for key in &physical_keys {
            batch.tombstone(7, key.clone());
        }
        assert_eq!(
            queue.submit_blob_strata(&guard, b"blob", 100, batch)?,
            submitted_lsn
        );
        assert_eq!(store.index().get_next_lsn()?, before);
        for key in &physical_keys {
            assert_eq!(store.get_from_shard(7, key)?, None);
        }
        queue.acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 100)])?;
        assert!(store.index().submitted_batch_lsns().is_empty()?);
        // New data is admitted only after the acknowledgement is durably committed.
        for key in &physical_keys {
            store.put(7, key, b"new")?;
        }
        store.sync()?;
    }
    let (store, queue) = open_store(dir.path());
    let snapshot = queue.durable_snapshot()?;
    assert!(
        queue
            .blobs(snapshot.as_ref())?
            .next()
            .unwrap()?
            .1
            .commands()
            .is_empty()
    );
    for key in &physical_keys {
        assert_eq!(store.get_from_shard(7, key)?, Some(b"new".to_vec()));
    }
    Ok(())
}

#[tokio::test]
async fn queue_retries_lost_delete_after_lsn_reuse_and_repeated_crashes() -> Result<()> {
    let dir = tempdir().unwrap();
    let blob = key(b"blob");
    {
        let (store, queue) = open_store(dir.path());
        store.add_shard(7)?;
        let _guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
        store.put(7, &blob, b"old")?;
        store.sync()?;
        queue.write_batch(|b| b.append(b"blob", 100, delete(), vec![]))?;
        drop(queue.durable_snapshot()?);
    }
    for _ in 0..2 {
        let (checkpoint, lost_lsn, wal_dir) = {
            let (store, queue) = open_store(dir.path());
            let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
            let checkpoint = store.index().get_store_checkpoint()?.unwrap();
            let mut batch = store.batch();
            batch.tombstone(7, blob.clone());
            let lsn = queue.submit_blob_strata(&guard, b"blob", 100, batch)?;
            store.index().flush_wal(true)?;
            assert!(store.published_lsn()? < lsn);
            (checkpoint, lsn, store.config().namespace_dir().join("wal"))
        };
        let wal = wal_dir.join(format!("wal-{:020}.log", checkpoint.wal_position.log_id));
        OpenOptions::new()
            .write(true)
            .open(wal)
            .unwrap()
            .set_len(checkpoint.wal_position.offset)
            .unwrap();
        {
            let (store, _) = open_store(dir.path());
            assert!(store.index().submitted_batch_lsns().is_empty()?);
            assert_eq!(store.get_from_shard(7, &blob)?, Some(b"old".to_vec()));
            assert_eq!(store.put(7, &key(b"unrelated"), b"value")?, lost_lsn);
            store.sync()?;
            // Crash again after unrelated writes pass the discarded delete's old LSN.
        }
    }
    let (store, queue) = open_store(dir.path());
    let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
    let before = store.index().get_next_lsn()?;
    let mut batch = store.batch();
    batch.tombstone(7, blob.clone());
    assert_eq!(
        queue.submit_blob_strata(&guard, b"blob", 100, batch)?,
        before
    );
    assert_eq!(store.index().get_next_lsn()?, before + 1);
    store.sync()?;
    queue.acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 100)])?;
    assert_eq!(store.get_from_shard(7, &blob)?, None);
    Ok(())
}

#[tokio::test]
async fn lifetime_retry_does_not_reapply_and_acknowledgement_preserves_newer_work() -> Result<()> {
    let dir = tempdir().unwrap();
    let blob = key(b"blob");
    let first_lsn;
    {
        let (store, queue) = open_store(dir.path());
        store.add_shard(7)?;
        let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
        store.set_blob_lifetime(&blob, 45)?;
        store.put(7, &blob, b"value")?;
        store.sync()?;
        queue.write_batch(|b| {
            b.append(
                b"blob",
                0,
                BlobOperation::SetLifetime { end_epoch: 50 },
                vec![],
            )
        })?;
        drop(queue.durable_snapshot()?);
        let mut batch = store.batch();
        batch.set_blob_lifetime(blob.clone(), 50);
        first_lsn = queue.submit_blob_strata(&guard, b"blob", 0, batch)?;
        store.sync()?;
    }
    let (store, queue) = open_store(dir.path());
    let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
    let before = store.index().get_next_lsn()?;
    let mut batch = store.batch();
    batch.set_blob_lifetime(blob.clone(), 50);
    assert_eq!(
        queue.submit_blob_strata(&guard, b"blob", 0, batch)?,
        first_lsn
    );
    assert_eq!(store.index().get_next_lsn()?, before);
    queue.write_batch(|b| {
        b.append(
            b"blob",
            u64::MAX,
            BlobOperation::SetLifetime { end_epoch: 60 },
            vec![],
        )
    })?;
    queue.acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 0)])?;
    let snapshot = queue.durable_snapshot()?;
    let row = queue.blobs(snapshot.as_ref())?.next().unwrap()?.1;
    assert_eq!(row.commands().len(), 1);
    assert_eq!(row.commands()[0].event_index, u64::MAX);
    let mut batch = store.batch();
    batch.set_blob_lifetime(blob.clone(), 60);
    queue.submit_blob_strata(&guard, b"blob", u64::MAX, batch)?;
    store.sync()?;
    queue.acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", u64::MAX)])?;
    let mut batch = store.batch();
    batch.advance_epoch_to(51);
    batch.write()?;
    assert_eq!(store.get_from_shard(7, &blob)?, Some(b"value".to_vec()));
    Ok(())
}

#[tokio::test]
async fn cancelled_out_of_order_failed_and_cross_store_submissions_do_not_apply() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let guard = queue.lock_blobs(&[b"blob"]).await.unwrap();
    queue.write_batch(|b| {
        b.append(b"blob", 1, delete(), vec![])?;
        b.register(b"blob", 2, 50, vec![])?;
        b.append(b"blob", 3, delete(), vec![])
    })?;
    drop(queue.durable_snapshot()?);
    let before = store.index().get_next_lsn()?;
    for event in [1, 3] {
        let mut batch = store.batch();
        batch.set_blob_lifetime(key(b"blob"), 50);
        assert!(matches!(
            queue.submit_blob_strata(&guard, b"blob", event, batch),
            Err(Error::InvalidPendingOperation(_))
        ));
    }
    assert!(matches!(
        queue.submit_blob_strata(&guard, b"blob", 2, store.batch()),
        Err(Error::Store(store::Error::EmptyTrackedBatch))
    ));
    let mut batch = store.batch();
    batch.set_blob_lifetime(key(b"blob"), 42);
    assert!(matches!(
        queue.submit_blob_strata(&guard, b"blob", 2, batch),
        Err(Error::Store(store::Error::InvalidBlobLifetime { .. }))
    ));
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    assert_eq!(store.index().get_next_lsn()?, before);
    assert!(
        queue
            .acknowledge_blobs_rocksdb(&guard, &store, &[(b"blob", 2)])
            .is_err()
    );
    let other_dir = tempdir().unwrap();
    let (other, _) = open_store(other_dir.path());
    let other_before = other.index().get_next_lsn()?;
    let mut batch = other.batch();
    batch.set_blob_lifetime(key(b"blob"), 50);
    assert!(matches!(
        queue.submit_blob_strata(&guard, b"blob", 2, batch),
        Err(Error::InvalidPendingOperation(_))
    ));
    assert!(other.index().submitted_batch_lsns().is_empty()?);
    assert_eq!(other.index().get_next_lsn()?, other_before);
    Ok(())
}

#[tokio::test]
async fn blob_and_epoch_records_with_the_same_event_index_are_independent() -> Result<()> {
    let dir = tempdir().unwrap();
    let epoch_lsn;
    {
        let (store, queue) = open_store(dir.path());
        let guard = queue.lock_blobs(&[b"a", b"b"]).await.unwrap();
        queue.write_batch(|b| {
            b.register(b"a", 0, 50, vec![])?;
            b.register(b"b", 0, 50, vec![])?;
            b.advance_epoch(0, 45, vec![])
        })?;
        drop(queue.durable_snapshot()?);
        let mut lsns = Vec::new();
        for name in [b"a", b"b"] {
            let mut batch = store.batch();
            batch.set_blob_lifetime(key(name), 50);
            lsns.push(queue.submit_blob_strata(&guard, name, 0, batch)?);
            if name == b"a" {
                store.sync()?;
            }
        }
        assert_ne!(lsns[0], lsns[1]);
        // A is durable, B is not: the whole acknowledgement must abort, retaining A too.
        assert!(
            queue
                .acknowledge_blobs_rocksdb(&guard, &store, &[(b"a", 0), (b"b", 0)])
                .is_err()
        );
        assert_eq!(
            queue.blobs.get(&b"a".to_vec())?.unwrap().commands().len(),
            1
        );
        let before = store.index().get_next_lsn()?;
        let mut batch = store.batch();
        batch.set_blob_lifetime(key(b"a"), 50);
        assert_eq!(queue.submit_blob_strata(&guard, b"a", 0, batch)?, lsns[0]);
        assert_eq!(store.index().get_next_lsn()?, before);
        store.sync()?;
        queue.acknowledge_blobs_rocksdb(&guard, &store, &[(b"a", 0), (b"b", 0)])?;
        drop(guard);
        let lifecycle = queue.lock_lifecycle().await.unwrap();
        epoch_lsn = queue.submit_epoch_strata(&lifecycle, &store, 0)?;
        assert!(epoch_lsn > lsns[1]);
        assert!(
            queue
                .acknowledge_epoch_rocksdb(&lifecycle, &store, 0)
                .is_err()
        );
        store.sync()?;
    }
    let (store, queue) = open_store(dir.path());
    let lifecycle = queue.lock_lifecycle().await.unwrap();
    let before = store.index().get_next_lsn()?;
    assert_eq!(queue.submit_epoch_strata(&lifecycle, &store, 0)?, epoch_lsn);
    assert_eq!(store.index().get_next_lsn()?, before);
    assert_eq!(store.current_epoch()?, 45);
    queue.acknowledge_epoch_rocksdb(&lifecycle, &store, 0)?;
    let snapshot = queue.durable_snapshot()?;
    assert!(queue.barriers(snapshot.as_ref())?.next().is_none());
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    Ok(())
}
