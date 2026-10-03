use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use super::*;
use crate::queue::{
    BlobOperation, ShardGeneration,
    replay_tests::{key, open_store},
};
use tempfile::tempdir;

fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

fn delete(shards: &[u64]) -> BlobOperation {
    BlobOperation::Delete {
        shards: shards
            .iter()
            .map(|&shard| ShardGeneration {
                shard,
                generation: 0,
            })
            .collect(),
        cancellable: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reacquisition_keeps_one_lock_per_blob_and_cleans_up() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempdir().unwrap();
    let (_store, queue) = open_store(dir.path());
    let active = Arc::new(std::array::from_fn::<_, 8, _>(|_| AtomicUsize::new(0)));
    let mut tasks = Vec::new();
    for task in 0..32 {
        let queue = queue.clone();
        let active = active.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..32 {
                let index = (task + round) % active.len();
                let key = [index as u8];
                let guard = queue.lock_blobs(&[&key]).await.unwrap();
                assert_eq!(active[index].fetch_add(1, Ordering::SeqCst), 0);
                tokio::task::yield_now().await;
                assert_eq!(active[index].fetch_sub(1, Ordering::SeqCst), 1);
                drop(guard);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert!(queue.coordination.blobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn foreground_admission_reads_latest_work_and_halts_waiting_puts() -> Result<()> {
    let dir = tempdir().unwrap();
    let (_store, queue) = open_store(dir.path());
    let guard = queue.lock_blobs(&[b"a"]).await?;
    assert!(queue.pending_blob(&guard, b"a")?.commands().is_empty());
    assert!(queue.pending_blob(&guard, b"b").is_err());
    queue.write_batch(|b| b.register(b"a", 1, 50, vec![]))?;
    assert_eq!(
        queue.pending_blob(&guard, b"a")?.commands()[0].event_index,
        1
    );
    let mut waiting = Box::pin(queue.lock_blobs(&[b"a"]));
    assert_pending(waiting.as_mut());
    queue.halt("foreground durability failure".into());
    drop(guard);
    assert!(matches!(waiting.await, Err(Error::WorkerHalted { .. })));
    assert!(
        queue
            .write_batch(|b| b.register(b"b", 2, 50, vec![]))
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn clones_serialize_the_same_blob_and_allow_other_blobs() {
    let dir = tempdir().unwrap();
    let (_store, queue) = open_store(dir.path());
    let clone = queue.clone();
    let first = queue.lock_blobs(&[b"a"]).await.unwrap();
    let mut same = Box::pin(clone.lock_blobs(&[b"a"]));
    assert_pending(same.as_mut());
    let other = clone.lock_blobs(&[b"b"]).await.unwrap();
    assert_eq!(queue.coordination.blobs.lock().unwrap().len(), 2);
    drop(other);
    assert_eq!(queue.coordination.blobs.lock().unwrap().len(), 1);
    drop(first);
    let second = same.await.unwrap();
    assert!(queue.check_blob_lock(&second, b"a").is_ok());
    drop(second);
    assert!(queue.coordination.blobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_group_releases_partial_locks_and_lifecycle_admission() {
    let dir = tempdir().unwrap();
    let (_store, queue) = open_store(dir.path());
    let held = queue.lock_blobs(&[b"b"]).await.unwrap();
    let mut group = Box::pin(queue.lock_blobs(&[b"b", b"a", b"a"]));
    assert_pending(group.as_mut());
    {
        let blobs = queue.coordination.blobs.lock().unwrap();
        assert_eq!(blobs.len(), 2);
        // Sorted acquisition holds a before waiting on b, despite the caller's reverse order.
        assert!(
            blobs
                .get(b"a".as_slice())
                .unwrap()
                .upgrade()
                .unwrap()
                .mutex
                .try_lock()
                .is_err()
        );
    }
    let mut lifecycle = Box::pin(queue.lock_lifecycle());
    assert_pending(lifecycle.as_mut());
    let mut later = Box::pin(queue.lock_blobs(&[b"c"]));
    assert_pending(later.as_mut());
    drop(group);
    assert_eq!(queue.coordination.blobs.lock().unwrap().len(), 1);
    drop(held);
    let lifecycle = lifecycle.await.unwrap();
    assert!(queue.coordination.blobs.lock().unwrap().is_empty());
    assert_pending(later.as_mut());
    drop(lifecycle);
    drop(later.await);
    assert!(queue.coordination.blobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn opposite_order_groups_make_progress_after_release() {
    let dir = tempdir().unwrap();
    let (_store, queue) = open_store(dir.path());
    let first = queue.lock_blobs(&[b"b", b"a", b"a"]).await.unwrap();
    assert_eq!(first.entries.len(), 2);
    let mut second = Box::pin(queue.lock_blobs(&[b"a", b"b"]));
    assert_pending(second.as_mut());
    drop(first);
    let second = second.await.unwrap();
    assert!(queue.check_blob_lock(&second, b"a").is_ok());
    assert!(queue.check_blob_lock(&second, b"b").is_ok());
    drop(second);
    assert!(queue.coordination.blobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn registration_waits_for_delete_durability_and_acknowledgement() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    {
        let lifecycle = queue.lock_lifecycle().await.unwrap();
        queue.add_shard_strata(&lifecycle, &store, 7)?;
    }
    let physical = key(b"sliver");
    let worker = queue.lock_blobs(&[b"blob"]).await.unwrap();
    store.put(7, &physical, b"old")?;
    store.sync()?;
    queue.write_batch(|b| b.append(b"blob", 1, delete(&[7]), vec![]))?;
    drop(queue.durable_snapshot()?);
    let mut batch = store.batch();
    batch.tombstone(7, physical.clone());
    let lsn = queue.submit_blob_strata(&worker, b"blob", 1, batch)?;

    let mut registration = Box::pin(queue.lock_blobs(&[b"blob"]));
    assert_pending(registration.as_mut());
    assert!(
        queue
            .acknowledge_blobs_rocksdb(&worker, &store, &[(b"blob", 1)])
            .is_err()
    );
    store.sync()?;
    assert_pending(registration.as_mut());
    let mut retry = store.batch();
    retry.tombstone(7, physical.clone());
    let next_lsn = store.index().get_next_lsn()?;
    assert_eq!(queue.submit_blob_strata(&worker, b"blob", 1, retry)?, lsn);
    assert_eq!(store.index().get_next_lsn()?, next_lsn);
    queue.acknowledge_blobs_rocksdb(&worker, &store, &[(b"blob", 1)])?;
    assert_pending(registration.as_mut());
    drop(worker);

    let registered = registration.await.unwrap();
    queue.write_batch(|b| b.register(b"blob", 2, 60, vec![]))?;
    drop(queue.durable_snapshot()?);
    let mut batch = store.batch();
    batch.set_blob_lifetime(physical.clone(), 60);
    queue.submit_blob_strata(&registered, b"blob", 2, batch)?;
    store.sync()?;
    queue.acknowledge_blobs_rocksdb(&registered, &store, &[(b"blob", 2)])?;
    store.put(7, &physical, b"new")?;
    store.sync()?;
    drop(registered);
    drop(queue);
    drop(store);
    let (store, _) = open_store(dir.path());
    assert_eq!(store.get_from_shard(7, &physical)?, Some(b"new".to_vec()));
    Ok(())
}

#[tokio::test]
async fn registration_cancels_a_delete_selected_before_the_blob_lock() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    queue.write_batch(|b| b.append(b"blob", 1, delete(&[7]), vec![]))?;
    let snapshot = queue.durable_snapshot()?;
    let event = queue
        .blobs(snapshot.as_ref())?
        .next()
        .unwrap()?
        .1
        .commands()[0]
        .event_index;
    {
        let _registration = queue.lock_blobs(&[b"blob"]).await.unwrap();
        queue.write_batch(|b| b.register(b"blob", 2, 60, vec![]))?;
    }
    let worker = queue.lock_blobs(&[b"blob"]).await.unwrap();
    let mut batch = store.batch();
    batch.tombstone(7, key(b"sliver"));
    assert!(matches!(
        queue.submit_blob_strata(&worker, b"blob", event, batch),
        Err(Error::InvalidPendingOperation(_))
    ));
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    Ok(())
}

#[tokio::test]
async fn stale_shard_generation_rejects_the_entire_delete_before_writing() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let physical = key(b"sliver");
    {
        let lifecycle = queue.lock_lifecycle().await.unwrap();
        queue.add_shard_strata(&lifecycle, &store, 7)?;
        queue.add_shard_strata(&lifecycle, &store, 8)?;
    }
    {
        let _blobs = queue.lock_blobs(&[b"blob"]).await.unwrap();
        store.put(7, &physical, b"old-seven")?;
        store.put(8, &physical, b"eight")?;
        store.sync()?;
        queue.write_batch(|b| b.append(b"blob", 1, delete(&[8, 7]), vec![]))?;
    }
    drop(queue.durable_snapshot()?);
    {
        let lifecycle = queue.lock_lifecycle().await.unwrap();
        queue.drop_shard_strata(&lifecycle, &store, 7)?;
        let replacement = queue.add_shard_strata(&lifecycle, &store, 7)?;
        assert_eq!(replacement.generation, 1);
        store.sync()?;
    }
    let worker = queue.lock_blobs(&[b"blob"]).await.unwrap();
    store.put(7, &physical, b"new-seven")?;
    store.sync()?;
    let before = store.index().get_next_lsn()?;
    let mut batch = store.batch();
    batch
        .tombstone(8, physical.clone())
        .tombstone(7, physical.clone());
    assert!(matches!(
        queue.submit_blob_strata(&worker, b"blob", 1, batch),
        Err(Error::Store(store::Error::ShardUnavailable {
            generation: 0,
            current_generation: 1,
            ..
        }))
    ));
    assert_eq!(store.index().get_next_lsn()?, before);
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    assert_eq!(
        store.get_from_shard(7, &physical)?,
        Some(b"new-seven".to_vec())
    );
    assert_eq!(store.get_from_shard(8, &physical)?, Some(b"eight".to_vec()));
    assert_eq!(
        queue
            .blobs
            .get(&b"blob".to_vec())?
            .unwrap()
            .commands()
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn missing_dropped_future_and_out_of_range_shards_do_not_submit() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    {
        let lifecycle = queue.lock_lifecycle().await.unwrap();
        queue.add_shard_strata(&lifecycle, &store, 7)?;
        queue.add_shard_strata(&lifecycle, &store, 8)?;
        queue.drop_shard_strata(&lifecycle, &store, 8)?;
        store.sync()?;
    }
    let cases = [(9, 0), (8, 0), (7, 1), (u64::MAX, 0)];
    let before = store.index().get_next_lsn()?;
    for (shard, generation) in cases {
        let blob = shard.to_be_bytes();
        let guard = queue.lock_blobs(&[&blob]).await.unwrap();
        queue.write_batch(|b| {
            b.append(
                &blob,
                1,
                BlobOperation::Delete {
                    shards: vec![ShardGeneration { shard, generation }],
                    cancellable: true,
                },
                vec![],
            )
        })?;
        drop(queue.durable_snapshot()?);
        let mut batch = store.batch();
        batch.tombstone(7, key(b"sliver"));
        let result = queue.submit_blob_strata(&guard, &blob, 1, batch);
        match shard {
            9 => assert!(matches!(
                result,
                Err(Error::Store(store::Error::ShardNotFound { shard_id: 9 }))
            )),
            7 | 8 => assert!(matches!(
                result,
                Err(Error::Store(store::Error::ShardUnavailable { .. }))
            )),
            _ => assert!(matches!(result, Err(Error::InvalidPendingOperation(_)))),
        }
    }
    assert_eq!(store.index().get_next_lsn()?, before);
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    Ok(())
}

#[tokio::test]
async fn replay_rejects_foreign_guards_and_guards_for_other_blobs() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let foreign = PendingQueue::new(store.index().db().clone());
    queue.write_batch(|b| b.register(b"a", 1, 50, vec![]))?;
    drop(queue.durable_snapshot()?);
    let foreign_guard = foreign.lock_blobs(&[b"a"]).await.unwrap();
    let wrong_blob = queue.lock_blobs(&[b"b"]).await.unwrap();
    let before = store.index().get_next_lsn()?;
    for guard in [&foreign_guard, &wrong_blob] {
        let mut batch = store.batch();
        batch.set_blob_lifetime(key(b"a"), 50);
        assert!(matches!(
            queue.submit_blob_strata(guard, b"a", 1, batch),
            Err(Error::InvalidPendingOperation(_))
        ));
    }
    assert_eq!(store.index().get_next_lsn()?, before);
    let valid = queue.lock_blobs(&[b"a"]).await.unwrap();
    let mut batch = store.batch();
    batch.set_blob_lifetime(key(b"a"), 50);
    queue.submit_blob_strata(&valid, b"a", 1, batch)?;
    store.sync()?;
    assert!(
        queue
            .acknowledge_blobs_rocksdb(&wrong_blob, &store, &[(b"a", 1)])
            .is_err()
    );
    assert!(!store.index().submitted_batch_lsns().is_empty()?);
    queue
        .clone()
        .acknowledge_blobs_rocksdb(&valid, &store, &[(b"a", 1)])?;
    drop(foreign_guard);
    let lifecycle = foreign.lock_lifecycle().await.unwrap();
    assert!(queue.add_shard_strata(&lifecycle, &store, 7).is_err());
    assert!(store.shard_info(7)?.is_none());
    Ok(())
}
