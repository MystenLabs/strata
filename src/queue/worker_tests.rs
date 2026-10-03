use std::{
    future::Future,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
};

use index::{
    StrataIndex,
    port::{IndexDb, IndexSnapshot, IndexWriteBatch, RocksBackend, RowCursor},
};
use tempfile::tempdir;

use super::*;
use crate::{
    StrataStoreConfig, StrataStoreMetrics,
    queue::replay_tests::{key, open_store},
};

fn physical_keys(blob: &[u8]) -> Result<Vec<BlobKey>> {
    Ok([b'p', b's']
        .iter()
        .map(|suffix| {
            let mut bytes = blob.to_vec();
            bytes.push(*suffix);
            key(&bytes)
        })
        .collect())
}

fn worker(store: Arc<StrataStore>, queue: PendingQueue, max: usize) -> QueueWorker {
    QueueWorker::new(
        queue,
        store,
        physical_keys,
        WorkerConfig {
            max_blobs_per_batch: max,
            ..WorkerConfig::default()
        },
    )
    .unwrap()
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

fn pending(queue: &PendingQueue, blob: &[u8]) -> Vec<u64> {
    queue
        .blobs
        .get(&blob.to_vec())
        .unwrap()
        .unwrap_or_default()
        .commands()
        .iter()
        .map(|command| command.event_index)
        .collect()
}

// Real RocksDB and Strata, with narrowly placed fault/ordering hooks at their metadata port.
#[derive(Default)]
struct Hooks {
    after_flush: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    before_ack: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    fail_flush: AtomicBool,
    fail_ack: AtomicBool,
    fail_checkpoint: AtomicBool,
    synced_acks: AtomicUsize,
    synced_checkpoints: AtomicUsize,
}

struct HookDb {
    inner: RocksBackend,
    hooks: Arc<Hooks>,
}

impl std::fmt::Debug for HookDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HookDb")
    }
}

impl IndexDb for HookDb {
    fn get(&self, cf: &str, key: &[u8]) -> index::Result<Option<Vec<u8>>> {
        self.inner.get(cf, key)
    }
    fn contains_key(&self, cf: &str, key: &[u8]) -> index::Result<bool> {
        self.inner.contains_key(cf, key)
    }
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> index::Result<()> {
        self.inner.put(cf, key, value)
    }
    fn delete(&self, cf: &str, key: &[u8]) -> index::Result<()> {
        self.inner.delete(cf, key)
    }
    fn scan<'a>(&'a self, cf: &str) -> index::Result<Box<dyn RowCursor + 'a>> {
        self.inner.scan(cf)
    }
    fn snapshot<'a>(&'a self) -> index::Result<Box<dyn IndexSnapshot + 'a>> {
        self.inner.snapshot()
    }
    fn cf_exists(&self, cf: &str) -> bool {
        self.inner.cf_exists(cf)
    }
    fn create_cf(&self, cf: &str, options: &rocksdb::Options) -> index::Result<()> {
        self.inner.create_cf(cf, options)
    }
    fn write_batch(&self) -> Box<dyn IndexWriteBatch> {
        Box::new(HookBatch {
            inner: self.inner.write_batch(),
            hooks: self.hooks.clone(),
            queue_changes: false,
            checkpoint: false,
        })
    }
    fn flush_wal(&self, sync: bool) -> index::Result<()> {
        if self.hooks.fail_flush.swap(false, Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.flush_wal(sync)?;
        let hook = self.hooks.after_flush.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        Ok(())
    }
}

struct HookBatch {
    inner: Box<dyn IndexWriteBatch>,
    hooks: Arc<Hooks>,
    queue_changes: bool,
    checkpoint: bool,
}

fn injected() -> index::Error {
    index::Error::RocksDb("injected sync failure".into())
}

impl IndexWriteBatch for HookBatch {
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> index::Result<()> {
        self.checkpoint |= cf.ends_with("store_state")
            && key == index::port::codec::encode_key(&core_types::StoreStateKey::CommittedLsn)?;
        self.inner.put(cf, key, value)
    }
    fn delete(&mut self, cf: &str, key: &[u8]) -> index::Result<()> {
        self.queue_changes |= cf == EPOCH_BARRIERS_CF;
        self.inner.delete(cf, key)
    }
    fn merge(&mut self, cf: &str, key: &[u8], value: &[u8]) -> index::Result<()> {
        self.queue_changes |= cf == PENDING_BLOBS_CF;
        self.inner.merge(cf, key, value)
    }
    fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }
    fn write(self: Box<Self>, sync: bool) -> index::Result<()> {
        if sync {
            if self.queue_changes {
                let hook = self.hooks.before_ack.lock().unwrap().take();
                if let Some(hook) = hook {
                    hook();
                }
                if self.hooks.fail_ack.swap(false, Ordering::SeqCst) {
                    return Err(injected());
                }
                self.hooks.synced_acks.fetch_add(1, Ordering::SeqCst);
            } else if self.checkpoint {
                if self.hooks.fail_checkpoint.swap(false, Ordering::SeqCst) {
                    // Model a visible checkpoint followed by failed fsync. Visibility must not
                    // let the worker acknowledge or admit a subsequent put.
                    self.inner.write(false)?;
                    return Err(injected());
                }
                self.hooks.synced_checkpoints.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.inner.write(sync)
    }
}

fn open_hooked(root: &Path) -> (Arc<StrataStore>, PendingQueue, Arc<Hooks>) {
    let mut cfg = StrataStoreConfig::new(root, "test");
    cfg.starting_epoch = 42;
    cfg.gc_workers_enabled = false;
    cfg.lsm_memtable_max_age = Duration::from_secs(3600);
    let options = cf_options(rocksdb::Options::default())
        .into_iter()
        .map(|(name, options)| (name.to_owned(), options))
        .chain(index::cf_options_for_prefix(&cfg.index_cf_prefix()))
        .collect::<Vec<_>>();
    let hooks = Arc::new(Hooks::default());
    let db = Arc::new(HookDb {
        inner: RocksBackend::open(root.join("db"), None, &options).unwrap(),
        hooks: hooks.clone(),
    });
    let index = StrataIndex::from_db(db.clone(), cfg.index_cf_prefix()).unwrap();
    let store = StrataStore::from_index(cfg, index, StrataStoreMetrics::default()).unwrap();
    (Arc::new(store), PendingQueue::new(db), hooks)
}

#[tokio::test]
async fn bounded_groups_share_sync_and_preserve_all_physical_keys_and_later_commands() -> Result<()>
{
    let dir = tempdir().unwrap();
    let (store, queue, hooks) = open_hooked(dir.path());
    store.add_shard(7)?;
    for blob in [b"a", b"b", b"c"] {
        for key in physical_keys(blob)? {
            store.set_blob_lifetime(&key, 45)?;
            store.put(7, &key, b"value")?;
        }
    }
    store.sync()?;
    hooks.synced_acks.store(0, Ordering::SeqCst);
    hooks.synced_checkpoints.store(0, Ordering::SeqCst);
    queue.write_batch(|b| {
        b.append(b"a", 1, delete(&[7]), vec![])?;
        b.append(
            b"b",
            1,
            BlobOperation::SetLifetime { end_epoch: 60 },
            vec![],
        )?;
        b.append(b"b", 2, delete(&[7]), vec![])?;
        b.append(b"c", 1, delete(&[7]), vec![])
    })?;
    let worker = worker(store.clone(), queue.clone(), 2);
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 2);
    // The worker issues one sync request for the group. The engine may also publish its own
    // periodic checkpoints, so count the acknowledgement batch rather than assuming one fsync.
    assert!(hooks.synced_checkpoints.load(Ordering::SeqCst) >= 1);
    assert_eq!(hooks.synced_acks.load(Ordering::SeqCst), 1);
    assert!(pending(&queue, b"a").is_empty());
    assert_eq!(pending(&queue, b"b"), vec![2]);
    assert_eq!(pending(&queue, b"c"), vec![1]);
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    let mut epoch = store.batch();
    epoch.advance_epoch_to(50);
    epoch.write()?;
    for key in physical_keys(b"b")? {
        assert_eq!(store.get_from_shard(7, &key)?, Some(b"value".to_vec()));
    }
    for key in physical_keys(b"a")? {
        assert_eq!(store.get_from_shard(7, &key)?, None);
    }
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 2);
    assert!(worker.process_batch().await?.is_idle());
    for blob in [b"b", b"c"] {
        for key in physical_keys(blob)? {
            assert_eq!(store.get_from_shard(7, &key)?, None);
        }
    }
    Ok(())
}

#[tokio::test]
async fn epoch_barrier_waits_for_all_earlier_blob_fronts_and_excludes_later_work() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let store = Arc::new(store);
    store.add_shard(7)?;
    for key in physical_keys(b"z")? {
        store.set_blob_lifetime(&key, 44)?;
        store.put(7, &key, b"value")?;
    }
    queue.write_batch(|b| {
        b.register(b"z", 1, 46, vec![])?;
        b.append(
            b"z",
            2,
            BlobOperation::SetLifetime { end_epoch: 55 },
            vec![],
        )?;
        b.advance_epoch(2, 50, vec![])?;
        // Sorts before z but must not cross the epoch barrier.
        b.register(b"a", 3, 65, vec![])
    })?;
    let worker = worker(store.clone(), queue.clone(), 128);
    for _ in 0..2 {
        assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
        assert_eq!(store.current_epoch()?, 42);
        assert_eq!(pending(&queue, b"a"), vec![3]);
    }
    assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
    assert_eq!(store.current_epoch()?, 50);
    for key in physical_keys(b"z")? {
        assert_eq!(store.get_from_shard(7, &key)?, Some(b"value".to_vec()));
    }
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
    assert!(worker.process_batch().await?.is_idle());
    Ok(())
}

#[tokio::test]
async fn post_sync_writes_and_cancelled_selections_wait_for_a_new_durable_snapshot() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue, hooks) = open_hooked(dir.path());
    store.add_shard(7)?;
    for key in physical_keys(b"a")? {
        store.put(7, &key, b"value")?;
    }
    store.sync()?;
    queue.write_batch(|b| b.append(b"a", 1, delete(&[7]), vec![]))?;
    let producer = queue.clone();
    *hooks.after_flush.lock().unwrap() = Some(Box::new(move || {
        // Between selection's sync and its scan/locks: these edits are visible, not synced.
        let _guard = Handle::current()
            .block_on(producer.lock_blobs(&[b"a"]))
            .unwrap();
        producer
            .write_batch(|b| {
                b.register(b"a", 2, 60, vec![])?;
                b.register(b"b", 2, 60, vec![])
            })
            .unwrap();
    }));
    let worker = worker(store.clone(), queue.clone(), 128);
    let next = store.index().get_next_lsn()?;
    assert_eq!(
        worker.process_batch().await?,
        WorkerProgress {
            cancelled_blobs: 1,
            ..WorkerProgress::default()
        }
    );
    assert_eq!(store.index().get_next_lsn()?, next);
    assert_eq!(pending(&queue, b"a"), vec![2]);
    assert_eq!(pending(&queue, b"b"), vec![2]);
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 2);
    for key in physical_keys(b"a")? {
        assert_eq!(store.get_from_shard(7, &key)?, Some(b"value".to_vec()));
    }
    Ok(())
}

#[tokio::test]
async fn retired_generations_are_skipped_without_empty_writes_or_touching_replacements()
-> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let store = Arc::new(store);
    store.add_shard(7)?;
    store.add_shard(8)?;
    queue.write_batch(|b| {
        b.append(b"a", 1, delete(&[7, 8]), vec![])?;
        b.append(b"b", 1, delete(&[7]), vec![])
    })?;
    store.drop_shard(7)?;
    assert_eq!(store.add_shard(7)?.generation, 1);
    for blob in [b"a", b"b"] {
        for key in physical_keys(blob)? {
            store.put(7, &key, b"replacement")?;
            store.put(8, &key, b"old")?;
        }
    }
    let worker = worker(store.clone(), queue.clone(), 128);
    let next = store.index().get_next_lsn()?;
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 2);
    assert_eq!(store.index().get_next_lsn()?, next + 2); // Only a's two keys in shard 8.
    for blob in [b"a", b"b"] {
        for key in physical_keys(blob)? {
            assert_eq!(
                store.get_from_shard(7, &key)?,
                Some(b"replacement".to_vec())
            );
        }
    }
    for key in physical_keys(b"a")? {
        assert_eq!(store.get_from_shard(8, &key)?, None);
    }
    queue.write_batch(|b| b.append(b"b", 2, delete(&[8]), vec![]))?;
    store.drop_shard(8)?;
    let next = store.index().get_next_lsn()?;
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
    assert_eq!(store.index().get_next_lsn()?, next);
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    drop(worker);
    drop(queue);
    drop(store);
    let (store, queue) = open_store(dir.path());
    assert!(pending(&queue, b"b").is_empty());
    assert!(store.shard_info(8)?.unwrap().is_dropped());
    for key in physical_keys(b"b")? {
        assert_eq!(
            store.get_from_shard(7, &key)?,
            Some(b"replacement".to_vec())
        );
    }
    Ok(())
}

#[tokio::test]
async fn failed_ack_reopens_and_acknowledges_surviving_submission_without_replaying() -> Result<()>
{
    let dir = tempdir().unwrap();
    {
        let (store, queue, hooks) = open_hooked(dir.path());
        store.add_shard(7)?;
        queue.write_batch(|b| b.append(b"a", 1, delete(&[7]), vec![]))?;
        hooks.fail_ack.store(true, Ordering::SeqCst);
        let worker = worker(store.clone(), queue.clone(), 128);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *hooks.before_ack.lock().unwrap() = Some(Box::new(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }));
        let task_worker = worker.clone();
        let task = tokio::spawn(async move { task_worker.process_batch().await });
        started_rx.await.unwrap();
        let mut waiting = Box::pin(queue.lock_blobs(&[b"a"]));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        release_tx.send(()).unwrap();
        assert!(task.await.unwrap().is_err());
        // A producer already waiting at the lock must be rejected before it can write.
        assert!(matches!(waiting.await, Err(Error::WorkerHalted { .. })));
        assert_eq!(pending(&queue, b"a"), vec![1]);
        assert!(!store.index().submitted_batch_lsns().is_empty()?);
        assert!(matches!(
            queue.lock_blobs(&[b"a"]).await,
            Err(Error::WorkerHalted { .. })
        ));
        assert!(worker.process_batch().await.is_err());
    }
    let (store, queue) = open_store(dir.path());
    let store = Arc::new(store);
    let next = store.index().get_next_lsn()?;
    let worker = QueueWorker::new(
        queue.clone(),
        store.clone(),
        |_| panic!("must reuse recovered LSN"),
        WorkerConfig::default(),
    )?;
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1);
    assert_eq!(store.index().get_next_lsn()?, next);
    assert!(pending(&queue, b"a").is_empty());
    assert!(store.index().submitted_batch_lsns().is_empty()?);
    Ok(())
}

#[tokio::test]
async fn failed_input_sync_or_visible_checkpoint_closes_admission_without_acknowledging()
-> Result<()> {
    for checkpoint_failure in [false, true] {
        let dir = tempdir().unwrap();
        let (store, queue, hooks) = open_hooked(dir.path());
        queue.write_batch(|b| b.register(b"a", 1, 60, vec![]))?;
        if checkpoint_failure {
            hooks.fail_checkpoint.store(true, Ordering::SeqCst);
        } else {
            hooks.fail_flush.store(true, Ordering::SeqCst);
        }
        let worker = worker(store.clone(), queue.clone(), 128);
        assert!(worker.process_batch().await.is_err());
        assert_eq!(pending(&queue, b"a"), vec![1]);
        assert_eq!(hooks.synced_acks.load(Ordering::SeqCst), 0);
        assert!(matches!(
            queue.lock_lifecycle().await,
            Err(Error::WorkerHalted { .. })
        ));
        assert!(
            queue
                .write_batch(|b| b.register(b"b", 2, 60, vec![]))
                .is_err()
        );
        if checkpoint_failure {
            let lsn = store
                .index()
                .submitted_batch_lsns()
                .get(&blob_lsn_key_rocksdb(b"a", 1)?)?
                .unwrap();
            assert!(store.published_lsn()? >= lsn); // Visible is not evidence of successful fsync.
            assert!(
                store
                    .subscribe_durability_progress()
                    .borrow()
                    .halt_reason
                    .is_some()
            );
        } else {
            assert!(store.index().submitted_batch_lsns().is_empty()?);
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_generation_or_resolver_panic_halts_and_preserves_pending_work() -> Result<()> {
    for kind in 0..3 {
        let dir = tempdir().unwrap();
        let (store, queue) = open_store(dir.path());
        let store = Arc::new(store);
        if kind != 0 {
            store.add_shard(7)?;
        }
        let operation = if kind == 1 {
            BlobOperation::Delete {
                shards: vec![ShardGeneration {
                    shard: 7,
                    generation: 1,
                }],
                cancellable: true,
            }
        } else {
            delete(&[7])
        };
        queue.write_batch(|b| b.append(b"a", 1, operation, vec![]))?;
        let resolve: fn(&[u8]) -> Result<Vec<BlobKey>> = if kind == 2 {
            |_| panic!("resolver failed")
        } else {
            physical_keys
        };
        let worker = QueueWorker::new(
            queue.clone(),
            store.clone(),
            resolve,
            WorkerConfig::default(),
        )?;
        let before = store.index().get_next_lsn()?;
        assert!(worker.process_batch().await.is_err());
        assert!(matches!(
            queue.lock_blobs(&[b"a"]).await,
            Err(Error::WorkerHalted { .. })
        ));
        assert_eq!(store.index().get_next_lsn()?, before);
        assert_eq!(pending(&queue, b"a"), vec![1]);
    }
    Ok(())
}

#[tokio::test]
async fn bounded_passes_do_not_starve_later_blobs_and_wrap_before_epoch_advancement() -> Result<()>
{
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let store = Arc::new(store);
    queue.write_batch(|b| {
        b.register(b"a", 1, 60, vec![])?;
        b.append(
            b"a",
            2,
            BlobOperation::SetLifetime { end_epoch: 65 },
            vec![],
        )?;
        b.register(b"z", 1, 60, vec![])?;
        b.advance_epoch(3, 50, vec![])
    })?;
    let worker = worker(store.clone(), queue.clone(), 1);
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1); // a/1
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1); // z/1, despite a/2
    assert!(pending(&queue, b"z").is_empty());
    assert_eq!(pending(&queue, b"a"), vec![2]);
    assert_eq!(worker.process_batch().await?.acknowledged_blobs, 1); // Wrap to a/2 before barrier.
    assert_eq!(store.current_epoch()?, 42);
    assert_eq!(worker.process_batch().await?.acknowledged_epochs, 1);
    assert_eq!(store.current_epoch()?, 50);
    Ok(())
}

#[tokio::test]
async fn concurrent_passes_do_not_submit_the_same_command_twice() -> Result<()> {
    let dir = tempdir().unwrap();
    let (store, queue) = open_store(dir.path());
    let store = Arc::new(store);
    queue.write_batch(|b| b.register(b"a", 1, 60, vec![]))?;
    let first = worker(store.clone(), queue.clone(), 128);
    let second = worker(store.clone(), queue, 128);
    let before = store.index().get_next_lsn()?;
    let (a, b) = tokio::join!(first.process_batch(), second.process_batch());
    assert_eq!(a?.acknowledged_blobs + b?.acknowledged_blobs, 1);
    assert_eq!(store.index().get_next_lsn()?, before + 2);
    Ok(())
}

#[tokio::test]
async fn shutdown_and_caller_cancellation_finish_scheduled_passes() -> Result<()> {
    for cancel_caller in [false, true] {
        let dir = tempdir().unwrap();
        let (store, queue, hooks) = open_hooked(dir.path());
        queue.write_batch(|b| b.register(b"a", 1, 60, vec![]))?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *hooks.before_ack.lock().unwrap() = Some(Box::new(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }));
        let worker = worker(store.clone(), queue.clone(), 128);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task_worker = worker.clone();
        let task = tokio::spawn(async move { task_worker.run(shutdown_rx).await });
        started_rx.await.unwrap();
        // Pause after the Strata sync, before acknowledgement, while the worker owns the blob.
        let mut waiting = Box::pin(queue.lock_blobs(&[b"a"]));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if cancel_caller {
            task.abort();
        } else {
            shutdown_tx.send(true).unwrap();
        }
        release_tx.send(()).unwrap();
        if cancel_caller {
            assert!(task.await.unwrap_err().is_cancelled());
            // A following pass waits for the detached blocking pass to finish and sees it acked.
            assert!(worker.process_batch().await?.is_idle());
        } else {
            task.await.unwrap()?;
        }
        drop(waiting.await?);
        assert!(pending(&queue, b"a").is_empty());
        assert!(store.index().submitted_batch_lsns().is_empty()?);
        assert!(queue.lock_blobs(&[b"a"]).await.is_ok());
    }
    Ok(())
}
