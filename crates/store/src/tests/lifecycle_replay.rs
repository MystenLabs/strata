use super::*;

fn state(store: &StrataStore, key: &BlobKey) -> crate::blob_lsm::BlobState {
    let value = store
        .lsm()
        .unwrap()
        .get(
            crate::partition::partition_for_key(key.as_bytes(), store.config.lsm_partition_count),
            key.as_bytes(),
            &crate::blob_lsm::BlobMerge,
        )
        .unwrap()
        .unwrap();
    let lsm::StoredValue::Inline(bytes) = lsm::decode_value(&value).unwrap() else {
        panic!()
    };
    crate::blob_lsm::BlobState::decode(bytes).unwrap()
}

#[tokio::test]
async fn lifecycle_replay_preserves_new_puts_across_repeated_recovery() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"replay".to_vec()).unwrap();
    let other_shard;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        other_shard = store.add_shard(7).unwrap();
        let mut batch = store.batch();
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"old"[..]);
        batch.put(other_shard.id, key.clone(), &b"old"[..]);
        batch.tombstone_at_event(key.clone(), 0, vec![STANDALONE_SHARD, other_shard]);
        let result = batch.write().unwrap();
        store.sync().unwrap();
        assert!(store.published_lsn().unwrap() >= result.last_lsn().unwrap());
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.get_from_shard(other_shard.id, &key).unwrap(), None);
    }
    for _ in 0..3 {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        // The old queue row can survive cleanup. No new event is needed for the direct put;
        // the durable marker must protect it from another application of that same delete.
        let mut batch = store.batch();
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"new"[..]);
        batch.put(other_shard.id, key.clone(), &b"new"[..]);
        batch.tombstone_at_event(key.clone(), 0, vec![STANDALONE_SHARD, other_shard]);
        batch.write().unwrap();
        store.sync().unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            store.get_from_shard(other_shard.id, &key).unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(state(&store, &key).last_event_index, Some(0));
    }
}

#[tokio::test]
async fn lifecycle_replay_keeps_new_lifetimes_and_does_not_count_extensions_twice() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"lifetime".to_vec()).unwrap();
    let lifetime;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch.set_blob_lifetime_at_event(key.clone(), 0, 43);
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"value"[..]);
        batch.set_blob_lifetime_at_event(key.clone(), 5, 50);
        batch.advance_epoch_to(44);
        batch.write().unwrap();
        store.sync().unwrap();
        lifetime = state(&store, &key).lifetime.unwrap();
        assert_eq!(lifetime.lifecycle.extension_count, 1);
    }
    for _ in 0..3 {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        // The old lifetime is now in the past. Replaying it must neither shrink expiry nor
        // clear the current versions through the ordinary lifetime-reset behavior.
        batch.set_blob_lifetime_at_event(key.clone(), 0, 43);
        batch.set_blob_lifetime_at_event(key.clone(), 5, 50);
        batch.write().unwrap();
        store.sync().unwrap();
        assert_eq!(state(&store, &key).lifetime, Some(lifetime));
        assert_eq!(store.get(&key).unwrap(), Some(b"value".to_vec()));
    }
}

#[tokio::test]
async fn lifecycle_replay_progress_is_per_key_and_skips_older_deletes() {
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let a = BlobKey::new(b"a".to_vec()).unwrap();
    let b = BlobKey::new(b"b".to_vec()).unwrap();
    let mut batch = store.batch();
    batch.set_blob_lifetime_at_event(a.clone(), u64::MAX, 50);
    batch.set_blob_lifetime_at_event(b.clone(), 0, 60);
    batch.put(STANDALONE_SHARD.id, a.clone(), &b"a"[..]);
    batch.put(STANDALONE_SHARD.id, b.clone(), &b"b"[..]);
    batch.tombstone_at_event(a.clone(), 1, vec![STANDALONE_SHARD]);
    batch.tombstone_at_event(b.clone(), 1, vec![STANDALONE_SHARD]);
    batch.write().unwrap();
    assert_eq!(store.get(&a).unwrap(), Some(b"a".to_vec()));
    assert_eq!(store.get(&b).unwrap(), None);
    assert_eq!(state(&store, &a).last_event_index, Some(u64::MAX));
    assert_eq!(state(&store, &b).last_event_index, Some(1));
}

#[tokio::test]
async fn lifecycle_replay_epoch_targets_are_monotonic_across_reopen() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    for _ in 0..3 {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch
            .advance_epoch_to(45)
            .advance_epoch_to(43)
            .advance_epoch_to(45);
        let result = batch.write().unwrap();
        assert_eq!(result.op_epochs(), &[Some(45), Some(45), Some(45)]);
        store.sync().unwrap();
        assert_eq!(store.current_epoch().unwrap(), 45);
        for &lsn in result.op_lsns() {
            assert_eq!(store.epoch_at_lsn(lsn).unwrap(), Some(45));
        }
    }
}

#[tokio::test]
async fn lifecycle_replay_lost_wal_tail_loses_effect_and_marker_together() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"replay".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch.set_blob_lifetime_at_event(key.clone(), 0, 50);
        batch.advance_epoch_to(44);
        batch.write().unwrap();
        assert_eq!(state(&store, &key).last_event_index, Some(0));
        assert_eq!(store.published_lsn().unwrap(), 0);
    }
    // Model an un-fsynced store WAL lost on power failure, with its visible RocksDB metadata
    // surviving. Recovery must roll back the epoch and the blob marker, not merely the payload.
    std::fs::remove_file(Wal::path(cfg.namespace_dir().join("wal"), 1)).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.current_epoch().unwrap(), 42);
    let mut batch = store.batch();
    batch.set_blob_lifetime_at_event(key.clone(), 0, 50);
    batch.put(STANDALONE_SHARD.id, key.clone(), &b"value"[..]);
    batch.advance_epoch_to(44);
    batch.write().unwrap();
    store.sync().unwrap();
    assert_eq!(state(&store, &key).last_event_index, Some(0));
    assert_eq!(
        state(&store, &key)
            .lifetime
            .unwrap()
            .lifecycle
            .logical_end_epoch,
        50
    );
    assert_eq!(store.get(&key).unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.current_epoch().unwrap(), 44);
}

#[tokio::test]
async fn lifecycle_replay_retries_a_delete_lost_after_a_durable_prefix() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"prefix".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch.set_blob_lifetime_at_event(key.clone(), 0, 50);
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"value"[..]);
        batch.write().unwrap();
        store.sync().unwrap();
    }
    // Crash twice during replay. Each time only the old durable prefix survives, so event 1
    // must remain eligible; neither a previous attempt's LSN nor unrelated progress can prove it.
    for _ in 0..2 {
        let checkpoint = {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            assert_eq!(store.get(&key).unwrap(), Some(b"value".to_vec()));
            assert_eq!(state(&store, &key).last_event_index, Some(0));
            let checkpoint = store.index().get_store_checkpoint().unwrap().unwrap();
            let mut batch = store.batch();
            batch.tombstone_at_event(key.clone(), 1, vec![STANDALONE_SHARD]);
            let applied = batch.write().unwrap().last_lsn().unwrap();
            assert_eq!(store.get(&key).unwrap(), None);
            assert!(store.published_lsn().unwrap() < applied);
            checkpoint
        };
        OpenOptions::new()
            .write(true)
            .open(Wal::path(
                cfg.namespace_dir().join("wal"),
                checkpoint.wal_position.log_id,
            ))
            .unwrap()
            .set_len(checkpoint.wal_position.offset)
            .unwrap();
    }
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(state(&store, &key).last_event_index, Some(0));
        let mut batch = store.batch();
        batch.tombstone_at_event(key.clone(), 1, vec![STANDALONE_SHARD]);
        batch.write().unwrap();
        store.sync().unwrap();
    }
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(state(&store, &key).last_event_index, Some(1));
    assert_eq!(store.get(&key).unwrap(), None);
}
