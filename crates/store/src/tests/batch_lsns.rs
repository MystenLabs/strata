use super::*;

#[tokio::test]
async fn tracked_batch_records_last_lsn_and_rejects_duplicate_before_mutating() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    let tag = b"opaque-caller-key".to_vec();
    let lsn;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch.set_blob_lifetime(key.clone(), 50);
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"value"[..]);
        let result = batch.write_with_lsn(tag.clone()).unwrap();
        lsn = result.last_lsn().unwrap();
        assert_eq!(result.op_lsns(), &[lsn - 1, lsn]);
        assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), Some(lsn));
        assert!(store.published_lsn().unwrap() < lsn);

        let mut duplicate = store.batch();
        duplicate.tombstone(STANDALONE_SHARD.id, key.clone());
        assert!(matches!(
            duplicate.write_with_lsn(tag.clone()),
            Err(Error::BatchKeyAlreadyExists)
        ));
        assert_eq!(store.get(&key).unwrap(), Some(b"value".to_vec()));
        assert_eq!(store.index().get_next_lsn().unwrap(), lsn + 1);
        store.sync().unwrap();
    }
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), Some(lsn));
    assert!(store.published_lsn().unwrap() >= lsn);
}

#[tokio::test]
async fn rejected_batches_do_not_reserve_a_key_or_an_lsn() {
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let tag = b"rejected".to_vec();
    let next_lsn = store.index().get_next_lsn().unwrap();
    assert!(matches!(
        store.batch().write_with_lsn(tag.clone()),
        Err(Error::EmptyTrackedBatch)
    ));
    let mut batch = store.batch();
    batch.set_blob_lifetime(BlobKey::new(b"blob".to_vec()).unwrap(), 42);
    assert!(matches!(
        batch.write_with_lsn(tag.clone()),
        Err(Error::InvalidBlobLifetime { .. })
    ));
    assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), next_lsn);
}

#[tokio::test]
async fn rollback_durably_forgets_binding_before_lsn_reuse_and_another_restart() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    let tag = b"delete".to_vec();
    let kept_tag = b"kept".to_vec();
    let (checkpoint, lost_lsn);
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch.put(STANDALONE_SHARD.id, key.clone(), &b"value"[..]);
        batch.write_with_lsn(kept_tag.clone()).unwrap();
        store.sync().unwrap();
        checkpoint = store.index().get_store_checkpoint().unwrap().unwrap();
        let mut batch = store.batch();
        batch.tombstone(STANDALONE_SHARD.id, key.clone());
        lost_lsn = batch
            .write_with_lsn(tag.clone())
            .unwrap()
            .last_lsn()
            .unwrap();
        // The control table survives, while the un-fsynced Strata WAL tail is lost below.
        store.index().flush_wal(true).unwrap();
        assert!(store.published_lsn().unwrap() < lost_lsn);
    }
    OpenOptions::new()
        .write(true)
        .open(Wal::path(
            cfg.namespace_dir().join("wal"),
            checkpoint.wal_position.log_id,
        ))
        .unwrap()
        .set_len(checkpoint.wal_position.offset)
        .unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"value".to_vec()));
        assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), None);
        assert!(store.index().batch_lsns().get(&kept_tag).unwrap().is_some());
        // Unrelated work reuses the old number. It must never make the lost delete look applied.
        let unrelated = BlobKey::new(b"unrelated".to_vec()).unwrap();
        let reused_lsn = store.put(&unrelated, b"other").unwrap();
        assert_eq!(reused_lsn, lost_lsn);
        store.sync().unwrap();
    }
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert!(store.published_lsn().unwrap() >= lost_lsn);
        assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), None);
        let mut batch = store.batch();
        batch.tombstone(STANDALONE_SHARD.id, key.clone());
        let retry_lsn = batch
            .write_with_lsn(tag.clone())
            .unwrap()
            .last_lsn()
            .unwrap();
        assert!(retry_lsn > lost_lsn);
        store.sync().unwrap();
    }
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
    assert!(store.index().batch_lsns().get(&tag).unwrap().unwrap() > lost_lsn);
}

#[tokio::test]
async fn recovery_promotes_complete_unsynced_batch_with_its_binding() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let tag = b"epoch".to_vec();
    let last;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let mut batch = store.batch();
        batch
            .advance_epoch_to(45)
            .advance_epoch_to(43)
            .advance_epoch_to(45);
        let result = batch.write_with_lsn(tag.clone()).unwrap();
        assert_eq!(result.op_epochs(), &[Some(45), Some(45), Some(45)]);
        last = result.last_lsn().unwrap();
        assert!(store.published_lsn().unwrap() < last);
        store.index().flush_wal(true).unwrap();
    }
    for _ in 0..2 {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.index().batch_lsns().get(&tag).unwrap(), Some(last));
        assert_eq!(store.current_epoch().unwrap(), 45);
        assert!(store.published_lsn().unwrap() >= last);
        for lsn in 1..=last {
            assert_eq!(store.epoch_at_lsn(lsn).unwrap(), Some(45));
        }
    }
}
