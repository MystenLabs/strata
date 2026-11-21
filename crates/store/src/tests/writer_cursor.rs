//! The writer keeps its frontier (next LSN, active segment extent) in memory and in the cursor
//! file, and writes the index rows only with batches that need the index anyway.

use super::*;
use crate::writer_cursor::{WriterCursor, WriterCursorRecord, writer_cursor_path};

#[tokio::test]
async fn ordinary_writes_reach_the_index_rows_only_at_publication() {
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    let opened_at = store.index().get_next_lsn().unwrap();

    store.put(&key, b"payload").unwrap();
    store.tombstone(&key).unwrap();
    let lsn = store.put(&key, b"payload again").unwrap();

    assert_eq!(store.next_lsn(), lsn + 1);
    let index_next_lsn = store.index().get_next_lsn().unwrap();
    let row = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    // The rows were read before any publication, unless the one-second timer fired meanwhile.
    if store.published_lsn().unwrap() == 0 {
        assert_eq!(index_next_lsn, opened_at);
        assert_eq!(row.write_offset, 0);
        assert_eq!(row.max_lsn, None);
    }
    let committed = store
        .committed_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert!(committed.write_offset > 0);
    assert_eq!(committed.max_lsn, Some(lsn));

    store.sync().unwrap();

    assert_eq!(store.published_lsn().unwrap(), lsn);
    assert_eq!(store.index().get_next_lsn().unwrap(), lsn + 1);
    let row = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(row.write_offset, committed.write_offset);
    assert_eq!(row.durable_offset, committed.write_offset);
    assert_eq!(row.max_lsn, Some(lsn));
}

#[tokio::test]
async fn unpublished_writes_survive_a_reopen() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let keys = (0..5)
        .map(|i| BlobKey::new(format!("blob-{i}").into_bytes()).unwrap())
        .collect::<Vec<_>>();
    let last_lsn;
    {
        // Dropping the store stops the writer without a final publication, which leaves the
        // files as a process crash between two commits would.
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        for key in &keys {
            store.put(key, key.as_bytes()).unwrap();
        }
        last_lsn = store.next_lsn() - 1;
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.next_lsn(), last_lsn + 1);
    assert_eq!(store.index().get_next_lsn().unwrap(), last_lsn + 1);
    for key in &keys {
        assert_eq!(store.get(key).unwrap(), Some(key.as_bytes().to_vec()));
    }
    assert_eq!(store.put(&keys[0], b"again").unwrap(), last_lsn + 1);
}

#[tokio::test]
async fn writes_after_an_index_commit_are_recovered_on_top_of_it() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let before = BlobKey::new(b"before".to_vec()).unwrap();
    let after = BlobKey::new(b"after".to_vec()).unwrap();
    let epoch;
    let last_lsn;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&before, b"one").unwrap();
        // An epoch change is an index batch: it moves the `NextLsn` row, and the cursor records
        // written after it are based on the new value.
        let (new_epoch, epoch_lsn) = store.increment_epoch().unwrap();
        epoch = new_epoch;
        assert_eq!(store.index().get_next_lsn().unwrap(), epoch_lsn + 1);
        store.put(&after, b"two").unwrap();
        last_lsn = store.tombstone(&before).unwrap();
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.next_lsn(), last_lsn + 1);
    assert_eq!(store.index().get_current_epoch().unwrap(), Some(epoch));
    assert_eq!(store.get(&before).unwrap(), None);
    assert_eq!(store.get(&after).unwrap(), Some(b"two".to_vec()));
}

#[tokio::test]
async fn a_cursor_based_on_a_frontier_the_index_does_not_hold_is_ignored() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload").unwrap(), 1);
    }
    // A record names the `NextLsn` row it was written on top of. After a power loss the index
    // can come back without that row's last update; a record like that describes a history the
    // index no longer has.
    let path = writer_cursor_path(&cfg);
    let record = WriterCursor::read(&path).unwrap().unwrap();
    assert_eq!(record.next_lsn, 2);
    WriterCursor::open(path)
        .unwrap()
        .write(&WriterCursorRecord {
            index_next_lsn: record.index_next_lsn + 7,
            ..record
        })
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    // Recovery went by the index rows alone, as it does after any power loss: the write was
    // never published, so it is rolled back.
    assert_eq!(store.next_lsn(), 1);
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.put(&key, b"again").unwrap(), 1);
    assert_eq!(store.get(&key).unwrap(), Some(b"again".to_vec()));
}
