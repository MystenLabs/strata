use super::*;
use index::port::{
    RocksBackend,
    codec::{decode_value, encode_key},
};
use tempfile::tempdir;

fn command(event_index: u64, operation: BlobOperation) -> BlobCommand {
    BlobCommand {
        event_index,
        source: vec![],
        operation,
    }
}

fn delete(cancellable: bool) -> BlobOperation {
    BlobOperation::Delete {
        shards: vec![ShardGeneration {
            shard: 2,
            generation: 3,
        }],
        cancellable,
    }
}

fn merge(base: Option<&[u8]>, edits: Vec<BlobEdit>) -> Vec<u8> {
    let operands: Vec<_> = edits
        .into_iter()
        .map(|e| BlobOperand::V1(e).encode().unwrap())
        .collect();
    merge_pending(base, operands.iter().map(Vec::as_slice)).unwrap()
}

#[test]
fn registration_cancels_only_earlier_cancellable_deletes() {
    let base = merge(
        None,
        vec![
            BlobEdit::Append(command(10, delete(true))),
            BlobEdit::Append(command(20, delete(false))),
        ],
    );
    let lifetime = command(30, BlobOperation::SetLifetime { end_epoch: 15 });
    let later_delete = command(40, delete(true));
    let row = merge(
        Some(&base),
        vec![
            BlobEdit::Register(lifetime.clone()),
            BlobEdit::Append(later_delete.clone()),
        ],
    );
    let row: PendingBlobOps = decode_value(&row).unwrap();
    assert_eq!(
        row.commands(),
        &[command(20, delete(false)), lifetime, later_delete]
    );
}

#[test]
fn acknowledgement_preserves_newer_appends_on_either_side_of_the_merge() {
    let first = command(0, BlobOperation::SetLifetime { end_epoch: 15 });
    let next = command(8, BlobOperation::SetLifetime { end_epoch: 20 });
    for edits in [
        vec![
            BlobEdit::Append(first.clone()),
            BlobEdit::Append(next.clone()),
            BlobEdit::Acknowledge {
                through_event_index: 0,
            },
        ],
        vec![
            BlobEdit::Append(first.clone()),
            BlobEdit::Acknowledge {
                through_event_index: 0,
            },
            BlobEdit::Append(next.clone()),
        ],
    ] {
        let row: PendingBlobOps = decode_value(&merge(None, edits)).unwrap();
        assert_eq!(row.commands(), std::slice::from_ref(&next));
    }
}

#[test]
fn malformed_data_and_repeated_or_out_of_order_events_are_errors() {
    assert!(merge_pending(Some(&[255]), []).is_err());
    assert!(merge_pending(None, [&[255][..]]).is_err());
    let encoded = BlobOperand::V1(BlobEdit::Append(command(0, delete(true))))
        .encode()
        .unwrap();
    // Walrus must filter retries using the metadata committed alongside the queue write.
    assert!(merge_pending(None, [encoded.as_slice(), encoded.as_slice()]).is_err());
    let later = BlobOperand::V1(BlobEdit::Append(command(7, delete(true))))
        .encode()
        .unwrap();
    assert!(merge_pending(None, [later.as_slice(), encoded.as_slice()]).is_err());
    let bad_registration = BlobOperand::V1(BlobEdit::Register(command(2, delete(true))))
        .encode()
        .unwrap();
    assert!(merge_pending(None, [bad_registration.as_slice()]).is_err());
}

#[test]
fn event_indexes_include_zero_and_max_without_allocation() {
    let first = command(0, BlobOperation::SetLifetime { end_epoch: 15 });
    let last = command(u64::MAX, delete(true));
    let row = merge(
        None,
        vec![
            BlobEdit::Register(first.clone()),
            BlobEdit::Append(last.clone()),
        ],
    );
    let decoded: PendingBlobOps = decode_value(&row).unwrap();
    assert_eq!(decoded.commands(), &[first, last]);
    let acknowledged: PendingBlobOps = decode_value(&merge(
        Some(&row),
        vec![BlobEdit::Acknowledge {
            through_event_index: u64::MAX,
        }],
    ))
    .unwrap();
    assert!(acknowledged.commands().is_empty());
}

fn open(path: &std::path::Path) -> (Arc<RocksBackend>, PendingQueue) {
    let options = cf_options(rocksdb::Options::default())
        .into_iter()
        .map(|(name, options)| (name.to_owned(), options))
        .chain([("application".into(), rocksdb::Options::default())])
        .collect::<Vec<_>>();
    let db = Arc::new(RocksBackend::open(path, None, &options).unwrap());
    let queue = PendingQueue::new(db.clone());
    (db, queue)
}

#[test]
fn metadata_and_event_fanout_abort_or_commit_together() -> Result<()> {
    let dir = tempdir().unwrap();
    let (db, queue) = open(dir.path());
    let stage = |batch: &mut PendingBatch| {
        // One source event can update several blobs and publish a barrier in the same batch.
        batch.register(b"a", 0, 15, vec![])?;
        batch.register(b"b", 0, 15, vec![])?;
        batch.advance_epoch(0, 10, vec![])?;
        batch.metadata().put("application", b"event_index", b"0")?;
        Ok(())
    };
    let result: Result<()> = queue.write_batch(|batch| {
        stage(batch)?;
        Err(Error::InvalidPendingOperation("abort".into()))
    });
    assert!(result.is_err());
    assert_eq!(db.get("application", b"event_index")?, None);
    let snapshot = queue.durable_snapshot()?;
    assert!(queue.blobs(snapshot.as_ref())?.next().is_none());
    assert!(queue.barriers(snapshot.as_ref())?.next().is_none());
    queue.write_batch(stage)?;
    assert_eq!(db.get("application", b"event_index")?, Some(b"0".to_vec()));
    let snapshot = queue.durable_snapshot()?;
    let rows = queue
        .blobs(snapshot.as_ref())?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|(_, row)| row.commands()[0].event_index == 0)
    );
    assert_eq!(queue.barriers(snapshot.as_ref())?.next().unwrap()?.0, 0);
    Ok(())
}

#[test]
fn snapshots_stream_stable_rows_and_numeric_epoch_barriers() -> Result<()> {
    let dir = tempdir().unwrap();
    let (_, queue) = open(dir.path());
    queue.write_batch(|batch| {
        batch.register(b"a", 254, 15, vec![])?;
        batch.advance_epoch(255, 10, vec![])?;
        batch.advance_epoch(256, 11, vec![])?;
        batch.register(b"b", 257, 20, vec![])
    })?;
    let snapshot = queue.durable_snapshot()?;
    let mut rows = queue.blobs(snapshot.as_ref())?;
    // The later barrier scan must share the blob iterator's view despite intervening writes.
    queue.write_batch(|batch| {
        batch.append(b"a", 258, delete(true), vec![])?;
        batch.register(b"c", 259, 25, vec![])?;
        batch.advance_epoch(260, 12, vec![])
    })?;
    let a = rows.next().unwrap()?;
    assert_eq!(a.0, b"a");
    assert_eq!(a.1.commands().len(), 1);
    assert_eq!(rows.next().unwrap()?.0, b"b");
    assert!(rows.next().is_none());
    assert_eq!(
        queue
            .barriers(snapshot.as_ref())?
            .map(|r| r.map(|(event_index, _)| event_index))
            .collect::<Result<Vec<_>>>()?,
        vec![255, 256]
    );
    let latest = queue.durable_snapshot()?;
    assert_eq!(queue.blobs(latest.as_ref())?.count(), 3);
    assert_eq!(queue.barriers(latest.as_ref())?.count(), 3);
    Ok(())
}

#[test]
fn caller_event_indexes_survive_reopen_and_row_cleanup() -> Result<()> {
    let dir = tempdir().unwrap();
    {
        let (db, queue) = open(dir.path());
        queue.write_batch(|batch| {
            batch.append(b"a", 10, delete(true), vec![])?;
            batch.register(b"a", 20, 15, vec![])?;
            batch.register(b"b", 20, 15, vec![])
        })?;
        // Model cleanup under the worker's blob lock after Strata effects are durable.
        db.delete(PENDING_BLOBS_CF, &encode_key(b"b".as_slice())?)?;
        drop(queue.durable_snapshot()?);
    }
    let (_, queue) = open(dir.path());
    let snapshot = queue.durable_snapshot()?;
    let rows = queue
        .blobs(snapshot.as_ref())?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.commands()[0].event_index, 20);
    queue.write_batch(|batch| batch.register(b"b", 50, 20, vec![]))?;
    let latest = queue.durable_snapshot()?;
    let rows = queue.blobs(latest.as_ref())?.collect::<Result<Vec<_>>>()?;
    assert_eq!(rows[1].1.commands()[0].event_index, 50);
    Ok(())
}
