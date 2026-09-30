use super::*;
use crate::port::{
    RocksBackend,
    codec::{decode_value, encode_key, encode_value},
};
use tempfile::tempdir;

fn command(revision: u64, operation: BlobOperation) -> BlobCommand {
    BlobCommand {
        revision: Revision(revision),
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
            BlobEdit::Append(command(1, delete(true))),
            BlobEdit::Append(command(2, delete(false))),
        ],
    );
    let lifetime = command(3, BlobOperation::SetLifetime { end_epoch: 15 });
    let later_delete = command(4, delete(true));
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
        &[command(2, delete(false)), lifetime, later_delete]
    );
}

#[test]
fn acknowledgement_preserves_newer_appends_on_either_side_of_the_merge() {
    let first = command(7, BlobOperation::SetLifetime { end_epoch: 15 });
    let next = command(8, BlobOperation::SetLifetime { end_epoch: 20 });
    for edits in [
        vec![
            BlobEdit::Append(first.clone()),
            BlobEdit::Append(next.clone()),
            BlobEdit::Acknowledge {
                through: Revision(7),
            },
        ],
        vec![
            BlobEdit::Append(first.clone()),
            BlobEdit::Acknowledge {
                through: Revision(7),
            },
            BlobEdit::Append(next.clone()),
        ],
    ] {
        let row: PendingBlobOps = decode_value(&merge(None, edits)).unwrap();
        assert_eq!(row.commands(), std::slice::from_ref(&next));
    }
}

#[test]
fn malformed_data_and_invalid_revision_order_are_errors() {
    assert!(merge_pending(Some(&[255]), []).is_err());
    assert!(merge_pending(None, [&[255][..]]).is_err());
    let encoded = BlobOperand::V1(BlobEdit::Append(command(0, delete(true))))
        .encode()
        .unwrap();
    assert!(merge_pending(None, [encoded.as_slice()]).is_err());
    let encoded = BlobOperand::V1(BlobEdit::Append(command(1, delete(true))))
        .encode()
        .unwrap();
    assert!(merge_pending(None, [encoded.as_slice(), encoded.as_slice()]).is_err());
    let bad_registration = BlobOperand::V1(BlobEdit::Register(command(2, delete(true))))
        .encode()
        .unwrap();
    assert!(merge_pending(None, [bad_registration.as_slice()]).is_err());
    assert!(Revision(u64::MAX).next().is_err());
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
fn metadata_and_queue_abort_or_commit_together() -> Result<()> {
    let dir = tempdir().unwrap();
    let (db, queue) = open(dir.path());
    let result: Result<()> = queue.write_batch(|batch| {
        batch.register(b"a", 15, vec![])?;
        batch.advance_epoch(10, vec![])?;
        batch.metadata().put("application", b"progress", b"42")?;
        Err(Error::InvalidPendingOperation("abort".into()))
    });
    assert!(result.is_err());
    assert_eq!(db.get("application", b"progress")?, None);
    assert!(queue.durable_snapshot()?.blobs()?.next().is_none());
    assert!(queue.durable_snapshot()?.barriers()?.next().is_none());
    assert_eq!(
        queue.write_batch(|batch| {
            batch.metadata().put("application", b"progress", b"42")?;
            batch.register(b"a", 15, vec![])
        })?,
        Revision(1)
    );
    assert_eq!(db.get("application", b"progress")?, Some(b"42".to_vec()));
    Ok(())
}

#[test]
fn snapshots_stream_stable_rows_and_numeric_epoch_barriers() -> Result<()> {
    let dir = tempdir().unwrap();
    let (db, queue) = open(dir.path());
    db.put(LAST_REVISION_CF, &[], &encode_value(&Revision(253))?)?;
    queue.write_batch(|batch| {
        batch.register(b"a", 15, vec![])?;
        batch.advance_epoch(10, vec![])?;
        batch.advance_epoch(11, vec![])?;
        batch.register(b"b", 20, vec![])
    })?;
    let view = queue.durable_snapshot()?;
    queue.write_batch(|batch| {
        batch.append(b"a", delete(true), vec![])?;
        batch.register(b"c", 25, vec![])
    })?;
    let mut rows = view.blobs()?;
    let a = rows.next().unwrap()?;
    assert_eq!(a.0, b"a");
    assert_eq!(a.1.commands().len(), 1);
    assert_eq!(rows.next().unwrap()?.0, b"b");
    assert!(rows.next().is_none());
    assert_eq!(
        view.barriers()?
            .map(|r| r.map(|(revision, _)| revision))
            .collect::<Result<Vec<_>>>()?,
        vec![Revision(255), Revision(256)]
    );
    assert_eq!(queue.durable_snapshot()?.blobs()?.count(), 3);
    Ok(())
}

#[test]
fn merged_rows_and_allocator_survive_reopen_and_cleanup() -> Result<()> {
    let dir = tempdir().unwrap();
    {
        let (db, queue) = open(dir.path());
        queue.write_batch(|batch| {
            batch.append(b"a", delete(true), vec![])?;
            batch.register(b"a", 15, vec![])?;
            batch.append(b"b", delete(true), vec![])
        })?;
        let key = encode_key(b"b".as_slice())?;
        // Model cleanup under the worker's blob lock; the allocator is retained.
        db.delete(PENDING_BLOBS_CF, &key)?;
        drop(queue.durable_snapshot()?);
    }
    let (_, queue) = open(dir.path());
    let rows = queue
        .durable_snapshot()?
        .blobs()?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.commands()[0].revision, Revision(2));
    assert_eq!(
        queue.write_batch(|batch| batch.register(b"b", 20, vec![]))?,
        Revision(4)
    );
    Ok(())
}
