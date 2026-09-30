use super::*;
use std::collections::BTreeMap;

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
    let row: PendingBlobOps = bcs::from_bytes(&row).unwrap();
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
        let row: PendingBlobOps = bcs::from_bytes(&merge(None, edits)).unwrap();
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

#[derive(Default, Clone)]
struct State {
    revision: Revision,
    blobs: BTreeMap<Vec<u8>, PendingBlobOps>,
    barriers: BTreeMap<Revision, EpochBarrier>,
}

#[derive(Default)]
struct Memory {
    state: Mutex<State>,
    // Inject a write during the sync call to distinguish snapshot-before-sync from sync-before-scan.
    inject_on_sync: Mutex<bool>,
    fail_sync: Mutex<bool>,
}

#[derive(Default)]
struct Write {
    edits: Vec<(Vec<u8>, Vec<u8>)>,
    barriers: Vec<(Revision, EpochBarrier)>,
    revision: Revision,
}

impl QueueWrite for Write {
    type Error = Error;
    fn merge_blob(&mut self, key: &[u8], operand: &[u8]) -> Result<(), Error> {
        self.edits.push((key.to_vec(), operand.to_vec()));
        Ok(())
    }
    fn put_barrier(&mut self, revision: Revision, barrier: &EpochBarrier) -> Result<(), Error> {
        self.barriers.push((revision, barrier.clone()));
        Ok(())
    }
    fn set_last_revision(&mut self, revision: Revision) -> Result<(), Error> {
        self.revision = revision;
        Ok(())
    }
}

impl QueueStorage for Memory {
    type Error = Error;
    type Write = Write;
    type Snapshot<'a> = State;
    fn last_revision(&self) -> Result<Revision, Error> {
        Ok(self.state.lock().unwrap().revision)
    }
    fn batch(&self) -> Write {
        Write::default()
    }
    fn commit(&self, batch: Write) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        for (key, operand) in batch.edits {
            let base = state.blobs.get(&key).map(bcs::to_bytes).transpose()?;
            let merged = merge_pending(base.as_deref(), [operand.as_slice()])?;
            state.blobs.insert(key, bcs::from_bytes(&merged)?);
        }
        state.barriers.extend(batch.barriers);
        state.revision = batch.revision;
        Ok(())
    }
    fn snapshot(&self) -> Result<State, Error> {
        Ok(self.state.lock().unwrap().clone())
    }
    fn sync(&self) -> Result<(), Error> {
        if *self.fail_sync.lock().unwrap() {
            return Err(Error::Poisoned);
        }
        if *self.inject_on_sync.lock().unwrap() {
            self.state.lock().unwrap().blobs.insert(
                b"later".to_vec(),
                PendingBlobOps::V1(vec![command(99, delete(true))]),
            );
        }
        Ok(())
    }
}

impl QueueSnapshot for State {
    type Error = Error;
    fn blobs(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingBlobOps)>, Error> {
        Ok(self
            .blobs
            .iter()
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn barriers(
        &self,
        after: Option<Revision>,
        limit: usize,
    ) -> Result<Vec<(Revision, EpochBarrier)>, Error> {
        Ok(self
            .barriers
            .iter()
            .filter(|(revision, _)| after.is_none_or(|after| **revision > after))
            .take(limit)
            .map(|(k, v)| (*k, v.clone()))
            .collect())
    }
}

#[test]
fn durable_snapshot_excludes_writes_that_arrive_during_sync() {
    let queue = PendingQueue::new(Memory::default());
    queue
        .write_batch(|b| b.register(b"first", 10, vec![]))
        .unwrap();
    *queue.storage.inject_on_sync.lock().unwrap() = true;
    let snapshot = queue.durable_snapshot().unwrap();
    assert_eq!(snapshot.blobs(None, 10).unwrap().len(), 1);
    assert_eq!(queue.storage.state.lock().unwrap().blobs.len(), 2);
    assert_eq!(
        queue
            .durable_snapshot()
            .unwrap()
            .blobs(None, 10)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn failed_sync_never_returns_an_applicable_view() {
    let queue = PendingQueue::new(Memory::default());
    queue
        .write_batch(|b| b.append(b"a", delete(true), vec![]))
        .unwrap();
    *queue.storage.fail_sync.lock().unwrap() = true;
    assert!(queue.durable_snapshot().is_err());
}

#[test]
fn aborted_batch_does_not_publish_commands_or_consume_revisions() {
    let queue = PendingQueue::new(Memory::default());
    let result: Result<(), Error> = queue.write_batch(|b| {
        b.register(b"a", 15, vec![])?;
        b.advance_epoch(10, vec![])?;
        Err(Error::Poisoned)
    });
    assert!(result.is_err());
    let snapshot = queue.durable_snapshot().unwrap();
    assert!(snapshot.blobs(None, 10).unwrap().is_empty());
    assert!(snapshot.barriers(None, 10).unwrap().is_empty());
    assert_eq!(
        queue.write_batch(|b| b.register(b"a", 15, vec![])).unwrap(),
        Revision(1)
    );
}
