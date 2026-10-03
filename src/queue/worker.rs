//! Durable work selection, Strata application, and grouped acknowledgement.

use std::{sync::Arc, time::Duration};

use tokio::{runtime::Handle, sync::watch};

use crate::{BlobKey, ShardId, StrataStore};

use super::{replay::blob_lsn_key_rocksdb, *};

#[derive(Debug, Clone, Copy)]
pub struct WorkerConfig {
    /// At most this many blobs per pass, with one front command per blob. All submissions in
    /// the group share one Strata sync and one synced RocksDB acknowledgement.
    pub max_blobs_per_batch: usize,
    /// Delay between passes when no work is available. A backlog is drained without sleeping.
    pub poll_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_blobs_per_batch: 128,
            poll_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkerProgress {
    pub acknowledged_blobs: usize,
    pub acknowledged_epochs: usize,
    /// Selected commands cancelled before the worker acquired their blob locks. Retry selection
    /// with a fresh durable snapshot; never substitute a newer, possibly unsynced command.
    pub cancelled_blobs: usize,
}

impl WorkerProgress {
    pub fn is_idle(&self) -> bool {
        *self == Self::default()
    }
}

/// Applies lifecycle work above the storage engine. The application supplies only a stable,
/// deterministic mapping from a queue blob ID to its physical keys (e.g. both Walrus slivers).
/// The worker builds the batches, serializes passes across queue clones, and holds the blob or
/// lifecycle guards through Strata sync and the synced RocksDB acknowledgement.
///
/// Drain recovered work before admitting foreground writes. All producers/puts/shard changes
/// must use this queue's coordination and obey its event-ordering contract. On error the queue
/// rejects further guarded work; the supervisor must stop serving and reopen/recover the store.
/// Direct store calls do not participate in that admission gate.
#[derive(Clone)]
pub struct QueueWorker {
    queue: PendingQueue,
    store: Arc<StrataStore>,
    physical_keys: fn(&[u8]) -> Result<Vec<BlobKey>>,
    config: WorkerConfig,
}

enum SelectedWork {
    Blobs(Vec<(Vec<u8>, BlobCommand)>),
    Epoch(u64),
    Idle,
}

// Drop runs before these fields are released, including during unwinding. A failed/panicked pass
// must close admission BEFORE releasing its locks; otherwise a new put could pass pending work.
struct PassGuards {
    queue: PendingQueue,
    blobs: Option<LockedBlobs>,
    lifecycle: Option<LifecycleGuard>,
    finished: bool,
}

impl Drop for PassGuards {
    fn drop(&mut self) {
        if !self.finished {
            self.queue.halt("worker pass did not complete".into());
        }
    }
}

impl QueueWorker {
    pub fn new(
        queue: PendingQueue,
        store: Arc<StrataStore>,
        physical_keys: fn(&[u8]) -> Result<Vec<BlobKey>>,
        config: WorkerConfig,
    ) -> Result<Self> {
        queue.check_shared_rocksdb(&store)?;
        if config.max_blobs_per_batch == 0 || config.poll_interval.is_zero() {
            return Err(Error::InvalidPendingOperation(
                "worker batch size and poll interval must be positive".into(),
            ));
        }
        Ok(Self {
            queue,
            store,
            physical_keys,
            config,
        })
    }

    /// Finish one bounded group, or one epoch barrier after its preceding work has drained.
    /// Blocking I/O runs off the async runtime. Once scheduled, the entire pass continues even
    /// if the caller drops this future; cancellation must not abandon submitted work and unlock.
    /// Use `run`'s shutdown signal and await it for a graceful stop.
    pub async fn process_batch(&self) -> Result<WorkerProgress> {
        let worker = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut resume_after =
                Handle::current().block_on(worker.queue.coordination.worker.lock());
            worker.queue.check_running()?;
            let mut guards = PassGuards {
                queue: worker.queue.clone(),
                blobs: None,
                lifecycle: None,
                finished: false,
            };
            let result = worker.process(&mut guards, &mut resume_after);
            if let Err(error) = &result {
                worker.queue.halt(error.to_string());
            }
            guards.finished = result.is_ok();
            result
        })
        .await
        .map_err(|error| {
            let reason = error.to_string();
            self.queue.halt(reason.clone());
            Error::WorkerHalted { reason }
        })?
    }

    /// Poll until signalled or the sender is dropped. Shutdown finishes an in-flight group,
    /// including both durability barriers, before returning. Propagate errors to a supervisor;
    /// do not restart this worker against the same open database after an error.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() || shutdown.has_changed().is_err() {
                return Ok(());
            }
            if self.process_batch().await?.is_idle() {
                tokio::select! {
                    _ = tokio::time::sleep(self.config.poll_interval) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn select(&self, resume_after: &mut Option<Vec<u8>>) -> Result<SelectedWork> {
        // This captures the view BEFORE syncing. Scanning a latest iterator after the sync would
        // admit newer visible operations whose application metadata might not survive a crash.
        let snapshot = self.queue.durable_snapshot()?;
        let barrier = self
            .queue
            .barriers(snapshot.as_ref())?
            .next()
            .transpose()?
            .map(|(index, _)| index);
        let mut selected = Vec::new();
        // Scan after the last group's key, then wrap once. The port currently supports full
        // scans only. Compare encoded keys because RocksDB's order need not match raw blob IDs.
        'scan: for wrapped in [false, true] {
            if wrapped && resume_after.is_none() {
                break;
            }
            for row in self.queue.blobs(snapshot.as_ref())? {
                let (key, pending) = row?;
                let after_cursor = match resume_after.as_ref() {
                    Some(cursor) => encode_key(&key)? > *cursor,
                    None => true,
                };
                if after_cursor == wrapped {
                    continue;
                }
                if let Some(command) = pending.commands().first()
                    && barrier.is_none_or(|index| command.event_index <= index)
                {
                    selected.push((key, command.clone()));
                    if selected.len() == self.config.max_blobs_per_batch {
                        break 'scan;
                    }
                }
            }
        }
        if let Some((last, _)) = selected.last() {
            *resume_after = Some(encode_key(last)?);
            Ok(SelectedWork::Blobs(selected))
        } else if let Some(index) = barrier {
            // Producers finish all <= index fan-out before publishing this immutable barrier,
            // and publish it before higher-index work. No earlier work can arrive after this scan.
            Ok(SelectedWork::Epoch(index))
        } else {
            Ok(SelectedWork::Idle)
        }
    }

    fn process(
        &self,
        guards: &mut PassGuards,
        resume_after: &mut Option<Vec<u8>>,
    ) -> Result<WorkerProgress> {
        match self.select(resume_after)? {
            SelectedWork::Idle => Ok(WorkerProgress::default()),
            SelectedWork::Blobs(selected) => {
                let keys: Vec<_> = selected.iter().map(|(key, _)| key.as_slice()).collect();
                guards.blobs = Some(Handle::current().block_on(self.queue.lock_blobs(&keys))?);
                self.apply_blobs(guards.blobs.as_ref().unwrap(), &selected)
            }
            SelectedWork::Epoch(index) => {
                guards.lifecycle = Some(Handle::current().block_on(self.queue.lock_lifecycle())?);
                let guard = guards.lifecycle.as_ref().unwrap();
                self.queue.submit_epoch_strata(guard, &self.store, index)?;
                self.store.sync()?;
                self.queue
                    .acknowledge_epoch_rocksdb(guard, &self.store, index)?;
                Ok(WorkerProgress {
                    acknowledged_epochs: 1,
                    ..WorkerProgress::default()
                })
            }
        }
    }

    fn apply_blobs(
        &self,
        guard: &LockedBlobs,
        selected: &[(Vec<u8>, BlobCommand)],
    ) -> Result<WorkerProgress> {
        let mut progress = WorkerProgress::default();
        let mut submitted = Vec::new();
        let mut retired = Vec::new();
        for (key, command) in selected {
            self.queue.check_blob_lock(guard, key)?;
            let latest = self.queue.blobs.get(key)?.unwrap_or_default();
            if latest.commands().first() != Some(command) {
                progress.cancelled_blobs += 1;
                continue;
            }
            let lsn_key = blob_lsn_key_rocksdb(key, command.event_index)?;
            let has_submission = if self
                .store
                .index()
                .submitted_batch_lsns()
                .get(&lsn_key)?
                .is_some()
            {
                // Recovery retained this exact submission. Do not resolve or resubmit its keys.
                true
            } else {
                self.apply_blob(key, command, lsn_key)?
            };
            if has_submission {
                submitted.push((key.as_slice(), command.event_index));
            } else {
                retired.push((key.as_slice(), command.event_index));
            }
        }
        if submitted.is_empty() && retired.is_empty() {
            return Ok(progress);
        }

        // Submissions are sequential and return at visibility; the entire group shares this
        // durability barrier. It also persists shard retirement before acknowledging skipped
        // deletes that require no tombstones. Never invent an empty batch/LSN for those deletes.
        self.store.sync()?;
        // Keep empty rows: deleting a whole row based on the selected snapshot could lose a
        // concurrent newer append. The acknowledgement merge trims only this completed prefix.
        if retired.is_empty() {
            self.queue
                .acknowledge_blobs_rocksdb(guard, &self.store, &submitted)?;
        } else {
            self.queue
                .acknowledge_blob_group_rocksdb(guard, &self.store, &submitted, &retired)?;
        }
        progress.acknowledged_blobs = submitted.len() + retired.len();
        Ok(progress)
    }

    /// Returns false only when all targeted shard generations are retired (or the set is empty).
    fn apply_blob(&self, key: &[u8], command: &BlobCommand, lsn_key: Vec<u8>) -> Result<bool> {
        let mut shards = Vec::new();
        if let BlobOperation::Delete {
            shards: expected, ..
        } = &command.operation
        {
            for expected in expected {
                let shard_id: ShardId = expected.shard.try_into().map_err(|_| {
                    Error::InvalidPendingOperation(format!("invalid shard id {}", expected.shard))
                })?;
                let current = self
                    .store
                    .shard_info(shard_id)?
                    .ok_or(store::Error::ShardNotFound { shard_id })?;
                if current.current_generation < expected.generation {
                    return Err(store::Error::ShardUnavailable {
                        shard_id,
                        generation: expected.generation,
                        current_generation: current.current_generation,
                        state: current.state,
                    }
                    .into());
                }
                if current.current_generation == expected.generation && current.is_active() {
                    shards.push(shard_id);
                }
                // A newer generation or a dropped target proves the old generation is retired.
                // Missing/regressed registry state does not: fail above instead of losing work.
            }
            if shards.is_empty() {
                return Ok(false);
            }
        }
        let keys = (self.physical_keys)(key)?;
        if keys.is_empty() {
            return Err(Error::InvalidPendingOperation(
                "blob has no physical keys".into(),
            ));
        }
        let mut batch = self.store.batch();
        for key in keys {
            match command.operation {
                BlobOperation::SetLifetime { end_epoch } => {
                    batch.set_blob_lifetime(key, end_epoch);
                }
                BlobOperation::Delete { .. } => {
                    for &shard in &shards {
                        batch.tombstone(shard, key.clone());
                    }
                }
            }
        }
        batch.write_with_lsn(lsn_key)?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
