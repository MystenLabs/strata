use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    fs,
    io::Read,
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

use sha2::{Digest, Sha256};
use strata_accounting::{ActiveDeltaLog, ActiveDeltaLogState};
use strata_core::{
    BlobKey, MapRefOp, SegmentFileState, SegmentId, SegmentOwner, SegmentState, StrataLsn,
};
use strata_index::StrataIndex;

use crate::{
    Error, Result, SealedSegmentIntegrityPolicy, StoreHalt, StrataStoreConfig, StrataStoreMetrics,
    accounting::AccountingCommand,
    active_segment_state_from_path,
    layout::{segment_path, segment_state_path},
    unsealed_ingest_segment_count, unsealed_ingest_segment_ids,
};

const SEAL_COMMAND_RECV_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(crate) enum SealCommand {
    Seal(SegmentSealTask),
    Shutdown,
}

#[derive(Debug)]
enum SealPublisherEvent {
    Queued(SegmentId),
    Completed(SealTaskResult),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentSealTask {
    pub(crate) segment_id: SegmentId,
    pub(crate) sealed_len: u64,
    pub(crate) sealed_before_lsn: StrataLsn,
}

#[derive(Debug)]
pub(crate) struct SealWorker {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) ingest_owner: SegmentOwner,
    pub(crate) seal_rx: mpsc::Receiver<SealCommand>,
    pub(crate) durability_publish_lock: Arc<Mutex<()>>,
    pub(crate) accounting_tx: Option<mpsc::SyncSender<AccountingCommand>>,
    pub(crate) metrics: StrataStoreMetrics,
    pub(crate) store_halt: StoreHalt,
}

impl SealWorker {
    pub(crate) fn run(self) {
        let SealWorker {
            config,
            index,
            ingest_owner,
            seal_rx,
            durability_publish_lock,
            accounting_tx,
            metrics,
            store_halt,
        } = self;

        let (task_tx, task_rx) = mpsc::channel();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let (publisher_tx, publisher_rx) = mpsc::channel();
        let mut worker_handles = Vec::with_capacity(config.seal_worker_count);
        for worker_id in 0..config.seal_worker_count {
            let worker = SealTaskWorker {
                config: config.clone(),
                index: index.clone(),
                task_rx: Arc::clone(&task_rx),
                publisher_tx: publisher_tx.clone(),
            };
            match thread::Builder::new()
                .name(format!(
                    "strata-seal-worker-{}-{worker_id}",
                    config.namespace
                ))
                .spawn(move || worker.run())
            {
                Ok(handle) => worker_handles.push(handle),
                Err(source) => {
                    store_halt.halt(format!("fatal strata seal worker spawn error: {source}"));
                    drop(task_tx);
                    drop(publisher_tx);
                    join_seal_threads(worker_handles);
                    return;
                }
            }
        }
        let publisher = SealPublisher {
            config: config.clone(),
            index,
            ingest_owner,
            event_rx: publisher_rx,
            durability_publish_lock,
            accounting_tx,
            metrics,
            store_halt: store_halt.clone(),
        };
        let publisher_handle = match thread::Builder::new()
            .name(format!("strata-seal-publisher-{}", config.namespace))
            .spawn(move || publisher.run())
        {
            Ok(handle) => handle,
            Err(source) => {
                store_halt.halt(format!("fatal strata seal publisher spawn error: {source}"));
                drop(task_tx);
                join_seal_threads(worker_handles);
                return;
            }
        };

        dispatch_seal_commands(seal_rx, task_tx, publisher_tx, &store_halt);
        join_seal_threads(worker_handles);
        let _ = publisher_handle.join();
    }

    #[cfg(test)]
    pub(crate) fn seal_segment(&self, task: SegmentSealTask) -> Result<()> {
        let Some(completed) = prepare_seal_segment(&self.config, &self.index, task)? else {
            return Ok(());
        };
        publish_sealed_segment(
            &self.config,
            &self.index,
            self.ingest_owner,
            &self.durability_publish_lock,
            self.accounting_tx.as_ref(),
            &self.metrics,
            completed,
        )
    }
}

#[derive(Debug)]
pub(crate) struct CompletedSeal {
    pub(crate) task: SegmentSealTask,
    pub(crate) sealed_sha256: Option<[u8; 32]>,
    pub(crate) active_delta_state: ActiveDeltaLogState,
}

#[derive(Debug)]
struct SealTaskResult {
    task: SegmentSealTask,
    result: Result<Option<CompletedSeal>>,
}

#[derive(Debug)]
struct SealTaskWorker {
    config: StrataStoreConfig,
    index: StrataIndex,
    task_rx: Arc<Mutex<mpsc::Receiver<SegmentSealTask>>>,
    publisher_tx: mpsc::Sender<SealPublisherEvent>,
}

impl SealTaskWorker {
    fn run(self) {
        while let Some(task) = self.recv_task() {
            let result = prepare_seal_segment(&self.config, &self.index, task);
            let is_error = result.is_err();
            if self
                .publisher_tx
                .send(SealPublisherEvent::Completed(SealTaskResult {
                    task,
                    result,
                }))
                .is_err()
            {
                break;
            }
            if is_error {
                break;
            }
        }
    }

    fn recv_task(&self) -> Option<SegmentSealTask> {
        self.task_rx
            .lock()
            .expect("seal task receiver lock poisoned")
            .recv()
            .ok()
    }
}

#[derive(Debug)]
struct SealPublisher {
    config: StrataStoreConfig,
    index: StrataIndex,
    ingest_owner: SegmentOwner,
    event_rx: mpsc::Receiver<SealPublisherEvent>,
    accounting_tx: Option<mpsc::SyncSender<AccountingCommand>>,
    durability_publish_lock: Arc<Mutex<()>>,
    metrics: StrataStoreMetrics,
    store_halt: StoreHalt,
}

impl SealPublisher {
    fn run(self) {
        let mut completed = BTreeMap::new();
        let mut sealing = BinaryHeap::new();
        let mut sealing_ids = HashSet::new();
        while let Ok(event) = self.event_rx.recv() {
            if self.store_halt.error().is_some() {
                break;
            }

            let task_result = match event {
                SealPublisherEvent::Queued(segment_id) => {
                    if sealing_ids.insert(segment_id) {
                        sealing.push(Reverse(segment_id));
                    }
                    continue;
                }
                SealPublisherEvent::Completed(task_result) => task_result,
            };
            match task_result.result {
                Ok(Some(completed_seal)) => {
                    completed.insert(completed_seal.task.segment_id, completed_seal);
                    if let Err(error) =
                        self.publish_ready(&mut completed, &mut sealing, &mut sealing_ids)
                    {
                        self.halt_seal_error(task_result.task.segment_id, &error);
                        break;
                    }
                }
                Ok(None) => {
                    sealing_ids.remove(&task_result.task.segment_id);
                    if let Err(error) =
                        self.publish_ready(&mut completed, &mut sealing, &mut sealing_ids)
                    {
                        self.halt_seal_error(task_result.task.segment_id, &error);
                        break;
                    }
                }
                Err(error) => {
                    self.halt_seal_error(task_result.task.segment_id, &error);
                    break;
                }
            }
        }
    }

    fn publish_ready(
        &self,
        completed: &mut BTreeMap<SegmentId, CompletedSeal>,
        sealing: &mut BinaryHeap<Reverse<SegmentId>>,
        sealing_ids: &mut HashSet<SegmentId>,
    ) -> Result<()> {
        publish_ready_completed_seals(
            &self.config,
            &self.index,
            self.ingest_owner,
            &self.durability_publish_lock,
            self.accounting_tx.as_ref(),
            &self.metrics,
            completed,
            sealing,
            sealing_ids,
        )
    }

    fn halt_seal_error(&self, segment_id: SegmentId, error: &Error) {
        self.metrics.record_seal_error();
        self.store_halt.halt(format!(
            "fatal strata seal worker error sealing segment {segment_id}: {error}"
        ));
    }
}

pub(crate) fn publish_ready_completed_seals(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    ingest_owner: SegmentOwner,
    durability_publish_lock: &Arc<Mutex<()>>,
    accounting_tx: Option<&mpsc::SyncSender<AccountingCommand>>,
    metrics: &StrataStoreMetrics,
    completed: &mut BTreeMap<SegmentId, CompletedSeal>,
    sealing: &mut BinaryHeap<Reverse<SegmentId>>,
    sealing_ids: &mut HashSet<SegmentId>,
) -> Result<()> {
    loop {
        while sealing
            .peek()
            .is_some_and(|Reverse(id)| !sealing_ids.contains(id))
        {
            sealing.pop();
        }
        let Some(&Reverse(segment_id)) = sealing.peek() else {
            return Ok(());
        };
        let Some(completed_seal) = completed.remove(&segment_id) else {
            return Ok(());
        };
        publish_sealed_segment(
            config,
            index,
            ingest_owner,
            durability_publish_lock,
            accounting_tx,
            metrics,
            completed_seal,
        )?;
        sealing.pop();
        sealing_ids.remove(&segment_id);
    }
}

fn dispatch_seal_commands(
    seal_rx: mpsc::Receiver<SealCommand>,
    task_tx: mpsc::Sender<SegmentSealTask>,
    publisher_tx: mpsc::Sender<SealPublisherEvent>,
    store_halt: &StoreHalt,
) {
    loop {
        if store_halt.error().is_some() {
            break;
        }
        match seal_rx.recv_timeout(SEAL_COMMAND_RECV_TIMEOUT) {
            Ok(SealCommand::Seal(task)) => {
                if publisher_tx
                    .send(SealPublisherEvent::Queued(task.segment_id))
                    .is_err()
                {
                    break;
                }
                if task_tx.send(task).is_err() {
                    break;
                }
            }
            Ok(SealCommand::Shutdown) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn join_seal_threads(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.join();
    }
}

fn prepare_seal_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    task: SegmentSealTask,
) -> Result<Option<CompletedSeal>> {
    if let Some(existing) = index.get_segment_state(task.segment_id)?
        && existing.state == SegmentFileState::Sealed
    {
        return Ok(None);
    }

    let path = segment_path(config, task.segment_id);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
    file.sync_data().map_err(|source| Error::Io {
        path: path.clone(),
        source,
    })?;
    let sealed_sha256 = match config.sealed_segment_integrity_policy {
        SealedSegmentIntegrityPolicy::Checksum => Some(sha256_file_prefix(&path, task.sealed_len)?),
        SealedSegmentIntegrityPolicy::MetadataOnly => None,
    };
    let active_delta_state =
        ActiveDeltaLog::sync_existing(config.accounting_index_dir(), task.segment_id)?;
    let checkpoint_lsn = task.sealed_before_lsn.saturating_sub(1);
    if active_delta_state.durable_lsn < checkpoint_lsn {
        return Err(Error::DurabilityAccountingGap {
            required_lsn: checkpoint_lsn,
            active_delta_log_lsn: active_delta_state.durable_lsn,
        });
    }

    Ok(Some(CompletedSeal {
        task,
        sealed_sha256,
        active_delta_state,
    }))
}

fn publish_sealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    ingest_owner: SegmentOwner,
    durability_publish_lock: &Arc<Mutex<()>>,
    accounting_tx: Option<&mpsc::SyncSender<AccountingCommand>>,
    metrics: &StrataStoreMetrics,
    completed: CompletedSeal,
) -> Result<()> {
    let task = completed.task;
    if let Some(existing) = index.get_segment_state(task.segment_id)?
        && existing.state == SegmentFileState::Sealed
    {
        return Ok(());
    }

    let mut state = active_segment_state_from_path(
        config,
        ingest_owner,
        task.segment_id,
        task.sealed_len,
        task.sealed_len,
    );
    if let Some(existing) = index.get_segment_state(task.segment_id)? {
        state.volume_id = existing.volume_id;
        state.path = existing.path;
        state.placement_class = existing.placement_class;
        state.min_lsn = existing.min_lsn;
        state.max_lsn = existing.max_lsn;
        state.sealed_before_lsn = existing.sealed_before_lsn;
    }
    state.state = SegmentFileState::Sealed;
    state.sealed_before_lsn = Some(task.sealed_before_lsn);
    state.sealed_len = Some(task.sealed_len);
    state.sealed_sha256 = completed.sealed_sha256;

    let durable_lsn = {
        let _publish_guard = durability_publish_lock
            .lock()
            .expect("durability publish lock poisoned");
        let mut batch = index.batch();
        index.put_segment_state_batch(&mut batch, &state)?;
        let active_delta_state = durable_active_delta_state(index, completed.active_delta_state)?;
        let durable_lsn =
            durable_lsn_from_checkpoint(index, task.sealed_before_lsn, active_delta_state)?;
        index.put_durable_lsn_batch(&mut batch, durable_lsn)?;
        index.put_accounting_active_delta_log_state_batch(&mut batch, active_delta_state)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        durable_lsn
    };
    index.set_blob_compact_safe_lsn(durable_lsn);
    metrics.record_segment_sealed();
    metrics.set_durable_lsn(durable_lsn);
    metrics.set_unsealed_segments(unsealed_ingest_segment_count(index)?);
    if let Some(accounting_tx) = accounting_tx {
        let _ = accounting_tx.try_send(AccountingCommand::Run);
    }
    Ok(())
}

fn durable_active_delta_state(
    index: &StrataIndex,
    candidate: ActiveDeltaLogState,
) -> Result<ActiveDeltaLogState> {
    let Some(current) = index.get_accounting_active_delta_log_state()? else {
        return Ok(candidate);
    };
    if current.durable_lsn > candidate.durable_lsn {
        return Ok(current);
    }
    if current.durable_lsn < candidate.durable_lsn {
        return Ok(candidate);
    }
    if current.segment_id > candidate.segment_id {
        return Ok(current);
    }
    if current.segment_id < candidate.segment_id {
        return Ok(candidate);
    }
    Ok(ActiveDeltaLogState {
        segment_id: current.segment_id,
        durable_offset: current.durable_offset.max(candidate.durable_offset),
        durable_lsn: current.durable_lsn,
    })
}

fn durable_lsn_from_checkpoint(
    index: &StrataIndex,
    sealed_before_lsn: StrataLsn,
    active_delta_state: ActiveDeltaLogState,
) -> Result<StrataLsn> {
    let current_durable_lsn = index.get_durable_lsn()?;
    let checkpoint_lsn = sealed_before_lsn.saturating_sub(1);
    Ok(checkpoint_lsn
        .min(active_delta_state.durable_lsn)
        .max(current_durable_lsn))
}

pub(crate) fn enqueue_unsealed_segments_for_sealing(
    index: &StrataIndex,
    active_segment_id: SegmentId,
    seal_tx: &mpsc::Sender<SealCommand>,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let committed_before_lsn = index.get_next_lsn()?;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if segment_id < active_segment_id
            && let Some(state) = index.get_segment_state(segment_id)?
        {
            seal_tx
                .send(SealCommand::Seal(SegmentSealTask {
                    segment_id,
                    sealed_len: state.write_offset,
                    sealed_before_lsn: state
                        .sealed_before_lsn
                        .unwrap_or_else(|| {
                            state
                                .max_lsn
                                .and_then(|lsn| lsn.checked_add(1))
                                .unwrap_or(1)
                        })
                        .min(committed_before_lsn),
                }))
                .map_err(|_| Error::SealQueueClosed)?;
            metrics.record_seal_enqueued();
        }
    }
    Ok(())
}

pub(crate) fn verify_sealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    for (_, state) in index.iter_segment_states()? {
        if state.state == SegmentFileState::Sealed {
            verify_sealed_segment(config, &state)?;
        }
    }
    Ok(())
}

fn verify_sealed_segment(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    let segment_id = state.segment_id;
    let path = segment_state_path(config, state);
    let expected_len = state
        .sealed_len
        .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::SealedSegmentMissing { segment_id, path });
        }
        Err(source) => {
            return Err(Error::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let actual_len = metadata.len();
    if actual_len != expected_len {
        return Err(Error::SealedSegmentLengthMismatch {
            segment_id,
            path,
            expected_len,
            actual_len,
        });
    }

    if config.sealed_segment_integrity_policy == SealedSegmentIntegrityPolicy::Checksum {
        let expected = state
            .sealed_sha256
            .ok_or(Error::SealedSegmentMissingChecksum { segment_id })?;
        let actual = sha256_file_prefix(&path, expected_len)?;
        if actual != expected {
            return Err(Error::SealedSegmentChecksumMismatch {
                segment_id,
                path,
                expected,
                actual,
            });
        }
    }

    Ok(())
}

pub(crate) fn sha256_file_prefix(path: &Path, len: u64) -> Result<[u8; 32]> {
    let mut file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut remaining = len;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    while remaining > 0 {
        let to_read = remaining.min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..to_read])
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "segment ended before sealed length",
                ),
            });
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }

    Ok(hasher.finalize().into())
}

pub(crate) fn active_segment_durable_offset(
    index: &StrataIndex,
    segment_id: SegmentId,
) -> Result<u64> {
    Ok(index
        .get_segment_state(segment_id)?
        .map_or(0, |state| state.durable_offset))
}

/// Computes the store durable LSN while requiring the active accounting delta log to cover the
/// same committed prefix.
///
/// Foreground sync fsyncs the segment file and active accounting log first, then publishes
/// `store_state[DurableLsn]` and `accounting_index[ActiveDeltaLogState]` in one synced RocksDB
/// batch. That single batch is what keeps recovery from seeing a new store durable LSN without the
/// matching accounting-log state row. The batch atomicity does not replace the filesystem ordering:
/// if we persisted the metadata before fsyncing either file, a crash could leave `durable_lsn`
/// pointing at missing payload bytes or missing accounting deltas.
///
/// The final `max(current_durable_lsn)` is a monotonicity floor, not a way to newly publish an LSN
/// past the accounting log. In a healthy store, the persisted delta-log frontier is never below the
/// already-published store durable LSN; otherwise accounting could be unable to replay the missing
/// LSNs. Normal foreground sync and recovery callers first raise the in-memory
/// `ActiveDeltaLogState::durable_lsn` to at least `current_durable_lsn`, then commit the store
/// durable LSN and `ActiveDeltaLogState` together. The max only prevents this helper from moving a
/// public durable promise backward if it is called while repairing or observing pre-existing skew.
///
/// Examples:
/// - current durable LSN is 5, payload is durable through LSN 10, but accounting deltas are durable
///   only through LSN 8: return 8.
/// - current durable LSN is already 9, payload is durable through LSN 10, but the provided
///   delta-log frontier is 8: return 9. That is not a healthy steady state; it preserves the
///   existing durable promise while the caller repairs or rejects the skew.
/// - accounting deltas are durable through LSN 12, but payload is durable only through LSN 10:
///   return 10. The accounting log may be ahead, the store cannot publish the extra LSNs yet.
/// - both are durable through LSN 10: return 10, and the store durable LSN plus
///   `ActiveDeltaLogState` become visible together in the metadata batch.
pub(crate) fn durable_lsn_with_accounting_frontier(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
    active_delta_state: Option<ActiveDeltaLogState>,
) -> Result<StrataLsn> {
    let current_durable_lsn = index.get_durable_lsn()?;
    let payload_durable_lsn = compute_durable_lsn(index, current_durable_lsn, override_state)?;
    let delta_durable_lsn = active_delta_state
        .or(index.get_accounting_active_delta_log_state()?)
        .map_or(current_durable_lsn, |state| state.durable_lsn);
    Ok(payload_durable_lsn
        .min(delta_durable_lsn)
        .max(current_durable_lsn))
}

/// Walks the contiguous store LSN stream using only payload/metadata durability.
///
/// This is the payload side of `durable_lsn_with_accounting_frontier`, the accounting delta-log
/// clamp happens afterwards. The scan starts at the current durable LSN and advances one LSN at a
/// time. A later durable-looking operation cannot skip over a missing or non-durable earlier LSN,
/// because `durable_lsn` is a contiguous crash-recoverable prefix.
///
/// Examples:
/// - current durable LSN is 5, LSN 6 is a blob op whose record end offset is covered by the
///   segment's durable offset, and LSN 7 is an epoch change: advance to 7.
/// - LSN 8 has no unaccounted blob op and no epoch change: stop at 7, even if LSN 9 has durable
///   bytes.
/// - LSN 6 has a record ending at offset 8192 but the segment durable offset is only 4096: stop at
///   5. Syncing the accounting delta log cannot make the store publish LSN 6 without the payload
///   bytes being durable too.
fn compute_durable_lsn(
    index: &StrataIndex,
    current_durable_lsn: StrataLsn,
    override_state: Option<&SegmentState>,
) -> Result<StrataLsn> {
    let states = segment_states_with_override(index, override_state)?;
    let mut durable_lsn = current_durable_lsn;

    loop {
        let Some(next_lsn) = durable_lsn.checked_add(1) else {
            break;
        };
        if let Some(key) = index.get_unaccounted_lsn_op(next_lsn)? {
            if !unaccounted_lsn_is_durable(index, next_lsn, &key, &states)? {
                break;
            }
        } else if index.get_epoch_change(next_lsn)?.is_none() {
            break;
        }
        durable_lsn = next_lsn;
    }

    Ok(durable_lsn)
}

fn segment_states_with_override(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
) -> Result<Vec<(SegmentId, SegmentState)>> {
    let mut states = index.iter_segment_states()?;
    if let Some(override_state) = override_state {
        let mut replaced = false;
        for (segment_id, state) in &mut states {
            if *segment_id == override_state.segment_id {
                *state = override_state.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            states.push((override_state.segment_id, override_state.clone()));
        }
    }
    Ok(states)
}

fn unaccounted_lsn_is_durable(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let (op, lifecycle_op) = index.blob_ops_at_lsn(key, lsn)?;
    let map_ref = index.blob_map_ref_at_lsn(key, lsn)?;
    if !map_ref_is_durable(map_ref.as_ref(), states)? {
        return Ok(false);
    }

    let Some(op) = op else {
        return Ok(lifecycle_op.is_some() || map_ref.is_some());
    };
    let Some(record_ref) = op.entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    let is_durable = states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            state.state != SegmentFileState::Deleted && state.durable_offset >= record_end_offset
        });
    if !is_durable {
        return Ok(false);
    }
    Ok(true)
}

fn map_ref_is_durable(
    map_ref: Option<&MapRefOp>,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let Some(map_ref) = map_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = map_ref.to.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    Ok(states
        .iter()
        .find(|(candidate, _)| *candidate == map_ref.to.segment_id)
        .is_some_and(|(_, state)| {
            state.state != SegmentFileState::Deleted && state.durable_offset >= record_end_offset
        }))
}
