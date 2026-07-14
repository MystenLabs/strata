//! Accounting is the store's sidecar-driven derived-state engine.
//!
//! Three readers need different breadcrumbs here:
//! - Systems engineer: preserve the frontier contract. Payload/index writes become durable first;
//!   the active delta log is made durable with that prefix; sidecar compaction then publishes
//!   derived ref events and GC overlay operands from compaction events.
//! - New contributor: the foreground writer never resolves blob keys for accounting. It appends
//!   cheap deltas; `AccountingSidecar` ingests those deltas into compact files and applies
//!   compaction events back to the main index as segment ref events and GC overlay operands.
//! - Future maintainer: most choices below are defensive ordering choices. For example, advancing a
//!   cursor before publishing its prepared sidecar manifest would make replay skip deltas after a
//!   crash; applying derived rows separately from the manifest would either duplicate or lose
//!   accounting effects after restart.
//!
//! Inline comments call out those perspectives as `Systems invariant`, `Ramp-up`, and
//! `Future-maintainer note` where the local code shape is otherwise surprising.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use strata_accounting::{
    AccountingIndex, AccountingIndexConfig, ActiveDeltaLog, ActiveDeltaLogReadCursor,
    ActiveDeltaLogState, CompactionEventBatch, Manifest, RefEvent as AccountingRefEvent,
};
use strata_core::{
    BlobLifecycle, Epoch, GcRelocation, RecordRef, SegmentFileState, SegmentGcLifetimeUpdate,
    SegmentGcLiveRecord, SegmentGcOverlayMergeOp, SegmentGcRecordRange, SegmentId, SegmentRefEvent,
    SegmentRefEventKey, ShardCleanupState, StrataLsn,
};
use strata_index::StrataIndex;
use typed_store::rocks::DBBatch;

use crate::{Error, Result, config::StrataStoreConfig, gc::GcCommand};

#[derive(Debug)]
pub(crate) enum AccountingCommand {
    /// Wake the worker so it can consume the highest-priority coalesced request.
    Wake,
    /// Stop the background worker after pending channel work has drained.
    Shutdown,
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum AccountingRequest {
    None = 0,
    Ingest = 1,
    Materialize = 2,
}

/// Non-blocking accounting request handle shared by foreground, seal, and GC paths.
///
/// The wake channel remains bounded to one so repeated ingest requests coalesce. Request priority
/// lives separately: a materialization request promotes an already queued ingest wakeup instead of
/// being dropped behind it or applying backpressure to the caller.
#[derive(Clone, Debug)]
pub(crate) struct AccountingRequestSender {
    command_tx: mpsc::SyncSender<AccountingCommand>,
    pending_request: Arc<AtomicU8>,
    pending_materialize_through_lsn: Arc<AtomicU64>,
}

impl AccountingRequestSender {
    pub(crate) fn request_ingest(&self) {
        self.request(AccountingRequest::Ingest);
    }

    pub(crate) fn request_materialize(&self, through_lsn: StrataLsn) {
        self.pending_materialize_through_lsn
            .fetch_max(through_lsn, Ordering::AcqRel);
        self.request(AccountingRequest::Materialize);
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.command_tx.send(AccountingCommand::Shutdown);
    }

    fn request(&self, request: AccountingRequest) {
        self.pending_request
            .fetch_max(request as u8, Ordering::AcqRel);
        let _ = self.command_tx.try_send(AccountingCommand::Wake);
    }
}

pub(crate) fn accounting_request_channel() -> (
    AccountingRequestSender,
    mpsc::Receiver<AccountingCommand>,
    Arc<AtomicU8>,
    Arc<AtomicU64>,
) {
    let (command_tx, command_rx) = mpsc::sync_channel(1);
    let pending_request = Arc::new(AtomicU8::new(AccountingRequest::None as u8));
    let pending_materialize_through_lsn = Arc::new(AtomicU64::new(0));
    (
        AccountingRequestSender {
            command_tx,
            pending_request: Arc::clone(&pending_request),
            pending_materialize_through_lsn: Arc::clone(&pending_materialize_through_lsn),
        },
        command_rx,
        pending_request,
        pending_materialize_through_lsn,
    )
}

/// Background accounting loop for one store.
///
/// It owns no mutable store state itself; instead it serializes sidecar maintenance over the shared
/// index: ingest durable active-log deltas, compact sidecar runs, then publish compaction events
/// back into the main index.
///
/// Failure example: if two sidecar runs raced, both could compact the same input run and publish
/// duplicate non-idempotent GC overlay allocations.
#[derive(Debug)]
pub(crate) struct AccountingWorker {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) interval: Duration,
    pub(crate) command_rx: mpsc::Receiver<AccountingCommand>,
    pub(crate) pending_request: Arc<AtomicU8>,
    pub(crate) pending_materialize_through_lsn: Arc<AtomicU64>,
    pub(crate) run_lock: Arc<Mutex<()>>,
    pub(crate) gc_txs: Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
}

impl AccountingWorker {
    /// Runs until shutdown or channel disconnect.
    ///
    /// Ingest requests force the sidecar to consume durable deltas while compaction still respects
    /// its thresholds. Materialize requests force the complete pipeline for rare semantic work
    /// such as shard cleanup and epoch changes. Timer ticks retain the wall-clock backstop that
    /// eventually materializes quiet stores.
    pub(crate) fn run(self) {
        let mut sidecar = AccountingSidecar::open(self.config.clone(), self.index.clone()).ok();
        loop {
            match self.command_rx.recv_timeout(self.interval) {
                Ok(AccountingCommand::Wake) => {
                    let Some((mut mode, mut materialize_through_lsn)) = take_pending_run_mode(
                        &self.pending_request,
                        &self.pending_materialize_through_lsn,
                    ) else {
                        continue;
                    };
                    if let Some(through_lsn) = materialize_through_lsn
                        && self
                            .index
                            .get_durable_lsn()
                            .is_ok_and(|durable_lsn| durable_lsn < through_lsn)
                    {
                        // The semantic operation is visible but not crash-safe yet. Preserve its
                        // priority for the sync/seal wakeup that makes the target durable, while
                        // keeping this early wakeup on the cheap ingest-only path.
                        rearm_materialization(
                            &self.pending_request,
                            &self.pending_materialize_through_lsn,
                            through_lsn,
                        );
                        mode = SidecarRunMode::Nudged;
                        materialize_through_lsn = None;
                    }
                    // Systems invariant: one sidecar pass at a time. Live-allocation overlay
                    // operands are not idempotent, so duplicate application would corrupt GC
                    // summary counters.
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    let succeeded = Self::run_sidecar(
                        &mut sidecar,
                        &self.config,
                        &self.index,
                        &self.gc_txs,
                        mode,
                    );
                    if let Some(through_lsn) = materialize_through_lsn {
                        let reached_target = succeeded
                            && matches!(
                                self.index.get_accounted_lsn(),
                                Ok(accounted_lsn) if accounted_lsn >= through_lsn
                            );
                        if !reached_target {
                            rearm_materialization(
                                &self.pending_request,
                                &self.pending_materialize_through_lsn,
                                through_lsn,
                            );
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    let _ = Self::run_sidecar(
                        &mut sidecar,
                        &self.config,
                        &self.index,
                        &self.gc_txs,
                        SidecarRunMode::Maintenance,
                    );
                }
                Ok(AccountingCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    fn run_sidecar(
        sidecar: &mut Option<AccountingSidecar>,
        config: &StrataStoreConfig,
        index: &StrataIndex,
        gc_txs: &Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
        mode: SidecarRunMode,
    ) -> bool {
        if sidecar.is_none() {
            // Future-maintainer note: sidecar setup can fail transiently while the active log or
            // manifest is being initialized. Retry later instead of killing the worker.
            *sidecar = AccountingSidecar::open(config.clone(), index.clone()).ok();
        }
        let failed = if let Some(current_sidecar) = sidecar.as_mut() {
            let result = match mode {
                SidecarRunMode::Nudged => current_sidecar.run_nudged(),
                SidecarRunMode::Materialize => current_sidecar.run_materializing(),
                SidecarRunMode::Maintenance => current_sidecar.run(),
            };
            match result {
                Ok(should_nudge_gc) => {
                    if should_nudge_gc {
                        nudge_gc(gc_txs);
                    }
                    false
                }
                Err(_) => true,
            }
        } else {
            false
        };
        if failed {
            // A sidecar run may have written immutable run files or even committed its synced
            // RocksDB batch before reporting an error. Reopen from durable metadata on the next
            // pass so the in-memory manifest never drifts from RocksDB.
            *sidecar = None;
        }
        !failed
    }
}

fn nudge_gc(gc_txs: &Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>) {
    let gc_txs = gc_txs.lock().expect("gc tx list lock poisoned");
    for gc_tx in gc_txs.iter() {
        let _ = gc_tx.send(GcCommand::Run);
    }
}

#[derive(Clone, Copy, Debug)]
enum SidecarRunMode {
    Nudged,
    Materialize,
    Maintenance,
}

fn take_pending_run_mode(
    pending_request: &AtomicU8,
    pending_materialize_through_lsn: &AtomicU64,
) -> Option<(SidecarRunMode, Option<StrataLsn>)> {
    match pending_request.swap(AccountingRequest::None as u8, Ordering::AcqRel) {
        value if value == AccountingRequest::None as u8 => None,
        value if value == AccountingRequest::Ingest as u8 => Some((SidecarRunMode::Nudged, None)),
        value if value == AccountingRequest::Materialize as u8 => {
            let through_lsn = pending_materialize_through_lsn.swap(0, Ordering::AcqRel);
            (through_lsn != 0).then_some((SidecarRunMode::Materialize, Some(through_lsn)))
        }
        value => panic!("unknown accounting request priority {value}"),
    }
}

fn rearm_materialization(
    pending_request: &AtomicU8,
    pending_materialize_through_lsn: &AtomicU64,
    through_lsn: StrataLsn,
) {
    pending_materialize_through_lsn.fetch_max(through_lsn, Ordering::AcqRel);
    pending_request.fetch_max(AccountingRequest::Materialize as u8, Ordering::AcqRel);
}

#[cfg(test)]
mod request_tests {
    use std::sync::mpsc::TryRecvError;

    use super::*;

    #[test]
    fn materialize_promotes_an_already_queued_ingest_wakeup() {
        let (sender, command_rx, pending_request, pending_materialize_through_lsn) =
            accounting_request_channel();

        sender.request_ingest();
        sender.request_materialize(7);

        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((SidecarRunMode::Materialize, Some(7)))
        ));
    }

    #[test]
    fn unfinished_materialization_promotes_the_next_ingest_wakeup() {
        let (sender, command_rx, pending_request, pending_materialize_through_lsn) =
            accounting_request_channel();

        sender.request_materialize(9);
        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((SidecarRunMode::Materialize, Some(9)))
        ));

        rearm_materialization(&pending_request, &pending_materialize_through_lsn, 9);
        sender.request_ingest();

        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((SidecarRunMode::Materialize, Some(9)))
        ));
    }
}

fn lifecycle_is_expired(lifecycle: Option<BlobLifecycle>, current_epoch: Epoch) -> bool {
    lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch)
}

#[cfg(test)]
/// Runs the sidecar once from tests without waiting for the background interval.
///
/// Failure example: without this hook, tests would either sleep on wall-clock intervals or poke
/// private sidecar internals, both of which make ordering bugs harder to reproduce.
pub(crate) fn run_accounting_sidecar_once(
    index: &StrataIndex,
    config: &StrataStoreConfig,
    force: bool,
) -> Result<()> {
    let mut sidecar = AccountingSidecar::open(config.clone(), index.clone())?;
    sidecar.run_once(force)
}

#[cfg(test)]
pub(crate) fn run_accounting_sidecar_nudged_once(
    index: &StrataIndex,
    config: &StrataStoreConfig,
) -> Result<()> {
    let mut sidecar = AccountingSidecar::open(config.clone(), index.clone())?;
    sidecar.run_nudged().map(|_| ())
}

#[cfg(test)]
pub(crate) fn run_accounting_sidecar_materializing_once(
    index: &StrataIndex,
    config: &StrataStoreConfig,
) -> Result<()> {
    let mut sidecar = AccountingSidecar::open(config.clone(), index.clone())?;
    sidecar.run_materializing().map(|_| ())
}

/// Incremental sidecar builder for durable accounting delta logs.
///
/// Ramp-up: the foreground writer appends `AccountingDelta`s to segment-aligned accounting logs;
/// this object reads their durable ranges, writes immutable sidecar runs, then records a manifest
/// and consumed cursor in the main index.
///
/// Failure example: without the sidecar, recovering or querying accounting history would need to
/// replay an ever-growing active log, making long-running stores slower over time.
struct AccountingSidecar {
    config: StrataStoreConfig,
    index: StrataIndex,
    accounting_index: AccountingIndex,
    last_forced_run: Instant,
}

impl AccountingSidecar {
    /// Opens the sidecar index from the manifest stored in the main index.
    ///
    /// Failure example: opening from directory contents alone would resurrect files that were
    /// created by a crashed compaction but never committed to the manifest.
    fn open(config: StrataStoreConfig, index: StrataIndex) -> Result<Self> {
        let manifest = index.get_accounting_index_manifest()?;
        let accounting_index = AccountingIndex::open_with_manifest(
            AccountingIndexConfig::new(
                config.accounting_index_dir(),
                config.accounting_sidecar_partition_count(),
            ),
            manifest,
        )?;
        Ok(Self {
            config,
            index,
            accounting_index,
            last_forced_run: Instant::now(),
        })
    }

    /// Runs one normal sidecar maintenance tick.
    ///
    /// `force` becomes true on a wall-clock cadence so low-write stores still eventually compact
    /// small runs. Without that, a quiet store could accumulate many tiny delta files forever.
    fn run(&mut self) -> Result<bool> {
        self.refresh_accounting_index()?;
        let pending_shard_drop = self
            .index
            .iter_shard_cleanup_jobs()?
            .into_iter()
            .any(|job| job.state == ShardCleanupState::PendingAccounting);
        let force = pending_shard_drop
            || self.last_forced_run.elapsed() >= self.config.accounting_sidecar_interval;
        let should_nudge_gc = self.run_once_materializing(force)?;
        if force {
            self.last_forced_run = Instant::now();
        }
        Ok(should_nudge_gc)
    }

    /// Ingests durable deltas for an explicit writer/sync nudge.
    ///
    /// Nudges bypass only the ingest threshold. Delta and major compaction retain their run-count
    /// and byte thresholds so a segment rollover does not force a synced publication for every
    /// populated partition. Unlike a wall-clock forced maintenance pass, a nudge does not reset
    /// `last_forced_run`; low-volume traffic therefore cannot postpone materialization forever.
    fn run_nudged(&mut self) -> Result<bool> {
        self.refresh_accounting_index()?;
        let mut should_nudge_gc = self.ingest_active_delta_log(true)?;
        should_nudge_gc |= self.compact_sidecar(false, false)?;
        should_nudge_gc |= self.materialize_shard_drops()?;
        Ok(should_nudge_gc)
    }

    /// Forces ingest, delta compaction, and major compaction for semantic operations whose
    /// asynchronous cleanup should begin immediately.
    fn run_materializing(&mut self) -> Result<bool> {
        self.refresh_accounting_index()?;
        let should_nudge_gc = self.run_once_materializing(true)?;
        self.last_forced_run = Instant::now();
        Ok(should_nudge_gc)
    }

    /// Refreshes this handle after another serialized publisher advances the durable manifest.
    ///
    /// The background worker is normally the only production publisher, but tests and maintenance
    /// tooling can open a second handle under the same run lock. Compaction may delete files from
    /// the previous manifest after publishing its replacement, so a long-lived stale handle must
    /// adopt the durable generation before preparing more work.
    fn refresh_accounting_index(&mut self) -> Result<()> {
        let Some(manifest) = self.index.get_accounting_index_manifest()? else {
            return Ok(());
        };
        if manifest.generation != self.accounting_index.manifest().generation {
            self.accounting_index.apply_manifest(manifest)?;
        }
        Ok(())
    }

    /// Ingests new active-log deltas first, then compacts sidecar files.
    ///
    /// Failure example: compacting before ingestion would not corrupt data, but it can repeatedly
    /// compact stale partitions while a large active-log backlog continues to grow.
    #[cfg(test)]
    fn run_once(&mut self, force: bool) -> Result<()> {
        let _ = self.ingest_active_delta_log(force)?;
        let _ = self.compact_sidecar(force, false)?;
        if force {
            let _ = self.materialize_shard_drops()?;
        }
        Ok(())
    }

    /// Runs a sidecar pass that can advance the main-index accounting frontier.
    ///
    /// Major compaction is what materializes patch state into ordered ref events, so production
    /// forced passes must bypass major thresholds as well as ingest/delta thresholds.
    fn run_once_materializing(&mut self, force: bool) -> Result<bool> {
        let mut should_nudge_gc = self.ingest_active_delta_log(force)?;
        should_nudge_gc |= self.compact_sidecar(force, force)?;
        should_nudge_gc |= self.materialize_shard_drops()?;
        Ok(should_nudge_gc)
    }

    fn materialize_shard_drops(&mut self) -> Result<bool> {
        let drops = self.accounting_index.pending_shard_drops();
        let mut materialized = false;
        for drop in drops {
            let prepared = match self.accounting_index.prepare_materialize_shard_drop(drop) {
                Ok(prepared) => prepared,
                Err(strata_accounting::Error::ShardDropRequiresCompaction { .. }) => continue,
                Err(error) => return Err(error.into()),
            };
            let manifest = prepared.manifest().clone();
            self.commit_sidecar_state(Some(&manifest), None, Some(&prepared.event_batch))?;
            self.accounting_index.apply_prepared_shard_drop(prepared)?;
            materialized = true;
        }
        Ok(materialized)
    }

    /// Copies durable active-log deltas into immutable sidecar runs and advances the read cursor.
    ///
    /// Systems invariant: only the durable active-log range is read. If we read past
    /// `durable_state`, a process crash could make the sidecar remember deltas for store writes
    /// that were never acknowledged as durable.
    ///
    /// Failure example: if `next_cursor` were committed before the prepared manifest is published, a
    /// crash would skip those deltas forever on the next sidecar pass.
    fn ingest_active_delta_log(&mut self, force: bool) -> Result<bool> {
        let Some(durable_state) = self.active_delta_log_state()? else {
            return Ok(false);
        };
        let cursor = self.active_delta_log_read_cursor()?;
        // Retry cleanup from an already durable cursor before doing more sidecar work. This makes a
        // prior unlink failure recoverable without needing another cursor transition.
        self.reclaim_consumed_delta_logs(cursor)?;
        let read = ActiveDeltaLog::read_durable_range(
            self.config.accounting_index_dir(),
            cursor,
            durable_state,
        )?;
        let next_cursor = read.next_cursor(cursor);
        if read.deltas.is_empty() {
            if next_cursor != cursor {
                let _ = self.commit_sidecar_state(None, Some(next_cursor), None)?;
                self.reclaim_consumed_delta_logs(next_cursor)?;
            }
            return Ok(false);
        }
        if !force
            && !count_threshold_reached(
                read.deltas.len(),
                self.config.accounting_sidecar_ingest_record_threshold,
            )
        {
            // Ramp-up: threshold 0 means "disable this trigger"; `force` is the backstop that
            // eventually ingests small batches anyway.
            return Ok(false);
        }

        let prepared = self
            .accounting_index
            .prepare_accounting_deltas(read.deltas)?;
        let manifest = prepared.manifest().clone();
        // Systems invariant: publish manifest + cursor before applying the in-memory manifest.
        // The run files were already written by `prepare_accounting_deltas`; if we crash after this
        // batch, reopening from RocksDB sees the new manifest and cursor together.
        let should_nudge_gc =
            self.commit_sidecar_state(Some(&manifest), Some(next_cursor), None)?;
        self.accounting_index
            .apply_prepared_accounting_deltas(prepared)?;
        self.reclaim_consumed_delta_logs(next_cursor)?;
        Ok(should_nudge_gc)
    }

    /// Reclaims only logs whose durable sidecar handoff and data-segment seal are both complete.
    ///
    /// Cursor ordering alone is insufficient: foreground sync can publish a later active-log
    /// segment while a seal worker still needs an older log for its final fsync. `Sealed` (or
    /// `Deleted`) is the durable proof that no seal worker can need that file again.
    fn reclaim_consumed_delta_logs(&self, cursor: ActiveDeltaLogReadCursor) -> Result<()> {
        if cursor.segment_id == 0 {
            return Ok(());
        }
        let reclaimable = self
            .index
            .iter_segment_states()?
            .into_iter()
            .filter_map(|(segment_id, state)| {
                (segment_id < cursor.segment_id
                    && matches!(
                        state.state,
                        SegmentFileState::Sealed | SegmentFileState::Deleted
                    ))
                .then_some(segment_id)
            })
            .collect::<BTreeSet<_>>();
        ActiveDeltaLog::remove_segments(self.config.accounting_index_dir(), &reclaimable)?;
        Ok(())
    }

    /// Compacts sidecar delta and patch runs partition by partition.
    ///
    /// Failure example: without compaction, sidecar queries eventually fan out over many tiny files;
    /// without the manifest checks here, a no-op compaction could publish churn and make recovery
    /// inspect files that no query needs.
    fn compact_sidecar(
        &mut self,
        force_delta_compaction: bool,
        force_major_compaction: bool,
    ) -> Result<bool> {
        let partitions = self
            .accounting_index
            .manifest()
            .partitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut should_nudge_gc = false;

        // Future-maintainer note: collect keys first because each application updates the manifest.
        // Iterating the map directly while mutating it would either fail borrowing or skip work.
        for partition in partitions {
            if self.should_compact_deltas(partition, force_delta_compaction) {
                let prepared = self.accounting_index.prepare_compact_partition(partition)?;
                if !prepared.event_batch.input_run_ids.is_empty() {
                    let manifest = prepared.manifest().clone();
                    should_nudge_gc |= self.commit_sidecar_state(
                        Some(&manifest),
                        None,
                        Some(&prepared.event_batch),
                    )?;
                    self.accounting_index.apply_prepared_compaction(prepared)?;
                }
            }

            if self.should_major_compact(partition, force_major_compaction) {
                let prepared = self
                    .accounting_index
                    .prepare_major_compact_partition(partition)?;
                if prepared.output.is_some() {
                    let manifest = prepared.manifest().clone();
                    should_nudge_gc |= self.commit_sidecar_state(
                        Some(&manifest),
                        None,
                        Some(&prepared.event_batch),
                    )?;
                    self.accounting_index
                        .apply_prepared_major_compaction(prepared)?;
                }
            }
        }
        Ok(should_nudge_gc)
    }

    /// Decides whether delta runs should be compacted into a patch run.
    ///
    /// Failure example: always compacting would turn every background tick into rewrite work;
    /// never compacting would make reads and future compactions scale with run count.
    fn should_compact_deltas(&self, partition: u32, force: bool) -> bool {
        let partition = self
            .accounting_index
            .manifest()
            .partitions
            .get(&partition)
            .expect("partition was read from manifest");
        if partition.deltas.is_empty() {
            return false;
        }
        let delta_bytes = partition.deltas.iter().map(|run| run.file_len).sum::<u64>();
        force
            || count_threshold_reached(
                partition.deltas.len(),
                self.config.accounting_sidecar_delta_run_count_threshold,
            )
            || bytes_threshold_reached(
                delta_bytes,
                self.config.accounting_sidecar_delta_run_bytes_threshold,
            )
    }

    /// Decides whether patch runs should be compacted into a new base run.
    ///
    /// Failure example: without major compaction, patch chains can grow without bound, so answering
    /// "what is live in this segment?" would require replaying every historical patch.
    fn should_major_compact(&self, partition: u32, force: bool) -> bool {
        let partition = self
            .accounting_index
            .manifest()
            .partitions
            .get(&partition)
            .expect("partition was read from manifest");
        if partition.patches.is_empty() {
            return false;
        }
        let patch_bytes = partition
            .patches
            .iter()
            .map(|run| run.file_len)
            .sum::<u64>();
        force
            || count_threshold_reached(
                partition.patches.len(),
                self.config.accounting_sidecar_major_patch_count_threshold,
            )
            || bytes_threshold_reached(
                patch_bytes,
                self.config.accounting_sidecar_major_patch_bytes_threshold,
            )
    }

    /// Reads the foreground-published durable active-log frontier.
    ///
    /// Failure example: using the file length instead would let the sidecar ingest complete but
    /// unsynced frames that recovery may later discard.
    fn active_delta_log_state(&self) -> Result<Option<ActiveDeltaLogState>> {
        Ok(self.index.get_accounting_active_delta_log_state()?)
    }

    /// Reads where sidecar ingestion last stopped, defaulting to the start of the active log.
    ///
    /// Failure example: treating a missing cursor as an error would make first startup unable to
    /// build sidecar state even though there is simply nothing consumed yet.
    fn active_delta_log_read_cursor(&self) -> Result<ActiveDeltaLogReadCursor> {
        self.index
            .get_accounting_active_delta_log_consumed_cursor()
            .map(|cursor| cursor.unwrap_or_default())
            .map_err(Error::from)
    }

    /// Commits sidecar metadata to the main index and fsyncs the RocksDB WAL.
    ///
    /// Systems invariant: manifest and cursor move together. If a cursor advanced without the
    /// matching manifest, the next run would skip deltas; if a manifest advanced without its cursor,
    /// the same deltas could be ingested twice into another run.
    ///
    /// The prepared-manifest generation check in `AccountingIndex::apply_prepared_*` happens after
    /// this batch is durable. That check protects the in-memory sidecar mirror and its obsolete-file
    /// cleanup from stale local work; it does not make this RocksDB write conditional. Durable
    /// serialization comes from the single sidecar worker/run lock. Under that serialization, this
    /// batch is the state transition: manifest, consumed cursor, derived ref/overlay rows, and
    /// frontier movement become visible together, then the WAL is fsynced before the in-memory
    /// sidecar mirror is advanced.
    fn commit_sidecar_state(
        &self,
        manifest: Option<&Manifest>,
        cursor: Option<ActiveDeltaLogReadCursor>,
        event_batch: Option<&CompactionEventBatch>,
    ) -> Result<bool> {
        let mut context = SidecarAccountingContext::new(&self.index)?;
        if let Some(event_batch) = event_batch {
            context.apply_compaction_event_batch(event_batch)?;
        }
        let frontier =
            match manifest {
                Some(manifest) => Some(context.advance_frontier(manifest, |key| {
                    self.accounting_index.partition_for_key(key)
                })?),
                None => None,
            };

        let mut batch = self.index.batch();
        if let Some(manifest) = manifest {
            self.index
                .put_accounting_index_manifest_batch(&mut batch, manifest)?;
        }
        if let Some(cursor) = cursor {
            self.index
                .put_accounting_active_delta_log_consumed_cursor_batch(&mut batch, cursor)?;
        }
        let current_accounted_lsn = self.index.get_accounted_lsn()?;
        if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            context.remove_relocations_through_lsn(frontier.accounted_lsn);
        }
        let should_nudge_gc = if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            !frontier.completed_shard_drops.is_empty()
                || frontier.materialized_epoch_change
                || accounting_frontier_unblocks_empty_delete(
                    &self.index,
                    &context,
                    current_accounted_lsn,
                    frontier.accounted_lsn,
                )?
        } else {
            false
        };
        context.write_to_batch(&mut batch)?;
        if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            self.index
                .remove_unaccounted_lsn_ops_batch(&mut batch, &frontier.consumed_lsns)?;
            self.index
                .put_accounted_lsn_batch(&mut batch, frontier.accounted_lsn)?;
            for shard in &frontier.completed_shard_drops {
                if let Some(mut job) = self.index.get_shard_cleanup_job(*shard)? {
                    job.state = ShardCleanupState::ReadyForGc;
                    self.index.put_shard_cleanup_job_batch(&mut batch, job)?;
                }
            }
        }
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        Ok(should_nudge_gc)
    }
}

fn accounting_frontier_unblocks_empty_delete(
    index: &StrataIndex,
    context: &SidecarAccountingContext<'_>,
    previous_lsn: StrataLsn,
    accounted_lsn: StrataLsn,
) -> Result<bool> {
    for (_, state) in index.iter_segment_states()? {
        if state.state == SegmentFileState::Sealed
            && state
                .max_lsn
                .is_some_and(|max_lsn| previous_lsn < max_lsn && max_lsn <= accounted_lsn)
        {
            let mut overlay = index
                .get_segment_gc_overlay(state.segment_id)?
                .unwrap_or_default();
            if let Some(ops) = context.gc_overlay_ops.get(&state.segment_id) {
                overlay.apply_merge_ops(ops.clone());
            }
            if overlay.summary.live_ref_count == 0 {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Interprets a zero count threshold as disabled.
///
/// Failure example: if `0` meant "always reached", operators could accidentally turn every tick
/// into a forced ingest or compaction by leaving a threshold unset.
fn count_threshold_reached(value: usize, threshold: usize) -> bool {
    threshold != 0 && value >= threshold
}

/// Interprets a zero byte threshold as disabled.
///
/// Failure example: treating zero as reached would compact empty or tiny runs on every background
/// loop, wasting I/O on stores with little traffic.
fn bytes_threshold_reached(value: u64, threshold: u64) -> bool {
    threshold != 0 && value >= threshold
}

/// Result of advancing the store-global accounting cursor after sidecar materialization.
#[derive(Debug, Default)]
struct FrontierUpdate {
    accounted_lsn: StrataLsn,
    consumed_lsns: Vec<StrataLsn>,
    materialized_epoch_change: bool,
    completed_shard_drops: Vec<strata_core::ShardKey>,
}

/// Scratchpad for one sidecar commit.
///
/// Ramp-up: sidecar compaction produces ref events. This context translates those events into the
/// main-index rows GC already consumes, while keeping the RocksDB commit atomic with the sidecar
/// manifest/cursor update.
///
/// Failure example: applying event rows outside the manifest commit would either duplicate
/// non-idempotent overlay allocations after a retry or lose them after a crash.
struct SidecarAccountingContext<'a> {
    index: &'a StrataIndex,
    ref_events: BTreeMap<SegmentRefEventKey, SegmentRefEvent>,
    gc_overlay_ops: BTreeMap<SegmentId, Vec<SegmentGcOverlayMergeOp>>,
    relocations: BTreeMap<RecordRef, GcRelocation>,
    removed_relocations: BTreeSet<RecordRef>,
}

impl<'a> SidecarAccountingContext<'a> {
    fn new(index: &'a StrataIndex) -> Result<Self> {
        Ok(Self {
            index,
            ref_events: BTreeMap::new(),
            gc_overlay_ops: BTreeMap::new(),
            relocations: index.iter_gc_relocations()?.into_iter().collect(),
            removed_relocations: BTreeSet::new(),
        })
    }

    /// Applies all sidecar-produced ref events for one compaction.
    ///
    /// The events are structural; this layer translates them into ordered ref events and overlay
    /// merge operands. The overlay fold owns summary counter updates.
    fn apply_compaction_event_batch(&mut self, batch: &CompactionEventBatch) -> Result<()> {
        for event in &batch.events {
            self.apply_ref_event(event)?;
        }
        Ok(())
    }

    fn apply_ref_event(&mut self, event: &AccountingRefEvent) -> Result<()> {
        // The sidecar event stream is logical and key-oriented; the main index rows are
        // segment-oriented. This translation preserves that one event can fan out into several
        // physical rows: ordered ref events and GC overlay operands.
        match event {
            AccountingRefEvent::Live {
                lsn,
                record_ref,
                lifecycle,
                ..
            } => self.add_ref(*lsn, *record_ref, *lifecycle),
            AccountingRefEvent::Retired {
                lsn,
                record_ref,
                lifecycle,
                ..
            } => self.retire_ref(*lsn, *record_ref, *lifecycle),
            AccountingRefEvent::LifecycleChanged {
                lsn,
                record_ref,
                old,
                new,
                ..
            } => self.change_lifecycle(*lsn, *record_ref, *old, *new),
            AccountingRefEvent::Mapped {
                lsn,
                from,
                to,
                lifecycle,
                ..
            } => self.map_ref(*lsn, *from, *to, *lifecycle),
        }
    }

    /// Advances the global accounting cursor as far as sidecar materialization permits.
    ///
    /// This still inspects `unaccounted_lsn_ops`, but only to compute a contiguous global cleanup
    /// frontier. It does not resolve blob state or derive GC state from blob keys.
    fn advance_frontier(
        &mut self,
        manifest: &Manifest,
        partition_for_key: impl Fn(&strata_core::BlobKey) -> u32,
    ) -> Result<FrontierUpdate> {
        let durable_lsn = self.index.get_durable_lsn()?;
        let mut accounted_lsn = self.index.get_accounted_lsn()?;
        let mut consumed_lsns = Vec::new();
        let mut materialized_epoch_change = false;
        let mut completed_shard_drops = Vec::new();

        loop {
            let Some(next_lsn) = accounted_lsn.checked_add(1) else {
                break;
            };
            if next_lsn > durable_lsn {
                break;
            }

            if let Some(key) = self.index.get_unaccounted_lsn_op(next_lsn)? {
                let partition = partition_for_key(&key);
                let Some(partition) = manifest.partitions.get(&partition) else {
                    break;
                };
                if partition.materialized_through_lsn < next_lsn {
                    break;
                }
                consumed_lsns.push(next_lsn);
                accounted_lsn = next_lsn;
                continue;
            }

            if let Some(epoch) = self.index.get_epoch_change(next_lsn)? {
                self.expire_live_refs(next_lsn, epoch)?;
                materialized_epoch_change = true;
                accounted_lsn = next_lsn;
                continue;
            }

            if let Some(drop) = manifest
                .shard_drops
                .iter()
                .find(|drop| drop.lsn == next_lsn && drop.materialized)
            {
                completed_shard_drops.push(drop.shard);
                accounted_lsn = next_lsn;
                continue;
            }

            break;
        }

        Ok(FrontierUpdate {
            accounted_lsn,
            consumed_lsns,
            materialized_epoch_change,
            completed_shard_drops,
        })
    }

    /// Stages all derived rows into the caller's sidecar metadata batch.
    fn write_to_batch(self, batch: &mut DBBatch) -> Result<()> {
        for (key, event) in self.ref_events {
            self.index.put_segment_ref_event_batch(batch, key, &event)?;
        }
        for (segment_id, ops) in self.gc_overlay_ops {
            self.index
                .merge_segment_gc_overlay_batch(batch, segment_id, ops)?;
        }
        for from in self.removed_relocations {
            self.index.delete_gc_relocation_batch(batch, from)?;
        }
        Ok(())
    }

    /// Adds a newly materialized payload ref to the main-index accounting rows.
    fn add_ref(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        let epoch = self.epoch_at_lsn(lsn)?;
        if lifecycle_is_expired(lifecycle, epoch) {
            // Expired-on-arrival bytes are still part of the segment's physical footprint, but they
            // never enter a live bucket. The overlay summary records them as expired immediately.
            self.put_ref_event(lsn, record_ref, SegmentRefEvent::Expired);
            self.stage_overlay_op(
                record_ref.segment_id,
                SegmentGcOverlayMergeOp::AddExpiredBatch {
                    ranges: vec![SegmentGcRecordRange::from(record_ref)],
                },
            );
        } else {
            self.stage_overlay_op(
                record_ref.segment_id,
                SegmentGcOverlayMergeOp::AddLiveBatch {
                    records: vec![SegmentGcLiveRecord {
                        range: SegmentGcRecordRange::from(record_ref),
                        lifecycle,
                    }],
                },
            );
        }
        Ok(())
    }

    /// Retires a materialized payload ref.
    ///
    fn retire_ref(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        _lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        self.put_ref_event(lsn, record_ref, SegmentRefEvent::Retired);
        self.retire_overlay(record_ref);
        if let Some(relocation) = self.relocation_for(lsn, record_ref) {
            self.put_ref_event(lsn, relocation.to, SegmentRefEvent::Retired);
            self.add_retired_overlay(relocation.to);
        }
        Ok(())
    }

    /// Materializes a GC relocation after its `MapRef` reaches accounting.
    fn map_ref(
        &mut self,
        lsn: StrataLsn,
        from: RecordRef,
        to: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        self.put_ref_event(lsn, from, SegmentRefEvent::Retired);
        self.retire_overlay(from);
        self.add_ref(lsn, to, lifecycle)
    }

    /// Applies a lifecycle change for an already materialized payload ref.
    fn change_lifecycle(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        old: Option<BlobLifecycle>,
        new: Option<BlobLifecycle>,
    ) -> Result<()> {
        if old == new {
            return Ok(());
        }

        let epoch = self.epoch_at_lsn(lsn)?;
        if lifecycle_is_expired(old, epoch) {
            return Ok(());
        }
        if lifecycle_is_expired(new, epoch) {
            self.expire_ref(lsn, record_ref);
        } else {
            self.set_lifetime_overlay(record_ref, new);
            self.put_ref_event(
                lsn,
                record_ref,
                SegmentRefEvent::LifecycleChanged { lifecycle: new },
            );
        }
        Ok(())
    }

    /// Applies an epoch change to live lifecycle buckets and copy-planning overlays.
    ///
    /// Failure example: if this only updated the overlay summary, GC publish reconciliation would
    /// not have exact per-range ref events for records copied from an expiring segment.
    fn expire_live_refs(&mut self, lsn: StrataLsn, epoch: Epoch) -> Result<()> {
        for (segment_id, _) in self.index.iter_segment_states()? {
            let mut overlay = self
                .index
                .get_segment_gc_overlay(segment_id)?
                .unwrap_or_default();
            if let Some(ops) = self.gc_overlay_ops.get(&segment_id) {
                overlay.apply_merge_ops(ops.clone());
            }
            let lifetimes = overlay.lifetimes;
            let mut expired = Vec::new();
            for lifetime in lifetimes {
                if lifetime.lifecycle.logical_end_epoch <= epoch {
                    self.put_ref_event_range(
                        lsn,
                        segment_id,
                        lifetime.range,
                        SegmentRefEvent::Expired,
                    );
                    expired.push(lifetime.range);
                }
            }
            if !expired.is_empty() {
                self.stage_overlay_op(
                    segment_id,
                    SegmentGcOverlayMergeOp::ExpireBatch { ranges: expired },
                );
            }
        }
        Ok(())
    }

    fn epoch_at_lsn(&self, lsn: StrataLsn) -> Result<Epoch> {
        self.index
            .latest_epoch_at_lsn(lsn)?
            .map(|(_, epoch)| epoch)
            .ok_or(Error::EpochNotInitialized)
    }

    /// Stages an ordered event for a physical record reference.
    ///
    /// Failure example: without events, GC publish reconciliation could not see that a copied source
    /// range was retired after the accounting snapshot used to prepare the plan.
    fn put_ref_event(&mut self, lsn: StrataLsn, record_ref: RecordRef, event: SegmentRefEvent) {
        self.put_ref_event_range(
            lsn,
            record_ref.segment_id,
            SegmentGcRecordRange::from(record_ref),
            event,
        );
    }

    /// Stages a ref event when the caller already has the segment-local range.
    ///
    /// Failure example: if events were keyed only by record offset, two segments with the same
    /// offset would collide and one event would disappear.
    fn put_ref_event_range(
        &mut self,
        lsn: StrataLsn,
        segment_id: SegmentId,
        range: SegmentGcRecordRange,
        event: SegmentRefEvent,
    ) {
        self.ref_events.insert(
            SegmentRefEventKey {
                segment_id,
                lsn,
                offset: range.offset,
            },
            event,
        );
    }

    /// Stages a GC overlay operation that marks this record range retired.
    ///
    /// Failure example: without the overlay, sealed-segment GC would have to re-resolve blob
    /// history for every candidate record instead of reading compact per-segment hints.
    fn retire_overlay(&mut self, record_ref: RecordRef) {
        self.retire_overlay_range(
            record_ref.segment_id,
            SegmentGcRecordRange::from(record_ref),
        )
    }

    /// Accounts a newly materialized range that was already terminal before its `MapRef` was
    /// materialized.
    fn add_retired_overlay(&mut self, record_ref: RecordRef) {
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::AddRetiredBatch {
                ranges: vec![SegmentGcRecordRange::from(record_ref)],
            },
        )
    }

    /// Stages a GC overlay retire operation when the caller already has a segment-local range.
    fn retire_overlay_range(&mut self, segment_id: SegmentId, range: SegmentGcRecordRange) {
        self.stage_overlay_op(
            segment_id,
            SegmentGcOverlayMergeOp::RetireBatch {
                ranges: vec![range],
            },
        )
    }

    /// Stages a lifecycle-expiry transition for one physical record.
    fn expire_ref(&mut self, lsn: StrataLsn, record_ref: RecordRef) {
        self.put_ref_event(lsn, record_ref, SegmentRefEvent::Expired);
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::ExpireBatch {
                ranges: vec![SegmentGcRecordRange::from(record_ref)],
            },
        );
    }

    /// Stages a GC overlay operation that records or clears a record's lifetime hint.
    ///
    /// Failure example: if lifetime changes updated only summary counters, GC could reclaim a
    /// record whose logical lifetime was extended after the original put.
    fn set_lifetime_overlay(&mut self, record_ref: RecordRef, lifecycle: Option<BlobLifecycle>) {
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::LifetimeBatch {
                updates: vec![SegmentGcLifetimeUpdate {
                    range: SegmentGcRecordRange::from(record_ref),
                    lifecycle,
                }],
            },
        )
    }

    /// Returns the active relocation for a source ref if the event happened before its publish LSN.
    fn relocation_for(&self, lsn: StrataLsn, record_ref: RecordRef) -> Option<GcRelocation> {
        self.relocations
            .get(&record_ref)
            .copied()
            .filter(|relocation| lsn < relocation.publish_lsn)
    }

    /// Drops relocation rows whose `MapRef` has reached the accounted frontier.
    fn remove_relocations_through_lsn(&mut self, accounted_lsn: StrataLsn) {
        let expired = self
            .relocations
            .iter()
            .filter_map(|(from, relocation)| {
                (relocation.publish_lsn <= accounted_lsn).then_some(*from)
            })
            .collect::<Vec<_>>();
        for from in expired {
            self.relocations.remove(&from);
            self.removed_relocations.insert(from);
        }
    }

    /// Records an overlay merge operand for this commit.
    fn stage_overlay_op(&mut self, segment_id: SegmentId, op: SegmentGcOverlayMergeOp) {
        self.gc_overlay_ops.entry(segment_id).or_default().push(op);
    }
}
