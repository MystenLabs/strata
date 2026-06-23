//! Accounting is the store's sidecar-driven derived-state engine.
//!
//! Three readers need different breadcrumbs here:
//! - Systems engineer: preserve the frontier contract. Payload/index writes become durable first;
//!   the active delta log is made durable with that prefix; sidecar compaction then publishes
//!   derived stats, ref states, ref events, and GC overlays from compaction events.
//! - New contributor: the foreground writer never resolves blob keys for accounting. It appends
//!   cheap deltas; `AccountingSidecar` ingests those deltas into compact files and applies
//!   compaction events back to the main index for GC.
//! - Future maintainer: most choices below are defensive ordering choices. For example, advancing a
//!   cursor before publishing its prepared sidecar manifest would make replay skip deltas after a
//!   crash; applying event rows separately from the manifest would either duplicate or lose
//!   accounting effects after restart.
//!
//! Inline comments call out those perspectives as `Systems invariant`, `Ramp-up`, and
//! `Future-maintainer note` where the local code shape is otherwise surprising.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

use strata_accounting::{
    AccountingIndex, AccountingIndexConfig, ActiveDeltaLog, ActiveDeltaLogReadCursor,
    ActiveDeltaLogState, CompactionEventBatch, Manifest, RefEvent as AccountingRefEvent,
};
use strata_core::{
    BlobLifecycle, Epoch, RecordRef, SegmentGcLifetimeUpdate, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentId, SegmentRefEvent, SegmentRefEventKey, SegmentRefKey,
    SegmentRefState, SegmentRefStatus, SegmentState, SegmentStats, StrataLsn,
};
use strata_index::StrataIndex;
use typed_store::rocks::DBBatch;

use crate::{
    Error, Result,
    config::StrataStoreConfig,
    stats::{
        add_live_lifecycle_stats, expire_live_lifecycle_stats_through, lifecycle_is_expired,
        remove_live_lifecycle_stats,
    },
};

#[derive(Debug)]
pub(crate) enum AccountingCommand {
    /// Ask the background worker to catch accounting up soon.
    ///
    /// Failure example: without a cheap nudge from writes and syncs, accounting would only move on
    /// the periodic interval, so GC could keep reclaimable sealed segments pinned far longer than
    /// the writer intended.
    Run,
    /// Stop the background worker after pending channel work has drained.
    Shutdown,
}

/// Background accounting loop for one store.
///
/// It owns no mutable store state itself; instead it serializes sidecar maintenance over the shared
/// index: ingest durable active-log deltas, compact sidecar runs, then publish compaction events
/// back into the main index.
///
/// Failure example: if two sidecar runs raced, both could compact the same input run and publish
/// duplicate segment-stat deltas.
#[derive(Debug)]
pub(crate) struct AccountingWorker {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) interval: Duration,
    pub(crate) command_rx: mpsc::Receiver<AccountingCommand>,
    pub(crate) run_lock: Arc<Mutex<()>>,
}

impl AccountingWorker {
    /// Runs until shutdown or channel disconnect.
    ///
    /// Explicit `Run` commands are correctness nudges from writes/syncs: they force the sidecar to
    /// ingest and materialize durable deltas so `accounted_lsn` can advance. Timer ticks are cheaper
    /// maintenance passes that still respect size/count thresholds unless the wall-clock backstop
    /// fires.
    pub(crate) fn run(self) {
        let mut sidecar = AccountingSidecar::open(self.config.clone(), self.index.clone()).ok();
        loop {
            match self.command_rx.recv_timeout(self.interval) {
                Ok(AccountingCommand::Run) => {
                    // Systems invariant: one sidecar pass at a time. Compaction events are signed
                    // deltas, so duplicate application would corrupt segment stats.
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    Self::run_sidecar(
                        &mut sidecar,
                        &self.config,
                        &self.index,
                        SidecarRunMode::Forced,
                    );
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    Self::run_sidecar(
                        &mut sidecar,
                        &self.config,
                        &self.index,
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
        mode: SidecarRunMode,
    ) {
        if sidecar.is_none() {
            // Future-maintainer note: sidecar setup can fail transiently while the active log or
            // manifest is being initialized. Retry later instead of killing the worker.
            *sidecar = AccountingSidecar::open(config.clone(), index.clone()).ok();
        }
        if let Some(sidecar) = sidecar.as_mut() {
            let _ = match mode {
                SidecarRunMode::Forced => sidecar.run_forced(),
                SidecarRunMode::Maintenance => sidecar.run(),
            };
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SidecarRunMode {
    Forced,
    Maintenance,
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

/// Incremental sidecar builder for the active accounting delta log.
///
/// Ramp-up: the foreground writer appends `AccountingDelta`s to `active-delta.log`; this object
/// reads the durable part of that log, writes immutable sidecar runs, then records a manifest and
/// consumed cursor in the main index.
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
    fn run(&mut self) -> Result<()> {
        let force = self.last_forced_run.elapsed() >= self.config.accounting_sidecar_interval;
        self.run_once_materializing(force)?;
        if force {
            self.last_forced_run = Instant::now();
        }
        Ok(())
    }

    /// Forces a catch-up pass for an explicit writer/sync nudge.
    ///
    /// This bypasses the sidecar size/count thresholds because callers waiting on durability expect
    /// any durable accounting deltas to be reflected in GC-facing rows promptly.
    fn run_forced(&mut self) -> Result<()> {
        self.run_once_materializing(true)?;
        self.last_forced_run = Instant::now();
        Ok(())
    }

    /// Ingests new active-log deltas first, then compacts sidecar files.
    ///
    /// Failure example: compacting before ingestion would not corrupt data, but it can repeatedly
    /// compact stale partitions while a large active-log backlog continues to grow.
    #[cfg(test)]
    fn run_once(&mut self, force: bool) -> Result<()> {
        self.ingest_active_delta_log(force)?;
        self.compact_sidecar(force, false)?;
        Ok(())
    }

    /// Runs a sidecar pass that can advance the main-index accounting frontier.
    ///
    /// Major compaction is what materializes patch state into ordered ref events, so production
    /// forced passes must bypass major thresholds as well as ingest/delta thresholds.
    fn run_once_materializing(&mut self, force: bool) -> Result<()> {
        self.ingest_active_delta_log(force)?;
        self.compact_sidecar(force, force)?;
        Ok(())
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
        let read = ActiveDeltaLog::read_durable_range(
            self.config.accounting_index_dir(),
            cursor,
            durable_state,
        )?;
        if read.deltas.is_empty() {
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

        let next_cursor = read.next_cursor(cursor);
        let prepared = self
            .accounting_index
            .prepare_accounting_deltas(read.deltas)?;
        let manifest = prepared.manifest().clone();
        // Systems invariant: publish manifest + cursor before applying the in-memory manifest.
        // The run files were already written by `prepare_accounting_deltas`; if we crash after this
        // batch, reopening from RocksDB sees the new manifest and cursor together.
        self.commit_sidecar_state(Some(&manifest), Some(next_cursor), None)?;
        self.accounting_index
            .apply_prepared_accounting_deltas(prepared)?;
        Ok(true)
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
    ) -> Result<()> {
        let partitions = self
            .accounting_index
            .manifest()
            .partitions
            .keys()
            .copied()
            .collect::<Vec<_>>();

        // Future-maintainer note: collect keys first because each application updates the manifest.
        // Iterating the map directly while mutating it would either fail borrowing or skip work.
        for partition in partitions {
            if self.should_compact_deltas(partition, force_delta_compaction) {
                let prepared = self.accounting_index.prepare_compact_partition(partition)?;
                if !prepared.event_batch.input_run_ids.is_empty() {
                    let manifest = prepared.manifest().clone();
                    self.commit_sidecar_state(Some(&manifest), None, Some(&prepared.event_batch))?;
                    self.accounting_index.apply_prepared_compaction(prepared)?;
                }
            }

            if self.should_major_compact(partition, force_major_compaction) {
                let prepared = self
                    .accounting_index
                    .prepare_major_compact_partition(partition)?;
                if prepared.output.is_some() {
                    let manifest = prepared.manifest().clone();
                    self.commit_sidecar_state(Some(&manifest), None, Some(&prepared.event_batch))?;
                    self.accounting_index
                        .apply_prepared_major_compaction(prepared)?;
                }
            }
        }
        Ok(())
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
    /// batch is the state transition: manifest, consumed cursor, derived ref/stat rows, and frontier
    /// movement become visible together, then the WAL is fsynced before the in-memory sidecar mirror
    /// is advanced.
    fn commit_sidecar_state(
        &self,
        manifest: Option<&Manifest>,
        cursor: Option<ActiveDeltaLogReadCursor>,
        event_batch: Option<&CompactionEventBatch>,
    ) -> Result<()> {
        let mut context = SidecarAccountingContext::new(&self.index);
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
        context.write_to_batch(&mut batch)?;
        let current_accounted_lsn = self.index.get_accounted_lsn()?;
        if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            self.index
                .remove_unaccounted_lsn_ops_batch(&mut batch, &frontier.consumed_lsns)?;
            self.index
                .put_accounted_lsn_batch(&mut batch, frontier.accounted_lsn)?;
        }
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        Ok(())
    }
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
}

/// Scratchpad for one sidecar commit.
///
/// Ramp-up: sidecar compaction produces ref events. This context translates those events into the
/// main-index rows GC already consumes, while keeping the RocksDB commit atomic with the sidecar
/// manifest/cursor update.
///
/// Failure example: applying event rows outside the manifest commit would either duplicate signed
/// stat deltas after a retry or lose them after a crash.
struct SidecarAccountingContext<'a> {
    index: &'a StrataIndex,
    stats: BTreeMap<SegmentId, SegmentStats>,
    ref_states: BTreeMap<SegmentRefKey, SegmentRefState>,
    ref_events: BTreeMap<SegmentRefEventKey, SegmentRefEvent>,
    gc_overlay_ops: BTreeMap<SegmentId, Vec<SegmentGcOverlayMergeOp>>,
    states: BTreeMap<SegmentId, SegmentState>,
}

impl<'a> SidecarAccountingContext<'a> {
    fn new(index: &'a StrataIndex) -> Self {
        Self {
            index,
            stats: BTreeMap::new(),
            ref_states: BTreeMap::new(),
            ref_events: BTreeMap::new(),
            gc_overlay_ops: BTreeMap::new(),
            states: BTreeMap::new(),
        }
    }

    /// Applies all sidecar-produced ref events for one compaction.
    ///
    /// The events are structural; this layer classifies them against the epoch that was active at
    /// each event LSN so existing GC-facing `SegmentStats` buckets stay correct.
    fn apply_compaction_event_batch(&mut self, batch: &CompactionEventBatch) -> Result<()> {
        for event in &batch.events {
            self.apply_ref_event(event)?;
        }
        Ok(())
    }

    fn apply_ref_event(&mut self, event: &AccountingRefEvent) -> Result<()> {
        // The sidecar event stream is logical and key-oriented; the main index rows are
        // segment-oriented. This translation preserves that one event can fan out into several
        // physical rows: ref state/event history, placement-aware stats, and GC overlay operands.
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
            } => {
                // Main-index consumers already understand retire and live transitions. Decomposing a
                // map here keeps relocated bytes indistinguishable from "old range stopped being
                // protected, new range became protected" for stats, ref states, and overlay hints.
                self.retire_ref(*lsn, *from, *lifecycle)?;
                self.add_ref(*lsn, *to, *lifecycle)
            }
        }
    }

    /// Advances the global accounting cursor as far as sidecar materialization permits.
    ///
    /// This still inspects `unaccounted_lsn_ops`, but only to compute a contiguous global cleanup
    /// frontier. It does not resolve blob state or derive stats from blob keys.
    fn advance_frontier(
        &mut self,
        manifest: &Manifest,
        partition_for_key: impl Fn(&strata_core::BlobKey) -> u32,
    ) -> Result<FrontierUpdate> {
        let durable_lsn = self.index.get_durable_lsn()?;
        let mut accounted_lsn = self.index.get_accounted_lsn()?;
        let mut consumed_lsns = Vec::new();

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
                accounted_lsn = next_lsn;
                continue;
            }

            break;
        }

        Ok(FrontierUpdate {
            accounted_lsn,
            consumed_lsns,
        })
    }

    /// Stages all derived rows into the caller's sidecar metadata batch.
    fn write_to_batch(self, batch: &mut DBBatch) -> Result<()> {
        for (segment_id, stats) in self.stats {
            self.index
                .put_segment_stats_batch(batch, segment_id, &stats)?;
        }
        for (key, state) in self.ref_states {
            self.index.put_segment_ref_state_batch(batch, key, &state)?;
        }
        for (key, event) in self.ref_events {
            self.index.put_segment_ref_event_batch(batch, key, &event)?;
        }
        for (segment_id, ops) in self.gc_overlay_ops {
            self.index
                .merge_segment_gc_overlay_batch(batch, segment_id, ops)?;
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
        let state = self.segment_state(record_ref.segment_id)?.clone();
        let stats = self.segment_stats(record_ref.segment_id)?;
        stats.total_bytes = stats.total_bytes.saturating_add(record_ref.len);
        if lifecycle_is_expired(lifecycle, epoch) {
            // Expired-on-arrival bytes are still part of the segment's physical footprint, so
            // `total_bytes` increases, but they never enter a live bucket. The overlay receives a
            // retire immediately so copy planning can skip the range without waiting for epoch
            // advancement to revisit it.
            stats.expired_bytes = stats.expired_bytes.saturating_add(record_ref.len);
            self.put_ref_state(lsn, record_ref, SegmentRefStatus::Retired, lifecycle);
            self.put_ref_event(lsn, record_ref, SegmentRefEvent::Retired);
            self.retire_overlay(record_ref);
        } else {
            add_live_ref(stats, &state, record_ref.len, lifecycle);
            self.put_ref_state(lsn, record_ref, SegmentRefStatus::Live, lifecycle);
            if lifecycle.is_some() {
                self.set_lifetime_overlay(record_ref, lifecycle);
            }
        }
        Ok(())
    }

    /// Retires a materialized payload ref.
    ///
    /// Ref state, not just lifecycle, decides which aggregate bucket to move from. Sidecar events
    /// can be applied before the global `accounted_lsn` frontier reaches their LSN, so epoch
    /// expiration may not have been staged yet.
    fn retire_ref(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        let previous = self.ref_state(record_ref)?;
        if previous
            .as_ref()
            .is_some_and(|state| state.status == SegmentRefStatus::Live)
        {
            let previous_lifecycle = previous.as_ref().and_then(|state| state.lifecycle);
            let epoch = self.epoch_at_lsn(lsn)?;
            let segment = self.segment_state(record_ref.segment_id)?.clone();
            let stats = self.segment_stats(record_ref.segment_id)?;
            remove_live_ref(stats, &segment, record_ref.len, previous_lifecycle);
            if lifecycle_is_expired(previous_lifecycle, epoch) {
                stats.expired_bytes = stats.expired_bytes.saturating_add(record_ref.len);
            } else {
                stats.tombstoned_bytes = stats.tombstoned_bytes.saturating_add(record_ref.len);
            }
        }

        self.put_ref_state(lsn, record_ref, SegmentRefStatus::Retired, lifecycle);
        self.put_ref_event(lsn, record_ref, SegmentRefEvent::Retired);
        self.retire_overlay(record_ref);
        Ok(())
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
        let current = self.ref_state(record_ref)?;
        let current_status = current
            .as_ref()
            .map_or(SegmentRefStatus::Retired, |state| state.status);
        let old_counted_live = current_status == SegmentRefStatus::Live;
        let new_expired = lifecycle_is_expired(new, epoch);
        let segment = self.segment_state(record_ref.segment_id)?.clone();
        let stats = self.segment_stats(record_ref.segment_id)?;

        match (old_counted_live, new_expired) {
            (true, false) => {
                // Live before, live after: only the placement/lifetime bucket changes. The range
                // stays copy-eligible, so the overlay is updated with the replacement hint.
                remove_live_ref(stats, &segment, record_ref.len, old);
                add_live_ref(stats, &segment, record_ref.len, new);
                self.put_ref_state(lsn, record_ref, SegmentRefStatus::Live, new);
                self.set_lifetime_overlay(record_ref, new);
                self.put_ref_event(
                    lsn,
                    record_ref,
                    SegmentRefEvent::LifecycleChanged { lifecycle: new },
                );
            }
            (true, true) => {
                // Live before, expired after: move the bytes out of live accounting and mark the
                // physical range retired for GC. This is a lifecycle-driven retirement, not a user
                // tombstone, so it lands in the expired bucket.
                remove_live_ref(stats, &segment, record_ref.len, old);
                stats.expired_bytes = stats.expired_bytes.saturating_add(record_ref.len);
                self.put_ref_state(lsn, record_ref, SegmentRefStatus::Retired, new);
                self.retire_overlay(record_ref);
                self.put_ref_event(lsn, record_ref, SegmentRefEvent::Retired);
            }
            (false, false) => {
                // Retired/expired before, live after: a lifetime extension can make a range
                // copy-eligible again. Remove it from expired bytes, restore live bucket accounting,
                // and overwrite the overlay's dead knowledge with a lifetime update.
                stats.expired_bytes = stats.expired_bytes.saturating_sub(record_ref.len);
                add_live_ref(stats, &segment, record_ref.len, new);
                self.put_ref_state(lsn, record_ref, SegmentRefStatus::Live, new);
                self.set_lifetime_overlay(record_ref, new);
                self.put_ref_event(
                    lsn,
                    record_ref,
                    SegmentRefEvent::LifecycleChanged { lifecycle: new },
                );
            }
            (false, true) => {
                // Still retired after the change. Write the latest lifecycle into ref state for
                // audit/replay, but keep the overlay in retired form so GC continues to skip it.
                self.put_ref_state(lsn, record_ref, SegmentRefStatus::Retired, new);
                self.retire_overlay(record_ref);
            }
        }
        Ok(())
    }

    /// Applies an epoch change to live lifecycle buckets and per-record ref states.
    ///
    /// Failure example: if this only updated aggregate stats, later GC overlay scans would still see
    /// the expired records as live. If it only updated ref states, segment stats would continue to
    /// report those bytes as pinned.
    fn expire_live_refs(&mut self, lsn: StrataLsn, epoch: Epoch) -> Result<()> {
        for (segment_id, state) in self.index.iter_segment_states()? {
            let stats = self.segment_stats(segment_id)?;
            expire_live_lifecycle_stats_through(stats, state.placement_class, epoch);
        }
        let mut ref_states = self.index.iter_all_segment_ref_state()?;
        // Future-maintainer note: include updates already staged in this pass. Otherwise an LSN
        // sequence like "put live, increment epoch" could miss the just-added ref because it has not
        // been flushed to RocksDB yet.
        for (key, state) in &self.ref_states {
            if let Some(existing) = ref_states
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                existing.1 = *state;
            } else {
                ref_states.push((*key, *state));
            }
        }
        for (key, state) in ref_states {
            if state.status == SegmentRefStatus::Live
                && state
                    .lifecycle
                    .is_some_and(|lifecycle| lifecycle.logical_end_epoch <= epoch)
            {
                self.put_ref_state_key(lsn, key, SegmentRefStatus::Retired, state.lifecycle);
                self.put_ref_event_key(lsn, key, SegmentRefEvent::Retired);
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

    /// Loads and caches immutable segment state needed for placement-class-aware stats.
    ///
    /// Failure example: missing segment state means a blob version points at a segment accounting
    /// cannot classify; silently defaulting would put bytes into the wrong placement bucket.
    fn segment_state(&mut self, segment_id: SegmentId) -> Result<&SegmentState> {
        if !self.states.contains_key(&segment_id) {
            let state = self
                .index
                .get_segment_state(segment_id)?
                .ok_or(Error::AccountingMissingSegmentState { segment_id })?;
            self.states.insert(segment_id, state);
        }
        Ok(self.states.get(&segment_id).expect("state inserted above"))
    }

    /// Loads current segment stats or starts from zero, then returns the mutable staged copy.
    ///
    /// Failure example: reading from RocksDB for every transition would ignore earlier staged
    /// changes in the same pass and can double-count when several LSNs touch one segment.
    fn segment_stats(&mut self, segment_id: SegmentId) -> Result<&mut SegmentStats> {
        if !self.stats.contains_key(&segment_id) {
            let stats = self
                .index
                .get_segment_stats(segment_id)?
                .unwrap_or_default();
            self.stats.insert(segment_id, stats);
        }
        Ok(self
            .stats
            .get_mut(&segment_id)
            .expect("stats inserted above"))
    }

    fn ref_state(&self, record_ref: RecordRef) -> Result<Option<SegmentRefState>> {
        let key = SegmentRefKey {
            segment_id: record_ref.segment_id,
            offset: record_ref.offset,
        };
        if let Some(state) = self.ref_states.get(&key) {
            return Ok(Some(*state));
        }
        Ok(self.index.get_segment_ref_state(key)?)
    }

    /// Stages the latest state for a physical record reference.
    ///
    /// Failure example: without this state, epoch expiration and GC cannot tell whether a specific
    /// record offset is still live, retired, or only live until a future epoch.
    fn put_ref_state(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        status: SegmentRefStatus,
        lifecycle: Option<BlobLifecycle>,
    ) {
        self.put_ref_state_key(
            lsn,
            SegmentRefKey {
                segment_id: record_ref.segment_id,
                offset: record_ref.offset,
            },
            status,
            lifecycle,
        );
    }

    /// Stages an ordered event for a physical record reference.
    ///
    /// Failure example: without events, incremental GC overlay consumers would need to rescan all
    /// ref states after every accounting pass.
    fn put_ref_event(&mut self, lsn: StrataLsn, record_ref: RecordRef, event: SegmentRefEvent) {
        self.put_ref_event_key(
            lsn,
            SegmentRefKey {
                segment_id: record_ref.segment_id,
                offset: record_ref.offset,
            },
            event,
        );
    }

    /// Stages a ref state when the caller already has the compact segment/offset key.
    ///
    /// Failure example: epoch expiration works from `SegmentRefKey`s, not full `RecordRef`s, so
    /// forcing callers to reconstruct a fake record length would invite accidental wrong lengths.
    fn put_ref_state_key(
        &mut self,
        lsn: StrataLsn,
        key: SegmentRefKey,
        status: SegmentRefStatus,
        lifecycle: Option<BlobLifecycle>,
    ) {
        self.ref_states.insert(
            key,
            SegmentRefState {
                status,
                lifecycle,
                last_accounted_lsn: lsn,
            },
        );
    }

    /// Stages a ref event when the caller already has the compact segment/offset key.
    ///
    /// Failure example: if events were keyed only by record offset, two segments with the same
    /// offset would collide and one event would disappear.
    fn put_ref_event_key(&mut self, lsn: StrataLsn, key: SegmentRefKey, event: SegmentRefEvent) {
        self.ref_events.insert(
            SegmentRefEventKey {
                segment_id: key.segment_id,
                lsn,
                offset: key.offset,
            },
            event,
        );
    }

    /// Stages a GC overlay operation that marks this record range retired.
    ///
    /// Failure example: without the overlay, sealed-segment GC would have to re-resolve blob
    /// history for every candidate record instead of reading compact per-segment hints.
    fn retire_overlay(&mut self, record_ref: RecordRef) {
        self.gc_overlay_ops
            .entry(record_ref.segment_id)
            .or_default()
            .push(SegmentGcOverlayMergeOp::RetireBatch {
                ranges: vec![SegmentGcRecordRange::from(record_ref)],
            });
    }

    /// Stages a GC overlay operation that records or clears a record's lifetime hint.
    ///
    /// Failure example: if lifetime changes updated only stats, GC could reclaim a record whose
    /// logical lifetime was extended after the original put.
    fn set_lifetime_overlay(&mut self, record_ref: RecordRef, lifecycle: Option<BlobLifecycle>) {
        self.gc_overlay_ops
            .entry(record_ref.segment_id)
            .or_default()
            .push(SegmentGcOverlayMergeOp::LifetimeBatch {
                updates: vec![SegmentGcLifetimeUpdate {
                    range: SegmentGcRecordRange::from(record_ref),
                    lifecycle,
                }],
            });
    }
}

/// Adds bytes to the live bucket selected by placement class and lifecycle.
///
/// Failure example: bypassing the shared stats helper here could classify exact-epoch bytes
/// differently from the expiration path.
fn add_live_ref(
    stats: &mut SegmentStats,
    state: &SegmentState,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    add_live_lifecycle_stats(stats, state.placement_class, record_len, lifecycle);
}

/// Removes bytes from the live bucket selected by placement class and lifecycle.
///
/// Failure example: subtracting from a generic `live_bytes` counter would leave the per-lifetime
/// buckets inconsistent with aggregate segment stats.
fn remove_live_ref(
    stats: &mut SegmentStats,
    state: &SegmentState,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    remove_live_lifecycle_stats(stats, state.placement_class, record_len, lifecycle);
}
