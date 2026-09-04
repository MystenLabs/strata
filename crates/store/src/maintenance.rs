//! Background maintenance workers owned by the store: the garbage-log sweeper,
//! the blob-LSM memtable flusher, the blob-LSM compactor, and the relocation-LSM
//! flush/compaction helpers they share with the write path.
//!
//! Three loops run here, each on its own thread, each woken either by a nudge from the paths that
//! create its work or by a short fallback timer so nothing waits on a lost nudge:
//!
//! The sweeper folds committed global garbage frames into per-segment overlay files, and as a side
//! effect it reaffirms the durable relocation frontier after its own synced batches.
//! The flusher turns frozen blob-LSM memtables into patch SSTs so memory stays bounded and the
//! store WAL can be reclaimed. The compactor merges those patches back down, and while doing so it
//! is the *producer* of most of the garbage the GC pipeline consumes — when compaction folds an
//! overwrite of key "k", it emits the Retired event for k's old bytes into the global garbage log,
//! which the sweeper folds into that segment's overlay, which is what later tells the GC planner
//! that segment S7 is mostly dead. Compaction also heals: a full pass rewrites blob rows that
//! still point at relocated bytes (S7 refs) to their new home (S42), which is what eventually lets
//! the relocation entries themselves be dropped.
//!
//! Every durable publication in this file follows one shape: build the artifact (SST, garbage
//! frame) and sync it first; then merge the manifest edit into RocksDB in one synced batch; then
//! install the merged manifest into the in-memory LSM so readers see it. Because these batches are
//! synced, each one also hardens every earlier write in the RocksDB WAL. GC relocation activation
//! now syncs itself; the sweeper/compactor frontier updates are conservative reaffirmations.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, Weak, atomic::AtomicU64, mpsc},
    time::{Duration, Instant},
};

use crate::{
    BLOB_LSM_MANIFEST, Error, GARBAGE_LOG_HEAD, GARBAGE_LOG_SWEEP_CURSOR, GARBAGE_SWEEP_INTERVAL,
    LSM_COMPACTION_PATCH_BYTES, LSM_COMPACTION_PATCH_COUNT, LSM_COMPACTION_TARGET_BYTES,
    LSM_GARBAGE_LOG_MAX_BYTES, LSM_MEMTABLE_MAX_AGE, LSM_OBSOLETE_CLEANUP_INTERVAL,
    RELOCATION_LSM_MANIFEST, Result, StoreHalt, StrataStoreConfig, StrataStoreMetrics,
    blob_lsm::{BlobCompactionSnapshot, BlobMergeWithRelocations},
    gc::GcCommand,
    metrics::MainCompactionKind,
    relocation::{RelocationCache, RelocationMerge, RelocationStore},
    wal::WalReclaimer,
};
use core_types::SegmentFileState;
use index::StrataIndex;
use lsm::{
    GarbageLog, Lsm, Manifest as LsmManifest, ManifestEdit, TableMeta,
    select_base_compaction_inputs, select_compaction_inputs, select_patch_compaction_inputs,
    select_patch_group_inputs, write_compaction, write_patch_compaction,
};

/// Folds the global garbage log into per-segment overlays on a one-second cadence.
///
/// Compaction and GC publication append garbage events (Retired/Expired/SetLifecycle) to one
/// global append-only log. Nothing downstream reads that log directly — the planner and the
/// publish-time revalidation both read per-segment overlay files and summaries. This sweeper is
/// the bridge: it moves committed frames from the global log into each touched segment's local
/// file and summary row, batch by batch.
pub(crate) struct GarbageLogSweeper {
    pub(crate) index: StrataIndex,
    pub(crate) global_log_dir: PathBuf,
    pub(crate) namespace_dir: PathBuf,
    pub(crate) garbage_publish_lock: Arc<Mutex<()>>,
    pub(crate) relocations: Weak<RelocationStore>,
    pub(crate) durable_relocation_lsn: Arc<AtomicU64>,
    pub(crate) gc_txs: Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
    pub(crate) shutdown_rx: mpsc::Receiver<()>,
}

impl GarbageLogSweeper {
    /// The loop: drain everything, wake GC if anything moved, nap GARBAGE_SWEEP_INTERVAL (one
    /// second), repeat until shutdown.
    ///
    /// A drain that advanced anything broadcasts GcCommand::Run to every GC worker — freshly
    /// folded garbage is exactly what unlocks new plans (a segment's live counter hitting zero
    /// makes it deletable; new retired bytes make it fragmented enough to copy). A failed drain is
    /// printed and retried on the next tick rather than halting the store: sweeping is idempotent,
    /// resumes from its durable cursor, and a transient I/O error should not take down foreground
    /// writes.
    pub(crate) fn run(self) {
        loop {
            match self.drain() {
                Ok(true) => {
                    let gc_txs = self.gc_txs.lock().expect("gc tx list lock poisoned");
                    for gc_tx in gc_txs.iter() {
                        let _ = gc_tx.send(GcCommand::Run);
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    eprintln!("background Strata garbage-log sweep failed: {error:?}");
                }
            }
            match self.shutdown_rx.recv_timeout(GARBAGE_SWEEP_INTERVAL) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// Sweeps bounded batches until the global log is drained, and doubles as the durability
    /// heartbeat for GC relocation activations.
    ///
    /// Each iteration, under the garbage-publication lock: first sample the relocation LSM's
    /// last activation sequence, then run one bounded sweep. The sweep itself syncs each touched
    /// segment-local file and commits its cursor, positions, and summaries in one *synced* RocksDB
    /// batch. That sync is the whole trick: RocksDB WAL syncs are cumulative, so it also hardens
    /// every batch written before it. After the lock is released, `durable_relocation_lsn` is
    /// raised to the sampled sequence. Normal GC activation already synced and announced that
    /// sequence itself; this remains a conservative reaffirmation for recovered or legacy work.
    ///
    /// The ordering is load-bearing in both directions. The sample happens *under the same lock
    /// activations take*, so it can never observe a half-activated publish; and it happens
    /// *before* the synced sweep, so every sequence it promotes was activated by a batch that the
    /// sweep's sync provably covered. Sampling after the sweep could catch an activation that
    /// slipped in behind the sync and promote a sequence that is not durable yet — declaring safe
    /// a source deletion that a crash could still orphan. Because sweeps run every second, an
    /// activation becomes deletion-safe within about a second without GC ever issuing its own
    /// RocksDB fsync.
    fn drain(&self) -> Result<bool> {
        let mut advanced = false;
        loop {
            let (swept, relocation_lsn, expiry_frontier_advanced) = {
                let _publish_guard = self
                    .garbage_publish_lock
                    .lock()
                    .expect("garbage publish lock poisoned");
                let relocation_lsn = self
                    .relocations
                    .upgrade()
                    .map(|relocations| relocations.lsm().last_lsn())
                    .transpose()?
                    .flatten()
                    .unwrap_or_default();
                let swept = self.index.sweep_garbage_log(
                    &self.global_log_dir,
                    &self.namespace_dir,
                    GARBAGE_LOG_HEAD,
                    GARBAGE_LOG_SWEEP_CURSOR,
                )?;
                // A compaction publishes its new base frontiers and its garbage-log head in one
                // batch. Only the iteration that observes no remaining frame may expose that
                // coverage to GC: at this point every Expired event produced by those bases is in
                // the segment summaries. The shared publication lock prevents a new compaction or
                // GC publish from appending between the empty-log observation and the frontier
                // write below.
                let expiry_frontier_advanced = if swept {
                    false
                } else {
                    self.refresh_expiry_accounting_frontier()?
                };
                (swept, relocation_lsn, expiry_frontier_advanced)
            };
            advanced |= expiry_frontier_advanced;
            if !swept {
                break;
            }
            self.durable_relocation_lsn
                .fetch_max(relocation_lsn, std::sync::atomic::Ordering::Release);
            advanced = true;
        }
        Ok(advanced)
    }

    /// Advances the durable frontier only when both merge coverage and garbage accounting agree.
    ///
    /// Suppose epoch 50 was published at LSN 120. Major compaction may already have stamped every
    /// base as merged through 120, but an Expired frame can still sit between the global head and
    /// sweep cursor. Publishing 120 in that state would let GC read old `live_bytes`. Requiring an
    /// empty global log before copying the manifest-derived bound closes that final gap.
    ///
    /// The caller holds `garbage_publish_lock`, which serializes all garbage-head publishers.
    fn refresh_expiry_accounting_frontier(&self) -> Result<bool> {
        let head = self
            .index
            .get_garbage_log_position(GARBAGE_LOG_HEAD)?
            .unwrap_or_default();
        let cursor = self
            .index
            .get_garbage_log_position(GARBAGE_LOG_SWEEP_CURSOR)?
            .unwrap_or_default();
        if head != cursor {
            return Ok(false);
        }
        let Some(manifest) = self.index.get_lsm_manifest(BLOB_LSM_MANIFEST)? else {
            return Ok(false);
        };
        let candidate = manifest.merge_applied_through_lsn();
        let current = self
            .index
            .get_blob_expiry_accounted_lsn()?
            .unwrap_or_default();
        // The write-merge frontier is the same bound minus the per-base re-read requirement. It
        // is what lets GC judge known end epochs against the clock: once it passes an epoch
        // transition, no lifetime extension from before that transition is still unmerged, and
        // the drained log above guarantees every merged extension's hint reached its summary.
        let writes_candidate = manifest.writes_merged_through_lsn();
        let writes_current = self.index.get_blob_writes_merged_lsn()?.unwrap_or_default();
        // LSN 0 is only the genesis epoch and cannot expire a valid foreground write: lifetimes
        // must be strictly greater than the current epoch when assigned. Waiting for a positive
        // frontier also avoids waking GC on every empty store merely to publish genesis coverage.
        if candidate <= current && writes_candidate <= writes_current {
            return Ok(false);
        }

        let mut batch = self.index.batch();
        if candidate > current {
            self.index
                .put_blob_expiry_accounted_lsn_batch(&mut batch, candidate)?;
        }
        if writes_candidate > writes_current {
            self.index
                .put_blob_writes_merged_lsn_batch(&mut batch, writes_candidate)?;
        }
        batch.write_with_sync(true).map_err(index::Error::from)?;
        Ok(true)
    }
}

pub(crate) fn garbage_log_dir(config: &StrataStoreConfig) -> PathBuf {
    config.namespace_dir().join("garbage-log")
}

/// Turns frozen blob-LSM memtables into durable patch SSTs.
///
/// The foreground writer only appends to the active memtable; when a write rolls it, the frozen
/// generation sits in memory until this thread flushes it. Two things depend on that happening
/// promptly: memory (frozen memtables accumulate) and store-WAL reclamation (the WAL can only be
/// trimmed up to what both LSMs have materialized into SSTs).
pub(crate) struct LsmFlusher {
    pub(crate) index: StrataIndex,
    pub(crate) lsm: Weak<Lsm>,
    pub(crate) wake_rx: mpsc::Receiver<()>,
    pub(crate) compact_tx: mpsc::Sender<()>,
    pub(crate) store_halt: StoreHalt,
}

impl LsmFlusher {
    /// The loop: wake on a writer nudge (a commit just rolled a memtable) or on the
    /// LSM_MEMTABLE_MAX_AGE fallback tick (one second). A timer tick additionally calls
    /// roll_memtable_if_due so a quiet store still freezes an aging active memtable — without
    /// this, a trickle workload could keep the same memtable open forever and pin the WAL behind
    /// it. Then flush everything frozen, and nudge the compactor when new patches appeared. Any
    /// failure halts the store and the LSM: a store that cannot flush cannot bound memory or
    /// reclaim its WAL, and continuing would only push the failure somewhere less obvious.
    pub(crate) fn run(self) {
        loop {
            let timed = match self.wake_rx.recv_timeout(LSM_MEMTABLE_MAX_AGE) {
                Ok(()) => false,
                Err(mpsc::RecvTimeoutError::Timeout) => true,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            };
            let Some(lsm) = self.lsm.upgrade() else {
                return;
            };
            let mut rolled = false;
            if timed {
                for partition in 0..lsm.manifest().partition_count {
                    match lsm.roll_memtable_if_due(partition) {
                        Ok(partition_rolled) => rolled |= partition_rolled.is_some(),
                        Err(error) => {
                            let reason = format!("LSM memtable rollover failed: {error}");
                            self.store_halt.halt(reason.clone());
                            lsm.halt(reason);
                            return;
                        }
                    }
                }
            }
            if let Err(error) = self.flush_all(&lsm) {
                let reason = format!("LSM flush failed: {error}");
                self.store_halt.halt(reason.clone());
                lsm.halt(reason);
                return;
            }
            if (rolled || !timed) && self.compact_tx.send(()).is_err() {
                let reason = "LSM compactor stopped".to_owned();
                self.store_halt.halt(reason.clone());
                lsm.halt(reason);
                return;
            }
        }
    }

    /// Drains every frozen memtable into its own patch SST, then advances the materialized
    /// frontier.
    ///
    /// Each flush writes and syncs one SST. A table beyond `committed_lsn` becomes an in-memory
    /// pending table: readers use it immediately, but it does not enter the durable manifest until
    /// a later WAL sync covers its `max_lsn`. A crash removes such an unreferenced table and replays
    /// it from the WAL. Afterwards materialize_through(committed_lsn) advances the frontier across
    /// LSNs that have no keyed rows at all (epoch changes, relocation-only stretches); without
    /// that hop, one metadata-only LSN would pin store-WAL reclamation forever.
    fn flush_all(&self, lsm: &Lsm) -> Result<()> {
        let committed_lsn = self.index.get_committed_lsn()?;
        loop {
            let mut flushed = false;
            let partition_count = lsm.manifest().partition_count;
            for partition in 0..partition_count {
                if lsm.frozen_generations(partition)?.is_empty() {
                    continue;
                }
                let target = lsm.allocate_patch_target()?;
                flushed |= lsm
                    .flush_one(partition, target, committed_lsn, |edit| {
                        publish_blob_lsm_edit(&self.index, edit)
                    })?
                    .is_some();
            }
            if !flushed {
                break;
            }
        }
        lsm.materialize_through(committed_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        Ok(())
    }
}

/// Publishes the blob-LSM durability frontier and reclaims the store WAL away from the serialized
/// writer. Durability completions wake this worker, while the fallback tick catches SSTs that a
/// flusher finished after the last foreground commit.
pub(crate) struct WalReclaimWorker {
    pub(crate) index: StrataIndex,
    pub(crate) lsm: Arc<Lsm>,
    pub(crate) wal: WalReclaimer,
    pub(crate) wake_rx: mpsc::Receiver<()>,
    pub(crate) store_halt: StoreHalt,
    pub(crate) metrics: StrataStoreMetrics,
}

impl WalReclaimWorker {
    pub(crate) fn run(self) {
        loop {
            match self.wake_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
            while self.wake_rx.try_recv().is_ok() {}
            if let Err(error) = self.reclaim() {
                let reason = format!("store WAL reclamation failed: {error}");
                self.store_halt.halt(reason.clone());
                self.lsm.halt(reason);
                return;
            }
        }
    }

    fn reclaim(&self) -> Result<()> {
        let total_started = Instant::now();
        let committed_lsn = self.index.get_committed_lsn()?;

        let started = Instant::now();
        let published = self.lsm.publish_pending_through(committed_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        });
        self.metrics
            .record_wal_reclaim_phase("publish_pending", started.elapsed());
        published?;

        let started = Instant::now();
        let materialized = self.lsm.materialize_through(committed_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        });
        self.metrics
            .record_wal_reclaim_phase("materialize", started.elapsed());
        materialized?;

        let reclaim_through = self.lsm.manifest().materialized_through.unwrap_or_default();
        if reclaim_through == 0 {
            self.metrics
                .record_wal_reclaim_phase("total", total_started.elapsed());
            return Ok(());
        }

        let started = Instant::now();
        let plan = self.wal.plan(reclaim_through);
        self.metrics
            .record_wal_reclaim_phase("plan", started.elapsed());
        let plan = plan?;

        let started = Instant::now();
        let retained_from = plan.retained_from();
        let persisted = self.index.get_store_wal_retained_from()?;
        let current = persisted.unwrap_or(self.lsm.manifest().wal_retained_from);
        if retained_from > current || persisted.is_none() {
            let mut batch = self.index.batch();
            self.index
                .put_store_wal_retained_from_batch(&mut batch, retained_from.max(current))?;
            batch.write_with_sync(true).map_err(index::Error::from)?;
        }
        self.metrics
            .record_wal_reclaim_phase("boundary_sync", started.elapsed());

        let started = Instant::now();
        let reclaimed = self.wal.apply(plan);
        self.metrics
            .record_wal_reclaim_phase("unlink", started.elapsed());
        reclaimed?;
        self.metrics
            .record_wal_reclaim_phase("total", total_started.elapsed());
        Ok(())
    }
}

/// Merges blob-LSM patches down, emits the garbage events GC lives on, and heals stale refs.
///
/// One thread, three jobs per wake: compact the blob LSM if pressure warrants, compact the
/// relocation LSM if pressure warrants, and retry unlinking SSTs that were replaced earlier but
/// were still pinned by readers at the time.
pub(crate) struct LsmCompactor {
    pub(crate) index: StrataIndex,
    pub(crate) lsm: Weak<Lsm>,
    pub(crate) relocations: Weak<RelocationStore>,
    pub(crate) relocation_cache: Weak<RelocationCache>,
    pub(crate) durable_relocation_lsn: Arc<AtomicU64>,
    pub(crate) garbage_log_dir: PathBuf,
    pub(crate) compaction_admission_lock: Arc<RwLock<()>>,
    pub(crate) garbage_publish_lock: Arc<Mutex<()>>,
    pub(crate) wake_rx: mpsc::Receiver<()>,
    pub(crate) store_halt: StoreHalt,
    pub(crate) metrics: StrataStoreMetrics,
    pub(crate) obsolete: Vec<TableMeta>,
}

impl LsmCompactor {
    /// The loop: wake on a nudge (the flusher after new patches, the writer after a durability
    /// sync, GC after activating relocations) or on the one-second periodic deadline. Reaching the
    /// deadline sets `force`, which both bypasses the patch-pressure thresholds and requests the
    /// full (base-materializing) form, so healing and garbage discovery keep happening even when
    /// nudges arrive continuously. Any failure halts the store and the LSM — compaction publishes
    /// manifests, and a half-trusted manifest is not a state to keep running in.
    pub(crate) fn run(mut self) {
        let mut next_forced_pass = Instant::now() + LSM_OBSOLETE_CLEANUP_INTERVAL;
        loop {
            // Use an absolute deadline rather than a fresh timeout after every nudge. Otherwise a
            // continuously non-empty wake queue can postpone cold-base expiry indefinitely.
            let force = match next_forced_pass.checked_duration_since(Instant::now()) {
                None | Some(Duration::ZERO) => true,
                Some(wait) => match self.wake_rx.recv_timeout(wait) {
                    Ok(()) => false,
                    Err(mpsc::RecvTimeoutError::Timeout) => true,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return,
                },
            };
            let Some(lsm) = self.lsm.upgrade() else {
                return;
            };
            if let Err(error) = self.compact(&lsm, force) {
                let reason = format!("LSM compaction failed: {error}");
                self.store_halt.halt(reason.clone());
                lsm.halt(reason);
                return;
            }
            if let Err(error) = self.compact_relocations_if_needed() {
                let reason = format!("relocation LSM compaction failed: {error}");
                self.store_halt.halt(reason.clone());
                lsm.halt(reason);
                return;
            }
            self.cleanup_obsolete(&lsm);
            if force {
                // Schedule from completion rather than replaying missed ticks. A full compaction
                // can itself take longer than the interval; immediately replaying those ticks
                // could starve newly created patch work.
                next_forced_pass = Instant::now() + LSM_OBSOLETE_CLEANUP_INTERVAL;
            }
        }
    }

    /// Compacts the relocation LSM when a partition is under patch pressure.
    ///
    /// Every GC publish adds one patch SST to the relocation manifest, so a busy GC period grows
    /// a long patch chain that every relocation lookup must walk. Count pressure merges a size
    /// tier of patches among themselves; only once the patch tier is a set fraction of the base
    /// does a full pass fold it into new base tables (see `relocation_compaction_shape`). The
    /// admission lock is taken in read mode to exclude GC publication for the duration —
    /// activation edits the same manifest, and the merge-batch's live-file validation must not
    /// race it.
    ///
    /// The sample-then-advance dance around `durable_relocation_lsn` is the same trick the
    /// sweeper's drain uses, for the same reason: compact_relocation_lsm publishes its manifest
    /// with a synced batch, and that sync hardens every relocation activation written before the
    /// sample. See GarbageLogSweeper::drain for the full argument.
    fn compact_relocations_if_needed(&self) -> Result<()> {
        let Some(relocations) = self.relocations.upgrade() else {
            return Ok(());
        };
        let Some(relocation_cache) = self.relocation_cache.upgrade() else {
            return Ok(());
        };
        let manifest = relocations.lsm().manifest();
        let pressured = manifest
            .partitions
            .iter()
            .filter_map(|(&partition, tables)| {
                relocation_compaction_shape(&tables.patches, &tables.base)
                    .map(|shape| (partition, shape))
            })
            .collect::<Vec<_>>();
        if pressured.is_empty() {
            return Ok(());
        }

        let admission_lock = Arc::clone(&self.compaction_admission_lock);
        let _admission_guard = admission_lock
            .read()
            .expect("compaction admission lock poisoned");
        let relocation_lsn = relocations.lsm().last_lsn()?.unwrap_or_default();
        let mut compacted = false;
        for (partition, shape) in pressured {
            compacted |= compact_relocation_lsm_partition(
                &self.index,
                &relocations,
                &relocation_cache,
                &self.metrics,
                partition,
                shape,
            )?;
        }
        if compacted {
            self.durable_relocation_lsn
                .fetch_max(relocation_lsn, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// One blob-LSM compaction pass, from admission to installed manifest, in code order.
    ///
    /// Admission and thresholds. The admission lock is taken in read mode: compactions may run
    /// beside each other conceptually, but GC publication takes it in write mode, so a relocation
    /// view can never be reconciled and activated while a compaction is mid-flight (the TODO
    /// below describes the finer-grained future). Then the pressure gates: skip unless the patch
    /// count or patch bytes crossed their thresholds, or `force` (the periodic tick) says run
    /// anyway. A forced pass can also select one cold base whose expiry-accounting frontier is
    /// behind the latest epoch transition. For example, a base last merged at LSN 40 is selected
    /// after an epoch change at LSN 50 even when no user has written a patch over that key range.
    ///
    /// The durability gate. Compaction sees only the temporal patch prefix that is fully below
    /// published_lsn. Newer patches stay live in the real manifest but are absent from the
    /// selection view, so their key-range overlap cannot starve older durable patches. The merge
    /// snapshot stops before the first excluded patch: compaction must not apply a global event
    /// without seeing an earlier mutation held in a patch that straddles published_lsn.
    ///
    /// Two shapes of pass. Count pressure runs a *partial* pass: coalesce many small patches into
    /// fewer big ones — cheap, no relocation healing, no shard fencing. Byte pressure or the
    /// periodic force runs the *full* pass that materializes a base, and that is where the heavy
    /// machinery lives: a relocation scan over the input key range (bounded by the relocation
    /// LSM's current sequence) lets the merge rewrite blob rows that still point at relocated
    /// bytes — a row for key "a" still referencing S7 is healed to point at S42, counted in
    /// healed_references; the shard registry, drop LSNs, and already-deleted shard segments let
    /// it drop rows fenced by dropped generations; and the epoch-change history (up to the
    /// materialized frontier, capped by published_lsn) drives expiry decisions.
    ///
    /// This merge is where most garbage is born. When folding reveals that an overwrite retired
    /// key "k"'s old bytes in S7, the merge emits the Retired event for that range — the very
    /// events the sweeper later folds into S7's overlay, which is how the GC planner ever learns
    /// S7 is worth collecting.
    ///
    /// Publication. Under the garbage-publication lock: open the global garbage log at
    /// its committed head, then publish_lsm_compaction appends the garbage frame (synced) and
    /// commits the manifest edit plus the frame's end position in one synced RocksDB batch —
    /// SSTs first became durable in write_compaction, so the manifest never references bytes that
    /// could vanish. Finally the merged manifest is read back and installed in memory, metrics
    /// are recorded, and the replaced input SSTs go onto the deferred `obsolete` list rather than
    /// being unlinked here — a concurrent reader may still hold them pinned.
    fn compact(&mut self, lsm: &Lsm, force: bool) -> Result<()> {
        let partition_count = lsm.manifest().partition_count;
        for partition in 0..partition_count {
            self.compact_partition(lsm, partition, force)?;
        }
        Ok(())
    }

    fn compact_partition(&mut self, lsm: &Lsm, partition: u32, force: bool) -> Result<()> {
        // TODO: Replace this coarse admission barrier with late relocation resolution at garbage
        // publication time. Carry shard/payload identity and the original transition LSN so an
        // already-built compaction can retarget every event through the latest relocation map.
        // That would remove this GC/compaction exclusion and let GC initialize a destination from
        // a known source lifecycle instead of waiting for compaction to seed its baseline.
        let admission_lock = Arc::clone(&self.compaction_admission_lock);
        let _admission_guard = admission_lock
            .read()
            .expect("compaction admission lock poisoned");
        let manifest = lsm.manifest();
        let published_lsn = self.index.get_committed_lsn()?;
        let mut compaction_manifest = (*manifest).clone();
        let partition_patches = &compaction_manifest
            .partitions
            .get(&partition)
            .expect("validated manifest contains every partition")
            .patches;
        let compact_through_lsn = partition_patches
            .iter()
            .filter(|patch| {
                patch.max_lsn.expect("validated patch has a maximum LSN") > published_lsn
            })
            .map(|patch| {
                patch
                    .min_lsn
                    .expect("validated patch has a minimum LSN")
                    .saturating_sub(1)
            })
            .min()
            .map_or(published_lsn, |before_pending| {
                published_lsn.min(before_pending)
            });
        compaction_manifest
            .partitions
            .get_mut(&partition)
            .expect("validated manifest contains every partition")
            .patches
            .retain(|patch| {
                patch.max_lsn.expect("validated patch has a maximum LSN") <= compact_through_lsn
            });
        let partition_manifest = &compaction_manifest.partitions[&partition];
        let patches = &partition_manifest.patches;
        let patch_bytes = patches
            .iter()
            .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
        if !force
            && patches.len() < LSM_COMPACTION_PATCH_COUNT
            && patch_bytes < LSM_COMPACTION_PATCH_BYTES
        {
            return Ok(());
        }
        // The LSM frontier proves that every earlier keyed mutation is represented in SSTs;
        // publication is the store-wide durability bound for RocksDB-only transitions.
        let materialized_through_lsn = manifest
            .materialized_through
            .unwrap_or_default()
            .min(compact_through_lsn);
        let emit_garbage_from_lsn = self
            .index
            .get_blob_compaction_garbage_from_lsn()?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "blob compaction garbage cutover is missing".to_owned(),
            })?;
        let epoch_changes = self
            .index
            .iter_epoch_changes_from(0)?
            .into_iter()
            .filter(|(lsn, _)| *lsn <= materialized_through_lsn)
            .collect::<Vec<_>>();
        let expiry_target_lsn = epoch_changes
            .last()
            .map(|(lsn, _)| *lsn)
            .unwrap_or_default();

        // A forced pass advances expiry across one cold base per partition. Starting from the base
        // (rather than mixing it with unrelated patch-pressure work) keeps the output range
        // contiguous: if bases [a,f] and [n,z] have an untouched [g,m] base between them, writing
        // the two disjoint seeds into one SST would create an invalid [a,z] overlap. The base-seed
        // selector still pulls in every patch transitively connected to this one range.
        //
        // TODO: Replace the fixed one-base cadence with a backlog-aware scheduler. It should pick
        // the oldest merge frontier, derive an expiry work rate from the stale-base backlog, and
        // re-evaluate patch soft/hard pressure after every base so neither expiry nor new patch
        // compaction can starve the other.
        let stale_base = if force {
            partition_manifest
                .base
                .iter()
                .find(|base| base.merge_applied_through_lsn.unwrap_or_default() < expiry_target_lsn)
        } else {
            None
        };

        // Count pressure coalesces patches; byte pressure and the periodic pass materialize a base.
        // A quiet partition with no patches still enters the full path when `stale_base` exists,
        // which is the new cold-key expiry sweep.
        let partial = !force && patch_bytes < LSM_COMPACTION_PATCH_BYTES;
        if patches.is_empty() && stale_base.is_none() {
            return Ok(());
        }
        let tables = lsm.table_store();
        let selected = if partial {
            select_patch_compaction_inputs(&compaction_manifest, &tables, partition, patches)
        } else if let Some(base) = stale_base {
            select_base_compaction_inputs(&compaction_manifest, &tables, partition, base)
        } else {
            select_compaction_inputs(&compaction_manifest, &tables, partition, &patches[0])
        };
        let Some(inputs) = selected? else {
            return Ok(());
        };
        let obsolete = if partial {
            inputs.patches.clone()
        } else {
            inputs.base.iter().chain(&inputs.patches).cloned().collect()
        };
        let input_bytes = obsolete
            .iter()
            .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
        let started = Instant::now();
        let epoch_snapshot = BlobCompactionSnapshot {
            materialized_through_lsn,
            emit_garbage_from_lsn,
            epoch_changes,
            ..BlobCompactionSnapshot::default()
        };
        let relocation_max_lsn = self
            .relocations
            .upgrade()
            .map(|relocations| relocations.lsm().last_lsn())
            .transpose()?
            .flatten()
            .unwrap_or_default();
        let relocation_scan = match relocation_max_lsn {
            max_lsn if max_lsn != 0 => self
                .relocations
                .upgrade()
                .map(|relocations| {
                    relocations.scan(partition, &inputs.first_key, &inputs.last_key, max_lsn)
                })
                .transpose()?,
            _ => None,
        };
        let (edit, garbage, healed_references) = if partial {
            let merge = BlobMergeWithRelocations::new(relocation_scan, epoch_snapshot);
            let (edit, garbage) =
                write_patch_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
                    lsm.allocate_patch_target()
                })?;
            (edit, garbage, 0)
        } else {
            let snapshot = BlobCompactionSnapshot {
                shard_infos: self.index.iter_shards()?.into_iter().collect(),
                shard_drop_lsns: self
                    .index
                    .iter_shard_cleanup_jobs()?
                    .into_iter()
                    .map(|job| (job.shard, job.drop_lsn))
                    .collect(),
                reclaimed_shard_segments: self
                    .index
                    .iter_segment_states()?
                    .into_iter()
                    .filter_map(|(segment_id, state)| {
                        (state.state == SegmentFileState::Deleted)
                            .then(|| state.owner.shard().map(|shard| (segment_id, shard)))
                            .flatten()
                    })
                    .collect(),
                ..epoch_snapshot
            };
            let merge = BlobMergeWithRelocations::new(relocation_scan, snapshot);
            let (mut edit, garbage) =
                write_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
                    lsm.allocate_base_target()
                })?;
            // All rows in every output passed through `merge` with the epoch snapshot bounded by
            // this exact materialized frontier. If the frontier is 120, a later global coverage
            // calculation may count these bases as having considered the epoch transition at 120;
            // it must not stamp the store's newer `current_epoch` instead.
            for table in &mut edit.add_base {
                table.merge_applied_through_lsn = Some(materialized_through_lsn);
            }
            (edit, garbage, merge.healed_references())
        };
        let output_bytes = edit
            .add_base
            .iter()
            .chain(&edit.add_patches)
            .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));

        let _publish_guard = self
            .garbage_publish_lock
            .lock()
            .expect("garbage publication lock poisoned");
        let committed = self
            .index
            .get_garbage_log_position(GARBAGE_LOG_HEAD)?
            .unwrap_or_default();
        let mut garbage_log =
            GarbageLog::open(&self.garbage_log_dir, LSM_GARBAGE_LOG_MAX_BYTES, committed)?;
        self.index.publish_lsm_compaction(
            BLOB_LSM_MANIFEST,
            &edit,
            GARBAGE_LOG_HEAD,
            &mut garbage_log,
            &garbage,
        )?;
        lsm.reload_manifest(|| read_blob_lsm_manifest(&self.index))?;
        self.metrics.record_main_compaction(
            if partial {
                MainCompactionKind::Minor
            } else {
                MainCompactionKind::Full
            },
            compact_through_lsn,
            healed_references,
            input_bytes,
            output_bytes,
            started.elapsed(),
        );
        drop(inputs);
        self.obsolete.extend(obsolete);
        Ok(())
    }

    /// Retries unlinking SSTs that earlier compactions replaced.
    ///
    /// A replaced table cannot be removed while an in-flight reader still pins it, so each pass
    /// attempts every deferred table and keeps the ones that are still pinned for the next wake.
    /// An unlink error keeps the whole remainder and logs to stderr instead of halting — the
    /// files are unreferenced by any manifest, so the only cost of retrying later is disk space.
    fn cleanup_obsolete(&mut self, lsm: &Lsm) {
        let mut pending = std::mem::take(&mut self.obsolete);
        let mut retained = Vec::new();
        while let Some(table) = pending.pop() {
            match lsm.table_store().remove_if_unpinned(&table) {
                Ok(true) => {}
                Ok(false) => retained.push(table),
                Err(error) => {
                    eprintln!(
                        "background Strata obsolete-SST cleanup failed for {}: {error:?}",
                        table.relative_path
                    );
                    retained.push(table);
                    retained.extend(pending);
                    break;
                }
            }
        }
        self.obsolete = retained;
    }
}

/// Publishes one blob-LSM manifest edit as its own synced RocksDB batch and returns the merged
/// result.
///
/// This is the callback handed to flush_one/materialize_through: those helpers build the edit,
/// this function makes it durable, and the returned manifest is what they install in memory. The
/// sync is deliberate — flush and frontier publications run outside any other durability
/// envelope, so each edit must stand on its own. GC relocation activation follows the same rule
/// for its larger atomic metadata batch.
pub(crate) fn publish_blob_lsm_edit(
    index: &StrataIndex,
    edit: &ManifestEdit,
) -> lsm::Result<LsmManifest> {
    let publish = || -> Result<LsmManifest> {
        let mut batch = index.batch();
        index.merge_lsm_manifest_batch(&mut batch, BLOB_LSM_MANIFEST, edit)?;
        batch.write_with_sync(true).map_err(index::Error::from)?;
        index
            .get_lsm_manifest(BLOB_LSM_MANIFEST)?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "published blob LSM manifest is missing".to_owned(),
            })
    };
    publish().map_err(|error| lsm::Error::InvalidManifest {
        reason: format!("blob manifest publication failed: {error}"),
    })
}

pub(crate) fn read_blob_lsm_manifest(index: &StrataIndex) -> lsm::Result<LsmManifest> {
    read_lsm_manifest(index, BLOB_LSM_MANIFEST, "blob")
}

/// The relocation-manifest twin of publish_blob_lsm_edit; identical shape, different manifest row.
pub(crate) fn publish_relocation_lsm_edit(
    index: &StrataIndex,
    edit: &ManifestEdit,
) -> lsm::Result<LsmManifest> {
    let publish = || -> Result<LsmManifest> {
        let mut batch = index.batch();
        index.merge_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, edit)?;
        batch.write_with_sync(true).map_err(index::Error::from)?;
        index
            .get_lsm_manifest(RELOCATION_LSM_MANIFEST)?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "published relocation LSM manifest is missing".to_owned(),
            })
    };
    publish().map_err(|error| lsm::Error::InvalidManifest {
        reason: format!("relocation manifest publication failed: {error}"),
    })
}

pub(crate) fn read_relocation_lsm_manifest(index: &StrataIndex) -> lsm::Result<LsmManifest> {
    read_lsm_manifest(index, RELOCATION_LSM_MANIFEST, "relocation")
}

fn read_lsm_manifest(
    index: &StrataIndex,
    name: &str,
    description: &str,
) -> lsm::Result<LsmManifest> {
    index
        .get_lsm_manifest(name)
        .map_err(|error| lsm::Error::InvalidManifest {
            reason: format!("{description} manifest reload failed: {error}"),
        })?
        .ok_or_else(|| lsm::Error::InvalidManifest {
            reason: format!("published {description} LSM manifest is missing"),
        })
}

/// Flushes the relocation LSM's memtable rows into a patch SST, then compacts if pressure built.
///
/// The steady-state relocation path never writes memtables — GC publishes immutable L0 tables
/// directly. Memtable rows exist only from legacy shared-WAL recovery, which replays old-format
/// relocation entries as ordinary LSM writes at open. This flushes whatever is frozen, advances
/// the relocation materialized frontier through the LSM's own last sequence (each edit published
/// synced via publish_relocation_lsm_edit), and finally runs a compaction if the patch chain
/// crossed the shared thresholds.
pub(crate) fn flush_relocation_lsm(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let publish_through = relocations.lsm().last_lsn()?.unwrap_or_default();
    loop {
        let mut flushed = false;
        let partition_count = relocations.lsm().manifest().partition_count;
        for partition in 0..partition_count {
            let target = relocations.lsm().allocate_patch_target()?;
            flushed |= relocations
                .lsm()
                .flush_one(partition, target, publish_through, |edit| {
                    publish_relocation_lsm_edit(index, edit)
                })?
                .is_some();
        }
        if !flushed {
            break;
        }
    }
    relocations
        .lsm()
        .materialize_through(publish_through, |edit| {
            publish_relocation_lsm_edit(index, edit)
        })?;

    let manifest = relocations.lsm().manifest();
    let pressured = manifest
        .partitions
        .iter()
        .filter_map(|(&partition, tables)| {
            relocation_compaction_shape(&tables.patches, &tables.base)
                .map(|shape| (partition, shape))
        })
        .collect::<Vec<_>>();
    for (partition, shape) in pressured {
        compact_relocation_lsm_partition(
            index,
            relocations,
            relocation_cache,
            metrics,
            partition,
            shape,
        )?;
    }
    Ok(())
}

/// How one relocation-LSM compaction pass rewrites a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelocationCompactionShape {
    /// Merge one size tier of patches among themselves; base tables are neither read nor
    /// rewritten, and no entry is retired.
    Partial,
    /// Fold the oldest patch's overlap component into fresh base tables and retire entries whose
    /// destination segment is gone.
    Full,
}

impl RelocationCompactionShape {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Full => "full",
        }
    }
}

/// The patch tier is folded into the base only once it reaches this fraction of the base, so the
/// bytes rewritten into base tables stay a constant multiple of the bytes GC publishes instead of
/// a whole-base rewrite every few publishes.
const RELOCATION_FULL_COMPACTION_BASE_DIVISOR: u64 = 4;
/// A patch joins a tier merge when it is at most this many times larger than the smallest patch,
/// which keeps each byte's number of tier rewrites logarithmic in the tier size.
const RELOCATION_PATCH_TIER_SIZE_RATIO: u64 = 4;
/// Upper bound on the patches one tier merge reads.
const RELOCATION_PATCH_TIER_MAX_INPUTS: usize = 16;

/// Decides whether a relocation partition needs a pass, and which shape.
///
/// Byte pressure relative to the base selects the full pass. Otherwise the shared patch-count or
/// patch-byte pressure selects a tier merge, which `select_relocation_patch_tier` may still
/// decline when no two patches are of comparable size.
fn relocation_compaction_shape(
    patches: &[TableMeta],
    base: &[TableMeta],
) -> Option<RelocationCompactionShape> {
    if patches.is_empty() {
        return None;
    }
    let patch_bytes = patches
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
    let base_bytes = base
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
    let full_threshold =
        LSM_COMPACTION_PATCH_BYTES.max(base_bytes / RELOCATION_FULL_COMPACTION_BASE_DIVISOR);
    if patch_bytes >= full_threshold {
        return Some(RelocationCompactionShape::Full);
    }
    if patches.len() >= LSM_COMPACTION_PATCH_COUNT || patch_bytes >= LSM_COMPACTION_PATCH_BYTES {
        return Some(RelocationCompactionShape::Partial);
    }
    None
}

/// Picks the size tier to merge: the smallest patch and every patch within a constant factor of
/// it, in manifest order. Returns nothing when fewer than two patches are comparable, because
/// merging a small patch into a much larger one is exactly the rewrite the tier avoids.
fn select_relocation_patch_tier(patches: &[TableMeta]) -> Vec<TableMeta> {
    let Some(smallest) = patches.iter().map(|table| table.file_len).min() else {
        return Vec::new();
    };
    let limit = smallest
        .max(1)
        .saturating_mul(RELOCATION_PATCH_TIER_SIZE_RATIO);
    let mut tier = patches
        .iter()
        .filter(|table| table.file_len <= limit)
        .cloned()
        .collect::<Vec<_>>();
    tier.truncate(RELOCATION_PATCH_TIER_MAX_INPUTS);
    if tier.len() < 2 {
        tier.clear();
    }
    tier
}

/// Test helper: forces a full pass over every partition that has patches.
///
/// The merge does two things per key identity: keep only the newest value (Replace — a record
/// relocated twice keeps only its latest destination), and drop entries whose destination
/// segment has since been Deleted. That second rule is how relocation rows eventually die: once
/// compaction has healed every blob row that pointed into S42 and S42 itself is retired and
/// deleted, the A → S42 and D → S42 entries are pure dead weight, and this pass removes them and
/// evicts them from the relocation cache. Relocation compaction emits no garbage events (the
/// debug_assert) — destinations were accounted for by segment deletion, not by this fold.
///
/// Publication follows the standard shape: SSTs are durable from write_compaction, the manifest
/// edit commits in one synced batch, the merged manifest installs in memory. Unlike blob
/// compaction, the replaced inputs are unlinked immediately (still respecting reader pins) —
/// there is no deferred-obsolete list on this path. Returns whether a compaction actually ran,
/// which the caller uses to decide whether to advance `durable_relocation_lsn`.
#[cfg(test)]
pub(crate) fn compact_relocation_lsm(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
) -> Result<bool> {
    compact_relocation_lsm_with_shape(
        index,
        relocations,
        relocation_cache,
        metrics,
        RelocationCompactionShape::Full,
    )
}

/// Test helper: runs one pass of the given shape over every partition.
#[cfg(test)]
pub(crate) fn compact_relocation_lsm_with_shape(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
    shape: RelocationCompactionShape,
) -> Result<bool> {
    let partition_count = relocations.lsm().manifest().partition_count;
    let mut compacted = false;
    for partition in 0..partition_count {
        compacted |= compact_relocation_lsm_partition(
            index,
            relocations,
            relocation_cache,
            metrics,
            partition,
            shape,
        )?;
    }
    Ok(compacted)
}

/// One relocation-LSM compaction pass over one partition.
///
/// A partial pass reads only a size tier of patches and writes one merged patch: a record
/// relocated twice keeps only its latest destination, and nothing else changes. Selection is by
/// size rather than overlap component (`select_patch_group_inputs`), which is sound because the
/// merge is last-write-wins and every row keeps its own lsn. The output is not split by size so
/// tiers keep growing geometrically instead of degenerating into equal-sized files that would be
/// merged again and again.
///
/// A full pass is the original rewrite of one overlap-connected component into fresh base tables.
/// Besides keeping the latest destination per identity, it drops entries whose destination
/// segment has since been Deleted: once compaction has healed every blob row that pointed into
/// S42 and S42 itself is retired and deleted, the A → S42 and D → S42 entries are pure dead
/// weight, and this pass removes them and evicts them from the relocation cache. Relocation
/// compaction emits no garbage events (the debug_assert) — destinations were accounted for by
/// segment deletion, not by this fold.
///
/// Publication follows the standard shape: SSTs are durable from the writer, the manifest edit
/// commits in one synced batch, the merged manifest installs in memory. Unlike blob compaction,
/// the replaced inputs are unlinked immediately (still respecting reader pins) — there is no
/// deferred-obsolete list on this path. Returns whether a compaction actually ran, which the
/// caller uses to decide whether to advance `durable_relocation_lsn`.
fn compact_relocation_lsm_partition(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
    partition: u32,
    shape: RelocationCompactionShape,
) -> Result<bool> {
    let manifest = relocations.lsm().manifest();
    let partition_manifest = &manifest.partitions[&partition];
    let tables = relocations.lsm().table_store();
    let (inputs, merge) = match shape {
        RelocationCompactionShape::Partial => {
            let tier = select_relocation_patch_tier(&partition_manifest.patches);
            if tier.is_empty() {
                return Ok(false);
            }
            let Some(inputs) = select_patch_group_inputs(&manifest, &tables, partition, &tier)?
            else {
                return Ok(false);
            };
            (inputs, RelocationMerge::new(HashSet::new()))
        }
        RelocationCompactionShape::Full => {
            let Some(seed) = partition_manifest.patches.first() else {
                return Ok(false);
            };
            let Some(inputs) = select_compaction_inputs(&manifest, &tables, partition, seed)?
            else {
                return Ok(false);
            };
            let dead_segments = index
                .iter_segment_states()?
                .into_iter()
                .filter_map(|(segment_id, state)| {
                    (state.state == SegmentFileState::Deleted).then_some(segment_id)
                })
                .collect();
            (inputs, RelocationMerge::new(dead_segments))
        }
    };
    let obsolete = match shape {
        RelocationCompactionShape::Partial => inputs.patches.clone(),
        RelocationCompactionShape::Full => {
            inputs.base.iter().chain(&inputs.patches).cloned().collect()
        }
    };
    let input_bytes = obsolete
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
    let started = Instant::now();
    let (edit, garbage) = match shape {
        RelocationCompactionShape::Partial => {
            write_patch_compaction(&inputs, &merge, u64::MAX, || {
                relocations.lsm().allocate_patch_target()
            })?
        }
        RelocationCompactionShape::Full => {
            write_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
                relocations.lsm().allocate_base_target()
            })?
        }
    };
    debug_assert!(garbage.is_empty());
    let output_bytes = edit
        .add_base
        .iter()
        .chain(&edit.add_patches)
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));

    let mut batch = index.batch();
    index.merge_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, &edit)?;
    batch.write_with_sync(true).map_err(index::Error::from)?;
    relocations
        .lsm()
        .reload_manifest(|| read_relocation_lsm_manifest(index))?;
    let (examined, dropped) = merge.counts();
    let dropped_entries = merge.dropped_entries();
    debug_assert_eq!(dropped, dropped_entries.len() as u64);
    relocation_cache.remove_dropped(&dropped_entries);
    metrics.record_relocation_compaction(
        examined,
        dropped,
        input_bytes,
        output_bytes,
        started.elapsed(),
    );
    metrics.record_relocation_compaction_pass(shape.metric_label(), input_bytes);
    drop(inputs);
    for table in obsolete {
        tables.remove_if_unpinned(&table)?;
    }
    Ok(true)
}

#[cfg(test)]
mod relocation_compaction_tests {
    use super::*;

    fn meta(id: u64, file_len: u64) -> TableMeta {
        TableMeta {
            id,
            partition: 0,
            relative_path: format!("patch-{id}.sst"),
            first_key: b"a".to_vec(),
            last_key: b"z".to_vec(),
            min_lsn: Some(id),
            max_lsn: Some(id),
            merge_applied_through_lsn: None,
            record_count: 1,
            file_len,
            checksum: [0; 32],
        }
    }

    const MIB: u64 = 1 << 20;

    #[test]
    fn shape_needs_patches_and_pressure() {
        assert_eq!(relocation_compaction_shape(&[], &[]), None);
        assert_eq!(relocation_compaction_shape(&[meta(1, MIB)], &[]), None);
        let eight = (1..=8).map(|id| meta(id, MIB)).collect::<Vec<_>>();
        assert_eq!(
            relocation_compaction_shape(&eight, &[]),
            Some(RelocationCompactionShape::Partial)
        );
    }

    #[test]
    fn shape_goes_full_only_once_the_tier_is_a_fraction_of_the_base() {
        let base = vec![meta(100, 1024 * MIB)];
        let below = (1..=3).map(|id| meta(id, 80 * MIB)).collect::<Vec<_>>();
        assert_eq!(
            relocation_compaction_shape(&below, &base),
            Some(RelocationCompactionShape::Partial)
        );
        let above = (1..=4).map(|id| meta(id, 80 * MIB)).collect::<Vec<_>>();
        assert_eq!(
            relocation_compaction_shape(&above, &base),
            Some(RelocationCompactionShape::Full)
        );
        // With no base yet, the shared byte threshold alone selects the full pass.
        assert_eq!(
            relocation_compaction_shape(&[meta(1, LSM_COMPACTION_PATCH_BYTES)], &[]),
            Some(RelocationCompactionShape::Full)
        );
    }

    #[test]
    fn tier_selects_comparable_sizes_in_manifest_order() {
        let patches = vec![
            meta(1, MIB),
            meta(2, 10 * MIB),
            meta(3, MIB),
            meta(4, 4 * MIB),
        ];
        let tier = select_relocation_patch_tier(&patches);
        assert_eq!(
            tier.iter().map(|table| table.id).collect::<Vec<_>>(),
            [1, 3, 4]
        );
        assert!(select_relocation_patch_tier(&[meta(1, MIB), meta(2, 5 * MIB)]).is_empty());
        assert!(select_relocation_patch_tier(&[]).is_empty());
        let many = (1..=20).map(|id| meta(id, MIB)).collect::<Vec<_>>();
        assert_eq!(
            select_relocation_patch_tier(&many).len(),
            RELOCATION_PATCH_TIER_MAX_INPUTS
        );
    }
}
