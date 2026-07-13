//! High-level Strata blob store composed from segment files and the Strata index.
//!
//! This crate coordinates the storage protocol. Segment files hold bytes, the index holds
//! durable metadata, and the store is responsible for keeping both sides consistent across
//! writes, syncs, crashes, recovery, and sealing.
//!
//! Write path:
//!
//! ```text
//! StrataStore::put
//!   -> enqueue write command
//!   -> reserve the next store-global LSN
//!   -> SegmentWriter::append
//!   -> commit an atomic index batch:
//!        blob_versions[key] merge-append BlobEntry(record_ref)
//!        segment_states[(store, segment_id)].write_offset = end_of_record
//!        store_state[(store, NextLsn)] = lsn + 1
//!        unaccounted_lsn_ops[(store, lsn)] = key
//!   -> return to caller only after the batch commits
//! ```
//!
//! Epoch path:
//!
//! ```text
//! open new namespace
//!   -> epoch_changes[0] = starting_epoch
//!   -> store_state[CurrentEpoch] = starting_epoch
//!
//! StrataStore::increment_epoch
//!   -> submit a one-op batch containing BatchOp::IncrementEpoch
//!   -> reserve one store-global LSN
//!   -> commit the epoch row with the rest of the batch:
//!   -> epoch_changes[(store, lsn)] = current_epoch + 1
//!   -> store_state[(store, CurrentEpoch)] = current_epoch + 1
//!   -> store_state[(store, NextLsn)] = lsn + 1
//! ```
//!
//! Sync path:
//!
//! ```text
//! StrataStore::sync
//!   -> fsync active segment bytes
//!   -> fsync the active segment's accounting delta log through the committed LSN
//!   -> advance segment_states[active].durable_offset
//!   -> advance durable_lsn while unaccounted blob LSNs are covered by durable bytes
//!   -> publish store_state[DurableLsn] and ActiveDeltaLogState together
//!   -> fsync RocksDB WAL
//! ```
//!
//! Startup path:
//!
//! ```text
//! open
//!   -> validate config and create ingest directory
//!   -> discard stale GC staging directories
//!   -> discard or reject orphan segment files with no index state
//!   -> recover unsealed segments
//!   -> verify sealed segment files according to SealedSegmentIntegrityPolicy
//!   -> choose active segment
//!   -> start accounting, sealer, and writer workers
//! ```
//!
//! Crash model:
//!
//! - Unsealed segments are scanned from offset 0. The store keeps the longest valid prefix that
//!   is compatible with the recovery policy.
//! - Orphan segment files without index state are ignored by point-in-time recovery by deleting
//!   the file before any active writer is opened.
//! - Lost unaccounted LSNs are rolled back from `blob_versions` and `unaccounted_lsn_ops`.
//! - Sealed segments are expected to be stable. On open, their files must exist and match
//!   indexed length; optional checksum verification recomputes the sealed SHA-256 digest.
//! - `durable_lsn` means every logical operation up to that store-global LSN is recoverable after
//!   restart.
//!
//! Read path:
//!
//! ```text
//! get_blob
//!   -> resolve latest blob version from the packed blob-version state
//!   -> SegmentReader::read_record
//!   -> verify record key
//!   -> verify full-record checksum unless ReadOptions disables it
//!
//! stream_blob
//!   -> resolve latest blob version from the packed blob-version state
//!   -> read record header and key trailer
//!   -> validate requested payload range
//!   -> return a blocking file-range stream
//! ```
//!
//! Blob lifetime path:
//!
//! ```text
//! StrataStore::set_blob_lifetime
//!   -> append a metadata-only blob lifecycle op
//!   -> do not read segment state
//!   -> do not update GC overlay summary/ranges
//! ```
//!
//! Reads resolve payload and lifecycle state from the packed `blob_versions` row. The lifecycle
//! merge operator folds metadata ops into the blob-level head only through the blob compaction
//! frontier published by the index.
//!
//! Accounting path (background, see `accounting.rs`):
//!
//! ```text
//! accounting worker (interval tick or nudge from the writer)
//!   -> read the durable range of segment-aligned accounting delta logs
//!   -> write immutable sidecar delta runs and publish manifest + consumed cursor together
//!   -> compact delta runs into patch runs
//!   -> major-compact patches/base state, producing ordered ref events
//!   -> apply those events to segment ref events and GC overlay summary/ranges
//!   -> advance accounted_lsn while sidecar materialization covers the next durable LSN
//! ```
//!
//! The accounting worker is the writer of GC overlay summary/ranges, including mixed-ingest
//! retirements caused by shard drop. The foreground blob write path only appends cheap accounting
//! deltas and persists resumable cleanup jobs.
//! Accounting lag is expected: `accounted_lsn` says how far sidecar compaction events have been
//! reflected in the GC-facing rows. The sidecar manifest/cursor and derived rows commit atomically,
//! so crash retry reopens from one published sidecar state instead of replaying blob keys from the
//! packed version rows. Accounting delta log files are aligned with ingest segments. The writer
//! appends deltas in LSN order and rolls the active accounting log with the active segment; seal
//! workers fsync closed accounting logs in parallel with their matching segment files.

mod accounting;
mod config;
mod error;
mod gc;
mod gc_rate_limiter;
mod layout;
mod metrics;
mod read;
mod reader_cache;
mod seal;
mod shard_gc;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use strata_accounting::{
    AccountingDelta, ActiveDeltaLog, ActiveDeltaLogState, BlobUpdate,
    EpochChange as AccountingEpochChange, GcMapRefDelta,
};
#[cfg(test)]
use strata_core::SegmentGcLiveRecord;
use strata_core::{
    BlobKey, BlobLifecycle, BlobLifecycleAction, BlobLifecycleHead, BlobLifecycleMergeOp,
    BlobLifecycleOp, BlobState, BlobVersionKey, Epoch, GcRelocation, MapRefOp, PlacementClass,
    PutEntry, PutMergeOp, PutOp, RecordRef, SegmentFileState, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentId, SegmentOwner, SegmentRefEvent, SegmentState, ShardCleanupJob,
    ShardCleanupState, ShardId, ShardInfo, ShardKey, ShardState, StrataLsn, encoded_record_len,
};
use strata_gc::GcAction;
use strata_index::StrataIndex;
pub use strata_index::{AccountingRefEvent, AccountingSnapshot, AccountingSnapshotGuard};
use strata_segment::{SegmentScanner, SegmentWriter};

use accounting::{AccountingRequestSender, AccountingWorker, accounting_request_channel};
pub use config::{
    DEFAULT_ACCOUNTING_INTERVAL, DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN, DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
    DEFAULT_GC_SYNC_IMPACT_THRESHOLD, DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT,
    DEFAULT_SEAL_WORKER_COUNT, DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SEGMENT_READER_CACHE_CAPACITY,
    DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT, SealedSegmentIntegrityPolicy, StrataRecoveryPolicy,
    StrataStoreConfig,
};
pub use error::{Error, Result};
pub use gc::{
    GcAccountingLag, GcPublishResult, GcPublishedOutputSegment, GcPublishedRecord,
    GcStagedCopiedRecord, GcStagedOutputSegment, PreparedGcCopy, PreparedGcPlan,
};
use gc::{
    GcCommand, GcConcurrencyConfig, GcConcurrencyController, GcExecutor, GcPrepublishedCopy,
    GcPrepublishedOutputSegment, GcSourceClaims, GcWorker,
};
use gc_rate_limiter::GcIoLimiter;
use layout::{
    parse_segment_file_name, relative_segment_path, retention_dir, segment_path, segment_state_path,
};
use metrics::PutMetric;
pub use metrics::StrataStoreMetrics;
pub use read::{ReadOptions, StoreGetProfile};
use reader_cache::SegmentReaderCache;
use seal::{
    SealCommand, SealWorker, SegmentSealTask, active_segment_durable_offset,
    durable_lsn_with_accounting_frontier, enqueue_unsealed_segments_for_sealing,
    verify_sealed_segments,
};
use shard_gc::shard_generation_is_obsolete;
pub use strata_gc::{GcPlanner, GcPlannerConfig};

const FIRST_SEGMENT_ID: SegmentId = 1;
/// How long the writer naps while waiting for the sealer to drain its backlog. Short, because
/// this sleep sits on the foreground put path during rollover backpressure.
const SEAL_BACKLOG_WAIT: Duration = Duration::from_millis(10);
const DURABILITY_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(20 * 60);
/// Default logical shard used by the standalone convenience APIs.
pub(crate) const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};
/// Explicit owner used by mixed ingest segment metadata.
pub(crate) const INGEST_SEGMENT_OWNER: SegmentOwner = SegmentOwner::Store;

/// Single-namespace Strata store.
#[derive(Debug)]
pub struct StrataStore {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) write_tx: Option<mpsc::SyncSender<WriteCommand>>,
    writer_handle: Option<JoinHandle<()>>,
    seal_tx: Option<mpsc::Sender<SealCommand>>,
    seal_handle: Option<JoinHandle<()>>,
    accounting_tx: Option<AccountingRequestSender>,
    accounting_handle: Option<JoinHandle<()>>,
    pub(crate) gc_txs: Vec<mpsc::Sender<GcCommand>>,
    gc_handles: Vec<JoinHandle<()>>,
    pub(crate) accounting_lock: Arc<Mutex<()>>,
    pub(crate) gc_publish_cleanup_lock: Arc<Mutex<()>>,
    pub(crate) gc_claims: Arc<GcSourceClaims>,
    pub(crate) gc_concurrency: Arc<GcConcurrencyController>,
    pub(crate) gc_io_limiter: Arc<GcIoLimiter>,
    pub(crate) segment_ids: SegmentIdAllocator,
    pub(crate) reader_cache: Arc<SegmentReaderCache>,
    pub(crate) store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
}

/// What the read path needs from the index: where the payload bytes live, plus the current
/// blob-level lifecycle when one has been recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBlobVersion {
    pub head_lsn: StrataLsn,
    pub record_ref: strata_core::RecordRef,
    pub generation: strata_core::Generation,
    pub lifecycle: Option<BlobLifecycle>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StoreHalt {
    reason: Arc<Mutex<Option<String>>>,
}

impl StoreHalt {
    fn halt(&self, reason: impl Into<String>) {
        let mut guard = self.reason.lock().expect("store halt lock poisoned");
        if guard.is_none() {
            *guard = Some(reason.into());
        }
    }

    fn error(&self) -> Option<Error> {
        self.reason
            .lock()
            .expect("store halt lock poisoned")
            .clone()
            .map(|reason| Error::StoreHalted { reason })
    }

    fn check(&self) -> Result<()> {
        match self.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentIdAllocator {
    next_segment_id: Arc<Mutex<SegmentId>>,
}

impl SegmentIdAllocator {
    fn new(next_segment_id: SegmentId) -> Self {
        Self {
            next_segment_id: Arc::new(Mutex::new(next_segment_id)),
        }
    }

    pub(crate) fn allocate(&self) -> Result<SegmentId> {
        let mut next = self
            .next_segment_id
            .lock()
            .expect("segment id allocator lock poisoned");
        let segment_id = *next;
        *next = next
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        Ok(segment_id)
    }
}

impl StrataStore {
    /// Opens a standalone store using the configured on disk index directory.
    ///
    /// Callers should not separately open the index and then start store
    /// workers out of order. For example, starting a writer before orphan file reconciliation can
    /// make a segment file left by a crashed rollover look like usable active data.
    pub fn open(config: StrataStoreConfig, metrics: StrataStoreMetrics) -> Result<Self> {
        let index = StrataIndex::open_path(
            config.standalone_index_dir(),
            config.index_cf_prefix(),
            config.namespace.as_str(),
        )?;
        Self::from_index(config, index, metrics)
    }

    /// Opens a store around an already created index handle.
    ///
    /// Tests and embedders that share an index still get the exact same
    /// recovery sequencing as `open`. If this bypassed `open_inner`, a stale segment state could
    /// survive in the shared index while the writer appends new bytes against a different view.
    pub fn from_index(
        config: StrataStoreConfig,
        index: StrataIndex,
        metrics: StrataStoreMetrics,
    ) -> Result<Self> {
        Self::open_inner(config, index, metrics)
    }

    /// The real open path. The order of the recovery steps is deliberate and most of them only
    /// make sense before any worker thread exists:
    ///
    /// 1. Orphan segment files are reconciled first so a file from a crashed rollover can't be
    ///    mistaken for real data once a writer is running. Orphan files can happen because when
    ///    sealing a segment, we write to the index update batch, but the process can crash before
    ///    it could flush the memtable and fsync the RocksDB WAL for that strata index update.
    ///    So we need to reconcile orphan files before starting the writer.
    /// 2. Unsealed segments are scanned and truncated, lost LSNs are rolled back, and the durable
    ///    frontier is recomputed.
    /// 3. Sealed segments are only *verified*; they were declared immutable at seal time, so
    ///    anything wrong with them is an error, not something to repair silently.
    /// 4. Only then are the workers started: sealer first (the writer hands rollovers to it), then
    ///    the writer, with accounting spawned alongside since both the writer and sealer nudge it.
    ///    GC starts after the writer because GC publish uses the same serialized write queue.
    ///
    /// Everything mutable ends up owned by the writer thread; the `StrataStore` handle itself
    /// only holds channels, the index, and the read-side cache.
    fn open_inner(
        config: StrataStoreConfig,
        index: StrataIndex,
        metrics: StrataStoreMetrics,
    ) -> Result<Self> {
        validate_config(&config)?;
        ensure_ingest_dir(&config)?;
        cleanup_stale_gc_staging_dirs(&config)?;
        ensure_default_shard_registered(&index)?;
        ensure_epoch_initialized(&index, config.starting_epoch)?;
        reconcile_orphan_ingest_segment_files(&config, &index)?;
        recover_unsealed_segments(&config, &index, &metrics)?;
        let current_epoch = index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)?;
        cleanup_pending_gc_outputs(&config, &index)?;
        verify_sealed_segments(&config, &index)?;
        let active_segment_id = choose_active_segment_id(&index)?;
        let active_accounting_delta_log =
            recover_active_accounting_delta_log(&config, &index, &metrics, active_segment_id)?;
        let segment_ids =
            SegmentIdAllocator::new(next_segment_id_after(&index, active_segment_id)?);
        let active_writer = open_active_writer(&config, active_segment_id)?;
        let durable_offset = active_segment_durable_offset(&index, active_writer.segment_id())?;
        let store_state = index.get_store_state()?.unwrap_or_default();
        let active_segment_state = publish_active_segment_state(
            &config,
            &index,
            INGEST_SEGMENT_OWNER,
            &active_writer,
            durable_offset,
        )?;
        metrics.set_active_segment(
            active_writer.segment_id(),
            active_writer.write_offset(),
            durable_offset,
        );
        metrics.set_lsn_state(store_state.next_lsn, store_state.durable_lsn);
        metrics.set_current_epoch(current_epoch);
        metrics.set_unsealed_segments(unsealed_ingest_segment_count(&index)?);
        let (seal_tx, seal_rx) = mpsc::channel();
        let (
            accounting_tx,
            accounting_rx,
            pending_accounting_request,
            pending_materialize_through_lsn,
        ) = accounting_request_channel();
        let accounting_lock = Arc::new(Mutex::new(()));
        let gc_publish_cleanup_lock = Arc::new(Mutex::new(()));
        let accounting_gc_txs = Arc::new(Mutex::new(Vec::new()));
        let gc_claims = Arc::new(GcSourceClaims::default());
        let gc_io_limiter = Arc::new(GcIoLimiter::new(config.gc_io_bytes_per_sec));
        let store_halt = StoreHalt::default();
        let durability_publish_lock = Arc::new(Mutex::new(()));
        let gc_concurrency = Arc::new(GcConcurrencyController::new(
            GcConcurrencyConfig::from_store_config(&config),
            metrics.clone(),
        ));
        let accounting_handle = if config.accounting_worker_enabled {
            let accounting_worker = AccountingWorker {
                config: config.clone(),
                index: index.clone(),
                interval: config.accounting_interval,
                command_rx: accounting_rx,
                pending_request: pending_accounting_request,
                pending_materialize_through_lsn,
                run_lock: Arc::clone(&accounting_lock),
                gc_txs: Arc::clone(&accounting_gc_txs),
            };
            Some(
                thread::Builder::new()
                    .name(format!("strata-accounting-{}", config.namespace))
                    .spawn(move || accounting_worker.run())
                    .map_err(|source| Error::ThreadSpawn { source })?,
            )
        } else {
            drop(accounting_rx);
            None
        };
        let store_accounting_tx = accounting_handle.as_ref().map(|_| accounting_tx.clone());
        let seal_worker = SealWorker {
            config: config.clone(),
            index: index.clone(),
            ingest_owner: INGEST_SEGMENT_OWNER,
            seal_rx,
            durability_publish_lock: Arc::clone(&durability_publish_lock),
            accounting_tx: store_accounting_tx.clone(),
            metrics: metrics.clone(),
            store_halt: store_halt.clone(),
        };
        let seal_handle = thread::Builder::new()
            .name(format!("strata-sealer-{}", config.namespace))
            .spawn(move || seal_worker.run())
            .map_err(|source| Error::SealThreadSpawn { source })?;
        enqueue_unsealed_segments_for_sealing(&index, active_segment_id, &seal_tx, &metrics)?;

        let (write_tx, write_rx) = mpsc::sync_channel(config.write_queue_capacity);
        let reader_cache = Arc::new(SegmentReaderCache::new(
            config.segment_reader_cache_capacity,
        ));
        let coordinator = WriteCoordinator {
            config: config.clone(),
            index: index.clone(),
            active_writer,
            active_accounting_delta_log,
            durability_publish_lock: Arc::clone(&durability_publish_lock),
            active_segment_state,
            durable_offset,
            last_checkpoint_at: Instant::now(),
            last_checkpoint_next_lsn: store_state.next_lsn,
            pending_rollovers: Vec::new(),
            segment_ids: segment_ids.clone(),
            seal_tx: seal_tx.clone(),
            accounting_tx: store_accounting_tx.clone(),
            write_rx,
            ingest_owner: INGEST_SEGMENT_OWNER,
            reader_cache: Arc::clone(&reader_cache),
            gc_concurrency: Arc::clone(&gc_concurrency),
            store_halt: store_halt.clone(),
            metrics: metrics.clone(),
        };
        let writer_handle = thread::Builder::new()
            .name(format!("strata-writer-{}", config.namespace))
            .spawn(move || coordinator.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let configured_gc_workers = if config.gc_workers_enabled {
            config.gc_worker_count
        } else {
            0
        };
        let mut gc_txs = Vec::with_capacity(configured_gc_workers);
        let mut gc_handles = Vec::with_capacity(configured_gc_workers);
        for worker_index in 0..configured_gc_workers {
            let (gc_tx, gc_rx) = mpsc::channel();
            let gc_worker = GcWorker {
                executor: GcExecutor {
                    config: config.clone(),
                    index: index.clone(),
                    write_tx: write_tx.clone(),
                    accounting_lock: Arc::clone(&accounting_lock),
                    publish_cleanup_lock: Arc::clone(&gc_publish_cleanup_lock),
                    claims: Arc::clone(&gc_claims),
                    gc_concurrency: Arc::clone(&gc_concurrency),
                    gc_io_limiter: Arc::clone(&gc_io_limiter),
                    segment_ids: segment_ids.clone(),
                    reader_cache: Arc::clone(&reader_cache),
                    store_halt: store_halt.clone(),
                    metrics: metrics.clone(),
                },
                planner: GcPlanner::new(config.gc_planner_config.clone()),
                interval: config.gc_interval,
                command_rx: gc_rx,
            };
            match thread::Builder::new()
                .name(format!("strata-gc-{}-{worker_index}", config.namespace))
                .spawn(move || gc_worker.run())
            {
                Ok(gc_handle) => {
                    accounting_gc_txs
                        .lock()
                        .expect("gc tx list lock poisoned")
                        .push(gc_tx.clone());
                    gc_txs.push(gc_tx);
                    gc_handles.push(gc_handle);
                }
                Err(source) => {
                    for gc_tx in gc_txs {
                        let _ = gc_tx.send(GcCommand::Shutdown);
                    }
                    for gc_handle in gc_handles {
                        let _ = gc_handle.join();
                    }
                    return Err(Error::ThreadSpawn { source });
                }
            }
        }

        let pending_shard_cleanup_lsn = index
            .iter_shard_cleanup_jobs()?
            .into_iter()
            .map(|job| job.drop_lsn)
            .max();
        if let Some(drop_lsn) = pending_shard_cleanup_lsn {
            if let Some(accounting_tx) = &store_accounting_tx {
                accounting_tx.request_materialize(drop_lsn);
            }
            for gc_tx in &gc_txs {
                let _ = gc_tx.send(GcCommand::Run);
            }
        }

        Ok(Self {
            reader_cache,
            config,
            index,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
            seal_tx: Some(seal_tx),
            seal_handle: Some(seal_handle),
            accounting_tx: store_accounting_tx,
            accounting_handle,
            gc_txs,
            gc_handles,
            accounting_lock,
            gc_publish_cleanup_lock,
            gc_claims,
            gc_concurrency,
            gc_io_limiter,
            segment_ids,
            store_halt,
            metrics,
        })
    }

    pub fn config(&self) -> &StrataStoreConfig {
        &self.config
    }

    pub fn index(&self) -> &StrataIndex {
        &self.index
    }

    pub fn metrics(&self) -> &StrataStoreMetrics {
        &self.metrics
    }

    /// Current number of background GC workers the runtime tuner may admit concurrently.
    pub fn gc_active_worker_limit(&self) -> usize {
        if self.config.gc_workers_enabled {
            self.gc_concurrency.active_limit()
        } else {
            0
        }
    }

    /// Current store-wide background GC I/O budget selected by the runtime tuner.
    pub fn gc_active_io_bytes_per_sec(&self) -> u64 {
        if self.config.gc_workers_enabled {
            self.gc_concurrency.active_io_bytes_per_sec()
        } else {
            0
        }
    }

    #[cfg(test)]
    fn shard(&self) -> ShardKey {
        STANDALONE_SHARD
    }

    /// Starts a client-side batch whose operations commit under one store-global LSN allocation.
    ///
    /// Callers that need "put blob, then increment epoch" should not issue
    /// separate commands and hope no other writer interleaves. Without this batch wrapper another
    /// put could land between them and accounting would replay a different history than intended.
    pub fn batch(&self) -> StrataBatch<'_> {
        StrataBatch {
            store: self,
            ops: Vec::new(),
        }
    }

    /// Reads the current shard registry entry.
    ///
    /// Writers must observe generation changes after drop/re-add. A caller
    /// that cached only `shard_id = 7` would otherwise be unable to tell old generation 0 data from
    /// newly-created generation 1 data.
    pub fn shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.index.get_shard_info(shard_id)?)
    }

    /// Registers a logical shard, or returns its current active generation.
    ///
    /// Shard creation is serialized through the writer so two concurrent
    /// creators cannot both decide that shard 12 starts at generation 0 and race to publish
    /// conflicting registry rows.
    pub fn add_shard(&self, shard_id: ShardId) -> Result<ShardKey> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::AddShard(AddShardRequest {
            shard_id,
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    /// Durably fences a logical shard generation and schedules asynchronous reclamation.
    pub fn drop_shard(&self, shard_id: ShardId) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::DropShard(DropShardRequest {
            shard_id,
            response_tx,
        }))?;
        let shard = response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        if let (Some(accounting_tx), Some(job)) = (
            self.accounting_tx.as_ref(),
            self.index.get_shard_cleanup_job(shard)?,
        ) {
            accounting_tx.request_materialize(job.drop_lsn);
        }
        Ok(())
    }

    /// Writes a blob and returns its LSN. Returning means *visible*, not durable: the bytes are
    /// in the segment file and the index points at them, but only `sync` (or the periodic sync)
    /// makes them crash-safe. Callers that need durability gate on `durable_lsn() >= lsn`.
    /// For example, if a caller acknowledges an upstream event immediately after `put` and the
    /// machine loses power before `sync`, recovery may roll the blob back while the upstream event
    /// cursor has already advanced.
    ///
    /// All mutations are funneled through one writer thread (see `WriteCoordinator`), so this
    /// just packages the request and blocks on the response channel.
    pub fn put(&self, shard_id: ShardId, key: &BlobKey, payload: &[u8]) -> Result<StrataLsn> {
        self.put_arc(shard_id, key.clone(), Arc::from(payload))
    }

    /// Writes a blob from shared bytes without forcing the caller to copy them first.
    ///
    /// Write queue can hold the payload until the writer thread reaches
    /// it. Passing borrowed bytes across that boundary would let the caller mutate or drop the
    /// buffer before the segment append actually happens.
    pub fn put_arc(
        &self,
        shard_id: ShardId,
        key: BlobKey,
        payload: Arc<[u8]>,
    ) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Put {
            shard_id,
            key,
            payload,
        }])?;
        result.first_lsn().ok_or(Error::WriteResponseDropped)
    }

    /// Records or updates a blob's logical lifetime without rewriting its payload.
    ///
    /// Lifetime changes are metadata only LSNs so accounting can update
    /// expiration overlay state without touching segment bytes. Rewriting the blob just to change its
    /// lifetime would create a second payload record and could make GC think the old record was
    /// still live until accounting catches up.
    pub fn set_blob_lifetime(&self, key: &BlobKey, logical_end_epoch: Epoch) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::SetBlobLifetime {
            key: key.clone(),
            logical_end_epoch,
        }])?;
        result.first_lsn().ok_or(Error::WriteResponseDropped)
    }

    /// Appends a logical delete for a blob.
    ///
    /// A tombstone is an ordered LSN, not an in place removal. If we deleted
    /// the version row immediately, recovery after a crash could resurrect an older payload because
    /// there would be no durable delete marker to hide it.
    pub fn tombstone(&self, key: &BlobKey) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Tombstone { key: key.clone() }])?;
        Ok(result.first_lsn().unwrap_or(0))
    }

    /// Sends a prepared list of operations to the single writer and waits for the committed result.
    ///
    /// LSNs, segment offsets, and epoch rows must be allocated together by
    /// the owner of the active writer. If callers wrote directly to the index from many threads,
    /// two puts could both publish `next_lsn = 42` while their bytes landed at different offsets.
    fn write_batch(&self, ops: Vec<BatchOp>) -> Result<BatchWriteResult> {
        let (response_tx, response_rx) = mpsc::channel();
        let (profile, profile_rx) = self.profile_channel();
        let command = WriteCommand::Batch(BatchWriteRequest {
            ops,
            response_tx,
            profile,
        });
        let queue_send = self.send_write_command(command)?;
        let result = response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        self.finish_write_profile(profile_rx, queue_send)?;
        Ok(result)
    }

    /// Drops the cached file descriptor for a segment.
    ///
    /// Segment cleanup must call this before unlinking or reusing a segment path. The read path
    /// checks indexed segment state before serving refs, so an old cached descriptor cannot bypass
    /// a published `Deleted` state.
    pub fn evict_segment_reader(&self, segment_id: SegmentId) {
        self.reader_cache.evict(segment_id);
        self.metrics.record_reader_cache_eviction();
    }

    /// Returns the persisted current epoch.
    pub fn current_epoch(&self) -> Result<Epoch> {
        self.index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)
    }

    /// Resolves the epoch that was active at a specific LSN.
    ///
    /// Failure mode avoided: accounting needs to classify an old write under the epoch that was
    /// true when it happened. Using today's epoch for LSN 25 after several increments would expire
    /// or pin bytes in the wrong bucket.
    pub fn epoch_at_lsn(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        Ok(self.index.latest_epoch_at_lsn(lsn)?.map(|(_, epoch)| epoch))
    }

    /// Appends an epoch change operation and returns the new epoch with its LSN.
    ///
    /// Epoch increments consume LSNs so they are ordered with blob writes.
    /// Without that, a crash replay could see "blob A was written before epoch 9" while accounting
    /// had previously counted it as written after epoch 9.
    pub fn increment_epoch(&self) -> Result<(Epoch, StrataLsn)> {
        let result = self.write_batch(vec![BatchOp::IncrementEpoch])?;
        let Some(lsn) = result.first_lsn() else {
            return Err(Error::WriteResponseDropped);
        };
        let Some(epoch) = result.epoch_for_op(0) else {
            return Err(Error::WriteResponseDropped);
        };
        Ok((epoch, lsn))
    }

    /// Makes everything written so far crash-safe. Writes are visible immediately but only
    /// durable after a sync — fsyncing per put would destroy throughput on spinning disks, so
    /// durability is batched here. See `WriteCoordinator::sync_data` for the ordering invariant.
    pub fn sync(&self) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        let (profile, profile_rx) = self.profile_channel();
        let command = WriteCommand::Sync(SyncRequest {
            response_tx,
            profile,
        });
        let queue_send = self.send_write_command(command)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        self.finish_sync_profile(profile_rx, queue_send)?;
        Ok(())
    }

    /// Every operation with `lsn <= durable_lsn` survives a crash. This is the value callers
    /// (e.g. the Walrus event cursor) gate on before acknowledging work as done.
    pub fn durable_lsn(&self) -> Result<StrataLsn> {
        Ok(self.index.get_durable_lsn()?)
    }

    /// How far the background accounting worker has folded operations into GC overlay state.
    /// Always `<= durable_lsn`; the gap is accounting lag, not a correctness problem.
    pub fn accounted_lsn(&self) -> Result<StrataLsn> {
        Ok(self.index.get_accounted_lsn()?)
    }

    /// Pins the current accounted frontier for a long running GC job.
    ///
    /// The returned guard does not hold a RocksDB snapshot. It only prevents ref-event cleanup from
    /// deleting events newer than the captured `accounted_lsn` while GC copies records. Dropping the
    /// guard releases that retention pin.
    pub fn create_accounting_snapshot(&self) -> Result<AccountingSnapshotGuard> {
        Ok(self.index.create_accounting_snapshot()?)
    }

    /// Returns accounting ref events published after the frontier captured by `snapshot`.
    pub fn accounting_changes_since(
        &self,
        snapshot: &AccountingSnapshotGuard,
    ) -> Result<Vec<AccountingRefEvent>> {
        Ok(self.index.accounting_changes_since(snapshot)?)
    }

    /// Enqueues work for the writer and records queue metrics around the send.
    ///
    /// Failure mode avoided: if the writer has exited, this converts the broken channel into a
    /// store error and immediately undoes the queued metric. Otherwise a caller could block on a
    /// response that will never be sent while dashboards show phantom queued work.
    fn send_write_command(&self, command: WriteCommand) -> Result<Duration> {
        self.store_halt.check()?;
        let started = Instant::now();
        self.metrics.enqueue_write_command();
        let result = self
            .write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)
            .and_then(|write_tx| write_tx.send(command).map_err(|_| Error::WriteQueueClosed));
        if result.is_err() {
            self.metrics.dequeue_write_command();
        }
        let elapsed = started.elapsed();
        self.metrics
            .record_write_queue_send(result.is_ok(), elapsed);
        self.gc_concurrency.observe_write_queue_send(elapsed);
        result.map(|_| elapsed)
    }

    fn profile_channel<P>(&self) -> (ProfileRequest<P>, Option<mpsc::Receiver<P>>) {
        if !self.metrics.internal_profile_enabled() {
            return (ProfileRequest::default(), None);
        }

        let (tx, rx) = mpsc::channel();
        (ProfileRequest::enabled(tx), Some(rx))
    }

    fn finish_write_profile(
        &self,
        profile_rx: Option<mpsc::Receiver<StoreWriteProfile>>,
        queue_send: Duration,
    ) -> Result<()> {
        let Some(profile_rx) = profile_rx else {
            return Ok(());
        };
        let mut profile = profile_rx.recv().map_err(|_| Error::WriteResponseDropped)?;
        profile.queue_send = queue_send;
        profile.queue_wait = profile.queue_wait.saturating_sub(queue_send);
        self.metrics.record_write_profile(profile);
        Ok(())
    }

    fn finish_sync_profile(
        &self,
        profile_rx: Option<mpsc::Receiver<StoreSyncProfile>>,
        queue_send: Duration,
    ) -> Result<()> {
        let Some(profile_rx) = profile_rx else {
            return Ok(());
        };
        let mut profile = profile_rx.recv().map_err(|_| Error::WriteResponseDropped)?;
        profile.queue_send = queue_send;
        profile.queue_wait = profile.queue_wait.saturating_sub(queue_send);
        self.metrics.record_sync_profile(profile);
        Ok(())
    }
}

// Shutdown order matters: GC goes first because it publishes through the writer queue. Then the
// writer drains and exits before the sealer/accounting workers it can still nudge. Joins are
// best-effort; a panicked worker shouldn't turn drop into a second panic.
impl Drop for StrataStore {
    fn drop(&mut self) {
        for gc_tx in self.gc_txs.drain(..) {
            let _ = gc_tx.send(GcCommand::Shutdown);
        }
        for gc_handle in self.gc_handles.drain(..) {
            let _ = gc_handle.join();
        }
        if let Some(write_tx) = self.write_tx.take() {
            let _ = write_tx.send(WriteCommand::Shutdown);
        }
        if let Some(writer_handle) = self.writer_handle.take() {
            let _ = writer_handle.join();
        }
        if let Some(seal_tx) = self.seal_tx.take() {
            let _ = seal_tx.send(SealCommand::Shutdown);
        }
        if let Some(seal_handle) = self.seal_handle.take() {
            let _ = seal_handle.join();
        }
        if let Some(accounting_tx) = self.accounting_tx.take() {
            accounting_tx.shutdown();
        }
        if let Some(accounting_handle) = self.accounting_handle.take() {
            let _ = accounting_handle.join();
        }
    }
}

#[derive(Debug)]
enum WriteCommand {
    AddShard(AddShardRequest),
    Batch(BatchWriteRequest),
    DropShard(DropShardRequest),
    GcPublish(GcPublishRequest),
    Sync(SyncRequest),
    Shutdown,
}

#[derive(Debug)]
struct AddShardRequest {
    shard_id: ShardId,
    response_tx: mpsc::Sender<Result<ShardKey>>,
}

#[derive(Debug)]
struct ProfileRequest<P> {
    enqueued_at: Option<Instant>,
    tx: Option<mpsc::Sender<P>>,
}

impl<P> Default for ProfileRequest<P> {
    fn default() -> Self {
        Self {
            enqueued_at: None,
            tx: None,
        }
    }
}

impl<P> ProfileRequest<P> {
    fn enabled(tx: mpsc::Sender<P>) -> Self {
        Self {
            enqueued_at: Some(Instant::now()),
            tx: Some(tx),
        }
    }

    fn queue_wait(&self, started: Instant) -> Option<Duration> {
        self.tx
            .as_ref()
            .map(|_| started.saturating_duration_since(self.enqueued_at.unwrap_or(started)))
    }

    fn send(self, profile: P) {
        if let Some(tx) = self.tx {
            let _ = tx.send(profile);
        }
    }
}

#[derive(Debug)]
struct BatchWriteRequest {
    ops: Vec<BatchOp>,
    response_tx: mpsc::Sender<Result<BatchWriteResult>>,
    profile: ProfileRequest<StoreWriteProfile>,
}

#[derive(Debug)]
struct DropShardRequest {
    shard_id: ShardId,
    response_tx: mpsc::Sender<Result<ShardKey>>,
}

#[derive(Debug)]
pub(crate) struct GcPublishRequest {
    /// Prepared copy bundle whose output files are already protected as pending segment rows.
    copy: GcPrepublishedCopy,
    /// One-shot response channel back to the caller that requested GC publication.
    response_tx: mpsc::Sender<Result<GcPublishResult>>,
}

#[derive(Debug)]
struct SyncRequest {
    response_tx: mpsc::Sender<Result<()>>,
    profile: ProfileRequest<StoreSyncProfile>,
}

#[derive(Debug)]
enum BatchOp {
    Put {
        shard_id: ShardId,
        key: BlobKey,
        payload: Arc<[u8]>,
    },
    SetBlobLifetime {
        key: BlobKey,
        logical_end_epoch: Epoch,
    },
    Tombstone {
        key: BlobKey,
    },
    IncrementEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchWriteResult {
    op_lsns: Vec<StrataLsn>,
    op_epochs: Vec<Option<Epoch>>,
}

impl BatchWriteResult {
    /// LSNs in the same order as the submitted operations.
    ///
    /// Callers should not infer "the next operation is previous + 1" after
    /// a failed or empty batch. The writer is the source of truth for what actually committed.
    pub fn op_lsns(&self) -> &[StrataLsn] {
        &self.op_lsns
    }

    /// Epoch outputs in operation order; non-epoch operations have `None`.
    ///
    /// Mixed batches need to know which op advanced the epoch. Returning a
    /// single final epoch would make `put, increment, put` ambiguous to callers recording fences.
    pub fn op_epochs(&self) -> &[Option<Epoch>] {
        &self.op_epochs
    }

    pub fn epoch_for_op(&self, index: usize) -> Option<Epoch> {
        self.op_epochs.get(index).copied().flatten()
    }

    pub fn first_lsn(&self) -> Option<StrataLsn> {
        self.op_lsns.first().copied()
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.op_lsns.last().copied()
    }

    pub fn last_epoch(&self) -> Option<Epoch> {
        self.op_epochs.iter().rev().find_map(|epoch| *epoch)
    }
}

/// Temporary write-path timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreWriteProfile {
    /// Time spent blocked in the client-side bounded queue send.
    pub queue_send: Duration,
    /// Time from client submission until the writer starts the command, excluding `queue_send`.
    pub queue_wait: Duration,
    pub prepare_batch: Duration,
    pub segment_capacity: Duration,
    pub segment_append: Duration,
    pub accounting_delta_append: Duration,
    pub index_batch_commit: Duration,
    pub rollover_post_commit: Duration,
    pub accounting_nudge: Duration,
    pub response_send: Duration,
    pub writer_total: Duration,
}

/// Temporary sync-path timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreSyncProfile {
    /// Time spent blocked in the client-side bounded queue send.
    pub queue_send: Duration,
    /// Time from client submission until the writer starts the command, excluding `queue_send`.
    pub queue_wait: Duration,
    pub segment_sync: Duration,
    pub accounting_delta_sync: Duration,
    /// Includes durable segment state assembly, durable LSN computation, and batch construction.
    pub durable_lsn_compute: Duration,
    /// RocksDB batch commit with synchronous WAL durability.
    pub index_batch_commit: Duration,
    pub state_update: Duration,
    pub accounting_nudge: Duration,
    pub response_send: Duration,
    pub writer_total: Duration,
}

#[cfg(feature = "internal-profiling")]
pub trait StoreProfileSink: Send + Sync + std::fmt::Debug {
    fn record_write(&self, profile: StoreWriteProfile);
    fn record_sync(&self, profile: StoreSyncProfile);
}

impl ProfileRequest<StoreWriteProfile> {
    fn begin(&self, started: Instant) -> Option<StoreWriteProfile> {
        self.queue_wait(started)
            .map(|queue_wait| StoreWriteProfile {
                queue_wait,
                ..StoreWriteProfile::default()
            })
    }
}

impl ProfileRequest<StoreSyncProfile> {
    fn begin(&self, started: Instant) -> Option<StoreSyncProfile> {
        self.queue_wait(started).map(|queue_wait| StoreSyncProfile {
            queue_wait,
            ..StoreSyncProfile::default()
        })
    }
}

fn profile_phase<P, T>(
    profile: Option<&mut P>,
    record_elapsed: impl FnOnce(&mut P, Duration),
    action: impl FnOnce() -> T,
) -> T {
    let Some(profile) = profile else {
        return action();
    };

    let started = Instant::now();
    let result = action();
    record_elapsed(profile, started.elapsed());
    result
}

#[derive(Debug)]
pub struct StrataBatch<'a> {
    store: &'a StrataStore,
    ops: Vec<BatchOp>,
}

impl<'a> StrataBatch<'a> {
    /// Adds a payload write to this batch.
    ///
    /// Batching submits all operations as one writer command. That keeps
    /// `put, tombstone` in one batch from being interleaved by another writer between the two
    /// operations.
    pub fn put(
        &mut self,
        shard_id: ShardId,
        key: BlobKey,
        payload: impl Into<Arc<[u8]>>,
    ) -> &mut Self {
        self.ops.push(BatchOp::Put {
            shard_id,
            key,
            payload: payload.into(),
        });
        self
    }

    /// Adds a metadata only lifetime update to this batch.
    ///
    /// When a lifetime change is batched with other ops, it shares the same
    /// contiguous LSN reservation. Otherwise a concurrent tombstone could slip between the caller's
    /// payload write and its lifetime update.
    pub fn set_blob_lifetime(&mut self, key: BlobKey, logical_end_epoch: Epoch) -> &mut Self {
        self.ops.push(BatchOp::SetBlobLifetime {
            key,
            logical_end_epoch,
        });
        self
    }

    /// Adds a tombstone to this batch.
    ///
    /// Tombstones remain ordered relative to any preceding puts in the same
    /// batch. Without this, deleting a key after writing a replacement could race with another put
    /// and hide the wrong version.
    pub fn tombstone(&mut self, key: BlobKey) -> &mut Self {
        self.ops.push(BatchOp::Tombstone { key });
        self
    }

    /// Adds an epoch increment to this batch.
    ///
    /// Epoch changes are treated like logical operations. A batch such as
    /// `put A, increment epoch, put B` must replay exactly that order after crash recovery so A and
    /// B do not end up in the same accounting epoch.
    pub fn increment_epoch(&mut self) -> &mut Self {
        self.ops.push(BatchOp::IncrementEpoch);
        self
    }

    /// Submits the accumulated operations to the writer.
    ///
    /// The batch is consumed on write, so callers cannot accidentally submit
    /// the same prepared operations twice and create duplicate records with new LSNs.
    pub fn write(self) -> Result<BatchWriteResult> {
        self.store.write_batch(self.ops)
    }
}

#[derive(Debug)]
struct PreparedBatch {
    next_lsn: StrataLsn,
    result: BatchWriteResult,
    ops: Vec<PreparedBatchOp>,
}

#[derive(Debug)]
enum PreparedBatchOp {
    Put {
        shard: ShardKey,
        key: BlobKey,
        payload: Arc<[u8]>,
        lsn: StrataLsn,
        current_epoch: Epoch,
        record_ref: Option<RecordRef>,
        record_bytes: u64,
    },
    Lifecycle {
        key: BlobKey,
        lsn: StrataLsn,
        lifecycle_op: BlobLifecycleMergeOp,
    },
    EpochChange {
        lsn: StrataLsn,
        epoch: Epoch,
    },
}

fn accounting_delta_for_prepared_op(op: &PreparedBatchOp) -> Option<AccountingDelta> {
    match op {
        PreparedBatchOp::Put {
            shard,
            key,
            lsn,
            current_epoch,
            record_ref,
            ..
        } => Some(AccountingDelta::Blob(BlobUpdate::Put {
            lsn: *lsn,
            key: key.clone(),
            shard: *shard,
            record_ref: record_ref.expect("put record ref must be filled before delta append"),
            current_epoch: *current_epoch,
            // Foreground put should not read current blob metadata. `None` means "preserve any
            // materialized lifecycle"; SetLifetime deltas carry actual lifecycle changes.
            lifecycle: None,
        })),
        PreparedBatchOp::Lifecycle {
            key,
            lsn: _,
            lifecycle_op:
                BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                    lsn,
                    action:
                        BlobLifecycleAction::SetLifetime {
                            logical_end_epoch,
                            current_epoch,
                        },
                }),
        } => Some(AccountingDelta::Blob(BlobUpdate::SetLifetime {
            lsn: *lsn,
            key: key.clone(),
            logical_end_epoch: *logical_end_epoch,
            current_epoch: *current_epoch,
        })),
        PreparedBatchOp::Lifecycle {
            key,
            lsn: _,
            lifecycle_op:
                BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                    lsn,
                    action: BlobLifecycleAction::Tombstone,
                }),
        } => Some(AccountingDelta::Blob(BlobUpdate::Tombstone {
            lsn: *lsn,
            key: key.clone(),
        })),
        PreparedBatchOp::Lifecycle {
            lifecycle_op: BlobLifecycleMergeOp::RollbackFrom { .. },
            ..
        } => None,
        PreparedBatchOp::EpochChange { lsn, epoch } => {
            Some(AccountingDelta::Epoch(AccountingEpochChange {
                lsn: *lsn,
                epoch: *epoch,
            }))
        }
    }
}

#[derive(Debug)]
struct PendingRollover {
    old_segment_state: SegmentState,
    new_segment_state: SegmentState,
    seal_task: SegmentSealTask,
}

impl PendingRollover {
    /// Adds the old-segment `Sealing` row and the new open segment row to a write batch.
    ///
    /// Rollover metadata must commit atomically with the writer metadata batch that first publishes
    /// later segment or LSN state.
    fn apply_batch(
        &self,
        index: &StrataIndex,
        batch: &mut typed_store::rocks::DBBatch,
    ) -> Result<()> {
        index.put_segment_state_batch(batch, &self.old_segment_state)?;
        index.put_segment_state_batch(batch, &self.new_segment_state)?;
        Ok(())
    }

    /// Queues sealing only after the index commit that made the rollover visible.
    ///
    /// Failure mode avoided: if the sealer hashed and published an old segment before the
    /// `Sealing` row committed, a crash could leave sealed bytes on disk while the index still
    /// believes the segment is open and appendable.
    fn run_post_commit(self, seal_tx: mpsc::Sender<SealCommand>, metrics: StrataStoreMetrics) {
        seal_action(seal_tx, self.seal_task, metrics).run();
    }
}

#[derive(Debug)]
enum PostCommitAction {
    MaybeNudgeAccounting {
        latest_lsn: StrataLsn,
        threshold: usize,
        materialize: bool,
        accounting_tx: AccountingRequestSender,
    },
    EnqueueSeal {
        seal_tx: mpsc::Sender<SealCommand>,
        task: SegmentSealTask,
        metrics: StrataStoreMetrics,
    },
}

impl PostCommitAction {
    /// Runs side effects that are safe only after the index batch has committed.
    ///
    /// These actions intentionally do not happen during batch assembly. For
    /// example, nudging accounting before the blob-version batch commits could make accounting
    /// observe an LSN in the delta log whose index entry is not visible yet.
    fn run(self) {
        match self {
            Self::MaybeNudgeAccounting {
                latest_lsn,
                threshold,
                materialize,
                accounting_tx,
            } => {
                if materialize {
                    accounting_tx.request_materialize(latest_lsn);
                } else if threshold == 0 || latest_lsn % threshold as u64 == 0 {
                    accounting_tx.request_ingest();
                }
            }
            Self::EnqueueSeal {
                seal_tx,
                task,
                metrics,
            } => {
                if seal_tx.send(SealCommand::Seal(task)).is_ok() {
                    metrics.record_seal_enqueued();
                }
            }
        }
    }
}

fn accounting_nudge_action(
    latest_lsn: StrataLsn,
    threshold: usize,
    materialize: bool,
    accounting_tx: AccountingRequestSender,
) -> PostCommitAction {
    PostCommitAction::MaybeNudgeAccounting {
        latest_lsn,
        threshold,
        materialize,
        accounting_tx,
    }
}

fn seal_action(
    seal_tx: mpsc::Sender<SealCommand>,
    task: SegmentSealTask,
    metrics: StrataStoreMetrics,
) -> PostCommitAction {
    PostCommitAction::EnqueueSeal {
        seal_tx,
        task,
        metrics,
    }
}

/// One store writer thread that owns segment append order and global LSN allocation.
///
/// One writer thread per store, on purpose:
/// - LSN allocation is store-global and serialized by this writer.
/// - Segment appends must be ordered, and one sequentially-appended file is exactly the
///   I/O pattern HDDs are good at.
/// - "Sync" is just another command in the same queue, so durability snapshots never race an
///   in-flight append for this store.
///
/// Explicit sync still waits for the active segment and active accounting-log fsyncs. Ordinary
/// writes only append buffered accounting deltas; closed accounting epochs are fsynced by seal
/// workers alongside their matching segment files.
#[derive(Debug)]
struct WriteCoordinator {
    config: StrataStoreConfig,
    index: StrataIndex,
    active_writer: SegmentWriter,
    active_accounting_delta_log: ActiveDeltaLog,
    durability_publish_lock: Arc<Mutex<()>>,
    active_segment_state: SegmentState,
    durable_offset: u64,
    last_checkpoint_at: Instant,
    last_checkpoint_next_lsn: StrataLsn,
    pending_rollovers: Vec<PendingRollover>,
    segment_ids: SegmentIdAllocator,
    seal_tx: mpsc::Sender<SealCommand>,
    accounting_tx: Option<AccountingRequestSender>,
    write_rx: mpsc::Receiver<WriteCommand>,
    ingest_owner: SegmentOwner,
    reader_cache: Arc<SegmentReaderCache>,
    gc_concurrency: Arc<GcConcurrencyController>,
    store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
}

impl WriteCoordinator {
    /// Main writer loop: every mutation and sync is serialized through this receiver.
    ///
    /// Failure mode avoided: sync must not race append. If sync ran on a separate thread, it could
    /// publish durable_lsn 10 while a concurrent append for LSN 10 had reserved an offset but not
    /// finished writing its record bytes.
    fn run(mut self) {
        loop {
            let timeout = self.next_checkpoint_timeout();
            let command = match self.write_rx.recv_timeout(timeout) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.process_timed_checkpoint() {
                        self.halt_writer_error("timed durability checkpoint", &error);
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            if matches!(command, WriteCommand::Shutdown) {
                break;
            }
            self.metrics.dequeue_write_command();
            if let Some(error) = self.store_halt.error() {
                Self::send_command_error(command, error);
                continue;
            }
            match command {
                WriteCommand::AddShard(request) => {
                    self.process_add_shard(request);
                }
                WriteCommand::Batch(request) => {
                    self.process_batch(request);
                }
                WriteCommand::DropShard(request) => {
                    self.process_drop_shard(request);
                }
                WriteCommand::GcPublish(request) => {
                    self.process_gc_publish(request);
                }
                WriteCommand::Sync(request) => {
                    self.process_sync(request);
                }
                WriteCommand::Shutdown => unreachable!("shutdown is handled before dispatch"),
            }
        }
    }

    fn next_checkpoint_timeout(&self) -> Duration {
        DURABILITY_CHECKPOINT_INTERVAL.saturating_sub(self.last_checkpoint_at.elapsed())
    }

    fn send_command_error(command: WriteCommand, error: Error) {
        match command {
            WriteCommand::AddShard(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::Batch(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::DropShard(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::GcPublish(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::Sync(request) => {
                let _ = request.response_tx.send(Err(error));
            }
            WriteCommand::Shutdown => {}
        }
    }

    fn process_add_shard(&mut self, request: AddShardRequest) {
        let result = self.submit_add_shard(request.shard_id);
        let _ = request.response_tx.send(result);
    }

    /// Creates or reactivates a shard generation through the writer queue.
    ///
    /// Drop/re-add must bump generation exactly once. Without this serialized
    /// registry update, one thread could resurrect generation 0 while another has already dropped
    /// it and started generation 1, making old writes visible in the new namespace.
    fn submit_add_shard(&mut self, shard_id: ShardId) -> Result<ShardKey> {
        let info = match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => return Ok(info.key(shard_id)),
            Some(info) if info.is_dropped() => {
                ShardInfo::active(info.current_generation.checked_add(1).ok_or(
                    Error::ShardGenerationOverflow {
                        shard_id,
                        current_generation: info.current_generation,
                    },
                )?)
            }
            Some(info) => {
                return Err(Error::ShardUnavailable {
                    shard_id,
                    generation: info.current_generation,
                    current_generation: info.current_generation,
                    state: info.state,
                });
            }
            None => ShardInfo::active(0),
        };

        let mut batch = self.index.batch();
        self.index
            .put_shard_info_batch(&mut batch, shard_id, info)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        self.index.set_cached_shard_info(shard_id, info);
        Ok(info.key(shard_id))
    }

    fn process_drop_shard(&mut self, request: DropShardRequest) {
        let result = self.submit_drop_shard(request.shard_id);
        let _ = request.response_tx.send(result);
    }

    /// Runs a GC publish request on the writer thread and reports the result to the caller.
    ///
    /// GC publish needs the writer thread because it assigns store-global LSNs, appends active
    /// accounting deltas, and commits segment metadata in the same ordering domain as foreground
    /// writes.
    fn process_gc_publish(&mut self, request: GcPublishRequest) {
        let result = self.submit_gc_publish(request.copy);
        let _ = request.response_tx.send(result);
    }

    /// Validates that a shard can be dropped and delegates the durable registry update.
    ///
    /// Treating "already dropped" as success makes retries idempotent after
    /// caller timeouts. Treating missing shards as success would hide bugs where a caller thinks it
    /// deleted tenant 42 but that tenant was never registered.
    fn submit_drop_shard(&mut self, shard_id: ShardId) -> Result<ShardKey> {
        let Some(info) = self.index.get_shard_info(shard_id)? else {
            return Err(Error::ShardNotFound { shard_id });
        };
        if info.state == ShardState::Dropped {
            return Ok(info.key(shard_id));
        }

        // The drop LSN orders the accounting sweep after every preceding payload transition.
        self.sync_data(None)?;
        let shard = info.key(shard_id);
        self.mark_shard_dropped(shard_id, shard)?;
        Ok(shard)
    }

    /// Persists the dropped shard state, accounting fence, and resumable cleanup job.
    fn mark_shard_dropped(&mut self, shard_id: ShardId, shard: ShardKey) -> Result<()> {
        let dropped_info = ShardInfo {
            current_generation: shard.generation,
            state: ShardState::Dropped,
        };
        let drop_lsn = self.index.get_next_lsn()?;
        let next_lsn = drop_lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;

        let commit_result = (|| {
            let active_delta_state = self.append_and_sync_accounting_delta(
                AccountingDelta::ShardDropped {
                    lsn: drop_lsn,
                    shard,
                },
                drop_lsn,
            )?;

            let _publish_guard = self
                .durability_publish_lock
                .lock()
                .expect("durability publish lock poisoned");
            let mut batch = self.index.batch();
            self.index
                .put_shard_info_batch(&mut batch, shard_id, dropped_info)?;
            self.index.put_shard_cleanup_job_batch(
                &mut batch,
                ShardCleanupJob {
                    shard,
                    drop_lsn,
                    state: ShardCleanupState::PendingAccounting,
                },
            )?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            self.index.put_durable_lsn_batch(&mut batch, drop_lsn)?;
            self.index
                .put_accounting_active_delta_log_state_batch(&mut batch, active_delta_state)?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
            Ok::<(), Error>(())
        })();

        if let Err(error) = commit_result {
            return Err(error);
        }

        self.index.set_cached_shard_info(shard_id, dropped_info);
        self.index.set_blob_compact_safe_lsn(drop_lsn);
        self.metrics.set_next_lsn(next_lsn);
        self.metrics.set_durable_lsn(drop_lsn);
        if let Some(accounting_tx) = &self.accounting_tx {
            accounting_tx.request_materialize(drop_lsn);
        }
        Ok(())
    }

    /// Handles one client batch and records user visible put metrics.
    ///
    /// Metrics are recorded once per submitted put after the writer knows
    /// whether the batch committed or was rejected during validation. Recording during append would
    /// count a write as successful before the index commit that makes it visible.
    fn process_batch(&mut self, request: BatchWriteRequest) {
        let BatchWriteRequest {
            ops,
            response_tx,
            profile: profile_request,
        } = request;
        let started = Instant::now();
        let put_count = ops
            .iter()
            .filter(|op| matches!(op, BatchOp::Put { .. }))
            .count();
        let mut profile = profile_request.begin(started);
        match self.submit_batch(ops, response_tx, profile.as_mut()) {
            Ok((result, put_metrics)) => {
                for metric in put_metrics {
                    self.metrics.record_put(Ok(metric), started.elapsed());
                }
                if let Some(last_lsn) = result.last_lsn() {
                    self.metrics.set_next_lsn(last_lsn.saturating_add(1));
                }
                if let Some(epoch) = result.last_epoch() {
                    self.metrics.set_current_epoch(epoch);
                }
            }
            Err(()) => {
                for _ in 0..put_count {
                    self.metrics.record_put(Err(()), started.elapsed());
                }
            }
        }
        if let Some(mut profile) = profile {
            profile.writer_total = started.elapsed();
            profile_request.send(profile);
        }
    }

    fn process_sync(&mut self, request: SyncRequest) {
        let SyncRequest {
            response_tx,
            profile: profile_request,
        } = request;
        let started = Instant::now();
        let mut profile = profile_request.begin(started);
        let result = self.sync_data(profile.as_mut());
        if let Some(profile) = profile.as_mut() {
            profile.writer_total = started.elapsed();
        }
        let _ = profile_phase(
            profile.as_mut(),
            |profile, elapsed| profile.response_send = elapsed,
            || response_tx.send(result),
        );
        if let Some(profile) = profile {
            profile_request.send(profile);
        }
    }

    /// Full write transaction for a batch: validate, reserve LSNs, append payload bytes, append
    /// accounting deltas, then commit one index batch.
    ///
    /// User visible validation errors return normally before physical writer state changes. Once
    /// the physical write path starts, file/accounting-log/index failures are rolled back with best effort
    /// but on failure we halt the store and crash recovery remains the single repair path for partially
    /// published bytes or rollovers
    fn submit_batch(
        &mut self,
        ops: Vec<BatchOp>,
        response_tx: mpsc::Sender<Result<BatchWriteResult>>,
        mut profile: Option<&mut StoreWriteProfile>,
    ) -> std::result::Result<(BatchWriteResult, Vec<PutMetric>), ()> {
        if ops.is_empty() {
            let result = BatchWriteResult::default();
            let _ = profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || response_tx.send(Ok(result.clone())),
            );
            return Ok((result, Vec::new()));
        }

        let mut prepared = match profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.prepare_batch += elapsed,
            || self.prepare_batch(ops),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.response_send += elapsed,
                    || response_tx.send(Err(error)),
                );
                return Err(());
            }
        };

        let mut appended_records = 0_u64;
        let mut appended_bytes = 0_u64;
        let mut put_metrics = Vec::new();
        for op in &mut prepared.ops {
            if let PreparedBatchOp::Put {
                shard,
                key,
                payload,
                lsn,
                record_ref,
                record_bytes,
                ..
            } = op
            {
                let capacity_result = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.segment_capacity += elapsed,
                    || self.ensure_segment_capacity(*record_bytes, *lsn),
                );
                if let Err(error) = capacity_result {
                    match error {
                        error @ Error::Segment(
                            strata_segment::Error::SegmentFull { .. }
                            | strata_segment::Error::RangeOverflow,
                        ) => {
                            let _ = profile_phase(
                                profile.as_deref_mut(),
                                |profile, elapsed| profile.response_send += elapsed,
                                || response_tx.send(Err(error)),
                            );
                            return Err(());
                        }
                        error => {
                            self.halt_submit_batch_failure(
                                "segment rollover",
                                &error,
                                appended_records,
                                appended_bytes,
                            );
                            let _ = profile_phase(
                                profile.as_deref_mut(),
                                |profile, elapsed| profile.response_send += elapsed,
                                || response_tx.send(Err(error)),
                            );
                            return Err(());
                        }
                    }
                }

                let append_result = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.segment_append += elapsed,
                    || {
                        self.active_writer
                            .append_for_shard(&*key, *lsn, *shard, payload.as_ref())
                    },
                );
                let outcome = match append_result {
                    Ok(outcome) => outcome,
                    Err(
                        error @ (strata_segment::Error::SegmentFull { .. }
                        | strata_segment::Error::RangeOverflow),
                    ) => {
                        let invariant = "append_for_shard returned a capacity error after ensure_segment_capacity";
                        let error = self.halt_submit_batch_invariant(
                            invariant,
                            &error,
                            appended_records,
                            appended_bytes,
                        );
                        let _ = profile_phase(
                            profile.as_deref_mut(),
                            |profile, elapsed| profile.response_send += elapsed,
                            || response_tx.send(Err(error)),
                        );
                        return Err(());
                    }
                    Err(error) => {
                        let terminal =
                            matches!(error, strata_segment::Error::AppendRollbackFailed { .. });
                        let error = Error::from(error);
                        self.metrics
                            .record_orphaned_segment_bytes(appended_records, appended_bytes);
                        if terminal {
                            self.halt_writer_error("segment append", &error);
                        }
                        let _ = profile_phase(
                            profile.as_deref_mut(),
                            |profile, elapsed| profile.response_send += elapsed,
                            || response_tx.send(Err(error)),
                        );
                        return Err(());
                    }
                };

                *record_ref = Some(outcome.record_ref);
                *record_bytes = outcome.record_len;
                appended_records = appended_records.saturating_add(1);
                appended_bytes = appended_bytes.saturating_add(outcome.record_len);
                put_metrics.push(PutMetric {
                    payload_bytes: payload.len() as u64,
                    record_bytes: outcome.record_len,
                });
                self.active_segment_state.write_offset = self.active_writer.write_offset();
                self.active_segment_state.min_lsn = Some(
                    self.active_segment_state
                        .min_lsn
                        .map_or(*lsn, |first| first.min(*lsn)),
                );
                self.active_segment_state.max_lsn = Some(
                    self.active_segment_state
                        .max_lsn
                        .map_or(*lsn, |last| last.max(*lsn)),
                );
            }

            let accounting_result = profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.accounting_delta_append += elapsed,
                || self.append_accounting_delta_for_op(op),
            );
            if let Err(error) = accounting_result {
                self.halt_submit_batch_failure(
                    "accounting delta append",
                    &error,
                    appended_records,
                    appended_bytes,
                );
                let _ = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.response_send += elapsed,
                    || response_tx.send(Err(error)),
                );
                return Err(());
            }
        }

        let pending_rollovers = self.take_pending_rollovers();
        let commit_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.index_batch_commit += elapsed,
            || self.commit_write_batch(&pending_rollovers, &prepared),
        );
        if let Err(error) = commit_result {
            self.halt_submit_batch_failure(
                "index batch commit",
                &error,
                appended_records,
                appended_bytes,
            );
            let _ = profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.response_send += elapsed,
                || response_tx.send(Err(error)),
            );
            return Err(());
        }
        profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.rollover_post_commit += elapsed,
            || self.run_rollover_post_commit(pending_rollovers),
        );
        let materialize_accounting = prepared.result.last_epoch().is_some();
        if let (Some(last_lsn), Some(accounting_tx)) =
            (prepared.result.last_lsn(), self.accounting_tx.clone())
        {
            profile_phase(
                profile.as_deref_mut(),
                |profile, elapsed| profile.accounting_nudge += elapsed,
                || {
                    accounting_nudge_action(
                        last_lsn,
                        self.config.accounting_unaccounted_threshold,
                        materialize_accounting,
                        accounting_tx,
                    )
                    .run()
                },
            );
        }
        self.metrics.set_active_segment(
            self.active_writer.segment_id(),
            self.active_writer.write_offset(),
            self.durable_offset,
        );
        let result = prepared.result;
        let _ = profile_phase(
            profile,
            |profile, elapsed| profile.response_send += elapsed,
            || response_tx.send(Ok(result.clone())),
        );
        Ok((result, put_metrics))
    }

    fn halt_submit_batch_failure(
        &self,
        context: &str,
        error: &Error,
        appended_records: u64,
        appended_bytes: u64,
    ) {
        self.metrics
            .record_orphaned_segment_bytes(appended_records, appended_bytes);
        self.halt_writer_error(context, error);
    }

    fn halt_submit_batch_invariant(
        &self,
        invariant: &str,
        error: &strata_segment::Error,
        appended_records: u64,
        appended_bytes: u64,
    ) -> Error {
        self.metrics
            .record_orphaned_segment_bytes(appended_records, appended_bytes);
        let reason = format!("fatal strata writer invariant violation: {invariant}: {error}");
        self.store_halt.halt(reason.clone());
        Error::StoreHalted { reason }
    }

    fn halt_writer_error(&self, context: &str, error: &Error) {
        self.store_halt.halt(format!(
            "fatal strata writer error during {context}: {error}"
        ));
    }

    /// Publishes preprotected GC output segments as `MapRef` operations.
    ///
    /// The caller already holds the accounting run lock before this command reaches the writer.
    /// Keeping the pause outside the writer loop means a long accounting pass can delay the GC
    /// caller without stalling unrelated user writes. This writer-side critical section only does
    /// the ordering-sensitive work: assign the GC LSN range, append the bulk accounting delta, and
    /// commit rollbackable index metadata.
    ///
    /// TODO: publish large GC copies in bounded chunks. File rename/fsync and forced durability are
    /// already outside this path, so chunking is mainly about write-queue fairness: a very large
    /// copy can still build and commit one large RocksDB batch of MapRefs, unaccounted-LSN rows,
    /// and relocation rows while foreground writes wait behind it.
    fn submit_gc_publish(&mut self, copy: GcPrepublishedCopy) -> Result<GcPublishResult> {
        let reconciled_accounted_lsn = self.index.get_accounted_lsn()?;
        match &copy.plan.action {
            GcAction::DeleteSegment { .. }
            | GcAction::DeleteSegments { .. }
            | GcAction::ReclassifySegment { .. } => {
                if !copy.outputs.is_empty() || !copy.copied_records.is_empty() {
                    return Err(Error::GcInvalidPlan(
                        "metadata action cannot include staged outputs or copied records",
                    ));
                }
                self.apply_gc_metadata_action(&copy.plan.action)?;
                return Ok(GcPublishResult {
                    reconciled_accounted_lsn,
                    output_segments: Vec::new(),
                    published_records: Vec::new(),
                    skipped_records: Vec::new(),
                });
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
        }

        let accounting_changes = self
            .index
            .accounting_changes_since(&copy.accounting_snapshot)?;
        let mut obsolete_shards = BTreeSet::new();
        for record in &copy.copied_records {
            if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                obsolete_shards.insert(record.source.shard);
            }
        }
        let (survivors, skipped_records) =
            split_gc_copied_records(copy.copied_records, &accounting_changes, &obsolete_shards);

        if survivors.is_empty() {
            let mut batch = self.index.batch();
            for output in &copy.outputs {
                self.index
                    .put_segment_state_batch(&mut batch, &output.deleted_state(&self.config))?;
            }
            batch.write().map_err(strata_index::Error::from)?;
            return Ok(GcPublishResult {
                reconciled_accounted_lsn,
                output_segments: Vec::new(),
                published_records: Vec::new(),
                skipped_records: skipped_records
                    .into_iter()
                    .map(|skipped| skipped.record)
                    .collect(),
            });
        }

        let mut output_plan = self.plan_gc_output_segments(&copy.outputs, &survivors)?;
        let published_records = assign_gc_publish_lsns(
            self.index.get_next_lsn()?,
            &survivors,
            &output_plan.staged_to_final_segment_id,
        )?;
        apply_gc_output_lsn_bounds(&mut output_plan.segment_states, &published_records);
        let skipped_output_ranges =
            skipped_gc_output_ranges(&skipped_records, &output_plan.staged_to_final_segment_id);

        let pending_rollovers = self.take_pending_rollovers();
        let accounting_delta = gc_publish_accounting_delta(&published_records);
        let commit_result = (|| {
            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            for state in &output_plan.segment_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
            }
            for output in &copy.outputs {
                if !output_plan
                    .used_staged_ids
                    .contains(&output.staged_segment_id)
                {
                    self.index
                        .put_segment_state_batch(&mut batch, &output.deleted_state(&self.config))?;
                }
            }
            for record in &published_records {
                self.index.put_gc_relocation_batch(
                    &mut batch,
                    record.source.from,
                    &GcRelocation {
                        publish_lsn: record.publish_lsn,
                        to: record.to,
                    },
                )?;
                self.index.map_blob_ref_batch(
                    &mut batch,
                    &record.source.key,
                    MapRefOp {
                        publish_lsn: record.publish_lsn,
                        shard: record.source.shard,
                        payload_lsn: record.source.payload_lsn,
                        from: record.source.from,
                        to: record.to,
                    },
                )?;
                self.index.put_blob_unaccounted_lsn_op_batch(
                    &mut batch,
                    record.publish_lsn,
                    &record.source.key,
                )?;
            }
            for ((segment_id, kind), ranges) in skipped_output_ranges {
                let op = match kind {
                    GcSkippedCopiedRecordKind::Retired => {
                        SegmentGcOverlayMergeOp::AddRetiredBatch { ranges }
                    }
                    GcSkippedCopiedRecordKind::Expired => {
                        SegmentGcOverlayMergeOp::AddExpiredBatch { ranges }
                    }
                };
                self.index
                    .merge_segment_gc_overlay_batch(&mut batch, segment_id, vec![op])?;
            }

            let next_lsn = published_records
                .last()
                .and_then(|record| record.publish_lsn.checked_add(1))
                .ok_or(strata_segment::Error::RangeOverflow)?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            if let Some(delta) = accounting_delta.as_ref() {
                self.append_accounting_delta(delta)?;
            }
            if let Err(error) = batch
                .write()
                .map_err(strata_index::Error::from)
                .map_err(Error::from)
            {
                return Err(GcPublishCommitError::IndexCommit(error));
            }
            Ok::<(), GcPublishCommitError>(())
        })();

        match commit_result {
            Ok(()) => {
                self.run_rollover_post_commit(pending_rollovers);
                let next_lsn = published_records
                    .last()
                    .and_then(|record| record.publish_lsn.checked_add(1))
                    .expect("published records are non-empty and checked above");
                self.metrics.set_next_lsn(next_lsn);
                if let Some(accounting_tx) = &self.accounting_tx {
                    accounting_tx.request_ingest();
                }
                Ok(GcPublishResult {
                    reconciled_accounted_lsn,
                    output_segments: output_plan.published_outputs,
                    published_records,
                    skipped_records: skipped_records
                        .into_iter()
                        .map(|skipped| skipped.record)
                        .collect(),
                })
            }
            Err(GcPublishCommitError::BeforeIndexBatch(error)) => {
                self.restore_pending_rollovers(pending_rollovers);
                Err(error)
            }
            Err(GcPublishCommitError::IndexCommit(error)) => {
                self.halt_writer_error("gc publish index batch commit", &error);
                Err(error)
            }
        }
    }

    fn apply_gc_metadata_action(&self, action: &GcAction) -> Result<()> {
        match action {
            GcAction::DeleteSegment { segment_id } => {
                self.delete_empty_gc_segments(&[*segment_id])?;
            }
            GcAction::DeleteSegments { segment_ids } => {
                self.delete_empty_gc_segments(segment_ids)?;
            }
            GcAction::ReclassifySegment {
                segment_id,
                placement_class,
            } => {
                self.reclassify_gc_segment(*segment_id, *placement_class)?;
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {
                return Err(Error::GcInvalidPlan(
                    "copy action must use the copy publish path",
                ));
            }
        }
        Ok(())
    }

    fn delete_empty_gc_segments(&self, segment_ids: &[SegmentId]) -> Result<()> {
        let mut states = Vec::with_capacity(segment_ids.len());
        let mut states_to_commit = Vec::new();
        for segment_id in segment_ids {
            let mut state = self.index.get_segment_state(*segment_id)?.ok_or(
                Error::GcMissingSourceSegment {
                    segment_id: *segment_id,
                },
            )?;
            if state.state == SegmentFileState::Deleted {
                states.push(state);
                continue;
            }
            if state.state != SegmentFileState::Sealed {
                return Err(Error::GcSourceSegmentNotSealed {
                    segment_id: *segment_id,
                    state: state.state,
                });
            }

            let summary = self
                .index
                .get_segment_gc_overlay(*segment_id)?
                .unwrap_or_default()
                .summary;
            if summary.live_ref_count != 0 {
                return Err(Error::GcSourceSegmentNotEmpty {
                    segment_id: *segment_id,
                    live_ref_count: summary.live_ref_count,
                });
            }

            state.state = SegmentFileState::Deleted;
            states_to_commit.push(state.clone());
            states.push(state);
        }

        if !states_to_commit.is_empty() {
            let mut batch = self.index.batch();
            for state in &states_to_commit {
                self.index.put_segment_state_batch(&mut batch, state)?;
            }
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
        }

        for state in &states {
            self.reader_cache.evict(state.segment_id);
            self.metrics.record_reader_cache_eviction();
        }
        unlink_gc_segment_files(&self.config, &states)
    }

    fn reclassify_gc_segment(
        &self,
        segment_id: SegmentId,
        placement_class: PlacementClass,
    ) -> Result<()> {
        let mut state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        if state.state != SegmentFileState::Sealed {
            return Err(Error::GcSourceSegmentNotSealed {
                segment_id,
                state: state.state,
            });
        }
        if state.placement_class == placement_class {
            return Ok(());
        }

        state.placement_class = placement_class;
        let mut batch = self.index.batch();
        self.index.put_segment_state_batch(&mut batch, &state)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        Ok(())
    }

    /// Selects the prepublished output segments that still contain surviving copied records.
    ///
    /// Only outputs that contain surviving copied records are finalized as `Sealed`. The returned
    /// map is the translation table from temporary staged segment ids to final durable segment ids,
    /// which later helpers use to build `MapRef` destinations.
    fn plan_gc_output_segments(
        &self,
        outputs: &[GcPrepublishedOutputSegment],
        survivors: &[GcStagedCopiedRecord],
    ) -> Result<PlannedGcOutputSegments> {
        let used_staged_ids = survivors
            .iter()
            .map(|record| record.staged.segment_id)
            .collect::<BTreeSet<_>>();
        let mut outputs_by_staged_id = outputs
            .iter()
            .map(|output| (output.staged_segment_id, output))
            .collect::<BTreeMap<_, _>>();
        let mut staged_to_final_segment_id = BTreeMap::new();
        let mut published_outputs = Vec::new();
        let mut segment_states = Vec::new();

        for staged_segment_id in &used_staged_ids {
            let output = match outputs_by_staged_id.remove(staged_segment_id) {
                Some(output) => output,
                None => {
                    return Err(Error::GcMissingStagedOutput {
                        staged_segment_id: *staged_segment_id,
                    });
                }
            };
            let final_segment_id = output.segment_id;
            let published_output = output.published_output();
            published_outputs.push(published_output);
            staged_to_final_segment_id.insert(output.staged_segment_id, final_segment_id);
            segment_states.push(output.sealed_state(&self.config));
        }

        Ok(PlannedGcOutputSegments {
            staged_to_final_segment_id,
            used_staged_ids,
            published_outputs,
            segment_states,
        })
    }

    /// Resolves a client batch into concrete LSNs and per op metadata before any bytes are written.
    ///
    /// Every operation in a batch reserves a contiguous LSN range. If LSNs
    /// were assigned lazily during append, a too-large payload error halfway through could leave
    /// later metadata ops committed at unexpected LSNs.
    fn prepare_batch(&self, ops: Vec<BatchOp>) -> Result<PreparedBatch> {
        let first_lsn = self.index.get_next_lsn()?;
        let mut prepared_ops = Vec::with_capacity(ops.len());
        let mut op_lsns = Vec::with_capacity(ops.len());
        let mut op_epochs = Vec::with_capacity(ops.len());
        let mut current_epoch = self.index.get_current_epoch()?;

        for (index, op) in ops.into_iter().enumerate() {
            let lsn = first_lsn
                .checked_add(index as u64)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            match op {
                BatchOp::Put {
                    shard_id,
                    key,
                    payload,
                } => {
                    // Why do we care about the current epoch here?
                    // The reason to remember the epoch at which the put was submitted is to
                    // ensure that this put's visibility can be judged by the accounting later on.
                    // Imagine if this was the sequence:
                    // current_epoch = 10
                    // LSN 100: SetLifetime { logical_end_epoch: 50 }
                    // LSN 101: Put { key: "foo", payload: "bar" }
                    // LSN 102: Put { key: "foo", payload: "baz" }
                    // LSN 103: ChangeEpoch { epoch: 50 }
                    // LSN 104: Put { key: "baz", payload: "qux"}
                    // The first Put has no explicit lifecycle, but the key already has an explicit
                    // lifecycle ending at epoch 50. Since 50 > current_epoch(10), accounting lets
                    // the new physical record inherit that lifecycle. GC then knows the bytes
                    // for "bar" belong in the "expires at 50" segment.
                    // Later when the second Put comes along at epoch < 50, accounting lets it
                    // inherit the lifecycle of the key, and the bytes for "baz" belong in the
                    // "expires at 50" segment. The bytes for "bar" at this point are eligible for
                    // garbage collection since the key is overwritten.
                    // Subsequently epoch advances to 50 and the final Put at LSN 104 happens and
                    // if do not record the current epoch at which this put was submitted, then
                    // the accounting would not know that the bytes for "qux" should not inherit
                    // an expired lifetime. It would think that the bytes for "qux" should belong
                    // in the "expires at 50" segment (Important thing to know is that compaction
                    // does not know about the epoch change as it is not a key based operation)
                    let current_epoch = current_epoch.ok_or(Error::EpochNotInitialized)?;
                    let shard = self.openable_shard_key(shard_id)?;
                    let record_bytes = encoded_record_len(&key, payload.len())
                        .map_err(strata_segment::Error::from)?;
                    if record_bytes > self.config.segment_max_bytes {
                        return Err(strata_segment::Error::SegmentFull {
                            max_size: self.config.segment_max_bytes,
                            attempted_size: record_bytes,
                        }
                        .into());
                    }
                    prepared_ops.push(PreparedBatchOp::Put {
                        shard,
                        key,
                        payload,
                        lsn,
                        current_epoch,
                        record_ref: None,
                        record_bytes,
                    });
                    op_epochs.push(None);
                }
                BatchOp::SetBlobLifetime {
                    key,
                    logical_end_epoch,
                } => {
                    let epoch = current_epoch.ok_or(Error::EpochNotInitialized)?;
                    if logical_end_epoch <= epoch {
                        return Err(Error::InvalidBlobLifetime {
                            logical_end_epoch,
                            current_epoch: epoch,
                        });
                    }
                    prepared_ops.push(PreparedBatchOp::Lifecycle {
                        key,
                        lsn,
                        lifecycle_op: BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                            lsn,
                            action: BlobLifecycleAction::SetLifetime {
                                logical_end_epoch,
                                current_epoch: epoch,
                            },
                        }),
                    });
                    op_epochs.push(None);
                }
                BatchOp::Tombstone { key } => {
                    prepared_ops.push(PreparedBatchOp::Lifecycle {
                        key,
                        lsn,
                        lifecycle_op: BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                            lsn,
                            action: BlobLifecycleAction::Tombstone,
                        }),
                    });
                    op_epochs.push(None);
                }
                BatchOp::IncrementEpoch => {
                    let next_epoch = current_epoch
                        .ok_or(Error::EpochNotInitialized)?
                        .checked_add(1)
                        .ok_or(strata_segment::Error::RangeOverflow)?;
                    current_epoch = Some(next_epoch);
                    prepared_ops.push(PreparedBatchOp::EpochChange {
                        lsn,
                        epoch: next_epoch,
                    });
                    op_epochs.push(Some(next_epoch));
                }
            }
            op_lsns.push(lsn);
        }

        Ok(PreparedBatch {
            next_lsn: first_lsn
                .checked_add(op_lsns.len() as u64)
                .ok_or(strata_segment::Error::RangeOverflow)?,
            result: BatchWriteResult { op_lsns, op_epochs },
            ops: prepared_ops,
        })
    }

    /// Temporarily removes staged rollover metadata so it can be included in the current durable
    /// index batch exactly once.
    fn take_pending_rollovers(&mut self) -> Vec<PendingRollover> {
        std::mem::take(&mut self.pending_rollovers)
    }

    /// Restores rollover metadata when a non-foreground metadata publish fails before committing.
    ///
    /// Foreground `submit_batch` failures after physical writer work starts are fatal instead.
    fn restore_pending_rollovers(&mut self, pending_rollovers: Vec<PendingRollover>) {
        self.pending_rollovers = pending_rollovers;
    }

    /// Commits the index side of a prepared batch.
    ///
    /// Blob versions, unaccounted LSN rows, epoch changes, active segment
    /// offsets, rollover rows, and `next_lsn` must move together. If `next_lsn` advanced without
    /// the blob row, recovery would skip that LSN forever and create a hole in the history.
    fn commit_write_batch(
        &self,
        pending_rollovers: &[PendingRollover],
        prepared: &PreparedBatch,
    ) -> Result<()> {
        let mut batch = self.index.batch();
        for rollover in pending_rollovers {
            rollover.apply_batch(&self.index, &mut batch)?;
        }

        let mut wrote_payload = false;
        for op in &prepared.ops {
            match op {
                PreparedBatchOp::Put {
                    shard,
                    key,
                    lsn,
                    record_ref,
                    record_bytes,
                    ..
                } => {
                    let record_ref =
                        record_ref.expect("put record ref must be filled before commit");
                    let entry = PutEntry {
                        record_ref: Some(record_ref),
                        lsn: *lsn,
                        generation: *lsn,
                        state: BlobState::Live,
                    };
                    self.index.apply_blob_version_merge_op_batch(
                        &mut batch,
                        key,
                        PutMergeOp::Append(PutOp {
                            shard: *shard,
                            entry,
                        }),
                    )?;
                    self.index
                        .put_blob_unaccounted_lsn_op_batch(&mut batch, *lsn, key)?;
                    let _ = record_bytes;
                    wrote_payload = true;
                }
                PreparedBatchOp::Lifecycle {
                    key,
                    lsn,
                    lifecycle_op,
                } => {
                    self.index.apply_blob_lifecycle_merge_op_batch(
                        &mut batch,
                        key,
                        lifecycle_op.clone(),
                    )?;
                    self.index
                        .put_blob_unaccounted_lsn_op_batch(&mut batch, *lsn, key)?;
                }
                PreparedBatchOp::EpochChange { lsn, epoch } => {
                    self.index
                        .put_epoch_change_batch(&mut batch, *lsn, *epoch)?;
                    self.index.put_current_epoch_batch(&mut batch, *epoch)?;
                }
            }
        }
        if wrote_payload {
            self.index
                .put_segment_state_batch(&mut batch, &self.active_segment_state)?;
        }
        self.index
            .put_next_lsn_batch(&mut batch, prepared.next_lsn)?;
        batch.write().map_err(strata_index::Error::from)?;
        Ok(())
    }

    /// Appends the ordered accounting delta for one prepared op to the currently active accounting
    /// epoch.
    fn append_accounting_delta_for_op(&mut self, op: &PreparedBatchOp) -> Result<()> {
        let Some(delta) = accounting_delta_for_prepared_op(op) else {
            return Ok(());
        };
        self.append_accounting_delta(&delta)
    }

    fn append_accounting_delta(&mut self, delta: &AccountingDelta) -> Result<()> {
        self.active_accounting_delta_log.append(delta)?;
        Ok(())
    }

    fn append_and_sync_accounting_delta(
        &mut self,
        delta: AccountingDelta,
        durable_lsn: StrataLsn,
    ) -> Result<ActiveDeltaLogState> {
        self.active_accounting_delta_log.append(&delta)?;
        self.active_accounting_delta_log.sync_data()?;
        let state = self.active_accounting_delta_log.state();
        if state.durable_lsn < durable_lsn {
            return Err(Error::DurabilityAccountingGap {
                required_lsn: durable_lsn,
                active_delta_log_lsn: state.durable_lsn,
            });
        }
        Ok(state)
    }

    /// Returns the active generation key for a shard that can accept writes.
    ///
    /// A stale writer that only knows `shard_id` must not write into a shard
    /// after it has been dropped and recreated. This forces every put to use the current generation
    /// stored in the registry.
    fn openable_shard_key(&self, shard_id: ShardId) -> Result<ShardKey> {
        match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => Ok(info.key(shard_id)),
            Some(info) => Err(Error::ShardUnavailable {
                shard_id,
                generation: info.current_generation,
                current_generation: info.current_generation,
                state: info.state,
            }),
            None => Err(Error::ShardNotFound { shard_id }),
        }
    }

    /// Runs all rollover side effects whose metadata was just committed.
    ///
    /// Failure mode avoided: the sealer queue is outside RocksDB and cannot be rolled back. Running
    /// this only after commit means a crash before commit has no queued seal for an index-invisible
    /// segment.
    fn run_rollover_post_commit(&self, pending_rollovers: Vec<PendingRollover>) {
        for rollover in pending_rollovers {
            rollover.run_post_commit(self.seal_tx.clone(), self.metrics.clone());
        }
    }

    fn process_timed_checkpoint(&mut self) -> Result<()> {
        self.last_checkpoint_at = Instant::now();
        let sealed_before_lsn = self.index.get_next_lsn()?;
        if sealed_before_lsn <= self.last_checkpoint_next_lsn {
            return Ok(());
        }

        self.rollover_active_segment(sealed_before_lsn)?;
        let pending_rollovers = self.take_pending_rollovers();
        let commit_result = (|| {
            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            batch.write().map_err(strata_index::Error::from)?;
            Ok::<(), Error>(())
        })();

        match commit_result {
            Ok(()) => {
                self.run_rollover_post_commit(pending_rollovers);
                Ok(())
            }
            Err(error) => {
                self.restore_pending_rollovers(pending_rollovers);
                Err(error)
            }
        }
    }

    /// Swaps in a fresh segment and hands the full one to the sealer.
    ///
    /// The pre-existing-file check handles a specific crash: a previous run created the new
    /// segment file but died before the index batch committed. That file has no index state, so
    /// nothing references it and it's safe to delete and recreate. (The startup orphan sweep
    /// catches the same case, but a rollover can hit it mid-run too.)
    ///
    /// The old segment flips to `Sealing` in the next LSN-bearing commit that also publishes the
    /// new segment, so rollover metadata stays in the same store-global order as preceding payload
    /// writes. Sealing itself is queued only after that commit succeeds.
    fn rollover_active_segment(&mut self, sealed_before_lsn: StrataLsn) -> Result<()> {
        self.wait_for_seal_backlog_capacity()?;
        let old_segment_id = self.active_writer.segment_id();
        if self.active_accounting_delta_log.segment_id() != old_segment_id {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "active accounting log segment {} does not match active data segment {}",
                    self.active_accounting_delta_log.segment_id(),
                    old_segment_id
                ),
            });
        }
        self.active_accounting_delta_log.flush_for_rollover()?;
        let old_write_offset = self.active_writer.write_offset();
        let new_segment_id = self.segment_ids.allocate()?;
        let new_path = segment_path(&self.config, new_segment_id);
        if new_path.exists() && self.index.get_segment_state(new_segment_id)?.is_none() {
            fs::remove_file(&new_path).map_err(|source| Error::Io {
                path: new_path.clone(),
                source,
            })?;
        }
        let new_accounting_path =
            ActiveDeltaLog::path(self.config.accounting_index_dir(), new_segment_id);
        if new_accounting_path.exists() && self.index.get_segment_state(new_segment_id)?.is_none() {
            fs::remove_file(&new_accounting_path).map_err(|source| Error::Io {
                path: new_accounting_path.clone(),
                source,
            })?;
        }

        let new_writer = SegmentWriter::create(
            &new_path,
            new_segment_id,
            PlacementClass::Ingest,
            self.config.segment_max_bytes,
        )?;
        let new_accounting_log = ActiveDeltaLog::open(
            self.config.accounting_index_dir(),
            new_segment_id,
            self.active_accounting_delta_log.state(),
        )?;
        let new_state = active_segment_state(&self.config, self.ingest_owner, &new_writer, 0);
        let mut old_state = self.active_segment_state.clone();
        old_state.write_offset = old_write_offset;
        old_state.durable_offset = self.durable_offset;
        old_state.state = SegmentFileState::Sealing;
        old_state.sealed_before_lsn = Some(sealed_before_lsn);

        self.pending_rollovers.push(PendingRollover {
            old_segment_state: old_state,
            new_segment_state: new_state.clone(),
            seal_task: SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: old_write_offset,
                sealed_before_lsn,
            },
        });
        self.active_writer = new_writer;
        self.active_accounting_delta_log = new_accounting_log;
        self.active_segment_state = new_state;
        self.durable_offset = 0;
        self.last_checkpoint_at = Instant::now();
        self.last_checkpoint_next_lsn = sealed_before_lsn;
        self.metrics.set_active_segment(
            self.active_writer.segment_id(),
            self.active_writer.write_offset(),
            self.durable_offset,
        );
        Ok(())
    }

    /// Ensures the active segment can fit the next record, rolling over as many times as needed.
    ///
    /// Records are never split across segment files. If a too large record
    /// were partially appended before discovering the limit, recovery would only see a torn record
    /// and would have to roll back unrelated later LSNs.
    fn ensure_segment_capacity(&mut self, record_len: u64, record_lsn: StrataLsn) -> Result<()> {
        loop {
            let attempted_size = self
                .active_writer
                .write_offset()
                .checked_add(record_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            if attempted_size <= self.config.segment_max_bytes {
                return Ok(());
            }
            // This record cannot fit into an empty segment.
            // So we must return an error since Strata records
            // are never split across segment files.
            // TODO: Why can we not just have a large enough segment to accommodate the record?
            if self.active_writer.write_offset() == 0 {
                return Err(strata_segment::Error::SegmentFull {
                    max_size: self.config.segment_max_bytes,
                    attempted_size,
                }
                .into());
            }
            self.rollover_active_segment(record_lsn)?;
        }
    }

    /// Backpressure: if the sealer can't keep up, writes eventually block here instead of
    /// accumulating unbounded unsealed segments. Unsealed segments are the expensive thing at
    /// restart (each one gets a full recovery scan), so the cap directly bounds worst-case
    /// recovery time.
    fn wait_for_seal_backlog_capacity(&self) -> Result<()> {
        let started = Instant::now();
        let mut waiting = false;
        loop {
            if unsealed_ingest_segment_count(&self.index)? < self.config.max_unsealed_segments {
                if waiting {
                    self.metrics
                        .finish_seal_backpressure_wait(started.elapsed());
                    self.gc_concurrency.set_seal_backpressure(false);
                }
                return Ok(());
            }
            if !waiting {
                waiting = true;
                self.metrics.start_seal_backpressure_wait();
                self.gc_concurrency.set_seal_backpressure(true);
            }
            thread::sleep(SEAL_BACKLOG_WAIT);
        }
    }

    /// The durability step. The ordering here is the single most load bearing thing in this
    /// file:
    ///
    /// 1. fsync the segment bytes and active accounting delta log,
    /// 2. then write durable offsets + durable_lsn to the index,
    /// 3. then fsync the RocksDB WAL.
    ///
    /// Bytes become durable strictly before the metadata that claims they are. A crash between
    /// any two steps leaves the index claiming *less* than what's on disk — never more — and
    /// recovery re-derives the frontier (it can even promote bytes the crash interrupted us from
    /// claiming). Reversing 1 and 2 would let a persisted durable_lsn point at bytes that never
    /// reached the platter, which is the one lie this design must never tell, because the Walrus
    /// event cursor advances based on it.
    ///
    /// The durable LSN frontier itself is computed by walking unaccounted ops forward while their
    /// record bytes are covered by fsynced offsets (see `seal::compute_durable_lsn`), then clamped
    /// to the accounting delta log frontier.
    fn sync_data(&mut self, mut profile: Option<&mut StoreSyncProfile>) -> Result<()> {
        let started = Instant::now();
        let previous_durable_offset = self.durable_offset;
        let durable_offset = self.active_writer.write_offset();
        let segment_sync_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.segment_sync += elapsed,
            || self.active_writer.sync_data(),
        );
        if let Err(error) = segment_sync_result {
            self.metrics.record_sync(Err(()), started.elapsed());
            return Err(error.into());
        }
        let accounting_sync_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.accounting_delta_sync += elapsed,
            || {
                let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
                self.active_accounting_delta_log.sync_data()?;
                let state = self.active_accounting_delta_log.state();
                if state.durable_lsn < committed_lsn {
                    return Err(Error::DurabilityAccountingGap {
                        required_lsn: committed_lsn,
                        active_delta_log_lsn: state.durable_lsn,
                    });
                }
                Ok(state)
            },
        );
        let active_delta_state = match accounting_sync_result {
            Ok(state) => state,
            Err(error) => {
                self.metrics.record_sync(Err(()), started.elapsed());
                return Err(error);
            }
        };

        let _publish_guard = self
            .durability_publish_lock
            .lock()
            .expect("durability publish lock poisoned");
        let (state, batch, durable_lsn) = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.durable_lsn_compute += elapsed,
            || {
                let mut state = self.active_segment_state.clone();
                if let Some(existing) = self
                    .index
                    .get_segment_state(self.active_writer.segment_id())?
                {
                    state.volume_id = existing.volume_id;
                    state.path = existing.path;
                    state.placement_class = existing.placement_class;
                    state.state = existing.state;
                    state.min_lsn = existing.min_lsn;
                    state.max_lsn = existing.max_lsn;
                    state.sealed_before_lsn = existing.sealed_before_lsn;
                    state.sealed_len = existing.sealed_len;
                    state.sealed_sha256 = existing.sealed_sha256;
                }
                state.write_offset = self.active_writer.write_offset();
                state.durable_offset = durable_offset;
                let mut batch = self.index.batch();
                self.index.put_segment_state_batch(&mut batch, &state)?;
                let current_durable_lsn = self.index.get_durable_lsn()?;
                let mut active_delta_state = active_delta_state;
                active_delta_state.durable_lsn =
                    active_delta_state.durable_lsn.max(current_durable_lsn);
                let durable_lsn = durable_lsn_with_accounting_frontier(
                    &self.index,
                    Some(&state),
                    Some(active_delta_state),
                )?;
                self.index.put_durable_lsn_batch(&mut batch, durable_lsn)?;
                self.index
                    .put_accounting_active_delta_log_state_batch(&mut batch, active_delta_state)?;
                Ok::<_, Error>((state, batch, durable_lsn))
            },
        )?;
        let commit_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.index_batch_commit += elapsed,
            || {
                batch
                    .write_with_sync(true)
                    .map_err(strata_index::Error::from)
            },
        );
        if let Err(error) = commit_result {
            let error = Error::from(error);
            self.metrics.record_sync(Err(()), started.elapsed());
            self.halt_writer_error("sync metadata commit", &error);
            return Err(error);
        }
        self.active_segment_state = state;
        profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.state_update += elapsed,
            || {
                self.index.set_blob_compact_safe_lsn(durable_lsn);
                self.durable_offset = durable_offset;
                self.active_segment_state.durable_offset = durable_offset;
                self.metrics.set_active_segment(
                    self.active_writer.segment_id(),
                    self.active_writer.write_offset(),
                    self.durable_offset,
                );
                self.metrics.set_durable_lsn(durable_lsn);
            },
        );
        self.metrics.record_sync(
            Ok(durable_offset.saturating_sub(previous_durable_offset)),
            started.elapsed(),
        );
        self.gc_concurrency.observe_sync(
            started.elapsed(),
            durable_offset.saturating_sub(previous_durable_offset),
        );
        if self.accounting_tx.is_some() {
            profile_phase(
                profile,
                |profile, elapsed| profile.accounting_nudge += elapsed,
                || self.nudge_accounting(),
            );
        }
        Ok(())
    }

    /// Requests share a bounded(1) wake channel but retain their highest pending priority
    /// separately, so coalescing never applies backpressure or loses a materialization request.
    fn nudge_accounting(&self) {
        if let Some(accounting_tx) = &self.accounting_tx {
            accounting_tx.request_ingest();
        }
    }
}

#[derive(Debug)]
struct PlannedGcOutputSegments {
    /// Translation from temporary staging segment ids to final durable segment ids.
    staged_to_final_segment_id: BTreeMap<SegmentId, SegmentId>,
    /// Staging segment ids that actually contain at least one survivor.
    used_staged_ids: BTreeSet<SegmentId>,
    /// User-facing publication metadata for every output segment made visible.
    published_outputs: Vec<GcPublishedOutputSegment>,
    /// Durable segment state rows to install in the publish batch.
    segment_states: Vec<SegmentState>,
}

#[derive(Debug)]
enum GcPublishCommitError {
    BeforeIndexBatch(Error),
    IndexCommit(Error),
}

impl From<Error> for GcPublishCommitError {
    fn from(error: Error) -> Self {
        Self::BeforeIndexBatch(error)
    }
}

impl From<strata_accounting::Error> for GcPublishCommitError {
    fn from(error: strata_accounting::Error) -> Self {
        Self::BeforeIndexBatch(error.into())
    }
}

impl From<strata_index::Error> for GcPublishCommitError {
    fn from(error: strata_index::Error) -> Self {
        Self::BeforeIndexBatch(error.into())
    }
}

impl From<strata_segment::Error> for GcPublishCommitError {
    fn from(error: strata_segment::Error) -> Self {
        Self::BeforeIndexBatch(error.into())
    }
}

/// Terminal state assigned to copied bytes that became stale before publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GcSkippedCopiedRecordKind {
    /// The source was overwritten, tombstoned, or mapped before publish.
    Retired,
    /// The source became dead because its lifecycle expired before publish.
    Expired,
}

/// A staged copy whose source is no longer eligible to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GcSkippedCopiedRecord {
    /// Original staged copy metadata. This is still useful to classify bytes already present in an
    /// output file that also contains survivors.
    record: GcStagedCopiedRecord,
    /// Whether those bytes should be accounted as retired or expired garbage.
    kind: GcSkippedCopiedRecordKind,
}

/// Reconciles copied records against ref events published after the GC planning snapshot.
///
/// A terminal event means the staged copy must not receive a `MapRef`; the source no longer
/// protects that logical blob. A lifecycle-only event keeps the copy publishable, but the survivor
/// must inherit the freshest lifecycle so destination overlay state is accurate at publish time.
fn split_gc_copied_records(
    records: Vec<GcStagedCopiedRecord>,
    accounting_changes: &[AccountingRefEvent],
    obsolete_shards: &BTreeSet<ShardKey>,
) -> (Vec<GcStagedCopiedRecord>, Vec<GcSkippedCopiedRecord>) {
    let mut terminal_sources = BTreeMap::<(SegmentId, u64), GcSkippedCopiedRecordKind>::new();
    let mut latest_lifecycles = BTreeMap::<(SegmentId, u64), Option<BlobLifecycle>>::new();
    for event in accounting_changes {
        let source_key = (event.key.segment_id, event.key.offset);
        match event.event {
            SegmentRefEvent::Retired => {
                terminal_sources.insert(source_key, GcSkippedCopiedRecordKind::Retired);
            }
            SegmentRefEvent::Expired => {
                terminal_sources.insert(source_key, GcSkippedCopiedRecordKind::Expired);
            }
            SegmentRefEvent::LifecycleChanged { lifecycle } => {
                latest_lifecycles.insert(source_key, lifecycle);
            }
        }
    }

    let mut survivors = Vec::new();
    let mut skipped = Vec::new();
    for mut record in records {
        if obsolete_shards.contains(&record.source.shard) {
            skipped.push(GcSkippedCopiedRecord {
                record,
                kind: GcSkippedCopiedRecordKind::Retired,
            });
            continue;
        }
        let source_key = (record.source.from.segment_id, record.source.from.offset);
        if let Some(kind) = terminal_sources.get(&source_key).copied() {
            skipped.push(GcSkippedCopiedRecord { record, kind });
        } else {
            if let Some(lifecycle) = latest_lifecycles.get(&source_key).copied() {
                record.source.lifecycle = lifecycle;
            }
            survivors.push(record);
        }
    }
    (survivors, skipped)
}

/// Assigns consecutive publish LSNs and final destination refs to copied survivors.
///
/// The staged record already knows its offset and length inside a temporary output file. This helper
/// replaces the temporary segment id with the final durable segment id and pairs each move with the
/// LSN that will order its `MapRef` in the accounting delta log.
fn assign_gc_publish_lsns(
    first_lsn: StrataLsn,
    records: &[GcStagedCopiedRecord],
    staged_to_final_segment_id: &BTreeMap<SegmentId, SegmentId>,
) -> Result<Vec<GcPublishedRecord>> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let publish_lsn = first_lsn
                .checked_add(index as u64)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            let segment_id = staged_to_final_segment_id
                .get(&record.staged.segment_id)
                .copied()
                .ok_or(Error::GcMissingStagedOutput {
                    staged_segment_id: record.staged.segment_id,
                })?;
            Ok(GcPublishedRecord {
                source: record.source.clone(),
                to: RecordRef {
                    segment_id,
                    offset: record.staged.offset,
                    len: record.staged.len,
                },
                publish_lsn,
            })
        })
        .collect()
}

/// Updates output segment LSN bounds from the records published into each segment.
///
/// GC output segments are sealed before they enter the manifest, so their `write_offset` is already
/// known. Their `min_lsn`/`max_lsn` bounds come from the synthetic `MapRef` LSNs assigned during
/// publish and are used by later liveness-completeness checks.
fn apply_gc_output_lsn_bounds(
    states: &mut [SegmentState],
    published_records: &[GcPublishedRecord],
) {
    let mut bounds = BTreeMap::<SegmentId, (StrataLsn, StrataLsn)>::new();
    for record in published_records {
        bounds
            .entry(record.to.segment_id)
            .and_modify(|(min_lsn, max_lsn)| {
                *min_lsn = (*min_lsn).min(record.publish_lsn);
                *max_lsn = (*max_lsn).max(record.publish_lsn);
            })
            .or_insert((record.publish_lsn, record.publish_lsn));
    }
    for state in states {
        if let Some((min_lsn, max_lsn)) = bounds.get(&state.segment_id).copied() {
            state.min_lsn = Some(min_lsn);
            state.max_lsn = Some(max_lsn);
        }
    }
}

/// Groups stale copied output ranges by final output segment and terminal kind.
///
/// A staging file can contain both survivors and stale copies. If any survivor is published, the
/// whole sealed output file becomes durable, so stale ranges inside it must be accounted as garbage
/// in the output segment rather than silently ignored.
fn skipped_gc_output_ranges(
    skipped_records: &[GcSkippedCopiedRecord],
    staged_to_final_segment_id: &BTreeMap<SegmentId, SegmentId>,
) -> BTreeMap<(SegmentId, GcSkippedCopiedRecordKind), Vec<SegmentGcRecordRange>> {
    let mut ranges =
        BTreeMap::<(SegmentId, GcSkippedCopiedRecordKind), Vec<SegmentGcRecordRange>>::new();
    for record in skipped_records {
        if let Some(segment_id) = staged_to_final_segment_id.get(&record.record.staged.segment_id) {
            ranges
                .entry((*segment_id, record.kind))
                .or_default()
                .push(SegmentGcRecordRange {
                    offset: record.record.staged.offset,
                    len: record.record.staged.len,
                });
        }
    }
    ranges
}

/// Builds the bulk active log delta that makes GC relocations visible to blob accounting.
///
/// Each record still has its own logical publish LSN, but the active log stores the whole publish
/// chunk as one frame. Sidecar ingestion expands the frame back into ordered `MapRef` updates using
/// `base_lsn + index`.
fn gc_publish_accounting_delta(records: &[GcPublishedRecord]) -> Option<AccountingDelta> {
    let base_lsn = records.first()?.publish_lsn;
    Some(AccountingDelta::GcMapRefBatch {
        base_lsn,
        maps: records
            .iter()
            .map(|record| GcMapRefDelta {
                key: record.source.key.clone(),
                from: record.source.from,
                to: record.to,
            })
            .collect(),
    })
}

fn gc_segment_file_path(config: &StrataStoreConfig, state: &SegmentState) -> std::path::PathBuf {
    if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    }
}

fn unlink_gc_segment_file(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    unlink_gc_segment_files(config, std::slice::from_ref(state))
}

fn unlink_gc_segment_files(config: &StrataStoreConfig, states: &[SegmentState]) -> Result<()> {
    let mut parents = BTreeSet::new();
    for state in states {
        let path = gc_segment_file_path(config, state);
        match fs::remove_file(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
    }

    for parent in parents {
        sync_dir(&parent)?;
        prune_empty_retention_dirs(config, parent)?;
    }
    Ok(())
}

pub(crate) fn prune_empty_retention_dirs(
    config: &StrataStoreConfig,
    mut directory: std::path::PathBuf,
) -> Result<()> {
    let root = retention_dir(config);
    while directory != root && directory.starts_with(&root) {
        match fs::remove_dir(&directory) {
            Ok(()) => {
                sync_parent_dir(&directory)?;
                let Some(parent) = directory.parent() else {
                    break;
                };
                directory = parent.to_path_buf();
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = directory.parent() else {
                    break;
                };
                directory = parent.to_path_buf();
            }
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                break;
            }
            Err(source) => {
                return Err(Error::Io {
                    path: directory,
                    source,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Removes GC staging attempts left behind by a crash before publish/prepublish.
///
/// Staging files are never referenced by segment state; after restart there is no in-memory
/// `PreparedGcCopy` that could publish them, so the only correct recovery action is deletion.
fn cleanup_stale_gc_staging_dirs(config: &StrataStoreConfig) -> Result<()> {
    let staging_root = config.namespace_dir().join("gc-staging");
    match fs::remove_dir_all(&staging_root) {
        Ok(()) => sync_parent_dir(&staging_root),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: staging_root,
            source,
        }),
    }
}

/// Resolves a key to its readable payload, or None for missing/tombstoned blobs.
///
/// A live head without a payload ref should not be produced by new writes. If recovery leaves such
/// a head behind, there are no bytes to return, so None is the honest answer.
pub(crate) fn resolve_blob_version(
    index: &StrataIndex,
    shard: ShardKey,
    key: &BlobKey,
) -> Result<Option<ResolvedBlobVersion>> {
    let Some(state) = index.get_blob_state(key)? else {
        return Ok(None);
    };
    let Some(head) = state.versions.resolve_head(shard) else {
        return Ok(None);
    };
    if head.entry.is_tombstone() {
        return Ok(None);
    }

    let Some(record_ref) = head.entry.record_ref else {
        return Ok(None);
    };
    let lifecycle = state.lifecycle.resolve_at(StrataLsn::MAX);
    if lifecycle
        .tombstone_lsn
        .is_some_and(|tombstone_lsn| tombstone_lsn > head.head_lsn)
    {
        return Ok(None);
    }
    if lifecycle
        .expiry_lsn
        .is_some_and(|expiry_lsn| expiry_lsn > head.head_lsn)
    {
        return Ok(None);
    }
    let current_epoch = index
        .get_current_epoch()?
        .ok_or(Error::EpochNotInitialized)?;
    let lifecycle = effective_lifecycle_for_payload(index, head.head_lsn, &lifecycle)?;
    if lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch) {
        return Ok(None);
    }
    Ok(Some(ResolvedBlobVersion {
        head_lsn: head.head_lsn,
        record_ref,
        generation: head.entry.generation,
        lifecycle,
    }))
}

fn effective_lifecycle_for_payload(
    index: &StrataIndex,
    payload_lsn: StrataLsn,
    lifecycle: &BlobLifecycleHead,
) -> Result<Option<BlobLifecycle>> {
    let Some(lifetime) = &lifecycle.lifetime else {
        return Ok(None);
    };
    if lifetime.lsn <= payload_lsn {
        let payload_epoch = index
            .latest_epoch_at_lsn(payload_lsn)?
            .map(|(_, epoch)| epoch)
            .ok_or(Error::EpochNotInitialized)?;
        if lifetime.lifecycle.logical_end_epoch <= payload_epoch {
            return Ok(None);
        }
    }
    Ok(Some(lifetime.lifecycle))
}

/// Opens the segment chosen for appends, creating it only if recovery did not already leave a file
/// for that segment id.
///
/// Failure mode avoided: after a clean reopen, the active segment usually already exists with valid
/// trailing bytes. Recreating it would truncate those bytes and force recovery to roll back
/// committed-but-not-yet-sealed writes.
fn open_active_writer(
    config: &StrataStoreConfig,
    active_segment_id: SegmentId,
) -> Result<SegmentWriter> {
    ensure_ingest_dir(config)?;

    let active_path = segment_path(config, active_segment_id);
    if active_path.exists() {
        Ok(SegmentWriter::open_existing(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
        )?)
    } else {
        Ok(SegmentWriter::create(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
        )?)
    }
}

fn ensure_ingest_dir(config: &StrataStoreConfig) -> Result<()> {
    fs::create_dir_all(config.ingest_dir()).map_err(|source| Error::Io {
        path: config.ingest_dir(),
        source,
    })
}

/// First open of a shard automatically registers it, afterwards the (id, generation) pair must match the
/// Creates the standalone convenience shard on a new namespace.
///
/// An existing row is intentionally left unchanged. In particular, reopening a store after shard
/// zero was dropped must preserve that fence so `add_shard(0)` can create the next generation.
fn ensure_default_shard_registered(index: &StrataIndex) -> Result<()> {
    if index.get_shard_info(STANDALONE_SHARD.id)?.is_none() {
        index.put_shard_info(
            STANDALONE_SHARD.id,
            ShardInfo::active(STANDALONE_SHARD.generation),
        )?;
    }
    Ok(())
}

/// Deletes (or, under AbsoluteConsistency, reports) segment files that have no index state.
///
/// An orphan can only mean one thing: a rollover crashed after creating the file but before the
/// index batch committed, so no reference to it ever existed. It must be removed *before* any
/// writer starts, because the writer picks segment ids by incrementing past the indexed maximum
/// and would otherwise happily reuse the orphan's id with stale bytes already in the file.
fn reconcile_orphan_ingest_segment_files(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    let indexed_segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .collect::<BTreeSet<_>>();
    let ingest_dir = config.ingest_dir();
    let entries = fs::read_dir(&ingest_dir).map_err(|source| Error::Io {
        path: ingest_dir.clone(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: ingest_dir.clone(),
            source,
        })?;
        let path = entry.path();
        let Some(file_name) = path.file_name() else {
            continue;
        };
        let Some(segment_id) = parse_segment_file_name(file_name) else {
            continue;
        };
        if indexed_segment_ids.contains(&segment_id) {
            continue;
        }

        match config.recovery_policy {
            StrataRecoveryPolicy::PointInTime => {
                fs::remove_file(&path).map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            }
            StrataRecoveryPolicy::AbsoluteConsistency => {
                return Err(Error::OrphanSegmentFile { segment_id, path });
            }
        }
    }

    Ok(())
}

/// Recovery driver for everything that wasn't sealed. Three phases, in order:
///
/// 1. Scan each unsealed segment (in segment-id order, which is also write order) and keep its
///    longest valid prefix. The first segment that comes up short poisons everything after it:
///    later segments hold later LSNs, and keeping LSN 50 while LSN 40 is gone would break the
///    "durable means a contiguous prefix" contract — so later segments are discarded outright.
/// 2. Roll back index entries whose bytes didn't survive (see `rollback_lost_operations`).
///
/// This deliberately does not advance `next_lsn` from records found only in segment files. The
/// segment file is the payload log, not the commit log: a batch can reserve LSN 10 for an epoch
/// increment and LSN 11 for a put, write the LSN 11 payload record, then crash before the RocksDB
/// batch publishes either operation. If recovery treated that segment record as committed and
/// bumped `next_lsn` to 12, it would create a hole at LSN 10 and silently drop the epoch change.
/// Even a put-only batch has the same shape: if the process exits after writing payload bytes but
/// before RocksDB publishes the batch, recovery must not make those bytes visible. Only RocksDB's
/// batch tells us which LSNs committed; segment recovery can promote/truncate bytes for
/// already-indexed operations, but it must not discover new committed LSNs from payload bytes
/// alone.
///
/// The caller recomputes the durable LSN frontier after the active accounting delta log has been
/// truncated to the same committed prefix. Post recovery we can assert the fact that index has
/// next_lsn == durable_lsn + 1.
fn recover_unsealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut discard_later_segments = false;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if discard_later_segments {
            discard_unsealed_segment(config, index, segment_id, metrics)?;
            continue;
        }

        let recovered = recover_unsealed_segment(config, index, segment_id, metrics)?;
        metrics.record_recovered_segment(recovered.is_complete);
        if !recovered.is_complete {
            discard_later_segments = true;
        }
    }
    rollback_lost_operations(config, index, metrics)?;
    Ok(())
}

/// Reopens the active accounting delta log and makes it agree with the recovered index prefix.
///
/// The active log is a sidecar replay source for accounting. The writer appends deltas before it
/// commits the matching RocksDB index batch, but the log append is not made durable until a later
/// sync, so a crash can leave the two prefixes disagreeing in either direction:
///
/// - Log ahead of RocksDB: the delta append happened, but the index batch did not commit. Example:
///   the log contains LSN 25 while `next_lsn` still says 25 is free. Those deltas are not store
///   history, so we truncate them before accounting can replay phantom work.
/// - RocksDB ahead of the log: the index batch survived, but the active log's valid prefix does not
///   reach every committed LSN. Accounting would never see those committed operations, so recovery
///   rolls the store back to the first LSN missing from the log.
///
/// Once both prefixes match, we fsync the trimmed log. That makes the log state a valid accounting
/// frontier for the recovered `durable_lsn`: payload bytes may have been promoted by segment
/// recovery, but an LSN is not durable until its accounting delta is durable too. In summary, the
/// rough order of operations here to ensure durable_lsn is consistent in log and segment files:
/// 1. Trim active delta log to current committed prefix.
/// 2. If active delta log is shorter than RocksDB’s committed prefix, call rollback_operations_from(...),
///    which rewinds next_lsn.
/// 3. Recompute committed_lsn from the new next_lsn.
/// 4. Sync the recovered active delta log.
/// 5. Call advance_recovered_durable_lsn, which finally writes the recovered durable_lsn.
fn recover_active_accounting_delta_log(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    active_segment_id: SegmentId,
) -> Result<ActiveDeltaLog> {
    let mut log = open_active_accounting_delta_log(config, index, active_segment_id)?;

    // First trim the easy mismatch: deltas for operations that never committed to RocksDB.
    let committed_lsn = index.get_next_lsn()?.saturating_sub(1);
    log.truncate_after_lsn(committed_lsn)?;

    // Then handle the opposite mismatch. If the log is shorter than RocksDB's committed prefix,
    // keep the contiguous-prefix invariant by rolling back store metadata that accounting could not
    // replay. Durable operations are not rollbackable here: if the active log cannot replay through
    // `durable_lsn`, the durable promise is already broken and recovery must stop. Otherwise
    // `rollback_operations_from` rewinds only the non-durable committed tail, so recompute
    // `committed_lsn`.
    let delta_log_lsn =
        ActiveDeltaLog::max_lsn_through(config.accounting_index_dir(), active_segment_id)?
            .unwrap_or_default();
    if delta_log_lsn < committed_lsn {
        let durable_lsn = index.get_durable_lsn()?;
        if delta_log_lsn < durable_lsn {
            return Err(Error::RecoveryDurableAccountingGap {
                durable_lsn,
                active_delta_log_lsn: delta_log_lsn,
            });
        }
        // Here we are only rolling back the non-durable store operations.
        rollback_operations_from(config, index, metrics, delta_log_lsn.saturating_add(1))?;
    }
    let committed_lsn = index.get_next_lsn()?.saturating_sub(1);
    log.truncate_after_lsn(committed_lsn)?;

    // Persist the recovered log prefix before publishing a recomputed durable frontier that depends
    // on it. This is the recovery equivalent of the foreground sync ordering.
    log.sync_data()?;
    advance_recovered_durable_lsn(index, metrics, log.state())?;
    Ok(log)
}

/// Opens the active accounting delta log at the persisted durable frontier.
///
/// Failure mode avoided: the delta log and store durable_lsn are flushed in separate files. If
/// RocksDB remembers durable_lsn 40 but the delta-log state row still says 37, using the lower
/// value would make recovery treat already-acknowledged operations as not covered by accounting
/// deltas.
fn open_active_accounting_delta_log(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    active_segment_id: SegmentId,
) -> Result<ActiveDeltaLog> {
    let mut durable_state =
        index
            .get_accounting_active_delta_log_state()?
            .unwrap_or(ActiveDeltaLogState {
                segment_id: 0,
                durable_offset: 0,
                durable_lsn: index.get_durable_lsn()?,
            });

    durable_state.durable_lsn = durable_state.durable_lsn.max(index.get_durable_lsn()?);
    Ok(ActiveDeltaLog::open(
        config.accounting_index_dir(),
        active_segment_id,
        durable_state,
    )?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentRecovery {
    is_complete: bool,
}

/// Recovers one unsealed segment by scanning records from offset 0 and keeping the longest
/// checksummed-valid prefix.
///
/// The scan deliberately validates *past* the persisted durable offset: after a process crash
/// (as opposed to power loss) appended bytes usually survive in the kernel page cache, and after
/// a power loss they may still have been fsynced without the durable-offset row committing. If
/// complete records are sitting there and the index has matching entries, throwing them away
/// would be rolling back writes for no reason — so they get promoted instead.
///
/// `is_complete` is the signal the driver uses to discard later segments: an incomplete prefix
/// means some indexed LSNs in this segment are gone, so nothing after it may be kept either.
fn recover_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    metrics: &StrataStoreMetrics,
) -> Result<SegmentRecovery> {
    let path = segment_path(config, segment_id);
    let existing_state = index.get_segment_state(segment_id)?;
    let expected_write_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.write_offset);
    let durable_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.durable_offset);
    if !path.exists() {
        if expected_write_offset == 0 {
            return Ok(SegmentRecovery { is_complete: true });
        }
        return recover_missing_unsealed_segment(
            config,
            index,
            segment_id,
            expected_write_offset,
            durable_offset,
            metrics,
        );
    }

    let mut scanner = SegmentScanner::open(&path, segment_id)?;
    let prefix = scanner.scan_recoverable_prefix(durable_offset)?;
    let is_complete = prefix.valid_len >= expected_write_offset;
    let recovered_write_offset = prefix.valid_len.min(expected_write_offset);
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency
        && (prefix.valid_len != expected_write_offset || prefix.file_len != expected_write_offset)
    {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset,
        });
    }

    let recovered_durable_offset = persist_recovered_segment_prefix(
        &path,
        prefix.file_len,
        durable_offset,
        recovered_write_offset,
    )?;

    apply_recovered_segment_prefix(
        config,
        index,
        segment_id,
        RecoveredSegmentPrefix {
            existing_state,
            durable_offset: recovered_durable_offset,
            recovered_write_offset,
            records: &prefix.records,
        },
        metrics,
    )?;
    Ok(SegmentRecovery { is_complete })
}

/// Truncates a recovered segment to its valid prefix and fsyncs, so the garbage tail can never
/// be mistaken for data by a later scan. Returns the new durable offset: bytes the scan validated
/// beyond the old durable offset are promoted (they're provably on disk after this fsync), which
/// is how recovery can end up *more* durable than the pre-crash metadata claimed.
fn persist_recovered_segment_prefix(
    path: &Path,
    file_len: u64,
    durable_offset: u64,
    recovered_write_offset: u64,
) -> Result<u64> {
    let needs_truncate = file_len != recovered_write_offset;
    let promotes_recovered_bytes = recovered_write_offset > durable_offset;
    if !needs_truncate && !promotes_recovered_bytes {
        return Ok(durable_offset);
    }

    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if needs_truncate {
        file.set_len(recovered_write_offset)
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }
    file.sync_data().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;

    if promotes_recovered_bytes {
        Ok(recovered_write_offset)
    } else {
        Ok(durable_offset)
    }
}

/// An indexed unsealed segment whose file vanished. The durable offset draws the line between
/// "annoying" and "catastrophic": if no bytes were ever declared durable, the file only held
/// unacknowledged writes and point-in-time recovery may discard it like a torn tail. But if
/// bytes *were* declared durable, someone upstream may have already acted on that promise (the
/// event cursor advanced), so this is unrecoverable data loss and must be a hard error rather
/// than a silent rollback.
fn recover_missing_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    expected_write_offset: u64,
    durable_offset: u64,
    metrics: &StrataStoreMetrics,
) -> Result<SegmentRecovery> {
    if durable_offset > 0 {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    discard_unsealed_segment(config, index, segment_id, metrics)?;
    Ok(SegmentRecovery { is_complete: false })
}

struct RecoveredSegmentPrefix<'a> {
    existing_state: Option<SegmentState>,
    durable_offset: u64,
    recovered_write_offset: u64,
    records: &'a [strata_segment::ScannedRecord],
}

/// Publishes the post scan segment state and rebuilds its LSN bounds from scratch.
///
/// min/max LSN can't be trusted from the old state (the tail they described may be gone), so
/// they're recomputed by cross-checking each scanned record against the index: a record only
/// counts if the index has an entry at that exact (key, lsn) pointing at that exact record ref.
/// Records that fail the cross-check are fine to skip — they're bytes whose index batch never
/// committed, and the upcoming rollback pass is what handles the reverse case (index entries
/// whose bytes are gone).
fn apply_recovered_segment_prefix(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    prefix: RecoveredSegmentPrefix<'_>,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut state = active_segment_state_from_path(
        config,
        INGEST_SEGMENT_OWNER,
        segment_id,
        prefix.recovered_write_offset,
        prefix.durable_offset,
    );
    if let Some(existing) = prefix.existing_state {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.state = existing.state;
        state.sealed_before_lsn = existing.sealed_before_lsn;
        state.sealed_len = existing.sealed_len;
    }
    state.min_lsn = None;
    state.max_lsn = None;

    let mut batch = index.batch();
    let mut recovered_record_count = 0_u64;

    for record in prefix.records {
        let record_end = record
            .record_ref
            .offset
            .checked_add(record.record_len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        if record_end > prefix.recovered_write_offset {
            continue;
        }
        let version_key = BlobVersionKey {
            key: record.key.clone(),
            lsn: record.header.generation,
        };
        let Some(op) = index.blob_version_op_at_lsn(&version_key.key, version_key.lsn)? else {
            continue;
        };
        let entry = op.entry;
        if entry.record_ref != Some(record.record_ref) {
            continue;
        }
        recovered_record_count = recovered_record_count.saturating_add(1);

        state.min_lsn = Some(
            state
                .min_lsn
                .map_or(entry.lsn, |first| first.min(entry.lsn)),
        );
        state.max_lsn = Some(state.max_lsn.map_or(entry.lsn, |last| last.max(entry.lsn)));
    }

    index.put_segment_state_batch(&mut batch, &state)?;

    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    metrics.record_recovered_records(recovered_record_count, prefix.recovered_write_offset);
    Ok(())
}

/// Drops an unsealed segment entirely (used when a preceding segment lost data, see the driver).
/// Metadata is marked `Deleted` and flushed *before* the unlink: if we crash in between, the next
/// open sees a Deleted segment with a leftover file, which the orphan/recovery paths handle. The
/// reverse order could leave an Open segment state pointing at nothing — which is the
/// "durable bytes vanished" hard-error case.
fn discard_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut state = active_segment_state_from_path(config, INGEST_SEGMENT_OWNER, segment_id, 0, 0);
    if let Some(existing) = index.get_segment_state(segment_id)? {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.path = existing.path;
    }
    state.state = SegmentFileState::Deleted;
    state.write_offset = 0;
    state.durable_offset = 0;
    state.sealed_len = None;
    state.sealed_sha256 = None;

    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &state)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;

    let path = segment_path(config, segment_id);
    match fs::remove_file(&path) {
        Ok(()) => {
            metrics.record_discarded_segment();
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            metrics.record_discarded_segment();
            Ok(())
        }
        Err(source) => Err(Error::Io {
            path: path.clone(),
            source,
        }),
    }
}

/// Seeds the epoch timeline for a fresh namespace. The genesis row lives at LSN 0 — below every
/// real LSN — so `latest_epoch_at_lsn(any)` always has an answer; accounting and rollback both
/// rely on "epoch at LSN" never being undefined. `config.starting_epoch` only matters on first
/// creation; after that the persisted timeline wins, so changing the config later is a no-op.
/// The middle case (timeline rows exist but `CurrentEpoch` is missing)
/// rebuilds the register from the timeline, consistent with the timeline being the truth.
fn ensure_epoch_initialized(index: &StrataIndex, starting_epoch: Epoch) -> Result<Epoch> {
    if let Some(current_epoch) = index.get_current_epoch()? {
        return Ok(current_epoch);
    }

    let latest_epoch = index.latest_epoch_at_lsn(StrataLsn::MAX)?;
    let current_epoch = latest_epoch.map_or(starting_epoch, |(_, epoch)| epoch);
    let mut batch = index.batch();
    if latest_epoch.is_none() {
        index.put_epoch_change_batch(&mut batch, 0, current_epoch)?;
    }
    index.put_current_epoch_batch(&mut batch, current_epoch)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(current_epoch)
}

/// Point-in-time rollback: find the lowest LSN whose operation did not survive the crash, then
/// erase that LSN *and everything after it* — including ops whose bytes did survive.
///
/// The all-or-nothing tail erase is the point. Keeping LSN 50 while LSN 48 is gone would create
/// a history with a hole, and everything downstream — the durable frontier walk, accounting's
/// in-order replay, the epoch timeline — assumes LSNs form a contiguous prefix. This mirrors
/// RocksDB's own point-in-time WAL recovery, which the old all-in-RocksDB design got for free;
/// splitting payloads out of RocksDB means reimplementing it here.
///
/// Epoch-change rows past the cutoff are erased too, and `next_lsn` rewinds to the cutoff so
/// those LSNs get reissued. Anything left behind at a reused LSN would resurface as a phantom
/// op. The whole thing is one batch + WAL fsync; recovery is single-threaded so nobody can
/// observe the intermediate state.
///
/// Ops at or below `durable_lsn` are exempt from the survival check: if one of those is missing
/// we've already broken a promise, and the segment-recovery pass will have surfaced that as a
/// hard error rather than something to quietly roll back.
fn rollback_lost_operations(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let durable_lsn = index.get_durable_lsn()?;
    let states = index.iter_segment_states()?;
    let mut rollback_from = None;
    for (lsn, key) in index.iter_unaccounted_lsn_ops()? {
        if lsn <= durable_lsn
            || unaccounted_non_durable_operation_survived_recovery(index, lsn, &key, &states)?
        {
            continue;
        }
        rollback_from = Some(rollback_from.map_or(lsn, |current: StrataLsn| current.min(lsn)));
    }

    let Some(rollback_from) = rollback_from else {
        return Ok(());
    };

    rollback_operations_from(config, index, metrics, rollback_from)
}

/// Erases every unaccounted blob and epoch operation from `rollback_from` onward and rewinds the
/// store frontiers to that LSN.
///
/// Failure mode avoided: the cleanup must remove both blob-version merge ops and epoch rows. If
/// rollback removed only payload rows but left an epoch change at LSN 51, the next write reusing
/// LSN 51 would inherit an impossible epoch timeline.
fn rollback_operations_from(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    rollback_from: StrataLsn,
) -> Result<()> {
    let mut entries = index
        .iter_unaccounted_lsn_ops()?
        .into_iter()
        .filter(|(lsn, _)| *lsn >= rollback_from)
        .collect::<Vec<_>>();
    entries.sort_by_key(|(lsn, _)| std::cmp::Reverse(*lsn));

    let mut batch = index.batch();
    let hidden_epoch_changes = index
        .iter_epoch_changes_from(rollback_from)?
        .into_iter()
        .map(|(lsn, _)| lsn)
        .collect::<Vec<_>>();
    let mut hidden_version_count = 0usize;
    let mut hidden_versions = Vec::new();
    for (lsn, key) in entries {
        hidden_versions.push((key, lsn));
        hidden_version_count = hidden_version_count.saturating_add(1);
        index.remove_unaccounted_lsn_ops_batch(&mut batch, &[lsn])?;
    }
    index.remove_blob_ops_at_lsns_batch(&mut batch, &hidden_versions)?;
    index.remove_epoch_changes_batch(&mut batch, &hidden_epoch_changes)?;
    let hidden_relocations =
        index.remove_gc_relocations_from_lsn_batch(&mut batch, rollback_from)?;
    let hidden_gc_outputs =
        remove_gc_output_segments_from_lsn_batch(index, &mut batch, rollback_from)?;
    let rollback_ops = hidden_version_count.saturating_add(hidden_epoch_changes.len()) as u64;

    let previous_lsn = rollback_from.saturating_sub(1);
    let current_epoch = index
        .latest_epoch_at_lsn(previous_lsn)?
        .map(|(_, epoch)| epoch)
        .ok_or(Error::EpochNotInitialized)?;
    index.put_current_epoch_batch(&mut batch, current_epoch)?;
    index.put_next_lsn_batch(&mut batch, rollback_from)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    metrics.set_next_lsn(rollback_from);
    metrics.set_current_epoch(current_epoch);
    metrics.record_rollback(
        rollback_from,
        rollback_ops
            .saturating_add(hidden_relocations as u64)
            .saturating_add(hidden_gc_outputs.len() as u64),
    );
    for state in hidden_gc_outputs {
        unlink_gc_segment_file(config, &state)?;
    }
    Ok(())
}

fn remove_gc_output_segments_from_lsn_batch(
    index: &StrataIndex,
    batch: &mut typed_store::rocks::DBBatch,
    rollback_from: StrataLsn,
) -> Result<Vec<SegmentState>> {
    let mut removed = Vec::new();
    for (_, mut state) in index.iter_segment_states()? {
        if !gc_output_segment_is_hidden_by_rollback(&state, rollback_from) {
            continue;
        }
        let original = state.clone();
        state.state = SegmentFileState::Deleted;
        index.put_segment_state_batch(batch, &state)?;
        removed.push(original);
    }
    Ok(removed)
}

fn gc_output_segment_is_hidden_by_rollback(state: &SegmentState, rollback_from: StrataLsn) -> bool {
    state.state == SegmentFileState::Sealed
        && state.placement_class != PlacementClass::Ingest
        && state
            .min_lsn
            .is_some_and(|min_lsn| min_lsn >= rollback_from)
}

/// Recomputes the durable LSN frontier after recovery has settled what survived. Runs last so it
/// can pick up bytes the scan promoted — the persisted durable_lsn is a floor, not the truth,
/// after a crash.
fn advance_recovered_durable_lsn(
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    active_delta_state: ActiveDeltaLogState,
) -> Result<()> {
    let mut batch = index.batch();
    let current_durable_lsn = index.get_durable_lsn()?;
    let mut active_delta_state = active_delta_state;
    active_delta_state.durable_lsn = active_delta_state.durable_lsn.max(current_durable_lsn);
    let durable_lsn = durable_lsn_with_accounting_frontier(index, None, Some(active_delta_state))?;
    index.put_durable_lsn_batch(&mut batch, durable_lsn)?;
    index.put_accounting_active_delta_log_state_batch(&mut batch, active_delta_state)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    index.set_blob_compact_safe_lsn(durable_lsn);
    metrics.set_durable_lsn(durable_lsn);
    Ok(())
}

/// Did this op's effects survive the crash? Metadata only ops (tombstones, extensions — no
/// record_ref) survive iff their index entry exists, since the entry *is* the op. Payload ops
/// additionally need their bytes inside the segment's recovered extent. This checks
/// `write_offset`, not `durable_offset`, because it runs after the recovery scan truncated files
/// to their validated prefix — at this moment write_offset means "bytes verified present", which
/// is exactly the survival question.
fn unaccounted_non_durable_operation_survived_recovery(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let (op, lifecycle_op) = index.blob_ops_at_lsn(key, lsn)?;
    let map_ref = index.blob_map_ref_at_lsn(key, lsn)?;
    if !map_ref_survived(map_ref.as_ref(), states)? {
        return Ok(false);
    }
    let Some(op) = op else {
        // There is no survival check for lifecycle ops like tombstone or epoch extension.
        // If map ref is present, right above we already checked it survived.
        return Ok(lifecycle_op.is_some() || map_ref.is_some());
    };
    let Some(record_ref) = op.entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    let survived = states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            state.state != SegmentFileState::Deleted && state.write_offset >= record_end_offset
        });
    if !survived {
        return Ok(false);
    }
    Ok(true)
}

/// Used during crash recovery rollback. It answers a simple question:
/// For this unaccounted GC publish LSN, did the destination bytes that MapRef points to survive the crash?
/// Prepublish makes the segment file durable before MapRef, so in the normal intended path the segment should survive.
/// map_ref_survived still matters because recovery code is defensive and generic:
/// Recovery uses one rule for all unaccounted LSNs.
/// For normal puts, it checks the payload ref survived.
/// For GC MapRefs, it checks the destination ref survived.
/// Without this, MapRef only LSNs would look like “metadata-only survived” even if the destination segment is missing/truncated.
/// Prepublish should make destination bytes survive before MapRef is written.
/// map_ref_survived is the recovery assertion/enforcement of that invariant, not the primary mechanism that makes it true.
fn map_ref_survived(
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
            state.state != SegmentFileState::Deleted && state.write_offset >= record_end_offset
        }))
}

/// Makes the active segment visible in the index at open time, before any write happens. This is
/// what keeps a brand new (or just recovered) segment from looking like an orphan to the next
/// crash recovery. Invoked after restart on the active segment.
fn publish_active_segment_state(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> Result<SegmentState> {
    let existing = index.get_segment_state(active_writer.segment_id())?;
    let state = active_segment_state_with_lsn(
        config,
        owner,
        active_writer,
        durable_offset,
        existing.as_ref(),
        None,
    );
    index.put_segment_state(&state)?;
    Ok(state)
}

/// Builds the normal open segment state row for the current writer.
///
/// All open ingest rows should use the same relative path and explicit store owner.
/// Hand building this in multiple places risks one path being absolute, so a later move of the
/// store root would make that segment unreadable while others still resolve correctly.
fn active_segment_state(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> SegmentState {
    active_segment_state_with_lsn(config, owner, active_writer, durable_offset, None, None)
}

/// Builds the segment state row for the active writer. Fields the writer doesn't own
/// (volume, placement class, LSN bounds) are carried over from the existing row so a routine
/// state update can't clobber what background reorganization or recovery set. min/max LSN are
/// maintained per segment so the durable-frontier walk and GC can reason about which LSNs a
/// segment covers without scanning it.
fn active_segment_state_with_lsn(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    active_writer: &SegmentWriter,
    durable_offset: u64,
    existing: Option<&SegmentState>,
    appended_lsn: Option<StrataLsn>,
) -> SegmentState {
    let mut state = active_segment_state_from_path(
        config,
        owner,
        active_writer.segment_id(),
        active_writer.write_offset(),
        durable_offset,
    );
    if let Some(existing) = existing {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.min_lsn = existing.min_lsn;
        state.max_lsn = existing.max_lsn;
        state.sealed_before_lsn = existing.sealed_before_lsn;
    }
    if let Some(lsn) = appended_lsn {
        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }
    state
}

/// Creates a fresh segment state row from an on disk path.
///
/// New rows start with no sealed checksum or LSN bounds. Accidentally
/// carrying those fields from a previous segment id would make recovery think an open segment is
/// sealed or make GC believe it contains LSNs it never wrote.
fn active_segment_state_from_path(
    config: &StrataStoreConfig,
    owner: SegmentOwner,
    segment_id: SegmentId,
    write_offset: u64,
    durable_offset: u64,
) -> SegmentState {
    let path = segment_path(config, segment_id);
    SegmentState {
        owner,
        segment_id,
        volume_id: 0,
        path: relative_segment_path(config, path),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset,
        durable_offset,
        min_lsn: None,
        max_lsn: None,
        sealed_before_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

/// Rejects configs that would break the store's ordering or worker assumptions.
///
/// Some invalid values do not fail fast by themselves. For example,
/// `max_unsealed_segments = 1` would make the writer roll over into a second segment and then
/// wait forever for the backlog to drop below one, blocking every future write.
fn validate_config(config: &StrataStoreConfig) -> Result<()> {
    if config.namespace.trim().is_empty() {
        return Err(Error::InvalidConfig("namespace cannot be empty"));
    }
    if config.namespace.contains('/') {
        return Err(Error::InvalidConfig("namespace cannot contain '/'"));
    }
    if config.segment_max_bytes == 0 {
        return Err(Error::InvalidConfig("segment_max_bytes must be non-zero"));
    }
    if config.write_queue_capacity == 0 {
        return Err(Error::InvalidConfig(
            "write_queue_capacity must be non-zero",
        ));
    }
    // At least 2 because rollover inherently has two unsealed segments alive at once: the full
    // one waiting on the sealer and the fresh one being written. A cap of 1 would deadlock the
    // writer against its own rollover.
    if config.max_unsealed_segments < 2 {
        return Err(Error::InvalidConfig(
            "max_unsealed_segments must be at least 2",
        ));
    }
    if config.seal_worker_count == 0 {
        return Err(Error::InvalidConfig("seal_worker_count must be non-zero"));
    }
    if config.gc_workers_enabled && !config.accounting_worker_enabled {
        return Err(Error::InvalidConfig(
            "gc workers require the accounting worker",
        ));
    }
    if config.accounting_interval.is_zero() {
        return Err(Error::InvalidConfig("accounting_interval must be non-zero"));
    }
    if config.accounting_sidecar_partition_count == 0 {
        return Err(Error::InvalidConfig(
            "accounting_sidecar_partition_count must be non-zero",
        ));
    }
    if config.accounting_sidecar_interval.is_zero() {
        return Err(Error::InvalidConfig(
            "accounting_sidecar_interval must be non-zero",
        ));
    }
    if config.gc_interval.is_zero() {
        return Err(Error::InvalidConfig("gc_interval must be non-zero"));
    }
    if config.gc_worker_count == 0 {
        return Err(Error::InvalidConfig("gc_worker_count must be non-zero"));
    }
    if config.gc_initial_worker_count == 0 {
        return Err(Error::InvalidConfig(
            "gc_initial_worker_count must be non-zero",
        ));
    }
    if config.gc_initial_worker_count > config.gc_worker_count {
        return Err(Error::InvalidConfig(
            "gc_initial_worker_count must not exceed gc_worker_count",
        ));
    }
    if config.gc_tuning_window_cycles == 0 {
        return Err(Error::InvalidConfig(
            "gc_tuning_window_cycles must be non-zero",
        ));
    }
    if config.gc_sync_impact_threshold.is_zero() {
        return Err(Error::InvalidConfig(
            "gc_sync_impact_threshold must be non-zero",
        ));
    }
    if config.gc_io_bytes_per_sec == 0 {
        return Err(Error::InvalidConfig("gc_io_bytes_per_sec must be non-zero"));
    }
    if config.gc_min_io_bytes_per_sec == 0 {
        return Err(Error::InvalidConfig(
            "gc_min_io_bytes_per_sec must be non-zero",
        ));
    }
    if config.gc_min_io_bytes_per_sec > config.gc_io_bytes_per_sec {
        return Err(Error::InvalidConfig(
            "gc_min_io_bytes_per_sec must not exceed gc_io_bytes_per_sec",
        ));
    }
    Ok(())
}

/// Resume the highest open ingest segment if there is one, otherwise allocate one past the
/// highest id ever used. Ids are never reused — even for Deleted segments — because a reused id
/// could collide with a leftover file or a stale cached reader for the old segment.
fn choose_active_segment_id(index: &StrataIndex) -> Result<SegmentId> {
    let states = index.iter_segment_states()?;
    if let Some(segment_id) = states
        .iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && state.state == SegmentFileState::Open
        })
        .map(|(segment_id, _)| *segment_id)
        .max()
    {
        return Ok(segment_id);
    }

    Ok(states
        .iter()
        .map(|(segment_id, _)| *segment_id)
        .max()
        .and_then(|segment_id| segment_id.checked_add(1))
        .unwrap_or(FIRST_SEGMENT_ID))
}

fn next_segment_id_after(index: &StrataIndex, active_segment_id: SegmentId) -> Result<SegmentId> {
    index
        .iter_segment_states()?
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .chain(std::iter::once(active_segment_id))
        .max()
        .and_then(|segment_id| segment_id.checked_add(1))
        .ok_or_else(|| strata_segment::Error::RangeOverflow.into())
}

/// Cleans up pending GC output segments.
///
/// Pending GC output segments are segments that are pre published by GC. They are marked
/// as `PendingGcOutput` and are deleted if there is a crash before the GC publish LSN could become
/// durable.
fn cleanup_pending_gc_outputs(config: &StrataStoreConfig, index: &StrataIndex) -> Result<()> {
    let pending_outputs = index
        .iter_segment_states()?
        .into_iter()
        .filter_map(|(_, state)| {
            (state.state == SegmentFileState::PendingGcOutput).then_some(state)
        })
        .collect::<Vec<_>>();
    if pending_outputs.is_empty() {
        return Ok(());
    }

    for state in &pending_outputs {
        unlink_gc_segment_file(config, state)?;
    }

    let mut batch = index.batch();
    for mut state in pending_outputs {
        state.state = SegmentFileState::Deleted;
        index.put_segment_state_batch(&mut batch, &state)?;
    }
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(())
}

/// Returns unsealed ingest segments in write order.
///
/// Recovery must scan low segment ids first. If segment 3 is recovered before
/// segment 2 and segment 2 then turns out to have lost LSN 40, keeping segment 3's later LSNs would
/// create a non-contiguous history.
fn unsealed_ingest_segment_ids(index: &StrataIndex) -> Result<Vec<SegmentId>> {
    let mut segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && is_unsealed_state(state.state)
        })
        .map(|(segment_id, _)| segment_id)
        .collect::<Vec<_>>();
    segment_ids.sort_unstable();
    Ok(segment_ids)
}

fn unsealed_ingest_segment_count(index: &StrataIndex) -> Result<usize> {
    Ok(unsealed_ingest_segment_ids(index)?.len())
}

fn is_unsealed_state(state: SegmentFileState) -> bool {
    matches!(state, SegmentFileState::Open | SegmentFileState::Sealing)
}

#[cfg(test)]
mod tests;
