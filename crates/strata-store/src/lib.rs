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
//!   -> advance segment_states[active].durable_offset
//!   -> advance durable_lsn while unaccounted blob LSNs are covered by durable bytes
//!   -> store_state[DurableLsn] = durable_lsn
//!   -> fsync RocksDB WAL
//! ```
//!
//! Startup path:
//!
//! ```text
//! open
//!   -> validate config and create ingest directory
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
//!   -> read the durable range of active-delta.log
//!   -> write immutable sidecar delta runs and publish manifest + consumed cursor together
//!   -> compact delta runs into patch runs
//!   -> major-compact patches/base state, producing ordered ref events
//!   -> apply those events to segment ref events and GC overlay summary/ranges
//!   -> advance accounted_lsn while sidecar materialization covers the next durable LSN
//! ```
//!
//! The accounting worker is the *only* writer of GC overlay summary/ranges. The foreground write
//! path never reads or writes GC accounting state; it only appends cheap accounting deltas.
//! Accounting lag is expected: `accounted_lsn` says how far sidecar compaction events have been
//! reflected in the GC-facing rows. The sidecar manifest/cursor and derived rows commit atomically,
//! so crash retry reopens from one published sidecar state instead of replaying blob keys from the
//! packed version rows.

mod accounting;
mod config;
mod error;
mod gc;
mod layout;
mod metrics;
mod read;
mod reader_cache;
mod seal;

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
use strata_core::{
    BlobEntry, BlobKey, BlobLifecycle, BlobLifecycleAction, BlobLifecycleHead,
    BlobLifecycleMergeOp, BlobLifecycleOp, BlobState, BlobVersionKey, Epoch, GcRelocation,
    PlacementClass, RecordRef, SegmentFileState, SegmentGcLiveRecord, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentId, SegmentRefEvent, SegmentRefEventKey, SegmentState, ShardId,
    ShardInfo, ShardKey, ShardState, StrataLsn, VersionMergeOp, VersionOp, encoded_record_len,
};
use strata_gc::{GcAction, GcPlan};
use strata_index::StrataIndex;
pub use strata_index::{AccountingRefEvent, AccountingSnapshot, AccountingSnapshotGuard};
use strata_segment::{SegmentScanner, SegmentWriter};

use accounting::{AccountingCommand, AccountingWorker};
pub use config::{
    DEFAULT_ACCOUNTING_INTERVAL, DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
    DEFAULT_GC_SYNC_IMPACT_THRESHOLD, DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT,
    DEFAULT_SEGMENT_READER_CACHE_CAPACITY, SealedSegmentIntegrityPolicy, StrataRecoveryPolicy,
    StrataStoreConfig,
};
pub use error::{Error, Result};
pub use gc::{
    GcAccountingLag, GcPublishResult, GcPublishedOutputSegment, GcPublishedRecord,
    GcStagedCopiedRecord, GcStagedOutputSegment, PreparedGcCopy, PreparedGcPlan,
};
use gc::{
    GcCommand, GcConcurrencyConfig, GcConcurrencyController, GcExecutor, GcSourceClaims, GcWorker,
};
use layout::{parse_segment_file_name, relative_segment_path, segment_path, segment_state_path};
use metrics::PutMetric;
pub use metrics::StrataStoreMetrics;
pub use read::{ReadOptions, StoreGetProfile};
use reader_cache::SegmentReaderCache;
use seal::{
    SealCommand, SealWorker, SegmentSealTask, active_segment_durable_offset,
    durable_lsn_with_accounting_frontier, enqueue_unsealed_segments_for_sealing,
    verify_sealed_segments,
};
pub use strata_gc::{GcPlanner, GcPlannerConfig};

const FIRST_SEGMENT_ID: SegmentId = 1;
/// How long the writer naps while waiting for the sealer to drain its backlog. Short, because
/// this sleep sits on the foreground put path during rollover backpressure.
const SEAL_BACKLOG_WAIT: Duration = Duration::from_millis(10);
/// Store-global metadata namespace used for segment state, epoch state, and the global LSN
/// frontier. Logical shards still live in blob version ops and the shard registry.
pub(crate) const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};
/// Persisted scope for store-global metadata while the index schema still keys those rows by
/// `ShardKey`.
pub(crate) const STORE_SCOPE: ShardKey = STANDALONE_SHARD;

/// Single-namespace Strata store.
#[derive(Debug)]
pub struct StrataStore {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) write_tx: Option<mpsc::SyncSender<WriteCommand>>,
    writer_handle: Option<JoinHandle<()>>,
    seal_tx: Option<mpsc::Sender<SealCommand>>,
    seal_handle: Option<JoinHandle<()>>,
    accounting_tx: Option<mpsc::SyncSender<AccountingCommand>>,
    accounting_handle: Option<JoinHandle<()>>,
    pub(crate) gc_txs: Vec<mpsc::Sender<GcCommand>>,
    gc_handles: Vec<JoinHandle<()>>,
    pub(crate) accounting_lock: Arc<Mutex<()>>,
    pub(crate) gc_claims: Arc<GcSourceClaims>,
    pub(crate) gc_concurrency: Arc<GcConcurrencyController>,
    pub(crate) reader_cache: Arc<SegmentReaderCache>,
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

impl StrataStore {
    /// Opens a standalone store using the configured on-disk index directory.
    ///
    /// Failure mode avoided: callers should not separately open the index and then start store
    /// workers out of order. For example, starting a writer before orphan-file reconciliation can
    /// make a segment file left by a crashed rollover look like usable active data.
    pub fn open(config: StrataStoreConfig, metrics: StrataStoreMetrics) -> Result<Self> {
        let index =
            StrataIndex::open_path(config.standalone_index_dir(), config.index_cf_prefix())?;
        Self::from_index(config, index, metrics)
    }

    /// Opens a store around an already-created index handle.
    ///
    /// Failure mode avoided: tests and embedders that share an index still get the exact same
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
        ensure_shard_active(&index, STANDALONE_SHARD)?;
        ensure_epoch_initialized(&index, config.starting_epoch)?;
        reconcile_orphan_ingest_segment_files(&config, &index)?;
        recover_unsealed_segments(&config, &index, &metrics)?;
        let active_accounting_delta_log =
            recover_active_accounting_delta_log(&config, &index, &metrics)?;
        let current_epoch = index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)?;
        verify_sealed_segments(&config, &index)?;
        let active_segment_id = choose_active_segment_id(&index)?;
        let active_writer = open_active_writer(&config, active_segment_id)?;
        let durable_offset = active_segment_durable_offset(&index, active_writer.segment_id())?;
        let store_state = index.get_store_state()?.unwrap_or_default();
        let active_segment_state = publish_active_segment_state(
            &config,
            &index,
            STORE_SCOPE,
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
        let (accounting_tx, accounting_rx) = mpsc::sync_channel(1);
        let accounting_lock = Arc::new(Mutex::new(()));
        let gc_claims = Arc::new(GcSourceClaims::default());
        let gc_concurrency = Arc::new(GcConcurrencyController::new(
            GcConcurrencyConfig::from_store_config(&config),
            metrics.clone(),
        ));
        let accounting_worker = AccountingWorker {
            config: config.clone(),
            index: index.clone(),
            interval: config.accounting_interval,
            command_rx: accounting_rx,
            run_lock: Arc::clone(&accounting_lock),
        };
        let accounting_handle = thread::Builder::new()
            .name(format!("strata-accounting-{}", config.namespace))
            .spawn(move || accounting_worker.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let seal_worker = SealWorker {
            config: config.clone(),
            index: index.clone(),
            store_scope: STORE_SCOPE,
            seal_rx,
            accounting_tx: accounting_tx.clone(),
            metrics: metrics.clone(),
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
            active_segment_state,
            durable_offset,
            pending_rollovers: Vec::new(),
            seal_tx: seal_tx.clone(),
            accounting_tx: accounting_tx.clone(),
            write_rx,
            store_scope: STORE_SCOPE,
            reader_cache: Arc::clone(&reader_cache),
            gc_concurrency: Arc::clone(&gc_concurrency),
            metrics: metrics.clone(),
        };
        let writer_handle = thread::Builder::new()
            .name(format!("strata-writer-{}", config.namespace))
            .spawn(move || coordinator.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let mut gc_txs = Vec::with_capacity(config.gc_worker_count);
        let mut gc_handles = Vec::with_capacity(config.gc_worker_count);
        for worker_index in 0..config.gc_worker_count {
            let (gc_tx, gc_rx) = mpsc::channel();
            let gc_worker = GcWorker {
                executor: GcExecutor {
                    config: config.clone(),
                    index: index.clone(),
                    write_tx: write_tx.clone(),
                    accounting_lock: Arc::clone(&accounting_lock),
                    claims: Arc::clone(&gc_claims),
                    gc_concurrency: Arc::clone(&gc_concurrency),
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

        Ok(Self {
            reader_cache,
            config,
            index,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
            seal_tx: Some(seal_tx),
            seal_handle: Some(seal_handle),
            accounting_tx: Some(accounting_tx),
            accounting_handle: Some(accounting_handle),
            gc_txs,
            gc_handles,
            accounting_lock,
            gc_claims,
            gc_concurrency,
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
        self.gc_concurrency.active_limit()
    }

    #[cfg(test)]
    fn shard(&self) -> ShardKey {
        STANDALONE_SHARD
    }

    /// Starts a client-side batch whose operations commit under one store-global LSN allocation.
    ///
    /// Failure mode avoided: callers that need "put blob, then increment epoch" should not issue
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
    /// Failure mode avoided: writers must observe generation changes after drop/re-add. A caller
    /// that cached only `shard_id = 7` would otherwise be unable to tell old generation 0 data from
    /// newly-created generation 1 data.
    pub fn shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.index.get_shard_info(shard_id)?)
    }

    /// Registers a logical shard, or returns its current active generation.
    ///
    /// Failure mode avoided: shard creation is serialized through the writer so two concurrent
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

    /// Marks a logical shard as dropped.
    ///
    /// Failure mode avoided: dropping through the writer drains pending rollovers first. Without
    /// that ordering, a crash could leave a shard marked dropped while the segment containing its
    /// last writes still looks `Open`, which would confuse recovery and GC ownership.
    pub fn drop_shard(&self, shard_id: ShardId) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::DropShard(DropShardRequest {
            shard_id,
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
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
    /// Failure mode avoided: the write queue can hold the payload until the writer thread reaches
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
    /// Failure mode avoided: lifetime changes are metadata-only LSNs so accounting can update
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
    /// Failure mode avoided: a tombstone is an ordered LSN, not an in-place removal. If we deleted
    /// the version row immediately, recovery after a crash could resurrect an older payload because
    /// there would be no durable delete marker to hide it.
    pub fn tombstone(&self, key: &BlobKey) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Tombstone { key: key.clone() }])?;
        Ok(result.first_lsn().unwrap_or(0))
    }

    /// Sends a prepared list of operations to the single writer and waits for the committed result.
    ///
    /// Failure mode avoided: LSNs, segment offsets, and epoch rows must be allocated together by
    /// the owner of the active writer. If callers wrote directly to the index from many threads,
    /// two puts could both publish `next_lsn = 42` while their bytes landed at different offsets.
    fn write_batch(&self, ops: Vec<BatchOp>) -> Result<BatchWriteResult> {
        let (response_tx, response_rx) = mpsc::channel();
        let command = WriteCommand::Batch(BatchWriteRequest { ops, response_tx });
        self.send_write_command(command)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    /// Drops the cached file descriptor for a segment.
    ///
    /// Segment cleanup must call this before unlinking or reusing a segment path. The read path
    /// checks indexed segment state before serving refs, so an old cached descriptor cannot bypass
    /// a published `Deleting` or `Deleted` state.
    pub fn evict_segment_reader(&self, segment_id: SegmentId) {
        self.reader_cache.evict(segment_id);
        self.metrics.record_reader_cache_eviction();
    }

    /// Returns the persisted current epoch.
    ///
    /// Failure mode avoided: reopen must not trust `config.starting_epoch` after genesis. If an
    /// operator changes the config from 100 to 1, this still reports the epoch timeline stored in
    /// the index instead of making new lifetime updates appear to move backward.
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

    /// Appends an epoch-change operation and returns the new epoch with its LSN.
    ///
    /// Failure mode avoided: epoch increments consume LSNs so they are ordered with blob writes.
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
        self.send_write_command(WriteCommand::Sync(response_tx))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
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

    /// Pins the current accounted frontier for a long-running GC job.
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
    fn send_write_command(&self, command: WriteCommand) -> Result<()> {
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
        self.metrics
            .record_write_queue_send(result.is_ok(), started.elapsed());
        self.gc_concurrency
            .observe_write_queue_send(started.elapsed());
        result
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
            let _ = accounting_tx.send(AccountingCommand::Shutdown);
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
    Sync(mpsc::Sender<Result<()>>),
    Shutdown,
}

#[derive(Debug)]
struct AddShardRequest {
    shard_id: ShardId,
    response_tx: mpsc::Sender<Result<ShardKey>>,
}

#[derive(Debug)]
struct BatchWriteRequest {
    ops: Vec<BatchOp>,
    response_tx: mpsc::Sender<Result<BatchWriteResult>>,
}

#[derive(Debug)]
struct DropShardRequest {
    shard_id: ShardId,
    response_tx: mpsc::Sender<Result<()>>,
}

#[derive(Debug)]
pub(crate) struct GcPublishRequest {
    /// Prepared copy bundle whose staging files are ready to become durable segment files.
    copy: PreparedGcCopy,
    /// One-shot response channel back to the caller that requested GC publication.
    response_tx: mpsc::Sender<Result<GcPublishResult>>,
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
    /// Failure mode avoided: callers should not infer "the next operation is previous + 1" after
    /// a failed or empty batch. The writer is the source of truth for what actually committed.
    pub fn op_lsns(&self) -> &[StrataLsn] {
        &self.op_lsns
    }

    /// Epoch outputs in operation order; non-epoch operations have `None`.
    ///
    /// Failure mode avoided: mixed batches need to know which op advanced the epoch. Returning a
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

#[derive(Debug)]
pub struct StrataBatch<'a> {
    store: &'a StrataStore,
    ops: Vec<BatchOp>,
}

impl<'a> StrataBatch<'a> {
    /// Adds a payload write to this batch.
    ///
    /// Failure mode avoided: batching submits all operations as one writer command. That keeps
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

    /// Adds a metadata-only lifetime update to this batch.
    ///
    /// Failure mode avoided: when a lifetime change is batched with other ops, it shares the same
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
    /// Failure mode avoided: tombstones remain ordered relative to any preceding puts in the same
    /// batch. Without this, deleting a key after writing a replacement could race with another put
    /// and hide the wrong version.
    pub fn tombstone(&mut self, key: BlobKey) -> &mut Self {
        self.ops.push(BatchOp::Tombstone { key });
        self
    }

    /// Adds an epoch increment to this batch.
    ///
    /// Failure mode avoided: epoch changes are treated like logical operations. A batch such as
    /// `put A, increment epoch, put B` must replay exactly that order after crash recovery so A and
    /// B do not end up in the same accounting epoch.
    pub fn increment_epoch(&mut self) -> &mut Self {
        self.ops.push(BatchOp::IncrementEpoch);
        self
    }

    /// Submits the accumulated operations to the writer.
    ///
    /// Failure mode avoided: the batch is consumed on write, so callers cannot accidentally submit
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

/// Converts a committed writer batch into active accounting-log deltas.
///
/// Failure mode avoided: accounting replays from this side log without reading the foreground
/// writer's in-memory state. If a put were committed to `blob_versions` but missing here, a crash
/// before sidecar ingestion would leave the GC overlay summary unaware of those live bytes.
fn accounting_deltas_for_prepared_batch(prepared: &PreparedBatch) -> Vec<AccountingDelta> {
    let mut deltas = Vec::with_capacity(prepared.ops.len());
    for op in &prepared.ops {
        match op {
            PreparedBatchOp::Put {
                shard,
                key,
                lsn,
                current_epoch,
                record_ref,
                ..
            } => deltas.push(AccountingDelta::Blob(BlobUpdate::Put {
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
            } => deltas.push(AccountingDelta::Blob(BlobUpdate::SetLifetime {
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
            } => deltas.push(AccountingDelta::Blob(BlobUpdate::Tombstone {
                lsn: *lsn,
                key: key.clone(),
            })),
            PreparedBatchOp::Lifecycle {
                lifecycle_op: BlobLifecycleMergeOp::RollbackFrom { .. },
                ..
            } => {}
            PreparedBatchOp::EpochChange { lsn, epoch } => {
                deltas.push(AccountingDelta::Epoch(AccountingEpochChange {
                    lsn: *lsn,
                    epoch: *epoch,
                }));
            }
        }
    }
    deltas
}

#[derive(Debug)]
struct PendingRollover {
    old_segment_state: SegmentState,
    new_segment_state: SegmentState,
    seal_task: SegmentSealTask,
}

impl PendingRollover {
    /// Adds the old-segment `Sealing` row and the new open-segment row to a write batch.
    ///
    /// Failure mode avoided: rollover metadata must commit atomically with an LSN-bearing write or
    /// shard drop. If the old segment were marked `Sealing` without publishing the new active row,
    /// a crash could reopen with no writable segment.
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
        accounting_tx: mpsc::SyncSender<AccountingCommand>,
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
    /// Failure mode avoided: these actions intentionally do not happen during batch assembly. For
    /// example, nudging accounting before the blob-version batch commits could make accounting
    /// observe an LSN in the delta log whose index entry is not visible yet.
    fn run(self) {
        match self {
            Self::MaybeNudgeAccounting {
                latest_lsn,
                threshold,
                accounting_tx,
            } => {
                if threshold == 0 || latest_lsn % threshold as u64 == 0 {
                    let _ = accounting_tx.try_send(AccountingCommand::Run);
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
    accounting_tx: mpsc::SyncSender<AccountingCommand>,
) -> PostCommitAction {
    PostCommitAction::MaybeNudgeAccounting {
        latest_lsn,
        threshold,
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
/// The price is that a slow fsync stalls the queue. That is an accepted trade: the sync cadence
/// is the throughput knob, not per-op concurrency.
#[derive(Debug)]
struct WriteCoordinator {
    config: StrataStoreConfig,
    index: StrataIndex,
    active_writer: SegmentWriter,
    active_accounting_delta_log: ActiveDeltaLog,
    active_segment_state: SegmentState,
    durable_offset: u64,
    pending_rollovers: Vec<PendingRollover>,
    seal_tx: mpsc::Sender<SealCommand>,
    accounting_tx: mpsc::SyncSender<AccountingCommand>,
    write_rx: mpsc::Receiver<WriteCommand>,
    store_scope: ShardKey,
    reader_cache: Arc<SegmentReaderCache>,
    gc_concurrency: Arc<GcConcurrencyController>,
    metrics: StrataStoreMetrics,
}

impl WriteCoordinator {
    /// Main writer loop: every mutation and sync is serialized through this receiver.
    ///
    /// Failure mode avoided: sync must not race append. If sync ran on a separate thread, it could
    /// publish durable_lsn 10 while a concurrent append for LSN 10 had reserved an offset but not
    /// finished writing its record bytes.
    fn run(mut self) {
        while let Ok(command) = self.write_rx.recv() {
            if matches!(command, WriteCommand::Shutdown) {
                break;
            }
            self.metrics.dequeue_write_command();
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
                WriteCommand::Sync(response_tx) => {
                    let result = self.sync_data();
                    let _ = response_tx.send(result);
                }
                WriteCommand::Shutdown => unreachable!("shutdown is handled before dispatch"),
            }
        }
    }

    fn process_add_shard(&mut self, request: AddShardRequest) {
        let result = self.submit_add_shard(request.shard_id);
        let _ = request.response_tx.send(result);
    }

    /// Creates or reactivates a shard generation through the writer queue.
    ///
    /// Failure mode avoided: drop/re-add must bump generation exactly once. Without this serialized
    /// registry update, one thread could resurrect generation 0 while another has already dropped
    /// it and started generation 1, making old writes visible in the new namespace.
    fn submit_add_shard(&mut self, shard_id: ShardId) -> Result<ShardKey> {
        let info = match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => return Ok(info.key(shard_id)),
            Some(info) if info.state == ShardState::Dropped => {
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
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
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
    /// Failure mode avoided: treating "already dropped" as success makes retries idempotent after
    /// caller timeouts. Treating missing shards as success would hide bugs where a caller thinks it
    /// deleted tenant 42 but that tenant was never registered.
    fn submit_drop_shard(&mut self, shard_id: ShardId) -> Result<()> {
        let Some(info) = self.index.get_shard_info(shard_id)? else {
            return Err(Error::ShardNotFound { shard_id });
        };
        if info.state == ShardState::Dropped {
            return Ok(());
        }
        if info.state != ShardState::Active && info.state != ShardState::Dropping {
            return Err(Error::ShardUnavailable {
                shard_id,
                generation: info.current_generation,
                current_generation: info.current_generation,
                state: info.state,
            });
        }

        let shard = info.key(shard_id);
        self.mark_shard_dropped(shard_id, shard)
    }

    /// Persists the dropped shard state together with any rollover metadata already staged.
    ///
    /// Failure mode avoided: a rollover can be pending when the next command is `drop_shard`
    /// instead of a put. If the drop skipped the pending rollover rows, the old segment would stay
    /// `Open` forever and recovery would append to or rescan the wrong file.
    fn mark_shard_dropped(&mut self, shard_id: ShardId, shard: ShardKey) -> Result<()> {
        let dropped_info = ShardInfo {
            current_generation: shard.generation,
            state: ShardState::Dropped,
        };
        let pending_rollovers = self.take_pending_rollovers();

        let commit_result = (|| {
            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            self.index
                .put_shard_info_batch(&mut batch, shard_id, dropped_info)?;
            batch.write().map_err(strata_index::Error::from)?;
            self.index.flush_wal(true)?;
            Ok(())
        })();

        match commit_result {
            Ok(()) => {
                self.index.set_cached_shard_info(shard_id, dropped_info);
                self.run_rollover_post_commit(pending_rollovers);
                Ok(())
            }
            Err(error) => {
                self.restore_pending_rollovers(pending_rollovers);
                Err(error)
            }
        }
    }

    /// Handles one client batch and records user-visible put metrics.
    ///
    /// Failure mode avoided: metrics are recorded once per submitted put after the writer knows
    /// whether the batch committed. Recording during append would count a write as successful even
    /// if the later index batch failed and recovery had to discard the orphaned bytes.
    fn process_batch(&mut self, request: BatchWriteRequest) {
        let started = Instant::now();
        let put_count = request
            .ops
            .iter()
            .filter(|op| matches!(op, BatchOp::Put { .. }))
            .count();
        match self.submit_batch(request) {
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
    }

    /// Full write transaction for a batch: reserve LSNs, append payload bytes, append accounting
    /// deltas, then commit one index batch.
    ///
    /// Failure mode avoided: bytes may be orphaned if the process dies after append but before
    /// index commit, but recovery can discard bytes with no committed index entry. The reverse
    /// ordering would be worse: an index entry could point at bytes that were never written.
    fn submit_batch(
        &mut self,
        request: BatchWriteRequest,
    ) -> std::result::Result<(BatchWriteResult, Vec<PutMetric>), ()> {
        let response_tx = request.response_tx;
        if request.ops.is_empty() {
            let result = BatchWriteResult::default();
            let _ = response_tx.send(Ok(result.clone()));
            return Ok((result, Vec::new()));
        }

        let mut prepared = match self.prepare_batch(request.ops) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = response_tx.send(Err(error));
                return Err(());
            }
        };

        let mut appended_records = 0_u64;
        let mut appended_bytes = 0_u64;
        let mut put_metrics = Vec::new();
        for op in &mut prepared.ops {
            let PreparedBatchOp::Put {
                shard,
                key,
                payload,
                lsn,
                record_ref,
                record_bytes,
                ..
            } = op
            else {
                continue;
            };

            if let Err(error) = self.ensure_segment_capacity(*record_bytes) {
                self.metrics
                    .record_orphaned_segment_bytes(appended_records, appended_bytes);
                let _ = response_tx.send(Err(error));
                return Err(());
            }

            let outcome =
                match self
                    .active_writer
                    .append_for_shard(&*key, *lsn, *shard, payload.as_ref())
                {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.metrics
                            .record_orphaned_segment_bytes(appended_records, appended_bytes);
                        let _ = response_tx.send(Err(error.into()));
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

        let pending_rollovers = self.take_pending_rollovers();
        let accounting_delta_position = self.active_accounting_delta_log.position();
        if let Err(error) = self.append_accounting_deltas(&prepared) {
            let error = match self
                .active_accounting_delta_log
                .rollback_to(accounting_delta_position)
            {
                Ok(()) => error,
                Err(rollback_error) => rollback_error.into(),
            };
            self.restore_pending_rollovers(pending_rollovers);
            self.metrics
                .record_orphaned_segment_bytes(appended_records, appended_bytes);
            let _ = response_tx.send(Err(error));
            return Err(());
        }
        if let Err(error) = self.commit_write_batch(&pending_rollovers, &prepared) {
            let error = match self
                .active_accounting_delta_log
                .rollback_to(accounting_delta_position)
            {
                Ok(()) => error,
                Err(rollback_error) => rollback_error.into(),
            };
            self.restore_pending_rollovers(pending_rollovers);
            self.metrics
                .record_orphaned_segment_bytes(appended_records, appended_bytes);
            let _ = response_tx.send(Err(error));
            return Err(());
        }
        self.run_rollover_post_commit(pending_rollovers);
        if let Some(last_lsn) = prepared.result.last_lsn() {
            accounting_nudge_action(
                last_lsn,
                self.config.accounting_unaccounted_threshold,
                self.accounting_tx.clone(),
            )
            .run();
        }
        self.metrics.set_active_segment(
            self.active_writer.segment_id(),
            self.active_writer.write_offset(),
            self.durable_offset,
        );
        let result = prepared.result;
        let _ = response_tx.send(Ok(result.clone()));
        Ok((result, put_metrics))
    }

    /// Publishes GC-staged copies as durable segment metadata and `MapRef` operations.
    ///
    /// The caller already holds the accounting run lock before this command reaches the writer.
    /// Keeping the pause outside the writer loop means a long accounting pass can delay the GC
    /// caller without stalling unrelated user writes. This writer-side critical section only does
    /// the ordering-sensitive work: sync previous writes, assign the GC LSN range, append the bulk
    /// accounting delta, and commit the index metadata.
    fn submit_gc_publish(&mut self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        self.sync_data()?;
        let reconciled_accounted_lsn = self.index.get_accounted_lsn()?;
        if gc_plan_has_metadata_action(&copy.plan) {
            if gc_plan_has_copy_action(&copy.plan)
                || !copy.outputs.is_empty()
                || !copy.copied_records.is_empty()
            {
                return Err(Error::GcInvalidPlan(
                    "metadata actions cannot be mixed with copied records",
                ));
            }
            self.apply_gc_metadata_actions(&copy.plan)?;
            return Ok(GcPublishResult {
                reconciled_accounted_lsn,
                output_segments: Vec::new(),
                published_records: Vec::new(),
                skipped_records: Vec::new(),
            });
        }

        let accounting_changes = self
            .index
            .accounting_changes_since(&copy.accounting_snapshot)?;
        let (survivors, skipped_records) =
            split_gc_copied_records(copy.copied_records, &accounting_changes);

        if survivors.is_empty() {
            // Every staged copy became stale before publish. The staging files are still invisible:
            // no segment state has been installed and no blob version points at them. Turning them
            // into durable segments would only create metadata and later GC work for bytes that no
            // surviving `MapRef` can use, so discard the temporary files and report the skipped
            // records to the caller. This also keeps the publish-LSN assignment below simple: once
            // we continue past this point, there is at least one record to map and one LSN to
            // reserve.
            remove_gc_staging_outputs(copy.outputs)?;
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
        let published_records = match assign_gc_publish_lsns(
            self.index.get_next_lsn()?,
            &survivors,
            &output_plan.staged_to_final_segment_id,
        ) {
            Ok(published_records) => published_records,
            Err(error) => {
                return Err(cleanup_uncommitted_gc_outputs(
                    &copy.outputs,
                    &output_plan,
                    error,
                ));
            }
        };
        apply_gc_output_lsn_bounds(&mut output_plan.segment_states, &published_records);
        let live_output_records = live_gc_output_records(&published_records);
        let skipped_output_ranges =
            skipped_gc_output_ranges(&skipped_records, &output_plan.staged_to_final_segment_id);

        let accounting_delta_position = self.active_accounting_delta_log.position();
        let pending_rollovers = self.take_pending_rollovers();
        let commit_result = (|| {
            if let Some(delta) = gc_publish_accounting_delta(&published_records) {
                self.active_accounting_delta_log.append(&delta)?;
            }
            self.active_accounting_delta_log.sync_data()?;
            let active_delta_state = self.active_accounting_delta_log.state();

            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            for state in &output_plan.segment_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
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
                    record.source.shard,
                    record.source.payload_lsn,
                    record.source.from,
                    record.to,
                )?;
                self.index.put_blob_unaccounted_lsn_op_batch(
                    &mut batch,
                    record.publish_lsn,
                    &record.source.key,
                )?;
                self.index.put_segment_ref_event_batch(
                    &mut batch,
                    SegmentRefEventKey {
                        segment_id: record.source.from.segment_id,
                        lsn: record.publish_lsn,
                        offset: record.source.from.offset,
                    },
                    &SegmentRefEvent::Retired,
                )?;
                self.index.merge_segment_gc_overlay_batch(
                    &mut batch,
                    record.source.from.segment_id,
                    vec![SegmentGcOverlayMergeOp::RetireBatch {
                        ranges: vec![SegmentGcRecordRange::from(record.source.from)],
                    }],
                )?;
            }
            for (segment_id, records) in live_output_records {
                self.index.merge_segment_gc_overlay_batch(
                    &mut batch,
                    segment_id,
                    vec![SegmentGcOverlayMergeOp::AddLiveBatch { records }],
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
            let durable_lsn = published_records
                .last()
                .map(|record| record.publish_lsn)
                .expect("survivors are non-empty");
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            self.index
                .put_accounting_active_delta_log_state_batch(&mut batch, active_delta_state)?;
            self.index.put_durable_lsn_batch(&mut batch, durable_lsn)?;
            batch
                .write()
                .map_err(strata_index::Error::from)
                .map_err(Error::from)?;
            self.index
                .flush_wal(true)
                .map_err(|error| GcPublishCommitError::AfterIndexBatch(error.into()))?;
            Ok::<(), GcPublishCommitError>(())
        })();

        match commit_result {
            Ok(()) => {
                self.run_rollover_post_commit(pending_rollovers);
                let durable_lsn = published_records
                    .last()
                    .map(|record| record.publish_lsn)
                    .expect("survivors are non-empty");
                self.index.set_blob_compact_safe_lsn(durable_lsn);
                self.metrics.set_durable_lsn(durable_lsn);
                self.metrics.set_next_lsn(durable_lsn.saturating_add(1));
                let _ = self.accounting_tx.try_send(AccountingCommand::Run);
                remove_unused_gc_staging_outputs(copy.outputs, &output_plan.used_staged_ids)?;
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
                let error = match self
                    .active_accounting_delta_log
                    .rollback_to(accounting_delta_position)
                {
                    Ok(()) => error,
                    Err(rollback_error) => rollback_error.into(),
                };
                self.restore_pending_rollovers(pending_rollovers);
                Err(cleanup_uncommitted_gc_outputs(
                    &copy.outputs,
                    &output_plan,
                    error,
                ))
            }
            Err(GcPublishCommitError::AfterIndexBatch(error)) => {
                self.run_rollover_post_commit(pending_rollovers);
                Err(error)
            }
        }
    }

    fn apply_gc_metadata_actions(&self, plan: &GcPlan) -> Result<()> {
        for action in &plan.actions {
            match action {
                GcAction::DeleteSegment { segment_id } => {
                    self.delete_empty_gc_segment(*segment_id)?;
                }
                GcAction::ReclassifySegment {
                    segment_id,
                    placement_class,
                } => {
                    self.reclassify_gc_segment(*segment_id, *placement_class)?;
                }
                GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {
                    return Err(Error::GcInvalidPlan(
                        "copy actions must use the copy publish path",
                    ));
                }
            }
        }
        Ok(())
    }

    fn delete_empty_gc_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        if state.state == SegmentFileState::Deleted {
            self.reader_cache.evict(segment_id);
            self.metrics.record_reader_cache_eviction();
            return unlink_gc_segment_file(&self.config, &state);
        }
        if state.state != SegmentFileState::Sealed && state.state != SegmentFileState::Deleting {
            return Err(Error::GcSourceSegmentNotSealed {
                segment_id,
                state: state.state,
            });
        }

        let summary = self
            .index
            .get_segment_gc_overlay(segment_id)?
            .unwrap_or_default()
            .summary;
        if summary.live_ref_count != 0 {
            return Err(Error::GcSourceSegmentNotEmpty {
                segment_id,
                live_ref_count: summary.live_ref_count,
            });
        }

        state.state = SegmentFileState::Deleted;
        let mut batch = self.index.batch();
        self.index.put_segment_state_batch(&mut batch, &state)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        self.reader_cache.evict(segment_id);
        self.metrics.record_reader_cache_eviction();
        unlink_gc_segment_file(&self.config, &state)
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
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        Ok(())
    }

    /// Converts sealed GC staging files into final segment identities and on-disk locations.
    ///
    /// Only staging files that contain surviving copied records are renamed into the segment
    /// directory. The returned map is the translation table from temporary staged segment ids to
    /// final durable segment ids, which later helpers use to build `MapRef` destinations.
    fn plan_gc_output_segments(
        &self,
        outputs: &[GcStagedOutputSegment],
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
        let mut next_segment_id = self.next_gc_output_segment_id()?;
        let mut staged_to_final_segment_id = BTreeMap::new();
        let mut published_outputs = Vec::new();
        let mut segment_states = Vec::new();

        for staged_segment_id in &used_staged_ids {
            let output = match outputs_by_staged_id.remove(staged_segment_id) {
                Some(output) => output,
                None => {
                    return Err(cleanup_gc_output_paths(
                        &published_outputs,
                        Error::GcMissingStagedOutput {
                            staged_segment_id: *staged_segment_id,
                        },
                    ));
                }
            };
            let final_segment_id = next_segment_id;
            next_segment_id = match next_segment_id.checked_add(1) {
                Some(next) => next,
                None => {
                    return Err(cleanup_gc_output_paths(
                        &published_outputs,
                        strata_segment::Error::RangeOverflow.into(),
                    ));
                }
            };
            let final_path = segment_path(&self.config, final_segment_id);
            if final_path.exists() {
                return Err(cleanup_gc_output_paths(
                    &published_outputs,
                    Error::GcOutputSegmentExists {
                        segment_id: final_segment_id,
                        path: final_path,
                    },
                ));
            }
            if let Err(error) = fs::rename(&output.path, &final_path).map_err(|source| Error::Io {
                path: final_path.clone(),
                source,
            }) {
                return Err(cleanup_gc_output_paths(&published_outputs, error));
            }
            let published_output = GcPublishedOutputSegment {
                staged_segment_id: output.staged_segment_id,
                segment_id: final_segment_id,
                path: final_path.clone(),
                placement_class: output.placement_class,
                sealed_len: output.sealed_len,
            };
            published_outputs.push(published_output);
            if let Err(error) = sync_parent_dir(&final_path) {
                return Err(cleanup_gc_output_paths(&published_outputs, error));
            }
            if output.path.parent() != final_path.parent() {
                if let Err(error) = sync_parent_dir(&output.path) {
                    return Err(cleanup_gc_output_paths(&published_outputs, error));
                }
            }

            let state = SegmentState {
                shard: self.store_scope,
                segment_id: final_segment_id,
                volume_id: 0,
                path: relative_segment_path(&self.config, final_path.clone()),
                placement_class: output.placement_class,
                state: SegmentFileState::Sealed,
                write_offset: output.sealed_len,
                durable_offset: output.sealed_len,
                min_lsn: None,
                max_lsn: None,
                sealed_len: Some(output.sealed_len),
                sealed_sha256: Some(output.sealed_sha256),
            };
            staged_to_final_segment_id.insert(output.staged_segment_id, final_segment_id);
            segment_states.push(state);
        }

        Ok(PlannedGcOutputSegments {
            staged_to_final_segment_id,
            used_staged_ids,
            published_outputs,
            segment_states,
        })
    }

    /// Chooses the first segment id that a GC output file may use.
    ///
    /// This must consider committed segment rows, the current active writer, and pending rollover
    /// rows that have not yet been applied to RocksDB. Otherwise GC could rename a staging file on
    /// top of a segment id that a rollover is about to publish.
    fn next_gc_output_segment_id(&self) -> Result<SegmentId> {
        let mut next = self
            .index
            .iter_segment_states()?
            .into_iter()
            .map(|(segment_id, _)| segment_id)
            .chain(std::iter::once(self.active_writer.segment_id()))
            .max()
            .and_then(|segment_id| segment_id.checked_add(1))
            .unwrap_or(FIRST_SEGMENT_ID);
        for rollover in &self.pending_rollovers {
            next = next.max(rollover.old_segment_state.segment_id.saturating_add(1));
            next = next.max(rollover.new_segment_state.segment_id.saturating_add(1));
        }
        Ok(next)
    }

    /// Resolves a client batch into concrete LSNs and per-op metadata before any bytes are written.
    ///
    /// Failure mode avoided: every operation in a batch reserves a contiguous LSN range. If LSNs
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
                    let current_epoch = current_epoch.ok_or(Error::EpochNotInitialized)?;
                    let shard = self.openable_shard_key(shard_id)?;
                    let record_bytes = encoded_record_len(&key, payload.len())
                        .map_err(strata_segment::Error::from)?;
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

    /// Temporarily removes staged rollover metadata so it can be included in the next durable
    /// index batch exactly once.
    ///
    /// Failure mode avoided: if a batch commit fails after we started adding rollover rows, the
    /// pending rollover must not be lost from memory. Otherwise the writer would continue on the
    /// new segment while the old one never gets sealed.
    fn take_pending_rollovers(&mut self) -> Vec<PendingRollover> {
        std::mem::take(&mut self.pending_rollovers)
    }

    /// Restores rollover metadata when a batch that tried to publish it fails.
    ///
    /// Failure mode avoided: a transient RocksDB write error should not silently drop the sealer's
    /// work item. Restoring lets the next successful batch publish the same old/new segment state.
    fn restore_pending_rollovers(&mut self, pending_rollovers: Vec<PendingRollover>) {
        self.pending_rollovers = pending_rollovers;
    }

    /// Commits the index side of a prepared batch.
    ///
    /// Failure mode avoided: blob versions, unaccounted LSN rows, epoch changes, active segment
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
                    let entry = BlobEntry {
                        record_ref: Some(record_ref),
                        lsn: *lsn,
                        generation: *lsn,
                        state: BlobState::Live,
                    };
                    self.index.apply_blob_version_merge_op_batch(
                        &mut batch,
                        key,
                        VersionMergeOp::Append(VersionOp {
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

    /// Appends accounting deltas for the prepared batch before the index batch commits.
    ///
    /// If the later index commit fails, the caller rolls this log back to its previous
    /// position so uncommitted deltas do not become phantom accounting work.
    fn append_accounting_deltas(&mut self, prepared: &PreparedBatch) -> Result<()> {
        let deltas = accounting_deltas_for_prepared_batch(prepared);
        self.active_accounting_delta_log.append_all(&deltas)?;
        Ok(())
    }

    /// Returns the active generation key for a shard that can accept writes.
    ///
    /// Failure mode avoided: a stale writer that only knows `shard_id` must not write into a shard
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
    fn rollover_active_segment(&mut self) -> Result<()> {
        self.wait_for_seal_backlog_capacity()?;
        let old_segment_id = self.active_writer.segment_id();
        let old_write_offset = self.active_writer.write_offset();
        let new_segment_id = old_segment_id
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        let new_path = segment_path(&self.config, new_segment_id);
        if new_path.exists() && self.index.get_segment_state(new_segment_id)?.is_none() {
            fs::remove_file(&new_path).map_err(|source| Error::Io {
                path: new_path.clone(),
                source,
            })?;
        }

        let new_writer = SegmentWriter::create(
            &new_path,
            new_segment_id,
            PlacementClass::Ingest,
            self.config.segment_max_bytes,
        )?;
        let new_state = active_segment_state(&self.config, self.store_scope, &new_writer, 0);
        let mut old_state = self.active_segment_state.clone();
        old_state.write_offset = old_write_offset;
        old_state.durable_offset = self.durable_offset;
        old_state.state = SegmentFileState::Sealing;

        self.pending_rollovers.push(PendingRollover {
            old_segment_state: old_state,
            new_segment_state: new_state.clone(),
            seal_task: SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: old_write_offset,
            },
        });
        self.active_writer = new_writer;
        self.active_segment_state = new_state;
        self.durable_offset = 0;
        self.metrics.set_active_segment(
            self.active_writer.segment_id(),
            self.active_writer.write_offset(),
            self.durable_offset,
        );
        Ok(())
    }

    /// Ensures the active segment can fit the next record, rolling over as many times as needed.
    ///
    /// Failure mode avoided: records are never split across segment files. If a too-large record
    /// were partially appended before discovering the limit, recovery would only see a torn record
    /// and would have to roll back unrelated later LSNs.
    fn ensure_segment_capacity(&mut self, record_len: u64) -> Result<()> {
        loop {
            let attempted_size = self
                .active_writer
                .write_offset()
                .checked_add(record_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            if attempted_size <= self.config.segment_max_bytes {
                return Ok(());
            }
            if self.active_writer.write_offset() == 0 {
                return Err(strata_segment::Error::SegmentFull {
                    max_size: self.config.segment_max_bytes,
                    attempted_size,
                }
                .into());
            }
            self.rollover_active_segment()?;
        }
    }

    /// Backpressure: if the sealer can't keep up, writes eventually block here instead of
    /// accumulating unbounded unsealed segments. Unsealed segments are the expensive thing at
    /// restart (each one gets a full recovery scan), so the cap directly bounds worst-case
    /// recovery time. A `SealFailed` segment turns the stall into a hard error — sealing failures
    /// don't self-heal, and silently writing forever in front of one would just grow the blast
    /// radius.
    fn wait_for_seal_backlog_capacity(&self) -> Result<()> {
        let started = Instant::now();
        let mut waiting = false;
        loop {
            if let Some(segment_id) = first_seal_failed_segment(&self.index)? {
                if waiting {
                    self.metrics
                        .finish_seal_backpressure_wait(started.elapsed());
                    self.gc_concurrency.set_seal_backpressure(false);
                }
                return Err(Error::SealFailed { segment_id });
            }
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
    fn sync_data(&mut self) -> Result<()> {
        let started = Instant::now();
        let previous_durable_offset = self.durable_offset;
        let durable_offset = self.active_writer.write_offset();
        if let Err(error) = self.active_writer.sync_data() {
            self.metrics.record_sync(Err(()), started.elapsed());
            return Err(error.into());
        }
        if let Err(error) = self.active_accounting_delta_log.sync_data() {
            self.metrics.record_sync(Err(()), started.elapsed());
            return Err(error.into());
        }

        let durable_lsn = {
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
                state.sealed_len = existing.sealed_len;
                state.sealed_sha256 = existing.sealed_sha256;
            }
            state.write_offset = self.active_writer.write_offset();
            state.durable_offset = durable_offset;
            let mut batch = self.index.batch();
            self.index.put_segment_state_batch(&mut batch, &state)?;
            let current_durable_lsn = self.index.get_durable_lsn()?;
            let mut active_delta_state = self.active_accounting_delta_log.state();
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
            if let Err(error) = batch.write().map_err(strata_index::Error::from) {
                self.metrics.record_sync(Err(()), started.elapsed());
                return Err(error.into());
            }
            self.active_segment_state = state;
            durable_lsn
        };
        if let Err(error) = self.index.flush_wal(true) {
            self.metrics.record_sync(Err(()), started.elapsed());
            return Err(error.into());
        }
        self.index.set_blob_compact_safe_lsn(durable_lsn);
        self.durable_offset = durable_offset;
        self.active_segment_state.durable_offset = durable_offset;
        self.metrics.set_active_segment(
            self.active_writer.segment_id(),
            self.active_writer.write_offset(),
            self.durable_offset,
        );
        self.metrics.set_durable_lsn(durable_lsn);
        self.metrics.record_sync(
            Ok(durable_offset.saturating_sub(previous_durable_offset)),
            started.elapsed(),
        );
        self.gc_concurrency.observe_sync(
            started.elapsed(),
            durable_offset.saturating_sub(previous_durable_offset),
        );
        self.nudge_accounting();
        Ok(())
    }

    /// `try_send` into a bounded(1) channel: if a run is already queued, the nudge coalesces into
    /// it and the drop is intentional. Accounting must never apply backpressure to the writer.
    fn nudge_accounting(&self) {
        let _ = self.accounting_tx.try_send(AccountingCommand::Run);
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
    AfterIndexBatch(Error),
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

/// Groups destination live-record overlay additions by output segment.
///
/// GC publish accounts destination bytes immediately. Sidecar accounting later folds the matching
/// `MapRef`, but that fold only retires the source; it must not allocate the destination again.
fn live_gc_output_records(
    published_records: &[GcPublishedRecord],
) -> BTreeMap<SegmentId, Vec<SegmentGcLiveRecord>> {
    let mut records = BTreeMap::<SegmentId, Vec<SegmentGcLiveRecord>>::new();
    for record in published_records {
        records
            .entry(record.to.segment_id)
            .or_default()
            .push(SegmentGcLiveRecord {
                range: SegmentGcRecordRange::from(record.to),
                lifecycle: record.source.lifecycle,
            });
    }
    records
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

fn gc_plan_has_metadata_action(plan: &GcPlan) -> bool {
    plan.actions.iter().any(|action| {
        matches!(
            action,
            GcAction::DeleteSegment { .. } | GcAction::ReclassifySegment { .. }
        )
    })
}

fn gc_plan_has_copy_action(plan: &GcPlan) -> bool {
    plan.actions.iter().any(|action| {
        matches!(
            action,
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. }
        )
    })
}

/// Builds the bulk active-log delta that makes GC relocations visible to blob accounting.
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

/// Deletes staging files that were not needed because all their copied records were skipped.
///
/// Surviving staging files have already been renamed into final segment paths. This helper cleans
/// up the rest after the publish batch commits so temporary files do not accumulate.
fn remove_unused_gc_staging_outputs(
    outputs: Vec<GcStagedOutputSegment>,
    used_staged_ids: &BTreeSet<SegmentId>,
) -> Result<()> {
    remove_unused_gc_staging_output_files(&outputs, used_staged_ids)
}

fn remove_unused_gc_staging_output_files(
    outputs: &[GcStagedOutputSegment],
    used_staged_ids: &BTreeSet<SegmentId>,
) -> Result<()> {
    for output in outputs {
        if !used_staged_ids.contains(&output.staged_segment_id) {
            remove_gc_staging_output(&output.path)?;
        }
    }
    Ok(())
}

fn cleanup_uncommitted_gc_outputs(
    outputs: &[GcStagedOutputSegment],
    output_plan: &PlannedGcOutputSegments,
    error: Error,
) -> Error {
    let cleanup_result = (|| {
        remove_gc_published_output_files(&output_plan.published_outputs)?;
        remove_unused_gc_staging_output_files(outputs, &output_plan.used_staged_ids)?;
        Ok(())
    })();
    match cleanup_result {
        Ok(()) => error,
        Err(cleanup_error) => cleanup_error,
    }
}

fn cleanup_gc_output_paths(outputs: &[GcPublishedOutputSegment], error: Error) -> Error {
    match remove_gc_published_output_files(outputs) {
        Ok(()) => error,
        Err(cleanup_error) => cleanup_error,
    }
}

fn remove_gc_published_output_files(outputs: &[GcPublishedOutputSegment]) -> Result<()> {
    for output in outputs {
        remove_gc_output_file(&output.path)?;
    }
    Ok(())
}

/// Deletes every staging file in a prepared copy.
///
/// This is used when reconciliation finds no survivors at all, so no output segment state was
/// installed and every staged file is still temporary and invisible.
fn remove_gc_staging_outputs(outputs: Vec<GcStagedOutputSegment>) -> Result<()> {
    for output in outputs {
        remove_gc_staging_output(&output.path)?;
    }
    Ok(())
}

/// Removes one staging file and syncs its parent directory.
///
/// Missing files are treated as already-cleaned-up. That keeps publish retry/error cleanup
/// idempotent without hiding real I/O errors from the caller.
fn remove_gc_staging_output(path: &Path) -> Result<()> {
    remove_gc_output_file(path)
}

fn remove_gc_output_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent_dir(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn unlink_gc_segment_file(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    let path = if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    };
    match fs::remove_file(&path) {
        Ok(()) => sync_parent_dir(&path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io { path, source }),
    }
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = fs::File::open(parent).map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })
}

/// Resolves a key to its readable payload, or None for missing/tombstoned blobs.
///
/// A live head without a payload ref should not be produced by new writes. If recovery or legacy
/// state leaves such a head behind, there are no bytes to return, so None is the honest answer.
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
/// registry exactly. The generation check is what makes shard drop and re-add safe: a stale handle
/// from before a drop carries the old generation and gets rejected here, instead of silently
/// writing into a namespace whose metadata was already torn down.
fn ensure_shard_active(index: &StrataIndex, shard: ShardKey) -> Result<()> {
    match index.get_shard_info(shard.id)? {
        Some(info) if info.current_generation == shard.generation && info.is_active() => Ok(()),
        Some(info) => Err(Error::ShardUnavailable {
            shard_id: shard.id,
            generation: shard.generation,
            current_generation: info.current_generation,
            state: info.state,
        }),
        None => {
            index.put_shard_info(shard.id, ShardInfo::active(shard.generation))?;
            Ok(())
        }
    }
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
/// Even a put-only batch has the same shape: a RocksDB commit failure observed by the caller would
/// become a visible write after restart. Only RocksDB's batch tells us which LSNs committed; segment
/// recovery can promote/truncate bytes for already-indexed operations, but it must not discover new
/// committed LSNs from payload bytes alone.
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
    rollback_lost_operations(index, metrics)?;
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
) -> Result<ActiveDeltaLog> {
    let mut log = open_active_accounting_delta_log(config, index)?;

    // First trim the easy mismatch: deltas for operations that never committed to RocksDB.
    let committed_lsn = index.get_next_lsn()?.saturating_sub(1);
    log.truncate_after_lsn(committed_lsn)?;

    // Then handle the opposite mismatch. If the log is shorter than RocksDB's committed prefix,
    // keep the contiguous-prefix invariant by rolling back store metadata that accounting could not
    // replay. `rollback_operations_from` also rewinds `next_lsn`, so recompute `committed_lsn`.
    let delta_log_lsn = log.max_lsn().unwrap_or_default();
    if delta_log_lsn < committed_lsn {
        rollback_operations_from(index, metrics, delta_log_lsn.saturating_add(1))?;
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
) -> Result<ActiveDeltaLog> {
    let mut durable_state =
        index
            .get_accounting_active_delta_log_state()?
            .unwrap_or(ActiveDeltaLogState {
                durable_offset: 0,
                durable_lsn: index.get_durable_lsn()?,
            });

    durable_state.durable_lsn = durable_state.durable_lsn.max(index.get_durable_lsn()?);
    Ok(ActiveDeltaLog::open(
        config.accounting_index_dir(),
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

/// Publishes the post-scan segment state and rebuilds its LSN bounds from scratch.
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
        STORE_SCOPE,
        segment_id,
        prefix.recovered_write_offset,
        prefix.durable_offset,
    );
    if let Some(existing) = prefix.existing_state {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.state = existing.state;
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
        let ops = index.blob_version_ops_at_lsn(&version_key.key, version_key.lsn)?;
        let Some(entry) = ops
            .into_iter()
            .map(|op| op.entry)
            .find(|entry| entry.record_ref == Some(record.record_ref))
        else {
            continue;
        };
        recovered_record_count = recovered_record_count.saturating_add(1);

        state.min_lsn = Some(
            state
                .min_lsn
                .map_or(entry.lsn, |first| first.min(entry.lsn)),
        );
        state.max_lsn = Some(state.max_lsn.map_or(entry.lsn, |last| last.max(entry.lsn)));
    }

    index.put_segment_state_batch(&mut batch, &state)?;

    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
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
    let mut state = active_segment_state_from_path(config, STORE_SCOPE, segment_id, 0, 0);
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
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;

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
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
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
fn rollback_lost_operations(index: &StrataIndex, metrics: &StrataStoreMetrics) -> Result<()> {
    let durable_lsn = index.get_durable_lsn()?;
    let states = index.iter_segment_states()?;
    let mut rollback_from = None;
    for (lsn, key) in index.iter_unaccounted_lsn_ops()? {
        if lsn <= durable_lsn || unaccounted_operation_survived(index, lsn, &key, &states)? {
            continue;
        }
        rollback_from = Some(rollback_from.map_or(lsn, |current: StrataLsn| current.min(lsn)));
    }

    let Some(rollback_from) = rollback_from else {
        return Ok(());
    };

    rollback_operations_from(index, metrics, rollback_from)
}

/// Erases every unaccounted blob and epoch operation from `rollback_from` onward and rewinds the
/// store frontiers to that LSN.
///
/// Failure mode avoided: the cleanup must remove both blob-version merge ops and epoch rows. If
/// rollback removed only payload rows but left an epoch change at LSN 51, the next write reusing
/// LSN 51 would inherit an impossible epoch timeline.
fn rollback_operations_from(
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
    let rollback_ops = hidden_version_count.saturating_add(hidden_epoch_changes.len()) as u64;

    let previous_lsn = rollback_from.saturating_sub(1);
    let current_epoch = index
        .latest_epoch_at_lsn(previous_lsn)?
        .map(|(_, epoch)| epoch)
        .ok_or(Error::EpochNotInitialized)?;
    index.put_current_epoch_batch(&mut batch, current_epoch)?;
    index.put_next_lsn_batch(&mut batch, rollback_from)?;
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    metrics.set_next_lsn(rollback_from);
    metrics.set_current_epoch(current_epoch);
    metrics.record_rollback(rollback_from, rollback_ops);
    Ok(())
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
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    index.set_blob_compact_safe_lsn(durable_lsn);
    metrics.set_durable_lsn(durable_lsn);
    Ok(())
}

/// Did this op's effects survive the crash? Metadata-only ops (tombstones, extensions — no
/// record_ref) survive iff their index entry exists, since the entry *is* the op. Payload ops
/// additionally need their bytes inside the segment's recovered extent. This checks
/// `write_offset`, not `durable_offset`, because it runs after the recovery scan truncated files
/// to their validated prefix — at this moment write_offset means "bytes verified present", which
/// is exactly the survival question.
fn unaccounted_operation_survived(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let (ops, lifecycle_ops) = index.blob_ops_at_lsn(key, lsn)?;
    if ops.is_empty() {
        return Ok(!lifecycle_ops.is_empty());
    };
    for op in ops {
        let Some(record_ref) = op.entry.record_ref else {
            continue;
        };
        let Some(record_end_offset) = record_ref.end_offset() else {
            return Err(strata_segment::Error::RangeOverflow.into());
        };
        let survived = states
            .iter()
            .find(|(candidate, _)| *candidate == record_ref.segment_id)
            .is_some_and(|(_, state)| {
                !matches!(
                    state.state,
                    SegmentFileState::SealFailed
                        | SegmentFileState::Deleting
                        | SegmentFileState::Deleted
                ) && state.write_offset >= record_end_offset
            });
        if !survived {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Makes the active segment visible in the index at open time, before any write happens. This is
/// what keeps a brand-new (or just-recovered) segment from looking like an orphan to the next
/// crash recovery. GC overlay state is populated lazily as accounting materializes refs.
fn publish_active_segment_state(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    store_scope: ShardKey,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> Result<SegmentState> {
    let existing = index.get_segment_state(active_writer.segment_id())?;
    let state = active_segment_state_with_lsn(
        config,
        store_scope,
        active_writer,
        durable_offset,
        existing.as_ref(),
        None,
    );
    index.put_segment_state(&state)?;
    Ok(state)
}

/// Builds the normal open-segment state row for the current writer.
///
/// Failure mode avoided: all open segment rows should use the same relative path and store scope.
/// Hand-building this in multiple places risks one path being absolute, so a later move of the
/// store root would make that segment unreadable while others still resolve correctly.
fn active_segment_state(
    config: &StrataStoreConfig,
    store_scope: ShardKey,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> SegmentState {
    active_segment_state_with_lsn(
        config,
        store_scope,
        active_writer,
        durable_offset,
        None,
        None,
    )
}

/// Builds the segment-state row for the active writer. Fields the writer doesn't own
/// (volume, placement class, LSN bounds) are carried over from the existing row so a routine
/// state update can't clobber what background reorganization or recovery set. min/max LSN are
/// maintained per segment so the durable-frontier walk and GC can reason about which LSNs a
/// segment covers without scanning it.
fn active_segment_state_with_lsn(
    config: &StrataStoreConfig,
    store_scope: ShardKey,
    active_writer: &SegmentWriter,
    durable_offset: u64,
    existing: Option<&SegmentState>,
    appended_lsn: Option<StrataLsn>,
) -> SegmentState {
    let mut state = active_segment_state_from_path(
        config,
        store_scope,
        active_writer.segment_id(),
        active_writer.write_offset(),
        durable_offset,
    );
    if let Some(existing) = existing {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.min_lsn = existing.min_lsn;
        state.max_lsn = existing.max_lsn;
    }
    if let Some(lsn) = appended_lsn {
        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }
    state
}

/// Creates a fresh segment-state row from an on-disk path.
///
/// Failure mode avoided: new rows start with no sealed checksum or LSN bounds. Accidentally
/// carrying those fields from a previous segment id would make recovery think an open segment is
/// sealed or make GC believe it contains LSNs it never wrote.
fn active_segment_state_from_path(
    config: &StrataStoreConfig,
    store_scope: ShardKey,
    segment_id: SegmentId,
    write_offset: u64,
    durable_offset: u64,
) -> SegmentState {
    let path = segment_path(config, segment_id);
    SegmentState {
        shard: store_scope,
        segment_id,
        volume_id: 0,
        path: relative_segment_path(config, path),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset,
        durable_offset,
        min_lsn: None,
        max_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

/// Rejects configs that would break the store's ordering or worker assumptions.
///
/// Failure mode avoided: some invalid values do not fail fast by themselves. For example,
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
    Ok(())
}

/// Resume the highest open ingest segment if there is one; otherwise allocate one past the
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

/// Returns unsealed ingest segments in write order.
///
/// Failure mode avoided: recovery must scan low segment ids first. If segment 3 is recovered before
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

/// Finds the first ingest segment whose sealing failed.
///
/// Failure mode avoided: a failed seal is a permanent blockage until repaired. If rollover
/// backpressure ignored it, the writer could keep producing new unsealed segments while the old
/// failed segment never becomes immutable or GC-safe.
fn first_seal_failed_segment(index: &StrataIndex) -> Result<Option<SegmentId>> {
    Ok(index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest
                && state.state == SegmentFileState::SealFailed
        })
        .map(|(segment_id, _)| segment_id)
        .min())
}

fn is_unsealed_state(state: SegmentFileState) -> bool {
    matches!(
        state,
        SegmentFileState::Open | SegmentFileState::Sealing | SegmentFileState::SealFailed
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        ops::Deref,
        path::Path,
        sync::{Once, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use prometheus::Registry;
    use strata_accounting::{
        AccountingIndex, AccountingIndexConfig, ActiveDeltaLogReadCursor, ActiveDeltaLogState,
    };
    use strata_core::{
        BlobLifecycle, EpochBucket, FIXED_RECORD_HEADER_LEN, SegmentGcLifetimeRange,
        SegmentGcRecordRange, SegmentRefEvent, SegmentRefEventKey, StrataStoreState,
    };
    use strata_gc::{
        DestinationClass, GcAction, GcCopyRecord, GcPlan, GcPlanner, GcPlannerConfig, GcScenario,
    };
    use tempfile::tempdir;
    use typed_store::{
        DBMetrics,
        rocks::{MetricConf, open_cf},
    };

    use super::*;

    static INIT_TYPED_STORE_METRICS: Once = Once::new();
    const TEST_KEY_LEN: u64 = 6;
    const TEST_PAYLOAD_LEN: u64 = 9;
    const TEST_RECORD_LEN: u64 = FIXED_RECORD_HEADER_LEN as u64 + TEST_KEY_LEN + TEST_PAYLOAD_LEN;
    const TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD: u64 = TEST_RECORD_LEN * 2 - 1;

    fn gc_range(record_ref: RecordRef) -> SegmentGcRecordRange {
        SegmentGcRecordRange::from(record_ref)
    }

    fn gc_ranges_contain(ranges: &[SegmentGcRecordRange], record_ref: RecordRef) -> bool {
        let range = gc_range(record_ref);
        ranges.iter().any(|candidate| {
            candidate.offset <= range.offset
                && candidate.offset.saturating_add(candidate.len)
                    >= range.offset.saturating_add(range.len)
        })
    }

    fn gc_staged_record(source_offset: u64, staged_offset: u64) -> GcStagedCopiedRecord {
        GcStagedCopiedRecord {
            source: GcCopyRecord {
                key: BlobKey::new(format!("blob-{source_offset}").into_bytes()).unwrap(),
                shard: STANDALONE_SHARD,
                payload_lsn: source_offset,
                from: RecordRef {
                    segment_id: 7,
                    offset: source_offset,
                    len: 8,
                },
                lifecycle: None,
                destination_class: DestinationClass::Spillover,
            },
            staged: RecordRef {
                segment_id: 1,
                offset: staged_offset,
                len: 8,
            },
        }
    }

    #[test]
    fn gc_publish_reconciliation_keeps_lifecycle_only_source_touches() {
        let lifecycle_touched = gc_staged_record(10, 0);
        let retired = gc_staged_record(30, 8);
        let accounting_changes = vec![
            AccountingRefEvent {
                key: SegmentRefEventKey {
                    segment_id: 7,
                    lsn: 50,
                    offset: lifecycle_touched.source.from.offset,
                },
                event: SegmentRefEvent::LifecycleChanged { lifecycle: None },
            },
            AccountingRefEvent {
                key: SegmentRefEventKey {
                    segment_id: 7,
                    lsn: 51,
                    offset: retired.source.from.offset,
                },
                event: SegmentRefEvent::Retired,
            },
        ];

        let (survivors, skipped) = split_gc_copied_records(
            vec![lifecycle_touched.clone(), retired.clone()],
            &accounting_changes,
        );

        assert_eq!(survivors, vec![lifecycle_touched]);
        assert_eq!(
            skipped,
            vec![GcSkippedCopiedRecord {
                record: retired,
                kind: GcSkippedCopiedRecordKind::Retired,
            }]
        );
    }

    fn segment_summary(
        index: &StrataIndex,
        segment_id: SegmentId,
    ) -> strata_core::SegmentGcSummary {
        index
            .get_segment_gc_overlay(segment_id)
            .unwrap()
            .unwrap_or_default()
            .summary
    }

    fn active_delta_log_state(index: &StrataIndex) -> ActiveDeltaLogState {
        index
            .get_accounting_active_delta_log_state()
            .unwrap()
            .unwrap()
    }

    fn active_delta_log_read_cursor(index: &StrataIndex) -> ActiveDeltaLogReadCursor {
        index
            .get_accounting_active_delta_log_consumed_cursor()
            .unwrap()
            .unwrap()
    }

    fn open_accounting_sidecar(store: &StrataStore) -> AccountingIndex {
        let manifest = store.index().get_accounting_index_manifest().unwrap();
        AccountingIndex::open_with_manifest(
            AccountingIndexConfig::new(
                store.config().accounting_index_dir(),
                store.config().accounting_sidecar_partition_count(),
            ),
            manifest,
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct StandaloneStore {
        store: StrataStore,
    }

    impl Deref for StandaloneStore {
        type Target = StrataStore;

        fn deref(&self) -> &Self::Target {
            &self.store
        }
    }

    impl StandaloneStore {
        fn put(&self, key: &BlobKey, payload: &[u8]) -> Result<StrataLsn> {
            self.store.put(STANDALONE_SHARD.id, key, payload)
        }

        fn tombstone(&self, key: &BlobKey) -> Result<StrataLsn> {
            self.store.tombstone(key)
        }

        fn extend(&self, key: &BlobKey, new_logical_end_epoch: Epoch) -> Result<Option<StrataLsn>> {
            self.store
                .set_blob_lifetime(key, new_logical_end_epoch)
                .map(Some)
        }

        fn increment_epoch(&self) -> Result<(Epoch, StrataLsn)> {
            self.store.increment_epoch()
        }
    }

    fn try_open_standalone_store(
        config: StrataStoreConfig,
        metrics: StrataStoreMetrics,
    ) -> Result<StandaloneStore> {
        let store = StrataStore::open(config, metrics)?;
        Ok(StandaloneStore { store })
    }

    fn stop_accounting_worker(store: &mut StrataStore) {
        if let Some(accounting_tx) = store.accounting_tx.take() {
            let _ = accounting_tx.send(AccountingCommand::Shutdown);
        }
        if let Some(accounting_handle) = store.accounting_handle.take() {
            let _ = accounting_handle.join();
        }
    }

    fn init_typed_store_metrics() {
        INIT_TYPED_STORE_METRICS.call_once(|| {
            DBMetrics::get();
        });
    }

    fn config(root_dir: &Path, namespace: &str) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: root_dir.to_path_buf(),
            namespace: namespace.to_owned(),
            segment_max_bytes: 1 << 20,
            write_queue_capacity: 128,
            max_unsealed_segments: 8,
            segment_reader_cache_capacity: 16,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            accounting_interval: DEFAULT_ACCOUNTING_INTERVAL,
            accounting_unaccounted_threshold: DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
            accounting_sidecar_partition_count: DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT,
            accounting_sidecar_interval: DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
            accounting_sidecar_ingest_record_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD,
            accounting_sidecar_delta_run_count_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
            accounting_sidecar_delta_run_bytes_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
            accounting_sidecar_major_patch_count_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
            accounting_sidecar_major_patch_bytes_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
            gc_interval: Duration::from_secs(3600),
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_planner_config: GcPlannerConfig::default(),
            gc_max_accounting_lag_lsn: DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
            starting_epoch: 42,
        }
    }

    fn counter_value(registry: &Registry, name: &str) -> f64 {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .and_then(|family| {
                family
                    .get_metric()
                    .first()
                    .map(|metric| metric.get_counter().value())
            })
            .unwrap_or_else(|| panic!("missing counter metric {name}"))
    }

    fn gauge_value(registry: &Registry, name: &str) -> i64 {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .and_then(|family| {
                family
                    .get_metric()
                    .first()
                    .map(|metric| metric.get_gauge().value() as i64)
            })
            .unwrap_or_else(|| panic!("missing gauge metric {name}"))
    }

    fn histogram_sample_count(registry: &Registry, name: &str) -> u64 {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .and_then(|family| {
                family
                    .get_metric()
                    .first()
                    .map(|metric| metric.get_histogram().sample_count())
            })
            .unwrap_or_else(|| panic!("missing histogram metric {name}"))
    }

    fn wait_for_segment_state(
        index: &StrataIndex,
        segment_id: SegmentId,
        expected_state: SegmentFileState,
    ) -> SegmentState {
        let started = Instant::now();
        loop {
            let state = index.get_segment_state(segment_id).unwrap().unwrap();
            if state.state == expected_state {
                return state;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "timed out waiting for segment {segment_id} to become {expected_state:?}; current state was {:?}",
                state.state
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_accounted_lsn(store: &StrataStore, expected_lsn: StrataLsn) {
        let started = Instant::now();
        loop {
            let accounted_lsn = store.accounted_lsn().unwrap();
            if accounted_lsn >= expected_lsn {
                return;
            }
            {
                let _guard = store
                    .accounting_lock
                    .lock()
                    .expect("accounting run lock poisoned");
                accounting::run_accounting_sidecar_once(store.index(), store.config(), true)
                    .unwrap();
            }
            let accounted_lsn = store.accounted_lsn().unwrap();
            if accounted_lsn >= expected_lsn {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "timed out waiting for accounted_lsn to reach {expected_lsn}; current accounted_lsn was {accounted_lsn}",
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn put_test_segment_state(index: &StrataIndex, segment_id: SegmentId, state: SegmentFileState) {
        index
            .put_segment_state(&SegmentState {
                shard: STORE_SCOPE,
                segment_id,
                volume_id: 0,
                path: format!("ingest/{segment_id:012}.data"),
                placement_class: PlacementClass::Ingest,
                state,
                write_offset: 64,
                durable_offset: 0,
                min_lsn: Some(segment_id),
                max_lsn: Some(segment_id),
                sealed_len: None,
                sealed_sha256: None,
            })
            .unwrap();
    }

    fn version_key(key: &BlobKey, lsn: StrataLsn) -> BlobVersionKey {
        BlobVersionKey {
            key: key.clone(),
            lsn,
        }
    }

    fn seal_first_segment(config: &StrataStoreConfig) -> SegmentState {
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config.clone(), StrataStoreMetrics::default()).unwrap();

        store.put(&key_1, b"payload-a").unwrap();
        store.put(&key_2, b"payload-b").unwrap();

        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed)
    }

    #[tokio::test]
    async fn standalone_put_get_round_trip() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"hello strata").unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
        assert!(dir.path().join("default").join("ingest").exists());
        assert!(dir.path().join("default").join("index").exists());
    }

    #[tokio::test]
    async fn from_index_writes_logical_shard_versions_with_global_store_state() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "shard-a");
        let index =
            StrataIndex::open_path(dir.path().join("shared-index"), cfg.index_cf_prefix()).unwrap();
        let shard = ShardKey {
            id: 5,
            generation: 2,
        };
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        index
            .put_shard_info(shard.id, ShardInfo::active(shard.generation))
            .unwrap();
        let store =
            StrataStore::from_index(cfg, index.clone(), StrataStoreMetrics::default()).unwrap();

        let lsn = store.put(shard.id, &key, b"hello shard").unwrap();

        assert_eq!(lsn, 1);
        assert_eq!(
            store.get_from_shard(shard.id, &key).unwrap(),
            Some(b"hello shard".to_vec())
        );
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(
            index.get_shard_info(shard.id).unwrap(),
            Some(ShardInfo::active(shard.generation))
        );
        assert_eq!(index.get_next_lsn().unwrap(), 2);
        assert!(index.resolve_blob_head(&key, shard).unwrap().is_some());
        assert!(index.get_segment_state(FIRST_SEGMENT_ID).unwrap().is_some());
        assert_eq!(index.get_blob_entry(&key).unwrap(), None);
    }

    #[tokio::test]
    async fn store_writes_logical_shard_into_record_header() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "record-shard");
        let index =
            StrataIndex::open_path(dir.path().join("shared-index"), cfg.index_cf_prefix()).unwrap();
        let shard = ShardKey {
            id: 5,
            generation: 2,
        };
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        index
            .put_shard_info(shard.id, ShardInfo::active(shard.generation))
            .unwrap();
        let store =
            StrataStore::from_index(cfg.clone(), index.clone(), StrataStoreMetrics::default())
                .unwrap();

        let lsn = store.put(shard.id, &key, b"hello shard").unwrap();
        let record_ref = index
            .get_blob_version_for_shard(&version_key(&key, lsn), shard)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let mut reader = strata_segment::SegmentReader::open(
            segment_path(&cfg, record_ref.segment_id),
            record_ref.segment_id,
        )
        .unwrap();

        let metadata = reader.read_record_metadata(record_ref).unwrap();

        assert_eq!(metadata.header.shard, shard);
    }

    #[tokio::test]
    async fn store_hosts_multiple_logical_shards_inside_one_index() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "single-store");
        let index =
            StrataIndex::open_path(dir.path().join("shared-index"), cfg.index_cf_prefix()).unwrap();
        index.put_shard_info(10, ShardInfo::active(4)).unwrap();
        let store =
            StrataStore::from_index(cfg, index.clone(), StrataStoreMetrics::default()).unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        let shard_a = store.add_shard(10).unwrap();
        let shard_b = store.add_shard(20).unwrap();
        let lsn_a = store.put(10, &key, b"primary").unwrap();
        let lsn_b = store.put(20, &key, b"secondary").unwrap();

        assert_eq!(
            shard_a,
            ShardKey {
                id: 10,
                generation: 4
            }
        );
        assert_eq!(
            shard_b,
            ShardKey {
                id: 20,
                generation: 0
            }
        );
        assert_eq!((lsn_a, lsn_b), (1, 2));
        assert_eq!(store.shard_info(10).unwrap(), Some(ShardInfo::active(4)));
        assert_eq!(store.shard_info(20).unwrap(), Some(ShardInfo::active(0)));
        assert_eq!(
            store.get_from_shard(10, &key).unwrap(),
            Some(b"primary".to_vec())
        );
        assert_eq!(
            store.get_from_shard(20, &key).unwrap(),
            Some(b"secondary".to_vec())
        );
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(index.get_next_lsn().unwrap(), 3);
        assert!(index.resolve_blob_head(&key, shard_a).unwrap().is_some());
        assert!(index.resolve_blob_head(&key, shard_b).unwrap().is_some());
        assert_eq!(index.get_blob_entry(&key).unwrap(), None);
        assert!(!dir.path().join("single-store").join("index").exists());
    }

    #[tokio::test]
    async fn concurrent_logical_shard_puts_use_one_global_sequence() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = Arc::new(
            StrataStore::open(
                config(dir.path(), "single-store"),
                StrataStoreMetrics::default(),
            )
            .unwrap(),
        );
        store.add_shard(10).unwrap();
        store.add_shard(20).unwrap();

        let mut handles = Vec::new();
        for (shard_id, prefix) in [(10, "primary"), (20, "secondary")] {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut writes = Vec::new();
                for i in 0..32 {
                    let key = BlobKey::new(format!("{prefix}-{i}").into_bytes()).unwrap();
                    let payload = format!("payload-{prefix}-{i}").into_bytes();
                    let lsn = store.put(shard_id, &key, &payload).unwrap();
                    writes.push((shard_id, key, payload, lsn));
                }
                writes
            }));
        }

        let writes = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let lsns = writes
            .iter()
            .map(|(_, _, _, lsn)| *lsn)
            .collect::<BTreeSet<_>>();

        assert_eq!(writes.len(), 64);
        assert_eq!(lsns.len(), 64);
        assert_eq!(lsns.first().copied(), Some(1));
        assert_eq!(lsns.last().copied(), Some(64));
        assert_eq!(store.index().get_next_lsn().unwrap(), 65);
        for (shard_id, key, payload, _) in writes {
            assert_eq!(store.get_from_shard(shard_id, &key).unwrap(), Some(payload));
        }
    }

    #[tokio::test]
    async fn store_blob_ops_apply_to_all_active_logical_shard_heads() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        let shard_a = store.add_shard(10).unwrap();
        let shard_b = store.add_shard(20).unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        let lsn_a = store.put(10, &key, b"primary").unwrap();
        let lsn_b = store.put(20, &key, b"secondary").unwrap();
        let ref_a = store
            .index()
            .get_blob_version_for_shard(&version_key(&key, lsn_a), shard_a)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version_for_shard(&version_key(&key, lsn_b), shard_b)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let extend_lsn = store.set_blob_lifetime(&key, 50).unwrap();
        let tombstone_lsn = store.tombstone(&key).unwrap();

        assert_eq!((lsn_a, lsn_b, extend_lsn, tombstone_lsn), (1, 2, 3, 4));
        assert_eq!(
            store.index().get_unaccounted_lsn_op(extend_lsn).unwrap(),
            Some(key.clone())
        );
        assert_eq!(
            store
                .index()
                .blob_version_ops_at_lsn(&key, extend_lsn)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store
                .index()
                .blob_lifecycle_ops_at_lsn(&key, extend_lsn)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .index()
                .blob_version_ops_at_lsn(&key, tombstone_lsn)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store
                .index()
                .blob_lifecycle_ops_at_lsn(&key, tombstone_lsn)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(store.get_from_shard(10, &key).unwrap(), None);
        assert_eq!(store.get_from_shard(20, &key).unwrap(), None);
        assert_eq!(
            store
                .index()
                .resolve_blob_head(&key, shard_a)
                .unwrap()
                .unwrap()
                .head_lsn,
            lsn_a
        );
        assert_eq!(
            store
                .index()
                .resolve_blob_head(&key, shard_b)
                .unwrap()
                .unwrap()
                .head_lsn,
            lsn_b
        );

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let mut expected_tombstoned = std::collections::BTreeMap::new();
        *expected_tombstoned.entry(ref_a.segment_id).or_insert(0) += ref_a.len;
        *expected_tombstoned.entry(ref_b.segment_id).or_insert(0) += ref_b.len;
        for (segment_id, retired_bytes) in expected_tombstoned {
            let stats = segment_summary(store.index(), segment_id);
            assert_eq!(stats.total_bytes, retired_bytes);
            assert_eq!(stats.live_bytes, 0);
            assert_eq!(stats.live_ref_count, 0);
            assert_eq!(stats.retired_bytes, retired_bytes);
            assert!(stats.future_epoch_histogram.is_empty());
        }
        assert_eq!(
            store.index().iter_unaccounted_lsn_ops().unwrap(),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn tombstone_barrier_keeps_later_shard_put_visible_only_for_that_shard() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        store.add_shard(10).unwrap();
        store.add_shard(20).unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        store.put(10, &key, b"primary-old").unwrap();
        store.put(20, &key, b"secondary-old").unwrap();
        let tombstone_lsn = store.tombstone(&key).unwrap();
        let resurrect_lsn = store.put(10, &key, b"primary-new").unwrap();

        assert!(resurrect_lsn > tombstone_lsn);
        assert_eq!(
            store.get_from_shard(10, &key).unwrap(),
            Some(b"primary-new".to_vec())
        );
        assert_eq!(store.get_from_shard(20, &key).unwrap(), None);
        assert_eq!(
            store
                .index()
                .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
                .unwrap()
                .tombstone_lsn,
            Some(tombstone_lsn)
        );
    }

    #[tokio::test]
    async fn store_batch_buffers_ops_until_write_and_returns_global_lsns() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        store.add_shard(10).unwrap();
        store.add_shard(20).unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

        let mut batch = store.batch();
        batch
            .put(10, key_a.clone(), Arc::<[u8]>::from(&b"payload-a"[..]))
            .put(20, key_b.clone(), Arc::<[u8]>::from(&b"payload-b"[..]))
            .set_blob_lifetime(key_a.clone(), 50)
            .tombstone(key_b.clone());
        assert_eq!(store.index().get_next_lsn().unwrap(), 1);

        let result = batch.write().unwrap();

        assert_eq!(result.op_lsns(), &[1, 2, 3, 4]);
        assert_eq!(store.index().get_next_lsn().unwrap(), 5);
        assert_eq!(
            store.get_from_shard(10, &key_a).unwrap(),
            Some(b"payload-a".to_vec())
        );
        assert_eq!(store.get_from_shard(20, &key_b).unwrap(), None);
        assert_eq!(
            store
                .index()
                .resolve_blob_lifecycle_at(&key_a, StrataLsn::MAX)
                .unwrap()
                .lifetime
                .unwrap()
                .lifecycle
                .logical_end_epoch,
            50
        );
    }

    #[tokio::test]
    async fn store_batch_can_mix_epoch_changes_with_blob_ops() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

        let mut batch = store.batch();
        batch
            .put(
                STANDALONE_SHARD.id,
                key_a.clone(),
                Arc::<[u8]>::from(&b"payload-a"[..]),
            )
            .increment_epoch()
            .put(
                STANDALONE_SHARD.id,
                key_b.clone(),
                Arc::<[u8]>::from(&b"payload-b"[..]),
            );

        let result = batch.write().unwrap();

        assert_eq!(result.op_lsns(), &[1, 2, 3]);
        assert_eq!(result.op_epochs(), &[None, Some(43), None]);
        assert_eq!(result.epoch_for_op(1), Some(43));
        assert_eq!(result.last_epoch(), Some(43));
        assert_eq!(store.current_epoch().unwrap(), 43);
        assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(42));
        assert_eq!(store.epoch_at_lsn(2).unwrap(), Some(43));
        assert_eq!(store.index().get_epoch_change(2).unwrap(), Some(43));
        assert_eq!(store.index().get_next_lsn().unwrap(), 4);
        assert_eq!(store.get(&key_a).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 3);
    }

    #[tokio::test]
    async fn sync_publishes_active_accounting_delta_log_state() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        store.tombstone(&key).unwrap();
        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), epoch_lsn);
        let state = store
            .index()
            .get_accounting_active_delta_log_state()
            .unwrap()
            .unwrap();
        assert_eq!(state.durable_lsn, epoch_lsn);
        assert!(state.durable_offset > 0);
    }

    #[tokio::test]
    async fn accounting_sidecar_ingests_active_delta_log_and_compacts_to_patch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"sidecar-ingest".to_vec()).unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.accounting_interval = Duration::from_secs(3600);
        cfg.accounting_sidecar_ingest_record_threshold = usize::MAX;
        cfg.accounting_sidecar_major_patch_count_threshold = 0;
        cfg.accounting_sidecar_major_patch_bytes_threshold = 0;
        let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        stop_accounting_worker(&mut store.store);

        store.put(&key, b"payload").unwrap();
        store.sync().unwrap();
        accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();

        let active_state = active_delta_log_state(store.index());
        let consumed_cursor = active_delta_log_read_cursor(store.index());
        assert_eq!(consumed_cursor.offset, active_state.durable_offset);
        assert_eq!(consumed_cursor.max_lsn, active_state.durable_lsn);

        let sidecar = open_accounting_sidecar(&store);
        let partition = sidecar.manifest().partitions.values().next().unwrap();
        let delta_count = sidecar
            .manifest()
            .partitions
            .values()
            .map(|partition| partition.deltas.len())
            .sum::<usize>();
        let patch_count = sidecar
            .manifest()
            .partitions
            .values()
            .map(|partition| partition.patches.len())
            .sum::<usize>();
        assert_eq!(delta_count, 0);
        assert_eq!(patch_count, 1);
        assert!(partition.base.is_none());
    }

    #[tokio::test]
    async fn accounting_sidecar_major_compacts_when_patch_threshold_reached() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"sidecar-major".to_vec()).unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.accounting_interval = Duration::from_secs(3600);
        cfg.accounting_sidecar_ingest_record_threshold = usize::MAX;
        cfg.accounting_sidecar_major_patch_count_threshold = 1;
        let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        stop_accounting_worker(&mut store.store);

        store.put(&key, b"payload").unwrap();
        store.sync().unwrap();
        accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();

        let sidecar = open_accounting_sidecar(&store);
        let base_count = sidecar
            .manifest()
            .partitions
            .values()
            .filter(|partition| partition.base.is_some())
            .count();
        let patch_count = sidecar
            .manifest()
            .partitions
            .values()
            .map(|partition| partition.patches.len())
            .sum::<usize>();
        assert_eq!(base_count, 1);
        assert_eq!(patch_count, 0);
    }

    #[tokio::test]
    async fn store_rejects_missing_and_inactive_logical_shards() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "single-store");
        let index =
            StrataIndex::open_path(dir.path().join("shared-index"), cfg.index_cf_prefix()).unwrap();
        index
            .put_shard_info(
                30,
                ShardInfo {
                    current_generation: 2,
                    state: strata_core::ShardState::Dropped,
                },
            )
            .unwrap();
        let store = StrataStore::from_index(cfg, index, StrataStoreMetrics::default()).unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        let error = store.put(31, &key, b"missing").unwrap_err();
        match error {
            Error::ShardNotFound { shard_id } => assert_eq!(shard_id, 31),
            other => panic!("unexpected error: {other:?}"),
        }

        let error = store.put(30, &key, b"dropped").unwrap_err();

        match error {
            Error::ShardUnavailable {
                shard_id,
                generation,
                current_generation,
                state,
            } => {
                assert_eq!(shard_id, 30);
                assert_eq!(generation, 2);
                assert_eq!(current_generation, 2);
                assert_eq!(state, strata_core::ShardState::Dropped);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn store_add_after_drop_bumps_generation_and_hides_old_versions() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        let first_shard = store.add_shard(40).unwrap();
        assert_eq!(
            first_shard,
            ShardKey {
                id: 40,
                generation: 0
            }
        );
        store.put(40, &key, b"old generation").unwrap();
        assert_eq!(
            store.get_from_shard(40, &key).unwrap(),
            Some(b"old generation".to_vec())
        );

        store.drop_shard(40).unwrap();
        assert_eq!(
            store.shard_info(40).unwrap(),
            Some(ShardInfo {
                current_generation: 0,
                state: strata_core::ShardState::Dropped,
            })
        );
        assert!(store.put(40, &key, b"dropped").is_err());
        assert!(store.get_from_shard(40, &key).is_err());
        assert_eq!(
            store.index().resolve_blob_head(&key, first_shard).unwrap(),
            None
        );

        let second_shard = store.add_shard(40).unwrap();
        assert_eq!(
            second_shard,
            ShardKey {
                id: 40,
                generation: 1
            }
        );

        assert_eq!(store.get_from_shard(40, &key).unwrap(), None);
        store.put(40, &key, b"new generation").unwrap();
        assert_eq!(
            store.get_from_shard(40, &key).unwrap(),
            Some(b"new generation".to_vec())
        );
        assert!(
            store
                .index()
                .resolve_blob_head(&key, second_shard)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn drop_shard_marks_metadata_without_accounting_or_tombstones() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
        let shard = store.add_shard(41).unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

        let put_lsn = store.put(41, &key, b"old generation").unwrap();
        assert_eq!(store.index().get_next_lsn().unwrap(), put_lsn + 1);

        store.drop_shard(41).unwrap();

        assert_eq!(
            store.shard_info(41).unwrap(),
            Some(ShardInfo {
                current_generation: 0,
                state: ShardState::Dropped,
            })
        );
        assert!(store.get_from_shard(41, &key).is_err());
        assert_eq!(store.index().resolve_blob_head(&key, shard).unwrap(), None);
        assert_eq!(store.index().get_next_lsn().unwrap(), put_lsn + 1);
        assert_eq!(
            store.index().get_unaccounted_lsn_op(put_lsn).unwrap(),
            Some(key)
        );
    }

    #[tokio::test]
    async fn store_drop_missing_shard_fails() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store = StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

        let error = store.drop_shard(50).unwrap_err();

        match error {
            Error::ShardNotFound { shard_id } => assert_eq!(shard_id, 50),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn new_store_records_starting_epoch_as_lsn_zero_genesis() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.current_epoch().unwrap(), 42);
        assert_eq!(store.epoch_at_lsn(0).unwrap(), Some(42));
        assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(42));
        assert_eq!(store.index().get_epoch_change(0).unwrap(), Some(42));
        assert_eq!(store.index().get_next_lsn().unwrap(), 1);
        assert_eq!(store.index().get_durable_lsn().unwrap(), 0);
        assert_eq!(
            store.index().get_shard_info(0).unwrap(),
            Some(ShardInfo::active(0))
        );
    }

    #[tokio::test]
    async fn reopen_uses_persisted_epoch_instead_of_config_starting_epoch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            assert_eq!(store.increment_epoch().unwrap(), (43, 1));
            store.sync().unwrap();
        }

        cfg.starting_epoch = 99;
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.current_epoch().unwrap(), 43);
        assert_eq!(store.epoch_at_lsn(0).unwrap(), Some(42));
        assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(43));
    }

    #[tokio::test]
    async fn increment_epoch_consumes_lsn_and_is_metadata_durable() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert_eq!(store.increment_epoch().unwrap(), (43, 1));
        assert_eq!(store.current_epoch().unwrap(), 43);
        assert_eq!(store.index().get_next_lsn().unwrap(), 2);
        assert_eq!(store.index().get_epoch_change(1).unwrap(), Some(43));
        assert_eq!(store.put(&key, b"payload").unwrap(), 2);

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn get_with_options_can_skip_checksum_verification() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"hello strata").unwrap();
        store.sync().unwrap();

        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let mut segment = OpenOptions::new()
            .write(true)
            .open(segment_path(store.config(), record_ref.segment_id))
            .unwrap();
        segment
            .seek(SeekFrom::Start(
                record_ref.offset + FIXED_RECORD_HEADER_LEN as u64,
            ))
            .unwrap();
        segment.write_all(b"H").unwrap();

        let err = store.get(&key).unwrap_err();
        assert!(matches!(
            err,
            Error::Segment(strata_segment::Error::Core(
                strata_core::Error::RecordChecksumMismatch { .. }
            ))
        ));

        assert_eq!(
            store
                .get_with_options(&key, ReadOptions::skip_checksum_verification())
                .unwrap(),
            Some(b"Hello strata".to_vec())
        );
    }

    #[tokio::test]
    async fn get_blob_range_reads_payload_slice() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"hello strata").unwrap();

        assert_eq!(
            store.get_blob_range(&key, 6..12).unwrap(),
            Some(b"strata".to_vec())
        );
    }

    #[tokio::test]
    async fn cached_reader_is_evictable_when_segment_is_deleted() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"hello strata").unwrap();
        store.sync().unwrap();

        assert_eq!(store.reader_cache_len(), 0);
        assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
        assert_eq!(store.reader_cache_len(), 1);

        let mut state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        state.state = SegmentFileState::Deleted;
        let mut batch = store.index().batch();
        store
            .index()
            .put_segment_state_batch(&mut batch, &state)
            .unwrap();
        batch.write().unwrap();
        store.index().flush_wal(true).unwrap();
        store.evict_segment_reader(FIRST_SEGMENT_ID);

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
        assert_eq!(store.reader_cache_len(), 0);
    }

    #[tokio::test]
    async fn stream_blob_reads_payload_slice() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"hello strata").unwrap();

        let mut stream = store.stream_blob(&key, 0..5).unwrap().unwrap();
        let mut read = Vec::new();
        stream.read_to_end(&mut read).unwrap();

        assert_eq!(read, b"hello");
        assert_eq!(stream.remaining(), 0);
    }

    #[tokio::test]
    async fn read_range_missing_and_tombstone_return_none() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let missing = BlobKey::new(b"missing".to_vec()).unwrap();
        let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert_eq!(store.get_blob_range(&missing, 0..1).unwrap(), None);
        assert!(store.stream_blob(&missing, 0..1).unwrap().is_none());

        store.put(&tombstoned, b"payload").unwrap();
        store.tombstone(&tombstoned).unwrap();

        assert_eq!(store.get_blob_range(&tombstoned, 0..1).unwrap(), None);
        assert!(store.stream_blob(&tombstoned, 0..1).unwrap().is_none());
    }

    #[tokio::test]
    async fn read_range_rejects_out_of_bounds_range() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();

        let err = store.get_blob_range(&key, 0..8).unwrap_err();

        assert!(matches!(
            err,
            Error::Segment(strata_segment::Error::InvalidPayloadRange { .. })
        ));
    }

    #[tokio::test]
    async fn range_read_rejects_index_record_key_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key_a, b"payload-a").unwrap();
        store.put(&key_b, b"payload-b").unwrap();

        let mut entry_b = store.index().get_blob_entry(&key_b).unwrap().unwrap();
        entry_b.lsn += 1;
        store.index().put_blob_entry(&key_a, &entry_b).unwrap();

        let err = store.get_blob_range(&key_a, 0..1).unwrap_err();

        assert!(matches!(
            err,
            Error::KeyMismatch {
                requested,
                found
            } if requested == key_a && found == key_b
        ));
    }

    #[tokio::test]
    async fn store_from_index_does_not_create_local_index_dir() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let db_dir = tempdir().unwrap();
        let db = open_cf(
            db_dir.path(),
            None,
            MetricConf::new("strata_store_test"),
            &["existing"],
        )
        .unwrap();
        let index = StrataIndex::from_db(db, "strata/shard-99").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::from_index(
            config(dir.path(), "shard-99"),
            index,
            StrataStoreMetrics::default(),
        )
        .unwrap();

        store.put(STANDALONE_SHARD.id, &key, b"payload").unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        assert!(dir.path().join("shard-99").join("ingest").exists());
        assert!(!dir.path().join("shard-99").join("index").exists());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"missing".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
    }

    #[tokio::test]
    async fn point_in_time_recovery_removes_orphan_segment_file_before_opening_active_writer() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
        fs::create_dir_all(cfg.ingest_dir()).unwrap();
        fs::write(&orphan_path, b"stale bytes").unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        assert_eq!(fs::metadata(&orphan_path).unwrap().len(), 0);

        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        store.put(&key, b"payload").unwrap();

        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(entry.record_ref.unwrap().offset, 0);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    }

    #[tokio::test]
    async fn absolute_consistency_rejects_orphan_segment_file() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
        let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
        fs::create_dir_all(cfg.ingest_dir()).unwrap();
        fs::write(&orphan_path, b"stale bytes").unwrap();

        let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
        assert!(matches!(
            err,
            Error::OrphanSegmentFile {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn tombstone_hides_payload() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();
        let put_lsn = store.put(&key, b"payload").unwrap();

        let tombstone_lsn = store.tombstone(&key).unwrap();

        assert_eq!(put_lsn, 1);
        assert_eq!(tombstone_lsn, 2);
        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
        let put_entry = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key)
                .unwrap()
                .unwrap()
                .record_ref,
            put_entry.record_ref
        );
        assert_eq!(
            store
                .index()
                .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
                .unwrap()
                .tombstone_lsn,
            Some(tombstone_lsn)
        );
        assert_eq!(put_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
        assert_eq!(store.durable_lsn().unwrap(), 0);

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn metrics_track_core_store_operations() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let missing = BlobKey::new(b"missing".to_vec()).unwrap();
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
        let store = try_open_standalone_store(config(dir.path(), "default"), metrics).unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        assert_eq!(store.get(&missing).unwrap(), None);
        assert_eq!(
            store.get_blob_range(&key, 1..4).unwrap(),
            Some(b"ayl".to_vec())
        );
        store.sync().unwrap();
        let tombstone_lsn = store.tombstone(&key).unwrap();

        assert_eq!(put_lsn, 1);
        assert_eq!(tombstone_lsn, 2);
        assert_eq!(
            gauge_value(&registry, "strata_store_queued_write_commands"),
            0
        );
        assert_eq!(
            histogram_sample_count(&registry, "strata_store_write_queue_send_duration_seconds"),
            3
        );
        assert_eq!(
            counter_value(&registry, "strata_store_write_queue_send_errors_total"),
            0.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_put_calls_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_put_errors_total"),
            0.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_put_payload_bytes_total"),
            7.0
        );
        assert!(
            counter_value(&registry, "strata_store_put_record_bytes_total")
                > counter_value(&registry, "strata_store_put_payload_bytes_total")
        );
        assert_eq!(
            counter_value(&registry, "strata_store_sync_calls_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_sync_errors_total"),
            0.0
        );
        assert!(counter_value(&registry, "strata_store_sync_bytes_total") > 0.0);
        assert_eq!(
            counter_value(&registry, "strata_store_get_calls_total"),
            2.0
        );
        assert_eq!(counter_value(&registry, "strata_store_get_hits_total"), 1.0);
        assert_eq!(
            counter_value(&registry, "strata_store_get_misses_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_get_payload_bytes_total"),
            7.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_range_read_calls_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_range_read_hits_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_range_read_payload_bytes_total"),
            3.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_stream_calls_total"),
            1.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_stream_hits_total"),
            1.0
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_active_segment_id"),
            FIRST_SEGMENT_ID as i64
        );
        assert!(gauge_value(&registry, "strata_store_active_segment_write_offset") > 0);
        assert!(gauge_value(&registry, "strata_store_active_segment_durable_offset") > 0);
        assert_eq!(gauge_value(&registry, "strata_store_next_lsn"), 3);
        assert_eq!(gauge_value(&registry, "strata_store_durable_lsn"), 1);
        assert_eq!(gauge_value(&registry, "strata_store_pending_lsn_count"), 1);
        assert_eq!(
            counter_value(&registry, "strata_store_seal_backpressure_waits_total"),
            0.0
        );
        assert_eq!(
            histogram_sample_count(
                &registry,
                "strata_store_seal_backpressure_wait_duration_seconds"
            ),
            0
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_seal_backpressure_current"),
            0
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_gc_configured_workers"),
            DEFAULT_GC_WORKER_COUNT as i64
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_gc_active_worker_limit"),
            DEFAULT_GC_INITIAL_WORKER_COUNT as i64
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_gc_in_flight_workers"),
            0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_gc_admitted_total"),
            0.0
        );
        assert_eq!(
            counter_value(&registry, "strata_store_gc_skipped_by_tuner_total"),
            0.0
        );
    }

    #[tokio::test]
    async fn metrics_track_seal_backpressure_waits() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.max_unsealed_segments = 2;
        ensure_ingest_dir(&cfg).unwrap();
        let index =
            StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
        put_test_segment_state(&index, 1, SegmentFileState::Sealing);
        put_test_segment_state(&index, 2, SegmentFileState::Open);

        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
        let gc_concurrency = Arc::new(GcConcurrencyController::new(
            GcConcurrencyConfig::from_store_config(&cfg),
            metrics.clone(),
        ));
        let active_writer = SegmentWriter::create(
            segment_path(&cfg, 2),
            2,
            PlacementClass::Ingest,
            cfg.segment_max_bytes,
        )
        .unwrap();
        let (seal_tx, _seal_rx) = mpsc::channel();
        let (_write_tx, write_rx) = mpsc::sync_channel(1);
        let (accounting_tx, _accounting_rx) = mpsc::sync_channel(1);
        let active_segment_state = active_segment_state(&cfg, STORE_SCOPE, &active_writer, 0);
        let active_accounting_delta_log =
            ActiveDeltaLog::open(cfg.accounting_index_dir(), ActiveDeltaLogState::default())
                .unwrap();
        let coordinator = WriteCoordinator {
            config: cfg.clone(),
            index: index.clone(),
            active_writer,
            active_accounting_delta_log,
            active_segment_state,
            durable_offset: 0,
            pending_rollovers: Vec::new(),
            seal_tx,
            accounting_tx,
            write_rx,
            store_scope: STORE_SCOPE,
            reader_cache: Arc::new(SegmentReaderCache::new(cfg.segment_reader_cache_capacity)),
            gc_concurrency,
            metrics,
        };

        let unblocker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            put_test_segment_state(&index, 1, SegmentFileState::Sealed);
        });

        coordinator.wait_for_seal_backlog_capacity().unwrap();
        unblocker.join().unwrap();

        assert_eq!(
            counter_value(&registry, "strata_store_seal_backpressure_waits_total"),
            1.0
        );
        assert_eq!(
            histogram_sample_count(
                &registry,
                "strata_store_seal_backpressure_wait_duration_seconds"
            ),
            1
        );
        assert_eq!(
            gauge_value(&registry, "strata_store_seal_backpressure_current"),
            0
        );
    }

    #[tokio::test]
    async fn set_blob_lifetime_preserves_payload_until_accounting() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let before = store.index().get_blob_entry(&key).unwrap().unwrap();

        let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

        let after = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(put_lsn, 1);
        assert_eq!(extend_lsn, 2);
        assert_eq!(after, before);
        let lifecycle = store
            .index()
            .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
            .unwrap()
            .lifetime
            .unwrap()
            .lifecycle;
        assert_eq!(lifecycle.logical_end_epoch, 50);
        assert_eq!(lifecycle.extension_count, 0);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.record_ref, before.record_ref.unwrap());
        assert_eq!(resolved.generation, before.generation);
        assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);

        let stats = segment_summary(store.index(), resolved.record_ref.segment_id);
        assert_eq!(stats, strata_core::SegmentGcSummary::default());

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 2);
        wait_for_accounted_lsn(&store, 2);
        let stats = segment_summary(store.index(), resolved.record_ref.segment_id);
        assert_eq!(stats.future_epoch_histogram.get(&43), None);
        assert_eq!(
            stats.future_epoch_histogram.get(&50),
            Some(&EpochBucket {
                refs: 1,
                bytes: resolved.record_ref.len,
            })
        );
        assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
        assert_eq!(stats.extension_count_histogram.get(&1), None);
        assert_eq!(stats.unknown_lifetime_bytes, 0);
        assert_eq!(stats.unknown_lifetime_ref_count, 0);
        assert_eq!(stats.min_live_end_epoch, Some(50));
        assert_eq!(stats.max_live_end_epoch, Some(50));
        assert_eq!(stats.live_bytes, resolved.record_ref.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.total_bytes, resolved.record_ref.len);
    }

    #[tokio::test]
    async fn accounting_updates_exact_epoch_segment_pinning_after_lifetime_update() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        let record_ref = entry.record_ref.unwrap();
        let mut state = store
            .index()
            .get_segment_state(record_ref.segment_id)
            .unwrap()
            .unwrap();
        state.placement_class = PlacementClass::ExactEpoch(42);
        store.index().put_segment_state(&state).unwrap();

        let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats, strata_core::SegmentGcSummary::default());

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, extend_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.future_epoch_histogram.get(&42), None);
        assert_eq!(
            stats.future_epoch_histogram.get(&50),
            Some(&EpochBucket {
                refs: 1,
                bytes: record_ref.len,
            })
        );
        assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
        assert_eq!(stats.extension_count_histogram.get(&1), None);
    }

    #[tokio::test]
    async fn accounting_tracks_unknown_lifetime_until_metadata_arrives() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.live_bytes, record_ref.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.unknown_lifetime_bytes, record_ref.len);
        assert_eq!(stats.unknown_lifetime_ref_count, 1);
        assert!(stats.future_epoch_histogram.is_empty());

        let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.live_bytes, record_ref.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.unknown_lifetime_bytes, 0);
        assert_eq!(stats.unknown_lifetime_ref_count, 0);
        assert_eq!(
            stats.future_epoch_histogram.get(&50),
            Some(&EpochBucket {
                refs: 1,
                bytes: record_ref.len,
            })
        );
    }

    #[tokio::test]
    async fn accounting_applies_lifetime_written_before_payload() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
        let put_lsn = store.put(&key, b"payload").unwrap();
        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        assert_eq!((lifetime_lsn, put_lsn), (1, 2));
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.live_bytes, record_ref.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.unknown_lifetime_bytes, 0);
        assert_eq!(stats.unknown_lifetime_ref_count, 0);
        assert_eq!(
            stats.future_epoch_histogram.get(&50),
            Some(&EpochBucket {
                refs: 1,
                bytes: record_ref.len,
            })
        );
    }

    #[tokio::test]
    async fn set_blob_lifetime_missing_or_tombstoned_blob_records_metadata() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let missing = BlobKey::new(b"missing".to_vec()).unwrap();
        let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert_eq!(store.extend(&missing, 50).unwrap(), Some(1));
        assert_eq!(store.get(&missing).unwrap(), None);
        assert_eq!(store.index().get_next_lsn().unwrap(), 2);

        store.put(&tombstoned, b"payload").unwrap();
        store.tombstone(&tombstoned).unwrap();

        assert_eq!(store.extend(&tombstoned, 50).unwrap(), Some(4));
        assert_eq!(store.get(&tombstoned).unwrap(), None);
        assert_eq!(store.index().get_next_lsn().unwrap(), 5);
    }

    #[tokio::test]
    async fn read_after_lifetime_update_chain_resolves_latest_lifetime() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        let extension_count = 4;
        let last_epoch = 42 + extension_count;
        for epoch in 43..=last_epoch {
            store.extend(&key, epoch).unwrap().unwrap();
        }

        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        let latest = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(latest.record_ref, Some(resolved.record_ref));
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

        let lifecycle = resolved.lifecycle.unwrap();
        assert_eq!(lifecycle.logical_end_epoch, last_epoch);
        assert_eq!(lifecycle.extension_count, extension_count as u32 - 1);
    }

    #[tokio::test]
    async fn store_set_blob_lifetime_preserves_payload_and_updates_lifecycle() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

        assert_eq!(put_lsn, 1);
        assert_eq!(extend_lsn, 2);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
    }

    #[tokio::test]
    async fn set_blob_lifetime_rejects_current_or_past_epoch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert!(matches!(
            store.set_blob_lifetime(&key, 42),
            Err(Error::InvalidBlobLifetime {
                logical_end_epoch: 42,
                current_epoch: 42
            })
        ));
        assert!(matches!(
            store.set_blob_lifetime(&key, 41),
            Err(Error::InvalidBlobLifetime {
                logical_end_epoch: 41,
                current_epoch: 42
            })
        ));
    }

    #[tokio::test]
    async fn expired_lifetime_hides_blob_reads_before_gc() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        store.set_blob_lifetime(&key, 43).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

        store.increment_epoch().unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
    }

    #[tokio::test]
    async fn later_lifetime_before_expiry_keeps_current_blob_visible() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        store.set_blob_lifetime(&key, 43).unwrap();
        store.set_blob_lifetime(&key, 50).unwrap();
        store.increment_epoch().unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
    }

    #[tokio::test]
    async fn new_put_after_policy_expiry_does_not_inherit_stale_lifetime() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.set_blob_lifetime(&key, 43).unwrap();
        store.increment_epoch().unwrap();
        store.put(&key, b"new").unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"new".to_vec()));
        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.lifecycle, None);
    }

    #[tokio::test]
    async fn lifetime_update_after_expiry_applies_only_to_future_put() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"old").unwrap();
        store.set_blob_lifetime(&key, 43).unwrap();
        store.increment_epoch().unwrap();
        store.set_blob_lifetime(&key, 50).unwrap();
        assert_eq!(store.get(&key).unwrap(), None);

        store.put(&key, b"new").unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"new".to_vec()));
        let resolved = resolve_blob_version(store.index(), store.shard(), &key)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
    }

    #[tokio::test]
    async fn reopen_reads_existing_data() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = try_open_standalone_store(
                config(dir.path(), "default"),
                StrataStoreMetrics::default(),
            )
            .unwrap();
            store.put(&key, b"payload").unwrap();
            store.sync().unwrap();
        }

        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    }

    #[tokio::test]
    async fn sync_advances_durable_offset_after_segment_fsync() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();

        let unsynced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(unsynced.durable_offset, 0);
        assert!(unsynced.write_offset > 0);
        assert_eq!(store.durable_lsn().unwrap(), 0);
        assert_eq!(
            store.index().iter_unaccounted_lsn_ops().unwrap(),
            vec![(1, key.clone())]
        );

        store.sync().unwrap();

        let synced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(synced.durable_offset, unsynced.write_offset);
        assert_eq!(synced.write_offset, unsynced.write_offset);
        assert_eq!(store.durable_lsn().unwrap(), 1);
        wait_for_accounted_lsn(&store, 1);
        assert_eq!(store.accounted_lsn().unwrap(), 1);
        assert_eq!(
            store.index().iter_unaccounted_lsn_ops().unwrap(),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn put_after_sync_preserves_durable_offset() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key_1, b"payload-a").unwrap();
        store.sync().unwrap();
        let synced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();

        store.put(&key_2, b"payload-b").unwrap();

        let after_put = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(after_put.durable_offset, synced.durable_offset);
        assert!(after_put.write_offset > synced.write_offset);
    }

    #[tokio::test]
    async fn recovery_keeps_complete_unsynced_record_that_survived() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        let state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert!(state.write_offset > 0);
        assert_eq!(state.durable_offset, state.write_offset);
        assert_eq!(store.durable_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_removes_live_index_entry_when_segment_bytes_are_missing() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(0)
            .unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
    }

    #[tokio::test]
    async fn recovery_ignores_segment_record_when_index_version_is_missing() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            let segment_path = segment_path(&cfg, FIRST_SEGMENT_ID);
            let mut segment = SegmentWriter::open_existing(
                &segment_path,
                FIRST_SEGMENT_ID,
                PlacementClass::Ingest,
                1 << 20,
            )
            .unwrap();
            segment.append(&key, 1, b"payload").unwrap();
            let write_offset = segment.write_offset();
            drop(segment);

            let state = active_segment_state_from_path(
                &cfg,
                STORE_SCOPE,
                FIRST_SEGMENT_ID,
                write_offset,
                0,
            );
            let mut batch = store.index().batch();
            store
                .index()
                .put_segment_state_batch(&mut batch, &state)
                .unwrap();
            store
                .index()
                .put_store_state_batch(&mut batch, &StrataStoreState::default())
                .unwrap();
            batch.write().unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
        assert_eq!(store.index().get_next_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_truncates_partial_tail() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let valid_len;
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
            valid_len = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .write_offset;
        }

        let path = segment_path(&cfg, FIRST_SEGMENT_ID);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);
        assert!(std::fs::metadata(&path).unwrap().len() > valid_len);

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[tokio::test]
    async fn recovery_removes_tombstone_when_rolled_back_lsn_range_is_lost() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload").unwrap();
            store.tombstone(&key).unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(0)
            .unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.durable_lsn().unwrap(), 0);
    }

    #[tokio::test]
    async fn recovery_removes_epoch_changes_after_rolled_back_blob_lsn() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            assert_eq!(store.put(&key, b"payload").unwrap(), 1);
            assert_eq!(store.increment_epoch().unwrap(), (43, 2));
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(0)
            .unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.current_epoch().unwrap(), 42);
        assert_eq!(store.epoch_at_lsn(2).unwrap(), Some(42));
        assert_eq!(store.index().get_epoch_change(2).unwrap(), None);
        assert_eq!(store.index().get_next_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_rolls_back_lost_overwrite_to_previous_entry() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let first_len;
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload-a").unwrap();
            store.sync().unwrap();
            first_len = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .write_offset;
            store.put(&key, b"payload-b").unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(first_len)
            .unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload-a".to_vec()));
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(entry.lsn, 1);
        assert_eq!(store.durable_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn put_overwrite_keeps_blob_versions_for_old_records() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let min_lsn = store.put(&key, b"payload-a").unwrap();
        let second_lsn = store.put(&key, b"payload-b").unwrap();
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();

        assert_ne!(min_lsn, second_lsn);
        assert_eq!(entry.lsn, second_lsn);
        let first_entry = store
            .index()
            .get_blob_version(&version_key(&key, min_lsn))
            .unwrap()
            .unwrap();
        let second_entry = store
            .index()
            .get_blob_version(&version_key(&key, second_lsn))
            .unwrap()
            .unwrap();
        assert_eq!(first_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
        assert_eq!(
            second_entry.record_ref.unwrap().segment_id,
            FIRST_SEGMENT_ID
        );
    }

    #[tokio::test]
    async fn accounting_tombstones_overwritten_payload_summary() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let first_lsn = store.put(&key, b"payload-a").unwrap();
        let second_lsn = store.put(&key, b"payload-b").unwrap();
        let lifetime_lsn = store.extend(&key, 44).unwrap().unwrap();
        let first_record_ref = store
            .index()
            .get_blob_version(&version_key(&key, first_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let second_record_ref = store
            .index()
            .get_blob_version(&version_key(&key, second_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let stats = segment_summary(store.index(), first_record_ref.segment_id);
        assert_eq!(stats, strata_core::SegmentGcSummary::default());

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let stats = segment_summary(store.index(), first_record_ref.segment_id);
        assert_eq!(
            stats.total_bytes,
            first_record_ref.len + second_record_ref.len
        );
        assert_eq!(stats.live_bytes, second_record_ref.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.retired_bytes, first_record_ref.len);
        assert_eq!(stats.future_epoch_histogram.get(&43), None);
        assert_eq!(
            stats.future_epoch_histogram.get(&44),
            Some(&EpochBucket {
                refs: 1,
                bytes: second_record_ref.len,
            })
        );
        assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
        assert_eq!(
            store.index().iter_unaccounted_lsn_ops().unwrap(),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn accounting_tombstones_deleted_payload_summary() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let tombstone_lsn = store.tombstone(&key).unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.total_bytes, record_ref.len);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
        assert_eq!(stats.retired_bytes, record_ref.len);
        assert!(stats.future_epoch_histogram.is_empty());
        assert!(stats.extension_count_histogram.is_empty());
        assert_eq!(store.get(&key).unwrap(), None);
    }

    #[tokio::test]
    async fn accounting_leaves_unknown_lifetime_put_copy_eligible() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);

        let overlay = store
            .index()
            .get_segment_gc_overlay(record_ref.segment_id)
            .unwrap()
            .unwrap_or_default();
        assert!(overlay.expired.is_empty());
        assert!(overlay.retired.is_empty());
        assert!(overlay.lifetimes.is_empty());
    }

    #[tokio::test]
    async fn accounting_publishes_retired_segment_ref_event_for_tombstone() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let tombstone_lsn = store.tombstone(&key).unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        assert_eq!(
            store
                .index()
                .iter_segment_ref_events_since(record_ref.segment_id, put_lsn)
                .unwrap(),
            vec![(
                strata_core::SegmentRefEventKey {
                    segment_id: record_ref.segment_id,
                    lsn: tombstone_lsn,
                    offset: record_ref.offset,
                },
                SegmentRefEvent::Retired,
            )]
        );
        let overlay = store
            .index()
            .get_segment_gc_overlay(record_ref.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
        assert!(overlay.expired.is_empty());
        assert!(overlay.lifetimes.is_empty());
    }

    #[tokio::test]
    async fn accounting_snapshot_guard_reports_later_ref_events() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);

        let snapshot = store.create_accounting_snapshot().unwrap();
        assert_eq!(snapshot.accounted_lsn(), put_lsn);

        let tombstone_lsn = store.tombstone(&key).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        assert_eq!(
            store.accounting_changes_since(&snapshot).unwrap(),
            vec![AccountingRefEvent {
                key: strata_core::SegmentRefEventKey {
                    segment_id: record_ref.segment_id,
                    lsn: tombstone_lsn,
                    offset: record_ref.offset,
                },
                event: SegmentRefEvent::Retired,
            }]
        );
    }

    #[tokio::test]
    async fn gc_publish_does_not_force_accounting_to_catch_up() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let mut store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);
        stop_accounting_worker(&mut store.store);

        let accounting_snapshot = store.create_accounting_snapshot().unwrap();
        assert_eq!(accounting_snapshot.accounted_lsn(), put_lsn);

        let tombstone_lsn = store.tombstone(&key).unwrap();
        let copy = PreparedGcCopy {
            accounting_snapshot,
            plan: GcPlan {
                scenario: GcScenario::EmptyDelete,
                actions: Vec::new(),
                copied_bytes: 0,
                expected_reclaim_bytes: 0,
                score: 0,
            },
            outputs: Vec::new(),
            copied_records: Vec::new(),
            claim: None,
        };

        let published = store.publish_prepared_gc_copy(copy).unwrap();

        assert_eq!(published.reconciled_accounted_lsn, put_lsn);
        assert!(store.durable_lsn().unwrap() >= tombstone_lsn);
        assert!(store.accounted_lsn().unwrap() < tombstone_lsn);
    }

    #[tokio::test]
    async fn gc_publish_waiting_for_accounting_lock_does_not_block_writer() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key_a, b"payload-a").unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, put_lsn);
        let accounting_snapshot = store.create_accounting_snapshot().unwrap();
        let copy = PreparedGcCopy {
            accounting_snapshot,
            plan: GcPlan {
                scenario: GcScenario::EmptyDelete,
                actions: Vec::new(),
                copied_bytes: 0,
                expected_reclaim_bytes: 0,
                score: 0,
            },
            outputs: Vec::new(),
            copied_records: Vec::new(),
            claim: None,
        };

        let accounting_lock = store.store.accounting_lock.clone();
        let accounting_guard = accounting_lock
            .lock()
            .expect("accounting run lock poisoned");
        std::thread::scope(|scope| {
            let publish = scope.spawn(|| store.publish_prepared_gc_copy(copy));
            std::thread::sleep(Duration::from_millis(50));

            let (put_tx, put_rx) = mpsc::channel();
            let store_ref = &store;
            let key_b_ref = &key_b;
            let put = scope.spawn(move || {
                let result = store_ref.put(key_b_ref, b"payload-b");
                put_tx.send(result).unwrap();
            });
            let put_result = put_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("writer blocked behind GC accounting-lock wait");
            assert!(put_result.unwrap() > put_lsn);

            drop(accounting_guard);
            assert!(publish.join().unwrap().is_ok());
            put.join().unwrap();
        });
    }

    #[tokio::test]
    async fn gc_prepare_plan_skips_claimed_source_and_uses_next_candidate() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();
        let larger_segment_id = 10;
        let smaller_segment_id = 11;
        for (segment_id, bytes) in [(larger_segment_id, 100), (smaller_segment_id, 10)] {
            let state = SegmentState {
                shard: STORE_SCOPE,
                segment_id,
                volume_id: 0,
                path: format!("ingest/{segment_id:012}.data"),
                placement_class: PlacementClass::Spillover,
                state: SegmentFileState::Sealed,
                write_offset: bytes,
                durable_offset: bytes,
                min_lsn: Some(0),
                max_lsn: Some(0),
                sealed_len: Some(bytes),
                sealed_sha256: None,
            };
            let mut batch = store.index().batch();
            store
                .index()
                .put_segment_state_batch(&mut batch, &state)
                .unwrap();
            store
                .index()
                .merge_segment_gc_overlay_batch(
                    &mut batch,
                    segment_id,
                    vec![SegmentGcOverlayMergeOp::AddRetiredBatch {
                        ranges: vec![SegmentGcRecordRange {
                            offset: 0,
                            len: bytes,
                        }],
                    }],
                )
                .unwrap();
            batch.write().unwrap();
        }
        store.index().flush_wal(true).unwrap();

        let _claim = store
            .store
            .gc_claims
            .try_claim(BTreeSet::from([larger_segment_id]))
            .unwrap();
        let prepared = store
            .prepare_gc_plan(&GcPlanner::new(GcPlannerConfig::default()))
            .unwrap()
            .unwrap();

        assert_eq!(
            prepared.plan.actions,
            vec![GcAction::DeleteSegment {
                segment_id: smaller_segment_id
            }]
        );
    }

    #[tokio::test]
    async fn gc_prepare_plan_defers_when_accounting_lag_exceeds_configured_limit() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.gc_max_accounting_lag_lsn = Some(2);
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let mut batch = store.index().batch();
        store.index().put_durable_lsn_batch(&mut batch, 5).unwrap();
        store
            .index()
            .put_accounted_lsn_batch(&mut batch, 2)
            .unwrap();
        batch.write().unwrap();

        let lag = store.gc_deferred_by_accounting_lag().unwrap().unwrap();
        assert_eq!(lag.durable_lsn, 5);
        assert_eq!(lag.accounted_lsn, 2);
        assert_eq!(lag.lag_lsn, 3);
        assert_eq!(lag.max_lag_lsn, Some(2));

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
    }

    #[tokio::test]
    async fn gc_prepare_plan_scans_real_segment_and_selects_live_records() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_c);

        let ref_a = store
            .index()
            .get_blob_version(&version_key(&key_a, lsn_a))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version(&version_key(&key_b, lsn_b))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

        assert!(prepared.accounting_snapshot.accounted_lsn() >= tombstone_lsn);
        assert_eq!(prepared.plan.scenario, GcScenario::L0Compaction);
        assert_eq!(prepared.plan.copied_bytes, ref_b.len);

        let selection = prepared.copy_selection.as_ref().unwrap();
        assert_eq!(selection.copied_bytes, ref_b.len);
        assert_eq!(selection.records.len(), 1);
        assert_eq!(selection.records[0].key, key_b);
        assert_eq!(selection.records[0].payload_lsn, lsn_b);
        assert_eq!(selection.records[0].from, ref_b);
        assert_eq!(
            selection.records[0].destination_class,
            DestinationClass::Spillover
        );

        assert_eq!(
            store
                .index()
                .get_segment_gc_overlay(ref_a.segment_id)
                .unwrap()
                .unwrap()
                .retired,
            vec![gc_range(ref_a)]
        );

        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        assert_eq!(copied.plan.scenario, GcScenario::L0Compaction);
        assert_eq!(copied.outputs.len(), 1);
        assert_eq!(copied.copied_records.len(), 1);
        let output = &copied.outputs[0];
        assert_eq!(output.destination_class, DestinationClass::Spillover);
        assert_eq!(output.placement_class, PlacementClass::Spillover);
        assert_eq!(output.sealed_len, ref_b.len);
        assert!(output.path.exists());

        let copied_record = &copied.copied_records[0];
        assert_eq!(copied_record.source.from, ref_b);
        assert_eq!(copied_record.staged.segment_id, output.staged_segment_id);
        assert_eq!(copied_record.staged.offset, 0);
        assert_eq!(copied_record.staged.len, ref_b.len);

        let mut staged_reader =
            strata_segment::SegmentReader::open(&output.path, output.staged_segment_id).unwrap();
        assert_eq!(
            staged_reader.read_payload(copied_record.staged).unwrap(),
            b"payload-b"
        );
    }

    #[tokio::test]
    async fn gc_publish_empty_delete_plan_deletes_segment_file() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        store.sync().unwrap();
        let sealed_state =
            wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        let sealed_path = segment_state_path(store.config(), &sealed_state);
        assert!(sealed_path.exists());
        wait_for_accounted_lsn(&store, lsn_b);

        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

        assert_eq!(prepared.accounting_snapshot.accounted_lsn(), tombstone_lsn);
        assert_eq!(prepared.plan.scenario, GcScenario::EmptyDelete);
        assert_eq!(
            prepared.plan.actions,
            vec![GcAction::DeleteSegment {
                segment_id: FIRST_SEGMENT_ID
            }]
        );
        assert!(prepared.copy_selection.is_none());

        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.output_segments.is_empty());
        assert!(published.published_records.is_empty());
        assert!(published.skipped_records.is_empty());
        assert_eq!(
            store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::Deleted
        );
        assert!(!sealed_path.exists());
        assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
        assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(store.get(&key_a).unwrap(), None);
        assert!(lsn_a < lsn_b);
    }

    #[tokio::test]
    async fn gc_worker_request_runs_production_gc_plan() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        store.sync().unwrap();
        let sealed_state =
            wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        let sealed_path = segment_state_path(store.config(), &sealed_state);
        wait_for_accounted_lsn(&store, lsn_b);

        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);
        store.request_gc().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let state = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap();
            if state.state == SegmentFileState::Deleted && !sealed_path.exists() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gc worker did not delete segment and remove file"
            );
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(store.get(&key_a).unwrap(), None);
        assert!(lsn_a < lsn_b);
    }

    #[tokio::test]
    async fn gc_worker_count_broadcasts_request_to_parallel_workers() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        cfg.gc_worker_count = 2;
        cfg.gc_initial_worker_count = 2;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let _lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        store.sync().unwrap();
        let first_state =
            wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        let second_segment_id = FIRST_SEGMENT_ID + 1;
        let second_state =
            wait_for_segment_state(store.index(), second_segment_id, SegmentFileState::Sealed);
        store.sync().unwrap();
        let first_path = segment_state_path(store.config(), &first_state);
        let second_path = segment_state_path(store.config(), &second_state);
        wait_for_accounted_lsn(&store, lsn_c);

        store.tombstone(&key_a).unwrap();
        let tombstone_lsn_b = store.tombstone(&key_b).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn_b);
        assert_eq!(store.store.gc_txs.len(), 2);

        store.request_gc().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let first_deleted = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .state
                == SegmentFileState::Deleted;
            let second_deleted = store
                .index()
                .get_segment_state(second_segment_id)
                .unwrap()
                .unwrap()
                .state
                == SegmentFileState::Deleted;
            if first_deleted && second_deleted && !first_path.exists() && !second_path.exists() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "two-worker GC request did not delete both empty source segments"
            );
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(store.get(&key_a).unwrap(), None);
        assert_eq!(store.get(&key_b).unwrap(), None);
        assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));
        assert!(lsn_b < lsn_c);
    }

    #[tokio::test]
    async fn gc_publish_reclassify_plan_updates_segment_placement() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();
        let current_epoch = store.current_epoch().unwrap();
        let segment_id = 10;
        let range = SegmentGcRecordRange {
            offset: 0,
            len: TEST_RECORD_LEN,
        };
        let state = SegmentState {
            shard: STORE_SCOPE,
            segment_id,
            volume_id: 0,
            path: format!("ingest/{segment_id:012}.data"),
            placement_class: PlacementClass::ExactEpoch(current_epoch),
            state: SegmentFileState::Sealed,
            write_offset: TEST_RECORD_LEN,
            durable_offset: TEST_RECORD_LEN,
            min_lsn: Some(0),
            max_lsn: Some(0),
            sealed_len: Some(TEST_RECORD_LEN),
            sealed_sha256: None,
        };
        let mut batch = store.index().batch();
        store
            .index()
            .put_segment_state_batch(&mut batch, &state)
            .unwrap();
        store
            .index()
            .merge_segment_gc_overlay_batch(
                &mut batch,
                segment_id,
                vec![SegmentGcOverlayMergeOp::AddLiveBatch {
                    records: vec![SegmentGcLiveRecord {
                        range,
                        lifecycle: Some(BlobLifecycle {
                            logical_end_epoch: current_epoch + 10,
                            extension_count: 2,
                        }),
                    }],
                }],
            )
            .unwrap();
        batch.write().unwrap();
        store.index().flush_wal(true).unwrap();

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: 1,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

        assert_eq!(prepared.plan.scenario, GcScenario::PinnedEpochExpiry);
        assert_eq!(
            prepared.plan.actions,
            vec![GcAction::ReclassifySegment {
                segment_id,
                placement_class: PlacementClass::Spillover,
            }]
        );
        assert!(prepared.copy_selection.is_none());

        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.output_segments.is_empty());
        assert!(published.published_records.is_empty());
        assert!(published.skipped_records.is_empty());
        assert_eq!(
            store
                .index()
                .get_segment_state(segment_id)
                .unwrap()
                .unwrap()
                .placement_class,
            PlacementClass::Spillover
        );
    }

    #[tokio::test]
    async fn gc_publish_maps_surviving_copied_record_to_output_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_c);

        let ref_a = store
            .index()
            .get_blob_version(&version_key(&key_a, lsn_a))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version(&version_key(&key_b, lsn_b))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        let staged_path = copied.outputs[0].path.clone();

        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.reconciled_accounted_lsn >= tombstone_lsn);
        assert_eq!(published.skipped_records, Vec::new());
        assert_eq!(published.output_segments.len(), 1);
        assert_eq!(published.published_records.len(), 1);
        assert!(!staged_path.exists());
        assert!(published.output_segments[0].path.exists());

        let published_record = &published.published_records[0];
        assert_eq!(published_record.source.from, ref_b);
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_b)
                .unwrap()
                .unwrap()
                .record_ref,
            Some(published_record.to)
        );
        assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(
            store
                .index()
                .get_segment_state(published_record.to.segment_id)
                .unwrap()
                .unwrap()
                .placement_class,
            PlacementClass::Spillover
        );
        assert!(store.durable_lsn().unwrap() >= published_record.publish_lsn);

        wait_for_accounted_lsn(&store, published_record.publish_lsn);

        let source_overlay = store
            .index()
            .get_segment_gc_overlay(ref_a.segment_id)
            .unwrap()
            .unwrap();
        assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
        assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

        let output_summary = segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary.live_bytes, ref_b.len);
        assert_eq!(output_summary.live_ref_count, 1);
        assert_eq!(output_summary.total_bytes, ref_b.len);
    }

    #[tokio::test]
    async fn gc_publish_pre_commit_failure_removes_renamed_output_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 4 - 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let key_d = BlobKey::new(b"blob-d".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        let lsn_d = store.put(&key_d, b"payload-d").unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_d);

        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        assert_eq!(copied.copied_records.len(), 2);
        assert_eq!(copied.outputs.len(), 1);
        let staged_path = copied.outputs[0].path.clone();
        let output_segment_id = store
            .index()
            .iter_segment_states()
            .unwrap()
            .into_iter()
            .map(|(segment_id, _)| segment_id)
            .max()
            .unwrap()
            .checked_add(1)
            .unwrap();
        let final_path = segment_path(store.config(), output_segment_id);

        let mut batch = store.index().batch();
        store
            .index()
            .put_next_lsn_batch(&mut batch, StrataLsn::MAX)
            .unwrap();
        batch.write().unwrap();
        store.index().flush_wal(true).unwrap();

        let err = store.publish_prepared_gc_copy(copied).unwrap_err();

        assert!(matches!(
            err,
            Error::Segment(strata_segment::Error::RangeOverflow)
        ));
        assert!(!staged_path.exists());
        assert!(!final_path.exists());
        assert!(
            store
                .index()
                .get_segment_state(output_segment_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));
        assert!(lsn_b < lsn_c);
    }

    #[tokio::test]
    async fn gc_publish_tombstoned_unaccounted_copy_retires_destination_after_forwarding() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
        cfg.accounting_interval = Duration::from_secs(3600);
        cfg.accounting_sidecar_major_patch_count_threshold = 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_c);

        let ref_a = store
            .index()
            .get_blob_version(&version_key(&key_a, lsn_a))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version(&version_key(&key_b, lsn_b))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_a_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        stop_accounting_worker(&mut store.store);

        let tombstone_b_lsn = store.tombstone(&key_b).unwrap();
        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.reconciled_accounted_lsn < tombstone_b_lsn);
        assert_eq!(published.published_records.len(), 1);
        assert_eq!(store.get(&key_b).unwrap(), None);

        let published_record = &published.published_records[0];
        assert_eq!(published_record.source.from, ref_b);
        let output_summary_before_accounting =
            segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary_before_accounting.total_bytes, ref_b.len);
        assert_eq!(output_summary_before_accounting.live_bytes, ref_b.len);
        assert_eq!(output_summary_before_accounting.live_ref_count, 1);
        assert_eq!(output_summary_before_accounting.garbage_bytes(), 0);
        assert!(store.index().get_gc_relocation(ref_b).unwrap().is_some());

        for _ in 0..4 {
            accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
            if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
                break;
            }
        }
        assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

        let source_overlay = store
            .index()
            .get_segment_gc_overlay(ref_a.segment_id)
            .unwrap()
            .unwrap();
        assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
        assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

        let output_summary = segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary.total_bytes, ref_b.len);
        assert_eq!(output_summary.live_bytes, 0);
        assert_eq!(output_summary.live_ref_count, 0);
        assert_eq!(output_summary.retired_bytes, ref_b.len);
        assert_eq!(output_summary.garbage_bytes(), ref_b.len);
        assert!(store.index().get_gc_relocation(ref_b).unwrap().is_none());
    }

    #[tokio::test]
    async fn gc_publish_unaccounted_epoch_change_expires_relocated_destination() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
        cfg.accounting_interval = Duration::from_secs(3600);
        cfg.accounting_sidecar_major_patch_count_threshold = 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        let lifetime_b_lsn = store.extend(&key_b, 43).unwrap().unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_c.max(lifetime_b_lsn));

        let ref_a = store
            .index()
            .get_blob_version(&version_key(&key_a, lsn_a))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version(&version_key(&key_b, lsn_b))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_a_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        stop_accounting_worker(&mut store.store);

        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        assert_eq!(store.get(&key_b).unwrap(), None);
        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.reconciled_accounted_lsn < epoch_lsn);
        assert_eq!(published.published_records.len(), 1);
        let published_record = &published.published_records[0];
        assert_eq!(published_record.source.from, ref_b);
        assert_eq!(
            published_record.source.lifecycle.unwrap().logical_end_epoch,
            43
        );

        let output_summary_before_accounting =
            segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary_before_accounting.live_bytes, ref_b.len);
        assert_eq!(output_summary_before_accounting.expired_bytes, 0);

        for _ in 0..4 {
            accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
            if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
                break;
            }
        }
        assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

        let source_overlay = store
            .index()
            .get_segment_gc_overlay(ref_a.segment_id)
            .unwrap()
            .unwrap();
        assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
        assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

        let output_summary = segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary.total_bytes, ref_b.len);
        assert_eq!(output_summary.live_bytes, 0);
        assert_eq!(output_summary.live_ref_count, 0);
        assert_eq!(output_summary.expired_bytes, ref_b.len);
        assert_eq!(output_summary.retired_bytes, 0);
        assert_eq!(output_summary.garbage_bytes(), ref_b.len);
    }

    #[tokio::test]
    async fn gc_publish_forwards_lagging_lifetime_before_epoch_expiry() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
        cfg.accounting_interval = Duration::from_secs(3600);
        cfg.accounting_sidecar_major_patch_count_threshold = 1;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        let lsn_a = store.put(&key_a, b"payload-a").unwrap();
        let lsn_b = store.put(&key_b, b"payload-b").unwrap();
        let lsn_c = store.put(&key_c, b"payload-c").unwrap();
        store.sync().unwrap();
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lsn_c);

        let ref_a = store
            .index()
            .get_blob_version(&version_key(&key_a, lsn_a))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_version(&version_key(&key_b, lsn_b))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_a_lsn);

        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
            min_reclaim_bytes: 1,
            min_garbage_ratio_bps: 1,
            min_exact_epoch_bucket_bytes: 1,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 1,
            max_join_sources: 4,
        });
        let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
        assert_eq!(
            prepared.copy_selection.as_ref().unwrap().records[0].lifecycle,
            None
        );
        let copied = store.copy_prepared_gc_plan(prepared).unwrap();
        stop_accounting_worker(&mut store.store);

        let lifetime_b_lsn = store.extend(&key_b, 43).unwrap().unwrap();
        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        assert_eq!(store.get(&key_b).unwrap(), None);
        let published = store.publish_prepared_gc_copy(copied).unwrap();

        assert!(published.reconciled_accounted_lsn < lifetime_b_lsn);
        assert!(published.reconciled_accounted_lsn < epoch_lsn);
        assert_eq!(published.published_records.len(), 1);
        let published_record = &published.published_records[0];
        assert_eq!(published_record.source.from, ref_b);
        assert_eq!(published_record.source.lifecycle, None);

        let output_summary_before_accounting =
            segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary_before_accounting.live_bytes, ref_b.len);
        assert_eq!(output_summary_before_accounting.expired_bytes, 0);

        for _ in 0..4 {
            accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
            if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
                break;
            }
        }
        assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

        let source_overlay = store
            .index()
            .get_segment_gc_overlay(ref_a.segment_id)
            .unwrap()
            .unwrap();
        assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
        assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

        let output_summary = segment_summary(store.index(), published_record.to.segment_id);
        assert_eq!(output_summary.total_bytes, ref_b.len);
        assert_eq!(output_summary.live_bytes, 0);
        assert_eq!(output_summary.live_ref_count, 0);
        assert_eq!(output_summary.expired_bytes, ref_b.len);
        assert_eq!(output_summary.retired_bytes, 0);
        assert_eq!(output_summary.garbage_bytes(), ref_b.len);
    }

    #[tokio::test]
    async fn accounting_publishes_lifecycle_changed_segment_ref_event() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();

        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let lifecycle = BlobLifecycle {
            logical_end_epoch: 50,
            extension_count: 0,
        };
        assert_eq!(
            store
                .index()
                .iter_segment_ref_events_since(record_ref.segment_id, put_lsn)
                .unwrap(),
            vec![(
                strata_core::SegmentRefEventKey {
                    segment_id: record_ref.segment_id,
                    lsn: lifetime_lsn,
                    offset: record_ref.offset,
                },
                SegmentRefEvent::LifecycleChanged {
                    lifecycle: Some(lifecycle),
                },
            )]
        );
        let overlay = store
            .index()
            .get_segment_gc_overlay(record_ref.segment_id)
            .unwrap()
            .unwrap();
        assert!(overlay.expired.is_empty());
        assert!(overlay.retired.is_empty());
        assert_eq!(
            overlay.lifetimes,
            vec![SegmentGcLifetimeRange {
                range: gc_range(record_ref),
                lifecycle,
            }]
        );
    }

    #[tokio::test]
    async fn accounting_gc_overlay_retire_removes_lifetime_hint() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        let put_lsn = store.put(&key, b"payload").unwrap();
        let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
        let record_ref = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let overlay = store
            .index()
            .get_segment_gc_overlay(record_ref.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(overlay.lifetimes.len(), 1);

        let tombstone_lsn = store.tombstone(&key).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let overlay = store
            .index()
            .get_segment_gc_overlay(record_ref.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
        assert!(overlay.expired.is_empty());
        assert!(overlay.lifetimes.is_empty());
    }

    #[tokio::test]
    async fn accounting_updates_gc_summary_on_epoch_change() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key_a, b"payload-a").unwrap();
        store.put(&key_b, b"payload-bb").unwrap();
        store.extend(&key_a, 43).unwrap().unwrap();
        let lifetime_b_lsn = store.extend(&key_b, 50).unwrap().unwrap();
        let ref_a = store
            .index()
            .get_blob_entry(&key_a)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let ref_b = store
            .index()
            .get_blob_entry(&key_b)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_b_lsn);

        let stats = segment_summary(store.index(), ref_a.segment_id);
        assert_eq!(stats.live_bytes, ref_a.len + ref_b.len);
        assert_eq!(stats.live_ref_count, 2);
        assert_eq!(stats.expired_bytes, 0);

        let (epoch, epoch_lsn) = store.increment_epoch().unwrap();
        assert_eq!(epoch, 43);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, epoch_lsn);

        let stats = segment_summary(store.index(), ref_a.segment_id);
        assert_eq!(stats.live_bytes, ref_b.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(stats.expired_bytes, ref_a.len);
        assert_eq!(stats.retired_bytes, 0);
        assert_eq!(stats.future_epoch_histogram.get(&43), None);
        assert_eq!(
            stats.future_epoch_histogram.get(&50),
            Some(&EpochBucket {
                refs: 1,
                bytes: ref_b.len,
            })
        );
        assert_eq!(stats.min_live_end_epoch, Some(50));
        assert!(!stats.is_empty());

        let mut last_epoch_lsn = epoch_lsn;
        while store.current_epoch().unwrap() < 50 {
            let (_, lsn) = store.increment_epoch().unwrap();
            last_epoch_lsn = lsn;
        }
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, last_epoch_lsn);

        let stats = segment_summary(store.index(), ref_a.segment_id);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
        assert_eq!(stats.expired_bytes, ref_a.len + ref_b.len);
        assert_eq!(stats.total_bytes, ref_a.len + ref_b.len);
        assert!(stats.future_epoch_histogram.is_empty());
        assert_eq!(stats.min_live_end_epoch, None);
        assert!(stats.is_empty());
    }

    #[tokio::test]
    async fn accounting_skips_live_counters_for_tombstone_of_expired_blob() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, epoch_lsn);

        let tombstone_lsn = store.tombstone(&key).unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, tombstone_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.total_bytes, record_ref.len);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
        assert_eq!(stats.expired_bytes, 0);
        assert_eq!(stats.retired_bytes, record_ref.len);
        assert_eq!(store.get(&key).unwrap(), None);
    }

    #[tokio::test]
    async fn accounting_orders_blob_ops_and_epoch_changes_within_one_run() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key_a, b"payload-a").unwrap();
        store.extend(&key_a, 43).unwrap().unwrap();
        let ref_a = store
            .index()
            .get_blob_entry(&key_a)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.increment_epoch().unwrap();
        let tombstone_lsn = store.tombstone(&key_a).unwrap();
        let put_b_lsn = store.put(&key_b, b"payload-bb").unwrap();
        let lifetime_b_lsn = store.extend(&key_b, 44).unwrap().unwrap();
        let ref_b = store
            .index()
            .get_blob_entry(&key_b)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_b_lsn.max(put_b_lsn).max(tombstone_lsn));

        // The put of blob A is accounted live at epoch 42, the epoch change to 43 expires it, and
        // the later tombstone moves it from expired bytes to permanently retired bytes.
        let stats = segment_summary(store.index(), ref_a.segment_id);
        assert_eq!(stats.total_bytes, ref_a.len + ref_b.len);
        assert_eq!(stats.expired_bytes, 0);
        assert_eq!(stats.retired_bytes, ref_a.len);
        assert_eq!(stats.live_bytes, ref_b.len);
        assert_eq!(stats.live_ref_count, 1);
        assert_eq!(
            stats.future_epoch_histogram.get(&44),
            Some(&EpochBucket {
                refs: 1,
                bytes: ref_b.len,
            })
        );
    }

    #[tokio::test]
    async fn accounting_does_not_revive_expired_blob_on_extension() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, epoch_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.expired_bytes, record_ref.len);
        assert_eq!(stats.live_ref_count, 0);

        let extend_lsn = store.extend(&key, 50).unwrap().unwrap();
        assert_eq!(store.get(&key).unwrap(), None);
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, extend_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.expired_bytes, record_ref.len);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
        assert_eq!(stats.future_epoch_histogram.get(&50), None);
        assert_eq!(stats.min_live_end_epoch, None);
    }

    #[tokio::test]
    async fn accounting_expires_future_epoch_bucket_for_exact_epoch_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();

        store.put(&key, b"payload").unwrap();
        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let mut state = store
            .index()
            .get_segment_state(record_ref.segment_id)
            .unwrap()
            .unwrap();
        state.placement_class = PlacementClass::ExactEpoch(42);
        store.index().put_segment_state(&state).unwrap();
        let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, lifetime_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(
            stats.future_epoch_histogram.get(&43),
            Some(&EpochBucket {
                refs: 1,
                bytes: record_ref.len,
            })
        );

        let (_, epoch_lsn) = store.increment_epoch().unwrap();
        store.sync().unwrap();
        wait_for_accounted_lsn(&store, epoch_lsn);

        let stats = segment_summary(store.index(), record_ref.segment_id);
        assert_eq!(stats.future_epoch_histogram.get(&43), None);
        assert_eq!(stats.expired_bytes, record_ref.len);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
    }

    #[tokio::test]
    async fn put_assigns_monotonic_lsn_across_reopen() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            let lsn_1 = store.put(&key_1, b"payload-a").unwrap();
            let lsn_2 = store.put(&key_2, b"payload-b").unwrap();

            assert_eq!(lsn_1, 1);
            assert_eq!(lsn_2, 2);
            assert_eq!(
                store
                    .index()
                    .get_blob_entry(&key_1)
                    .unwrap()
                    .unwrap()
                    .record_ref
                    .unwrap()
                    .segment_id,
                1
            );
            assert_eq!(
                store
                    .index()
                    .get_blob_entry(&key_2)
                    .unwrap()
                    .unwrap()
                    .record_ref
                    .unwrap()
                    .segment_id,
                2
            );
            assert_eq!(
                store.index().get_blob_entry(&key_2).unwrap().unwrap().lsn,
                2
            );
            assert_eq!(store.index().get_next_lsn().unwrap(), 3);
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        let lsn_3 = store.put(&key_3, b"x").unwrap();

        assert_eq!(lsn_3, 3);
        assert_eq!(store.index().get_next_lsn().unwrap(), 4);
        let active_state = store.index().get_segment_state(2).unwrap().unwrap();
        assert_eq!(active_state.min_lsn, Some(2));
        assert_eq!(active_state.max_lsn, Some(3));
    }

    #[tokio::test]
    async fn recovery_rolls_back_ops_missing_from_active_delta_log() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            assert_eq!(store.put(&key, b"payload-a").unwrap(), 1);
            assert_eq!(store.index().get_next_lsn().unwrap(), 2);
        }

        std::fs::remove_file(cfg.accounting_index_dir().join("active-delta.log")).unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.index().get_next_lsn().unwrap(), 1);
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.put(&key, b"payload-b").unwrap(), 1);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
    }

    #[tokio::test]
    async fn recovery_keeps_latest_valid_blob_version() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let second;
        let second_lsn;
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key, b"payload-a").unwrap();
            second_lsn = store.put(&key, b"payload-b").unwrap();
            second = store
                .index()
                .get_blob_entry(&key)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_version(&version_key(&key, second_lsn))
                .unwrap()
                .unwrap()
                .record_ref,
            Some(second)
        );
        assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
    }

    #[tokio::test]
    async fn point_in_time_recovery_discards_higher_segments_after_lower_gap() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let first_end;
        let second_end;
        {
            let index =
                StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
            fs::create_dir_all(cfg.ingest_dir()).unwrap();

            let segment_1_path = segment_path(&cfg, 1);
            let mut segment_1 =
                SegmentWriter::create(&segment_1_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_a = segment_1.append(&key_a, 1, b"payload-a").unwrap();
            first_end = segment_1.write_offset();
            let out_b = segment_1.append(&key_b, 2, b"payload-b").unwrap();
            second_end = segment_1.write_offset();
            drop(segment_1);
            OpenOptions::new()
                .write(true)
                .open(&segment_1_path)
                .unwrap()
                .set_len(first_end)
                .unwrap();

            let segment_2_path = segment_path(&cfg, 2);
            let mut segment_2 =
                SegmentWriter::create(&segment_2_path, 2, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_c = segment_2.append(&key_c, 3, b"payload-c").unwrap();
            let segment_2_end = segment_2.write_offset();
            drop(segment_2);

            let mut segment_1_state =
                active_segment_state_from_path(&cfg, STORE_SCOPE, 1, second_end, 0);
            segment_1_state.state = SegmentFileState::Sealing;
            let segment_2_state =
                active_segment_state_from_path(&cfg, STORE_SCOPE, 2, segment_2_end, 0);

            let mut active_delta_log =
                ActiveDeltaLog::open(cfg.accounting_index_dir(), ActiveDeltaLogState::default())
                    .unwrap();
            for (key, record_ref, lsn) in [
                (&key_a, out_a.record_ref, 1),
                (&key_b, out_b.record_ref, 2),
                (&key_c, out_c.record_ref, 3),
            ] {
                active_delta_log
                    .append(&AccountingDelta::Blob(BlobUpdate::Put {
                        lsn,
                        key: key.clone(),
                        shard: STORE_SCOPE,
                        record_ref,
                        current_epoch: 42,
                        lifecycle: None,
                    }))
                    .unwrap();
            }
            active_delta_log.sync_data().unwrap();

            let mut batch = index.batch();
            for (key, record_ref, lsn) in [
                (&key_a, out_a.record_ref, 1),
                (&key_b, out_b.record_ref, 2),
                (&key_c, out_c.record_ref, 3),
            ] {
                index
                    .put_blob_version_batch(
                        &mut batch,
                        key,
                        &BlobEntry {
                            record_ref: Some(record_ref),
                            lsn,
                            generation: lsn,
                            state: BlobState::Live,
                        },
                    )
                    .unwrap();
                index
                    .put_blob_unaccounted_lsn_op_batch(&mut batch, lsn, key)
                    .unwrap();
            }
            index
                .put_segment_state_batch(&mut batch, &segment_1_state)
                .unwrap();
            index
                .put_segment_state_batch(&mut batch, &segment_2_state)
                .unwrap();
            index
                .put_accounting_active_delta_log_state_batch(&mut batch, active_delta_log.state())
                .unwrap();
            batch.write().unwrap();
            index.flush_wal(true).unwrap();
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        assert_eq!(store.get(&key_a).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_b).unwrap(), None);
        assert_eq!(store.get(&key_c).unwrap(), None);
        assert_eq!(
            store
                .index()
                .get_segment_state(1)
                .unwrap()
                .unwrap()
                .write_offset,
            first_end
        );
        assert_eq!(
            store.index().get_segment_state(2).unwrap().unwrap().state,
            SegmentFileState::Deleted
        );
        assert_eq!(
            store.index().get_blob_entry(&key_a).unwrap().unwrap().state,
            BlobState::Live
        );
        assert_eq!(store.index().get_blob_entry(&key_b).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key_c).unwrap(), None);
    }

    #[tokio::test]
    async fn absolute_consistency_recovery_fails_on_unsealed_gap() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let first_end;
        let second_end;
        {
            let index =
                StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
            fs::create_dir_all(cfg.ingest_dir()).unwrap();
            let segment_path = segment_path(&cfg, 1);
            let mut segment =
                SegmentWriter::create(&segment_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_a = segment.append(&key_a, 1, b"payload-a").unwrap();
            first_end = segment.write_offset();
            let out_b = segment.append(&key_b, 2, b"payload-b").unwrap();
            second_end = segment.write_offset();
            drop(segment);
            OpenOptions::new()
                .write(true)
                .open(&segment_path)
                .unwrap()
                .set_len(first_end)
                .unwrap();

            let state = active_segment_state_from_path(&cfg, STORE_SCOPE, 1, second_end, 0);
            let mut batch = index.batch();
            for (key, record_ref, lsn) in
                [(&key_a, out_a.record_ref, 1), (&key_b, out_b.record_ref, 2)]
            {
                index
                    .put_blob_version_batch(
                        &mut batch,
                        key,
                        &BlobEntry {
                            record_ref: Some(record_ref),
                            lsn,
                            generation: lsn,
                            state: BlobState::Live,
                        },
                    )
                    .unwrap();
                index
                    .put_blob_unaccounted_lsn_op_batch(&mut batch, lsn, key)
                    .unwrap();
            }
            index.put_segment_state_batch(&mut batch, &state).unwrap();
            batch.write().unwrap();
            index.flush_wal(true).unwrap();
        }

        let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();

        assert!(matches!(
            err,
            Error::RecoveryInconsistent {
                segment_id: 1,
                expected_write_offset,
                recovered_write_offset,
            } if expected_write_offset == second_end && recovered_write_offset == first_end
        ));
    }

    #[tokio::test]
    async fn unsealed_segment_count_includes_open_sealing_and_failed_segments() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path().join("index"), "strata/default").unwrap();

        put_test_segment_state(&index, 1, SegmentFileState::Open);
        put_test_segment_state(&index, 2, SegmentFileState::Sealing);
        put_test_segment_state(&index, 3, SegmentFileState::SealFailed);
        put_test_segment_state(&index, 4, SegmentFileState::Sealed);

        assert_eq!(unsealed_ingest_segment_ids(&index).unwrap(), vec![1, 2, 3]);
        assert_eq!(unsealed_ingest_segment_count(&index).unwrap(), 3);
        assert_eq!(first_seal_failed_segment(&index).unwrap(), Some(3));
    }

    #[tokio::test]
    async fn reopen_detects_missing_sealed_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        seal_first_segment(&cfg);

        std::fs::remove_file(segment_path(&cfg, FIRST_SEGMENT_ID)).unwrap();

        let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentMissing {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn reopen_detects_sealed_segment_length_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let sealed = seal_first_segment(&cfg);
        let sealed_len = sealed.sealed_len.unwrap();
        assert!(sealed_len > 0);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(sealed_len - 1)
            .unwrap();

        let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentLengthMismatch {
                segment_id: FIRST_SEGMENT_ID,
                expected_len,
                actual_len,
                ..
            } if expected_len == sealed_len && actual_len == sealed_len - 1
        ));
    }

    #[tokio::test]
    async fn metadata_only_reopen_does_not_hash_sealed_segment_bytes() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::MetadataOnly;
        seal_first_segment(&cfg);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .write_all(b"X")
            .unwrap();

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        assert_eq!(
            store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::Sealed
        );
    }

    #[tokio::test]
    async fn checksum_reopen_detects_sealed_segment_hash_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::Checksum;
        seal_first_segment(&cfg);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .write_all(b"X")
            .unwrap();

        let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentChecksumMismatch {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn seal_worker_marks_segment_failed_on_seal_error() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let index =
            StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
        put_test_segment_state(&index, 1, SegmentFileState::Sealing);
        let (_seal_tx, seal_rx) = mpsc::channel();
        let (accounting_tx, _accounting_rx) = mpsc::sync_channel(1);
        let worker = SealWorker {
            config: cfg,
            index: index.clone(),
            store_scope: STORE_SCOPE,
            seal_rx,
            accounting_tx,
            metrics: StrataStoreMetrics::default(),
        };

        assert!(
            worker
                .seal_segment(SegmentSealTask {
                    segment_id: 1,
                    sealed_len: 64,
                })
                .is_err()
        );
        worker.mark_seal_failed(1).unwrap();

        let state = index.get_segment_state(1).unwrap().unwrap();
        assert_eq!(state.state, SegmentFileState::SealFailed);
    }

    #[tokio::test]
    async fn rollover_switches_active_segment_and_seal_worker_seals_old_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

        store.put(&key_1, b"payload-a").unwrap();
        store.put(&key_2, b"payload-b").unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_1)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            1
        );
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_2)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            2
        );
        assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));

        let sealed = wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
        assert_eq!(sealed.durable_offset, sealed.write_offset);
        assert_eq!(sealed.sealed_len, Some(sealed.write_offset));
        assert_eq!(
            sealed.sealed_sha256,
            Some(
                seal::sha256_file_prefix(&segment_path(store.config(), 1), sealed.write_offset)
                    .unwrap()
            )
        );
        assert_eq!(store.durable_lsn().unwrap(), 0);

        let open = store.index().get_segment_state(2).unwrap().unwrap();
        assert_eq!(open.state, SegmentFileState::Open);
        assert_eq!(open.sealed_sha256, None);
        let open_segment_ids = store
            .index()
            .iter_segment_states()
            .unwrap()
            .into_iter()
            .filter(|(_, state)| {
                state.placement_class == PlacementClass::Ingest
                    && state.state == SegmentFileState::Open
            })
            .map(|(segment_id, _)| segment_id)
            .collect::<Vec<_>>();
        assert_eq!(open_segment_ids, vec![2]);

        store.sync().unwrap();
        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn reopen_after_rollover_appends_to_highest_open_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
        {
            let store =
                try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
            store.put(&key_1, b"payload-a").unwrap();
            store.put(&key_2, b"payload-b").unwrap();
            wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
        }

        let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
        store.put(&key_3, b"x").unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_3)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            2
        );
        assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(store.get(&key_3).unwrap(), Some(b"x".to_vec()));
    }
}
