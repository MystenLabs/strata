//! Store construction behind `StrataStore::open`: config validation, index and
//! LSM and WAL opening, crash-recovery orchestration, and worker spawning.

use std::{
    fs,
    num::NonZeroU32,
    sync::{Arc, Mutex, RwLock, atomic::AtomicU64, mpsc},
    thread::{self, JoinHandle},
    time::Instant,
};

use strata_core::{
    Epoch, PlacementClass, SegmentFileState, SegmentGcSummary, SegmentId, ShardInfo,
    StoreCheckpoint, StrataLsn, WalPosition,
};
use strata_index::StrataIndex;
use strata_lsm::{
    Lsm, LsmOptions, Manifest as LsmManifest, MemtableRolloverPolicy, Mutation as LsmMutation,
};
use strata_segment::{SegmentFactory, SegmentIdAllocator, SegmentIoObserver, SegmentWriter};

use crate::{
    BLOB_LSM_MANIFEST, DEFAULT_RELOCATION_CACHE_ENTRIES, Error, FIRST_SEGMENT_ID, GcPlanner,
    INGEST_SEGMENT_OWNER, LSM_BASE_FORMAT, LSM_FILE_SYNC_WORKERS, LSM_MEMTABLE_MAX_AGE,
    LSM_MEMTABLE_MAX_KEYS, LSM_PATCH_FORMAT, RELOCATION_LSM_BASE_FORMAT, RELOCATION_LSM_MANIFEST,
    RELOCATION_LSM_PATCH_FORMAT, RETIRED_PROJECTION_DIR, Result, STANDALONE_SHARD, StoreHalt,
    StrataStore, StrataStoreConfig, StrataStoreMetrics, WriteCoordinator,
    file_sync::file_sync_channel,
    fs_util::{sync_parent_dir, unlink_gc_segment_file},
    gc::{
        GcCommand, GcConcurrencyConfig, GcConcurrencyController, GcExecutor, GcSourceClaims,
        GcWorker,
    },
    gc_rate_limiter::GcIoLimiter,
    layout::segment_path,
    maintenance::{GarbageLogSweeper, LsmCompactor, LsmFlusher, garbage_log_dir},
    reader_cache::SegmentReaderCache,
    recovery::{
        publish_recovered_store_checkpoint, reconcile_orphan_ingest_segment_files,
        recover_store_wal_prefix, recover_unsealed_segments,
    },
    relocation::{RelocationCache, RelocationEntry, RelocationStore},
    seal::{active_segment_durable_offset, seal_recovered_segments, verify_sealed_segments},
    segment_state::{
        SegmentAllocationTracker, publish_active_segment_state, unsealed_ingest_segment_count,
    },
    wal::Wal,
    wal_format::StoreWalMutation,
};

/// Aggregates authoritative per-segment GC summaries for metric initialization.
fn gc_known_summary(index: &StrataIndex) -> Result<SegmentGcSummary> {
    let mut total = SegmentGcSummary::default();
    for (segment_id, state) in index.iter_segment_states()? {
        if state.state == SegmentFileState::Deleted {
            continue;
        }
        let Some(summary) = index.get_segment_gc_summary(segment_id)? else {
            continue;
        };
        total.total_bytes = total.total_bytes.saturating_add(summary.total_bytes);
        total.live_bytes = total.live_bytes.saturating_add(summary.live_bytes);
        total.retired_bytes = total.retired_bytes.saturating_add(summary.retired_bytes);
        total.expired_bytes = total.expired_bytes.saturating_add(summary.expired_bytes);
        total.live_ref_count = total.live_ref_count.saturating_add(summary.live_ref_count);
    }
    Ok(total)
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
    /// 4. Only then are the workers started. The LSM flusher publishes immutable memtables, a
    ///    separate compactor merges durable tables, and GC publishes relocation L0s on its
    ///    independent path.
    ///
    /// Foreground segment/WAL state remains writer-owned; GC only shares the index, relocation
    /// LSM, and the small publication locks needed for atomic activation.
    fn open_inner(
        config: StrataStoreConfig,
        index: StrataIndex,
        metrics: StrataStoreMetrics,
    ) -> Result<Self> {
        validate_config(&config)?;
        cleanup_retired_projection_dir(&config)?;
        ensure_ingest_dir(&config)?;
        cleanup_stale_gc_staging_dirs(&config)?;
        ensure_default_shard_registered(&index)?;
        ensure_epoch_initialized(&index, config.starting_epoch)?;
        reconcile_orphan_ingest_segment_files(&config, &index)?;
        recover_unsealed_segments(&config, &index, &metrics)?;
        recover_store_wal_prefix(&config, &index, &metrics)?;
        ensure_blob_compaction_garbage_cutover(&index)?;
        let current_epoch = index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)?;
        cleanup_pending_gc_outputs(&config, &index)?;
        let active_segment_id = choose_active_segment_id(&index)?;
        seal_recovered_segments(&config, &index, active_segment_id, &metrics)?;
        verify_sealed_segments(&config, &index)?;
        let segment_ids =
            SegmentIdAllocator::new(next_segment_id_after(&index, active_segment_id)?);
        let segment_io_observer: Arc<dyn SegmentIoObserver> = Arc::new(metrics.clone());
        let active_writer =
            open_active_writer(&config, active_segment_id, Arc::clone(&segment_io_observer))?;
        let durable_offset = active_segment_durable_offset(&index, active_writer.segment_id())?;
        let next_lsn = index.get_next_lsn()?;
        let published_lsn = index.get_committed_lsn()?;
        let active_segment_state = publish_active_segment_state(
            &config,
            &index,
            INGEST_SEGMENT_OWNER,
            &active_writer,
            durable_offset,
            next_lsn,
        )?;
        metrics.set_active_segment(
            active_writer.segment_id(),
            active_writer.write_offset(),
            durable_offset,
        );
        metrics.set_lsn_state(next_lsn, published_lsn);
        let store_checkpoint = index.get_store_checkpoint()?;
        let (store_wal, blob_recovery, relocation_recovery, lsm_sync_handles) =
            open_store_wal(&config, &index, next_lsn, store_checkpoint)?;
        let lsm = open_lsm(&config, &index, next_lsn, blob_recovery)?;
        publish_recovered_store_checkpoint(&index, &metrics, &store_wal, &active_segment_state)?;
        let relocations = open_relocation_lsm(&config, &index, next_lsn, relocation_recovery)?;
        let durable_relocation_lsn = Arc::new(AtomicU64::new(relocations.manifest_sequence()));
        let relocation_cache = Arc::new(RelocationCache::new(DEFAULT_RELOCATION_CACHE_ENTRIES));
        let live_snapshots = lsm.live_snapshots();
        metrics.initialize_gc_known(&gc_known_summary(&index)?);
        metrics.set_gc_relocating_segments(gc_relocating_segment_count(&index)?);
        metrics.set_current_epoch(current_epoch);
        metrics.set_unsealed_segments(unsealed_ingest_segment_count(&index)?);
        let gc_publish_cleanup_lock = Arc::new(Mutex::new(()));
        let gc_wake_txs = Arc::new(Mutex::new(Vec::new()));
        let gc_claims = Arc::new(GcSourceClaims::default());
        let gc_io_limiter = Arc::new(GcIoLimiter::new(config.gc_io_bytes_per_sec));
        let store_halt = StoreHalt::default();
        let compaction_admission_lock = Arc::new(RwLock::new(()));
        let garbage_publish_lock = Arc::new(Mutex::new(()));
        let relocation_durability_lock = Arc::new(Mutex::new(()));
        let gc_concurrency = Arc::new(GcConcurrencyController::new(
            GcConcurrencyConfig::from_store_config(&config),
            metrics.clone(),
        ));
        let mut recovered_frozen_memtables = false;
        for partition in 0..config.lsm_partition_count {
            recovered_frozen_memtables |= !lsm.frozen_generations(partition)?.is_empty();
        }
        let (lsm_compact_tx, lsm_compact_rx) = mpsc::channel();
        let lsm_compactor = LsmCompactor {
            index: index.clone(),
            lsm: Arc::downgrade(&lsm),
            relocations: Arc::downgrade(&relocations),
            relocation_cache: Arc::downgrade(&relocation_cache),
            durable_relocation_lsn: Arc::clone(&durable_relocation_lsn),
            garbage_log_dir: garbage_log_dir(&config),
            compaction_admission_lock: Arc::clone(&compaction_admission_lock),
            garbage_publish_lock: Arc::clone(&garbage_publish_lock),
            wake_rx: lsm_compact_rx,
            store_halt: store_halt.clone(),
            metrics: metrics.clone(),
            obsolete: Vec::new(),
        };
        let lsm_compact_handle = thread::Builder::new()
            .name(format!("strata-lsm-compact-{}", config.namespace))
            .spawn(move || lsm_compactor.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let (lsm_flush_tx, lsm_flush_rx) = mpsc::channel();
        let lsm_flusher = LsmFlusher {
            index: index.clone(),
            lsm: Arc::downgrade(&lsm),
            wake_rx: lsm_flush_rx,
            compact_tx: lsm_compact_tx.clone(),
            store_halt: store_halt.clone(),
        };
        let lsm_flush_handle = thread::Builder::new()
            .name(format!("strata-lsm-flush-{}", config.namespace))
            .spawn(move || lsm_flusher.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        if recovered_frozen_memtables {
            lsm_flush_tx.send(()).map_err(|_| Error::StoreHalted {
                reason: "LSM flusher stopped during recovery".to_owned(),
            })?;
        }
        lsm_compact_tx.send(()).map_err(|_| Error::StoreHalted {
            reason: "LSM compactor stopped during recovery".to_owned(),
        })?;
        let (garbage_sweep_tx, garbage_sweep_rx) = mpsc::channel();
        let garbage_sweeper = GarbageLogSweeper {
            index: index.clone(),
            global_log_dir: garbage_log_dir(&config),
            namespace_dir: config.namespace_dir(),
            garbage_publish_lock: Arc::clone(&garbage_publish_lock),
            relocations: Arc::downgrade(&relocations),
            durable_relocation_lsn: Arc::clone(&durable_relocation_lsn),
            gc_txs: Arc::clone(&gc_wake_txs),
            shutdown_rx: garbage_sweep_rx,
        };
        let garbage_sweep_handle = thread::Builder::new()
            .name(format!("strata-garbage-sweeper-{}", config.namespace))
            .spawn(move || garbage_sweeper.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let (write_tx, write_rx) = mpsc::sync_channel(config.write_queue_capacity);
        let (durability_ready_tx, durability_ready_rx) = mpsc::channel();
        let reader_cache = Arc::new(SegmentReaderCache::new(
            config.segment_reader_cache_capacity,
            Arc::clone(&segment_io_observer),
        ));
        let segment_sync_tx = store_wal.file_sync_sender();
        let coordinator = WriteCoordinator {
            config: config.clone(),
            index: index.clone(),
            lsm: Arc::clone(&lsm),
            wal: store_wal,
            segment: active_writer,
            segment_factory: SegmentFactory::new(
                config.ingest_dir(),
                segment_ids.clone(),
                PlacementClass::Ingest,
                config.segment_max_bytes,
            )
            .with_io_observer(segment_io_observer),
            segment_sync_tx,
            pending_segment_syncs: Vec::new(),
            internal_write_tx: write_tx.clone(),
            sync_done_tx: durability_ready_tx,
            sync_done_rx: durability_ready_rx,
            sync_and_commit_in_flight: None,
            pending_sync_requests: Vec::new(),
            relocation_durability_lock: Arc::clone(&relocation_durability_lock),
            durable_relocation_lsn: Arc::clone(&durable_relocation_lsn),
            active_segment_state,
            durable_offset,
            active_allocation_records: 0,
            active_allocation_tracker: Arc::new(SegmentAllocationTracker::default()),
            pending_segment_bytes: 0,
            oldest_uncommitted_at: None,
            last_committed_at: Instant::now(),
            pending_rollovers: Vec::new(),
            lsm_flush_tx: lsm_flush_tx.clone(),
            lsm_compact_tx: lsm_compact_tx.clone(),
            write_rx,
            ingest_owner: INGEST_SEGMENT_OWNER,
            relocations: Arc::clone(&relocations),
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
                    publish_cleanup_lock: Arc::clone(&gc_publish_cleanup_lock),
                    garbage_publish_lock: Arc::clone(&garbage_publish_lock),
                    relocation_durability_lock: Arc::clone(&relocation_durability_lock),
                    compaction_admission_lock: Arc::clone(&compaction_admission_lock),
                    relocations: Arc::clone(&relocations),
                    relocation_cache: Arc::clone(&relocation_cache),
                    durable_relocation_lsn: Arc::clone(&durable_relocation_lsn),
                    live_snapshots: live_snapshots.clone(),
                    lsm_compact_tx: lsm_compact_tx.clone(),
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
                    gc_wake_txs
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
        if pending_shard_cleanup_lsn.is_some() {
            for gc_tx in &gc_txs {
                let _ = gc_tx.send(GcCommand::Run);
            }
        }

        Ok(Self {
            reader_cache,
            relocations,
            relocation_cache,
            lsm: Arc::downgrade(&lsm),
            live_snapshots,
            config,
            index,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
            lsm_flush_tx: Some(lsm_flush_tx),
            lsm_compact_tx: Some(lsm_compact_tx),
            lsm_flush_handle: Some(lsm_flush_handle),
            lsm_compact_handle: Some(lsm_compact_handle),
            lsm_sync_handles,
            garbage_sweep_tx: Some(garbage_sweep_tx),
            garbage_sweep_handle: Some(garbage_sweep_handle),
            gc_txs,
            gc_handles,
            gc_publish_cleanup_lock,
            garbage_publish_lock,
            relocation_durability_lock,
            compaction_admission_lock,
            durable_relocation_lsn,
            gc_claims,
            gc_concurrency,
            gc_io_limiter,
            segment_ids,
            store_halt,
            metrics,
        })
    }
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

/// Removes files owned exclusively by the retired projection engine.
///
/// The exact namespace child is fixed by the old layout. Refusing non-directories avoids following
/// a replacement symlink or deleting an unexpected file.
pub(crate) fn cleanup_retired_projection_dir(config: &StrataStoreConfig) -> Result<()> {
    let path = config.namespace_dir().join(RETIRED_PROJECTION_DIR);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(Error::Io { path, source }),
    };
    if !metadata.file_type().is_dir() {
        return Err(Error::Io {
            path,
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "retired projection path is not a directory",
            ),
        });
    }
    fs::remove_dir_all(&path).map_err(|source| Error::Io {
        path: path.clone(),
        source,
    })?;
    sync_parent_dir(&path)
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
    io_observer: Arc<dyn SegmentIoObserver>,
) -> Result<SegmentWriter> {
    ensure_ingest_dir(config)?;

    let active_path = segment_path(config, active_segment_id);
    if active_path.exists() {
        Ok(SegmentWriter::open_existing_with_io_observer(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
            io_observer,
        )?)
    } else {
        Ok(SegmentWriter::create_with_io_observer(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
            io_observer,
        )?)
    }
}

pub(crate) fn open_lsm(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    next_lsn: StrataLsn,
    recovered: Vec<(StrataLsn, LsmMutation)>,
) -> Result<Arc<Lsm>> {
    open_lsm_with_options(
        config,
        index,
        next_lsn,
        recovered,
        LsmOptions {
            rollover_policy: Some(MemtableRolloverPolicy::new(
                LSM_MEMTABLE_MAX_KEYS,
                LSM_MEMTABLE_MAX_AGE,
            )),
            ..LsmOptions::default()
        },
    )
}

fn open_lsm_with_options(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    next_lsn: StrataLsn,
    recovered: Vec<(StrataLsn, LsmMutation)>,
    options: LsmOptions,
) -> Result<Arc<Lsm>> {
    let lsm_dir = config.namespace_dir().join("lsm");
    let last_lsn = next_lsn.checked_sub(1).filter(|lsn| *lsn != 0);
    let manifest = Arc::new(load_blob_lsm_manifest(config, index)?);
    Ok(Arc::new(Lsm::from_parts(
        lsm_dir.join("tables"),
        manifest,
        recovered,
        last_lsn,
        options,
    )?))
}

pub(crate) fn open_store_wal(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    next_lsn: StrataLsn,
    checkpoint: Option<StoreCheckpoint>,
) -> Result<(
    Wal,
    Vec<(StrataLsn, LsmMutation)>,
    Vec<RelocationEntry>,
    Vec<JoinHandle<()>>,
)> {
    let (sync_tx, syncer) = file_sync_channel();
    let mut sync_handles = Vec::with_capacity(LSM_FILE_SYNC_WORKERS);
    for worker in 0..LSM_FILE_SYNC_WORKERS {
        let syncer = syncer.clone();
        sync_handles.push(
            thread::Builder::new()
                .name(format!("strata-file-sync-{}-{worker}", config.namespace))
                .spawn(move || syncer.run())
                .map_err(|source| Error::ThreadSpawn { source })?,
        );
    }
    drop(syncer);

    let last_lsn = next_lsn.checked_sub(1).filter(|lsn| *lsn != 0);
    let checkpoint_position =
        checkpoint.map_or(WalPosition::default(), |checkpoint| checkpoint.wal_position);
    // The physical checkpoint and PublishedLsn are committed in one RocksDB batch. There is no
    // second logical "checkpoint LSN" to reconcile during recovery.
    let committed_lsn = match index.get_committed_lsn()? {
        0 => None,
        lsn => Some(lsn),
    };
    let (materialized_through, retained_from) = store_wal_recovery_state(config, index)?;
    let wal = Wal::recover(
        config.namespace_dir().join("wal"),
        config.segment_max_bytes,
        checkpoint_position,
        committed_lsn,
        materialized_through,
        retained_from,
        last_lsn,
        sync_tx,
    )?;
    let mut blob = Vec::new();
    let mut relocations = Vec::new();
    wal.replay(|entry| {
        let mutation = StoreWalMutation::decode(&entry.payload)
            .map_err(|error| Error::InvalidWal(error.to_string()))?;
        match mutation {
            StoreWalMutation::Blob(mutation) => blob.push((entry.lsn, mutation)),
            StoreWalMutation::Relocation(relocation) => {
                if relocation.publish_lsn != entry.lsn {
                    return Err(Error::InvalidWal(format!(
                        "relocation publish LSN {} differs from WAL LSN {}",
                        relocation.publish_lsn, entry.lsn
                    )));
                }
                relocations.push(relocation);
            }
            StoreWalMutation::Epoch { .. } | StoreWalMutation::ShardDrop { .. } => {}
        }
        Ok(())
    })?;
    Ok((wal, blob, relocations, sync_handles))
}

/// Returns the two store wide facts needed to recover the foreground store WAL.
///
/// Relocations are durable immutable L0 files and no longer consume this WAL. The retained file ID
/// is store state; the blob-manifest value is read only to open databases created before that state
/// key existed.
pub(crate) fn store_wal_recovery_state(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<(Option<StrataLsn>, u64)> {
    let blob = load_blob_lsm_manifest(config, index)?;
    let retained_from = index
        .get_store_wal_retained_from()?
        .unwrap_or(blob.wal_retained_from);
    Ok((blob.materialized_through, retained_from))
}

pub(crate) fn open_relocation_lsm(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    next_lsn: StrataLsn,
    recovered: Vec<RelocationEntry>,
) -> Result<Arc<RelocationStore>> {
    let root = config.relocation_dir();
    let manifest = Arc::new(load_relocation_lsm_manifest(config, index)?);
    let recovered_last_lsn = recovered.iter().map(|entry| entry.publish_lsn).max();
    let recovered = recovered
        .into_iter()
        .map(|entry| {
            (
                entry.publish_lsn,
                RelocationStore::lsm_mutation(config.lsm_partition_count, &entry),
            )
        })
        .collect();
    let manifest_lsn = manifest
        .partitions
        .values()
        .flat_map(|partition| partition.base.iter().chain(&partition.patches))
        .filter_map(|table| table.max_lsn)
        .chain(manifest.materialized_through)
        .max();
    let last_lsn = next_lsn
        .checked_sub(1)
        .filter(|lsn| *lsn != 0)
        .into_iter()
        .chain(recovered_last_lsn)
        .chain(manifest_lsn)
        .max();
    let lsm = Arc::new(Lsm::from_parts(
        root.join("tables"),
        manifest,
        recovered,
        last_lsn,
        LsmOptions {
            rollover_policy: Some(MemtableRolloverPolicy::new(
                LSM_MEMTABLE_MAX_KEYS,
                LSM_MEMTABLE_MAX_AGE,
            )),
            ..LsmOptions::default()
        },
    )?);
    Ok(Arc::new(RelocationStore::new(lsm)))
}

pub(crate) fn load_blob_lsm_manifest(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<LsmManifest> {
    if let Some(manifest) = index.get_lsm_manifest(BLOB_LSM_MANIFEST)? {
        if manifest.schema_id != LSM_BASE_FORMAT
            || manifest.patch_format_id != LSM_PATCH_FORMAT
            || manifest.partition_count != config.lsm_partition_count
        {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "incompatible blob LSM manifest: schema {}, patch format {}, partitions {}",
                    manifest.schema_id, manifest.patch_format_id, manifest.partition_count
                ),
            });
        }
        return Ok(manifest);
    }

    let partition_count = NonZeroU32::new(config.lsm_partition_count)
        .ok_or(Error::InvalidConfig("lsm_partition_count must be non-zero"))?;
    let manifest = LsmManifest::empty(LSM_BASE_FORMAT, LSM_PATCH_FORMAT, partition_count);
    let mut batch = index.batch();
    index.put_lsm_manifest_batch(&mut batch, BLOB_LSM_MANIFEST, &manifest)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(manifest)
}

pub(crate) fn load_relocation_lsm_manifest(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<LsmManifest> {
    if let Some(manifest) = index.get_lsm_manifest(RELOCATION_LSM_MANIFEST)? {
        if manifest.schema_id != RELOCATION_LSM_BASE_FORMAT
            || manifest.patch_format_id != RELOCATION_LSM_PATCH_FORMAT
            || manifest.partition_count != config.lsm_partition_count
        {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "incompatible relocation LSM manifest: schema {}, patch format {}, partitions {}",
                    manifest.schema_id, manifest.patch_format_id, manifest.partition_count
                ),
            });
        }
        return Ok(manifest);
    }

    let manifest = LsmManifest::empty(
        RELOCATION_LSM_BASE_FORMAT,
        RELOCATION_LSM_PATCH_FORMAT,
        NonZeroU32::new(config.lsm_partition_count)
            .ok_or(Error::InvalidConfig("lsm_partition_count must be non-zero"))?,
    );
    let mut batch = index.batch();
    index.put_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, &manifest)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(manifest)
}

pub(crate) fn ensure_ingest_dir(config: &StrataStoreConfig) -> Result<()> {
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

/// Seeds the epoch timeline for a fresh namespace. The genesis row lives at LSN 0 — below every
/// real LSN — so `latest_epoch_at_lsn(any)` always has an answer during compaction and rollback.
/// `config.starting_epoch` only matters on first
/// creation; after that the persisted timeline wins, so changing the config later is a no-op.
/// The middle case (timeline rows exist but `CurrentEpoch` is missing)
/// rebuilds the register from the timeline, consistent with the timeline being the truth.
pub(crate) fn ensure_epoch_initialized(
    index: &StrataIndex,
    starting_epoch: Epoch,
) -> Result<Epoch> {
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

/// Establishes the lower bound for compaction-emitted terminal garbage in databases created before
/// the marker existed.
fn ensure_blob_compaction_garbage_cutover(index: &StrataIndex) -> Result<StrataLsn> {
    if let Some(lsn) = index.get_blob_compaction_garbage_from_lsn()? {
        return Ok(lsn);
    }
    let lsn = 1;
    let mut batch = index.batch();
    index.put_blob_compaction_garbage_from_lsn_batch(&mut batch, lsn)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(lsn)
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
    if config.lsm_partition_count == 0 {
        return Err(Error::InvalidConfig("lsm_partition_count must be non-zero"));
    }
    if config.write_queue_capacity == 0 {
        return Err(Error::InvalidConfig(
            "write_queue_capacity must be non-zero",
        ));
    }
    // At least 2 because rollover inherently has two unsealed segments alive at once: the full
    // one awaiting durability and the fresh one being written. A cap of 1 would deadlock the
    // writer against its own rollover.
    if config.max_unsealed_segments < 2 {
        return Err(Error::InvalidConfig(
            "max_unsealed_segments must be at least 2",
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

fn gc_relocating_segment_count(index: &StrataIndex) -> Result<usize> {
    Ok(index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| state.state == SegmentFileState::GcRelocating)
        .count())
}

/// Cleans up pending GC output segments.
///
/// Pending GC output segments are segments that are pre published by GC. They are marked
/// as `PendingGcOutput` and are deleted if a crash loses the later relocation activation batch.
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
