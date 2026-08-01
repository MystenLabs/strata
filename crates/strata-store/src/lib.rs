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
//!   -> store writer assigns the next store-global LSN
//!   -> append segment bytes and a routed store-WAL entry
//!   -> pass (LSN, keyed mutation) to the unlogged LSM
//!   -> commit an atomic index batch:
//!        segment_states[(store, segment_id)].write_offset = end_of_record
//!        store_state[(store, NextLsn)] = lsn + 1
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
//!   -> store fsyncs active segment bytes, then its WAL
//!   -> advance segment_states[active].durable_offset
//!   -> publish store_state[PublishedLsn] and the store checkpoint together
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
//!   -> recover unsealed segments and validate the exact store-WAL prefix
//!   -> verify sealed segment files according to SealedSegmentIntegrityPolicy
//!   -> load both LSM manifests, remove unpublished SSTs, and route the remaining store-WAL suffix
//!   -> choose active segment
//!   -> start memtable flush/compaction, garbage sweep, sealer, writer, and GC
//! ```
//!
//! Crash model:
//!
//! - Unsealed segments are scanned from offset 0. The store keeps the longest valid prefix that
//!   is compatible with the recovery policy.
//! - Orphan segment files without index state are ignored by point-in-time recovery by deleting
//!   the file before any active writer is opened.
//! - A complete committed store-WAL tail is promoted. If an incomplete tail is newer than
//!   `published_lsn`, its logical operations and segment bytes are rolled back together.
//! - Sealed segments are expected to be stable. On open, their files must exist and match
//!   indexed length; optional checksum verification recomputes the sealed SHA-256 digest.
//! - `published_lsn` means every logical operation up to that store-global LSN is recoverable after
//!   restart.
//!
//! Read path:
//!
//! ```text
//! get_blob
//!   -> merge the blob's LSM operands into its current state
//!   -> SegmentReader::read_record
//!   -> verify record key
//!   -> verify full-record checksum unless ReadOptions disables it
//!
//! stream_blob
//!   -> merge the blob's LSM operands into its current state
//!   -> read record header and key trailer
//!   -> validate requested payload range
//!   -> return a blocking file-range stream
//! ```
//!
//! Blob lifetime path:
//!
//! ```text
//! StrataStore::set_blob_lifetime
//!   -> append a metadata-only BlobMutation to the LSM
//!   -> do not read segment state
//!   -> do not update GC overlay summary/ranges
//! ```
//!
//! The Store-owned LSM merge operator materializes shard versions, lifetime changes, and
//! tombstones. GC moves live in the relocation LSM and are folded into main rows by the streaming
//! compaction join.
//!
//! Blob-LSM compaction emits terminal transitions into the global garbage log; the sweeper folds
//! them into per-segment summaries and local garbage logs. GC plans and copies from that state and
//! revalidates every copied record against the current blob LSM before publication.
pub mod blob_lsm;
mod config;
mod error;
mod file_sync;
mod gc;
mod gc_rate_limiter;
mod layout;
mod metrics;
mod read;
mod reader_cache;
mod relocation_cache;
mod seal;
mod shard_gc;
mod wal;
mod wal_format;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use file_sync::file_sync_channel;
use strata_core::{
    BlobKey, BlobLifecycle, Epoch, GarbageEvent, PlacementClass, RecordRef, SegmentFileState,
    SegmentGcRecordRange, SegmentGcSummary, SegmentId, SegmentKey, SegmentOwner, SegmentState,
    ShardCleanupJob, ShardCleanupState, ShardId, ShardInfo, ShardKey, ShardState, StoreCheckpoint,
    StrataLsn, WalPosition, encoded_record_len,
};
use strata_gc::GcAction;
use strata_index::StrataIndex;
use strata_lsm::{
    GarbageLog, GarbageRecord, LiveSnapshots, Lsm, LsmOptions, Manifest as LsmManifest,
    ManifestEdit, MemtableRolloverPolicy, Mutation as LsmMutation, SegmentGarbageLog, StoredValue,
    TableMeta, decode_value, select_compaction_inputs, select_patch_compaction_inputs,
    write_compaction, write_patch_compaction,
};
use strata_relocation::{RelocationEntry, RelocationMerge, RelocationStore};
use strata_segment::{SegmentFactory, SegmentIdAllocator, SegmentScanner, SegmentWriter};
use wal::{Wal, WalEntry};
use wal_format::StoreWalMutation;

pub use config::{
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MIN_IO_BYTES_PER_SEC, DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
    DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT, DEFAULT_SEAL_WORKER_COUNT,
    DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SEGMENT_READER_CACHE_CAPACITY,
    DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT, SealedSegmentIntegrityPolicy, StrataRecoveryPolicy,
    StrataStoreConfig,
};
pub use error::{Error, Result};
use gc::{
    GcCommand, GcConcurrencyConfig, GcConcurrencyController, GcExecutor, GcPrepublishedCopy,
    GcPrepublishedOutputSegment, GcSourceClaims, GcWorker,
};
pub use gc::{
    GcPublishResult, GcPublishedOutputSegment, GcPublishedRecord, GcStagedCopiedRecord,
    GcStagedOutputSegment, PreparedGcCopy, PreparedGcPlan,
};
use gc_rate_limiter::GcIoLimiter;
use layout::{
    parse_segment_file_name, relative_segment_path, retention_dir, segment_path, segment_state_path,
};
pub use metrics::StrataStoreMetrics;
use metrics::{GcKnownDelta, PutMetric};
pub use read::{ReadOptions, StoreGetProfile};
use reader_cache::SegmentReaderCache;
pub use relocation_cache::DEFAULT_RELOCATION_CACHE_ENTRIES;
use relocation_cache::RelocationCache;
use seal::{
    SealCommand, SealWorker, SegmentSealTask, active_segment_durable_offset,
    enqueue_unsealed_segments_for_sealing, verify_sealed_segments,
};
use shard_gc::shard_generation_is_obsolete;
pub use strata_gc::{GcPlanner, GcPlannerConfig};

use crate::blob_lsm::{
    BlobCompactionSnapshot, BlobMerge, BlobMergeWithRelocations, BlobMutation,
    BlobState as LsmBlobState, terminal_garbage_record,
};
const FIRST_SEGMENT_ID: SegmentId = 1;
/// How long the writer naps while waiting for the sealer to drain its backlog. Short, because
/// this sleep sits on the foreground put path during rollover backpressure.
const SEAL_BACKLOG_WAIT: Duration = Duration::from_millis(10);
const DURABILITY_PUBLISH_INTERVAL: Duration = Duration::from_secs(20 * 60);
const SEGMENT_ROLLOVER_INTERVAL: Duration = Duration::from_secs(20 * 60);
const GARBAGE_LOG_HEAD: &str = "lsm-garbage";
const GARBAGE_LOG_SWEEP_CURSOR: &str = "lsm-garbage-sweep";
const GARBAGE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const LSM_MEMTABLE_MAX_AGE: Duration = Duration::from_secs(1);
const LSM_MEMTABLE_MAX_KEYS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();
const LSM_FILE_SYNC_WORKERS: usize = 2;
const LSM_FILE_SYNC_QUEUE_CAPACITY: usize = 64;
const LSM_COMPACTION_PATCH_COUNT: usize = 8;
const LSM_COMPACTION_PATCH_BYTES: u64 = 64 * 1024 * 1024;
const LSM_COMPACTION_TARGET_BYTES: u64 = 64 * 1024 * 1024;
const LSM_GARBAGE_LOG_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const LSM_OBSOLETE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const LSM_BASE_FORMAT: &str = "store-base-v2";
const LSM_PATCH_FORMAT: &str = "store-patch-v2";
const BLOB_LSM_MANIFEST: &str = "blob";
const RETIRED_PROJECTION_DIR: &str = "accounting-index";
const RELOCATION_LSM_BASE_FORMAT: &str = "relocation-base-v1";
const RELOCATION_LSM_PATCH_FORMAT: &str = "relocation-patch-v1";
const RELOCATION_LSM_MANIFEST: &str = "relocation";
/// Default logical shard used by the standalone convenience APIs.
pub(crate) const STANDALONE_SHARD: ShardKey = ShardKey {
    id: 0,
    generation: 0,
};
/// Explicit owner used by mixed ingest segment metadata.
pub(crate) const INGEST_SEGMENT_OWNER: SegmentOwner = SegmentOwner::Store;

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

struct GarbageLogSweeper {
    index: StrataIndex,
    global_log_dir: PathBuf,
    namespace_dir: PathBuf,
    durability_publish_lock: Arc<Mutex<()>>,
    gc_txs: Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
    shutdown_rx: mpsc::Receiver<()>,
}

impl GarbageLogSweeper {
    fn run(self) {
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

    fn drain(&self) -> Result<bool> {
        let mut advanced = false;
        loop {
            let swept = {
                let _publish_guard = self
                    .durability_publish_lock
                    .lock()
                    .expect("durability publish lock poisoned");
                self.index.sweep_garbage_log(
                    &self.global_log_dir,
                    &self.namespace_dir,
                    GARBAGE_LOG_HEAD,
                    GARBAGE_LOG_SWEEP_CURSOR,
                )?
            };
            if !swept {
                break;
            }
            advanced = true;
        }
        Ok(advanced)
    }
}

fn garbage_log_dir(config: &StrataStoreConfig) -> PathBuf {
    config.namespace_dir().join("garbage-log")
}

struct LsmFlusher {
    index: StrataIndex,
    lsm: Weak<Lsm>,
    wake_rx: mpsc::Receiver<()>,
    compact_tx: mpsc::Sender<()>,
    store_halt: StoreHalt,
}

impl LsmFlusher {
    fn run(self) {
        loop {
            let timed = match self.wake_rx.recv_timeout(LSM_MEMTABLE_MAX_AGE) {
                Ok(()) => false,
                Err(mpsc::RecvTimeoutError::Timeout) => true,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            };
            let Some(lsm) = self.lsm.upgrade() else {
                return;
            };
            let rolled = if timed {
                match lsm.roll_memtable_if_due(0) {
                    Ok(rolled) => rolled.is_some(),
                    Err(error) => {
                        let reason = format!("LSM memtable rollover failed: {error}");
                        self.store_halt.halt(reason.clone());
                        lsm.halt(reason);
                        return;
                    }
                }
            } else {
                false
            };
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

    fn flush_all(&self, lsm: &Lsm) -> Result<()> {
        loop {
            let table_id = lsm.manifest().next_table_id;
            let relative_path = format!("patch-{table_id:020}.sst");
            if lsm
                .flush_one(0, table_id, relative_path, |edit| {
                    publish_blob_lsm_edit(&self.index, edit)
                })?
                .is_none()
            {
                break;
            }
        }
        let published_lsn = self.index.get_published_lsn()?;
        lsm.materialize_through(published_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        Ok(())
    }
}

struct LsmCompactor {
    index: StrataIndex,
    lsm: Weak<Lsm>,
    relocations: Weak<RelocationStore>,
    garbage_log_dir: PathBuf,
    garbage_publish_lock: Arc<Mutex<()>>,
    wake_rx: mpsc::Receiver<()>,
    store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
    obsolete: Vec<TableMeta>,
}

impl LsmCompactor {
    fn run(mut self) {
        loop {
            match self.wake_rx.recv_timeout(LSM_OBSOLETE_CLEANUP_INTERVAL) {
                Ok(()) => {
                    let Some(lsm) = self.lsm.upgrade() else {
                        return;
                    };
                    if let Err(error) = self.compact(&lsm, false) {
                        let reason = format!("LSM compaction failed: {error}");
                        self.store_halt.halt(reason.clone());
                        lsm.halt(reason);
                        return;
                    }
                    self.cleanup_obsolete(&lsm);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let Some(lsm) = self.lsm.upgrade() else {
                        return;
                    };
                    if let Err(error) = self.compact(&lsm, true) {
                        let reason = format!("LSM compaction failed: {error}");
                        self.store_halt.halt(reason.clone());
                        lsm.halt(reason);
                        return;
                    }
                    self.cleanup_obsolete(&lsm);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn compact(&mut self, lsm: &Lsm, force: bool) -> Result<()> {
        let manifest = lsm.manifest();
        let patches = &manifest.partitions[&0].patches;
        let patch_bytes = patches
            .iter()
            .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
        if !force
            && patches.len() < LSM_COMPACTION_PATCH_COUNT
            && patch_bytes < LSM_COMPACTION_PATCH_BYTES
        {
            return Ok(());
        }
        if patches.is_empty() {
            return Ok(());
        }
        let published_lsn = self.index.get_published_lsn()?;
        if patches
            .iter()
            .any(|table| table.max_lsn.is_none_or(|lsn| lsn > published_lsn))
        {
            return Ok(());
        }

        // Count pressure coalesces patches; byte pressure and the periodic pass materialize a base.
        let partial = !force && patch_bytes < LSM_COMPACTION_PATCH_BYTES;
        let tables = lsm.table_store();
        let selected = if partial {
            select_patch_compaction_inputs(&manifest, &tables, 0, patches)
        } else {
            select_compaction_inputs(&manifest, &tables, 0, patches)
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
        let mut next_table_id = manifest.next_table_id;
        let started = Instant::now();
        // The LSM frontier proves that every earlier keyed mutation is represented in SSTs;
        // publication is the store-wide durability bound for RocksDB-only transitions.
        let materialized_through_lsn = manifest
            .materialized_through
            .unwrap_or_default()
            .min(published_lsn);
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
        let epoch_snapshot = BlobCompactionSnapshot {
            materialized_through_lsn,
            emit_garbage_from_lsn,
            epoch_changes,
            ..BlobCompactionSnapshot::default()
        };
        let (edit, garbage, healed_references) = if partial {
            let merge = BlobMergeWithRelocations::new(None, epoch_snapshot);
            let (edit, garbage) =
                write_patch_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
                    let id = next_table_id;
                    next_table_id = next_table_id.saturating_add(1);
                    (id, format!("patch-{id:020}.sst"))
                })?;
            (edit, garbage, 0)
        } else {
            // TODO: An epoch advance does not currently rewrite base-only ranges. Reads remain
            // correct because they resolve against the latest epoch, but GC discovery waits until
            // a later patch causes each range to enter full compaction. Add an incremental base
            // sweep or a per-base-SST applied frontier when prompt discovery is required.
            let relocation_max_lsn = self.index.get_published_lsn()?;
            let relocation_scan = match relocation_max_lsn {
                max_lsn if max_lsn != 0 => self
                    .relocations
                    .upgrade()
                    .map(|relocations| {
                        relocations.scan(0, &inputs.first_key, &inputs.last_key, max_lsn)
                    })
                    .transpose()?,
                _ => None,
            };
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
            let (edit, garbage) =
                write_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
                    let id = next_table_id;
                    next_table_id = next_table_id.saturating_add(1);
                    (id, format!("base-{id:020}.sst"))
                })?;
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
        let published = self
            .index
            .get_lsm_manifest(BLOB_LSM_MANIFEST)?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "published blob LSM manifest is missing".to_owned(),
            })?;
        lsm.install_manifest(published)?;
        self.metrics.record_main_compaction(
            healed_references,
            input_bytes,
            output_bytes,
            started.elapsed(),
        );
        drop(inputs);
        self.obsolete.extend(obsolete);
        Ok(())
    }

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

fn publish_blob_lsm_edit(
    index: &StrataIndex,
    edit: &ManifestEdit,
) -> strata_lsm::Result<LsmManifest> {
    let publish = || -> Result<LsmManifest> {
        let mut batch = index.batch();
        index.merge_lsm_manifest_batch(&mut batch, BLOB_LSM_MANIFEST, edit)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        index
            .get_lsm_manifest(BLOB_LSM_MANIFEST)?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "published blob LSM manifest is missing".to_owned(),
            })
    };
    publish().map_err(|error| strata_lsm::Error::InvalidManifest {
        reason: format!("blob manifest publication failed: {error}"),
    })
}

fn publish_relocation_lsm_edit(
    index: &StrataIndex,
    edit: &ManifestEdit,
) -> strata_lsm::Result<LsmManifest> {
    let publish = || -> Result<LsmManifest> {
        let mut batch = index.batch();
        index.merge_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, edit)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        index
            .get_lsm_manifest(RELOCATION_LSM_MANIFEST)?
            .ok_or_else(|| Error::InvariantViolation {
                reason: "published relocation LSM manifest is missing".to_owned(),
            })
    };
    publish().map_err(|error| strata_lsm::Error::InvalidManifest {
        reason: format!("relocation manifest publication failed: {error}"),
    })
}

fn flush_relocation_lsm(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let id = relocations.lsm().manifest().next_table_id;
    relocations
        .lsm()
        .flush_one(0, id, format!("patch-{id:020}.sst"), |edit| {
            publish_relocation_lsm_edit(index, edit)
        })?;
    relocations
        .lsm()
        .materialize_through(index.get_published_lsn()?, |edit| {
            publish_relocation_lsm_edit(index, edit)
        })?;

    let manifest = relocations.lsm().manifest();
    let patches = &manifest.partitions[&0].patches;
    let patch_bytes = patches
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
    if patches.len() < LSM_COMPACTION_PATCH_COUNT && patch_bytes < LSM_COMPACTION_PATCH_BYTES {
        return Ok(());
    }

    compact_relocation_lsm(index, relocations, relocation_cache, metrics)
}

fn compact_relocation_lsm(
    index: &StrataIndex,
    relocations: &RelocationStore,
    relocation_cache: &RelocationCache,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let manifest = relocations.lsm().manifest();
    let patches = &manifest.partitions[&0].patches;
    let published_lsn = index.get_published_lsn()?;
    if published_lsn == 0 {
        return Ok(());
    }
    if patches
        .iter()
        .any(|table| table.max_lsn.is_none_or(|lsn| lsn > published_lsn))
    {
        return Ok(());
    }

    let tables = relocations.lsm().table_store();
    let Some(inputs) = select_compaction_inputs(&manifest, &tables, 0, patches)? else {
        return Ok(());
    };
    let obsolete = inputs
        .base
        .iter()
        .chain(&inputs.patches)
        .cloned()
        .collect::<Vec<_>>();
    let input_bytes = obsolete
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));
    let merge = RelocationMerge::new(
        index
            .iter_segment_states()?
            .into_iter()
            .filter_map(|(segment_id, state)| {
                (state.state == SegmentFileState::Deleted).then_some(segment_id)
            })
            .collect(),
    );
    let mut next_table_id = manifest.next_table_id;
    let started = Instant::now();
    let (edit, garbage) = write_compaction(&inputs, &merge, LSM_COMPACTION_TARGET_BYTES, || {
        let id = next_table_id;
        next_table_id = next_table_id.saturating_add(1);
        (id, format!("base-{id:020}.sst"))
    })?;
    debug_assert!(garbage.is_empty());
    let output_bytes = edit
        .add_base
        .iter()
        .fold(0u64, |bytes, table| bytes.saturating_add(table.file_len));

    let mut batch = index.batch();
    index.merge_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, &edit)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    let published = index
        .get_lsm_manifest(RELOCATION_LSM_MANIFEST)?
        .ok_or_else(|| Error::InvariantViolation {
            reason: "published relocation LSM manifest is missing".to_owned(),
        })?;
    relocations.lsm().install_manifest(published)?;
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
    drop(inputs);
    for table in obsolete {
        tables.remove_if_unpinned(&table)?;
    }
    Ok(())
}

/// Single-namespace Strata store.
#[derive(Debug)]
pub struct StrataStore {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    lsm: Weak<Lsm>,
    pub(crate) write_tx: Option<mpsc::SyncSender<WriteCommand>>,
    writer_handle: Option<JoinHandle<()>>,
    lsm_flush_tx: Option<mpsc::Sender<()>>,
    lsm_flush_handle: Option<JoinHandle<()>>,
    lsm_compact_handle: Option<JoinHandle<()>>,
    lsm_sync_handles: Vec<JoinHandle<()>>,
    seal_tx: Option<mpsc::Sender<SealCommand>>,
    seal_handle: Option<JoinHandle<()>>,
    garbage_sweep_tx: Option<mpsc::Sender<()>>,
    garbage_sweep_handle: Option<JoinHandle<()>>,
    pub(crate) gc_txs: Vec<mpsc::Sender<GcCommand>>,
    gc_handles: Vec<JoinHandle<()>>,
    pub(crate) gc_publish_cleanup_lock: Arc<Mutex<()>>,
    pub(crate) durability_publish_lock: Arc<Mutex<()>>,
    pub(crate) gc_claims: Arc<GcSourceClaims>,
    pub(crate) gc_concurrency: Arc<GcConcurrencyController>,
    pub(crate) gc_io_limiter: Arc<GcIoLimiter>,
    pub(crate) segment_ids: SegmentIdAllocator,
    pub(crate) reader_cache: Arc<SegmentReaderCache>,
    pub(crate) relocations: Arc<RelocationStore>,
    pub(crate) relocation_cache: Arc<RelocationCache>,
    #[cfg(test)]
    live_snapshots: LiveSnapshots,
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
    pub payload_lsn: StrataLsn,
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
    ///    separate compactor merges durable tables, and the sealer handles segment rollovers. GC
    ///    starts after the writer because GC publish uses the same serialized write queue.
    ///
    /// Everything mutable ends up owned by the writer thread; the `StrataStore` handle itself
    /// only holds channels, the index, and the read-side cache.
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
        verify_sealed_segments(&config, &index)?;
        let active_segment_id = choose_active_segment_id(&index)?;
        let segment_ids =
            SegmentIdAllocator::new(next_segment_id_after(&index, active_segment_id)?);
        let active_writer = open_active_writer(&config, active_segment_id)?;
        let durable_offset = active_segment_durable_offset(&index, active_writer.segment_id())?;
        let next_lsn = index.get_next_lsn()?;
        let published_lsn = index.get_published_lsn()?;
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
        let live_snapshots = lsm.live_snapshots();
        metrics.initialize_gc_known(&gc_known_summary(&index)?);
        metrics.set_gc_relocating_segments(gc_relocating_segment_count(&index)?);
        metrics.set_current_epoch(current_epoch);
        metrics.set_unsealed_segments(unsealed_ingest_segment_count(&index)?);
        let (seal_tx, seal_rx) = mpsc::channel();
        let gc_publish_cleanup_lock = Arc::new(Mutex::new(()));
        let gc_wake_txs = Arc::new(Mutex::new(Vec::new()));
        let gc_claims = Arc::new(GcSourceClaims::default());
        let gc_io_limiter = Arc::new(GcIoLimiter::new(config.gc_io_bytes_per_sec));
        let store_halt = StoreHalt::default();
        let durability_publish_lock = Arc::new(Mutex::new(()));
        let gc_concurrency = Arc::new(GcConcurrencyController::new(
            GcConcurrencyConfig::from_store_config(&config),
            metrics.clone(),
        ));
        let recovered_frozen_memtables = !lsm.frozen_generations(0)?.is_empty();
        let (lsm_compact_tx, lsm_compact_rx) = mpsc::channel();
        let lsm_compactor = LsmCompactor {
            index: index.clone(),
            lsm: Arc::downgrade(&lsm),
            relocations: Arc::downgrade(&relocations),
            garbage_log_dir: garbage_log_dir(&config),
            garbage_publish_lock: Arc::clone(&durability_publish_lock),
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
            durability_publish_lock: Arc::clone(&durability_publish_lock),
            gc_txs: Arc::clone(&gc_wake_txs),
            shutdown_rx: garbage_sweep_rx,
        };
        let garbage_sweep_handle = thread::Builder::new()
            .name(format!("strata-garbage-sweeper-{}", config.namespace))
            .spawn(move || garbage_sweeper.run())
            .map_err(|source| Error::ThreadSpawn { source })?;
        let seal_worker = SealWorker {
            config: config.clone(),
            index: index.clone(),
            ingest_owner: INGEST_SEGMENT_OWNER,
            seal_rx,
            durability_publish_lock: Arc::clone(&durability_publish_lock),
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
        let relocation_cache = Arc::new(RelocationCache::new(DEFAULT_RELOCATION_CACHE_ENTRIES));
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
            ),
            live_snapshots: live_snapshots.clone(),
            durability_publish_lock: Arc::clone(&durability_publish_lock),
            active_segment_state,
            durable_offset,
            pending_allocation_records: 0,
            last_durability_publish_at: Instant::now(),
            last_segment_rollover_at: Instant::now(),
            last_segment_rollover_next_lsn: next_lsn,
            pending_rollovers: Vec::new(),
            lsm_flush_tx: lsm_flush_tx.clone(),
            lsm_compact_tx: lsm_compact_tx.clone(),
            seal_tx: seal_tx.clone(),
            write_rx,
            ingest_owner: INGEST_SEGMENT_OWNER,
            reader_cache: Arc::clone(&reader_cache),
            relocations: Arc::clone(&relocations),
            relocation_cache: Arc::clone(&relocation_cache),
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
                    publish_cleanup_lock: Arc::clone(&gc_publish_cleanup_lock),
                    durability_publish_lock: Arc::clone(&durability_publish_lock),
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
            #[cfg(test)]
            live_snapshots,
            config,
            index,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
            lsm_flush_tx: Some(lsm_flush_tx),
            lsm_flush_handle: Some(lsm_flush_handle),
            lsm_compact_handle: Some(lsm_compact_handle),
            lsm_sync_handles,
            seal_tx: Some(seal_tx),
            seal_handle: Some(seal_handle),
            garbage_sweep_tx: Some(garbage_sweep_tx),
            garbage_sweep_handle: Some(garbage_sweep_handle),
            gc_txs,
            gc_handles,
            gc_publish_cleanup_lock,
            durability_publish_lock,
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

    fn lsm(&self) -> Result<Arc<Lsm>> {
        self.lsm.upgrade().ok_or_else(|| Error::StoreHalted {
            reason: "LSM writer has stopped".to_owned(),
        })
    }

    pub fn metrics(&self) -> &StrataStoreMetrics {
        &self.metrics
    }

    /// Current immutable data-block cache statistics for the main LSM.
    pub fn main_lsm_block_cache_stats(&self) -> Result<strata_lsm::BlockCacheStats> {
        Ok(self.lsm()?.block_cache_stats())
    }

    /// Current immutable data-block cache statistics for the relocation LSM.
    pub fn relocation_lsm_block_cache_stats(&self) -> strata_lsm::BlockCacheStats {
        self.relocations.lsm().block_cache_stats()
    }

    /// Clears resolved relocation pointers without evicting immutable relocation SST blocks.
    pub fn clear_relocation_cache(&self) {
        self.relocation_cache.clear();
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
    /// put could land between them and the LSM would record a different history than intended.
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
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        Ok(())
    }

    /// Writes a blob and returns its LSN. Returning means *visible*, not durable: the bytes are
    /// in the segment file and the index points at them, but only `sync` (or the periodic sync)
    /// makes them crash-safe. Callers that need durability gate on `published_lsn() >= lsn`.
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
    /// Lifetime changes are metadata-only LSNs. Blob-LSM compaction applies them during merge and
    /// emits expiration garbage without touching segment bytes. Rewriting the blob just to change
    /// its lifetime would create an unnecessary second payload record.
    pub fn set_blob_lifetime(&self, key: &BlobKey, logical_end_epoch: Epoch) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::SetBlobLifetime {
            key: key.clone(),
            logical_end_epoch,
        }])?;
        result.first_lsn().ok_or(Error::WriteResponseDropped)
    }

    /// Appends a logical delete for one blob shard generation.
    ///
    /// A tombstone is an ordered LSN, not an in place removal. If we deleted
    /// the version row immediately, recovery after a crash could resurrect an older payload because
    /// there would be no durable delete marker to hide it. Other shards holding the same key remain
    /// visible.
    pub fn tombstone(&self, shard_id: ShardId, key: &BlobKey) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Tombstone {
            shard_id,
            key: key.clone(),
        }])?;
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
    /// Failure mode avoided: snapshot compaction must classify an old write under the epoch that
    /// was true when it happened. Using today's epoch for LSN 25 after several increments would
    /// expire or pin bytes in the wrong bucket.
    pub fn epoch_at_lsn(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        Ok(self.index.latest_epoch_at_lsn(lsn)?.map(|(_, epoch)| epoch))
    }

    /// Appends an epoch change operation and returns the new epoch with its LSN.
    ///
    /// Epoch increments consume LSNs so they are ordered with blob writes.
    /// Without that, crash recovery and later compaction could disagree about whether blob A was
    /// written before epoch 9.
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

    /// Rolls the current ingest segment so all preceding writes can be sealed and considered by
    /// retention organization or garbage collection.
    ///
    /// Production performs this rollover periodically. Administrative tools and benchmarks can
    /// request it explicitly when they need a bounded active tail. This does not publish the WAL.
    /// Returning means the rollover metadata is visible and sealing has been queued; callers that
    /// require the sealed file should wait for the segment state to leave `Sealing`.
    pub fn rollover_active_segment_for_sealing(&self) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::RolloverSegment(RolloverSegmentRequest {
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        Ok(())
    }

    /// Flushes the active relocation memtable once its normal age or size threshold is due.
    ///
    /// Quiet maintenance tools and benchmarks can call this after a GC publish so the same
    /// on-disk lookup and compaction paths used under sustained GC are exercised without issuing a
    /// synthetic relocation.
    pub fn flush_relocation_memtable_if_due(&self) -> Result<bool> {
        if self.relocations.lsm().roll_memtable_if_due(0)?.is_none() {
            return Ok(false);
        }
        flush_relocation_lsm(
            &self.index,
            &self.relocations,
            &self.relocation_cache,
            &self.metrics,
        )?;
        Ok(true)
    }

    /// Every operation with `lsn <= published_lsn` survives a crash. This is the value callers
    /// (e.g. the Walrus event cursor) gate on before acknowledging work as done.
    pub fn published_lsn(&self) -> Result<StrataLsn> {
        Ok(self.index.get_published_lsn()?)
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
// writer drains, then the LSM flusher and compactor release their engine references so file-sync
// workers can exit. The sealer the writer can still nudge stops afterwards.
// Joins are best-effort; a panicked worker shouldn't turn drop into a second panic.
impl Drop for StrataStore {
    fn drop(&mut self) {
        for gc_tx in self.gc_txs.drain(..) {
            let _ = gc_tx.send(GcCommand::Shutdown);
        }
        for gc_handle in self.gc_handles.drain(..) {
            let _ = gc_handle.join();
        }
        if let Some(garbage_sweep_tx) = self.garbage_sweep_tx.take() {
            let _ = garbage_sweep_tx.send(());
        }
        if let Some(garbage_sweep_handle) = self.garbage_sweep_handle.take() {
            let _ = garbage_sweep_handle.join();
        }
        if let Some(write_tx) = self.write_tx.take() {
            let _ = write_tx.send(WriteCommand::Shutdown);
        }
        if let Some(writer_handle) = self.writer_handle.take() {
            let _ = writer_handle.join();
        }
        self.lsm_flush_tx.take();
        if let Some(lsm_flush_handle) = self.lsm_flush_handle.take() {
            let _ = lsm_flush_handle.join();
        }
        if let Some(lsm_compact_handle) = self.lsm_compact_handle.take() {
            let _ = lsm_compact_handle.join();
        }
        for sync_handle in self.lsm_sync_handles.drain(..) {
            let _ = sync_handle.join();
        }
        if let Some(seal_tx) = self.seal_tx.take() {
            let _ = seal_tx.send(SealCommand::Shutdown);
        }
        if let Some(seal_handle) = self.seal_handle.take() {
            let _ = seal_handle.join();
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum WriteCommand {
    AddShard(AddShardRequest),
    Batch(BatchWriteRequest),
    DropShard(DropShardRequest),
    GcPublish(GcPublishRequest),
    RolloverSegment(RolloverSegmentRequest),
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
    /// Prepublished copy bundle to reconcile and commit in the ordered writer lane.
    publish: GcPrepublishedCopy,
    /// One-shot response channel back to the caller that requested GC publication.
    response_tx: mpsc::Sender<Result<GcPublishResult>>,
}

#[derive(Debug)]
pub(crate) struct GcPreparedPublish {
    copy: GcPrepublishedCopy,
    reconciled_lsn: StrataLsn,
    skipped_records: Vec<GcSkippedCopiedRecord>,
}

#[derive(Debug)]
struct SyncRequest {
    response_tx: mpsc::Sender<Result<()>>,
    profile: ProfileRequest<StoreSyncProfile>,
}

#[derive(Debug)]
struct RolloverSegmentRequest {
    response_tx: mpsc::Sender<Result<()>>,
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
        shard_id: ShardId,
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
    pub index_batch_commit: Duration,
    pub rollover_post_commit: Duration,
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
    /// Includes durable segment state assembly, published LSN computation, and batch construction.
    pub published_lsn_compute: Duration,
    /// RocksDB batch commit with synchronous WAL durability.
    pub index_batch_commit: Duration,
    pub state_update: Duration,
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

    /// Adds a shard-scoped tombstone to this batch.
    ///
    /// Tombstones remain ordered relative to any preceding puts in the same
    /// batch. Without this, deleting a key after writing a replacement could race with another put
    /// and hide the wrong version.
    pub fn tombstone(&mut self, shard_id: ShardId, key: BlobKey) -> &mut Self {
        self.ops.push(BatchOp::Tombstone { shard_id, key });
        self
    }

    /// Adds an epoch increment to this batch.
    ///
    /// Epoch changes are treated like logical operations. A batch such as
    /// `put A, increment epoch, put B` must replay exactly that order after crash recovery so A and
    /// B do not end up in the same logical epoch.
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
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    Tombstone {
        shard: ShardKey,
        key: BlobKey,
        lsn: StrataLsn,
    },
    EpochChange {
        lsn: StrataLsn,
        epoch: Epoch,
    },
}

impl PreparedBatchOp {
    fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Lifecycle { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::EpochChange { lsn, .. } => *lsn,
        }
    }

    fn blob_mutation(&self) -> Result<Option<LsmMutation>> {
        match self {
            Self::Put {
                shard,
                key,
                current_epoch,
                record_ref,
                ..
            } => Ok(Some(LsmMutation::PutBlob {
                partition: 0,
                key: key.as_bytes().to_vec(),
                metadata: BlobMutation::encode_put_metadata(*shard, *current_epoch),
                record_ref: record_ref.ok_or_else(|| Error::InvariantViolation {
                    reason: format!("payload segment reference is missing at LSN {}", self.lsn()),
                })?,
            })),
            Self::Lifecycle {
                key,
                logical_end_epoch,
                current_epoch,
                ..
            } => Ok(Some(LsmMutation::Put {
                partition: 0,
                key: key.as_bytes().to_vec(),
                value: BlobMutation::SetLifetime {
                    logical_end_epoch: *logical_end_epoch,
                    current_epoch: *current_epoch,
                }
                .encode_inline()?,
            })),
            Self::Tombstone { shard, key, .. } => Ok(Some(LsmMutation::Put {
                partition: 0,
                key: key.as_bytes().to_vec(),
                value: BlobMutation::Tombstone { shard: *shard }.encode_inline()?,
            })),
            Self::EpochChange { .. } => Ok(None),
        }
    }
}

#[derive(Debug)]
struct PendingRollover {
    old_segment_state: SegmentState,
    new_segment_state: SegmentState,
    new_segment_published_at_lsn: StrataLsn,
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
        index.put_segment_published_at_lsn_batch(
            batch,
            self.new_segment_state.segment_id,
            self.new_segment_published_at_lsn,
        )?;
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
    EnqueueSeal {
        seal_tx: mpsc::Sender<SealCommand>,
        task: SegmentSealTask,
        metrics: StrataStoreMetrics,
    },
}

impl PostCommitAction {
    /// Runs side effects that are safe only after the index batch has committed.
    ///
    /// These actions intentionally do not happen during batch assembly. For example, queuing a
    /// seal before its metadata batch commits could expose a rollover whose index entry is not
    /// visible yet.
    fn run(self) {
        match self {
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

/// Owner of the global sequence, payload segment, and store WAL.
///
/// There is exactly one routing decision here: every LSN is appended to `wal`, then keyed records
/// go to an LSM while epoch/shard records go to RocksDB. Neither LSM allocates LSNs or performs
/// durability I/O.
struct WriteCoordinator {
    config: StrataStoreConfig,
    index: StrataIndex,
    lsm: Arc<Lsm>,
    wal: Wal,
    segment: SegmentWriter,
    segment_factory: SegmentFactory,
    live_snapshots: LiveSnapshots,
    durability_publish_lock: Arc<Mutex<()>>,
    active_segment_state: SegmentState,
    durable_offset: u64,
    pending_allocation_records: u64,
    last_durability_publish_at: Instant,
    last_segment_rollover_at: Instant,
    last_segment_rollover_next_lsn: StrataLsn,
    pending_rollovers: Vec<PendingRollover>,
    lsm_flush_tx: mpsc::Sender<()>,
    lsm_compact_tx: mpsc::Sender<()>,
    seal_tx: mpsc::Sender<SealCommand>,
    write_rx: mpsc::Receiver<WriteCommand>,
    ingest_owner: SegmentOwner,
    reader_cache: Arc<SegmentReaderCache>,
    relocations: Arc<RelocationStore>,
    relocation_cache: Arc<RelocationCache>,
    gc_concurrency: Arc<GcConcurrencyController>,
    store_halt: StoreHalt,
    metrics: StrataStoreMetrics,
}

impl WriteCoordinator {
    /// Main compatibility loop for store metadata publication and administrative operations.
    fn run(mut self) {
        loop {
            let timeout = self.next_maintenance_timeout();
            let command = match self.write_rx.recv_timeout(timeout) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.process_scheduled_maintenance() {
                        self.halt_writer_error("scheduled writer maintenance", &error);
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
                WriteCommand::RolloverSegment(request) => {
                    let result = self.process_segment_rollover();
                    let _ = request.response_tx.send(result);
                }
                WriteCommand::Sync(request) => {
                    self.process_sync(request);
                }
                WriteCommand::Shutdown => unreachable!("shutdown is handled before dispatch"),
            }
        }
    }

    fn next_maintenance_timeout(&self) -> Duration {
        self.next_durability_publish_timeout()
            .min(self.next_segment_rollover_timeout())
    }

    fn next_durability_publish_timeout(&self) -> Duration {
        DURABILITY_PUBLISH_INTERVAL.saturating_sub(self.last_durability_publish_at.elapsed())
    }

    fn next_segment_rollover_timeout(&self) -> Duration {
        SEGMENT_ROLLOVER_INTERVAL.saturating_sub(self.last_segment_rollover_at.elapsed())
    }

    fn process_scheduled_maintenance(&mut self) -> Result<()> {
        // Rollover first when both clocks expire together. The following publication then fsyncs
        // the store WAL and the RocksDB metadata that installed the replacement active segment.
        if self.next_segment_rollover_timeout().is_zero() {
            self.process_segment_rollover()?;
        }
        if self.next_durability_publish_timeout().is_zero() {
            self.process_scheduled_durability_publish()?;
        }
        Ok(())
    }

    fn process_scheduled_durability_publish(&mut self) -> Result<()> {
        let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        let published_lsn = self.index.get_published_lsn()?;
        if published_lsn > committed_lsn {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published LSN {published_lsn} follows committed LSN {committed_lsn}"
                ),
            });
        }
        if published_lsn == committed_lsn {
            self.last_durability_publish_at = Instant::now();
            return Ok(());
        }
        self.sync_data(None)
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
            WriteCommand::RolloverSegment(request) => {
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
        Ok(info.key(shard_id))
    }

    fn process_drop_shard(&mut self, request: DropShardRequest) {
        let result = self.submit_drop_shard(request.shard_id);
        let _ = request.response_tx.send(result);
    }

    /// Runs a GC publish request on the writer thread and reports the result to the caller.
    ///
    /// GC publish allocates from the store-global sequence, then commits its metadata in this
    /// serialized publication lane.
    fn process_gc_publish(&mut self, request: GcPublishRequest) {
        let result = self.submit_gc_publish(request.publish);
        let _ = request.response_tx.send(result);
    }

    /// Validates that a shard can be dropped and appends the asynchronous registry update.
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

        // Writer serialization gives the drop an LSN after every preceding payload transition.
        // It becomes crash-durable at the next ordinary `sync()`; cleanup is gated by that
        // published frontier below.
        let shard = info.key(shard_id);
        self.mark_shard_dropped(shard_id, shard)?;
        Ok(shard)
    }

    /// Stores the drop in the store WAL and RocksDB. No fake LSM row is created.
    fn mark_shard_dropped(&mut self, shard_id: ShardId, shard: ShardKey) -> Result<()> {
        let dropped_info = ShardInfo {
            current_generation: shard.generation,
            state: ShardState::Dropped,
        };
        let drop_lsn = self.index.get_next_lsn()?;
        let next_lsn = drop_lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        self.wal.append(&[WalEntry {
            lsn: drop_lsn,
            payload: StoreWalMutation::ShardDrop { shard }.encode()?,
        }])?;

        let commit_result = (|| {
            let mut batch = self.index.batch();
            self.index
                .put_shard_info_batch(&mut batch, shard_id, dropped_info)?;
            self.index.put_shard_cleanup_job_batch(
                &mut batch,
                ShardCleanupJob {
                    shard,
                    drop_lsn,
                    // Shard-owned files can be removed as a unit as soon as the durable generation
                    // fence is published. Mixed ingest refs are retired later by ordinary blob-LSM
                    // compaction and do not block this bulk cleanup.
                    state: ShardCleanupState::ReadyForGc,
                },
            )?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            batch.write().map_err(strata_index::Error::from)?;
            Ok::<(), Error>(())
        })();

        if let Err(error) = commit_result {
            self.halt_writer_error("shard drop after store-WAL append", &error);
            return Err(error);
        }

        self.metrics.set_next_lsn(next_lsn);
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

    /// Full write transaction for a batch: validate, append the payload/store-WAL records, apply
    /// keyed projections, then commit one index metadata batch.
    ///
    /// User visible validation errors return normally before physical writer state changes. Once
    /// the physical write path starts, segment/WAL/index failures halt the store and crash recovery
    /// remains the single repair path for partially published bytes or rollovers.
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
        let mut lsm_writes = Vec::new();
        let mut wal_entries = Vec::with_capacity(prepared.ops.len());
        for op in &mut prepared.ops {
            let lsn = op.lsn();
            if let PreparedBatchOp::Put {
                shard,
                key,
                payload,
                record_ref,
                record_bytes,
                ..
            } = op
            {
                let append = profile_phase(
                    profile.as_deref_mut(),
                    |profile, elapsed| profile.segment_append += elapsed,
                    || {
                        if self.segment.ensure_capacity(*record_bytes).is_err() {
                            self.rollover_active_segment(lsn)?;
                        }
                        self.segment
                            .append_for_shard(key, lsn, *shard, payload)
                            .map_err(Error::from)
                    },
                );
                let written = match append {
                    Ok(written) => written,
                    Err(error) => {
                        self.halt_submit_batch_failure(
                            "payload segment append",
                            &error,
                            appended_records,
                            appended_bytes,
                        );
                        let _ = response_tx.send(Err(error));
                        return Err(());
                    }
                };
                if written.record_len != *record_bytes {
                    let error = Error::InvariantViolation {
                        reason: format!(
                            "segment put at LSN {lsn} wrote {}, expected {record_bytes}",
                            written.record_len
                        ),
                    };
                    self.halt_submit_batch_failure(
                        "payload segment length",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
                *record_ref = Some(written.record_ref);
                let Some(write_offset) = written.record_ref.end_offset() else {
                    let error = Error::InvariantViolation {
                        reason: format!("record reference at LSN {lsn} overflows its segment"),
                    };
                    self.halt_submit_batch_failure(
                        "advance active segment offset",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                };
                appended_records = appended_records.saturating_add(1);
                appended_bytes = appended_bytes.saturating_add(written.record_ref.len);
                self.pending_allocation_records = self.pending_allocation_records.saturating_add(1);
                put_metrics.push(PutMetric {
                    payload_bytes: payload.len() as u64,
                    record_bytes: *record_bytes,
                });
                self.active_segment_state.write_offset = write_offset;
                self.active_segment_state.min_lsn = Some(
                    self.active_segment_state
                        .min_lsn
                        .map_or(lsn, |first| first.min(lsn)),
                );
                self.active_segment_state.max_lsn = Some(
                    self.active_segment_state
                        .max_lsn
                        .map_or(lsn, |last| last.max(lsn)),
                );
            }

            let store_mutation = match op.blob_mutation() {
                Ok(Some(mutation)) => {
                    lsm_writes.push((lsn, mutation.clone()));
                    StoreWalMutation::Blob(mutation)
                }
                Ok(None) => match op {
                    PreparedBatchOp::EpochChange { epoch, .. } => {
                        StoreWalMutation::Epoch { epoch: *epoch }
                    }
                    _ => unreachable!("only epoch changes are RocksDB-only batch operations"),
                },
                Err(error) => {
                    self.halt_submit_batch_failure(
                        "encode store WAL mutation",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
            };
            let payload = match store_mutation.encode() {
                Ok(payload) => payload,
                Err(error) => {
                    self.halt_submit_batch_failure(
                        "encode store WAL record",
                        &error,
                        appended_records,
                        appended_bytes,
                    );
                    let _ = response_tx.send(Err(error));
                    return Err(());
                }
            };
            wal_entries.push(WalEntry { lsn, payload });
            prepared.result.op_lsns.push(lsn);
        }

        if let Err(error) = self.wal.append(&wal_entries).map_err(Error::from) {
            self.halt_submit_batch_failure(
                "store WAL append",
                &error,
                appended_records,
                appended_bytes,
            );
            let _ = response_tx.send(Err(error));
            return Err(());
        }
        let lsm_write = match self.lsm.write_batch(lsm_writes) {
            Ok(result) => result,
            Err(error) => {
                let error = Error::from(error);
                self.halt_submit_batch_failure(
                    "blob LSM apply",
                    &error,
                    appended_records,
                    appended_bytes,
                );
                let _ = response_tx.send(Err(error));
                return Err(());
            }
        };
        let rolled_memtable = !lsm_write.rolled_memtables.is_empty();

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
        self.request_lsm_flush(rolled_memtable);
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
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

    fn halt_writer_error(&self, context: &str, error: &Error) {
        self.store_halt.halt(format!(
            "fatal strata writer error during {context}: {error}"
        ));
    }

    /// Publishes copied GC outputs after revalidating them in writer order.
    ///
    /// TODO: publish large GC copies in bounded chunks. File construction and fsync are outside
    /// this path, but a large copy still commits one RocksDB batch of relocation and segment
    /// metadata while foreground writes wait behind it.
    fn submit_gc_publish(&mut self, copy: GcPrepublishedCopy) -> Result<GcPublishResult> {
        let publish = self.prepare_gc_publish(copy)?;
        self.commit_gc_publish(publish)
    }

    /// Revalidates copied refs against the current LSM while holding the writer ordering lane.
    ///
    /// Every foreground mutation already ahead of this request is visible here, and no later
    /// mutation can enter until the matching relocation metadata batch is appended below.
    fn prepare_gc_publish(&self, mut copy: GcPrepublishedCopy) -> Result<GcPreparedPublish> {
        let reconciled_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if matches!(
            &copy.plan.action,
            GcAction::DeleteSegment { .. }
                | GcAction::DeleteSegments { .. }
                | GcAction::ReclassifySegment { .. }
        ) {
            return Ok(GcPreparedPublish {
                copy,
                reconciled_lsn,
                skipped_records: Vec::new(),
            });
        }

        let current_epoch = self
            .index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)?;
        let mut survivors = Vec::with_capacity(copy.copied_records.len());
        let mut skipped_records = Vec::new();
        for mut record in std::mem::take(&mut copy.copied_records) {
            let skipped = if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                Some(GcSkippedCopiedRecordKind::Retired)
            } else {
                let state = match self.lsm.get(0, record.source.key.as_bytes(), &BlobMerge)? {
                    Some(encoded) => match decode_value(&encoded)? {
                        StoredValue::Inline(bytes) => Some(LsmBlobState::decode(bytes)?),
                        StoredValue::Blob { .. } => {
                            return Err(Error::InvariantViolation {
                                reason: format!(
                                    "materialized blob state for {:?} is segment-backed",
                                    record.source.key
                                ),
                            });
                        }
                    },
                    None => None,
                };
                match state {
                    Some(state)
                        if state
                            .versions
                            .get(&record.source.shard)
                            .is_some_and(|version| {
                                version.lsn == record.source.payload_lsn
                                    && version.record_ref == record.source.from
                            }) =>
                    {
                        match state.resolve(record.source.shard, current_epoch) {
                            Some((_, lifecycle)) => {
                                record.source.lifecycle = lifecycle;
                                None
                            }
                            None => Some(GcSkippedCopiedRecordKind::Expired),
                        }
                    }
                    _ => Some(GcSkippedCopiedRecordKind::Retired),
                }
            };
            if let Some(kind) = skipped {
                skipped_records.push(GcSkippedCopiedRecord { record, kind });
            } else {
                survivors.push(record);
            }
        }
        survivors.sort_by_key(|record| {
            (
                record.source.from.segment_id,
                record.source.from.offset,
                record.source.from.len,
                record.source.payload_lsn,
                record.source.shard,
                record.source.key.clone(),
            )
        });

        copy.copied_records = survivors;
        Ok(GcPreparedPublish {
            copy,
            reconciled_lsn,
            skipped_records,
        })
    }

    fn commit_gc_publish(&mut self, publish: GcPreparedPublish) -> Result<GcPublishResult> {
        let GcPreparedPublish {
            copy,
            reconciled_lsn,
            skipped_records,
        } = publish;
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
                    reconciled_lsn,
                    output_segments: Vec::new(),
                    published_records: Vec::new(),
                    skipped_records: Vec::new(),
                });
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
        }

        let survivors = copy.copied_records;
        for record in &survivors {
            if shard_generation_is_obsolete(&self.index, record.source.shard)? {
                return Err(Error::GcInvalidPlan(
                    "copied shard generation became obsolete before publish",
                ));
            }
        }

        if survivors.is_empty() {
            let mut batch = self.index.batch();
            for output in &copy.outputs {
                self.index
                    .put_segment_state_batch(&mut batch, &output.deleted_state(&self.config))?;
            }
            batch.write().map_err(strata_index::Error::from)?;
            return Ok(GcPublishResult {
                reconciled_lsn,
                output_segments: Vec::new(),
                published_records: Vec::new(),
                skipped_records: skipped_records
                    .into_iter()
                    .map(|skipped| skipped.record)
                    .collect(),
            });
        }

        let mut output_plan = self.plan_gc_output_segments(&copy.outputs, &survivors)?;
        let published_output_bytes =
            output_plan
                .published_outputs
                .iter()
                .try_fold(0_u64, |total, output| {
                    total
                        .checked_add(output.sealed_len)
                        .ok_or(strata_segment::Error::RangeOverflow)
                })?;
        let output_bytes_by_source =
            gc_output_bytes_by_source(&survivors, &skipped_records, &output_plan.used_staged_ids)?;
        let attributed_output_bytes =
            output_bytes_by_source
                .values()
                .try_fold(0_u64, |total, bytes| {
                    total
                        .checked_add(*bytes)
                        .ok_or(strata_segment::Error::RangeOverflow)
                })?;
        if attributed_output_bytes != published_output_bytes {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "published GC output bytes {published_output_bytes} do not match source-attributed bytes {attributed_output_bytes}"
                ),
            });
        }
        let skipped_output_ranges =
            skipped_gc_output_ranges(&skipped_records, &output_plan.staged_to_final_segment_id);
        let relocating_source_states = self.plan_gc_relocating_source_states(
            survivors.iter().map(|record| record.source.from.segment_id),
        )?;
        let first_publish_lsn = self.index.get_next_lsn()?;
        let publish_lsns = (0..survivors.len())
            .map(|offset| {
                first_publish_lsn
                    .checked_add(
                        u64::try_from(offset)
                            .map_err(|_| Error::from(strata_segment::Error::RangeOverflow))?,
                    )
                    .ok_or_else(|| Error::from(strata_segment::Error::RangeOverflow))
            })
            .collect::<Result<Vec<_>>>()?;
        let published_records = match assign_gc_publish_lsns(
            &publish_lsns,
            &survivors,
            &output_plan.staged_to_final_segment_id,
        ) {
            Ok(records) => records,
            Err(error) => {
                self.halt_writer_error("apply GC LSM lsns", &error);
                return Err(error);
            }
        };
        let reclaim_publish_lsn = published_records
            .first()
            .expect("surviving GC records are non-empty")
            .publish_lsn;
        let last_publish_lsn = published_records
            .last()
            .expect("surviving GC records are non-empty")
            .publish_lsn;
        apply_gc_output_lsn_bounds(&mut output_plan.segment_states, &published_records);
        let relocation_entries = published_records
            .iter()
            .map(|record| RelocationEntry {
                key: record.source.key.clone(),
                shard: record.source.shard,
                payload_lsn: record.source.payload_lsn,
                publish_lsn: record.publish_lsn,
                to: record.to,
            })
            .collect::<Vec<_>>();
        let wal_entries = relocation_entries
            .iter()
            .map(|entry| {
                Ok(WalEntry {
                    lsn: entry.publish_lsn,
                    payload: StoreWalMutation::Relocation(entry.clone()).encode()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if let Err(error) = self.wal.append(&wal_entries).map_err(Error::from) {
            self.halt_writer_error("GC store WAL append", &error);
            return Err(error);
        }
        let relocation_write = match self.relocations.write_batch(0, &relocation_entries) {
            Ok(write) => write,
            Err(error) => {
                let error = Error::from(error);
                self.halt_writer_error("GC relocation LSM write", &error);
                return Err(error);
            }
        };
        if relocation_write.lsns.len() != relocation_entries.len() {
            let error = Error::InvariantViolation {
                reason: "relocation LSM returned an unexpected LSN count".to_owned(),
            };
            self.halt_writer_error("GC relocation LSM write result", &error);
            return Err(error);
        }
        let store_checkpoint = match self.sync_store_files() {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.halt_writer_error("GC store durability checkpoint", &error);
                return Err(error);
            }
        };
        if self.wal.last_lsn() != Some(last_publish_lsn) {
            let error = Error::InvariantViolation {
                reason: format!(
                    "GC store WAL ends at {:?}, expected {last_publish_lsn}",
                    self.wal.last_lsn()
                ),
            };
            self.halt_writer_error("GC store durability frontier", &error);
            return Err(error);
        }
        let rolled_relocation_memtable = !relocation_write.rolled_memtables.is_empty();

        let pending_rollovers = self.take_pending_rollovers();
        let mut relocation_garbage = published_records
            .iter()
            .map(|record| {
                terminal_garbage_record(
                    record.source.key.as_bytes(),
                    record.publish_lsn,
                    record.source.from,
                    record.source.lifecycle,
                    GarbageEvent::Retired {
                        record: record.source.from,
                    },
                )
                .map_err(Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        relocation_garbage.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.lsn.cmp(&right.lsn))
                .then_with(|| left.event.record().offset.cmp(&right.event.record().offset))
                .then_with(|| left.event.record().len.cmp(&right.event.record().len))
        });
        let durability_publish_lock = Arc::clone(&self.durability_publish_lock);
        let mut skipped_output_delta = GcKnownDelta::default();
        let commit_result = (|| {
            let _publish_guard = durability_publish_lock
                .lock()
                .expect("durability publication lock poisoned");
            let committed_garbage = self
                .index
                .get_garbage_log_position(GARBAGE_LOG_HEAD)?
                .unwrap_or_default();
            let mut garbage_log = GarbageLog::open(
                garbage_log_dir(&self.config),
                LSM_GARBAGE_LOG_MAX_BYTES,
                committed_garbage,
            )
            .map_err(Error::from)?;
            let garbage_position = garbage_log
                .append(&relocation_garbage)
                .map_err(Error::from)?;
            let (output_summaries, output_garbage) = initial_gc_output_metadata(
                &output_plan.segment_states,
                &published_records,
                &skipped_records,
                &output_plan.staged_to_final_segment_id,
                self.index
                    .get_current_epoch()?
                    .ok_or(Error::EpochNotInitialized)?,
            )?;
            let mut output_garbage_positions = BTreeMap::new();
            for state in &output_plan.segment_states {
                let Some(records) = output_garbage.get(&state.segment_id) else {
                    continue;
                };
                let path = segment_garbage_log_path(segment_state_path(&self.config, state));
                let position = SegmentGarbageLog::open(path, 0)
                    .map_err(Error::from)?
                    .append(records)
                    .map_err(Error::from)?;
                output_garbage_positions.insert(state.segment_id, position);
            }

            let mut batch = self.index.batch();
            for rollover in &pending_rollovers {
                rollover.apply_batch(&self.index, &mut batch)?;
            }
            for state in &output_plan.segment_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
                self.index.put_segment_gc_summary_batch(
                    &mut batch,
                    state.segment_id,
                    output_summaries
                        .get(&state.segment_id)
                        .expect("every output has an initial summary"),
                )?;
                if let Some(position) = output_garbage_positions.get(&state.segment_id) {
                    self.index.put_segment_garbage_log_position_batch(
                        &mut batch,
                        state.segment_id,
                        *position,
                    )?;
                }
                self.index.put_segment_published_at_lsn_batch(
                    &mut batch,
                    state.segment_id,
                    reclaim_publish_lsn,
                )?;
            }
            for state in &relocating_source_states {
                self.index.put_segment_state_batch(&mut batch, state)?;
            }
            self.index.put_garbage_log_position_batch(
                &mut batch,
                GARBAGE_LOG_HEAD,
                garbage_position,
            )?;
            for (source_segment_id, output_bytes) in &output_bytes_by_source {
                self.index.put_gc_reclaim_pending_batch(
                    &mut batch,
                    *source_segment_id,
                    reclaim_publish_lsn,
                    *output_bytes,
                )?;
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
            for ((_segment_id, kind), ranges) in skipped_output_ranges {
                let bytes = ranges.iter().map(|range| range.len).sum::<u64>();
                skipped_output_delta.total_bytes += i128::from(bytes);
                match kind {
                    GcSkippedCopiedRecordKind::Retired => {
                        skipped_output_delta.retired_bytes += i128::from(bytes);
                    }
                    GcSkippedCopiedRecordKind::Expired => {
                        skipped_output_delta.expired_bytes += i128::from(bytes);
                    }
                }
            }

            let next_lsn = published_records
                .last()
                .and_then(|record| record.publish_lsn.checked_add(1))
                .ok_or(strata_segment::Error::RangeOverflow)?;
            self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
            self.index
                .put_published_lsn_batch(&mut batch, last_publish_lsn)?;
            self.index
                .put_store_checkpoint_batch(&mut batch, store_checkpoint)?;
            if let Err(error) = batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)
                .map_err(Error::from)
            {
                return Err(GcPublishCommitError::IndexCommit(error));
            }
            Ok::<(), GcPublishCommitError>(())
        })();

        match commit_result {
            Ok(()) => {
                self.last_durability_publish_at = Instant::now();
                if let Err(error) = self.lsm.materialize_through(last_publish_lsn, |edit| {
                    publish_blob_lsm_edit(&self.index, edit)
                }) {
                    let error = Error::from(error);
                    self.halt_writer_error("GC metadata materialization", &error);
                    return Err(error);
                }
                if rolled_relocation_memtable {
                    flush_relocation_lsm(
                        &self.index,
                        &self.relocations,
                        &self.relocation_cache,
                        &self.metrics,
                    )?;
                }
                for record in &published_records {
                    self.relocation_cache.insert(
                        record.source.key.clone(),
                        record.source.shard,
                        record.source.payload_lsn,
                        record.to,
                    );
                }
                self.metrics.apply_gc_known_delta(skipped_output_delta);
                self.metrics
                    .add_gc_relocating_segments(relocating_source_states.len());
                self.metrics
                    .record_gc_output_published(published_output_bytes);
                self.run_rollover_post_commit(pending_rollovers);
                let next_lsn = published_records
                    .last()
                    .and_then(|record| record.publish_lsn.checked_add(1))
                    .expect("published records are non-empty and checked above");
                self.metrics.set_next_lsn(next_lsn);
                self.metrics.set_published_lsn(last_publish_lsn);
                Ok(GcPublishResult {
                    reconciled_lsn,
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
                self.halt_writer_error("gc publish before index batch", &error);
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

    /// Fences every source that published at least one surviving relocation.
    ///
    /// The state transition shares the same RocksDB batch as the relocation-table descriptors.
    /// A planner therefore cannot observe a committed relocation without also observing that its
    /// source is ineligible for another copy plan.
    fn plan_gc_relocating_source_states(
        &self,
        source_ids: impl IntoIterator<Item = SegmentId>,
    ) -> Result<Vec<SegmentState>> {
        let source_ids = source_ids.into_iter().collect::<BTreeSet<_>>();
        let mut states = Vec::with_capacity(source_ids.len());
        for segment_id in source_ids {
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
            state.state = SegmentFileState::GcRelocating;
            states.push(state);
        }
        Ok(states)
    }

    fn delete_empty_gc_segments(&self, segment_ids: &[SegmentId]) -> Result<()> {
        let mut states = Vec::with_capacity(segment_ids.len());
        let mut states_to_commit = Vec::new();
        let mut summaries_to_remove = Vec::new();
        let mut relocating_segments_to_remove = 0;
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
            if !matches!(
                state.state,
                SegmentFileState::Sealed | SegmentFileState::GcRelocating
            ) {
                return Err(Error::GcSourceSegmentNotSealed {
                    segment_id: *segment_id,
                    state: state.state,
                });
            }
            let summary = self
                .index
                .get_segment_gc_summary(*segment_id)?
                .unwrap_or_default();
            if summary.live_ref_count != 0 {
                return Err(Error::GcSourceSegmentNotEmpty {
                    segment_id: *segment_id,
                    live_ref_count: summary.live_ref_count,
                });
            }
            let published_at_lsn = self.index.get_segment_published_at_lsn(state.segment_id)?;
            if self.live_snapshots.protects(published_at_lsn) {
                continue;
            }

            if state.state == SegmentFileState::GcRelocating {
                relocating_segments_to_remove += 1;
            }
            state.state = SegmentFileState::Deleted;
            summaries_to_remove.push(summary);
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
            for summary in &summaries_to_remove {
                self.metrics.remove_gc_known_summary(summary);
            }
            self.metrics
                .remove_gc_relocating_segments(relocating_segments_to_remove);
        }

        for state in &states {
            self.reader_cache.evict(state.segment_id);
            self.metrics.record_reader_cache_eviction();
        }
        let unlinked_segments = unlink_gc_segment_files(&self.config, &states)?;
        if !unlinked_segments.is_empty() {
            let source_segment_ids = unlinked_segments
                .iter()
                .map(|(segment_id, _)| *segment_id)
                .collect::<Vec<_>>();
            let mut batch = self.index.batch();
            let output_bytes_by_source = self
                .index
                .remove_gc_reclaim_pending_for_sources_batch(&mut batch, &source_segment_ids)?;
            batch.write().map_err(strata_index::Error::from)?;
            for (segment_id, source_bytes) in unlinked_segments {
                let output_bytes = output_bytes_by_source
                    .get(&segment_id)
                    .copied()
                    .unwrap_or(0);
                self.metrics
                    .record_gc_source_deleted(source_bytes, output_bytes);
            }
        }
        Ok(())
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
    /// map translates temporary staged segment ids to final durable segment ids.
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

    /// Validates a client batch and assigns its contiguous store-owned LSN range.
    fn prepare_batch(&self, ops: Vec<BatchOp>) -> Result<PreparedBatch> {
        let mut prepared_ops = Vec::with_capacity(ops.len());
        let mut op_epochs = Vec::with_capacity(ops.len());
        let mut current_epoch = self.index.get_current_epoch()?;
        let mut next_lsn = self.index.get_next_lsn()?;

        for op in ops {
            let lsn = next_lsn;
            next_lsn = next_lsn
                .checked_add(1)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            match op {
                BatchOp::Put {
                    shard_id,
                    key,
                    payload,
                } => {
                    // Why do we care about the current epoch here?
                    // The reason to remember the epoch at which the put was submitted is to
                    // ensure that this put's visibility can be judged by LSM compaction later on.
                    // Imagine if this was the sequence:
                    // current_epoch = 10
                    // LSN 100: SetLifetime { logical_end_epoch: 50 }
                    // LSN 101: Put { key: "foo", payload: "bar" }
                    // LSN 102: Put { key: "foo", payload: "baz" }
                    // LSN 103: ChangeEpoch { epoch: 50 }
                    // LSN 104: Put { key: "baz", payload: "qux"}
                    // The first Put has no explicit lifecycle, but the key already has an explicit
                    // lifecycle ending at epoch 50. Since 50 > current_epoch(10), compaction lets
                    // the new physical record inherit that lifecycle. GC then knows the bytes
                    // for "bar" belong in the "expires at 50" segment.
                    // Later when the second Put comes along at epoch < 50, compaction lets it
                    // inherit the lifecycle of the key, and the bytes for "baz" belong in the
                    // "expires at 50" segment. The bytes for "bar" at this point are eligible for
                    // garbage collection since the key is overwritten.
                    // Subsequently epoch advances to 50 and the final Put at LSN 104 happens and
                    // if do not record the current epoch at which this put was submitted, then
                    // compaction would not know that the bytes for "qux" should not inherit
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
                        logical_end_epoch,
                        current_epoch: epoch,
                    });
                    op_epochs.push(None);
                }
                BatchOp::Tombstone { shard_id, key } => {
                    let shard = self.openable_shard_key(shard_id)?;
                    prepared_ops.push(PreparedBatchOp::Tombstone { shard, key, lsn });
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
        }

        Ok(PreparedBatch {
            result: BatchWriteResult {
                op_lsns: Vec::with_capacity(prepared_ops.len()),
                op_epochs,
            },
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
    /// Epoch changes, active segment offsets, rollover rows, and `next_lsn` move together. Blob
    /// state is already ordered and visible in the LSM at this point.
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
                PreparedBatchOp::Put { .. } => {
                    wrote_payload = true;
                }
                PreparedBatchOp::Lifecycle { .. } | PreparedBatchOp::Tombstone { .. } => {}
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
        let next_lsn = prepared
            .result
            .last_lsn()
            .and_then(|lsn| lsn.checked_add(1))
            .ok_or(strata_segment::Error::RangeOverflow)?;
        self.index.put_next_lsn_batch(&mut batch, next_lsn)?;
        batch.write().map_err(strata_index::Error::from)?;
        Ok(())
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

    fn request_lsm_flush(&self, rolled_memtable: bool) {
        if rolled_memtable && self.lsm_flush_tx.send(()).is_err() {
            let reason = "LSM memtable flusher stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    fn request_lsm_compaction(&self) {
        if self.lsm_compact_tx.send(()).is_err() {
            let reason = "LSM compactor stopped".to_owned();
            self.store_halt.halt(reason.clone());
            self.lsm.halt(reason);
        }
    }

    /// Rolls a non-empty payload segment on its own cadence.
    ///
    /// This is not a durability publication: it syncs the segment being closed, but it neither
    /// syncs the store WAL nor advances `PublishedLsn`. `sync_data` owns that separate boundary.
    fn process_segment_rollover(&mut self) -> Result<()> {
        self.last_segment_rollover_at = Instant::now();
        let sealed_before_lsn = self.index.get_next_lsn()?;
        if sealed_before_lsn <= self.last_segment_rollover_next_lsn {
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

    /// Rolls the store-owned payload segment and stages its RocksDB metadata.
    ///
    /// Segment rollover is intentionally here, beside WAL ownership. The LSM sees only the
    /// `RecordRef` produced after this method installs the next segment.
    fn rollover_active_segment(&mut self, sealed_before_lsn: StrataLsn) -> Result<()> {
        self.wait_for_seal_backlog_capacity()?;
        let old_segment_id = self.active_segment_state.segment_id;
        let sealed_length = self.segment.write_offset();
        if self.segment.segment_id() != old_segment_id
            || sealed_length != self.active_segment_state.write_offset
        {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "payload writer is segment {} at {sealed_length}, expected {old_segment_id} at {}",
                    self.segment.segment_id(),
                    self.active_segment_state.write_offset
                ),
            });
        }
        // A rollover is rare and already enters the sealing path, so synchronously closing this
        // segment keeps the hand-off obvious without adding another pending-sync state machine.
        self.segment.sync_data()?;
        let next = self.segment_factory.create()?;
        let new_segment_id = next.segment_id();
        if new_segment_id <= old_segment_id {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "replacement segment {new_segment_id} must follow active segment {old_segment_id}"
                ),
            });
        }
        let new_state =
            active_segment_state_from_path(&self.config, self.ingest_owner, new_segment_id, 0, 0);
        let mut old_state = self.active_segment_state.clone();
        old_state.write_offset = sealed_length;
        old_state.durable_offset = self.durable_offset;
        old_state.state = SegmentFileState::Sealing;
        old_state.sealed_before_lsn = Some(sealed_before_lsn);

        self.pending_rollovers.push(PendingRollover {
            old_segment_state: old_state,
            new_segment_state: new_state.clone(),
            new_segment_published_at_lsn: sealed_before_lsn,
            seal_task: SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: sealed_length,
                sealed_before_lsn,
                allocation_records: self.pending_allocation_records,
            },
        });
        self.segment = next;
        self.pending_allocation_records = 0;
        self.active_segment_state = new_state;
        self.durable_offset = 0;
        self.last_segment_rollover_at = Instant::now();
        self.last_segment_rollover_next_lsn = sealed_before_lsn;
        self.metrics.set_active_segment(
            self.active_segment_state.segment_id,
            self.active_segment_state.write_offset,
            self.durable_offset,
        );
        Ok(())
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

    /// Syncs the two store-owned append streams and returns their physical coordinates.
    ///
    /// The returned value has no logical frontier. Its LSN is the `PublishedLsn` written beside it
    /// in the same RocksDB batch by the caller.
    fn sync_store_files(&mut self) -> Result<StoreCheckpoint> {
        self.segment.sync_data()?;
        let wal_position = self.wal.sync()?;
        self.wal.wait_for_sync(wal_position)?;
        Ok(StoreCheckpoint {
            wal_position,
            active_segment_id: self.segment.segment_id(),
            active_segment_offset: self.segment.write_offset(),
        })
    }

    /// Advances both keyed projections, then reclaims only the store-WAL prefix covered by both.
    fn reclaim_store_wal(&mut self, published_lsn: StrataLsn) -> Result<()> {
        self.lsm.materialize_through(published_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        self.relocations
            .lsm()
            .materialize_through(published_lsn, |edit| {
                publish_relocation_lsm_edit(&self.index, edit)
            })?;

        let blob_frontier = self.lsm.manifest().materialized_through.unwrap_or_default();
        let relocation_frontier = self
            .relocations
            .lsm()
            .manifest()
            .materialized_through
            .unwrap_or_default();
        let reclaim_through = blob_frontier.min(relocation_frontier);
        if reclaim_through == 0 {
            return Ok(());
        }

        let retained_from = self.wal.retained_from_after(reclaim_through)?;
        let persisted = self.index.get_store_wal_retained_from()?;
        let current = persisted.unwrap_or(self.lsm.manifest().wal_retained_from);
        if retained_from > current || persisted.is_none() {
            // Publish the store-owned deletion boundary before unlinking anything. Falling back
            // to the manifest migrates databases written when this value lived on the blob LSM.
            let mut batch = self.index.batch();
            self.index
                .put_store_wal_retained_from_batch(&mut batch, retained_from.max(current))?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
        }
        self.wal.reclaim_through(reclaim_through)?;
        Ok(())
    }

    /// The durability step. The ordering here is the single most load bearing thing in this
    /// file:
    ///
    /// 1. fsync the store-owned payload segment, then the store WAL,
    /// 2. then write durable offsets, allocation baseline, and published_lsn to the index,
    /// 3. then fsync the RocksDB WAL.
    ///
    /// Bytes become durable strictly before the metadata that claims they are. A crash between
    /// any two steps leaves the index claiming *less* than what's on disk — never more — and
    /// recovery re-derives the frontier (it can even promote bytes the crash interrupted us from
    /// claiming). Reversing 1 and 2 would let a persisted published_lsn point at bytes that never
    /// reached the platter, which is the one lie this design must never tell, because the Walrus
    /// event cursor advances based on it.
    ///
    /// The allocation baseline, store checkpoint, and published_lsn share one RocksDB batch, so
    /// compaction cannot retire a published record before GC knows that record started live.
    fn sync_data(&mut self, mut profile: Option<&mut StoreSyncProfile>) -> Result<()> {
        let started = Instant::now();
        let previous_durable_offset = self.durable_offset;
        let durable_offset = self.active_segment_state.write_offset;
        let durability_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.segment_sync += elapsed,
            || self.sync_store_files(),
        );
        let store_checkpoint = match durability_result {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.metrics.record_sync(Err(()), started.elapsed());
                return Err(error.into());
            }
        };
        let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if self.wal.last_lsn().unwrap_or_default() != committed_lsn
            || store_checkpoint.active_segment_id != self.active_segment_state.segment_id
            || store_checkpoint.active_segment_offset != durable_offset
        {
            let error = Error::InvariantViolation {
                reason: format!(
                    "store checkpoint {store_checkpoint:?} does not match store LSN {committed_lsn} and active segment {} at {durable_offset}",
                    self.active_segment_state.segment_id
                ),
            };
            self.metrics.record_sync(Err(()), started.elapsed());
            self.halt_writer_error("sync store checkpoint", &error);
            return Err(error);
        }
        let _publish_guard = self
            .durability_publish_lock
            .lock()
            .expect("durability publish lock poisoned");
        let (state, batch, published_lsn) = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.published_lsn_compute += elapsed,
            || {
                let mut state = self.active_segment_state.clone();
                if let Some(existing) = self
                    .index
                    .get_segment_state(self.active_segment_state.segment_id)?
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
                state.write_offset = self.active_segment_state.write_offset;
                state.durable_offset = durable_offset;
                let mut batch = self.index.batch();
                self.index.put_segment_state_batch(&mut batch, &state)?;
                publish_segment_allocation_baseline(
                    &self.index,
                    &mut batch,
                    state.segment_id,
                    durable_offset,
                    self.pending_allocation_records,
                )?;
                let current_published_lsn = self.index.get_published_lsn()?;
                if current_published_lsn > committed_lsn {
                    return Err(Error::InvariantViolation {
                        reason: format!(
                            "published LSN {current_published_lsn} follows committed LSN {committed_lsn}"
                        ),
                    });
                }
                let published_lsn = committed_lsn;
                self.index
                    .put_published_lsn_batch(&mut batch, published_lsn)?;
                self.index
                    .put_store_checkpoint_batch(&mut batch, store_checkpoint)?;
                Ok::<_, Error>((state, batch, published_lsn))
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
        self.pending_allocation_records = 0;
        self.last_durability_publish_at = Instant::now();
        profile_phase(
            profile,
            |profile, elapsed| profile.state_update += elapsed,
            || {
                self.durable_offset = durable_offset;
                self.active_segment_state.durable_offset = durable_offset;
                self.metrics.set_active_segment(
                    self.active_segment_state.segment_id,
                    self.active_segment_state.write_offset,
                    self.durable_offset,
                );
                self.metrics.set_published_lsn(published_lsn);
            },
        );
        // Reclaiming the WAL may publish LSM materialization frontiers. Those writes do not need
        // to share the durability-publication mutex: published_lsn and the store checkpoint are
        // already committed, and materialization can only lag that durable frontier.
        drop(_publish_guard);
        if let Err(error) = self.reclaim_store_wal(published_lsn) {
            self.halt_writer_error("reclaim store WAL", &error);
            return Err(error);
        }
        self.metrics.record_sync(
            Ok(durable_offset.saturating_sub(previous_durable_offset)),
            started.elapsed(),
        );
        self.gc_concurrency.observe_sync(
            started.elapsed(),
            durable_offset.saturating_sub(previous_durable_offset),
        );
        self.request_lsm_compaction();
        Ok(())
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
    /// Durable segment state rows to publish in the metadata batch.
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
    /// Whether those bytes should be classified as retired or expired garbage.
    kind: GcSkippedCopiedRecordKind,
}

/// Attributes every byte retained in published GC outputs to its original source segment.
///
/// A published output can contain a staged record that became stale during copy reconciliation as
/// long as another record in the same output survived. Those stale bytes still occupy disk, so
/// they must be included when computing net reclamation for the eventual source deletion.
fn gc_output_bytes_by_source(
    survivors: &[GcStagedCopiedRecord],
    skipped: &[GcSkippedCopiedRecord],
    used_staged_ids: &BTreeSet<SegmentId>,
) -> Result<BTreeMap<SegmentId, u64>> {
    let mut bytes_by_source = BTreeMap::new();
    let records = survivors
        .iter()
        .chain(skipped.iter().map(|skipped| &skipped.record));
    for record in records {
        if !used_staged_ids.contains(&record.staged.segment_id) {
            continue;
        }
        let bytes = bytes_by_source
            .entry(record.source.from.segment_id)
            .or_insert(0_u64);
        *bytes = bytes
            .checked_add(record.staged.len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
    }
    Ok(bytes_by_source)
}

/// Assigns consecutive publish LSNs and final destination refs to copied survivors.
///
/// The staged record already knows its offset and length inside a temporary output file. This helper
/// replaces the temporary segment id with the final durable segment id and pairs each move with the
/// LSN that orders its relocation in the main LSM.
fn assign_gc_publish_lsns(
    lsns: &[StrataLsn],
    records: &[GcStagedCopiedRecord],
    staged_to_final_segment_id: &BTreeMap<SegmentId, SegmentId>,
) -> Result<Vec<GcPublishedRecord>> {
    if lsns.len() != records.len() {
        return Err(Error::InvariantViolation {
            reason: format!(
                "LSM returned {} GC lsns for {} records",
                lsns.len(),
                records.len()
            ),
        });
    }
    records
        .iter()
        .zip(lsns)
        .map(|(record, lsn)| {
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
                publish_lsn: *lsn,
            })
        })
        .collect()
}

/// Updates output segment logical bounds from the records committed by one GC batch.
///
/// GC output segments are sealed before they enter the manifest, so their `write_offset` is already
/// known. `min_lsn`/`max_lsn` are the per-segment relocation bounds used by later completeness
/// checks. The companion publication-LSN row is written atomically with these states.
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
/// whole sealed output file becomes durable, so stale ranges inside it must be classified as garbage
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

#[allow(clippy::type_complexity)]
fn initial_gc_output_metadata(
    states: &[SegmentState],
    published: &[GcPublishedRecord],
    skipped: &[GcSkippedCopiedRecord],
    staged_to_final: &BTreeMap<SegmentId, SegmentId>,
    current_epoch: Epoch,
) -> Result<(
    BTreeMap<SegmentId, SegmentGcSummary>,
    BTreeMap<SegmentId, Vec<GarbageRecord>>,
)> {
    let mut summaries = states
        .iter()
        .map(|state| (state.segment_id, SegmentGcSummary::default()))
        .collect::<BTreeMap<_, _>>();
    let mut garbage = BTreeMap::<SegmentId, Vec<GarbageRecord>>::new();

    for record in published {
        let lifecycle = record.source.lifecycle;
        let expired =
            lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch);
        let summary =
            summaries
                .get_mut(&record.to.segment_id)
                .ok_or_else(|| Error::InvariantViolation {
                    reason: format!(
                        "published GC record targets missing output segment {}",
                        record.to.segment_id
                    ),
                })?;
        add_initial_gc_output_record(
            summary,
            record.to,
            lifecycle,
            expired.then_some(GcSkippedCopiedRecordKind::Expired),
        )?;

        let event = if expired {
            Some(GarbageEvent::Expired { record: record.to })
        } else {
            lifecycle.map(|lifecycle| GarbageEvent::SetLifecycle {
                record: record.to,
                lifecycle: Some(lifecycle),
            })
        };
        if let Some(event) = event {
            garbage
                .entry(record.to.segment_id)
                .or_default()
                .push(GarbageRecord {
                    key: SegmentKey {
                        segment_id: record.to.segment_id,
                        blob_key: record.source.key.clone(),
                    },
                    lsn: record.publish_lsn,
                    event,
                    summary_delta: Default::default(),
                });
        }
    }

    for skipped in skipped {
        let Some(&segment_id) = staged_to_final.get(&skipped.record.staged.segment_id) else {
            continue;
        };
        let record_ref = RecordRef {
            segment_id,
            offset: skipped.record.staged.offset,
            len: skipped.record.staged.len,
        };
        let summary = summaries
            .get_mut(&segment_id)
            .ok_or_else(|| Error::InvariantViolation {
                reason: format!("skipped GC record targets missing output segment {segment_id}"),
            })?;
        add_initial_gc_output_record(summary, record_ref, None, Some(skipped.kind))?;
        let event = match skipped.kind {
            GcSkippedCopiedRecordKind::Retired => GarbageEvent::Retired { record: record_ref },
            GcSkippedCopiedRecordKind::Expired => GarbageEvent::Expired { record: record_ref },
        };
        garbage.entry(segment_id).or_default().push(GarbageRecord {
            key: SegmentKey {
                segment_id,
                blob_key: skipped.record.source.key.clone(),
            },
            lsn: skipped.record.source.payload_lsn,
            event,
            summary_delta: Default::default(),
        });
    }

    for state in states {
        let summary = summaries
            .get_mut(&state.segment_id)
            .expect("summary was initialized from this state");
        if summary.total_bytes != state.sealed_len.unwrap_or_default() {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "GC output segment {} accounts for {} of {} sealed bytes",
                    state.segment_id,
                    summary.total_bytes,
                    state.sealed_len.unwrap_or_default()
                ),
            });
        }
        summary.min_live_end_epoch = summary.future_epoch_histogram.keys().next().copied();
        summary.max_live_end_epoch = summary.future_epoch_histogram.keys().next_back().copied();
    }

    Ok((summaries, garbage))
}

fn add_initial_gc_output_record(
    summary: &mut SegmentGcSummary,
    record: RecordRef,
    lifecycle: Option<BlobLifecycle>,
    terminal: Option<GcSkippedCopiedRecordKind>,
) -> Result<()> {
    checked_summary_add(&mut summary.total_bytes, record.len, record.segment_id)?;
    match terminal {
        Some(GcSkippedCopiedRecordKind::Retired) => {
            checked_summary_add(&mut summary.retired_bytes, record.len, record.segment_id)?;
        }
        Some(GcSkippedCopiedRecordKind::Expired) => {
            checked_summary_add(&mut summary.expired_bytes, record.len, record.segment_id)?;
            if let Some(lifecycle) = lifecycle {
                checked_summary_add(
                    summary
                        .extension_count_histogram
                        .entry(lifecycle.extension_count)
                        .or_default(),
                    1,
                    record.segment_id,
                )?;
            }
        }
        None => {
            checked_summary_add(&mut summary.live_bytes, record.len, record.segment_id)?;
            checked_summary_add(&mut summary.live_ref_count, 1, record.segment_id)?;
            match lifecycle {
                Some(lifecycle) => {
                    let bucket = summary
                        .future_epoch_histogram
                        .entry(lifecycle.logical_end_epoch)
                        .or_default();
                    checked_summary_add(&mut bucket.bytes, record.len, record.segment_id)?;
                    checked_summary_add(&mut bucket.refs, 1, record.segment_id)?;
                    checked_summary_add(
                        summary
                            .extension_count_histogram
                            .entry(lifecycle.extension_count)
                            .or_default(),
                        1,
                        record.segment_id,
                    )?;
                }
                None => {
                    checked_summary_add(
                        &mut summary.unknown_lifetime_bytes,
                        record.len,
                        record.segment_id,
                    )?;
                    checked_summary_add(
                        &mut summary.unknown_lifetime_ref_count,
                        1,
                        record.segment_id,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn checked_summary_add(value: &mut u64, amount: u64, segment_id: SegmentId) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("GC summary overflow for segment {segment_id}"),
        })?;
    Ok(())
}

fn gc_segment_file_path(config: &StrataStoreConfig, state: &SegmentState) -> std::path::PathBuf {
    if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    }
}

pub(crate) fn segment_garbage_log_path(mut segment_path: PathBuf) -> PathBuf {
    segment_path.set_extension("glog");
    segment_path
}

fn unlink_gc_segment_file(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    unlink_gc_segment_files(config, std::slice::from_ref(state)).map(|_| ())
}

fn unlink_gc_segment_files(
    config: &StrataStoreConfig,
    states: &[SegmentState],
) -> Result<Vec<(SegmentId, u64)>> {
    let mut parents = BTreeSet::new();
    let mut unlinked_segments = Vec::new();
    for state in states {
        let path = gc_segment_file_path(config, state);
        let file_len = match fs::metadata(&path) {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(Error::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        match fs::remove_file(&path) {
            Ok(()) => {
                unlinked_segments.push((
                    state.segment_id,
                    file_len.expect("metadata existed before removal"),
                ));
                if let Some(parent) = path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
        let garbage_path = segment_garbage_log_path(path.clone());
        match fs::remove_file(&garbage_path) {
            Ok(()) => {
                if let Some(parent) = garbage_path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(Error::Io {
                    path: garbage_path,
                    source,
                });
            }
        }
    }

    for parent in parents {
        sync_dir(&parent)?;
        prune_empty_retention_dirs(config, parent)?;
    }
    Ok(unlinked_segments)
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

/// Removes files owned exclusively by the retired projection engine.
///
/// The exact namespace child is fixed by the old layout. Refusing non-directories avoids following
/// a replacement symlink or deleting an unexpected file.
fn cleanup_retired_projection_dir(config: &StrataStoreConfig) -> Result<()> {
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

/// Resolves a key to its readable payload, or None for missing/tombstoned blobs.
///
/// A live head without a payload ref should not be produced by new writes. If recovery leaves such
/// a head behind, there are no bytes to return, so None is the honest answer.
pub(crate) fn resolve_blob_version(
    store: &StrataStore,
    shard: ShardKey,
    key: &BlobKey,
) -> Result<Option<ResolvedBlobVersion>> {
    let lsm = store.lsm()?;
    let Some(encoded) = lsm.get(0, key.as_bytes(), &BlobMerge)? else {
        return Ok(None);
    };
    let StoredValue::Inline(bytes) = decode_value(&encoded)? else {
        return Err(Error::InvariantViolation {
            reason: format!("materialized blob state for {key:?} is segment-backed"),
        });
    };
    let state = LsmBlobState::decode(bytes)?;
    let current_epoch = store.current_epoch()?;
    let Some((version, lifecycle)) = state.resolve(shard, current_epoch) else {
        return Ok(None);
    };
    Ok(Some(ResolvedBlobVersion {
        head_lsn: version.lsn,
        record_ref: version.record_ref,
        generation: version.lsn,
        lifecycle,
        payload_lsn: version.lsn,
    }))
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

fn open_lsm(
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
    let manifest = Arc::new(load_blob_lsm_manifest(index)?);
    Ok(Arc::new(Lsm::from_parts(
        lsm_dir.join("tables"),
        manifest,
        recovered,
        last_lsn,
        options,
    )?))
}

fn open_store_wal(
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
    let (sync_tx, syncer) = file_sync_channel(LSM_FILE_SYNC_QUEUE_CAPACITY);
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
    let published_lsn = match index.get_published_lsn()? {
        0 => None,
        lsn => Some(lsn),
    };
    let (materialized_through, retained_from) = store_wal_recovery_state(index)?;
    let wal = Wal::recover(
        config.namespace_dir().join("wal"),
        config.segment_max_bytes,
        checkpoint_position,
        published_lsn,
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

/// Returns the two store-wide facts needed to recover the shared WAL.
///
/// A prefix is replay-safe only when both keyed projections have materialized it. For example,
/// blob=100 and relocation=80 means the store frontier is 80, never 100. The retained file ID is
/// store state; the blob-manifest value is read only to open databases created before that state
/// key existed.
fn store_wal_recovery_state(index: &StrataIndex) -> Result<(Option<StrataLsn>, u64)> {
    let blob = load_blob_lsm_manifest(index)?;
    let relocation = load_relocation_lsm_manifest(index)?;
    let materialized_through = blob
        .materialized_through
        .zip(relocation.materialized_through)
        .map(|(blob, relocation)| blob.min(relocation));
    let retained_from = index
        .get_store_wal_retained_from()?
        .unwrap_or(blob.wal_retained_from);
    Ok((materialized_through, retained_from))
}

fn open_relocation_lsm(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    next_lsn: StrataLsn,
    recovered: Vec<RelocationEntry>,
) -> Result<Arc<RelocationStore>> {
    let root = config.relocation_dir();
    let manifest = Arc::new(load_relocation_lsm_manifest(index)?);
    let recovered = recovered
        .into_iter()
        .map(|entry| (entry.publish_lsn, RelocationStore::lsm_mutation(0, &entry)))
        .collect();
    let last_lsn = next_lsn.checked_sub(1).filter(|lsn| *lsn != 0);
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

fn load_blob_lsm_manifest(index: &StrataIndex) -> Result<LsmManifest> {
    if let Some(manifest) = index.get_lsm_manifest(BLOB_LSM_MANIFEST)? {
        if manifest.schema_id != LSM_BASE_FORMAT
            || manifest.patch_format_id != LSM_PATCH_FORMAT
            || manifest.partition_count != 1
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

    let manifest = LsmManifest::empty(LSM_BASE_FORMAT, LSM_PATCH_FORMAT, NonZeroU32::MIN);
    let mut batch = index.batch();
    index.put_lsm_manifest_batch(&mut batch, BLOB_LSM_MANIFEST, &manifest)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(manifest)
}

fn load_relocation_lsm_manifest(index: &StrataIndex) -> Result<LsmManifest> {
    if let Some(manifest) = index.get_lsm_manifest(RELOCATION_LSM_MANIFEST)? {
        if manifest.schema_id != RELOCATION_LSM_BASE_FORMAT
            || manifest.patch_format_id != RELOCATION_LSM_PATCH_FORMAT
            || manifest.partition_count != 1
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
        NonZeroU32::MIN,
    );
    let mut batch = index.batch();
    index.put_lsm_manifest_batch(&mut batch, RELOCATION_LSM_MANIFEST, &manifest)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    Ok(manifest)
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
/// 2. Roll back the logical tail beginning with the first payload that did not survive.
///
/// This deliberately does not advance `next_lsn` from records found only in segment files. The
/// segment file is the payload log, not the commit log: a batch can reserve LSN 10 for an epoch
/// increment and LSN 11 for a put, write the LSN 11 payload record, then crash before the RocksDB
/// batch publishes either operation. If recovery treated that segment record as committed and
/// bumped `next_lsn` to 12, it would create a hole at LSN 10 and silently drop the epoch change.
/// Even a put-only batch has the same shape: if the process exits after writing payload bytes but
/// before RocksDB publishes the batch, recovery must not make those bytes visible. Only RocksDB's
/// batch tells us which LSNs committed; segment recovery can promote/truncate bytes for
/// already-committed operations, but it must not discover new committed LSNs from payload bytes
/// alone.
///
/// The caller next validates the same logical prefix against the store WAL. A complete tail is
/// promoted; an incomplete unpublished tail is truncated from both the logical and segment paths.
fn recover_unsealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut discard_later_segments = false;
    let mut rollback_from = None;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if discard_later_segments {
            if let Some(state) = index.get_segment_state(segment_id)? {
                rollback_from = min_lsn(rollback_from, state.min_lsn);
            }
            discard_unsealed_segment(config, index, segment_id, metrics)?;
            continue;
        }

        let recovered = recover_unsealed_segment(config, index, segment_id, metrics)?;
        metrics.record_recovered_segment(recovered.is_complete);
        if !recovered.is_complete {
            discard_later_segments = true;
            rollback_from = min_lsn(rollback_from, recovered.rollback_from);
        }
    }
    if let Some(rollback_from) = rollback_from {
        rollback_operations_from(index, metrics, rollback_from)?;
    }
    Ok(())
}

/// Removes payload records that belong to a logical WAL tail being rolled back.
///
/// Metadata-only operations do not consume segment bytes, so some segments need no change. When
/// payloads are present, every byte at or after the first rolled-back LSN is truncated and the
/// segment GC baseline is rebuilt by the normal recovered-prefix publisher.
fn truncate_unsealed_segments_from_lsn(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    rollback_from: StrataLsn,
) -> Result<()> {
    for segment_id in unsealed_ingest_segment_ids(index)? {
        let Some(existing_state) = index.get_segment_state(segment_id)? else {
            continue;
        };
        let path = segment_path(config, segment_id);
        if !path.exists() {
            continue;
        }
        let mut scanner = SegmentScanner::open(&path, segment_id)?;
        let prefix = scanner.scan_recoverable_prefix(existing_state.durable_offset)?;
        let Some(first_hidden) = prefix
            .records
            .iter()
            .find(|record| record.header.generation >= rollback_from)
        else {
            continue;
        };
        let recovered_write_offset = first_hidden.record_ref.offset;
        // The preceding segment scan may have fsynced a complete buffered tail before WAL
        // validation discovered that the matching logical operations were unavailable. Those
        // promoted bytes are still newer than published_lsn and may be truncated here.
        let retained_durable_offset = existing_state.durable_offset.min(recovered_write_offset);
        let recovered_durable_offset = persist_recovered_segment_prefix(
            &path,
            prefix.file_len,
            retained_durable_offset,
            recovered_write_offset,
        )?;
        apply_recovered_segment_prefix(
            config,
            index,
            segment_id,
            RecoveredSegmentPrefix {
                existing_state: Some(existing_state),
                durable_offset: recovered_durable_offset,
                recovered_write_offset,
                records: &prefix.records,
            },
            metrics,
        )?;
    }
    Ok(())
}

fn min_lsn(current: Option<StrataLsn>, candidate: Option<StrataLsn>) -> Option<StrataLsn> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (current, candidate) => current.or(candidate),
    }
}

/// Validates the store WAL against the logical prefix selected by segment recovery.
///
/// A complete WAL tail is retained even when its checkpoint metadata did not reach disk: opening
/// the WAL will fsync and promote it. If the exact tail is unavailable, point-in-time recovery
/// discards only operations newer than `published_lsn`; that frontier is the last prefix callers
/// were promised would survive. The fallback target is validated independently, so corruption in
/// the published prefix remains a hard recovery error.
fn recover_store_wal_prefix(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let requested_lsn = index.get_next_lsn()?.checked_sub(1).filter(|lsn| *lsn != 0);
    let published_lsn = index.get_published_lsn()?;
    let published_target = (published_lsn != 0).then_some(published_lsn);
    let checkpoint = index.get_store_checkpoint()?;
    let (materialized_through, retained_from) = store_wal_recovery_state(index)?;
    let validate = |checkpoint: Option<StoreCheckpoint>, last_lsn| {
        Wal::validate_recovery_target(
            config.namespace_dir().join("wal"),
            checkpoint.map_or(WalPosition::default(), |checkpoint| checkpoint.wal_position),
            published_target,
            materialized_through,
            retained_from,
            last_lsn,
        )
    };
    if validate(checkpoint, requested_lsn).is_ok() {
        return Ok(());
    }
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency {
        validate(checkpoint, requested_lsn)?;
        unreachable!("failed WAL validation returned success on retry");
    }

    if requested_lsn.is_none_or(|requested| requested <= published_lsn) {
        validate(checkpoint, requested_lsn)?;
        unreachable!("failed published WAL validation returned success on retry");
    }
    let fallback_checkpoint = if published_lsn == 0 { None } else { checkpoint };
    validate(fallback_checkpoint, published_target)?;
    if fallback_checkpoint != checkpoint {
        let mut batch = index.batch();
        index.put_store_checkpoint_batch(
            &mut batch,
            StoreCheckpoint {
                wal_position: WalPosition::default(),
                active_segment_id: 0,
                active_segment_offset: 0,
            },
        )?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
    }

    let rollback_from = published_lsn
        .checked_add(1)
        .ok_or(strata_segment::Error::RangeOverflow)?;
    truncate_unsealed_segments_from_lsn(config, index, metrics, rollback_from)?;
    rollback_operations_from(index, metrics, rollback_from)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentRecovery {
    is_complete: bool,
    rollback_from: Option<StrataLsn>,
}

/// Recovers one unsealed segment by scanning records from offset 0 and keeping the longest
/// checksummed-valid prefix.
///
/// The scan deliberately validates *past* the persisted durable offset: after a process crash
/// (as opposed to power loss) appended bytes usually survive in the kernel page cache, and after
/// a power loss they may still have been fsynced without the durable-offset row committing. If
/// complete records are sitting there inside the committed write prefix, throwing them away would
/// be rolling back writes for no reason — so they get promoted and the matching store-WAL entries
/// are replayed.
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
            return Ok(SegmentRecovery {
                is_complete: true,
                rollback_from: None,
            });
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
    let recovered_max_lsn = prefix
        .records
        .iter()
        .filter(|record| {
            record
                .record_ref
                .offset
                .checked_add(record.record_len)
                .is_some_and(|end| end <= recovered_write_offset)
        })
        .map(|record| record.header.generation)
        .max();
    let rollback_from = if is_complete {
        None
    } else {
        Some(
            recovered_max_lsn
                .and_then(|lsn| lsn.checked_add(1))
                .or_else(|| existing_state.as_ref().and_then(|state| state.min_lsn))
                .unwrap_or(index.get_next_lsn()?),
        )
    };

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
    Ok(SegmentRecovery {
        is_complete,
        rollback_from,
    })
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
    let rollback_from = index
        .get_segment_state(segment_id)?
        .and_then(|state| state.min_lsn);
    discard_unsealed_segment(config, index, segment_id, metrics)?;
    Ok(SegmentRecovery {
        is_complete: false,
        rollback_from,
    })
}

struct RecoveredSegmentPrefix<'a> {
    existing_state: Option<SegmentState>,
    durable_offset: u64,
    recovered_write_offset: u64,
    records: &'a [strata_segment::ScannedRecord],
}

/// Publishes the post scan segment state and rebuilds its LSN bounds from scratch.
///
/// min/max LSN can't be trusted from the old state because the tail they described may be gone, so
/// they are recomputed from the checksummed prefix bounded by the committed write offset.
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
    let previous_baseline_bytes = index
        .get_segment_gc_summary(segment_id)?
        .map_or(0, |summary| summary.total_bytes);
    let reset_baseline = previous_baseline_bytes > prefix.recovered_write_offset;
    let baseline_bytes = if reset_baseline {
        0
    } else {
        previous_baseline_bytes
    };
    let mut allocation_records = 0_u64;

    for record in prefix.records {
        let record_end = record
            .record_ref
            .offset
            .checked_add(record.record_len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        if record_end > prefix.recovered_write_offset {
            continue;
        }
        recovered_record_count = recovered_record_count.saturating_add(1);
        if record_end > baseline_bytes {
            if record.record_ref.offset < baseline_bytes {
                return Err(Error::InvariantViolation {
                    reason: format!(
                        "segment {segment_id} GC baseline {baseline_bytes} splits record at {}",
                        record.record_ref.offset
                    ),
                });
            }
            allocation_records = allocation_records.saturating_add(1);
        }
        let lsn = record.header.generation;

        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }

    index.put_segment_state_batch(&mut batch, &state)?;
    if reset_baseline {
        // Point-in-time recovery may deliberately truncate a prefix previously counted by GC.
        // Rebuild conservatively: every surviving record starts live and unknown.
        index.put_segment_gc_summary_batch(
            &mut batch,
            segment_id,
            &SegmentGcSummary {
                total_bytes: prefix.recovered_write_offset,
                live_bytes: prefix.recovered_write_offset,
                live_ref_count: recovered_record_count,
                unknown_lifetime_bytes: prefix.recovered_write_offset,
                unknown_lifetime_ref_count: recovered_record_count,
                ..Default::default()
            },
        )?;
    } else {
        publish_segment_allocation_baseline(
            index,
            &mut batch,
            segment_id,
            prefix.recovered_write_offset,
            allocation_records,
        )?;
    }

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
/// real LSN — so `latest_epoch_at_lsn(any)` always has an answer during compaction and rollback.
/// `config.starting_epoch` only matters on first
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

/// Erases epoch operations from `rollback_from` onward and rewinds the store frontiers to that LSN.
///
/// Blob patches live in the store WAL, which is reopened through the rewound `next_lsn`; RocksDB only
/// needs to remove its auxiliary LSN and epoch rows.
fn rollback_operations_from(
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    rollback_from: StrataLsn,
) -> Result<()> {
    let mut batch = index.batch();
    let hidden_epoch_changes = index
        .iter_epoch_changes_from(rollback_from)?
        .into_iter()
        .map(|(lsn, _)| lsn)
        .collect::<Vec<_>>();
    index.remove_epoch_changes_batch(&mut batch, &hidden_epoch_changes)?;
    let rolled_back_drops = index
        .iter_shard_cleanup_jobs()?
        .into_iter()
        .filter(|job| job.drop_lsn >= rollback_from)
        .collect::<Vec<_>>();
    for job in &rolled_back_drops {
        // Cleanup cannot have crossed an unpublished drop because the GC worker gates it on
        // `published_lsn`. Restoring the same generation therefore makes the pre-drop shard
        // visible again without resurrecting physically reclaimed files.
        if index.get_shard_info(job.shard.id)?.is_some_and(|info| {
            info.current_generation == job.shard.generation && info.state == ShardState::Dropped
        }) {
            index.put_shard_info_batch(
                &mut batch,
                job.shard.id,
                ShardInfo {
                    current_generation: job.shard.generation,
                    state: ShardState::Active,
                },
            )?;
        }
        index.delete_shard_cleanup_job_batch(&mut batch, job.shard)?;
    }
    let rollback_ops = hidden_epoch_changes
        .len()
        .saturating_add(rolled_back_drops.len()) as u64;

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
    metrics.record_rollback(rollback_from, rollback_ops);
    Ok(())
}

/// Publishes the exact store-WAL prefix selected by recovery.
///
/// `Wal::recover` has already fsynced a promoted complete tail, and segment recovery has fsynced
/// the referenced active data prefix. The LSM is only the in-memory projection of that log.
fn publish_recovered_store_checkpoint(
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    wal: &Wal,
    active_segment_state: &SegmentState,
) -> Result<()> {
    let recovered_lsn = index.get_next_lsn()?.saturating_sub(1);
    let checkpoint = StoreCheckpoint {
        wal_position: wal.position(),
        active_segment_id: active_segment_state.segment_id,
        active_segment_offset: active_segment_state.write_offset,
    };
    if checkpoint.active_segment_id != active_segment_state.segment_id
        || checkpoint.active_segment_offset != active_segment_state.write_offset
    {
        return Err(Error::InvariantViolation {
            reason: format!(
                "recovered store checkpoint {checkpoint:?} does not match store LSN {recovered_lsn} and active segment {} at {}",
                active_segment_state.segment_id, active_segment_state.write_offset
            ),
        });
    }
    let current_published_lsn = index.get_published_lsn()?;
    if current_published_lsn > recovered_lsn {
        return Err(Error::InvariantViolation {
            reason: format!(
                "published LSN {current_published_lsn} follows recovered LSN {recovered_lsn}"
            ),
        });
    }
    let mut batch = index.batch();
    let published_lsn = recovered_lsn;
    index.put_published_lsn_batch(&mut batch, published_lsn)?;
    index.put_store_checkpoint_batch(&mut batch, checkpoint)?;
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    metrics.set_published_lsn(published_lsn);
    Ok(())
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
    next_lsn: StrataLsn,
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
    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &state)?;
    if existing.is_none() {
        index.put_segment_published_at_lsn_batch(
            &mut batch,
            active_writer.segment_id(),
            next_lsn,
        )?;
    }
    batch.write().map_err(strata_index::Error::from)?;
    Ok(state)
}

/// Builds the normal open segment state row for the current writer.
///
/// All open ingest rows should use the same relative path and explicit store owner.
/// Hand building this in multiple places risks one path being absolute, so a later move of the
/// store root would make that segment unreadable while others still resolve correctly.
#[cfg(test)]
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

pub(crate) fn publish_segment_allocation_baseline(
    index: &StrataIndex,
    batch: &mut typed_store::rocks::DBBatch,
    segment_id: SegmentId,
    durable_bytes: u64,
    allocation_records: u64,
) -> Result<()> {
    let mut summary = index
        .get_segment_gc_summary(segment_id)?
        .unwrap_or_default();
    if summary.total_bytes > durable_bytes {
        return Err(Error::InvariantViolation {
            reason: format!(
                "segment {segment_id} GC baseline {} exceeds durable bytes {durable_bytes}",
                summary.total_bytes
            ),
        });
    }
    let allocation_bytes = durable_bytes - summary.total_bytes;
    if (allocation_bytes == 0) != (allocation_records == 0) {
        return Err(Error::InvariantViolation {
            reason: format!(
                "segment {segment_id} GC baseline advances by {allocation_bytes} bytes and {allocation_records} records"
            ),
        });
    }

    summary.total_bytes = durable_bytes;
    summary.live_bytes = summary
        .live_bytes
        .checked_add(allocation_bytes)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("segment {segment_id} live-byte baseline overflow"),
        })?;
    summary.live_ref_count = summary
        .live_ref_count
        .checked_add(allocation_records)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("segment {segment_id} live-ref baseline overflow"),
        })?;
    summary.unknown_lifetime_bytes = summary
        .unknown_lifetime_bytes
        .checked_add(allocation_bytes)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("segment {segment_id} unknown-lifetime byte baseline overflow"),
        })?;
    summary.unknown_lifetime_ref_count = summary
        .unknown_lifetime_ref_count
        .checked_add(allocation_records)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("segment {segment_id} unknown-lifetime ref baseline overflow"),
        })?;
    index.put_segment_gc_summary_batch(batch, segment_id, &summary)?;
    Ok(())
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
