//! Background maintenance workers owned by the store: the garbage-log sweeper,
//! the blob-LSM memtable flusher, the blob-LSM compactor, and the relocation-LSM
//! flush/compaction helpers they share with the write path.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, Weak, mpsc},
    time::Instant,
};

use strata_core::SegmentFileState;
use strata_index::StrataIndex;
use strata_lsm::{
    GarbageLog, Lsm, Manifest as LsmManifest, ManifestEdit, TableMeta, select_compaction_inputs,
    select_patch_compaction_inputs, write_compaction, write_patch_compaction,
};
use strata_relocation::{RelocationMerge, RelocationStore};

use crate::{
    BLOB_LSM_MANIFEST, Error, GARBAGE_LOG_HEAD, GARBAGE_LOG_SWEEP_CURSOR, GARBAGE_SWEEP_INTERVAL,
    LSM_COMPACTION_PATCH_BYTES, LSM_COMPACTION_PATCH_COUNT, LSM_COMPACTION_TARGET_BYTES,
    LSM_GARBAGE_LOG_MAX_BYTES, LSM_MEMTABLE_MAX_AGE, LSM_OBSOLETE_CLEANUP_INTERVAL,
    RELOCATION_LSM_MANIFEST, Result, StoreHalt, StrataStoreConfig, StrataStoreMetrics,
    blob_lsm::{BlobCompactionSnapshot, BlobMergeWithRelocations},
    gc::GcCommand,
    relocation_cache::RelocationCache,
};

pub(crate) struct GarbageLogSweeper {
    pub(crate) index: StrataIndex,
    pub(crate) global_log_dir: PathBuf,
    pub(crate) namespace_dir: PathBuf,
    pub(crate) durability_publish_lock: Arc<Mutex<()>>,
    pub(crate) gc_txs: Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
    pub(crate) shutdown_rx: mpsc::Receiver<()>,
}

impl GarbageLogSweeper {
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

pub(crate) fn garbage_log_dir(config: &StrataStoreConfig) -> PathBuf {
    config.namespace_dir().join("garbage-log")
}

pub(crate) struct LsmFlusher {
    pub(crate) index: StrataIndex,
    pub(crate) lsm: Weak<Lsm>,
    pub(crate) wake_rx: mpsc::Receiver<()>,
    pub(crate) compact_tx: mpsc::Sender<()>,
    pub(crate) store_halt: StoreHalt,
}

impl LsmFlusher {
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

pub(crate) struct LsmCompactor {
    pub(crate) index: StrataIndex,
    pub(crate) lsm: Weak<Lsm>,
    pub(crate) relocations: Weak<RelocationStore>,
    pub(crate) garbage_log_dir: PathBuf,
    pub(crate) garbage_publish_lock: Arc<Mutex<()>>,
    pub(crate) wake_rx: mpsc::Receiver<()>,
    pub(crate) store_halt: StoreHalt,
    pub(crate) metrics: StrataStoreMetrics,
    pub(crate) obsolete: Vec<TableMeta>,
}

impl LsmCompactor {
    pub(crate) fn run(mut self) {
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

pub(crate) fn publish_blob_lsm_edit(
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

pub(crate) fn publish_relocation_lsm_edit(
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

pub(crate) fn flush_relocation_lsm(
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

pub(crate) fn compact_relocation_lsm(
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
