use std::{collections::BTreeMap, fs, num::NonZeroU32, path::PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use strata_core::BlobKey;
use xxhash_rust::xxh3::xxh3_64;

use crate::active_log::AccountingDelta;
use crate::events::CompactionEventBatch;
use crate::manifest::{
    EpochChange, Manifest, PartitionManifest, RunKind, RunMeta, advance_manifest_generation,
    allocate_run_id, manifest_from_bytes, manifest_to_bytes, partition_mut, push_epoch_change,
    validate_manifest,
};
use crate::run_io::{
    RunFile, RunRecordReader, RunRecords, open_run_record_reader, partition_dir, run_file_name,
    write_run_file_atomic,
};
use crate::state::{
    BlobUpdate, MaterializedBlobState, PatchRecord, RecordLsn, StateRecord, compact_delta_updates,
    fold_patch_update, fold_update, sort_updates,
};
use crate::{Error, FORMAT_VERSION, PartitionId, Result, RunId, merge};

/// Runtime configuration for opening the sidecar accounting index.
#[derive(Debug, Clone)]
pub struct AccountingIndexConfig {
    pub root_dir: PathBuf,
    pub partition_count: NonZeroU32,
}

impl AccountingIndexConfig {
    pub fn new(root_dir: impl Into<PathBuf>, partition_count: NonZeroU32) -> Self {
        Self {
            root_dir: root_dir.into(),
            partition_count,
        }
    }
}

/// Open accounting index handle with the current durable manifest in memory.
#[derive(Debug)]
pub struct AccountingIndex {
    config: AccountingIndexConfig,
    manifest: Manifest,
}

/// Transient publication plan for adding freshly written delta run files.
///
/// Prepared values are not durable database rows and are not physical run files. They are in-memory
/// guards created after any needed files have been synced but before the owner has accepted the new
/// manifest. Dropping one abandons its candidate manifest and may leave unreferenced files for later
/// cleanup; applying one checks `base_generation` before replacing this handle's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDeltaRuns {
    /// These metas identify new physical delta files that already exist on disk. They become live
    /// only if the candidate manifest is published; until then they are just prepared artifacts.
    pub metas: Vec<RunMeta>,
    /// Captures the manifest generation observed before file creation. It protects the in-memory
    /// root from being rewound by a stale prepared value and prevents stale obsolete-file cleanup.
    base_generation: u64,
    /// Candidate durable root containing the new delta entries and the next generation. The owner
    /// publishes these bytes, not the mutable `AccountingIndex` handle itself.
    manifest: Manifest,
}

/// Transient publication plan for ingesting durable active-log accounting deltas.
///
/// This prepared value can contain both partitioned blob update runs and global epoch changes. It is
/// born after active-log records have been translated into sidecar metadata, and it is retired when
/// the owner commits the candidate manifest and advances the active-log ingestion cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAccountingDeltas {
    /// Blob updates become physical delta run files so later delta compaction can group them by key.
    /// The list is empty when the active-log batch contained only epoch changes.
    pub delta_metas: Vec<RunMeta>,
    /// Epoch changes stay in the manifest because they are global timeline facts rather than
    /// partition-local run records.
    pub epoch_changes: Vec<EpochChange>,
    /// Same local generation guard used by every prepared manifest: it prevents an older active-log
    /// ingest plan from replacing a newer accepted root in this handle.
    base_generation: u64,
    /// Candidate durable root that ties blob delta files and epoch changes into one publication
    /// order.
    manifest: Manifest,
}

/// Transient publication plan for a manifest-only epoch transition.
///
/// No physical run file is created for this path. The value exists so epoch-only updates follow the
/// same prepare/publish/apply protocol as blob updates and cannot bypass the manifest generation
/// guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEpochChange {
    /// The timeline entry to be added to the durable root. It becomes authoritative only when the
    /// candidate manifest is accepted by the owner.
    pub change: EpochChange,
    /// Local stale-work guard captured before cloning the root.
    base_generation: u64,
    /// Candidate durable root with the epoch timeline updated and deduplicated.
    manifest: Manifest,
}

/// Transient publication plan for delta-to-patch compaction.
///
/// This value is born after a replacement patch file has been written and after any closed local ref
/// events have been derived from the input delta runs. It is retired after the owner durably publishes
/// the event rows plus candidate manifest; only then can the input delta files be removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCompaction {
    /// The derived durable rows that must be committed beside the candidate manifest. They describe
    /// physical GC/accounting effects for refs whose full lifetime was visible inside the delta set.
    pub event_batch: CompactionEventBatch,
    /// Local generation guard captured before the output patch file was allocated.
    base_generation: u64,
    /// Candidate root that removes the consumed delta runs and appends the new patch run without
    /// touching older base/patch layers.
    manifest: Manifest,
    /// Physical input files that become unreachable if and only if the candidate manifest is accepted.
    /// Cleanup is delayed until after apply so a failed publish does not delete the current root set.
    obsolete_runs: Vec<RunMeta>,
}

/// Transient publication plan for major compaction into a replacement base run.
///
/// This value is born after the base/patch stack has been folded into a new base file and all
/// resulting ref events have been translated into accounting and GC overlay mutations. It is retired
/// after the owner publishes those rows with the candidate manifest; after that, the old base and
/// patch files can be removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMajorCompaction {
    /// The replacement base file, absent when there was no base or patch input to compact. A present
    /// value is already on disk but is not live until the candidate manifest is accepted.
    pub output: Option<RunMeta>,
    /// The durable side effects produced while materializing residual patch history against the base
    /// image. This is where old base refs are actually retired by tombstones or MapRef rewrites.
    pub event_batch: CompactionEventBatch,
    /// Local generation guard captured before allocating the replacement base run.
    base_generation: u64,
    /// Candidate root that replaces the old base/patch stack with the new base image and advances the
    /// partition materialization watermark.
    manifest: Manifest,
    /// Old base and patch files that are no longer reachable after successful publish. They remain on
    /// disk until apply confirms this handle has accepted the candidate manifest.
    obsolete_runs: Vec<RunMeta>,
}

impl PreparedDeltaRuns {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl PreparedAccountingDeltas {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl PreparedEpochChange {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl PreparedCompaction {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl PreparedMajorCompaction {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl AccountingIndex {
    pub fn open(config: AccountingIndexConfig) -> Result<Self> {
        Self::open_with_manifest(config, None)
    }

    pub fn open_from_manifest_bytes(
        config: AccountingIndexConfig,
        manifest: Option<&[u8]>,
    ) -> Result<Self> {
        let manifest = manifest.map(manifest_from_bytes).transpose()?;
        Self::open_with_manifest(config, manifest)
    }

    pub fn open_with_manifest(
        config: AccountingIndexConfig,
        manifest: Option<Manifest>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.root_dir).map_err(|source| Error::Io {
            path: config.root_dir.clone(),
            source,
        })?;

        let manifest = manifest.unwrap_or_else(|| Manifest::new(config.partition_count));
        validate_manifest(&manifest, config.partition_count)?;

        Ok(Self { config, manifest })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }

    /// Replaces the in-memory manifest with a manifest already accepted by the owner.
    ///
    /// This is for opening from, or catching up to, the durable manifest stored by the owner
    /// (normally RocksDB). It intentionally does not perform the prepared-manifest generation check
    /// because externally loaded durable state is authoritative. Prepared operations must use
    /// `apply_prepared_manifest` through their typed `apply_prepared_*` wrappers so stale local work
    /// cannot roll back this handle's in-memory manifest or trigger obsolete-file cleanup from an old
    /// view.
    pub fn apply_manifest(&mut self, manifest: Manifest) -> Result<()> {
        validate_manifest(&manifest, self.config.partition_count)?;
        self.manifest = manifest;
        Ok(())
    }

    fn apply_prepared_manifest(&mut self, base_generation: u64, manifest: Manifest) -> Result<()> {
        // Preparation is allowed to do real I/O: it may create new run files and build a manifest
        // that references them before the owner publishes the manifest plus derived RocksDB rows.
        // The captured generation is the guard that says "this prepared manifest was derived from
        // exactly the state this handle still holds." If another prepared value has already advanced
        // the handle, accepting this one would resurrect stale run lists and could delete files that
        // the current manifest still needs.
        //
        // This check is intentionally local. It runs when the caller applies the prepared value to
        // this handle, often after an external durable batch has been written. It prevents stale
        // in-memory replacement and stale obsolete-file cleanup, but the owner still owns durable
        // stale-write prevention if multiple publishers can write the stored manifest.
        if self.manifest.generation != base_generation {
            return Err(Error::StalePreparedManifest {
                current: self.manifest.generation,
                prepared_from: base_generation,
            });
        }
        self.apply_manifest(manifest)
    }

    pub fn partition_for_key(&self, key: &BlobKey) -> PartitionId {
        (xxh3_64(key.as_bytes()) % u64::from(self.config.partition_count.get())) as PartitionId
    }

    pub fn append_delta_run(&mut self, updates: Vec<BlobUpdate>) -> Result<Vec<RunMeta>> {
        let prepared = self.prepare_delta_runs(updates)?;
        let metas = prepared.metas.clone();
        self.apply_prepared_delta_runs(prepared)?;
        Ok(metas)
    }

    pub fn prepare_delta_runs(&self, updates: Vec<BlobUpdate>) -> Result<PreparedDeltaRuns> {
        if updates.is_empty() {
            return Err(Error::EmptyDeltaRun);
        }

        // A delta run is made visible by the manifest, not by the file write. The file is written
        // first so the candidate manifest never points at missing bytes, but readers must ignore it
        // until the owner publishes the manifest that names the new `RunMeta`.
        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let metas = self.write_delta_runs(&mut manifest, updates)?;
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedDeltaRuns {
            metas,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_delta_runs(&mut self, prepared: PreparedDeltaRuns) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }

    pub fn prepare_accounting_deltas(
        &self,
        deltas: Vec<AccountingDelta>,
    ) -> Result<PreparedAccountingDeltas> {
        let mut blob_updates = Vec::new();
        let mut epoch_changes = Vec::new();
        for delta in deltas {
            match delta {
                AccountingDelta::Blob(update) => blob_updates.push(update),
                AccountingDelta::Epoch(change) => epoch_changes.push(change),
            }
        }
        if blob_updates.is_empty() && epoch_changes.is_empty() {
            return Err(Error::EmptyDeltaRun);
        }

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let delta_metas = if blob_updates.is_empty() {
            Vec::new()
        } else {
            self.write_delta_runs(&mut manifest, blob_updates)?
        };
        for change in &epoch_changes {
            push_epoch_change(&mut manifest, *change);
        }
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedAccountingDeltas {
            delta_metas,
            epoch_changes,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_accounting_deltas(
        &mut self,
        prepared: PreparedAccountingDeltas,
    ) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }

    pub fn record_epoch_change(&mut self, change: EpochChange) -> Result<()> {
        let prepared = self.prepare_epoch_change(change)?;
        self.apply_prepared_epoch_change(prepared)
    }

    pub fn prepare_epoch_change(&self, change: EpochChange) -> Result<PreparedEpochChange> {
        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        push_epoch_change(&mut manifest, change);
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedEpochChange {
            change,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_epoch_change(&mut self, prepared: PreparedEpochChange) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }

    pub fn materialized_state(&self, key: &BlobKey) -> Result<Option<MaterializedBlobState>> {
        let partition = self.partition_for_key(key);
        self.materialized_state_in_partition(partition, key)
    }

    pub fn current_state(&self, key: &BlobKey) -> Result<Option<MaterializedBlobState>> {
        let partition = self.partition_for_key(key);
        let mut state = self.materialized_state_in_partition(partition, key)?;
        // Current reads add the newest delta runs on top of the materialized base+patch image, but
        // they deliberately discard emitted RefEvents. Reads answer "what is live now"; only
        // compaction publishes the physical accounting transitions that make old bytes collectable.
        for run in &self.partition(partition)?.deltas {
            for update in self.read_delta_run(run)? {
                if update.key() == key {
                    fold_update(&mut state, &update, &mut |_| {});
                }
            }
        }
        Ok(state)
    }

    pub fn compact_partition(&mut self, partition: PartitionId) -> Result<CompactionEventBatch> {
        let prepared = self.prepare_compact_partition(partition)?;
        let event_batch = prepared.event_batch.clone();
        self.apply_prepared_compaction(prepared)?;
        Ok(event_batch)
    }

    pub fn prepare_compact_partition(&self, partition: PartitionId) -> Result<PreparedCompaction> {
        self.validate_partition(partition)?;
        let input_deltas = self.partition(partition)?.deltas.clone();
        if input_deltas.is_empty() {
            return Ok(PreparedCompaction {
                event_batch: CompactionEventBatch {
                    partition,
                    ..CompactionEventBatch::default()
                },
                base_generation: self.manifest.generation,
                manifest: self.manifest.clone(),
                obsolete_runs: Vec::new(),
            });
        }

        let mut batch = CompactionEventBatch {
            partition,
            input_run_ids: input_deltas.iter().map(|meta| meta.id).collect(),
            ..CompactionEventBatch::default()
        };
        let mut delta_inputs = Vec::new();
        for run in &input_deltas {
            batch.max_lsn = batch.max_lsn.max(run.max_lsn.unwrap_or(0));
            delta_inputs.push(self.delta_run_records(run)?);
        }

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let output_run_id = allocate_run_id(&mut manifest);
        let mut merged = merge::DeltaRunMerger::new(delta_inputs)?;
        let output_records = std::iter::from_fn(|| {
            let group = match merged.next()? {
                Ok(group) => group,
                Err(error) => return Some(Err(error)),
            };
            // Delta compaction is intentionally shallow. It can reorder sorted delta runs into one
            // key-grouped patch and close lifetimes that are fully visible inside those deltas, but
            // it does not read the older base/patch layers. Any residual operation that could affect
            // older materialized state must remain in the patch run for major compaction.
            let updates = compact_delta_updates(&group.key, &group.updates, &mut batch);
            Some(Ok(PatchRecord {
                key: group.key,
                updates,
            }))
        });

        let output_meta =
            self.write_framed_run(output_run_id, RunKind::Patch, partition, output_records)?;
        batch.output_run_id = Some(output_meta.id);

        {
            let partition_manifest = partition_mut(&mut manifest, partition)?;
            // The new patch run is a physical replacement for all input delta runs in this
            // partition. Existing base and patch runs stay in place because their state was not read
            // and therefore cannot be rewritten by this compaction level.
            partition_manifest.deltas.clear();
            partition_manifest.patches.push(output_meta);
        }
        advance_manifest_generation(&mut manifest)?;

        Ok(PreparedCompaction {
            event_batch: batch,
            base_generation,
            manifest,
            obsolete_runs: input_deltas,
        })
    }

    pub fn apply_prepared_compaction(&mut self, prepared: PreparedCompaction) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)?;
        // File cleanup is sequenced after the generation guard. If this prepared compaction was
        // derived from an old manifest, its input files may still be reachable from the current root.
        self.remove_runs(&prepared.obsolete_runs);
        Ok(())
    }

    pub fn major_compact_partition(&mut self, partition: PartitionId) -> Result<Option<RunMeta>> {
        let prepared = self.prepare_major_compact_partition(partition)?;
        let output = prepared.output.clone();
        self.apply_prepared_major_compaction(prepared)?;
        Ok(output)
    }

    pub fn prepare_major_compact_partition(
        &self,
        partition: PartitionId,
    ) -> Result<PreparedMajorCompaction> {
        self.validate_partition(partition)?;
        let old_base = self.partition(partition)?.base.clone();
        let old_patches = self.partition(partition)?.patches.clone();
        if old_base.is_none() && old_patches.is_empty() {
            return Ok(PreparedMajorCompaction {
                output: None,
                event_batch: CompactionEventBatch {
                    partition,
                    ..CompactionEventBatch::default()
                },
                base_generation: self.manifest.generation,
                manifest: self.manifest.clone(),
                obsolete_runs: Vec::new(),
            });
        }

        let mut batch = CompactionEventBatch {
            partition,
            input_run_ids: old_base
                .iter()
                .chain(old_patches.iter())
                .map(|meta| meta.id)
                .collect(),
            max_lsn: old_base
                .iter()
                .chain(old_patches.iter())
                .filter_map(|meta| meta.max_lsn)
                .max()
                .unwrap_or_default(),
            ..CompactionEventBatch::default()
        };
        let base_records = old_base
            .as_ref()
            .map(|base| self.state_run_records(base))
            .transpose()?;
        let mut patch_inputs = Vec::new();
        for (index, patch) in old_patches.iter().enumerate() {
            patch_inputs.push(merge::PatchRunInput {
                precedence: index + 1,
                records: self.patch_run_records(patch)?,
            });
        }
        let mut merged = merge::BasePatchMerger::new(base_records, patch_inputs)?;

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let output_run_id = allocate_run_id(&mut manifest);
        let output_records = std::iter::from_fn(|| {
            let merged_key = match merged.next()? {
                Ok(merged_key) => merged_key,
                Err(error) => return Some(Err(error)),
            };
            // Major compaction is the first point where residual patch history can be interpreted
            // against the older materialized row. Folding here is what turns "there was a tombstone
            // in a patch" into "the base ref is now retired" and what resolves pending MapRef
            // rewrites whose source ref lived in the base.
            let mut state = merged_key.base_state;
            for update in &merged_key.patch_updates {
                fold_patch_update(&mut state, update, &mut |event| {
                    batch.record_event(event);
                });
            }
            Some(Ok(StateRecord {
                key: merged_key.key,
                state: state.unwrap_or_default(),
            }))
        });
        let output_meta =
            self.write_framed_run(output_run_id, RunKind::Base, partition, output_records)?;
        batch.output_run_id = Some(output_meta.id);
        batch.max_lsn = batch.max_lsn.max(output_meta.max_lsn.unwrap_or_default());

        {
            let partition_manifest = partition_mut(&mut manifest, partition)?;
            // The output base run is the new materialized image of base plus every patch. Once the
            // manifest points at it, old base/patch files are obsolete, and future materialized reads
            // no longer need to replay those patches.
            partition_manifest.base = Some(output_meta.clone());
            partition_manifest.patches.clear();
            partition_manifest.materialized_through_lsn = partition_manifest
                .materialized_through_lsn
                .max(batch.max_lsn);
        }
        advance_manifest_generation(&mut manifest)?;

        let mut obsolete_runs = old_patches;
        if let Some(base) = old_base.as_ref() {
            obsolete_runs.push(base.clone());
        }
        Ok(PreparedMajorCompaction {
            output: Some(output_meta),
            event_batch: batch,
            base_generation,
            manifest,
            obsolete_runs,
        })
    }

    pub fn apply_prepared_major_compaction(
        &mut self,
        prepared: PreparedMajorCompaction,
    ) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)?;
        // The replacement base is live only after the manifest swap. Until then, old base/patch
        // files remain the readable state of the partition and must not be deleted.
        self.remove_runs(&prepared.obsolete_runs);
        Ok(())
    }

    fn materialized_state_in_partition(
        &self,
        partition: PartitionId,
        key: &BlobKey,
    ) -> Result<Option<MaterializedBlobState>> {
        let partition_manifest = self.partition(partition)?;
        let mut state = if let Some(base) = &partition_manifest.base {
            self.find_state_in_run(base, key)?
        } else {
            None
        };
        // Patch runs are residual history above the base. Folding them here must use manifest order
        // because each patch may contain tombstones or pending maps whose meaning depends on all
        // older materialized state having already been applied.
        for patch in &partition_manifest.patches {
            if let Some(record) = self.find_patch_in_run(patch, key)? {
                for update in record.updates {
                    fold_patch_update(&mut state, &update, &mut |_| {});
                }
            }
        }
        Ok(state)
    }

    fn find_state_in_run(
        &self,
        meta: &RunMeta,
        key: &BlobKey,
    ) -> Result<Option<MaterializedBlobState>> {
        for record in self.state_run_records(meta)? {
            let record = record?;
            match record.key.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(record.state)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    fn find_patch_in_run(&self, meta: &RunMeta, key: &BlobKey) -> Result<Option<PatchRecord>> {
        for record in self.patch_run_records(meta)? {
            let record = record?;
            match record.key.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(record)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    fn state_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<StateRecord>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Base {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Base,
            });
        }
        Ok(reader)
    }

    fn patch_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<PatchRecord>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Patch {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Patch,
            });
        }
        Ok(reader)
    }

    fn read_delta_run(&self, meta: &RunMeta) -> Result<Vec<BlobUpdate>> {
        self.delta_run_records(meta)?.collect()
    }

    fn delta_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<BlobUpdate>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Delta {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Delta,
            });
        }
        Ok(reader)
    }

    fn open_run_record_reader<T>(&self, meta: &RunMeta) -> Result<RunRecordReader<T>>
    where
        T: DeserializeOwned,
    {
        let path = self.config.root_dir.join(&meta.path);
        let reader = open_run_record_reader(&path)?;
        if reader.header.kind != meta.kind {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: meta.kind,
            });
        }
        if reader.header.partition != meta.partition {
            return Err(Error::UnexpectedRunPartition {
                run_id: meta.id,
                actual: reader.header.partition,
                expected: meta.partition,
            });
        }
        Ok(reader)
    }

    fn write_run(&self, run_id: RunId, run: &RunFile) -> Result<RunMeta> {
        match &run.records {
            RunRecords::State(records) => self.write_framed_run(
                run_id,
                run.kind,
                run.partition,
                records.iter().map(|record| Ok(record)),
            ),
            RunRecords::Patch(records) => self.write_framed_run(
                run_id,
                run.kind,
                run.partition,
                records.iter().map(|record| Ok(record)),
            ),
            RunRecords::Delta(records) => self.write_framed_run(
                run_id,
                run.kind,
                run.partition,
                records.iter().map(|record| Ok(record)),
            ),
        }
    }

    fn write_delta_runs(
        &self,
        manifest: &mut Manifest,
        updates: Vec<BlobUpdate>,
    ) -> Result<Vec<RunMeta>> {
        let mut by_partition = BTreeMap::<PartitionId, Vec<BlobUpdate>>::new();
        for update in updates {
            let partition = self.partition_for_key(update.key());
            by_partition.entry(partition).or_default().push(update);
        }

        let mut metas = Vec::new();
        for (partition, mut updates) in by_partition {
            sort_updates(&mut updates);
            let run_id = allocate_run_id(manifest);
            let run = RunFile {
                format_version: FORMAT_VERSION,
                kind: RunKind::Delta,
                partition,
                records: RunRecords::Delta(updates),
            };
            let meta = self.write_run(run_id, &run)?;
            partition_mut(manifest, partition)?
                .deltas
                .push(meta.clone());
            metas.push(meta);
        }
        Ok(metas)
    }

    fn write_framed_run<I, T>(
        &self,
        run_id: RunId,
        kind: RunKind,
        partition: PartitionId,
        records: I,
    ) -> Result<RunMeta>
    where
        I: IntoIterator<Item = Result<T>>,
        T: Serialize + RecordLsn,
    {
        let partition_dir = partition_dir(&self.config.root_dir, partition);
        fs::create_dir_all(&partition_dir).map_err(|source| Error::Io {
            path: partition_dir.clone(),
            source,
        })?;

        let file_name = run_file_name(kind, run_id);
        let relative_path = format!("partition-{partition:05}/{file_name}");
        let path = self.config.root_dir.join(&relative_path);
        let (max_lsn, file_len) = write_run_file_atomic(&path, kind, partition, records)?;

        Ok(RunMeta {
            id: run_id,
            kind,
            partition,
            path: relative_path,
            max_lsn,
            file_len,
        })
    }

    fn partition(&self, partition: PartitionId) -> Result<&PartitionManifest> {
        self.validate_partition(partition)?;
        Ok(self
            .manifest
            .partitions
            .get(&partition)
            .expect("validated partition must exist"))
    }

    fn validate_partition(&self, partition: PartitionId) -> Result<()> {
        if partition >= self.config.partition_count.get() {
            return Err(Error::InvalidPartition(partition));
        }
        Ok(())
    }

    fn remove_runs(&self, runs: &[RunMeta]) {
        for run in runs {
            self.remove_run(run);
        }
    }

    fn remove_run(&self, run: &RunMeta) {
        let _ = fs::remove_file(self.config.root_dir.join(&run.path));
    }
}
