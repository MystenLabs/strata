use super::*;

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
    pub(super) base_generation: u64,
    /// Candidate durable root containing the new delta entries and the next generation. The owner
    /// publishes these bytes, not the mutable `AccountingIndex` handle itself.
    pub(super) manifest: Manifest,
}

/// Transient publication plan for ingesting durable active-log accounting deltas.
///
/// This prepared value can contain both partitioned blob update runs and global epoch changes. It is
/// born after active-log records have been translated into accounting-index metadata, and it is retired when
/// the owner commits the candidate manifest and advances the active-log ingestion cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAccountingDeltas {
    /// Blob updates become physical delta run files so later delta compaction can group them by key.
    /// The list is empty when the active-log batch contained only epoch changes.
    pub delta_metas: Vec<RunMeta>,
    /// Epoch changes stay in the manifest because they are global timeline facts rather than
    /// partition-local run records.
    pub epoch_changes: Vec<EpochChange>,
    /// Shard drops remain pending until the materialized payload state is swept.
    pub shard_drops: Vec<crate::ShardDrop>,
    /// Same local generation guard used by every prepared manifest: it prevents an older active-log
    /// ingest plan from replacing a newer accepted root in this handle.
    pub(super) base_generation: u64,
    /// Candidate durable root that ties blob delta files and epoch changes into one publication
    /// order.
    pub(super) manifest: Manifest,
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
    pub(super) base_generation: u64,
    /// Candidate durable root with the epoch timeline updated and deduplicated.
    pub(super) manifest: Manifest,
}

/// Transient publication plan for delta-to-patch compaction.
///
/// This value is born after a replacement patch file has been written and after any closed local ref
/// events have been derived from the input delta runs. It is retired after the owner durably publishes
/// the event rows plus candidate manifest; only then can the input delta files be removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDeltaCompaction {
    /// The derived durable rows that must be committed beside the candidate manifest. They describe
    /// physical GC/accounting effects for refs whose full lifetime was visible inside the delta set.
    pub event_batch: CompactionEventBatch,
    /// Local generation guard captured before the output patch file was allocated.
    pub(super) base_generation: u64,
    /// Candidate root that removes the consumed delta runs and appends the new patch run without
    /// touching older base/patch layers.
    pub(super) manifest: Manifest,
    /// Physical input files that become unreachable if and only if the candidate manifest is accepted.
    /// Cleanup is delayed until after apply so a failed publish does not delete the current root set.
    pub(super) obsolete_runs: Vec<RunMeta>,
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
    pub(super) base_generation: u64,
    /// Candidate root that replaces the old base/patch stack with the new base image and advances the
    /// partition materialization watermark.
    pub(super) manifest: Manifest,
    /// Old base and patch files that are no longer reachable after successful publish. They remain on
    /// disk until apply confirms this handle has accepted the candidate manifest.
    pub(super) obsolete_runs: Vec<RunMeta>,
}

/// Replacement accounting-index root after removing one dropped generation from every partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedShardDrop {
    pub drop: crate::ShardDrop,
    pub event_batch: CompactionEventBatch,
    pub(super) base_generation: u64,
    pub(super) manifest: Manifest,
    pub(super) obsolete_runs: Vec<RunMeta>,
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

impl PreparedDeltaCompaction {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        manifest_to_bytes(&self.manifest)
    }
}

impl PreparedShardDrop {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
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
