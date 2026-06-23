use std::{collections::BTreeMap, num::NonZeroU32};

use serde::{Deserialize, Serialize};
use strata_core::{Epoch, StrataLsn};

use crate::{Error, FORMAT_VERSION, PartitionId, Result, RunId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunKind {
    Base,
    Patch,
    Delta,
}

/// Durable manifest row for one physical run file on disk.
///
/// `RunMeta` is not the run contents; it is the durable root pointer that makes a file part of the
/// live LSM sidecar. A run file is born first as a synced file under `partition-N/`, then becomes
/// reachable only when a manifest containing this row is accepted by the owner. The row is retired
/// when a later manifest replaces the run through delta or major compaction; only after that publish
/// may the file be removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    /// Stable lineage identifier used in compaction event batches and obsolete-run cleanup. The
    /// identifier lets the owner reason about which physical inputs produced an output without
    /// parsing paths or depending on file naming details.
    pub id: RunId,
    /// The LSM level is recorded in the manifest so readers can validate that the file they opened
    /// matches the layer they are about to fold. That avoids treating a base image as residual patch
    /// history, or replaying a delta file as already-materialized state, after corruption or a bad
    /// manifest update.
    pub kind: RunKind,
    /// Runs are partition-local physical files. Keeping the partition in the row gives compaction a
    /// cheap integrity check that a manifest entry has not crossed hash-partition boundaries, which
    /// would otherwise make per-partition folding silently wrong.
    pub partition: PartitionId,
    /// Relative path keeps the durable manifest independent of the process-local root directory. A
    /// reopened sidecar can relocate its root while preserving the same manifest bytes stored by the
    /// owner.
    pub path: String,
    /// Compaction needs a watermark for scheduling and event publication before it has folded every
    /// record. Storing the highest folded LSN here lets a compaction batch advertise its durable
    /// coverage and advance `materialized_through_lsn` without rescanning output files.
    pub max_lsn: Option<StrataLsn>,
    /// Persisting the physical size beside the root pointer gives the owner a cheap storage-cost and
    /// sanity signal for a run that may not be opened during scheduling. It is metadata about the
    /// file's physical footprint, not part of the logical fold order.
    pub file_len: u64,
}

/// Durable LSM stack for a single hash partition.
///
/// This is the partition-local catalog inside the manifest row, not a separate file. It is born with
/// the manifest and evolves by append/compaction: active ingestion appends delta runs, delta
/// compaction replaces those deltas with patch runs, and major compaction collapses base plus patches
/// into a new base. A whole `PartitionManifest` generation is retired when the owner replaces the
/// enclosing `Manifest`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionManifest {
    /// The optional base is the materialized image that lets reads start from one folded row per key
    /// instead of replaying all historical updates. It is absent for a new partition, replaced by
    /// major compaction, and kept stable across delta compaction because delta compaction never reads
    /// or rewrites older materialized state.
    pub base: Option<RunMeta>,
    /// Patch runs are ordered residual histories above the base. Their manifest order is semantic:
    /// major compaction folds older patches before newer patches, so reordering this vector would
    /// change overwrite, tombstone, and pending-map behavior.
    pub patches: Vec<RunMeta>,
    /// Delta runs are the newest sorted update files awaiting shallow compaction. They are visible to
    /// `current_state` reads but not to `materialized_state`; delta compaction retires the whole list
    /// for this partition by publishing a patch run that preserves any history requiring base state.
    pub deltas: Vec<RunMeta>,
    /// This watermark describes how far the base image has absorbed residual patch state. It advances
    /// at major compaction, not merely at delta compaction, because a patch can still contain
    /// tombstones or MapRef rewrites whose physical effect is unresolved until the base row is folded.
    pub materialized_through_lsn: StrataLsn,
}

/// Durable root row for the entire accounting sidecar.
///
/// The manifest is encoded by this crate but stored by the owner, normally in RocksDB beside the
/// derived accounting and GC rows. It is the logical root set for all live run files: any run
/// reachable from this struct must be retained, while old runs can be removed only after a newer
/// manifest that excludes them has been accepted. New manifests are born by cloning the current root,
/// mutating the clone, and advancing its generation; the previous root is retired by the owner's
/// atomic publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The manifest bytes are durable across process versions, so the root row carries an explicit
    /// format fence. Opening with an incompatible format fails before the sidecar interprets stale
    /// paths or record layouts.
    pub format_version: u32,
    /// Monotonic generation of the manifest held by this `AccountingIndex` handle.
    ///
    /// This generation is a local prepared-state guard, not a durable compare-and-swap by itself.
    /// It catches a prepared value that was derived from an older in-memory manifest before that
    /// value can replace this handle's manifest or delete obsolete run files. For example, if two
    /// preparations both start from generation N and one has already been applied locally as N+1, the
    /// second apply is rejected instead of rewinding the handle.
    ///
    /// Durable stale-write prevention still belongs to the owner that stores the manifest. Today the
    /// store sidecar serializes durable publication with a single worker/lock and writes the manifest
    /// in one RocksDB batch with its derived rows. If the design ever allows multiple durable sidecar
    /// publishers, the owner must add a RocksDB-level CAS/transaction or an exclusive process lock
    /// around this generation; the post-publish in-memory check cannot protect already-persisted
    /// state.
    pub generation: u64,
    /// The hash topology is part of the durable contract. Changing it without rebuilding the sidecar
    /// would strand keys in the wrong partition directories and make per-partition compaction
    /// incomplete, so open-time validation rejects mismatches.
    pub partition_count: u32,
    /// Run ids are allocated from the manifest clone so prepared work can create physical files
    /// before publish while still avoiding id/path collisions with future accepted manifests.
    pub next_run_id: RunId,
    /// This map is the live root set for partition-local run files. The owner does not discover live
    /// files by scanning directories; it trusts this catalog, which is why obsolete-file deletion is
    /// delayed until after a prepared manifest has passed the generation guard.
    pub partitions: BTreeMap<PartitionId, PartitionManifest>,
    /// Epoch changes are global accounting timeline entries rather than partition-local blob rows.
    /// Keeping them in the same durable root as run metadata gives blob updates and epoch transitions
    /// one publication order when both are ingested from the active log.
    pub epoch_changes: Vec<EpochChange>,
}

impl Manifest {
    pub(crate) fn new(partition_count: NonZeroU32) -> Self {
        let mut partitions = BTreeMap::new();
        for partition in 0..partition_count.get() {
            partitions.insert(partition, PartitionManifest::default());
        }

        Self {
            format_version: FORMAT_VERSION,
            generation: 0,
            partition_count: partition_count.get(),
            next_run_id: 1,
            partitions,
            epoch_changes: Vec::new(),
        }
    }
}

/// Epoch transition recorded at a global Strata LSN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochChange {
    pub lsn: StrataLsn,
    pub epoch: Epoch,
}

pub fn manifest_to_bytes(manifest: &Manifest) -> Result<Vec<u8>> {
    Ok(bcs::to_bytes(manifest)?)
}

pub fn manifest_from_bytes(bytes: &[u8]) -> Result<Manifest> {
    let manifest: Manifest = bcs::from_bytes(bytes)?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(Error::IncompatibleManifestVersion {
            actual: manifest.format_version,
            expected: FORMAT_VERSION,
        });
    }
    Ok(manifest)
}

pub(crate) fn validate_manifest(manifest: &Manifest, partition_count: NonZeroU32) -> Result<()> {
    if manifest.format_version != FORMAT_VERSION {
        return Err(Error::IncompatibleManifestVersion {
            actual: manifest.format_version,
            expected: FORMAT_VERSION,
        });
    }
    if manifest.partition_count != partition_count.get() {
        return Err(Error::PartitionCountChanged {
            stored: manifest.partition_count,
            configured: partition_count.get(),
        });
    }
    for partition in 0..partition_count.get() {
        if !manifest.partitions.contains_key(&partition) {
            return Err(Error::InvalidPartition(partition));
        }
    }
    Ok(())
}

pub(crate) fn advance_manifest_generation(manifest: &mut Manifest) -> Result<()> {
    manifest.generation =
        manifest
            .generation
            .checked_add(1)
            .ok_or(Error::ManifestGenerationOverflow {
                generation: manifest.generation,
            })?;
    Ok(())
}

pub(crate) fn allocate_run_id(manifest: &mut Manifest) -> RunId {
    let id = manifest.next_run_id;
    manifest.next_run_id = manifest.next_run_id.saturating_add(1);
    id
}

pub(crate) fn partition_mut(
    manifest: &mut Manifest,
    partition: PartitionId,
) -> Result<&mut PartitionManifest> {
    manifest
        .partitions
        .get_mut(&partition)
        .ok_or(Error::InvalidPartition(partition))
}

pub(crate) fn push_epoch_change(manifest: &mut Manifest, change: EpochChange) {
    manifest.epoch_changes.push(change);
    manifest.epoch_changes.sort_by_key(|change| change.lsn);
    manifest
        .epoch_changes
        .dedup_by_key(|change| (change.lsn, change.epoch));
}
