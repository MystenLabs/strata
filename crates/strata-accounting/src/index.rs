//! File-backed accounting-index handle and durable manifest lifecycle.
//!
//! Preparation types live in `prepared`; ingest and query operations are separated from physical
//! run I/O; delta, major, and shard-drop compaction live under `compaction`.

use std::{collections::BTreeMap, fs, num::NonZeroU32, path::PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use strata_core::{BlobKey, SegmentOwner};
use xxhash_rust::xxh3::xxh3_64;

use crate::active_log::AccountingLogEntry;
use crate::events::{CompactionEventBatch, RefEvent, RetireReason};
use crate::manifest::{
    EpochChange, Manifest, PartitionManifest, RunKind, RunMeta, advance_manifest_generation,
    allocate_run_id, manifest_from_bytes, manifest_to_bytes, partition_mut, push_epoch_change,
    push_shard_drop, validate_manifest,
};
use crate::run_io::{
    RunFile, RunRecordReader, RunRecords, open_run_record_reader, partition_dir, run_file_name,
    write_run_file_atomic,
};
use crate::state::{
    BlobUpdate, MaterializedBlobState, PatchRecord, RecordLsn, StateRecord, fold_patch_update,
    fold_update, sort_updates,
};
use crate::{Error, FORMAT_VERSION, PartitionId, Result, RunId};

/// Runtime configuration for opening the file-backed accounting index.
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

mod compaction;
mod ingest;
mod prepared;
mod query;
mod runs;

pub use prepared::{
    PreparedAccountingDeltas, PreparedDeltaCompaction, PreparedDeltaRuns, PreparedEpochChange,
    PreparedMajorCompaction, PreparedShardDrop,
};

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
}
