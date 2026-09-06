use std::sync::MutexGuard;

use lsm::{GarbageLogPosition, Manifest, ManifestEdit};
use rocksdb::MergeOperands;
use typed_store::{
    Map,
    rocks::{DBBatch, default_db_options},
};

use crate::{Error, Result, StrataIndex};

/// Exclusive right to publish LSM manifests, held from preparing an edit until the batch carrying
/// it has been written.
pub struct LsmManifestPublishGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}

const MAX_NAME_BYTES: usize = 1024;

impl StrataIndex {
    pub fn get_garbage_log_position(&self, name: &str) -> Result<Option<GarbageLogPosition>> {
        validate_name(name)?;
        Ok(self.garbage_log_positions.get(&name.to_owned())?)
    }

    /// Stages the end of the last synced frame accepted by the caller.
    pub fn put_garbage_log_position_batch(
        &self,
        batch: &mut DBBatch,
        name: &str,
        position: GarbageLogPosition,
    ) -> Result<()> {
        validate_name(name)?;
        batch.insert_batch(self.garbage_log_positions(), [(name.to_owned(), position)])?;
        Ok(())
    }

    pub fn get_lsm_manifest(&self, name: &str) -> Result<Option<Manifest>> {
        validate_name(name)?;
        Ok(self.lsm_manifests.get(&name.to_owned())?)
    }

    /// Stores a validated materialized manifest in an existing RocksDB batch.
    pub fn put_lsm_manifest_batch(
        &self,
        batch: &mut DBBatch,
        name: &str,
        manifest: &Manifest,
    ) -> Result<()> {
        validate_name(name)?;
        manifest
            .validate()
            .map_err(|error| Error::InvalidLsmManifest(error.to_string()))?;
        batch.insert_batch(self.lsm_manifests(), [(name.to_owned(), manifest)])?;
        Ok(())
    }

    /// Takes the manifest publication lock.
    ///
    /// Every publisher holds this from [`Self::merge_lsm_manifest_batch`] until the batch carrying
    /// the edit has been written, so each publication reads the manifest the previous one wrote.
    /// One lock covers every manifest: a batch may carry edits for several, and publications are
    /// rare enough (a few per second) that serializing them across manifests costs nothing.
    pub fn lock_lsm_manifests(&self) -> LsmManifestPublishGuard<'_> {
        LsmManifestPublishGuard {
            _guard: self
                .manifest_publish_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    /// Applies one edit to the live manifest and stages the whole result in an existing batch.
    ///
    /// Callers can place edits for several LSMs beside segment or frontier updates in the same
    /// batch. RocksDB then gives the complete publication one sequence number. The caller must keep
    /// the edit's input SSTs reserved until that batch commits; otherwise another publisher could
    /// invalidate the live-file check performed here.
    ///
    /// The manifest is written whole rather than as a merge operand. Operands made every read
    /// re-apply every edit since RocksDB last compacted the key, so a busy compactor (hundreds of
    /// tier merges a minute over many partitions) made each manifest read, and with it each pass,
    /// slower for the life of the process. Writing the applied manifest keeps reads flat; the
    /// publication guard keeps concurrent publishers from overwriting each other's edit, which is
    /// what the merge operands used to allow lock-free. Returns the manifest as published.
    pub fn merge_lsm_manifest_batch(
        &self,
        batch: &mut DBBatch,
        name: &str,
        edit: &ManifestEdit,
        _guard: &LsmManifestPublishGuard<'_>,
    ) -> Result<Manifest> {
        validate_name(name)?;
        edit.validate()
            .map_err(|error| Error::InvalidLsmManifest(error.to_string()))?;
        let mut current = self.get_lsm_manifest(name)?.ok_or_else(|| {
            Error::InvalidLsmManifest(format!("manifest {name:?} has not been initialized"))
        })?;
        current
            .apply(edit)
            .map_err(|error| Error::InvalidLsmManifest(error.to_string()))?;
        batch.insert_batch(self.lsm_manifests(), [(name.to_owned(), &current)])?;
        Ok(current)
    }
}

/// Merge operator kept for manifests written by earlier versions as operand chains; new
/// publications write the manifest whole, which supersedes any operands before them.
pub(crate) fn lsm_manifests_cf_options() -> rocksdb::Options {
    let mut options = default_db_options().options;
    options.set_merge_operator(
        "strata-lsm-manifest-merge",
        |_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands| {
            merge_manifest(existing, operands)
        },
        // Applying an edit requires the current live file set. Leaving operands separate keeps
        // partial merge from accidentally accepting a removal it cannot validate.
        |_key: &[u8], _existing: Option<&[u8]>, _operands: &MergeOperands| None,
    );
    options
}

fn merge_manifest(existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    // Public writes validate against the live manifest first. Failure here therefore means stored
    // bytes are corrupt or a caller bypassed the reservation/publication contract.
    let mut manifest = bcs::from_bytes::<Manifest>(existing?).ok()?;
    for operand in operands {
        let edit = bcs::from_bytes::<ManifestEdit>(operand).ok()?;
        manifest.apply(&edit).ok()?;
    }
    bcs::to_bytes(&manifest).ok()
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        Err(Error::InvalidLsmManifest(
            "name must contain 1 to 1024 bytes".to_owned(),
        ))
    } else {
        Ok(())
    }
}
