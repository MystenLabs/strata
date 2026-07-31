use rocksdb::MergeOperands;
use strata_lsm::{GarbageLogPosition, Manifest, ManifestEdit};
use typed_store::{
    Map,
    rocks::{DBBatch, default_db_options},
};

use crate::{Error, Result, StrataIndex};

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

    /// Adds one edit operand to an existing RocksDB batch.
    ///
    /// Callers can place edits for several LSMs beside segment or frontier updates in the same
    /// batch. RocksDB then gives the complete publication one sequence number. The caller must keep
    /// the edit's input SSTs reserved until that batch commits; otherwise another publisher could
    /// invalidate the live-file check performed here.
    pub fn merge_lsm_manifest_batch(
        &self,
        batch: &mut DBBatch,
        name: &str,
        edit: &ManifestEdit,
    ) -> Result<()> {
        validate_name(name)?;
        edit.validate()
            .map_err(|error| Error::InvalidLsmManifest(error.to_string()))?;
        let mut current = self.get_lsm_manifest(name)?.ok_or_else(|| {
            Error::InvalidLsmManifest(format!("manifest {name:?} has not been initialized"))
        })?;
        current
            .apply(edit)
            .map_err(|error| Error::InvalidLsmManifest(error.to_string()))?;
        let operand =
            bcs::to_bytes(edit).map_err(|error| Error::Serialization(error.to_string()))?;
        batch.partial_merge_batch(self.lsm_manifests(), [(name.to_owned(), operand)])?;
        Ok(())
    }
}

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
