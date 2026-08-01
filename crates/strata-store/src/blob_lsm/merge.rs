//! The two LSM merge-operator entry points for Store blob state.

use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use strata_lsm::{
    GarbageRecord, MergeOperator, Result, StoredValue, StrataLsn, decode_value, encode_inline_value,
};
use strata_relocation::RelocationScan;

use super::format::{BlobMutation, BlobMutationWithLSN, BlobState, invalid};
use super::reduce::reduce_patch_mutations;
use super::snapshot::BlobCompactionSnapshot;

/// Materializes Store blob operations during reads and compaction.
pub struct BlobMerge;

impl MergeOperator for BlobMerge {
    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut state = match base {
            Some(base) => match decode_value(base)? {
                StoredValue::Inline(bytes) => BlobState::decode(bytes)?,
                StoredValue::Blob { .. } => {
                    return Err(invalid("materialized blob state cannot be segment-backed"));
                }
            },
            None => BlobState::default(),
        };

        for mutation in decode_patches(patches)? {
            state.apply(key, mutation, emit)?;
        }

        Ok(Some(encode_inline_value(&state.encode()?)))
    }

    fn partial_merge(
        &self,
        key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        partial_merge_blob(key, patches, None, emit)
    }
}

fn partial_merge_blob(
    key: &[u8],
    patches: &[(StrataLsn, &[u8])],
    snapshot: Option<&BlobCompactionSnapshot>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<Option<Vec<u8>>> {
    let decoded = decode_patches(patches)?;
    let mutations = reduce_patch_mutations(key, decoded.clone(), snapshot, emit)?;
    if patches.len() < 2 && mutations == decoded {
        return Ok(None);
    }
    Ok(Some(encode_inline_value(
        &BlobMutationWithLSN::encode_batch(&mutations)?,
    )))
}

pub(crate) struct BlobMergeWithRelocations {
    relocations: Option<Mutex<RelocationScan>>,
    snapshot: BlobCompactionSnapshot,
    healed_references: AtomicU64,
}

impl BlobMergeWithRelocations {
    pub(crate) fn new(
        relocations: Option<RelocationScan>,
        snapshot: BlobCompactionSnapshot,
    ) -> Self {
        Self {
            relocations: relocations
                .filter(|relocations| relocations.current().is_some())
                .map(Mutex::new),
            snapshot,
            healed_references: AtomicU64::new(0),
        }
    }

    pub(crate) fn healed_references(&self) -> u64 {
        self.healed_references.load(Ordering::Relaxed)
    }
}

impl MergeOperator for BlobMergeWithRelocations {
    fn merge_base(&self) -> bool {
        true
    }

    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let Some(value) = BlobMerge.merge(key, base, patches, emit)? else {
            return Ok(None);
        };
        let StoredValue::Inline(bytes) = decode_value(&value)? else {
            return Err(invalid("materialized blob state cannot be segment-backed"));
        };
        let mut state = BlobState::decode(bytes)?;
        if let Some(relocations) = &self.relocations {
            let mut relocations = relocations
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            while relocations
                .current()
                .is_some_and(|relocation| relocation.key.as_bytes() < key)
            {
                relocations
                    .advance()
                    .map_err(|error| invalid(format!("relocation scan failed: {error}")))?;
            }
            while let Some(relocation) = relocations
                .current()
                .filter(|relocation| relocation.key.as_bytes() == key)
            {
                if let Some(version) = state
                    .versions
                    .get_mut(&relocation.shard)
                    .filter(|version| version.lsn == relocation.payload_lsn)
                {
                    if version.record_ref.len != relocation.to.len {
                        return Err(invalid("relocation changed the payload length"));
                    }
                    if version.record_ref != relocation.to {
                        version.record_ref = relocation.to;
                        self.healed_references.fetch_add(1, Ordering::Relaxed);
                    }
                }
                relocations
                    .advance()
                    .map_err(|error| invalid(format!("relocation scan failed: {error}")))?;
            }
        }

        state.prune_with_snapshot(key, &self.snapshot, emit)?;
        if state.versions.is_empty() && state.lifetime.is_none() {
            return Ok(None);
        }
        Ok(Some(encode_inline_value(&state.encode()?)))
    }

    fn partial_merge(
        &self,
        key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        partial_merge_blob(key, patches, Some(&self.snapshot), emit)
    }
}

pub(crate) fn decode_patches(patches: &[(StrataLsn, &[u8])]) -> Result<Vec<BlobMutationWithLSN>> {
    let mut mutations = Vec::new();
    for &(outer_lsn, encoded) in patches {
        match decode_value(encoded)? {
            StoredValue::Blob {
                metadata,
                record_ref,
            } => mutations.push(BlobMutationWithLSN {
                lsn: outer_lsn,
                mutation: BlobMutation::decode_put_metadata(metadata, record_ref)?,
            }),
            StoredValue::Inline(bytes) => {
                mutations.extend(BlobMutationWithLSN::decode_inline(outer_lsn, bytes)?);
            }
        }
    }
    mutations.sort_unstable_by_key(|mutation| mutation.lsn);
    for pair in mutations.windows(2) {
        if pair[0].lsn >= pair[1].lsn {
            return Err(invalid("patch mutation LSNs are not strictly increasing"));
        }
    }
    Ok(mutations)
}
