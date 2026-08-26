//! The two LSM merge-operator entry points for Store blob state.

use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use super::format::{BlobMutation, BlobMutationWithLSN, BlobState, invalid};
use super::garbage::{emit_lifetime_change, terminal_garbage_record};
use super::reduce::reduce_patch_mutations;
use super::snapshot::BlobCompactionSnapshot;
use super::state::effective_lifecycle;
use crate::relocation::{RelocationEntry, RelocationScan};
use strata_core::GarbageEvent;
use strata_lsm::{
    GarbageRecord, MergeOperator, Result, StoredValue, StrataLsn, decode_value, encode_inline_value,
};

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

    fn relocations_for_key(&self, key: &[u8]) -> Result<Vec<RelocationEntry>> {
        let Some(relocations) = &self.relocations else {
            return Ok(Vec::new());
        };
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
        let mut entries = Vec::new();
        while let Some(relocation) = relocations
            .current()
            .filter(|relocation| relocation.key.as_bytes() == key)
        {
            entries.push(relocation.clone());
            relocations
                .advance()
                .map_err(|error| invalid(format!("relocation scan failed: {error}")))?;
        }
        Ok(entries)
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
        let relocations = self.relocations_for_key(key)?;
        // Remember relocations whose old physical ref entered this merge. If that ref disappears,
        // the destination was born dead and needs its own terminal event. An already-healed input
        // is retired by the normal blob mutation below and must not be retired a second time.
        let mut input_record_refs = BTreeMap::new();
        if !relocations.is_empty() {
            if let Some(base) = base {
                let StoredValue::Inline(bytes) = decode_value(base)? else {
                    return Err(invalid("materialized blob state cannot be segment-backed"));
                };
                for (shard, version) in BlobState::decode(bytes)?.versions {
                    input_record_refs.insert((shard, version.lsn), version.record_ref);
                }
            }
            for mutation in decode_patches(patches)? {
                if let BlobMutation::Put {
                    shard, record_ref, ..
                } = mutation.mutation
                {
                    input_record_refs.insert((shard, mutation.lsn), record_ref);
                }
            }
        }
        let Some(value) = BlobMerge.merge(key, base, patches, emit)? else {
            return Ok(None);
        };
        let StoredValue::Inline(bytes) = decode_value(&value)? else {
            return Err(invalid("materialized blob state cannot be segment-backed"));
        };
        let mut state = BlobState::decode(bytes)?;
        for relocation in relocations {
            let lifecycle = state
                .versions
                .get(&relocation.shard)
                .filter(|version| version.lsn == relocation.payload_lsn)
                .map(|version| effective_lifecycle(state.lifetime, version));
            if let Some(lifecycle) = lifecycle {
                let version = state
                    .versions
                    .get_mut(&relocation.shard)
                    .expect("matching relocation version was resolved above");
                if version.record_ref.len != relocation.to.len {
                    return Err(invalid("relocation changed the payload length"));
                }
                if version.record_ref != relocation.to {
                    version.record_ref = relocation.to;
                    self.healed_references.fetch_add(1, Ordering::Relaxed);
                    if let Some(lifecycle) = lifecycle {
                        emit_lifetime_change(
                            key,
                            relocation.publish_lsn,
                            *version,
                            None,
                            Some(lifecycle),
                            emit,
                        )?;
                    }
                }
            } else if relocation.publish_lsn <= self.snapshot.materialized_through_lsn
                && input_record_refs
                    .get(&(relocation.shard, relocation.payload_lsn))
                    .is_some_and(|record_ref| *record_ref != relocation.to)
            {
                // GC may conservatively publish a copy whose tombstone, overwrite, or expiry had
                // not reached the garbage log yet. Once a complete blob compaction proves that the
                // payload identity is absent, retire the physical destination created by GC.
                emit(terminal_garbage_record(
                    key,
                    relocation.publish_lsn,
                    relocation.to,
                    None,
                    GarbageEvent::Retired {
                        record: relocation.to,
                    },
                )?)?;
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
