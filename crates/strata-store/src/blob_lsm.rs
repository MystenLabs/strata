//! Store-owned blob state stored behind one exact LSM key.

use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use strata_core::{
    BlobKey, BlobLifecycle, Epoch, GarbageEvent, RecordRef, SegmentGcSummaryDelta, SegmentId,
    SegmentKey, ShardId, ShardInfo, ShardKey, ShardState,
};
use strata_lsm::{
    GarbageRecord, MergeOperator, Result, StoredValue, StrataLsn, decode_value, encode_inline_value,
};
use strata_relocation::RelocationScan;

pub use crate::blob_format::{BlobLifetime, BlobState, BlobVersion};
use crate::blob_format::{BlobMutation, BlobMutationWithLSN, invalid};

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
    lazy_global: Option<&LazyGlobalState>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<Option<Vec<u8>>> {
    let decoded = decode_patches(patches)?;
    let mutations = reduce_patch_mutations(key, decoded.clone(), lazy_global, emit)?;
    if patches.len() < 2 && mutations == decoded {
        return Ok(None);
    }
    Ok(Some(encode_inline_value(
        &BlobMutationWithLSN::encode_batch(&mutations)?,
    )))
}

pub(crate) struct BlobMergeWithRelocations {
    relocations: Option<Mutex<RelocationScan>>,
    lazy_global: LazyGlobalState,
    healed_references: AtomicU64,
}

/// Durable global facts captured once before one blob-LSM compaction.
///
/// The merge operator never performs an index lookup for an individual blob. Epoch transitions,
/// shard generation fences, and bulk-reclaimed shard segments are all resolved from this snapshot.
#[derive(Debug, Clone, Default)]
pub(crate) struct LazyGlobalState {
    /// Complete manifest frontier bounding every global fact this compaction may apply.
    pub(crate) materialized_through_lsn: StrataLsn,
    pub(crate) materialize_from_lsn: StrataLsn,
    pub(crate) epoch_changes: Vec<(StrataLsn, Epoch)>,
    pub(crate) shard_infos: BTreeMap<ShardId, ShardInfo>,
    pub(crate) shard_drop_lsns: BTreeMap<ShardKey, StrataLsn>,
    pub(crate) reclaimed_shard_segments: BTreeMap<SegmentId, ShardKey>,
}

#[derive(Debug, Clone, Copy)]
struct LazyTransition {
    lsn: StrataLsn,
    emit_garbage: bool,
}

impl BlobMergeWithRelocations {
    pub(crate) fn new(relocations: Option<RelocationScan>, lazy_global: LazyGlobalState) -> Self {
        Self {
            relocations: relocations
                .filter(|relocations| relocations.current().is_some())
                .map(Mutex::new),
            lazy_global,
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

        state.prune_lazy_global(key, &self.lazy_global, emit)?;
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
        partial_merge_blob(key, patches, Some(&self.lazy_global), emit)
    }
}

impl BlobState {
    /// Resolves one shard at the current epoch.
    pub fn resolve(
        &self,
        shard: ShardKey,
        current_epoch: Epoch,
    ) -> Option<(&BlobVersion, Option<BlobLifecycle>)> {
        let version = self.versions.get(&shard)?;
        let lifecycle = effective_lifecycle(self.lifetime, version);
        if lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch) {
            return None;
        }
        Some((version, lifecycle))
    }

    fn apply(
        &mut self,
        key: &[u8],
        mutation: BlobMutationWithLSN,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let BlobMutationWithLSN { lsn, mutation } = mutation;
        match mutation {
            BlobMutation::Put {
                shard,
                write_epoch,
                record_ref,
            } => self.put(key, lsn, shard, write_epoch, record_ref, emit),
            BlobMutation::SetLifetime {
                logical_end_epoch,
                current_epoch,
            } => self.set_lifetime(key, lsn, logical_end_epoch, current_epoch, emit),
            BlobMutation::Tombstone { shard } => self.tombstone(key, lsn, shard, emit),
        }
    }

    fn prune_lazy_global(
        &mut self,
        key: &[u8],
        global: &LazyGlobalState,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let shards = self.versions.keys().copied().collect::<Vec<_>>();
        for shard in shards {
            let version = self
                .versions
                .get(&shard)
                .copied()
                .expect("shard key was collected from this state");
            let lifecycle = effective_lifecycle(self.lifetime, &version);

            if let Some(transition) = global.shard_retirement(shard) {
                self.versions.remove(&shard);
                let bulk_reclaimed = global
                    .reclaimed_shard_segments
                    .get(&version.record_ref.segment_id)
                    .is_some_and(|owner| *owner == shard);
                if transition.emit_garbage && !bulk_reclaimed {
                    emit_record(
                        key,
                        transition.lsn,
                        version,
                        lifecycle,
                        GarbageEvent::Retired {
                            record: version.record_ref,
                        },
                        emit,
                    )?;
                }
                continue;
            }

            let Some(lifecycle) = lifecycle else {
                continue;
            };
            let Some(transition) = global.expiry(lifecycle.logical_end_epoch) else {
                continue;
            };
            self.versions.remove(&shard);
            if transition.emit_garbage {
                emit_record(
                    key,
                    transition.lsn,
                    version,
                    Some(lifecycle),
                    GarbageEvent::Expired {
                        record: version.record_ref,
                    },
                    emit,
                )?;
            }
        }
        Ok(())
    }

    fn put(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        shard: ShardKey,
        write_epoch: Epoch,
        record_ref: RecordRef,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let next = BlobVersion {
            lsn,
            write_epoch,
            record_ref,
        };
        if let Some(previous) = self.versions.insert(shard, next) {
            emit_record(
                key,
                lsn,
                previous,
                effective_lifecycle(self.lifetime, &previous),
                GarbageEvent::Retired {
                    record: previous.record_ref,
                },
                emit,
            )?;
        }
        if let Some(lifecycle) = effective_lifecycle(self.lifetime, &next) {
            emit_lifetime_change(key, lsn, next, None, Some(lifecycle), emit)?;
        }
        Ok(())
    }

    fn set_lifetime(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let previous = self.lifetime;
        let previous_expired =
            previous.is_some_and(|head| head.lifecycle.logical_end_epoch <= current_epoch);
        let extension_count = if previous_expired {
            0
        } else {
            previous.map_or(0, |head| head.lifecycle.extension_count.saturating_add(1))
        };
        let next = BlobLifetime {
            lsn,
            lifecycle: BlobLifecycle {
                logical_end_epoch,
                extension_count,
            },
        };

        if previous_expired {
            for version in self.versions.values() {
                emit_record(
                    key,
                    lsn,
                    *version,
                    effective_lifecycle(previous, version),
                    GarbageEvent::Expired {
                        record: version.record_ref,
                    },
                    emit,
                )?;
            }
            self.versions.clear();
        } else {
            for version in self.versions.values() {
                let before = effective_lifecycle(previous, version);
                let after = effective_lifecycle(Some(next), version);
                if before != after {
                    emit_lifetime_change(key, lsn, *version, before, after, emit)?;
                }
            }
        }
        self.lifetime = Some(next);
        Ok(())
    }

    fn tombstone(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        shard: ShardKey,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        if let Some(version) = self.versions.remove(&shard) {
            emit_record(
                key,
                lsn,
                version,
                effective_lifecycle(self.lifetime, &version),
                GarbageEvent::Retired {
                    record: version.record_ref,
                },
                emit,
            )?;
        }
        Ok(())
    }
}

fn decode_patches(patches: &[(StrataLsn, &[u8])]) -> Result<Vec<BlobMutationWithLSN>> {
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

#[derive(Debug, Clone, Copy)]
struct PatchBucket {
    mutation_index: usize,
    // A bucket always holds the last mutation for its shard. `None` is a tombstone.
    put: Option<BlobVersion>,
    // `None` means the patch does not prove a lifetime for this bucket.
    logical_end_epoch: Option<Epoch>,
}

enum PatchEvent {
    Mutation(usize),
    ChangeEpoch {
        lsn: StrataLsn,
        epoch: Epoch,
        emit_garbage: bool,
    },
}

fn merge_patch_events<'a>(
    mutations: &'a [BlobMutationWithLSN],
    lazy_global: Option<&'a LazyGlobalState>,
) -> impl Iterator<Item = PatchEvent> + 'a {
    let (epoch_changes, materialize_from_lsn) = match lazy_global {
        Some(global) => {
            let end = global
                .epoch_changes
                .partition_point(|(lsn, _)| *lsn <= global.materialized_through_lsn);
            (&global.epoch_changes[..end], global.materialize_from_lsn)
        }
        None => (&[][..], StrataLsn::default()),
    };

    let mut mutation_index = 0;
    let mut epoch_change_index = 0;
    std::iter::from_fn(move || {
        if mutation_index == mutations.len() && epoch_change_index == epoch_changes.len() {
            return None;
        }

        // Both lists are already sorted. Real mutations win the impossible equal-LSN tie.
        let take_mutation = epoch_change_index == epoch_changes.len()
            || mutation_index < mutations.len()
                && mutations[mutation_index].lsn <= epoch_changes[epoch_change_index].0;

        if take_mutation {
            let event = PatchEvent::Mutation(mutation_index);
            mutation_index += 1;
            Some(event)
        } else {
            let (lsn, epoch) = epoch_changes[epoch_change_index];
            let event = PatchEvent::ChangeEpoch {
                lsn,
                epoch,
                emit_garbage: lsn >= materialize_from_lsn,
            };
            epoch_change_index += 1;
            Some(event)
        }
    })
}

/// Reduces all selected patches for one key while treating the materialized base as unknown.
///
/// Epoch changes are virtual mutations. The reducer merges them with the real mutations by LSN and
/// keeps one bucket per shard. A Put or Tombstone replaces its shard's bucket; a SetLifetime changes
/// the lifetime of every bucket; and an epoch change drops the buckets whose lifetime has ended.
/// After the scan, every SetLifetime and the mutation left in each bucket survive in their original
/// LSN order.
///
/// Every `SetLifetime` is retained: an older lifetime may expire an unknown-base version before a
/// later lifetime replaces it, and the complete chain determines `extension_count`. Patch-local
/// Puts can still be removed once a later same-shard mutation or a visible, patch-known expiry makes
/// them terminal. Exact terminal timing, and whether a dead record is classified as `Retired` or
/// `Expired`, are intentionally not semantic; exactly one terminal event must account for it.
///
/// Examples use `P1(A)@1` for a Put of A in shard 1, `T1@1` for its Tombstone, `L(10)@1`
/// for a SetLifetime ending at epoch 10, `E(10)@1` for an epoch transition, and `R`/`X` for emitted
/// Retired/Expired garbage.
///
/// - Same-shard mutations collapse across retained lifetimes:
///
///   ```text
///   patches:  P1(A)@A -> P1(B)@B -> L(X)@C -> P1(D)@D -> T1@E
///   reduced:                          L(X)@C               -> T1@E
///   emitted:             R(A)@B                 R(B)@D    R(D)@E
///   ```
///
///   Applying `L(X)` to an unknown base before `T1` may move that base record's terminal cause or
///   LSN, which is allowed by the terminal-equivalence contract.
///
/// - A patch lifetime applies to current Puts and future Puts, but a tombstone ends only its shard's
///   current version:
///
///   ```text
///   patches:  P1(A)@1 -> L(10)@2 -> T1@3 -> P1(B)@4 -> E(10)@5
///   reduced:             L(10)@2
///   emitted:                         R(A)@3             X(B)@5
///   ```
///
///   The lifetime survives `T1`, so B inherits it and expires.
///
/// - A later lifetime is retained and can extend a current version before the old end epoch:
///
///   ```text
///   patches:  L(10)@1 -> P1(A)@2 -> L(20)@3 -> E(10)@4
///   reduced:  L(10)@1 -> P1(A)@2 -> L(20)@3
///   emitted:  none
///   ```
///
/// - Expiry is applied only when its transition is at or below `materialized_through_lsn`:
///
///   ```text
///   patches:  L(10)@1 -> P1(A)@2
///   global:   E(10)@5, materialized through 4
///   reduced:  L(10)@1 -> P1(A)@2
///   emitted:  none
///   ```
///
///   Once the frontier reaches 5, the same reduction retains only `L(10)@1` and emits `X(A)@5`.
///
/// - A Put whose only possible lifetime comes from the unknown base cannot be eagerly expired:
///
///   ```text
///   base:     L(10)
///   patches:  P1(A)@2
///   global:   E(10)@5
///   reduced:  P1(A)@2
///   ```
fn reduce_patch_mutations(
    key: &[u8],
    mutations: Vec<BlobMutationWithLSN>,
    lazy_global: Option<&LazyGlobalState>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<Vec<BlobMutationWithLSN>> {
    let mut buckets = BTreeMap::<ShardKey, PatchBucket>::new();
    let mut current_lifetime = None::<Epoch>;
    for event in merge_patch_events(&mutations, lazy_global) {
        let mutation_index = match event {
            PatchEvent::Mutation(mutation_index) => mutation_index,
            PatchEvent::ChangeEpoch {
                lsn,
                epoch,
                emit_garbage,
            } => {
                change_patch_epoch(key, lsn, epoch, emit_garbage, &mut buckets, emit)?;
                if current_lifetime.is_some_and(|logical_end_epoch| logical_end_epoch <= epoch) {
                    // Future mutations start a new bucket generation and do not inherit an expired
                    // lifetime. The SetLifetime remains in the output as a base barrier.
                    current_lifetime = None;
                }
                continue;
            }
        };

        let BlobMutationWithLSN { lsn, mutation } = mutations[mutation_index];
        match mutation {
            BlobMutation::Put {
                shard,
                write_epoch,
                record_ref,
            } => {
                let version = BlobVersion {
                    lsn,
                    write_epoch,
                    record_ref,
                };
                let logical_end_epoch =
                    current_lifetime.filter(|logical_end_epoch| *logical_end_epoch > write_epoch);
                replace_patch_bucket(
                    key,
                    shard,
                    PatchBucket {
                        mutation_index,
                        put: Some(version),
                        logical_end_epoch,
                    },
                    lsn,
                    &mut buckets,
                    emit,
                )?;
            }
            BlobMutation::SetLifetime {
                logical_end_epoch, ..
            } => {
                current_lifetime = Some(logical_end_epoch);
                for bucket in buckets.values_mut() {
                    bucket.logical_end_epoch = Some(logical_end_epoch);
                }
            }
            BlobMutation::Tombstone { shard } => {
                replace_patch_bucket(
                    key,
                    shard,
                    PatchBucket {
                        mutation_index,
                        put: None,
                        logical_end_epoch: current_lifetime,
                    },
                    lsn,
                    &mut buckets,
                    emit,
                )?;
            }
        }
    }

    let mut keep = vec![false; mutations.len()];
    for (index, mutation) in mutations.iter().enumerate() {
        if matches!(mutation.mutation, BlobMutation::SetLifetime { .. }) {
            keep[index] = true;
        }
    }
    for bucket in buckets.values() {
        keep[bucket.mutation_index] = true;
    }
    Ok(mutations
        .into_iter()
        .zip(keep)
        .filter_map(|(mutation, keep)| keep.then_some(mutation))
        .collect())
}

fn replace_patch_bucket(
    key: &[u8],
    shard: ShardKey,
    next: PatchBucket,
    lsn: StrataLsn,
    buckets: &mut BTreeMap<ShardKey, PatchBucket>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    if let Some(previous) = buckets.insert(shard, next)
        && let Some(version) = previous.put
    {
        emit(terminal_garbage_record(
            key,
            lsn,
            version.record_ref,
            None,
            GarbageEvent::Retired {
                record: version.record_ref,
            },
        )?)?;
    }
    Ok(())
}

fn change_patch_epoch(
    key: &[u8],
    transition_lsn: StrataLsn,
    epoch: Epoch,
    emit_garbage: bool,
    buckets: &mut BTreeMap<ShardKey, PatchBucket>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    let expired_shards = buckets
        .iter()
        .filter_map(|(&shard, bucket)| {
            bucket
                .logical_end_epoch
                .is_some_and(|logical_end_epoch| logical_end_epoch <= epoch)
                .then_some(shard)
        })
        .collect::<Vec<_>>();

    for shard in expired_shards {
        let bucket = buckets
            .remove(&shard)
            .expect("expired shard was collected from its bucket");
        if emit_garbage && let Some(version) = bucket.put {
            emit(terminal_garbage_record(
                key,
                transition_lsn,
                version.record_ref,
                None,
                GarbageEvent::Expired {
                    record: version.record_ref,
                },
            )?)?;
        }
    }
    Ok(())
}

impl LazyGlobalState {
    fn expiry(&self, logical_end_epoch: Epoch) -> Option<LazyTransition> {
        let index = self
            .epoch_changes
            .partition_point(|(_, epoch)| *epoch < logical_end_epoch);
        let &(lsn, _) = self.epoch_changes.get(index)?;
        if lsn > self.materialized_through_lsn {
            return None;
        }
        Some(LazyTransition {
            lsn,
            emit_garbage: lsn >= self.materialize_from_lsn,
        })
    }

    fn shard_retirement(&self, shard: ShardKey) -> Option<LazyTransition> {
        if let Some(&drop_lsn) = self.shard_drop_lsns.get(&shard) {
            if drop_lsn > self.materialized_through_lsn {
                return None;
            }
            return Some(LazyTransition {
                lsn: drop_lsn,
                emit_garbage: drop_lsn >= self.materialize_from_lsn,
            });
        }

        let obsolete = match self.shard_infos.get(&shard.id) {
            Some(info) if shard.generation < info.current_generation => true,
            Some(info)
                if shard.generation == info.current_generation
                    && info.state == ShardState::Dropped =>
            {
                true
            }
            _ => false,
        };
        // A missing drop tombstone identifies a generation handled before lazy materialization was
        // introduced. Its physical retirement was already projected by the pre-cutover path.
        obsolete.then_some(LazyTransition {
            lsn: self.materialized_through_lsn,
            emit_garbage: false,
        })
    }
}

fn effective_lifecycle(
    lifetime: Option<BlobLifetime>,
    version: &BlobVersion,
) -> Option<BlobLifecycle> {
    let lifetime = lifetime?;
    if lifetime.lsn <= version.lsn && lifetime.lifecycle.logical_end_epoch <= version.write_epoch {
        None
    } else {
        Some(lifetime.lifecycle)
    }
}

fn emit_lifetime_change(
    key: &[u8],
    lsn: StrataLsn,
    version: BlobVersion,
    before: Option<BlobLifecycle>,
    after: Option<BlobLifecycle>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    let bytes = i128::from(version.record_ref.len);
    let mut summary_delta = SegmentGcSummaryDelta::default();
    classify_lifetime(&mut summary_delta, before, -bytes, -1, true);
    classify_lifetime(&mut summary_delta, after, bytes, 1, true);
    emit(GarbageRecord {
        key: segment_key(key, version.record_ref)?,
        lsn,
        event: GarbageEvent::SetLifecycle {
            record: version.record_ref,
            lifecycle: after,
        },
        summary_delta,
    })
}

fn emit_record(
    key: &[u8],
    lsn: StrataLsn,
    version: BlobVersion,
    lifecycle: Option<BlobLifecycle>,
    event: GarbageEvent,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    emit(terminal_garbage_record(
        key,
        lsn,
        version.record_ref,
        lifecycle,
        event,
    )?)
}

pub(crate) fn terminal_garbage_record(
    key: &[u8],
    lsn: StrataLsn,
    record: RecordRef,
    lifecycle: Option<BlobLifecycle>,
    event: GarbageEvent,
) -> Result<GarbageRecord> {
    let bytes = i128::from(record.len);
    let mut summary_delta = SegmentGcSummaryDelta {
        live_bytes: -bytes,
        live_ref_count: -1,
        ..SegmentGcSummaryDelta::default()
    };
    match event {
        GarbageEvent::Retired { .. } => {
            summary_delta.retired_bytes = bytes;
            classify_lifetime(&mut summary_delta, lifecycle, -bytes, -1, true);
        }
        GarbageEvent::Expired { .. } => {
            summary_delta.expired_bytes = bytes;
            classify_lifetime(&mut summary_delta, lifecycle, -bytes, -1, false);
        }
        GarbageEvent::SetLifecycle { .. } => unreachable!("handled separately"),
    }
    Ok(GarbageRecord {
        key: segment_key(key, record)?,
        lsn,
        event,
        summary_delta,
    })
}

fn classify_lifetime(
    delta: &mut SegmentGcSummaryDelta,
    lifecycle: Option<BlobLifecycle>,
    bytes: i128,
    refs: i128,
    include_extension: bool,
) {
    match lifecycle {
        Some(lifecycle) => {
            *delta
                .epoch_bytes
                .entry(lifecycle.logical_end_epoch)
                .or_default() += bytes;
            *delta
                .epoch_refs
                .entry(lifecycle.logical_end_epoch)
                .or_default() += refs;
            if include_extension {
                *delta
                    .extension_counts
                    .entry(lifecycle.extension_count)
                    .or_default() += refs;
            }
        }
        None => {
            delta.unknown_lifetime_bytes += bytes;
            delta.unknown_lifetime_ref_count += refs;
        }
    }
}

fn segment_key(key: &[u8], record_ref: RecordRef) -> Result<SegmentKey> {
    Ok(SegmentKey {
        segment_id: record_ref.segment_id,
        blob_key: BlobKey::new(key.to_vec()).map_err(|error| invalid(error.to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_lsm::encode_blob_value;

    fn shard(id: u32, generation: u64) -> ShardKey {
        ShardKey { id, generation }
    }

    fn record(segment_id: u64, offset: u64) -> RecordRef {
        RecordRef {
            segment_id,
            offset,
            len: 100,
        }
    }

    fn put_patch(shard: ShardKey, write_epoch: Epoch, record_ref: RecordRef) -> Vec<u8> {
        encode_blob_value(
            &BlobMutation::encode_put_metadata(shard, write_epoch),
            record_ref,
        )
    }

    fn inline_patch(mutation: BlobMutation) -> Vec<u8> {
        encode_inline_value(&mutation.encode_inline().unwrap())
    }

    fn merge(patches: &[(StrataLsn, Vec<u8>)]) -> (BlobState, Vec<GarbageRecord>) {
        let borrowed = patches
            .iter()
            .map(|(lsn, bytes)| (*lsn, bytes.as_slice()))
            .collect::<Vec<_>>();
        let mut garbage = Vec::new();
        let encoded = BlobMerge
            .merge(b"blob", None, &borrowed, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
            panic!("blob merge must materialize inline state");
        };
        (BlobState::decode(bytes).unwrap(), garbage)
    }

    fn partial_merge_with_global(
        patches: &[(StrataLsn, Vec<u8>)],
        global: LazyGlobalState,
    ) -> (Vec<u8>, Vec<GarbageRecord>) {
        let borrowed = patches
            .iter()
            .map(|(lsn, bytes)| (*lsn, bytes.as_slice()))
            .collect::<Vec<_>>();
        let merge = BlobMergeWithRelocations::new(None, global);
        let mut garbage = Vec::new();
        let value = merge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .expect("test patches must produce a partial merge");
        (value, garbage)
    }

    fn aggregate_garbage_deltas(records: &[GarbageRecord]) -> SegmentGcSummaryDelta {
        let mut total = SegmentGcSummaryDelta::default();
        for record in records {
            let delta = &record.summary_delta;
            total.total_bytes += delta.total_bytes;
            total.live_bytes += delta.live_bytes;
            total.retired_bytes += delta.retired_bytes;
            total.expired_bytes += delta.expired_bytes;
            total.live_ref_count += delta.live_ref_count;
            total.unknown_lifetime_bytes += delta.unknown_lifetime_bytes;
            total.unknown_lifetime_ref_count += delta.unknown_lifetime_ref_count;
            for (&epoch, &change) in &delta.epoch_bytes {
                *total.epoch_bytes.entry(epoch).or_default() += change;
            }
            for (&epoch, &change) in &delta.epoch_refs {
                *total.epoch_refs.entry(epoch).or_default() += change;
            }
            for (&extension_count, &change) in &delta.extension_counts {
                *total.extension_counts.entry(extension_count).or_default() += change;
            }
        }
        total.epoch_bytes.retain(|_, change| *change != 0);
        total.epoch_refs.retain(|_, change| *change != 0);
        total.extension_counts.retain(|_, change| *change != 0);
        total
    }

    fn assert_functionally_equivalent_garbage(left: &[GarbageRecord], right: &[GarbageRecord]) {
        let left = aggregate_garbage_deltas(left);
        let right = aggregate_garbage_deltas(right);
        assert_eq!(left.total_bytes, right.total_bytes);
        assert_eq!(left.live_bytes, right.live_bytes);
        assert_eq!(
            left.retired_bytes + left.expired_bytes,
            right.retired_bytes + right.expired_bytes
        );
        assert_eq!(left.live_ref_count, right.live_ref_count);
        assert_eq!(left.unknown_lifetime_bytes, right.unknown_lifetime_bytes);
        assert_eq!(
            left.unknown_lifetime_ref_count,
            right.unknown_lifetime_ref_count
        );
        assert_eq!(left.epoch_bytes, right.epoch_bytes);
        assert_eq!(left.epoch_refs, right.epoch_refs);
    }

    fn compact_state(
        state: BlobState,
        global: LazyGlobalState,
    ) -> (Option<BlobState>, Vec<GarbageRecord>) {
        let base = encode_inline_value(&state.encode().unwrap());
        let merge = BlobMergeWithRelocations::new(None, global);
        let mut garbage = Vec::new();
        let value = merge
            .merge(b"blob", Some(&base), &[], &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap();
        let state = value.map(|value| {
            let StoredValue::Inline(bytes) = decode_value(&value).unwrap() else {
                panic!("blob compaction must materialize inline state");
            };
            BlobState::decode(bytes).unwrap()
        });
        (state, garbage)
    }

    #[test]
    fn mutation_codecs_round_trip() {
        let record_ref = record(7, 8);
        let put = BlobMutation::decode_put_metadata(
            &BlobMutation::encode_put_metadata(shard(1, 2), 3),
            record_ref,
        )
        .unwrap();
        assert_eq!(
            put,
            BlobMutation::Put {
                shard: shard(1, 2),
                write_epoch: 3,
                record_ref,
            }
        );
        assert!(put.encode_inline().is_err());

        let inline_mutations = [
            BlobMutation::SetLifetime {
                logical_end_epoch: 9,
                current_epoch: 4,
            },
            BlobMutation::Tombstone { shard: shard(5, 6) },
        ];
        for mutation in inline_mutations {
            assert_eq!(
                BlobMutationWithLSN::decode_inline(10, &mutation.encode_inline().unwrap()).unwrap(),
                vec![BlobMutationWithLSN { lsn: 10, mutation }]
            );
        }
    }

    #[test]
    fn materialized_state_codec_round_trips() {
        let state = BlobState {
            versions: BTreeMap::from([(
                shard(1, 2),
                BlobVersion {
                    lsn: 3,
                    write_epoch: 5,
                    record_ref: record(6, 7),
                },
            )]),
            lifetime: Some(BlobLifetime {
                lsn: 8,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: 10,
                    extension_count: 11,
                },
            }),
        };
        assert_eq!(BlobState::decode(&state.encode().unwrap()).unwrap(), state);
    }

    #[test]
    fn merge_keeps_one_version_per_shard_and_retires_overwrites() {
        let first = record(1, 10);
        let other = record(2, 20);
        let replacement = record(3, 30);
        let shard_a = shard(1, 1);
        let shard_b = shard(1, 2);
        let patches = [
            (1, put_patch(shard_a, 5, first)),
            (2, put_patch(shard_b, 5, other)),
            (3, put_patch(shard_a, 6, replacement)),
        ];

        let (state, garbage) = merge(&patches);

        assert_eq!(state.resolve(shard_a, 6).unwrap().0.record_ref, replacement);
        assert_eq!(state.resolve(shard_b, 6).unwrap().0.record_ref, other);
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].event, GarbageEvent::Retired { record: first });
        assert_eq!(garbage[0].lsn, 3);
    }

    #[test]
    fn tombstone_only_retires_its_shard() {
        let shard_a = shard(1, 1);
        let shard_b = shard(2, 1);
        let record_a = record(1, 10);
        let record_b = record(2, 20);
        let patches = [
            (1, put_patch(shard_a, 5, record_a)),
            (2, put_patch(shard_b, 5, record_b)),
            (3, inline_patch(BlobMutation::Tombstone { shard: shard_a })),
        ];

        let (state, garbage) = merge(&patches);

        assert!(state.resolve(shard_a, 5).is_none());
        assert_eq!(state.resolve(shard_b, 5).unwrap().0.record_ref, record_b);
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].event, GarbageEvent::Retired { record: record_a });
        assert_eq!(garbage[0].lsn, 3);
    }

    #[test]
    fn partial_merge_batches_operations_without_changing_full_merge() {
        let shard_a = shard(1, 1);
        let shard_b = shard(2, 1);
        let first_a = record(1, 10);
        let first_b = record(2, 20);
        let second_a = record(3, 30);
        let final_a = record(4, 40);
        let patches = [
            (1, put_patch(shard_a, 5, first_a)),
            (2, put_patch(shard_b, 5, first_b)),
            (
                3,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (4, put_patch(shard_a, 6, second_a)),
            (5, inline_patch(BlobMutation::Tombstone { shard: shard_b })),
            (
                6,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 20,
                    current_epoch: 10,
                }),
            ),
            (7, put_patch(shard_a, 10, final_a)),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut partial_garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                partial_garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();

        let direct = merge(&patches);
        let (from_batch_state, mut from_batch_garbage) = merge(&[(7, batched)]);
        from_batch_garbage.extend(partial_garbage);

        assert_eq!(from_batch_state, direct.0);
        assert_functionally_equivalent_garbage(&from_batch_garbage, &direct.1);
    }

    #[test]
    fn partial_merge_preserves_lifetimes_without_expiring_puts() {
        let shard = shard(1, 1);
        let record = record(2, 20);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
            (
                3,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 20,
                    current_epoch: 9,
                }),
            ),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut garbage = Vec::new();
        let batch = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();

        assert!(garbage.is_empty());
        let decoded = decode_patches(&[(3, batch.as_slice())]).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(
            decoded[0].mutation,
            BlobMutation::SetLifetime {
                logical_end_epoch: 10,
                current_epoch: 5,
            }
        );
        assert!(matches!(
            decoded[1].mutation,
            BlobMutation::Put { record_ref, .. } if record_ref == record
        ));
        assert_eq!(
            decoded[2].mutation,
            BlobMutation::SetLifetime {
                logical_end_epoch: 20,
                current_epoch: 9,
            }
        );
    }

    #[test]
    fn partial_merge_expires_patch_puts_before_and_after_a_lifetime() {
        let shard_a = shard(1, 1);
        let shard_b = shard(2, 1);
        let before = record(1, 10);
        let after = record(2, 20);
        let patches = [
            (1, put_patch(shard_a, 5, before)),
            (
                2,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (3, put_patch(shard_b, 5, after)),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 4,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (4, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            }]
        );
        for record in [before, after] {
            assert!(garbage.iter().any(|garbage| {
                garbage.lsn == 4 && garbage.event == GarbageEvent::Expired { record }
            }));
        }
    }

    #[test]
    fn partial_merge_starts_a_new_bucket_after_lifetime_expiry() {
        let shard = shard(1, 1);
        let expired = record(1, 10);
        let surviving = record(2, 20);
        let patches = [
            (1, put_patch(shard, 5, expired)),
            (
                2,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 8,
                    current_epoch: 5,
                }),
            ),
            (
                4,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 8,
                }),
            ),
            (5, put_patch(shard, 8, surviving)),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 5,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (3, 8)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(5, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 3);
        assert!(matches!(
            decoded[0],
            BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 8,
                    ..
                },
            }
        ));
        assert!(matches!(
            decoded[1],
            BlobMutationWithLSN {
                lsn: 4,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    ..
                },
            }
        ));
        assert!(matches!(
            decoded[2],
            BlobMutationWithLSN {
                lsn: 5,
                mutation: BlobMutation::Put { record_ref, .. },
            } if record_ref == surviving
        ));
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].lsn, 3);
        assert_eq!(garbage[0].event, GarbageEvent::Expired { record: expired });
    }

    #[test]
    fn partial_merge_epoch_expiry_drops_a_tombstone_bucket() {
        let shard = shard(1, 1);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 8,
                    current_epoch: 5,
                }),
            ),
            (2, inline_patch(BlobMutation::Tombstone { shard })),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 3,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (3, 8)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![BlobMutationWithLSN {
                lsn: 1,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 8,
                    current_epoch: 5,
                },
            }]
        );
        assert!(garbage.is_empty());
    }

    #[test]
    fn partial_merge_lifetime_survives_a_tombstone_for_a_future_put() {
        let shard = shard(1, 1);
        let first = record(1, 10);
        let second = record(2, 20);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, first)),
            (3, inline_patch(BlobMutation::Tombstone { shard })),
            (4, put_patch(shard, 5, second)),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 5,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (5, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(4, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![BlobMutationWithLSN {
                lsn: 1,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            }]
        );
        assert!(garbage.iter().any(|garbage| {
            garbage.lsn == 3 && garbage.event == GarbageEvent::Retired { record: first }
        }));
        assert!(garbage.iter().any(|garbage| {
            garbage.lsn == 5 && garbage.event == GarbageEvent::Expired { record: second }
        }));
    }

    #[test]
    fn partial_merge_later_lifetime_prevents_the_old_epoch_from_expiring_a_put() {
        let shard = shard(1, 1);
        let record = record(1, 10);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
            (
                3,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 20,
                    current_epoch: 9,
                }),
            ),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 4,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (4, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 3);
        assert!(matches!(
            decoded[1],
            BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::Put { record_ref, .. },
            } if record_ref == record
        ));
        assert!(garbage.is_empty());
    }

    #[test]
    fn partial_merge_does_not_expire_beyond_the_materialized_frontier() {
        let shard = shard(1, 1);
        let record = record(1, 10);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 4,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (5, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 2);
        assert!(matches!(
            decoded[1],
            BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::Put { record_ref, .. },
            } if record_ref == record
        ));
        assert!(garbage.is_empty());
    }

    #[test]
    fn partial_merge_does_not_expire_a_put_with_only_an_unknown_base_lifetime() {
        let shard = shard(1, 1);
        let record = record(1, 10);
        let patch = put_patch(shard, 5, record);
        let patches = [(2, patch.as_slice())];
        let merge = BlobMergeWithRelocations::new(
            None,
            LazyGlobalState {
                materialized_through_lsn: 5,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (5, 10)],
                ..LazyGlobalState::default()
            },
        );
        let mut garbage = Vec::new();

        let merged = merge
            .partial_merge(b"blob", &patches, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap();

        assert!(merged.is_none());
        assert!(garbage.is_empty());
    }

    #[test]
    fn partial_merge_silently_prunes_expiry_before_the_materialization_cutover() {
        let shard = shard(1, 1);
        let record = record(1, 10);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 3,
                materialize_from_lsn: 4,
                epoch_changes: vec![(0, 5), (3, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![BlobMutationWithLSN {
                lsn: 1,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            }]
        );
        assert!(garbage.is_empty());
    }

    #[test]
    fn partial_merge_expiry_precedes_a_later_tombstone() {
        let shard = shard(1, 1);
        let record = record(1, 10);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
            (4, inline_patch(BlobMutation::Tombstone { shard })),
        ];
        let (batched, garbage) = partial_merge_with_global(
            &patches,
            LazyGlobalState {
                materialized_through_lsn: 4,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (3, 10)],
                ..LazyGlobalState::default()
            },
        );
        let decoded = decode_patches(&[(4, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![
                BlobMutationWithLSN {
                    lsn: 1,
                    mutation: BlobMutation::SetLifetime {
                        logical_end_epoch: 10,
                        current_epoch: 5,
                    },
                },
                BlobMutationWithLSN {
                    lsn: 4,
                    mutation: BlobMutation::Tombstone { shard },
                },
            ]
        );
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].lsn, 3);
        assert_eq!(garbage[0].event, GarbageEvent::Expired { record });
    }

    #[test]
    fn partial_merge_reduces_shard_history_after_a_lifetime_barrier() {
        let shard = shard(1, 1);
        let record = record(2, 20);
        let patches = [
            (
                1,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (2, put_patch(shard, 5, record)),
            (3, inline_patch(BlobMutation::Tombstone { shard })),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut partial_garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                partial_garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[0],
            BlobMutationWithLSN {
                lsn: 1,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            }
        );
        assert_eq!(
            decoded[1],
            BlobMutationWithLSN {
                lsn: 3,
                mutation: BlobMutation::Tombstone { shard },
            }
        );
        assert_eq!(partial_garbage.len(), 1);
        assert_eq!(partial_garbage[0].lsn, 3);
        assert_eq!(partial_garbage[0].event, GarbageEvent::Retired { record });

        let direct = merge(&patches);
        let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
        from_batch_garbage.extend(partial_garbage);

        assert_eq!(from_batch_state, direct.0);
        assert_eq!(
            aggregate_garbage_deltas(&from_batch_garbage),
            aggregate_garbage_deltas(&direct.1)
        );
    }

    #[test]
    fn partial_merge_terminal_event_may_subsume_expiry_from_a_base_lifetime() {
        let shard = shard(1, 1);
        let base_record = record(1, 10);
        let patch_record = record(2, 20);
        let base_state = BlobState {
            versions: BTreeMap::from([(
                shard,
                BlobVersion {
                    lsn: 2,
                    write_epoch: 5,
                    record_ref: base_record,
                },
            )]),
            lifetime: Some(BlobLifetime {
                lsn: 3,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: 10,
                    extension_count: 0,
                },
            }),
        };
        let patches = [
            (4, put_patch(shard, 5, patch_record)),
            (6, inline_patch(BlobMutation::Tombstone { shard })),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let decoded = decode_patches(&[(6, batched.as_slice())]).unwrap();

        assert_eq!(
            decoded,
            vec![BlobMutationWithLSN {
                lsn: 6,
                mutation: BlobMutation::Tombstone { shard },
            }]
        );

        // Epoch 10 is reached between the patch-local put and tombstone. The partial merger
        // cannot see either this transition or the lifetime in the base, so the tombstone
        // terminally accounts for the patch record as Retired rather than Expired.
        assert_eq!(
            garbage
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        GarbageEvent::Retired { record } | GarbageEvent::Expired { record }
                            if record == patch_record
                    )
                })
                .count(),
            1
        );

        let base = encode_inline_value(&base_state.encode().unwrap());
        let patches = [(6, batched.as_slice())];
        let merge = BlobMergeWithRelocations::new(
            None,
            LazyGlobalState {
                materialized_through_lsn: 6,
                materialize_from_lsn: 0,
                epoch_changes: vec![(0, 5), (5, 10)],
                ..LazyGlobalState::default()
            },
        );
        let encoded = merge
            .merge(b"blob", Some(&base), &patches, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
            panic!("blob compaction must materialize inline state");
        };
        let state = BlobState::decode(bytes).unwrap();

        assert!(state.resolve(shard, 10).is_none());
        assert_eq!(
            garbage
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        GarbageEvent::Retired { record } | GarbageEvent::Expired { record }
                            if record == patch_record
                    )
                })
                .count(),
            1
        );
    }

    #[test]
    fn partial_merge_reduces_terminal_puts_across_a_retained_lifetime() {
        let shard = shard(1, 1);
        let record = record(2, 20);
        let patches = [
            (1, put_patch(shard, 5, record)),
            (
                2,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (3, inline_patch(BlobMutation::Tombstone { shard })),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut partial_garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                partial_garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[0],
            BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                },
            }
        );
        assert_eq!(
            decoded[1],
            BlobMutationWithLSN {
                lsn: 3,
                mutation: BlobMutation::Tombstone { shard },
            }
        );
        assert_eq!(partial_garbage.len(), 1);
        assert_eq!(partial_garbage[0].lsn, 3);
        assert_eq!(partial_garbage[0].event, GarbageEvent::Retired { record });

        let direct = merge(&patches);
        let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
        from_batch_garbage.extend(partial_garbage);

        assert_eq!(from_batch_state, direct.0);
        assert_eq!(
            aggregate_garbage_deltas(&from_batch_garbage),
            aggregate_garbage_deltas(&direct.1)
        );
    }

    #[test]
    fn partial_merge_reduces_shard_history_and_emits_terminal_garbage() {
        let shard = shard(1, 1);
        let first = record(1, 10);
        let second = record(2, 20);
        let third = record(3, 30);
        let patches = [
            (1, put_patch(shard, 5, first)),
            (2, put_patch(shard, 6, second)),
            (3, put_patch(shard, 7, third)),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut partial_garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                partial_garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let decoded = decode_patches(&[(3, batched.as_slice())]).unwrap();

        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0],
            BlobMutationWithLSN {
                lsn: 3,
                mutation: BlobMutation::Put { record_ref, .. },
            } if record_ref == third
        ));

        let direct = merge(&patches);
        let (from_batch_state, mut from_batch_garbage) = merge(&[(3, batched)]);
        from_batch_garbage.extend(partial_garbage);

        assert_eq!(from_batch_state, direct.0);
        assert_eq!(from_batch_garbage, direct.1);
    }

    #[test]
    fn partial_merge_final_put_retires_the_unknown_base_without_a_synthetic_tombstone() {
        let shard = shard(1, 1);
        let base_record = record(1, 10);
        let first_patch_record = record(2, 20);
        let final_patch_record = record(3, 30);
        let base_state = BlobState {
            versions: BTreeMap::from([(
                shard,
                BlobVersion {
                    lsn: 0,
                    write_epoch: 5,
                    record_ref: base_record,
                },
            )]),
            lifetime: None,
        };
        let patches = [
            (1, put_patch(shard, 5, first_patch_record)),
            (2, put_patch(shard, 5, final_patch_record)),
        ];
        let borrowed = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut garbage = Vec::new();
        let batched = BlobMerge
            .partial_merge(b"blob", &borrowed, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let decoded = decode_patches(&[(2, batched.as_slice())]).unwrap();

        assert!(matches!(
            decoded.as_slice(),
            [BlobMutationWithLSN {
                lsn: 2,
                mutation: BlobMutation::Put { record_ref, .. },
            }] if *record_ref == final_patch_record
        ));

        let base = encode_inline_value(&base_state.encode().unwrap());
        let patches = [(2, batched.as_slice())];
        let encoded = BlobMerge
            .merge(b"blob", Some(&base), &patches, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();
        let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
            panic!("blob merge must materialize inline state");
        };
        let state = BlobState::decode(bytes).unwrap();

        let version = state.resolve(shard, 5).unwrap().0;
        assert_eq!(version.lsn, 2);
        assert_eq!(version.record_ref, final_patch_record);
        assert!(garbage.iter().any(|record| {
            record.lsn == 2
                && record.event
                    == GarbageEvent::Retired {
                        record: first_patch_record,
                    }
        }));
        assert!(garbage.iter().any(|record| {
            record.lsn == 2
                && record.event
                    == GarbageEvent::Retired {
                        record: base_record,
                    }
        }));
    }

    #[test]
    fn lifetime_expiry_and_tombstone_match_store_visibility() {
        let shard = shard(1, 1);
        let first = record(1, 10);
        let after_expiry = record(2, 20);
        let patches = [
            (1, put_patch(shard, 5, first)),
            (
                2,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 10,
                    current_epoch: 5,
                }),
            ),
            (
                3,
                inline_patch(BlobMutation::SetLifetime {
                    logical_end_epoch: 20,
                    current_epoch: 10,
                }),
            ),
            (4, put_patch(shard, 10, after_expiry)),
        ];
        let (state, garbage) = merge(&patches);

        let (version, lifecycle) = state.resolve(shard, 10).unwrap();
        assert_eq!(version.record_ref, after_expiry);
        assert_eq!(lifecycle.unwrap().logical_end_epoch, 20);
        assert!(
            garbage
                .iter()
                .any(|record| record.event == GarbageEvent::Expired { record: first })
        );
        assert!(garbage.iter().any(|record| {
            record.event
                == GarbageEvent::SetLifecycle {
                    record: after_expiry,
                    lifecycle: Some(BlobLifecycle {
                        logical_end_epoch: 20,
                        extension_count: 0,
                    }),
                }
        }));

        let tombstone = (5, inline_patch(BlobMutation::Tombstone { shard }));
        let mut all = patches.to_vec();
        all.push(tombstone);
        let (state, garbage) = merge(&all);
        assert!(state.resolve(shard, 10).is_none());
        assert!(garbage.iter().any(|record| record.event
            == GarbageEvent::Retired {
                record: after_expiry
            }));
    }

    #[test]
    fn lazy_compaction_waits_for_the_materialized_epoch_frontier() {
        let shard = shard(1, 1);
        let record = record(7, 10);
        let state = BlobState {
            versions: BTreeMap::from([(
                shard,
                BlobVersion {
                    lsn: 2,
                    write_epoch: 5,
                    record_ref: record,
                },
            )]),
            lifetime: Some(BlobLifetime {
                lsn: 1,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: 10,
                    extension_count: 0,
                },
            }),
        };
        let epoch_changes = vec![(0, 5), (4, 10)];

        // LSN 3 may still contain a pre-expiry extension in a memtable. The index knows that
        // epoch 10 was reached at LSN 4, but an SST-only compaction is complete only through LSN 2.
        let (before_extension, garbage) = compact_state(
            state.clone(),
            LazyGlobalState {
                materialized_through_lsn: 2,
                materialize_from_lsn: 0,
                epoch_changes: epoch_changes.clone(),
                ..LazyGlobalState::default()
            },
        );
        assert_eq!(
            before_extension.unwrap().versions[&shard].record_ref,
            record
        );
        assert!(garbage.is_empty());

        let encoded_base = encode_inline_value(&state.encode().unwrap());
        let extension = inline_patch(BlobMutation::SetLifetime {
            logical_end_epoch: 20,
            current_epoch: 9,
        });
        let merge = BlobMergeWithRelocations::new(
            None,
            LazyGlobalState {
                materialized_through_lsn: 4,
                materialize_from_lsn: 0,
                epoch_changes,
                ..LazyGlobalState::default()
            },
        );
        let mut garbage = Vec::new();
        let merged = merge
            .merge(
                b"blob",
                Some(&encoded_base),
                &[(3, extension.as_slice())],
                &mut |record| {
                    garbage.push(record);
                    Ok(())
                },
            )
            .unwrap()
            .unwrap();
        let StoredValue::Inline(bytes) = decode_value(&merged).unwrap() else {
            panic!("full merge must materialize inline state");
        };
        let state = BlobState::decode(bytes).unwrap();

        assert_eq!(state.resolve(shard, 10).unwrap().0.record_ref, record);
        assert_eq!(
            state
                .resolve(shard, 10)
                .unwrap()
                .1
                .unwrap()
                .logical_end_epoch,
            20
        );
        assert!(
            garbage
                .iter()
                .all(|record| !matches!(record.event, GarbageEvent::Expired { .. }))
        );
    }

    #[test]
    fn lazy_compaction_expires_versions_at_the_first_reaching_epoch() {
        let shard = shard(1, 1);
        let record = record(7, 10);
        let state = BlobState {
            versions: BTreeMap::from([(
                shard,
                BlobVersion {
                    lsn: 3,
                    write_epoch: 5,
                    record_ref: record,
                },
            )]),
            lifetime: Some(BlobLifetime {
                lsn: 4,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: 10,
                    extension_count: 0,
                },
            }),
        };
        let global = LazyGlobalState {
            materialized_through_lsn: 20,
            materialize_from_lsn: 8,
            epoch_changes: vec![(0, 5), (9, 8), (12, 10), (18, 11)],
            shard_infos: BTreeMap::from([(1, ShardInfo::active(1))]),
            ..LazyGlobalState::default()
        };

        let (state, garbage) = compact_state(state, global);

        assert!(state.unwrap().versions.is_empty());
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].lsn, 12);
        assert_eq!(garbage[0].event, GarbageEvent::Expired { record });
    }

    #[test]
    fn lazy_compaction_silently_prunes_legacy_global_expiry() {
        let shard = shard(1, 1);
        let record = record(7, 10);
        let state = BlobState {
            versions: BTreeMap::from([(
                shard,
                BlobVersion {
                    lsn: 3,
                    write_epoch: 5,
                    record_ref: record,
                },
            )]),
            lifetime: Some(BlobLifetime {
                lsn: 4,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: 10,
                    extension_count: 0,
                },
            }),
        };
        let global = LazyGlobalState {
            materialized_through_lsn: 20,
            materialize_from_lsn: 13,
            epoch_changes: vec![(0, 5), (12, 10)],
            shard_infos: BTreeMap::from([(1, ShardInfo::active(1))]),
            ..LazyGlobalState::default()
        };

        let (state, garbage) = compact_state(state, global);

        assert!(state.unwrap().versions.is_empty());
        assert!(garbage.is_empty());
    }

    #[test]
    fn lazy_compaction_retires_mixed_refs_but_silently_prunes_bulk_reclaimed_refs() {
        let dropped = shard(1, 4);
        let mixed = record(7, 10);
        let owned = record(8, 20);
        let state = BlobState {
            versions: BTreeMap::from([
                (
                    dropped,
                    BlobVersion {
                        lsn: 3,
                        write_epoch: 5,
                        record_ref: mixed,
                    },
                ),
                (
                    shard(2, 4),
                    BlobVersion {
                        lsn: 4,
                        write_epoch: 5,
                        record_ref: owned,
                    },
                ),
            ]),
            lifetime: None,
        };
        let second_dropped = shard(2, 4);
        let global = LazyGlobalState {
            materialized_through_lsn: 20,
            materialize_from_lsn: 8,
            shard_infos: BTreeMap::from([
                (
                    1,
                    ShardInfo {
                        current_generation: 4,
                        state: ShardState::Dropped,
                    },
                ),
                (
                    2,
                    ShardInfo {
                        current_generation: 4,
                        state: ShardState::Dropped,
                    },
                ),
            ]),
            shard_drop_lsns: BTreeMap::from([(dropped, 11), (second_dropped, 12)]),
            reclaimed_shard_segments: BTreeMap::from([(owned.segment_id, second_dropped)]),
            ..LazyGlobalState::default()
        };

        let (state, garbage) = compact_state(state, global);

        assert!(state.is_none());
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].lsn, 11);
        assert_eq!(garbage[0].event, GarbageEvent::Retired { record: mixed });
    }

    #[test]
    fn lazy_compaction_silently_prunes_shards_owned_by_the_pre_cutover_path() {
        let pre_cutover_drop = shard(1, 4);
        let legacy_without_job = shard(2, 3);
        let state = BlobState {
            versions: BTreeMap::from([
                (
                    pre_cutover_drop,
                    BlobVersion {
                        lsn: 3,
                        write_epoch: 5,
                        record_ref: record(7, 10),
                    },
                ),
                (
                    legacy_without_job,
                    BlobVersion {
                        lsn: 4,
                        write_epoch: 5,
                        record_ref: record(8, 20),
                    },
                ),
            ]),
            lifetime: None,
        };
        let global = LazyGlobalState {
            materialized_through_lsn: 20,
            materialize_from_lsn: 12,
            shard_infos: BTreeMap::from([
                (
                    1,
                    ShardInfo {
                        current_generation: 4,
                        state: ShardState::Dropped,
                    },
                ),
                (2, ShardInfo::active(4)),
            ]),
            shard_drop_lsns: BTreeMap::from([(pre_cutover_drop, 11)]),
            ..LazyGlobalState::default()
        };

        let (state, garbage) = compact_state(state, global);

        assert!(state.is_none());
        assert!(garbage.is_empty());
    }
}
