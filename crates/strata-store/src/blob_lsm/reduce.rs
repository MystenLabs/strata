//! Patch reduction for partial merges that cannot see the materialized base.

use std::collections::BTreeMap;

use strata_core::{BlobLifecycle, Epoch, GarbageEvent, ShardKey};
use strata_lsm::{GarbageRecord, Result, StrataLsn};

use super::format::{BlobMutation, BlobMutationWithLSN, BlobVersion};
use super::garbage::{emit_lifetime_change, terminal_garbage_record};
use super::snapshot::BlobCompactionSnapshot;
use crate::relocation::RelocationEntry;

#[derive(Debug, Clone, Copy)]
struct PatchLifecycleHint {
    lsn: StrataLsn,
    lifecycle: BlobLifecycle,
}

#[derive(Debug, Clone, Copy)]
struct PatchBucket {
    mutation_index: usize,
    // A bucket always holds the last mutation for its shard. `None` is a tombstone.
    put: Option<BlobVersion>,
    // `None` means the patch does not prove a lifetime for this bucket.
    logical_end_epoch: Option<Epoch>,
    // A patch-local lifetime established before this Put and unchanged afterwards. Partial
    // compaction can publish this as an early GC routing hint without consulting the base.
    lifecycle_hint: Option<PatchLifecycleHint>,
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
    snapshot: Option<&'a BlobCompactionSnapshot>,
) -> impl Iterator<Item = PatchEvent> + 'a {
    let (epoch_changes, emit_garbage_from_lsn) = match snapshot {
        Some(snapshot) => {
            let end = snapshot
                .epoch_changes
                .partition_point(|(lsn, _)| *lsn <= snapshot.materialized_through_lsn);
            (
                &snapshot.epoch_changes[..end],
                snapshot.emit_garbage_from_lsn,
            )
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
                emit_garbage: lsn >= emit_garbage_from_lsn,
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
pub(crate) fn reduce_patch_mutations(
    key: &[u8],
    mutations: Vec<BlobMutationWithLSN>,
    snapshot: Option<&BlobCompactionSnapshot>,
    relocations: &[RelocationEntry],
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<Vec<BlobMutationWithLSN>> {
    let mut buckets = BTreeMap::<ShardKey, PatchBucket>::new();
    let mut current_lifetime = None::<PatchLifecycleHint>;
    for event in merge_patch_events(&mutations, snapshot) {
        let mutation_index = match event {
            PatchEvent::Mutation(mutation_index) => mutation_index,
            PatchEvent::ChangeEpoch {
                lsn,
                epoch,
                emit_garbage,
            } => {
                change_patch_epoch(
                    key,
                    lsn,
                    epoch,
                    emit_garbage,
                    &mut buckets,
                    relocations,
                    emit,
                )?;
                if current_lifetime.is_some_and(|hint| hint.lifecycle.logical_end_epoch <= epoch) {
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
                let lifecycle_hint =
                    current_lifetime.filter(|hint| hint.lifecycle.logical_end_epoch > write_epoch);
                replace_patch_bucket(
                    key,
                    shard,
                    PatchBucket {
                        mutation_index,
                        put: Some(version),
                        logical_end_epoch: lifecycle_hint
                            .map(|hint| hint.lifecycle.logical_end_epoch),
                        lifecycle_hint,
                    },
                    lsn,
                    &mut buckets,
                    relocations,
                    emit,
                )?;
            }
            BlobMutation::SetLifetime {
                logical_end_epoch,
                current_epoch,
            } => {
                let extension_count = current_lifetime
                    .filter(|hint| hint.lifecycle.logical_end_epoch > current_epoch)
                    .map_or(0, |hint| hint.lifecycle.extension_count.saturating_add(1));
                current_lifetime = Some(PatchLifecycleHint {
                    lsn,
                    lifecycle: BlobLifecycle {
                        logical_end_epoch,
                        extension_count,
                    },
                });
                for bucket in buckets.values_mut() {
                    bucket.logical_end_epoch = Some(logical_end_epoch);
                    // A lifetime changed after this Put. Full compaction must resolve its exact
                    // extension count before GC receives the new routing hint.
                    bucket.lifecycle_hint = None;
                }
            }
            BlobMutation::Tombstone { shard } => {
                replace_patch_bucket(
                    key,
                    shard,
                    PatchBucket {
                        mutation_index,
                        put: None,
                        logical_end_epoch: current_lifetime
                            .map(|hint| hint.lifecycle.logical_end_epoch),
                        lifecycle_hint: None,
                    },
                    lsn,
                    &mut buckets,
                    relocations,
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
    for (&shard, bucket) in &buckets {
        keep[bucket.mutation_index] = true;
        if let (Some(version), Some(hint)) = (bucket.put, bucket.lifecycle_hint) {
            // The hint LSN deliberately precedes the Put. Both mutations are durable before this
            // partial compaction can publish, while the distinct position lets a later full merge
            // restate or correct an extension count at the Put LSN without creating a conflicting
            // garbage-log event.
            let (version, event_lsn) =
                resolve_event_version(key, shard, version, hint.lsn, relocations);
            emit_lifetime_change(key, event_lsn, version, None, Some(hint.lifecycle), emit)?;
        }
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
    relocations: &[RelocationEntry],
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    if let Some(previous) = buckets.insert(shard, next)
        && let Some(version) = previous.put
    {
        let (version, event_lsn) = resolve_event_version(key, shard, version, lsn, relocations);
        emit(terminal_garbage_record(
            key,
            event_lsn,
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
    relocations: &[RelocationEntry],
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
            let (version, event_lsn) =
                resolve_event_version(key, shard, version, transition_lsn, relocations);
            emit(terminal_garbage_record(
                key,
                event_lsn,
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

fn resolve_event_version(
    key: &[u8],
    shard: ShardKey,
    mut version: BlobVersion,
    mut event_lsn: StrataLsn,
    relocations: &[RelocationEntry],
) -> (BlobVersion, StrataLsn) {
    // Partial reduction may remove the last main-LSM evidence for this payload. Redirect its
    // physical event before that happens so a relocated destination cannot remain live forever.
    if let Some(relocation) = relocations.iter().find(|relocation| {
        relocation.key.as_bytes() == key
            && relocation.shard == shard
            && relocation.payload_lsn == version.lsn
    }) {
        version.record_ref = relocation.to;
        event_lsn = event_lsn.max(relocation.publish_lsn);
    }
    (version, event_lsn)
}
