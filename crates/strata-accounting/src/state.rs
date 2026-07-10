use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use strata_core::{BlobKey, BlobLifecycle, Epoch, RecordRef, SegmentOwner, ShardKey, StrataLsn};

use crate::events::{CompactionEventBatch, RefEvent, RetireReason};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobUpdate {
    Put {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        current_epoch: Epoch,
        lifecycle: Option<BlobLifecycle>,
    },
    Tombstone {
        lsn: StrataLsn,
        key: BlobKey,
    },
    SetLifetime {
        lsn: StrataLsn,
        key: BlobKey,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    MapRef {
        lsn: StrataLsn,
        key: BlobKey,
        from: RecordRef,
        to: RecordRef,
    },
}

impl BlobUpdate {
    pub fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::SetLifetime { lsn, .. }
            | Self::MapRef { lsn, .. } => *lsn,
        }
    }

    pub fn key(&self) -> &BlobKey {
        match self {
            Self::Put { key, .. }
            | Self::Tombstone { key, .. }
            | Self::SetLifetime { key, .. }
            | Self::MapRef { key, .. } => key,
        }
    }
}

/// Physical payload selected by the folded state for one blob key.
///
/// This is not the logical blob value; it is the storage address whose segment bytes are currently
/// protected by the accounting index. It is born when a `Put` survives into the folded state, is
/// rewritten by `MapRef`, and is retired when a later put or tombstone removes this physical range
/// from the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivePayload {
    /// The payload's own write LSN is kept separately from `head_lsn` because later metadata-only
    /// operations can advance the folded state without changing which physical record range is live.
    pub payload_lsn: StrataLsn,
    /// Shard identity travels with the live physical ref so a future `Live` event can be reconstructed
    /// from folded state without consulting the original delta log.
    pub shard: ShardKey,
    /// Physical ownership determines whether shard drop needs a range retirement or can rely on
    /// whole-directory deletion.
    pub owner: SegmentOwner,
    /// This is the byte range that segment accounting and the GC overlay protect. The reducer treats
    /// changes to this field as physical storage transitions, not merely logical metadata updates.
    pub record_ref: RecordRef,
}

/// Deferred or applied physical rewrite for a live payload range.
///
/// `MapRef` models storage relocation: the logical key is unchanged, but GC must retire one segment
/// range and optionally install lifecycle metadata on another. It can be born before the source
/// payload is visible in the current fold, so the state keeps unresolved maps until the matching
/// physical ref appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapRef {
    /// The rewrite's LSN determines the retire/live ordering used for event publication once the
    /// source range is found.
    pub lsn: StrataLsn,
    /// Source range that must still be protected until the rewrite is materialized.
    pub from: RecordRef,
    /// Replacement range that inherits the key's lifecycle when the rewrite is applied.
    pub to: RecordRef,
}

/// Logical delete marker that shadows older physical payload state.
///
/// A tombstone is durable state, not an immediate absence. It is born when a delete is folded and can
/// outlive the compaction level that first saw it so that a later major compaction can retire an
/// older base payload at the correct LSN. It is cleared only by a later `Put`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The delete LSN is the point at which an older physical record becomes tombstoned for GC
    /// accounting, even if that older record is discovered in a later compaction layer.
    pub lsn: StrataLsn,
}

/// Key-level lifecycle metadata carried across payload changes.
///
/// Lifecycle is stored independently from `LivePayload` because lifetime extensions can arrive
/// before a payload exists, after a payload exists, or between physical rewrites. The value is born
/// from `Put` lifecycle metadata or `SetLifetime`, follows future puts/maps, and can be cleared by a
/// lifecycle change that removes overlay metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleChange {
    /// This LSN lets metadata-only changes advance the folded state without pretending a new payload
    /// record was written.
    pub lsn: StrataLsn,
    /// `None` is meaningful: it represents clearing lifecycle overlay state for the current physical
    /// range rather than the absence of a recorded change.
    pub new_lifecycle: Option<BlobLifecycle>,
}

/// Durable per-key image stored in base runs and reused as the reducer's transient state.
///
/// This struct is the point where logical blob history becomes physical storage truth. In a base run,
/// one `MaterializedBlobState` row represents all updates for a key that have been absorbed by major
/// compaction. During delta and patch folding, the same shape is used in memory so event generation
/// and persisted state follow one state machine. A persisted row is born when major compaction writes
/// a base run, is superseded by the next base run for the partition, and may contain no live payload
/// when the important durable fact is a tombstone, lifecycle marker, or unresolved map.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MaterializedBlobState {
    /// The folded state needs a causality watermark even when the latest update did not create a live
    /// payload. This is what lets base-run metadata advertise how far compaction has materialized a
    /// key whose last operation may have been a tombstone, lifecycle change, or pending map.
    pub head_lsn: StrataLsn,
    /// One live physical range per shard generation. The foreground index permits the same blob key
    /// to be live in multiple shards, so accounting must protect those ranges independently.
    /// An empty map does not make the row disposable because tombstones and pending maps can still
    /// have future physical effects.
    pub payloads: BTreeMap<ShardKey, LivePayload>,
    /// Lifecycle is key-level intent carried until there is a physical range to annotate. Keeping it
    /// beside, rather than inside, the payload lets a later put inherit the lifetime and lets a map
    /// transfer that lifetime to the replacement segment range.
    pub lifecycle: Option<LifecycleChange>,
    /// This marker preserves a delete across LSM levels. It lets shallow compaction record "an older
    /// payload must die at this LSN" without seeing that older payload yet, which is why tombstoned
    /// rows may still need to be materialized into the next base run.
    pub tombstone: Option<Tombstone>,
    /// Pending maps are delayed physical rewrites. They are kept in the state row because the source
    /// ref may live in an older base row or may be introduced by a later patch; dropping them would
    /// leak or prematurely retire segment ranges.
    pub pending_maps: Vec<MapRef>,
}

impl MaterializedBlobState {
    pub fn is_live(&self) -> bool {
        !self.payloads.is_empty()
    }

    pub fn lifecycle_value(&self) -> Option<BlobLifecycle> {
        self.lifecycle.and_then(|change| change.new_lifecycle)
    }
}

/// Keyed state row stored inside base runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StateRecord {
    pub(crate) key: BlobKey,
    pub(crate) state: MaterializedBlobState,
}

/// Keyed update summary stored inside patch runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PatchRecord {
    pub(crate) key: BlobKey,
    pub(crate) updates: Vec<PatchUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PatchUpdate {
    Put {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        current_epoch: Epoch,
        lifecycle: Option<BlobLifecycle>,
    },
    Tombstone {
        lsn: StrataLsn,
        key: BlobKey,
    },
    SetLifetime {
        lsn: StrataLsn,
        key: BlobKey,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    MapRef {
        lsn: StrataLsn,
        key: BlobKey,
        from: RecordRef,
        to: RecordRef,
    },
}

impl PatchUpdate {
    pub(crate) fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::SetLifetime { lsn, .. }
            | Self::MapRef { lsn, .. } => *lsn,
        }
    }
}

impl From<BlobUpdate> for PatchUpdate {
    fn from(update: BlobUpdate) -> Self {
        match update {
            BlobUpdate::Put {
                lsn,
                key,
                shard,
                record_ref,
                current_epoch,
                lifecycle,
            } => Self::Put {
                lsn,
                key,
                shard,
                record_ref,
                current_epoch,
                lifecycle,
            },
            BlobUpdate::Tombstone { lsn, key } => Self::Tombstone { lsn, key },
            BlobUpdate::SetLifetime {
                lsn,
                key,
                logical_end_epoch,
                current_epoch,
            } => Self::SetLifetime {
                lsn,
                key,
                logical_end_epoch,
                current_epoch,
            },
            BlobUpdate::MapRef { lsn, key, from, to } => Self::MapRef { lsn, key, from, to },
        }
    }
}

pub(crate) trait RecordLsn {
    fn record_lsn(&self) -> StrataLsn;
}

impl RecordLsn for StateRecord {
    fn record_lsn(&self) -> StrataLsn {
        self.state.head_lsn
    }
}

impl RecordLsn for PatchRecord {
    fn record_lsn(&self) -> StrataLsn {
        self.updates
            .iter()
            .map(PatchUpdate::lsn)
            .max()
            .unwrap_or_default()
    }
}

impl RecordLsn for PatchUpdate {
    fn record_lsn(&self) -> StrataLsn {
        self.lsn()
    }
}

impl RecordLsn for BlobUpdate {
    fn record_lsn(&self) -> StrataLsn {
        self.lsn()
    }
}

impl<T> RecordLsn for &T
where
    T: RecordLsn + ?Sized,
{
    fn record_lsn(&self) -> StrataLsn {
        (*self).record_lsn()
    }
}

pub(crate) fn compact_delta_updates(
    key: &BlobKey,
    updates: &[BlobUpdate],
    batch: &mut CompactionEventBatch,
) -> Vec<PatchUpdate> {
    // Input updates are already sorted for one key. This pass decides how much history can be
    // safely collapsed while looking only at those updates. The dividing line is physical: if a
    // payload is born and retired entirely inside this delta group, its bytes can be accounted and
    // tombstoned now. Anything that might touch an older base/patch payload must remain as residual
    // patch history so major compaction can fold it with the missing state.
    if updates
        .iter()
        .any(|update| matches!(update, BlobUpdate::MapRef { .. }))
    {
        // MapRef is the sharp edge for shallow compaction. Its `from` ref may be in an older base
        // row, or the MapRef may precede the Put that will later materialize that ref. Emitting or
        // discarding anything around it without the complete folded state can create the wrong
        // retire/lifetime overlay for the physical segment range, so the entire key group stays raw.
        return raw_patch_updates(updates);
    }

    let put_shards = updates
        .iter()
        .filter_map(|update| match update {
            BlobUpdate::Put { shard, .. } => Some(*shard),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if put_shards.len() > 1 {
        // A Put supersedes only the same shard's payload. Without the older folded state, shallow
        // compaction cannot determine which shard lifetimes are locally closed, so preserve the
        // complete group for major compaction.
        return raw_patch_updates(updates);
    }

    let Some(first_put_index) = updates
        .iter()
        .position(|update| matches!(update, BlobUpdate::Put { .. }))
    else {
        // Metadata-only groups can only describe older state that this shallow pass has not read:
        // a tombstone may need to kill a base payload, and a lifetime change may need to annotate
        // that payload. Keep the records raw so major compaction can apply them with the base row.
        return raw_patch_updates(updates);
    };
    let last_terminal_index = updates
        .iter()
        .rposition(|update| {
            matches!(
                update,
                BlobUpdate::Put { .. } | BlobUpdate::Tombstone { .. }
            )
        })
        .expect("first Put is also a terminal update");

    if first_put_index == last_terminal_index {
        // There is no locally closed payload lifetime to account. The one terminal update must stay
        // residual because it may still overwrite or tombstone an older payload in the base/patch
        // stack.
        return raw_patch_updates(updates);
    }

    // Final-state-only policy: fold the middle window to emit events for transient refs, then keep
    // only the prefix that predates the first local payload and the final terminal update. If that
    // terminal update is a Tombstone, it must still retire an older base payload later. If it is a
    // Put, it must later overwrite the older payload and become the materialized live ref.
    let close_reason = match updates[last_terminal_index] {
        BlobUpdate::Put { .. } => RetireReason::Overwritten,
        BlobUpdate::Tombstone { .. } => RetireReason::Tombstoned,
        BlobUpdate::SetLifetime { .. } | BlobUpdate::MapRef { .. } => {
            unreachable!("last terminal update must be Put or Tombstone")
        }
    };
    emit_closed_delta_events(
        key,
        &updates[first_put_index..last_terminal_index],
        updates[last_terminal_index].lsn(),
        close_reason,
        batch,
    );
    let mut residual = raw_patch_updates(&updates[..first_put_index]);
    residual.extend(raw_patch_updates(&updates[last_terminal_index..]));
    residual
}

fn raw_patch_updates(updates: &[BlobUpdate]) -> Vec<PatchUpdate> {
    updates.iter().cloned().map(PatchUpdate::from).collect()
}

fn emit_closed_delta_events(
    key: &BlobKey,
    updates: &[BlobUpdate],
    close_lsn: StrataLsn,
    close_reason: RetireReason,
    batch: &mut CompactionEventBatch,
) {
    // This folds a self-contained lifetime that will not survive into the patch. The synthetic
    // closing retire uses the final terminal update's LSN even though that terminal update remains
    // residual; this keeps physical byte accounting aligned with the point where the transient ref
    // stopped being live.
    let mut state = None;
    for update in updates {
        fold_update(&mut state, update, &mut |event| {
            batch.record_event(event);
        });
    }

    if let Some(current) = state.as_mut() {
        let lifecycle = current.lifecycle_value();
        for payload in std::mem::take(&mut current.payloads).into_values() {
            batch.record_event(RefEvent::Retired {
                lsn: close_lsn,
                key: key.clone(),
                shard: payload.shard,
                record_ref: payload.record_ref,
                lifecycle,
                reason: close_reason,
            });
        }
    }
}

pub(crate) fn fold_patch_update(
    state: &mut Option<MaterializedBlobState>,
    update: &PatchUpdate,
    emit: &mut impl FnMut(RefEvent),
) {
    // Patch updates are stored separately from delta updates because they are durable summaries, but
    // the state machine is identical. Converting at the boundary keeps all ref-event accounting in
    // one fold implementation, which is important because GC summary and overlay operands must stay
    // in lockstep for each logical transition.
    match update {
        PatchUpdate::Put {
            lsn,
            key,
            shard,
            record_ref,
            current_epoch,
            lifecycle,
        } => fold_update(
            state,
            &BlobUpdate::Put {
                lsn: *lsn,
                key: key.clone(),
                shard: *shard,
                record_ref: *record_ref,
                current_epoch: *current_epoch,
                lifecycle: *lifecycle,
            },
            emit,
        ),
        PatchUpdate::Tombstone { lsn, key } => fold_update(
            state,
            &BlobUpdate::Tombstone {
                lsn: *lsn,
                key: key.clone(),
            },
            emit,
        ),
        PatchUpdate::SetLifetime {
            lsn,
            key,
            logical_end_epoch,
            current_epoch,
        } => fold_update(
            state,
            &BlobUpdate::SetLifetime {
                lsn: *lsn,
                key: key.clone(),
                logical_end_epoch: *logical_end_epoch,
                current_epoch: *current_epoch,
            },
            emit,
        ),
        PatchUpdate::MapRef { lsn, key, from, to } => fold_update(
            state,
            &BlobUpdate::MapRef {
                lsn: *lsn,
                key: key.clone(),
                from: *from,
                to: *to,
            },
            emit,
        ),
    }
}

pub(crate) fn fold_update(
    state: &mut Option<MaterializedBlobState>,
    update: &BlobUpdate,
    emit: &mut impl FnMut(RefEvent),
) {
    // This is the single-key reducer used by current-state reads, delta compaction, and major
    // compaction. Callers choose whether emitted events are ignored or published; the state
    // transitions are kept identical so a ref is not counted differently depending on which
    // compaction level first sees it.
    let key = update.key().clone();
    match update {
        BlobUpdate::Put {
            lsn,
            shard,
            record_ref,
            current_epoch,
            lifecycle,
            ..
        } => {
            let current = state.get_or_insert_with(MaterializedBlobState::default);
            let requested_lifecycle = *lifecycle;
            let previous_lifecycle = current.lifecycle_value();
            let inherited_lifecycle = requested_lifecycle.or_else(|| {
                previous_lifecycle.filter(|lifecycle| lifecycle.logical_end_epoch > *current_epoch)
            });
            // A Put replaces the live physical record for this shard. Other shards may carry the
            // same logical key and remain independently live.
            if let Some(payload) = current.payloads.remove(shard) {
                emit(RefEvent::Retired {
                    lsn: *lsn,
                    key: key.clone(),
                    shard: payload.shard,
                    record_ref: payload.record_ref,
                    lifecycle: current.lifecycle_value(),
                    reason: RetireReason::Overwritten,
                });
            }
            current.head_lsn = *lsn;
            current.payloads.insert(
                *shard,
                LivePayload {
                    payload_lsn: *lsn,
                    shard: *shard,
                    owner: SegmentOwner::Store,
                    record_ref: *record_ref,
                },
            );
            // A Put without explicit lifecycle inherits any prior key-level lifecycle. If there was
            // no prior active lifecycle, record the absence so the base row still reflects that this
            // payload was observed and does not need a later inferred default.
            if requested_lifecycle.is_some()
                || current.lifecycle.is_none()
                || current.lifecycle_value() != inherited_lifecycle
            {
                if previous_lifecycle != inherited_lifecycle {
                    for payload in current
                        .payloads
                        .values()
                        .filter(|payload| payload.shard != *shard)
                    {
                        emit(RefEvent::LifecycleChanged {
                            lsn: *lsn,
                            key: key.clone(),
                            shard: payload.shard,
                            record_ref: payload.record_ref,
                            old: previous_lifecycle,
                            new: inherited_lifecycle,
                        });
                    }
                }
                current.lifecycle = Some(LifecycleChange {
                    lsn: *lsn,
                    new_lifecycle: inherited_lifecycle,
                });
            }
            current.tombstone = None;
            emit(RefEvent::Live {
                lsn: *lsn,
                key,
                shard: *shard,
                record_ref: *record_ref,
                lifecycle: inherited_lifecycle,
            });
            apply_pending_maps(update.key(), current, emit);
        }
        BlobUpdate::Tombstone { lsn, .. } => {
            let current = state.get_or_insert_with(MaterializedBlobState::default);
            // Tombstones shadow older base/patch payloads. Even when no payload is currently loaded,
            // the marker must survive as residual state so a later major compaction can retire an
            // older physical ref at this LSN.
            for payload in std::mem::take(&mut current.payloads).into_values() {
                emit(RefEvent::Retired {
                    lsn: *lsn,
                    key: key.clone(),
                    shard: payload.shard,
                    record_ref: payload.record_ref,
                    lifecycle: current.lifecycle_value(),
                    reason: RetireReason::Tombstoned,
                });
            }
            current.head_lsn = *lsn;
            current.tombstone = Some(Tombstone { lsn: *lsn });
        }
        BlobUpdate::SetLifetime {
            lsn,
            logical_end_epoch,
            current_epoch,
            ..
        } => {
            let current = state.get_or_insert_with(MaterializedBlobState::default);
            let old = current.lifecycle_value();
            let old_expired = old.is_some_and(|old| old.logical_end_epoch <= *current_epoch);
            let active_old = old.filter(|_| !old_expired);
            // Lifetime extension is key-level metadata, but GC can enforce it only on the currently
            // live record range. If no payload is live yet, the lifecycle is carried in state and
            // attached when a Put later materializes a physical range.
            let new = active_old.map_or_else(
                || BlobLifecycle::new(*logical_end_epoch),
                |old| BlobLifecycle {
                    logical_end_epoch: *logical_end_epoch,
                    extension_count: old.extension_count.saturating_add(1),
                },
            );
            current.head_lsn = current.head_lsn.max(*lsn);
            current.lifecycle = Some(LifecycleChange {
                lsn: *lsn,
                new_lifecycle: Some(new),
            });
            if !old_expired && old != Some(new) {
                for payload in current.payloads.values() {
                    emit(RefEvent::LifecycleChanged {
                        lsn: *lsn,
                        key: key.clone(),
                        shard: payload.shard,
                        record_ref: payload.record_ref,
                        old,
                        new: Some(new),
                    });
                }
            }
        }
        BlobUpdate::MapRef { lsn, from, to, .. } => {
            let current = state.get_or_insert_with(MaterializedBlobState::default);
            current.head_lsn = current.head_lsn.max(*lsn);
            let lifecycle = current.lifecycle_value();
            if let Some(payload) = current
                .payloads
                .values_mut()
                .find(|payload| payload.record_ref == *from)
            {
                // When the source ref is present, MapRef is an in-place physical rewrite of the live
                // payload: retire the old range, install the new range, and carry lifecycle metadata
                // onto the new segment range.
                let shard = payload.shard;
                payload.record_ref = *to;
                payload.owner = SegmentOwner::Shard(shard);
                emit(RefEvent::Mapped {
                    lsn: *lsn,
                    key,
                    shard,
                    from: *from,
                    to: *to,
                    lifecycle,
                });
            } else {
                // If the source ref is not currently visible, the operation is not wrong; it is just
                // early relative to this fold. Keeping it pending lets a later Put or base row supply
                // the physical range before the rewrite is emitted.
                current.pending_maps.push(MapRef {
                    lsn: *lsn,
                    from: *from,
                    to: *to,
                });
            }
        }
    }
}

fn apply_pending_maps(
    key: &BlobKey,
    state: &mut MaterializedBlobState,
    emit: &mut impl FnMut(RefEvent),
) {
    let lifecycle = state.lifecycle_value();
    let mut index = 0;
    while index < state.pending_maps.len() {
        let map = state.pending_maps[index];
        let matching_shard = state
            .payloads
            .iter()
            .find_map(|(shard, payload)| (map.from == payload.record_ref).then_some(*shard));
        if let Some(shard) = matching_shard {
            // Pending maps are replayed at the moment the source physical ref appears. This is why
            // delta compaction keeps MapRef groups raw: only the full folded state can prove which
            // segment range should be retired and which replacement range inherits lifecycle state.
            state.pending_maps.remove(index);
            let payload = state
                .payloads
                .get_mut(&shard)
                .expect("matching shard payload must remain present");
            payload.record_ref = map.to;
            payload.owner = SegmentOwner::Shard(shard);
            state.head_lsn = state.head_lsn.max(map.lsn);
            emit(RefEvent::Mapped {
                lsn: map.lsn,
                key: key.clone(),
                shard,
                from: map.from,
                to: map.to,
                lifecycle,
            });
        } else {
            index += 1;
        }
    }
}

pub(crate) fn sort_updates(updates: &mut [BlobUpdate]) {
    // Run readers and k-way mergers rely on this physical ordering. The reducer itself is per-key,
    // but compaction needs all updates for a key to arrive as one contiguous, LSN-ordered slice.
    updates.sort_by(|left, right| {
        left.key()
            .cmp(right.key())
            .then_with(|| left.lsn().cmp(&right.lsn()))
    });
}
