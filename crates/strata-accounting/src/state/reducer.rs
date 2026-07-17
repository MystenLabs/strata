use strata_core::{BlobKey, BlobLifecycle, SegmentOwner};

use crate::events::{RefEvent, RetireReason};

use super::{
    BlobUpdate, LifecycleChange, LivePayload, MapRef, MaterializedBlobState, PatchUpdate, Tombstone,
};

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
