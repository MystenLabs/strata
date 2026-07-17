use std::collections::BTreeSet;

use strata_core::{BlobKey, StrataLsn};

use crate::{
    events::{CompactionEventBatch, RefEvent, RetireReason},
    state::{BlobUpdate, PatchUpdate, fold_update},
};

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
