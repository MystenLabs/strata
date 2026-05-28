use strata_core::{
    BlobKey, BlobVersionKey, SegmentFileState, SegmentId, SegmentState, StrataLsn, StrataStoreState,
};
use strata_index::StrataIndex;
use typed_store::Map;

use crate::Result;

pub(crate) fn active_segment_durable_offset(
    index: &StrataIndex,
    segment_id: SegmentId,
) -> Result<u64> {
    Ok(index
        .get_segment_state(segment_id)?
        .map_or(0, |state| state.durable_offset))
}

pub(crate) fn store_state_with_advanced_durable_lsn(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
    batch: &mut typed_store::rocks::DBBatch,
) -> Result<StrataStoreState> {
    let mut store_state = index.get_store_state()?.unwrap_or_default();
    store_state.durable_lsn =
        compute_durable_lsn(index, store_state.durable_lsn, override_state, batch)?;
    Ok(store_state)
}

fn compute_durable_lsn(
    index: &StrataIndex,
    current_durable_lsn: StrataLsn,
    override_state: Option<&SegmentState>,
    batch: &mut typed_store::rocks::DBBatch,
) -> Result<StrataLsn> {
    let states = segment_states_with_override(index, override_state)?;
    let mut durable_lsn = current_durable_lsn;

    loop {
        let Some(next_lsn) = durable_lsn.checked_add(1) else {
            break;
        };
        let Some(key) = index
            .pending_lsn_ops()
            .get(&next_lsn)
            .map_err(strata_index::Error::from)?
        else {
            break;
        };
        if !pending_lsn_is_durable(index, next_lsn, &key, &states)? {
            break;
        }
        batch
            .delete_batch(index.pending_lsn_ops(), [&next_lsn])
            .map_err(strata_index::Error::from)?;
        durable_lsn = next_lsn;
    }

    Ok(durable_lsn)
}

fn segment_states_with_override(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
) -> Result<Vec<(SegmentId, SegmentState)>> {
    let mut states = index.iter_segment_states()?;
    if let Some(override_state) = override_state {
        let mut replaced = false;
        for (_, state) in &mut states {
            if state.segment_id == override_state.segment_id {
                *state = override_state.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            states.push((override_state.segment_id, override_state.clone()));
        }
    }
    Ok(states)
}

fn pending_lsn_is_durable(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let Some(entry) = index.get_blob_version(&BlobVersionKey {
        key: key.clone(),
        lsn,
    })?
    else {
        return Ok(false);
    };
    let Some(record_ref) = entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    Ok(states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            !matches!(
                state.state,
                SegmentFileState::SealFailed
                    | SegmentFileState::Deleting
                    | SegmentFileState::Deleted
            ) && state.durable_offset >= record_end_offset
        }))
}
