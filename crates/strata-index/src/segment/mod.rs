pub(crate) mod gc_overlay;

use strata_core::{
    SegmentId, SegmentOwner, SegmentRefEvent, SegmentRefEventKey, SegmentState, ShardKey,
};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    pub fn get_segment_state(&self, segment_id: SegmentId) -> Result<Option<SegmentState>> {
        Ok(self.segment_states.get(&segment_id)?)
    }

    pub fn get_segment_state_for_shard(
        &self,
        shard: ShardKey,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentState>> {
        Ok(self
            .get_segment_state(segment_id)?
            .filter(|state| state.owner == SegmentOwner::Shard(shard)))
    }

    pub fn put_segment_state(&self, state: &SegmentState) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_state_batch(&mut batch, state)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_state_batch(&self, batch: &mut DBBatch, state: &SegmentState) -> Result<()> {
        batch
            .insert_batch(self.segment_states(), [(&state.segment_id, state)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_segment_ref_event_batch(
        &self,
        batch: &mut DBBatch,
        key: SegmentRefEventKey,
        event: &SegmentRefEvent,
    ) -> Result<()> {
        // Ref events are keyed by segment, LSN, and offset so a copied record can be reconciled
        // against exactly the physical range it came from. They are the ordered evidence of
        // transitions that happened while GC may have been running.
        batch
            .insert_batch(self.segment_ref_events(), [(&key, event)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_segment_ref_events_since(
        &self,
        segment_id: SegmentId,
        since_lsn: strata_core::StrataLsn,
    ) -> Result<Vec<(SegmentRefEventKey, SegmentRefEvent)>> {
        let mut events = self
            .segment_ref_events
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, event)) if key.segment_id == segment_id && key.lsn > since_lsn => {
                    Some(Ok((key, event)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        events.sort_by_key(|(key, _)| (key.lsn, key.offset));
        Ok(events)
    }

    pub fn iter_segment_states(&self) -> Result<Vec<(SegmentId, SegmentState)>> {
        let mut states = self
            .segment_states
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        states.sort_by_key(|(segment_id, state)| (*segment_id, state.owner));
        Ok(states)
    }

    pub fn iter_segment_states_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(SegmentId, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((segment_id, state)) if state.owner == SegmentOwner::Shard(shard) => {
                    Some(Ok((segment_id, state)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }
}
