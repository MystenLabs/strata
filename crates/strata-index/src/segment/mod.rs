pub(crate) mod gc_overlay;

use strata_core::{
    SegmentId, SegmentKey, SegmentRefEvent, SegmentRefEventKey, SegmentState, ShardKey,
};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::{STANDALONE_SHARD, StrataIndex};

impl StrataIndex {
    pub fn get_segment_state(&self, segment_id: SegmentId) -> Result<Option<SegmentState>> {
        self.get_segment_state_for_shard(STANDALONE_SHARD, segment_id)
    }

    pub fn get_segment_state_for_shard(
        &self,
        shard: ShardKey,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentState>> {
        Ok(self.segment_states.get(&SegmentKey { shard, segment_id })?)
    }

    pub fn put_segment_state(&self, state: &SegmentState) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_state_batch(&mut batch, state)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_state_batch(&self, batch: &mut DBBatch, state: &SegmentState) -> Result<()> {
        batch
            .insert_batch(
                self.segment_states(),
                [(
                    &SegmentKey {
                        shard: state.shard,
                        segment_id: state.segment_id,
                    },
                    state,
                )],
            )
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
        self.iter_segment_states_for_shard(STANDALONE_SHARD)
    }

    pub fn iter_segment_states_by_key(&self) -> Result<Vec<(SegmentKey, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn iter_segment_states_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(SegmentId, SegmentState)>> {
        self.segment_states
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, state)) if key.shard == shard => Some(Ok((key.segment_id, state))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }
}
