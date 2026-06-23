pub(crate) mod gc_overlay;

use strata_core::{
    SegmentId, SegmentKey, SegmentRefEvent, SegmentRefEventKey, SegmentRefKey, SegmentRefState,
    SegmentState, SegmentStats, ShardKey,
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

    pub fn get_segment_stats(&self, segment_id: SegmentId) -> Result<Option<SegmentStats>> {
        self.get_segment_stats_for_shard(STANDALONE_SHARD, segment_id)
    }

    pub fn get_segment_stats_for_shard(
        &self,
        shard: ShardKey,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentStats>> {
        Ok(self.segment_stats.get(&SegmentKey { shard, segment_id })?)
    }

    pub fn put_segment_stats(&self, segment_id: SegmentId, stats: &SegmentStats) -> Result<()> {
        let mut batch = self.batch();
        self.put_segment_stats_batch(&mut batch, segment_id, stats)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_segment_stats_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        stats: &SegmentStats,
    ) -> Result<()> {
        self.put_segment_stats_for_shard_batch(batch, STANDALONE_SHARD, segment_id, stats)
    }

    pub fn put_segment_stats_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        segment_id: SegmentId,
        stats: &SegmentStats,
    ) -> Result<()> {
        // Segment stats are the aggregate accounting view; they intentionally live beside, not
        // inside, the GC overlay. Stats answer "how many bytes are live/tombstoned/expired", while
        // the overlay answers "which physical record ranges should copy planning skip or route."
        batch
            .insert_batch(
                self.segment_stats(),
                [(&SegmentKey { shard, segment_id }, stats)],
            )
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_segment_stats_for_shard(
        &self,
        shard: ShardKey,
    ) -> Result<Vec<(SegmentId, SegmentStats)>> {
        self.segment_stats
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, stats)) if key.shard == shard => Some(Ok((key.segment_id, stats))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn get_segment_ref_state(&self, key: SegmentRefKey) -> Result<Option<SegmentRefState>> {
        Ok(self.segment_ref_state.get(&key)?)
    }

    pub fn put_segment_ref_state_batch(
        &self,
        batch: &mut DBBatch,
        key: SegmentRefKey,
        state: &SegmentRefState,
    ) -> Result<()> {
        // Ref state is the latest truth for one segment-local offset. It is kept separate from
        // segment_ref_events so GC/reconciliation can query current liveness without replaying the
        // event stream, while still retaining ordered event rows for races with copying.
        batch
            .insert_batch(self.segment_ref_state(), [(&key, state)])
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
        // against exactly the physical range it came from. They are not a replacement for ref state;
        // they are the ordered evidence of transitions that happened while GC may have been running.
        batch
            .insert_batch(self.segment_ref_events(), [(&key, event)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn iter_segment_ref_state(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<(SegmentRefKey, SegmentRefState)>> {
        let mut refs = self
            .segment_ref_state
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, state)) if key.segment_id == segment_id => Some(Ok((key, state))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        refs.sort_by_key(|(key, _)| key.offset);
        Ok(refs)
    }

    pub fn iter_all_segment_ref_state(&self) -> Result<Vec<(SegmentRefKey, SegmentRefState)>> {
        let mut refs = self
            .segment_ref_state
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        refs.sort_by_key(|(key, _)| (key.segment_id, key.offset));
        Ok(refs)
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
