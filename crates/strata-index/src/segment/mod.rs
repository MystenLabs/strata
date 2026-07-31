pub(crate) mod gc_summary;

use strata_core::{SegmentId, SegmentOwner, SegmentState, ShardKey};
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

    /// Returns when a physical segment first became visible.
    ///
    /// Older stores have no row in this column family; zero keeps their segments conservatively
    /// protected by every live snapshot.
    pub fn get_segment_published_at_lsn(
        &self,
        segment_id: SegmentId,
    ) -> Result<strata_core::StrataLsn> {
        Ok(self.segment_publication_lsns.get(&segment_id)?.unwrap_or(0))
    }

    pub fn put_segment_published_at_lsn_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        published_at_lsn: strata_core::StrataLsn,
    ) -> Result<()> {
        batch
            .insert_batch(
                self.segment_publication_lsns(),
                [(&segment_id, &published_at_lsn)],
            )
            .map_err(Error::from)?;
        Ok(())
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
