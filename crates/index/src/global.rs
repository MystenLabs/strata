use core_types::{Epoch, StoreCheckpoint, StoreStateKey, StrataLsn, WalPosition};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    pub fn get_next_lsn(&self) -> Result<StrataLsn> {
        Ok(self.store_state.get(&StoreStateKey::NextLsn)?.unwrap_or(1))
    }

    pub fn get_committed_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::CommittedLsn)?
            .unwrap_or_default())
    }

    pub fn get_current_epoch(&self) -> Result<Option<Epoch>> {
        Ok(self.store_state.get(&StoreStateKey::CurrentEpoch)?)
    }

    pub fn get_blob_compaction_garbage_from_lsn(&self) -> Result<Option<StrataLsn>> {
        Ok(self
            .store_state
            .get(&StoreStateKey::BlobCompactionGarbageFromLsn)?)
    }

    /// Returns the durable expiry-accounting frontier consumed by GC planning.
    ///
    /// Missing means no background pass has yet proved end-to-end coverage. In particular, it is
    /// different from LSN 0: an upgraded store with old base SSTs must sweep those bases before GC
    /// treats even an old exact-epoch directory as expiry-complete.
    pub fn get_blob_expiry_accounted_lsn(&self) -> Result<Option<StrataLsn>> {
        Ok(self
            .store_state
            .get(&StoreStateKey::BlobExpiryAccountedLsn)?)
    }

    /// Returns the durable write-merge frontier: every blob mutation below it has been merged
    /// into a base table and its garbage swept into the segment summaries.
    pub fn get_blob_writes_merged_lsn(&self) -> Result<Option<StrataLsn>> {
        Ok(self.store_state.get(&StoreStateKey::BlobWritesMergedLsn)?)
    }

    pub fn get_store_wal_retained_from(&self) -> Result<Option<u64>> {
        Ok(self.store_state.get(&StoreStateKey::StoreWalRetainedFrom)?)
    }

    pub fn get_store_checkpoint(&self) -> Result<Option<StoreCheckpoint>> {
        let values = [
            self.store_state.get(&StoreStateKey::LsmWalLogId)?,
            self.store_state.get(&StoreStateKey::LsmWalOffset)?,
            self.store_state.get(&StoreStateKey::LsmActiveSegmentId)?,
            self.store_state
                .get(&StoreStateKey::LsmActiveSegmentOffset)?,
        ];
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        let [
            Some(log_id),
            Some(offset),
            Some(segment_id),
            Some(segment_offset),
        ] = values
        else {
            return Err(Error::InvalidStoreCheckpoint(
                "checkpoint fields are incomplete".to_owned(),
            ));
        };
        Ok(Some(StoreCheckpoint {
            wal_position: WalPosition { log_id, offset },
            active_segment_id: segment_id,
            active_segment_offset: segment_offset,
        }))
    }

    pub fn put_next_lsn_batch(&self, batch: &mut DBBatch, next_lsn: StrataLsn) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::NextLsn, &next_lsn)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_commit_lsn_batch(
        &self,
        batch: &mut DBBatch,
        published_lsn: StrataLsn,
    ) -> Result<()> {
        // Publication is the Store fence for foreground writes and blob-version compaction.
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::CommittedLsn, &published_lsn)],
        )?;
        Ok(())
    }

    pub fn put_store_wal_retained_from_batch(
        &self,
        batch: &mut DBBatch,
        first_log_id: u64,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::StoreWalRetainedFrom, &first_log_id)],
        )?;
        Ok(())
    }

    pub fn put_store_checkpoint_batch(
        &self,
        batch: &mut DBBatch,
        checkpoint: StoreCheckpoint,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [
                (&StoreStateKey::LsmWalLogId, &checkpoint.wal_position.log_id),
                (
                    &StoreStateKey::LsmWalOffset,
                    &checkpoint.wal_position.offset,
                ),
                (
                    &StoreStateKey::LsmActiveSegmentId,
                    &checkpoint.active_segment_id,
                ),
                (
                    &StoreStateKey::LsmActiveSegmentOffset,
                    &checkpoint.active_segment_offset,
                ),
            ],
        )?;
        Ok(())
    }

    pub fn put_current_epoch_batch(&self, batch: &mut DBBatch, epoch: Epoch) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::CurrentEpoch, &epoch)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_blob_compaction_garbage_from_lsn_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::BlobCompactionGarbageFromLsn, &lsn)],
        )?;
        Ok(())
    }

    /// Persists the end-to-end expiry frontier in the caller's batch.
    ///
    /// The caller advances this only after major-compaction coverage is complete and the global
    /// garbage log is drained. Storing it beside the segment summaries lets `build_gc_snapshot`
    /// read both from one RocksDB snapshot; a planner can never combine a new frontier with old
    /// per-segment counters.
    pub fn put_blob_expiry_accounted_lsn_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::BlobExpiryAccountedLsn, &lsn)],
        )?;
        Ok(())
    }

    /// Persists the write-merge frontier in the caller's batch, under the same drained-garbage-log
    /// rule as the expiry frontier.
    pub fn put_blob_writes_merged_lsn_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::BlobWritesMergedLsn, &lsn)],
        )?;
        Ok(())
    }

    pub fn put_epoch_change_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        epoch: Epoch,
    ) -> Result<()> {
        batch
            .insert_batch(self.epoch_changes(), [(&lsn, &epoch)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_epoch_change(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        Ok(self.epoch_changes.get(&lsn)?)
    }

    pub fn latest_epoch_at_lsn(&self, max_lsn: StrataLsn) -> Result<Option<(StrataLsn, Epoch)>> {
        let mut latest = None;
        for result in self.epoch_changes.safe_iter()? {
            let (lsn, epoch) = result?;
            if lsn <= max_lsn && latest.is_none_or(|(latest_lsn, _)| lsn > latest_lsn) {
                latest = Some((lsn, epoch));
            }
        }
        Ok(latest)
    }

    pub fn iter_epoch_changes_from(&self, min_lsn: StrataLsn) -> Result<Vec<(StrataLsn, Epoch)>> {
        let mut changes = self
            .epoch_changes
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((lsn, epoch)) if lsn >= min_lsn => Some(Ok((lsn, epoch))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        changes.sort_by_key(|(lsn, _)| *lsn);
        Ok(changes)
    }

    pub fn remove_epoch_changes_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        for lsn in lsns {
            batch.delete_batch(&self.epoch_changes, [*lsn])?;
        }
        Ok(())
    }
}
