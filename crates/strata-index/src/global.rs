use strata_core::{Epoch, StoreStateKey, StrataLsn};
use strata_lsm::{LsmCheckpoint, WalPosition};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    pub fn get_next_lsn(&self) -> Result<StrataLsn> {
        Ok(self.store_state.get(&StoreStateKey::NextLsn)?.unwrap_or(1))
    }

    pub fn get_published_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::PublishedLsn)?
            .unwrap_or_default())
    }

    pub fn get_current_epoch(&self) -> Result<Option<Epoch>> {
        Ok(self.store_state.get(&StoreStateKey::CurrentEpoch)?)
    }

    pub fn get_lazy_global_materialization_from_lsn(&self) -> Result<Option<StrataLsn>> {
        Ok(self
            .store_state
            .get(&StoreStateKey::LazyGlobalMaterializationFromLsn)?)
    }

    pub fn get_lsm_checkpoint(&self) -> Result<Option<LsmCheckpoint>> {
        let values = [
            self.store_state.get(&StoreStateKey::LsmDurableLsn)?,
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
            Some(durable_lsn),
            Some(log_id),
            Some(offset),
            Some(segment_id),
            Some(segment_offset),
        ] = values
        else {
            return Err(Error::InvalidLsmCheckpoint(
                "checkpoint fields are incomplete".to_owned(),
            ));
        };
        Ok(Some(LsmCheckpoint {
            durable_lsn: (durable_lsn != 0).then_some(durable_lsn),
            wal_position: WalPosition { log_id, offset },
            active_segment_id: segment_id,
            active_segment_offset: segment_offset,
        }))
    }

    pub fn get_relocation_lsm_checkpoint(&self) -> Result<Option<LsmCheckpoint>> {
        let values = [
            self.store_state
                .get(&StoreStateKey::RelocationLsmDurableLsn)?,
            self.store_state
                .get(&StoreStateKey::RelocationLsmWalLogId)?,
            self.store_state
                .get(&StoreStateKey::RelocationLsmWalOffset)?,
        ];
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        let [Some(durable_lsn), Some(log_id), Some(offset)] = values else {
            return Err(Error::InvalidLsmCheckpoint(
                "relocation checkpoint fields are incomplete".to_owned(),
            ));
        };
        Ok(Some(LsmCheckpoint {
            durable_lsn: (durable_lsn != 0).then_some(durable_lsn),
            wal_position: WalPosition { log_id, offset },
            active_segment_id: 1,
            active_segment_offset: 0,
        }))
    }

    pub fn put_next_lsn_batch(&self, batch: &mut DBBatch, next_lsn: StrataLsn) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::NextLsn, &next_lsn)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_published_lsn_batch(
        &self,
        batch: &mut DBBatch,
        published_lsn: StrataLsn,
    ) -> Result<()> {
        // Publication is the Store fence for foreground writes and blob-version compaction.
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::PublishedLsn, &published_lsn)],
        )?;
        Ok(())
    }

    pub fn put_lsm_checkpoint_batch(
        &self,
        batch: &mut DBBatch,
        checkpoint: LsmCheckpoint,
    ) -> Result<()> {
        let durable_lsn = checkpoint.durable_lsn.unwrap_or_default();
        batch.insert_batch(
            self.store_state(),
            [
                (&StoreStateKey::LsmDurableLsn, &durable_lsn),
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

    pub fn put_relocation_lsm_checkpoint_batch(
        &self,
        batch: &mut DBBatch,
        checkpoint: LsmCheckpoint,
    ) -> Result<()> {
        if checkpoint.active_segment_id != 1 || checkpoint.active_segment_offset != 0 {
            return Err(Error::InvalidLsmCheckpoint(
                "relocation LSM wrote to its unused segment".to_owned(),
            ));
        }
        let durable_lsn = checkpoint.durable_lsn.unwrap_or_default();
        batch.insert_batch(
            self.store_state(),
            [
                (&StoreStateKey::RelocationLsmDurableLsn, &durable_lsn),
                (
                    &StoreStateKey::RelocationLsmWalLogId,
                    &checkpoint.wal_position.log_id,
                ),
                (
                    &StoreStateKey::RelocationLsmWalOffset,
                    &checkpoint.wal_position.offset,
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

    pub fn put_lazy_global_materialization_from_lsn_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
    ) -> Result<()> {
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::LazyGlobalMaterializationFromLsn, &lsn)],
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
