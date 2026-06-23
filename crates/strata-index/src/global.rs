use strata_core::{BlobKey, Epoch, StoreStateKey, StrataLsn, StrataStoreState};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

impl StrataIndex {
    pub fn get_next_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::NextLsn)?
            .unwrap_or_else(|| StrataStoreState::default().next_lsn))
    }

    pub fn get_durable_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::DurableLsn)?
            .unwrap_or_else(|| StrataStoreState::default().durable_lsn))
    }

    pub fn get_accounted_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .store_state
            .get(&StoreStateKey::AccountedLsn)?
            .unwrap_or_else(|| StrataStoreState::default().accounted_lsn))
    }

    pub fn get_current_epoch(&self) -> Result<Option<Epoch>> {
        Ok(self.store_state.get(&StoreStateKey::CurrentEpoch)?)
    }

    pub fn get_store_state(&self) -> Result<Option<StrataStoreState>> {
        Ok(Some(StrataStoreState {
            next_lsn: self.get_next_lsn()?,
            durable_lsn: self.get_durable_lsn()?,
            accounted_lsn: self.get_accounted_lsn()?,
        }))
    }

    pub fn put_next_lsn_batch(&self, batch: &mut DBBatch, next_lsn: StrataLsn) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::NextLsn, &next_lsn)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_durable_lsn_batch(&self, batch: &mut DBBatch, durable_lsn: StrataLsn) -> Result<()> {
        // Durability is the publication fence for foreground writes and for blob-version compaction.
        // Callers that advance this row must publish the in-memory blob compaction frontier only
        // after the batch is written and the RocksDB WAL has been synced.
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::DurableLsn, &durable_lsn)],
        )?;
        Ok(())
    }

    pub fn put_accounted_lsn_batch(
        &self,
        batch: &mut DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        // AccountedLsn is the store-global cursor for sidecar materialization. Once this row is
        // committed, segment stats, ref states, ref events, and GC overlay operands are durable
        // through this LSN.
        batch.insert_batch(
            self.store_state(),
            [(&StoreStateKey::AccountedLsn, &accounted_lsn)],
        )?;
        Ok(())
    }

    /// Commits accounting results.
    ///
    /// Blob-version compaction follows the durable LSN frontier, not this accounting frontier. The
    /// accounting cursor still controls when pending accounting rows can be removed and when derived
    /// segment stats, ref states, ref events, and GC overlay operands are durable.
    pub fn commit_accounting_batch(
        &self,
        mut batch: DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<()> {
        self.put_accounted_lsn_batch(&mut batch, accounted_lsn)?;
        batch.write()?;
        self.flush_wal(true)?;
        Ok(())
    }

    pub fn put_current_epoch_batch(&self, batch: &mut DBBatch, epoch: Epoch) -> Result<()> {
        batch
            .insert_batch(self.store_state(), [(&StoreStateKey::CurrentEpoch, &epoch)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn put_store_state_batch(
        &self,
        batch: &mut DBBatch,
        state: &StrataStoreState,
    ) -> Result<()> {
        self.put_next_lsn_batch(batch, state.next_lsn)?;
        self.put_durable_lsn_batch(batch, state.durable_lsn)?;
        self.put_accounted_lsn_batch(batch, state.accounted_lsn)
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

    pub fn put_blob_unaccounted_lsn_op_batch(
        &self,
        batch: &mut DBBatch,
        lsn: StrataLsn,
        key: &BlobKey,
    ) -> Result<()> {
        // This reverse index is the bridge from the global LSN stream back to packed per-blob
        // history. Accounting uses it to find exactly which blob key must be unfolded for an LSN
        // before the blob_versions value is allowed to compact that LSN away.
        batch
            .insert_batch(self.unaccounted_lsn_ops(), [(&lsn, key)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_unaccounted_lsn_op(&self, lsn: StrataLsn) -> Result<Option<BlobKey>> {
        Ok(self.unaccounted_lsn_ops.get(&lsn)?)
    }

    pub fn iter_unaccounted_lsn_ops(&self) -> Result<Vec<(StrataLsn, BlobKey)>> {
        self.iter_unaccounted_lsn_ops_from(0)
    }

    pub fn iter_unaccounted_lsn_ops_from(
        &self,
        min_lsn: StrataLsn,
    ) -> Result<Vec<(StrataLsn, BlobKey)>> {
        let mut ops = self
            .unaccounted_lsn_ops
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((lsn, blob_key)) if lsn >= min_lsn => Some(Ok((lsn, blob_key))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        ops.sort_by_key(|(lsn, _)| *lsn);
        Ok(ops)
    }

    pub fn remove_unaccounted_lsn_ops_batch(
        &self,
        batch: &mut DBBatch,
        lsns: &[StrataLsn],
    ) -> Result<()> {
        // Removing these rows is safe only after accounting has materialized their physical effects.
        // Once gone, restart recovery will no longer ask the blob index to explain those LSNs.
        for lsn in lsns {
            batch.delete_batch(self.unaccounted_lsn_ops(), [*lsn])?;
        }
        Ok(())
    }
}
