use std::collections::BTreeMap;

use strata_core::{
    GcRelocation, RecordRef, SegmentId, SegmentOwner, ShardId, ShardInfo, StoreStateKey, StrataLsn,
};
use strata_gc::{GcSnapshot, SegmentSnapshot};
use typed_store::Map;
use typed_store::rocks::DBBatch;

use crate::{Error, Result};

use super::{AccountingSnapshotGuard, StrataIndex, shard::shard_generation_is_obsolete};

impl StrataIndex {
    /// Builds a point in time GC planning view for an active accounting snapshot.
    ///
    /// The planner consumes a plain `GcSnapshot`, not live database handles. This method is the
    /// boundary where the index pins a RocksDB snapshot, reads every GC-facing row from the same
    /// database view, and then releases the RocksDB snapshot before returning.
    ///
    /// The returned `accounted_lsn` comes from `accounting_snapshot`, not from the RocksDB view
    /// taken here. A GC run must use the same guard again at publish time with
    /// `accounting_changes_since` so it can reconcile ref events that accounting materialized while
    /// records were being copied.
    ///
    /// The returned value is still advisory: a GC executor must revalidate source refs before
    /// publishing `MapRef` operations or deleting segments.
    ///
    /// `None` means the namespace has not published a current epoch yet, so epoch-sensitive GC
    /// planning should not run.
    pub fn build_gc_snapshot(
        &self,
        accounting_snapshot: &AccountingSnapshotGuard,
    ) -> Result<Option<GcSnapshot>> {
        let snapshot = self.db.snapshot();
        let Some(current_epoch) = self
            .store_state
            .get_with_snapshot(&snapshot, &StoreStateKey::CurrentEpoch)?
        else {
            return Ok(None);
        };
        let shard_infos = self
            .shards
            .safe_iter_with_snapshot(&snapshot)?
            .collect::<std::result::Result<BTreeMap<ShardId, ShardInfo>, _>>()
            .map_err(Error::from)?;

        let mut segments = Vec::new();
        for result in self.segment_states.safe_iter_with_snapshot(&snapshot)? {
            let (_, state) = result?;
            if let SegmentOwner::Shard(shard) = state.owner
                && shard_generation_is_obsolete(shard, &shard_infos)
            {
                continue;
            }
            let summary = self.segment_gc_summary_with_snapshot(&snapshot, state.segment_id)?;
            segments.push(SegmentSnapshot {
                state,
                summary,
                claimed: false,
            });
        }
        segments.sort_by_key(|segment| (segment.state.owner, segment.state.segment_id));

        Ok(Some(GcSnapshot {
            current_epoch,
            accounted_lsn: accounting_snapshot.accounted_lsn(),
            segments,
        }))
    }

    fn segment_gc_summary_with_snapshot(
        &self,
        snapshot: &typed_store::rocks::RocksDBSnapshot<'_>,
        segment_id: SegmentId,
    ) -> Result<strata_core::SegmentGcSummary> {
        self.segment_gc_overlay
            .get_with_snapshot(snapshot, &segment_id)
            .map(|overlay| overlay.unwrap_or_default().summary)
            .map_err(Error::from)
    }

    /// Installs or updates a GC relocation forwarding row in the caller's atomic batch.
    pub fn put_gc_relocation_batch(
        &self,
        batch: &mut DBBatch,
        from: RecordRef,
        relocation: &GcRelocation,
    ) -> Result<()> {
        batch
            .insert_batch(self.gc_relocations(), [(&from, relocation)])
            .map_err(Error::from)?;
        Ok(())
    }

    /// Deletes one GC relocation forwarding row in the caller's atomic batch.
    pub fn delete_gc_relocation_batch(&self, batch: &mut DBBatch, from: RecordRef) -> Result<()> {
        batch.delete_batch(self.gc_relocations(), [from])?;
        Ok(())
    }

    /// Reads a relocation row by its source physical record.
    pub fn get_gc_relocation(&self, from: RecordRef) -> Result<Option<GcRelocation>> {
        Ok(self.gc_relocations.get(&from)?)
    }

    /// Returns all active relocation rows, sorted by source record.
    pub fn iter_gc_relocations(&self) -> Result<Vec<(RecordRef, GcRelocation)>> {
        self.gc_relocations
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    /// Removes relocation rows whose publish LSN has already been accounted.
    pub fn remove_gc_relocations_through_lsn_batch(
        &self,
        batch: &mut DBBatch,
        accounted_lsn: StrataLsn,
    ) -> Result<usize> {
        let rows = self.iter_gc_relocations()?;
        let mut removed = 0;
        for (from, relocation) in rows {
            if relocation.publish_lsn <= accounted_lsn {
                self.delete_gc_relocation_batch(batch, from)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Removes relocation rows whose publish LSN is being rolled back during recovery.
    pub fn remove_gc_relocations_from_lsn_batch(
        &self,
        batch: &mut DBBatch,
        rollback_from: StrataLsn,
    ) -> Result<usize> {
        let rows = self.iter_gc_relocations()?;
        let mut removed = 0;
        for (from, relocation) in rows {
            if relocation.publish_lsn >= rollback_from {
                self.delete_gc_relocation_batch(batch, from)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}
