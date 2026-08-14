use std::collections::BTreeMap;

use strata_core::{SegmentId, SegmentOwner, ShardId, ShardInfo, StoreStateKey, StrataLsn};
use strata_gc::{GcSnapshot, SegmentSnapshot};
use typed_store::Map;
use typed_store::rocks::DBBatch;

use crate::{Error, Result};

use super::{StrataIndex, shard::shard_generation_is_obsolete};

impl StrataIndex {
    /// Builds a point-in-time GC planning view from published segment state.
    ///
    /// The planner consumes a plain `GcSnapshot`, not live database handles. This method is the
    /// boundary where the index pins a RocksDB snapshot, reads every GC-facing row from the same
    /// database view, and then releases the RocksDB snapshot before returning.
    ///
    /// The returned value is still advisory: a GC executor must revalidate source refs before
    /// publishing relocation tables or deleting segments.
    ///
    /// `None` means the namespace has not published a current epoch yet, so epoch-sensitive GC
    /// planning should not run.
    pub fn build_gc_snapshot(&self) -> Result<Option<GcSnapshot>> {
        let snapshot = self.db.snapshot();
        let Some(current_epoch) = self
            .store_state
            .get_with_snapshot(&snapshot, &StoreStateKey::CurrentEpoch)?
        else {
            return Ok(None);
        };
        let published_lsn = self
            .store_state
            .get_with_snapshot(&snapshot, &StoreStateKey::CommittedLsn)?
            .unwrap_or_default();
        let expiry_accounted_lsn = self
            .store_state
            .get_with_snapshot(&snapshot, &StoreStateKey::BlobExpiryAccountedLsn)?;
        let expiry_accounted_epoch = if let Some(accounted_lsn) = expiry_accounted_lsn {
            // Epoch history and the frontier are read from the same RocksDB snapshot as the
            // segment summaries below. For example, frontier LSN 120 maps to epoch 50 only when
            // the `(120, 50)` history row is visible here; this prevents a newly published epoch
            // pointer from being paired with counters from before its expiry sweep.
            let mut accounted_epoch = None;
            for result in self.epoch_changes.safe_iter_with_snapshot(&snapshot)? {
                let (lsn, epoch) = result?;
                if lsn <= accounted_lsn
                    && accounted_epoch.is_none_or(|(latest_lsn, _)| lsn > latest_lsn)
                {
                    accounted_epoch = Some((lsn, epoch));
                }
            }
            accounted_epoch.map(|(_, epoch)| epoch)
        } else {
            None
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
            let summary = self
                .segment_gc_summaries
                .get_with_snapshot(&snapshot, &state.segment_id)?
                .unwrap_or_default();
            segments.push(SegmentSnapshot {
                state,
                summary,
                claimed: false,
            });
        }
        segments.sort_by_key(|segment| (segment.state.owner, segment.state.segment_id));

        Ok(Some(GcSnapshot {
            current_epoch,
            expiry_accounted_epoch,
            published_lsn,
            segments,
        }))
    }

    /// Persists GC output bytes and the relocation activation that must be durable before the
    /// source can be unlinked.
    pub fn put_gc_reclaim_pending_batch(
        &self,
        batch: &mut DBBatch,
        source_segment_id: SegmentId,
        activation_lsn: StrataLsn,
        output_bytes: u64,
    ) -> Result<()> {
        let key = (source_segment_id, activation_lsn);
        batch
            .insert_batch(self.gc_reclaim_pending(), [(&key, &output_bytes)])
            .map_err(Error::from)?;
        Ok(())
    }

    /// Returns all pending reclaim attribution rows, ordered by source segment and activation
    /// sequence.
    pub fn iter_gc_reclaim_pending(&self) -> Result<Vec<((SegmentId, StrataLsn), u64)>> {
        self.gc_reclaim_pending
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub fn get_gc_reclaim_activation_lsn(
        &self,
        source_segment_id: SegmentId,
    ) -> Result<Option<StrataLsn>> {
        Ok(self
            .iter_gc_reclaim_pending()?
            .into_iter()
            .filter_map(|((segment_id, activation_lsn), _)| {
                (segment_id == source_segment_id).then_some(activation_lsn)
            })
            .max())
    }

    /// Removes and sums published-output attribution for deleted source segments in one scan.
    pub fn remove_gc_reclaim_pending_for_sources_batch(
        &self,
        batch: &mut DBBatch,
        source_segment_ids: &[SegmentId],
    ) -> Result<BTreeMap<SegmentId, u64>> {
        let rows = self.iter_gc_reclaim_pending()?;
        let mut keys = Vec::new();
        let mut output_bytes = source_segment_ids
            .iter()
            .copied()
            .map(|segment_id| (segment_id, 0_u64))
            .collect::<BTreeMap<_, _>>();
        for (key, bytes) in rows {
            let Some(total) = output_bytes.get_mut(&key.0) else {
                continue;
            };
            keys.push(key);
            *total = total.saturating_add(bytes);
        }
        batch.delete_batch(self.gc_reclaim_pending(), keys)?;
        Ok(output_bytes)
    }

    /// Removes legacy reclaim attribution created by GC publications hidden during foreground-LSN
    /// recovery rollback.
    pub fn remove_gc_reclaim_pending_from_lsn_batch(
        &self,
        batch: &mut DBBatch,
        rollback_from: StrataLsn,
    ) -> Result<usize> {
        let keys = self
            .iter_gc_reclaim_pending()?
            .into_iter()
            .filter_map(|(key, _)| (key.1 >= rollback_from).then_some(key))
            .collect::<Vec<_>>();
        let removed = keys.len();
        batch.delete_batch(self.gc_reclaim_pending(), keys)?;
        Ok(removed)
    }
}
