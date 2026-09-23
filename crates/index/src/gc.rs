use std::collections::BTreeMap;

use crate::port::map::IndexBatch;
use core_types::{Epoch, SegmentId, SegmentOwner, ShardId, ShardInfo, StoreStateKey, StrataLsn};
use gc_planner::{GcSnapshot, SegmentSnapshot};

use crate::{Error, Result};

use super::{StrataIndex, shard::shard_generation_is_obsolete};

/// Actual GC byte attribution consumed when one or more source files are physically unlinked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcReclaimAttribution {
    pub output_bytes: u64,
    /// Missing for legacy reclaim rows created before per-strategy attribution was persisted.
    pub strategy: Option<String>,
}

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
        let snapshot = self.db.snapshot()?;
        let Some(current_epoch) = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::CurrentEpoch)?
        else {
            return Ok(None);
        };
        let published_lsn = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::CommittedLsn)?
            .unwrap_or_default();
        let expiry_accounted_lsn = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::BlobExpiryAccountedLsn)?;
        // Epoch history and the frontiers are read from the same RocksDB snapshot as the segment
        // summaries below. For example, frontier LSN 120 maps to epoch 50 only when the
        // `(120, 50)` history row is visible here; this prevents a newly published epoch pointer
        // from being paired with counters from before its expiry sweep.
        let expiry_accounted_epoch = match expiry_accounted_lsn {
            Some(lsn) => latest_epoch_at_or_before(
                self.epoch_changes
                    .safe_iter_with_snapshot(snapshot.as_ref())?,
                lsn,
            )?,
            None => None,
        };
        let writes_merged_lsn = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::BlobWritesMergedLsn)?;
        let writes_merged_epoch = match writes_merged_lsn {
            Some(lsn) => latest_epoch_at_or_before(
                self.epoch_changes
                    .safe_iter_with_snapshot(snapshot.as_ref())?,
                lsn,
            )?,
            None => None,
        };
        let shard_infos = self
            .shards
            .safe_iter_with_snapshot(snapshot.as_ref())?
            .collect::<std::result::Result<BTreeMap<ShardId, ShardInfo>, _>>()?;

        // Compaction does not report epoch expiry per record; the planner judges known end
        // epochs against the clock, capped by the write-merge frontier so no unmerged extension
        // can still move a bucket. Hand it summaries with those buckets already moved to expired
        // so every strict counter it reads (garbage ratio, bytes to copy, join eligibility) sees
        // the same view as the clock-live helper.
        let clock_expiry_epoch = writes_merged_epoch.map(|epoch| epoch.min(current_epoch));
        let mut segments = Vec::new();
        for result in self
            .segment_states
            .safe_iter_with_snapshot(snapshot.as_ref())?
        {
            let (_, state) = result?;
            if let SegmentOwner::Shard(shard) = state.owner
                && shard_generation_is_obsolete(shard, &shard_infos)
            {
                continue;
            }
            let summary = self
                .segment_gc_summaries
                .get_with_snapshot(snapshot.as_ref(), &state.segment_id)?
                .unwrap_or_default();
            let summary = match clock_expiry_epoch {
                Some(epoch) => summary.as_of_epoch(epoch),
                None => summary,
            };
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
            writes_merged_epoch,
            lifecycle_accounted_lsn: expiry_accounted_lsn,
            published_lsn,
            segments,
        }))
    }

    /// Epoch through which GC may trust a record's known end epoch against the clock: the current
    /// epoch, capped by the epoch whose transition the write-merge frontier has passed.
    ///
    /// The clock, the frontier, and the epoch history are read from one RocksDB snapshot so a
    /// newly published epoch pointer cannot be paired with an older frontier. `None` means no
    /// frontier has been published yet, which disables clock-based expiry.
    pub fn clock_expiry_epoch(&self) -> Result<Option<Epoch>> {
        let snapshot = self.db.snapshot()?;
        let Some(current_epoch) = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::CurrentEpoch)?
        else {
            return Ok(None);
        };
        let Some(writes_merged_lsn) = self
            .store_state
            .get_with_snapshot(snapshot.as_ref(), &StoreStateKey::BlobWritesMergedLsn)?
        else {
            return Ok(None);
        };
        Ok(latest_epoch_at_or_before(
            self.epoch_changes
                .safe_iter_with_snapshot(snapshot.as_ref())?,
            writes_merged_lsn,
        )?
        .map(|epoch| epoch.min(current_epoch)))
    }

    /// Persists GC output bytes and the relocation activation that must be durable before the
    /// source can be unlinked.
    pub fn put_gc_reclaim_pending_batch(
        &self,
        batch: &mut IndexBatch,
        source_segment_id: SegmentId,
        activation_lsn: StrataLsn,
        output_bytes: u64,
        strategy: &str,
    ) -> Result<()> {
        let key = (source_segment_id, activation_lsn);
        batch.insert_batch(self.gc_reclaim_pending(), [(&key, &output_bytes)])?;
        let strategy = strategy.to_owned();
        batch.insert_batch(self.gc_reclaim_strategies(), [(&key, &strategy)])?;
        Ok(())
    }

    /// Returns all pending reclaim attribution rows, ordered by source segment and activation
    /// sequence.
    pub fn iter_gc_reclaim_pending(&self) -> Result<Vec<((SegmentId, StrataLsn), u64)>> {
        self.gc_reclaim_pending
            .safe_iter()?
            .collect::<std::result::Result<Vec<_>, _>>()
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
        batch: &mut IndexBatch,
        source_segment_ids: &[SegmentId],
    ) -> Result<BTreeMap<SegmentId, GcReclaimAttribution>> {
        let rows = self.iter_gc_reclaim_pending()?;
        let strategies = self
            .gc_reclaim_strategies
            .safe_iter()?
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()?;
        let mut keys = Vec::new();
        let mut row_counts = BTreeMap::<SegmentId, usize>::new();
        let mut attribution = source_segment_ids
            .iter()
            .copied()
            .map(|segment_id| {
                (
                    segment_id,
                    GcReclaimAttribution {
                        output_bytes: 0,
                        strategy: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (key, bytes) in rows {
            let Some(total) = attribution.get_mut(&key.0) else {
                continue;
            };
            keys.push(key);
            total.output_bytes = total.output_bytes.saturating_add(bytes);
            let strategy = strategies.get(&key).cloned();
            let count = row_counts.entry(key.0).or_default();
            if *count == 0 {
                total.strategy = strategy;
            } else if total.strategy != strategy {
                // Multiple publications for one source are not expected. If legacy or mixed
                // attribution is encountered, preserve byte correctness and label it unknown.
                total.strategy = None;
            }
            *count += 1;
        }
        batch.delete_batch(self.gc_reclaim_pending(), keys.iter().copied())?;
        batch.delete_batch(self.gc_reclaim_strategies(), keys)?;
        Ok(attribution)
    }

    /// Removes legacy reclaim attribution created by GC publications hidden during foreground-LSN
    /// recovery rollback.
    pub fn remove_gc_reclaim_pending_from_lsn_batch(
        &self,
        batch: &mut IndexBatch,
        rollback_from: StrataLsn,
    ) -> Result<usize> {
        let keys = self
            .iter_gc_reclaim_pending()?
            .into_iter()
            .filter_map(|(key, _)| (key.1 >= rollback_from).then_some(key))
            .collect::<Vec<_>>();
        let removed = keys.len();
        batch.delete_batch(self.gc_reclaim_pending(), keys.iter().copied())?;
        batch.delete_batch(self.gc_reclaim_strategies(), keys)?;
        Ok(removed)
    }
}

/// The epoch published at the latest transition whose LSN is at or below `lsn`.
fn latest_epoch_at_or_before<I, E>(changes: I, lsn: StrataLsn) -> Result<Option<Epoch>>
where
    I: IntoIterator<Item = std::result::Result<(StrataLsn, Epoch), E>>,
    Error: From<E>,
{
    let mut latest = None;
    for result in changes {
        let (change_lsn, epoch) = result?;
        if change_lsn <= lsn && latest.is_none_or(|(latest_lsn, _)| change_lsn > latest_lsn) {
            latest = Some((change_lsn, epoch));
        }
    }
    Ok(latest.map(|(_, epoch)| epoch))
}
