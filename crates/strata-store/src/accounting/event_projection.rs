use std::collections::{BTreeMap, BTreeSet};

use strata_accounting::{CompactionEventBatch, Manifest, RefEvent as AccountingRefEvent};
use strata_core::{
    BlobLifecycle, Epoch, GcRelocation, RecordRef, SegmentGcLifetimeUpdate, SegmentGcLiveRecord,
    SegmentGcOverlayMergeOp, SegmentGcRecordRange, SegmentId, SegmentRefEvent, SegmentRefEventKey,
    StrataLsn,
};
use strata_index::StrataIndex;
use typed_store::rocks::DBBatch;

use crate::{
    Error, Result,
    metrics::{AccountingEventCounts, AccountingOverlayDelta},
};

fn lifecycle_is_expired(lifecycle: Option<BlobLifecycle>, current_epoch: Epoch) -> bool {
    lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch)
}

pub(super) struct FrontierUpdate {
    pub(super) accounted_lsn: StrataLsn,
    pub(super) consumed_lsns: Vec<StrataLsn>,
    pub(super) materialized_epoch_change: bool,
    pub(super) completed_shard_drops: Vec<strata_core::ShardKey>,
}

/// Scratchpad for one processor commit.
///
/// Ramp-up: processor compaction produces ref events. This context translates those events into the
/// main-index rows GC already consumes, while keeping the RocksDB commit atomic with the processor
/// manifest/cursor update.
///
/// Failure example: applying event rows outside the manifest commit would either duplicate
/// non-idempotent overlay allocations after a retry or lose them after a crash.
pub(super) struct AccountingProjection<'a> {
    store_index: &'a StrataIndex,
    ref_events: BTreeMap<SegmentRefEventKey, SegmentRefEvent>,
    pub(super) gc_overlay_ops: BTreeMap<SegmentId, Vec<SegmentGcOverlayMergeOp>>,
    relocations: BTreeMap<RecordRef, GcRelocation>,
    removed_relocations: BTreeSet<RecordRef>,
}

impl<'a> AccountingProjection<'a> {
    pub(super) fn new(store_index: &'a StrataIndex) -> Result<Self> {
        Ok(Self {
            store_index,
            ref_events: BTreeMap::new(),
            gc_overlay_ops: BTreeMap::new(),
            relocations: store_index.iter_gc_relocations()?.into_iter().collect(),
            removed_relocations: BTreeSet::new(),
        })
    }

    /// Applies all processor-produced ref events for one compaction.
    ///
    /// The events are structural; this layer translates them into ordered ref events and overlay
    /// merge operands. The overlay fold owns summary counter updates.
    pub(super) fn apply_compaction_event_batch(
        &mut self,
        batch: &CompactionEventBatch,
    ) -> Result<()> {
        for event in &batch.events {
            self.apply_ref_event(event)?;
        }
        Ok(())
    }

    fn apply_ref_event(&mut self, event: &AccountingRefEvent) -> Result<()> {
        // The processor event stream is logical and key-oriented; the main index rows are
        // segment-oriented. This translation preserves that one event can fan out into several
        // physical rows: ordered ref events and GC overlay operands.
        match event {
            AccountingRefEvent::Live {
                lsn,
                record_ref,
                lifecycle,
                ..
            } => self.add_ref(*lsn, *record_ref, *lifecycle),
            AccountingRefEvent::Retired {
                lsn,
                record_ref,
                lifecycle,
                ..
            } => self.retire_ref(*lsn, *record_ref, *lifecycle),
            AccountingRefEvent::LifecycleChanged {
                lsn,
                record_ref,
                old,
                new,
                ..
            } => self.change_lifecycle(*lsn, *record_ref, *old, *new),
            AccountingRefEvent::Mapped {
                lsn,
                from,
                to,
                lifecycle,
                ..
            } => self.map_ref(*lsn, *from, *to, *lifecycle),
        }
    }

    /// Advances the global accounting cursor as far as processor materialization permits.
    ///
    /// This still inspects `unaccounted_lsn_ops`, but only to compute a contiguous global cleanup
    /// frontier. It does not resolve blob state or derive GC state from blob keys.
    pub(super) fn advance_frontier(
        &mut self,
        manifest: &Manifest,
        partition_for_key: impl Fn(&strata_core::BlobKey) -> u32,
    ) -> Result<FrontierUpdate> {
        let durable_lsn = self.store_index.get_durable_lsn()?;
        let mut accounted_lsn = self.store_index.get_accounted_lsn()?;
        let mut consumed_lsns = Vec::new();
        let mut materialized_epoch_change = false;
        let mut completed_shard_drops = Vec::new();

        loop {
            let Some(next_lsn) = accounted_lsn.checked_add(1) else {
                break;
            };
            if next_lsn > durable_lsn {
                break;
            }

            if let Some(key) = self.store_index.get_unaccounted_lsn_op(next_lsn)? {
                let partition = partition_for_key(&key);
                let Some(partition) = manifest.partitions.get(&partition) else {
                    break;
                };
                if partition.materialized_through_lsn < next_lsn {
                    break;
                }
                consumed_lsns.push(next_lsn);
                accounted_lsn = next_lsn;
                continue;
            }

            if let Some(epoch) = self.store_index.get_epoch_change(next_lsn)? {
                self.expire_live_refs(next_lsn, epoch)?;
                materialized_epoch_change = true;
                accounted_lsn = next_lsn;
                continue;
            }

            if let Some(drop) = manifest
                .shard_drops
                .iter()
                .find(|drop| drop.lsn == next_lsn && drop.materialized)
            {
                completed_shard_drops.push(drop.shard);
                accounted_lsn = next_lsn;
                continue;
            }

            break;
        }

        Ok(FrontierUpdate {
            accounted_lsn,
            consumed_lsns,
            materialized_epoch_change,
            completed_shard_drops,
        })
    }

    /// Stages all derived rows into the caller's processor metadata batch.
    pub(super) fn write_to_batch(self, batch: &mut DBBatch) -> Result<()> {
        for (key, event) in self.ref_events {
            self.store_index
                .put_segment_ref_event_batch(batch, key, &event)?;
        }
        for (segment_id, ops) in self.gc_overlay_ops {
            self.store_index
                .merge_segment_gc_overlay_batch(batch, segment_id, ops)?;
        }
        for from in self.removed_relocations {
            self.store_index.delete_gc_relocation_batch(batch, from)?;
        }
        Ok(())
    }

    /// Computes the exact aggregate summary change that the staged merge operands will publish.
    /// This reads only touched segments and is evaluated before the synced batch; the caller updates
    /// Prometheus only after that batch succeeds.
    pub(super) fn overlay_summary_delta(&self) -> Result<AccountingOverlayDelta> {
        let mut delta = AccountingOverlayDelta::default();
        for (segment_id, ops) in &self.gc_overlay_ops {
            let before = self
                .store_index
                .get_segment_gc_overlay(*segment_id)?
                .unwrap_or_default();
            let mut after = before.clone();
            after.apply_merge_ops(ops.clone());
            delta.total_bytes +=
                i128::from(after.summary.total_bytes) - i128::from(before.summary.total_bytes);
            delta.live_bytes +=
                i128::from(after.summary.live_bytes) - i128::from(before.summary.live_bytes);
            delta.retired_bytes +=
                i128::from(after.summary.retired_bytes) - i128::from(before.summary.retired_bytes);
            delta.expired_bytes +=
                i128::from(after.summary.expired_bytes) - i128::from(before.summary.expired_bytes);
            delta.live_ref_count += i128::from(after.summary.live_ref_count)
                - i128::from(before.summary.live_ref_count);
        }
        Ok(delta)
    }

    pub(super) fn event_counts(
        &self,
        event_batch: Option<&CompactionEventBatch>,
    ) -> AccountingEventCounts {
        let mut counts = AccountingEventCounts::default();
        if let Some(event_batch) = event_batch {
            for event in &event_batch.events {
                match event {
                    AccountingRefEvent::Live { .. } => {
                        counts.live = counts.live.saturating_add(1);
                    }
                    AccountingRefEvent::Retired { .. } => {
                        counts.retired = counts.retired.saturating_add(1);
                    }
                    AccountingRefEvent::LifecycleChanged { .. } => {
                        counts.lifecycle_changed = counts.lifecycle_changed.saturating_add(1);
                    }
                    AccountingRefEvent::Mapped { .. } => {
                        counts.mapped = counts.mapped.saturating_add(1);
                    }
                }
            }
        }
        counts.expired = self
            .ref_events
            .values()
            .filter(|event| matches!(event, SegmentRefEvent::Expired))
            .count() as u64;
        counts
    }

    /// Adds a newly materialized payload ref to the main-index accounting rows.
    fn add_ref(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        let epoch = self.epoch_at_lsn(lsn)?;
        if lifecycle_is_expired(lifecycle, epoch) {
            // Expired-on-arrival bytes are still part of the segment's physical footprint, but they
            // never enter a live bucket. The overlay summary records them as expired immediately.
            self.put_ref_event(lsn, record_ref, SegmentRefEvent::Expired);
            self.stage_overlay_op(
                record_ref.segment_id,
                SegmentGcOverlayMergeOp::AddExpiredBatch {
                    ranges: vec![SegmentGcRecordRange::from(record_ref)],
                },
            );
        } else {
            self.stage_overlay_op(
                record_ref.segment_id,
                SegmentGcOverlayMergeOp::AddLiveBatch {
                    records: vec![SegmentGcLiveRecord {
                        range: SegmentGcRecordRange::from(record_ref),
                        lifecycle,
                    }],
                },
            );
        }
        Ok(())
    }

    /// Retires a materialized payload ref.
    ///
    fn retire_ref(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        _lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        self.put_ref_event(lsn, record_ref, SegmentRefEvent::Retired);
        self.retire_overlay(record_ref);
        if let Some(relocation) = self.relocation_for(lsn, record_ref) {
            self.put_ref_event(lsn, relocation.to, SegmentRefEvent::Retired);
            self.add_retired_overlay(relocation.to);
        }
        Ok(())
    }

    /// Materializes a GC relocation after its `MapRef` reaches accounting.
    fn map_ref(
        &mut self,
        lsn: StrataLsn,
        from: RecordRef,
        to: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> Result<()> {
        self.put_ref_event(lsn, from, SegmentRefEvent::Retired);
        self.retire_overlay(from);
        self.add_ref(lsn, to, lifecycle)
    }

    /// Applies a lifecycle change for an already materialized payload ref.
    fn change_lifecycle(
        &mut self,
        lsn: StrataLsn,
        record_ref: RecordRef,
        old: Option<BlobLifecycle>,
        new: Option<BlobLifecycle>,
    ) -> Result<()> {
        if old == new {
            return Ok(());
        }

        let epoch = self.epoch_at_lsn(lsn)?;
        if lifecycle_is_expired(old, epoch) {
            return Ok(());
        }
        if lifecycle_is_expired(new, epoch) {
            self.expire_ref(lsn, record_ref);
        } else {
            self.set_lifetime_overlay(record_ref, new);
            self.put_ref_event(
                lsn,
                record_ref,
                SegmentRefEvent::LifecycleChanged { lifecycle: new },
            );
        }
        Ok(())
    }

    /// Applies an epoch change to live lifecycle buckets and copy-planning overlays.
    ///
    /// Failure example: if this only updated the overlay summary, GC publish reconciliation would
    /// not have exact per-range ref events for records copied from an expiring segment.
    fn expire_live_refs(&mut self, lsn: StrataLsn, epoch: Epoch) -> Result<()> {
        for (segment_id, _) in self.store_index.iter_segment_states()? {
            let mut overlay = self
                .store_index
                .get_segment_gc_overlay(segment_id)?
                .unwrap_or_default();
            if let Some(ops) = self.gc_overlay_ops.get(&segment_id) {
                overlay.apply_merge_ops(ops.clone());
            }
            let lifetimes = overlay.lifetimes;
            let mut expired = Vec::new();
            for lifetime in lifetimes {
                if lifetime.lifecycle.logical_end_epoch <= epoch {
                    self.put_ref_event_range(
                        lsn,
                        segment_id,
                        lifetime.range,
                        SegmentRefEvent::Expired,
                    );
                    expired.push(lifetime.range);
                }
            }
            if !expired.is_empty() {
                self.stage_overlay_op(
                    segment_id,
                    SegmentGcOverlayMergeOp::ExpireBatch { ranges: expired },
                );
            }
        }
        Ok(())
    }

    fn epoch_at_lsn(&self, lsn: StrataLsn) -> Result<Epoch> {
        self.store_index
            .latest_epoch_at_lsn(lsn)?
            .map(|(_, epoch)| epoch)
            .ok_or(Error::EpochNotInitialized)
    }

    /// Stages an ordered event for a physical record reference.
    ///
    /// Failure example: without events, GC publish reconciliation could not see that a copied source
    /// range was retired after the accounting snapshot used to prepare the plan.
    fn put_ref_event(&mut self, lsn: StrataLsn, record_ref: RecordRef, event: SegmentRefEvent) {
        self.put_ref_event_range(
            lsn,
            record_ref.segment_id,
            SegmentGcRecordRange::from(record_ref),
            event,
        );
    }

    /// Stages a ref event when the caller already has the segment-local range.
    ///
    /// Failure example: if events were keyed only by record offset, two segments with the same
    /// offset would collide and one event would disappear.
    fn put_ref_event_range(
        &mut self,
        lsn: StrataLsn,
        segment_id: SegmentId,
        range: SegmentGcRecordRange,
        event: SegmentRefEvent,
    ) {
        self.ref_events.insert(
            SegmentRefEventKey {
                segment_id,
                lsn,
                offset: range.offset,
            },
            event,
        );
    }

    /// Stages a GC overlay operation that marks this record range retired.
    ///
    /// Failure example: without the overlay, sealed-segment GC would have to re-resolve blob
    /// history for every candidate record instead of reading compact per-segment hints.
    fn retire_overlay(&mut self, record_ref: RecordRef) {
        self.retire_overlay_range(
            record_ref.segment_id,
            SegmentGcRecordRange::from(record_ref),
        )
    }

    /// Accounts a newly materialized range that was already terminal before its `MapRef` was
    /// materialized.
    fn add_retired_overlay(&mut self, record_ref: RecordRef) {
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::AddRetiredBatch {
                ranges: vec![SegmentGcRecordRange::from(record_ref)],
            },
        )
    }

    /// Stages a GC overlay retire operation when the caller already has a segment-local range.
    fn retire_overlay_range(&mut self, segment_id: SegmentId, range: SegmentGcRecordRange) {
        self.stage_overlay_op(
            segment_id,
            SegmentGcOverlayMergeOp::RetireBatch {
                ranges: vec![range],
            },
        )
    }

    /// Stages a lifecycle-expiry transition for one physical record.
    fn expire_ref(&mut self, lsn: StrataLsn, record_ref: RecordRef) {
        self.put_ref_event(lsn, record_ref, SegmentRefEvent::Expired);
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::ExpireBatch {
                ranges: vec![SegmentGcRecordRange::from(record_ref)],
            },
        );
    }

    /// Stages a GC overlay operation that records or clears a record's lifetime hint.
    ///
    /// Failure example: if lifetime changes updated only summary counters, GC could reclaim a
    /// record whose logical lifetime was extended after the original put.
    fn set_lifetime_overlay(&mut self, record_ref: RecordRef, lifecycle: Option<BlobLifecycle>) {
        self.stage_overlay_op(
            record_ref.segment_id,
            SegmentGcOverlayMergeOp::LifetimeBatch {
                updates: vec![SegmentGcLifetimeUpdate {
                    range: SegmentGcRecordRange::from(record_ref),
                    lifecycle,
                }],
            },
        )
    }

    /// Returns the active relocation for a source ref if the event happened before its publish LSN.
    fn relocation_for(&self, lsn: StrataLsn, record_ref: RecordRef) -> Option<GcRelocation> {
        self.relocations
            .get(&record_ref)
            .copied()
            .filter(|relocation| lsn < relocation.publish_lsn)
    }

    /// Drops relocation rows whose `MapRef` has reached the accounted frontier.
    pub(super) fn remove_relocations_through_lsn(&mut self, accounted_lsn: StrataLsn) {
        let expired = self
            .relocations
            .iter()
            .filter_map(|(from, relocation)| {
                (relocation.publish_lsn <= accounted_lsn).then_some(*from)
            })
            .collect::<Vec<_>>();
        for from in expired {
            self.relocations.remove(&from);
            self.removed_relocations.insert(from);
        }
    }

    /// Records an overlay merge operand for this commit.
    fn stage_overlay_op(&mut self, segment_id: SegmentId, op: SegmentGcOverlayMergeOp) {
        self.gc_overlay_ops.entry(segment_id).or_default().push(op);
    }
}
