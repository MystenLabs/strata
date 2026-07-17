use strata_accounting::{ActiveDeltaLogReadCursor, CompactionEventBatch, Manifest};
use strata_core::{SegmentFileState, ShardCleanupState, StrataLsn};
use strata_index::StrataIndex;

use crate::Result;

use super::{AccountingProcessor, AccountingProjection};

impl AccountingProcessor {
    /// Publishes accounting metadata to the store index and fsyncs the RocksDB WAL.
    ///
    /// Manifest and cursor move together. Derived ref rows, GC overlay operands, and frontier
    /// movement share the same batch so retries cannot duplicate or lose accounting effects.
    pub(super) fn publish_accounting_transition(
        &self,
        manifest: Option<&Manifest>,
        cursor: Option<ActiveDeltaLogReadCursor>,
        event_batch: Option<&CompactionEventBatch>,
    ) -> Result<bool> {
        let mut projection = AccountingProjection::new(&self.store_index)?;
        if let Some(event_batch) = event_batch {
            projection.apply_compaction_event_batch(event_batch)?;
        }
        let frontier =
            match manifest {
                Some(manifest) => Some(projection.advance_frontier(manifest, |key| {
                    self.accounting_index.partition_for_key(key)
                })?),
                None => None,
            };

        let mut batch = self.store_index.batch();
        if let Some(manifest) = manifest {
            self.store_index
                .put_accounting_index_manifest_batch(&mut batch, manifest)?;
        }
        if let Some(cursor) = cursor {
            self.store_index
                .put_accounting_active_delta_log_consumed_cursor_batch(&mut batch, cursor)?;
        }
        let current_accounted_lsn = self.store_index.get_accounted_lsn()?;
        if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            projection.remove_relocations_through_lsn(frontier.accounted_lsn);
        }
        let should_nudge_gc = if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            !frontier.completed_shard_drops.is_empty()
                || frontier.materialized_epoch_change
                || accounting_frontier_unblocks_empty_delete(
                    &self.store_index,
                    &projection,
                    current_accounted_lsn,
                    frontier.accounted_lsn,
                )?
        } else {
            false
        };
        let overlay_delta = projection.overlay_summary_delta()?;
        let event_counts = projection.event_counts(event_batch);
        projection.write_to_batch(&mut batch)?;
        if let Some(frontier) = frontier.as_ref()
            && frontier.accounted_lsn > current_accounted_lsn
        {
            self.store_index
                .remove_unaccounted_lsn_ops_batch(&mut batch, &frontier.consumed_lsns)?;
            self.store_index
                .put_accounted_lsn_batch(&mut batch, frontier.accounted_lsn)?;
            for shard in &frontier.completed_shard_drops {
                if let Some(mut job) = self.store_index.get_shard_cleanup_job(*shard)? {
                    job.state = ShardCleanupState::ReadyForGc;
                    self.store_index
                        .put_shard_cleanup_job_batch(&mut batch, job)?;
                }
            }
        }
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        let published_accounted_lsn = frontier
            .as_ref()
            .map_or(current_accounted_lsn, |frontier| frontier.accounted_lsn);
        self.metrics.publish_accounting_state(
            published_accounted_lsn,
            manifest,
            overlay_delta,
            event_counts,
        );
        Ok(should_nudge_gc)
    }
}

fn accounting_frontier_unblocks_empty_delete(
    store_index: &StrataIndex,
    projection: &AccountingProjection<'_>,
    previous_lsn: StrataLsn,
    accounted_lsn: StrataLsn,
) -> Result<bool> {
    for (_, state) in store_index.iter_segment_states()? {
        let may_have_become_empty = match state.state {
            SegmentFileState::GcRelocating => true,
            SegmentFileState::Sealed => state
                .max_lsn
                .is_some_and(|max_lsn| previous_lsn < max_lsn && max_lsn <= accounted_lsn),
            _ => false,
        };
        if may_have_become_empty {
            let mut overlay = store_index
                .get_segment_gc_overlay(state.segment_id)?
                .unwrap_or_default();
            if let Some(ops) = projection.gc_overlay_ops.get(&state.segment_id) {
                overlay.apply_merge_ops(ops.clone());
            }
            if overlay.summary.live_ref_count == 0 {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
