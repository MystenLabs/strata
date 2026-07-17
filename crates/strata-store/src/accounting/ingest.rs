use std::{collections::BTreeSet, time::Instant};

use strata_accounting::{AccountingLogDurablePosition, ActiveDeltaLog, ActiveDeltaLogReadCursor};
use strata_core::SegmentFileState;

use crate::{Error, Result, metrics::AccountingStage};

use super::{AccountingProcessor, compaction::count_threshold_reached};

impl AccountingProcessor {
    /// Copies durable active-log entries into immutable accounting runs and advances the read cursor.
    pub(super) fn ingest_active_delta_log(&mut self, force: bool) -> Result<bool> {
        let Some(durable_position) = self.accounting_log_durable_position()? else {
            return Ok(false);
        };
        let cursor = self.active_delta_log_read_cursor()?;
        // Retry cleanup from an already durable cursor before doing more work. This makes a prior
        // unlink failure recoverable without needing another cursor transition.
        self.reclaim_consumed_delta_logs(cursor)?;
        let started = Instant::now();
        let read = match ActiveDeltaLog::read_durable_range(
            self.config.accounting_index_dir(),
            cursor,
            durable_position,
        ) {
            Ok(read) => read,
            Err(error) => {
                self.metrics.record_accounting_stage(
                    AccountingStage::Ingest,
                    false,
                    started.elapsed(),
                    0,
                    0,
                );
                return Err(error.into());
            }
        };
        let next_cursor = read.next_cursor(cursor);
        if read.entries.is_empty() {
            if next_cursor != cursor {
                let _ = self.publish_accounting_transition(None, Some(next_cursor), None)?;
                self.reclaim_consumed_delta_logs(next_cursor)?;
            }
            return Ok(false);
        }
        if !force
            && !count_threshold_reached(
                read.entries.len(),
                self.config.accounting_ingest_record_threshold,
            )
        {
            // Threshold zero disables this trigger; forced passes eventually ingest small batches.
            return Ok(false);
        }

        let input_bytes = read.bytes_read;
        let prepared = match self
            .accounting_index
            .prepare_accounting_deltas(read.entries)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.metrics.record_accounting_stage(
                    AccountingStage::Ingest,
                    false,
                    started.elapsed(),
                    input_bytes,
                    0,
                );
                return Err(error.into());
            }
        };
        let output_bytes = prepared
            .delta_metas
            .iter()
            .map(|run| run.file_len)
            .sum::<u64>();
        let manifest = prepared.manifest().clone();
        // Publish manifest + cursor before applying the in-memory manifest. The prepared run files
        // are already synced, so recovery observes the new manifest and cursor together.
        let should_nudge_gc =
            match self.publish_accounting_transition(Some(&manifest), Some(next_cursor), None) {
                Ok(should_nudge_gc) => should_nudge_gc,
                Err(error) => {
                    self.metrics.record_accounting_stage(
                        AccountingStage::Ingest,
                        false,
                        started.elapsed(),
                        input_bytes,
                        output_bytes,
                    );
                    return Err(error);
                }
            };
        self.metrics.record_accounting_stage(
            AccountingStage::Ingest,
            true,
            started.elapsed(),
            input_bytes,
            output_bytes,
        );
        self.accounting_index
            .apply_prepared_accounting_deltas(prepared)?;
        self.reclaim_consumed_delta_logs(next_cursor)?;
        Ok(should_nudge_gc)
    }

    /// Reclaims logs whose durable accounting handoff and data-segment seal are both complete.
    fn reclaim_consumed_delta_logs(&self, cursor: ActiveDeltaLogReadCursor) -> Result<()> {
        if cursor.segment_id == 0 {
            return Ok(());
        }
        let reclaimable = self
            .store_index
            .iter_segment_states()?
            .into_iter()
            .filter_map(|(segment_id, state)| {
                (segment_id < cursor.segment_id
                    && matches!(
                        state.state,
                        SegmentFileState::Sealed | SegmentFileState::Deleted
                    ))
                .then_some(segment_id)
            })
            .collect::<BTreeSet<_>>();
        ActiveDeltaLog::remove_segments(self.config.accounting_index_dir(), &reclaimable)?;
        Ok(())
    }

    /// Reads the foreground-published durable active-log frontier.
    fn accounting_log_durable_position(&self) -> Result<Option<AccountingLogDurablePosition>> {
        Ok(self.store_index.get_accounting_log_durable_position()?)
    }

    /// Reads where accounting ingestion last stopped, defaulting to the beginning.
    fn active_delta_log_read_cursor(&self) -> Result<ActiveDeltaLogReadCursor> {
        self.store_index
            .get_accounting_active_delta_log_consumed_cursor()
            .map(|cursor| cursor.unwrap_or_default())
            .map_err(Error::from)
    }
}
