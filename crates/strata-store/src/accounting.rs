//! Accounting is the store's background derived-state engine.
//!
//! The foreground writer appends ordered deltas. The accounting worker ingests those deltas,
//! compacts the file-backed accounting index, projects logical ref events into segment-oriented GC
//! state, and advances the durable accounting frontier.
//!
//! See `docs/accounting.md` for the end-to-end flow, vocabulary, publication protocol, and source
//! map.

use strata_core::{SegmentFileState, SegmentGcSummary};
use strata_index::StrataIndex;

use crate::Result;

mod compaction;
mod event_projection;
mod ingest;
mod processor;
mod publish;
mod requests;
mod worker;

pub(crate) use requests::{AccountingCommand, AccountingRequestSender, accounting_request_channel};
pub(crate) use worker::AccountingWorker;

use event_projection::AccountingProjection;
use processor::AccountingProcessor;
use requests::{
    AccountingPassMode, AccountingPassPolicy, rearm_materialization, take_pending_run_mode,
};

#[cfg(test)]
pub(crate) use processor::{
    run_accounting_materializing_once, run_accounting_nudged_once, run_accounting_once,
};

/// Aggregates persisted per-segment accounting summaries for metric initialization and snapshots.
pub(crate) fn gc_known_summary(store_index: &StrataIndex) -> Result<SegmentGcSummary> {
    let mut total = SegmentGcSummary::default();
    for (segment_id, state) in store_index.iter_segment_states()? {
        if state.state == SegmentFileState::Deleted {
            continue;
        }
        let Some(overlay) = store_index.get_segment_gc_overlay(segment_id)? else {
            continue;
        };
        total.total_bytes = total
            .total_bytes
            .saturating_add(overlay.summary.total_bytes);
        total.live_bytes = total.live_bytes.saturating_add(overlay.summary.live_bytes);
        total.retired_bytes = total
            .retired_bytes
            .saturating_add(overlay.summary.retired_bytes);
        total.expired_bytes = total
            .expired_bytes
            .saturating_add(overlay.summary.expired_bytes);
        total.live_ref_count = total
            .live_ref_count
            .saturating_add(overlay.summary.live_ref_count);
    }
    Ok(total)
}
