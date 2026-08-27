use std::collections::BTreeMap;

use core_types::{SegmentGcSummary, SegmentGcSummaryDelta, SegmentId};
use rocksdb::MergeOperands;
use typed_store::{
    Map,
    rocks::{DBBatch, default_db_options},
};

use crate::{Error, Result, StrataIndex};

impl StrataIndex {
    pub fn get_segment_gc_summary(
        &self,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentGcSummary>> {
        Ok(self.segment_gc_summaries.get(&segment_id)?)
    }

    pub fn put_segment_gc_summary_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        summary: &SegmentGcSummary,
    ) -> Result<()> {
        batch.insert_batch(self.segment_gc_summaries(), [(&segment_id, summary)])?;
        Ok(())
    }

    pub fn merge_segment_gc_summary_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        delta: &SegmentGcSummaryDelta,
    ) -> Result<()> {
        if delta == &SegmentGcSummaryDelta::default() {
            return Ok(());
        }
        let operand =
            bcs::to_bytes(delta).map_err(|error| Error::Serialization(error.to_string()))?;
        batch.partial_merge_batch(self.segment_gc_summaries(), [(&segment_id, operand)])?;
        Ok(())
    }
}

pub(crate) fn segment_gc_summaries_cf_options() -> rocksdb::Options {
    let mut options = default_db_options().options;
    options.set_merge_operator(
        "strata-segment-gc-summary-merge",
        |_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands| {
            merge_segment_gc_summary(existing, operands)
        },
        // Combining signed deltas is an optional optimization. Keeping operands separate makes the
        // initial implementation and its overflow behavior easier to audit.
        |_key: &[u8], _existing: Option<&[u8]>, _operands: &MergeOperands| None,
    );
    options
}

fn merge_segment_gc_summary(existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut summary = match existing {
        Some(bytes) => bcs::from_bytes::<SegmentGcSummary>(bytes).ok()?,
        None => SegmentGcSummary::default(),
    };
    for operand in operands {
        let delta = bcs::from_bytes::<SegmentGcSummaryDelta>(operand).ok()?;
        apply_segment_gc_summary_delta(&mut summary, &delta)?;
    }
    bcs::to_bytes(&summary).ok()
}

pub(crate) fn apply_segment_gc_summary_delta(
    summary: &mut SegmentGcSummary,
    delta: &SegmentGcSummaryDelta,
) -> Option<()> {
    summary.total_bytes = add_signed(summary.total_bytes, delta.total_bytes)?;
    summary.live_bytes = add_signed(summary.live_bytes, delta.live_bytes)?;
    summary.retired_bytes = add_signed(summary.retired_bytes, delta.retired_bytes)?;
    summary.expired_bytes = add_signed(summary.expired_bytes, delta.expired_bytes)?;
    summary.live_ref_count = add_signed(summary.live_ref_count, delta.live_ref_count)?;
    summary.unknown_lifetime_bytes =
        add_signed(summary.unknown_lifetime_bytes, delta.unknown_lifetime_bytes)?;
    summary.unknown_lifetime_ref_count = add_signed(
        summary.unknown_lifetime_ref_count,
        delta.unknown_lifetime_ref_count,
    )?;

    for (&epoch, &change) in &delta.epoch_bytes {
        let bucket = summary.future_epoch_histogram.entry(epoch).or_default();
        bucket.bytes = add_signed(bucket.bytes, change)?;
    }
    for (&epoch, &change) in &delta.epoch_refs {
        let bucket = summary.future_epoch_histogram.entry(epoch).or_default();
        bucket.refs = add_signed(bucket.refs, change)?;
    }
    if summary
        .future_epoch_histogram
        .values()
        .any(|bucket| (bucket.refs == 0) != (bucket.bytes == 0))
    {
        return None;
    }
    summary
        .future_epoch_histogram
        .retain(|_, bucket| bucket.refs != 0);
    summary.min_live_end_epoch = summary.future_epoch_histogram.keys().next().copied();
    summary.max_live_end_epoch = summary.future_epoch_histogram.keys().next_back().copied();

    apply_histogram(
        &mut summary.extension_count_histogram,
        &delta.extension_counts,
    )
}

fn apply_histogram<K: Copy + Ord>(
    histogram: &mut BTreeMap<K, u64>,
    changes: &BTreeMap<K, i128>,
) -> Option<()> {
    for (&key, &change) in changes {
        let value = add_signed(histogram.get(&key).copied().unwrap_or_default(), change)?;
        if value == 0 {
            histogram.remove(&key);
        } else {
            histogram.insert(key, value);
        }
    }
    Some(())
}

fn add_signed(value: u64, change: i128) -> Option<u64> {
    u64::try_from(i128::from(value).checked_add(change)?).ok()
}
