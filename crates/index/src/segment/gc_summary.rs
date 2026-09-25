use std::collections::BTreeMap;

use crate::port::{
    codec::{decode_in_merge, encode_in_merge, encode_value},
    map::IndexBatch,
    options::default_db_options,
};
use core_types::{SegmentGcSummary, SegmentGcSummaryDelta, SegmentId};
use rocksdb::MergeOperands;

use crate::{Result, StrataIndex};

impl StrataIndex {
    pub fn get_segment_gc_summary(
        &self,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentGcSummary>> {
        self.segment_gc_summaries.get(&segment_id)
    }

    pub fn put_segment_gc_summary_batch(
        &self,
        batch: &mut IndexBatch,
        segment_id: SegmentId,
        summary: &SegmentGcSummary,
    ) -> Result<()> {
        batch.insert_batch(self.segment_gc_summaries(), [(&segment_id, summary)])?;
        Ok(())
    }

    pub fn merge_segment_gc_summary_batch(
        &self,
        batch: &mut IndexBatch,
        segment_id: SegmentId,
        delta: &SegmentGcSummaryDelta,
    ) -> Result<()> {
        if delta == &SegmentGcSummaryDelta::default() {
            return Ok(());
        }
        let operand = encode_value(delta)?;
        batch.partial_merge_batch(self.segment_gc_summaries(), [(&segment_id, operand)])?;
        Ok(())
    }
}

pub(crate) fn segment_gc_summaries_cf_options() -> rocksdb::Options {
    let mut options = default_db_options();
    options.set_merge_operator(
        GC_SUMMARY_MERGE,
        |key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands| {
            merge_segment_gc_summary(key, existing, operands)
        },
        // Combining signed deltas is an optional optimization. Keeping operands separate makes the
        // initial implementation and its overflow behavior easier to audit.
        |_key: &[u8], _existing: Option<&[u8]>, _operands: &MergeOperands| None,
    );
    options
}

/// Name reported in panic messages, and the operator name RocksDB records.
const GC_SUMMARY_MERGE: &str = "strata-segment-gc-summary-merge";

/// Folds signed deltas into a segment's GC summary.
///
/// Decode failures crash the process rather than returning `None`; see
/// [`crate::port::codec::decode_in_merge`] for why silence is the worse option there. Arithmetic
/// failures are different and still return `None`, because an un-appliable delta is a defined
/// outcome rather than a corrupt input.
fn merge_segment_gc_summary(
    key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut summary = match existing {
        Some(bytes) => decode_in_merge::<SegmentGcSummary>(GC_SUMMARY_MERGE, key, bytes),
        None => SegmentGcSummary::default(),
    };
    for operand in operands {
        let delta = decode_in_merge::<SegmentGcSummaryDelta>(GC_SUMMARY_MERGE, key, operand);
        // Unlike a decode failure, an arithmetic overflow here is an expected outcome with defined
        // behaviour: a delta from an abandoned batch can legitimately fail to apply, and the merge
        // is meant to fail rather than crash. See the abandoned-batch case in
        // `tests/segment_gc_summary.rs`.
        apply_segment_gc_summary_delta(&mut summary, &delta)?;
    }
    Some(encode_in_merge(GC_SUMMARY_MERGE, key, &summary))
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
