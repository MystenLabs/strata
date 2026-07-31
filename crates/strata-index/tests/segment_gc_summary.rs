use std::collections::BTreeMap;

use strata_core::{EpochBucket, SegmentGcSummary, SegmentGcSummaryDelta};
use strata_index::StrataIndex;
use tempfile::TempDir;
use typed_store::DBMetrics;

const SEGMENT_ID: u64 = 17;

#[tokio::test]
async fn summary_deltas_merge_survive_reopen_and_ignore_abandoned_batches() {
    DBMetrics::get();
    let directory = TempDir::new().unwrap();
    let metric_suffix = directory.path().display().to_string();
    let index = StrataIndex::open_path(directory.path(), "strata", &metric_suffix).unwrap();

    // Three hundred live bytes plus one hundred bytes that were already expired when materialized.
    let initial = SegmentGcSummaryDelta {
        total_bytes: 400,
        live_bytes: 300,
        expired_bytes: 100,
        live_ref_count: 2,
        unknown_lifetime_bytes: 100,
        unknown_lifetime_ref_count: 1,
        epoch_bytes: BTreeMap::from([(50, 200)]),
        epoch_refs: BTreeMap::from([(50, 1)]),
        extension_counts: BTreeMap::from([(0, 1)]),
        ..Default::default()
    };
    let mut batch = index.batch();
    index
        .merge_segment_gc_summary_batch(&mut batch, SEGMENT_ID, &initial)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    // Retire the epoch-50 ref and assign the previously unknown ref to epoch 60.
    let transition = SegmentGcSummaryDelta {
        live_bytes: -200,
        retired_bytes: 200,
        live_ref_count: -1,
        unknown_lifetime_bytes: -100,
        unknown_lifetime_ref_count: -1,
        epoch_bytes: BTreeMap::from([(50, -200), (60, 100)]),
        epoch_refs: BTreeMap::from([(50, -1), (60, 1)]),
        extension_counts: BTreeMap::from([(0, -1), (1, 1)]),
        ..Default::default()
    };
    let mut batch = index.batch();
    index
        .merge_segment_gc_summary_batch(&mut batch, SEGMENT_ID, &transition)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let expected = SegmentGcSummary {
        total_bytes: 400,
        live_bytes: 100,
        retired_bytes: 200,
        expired_bytes: 100,
        live_ref_count: 1,
        min_live_end_epoch: Some(60),
        max_live_end_epoch: Some(60),
        future_epoch_histogram: BTreeMap::from([(
            60,
            EpochBucket {
                refs: 1,
                bytes: 100,
            },
        )]),
        extension_count_histogram: BTreeMap::from([(1, 1)]),
        ..Default::default()
    };
    assert_eq!(
        index.get_segment_gc_summary(SEGMENT_ID).unwrap(),
        Some(expected.clone())
    );

    // A prepared expiry has no effect unless its RocksDB batch is written.
    let expiry = SegmentGcSummaryDelta {
        live_bytes: -100,
        expired_bytes: 100,
        live_ref_count: -1,
        epoch_bytes: BTreeMap::from([(60, -100)]),
        epoch_refs: BTreeMap::from([(60, -1)]),
        extension_counts: BTreeMap::from([(1, -1)]),
        ..Default::default()
    };
    let mut abandoned = index.batch();
    index
        .merge_segment_gc_summary_batch(&mut abandoned, SEGMENT_ID, &expiry)
        .unwrap();
    drop(abandoned);
    drop(index);

    let reopened = StrataIndex::open_path(directory.path(), "strata", &metric_suffix).unwrap();
    assert_eq!(
        reopened.get_segment_gc_summary(SEGMENT_ID).unwrap(),
        Some(expected)
    );

    let mut batch = reopened.batch();
    reopened
        .merge_segment_gc_summary_batch(
            &mut batch,
            1,
            &SegmentGcSummaryDelta {
                retired_bytes: -1,
                ..Default::default()
            },
        )
        .unwrap();
    reopened
        .merge_segment_gc_summary_batch(
            &mut batch,
            2,
            &SegmentGcSummaryDelta {
                total_bytes: i128::from(u64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    reopened
        .merge_segment_gc_summary_batch(
            &mut batch,
            2,
            &SegmentGcSummaryDelta {
                total_bytes: 1,
                ..Default::default()
            },
        )
        .unwrap();
    batch.write_with_sync(true).unwrap();

    assert!(reopened.get_segment_gc_summary(1).is_err());
    assert!(reopened.get_segment_gc_summary(2).is_err());
}
