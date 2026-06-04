use strata_core::{BlobLifecycle, PlacementClass, SegmentStats};

pub(crate) fn add_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    lifecycle: BlobLifecycle,
) {
    increment_histogram(
        &mut stats.future_epoch_histogram,
        lifecycle.logical_end_epoch,
    );
    increment_histogram(
        &mut stats.extension_count_histogram,
        lifecycle.extension_count,
    );
    if lifecycle_is_pinned(placement_class, lifecycle) {
        stats.pinned_bytes = stats.pinned_bytes.saturating_add(record_len);
    }
    refresh_live_epoch_bounds(stats);
}

fn lifecycle_is_pinned(placement_class: PlacementClass, lifecycle: BlobLifecycle) -> bool {
    match placement_class {
        PlacementClass::ExactEpoch(physical_epoch) => lifecycle.logical_end_epoch > physical_epoch,
        PlacementClass::Ingest | PlacementClass::Spillover => false,
    }
}

fn increment_histogram<K>(histogram: &mut std::collections::BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    *histogram.entry(key).or_default() += 1;
}

fn refresh_live_epoch_bounds(stats: &mut SegmentStats) {
    stats.min_live_end_epoch = stats.future_epoch_histogram.keys().next().copied();
    stats.max_live_end_epoch = stats.future_epoch_histogram.keys().next_back().copied();
}
