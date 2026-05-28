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

pub(crate) fn update_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    old_lifecycle: BlobLifecycle,
    new_lifecycle: BlobLifecycle,
) {
    if old_lifecycle.logical_end_epoch != new_lifecycle.logical_end_epoch {
        decrement_histogram(
            &mut stats.future_epoch_histogram,
            old_lifecycle.logical_end_epoch,
        );
        increment_histogram(
            &mut stats.future_epoch_histogram,
            new_lifecycle.logical_end_epoch,
        );
        refresh_live_epoch_bounds(stats);
    }

    if old_lifecycle.extension_count != new_lifecycle.extension_count {
        decrement_histogram(
            &mut stats.extension_count_histogram,
            old_lifecycle.extension_count,
        );
        increment_histogram(
            &mut stats.extension_count_histogram,
            new_lifecycle.extension_count,
        );
    }

    match (
        lifecycle_is_pinned(placement_class, old_lifecycle),
        lifecycle_is_pinned(placement_class, new_lifecycle),
    ) {
        (false, true) => {
            stats.pinned_bytes = stats.pinned_bytes.saturating_add(record_len);
        }
        (true, false) => {
            stats.pinned_bytes = stats.pinned_bytes.saturating_sub(record_len);
        }
        _ => {}
    }
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

fn decrement_histogram<K>(histogram: &mut std::collections::BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    match histogram.get_mut(&key) {
        Some(count) if *count > 1 => *count -= 1,
        Some(_) => {
            histogram.remove(&key);
        }
        None => {}
    }
}

fn refresh_live_epoch_bounds(stats: &mut SegmentStats) {
    stats.min_live_end_epoch = stats.future_epoch_histogram.keys().next().copied();
    stats.max_live_end_epoch = stats.future_epoch_histogram.keys().next_back().copied();
}
