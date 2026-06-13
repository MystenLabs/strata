use strata_core::{BlobLifecycle, Epoch, PlacementClass, SegmentStats};

pub(crate) fn add_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    stats.live_bytes = stats.live_bytes.saturating_add(record_len);
    stats.live_ref_count = stats.live_ref_count.saturating_add(1);
    let Some(lifecycle) = lifecycle else {
        stats.unknown_lifetime_bytes = stats.unknown_lifetime_bytes.saturating_add(record_len);
        stats.unknown_lifetime_ref_count = stats.unknown_lifetime_ref_count.saturating_add(1);
        return;
    };
    let bucket = stats
        .future_epoch_histogram
        .entry(lifecycle.logical_end_epoch)
        .or_default();
    bucket.refs = bucket.refs.saturating_add(1);
    bucket.bytes = bucket.bytes.saturating_add(record_len);
    increment_histogram(
        &mut stats.extension_count_histogram,
        lifecycle.extension_count,
    );
    if lifecycle_is_pinned(placement_class, lifecycle) {
        stats.pinned_bytes = stats.pinned_bytes.saturating_add(record_len);
    }
    refresh_live_epoch_bounds(stats);
}

pub(crate) fn remove_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    stats.live_bytes = stats.live_bytes.saturating_sub(record_len);
    stats.live_ref_count = stats.live_ref_count.saturating_sub(1);
    let Some(lifecycle) = lifecycle else {
        stats.unknown_lifetime_bytes = stats.unknown_lifetime_bytes.saturating_sub(record_len);
        stats.unknown_lifetime_ref_count = stats.unknown_lifetime_ref_count.saturating_sub(1);
        return;
    };
    if let Some(bucket) = stats
        .future_epoch_histogram
        .get_mut(&lifecycle.logical_end_epoch)
    {
        bucket.refs = bucket.refs.saturating_sub(1);
        bucket.bytes = bucket.bytes.saturating_sub(record_len);
        if bucket.refs == 0 {
            stats
                .future_epoch_histogram
                .remove(&lifecycle.logical_end_epoch);
        }
    }
    decrement_histogram(
        &mut stats.extension_count_histogram,
        lifecycle.extension_count,
    );
    if lifecycle_is_pinned(placement_class, lifecycle) {
        stats.pinned_bytes = stats.pinned_bytes.saturating_sub(record_len);
    }
    refresh_live_epoch_bounds(stats);
}

/// Folds every histogram bucket with end epoch at or below `epoch` into the expired counters.
///
/// Returns true when any bucket was drained. After this runs, every remaining bucket has an end
/// epoch greater than `epoch`, so live counters only describe refs that can still be read.
pub(crate) fn expire_live_lifecycle_stats_through(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    epoch: Epoch,
) -> bool {
    let retained = match epoch.checked_add(1) {
        Some(first_live_epoch) => stats.future_epoch_histogram.split_off(&first_live_epoch),
        None => Default::default(),
    };
    let expired = std::mem::replace(&mut stats.future_epoch_histogram, retained);
    if expired.is_empty() {
        return false;
    }

    for (end_epoch, bucket) in expired {
        stats.live_ref_count = stats.live_ref_count.saturating_sub(bucket.refs);
        stats.live_bytes = stats.live_bytes.saturating_sub(bucket.bytes);
        stats.expired_bytes = stats.expired_bytes.saturating_add(bucket.bytes);
        if let PlacementClass::ExactEpoch(physical_epoch) = placement_class
            && end_epoch > physical_epoch
        {
            stats.pinned_bytes = stats.pinned_bytes.saturating_sub(bucket.bytes);
        }
    }
    refresh_live_epoch_bounds(stats);
    true
}

pub(crate) fn lifecycle_is_expired(lifecycle: Option<BlobLifecycle>, current_epoch: Epoch) -> bool {
    lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch)
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
    let Some(count) = histogram.get_mut(&key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        histogram.remove(&key);
    }
}

fn refresh_live_epoch_bounds(stats: &mut SegmentStats) {
    stats.min_live_end_epoch = stats.future_epoch_histogram.keys().next().copied();
    stats.max_live_end_epoch = stats.future_epoch_histogram.keys().next_back().copied();
}
