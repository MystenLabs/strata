//! Differential tests for the indexed overlay fold.
//!
//! `ReferenceOverlayFold` is the previous per-event implementation, kept verbatim so random
//! operation sequences can prove the indexed fold produces identical overlays and summaries.

use std::time::{Duration, Instant};

use super::*;

trait ReferenceOverlayFold {
    fn reference_apply_merge_ops(&mut self, ops: impl IntoIterator<Item = SegmentGcOverlayMergeOp>);
    fn reference_apply_merge_op(&mut self, op: SegmentGcOverlayMergeOp);
    fn apply_merge_op_unchecked(&mut self, op: SegmentGcOverlayMergeOp);
    fn add_live_record(&mut self, record: SegmentGcLiveRecord);
    fn add_retired_record(&mut self, range: SegmentGcRecordRange);
    fn add_expired_record(&mut self, range: SegmentGcRecordRange);
    fn expire_range(&mut self, range: SegmentGcRecordRange);
    fn retire_range(&mut self, range: SegmentGcRecordRange);
    fn apply_lifetime_update(&mut self, update: SegmentGcLifetimeUpdate);
    fn lifecycle_for_range(&self, range: SegmentGcRecordRange) -> Option<BlobLifecycle>;
    fn normalize(&mut self);
}

impl ReferenceOverlayFold for SegmentGcOverlay {
    /// Applies GC overlay merge operations in commit order and leaves the overlay canonical.
    fn reference_apply_merge_ops(
        &mut self,
        ops: impl IntoIterator<Item = SegmentGcOverlayMergeOp>,
    ) {
        for op in ops {
            self.apply_merge_op_unchecked(op);
        }
        self.normalize();
    }

    /// Applies one GC overlay merge operation and leaves the overlay canonical.
    fn reference_apply_merge_op(&mut self, op: SegmentGcOverlayMergeOp) {
        self.apply_merge_op_unchecked(op);
        self.normalize();
    }

    fn apply_merge_op_unchecked(&mut self, op: SegmentGcOverlayMergeOp) {
        match op {
            SegmentGcOverlayMergeOp::AddLiveBatch { records } => {
                for record in records {
                    self.add_live_record(record);
                }
            }
            SegmentGcOverlayMergeOp::AddRetiredBatch { ranges } => {
                for range in ranges {
                    self.add_retired_record(range);
                }
            }
            SegmentGcOverlayMergeOp::AddExpiredBatch { ranges } => {
                for range in ranges {
                    self.add_expired_record(range);
                }
            }
            SegmentGcOverlayMergeOp::ExpireBatch { ranges } => {
                for range in ranges {
                    self.expire_range(range);
                }
            }
            SegmentGcOverlayMergeOp::RetireBatch { ranges } => {
                for range in ranges {
                    self.retire_range(range);
                }
            }
            SegmentGcOverlayMergeOp::LifetimeBatch { updates } => {
                for update in updates {
                    self.apply_lifetime_update(update);
                }
            }
        }
    }

    fn add_live_record(&mut self, record: SegmentGcLiveRecord) {
        if is_empty_gc_range(record.range) {
            return;
        }

        subtract_gc_range(&mut self.expired, record.range);
        subtract_gc_range(&mut self.retired, record.range);
        remove_lifetimes_overlapping(&mut self.lifetimes, record.range);
        add_live_summary(&mut self.summary, record.range.len, record.lifecycle);
        self.summary.total_bytes = self.summary.total_bytes.saturating_add(record.range.len);
        if let Some(lifecycle) = record.lifecycle {
            self.lifetimes.push(SegmentGcLifetimeRange {
                range: record.range,
                lifecycle,
            });
        }
    }

    fn add_retired_record(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) {
            return;
        }

        if self
            .retired
            .iter()
            .any(|retired| range_contains(*retired, range))
        {
            return;
        }

        let was_expired = self
            .expired
            .iter()
            .any(|expired| range_contains(*expired, range));
        if was_expired {
            subtract_gc_range(&mut self.expired, range);
            self.summary.expired_bytes = self.summary.expired_bytes.saturating_sub(range.len);
        } else {
            self.summary.total_bytes = self.summary.total_bytes.saturating_add(range.len);
        }
        subtract_gc_range(&mut self.expired, range);
        remove_lifetimes_overlapping(&mut self.lifetimes, range);
        self.summary.retired_bytes = self.summary.retired_bytes.saturating_add(range.len);
        self.retired.push(range);
        coalesce_gc_ranges(&mut self.retired);
    }

    fn add_expired_record(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) {
            return;
        }

        if self
            .retired
            .iter()
            .any(|retired| range_contains(*retired, range))
        {
            return;
        }
        subtract_gc_range(&mut self.retired, range);
        remove_lifetimes_overlapping(&mut self.lifetimes, range);
        self.summary.total_bytes = self.summary.total_bytes.saturating_add(range.len);
        self.summary.expired_bytes = self.summary.expired_bytes.saturating_add(range.len);
        self.expired.push(range);
        coalesce_gc_ranges(&mut self.expired);
    }

    fn expire_range(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range)
            || self
                .retired
                .iter()
                .any(|retired| range_contains(*retired, range))
            || self
                .expired
                .iter()
                .any(|expired| range_contains(*expired, range))
        {
            return;
        }

        let lifecycle = self.lifecycle_for_range(range);
        remove_live_summary(&mut self.summary, range.len, lifecycle);
        self.summary.expired_bytes = self.summary.expired_bytes.saturating_add(range.len);
        remove_lifetimes_overlapping(&mut self.lifetimes, range);
        self.expired.push(range);
        coalesce_gc_ranges(&mut self.expired);
    }

    fn retire_range(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range)
            || self
                .retired
                .iter()
                .any(|retired| range_contains(*retired, range))
        {
            return;
        }

        if self
            .expired
            .iter()
            .any(|expired| range_contains(*expired, range))
        {
            self.summary.expired_bytes = self.summary.expired_bytes.saturating_sub(range.len);
            self.summary.retired_bytes = self.summary.retired_bytes.saturating_add(range.len);
            subtract_gc_range(&mut self.expired, range);
        } else {
            let lifecycle = self.lifecycle_for_range(range);
            remove_live_summary(&mut self.summary, range.len, lifecycle);
            self.summary.retired_bytes = self.summary.retired_bytes.saturating_add(range.len);
        }

        remove_lifetimes_overlapping(&mut self.lifetimes, range);
        self.retired.push(range);
        coalesce_gc_ranges(&mut self.retired);
    }

    fn apply_lifetime_update(&mut self, update: SegmentGcLifetimeUpdate) {
        if is_empty_gc_range(update.range) {
            return;
        }

        if self
            .retired
            .iter()
            .any(|retired| range_contains(*retired, update.range))
        {
            return;
        }

        if self
            .expired
            .iter()
            .any(|expired| range_contains(*expired, update.range))
        {
            return;
        }

        let old = self.lifecycle_for_range(update.range);
        if old == update.lifecycle {
            return;
        }
        remove_live_summary(&mut self.summary, update.range.len, old);
        add_live_summary(&mut self.summary, update.range.len, update.lifecycle);
        remove_lifetimes_overlapping(&mut self.lifetimes, update.range);

        if let Some(lifecycle) = update.lifecycle {
            self.lifetimes.push(SegmentGcLifetimeRange {
                range: update.range,
                lifecycle,
            });
            self.lifetimes
                .sort_by_key(|entry| (entry.range.offset, entry.range.len));
        }
    }

    fn lifecycle_for_range(&self, range: SegmentGcRecordRange) -> Option<BlobLifecycle> {
        self.lifetimes
            .iter()
            .find(|entry| range_contains(entry.range, range))
            .map(|entry| entry.lifecycle)
    }

    fn normalize(&mut self) {
        coalesce_gc_ranges(&mut self.retired);
        coalesce_gc_ranges(&mut self.expired);
        self.lifetimes.retain(|entry| {
            !is_empty_gc_range(entry.range)
                && !range_overlaps_any(entry.range, &self.retired)
                && !range_overlaps_any(entry.range, &self.expired)
        });
        self.lifetimes
            .sort_by_key(|entry| (entry.range.offset, entry.range.len));
    }
}

fn coalesce_gc_ranges(ranges: &mut Vec<SegmentGcRecordRange>) {
    ranges.retain(|range| !is_empty_gc_range(*range));
    ranges.sort_by_key(|range| (range.offset, range.len));

    let mut coalesced: Vec<SegmentGcRecordRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        let Some(last) = coalesced.last_mut() else {
            coalesced.push(range);
            continue;
        };

        let last_end = gc_range_end(*last);
        let range_end = gc_range_end(range);
        if range.offset <= last_end {
            let new_end = last_end.max(range_end);
            last.len = new_end.saturating_sub(last.offset);
        } else {
            coalesced.push(range);
        }
    }

    *ranges = coalesced;
}

fn subtract_gc_range(ranges: &mut Vec<SegmentGcRecordRange>, removed: SegmentGcRecordRange) {
    if is_empty_gc_range(removed) {
        return;
    }

    let removed_end = gc_range_end(removed);
    let mut remaining = Vec::with_capacity(ranges.len().saturating_add(1));
    for range in ranges.drain(..) {
        let range_end = gc_range_end(range);
        if range_end <= removed.offset || range.offset >= removed_end {
            remaining.push(range);
            continue;
        }

        if range.offset < removed.offset {
            remaining.push(SegmentGcRecordRange {
                offset: range.offset,
                len: removed.offset.saturating_sub(range.offset),
            });
        }
        if range_end > removed_end {
            remaining.push(SegmentGcRecordRange {
                offset: removed_end,
                len: range_end.saturating_sub(removed_end),
            });
        }
    }

    *ranges = remaining;
}

fn remove_lifetimes_overlapping(
    lifetimes: &mut Vec<SegmentGcLifetimeRange>,
    range: SegmentGcRecordRange,
) {
    lifetimes.retain(|entry| !gc_ranges_overlap(entry.range, range));
}

fn range_overlaps_any(range: SegmentGcRecordRange, ranges: &[SegmentGcRecordRange]) -> bool {
    ranges
        .iter()
        .any(|candidate| gc_ranges_overlap(range, *candidate))
}

fn range_contains(container: SegmentGcRecordRange, contained: SegmentGcRecordRange) -> bool {
    contained.offset >= container.offset && gc_range_end(contained) <= gc_range_end(container)
}
fn gc_ranges_overlap(left: SegmentGcRecordRange, right: SegmentGcRecordRange) -> bool {
    !is_empty_gc_range(left)
        && !is_empty_gc_range(right)
        && left.offset < gc_range_end(right)
        && right.offset < gc_range_end(left)
}

/// xorshift64* PRNG so the fuzz sequence is deterministic across runs.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn random_range(rng: &mut Rng) -> SegmentGcRecordRange {
    if rng.below(10) < 8 {
        // Whole records, the common case: 20 slots of 10 bytes.
        SegmentGcRecordRange {
            offset: rng.below(20) * 10,
            len: 10,
        }
    } else {
        // Arbitrary spans, including empty ones and ones straddling record boundaries.
        SegmentGcRecordRange {
            offset: rng.below(200),
            len: rng.below(26),
        }
    }
}

fn random_lifecycle(rng: &mut Rng) -> Option<BlobLifecycle> {
    if rng.below(4) == 0 {
        return None;
    }
    Some(BlobLifecycle {
        logical_end_epoch: [5, 10, 20][rng.below(3) as usize],
        extension_count: rng.below(2) as u32,
    })
}

fn random_op(rng: &mut Rng) -> SegmentGcOverlayMergeOp {
    let count = 1 + rng.below(3) as usize;
    match rng.below(6) {
        0 => SegmentGcOverlayMergeOp::AddLiveBatch {
            records: (0..count)
                .map(|_| SegmentGcLiveRecord {
                    range: random_range(rng),
                    lifecycle: random_lifecycle(rng),
                })
                .collect(),
        },
        1 => SegmentGcOverlayMergeOp::AddRetiredBatch {
            ranges: (0..count).map(|_| random_range(rng)).collect(),
        },
        2 => SegmentGcOverlayMergeOp::AddExpiredBatch {
            ranges: (0..count).map(|_| random_range(rng)).collect(),
        },
        3 => SegmentGcOverlayMergeOp::ExpireBatch {
            ranges: (0..count).map(|_| random_range(rng)).collect(),
        },
        4 => SegmentGcOverlayMergeOp::RetireBatch {
            ranges: (0..count).map(|_| random_range(rng)).collect(),
        },
        _ => SegmentGcOverlayMergeOp::LifetimeBatch {
            updates: (0..count)
                .map(|_| SegmentGcLifetimeUpdate {
                    range: random_range(rng),
                    lifecycle: random_lifecycle(rng),
                })
                .collect(),
        },
    }
}

#[test]
fn indexed_fold_matches_reference_fold_for_batched_ops() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for iteration in 0..4000 {
        let mut actual = SegmentGcOverlay::default();
        let mut expected = SegmentGcOverlay::default();
        for _ in 0..1 + rng.below(4) {
            let ops = (0..1 + rng.below(12))
                .map(|_| random_op(&mut rng))
                .collect::<Vec<_>>();
            actual.apply_merge_ops(ops.clone());
            expected.reference_apply_merge_ops(ops.clone());
            assert_eq!(
                actual, expected,
                "iteration {iteration} diverged after {ops:?}"
            );
        }
    }
}

#[test]
fn indexed_fold_matches_reference_fold_for_single_ops() {
    let mut rng = Rng(0x0123_4567_89AB_CDEF);
    for iteration in 0..4000 {
        let mut actual = SegmentGcOverlay::default();
        let mut expected = SegmentGcOverlay::default();
        for _ in 0..1 + rng.below(24) {
            let op = random_op(&mut rng);
            actual.apply_merge_op(op.clone());
            expected.reference_apply_merge_op(op.clone());
            assert_eq!(
                actual, expected,
                "iteration {iteration} diverged after {op:?}"
            );
        }
    }
}

#[test]
fn indexed_fold_preserves_a_seeded_overlay() {
    // A fold over an overlay that already has canonical state, as the sweeper produces one.
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..500 {
        let mut seed = SegmentGcOverlay::default();
        seed.reference_apply_merge_ops((0..8).map(|_| random_op(&mut rng)));
        let mut actual = seed.clone();
        let mut expected = seed;
        let ops = (0..8).map(|_| random_op(&mut rng)).collect::<Vec<_>>();
        actual.apply_merge_ops(ops.clone());
        expected.reference_apply_merge_ops(ops);
        assert_eq!(actual, expected);
    }
}

/// One 1 GiB segment of 16 KiB records: a lifetime per record, then an expiry per record, which is
/// the event volume the sweeper folds for every sweep touching such a segment.
#[test]
#[ignore = "timing probe; run with --ignored --nocapture --release"]
fn folding_a_small_record_segment_is_fast() {
    const RECORDS: u64 = 65_536;
    const LEN: u64 = 16 << 10;
    let lifecycle = Some(BlobLifecycle {
        logical_end_epoch: 40,
        extension_count: 0,
    });
    let range = |index: u64| SegmentGcRecordRange {
        offset: index * LEN,
        len: LEN,
    };
    let ops = (0..RECORDS)
        .map(|index| SegmentGcOverlayMergeOp::LifetimeBatch {
            updates: vec![SegmentGcLifetimeUpdate {
                range: range(index),
                lifecycle,
            }],
        })
        .chain(
            (0..RECORDS).map(|index| SegmentGcOverlayMergeOp::ExpireBatch {
                ranges: vec![range(index)],
            }),
        )
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut overlay = SegmentGcOverlay::default();
    overlay.apply_merge_ops(ops);
    let elapsed = started.elapsed();
    println!("folded {} events in {elapsed:?}", RECORDS * 2);
    assert_eq!(overlay.expired.len(), 1);
    assert!(overlay.lifetimes.is_empty());
    assert_eq!(overlay.summary.expired_bytes, RECORDS * LEN);
    assert!(elapsed < Duration::from_secs(10), "fold took {elapsed:?}");
}

/// The previous per-event fold on an eighth of the segment above, for the before/after ratio.
#[test]
#[ignore = "timing probe; run with --ignored --nocapture --release"]
fn reference_fold_of_a_small_record_segment_for_comparison() {
    const RECORDS: u64 = 8_192;
    const LEN: u64 = 16 << 10;
    let lifecycle = Some(BlobLifecycle {
        logical_end_epoch: 40,
        extension_count: 0,
    });
    let range = |index: u64| SegmentGcRecordRange {
        offset: index * LEN,
        len: LEN,
    };
    let ops = (0..RECORDS)
        .map(|index| SegmentGcOverlayMergeOp::LifetimeBatch {
            updates: vec![SegmentGcLifetimeUpdate {
                range: range(index),
                lifecycle,
            }],
        })
        .chain(
            (0..RECORDS).map(|index| SegmentGcOverlayMergeOp::ExpireBatch {
                ranges: vec![range(index)],
            }),
        )
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut reference = SegmentGcOverlay::default();
    reference.reference_apply_merge_ops(ops.clone());
    let reference_elapsed = started.elapsed();
    let started = Instant::now();
    let mut indexed = SegmentGcOverlay::default();
    indexed.apply_merge_ops(ops);
    let indexed_elapsed = started.elapsed();
    println!(
        "{} events: reference {reference_elapsed:?}, indexed {indexed_elapsed:?}",
        RECORDS * 2
    );
    assert_eq!(reference, indexed);
}

#[test]
fn live_after_epoch_discounts_ended_buckets_and_keeps_the_unclassified_remainder() {
    let mut summary = SegmentGcSummary {
        total_bytes: 1_000,
        live_bytes: 1_000,
        live_ref_count: 5,
        unknown_lifetime_bytes: 100,
        unknown_lifetime_ref_count: 1,
        ..SegmentGcSummary::default()
    };
    summary.future_epoch_histogram.insert(
        50,
        EpochBucket {
            refs: 2,
            bytes: 500,
        },
    );
    summary.future_epoch_histogram.insert(
        70,
        EpochBucket {
            refs: 1,
            bytes: 200,
        },
    );
    // 5 live refs: 2 end at 50, 1 ends at 70, 1 is unknown, 1 is unclassified.

    let live = summary.live_after_epoch(49);
    assert_eq!((live.refs, live.bytes), (5, 1_000));
    let live = summary.live_after_epoch(50);
    assert_eq!((live.refs, live.bytes), (3, 500));
    let live = summary.live_after_epoch(70);
    assert_eq!((live.refs, live.bytes), (2, 300));

    // A fully classified summary reaches zero once every bucket has ended.
    summary.live_ref_count = 4;
    summary.live_bytes = 800;
    summary.unknown_lifetime_bytes = 0;
    summary.unknown_lifetime_ref_count = 0;
    let live = summary.live_after_epoch(70);
    assert_eq!((live.refs, live.bytes), (1, 100));
    summary.live_ref_count = 3;
    summary.live_bytes = 700;
    let live = summary.live_after_epoch(70);
    assert_eq!((live.refs, live.bytes), (0, 0));
}
