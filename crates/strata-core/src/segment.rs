use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlobKey, BlobLifecycle, Epoch, RecordRef, ShardKey, StrataLsn};

pub type SegmentId = u64;
pub type VolumeId = u32;

/// Sort key that keeps garbage for one physical segment together.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentKey {
    pub segment_id: SegmentId,
    pub blob_key: BlobKey,
}

/// One physical transition used to classify a record during GC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GarbageEvent {
    Retired {
        record: RecordRef,
    },
    Expired {
        record: RecordRef,
    },
    /// Sets the initial lifecycle, changes it, or explicitly clears it with `None`.
    SetLifecycle {
        record: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    },
}

impl GarbageEvent {
    pub fn record(&self) -> RecordRef {
        match self {
            Self::Retired { record }
            | Self::Expired { record }
            | Self::SetLifecycle { record, .. } => *record,
        }
    }
}

/// Physical owner of a segment file.
///
/// Mixed ingest segments are store-owned because they may contain records from many shards.
/// Retention segments are owned by one concrete shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SegmentOwner {
    Store,
    Shard(ShardKey),
}

impl SegmentOwner {
    pub fn shard(self) -> Option<ShardKey> {
        match self {
            Self::Store => None,
            Self::Shard(shard) => Some(shard),
        }
    }
}

/// Segment-local byte range for one encoded record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentGcRecordRange {
    /// Starting byte offset of the encoded record within the segment file.
    pub offset: u64,
    /// Encoded record length, including header, payload, and key trailer bytes.
    pub len: u64,
}

impl SegmentGcRecordRange {
    pub fn end_offset(self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }
}

impl From<crate::RecordRef> for SegmentGcRecordRange {
    fn from(record_ref: crate::RecordRef) -> Self {
        Self {
            offset: record_ref.offset,
            len: record_ref.len,
        }
    }
}

/// Known lifetime for a segment-local live or copy eligible record range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGcLifetimeRange {
    /// Physical record range the lifetime hint applies to.
    pub range: SegmentGcRecordRange,
    /// Logical lifetime used by GC to route the range during copy.
    pub lifecycle: BlobLifecycle,
}

/// Lifetime update folded into a segment-local GC overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGcLifetimeUpdate {
    /// Physical record range whose routing hint changed.
    pub range: SegmentGcRecordRange,
    /// New lifetime hint. `None` clears the hint while keeping the range copy-eligible.
    pub lifecycle: Option<BlobLifecycle>,
}

/// Live record allocation folded into a segment-local GC overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGcLiveRecord {
    /// Physical record range that has become protected by a live logical key.
    pub range: SegmentGcRecordRange,
    /// Optional logical lifetime known when the live range was materialized.
    pub lifecycle: Option<BlobLifecycle>,
}

/// Cheap segment summary used by GC planning.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentGcSummary {
    /// Total encoded bytes represented for this segment by the GC overlay fold.
    pub total_bytes: u64,
    /// Bytes currently protected by live refs.
    pub live_bytes: u64,
    /// Bytes permanently retired by overwrite, tombstone, shard drop, or completed relocation.
    pub retired_bytes: u64,
    /// Bytes whose lifecycle has ended and are collectable unless already permanently retired.
    pub expired_bytes: u64,
    /// Number of currently live physical refs in this segment.
    pub live_ref_count: u64,
    /// Live bytes without a known end epoch, routed to spillover by default.
    pub unknown_lifetime_bytes: u64,
    /// Number of live refs without a known end epoch.
    pub unknown_lifetime_ref_count: u64,
    /// Earliest end epoch among live refs with known lifetimes.
    pub min_live_end_epoch: Option<Epoch>,
    /// Latest end epoch among live refs with known lifetimes.
    pub max_live_end_epoch: Option<Epoch>,
    /// Live bytes and ref counts grouped by logical end epoch for placement planning.
    pub future_epoch_histogram: BTreeMap<Epoch, EpochBucket>,
    /// Extension counts of refs added live to this segment. Refs stay in their bucket after they
    /// expire: per-epoch extension counts are not tracked, so expiry sweeps cannot remove them.
    pub extension_count_histogram: BTreeMap<u32, u64>,
}

/// Additive RocksDB merge operand for one segment's GC summary.
///
/// LSM compaction resolves record lifecycles before producing this delta. Negative values move bytes or
/// references out of their previous state; positive values move them into their new state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentGcSummaryDelta {
    pub total_bytes: i128,
    pub live_bytes: i128,
    pub retired_bytes: i128,
    pub expired_bytes: i128,
    pub live_ref_count: i128,
    pub unknown_lifetime_bytes: i128,
    pub unknown_lifetime_ref_count: i128,
    pub epoch_bytes: BTreeMap<Epoch, i128>,
    pub epoch_refs: BTreeMap<Epoch, i128>,
    pub extension_counts: BTreeMap<u32, i128>,
}

impl SegmentGcSummary {
    pub fn garbage_bytes(&self) -> u64 {
        self.retired_bytes.saturating_add(self.expired_bytes)
    }

    pub fn garbage_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.garbage_bytes() as f64 / self.total_bytes as f64
        }
    }

    pub fn is_empty(&self) -> bool {
        self.live_ref_count == 0
    }
}

/// Stale tolerant segment local overlay used by GC copy planning.
///
/// `retired` ranges are permanently skippable because the key no longer protects the bytes.
/// `expired` ranges are skippable because their lifecycle ended and cannot be revived by a later
/// lifetime update. Ranges absent from both are eligible to copy, not necessarily proven
/// live in the freshest blob-version view. `lifetimes` contains routing hints for eligible ranges
/// with known expiry; absent lifetime means spillover/unknown routing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SegmentGcOverlay {
    /// Compact aggregate counters that let the planner score segments without scanning files.
    pub summary: SegmentGcSummary,
    /// Ranges expired by lifecycle. These are skippable and are not revivable by extension.
    pub expired: Vec<SegmentGcRecordRange>,
    /// Ranges permanently retired. These bytes should never be copied by GC.
    pub retired: Vec<SegmentGcRecordRange>,
    /// Routing hints for copy-eligible ranges with known logical lifetimes.
    pub lifetimes: Vec<SegmentGcLifetimeRange>,
}

impl SegmentGcOverlay {
    /// Applies GC overlay merge operations in commit order and leaves the overlay canonical.
    pub fn apply_merge_ops(&mut self, ops: impl IntoIterator<Item = SegmentGcOverlayMergeOp>) {
        for op in ops {
            self.apply_merge_op_unchecked(op);
        }
        self.normalize();
    }

    /// Applies one GC overlay merge operation and leaves the overlay canonical.
    pub fn apply_merge_op(&mut self, op: SegmentGcOverlayMergeOp) {
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

/// Merge operand for `SegmentGcOverlay`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentGcOverlayMergeOp {
    /// Accounts newly protected live ranges.
    AddLiveBatch { records: Vec<SegmentGcLiveRecord> },
    /// Accounts newly written ranges that were already permanently dead at materialization time.
    AddRetiredBatch { ranges: Vec<SegmentGcRecordRange> },
    /// Accounts newly written ranges that were already expired at materialization time.
    AddExpiredBatch { ranges: Vec<SegmentGcRecordRange> },
    /// Marks ranges as expired. Expired ranges are not revivable by later lifetime updates.
    ExpireBatch { ranges: Vec<SegmentGcRecordRange> },
    /// Marks segment-local ranges as definitely not protecting live data. This is stronger than a
    /// lifetime hint and removes overlapping lifecycle overlay state when folded.
    RetireBatch { ranges: Vec<SegmentGcRecordRange> },
    /// Sets or clears lifecycle routing for ranges that remain copy-eligible. A `None`
    /// lifecycle is an explicit clear, not the absence of an update.
    LifetimeBatch {
        updates: Vec<SegmentGcLifetimeUpdate>,
    },
}

fn add_live_summary(
    summary: &mut SegmentGcSummary,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    summary.live_bytes = summary.live_bytes.saturating_add(record_len);
    summary.live_ref_count = summary.live_ref_count.saturating_add(1);
    let Some(lifecycle) = lifecycle else {
        summary.unknown_lifetime_bytes = summary.unknown_lifetime_bytes.saturating_add(record_len);
        summary.unknown_lifetime_ref_count = summary.unknown_lifetime_ref_count.saturating_add(1);
        return;
    };
    let bucket = summary
        .future_epoch_histogram
        .entry(lifecycle.logical_end_epoch)
        .or_default();
    bucket.refs = bucket.refs.saturating_add(1);
    bucket.bytes = bucket.bytes.saturating_add(record_len);
    increment_histogram(
        &mut summary.extension_count_histogram,
        lifecycle.extension_count,
    );
    refresh_live_epoch_bounds(summary);
}

fn remove_live_summary(
    summary: &mut SegmentGcSummary,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    summary.live_bytes = summary.live_bytes.saturating_sub(record_len);
    summary.live_ref_count = summary.live_ref_count.saturating_sub(1);
    let Some(lifecycle) = lifecycle else {
        summary.unknown_lifetime_bytes = summary.unknown_lifetime_bytes.saturating_sub(record_len);
        summary.unknown_lifetime_ref_count = summary.unknown_lifetime_ref_count.saturating_sub(1);
        return;
    };
    if let Some(bucket) = summary
        .future_epoch_histogram
        .get_mut(&lifecycle.logical_end_epoch)
    {
        bucket.refs = bucket.refs.saturating_sub(1);
        bucket.bytes = bucket.bytes.saturating_sub(record_len);
        if bucket.refs == 0 {
            summary
                .future_epoch_histogram
                .remove(&lifecycle.logical_end_epoch);
        }
    }
    decrement_histogram(
        &mut summary.extension_count_histogram,
        lifecycle.extension_count,
    );
    refresh_live_epoch_bounds(summary);
}

fn refresh_live_epoch_bounds(summary: &mut SegmentGcSummary) {
    summary.min_live_end_epoch = summary.future_epoch_histogram.keys().next().copied();
    summary.max_live_end_epoch = summary.future_epoch_histogram.keys().next_back().copied();
}

fn increment_histogram<K>(histogram: &mut BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    *histogram.entry(key).or_default() += 1;
}

fn decrement_histogram<K>(histogram: &mut BTreeMap<K, u64>, key: K)
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

fn is_empty_gc_range(range: SegmentGcRecordRange) -> bool {
    range.len == 0
}

fn gc_range_end(range: SegmentGcRecordRange) -> u64 {
    range.offset.saturating_add(range.len)
}

/// Physical placement class for a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementClass {
    Ingest,
    ExactEpoch(Epoch),
    Spillover,
}

/// Durable segment file lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentFileState {
    /// The active ingest segment. New records may be appended, and only the durable prefix is
    /// guaranteed to survive a crash.
    Open,
    /// An ingest segment that has stopped accepting appends and is waiting for the seal worker to
    /// flush, checksum, and publish it as immutable.
    Sealing,
    /// An immutable segment whose complete contents and sealed metadata are durable and readable.
    Sealed,
    /// A segment that is no longer readable or eligible for planning. Its file has been removed or
    /// is scheduled for idempotent removal.
    Deleted,
    /// A sealed GC output published at its final path before its relocation entries are written. Recovery
    /// deletes it if publication does not commit.
    PendingGcOutput,
    /// A sealed GC source whose replacement refs have been published. The source remains readable
    /// and fenced from rewrites until reference processing makes it eligible for final deletion.
    GcRelocating,
}

/// Durable metadata for one segment file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentState {
    pub owner: SegmentOwner,
    pub segment_id: SegmentId,
    pub volume_id: VolumeId,
    pub path: String,
    pub placement_class: PlacementClass,
    pub state: SegmentFileState,
    pub write_offset: u64,
    pub durable_offset: u64,
    pub min_lsn: Option<StrataLsn>,
    pub max_lsn: Option<StrataLsn>,
    /// Exclusive logical checkpoint boundary assigned when an ingest segment is rolled over.
    pub sealed_before_lsn: Option<StrataLsn>,
    pub sealed_len: Option<u64>,
    /// SHA-256 digest of the sealed bytes, present only after the segment is finalized.
    pub sealed_sha256: Option<[u8; 32]>,
}

/// Live refs and bytes in one segment that expire at one logical end epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EpochBucket {
    pub refs: u64,
    pub bytes: u64,
}
