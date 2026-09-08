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

    /// Live bytes and refs the clock has not ended by `epoch`: every histogram bucket ending after
    /// it, plus bytes whose lifetime is unknown.
    ///
    /// A record whose known end epoch is at or before `epoch` is dead once the clock reaches it,
    /// whether or not compaction has produced its per-record Expired event yet. Callers may rely
    /// on that only once every write from before the transition to `epoch` has been merged, so
    /// no lifetime extension can still be unfolded; the GC snapshot's clock-expiry epoch is that
    /// bound.
    pub fn live_after_epoch(&self, epoch: Epoch) -> EpochBucket {
        let mut live = EpochBucket {
            refs: self.unknown_lifetime_ref_count,
            bytes: self.unknown_lifetime_bytes,
        };
        let mut classified = live;
        for (&end_epoch, bucket) in &self.future_epoch_histogram {
            classified.refs = classified.refs.saturating_add(bucket.refs);
            classified.bytes = classified.bytes.saturating_add(bucket.bytes);
            if end_epoch > epoch {
                live.refs = live.refs.saturating_add(bucket.refs);
                live.bytes = live.bytes.saturating_add(bucket.bytes);
            }
        }
        // Live refs the histogram and unknown counters do not account for (a summary written
        // before lifetimes were tracked, or one built by hand) cannot be judged by the clock and
        // stay live.
        live.refs = live
            .refs
            .saturating_add(self.live_ref_count.saturating_sub(classified.refs));
        live.bytes = live
            .bytes
            .saturating_add(self.live_bytes.saturating_sub(classified.bytes));
        live
    }

    /// The summary as the clock sees it at `epoch`: every histogram bucket ending at or before
    /// it is moved from live to expired, which is exactly what per-record Expired events used to
    /// do one record at a time.
    ///
    /// Compaction no longer emits those events for epoch expiry; a record's end epoch reaches the
    /// summary once, as its lifetime hint, and the clock does the rest. The precondition is the
    /// same as for [`Self::live_after_epoch`]: every write from before the transition to `epoch`
    /// has been merged, so no extension can still move a bucket. Extension counts are left alone,
    /// as they are for event-driven expiry.
    pub fn as_of_epoch(&self, epoch: Epoch) -> Self {
        let mut view = self.clone();
        let mut ended = EpochBucket::default();
        view.future_epoch_histogram.retain(|&end_epoch, bucket| {
            if end_epoch <= epoch {
                ended.refs = ended.refs.saturating_add(bucket.refs);
                ended.bytes = ended.bytes.saturating_add(bucket.bytes);
                false
            } else {
                true
            }
        });
        view.live_bytes = view.live_bytes.saturating_sub(ended.bytes);
        view.live_ref_count = view.live_ref_count.saturating_sub(ended.refs);
        view.expired_bytes = view.expired_bytes.saturating_add(ended.bytes);
        view.min_live_end_epoch = view.future_epoch_histogram.keys().next().copied();
        view.max_live_end_epoch = view.future_epoch_histogram.keys().next_back().copied();
        view
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
    ///
    /// The fold runs on indexed working copies of the three range lists, so each operation is a
    /// logarithmic lookup plus the spans it actually touches. The lists themselves stay plain
    /// vectors because every consumer (the copy classifier, the planner, tests) merge-joins
    /// against them in offset order. A segment with 65k records produces two events per record,
    /// and the sweeper replays a segment's whole history on every sweep, so per-event scans and
    /// re-sorts made one fold quadratic in records per segment.
    pub fn apply_merge_ops(&mut self, ops: impl IntoIterator<Item = SegmentGcOverlayMergeOp>) {
        let mut fold = OverlayFold::take(self);
        for op in ops {
            fold.apply(&mut self.summary, op);
        }
        fold.restore(self);
    }

    /// Applies one GC overlay merge operation and leaves the overlay canonical.
    pub fn apply_merge_op(&mut self, op: SegmentGcOverlayMergeOp) {
        self.apply_merge_ops([op]);
    }
}

/// Indexed working form of an overlay's range lists during one fold.
struct OverlayFold {
    retired: GcRangeSet,
    expired: GcRangeSet,
    lifetimes: GcLifetimeSet,
}

impl OverlayFold {
    fn take(overlay: &mut SegmentGcOverlay) -> Self {
        Self {
            retired: GcRangeSet::from_ranges(std::mem::take(&mut overlay.retired)),
            expired: GcRangeSet::from_ranges(std::mem::take(&mut overlay.expired)),
            lifetimes: GcLifetimeSet::from_ranges(std::mem::take(&mut overlay.lifetimes)),
        }
    }

    /// Writes the canonical lists back: coalesced spans in offset order, and only the lifetime
    /// hints that no retired or expired span overlaps.
    fn restore(self, overlay: &mut SegmentGcOverlay) {
        let Self {
            retired,
            expired,
            lifetimes,
        } = self;
        overlay.lifetimes =
            lifetimes.into_ranges(|range| !retired.overlaps(range) && !expired.overlaps(range));
        overlay.retired = retired.into_ranges();
        overlay.expired = expired.into_ranges();
    }

    fn apply(&mut self, summary: &mut SegmentGcSummary, op: SegmentGcOverlayMergeOp) {
        match op {
            SegmentGcOverlayMergeOp::AddLiveBatch { records } => {
                for record in records {
                    self.add_live_record(summary, record);
                }
            }
            SegmentGcOverlayMergeOp::AddRetiredBatch { ranges } => {
                for range in ranges {
                    self.add_retired_record(summary, range);
                }
            }
            SegmentGcOverlayMergeOp::AddExpiredBatch { ranges } => {
                for range in ranges {
                    self.add_expired_record(summary, range);
                }
            }
            SegmentGcOverlayMergeOp::ExpireBatch { ranges } => {
                for range in ranges {
                    self.expire_range(summary, range);
                }
            }
            SegmentGcOverlayMergeOp::RetireBatch { ranges } => {
                for range in ranges {
                    self.retire_range(summary, range);
                }
            }
            SegmentGcOverlayMergeOp::LifetimeBatch { updates } => {
                for update in updates {
                    self.apply_lifetime_update(summary, update);
                }
            }
        }
    }

    fn add_live_record(&mut self, summary: &mut SegmentGcSummary, record: SegmentGcLiveRecord) {
        if is_empty_gc_range(record.range) {
            return;
        }

        self.expired.subtract(record.range);
        self.retired.subtract(record.range);
        self.lifetimes.remove_overlapping(record.range);
        add_live_summary(summary, record.range.len, record.lifecycle);
        summary.total_bytes = summary.total_bytes.saturating_add(record.range.len);
        if let Some(lifecycle) = record.lifecycle {
            self.lifetimes.insert(record.range, lifecycle);
        }
    }

    fn add_retired_record(&mut self, summary: &mut SegmentGcSummary, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) || self.retired.contains(range) {
            return;
        }

        if self.expired.contains(range) {
            self.expired.subtract(range);
            summary.expired_bytes = summary.expired_bytes.saturating_sub(range.len);
        } else {
            summary.total_bytes = summary.total_bytes.saturating_add(range.len);
        }
        self.expired.subtract(range);
        self.lifetimes.remove_overlapping(range);
        summary.retired_bytes = summary.retired_bytes.saturating_add(range.len);
        self.retired.insert(range);
    }

    fn add_expired_record(&mut self, summary: &mut SegmentGcSummary, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) || self.retired.contains(range) {
            return;
        }

        self.retired.subtract(range);
        self.lifetimes.remove_overlapping(range);
        summary.total_bytes = summary.total_bytes.saturating_add(range.len);
        summary.expired_bytes = summary.expired_bytes.saturating_add(range.len);
        self.expired.insert(range);
    }

    fn expire_range(&mut self, summary: &mut SegmentGcSummary, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) || self.retired.contains(range) || self.expired.contains(range)
        {
            return;
        }

        let lifecycle = self.lifetimes.lifecycle_for_range(range);
        remove_live_summary(summary, range.len, lifecycle);
        summary.expired_bytes = summary.expired_bytes.saturating_add(range.len);
        self.lifetimes.remove_overlapping(range);
        self.expired.insert(range);
    }

    fn retire_range(&mut self, summary: &mut SegmentGcSummary, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) || self.retired.contains(range) {
            return;
        }

        if self.expired.contains(range) {
            summary.expired_bytes = summary.expired_bytes.saturating_sub(range.len);
            summary.retired_bytes = summary.retired_bytes.saturating_add(range.len);
            self.expired.subtract(range);
        } else {
            let lifecycle = self.lifetimes.lifecycle_for_range(range);
            remove_live_summary(summary, range.len, lifecycle);
            summary.retired_bytes = summary.retired_bytes.saturating_add(range.len);
        }

        self.lifetimes.remove_overlapping(range);
        self.retired.insert(range);
    }

    fn apply_lifetime_update(
        &mut self,
        summary: &mut SegmentGcSummary,
        update: SegmentGcLifetimeUpdate,
    ) {
        if is_empty_gc_range(update.range)
            || self.retired.contains(update.range)
            || self.expired.contains(update.range)
        {
            return;
        }

        let old = self.lifetimes.lifecycle_for_range(update.range);
        if old == update.lifecycle {
            return;
        }
        remove_live_summary(summary, update.range.len, old);
        add_live_summary(summary, update.range.len, update.lifecycle);
        self.lifetimes.remove_overlapping(update.range);
        if let Some(lifecycle) = update.lifecycle {
            self.lifetimes.insert(update.range, lifecycle);
        }
    }
}

/// Coalesced, non-overlapping byte spans keyed by start offset; the value is the exclusive end.
///
/// Adjacent spans are merged on insert, which is the same canonical form the vector lists use.
#[derive(Default)]
struct GcRangeSet {
    spans: BTreeMap<u64, u64>,
}

impl GcRangeSet {
    fn from_ranges(ranges: Vec<SegmentGcRecordRange>) -> Self {
        let mut set = Self::default();
        for range in ranges {
            set.insert(range);
        }
        set
    }

    fn into_ranges(self) -> Vec<SegmentGcRecordRange> {
        self.spans
            .into_iter()
            .map(|(offset, end)| SegmentGcRecordRange {
                offset,
                len: end.saturating_sub(offset),
            })
            .collect()
    }

    /// The span starting at or before `offset`, as owned values so the borrow ends here.
    fn span_at_or_before(&self, offset: u64) -> Option<(u64, u64)> {
        self.spans
            .range(..=offset)
            .next_back()
            .map(|(&start, &end)| (start, end))
    }

    /// The span starting strictly before `offset`.
    fn span_before(&self, offset: u64) -> Option<(u64, u64)> {
        self.spans
            .range(..offset)
            .next_back()
            .map(|(&start, &end)| (start, end))
    }

    /// The first span starting within `start..=end`.
    fn first_span_within(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        self.spans
            .range(start..=end)
            .next()
            .map(|(&start, &end)| (start, end))
    }

    /// Whether one span covers the whole range.
    fn contains(&self, range: SegmentGcRecordRange) -> bool {
        self.span_at_or_before(range.offset)
            .is_some_and(|(_, end)| end >= gc_range_end(range))
    }

    /// Whether any span strictly overlaps the range.
    fn overlaps(&self, range: SegmentGcRecordRange) -> bool {
        if is_empty_gc_range(range) {
            return false;
        }
        self.span_before(gc_range_end(range))
            .is_some_and(|(_, end)| end > range.offset)
    }

    /// Adds the range, merging it with every span it overlaps or touches.
    fn insert(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) {
            return;
        }
        let mut start = range.offset;
        let mut end = gc_range_end(range);
        if let Some((span_start, span_end)) = self.span_at_or_before(start)
            && span_end >= start
        {
            start = span_start;
            end = end.max(span_end);
            self.spans.remove(&span_start);
        }
        while let Some((span_start, span_end)) = self.first_span_within(start, end) {
            end = end.max(span_end);
            self.spans.remove(&span_start);
        }
        self.spans.insert(start, end);
    }

    /// Removes the range from every span, keeping the parts of those spans outside it.
    fn subtract(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) {
            return;
        }
        let start = range.offset;
        let end = gc_range_end(range);
        let mut removed = Vec::new();
        let mut pieces = Vec::new();
        if let Some((span_start, span_end)) = self.span_before(start)
            && span_end > start
        {
            removed.push(span_start);
            pieces.push((span_start, start));
            if span_end > end {
                pieces.push((end, span_end));
            }
        }
        for (&span_start, &span_end) in self.spans.range(start..end) {
            removed.push(span_start);
            if span_end > end {
                pieces.push((end, span_end));
            }
        }
        for span_start in removed {
            self.spans.remove(&span_start);
        }
        for (span_start, span_end) in pieces {
            self.spans.insert(span_start, span_end);
        }
    }
}

/// Lifetime hints keyed by start offset; the value is the exclusive end and the lifecycle.
///
/// Every mutation removes overlapping hints before inserting, so entries never overlap and the
/// hint covering a record is always the one starting at or before it.
#[derive(Default)]
struct GcLifetimeSet {
    entries: BTreeMap<u64, (u64, BlobLifecycle)>,
}

impl GcLifetimeSet {
    fn from_ranges(mut ranges: Vec<SegmentGcLifetimeRange>) -> Self {
        // Canonical input never has two hints at one offset; if it did, the previous
        // first-match lookup would have chosen the shorter one.
        ranges.sort_by_key(|entry| (entry.range.offset, entry.range.len));
        let mut set = Self::default();
        for entry in ranges {
            if is_empty_gc_range(entry.range) {
                continue;
            }
            set.entries
                .entry(entry.range.offset)
                .or_insert((gc_range_end(entry.range), entry.lifecycle));
        }
        set
    }

    fn into_ranges(
        self,
        keep: impl Fn(SegmentGcRecordRange) -> bool,
    ) -> Vec<SegmentGcLifetimeRange> {
        self.entries
            .into_iter()
            .map(|(offset, (end, lifecycle))| SegmentGcLifetimeRange {
                range: SegmentGcRecordRange {
                    offset,
                    len: end.saturating_sub(offset),
                },
                lifecycle,
            })
            .filter(|entry| keep(entry.range))
            .collect()
    }

    /// The lifecycle of the hint that wholly contains the range, if any.
    fn lifecycle_for_range(&self, range: SegmentGcRecordRange) -> Option<BlobLifecycle> {
        self.entries
            .range(..=range.offset)
            .next_back()
            .and_then(|(_, &(end, lifecycle))| (end >= gc_range_end(range)).then_some(lifecycle))
    }

    /// Drops every hint that strictly overlaps the range.
    fn remove_overlapping(&mut self, range: SegmentGcRecordRange) {
        if is_empty_gc_range(range) {
            return;
        }
        let start = range.offset;
        let end = gc_range_end(range);
        let mut removed = Vec::new();
        if let Some((&entry_start, &(entry_end, _))) = self.entries.range(..start).next_back()
            && entry_end > start
        {
            removed.push(entry_start);
        }
        removed.extend(self.entries.range(start..end).map(|(&offset, _)| offset));
        for offset in removed {
            self.entries.remove(&offset);
        }
    }

    fn insert(&mut self, range: SegmentGcRecordRange, lifecycle: BlobLifecycle) {
        self.entries
            .insert(range.offset, (gc_range_end(range), lifecycle));
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
    /// An ingest segment that has stopped accepting appends and is waiting for durability
    /// publication to flush, checksum, and publish it as immutable.
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

#[cfg(test)]
mod tests;
