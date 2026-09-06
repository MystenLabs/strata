//! Sweeper-side cache of folded segment garbage overlays.
//!
//! Folding a segment's garbage overlay means reading its whole segment-local log and replaying
//! every event, so a sweep that touches a thousand mature segments spends most of its time
//! rebuilding overlays it rebuilt a minute ago. The sweeper keeps the overlays it produced,
//! keyed by the segment and the log position they correspond to, and reuses one whenever the
//! segment's log is still at that position.
//!
//! The overlay's summary starts from the segment's summary row, which other publishers also
//! move (allocation baselines, GC output accounting). Each entry therefore remembers the row
//! value the sweep expected to leave behind; on reuse the difference between that and the row's
//! current value is applied to the cached summary, which yields exactly the summary a fresh fold
//! from the current row would.

use std::collections::HashMap;

use core_types::{SegmentGcOverlay, SegmentGcSummary, SegmentId};

/// Overlays retained across sweeps, bounded by their estimated heap footprint.
#[derive(Debug)]
pub(crate) struct OverlayCache {
    capacity_bytes: usize,
    used_bytes: usize,
    tick: u64,
    entries: HashMap<SegmentId, CachedOverlay>,
}

#[derive(Debug)]
struct CachedOverlay {
    committed: u64,
    expected_row: SegmentGcSummary,
    overlay: SegmentGcOverlay,
    bytes: usize,
    last_used: u64,
}

impl OverlayCache {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            used_bytes: 0,
            tick: 0,
            entries: HashMap::new(),
        }
    }

    /// Removes and returns the overlay cached for `segment_id` if it still describes the log
    /// prefix ending at `committed`. A stale entry is dropped.
    pub(crate) fn take(
        &mut self,
        segment_id: SegmentId,
        committed: u64,
    ) -> Option<(SegmentGcSummary, SegmentGcOverlay)> {
        let entry = self.entries.remove(&segment_id)?;
        self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
        (entry.committed == committed).then_some((entry.expected_row, entry.overlay))
    }

    /// Caches the overlay that corresponds to the segment's log prefix ending at `committed`,
    /// together with the summary row the sweep expects to have left behind.
    pub(crate) fn insert(
        &mut self,
        segment_id: SegmentId,
        committed: u64,
        expected_row: SegmentGcSummary,
        overlay: SegmentGcOverlay,
    ) {
        let bytes = overlay_bytes(&overlay);
        if bytes > self.capacity_bytes {
            return;
        }
        if let Some(previous) = self.entries.remove(&segment_id) {
            self.used_bytes = self.used_bytes.saturating_sub(previous.bytes);
        }
        while self.used_bytes.saturating_add(bytes) > self.capacity_bytes {
            let Some((&victim, _)) = self.entries.iter().min_by_key(|(_, entry)| entry.last_used)
            else {
                break;
            };
            let evicted = self.entries.remove(&victim).expect("victim was present");
            self.used_bytes = self.used_bytes.saturating_sub(evicted.bytes);
        }
        self.tick = self.tick.wrapping_add(1);
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.entries.insert(
            segment_id,
            CachedOverlay {
                committed,
                expected_row,
                overlay,
                bytes,
                last_used: self.tick,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

fn overlay_bytes(overlay: &SegmentGcOverlay) -> usize {
    const RANGE_BYTES: usize = 16;
    const LIFETIME_BYTES: usize = 40;
    const BUCKET_BYTES: usize = 48;
    256 + (overlay.expired.len() + overlay.retired.len()) * RANGE_BYTES
        + overlay.lifetimes.len() * LIFETIME_BYTES
        + (overlay.summary.future_epoch_histogram.len()
            + overlay.summary.extension_count_histogram.len())
            * BUCKET_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlay(ranges: usize) -> SegmentGcOverlay {
        SegmentGcOverlay {
            summary: SegmentGcSummary::default(),
            expired: (0..ranges as u64)
                .map(|offset| core_types::SegmentGcRecordRange { offset, len: 1 })
                .collect(),
            retired: Vec::new(),
            lifetimes: Vec::new(),
        }
    }

    #[test]
    fn entries_are_reused_only_at_their_committed_position() {
        let mut cache = OverlayCache::new(1 << 20);
        cache.insert(7, 100, SegmentGcSummary::default(), overlay(3));
        assert!(cache.take(7, 99).is_none());
        assert!(
            cache.take(7, 100).is_none(),
            "the stale take dropped the entry"
        );
        cache.insert(7, 100, SegmentGcSummary::default(), overlay(3));
        let (_, taken) = cache.take(7, 100).unwrap();
        assert_eq!(taken.expired.len(), 3);
        assert!(cache.take(7, 100).is_none(), "take removes the entry");
    }

    #[test]
    fn least_recently_used_entries_are_evicted_to_fit() {
        // Each entry with 100 ranges costs 256 + 1600 bytes; three fit, four do not.
        let mut cache = OverlayCache::new(6_000);
        for segment in 1..=3 {
            cache.insert(segment, 1, SegmentGcSummary::default(), overlay(100));
        }
        assert_eq!(cache.len(), 3);
        cache.insert(4, 1, SegmentGcSummary::default(), overlay(100));
        assert_eq!(cache.len(), 3);
        assert!(cache.take(1, 1).is_none(), "segment 1 was the oldest");
        assert!(cache.take(4, 1).is_some());
        // An overlay larger than the whole cache is never kept.
        cache.insert(5, 1, SegmentGcSummary::default(), overlay(10_000));
        assert!(cache.take(5, 1).is_none());
    }
}
