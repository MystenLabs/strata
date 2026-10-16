//! Sweeper-side cache of folded segment garbage overlays.
//!
//! Folding a segment's garbage overlay means reading its whole segment-local log and replaying
//! every event, so a sweep that touches a thousand mature segments spends most of its time
//! rebuilding overlays it rebuilt a minute ago. The sweeper keeps the overlays it produced,
//! keyed by the segment and the log position they correspond to, and reuses one whenever the
//! segment's log is still at that position.
//! The matching validated log writer is retained too, so a cache hit skips recovery's full
//! history scan. Entries are published only after a successful durable sweep commit.
//!
//! The overlay's summary starts from the segment's summary row, which other publishers also
//! move (allocation baselines, GC output accounting). Each entry therefore remembers the row
//! value the sweep expected to leave behind; on reuse the difference between that and the row's
//! current value is applied to the cached summary, which yields exactly the summary a fresh fold
//! from the current row would.

use std::{collections::HashMap, path::Path};

use core_types::{SegmentGcOverlay, SegmentGcSummary, SegmentId};
use lsm::SegmentGarbageLog;

use crate::Result;

/// Bound open log handles as well as overlay memory.
const MAX_CACHED_LOGS: usize = 1024;

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
    log: SegmentGarbageLog,
}

impl OverlayCache {
    pub(crate) fn remove(&mut self, segment_id: SegmentId) {
        if let Some(entry) = self.entries.remove(&segment_id) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
        }
    }

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
        path: &Path,
    ) -> Result<Option<(SegmentGcSummary, SegmentGcOverlay, SegmentGarbageLog)>> {
        let Some(entry) = self.entries.remove(&segment_id) else {
            return Ok(None);
        };
        self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
        if entry.committed != committed || !entry.log.is_at_committed_position(path, committed)? {
            return Ok(None);
        }
        Ok(Some((entry.expected_row, entry.overlay, entry.log)))
    }

    /// Caches the overlay that corresponds to the segment's log prefix ending at `committed`,
    /// together with the summary row the sweep expects to have left behind.
    /// Only insert after both the log and the metadata batch have been synced successfully.
    pub(crate) fn insert(
        &mut self,
        segment_id: SegmentId,
        committed: u64,
        expected_row: SegmentGcSummary,
        overlay: SegmentGcOverlay,
        log: SegmentGarbageLog,
    ) {
        let bytes = overlay_bytes(&overlay);
        if bytes > self.capacity_bytes {
            return;
        }
        if let Some(previous) = self.entries.remove(&segment_id) {
            self.used_bytes = self.used_bytes.saturating_sub(previous.bytes);
        }
        while self.used_bytes.saturating_add(bytes) > self.capacity_bytes
            || self.entries.len() >= MAX_CACHED_LOGS
        {
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
                log,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

pub(crate) fn overlay_bytes(overlay: &SegmentGcOverlay) -> usize {
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

    fn cached_log(path: &Path) -> (SegmentGarbageLog, u64) {
        let log = SegmentGarbageLog::open(path, 0).unwrap();
        let committed = std::fs::metadata(path).unwrap().len();
        (log, committed)
    }

    #[test]
    fn entries_are_reused_only_at_their_committed_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("7.glog");
        let mut cache = OverlayCache::new(1 << 20);
        let (log, committed) = cached_log(&path);
        cache.insert(7, committed, SegmentGcSummary::default(), overlay(3), log);
        assert!(cache.take(7, committed - 1, &path).unwrap().is_none());
        assert!(cache.take(7, committed, &path).unwrap().is_none());
        let (log, committed) = cached_log(&path);
        cache.insert(7, committed, SegmentGcSummary::default(), overlay(3), log);
        let (_, taken, _) = cache.take(7, committed, &path).unwrap().unwrap();
        assert_eq!(taken.expired.len(), 3);
        assert!(cache.take(7, committed, &path).unwrap().is_none());
    }

    #[test]
    fn least_recently_used_entries_are_evicted_to_fit() {
        let dir = tempfile::tempdir().unwrap();
        // Each entry with 100 ranges costs 256 + 1600 bytes; three fit, four do not.
        let mut cache = OverlayCache::new(6_000);
        for segment in 1..=4 {
            let path = dir.path().join(format!("{segment}.glog"));
            let (log, committed) = cached_log(&path);
            cache.insert(
                segment,
                committed,
                SegmentGcSummary::default(),
                overlay(100),
                log,
            );
        }
        assert_eq!(cache.len(), 3);
        assert!(!cache.entries.contains_key(&1), "segment 1 was the oldest");
        assert!(cache.entries.contains_key(&4));
        // An overlay larger than the whole cache is never kept.
        let (log, committed) = cached_log(&dir.path().join("5.glog"));
        cache.insert(
            5,
            committed,
            SegmentGcSummary::default(),
            overlay(10_000),
            log,
        );
        assert!(!cache.entries.contains_key(&5));
    }

    #[test]
    fn log_handles_are_bounded_even_for_tiny_overlays() {
        let _ = fdlimit::raise_fd_limit();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.glog");
        let (_, committed) = cached_log(&path);
        let mut cache = OverlayCache::new(1 << 30);
        for segment in 0..=MAX_CACHED_LOGS as u64 {
            let log = SegmentGarbageLog::open(&path, committed).unwrap();
            cache.insert(
                segment,
                committed,
                SegmentGcSummary::default(),
                overlay(0),
                log,
            );
        }
        assert_eq!(cache.len(), MAX_CACHED_LOGS);
        assert!(!cache.entries.contains_key(&0));
    }

    #[test]
    fn replacing_a_log_invalidates_its_overlay_too() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("7.glog");
        let (log, committed) = cached_log(&path);
        let mut cache = OverlayCache::new(1 << 20);
        cache.insert(7, committed, SegmentGcSummary::default(), overlay(3), log);
        let replacement = dir.path().join("replacement.glog");
        let (_, replacement_position) = cached_log(&replacement);
        assert_eq!(committed, replacement_position);
        std::fs::rename(replacement, &path).unwrap();
        assert!(cache.take(7, committed, &path).unwrap().is_none());
    }
}
