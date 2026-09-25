//! Guards the allocation cost of a full column-family scan.
//!
//! A scan must not allocate per row at the storage-port boundary. It once did: the port yielded
//! owned `(Vec<u8>, Vec<u8>)` pairs built from `rocksdb::DBIterator`, which itself boxes both key
//! and value, so every row of every scan cost four allocations before anything was decoded. Scans
//! run on each GC cycle and each segment seal, so that scaled with segment count.
//!
//! The port now hands out borrowed bytes through a lending cursor, and the only allocations left
//! are the ones the decoded types genuinely need.
//!
//! This test installs a counting global allocator, so it lives in its own test binary and must not
//! gain a second test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use core_types::{PlacementClass, SegmentFileState, SegmentId, SegmentOwner, SegmentState};
use index::StrataIndex;
use tempfile::tempdir;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// `SegmentState` carries one `String`, so a decoded row is expected to cost one allocation.
const MAX_ALLOCS_PER_DECODED_ROW: f64 = 2.0;

fn segment_state(segment_id: SegmentId) -> SegmentState {
    SegmentState {
        owner: SegmentOwner::Store,
        segment_id,
        volume_id: 0,
        path: format!("{segment_id:06}.data"),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset: 128,
        durable_offset: 64,
        min_lsn: Some(1),
        max_lsn: Some(3),
        sealed_before_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

fn measure(body: impl FnOnce()) -> usize {
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    body();
    COUNTING.store(false, Ordering::Relaxed);
    ALLOCS.load(Ordering::Relaxed)
}

/// Scans `rows` records and reports (allocations at the port boundary, allocations for the whole
/// decoded scan).
fn scan_costs(rows: usize) -> (usize, usize) {
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "scan", "scan").unwrap();

    let mut batch = index.batch();
    for id in 0..rows as u64 {
        index
            .put_segment_state_batch(&mut batch, &segment_state(id))
            .unwrap();
    }
    batch.write_with_sync(true).unwrap();

    // Warm the block cache so first-touch reads are not counted.
    assert_eq!(index.iter_segment_states().unwrap().len(), rows);

    let cf = index.cf_names().segment_states.clone();
    let mut touched = 0usize;
    let raw = measure(|| {
        let mut cursor = index.db().scan(&cf).unwrap();
        while cursor.next_row().unwrap() {
            let (key, value) = cursor.row();
            // Touch both so nothing can be optimized away.
            touched += usize::from(!key.is_empty()) + usize::from(!value.is_empty());
        }
    });
    assert_eq!(touched, rows * 2);

    let mut scanned = Vec::new();
    let decoded = measure(|| scanned = index.iter_segment_states().unwrap());
    assert_eq!(scanned.len(), rows);

    (raw, decoded)
}

/// The port boundary must cost the same whether a scan covers a thousand rows or ten thousand.
///
/// Comparing two sizes is the point: a fixed budget would pass a per-row regression as long as the
/// budget was generous, whereas a cost that grows with row count fails here immediately.
#[test]
fn scanning_does_not_allocate_per_row_at_the_port_boundary() {
    let (small_raw, small_decoded) = scan_costs(1_000);
    let (large_raw, large_decoded) = scan_costs(10_000);

    assert_eq!(
        small_raw, large_raw,
        "the port boundary allocated {small_raw} times for 1,000 rows and {large_raw} for 10,000; \
         a scan must borrow the backend's bytes rather than copy each row"
    );

    for (rows, decoded) in [(1_000usize, small_decoded), (10_000, large_decoded)] {
        let per_row = decoded as f64 / rows as f64;
        assert!(
            per_row <= MAX_ALLOCS_PER_DECODED_ROW,
            "decoded scan of {rows} rows cost {per_row:.2} allocations per row, over the \
             {MAX_ALLOCS_PER_DECODED_ROW:.2} budget"
        );
    }
}
