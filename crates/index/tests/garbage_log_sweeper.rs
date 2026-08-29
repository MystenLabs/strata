use std::sync::Once;

use core_types::{
    BlobKey, PlacementClass, RecordRef, SegmentFileState, SegmentGcRecordRange, SegmentGcSummary,
    SegmentGcSummaryDelta, SegmentId, SegmentOwner, SegmentState,
};
use index::StrataIndex;
use lsm::{
    GarbageEvent, GarbageLog, GarbageLogPosition, GarbageRecord, SegmentGarbageLog, SegmentKey,
    read_segment_garbage,
};
use tempfile::TempDir;
use typed_store::DBMetrics;

const HEAD: &str = "segment-ref";
const CURSOR: &str = "segment-ref-sweep";
static INIT_TYPED_STORE_METRICS: Once = Once::new();

fn init_typed_store_metrics() {
    INIT_TYPED_STORE_METRICS.call_once(|| {
        DBMetrics::get();
    });
}

#[tokio::test]
async fn sweep_copies_details_before_publishing_summaries_and_cursor() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    put_ready_segment(&index, 1, 100, 1);
    put_ready_segment(&index, 2, 50, 1);

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log =
        GarbageLog::open(&global_dir, 1024 * 1024, GarbageLogPosition::default()).unwrap();
    let first_frame = vec![
        event(
            b"a",
            1,
            GarbageEvent::SetLifecycle {
                record: record(1, 0, 100),
                lifecycle: None,
            },
            lifecycle_delta(),
        ),
        event(
            b"a",
            2,
            GarbageEvent::Retired {
                record: record(1, 0, 100),
            },
            retire_delta(100),
        ),
        event(
            b"c",
            3,
            GarbageEvent::SetLifecycle {
                record: record(2, 0, 50),
                lifecycle: None,
            },
            lifecycle_delta(),
        ),
    ];
    let head = log.append(&first_frame).unwrap();
    publish_head(&index, head);

    assert_eq!(
        index.get_segment_gc_summary(1).unwrap(),
        Some(live_summary(100, 1))
    );
    assert!(index.get_segment_garbage_log_position(1).unwrap().is_none());

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));

    let first_summary = index.get_segment_gc_summary(1).unwrap().unwrap();
    assert_eq!(first_summary.total_bytes, 100);
    assert_eq!(first_summary.live_bytes, 0);
    assert_eq!(first_summary.retired_bytes, 100);
    let first_overlay = index
        .read_segment_garbage_overlay(&namespace_dir, 1)
        .unwrap()
        .unwrap();
    assert_eq!(first_overlay.summary, first_summary);
    assert_eq!(
        first_overlay.retired,
        vec![SegmentGcRecordRange {
            offset: 0,
            len: 100,
        }]
    );
    let second_summary = index.get_segment_gc_summary(2).unwrap().unwrap();
    assert_eq!(second_summary.total_bytes, 50);
    assert_eq!(second_summary.live_bytes, 50);

    let first_path = namespace_dir.join("segments/1.glog");
    let first_position = index.get_segment_garbage_log_position(1).unwrap().unwrap();
    assert_eq!(
        read_segment_garbage(first_path, first_position).unwrap(),
        first_frame[..2]
    );

    let second_frame = vec![event(
        b"c",
        4,
        GarbageEvent::Retired {
            record: record(2, 0, 50),
        },
        retire_delta(50),
    )];
    let head = log.append(&second_frame).unwrap();
    publish_head(&index, head);
    // Publishing the global frame alone must not make GC see its summary delta.
    assert_eq!(
        index
            .get_segment_gc_summary(2)
            .unwrap()
            .unwrap()
            .retired_bytes,
        0
    );

    // Model a crash after syncing the local append but before committing its RocksDB batch.
    let second_path = namespace_dir.join("segments/2.glog");
    let second_position = index.get_segment_garbage_log_position(2).unwrap().unwrap();
    let orphan = event(
        b"orphan",
        99,
        GarbageEvent::Retired {
            record: record(2, 50, 1),
        },
        retire_delta(1),
    );
    let mut local = SegmentGarbageLog::open(&second_path, second_position).unwrap();
    local.append(std::slice::from_ref(&orphan)).unwrap();
    drop(local);

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    let summary = index.get_segment_gc_summary(2).unwrap().unwrap();
    assert_eq!(summary.total_bytes, 50);
    assert_eq!(summary.live_bytes, 0);
    assert_eq!(summary.retired_bytes, 50);
    let overlay = index
        .read_segment_garbage_overlay(&namespace_dir, 2)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.summary, summary);
    assert_eq!(
        overlay.retired,
        vec![SegmentGcRecordRange { offset: 0, len: 50 }]
    );

    let second_position = index.get_segment_garbage_log_position(2).unwrap().unwrap();
    let local_events = read_segment_garbage(second_path, second_position).unwrap();
    assert_eq!(
        local_events,
        vec![first_frame[2].clone(), second_frame[0].clone()]
    );
    assert!(!local_events.contains(&orphan));
    assert!(!sweep(&index, &global_dir, &namespace_dir));
}

#[tokio::test]
async fn sweep_discards_events_for_wholesale_deleted_segments() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    put_ready_segment(&index, 1, 100, 1);
    let mut deleted = index.get_segment_state(1).unwrap().unwrap();
    deleted.state = SegmentFileState::Deleted;
    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &deleted).unwrap();
    batch.write_with_sync(true).unwrap();

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log =
        GarbageLog::open(&global_dir, 1024 * 1024, GarbageLogPosition::default()).unwrap();
    let head = log
        .append(&[event(
            b"deleted",
            5,
            GarbageEvent::Retired {
                record: record(1, 0, 100),
            },
            retire_delta(100),
        )])
        .unwrap();
    publish_head(&index, head);

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    assert!(index.get_segment_garbage_log_position(1).unwrap().is_none());
    assert_eq!(
        index.get_segment_gc_summary(1).unwrap(),
        Some(live_summary(100, 1))
    );
}

#[tokio::test]
async fn sweep_reclaims_a_global_log_after_crossing_its_boundary() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    put_ready_segment(&index, 1, 100, 1);

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log = GarbageLog::open(&global_dir, 1, GarbageLogPosition::default()).unwrap();
    let first = log
        .append(&[event(
            b"a",
            1,
            GarbageEvent::SetLifecycle {
                record: record(1, 0, 100),
                lifecycle: None,
            },
            lifecycle_delta(),
        )])
        .unwrap();
    let head = log
        .append(&[event(
            b"a",
            2,
            GarbageEvent::Retired {
                record: record(1, 0, 100),
            },
            retire_delta(100),
        )])
        .unwrap();
    assert_eq!((first.log_id, head.log_id), (1, 2));
    publish_head(&index, head);

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    assert!(!GarbageLog::path(&global_dir, 1).exists());
    assert!(GarbageLog::path(&global_dir, 2).exists());
    assert!(!sweep(&index, &global_dir, &namespace_dir));
}

#[tokio::test]
async fn sweep_stops_at_the_frame_batch_limit() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    put_ready_segment(&index, 1, 257, 257);

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log =
        GarbageLog::open(&global_dir, 1024 * 1024, GarbageLogPosition::default()).unwrap();
    let mut positions = Vec::new();
    for sequence in 1..=257 {
        positions.push(
            log.append(&[event(
                b"a",
                sequence,
                GarbageEvent::SetLifecycle {
                    record: record(1, sequence - 1, 1),
                    lifecycle: None,
                },
                lifecycle_delta(),
            )])
            .unwrap(),
        );
    }
    publish_head(&index, positions[256]);

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(
        index.get_garbage_log_position(CURSOR).unwrap(),
        Some(positions[255])
    );
    assert_eq!(
        index
            .get_segment_gc_summary(1)
            .unwrap()
            .unwrap()
            .total_bytes,
        257
    );

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(
        index.get_garbage_log_position(CURSOR).unwrap(),
        Some(positions[256])
    );
    assert_eq!(
        index
            .get_segment_gc_summary(1)
            .unwrap()
            .unwrap()
            .total_bytes,
        257
    );
}

#[tokio::test]
async fn sweep_waits_for_the_segment_baseline() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    index.put_segment_state(&segment_state(1)).unwrap();

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log =
        GarbageLog::open(&global_dir, 1024 * 1024, GarbageLogPosition::default()).unwrap();
    let head = log
        .append(&[event(
            b"a",
            1,
            GarbageEvent::Retired {
                record: record(1, 0, 10),
            },
            SegmentGcSummaryDelta::default(),
        )])
        .unwrap();
    publish_head(&index, head);

    assert!(!sweep(&index, &global_dir, &namespace_dir));
    assert!(index.get_garbage_log_position(CURSOR).unwrap().is_none());
    assert!(index.get_segment_garbage_log_position(1).unwrap().is_none());

    let mut batch = index.batch();
    index
        .put_segment_gc_summary_batch(&mut batch, 1, &live_summary(10, 1))
        .unwrap();
    batch.write_with_sync(true).unwrap();

    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
}

#[tokio::test]
async fn completed_cursor_finishes_reclamation_after_restart() {
    init_typed_store_metrics();
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();

    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log = GarbageLog::open(&global_dir, 1, GarbageLogPosition::default()).unwrap();
    log.append(&[event(
        b"a",
        1,
        GarbageEvent::Retired {
            record: record(1, 0, 100),
        },
        retire_delta(100),
    )])
    .unwrap();
    let head = log
        .append(&[event(
            b"b",
            2,
            GarbageEvent::Retired {
                record: record(1, 100, 100),
            },
            retire_delta(100),
        )])
        .unwrap();
    assert_eq!(head.log_id, 2);
    publish_head(&index, head);
    let mut batch = index.batch();
    index
        .put_garbage_log_position_batch(&mut batch, CURSOR, head)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    assert!(!sweep(&index, &global_dir, &namespace_dir));
    assert!(!GarbageLog::path(&global_dir, 1).exists());
    assert!(GarbageLog::path(&global_dir, 2).exists());
}

fn sweep(
    index: &StrataIndex,
    global_dir: &std::path::Path,
    namespace_dir: &std::path::Path,
) -> bool {
    index
        .sweep_garbage_log(global_dir, namespace_dir, HEAD, CURSOR)
        .unwrap()
}

fn publish_head(index: &StrataIndex, position: GarbageLogPosition) {
    let mut batch = index.batch();
    index
        .put_garbage_log_position_batch(&mut batch, HEAD, position)
        .unwrap();
    batch.write_with_sync(true).unwrap();
}

fn event(
    key: &[u8],
    sequence: u64,
    event: GarbageEvent,
    delta: SegmentGcSummaryDelta,
) -> GarbageRecord {
    let segment_id = match &event {
        GarbageEvent::Retired { record }
        | GarbageEvent::Expired { record }
        | GarbageEvent::SetLifecycle { record, .. } => record.segment_id,
    };
    GarbageRecord {
        key: SegmentKey {
            segment_id,
            blob_key: BlobKey::new(key).unwrap(),
        },
        lsn: sequence,
        event,
        summary_delta: delta,
    }
}

fn record(segment_id: SegmentId, offset: u64, len: u64) -> RecordRef {
    RecordRef {
        segment_id,
        offset,
        len,
    }
}

fn lifecycle_delta() -> SegmentGcSummaryDelta {
    SegmentGcSummaryDelta::default()
}

fn retire_delta(bytes: i128) -> SegmentGcSummaryDelta {
    SegmentGcSummaryDelta {
        live_bytes: -bytes,
        retired_bytes: bytes,
        live_ref_count: -1,
        unknown_lifetime_bytes: -bytes,
        unknown_lifetime_ref_count: -1,
        ..Default::default()
    }
}

fn put_ready_segment(index: &StrataIndex, segment_id: SegmentId, bytes: u64, refs: u64) {
    let mut batch = index.batch();
    index
        .put_segment_state_batch(&mut batch, &segment_state(segment_id))
        .unwrap();
    index
        .put_segment_gc_summary_batch(&mut batch, segment_id, &live_summary(bytes, refs))
        .unwrap();
    batch.write_with_sync(true).unwrap();
}

fn live_summary(bytes: u64, refs: u64) -> SegmentGcSummary {
    SegmentGcSummary {
        total_bytes: bytes,
        live_bytes: bytes,
        live_ref_count: refs,
        unknown_lifetime_bytes: bytes,
        unknown_lifetime_ref_count: refs,
        ..Default::default()
    }
}

fn segment_state(segment_id: SegmentId) -> SegmentState {
    SegmentState {
        owner: SegmentOwner::Store,
        segment_id,
        volume_id: 0,
        path: format!("segments/{segment_id}.data"),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Sealed,
        write_offset: 0,
        durable_offset: 0,
        min_lsn: None,
        max_lsn: None,
        sealed_before_lsn: None,
        sealed_len: Some(0),
        sealed_sha256: Some([0; 32]),
    }
}
