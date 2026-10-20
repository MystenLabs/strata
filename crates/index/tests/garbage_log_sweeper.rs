use std::collections::BTreeMap;

use core_types::{
    BlobKey, BlobLifecycle, EpochBucket, PlacementClass, RecordRef, SegmentFileState,
    SegmentGcRecordRange, SegmentGcSummary, SegmentGcSummaryDelta, SegmentId, SegmentOwner,
    SegmentState,
};
use index::StrataIndex;
use lsm::{
    GarbageEvent, GarbageLog, GarbageLogPosition, GarbageRecord, SegmentGarbageLog, SegmentKey,
    read_segment_garbage,
};
use tempfile::TempDir;

const HEAD: &str = "segment-ref";
const CURSOR: &str = "segment-ref-sweep";

#[tokio::test]
async fn failed_sweep_drops_cached_writers_and_retries_from_committed_positions() {
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(directory.path().join("index"), "strata", "test").unwrap();
    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log = GarbageLog::open(&global_dir, 1024 * 1024, Default::default()).unwrap();
    let initial: Vec<_> = (1..=2)
        .map(|id| {
            put_ready_segment(&index, id, 100, 1);
            event(
                b"a",
                1,
                GarbageEvent::SetLifecycle {
                    record: record(id, 0, 100),
                    lifecycle: None,
                },
                Default::default(),
            )
        })
        .collect();
    publish_head(&index, log.append(&initial).unwrap());
    assert!(sweep(&index, &global_dir, &namespace_dir));
    let committed_cursor = index.get_garbage_log_position(CURSOR).unwrap();
    let committed_position = index.get_segment_garbage_log_position(1).unwrap().unwrap();

    let retirements: Vec<_> = (1..=2)
        .map(|id| {
            event(
                b"a",
                2,
                GarbageEvent::Retired {
                    record: record(id, 0, 100),
                },
                retire_delta(100),
            )
        })
        .collect();
    let head = log.append(&retirements).unwrap();
    publish_head(&index, head);
    // A forwarding route must survive the failed batch, including after writers appended.
    put_ready_segment(&index, 3, 100, 1);
    let mut activation = index.batch();
    index
        .put_garbage_forwarding_batch(
            &mut activation,
            head,
            [(record(1, 0, 100), record(3, 0, 100))],
        )
        .unwrap();
    activation.write_with_sync(true).unwrap();
    // Segment 1 appends successfully, then segment 2 fails before the metadata commit.
    let second_path = namespace_dir.join("segments/2.glog");
    let saved = std::fs::read(&second_path).unwrap();
    std::fs::write(&second_path, []).unwrap();
    assert!(
        index
            .sweep_garbage_log(&global_dir, &namespace_dir, HEAD, CURSOR)
            .is_err()
    );
    assert_eq!(
        index.get_garbage_log_position(CURSOR).unwrap(),
        committed_cursor
    );
    assert_eq!(
        index.get_segment_garbage_log_position(1).unwrap(),
        Some(committed_position)
    );
    assert_eq!(
        index.get_segment_gc_summary(1).unwrap().unwrap().live_bytes,
        100
    );
    assert!(
        std::fs::metadata(namespace_dir.join("segments/1.glog"))
            .unwrap()
            .len()
            > committed_position
    );

    std::fs::write(&second_path, saved).unwrap();
    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    assert_eq!(
        index.get_segment_gc_summary(3).unwrap().unwrap().live_bytes,
        0
    );
    for id in 1..=2 {
        let position = index.get_segment_garbage_log_position(id).unwrap().unwrap();
        let events =
            read_segment_garbage(namespace_dir.join(format!("segments/{id}.glog")), position)
                .unwrap();
        assert_eq!(
            events,
            vec![
                initial[id as usize - 1].clone(),
                retirements[id as usize - 1].clone()
            ]
        );
        let summary = index.get_segment_gc_summary(id).unwrap().unwrap();
        assert_eq!(summary.live_bytes, 0);
        assert_eq!(summary.retired_bytes, 100);
    }
}

#[tokio::test]
async fn sweep_copies_details_before_publishing_summaries_and_cursor() {
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
async fn repeated_lifecycle_restatement_is_idempotent_for_summary_merges() {
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
    let mut log =
        GarbageLog::open(&global_dir, 1024 * 1024, GarbageLogPosition::default()).unwrap();
    let lifecycle = BlobLifecycle {
        logical_end_epoch: 50,
        extension_count: 2,
    };
    let restatement = event(
        b"a",
        1,
        GarbageEvent::SetLifecycle {
            record: record(1, 0, 100),
            lifecycle: Some(lifecycle),
        },
        set_lifecycle_delta(100, lifecycle),
    );

    let first_head = log.append(std::slice::from_ref(&restatement)).unwrap();
    publish_head(&index, first_head);
    assert!(sweep(&index, &global_dir, &namespace_dir));

    let expected = SegmentGcSummary {
        total_bytes: 100,
        live_bytes: 100,
        live_ref_count: 1,
        min_live_end_epoch: Some(50),
        max_live_end_epoch: Some(50),
        future_epoch_histogram: BTreeMap::from([(
            50,
            EpochBucket {
                bytes: 100,
                refs: 1,
            },
        )]),
        extension_count_histogram: BTreeMap::from([(2, 1)]),
        ..Default::default()
    };
    assert_eq!(
        index.get_segment_gc_summary(1).unwrap(),
        Some(expected.clone())
    );

    // Relocation-aware compaction may emit this exact state assignment again in a later frame.
    // Its stored transition delta is not idempotent, but applying the event to the overlay is.
    let second_head = log.append(std::slice::from_ref(&restatement)).unwrap();
    publish_head(&index, second_head);
    assert!(sweep(&index, &global_dir, &namespace_dir));
    assert_eq!(
        index.get_segment_gc_summary(1).unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        index
            .read_segment_garbage_overlay(&namespace_dir, 1)
            .unwrap()
            .unwrap()
            .summary,
        expected
    );
}

#[tokio::test]
async fn legacy_lifetime_count_correction_survives_reopen_and_retirement() {
    let directory = TempDir::new().unwrap();
    let open = || {
        StrataIndex::open_path(
            directory.path().join("index"),
            "strata",
            directory.path().display().to_string(),
        )
        .unwrap()
    };
    let index = open();
    put_ready_segment(&index, 1, 100, 1);
    let global_dir = directory.path().join("global-garbage");
    let namespace_dir = directory.path().join("namespace");
    let mut log = GarbageLog::open(&global_dir, 1024 * 1024, Default::default()).unwrap();
    for extension_count in [1, 16] {
        let lifecycle = BlobLifecycle {
            logical_end_epoch: 54,
            extension_count,
        };
        let head = log
            .append(&[event(
                b"a",
                500,
                GarbageEvent::SetLifecycle {
                    record: record(1, 0, 100),
                    lifecycle: Some(lifecycle),
                },
                set_lifecycle_delta(100, lifecycle),
            )])
            .unwrap();
        publish_head(&index, head);
        assert!(sweep(&index, &global_dir, &namespace_dir));
    }
    let expected = index.get_segment_gc_summary(1).unwrap().unwrap();
    assert_eq!(
        expected.extension_count_histogram,
        BTreeMap::from([(16, 1)])
    );
    drop(index);

    // Reopening removes the overlay cache. GC and the next sweep must fold the persisted log.
    let index = open();
    let overlay = index
        .read_segment_garbage_overlay(&namespace_dir, 1)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.summary, expected);
    assert_eq!(overlay.lifetimes[0].lifecycle.extension_count, 16);
    let head = log
        .append(&[event(
            b"a",
            501,
            GarbageEvent::Retired {
                record: record(1, 0, 100),
            },
            Default::default(),
        )])
        .unwrap();
    publish_head(&index, head);
    assert!(sweep(&index, &global_dir, &namespace_dir));
    let summary = index.get_segment_gc_summary(1).unwrap().unwrap();
    assert_eq!(summary.live_bytes, 0);
    assert_eq!(summary.retired_bytes, 100);
    assert!(summary.extension_count_histogram.is_empty());
    assert!(summary.future_epoch_histogram.is_empty());
}

#[tokio::test]
async fn sweep_discards_events_for_wholesale_deleted_segments() {
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

fn set_lifecycle_delta(bytes: i128, lifecycle: BlobLifecycle) -> SegmentGcSummaryDelta {
    SegmentGcSummaryDelta {
        unknown_lifetime_bytes: -bytes,
        unknown_lifetime_ref_count: -1,
        epoch_bytes: BTreeMap::from([(lifecycle.logical_end_epoch, bytes)]),
        epoch_refs: BTreeMap::from([(lifecycle.logical_end_epoch, 1)]),
        extension_counts: BTreeMap::from([(lifecycle.extension_count, 1)]),
        ..Default::default()
    }
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

#[tokio::test]
async fn sweep_bounds_records_even_when_few_frames_are_pending() {
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    let records_per_frame = 16_384u64;
    let count = 3 * records_per_frame;
    put_ready_segment(&index, 1, count, count);
    let global = directory.path().join("global");
    let namespace = directory.path().join("namespace");
    let mut log =
        GarbageLog::open(&global, 64 * 1024 * 1024, GarbageLogPosition::default()).unwrap();
    for frame in 0..3 {
        let records: Vec<_> = (frame * records_per_frame..(frame + 1) * records_per_frame)
            .map(|i| {
                event(
                    &i.to_be_bytes(),
                    i + 1,
                    GarbageEvent::Retired {
                        record: record(1, i, 1),
                    },
                    retire_delta(1),
                )
            })
            .collect();
        log.append(&records).unwrap();
    }
    let head = log.position();
    publish_head(&index, head);
    assert!(sweep(&index, &global, &namespace));
    assert_ne!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    assert_eq!(
        index
            .get_segment_gc_summary(1)
            .unwrap()
            .unwrap()
            .retired_bytes,
        2 * records_per_frame
    );
    assert!(sweep(&index, &global, &namespace));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    assert_eq!(
        index
            .get_segment_gc_summary(1)
            .unwrap()
            .unwrap()
            .retired_bytes,
        count
    );
}

/// A pending lifetime and deletion must follow both relocations across a restart. The GC
/// retirement frames after each route's boundary must affect only their source copies.
#[test]
fn pending_garbage_follows_moves_after_restart_without_retiring_live_copies() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("index");
    let global = dir.path().join("global");
    let namespace = dir.path().join("namespace");
    let index = StrataIndex::open_path(&db, "strata", "test").unwrap();
    for id in 1..=3 {
        put_ready_segment(&index, id, 200, 2);
    }
    let mut log = GarbageLog::open(&global, 1, Default::default()).unwrap();
    let pending = vec![
        event(
            b"a",
            2,
            GarbageEvent::SetLifecycle {
                record: record(1, 0, 100),
                lifecycle: Some(BlobLifecycle {
                    logical_end_epoch: 50,
                    extension_count: 0,
                }),
            },
            Default::default(),
        ),
        event(
            b"b",
            3,
            GarbageEvent::Retired {
                record: record(1, 100, 100),
            },
            Default::default(),
        ),
    ];
    let before_first_move = log.append(&pending).unwrap();
    publish_head(&index, before_first_move);
    let mut batch = index.batch();
    index
        .put_garbage_forwarding_batch(
            &mut batch,
            before_first_move,
            [
                (record(1, 0, 100), record(2, 0, 100)),
                (record(1, 100, 100), record(2, 100, 100)),
            ],
        )
        .unwrap();
    batch.write_with_sync(true).unwrap();
    let old_retirements = |id| {
        vec![
            event(
                b"a",
                u64::MAX,
                GarbageEvent::Retired {
                    record: record(id, 0, 100),
                },
                Default::default(),
            ),
            event(
                b"b",
                u64::MAX,
                GarbageEvent::Retired {
                    record: record(id, 100, 100),
                },
                Default::default(),
            ),
        ]
    };
    let before_second_move = log.append(&old_retirements(1)).unwrap();
    publish_head(&index, before_second_move);
    let mut batch = index.batch();
    index
        .put_garbage_forwarding_batch(
            &mut batch,
            before_second_move,
            [
                (record(2, 0, 100), record(3, 0, 100)),
                (record(2, 100, 100), record(3, 100, 100)),
            ],
        )
        .unwrap();
    batch.write_with_sync(true).unwrap();
    let head = log.append(&old_retirements(2)).unwrap();
    publish_head(&index, head);
    // Nothing has been swept, and neither activation waited for it.
    assert!(index.get_garbage_log_position(CURSOR).unwrap().is_none());
    drop(index);
    let index = StrataIndex::open_path(&db, "strata", "test").unwrap();
    assert!(sweep(&index, &global, &namespace));
    assert_eq!(index.get_garbage_log_position(CURSOR).unwrap(), Some(head));
    for id in 1..=2 {
        assert_eq!(
            index
                .get_segment_gc_summary(id)
                .unwrap()
                .unwrap()
                .live_bytes,
            0
        );
    }
    let overlay = index
        .read_segment_garbage_overlay(&namespace, 3)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.summary.live_bytes, 100);
    assert_eq!(overlay.summary.retired_bytes, 100);
    assert_eq!(overlay.summary.future_epoch_histogram[&50].bytes, 100);
    assert_eq!(
        overlay.retired,
        vec![SegmentGcRecordRange {
            offset: 100,
            len: 100
        }]
    );
    assert_eq!(overlay.lifetimes[0].lifecycle.logical_end_epoch, 50);
    // A retry after a completed cursor neither duplicates deltas nor re-forwards retirement.
    drop(index);
    let index = StrataIndex::open_path(&db, "strata", "test").unwrap();
    assert!(!sweep(&index, &global, &namespace));
    assert_eq!(
        index
            .read_segment_garbage_overlay(&namespace, 3)
            .unwrap()
            .unwrap(),
        overlay
    );
}

#[test]
fn reducers_preserve_each_segments_event_order_across_frames() {
    let dir = TempDir::new().unwrap();
    let index = StrataIndex::open_path(dir.path().join("index"), "strata", "test").unwrap();
    let global = dir.path().join("global");
    let namespace = dir.path().join("namespace");
    let mut log = GarbageLog::open(&global, 1 << 20, Default::default()).unwrap();
    for id in 1..=32 {
        put_ready_segment(&index, id, 100, 1);
    }
    let mut head = Default::default();
    for lsn in 1..=5 {
        let records = (1..=32)
            .map(|id| {
                event(
                    b"a",
                    lsn,
                    GarbageEvent::SetLifecycle {
                        record: record(id, 0, 100),
                        lifecycle: Some(BlobLifecycle {
                            logical_end_epoch: 10 + lsn,
                            extension_count: lsn as u32,
                        }),
                    },
                    Default::default(),
                )
            })
            .collect::<Vec<_>>();
        head = log.append(&records).unwrap();
    }
    publish_head(&index, head);
    assert!(sweep(&index, &global, &namespace));
    for id in 1..=32 {
        let overlay = index
            .read_segment_garbage_overlay(&namespace, id)
            .unwrap()
            .unwrap();
        assert_eq!(overlay.summary.live_bytes, 100);
        assert_eq!(overlay.summary.future_epoch_histogram.len(), 1);
        assert_eq!(overlay.summary.future_epoch_histogram[&15].bytes, 100);
        assert_eq!(overlay.lifetimes[0].lifecycle.logical_end_epoch, 15);
        assert_eq!(overlay.lifetimes[0].lifecycle.extension_count, 5);
    }
}
