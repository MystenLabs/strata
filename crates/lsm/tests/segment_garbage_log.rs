use core_types::{
    BlobKey, BlobLifecycle, GarbageEvent, RecordRef, SegmentGcLifetimeRange, SegmentGcRecordRange,
    SegmentGcSummary, SegmentKey,
};
use lsm::{GarbageRecord, SegmentGarbageLog, fold_segment_garbage, read_segment_garbage};

#[test]
fn reopen_discards_an_unpublished_tail_before_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7.glog");
    let first = vec![event(b"a", 1, retire(1, 6)), event(b"b", 1, retire(20, 5))];

    let mut file = SegmentGarbageLog::open(&path, 0).unwrap();
    let committed = file.append(&first).unwrap();
    file.append(&[event(b"c", 1, retire(40, 10))]).unwrap();
    let committed_records = read_segment_garbage(&path, committed).unwrap();
    assert_eq!(committed_records, first);
    assert_eq!(
        fold_segment_garbage(committed_records, SegmentGcSummary::default())
            .unwrap()
            .retired,
        vec![range(1, 6), range(20, 5)]
    );
    drop(file);

    let replacement = event(b"d", 1, retire(60, 7));
    let mut file = SegmentGarbageLog::open(&path, committed).unwrap();
    let new_committed = file.append(std::slice::from_ref(&replacement)).unwrap();
    drop(file);

    let mut expected = first;
    expected.push(replacement);
    assert_eq!(
        read_segment_garbage(&path, new_committed).unwrap(),
        expected
    );
}

#[test]
fn folds_history_without_reviving_garbage() {
    let retired = record(10, 8);
    let expired = record(30, 4);
    let cleared = record(50, 5);
    let live = record(70, 6);
    let records = vec![
        event(
            b"a",
            1,
            GarbageEvent::SetLifecycle {
                record: retired,
                lifecycle: Some(BlobLifecycle::new(5)),
            },
        ),
        event(
            b"a",
            2,
            GarbageEvent::SetLifecycle {
                record: retired,
                lifecycle: Some(BlobLifecycle::new(9)),
            },
        ),
        event(b"a", 3, GarbageEvent::Retired { record: retired }),
        event(
            b"a",
            4,
            GarbageEvent::SetLifecycle {
                record: retired,
                lifecycle: Some(BlobLifecycle::new(12)),
            },
        ),
        event(
            b"b",
            1,
            GarbageEvent::SetLifecycle {
                record: expired,
                lifecycle: Some(BlobLifecycle::new(6)),
            },
        ),
        event(b"b", 2, GarbageEvent::Expired { record: expired }),
        event(
            b"c",
            1,
            GarbageEvent::SetLifecycle {
                record: cleared,
                lifecycle: Some(BlobLifecycle::new(7)),
            },
        ),
        event(
            b"c",
            2,
            GarbageEvent::SetLifecycle {
                record: cleared,
                lifecycle: None,
            },
        ),
        event(
            b"d",
            1,
            GarbageEvent::SetLifecycle {
                record: live,
                lifecycle: Some(BlobLifecycle::new(11)),
            },
        ),
    ];
    let summary = SegmentGcSummary {
        total_bytes: 23,
        live_bytes: 11,
        retired_bytes: 8,
        expired_bytes: 4,
        ..Default::default()
    };

    let overlay = fold_segment_garbage(records, summary.clone()).unwrap();

    assert_eq!(overlay.retired, vec![SegmentGcRecordRange::from(retired)]);
    assert_eq!(overlay.expired, vec![SegmentGcRecordRange::from(expired)]);
    assert_eq!(
        overlay.lifetimes,
        vec![SegmentGcLifetimeRange {
            range: SegmentGcRecordRange::from(live),
            lifecycle: BlobLifecycle::new(11),
        }]
    );
    assert_eq!(overlay.summary, summary);
}

#[test]
fn folds_identical_duplicate_event_once() {
    let duplicate = event(b"a", 1, retire(1, 6));
    let overlay = fold_segment_garbage(
        vec![duplicate.clone(), duplicate],
        SegmentGcSummary::default(),
    )
    .unwrap();
    assert_eq!(overlay.retired, vec![range(1, 6)]);
}

#[test]
fn rejects_conflicting_event_at_one_position() {
    let record = record(1, 6);
    let error = fold_segment_garbage(
        vec![
            event(b"a", 1, GarbageEvent::Retired { record }),
            event(b"a", 1, GarbageEvent::Expired { record }),
        ],
        SegmentGcSummary::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conflicting events for one key, lsn, and record")
    );
}

fn event(key: &[u8], sequence: u64, event: GarbageEvent) -> GarbageRecord {
    GarbageRecord {
        key: SegmentKey {
            segment_id: 7,
            blob_key: BlobKey::new(key).unwrap(),
        },
        lsn: sequence,
        event,
        summary_delta: Default::default(),
    }
}

fn record(offset: u64, len: u64) -> RecordRef {
    RecordRef {
        segment_id: 7,
        offset,
        len,
    }
}

fn retire(offset: u64, len: u64) -> GarbageEvent {
    GarbageEvent::Retired {
        record: record(offset, len),
    }
}

fn range(offset: u64, len: u64) -> SegmentGcRecordRange {
    SegmentGcRecordRange { offset, len }
}
