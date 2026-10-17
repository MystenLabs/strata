use core_types::{
    BlobKey, BlobLifecycle, GarbageEvent, RecordRef, SegmentGcLifetimeRange, SegmentGcRecordRange,
    SegmentGcSummary, SegmentKey,
};
use lsm::{GarbageRecord, SegmentGarbageLog, fold_segment_garbage, read_segment_garbage};

#[test]
#[cfg(unix)]
fn validated_writer_resumes_only_the_same_committed_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7.glog");
    let first = event(b"a", 1, retire(1, 6));
    let mut file = SegmentGarbageLog::open(&path, 0).unwrap();
    let committed = file.append(std::slice::from_ref(&first)).unwrap();
    assert!(file.is_at_committed_position(&path, committed).unwrap());
    assert!(!file.is_at_committed_position(&path, 0).unwrap());
    assert!(!file.is_at_committed_position(&path, committed - 1).unwrap());
    assert!(
        !file
            .is_at_committed_position(&dir.path().join("8.glog"), committed)
            .unwrap()
    );

    let second = event(b"b", 2, retire(20, 5));
    let next = file.append(std::slice::from_ref(&second)).unwrap();
    assert!(!file.is_at_committed_position(&path, committed).unwrap());
    assert!(file.is_at_committed_position(&path, next).unwrap());
    assert_eq!(
        read_segment_garbage(&path, next).unwrap(),
        vec![first, second]
    );

    // An external append invalidates a cached writer even if its own offset is unchanged.
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"unpublished tail")
        .unwrap();
    assert!(!file.is_at_committed_position(&path, next).unwrap());
    file = SegmentGarbageLog::open(&path, next).unwrap();
    assert!(file.is_at_committed_position(&path, next).unwrap());

    // A same-length replacement must not reuse the old open file or its validated history.
    let replacement = dir.path().join("replacement.glog");
    std::fs::copy(&path, &replacement).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    assert!(!file.is_at_committed_position(&path, next).unwrap());
    file = SegmentGarbageLog::open(&path, next).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(next - 1)
        .unwrap();
    assert!(!file.is_at_committed_position(&path, next).unwrap());
    assert!(SegmentGarbageLog::open(&path, next).is_err());
}

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

#[test]
fn reopens_legacy_lifetime_count_corrections_in_publication_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7.glog");
    let record = record(1, 6);
    let hint = |extension_count| {
        event(
            b"a",
            500,
            GarbageEvent::SetLifecycle {
                record,
                lifecycle: Some(BlobLifecycle {
                    logical_end_epoch: 54,
                    extension_count,
                }),
            },
        )
    };
    let mut file = SegmentGarbageLog::open(&path, 0).unwrap();
    file.append(&[hint(1)]).unwrap();
    let committed = file.append(&[hint(16), hint(16)]).unwrap();
    drop(file);
    let summary = SegmentGcSummary {
        total_bytes: 6,
        live_bytes: 6,
        ..Default::default()
    };
    let overlay = fold_segment_garbage(
        read_segment_garbage(&path, committed).unwrap(),
        summary.clone(),
    )
    .unwrap();
    assert_eq!(overlay.summary, summary);
    assert_eq!(overlay.lifetimes[0].lifecycle.extension_count, 16);
    assert_eq!(overlay.lifetimes[0].lifecycle.logical_end_epoch, 54);

    // Publication order, rather than the numeric maximum, must agree with the live sweeper.
    let overlay = fold_segment_garbage(vec![hint(16), hint(1)], summary).unwrap();
    assert_eq!(overlay.lifetimes[0].lifecycle.extension_count, 1);
}

#[test]
fn rejects_different_expiry_at_one_position() {
    let record = record(1, 6);
    let records = [10, 20].map(|end| {
        event(
            b"a",
            1,
            GarbageEvent::SetLifecycle {
                record,
                lifecycle: Some(BlobLifecycle::new(end)),
            },
        )
    });
    assert!(matches!(
        fold_segment_garbage(records.to_vec(), Default::default()),
        Err(lsm::Error::InvalidGarbageLog(_))
    ));
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
