use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
};

use strata_core::{BlobKey, BlobLifecycle, RecordRef, SegmentGcSummaryDelta};
use strata_lsm::{Error, GarbageEvent, GarbageLog, GarbageLogPosition, GarbageRecord, SegmentKey};

#[test]
fn append_syncs_frames_and_reopens_at_the_committed_end() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();

    let first = log.append(&[event(1, b"a"), event(2, b"b")]).unwrap();
    let second = log.append(&[event(3, b"c")]).unwrap();
    assert_eq!(first.log_id, 1);
    assert!(second.offset > first.offset);
    drop(log);

    let reopened = GarbageLog::open(dir.path(), 1024, second).unwrap();
    assert_eq!(reopened.position(), second);
    assert_eq!(
        fs::metadata(GarbageLog::path(dir.path(), 1)).unwrap().len(),
        second.offset
    );
}

#[test]
fn reopen_discards_complete_but_uncommitted_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let committed = log.append(&[event(1, b"committed")]).unwrap();
    let uncommitted = log.append(&[event(2, b"uncommitted")]).unwrap();
    assert!(uncommitted.offset > committed.offset);
    drop(log);

    let mut reopened = GarbageLog::open(dir.path(), 1024, committed).unwrap();
    assert_eq!(
        fs::metadata(GarbageLog::path(dir.path(), 1)).unwrap().len(),
        committed.offset
    );
    let replacement = reopened.append(&[event(2, b"replacement")]).unwrap();
    assert_eq!(replacement.log_id, 1);
    assert!(replacement.offset > committed.offset);
}

#[test]
fn reopen_discards_a_torn_uncommitted_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let committed = log.append(&[event(1, b"committed")]).unwrap();
    drop(log);

    let path = GarbageLog::path(dir.path(), 1);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[0xaa; 17])
        .unwrap();
    assert!(fs::metadata(&path).unwrap().len() > committed.offset);

    GarbageLog::open(dir.path(), 1024, committed).unwrap();
    assert_eq!(fs::metadata(path).unwrap().len(), committed.offset);
}

#[test]
fn rollover_is_soft_and_orphaned_new_logs_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    // One record fits, but a second frame rolls to the next file.
    let mut log = GarbageLog::open(dir.path(), 128, GarbageLogPosition::default()).unwrap();
    let committed = log.append(&[event(1, &[1; 40])]).unwrap();
    let uncommitted = log.append(&[event(2, &[2; 40])]).unwrap();
    assert_eq!(committed.log_id, 1);
    assert_eq!(uncommitted.log_id, 2);
    assert!(GarbageLog::path(dir.path(), 2).exists());
    drop(log);

    let mut reopened = GarbageLog::open(dir.path(), 128, committed).unwrap();
    assert!(!GarbageLog::path(dir.path(), 2).exists());
    let replacement = reopened.append(&[event(2, &[3; 40])]).unwrap();
    assert_eq!(replacement.log_id, 2);
}

#[test]
fn reclaim_removes_only_logs_older_than_the_retained_position() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1, GarbageLogPosition::default()).unwrap();
    let first = log.append(&[event(1, b"one")]).unwrap();
    let second = log.append(&[event(2, b"two")]).unwrap();
    let third = log.append(&[event(3, b"three")]).unwrap();
    assert_eq!((first.log_id, second.log_id, third.log_id), (1, 2, 3));

    GarbageLog::reclaim_before(dir.path(), second).unwrap();
    assert!(!GarbageLog::path(dir.path(), 1).exists());
    assert!(GarbageLog::path(dir.path(), 2).exists());
    assert!(GarbageLog::path(dir.path(), 3).exists());

    GarbageLog::reclaim_before(dir.path(), third).unwrap();
    assert!(!GarbageLog::path(dir.path(), 2).exists());
    assert!(GarbageLog::path(dir.path(), 3).exists());
}

#[test]
fn a_single_large_batch_may_exceed_the_rollover_target() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 64, GarbageLogPosition::default()).unwrap();

    let position = log.append(&[event(1, &[7; 128])]).unwrap();
    assert_eq!(position.log_id, 1);
    assert!(position.offset > 64);
}

#[test]
fn corruption_inside_the_committed_prefix_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let committed = log.append(&[event(1, b"event")]).unwrap();
    drop(log);

    let path = GarbageLog::path(dir.path(), 1);
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    // Header + frame prefix + first record length points at the first event byte.
    file.seek(SeekFrom::Start(12 + 12 + 4)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();

    let error = GarbageLog::open(dir.path(), 1024, committed).unwrap_err();
    assert!(matches!(error, Error::CorruptGarbageLog { .. }));
}

#[test]
fn an_empty_batch_is_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let before = log.position();

    assert!(matches!(log.append(&[]), Err(Error::InvalidGarbageLog(_))));
    assert_eq!(log.position(), before);
}

#[test]
fn an_unsorted_batch_is_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let before = log.position();

    let error = log
        .append(&[event(2, b"later"), event(1, b"earlier")])
        .unwrap_err();
    assert!(matches!(error, Error::InvalidGarbageLog(_)));
    assert_eq!(log.position(), before);
}

#[test]
fn committed_frames_can_be_read_across_rolled_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1, GarbageLogPosition::default()).unwrap();
    let first_events = vec![event(1, b"one"), event(2, b"two")];
    let second_events = vec![event(3, b"three")];

    let first = log.append(&first_events).unwrap();
    let head = log.append(&second_events).unwrap();
    assert_eq!(first.log_id, 1);
    assert_eq!(head.log_id, 2);

    let (events, cursor) = GarbageLog::read_next(dir.path(), GarbageLogPosition::default(), head)
        .unwrap()
        .unwrap();
    assert_eq!(events, first_events);
    assert_eq!(cursor, first);

    let (events, cursor) = GarbageLog::read_next(dir.path(), cursor, head)
        .unwrap()
        .unwrap();
    assert_eq!(events, second_events);
    assert_eq!(cursor, head);
    assert!(
        GarbageLog::read_next(dir.path(), cursor, head)
            .unwrap()
            .is_none()
    );
}

#[test]
fn typed_garbage_records_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let records = vec![
        record(
            b"a",
            1,
            GarbageEvent::Retired {
                record: record_ref(1, 10, 20),
            },
            SegmentGcSummaryDelta {
                retired_bytes: 20,
                ..Default::default()
            },
        ),
        record(
            b"b",
            2,
            GarbageEvent::Expired {
                record: record_ref(1, 30, 40),
            },
            SegmentGcSummaryDelta {
                expired_bytes: 40,
                ..Default::default()
            },
        ),
        record(
            b"c",
            3,
            GarbageEvent::SetLifecycle {
                record: record_ref(1, 70, 50),
                lifecycle: Some(BlobLifecycle::new(9)),
            },
            SegmentGcSummaryDelta::default(),
        ),
    ];

    let head = log.append(&records).unwrap();
    assert_eq!(
        GarbageLog::path(dir.path(), 1)
            .extension()
            .and_then(|extension| extension.to_str()),
        Some("glog")
    );
    let (read, cursor) = GarbageLog::read_next(dir.path(), GarbageLogPosition::default(), head)
        .unwrap()
        .unwrap();
    assert_eq!(read, records);
    assert_eq!(cursor, head);
}

#[test]
fn one_transition_can_describe_two_records_for_the_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let records = vec![
        record(
            b"a",
            7,
            GarbageEvent::Retired {
                record: record_ref(1, 10, 20),
            },
            SegmentGcSummaryDelta::default(),
        ),
        record(
            b"a",
            7,
            GarbageEvent::SetLifecycle {
                record: record_ref(1, 30, 20),
                lifecycle: Some(BlobLifecycle::new(9)),
            },
            SegmentGcSummaryDelta::default(),
        ),
    ];

    let head = log.append(&records).unwrap();
    assert_eq!(
        GarbageLog::read_next(dir.path(), GarbageLogPosition::default(), head)
            .unwrap()
            .unwrap()
            .0,
        records
    );
}

#[test]
fn key_and_record_must_name_the_same_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = GarbageLog::open(dir.path(), 1024, GarbageLogPosition::default()).unwrap();
    let mut invalid = event(1, b"key");
    invalid.event = GarbageEvent::Retired {
        record: record_ref(2, 0, 1),
    };

    assert!(matches!(
        log.append(&[invalid]),
        Err(Error::InvalidGarbageLog(_))
    ));
}

fn event(sequence: u64, value: &[u8]) -> GarbageRecord {
    record(
        value,
        sequence,
        GarbageEvent::Retired {
            record: record_ref(1, sequence, value.len() as u64),
        },
        SegmentGcSummaryDelta::default(),
    )
}

fn record(
    key: &[u8],
    sequence: u64,
    event: GarbageEvent,
    summary_delta: SegmentGcSummaryDelta,
) -> GarbageRecord {
    GarbageRecord {
        key: SegmentKey {
            segment_id: 1,
            blob_key: BlobKey::new(key).unwrap(),
        },
        lsn: sequence,
        event,
        summary_delta,
    }
}

fn record_ref(segment_id: u64, offset: u64, len: u64) -> RecordRef {
    RecordRef {
        segment_id,
        offset,
        len,
    }
}
