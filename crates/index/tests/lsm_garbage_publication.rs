use std::{
    num::NonZeroU32,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use core_types::{BlobKey, RecordRef, SegmentGcSummaryDelta};
use index::StrataIndex;
use lsm::{
    GarbageEvent, GarbageLog, GarbageLogPosition, GarbageRecord, Manifest, ManifestEdit,
    MergeOperator, Result, SegmentKey, StrataLsn, TableStore, TableTarget, TableWriter,
    select_compaction_inputs, write_compaction,
};
use tempfile::TempDir;

const LSM_NAME: &str = "lsm";
const LOG_NAME: &str = "segment-ref";
const SCHEMA: &str = "lsm-v1";
const PATCH_FORMAT: &str = "patch-v1";

struct Replace;

impl MergeOperator for Replace {
    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut value = base.map(<[u8]>::to_vec);
        for (lsn, patch) in patches {
            emit(garbage_record(key, *lsn, patch))?;
            value = Some(patch.to_vec());
        }
        Ok(value)
    }
}

#[tokio::test]
async fn compaction_appends_garbage_before_locking_and_merges_the_latest_manifest() {
    let directory = TempDir::new().unwrap();
    let db_path = directory.path().join("index");
    let lsm_path = directory.path().join("lsm");
    let log_path = directory.path().join("garbage");
    let metric_suffix = directory.path().display().to_string();
    let index = StrataIndex::open_path(&db_path, "strata", &metric_suffix).unwrap();

    let mut base = TableWriter::create_base(&lsm_path, "base.sst", 1, 0, SCHEMA).unwrap();
    base.add(b"key", b"old").unwrap();
    let base = base.finish().unwrap();
    let mut patch = TableWriter::create_patch(&lsm_path, "patch.sst", 2, 0, PATCH_FORMAT).unwrap();
    patch.add_patch(b"key", 10, b"new").unwrap();
    let patch = patch.finish().unwrap();

    let mut manifest = Manifest::empty(SCHEMA, PATCH_FORMAT, NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![base],
            add_patches: vec![patch.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(&mut batch, LSM_NAME, &manifest)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let files = Arc::new(TableStore::new(&lsm_path));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &patch)
        .unwrap()
        .unwrap();
    let (edit, records) =
        write_compaction(&inputs, &Replace, 1024 * 1024, || Ok(TableTarget::base(3))).unwrap();
    assert_eq!(records, [garbage_record(b"key", 10, b"new")]);

    // A tiny soft limit makes the next frame roll, which lets recovery prove it was unpublished.
    let mut log = GarbageLog::open(&log_path, 1, GarbageLogPosition::default()).unwrap();
    let initial_position = log.position();
    let (appended_while_locked, committed) = thread::scope(|scope| {
        let guard = index.lock_lsm_manifests();
        let publisher = scope
            .spawn(|| index.publish_lsm_compaction(LSM_NAME, &edit, LOG_NAME, &mut log, &records));

        // Another publisher holds the manifest lock. Compaction must still be able to write
        // its garbage frame, but cannot expose that frame's position until its edit commits.
        let deadline = Instant::now() + Duration::from_secs(5);
        let appended = loop {
            let position = GarbageLogPosition {
                log_id: initial_position.log_id,
                offset: std::fs::metadata(GarbageLog::path(&log_path, initial_position.log_id))
                    .unwrap()
                    .len(),
            };
            if let Ok(Some((written, _))) =
                GarbageLog::read_next(&log_path, GarbageLogPosition::default(), position)
                && written == records
            {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(index.get_garbage_log_position(LOG_NAME).unwrap(), None);
        assert_eq!(
            index.get_lsm_manifest(LSM_NAME).unwrap(),
            Some(manifest.clone())
        );

        // Advance the frontier while compaction waits. Its previously prepared edit must merge
        // into this newer manifest rather than replacing it with the original snapshot.
        let mut batch = index.batch();
        index
            .merge_lsm_manifest_batch(
                &mut batch,
                LSM_NAME,
                &ManifestEdit {
                    remove: Vec::new(),
                    add_base: Vec::new(),
                    add_patches: Vec::new(),
                    materialized_through: Some(10),
                    wal_retained_from: None,
                },
                &guard,
            )
            .unwrap();
        batch.write_with_sync(true).unwrap();
        drop(guard);
        (appended, publisher.join().unwrap().unwrap())
    });
    assert!(
        appended_while_locked,
        "garbage append waited for the manifest lock"
    );
    assert_eq!(
        index.get_garbage_log_position(LOG_NAME).unwrap(),
        Some(committed)
    );

    let orphan = log
        .append(&[garbage_record(b"later", 11, b"unpublished")])
        .unwrap();
    assert_eq!(orphan.log_id, 2);
    drop(log);
    drop(index);

    let reopened = StrataIndex::open_path(&db_path, "strata", &metric_suffix).unwrap();
    let reopened_manifest = reopened.get_lsm_manifest(LSM_NAME).unwrap().unwrap();
    assert_eq!(reopened_manifest.generation, 3);
    assert_eq!(reopened_manifest.materialized_through, Some(10));
    assert_eq!(
        reopened_manifest.partitions[&0].base[0].relative_path,
        "base-00000000000000000003.sst"
    );
    assert!(reopened_manifest.partitions[&0].patches.is_empty());

    GarbageLog::open(&log_path, 1, committed).unwrap();
    assert!(GarbageLog::path(&log_path, 1).exists());
    assert!(!GarbageLog::path(&log_path, 2).exists());
}

#[tokio::test]
async fn failed_manifest_publication_discards_uncommitted_garbage_on_reopen() {
    let directory = TempDir::new().unwrap();
    let log_path = directory.path().join("garbage");
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    let manifest = Manifest::empty(SCHEMA, PATCH_FORMAT, NonZeroU32::new(1).unwrap());
    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(&mut batch, LSM_NAME, &manifest)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let mut log = GarbageLog::open(&log_path, 1024, GarbageLogPosition::default()).unwrap();
    let initial_position = log.position();
    let records = [garbage_record(b"key", 10, b"old")];
    let invalid_edit = ManifestEdit {
        remove: vec!["missing.sst".to_owned()],
        add_base: Vec::new(),
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    };
    let error = index
        .publish_lsm_compaction(LSM_NAME, &invalid_edit, LOG_NAME, &mut log, &records)
        .unwrap_err();
    assert!(matches!(error, index::Error::InvalidLsmManifest(_)));
    assert!(log.position().offset > initial_position.offset);
    assert_eq!(index.get_garbage_log_position(LOG_NAME).unwrap(), None);
    assert_eq!(index.get_lsm_manifest(LSM_NAME).unwrap(), Some(manifest));

    let edit = ManifestEdit {
        remove: Vec::new(),
        add_base: Vec::new(),
        add_patches: Vec::new(),
        materialized_through: Some(10),
        wal_retained_from: None,
    };
    let error = index
        .publish_lsm_compaction(LSM_NAME, &edit, LOG_NAME, &mut log, &records)
        .unwrap_err();
    assert!(matches!(
        error,
        index::Error::Lsm(lsm::Error::InvalidGarbageLog(_))
    ));
    drop(log);

    // Recovery trusts the committed head, so the frame from the failed edit is discarded.
    let committed = index
        .get_garbage_log_position(LOG_NAME)
        .unwrap()
        .unwrap_or_default();
    let mut log = GarbageLog::open(&log_path, 1024, committed).unwrap();
    assert_eq!(log.position(), initial_position);
    let position = index
        .publish_lsm_compaction(LSM_NAME, &edit, LOG_NAME, &mut log, &records)
        .unwrap();
    assert_eq!(
        index.get_garbage_log_position(LOG_NAME).unwrap(),
        Some(position)
    );
    assert_eq!(
        GarbageLog::read_next(&log_path, committed, position).unwrap(),
        Some((records.to_vec(), position))
    );
    assert_eq!(
        index
            .get_lsm_manifest(LSM_NAME)
            .unwrap()
            .unwrap()
            .materialized_through,
        Some(10)
    );
}

fn garbage_record(key: &[u8], lsn: StrataLsn, value: &[u8]) -> GarbageRecord {
    GarbageRecord {
        key: SegmentKey {
            segment_id: 1,
            blob_key: BlobKey::new(key).unwrap(),
        },
        lsn,
        event: GarbageEvent::Retired {
            record: RecordRef {
                segment_id: 1,
                offset: lsn,
                len: value.len() as u64,
            },
        },
        summary_delta: SegmentGcSummaryDelta::default(),
    }
}

#[tokio::test]
async fn streamed_frames_are_all_unpublished_when_appending_fails() {
    let directory = TempDir::new().unwrap();
    let index = StrataIndex::open_path(
        directory.path().join("index"),
        "strata",
        directory.path().display().to_string(),
    )
    .unwrap();
    let manifest = Manifest::empty(SCHEMA, PATCH_FORMAT, NonZeroU32::new(1).unwrap());
    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(&mut batch, LSM_NAME, &manifest)
        .unwrap();
    batch.write_with_sync(true).unwrap();
    let edit = ManifestEdit {
        remove: Vec::new(),
        add_base: Vec::new(),
        add_patches: Vec::new(),
        materialized_through: Some(20),
        wal_retained_from: None,
    };
    let path = directory.path().join("garbage");
    let mut log = GarbageLog::open(&path, 1, GarbageLogPosition::default()).unwrap();
    let result =
        index.publish_lsm_compaction_streaming(LSM_NAME, &edit, LOG_NAME, &mut log, |log| {
            log.append_unsynced(&[garbage_record(b"a", 10, b"old")])?;
            log.append_unsynced(&[garbage_record(b"b", 20, b"old")])?;
            Err(lsm::Error::InvalidGarbageLog(
                "injected staging read failure".into(),
            ))
        });
    assert!(result.is_err());
    assert_eq!(index.get_garbage_log_position(LOG_NAME).unwrap(), None);
    assert_eq!(index.get_lsm_manifest(LSM_NAME).unwrap(), Some(manifest));
    drop(log);
    let mut log = GarbageLog::open(&path, 1, GarbageLogPosition::default()).unwrap();
    assert!(!GarbageLog::path(&path, 2).exists());
    let position = index
        .publish_lsm_compaction_streaming(LSM_NAME, &edit, LOG_NAME, &mut log, |log| {
            log.append_unsynced(&[garbage_record(b"a", 10, b"old")])?;
            log.append_unsynced(&[garbage_record(b"b", 20, b"old")])?;
            Ok(())
        })
        .unwrap();
    drop(log);
    let _recovered = GarbageLog::open(&path, 1, position).unwrap();
    let (_, first) = GarbageLog::read_next(&path, GarbageLogPosition::default(), position)
        .unwrap()
        .unwrap();
    let (_, last) = GarbageLog::read_next(&path, first, position)
        .unwrap()
        .unwrap();
    assert_eq!(last, position);
    assert_eq!(
        index
            .get_lsm_manifest(LSM_NAME)
            .unwrap()
            .unwrap()
            .materialized_through,
        Some(20)
    );
}
