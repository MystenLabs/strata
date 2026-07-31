use std::{num::NonZeroU32, sync::Arc};

use strata_core::{BlobKey, RecordRef, SegmentGcSummaryDelta};
use strata_index::StrataIndex;
use strata_lsm::{
    GarbageEvent, GarbageLog, GarbageLogPosition, GarbageRecord, Manifest, ManifestEdit,
    MergeOperator, Result, SegmentKey, StrataLsn, TableStore, TableWriter,
    select_compaction_inputs, write_compaction,
};
use tempfile::TempDir;
use typed_store::DBMetrics;

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
async fn compaction_garbage_records_are_synced_before_the_manifest_is_published() {
    DBMetrics::get();
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
    let inputs = select_compaction_inputs(&manifest, &files, 0, &[patch])
        .unwrap()
        .unwrap();
    let (edit, records) = write_compaction(&inputs, &Replace, 1024 * 1024, || {
        (3, "compacted.sst".to_owned())
    })
    .unwrap();
    assert_eq!(records, [garbage_record(b"key", 10, b"new")]);

    // A tiny soft limit makes the next frame roll, which lets recovery prove it was unpublished.
    let mut log = GarbageLog::open(&log_path, 1, GarbageLogPosition::default()).unwrap();
    let committed = index
        .publish_lsm_compaction(LSM_NAME, &edit, LOG_NAME, &mut log, &records)
        .unwrap();
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
    assert_eq!(reopened_manifest.generation, 2);
    assert_eq!(
        reopened_manifest.partitions[&0].base[0].relative_path,
        "compacted.sst"
    );
    assert!(reopened_manifest.partitions[&0].patches.is_empty());

    GarbageLog::open(&log_path, 1, committed).unwrap();
    assert!(GarbageLog::path(&log_path, 1).exists());
    assert!(!GarbageLog::path(&log_path, 2).exists());
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
