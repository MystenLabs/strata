use std::{
    num::NonZeroU32,
    thread,
    time::{Duration, Instant},
};

use strata_core::{BlobKey, GarbageEvent, RecordRef, SegmentGcRecordRange, SegmentGcSummaryDelta};
use strata_index::StrataIndex;
use strata_lsm::{GarbageLog, GarbageRecord, Manifest, ManifestEdit, SegmentKey, TableMeta};
use tempfile::tempdir;

use crate::{GARBAGE_LOG_HEAD, garbage_log_dir};

use super::{StrataStoreMetrics, config, init_typed_store_metrics, try_open_standalone_store};

const LSM_NAME: &str = "primary";
const MAX_FILE_BYTES: u64 = 1024 * 1024 * 1024;

#[tokio::test]
async fn running_store_periodically_sweeps_a_committed_frame() {
    init_typed_store_metrics();
    let directory = tempdir().unwrap();
    let mut store_config = config(directory.path(), "garbage-sweeper");
    store_config.gc_workers_enabled = false;
    let store = try_open_standalone_store(store_config, StrataStoreMetrics::default()).unwrap();

    initialize_manifest(store.index());
    publish_compaction(store.index(), store.config());

    wait_for_sweep(&store);
}

#[tokio::test]
async fn store_open_sweeps_an_already_committed_global_frame() {
    init_typed_store_metrics();
    let directory = tempdir().unwrap();
    let mut store_config = config(directory.path(), "garbage-recovery");
    store_config.gc_workers_enabled = false;

    let index = StrataIndex::open_path(
        store_config.standalone_index_dir(),
        store_config.index_cf_prefix(),
        directory.path().display().to_string(),
    )
    .unwrap();
    initialize_manifest(&index);
    publish_compaction(&index, &store_config);
    drop(index);

    let store = try_open_standalone_store(store_config, StrataStoreMetrics::default()).unwrap();
    wait_for_sweep(&store);
}

fn publish_compaction(index: &StrataIndex, config: &crate::StrataStoreConfig) {
    let mut batch = index.batch();
    index
        .put_segment_gc_summary_batch(&mut batch, 1, &Default::default())
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let committed = index
        .get_garbage_log_position(GARBAGE_LOG_HEAD)
        .unwrap()
        .unwrap_or_default();
    let mut log = GarbageLog::open(garbage_log_dir(config), MAX_FILE_BYTES, committed).unwrap();
    index
        .publish_lsm_compaction(
            LSM_NAME,
            &manifest_edit(),
            GARBAGE_LOG_HEAD,
            &mut log,
            &[garbage_record()],
        )
        .unwrap();
}

fn initialize_manifest(index: &StrataIndex) {
    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(
            &mut batch,
            LSM_NAME,
            &Manifest::empty(
                "primary-v1",
                "primary-patch-v1",
                NonZeroU32::new(1).unwrap(),
            ),
        )
        .unwrap();
    batch.write_with_sync(true).unwrap();
}

fn manifest_edit() -> ManifestEdit {
    ManifestEdit {
        remove: Vec::new(),
        add_base: vec![TableMeta {
            id: 1,
            partition: 0,
            relative_path: "base.sst".to_owned(),
            first_key: b"a".to_vec(),
            last_key: b"z".to_vec(),
            min_lsn: None,
            max_lsn: None,
            record_count: 1,
            file_len: 1,
            checksum: [1; 32],
        }],
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    }
}

fn garbage_record() -> GarbageRecord {
    let record = RecordRef {
        segment_id: 1,
        offset: 0,
        len: 10,
    };
    GarbageRecord {
        key: SegmentKey {
            segment_id: 1,
            blob_key: BlobKey::new(b"blob".to_vec()).unwrap(),
        },
        lsn: 1,
        event: GarbageEvent::Retired { record },
        summary_delta: SegmentGcSummaryDelta {
            total_bytes: 10,
            retired_bytes: 10,
            ..Default::default()
        },
    }
}

fn wait_for_sweep(store: &super::StandaloneStore) {
    let started = Instant::now();
    loop {
        let overlay = store
            .index()
            .read_segment_garbage_overlay(store.config().namespace_dir(), 1)
            .unwrap()
            .unwrap();
        if overlay.summary.retired_bytes == 10 {
            assert_eq!(
                overlay.retired,
                vec![SegmentGcRecordRange { offset: 0, len: 10 }]
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "garbage sweeper did not publish the segment-local state"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
