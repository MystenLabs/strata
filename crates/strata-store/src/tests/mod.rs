use std::{
    cmp::Reverse,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::Path,
    sync::{Arc, Mutex, Once, mpsc},
    thread,
    time::{Duration, Instant},
};

use prometheus::Registry;
use strata_accounting::{
    AccountingIndex, AccountingIndexConfig, ActiveDeltaLogReadCursor, ActiveDeltaLogState,
};
use strata_core::{
    BlobLifecycle, EpochBucket, FIXED_RECORD_HEADER_LEN, SegmentGcLifetimeRange,
    SegmentGcRecordRange, SegmentRefEvent, SegmentRefEventKey, StrataStoreState,
};
use strata_gc::{
    DestinationClass, GcAction, GcCopyRecord, GcPlan, GcPlanner, GcPlannerConfig, GcScenario,
};
use tempfile::tempdir;
use typed_store::{
    DBMetrics,
    rocks::{MetricConf, open_cf},
};

use super::*;

static INIT_TYPED_STORE_METRICS: Once = Once::new();
const TEST_KEY_LEN: u64 = 6;
const TEST_PAYLOAD_LEN: u64 = 9;
const TEST_RECORD_LEN: u64 = FIXED_RECORD_HEADER_LEN as u64 + TEST_KEY_LEN + TEST_PAYLOAD_LEN;
const TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD: u64 = TEST_RECORD_LEN * 2 - 1;

fn open_test_index(path: impl AsRef<Path>, cf_prefix: impl AsRef<str>) -> StrataIndex {
    let path = path.as_ref();
    StrataIndex::open_path(path, cf_prefix, path.display().to_string()).unwrap()
}

fn test_active_accounting_log(cfg: &StrataStoreConfig, segment_id: SegmentId) -> ActiveDeltaLog {
    ActiveDeltaLog::open(
        cfg.accounting_index_dir(),
        segment_id,
        ActiveDeltaLogState::default(),
    )
    .unwrap()
}

fn gc_range(record_ref: RecordRef) -> SegmentGcRecordRange {
    SegmentGcRecordRange::from(record_ref)
}

fn gc_ranges_contain(ranges: &[SegmentGcRecordRange], record_ref: RecordRef) -> bool {
    let range = gc_range(record_ref);
    ranges.iter().any(|candidate| {
        candidate.offset <= range.offset
            && candidate.offset.saturating_add(candidate.len)
                >= range.offset.saturating_add(range.len)
    })
}

fn gc_staged_record(source_offset: u64, staged_offset: u64) -> GcStagedCopiedRecord {
    GcStagedCopiedRecord {
        source: GcCopyRecord {
            key: BlobKey::new(format!("blob-{source_offset}").into_bytes()).unwrap(),
            shard: STANDALONE_SHARD,
            payload_lsn: source_offset,
            from: RecordRef {
                segment_id: 7,
                offset: source_offset,
                len: 8,
            },
            lifecycle: None,
            destination_class: DestinationClass::Spillover,
        },
        staged: RecordRef {
            segment_id: 1,
            offset: staged_offset,
            len: 8,
        },
    }
}

#[test]
fn gc_publish_reconciliation_keeps_lifecycle_only_source_touches() {
    let lifecycle_touched = gc_staged_record(10, 0);
    let retired = gc_staged_record(30, 8);
    let accounting_changes = vec![
        AccountingRefEvent {
            key: SegmentRefEventKey {
                segment_id: 7,
                lsn: 50,
                offset: lifecycle_touched.source.from.offset,
            },
            event: SegmentRefEvent::LifecycleChanged { lifecycle: None },
        },
        AccountingRefEvent {
            key: SegmentRefEventKey {
                segment_id: 7,
                lsn: 51,
                offset: retired.source.from.offset,
            },
            event: SegmentRefEvent::Retired,
        },
    ];

    let (survivors, skipped) = split_gc_copied_records(
        vec![lifecycle_touched.clone(), retired.clone()],
        &accounting_changes,
        &BTreeSet::new(),
    );

    assert_eq!(survivors, vec![lifecycle_touched]);
    assert_eq!(
        skipped,
        vec![GcSkippedCopiedRecord {
            record: retired,
            kind: GcSkippedCopiedRecordKind::Retired,
        }]
    );
}

#[test]
fn gc_publish_reconciliation_rejects_obsolete_shard_generation() {
    let record = gc_staged_record(10, 0);
    let obsolete_shards = BTreeSet::from([record.source.shard]);

    let (survivors, skipped) = split_gc_copied_records(vec![record.clone()], &[], &obsolete_shards);

    assert!(survivors.is_empty());
    assert_eq!(
        skipped,
        vec![GcSkippedCopiedRecord {
            record,
            kind: GcSkippedCopiedRecordKind::Retired,
        }]
    );
}

fn segment_summary(index: &StrataIndex, segment_id: SegmentId) -> strata_core::SegmentGcSummary {
    index
        .get_segment_gc_overlay(segment_id)
        .unwrap()
        .unwrap_or_default()
        .summary
}

fn active_delta_log_state(index: &StrataIndex) -> ActiveDeltaLogState {
    index
        .get_accounting_active_delta_log_state()
        .unwrap()
        .unwrap()
}

fn active_delta_log_read_cursor(index: &StrataIndex) -> ActiveDeltaLogReadCursor {
    index
        .get_accounting_active_delta_log_consumed_cursor()
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn durable_frontier_advances_across_durable_gc_map_ref() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(dir.path(), "strata");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let from = RecordRef {
        segment_id: 1,
        offset: 0,
        len: TEST_RECORD_LEN,
    };
    let to = RecordRef {
        segment_id: 2,
        offset: 0,
        len: TEST_RECORD_LEN,
    };
    let entry = PutEntry {
        record_ref: Some(from),
        lsn: 1,
        generation: 1,
        state: BlobState::Live,
    };
    let to_end_offset = to.end_offset().unwrap();
    let mut output_state = SegmentState {
        owner: INGEST_SEGMENT_OWNER,
        segment_id: to.segment_id,
        volume_id: 0,
        path: format!("gc/{:012}.data", to.segment_id),
        placement_class: PlacementClass::Spillover,
        state: SegmentFileState::Sealed,
        write_offset: to_end_offset,
        durable_offset: to_end_offset - 1,
        min_lsn: None,
        max_lsn: None,
        sealed_before_lsn: None,
        sealed_len: Some(to_end_offset),
        sealed_sha256: None,
    };

    let mut batch = index.batch();
    index.put_durable_lsn_batch(&mut batch, 1).unwrap();
    index
        .merge_blob_version_batch(&mut batch, &key, STANDALONE_SHARD, &entry)
        .unwrap();
    index
        .map_blob_ref_batch(
            &mut batch,
            &key,
            MapRefOp {
                publish_lsn: 2,
                shard: STANDALONE_SHARD,
                payload_lsn: entry.lsn,
                from,
                to,
            },
        )
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 2, &key)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &output_state)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        durable_lsn_with_accounting_frontier(
            &index,
            None,
            Some(ActiveDeltaLogState {
                segment_id: 1,
                durable_offset: 0,
                durable_lsn: 2,
            }),
        )
        .unwrap(),
        1
    );

    output_state.durable_offset = to_end_offset;
    let mut batch = index.batch();
    index
        .put_segment_state_batch(&mut batch, &output_state)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        durable_lsn_with_accounting_frontier(
            &index,
            None,
            Some(ActiveDeltaLogState {
                segment_id: 1,
                durable_offset: 0,
                durable_lsn: 2,
            }),
        )
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn recovery_rollback_removes_gc_relocations_at_hidden_publish_lsns() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let from_kept = RecordRef {
        segment_id: 1,
        offset: 0,
        len: TEST_RECORD_LEN,
    };
    let from_hidden = RecordRef {
        segment_id: 1,
        offset: TEST_RECORD_LEN,
        len: TEST_RECORD_LEN,
    };
    let to_kept = RecordRef {
        segment_id: 3,
        offset: 0,
        len: TEST_RECORD_LEN,
    };
    let to_hidden = RecordRef {
        segment_id: 2,
        offset: TEST_RECORD_LEN,
        len: TEST_RECORD_LEN,
    };
    let entry = PutEntry {
        record_ref: Some(from_hidden),
        lsn: 4,
        generation: 1,
        state: BlobState::Live,
    };
    let output_path = segment_path(&cfg, to_hidden.segment_id);
    let output_len = to_hidden.end_offset().unwrap();
    std::fs::write(&output_path, vec![0; output_len as usize]).unwrap();
    let output_state = SegmentState {
        owner: INGEST_SEGMENT_OWNER,
        segment_id: to_hidden.segment_id,
        volume_id: 0,
        path: relative_segment_path(&cfg, output_path.clone()),
        placement_class: PlacementClass::Spillover,
        state: SegmentFileState::Sealed,
        write_offset: output_len,
        durable_offset: output_len,
        min_lsn: Some(6),
        max_lsn: Some(6),
        sealed_before_lsn: None,
        sealed_len: Some(output_len),
        sealed_sha256: None,
    };

    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 7).unwrap();
    index.put_durable_lsn_batch(&mut batch, 4).unwrap();
    index.put_current_epoch_batch(&mut batch, 42).unwrap();
    index.put_epoch_change_batch(&mut batch, 0, 42).unwrap();
    index
        .merge_blob_version_batch(&mut batch, &key, STANDALONE_SHARD, &entry)
        .unwrap();
    index
        .map_blob_ref_batch(
            &mut batch,
            &key,
            MapRefOp {
                publish_lsn: 6,
                shard: STANDALONE_SHARD,
                payload_lsn: entry.lsn,
                from: from_hidden,
                to: to_hidden,
            },
        )
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 6, &key)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &output_state)
        .unwrap();
    index
        .put_gc_relocation_batch(
            &mut batch,
            from_kept,
            &GcRelocation {
                publish_lsn: 5,
                to: to_kept,
            },
        )
        .unwrap();
    index
        .put_gc_relocation_batch(
            &mut batch,
            from_hidden,
            &GcRelocation {
                publish_lsn: 6,
                to: to_hidden,
            },
        )
        .unwrap();
    batch.write().unwrap();

    rollback_operations_from(&cfg, &index, &StrataStoreMetrics::default(), 6).unwrap();

    assert_eq!(index.get_next_lsn().unwrap(), 6);
    assert!(!output_path.exists());
    assert_eq!(
        index
            .get_segment_state(to_hidden.segment_id)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::Deleted
    );
    assert_eq!(
        index
            .get_blob_version_for_shard(&version_key(&key, entry.lsn), STANDALONE_SHARD)
            .unwrap()
            .unwrap()
            .record_ref,
        Some(from_hidden)
    );
    assert_eq!(
        index.get_gc_relocation(from_kept).unwrap(),
        Some(GcRelocation {
            publish_lsn: 5,
            to: to_kept,
        })
    );
    assert_eq!(index.get_gc_relocation(from_hidden).unwrap(), None);
}

#[tokio::test]
async fn open_cleans_stale_pending_gc_output() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    ensure_epoch_initialized(&index, cfg.starting_epoch).unwrap();
    let segment_id = 42;
    let path = segment_path(&cfg, segment_id);
    std::fs::write(&path, b"pending").unwrap();
    let state = SegmentState {
        owner: INGEST_SEGMENT_OWNER,
        segment_id,
        volume_id: 0,
        path: relative_segment_path(&cfg, path.clone()),
        placement_class: PlacementClass::Spillover,
        state: SegmentFileState::PendingGcOutput,
        write_offset: 7,
        durable_offset: 7,
        min_lsn: None,
        max_lsn: None,
        sealed_before_lsn: None,
        sealed_len: Some(7),
        sealed_sha256: None,
    };
    index.put_segment_state(&state).unwrap();
    index.flush_wal(true).unwrap();
    drop(index);

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert!(!path.exists());
    assert_eq!(
        store
            .index()
            .get_segment_state(segment_id)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::Deleted
    );
}

#[tokio::test]
async fn open_cleans_stale_gc_staging_dirs() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let staging_attempt = cfg.namespace_dir().join("gc-staging").join("123-456-0");
    std::fs::create_dir_all(&staging_attempt).unwrap();
    std::fs::write(staging_attempt.join("000000000001.data"), b"staged").unwrap();

    let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();

    assert!(!cfg.namespace_dir().join("gc-staging").exists());
    assert!(store.config().ingest_dir().exists());
}

fn open_accounting_sidecar(store: &StrataStore) -> AccountingIndex {
    let manifest = store.index().get_accounting_index_manifest().unwrap();
    AccountingIndex::open_with_manifest(
        AccountingIndexConfig::new(
            store.config().accounting_index_dir(),
            store.config().accounting_sidecar_partition_count(),
        ),
        manifest,
    )
    .unwrap()
}

#[derive(Debug)]
struct StandaloneStore {
    store: StrataStore,
}

impl Deref for StandaloneStore {
    type Target = StrataStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl StandaloneStore {
    fn put(&self, key: &BlobKey, payload: &[u8]) -> Result<StrataLsn> {
        self.store.put(STANDALONE_SHARD.id, key, payload)
    }

    fn tombstone(&self, key: &BlobKey) -> Result<StrataLsn> {
        self.store.tombstone(key)
    }

    fn extend(&self, key: &BlobKey, new_logical_end_epoch: Epoch) -> Result<Option<StrataLsn>> {
        self.store
            .set_blob_lifetime(key, new_logical_end_epoch)
            .map(Some)
    }

    fn increment_epoch(&self) -> Result<(Epoch, StrataLsn)> {
        self.store.increment_epoch()
    }
}

fn try_open_standalone_store(
    config: StrataStoreConfig,
    metrics: StrataStoreMetrics,
) -> Result<StandaloneStore> {
    let store = StrataStore::open(config, metrics)?;
    Ok(StandaloneStore { store })
}

fn stop_accounting_worker(store: &mut StrataStore) {
    if let Some(accounting_tx) = store.accounting_tx.take() {
        let _ = accounting_tx.send(AccountingCommand::Shutdown);
    }
    if let Some(accounting_handle) = store.accounting_handle.take() {
        let _ = accounting_handle.join();
    }
}

fn init_typed_store_metrics() {
    INIT_TYPED_STORE_METRICS.call_once(|| {
        DBMetrics::get();
    });
}

fn config(root_dir: &Path, namespace: &str) -> StrataStoreConfig {
    StrataStoreConfig {
        root_dir: root_dir.to_path_buf(),
        namespace: namespace.to_owned(),
        segment_max_bytes: 1 << 20,
        write_queue_capacity: 128,
        max_unsealed_segments: 8,
        seal_worker_count: DEFAULT_SEAL_WORKER_COUNT,
        segment_reader_cache_capacity: 16,
        recovery_policy: StrataRecoveryPolicy::PointInTime,
        sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
        accounting_worker_enabled: true,
        accounting_interval: DEFAULT_ACCOUNTING_INTERVAL,
        accounting_unaccounted_threshold: DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
        accounting_sidecar_partition_count: DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT,
        accounting_sidecar_interval: DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
        accounting_sidecar_ingest_record_threshold:
            DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD,
        accounting_sidecar_delta_run_count_threshold:
            DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
        accounting_sidecar_delta_run_bytes_threshold:
            DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
        accounting_sidecar_major_patch_count_threshold:
            DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
        accounting_sidecar_major_patch_bytes_threshold:
            DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
        gc_workers_enabled: true,
        gc_interval: Duration::from_secs(3600),
        gc_worker_count: DEFAULT_GC_WORKER_COUNT,
        gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
        gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
        gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
        gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
        gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
        gc_planner_config: GcPlannerConfig::default(),
        gc_max_accounting_lag_lsn: DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
        shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
        starting_epoch: 42,
    }
}

fn counter_value(registry: &Registry, name: &str) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_counter().value())
        })
        .unwrap_or_else(|| panic!("missing counter metric {name}"))
}

fn gauge_value(registry: &Registry, name: &str) -> i64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_gauge().value() as i64)
        })
        .unwrap_or_else(|| panic!("missing gauge metric {name}"))
}

fn histogram_sample_count(registry: &Registry, name: &str) -> u64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_histogram().sample_count())
        })
        .unwrap_or_else(|| panic!("missing histogram metric {name}"))
}

fn wait_for_segment_state(
    index: &StrataIndex,
    segment_id: SegmentId,
    expected_state: SegmentFileState,
) -> SegmentState {
    let started = Instant::now();
    loop {
        let state = index.get_segment_state(segment_id).unwrap().unwrap();
        if state.state == expected_state {
            return state;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timed out waiting for segment {segment_id} to become {expected_state:?}; current state was {:?}",
            state.state
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_accounted_lsn(store: &StrataStore, expected_lsn: StrataLsn) {
    let started = Instant::now();
    loop {
        let accounted_lsn = store.accounted_lsn().unwrap();
        if accounted_lsn >= expected_lsn {
            return;
        }
        {
            let _guard = store
                .accounting_lock
                .lock()
                .expect("accounting run lock poisoned");
            accounting::run_accounting_sidecar_materializing_once(store.index(), store.config())
                .unwrap();
        }
        let accounted_lsn = store.accounted_lsn().unwrap();
        if accounted_lsn >= expected_lsn {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "timed out waiting for accounted_lsn to reach {expected_lsn}; current accounted_lsn was {accounted_lsn}; durable_lsn was {}; active_delta_state was {:?}; active_delta_cursor was {:?}",
            store.durable_lsn().unwrap(),
            store
                .index()
                .get_accounting_active_delta_log_state()
                .unwrap(),
            store
                .index()
                .get_accounting_active_delta_log_consumed_cursor()
                .unwrap(),
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_shard_cleanup(store: &StrataStore, shard: ShardKey) {
    let started = Instant::now();
    loop {
        if store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .is_none()
        {
            return;
        }
        {
            let _guard = store
                .accounting_lock
                .lock()
                .expect("accounting run lock poisoned");
            accounting::run_accounting_sidecar_materializing_once(store.index(), store.config())
                .unwrap();
        }
        store
            .gc_executor()
            .unwrap()
            .cleanup_ready_shard_generations()
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "timed out waiting for cleanup of shard {shard:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn put_test_segment_state(index: &StrataIndex, segment_id: SegmentId, state: SegmentFileState) {
    index
        .put_segment_state(&SegmentState {
            owner: INGEST_SEGMENT_OWNER,
            segment_id,
            volume_id: 0,
            path: format!("ingest/{segment_id:012}.data"),
            placement_class: PlacementClass::Ingest,
            state,
            write_offset: 64,
            durable_offset: 0,
            min_lsn: Some(segment_id),
            max_lsn: Some(segment_id),
            sealed_before_lsn: None,
            sealed_len: None,
            sealed_sha256: None,
        })
        .unwrap();
}

fn version_key(key: &BlobKey, lsn: StrataLsn) -> BlobVersionKey {
    BlobVersionKey {
        key: key.clone(),
        lsn,
    }
}

fn seal_first_segment(config: &StrataStoreConfig) -> SegmentState {
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(config.clone(), StrataStoreMetrics::default()).unwrap();

    store.put(&key_1, b"payload-a").unwrap();
    store.put(&key_2, b"payload-b").unwrap();

    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed)
}

#[tokio::test]
async fn standalone_put_get_round_trip() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"hello strata").unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
    assert!(dir.path().join("default").join("ingest").exists());
    assert!(dir.path().join("default").join("index").exists());
}

#[tokio::test]
async fn background_accounting_and_gc_workers_can_be_disabled() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "no-background-maintenance");
    cfg.accounting_worker_enabled = false;
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert!(store.store.accounting_tx.is_none());
    assert!(store.store.accounting_handle.is_none());
    assert!(store.store.gc_txs.is_empty());
    assert!(store.store.gc_handles.is_empty());
    assert_eq!(store.gc_active_worker_limit(), 0);
    assert_eq!(store.gc_active_io_bytes_per_sec(), 0);

    let lsn = store.put(&key, b"hello strata").unwrap();
    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), lsn);
    assert_eq!(store.accounted_lsn().unwrap(), 0);
    assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
}

#[tokio::test]
async fn gc_workers_require_background_accounting() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "invalid-background-maintenance");
    cfg.accounting_worker_enabled = false;

    let error = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidConfig("gc workers require the accounting worker")
    ));
}

#[tokio::test]
async fn from_index_writes_logical_shard_versions_with_global_store_state() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "shard-a");
    let index = open_test_index(dir.path().join("shared-index"), cfg.index_cf_prefix());
    let shard = ShardKey {
        id: 5,
        generation: 2,
    };
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    index
        .put_shard_info(shard.id, ShardInfo::active(shard.generation))
        .unwrap();
    let store = StrataStore::from_index(cfg, index.clone(), StrataStoreMetrics::default()).unwrap();

    let lsn = store.put(shard.id, &key, b"hello shard").unwrap();

    assert_eq!(lsn, 1);
    assert_eq!(
        store.get_from_shard(shard.id, &key).unwrap(),
        Some(b"hello shard".to_vec())
    );
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(
        index.get_shard_info(shard.id).unwrap(),
        Some(ShardInfo::active(shard.generation))
    );
    assert_eq!(index.get_next_lsn().unwrap(), 2);
    assert!(index.resolve_blob_head(&key, shard).unwrap().is_some());
    assert!(index.get_segment_state(FIRST_SEGMENT_ID).unwrap().is_some());
    assert_eq!(index.get_blob_entry(&key).unwrap(), None);
}

#[tokio::test]
async fn store_writes_logical_shard_into_record_header() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "record-shard");
    let index = open_test_index(dir.path().join("shared-index"), cfg.index_cf_prefix());
    let shard = ShardKey {
        id: 5,
        generation: 2,
    };
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    index
        .put_shard_info(shard.id, ShardInfo::active(shard.generation))
        .unwrap();
    let store =
        StrataStore::from_index(cfg.clone(), index.clone(), StrataStoreMetrics::default()).unwrap();

    let lsn = store.put(shard.id, &key, b"hello shard").unwrap();
    let record_ref = index
        .get_blob_version_for_shard(&version_key(&key, lsn), shard)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let mut reader = strata_segment::SegmentReader::open(
        segment_path(&cfg, record_ref.segment_id),
        record_ref.segment_id,
    )
    .unwrap();

    let metadata = reader.read_record_metadata(record_ref).unwrap();

    assert_eq!(metadata.header.shard, shard);
}

#[tokio::test]
async fn store_hosts_multiple_logical_shards_inside_one_index() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "single-store");
    let index = open_test_index(dir.path().join("shared-index"), cfg.index_cf_prefix());
    index.put_shard_info(10, ShardInfo::active(4)).unwrap();
    let store = StrataStore::from_index(cfg, index.clone(), StrataStoreMetrics::default()).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let shard_a = store.add_shard(10).unwrap();
    let shard_b = store.add_shard(20).unwrap();
    let lsn_a = store.put(10, &key, b"primary").unwrap();
    let lsn_b = store.put(20, &key, b"secondary").unwrap();

    assert_eq!(
        shard_a,
        ShardKey {
            id: 10,
            generation: 4
        }
    );
    assert_eq!(
        shard_b,
        ShardKey {
            id: 20,
            generation: 0
        }
    );
    assert_eq!((lsn_a, lsn_b), (1, 2));
    assert_eq!(store.shard_info(10).unwrap(), Some(ShardInfo::active(4)));
    assert_eq!(store.shard_info(20).unwrap(), Some(ShardInfo::active(0)));
    assert_eq!(
        store.get_from_shard(10, &key).unwrap(),
        Some(b"primary".to_vec())
    );
    assert_eq!(
        store.get_from_shard(20, &key).unwrap(),
        Some(b"secondary".to_vec())
    );
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(index.get_next_lsn().unwrap(), 3);
    assert!(index.resolve_blob_head(&key, shard_a).unwrap().is_some());
    assert!(index.resolve_blob_head(&key, shard_b).unwrap().is_some());
    assert_eq!(index.get_blob_entry(&key).unwrap(), None);
    assert!(!dir.path().join("single-store").join("index").exists());
}

#[tokio::test]
async fn concurrent_logical_shard_puts_use_one_global_sequence() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store = Arc::new(
        StrataStore::open(
            config(dir.path(), "single-store"),
            StrataStoreMetrics::default(),
        )
        .unwrap(),
    );
    store.add_shard(10).unwrap();
    store.add_shard(20).unwrap();

    let mut handles = Vec::new();
    for (shard_id, prefix) in [(10, "primary"), (20, "secondary")] {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let mut writes = Vec::new();
            for i in 0..32 {
                let key = BlobKey::new(format!("{prefix}-{i}").into_bytes()).unwrap();
                let payload = format!("payload-{prefix}-{i}").into_bytes();
                let lsn = store.put(shard_id, &key, &payload).unwrap();
                writes.push((shard_id, key, payload, lsn));
            }
            writes
        }));
    }

    let writes = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let lsns = writes
        .iter()
        .map(|(_, _, _, lsn)| *lsn)
        .collect::<BTreeSet<_>>();

    assert_eq!(writes.len(), 64);
    assert_eq!(lsns.len(), 64);
    assert_eq!(lsns.first().copied(), Some(1));
    assert_eq!(lsns.last().copied(), Some(64));
    assert_eq!(store.index().get_next_lsn().unwrap(), 65);
    for (shard_id, key, payload, _) in writes {
        assert_eq!(store.get_from_shard(shard_id, &key).unwrap(), Some(payload));
    }
}

#[tokio::test]
async fn store_blob_ops_apply_to_all_active_logical_shard_heads() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    let shard_a = store.add_shard(10).unwrap();
    let shard_b = store.add_shard(20).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let lsn_a = store.put(10, &key, b"primary").unwrap();
    let lsn_b = store.put(20, &key, b"secondary").unwrap();
    let ref_a = store
        .index()
        .get_blob_version_for_shard(&version_key(&key, lsn_a), shard_a)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version_for_shard(&version_key(&key, lsn_b), shard_b)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let extend_lsn = store.set_blob_lifetime(&key, 50).unwrap();
    let tombstone_lsn = store.tombstone(&key).unwrap();

    assert_eq!((lsn_a, lsn_b, extend_lsn, tombstone_lsn), (1, 2, 3, 4));
    assert_eq!(
        store.index().get_unaccounted_lsn_op(extend_lsn).unwrap(),
        Some(key.clone())
    );
    assert_eq!(
        store
            .index()
            .blob_version_op_at_lsn(&key, extend_lsn)
            .unwrap(),
        None
    );
    assert!(
        store
            .index()
            .blob_lifecycle_op_at_lsn(&key, extend_lsn)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .index()
            .blob_version_op_at_lsn(&key, tombstone_lsn)
            .unwrap(),
        None
    );
    assert!(
        store
            .index()
            .blob_lifecycle_op_at_lsn(&key, tombstone_lsn)
            .unwrap()
            .is_some()
    );
    assert_eq!(store.get_from_shard(10, &key).unwrap(), None);
    assert_eq!(store.get_from_shard(20, &key).unwrap(), None);
    assert_eq!(
        store
            .index()
            .resolve_blob_head(&key, shard_a)
            .unwrap()
            .unwrap()
            .head_lsn,
        lsn_a
    );
    assert_eq!(
        store
            .index()
            .resolve_blob_head(&key, shard_b)
            .unwrap()
            .unwrap()
            .head_lsn,
        lsn_b
    );

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let mut expected_tombstoned = std::collections::BTreeMap::new();
    *expected_tombstoned.entry(ref_a.segment_id).or_insert(0) += ref_a.len;
    *expected_tombstoned.entry(ref_b.segment_id).or_insert(0) += ref_b.len;
    for (segment_id, retired_bytes) in expected_tombstoned {
        let stats = segment_summary(store.index(), segment_id);
        assert_eq!(stats.total_bytes, retired_bytes);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.live_ref_count, 0);
        assert_eq!(stats.retired_bytes, retired_bytes);
        assert!(stats.future_epoch_histogram.is_empty());
    }
    assert_eq!(
        store.index().iter_unaccounted_lsn_ops().unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn tombstone_barrier_keeps_later_shard_put_visible_only_for_that_shard() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    store.add_shard(10).unwrap();
    store.add_shard(20).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    store.put(10, &key, b"primary-old").unwrap();
    store.put(20, &key, b"secondary-old").unwrap();
    let tombstone_lsn = store.tombstone(&key).unwrap();
    let resurrect_lsn = store.put(10, &key, b"primary-new").unwrap();

    assert!(resurrect_lsn > tombstone_lsn);
    assert_eq!(
        store.get_from_shard(10, &key).unwrap(),
        Some(b"primary-new".to_vec())
    );
    assert_eq!(store.get_from_shard(20, &key).unwrap(), None);
    assert_eq!(
        store
            .index()
            .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
            .unwrap()
            .tombstone_lsn,
        Some(tombstone_lsn)
    );
}

#[tokio::test]
async fn store_batch_buffers_ops_until_write_and_returns_global_lsns() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    store.add_shard(10).unwrap();
    store.add_shard(20).unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

    let mut batch = store.batch();
    batch
        .put(10, key_a.clone(), Arc::<[u8]>::from(&b"payload-a"[..]))
        .put(20, key_b.clone(), Arc::<[u8]>::from(&b"payload-b"[..]))
        .set_blob_lifetime(key_a.clone(), 50)
        .tombstone(key_b.clone());
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);

    let result = batch.write().unwrap();

    assert_eq!(result.op_lsns(), &[1, 2, 3, 4]);
    assert_eq!(store.index().get_next_lsn().unwrap(), 5);
    assert_eq!(
        store.get_from_shard(10, &key_a).unwrap(),
        Some(b"payload-a".to_vec())
    );
    assert_eq!(store.get_from_shard(20, &key_b).unwrap(), None);
    assert_eq!(
        store
            .index()
            .resolve_blob_lifecycle_at(&key_a, StrataLsn::MAX)
            .unwrap()
            .lifetime
            .unwrap()
            .lifecycle
            .logical_end_epoch,
        50
    );
}

#[tokio::test]
async fn store_batch_can_mix_epoch_changes_with_blob_ops() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

    let mut batch = store.batch();
    batch
        .put(
            STANDALONE_SHARD.id,
            key_a.clone(),
            Arc::<[u8]>::from(&b"payload-a"[..]),
        )
        .increment_epoch()
        .put(
            STANDALONE_SHARD.id,
            key_b.clone(),
            Arc::<[u8]>::from(&b"payload-b"[..]),
        );

    let result = batch.write().unwrap();

    assert_eq!(result.op_lsns(), &[1, 2, 3]);
    assert_eq!(result.op_epochs(), &[None, Some(43), None]);
    assert_eq!(result.epoch_for_op(1), Some(43));
    assert_eq!(result.last_epoch(), Some(43));
    assert_eq!(store.current_epoch().unwrap(), 43);
    assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(42));
    assert_eq!(store.epoch_at_lsn(2).unwrap(), Some(43));
    assert_eq!(store.index().get_epoch_change(2).unwrap(), Some(43));
    assert_eq!(store.index().get_next_lsn().unwrap(), 4);
    assert_eq!(store.get(&key_a).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));

    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), 3);
}

#[tokio::test]
async fn sync_publishes_active_accounting_delta_log_state() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    store.tombstone(&key).unwrap();
    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), epoch_lsn);
    let state = store
        .index()
        .get_accounting_active_delta_log_state()
        .unwrap()
        .unwrap();
    assert_eq!(state.durable_lsn, epoch_lsn);
    assert!(state.durable_offset > 0);
}

#[tokio::test]
async fn accounting_sidecar_ingests_active_delta_log_and_compacts_to_patch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"sidecar-ingest".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.accounting_interval = Duration::from_secs(3600);
    cfg.accounting_sidecar_ingest_record_threshold = usize::MAX;
    cfg.accounting_sidecar_major_patch_count_threshold = 0;
    cfg.accounting_sidecar_major_patch_bytes_threshold = 0;
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    stop_accounting_worker(&mut store.store);

    store.put(&key, b"payload").unwrap();
    store.sync().unwrap();
    accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();

    let active_state = active_delta_log_state(store.index());
    let consumed_cursor = active_delta_log_read_cursor(store.index());
    assert_eq!(consumed_cursor.offset, active_state.durable_offset);
    assert_eq!(consumed_cursor.max_lsn, active_state.durable_lsn);

    let sidecar = open_accounting_sidecar(&store);
    let partition = sidecar.manifest().partitions.values().next().unwrap();
    let delta_count = sidecar
        .manifest()
        .partitions
        .values()
        .map(|partition| partition.deltas.len())
        .sum::<usize>();
    let patch_count = sidecar
        .manifest()
        .partitions
        .values()
        .map(|partition| partition.patches.len())
        .sum::<usize>();
    assert_eq!(delta_count, 0);
    assert_eq!(patch_count, 1);
    assert!(partition.base.is_none());
}

#[tokio::test]
async fn accounting_sidecar_major_compacts_when_patch_threshold_reached() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"sidecar-major".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.accounting_interval = Duration::from_secs(3600);
    cfg.accounting_sidecar_ingest_record_threshold = usize::MAX;
    cfg.accounting_sidecar_major_patch_count_threshold = 1;
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    stop_accounting_worker(&mut store.store);

    store.put(&key, b"payload").unwrap();
    store.sync().unwrap();
    accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();

    let sidecar = open_accounting_sidecar(&store);
    let base_count = sidecar
        .manifest()
        .partitions
        .values()
        .filter(|partition| partition.base.is_some())
        .count();
    let patch_count = sidecar
        .manifest()
        .partitions
        .values()
        .map(|partition| partition.patches.len())
        .sum::<usize>();
    assert_eq!(base_count, 1);
    assert_eq!(patch_count, 0);
}

#[tokio::test]
async fn store_rejects_missing_and_inactive_logical_shards() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "single-store");
    let index = open_test_index(dir.path().join("shared-index"), cfg.index_cf_prefix());
    index
        .put_shard_info(
            30,
            ShardInfo {
                current_generation: 2,
                state: strata_core::ShardState::Dropped,
            },
        )
        .unwrap();
    let store = StrataStore::from_index(cfg, index, StrataStoreMetrics::default()).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let error = store.put(31, &key, b"missing").unwrap_err();
    match error {
        Error::ShardNotFound { shard_id } => assert_eq!(shard_id, 31),
        other => panic!("unexpected error: {other:?}"),
    }

    let error = store.put(30, &key, b"dropped").unwrap_err();

    match error {
        Error::ShardUnavailable {
            shard_id,
            generation,
            current_generation,
            state,
        } => {
            assert_eq!(shard_id, 30);
            assert_eq!(generation, 2);
            assert_eq!(current_generation, 2);
            assert_eq!(state, strata_core::ShardState::Dropped);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn store_add_after_drop_bumps_generation_and_hides_old_versions() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let first_shard = store.add_shard(40).unwrap();
    assert_eq!(
        first_shard,
        ShardKey {
            id: 40,
            generation: 0
        }
    );
    store.put(40, &key, b"old generation").unwrap();
    assert_eq!(
        store.get_from_shard(40, &key).unwrap(),
        Some(b"old generation".to_vec())
    );

    store.drop_shard(40).unwrap();
    assert_eq!(
        store.shard_info(40).unwrap(),
        Some(ShardInfo {
            current_generation: 0,
            state: strata_core::ShardState::Dropped,
        })
    );
    assert!(store.put(40, &key, b"dropped").is_err());
    assert!(store.get_from_shard(40, &key).is_err());
    assert_eq!(
        store.index().resolve_blob_head(&key, first_shard).unwrap(),
        None
    );

    let second_shard = store.add_shard(40).unwrap();
    assert_eq!(
        second_shard,
        ShardKey {
            id: 40,
            generation: 1
        }
    );

    assert_eq!(store.get_from_shard(40, &key).unwrap(), None);
    store.put(40, &key, b"new generation").unwrap();
    assert_eq!(
        store.get_from_shard(40, &key).unwrap(),
        Some(b"new generation".to_vec())
    );
    assert!(
        store
            .index()
            .resolve_blob_head(&key, second_shard)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn drop_shard_retires_mixed_ingest_bytes_without_tombstones() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.accounting_sidecar_major_patch_count_threshold = 1;
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    stop_accounting_worker(&mut store.store);
    let shard = store.add_shard(41).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let put_lsn = store.store.put(41, &key, b"old generation").unwrap();
    let record_ref = store
        .index()
        .resolve_blob_head(&key, shard)
        .unwrap()
        .unwrap()
        .entry
        .record_ref
        .unwrap();
    let drop_lsn = put_lsn + 1;
    assert_eq!(store.index().get_next_lsn().unwrap(), drop_lsn);

    store.store.drop_shard(41).unwrap();

    assert_eq!(
        store.store.shard_info(41).unwrap(),
        Some(ShardInfo {
            current_generation: 0,
            state: ShardState::Dropped,
        })
    );
    assert!(store.store.get_from_shard(41, &key).is_err());
    assert_eq!(store.index().resolve_blob_head(&key, shard).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), drop_lsn + 1);
    assert_eq!(store.index().get_durable_lsn().unwrap(), drop_lsn);
    assert_eq!(
        store.index().get_unaccounted_lsn_op(put_lsn).unwrap(),
        Some(key.clone())
    );

    let job = store.index().get_shard_cleanup_job(shard).unwrap().unwrap();
    assert_eq!(job.drop_lsn, drop_lsn);
    assert_eq!(job.state, ShardCleanupState::PendingAccounting);

    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap_or_default();
    assert!(overlay.retired.is_empty());

    accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::ReadyForGc
    );
    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
    assert_eq!(overlay.summary.total_bytes, record_ref.len);
    assert_eq!(overlay.summary.live_ref_count, 0);
    assert_eq!(overlay.summary.retired_bytes, record_ref.len);
    assert_eq!(
        store
            .store
            .gc_executor()
            .unwrap()
            .cleanup_ready_shard_generations()
            .unwrap(),
        1
    );
    assert!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn drop_shard_does_not_wait_for_accounting_or_gc_claims() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store = Arc::new(
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap(),
    );
    let shard = store.add_shard(43).unwrap();
    store
        .put(
            shard.id,
            &BlobKey::new(b"nonblocking-drop".to_vec()).unwrap(),
            b"payload",
        )
        .unwrap();

    let accounting_lock = Arc::clone(&store.accounting_lock);
    let _accounting_guard = accounting_lock
        .lock()
        .expect("accounting run lock poisoned");
    let _claim = store
        .gc_claims
        .try_claim(BTreeSet::from([FIRST_SEGMENT_ID]))
        .unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let drop_store = Arc::clone(&store);
    let handle = std::thread::spawn(move || {
        result_tx.send(drop_store.drop_shard(shard.id)).unwrap();
    });

    result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("drop_shard blocked on accounting or GC claim")
        .unwrap();
    handle.join().unwrap();
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::PendingAccounting
    );
}

#[tokio::test]
async fn reopen_finishes_durable_shard_drop_cleanup() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let shard;
    let ingest_ref;
    let retention_segment_id = 99;
    let retention_path;

    {
        let mut store =
            try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        stop_accounting_worker(&mut store.store);
        shard = store.add_shard(42).unwrap();
        let key = BlobKey::new(b"crashed-drop".to_vec()).unwrap();
        store.store.put(shard.id, &key, b"ingest payload").unwrap();
        store.sync().unwrap();
        ingest_ref = store
            .index()
            .resolve_blob_head(&key, shard)
            .unwrap()
            .unwrap()
            .entry
            .record_ref
            .unwrap();

        retention_path = layout::retention_segment_path(
            store.config(),
            shard,
            PlacementClass::Spillover,
            retention_segment_id,
        );
        std::fs::create_dir_all(retention_path.parent().unwrap()).unwrap();
        std::fs::write(&retention_path, b"orphaned by crash").unwrap();
        store
            .index()
            .put_segment_state(&SegmentState {
                owner: SegmentOwner::Shard(shard),
                segment_id: retention_segment_id,
                volume_id: 0,
                path: relative_segment_path(store.config(), retention_path.clone()),
                placement_class: PlacementClass::Spillover,
                state: SegmentFileState::Sealed,
                write_offset: 17,
                durable_offset: 17,
                min_lsn: Some(1),
                max_lsn: Some(1),
                sealed_before_lsn: None,
                sealed_len: Some(17),
                sealed_sha256: None,
            })
            .unwrap();
        store.store.drop_shard(shard.id).unwrap();
        assert_eq!(
            store
                .index()
                .get_shard_cleanup_job(shard)
                .unwrap()
                .unwrap()
                .state,
            ShardCleanupState::PendingAccounting
        );
    }

    let reopened = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while reopened
        .index()
        .get_shard_cleanup_job(shard)
        .unwrap()
        .is_some()
        && Instant::now() < deadline
    {
        reopened.store.request_gc().unwrap();
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        !retention_path.exists(),
        "job={:?} accounted_lsn={} durable_lsn={} shard_drops={:?}",
        reopened.index().get_shard_cleanup_job(shard).unwrap(),
        reopened.index().get_accounted_lsn().unwrap(),
        reopened.index().get_durable_lsn().unwrap(),
        reopened
            .index()
            .get_accounting_index_manifest()
            .unwrap()
            .map(|manifest| manifest.shard_drops)
    );
    assert!(
        reopened
            .index()
            .get_segment_state(retention_segment_id)
            .unwrap()
            .is_none()
    );
    let overlay = reopened
        .index()
        .get_segment_gc_overlay(ingest_ref.segment_id)
        .unwrap()
        .unwrap();
    assert!(overlay.retired.contains(&gc_range(ingest_ref)));
}

#[tokio::test]
async fn store_drop_missing_shard_fails() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();

    let error = store.drop_shard(50).unwrap_err();

    match error {
        Error::ShardNotFound { shard_id } => assert_eq!(shard_id, 50),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn store_can_drop_default_logical_shard() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let store = StrataStore::open(cfg.clone(), StrataStoreMetrics::default()).unwrap();
    let key = BlobKey::new(b"default-shard".to_vec()).unwrap();

    store.put(STANDALONE_SHARD.id, &key, b"payload").unwrap();
    let ingest_segment_id = store
        .index()
        .resolve_blob_head(&key, STANDALONE_SHARD)
        .unwrap()
        .unwrap()
        .entry
        .record_ref
        .unwrap()
        .segment_id;
    store.drop_shard(STANDALONE_SHARD.id).unwrap();

    assert_eq!(
        store.shard_info(STANDALONE_SHARD.id).unwrap(),
        Some(ShardInfo {
            current_generation: STANDALONE_SHARD.generation,
            state: ShardState::Dropped,
        })
    );
    assert!(store.get_from_shard(STANDALONE_SHARD.id, &key).is_err());
    assert_eq!(
        store
            .index()
            .get_segment_state(ingest_segment_id)
            .unwrap()
            .unwrap()
            .owner,
        SegmentOwner::Store
    );
    drop(store);

    let reopened = StrataStore::open(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(
        reopened.shard_info(STANDALONE_SHARD.id).unwrap(),
        Some(ShardInfo {
            current_generation: STANDALONE_SHARD.generation,
            state: ShardState::Dropped,
        })
    );
    assert_eq!(
        reopened.add_shard(STANDALONE_SHARD.id).unwrap(),
        ShardKey {
            id: STANDALONE_SHARD.id,
            generation: STANDALONE_SHARD.generation + 1,
        }
    );
}

#[tokio::test]
async fn halted_store_rejects_writer_commands() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let key = BlobKey::new(b"halted".to_vec()).unwrap();

    store.store_halt.halt("terminal append rollback failure");

    assert!(matches!(
        store.put(&key, b"value"),
        Err(Error::StoreHalted { reason }) if reason == "terminal append rollback failure"
    ));
    assert!(matches!(
        store.sync(),
        Err(Error::StoreHalted { reason }) if reason == "terminal append rollback failure"
    ));
    assert!(matches!(
        store.request_gc(),
        Err(Error::StoreHalted { reason }) if reason == "terminal append rollback failure"
    ));
}

#[tokio::test]
async fn new_store_records_starting_epoch_as_lsn_zero_genesis() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.current_epoch().unwrap(), 42);
    assert_eq!(store.epoch_at_lsn(0).unwrap(), Some(42));
    assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(42));
    assert_eq!(store.index().get_epoch_change(0).unwrap(), Some(42));
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);
    assert_eq!(store.index().get_durable_lsn().unwrap(), 0);
    assert_eq!(
        store.index().get_shard_info(0).unwrap(),
        Some(ShardInfo::active(0))
    );
}

#[tokio::test]
async fn reopen_uses_persisted_epoch_instead_of_config_starting_epoch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.increment_epoch().unwrap(), (43, 1));
        store.sync().unwrap();
    }

    cfg.starting_epoch = 99;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.current_epoch().unwrap(), 43);
    assert_eq!(store.epoch_at_lsn(0).unwrap(), Some(42));
    assert_eq!(store.epoch_at_lsn(1).unwrap(), Some(43));
}

#[tokio::test]
async fn increment_epoch_consumes_lsn_and_is_metadata_durable() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert_eq!(store.increment_epoch().unwrap(), (43, 1));
    assert_eq!(store.current_epoch().unwrap(), 43);
    assert_eq!(store.index().get_next_lsn().unwrap(), 2);
    assert_eq!(store.index().get_epoch_change(1).unwrap(), Some(43));
    assert_eq!(store.put(&key, b"payload").unwrap(), 2);

    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), 2);
}

#[tokio::test]
async fn get_with_options_can_skip_checksum_verification() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"hello strata").unwrap();
    store.sync().unwrap();

    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let mut segment = OpenOptions::new()
        .write(true)
        .open(segment_path(store.config(), record_ref.segment_id))
        .unwrap();
    segment
        .seek(SeekFrom::Start(
            record_ref.offset + FIXED_RECORD_HEADER_LEN as u64,
        ))
        .unwrap();
    segment.write_all(b"H").unwrap();

    let err = store.get(&key).unwrap_err();
    assert!(matches!(
        err,
        Error::Segment(strata_segment::Error::Core(
            strata_core::Error::RecordChecksumMismatch { .. }
        ))
    ));

    assert_eq!(
        store
            .get_with_options(&key, ReadOptions::skip_checksum_verification())
            .unwrap(),
        Some(b"Hello strata".to_vec())
    );
}

#[tokio::test]
async fn get_blob_range_reads_payload_slice() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"hello strata").unwrap();

    assert_eq!(
        store.get_blob_range(&key, 6..12).unwrap(),
        Some(b"strata".to_vec())
    );
}

#[tokio::test]
async fn cached_reader_is_evictable_when_segment_is_deleted() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"hello strata").unwrap();
    store.sync().unwrap();

    assert_eq!(store.reader_cache_len(), 0);
    assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
    assert_eq!(store.reader_cache_len(), 1);

    let mut state = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    state.state = SegmentFileState::Deleted;
    let mut batch = store.index().batch();
    store
        .index()
        .put_segment_state_batch(&mut batch, &state)
        .unwrap();
    batch.write().unwrap();
    store.index().flush_wal(true).unwrap();
    store.evict_segment_reader(FIRST_SEGMENT_ID);

    assert_eq!(store.get(&key).unwrap(), None);
    assert!(!store.contains(&key).unwrap());
    assert_eq!(store.reader_cache_len(), 0);
}

#[tokio::test]
async fn read_retries_once_when_not_found_segment_was_deleted() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let payload_lsn = store.put(&key, b"hello strata").unwrap();
    let old_ref = store
        .index()
        .get_blob_version(&version_key(&key, payload_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let new_ref = RecordRef {
        segment_id: FIRST_SEGMENT_ID + 100,
        offset: 0,
        len: old_ref.len,
    };
    let attempts = std::cell::Cell::new(0);

    let payload = store
        .read_live_record_ref(STANDALONE_SHARD, &key, |record_ref, _path| {
            let attempt = attempts.get();
            attempts.set(attempt + 1);
            if attempt == 0 {
                assert_eq!(record_ref, old_ref);

                let mut old_state = store
                    .index()
                    .get_segment_state(old_ref.segment_id)
                    .unwrap()
                    .unwrap();
                old_state.state = SegmentFileState::Deleted;

                let new_state = SegmentState {
                    owner: INGEST_SEGMENT_OWNER,
                    segment_id: new_ref.segment_id,
                    volume_id: 0,
                    path: format!("gc/{:012}.data", new_ref.segment_id),
                    placement_class: PlacementClass::Spillover,
                    state: SegmentFileState::Sealed,
                    write_offset: new_ref.len,
                    durable_offset: new_ref.len,
                    min_lsn: Some(payload_lsn + 1),
                    max_lsn: Some(payload_lsn + 1),
                    sealed_before_lsn: None,
                    sealed_len: Some(new_ref.len),
                    sealed_sha256: None,
                };

                let mut batch = store.index().batch();
                store
                    .index()
                    .put_segment_state_batch(&mut batch, &old_state)
                    .unwrap();
                store
                    .index()
                    .put_segment_state_batch(&mut batch, &new_state)
                    .unwrap();
                store
                    .index()
                    .map_blob_ref_batch(
                        &mut batch,
                        &key,
                        MapRefOp {
                            publish_lsn: payload_lsn + 1,
                            shard: STANDALONE_SHARD,
                            payload_lsn,
                            from: old_ref,
                            to: new_ref,
                        },
                    )
                    .unwrap();
                batch.write().unwrap();

                return Err(Error::Segment(strata_segment::Error::Io {
                    path: segment_path(store.config(), old_ref.segment_id),
                    source: std::io::ErrorKind::NotFound.into(),
                }));
            }

            assert_eq!(record_ref, new_ref);
            Ok(b"hello strata".to_vec())
        })
        .unwrap();

    assert_eq!(payload, Some(b"hello strata".to_vec()));
    assert_eq!(attempts.get(), 2);
}

#[tokio::test]
async fn stream_blob_reads_payload_slice() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"hello strata").unwrap();

    let mut stream = store.stream_blob(&key, 0..5).unwrap().unwrap();
    let mut read = Vec::new();
    stream.read_to_end(&mut read).unwrap();

    assert_eq!(read, b"hello");
    assert_eq!(stream.remaining(), 0);
}

#[tokio::test]
async fn read_range_missing_and_tombstone_return_none() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let missing = BlobKey::new(b"missing".to_vec()).unwrap();
    let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert_eq!(store.get_blob_range(&missing, 0..1).unwrap(), None);
    assert!(store.stream_blob(&missing, 0..1).unwrap().is_none());

    store.put(&tombstoned, b"payload").unwrap();
    store.tombstone(&tombstoned).unwrap();

    assert_eq!(store.get_blob_range(&tombstoned, 0..1).unwrap(), None);
    assert!(store.stream_blob(&tombstoned, 0..1).unwrap().is_none());
}

#[tokio::test]
async fn read_range_rejects_out_of_bounds_range() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();

    let err = store.get_blob_range(&key, 0..8).unwrap_err();

    assert!(matches!(
        err,
        Error::Segment(strata_segment::Error::InvalidPayloadRange { .. })
    ));
}

#[tokio::test]
async fn range_read_rejects_index_record_key_mismatch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();

    let mut entry_b = store.index().get_blob_entry(&key_b).unwrap().unwrap();
    entry_b.lsn += 1;
    store.index().put_blob_entry(&key_a, &entry_b).unwrap();

    let err = store.get_blob_range(&key_a, 0..1).unwrap_err();

    assert!(matches!(
        err,
        Error::KeyMismatch {
            requested,
            found
        } if requested == key_a && found == key_b
    ));
}

#[tokio::test]
async fn store_from_index_does_not_create_local_index_dir() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let db_dir = tempdir().unwrap();
    let db = open_cf(
        db_dir.path(),
        None,
        MetricConf::new("strata_store_test"),
        &["existing"],
    )
    .unwrap();
    let index = StrataIndex::from_db(db, "strata/shard-99").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store = StrataStore::from_index(
        config(dir.path(), "shard-99"),
        index,
        StrataStoreMetrics::default(),
    )
    .unwrap();

    store.put(STANDALONE_SHARD.id, &key, b"payload").unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    assert!(dir.path().join("shard-99").join("ingest").exists());
    assert!(!dir.path().join("shard-99").join("index").exists());
}

#[tokio::test]
async fn get_missing_returns_none() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"missing".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert_eq!(store.get(&key).unwrap(), None);
    assert!(!store.contains(&key).unwrap());
}

#[tokio::test]
async fn point_in_time_recovery_removes_orphan_segment_file_before_opening_active_writer() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
    fs::create_dir_all(cfg.ingest_dir()).unwrap();
    fs::write(&orphan_path, b"stale bytes").unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(fs::metadata(&orphan_path).unwrap().len(), 0);

    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    store.put(&key, b"payload").unwrap();

    let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
    assert_eq!(entry.record_ref.unwrap().offset, 0);
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
}

#[tokio::test]
async fn absolute_consistency_rejects_orphan_segment_file() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
    let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
    fs::create_dir_all(cfg.ingest_dir()).unwrap();
    fs::write(&orphan_path, b"stale bytes").unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(
        err,
        Error::OrphanSegmentFile {
            segment_id: FIRST_SEGMENT_ID,
            ..
        }
    ));
}

#[tokio::test]
async fn tombstone_hides_payload() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let put_lsn = store.put(&key, b"payload").unwrap();

    let tombstone_lsn = store.tombstone(&key).unwrap();

    assert_eq!(put_lsn, 1);
    assert_eq!(tombstone_lsn, 2);
    assert_eq!(store.get(&key).unwrap(), None);
    assert!(!store.contains(&key).unwrap());
    let put_entry = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref,
        put_entry.record_ref
    );
    assert_eq!(
        store
            .index()
            .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
            .unwrap()
            .tombstone_lsn,
        Some(tombstone_lsn)
    );
    assert_eq!(put_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
    assert_eq!(store.durable_lsn().unwrap(), 0);

    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), 2);
}

#[tokio::test]
async fn metrics_track_core_store_operations() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let missing = BlobKey::new(b"missing".to_vec()).unwrap();
    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let store = try_open_standalone_store(config(dir.path(), "default"), metrics).unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    assert_eq!(store.get(&missing).unwrap(), None);
    assert_eq!(
        store.get_blob_range(&key, 1..4).unwrap(),
        Some(b"ayl".to_vec())
    );
    store.sync().unwrap();
    let tombstone_lsn = store.tombstone(&key).unwrap();

    assert_eq!(put_lsn, 1);
    assert_eq!(tombstone_lsn, 2);
    assert_eq!(
        gauge_value(&registry, "strata_store_queued_write_commands"),
        0
    );
    assert_eq!(
        histogram_sample_count(&registry, "strata_store_write_queue_send_duration_seconds"),
        3
    );
    assert_eq!(
        counter_value(&registry, "strata_store_write_queue_send_errors_total"),
        0.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_put_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_put_errors_total"),
        0.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_put_payload_bytes_total"),
        7.0
    );
    assert!(
        counter_value(&registry, "strata_store_put_record_bytes_total")
            > counter_value(&registry, "strata_store_put_payload_bytes_total")
    );
    assert_eq!(
        counter_value(&registry, "strata_store_sync_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_sync_errors_total"),
        0.0
    );
    assert!(counter_value(&registry, "strata_store_sync_bytes_total") > 0.0);
    assert_eq!(
        counter_value(&registry, "strata_store_get_calls_total"),
        2.0
    );
    assert_eq!(counter_value(&registry, "strata_store_get_hits_total"), 1.0);
    assert_eq!(
        counter_value(&registry, "strata_store_get_misses_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_get_payload_bytes_total"),
        7.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_range_read_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_range_read_hits_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_range_read_payload_bytes_total"),
        3.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_stream_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_stream_hits_total"),
        1.0
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_active_segment_id"),
        FIRST_SEGMENT_ID as i64
    );
    assert!(gauge_value(&registry, "strata_store_active_segment_write_offset") > 0);
    assert!(gauge_value(&registry, "strata_store_active_segment_durable_offset") > 0);
    assert_eq!(gauge_value(&registry, "strata_store_next_lsn"), 3);
    assert_eq!(gauge_value(&registry, "strata_store_durable_lsn"), 1);
    assert_eq!(gauge_value(&registry, "strata_store_pending_lsn_count"), 1);
    assert_eq!(
        counter_value(&registry, "strata_store_seal_backpressure_waits_total"),
        0.0
    );
    assert_eq!(
        histogram_sample_count(
            &registry,
            "strata_store_seal_backpressure_wait_duration_seconds"
        ),
        0
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_seal_backpressure_current"),
        0
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_configured_workers"),
        DEFAULT_GC_WORKER_COUNT as i64
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_active_worker_limit"),
        DEFAULT_GC_INITIAL_WORKER_COUNT as i64
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_configured_io_bytes_per_sec"),
        DEFAULT_GC_IO_BYTES_PER_SEC as i64
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_min_io_bytes_per_sec"),
        DEFAULT_GC_MIN_IO_BYTES_PER_SEC as i64
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_active_io_bytes_per_sec"),
        DEFAULT_GC_IO_BYTES_PER_SEC as i64
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_gc_in_flight_workers"),
        0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_gc_admitted_total"),
        0.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_gc_skipped_by_tuner_total"),
        0.0
    );
}

#[tokio::test]
async fn metrics_track_seal_backpressure_waits() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.max_unsealed_segments = 2;
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    put_test_segment_state(&index, 1, SegmentFileState::Sealing);
    put_test_segment_state(&index, 2, SegmentFileState::Open);

    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let gc_concurrency = Arc::new(GcConcurrencyController::new(
        GcConcurrencyConfig::from_store_config(&cfg),
        metrics.clone(),
    ));
    let active_writer = SegmentWriter::create(
        segment_path(&cfg, 2),
        2,
        PlacementClass::Ingest,
        cfg.segment_max_bytes,
    )
    .unwrap();
    let (seal_tx, _seal_rx) = mpsc::channel();
    let (_write_tx, write_rx) = mpsc::sync_channel(1);
    let (accounting_tx, _accounting_rx) = mpsc::sync_channel(1);
    let active_segment_state = active_segment_state(&cfg, INGEST_SEGMENT_OWNER, &active_writer, 0);
    let coordinator = WriteCoordinator {
        config: cfg.clone(),
        index: index.clone(),
        active_writer,
        active_accounting_delta_log: test_active_accounting_log(&cfg, 2),
        durability_publish_lock: Arc::new(Mutex::new(())),
        active_segment_state,
        durable_offset: 0,
        last_checkpoint_at: Instant::now(),
        last_checkpoint_next_lsn: index.get_next_lsn().unwrap(),
        pending_rollovers: Vec::new(),
        segment_ids: SegmentIdAllocator::new(3),
        seal_tx,
        accounting_tx: Some(accounting_tx),
        write_rx,
        ingest_owner: INGEST_SEGMENT_OWNER,
        reader_cache: Arc::new(SegmentReaderCache::new(cfg.segment_reader_cache_capacity)),
        gc_concurrency,
        store_halt: StoreHalt::default(),
        metrics,
    };

    let unblocker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        put_test_segment_state(&index, 1, SegmentFileState::Sealed);
    });

    coordinator.wait_for_seal_backlog_capacity().unwrap();
    unblocker.join().unwrap();

    assert_eq!(
        counter_value(&registry, "strata_store_seal_backpressure_waits_total"),
        1.0
    );
    assert_eq!(
        histogram_sample_count(
            &registry,
            "strata_store_seal_backpressure_wait_duration_seconds"
        ),
        1
    );
    assert_eq!(
        gauge_value(&registry, "strata_store_seal_backpressure_current"),
        0
    );
}

#[tokio::test]
async fn set_blob_lifetime_preserves_payload_until_accounting() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let before = store.index().get_blob_entry(&key).unwrap().unwrap();

    let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

    let after = store.index().get_blob_entry(&key).unwrap().unwrap();
    assert_eq!(put_lsn, 1);
    assert_eq!(extend_lsn, 2);
    assert_eq!(after, before);
    let lifecycle = store
        .index()
        .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
        .unwrap()
        .lifetime
        .unwrap()
        .lifecycle;
    assert_eq!(lifecycle.logical_end_epoch, 50);
    assert_eq!(lifecycle.extension_count, 0);
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.record_ref, before.record_ref.unwrap());
    assert_eq!(resolved.generation, before.generation);
    assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);

    let stats = segment_summary(store.index(), resolved.record_ref.segment_id);
    assert_eq!(stats, strata_core::SegmentGcSummary::default());

    store.sync().unwrap();

    assert_eq!(store.durable_lsn().unwrap(), 2);
    wait_for_accounted_lsn(&store, 2);
    let stats = segment_summary(store.index(), resolved.record_ref.segment_id);
    assert_eq!(stats.future_epoch_histogram.get(&43), None);
    assert_eq!(
        stats.future_epoch_histogram.get(&50),
        Some(&EpochBucket {
            refs: 1,
            bytes: resolved.record_ref.len,
        })
    );
    assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
    assert_eq!(stats.extension_count_histogram.get(&1), None);
    assert_eq!(stats.unknown_lifetime_bytes, 0);
    assert_eq!(stats.unknown_lifetime_ref_count, 0);
    assert_eq!(stats.min_live_end_epoch, Some(50));
    assert_eq!(stats.max_live_end_epoch, Some(50));
    assert_eq!(stats.live_bytes, resolved.record_ref.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.total_bytes, resolved.record_ref.len);
}

#[tokio::test]
async fn accounting_updates_exact_epoch_segment_pinning_after_lifetime_update() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
    let record_ref = entry.record_ref.unwrap();
    let mut state = store
        .index()
        .get_segment_state(record_ref.segment_id)
        .unwrap()
        .unwrap();
    state.placement_class = PlacementClass::ExactEpoch(42);
    store.index().put_segment_state(&state).unwrap();

    let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats, strata_core::SegmentGcSummary::default());

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, extend_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.future_epoch_histogram.get(&42), None);
    assert_eq!(
        stats.future_epoch_histogram.get(&50),
        Some(&EpochBucket {
            refs: 1,
            bytes: record_ref.len,
        })
    );
    assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
    assert_eq!(stats.extension_count_histogram.get(&1), None);
}

#[tokio::test]
async fn accounting_tracks_unknown_lifetime_until_metadata_arrives() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.live_bytes, record_ref.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.unknown_lifetime_bytes, record_ref.len);
    assert_eq!(stats.unknown_lifetime_ref_count, 1);
    assert!(stats.future_epoch_histogram.is_empty());

    let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.live_bytes, record_ref.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.unknown_lifetime_bytes, 0);
    assert_eq!(stats.unknown_lifetime_ref_count, 0);
    assert_eq!(
        stats.future_epoch_histogram.get(&50),
        Some(&EpochBucket {
            refs: 1,
            bytes: record_ref.len,
        })
    );
}

#[tokio::test]
async fn accounting_applies_lifetime_written_before_payload() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
    let put_lsn = store.put(&key, b"payload").unwrap();
    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    assert_eq!((lifetime_lsn, put_lsn), (1, 2));
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.live_bytes, record_ref.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.unknown_lifetime_bytes, 0);
    assert_eq!(stats.unknown_lifetime_ref_count, 0);
    assert_eq!(
        stats.future_epoch_histogram.get(&50),
        Some(&EpochBucket {
            refs: 1,
            bytes: record_ref.len,
        })
    );
}

#[tokio::test]
async fn set_blob_lifetime_missing_or_tombstoned_blob_records_metadata() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let missing = BlobKey::new(b"missing".to_vec()).unwrap();
    let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert_eq!(store.extend(&missing, 50).unwrap(), Some(1));
    assert_eq!(store.get(&missing).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), 2);

    store.put(&tombstoned, b"payload").unwrap();
    store.tombstone(&tombstoned).unwrap();

    assert_eq!(store.extend(&tombstoned, 50).unwrap(), Some(4));
    assert_eq!(store.get(&tombstoned).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), 5);
}

#[tokio::test]
async fn read_after_lifetime_update_chain_resolves_latest_lifetime() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let extension_count = 4;
    let last_epoch = 42 + extension_count;
    for epoch in 43..=last_epoch {
        store.extend(&key, epoch).unwrap().unwrap();
    }

    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    let latest = store.index().get_blob_entry(&key).unwrap().unwrap();
    assert_eq!(latest.record_ref, Some(resolved.record_ref));
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

    let lifecycle = resolved.lifecycle.unwrap();
    assert_eq!(lifecycle.logical_end_epoch, last_epoch);
    assert_eq!(lifecycle.extension_count, extension_count as u32 - 1);
}

#[tokio::test]
async fn store_set_blob_lifetime_preserves_payload_and_updates_lifecycle() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

    assert_eq!(put_lsn, 1);
    assert_eq!(extend_lsn, 2);
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
}

#[tokio::test]
async fn set_blob_lifetime_rejects_current_or_past_epoch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert!(matches!(
        store.set_blob_lifetime(&key, 42),
        Err(Error::InvalidBlobLifetime {
            logical_end_epoch: 42,
            current_epoch: 42
        })
    ));
    assert!(matches!(
        store.set_blob_lifetime(&key, 41),
        Err(Error::InvalidBlobLifetime {
            logical_end_epoch: 41,
            current_epoch: 42
        })
    ));
}

#[tokio::test]
async fn expired_lifetime_hides_blob_reads_before_gc() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    store.set_blob_lifetime(&key, 43).unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

    store.increment_epoch().unwrap();

    assert_eq!(store.get(&key).unwrap(), None);
    assert!(!store.contains(&key).unwrap());
}

#[tokio::test]
async fn later_lifetime_before_expiry_keeps_current_blob_visible() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    store.set_blob_lifetime(&key, 43).unwrap();
    store.set_blob_lifetime(&key, 50).unwrap();
    store.increment_epoch().unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
}

#[tokio::test]
async fn new_put_after_policy_expiry_does_not_inherit_stale_lifetime() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.set_blob_lifetime(&key, 43).unwrap();
    store.increment_epoch().unwrap();
    store.put(&key, b"new").unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"new".to_vec()));
    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.lifecycle, None);
}

#[tokio::test]
async fn lifetime_update_after_expiry_applies_only_to_future_put() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"old").unwrap();
    store.set_blob_lifetime(&key, 43).unwrap();
    store.increment_epoch().unwrap();
    store.set_blob_lifetime(&key, 50).unwrap();
    assert_eq!(store.get(&key).unwrap(), None);

    store.put(&key, b"new").unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"new".to_vec()));
    let resolved = resolve_blob_version(store.index(), store.shard(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.lifecycle.unwrap().logical_end_epoch, 50);
}

#[tokio::test]
async fn reopen_reads_existing_data() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store =
            try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
                .unwrap();
        store.put(&key, b"payload").unwrap();
        store.sync().unwrap();
    }

    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
}

#[tokio::test]
async fn sync_advances_durable_offset_after_segment_fsync() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();

    let unsynced = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(unsynced.durable_offset, 0);
    assert!(unsynced.write_offset > 0);
    assert_eq!(store.durable_lsn().unwrap(), 0);
    assert_eq!(
        store.index().iter_unaccounted_lsn_ops().unwrap(),
        vec![(1, key.clone())]
    );

    store.sync().unwrap();

    let synced = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(synced.durable_offset, unsynced.write_offset);
    assert_eq!(synced.write_offset, unsynced.write_offset);
    assert_eq!(store.durable_lsn().unwrap(), 1);
    wait_for_accounted_lsn(&store, 1);
    assert_eq!(store.accounted_lsn().unwrap(), 1);
    assert_eq!(
        store.index().iter_unaccounted_lsn_ops().unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn put_after_sync_preserves_durable_offset() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key_1, b"payload-a").unwrap();
    store.sync().unwrap();
    let synced = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();

    store.put(&key_2, b"payload-b").unwrap();

    let after_put = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(after_put.durable_offset, synced.durable_offset);
    assert!(after_put.write_offset > synced.write_offset);
}

#[tokio::test]
async fn recovery_keeps_complete_unsynced_record_that_survived() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload").unwrap();
        store.index().flush_wal(true).unwrap();
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    let state = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert!(state.write_offset > 0);
    assert_eq!(state.durable_offset, state.write_offset);
    assert_eq!(store.durable_lsn().unwrap(), 1);
}

#[tokio::test]
async fn recovery_removes_live_index_entry_when_segment_bytes_are_missing() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload").unwrap();
        store.index().flush_wal(true).unwrap();
    }

    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .set_len(0)
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
}

#[tokio::test]
async fn recovery_ignores_segment_record_when_index_version_is_missing() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let segment_path = segment_path(&cfg, FIRST_SEGMENT_ID);
        let mut segment = SegmentWriter::open_existing(
            &segment_path,
            FIRST_SEGMENT_ID,
            PlacementClass::Ingest,
            1 << 20,
        )
        .unwrap();
        segment.append(&key, 1, b"payload").unwrap();
        let write_offset = segment.write_offset();
        drop(segment);

        let state = active_segment_state_from_path(
            &cfg,
            INGEST_SEGMENT_OWNER,
            FIRST_SEGMENT_ID,
            write_offset,
            0,
        );
        let mut batch = store.index().batch();
        store
            .index()
            .put_segment_state_batch(&mut batch, &state)
            .unwrap();
        store
            .index()
            .put_store_state_batch(&mut batch, &StrataStoreState::default())
            .unwrap();
        batch.write().unwrap();
        store.index().flush_wal(true).unwrap();
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);
}

#[tokio::test]
async fn recovery_truncates_partial_tail() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let valid_len;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload").unwrap();
        store.index().flush_wal(true).unwrap();
        valid_len = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap()
            .write_offset;
    }

    let path = segment_path(&cfg, FIRST_SEGMENT_ID);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"partial").unwrap();
    drop(file);
    assert!(std::fs::metadata(&path).unwrap().len() > valid_len);

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
}

#[tokio::test]
async fn recovery_removes_tombstone_when_rolled_back_lsn_range_is_lost() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload").unwrap();
        store.tombstone(&key).unwrap();
        store.index().flush_wal(true).unwrap();
    }

    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .set_len(0)
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.durable_lsn().unwrap(), 0);
}

#[tokio::test]
async fn recovery_removes_epoch_changes_after_rolled_back_blob_lsn() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload").unwrap(), 1);
        assert_eq!(store.increment_epoch().unwrap(), (43, 2));
        store.index().flush_wal(true).unwrap();
    }

    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .set_len(0)
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.current_epoch().unwrap(), 42);
    assert_eq!(store.epoch_at_lsn(2).unwrap(), Some(42));
    assert_eq!(store.index().get_epoch_change(2).unwrap(), None);
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);
}

#[tokio::test]
async fn recovery_rolls_back_lost_overwrite_to_previous_entry() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let first_len;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload-a").unwrap();
        store.sync().unwrap();
        first_len = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap()
            .write_offset;
        store.put(&key, b"payload-b").unwrap();
        store.index().flush_wal(true).unwrap();
    }

    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .set_len(first_len)
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key).unwrap(), Some(b"payload-a".to_vec()));
    let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
    assert_eq!(entry.lsn, 1);
    assert_eq!(store.durable_lsn().unwrap(), 1);
}

#[tokio::test]
async fn put_overwrite_keeps_blob_versions_for_old_records() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let min_lsn = store.put(&key, b"payload-a").unwrap();
    let second_lsn = store.put(&key, b"payload-b").unwrap();
    let entry = store.index().get_blob_entry(&key).unwrap().unwrap();

    assert_ne!(min_lsn, second_lsn);
    assert_eq!(entry.lsn, second_lsn);
    let first_entry = store
        .index()
        .get_blob_version(&version_key(&key, min_lsn))
        .unwrap()
        .unwrap();
    let second_entry = store
        .index()
        .get_blob_version(&version_key(&key, second_lsn))
        .unwrap()
        .unwrap();
    assert_eq!(first_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
    assert_eq!(
        second_entry.record_ref.unwrap().segment_id,
        FIRST_SEGMENT_ID
    );
}

#[tokio::test]
async fn accounting_tombstones_overwritten_payload_summary() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let first_lsn = store.put(&key, b"payload-a").unwrap();
    let second_lsn = store.put(&key, b"payload-b").unwrap();
    let lifetime_lsn = store.extend(&key, 44).unwrap().unwrap();
    let first_record_ref = store
        .index()
        .get_blob_version(&version_key(&key, first_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let second_record_ref = store
        .index()
        .get_blob_version(&version_key(&key, second_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let stats = segment_summary(store.index(), first_record_ref.segment_id);
    assert_eq!(stats, strata_core::SegmentGcSummary::default());

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let stats = segment_summary(store.index(), first_record_ref.segment_id);
    assert_eq!(
        stats.total_bytes,
        first_record_ref.len + second_record_ref.len
    );
    assert_eq!(stats.live_bytes, second_record_ref.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.retired_bytes, first_record_ref.len);
    assert_eq!(stats.future_epoch_histogram.get(&43), None);
    assert_eq!(
        stats.future_epoch_histogram.get(&44),
        Some(&EpochBucket {
            refs: 1,
            bytes: second_record_ref.len,
        })
    );
    assert_eq!(stats.extension_count_histogram.get(&0), Some(&1));
    assert_eq!(
        store.index().iter_unaccounted_lsn_ops().unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn accounting_tombstones_deleted_payload_summary() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let tombstone_lsn = store.tombstone(&key).unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.total_bytes, record_ref.len);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
    assert_eq!(stats.retired_bytes, record_ref.len);
    assert!(stats.future_epoch_histogram.is_empty());
    assert!(stats.extension_count_histogram.is_empty());
    assert_eq!(store.get(&key).unwrap(), None);
}

#[tokio::test]
async fn accounting_leaves_unknown_lifetime_put_copy_eligible() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);

    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap_or_default();
    assert!(overlay.expired.is_empty());
    assert!(overlay.retired.is_empty());
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn accounting_publishes_retired_segment_ref_event_for_tombstone() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let tombstone_lsn = store.tombstone(&key).unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    assert_eq!(
        store
            .index()
            .iter_segment_ref_events_since(record_ref.segment_id, put_lsn)
            .unwrap(),
        vec![(
            strata_core::SegmentRefEventKey {
                segment_id: record_ref.segment_id,
                lsn: tombstone_lsn,
                offset: record_ref.offset,
            },
            SegmentRefEvent::Retired,
        )]
    );
    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
    assert!(overlay.expired.is_empty());
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn accounting_snapshot_guard_reports_later_ref_events() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);

    let snapshot = store.create_accounting_snapshot().unwrap();
    assert_eq!(snapshot.accounted_lsn(), put_lsn);

    let tombstone_lsn = store.tombstone(&key).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    assert_eq!(
        store.accounting_changes_since(&snapshot).unwrap(),
        vec![AccountingRefEvent {
            key: strata_core::SegmentRefEventKey {
                segment_id: record_ref.segment_id,
                lsn: tombstone_lsn,
                offset: record_ref.offset,
            },
            event: SegmentRefEvent::Retired,
        }]
    );
}

#[tokio::test]
async fn gc_publish_does_not_force_accounting_to_catch_up() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);
    stop_accounting_worker(&mut store.store);

    let accounting_snapshot = store.create_accounting_snapshot().unwrap();
    assert_eq!(accounting_snapshot.accounted_lsn(), put_lsn);

    let tombstone_lsn = store.tombstone(&key).unwrap();
    let copy = PreparedGcCopy {
        accounting_snapshot,
        plan: GcPlan {
            scenario: GcScenario::EmptyDelete,
            action: GcAction::MoveLiveBytes {
                source_segment_id: FIRST_SEGMENT_ID,
                routes: Vec::new(),
            },
            copied_bytes: 0,
            expected_reclaim_bytes: 0,
            score: 0,
        },
        outputs: Vec::new(),
        copied_records: Vec::new(),
        claim: None,
    };

    let published = store.publish_prepared_gc_copy(copy).unwrap();

    assert_eq!(published.reconciled_accounted_lsn, put_lsn);
    assert!(store.durable_lsn().unwrap() < tombstone_lsn);
    assert!(store.accounted_lsn().unwrap() < tombstone_lsn);
}

#[tokio::test]
async fn gc_publish_waiting_for_accounting_lock_does_not_block_writer() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key_a, b"payload-a").unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, put_lsn);
    let accounting_snapshot = store.create_accounting_snapshot().unwrap();
    let copy = PreparedGcCopy {
        accounting_snapshot,
        plan: GcPlan {
            scenario: GcScenario::EmptyDelete,
            action: GcAction::MoveLiveBytes {
                source_segment_id: FIRST_SEGMENT_ID,
                routes: Vec::new(),
            },
            copied_bytes: 0,
            expected_reclaim_bytes: 0,
            score: 0,
        },
        outputs: Vec::new(),
        copied_records: Vec::new(),
        claim: None,
    };

    let accounting_lock = store.store.accounting_lock.clone();
    let accounting_guard = accounting_lock
        .lock()
        .expect("accounting run lock poisoned");
    std::thread::scope(|scope| {
        let publish = scope.spawn(|| store.publish_prepared_gc_copy(copy));
        std::thread::sleep(Duration::from_millis(50));

        let (put_tx, put_rx) = mpsc::channel();
        let store_ref = &store;
        let key_b_ref = &key_b;
        let put = scope.spawn(move || {
            let result = store_ref.put(key_b_ref, b"payload-b");
            put_tx.send(result).unwrap();
        });
        let put_result = put_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer blocked behind GC accounting-lock wait");
        assert!(put_result.unwrap() > put_lsn);

        drop(accounting_guard);
        assert!(publish.join().unwrap().is_ok());
        put.join().unwrap();
    });
}

#[tokio::test]
async fn gc_prepare_plan_skips_claimed_source_and_uses_next_candidate() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let larger_segment_id = 10;
    let smaller_segment_id = 11;
    for (segment_id, bytes) in [(larger_segment_id, 100), (smaller_segment_id, 10)] {
        let state = SegmentState {
            owner: INGEST_SEGMENT_OWNER,
            segment_id,
            volume_id: 0,
            path: format!("ingest/{segment_id:012}.data"),
            placement_class: PlacementClass::Spillover,
            state: SegmentFileState::Sealed,
            write_offset: bytes,
            durable_offset: bytes,
            min_lsn: Some(0),
            max_lsn: Some(0),
            sealed_before_lsn: None,
            sealed_len: Some(bytes),
            sealed_sha256: None,
        };
        let mut batch = store.index().batch();
        store
            .index()
            .put_segment_state_batch(&mut batch, &state)
            .unwrap();
        store
            .index()
            .merge_segment_gc_overlay_batch(
                &mut batch,
                segment_id,
                vec![SegmentGcOverlayMergeOp::AddRetiredBatch {
                    ranges: vec![SegmentGcRecordRange {
                        offset: 0,
                        len: bytes,
                    }],
                }],
            )
            .unwrap();
        batch.write().unwrap();
    }
    store.index().flush_wal(true).unwrap();

    let _claim = store
        .store
        .gc_claims
        .try_claim(BTreeSet::from([larger_segment_id]))
        .unwrap();
    let prepared = store
        .prepare_gc_plan(&GcPlanner::new(GcPlannerConfig::default()))
        .unwrap()
        .unwrap();

    assert_eq!(
        prepared.plan.action,
        GcAction::DeleteSegments {
            segment_ids: vec![smaller_segment_id]
        }
    );
}

#[tokio::test]
async fn gc_prepare_plan_defers_when_accounting_lag_exceeds_configured_limit() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_max_accounting_lag_lsn = Some(2);
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let mut batch = store.index().batch();
    store.index().put_durable_lsn_batch(&mut batch, 5).unwrap();
    store
        .index()
        .put_accounted_lsn_batch(&mut batch, 2)
        .unwrap();
    batch.write().unwrap();

    let lag = store.gc_deferred_by_accounting_lag().unwrap().unwrap();
    assert_eq!(lag.durable_lsn, 5);
    assert_eq!(lag.accounted_lsn, 2);
    assert_eq!(lag.lag_lsn, 3);
    assert_eq!(lag.max_lag_lsn, Some(2));

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
}

#[tokio::test]
async fn gc_prepare_plan_scans_real_segment_and_selects_live_records() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c);

    let ref_a = store
        .index()
        .get_blob_version(&version_key(&key_a, lsn_a))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version(&version_key(&key_b, lsn_b))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

    assert!(prepared.accounting_snapshot.accounted_lsn() >= tombstone_lsn);
    assert_eq!(prepared.plan.scenario, GcScenario::L0Compaction);
    assert_eq!(prepared.plan.copied_bytes, ref_b.len);

    assert_eq!(
        store
            .index()
            .get_segment_gc_overlay(ref_a.segment_id)
            .unwrap()
            .unwrap()
            .retired,
        vec![gc_range(ref_a)]
    );

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.plan.scenario, GcScenario::L0Compaction);
    assert_eq!(copied.outputs.len(), 1);
    assert_eq!(copied.copied_records.len(), 1);
    let output = &copied.outputs[0];
    assert_eq!(output.destination_class, DestinationClass::Spillover);
    assert_eq!(output.placement_class, PlacementClass::Spillover);
    assert_eq!(output.sealed_len, ref_b.len);
    assert!(output.path.exists());

    let copied_record = &copied.copied_records[0];
    assert_eq!(copied_record.source.key, key_b);
    assert_eq!(copied_record.source.payload_lsn, lsn_b);
    assert_eq!(copied_record.source.from, ref_b);
    assert_eq!(
        copied_record.source.destination_class,
        DestinationClass::Spillover
    );
    assert_eq!(copied_record.staged.segment_id, output.staged_segment_id);
    assert_eq!(copied_record.staged.offset, 0);
    assert_eq!(copied_record.staged.len, ref_b.len);

    let mut staged_reader =
        strata_segment::SegmentReader::open(&output.path, output.staged_segment_id).unwrap();
    assert_eq!(
        staged_reader.read_payload(copied_record.staged).unwrap(),
        b"payload-b"
    );
}

#[tokio::test]
async fn gc_copy_splits_mixed_ingest_records_into_shard_retention_segments() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let shard_a = store.add_shard(7).unwrap();
    let shard_b = store.add_shard(8).unwrap();

    let lsn_a = store.store.put(shard_a.id, &key_a, b"payload-a").unwrap();
    let lsn_b = store.store.put(shard_b.id, &key_b, b"payload-b").unwrap();
    let lsn_c = store.store.put(shard_a.id, &key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    assert_eq!(prepared.plan.scenario, GcScenario::L0Compaction);
    assert_eq!(prepared.plan.copied_bytes, TEST_RECORD_LEN * 2);

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.outputs.len(), 2);
    assert_eq!(
        copied
            .outputs
            .iter()
            .map(|output| output.shard)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([shard_a, shard_b])
    );
    for output in &copied.outputs {
        assert_eq!(output.destination_class, DestinationClass::Spillover);
        assert_eq!(output.placement_class, PlacementClass::Spillover);
        assert_eq!(output.sealed_len, TEST_RECORD_LEN);
    }

    let published = store.publish_prepared_gc_copy(copied).unwrap();
    assert_eq!(published.skipped_records, Vec::new());
    assert_eq!(published.output_segments.len(), 2);
    assert_eq!(published.published_records.len(), 2);

    for output in &published.output_segments {
        let expected_path = layout::retention_segment_path(
            store.config(),
            output.shard,
            PlacementClass::Spillover,
            output.segment_id,
        );
        assert_eq!(output.path, expected_path);
        assert!(output.path.exists());
        let state = store
            .index()
            .get_segment_state(output.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(state.owner, SegmentOwner::Shard(output.shard));
        assert_eq!(state.placement_class, PlacementClass::Spillover);
        assert_eq!(segment_state_path(store.config(), &state), output.path);
    }

    let shard_a_dir = layout::shard_retention_dir(store.config(), shard_a);
    let shard_b_dir = layout::shard_retention_dir(store.config(), shard_b);
    let shard_a_segment_ids = published
        .output_segments
        .iter()
        .filter_map(|output| (output.shard == shard_a).then_some(output.segment_id))
        .collect::<Vec<_>>();
    assert!(shard_a_dir.exists());
    assert!(shard_b_dir.exists());

    store.store.drop_shard(shard_a.id).unwrap();
    wait_for_shard_cleanup(&store.store, shard_a);

    assert!(!shard_a_dir.exists());
    assert!(shard_b_dir.exists());
    assert!(
        store
            .index()
            .iter_segment_states_for_shard(shard_a)
            .unwrap()
            .is_empty()
    );
    for segment_id in shard_a_segment_ids {
        assert!(
            store
                .index()
                .get_segment_state(segment_id)
                .unwrap()
                .is_none()
        );
    }

    assert!(store.get_from_shard(shard_a.id, &key_a).is_err());
    assert_eq!(
        store.get_from_shard(shard_b.id, &key_b).unwrap(),
        Some(b"payload-b".to_vec())
    );
    assert!(lsn_a < lsn_b && lsn_b < lsn_c);
}

#[tokio::test]
async fn gc_publish_skips_copy_prepared_before_shard_drop() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let dropped_shard = store.add_shard(17).unwrap();
    let kept_shard = store.add_shard(18).unwrap();
    let dropped_key = BlobKey::new(b"drop-copy".to_vec()).unwrap();
    let kept_key = BlobKey::new(b"keep-copy".to_vec()).unwrap();
    let rollover_key = BlobKey::new(b"roll-copy".to_vec()).unwrap();

    store
        .store
        .put(dropped_shard.id, &dropped_key, b"payload-a")
        .unwrap();
    store
        .store
        .put(kept_shard.id, &kept_key, b"payload-b")
        .unwrap();
    let rollover_lsn = store
        .store
        .put(dropped_shard.id, &rollover_key, b"payload-c")
        .unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, rollover_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.outputs.len(), 2);

    store.store.drop_shard(dropped_shard.id).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert_eq!(published.skipped_records.len(), 1);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(published.published_records[0].source.shard, kept_shard);
    assert_eq!(published.output_segments.len(), 1);
    assert_eq!(published.output_segments[0].shard, kept_shard);
    assert!(!layout::shard_retention_dir(store.config(), dropped_shard).exists());
    assert!(layout::shard_retention_dir(store.config(), kept_shard).exists());
    assert_eq!(
        store.get_from_shard(kept_shard.id, &kept_key).unwrap(),
        Some(b"payload-b".to_vec())
    );
}

#[tokio::test]
async fn gc_publish_empty_delete_plan_deletes_segment_file() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    store.sync().unwrap();
    let sealed_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    let sealed_path = segment_state_path(store.config(), &sealed_state);
    assert!(sealed_path.exists());
    wait_for_accounted_lsn(&store, lsn_b);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

    assert_eq!(prepared.accounting_snapshot.accounted_lsn(), tombstone_lsn);
    assert_eq!(prepared.plan.scenario, GcScenario::EmptyDelete);
    assert_eq!(
        prepared.plan.action,
        GcAction::DeleteSegments {
            segment_ids: vec![FIRST_SEGMENT_ID]
        }
    );

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.output_segments.is_empty());
    assert!(published.published_records.is_empty());
    assert!(published.skipped_records.is_empty());
    assert_eq!(
        store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::Deleted
    );
    assert!(!sealed_path.exists());
    assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&key_a).unwrap(), None);
    assert!(lsn_a < lsn_b);
}

#[tokio::test]
async fn gc_publish_empty_delete_plan_batches_multiple_segment_files() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    let first_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    let second_segment_id = FIRST_SEGMENT_ID + 1;
    let second_state =
        wait_for_segment_state(store.index(), second_segment_id, SegmentFileState::Sealed);
    store.sync().unwrap();
    let first_path = segment_state_path(store.config(), &first_state);
    let second_path = segment_state_path(store.config(), &second_state);
    assert!(first_path.exists());
    assert!(second_path.exists());
    wait_for_accounted_lsn(&store, lsn_c);

    store.tombstone(&key_a).unwrap();
    let tombstone_lsn_b = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn_b);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

    assert_eq!(prepared.plan.scenario, GcScenario::EmptyDelete);
    assert_eq!(
        prepared.plan.action,
        GcAction::DeleteSegments {
            segment_ids: vec![FIRST_SEGMENT_ID, second_segment_id]
        }
    );

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.output_segments.is_empty());
    assert!(published.published_records.is_empty());
    assert!(published.skipped_records.is_empty());
    for segment_id in [FIRST_SEGMENT_ID, second_segment_id] {
        assert_eq!(
            store
                .index()
                .get_segment_state(segment_id)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::Deleted
        );
    }
    assert!(!first_path.exists());
    assert!(!second_path.exists());
    assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
    assert_eq!(store.get(&key_a).unwrap(), None);
    assert_eq!(store.get(&key_b).unwrap(), None);
    assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));
    assert!(lsn_b < lsn_c);
}

#[tokio::test]
async fn gc_worker_request_runs_production_gc_plan() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    store.sync().unwrap();
    let sealed_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    let sealed_path = segment_state_path(store.config(), &sealed_state);
    wait_for_accounted_lsn(&store, lsn_b);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);
    store.request_gc().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        if state.state == SegmentFileState::Deleted && !sealed_path.exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "gc worker did not delete segment and remove file"
        );
        thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&key_a).unwrap(), None);
    assert!(lsn_a < lsn_b);
}

#[tokio::test]
async fn accounting_epoch_expiry_nudges_gc_after_materializing_empty_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    cfg.gc_interval = Duration::from_secs(3600);
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.extend(&key_a, 43).unwrap().unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    store.sync().unwrap();
    let mut sealed_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    sealed_state.placement_class = PlacementClass::ExactEpoch(43);
    store.index().put_segment_state(&sealed_state).unwrap();
    let sealed_path = segment_state_path(store.config(), &sealed_state);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_b);

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        if state.state == SegmentFileState::Deleted && !sealed_path.exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "accounting did not nudge GC after epoch expiry"
        );
        thread::sleep(Duration::from_millis(10));
    }

    assert!(store.accounted_lsn().unwrap() >= epoch_lsn);
    assert_eq!(store.get(&key_a).unwrap(), None);
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
}

#[tokio::test]
async fn gc_worker_count_broadcasts_request_to_parallel_workers() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    cfg.gc_worker_count = 2;
    cfg.gc_initial_worker_count = 2;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let _lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    let first_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    let second_segment_id = FIRST_SEGMENT_ID + 1;
    let second_state =
        wait_for_segment_state(store.index(), second_segment_id, SegmentFileState::Sealed);
    store.sync().unwrap();
    let first_path = segment_state_path(store.config(), &first_state);
    let second_path = segment_state_path(store.config(), &second_state);
    wait_for_accounted_lsn(&store, lsn_c);

    store.tombstone(&key_a).unwrap();
    let tombstone_lsn_b = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn_b);
    assert_eq!(store.store.gc_txs.len(), 2);

    store.request_gc().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let first_deleted = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap()
            .state
            == SegmentFileState::Deleted;
        let second_deleted = store
            .index()
            .get_segment_state(second_segment_id)
            .unwrap()
            .unwrap()
            .state
            == SegmentFileState::Deleted;
        if first_deleted && second_deleted && !first_path.exists() && !second_path.exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "two-worker GC request did not delete both empty source segments"
        );
        thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(store.get(&key_a).unwrap(), None);
    assert_eq!(store.get(&key_b).unwrap(), None);
    assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));
    assert!(lsn_b < lsn_c);
}

#[tokio::test]
async fn gc_publish_reclassify_plan_updates_segment_placement() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();
    let current_epoch = store.current_epoch().unwrap();
    let segment_id = 10;
    let range = SegmentGcRecordRange {
        offset: 0,
        len: TEST_RECORD_LEN,
    };
    let state = SegmentState {
        owner: INGEST_SEGMENT_OWNER,
        segment_id,
        volume_id: 0,
        path: format!("ingest/{segment_id:012}.data"),
        placement_class: PlacementClass::ExactEpoch(current_epoch),
        state: SegmentFileState::Sealed,
        write_offset: TEST_RECORD_LEN,
        durable_offset: TEST_RECORD_LEN,
        min_lsn: Some(0),
        max_lsn: Some(0),
        sealed_before_lsn: None,
        sealed_len: Some(TEST_RECORD_LEN),
        sealed_sha256: None,
    };
    let mut batch = store.index().batch();
    store
        .index()
        .put_segment_state_batch(&mut batch, &state)
        .unwrap();
    store
        .index()
        .merge_segment_gc_overlay_batch(
            &mut batch,
            segment_id,
            vec![SegmentGcOverlayMergeOp::AddLiveBatch {
                records: vec![SegmentGcLiveRecord {
                    range,
                    lifecycle: Some(BlobLifecycle {
                        logical_end_epoch: current_epoch + 10,
                        extension_count: 2,
                    }),
                }],
            }],
        )
        .unwrap();
    batch.write().unwrap();
    store.index().flush_wal(true).unwrap();

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: 1,
        max_l0_copy_bytes_per_plan: 1,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();

    assert_eq!(prepared.plan.scenario, GcScenario::PinnedEpochExpiry);
    assert_eq!(
        prepared.plan.action,
        GcAction::ReclassifySegment {
            segment_id,
            placement_class: PlacementClass::Spillover,
        }
    );

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.output_segments.is_empty());
    assert!(published.published_records.is_empty());
    assert!(published.skipped_records.is_empty());
    assert_eq!(
        store
            .index()
            .get_segment_state(segment_id)
            .unwrap()
            .unwrap()
            .placement_class,
        PlacementClass::Spillover
    );
}

#[tokio::test]
async fn gc_publish_maps_surviving_copied_record_to_output_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c);

    let ref_a = store
        .index()
        .get_blob_version(&version_key(&key_a, lsn_a))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version(&version_key(&key_b, lsn_b))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    let staged_path = copied.outputs[0].path.clone();

    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_accounted_lsn >= tombstone_lsn);
    assert_eq!(published.skipped_records, Vec::new());
    assert_eq!(published.output_segments.len(), 1);
    assert_eq!(published.published_records.len(), 1);
    assert!(!staged_path.exists());
    assert!(published.output_segments[0].path.exists());

    let published_record = &published.published_records[0];
    assert_eq!(published_record.source.from, ref_b);
    assert_eq!(
        store
            .index()
            .get_blob_entry(&key_b)
            .unwrap()
            .unwrap()
            .record_ref,
        Some(published_record.to)
    );
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(
        store
            .index()
            .get_segment_state(published_record.to.segment_id)
            .unwrap()
            .unwrap()
            .placement_class,
        PlacementClass::Spillover
    );
    assert!(store.durable_lsn().unwrap() < published_record.publish_lsn);
    store.sync().unwrap();
    assert!(store.durable_lsn().unwrap() >= published_record.publish_lsn);

    wait_for_accounted_lsn(&store, published_record.publish_lsn);

    let source_overlay = store
        .index()
        .get_segment_gc_overlay(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

    let output_summary = segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary.live_bytes, ref_b.len);
    assert_eq!(output_summary.live_ref_count, 1);
    assert_eq!(output_summary.total_bytes, ref_b.len);
}

#[tokio::test]
async fn gc_publish_pre_commit_failure_removes_renamed_output_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 4 - 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let key_d = BlobKey::new(b"blob-d".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    let lsn_d = store.put(&key_d, b"payload-d").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_d);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.copied_records.len(), 2);
    assert_eq!(copied.outputs.len(), 1);
    let staged_path = copied.outputs[0].path.clone();
    let output_segment_id = store
        .index()
        .iter_segment_states()
        .unwrap()
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .max()
        .unwrap()
        .checked_add(1)
        .unwrap();
    let final_path = layout::retention_segment_path(
        store.config(),
        copied.outputs[0].shard,
        copied.outputs[0].placement_class,
        output_segment_id,
    );

    let mut batch = store.index().batch();
    store
        .index()
        .put_next_lsn_batch(&mut batch, StrataLsn::MAX)
        .unwrap();
    batch.write().unwrap();
    store.index().flush_wal(true).unwrap();

    let err = store.publish_prepared_gc_copy(copied).unwrap_err();

    assert!(matches!(
        err,
        Error::Segment(strata_segment::Error::RangeOverflow)
    ));
    assert!(!staged_path.exists());
    assert!(!final_path.exists());
    assert!(
        store
            .index()
            .get_segment_state(output_segment_id)
            .unwrap()
            .is_some_and(|state| state.state == SegmentFileState::Deleted)
    );
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));
    assert!(lsn_b < lsn_c);
}

#[tokio::test]
async fn gc_publish_tombstoned_unaccounted_copy_retires_destination_after_forwarding() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.accounting_interval = Duration::from_secs(3600);
    cfg.accounting_sidecar_major_patch_count_threshold = 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c);

    let ref_a = store
        .index()
        .get_blob_version(&version_key(&key_a, lsn_a))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version(&version_key(&key_b, lsn_b))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_a_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    stop_accounting_worker(&mut store.store);

    let tombstone_b_lsn = store.tombstone(&key_b).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_accounted_lsn < tombstone_b_lsn);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(store.get(&key_b).unwrap(), None);

    let published_record = &published.published_records[0];
    assert_eq!(published_record.source.from, ref_b);
    let output_summary_before_accounting =
        segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary_before_accounting.total_bytes, 0);
    assert_eq!(output_summary_before_accounting.live_bytes, 0);
    assert_eq!(output_summary_before_accounting.live_ref_count, 0);
    assert_eq!(output_summary_before_accounting.garbage_bytes(), 0);
    assert!(store.index().get_gc_relocation(ref_b).unwrap().is_some());

    store.sync().unwrap();
    for _ in 0..4 {
        accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
        if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
            break;
        }
    }
    assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

    let source_overlay = store
        .index()
        .get_segment_gc_overlay(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

    let output_summary = segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary.total_bytes, ref_b.len);
    assert_eq!(output_summary.live_bytes, 0);
    assert_eq!(output_summary.live_ref_count, 0);
    assert_eq!(output_summary.retired_bytes, ref_b.len);
    assert_eq!(output_summary.garbage_bytes(), ref_b.len);
    assert!(store.index().get_gc_relocation(ref_b).unwrap().is_none());
}

#[tokio::test]
async fn gc_publish_unaccounted_epoch_change_expires_relocated_destination() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.accounting_interval = Duration::from_secs(3600);
    cfg.accounting_sidecar_major_patch_count_threshold = 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    let lifetime_b_lsn = store.extend(&key_b, 43).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c.max(lifetime_b_lsn));

    let ref_a = store
        .index()
        .get_blob_version(&version_key(&key_a, lsn_a))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version(&version_key(&key_b, lsn_b))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_a_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    stop_accounting_worker(&mut store.store);

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(store.get(&key_b).unwrap(), None);
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_accounted_lsn < epoch_lsn);
    assert_eq!(published.published_records.len(), 1);
    let published_record = &published.published_records[0];
    assert_eq!(published_record.source.from, ref_b);
    assert_eq!(
        published_record.source.lifecycle.unwrap().logical_end_epoch,
        43
    );

    let output_summary_before_accounting =
        segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary_before_accounting.total_bytes, 0);
    assert_eq!(output_summary_before_accounting.live_bytes, 0);
    assert_eq!(output_summary_before_accounting.expired_bytes, 0);

    store.sync().unwrap();
    for _ in 0..4 {
        accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
        if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
            break;
        }
    }
    assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

    let source_overlay = store
        .index()
        .get_segment_gc_overlay(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

    let output_summary = segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary.total_bytes, ref_b.len);
    assert_eq!(output_summary.live_bytes, 0);
    assert_eq!(output_summary.live_ref_count, 0);
    assert_eq!(output_summary.expired_bytes, ref_b.len);
    assert_eq!(output_summary.retired_bytes, 0);
    assert_eq!(output_summary.garbage_bytes(), ref_b.len);
}

#[tokio::test]
async fn gc_publish_forwards_lagging_lifetime_before_epoch_expiry() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.accounting_interval = Duration::from_secs(3600);
    cfg.accounting_sidecar_major_patch_count_threshold = 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let mut store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lsn_c);

    let ref_a = store
        .index()
        .get_blob_version(&version_key(&key_a, lsn_a))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_version(&version_key(&key_b, lsn_b))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_a_lsn);

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 1,
        min_reclaim_bytes: 1,
        min_garbage_ratio_bps: 1,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.copied_records[0].source.lifecycle, None);
    stop_accounting_worker(&mut store.store);

    let lifetime_b_lsn = store.extend(&key_b, 43).unwrap().unwrap();
    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(store.get(&key_b).unwrap(), None);
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_accounted_lsn < lifetime_b_lsn);
    assert!(published.reconciled_accounted_lsn < epoch_lsn);
    assert_eq!(published.published_records.len(), 1);
    let published_record = &published.published_records[0];
    assert_eq!(published_record.source.from, ref_b);
    assert_eq!(published_record.source.lifecycle, None);

    let output_summary_before_accounting =
        segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary_before_accounting.total_bytes, 0);
    assert_eq!(output_summary_before_accounting.live_bytes, 0);
    assert_eq!(output_summary_before_accounting.expired_bytes, 0);

    store.sync().unwrap();
    for _ in 0..4 {
        accounting::run_accounting_sidecar_once(store.index(), store.config(), true).unwrap();
        if store.accounted_lsn().unwrap() >= published_record.publish_lsn {
            break;
        }
    }
    assert!(store.accounted_lsn().unwrap() >= published_record.publish_lsn);

    let source_overlay = store
        .index()
        .get_segment_gc_overlay(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

    let output_summary = segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary.total_bytes, ref_b.len);
    assert_eq!(output_summary.live_bytes, 0);
    assert_eq!(output_summary.live_ref_count, 0);
    assert_eq!(output_summary.expired_bytes, ref_b.len);
    assert_eq!(output_summary.retired_bytes, 0);
    assert_eq!(output_summary.garbage_bytes(), ref_b.len);
}

#[tokio::test]
async fn accounting_publishes_lifecycle_changed_segment_ref_event() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();

    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let lifecycle = BlobLifecycle {
        logical_end_epoch: 50,
        extension_count: 0,
    };
    assert_eq!(
        store
            .index()
            .iter_segment_ref_events_since(record_ref.segment_id, put_lsn)
            .unwrap(),
        vec![(
            strata_core::SegmentRefEventKey {
                segment_id: record_ref.segment_id,
                lsn: lifetime_lsn,
                offset: record_ref.offset,
            },
            SegmentRefEvent::LifecycleChanged {
                lifecycle: Some(lifecycle),
            },
        )]
    );
    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap();
    assert!(overlay.expired.is_empty());
    assert!(overlay.retired.is_empty());
    assert_eq!(
        overlay.lifetimes,
        vec![SegmentGcLifetimeRange {
            range: gc_range(record_ref),
            lifecycle,
        }]
    );
}

#[tokio::test]
async fn accounting_gc_overlay_retire_removes_lifetime_hint() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
    let record_ref = store
        .index()
        .get_blob_version(&version_key(&key, put_lsn))
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.lifetimes.len(), 1);

    let tombstone_lsn = store.tombstone(&key).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let overlay = store
        .index()
        .get_segment_gc_overlay(record_ref.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
    assert!(overlay.expired.is_empty());
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn accounting_updates_gc_summary_on_epoch_change() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-bb").unwrap();
    store.extend(&key_a, 43).unwrap().unwrap();
    let lifetime_b_lsn = store.extend(&key_b, 50).unwrap().unwrap();
    let ref_a = store
        .index()
        .get_blob_entry(&key_a)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let ref_b = store
        .index()
        .get_blob_entry(&key_b)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_b_lsn);

    let stats = segment_summary(store.index(), ref_a.segment_id);
    assert_eq!(stats.live_bytes, ref_a.len + ref_b.len);
    assert_eq!(stats.live_ref_count, 2);
    assert_eq!(stats.expired_bytes, 0);

    let (epoch, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(epoch, 43);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, epoch_lsn);

    let stats = segment_summary(store.index(), ref_a.segment_id);
    assert_eq!(stats.live_bytes, ref_b.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(stats.expired_bytes, ref_a.len);
    assert_eq!(stats.retired_bytes, 0);
    assert_eq!(stats.future_epoch_histogram.get(&43), None);
    assert_eq!(
        stats.future_epoch_histogram.get(&50),
        Some(&EpochBucket {
            refs: 1,
            bytes: ref_b.len,
        })
    );
    assert_eq!(stats.min_live_end_epoch, Some(50));
    assert!(!stats.is_empty());

    let mut last_epoch_lsn = epoch_lsn;
    while store.current_epoch().unwrap() < 50 {
        let (_, lsn) = store.increment_epoch().unwrap();
        last_epoch_lsn = lsn;
    }
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, last_epoch_lsn);

    let stats = segment_summary(store.index(), ref_a.segment_id);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
    assert_eq!(stats.expired_bytes, ref_a.len + ref_b.len);
    assert_eq!(stats.total_bytes, ref_a.len + ref_b.len);
    assert!(stats.future_epoch_histogram.is_empty());
    assert_eq!(stats.min_live_end_epoch, None);
    assert!(stats.is_empty());
}

#[tokio::test]
async fn accounting_skips_live_counters_for_tombstone_of_expired_blob() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, epoch_lsn);

    let tombstone_lsn = store.tombstone(&key).unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, tombstone_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.total_bytes, record_ref.len);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
    assert_eq!(stats.expired_bytes, 0);
    assert_eq!(stats.retired_bytes, record_ref.len);
    assert_eq!(store.get(&key).unwrap(), None);
}

#[tokio::test]
async fn accounting_orders_blob_ops_and_epoch_changes_within_one_run() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.extend(&key_a, 43).unwrap().unwrap();
    let ref_a = store
        .index()
        .get_blob_entry(&key_a)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.increment_epoch().unwrap();
    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    let put_b_lsn = store.put(&key_b, b"payload-bb").unwrap();
    let lifetime_b_lsn = store.extend(&key_b, 44).unwrap().unwrap();
    let ref_b = store
        .index()
        .get_blob_entry(&key_b)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_b_lsn.max(put_b_lsn).max(tombstone_lsn));

    // The put of blob A is accounted live at epoch 42, the epoch change to 43 expires it, and
    // the later tombstone moves it from expired bytes to permanently retired bytes.
    let stats = segment_summary(store.index(), ref_a.segment_id);
    assert_eq!(stats.total_bytes, ref_a.len + ref_b.len);
    assert_eq!(stats.expired_bytes, 0);
    assert_eq!(stats.retired_bytes, ref_a.len);
    assert_eq!(stats.live_bytes, ref_b.len);
    assert_eq!(stats.live_ref_count, 1);
    assert_eq!(
        stats.future_epoch_histogram.get(&44),
        Some(&EpochBucket {
            refs: 1,
            bytes: ref_b.len,
        })
    );
}

#[tokio::test]
async fn accounting_does_not_revive_expired_blob_on_extension() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, epoch_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.expired_bytes, record_ref.len);
    assert_eq!(stats.live_ref_count, 0);

    let extend_lsn = store.extend(&key, 50).unwrap().unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, extend_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.expired_bytes, record_ref.len);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
    assert_eq!(stats.future_epoch_histogram.get(&50), None);
    assert_eq!(stats.min_live_end_epoch, None);
}

#[tokio::test]
async fn accounting_expires_future_epoch_bucket_for_exact_epoch_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let record_ref = store
        .index()
        .get_blob_entry(&key)
        .unwrap()
        .unwrap()
        .record_ref
        .unwrap();
    let mut state = store
        .index()
        .get_segment_state(record_ref.segment_id)
        .unwrap()
        .unwrap();
    state.placement_class = PlacementClass::ExactEpoch(42);
    store.index().put_segment_state(&state).unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, lifetime_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(
        stats.future_epoch_histogram.get(&43),
        Some(&EpochBucket {
            refs: 1,
            bytes: record_ref.len,
        })
    );

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();
    wait_for_accounted_lsn(&store, epoch_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.future_epoch_histogram.get(&43), None);
    assert_eq!(stats.expired_bytes, record_ref.len);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
}

#[tokio::test]
async fn put_assigns_monotonic_lsn_across_reopen() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        let lsn_1 = store.put(&key_1, b"payload-a").unwrap();
        let lsn_2 = store.put(&key_2, b"payload-b").unwrap();

        assert_eq!(lsn_1, 1);
        assert_eq!(lsn_2, 2);
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_1)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            1
        );
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_2)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            2
        );
        assert_eq!(
            store.index().get_blob_entry(&key_2).unwrap().unwrap().lsn,
            2
        );
        assert_eq!(store.index().get_next_lsn().unwrap(), 3);
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let lsn_3 = store.put(&key_3, b"x").unwrap();

    assert_eq!(lsn_3, 3);
    assert_eq!(store.index().get_next_lsn().unwrap(), 4);
    let active_state = store.index().get_segment_state(2).unwrap().unwrap();
    assert_eq!(active_state.min_lsn, Some(2));
    assert_eq!(active_state.max_lsn, Some(3));
}

#[tokio::test]
async fn recovery_rolls_back_ops_missing_from_active_delta_log() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload-a").unwrap(), 1);
        assert_eq!(store.index().get_next_lsn().unwrap(), 2);
    }

    std::fs::remove_file(ActiveDeltaLog::path(cfg.accounting_index_dir(), 1)).unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.put(&key, b"payload-b").unwrap(), 1);
    assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
}

#[tokio::test]
async fn recovery_rejects_missing_active_delta_log_for_durable_lsn() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let mut store =
            try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload-a").unwrap(), 1);
        store.sync().unwrap();
        assert_eq!(store.durable_lsn().unwrap(), 1);
        stop_accounting_worker(&mut store.store);

        let mut batch = store.index().batch();
        store
            .index()
            .put_accounting_active_delta_log_state_batch(&mut batch, ActiveDeltaLogState::default())
            .unwrap();
        batch.write_with_sync(true).unwrap();
    }

    std::fs::remove_file(ActiveDeltaLog::path(cfg.accounting_index_dir(), 1)).unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(
        err,
        Error::RecoveryDurableAccountingGap {
            durable_lsn: 1,
            active_delta_log_lsn: 0,
        }
    ));
}

#[tokio::test]
async fn recovery_keeps_latest_valid_blob_version() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let second;
    let second_lsn;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key, b"payload-a").unwrap();
        second_lsn = store.put(&key, b"payload-b").unwrap();
        second = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        store.index().flush_wal(true).unwrap();
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(
        store
            .index()
            .get_blob_version(&version_key(&key, second_lsn))
            .unwrap()
            .unwrap()
            .record_ref,
        Some(second)
    );
    assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
}

#[tokio::test]
async fn point_in_time_recovery_discards_higher_segments_after_lower_gap() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let first_end;
    let second_end;
    {
        let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
        fs::create_dir_all(cfg.ingest_dir()).unwrap();

        let segment_1_path = segment_path(&cfg, 1);
        let mut segment_1 =
            SegmentWriter::create(&segment_1_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
        let out_a = segment_1.append(&key_a, 1, b"payload-a").unwrap();
        first_end = segment_1.write_offset();
        let out_b = segment_1.append(&key_b, 2, b"payload-b").unwrap();
        second_end = segment_1.write_offset();
        drop(segment_1);
        OpenOptions::new()
            .write(true)
            .open(&segment_1_path)
            .unwrap()
            .set_len(first_end)
            .unwrap();

        let segment_2_path = segment_path(&cfg, 2);
        let mut segment_2 =
            SegmentWriter::create(&segment_2_path, 2, PlacementClass::Ingest, 1 << 20).unwrap();
        let out_c = segment_2.append(&key_c, 3, b"payload-c").unwrap();
        let segment_2_end = segment_2.write_offset();
        drop(segment_2);

        let mut segment_1_state =
            active_segment_state_from_path(&cfg, INGEST_SEGMENT_OWNER, 1, second_end, 0);
        segment_1_state.state = SegmentFileState::Sealing;
        let segment_2_state =
            active_segment_state_from_path(&cfg, INGEST_SEGMENT_OWNER, 2, segment_2_end, 0);

        let mut segment_1_delta_log = ActiveDeltaLog::open(
            cfg.accounting_index_dir(),
            1,
            ActiveDeltaLogState::default(),
        )
        .unwrap();
        for (key, record_ref, lsn) in [(&key_a, out_a.record_ref, 1), (&key_b, out_b.record_ref, 2)]
        {
            segment_1_delta_log
                .append(&AccountingDelta::Blob(BlobUpdate::Put {
                    lsn,
                    key: key.clone(),
                    shard: STANDALONE_SHARD,
                    record_ref,
                    current_epoch: 42,
                    lifecycle: None,
                }))
                .unwrap();
        }
        segment_1_delta_log.sync_data().unwrap();
        let mut active_delta_log =
            ActiveDeltaLog::open(cfg.accounting_index_dir(), 2, segment_1_delta_log.state())
                .unwrap();
        active_delta_log
            .append(&AccountingDelta::Blob(BlobUpdate::Put {
                lsn: 3,
                key: key_c.clone(),
                shard: STANDALONE_SHARD,
                record_ref: out_c.record_ref,
                current_epoch: 42,
                lifecycle: None,
            }))
            .unwrap();
        active_delta_log.sync_data().unwrap();

        let mut batch = index.batch();
        for (key, record_ref, lsn) in [
            (&key_a, out_a.record_ref, 1),
            (&key_b, out_b.record_ref, 2),
            (&key_c, out_c.record_ref, 3),
        ] {
            index
                .put_blob_version_batch(
                    &mut batch,
                    key,
                    &PutEntry {
                        record_ref: Some(record_ref),
                        lsn,
                        generation: lsn,
                        state: BlobState::Live,
                    },
                )
                .unwrap();
            index
                .put_blob_unaccounted_lsn_op_batch(&mut batch, lsn, key)
                .unwrap();
        }
        index
            .put_segment_state_batch(&mut batch, &segment_1_state)
            .unwrap();
        index
            .put_segment_state_batch(&mut batch, &segment_2_state)
            .unwrap();
        index
            .put_accounting_active_delta_log_state_batch(&mut batch, active_delta_log.state())
            .unwrap();
        batch.write().unwrap();
        index.flush_wal(true).unwrap();
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    assert_eq!(store.get(&key_a).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_b).unwrap(), None);
    assert_eq!(store.get(&key_c).unwrap(), None);
    assert_eq!(
        store
            .index()
            .get_segment_state(1)
            .unwrap()
            .unwrap()
            .write_offset,
        first_end
    );
    assert_eq!(
        store.index().get_segment_state(2).unwrap().unwrap().state,
        SegmentFileState::Deleted
    );
    assert_eq!(
        store.index().get_blob_entry(&key_a).unwrap().unwrap().state,
        BlobState::Live
    );
    assert_eq!(store.index().get_blob_entry(&key_b).unwrap(), None);
    assert_eq!(store.index().get_blob_entry(&key_c).unwrap(), None);
}

#[tokio::test]
async fn absolute_consistency_recovery_fails_on_unsealed_gap() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let first_end;
    let second_end;
    {
        let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
        fs::create_dir_all(cfg.ingest_dir()).unwrap();
        let segment_path = segment_path(&cfg, 1);
        let mut segment =
            SegmentWriter::create(&segment_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
        let out_a = segment.append(&key_a, 1, b"payload-a").unwrap();
        first_end = segment.write_offset();
        let out_b = segment.append(&key_b, 2, b"payload-b").unwrap();
        second_end = segment.write_offset();
        drop(segment);
        OpenOptions::new()
            .write(true)
            .open(&segment_path)
            .unwrap()
            .set_len(first_end)
            .unwrap();

        let state = active_segment_state_from_path(&cfg, INGEST_SEGMENT_OWNER, 1, second_end, 0);
        let mut batch = index.batch();
        for (key, record_ref, lsn) in [(&key_a, out_a.record_ref, 1), (&key_b, out_b.record_ref, 2)]
        {
            index
                .put_blob_version_batch(
                    &mut batch,
                    key,
                    &PutEntry {
                        record_ref: Some(record_ref),
                        lsn,
                        generation: lsn,
                        state: BlobState::Live,
                    },
                )
                .unwrap();
            index
                .put_blob_unaccounted_lsn_op_batch(&mut batch, lsn, key)
                .unwrap();
        }
        index.put_segment_state_batch(&mut batch, &state).unwrap();
        batch.write().unwrap();
        index.flush_wal(true).unwrap();
    }

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();

    assert!(matches!(
        err,
        Error::RecoveryInconsistent {
            segment_id: 1,
            expected_write_offset,
            recovered_write_offset,
        } if expected_write_offset == second_end && recovered_write_offset == first_end
    ));
}

#[tokio::test]
async fn unsealed_segment_count_includes_open_and_sealing_segments() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(dir.path().join("index"), "strata/default");

    put_test_segment_state(&index, 1, SegmentFileState::Open);
    put_test_segment_state(&index, 2, SegmentFileState::Sealing);
    put_test_segment_state(&index, 3, SegmentFileState::Sealed);

    assert_eq!(unsealed_ingest_segment_ids(&index).unwrap(), vec![1, 2]);
    assert_eq!(unsealed_ingest_segment_count(&index).unwrap(), 2);
}

#[tokio::test]
async fn seal_publisher_waits_for_lowest_sealing_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    put_test_segment_state(&index, 1, SegmentFileState::Sealing);
    put_test_segment_state(&index, 2, SegmentFileState::Sealing);
    let mut completed = BTreeMap::new();
    let mut sealing = [Reverse(1), Reverse(2)].into_iter().collect();
    let mut sealing_ids = [1, 2].into_iter().collect();
    let durability_publish_lock = Arc::new(Mutex::new(()));

    completed.insert(
        2,
        seal::CompletedSeal {
            task: SegmentSealTask {
                segment_id: 2,
                sealed_len: 64,
                sealed_before_lsn: 1,
            },
            sealed_sha256: None,
            active_delta_state: ActiveDeltaLogState::default(),
        },
    );
    seal::publish_ready_completed_seals(
        &cfg,
        &index,
        INGEST_SEGMENT_OWNER,
        &durability_publish_lock,
        None,
        &StrataStoreMetrics::default(),
        &mut completed,
        &mut sealing,
        &mut sealing_ids,
    )
    .unwrap();
    assert_eq!(
        index.get_segment_state(1).unwrap().unwrap().state,
        SegmentFileState::Sealing
    );
    assert_eq!(
        index.get_segment_state(2).unwrap().unwrap().state,
        SegmentFileState::Sealing
    );

    completed.insert(
        1,
        seal::CompletedSeal {
            task: SegmentSealTask {
                segment_id: 1,
                sealed_len: 64,
                sealed_before_lsn: 1,
            },
            sealed_sha256: None,
            active_delta_state: ActiveDeltaLogState::default(),
        },
    );
    seal::publish_ready_completed_seals(
        &cfg,
        &index,
        INGEST_SEGMENT_OWNER,
        &durability_publish_lock,
        None,
        &StrataStoreMetrics::default(),
        &mut completed,
        &mut sealing,
        &mut sealing_ids,
    )
    .unwrap();
    assert_eq!(
        index.get_segment_state(1).unwrap().unwrap().state,
        SegmentFileState::Sealed
    );
    assert_eq!(
        index.get_segment_state(2).unwrap().unwrap().state,
        SegmentFileState::Sealed
    );
    assert!(completed.is_empty());
}

#[tokio::test]
async fn reopen_detects_missing_sealed_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    seal_first_segment(&cfg);

    std::fs::remove_file(segment_path(&cfg, FIRST_SEGMENT_ID)).unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(
        err,
        Error::SealedSegmentMissing {
            segment_id: FIRST_SEGMENT_ID,
            ..
        }
    ));
}

#[tokio::test]
async fn reopen_detects_sealed_segment_length_mismatch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let sealed = seal_first_segment(&cfg);
    let sealed_len = sealed.sealed_len.unwrap();
    assert!(sealed_len > 0);

    OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .set_len(sealed_len - 1)
        .unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(
        err,
        Error::SealedSegmentLengthMismatch {
            segment_id: FIRST_SEGMENT_ID,
            expected_len,
            actual_len,
            ..
        } if expected_len == sealed_len && actual_len == sealed_len - 1
    ));
}

#[tokio::test]
async fn metadata_only_reopen_does_not_hash_sealed_segment_bytes() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::MetadataOnly;
    let sealed = seal_first_segment(&cfg);
    assert_eq!(sealed.sealed_sha256, None);

    OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .write_all(b"X")
        .unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(
        store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::Sealed
    );
}

#[tokio::test]
async fn checksum_reopen_detects_sealed_segment_hash_mismatch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::Checksum;
    let sealed = seal_first_segment(&cfg);
    assert!(sealed.sealed_sha256.is_some());

    OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, FIRST_SEGMENT_ID))
        .unwrap()
        .write_all(b"X")
        .unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(
        err,
        Error::SealedSegmentChecksumMismatch {
            segment_id: FIRST_SEGMENT_ID,
            ..
        }
    ));
}

#[tokio::test]
async fn seal_segment_reports_error_without_marking_failed() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    put_test_segment_state(&index, 1, SegmentFileState::Sealing);
    let (_seal_tx, seal_rx) = mpsc::channel();
    let (accounting_tx, _accounting_rx) = mpsc::sync_channel(1);
    let worker = SealWorker {
        config: cfg,
        index: index.clone(),
        ingest_owner: INGEST_SEGMENT_OWNER,
        seal_rx,
        durability_publish_lock: Arc::new(Mutex::new(())),
        accounting_tx: Some(accounting_tx),
        metrics: StrataStoreMetrics::default(),
        store_halt: StoreHalt::default(),
    };

    assert!(
        worker
            .seal_segment(SegmentSealTask {
                segment_id: 1,
                sealed_len: 64,
                sealed_before_lsn: 1,
            })
            .is_err()
    );
    let state = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(state.state, SegmentFileState::Sealing);
}

#[tokio::test]
async fn rollover_switches_active_segment_and_seal_worker_seals_old_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    cfg.seal_worker_count = 2;
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_1, b"payload-a").unwrap();
    store.put(&key_2, b"payload-b").unwrap();

    assert_eq!(
        store
            .index()
            .get_blob_entry(&key_1)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap()
            .segment_id,
        1
    );
    assert_eq!(
        store
            .index()
            .get_blob_entry(&key_2)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap()
            .segment_id,
        2
    );
    assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));

    let sealed = wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
    assert_eq!(sealed.durable_offset, sealed.write_offset);
    assert_eq!(sealed.sealed_len, Some(sealed.write_offset));
    assert_eq!(sealed.sealed_sha256, None);
    assert_eq!(store.durable_lsn().unwrap(), 1);

    let open = store.index().get_segment_state(2).unwrap().unwrap();
    assert_eq!(open.state, SegmentFileState::Open);
    assert_eq!(open.sealed_sha256, None);
    let open_segment_ids = store
        .index()
        .iter_segment_states()
        .unwrap()
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && state.state == SegmentFileState::Open
        })
        .map(|(segment_id, _)| segment_id)
        .collect::<Vec<_>>();
    assert_eq!(open_segment_ids, vec![2]);

    store.sync().unwrap();
    assert_eq!(store.durable_lsn().unwrap(), 2);
}

#[tokio::test]
async fn timed_checkpoint_rolls_empty_segment_for_metadata_only_lsn() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    ensure_epoch_initialized(&index, cfg.starting_epoch).unwrap();
    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 2).unwrap();
    batch.write().unwrap();

    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let gc_concurrency = Arc::new(GcConcurrencyController::new(
        GcConcurrencyConfig::from_store_config(&cfg),
        metrics.clone(),
    ));
    let active_writer = SegmentWriter::create(
        segment_path(&cfg, 1),
        1,
        PlacementClass::Ingest,
        cfg.segment_max_bytes,
    )
    .unwrap();
    let active_segment_state = active_segment_state(&cfg, INGEST_SEGMENT_OWNER, &active_writer, 0);
    let (seal_tx, seal_rx) = mpsc::channel();
    let (_write_tx, write_rx) = mpsc::sync_channel(1);

    let mut coordinator = WriteCoordinator {
        config: cfg.clone(),
        index: index.clone(),
        active_writer,
        active_accounting_delta_log: test_active_accounting_log(&cfg, 1),
        durability_publish_lock: Arc::new(Mutex::new(())),
        active_segment_state,
        durable_offset: 0,
        last_checkpoint_at: Instant::now() - DURABILITY_CHECKPOINT_INTERVAL,
        last_checkpoint_next_lsn: 1,
        pending_rollovers: Vec::new(),
        segment_ids: SegmentIdAllocator::new(2),
        seal_tx,
        accounting_tx: None,
        write_rx,
        ingest_owner: INGEST_SEGMENT_OWNER,
        reader_cache: Arc::new(SegmentReaderCache::new(cfg.segment_reader_cache_capacity)),
        gc_concurrency,
        store_halt: StoreHalt::default(),
        metrics,
    };

    coordinator.process_timed_checkpoint().unwrap();

    let old_state = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(old_state.state, SegmentFileState::Sealing);
    assert_eq!(old_state.write_offset, 0);
    assert_eq!(old_state.sealed_before_lsn, Some(2));
    let new_state = index.get_segment_state(2).unwrap().unwrap();
    assert_eq!(new_state.state, SegmentFileState::Open);
    let SealCommand::Seal(task) = seal_rx.recv_timeout(Duration::from_secs(1)).unwrap() else {
        panic!("expected seal command");
    };
    assert_eq!(task.segment_id, 1);
    assert_eq!(task.sealed_len, 0);
    assert_eq!(task.sealed_before_lsn, 2);
}

#[tokio::test]
async fn reopen_after_rollover_appends_to_highest_open_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key_1, b"payload-a").unwrap();
        store.put(&key_2, b"payload-b").unwrap();
        wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    store.put(&key_3, b"x").unwrap();

    assert_eq!(
        store
            .index()
            .get_blob_entry(&key_3)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap()
            .segment_id,
        2
    );
    assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&key_3).unwrap(), Some(b"x".to_vec()));
}
