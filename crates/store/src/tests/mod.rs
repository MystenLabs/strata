use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::Path,
    sync::{Arc, Mutex, Once, mpsc},
    thread,
    time::{Duration, Instant},
};

use core_types::{BlobLifecycle, EpochBucket, FIXED_RECORD_HEADER_LEN, SegmentGcRecordRange};
use gc_planner::{
    DestinationClass, GcAction, GcCopyRecord, GcPlanner, GcPlannerConfig, GcScenario,
};
use lsm::{TableWriter, encode_inline_value};
use prometheus::Registry;
use tempfile::tempdir;
use typed_store::{
    DBMetrics,
    rocks::{MetricConf, open_cf},
};

use super::*;
use crate::{
    blob_lsm::BlobMutation, maintenance::publish_blob_lsm_edit, relocation::RelocationEntry,
};

mod garbage_log;

static INIT_TYPED_STORE_METRICS: Once = Once::new();
const TEST_KEY_LEN: u64 = 6;
const TEST_PAYLOAD_LEN: u64 = 9;
const TEST_RECORD_LEN: u64 = FIXED_RECORD_HEADER_LEN as u64 + TEST_KEY_LEN + TEST_PAYLOAD_LEN;
const TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD: u64 = TEST_RECORD_LEN * 2 - 1;

#[test]
fn retired_projection_files_are_removed_without_following_other_paths() {
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let retired = cfg.namespace_dir().join(RETIRED_PROJECTION_DIR);
    std::fs::create_dir_all(&retired).unwrap();
    std::fs::write(retired.join("delta-000001.run"), b"obsolete").unwrap();

    cleanup_retired_projection_dir(&cfg).unwrap();

    assert!(!retired.exists());
}

fn open_test_index(path: impl AsRef<Path>, cf_prefix: impl AsRef<str>) -> StrataIndex {
    let path = path.as_ref();
    StrataIndex::open_path(path, cf_prefix, path.display().to_string()).unwrap()
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
fn gc_output_byte_attribution_includes_skipped_bytes_retained_in_used_output() {
    let survivor = gc_staged_record(10, 0);
    let skipped_record = gc_staged_record(30, 8);
    let skipped = vec![GcSkippedCopiedRecord {
        record: skipped_record,
        kind: GcSkippedCopiedRecordKind::Retired,
    }];

    let bytes = gc_output_bytes_by_source(&[survivor], &skipped, &BTreeSet::from([1])).unwrap();

    assert_eq!(bytes, BTreeMap::from([(7, 16)]));
}

fn segment_summary(index: &StrataIndex, segment_id: SegmentId) -> core_types::SegmentGcSummary {
    index
        .get_segment_gc_summary(segment_id)
        .unwrap()
        .unwrap_or_default()
}

fn segment_overlay(store: &StrataStore, segment_id: SegmentId) -> core_types::SegmentGcOverlay {
    store
        .index()
        .read_segment_garbage_overlay(store.config().namespace_dir(), segment_id)
        .unwrap()
        .unwrap_or_default()
}

fn lsm_blob(store: &StrataStore, shard: ShardKey, key: &BlobKey) -> ResolvedBlobVersion {
    resolve_blob_version(store, shard, key)
        .unwrap()
        .expect("blob must be visible in the LSM")
}

fn lsm_blob_ref(store: &StrataStore, key: &BlobKey) -> RecordRef {
    lsm_blob(store, STANDALONE_SHARD, key).record_ref
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
        self.store.tombstone(STANDALONE_SHARD.id, key)
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

#[tokio::test]
async fn payload_updates_cannot_reuse_an_old_relocation() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob".to_vec()).unwrap();
    let store = try_open_standalone_store(
        config(dir.path(), "relocation-cache"),
        StrataStoreMetrics::default(),
    )
    .unwrap();

    let first_lsn = store.put(&key, b"first").unwrap();
    store.relocation_cache.insert(
        key.clone(),
        STANDALONE_SHARD,
        first_lsn,
        RecordRef {
            segment_id: 10,
            offset: 20,
            len: 30,
        },
    );
    store.set_blob_lifetime(&key, 100).unwrap();
    assert_eq!(store.relocation_cache.len(), 1);

    let second_lsn = store.put(&key, b"second").unwrap();
    assert_eq!(store.relocation_cache.len(), 1);
    assert_eq!(
        store
            .relocation_cache
            .get(&key, STANDALONE_SHARD, second_lsn),
        None
    );
    store.tombstone(&key).unwrap();
    assert_eq!(store.relocation_cache.len(), 1);
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
        segment_reader_cache_capacity: 16,
        lsm_partition_count: DEFAULT_LSM_PARTITION_COUNT,
        recovery_policy: StrataRecoveryPolicy::PointInTime,
        sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
        gc_workers_enabled: true,
        gc_interval: Duration::from_secs(3600),
        gc_worker_count: DEFAULT_GC_WORKER_COUNT,
        gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
        gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
        gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
        gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
        gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
        gc_planner_config: GcPlannerConfig::default(),
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

fn counter_value_with_labels(registry: &Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family.get_metric().iter().find_map(|metric| {
                labels
                    .iter()
                    .all(|(name, value)| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == *name && label.value() == *value)
                    })
                    .then(|| metric.get_counter().value())
            })
        })
        .unwrap_or_else(|| panic!("missing counter metric {name} with labels {labels:?}"))
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

fn histogram_sample_count_with_labels(
    registry: &Registry,
    name: &str,
    labels: &[(&str, &str)],
) -> u64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family.get_metric().iter().find_map(|metric| {
                labels
                    .iter()
                    .all(|(name, value)| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == *name && label.value() == *value)
                    })
                    .then(|| metric.get_histogram().sample_count())
            })
        })
        .unwrap_or_else(|| panic!("missing histogram metric {name} with labels {labels:?}"))
}

fn histogram_sample_sum(registry: &Registry, name: &str) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_histogram().sample_sum())
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
fn wait_for_lsm_gc(store: &StrataStore, expected_lsn: StrataLsn) {
    let started = Instant::now();
    loop {
        let manifest = store.lsm().unwrap().manifest();
        let head = store
            .index()
            .get_garbage_log_position(GARBAGE_LOG_HEAD)
            .unwrap()
            .unwrap_or_default();
        let swept = store
            .index()
            .get_garbage_log_position(GARBAGE_LOG_SWEEP_CURSOR)
            .unwrap()
            .unwrap_or_default();
        let materialized = manifest
            .materialized_through
            .is_some_and(|lsn| lsn >= expected_lsn);
        let patch_count = manifest
            .partitions
            .values()
            .map(|partition| partition.patches.len())
            .sum::<usize>();
        let empty_output_settled = started.elapsed() >= Duration::from_secs(2);
        if store.published_lsn().unwrap() >= expected_lsn
            && (materialized || empty_output_settled)
            && patch_count == 0
            && swept == head
        {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for LSM GC materialization through {expected_lsn}; manifest frontier was {:?}, patch count was {}, head was {:?}, swept was {:?}",
            manifest.materialized_through,
            patch_count,
            head,
            swept,
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_expiry_accounting(store: &StrataStore, expected_lsn: StrataLsn) {
    let started = Instant::now();
    loop {
        let accounted = store
            .index()
            .get_blob_expiry_accounted_lsn()
            .unwrap()
            .unwrap_or_default();
        if accounted >= expected_lsn {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for expiry accounting through LSN {expected_lsn}; current frontier was {accounted}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_shard_cleanup(store: &StrataStore, shard: ShardKey) {
    let started = Instant::now();
    loop {
        match store.index().get_shard_cleanup_job(shard).unwrap() {
            None => return,
            Some(job) if job.state == ShardCleanupState::ShardOwnedReclaimed => return,
            Some(_) => {}
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

fn seal_first_segment(config: &StrataStoreConfig) -> SegmentState {
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let store = try_open_standalone_store(config.clone(), StrataStoreMetrics::default()).unwrap();

    store.put(&key_1, b"payload-a").unwrap();
    store.put(&key_2, b"payload-b").unwrap();
    store.sync().unwrap();

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
async fn store_wal_routes_payload_and_metadata_without_fake_lsm_rows() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "lsm-write-path");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
    let lsm = store.lsm.upgrade().unwrap();

    assert_eq!(store.put(&key, b"payload").unwrap(), 1);
    assert_eq!(lsm.last_lsn().unwrap(), Some(1));

    // Epoch LSN 2 belongs in the store WAL and RocksDB, not in the blob LSM. The gap is
    // intentional: the next blob mutation keeps its global store LSN instead of being renumbered.
    assert_eq!(store.increment_epoch().unwrap(), (43, 2));
    assert_eq!(lsm.last_lsn().unwrap(), Some(1));

    assert_eq!(store.tombstone(&key).unwrap(), 3);
    assert_eq!(lsm.last_lsn().unwrap(), Some(3));

    // A shard drop is the same kind of RocksDB-only projection: it consumes global LSN 4 and is
    // recoverable from the store WAL, but it does not manufacture a blob key.
    store.drop_shard(STANDALONE_SHARD.id).unwrap();
    assert_eq!(store.index().get_next_lsn().unwrap(), 5);
    assert_eq!(lsm.last_lsn().unwrap(), Some(3));
    store.sync().unwrap();

    let wal = Wal::path(cfg.namespace_dir().join("wal"), 1);
    assert_eq!(
        wal.extension().and_then(|extension| extension.to_str()),
        Some("log")
    );
    assert!(fs::metadata(wal).unwrap().len() > 12);
}

#[tokio::test]
async fn store_wal_recovery_uses_the_blob_projection_frontier() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "store-wal-frontier");
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    load_blob_lsm_manifest(&cfg, &index).unwrap();
    load_relocation_lsm_manifest(&cfg, &index).unwrap();

    let mut batch = index.batch();
    index
        .merge_lsm_manifest_batch(
            &mut batch,
            BLOB_LSM_MANIFEST,
            &ManifestEdit {
                remove: Vec::new(),
                add_base: Vec::new(),
                add_patches: Vec::new(),
                materialized_through: Some(100),
                wal_retained_from: None,
            },
        )
        .unwrap();
    index
        .merge_lsm_manifest_batch(
            &mut batch,
            RELOCATION_LSM_MANIFEST,
            &ManifestEdit {
                remove: Vec::new(),
                add_base: Vec::new(),
                add_patches: Vec::new(),
                materialized_through: Some(80),
                wal_retained_from: None,
            },
        )
        .unwrap();
    index
        .put_store_wal_retained_from_batch(&mut batch, 3)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        store_wal_recovery_state(&cfg, &index).unwrap(),
        (Some(100), 3)
    );
}

#[tokio::test]
async fn configured_partition_count_is_shared_by_both_lsms() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "configured-lsm-partitions");
    cfg.lsm_partition_count = 4;
    let keys = (0..cfg.lsm_partition_count)
        .map(|partition| {
            (0..)
                .map(|candidate| {
                    BlobKey::new(
                        format!("partition-{partition}-candidate-{candidate}").into_bytes(),
                    )
                    .unwrap()
                })
                .find(|key| {
                    crate::partition::partition_for_key(key.as_bytes(), cfg.lsm_partition_count)
                        == partition
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
    assert_eq!(
        store.lsm().unwrap().manifest().partition_count,
        cfg.lsm_partition_count
    );
    assert_eq!(
        store.relocations.lsm().manifest().partition_count,
        cfg.lsm_partition_count
    );
    let mut last_lsn = 0;
    for (partition, key) in keys.iter().enumerate() {
        last_lsn = store.put(key, &[partition as u8]).unwrap();
    }
    store.sync().unwrap();
    let started = Instant::now();
    loop {
        let manifest = store.lsm().unwrap().manifest();
        let every_partition_flushed = manifest
            .partitions
            .values()
            .all(|partition| !partition.base.is_empty() || !partition.patches.is_empty());
        if manifest
            .materialized_through
            .is_some_and(|frontier| frontier >= last_lsn)
            && every_partition_flushed
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timed out waiting for every configured LSM partition to flush"
        );
        thread::sleep(Duration::from_millis(10));
    }
    drop(store);

    let reopened = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
    for (partition, key) in keys.iter().enumerate() {
        assert_eq!(reopened.get(key).unwrap(), Some(vec![partition as u8]));
    }
    drop(reopened);

    cfg.lsm_partition_count = 2;
    let error = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(error, Error::InvariantViolation { .. }));
}

#[tokio::test]
async fn synced_store_checkpoint_reopens_the_existing_wal_prefix() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "lsm-checkpoint");
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

    let first_checkpoint = {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key_a, b"one").unwrap(), 1);
        store.sync().unwrap();
        assert_eq!(store.index().get_committed_lsn().unwrap(), 1);
        store.index().get_store_checkpoint().unwrap().unwrap()
    };
    assert_eq!(first_checkpoint.active_segment_id, FIRST_SEGMENT_ID);
    assert!(first_checkpoint.wal_position.offset > 12);

    let second_checkpoint = {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(
            store.index().get_store_checkpoint().unwrap(),
            Some(first_checkpoint)
        );
        assert_eq!(store.put(&key_b, b"two").unwrap(), 2);
        store.sync().unwrap();
        assert_eq!(store.index().get_committed_lsn().unwrap(), 2);
        store.index().get_store_checkpoint().unwrap().unwrap()
    };

    assert_eq!(
        second_checkpoint.wal_position.log_id,
        first_checkpoint.wal_position.log_id
    );
    assert!(
        second_checkpoint.wal_position.offset > first_checkpoint.wal_position.offset,
        "reopen must append after the persisted WAL prefix"
    );
    assert_eq!(
        fs::metadata(Wal::path(
            cfg.namespace_dir().join("wal"),
            second_checkpoint.wal_position.log_id,
        ))
        .unwrap()
        .len(),
        second_checkpoint.wal_position.offset
    );
}

#[tokio::test]
async fn relocation_compaction_reclaims_dead_destinations_and_reopens_current_bytes() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "relocation-reclaim");
    cfg.gc_workers_enabled = false;
    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "relocation-reclaim").unwrap();
    let live_key = BlobKey::new(b"blob-live".to_vec()).unwrap();
    let dead_key = BlobKey::new(b"blob-dead".to_vec()).unwrap();
    let live_payload = b"payload-live";
    let mut store = try_open_standalone_store(cfg.clone(), metrics.clone()).unwrap();

    store.put(&live_key, live_payload).unwrap();
    store.put(&dead_key, b"payload-dead").unwrap();
    let live_version = lsm_blob(&store, STANDALONE_SHARD, &live_key);
    let dead_version = lsm_blob(&store, STANDALONE_SHARD, &dead_key);
    let tombstone_lsn = store.tombstone(&dead_key).unwrap();
    store.sync().unwrap();
    let lsm = store.lsm().unwrap();
    store
        .store
        .write_tx
        .take()
        .unwrap()
        .send(WriteCommand::Shutdown)
        .unwrap();
    store.store.writer_handle.take().unwrap().join().unwrap();
    store.store.wal_reclaim_tx.take();
    store
        .store
        .wal_reclaim_handle
        .take()
        .unwrap()
        .join()
        .unwrap();
    store.store.lsm_flush_tx.take();
    store.store.lsm_flush_handle.take().unwrap().join().unwrap();
    store.store.lsm_compact_tx.take();
    store
        .store
        .lsm_compact_handle
        .take()
        .unwrap()
        .join()
        .unwrap();
    // The production GC path writes these relocation records through WriteCoordinator. This test
    // drives compaction directly, so keep a store WAL open and mirror that one production step.
    // It is deliberately the *store* WAL; the relocation LSM has no private log.
    let checkpoint = store.index().get_store_checkpoint().unwrap();
    let (mut store_wal, _, _, store_wal_sync_handles) = open_store_wal(
        store.config(),
        store.index(),
        store.index().get_next_lsn().unwrap(),
        checkpoint,
    )
    .unwrap();

    let segment_b = 99;
    let segment_c = 100;
    let c_path = layout::retention_segment_path(
        store.config(),
        STANDALONE_SHARD,
        PlacementClass::Spillover,
        segment_c,
    );
    std::fs::create_dir_all(c_path.parent().unwrap()).unwrap();
    let mut c_writer = SegmentWriter::create(
        &c_path,
        segment_c,
        PlacementClass::Spillover,
        store.config().segment_max_bytes,
    )
    .unwrap();
    let ref_c = c_writer
        .append_for_shard(
            &live_key,
            live_version.payload_lsn,
            STANDALONE_SHARD,
            live_payload,
        )
        .unwrap()
        .record_ref;
    let c_len = c_writer.seal().unwrap();
    assert_eq!(ref_c.len, live_version.record_ref.len);

    let ref_b_live = RecordRef {
        segment_id: segment_b,
        offset: 0,
        len: live_version.record_ref.len,
    };
    let ref_b_dead = RecordRef {
        segment_id: segment_b,
        offset: ref_b_live.len,
        len: dead_version.record_ref.len,
    };
    let first_relocations = [
        RelocationEntry {
            key: live_key.clone(),
            shard: STANDALONE_SHARD,
            payload_lsn: live_version.payload_lsn,
            publish_lsn: tombstone_lsn + 1,
            to: ref_b_live,
        },
        RelocationEntry {
            key: dead_key.clone(),
            shard: STANDALONE_SHARD,
            payload_lsn: dead_version.payload_lsn,
            publish_lsn: tombstone_lsn + 2,
            to: ref_b_dead,
        },
    ];
    store_wal
        .append(
            &first_relocations
                .iter()
                .map(|entry| WalEntry {
                    lsn: entry.publish_lsn,
                    payload: StoreWalMutation::Relocation(entry.clone())
                        .encode()
                        .unwrap(),
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    store
        .relocations
        .write_batch(0, &first_relocations)
        .unwrap();
    let first_wal_position = store_wal.sync().unwrap();
    let mut first_store_checkpoint = store.index().get_store_checkpoint().unwrap().unwrap();
    first_store_checkpoint.wal_position = first_wal_position;
    let mut segment_b_state = SegmentState {
        owner: SegmentOwner::Shard(STANDALONE_SHARD),
        segment_id: segment_b,
        volume_id: 0,
        path: relative_segment_path(
            store.config(),
            layout::retention_segment_path(
                store.config(),
                STANDALONE_SHARD,
                PlacementClass::Spillover,
                segment_b,
            ),
        ),
        placement_class: PlacementClass::Spillover,
        state: SegmentFileState::Sealed,
        write_offset: ref_b_live.len + ref_b_dead.len,
        durable_offset: ref_b_live.len + ref_b_dead.len,
        min_lsn: Some(tombstone_lsn + 1),
        max_lsn: Some(tombstone_lsn + 2),
        sealed_before_lsn: None,
        sealed_len: Some(ref_b_live.len + ref_b_dead.len),
        sealed_sha256: None,
    };
    let segment_c_state = SegmentState {
        owner: SegmentOwner::Shard(STANDALONE_SHARD),
        segment_id: segment_c,
        volume_id: 0,
        path: relative_segment_path(store.config(), c_path),
        placement_class: PlacementClass::Spillover,
        state: SegmentFileState::Sealed,
        write_offset: c_len,
        durable_offset: c_len,
        min_lsn: Some(tombstone_lsn + 3),
        max_lsn: Some(tombstone_lsn + 3),
        sealed_before_lsn: None,
        sealed_len: Some(c_len),
        sealed_sha256: None,
    };
    let mut batch = store.index().batch();
    store
        .index()
        .put_segment_state_batch(&mut batch, &segment_b_state)
        .unwrap();
    store
        .index()
        .put_commit_lsn_batch(&mut batch, tombstone_lsn + 2)
        .unwrap();
    store
        .index()
        .put_store_checkpoint_batch(&mut batch, first_store_checkpoint)
        .unwrap();
    store
        .index()
        .put_next_lsn_batch(&mut batch, tombstone_lsn + 3)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    thread::sleep(LSM_MEMTABLE_MAX_AGE + Duration::from_millis(100));
    assert!(
        store
            .relocations
            .lsm()
            .roll_memtable_if_due(0)
            .unwrap()
            .is_some()
    );
    flush_relocation_lsm(
        store.index(),
        &store.relocations,
        &store.relocation_cache,
        &metrics,
    )
    .unwrap();
    compact_relocation_lsm(
        store.index(),
        &store.relocations,
        &store.relocation_cache,
        &metrics,
    )
    .unwrap();
    assert_eq!(
        store
            .relocations
            .lookup(&live_key, STANDALONE_SHARD, live_version.payload_lsn)
            .unwrap()
            .unwrap()
            .to,
        ref_b_live
    );
    assert!(
        store
            .relocations
            .lookup(&dead_key, STANDALONE_SHARD, dead_version.payload_lsn)
            .unwrap()
            .is_some()
    );

    let final_relocation = RelocationEntry {
        key: live_key.clone(),
        shard: STANDALONE_SHARD,
        payload_lsn: live_version.payload_lsn,
        publish_lsn: tombstone_lsn + 3,
        to: ref_c,
    };
    store_wal
        .append(&[WalEntry {
            lsn: final_relocation.publish_lsn,
            payload: StoreWalMutation::Relocation(final_relocation.clone())
                .encode()
                .unwrap(),
        }])
        .unwrap();
    store
        .relocations
        .write_batch(0, &[final_relocation])
        .unwrap();
    let final_wal_position = store_wal.sync().unwrap();
    let mut final_store_checkpoint = store.index().get_store_checkpoint().unwrap().unwrap();
    final_store_checkpoint.wal_position = final_wal_position;
    let mut source_state = store
        .index()
        .get_segment_state(live_version.record_ref.segment_id)
        .unwrap()
        .unwrap();
    source_state.state = SegmentFileState::Deleted;
    segment_b_state.state = SegmentFileState::Deleted;
    let mut batch = store.index().batch();
    store
        .index()
        .put_segment_state_batch(&mut batch, &source_state)
        .unwrap();
    store
        .index()
        .put_segment_state_batch(&mut batch, &segment_b_state)
        .unwrap();
    store
        .index()
        .put_segment_state_batch(&mut batch, &segment_c_state)
        .unwrap();
    store
        .index()
        .put_commit_lsn_batch(&mut batch, tombstone_lsn + 3)
        .unwrap();
    store
        .index()
        .put_store_checkpoint_batch(&mut batch, final_store_checkpoint)
        .unwrap();
    store
        .index()
        .put_next_lsn_batch(&mut batch, tombstone_lsn + 4)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    assert_eq!(lsm_blob_ref(&store, &live_key), live_version.record_ref);
    assert_eq!(store.get(&live_key).unwrap(), Some(live_payload.to_vec()));
    store.relocation_cache.insert(
        dead_key.clone(),
        STANDALONE_SHARD,
        dead_version.payload_lsn,
        ref_b_dead,
    );

    thread::sleep(LSM_MEMTABLE_MAX_AGE + Duration::from_millis(100));
    assert!(
        store
            .relocations
            .lsm()
            .roll_memtable_if_due(0)
            .unwrap()
            .is_some()
    );
    flush_relocation_lsm(
        store.index(),
        &store.relocations,
        &store.relocation_cache,
        &metrics,
    )
    .unwrap();
    assert_eq!(
        store.relocations.lsm().manifest().partitions[&0]
            .patches
            .len(),
        1
    );
    compact_relocation_lsm(
        store.index(),
        &store.relocations,
        &store.relocation_cache,
        &metrics,
    )
    .unwrap();

    let manifest = store.relocations.lsm().manifest();
    assert!(manifest.partitions[&0].patches.is_empty());
    assert_eq!(manifest.partitions[&0].base.len(), 1);
    assert_eq!(
        store
            .relocations
            .lookup(&live_key, STANDALONE_SHARD, live_version.payload_lsn)
            .unwrap()
            .unwrap()
            .to,
        ref_c
    );
    assert!(
        store
            .relocations
            .lookup(&dead_key, STANDALONE_SHARD, dead_version.payload_lsn)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .relocation_cache
            .get(&dead_key, STANDALONE_SHARD, dead_version.payload_lsn),
        None
    );
    assert_eq!(
        store
            .relocation_cache
            .get(&live_key, STANDALONE_SHARD, live_version.payload_lsn),
        Some(ref_c)
    );
    assert_eq!(lsm_blob_ref(&store, &live_key), live_version.record_ref);
    assert_eq!(store.get(&live_key).unwrap(), Some(live_payload.to_vec()));
    assert_eq!(
        counter_value_with_labels(
            &registry,
            "strata_store_relocation_lookups_total",
            &[("result", "hit")]
        ),
        1.0
    );
    assert_eq!(
        histogram_sample_count(&registry, "strata_store_relocation_lookup_duration_seconds"),
        1
    );
    assert_eq!(
        counter_value_with_labels(
            &registry,
            "strata_store_relocation_cache_requests_total",
            &[("result", "hit")]
        ),
        1.0
    );
    assert_eq!(
        counter_value_with_labels(
            &registry,
            "strata_store_relocation_cache_requests_total",
            &[("result", "miss")]
        ),
        1.0
    );
    assert_eq!(
        counter_value(
            &registry,
            "strata_store_relocation_compaction_entries_examined_total"
        ),
        4.0
    );
    assert_eq!(
        counter_value(
            &registry,
            "strata_store_relocation_compaction_entries_dropped_total"
        ),
        1.0
    );
    assert!(
        counter_value(
            &registry,
            "strata_store_relocation_compaction_input_bytes_total"
        ) > 0.0
    );
    assert!(
        counter_value(
            &registry,
            "strata_store_relocation_compaction_output_bytes_total"
        ) > 0.0
    );

    store_wal.sync().unwrap();
    drop(store_wal);
    for handle in store_wal_sync_handles {
        handle.join().unwrap();
    }
    drop(lsm);
    drop(store);
    let reopened = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(lsm_blob_ref(&reopened, &live_key), live_version.record_ref);
    assert_eq!(
        reopened.get(&live_key).unwrap(),
        Some(live_payload.to_vec())
    );
    assert_eq!(
        reopened
            .relocations
            .lookup(&live_key, STANDALONE_SHARD, live_version.payload_lsn)
            .unwrap()
            .unwrap()
            .to,
        ref_c
    );
    assert!(
        reopened
            .relocations
            .lookup(&dead_key, STANDALONE_SHARD, dead_version.payload_lsn)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn recovered_store_tail_promotes_the_matching_wal_tail() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "lsm-replay-base");
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();

    let (first_checkpoint, ref_a, ref_b) = {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key_a, b"one").unwrap(), 1);
        let ref_a = lsm_blob_ref(&store, &key_a);
        store.sync().unwrap();
        let checkpoint = store.index().get_store_checkpoint().unwrap().unwrap();
        assert_eq!(store.put(&key_b, b"two").unwrap(), 2);
        let ref_b = lsm_blob_ref(&store, &key_b);
        (checkpoint, ref_a, ref_b)
    };

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.get(&key_b).unwrap(), Some(b"two".to_vec()));
    let summary = store
        .index()
        .get_segment_gc_summary(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(summary.total_bytes, ref_a.len + ref_b.len);
    assert_eq!(summary.live_bytes, ref_a.len + ref_b.len);
    assert_eq!(summary.live_ref_count, 2);
    store.sync().unwrap();
    let recovered = store.index().get_store_checkpoint().unwrap().unwrap();
    assert_eq!(store.index().get_committed_lsn().unwrap(), 2);
    assert!(recovered.wal_position.offset > first_checkpoint.wal_position.offset);

    assert_eq!(store.put(&key_c, b"three").unwrap(), 3);
    store.sync().unwrap();
    let advanced = store.index().get_store_checkpoint().unwrap().unwrap();
    assert_eq!(store.index().get_committed_lsn().unwrap(), 3);
    assert!(advanced.wal_position.offset > recovered.wal_position.offset);
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

    store.put(shard.id, &key, b"hello shard").unwrap();
    let record_ref = lsm_blob(&store, shard, &key).record_ref;
    let mut reader = segment::SegmentReader::open(
        segment_path(&cfg, record_ref.segment_id),
        record_ref.segment_id,
    )
    .unwrap();

    let metadata = reader.read_record_metadata(record_ref).unwrap();

    assert_eq!(metadata.header.shard, shard);
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
async fn tombstone_only_hides_the_target_shard_generation() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store =
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap();
    store.add_shard(10).unwrap();
    store.add_shard(20).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    store.put(10, &key, b"primary-old").unwrap();
    store.put(20, &key, b"secondary-old").unwrap();
    let tombstone_lsn = store.tombstone(10, &key).unwrap();
    let resurrect_lsn = store.put(10, &key, b"primary-new").unwrap();

    assert!(resurrect_lsn > tombstone_lsn);
    assert_eq!(
        store.get_from_shard(10, &key).unwrap(),
        Some(b"primary-new".to_vec())
    );
    assert_eq!(
        store.get_from_shard(20, &key).unwrap(),
        Some(b"secondary-old".to_vec())
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
        .tombstone(20, key_b.clone());
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
        lsm_blob(
            &store,
            ShardKey {
                id: 10,
                generation: 0,
            },
            &key_a,
        )
        .lifecycle
        .unwrap()
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

    assert_eq!(store.published_lsn().unwrap(), 3);
}
#[tokio::test]
async fn epoch_change_is_published_by_the_store_checkpoint() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"semantic-materialization".to_vec()).unwrap();
    let cfg = config(dir.path(), "default");
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key, b"payload").unwrap();
    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();

    assert_eq!(store.published_lsn().unwrap(), epoch_lsn);
}
#[tokio::test]
async fn sync_publishes_new_allocations_without_overwriting_garbage() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "allocation-baseline");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let key_a = BlobKey::new(b"allocation-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"allocation-b".to_vec()).unwrap();

    store.put(&key_a, b"one").unwrap();
    let ref_a = lsm_blob_ref(&store, &key_a);
    assert!(
        store
            .index()
            .get_segment_gc_summary(ref_a.segment_id)
            .unwrap()
            .is_none()
    );
    store.sync().unwrap();

    let mut summary = store
        .index()
        .get_segment_gc_summary(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(summary.total_bytes, ref_a.len);
    assert_eq!(summary.live_bytes, ref_a.len);
    assert_eq!(summary.live_ref_count, 1);
    summary.live_bytes = 0;
    summary.retired_bytes = ref_a.len;
    summary.live_ref_count = 0;
    summary.unknown_lifetime_bytes = 0;
    summary.unknown_lifetime_ref_count = 0;
    let mut batch = store.index().batch();
    store
        .index()
        .put_segment_gc_summary_batch(&mut batch, ref_a.segment_id, &summary)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    store.put(&key_b, b"two").unwrap();
    let ref_b = lsm_blob_ref(&store, &key_b);
    assert_eq!(ref_b.segment_id, ref_a.segment_id);
    store.sync().unwrap();

    let summary = store
        .index()
        .get_segment_gc_summary(ref_a.segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(summary.total_bytes, ref_a.len + ref_b.len);
    assert_eq!(summary.live_bytes, ref_b.len);
    assert_eq!(summary.retired_bytes, ref_a.len);
    assert_eq!(summary.live_ref_count, 1);
    assert_eq!(summary.unknown_lifetime_bytes, ref_b.len);
    assert_eq!(summary.unknown_lifetime_ref_count, 1);
}

#[tokio::test]
async fn foreground_sync_does_not_wait_for_garbage_publication() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let store = Arc::new(
        StrataStore::open(config(dir.path(), "default"), StrataStoreMetrics::default()).unwrap(),
    );
    let garbage_lock = Arc::clone(&store.garbage_publish_lock);
    let garbage_guard = garbage_lock.lock().unwrap();

    store
        .put(
            STANDALONE_SHARD.id,
            &BlobKey::new(b"foreground-sync".to_vec()).unwrap(),
            b"payload",
        )
        .unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let sync_store = Arc::clone(&store);
    let handle = thread::spawn(move || result_tx.send(sync_store.sync()).unwrap());

    let result = result_rx.recv_timeout(Duration::from_secs(2));
    drop(garbage_guard);
    handle.join().unwrap();
    result
        .expect("foreground sync waited for garbage publication")
        .unwrap();
}

#[tokio::test]
async fn shard_drop_is_visible_immediately_and_durable_after_sync() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"shard-drop-materialization".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let previous_published = store.published_lsn().unwrap();
    store.put(&key, b"payload").unwrap();
    store.drop_shard(STANDALONE_SHARD.id).unwrap();
    let drop_lsn = store
        .index()
        .get_shard_cleanup_job(STANDALONE_SHARD)
        .unwrap()
        .unwrap()
        .drop_lsn;

    assert_eq!(store.published_lsn().unwrap(), previous_published);
    store.sync().unwrap();
    assert_eq!(store.published_lsn().unwrap(), drop_lsn);
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(STANDALONE_SHARD)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::ReadyForGc
    );
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
                state: core_types::ShardState::Dropped,
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
            assert_eq!(state, core_types::ShardState::Dropped);
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
            state: core_types::ShardState::Dropped,
        })
    );
    assert!(store.put(40, &key, b"dropped").is_err());
    assert!(store.get_from_shard(40, &key).is_err());
    let second_shard = store.add_shard(40).unwrap();
    assert_eq!(
        second_shard,
        ShardKey {
            id: 40,
            generation: 1
        }
    );

    assert_eq!(store.get_from_shard(40, &key).unwrap(), None);
    let second_lsn = store.put(40, &key, b"new generation").unwrap();
    assert_eq!(
        store.get_from_shard(40, &key).unwrap(),
        Some(b"new generation".to_vec())
    );
    assert_eq!(lsm_blob(&store, second_shard, &key).head_lsn, second_lsn);
}

#[tokio::test]
async fn drop_shard_retires_mixed_ingest_bytes_without_tombstones() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let shard = store.add_shard(41).unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();

    let put_lsn = store.store.put(41, &key, b"old generation").unwrap();
    let record_ref = lsm_blob(&store, shard, &key).record_ref;
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
    assert_eq!(store.index().get_next_lsn().unwrap(), drop_lsn + 1);
    assert!(store.index().get_committed_lsn().unwrap() < drop_lsn);
    store.sync().unwrap();
    assert_eq!(store.index().get_committed_lsn().unwrap(), drop_lsn);
    let job = store.index().get_shard_cleanup_job(shard).unwrap().unwrap();
    assert_eq!(job.drop_lsn, drop_lsn);
    assert_eq!(job.state, ShardCleanupState::ReadyForGc);

    // A drop does not scan mixed ingest state. Touching the same LSM key through another active
    // shard makes ordinary compaction discover and retire the dropped generation.
    let live_shard = store.add_shard(42).unwrap();
    let touch_lsn = store
        .store
        .put(live_shard.id, &key, b"live generation")
        .unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store.store, touch_lsn);
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::ReadyForGc
    );
    let authoritative = segment_overlay(&store, record_ref.segment_id);
    assert_eq!(authoritative.retired, vec![gc_range(record_ref)]);
    assert_eq!(authoritative.summary.live_ref_count, 1);
    assert_eq!(authoritative.summary.retired_bytes, record_ref.len);
    assert_eq!(
        store
            .store
            .gc_executor()
            .unwrap()
            .cleanup_ready_shard_generations()
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::ShardOwnedReclaimed
    );
}

#[tokio::test]
async fn drop_shard_does_not_wait_for_gc_claims() {
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
        .expect("drop_shard blocked on a GC claim")
        .unwrap();
    handle.join().unwrap();
    assert_eq!(
        store
            .index()
            .get_shard_cleanup_job(shard)
            .unwrap()
            .unwrap()
            .state,
        ShardCleanupState::ReadyForGc
    );
}

#[tokio::test]
async fn reopen_finishes_durable_shard_drop_cleanup() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_interval = Duration::from_secs(3600);
    let shard;
    let retention_segment_id = 99;
    let retention_path;

    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        shard = store.add_shard(42).unwrap();
        let key = BlobKey::new(b"crashed-drop".to_vec()).unwrap();
        store.store.put(shard.id, &key, b"ingest payload").unwrap();
        store.sync().unwrap();

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
            ShardCleanupState::ReadyForGc
        );
    }

    let reopened = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while reopened
        .index()
        .get_shard_cleanup_job(shard)
        .unwrap()
        .is_some_and(|job| job.state != ShardCleanupState::ShardOwnedReclaimed)
        && Instant::now() < deadline
    {
        reopened.store.request_gc().unwrap();
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        !retention_path.exists(),
        "job={:?} published_lsn={}",
        reopened.index().get_shard_cleanup_job(shard).unwrap(),
        reopened.index().get_committed_lsn().unwrap(),
    );
    let state = reopened
        .index()
        .get_segment_state(retention_segment_id)
        .unwrap()
        .unwrap();
    assert_eq!(state.state, SegmentFileState::Deleted);
    assert_eq!(state.owner, SegmentOwner::Shard(shard));
    assert!(
        reopened
            .get_from_shard(shard.id, &BlobKey::new(b"crashed-drop".to_vec()).unwrap())
            .is_err()
    );
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
    let ingest_segment_id = lsm_blob_ref(&store, &key).segment_id;
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
    assert_eq!(store.index().get_committed_lsn().unwrap(), 0);
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

    assert_eq!(store.published_lsn().unwrap(), 2);
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

    let record_ref = lsm_blob_ref(&store, &key);
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
        Error::Segment(segment::Error::Core(
            core_types::Error::RecordChecksumMismatch { .. }
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
    let old_ref = lsm_blob_ref(&store, &key);
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
                batch.write().unwrap();
                store
                    .relocations
                    .write_batch(
                        0,
                        &[RelocationEntry {
                            key: key.clone(),
                            shard: STANDALONE_SHARD,
                            payload_lsn,
                            publish_lsn: payload_lsn + 1,
                            to: new_ref,
                        }],
                    )
                    .unwrap();

                return Err(Error::Segment(segment::Error::Io {
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
        Error::Segment(segment::Error::InvalidPayloadRange { .. })
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

    assert_eq!(lsm_blob_ref(&store, &key).offset, 0);
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
    let record_ref = lsm_blob_ref(&store, &key);

    let tombstone_lsn = store.tombstone(&key).unwrap();

    assert_eq!(put_lsn, 1);
    assert_eq!(tombstone_lsn, 2);
    assert_eq!(store.get(&key).unwrap(), None);
    assert!(!store.contains(&key).unwrap());
    assert_eq!(record_ref.segment_id, FIRST_SEGMENT_ID);
    assert_eq!(store.published_lsn().unwrap(), 0);

    store.sync().unwrap();

    assert_eq!(store.published_lsn().unwrap(), 2);
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
        counter_value(&registry, "strata_store_segment_file_bytes_written_total"),
        counter_value(&registry, "strata_store_put_record_bytes_total")
    );
    assert!(
        counter_value(&registry, "strata_store_segment_file_bytes_read_total")
            > counter_value(&registry, "strata_store_get_payload_bytes_total")
    );
    assert_eq!(
        counter_value(&registry, "strata_store_delete_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_delete_errors_total"),
        0.0
    );
    assert_eq!(
        histogram_sample_count(&registry, "strata_store_delete_duration_seconds"),
        1
    );
    assert_eq!(
        counter_value(&registry, "strata_store_sync_calls_total"),
        1.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_sync_errors_total"),
        0.0
    );
    for phase in [
        "capture",
        "segment_files",
        "wal",
        "completion_queue",
        "relocation_lock",
        "metadata_build",
        "index_sync_commit",
        "state_update",
        "wal_reclaim",
        "unattributed",
    ] {
        assert_eq!(
            histogram_sample_count_with_labels(
                &registry,
                "strata_store_sync_phase_duration_seconds",
                &[("phase", phase)],
            ),
            1,
            "phase {phase}",
        );
    }
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
    assert_eq!(gauge_value(&registry, "strata_store_published_lsn"), 1);
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
    let sealing_path = segment_path(&cfg, 1);
    drop(
        SegmentWriter::create(
            &sealing_path,
            1,
            PlacementClass::Ingest,
            cfg.segment_max_bytes,
        )
        .unwrap(),
    );

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
    let (write_tx, write_rx) = mpsc::sync_channel(1);
    let (sync_done_tx, sync_done_rx) = mpsc::channel();
    let active_segment_state = active_segment_state(&cfg, INGEST_SEGMENT_OWNER, &active_writer, 0);
    let (wal, recovered, _relocation_recovery, _lsm_sync_handles) =
        open_store_wal(&cfg, &index, index.get_next_lsn().unwrap(), None).unwrap();
    let segment_sync_tx = wal.file_sync_sender();
    let lsm = open_lsm(&cfg, &index, index.get_next_lsn().unwrap(), recovered).unwrap();
    let (lsm_flush_tx, _lsm_flush_rx) = mpsc::channel();
    let (lsm_compact_tx, _lsm_compact_rx) = mpsc::channel();
    let (wal_reclaim_tx, _wal_reclaim_rx) = mpsc::sync_channel(1);
    let mut coordinator = WriteCoordinator {
        config: cfg.clone(),
        index: index.clone(),
        lsm,
        wal,
        segment: active_writer,
        segment_factory: SegmentFactory::new(
            cfg.ingest_dir(),
            SegmentIdAllocator::new(3),
            PlacementClass::Ingest,
            cfg.segment_max_bytes,
        ),
        segment_sync_tx,
        pending_segment_syncs: Vec::new(),
        internal_write_tx: write_tx,
        sync_done_tx,
        sync_done_rx,
        sync_and_commit_in_flight: None,
        pending_sync_requests: Vec::new(),
        active_segment_state,
        durable_offset: 0,
        active_allocation_records: 0,
        active_allocation_tracker: Arc::new(SegmentAllocationTracker::default()),
        pending_segment_bytes: 0,
        unsealed_segments: 2,
        oldest_uncommitted_at: None,
        last_committed_at: Instant::now(),
        pending_rollovers: Vec::new(),
        lsm_flush_tx,
        lsm_compact_tx,
        wal_reclaim_tx,
        write_rx,
        ingest_owner: INGEST_SEGMENT_OWNER,
        gc_concurrency,
        store_halt: StoreHalt::default(),
        metrics,
    };

    coordinator.pending_segment_syncs.push(SegmentSync {
        segment_id: 1,
        path: sealing_path,
        durable_offset: 0,
        sealed_before_lsn: Some(1),
        sealed_sha256: Arc::new(Mutex::new(None)),
        allocation_records: 0,
        allocation_tracker: Arc::new(SegmentAllocationTracker::default()),
    });
    coordinator.start_sync_and_commit(true).unwrap();

    coordinator.wait_for_seal_backlog_capacity().unwrap();

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

    let resolved = resolve_blob_version(&store, store.shard(), &key)
        .unwrap()
        .unwrap();
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
    let resolved = resolve_blob_version(&store, store.shard(), &key)
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
    let resolved = resolve_blob_version(&store, store.shard(), &key)
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
    let resolved = resolve_blob_version(&store, store.shard(), &key)
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
    let resolved = resolve_blob_version(&store, store.shard(), &key)
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
    assert_eq!(store.published_lsn().unwrap(), 0);
    store.sync().unwrap();

    let synced = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(synced.state, SegmentFileState::Open);
    assert_eq!(synced.durable_offset, unsynced.write_offset);
    assert_eq!(synced.write_offset, unsynced.write_offset);
    assert_eq!(store.published_lsn().unwrap(), 1);
    wait_for_lsm_gc(&store, 1);
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
    assert_eq!(store.published_lsn().unwrap(), 1);
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
    assert_eq!(lsm_blob(&store, store.shard(), &key).head_lsn, 1);
    assert_eq!(store.published_lsn().unwrap(), 1);
}

#[tokio::test]
async fn put_overwrite_materializes_the_latest_lsm_version() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let min_lsn = store.put(&key, b"payload-a").unwrap();
    let first_record_ref = lsm_blob_ref(&store, &key);
    let second_lsn = store.put(&key, b"payload-b").unwrap();
    let latest = lsm_blob(&store, store.shard(), &key);

    assert_ne!(min_lsn, second_lsn);
    assert_eq!(latest.head_lsn, second_lsn);
    assert_eq!(first_record_ref.segment_id, FIRST_SEGMENT_ID);
    assert_eq!(latest.record_ref.segment_id, FIRST_SEGMENT_ID);
}

#[tokio::test]
async fn main_compaction_does_not_wait_for_a_newer_overlapping_patch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let store = try_open_standalone_store(cfg, metrics).unwrap();
    let lsm = store.lsm().unwrap();

    let compaction_guard = store
        .compaction_admission_lock
        .write()
        .expect("compaction admission lock poisoned");
    let value = encode_inline_value(
        &BlobMutation::Tombstone {
            shard: STANDALONE_SHARD,
        }
        .encode_inline()
        .unwrap(),
    );
    let mut patches = Vec::new();
    for lsn in 1..=9 {
        let path = format!("test-patch-{lsn}.sst");
        let mut writer =
            TableWriter::create_patch(lsm.table_store().root(), path, lsn, 0, LSM_PATCH_FORMAT)
                .unwrap();
        writer.add_patch(b"same-key", lsn, &value).unwrap();
        patches.push(writer.finish().unwrap());
    }
    let published = publish_blob_lsm_edit(
        store.index(),
        &ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: patches,
            materialized_through: None,
            wal_retained_from: None,
        },
    )
    .unwrap();
    lsm.install_manifest(published).unwrap();

    let mut batch = store.index().batch();
    store.index().put_commit_lsn_batch(&mut batch, 8).unwrap();
    store.index().put_next_lsn_batch(&mut batch, 10).unwrap();
    batch.write_with_sync(true).unwrap();
    drop(compaction_guard);
    store.lsm_compact_tx.as_ref().unwrap().send(()).unwrap();

    let started = Instant::now();
    loop {
        let manifest = lsm.manifest();
        let old_patches_remain = manifest.partitions[&0]
            .patches
            .iter()
            .any(|patch| patch.relative_path == "test-patch-1.sst");
        if histogram_sample_count(&registry, "strata_store_main_compaction_duration_seconds") >= 1
            && !old_patches_remain
        {
            assert!(
                manifest.partitions[&0]
                    .patches
                    .iter()
                    .any(|patch| patch.relative_path == "test-patch-9.sst")
            );
            assert_eq!(
                gauge_value(&registry, "strata_store_main_minor_compaction_lsn"),
                8
            );
            assert_eq!(
                gauge_value(&registry, "strata_store_main_full_compaction_lsn"),
                0
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "durable patches were not compacted past the newer overlapping patch"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn blob_lsm_materializes_overwrite_and_lifetime() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let first_lsn = store.put(&key, b"payload-a").unwrap();
    let first_record_ref = lsm_blob_ref(&store, &key);
    let second_lsn = store.put(&key, b"payload-b").unwrap();
    let second_record_ref = lsm_blob_ref(&store, &key);
    let lifetime_lsn = store.extend(&key, 44).unwrap().unwrap();
    assert_eq!((first_lsn, second_lsn), (1, 2));

    let stats = segment_summary(store.index(), first_record_ref.segment_id);
    assert_eq!(stats, core_types::SegmentGcSummary::default());

    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_lsn);

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
}
#[tokio::test]
async fn blob_lsm_leaves_unknown_lifetime_put_copy_eligible() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    let put_lsn = store.put(&key, b"payload").unwrap();
    let record_ref = lsm_blob_ref(&store, &key);

    store.sync().unwrap();
    wait_for_lsm_gc(&store, put_lsn);

    let overlay = segment_overlay(&store, record_ref.segment_id);
    assert!(overlay.expired.is_empty());
    assert!(overlay.retired.is_empty());
    assert!(overlay.lifetimes.is_empty());
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
            .put_segment_gc_summary_batch(
                &mut batch,
                segment_id,
                &core_types::SegmentGcSummary {
                    total_bytes: bytes,
                    retired_bytes: bytes,
                    ..Default::default()
                },
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
async fn gc_prepare_plan_scans_real_segment_and_selects_live_records() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    cfg.lsm_partition_count = 4;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    for key in [&key_a, &key_b, &key_c] {
        store.set_blob_lifetime(key, 50).unwrap();
    }
    store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_c);

    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);
    wait_for_lsm_gc(&store, tombstone_lsn);

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
    assert_eq!(prepared.plan.copied_bytes, ref_b.len);

    assert_eq!(
        segment_overlay(&store, ref_a.segment_id).retired,
        vec![gc_range(ref_a)]
    );

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.plan.scenario, GcScenario::L0Compaction);
    assert_eq!(copied.outputs.len(), 1);
    assert_eq!(copied.copied_records.len(), 1);
    let output = &copied.outputs[0];
    assert_eq!(output.destination_class, DestinationClass::ExactEpoch(50));
    assert_eq!(output.placement_class, PlacementClass::ExactEpoch(50));
    assert_eq!(output.sealed_len, ref_b.len);
    assert!(output.path.exists());

    let copied_record = &copied.copied_records[0];
    assert_eq!(copied_record.source.key, key_b);
    assert_eq!(copied_record.source.payload_lsn, lsn_b);
    assert_eq!(copied_record.source.from, ref_b);
    assert_eq!(
        copied_record.source.destination_class,
        DestinationClass::ExactEpoch(50)
    );
    assert_eq!(copied_record.staged.segment_id, output.staged_segment_id);
    assert_eq!(copied_record.staged.offset, 0);
    assert_eq!(copied_record.staged.len, ref_b.len);

    let mut staged_reader =
        segment::SegmentReader::open(&output.path, output.staged_segment_id).unwrap();
    assert_eq!(
        staged_reader.read_payload(copied_record.staged).unwrap(),
        b"payload-b"
    );
}

#[tokio::test]
async fn gc_copy_uses_planning_snapshot_overlay_across_concurrent_retirement() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 4 - 1;
    cfg.gc_workers_enabled = false;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let key_d = BlobKey::new(b"blob-d".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    for key in [&key_a, &key_b, &key_c, &key_d] {
        store.set_blob_lifetime(key, 50).unwrap();
    }
    let lsn_a = store.put(&key_a, b"payload-a").unwrap();
    let lsn_b = store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    let lsn_d = store.put(&key_d, b"payload-d").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_d);

    let ref_b = lsm_blob_ref(&store, &key_b);
    let ref_c = lsm_blob_ref(&store, &key_c);
    assert_eq!(ref_b.segment_id, FIRST_SEGMENT_ID);
    assert_eq!(ref_c.segment_id, FIRST_SEGMENT_ID);

    let first_tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, first_tombstone_lsn);
    wait_for_lsm_gc(&store, first_tombstone_lsn);

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
    assert_eq!(prepared.plan.copied_bytes, ref_b.len + ref_c.len);

    // Materialize another tombstone after planning. The live overlay in RocksDB now differs from
    // the retained planning overlay, which previously caused CopyBytesMismatch during the scan.
    let second_tombstone_lsn = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, second_tombstone_lsn);
    assert_eq!(store.get(&key_b).unwrap(), None);

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.copied_records.len(), 2);
    assert_eq!(
        copied
            .copied_records
            .iter()
            .map(|record| record.source.key.clone())
            .collect::<Vec<_>>(),
        vec![key_b.clone(), key_c.clone()]
    );

    let published = store.publish_prepared_gc_copy(copied).unwrap();
    assert_eq!(published.skipped_records.len(), 1);
    assert_eq!(published.skipped_records[0].source.key, key_b);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(published.published_records[0].source.key, key_c.clone());
    assert_eq!(store.get(&key_b).unwrap(), None);
    assert_eq!(store.get(&key_c).unwrap(), Some(b"payload-c".to_vec()));

    // Keep the write LSN assertions explicit so the fixture cannot silently stop placing the
    // intended records before the planning frontier.
    assert!(lsn_a < lsn_b && lsn_b < lsn_c && lsn_c < lsn_d);
}

#[tokio::test]
async fn gc_copy_splits_mixed_ingest_records_into_shard_retention_segments() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let shard_a = store.add_shard(7).unwrap();
    let shard_b = store.add_shard(8).unwrap();

    for key in [&key_a, &key_b, &key_c] {
        store.set_blob_lifetime(key, 50).unwrap();
    }
    let lsn_a = store.store.put(shard_a.id, &key_a, b"payload-a").unwrap();
    let lsn_b = store.store.put(shard_b.id, &key_b, b"payload-b").unwrap();
    let lsn_c = store.store.put(shard_a.id, &key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_c);

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
        assert_eq!(output.destination_class, DestinationClass::ExactEpoch(50));
        assert_eq!(output.placement_class, PlacementClass::ExactEpoch(50));
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
            PlacementClass::ExactEpoch(50),
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
        assert_eq!(state.placement_class, PlacementClass::ExactEpoch(50));
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
    store.sync().unwrap();
    wait_for_shard_cleanup(&store.store, shard_a);

    assert!(!shard_a_dir.exists());
    assert!(shard_b_dir.exists());
    assert!(
        store
            .index()
            .iter_segment_states_for_shard(shard_a)
            .unwrap()
            .iter()
            .all(|(_, state)| state.state == SegmentFileState::Deleted)
    );
    for segment_id in shard_a_segment_ids {
        let state = store
            .index()
            .get_segment_state(segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(state.owner, SegmentOwner::Shard(shard_a));
        assert_eq!(state.state, SegmentFileState::Deleted);
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
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    let dropped_shard = store.add_shard(17).unwrap();
    let kept_shard = store.add_shard(18).unwrap();
    let dropped_key = BlobKey::new(b"drop-copy".to_vec()).unwrap();
    let kept_key = BlobKey::new(b"keep-copy".to_vec()).unwrap();
    let rollover_key = BlobKey::new(b"roll-copy".to_vec()).unwrap();

    for key in [&dropped_key, &kept_key, &rollover_key] {
        store.set_blob_lifetime(key, 50).unwrap();
    }
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
    wait_for_lsm_gc(&store, rollover_lsn);

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
    cfg.gc_workers_enabled = false;
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
    wait_for_lsm_gc(&store, lsn_b);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);
    wait_for_lsm_gc(&store, tombstone_lsn);

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
    cfg.gc_workers_enabled = false;
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
    wait_for_lsm_gc(&store, lsn_c);

    store.tombstone(&key_a).unwrap();
    let tombstone_lsn_b = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn_b);
    wait_for_lsm_gc(&store, tombstone_lsn_b);

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
    cfg.gc_interval = Duration::from_secs(3600);
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

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);
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
async fn cold_epoch_expiry_enables_gc_after_the_accounting_frontier() {
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
    store.put(&key_b, b"payload-b").unwrap();
    store.sync().unwrap();
    let mut sealed_state =
        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    sealed_state.placement_class = PlacementClass::ExactEpoch(43);
    store.index().put_segment_state(&sealed_state).unwrap();
    let sealed_path = segment_state_path(store.config(), &sealed_state);
    store.sync().unwrap();

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();
    assert_eq!(store.get(&key_a).unwrap(), None);
    wait_for_expiry_accounting(&store, epoch_lsn);

    // The planner could observe current epoch 43 before this frontier, but must not use it to move
    // the apparently-live ExactEpoch(43) segment. The cold major compaction and garbage sweep make
    // the frontier visible only after the summary reaches zero live refs; that sweep also nudges GC
    // so no synthetic lifetime update is needed to delete the segment.
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
            "expiry-accounting frontier did not enable empty-segment GC"
        );
        thread::sleep(Duration::from_millis(10));
    }

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
    wait_for_lsm_gc(&store, lsn_c);

    store.tombstone(&key_a).unwrap();
    let tombstone_lsn_b = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn_b);
    wait_for_lsm_gc(&store, tombstone_lsn_b);
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
    let mut cfg = config(dir.path(), "default");
    // This test drives prepare/copy/publish itself and intentionally creates only index metadata
    // for the source segment. Once the fixture publishes an expiry frontier, a production GC
    // worker would be woken and could race the manual path to that nonexistent source file.
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
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
    let lifecycle = BlobLifecycle {
        logical_end_epoch: current_epoch + 10,
        extension_count: 2,
    };
    store
        .index()
        .put_segment_gc_summary_batch(
            &mut batch,
            segment_id,
            &core_types::SegmentGcSummary {
                total_bytes: range.len,
                live_bytes: range.len,
                live_ref_count: 1,
                min_live_end_epoch: Some(lifecycle.logical_end_epoch),
                max_live_end_epoch: Some(lifecycle.logical_end_epoch),
                future_epoch_histogram: BTreeMap::from([(
                    lifecycle.logical_end_epoch,
                    EpochBucket {
                        refs: 1,
                        bytes: range.len,
                    },
                )]),
                extension_count_histogram: BTreeMap::from([(lifecycle.extension_count, 1)]),
                ..Default::default()
            },
        )
        .unwrap();
    // This test constructs an exact-epoch segment and its GC summary directly, bypassing the
    // blob-LSM compaction and garbage-sweeper pipeline that normally advances this frontier.
    // LSN 0 contains the store's genesis epoch transition, so accounting through LSN 0 means:
    // "all bases have applied the genesis epoch, and its garbage records have been swept."
    // Without this explicit test fixture state, the planner must conservatively return no plan.
    store
        .index()
        .put_blob_expiry_accounted_lsn_batch(&mut batch, 0)
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
async fn join_multiple_fully_evacuates_and_deletes_every_source() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    cfg.lsm_partition_count = 4;
    let keys = ["blob-a", "blob-b", "blob-c", "blob-d", "blob-e"]
        .map(|key| BlobKey::new(key.as_bytes().to_vec()).unwrap());
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    for key in &keys {
        store.set_blob_lifetime(key, 50).unwrap();
    }
    let mut last_put_lsn = 0;
    for (key, payload) in keys.iter().zip([
        b"payload-a".as_slice(),
        b"payload-b".as_slice(),
        b"payload-c".as_slice(),
        b"payload-d".as_slice(),
        b"payload-e".as_slice(),
    ]) {
        last_put_lsn = store.put(key, payload).unwrap();
    }
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    wait_for_segment_state(
        store.index(),
        FIRST_SEGMENT_ID + 1,
        SegmentFileState::Sealed,
    );
    store.sync().unwrap();
    wait_for_lsm_gc(&store, last_put_lsn);

    let live_left = lsm_blob_ref(&store, &keys[1]);
    let live_right = lsm_blob_ref(&store, &keys[3]);
    assert_eq!(live_left.segment_id, FIRST_SEGMENT_ID);
    assert_eq!(live_right.segment_id, FIRST_SEGMENT_ID + 1);

    store.tombstone(&keys[0]).unwrap();
    let last_tombstone_lsn = store.tombstone(&keys[2]).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, last_tombstone_lsn);
    wait_for_lsm_gc(&store, last_tombstone_lsn);

    let source_ids = [live_left.segment_id, live_right.segment_id];
    let source_paths = source_ids.map(|segment_id| {
        let mut state = store
            .index()
            .get_segment_state(segment_id)
            .unwrap()
            .unwrap();
        let path = segment_state_path(store.config(), &state);
        state.placement_class = PlacementClass::ExactEpoch(50);
        (state, path)
    });
    let mut batch = store.index().batch();
    for (state, _) in &source_paths {
        store
            .index()
            .put_segment_state_batch(&mut batch, state)
            .unwrap();
    }
    batch.write_with_sync(true).unwrap();

    let planner = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        max_l0_copy_bytes_per_plan: TEST_RECORD_LEN * 4,
        min_l0_rewrite_epoch_distance: 1,
        min_l0_rewrite_useful_ratio_bps: 6_600,
        min_reclaim_bytes: u64::MAX,
        min_garbage_ratio_bps: 10_000,
        min_exact_epoch_bucket_bytes: 1,
        min_exact_epoch_distance: 1,
        max_exact_epoch_extension_count: 1,
        min_join_output_bytes: 1,
        max_join_sources: 4,
    });
    let prepared = store.prepare_gc_plan(&planner).unwrap().unwrap();
    assert_eq!(prepared.plan.scenario, GcScenario::JoinMultiple);
    let GcAction::MoveLiveBytesFromSources { routes } = &prepared.plan.action else {
        panic!("expected a full-source join action");
    };
    assert_eq!(
        routes
            .iter()
            .map(|route| route.source_segment_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(source_ids)
    );
    assert_eq!(prepared.plan.copied_bytes, TEST_RECORD_LEN * 2);

    let copied = store.copy_prepared_gc_plan(prepared).unwrap();
    assert_eq!(copied.outputs.len(), 1);
    assert_eq!(copied.copied_records.len(), 2);
    let published = store.publish_prepared_gc_copy(copied).unwrap();
    assert_eq!(published.output_segments.len(), 1);
    assert_eq!(published.published_records.len(), 2);
    for segment_id in source_ids {
        assert_eq!(
            store
                .index()
                .get_segment_state(segment_id)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::GcRelocating
        );
    }

    loop {
        let swept = {
            let _publish_guard = store
                .store
                .garbage_publish_lock
                .lock()
                .expect("garbage publication lock poisoned");
            store
                .index()
                .sweep_garbage_log(
                    garbage_log_dir(store.config()),
                    store.config().namespace_dir(),
                    GARBAGE_LOG_HEAD,
                    GARBAGE_LOG_SWEEP_CURSOR,
                )
                .unwrap()
        };
        if !swept {
            break;
        }
    }

    let prepared_delete = store.prepare_gc_plan(&planner).unwrap().unwrap();
    assert_eq!(prepared_delete.plan.scenario, GcScenario::EmptyDelete);
    assert_eq!(
        prepared_delete.plan.action,
        GcAction::DeleteSegments {
            segment_ids: source_ids.to_vec(),
        }
    );
    let copied_delete = store.copy_prepared_gc_plan(prepared_delete).unwrap();
    store.publish_prepared_gc_copy(copied_delete).unwrap();

    for (state, path) in source_paths {
        assert_eq!(
            store
                .index()
                .get_segment_state(state.segment_id)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::Deleted
        );
        assert!(!path.exists());
    }
    assert_eq!(store.get(&keys[1]).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&keys[3]).unwrap(), Some(b"payload-d".to_vec()));
}

#[tokio::test]
async fn gc_publish_maps_surviving_copied_record_to_output_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    cfg.lsm_partition_count = 4;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let mut store = try_open_standalone_store(cfg, metrics).unwrap();

    let first_segment_published_lsn = store.set_blob_lifetime(&key_a, 50).unwrap();
    store.set_blob_lifetime(&key_b, 50).unwrap();
    store.set_blob_lifetime(&key_c, 50).unwrap();
    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();
    store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();

    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);

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
    store
        .store
        .garbage_sweep_tx
        .take()
        .unwrap()
        .send(())
        .unwrap();
    store
        .store
        .garbage_sweep_handle
        .take()
        .unwrap()
        .join()
        .unwrap();
    let next_lsn_before_publish = store.index().get_next_lsn().unwrap();
    let published_lsn_before_publish = store.published_lsn().unwrap();
    let checkpoint_before_publish = store.index().get_store_checkpoint().unwrap();

    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_lsn >= tombstone_lsn);
    assert_eq!(published.skipped_records, Vec::new());
    assert_eq!(published.output_segments.len(), 1);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(
        store.index().get_next_lsn().unwrap(),
        next_lsn_before_publish
    );
    assert_eq!(store.published_lsn().unwrap(), published_lsn_before_publish);
    assert_eq!(
        store.index().get_store_checkpoint().unwrap(),
        checkpoint_before_publish
    );
    assert_eq!(store.relocation_cache.len(), 1);
    assert_eq!(
        counter_value(&registry, "strata_store_gc_output_bytes_total"),
        ref_b.len as f64
    );
    assert_eq!(
        counter_value(&registry, "strata_store_gc_source_deleted_bytes_total"),
        0.0
    );
    assert_eq!(
        counter_value(&registry, "strata_store_gc_reclaimed_bytes_total"),
        0.0
    );
    assert!(!staged_path.exists());
    assert!(published.output_segments[0].path.exists());

    let published_record = &published.published_records[0];
    assert_eq!(published_record.publish_lsn, published_lsn_before_publish);
    assert_eq!(published_record.source.from, ref_b);
    assert_eq!(
        store
            .index()
            .get_segment_published_at_lsn(ref_b.segment_id)
            .unwrap(),
        first_segment_published_lsn
    );
    assert_eq!(
        store
            .index()
            .get_segment_state(ref_b.segment_id)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::GcRelocating
    );
    assert!(store.prepare_gc_plan(&planner).unwrap().is_none());
    assert_eq!(lsm_blob_ref(&store, &key_b), ref_b);
    let relocation = store
        .relocations
        .lookup(
            &key_b,
            STANDALONE_SHARD,
            published_record.source.payload_lsn,
        )
        .unwrap()
        .unwrap();
    assert_eq!(relocation.to, published_record.to);
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(
        store
            .index()
            .get_segment_state(published_record.to.segment_id)
            .unwrap()
            .unwrap()
            .placement_class,
        PlacementClass::ExactEpoch(50)
    );
    assert_eq!(
        store
            .index()
            .get_segment_published_at_lsn(published_record.to.segment_id)
            .unwrap(),
        published_record.publish_lsn
    );
    assert_eq!(store.published_lsn().unwrap(), published_record.publish_lsn);
    let executor = store.store.gc_executor().unwrap();
    assert!(
        executor
            .relocation_activation_is_durable(ref_b.segment_id)
            .unwrap()
    );

    loop {
        let swept = {
            let _publish_guard = store
                .store
                .garbage_publish_lock
                .lock()
                .expect("garbage publication lock poisoned");
            store
                .index()
                .sweep_garbage_log(
                    garbage_log_dir(store.config()),
                    store.config().namespace_dir(),
                    GARBAGE_LOG_HEAD,
                    GARBAGE_LOG_SWEEP_CURSOR,
                )
                .unwrap()
        };
        if !swept {
            break;
        }
    }

    wait_for_lsm_gc(&store, published_record.publish_lsn);

    let source_overlay = segment_overlay(&store, ref_a.segment_id);
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(gc_ranges_contain(&source_overlay.retired, ref_b));

    let output_summary = segment_summary(store.index(), published_record.to.segment_id);
    assert_eq!(output_summary.live_bytes, ref_b.len);
    assert_eq!(output_summary.live_ref_count, 1);
    assert_eq!(output_summary.total_bytes, ref_b.len);

    let snapshot = store
        .store
        .live_snapshots
        .pin(store.index().get_next_lsn().unwrap().saturating_sub(1));
    let prepared_delete = store.prepare_gc_plan(&planner).unwrap().unwrap();
    assert_eq!(prepared_delete.plan.scenario, GcScenario::EmptyDelete);
    assert_eq!(
        prepared_delete.plan.action,
        GcAction::DeleteSegments {
            segment_ids: vec![ref_b.segment_id]
        }
    );
    let copied_delete = store.copy_prepared_gc_plan(prepared_delete).unwrap();
    store.publish_prepared_gc_copy(copied_delete).unwrap();
    assert_eq!(
        store
            .index()
            .get_segment_state(ref_b.segment_id)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::GcRelocating
    );
    drop(snapshot);

    let older_snapshot = store
        .store
        .live_snapshots
        .pin(first_segment_published_lsn.saturating_sub(1));
    let prepared_delete = store.prepare_gc_plan(&planner).unwrap().unwrap();
    let copied_delete = store.copy_prepared_gc_plan(prepared_delete).unwrap();
    store.publish_prepared_gc_copy(copied_delete).unwrap();
    assert_eq!(
        store
            .index()
            .get_segment_state(ref_b.segment_id)
            .unwrap()
            .unwrap()
            .state,
        SegmentFileState::Deleted
    );
    drop(older_snapshot);
    assert_eq!(
        counter_value(&registry, "strata_store_gc_source_deleted_bytes_total"),
        (ref_a.len + ref_b.len) as f64
    );
    assert_eq!(
        counter_value(&registry, "strata_store_gc_reclaimed_bytes_total"),
        ref_a.len as f64
    );
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(
        counter_value_with_labels(
            &registry,
            "strata_store_relocation_cache_requests_total",
            &[("result", "hit")]
        ),
        1.0
    );

    let cfg = store.config().clone();
    drop(executor);
    drop(store);
    let reopened = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(reopened.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(
        reopened
            .relocations
            .lookup(
                &key_b,
                STANDALONE_SHARD,
                published_record.source.payload_lsn,
            )
            .unwrap()
            .unwrap()
            .to,
        published_record.to
    );
}

#[tokio::test]
async fn gc_publish_pre_commit_failure_removes_renamed_output_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 4 - 1;
    cfg.gc_workers_enabled = false;
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
    wait_for_lsm_gc(&store, lsn_d);
    let mut source_state = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    source_state.placement_class = PlacementClass::Spillover;
    store.index().put_segment_state(&source_state).unwrap();

    let tombstone_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);
    wait_for_lsm_gc(&store, tombstone_lsn);

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

    let source_segment_id = copied.copied_records[0].source.from.segment_id;
    let mut source_state = store
        .index()
        .get_segment_state(source_segment_id)
        .unwrap()
        .unwrap();
    source_state.state = SegmentFileState::Open;
    store.index().put_segment_state(&source_state).unwrap();

    let err = store.publish_prepared_gc_copy(copied).unwrap_err();

    assert!(matches!(
        err,
        Error::GcSourceSegmentNotSealed {
            segment_id,
            state: SegmentFileState::Open,
        } if segment_id == source_segment_id
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
async fn gc_publish_tombstoned_pending_copy_retires_destination_after_forwarding() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_c);
    let mut source_state = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    source_state.placement_class = PlacementClass::Spillover;
    store.index().put_segment_state(&source_state).unwrap();

    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);
    let payload_lsn_b = lsm_blob(&store, STANDALONE_SHARD, &key_b).payload_lsn;

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_a_lsn);
    wait_for_lsm_gc(&store, tombstone_a_lsn);

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

    let tombstone_b_lsn = store.tombstone(&key_b).unwrap();
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_lsn >= tombstone_b_lsn);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(published.output_segments.len(), 1);
    assert!(published.skipped_records.is_empty());
    assert_eq!(published.published_records[0].source.from, ref_b);
    assert_eq!(store.get(&key_b).unwrap(), None);
    let destination = published.published_records[0].to;
    let publish_lsn = published.published_records[0].publish_lsn;
    assert!(
        store
            .relocations
            .lookup(&key_b, STANDALONE_SHARD, payload_lsn_b)
            .unwrap()
            .is_some_and(|relocation| relocation.to == destination)
    );
    let source_overlay = segment_overlay(&store, ref_a.segment_id);
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));

    // The tombstone was not in a committed garbage event when GC reconciled, so the destination
    // starts conservatively live. Full compaction resolves the payload identity through the
    // relocation and retires the physical destination.
    let initial_destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(!gc_ranges_contain(
        &initial_destination_overlay.retired,
        destination
    ));
    store.sync().unwrap();
    wait_for_lsm_gc(&store, publish_lsn.max(tombstone_b_lsn));
    let destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(gc_ranges_contain(&destination_overlay.retired, destination));
}

#[tokio::test]
async fn gc_publish_pending_epoch_change_expires_relocated_destination() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    let lifetime_b_lsn = store.extend(&key_b, 43).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_c.max(lifetime_b_lsn));

    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);
    let payload_lsn_b = lsm_blob(&store, STANDALONE_SHARD, &key_b).payload_lsn;

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_a_lsn);
    wait_for_lsm_gc(&store, tombstone_a_lsn);

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

    // Leave a keyed patch pending as well as the epoch transition. GC must not infer expiry from
    // current_epoch; the subsequent full compaction is what emits the explicit Expired event.
    let lifetime_touch_lsn = store.extend(&key_b, 43).unwrap().unwrap();
    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(store.get(&key_b).unwrap(), None);
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_lsn >= lifetime_touch_lsn);
    assert!(published.reconciled_lsn >= epoch_lsn);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(published.output_segments.len(), 1);
    assert!(published.skipped_records.is_empty());
    assert_eq!(published.published_records[0].source.from, ref_b);
    let destination = published.published_records[0].to;
    let publish_lsn = published.published_records[0].publish_lsn;

    let source_overlay = segment_overlay(&store, ref_a.segment_id);
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(
        store
            .relocations
            .lookup(&key_b, STANDALONE_SHARD, payload_lsn_b)
            .unwrap()
            .is_some_and(|relocation| relocation.to == destination)
    );

    let initial_destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(!gc_ranges_contain(
        &initial_destination_overlay.expired,
        destination
    ));
    assert!(initial_destination_overlay.lifetimes.is_empty());
    store.sync().unwrap();
    wait_for_lsm_gc(&store, publish_lsn.max(lifetime_touch_lsn).max(epoch_lsn));
    let destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(gc_ranges_contain(&destination_overlay.expired, destination));
}

#[tokio::test]
async fn gc_publish_forwards_lagging_lifetime_then_retires_destination() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    cfg.gc_workers_enabled = false;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-b").unwrap();
    let lsn_c = store.put(&key_c, b"payload-c").unwrap();
    store.sync().unwrap();
    wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lsn_c);
    let mut source_state = store
        .index()
        .get_segment_state(FIRST_SEGMENT_ID)
        .unwrap()
        .unwrap();
    source_state.placement_class = PlacementClass::Spillover;
    store.index().put_segment_state(&source_state).unwrap();

    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);
    let payload_lsn_b = lsm_blob(&store, STANDALONE_SHARD, &key_b).payload_lsn;

    let tombstone_a_lsn = store.tombstone(&key_a).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_a_lsn);
    wait_for_lsm_gc(&store, tombstone_a_lsn);

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

    let lifetime_b_lsn = store.extend(&key_b, 50).unwrap().unwrap();
    assert_eq!(store.get(&key_b).unwrap(), Some(b"payload-b".to_vec()));
    let published = store.publish_prepared_gc_copy(copied).unwrap();

    assert!(published.reconciled_lsn >= lifetime_b_lsn);
    assert_eq!(published.published_records.len(), 1);
    assert_eq!(published.output_segments.len(), 1);
    assert!(published.skipped_records.is_empty());
    assert_eq!(published.published_records[0].source.from, ref_b);
    let destination = published.published_records[0].to;
    let publish_lsn = published.published_records[0].publish_lsn;

    let source_overlay = segment_overlay(&store, ref_a.segment_id);
    assert!(gc_ranges_contain(&source_overlay.retired, ref_a));
    assert!(
        store
            .relocations
            .lookup(&key_b, STANDALONE_SHARD, payload_lsn_b)
            .unwrap()
            .is_some_and(|relocation| relocation.to == destination)
    );

    // Output publication starts unknown even though the pending main-LSM patch already contains
    // lifetime 50. Compaction heals the main-LSM ref and emits SetLifecycle for the destination.
    let initial_destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(initial_destination_overlay.lifetimes.is_empty());
    store.sync().unwrap();
    wait_for_lsm_gc(&store, publish_lsn.max(lifetime_b_lsn));
    let destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(destination_overlay.lifetimes.iter().any(|entry| {
        entry.range == SegmentGcRecordRange::from(destination)
            && entry.lifecycle.logical_end_epoch == 50
    }));
    assert_eq!(lsm_blob_ref(&store, &key_b), destination);

    let tombstone_lsn = store.tombstone(&key_b).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);
    let destination_overlay = segment_overlay(&store, destination.segment_id);
    assert!(gc_ranges_contain(&destination_overlay.retired, destination));
    assert!(destination_overlay.lifetimes.is_empty());
    assert_eq!(destination_overlay.summary.live_bytes, 0);
    assert_eq!(destination_overlay.summary.live_ref_count, 0);
    assert_eq!(destination_overlay.summary.retired_bytes, destination.len);
}
#[tokio::test]
async fn blob_lsm_retire_removes_lifetime_hint() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 50).unwrap().unwrap();
    let record_ref = lsm_blob_ref(&store, &key);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_lsn);

    let overlay = segment_overlay(&store, record_ref.segment_id);
    assert_eq!(overlay.lifetimes.len(), 1);

    let tombstone_lsn = store.tombstone(&key).unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, tombstone_lsn);

    let overlay = segment_overlay(&store, record_ref.segment_id);
    assert_eq!(overlay.retired, vec![gc_range(record_ref)]);
    assert!(overlay.expired.is_empty());
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn cold_base_sweep_updates_gc_summary_without_a_user_touch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key_a, b"payload-a").unwrap();
    store.put(&key_b, b"payload-bb").unwrap();
    store.extend(&key_a, 43).unwrap().unwrap();
    let lifetime_b_lsn = store.extend(&key_b, 50).unwrap().unwrap();
    let ref_a = lsm_blob_ref(&store, &key_a);
    let ref_b = lsm_blob_ref(&store, &key_b);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_b_lsn);

    let stats = segment_summary(store.index(), ref_a.segment_id);
    assert_eq!(stats.live_bytes, ref_a.len + ref_b.len);
    assert_eq!(stats.live_ref_count, 2);
    assert_eq!(stats.expired_bytes, 0);

    let (epoch, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(epoch, 43);
    store.sync().unwrap();
    wait_for_expiry_accounting(&store, epoch_lsn);

    // No foreground mutation touched either key after the epoch change. Reaching the frontier
    // proves a forced major compaction nevertheless visited the cold base, emitted key A's expiry,
    // and waited for the sweeper to fold that event into this summary.
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
    wait_for_expiry_accounting(&store, last_epoch_lsn);

    // The second cold sweep likewise expires B without manufacturing a patch just to wake
    // compaction. At this point GC may safely consume the zero-live summary.
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
async fn continuous_compaction_wakes_do_not_starve_cold_base_sweep() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.gc_workers_enabled = false;
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_lsn);

    let compact_tx = store.store.lsm_compact_tx.as_ref().unwrap().clone();
    let (stop_tx, stop_rx) = mpsc::channel();
    let wake_handle = thread::spawn(move || {
        while stop_rx.try_recv().is_err() {
            if compact_tx.send(()).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    let (epoch, epoch_lsn) = store.increment_epoch().unwrap();
    assert_eq!(epoch, 43);
    store.sync().unwrap();

    let started = Instant::now();
    let advanced_while_wakes_continued = loop {
        let accounted = store
            .index()
            .get_blob_expiry_accounted_lsn()
            .unwrap()
            .unwrap_or_default();
        if accounted >= epoch_lsn {
            break true;
        }
        if started.elapsed() >= Duration::from_secs(5) {
            break false;
        }
        thread::sleep(Duration::from_millis(10));
    };

    stop_tx.send(()).unwrap();
    wake_handle.join().unwrap();
    assert!(
        advanced_while_wakes_continued,
        "continuous compaction nudges reset the forced-pass deadline"
    );
}

#[tokio::test]
async fn compaction_expiry_does_not_revive_blob_on_extension() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    let record_ref = lsm_blob_ref(&store, &key);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_lsn);

    let (_, epoch_lsn) = store.increment_epoch().unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, epoch_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.expired_bytes, 0);
    assert_eq!(stats.live_ref_count, 1);

    let extend_lsn = store.extend(&key, 50).unwrap().unwrap();
    assert_eq!(store.get(&key).unwrap(), None);
    store.sync().unwrap();
    wait_for_lsm_gc(&store, extend_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(stats.expired_bytes, record_ref.len);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.live_ref_count, 0);
    assert_eq!(stats.future_epoch_histogram.get(&50), None);
    assert_eq!(stats.min_live_end_epoch, None);
}

#[tokio::test]
async fn snapshot_compaction_expires_future_epoch_bucket_for_exact_epoch_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let store =
        try_open_standalone_store(config(dir.path(), "default"), StrataStoreMetrics::default())
            .unwrap();

    store.put(&key, b"payload").unwrap();
    let record_ref = lsm_blob_ref(&store, &key);
    let mut state = store
        .index()
        .get_segment_state(record_ref.segment_id)
        .unwrap()
        .unwrap();
    state.placement_class = PlacementClass::ExactEpoch(42);
    store.index().put_segment_state(&state).unwrap();
    let lifetime_lsn = store.extend(&key, 43).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, lifetime_lsn);

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
    wait_for_lsm_gc(&store, epoch_lsn);

    let stats = segment_summary(store.index(), record_ref.segment_id);
    assert_eq!(
        stats.future_epoch_histogram.get(&43),
        Some(&EpochBucket {
            refs: 1,
            bytes: record_ref.len,
        })
    );
    assert_eq!(stats.expired_bytes, 0);

    let touch_lsn = store.extend(&key, 50).unwrap().unwrap();
    store.sync().unwrap();
    wait_for_lsm_gc(&store, touch_lsn);

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
        assert_eq!(lsm_blob_ref(&store, &key_1).segment_id, 1);
        assert_eq!(lsm_blob_ref(&store, &key_2).segment_id, 2);
        assert_eq!(lsm_blob(&store, STANDALONE_SHARD, &key_2).head_lsn, 2);
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
async fn recovery_rolls_back_unpublished_ops_missing_from_store_wal() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload-a").unwrap(), 1);
        assert_eq!(store.index().get_next_lsn().unwrap(), 2);
    }

    std::fs::remove_file(Wal::path(cfg.namespace_dir().join("wal"), 1)).unwrap();

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    assert_eq!(store.index().get_next_lsn().unwrap(), 1);
    assert_eq!(store.get(&key).unwrap(), None);
    assert_eq!(store.put(&key, b"payload-b").unwrap(), 1);
    assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
}

#[tokio::test]
async fn recovery_rejects_missing_store_wal_for_published_lsn() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        assert_eq!(store.put(&key, b"payload-a").unwrap(), 1);
        store.sync().unwrap();
        assert_eq!(store.published_lsn().unwrap(), 1);
    }

    std::fs::remove_file(Wal::path(cfg.namespace_dir().join("wal"), 1)).unwrap();

    let err = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap_err();
    assert!(matches!(err, Error::InvalidWal(_)));
}
#[tokio::test]
async fn point_in_time_recovery_discards_higher_segments_after_lower_gap() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_RECORD_LEN * 3 - 1;
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
    let first_end;
    let second_end;
    {
        let store = try_open_standalone_store(cfg.clone(), StrataStoreMetrics::default()).unwrap();
        store.put(&key_a, b"payload-a").unwrap();
        store.put(&key_b, b"payload-b").unwrap();
        store.put(&key_c, b"payload-c").unwrap();
        let ref_a = lsm_blob_ref(&store, &key_a);
        let ref_b = lsm_blob_ref(&store, &key_b);
        let ref_c = lsm_blob_ref(&store, &key_c);
        assert_eq!(
            (ref_a.segment_id, ref_b.segment_id, ref_c.segment_id),
            (1, 1, 2)
        );
        first_end = ref_a.end_offset().unwrap();
        second_end = ref_b.end_offset().unwrap();
    }

    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    let mut segment_1_state = index.get_segment_state(1).unwrap().unwrap();
    segment_1_state.state = SegmentFileState::Sealing;
    segment_1_state.write_offset = second_end;
    segment_1_state.durable_offset = 0;
    segment_1_state.sealed_len = None;
    segment_1_state.sealed_sha256 = None;
    let mut segment_2_state = index.get_segment_state(2).unwrap().unwrap();
    segment_2_state.state = SegmentFileState::Open;
    segment_2_state.durable_offset = 0;
    segment_2_state.sealed_len = None;
    segment_2_state.sealed_sha256 = None;
    let mut batch = index.batch();
    index
        .put_segment_state_batch(&mut batch, &segment_1_state)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &segment_2_state)
        .unwrap();
    batch.write().unwrap();
    drop(index);
    OpenOptions::new()
        .write(true)
        .open(segment_path(&cfg, 1))
        .unwrap()
        .set_len(first_end)
        .unwrap();

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
        lsm_blob_ref(&store, &key_a).end_offset().unwrap(),
        first_end
    );
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
async fn recovery_seals_rolled_segments_before_starting_workers() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    put_test_segment_state(&index, 1, SegmentFileState::Sealing);
    put_test_segment_state(&index, 2, SegmentFileState::Open);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(segment_path(&cfg, 1))
        .unwrap()
        .write_all(&[0; 64])
        .unwrap();

    seal::seal_recovered_segments(&cfg, &index, 2, &StrataStoreMetrics::default()).unwrap();

    let sealed = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(sealed.state, SegmentFileState::Sealed);
    assert_eq!(sealed.durable_offset, 64);
    assert_eq!(sealed.sealed_len, Some(64));
    assert_eq!(
        index.get_segment_state(2).unwrap().unwrap().state,
        SegmentFileState::Open
    );
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
async fn recovery_seal_error_leaves_segment_unsealed() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let cfg = config(dir.path(), "default");
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    put_test_segment_state(&index, 1, SegmentFileState::Sealing);
    assert!(
        seal::seal_recovered_segments(&cfg, &index, 2, &StrataStoreMetrics::default()).is_err()
    );
    let state = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(state.state, SegmentFileState::Sealing);
}

#[tokio::test]
async fn rollover_switches_active_segment_and_durability_seals_old_segment() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "default").unwrap();
    let store = try_open_standalone_store(cfg, metrics).unwrap();

    assert_eq!(gauge_value(&registry, "strata_store_unsealed_segments"), 1);

    store.put(&key_1, b"payload-a").unwrap();
    store.put(&key_2, b"payload-b").unwrap();

    assert_eq!(gauge_value(&registry, "strata_store_unsealed_segments"), 2);

    assert_eq!(lsm_blob_ref(&store, &key_1).segment_id, 1);
    assert_eq!(lsm_blob_ref(&store, &key_2).segment_id, 2);
    assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));

    let sealing = store.index().get_segment_state(1).unwrap().unwrap();
    assert_eq!(sealing.state, SegmentFileState::Sealing);
    assert_eq!(store.published_lsn().unwrap(), 0);

    store.sync().unwrap();
    let sealed = wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
    assert_eq!(sealed.durable_offset, sealed.write_offset);
    assert_eq!(sealed.sealed_len, Some(sealed.write_offset));
    assert_eq!(sealed.sealed_sha256, None);
    assert_eq!(store.published_lsn().unwrap(), 2);
    assert_eq!(gauge_value(&registry, "strata_store_unsealed_segments"), 1);

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
}

#[tokio::test]
async fn one_batch_can_publish_multiple_rollovers_without_staging_new_segment_states() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
    let keys = [
        BlobKey::new(b"blob-a".to_vec()).unwrap(),
        BlobKey::new(b"blob-b".to_vec()).unwrap(),
        BlobKey::new(b"blob-c".to_vec()).unwrap(),
    ];
    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();

    let mut batch = store.batch();
    for key in &keys {
        batch.put(
            STANDALONE_SHARD.id,
            key.clone(),
            Arc::<[u8]>::from(&b"payload-a"[..]),
        );
    }
    assert_eq!(batch.write().unwrap().op_lsns(), &[1, 2, 3]);

    assert_eq!(lsm_blob_ref(&store, &keys[0]).segment_id, 1);
    assert_eq!(lsm_blob_ref(&store, &keys[1]).segment_id, 2);
    assert_eq!(lsm_blob_ref(&store, &keys[2]).segment_id, 3);
    assert_eq!(
        store.index().get_segment_state(1).unwrap().unwrap().state,
        SegmentFileState::Sealing
    );
    assert_eq!(
        store.index().get_segment_state(2).unwrap().unwrap().state,
        SegmentFileState::Sealing
    );
    assert_eq!(
        store.index().get_segment_state(3).unwrap().unwrap().state,
        SegmentFileState::Open
    );
    assert_eq!(store.index().get_segment_published_at_lsn(2).unwrap(), 2);
    assert_eq!(store.index().get_segment_published_at_lsn(3).unwrap(), 3);
}

#[tokio::test]
async fn segment_pressure_syncs_active_segment_without_rollover() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let mut cfg = config(dir.path(), "default");
    cfg.max_unsealed_segments = 3;
    ensure_ingest_dir(&cfg).unwrap();
    let index = open_test_index(cfg.standalone_index_dir(), cfg.index_cf_prefix());
    ensure_epoch_initialized(&index, cfg.starting_epoch).unwrap();
    index
        .put_shard_info(
            STANDALONE_SHARD.id,
            ShardInfo::active(STANDALONE_SHARD.generation),
        )
        .unwrap();
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
    let (wal, recovered, _relocation_recovery, _lsm_sync_handles) =
        open_store_wal(&cfg, &index, index.get_next_lsn().unwrap(), None).unwrap();
    let lsm = open_lsm(&cfg, &index, index.get_next_lsn().unwrap(), recovered).unwrap();
    let (segment_sync_tx, segment_syncer) = file_sync_channel();
    let (lsm_flush_tx, _lsm_flush_rx) = mpsc::channel();
    let (lsm_compact_tx, _lsm_compact_rx) = mpsc::channel();
    let (wal_reclaim_tx, _wal_reclaim_rx) = mpsc::sync_channel(1);
    let (write_tx, write_rx) = mpsc::sync_channel(1);
    let (sync_done_tx, sync_done_rx) = mpsc::channel();

    let mut coordinator = WriteCoordinator {
        config: cfg.clone(),
        index: index.clone(),
        lsm,
        wal,
        segment: active_writer,
        segment_factory: SegmentFactory::new(
            cfg.ingest_dir(),
            SegmentIdAllocator::new(2),
            PlacementClass::Ingest,
            cfg.segment_max_bytes,
        ),
        segment_sync_tx,
        pending_segment_syncs: Vec::new(),
        internal_write_tx: write_tx,
        sync_done_tx,
        sync_done_rx,
        sync_and_commit_in_flight: None,
        pending_sync_requests: Vec::new(),
        active_segment_state,
        durable_offset: 0,
        active_allocation_records: 0,
        active_allocation_tracker: Arc::new(SegmentAllocationTracker::default()),
        pending_segment_bytes: 0,
        unsealed_segments: 1,
        oldest_uncommitted_at: None,
        last_committed_at: Instant::now(),
        pending_rollovers: Vec::new(),
        lsm_flush_tx,
        lsm_compact_tx,
        wal_reclaim_tx,
        write_rx,
        ingest_owner: INGEST_SEGMENT_OWNER,
        gc_concurrency,
        store_halt: StoreHalt::default(),
        metrics,
    };

    let (first_response_tx, first_response_rx) = mpsc::channel();
    let (invalid_response_tx, invalid_response_rx) = mpsc::channel();
    let (epoch_response_tx, epoch_response_rx) = mpsc::channel();
    coordinator.process_batch_group(vec![
        BatchWriteRequest {
            ops: vec![BatchOp::Put {
                shard_id: STANDALONE_SHARD.id,
                key: BlobKey::new(b"durability-pressure".to_vec()).unwrap(),
                payload: Arc::from(&b"payload"[..]),
            }],
            response_tx: first_response_tx,
            profile: ProfileRequest::default(),
        },
        BatchWriteRequest {
            ops: vec![BatchOp::SetBlobLifetime {
                key: BlobKey::new(b"invalid-lifetime".to_vec()).unwrap(),
                logical_end_epoch: cfg.starting_epoch,
            }],
            response_tx: invalid_response_tx,
            profile: ProfileRequest::default(),
        },
        BatchWriteRequest {
            ops: vec![BatchOp::IncrementEpoch],
            response_tx: epoch_response_tx,
            profile: ProfileRequest::default(),
        },
    ]);
    assert_eq!(
        first_response_rx.recv().unwrap().unwrap().last_lsn(),
        Some(1)
    );
    assert!(matches!(
        invalid_response_rx.recv().unwrap(),
        Err(Error::InvalidBlobLifetime { .. })
    ));
    let epoch_result = epoch_response_rx.recv().unwrap().unwrap();
    assert_eq!(epoch_result.last_lsn(), Some(2));
    assert_eq!(epoch_result.last_epoch(), Some(cfg.starting_epoch + 1));
    assert_eq!(
        histogram_sample_count(&registry, "strata_store_requests_per_commit_group"),
        1
    );
    assert_eq!(
        histogram_sample_sum(&registry, "strata_store_requests_per_commit_group"),
        2.0
    );
    let record_bytes = coordinator.active_segment_state.write_offset;
    coordinator.pending_segment_bytes = SYNC_AND_COMMIT_SEGMENT_BYTES - record_bytes;
    coordinator.note_uncommitted_write(record_bytes).unwrap();
    assert!(coordinator.pending_segment_syncs.is_empty());
    assert_eq!(coordinator.sync_and_commit_in_flight, Some(2));

    let active_state = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(active_state.state, SegmentFileState::Open);
    assert_eq!(active_state.write_offset, record_bytes);
    assert_eq!(active_state.sealed_before_lsn, None);
    assert_eq!(index.get_segment_state(2).unwrap(), None);

    let (second_response_tx, second_response_rx) = mpsc::channel();
    coordinator.process_batch_group(vec![BatchWriteRequest {
        ops: vec![BatchOp::IncrementEpoch],
        response_tx: second_response_tx,
        profile: ProfileRequest::default(),
    }]);
    assert_eq!(
        second_response_rx
            .recv_timeout(Duration::from_millis(50))
            .unwrap()
            .unwrap()
            .last_lsn(),
        Some(3)
    );
    // The segment sync workers are not running yet. Publication is pending, but the following
    // write still returns without waiting for physical I/O.
    assert!(matches!(
        coordinator.write_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    let sync_handle = thread::spawn(move || segment_syncer.run());
    let completed = coordinator
        .sync_done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    coordinator.commit_after_sync(completed).unwrap();
    assert_eq!(index.get_committed_lsn().unwrap(), 2);
    assert_eq!(index.get_next_lsn().unwrap(), 4);
    let synced_state = index.get_segment_state(1).unwrap().unwrap();
    assert_eq!(synced_state.state, SegmentFileState::Open);
    assert_eq!(synced_state.durable_offset, record_bytes);
    assert_eq!(index.get_segment_state(2).unwrap(), None);
    drop(coordinator);
    sync_handle.join().unwrap();
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
        store.sync().unwrap();
        wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
    }

    let store = try_open_standalone_store(cfg, StrataStoreMetrics::default()).unwrap();
    store.put(&key_3, b"x").unwrap();

    assert_eq!(lsm_blob_ref(&store, &key_3).segment_id, 2);
    assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
    assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));
    assert_eq!(store.get(&key_3).unwrap(), Some(b"x".to_vec()));
}
