use std::{num::NonZeroU32, sync::Once};

use strata_accounting::{AccountingIndex, AccountingIndexConfig};
use strata_core::{
    BlobLifecycle, BlobLifecycleAction, BlobLifecycleMergeOp, BlobLifecycleOp, BlobState,
    PlacementClass, RecordRef, SegmentFileState, ShardInfo, ShardKey, ShardState,
};
use tempfile::tempdir;
use typed_store::{DBMetrics, rocks::open_cf};

use super::*;

static INIT_TYPED_STORE_METRICS: Once = Once::new();

fn init_typed_store_metrics() {
    INIT_TYPED_STORE_METRICS.call_once(|| {
        DBMetrics::get();
    });
}

fn blob_entry(segment_id: SegmentId, offset: u64) -> BlobEntry {
    BlobEntry {
        record_ref: Some(RecordRef {
            segment_id,
            offset,
            len: 1,
        }),
        lsn: 1,
        generation: 1,
        state: BlobState::Live,
    }
}

fn version_key(key: &BlobKey, lsn: strata_core::StrataLsn) -> BlobVersionKey {
    BlobVersionKey {
        key: key.clone(),
        lsn,
    }
}

fn segment_state(segment_id: SegmentId) -> SegmentState {
    segment_state_for_shard(STANDALONE_SHARD, segment_id)
}

fn segment_state_for_shard(shard: ShardKey, segment_id: SegmentId) -> SegmentState {
    SegmentState {
        shard,
        segment_id,
        volume_id: 0,
        path: format!("{segment_id:06}.data"),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset: 128,
        durable_offset: 64,
        min_lsn: Some(1),
        max_lsn: Some(3),
        sealed_len: None,
        sealed_sha256: None,
    }
}

fn unaccounted(_shard: ShardKey, key: &BlobKey) -> BlobKey {
    key.clone()
}

fn put_version_state(index: &StrataIndex, key: &BlobKey, state: &VersionState) {
    let blob_state = BlobVersionState {
        versions: state.clone(),
        lifecycle: BlobLifecycleState::default(),
    };
    let mut batch = index.batch();
    batch
        .insert_batch(index.blob_versions(), [(key, &blob_state)])
        .unwrap();
    batch.write().unwrap();
}

fn compact_blob_versions(index: &StrataIndex) {
    index.blob_versions.flush().unwrap();
    let cf = index.blob_versions.cf().unwrap();
    index.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
}

fn gc_range(offset: u64, len: u64) -> SegmentGcRecordRange {
    SegmentGcRecordRange { offset, len }
}

fn gc_lifetime(logical_end_epoch: Epoch) -> BlobLifecycle {
    BlobLifecycle {
        logical_end_epoch,
        extension_count: 0,
    }
}

#[tokio::test]
async fn open_path_persists_blob_entry_across_reopen() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let entry = blob_entry(7, 128);

    {
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        index.put_blob_entry(&key, &entry).unwrap();
    }

    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
}

#[tokio::test]
async fn from_db_creates_missing_cfs() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let db = open_cf(
        dir.path(),
        None,
        unique_metric_conf("strata_index_test"),
        &["existing"],
    )
    .unwrap();

    let index = StrataIndex::from_db(db, "embedded").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let entry = blob_entry(1, 0);
    index.put_blob_entry(&key, &entry).unwrap();

    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
}

#[tokio::test]
async fn shard_info_persists_across_reopen() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let shard_id = 17;
    let info = ShardInfo {
        current_generation: 4,
        state: ShardState::Dropping,
    };

    {
        let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
        index.put_shard_info(shard_id, info).unwrap();
    }

    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    assert_eq!(index.get_shard_info(shard_id).unwrap(), Some(info));
    assert_eq!(index.iter_shards().unwrap(), vec![(shard_id, info)]);
}

#[tokio::test]
async fn batch_writes_across_index_cfs_atomically() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let entry = blob_entry(9, 256);
    let state = segment_state(9);
    let stats = SegmentStats {
        total_bytes: 1024,
        live_bytes: 256,
        live_ref_count: 1,
        ..Default::default()
    };
    let manifest = AccountingIndex::open(AccountingIndexConfig::new(
        dir.path().join("accounting-index"),
        NonZeroU32::new(1).unwrap(),
    ))
    .unwrap()
    .manifest()
    .clone();
    let shard_id = 11;
    let shard_info = ShardInfo::active(2);

    let mut batch = index.batch();
    index
        .put_blob_entry_batch(&mut batch, &key, &entry)
        .unwrap();
    index.put_segment_state_batch(&mut batch, &state).unwrap();
    index
        .put_segment_stats_batch(&mut batch, state.segment_id, &stats)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, entry.lsn, &key)
        .unwrap();
    index
        .put_shard_info_batch(&mut batch, shard_id, shard_info)
        .unwrap();
    index
        .put_accounting_index_manifest_batch(&mut batch, &manifest)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(entry.clone()));
    assert_eq!(
        index.get_segment_state(state.segment_id).unwrap(),
        Some(state)
    );
    assert_eq!(index.get_segment_stats(9).unwrap(), Some(stats));
    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![(entry.lsn, unaccounted(STANDALONE_SHARD, &key))]
    );
    assert_eq!(index.get_shard_info(shard_id).unwrap(), Some(shard_info));
    assert_eq!(
        index.get_accounting_index_manifest().unwrap(),
        Some(manifest)
    );
}

#[tokio::test]
async fn latest_blob_version_returns_highest_lsn_for_key() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut first = blob_entry(7, 128);
    first.lsn = 1;
    let mut second = blob_entry(7, 256);
    second.lsn = 2;
    index.put_blob_entry(&key, &first).unwrap();
    index.put_blob_entry(&key, &second).unwrap();

    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(second));
}

#[tokio::test]
async fn blob_versions_are_stored_as_packed_state() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut first = blob_entry(7, 128);
    first.lsn = 1;
    let mut second = blob_entry(7, 256);
    second.lsn = 2;

    let mut batch = index.batch();
    index
        .put_blob_version_batch(&mut batch, &key, &first)
        .unwrap();
    index
        .put_blob_version_batch(&mut batch, &key, &second)
        .unwrap();
    batch.write().unwrap();

    let state = index.get_blob_version_state(&key).unwrap().unwrap();
    assert_eq!(state.heads.len(), 0);
    assert_eq!(state.tail.len(), 2);
    assert_eq!(
        index.latest_blob_version(&key).unwrap(),
        Some((version_key(&key, 2), second.clone()))
    );
    assert_eq!(
        index.iter_blob_versions().unwrap(),
        vec![
            (version_key(&key, 1), first),
            (version_key(&key, 2), second)
        ]
    );
}

#[tokio::test]
async fn map_blob_ref_merge_rewrites_exact_payload_ref() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let shard = ShardKey {
        id: 7,
        generation: 1,
    };
    let mut entry = blob_entry(1, 10);
    entry.lsn = 3;
    let from = entry.record_ref.unwrap();
    let to = RecordRef {
        segment_id: 2,
        offset: 20,
        len: from.len,
    };

    let mut batch = index.batch();
    index
        .merge_blob_version_batch(&mut batch, &key, shard, &entry)
        .unwrap();
    index
        .map_blob_ref_batch(&mut batch, &key, shard, entry.lsn, from, to)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index
            .get_blob_version_for_shard(&version_key(&key, entry.lsn), shard)
            .unwrap()
            .unwrap()
            .record_ref,
        Some(to)
    );
}

#[tokio::test]
async fn segment_gc_overlay_merge_coalesces_dead_ranges() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();

    let mut batch = index.batch();
    index
        .merge_segment_gc_overlay_batch(
            &mut batch,
            7,
            vec![SegmentGcOverlayMergeOp::RetireBatch {
                ranges: vec![gc_range(10, 5), gc_range(15, 5), gc_range(40, 3)],
            }],
        )
        .unwrap();
    batch.write().unwrap();

    let overlay = index.get_segment_gc_overlay(7).unwrap().unwrap();
    assert_eq!(overlay.dead, vec![gc_range(10, 10), gc_range(40, 3)]);
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn segment_gc_overlay_retire_removes_lifetime_hint() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let range = gc_range(20, 8);
    let lifecycle = gc_lifetime(50);

    let mut batch = index.batch();
    index
        .merge_segment_gc_overlay_batch(
            &mut batch,
            7,
            vec![
                SegmentGcOverlayMergeOp::LifetimeBatch {
                    updates: vec![SegmentGcLifetimeUpdate {
                        range,
                        lifecycle: Some(lifecycle),
                    }],
                },
                SegmentGcOverlayMergeOp::RetireBatch {
                    ranges: vec![range],
                },
            ],
        )
        .unwrap();
    batch.write().unwrap();

    let overlay = index.get_segment_gc_overlay(7).unwrap().unwrap();
    assert_eq!(overlay.dead, vec![range]);
    assert!(overlay.lifetimes.is_empty());
}

#[tokio::test]
async fn segment_gc_overlay_lifetime_update_revives_dead_subrange() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let lifecycle = gc_lifetime(60);

    let mut batch = index.batch();
    index
        .merge_segment_gc_overlay_batch(
            &mut batch,
            7,
            vec![
                SegmentGcOverlayMergeOp::RetireBatch {
                    ranges: vec![gc_range(0, 100)],
                },
                SegmentGcOverlayMergeOp::LifetimeBatch {
                    updates: vec![SegmentGcLifetimeUpdate {
                        range: gc_range(20, 10),
                        lifecycle: Some(lifecycle),
                    }],
                },
            ],
        )
        .unwrap();
    batch.write().unwrap();

    let overlay = index.get_segment_gc_overlay(7).unwrap().unwrap();
    assert_eq!(overlay.dead, vec![gc_range(0, 20), gc_range(30, 70)]);
    assert_eq!(
        overlay.lifetimes,
        vec![SegmentGcLifetimeRange {
            range: gc_range(20, 10),
            lifecycle,
        }]
    );
}

#[tokio::test]
async fn blob_versions_pack_payload_and_lifecycle_state_together() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let entry = blob_entry(7, 128);

    let mut batch = index.batch();
    index
        .put_blob_version_batch(&mut batch, &key, &entry)
        .unwrap();
    index
        .apply_blob_lifecycle_merge_op_batch(
            &mut batch,
            &key,
            BlobLifecycleMergeOp::Append(BlobLifecycleOp {
                lsn: 2,
                action: BlobLifecycleAction::SetLifetime {
                    logical_end_epoch: 50,
                },
            }),
        )
        .unwrap();
    batch.write().unwrap();

    let state = index.get_blob_state(&key).unwrap().unwrap();
    assert_eq!(state.versions.tail.len(), 1);
    assert_eq!(state.lifecycle.tail.len(), 1);
    assert_eq!(
        index
            .resolve_blob_lifecycle_at(&key, StrataLsn::MAX)
            .unwrap()
            .lifetime
            .unwrap()
            .lifecycle
            .logical_end_epoch,
        50
    );
    assert_eq!(
        index.blob_ops_at_lsn(&key, 2).unwrap(),
        (
            Vec::new(),
            vec![BlobLifecycleOp {
                lsn: 2,
                action: BlobLifecycleAction::SetLifetime {
                    logical_end_epoch: 50,
                },
            }]
        )
    );
}

#[tokio::test]
async fn durable_lsn_advances_global_blob_version_compaction_frontier() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let shard = ShardKey {
        id: 42,
        generation: 7,
    };
    let mut first = blob_entry(7, 128);
    first.lsn = 1;
    let mut second = blob_entry(7, 256);
    second.lsn = 2;

    let mut batch = index.batch();
    index
        .merge_blob_version_batch(&mut batch, &key, shard, &first)
        .unwrap();
    index
        .merge_blob_version_batch(&mut batch, &key, shard, &second)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, first.lsn, &key)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, second.lsn, &key)
        .unwrap();
    batch.write().unwrap();

    let mut batch = index.batch();
    index.put_durable_lsn_batch(&mut batch, 2).unwrap();
    batch.write().unwrap();
    index.flush_wal(true).unwrap();
    index.set_blob_compact_safe_lsn(2);
    assert_eq!(index.get_durable_lsn().unwrap(), 2);
    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![(1, key.clone()), (2, key.clone())]
    );
    let state = index.get_blob_version_state(&key).unwrap().unwrap();
    assert_eq!(state.tail, Vec::new());
    let head = state.heads.get(&shard).unwrap();
    assert_eq!(head.head_lsn, 2);
    assert_eq!(head.payload_lsn, Some(2));
    assert_eq!(head.entry.record_ref, second.record_ref);

    assert_eq!(
        index.resolve_blob_head(&key, shard).unwrap().unwrap(),
        head.clone()
    );
}

#[tokio::test]
async fn blob_versions_compaction_filter_removes_dropped_shard_generation() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let dropped_shard = ShardKey {
        id: 5,
        generation: 0,
    };
    let kept_shard = ShardKey {
        id: 6,
        generation: 0,
    };
    let mixed_key = BlobKey::new(b"mixed-blob".to_vec()).unwrap();
    let dropped_only_key = BlobKey::new(b"dropped-only-blob".to_vec()).unwrap();
    let mut dropped_entry = blob_entry(1, 128);
    dropped_entry.lsn = 1;
    let mut kept_entry = blob_entry(2, 256);
    kept_entry.lsn = 1;

    index
        .put_shard_info(dropped_shard.id, ShardInfo::active(0))
        .unwrap();
    index
        .put_shard_info(kept_shard.id, ShardInfo::active(0))
        .unwrap();

    let mut mixed_state = VersionState::default();
    mixed_state.append_op(VersionOp {
        shard: dropped_shard,
        entry: dropped_entry.clone(),
    });
    mixed_state.append_op(VersionOp {
        shard: kept_shard,
        entry: kept_entry.clone(),
    });
    put_version_state(&index, &mixed_key, &mixed_state);

    let mut dropped_only_state = VersionState::default();
    dropped_only_state.append_op(VersionOp {
        shard: dropped_shard,
        entry: dropped_entry,
    });
    put_version_state(&index, &dropped_only_key, &dropped_only_state);

    index
        .put_shard_info(
            dropped_shard.id,
            ShardInfo {
                current_generation: dropped_shard.generation,
                state: ShardState::Dropped,
            },
        )
        .unwrap();
    compact_blob_versions(&index);

    assert_eq!(
        index.resolve_blob_head(&mixed_key, dropped_shard).unwrap(),
        None
    );
    assert_eq!(
        index
            .resolve_blob_head(&mixed_key, kept_shard)
            .unwrap()
            .unwrap()
            .entry,
        kept_entry
    );
    assert!(
        index
            .get_blob_version_state(&dropped_only_key)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn blob_versions_compaction_filter_removes_stale_shard_generation() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let stale_shard = ShardKey {
        id: 5,
        generation: 0,
    };
    let current_shard = ShardKey {
        id: 5,
        generation: 1,
    };
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut stale_entry = blob_entry(1, 128);
    stale_entry.lsn = 7;
    let mut current_entry = blob_entry(2, 256);
    current_entry.lsn = 1;

    index
        .put_shard_info(
            current_shard.id,
            ShardInfo::active(current_shard.generation),
        )
        .unwrap();

    let mut state = VersionState::default();
    state.heads.insert(
        stale_shard,
        ShardHead {
            head_lsn: stale_entry.lsn,
            payload_lsn: Some(stale_entry.lsn),
            entry: stale_entry,
        },
    );
    state.append_op(VersionOp {
        shard: current_shard,
        entry: current_entry.clone(),
    });
    put_version_state(&index, &key, &state);
    compact_blob_versions(&index);

    assert_eq!(index.resolve_blob_head(&key, stale_shard).unwrap(), None);
    assert_eq!(
        index
            .resolve_blob_head(&key, current_shard)
            .unwrap()
            .unwrap()
            .entry,
        current_entry
    );
}

#[tokio::test]
async fn removing_blob_versions_deletes_exact_rows_and_exposes_previous_version() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let mut first = blob_entry(7, 128);
    first.lsn = 1;
    let mut second = blob_entry(7, 256);
    second.lsn = 2;
    let mut third = blob_entry(7, 512);
    third.lsn = 3;
    index.put_blob_entry(&key, &first).unwrap();
    index.put_blob_entry(&key, &second).unwrap();
    index.put_blob_entry(&key, &third).unwrap();

    let mut batch = index.batch();
    index
        .remove_blob_versions_batch(&mut batch, &[(key.clone(), 2)])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_blob_version(&version_key(&key, 2)).unwrap(), None);
    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(third.clone()));

    let mut batch = index.batch();
    index
        .remove_blob_versions_batch(&mut batch, &[(key.clone(), 3)])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_blob_version(&version_key(&key, 3)).unwrap(), None);
    assert_eq!(index.get_blob_entry(&key).unwrap(), Some(first));
}

#[tokio::test]
async fn removing_blob_versions_is_scoped_to_shard() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let shard = ShardKey {
        id: 9,
        generation: 3,
    };
    let mut standalone = blob_entry(7, 128);
    standalone.lsn = 1;
    let mut shard_entry = blob_entry(8, 256);
    shard_entry.lsn = 1;
    shard_entry.generation = 2;

    let mut batch = index.batch();
    index
        .put_blob_version_batch(&mut batch, &key, &standalone)
        .unwrap();
    index
        .merge_blob_version_batch(&mut batch, &key, shard, &shard_entry)
        .unwrap();
    batch.write().unwrap();

    let mut batch = index.batch();
    index
        .remove_blob_versions_for_shard_batch(&mut batch, shard, &[(key.clone(), 1)])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.get_blob_version(&version_key(&key, 1)).unwrap(),
        Some(standalone)
    );
    assert_eq!(
        index
            .get_blob_version_for_shard(&version_key(&key, 1), shard)
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn iterates_segment_states() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let state_1 = segment_state(1);
    let state_2 = segment_state(2);

    index.put_segment_state(&state_2).unwrap();
    index.put_segment_state(&state_1).unwrap();

    let states = index.iter_segment_states().unwrap();
    assert_eq!(states, vec![(1, state_1), (2, state_2)]);
}

#[tokio::test]
async fn segment_state_and_stats_are_keyed_by_shard() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let shard = ShardKey {
        id: 5,
        generation: 2,
    };
    let standalone_state = segment_state(1);
    let shard_state = segment_state_for_shard(shard, 1);
    let standalone_stats = SegmentStats {
        total_bytes: 11,
        ..Default::default()
    };
    let shard_stats = SegmentStats {
        total_bytes: 22,
        ..Default::default()
    };

    let mut batch = index.batch();
    index
        .put_segment_state_batch(&mut batch, &standalone_state)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &shard_state)
        .unwrap();
    index
        .put_segment_stats_batch(&mut batch, 1, &standalone_stats)
        .unwrap();
    index
        .put_segment_stats_for_shard_batch(&mut batch, shard, 1, &shard_stats)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_segment_state(1).unwrap(), Some(standalone_state));
    assert_eq!(
        index.get_segment_state_for_shard(shard, 1).unwrap(),
        Some(shard_state)
    );
    assert_eq!(index.get_segment_stats(1).unwrap(), Some(standalone_stats));
    assert_eq!(
        index.get_segment_stats_for_shard(shard, 1).unwrap(),
        Some(shard_stats)
    );
}

#[tokio::test]
async fn store_state_fields_update_independently() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();

    assert_eq!(
        index.get_store_state().unwrap(),
        Some(StrataStoreState::default())
    );
    assert_eq!(index.get_current_epoch().unwrap(), None);

    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 42).unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_next_lsn().unwrap(), 42);
    assert_eq!(index.get_durable_lsn().unwrap(), 0);
    assert_eq!(index.get_accounted_lsn().unwrap(), 0);

    let mut batch = index.batch();
    index.put_durable_lsn_batch(&mut batch, 41).unwrap();
    index.put_accounted_lsn_batch(&mut batch, 40).unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.get_store_state().unwrap(),
        Some(StrataStoreState {
            next_lsn: 42,
            durable_lsn: 41,
            accounted_lsn: 40,
        })
    );
}

#[tokio::test]
async fn lsn_keyed_tables_are_store_global() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();

    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 2).unwrap();
    index.put_epoch_change_batch(&mut batch, 1, 42).unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 1, &key_a)
        .unwrap();
    batch.write().unwrap();

    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 3).unwrap();
    index.put_epoch_change_batch(&mut batch, 1, 43).unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 1, &key_b)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_next_lsn().unwrap(), 3);
    assert_eq!(index.latest_epoch_at_lsn(1).unwrap(), Some((1, 43)));
    assert_eq!(index.iter_epoch_changes_from(0).unwrap(), vec![(1, 43)]);
    assert_eq!(index.iter_unaccounted_lsn_ops().unwrap(), vec![(1, key_b)]);
}

#[tokio::test]
async fn removing_shard_keyed_metadata_skips_blob_versions() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let shard = ShardKey {
        id: 5,
        generation: 2,
    };
    let other_shard = ShardKey {
        id: 6,
        generation: 1,
    };
    let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let other_key = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let mut entry = blob_entry(1, 128);
    entry.lsn = 1;
    let shard_state = segment_state_for_shard(shard, 1);
    let other_state = segment_state_for_shard(other_shard, 1);
    let shard_stats = SegmentStats {
        total_bytes: 11,
        ..Default::default()
    };
    let other_stats = SegmentStats {
        total_bytes: 22,
        ..Default::default()
    };

    let mut batch = index.batch();
    index
        .merge_blob_version_batch(&mut batch, &key, shard, &entry)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &shard_state)
        .unwrap();
    index
        .put_segment_state_batch(&mut batch, &other_state)
        .unwrap();
    index
        .put_segment_stats_for_shard_batch(&mut batch, shard, 1, &shard_stats)
        .unwrap();
    index
        .put_segment_stats_for_shard_batch(&mut batch, other_shard, 1, &other_stats)
        .unwrap();
    index.put_next_lsn_batch(&mut batch, 5).unwrap();
    index.put_durable_lsn_batch(&mut batch, 4).unwrap();
    index.put_accounted_lsn_batch(&mut batch, 3).unwrap();
    index.put_current_epoch_batch(&mut batch, 8).unwrap();
    index.put_epoch_change_batch(&mut batch, 2, 8).unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 1, &key)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 2, &other_key)
        .unwrap();
    batch.write().unwrap();

    let mut batch = index.batch();
    index
        .remove_shard_keyed_metadata_batch(&mut batch, shard)
        .unwrap();
    batch.write().unwrap();

    assert!(
        index
            .get_segment_state_for_shard(shard, 1)
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .get_segment_stats_for_shard(shard, 1)
            .unwrap()
            .is_none()
    );
    assert_eq!(index.get_next_lsn().unwrap(), 5);
    assert_eq!(index.get_durable_lsn().unwrap(), 4);
    assert_eq!(index.get_accounted_lsn().unwrap(), 3);
    assert_eq!(index.get_current_epoch().unwrap(), Some(8));
    assert_eq!(index.iter_epoch_changes_from(0).unwrap(), vec![(2, 8)]);
    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![
            (1, unaccounted(shard, &key)),
            (2, unaccounted(other_shard, &other_key))
        ]
    );
    assert!(index.resolve_blob_head(&key, shard).unwrap().is_some());
    assert_eq!(
        index.get_segment_state_for_shard(other_shard, 1).unwrap(),
        Some(other_state)
    );
    assert_eq!(
        index.get_segment_stats_for_shard(other_shard, 1).unwrap(),
        Some(other_stats)
    );
}

#[tokio::test]
async fn epoch_changes_track_genesis_and_lsn_ordered_updates() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();

    assert_eq!(index.latest_epoch_at_lsn(StrataLsn::MAX).unwrap(), None);

    let mut batch = index.batch();
    index.put_epoch_change_batch(&mut batch, 0, 42).unwrap();
    index.put_current_epoch_batch(&mut batch, 42).unwrap();
    index.put_epoch_change_batch(&mut batch, 5, 43).unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_current_epoch().unwrap(), Some(42));
    assert_eq!(index.latest_epoch_at_lsn(0).unwrap(), Some((0, 42)));
    assert_eq!(index.latest_epoch_at_lsn(4).unwrap(), Some((0, 42)));
    assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((5, 43)));
    assert_eq!(index.iter_epoch_changes_from(1).unwrap(), vec![(5, 43)]);

    let mut batch = index.batch();
    index.remove_epoch_changes_batch(&mut batch, &[5]).unwrap();
    batch.write().unwrap();

    assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((0, 42)));
}

#[tokio::test]
async fn iterates_unaccounted_lsn_ops_in_order() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();

    let mut batch = index.batch();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 2, &key_2)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 3, &key_3)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, 1, &key_1)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![
            (1, unaccounted(STANDALONE_SHARD, &key_1)),
            (2, unaccounted(STANDALONE_SHARD, &key_2)),
            (3, unaccounted(STANDALONE_SHARD, &key_3))
        ]
    );

    let mut batch = index.batch();
    index
        .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![
            (1, unaccounted(STANDALONE_SHARD, &key_1)),
            (3, unaccounted(STANDALONE_SHARD, &key_3))
        ]
    );
}

#[tokio::test]
async fn unaccounted_lsn_ops_are_retained_until_explicitly_removed() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "strata").unwrap();
    let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
    let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
    let entry_1 = blob_entry(1, 0);
    let mut entry_2 = blob_entry(1, 1);
    entry_2.lsn = 2;
    entry_2.generation = 2;

    let mut batch = index.batch();
    index
        .put_blob_version_batch(&mut batch, &key_1, &entry_1)
        .unwrap();
    index
        .put_blob_version_batch(&mut batch, &key_2, &entry_2)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, entry_1.lsn, &key_1)
        .unwrap();
    index
        .put_blob_unaccounted_lsn_op_batch(&mut batch, entry_2.lsn, &key_2)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.iter_unaccounted_lsn_ops_from(2).unwrap(),
        vec![(2, unaccounted(STANDALONE_SHARD, &key_2))]
    );

    let mut batch = index.batch();
    index
        .remove_blob_versions_batch(&mut batch, &[(key_2.clone(), 2)])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![
            (1, unaccounted(STANDALONE_SHARD, &key_1)),
            (2, unaccounted(STANDALONE_SHARD, &key_2))
        ]
    );

    let mut batch = index.batch();
    index
        .remove_unaccounted_lsn_ops_batch(&mut batch, &[2])
        .unwrap();
    batch.write().unwrap();

    assert_eq!(
        index.iter_unaccounted_lsn_ops().unwrap(),
        vec![(1, unaccounted(STANDALONE_SHARD, &key_1))]
    );
}
