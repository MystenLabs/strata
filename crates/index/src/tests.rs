use std::{collections::BTreeMap, num::NonZeroU32};

use core_types::{
    PlacementClass, SegmentFileState, SegmentGcSummary, SegmentOwner, ShardCleanupJob,
    ShardCleanupState, ShardInfo, ShardKey, ShardState,
};
use core_types::{StoreCheckpoint, WalPosition};
use lsm::{Manifest, ManifestEdit, TableMeta};
use tempfile::tempdir;
use typed_store::rocks::open_cf;

use super::*;

fn open_test_index(dir: &tempfile::TempDir) -> StrataIndex {
    StrataIndex::open_path(dir.path(), "strata", dir.path().display().to_string()).unwrap()
}

fn segment_state(segment_id: SegmentId) -> SegmentState {
    SegmentState {
        owner: SegmentOwner::Store,
        segment_id,
        volume_id: 0,
        path: format!("{segment_id:06}.data"),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset: 128,
        durable_offset: 64,
        min_lsn: Some(1),
        max_lsn: Some(3),
        sealed_before_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

fn lsm_table(id: u64, path: &str) -> TableMeta {
    TableMeta {
        id,
        partition: 0,
        relative_path: path.to_owned(),
        first_key: b"a".to_vec(),
        last_key: b"z".to_vec(),
        min_lsn: None,
        max_lsn: None,
        merge_applied_through_lsn: None,
        record_count: 1,
        file_len: 100,
        checksum: [id as u8; 32],
    }
}

#[tokio::test]
async fn open_path_persists_live_metadata_across_reopen() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let state = segment_state(7);
    {
        let index = open_test_index(&dir);
        index.put_segment_state(&state).unwrap();
        index
            .put_shard_info(
                17,
                ShardInfo {
                    current_generation: 4,
                    state: ShardState::Dropped,
                },
            )
            .unwrap();
    }

    let index = open_test_index(&dir);
    assert_eq!(index.get_segment_state(7).unwrap(), Some(state));
    assert_eq!(
        index.get_shard_info(17).unwrap(),
        Some(ShardInfo {
            current_generation: 4,
            state: ShardState::Dropped,
        })
    );
}

#[tokio::test]
async fn from_db_creates_only_the_live_column_families() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let db = open_cf(
        dir.path(),
        None,
        metric_conf_with_suffix("strata_index_test", dir.path().display().to_string()),
        &["existing"],
    )
    .unwrap();

    let index = StrataIndex::from_db(db, "embedded").unwrap();
    for name in index.cf_names().as_strs() {
        assert!(index.db().cf_handle(name).is_some(), "missing {name}");
    }
    assert_eq!(index.cf_names().as_strs().len(), 11);
}

#[tokio::test]
async fn shard_cleanup_jobs_are_independent_metadata() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    let shard = ShardKey {
        id: 17,
        generation: 4,
    };
    let mut batch = index.batch();
    index
        .put_shard_cleanup_job_batch(
            &mut batch,
            ShardCleanupJob {
                shard,
                drop_lsn: 42,
                state: ShardCleanupState::PendingMaterialization,
            },
        )
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let expected = ShardCleanupJob {
        shard,
        drop_lsn: 42,
        state: ShardCleanupState::ReadyForGc,
    };
    assert_eq!(index.get_shard_cleanup_job(shard).unwrap(), Some(expected));
    assert_eq!(index.iter_shard_cleanup_jobs().unwrap(), vec![expected]);

    let mut batch = index.batch();
    index
        .delete_shard_cleanup_job_batch(&mut batch, shard)
        .unwrap();
    batch.write_with_sync(true).unwrap();
    assert!(index.get_shard_cleanup_job(shard).unwrap().is_none());
}

#[tokio::test]
async fn gc_snapshot_uses_epoch_shards_segments_and_summaries() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    assert!(index.build_gc_snapshot().unwrap().is_none());

    let live = segment_state(1);
    let dropped_shard = ShardKey {
        id: 44,
        generation: 0,
    };
    let mut dropped = segment_state(2);
    dropped.owner = SegmentOwner::Shard(dropped_shard);
    let summary = SegmentGcSummary {
        total_bytes: 1_000,
        live_bytes: 100,
        retired_bytes: 900,
        live_ref_count: 1,
        ..Default::default()
    };
    let mut batch = index.batch();
    index.put_current_epoch_batch(&mut batch, 10).unwrap();
    index.put_commit_lsn_batch(&mut batch, 7).unwrap();
    index
        .put_shard_info_batch(
            &mut batch,
            dropped_shard.id,
            ShardInfo {
                current_generation: dropped_shard.generation,
                state: ShardState::Dropped,
            },
        )
        .unwrap();
    index.put_segment_state_batch(&mut batch, &live).unwrap();
    index.put_segment_state_batch(&mut batch, &dropped).unwrap();
    index
        .put_segment_gc_summary_batch(&mut batch, live.segment_id, &summary)
        .unwrap();
    batch.write().unwrap();

    let snapshot = index.build_gc_snapshot().unwrap().unwrap();
    assert_eq!(snapshot.current_epoch, 10);
    assert_eq!(snapshot.expiry_accounted_epoch, None);
    assert_eq!(snapshot.lifecycle_accounted_lsn, None);
    assert_eq!(snapshot.published_lsn, 7);
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(snapshot.segments[0].state, live);
    assert_eq!(snapshot.segments[0].summary, summary);

    // The durable frontier is an LSN, while the planner consumes an epoch. Both rows are read from
    // one RocksDB snapshot: accounting through LSN 5 includes epoch 9 at LSN 4 but not epoch 10 at
    // LSN 6, even though CurrentEpoch is already 10.
    let mut batch = index.batch();
    index.put_epoch_change_batch(&mut batch, 4, 9).unwrap();
    index.put_epoch_change_batch(&mut batch, 6, 10).unwrap();
    index
        .put_blob_expiry_accounted_lsn_batch(&mut batch, 5)
        .unwrap();
    batch.write().unwrap();
    let snapshot = index.build_gc_snapshot().unwrap().unwrap();
    assert_eq!(snapshot.expiry_accounted_epoch, Some(9));
    assert_eq!(snapshot.lifecycle_accounted_lsn, Some(5));
}

#[tokio::test]
async fn gc_reclaim_pending_rows_survive_until_removed() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    let mut batch = index.batch();
    index
        .put_gc_reclaim_pending_batch(&mut batch, 1, 4, 10)
        .unwrap();
    index
        .put_gc_reclaim_pending_batch(&mut batch, 1, 6, 20)
        .unwrap();
    index
        .put_gc_reclaim_pending_batch(&mut batch, 2, 6, 30)
        .unwrap();
    batch.write().unwrap();

    let mut batch = index.batch();
    assert_eq!(
        index
            .remove_gc_reclaim_pending_from_lsn_batch(&mut batch, 5)
            .unwrap(),
        2
    );
    batch.write().unwrap();
    assert_eq!(index.iter_gc_reclaim_pending().unwrap(), vec![((1, 4), 10)]);

    let mut batch = index.batch();
    assert_eq!(
        index
            .remove_gc_reclaim_pending_for_sources_batch(&mut batch, &[1])
            .unwrap(),
        BTreeMap::from([(1, 10)])
    );
    batch.write().unwrap();
    assert!(index.iter_gc_reclaim_pending().unwrap().is_empty());
}

#[tokio::test]
async fn segment_state_and_publication_lsn_round_trip() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    let first = segment_state(1);
    let second = segment_state(2);
    index.put_segment_state(&second).unwrap();
    index.put_segment_state(&first).unwrap();
    assert_eq!(
        index.iter_segment_states().unwrap(),
        vec![(1, first), (2, second)]
    );
    assert_eq!(index.get_segment_published_at_lsn(1).unwrap(), 0);

    let mut batch = index.batch();
    index
        .put_segment_published_at_lsn_batch(&mut batch, 1, 42)
        .unwrap();
    batch.write().unwrap();
    assert_eq!(index.get_segment_published_at_lsn(1).unwrap(), 42);
}

#[tokio::test]
async fn store_frontiers_and_checkpoint_round_trip() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    assert_eq!(index.get_next_lsn().unwrap(), 1);
    assert_eq!(index.get_published_lsn().unwrap(), 0);
    assert_eq!(index.get_store_wal_retained_from().unwrap(), None);

    let checkpoint = StoreCheckpoint {
        wal_position: WalPosition {
            log_id: 3,
            offset: 8192,
        },
        active_segment_id: 7,
        active_segment_offset: 4096,
    };
    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 42).unwrap();
    index.put_commit_lsn_batch(&mut batch, 41).unwrap();
    index
        .put_store_wal_retained_from_batch(&mut batch, 3)
        .unwrap();
    index
        .put_store_checkpoint_batch(&mut batch, checkpoint)
        .unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_next_lsn().unwrap(), 42);
    assert_eq!(index.get_published_lsn().unwrap(), 41);
    assert_eq!(index.get_store_wal_retained_from().unwrap(), Some(3));
    assert_eq!(index.get_store_checkpoint().unwrap(), Some(checkpoint));
}

#[tokio::test]
async fn epoch_changes_track_genesis_and_ordered_updates() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    let mut batch = index.batch();
    index.put_epoch_change_batch(&mut batch, 0, 42).unwrap();
    index.put_current_epoch_batch(&mut batch, 43).unwrap();
    index.put_epoch_change_batch(&mut batch, 5, 43).unwrap();
    batch.write().unwrap();

    assert_eq!(index.get_current_epoch().unwrap(), Some(43));
    assert_eq!(index.latest_epoch_at_lsn(4).unwrap(), Some((0, 42)));
    assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((5, 43)));
    assert_eq!(index.iter_epoch_changes_from(1).unwrap(), vec![(5, 43)]);

    let mut batch = index.batch();
    index.remove_epoch_changes_batch(&mut batch, &[5]).unwrap();
    batch.write().unwrap();
    assert_eq!(index.latest_epoch_at_lsn(5).unwrap(), Some((0, 42)));
}

#[tokio::test]
async fn lsm_manifest_edits_share_an_atomic_metadata_batch() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();
    let index = open_test_index(&dir);
    let mut manifest = Manifest::empty(
        "store-base-v1",
        "store-patch-v1",
        NonZeroU32::new(1).unwrap(),
    );
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![lsm_table(1, "A.sst")],
            add_patches: Vec::new(),
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let edit = ManifestEdit {
        remove: vec!["A.sst".to_owned()],
        add_base: vec![lsm_table(2, "B.sst")],
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    };
    let state = segment_state(17);

    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(&mut batch, "blob", &manifest)
        .unwrap();
    batch.write_with_sync(true).unwrap();
    let mut batch = index.batch();
    index
        .merge_lsm_manifest_batch(&mut batch, "blob", &edit)
        .unwrap();
    index.put_segment_state_batch(&mut batch, &state).unwrap();
    batch.write_with_sync(true).unwrap();

    manifest.apply(&edit).unwrap();
    assert_eq!(index.get_lsm_manifest("blob").unwrap(), Some(manifest));
    assert_eq!(index.get_segment_state(17).unwrap(), Some(state));
}
