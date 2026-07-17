//! File-backed index for Strata accounting.
//!
//! This crate does not serve user reads. RocksDB can remain the current read index while this
//! index compaction materializes blob update history into ref events and GC overlay operands.
//! The physical shape is deliberately small:
//!
//! ```text
//! partition-N/
//!   base-*.run     compacted key -> materialized accounting state
//!   patch-*.run    newer key-grouped update summaries
//!   delta-*.run    sorted blob updates waiting to be materialized
//! ```
//!
//! The manifest is encoded by this crate but should be stored by the owner, normally in the same
//! RocksDB batch that publishes compaction events and GC state.
//!
//! Delta compaction groups pending updates for one hash partition into a new patch run without
//! reading existing base or patch state. It may emit events for refs whose complete lifetime is
//! visible inside those deltas. Major compaction streams base state and residual patch updates
//! together, then emits events for the final residual state it materializes into the next base run.

use std::{io, path::PathBuf};

mod active_log;
mod events;
mod index;
mod manifest;
mod run_io;
mod state;

pub type PartitionId = u32;
pub type RunId = u64;

pub(crate) const FORMAT_VERSION: u32 = 2;
pub(crate) const ACTIVE_DELTA_LOG_FORMAT_VERSION: u32 = 1;

pub use active_log::{
    AccountingLogDurablePosition, AccountingLogEntry, ActiveDeltaLog, ActiveDeltaLogPosition,
    ActiveDeltaLogReadCursor, GcMapRefEntry,
};
pub use events::{CompactionEventBatch, RefEvent, RetireReason, SegmentGcSummaryDelta};
pub use index::{
    AccountingIndex, AccountingIndexConfig, PreparedAccountingDeltas, PreparedDeltaCompaction,
    PreparedDeltaRuns, PreparedEpochChange, PreparedMajorCompaction, PreparedShardDrop,
};
pub use manifest::{
    EpochChange, Manifest, PartitionManifest, RunKind, RunMeta, ShardDrop, manifest_from_bytes,
    manifest_to_bytes,
};
pub use state::{
    BlobUpdate, LifecycleChange, LivePayload, MapRef, MaterializedBlobState, Tombstone,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "accounting index manifest has incompatible format version {actual}, expected {expected}"
    )]
    IncompatibleManifestVersion { actual: u32, expected: u32 },

    #[error("run {run_id} has kind {actual:?}, expected {expected:?}")]
    UnexpectedRunKind {
        run_id: RunId,
        actual: RunKind,
        expected: RunKind,
    },

    #[error("run {run_id} belongs to partition {actual}, expected {expected}")]
    UnexpectedRunPartition {
        run_id: RunId,
        actual: PartitionId,
        expected: PartitionId,
    },

    #[error("partition count changed from {stored} to {configured}")]
    PartitionCountChanged { stored: u32, configured: u32 },

    #[error(
        "prepared accounting index manifest was based on generation {prepared_from}, but current generation is {current}"
    )]
    StalePreparedManifest { current: u64, prepared_from: u64 },

    #[error("accounting index manifest generation overflowed at {generation}")]
    ManifestGenerationOverflow { generation: u64 },

    #[error("accounting LSN overflowed at base {base_lsn} plus offset {offset}")]
    LsnOverflow { base_lsn: u64, offset: u64 },

    #[error("partition {0} is outside configured partition count")]
    InvalidPartition(PartitionId),

    #[error("delta run does not contain any key-scoped updates")]
    EmptyDeltaRun,

    #[error(
        "shard drop {shard:?} at LSN {lsn} requires all accounting partitions to be major compacted"
    )]
    ShardDropRequiresCompaction {
        shard: strata_core::ShardKey,
        lsn: u64,
    },

    #[error("I/O error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },

    #[error("run file is corrupt at {path}: {reason}")]
    CorruptRun { path: PathBuf, reason: String },

    #[error("run record frame is too large: {len} bytes")]
    RunFrameTooLarge { len: usize },

    #[error("encoding error: {0}")]
    Encode(#[from] bcs::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, num::NonZeroU32};

    use strata_core::{
        BlobKey, BlobLifecycle, RecordRef, SegmentGcLiveRecord, SegmentGcOverlayMergeOp,
        SegmentGcRecordRange, SegmentId, ShardKey, StrataLsn,
    };
    use tempfile::tempdir;

    use super::*;

    const SHARD: ShardKey = ShardKey {
        id: 7,
        generation: 2,
    };
    const OTHER_SHARD: ShardKey = ShardKey {
        id: 8,
        generation: 4,
    };

    fn index(partitions: u32) -> (tempfile::TempDir, AccountingIndex) {
        let dir = tempdir().unwrap();
        let config = AccountingIndexConfig::new(dir.path(), NonZeroU32::new(partitions).unwrap());
        let index = AccountingIndex::open(config).unwrap();
        (dir, index)
    }

    fn key(bytes: &[u8]) -> BlobKey {
        BlobKey::new(bytes.to_vec()).unwrap()
    }

    fn record_ref(segment_id: SegmentId, offset: u64) -> RecordRef {
        RecordRef {
            segment_id,
            offset,
            len: 100,
        }
    }

    fn gc_range(record_ref: RecordRef) -> SegmentGcRecordRange {
        SegmentGcRecordRange::from(record_ref)
    }

    fn gc_live_op(
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> SegmentGcOverlayMergeOp {
        SegmentGcOverlayMergeOp::AddLiveBatch {
            records: vec![SegmentGcLiveRecord {
                range: gc_range(record_ref),
                lifecycle,
            }],
        }
    }

    fn gc_retire_op(record_ref: RecordRef) -> SegmentGcOverlayMergeOp {
        SegmentGcOverlayMergeOp::RetireBatch {
            ranges: vec![gc_range(record_ref)],
        }
    }

    fn put(lsn: StrataLsn, key: &BlobKey, record_ref: RecordRef) -> BlobUpdate {
        put_for_shard(lsn, key, SHARD, record_ref)
    }

    fn put_for_shard(
        lsn: StrataLsn,
        key: &BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
    ) -> BlobUpdate {
        BlobUpdate::Put {
            lsn,
            key: key.clone(),
            shard,
            record_ref,
            current_epoch: 42,
            lifecycle: None,
        }
    }

    #[test]
    fn active_delta_log_syncs_and_reopens() {
        let dir = tempdir().unwrap();
        let key = key(b"active-log");
        let state = {
            let mut log =
                ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default())
                    .unwrap();
            log.append(&AccountingLogEntry::Blob(put(1, &key, record_ref(10, 0))))
                .unwrap();
            log.append(&AccountingLogEntry::Epoch(EpochChange {
                lsn: 2,
                epoch: 43,
            }))
            .unwrap();
            log.sync_data().unwrap();
            log.durable_position()
        };

        assert_eq!(state.durable_lsn, 2);
        assert!(state.durable_offset > 0);

        let reopened = ActiveDeltaLog::open(dir.path(), state.segment_id, state).unwrap();
        assert_eq!(reopened.durable_position(), state);
    }

    #[test]
    fn active_delta_log_round_trips_shard_drop() {
        let dir = tempdir().unwrap();
        let state = {
            let mut log =
                ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default())
                    .unwrap();
            log.append(&AccountingLogEntry::ShardDropped {
                lsn: 7,
                shard: SHARD,
            })
            .unwrap();
            log.sync_data().unwrap();
            log.durable_position()
        };

        let read = ActiveDeltaLog::read_durable_range(
            dir.path(),
            ActiveDeltaLogReadCursor::default(),
            state,
        )
        .unwrap();
        assert_eq!(
            read.entries,
            vec![AccountingLogEntry::ShardDropped {
                lsn: 7,
                shard: SHARD,
            }]
        );
    }

    #[test]
    fn active_delta_log_reads_durable_range_from_cursor() {
        let dir = tempdir().unwrap();
        let key = key(b"active-log-read");
        let state = {
            let mut log =
                ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default())
                    .unwrap();
            log.append(&AccountingLogEntry::Blob(put(1, &key, record_ref(10, 0))))
                .unwrap();
            log.append(&AccountingLogEntry::Blob(put(2, &key, record_ref(11, 0))))
                .unwrap();
            log.sync_data().unwrap();
            log.durable_position()
        };

        let cursor = ActiveDeltaLogReadCursor::default();
        let read = ActiveDeltaLog::read_durable_range(dir.path(), cursor, state).unwrap();
        assert_eq!(read.entries.len(), 2);
        assert!(read.bytes_read > 0);
        assert_eq!(read.max_lsn, Some(2));

        let cursor = read.next_cursor(cursor);
        assert_eq!(cursor.segment_id, 1);
        assert_eq!(cursor.offset, state.durable_offset);
        assert_eq!(cursor.max_lsn, 2);
        let read = ActiveDeltaLog::read_durable_range(dir.path(), cursor, state).unwrap();
        assert!(read.entries.is_empty());
        assert_eq!(read.bytes_read, 0);
        assert_eq!(read.end_offset, state.durable_offset);
    }

    #[test]
    fn active_delta_log_reads_bulk_gc_map_ref_as_one_frame() {
        let dir = tempdir().unwrap();
        let key_a = key(b"gc-map-a");
        let key_b = key(b"gc-map-b");
        let state = {
            let mut log =
                ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default())
                    .unwrap();
            log.append(&AccountingLogEntry::GcMapRefBatch {
                base_lsn: 10,
                maps: vec![
                    GcMapRefEntry {
                        key: key_a.clone(),
                        from: record_ref(1, 0),
                        to: record_ref(3, 0),
                    },
                    GcMapRefEntry {
                        key: key_b.clone(),
                        from: record_ref(2, 0),
                        to: record_ref(3, 64),
                    },
                ],
            })
            .unwrap();
            log.sync_data().unwrap();
            log.durable_position()
        };

        assert_eq!(state.durable_lsn, 11);
        let read = ActiveDeltaLog::read_durable_range(
            dir.path(),
            ActiveDeltaLogReadCursor::default(),
            state,
        )
        .unwrap();
        assert_eq!(read.entries.len(), 1);
        assert_eq!(read.max_lsn, Some(11));
    }

    #[test]
    fn active_delta_log_rolls_back_failed_commit_append() {
        let dir = tempdir().unwrap();
        let key = key(b"rollback");
        let mut log =
            ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default()).unwrap();
        let before = log.position();

        log.append(&AccountingLogEntry::Blob(put(1, &key, record_ref(10, 0))))
            .unwrap();
        log.rollback_to(before).unwrap();
        log.sync_data().unwrap();

        assert_eq!(log.durable_position().durable_lsn, 0);
        assert!(log.durable_position().durable_offset > 0);
    }

    #[test]
    fn active_delta_log_truncates_recovered_uncommitted_tail() {
        let dir = tempdir().unwrap();
        let key = key(b"truncate");
        let state = {
            let mut log =
                ActiveDeltaLog::open(dir.path(), 1, AccountingLogDurablePosition::default())
                    .unwrap();
            log.append(&AccountingLogEntry::Blob(put(1, &key, record_ref(10, 0))))
                .unwrap();
            log.append(&AccountingLogEntry::Blob(put(2, &key, record_ref(11, 0))))
                .unwrap();
            log.truncate_after_lsn(1).unwrap();
            log.sync_data().unwrap();
            log.durable_position()
        };

        assert_eq!(state.durable_lsn, 1);
        let reopened = ActiveDeltaLog::open(dir.path(), state.segment_id, state).unwrap();
        assert_eq!(reopened.durable_position(), state);
    }

    #[test]
    fn active_delta_log_removes_selected_segments_and_syncs_directory() {
        let dir = tempdir().unwrap();
        for segment_id in 1..=3 {
            drop(
                ActiveDeltaLog::open(
                    dir.path(),
                    segment_id,
                    AccountingLogDurablePosition::default(),
                )
                .unwrap(),
            );
        }

        let removed =
            ActiveDeltaLog::remove_segments(dir.path(), &BTreeSet::from([1, 3, 4])).unwrap();

        assert_eq!(removed, 2);
        assert!(!ActiveDeltaLog::path(dir.path(), 1).exists());
        assert!(ActiveDeltaLog::path(dir.path(), 2).exists());
        assert!(!ActiveDeltaLog::path(dir.path(), 3).exists());
    }

    #[test]
    fn appends_delta_runs_by_partition() {
        let (_dir, mut index) = index(4);
        let key_a = key(b"a");
        let key_b = key(b"b");
        let partition_a = index.partition_for_key(&key_a);
        let partition_b = index.partition_for_key(&key_b);

        index
            .append_delta_run(vec![
                put(1, &key_a, record_ref(1, 0)),
                put(2, &key_b, record_ref(2, 0)),
            ])
            .unwrap();

        assert_eq!(
            index
                .manifest()
                .partitions
                .get(&partition_a)
                .unwrap()
                .deltas
                .len(),
            1
        );
        assert_eq!(
            index
                .manifest()
                .partitions
                .get(&partition_b)
                .unwrap()
                .deltas
                .len(),
            1
        );
    }

    #[test]
    fn delta_compaction_emits_closed_local_ref_events() {
        let (_dir, mut index) = index(1);
        let key = key(b"hot-key");
        let first_ref = record_ref(10, 0);
        let second_ref = record_ref(11, 0);

        index
            .append_delta_run(vec![
                put(1, &key, first_ref),
                BlobUpdate::SetLifetime {
                    lsn: 2,
                    key: key.clone(),
                    logical_end_epoch: 50,
                    current_epoch: 42,
                },
                put(3, &key, second_ref),
                BlobUpdate::Tombstone {
                    lsn: 4,
                    key: key.clone(),
                },
            ])
            .unwrap();

        let batch = index.compact_delta_runs(0).unwrap();

        assert_eq!(batch.max_lsn, 4);
        assert_eq!(batch.events.len(), 5);
        assert!(matches!(batch.events[0], RefEvent::Live { .. }));
        assert!(matches!(batch.events[1], RefEvent::LifecycleChanged { .. }));
        assert!(matches!(
            batch.events[2],
            RefEvent::Retired {
                reason: RetireReason::Overwritten,
                ..
            }
        ));
        assert!(matches!(batch.events[3], RefEvent::Live { .. }));
        assert!(matches!(
            batch.events[4],
            RefEvent::Retired {
                reason: RetireReason::Tombstoned,
                ..
            }
        ));
        let lifecycle = BlobLifecycle::new(50);
        assert_eq!(
            batch.segment_gc_overlay_ops.get(&first_ref.segment_id),
            Some(&vec![
                gc_live_op(first_ref, None),
                SegmentGcOverlayMergeOp::LifetimeBatch {
                    updates: vec![strata_core::SegmentGcLifetimeUpdate {
                        range: gc_range(first_ref),
                        lifecycle: Some(lifecycle),
                    }],
                },
                gc_retire_op(first_ref)
            ])
        );
        assert_eq!(
            batch.segment_gc_overlay_ops.get(&second_ref.segment_id),
            Some(&vec![
                gc_live_op(second_ref, Some(lifecycle)),
                gc_retire_op(second_ref)
            ])
        );

        let prepared = index.prepare_major_compact_partition(0).unwrap();
        let major_batch = prepared.event_batch.clone();
        index.apply_prepared_major_compaction(prepared).unwrap();

        assert_eq!(major_batch.max_lsn, 4);
        assert!(major_batch.events.is_empty());

        let state = index.materialized_state(&key).unwrap().unwrap();
        assert!(!state.is_live());
        assert_eq!(state.tombstone, Some(Tombstone { lsn: 4 }));
        assert!(
            index
                .manifest()
                .partitions
                .get(&0)
                .unwrap()
                .deltas
                .is_empty()
        );
        assert!(
            index
                .manifest()
                .partitions
                .get(&0)
                .unwrap()
                .patches
                .is_empty()
        );
        assert!(index.manifest().partitions.get(&0).unwrap().base.is_some());
    }

    #[test]
    fn delta_compaction_merges_runs_by_key_then_lsn() {
        let (_dir, mut index) = index(1);
        let key = key(b"delta-merge-order");
        let first_ref = record_ref(10, 0);
        let final_ref = record_ref(30, 0);

        index
            .append_delta_run(vec![put(10, &key, first_ref), put(30, &key, final_ref)])
            .unwrap();
        index
            .append_delta_run(vec![BlobUpdate::Tombstone {
                lsn: 20,
                key: key.clone(),
            }])
            .unwrap();

        let batch = index.compact_delta_runs(0).unwrap();
        let state = index.materialized_state(&key).unwrap().unwrap();

        assert_eq!(batch.max_lsn, 30);
        assert_eq!(batch.events.len(), 2);
        assert!(
            matches!(batch.events[0], RefEvent::Live { lsn: 10, record_ref, .. } if record_ref == first_ref)
        );
        assert!(
            matches!(batch.events[1], RefEvent::Retired { lsn: 20, record_ref, reason: RetireReason::Tombstoned, .. } if record_ref == first_ref)
        );
        assert_eq!(state.payloads.get(&SHARD).unwrap().record_ref, final_ref);
    }

    #[test]
    fn accounting_keeps_same_key_payloads_live_in_independent_shards() {
        let (_dir, mut index) = index(1);
        let key = key(b"shared-key");
        let first_ref = record_ref(10, 0);
        let other_ref = record_ref(20, 0);
        let replacement_ref = record_ref(11, 0);

        index
            .append_delta_run(vec![
                put_for_shard(1, &key, SHARD, first_ref),
                put_for_shard(2, &key, OTHER_SHARD, other_ref),
            ])
            .unwrap();
        let shallow = index.compact_delta_runs(0).unwrap();
        assert!(shallow.events.is_empty());
        let prepared = index.prepare_major_compact_partition(0).unwrap();
        let initial = prepared.event_batch.clone();
        index.apply_prepared_major_compaction(prepared).unwrap();
        assert_eq!(
            initial
                .events
                .iter()
                .filter(|event| matches!(event, RefEvent::Live { .. }))
                .count(),
            2
        );

        let state = index.materialized_state(&key).unwrap().unwrap();
        assert_eq!(state.payloads.len(), 2);
        assert_eq!(state.payloads.get(&SHARD).unwrap().record_ref, first_ref);
        assert_eq!(
            state.payloads.get(&OTHER_SHARD).unwrap().record_ref,
            other_ref
        );

        index
            .append_delta_run(vec![put_for_shard(3, &key, SHARD, replacement_ref)])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        let prepared = index.prepare_major_compact_partition(0).unwrap();
        let replacement = prepared.event_batch.clone();
        index.apply_prepared_major_compaction(prepared).unwrap();
        assert!(replacement.events.iter().any(
            |event| matches!(event, RefEvent::Retired { shard, record_ref, .. } if *shard == SHARD && *record_ref == first_ref)
        ));

        let state = index.materialized_state(&key).unwrap().unwrap();
        assert_eq!(state.payloads.len(), 2);
        assert_eq!(
            state.payloads.get(&SHARD).unwrap().record_ref,
            replacement_ref
        );
        assert_eq!(
            state.payloads.get(&OTHER_SHARD).unwrap().record_ref,
            other_ref
        );
    }

    #[test]
    fn shard_drop_retires_ingest_payloads_without_retiring_shard_owned_payloads() {
        let (_dir, mut index) = index(1);
        let ingest_key = key(b"drop-ingest");
        let retention_key = key(b"drop-retention");
        let ingest_ref = record_ref(10, 0);
        let retention_source = record_ref(11, 0);
        let retention_ref = record_ref(20, 0);

        index
            .append_delta_run(vec![
                put_for_shard(1, &ingest_key, SHARD, ingest_ref),
                put_for_shard(2, &retention_key, SHARD, retention_source),
                BlobUpdate::MapRef {
                    lsn: 3,
                    key: retention_key.clone(),
                    from: retention_source,
                    to: retention_ref,
                },
            ])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        index.major_compact_partition(0).unwrap();

        let prepared = index
            .prepare_accounting_deltas(vec![AccountingLogEntry::ShardDropped {
                lsn: 4,
                shard: SHARD,
            }])
            .unwrap();
        index.apply_prepared_accounting_deltas(prepared).unwrap();
        let drop = index.pending_shard_drops()[0];
        let prepared = index.prepare_materialize_shard_drop(drop).unwrap();

        assert!(prepared.event_batch.events.iter().any(
            |event| matches!(event, RefEvent::Retired { record_ref, reason: RetireReason::ShardDropped, .. } if *record_ref == ingest_ref)
        ));
        assert!(!prepared.event_batch.events.iter().any(
            |event| matches!(event, RefEvent::Retired { record_ref, .. } if *record_ref == retention_ref)
        ));
        index.apply_prepared_shard_drop(prepared).unwrap();

        assert!(
            index
                .materialized_state(&ingest_key)
                .unwrap()
                .unwrap()
                .payloads
                .is_empty()
        );
        assert!(
            index
                .materialized_state(&retention_key)
                .unwrap()
                .unwrap()
                .payloads
                .is_empty()
        );
        assert!(index.pending_shard_drops().is_empty());
    }

    #[test]
    fn final_state_patch_retires_base_at_residual_tombstone_lsn() {
        let (_dir, mut index) = index(1);
        let key = key(b"base-final-state");
        let base_ref = record_ref(9, 0);
        let transient_ref = record_ref(10, 0);

        index
            .append_delta_run(vec![put(1, &key, base_ref)])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        index.major_compact_partition(0).unwrap();

        index
            .append_delta_run(vec![
                put(2, &key, transient_ref),
                BlobUpdate::Tombstone {
                    lsn: 3,
                    key: key.clone(),
                },
            ])
            .unwrap();
        let batch = index.compact_delta_runs(0).unwrap();
        assert!(
            matches!(batch.events[0], RefEvent::Live { record_ref, .. } if record_ref == transient_ref)
        );
        assert!(
            matches!(batch.events[1], RefEvent::Retired { record_ref, reason: RetireReason::Tombstoned, .. } if record_ref == transient_ref)
        );

        let prepared = index.prepare_major_compact_partition(0).unwrap();
        let major_batch = prepared.event_batch.clone();
        index.apply_prepared_major_compaction(prepared).unwrap();

        assert_eq!(major_batch.events.len(), 1);
        assert!(
            matches!(major_batch.events[0], RefEvent::Retired { lsn: 3, record_ref, reason: RetireReason::Tombstoned, .. } if record_ref == base_ref)
        );
        assert_eq!(
            index.materialized_state(&key).unwrap().unwrap().tombstone,
            Some(Tombstone { lsn: 3 })
        );
    }

    #[test]
    fn current_state_includes_uncompacted_deltas() {
        let (_dir, mut index) = index(1);
        let key = key(b"current");

        index
            .append_delta_run(vec![put(1, &key, record_ref(10, 0))])
            .unwrap();

        assert!(index.materialized_state(&key).unwrap().is_none());
        assert!(index.current_state(&key).unwrap().unwrap().is_live());
    }

    #[test]
    fn prepared_compaction_publish_is_external() {
        let (dir, mut index) = index(1);
        let config = AccountingIndexConfig::new(dir.path(), NonZeroU32::new(1).unwrap());
        let key = key(b"prepared");

        index
            .append_delta_run(vec![put(1, &key, record_ref(10, 0))])
            .unwrap();
        let prepared = index.prepare_delta_compaction(0).unwrap();
        let manifest = prepared.manifest_bytes().unwrap();

        assert!(index.materialized_state(&key).unwrap().is_none());
        assert!(
            AccountingIndex::open_from_manifest_bytes(config, Some(&manifest))
                .unwrap()
                .materialized_state(&key)
                .unwrap()
                .unwrap()
                .is_live()
        );

        index.apply_prepared_delta_compaction(prepared).unwrap();
        assert!(index.materialized_state(&key).unwrap().unwrap().is_live());
    }

    #[test]
    fn stale_prepared_manifest_is_rejected() {
        let (_dir, mut index) = index(1);
        let key = key(b"stale-prepared");
        let stale = index
            .prepare_delta_runs(vec![put(1, &key, record_ref(10, 0))])
            .unwrap();

        index
            .record_epoch_change(EpochChange { lsn: 2, epoch: 43 })
            .unwrap();

        assert!(matches!(
            index.apply_prepared_delta_runs(stale),
            Err(Error::StalePreparedManifest {
                current: 1,
                prepared_from: 0
            })
        ));
        assert_eq!(index.manifest().generation, 1);
        assert!(
            index
                .manifest()
                .epoch_changes
                .contains(&EpochChange { lsn: 2, epoch: 43 })
        );
    }

    #[test]
    fn compaction_keeps_unresolved_map_ref_pending() {
        let (_dir, mut index) = index(1);
        let key = key(b"mapped-later");
        let from = record_ref(10, 0);
        let to = record_ref(20, 0);

        index
            .append_delta_run(vec![BlobUpdate::MapRef {
                lsn: 7,
                key: key.clone(),
                from,
                to,
            }])
            .unwrap();

        let batch = index.compact_delta_runs(0).unwrap();
        let state = index.materialized_state(&key).unwrap().unwrap();

        assert!(batch.events.is_empty());
        assert!(state.payloads.is_empty());
        assert_eq!(state.pending_maps, vec![MapRef { lsn: 7, from, to }]);
    }

    #[test]
    fn pending_map_ref_applies_when_source_payload_materializes() {
        let (_dir, mut index) = index(1);
        let key = key(b"mapped-later");
        let from = record_ref(10, 0);
        let to = record_ref(20, 0);

        index
            .append_delta_run(vec![BlobUpdate::MapRef {
                lsn: 7,
                key: key.clone(),
                from,
                to,
            }])
            .unwrap();
        index.compact_delta_runs(0).unwrap();

        index.append_delta_run(vec![put(8, &key, from)]).unwrap();
        let batch = index.compact_delta_runs(0).unwrap();
        assert!(batch.events.is_empty());

        let prepared = index.prepare_major_compact_partition(0).unwrap();
        let major_batch = prepared.event_batch.clone();
        index.apply_prepared_major_compaction(prepared).unwrap();
        let state = index.materialized_state(&key).unwrap().unwrap();

        assert_eq!(state.payloads.get(&SHARD).unwrap().record_ref, to);
        assert!(state.pending_maps.is_empty());
        assert!(
            matches!(major_batch.events[0], RefEvent::Live { record_ref, .. } if record_ref == from)
        );
        assert!(
            matches!(major_batch.events[1], RefEvent::Mapped { from: mapped_from, to: mapped_to, .. } if mapped_from == from && mapped_to == to)
        );
        assert_eq!(
            major_batch.segment_gc_overlay_ops.get(&from.segment_id),
            Some(&vec![gc_live_op(from, None), gc_retire_op(from)])
        );
        assert_eq!(
            major_batch.segment_gc_overlay_ops.get(&to.segment_id),
            Some(&vec![gc_live_op(to, None)])
        );
    }

    #[test]
    fn patch_compaction_does_not_rewrite_base_until_major_compaction() {
        let (_dir, mut index) = index(1);
        let key_a = key(b"a");
        let key_b = key(b"b");

        index
            .append_delta_run(vec![
                put(1, &key_a, record_ref(10, 0)),
                put(2, &key_b, record_ref(10, 100)),
            ])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        let base = index.major_compact_partition(0).unwrap().unwrap();

        index
            .append_delta_run(vec![put(3, &key_a, record_ref(11, 0))])
            .unwrap();
        index.compact_delta_runs(0).unwrap();

        let partition = index.manifest().partitions.get(&0).unwrap();
        assert_eq!(partition.base.as_ref().unwrap().id, base.id);
        assert_eq!(partition.patches.len(), 1);
        assert_eq!(
            index
                .materialized_state(&key_b)
                .unwrap()
                .unwrap()
                .payloads
                .get(&SHARD)
                .unwrap()
                .record_ref,
            record_ref(10, 100)
        );
    }

    #[test]
    fn major_compaction_keeps_newest_patch_state_for_duplicate_key() {
        let (_dir, mut index) = index(1);
        let key = key(b"duplicate");

        index
            .append_delta_run(vec![put(1, &key, record_ref(10, 0))])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        index.major_compact_partition(0).unwrap();

        index
            .append_delta_run(vec![put(2, &key, record_ref(11, 0))])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        index
            .append_delta_run(vec![put(3, &key, record_ref(12, 0))])
            .unwrap();
        index.compact_delta_runs(0).unwrap();
        index.major_compact_partition(0).unwrap();

        let state = index.materialized_state(&key).unwrap().unwrap();
        assert_eq!(
            state.payloads.get(&SHARD).unwrap().record_ref,
            record_ref(12, 0)
        );
        let partition = index.manifest().partitions.get(&0).unwrap();
        assert!(partition.patches.is_empty());
        assert_eq!(partition.base.as_ref().unwrap().max_lsn, Some(3));
    }

    #[test]
    fn reopens_manifest_and_runs() {
        let dir = tempdir().unwrap();
        let config = AccountingIndexConfig::new(dir.path(), NonZeroU32::new(1).unwrap());
        let key = key(b"persisted");
        let manifest = {
            let mut index = AccountingIndex::open(config.clone()).unwrap();
            index
                .append_delta_run(vec![put(1, &key, record_ref(10, 0))])
                .unwrap();
            index.compact_delta_runs(0).unwrap();
            index.manifest_bytes().unwrap()
        };

        let reopened = AccountingIndex::open_from_manifest_bytes(config, Some(&manifest)).unwrap();
        assert!(
            reopened
                .materialized_state(&key)
                .unwrap()
                .unwrap()
                .is_live()
        );
    }

    #[test]
    fn records_epoch_changes_in_manifest() {
        let (_dir, mut index) = index(1);

        index
            .record_epoch_change(EpochChange { lsn: 10, epoch: 42 })
            .unwrap();

        assert_eq!(
            index.manifest().epoch_changes,
            vec![EpochChange { lsn: 10, epoch: 42 }]
        );
    }
}
