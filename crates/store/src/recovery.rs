use std::{collections::BTreeSet, fs, path::Path};

use core_types::{
    SegmentFileState, SegmentGcSummary, SegmentId, SegmentState, ShardInfo, ShardState,
    StoreCheckpoint, StrataLsn, WalPosition,
};
use index::StrataIndex;
use segment::SegmentScanner;

use crate::{
    Error, INGEST_SEGMENT_OWNER, Result, StrataRecoveryPolicy, StrataStoreConfig,
    StrataStoreMetrics,
    layout::{parse_segment_file_name, segment_path},
    open::store_wal_recovery_state,
    segment_state::{
        active_segment_state_from_path, publish_segment_allocation_delta,
        unsealed_ingest_segment_ids,
    },
    wal::Wal,
};

/// Deletes (or, under AbsoluteConsistency, reports) segment files that have no index state.
///
/// An orphan can only mean one thing: a rollover crashed after creating the file but before the
/// index batch committed, so no reference to it ever existed. It must be removed *before* any
/// writer starts, because the writer picks segment ids by incrementing past the indexed maximum
/// and would otherwise happily reuse the orphan's id with stale bytes already in the file.
pub(crate) fn reconcile_orphan_ingest_segment_files(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    let indexed_segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .collect::<BTreeSet<_>>();
    let ingest_dir = config.ingest_dir();
    let entries = fs::read_dir(&ingest_dir).map_err(|source| Error::Io {
        path: ingest_dir.clone(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: ingest_dir.clone(),
            source,
        })?;
        let path = entry.path();
        let Some(file_name) = path.file_name() else {
            continue;
        };
        let Some(segment_id) = parse_segment_file_name(file_name) else {
            continue;
        };
        if indexed_segment_ids.contains(&segment_id) {
            continue;
        }

        match config.recovery_policy {
            StrataRecoveryPolicy::PointInTime => {
                fs::remove_file(&path).map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            }
            StrataRecoveryPolicy::AbsoluteConsistency => {
                return Err(Error::OrphanSegmentFile { segment_id, path });
            }
        }
    }

    Ok(())
}

/// Recovery driver for everything that wasn't sealed. Three phases, in order:
///
/// 1. Scan each unsealed segment (in segment-id order, which is also write order) and keep its
///    longest valid prefix. The first segment that comes up short poisons everything after it:
///    later segments hold later LSNs, and keeping LSN 50 while LSN 40 is gone would break the
///    "durable means a contiguous prefix" contract — so later segments are discarded outright.
/// 2. Roll back the logical tail beginning with the first payload that did not survive.
///
/// This deliberately does not advance `next_lsn` from records found only in segment files. The
/// segment file is the payload log, not the commit log: a batch can reserve LSN 10 for an epoch
/// increment and LSN 11 for a put, write the LSN 11 payload record, then crash before the RocksDB
/// batch publishes either operation. If recovery treated that segment record as committed and
/// bumped `next_lsn` to 12, it would create a hole at LSN 10 and silently drop the epoch change.
/// Even a put-only batch has the same shape: if the process exits after writing payload bytes but
/// before RocksDB publishes the batch, recovery must not make those bytes visible. Only RocksDB's
/// batch tells us which LSNs committed; segment recovery can promote/truncate bytes for
/// already-committed operations, but it must not discover new committed LSNs from payload bytes
/// alone.
///
/// The caller next validates the same logical prefix against the store WAL. A complete tail is
/// promoted; an incomplete unpublished tail is truncated from both the logical and segment paths.
pub(crate) fn recover_unsealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut discard_later_segments = false;
    let mut rollback_from = None;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if discard_later_segments {
            if let Some(state) = index.get_segment_state(segment_id)? {
                rollback_from = min_lsn(rollback_from, state.min_lsn);
            }
            discard_unsealed_segment(config, index, segment_id, metrics)?;
            continue;
        }

        let recovered = recover_unsealed_segment(config, index, segment_id, metrics)?;
        metrics.record_recovered_segment(recovered.is_complete);
        if !recovered.is_complete {
            discard_later_segments = true;
            rollback_from = min_lsn(rollback_from, recovered.rollback_from);
        }
    }
    if let Some(rollback_from) = rollback_from {
        rollback_operations_from(index, metrics, rollback_from)?;
    }
    Ok(())
}

/// Removes payload records that belong to a logical WAL tail being rolled back.
///
/// Metadata-only operations do not consume segment bytes, so some segments need no change. When
/// payloads are present, every byte at or after the first rolled-back LSN is truncated and the
/// segment GC baseline is rebuilt by the normal recovered-prefix publisher.
fn truncate_unsealed_segments_from_lsn(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    rollback_from: StrataLsn,
) -> Result<()> {
    for segment_id in unsealed_ingest_segment_ids(index)? {
        let Some(existing_state) = index.get_segment_state(segment_id)? else {
            continue;
        };
        let path = segment_path(config, segment_id);
        if !path.exists() {
            continue;
        }
        let mut scanner = SegmentScanner::open(&path, segment_id)?;
        let prefix = scanner.scan_recoverable_prefix(existing_state.durable_offset)?;
        let Some(first_hidden) = prefix
            .records
            .iter()
            .find(|record| record.header.generation >= rollback_from)
        else {
            continue;
        };
        let recovered_write_offset = first_hidden.record_ref.offset;
        // The preceding segment scan may have fsynced a complete buffered tail before WAL
        // validation discovered that the matching logical operations were unavailable. Those
        // promoted bytes are still newer than published_lsn and may be truncated here.
        let retained_durable_offset = existing_state.durable_offset.min(recovered_write_offset);
        let recovered_durable_offset = persist_recovered_segment_prefix(
            &path,
            prefix.file_len,
            retained_durable_offset,
            recovered_write_offset,
        )?;
        apply_recovered_segment_prefix(
            config,
            index,
            segment_id,
            RecoveredSegmentPrefix {
                existing_state: Some(existing_state),
                durable_offset: recovered_durable_offset,
                recovered_write_offset,
                records: &prefix.records,
            },
            metrics,
        )?;
    }
    Ok(())
}

fn min_lsn(current: Option<StrataLsn>, candidate: Option<StrataLsn>) -> Option<StrataLsn> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (current, candidate) => current.or(candidate),
    }
}

/// Validates the store WAL against the logical prefix selected by segment recovery.
///
/// A complete WAL tail is retained even when its checkpoint metadata did not reach disk: opening
/// the WAL will fsync and promote it. If the exact tail is unavailable, point-in-time recovery
/// discards only operations newer than `published_lsn`; that frontier is the last prefix callers
/// were promised would survive. The fallback target is validated independently, so corruption in
/// the published prefix remains a hard recovery error.
pub(crate) fn recover_store_wal_prefix(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let requested_lsn = index.get_next_lsn()?.checked_sub(1).filter(|lsn| *lsn != 0);
    let published_lsn = index.get_committed_lsn()?;
    let published_target = (published_lsn != 0).then_some(published_lsn);
    let checkpoint = index.get_store_checkpoint()?;
    let (materialized_through, retained_from) = store_wal_recovery_state(config, index)?;
    let validate = |checkpoint: Option<StoreCheckpoint>, last_lsn| {
        Wal::validate_recovery_target(
            config.namespace_dir().join("wal"),
            checkpoint.map_or(WalPosition::default(), |checkpoint| checkpoint.wal_position),
            published_target,
            materialized_through,
            retained_from,
            last_lsn,
        )
    };
    if validate(checkpoint, requested_lsn).is_ok() {
        return Ok(());
    }
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency {
        validate(checkpoint, requested_lsn)?;
        unreachable!("failed WAL validation returned success on retry");
    }

    if requested_lsn.is_none_or(|requested| requested <= published_lsn) {
        validate(checkpoint, requested_lsn)?;
        unreachable!("failed published WAL validation returned success on retry");
    }
    let fallback_checkpoint = if published_lsn == 0 { None } else { checkpoint };
    validate(fallback_checkpoint, published_target)?;
    if fallback_checkpoint != checkpoint {
        let mut batch = index.batch();
        index.put_store_checkpoint_batch(
            &mut batch,
            StoreCheckpoint {
                wal_position: WalPosition::default(),
                active_segment_id: 0,
                active_segment_offset: 0,
            },
        )?;
        batch.write_with_sync(true).map_err(index::Error::from)?;
    }

    let rollback_from = published_lsn
        .checked_add(1)
        .ok_or(segment::Error::RangeOverflow)?;
    truncate_unsealed_segments_from_lsn(config, index, metrics, rollback_from)?;
    rollback_operations_from(index, metrics, rollback_from)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentRecovery {
    is_complete: bool,
    rollback_from: Option<StrataLsn>,
}

/// Recovers one unsealed segment by scanning records from offset 0 and keeping the longest
/// checksummed-valid prefix.
///
/// The scan deliberately validates *past* the persisted durable offset: after a process crash
/// (as opposed to power loss) appended bytes usually survive in the kernel page cache, and after
/// a power loss they may still have been fsynced without the durable-offset row committing. If
/// complete records are sitting there inside the committed write prefix, throwing them away would
/// be rolling back writes for no reason — so they get promoted and the matching store-WAL entries
/// are replayed.
///
/// `is_complete` is the signal the driver uses to discard later segments: an incomplete prefix
/// means some indexed LSNs in this segment are gone, so nothing after it may be kept either.
fn recover_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    metrics: &StrataStoreMetrics,
) -> Result<SegmentRecovery> {
    let path = segment_path(config, segment_id);
    let existing_state = index.get_segment_state(segment_id)?;
    let expected_write_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.write_offset);
    let durable_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.durable_offset);
    if !path.exists() {
        if expected_write_offset == 0 {
            return Ok(SegmentRecovery {
                is_complete: true,
                rollback_from: None,
            });
        }
        return recover_missing_unsealed_segment(
            config,
            index,
            segment_id,
            expected_write_offset,
            durable_offset,
            metrics,
        );
    }

    let mut scanner = SegmentScanner::open(&path, segment_id)?;
    let prefix = scanner.scan_recoverable_prefix(durable_offset)?;
    let is_complete = prefix.valid_len >= expected_write_offset;
    let recovered_write_offset = prefix.valid_len.min(expected_write_offset);
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency
        && (prefix.valid_len != expected_write_offset || prefix.file_len != expected_write_offset)
    {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset,
        });
    }

    let recovered_durable_offset = persist_recovered_segment_prefix(
        &path,
        prefix.file_len,
        durable_offset,
        recovered_write_offset,
    )?;
    let recovered_max_lsn = prefix
        .records
        .iter()
        .filter(|record| {
            record
                .record_ref
                .offset
                .checked_add(record.record_len)
                .is_some_and(|end| end <= recovered_write_offset)
        })
        .map(|record| record.header.generation)
        .max();
    let rollback_from = if is_complete {
        None
    } else {
        Some(
            recovered_max_lsn
                .and_then(|lsn| lsn.checked_add(1))
                .or_else(|| existing_state.as_ref().and_then(|state| state.min_lsn))
                .unwrap_or(index.get_next_lsn()?),
        )
    };

    apply_recovered_segment_prefix(
        config,
        index,
        segment_id,
        RecoveredSegmentPrefix {
            existing_state,
            durable_offset: recovered_durable_offset,
            recovered_write_offset,
            records: &prefix.records,
        },
        metrics,
    )?;
    Ok(SegmentRecovery {
        is_complete,
        rollback_from,
    })
}

/// Truncates a recovered segment to its valid prefix and fsyncs, so the garbage tail can never
/// be mistaken for data by a later scan. Returns the new durable offset: bytes the scan validated
/// beyond the old durable offset are promoted (they're provably on disk after this fsync), which
/// is how recovery can end up *more* durable than the pre-crash metadata claimed.
fn persist_recovered_segment_prefix(
    path: &Path,
    file_len: u64,
    durable_offset: u64,
    recovered_write_offset: u64,
) -> Result<u64> {
    let needs_truncate = file_len != recovered_write_offset;
    let promotes_recovered_bytes = recovered_write_offset > durable_offset;
    if !needs_truncate && !promotes_recovered_bytes {
        return Ok(durable_offset);
    }

    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if needs_truncate {
        file.set_len(recovered_write_offset)
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }
    file.sync_data().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;

    if promotes_recovered_bytes {
        Ok(recovered_write_offset)
    } else {
        Ok(durable_offset)
    }
}

/// An indexed unsealed segment whose file vanished. The durable offset draws the line between
/// "annoying" and "catastrophic": if no bytes were ever declared durable, the file only held
/// unacknowledged writes and point-in-time recovery may discard it like a torn tail. But if
/// bytes *were* declared durable, someone upstream may have already acted on that promise (the
/// event cursor advanced), so this is unrecoverable data loss and must be a hard error rather
/// than a silent rollback.
fn recover_missing_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    expected_write_offset: u64,
    durable_offset: u64,
    metrics: &StrataStoreMetrics,
) -> Result<SegmentRecovery> {
    if durable_offset > 0 {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    let rollback_from = index
        .get_segment_state(segment_id)?
        .and_then(|state| state.min_lsn);
    discard_unsealed_segment(config, index, segment_id, metrics)?;
    Ok(SegmentRecovery {
        is_complete: false,
        rollback_from,
    })
}

struct RecoveredSegmentPrefix<'a> {
    existing_state: Option<SegmentState>,
    durable_offset: u64,
    recovered_write_offset: u64,
    records: &'a [segment::ScannedRecord],
}

/// Publishes the post scan segment state and rebuilds its LSN bounds from scratch.
///
/// min/max LSN can't be trusted from the old state because the tail they described may be gone, so
/// they are recomputed from the checksummed prefix bounded by the committed write offset.
fn apply_recovered_segment_prefix(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    prefix: RecoveredSegmentPrefix<'_>,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut state = active_segment_state_from_path(
        config,
        INGEST_SEGMENT_OWNER,
        segment_id,
        prefix.recovered_write_offset,
        prefix.durable_offset,
    );
    if let Some(existing) = prefix.existing_state {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.state = existing.state;
        state.sealed_before_lsn = existing.sealed_before_lsn;
        state.sealed_len = existing.sealed_len;
    }
    state.min_lsn = None;
    state.max_lsn = None;

    let mut batch = index.batch();
    let mut recovered_record_count = 0_u64;
    let previous_baseline_bytes = index
        .get_segment_gc_summary(segment_id)?
        .map_or(0, |summary| summary.total_bytes);
    let reset_baseline = previous_baseline_bytes > prefix.recovered_write_offset;
    let baseline_bytes = if reset_baseline {
        0
    } else {
        previous_baseline_bytes
    };
    let mut allocation_records = 0_u64;

    for record in prefix.records {
        let record_end = record
            .record_ref
            .offset
            .checked_add(record.record_len)
            .ok_or(segment::Error::RangeOverflow)?;
        if record_end > prefix.recovered_write_offset {
            continue;
        }
        recovered_record_count = recovered_record_count.saturating_add(1);
        if record_end > baseline_bytes {
            if record.record_ref.offset < baseline_bytes {
                return Err(Error::InvariantViolation {
                    reason: format!(
                        "segment {segment_id} GC baseline {baseline_bytes} splits record at {}",
                        record.record_ref.offset
                    ),
                });
            }
            allocation_records = allocation_records.saturating_add(1);
        }
        let lsn = record.header.generation;

        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }

    index.put_segment_state_batch(&mut batch, &state)?;
    if reset_baseline {
        // Point-in-time recovery may deliberately truncate a prefix previously counted by GC.
        // Rebuild conservatively: every surviving record starts live and unknown.
        index.put_segment_gc_summary_batch(
            &mut batch,
            segment_id,
            &SegmentGcSummary {
                total_bytes: prefix.recovered_write_offset,
                live_bytes: prefix.recovered_write_offset,
                live_ref_count: recovered_record_count,
                unknown_lifetime_bytes: prefix.recovered_write_offset,
                unknown_lifetime_ref_count: recovered_record_count,
                ..Default::default()
            },
        )?;
    } else {
        publish_segment_allocation_delta(
            index,
            &mut batch,
            segment_id,
            prefix.recovered_write_offset - baseline_bytes,
            allocation_records,
        )?;
    }

    batch.write_with_sync(true).map_err(index::Error::from)?;
    metrics.record_recovered_records(recovered_record_count, prefix.recovered_write_offset);
    Ok(())
}

/// Drops an unsealed segment entirely (used when a preceding segment lost data, see the driver).
/// Metadata is marked `Deleted` and flushed *before* the unlink: if we crash in between, the next
/// open sees a Deleted segment with a leftover file, which the orphan/recovery paths handle. The
/// reverse order could leave an Open segment state pointing at nothing — which is the
/// "durable bytes vanished" hard-error case.
fn discard_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let mut state = active_segment_state_from_path(config, INGEST_SEGMENT_OWNER, segment_id, 0, 0);
    if let Some(existing) = index.get_segment_state(segment_id)? {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.path = existing.path;
    }
    state.state = SegmentFileState::Deleted;
    state.write_offset = 0;
    state.durable_offset = 0;
    state.sealed_len = None;
    state.sealed_sha256 = None;

    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &state)?;
    batch.write_with_sync(true).map_err(index::Error::from)?;

    let path = segment_path(config, segment_id);
    match fs::remove_file(&path) {
        Ok(()) => {
            metrics.record_discarded_segment();
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            metrics.record_discarded_segment();
            Ok(())
        }
        Err(source) => Err(Error::Io {
            path: path.clone(),
            source,
        }),
    }
}

/// Erases epoch operations from `rollback_from` onward and rewinds the store frontiers to that LSN.
///
/// Blob patches live in the store WAL, which is reopened through the rewound `next_lsn`; RocksDB only
/// needs to remove its auxiliary LSN and epoch rows.
fn rollback_operations_from(
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    rollback_from: StrataLsn,
) -> Result<()> {
    let mut batch = index.batch();
    let hidden_epoch_changes = index
        .iter_epoch_changes_from(rollback_from)?
        .into_iter()
        .map(|(lsn, _)| lsn)
        .collect::<Vec<_>>();
    index.remove_epoch_changes_batch(&mut batch, &hidden_epoch_changes)?;
    let rolled_back_drops = index
        .iter_shard_cleanup_jobs()?
        .into_iter()
        .filter(|job| job.drop_lsn >= rollback_from)
        .collect::<Vec<_>>();
    for job in &rolled_back_drops {
        // Cleanup cannot have crossed an unpublished drop because the GC worker gates it on
        // `published_lsn`. Restoring the same generation therefore makes the pre-drop shard
        // visible again without resurrecting physically reclaimed files.
        if index.get_shard_info(job.shard.id)?.is_some_and(|info| {
            info.current_generation == job.shard.generation && info.state == ShardState::Dropped
        }) {
            index.put_shard_info_batch(
                &mut batch,
                job.shard.id,
                ShardInfo {
                    current_generation: job.shard.generation,
                    state: ShardState::Active,
                },
            )?;
        }
        index.delete_shard_cleanup_job_batch(&mut batch, job.shard)?;
    }
    let rollback_ops = hidden_epoch_changes
        .len()
        .saturating_add(rolled_back_drops.len()) as u64;

    let previous_lsn = rollback_from.saturating_sub(1);
    let current_epoch = index
        .latest_epoch_at_lsn(previous_lsn)?
        .map(|(_, epoch)| epoch)
        .ok_or(Error::EpochNotInitialized)?;
    index.put_current_epoch_batch(&mut batch, current_epoch)?;
    index.put_next_lsn_batch(&mut batch, rollback_from)?;
    batch.write_with_sync(true).map_err(index::Error::from)?;
    metrics.set_next_lsn(rollback_from);
    metrics.set_current_epoch(current_epoch);
    metrics.record_rollback(rollback_from, rollback_ops);
    Ok(())
}

/// Publishes the exact store-WAL prefix selected by recovery.
///
/// `Wal::recover` has already fsynced a promoted complete tail, and segment recovery has fsynced
/// the referenced active data prefix. The LSM is only the in-memory projection of that log.
pub(crate) fn publish_recovered_store_checkpoint(
    index: &StrataIndex,
    metrics: &StrataStoreMetrics,
    wal: &Wal,
    active_segment_state: &SegmentState,
) -> Result<()> {
    let recovered_lsn = index.get_next_lsn()?.saturating_sub(1);
    let checkpoint = StoreCheckpoint {
        wal_position: wal.position(),
        active_segment_id: active_segment_state.segment_id,
        active_segment_offset: active_segment_state.write_offset,
    };
    if checkpoint.active_segment_id != active_segment_state.segment_id
        || checkpoint.active_segment_offset != active_segment_state.write_offset
    {
        return Err(Error::InvariantViolation {
            reason: format!(
                "recovered store checkpoint {checkpoint:?} does not match store LSN {recovered_lsn} and active segment {} at {}",
                active_segment_state.segment_id, active_segment_state.write_offset
            ),
        });
    }
    let current_published_lsn = index.get_committed_lsn()?;
    if current_published_lsn > recovered_lsn {
        return Err(Error::InvariantViolation {
            reason: format!(
                "published LSN {current_published_lsn} follows recovered LSN {recovered_lsn}"
            ),
        });
    }
    let mut batch = index.batch();
    let published_lsn = recovered_lsn;
    index.put_commit_lsn_batch(&mut batch, published_lsn)?;
    index.put_store_checkpoint_batch(&mut batch, checkpoint)?;
    batch.write_with_sync(true).map_err(index::Error::from)?;
    metrics.set_published_lsn(published_lsn);
    Ok(())
}
