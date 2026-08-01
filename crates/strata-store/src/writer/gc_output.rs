//! Pure helpers for the GC publication commit: LSN assignment for output
//! segments, skipped-record classification, and initial output metadata rows.

use std::collections::{BTreeMap, BTreeSet};

use strata_core::{
    BlobLifecycle, Epoch, GarbageEvent, RecordRef, SegmentGcRecordRange, SegmentGcSummary,
    SegmentId, SegmentKey, SegmentState, StrataLsn,
};
use strata_lsm::GarbageRecord;

use crate::{Error, GcPublishedOutputSegment, GcPublishedRecord, GcStagedCopiedRecord, Result};

#[derive(Debug)]
pub(crate) struct PlannedGcOutputSegments {
    /// Translation from temporary staging segment ids to final durable segment ids.
    pub(crate) staged_to_final_segment_id: BTreeMap<SegmentId, SegmentId>,
    /// Staging segment ids that actually contain at least one survivor.
    pub(crate) used_staged_ids: BTreeSet<SegmentId>,
    /// User-facing publication metadata for every output segment made visible.
    pub(crate) published_outputs: Vec<GcPublishedOutputSegment>,
    /// Durable segment state rows to publish in the metadata batch.
    pub(crate) segment_states: Vec<SegmentState>,
}

#[derive(Debug)]
pub(crate) enum GcPublishCommitError {
    BeforeIndexBatch(Error),
    IndexCommit(Error),
}

impl From<Error> for GcPublishCommitError {
    fn from(error: Error) -> Self {
        Self::BeforeIndexBatch(error)
    }
}

impl From<strata_index::Error> for GcPublishCommitError {
    fn from(error: strata_index::Error) -> Self {
        Self::BeforeIndexBatch(error.into())
    }
}

impl From<strata_segment::Error> for GcPublishCommitError {
    fn from(error: strata_segment::Error) -> Self {
        Self::BeforeIndexBatch(error.into())
    }
}

/// Terminal state assigned to copied bytes that became stale before publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GcSkippedCopiedRecordKind {
    /// The source was overwritten, tombstoned, or mapped before publish.
    Retired,
    /// The source became dead because its lifecycle expired before publish.
    Expired,
}

/// A staged copy whose source is no longer eligible to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcSkippedCopiedRecord {
    /// Original staged copy metadata. This is still useful to classify bytes already present in an
    /// output file that also contains survivors.
    pub(crate) record: GcStagedCopiedRecord,
    /// Whether those bytes should be classified as retired or expired garbage.
    pub(crate) kind: GcSkippedCopiedRecordKind,
}

/// Attributes every byte retained in published GC outputs to its original source segment.
///
/// A published output can contain a staged record that became stale during copy reconciliation as
/// long as another record in the same output survived. Those stale bytes still occupy disk, so
/// they must be included when computing net reclamation for the eventual source deletion.
pub(crate) fn gc_output_bytes_by_source(
    survivors: &[GcStagedCopiedRecord],
    skipped: &[GcSkippedCopiedRecord],
    used_staged_ids: &BTreeSet<SegmentId>,
) -> Result<BTreeMap<SegmentId, u64>> {
    let mut bytes_by_source = BTreeMap::new();
    let records = survivors
        .iter()
        .chain(skipped.iter().map(|skipped| &skipped.record));
    for record in records {
        if !used_staged_ids.contains(&record.staged.segment_id) {
            continue;
        }
        let bytes = bytes_by_source
            .entry(record.source.from.segment_id)
            .or_insert(0_u64);
        *bytes = bytes
            .checked_add(record.staged.len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
    }
    Ok(bytes_by_source)
}

/// Assigns consecutive publish LSNs and final destination refs to copied survivors.
///
/// The staged record already knows its offset and length inside a temporary output file. This helper
/// replaces the temporary segment id with the final durable segment id and pairs each move with the
/// LSN that orders its relocation in the main LSM.
pub(crate) fn assign_gc_publish_lsns(
    lsns: &[StrataLsn],
    records: &[GcStagedCopiedRecord],
    staged_to_final_segment_id: &BTreeMap<SegmentId, SegmentId>,
) -> Result<Vec<GcPublishedRecord>> {
    if lsns.len() != records.len() {
        return Err(Error::InvariantViolation {
            reason: format!(
                "LSM returned {} GC lsns for {} records",
                lsns.len(),
                records.len()
            ),
        });
    }
    records
        .iter()
        .zip(lsns)
        .map(|(record, lsn)| {
            let segment_id = staged_to_final_segment_id
                .get(&record.staged.segment_id)
                .copied()
                .ok_or(Error::GcMissingStagedOutput {
                    staged_segment_id: record.staged.segment_id,
                })?;
            Ok(GcPublishedRecord {
                source: record.source.clone(),
                to: RecordRef {
                    segment_id,
                    offset: record.staged.offset,
                    len: record.staged.len,
                },
                publish_lsn: *lsn,
            })
        })
        .collect()
}

/// Updates output segment logical bounds from the records committed by one GC batch.
///
/// GC output segments are sealed before they enter the manifest, so their `write_offset` is already
/// known. `min_lsn`/`max_lsn` are the per-segment relocation bounds used by later completeness
/// checks. The companion publication-LSN row is written atomically with these states.
pub(crate) fn apply_gc_output_lsn_bounds(
    states: &mut [SegmentState],
    published_records: &[GcPublishedRecord],
) {
    let mut bounds = BTreeMap::<SegmentId, (StrataLsn, StrataLsn)>::new();
    for record in published_records {
        bounds
            .entry(record.to.segment_id)
            .and_modify(|(min_lsn, max_lsn)| {
                *min_lsn = (*min_lsn).min(record.publish_lsn);
                *max_lsn = (*max_lsn).max(record.publish_lsn);
            })
            .or_insert((record.publish_lsn, record.publish_lsn));
    }
    for state in states {
        if let Some((min_lsn, max_lsn)) = bounds.get(&state.segment_id).copied() {
            state.min_lsn = Some(min_lsn);
            state.max_lsn = Some(max_lsn);
        }
    }
}

/// Groups stale copied output ranges by final output segment and terminal kind.
///
/// A staging file can contain both survivors and stale copies. If any survivor is published, the
/// whole sealed output file becomes durable, so stale ranges inside it must be classified as garbage
/// in the output segment rather than silently ignored.
pub(crate) fn skipped_gc_output_ranges(
    skipped_records: &[GcSkippedCopiedRecord],
    staged_to_final_segment_id: &BTreeMap<SegmentId, SegmentId>,
) -> BTreeMap<(SegmentId, GcSkippedCopiedRecordKind), Vec<SegmentGcRecordRange>> {
    let mut ranges =
        BTreeMap::<(SegmentId, GcSkippedCopiedRecordKind), Vec<SegmentGcRecordRange>>::new();
    for record in skipped_records {
        if let Some(segment_id) = staged_to_final_segment_id.get(&record.record.staged.segment_id) {
            ranges
                .entry((*segment_id, record.kind))
                .or_default()
                .push(SegmentGcRecordRange {
                    offset: record.record.staged.offset,
                    len: record.record.staged.len,
                });
        }
    }
    ranges
}

#[allow(clippy::type_complexity)]
pub(crate) fn initial_gc_output_metadata(
    states: &[SegmentState],
    published: &[GcPublishedRecord],
    skipped: &[GcSkippedCopiedRecord],
    staged_to_final: &BTreeMap<SegmentId, SegmentId>,
    current_epoch: Epoch,
) -> Result<(
    BTreeMap<SegmentId, SegmentGcSummary>,
    BTreeMap<SegmentId, Vec<GarbageRecord>>,
)> {
    let mut summaries = states
        .iter()
        .map(|state| (state.segment_id, SegmentGcSummary::default()))
        .collect::<BTreeMap<_, _>>();
    let mut garbage = BTreeMap::<SegmentId, Vec<GarbageRecord>>::new();

    for record in published {
        let lifecycle = record.source.lifecycle;
        let expired =
            lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch);
        let summary =
            summaries
                .get_mut(&record.to.segment_id)
                .ok_or_else(|| Error::InvariantViolation {
                    reason: format!(
                        "published GC record targets missing output segment {}",
                        record.to.segment_id
                    ),
                })?;
        add_initial_gc_output_record(
            summary,
            record.to,
            lifecycle,
            expired.then_some(GcSkippedCopiedRecordKind::Expired),
        )?;

        let event = if expired {
            Some(GarbageEvent::Expired { record: record.to })
        } else {
            lifecycle.map(|lifecycle| GarbageEvent::SetLifecycle {
                record: record.to,
                lifecycle: Some(lifecycle),
            })
        };
        if let Some(event) = event {
            garbage
                .entry(record.to.segment_id)
                .or_default()
                .push(GarbageRecord {
                    key: SegmentKey {
                        segment_id: record.to.segment_id,
                        blob_key: record.source.key.clone(),
                    },
                    lsn: record.publish_lsn,
                    event,
                    summary_delta: Default::default(),
                });
        }
    }

    for skipped in skipped {
        let Some(&segment_id) = staged_to_final.get(&skipped.record.staged.segment_id) else {
            continue;
        };
        let record_ref = RecordRef {
            segment_id,
            offset: skipped.record.staged.offset,
            len: skipped.record.staged.len,
        };
        let summary = summaries
            .get_mut(&segment_id)
            .ok_or_else(|| Error::InvariantViolation {
                reason: format!("skipped GC record targets missing output segment {segment_id}"),
            })?;
        add_initial_gc_output_record(summary, record_ref, None, Some(skipped.kind))?;
        let event = match skipped.kind {
            GcSkippedCopiedRecordKind::Retired => GarbageEvent::Retired { record: record_ref },
            GcSkippedCopiedRecordKind::Expired => GarbageEvent::Expired { record: record_ref },
        };
        garbage.entry(segment_id).or_default().push(GarbageRecord {
            key: SegmentKey {
                segment_id,
                blob_key: skipped.record.source.key.clone(),
            },
            lsn: skipped.record.source.payload_lsn,
            event,
            summary_delta: Default::default(),
        });
    }

    for state in states {
        let summary = summaries
            .get_mut(&state.segment_id)
            .expect("summary was initialized from this state");
        if summary.total_bytes != state.sealed_len.unwrap_or_default() {
            return Err(Error::InvariantViolation {
                reason: format!(
                    "GC output segment {} accounts for {} of {} sealed bytes",
                    state.segment_id,
                    summary.total_bytes,
                    state.sealed_len.unwrap_or_default()
                ),
            });
        }
        summary.min_live_end_epoch = summary.future_epoch_histogram.keys().next().copied();
        summary.max_live_end_epoch = summary.future_epoch_histogram.keys().next_back().copied();
    }

    Ok((summaries, garbage))
}

fn add_initial_gc_output_record(
    summary: &mut SegmentGcSummary,
    record: RecordRef,
    lifecycle: Option<BlobLifecycle>,
    terminal: Option<GcSkippedCopiedRecordKind>,
) -> Result<()> {
    checked_summary_add(&mut summary.total_bytes, record.len, record.segment_id)?;
    match terminal {
        Some(GcSkippedCopiedRecordKind::Retired) => {
            checked_summary_add(&mut summary.retired_bytes, record.len, record.segment_id)?;
        }
        Some(GcSkippedCopiedRecordKind::Expired) => {
            checked_summary_add(&mut summary.expired_bytes, record.len, record.segment_id)?;
            if let Some(lifecycle) = lifecycle {
                checked_summary_add(
                    summary
                        .extension_count_histogram
                        .entry(lifecycle.extension_count)
                        .or_default(),
                    1,
                    record.segment_id,
                )?;
            }
        }
        None => {
            checked_summary_add(&mut summary.live_bytes, record.len, record.segment_id)?;
            checked_summary_add(&mut summary.live_ref_count, 1, record.segment_id)?;
            match lifecycle {
                Some(lifecycle) => {
                    let bucket = summary
                        .future_epoch_histogram
                        .entry(lifecycle.logical_end_epoch)
                        .or_default();
                    checked_summary_add(&mut bucket.bytes, record.len, record.segment_id)?;
                    checked_summary_add(&mut bucket.refs, 1, record.segment_id)?;
                    checked_summary_add(
                        summary
                            .extension_count_histogram
                            .entry(lifecycle.extension_count)
                            .or_default(),
                        1,
                        record.segment_id,
                    )?;
                }
                None => {
                    checked_summary_add(
                        &mut summary.unknown_lifetime_bytes,
                        record.len,
                        record.segment_id,
                    )?;
                    checked_summary_add(
                        &mut summary.unknown_lifetime_ref_count,
                        1,
                        record.segment_id,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn checked_summary_add(value: &mut u64, amount: u64, segment_id: SegmentId) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| Error::InvariantViolation {
            reason: format!("GC summary overflow for segment {segment_id}"),
        })?;
    Ok(())
}
