//! Terminal garbage records and their GC summary accounting.

use core_types::{
    BlobKey, BlobLifecycle, GarbageEvent, RecordRef, SegmentGcSummaryDelta, SegmentKey,
};
use lsm::{GarbageRecord, Result, StrataLsn};

use super::format::{BlobVersion, invalid};

pub(crate) fn emit_lifetime_change(
    key: &[u8],
    lsn: StrataLsn,
    version: BlobVersion,
    before: Option<BlobLifecycle>,
    after: Option<BlobLifecycle>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    let bytes = i128::from(version.record_ref.len);
    let mut summary_delta = SegmentGcSummaryDelta::default();
    classify_lifetime(&mut summary_delta, before, -bytes, -1, true);
    classify_lifetime(&mut summary_delta, after, bytes, 1, true);
    emit(GarbageRecord {
        key: segment_key(key, version.record_ref)?,
        lsn,
        event: GarbageEvent::SetLifecycle {
            record: version.record_ref,
            lifecycle: after,
        },
        summary_delta,
    })
}

pub(crate) fn emit_record(
    key: &[u8],
    lsn: StrataLsn,
    version: BlobVersion,
    lifecycle: Option<BlobLifecycle>,
    event: GarbageEvent,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    emit(terminal_garbage_record(
        key,
        lsn,
        version.record_ref,
        lifecycle,
        event,
    )?)
}

pub(crate) fn terminal_garbage_record(
    key: &[u8],
    lsn: StrataLsn,
    record: RecordRef,
    lifecycle: Option<BlobLifecycle>,
    event: GarbageEvent,
) -> Result<GarbageRecord> {
    let bytes = i128::from(record.len);
    let mut summary_delta = SegmentGcSummaryDelta {
        live_bytes: -bytes,
        live_ref_count: -1,
        ..SegmentGcSummaryDelta::default()
    };
    match event {
        GarbageEvent::Retired { .. } => {
            summary_delta.retired_bytes = bytes;
            classify_lifetime(&mut summary_delta, lifecycle, -bytes, -1, true);
        }
        GarbageEvent::Expired { .. } => {
            summary_delta.expired_bytes = bytes;
            classify_lifetime(&mut summary_delta, lifecycle, -bytes, -1, false);
        }
        GarbageEvent::SetLifecycle { .. } => unreachable!("handled separately"),
    }
    Ok(GarbageRecord {
        key: segment_key(key, record)?,
        lsn,
        event,
        summary_delta,
    })
}

fn classify_lifetime(
    delta: &mut SegmentGcSummaryDelta,
    lifecycle: Option<BlobLifecycle>,
    bytes: i128,
    refs: i128,
    include_extension: bool,
) {
    match lifecycle {
        Some(lifecycle) => {
            *delta
                .epoch_bytes
                .entry(lifecycle.logical_end_epoch)
                .or_default() += bytes;
            *delta
                .epoch_refs
                .entry(lifecycle.logical_end_epoch)
                .or_default() += refs;
            if include_extension {
                *delta
                    .extension_counts
                    .entry(lifecycle.extension_count)
                    .or_default() += refs;
            }
        }
        None => {
            delta.unknown_lifetime_bytes += bytes;
            delta.unknown_lifetime_ref_count += refs;
        }
    }
}

fn segment_key(key: &[u8], record_ref: RecordRef) -> Result<SegmentKey> {
    Ok(SegmentKey {
        segment_id: record_ref.segment_id,
        blob_key: BlobKey::new(key.to_vec()).map_err(|error| invalid(error.to_string()))?,
    })
}
