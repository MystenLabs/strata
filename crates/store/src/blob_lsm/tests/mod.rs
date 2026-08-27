//! Shared fixtures for the blob-LSM tests.
//!
//! The child modules mirror the source split: `codec` covers `format`, `full_merge` covers
//! `state`, `partial_merge` covers `reduce`, and `compaction` covers snapshot-driven pruning.

use std::collections::BTreeMap;

use core_types::{
    BlobLifecycle, Epoch, GarbageEvent, RecordRef, SegmentGcSummaryDelta, SegmentKey, ShardKey,
};
use lsm::{
    GarbageRecord, MergeOperator, StoredValue, StrataLsn, decode_value, encode_blob_value,
    encode_inline_value,
};

use super::{BlobCompactionSnapshot, BlobMerge, BlobMergeWithRelocations, BlobMutation, BlobState};

mod codec;
mod compaction;
mod full_merge;
mod partial_merge;

fn shard(id: u32, generation: u64) -> ShardKey {
    ShardKey { id, generation }
}

fn record(segment_id: u64, offset: u64) -> RecordRef {
    RecordRef {
        segment_id,
        offset,
        len: 100,
    }
}

fn put_patch(shard: ShardKey, write_epoch: Epoch, record_ref: RecordRef) -> Vec<u8> {
    encode_blob_value(
        &BlobMutation::encode_put_metadata(shard, write_epoch),
        record_ref,
    )
}

fn inline_patch(mutation: BlobMutation) -> Vec<u8> {
    encode_inline_value(&mutation.encode_inline().unwrap())
}

fn merge(patches: &[(StrataLsn, Vec<u8>)]) -> (BlobState, Vec<GarbageRecord>) {
    let borrowed = patches
        .iter()
        .map(|(lsn, bytes)| (*lsn, bytes.as_slice()))
        .collect::<Vec<_>>();
    let mut garbage = Vec::new();
    let encoded = BlobMerge
        .merge(b"blob", None, &borrowed, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let StoredValue::Inline(bytes) = decode_value(&encoded).unwrap() else {
        panic!("blob merge must materialize inline state");
    };
    (BlobState::decode(bytes).unwrap(), garbage)
}

fn partial_merge_with_snapshot(
    patches: &[(StrataLsn, Vec<u8>)],
    snapshot: BlobCompactionSnapshot,
) -> (Vec<u8>, Vec<GarbageRecord>) {
    let borrowed = patches
        .iter()
        .map(|(lsn, bytes)| (*lsn, bytes.as_slice()))
        .collect::<Vec<_>>();
    let merge = BlobMergeWithRelocations::new(None, snapshot);
    let mut garbage = Vec::new();
    let value = merge
        .partial_merge(b"blob", &borrowed, &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap()
        .expect("test patches must produce a partial merge");
    (value, garbage)
}

fn aggregate_garbage_deltas(records: &[GarbageRecord]) -> SegmentGcSummaryDelta {
    let mut total = SegmentGcSummaryDelta::default();
    for record in records {
        let delta = &record.summary_delta;
        total.total_bytes += delta.total_bytes;
        total.live_bytes += delta.live_bytes;
        total.retired_bytes += delta.retired_bytes;
        total.expired_bytes += delta.expired_bytes;
        total.live_ref_count += delta.live_ref_count;
        total.unknown_lifetime_bytes += delta.unknown_lifetime_bytes;
        total.unknown_lifetime_ref_count += delta.unknown_lifetime_ref_count;
        for (&epoch, &change) in &delta.epoch_bytes {
            *total.epoch_bytes.entry(epoch).or_default() += change;
        }
        for (&epoch, &change) in &delta.epoch_refs {
            *total.epoch_refs.entry(epoch).or_default() += change;
        }
        for (&extension_count, &change) in &delta.extension_counts {
            *total.extension_counts.entry(extension_count).or_default() += change;
        }
    }
    total.epoch_bytes.retain(|_, change| *change != 0);
    total.epoch_refs.retain(|_, change| *change != 0);
    total.extension_counts.retain(|_, change| *change != 0);
    total
}

fn assert_functionally_equivalent_garbage(left: &[GarbageRecord], right: &[GarbageRecord]) {
    assert_eq!(garbage_facts(left), garbage_facts(right));
}

fn garbage_facts(
    records: &[GarbageRecord],
) -> BTreeMap<(SegmentKey, u64, u64), (bool, Option<BlobLifecycle>)> {
    let mut facts = BTreeMap::new();
    for record in records {
        let physical = record.event.record();
        let fact = facts
            .entry((record.key.clone(), physical.offset, physical.len))
            .or_insert((false, None));
        match record.event {
            GarbageEvent::Retired { .. } | GarbageEvent::Expired { .. } => fact.0 = true,
            GarbageEvent::SetLifecycle { lifecycle, .. } => {
                if fact.1.is_none_or(|(lsn, _)| record.lsn > lsn) {
                    fact.1 = Some((record.lsn, lifecycle));
                }
            }
        }
    }
    facts
        .into_iter()
        .map(|(record, (terminal, lifecycle))| {
            let lifecycle = (!terminal)
                .then(|| lifecycle.and_then(|(_, lifecycle)| lifecycle))
                .flatten();
            (record, (terminal, lifecycle))
        })
        .collect()
}

fn compact_state(
    state: BlobState,
    snapshot: BlobCompactionSnapshot,
) -> (Option<BlobState>, Vec<GarbageRecord>) {
    let base = encode_inline_value(&state.encode().unwrap());
    let merge = BlobMergeWithRelocations::new(None, snapshot);
    let mut garbage = Vec::new();
    let value = merge
        .merge(b"blob", Some(&base), &[], &mut |record| {
            garbage.push(record);
            Ok(())
        })
        .unwrap();
    let state = value.map(|value| {
        let StoredValue::Inline(bytes) = decode_value(&value).unwrap() else {
            panic!("blob compaction must materialize inline state");
        };
        BlobState::decode(bytes).unwrap()
    });
    (state, garbage)
}
