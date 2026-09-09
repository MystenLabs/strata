//! Read-only dump of segment states and GC summaries from a live or offline Strata index.
//!
//! Opens the index RocksDB as a secondary instance, so it works while the store is running.
//!
//! Usage: dump_gc_summaries <index-dir> <cf-prefix> <secondary-dir> <clock-cutoff-epoch> [limit]

use std::collections::BTreeMap;

use core_types::{PlacementClass, SegmentFileState, SegmentGcSummary, SegmentState};
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 5 {
        eprintln!(
            "usage: {} <index-dir> <cf-prefix> <secondary-dir> <cutoff-epoch> [limit]",
            args[0]
        );
        std::process::exit(2);
    }
    let (path, prefix, secondary) = (&args[1], &args[2], &args[3]);
    let cutoff: u64 = args[4].parse().expect("cutoff epoch");
    let limit: usize = args.get(5).map_or(8, |v| v.parse().expect("limit"));

    let mut existing = Options::default();
    existing.create_if_missing(false);
    let cf_names = DBWithThreadMode::<MultiThreaded>::list_cf(&existing, path).expect("list cfs");
    let known = index::cf_options_for_prefix(prefix)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let descriptors = cf_names
        .iter()
        .map(|name| {
            let options = known.get(name).cloned().unwrap_or_default();
            ColumnFamilyDescriptor::new(name.clone(), options)
        })
        .collect::<Vec<_>>();
    let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors_as_secondary(
        &existing,
        path,
        secondary,
        descriptors,
    )
    .expect("open secondary");
    db.try_catch_up_with_primary().expect("catch up");

    let states_cf = db
        .cf_handle(&format!("{prefix}/segment_states"))
        .expect("segment_states cf");
    let summaries_cf = db
        .cf_handle(&format!("{prefix}/segment_gc_summaries"))
        .expect("segment_gc_summaries cf");

    let mut states = BTreeMap::new();
    for item in db.iterator_cf(&states_cf, IteratorMode::Start) {
        let (key, value) = item.expect("state row");
        let id = u64::from_be_bytes(key[..8].try_into().expect("u64 key"));
        let state: SegmentState = bcs::from_bytes(&value).expect("segment state");
        states.insert(id, state);
    }
    let mut summaries = BTreeMap::new();
    for item in db.iterator_cf(&summaries_cf, IteratorMode::Start) {
        let (key, value) = item.expect("summary row");
        let id = u64::from_be_bytes(key[..8].try_into().expect("u64 key"));
        let summary: SegmentGcSummary = bcs::from_bytes(&value).expect("summary");
        summaries.insert(id, summary);
    }
    println!(
        "segments: {} states, {} summaries, cutoff epoch {cutoff}",
        states.len(),
        summaries.len()
    );

    // Aggregate by (placement, state) for live segments.
    let mut by_class: BTreeMap<String, (usize, u64, u64, u64, u64, u64, u64)> = BTreeMap::new();
    let mut examples = Vec::new();
    for (id, state) in &states {
        if state.state == SegmentFileState::Deleted {
            continue;
        }
        let summary = summaries.get(id).cloned().unwrap_or_default();
        let class = match state.placement_class {
            PlacementClass::ExactEpoch(epoch) if epoch <= cutoff => {
                "exact-epoch<=cutoff".to_owned()
            }
            PlacementClass::ExactEpoch(_) => "exact-epoch>cutoff".to_owned(),
            other => format!("{other:?}"),
        };
        let class = format!("{class} / {:?}", state.state);
        let ended: u64 = summary
            .future_epoch_histogram
            .iter()
            .filter(|(epoch, _)| **epoch <= cutoff)
            .map(|(_, bucket)| bucket.bytes)
            .sum();
        let later: u64 = summary
            .future_epoch_histogram
            .iter()
            .filter(|(epoch, _)| **epoch > cutoff)
            .map(|(_, bucket)| bucket.bytes)
            .sum();
        let classified = ended
            .saturating_add(later)
            .saturating_add(summary.unknown_lifetime_bytes);
        let remainder = summary.live_bytes.saturating_sub(classified);
        let entry = by_class.entry(class.clone()).or_default();
        entry.0 += 1;
        entry.1 += summary.total_bytes;
        entry.2 += summary.live_bytes;
        entry.3 += ended;
        entry.4 += later;
        entry.5 += summary.unknown_lifetime_bytes;
        entry.6 += remainder;
        let clock_live = summary.live_after_epoch(cutoff);
        if matches!(state.placement_class, PlacementClass::ExactEpoch(epoch) if epoch <= cutoff)
            && clock_live.bytes > 0
            && examples.len() < limit
        {
            examples.push((*id, state.placement_class, state.state, summary));
        }
    }
    println!("class / state: count total_MB live_MB ended_MB later_MB unknown_MB remainder_MB");
    for (class, (n, total, live, ended, later, unknown, rem)) in &by_class {
        println!(
            "  {class}: {n} {:.0} {:.0} {:.0} {:.0} {:.0} {:.0}",
            *total as f64 / 1e6,
            *live as f64 / 1e6,
            *ended as f64 / 1e6,
            *later as f64 / 1e6,
            *unknown as f64 / 1e6,
            *rem as f64 / 1e6
        );
    }
    println!("examples of expired exact-epoch segments still clock-live:");
    for (id, placement, state, summary) in examples {
        let buckets = summary
            .future_epoch_histogram
            .iter()
            .map(|(epoch, bucket)| format!("{epoch}:{}b/{}r", bucket.bytes, bucket.refs))
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "  segment {id} {placement:?} {state:?} total={} live={} refs={} expired={} retired={} unknown={}/{} min={:?} max={:?} ext={:?} buckets=[{buckets}]",
            summary.total_bytes,
            summary.live_bytes,
            summary.live_ref_count,
            summary.expired_bytes,
            summary.retired_bytes,
            summary.unknown_lifetime_bytes,
            summary.unknown_lifetime_ref_count,
            summary.min_live_end_epoch,
            summary.max_live_end_epoch,
            summary.extension_count_histogram,
        );
    }
}
