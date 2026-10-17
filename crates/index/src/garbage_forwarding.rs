//! Durable routes for garbage published before GC moved a physical record.
//!
//! A route covers only the global-log prefix preceding its activation. GC's own source
//! retirement is appended *after* that boundary and must never retire the destination.
//! Routes share the activation batch and are pruned only by a committed sweep past the boundary.

use std::collections::BTreeMap;

use core_types::{GarbageEvent, RecordRef, SegmentId};
use lsm::{GarbageLogPosition, GarbageRecord};
use serde::{Deserialize, Serialize};

use crate::{Error, Result, StrataIndex, port::map::IndexBatch};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GarbageForwarding {
    pub(crate) through: GarbageLogPosition,
    destinations: BTreeMap<(u64, u64), RecordRef>,
}

pub(crate) fn at_or_before(a: GarbageLogPosition, b: GarbageLogPosition) -> bool {
    (a.log_id, a.offset) <= (b.log_id, b.offset)
}

impl StrataIndex {
    /// Installs forwarding for the still-unswept prefix in the same batch as GC activation.
    /// Calls and sweeping must hold the owner's garbage-publication lock.
    pub fn put_garbage_forwarding_batch(
        &self,
        batch: &mut IndexBatch,
        through: GarbageLogPosition,
        moved: impl IntoIterator<Item = (RecordRef, RecordRef)>,
    ) -> Result<()> {
        let mut routes = BTreeMap::<SegmentId, GarbageForwarding>::new();
        for (from, to) in moved {
            // Segment IDs are allocated monotonically; this also makes cycles impossible.
            if to.segment_id <= from.segment_id || to.len != from.len {
                return Err(Error::InvalidGarbageSweep(
                    "invalid garbage forwarding destination".into(),
                ));
            }
            let route = routes
                .entry(from.segment_id)
                .or_insert_with(|| GarbageForwarding {
                    through,
                    destinations: BTreeMap::new(),
                });
            if route
                .destinations
                .insert((from.offset, from.len), to)
                .is_some()
            {
                return Err(Error::InvalidGarbageSweep(
                    "duplicate garbage forwarding source".into(),
                ));
            }
        }
        for (source, route) in routes {
            if self.garbage_forwarding.contains_key(&source)? {
                return Err(Error::InvalidGarbageSweep(
                    "source already has garbage forwarding".into(),
                ));
            }
            batch.insert_batch(&self.garbage_forwarding, [(source, route)])?;
        }
        Ok(())
    }

    /// Preserves accounting for the old copy and forwards the event through every applicable
    /// move. Applying it to intermediate copies matters until their own retirement is swept.
    pub(crate) fn route_garbage_record(
        &self,
        record: GarbageRecord,
        frame_end: GarbageLogPosition,
        routes: &mut BTreeMap<SegmentId, Option<GarbageForwarding>>,
        by_segment: &mut BTreeMap<SegmentId, Vec<GarbageRecord>>,
    ) -> Result<()> {
        let mut current = record;
        loop {
            let from = current.event.record();
            if let std::collections::btree_map::Entry::Vacant(entry) = routes.entry(from.segment_id)
            {
                entry.insert(self.garbage_forwarding.get(&from.segment_id)?);
            }
            let destination = routes[&from.segment_id].as_ref().and_then(|route| {
                at_or_before(frame_end, route.through)
                    .then(|| route.destinations.get(&(from.offset, from.len)).copied())
                    .flatten()
            });
            let Some(to) = destination else {
                by_segment.entry(from.segment_id).or_default().push(current);
                break;
            };
            if to.segment_id <= from.segment_id || to.len != from.len {
                return Err(Error::InvalidGarbageSweep(
                    "corrupt garbage forwarding route".into(),
                ));
            }
            let mut forwarded = current.clone();
            forwarded.key.segment_id = to.segment_id;
            forwarded.event = match forwarded.event {
                GarbageEvent::Retired { .. } => GarbageEvent::Retired { record: to },
                GarbageEvent::Expired { .. } => GarbageEvent::Expired { record: to },
                GarbageEvent::SetLifecycle { lifecycle, .. } => GarbageEvent::SetLifecycle {
                    record: to,
                    lifecycle,
                },
            };
            // Reducers derive accounting from the actual overlay transition, not this hint.
            forwarded.summary_delta = Default::default();
            by_segment.entry(from.segment_id).or_default().push(current);
            current = forwarded;
        }
        Ok(())
    }
}
