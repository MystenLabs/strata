//! Relocations activated while blob-LSM compaction passes were in flight.
//!
//! A pass reads the relocation LSM once, when it starts, and heals the rows it rewrites against
//! that view. Every garbage event it emits names the physical copy its rows point at. GC keeps
//! publishing while the pass runs, so by the time the pass publishes, some of those copies may
//! have moved: the merge retired key "a" at S7 offset 100, but GC has since copied that record to
//! S42 and S7 is on its way out. S7's summary does not need the event; S42's does, or the copy
//! there is counted live forever.
//!
//! This table is how a pass finds out. GC records every activation here in the same critical
//! section that publishes it (the garbage-publication lock), and a pass, in that same lock,
//! redirects its events through the activations newer than its own relocation view before
//! appending its frame. Entries are kept only while a pass that could still name the old copy is
//! running: the oldest in-flight pass's watermark is the prune bound, and with no pass in flight
//! the table is emptied.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use core_types::{BlobKey, RecordRef, StrataLsn};

#[derive(Debug, Default)]
pub(crate) struct RelocationActivations {
    state: Mutex<ActivationState>,
}

#[derive(Debug, Default)]
struct ActivationState {
    /// (key, old copy) -> (activation sequence, new copy).
    by_source: BTreeMap<(BlobKey, RecordRef), (StrataLsn, RecordRef)>,
    /// Activation sequence -> the sources it moved, for pruning from the front.
    by_sequence: BTreeMap<StrataLsn, Vec<(BlobKey, RecordRef)>>,
    /// Relocation watermark of every in-flight pass, as a multiset.
    in_flight: BTreeMap<StrataLsn, usize>,
}

/// Registration of one in-flight pass; dropping it lets activations older than every remaining
/// pass be forgotten.
#[derive(Debug)]
pub(crate) struct PassRegistration {
    activations: Arc<RelocationActivations>,
    watermark: StrataLsn,
}

impl RelocationActivations {
    /// Registers a pass whose relocation view ends at `watermark` (the relocation LSM's last
    /// sequence when the pass started). Activations above it are retained until the pass ends.
    pub(crate) fn begin_pass(self: &Arc<Self>, watermark: StrataLsn) -> PassRegistration {
        let mut state = self.lock();
        *state.in_flight.entry(watermark).or_default() += 1;
        PassRegistration {
            activations: Arc::clone(self),
            watermark,
        }
    }

    /// Records the relocations one GC publish activated at `sequence`. Called under the
    /// garbage-publication lock, after the activation batch is durable.
    pub(crate) fn record(
        &self,
        sequence: StrataLsn,
        moved: impl IntoIterator<Item = (BlobKey, RecordRef, RecordRef)>,
    ) {
        let mut state = self.lock();
        if state.in_flight.is_empty() {
            // No pass can name these copies by their old location: every later pass reads a
            // relocation view that already contains them.
            return;
        }
        for (key, from, to) in moved {
            state
                .by_sequence
                .entry(sequence)
                .or_default()
                .push((key.clone(), from));
            state.by_source.insert((key, from), (sequence, to));
        }
    }

    /// Where the bytes a pass with relocation view `watermark` knows as `from` live now, or
    /// `None` when nothing newer than that view moved them. Follows chains, so a copy moved twice
    /// during one long pass resolves to its final home.
    pub(crate) fn resolve(
        &self,
        key: &BlobKey,
        from: RecordRef,
        watermark: StrataLsn,
    ) -> Option<RecordRef> {
        let state = self.lock();
        let mut current = from;
        let mut moved = false;
        while let Some(&(sequence, to)) = state.by_source.get(&(key.clone(), current)) {
            if sequence <= watermark || to == current {
                break;
            }
            current = to;
            moved = true;
        }
        moved.then_some(current)
    }

    fn end_pass(&self, watermark: StrataLsn) {
        let mut state = self.lock();
        match state.in_flight.get_mut(&watermark) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                state.in_flight.remove(&watermark);
            }
            None => {}
        }
        let keep_above = state.in_flight.keys().next().copied();
        let drop_through = match keep_above {
            Some(oldest) => oldest,
            None => StrataLsn::MAX,
        };
        let retained = state.by_sequence.split_off(&drop_through.saturating_add(1));
        let dropped = std::mem::replace(&mut state.by_sequence, retained);
        for (_, sources) in dropped {
            for source in sources {
                state.by_source.remove(&source);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().by_source.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ActivationState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for PassRegistration {
    fn drop(&mut self) {
        self.activations.end_pass(self.watermark);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use core_types::{BlobKey, RecordRef, SegmentId};

    use super::RelocationActivations;

    fn at(segment: SegmentId, offset: u64) -> RecordRef {
        RecordRef {
            segment_id: segment,
            offset,
            len: 100,
        }
    }

    fn key() -> BlobKey {
        BlobKey::new(b"a".as_slice()).unwrap()
    }

    #[test]
    fn activations_after_the_pass_view_redirect_its_events() {
        let activations = Arc::new(RelocationActivations::default());
        let pass = activations.begin_pass(80);
        activations.record(87, [(key(), at(7, 100), at(42, 0))]);
        assert_eq!(activations.resolve(&key(), at(7, 100), 80), Some(at(42, 0)));
        // A pass that already saw sequence 87 healed the row itself.
        assert_eq!(activations.resolve(&key(), at(7, 100), 87), None);
        assert_eq!(activations.resolve(&key(), at(7, 200), 80), None);
        drop(pass);
        assert_eq!(activations.len(), 0, "nothing in flight keeps no entries");
    }

    #[test]
    fn chains_resolve_to_the_final_copy() {
        let activations = Arc::new(RelocationActivations::default());
        let _pass = activations.begin_pass(80);
        activations.record(87, [(key(), at(7, 100), at(42, 0))]);
        activations.record(95, [(key(), at(42, 0), at(99, 500))]);
        assert_eq!(
            activations.resolve(&key(), at(7, 100), 80),
            Some(at(99, 500))
        );
        // A pass that saw the first hop still needs the second.
        assert_eq!(
            activations.resolve(&key(), at(42, 0), 90),
            Some(at(99, 500))
        );
    }

    #[test]
    fn entries_live_as_long_as_the_oldest_pass_that_predates_them() {
        let activations = Arc::new(RelocationActivations::default());
        let old = activations.begin_pass(80);
        activations.record(87, [(key(), at(7, 100), at(42, 0))]);
        let young = activations.begin_pass(90);
        activations.record(95, [(key(), at(8, 0), at(43, 0))]);
        drop(young);
        assert_eq!(activations.len(), 2, "the old pass may still name both");
        drop(old);
        assert_eq!(activations.len(), 0);
        // Recorded with nothing in flight: nothing to redirect, nothing kept.
        activations.record(100, [(key(), at(9, 0), at(44, 0))]);
        assert_eq!(activations.len(), 0);
    }
}
