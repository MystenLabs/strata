//! Store-owned blob state stored behind one exact LSM key.
//!
//! Every blob key materializes to one [`BlobState`]: the latest [`BlobVersion`] per shard plus an
//! optional blob-wide [`BlobLifetime`]. Writers never read-modify-write that state — they append
//! encoded [`BlobMutation`] patches (Put / SetLifetime / Tombstone) and the LSM folds them in
//! during reads, partial merges, and compactions. Every version that becomes unreachable is
//! accounted for by exactly one terminal garbage record.
//!
//! Queued lifecycle events carry a caller-supplied event index in the same operand as their effect.
//! The materialized state retains the highest applied index, even after deletion or expiry. Full
//! merges ignore equal/older events; partial merges preserve event-bearing runs because the base
//! may already contain their replay marker. Existing v3 records remain readable and are still
//! emitted for keys without events; event operands and states with replay markers use v4.
//!
//! The submodules follow the data path:
//!
//! - [`mod@format`]: wire encoding for mutation patches and the materialized state.
//! - `merge`: the two `MergeOperator` entry points. [`BlobMerge`] serves reads and plain
//!   compactions; `BlobMergeWithRelocations` runs during snapshot compactions, where it also
//!   heals relocated record references and prunes retired or expired versions.
//! - `reduce`: collapses a run of patches without seeing the materialized base (partial merge).
//! - `state`: applies mutations to a materialized [`BlobState`] in LSN order (full merge).
//! - `snapshot`: `BlobCompactionSnapshot`, the global facts (epoch transitions, shard fences,
//!   bulk-reclaimed segments) captured once per compaction so merges never query the index.
//! - `garbage`: builds the terminal `GarbageRecord`s and their GC summary deltas.

pub mod format;
mod garbage;
mod merge;
mod reduce;
mod snapshot;
mod state;

pub use format::{BlobLifetime, BlobState, BlobVersion};
pub(crate) use format::{BlobMutation, global_operand_floor};
pub(crate) use garbage::terminal_garbage_record;
pub use merge::BlobMerge;
pub(crate) use merge::BlobMergeWithRelocations;
pub(crate) use snapshot::BlobCompactionSnapshot;
pub(crate) use state::effective_lifecycle;

#[cfg(test)]
mod tests;
