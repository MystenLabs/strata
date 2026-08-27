//! Store-owned blob state stored behind one exact LSM key.
//!
//! Every blob key materializes to one [`BlobState`]: the latest [`BlobVersion`] per shard plus an
//! optional blob-wide [`BlobLifetime`]. Writers never read-modify-write that state — they append
//! encoded [`BlobMutation`] patches (Put / SetLifetime / Tombstone) and the LSM folds them in
//! during reads, partial merges, and compactions. Every version that becomes unreachable is
//! accounted for by exactly one terminal garbage record.
//!
//! The submodules follow the data path:
//!
//! - [`format`]: wire encoding for mutation patches and the materialized state.
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

pub(crate) use format::BlobMutation;
pub use format::{BlobLifetime, BlobState, BlobVersion};
pub(crate) use garbage::terminal_garbage_record;
pub use merge::BlobMerge;
pub(crate) use merge::BlobMergeWithRelocations;
pub(crate) use snapshot::BlobCompactionSnapshot;

#[cfg(test)]
mod tests;
