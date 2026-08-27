//! Public facade for the Strata blob store.
//!
//! Most consumers should depend on this crate instead of the internal workspace crates. The
//! lower-level crates remain split out to keep storage vocabulary, segment I/O, metadata indexing,
//! accounting, GC planning, and store orchestration independently testable.

pub use core_types::{
    BlobKey, BlobKeyError, Epoch, Generation, PlacementClass, RecordRef, SegmentFileState,
    SegmentId, ShardGeneration, ShardId, ShardInfo, ShardKey, ShardState, StrataLsn,
};
pub use store::*;
