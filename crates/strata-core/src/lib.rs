//! Stable storage vocabulary and record wire format shared by all Strata crates.
//!
//! This crate owns the types that are used everywhere: blob keys, record headers, and segment metadata.
//! Record v4 is append-friendly and self-validating:
//!
//! ```text
//! +----------------------+----------------------+----------------------+
//! | fixed header (48 B)  | payload (N bytes)    | blob key (K bytes)   |
//! +----------------------+----------------------+----------------------+
//!
//! fixed header:
//!   magic | version | header_len | key_len | generation | payload_len
//!   xxh3_128_checksum | checksum_algorithm | shard_id | shard_generation
//! ```
//!
//! The record checksum is computed over:
//!
//! ```text
//! header(xxh3_128_checksum = 0) || payload || key
//! ```
//!
//! Core invariants:
//!
//! - `BlobKey` is non-empty and bounded by the record format limit.
//! - `RecordRef` is a physical pointer into one segment and includes the encoded record length.
//! - `SegmentState` is durable metadata; segment bytes live outside this crate.

mod checksum;
mod error;
mod ids;
mod key;
mod lifecycle;
mod put;
mod record;
mod segment;

pub use checksum::{Checksum, ChecksumAlgorithm};
pub use error::{Error, Result};
pub use ids::{
    BlobState, Epoch, Generation, RecordRef, ShardCleanupJob, ShardCleanupState, ShardGeneration,
    ShardId, ShardInfo, ShardKey, ShardState, StrataLsn,
};
pub use key::{BlobKey, BlobKeyError};
pub use lifecycle::{
    BlobLifecycle, BlobLifecycleAction, BlobLifecycleHead, BlobLifecycleMergeOp, BlobLifecycleOp,
    BlobLifecycleState, BlobLifetimeHead, ShardLsnKey, ShardStoreStateKey, StoreStateKey,
};
pub use put::PutEntry;
pub use put::{PutHead, PutMergeOp, PutOp, PutState};
pub use record::{
    DEFAULT_RECORD_SHARD, DecodedRecord, EncodedRecordParts, FIXED_RECORD_HEADER_LEN, RECORD_MAGIC,
    RECORD_VERSION, RecordHeader, RecordHeaderFields, encoded_record_len,
};
pub use segment::{
    EpochBucket, GarbageEvent, PlacementClass, SegmentFileState, SegmentGcLifetimeRange,
    SegmentGcLifetimeUpdate, SegmentGcLiveRecord, SegmentGcOverlay, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentGcSummary, SegmentGcSummaryDelta, SegmentId, SegmentKey,
    SegmentOwner, SegmentState, VolumeId,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
