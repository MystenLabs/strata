//! Stable storage vocabulary and record wire format shared by all Strata crates.
//!
//! This crate owns the types that are used everywhere: blob keys, record headers, and segment metadata.
//! Record v3 is append-friendly and self-validating:
//!
//! ```text
//! +----------------------+----------------------+----------------------+
//! | fixed header (48 B)  | payload (N bytes)    | blob key (K bytes)   |
//! +----------------------+----------------------+----------------------+
//!
//! fixed header:
//!   magic | version | header_len | key_len | generation | payload_len
//!   xxh3_128_checksum | checksum_algorithm
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
//! - `BlobVersionKey` orders versions by logical LSN for one blob key.
//! - `SegmentState` is durable metadata; segment bytes live outside this crate.

mod checksum;
mod error;
mod key;
mod lifecycle;
mod record;
mod segment;

pub use checksum::{Checksum, ChecksumAlgorithm};
pub use error::{Error, Result};
pub use key::{BlobKey, BlobKeyError};
pub use lifecycle::{
    BlobEntry, BlobLifecycle, BlobLifecycleAction, BlobLifecycleHead, BlobLifecycleMergeOp,
    BlobLifecycleOp, BlobLifecycleState, BlobLifetimeHead, BlobState, BlobVersionKey,
    BlobVersionState, Epoch, Generation, RecordRef, ShardGeneration, ShardHead, ShardId, ShardInfo,
    ShardKey, ShardLsnKey, ShardState, ShardStoreStateKey, StoreStateKey, StrataLsn,
    StrataStoreState, VersionMergeOp, VersionOp, VersionState,
};
pub use record::{
    DecodedRecord, EncodedRecordParts, FIXED_RECORD_HEADER_LEN, RECORD_MAGIC, RECORD_VERSION,
    RecordHeader, RecordHeaderFields, encoded_record_len,
};
pub use segment::{
    EpochBucket, PlacementClass, SegmentFileState, SegmentId, SegmentKey, SegmentState,
    SegmentStats, VolumeId,
};
