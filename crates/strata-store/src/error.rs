use std::path::PathBuf;

use strata_core::{BlobKey, SegmentId};

/// Result type used by `strata-store`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors emitted by high-level Strata store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("io error for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to spawn write coordinator: {source}")]
    ThreadSpawn {
        #[source]
        source: std::io::Error,
    },

    #[error("failed to spawn seal worker: {source}")]
    SealThreadSpawn {
        #[source]
        source: std::io::Error,
    },

    #[error("index error: {0}")]
    Index(#[from] strata_index::Error),

    #[error("segment error: {0}")]
    Segment(#[from] strata_segment::Error),

    #[error("record key mismatch: requested {requested:?}, found {found:?}")]
    KeyMismatch { requested: BlobKey, found: BlobKey },

    #[error("write queue is closed")]
    WriteQueueClosed,

    #[error("write coordinator dropped the response")]
    WriteResponseDropped,

    #[error("seal queue is closed")]
    SealQueueClosed,

    #[error("epoch metadata is not initialized")]
    EpochNotInitialized,

    #[error("segment {segment_id} failed sealing")]
    SealFailed { segment_id: SegmentId },

    #[error("orphan segment file {segment_id} has no index state at {path}")]
    OrphanSegmentFile {
        segment_id: SegmentId,
        path: PathBuf,
    },

    #[error("sealed segment {segment_id} is missing at {path}")]
    SealedSegmentMissing {
        segment_id: SegmentId,
        path: PathBuf,
    },

    #[error("sealed segment {segment_id} is missing sealed length metadata")]
    SealedSegmentMissingLength { segment_id: SegmentId },

    #[error("sealed segment {segment_id} is missing checksum metadata")]
    SealedSegmentMissingChecksum { segment_id: SegmentId },

    #[error(
        "sealed segment {segment_id} length mismatch for {path}: expected {expected_len}, got {actual_len}"
    )]
    SealedSegmentLengthMismatch {
        segment_id: SegmentId,
        path: PathBuf,
        expected_len: u64,
        actual_len: u64,
    },

    #[error(
        "sealed segment {segment_id} checksum mismatch for {path}: expected {expected:?}, actual {actual:?}"
    )]
    SealedSegmentChecksumMismatch {
        segment_id: SegmentId,
        path: PathBuf,
        expected: [u8; 32],
        actual: [u8; 32],
    },

    #[error(
        "recovery found segment {segment_id} at offset {recovered_write_offset}, expected {expected_write_offset}"
    )]
    RecoveryInconsistent {
        segment_id: SegmentId,
        expected_write_offset: u64,
        recovered_write_offset: u64,
    },
}
