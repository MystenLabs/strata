use std::path::PathBuf;

use strata_core::{
    BlobKey, Epoch, SegmentFileState, SegmentId, ShardGeneration, ShardId, ShardState, StrataLsn,
};

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

    #[error("index error: {0}")]
    Index(#[from] strata_index::Error),

    #[error("segment error: {0}")]
    Segment(#[from] strata_segment::Error),

    #[error("LSM error: {0}")]
    Lsm(#[from] strata_lsm::Error),

    #[error("file sync queue is closed")]
    FileSyncQueueClosed,

    #[error("store WAL sync failed: {0}")]
    WalSyncFailed(String),

    #[error("invalid store WAL: {0}")]
    InvalidWal(String),

    #[error("corrupt store WAL at {path}: {reason}")]
    CorruptWal { path: PathBuf, reason: String },

    #[error(
        "store WAL append rollback failed for {path} at offset {offset} after write error: {write_error}; rollback error: {rollback_error}"
    )]
    WalAppendRollbackFailed {
        path: PathBuf,
        offset: u64,
        write_error: std::io::Error,
        #[source]
        rollback_error: std::io::Error,
    },

    #[error("store WAL LSNs must increase: previous {previous:?}, next {next:?}")]
    WalLsnOutOfOrder {
        previous: StrataLsn,
        next: StrataLsn,
    },

    #[error("invalid relocation: {0}")]
    InvalidRelocation(String),

    #[error("gc selection error: {0}")]
    GcSelection(#[from] strata_gc::GcSelectionError),

    #[error("invalid gc plan: {0}")]
    GcInvalidPlan(&'static str),

    #[error("record key mismatch: requested {requested:?}, found {found:?}")]
    KeyMismatch { requested: BlobKey, found: BlobKey },

    #[error("write queue is closed")]
    WriteQueueClosed,

    #[error("write coordinator dropped the response")]
    WriteResponseDropped,

    #[error("durability queue is closed")]
    DurabilityQueueClosed,

    #[error("gc queue is closed")]
    GcQueueClosed,

    #[error("store is halted after terminal failure: {reason}")]
    StoreHalted { reason: String },

    #[error("store invariant violation: {reason}")]
    InvariantViolation { reason: String },

    #[error("epoch metadata is not initialized")]
    EpochNotInitialized,

    #[error(
        "invalid blob lifetime end epoch {logical_end_epoch}; it must be greater than current epoch {current_epoch}"
    )]
    InvalidBlobLifetime {
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },

    #[error("shard {shard_id} does not exist")]
    ShardNotFound { shard_id: ShardId },

    #[error("shard {shard_id} generation overflow at {current_generation}")]
    ShardGenerationOverflow {
        shard_id: ShardId,
        current_generation: ShardGeneration,
    },

    #[error(
        "shard {shard_id} generation {generation} is not active; current generation {current_generation}, state {state:?}"
    )]
    ShardUnavailable {
        shard_id: ShardId,
        generation: ShardGeneration,
        current_generation: ShardGeneration,
        state: ShardState,
    },

    #[error("gc source segment {segment_id} has no index state")]
    GcMissingSourceSegment { segment_id: SegmentId },

    #[error("gc staged output segment {staged_segment_id} is missing")]
    GcMissingStagedOutput { staged_segment_id: SegmentId },

    #[error("gc output segment {segment_id} already exists at {path}")]
    GcOutputSegmentExists {
        segment_id: SegmentId,
        path: PathBuf,
    },

    #[error("gc source segment {segment_id} is not sealed: {state:?}")]
    GcSourceSegmentNotSealed {
        segment_id: SegmentId,
        state: SegmentFileState,
    },

    #[error("gc source segment {segment_id} is not empty: {live_ref_count} live refs")]
    GcSourceSegmentNotEmpty {
        segment_id: SegmentId,
        live_ref_count: u64,
    },

    #[error(
        "gc source segment {segment_id} valid prefix mismatch for {path}: expected {expected_len}, got {valid_len}"
    )]
    GcSourceSegmentInvalidPrefix {
        segment_id: SegmentId,
        path: PathBuf,
        expected_len: u64,
        valid_len: u64,
    },

    #[error(
        "gc overlay for segment {segment_id} partially overlaps record range at offset {offset} len {len}"
    )]
    GcOverlayPartialRecordRange {
        segment_id: SegmentId,
        offset: u64,
        len: u64,
    },

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

impl Error {
    /// Returns a bounded Prometheus label for a background GC failure.
    ///
    /// The full error is emitted to stderr by the GC worker. This classification intentionally
    /// excludes paths, segment ids, and error messages so the metric cannot create unbounded label
    /// cardinality.
    pub(crate) fn gc_failure_reason(&self) -> &'static str {
        match self {
            Self::Io { .. } => "io",
            Self::Index(_) => "index",
            Self::Segment(_) => "segment",
            Self::GcSelection(_) => "selection",
            Self::GcInvalidPlan(_) => "invalid_plan",
            Self::WriteQueueClosed | Self::WriteResponseDropped | Self::GcQueueClosed => "queue",
            Self::StoreHalted { .. } => "store_halted",
            Self::InvariantViolation { .. } => "invariant_violation",
            Self::GcMissingSourceSegment { .. } => "missing_source",
            Self::GcMissingStagedOutput { .. } => "missing_staged_output",
            Self::GcOutputSegmentExists { .. } => "output_exists",
            Self::GcSourceSegmentNotSealed { .. } => "source_not_sealed",
            Self::GcSourceSegmentNotEmpty { .. } => "source_not_empty",
            Self::GcSourceSegmentInvalidPrefix { .. } => "invalid_source_prefix",
            Self::GcOverlayPartialRecordRange { .. } => "overlay_partial_record",
            Self::SealedSegmentMissing { .. }
            | Self::SealedSegmentMissingLength { .. }
            | Self::SealedSegmentMissingChecksum { .. }
            | Self::SealedSegmentLengthMismatch { .. }
            | Self::SealedSegmentChecksumMismatch { .. } => "sealed_segment_integrity",
            _ => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_failure_reasons_are_specific_and_bounded() {
        let source_state_error = Error::GcSourceSegmentNotSealed {
            segment_id: 7,
            state: SegmentFileState::Deleted,
        };
        assert_eq!(source_state_error.gc_failure_reason(), "source_not_sealed");

        let io_error = Error::Io {
            path: PathBuf::from("/tmp/gc-output"),
            source: std::io::Error::other("test failure"),
        };
        assert_eq!(io_error.gc_failure_reason(), "io");

        assert_eq!(Error::InvalidConfig("test").gc_failure_reason(), "other");
    }
}
