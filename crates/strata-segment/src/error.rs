use std::path::PathBuf;

/// Result type used by `strata-segment`.
pub type Result<T> = std::result::Result<T, Error>;

/// Segment file errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("core format error: {0}")]
    Core(#[from] strata_core::Error),

    #[error(
        "record ref points at segment {actual_segment_id}, expected segment {expected_segment_id}"
    )]
    WrongSegment {
        expected_segment_id: strata_core::SegmentId,
        actual_segment_id: strata_core::SegmentId,
    },

    #[error("payload ref range overflow")]
    RangeOverflow,

    #[error(
        "record ref length mismatch: record_ref.len={record_ref_len}, encoded_record_len={encoded_record_len}"
    )]
    InvalidRecordRefLength {
        record_ref_len: u64,
        encoded_record_len: u64,
    },

    #[error("invalid payload range {range_start}..{range_end} for payload length {payload_len}")]
    InvalidPayloadRange {
        payload_len: u64,
        range_start: u64,
        range_end: u64,
    },

    #[error("durable offset {durable_offset} is beyond valid segment prefix {valid_len} in {path}")]
    InvalidDurableOffset {
        path: PathBuf,
        durable_offset: u64,
        valid_len: u64,
    },

    #[error("segment would exceed max size: max={max_size}, attempted={attempted_size}")]
    SegmentFull { max_size: u64, attempted_size: u64 },
}

pub(crate) trait IoResultExt<T> {
    fn at_path(self, path: impl Into<PathBuf>) -> Result<T>;
}

impl<T> IoResultExt<T> for std::io::Result<T> {
    fn at_path(self, path: impl Into<PathBuf>) -> Result<T> {
        self.map_err(|source| Error::Io {
            path: path.into(),
            source,
        })
    }
}
