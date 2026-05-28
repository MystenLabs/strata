use crate::{BlobKeyError, Checksum};

/// Result type used by `strata-core`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors emitted while validating core types or decoding Strata records.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("blob key error: {0}")]
    BlobKey(#[from] BlobKeyError),

    #[error("buffer is too short: needed at least {needed} bytes, got {actual}")]
    BufferTooShort { needed: usize, actual: usize },

    #[error("invalid record magic: expected {expected:#x}, got {actual:#x}")]
    InvalidMagic { expected: u32, actual: u32 },

    #[error("unsupported record version: {0}")]
    UnsupportedVersion(u16),

    #[error("invalid record header length: expected {expected}, got {actual}")]
    InvalidHeaderLength { expected: u16, actual: u16 },

    #[error("unsupported checksum algorithm code: {0}")]
    UnsupportedChecksumAlgorithm(u32),

    #[error("record key length exceeds limit: {0}")]
    KeyTooLarge(u32),

    #[error("record payload length exceeds limit: {0}")]
    PayloadTooLarge(u64),

    #[error("invalid payload length: expected {expected} bytes, got {actual}")]
    InvalidPayloadLength { expected: u64, actual: u64 },

    #[error("record length overflow")]
    RecordLengthOverflow,

    #[error("record checksum mismatch: expected {expected:?}, actual {actual:?}")]
    RecordChecksumMismatch {
        expected: Checksum,
        actual: Checksum,
    },
}
