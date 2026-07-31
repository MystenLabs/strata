/// Result type used by `strata-index`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors emitted by Strata index operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("typed-store error: {0}")]
    TypedStore(#[from] typed_store::TypedStoreError),

    #[error("rocksdb error: {0}")]
    RocksDb(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid LSM manifest: {0}")]
    InvalidLsmManifest(String),

    #[error("invalid LSM checkpoint: {0}")]
    InvalidLsmCheckpoint(String),

    #[error(transparent)]
    Lsm(#[from] strata_lsm::Error),

    #[error("invalid garbage-log sweep: {0}")]
    InvalidGarbageSweep(String),
}
