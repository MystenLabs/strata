/// Result type used by `index`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors emitted by Strata index operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rocksdb error: {0}")]
    RocksDb(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid LSM manifest: {0}")]
    InvalidLsmManifest(String),

    #[error("invalid store checkpoint: {0}")]
    InvalidStoreCheckpoint(String),

    #[error(transparent)]
    Lsm(#[from] lsm::Error),

    #[error("invalid garbage-log sweep: {0}")]
    InvalidGarbageSweep(String),
}
