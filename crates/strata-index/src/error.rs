/// Result type used by `strata-index`.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors emitted by Strata index operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("typed-store error: {0}")]
    TypedStore(#[from] typed_store::TypedStoreError),

    #[error("rocksdb error: {0}")]
    RocksDb(String),
}
