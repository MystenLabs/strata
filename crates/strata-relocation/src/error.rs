pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid relocation: {0}")]
    Invalid(String),

    #[error("relocation LSM error: {0}")]
    Lsm(#[from] strata_lsm::Error),
}
