use std::fmt;

use serde::{Deserialize, Serialize};

/// Maximum encoded blob key length accepted by the record format.
pub const MAX_BLOB_KEY_LEN: usize = 4 * 1024;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobKey(Vec<u8>);

impl BlobKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, BlobKeyError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(BlobKeyError::Empty);
        }
        if bytes.len() > MAX_BLOB_KEY_LEN {
            return Err(BlobKeyError::TooLarge(bytes.len()));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl TryFrom<Vec<u8>> for BlobKey {
    type Error = BlobKeyError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&[u8]> for BlobKey {
    type Error = BlobKeyError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::new(value.to_vec())
    }
}

impl fmt::Debug for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BlobKey")
            .field(&format_args!("{} bytes", self.0.len()))
            .finish()
    }
}

/// Errors emitted while constructing a [`BlobKey`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlobKeyError {
    #[error("blob key cannot be empty")]
    Empty,

    #[error("blob key is too large: {0} bytes")]
    TooLarge(usize),
}
