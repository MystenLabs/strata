use serde::{Deserialize, Serialize};

/// Checksum algorithms supported by the Strata record format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Xxh3_128,
}

impl ChecksumAlgorithm {
    pub const fn code(self) -> u32 {
        match self {
            Self::Xxh3_128 => 1,
        }
    }

    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Self::Xxh3_128),
            _ => None,
        }
    }
}

/// A checksum value with its algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: u128,
}

impl Checksum {
    pub const fn new(algorithm: ChecksumAlgorithm, value: u128) -> Self {
        Self { algorithm, value }
    }

    pub const fn xxh3_128_value(value: u128) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Xxh3_128,
            value,
        }
    }

    pub fn xxh3_128(bytes: &[u8]) -> Self {
        Self::xxh3_128_value(xxhash_rust::xxh3::xxh3_128(bytes))
    }

    pub fn compute(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Self {
        match algorithm {
            ChecksumAlgorithm::Xxh3_128 => Self::xxh3_128(bytes),
        }
    }
}
