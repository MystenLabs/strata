use serde::{Deserialize, Serialize};

/// Checksum algorithms supported by the Strata record format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Crc32,
}

impl ChecksumAlgorithm {
    pub const fn code(self) -> u32 {
        match self {
            Self::Crc32 => 0,
        }
    }

    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            0 => Some(Self::Crc32),
            _ => None,
        }
    }
}

/// A checksum value with its algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: u32,
}

impl Checksum {
    pub fn crc32(bytes: &[u8]) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            value: crc32fast::hash(bytes),
        }
    }

    pub fn compute(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Self {
        match algorithm {
            ChecksumAlgorithm::Crc32 => Self::crc32(bytes),
        }
    }
}
