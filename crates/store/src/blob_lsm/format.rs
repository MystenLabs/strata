//! Persisted operation and materialized-state formats for the blob LSM.

use std::collections::BTreeMap;

use core_types::{BlobLifecycle, Epoch, RecordRef, ShardKey};
use lsm::{Error, Result, StrataLsn, decode_record_ref, encode_record_ref};

const VERSION: u8 = 3;
const PUT: u8 = 1;
const SET_LIFETIME: u8 = 2;
const TOMBSTONE: u8 = 3;
const BATCH: u8 = 4;

/// One self-contained logical mutation to a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlobMutation {
    Put {
        shard: ShardKey,
        write_epoch: Epoch,
        record_ref: RecordRef,
    },
    SetLifetime {
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    Tombstone {
        shard: ShardKey,
    },
}

impl BlobMutation {
    /// Encodes the Store-owned fields available before the LSM assigns a record reference.
    pub(crate) fn encode_put_metadata(shard: ShardKey, write_epoch: Epoch) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(VERSION);
        bytes.push(PUT);
        push_shard(&mut bytes, shard);
        push_u64(&mut bytes, write_epoch);
        bytes
    }

    /// Completes a segment-backed Put using the record reference assigned by the LSM.
    pub(crate) fn decode_put_metadata(bytes: &[u8], record_ref: RecordRef) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        decoder.version()?;
        let tag = decoder.u8()?;
        if tag != PUT {
            return Err(invalid(format!(
                "expected segment-backed Put tag, found {tag}"
            )));
        }
        let mutation = Self::Put {
            shard: decoder.shard()?,
            write_epoch: decoder.u64()?,
            record_ref,
        };
        decoder.finish()?;
        Ok(mutation)
    }

    /// Encodes a metadata-only mutation stored directly in an LSM patch row.
    pub(crate) fn encode_inline(self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.push(VERSION);
        match self {
            Self::Put { .. } => {
                return Err(invalid("Put must be segment-backed"));
            }
            Self::SetLifetime {
                logical_end_epoch,
                current_epoch,
            } => {
                bytes.push(SET_LIFETIME);
                push_u64(&mut bytes, logical_end_epoch);
                push_u64(&mut bytes, current_epoch);
            }
            Self::Tombstone { shard } => {
                bytes.push(TOMBSTONE);
                push_shard(&mut bytes, shard);
            }
        }
        Ok(bytes)
    }
}

/// A logical blob mutation paired with the LSN at which it originally occurred.
///
/// Partial merges retain this pair because the outer patch row has only one LSN.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobMutationWithLSN {
    pub(crate) lsn: StrataLsn,
    pub(crate) mutation: BlobMutation,
}

impl BlobMutationWithLSN {
    pub(crate) fn encode_batch(mutations: &[Self]) -> Result<Vec<u8>> {
        let count = u32::try_from(mutations.len())
            .map_err(|_| invalid("too many mutations in one blob patch"))?;
        let mut bytes = Vec::new();
        bytes.push(VERSION);
        bytes.push(BATCH);
        bytes.extend_from_slice(&count.to_le_bytes());
        for mutation in mutations {
            push_lsn(&mut bytes, mutation.lsn);
            match mutation.mutation {
                BlobMutation::Put {
                    shard,
                    write_epoch,
                    record_ref,
                } => {
                    bytes.push(PUT);
                    push_shard(&mut bytes, shard);
                    push_u64(&mut bytes, write_epoch);
                    bytes.extend_from_slice(&encode_record_ref(record_ref));
                }
                BlobMutation::SetLifetime {
                    logical_end_epoch,
                    current_epoch,
                } => {
                    bytes.push(SET_LIFETIME);
                    push_u64(&mut bytes, logical_end_epoch);
                    push_u64(&mut bytes, current_epoch);
                }
                BlobMutation::Tombstone { shard } => {
                    bytes.push(TOMBSTONE);
                    push_shard(&mut bytes, shard);
                }
            }
        }
        Ok(bytes)
    }

    /// Decodes either one metadata-only mutation or a partial-merge batch.
    pub(crate) fn decode_inline(outer_lsn: StrataLsn, bytes: &[u8]) -> Result<Vec<Self>> {
        let mut decoder = Decoder::new(bytes);
        decoder.version()?;
        let tag = decoder.u8()?;
        let mutations = match tag {
            PUT => return Err(invalid("Put must be segment-backed")),
            SET_LIFETIME => vec![Self {
                lsn: outer_lsn,
                mutation: BlobMutation::SetLifetime {
                    logical_end_epoch: decoder.u64()?,
                    current_epoch: decoder.u64()?,
                },
            }],
            TOMBSTONE => vec![Self {
                lsn: outer_lsn,
                mutation: BlobMutation::Tombstone {
                    shard: decoder.shard()?,
                },
            }],
            BATCH => {
                let count = decoder.u32()?;
                let mut mutations = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let lsn = decoder.lsn()?;
                    let mutation = match decoder.u8()? {
                        PUT => BlobMutation::Put {
                            shard: decoder.shard()?,
                            write_epoch: decoder.u64()?,
                            record_ref: decoder.record_ref()?,
                        },
                        SET_LIFETIME => BlobMutation::SetLifetime {
                            logical_end_epoch: decoder.u64()?,
                            current_epoch: decoder.u64()?,
                        },
                        TOMBSTONE => BlobMutation::Tombstone {
                            shard: decoder.shard()?,
                        },
                        tag => {
                            return Err(invalid(format!("unknown batched mutation tag {tag}")));
                        }
                    };
                    mutations.push(Self { lsn, mutation });
                }
                if mutations.is_empty()
                    || mutations
                        .last()
                        .is_some_and(|mutation| mutation.lsn > outer_lsn)
                {
                    return Err(invalid(
                        "batched patch contains no mutations or exceeds its outer LSN",
                    ));
                }
                validate_mutation_lsns(&mutations)?;
                mutations
            }
            tag => return Err(invalid(format!("unknown mutation tag {tag}"))),
        };
        decoder.finish()?;
        Ok(mutations)
    }
}

fn validate_mutation_lsns(mutations: &[BlobMutationWithLSN]) -> Result<()> {
    for pair in mutations.windows(2) {
        if pair[0].lsn >= pair[1].lsn {
            return Err(invalid("batched mutation LSNs are not strictly increasing"));
        }
    }
    Ok(())
}

/// The latest payload for one shard generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobVersion {
    pub lsn: StrataLsn,
    pub write_epoch: Epoch,
    pub record_ref: RecordRef,
}

/// The current blob-level lifetime and the operation which established it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLifetime {
    pub lsn: StrataLsn,
    pub lifecycle: BlobLifecycle,
}

/// Fully materialized Store state for one exact blob key.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlobState {
    pub versions: BTreeMap<ShardKey, BlobVersion>,
    pub lifetime: Option<BlobLifetime>,
}

impl BlobState {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let count = u32::try_from(self.versions.len())
            .map_err(|_| invalid("too many shard versions in one blob state"))?;
        let mut bytes = Vec::with_capacity(6 + self.versions.len() * 60);
        bytes.push(VERSION);
        bytes.extend_from_slice(&count.to_le_bytes());
        for (shard, version) in &self.versions {
            push_shard(&mut bytes, *shard);
            push_lsn(&mut bytes, version.lsn);
            push_u64(&mut bytes, version.write_epoch);
            bytes.extend_from_slice(&encode_record_ref(version.record_ref));
        }
        match self.lifetime {
            Some(lifetime) => {
                bytes.push(1);
                push_lsn(&mut bytes, lifetime.lsn);
                push_u64(&mut bytes, lifetime.lifecycle.logical_end_epoch);
                bytes.extend_from_slice(&lifetime.lifecycle.extension_count.to_le_bytes());
            }
            None => bytes.push(0),
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        decoder.version()?;
        let count = decoder.u32()?;
        let mut versions = BTreeMap::new();
        for _ in 0..count {
            let shard = decoder.shard()?;
            let version = BlobVersion {
                lsn: decoder.lsn()?,
                write_epoch: decoder.u64()?,
                record_ref: decoder.record_ref()?,
            };
            if versions.insert(shard, version).is_some() {
                return Err(invalid("duplicate shard in blob state"));
            }
        }
        let lifetime = match decoder.u8()? {
            0 => None,
            1 => Some(BlobLifetime {
                lsn: decoder.lsn()?,
                lifecycle: BlobLifecycle {
                    logical_end_epoch: decoder.u64()?,
                    extension_count: decoder.u32()?,
                },
            }),
            tag => return Err(invalid(format!("invalid lifetime marker {tag}"))),
        };
        let state = Self { versions, lifetime };
        decoder.finish()?;
        Ok(state)
    }
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_shard(bytes: &mut Vec<u8>, shard: ShardKey) {
    bytes.extend_from_slice(&shard.id.to_le_bytes());
    push_u64(bytes, shard.generation);
}

fn push_lsn(bytes: &mut Vec<u8>, lsn: StrataLsn) {
    push_u64(bytes, lsn);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn version(&mut self) -> Result<()> {
        let version = self.u8()?;
        if version != VERSION {
            return Err(invalid(format!("unsupported version {version}")));
        }
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated encoding"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn shard(&mut self) -> Result<ShardKey> {
        Ok(ShardKey {
            id: self.u32()?,
            generation: self.u64()?,
        })
    }

    fn lsn(&mut self) -> Result<StrataLsn> {
        self.u64()
    }

    fn record_ref(&mut self) -> Result<RecordRef> {
        decode_record_ref(self.take(24)?)
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(invalid("trailing bytes"));
        }
        Ok(())
    }
}

pub(crate) fn invalid(reason: impl Into<String>) -> Error {
    Error::Merge(format!("invalid Store blob encoding: {}", reason.into()))
}
