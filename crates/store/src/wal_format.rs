//! Logical record format for the store-owned WAL.
//!
//! The enum is the routing table. A reader never has to infer a destination from key bytes:
//!
//! ```text
//! Blob(..)       -> blob LSM
//! Relocation(..) -> relocation LSM
//! Epoch(..)      -> RocksDB epoch tables
//! ShardDrop(..)  -> RocksDB shard registry and cleanup queue
//! ```
//!
//! Payload bytes are not duplicated here. `Blob::PutBlob` stores the `RecordRef` produced by the
//! store's payload segment writer; `sync()` makes that segment durable before syncing this WAL.

use crate::{Error, Result, relocation::RelocationEntry};
use core_types::{BlobKey, Epoch, RecordRef, ShardKey};
use lsm::{Mutation, decode_record_ref, encode_record_ref};

const MAGIC: &[u8; 4] = b"STW1";
const BLOB_PUT: u8 = 1;
const BLOB_PUT_PREFIX: u8 = 2;
const BLOB_PUT_REF: u8 = 3;
const BLOB_PUT_PREFIX_REF: u8 = 4;
const EPOCH: u8 = 5;
const SHARD_DROP: u8 = 6;
const RELOCATION: u8 = 7;

/// One globally sequenced store mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreWalMutation {
    Blob(Mutation),
    Epoch { epoch: Epoch },
    ShardDrop { shard: ShardKey },
    Relocation(RelocationEntry),
}

impl StoreWalMutation {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        match self {
            Self::Blob(Mutation::Put {
                partition,
                key,
                value,
            }) => {
                out.push(BLOB_PUT);
                put_u32(&mut out, *partition);
                put_bytes(&mut out, key)?;
                put_bytes(&mut out, value)?;
            }
            Self::Blob(Mutation::PutPrefix {
                partition,
                key_prefix,
                key_suffix,
                value,
            }) => {
                out.push(BLOB_PUT_PREFIX);
                put_u32(&mut out, *partition);
                put_bytes(&mut out, key_prefix)?;
                put_bytes(&mut out, key_suffix)?;
                put_bytes(&mut out, value)?;
            }
            Self::Blob(Mutation::PutBlob {
                partition,
                key,
                metadata,
                record_ref,
            }) => {
                out.push(BLOB_PUT_REF);
                put_u32(&mut out, *partition);
                put_bytes(&mut out, key)?;
                put_bytes(&mut out, metadata)?;
                out.extend_from_slice(&encode_record_ref(*record_ref));
            }
            Self::Blob(Mutation::PutBlobPrefix {
                partition,
                key_prefix,
                key_suffix,
                metadata,
                record_ref,
            }) => {
                out.push(BLOB_PUT_PREFIX_REF);
                put_u32(&mut out, *partition);
                put_bytes(&mut out, key_prefix)?;
                put_bytes(&mut out, key_suffix)?;
                put_bytes(&mut out, metadata)?;
                out.extend_from_slice(&encode_record_ref(*record_ref));
            }
            Self::Epoch { epoch } => {
                out.push(EPOCH);
                out.extend_from_slice(&epoch.to_le_bytes());
            }
            Self::ShardDrop { shard } => {
                out.push(SHARD_DROP);
                out.extend_from_slice(&shard.id.to_le_bytes());
                out.extend_from_slice(&shard.generation.to_le_bytes());
            }
            Self::Relocation(entry) => {
                out.push(RELOCATION);
                put_bytes(&mut out, entry.key.as_bytes())?;
                out.extend_from_slice(&entry.shard.id.to_le_bytes());
                out.extend_from_slice(&entry.shard.generation.to_le_bytes());
                out.extend_from_slice(&entry.payload_lsn.to_le_bytes());
                out.extend_from_slice(&entry.publish_lsn.to_le_bytes());
                out.extend_from_slice(&encode_record_ref(entry.to));
            }
        }
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = Decoder::new(bytes);
        if input.take(MAGIC.len())? != MAGIC {
            return Err(invalid("store WAL record has the wrong magic"));
        }
        let kind = input.u8()?;
        let mutation = match kind {
            BLOB_PUT => Self::Blob(Mutation::Put {
                partition: input.u32()?,
                key: input.bytes()?,
                value: input.bytes()?,
            }),
            BLOB_PUT_PREFIX => Self::Blob(Mutation::PutPrefix {
                partition: input.u32()?,
                key_prefix: input.bytes()?,
                key_suffix: input.bytes()?,
                value: input.bytes()?,
            }),
            BLOB_PUT_REF => Self::Blob(Mutation::PutBlob {
                partition: input.u32()?,
                key: input.bytes()?,
                metadata: input.bytes()?,
                record_ref: input.record_ref()?,
            }),
            BLOB_PUT_PREFIX_REF => Self::Blob(Mutation::PutBlobPrefix {
                partition: input.u32()?,
                key_prefix: input.bytes()?,
                key_suffix: input.bytes()?,
                metadata: input.bytes()?,
                record_ref: input.record_ref()?,
            }),
            EPOCH => Self::Epoch {
                epoch: input.u64()?,
            },
            SHARD_DROP => Self::ShardDrop {
                shard: ShardKey {
                    id: input.u32()?,
                    generation: input.u64()?,
                },
            },
            RELOCATION => {
                let key = BlobKey::new(input.bytes()?).map_err(|error| {
                    invalid(format!("store WAL has an invalid relocation key: {error}"))
                })?;
                Self::Relocation(RelocationEntry {
                    key,
                    shard: ShardKey {
                        id: input.u32()?,
                        generation: input.u64()?,
                    },
                    payload_lsn: input.u64()?,
                    publish_lsn: input.u64()?,
                    to: input.record_ref()?,
                })
            }
            other => return Err(invalid(format!("unknown store WAL mutation kind {other}"))),
        };
        input.finish()?;
        Ok(mutation)
    }
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len =
        u32::try_from(bytes.len()).map_err(|_| invalid("store WAL field exceeds u32::MAX"))?;
    put_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("store WAL field length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated store WAL record"))?;
        self.offset = end;
        Ok(value)
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

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn record_ref(&mut self) -> Result<RecordRef> {
        decode_record_ref(self.take(24)?).map_err(Error::from)
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("store WAL record has trailing bytes"))
        }
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvariantViolation {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_explicit_and_round_trip() {
        let records = [
            StoreWalMutation::Blob(Mutation::Put {
                partition: 0,
                key: b"blob".to_vec(),
                value: b"patch".to_vec(),
            }),
            StoreWalMutation::Epoch { epoch: 9 },
            StoreWalMutation::ShardDrop {
                shard: ShardKey {
                    id: 4,
                    generation: 2,
                },
            },
        ];
        for record in records {
            assert_eq!(
                StoreWalMutation::decode(&record.encode().unwrap()).unwrap(),
                record
            );
        }
    }

    #[test]
    fn relocation_round_trips_with_its_publish_lsn() {
        let record = StoreWalMutation::Relocation(RelocationEntry {
            key: BlobKey::new(b"blob".to_vec()).unwrap(),
            shard: ShardKey {
                id: 3,
                generation: 7,
            },
            payload_lsn: 11,
            publish_lsn: 15,
            to: RecordRef {
                segment_id: 8,
                offset: 100,
                len: 20,
            },
        });
        assert_eq!(
            StoreWalMutation::decode(&record.encode().unwrap()).unwrap(),
            record
        );
    }
}
