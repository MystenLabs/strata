use crate::port::codec::{decode_value, encode_value};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Logical command identity shared by blob operations and epoch barriers. Zero is the empty
/// prefix. Never reuse revisions after row cleanup; these are unrelated to Strata's LSNs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Revision(pub u64);

impl Revision {
    pub fn next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| Error::InvalidPendingOperation("revision exhausted".into()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardGeneration {
    pub shard: u64,
    pub generation: u64,
}

/// Storage operations only: the embedder calculates aggregate lifetimes and expands pools.
/// The row key is an opaque blob identity; the adapter resolves its physical sliver keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobOperation {
    SetLifetime {
        end_epoch: u64,
    },
    Delete {
        shards: Vec<ShardGeneration>,
        cancellable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobCommand {
    pub revision: Revision,
    /// Opaque application provenance; never compared for ordering.
    pub source: Vec<u8>,
    pub operation: BlobOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochBarrier {
    V1 { epoch: u64, source: Vec<u8> },
}

/// Versioned row format, using the index’s MessagePack codec. Incompatible schema changes require a new version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingBlobOps {
    V1(Vec<BlobCommand>),
}

impl Default for PendingBlobOps {
    fn default() -> Self {
        Self::V1(Vec::new())
    }
}

impl PendingBlobOps {
    pub fn commands(&self) -> &[BlobCommand] {
        match self {
            Self::V1(commands) => commands,
        }
    }

    fn apply(&mut self, operand: BlobOperand) -> Result<()> {
        let Self::V1(commands) = self;
        let BlobOperand::V1(edit) = operand;
        let register = matches!(edit, BlobEdit::Register(_));
        match edit {
            BlobEdit::Acknowledge { through } => commands.retain(|c| c.revision > through),
            BlobEdit::Append(command) | BlobEdit::Register(command) => {
                if command.revision.0 == 0
                    || commands
                        .last()
                        .is_some_and(|c| c.revision >= command.revision)
                {
                    return Err(Error::InvalidPendingOperation(
                        "revisions must increase and start above zero".into(),
                    ));
                }
                if register {
                    if !matches!(command.operation, BlobOperation::SetLifetime { .. }) {
                        return Err(Error::InvalidPendingOperation(
                            "registration must initialize a lifetime".into(),
                        ));
                    }
                    commands.retain(|c| {
                        !matches!(
                            c.operation,
                            BlobOperation::Delete {
                                cancellable: true,
                                ..
                            }
                        )
                    });
                }
                commands.push(command);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobOperand {
    V1(BlobEdit),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobEdit {
    Append(BlobCommand),
    Register(BlobCommand),
    /// Only emit after Strata effects AND their replay identities are durable. This trims the
    /// applied prefix without dropping concurrent newer appends. Empty rows remain for bounded
    /// cleanup under the blob lock; do not delete a whole row using an old snapshot.
    Acknowledge {
        through: Revision,
    },
}

impl BlobOperand {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_value(self)
    }
}

/// RocksDB full-merge implementation; adapters install it on the pending-blob family and
/// disable partial merge. Resolving Register or Acknowledge without the base would discard
/// their effect on older operands. Corrupt/unknown data returns an error, never an empty queue.
pub fn merge_pending<'a>(
    existing: Option<&[u8]>,
    operands: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Vec<u8>> {
    let mut pending: PendingBlobOps = existing.map(decode_value).transpose()?.unwrap_or_default();
    for operand in operands {
        pending.apply(decode_value(operand)?)?;
    }
    encode_value(&pending)
}
