use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use strata_core::{BlobKey, RecordRef, SegmentId, ShardKey, StrataLsn};
use strata_lsm::{
    GarbageRecord, Lsm, LsmScan, MergeOperator, Mutation, Replace, StoredValue, WriteBatchResult,
    decode_value,
};

use crate::{Error, Result};

const KEY_SUFFIX_BYTES: usize = 20;
const RECORD_REF_BYTES: usize = 24;
const VALUE_BYTES: usize = 8 + RECORD_REF_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelocationEntry {
    pub key: BlobKey,
    pub shard: ShardKey,
    pub payload_lsn: StrataLsn,
    pub publish_lsn: StrataLsn,
    pub to: RecordRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Relocation {
    pub publish_lsn: StrataLsn,
    pub to: RecordRef,
}

pub struct RelocationStore {
    lsm: Arc<Lsm>,
}

pub struct RelocationMerge {
    dead_segments: HashSet<SegmentId>,
    examined: AtomicU64,
    dropped: Mutex<Vec<RelocationEntry>>,
}

pub struct RelocationScan {
    scan: LsmScan,
    current: Option<RelocationEntry>,
}

impl fmt::Debug for RelocationStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelocationStore")
            .field("manifest_generation", &self.lsm.manifest().generation)
            .finish()
    }
}

impl RelocationStore {
    pub fn new(lsm: Arc<Lsm>) -> Self {
        Self { lsm }
    }

    pub fn write_batch(
        &self,
        partition: u32,
        entries: &[RelocationEntry],
    ) -> Result<WriteBatchResult> {
        let mutations = entries
            .iter()
            .map(|entry| Mutation::PutPrefix {
                partition,
                key_prefix: entry.key.as_bytes().to_vec(),
                key_suffix: encode_suffix(entry.shard, entry.payload_lsn).to_vec(),
                value: encode_value(entry.publish_lsn, entry.to).to_vec(),
            })
            .collect();
        Ok(self.lsm.write_batch(mutations)?)
    }

    pub fn lookup(
        &self,
        partition: u32,
        key: &BlobKey,
        shard: ShardKey,
        payload_lsn: StrataLsn,
    ) -> Result<Option<Relocation>> {
        let mut encoded_key = Vec::with_capacity(key.len() + KEY_SUFFIX_BYTES);
        encoded_key.extend_from_slice(key.as_bytes());
        encoded_key.extend_from_slice(&encode_suffix(shard, payload_lsn));
        let Some(value) = self.lsm.get(partition, &encoded_key, &Replace)? else {
            return Ok(None);
        };
        let StoredValue::Inline(value) = decode_value(&value)? else {
            return Err(Error::Invalid(
                "relocation value is segment-backed".to_owned(),
            ));
        };
        decode_value_bytes(value).map(Some)
    }

    pub fn scan(
        &self,
        partition: u32,
        first_key: &[u8],
        last_key: &[u8],
        max_lsn: StrataLsn,
    ) -> Result<RelocationScan> {
        if first_key > last_key {
            return Err(Error::Invalid(
                "relocation scan range is reversed".to_owned(),
            ));
        }
        let mut end = last_key.to_vec();
        while end.last() == Some(&u8::MAX) {
            end.pop();
        }
        let end = if let Some(last) = end.last_mut() {
            *last += 1;
            Some(end)
        } else {
            None
        };
        RelocationScan::new(
            self.lsm
                .scan(partition, Some(first_key), end.as_deref(), max_lsn)?,
        )
    }

    pub fn lsm(&self) -> &Lsm {
        &self.lsm
    }
}

impl RelocationMerge {
    pub fn new(dead_segments: HashSet<SegmentId>) -> Self {
        Self {
            dead_segments,
            examined: AtomicU64::new(0),
            dropped: Mutex::new(Vec::new()),
        }
    }

    pub fn counts(&self) -> (u64, u64) {
        (
            self.examined.load(Ordering::Relaxed),
            self.dropped
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len() as u64,
        )
    }

    pub fn dropped_entries(&self) -> Vec<RelocationEntry> {
        self.dropped
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl MergeOperator for RelocationMerge {
    fn merge_base(&self) -> bool {
        true
    }

    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> strata_lsm::Result<()>,
    ) -> strata_lsm::Result<Option<Vec<u8>>> {
        self.examined.fetch_add(1, Ordering::Relaxed);
        let Some(value) = Replace.merge(key, base, patches, emit)? else {
            return Ok(None);
        };
        let StoredValue::Inline(bytes) = decode_value(&value)? else {
            return Err(strata_lsm::Error::Merge(
                "relocation value is segment-backed".to_owned(),
            ));
        };
        let relocation = decode_value_bytes(bytes)
            .map_err(|error| strata_lsm::Error::Merge(error.to_string()))?;
        if self.dead_segments.contains(&relocation.to.segment_id) {
            let (key, shard, payload_lsn) =
                decode_key(key).map_err(|error| strata_lsm::Error::Merge(error.to_string()))?;
            self.dropped
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RelocationEntry {
                    key,
                    shard,
                    payload_lsn,
                    publish_lsn: relocation.publish_lsn,
                    to: relocation.to,
                });
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }
}

impl RelocationScan {
    fn new(scan: LsmScan) -> Result<Self> {
        let mut scan = Self {
            scan,
            current: None,
        };
        scan.advance()?;
        Ok(scan)
    }

    pub fn current(&self) -> Option<&RelocationEntry> {
        self.current.as_ref()
    }

    pub fn advance(&mut self) -> Result<()> {
        self.current = match self.scan.next(&Replace)? {
            Some((key, value)) => {
                let (key, shard, payload_lsn) = decode_key(&key)?;
                let StoredValue::Inline(value) = decode_value(&value)? else {
                    return Err(Error::Invalid(
                        "relocation value is segment-backed".to_owned(),
                    ));
                };
                let relocation = decode_value_bytes(value)?;
                Some(RelocationEntry {
                    key,
                    shard,
                    payload_lsn,
                    publish_lsn: relocation.publish_lsn,
                    to: relocation.to,
                })
            }
            None => None,
        };
        Ok(())
    }
}

fn decode_key(key: &[u8]) -> Result<(BlobKey, ShardKey, StrataLsn)> {
    if key.len() <= KEY_SUFFIX_BYTES {
        return Err(Error::Invalid(
            "relocation key is missing its blob key".to_owned(),
        ));
    }
    let suffix = key.len() - KEY_SUFFIX_BYTES;
    Ok((
        BlobKey::new(key[..suffix].to_vec()).map_err(|error| Error::Invalid(error.to_string()))?,
        ShardKey {
            id: u32::from_be_bytes(
                key[suffix..suffix + 4]
                    .try_into()
                    .expect("checked suffix length"),
            ),
            generation: u64::from_be_bytes(
                key[suffix + 4..suffix + 12]
                    .try_into()
                    .expect("checked suffix length"),
            ),
        },
        u64::from_be_bytes(
            key[suffix + 12..]
                .try_into()
                .expect("checked suffix length"),
        ),
    ))
}

fn encode_suffix(shard: ShardKey, payload_lsn: StrataLsn) -> [u8; KEY_SUFFIX_BYTES] {
    let mut suffix = [0; KEY_SUFFIX_BYTES];
    suffix[..4].copy_from_slice(&shard.id.to_be_bytes());
    suffix[4..12].copy_from_slice(&shard.generation.to_be_bytes());
    suffix[12..].copy_from_slice(&payload_lsn.to_be_bytes());
    suffix
}

fn encode_value(publish_lsn: StrataLsn, to: RecordRef) -> [u8; VALUE_BYTES] {
    let mut value = [0; VALUE_BYTES];
    value[..8].copy_from_slice(&publish_lsn.to_le_bytes());
    value[8..16].copy_from_slice(&to.segment_id.to_le_bytes());
    value[16..24].copy_from_slice(&to.offset.to_le_bytes());
    value[24..].copy_from_slice(&to.len.to_le_bytes());
    value
}

fn decode_value_bytes(value: &[u8]) -> Result<Relocation> {
    if value.len() != VALUE_BYTES {
        return Err(Error::Invalid(format!(
            "encoded relocation has {} bytes instead of {VALUE_BYTES}",
            value.len()
        )));
    }
    Ok(Relocation {
        publish_lsn: u64::from_le_bytes(value[..8].try_into().expect("checked length")),
        to: RecordRef {
            segment_id: u64::from_le_bytes(value[8..16].try_into().expect("checked length")),
            offset: u64::from_le_bytes(value[16..24].try_into().expect("checked length")),
            len: u64::from_le_bytes(value[24..].try_into().expect("checked length")),
        },
    })
}

#[cfg(test)]
mod tests {
    use strata_lsm::encode_inline_value;

    use super::*;

    fn record(segment_id: SegmentId) -> RecordRef {
        RecordRef {
            segment_id,
            offset: 40,
            len: 80,
        }
    }

    #[test]
    fn relocation_compaction_drops_only_a_dead_current_destination() {
        let merge = RelocationMerge::new(HashSet::from([7]));
        let dead = encode_inline_value(&encode_value(11, record(7)));
        let live = encode_inline_value(&encode_value(12, record(8)));
        let shard = ShardKey {
            id: 2,
            generation: 3,
        };
        let mut key = b"key".to_vec();
        key.extend_from_slice(&encode_suffix(shard, 4));
        let mut base_key = b"base-key".to_vec();
        base_key.extend_from_slice(&encode_suffix(shard, 5));
        let mut emit = |_| Ok(());

        assert!(
            merge
                .merge(&key, None, &[(1, &dead)], &mut emit)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            merge
                .merge(&key, None, &[(1, &dead), (2, &live)], &mut emit)
                .unwrap(),
            Some(live)
        );
        assert!(
            merge
                .merge(&base_key, Some(&dead), &[], &mut emit)
                .unwrap()
                .is_none()
        );
        assert!(merge.merge_base());
        assert_eq!(merge.counts(), (3, 2));
        assert_eq!(
            merge.dropped_entries(),
            vec![
                RelocationEntry {
                    key: BlobKey::new(b"key".to_vec()).unwrap(),
                    shard,
                    payload_lsn: 4,
                    publish_lsn: 11,
                    to: record(7),
                },
                RelocationEntry {
                    key: BlobKey::new(b"base-key".to_vec()).unwrap(),
                    shard,
                    payload_lsn: 5,
                    publish_lsn: 11,
                    to: record(7),
                },
            ]
        );
    }
}
