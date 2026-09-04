use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use core_types::{BlobKey, RecordRef, SegmentId, ShardKey, StrataLsn};
#[cfg(test)]
use lsm::WriteBatchResult;
use lsm::{
    GarbageRecord, Lsm, LsmIter, ManifestEdit, MergeOperator, Mutation, Replace, StoredValue,
    TableMeta, TableWriter, decode_value, encode_inline_value,
};

use crate::{Error, Result, partition::partition_for_key};

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
    iter: LsmIter,
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

    #[cfg(test)]
    pub fn write_batch(
        &self,
        partition: u32,
        entries: &[RelocationEntry],
    ) -> Result<WriteBatchResult> {
        let mutations = entries
            .iter()
            .map(|entry| {
                (
                    entry.publish_lsn,
                    Self::lsm_mutation_in_partition(partition, entry),
                )
            })
            .collect();
        Ok(self.lsm.write_batch(mutations)?)
    }

    /// Writes one GC relocation batch directly as fully synced immutable patch tables.
    ///
    /// The returned edit is not visible until the caller atomically publishes it with the output
    /// and source segment states. Until then the table is an orphan and recovery may remove it.
    pub fn prepare_l0(&self, entries: &[RelocationEntry]) -> Result<(StrataLsn, ManifestEdit)> {
        if entries.is_empty() {
            return Err(Error::InvalidRelocation(
                "cannot prepare an empty relocation L0".to_owned(),
            ));
        }
        let manifest = self.lsm.manifest();
        let sequence = self
            .lsm
            .last_lsn()?
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidRelocation("relocation sequence overflow".to_owned()))?;
        let mut partitioned = BTreeMap::<u32, Vec<&RelocationEntry>>::new();
        for entry in entries {
            let partition = partition_for_key(entry.key.as_bytes(), manifest.partition_count);
            partitioned.entry(partition).or_default().push(entry);
        }
        for sorted in partitioned.values_mut() {
            sorted.sort_unstable_by(|left, right| {
                left.key.as_bytes().cmp(right.key.as_bytes()).then_with(|| {
                    encode_suffix(left.shard, left.payload_lsn)
                        .cmp(&encode_suffix(right.shard, right.payload_lsn))
                })
            });
            for pair in sorted.windows(2) {
                if pair[0].key == pair[1].key
                    && pair[0].shard == pair[1].shard
                    && pair[0].payload_lsn == pair[1].payload_lsn
                {
                    return Err(Error::InvalidRelocation(
                        "one GC publish contains duplicate relocation identities".to_owned(),
                    ));
                }
            }
        }
        let table_store = self.lsm.table_store();
        let table_root = table_store.root();
        let mut tables = Vec::with_capacity(partitioned.len());
        for (partition, sorted) in partitioned {
            let target = match self.lsm.allocate_patch_target() {
                Ok(target) => target,
                Err(error) => {
                    remove_prepared_tables(table_root, &tables);
                    return Err(error.into());
                }
            };
            let (table_id, relative_path) = match target.into_patch_parts() {
                Ok(parts) => parts,
                Err(error) => {
                    remove_prepared_tables(table_root, &tables);
                    return Err(error.into());
                }
            };
            let tmp_path = table_root.join(format!("{relative_path}.tmp"));
            let table = match (|| -> lsm::Result<TableMeta> {
                let mut writer = TableWriter::create_patch(
                    table_root,
                    &relative_path,
                    table_id,
                    partition,
                    &manifest.patch_format_id,
                )?;
                for entry in sorted {
                    writer.add_prefix_patch(
                        entry.key.as_bytes(),
                        &encode_suffix(entry.shard, entry.payload_lsn),
                        sequence,
                        &encode_inline_value(&encode_value(entry.publish_lsn, entry.to)),
                    )?;
                }
                writer.finish()
            })() {
                Ok(table) => table,
                Err(error) => {
                    let _ = std::fs::remove_file(tmp_path);
                    let _ = std::fs::remove_file(table_root.join(&relative_path));
                    remove_prepared_tables(table_root, &tables);
                    return Err(error.into());
                }
            };
            tables.push(table);
        }
        Ok((
            sequence,
            ManifestEdit {
                remove: Vec::new(),
                add_base: Vec::new(),
                add_patches: tables,
                materialized_through: Some(sequence),
                wal_retained_from: None,
            },
        ))
    }

    pub fn manifest_sequence(&self) -> StrataLsn {
        manifest_sequence(&self.lsm.manifest())
    }

    /// Encodes the keyed projection stored in the relocation LSM. Legacy shared-WAL recovery uses
    /// the same encoding as immutable relocation tables.
    pub fn lsm_mutation(partition_count: u32, entry: &RelocationEntry) -> Mutation {
        Self::lsm_mutation_in_partition(
            partition_for_key(entry.key.as_bytes(), partition_count),
            entry,
        )
    }

    fn lsm_mutation_in_partition(partition: u32, entry: &RelocationEntry) -> Mutation {
        Mutation::PutPrefix {
            partition,
            key_prefix: entry.key.as_bytes().to_vec(),
            key_suffix: encode_suffix(entry.shard, entry.payload_lsn).to_vec(),
            value: encode_value(entry.publish_lsn, entry.to).to_vec(),
        }
    }

    pub fn lookup(
        &self,
        key: &BlobKey,
        shard: ShardKey,
        payload_lsn: StrataLsn,
    ) -> Result<Option<Relocation>> {
        let partition = partition_for_key(key.as_bytes(), self.lsm.manifest().partition_count);
        let mut encoded_key = Vec::with_capacity(key.len() + KEY_SUFFIX_BYTES);
        encoded_key.extend_from_slice(key.as_bytes());
        encoded_key.extend_from_slice(&encode_suffix(shard, payload_lsn));
        let Some(value) = self.lsm.get(partition, &encoded_key, &Replace)? else {
            return Ok(None);
        };
        let StoredValue::Inline(value) = decode_value(&value)? else {
            return Err(Error::InvalidRelocation(
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
            return Err(Error::InvalidRelocation(
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
                .iter(partition, Some(first_key), end.as_deref(), max_lsn)?,
        )
    }

    pub fn lsm(&self) -> &Lsm {
        &self.lsm
    }
}

fn remove_prepared_tables(root: &std::path::Path, tables: &[TableMeta]) {
    for table in tables {
        let _ = std::fs::remove_file(root.join(&table.relative_path));
    }
}

fn manifest_sequence(manifest: &lsm::Manifest) -> StrataLsn {
    manifest
        .partitions
        .values()
        .flat_map(|partition| partition.base.iter().chain(&partition.patches))
        .filter_map(|table: &TableMeta| table.max_lsn)
        .chain(manifest.materialized_through)
        .max()
        .unwrap_or_default()
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
        emit: &mut dyn FnMut(GarbageRecord) -> lsm::Result<()>,
    ) -> lsm::Result<Option<Vec<u8>>> {
        self.examined.fetch_add(1, Ordering::Relaxed);
        let Some(value) = Replace.merge(key, base, patches, emit)? else {
            return Ok(None);
        };
        let StoredValue::Inline(bytes) = decode_value(&value)? else {
            return Err(lsm::Error::Merge(
                "relocation value is segment-backed".to_owned(),
            ));
        };
        let relocation =
            decode_value_bytes(bytes).map_err(|error| lsm::Error::Merge(error.to_string()))?;
        if self.dead_segments.contains(&relocation.to.segment_id) {
            let (key, shard, payload_lsn) =
                decode_key(key).map_err(|error| lsm::Error::Merge(error.to_string()))?;
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

    /// Patch-tier merge: one identity relocated several times keeps only its latest destination.
    ///
    /// Dead destinations are deliberately not dropped here. Removing a patch entry without seeing
    /// the base could resurface an older destination for the same identity; only the full pass,
    /// which sees the whole history, retires entries.
    fn partial_merge(
        &self,
        key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> lsm::Result<()>,
    ) -> lsm::Result<Option<Vec<u8>>> {
        self.examined.fetch_add(1, Ordering::Relaxed);
        Replace.partial_merge(key, patches, emit)
    }
}

impl RelocationScan {
    fn new(iter: LsmIter) -> Result<Self> {
        let mut scan = Self {
            iter,
            current: None,
        };
        scan.advance()?;
        Ok(scan)
    }

    pub fn current(&self) -> Option<&RelocationEntry> {
        self.current.as_ref()
    }

    pub fn advance(&mut self) -> Result<()> {
        self.current = match self.iter.next(&Replace)? {
            Some((key, value)) => {
                let (key, shard, payload_lsn) = decode_key(&key)?;
                let StoredValue::Inline(value) = decode_value(&value)? else {
                    return Err(Error::InvalidRelocation(
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
        return Err(Error::InvalidRelocation(
            "relocation key is missing its blob key".to_owned(),
        ));
    }
    let suffix = key.len() - KEY_SUFFIX_BYTES;
    Ok((
        BlobKey::new(key[..suffix].to_vec())
            .map_err(|error| Error::InvalidRelocation(error.to_string()))?,
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
        return Err(Error::InvalidRelocation(format!(
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
    use lsm::encode_inline_value;

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
