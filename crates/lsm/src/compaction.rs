use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use crate::{
    CompactionReservation, Error, GarbageRecord, Manifest, ManifestEdit, MergeOperator,
    OperandFloor, Result, TableMeta, TableReader, TableStore, TableTarget, TableWriter,
    table::{TableCursor, TableRow},
};

/// Files reserved for one compaction and their complete key bounds.
pub struct CompactionInputs {
    pub base: Vec<TableMeta>,
    pub patches: Vec<TableMeta>,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    schema_id: String,
    patch_format_id: String,
    reservation: CompactionReservation,
}

/// Selects and reserves one key-complete overlap component for full compaction.
///
/// Selection starts from one patch and closes transitively across both patch and base ranges.
/// Every base row is therefore compacted with every live patch SST that might contain the same
/// key, without bridging disconnected components. Returns `None` when another compaction already
/// reserved any selected file.
pub fn select_compaction_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patch: &TableMeta,
) -> Result<Option<CompactionInputs>> {
    select_inputs(
        manifest,
        tables,
        partition,
        std::slice::from_ref(patch),
        None,
        true,
        true,
    )
}

/// Selects one base seed and the complete transitive set of files that can affect its keys.
///
/// Ordinary full compaction starts from a patch. A global-state sweep sometimes needs the inverse:
/// revisit a cold base even though no foreground write touched it. For example, a base containing a
/// key whose lifetime ends at epoch 50 must still see the epoch-50 transition when it has no patch.
/// Starting from that base pulls in every overlapping patch, and the same closure loop then pulls
/// in any further bases and patches needed to keep the selected key set complete.
pub fn select_base_compaction_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    base: &TableMeta,
) -> Result<Option<CompactionInputs>> {
    select_inputs(manifest, tables, partition, &[], Some(base), true, true)
}

/// Selects and reserves exactly one base SST for a base-only sweep, reading no patches.
///
/// Rewriting a base by itself is only sound when the merge applies global transitions no later
/// than the LSN below which every live patch of the partition starts (see
/// [`Manifest::writes_merged_through_lsn`]): any operand that could change how a transition
/// applies to one of these keys is then already in the base. The caller bounds its merge snapshot
/// accordingly and stamps the output's `merge_applied_through_lsn` with that bound. The output
/// covers exactly the input's key range, so it cannot overlap a neighbouring base.
pub fn select_base_sweep_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    base: &TableMeta,
) -> Result<Option<CompactionInputs>> {
    select_inputs(manifest, tables, partition, &[], Some(base), true, false)
}

/// Selects and reserves an overlap-closed patch set for patch-only compaction.
///
/// Base SSTs are neither read nor reserved because [`write_patch_compaction`] never rewrites them.
pub fn select_patch_compaction_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patches: &[TableMeta],
) -> Result<Option<CompactionInputs>> {
    select_inputs(manifest, tables, partition, patches, None, false, true)
}

/// Selects and reserves exactly the given patch SSTs for patch-only compaction, without closing
/// over other patches that overlap them.
///
/// This is only sound for operators whose partial merge of any subsequence of one key's operands
/// is equivalent to merging the whole ordered sequence, such as last-write-wins: every row keeps
/// its own lsn, so an unselected overlapping patch stays correctly ordered against the merged
/// output. It lets a caller merge patches by size tier instead of by overlap component, which is
/// what bounds the write amplification of a patch tier.
pub fn select_patch_group_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patches: &[TableMeta],
) -> Result<Option<CompactionInputs>> {
    select_inputs(manifest, tables, partition, patches, None, false, false)
}

fn select_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patches: &[TableMeta],
    base: Option<&TableMeta>,
    include_base: bool,
    close_over_overlaps: bool,
) -> Result<Option<CompactionInputs>> {
    manifest.validate()?;
    if patches.is_empty() && base.is_none() {
        return Err(Error::InvalidTable(
            "compaction requires at least one base or patch SST".to_owned(),
        ));
    }
    if !include_base && base.is_some() {
        return Err(Error::InvalidTable(
            "patch-only compaction cannot start from a base SST".to_owned(),
        ));
    }
    let partition_manifest =
        manifest
            .partitions
            .get(&partition)
            .ok_or(Error::InvalidPartition {
                partition,
                partition_count: manifest.partition_count,
            })?;

    let mut selected_patches = Vec::with_capacity(patches.len());
    for patch in patches {
        let live = partition_manifest
            .patches
            .iter()
            .find(|live| live.relative_path == patch.relative_path);
        if live != Some(patch) {
            return Err(Error::InvalidTable(format!(
                "patch SST {} is not live in partition {partition}",
                patch.relative_path
            )));
        }
        if selected_patches
            .iter()
            .any(|selected: &TableMeta| selected.relative_path == patch.relative_path)
        {
            return Err(Error::InvalidTable(format!(
                "patch SST {} was selected more than once",
                patch.relative_path
            )));
        }
        selected_patches.push(patch.clone());
    }

    let mut selected_base = Vec::with_capacity(usize::from(base.is_some()));
    if let Some(table) = base {
        let live = partition_manifest
            .base
            .iter()
            .find(|live| live.relative_path == table.relative_path);
        if live != Some(table) {
            return Err(Error::InvalidTable(format!(
                "base SST {} is not live in partition {partition}",
                table.relative_path
            )));
        }
        selected_base.push(table.clone());
    }

    // Full compaction must close over the connected component spanning both levels, not merely
    // close patches before selecting base files. For example:
    //
    //   selected patch P1: [a, m]
    //   overlapping base B: [a, z]
    //   otherwise separate patch P2: [x, x]
    //
    // P1 pulls in B. Rewriting B also passes its base-only key `x` through `merge_base`. If P2 were
    // left behind, that merge could apply a global transition (such as expiring lifetime 10 at
    // epoch 10) without seeing P2's pre-expiry `SetLifetime(20)`. The rewritten base would have
    // already discarded the payload by the time P2 was read. Adding P2 can widen the base set
    // again, so patch and base additions repeat together until neither level grows.
    //
    // Patch-only compaction uses the same loop with an empty base side. Range overlap remains
    // conservative, but avoids inspecting unselected files to prove they do not contain a common
    // key. A patch group selection skips the loop entirely; see select_patch_group_inputs.
    if !close_over_overlaps {
        return reserve_inputs(manifest, tables, selected_base, selected_patches);
    }
    loop {
        let patch_additions = partition_manifest
            .patches
            .iter()
            .filter(|candidate| {
                !selected_patches
                    .iter()
                    .any(|selected| selected.relative_path == candidate.relative_path)
                    && (selected_patches
                        .iter()
                        .any(|selected| overlaps(candidate, selected))
                        || selected_base
                            .iter()
                            .any(|selected| overlaps(candidate, selected)))
            })
            .cloned()
            .collect::<Vec<_>>();
        let base_additions = if include_base {
            partition_manifest
                .base
                .iter()
                .filter(|candidate| {
                    !selected_base
                        .iter()
                        .any(|selected| selected.relative_path == candidate.relative_path)
                        && selected_patches
                            .iter()
                            .any(|selected| overlaps(candidate, selected))
                })
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if patch_additions.is_empty() && base_additions.is_empty() {
            break;
        }
        selected_patches.extend(patch_additions);
        selected_base.extend(base_additions);
    }

    reserve_inputs(manifest, tables, selected_base, selected_patches)
}

/// Reserves the selected files and computes their complete key bounds.
fn reserve_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    selected_base: Vec<TableMeta>,
    selected_patches: Vec<TableMeta>,
) -> Result<Option<CompactionInputs>> {
    let mut reserved = selected_base.clone();
    reserved.extend(selected_patches.iter().cloned());
    let Some(reservation) = tables.reserve_for_compaction(&reserved)? else {
        return Ok(None);
    };

    let first_key = reserved
        .iter()
        .map(|table| &table.first_key)
        .min()
        .expect("a base or patch seed makes the reservation non-empty")
        .clone();
    let last_key = reserved
        .iter()
        .map(|table| &table.last_key)
        .max()
        .expect("a base or patch seed makes the reservation non-empty")
        .clone();

    Ok(Some(CompactionInputs {
        base: selected_base,
        patches: selected_patches,
        first_key,
        last_key,
        schema_id: manifest.schema_id.clone(),
        patch_format_id: manifest.patch_format_id.clone(),
        reservation,
    }))
}

/// Streams selected inputs in key order through the merge operator.
///
/// Unchanged base rows are copied directly. `write` receives materialized base rows in key order;
/// `emit` receives garbage records produced by the merge operator.
pub fn merge_compaction(
    inputs: &CompactionInputs,
    merge: &dyn MergeOperator,
    mut write: impl FnMut(&[u8], usize, &[u8]) -> Result<()>,
    mut emit: impl FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    let root = inputs.reservation.root();
    let base = inputs
        .base
        .iter()
        .map(|table| TableReader::open_base(root, table, &inputs.schema_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;
    let patches = inputs
        .patches
        .iter()
        .map(|table| TableReader::open_patch(root, table, &inputs.patch_format_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;

    let mut rows = CompactionRows::new(base.into_iter().chain(patches).collect());
    while let Some(key) = rows.next_key().map(<[u8]>::to_vec) {
        let mut base_value = None;
        let mut key_prefix_len = 0;
        let mut key_patches = Vec::new();
        rows.consume_key(&key, |row| {
            if row.key_prefix_len != 0 {
                if key_prefix_len != 0 && key_prefix_len != row.key_prefix_len {
                    return Err(Error::InvalidManifest {
                        reason: "compaction found conflicting prefixes for one key".to_owned(),
                    });
                }
                key_prefix_len = row.key_prefix_len;
            }
            if let Some(lsn) = row.lsn {
                key_patches.push((lsn, row.value.clone()));
            } else if base_value.replace(row.value.clone()).is_some() {
                return Err(Error::InvalidManifest {
                    reason: "compaction found multiple base values for one key".to_owned(),
                });
            }
            Ok(())
        })?;

        if key_patches.is_empty() {
            if merge.merge_base() {
                if let Some(value) = merge.merge(&key, base_value.as_deref(), &[], &mut emit)? {
                    write(&key, key_prefix_len, &value)?;
                }
            } else if let Some(value) = base_value {
                write(&key, key_prefix_len, &value)?;
            }
            continue;
        }

        key_patches.sort_unstable_by_key(|(lsn, _)| *lsn);
        if let Some(lsn) = key_patches
            .windows(2)
            .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
        {
            return Err(Error::InvalidManifest {
                reason: format!("compaction found duplicate patch lsn {lsn:?} for one key"),
            });
        }
        let patch_refs: Vec<_> = key_patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect();
        if let Some(value) = merge.merge(&key, base_value.as_deref(), &patch_refs, &mut emit)? {
            write(&key, key_prefix_len, &value)?;
        }
    }
    Ok(())
}

/// Writes a prepared compaction and returns its manifest edit and derived garbage records.
///
/// `target_file_size` is a soft limit because one key is never split. `next_output` must return a
/// a fresh allocator-owned target each time a new output file is needed. Finished files are
/// synced, but this function does not publish the edit or delete input files.
pub fn write_compaction(
    inputs: &CompactionInputs,
    merge: &dyn MergeOperator,
    target_file_size: u64,
    mut next_output: impl FnMut() -> Result<TableTarget>,
) -> Result<(ManifestEdit, Vec<GarbageRecord>)> {
    if target_file_size == 0 {
        return Err(Error::InvalidTable(
            "compaction target file size must be non-zero".to_owned(),
        ));
    }

    let root = inputs.reservation.root();
    let partition = inputs
        .patches
        .first()
        .or_else(|| inputs.base.first())
        .expect("compaction inputs always contain a base or patch")
        .partition;
    let mut writer = None;
    let mut outputs = Vec::new();
    let mut records = Vec::new();
    merge_compaction(
        inputs,
        merge,
        |key, key_prefix_len, value| {
            if writer.is_none() {
                let (id, path) = next_output()?.into_base_parts()?;
                writer = Some(TableWriter::create_base(
                    root,
                    path,
                    id,
                    partition,
                    &inputs.schema_id,
                )?);
            }
            let current = writer.as_mut().expect("writer was opened above");
            if key_prefix_len == 0 {
                current.add(key, value)?;
            } else {
                current.add_prefix(&key[..key_prefix_len], &key[key_prefix_len..], value)?;
            }
            if current.estimated_len() >= target_file_size {
                outputs.push(writer.take().expect("writer is present").finish()?);
            }
            Ok(())
        },
        |record| {
            records.push(record);
            Ok(())
        },
    )?;
    if let Some(writer) = writer {
        outputs.push(writer.finish()?);
    }

    let edit = ManifestEdit {
        remove: inputs
            .base
            .iter()
            .chain(&inputs.patches)
            .map(|table| table.relative_path.clone())
            .collect(),
        add_base: outputs,
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    };
    edit.validate()?;
    records.sort_unstable_by(GarbageRecord::cmp_position);
    Ok((edit, records))
}

/// Rewrites patch SSTs without materializing them into a base table.
///
/// A partial merge may replace one key's operands with a single operand at their latest lsn.
/// Declined merges are copied unchanged.
pub fn write_patch_compaction(
    inputs: &CompactionInputs,
    merge: &dyn MergeOperator,
    target_file_size: u64,
    mut next_output: impl FnMut() -> Result<TableTarget>,
) -> Result<(ManifestEdit, Vec<GarbageRecord>)> {
    if target_file_size == 0 {
        return Err(Error::InvalidTable(
            "compaction target file size must be non-zero".to_owned(),
        ));
    }

    let root = inputs.reservation.root();
    let partition = inputs
        .patches
        .first()
        .expect("compaction inputs always contain a patch")
        .partition;
    let patches = inputs
        .patches
        .iter()
        .map(|table| TableReader::open_patch(root, table, &inputs.patch_format_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;
    // A partial merge keeps every operand's own LSN (a batch keeps the inner ones), so the merged
    // output cannot hold a global-state operand older than the lowest input floor. Inheriting the
    // inputs' floors is exact for untouched rows and conservative for reduced ones.
    let operand_floor = inputs
        .patches
        .iter()
        .map(|table| table.global_operand_floor)
        .reduce(OperandFloor::merge)
        .unwrap_or_default();
    let mut writer = None;
    let mut outputs = Vec::new();
    let mut records = Vec::new();

    let mut rows = CompactionRows::new(patches);
    while let Some(key) = rows.next_key().map(<[u8]>::to_vec) {
        let mut key_patches = Vec::new();
        let mut key_prefix_len = 0;
        rows.consume_key(&key, |row| {
            if row.key_prefix_len != 0 {
                if key_prefix_len != 0 && key_prefix_len != row.key_prefix_len {
                    return Err(Error::InvalidManifest {
                        reason: "compaction found conflicting prefixes for one key".to_owned(),
                    });
                }
                key_prefix_len = row.key_prefix_len;
            }
            key_patches.push((
                row.lsn.expect("patch cursor has lsned rows"),
                row.value.clone(),
            ));
            Ok(())
        })?;
        key_patches.sort_unstable_by_key(|(lsn, _)| *lsn);
        if let Some(lsn) = key_patches
            .windows(2)
            .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
        {
            return Err(Error::InvalidManifest {
                reason: format!("compaction found duplicate patch lsn {lsn:?} for one key"),
            });
        }

        let patch_refs = key_patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut merged_records = Vec::new();
        let merged = merge.partial_merge(&key, &patch_refs, &mut |record| {
            merged_records.push(record);
            Ok(())
        })?;

        if writer.is_none() {
            let (id, path) = next_output()?.into_patch_parts()?;
            let mut opened =
                TableWriter::create_patch(root, path, id, partition, &inputs.patch_format_id)?;
            opened.set_operand_floor(operand_floor)?;
            writer = Some(opened);
        }
        let current = writer.as_mut().expect("writer was opened above");
        match merged {
            Some(value) => {
                let lsn = key_patches.last().expect("key has patches").0;
                if key_prefix_len == 0 {
                    current.add_patch(&key, lsn, &value)?;
                } else {
                    current.add_prefix_patch(
                        &key[..key_prefix_len],
                        &key[key_prefix_len..],
                        lsn,
                        &value,
                    )?;
                }
                records.extend(merged_records);
            }
            None => {
                for (lsn, value) in key_patches {
                    if key_prefix_len == 0 {
                        current.add_patch(&key, lsn, &value)?;
                    } else {
                        current.add_prefix_patch(
                            &key[..key_prefix_len],
                            &key[key_prefix_len..],
                            lsn,
                            &value,
                        )?;
                    }
                }
            }
        }
        if current.estimated_len() >= target_file_size {
            outputs.push(writer.take().expect("writer is present").finish()?);
        }
    }
    if let Some(writer) = writer {
        outputs.push(writer.finish()?);
    }

    let edit = ManifestEdit {
        remove: inputs
            .patches
            .iter()
            .map(|table| table.relative_path.clone())
            .collect(),
        add_base: Vec::new(),
        add_patches: outputs,
        materialized_through: None,
        wal_retained_from: None,
    };
    edit.validate()?;
    records.sort_unstable_by(GarbageRecord::cmp_position);
    Ok((edit, records))
}

/// One heap entry per nonempty SST. Selecting a key and finding its matching inputs both
/// use the heap, rather than scanning every cursor for every key.
struct CompactionRows {
    cursors: Vec<TableCursor>,
    heads: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl CompactionRows {
    fn new(cursors: Vec<TableCursor>) -> Self {
        let heads = cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| {
                cursor
                    .current()
                    .map(|row| Reverse((row.key.clone(), index)))
            })
            .collect();
        Self { cursors, heads }
    }

    fn next_key(&self) -> Option<&[u8]> {
        self.heads.peek().map(|Reverse((key, _))| key.as_slice())
    }

    fn consume_key(
        &mut self,
        key: &[u8],
        mut visit: impl FnMut(&TableRow) -> Result<()>,
    ) -> Result<()> {
        while self.next_key() == Some(key) {
            let Reverse((_, index)) = self.heads.pop().expect("heap has this key");
            let cursor = &mut self.cursors[index];
            // Consume all versions of this key before reinserting the cursor. Ties are broken
            // by input index; the caller still orders the collected patch operands by LSN.
            while let Some(row) = cursor.current().filter(|row| row.key == key) {
                visit(row)?;
                cursor.advance()?;
            }
            if let Some(row) = cursor.current() {
                self.heads.push(Reverse((row.key.clone(), index)));
            }
        }
        Ok(())
    }
}

fn overlaps(left: &TableMeta, right: &TableMeta) -> bool {
    left.first_key <= right.last_key && right.first_key <= left.last_key
}
