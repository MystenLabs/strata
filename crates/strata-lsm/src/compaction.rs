use std::sync::Arc;

use crate::{
    CompactionReservation, Error, GarbageRecord, Manifest, ManifestEdit, MergeOperator, Result,
    TableMeta, TableReader, TableStore, TableWriter, table::TableCursor,
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

/// Selects and reserves a key-complete set of files for full compaction.
///
/// Selection closes transitively across both patch and base ranges. Every base row is therefore
/// compacted with every live patch SST that might contain the same key. Returns `None` when another
/// compaction already reserved any selected file.
pub fn select_compaction_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patches: &[TableMeta],
) -> Result<Option<CompactionInputs>> {
    select_inputs(manifest, tables, partition, patches, true)
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
    select_inputs(manifest, tables, partition, patches, false)
}

fn select_inputs(
    manifest: &Manifest,
    tables: &Arc<TableStore>,
    partition: u32,
    patches: &[TableMeta],
    include_base: bool,
) -> Result<Option<CompactionInputs>> {
    manifest.validate()?;
    if patches.is_empty() {
        return Err(Error::InvalidTable(
            "compaction requires at least one patch SST".to_owned(),
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

    let mut selected_base = Vec::<TableMeta>::new();

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
    // key.
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

    let mut reserved = selected_base.clone();
    reserved.extend(selected_patches.iter().cloned());
    let Some(reservation) = tables.reserve_for_compaction(&reserved)? else {
        return Ok(None);
    };

    let first_key = reserved
        .iter()
        .map(|table| &table.first_key)
        .min()
        .expect("patch input makes the reservation non-empty")
        .clone();
    let last_key = reserved
        .iter()
        .map(|table| &table.last_key)
        .max()
        .expect("patch input makes the reservation non-empty")
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
    let mut base = inputs
        .base
        .iter()
        .map(|table| TableReader::open_base(root, table, &inputs.schema_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;
    let mut patches = inputs
        .patches
        .iter()
        .map(|table| TableReader::open_patch(root, table, &inputs.patch_format_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;

    while let Some(key) = next_key(&base, &patches) {
        let mut base_value = None;
        let mut key_prefix_len = 0;
        for cursor in &mut base {
            while cursor
                .current()
                .is_some_and(|row| row.key.as_slice() == key)
            {
                if base_value.is_some() {
                    return Err(Error::InvalidManifest {
                        reason: "compaction found multiple base values for one key".to_owned(),
                    });
                }
                let row = cursor.current().expect("checked current row");
                if row.key_prefix_len != 0 {
                    if key_prefix_len != 0 && key_prefix_len != row.key_prefix_len {
                        return Err(Error::InvalidManifest {
                            reason: "compaction found conflicting prefixes for one key".to_owned(),
                        });
                    }
                    key_prefix_len = row.key_prefix_len;
                }
                base_value = Some(row.value.clone());
                cursor.advance()?;
            }
        }

        let mut key_patches = Vec::new();
        for cursor in &mut patches {
            while cursor
                .current()
                .is_some_and(|row| row.key.as_slice() == key)
            {
                let row = cursor.current().expect("checked current row");
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
                cursor.advance()?;
            }
        }

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
/// unique table ID and relative path each time a new output file is needed. Finished files are
/// synced, but this function does not publish the edit or delete input files.
pub fn write_compaction(
    inputs: &CompactionInputs,
    merge: &dyn MergeOperator,
    target_file_size: u64,
    mut next_output: impl FnMut() -> (u64, String),
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
    let mut writer = None;
    let mut outputs = Vec::new();
    let mut records = Vec::new();
    merge_compaction(
        inputs,
        merge,
        |key, key_prefix_len, value| {
            if writer.is_none() {
                let (id, path) = next_output();
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
    mut next_output: impl FnMut() -> (u64, String),
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
    let mut patches = inputs
        .patches
        .iter()
        .map(|table| TableReader::open_patch(root, table, &inputs.patch_format_id)?.into_cursor())
        .collect::<Result<Vec<_>>>()?;
    let mut writer = None;
    let mut outputs = Vec::new();
    let mut records = Vec::new();

    while let Some(key) = next_key(&[], &patches) {
        let mut key_patches = Vec::new();
        let mut key_prefix_len = 0;
        for cursor in &mut patches {
            while cursor
                .current()
                .is_some_and(|row| row.key.as_slice() == key)
            {
                let row = cursor.current().expect("checked current row");
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
                cursor.advance()?;
            }
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
            let (id, path) = next_output();
            writer = Some(TableWriter::create_patch(
                root,
                path,
                id,
                partition,
                &inputs.patch_format_id,
            )?);
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

fn next_key(base: &[TableCursor], patches: &[TableCursor]) -> Option<Vec<u8>> {
    base.iter()
        .chain(patches)
        .filter_map(TableCursor::current)
        .map(|row| row.key.as_slice())
        .min()
        .map(<[u8]>::to_vec)
}

fn overlaps(left: &TableMeta, right: &TableMeta) -> bool {
    left.first_key <= right.last_key && right.first_key <= left.last_key
}
