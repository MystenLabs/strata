use std::{num::NonZeroU32, path::Path, sync::Arc};

use lsm::{
    GarbageRecord, Manifest, ManifestEdit, MergeOperator, OperandFloor, Replace, Result, Snapshot,
    StrataLsn, TableMeta, TableStore, TableTarget, TableWriter, encode_inline_value,
    select_patch_compaction_inputs, select_patch_group_inputs, write_patch_compaction,
};
use tempfile::TempDir;

struct Append;

impl MergeOperator for Append {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        _emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut value = base.unwrap_or_default().to_vec();
        for (_, patch) in patches {
            value.extend_from_slice(patch);
        }
        Ok(Some(value))
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        _emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(Some(
            patches
                .iter()
                .flat_map(|(_, patch)| *patch)
                .copied()
                .collect(),
        ))
    }
}

#[test]
fn partial_compaction_replaces_only_patch_files() {
    let directory = TempDir::new().unwrap();
    let base = write_base(directory.path());
    let early = write_patch(directory.path(), "early.sst", 2, 5, b"-5");
    let late = write_patch(directory.path(), "late.sst", 3, 10, b"-10");
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![base.clone()],
            add_patches: vec![early.clone(), late],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let tables = Arc::new(TableStore::new(directory.path()));
    let inputs = select_patch_compaction_inputs(&manifest, &tables, 0, &[early])
        .unwrap()
        .unwrap();

    let (edit, garbage) =
        write_patch_compaction(&inputs, &Append, u64::MAX, || Ok(TableTarget::patch(4))).unwrap();

    assert!(garbage.is_empty());
    assert_eq!(edit.add_base, Vec::new());
    assert_eq!(edit.add_patches.len(), 1);
    assert_eq!(edit.remove, ["early.sst", "late.sst"]);
    manifest.apply(&edit).unwrap();
    assert_eq!(manifest.partitions[&0].base, [base]);

    let snapshot = Snapshot::new(tables, Arc::new(manifest), 10).unwrap();
    assert_eq!(
        snapshot.get(0, b"a", &Append).unwrap(),
        Some(b"A-5-10".to_vec())
    );
}

fn write_base(root: &Path) -> TableMeta {
    let mut writer = TableWriter::create_base(root, "base.sst", 1, 0, "base-v1").unwrap();
    writer.add(b"a", b"A").unwrap();
    writer.finish().unwrap()
}

fn write_patch(root: &Path, path: &str, id: u64, sequence: u64, value: &[u8]) -> TableMeta {
    let mut writer = TableWriter::create_patch(root, path, id, 0, "patch-v1").unwrap();
    writer.add_patch(b"a", sequence, value).unwrap();
    writer.finish().unwrap()
}

/// The merged patch inherits the lowest floor of its inputs, and an unclassified input makes the
/// output unclassified too.
#[test]
fn patch_compaction_inherits_the_operand_floor() {
    let directory = TempDir::new().unwrap();
    let mut early = write_patch(directory.path(), "early.sst", 2, 5, b"-5");
    early.global_operand_floor = OperandFloor::None;
    let mut late = write_patch(directory.path(), "late.sst", 3, 10, b"-10");
    late.global_operand_floor = OperandFloor::At(10);
    let unclassified = write_patch(directory.path(), "unclassified.sst", 4, 12, b"-12");
    assert_eq!(unclassified.global_operand_floor, OperandFloor::Unknown);
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: vec![early.clone(), late.clone(), unclassified.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let tables = Arc::new(TableStore::new(directory.path()));

    let inputs = select_patch_group_inputs(&manifest, &tables, 0, &[early, late])
        .unwrap()
        .unwrap();
    let (edit, _) =
        write_patch_compaction(&inputs, &Append, u64::MAX, || Ok(TableTarget::patch(5))).unwrap();
    assert_eq!(
        edit.add_patches[0].global_operand_floor,
        OperandFloor::At(10)
    );
    manifest.apply(&edit).unwrap();

    let merged = edit.add_patches[0].clone();
    let inputs = select_patch_group_inputs(&manifest, &tables, 0, &[merged, unclassified])
        .unwrap()
        .unwrap();
    let (edit, _) =
        write_patch_compaction(&inputs, &Append, u64::MAX, || Ok(TableTarget::patch(6))).unwrap();
    assert_eq!(
        edit.add_patches[0].global_operand_floor,
        OperandFloor::Unknown
    );
}

/// A patch group merges exactly the patches it names. The unselected patch in the middle keeps
/// its own lsn, so it still orders correctly against the merged output under last-write-wins.
#[test]
fn patch_group_compaction_merges_only_the_selected_patches() {
    let directory = TempDir::new().unwrap();
    let early = write_inline_patch(directory.path(), "early.sst", 2, 5, b"-5");
    let mid = write_inline_patch(directory.path(), "mid.sst", 3, 7, b"-7");
    let late = write_inline_patch(directory.path(), "late.sst", 4, 10, b"-10");
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: vec![early.clone(), mid.clone(), late.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let tables = Arc::new(TableStore::new(directory.path()));

    let inputs = select_patch_group_inputs(&manifest, &tables, 0, &[early.clone(), late.clone()])
        .unwrap()
        .unwrap();
    assert_eq!(inputs.patches, [early, late]);

    let (edit, garbage) =
        write_patch_compaction(&inputs, &Replace, u64::MAX, || Ok(TableTarget::patch(5))).unwrap();
    assert!(garbage.is_empty());
    assert!(edit.add_base.is_empty());
    assert_eq!(edit.add_patches.len(), 1);
    assert_eq!(edit.remove, ["early.sst", "late.sst"]);
    manifest.apply(&edit).unwrap();
    let patches = &manifest.partitions[&0].patches;
    assert_eq!(patches.len(), 2);
    assert!(patches.contains(&mid));

    let snapshot = Snapshot::new(tables, Arc::new(manifest), 10).unwrap();
    assert_eq!(
        snapshot.get(0, b"a", &Replace).unwrap(),
        Some(encode_inline_value(b"-10"))
    );
}

fn write_inline_patch(root: &Path, path: &str, id: u64, sequence: u64, value: &[u8]) -> TableMeta {
    let mut writer = TableWriter::create_patch(root, path, id, 0, "patch-v1").unwrap();
    writer
        .add_patch(b"a", sequence, &encode_inline_value(value))
        .unwrap();
    writer.finish().unwrap()
}
