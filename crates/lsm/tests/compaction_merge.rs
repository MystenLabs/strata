use std::{num::NonZeroU32, path::Path, sync::Arc};

use core_types::{BlobKey, RecordRef, SegmentGcSummaryDelta};
use lsm::{
    Error, GarbageEvent, GarbageRecord, Manifest, ManifestEdit, MergeOperator, Result, SegmentKey,
    StrataLsn, TableMeta, TableStore, TableWriter, merge_compaction, select_compaction_inputs,
};
use tempfile::TempDir;

struct Apply;

impl MergeOperator for Apply {
    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut value = base.map(<[u8]>::to_vec);
        for (lsn, patch) in patches {
            emit(garbage_record(key, *lsn, patch))?;
            if *patch == b"delete" {
                value = None;
            } else {
                value.get_or_insert_with(Vec::new).extend_from_slice(patch);
            }
        }
        Ok(value)
    }
}

#[test]
fn streams_keys_and_patches_in_logical_order() {
    let directory = TempDir::new().unwrap();
    let base = write_base(directory.path(), 1, &[("a", "A"), ("b", "B"), ("c", "C")]);
    let late = write_patch(
        directory.path(),
        "late.sst",
        2,
        &[("a", 10, "-10"), ("b", 12, "delete")],
    );
    let early = write_patch(
        directory.path(),
        "early.sst",
        3,
        &[("a", 5, "-5"), ("d", 7, "D")],
    );
    let unrelated = write_patch(
        directory.path(),
        "unrelated.sst",
        4,
        &[("z", 20, "ignored")],
    );
    let manifest = manifest(vec![base], vec![late.clone(), early, unrelated.clone()]);
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &late)
        .unwrap()
        .unwrap();

    assert_eq!(inputs.patches.len(), 2);
    assert!(!files.is_pinned(&unrelated));

    let mut rows = Vec::new();
    let mut records = Vec::new();
    merge_compaction(
        &inputs,
        &Apply,
        |key, _, value| {
            rows.push((key.to_vec(), value.to_vec()));
            Ok(())
        },
        |record| {
            records.push(record);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(
        rows,
        [
            (b"a".to_vec(), b"A-5-10".to_vec()),
            (b"c".to_vec(), b"C".to_vec()),
            (b"d".to_vec(), b"D".to_vec()),
        ]
    );
    assert_eq!(
        records,
        [
            garbage_record(b"a", 5, b"-5"),
            garbage_record(b"a", 10, b"-10"),
            garbage_record(b"b", 12, b"delete"),
            garbage_record(b"d", 7, b"D"),
        ]
    );
}

#[test]
fn rejects_duplicate_patch_lsns_across_files() {
    let directory = TempDir::new().unwrap();
    let first = write_patch(directory.path(), "first.sst", 1, &[("a", 5, "first")]);
    let second = write_patch(directory.path(), "second.sst", 2, &[("a", 5, "second")]);
    let manifest = manifest(Vec::new(), vec![first.clone(), second]);
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &first)
        .unwrap()
        .unwrap();

    let error = merge_compaction(&inputs, &Apply, |_, _, _| Ok(()), |_| Ok(())).unwrap_err();
    assert!(matches!(error, Error::InvalidManifest { .. }));
}

fn garbage_record(key: &[u8], lsn: StrataLsn, value: &[u8]) -> GarbageRecord {
    GarbageRecord {
        key: SegmentKey {
            segment_id: 1,
            blob_key: BlobKey::new(key).unwrap(),
        },
        lsn,
        event: GarbageEvent::Retired {
            record: RecordRef {
                segment_id: 1,
                offset: lsn,
                len: value.len() as u64,
            },
        },
        summary_delta: SegmentGcSummaryDelta::default(),
    }
}

fn manifest(base: Vec<TableMeta>, patches: Vec<TableMeta>) -> Manifest {
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: base,
            add_patches: patches,
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    manifest
}

fn write_base(root: &Path, id: u64, rows: &[(&str, &str)]) -> TableMeta {
    let mut writer = TableWriter::create_base(root, "base.sst", id, 0, "base-v1").unwrap();
    for (key, value) in rows {
        writer.add(key.as_bytes(), value.as_bytes()).unwrap();
    }
    writer.finish().unwrap()
}

fn write_patch(root: &Path, path: &str, id: u64, rows: &[(&str, u64, &str)]) -> TableMeta {
    let mut writer = TableWriter::create_patch(root, path, id, 0, "patch-v1").unwrap();
    for (key, sequence, value) in rows {
        writer
            .add_patch(key.as_bytes(), *sequence, value.as_bytes())
            .unwrap();
    }
    writer.finish().unwrap()
}
