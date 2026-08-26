use std::{num::NonZeroU32, path::Path, sync::Arc};

use strata_core::{BlobKey, RecordRef, SegmentGcSummaryDelta};
use strata_lsm::{
    GarbageEvent, GarbageRecord, Manifest, ManifestEdit, MergeOperator, Result, SegmentKey,
    Snapshot, StrataLsn, TableMeta, TableReader, TableStore, TableTarget, TableWriter,
    select_compaction_inputs, write_compaction,
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
fn writes_split_outputs_and_returns_the_publishable_edit() {
    let directory = TempDir::new().unwrap();
    let base = write_base(directory.path(), 1, &[("a", "A"), ("b", "B"), ("c", "C")]);
    let patch = write_patch(directory.path(), 2, &[("b", 2, "-2"), ("d", 3, "D")]);
    let manifest = manifest(base.clone(), patch.clone());
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &patch)
        .unwrap()
        .unwrap();

    let mut next_id = 10;
    let (edit, records) = write_compaction(
        &inputs,
        &Apply,
        1, // Deliberately tiny: every complete key becomes its own SST.
        || {
            let id = next_id;
            next_id += 1;
            Ok(TableTarget::base(id))
        },
    )
    .unwrap();

    assert_eq!(edit.remove, ["base.sst", "patch.sst"]);
    assert_eq!(edit.add_base.len(), 4);
    assert!(edit.add_base.iter().all(|table| table.record_count == 1));
    assert!(
        edit.add_base
            .iter()
            .all(|table| directory.path().join(&table.relative_path).exists())
    );
    assert_eq!(
        records,
        [
            garbage_record(b"b", 2, b"-2"),
            garbage_record(b"d", 3, b"D"),
        ]
    );

    // Preparing output neither publishes the edit nor removes its reserved inputs.
    assert_eq!(
        manifest.partitions[&0].base.as_slice(),
        std::slice::from_ref(&base)
    );
    assert!(directory.path().join(&base.relative_path).exists());
    assert!(directory.path().join(&patch.relative_path).exists());
    assert!(files.is_pinned(&base));
    assert!(files.is_pinned(&patch));

    let mut published = manifest;
    published.apply(&edit).unwrap();
    let snapshot = Snapshot::new(files, Arc::new(published), u64::MAX).unwrap();
    for (key, value) in [("a", "A"), ("b", "B-2"), ("c", "C"), ("d", "D")] {
        assert_eq!(
            snapshot.get(0, key.as_bytes(), &Apply).unwrap().as_deref(),
            Some(value.as_bytes())
        );
    }
}

#[test]
fn preserves_explicit_key_prefixes() {
    let directory = TempDir::new().unwrap();
    let base = write_base(directory.path(), 1, &[("z", "Z")]);
    let mut writer =
        TableWriter::create_patch(directory.path(), "prefix.sst", 2, 0, "patch-v1").unwrap();
    writer.add_prefix_patch(b"blob", b"-1", 1, b"A").unwrap();
    writer.add_prefix_patch(b"blob", b"-2", 2, b"B").unwrap();
    let patch = writer.finish().unwrap();
    let manifest = manifest(base, patch.clone());
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &patch)
        .unwrap()
        .unwrap();

    let (edit, _) =
        write_compaction(&inputs, &Apply, u64::MAX, || Ok(TableTarget::base(10))).unwrap();

    let reader = TableReader::open_base(directory.path(), &edit.add_base[0], "base-v1").unwrap();
    let mut rows = Vec::new();
    reader
        .scan_key_prefix(b"blob", |key, value| {
            rows.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rows,
        [
            (b"blob-1".to_vec(), b"A".to_vec()),
            (b"blob-2".to_vec(), b"B".to_vec()),
        ]
    );
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

fn manifest(base: TableMeta, patch: TableMeta) -> Manifest {
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![base],
            add_patches: vec![patch],
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

fn write_patch(root: &Path, id: u64, rows: &[(&str, u64, &str)]) -> TableMeta {
    let mut writer = TableWriter::create_patch(root, "patch.sst", id, 0, "patch-v1").unwrap();
    for (key, sequence, value) in rows {
        writer
            .add_patch(key.as_bytes(), *sequence, value.as_bytes())
            .unwrap();
    }
    writer.finish().unwrap()
}
