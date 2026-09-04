//! The table-reader cache: snapshots over the same files share one open reader.

use std::{num::NonZeroU32, path::Path, sync::Arc};

use lsm::{
    Manifest, ManifestEdit, Replace, Snapshot, TableMeta, TableStore, TableWriter,
    encode_inline_value,
};
use tempfile::TempDir;

#[test]
fn snapshots_share_cached_readers_until_the_file_is_removed() {
    let directory = TempDir::new().unwrap();
    let base = write_base(directory.path());
    let patch = write_patch(directory.path());
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![base.clone()],
            add_patches: vec![patch.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let tables = Arc::new(TableStore::new(directory.path()));
    let manifest = Arc::new(manifest);
    assert_eq!(tables.cached_reader_count(), 0);

    let first = Snapshot::new(Arc::clone(&tables), Arc::clone(&manifest), 10).unwrap();
    assert_eq!(tables.cached_reader_count(), 2);
    let second = Snapshot::new(Arc::clone(&tables), Arc::clone(&manifest), 10).unwrap();
    // A second snapshot over the same files opens nothing new.
    assert_eq!(tables.cached_reader_count(), 2);
    assert_eq!(
        first.get(0, b"a", &Replace).unwrap(),
        Some(encode_inline_value(b"-5"))
    );
    assert_eq!(
        second.get(0, b"a", &Replace).unwrap(),
        Some(encode_inline_value(b"-5"))
    );

    // The patch file cannot go while a snapshot pins it, and its reader stays with it.
    drop(first);
    assert!(!tables.remove_if_unpinned(&patch).unwrap());
    assert_eq!(tables.cached_reader_count(), 2);
    drop(second);
    assert!(tables.remove_if_unpinned(&patch).unwrap());
    assert_eq!(tables.cached_reader_count(), 1);

    let mut without_patch = (*manifest).clone();
    without_patch
        .apply(&ManifestEdit {
            remove: vec![patch.relative_path.clone()],
            add_base: Vec::new(),
            add_patches: Vec::new(),
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let third = Snapshot::new(Arc::clone(&tables), Arc::new(without_patch), 10).unwrap();
    assert_eq!(tables.cached_reader_count(), 1);
    assert_eq!(
        third.get(0, b"a", &Replace).unwrap(),
        Some(encode_inline_value(b"A"))
    );
}

fn write_base(root: &Path) -> TableMeta {
    let mut writer = TableWriter::create_base(root, "base.sst", 1, 0, "base-v1").unwrap();
    writer.add(b"a", &encode_inline_value(b"A")).unwrap();
    writer.finish().unwrap()
}

fn write_patch(root: &Path) -> TableMeta {
    let mut writer = TableWriter::create_patch(root, "patch.sst", 2, 0, "patch-v1").unwrap();
    writer
        .add_patch(b"a", 5, &encode_inline_value(b"-5"))
        .unwrap();
    writer.finish().unwrap()
}
