use std::sync::Arc;

use strata_lsm::{
    Error, Manifest, ManifestEdit, TableMeta, TableStore, select_base_compaction_inputs,
    select_compaction_inputs, select_patch_compaction_inputs,
};
use tempfile::TempDir;

#[test]
fn selects_the_overlap_closed_patch_set_and_relevant_base_files() {
    let base = [
        table(1, "base-a.sst", b"a", b"f", false),
        table(2, "base-b.sst", b"g", b"m", false),
        table(3, "base-c.sst", b"n", b"z", false),
    ];
    let patch = table(4, "patch.sst", b"h", b"j", true);
    let overlapping = table(5, "overlapping.sst", b"j", b"k", true);
    let separate = table(6, "separate.sst", b"n", b"p", true);
    let mut manifest =
        Manifest::empty("base-v1", "patch-v1", std::num::NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: base.to_vec(),
            add_patches: vec![patch.clone(), overlapping.clone(), separate.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();

    let directory = TempDir::new().unwrap();
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &patch)
        .unwrap()
        .unwrap();

    assert_eq!(inputs.base, [base[1].clone()]);
    assert_eq!(inputs.patches, [patch.clone(), overlapping.clone()]);
    assert_eq!(inputs.first_key, b"g");
    assert_eq!(inputs.last_key, b"m");
    assert!(files.is_pinned(&base[1]));
    assert!(files.is_pinned(&patch));
    assert!(files.is_pinned(&overlapping));
    assert!(!files.is_pinned(&separate));
    assert!(
        select_compaction_inputs(&manifest, &files, 0, &patch)
            .unwrap()
            .is_none()
    );

    drop(inputs);
    assert!(
        select_compaction_inputs(&manifest, &files, 0, &patch)
            .unwrap()
            .is_some()
    );
}

#[test]
fn full_selection_recloses_patches_after_the_base_range_expands() {
    let base = table(1, "base.sst", b"a", b"z", false);
    let seed = table(2, "seed.sst", b"a", b"m", true);
    let extension = table(3, "extension.sst", b"x", b"x", true);
    let separate = table(4, "separate.sst", b"zz", b"zz", true);
    let mut manifest =
        Manifest::empty("base-v1", "patch-v1", std::num::NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![base.clone()],
            add_patches: vec![seed.clone(), extension.clone(), separate.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();

    let directory = TempDir::new().unwrap();
    let files = Arc::new(TableStore::new(directory.path()));
    let patch_inputs =
        select_patch_compaction_inputs(&manifest, &files, 0, std::slice::from_ref(&seed))
            .unwrap()
            .unwrap();
    assert!(patch_inputs.base.is_empty());
    assert_eq!(patch_inputs.patches.as_slice(), std::slice::from_ref(&seed));
    assert!(!files.is_pinned(&base));
    assert!(!files.is_pinned(&extension));
    drop(patch_inputs);

    let full_inputs = select_compaction_inputs(&manifest, &files, 0, &seed)
        .unwrap()
        .unwrap();
    assert_eq!(full_inputs.base.as_slice(), std::slice::from_ref(&base));
    assert_eq!(full_inputs.patches, [seed.clone(), extension.clone()]);
    assert_eq!(full_inputs.first_key, b"a");
    assert_eq!(full_inputs.last_key, b"z");
    assert!(files.is_pinned(&base));
    assert!(files.is_pinned(&seed));
    assert!(files.is_pinned(&extension));
    assert!(!files.is_pinned(&separate));
}

#[test]
fn one_seed_does_not_bridge_a_disconnected_patch_across_an_unselected_base() {
    let earlier_base = table(1, "earlier.sst", b"300000", b"449092", false);
    let later_base = table(2, "later.sst", b"449102", b"997424", false);
    let later_patch = table(3, "later-patch.sst", b"937068", b"938090", true);
    let disconnected_patch = table(4, "disconnected.sst", b"1007655", b"1008677", true);
    assert!(disconnected_patch.last_key < earlier_base.first_key);
    let mut manifest =
        Manifest::empty("base-v1", "patch-v1", std::num::NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![earlier_base.clone(), later_base.clone()],
            add_patches: vec![later_patch.clone(), disconnected_patch.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();

    let directory = TempDir::new().unwrap();
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &files, 0, &later_patch)
        .unwrap()
        .unwrap();

    assert_eq!(inputs.base, [later_base]);
    assert_eq!(inputs.patches, [later_patch]);
    assert!(!files.is_pinned(&earlier_base));
    assert!(!files.is_pinned(&disconnected_patch));
}

#[test]
fn rejects_a_patch_that_is_not_live() {
    let manifest = Manifest::empty("base-v1", "patch-v1", std::num::NonZeroU32::new(1).unwrap());
    let directory = TempDir::new().unwrap();
    let files = Arc::new(TableStore::new(directory.path()));
    let error = select_compaction_inputs(
        &manifest,
        &files,
        0,
        &table(1, "stale.sst", b"a", b"z", true),
    )
    .err()
    .unwrap();

    assert!(matches!(error, Error::InvalidTable(_)));
}

#[test]
fn base_seed_selects_a_cold_base_and_every_overlapping_patch() {
    let cold = table(1, "cold.sst", b"a", b"z", false);
    let first = table(2, "first.sst", b"b", b"m", true);
    let transitive = table(3, "transitive.sst", b"m", b"x", true);
    let separate = table(4, "separate.sst", b"zz", b"zz", true);
    let mut manifest =
        Manifest::empty("base-v1", "patch-v1", std::num::NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: vec![cold.clone()],
            add_patches: vec![first.clone(), transitive.clone(), separate.clone()],
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();

    let directory = TempDir::new().unwrap();
    let files = Arc::new(TableStore::new(directory.path()));
    let inputs = select_base_compaction_inputs(&manifest, &files, 0, &cold)
        .unwrap()
        .unwrap();

    assert_eq!(inputs.base, [cold]);
    assert_eq!(inputs.patches, [first, transitive]);
    assert!(!files.is_pinned(&separate));
}

fn table(id: u64, path: &str, first: &[u8], last: &[u8], patch: bool) -> TableMeta {
    TableMeta {
        id,
        partition: 0,
        relative_path: path.to_owned(),
        first_key: first.to_vec(),
        last_key: last.to_vec(),
        min_lsn: patch.then_some(1),
        max_lsn: patch.then_some(1),
        merge_applied_through_lsn: None,
        record_count: 1,
        file_len: 1,
        checksum: [0; 32],
    }
}
