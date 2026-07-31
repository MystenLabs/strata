use std::{
    num::NonZeroU32,
    path::Path,
    sync::{Arc, Barrier},
    thread,
};

use strata_index::StrataIndex;
use strata_lsm::{
    Manifest, ManifestEdit, MergeOperator, Result as LsmResult, Snapshot, StrataLsn, TableMeta,
    TableStore, TableWriter,
};
use tempfile::TempDir;
use typed_store::DBMetrics;

const LSM_NAME: &str = "lsm";
const SCHEMA_ID: &str = "lsm-v1";
const PATCH_FORMAT_ID: &str = "patch-v1";

struct Replace;

impl MergeOperator for Replace {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        _emit: &mut dyn FnMut(strata_lsm::GarbageRecord) -> LsmResult<()>,
    ) -> LsmResult<Option<Vec<u8>>> {
        Ok(patches
            .last()
            .map(|(_, value)| value.to_vec())
            .or_else(|| base.map(<[u8]>::to_vec)))
    }
}

#[tokio::test]
async fn concurrent_compactions_survive_reopen_and_old_snapshots() {
    DBMetrics::get();
    let directory = TempDir::new().unwrap();
    let lsm_root = directory.path().join("lsm");
    let db_path = directory.path().join("index");
    let metric_suffix = directory.path().display().to_string();
    let index = StrataIndex::open_path(&db_path, "strata", &metric_suffix).unwrap();
    let files = Arc::new(TableStore::new(&lsm_root));

    let old_tables = [
        write_base(&lsm_root, 1, "base/a.sst", &[("a", "A")]),
        write_base(&lsm_root, 2, "base/b.sst", &[("b", "B")]),
        write_base(&lsm_root, 3, "base/c.sst", &[("c", "C")]),
        write_base(&lsm_root, 4, "base/d.sst", &[("d", "D")]),
    ];
    let mut initial = Manifest::empty(SCHEMA_ID, PATCH_FORMAT_ID, NonZeroU32::new(1).unwrap());
    initial
        .apply(&ManifestEdit {
            remove: Vec::new(),
            add_base: old_tables.to_vec(),
            add_patches: Vec::new(),
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let mut batch = index.batch();
    index
        .put_lsm_manifest_batch(&mut batch, LSM_NAME, &initial)
        .unwrap();
    batch.write_with_sync(true).unwrap();

    let old_snapshot = Snapshot::new(files.clone(), Arc::new(initial.clone()), u64::MAX).unwrap();
    assert_reads(&old_snapshot);

    // Disjoint reservations let both compactions finish without invalidating each other's work.
    let first_reservation = files
        .reserve_for_compaction(&old_tables[..2])
        .unwrap()
        .expect("A and B should be available");
    let second_reservation = files
        .reserve_for_compaction(&old_tables[2..])
        .unwrap()
        .expect("C and D should be available");
    let ab = write_base(&lsm_root, 5, "compacted/ab.sst", &[("a", "A"), ("b", "B")]);
    let cd = write_base(&lsm_root, 6, "compacted/cd.sst", &[("c", "C"), ("d", "D")]);

    // A synced output whose batch never commits is an orphan, not part of the live manifest.
    let orphan = write_base(&lsm_root, 7, "orphan/z.sst", &[("z", "orphan")]);
    let mut abandoned_batch = index.batch();
    index
        .merge_lsm_manifest_batch(
            &mut abandoned_batch,
            LSM_NAME,
            &ManifestEdit {
                remove: Vec::new(),
                add_base: vec![orphan.clone()],
                add_patches: Vec::new(),
                materialized_through: None,
                wal_retained_from: None,
            },
        )
        .unwrap();
    drop(abandoned_batch);
    assert_eq!(index.get_lsm_manifest(LSM_NAME).unwrap(), Some(initial));

    let first_edit = ManifestEdit {
        remove: old_tables[..2]
            .iter()
            .map(|table| table.relative_path.clone())
            .collect(),
        add_base: vec![ab],
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    };
    let second_edit = ManifestEdit {
        remove: old_tables[2..]
            .iter()
            .map(|table| table.relative_path.clone())
            .collect(),
        add_base: vec![cd],
        add_patches: Vec::new(),
        materialized_through: None,
        wal_retained_from: None,
    };

    // Both edits are prepared against the same live manifest, then published independently.
    let ready = Arc::new(Barrier::new(2));
    thread::scope(|scope| {
        let first_index = index.clone();
        let first_ready = ready.clone();
        let first = scope.spawn(move || {
            let _reservation = first_reservation;
            let mut batch = first_index.batch();
            first_index
                .merge_lsm_manifest_batch(&mut batch, LSM_NAME, &first_edit)
                .unwrap();
            first_ready.wait();
            batch.write_with_sync(true).unwrap();
        });

        let second_index = index.clone();
        let second_ready = ready;
        let second = scope.spawn(move || {
            let _reservation = second_reservation;
            let mut batch = second_index.batch();
            second_index
                .merge_lsm_manifest_batch(&mut batch, LSM_NAME, &second_edit)
                .unwrap();
            second_ready.wait();
            batch.write_with_sync(true).unwrap();
        });

        first.join().unwrap();
        second.join().unwrap();
    });

    let current = index.get_lsm_manifest(LSM_NAME).unwrap().unwrap();
    assert_eq!(current.generation, 3);
    assert_eq!(
        base_paths(&current),
        ["compacted/ab.sst", "compacted/cd.sst"]
    );
    assert!(!base_paths(&current).contains(&orphan.relative_path.as_str()));
    assert!(lsm_root.join(&orphan.relative_path).exists());

    let current_snapshot =
        Snapshot::new(files.clone(), Arc::new(current.clone()), u64::MAX).unwrap();
    assert_reads(&old_snapshot);
    assert_reads(&current_snapshot);

    // Publication makes inputs obsolete, but the old snapshot still prevents physical deletion.
    for table in &old_tables {
        assert!(!files.remove_if_unpinned(table).unwrap());
        assert!(lsm_root.join(&table.relative_path).exists());
    }
    drop(old_snapshot);
    for table in &old_tables {
        assert!(files.remove_if_unpinned(table).unwrap());
        assert!(!lsm_root.join(&table.relative_path).exists());
    }

    drop(current_snapshot);
    drop(index);
    let reopened = StrataIndex::open_path(&db_path, "strata", &metric_suffix).unwrap();
    assert_eq!(reopened.get_lsm_manifest(LSM_NAME).unwrap(), Some(current));
}

fn write_base(root: &Path, id: u64, relative_path: &str, rows: &[(&str, &str)]) -> TableMeta {
    let mut writer = TableWriter::create_base(root, relative_path, id, 0, SCHEMA_ID).unwrap();
    for (key, value) in rows {
        writer.add(key.as_bytes(), value.as_bytes()).unwrap();
    }
    writer.finish().unwrap()
}

fn assert_reads(snapshot: &Snapshot) {
    for (key, value) in [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")] {
        assert_eq!(
            snapshot
                .get(0, key.as_bytes(), &Replace)
                .unwrap()
                .as_deref(),
            Some(value.as_bytes())
        );
    }
}

fn base_paths(manifest: &Manifest) -> Vec<&str> {
    manifest.partitions[&0]
        .base
        .iter()
        .map(|table| table.relative_path.as_str())
        .collect()
}
