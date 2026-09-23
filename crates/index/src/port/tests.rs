//! Behavioural tests for the storage port's own backend.
//!
//! Cross-checks against the wrapper the index previously used live in the `port-compat` crate,
//! which is outside this workspace so that its `typed-store` dependency -- and the walrus git
//! repository it comes from -- stays out of this lockfile.

use std::sync::Arc;

use core_types::{SegmentId, StrataLsn};
use tempfile::tempdir;

use super::{IndexDb, RocksBackend, TypedMap, options::default_db_options};

const CF: &str = "port_behaviour";

fn open_port(path: &std::path::Path) -> Arc<dyn IndexDb> {
    Arc::new(
        RocksBackend::open(
            path,
            Some(default_db_options()),
            &[(CF.to_owned(), default_db_options())],
        )
        .unwrap(),
    )
}

#[test]
fn batches_are_atomic_and_support_delete() {
    let dir = tempdir().unwrap();
    let map: TypedMap<SegmentId, StrataLsn> = TypedMap::new(open_port(dir.path()), CF);
    map.insert(&1, &100).unwrap();
    map.insert(&2, &200).unwrap();

    let mut batch = map.batch();
    batch.insert_batch(&map, [(&3, &300)]).unwrap();
    batch.delete_batch(&map, [&1]).unwrap();
    assert!(batch.size_in_bytes() > 0);
    // Nothing is visible before the commit.
    assert_eq!(map.get(&3).unwrap(), None);
    assert_eq!(map.get(&1).unwrap(), Some(100));

    batch.write().unwrap();
    assert_eq!(map.get(&3).unwrap(), Some(300));
    assert_eq!(map.get(&1).unwrap(), None);
    assert!(map.contains_key(&2).unwrap());
}

#[test]
fn snapshots_ignore_writes_that_land_after_them() {
    let dir = tempdir().unwrap();
    let db = open_port(dir.path());
    let map: TypedMap<SegmentId, StrataLsn> = TypedMap::new(Arc::clone(&db), CF);
    map.insert(&1, &100).unwrap();

    let snapshot = db.snapshot().unwrap();
    map.insert(&1, &999).unwrap();

    assert_eq!(
        map.get_with_snapshot(snapshot.as_ref(), &1).unwrap(),
        Some(100)
    );
    assert_eq!(map.get(&1).unwrap(), Some(999));
}

#[test]
fn empty_families_report_empty_and_missing_families_error() {
    let dir = tempdir().unwrap();
    let db = open_port(dir.path());
    let map: TypedMap<SegmentId, StrataLsn> = TypedMap::new(Arc::clone(&db), CF);
    assert!(map.is_empty().unwrap());
    map.insert(&1, &1).unwrap();
    assert!(!map.is_empty().unwrap());

    assert!(db.get("not_open", b"k").is_err());
    assert!(db.cf_exists(CF));
    assert!(!db.cf_exists("not_open"));
}
