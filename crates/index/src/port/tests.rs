//! Behavioural tests for the storage port.
//!
//! The important ones are the compatibility tests: they open one physical database with both the
//! storage wrapper the index ships with today and the port's own backend, and check that each can
//! read what the other wrote. If those pass, adopting the port is not a format migration.

use std::sync::Arc;

use core_types::{SegmentId, StrataLsn};
use tempfile::tempdir;
use typed_store::{
    Map as _,
    rocks::{DBMap, MetricConf, ReadWriteOptions, open_cf_opts},
};

use super::{IndexDb, RocksBackend, TypedMap, options::default_db_options};

/// typed-store registers RocksDB metrics on a Tokio-backed sampler the first time it is used.
fn init_typed_store_metrics() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        typed_store::DBMetrics::get();
    });
}

const CF: &str = "port_compat";

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

fn open_wrapper(path: &std::path::Path) -> DBMap<SegmentId, StrataLsn> {
    init_typed_store_metrics();
    let options = typed_store::rocks::default_db_options().options;
    let db = open_cf_opts(
        path,
        Some(options.clone()),
        MetricConf::new("port_compat"),
        &[(CF, options)],
    )
    .unwrap();
    DBMap::reopen_with_class(&db, Some(CF), Some(CF), &ReadWriteOptions::default(), true).unwrap()
}

// Uses typed-store, whose metrics require a Tokio reactor; the port itself does not.
#[tokio::test]
async fn port_reads_rows_written_by_the_current_wrapper() {
    let dir = tempdir().unwrap();
    let rows: Vec<(SegmentId, StrataLsn)> = vec![(0, 0), (1, 10), (256, 20), (u64::MAX, 30)];

    {
        let map = open_wrapper(dir.path());
        for (key, value) in &rows {
            map.insert(key, value).unwrap();
        }
    }

    let map: TypedMap<SegmentId, StrataLsn> = TypedMap::new(open_port(dir.path()), CF);
    for (key, value) in &rows {
        assert_eq!(map.get(key).unwrap(), Some(*value), "key {key}");
    }
    // Scan order must still be numeric, which is what the segment and LSN sweeps depend on.
    assert_eq!(
        map.safe_iter()
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        rows
    );
}

// Uses typed-store, whose metrics require a Tokio reactor; the port itself does not.
#[tokio::test]
async fn the_current_wrapper_reads_rows_written_by_the_port() {
    let dir = tempdir().unwrap();
    let rows: Vec<(SegmentId, StrataLsn)> = vec![(0, 0), (7, 70), (u64::MAX, 99)];

    {
        let map: TypedMap<SegmentId, StrataLsn> = TypedMap::new(open_port(dir.path()), CF);
        let mut batch = map.batch();
        batch
            .insert_batch(&map, rows.iter().map(|(key, value)| (key, value)))
            .unwrap();
        batch.write_with_sync(true).unwrap();
    }

    let map = open_wrapper(dir.path());
    for (key, value) in &rows {
        assert_eq!(map.get(key).unwrap(), Some(*value), "key {key}");
    }
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
