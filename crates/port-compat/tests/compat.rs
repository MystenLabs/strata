//! Compatibility evidence for Strata's storage port.
//!
//! This crate is outside the repository's main workspace on purpose. Everything here depends on
//! `typed-store`, which lives inside the MystenLabs/walrus repository; keeping it out of the main
//! workspace means building or testing Strata never fetches walrus, while these checks still run in
//! CI as their own step.
//!
//! Two things are verified:
//!
//! * the port reads and writes byte-for-byte what the wrapper the index previously used did, so an
//!   existing database is still readable, and
//! * an embedder running its own typed-store RocksDB can host Strata's index, which is the
//!   integration path that motivated the port.

use core_types::{SegmentId, StrataLsn};
use index::port::{
    IndexDb, IndexSnapshot, IndexWriteBatch, KeyValue, RocksBackend, TypedMap, codec::encode_key,
    options::default_db_options,
};
use index::{Error, Result, StrataIndex};
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, WriteBatch, WriteOptions};
use serde::Serialize;
use std::sync::Arc;
use tempfile::tempdir;
use typed_store::{
    Map as _,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, open_cf_opts},
};

const CF: &str = "port_compat";

/// typed-store's metrics register lazily on first use.
///
/// This must be a single shared guard: `DBMetrics::init` constructs its collectors *before* storing
/// them in its `OnceCell`, so two threads reaching first use concurrently both register against
/// `prometheus::default_registry()` and the loser panics with `AlreadyReg`.
fn init_typed_store_metrics() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        typed_store::DBMetrics::get();
    });
}

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

/// The port's key encoding must be byte-identical to the one that wrote every key on disk. The index
/// crate pins the same vectors as literals; this checks those literals against the real thing.
#[test]
fn key_encoding_matches_the_previous_wrapper_exactly() {
    fn assert_same<K: Serialize>(key: &K) {
        assert_eq!(
            encode_key(key).unwrap(),
            typed_store::rocks::be_fix_int_ser(key).unwrap()
        );
    }

    assert_same(&0u64);
    assert_same(&1u64);
    assert_same(&u64::MAX);
    assert_same(&(7u64, 9u64));
    assert_same(&"lsm-manifest-name".to_owned());
    assert_same(&(42 as SegmentId, 1234 as StrataLsn));
}

#[tokio::test]
async fn port_reads_rows_written_by_the_previous_wrapper() {
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
            .collect::<index::Result<Vec<_>>>()
            .unwrap(),
        rows
    );
}

#[tokio::test]
async fn the_previous_wrapper_reads_rows_written_by_the_port() {
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

// ---------------------------------------------------------------------------------------------
// The embedding path: the adapter an embedder owns, and Strata running on top of it.
// ---------------------------------------------------------------------------------------------

type Raw = DBWithThreadMode<MultiThreaded>;

/// The adapter an embedder writes. Roughly 90 lines, and it needs nothing from Strata's internals.
#[derive(Debug)]
struct TypedStoreBackend(Arc<RocksDB>);

fn raw(db: &RocksDB) -> Result<&Raw> {
    match db {
        RocksDB::DB(wrapper) => Ok(&wrapper.underlying),
        RocksDB::OptimisticTransactionDB(_) => Err(Error::RocksDb(
            "Strata's index does not support optimistic transaction databases".to_owned(),
        )),
    }
}

fn cf<'a>(db: &'a Raw, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| Error::RocksDb(format!("column family {name} is not open")))
}

fn err(error: rocksdb::Error) -> Error {
    Error::RocksDb(error.into_string())
}

impl IndexDb for TypedStoreBackend {
    fn get(&self, name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db = raw(&self.0)?;
        Ok(db
            .get_pinned_cf(&cf(db, name)?, key)
            .map_err(err)?
            .map(|value| value.to_vec()))
    }

    fn contains_key(&self, name: &str, key: &[u8]) -> Result<bool> {
        Ok(self.get(name, key)?.is_some())
    }

    fn put(&self, name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let db = raw(&self.0)?;
        db.put_cf(&cf(db, name)?, key, value).map_err(err)
    }

    fn delete(&self, name: &str, key: &[u8]) -> Result<()> {
        let db = raw(&self.0)?;
        db.delete_cf(&cf(db, name)?, key).map_err(err)
    }

    fn iter<'a>(&'a self, name: &str) -> Result<Box<dyn Iterator<Item = Result<KeyValue>> + 'a>> {
        let db = raw(&self.0)?;
        let iter = db.iterator_cf(&cf(db, name)?, IteratorMode::Start);
        Ok(Box::new(iter.map(|row| {
            row.map(|(key, value)| (key.to_vec(), value.to_vec()))
                .map_err(err)
        })))
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn IndexSnapshot + 'a>> {
        let db = raw(&self.0)?;
        Ok(Box::new(Snapshot {
            db,
            snapshot: db.snapshot(),
        }))
    }

    fn write_batch(&self) -> Box<dyn IndexWriteBatch> {
        Box::new(Batch {
            db: Arc::clone(&self.0),
            batch: WriteBatch::default(),
        })
    }

    fn cf_exists(&self, name: &str) -> bool {
        raw(&self.0).is_ok_and(|db| db.cf_handle(name).is_some())
    }

    fn create_cf(&self, name: &str, options: &rocksdb::Options) -> Result<()> {
        raw(&self.0)?.create_cf(name, options).map_err(err)
    }

    fn drop_cf(&self, name: &str) -> Result<()> {
        raw(&self.0)?.drop_cf(name).map_err(err)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        raw(&self.0)?.flush_wal(sync).map_err(err)
    }
}

struct Snapshot<'a> {
    db: &'a Raw,
    snapshot: rocksdb::SnapshotWithThreadMode<'a, Raw>,
}

impl IndexSnapshot for Snapshot<'_> {
    fn get(&self, name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .snapshot
            .get_pinned_cf(&cf(self.db, name)?, key)
            .map_err(err)?
            .map(|value| value.to_vec()))
    }

    fn iter<'a>(&'a self, name: &str) -> Result<Box<dyn Iterator<Item = Result<KeyValue>> + 'a>> {
        let iter = self
            .snapshot
            .iterator_cf(&cf(self.db, name)?, IteratorMode::Start);
        Ok(Box::new(iter.map(|row| {
            row.map(|(key, value)| (key.to_vec(), value.to_vec()))
                .map_err(err)
        })))
    }
}

struct Batch {
    db: Arc<RocksDB>,
    batch: WriteBatch,
}

impl IndexWriteBatch for Batch {
    fn put(&mut self, name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = cf(raw(&self.db)?, name)?;
        self.batch.put_cf(&handle, key, value);
        Ok(())
    }

    fn delete(&mut self, name: &str, key: &[u8]) -> Result<()> {
        let handle = cf(raw(&self.db)?, name)?;
        self.batch.delete_cf(&handle, key);
        Ok(())
    }

    fn merge(&mut self, name: &str, key: &[u8], operand: &[u8]) -> Result<()> {
        let handle = cf(raw(&self.db)?, name)?;
        self.batch.merge_cf(&handle, key, operand);
        Ok(())
    }

    fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }

    fn write(self: Box<Self>, sync: bool) -> Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(sync);
        raw(&self.db)?.write_opt(self.batch, &options).map_err(err)
    }
}

/// Strata's index runs on the embedder's database, and the embedder's own `DBMap` keeps working in
/// the same instance, in the same process.
#[tokio::test]
async fn strata_index_runs_inside_an_embedders_typed_store_database() {
    init_typed_store_metrics();
    let dir = tempdir().unwrap();

    // The embedder opens its database and its own column family, exactly as it does today.
    let options = typed_store::rocks::default_db_options().options;
    let db = open_cf_opts(
        dir.path(),
        Some(options.clone()),
        MetricConf::new("embedder"),
        &[("embedder_own_cf", options)],
    )
    .unwrap();
    let embedder_map: DBMap<u64, String> = DBMap::reopen_with_class(
        &db,
        Some("embedder_own_cf"),
        Some("embedder_own_cf"),
        &ReadWriteOptions::default(),
        true,
    )
    .unwrap();
    embedder_map.insert(&1, &"embedder row".to_owned()).unwrap();

    // It hands the same handle to Strata through the adapter. Strata creates its families inside
    // the embedder's instance.
    let backend: Arc<dyn IndexDb> = Arc::new(TypedStoreBackend(Arc::clone(&db)));
    let index = StrataIndex::from_db(backend, "walrus/shard-7").unwrap();

    // Strata's metadata operations work against it, through both the batch and point paths.
    let mut batch = index.batch();
    index.put_next_lsn_batch(&mut batch, 9999).unwrap();
    batch.write_with_sync(true).unwrap();
    assert_eq!(index.get_next_lsn().unwrap(), 9999);

    // Strata's families are prefixed and live beside the embedder's, which is untouched.
    for name in index.cf_names().as_strs() {
        assert!(name.starts_with("walrus/shard-7/"), "unprefixed: {name}");
        assert!(index.db().cf_exists(name));
    }
    assert!(index.db().cf_exists("embedder_own_cf"));
    assert_eq!(
        embedder_map.get(&1).unwrap().as_deref(),
        Some("embedder row")
    );

    // And a read-your-writes batch still sees a consistent snapshot through the adapter.
    let mut indexed = index.indexed_batch().unwrap();
    indexed
        .put(index.segment_garbage_log_positions(), &7, &70)
        .unwrap();
    assert_eq!(
        indexed
            .get(index.segment_garbage_log_positions(), &7)
            .unwrap(),
        Some(70)
    );
    indexed.write().unwrap();
    assert_eq!(
        index.segment_garbage_log_positions().get(&7).unwrap(),
        Some(70)
    );
}
