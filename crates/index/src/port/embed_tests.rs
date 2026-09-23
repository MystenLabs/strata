//! Proof that an embedder running its own typed-store RocksDB can host Strata's index.
//!
//! This is the walrus integration path written out. `TypedStoreBackend` is the adapter walrus would
//! own: it wraps an `Arc<typed_store::rocks::RocksDB>` — the same handle walrus already builds its
//! own `DBMap`s from — and implements [`IndexDb`] by delegating to the `rocksdb` instance inside
//! it. Strata's column families then live in walrus's database, alongside walrus's own, in one
//! process with one block cache and one write-ahead log.
//!
//! Nothing here is part of Strata's shipped code. It lives in tests to keep the seam honest: if a
//! port change makes the adapter impossible to write, this file stops compiling.

use std::sync::Arc;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, WriteBatch, WriteOptions};
use tempfile::tempdir;
use typed_store::{
    Map as _,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, open_cf_opts},
};

use super::{IndexDb, IndexSnapshot, IndexWriteBatch, KeyValue, init_typed_store_metrics};
use crate::{Error, Result, StrataIndex};

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
