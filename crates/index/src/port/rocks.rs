//! Default [`IndexDb`] implementation, sitting directly on the `rocksdb` crate.

use std::{path::Path, sync::Arc};

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    WriteBatch, WriteOptions,
};

use crate::{Error, Result};

use super::{IndexDb, IndexSnapshot, IndexWriteBatch, KeyValue, options::default_db_options};

type Db = DBWithThreadMode<MultiThreaded>;

/// Environment override that fsyncs every single-key write, not just committed batches.
///
/// Strata's durability comes from committing batches with `sync` set, so this is a debugging and
/// testing knob. It replaces the `SUI_DB_SYNC_TO_DISK` variable the previous wrapper honoured;
/// that name no longer has any effect here.
const ENV_SYNC_TO_DISK: &str = "STRATA_DB_SYNC_TO_DISK";

/// A Strata index backed by a RocksDB instance this crate owns.
#[derive(Debug, Clone)]
pub struct RocksBackend {
    db: Arc<Db>,
    /// Whether [`IndexDb::put`] and [`IndexDb::delete`] fsync. Read once, at open.
    sync_writes: bool,
}

fn sync_writes_from_env() -> bool {
    std::env::var(ENV_SYNC_TO_DISK).is_ok_and(|value| value != "0")
}

fn write_options(sync: bool) -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(sync);
    options
}

impl RocksBackend {
    /// Opens (creating if absent) a RocksDB instance with the given column families.
    ///
    /// Column families already present on disk but absent from `cf_options` are reopened with
    /// default options. RocksDB refuses to open a database unless every existing family is
    /// named, and the index deliberately shares instances with families it does not own.
    pub fn open(
        path: impl AsRef<Path>,
        db_options: Option<rocksdb::Options>,
        cf_options: &[(String, rocksdb::Options)],
    ) -> Result<Self> {
        let path = path.as_ref();
        let mut options = db_options.unwrap_or_else(default_db_options);
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let named: std::collections::HashSet<&str> =
            cf_options.iter().map(|(name, _)| name.as_str()).collect();
        let mut descriptors: Vec<ColumnFamilyDescriptor> =
            Db::list_cf(&rocksdb::Options::default(), path)
                .unwrap_or_default()
                .into_iter()
                .filter(|existing| !named.contains(existing.as_str()))
                .map(|existing| ColumnFamilyDescriptor::new(existing, rocksdb::Options::default()))
                .collect();
        descriptors.extend(
            cf_options
                .iter()
                .map(|(name, options)| ColumnFamilyDescriptor::new(name, options.clone())),
        );

        let db = Db::open_cf_descriptors(&options, path, descriptors).map_err(rocks_error)?;
        Ok(Self {
            db: Arc::new(db),
            sync_writes: sync_writes_from_env(),
        })
    }

    /// Wraps a RocksDB instance the caller already opened.
    pub fn from_db(db: Arc<Db>) -> Self {
        Self {
            db,
            sync_writes: sync_writes_from_env(),
        }
    }

    /// The underlying handle, for callers that need RocksDB APIs outside the port.
    pub fn inner(&self) -> &Arc<Db> {
        &self.db
    }
}

/// Resolves a column-family handle, borrowing only the database.
///
/// Taking the database rather than `&self` keeps the borrow disjoint from the write batch, which
/// is mutated while the handle is live.
fn cf_handle<'a>(db: &'a Arc<Db>, cf: &str) -> Result<Arc<BoundColumnFamily<'a>>> {
    db.cf_handle(cf)
        .ok_or_else(|| Error::RocksDb(format!("column family {cf} is not open")))
}

// Reads use RocksDB's default `ReadOptions`. The previous wrapper set `ignore_range_deletions`,
// which only changes behaviour in the presence of range tombstones; Strata issues no range
// deletes, so the default is equivalent here and is the safer of the two settings.
impl IndexDb for RocksBackend {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = cf_handle(&self.db, cf)?;
        Ok(self
            .db
            .get_pinned_cf(&handle, key)
            .map_err(rocks_error)?
            .map(|value| value.to_vec()))
    }

    fn contains_key(&self, cf: &str, key: &[u8]) -> Result<bool> {
        let handle = cf_handle(&self.db, cf)?;
        Ok(self
            .db
            .get_pinned_cf(&handle, key)
            .map_err(rocks_error)?
            .is_some())
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = cf_handle(&self.db, cf)?;
        self.db
            .put_cf_opt(&handle, key, value, &write_options(self.sync_writes))
            .map_err(rocks_error)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = cf_handle(&self.db, cf)?;
        self.db
            .delete_cf_opt(&handle, key, &write_options(self.sync_writes))
            .map_err(rocks_error)
    }

    fn iter<'a>(&'a self, cf: &str) -> Result<Box<dyn Iterator<Item = Result<KeyValue>> + 'a>> {
        // `iterator_cf` only borrows the handle for the call itself; the returned iterator borrows
        // the database, so the scan stays lazy and never materializes the whole family.
        let handle = cf_handle(&self.db, cf)?;
        let iter = self.db.iterator_cf(&handle, IteratorMode::Start);
        Ok(Box::new(iter.map(|row| {
            row.map(|(key, value)| (key.to_vec(), value.to_vec()))
                .map_err(rocks_error)
        })))
    }

    fn snapshot<'a>(&'a self) -> Result<Box<dyn IndexSnapshot + 'a>> {
        Ok(Box::new(RocksSnapshot {
            db: &self.db,
            snapshot: self.db.snapshot(),
        }))
    }

    fn write_batch(&self) -> Box<dyn IndexWriteBatch> {
        Box::new(RocksWriteBatch {
            db: Arc::clone(&self.db),
            batch: WriteBatch::default(),
        })
    }

    fn cf_exists(&self, cf: &str) -> bool {
        self.db.cf_handle(cf).is_some()
    }

    fn create_cf(&self, cf: &str, options: &rocksdb::Options) -> Result<()> {
        self.db.create_cf(cf, options).map_err(rocks_error)
    }

    fn drop_cf(&self, cf: &str) -> Result<()> {
        self.db.drop_cf(cf).map_err(rocks_error)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        self.db.flush_wal(sync).map_err(rocks_error)
    }
}

struct RocksSnapshot<'a> {
    db: &'a Arc<Db>,
    snapshot: rocksdb::SnapshotWithThreadMode<'a, Db>,
}

impl IndexSnapshot for RocksSnapshot<'_> {
    fn iter<'a>(&'a self, cf: &str) -> Result<Box<dyn Iterator<Item = Result<KeyValue>> + 'a>> {
        let handle = cf_handle(self.db, cf)?;
        let iter = self.snapshot.iterator_cf(&handle, IteratorMode::Start);
        Ok(Box::new(iter.map(|row| {
            row.map(|(key, value)| (key.to_vec(), value.to_vec()))
                .map_err(rocks_error)
        })))
    }

    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = cf_handle(self.db, cf)?;
        Ok(self
            .snapshot
            .get_pinned_cf(&handle, key)
            .map_err(rocks_error)?
            .map(|value| value.to_vec()))
    }
}

struct RocksWriteBatch {
    db: Arc<Db>,
    batch: WriteBatch,
}

impl IndexWriteBatch for RocksWriteBatch {
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = cf_handle(&self.db, cf)?;
        self.batch.put_cf(&handle, key, value);
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = cf_handle(&self.db, cf)?;
        self.batch.delete_cf(&handle, key);
        Ok(())
    }

    fn merge(&mut self, cf: &str, key: &[u8], operand: &[u8]) -> Result<()> {
        let handle = cf_handle(&self.db, cf)?;
        self.batch.merge_cf(&handle, key, operand);
        Ok(())
    }

    fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }

    fn write(self: Box<Self>, sync: bool) -> Result<()> {
        self.db
            .write_opt(self.batch, &write_options(sync))
            .map_err(rocks_error)
    }
}

fn rocks_error(error: rocksdb::Error) -> Error {
    Error::RocksDb(error.into_string())
}
