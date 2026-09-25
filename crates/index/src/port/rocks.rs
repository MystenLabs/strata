//! Default [`IndexDb`] implementation, sitting directly on the `rocksdb` crate.

use std::{path::Path, sync::Arc};

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBAccess, DBRawIteratorWithThreadMode,
    DBWithThreadMode, MultiThreaded, ReadOptions, WriteBatch, WriteOptions,
};

use crate::{Error, Result};

use super::{IndexDb, IndexSnapshot, IndexWriteBatch, RowCursor, options::default_db_options};

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

    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>> {
        // `raw_iterator_cf_opt` hands back the bytes RocksDB already holds. The higher-level
        // `iterator_cf` boxes both key and value on every row, which is two allocations per row
        // before anything is even decoded.
        let handle = cf_handle(&self.db, cf)?;
        let iter = self.db.raw_iterator_cf_opt(&handle, ReadOptions::default());
        Ok(Box::new(RawCursor::new(iter)))
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

    fn flush_wal(&self, sync: bool) -> Result<()> {
        self.db.flush_wal(sync).map_err(rocks_error)
    }
}

struct RocksSnapshot<'a> {
    db: &'a Arc<Db>,
    snapshot: rocksdb::SnapshotWithThreadMode<'a, Db>,
}

impl IndexSnapshot for RocksSnapshot<'_> {
    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>> {
        let handle = cf_handle(self.db, cf)?;
        let iter = self
            .snapshot
            .raw_iterator_cf_opt(&handle, ReadOptions::default());
        Ok(Box::new(RawCursor::new(iter)))
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

/// Wraps a RocksDB raw iterator as a [`RowCursor`].
///
/// The first advance seeks to the first key; later ones step forward. A raw iterator reports
/// exhaustion and I/O failure the same way -- by going invalid -- so `status` distinguishes them.
struct RawCursor<'a, D: DBAccess> {
    iter: DBRawIteratorWithThreadMode<'a, D>,
    started: bool,
}

impl<'a, D: DBAccess> RawCursor<'a, D> {
    fn new(iter: DBRawIteratorWithThreadMode<'a, D>) -> Self {
        Self {
            iter,
            started: false,
        }
    }
}

impl<D: DBAccess> RowCursor for RawCursor<'_, D> {
    fn next_row(&mut self) -> Result<bool> {
        if self.started {
            self.iter.next();
        } else {
            self.iter.seek_to_first();
            self.started = true;
        }
        if self.iter.valid() {
            return Ok(true);
        }
        self.iter.status().map_err(rocks_error)?;
        Ok(false)
    }

    fn row(&self) -> (&[u8], &[u8]) {
        (
            self.iter.key().unwrap_or_default(),
            self.iter.value().unwrap_or_default(),
        )
    }
}

fn rocks_error(error: rocksdb::Error) -> Error {
    Error::RocksDb(error.into_string())
}
