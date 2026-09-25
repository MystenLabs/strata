//! Backend-agnostic storage port for the Strata metadata index.
//!
//! The index needs a small, well-understood slice of RocksDB: point reads, point writes, full
//! forward scans, atomic write batches with merge operands, and a read snapshot. This module
//! defines that slice as a trait so the index does not have to name any particular RocksDB
//! wrapper crate.
//!
//! The port is deliberately *byte-level*. Key and value encoding is Strata's durable on-disk
//! contract, so [`codec`] owns it rather than delegating to whatever backend is plugged in; a
//! backend cannot silently change the format out from under an existing index.
//!
//! The port abstracts over *which RocksDB wrapper*, not over *which storage engine* — naming
//! `rocksdb` types for column-family options is intentional and carries no dependency cost.
//!
//! Two implementations are expected:
//!
//! * [`rocks::RocksBackend`], the default in this crate, sitting directly on `rocksdb`.
//! * An embedder-supplied adapter. An embedder that already runs its own RocksDB instance can
//!   implement [`IndexDb`] over it, and Strata's column families then live in that instance
//!   without Strata depending on the embedder's wrapper crate.

pub mod codec;
pub mod map;
pub mod options;
pub mod rocks;

#[cfg(test)]
mod tests;

use std::fmt::Debug;

use crate::Result;

pub use map::TypedMap;
pub use rocks::RocksBackend;

/// Storage operations the Strata index needs from a RocksDB-like backend.
///
/// Implementations must be safe to share across threads: the index clones one handle into every
/// column-family map, and store workers read and write concurrently.
pub trait IndexDb: Send + Sync + Debug {
    /// Reads a single key, returning `None` when it is absent.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Reports whether a key is present without materializing its value.
    fn contains_key(&self, cf: &str, key: &[u8]) -> Result<bool>;

    /// Writes a single key, committing immediately.
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Deletes a single key, committing immediately.
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Scans a column family forward from its first key.
    ///
    /// The index only ever performs whole-family forward scans, so the port does not carry the
    /// bounded and reversed iterator variants.
    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>>;

    /// Captures a read snapshot for read-your-writes batches.
    fn snapshot<'a>(&'a self) -> Result<Box<dyn IndexSnapshot + 'a>>;

    /// Opens an empty atomic write batch.
    ///
    /// The returned batch owns whatever it needs to commit, so it may outlive this borrow.
    fn write_batch(&self) -> Box<dyn IndexWriteBatch>;

    /// Reports whether a column family is already open on this handle.
    fn cf_exists(&self, cf: &str) -> bool;

    /// Creates a column family that does not exist yet.
    fn create_cf(&self, cf: &str, options: &rocksdb::Options) -> Result<()>;

    /// Flushes the write-ahead log, optionally fsyncing it.
    fn flush_wal(&self, sync: bool) -> Result<()>;
}

/// A forward cursor over one column family.
///
/// This is a lending cursor rather than an [`Iterator`] on purpose. `Iterator` cannot yield items
/// that borrow from the iterator itself, so an `Iterator`-shaped port would have to hand out owned
/// `Vec`s — one allocation per key and per value, on every row of every scan. Splitting "advance"
/// from "look at the current row" lets a backend expose the bytes RocksDB already holds, and keeps
/// the trait object-safe.
///
/// [`TypedMap::safe_iter`] wraps this back up as a normal iterator of decoded rows, so callers
/// never see the split.
pub trait RowCursor {
    /// Advances to the next row, returning `false` once the scan is exhausted.
    fn next_row(&mut self) -> Result<bool>;

    /// The key and value of the row the cursor is on.
    ///
    /// Only meaningful after [`RowCursor::next_row`] returned `true`; the borrow ends at the next
    /// advance.
    fn row(&self) -> (&[u8], &[u8]);
}

/// A point-in-time read view, used to give write batches read-your-writes semantics.
pub trait IndexSnapshot: Send {
    /// Reads a key as of the instant the snapshot was captured.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scans a column family as of the instant the snapshot was captured.
    ///
    /// GC planning reads several families plus the epoch history and must see one consistent
    /// view across all of them, so snapshot-consistent scans are part of the port rather than a
    /// convenience layered on top of it.
    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>>;
}

/// An atomic write batch.
///
/// Nothing is durable until [`IndexWriteBatch::write`] returns. Consuming `self: Box<Self>` keeps
/// the trait object-safe while still making the commit a move.
pub trait IndexWriteBatch: Send {
    /// Stages a write.
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Stages a delete.
    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<()>;

    /// Stages a merge operand, resolved by the column family's merge operator.
    fn merge(&mut self, cf: &str, key: &[u8], operand: &[u8]) -> Result<()>;

    /// Size of the staged batch, used to bound batch growth before committing.
    fn size_in_bytes(&self) -> usize;

    /// Commits the batch. `sync` fsyncs the write-ahead log before returning.
    fn write(self: Box<Self>, sync: bool) -> Result<()>;
}
