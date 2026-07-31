//! Memtable, operation WAL, and immutable-table storage for Strata.
//!
//! This crate intentionally has a small surface. It defines byte ordering, merge semantics,
//! durable manifest metadata, an independently rolled WAL, concrete immutable SST formats,
//! point reads over a pinned snapshot, and prepared compactions. [`Lsm`] joins the active segment,
//! WAL, pipelined memtable writes, and snapshot reads behind one small façade. The segment format
//! and readers remain in [`segment`]. Recovery, scheduling, and durable manifest publication remain
//! the caller's job.
//!
//! The engine is designed around three properties:
//!
//! - foreground durability remains owned by the caller;
//! - reads merge a snapshot of split base tables and immutable patch tables;
//! - compaction is prepared off-path and becomes visible only when the caller publishes a manifest.
//!
//! Keys and values are opaque byte buffers ordered with Rust's unsigned lexicographic slice order.
//! Store-specific encoding, partitioning, and merge semantics remain outside this crate and are
//! identified durably in [`Manifest`].

mod compaction;
mod engine;
mod error;
mod file_sync;
mod garbage_log;
mod manifest;
mod memtable;
mod merge;
mod recovery;
mod snapshot;
mod table;
mod wal;

pub use compaction::{
    CompactionInputs, merge_compaction, select_compaction_inputs, select_patch_compaction_inputs,
    write_compaction, write_patch_compaction,
};
pub use engine::{
    Lsm, LsmCheckpoint, LsmOptions, Mutation, RolledMemtable, RolledSegment, SegmentRecord,
    StoredValue, WriteBatchResult, WriteResult, decode_record_ref, decode_value, encode_blob_value,
    encode_inline_value, encode_record_ref,
};
pub use error::{Error, Result};
pub use file_sync::{FileSyncSender, FileSyncTask, FileSyncer, file_sync_channel};
pub use garbage_log::{
    GarbageLog, GarbageLogPosition, GarbageRecord, SegmentGarbageLog, fold_segment_garbage,
    read_segment_garbage,
};
pub use manifest::{Manifest, ManifestEdit, PartitionManifest, TableMeta};
pub use memtable::{
    DEFAULT_MEMTABLE_BUFFER_BYTES, FrozenMemtable, Memtable, MemtableCursor, MemtableEntries,
    MemtableEntry, MemtableRolloverPolicy,
};
pub use merge::{MergeOperator, Replace};
pub use snapshot::{
    CompactionReservation, LiveSnapshots, LsmScan, Snapshot, SnapshotPin, TableStore,
};
pub use strata_core::{GarbageEvent, SegmentKey, StrataLsn};
pub use strata_segment as segment;
pub use table::{BlockCacheStats, DEFAULT_BLOCK_CACHE_BYTES, TableReader, TableWriter};
pub use wal::{Wal, WalEntry, WalPosition};

/// Current on-disk manifest and SST format version.
pub const FORMAT_VERSION: u32 = 7;
