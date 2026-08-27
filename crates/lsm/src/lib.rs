//! Unlogged memtable and immutable-table storage for Strata.
//!
//! This crate intentionally has a small surface. It defines byte ordering, merge semantics,
//! durable manifest metadata, concrete immutable SST formats, point reads and sorted range
//! iterators over a pinned snapshot, and prepared compactions. [`Lsm`] accepts caller-assigned LSNs;
//! WAL, payload-segment, recovery, scheduling, and durable publication remain the store's job.
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
mod garbage_log;
mod iterator;
mod manifest;
mod memtable;
mod merge;
mod recovery;
mod snapshot;
mod table;
mod table_format;

pub use compaction::{
    CompactionInputs, merge_compaction, select_base_compaction_inputs, select_compaction_inputs,
    select_patch_compaction_inputs, write_compaction, write_patch_compaction,
};
pub use core_types::{GarbageEvent, SegmentKey, StrataLsn};
pub use engine::{
    Lsm, LsmOptions, Mutation, RolledMemtable, StoredValue, TableTarget, WriteBatchResult,
    WriteResult, decode_record_ref, decode_value, encode_blob_value, encode_inline_value,
    encode_record_ref,
};
pub use error::{Error, Result};
pub use garbage_log::{
    GarbageLog, GarbageLogPosition, GarbageRecord, SegmentGarbageLog, fold_segment_garbage,
    read_segment_garbage,
};
pub use iterator::LsmIter;
pub use manifest::{Manifest, ManifestEdit, PartitionManifest, TableMeta};
pub use memtable::{
    DEFAULT_MEMTABLE_BUFFER_BYTES, FrozenMemtable, Memtable, MemtableEntries, MemtableEntry,
    MemtableRolloverPolicy,
};
pub use merge::{MergeOperator, Replace};
pub use segment;
pub use snapshot::{CompactionReservation, LiveSnapshots, Snapshot, SnapshotPin, TableStore};
pub use table::{BlockCacheStats, DEFAULT_BLOCK_CACHE_BYTES, TableReader, TableWriter};

/// Current on-disk manifest and SST format version.
pub const FORMAT_VERSION: u32 = 7;
