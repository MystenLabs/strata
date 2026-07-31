use std::{io, path::PathBuf};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Engine-level failures shared by codecs and the future table implementation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("merge failed: {0}")]
    Merge(String),

    #[error("invalid LSM manifest: {reason}")]
    InvalidManifest { reason: String },

    #[error("invalid SST: {0}")]
    InvalidTable(String),

    #[error("partition {partition:?} is outside the configured partition count {partition_count}")]
    InvalidPartition {
        partition: u32,
        partition_count: u32,
    },

    #[error("incompatible LSM schema: expected {expected}, found {actual}")]
    IncompatibleSchema { expected: String, actual: String },

    #[error("corrupt table at {path}: {reason}")]
    CorruptTable { path: PathBuf, reason: String },

    #[error("invalid garbage log: {0}")]
    InvalidGarbageLog(String),

    #[error("corrupt garbage log at {path}: {reason}")]
    CorruptGarbageLog { path: PathBuf, reason: String },

    #[error("serialization failed: {0}")]
    Serialization(String),

    #[error("file sync queue is closed")]
    FileSyncQueueClosed,

    #[error("LSM is halted: {reason}")]
    LsmHalted { reason: String },

    #[error("segment error: {0}")]
    Segment(#[from] strata_segment::Error),

    #[error("record format error: {0}")]
    Core(#[from] strata_core::Error),

    #[error("LSM LSN overflow")]
    LsnOverflow,

    #[error("invalid encoded record reference length {actual}, expected {expected}")]
    InvalidRecordRefEncoding { expected: usize, actual: usize },

    #[error("replacement segment {next} must follow active segment {current}")]
    SegmentOutOfOrder { current: u64, next: u64 },

    #[error("WAL sync failed: {0}")]
    WalSyncFailed(String),

    #[error("invalid WAL: {0}")]
    InvalidWal(String),

    #[error("corrupt WAL at {path}: {reason}")]
    CorruptWal { path: PathBuf, reason: String },

    #[error(
        "WAL append rollback failed for {path} at offset {offset} after write error: {write_error}; rollback error: {rollback_error}"
    )]
    WalAppendRollbackFailed {
        path: PathBuf,
        offset: u64,
        write_error: io::Error,
        #[source]
        rollback_error: io::Error,
    },

    #[error("WAL lsns must increase: previous {previous:?}, next {next:?}")]
    WalLsnOutOfOrder {
        previous: crate::StrataLsn,
        next: crate::StrataLsn,
    },

    #[error(
        "memtable generation {generation} is full: capacity {capacity} bytes, used {used} bytes, entry requires {required} bytes"
    )]
    MemtableFull {
        generation: u64,
        capacity: usize,
        used: usize,
        required: usize,
    },

    #[error(
        "memtable entry requires {required} bytes, exceeding generation capacity {capacity} bytes"
    )]
    MemtableEntryTooLarge { capacity: usize, required: usize },

    #[error("memtable lsns must increase: previous {previous:?}, next {next:?}")]
    MemtableLsnOutOfOrder {
        previous: crate::StrataLsn,
        next: crate::StrataLsn,
    },

    #[error("memtable generation must increase: current {current}, next {next}")]
    MemtableGenerationOutOfOrder { current: u64, next: u64 },

    #[error("memtable generation {generation} cannot be incremented")]
    MemtableGenerationOverflow { generation: u64 },

    #[error("replacement memtable generation {generation} is not empty")]
    MemtableReplacementNotEmpty { generation: u64 },

    #[error(
        "memtable cursor belongs to generation {cursor_generation}, not generation {generation}"
    )]
    MemtableCursorGeneration {
        generation: u64,
        cursor_generation: u64,
    },
}
