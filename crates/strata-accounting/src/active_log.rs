//! Segment aligned foreground accounting log.
//!
//! The public delta and cursor model stays here. Append and sync operations live in `writer`, durable
//! range traversal in `reader`, and crash-prefix/file-format handling in `recovery`.

use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use strata_core::{BlobKey, RecordRef, SegmentId, ShardKey, StrataLsn};

use crate::run_io::{read_record_frame, write_record_frame};
use crate::{ACTIVE_DELTA_LOG_FORMAT_VERSION, BlobUpdate, EpochChange, Error, Result};

const ACTIVE_DELTA_LOG_MAGIC: &[u8; 8] = b"STRADL01";
const ACTIVE_DELTA_LOG_FILE_PREFIX: &str = "active-delta-";
const ACTIVE_DELTA_LOG_FILE_SUFFIX: &str = ".log";

/// Durable foreground accounting entry appended in store-global LSN order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountingLogEntry {
    /// One foreground blob mutation or metadata mutation with an explicit LSN.
    Blob(BlobUpdate),
    /// Store-global epoch transition with an explicit LSN.
    Epoch(EpochChange),
    /// One physical active-log frame containing many GC map refs.
    ///
    /// The frame occupies one append in `active-delta.log`, but each map still owns a logical LSN:
    /// `base_lsn + index`. Keeping per-map logical LSNs avoids redefining the rest of accounting
    /// around "many keys at one LSN" while avoiding one filesystem frame per copied GC record.
    GcMapRefBatch {
        /// Logical LSN assigned to `maps[0]`.
        base_lsn: StrataLsn,
        /// Ordered map refs whose logical LSN is derived from position in this vector.
        maps: Vec<GcMapRefEntry>,
    },
    /// Durable logical fence for one dropped shard generation.
    ShardDropped { lsn: StrataLsn, shard: ShardKey },
}

/// One logical `MapRef` inside a bulk GC active log frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcMapRefEntry {
    /// Blob key whose payload version is being remapped by GC.
    pub key: BlobKey,
    /// Source physical record copied out of a GC candidate segment.
    pub from: RecordRef,
    /// Destination physical record in the newly published GC output segment.
    pub to: RecordRef,
}

impl AccountingLogEntry {
    pub fn lsn(&self) -> StrataLsn {
        match self {
            Self::Blob(update) => update.lsn(),
            Self::Epoch(change) => change.lsn,
            Self::GcMapRefBatch { base_lsn, maps } => maps
                .len()
                .checked_sub(1)
                .and_then(|last| base_lsn.checked_add(last as u64))
                .unwrap_or(*base_lsn),
            Self::ShardDropped { lsn, .. } => *lsn,
        }
    }
}

/// Crash-safe position in the active accounting log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccountingLogDurablePosition {
    pub segment_id: SegmentId,
    pub durable_offset: u64,
    pub durable_lsn: StrataLsn,
}

impl AccountingLogDurablePosition {
    pub fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(bytes)?)
    }
}

/// Durable cursor for accounting-index ingestion from the active accounting log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActiveDeltaLogReadCursor {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub max_lsn: StrataLsn,
}

impl ActiveDeltaLogReadCursor {
    pub fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(bytes)?)
    }
}

/// Entries read from a durable active-log range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveDeltaLogRead {
    pub entries: Vec<AccountingLogEntry>,
    /// Encoded active-log frame bytes consumed to produce `entries`.
    pub bytes_read: u64,
    pub end_segment_id: SegmentId,
    pub end_offset: u64,
    pub max_lsn: Option<StrataLsn>,
}

impl ActiveDeltaLogRead {
    pub fn next_cursor(&self, previous: ActiveDeltaLogReadCursor) -> ActiveDeltaLogReadCursor {
        ActiveDeltaLogReadCursor {
            segment_id: self.end_segment_id,
            offset: self.end_offset,
            max_lsn: self
                .max_lsn
                .map_or(previous.max_lsn, |max_lsn| previous.max_lsn.max(max_lsn)),
        }
    }
}

/// Append-only accounting delta log for the active writer.
#[derive(Debug)]
pub struct ActiveDeltaLog {
    segment_id: SegmentId,
    path: PathBuf,
    writer: BufWriter<fs::File>,
    write_offset: u64,
    max_lsn: Option<StrataLsn>,
    durable_offset: u64,
    durable_lsn: StrataLsn,
}

/// Rewind point used when the foreground RocksDB commit fails after delta-log append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveDeltaLogPosition {
    segment_id: SegmentId,
    write_offset: u64,
    max_lsn: Option<StrataLsn>,
    durable_offset: u64,
    durable_lsn: StrataLsn,
}

mod reader;
mod recovery;
mod writer;

use reader::read_durable_range;
use recovery::*;
