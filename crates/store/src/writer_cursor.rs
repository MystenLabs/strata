//! The writer cursor: how far the foreground writer has got, kept out of the index's write queue.
//!
//! Every commit group used to end with an index batch holding two small rows: `NextLsn` and the
//! active segment's state (its write offset and LSN bounds). The index admits one write group at
//! a time, so that batch waited behind whatever a background thread was writing: a collector
//! publication, a WAL-reclaim checkpoint, a compaction publication. Those batches take
//! milliseconds, and every put that arrived meanwhile queued behind the stalled writer.
//!
//! A group that changes nothing else now records the same two facts in a fixed-size file with one
//! positional write, and the writer keeps the authoritative values in memory. The index rows are
//! still written, with the rest of the batch, by every group that needs the index anyway (a
//! segment rollover, an epoch change, a caller-supplied LSN key), by a shard drop, and by every
//! durability publication, so `NextLsn` never falls behind `CommittedLsn` on disk.
//!
//! Recovery is unchanged. It still reads `NextLsn` and the segment row from the index; open copies
//! the cursor into those rows first when the cursor is the newer of the two. The record names the
//! `NextLsn` row it was written on top of, and open applies it only if the index still holds that
//! value:
//!
//! - After a process crash the index has lost nothing, the base matches, and the rows end up
//!   exactly where a per-put index commit would have left them. No acknowledged write is lost.
//! - After a power loss the index may have lost its last batches while the cursor file kept a
//!   later record, or the reverse. A record whose base is not what the index holds describes a
//!   history the index no longer has, so it is ignored and recovery proceeds from the index rows
//!   alone, as it always has after a power loss.
//!
//! The file is never synced. Whatever reaches disk is a whole record (it is smaller than a
//! sector and carries a checksum), and a missing or damaged file is the same as no cursor.

use std::{
    fs::{File, OpenOptions},
    io::ErrorKind,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use core_types::{PlacementClass, SegmentFileState, SegmentId, SegmentState, StrataLsn};
use index::StrataIndex;
use xxhash_rust::xxh3::xxh3_64;

use crate::{Error, Result, StrataStoreConfig};

const MAGIC: [u8; 8] = *b"STRCUR01";
const RECORD_LEN: usize = 72;
const CHECKSUM_OFFSET: usize = RECORD_LEN - 8;
const HAS_MIN_LSN: u64 = 1;
const HAS_MAX_LSN: u64 = 2;

pub(crate) fn writer_cursor_path(config: &StrataStoreConfig) -> PathBuf {
    config.namespace_dir().join("writer.cursor")
}

/// One cursor record: the writer's frontier after a commit group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriterCursorRecord {
    /// The `NextLsn` row the index held when this record was written.
    pub(crate) index_next_lsn: StrataLsn,
    /// The first LSN the writer has not assigned.
    pub(crate) next_lsn: StrataLsn,
    pub(crate) segment_id: SegmentId,
    pub(crate) write_offset: u64,
    pub(crate) min_lsn: Option<StrataLsn>,
    pub(crate) max_lsn: Option<StrataLsn>,
}

impl WriterCursorRecord {
    pub(crate) fn new(
        index_next_lsn: StrataLsn,
        next_lsn: StrataLsn,
        active_segment: &SegmentState,
    ) -> Self {
        Self {
            index_next_lsn,
            next_lsn,
            segment_id: active_segment.segment_id,
            write_offset: active_segment.write_offset,
            min_lsn: active_segment.min_lsn,
            max_lsn: active_segment.max_lsn,
        }
    }

    fn encode(&self) -> [u8; RECORD_LEN] {
        let mut flags = 0_u64;
        if self.min_lsn.is_some() {
            flags |= HAS_MIN_LSN;
        }
        if self.max_lsn.is_some() {
            flags |= HAS_MAX_LSN;
        }
        let mut bytes = [0_u8; RECORD_LEN];
        bytes[..8].copy_from_slice(&MAGIC);
        let fields = [
            self.index_next_lsn,
            self.next_lsn,
            self.segment_id,
            self.write_offset,
            flags,
            self.min_lsn.unwrap_or_default(),
            self.max_lsn.unwrap_or_default(),
        ];
        for (slot, field) in fields.iter().enumerate() {
            let start = 8 + slot * 8;
            bytes[start..start + 8].copy_from_slice(&field.to_le_bytes());
        }
        let checksum = xxh3_64(&bytes[..CHECKSUM_OFFSET]);
        bytes[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Returns `None` for anything that is not one intact record.
    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != RECORD_LEN || bytes[..8] != MAGIC {
            return None;
        }
        let field = |slot: usize| {
            let start = 8 + slot * 8;
            u64::from_le_bytes(bytes[start..start + 8].try_into().expect("eight bytes"))
        };
        if field(7) != xxh3_64(&bytes[..CHECKSUM_OFFSET]) {
            return None;
        }
        let flags = field(4);
        Some(Self {
            index_next_lsn: field(0),
            next_lsn: field(1),
            segment_id: field(2),
            write_offset: field(3),
            min_lsn: (flags & HAS_MIN_LSN != 0).then(|| field(5)),
            max_lsn: (flags & HAS_MAX_LSN != 0).then(|| field(6)),
        })
    }
}

/// The open cursor file. Only the writer thread writes it.
pub(crate) struct WriterCursor {
    file: File,
    path: PathBuf,
}

impl WriterCursor {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
        Ok(Self { file, path })
    }

    /// Replaces the record. One positional write into the page cache, no sync.
    pub(crate) fn write(&self, record: &WriterCursorRecord) -> Result<()> {
        self.file
            .write_all_at(&record.encode(), 0)
            .map_err(|source| Error::Io {
                path: self.path.clone(),
                source,
            })
    }

    /// Reads the record at `path`. A missing, short or damaged file is `None`.
    pub(crate) fn read(path: &Path) -> Result<Option<WriterCursorRecord>> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(WriterCursorRecord::decode(&bytes)),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::Io {
                path: path.to_path_buf(),
                source,
            }),
        }
    }
}

/// Copies the cursor into the index rows recovery reads, when the cursor is ahead of them and
/// was written on top of the `NextLsn` row the index still holds. Returns whether it applied.
///
/// Runs at open, before unsealed-segment and store-WAL recovery, which then treat the rows
/// exactly as if the writer had committed them through the index.
pub(crate) fn apply_writer_cursor(config: &StrataStoreConfig, index: &StrataIndex) -> Result<bool> {
    let Some(record) = WriterCursor::read(&writer_cursor_path(config))? else {
        return Ok(false);
    };
    let index_next_lsn = index.get_next_lsn()?;
    if record.index_next_lsn != index_next_lsn || record.next_lsn <= index_next_lsn {
        return Ok(false);
    }
    // The base matches, so the index holds the writer's last index batch and with it the row of
    // the segment that was active when the record was written. Anything else means the record
    // and the index disagree about history; leave the index alone.
    let Some(mut state) = index.get_segment_state(record.segment_id)? else {
        return Ok(false);
    };
    if state.state != SegmentFileState::Open
        || state.placement_class != PlacementClass::Ingest
        || record.write_offset < state.write_offset
    {
        return Ok(false);
    }
    state.write_offset = record.write_offset;
    state.min_lsn = record.min_lsn;
    state.max_lsn = record.max_lsn;
    let mut batch = index.batch();
    index.put_segment_state_batch(&mut batch, &state)?;
    index.put_next_lsn_batch(&mut batch, record.next_lsn)?;
    batch.write()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{RECORD_LEN, WriterCursor, WriterCursorRecord};

    fn record() -> WriterCursorRecord {
        WriterCursorRecord {
            index_next_lsn: 40,
            next_lsn: 57,
            segment_id: 9,
            write_offset: 1 << 20,
            min_lsn: Some(12),
            max_lsn: Some(56),
        }
    }

    #[test]
    fn a_record_round_trips_and_a_later_write_replaces_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("writer.cursor");
        assert_eq!(WriterCursor::read(&path).unwrap(), None);

        let cursor = WriterCursor::open(path.clone()).unwrap();
        assert_eq!(WriterCursor::read(&path).unwrap(), None);
        cursor.write(&record()).unwrap();
        assert_eq!(WriterCursor::read(&path).unwrap(), Some(record()));

        let empty_segment = WriterCursorRecord {
            index_next_lsn: 57,
            next_lsn: 57,
            segment_id: 10,
            write_offset: 0,
            min_lsn: None,
            max_lsn: None,
        };
        cursor.write(&empty_segment).unwrap();
        assert_eq!(WriterCursor::read(&path).unwrap(), Some(empty_segment));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), RECORD_LEN as u64);
    }

    #[test]
    fn a_damaged_or_truncated_record_reads_as_absent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("writer.cursor");
        let cursor = WriterCursor::open(path.clone()).unwrap();
        cursor.write(&record()).unwrap();
        let intact = std::fs::read(&path).unwrap();

        for byte in [0, 8, 20, 47, RECORD_LEN - 1] {
            let mut damaged = intact.clone();
            damaged[byte] ^= 0x40;
            std::fs::write(&path, &damaged).unwrap();
            assert_eq!(WriterCursor::read(&path).unwrap(), None, "byte {byte}");
        }
        std::fs::write(&path, &intact[..RECORD_LEN - 1]).unwrap();
        assert_eq!(WriterCursor::read(&path).unwrap(), None);
        std::fs::write(&path, &intact).unwrap();
        assert_eq!(WriterCursor::read(&path).unwrap(), Some(record()));
    }
}
