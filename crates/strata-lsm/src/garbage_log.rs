use std::{
    cmp::Ordering,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use strata_core::{
    GarbageEvent, SegmentGcLifetimeUpdate, SegmentGcOverlay, SegmentGcOverlayMergeOp,
    SegmentGcRecordRange, SegmentGcSummary, SegmentGcSummaryDelta, SegmentKey,
};

use crate::{
    Error, Result, StrataLsn,
    recovery::{
        MAGIC, VERSION, create_log, read_header, recover, remove_logs_before, sync_dir,
        validate_position,
    },
};

pub(super) const HEADER_LEN: u64 = 12;
pub(super) const FILE_PREFIX: &str = "garbage-";
pub(super) const FILE_SUFFIX: &str = ".glog";

/// End of the garbage-log prefix made visible by a RocksDB publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GarbageLogPosition {
    /// Zero means no garbage-log frame has been committed yet.
    pub log_id: u64,
    /// Exclusive byte offset of the last committed frame in `log_id`.
    pub offset: u64,
}

/// One compaction-produced garbage record.
///
/// The exact summary delta lets the sweeper update RocksDB without reconstructing prior record
/// state. GC ignores it when folding the segment-local log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GarbageRecord {
    pub key: SegmentKey,
    pub lsn: StrataLsn,
    pub event: GarbageEvent,
    pub summary_delta: SegmentGcSummaryDelta,
}

impl GarbageRecord {
    pub(crate) fn cmp_position(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.lsn.cmp(&other.lsn))
            .then_with(|| self.event.record().offset.cmp(&other.event.record().offset))
            .then_with(|| self.event.record().len.cmp(&other.event.record().len))
    }
}

/// Rolling append-only log for compaction-produced garbage records.
///
/// Each append is one checksummed frame. `append` syncs the frame before returning its end
/// position; the caller can then publish that position beside its manifest edit in RocksDB.
/// Appends and publication must be serialized so an unpublished frame can never sit before a
/// published one.
#[derive(Debug)]
pub struct GarbageLog {
    dir: PathBuf,
    max_file_bytes: u64,
    log_id: u64,
    offset: u64,
    file: File,
}

impl GarbageLog {
    /// Opens the committed prefix and removes any later, unpublished bytes or log files.
    ///
    /// `max_file_bytes` is a soft limit: one batch is never split across files, so a single large
    /// frame may exceed it.
    pub fn open(
        dir: impl AsRef<Path>,
        max_file_bytes: u64,
        committed: GarbageLogPosition,
    ) -> Result<Self> {
        if max_file_bytes == 0 {
            return Err(Error::InvalidGarbageLog(
                "maximum file size must be non-zero".to_owned(),
            ));
        }
        let dir = dir.as_ref().to_path_buf();
        let (file, log_id, offset) = recover(&dir, committed)?;

        Ok(Self {
            dir,
            max_file_bytes,
            log_id,
            offset,
            file,
        })
    }

    pub fn path(dir: impl AsRef<Path>, log_id: u64) -> PathBuf {
        dir.as_ref()
            .join(format!("{FILE_PREFIX}{log_id:020}{FILE_SUFFIX}"))
    }

    /// Deletes rolled files strictly older than a durably published consumer position.
    pub fn reclaim_before(dir: impl AsRef<Path>, retained: GarbageLogPosition) -> Result<()> {
        validate_position(retained)?;
        if retained != GarbageLogPosition::default() {
            remove_logs_before(dir.as_ref(), retained.log_id)?;
        }
        Ok(())
    }

    pub fn position(&self) -> GarbageLogPosition {
        GarbageLogPosition {
            log_id: self.log_id,
            offset: self.offset,
        }
    }

    /// Whether the writer has no bytes after the position committed in RocksDB.
    pub fn is_at_committed_position(&self, committed: GarbageLogPosition) -> bool {
        if committed == GarbageLogPosition::default() {
            self.log_id == 1 && self.offset == HEADER_LEN
        } else {
            self.position() == committed
        }
    }

    /// Reads the next complete frame after `cursor`, stopping at the committed `through` position.
    pub fn read_next(
        dir: impl AsRef<Path>,
        cursor: GarbageLogPosition,
        through: GarbageLogPosition,
    ) -> Result<Option<(Vec<GarbageRecord>, GarbageLogPosition)>> {
        if through == GarbageLogPosition::default() {
            return Ok(None);
        }
        validate_position(cursor)?;
        validate_position(through)?;
        if cursor == through {
            return Ok(None);
        }
        let dir = dir.as_ref();
        let mut log_id = if cursor == GarbageLogPosition::default() {
            1
        } else {
            cursor.log_id
        };
        let mut offset = if cursor == GarbageLogPosition::default() {
            HEADER_LEN
        } else {
            cursor.offset
        };

        loop {
            if log_id > through.log_id || (log_id == through.log_id && offset >= through.offset) {
                return Err(Error::InvalidGarbageLog(
                    "sweep cursor is beyond the committed garbage-log position".to_owned(),
                ));
            }
            let path = Self::path(dir, log_id);
            let file_len = File::open(&path)
                .and_then(|file| file.metadata())
                .map_err(|source| io_error(&path, source))?
                .len();
            if offset == file_len && log_id < through.log_id {
                log_id += 1;
                offset = HEADER_LEN;
                continue;
            }
            let (records, end) = read_records_at(&path, offset)?;
            if log_id > through.log_id || (log_id == through.log_id && end > through.offset) {
                return Err(Error::CorruptGarbageLog {
                    path,
                    reason: "frame extends beyond the committed position".to_owned(),
                });
            }
            return Ok(Some((
                records,
                GarbageLogPosition {
                    log_id,
                    offset: end,
                },
            )));
        }
    }

    /// Appends and syncs one sorted compaction batch as a single frame.
    pub fn append(&mut self, records: &[GarbageRecord]) -> Result<GarbageLogPosition> {
        if records
            .iter()
            .any(|record| record.key.segment_id != record.event.record().segment_id)
        {
            return Err(Error::InvalidGarbageLog(
                "garbage key and record reference name different segments".to_owned(),
            ));
        }
        if records
            .windows(2)
            .any(|pair| pair[0].cmp_position(&pair[1]).is_ge())
        {
            return Err(Error::InvalidGarbageLog(
                "garbage records must be strictly ordered by key, lsn, and record".to_owned(),
            ));
        }
        let records = encode_records(records)?;
        let frame_len = encoded_frame_len(&records)?;
        if self.offset > HEADER_LEN
            && self
                .offset
                .checked_add(frame_len)
                .is_none_or(|end| end > self.max_file_bytes)
        {
            self.roll()?;
        }

        write_frame(
            &mut self.file,
            &records,
            &Self::path(&self.dir, self.log_id),
        )?;
        self.file
            .sync_data()
            .map_err(|source| io_error(&Self::path(&self.dir, self.log_id), source))?;
        self.offset = self
            .offset
            .checked_add(frame_len)
            .ok_or_else(|| Error::InvalidGarbageLog("garbage log offset overflow".to_owned()))?;
        Ok(self.position())
    }

    fn roll(&mut self) -> Result<()> {
        let next_id = self
            .log_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidGarbageLog("garbage log id overflow".to_owned()))?;
        let (file, offset) = create_log(&self.dir, next_id)?;
        self.file = file;
        self.log_id = next_id;
        self.offset = offset;
        Ok(())
    }
}

/// One append-only `<segment>.glog` file.
pub struct SegmentGarbageLog {
    path: PathBuf,
    file: File,
    offset: u64,
}

impl SegmentGarbageLog {
    /// Opens the committed prefix and removes any later unpublished frames.
    pub fn open(path: impl AsRef<Path>, committed: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = path.parent().ok_or_else(|| {
            Error::InvalidGarbageLog("segment garbage log has no parent directory".to_owned())
        })?;
        fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;

        if committed == 0 {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|source| io_error(&path, source))?;
            file.write_all(MAGIC)
                .and_then(|_| file.write_all(&VERSION.to_le_bytes()))
                .map_err(|source| io_error(&path, source))?;
            file.sync_all().map_err(|source| io_error(&path, source))?;
            sync_dir(parent)?;
            return Ok(Self {
                path,
                file,
                offset: HEADER_LEN,
            });
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        read_header(&mut file, &path)?;
        let mut offset = HEADER_LEN;
        while offset < committed {
            let (_, end) = read_records_at(&path, offset)?;
            offset = end;
        }
        if offset != committed {
            return Err(Error::CorruptGarbageLog {
                path,
                reason: format!("committed offset {committed} is not a frame boundary"),
            });
        }
        if file
            .metadata()
            .map_err(|source| io_error(&path, source))?
            .len()
            != committed
        {
            file.set_len(committed)
                .map_err(|source| io_error(&path, source))?;
            file.sync_data().map_err(|source| io_error(&path, source))?;
        }
        file.seek(SeekFrom::Start(committed))
            .map_err(|source| io_error(&path, source))?;
        Ok(Self {
            path,
            file,
            offset: committed,
        })
    }

    /// Appends one garbage-record batch and returns its exclusive end offset.
    pub fn append(&mut self, records: &[GarbageRecord]) -> Result<u64> {
        let records = encode_records(records)?;
        let frame_len = encoded_frame_len(&records)?;
        write_frame(&mut self.file, &records, &self.path)?;
        self.file
            .sync_data()
            .map_err(|source| io_error(&self.path, source))?;
        self.offset = self.offset.checked_add(frame_len).ok_or_else(|| {
            Error::InvalidGarbageLog("segment garbage offset overflow".to_owned())
        })?;
        Ok(self.offset)
    }
}

/// Reads only the prefix made visible by the stored committed offset.
pub fn read_segment_garbage(path: impl AsRef<Path>, committed: u64) -> Result<Vec<GarbageRecord>> {
    if committed == 0 {
        return Ok(Vec::new());
    }
    let path = path.as_ref();
    let mut records = Vec::new();
    let mut offset = HEADER_LEN;
    while offset < committed {
        let (frame, end) = read_records_at(path, offset)?;
        if end > committed {
            return Err(Error::CorruptGarbageLog {
                path: path.to_path_buf(),
                reason: "frame extends beyond the committed segment garbage position".to_owned(),
            });
        }
        records.extend(frame);
        offset = end;
    }
    if offset != committed {
        return Err(Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: format!("committed offset {committed} is not a frame boundary"),
        });
    }
    Ok(records)
}

/// Folds one segment's garbage history into the shape consumed by the current GC scanner.
///
/// Segment bytes are copy-eligible unless an event says otherwise. `summary` is the authoritative
/// value read from RocksDB alongside the committed local-log position.
pub fn fold_segment_garbage(
    mut records: Vec<GarbageRecord>,
    summary: SegmentGcSummary,
) -> Result<SegmentGcOverlay> {
    records.sort_unstable_by(GarbageRecord::cmp_position);
    if records
        .iter()
        .any(|record| record.key.segment_id != record.event.record().segment_id)
    {
        return Err(Error::InvalidGarbageLog(
            "garbage key and record reference name different segments".to_owned(),
        ));
    }
    if records
        .windows(2)
        .any(|pair| pair[0].key.segment_id != pair[1].key.segment_id)
    {
        return Err(Error::InvalidGarbageLog(
            "segment garbage log contains records for multiple segments".to_owned(),
        ));
    }
    if records
        .windows(2)
        .any(|pair| pair[0].cmp_position(&pair[1]).is_eq())
    {
        return Err(Error::InvalidGarbageLog(
            "segment garbage log contains a duplicate key, lsn, and record".to_owned(),
        ));
    }

    let mut overlay = SegmentGcOverlay::default();
    overlay.apply_merge_ops(records.into_iter().map(|record| {
        let range = SegmentGcRecordRange::from(record.event.record());
        match record.event {
            GarbageEvent::Retired { .. } => SegmentGcOverlayMergeOp::RetireBatch {
                ranges: vec![range],
            },
            GarbageEvent::Expired { .. } => SegmentGcOverlayMergeOp::ExpireBatch {
                ranges: vec![range],
            },
            GarbageEvent::SetLifecycle { lifecycle, .. } => {
                SegmentGcOverlayMergeOp::LifetimeBatch {
                    updates: vec![SegmentGcLifetimeUpdate { range, lifecycle }],
                }
            }
        }
    }));
    overlay.summary = summary;
    Ok(overlay)
}

pub(super) fn encode_records(records: &[GarbageRecord]) -> Result<Vec<Vec<u8>>> {
    records
        .iter()
        .map(bcs::to_bytes)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Serialization(error.to_string()))
}

pub(super) fn encoded_frame_len(records: &[Vec<u8>]) -> Result<u64> {
    if records.is_empty() {
        return Err(Error::InvalidGarbageLog(
            "cannot append an empty garbage batch".to_owned(),
        ));
    }
    u32::try_from(records.len())
        .map_err(|_| Error::InvalidGarbageLog("garbage batch has too many records".to_owned()))?;

    let payload_len = records.iter().try_fold(0u64, |total, record| {
        u32::try_from(record.len())
            .map_err(|_| Error::InvalidGarbageLog("one garbage record is too large".to_owned()))?;
        total
            .checked_add(4)
            .and_then(|total| total.checked_add(record.len() as u64))
            .ok_or_else(|| Error::InvalidGarbageLog("garbage batch is too large".to_owned()))
    })?;
    12u64
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(32))
        .ok_or_else(|| Error::InvalidGarbageLog("garbage frame is too large".to_owned()))
}

pub(super) fn write_frame(file: &mut File, records: &[Vec<u8>], path: &Path) -> Result<()> {
    let record_count = records.len() as u32;
    let payload_len = records
        .iter()
        .map(|record| 4 + record.len() as u64)
        .sum::<u64>();
    let prefix = [
        payload_len.to_le_bytes().as_slice(),
        &record_count.to_le_bytes(),
    ]
    .concat();
    let mut checksum = Sha256::new();
    checksum.update(&prefix);
    file.write_all(&prefix)
        .map_err(|source| io_error(path, source))?;

    for record in records {
        let len = (record.len() as u32).to_le_bytes();
        checksum.update(len);
        checksum.update(record);
        file.write_all(&len)
            .and_then(|_| file.write_all(record))
            .map_err(|source| io_error(path, source))?;
    }
    file.write_all(&checksum.finalize())
        .map_err(|source| io_error(path, source))
}

pub(super) fn read_records_at(path: &Path, offset: u64) -> Result<(Vec<GarbageRecord>, u64)> {
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let file_len = file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len();
    read_header(&mut file, path)?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| io_error(path, source))?;

    let mut prefix = [0; 12];
    file.read_exact(&mut prefix)
        .map_err(|source| io_error(path, source))?;
    let payload_len = u64::from_le_bytes(prefix[..8].try_into().unwrap());
    let record_count = u32::from_le_bytes(prefix[8..].try_into().unwrap());
    let end = offset
        .checked_add(12)
        .and_then(|end| end.checked_add(payload_len))
        .and_then(|end| end.checked_add(32))
        .ok_or_else(|| Error::InvalidGarbageLog("garbage log offset overflow".to_owned()))?;
    if end > file_len || u64::from(record_count).saturating_mul(4) > payload_len {
        return Err(Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: "invalid frame length".to_owned(),
        });
    }
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| Error::InvalidGarbageLog("frame is too large".to_owned()))?;
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)
        .map_err(|source| io_error(path, source))?;
    let mut checksum = [0; 32];
    file.read_exact(&mut checksum)
        .map_err(|source| io_error(path, source))?;
    let actual = Sha256::new()
        .chain_update(prefix)
        .chain_update(&payload)
        .finalize();
    if checksum != actual.as_slice() {
        return Err(Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: "frame checksum mismatch".to_owned(),
        });
    }

    let mut records = Vec::with_capacity(record_count as usize);
    let mut remaining = payload.as_slice();
    for _ in 0..record_count {
        let len = remaining.get(..4).ok_or_else(|| Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: "truncated record length".to_owned(),
        })?;
        let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
        remaining = &remaining[4..];
        let bytes = remaining
            .get(..len)
            .ok_or_else(|| Error::CorruptGarbageLog {
                path: path.to_path_buf(),
                reason: "truncated record".to_owned(),
            })?;
        records.push(bcs::from_bytes::<GarbageRecord>(bytes).map_err(|error| {
            Error::CorruptGarbageLog {
                path: path.to_path_buf(),
                reason: format!("invalid garbage record: {error}"),
            }
        })?);
        remaining = &remaining[len..];
    }
    if !remaining.is_empty() {
        return Err(Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: "frame has trailing payload bytes".to_owned(),
        });
    }
    Ok((records, end))
}

pub(super) fn io_error(path: &Path, source: io::Error) -> Error {
    Error::Io {
        path: path.to_path_buf(),
        source,
    }
}
