//! The two garbage logs: the global rolling log that garbage events are born into, and the
//! per-segment sidecar files the sweeper re files them into.
//!
//! Every fact GC ever learns about dead bytes travels through this file. Blob LSM compaction and
//! GC publication append events (Retired, Expired, SetLifecycle) to one *global* append-only log —
//! in the running example from the store crate, the frame retiring A@S7 and D@S7 lands here at
//! publication time. The sweeper then reads the global log frame by frame (read_next) and appends
//! each event into the *segment-local* log sitting beside the segment file it describes
//! (S7's `.glog`). Folding a segment's local log plus its authoritative RocksDB summary produces
//! the SegmentGcOverlay (fold_segment_garbage) — the view the GC planner ranks segments by and
//! the publish-time revalidation classifies copies against. So the pipeline is: event born in the
//! global log → swept into the segment log → folded into an overlay → acted on by GC.
//!
//! Neither log trusts its own file length. Both follow the same visibility rule: `append` writes
//! one checksummed frame and fsyncs it, then returns the frame's end position — but the frame is
//! not *committed* until the caller publishes that position in RocksDB (beside a manifest edit,
//! a sweep cursor, or a GC activation batch). On open, everything past the committed position is
//! presumed to be a torn or unpublished tail and is truncated away. This is what makes
//! "append first, publish the position second" safe for every writer, and it is why appends and
//! position publication must be serialized by the caller: if an unpublished frame could sit in
//! front of a published one, truncating the unpublished tail would eat committed data.
//!
//! The physical format is shared by both logs. A file starts with a 12-byte header (8-byte magic
//! `STRGL002`, 4-byte LE version). Each frame is: a 12-byte prefix (u64 LE payload length,
//! u32 LE record count), then per record a 4-byte LE length and an explicitly encoded record,
//! then a 32-byte SHA-256 over the prefix and payload. Positions therefore always name exact
//! frame boundaries; offsets below the header length are meaningless, and the all-zero position
//! means "nothing has ever been committed".

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use core_types::{
    BlobKey, BlobLifecycle, GarbageEvent, RecordRef, SegmentGcLifetimeUpdate, SegmentGcOverlay,
    SegmentGcOverlayMergeOp, SegmentGcRecordRange, SegmentGcSummary, SegmentGcSummaryDelta,
    SegmentKey,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    #[serde(rename = "l")]
    pub log_id: u64,
    /// Exclusive byte offset of the last committed frame in `log_id`.
    #[serde(rename = "o")]
    pub offset: u64,
}

/// One compaction-produced garbage record.
///
/// The exact summary delta lets the sweeper update RocksDB without reconstructing prior record
/// state. GC ignores it when folding the segment-local log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GarbageRecord {
    pub key: SegmentKey,
    pub lsn: StrataLsn,
    pub event: GarbageEvent,
    pub summary_delta: SegmentGcSummaryDelta,
}

impl GarbageRecord {
    /// The canonical total order on garbage records: key, then LSN, then the physical range.
    ///
    /// One comparator serves three duties: the global log's append validation (batches must be
    /// strictly ascending), the fold's sort, and the fold's conflict detection — two *different*
    /// events comparing equal here would be two contradictory claims about the same bytes at the
    /// same logical moment.
    pub fn cmp_position(&self, other: &Self) -> Ordering {
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
///
/// "Rolling" means the log is a numbered chain of files (`garbage-<id>.glog`, ids from 1): when
/// the current file passes `max_file_bytes` a fresh file continues the chain, and files the
/// sweeper has fully consumed are reclaimed from the front. Two producers write here — blob-LSM
/// compaction and GC relocation publication — one consumer reads: the sweeper.
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
    /// `committed` is the position the caller read back from RocksDB, and it is the only thing
    /// trusted. The default position means nothing was ever committed: every existing log file is
    /// deleted and a fresh file 1 is created. Otherwise recovery scans the committed file frame
    /// by frame, verifying each checksum, to prove the committed offset really is the end of a
    /// valid frame — a committed position pointing past the valid prefix, or into the middle of
    /// a frame, is corruption, not something to silently round down. The file is then truncated
    /// to exactly the committed offset (dropping any torn or unpublished tail) and every file
    /// with a *later* id is removed. Files with earlier ids are left alone — they may still hold
    /// frames the sweeper has not consumed, and reclaim_before owns their deletion.
    ///
    /// This truncate-on-open is the other half of the append contract: a writer may append and
    /// sync a frame, then fail before its position commits. That frame is not lost data, it is a
    /// publication that never happened, and the next open makes the file agree.
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
    ///
    /// The sweeper calls this with its durable sweep cursor: once the cursor has moved into file
    /// N, files 1..N-1 contain only frames that were folded into segment-local logs and committed
    /// long ago, so they are unlinked wholesale. Only whole files are ever reclaimed — the file
    /// the cursor sits in stays, however far through it the cursor is — and the default position
    /// reclaims nothing, because a consumer that never committed has consumed nothing.
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
    ///
    /// This is the sweeper's read primitive: `cursor` is how far it has durably folded, `through`
    /// is the committed head, and each call hands back exactly one frame plus the position to
    /// commit after folding it. A default `through` (nothing ever committed) and a caught-up
    /// cursor both return None. A default cursor starts reading at file 1 just past the header.
    /// When the cursor sits at the end of a fully consumed file and committed frames exist in
    /// later files, the read hops to the next file's first frame.
    ///
    /// The two error shapes are deliberate: a cursor *beyond* `through` means the two RocksDB
    /// rows disagree (the consumer claims to have folded frames that were never committed), and a
    /// frame that starts inside the committed range but *ends* past it means the committed
    /// position itself does not sit on a frame boundary — both are metadata corruption, never
    /// something to read through.
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
    ///
    /// Two validations run before any byte is written. Every record's key and its event must name
    /// the same segment — the key is how the sweeper routes the event to a segment-local file,
    /// and a mismatch would file a death certificate under the wrong segment. And the batch must
    /// be *strictly* ascending in the canonical order (cmp_position) — equal neighbors are
    /// rejected too, since two events at one position are either a duplicate or a contradiction.
    ///
    /// Then the roll decision: if the frame would push a non-empty file past `max_file_bytes`,
    /// a new file continues the chain first. A frame is never split across files, and an empty
    /// file always accepts the frame whole — that pairing is why the size limit is soft and why
    /// every frame can be read without reassembly. The frame is written and fsynced before the
    /// new position is returned; making that position *mean* something is the caller's RocksDB
    /// commit, per the contract on the type.
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
///
/// The sidecar living next to a segment's data file, holding that segment's complete garbage
/// history in the same frame format as the global log. Two writers append here: the sweeper,
/// re-filing global frames segment by segment, and GC publication, writing an output segment's
/// born-dead ranges (in the running example, B's and C's bytes inside S42). The committed offset
/// is a per-segment row in RocksDB, published by whoever appended — same trust model as the
/// global log: the file's own length proves nothing.
pub struct SegmentGarbageLog {
    path: PathBuf,
    file: File,
    offset: u64,
}

impl SegmentGarbageLog {
    /// Opens the committed prefix and removes any later unpublished frames.
    ///
    /// `committed == 0` does not mean "open whatever is there" — it means nothing was ever
    /// published for this segment, so any existing file content is by definition an unpublished
    /// leftover and the file is created or truncated fresh (header only, synced along with its
    /// directory). This is exactly why GC publication can open a brand-new output's log with
    /// committed 0 and retry after a crash without seeing its own abandoned bytes.
    ///
    /// A non-zero `committed` is re-proven, not assumed: the frames are walked from the header
    /// and must land *exactly* on the committed offset — checksums verified along the way — and
    /// anything beyond it is truncated. Landing past or between frame boundaries is corruption.
    /// The file is then positioned at the committed offset, ready to append.
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
    ///
    /// Same frame-then-fsync shape as the global log, but note what is *missing* compared to
    /// GarbageLog::append: no ordering validation. That is deliberate, not an oversight. Each
    /// sweep appends whatever the global log delivered for this segment as its own frame, and
    /// records in later frames freely interleave in key order with records in earlier ones — a
    /// whole-file ordering invariant is impossible here. The ordering and conflict checks run at
    /// the other end instead, when fold_segment_garbage sorts the full history. The returned
    /// offset becomes meaningful only when the caller commits it as the segment's position row.
    pub fn append(&mut self, records: &[GarbageRecord]) -> Result<u64> {
        let offset = self.append_unsynced(records)?;
        self.sync()?;
        Ok(offset)
    }

    /// Appends one frame without syncing it. The caller must [`Self::sync`] this log, or sync
    /// its filesystem with [`sync_segment_garbage_logs`], before committing the returned offset.
    pub fn append_unsynced(&mut self, records: &[GarbageRecord]) -> Result<u64> {
        let records = encode_records(records)?;
        let frame_len = encoded_frame_len(&records)?;
        write_frame(&mut self.file, &records, &self.path)?;
        self.offset = self.offset.checked_add(frame_len).ok_or_else(|| {
            Error::InvalidGarbageLog("segment garbage offset overflow".to_owned())
        })?;
        Ok(self.offset)
    }

    pub fn sync(&self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|source| io_error(&self.path, source))
    }
}

/// Makes every unsynced append to `logs` durable.
///
/// A sweep that touches a thousand segments would otherwise pay a thousand fsyncs. On Linux one
/// `syncfs` on the directory holding the logs flushes them all in a single call; elsewhere each
/// log is synced on its own.
pub fn sync_segment_garbage_logs(directory: &Path, logs: &[SegmentGarbageLog]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let dir = File::open(directory).map_err(|source| io_error(directory, source))?;
        // SAFETY: `syncfs` takes a valid open file descriptor and has no other preconditions.
        if unsafe { libc::syncfs(dir.as_raw_fd()) } != 0 {
            return Err(io_error(directory, std::io::Error::last_os_error()));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        logs.iter().try_for_each(SegmentGarbageLog::sync)
    }
}

/// Reads only the prefix made visible by the stored committed offset.
///
/// The read-side twin of SegmentGarbageLog::open's trust model: committed 0 reads nothing (even
/// if a file exists — its bytes are unpublished), and the frames must tile the committed range
/// exactly, with a frame crossing the boundary or falling short reported as corruption. Bytes
/// past the committed offset are never even looked at; a torn tail from a crashed appender is
/// invisible here and gets truncated at the next open.
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
///
/// This is where the deferred validation for segment-local logs happens. The records — appended
/// across many frames in arrival order — are sorted into the canonical position order, exact
/// restatements are collapsed (the inline comment below explains the one legitimate source of
/// those), and three things are then rejected: an event whose key and record ref disagree about
/// the segment, records from more than one segment in one file, and two *different* events at
/// the same (key, lsn, range) position — the same contradictions the global log rejects at
/// append time, caught here instead.
///
/// The fold itself is a straight replay: each Retired event masks its range as retired, each
/// Expired as expired, each SetLifecycle installs a lifetime hint over its range. In the running
/// example this replay is how S7's overlay comes to say "B's range: retired, C's range: expired"
/// — the facts prepare_gc_publish later classifies copies against. The summary counters are
/// *not* recomputed from the replay; the RocksDB value is stamped on as-is, because the sweeper
/// maintains it transactionally from each record's `summary_delta` and the two views must not be
/// allowed to drift apart silently.
pub fn fold_segment_garbage(
    mut records: Vec<GarbageRecord>,
    summary: SegmentGcSummary,
) -> Result<SegmentGcOverlay> {
    records.sort_unstable_by(GarbageRecord::cmp_position);
    // Relocation-aware compaction may restate the same destination lifecycle at the relocation
    // LSN that initialized an output written by an older Store version. The segment summary is
    // authoritative and overlay transitions are idempotent, so collapse semantically identical
    // restatements while continuing to reject conflicting events at one logical position.
    records.dedup_by(|right, left| {
        left.key == right.key && left.lsn == right.lsn && left.event == right.event
    });
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
            "segment garbage log contains conflicting events for one key, lsn, and record"
                .to_owned(),
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

/// Encodes each record separately; per-record framing is added by write_frame.
pub(super) fn encode_records(records: &[GarbageRecord]) -> Result<Vec<Vec<u8>>> {
    records.iter().map(encode_record).collect()
}

const GARBAGE_EVENT_RETIRED: u8 = 1;
const GARBAGE_EVENT_EXPIRED: u8 = 2;
const GARBAGE_EVENT_SET_LIFECYCLE: u8 = 3;

/// Stable garbage-record payload format, all integers little-endian:
///
/// ```text
/// segment_id: u64 | key_len: u32 | key | lsn: u64
/// event_tag: u8 | record_ref: (u64, u64, u64)
/// [lifecycle_present: u8 | logical_end_epoch: u64 | extension_count: u32]
/// seven fixed i128 summary deltas
/// epoch_bytes: u32 count + (u64, i128)*
/// epoch_refs: u32 count + (u64, i128)*
/// extension_counts: u32 count + (u32, i128)*
/// ```
///
/// The bracketed lifecycle fields exist only for `SetLifecycle`; the two terminal event variants
/// end after their record reference. Map entries are emitted in strictly increasing key order.
fn encode_record(record: &GarbageRecord) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    put_u64(&mut encoded, record.key.segment_id);
    put_bytes(
        &mut encoded,
        record.key.blob_key.as_bytes(),
        "garbage record key",
    )?;
    put_u64(&mut encoded, record.lsn);
    match record.event {
        GarbageEvent::Retired { record } => {
            encoded.push(GARBAGE_EVENT_RETIRED);
            put_record_ref(&mut encoded, record);
        }
        GarbageEvent::Expired { record } => {
            encoded.push(GARBAGE_EVENT_EXPIRED);
            put_record_ref(&mut encoded, record);
        }
        GarbageEvent::SetLifecycle { record, lifecycle } => {
            encoded.push(GARBAGE_EVENT_SET_LIFECYCLE);
            put_record_ref(&mut encoded, record);
            match lifecycle {
                None => encoded.push(0),
                Some(lifecycle) => {
                    encoded.push(1);
                    put_u64(&mut encoded, lifecycle.logical_end_epoch);
                    put_u32(&mut encoded, lifecycle.extension_count);
                }
            }
        }
    }
    encode_summary_delta(&mut encoded, &record.summary_delta)?;
    Ok(encoded)
}

fn encode_summary_delta(encoded: &mut Vec<u8>, delta: &SegmentGcSummaryDelta) -> Result<()> {
    for value in [
        delta.total_bytes,
        delta.live_bytes,
        delta.retired_bytes,
        delta.expired_bytes,
        delta.live_ref_count,
        delta.unknown_lifetime_bytes,
        delta.unknown_lifetime_ref_count,
    ] {
        put_i128(encoded, value);
    }
    put_epoch_delta_map(encoded, &delta.epoch_bytes, "epoch-bytes delta map")?;
    put_epoch_delta_map(encoded, &delta.epoch_refs, "epoch-refs delta map")?;
    put_extension_delta_map(encoded, &delta.extension_counts)?;
    Ok(())
}

fn put_epoch_delta_map(
    encoded: &mut Vec<u8>,
    values: &BTreeMap<u64, i128>,
    name: &str,
) -> Result<()> {
    put_count(encoded, values.len(), name)?;
    for (&key, &value) in values {
        put_u64(encoded, key);
        put_i128(encoded, value);
    }
    Ok(())
}

fn put_extension_delta_map(encoded: &mut Vec<u8>, values: &BTreeMap<u32, i128>) -> Result<()> {
    put_count(encoded, values.len(), "extension-count delta map")?;
    for (&key, &value) in values {
        put_u32(encoded, key);
        put_i128(encoded, value);
    }
    Ok(())
}

fn put_count(encoded: &mut Vec<u8>, count: usize, name: &str) -> Result<()> {
    let count = u32::try_from(count)
        .map_err(|_| Error::InvalidGarbageLog(format!("{name} has too many entries")))?;
    put_u32(encoded, count);
    Ok(())
}

fn put_bytes(encoded: &mut Vec<u8>, bytes: &[u8], name: &str) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| Error::InvalidGarbageLog(format!("{name} exceeds u32::MAX bytes")))?;
    put_u32(encoded, len);
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn put_record_ref(encoded: &mut Vec<u8>, record: RecordRef) {
    put_u64(encoded, record.segment_id);
    put_u64(encoded, record.offset);
    put_u64(encoded, record.len);
}

fn put_u32(encoded: &mut Vec<u8>, value: u32) {
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn put_i128(encoded: &mut Vec<u8>, value: i128) {
    encoded.extend_from_slice(&value.to_le_bytes());
}

fn decode_record(encoded: &[u8]) -> std::result::Result<GarbageRecord, String> {
    let mut decoder = GarbageRecordDecoder::new(encoded);
    let segment_id = decoder.u64("segment id")?;
    let blob_key = BlobKey::new(decoder.bytes("blob key")?.to_vec())
        .map_err(|error| format!("invalid blob key: {error}"))?;
    let lsn = decoder.u64("lsn")?;
    let event_tag = decoder.u8("event tag")?;
    let record = decoder.record_ref()?;
    let event = match event_tag {
        GARBAGE_EVENT_RETIRED => GarbageEvent::Retired { record },
        GARBAGE_EVENT_EXPIRED => GarbageEvent::Expired { record },
        GARBAGE_EVENT_SET_LIFECYCLE => {
            let lifecycle = match decoder.u8("lifecycle presence")? {
                0 => None,
                1 => Some(BlobLifecycle {
                    logical_end_epoch: decoder.u64("logical end epoch")?,
                    extension_count: decoder.u32("extension count")?,
                }),
                other => return Err(format!("invalid lifecycle presence {other}")),
            };
            GarbageEvent::SetLifecycle { record, lifecycle }
        }
        other => return Err(format!("unknown garbage event tag {other}")),
    };
    let summary_delta = decoder.summary_delta()?;
    decoder.finish()?;
    Ok(GarbageRecord {
        key: SegmentKey {
            segment_id,
            blob_key,
        },
        lsn,
        event,
        summary_delta,
    })
}

struct GarbageRecordDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> GarbageRecordDecoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { remaining: encoded }
    }

    fn finish(self) -> std::result::Result<(), String> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "garbage record has {} trailing bytes",
                self.remaining.len()
            ))
        }
    }

    fn take(&mut self, len: usize, name: &str) -> std::result::Result<&'a [u8], String> {
        let bytes = self
            .remaining
            .get(..len)
            .ok_or_else(|| format!("truncated {name}"))?;
        self.remaining = &self.remaining[len..];
        Ok(bytes)
    }

    fn u8(&mut self, name: &str) -> std::result::Result<u8, String> {
        Ok(self.take(1, name)?[0])
    }

    fn u32(&mut self, name: &str) -> std::result::Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4, name)?.try_into().unwrap()))
    }

    fn u64(&mut self, name: &str) -> std::result::Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8, name)?.try_into().unwrap()))
    }

    fn i128(&mut self, name: &str) -> std::result::Result<i128, String> {
        Ok(i128::from_le_bytes(
            self.take(16, name)?.try_into().unwrap(),
        ))
    }

    fn bytes(&mut self, name: &str) -> std::result::Result<&'a [u8], String> {
        let len = self.u32(&format!("{name} length"))? as usize;
        self.take(len, name)
    }

    fn record_ref(&mut self) -> std::result::Result<RecordRef, String> {
        Ok(RecordRef {
            segment_id: self.u64("record segment id")?,
            offset: self.u64("record offset")?,
            len: self.u64("record length")?,
        })
    }

    fn summary_delta(&mut self) -> std::result::Result<SegmentGcSummaryDelta, String> {
        Ok(SegmentGcSummaryDelta {
            total_bytes: self.i128("total-bytes delta")?,
            live_bytes: self.i128("live-bytes delta")?,
            retired_bytes: self.i128("retired-bytes delta")?,
            expired_bytes: self.i128("expired-bytes delta")?,
            live_ref_count: self.i128("live-ref-count delta")?,
            unknown_lifetime_bytes: self.i128("unknown-lifetime-bytes delta")?,
            unknown_lifetime_ref_count: self.i128("unknown-lifetime-ref-count delta")?,
            epoch_bytes: self.epoch_delta_map("epoch-bytes delta map")?,
            epoch_refs: self.epoch_delta_map("epoch-refs delta map")?,
            extension_counts: self.extension_delta_map()?,
        })
    }

    fn epoch_delta_map(&mut self, name: &str) -> std::result::Result<BTreeMap<u64, i128>, String> {
        let count = self.u32(&format!("{name} entry count"))?;
        let mut values = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let key = self.u64(&format!("{name} key"))?;
            if previous.is_some_and(|previous| key <= previous) {
                return Err(format!("{name} keys are not strictly increasing"));
            }
            let value = self.i128(&format!("{name} value"))?;
            values.insert(key, value);
            previous = Some(key);
        }
        Ok(values)
    }

    fn extension_delta_map(&mut self) -> std::result::Result<BTreeMap<u32, i128>, String> {
        let name = "extension-count delta map";
        let count = self.u32("extension-count delta map entry count")?;
        let mut values = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let key = self.u32("extension-count delta map key")?;
            if previous.is_some_and(|previous| key <= previous) {
                return Err(format!("{name} keys are not strictly increasing"));
            }
            let value = self.i128("extension-count delta map value")?;
            values.insert(key, value);
            previous = Some(key);
        }
        Ok(values)
    }
}

/// Computes the exact on-disk frame size (12-byte prefix + 4-byte length per record + record
/// bytes + 32-byte checksum), rejecting empty batches and anything whose counts or lengths would
/// not fit the u32 fields of the frame format. Empty batches are refused because a zero-record
/// frame would advance the position while committing nothing — a pure hazard.
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

/// Writes one frame at the file's current position: prefix, length-delimited records, then the
/// SHA-256 of everything before it. The checksum is last on purpose — a torn write anywhere in
/// the frame leaves a checksum mismatch, which readers treat as "the log ends here", never as
/// data. Syncing is the caller's job.
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

/// Reads and fully validates the single frame starting at `offset`, returning its records and
/// exclusive end.
///
/// Everything is checked before anything is believed: the file header, the frame's claimed
/// length against the file, the SHA-256 over prefix and payload, each record's length field, and
/// finally that the payload holds exactly the claimed record count with no bytes left over.
/// Callers use this only at offsets a committed position vouches for; recovery uses its own
/// tolerant scanner (recovery.rs) that treats a bad frame as end-of-log rather than an error,
/// because past the committed position a bad frame is expected, not corrupt.
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
        records.push(
            decode_record(bytes).map_err(|reason| Error::CorruptGarbageLog {
                path: path.to_path_buf(),
                reason: format!("invalid garbage record: {reason}"),
            })?,
        );
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

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn explicit_codec_round_trips_every_event_and_summary_field() {
        let summary_delta = SegmentGcSummaryDelta {
            total_bytes: i128::MIN,
            live_bytes: -2,
            retired_bytes: 3,
            expired_bytes: -4,
            live_ref_count: 5,
            unknown_lifetime_bytes: -6,
            unknown_lifetime_ref_count: i128::MAX,
            epoch_bytes: BTreeMap::from([(7, -8), (9, 10)]),
            epoch_refs: BTreeMap::from([(11, -12), (13, 14)]),
            extension_counts: BTreeMap::from([(15, -16), (17, 18)]),
        };
        let record_ref = RecordRef {
            segment_id: 23,
            offset: 29,
            len: 31,
        };
        let events = [
            GarbageEvent::Retired { record: record_ref },
            GarbageEvent::Expired { record: record_ref },
            GarbageEvent::SetLifecycle {
                record: record_ref,
                lifecycle: None,
            },
            GarbageEvent::SetLifecycle {
                record: record_ref,
                lifecycle: Some(BlobLifecycle {
                    logical_end_epoch: 37,
                    extension_count: 41,
                }),
            },
        ];

        for event in events {
            let record = GarbageRecord {
                key: SegmentKey {
                    segment_id: 23,
                    blob_key: BlobKey::new(b"codec-key".to_vec()).unwrap(),
                },
                lsn: 43,
                event,
                summary_delta: summary_delta.clone(),
            };
            let encoded = encode_record(&record).unwrap();
            assert_eq!(decode_record(&encoded).unwrap(), record);
        }
    }

    #[test]
    fn explicit_codec_event_tags_are_stable() {
        let record_ref = RecordRef {
            segment_id: 1,
            offset: 2,
            len: 3,
        };
        let events = [
            (GarbageEvent::Retired { record: record_ref }, 1),
            (GarbageEvent::Expired { record: record_ref }, 2),
            (
                GarbageEvent::SetLifecycle {
                    record: record_ref,
                    lifecycle: None,
                },
                3,
            ),
        ];

        for (event, expected_tag) in events {
            let record = test_record(event, SegmentGcSummaryDelta::default());
            let encoded = encode_record(&record).unwrap();
            assert_eq!(encoded[event_tag_offset(&record)], expected_tag);
        }
    }

    #[test]
    fn explicit_codec_rejects_unknown_tags_and_trailing_bytes() {
        let record_ref = RecordRef {
            segment_id: 1,
            offset: 2,
            len: 3,
        };
        let record = test_record(
            GarbageEvent::Retired { record: record_ref },
            SegmentGcSummaryDelta::default(),
        );
        let mut encoded = encode_record(&record).unwrap();
        encoded[event_tag_offset(&record)] = 255;
        assert!(
            decode_record(&encoded)
                .unwrap_err()
                .contains("unknown garbage event tag")
        );

        let mut encoded = encode_record(&record).unwrap();
        encoded.push(0);
        assert!(
            decode_record(&encoded)
                .unwrap_err()
                .contains("trailing bytes")
        );
    }

    #[test]
    fn explicit_codec_rejects_noncanonical_map_order() {
        let record_ref = RecordRef {
            segment_id: 1,
            offset: 2,
            len: 3,
        };
        let record = test_record(
            GarbageEvent::Retired { record: record_ref },
            SegmentGcSummaryDelta {
                epoch_bytes: BTreeMap::from([(1, 10), (2, 20)]),
                ..Default::default()
            },
        );
        let mut encoded = encode_record(&record).unwrap();
        let map_start = event_tag_offset(&record) + 1 + 24 + 7 * 16;
        let first_entry = encoded[map_start + 4..map_start + 28].to_vec();
        let second_entry = encoded[map_start + 28..map_start + 52].to_vec();
        encoded[map_start + 4..map_start + 28].copy_from_slice(&second_entry);
        encoded[map_start + 28..map_start + 52].copy_from_slice(&first_entry);

        assert!(
            decode_record(&encoded)
                .unwrap_err()
                .contains("keys are not strictly increasing")
        );
    }

    fn test_record(event: GarbageEvent, summary_delta: SegmentGcSummaryDelta) -> GarbageRecord {
        GarbageRecord {
            key: SegmentKey {
                segment_id: 1,
                blob_key: BlobKey::new(b"k".to_vec()).unwrap(),
            },
            lsn: 4,
            event,
            summary_delta,
        }
    }

    fn event_tag_offset(record: &GarbageRecord) -> usize {
        8 + 4 + record.key.blob_key.len() + 8
    }
}
