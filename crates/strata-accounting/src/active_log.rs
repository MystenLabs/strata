use std::{
    fs::{self, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use strata_core::{BlobKey, RecordRef, StrataLsn};

use crate::run_io::{read_record_frame, write_record_frame};
use crate::{BlobUpdate, EpochChange, Error, FORMAT_VERSION, Result};

const ACTIVE_DELTA_LOG_MAGIC: &[u8; 8] = b"STRADL01";
const ACTIVE_DELTA_LOG_FILE_NAME: &str = "active-delta.log";

/// Raw foreground accounting event appended in store-global LSN order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountingDelta {
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
        maps: Vec<GcMapRefDelta>,
    },
}

/// One logical `MapRef` inside a bulk GC active-log frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcMapRefDelta {
    /// Blob key whose payload version is being remapped by GC.
    pub key: BlobKey,
    /// Source physical record copied out of a GC candidate segment.
    pub from: RecordRef,
    /// Destination physical record in the newly published GC output segment.
    pub to: RecordRef,
}

impl AccountingDelta {
    pub fn lsn(&self) -> StrataLsn {
        match self {
            Self::Blob(update) => update.lsn(),
            Self::Epoch(change) => change.lsn,
            Self::GcMapRefBatch { base_lsn, maps } => maps
                .len()
                .checked_sub(1)
                .and_then(|last| base_lsn.checked_add(last as u64))
                .unwrap_or(*base_lsn),
        }
    }
}

/// Durable cursor for the active accounting delta log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActiveDeltaLogState {
    pub durable_offset: u64,
    pub durable_lsn: StrataLsn,
}

impl ActiveDeltaLogState {
    pub fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(bytes)?)
    }
}

/// Durable cursor for sidecar ingestion from the active accounting delta log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActiveDeltaLogReadCursor {
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

/// Deltas read from a durable active-log range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveDeltaLogRead {
    pub deltas: Vec<AccountingDelta>,
    pub end_offset: u64,
    pub max_lsn: Option<StrataLsn>,
}

impl ActiveDeltaLogRead {
    pub fn next_cursor(&self, previous: ActiveDeltaLogReadCursor) -> ActiveDeltaLogReadCursor {
        ActiveDeltaLogReadCursor {
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
    write_offset: u64,
    max_lsn: Option<StrataLsn>,
    durable_offset: u64,
    durable_lsn: StrataLsn,
}

impl ActiveDeltaLog {
    pub fn open(root_dir: impl AsRef<Path>, durable_state: ActiveDeltaLogState) -> Result<Self> {
        let path = root_dir.as_ref().join(ACTIVE_DELTA_LOG_FILE_NAME);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let created = ensure_log_file(&path)?;

        // Recovery only trusts the longest prefix made of complete, valid frames.
        // Example: a process can crash after writing a frame length and part of
        // the payload for LSN 42; scanning stops before that torn frame so reopen
        // keeps all deltas through LSN 41 and discards the incomplete write.
        let recovered = scan_log_prefix(&path)?;

        // RocksDB stores the durable cursor after the log has been fsynced, so the
        // normal skew is `durable_state.durable_offset <= recovered.valid_offset`.
        // Equality is a clean shutdown. Less-than means the file has complete
        // frames that metadata did not publish yet; for example, we crashed after
        // fsyncing active-delta.log but before the RocksDB WAL persisted the new
        // active-delta-log state. Keep those bytes for the caller to reconcile
        // against the committed store LSN, but do not promote `durable_offset`
        // here because this layer cannot tell whether the extra deltas committed.
        //
        // The impossible skew is the opposite direction. If the cursor points past
        // the recovered prefix, the two durability records contradict each other:
        // RocksDB claims bytes are durable that the log file does not contain.
        // Continuing would let sidecar readers skip missing deltas, so treat this
        // as corruption instead of silently rewinding.
        if durable_state.durable_offset > recovered.valid_offset {
            return Err(Error::CorruptRun {
                path,
                reason: format!(
                    "active delta durable offset {} is past recovered offset {}",
                    durable_state.durable_offset, recovered.valid_offset
                ),
            });
        }

        // Remove any garbage after the valid prefix before accepting appends.
        // Otherwise a crash tail like a half written LSN 42 frame would remain in
        // the middle of the file after we append LSN 43, and the next recovery
        // scan would stop at the old torn bytes instead of seeing the new frame.
        if recovered.file_len != recovered.valid_offset {
            truncate_file(&path, recovered.valid_offset)?;
        }

        // A brand new log needs the directory entry itself to reach storage. If we
        // skip the parent directory fsync, a crash can leave RocksDB metadata that
        // refers to active delta.log while the filename was never made durable.
        if created {
            sync_parent_dir(&path)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;

        // Start writing exactly after the valid prefix, not at the old EOF. This
        // is what makes the truncation above effective for cases where the last
        // append was torn and recovered.valid_offset is earlier than file_len.
        file.seek(SeekFrom::Start(recovered.valid_offset))
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            write_offset: recovered.valid_offset,
            max_lsn: recovered.max_lsn,
            durable_offset: durable_state.durable_offset,
            // Keep the durable LSN no higher than what this file actually
            // contains. For example, if metadata says durable_lsn=100 but the
            // recovered log is empty after a fresh initialization, advertising
            // 100 would make recovery believe accounting deltas up to 100 are
            // present when they are not.
            durable_lsn: durable_state
                .durable_lsn
                .min(recovered.max_lsn.unwrap_or_default()),
        })
    }

    pub fn position(&self) -> ActiveDeltaLogPosition {
        ActiveDeltaLogPosition {
            write_offset: self.write_offset,
            max_lsn: self.max_lsn,
            durable_offset: self.durable_offset,
            durable_lsn: self.durable_lsn,
        }
    }

    pub fn state(&self) -> ActiveDeltaLogState {
        ActiveDeltaLogState {
            durable_offset: self.durable_offset,
            durable_lsn: self.durable_lsn,
        }
    }

    pub fn durable_lsn(&self) -> StrataLsn {
        self.durable_lsn
    }

    pub fn max_lsn(&self) -> Option<StrataLsn> {
        self.max_lsn
    }

    pub fn read_durable_range(
        root_dir: impl AsRef<Path>,
        cursor: ActiveDeltaLogReadCursor,
        durable_state: ActiveDeltaLogState,
    ) -> Result<ActiveDeltaLogRead> {
        let path = root_dir.as_ref().join(ACTIVE_DELTA_LOG_FILE_NAME);
        read_durable_range(&path, cursor, durable_state)
    }

    pub fn append(&mut self, delta: &AccountingDelta) -> Result<()> {
        let frame_len = frame_len(delta)?;
        let next_offset = self
            .write_offset
            .checked_add(frame_len)
            .ok_or(Error::RunFrameTooLarge { len: usize::MAX })?;
        write_record_frame(&mut self.writer, delta, &self.path)?;
        self.write_offset = next_offset;
        self.max_lsn = Some(self.max_lsn.map_or(delta.lsn(), |max| max.max(delta.lsn())));
        Ok(())
    }

    pub fn append_all<'a>(
        &mut self,
        deltas: impl IntoIterator<Item = &'a AccountingDelta>,
    ) -> Result<()> {
        for delta in deltas {
            self.append(delta)?;
        }
        Ok(())
    }

    pub fn rollback_to(&mut self, position: ActiveDeltaLogPosition) -> Result<()> {
        self.flush()?;
        truncate_open_file(self.writer.get_mut(), &self.path, position.write_offset)?;
        self.write_offset = position.write_offset;
        self.max_lsn = position.max_lsn;
        self.durable_offset = position.durable_offset;
        self.durable_lsn = position.durable_lsn;
        Ok(())
    }

    pub fn truncate_after_lsn(&mut self, max_lsn: StrataLsn) -> Result<()> {
        self.flush()?;
        let recovered = scan_log_prefix_until_lsn(&self.path, max_lsn)?;
        if recovered.valid_offset < self.write_offset {
            truncate_open_file(self.writer.get_mut(), &self.path, recovered.valid_offset)?;
            self.write_offset = recovered.valid_offset;
            self.max_lsn = recovered.max_lsn;
        }
        if self.durable_offset > self.write_offset {
            self.durable_offset = self.write_offset;
            self.durable_lsn = self.durable_lsn.min(max_lsn);
        }
        Ok(())
    }

    pub fn sync_data(&mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|source| Error::Io {
                path: self.path.clone(),
                source,
            })?;
        self.durable_offset = self.write_offset;
        if let Some(max_lsn) = self.max_lsn {
            self.durable_lsn = self.durable_lsn.max(max_lsn);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|source| Error::Io {
            path: self.path.clone(),
            source,
        })
    }
}

fn read_durable_range(
    path: &Path,
    cursor: ActiveDeltaLogReadCursor,
    durable_state: ActiveDeltaLogState,
) -> Result<ActiveDeltaLogRead> {
    if cursor.offset > durable_state.durable_offset {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: format!(
                "active delta read cursor {} is past durable offset {}",
                cursor.offset, durable_state.durable_offset
            ),
        });
    }
    if cursor.offset == durable_state.durable_offset {
        return Ok(ActiveDeltaLogRead {
            deltas: Vec::new(),
            end_offset: cursor.offset,
            max_lsn: None,
        });
    }

    let file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let start_offset = if cursor.offset == 0 {
        read_log_header(&mut reader, path)?;
        reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?
    } else {
        reader
            .seek(SeekFrom::Start(cursor.offset))
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?
    };

    if start_offset > durable_state.durable_offset {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: format!(
                "active delta read start {} is past durable offset {}",
                start_offset, durable_state.durable_offset
            ),
        });
    }

    let mut end_offset = start_offset;
    let mut max_lsn = None;
    let mut deltas = Vec::new();
    while end_offset < durable_state.durable_offset {
        let Some(delta) = read_delta_frame(&mut reader, path)? else {
            return Err(Error::CorruptRun {
                path: path.to_path_buf(),
                reason: format!(
                    "active delta log ended before durable offset {}",
                    durable_state.durable_offset
                ),
            });
        };
        end_offset = reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if end_offset > durable_state.durable_offset {
            return Err(Error::CorruptRun {
                path: path.to_path_buf(),
                reason: format!(
                    "active delta frame ended at {} past durable offset {}",
                    end_offset, durable_state.durable_offset
                ),
            });
        }
        max_lsn = Some(max_lsn.map_or(delta.lsn(), |max: StrataLsn| max.max(delta.lsn())));
        deltas.push(delta);
    }

    Ok(ActiveDeltaLogRead {
        deltas,
        end_offset,
        max_lsn,
    })
}

#[derive(Debug)]
struct RecoveredLogPrefix {
    valid_offset: u64,
    file_len: u64,
    max_lsn: Option<StrataLsn>,
}

fn ensure_log_file(path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(ACTIVE_DELTA_LOG_MAGIC)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    write_record_frame(
        &mut file,
        &ActiveDeltaLogHeader {
            format_version: FORMAT_VERSION,
        },
        path,
    )?;
    file.sync_all().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(true)
}

fn scan_log_prefix(path: &Path) -> Result<RecoveredLogPrefix> {
    scan_log_prefix_inner(path, |_| true)
}

fn scan_log_prefix_until_lsn(path: &Path, max_lsn: StrataLsn) -> Result<RecoveredLogPrefix> {
    scan_log_prefix_inner(path, |delta| delta.lsn() <= max_lsn)
}

fn scan_log_prefix_inner(
    path: &Path,
    keep_delta: impl Fn(&AccountingDelta) -> bool,
) -> Result<RecoveredLogPrefix> {
    let file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let file_len = file
        .metadata()
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let mut reader = BufReader::new(file);
    read_log_header(&mut reader, path)?;
    let mut valid_offset = reader.stream_position().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut max_lsn = None;

    loop {
        let frame_start = valid_offset;
        let Some(delta) = read_delta_frame(&mut reader, path)? else {
            break;
        };
        if !keep_delta(&delta) {
            break;
        }
        valid_offset = reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        max_lsn = Some(max_lsn.map_or(delta.lsn(), |max: StrataLsn| max.max(delta.lsn())));
        if valid_offset == frame_start {
            break;
        }
    }

    Ok(RecoveredLogPrefix {
        valid_offset,
        file_len,
        max_lsn,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ActiveDeltaLogHeader {
    format_version: u32,
}

fn read_log_header(reader: &mut impl Read, path: &Path) -> Result<()> {
    let mut magic = [0; 8];
    reader.read_exact(&mut magic).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if &magic != ACTIVE_DELTA_LOG_MAGIC {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: "invalid active delta log magic".to_owned(),
        });
    }
    let Some(header) = read_record_frame::<ActiveDeltaLogHeader>(reader, path)? else {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: "missing active delta log header".to_owned(),
        });
    };
    if header.format_version != FORMAT_VERSION {
        return Err(Error::IncompatibleManifestVersion {
            actual: header.format_version,
            expected: FORMAT_VERSION,
        });
    }
    Ok(())
}

fn read_delta_frame(reader: &mut impl Read, path: &Path) -> Result<Option<AccountingDelta>> {
    let mut len = [0; 4];
    match reader.read_exact(&mut len) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(source) => {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    }

    let len = u32::from_le_bytes(len) as usize;
    let mut bytes = vec![0; len];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(bcs::from_bytes(&bytes)?)),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn frame_len(delta: &AccountingDelta) -> Result<u64> {
    let encoded_len = bcs::to_bytes(delta)?.len();
    let frame_len = encoded_len
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(Error::RunFrameTooLarge { len: encoded_len })?;
    u64::try_from(frame_len).map_err(|_| Error::RunFrameTooLarge { len: frame_len })
}

fn truncate_file(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.set_len(len).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn truncate_open_file(file: &mut fs::File, path: &Path, len: u64) -> Result<()> {
    file.set_len(len).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.seek(SeekFrom::Start(len))
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = fs::File::open(parent).map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })
}
