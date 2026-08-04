use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use sha2::{Digest, Sha256};

use super::garbage_log::{
    FILE_PREFIX, FILE_SUFFIX, GarbageLog, GarbageLogPosition, HEADER_LEN, io_error,
};
use crate::{Error, Result};

pub(super) const MAGIC: &[u8; 8] = b"STRGL002";
pub(super) const VERSION: u32 = 2;

pub(super) fn recover(dir: &Path, committed: GarbageLogPosition) -> Result<(File, u64, u64)> {
    validate_position(committed)?;
    fs::create_dir_all(dir).map_err(|source| io_error(dir, source))?;

    if committed == GarbageLogPosition::default() {
        remove_logs_after(dir, 0)?;
        let (file, offset) = create_log(dir, 1)?;
        return Ok((file, 1, offset));
    }

    let path = GarbageLog::path(dir, committed.log_id);
    let scan = scan_log(&path, committed.offset)?;
    if !scan.committed_is_boundary {
        return Err(Error::CorruptGarbageLog {
            path,
            reason: format!(
                "committed offset {} is not the end of a valid frame",
                committed.offset
            ),
        });
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    if scan.file_len != committed.offset {
        file.set_len(committed.offset)
            .map_err(|source| io_error(&path, source))?;
        file.sync_data().map_err(|source| io_error(&path, source))?;
    }
    file.seek(SeekFrom::Start(committed.offset))
        .map_err(|source| io_error(&path, source))?;
    remove_logs_after(dir, committed.log_id)?;
    Ok((file, committed.log_id, committed.offset))
}

pub(super) fn create_log(dir: &Path, log_id: u64) -> Result<(File, u64)> {
    let path = GarbageLog::path(dir, log_id);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    file.write_all(MAGIC)
        .and_then(|_| file.write_all(&VERSION.to_le_bytes()))
        .map_err(|source| io_error(&path, source))?;
    file.sync_all().map_err(|source| io_error(&path, source))?;
    sync_dir(dir)?;
    Ok((file, HEADER_LEN))
}

struct LogScan {
    file_len: u64,
    committed_is_boundary: bool,
}

fn scan_log(path: &Path, committed_offset: u64) -> Result<LogScan> {
    let file = File::open(path).map_err(|source| io_error(path, source))?;
    let file_len = file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len();
    let mut reader = BufReader::new(file);
    read_header(&mut reader, path)?;

    let mut valid_offset = HEADER_LEN;
    let mut committed_is_boundary = committed_offset == HEADER_LEN;
    while read_frame(&mut reader, file_len, path)? {
        valid_offset = reader
            .stream_position()
            .map_err(|source| io_error(path, source))?;
        committed_is_boundary |= valid_offset == committed_offset;
    }
    if committed_offset > valid_offset {
        committed_is_boundary = false;
    }
    Ok(LogScan {
        file_len,
        committed_is_boundary,
    })
}

pub(super) fn read_header(reader: &mut impl Read, path: &Path) -> Result<()> {
    let mut header = [0; HEADER_LEN as usize];
    if reader.read_exact(&mut header).is_err()
        || &header[..8] != MAGIC
        || u32::from_le_bytes(header[8..].try_into().unwrap()) != VERSION
    {
        return Err(Error::CorruptGarbageLog {
            path: path.to_path_buf(),
            reason: "invalid header".to_owned(),
        });
    }
    Ok(())
}

fn read_frame(reader: &mut (impl Read + Seek), file_len: u64, path: &Path) -> Result<bool> {
    let frame_start = reader
        .stream_position()
        .map_err(|source| io_error(path, source))?;
    let mut prefix = [0; 12];
    if let Err(error) = reader.read_exact(&mut prefix) {
        return match error.kind() {
            io::ErrorKind::UnexpectedEof => Ok(false),
            _ => Err(io_error(path, error)),
        };
    }
    let payload_len = u64::from_le_bytes(prefix[..8].try_into().unwrap());
    let record_count = u32::from_le_bytes(prefix[8..].try_into().unwrap());
    let Some(frame_end) = frame_start
        .checked_add(12)
        .and_then(|offset| offset.checked_add(payload_len))
        .and_then(|offset| offset.checked_add(32))
    else {
        return Ok(false);
    };
    if frame_end > file_len {
        return Ok(false);
    }

    let mut checksum = Sha256::new();
    checksum.update(prefix);
    let mut remaining = payload_len;
    let mut buffer = [0; 64 * 1024];
    for _ in 0..record_count {
        if remaining < 4 {
            return Ok(false);
        }
        let mut len = [0; 4];
        reader
            .read_exact(&mut len)
            .map_err(|source| io_error(path, source))?;
        checksum.update(len);
        remaining -= 4;
        let record_len = u32::from_le_bytes(len) as u64;
        if record_len > remaining {
            return Ok(false);
        }
        let mut unread = record_len;
        while unread > 0 {
            let chunk = usize::try_from(unread.min(buffer.len() as u64)).unwrap();
            reader
                .read_exact(&mut buffer[..chunk])
                .map_err(|source| io_error(path, source))?;
            checksum.update(&buffer[..chunk]);
            unread -= chunk as u64;
        }
        remaining -= record_len;
    }
    if remaining != 0 {
        return Ok(false);
    }

    let mut stored_checksum = [0; 32];
    reader
        .read_exact(&mut stored_checksum)
        .map_err(|source| io_error(path, source))?;
    Ok(stored_checksum == checksum.finalize().as_slice())
}

pub(super) fn validate_position(position: GarbageLogPosition) -> Result<()> {
    if position == GarbageLogPosition::default()
        || (position.log_id > 0 && position.offset >= HEADER_LEN)
    {
        Ok(())
    } else {
        Err(Error::InvalidGarbageLog(format!(
            "invalid committed position ({}, {})",
            position.log_id, position.offset
        )))
    }
}

pub(super) fn remove_logs_before(dir: &Path, log_id: u64) -> Result<()> {
    remove_logs(dir, |id| id < log_id)
}

fn remove_logs_after(dir: &Path, log_id: u64) -> Result<()> {
    remove_logs(dir, |id| id > log_id)
}

fn remove_logs(dir: &Path, should_remove: impl Fn(u64) -> bool) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(dir).map_err(|source| io_error(dir, source))? {
        let entry = entry.map_err(|source| io_error(dir, source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix(FILE_PREFIX)
            .and_then(|name| name.strip_suffix(FILE_SUFFIX))
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        if should_remove(id) {
            fs::remove_file(entry.path()).map_err(|source| io_error(&entry.path(), source))?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

pub(super) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|source| io_error(dir, source))
}
