use super::*;

#[derive(Debug)]
pub(super) struct RecoveredLogPrefix {
    pub(super) header_offset: u64,
    pub(super) valid_offset: u64,
    pub(super) file_len: u64,
    pub(super) max_lsn: Option<StrataLsn>,
}

pub(super) fn ensure_log_file(path: &Path) -> Result<bool> {
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
            format_version: ACTIVE_DELTA_LOG_FORMAT_VERSION,
        },
        path,
    )?;
    file.sync_all().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(true)
}

pub(super) fn scan_log_prefix(path: &Path) -> Result<RecoveredLogPrefix> {
    scan_log_prefix_inner(path, |_| true)
}

pub(super) fn scan_log_prefix_until_lsn(
    path: &Path,
    max_lsn: StrataLsn,
) -> Result<RecoveredLogPrefix> {
    scan_log_prefix_inner(path, |entry| entry.lsn() <= max_lsn)
}

fn scan_log_prefix_inner(
    path: &Path,
    keep_entry: impl Fn(&AccountingLogEntry) -> bool,
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
    let header_offset = reader.stream_position().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut valid_offset = header_offset;
    let mut max_lsn = None;

    loop {
        let frame_start = valid_offset;
        let Some(entry) = read_log_entry(&mut reader, path)? else {
            break;
        };
        if !keep_entry(&entry) {
            break;
        }
        valid_offset = reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        max_lsn = Some(max_lsn.map_or(entry.lsn(), |max: StrataLsn| max.max(entry.lsn())));
        if valid_offset == frame_start {
            break;
        }
    }

    Ok(RecoveredLogPrefix {
        header_offset,
        valid_offset,
        file_len,
        max_lsn,
    })
}

pub(super) fn active_delta_log_segment_ids(root_dir: &Path) -> Result<BTreeSet<SegmentId>> {
    let mut ids = BTreeSet::new();
    let entries = match fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(ids),
        Err(source) => {
            return Err(Error::Io {
                path: root_dir.to_path_buf(),
                source,
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: root_dir.to_path_buf(),
            source,
        })?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(segment_id) = parse_log_file_name(&file_name) else {
            continue;
        };
        ids.insert(segment_id);
    }
    Ok(ids)
}

pub(super) fn log_file_name(segment_id: SegmentId) -> String {
    format!("{ACTIVE_DELTA_LOG_FILE_PREFIX}{segment_id:020}{ACTIVE_DELTA_LOG_FILE_SUFFIX}")
}

fn parse_log_file_name(file_name: &str) -> Option<SegmentId> {
    let segment_id = file_name
        .strip_prefix(ACTIVE_DELTA_LOG_FILE_PREFIX)?
        .strip_suffix(ACTIVE_DELTA_LOG_FILE_SUFFIX)?;
    segment_id.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ActiveDeltaLogHeader {
    format_version: u32,
}

pub(super) fn read_log_header(reader: &mut impl Read, path: &Path) -> Result<()> {
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
    if header.format_version != ACTIVE_DELTA_LOG_FORMAT_VERSION {
        return Err(Error::IncompatibleManifestVersion {
            actual: header.format_version,
            expected: ACTIVE_DELTA_LOG_FORMAT_VERSION,
        });
    }
    Ok(())
}

pub(super) fn read_log_entry(
    reader: &mut impl Read,
    path: &Path,
) -> Result<Option<AccountingLogEntry>> {
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

pub(super) fn frame_len(entry: &AccountingLogEntry) -> Result<u64> {
    let encoded_len = bcs::to_bytes(entry)?.len();
    let frame_len = encoded_len
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(Error::RunFrameTooLarge { len: encoded_len })?;
    u64::try_from(frame_len).map_err(|_| Error::RunFrameTooLarge { len: frame_len })
}

pub(super) fn truncate_file(path: &Path, len: u64) -> Result<()> {
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

pub(super) fn truncate_open_file(file: &mut fs::File, path: &Path, len: u64) -> Result<()> {
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

pub(super) fn sync_parent_dir(path: &Path) -> Result<()> {
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
