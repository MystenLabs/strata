use super::*;

pub(super) fn read_durable_range(
    root_dir: &Path,
    cursor: ActiveDeltaLogReadCursor,
    durable_position: AccountingLogDurablePosition,
) -> Result<ActiveDeltaLogRead> {
    if cursor.segment_id > durable_position.segment_id {
        return Err(Error::CorruptRun {
            path: root_dir.to_path_buf(),
            reason: format!(
                "active delta read cursor segment {} is past durable segment {}",
                cursor.segment_id, durable_position.segment_id
            ),
        });
    }

    let segment_ids = readable_log_segment_ids(root_dir, cursor.segment_id, durable_position)?;
    let Some(mut last_segment_id) = segment_ids.first().copied() else {
        return Ok(ActiveDeltaLogRead {
            entries: Vec::new(),
            bytes_read: 0,
            end_segment_id: durable_position.segment_id,
            end_offset: durable_position.durable_offset,
            max_lsn: None,
        });
    };
    let mut last_offset = 0;
    let mut entries = Vec::new();
    let mut bytes_read = 0_u64;
    let mut max_lsn = None;

    for segment_id in segment_ids {
        last_segment_id = segment_id;
        let path = ActiveDeltaLog::path(root_dir, segment_id);
        let recovered = scan_log_prefix(&path)?;
        let durable_offset = if segment_id == durable_position.segment_id {
            durable_position.durable_offset
        } else {
            recovered.valid_offset
        };
        let start_cursor_offset = if segment_id == cursor.segment_id {
            cursor.offset
        } else {
            0
        };
        let read =
            read_durable_range_from_file(&path, segment_id, start_cursor_offset, durable_offset)?;
        last_offset = read.end_offset;
        bytes_read = bytes_read.saturating_add(read.bytes_read);
        if let Some(read_max_lsn) = read.max_lsn {
            max_lsn = Some(max_lsn.map_or(read_max_lsn, |max: StrataLsn| max.max(read_max_lsn)));
        }
        entries.extend(read.entries);
    }

    Ok(ActiveDeltaLogRead {
        entries,
        bytes_read,
        end_segment_id: last_segment_id,
        end_offset: last_offset,
        max_lsn,
    })
}

fn read_durable_range_from_file(
    path: &Path,
    segment_id: SegmentId,
    cursor_offset: u64,
    durable_offset: u64,
) -> Result<ActiveDeltaLogRead> {
    if cursor_offset > durable_offset {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: format!(
                "active delta read cursor {} is past durable offset {}",
                cursor_offset, durable_offset
            ),
        });
    }
    if cursor_offset == durable_offset {
        return Ok(ActiveDeltaLogRead {
            entries: Vec::new(),
            bytes_read: 0,
            end_segment_id: segment_id,
            end_offset: cursor_offset,
            max_lsn: None,
        });
    }

    let file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let start_offset = if cursor_offset == 0 {
        read_log_header(&mut reader, path)?;
        reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?
    } else {
        reader
            .seek(SeekFrom::Start(cursor_offset))
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?
    };

    if start_offset > durable_offset {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: format!(
                "active delta read start {} is past durable offset {}",
                start_offset, durable_offset
            ),
        });
    }

    let mut end_offset = start_offset;
    let mut max_lsn = None;
    let mut entries = Vec::new();
    while end_offset < durable_offset {
        let Some(entry) = read_log_entry(&mut reader, path)? else {
            return Err(Error::CorruptRun {
                path: path.to_path_buf(),
                reason: format!("active delta log ended before durable offset {durable_offset}"),
            });
        };
        end_offset = reader.stream_position().map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if end_offset > durable_offset {
            return Err(Error::CorruptRun {
                path: path.to_path_buf(),
                reason: format!(
                    "active delta frame ended at {} past durable offset {}",
                    end_offset, durable_offset
                ),
            });
        }
        max_lsn = Some(max_lsn.map_or(entry.lsn(), |max: StrataLsn| max.max(entry.lsn())));
        entries.push(entry);
    }

    Ok(ActiveDeltaLogRead {
        entries,
        bytes_read: end_offset.saturating_sub(start_offset),
        end_segment_id: segment_id,
        end_offset,
        max_lsn,
    })
}

fn readable_log_segment_ids(
    root_dir: &Path,
    cursor_segment_id: SegmentId,
    durable_position: AccountingLogDurablePosition,
) -> Result<Vec<SegmentId>> {
    let mut ids = active_delta_log_segment_ids(root_dir)?;
    ids.retain(|segment_id| *segment_id <= durable_position.segment_id);
    if cursor_segment_id != 0 {
        ids.retain(|segment_id| *segment_id >= cursor_segment_id);
    }
    if durable_position.segment_id != 0 && !ids.contains(&durable_position.segment_id) {
        ids.insert(durable_position.segment_id);
    }
    Ok(ids.into_iter().collect())
}
