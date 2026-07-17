use super::*;

impl ActiveDeltaLog {
    pub fn path(root_dir: impl AsRef<Path>, segment_id: SegmentId) -> PathBuf {
        root_dir.as_ref().join(log_file_name(segment_id))
    }

    pub fn open(
        root_dir: impl AsRef<Path>,
        segment_id: SegmentId,
        durable_position: AccountingLogDurablePosition,
    ) -> Result<Self> {
        let path = Self::path(root_dir, segment_id);
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
        // keeps all entries through LSN 41 and discards the incomplete write.
        let recovered = scan_log_prefix(&path)?;

        // RocksDB stores the durable_position after the log has been fsynced, so the
        // normal skew is `durable_position.durable_offset <= recovered.valid_offset`.
        // Equality is a clean shutdown. Less-than means the file has complete
        // frames that metadata did not publish yet; for example, we crashed after
        // fsyncing active-delta.log but before the RocksDB WAL persisted the new
        // accounting-log durable_position. Keep those bytes for the caller to reconcile
        // against the committed store LSN, but do not promote `durable_offset`
        // here because this layer cannot tell whether the extra entries committed.
        //
        // The impossible skew is the opposite direction. If the cursor points past
        // the recovered prefix, the two durability records contradict each other:
        // RocksDB claims bytes are durable that the log file does not contain.
        // Continuing would let accounting-index readers skip missing entries, so treat this
        // as corruption instead of silently rewinding.
        let header_offset = recovered.header_offset;
        let (durable_offset, durable_lsn) = if durable_position.segment_id == segment_id {
            if durable_position.durable_offset > recovered.valid_offset {
                return Err(Error::CorruptRun {
                    path,
                    reason: format!(
                        "accounting log durable offset {} is past recovered offset {}",
                        durable_position.durable_offset, recovered.valid_offset
                    ),
                });
            }
            (
                durable_position.durable_offset,
                durable_position
                    .durable_lsn
                    .min(recovered.max_lsn.unwrap_or(durable_position.durable_lsn)),
            )
        } else if durable_position.segment_id < segment_id {
            (header_offset, durable_position.durable_lsn)
        } else {
            return Err(Error::CorruptRun {
                path,
                reason: format!(
                    "accounting log durable_position for segment {} is ahead of active segment {}",
                    durable_position.segment_id, segment_id
                ),
            });
        };

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
            segment_id,
            path,
            writer: BufWriter::new(file),
            write_offset: recovered.valid_offset,
            max_lsn: recovered.max_lsn,
            durable_offset,
            durable_lsn,
        })
    }

    pub fn position(&self) -> ActiveDeltaLogPosition {
        ActiveDeltaLogPosition {
            segment_id: self.segment_id,
            write_offset: self.write_offset,
            max_lsn: self.max_lsn,
            durable_offset: self.durable_offset,
            durable_lsn: self.durable_lsn,
        }
    }

    pub fn durable_position(&self) -> AccountingLogDurablePosition {
        AccountingLogDurablePosition {
            segment_id: self.segment_id,
            durable_offset: self.durable_offset,
            durable_lsn: self.durable_lsn,
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
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
        durable_position: AccountingLogDurablePosition,
    ) -> Result<ActiveDeltaLogRead> {
        read_durable_range(root_dir.as_ref(), cursor, durable_position)
    }

    pub fn sync_existing(
        root_dir: impl AsRef<Path>,
        segment_id: SegmentId,
    ) -> Result<AccountingLogDurablePosition> {
        let path = Self::path(root_dir, segment_id);
        let recovered = scan_log_prefix(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
        file.sync_data().map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        Ok(AccountingLogDurablePosition {
            segment_id,
            durable_offset: recovered.valid_offset,
            durable_lsn: recovered.max_lsn.unwrap_or_default(),
        })
    }

    pub fn max_lsn_through(
        root_dir: impl AsRef<Path>,
        through_segment_id: SegmentId,
    ) -> Result<Option<StrataLsn>> {
        let mut max_lsn = None;
        for segment_id in active_delta_log_segment_ids(root_dir.as_ref())? {
            if segment_id > through_segment_id {
                continue;
            }
            let path = Self::path(root_dir.as_ref(), segment_id);
            let recovered = scan_log_prefix(&path)?;
            if let Some(segment_max_lsn) = recovered.max_lsn {
                max_lsn = Some(
                    max_lsn.map_or(segment_max_lsn, |max: StrataLsn| max.max(segment_max_lsn)),
                );
            }
        }
        Ok(max_lsn)
    }

    /// Removes closed log segments whose contents have a durable replacement in the accounting index.
    ///
    /// The caller owns the handoff policy: every supplied segment must be strictly behind the
    /// durable consumed cursor, and its matching ingest segment must no longer need sealing. This
    /// layer only intersects the supplied ids with files that actually exist, unlinks them, and
    /// fsyncs the directory so a completed cleanup remains completed after a crash.
    pub fn remove_segments(
        root_dir: impl AsRef<Path>,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<usize> {
        if segment_ids.is_empty() {
            return Ok(0);
        }

        let root_dir = root_dir.as_ref();
        let present = active_delta_log_segment_ids(root_dir)?;
        let targets = present
            .intersection(segment_ids)
            .copied()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Ok(0);
        }

        let mut removed = 0;
        let mut first_error = None;
        for segment_id in &targets {
            let path = Self::path(root_dir, *segment_id);
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    if first_error.is_none() {
                        first_error = Some(Error::Io { path, source });
                    }
                }
            }
        }

        if removed > 0 {
            sync_parent_dir(&Self::path(root_dir, targets[0]))?;
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(removed)
    }

    pub fn append(&mut self, entry: &AccountingLogEntry) -> Result<()> {
        let frame_len = frame_len(entry)?;
        let next_offset = self
            .write_offset
            .checked_add(frame_len)
            .ok_or(Error::RunFrameTooLarge { len: usize::MAX })?;
        write_record_frame(&mut self.writer, entry, &self.path)?;
        self.write_offset = next_offset;
        self.max_lsn = Some(self.max_lsn.map_or(entry.lsn(), |max| max.max(entry.lsn())));
        Ok(())
    }

    pub fn append_all<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a AccountingLogEntry>,
    ) -> Result<()> {
        for entry in entries {
            self.append(entry)?;
        }
        Ok(())
    }

    pub fn rollback_to(&mut self, position: ActiveDeltaLogPosition) -> Result<()> {
        if position.segment_id != self.segment_id {
            return Err(Error::CorruptRun {
                path: self.path.clone(),
                reason: format!(
                    "cannot roll accounting segment {} back to position from segment {}",
                    self.segment_id, position.segment_id
                ),
            });
        }
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

    pub fn flush_for_rollover(&mut self) -> Result<()> {
        self.flush()
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|source| Error::Io {
            path: self.path.clone(),
            source,
        })
    }
}
