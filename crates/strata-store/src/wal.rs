//! Store-owned rolling write-ahead log.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

use sha2::{Digest, Sha256};

use strata_core::{StrataLsn, WalPosition};

use crate::{Error, Result, file_sync::FileSyncSender, file_sync::FileSyncTask};

const MAGIC: &[u8; 8] = b"STRWAL01";
const VERSION: u32 = 2;
const HEADER_LEN: u64 = 12;
const FRAME_PREFIX_LEN: u64 = 12;
const FRAME_CHECKSUM_LEN: u64 = 32;
const FRAME_OVERHEAD: u64 = FRAME_PREFIX_LEN + FRAME_CHECKSUM_LEN;
const ENTRY_HEADER_LEN: u64 = 12;
const FILE_PREFIX: &str = "wal-";
const FILE_SUFFIX: &str = ".log";

/// One caller-encoded operation persisted in the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub lsn: StrataLsn,
    pub payload: Vec<u8>,
}

/// Rolling WAL writer.
///
/// `committed` must come from the store checkpoint in RocksDB. Opening validates that prefix,
/// truncates later bytes, and removes later WAL files.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    max_file_bytes: u64,
    log_id: u64,
    offset: u64,
    file: File,
    last_lsn: Option<StrataLsn>,
    needs_dir_sync: bool,
    file_sync_tx: FileSyncSender,
    sync_tracker: Arc<WalSyncTracker>,
}

#[derive(Debug)]
struct WalSyncTracker {
    state: Mutex<WalSyncState>,
    changed: Condvar,
}

#[derive(Debug)]
struct WalSyncState {
    committed: WalPosition,
    last_submitted: WalPosition,
    next_ticket: u64,
    next_commit: u64,
    completed: BTreeMap<u64, WalSyncCompletion>,
    failure: Option<String>,
}

#[derive(Debug)]
enum WalSyncCompletion {
    Synced(WalPosition),
    Failed(String),
}

impl Wal {
    /// Validates that recovery can reopen the WAL at `last_lsn` without truncating any files.
    ///
    /// Store-level recovery uses this preflight to distinguish a complete buffered tail, which
    /// may be promoted, from an incomplete unpublished tail, which must be rolled back before the
    /// WAL is opened and truncated.
    pub fn validate_recovery_target(
        dir: impl AsRef<Path>,
        checkpoint: WalPosition,
        published_lsn: Option<StrataLsn>,
        materialized_through: Option<StrataLsn>,
        retained_from: u64,
        last_lsn: Option<StrataLsn>,
    ) -> Result<()> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(|source| io_error(dir, source))?;
        if retained_from == 0 {
            return Err(Error::InvalidWal(
                "first retained WAL file must be non-zero".to_owned(),
            ));
        }
        if retained_from > 1 && materialized_through.is_none() {
            return Err(Error::InvalidWal(
                "a reclaimed WAL prefix needs a materialized frontier".to_owned(),
            ));
        }
        if published_lsn.is_some_and(|published| last_lsn.is_none_or(|last| published > last)) {
            return Err(Error::InvalidWal(format!(
                "published lsn {published_lsn:?} follows recovered store lsn {last_lsn:?}"
            )));
        }
        if materialized_through
            .is_some_and(|materialized| last_lsn.is_none_or(|last| materialized > last))
        {
            return Err(Error::InvalidWal(format!(
                "materialized WAL lsn {materialized_through:?} follows recovered store lsn {last_lsn:?}"
            )));
        }
        if checkpoint != WalPosition::default()
            && retained_from > checkpoint.log_id
            && !matches!(
                (published_lsn, materialized_through),
                (Some(checkpoint), Some(materialized)) if checkpoint <= materialized
            )
        {
            return Err(Error::InvalidWal(
                "retained WAL prefix is not covered by the materialized frontier".to_owned(),
            ));
        }
        let ids = log_ids(dir)?;
        if !ids.is_empty() && !ids.contains(&retained_from) {
            return Err(Error::InvalidWal(format!(
                "first retained WAL file {retained_from} does not exist"
            )));
        }
        if checkpoint == WalPosition::default()
            && published_lsn.is_none()
            && last_lsn.is_some()
            && ids.is_empty()
        {
            if materialized_through
                .zip(last_lsn)
                .is_some_and(|(materialized, last)| materialized >= last)
            {
                // Legacy state may predate the operation WAL only when immutable tables cover the
                // requested store frontier.
                return Ok(());
            }
            return Err(Error::InvalidWal(format!(
                "WAL is missing before recovered store lsn {last_lsn:?}"
            )));
        }
        recover_position(
            dir,
            checkpoint,
            published_lsn,
            materialized_through,
            retained_from,
            last_lsn,
        )
        .map(|_| ())
    }

    /// Reopens the WAL at `last_lsn`, preserving complete frames after the durable checkpoint.
    ///
    /// The checkpoint prefix is validated strictly. Recovery only promotes a tail when it reaches
    /// the exact requested lsn at a frame boundary.
    ///
    /// # Arguments
    ///
    /// - `dir`: directory containing `wal-<log_id>.log` files.
    /// - `max_file_bytes`: soft rollover size for subsequent appends; one complete batch may exceed
    ///   it.
    /// - `checkpoint`: exact WAL file and byte offset covered by the RocksDB store checkpoint.
    /// - `published_lsn`: canonical store `PublishedLsn` atomically associated with
    ///   `checkpoint`, or `None` for an empty WAL. The checkpoint has no separate LSN frontier.
    /// - `materialized_through`: store-wide replay-safe LSN. This is
    ///   `min(blob_lsm.materialized_through, relocation_lsm.materialized_through)`, not either
    ///   projection's frontier by itself. For example, blob=100 and relocation=80 means 80.
    /// - `retained_from`: first WAL file ID that must exist. Files with smaller IDs were
    ///   intentionally reclaimed only after `materialized_through` made them unnecessary.
    /// - `last_lsn`: final globally committed LSN selected by store recovery, normally
    ///   `next_lsn - 1`. Complete frames after `checkpoint` are preserved only through this LSN.
    /// - `file_sync_tx`: store-owned worker queue used by the returned WAL for later syncs.
    #[allow(clippy::too_many_arguments)]
    pub fn recover(
        dir: impl AsRef<Path>,
        max_file_bytes: u64,
        checkpoint: WalPosition,
        published_lsn: Option<StrataLsn>,
        materialized_through: Option<StrataLsn>,
        retained_from: u64,
        last_lsn: Option<StrataLsn>,
        file_sync_tx: FileSyncSender,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(|source| io_error(dir, source))?;
        if retained_from == 0 {
            return Err(Error::InvalidWal(
                "first retained WAL file must be non-zero".to_owned(),
            ));
        }
        if retained_from > 1 && materialized_through.is_none() {
            return Err(Error::InvalidWal(
                "a reclaimed WAL prefix needs a materialized frontier".to_owned(),
            ));
        }
        if published_lsn.is_some_and(|published| last_lsn.is_none_or(|last| published > last)) {
            return Err(Error::InvalidWal(format!(
                "published lsn {published_lsn:?} follows recovered store lsn {last_lsn:?}"
            )));
        }
        if materialized_through
            .is_some_and(|materialized| last_lsn.is_none_or(|last| materialized > last))
        {
            return Err(Error::InvalidWal(format!(
                "materialized WAL lsn {materialized_through:?} follows recovered store lsn {last_lsn:?}"
            )));
        }
        if checkpoint != WalPosition::default()
            && retained_from > checkpoint.log_id
            && !matches!(
                (published_lsn, materialized_through),
                (Some(checkpoint), Some(materialized)) if checkpoint <= materialized
            )
        {
            return Err(Error::InvalidWal(
                "retained WAL prefix is not covered by the materialized frontier".to_owned(),
            ));
        }
        let ids = log_ids(dir)?;
        if !ids.is_empty() && !ids.contains(&retained_from) {
            return Err(Error::InvalidWal(format!(
                "first retained WAL file {retained_from} does not exist"
            )));
        }
        remove_logs_before(dir, retained_from)?;
        if checkpoint == WalPosition::default()
            && published_lsn.is_none()
            && last_lsn.is_some()
            && log_ids(dir)?.is_empty()
        {
            let mut wal = Self::open(dir, max_file_bytes, checkpoint, file_sync_tx)?;
            wal.last_lsn = last_lsn;
            return Ok(wal);
        }
        let recovered = recover_position(
            dir,
            checkpoint,
            published_lsn,
            materialized_through,
            retained_from,
            last_lsn,
        )?;
        if recovered != checkpoint {
            sync_logs_through(dir, recovered)?;
        }
        let mut wal = Self::open(dir, max_file_bytes, recovered, file_sync_tx)?;
        wal.last_lsn = last_lsn;
        Ok(wal)
    }

    pub fn open(
        dir: impl AsRef<Path>,
        max_file_bytes: u64,
        committed: WalPosition,
        file_sync_tx: FileSyncSender,
    ) -> Result<Self> {
        if max_file_bytes == 0 {
            return Err(Error::InvalidWal(
                "maximum WAL file size must be non-zero".to_owned(),
            ));
        }
        validate_position(committed)?;

        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|source| io_error(&dir, source))?;
        let sync_tracker = Arc::new(WalSyncTracker::new(committed));
        if committed == WalPosition::default() {
            remove_logs_after(&dir, 0)?;
            let (file, offset) = create_log(&dir, 1)?;
            return Ok(Self {
                dir,
                max_file_bytes,
                log_id: 1,
                offset,
                file,
                last_lsn: None,
                needs_dir_sync: true,
                file_sync_tx,
                sync_tracker,
            });
        }

        let ids = log_ids(&dir)?;
        let mut expected_id = ids.first().copied().ok_or_else(|| {
            Error::InvalidWal(format!(
                "committed WAL file {} does not exist",
                committed.log_id
            ))
        })?;
        if expected_id > committed.log_id {
            return Err(Error::InvalidWal(format!(
                "committed WAL file {} does not exist",
                committed.log_id
            )));
        }
        let mut last_lsn = None;
        let mut ignore = |_: &WalEntry| Ok(());
        for id in ids.into_iter().take_while(|id| *id <= committed.log_id) {
            if id != expected_id {
                return Err(Error::InvalidWal(format!(
                    "missing WAL file {expected_id} before committed file {}",
                    committed.log_id
                )));
            }
            let path = Self::path(&dir, id);
            let through = if id == committed.log_id {
                committed.offset
            } else {
                File::open(&path)
                    .and_then(|file| file.metadata())
                    .map_err(|source| io_error(&path, source))?
                    .len()
            };
            last_lsn = scan_log(&path, through, last_lsn, &mut ignore)?;
            expected_id = expected_id
                .checked_add(1)
                .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
        }
        if expected_id != committed.log_id.saturating_add(1) {
            return Err(Error::InvalidWal(format!(
                "committed WAL file {} does not exist",
                committed.log_id
            )));
        }

        let path = Self::path(&dir, committed.log_id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        if file
            .metadata()
            .map_err(|source| io_error(&path, source))?
            .len()
            != committed.offset
        {
            file.set_len(committed.offset)
                .map_err(|source| io_error(&path, source))?;
            file.sync_data().map_err(|source| io_error(&path, source))?;
        }
        file.seek(SeekFrom::Start(committed.offset))
            .map_err(|source| io_error(&path, source))?;
        remove_logs_after(&dir, committed.log_id)?;

        Ok(Self {
            dir,
            max_file_bytes,
            log_id: committed.log_id,
            offset: committed.offset,
            file,
            last_lsn,
            needs_dir_sync: false,
            file_sync_tx,
            sync_tracker,
        })
    }

    pub fn path(dir: impl AsRef<Path>, log_id: u64) -> PathBuf {
        dir.as_ref()
            .join(format!("{FILE_PREFIX}{log_id:020}{FILE_SUFFIX}"))
    }

    pub fn position(&self) -> WalPosition {
        WalPosition {
            log_id: self.log_id,
            offset: self.offset,
        }
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.last_lsn
    }

    /// Appends one ordered batch.
    ///
    /// The returned position is not durable until [`Self::committed_position`] reaches it.
    /// Crossing the file-size limit queues the old file for syncing and immediately starts a new
    /// one.
    pub fn append(&mut self, entries: &[WalEntry]) -> Result<WalPosition> {
        validate_append(entries, self.last_lsn)?;
        let frame_len = encoded_frame_len(entries)?;
        if self.offset > HEADER_LEN
            && self
                .offset
                .checked_add(frame_len)
                .is_none_or(|end| end > self.max_file_bytes)
        {
            self.roll()?;
        }
        let new_offset = self
            .offset
            .checked_add(frame_len)
            .ok_or_else(|| Error::InvalidWal("WAL offset overflow".to_owned()))?;

        let path = Self::path(&self.dir, self.log_id);
        let frame_start = self.offset;
        if let Err(write_error) = write_frame(&mut self.file, entries, frame_len) {
            if let Err(rollback_error) = self.rollback(frame_start) {
                return Err(Error::WalAppendRollbackFailed {
                    path,
                    offset: frame_start,
                    write_error,
                    rollback_error,
                });
            }
            return Err(io_error(&path, write_error));
        }
        self.offset = new_offset;
        self.last_lsn = entries.last().map(|entry| entry.lsn);
        Ok(self.position())
    }

    /// Queues a file sync and returns the position its completion will cover.
    ///
    /// The store must first sync any segment bytes referenced by the batch, then publish this
    /// position after [`Self::committed_position`] reaches it.
    pub fn sync(&mut self) -> Result<WalPosition> {
        let path = Self::path(&self.dir, self.log_id);
        let file = self
            .file
            .try_clone()
            .map_err(|source| io_error(&path, source))?;
        let position = self.position();
        let ticket = self.sync_tracker.reserve(position)?;
        let sync_tracker = Arc::clone(&self.sync_tracker);
        let dir = self.needs_dir_sync.then(|| self.dir.clone());
        if self
            .file_sync_tx
            .send(FileSyncTask::new(path, file, move |result| {
                let result = result.and_then(|()| match dir {
                    Some(dir) => sync_dir(&dir),
                    None => Ok(()),
                });
                sync_tracker.complete(ticket, position, result);
            }))
            .is_err()
        {
            self.sync_tracker
                .complete(ticket, position, Err(Error::FileSyncQueueClosed));
            return Err(Error::FileSyncQueueClosed);
        }
        self.needs_dir_sync = false;
        Ok(position)
    }

    /// Latest contiguous position whose queued file syncs have completed.
    #[cfg(test)]
    pub fn committed_position(&self) -> Result<WalPosition> {
        self.sync_tracker.committed()
    }

    /// Waits until `position` is committed or an earlier sync fails.
    pub fn wait_for_sync(&self, position: WalPosition) -> Result<()> {
        self.sync_tracker.wait_for(position)
    }

    /// Replays the current prefix in lsn order.
    pub fn replay(&self, mut apply: impl FnMut(&WalEntry) -> Result<()>) -> Result<()> {
        let ids = log_ids(&self.dir)?;
        let mut expected_id = ids.first().copied().ok_or_else(|| {
            Error::InvalidWal(format!("active WAL file {} does not exist", self.log_id))
        })?;
        let mut last_lsn = None;
        for id in ids.into_iter().take_while(|id| *id <= self.log_id) {
            if id != expected_id {
                return Err(Error::InvalidWal(format!(
                    "missing WAL file {expected_id} before active file {}",
                    self.log_id
                )));
            }
            let path = Self::path(&self.dir, id);
            let through = if id == self.log_id {
                self.offset
            } else {
                File::open(&path)
                    .and_then(|file| file.metadata())
                    .map_err(|source| io_error(&path, source))?
                    .len()
            };
            last_lsn = scan_log(&path, through, last_lsn, &mut apply)?;
            expected_id = expected_id
                .checked_add(1)
                .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
        }
        if expected_id != self.log_id.saturating_add(1) {
            return Err(Error::InvalidWal(format!(
                "active WAL file {} does not exist",
                self.log_id
            )));
        }
        Ok(())
    }

    /// Deletes complete rolled files whose final lsn is materialized in durable SST metadata.
    pub fn reclaim_through(&mut self, materialized: StrataLsn) -> Result<()> {
        let ids = log_ids(&self.dir)?;
        let mut expected_id = ids.first().copied().ok_or_else(|| {
            Error::InvalidWal(format!("active WAL file {} does not exist", self.log_id))
        })?;
        let mut previous = None;
        let mut remove = Vec::new();
        let mut ignore = |_: &WalEntry| Ok(());
        for id in ids.into_iter().take_while(|id| *id < self.log_id) {
            if id != expected_id {
                return Err(Error::InvalidWal(format!(
                    "missing WAL file {expected_id} before active file {}",
                    self.log_id
                )));
            }
            let path = Self::path(&self.dir, id);
            let file_len = File::open(&path)
                .and_then(|file| file.metadata())
                .map_err(|source| io_error(&path, source))?
                .len();
            previous = scan_log(&path, file_len, previous, &mut ignore)?;
            if previous.is_some_and(|lsn| lsn <= materialized) {
                remove.push(path);
            } else {
                break;
            }
            expected_id = expected_id
                .checked_add(1)
                .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
        }
        for path in &remove {
            fs::remove_file(path).map_err(|source| io_error(path, source))?;
        }
        if !remove.is_empty() {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    pub fn retained_from_after(&self, materialized: StrataLsn) -> Result<u64> {
        let ids = log_ids(&self.dir)?;
        let mut expected_id = ids.first().copied().ok_or_else(|| {
            Error::InvalidWal(format!("active WAL file {} does not exist", self.log_id))
        })?;
        let mut previous = None;
        let mut ignore = |_: &WalEntry| Ok(());
        for id in ids.into_iter().take_while(|id| *id < self.log_id) {
            if id != expected_id {
                return Err(Error::InvalidWal(format!(
                    "missing WAL file {expected_id} before active file {}",
                    self.log_id
                )));
            }
            let path = Self::path(&self.dir, id);
            let file_len = File::open(&path)
                .and_then(|file| file.metadata())
                .map_err(|source| io_error(&path, source))?
                .len();
            previous = scan_log(&path, file_len, previous, &mut ignore)?;
            if previous.is_none_or(|lsn| lsn > materialized) {
                return Ok(id);
            }
            expected_id = expected_id
                .checked_add(1)
                .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
        }
        Ok(self.log_id)
    }

    fn roll(&mut self) -> Result<()> {
        self.sync()?;
        let next_id = self
            .log_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
        let (file, offset) = create_log(&self.dir, next_id)?;
        self.file = file;
        self.log_id = next_id;
        self.offset = offset;
        self.needs_dir_sync = true;
        Ok(())
    }

    fn rollback(&mut self, offset: u64) -> std::io::Result<()> {
        self.file.set_len(offset)?;
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

impl WalSyncTracker {
    fn new(committed: WalPosition) -> Self {
        Self {
            state: Mutex::new(WalSyncState {
                committed,
                last_submitted: committed,
                next_ticket: 0,
                next_commit: 0,
                completed: BTreeMap::new(),
                failure: None,
            }),
            changed: Condvar::new(),
        }
    }

    fn reserve(&self, position: WalPosition) -> Result<u64> {
        let mut state = self.state.lock().expect("WAL sync tracker lock poisoned");
        if let Some(failure) = &state.failure {
            return Err(Error::WalSyncFailed(failure.clone()));
        }
        if position < state.last_submitted {
            return Err(Error::InvalidWal(
                "WAL sync positions must not move backwards".to_owned(),
            ));
        }
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| Error::InvalidWal("WAL sync ticket overflow".to_owned()))?;
        state.last_submitted = position;
        Ok(ticket)
    }

    fn complete(&self, ticket: u64, position: WalPosition, result: Result<()>) {
        let completion = match result {
            Ok(()) => WalSyncCompletion::Synced(position),
            Err(error) => WalSyncCompletion::Failed(error.to_string()),
        };
        let mut state = self.state.lock().expect("WAL sync tracker lock poisoned");
        state.completed.insert(ticket, completion);
        loop {
            let next_commit = state.next_commit;
            let Some(completion) = state.completed.remove(&next_commit) else {
                break;
            };
            match completion {
                WalSyncCompletion::Synced(position) => {
                    state.committed = position;
                    state.next_commit += 1;
                }
                WalSyncCompletion::Failed(error) => {
                    state.failure = Some(error);
                    break;
                }
            }
        }
        drop(state);
        self.changed.notify_all();
    }

    #[cfg(test)]
    fn committed(&self) -> Result<WalPosition> {
        let state = self.state.lock().expect("WAL sync tracker lock poisoned");
        match &state.failure {
            Some(failure) => Err(Error::WalSyncFailed(failure.clone())),
            None => Ok(state.committed),
        }
    }

    fn wait_for(&self, position: WalPosition) -> Result<()> {
        let mut state = self.state.lock().expect("WAL sync tracker lock poisoned");
        loop {
            if let Some(failure) = &state.failure {
                return Err(Error::WalSyncFailed(failure.clone()));
            }
            if state.committed >= position {
                return Ok(());
            }
            state = self
                .changed
                .wait(state)
                .expect("WAL sync tracker lock poisoned");
        }
    }
}

fn recover_position(
    dir: &Path,
    checkpoint: WalPosition,
    published_lsn: Option<StrataLsn>,
    materialized_through: Option<StrataLsn>,
    retained_from: u64,
    last_lsn: Option<StrataLsn>,
) -> Result<WalPosition> {
    fs::create_dir_all(dir).map_err(|source| io_error(dir, source))?;
    validate_position(checkpoint)?;
    if published_lsn.is_some_and(|published| last_lsn.is_none_or(|last| published > last)) {
        return Err(Error::InvalidWal(format!(
            "published lsn {published_lsn:?} follows recovered store lsn {last_lsn:?}"
        )));
    }
    if materialized_through
        .is_some_and(|materialized| last_lsn.is_none_or(|last| materialized > last))
    {
        return Err(Error::InvalidWal(format!(
            "materialized WAL lsn {materialized_through:?} follows recovered store lsn {last_lsn:?}"
        )));
    }

    let ids = log_ids(dir)?;
    let missing_checkpoint_is_materialized = match published_lsn {
        Some(lsn) => {
            checkpoint != WalPosition::default()
                && retained_from > checkpoint.log_id
                && materialized_through.is_some_and(|materialized| materialized >= lsn)
        }
        None => {
            checkpoint == WalPosition::default()
                && retained_from > 1
                && materialized_through.is_some()
        }
    };
    let actual_published_lsn = if missing_checkpoint_is_materialized {
        published_lsn
    } else {
        lsn_through_position(dir, checkpoint)?
    };
    let starts_after_materialized_checkpoint = actual_published_lsn.is_none()
        && published_lsn.is_some()
        && checkpoint.offset == HEADER_LEN;
    if actual_published_lsn != published_lsn && !starts_after_materialized_checkpoint {
        return Err(Error::InvalidWal(format!(
            "WAL bytes through the checkpoint end at {actual_published_lsn:?}, expected PublishedLsn {published_lsn:?}"
        )));
    }
    if last_lsn == published_lsn {
        return if missing_checkpoint_is_materialized {
            Ok(WalPosition {
                log_id: retained_from,
                offset: HEADER_LEN,
            })
        } else {
            Ok(checkpoint)
        };
    }
    let target = last_lsn.ok_or_else(|| {
        Error::InvalidWal("WAL recovery target cannot precede its checkpoint".to_owned())
    })?;

    let first_id = if missing_checkpoint_is_materialized {
        retained_from
    } else if checkpoint == WalPosition::default() {
        1
    } else {
        checkpoint.log_id
    };
    let mut expected_id = first_id;
    let mut previous = published_lsn;

    for id in ids.into_iter().filter(|id| *id >= first_id) {
        if id != expected_id {
            return Err(Error::InvalidWal(format!(
                "missing WAL file {expected_id} while recovering through {target:?}"
            )));
        }
        let path = Wal::path(dir, id);
        let mut file = File::open(&path).map_err(|source| io_error(&path, source))?;
        let file_len = file
            .metadata()
            .map_err(|source| io_error(&path, source))?
            .len();
        read_header(&mut file, &path)?;
        let mut offset = if !missing_checkpoint_is_materialized
            && checkpoint != WalPosition::default()
            && id == checkpoint.log_id
        {
            checkpoint.offset
        } else {
            HEADER_LEN
        };

        while offset < file_len {
            file.seek(SeekFrom::Start(offset))
                .map_err(|source| io_error(&path, source))?;
            let (entries, end) = read_frame(&mut file, &path, offset, file_len)?;
            if entries.is_empty() {
                return Err(corrupt(&path, "empty WAL frame"));
            }
            for entry in &entries {
                if previous.is_some_and(|previous| entry.lsn <= previous) {
                    return Err(corrupt(&path, "entries are not strictly ordered by lsn"));
                }
                previous = Some(entry.lsn);
            }

            let frame_last = entries.last().expect("non-empty frame").lsn;
            if frame_last > target {
                let reason = if entries.iter().any(|entry| entry.lsn == target) {
                    format!("recovery target {target:?} is not a WAL frame boundary")
                } else {
                    format!("WAL skips recovery target {target:?}")
                };
                return Err(Error::InvalidWal(reason));
            }
            offset = end;
            if frame_last == target {
                return Ok(WalPosition { log_id: id, offset });
            }
        }

        expected_id = expected_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
    }

    Err(Error::InvalidWal(format!(
        "WAL ends at {previous:?} before recovered store lsn {target:?}"
    )))
}

fn lsn_through_position(dir: &Path, position: WalPosition) -> Result<Option<StrataLsn>> {
    if position == WalPosition::default() {
        return Ok(None);
    }

    let ids = log_ids(dir)?;
    let mut expected_id = ids.first().copied().ok_or_else(|| {
        Error::InvalidWal(format!(
            "checkpoint WAL file {} does not exist",
            position.log_id
        ))
    })?;
    if expected_id > position.log_id {
        return Err(Error::InvalidWal(format!(
            "checkpoint WAL file {} does not exist",
            position.log_id
        )));
    }
    let mut last_lsn = None;
    let mut ignore = |_: &WalEntry| Ok(());
    for id in ids.into_iter().take_while(|id| *id <= position.log_id) {
        if id != expected_id {
            return Err(Error::InvalidWal(format!(
                "missing WAL file {expected_id} before checkpoint file {}",
                position.log_id
            )));
        }
        let path = Wal::path(dir, id);
        let through = if id == position.log_id {
            position.offset
        } else {
            File::open(&path)
                .and_then(|file| file.metadata())
                .map_err(|source| io_error(&path, source))?
                .len()
        };
        last_lsn = scan_log(&path, through, last_lsn, &mut ignore)?;
        expected_id = expected_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidWal("WAL id overflow".to_owned()))?;
    }
    if expected_id != position.log_id.saturating_add(1) {
        return Err(Error::InvalidWal(format!(
            "checkpoint WAL file {} does not exist",
            position.log_id
        )));
    }
    Ok(last_lsn)
}

fn sync_logs_through(dir: &Path, position: WalPosition) -> Result<()> {
    for id in log_ids(dir)?
        .into_iter()
        .take_while(|id| *id <= position.log_id)
    {
        let path = Wal::path(dir, id);
        File::open(&path)
            .and_then(|file| file.sync_data())
            .map_err(|source| io_error(&path, source))?;
    }
    sync_dir(dir)
}

fn validate_position(position: WalPosition) -> Result<()> {
    if position == WalPosition::default() || (position.log_id > 0 && position.offset >= HEADER_LEN)
    {
        Ok(())
    } else {
        Err(Error::InvalidWal(format!(
            "invalid WAL position ({}, {})",
            position.log_id, position.offset
        )))
    }
}

fn validate_append(entries: &[WalEntry], previous: Option<StrataLsn>) -> Result<()> {
    if entries.is_empty() {
        return Err(Error::InvalidWal("cannot append an empty batch".to_owned()));
    }
    let mut previous = previous;
    for entry in entries {
        if let Some(previous) = previous
            && entry.lsn <= previous
        {
            return Err(Error::WalLsnOutOfOrder {
                previous,
                next: entry.lsn,
            });
        }
        previous = Some(entry.lsn);
    }
    Ok(())
}

fn encoded_frame_len(entries: &[WalEntry]) -> Result<u64> {
    u32::try_from(entries.len())
        .map_err(|_| Error::InvalidWal("WAL frame has too many entries".to_owned()))?;
    let payload_len = entries.iter().try_fold(0u64, |len, entry| {
        u32::try_from(entry.payload.len())
            .map_err(|_| Error::InvalidWal("WAL entry is too large".to_owned()))?;
        len.checked_add(ENTRY_HEADER_LEN)
            .and_then(|len| len.checked_add(entry.payload.len() as u64))
            .ok_or_else(|| Error::InvalidWal("WAL frame is too large".to_owned()))
    })?;
    payload_len
        .checked_add(FRAME_OVERHEAD)
        .ok_or_else(|| Error::InvalidWal("WAL frame is too large".to_owned()))
}

fn write_frame(file: &mut File, entries: &[WalEntry], frame_len: u64) -> std::io::Result<()> {
    let payload_len = (frame_len - FRAME_OVERHEAD).to_le_bytes();
    let entry_count = (entries.len() as u32).to_le_bytes();
    let mut checksum = Sha256::new();
    checksum.update(payload_len);
    checksum.update(entry_count);
    file.write_all(&payload_len)?;
    file.write_all(&entry_count)?;

    for entry in entries {
        let mut header = [0; ENTRY_HEADER_LEN as usize];
        header[..8].copy_from_slice(&entry.lsn.to_le_bytes());
        header[8..].copy_from_slice(&(entry.payload.len() as u32).to_le_bytes());
        checksum.update(header);
        checksum.update(&entry.payload);
        file.write_all(&header)?;
        file.write_all(&entry.payload)?;
    }
    file.write_all(&checksum.finalize())
}

fn scan_log(
    path: &Path,
    through: u64,
    mut previous: Option<StrataLsn>,
    apply: &mut dyn FnMut(&WalEntry) -> Result<()>,
) -> Result<Option<StrataLsn>> {
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let file_len = file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len();
    if through < HEADER_LEN || through > file_len {
        return Err(corrupt(path, "committed position is outside the file"));
    }
    read_header(&mut file, path)?;
    let mut offset = HEADER_LEN;
    while offset < through {
        let (entries, end) = read_frame(&mut file, path, offset, through)?;
        if entries.is_empty() {
            return Err(corrupt(path, "empty WAL frame"));
        }
        for entry in &entries {
            if previous.is_some_and(|previous| entry.lsn <= previous) {
                return Err(corrupt(path, "entries are not strictly ordered by lsn"));
            }
            apply(entry)?;
            previous = Some(entry.lsn);
        }
        offset = end;
    }
    if offset != through {
        return Err(corrupt(path, "committed position is not a frame boundary"));
    }
    Ok(previous)
}

fn read_frame(
    file: &mut File,
    path: &Path,
    offset: u64,
    through: u64,
) -> Result<(Vec<WalEntry>, u64)> {
    let mut prefix = [0; FRAME_PREFIX_LEN as usize];
    file.read_exact(&mut prefix)
        .map_err(|_| corrupt(path, "truncated frame prefix"))?;
    let payload_len = u64::from_le_bytes(prefix[..8].try_into().unwrap());
    let entry_count = u32::from_le_bytes(prefix[8..].try_into().unwrap());
    let end = offset
        .checked_add(FRAME_OVERHEAD)
        .and_then(|end| end.checked_add(payload_len))
        .ok_or_else(|| corrupt(path, "frame length overflows"))?;
    if end > through {
        return Err(corrupt(path, "frame extends beyond committed position"));
    }
    if u64::from(entry_count).saturating_mul(ENTRY_HEADER_LEN) > payload_len {
        return Err(corrupt(path, "invalid frame entry count"));
    }

    let mut hasher = Sha256::new();
    hasher.update(prefix);
    let mut remaining = payload_len;
    let mut entries = Vec::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        let mut header = [0; ENTRY_HEADER_LEN as usize];
        file.read_exact(&mut header)
            .map_err(|_| corrupt(path, "truncated entry header"))?;
        hasher.update(header);
        remaining -= ENTRY_HEADER_LEN;

        let payload_len = u32::from_le_bytes(header[8..].try_into().unwrap()) as u64;
        if payload_len > remaining {
            return Err(corrupt(path, "entry extends beyond frame"));
        }
        let mut payload =
            vec![0; usize::try_from(payload_len).map_err(|_| corrupt(path, "entry is too large"))?];
        file.read_exact(&mut payload)
            .map_err(|_| corrupt(path, "truncated entry payload"))?;
        hasher.update(&payload);
        remaining -= payload_len;
        entries.push(WalEntry {
            lsn: u64::from_le_bytes(header[..8].try_into().unwrap()),
            payload,
        });
    }
    if remaining != 0 {
        return Err(corrupt(path, "frame has trailing payload bytes"));
    }

    let mut stored_checksum = [0; FRAME_CHECKSUM_LEN as usize];
    file.read_exact(&mut stored_checksum)
        .map_err(|_| corrupt(path, "truncated frame checksum"))?;
    if stored_checksum != hasher.finalize().as_slice() {
        return Err(corrupt(path, "frame checksum mismatch"));
    }
    Ok((entries, end))
}

fn read_header(reader: &mut impl Read, path: &Path) -> Result<()> {
    let mut header = [0; HEADER_LEN as usize];
    if reader.read_exact(&mut header).is_err()
        || &header[..8] != MAGIC
        || u32::from_le_bytes(header[8..].try_into().unwrap()) != VERSION
    {
        return Err(corrupt(path, "invalid header"));
    }
    Ok(())
}

fn create_log(dir: &Path, log_id: u64) -> Result<(File, u64)> {
    let path = Wal::path(dir, log_id);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    file.write_all(MAGIC)
        .and_then(|_| file.write_all(&VERSION.to_le_bytes()))
        .map_err(|source| io_error(&path, source))?;
    Ok((file, HEADER_LEN))
}

fn log_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| io_error(dir, source))? {
        let entry = entry.map_err(|source| io_error(dir, source))?;
        let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix(FILE_PREFIX))
            .and_then(|name| name.strip_suffix(FILE_SUFFIX))
            .and_then(|id| id.parse().ok())
        else {
            continue;
        };
        ids.push(id);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn remove_logs_after(dir: &Path, log_id: u64) -> Result<()> {
    let mut removed = false;
    for id in log_ids(dir)?
        .into_iter()
        .filter(|candidate| log_id == 0 || *candidate > log_id)
    {
        let path = Wal::path(dir, id);
        fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
        removed = true;
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

fn remove_logs_before(dir: &Path, log_id: u64) -> Result<()> {
    let mut removed = false;
    for id in log_ids(dir)?
        .into_iter()
        .filter(|candidate| *candidate < log_id)
    {
        let path = Wal::path(dir, id);
        fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
        removed = true;
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|source| io_error(dir, source))
}

fn io_error(path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> Error {
    Error::CorruptWal {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Seek, Write},
        thread,
    };

    use tempfile::tempdir;

    use super::*;

    fn entry(sequence: u64, payload: &[u8]) -> WalEntry {
        WalEntry {
            lsn: sequence,
            payload: payload.to_vec(),
        }
    }

    fn open_wal(path: &Path, max_file_bytes: u64, committed: WalPosition) -> Result<Wal> {
        let (file_sync_tx, syncer) = crate::file_sync_channel(8);
        thread::spawn(move || syncer.run());
        Wal::open(path, max_file_bytes, committed, file_sync_tx)
    }

    fn recover_wal(
        path: &Path,
        max_file_bytes: u64,
        checkpoint: WalPosition,
        published_lsn: Option<StrataLsn>,
        last_lsn: Option<StrataLsn>,
    ) -> Result<Wal> {
        let (file_sync_tx, syncer) = crate::file_sync_channel(8);
        thread::spawn(move || syncer.run());
        Wal::recover(
            path,
            max_file_bytes,
            checkpoint,
            published_lsn,
            None,
            1,
            last_lsn,
            file_sync_tx,
        )
    }

    fn replay(wal: &Wal) -> Vec<WalEntry> {
        let mut entries = Vec::new();
        wal.replay(|entry| {
            entries.push(entry.clone());
            Ok(())
        })
        .unwrap();
        entries
    }

    fn sync_wal(wal: &mut Wal) -> WalPosition {
        let position = wal.sync().unwrap();
        wal.wait_for_sync(position).unwrap();
        assert_eq!(wal.committed_position().unwrap(), position);
        position
    }

    #[test]
    fn synced_entries_replay_from_the_committed_position() {
        let dir = tempdir().unwrap();
        let entries = vec![entry(1, b"put"), entry(2, b"")];
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();

        let appended = wal.append(&entries).unwrap();
        assert_eq!(sync_wal(&mut wal), appended);
        drop(wal);

        let wal = open_wal(dir.path(), 1 << 20, appended).unwrap();
        assert_eq!(replay(&wal), entries);
    }

    #[test]
    fn reopen_discards_an_unpublished_tail() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(1, b"committed")]).unwrap();
        let committed = sync_wal(&mut wal);
        wal.append(&[entry(2, b"unpublished")]).unwrap();
        assert!(wal.position().offset > committed.offset);
        drop(wal);

        let wal = open_wal(dir.path(), 1 << 20, committed).unwrap();
        assert_eq!(replay(&wal), vec![entry(1, b"committed")]);
        assert_eq!(
            fs::metadata(Wal::path(dir.path(), committed.log_id))
                .unwrap()
                .len(),
            committed.offset
        );
    }

    #[test]
    fn recovery_promotes_a_complete_tail_through_the_requested_lsn() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(1, b"checkpoint")]).unwrap();
        let checkpoint = sync_wal(&mut wal);
        let recovered = wal.append(&[entry(2, b"recover")]).unwrap();
        drop(wal);

        let wal = recover_wal(dir.path(), 1 << 20, checkpoint, Some(1), Some(2)).unwrap();
        assert_eq!(wal.position(), recovered);
        assert_eq!(wal.committed_position().unwrap(), recovered);
        assert_eq!(wal.last_lsn(), Some(2));
        assert_eq!(
            replay(&wal),
            vec![entry(1, b"checkpoint"), entry(2, b"recover")]
        );
    }

    #[test]
    fn recovery_can_start_a_wal_after_materialized_legacy_state() {
        let dir = tempdir().unwrap();
        let lsn = 7;
        let wal =
            recover_wal(dir.path(), 1 << 20, WalPosition::default(), None, Some(lsn)).unwrap();
        let checkpoint = wal.position();
        assert_eq!(checkpoint.offset, HEADER_LEN);
        assert_eq!(wal.last_lsn(), Some(lsn));
        assert!(replay(&wal).is_empty());
        drop(wal);

        let mut wal = recover_wal(dir.path(), 1 << 20, checkpoint, Some(lsn), Some(lsn)).unwrap();
        assert_eq!(wal.last_lsn(), Some(lsn));
        wal.append(&[entry(8, b"next")]).unwrap();
    }

    #[test]
    fn recovery_discards_complete_frames_after_the_requested_lsn() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(1, b"checkpoint")]).unwrap();
        let checkpoint = sync_wal(&mut wal);
        let recovered = wal.append(&[entry(2, b"recover")]).unwrap();
        wal.append(&[entry(3, b"unpublished")]).unwrap();
        drop(wal);

        let wal = recover_wal(dir.path(), 1 << 20, checkpoint, Some(1), Some(2)).unwrap();
        assert_eq!(wal.position(), recovered);
        assert_eq!(
            replay(&wal),
            vec![entry(1, b"checkpoint"), entry(2, b"recover")]
        );
        assert_eq!(
            fs::metadata(Wal::path(dir.path(), recovered.log_id))
                .unwrap()
                .len(),
            recovered.offset
        );
    }

    #[test]
    fn corruption_inside_the_committed_prefix_is_rejected() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(1, b"alpha")]).unwrap();
        let committed = sync_wal(&mut wal);
        drop(wal);

        let path = Wal::path(dir.path(), committed.log_id);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN + FRAME_PREFIX_LEN))
            .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_data().unwrap();

        assert!(matches!(
            open_wal(dir.path(), 1 << 20, committed),
            Err(Error::CorruptWal { .. })
        ));
    }

    #[test]
    fn rolls_between_batches_and_replays_across_files() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let second = entry(2, b"beta");
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let mut wal = open_wal(dir.path(), max_file_bytes, WalPosition::default()).unwrap();

        assert_eq!(wal.append(std::slice::from_ref(&first)).unwrap().log_id, 1);
        let committed = wal.append(std::slice::from_ref(&second)).unwrap();
        assert_eq!(committed.log_id, 2);
        assert_eq!(sync_wal(&mut wal), committed);
        drop(wal);

        let wal = open_wal(dir.path(), max_file_bytes, committed).unwrap();
        assert_eq!(replay(&wal), vec![first, second]);
    }

    #[test]
    fn recovery_starts_after_a_materialized_wal_prefix() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let second = entry(2, b"beta");
        let first_lsn = first.lsn;
        let second_lsn = second.lsn;
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let mut wal = open_wal(dir.path(), max_file_bytes, WalPosition::default()).unwrap();

        let checkpoint = wal.append(&[first]).unwrap();
        sync_wal(&mut wal);
        let recovered = wal.append(std::slice::from_ref(&second)).unwrap();
        sync_wal(&mut wal);
        wal.reclaim_through(first_lsn).unwrap();
        assert!(!Wal::path(dir.path(), checkpoint.log_id).exists());
        assert!(Wal::path(dir.path(), recovered.log_id).exists());
        drop(wal);

        let (file_sync_tx, syncer) = crate::file_sync_channel(8);
        thread::spawn(move || syncer.run());
        let wal = Wal::recover(
            dir.path(),
            max_file_bytes,
            checkpoint,
            Some(first_lsn),
            Some(first_lsn),
            2,
            Some(second_lsn),
            file_sync_tx,
        )
        .unwrap();
        assert_eq!(wal.position(), recovered);
        assert_eq!(wal.last_lsn(), Some(second_lsn));
        assert_eq!(replay(&wal), vec![second]);
    }

    #[test]
    fn recovery_finishes_reclamation_published_before_delete() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let second = entry(2, b"beta");
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let mut wal = open_wal(dir.path(), max_file_bytes, WalPosition::default()).unwrap();

        let checkpoint = wal.append(std::slice::from_ref(&first)).unwrap();
        sync_wal(&mut wal);
        let recovered = wal.append(std::slice::from_ref(&second)).unwrap();
        sync_wal(&mut wal);
        drop(wal);
        assert!(Wal::path(dir.path(), 1).exists());

        let (file_sync_tx, syncer) = crate::file_sync_channel(8);
        thread::spawn(move || syncer.run());
        let wal = Wal::recover(
            dir.path(),
            max_file_bytes,
            checkpoint,
            Some(first.lsn),
            Some(first.lsn),
            2,
            Some(second.lsn),
            file_sync_tx,
        )
        .unwrap();
        assert_eq!(wal.position(), recovered);
        assert!(!Wal::path(dir.path(), 1).exists());
        assert_eq!(replay(&wal), vec![second]);
    }

    #[test]
    fn recovery_rejects_a_missing_first_retained_wal() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let second = entry(2, b"beta");
        let third = entry(3, b"gamma");
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let mut wal = open_wal(dir.path(), max_file_bytes, WalPosition::default()).unwrap();

        let checkpoint = wal.append(std::slice::from_ref(&first)).unwrap();
        sync_wal(&mut wal);
        wal.append(std::slice::from_ref(&second)).unwrap();
        wal.append(std::slice::from_ref(&third)).unwrap();
        sync_wal(&mut wal);
        wal.reclaim_through(first.lsn).unwrap();
        drop(wal);
        fs::remove_file(Wal::path(dir.path(), 2)).unwrap();

        let (file_sync_tx, syncer) = crate::file_sync_channel(8);
        thread::spawn(move || syncer.run());
        assert!(matches!(
            Wal::recover(
                dir.path(),
                max_file_bytes,
                checkpoint,
                Some(first.lsn),
                Some(first.lsn),
                2,
                Some(third.lsn),
                file_sync_tx,
            ),
            Err(Error::InvalidWal(_))
        ));
    }

    #[test]
    fn reopening_an_older_position_removes_later_rollovers() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let mut wal = open_wal(dir.path(), max_file_bytes, WalPosition::default()).unwrap();

        let committed = wal.append(&[first]).unwrap();
        wal.append(&[entry(2, b"beta")]).unwrap();
        assert_eq!(wal.position().log_id, 2);
        wal.wait_for_sync(committed).unwrap();
        drop(wal);

        let wal = open_wal(dir.path(), max_file_bytes, committed).unwrap();
        assert_eq!(wal.position(), committed);
        assert!(!Wal::path(dir.path(), 2).exists());
    }

    #[test]
    fn rejects_out_of_order_lsns_without_mutation() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(2, b"first")]).unwrap();
        let before = wal.position();

        assert!(matches!(
            wal.append(&[entry(1, b"late")]),
            Err(Error::WalLsnOutOfOrder { .. })
        ));
        assert_eq!(wal.position(), before);
    }

    #[test]
    fn committed_position_must_end_on_a_frame_boundary() {
        let dir = tempdir().unwrap();
        let mut wal = open_wal(dir.path(), 1 << 20, WalPosition::default()).unwrap();
        wal.append(&[entry(1, b"alpha")]).unwrap();
        let committed = sync_wal(&mut wal);
        drop(wal);

        assert!(matches!(
            open_wal(
                dir.path(),
                1 << 20,
                WalPosition {
                    offset: committed.offset - 1,
                    ..committed
                }
            ),
            Err(Error::CorruptWal { .. })
        ));
    }

    #[test]
    fn rollover_queues_sync_without_waiting_for_a_worker() {
        let dir = tempdir().unwrap();
        let first = entry(1, b"alpha");
        let max_file_bytes = HEADER_LEN + encoded_frame_len(std::slice::from_ref(&first)).unwrap();
        let (file_sync_tx, syncer) = crate::file_sync_channel(2);
        let mut wal = Wal::open(
            dir.path(),
            max_file_bytes,
            WalPosition::default(),
            file_sync_tx,
        )
        .unwrap();

        wal.append(&[first]).unwrap();
        wal.append(&[entry(2, b"beta")]).unwrap();
        assert_eq!(wal.position().log_id, 2);
        assert_eq!(wal.committed_position().unwrap(), WalPosition::default());

        let worker = thread::spawn(move || syncer.run());
        let committed = sync_wal(&mut wal);
        drop(wal);
        worker.join().unwrap();
        assert_eq!(committed.log_id, 2);
    }

    #[test]
    fn committed_position_waits_for_out_of_order_completions() {
        let tracker = WalSyncTracker::new(WalPosition::default());
        let first = WalPosition {
            log_id: 1,
            offset: 100,
        };
        let second = WalPosition {
            log_id: 2,
            offset: 80,
        };
        let first_ticket = tracker.reserve(first).unwrap();
        let second_ticket = tracker.reserve(second).unwrap();

        tracker.complete(second_ticket, second, Ok(()));
        assert_eq!(tracker.committed().unwrap(), WalPosition::default());
        tracker.complete(first_ticket, first, Ok(()));
        assert_eq!(tracker.committed().unwrap(), second);
    }
}
