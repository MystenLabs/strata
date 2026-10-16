//! Disk-backed compaction garbage. The input SSTs remain authoritative until publication, so
//! this staging log needs no recovery: a crash discards it with the other GC staging files.

use std::{fs, path::Path};

use lsm::{GarbageLog, GarbageLogPosition, GarbageRecord, Result};

const FRAME_RECORDS: usize = 16_384;
const FILE_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct GarbageSpool {
    directory: tempfile::TempDir,
    log: GarbageLog,
    records: Vec<GarbageRecord>,
}

impl GarbageSpool {
    pub(super) fn new(namespace_dir: &Path) -> Result<Self> {
        // Store startup removes this staging tree before starting any maintenance workers.
        let root = namespace_dir.join("gc-staging");
        fs::create_dir_all(&root).map_err(|source| lsm::Error::Io {
            path: root.clone(),
            source,
        })?;
        let directory = tempfile::Builder::new()
            .prefix("compaction-")
            .tempdir_in(&root)
            .map_err(|source| lsm::Error::Io { path: root, source })?;
        let log = GarbageLog::open(directory.path(), FILE_BYTES, GarbageLogPosition::default())?;
        Ok(Self {
            directory,
            log,
            records: Vec::new(),
        })
    }

    pub(super) fn push(&mut self, record: GarbageRecord) -> Result<()> {
        // Compaction visits keys in order. Keep a key's events together so sorting each frame
        // cannot reorder two events for the same physical record across frames.
        if self.records.len() >= FRAME_RECORDS
            && self
                .records
                .last()
                .is_some_and(|last| last.key.blob_key != record.key.blob_key)
        {
            self.flush()?;
        }
        self.records.push(record);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.records.is_empty() {
            self.records.sort_unstable_by(GarbageRecord::cmp_position);
            self.log.append_unsynced(&self.records)?;
            self.records.clear();
        }
        Ok(())
    }

    pub(super) fn append_to(
        &mut self,
        destination: &mut GarbageLog,
        mut redirect: impl FnMut(&mut [GarbageRecord]),
    ) -> Result<()> {
        self.flush()?;
        if self
            .log
            .is_at_committed_position(GarbageLogPosition::default())
        {
            return Ok(());
        }
        let mut cursor = GarbageLogPosition::default();
        while let Some((mut records, end)) =
            GarbageLog::read_next(self.directory.path(), cursor, self.log.position())?
        {
            redirect(&mut records);
            destination.append_unsynced(&records)?;
            cursor = end;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{BlobKey, GarbageEvent, RecordRef, SegmentGcSummaryDelta, SegmentKey};

    fn record(key: u64, lsn: u64) -> GarbageRecord {
        GarbageRecord {
            key: SegmentKey {
                segment_id: 1,
                blob_key: BlobKey::new(key.to_be_bytes()).unwrap(),
            },
            lsn,
            event: GarbageEvent::Retired {
                record: RecordRef {
                    segment_id: 1,
                    offset: key,
                    len: 1,
                },
            },
            summary_delta: SegmentGcSummaryDelta::default(),
        }
    }

    #[test]
    fn spools_bounded_frames_without_splitting_a_keys_events() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = GarbageSpool::new(dir.path()).unwrap();
        let keys = FRAME_RECORDS * 3;
        for key in 0..keys as u64 {
            // Deliberately emitted out of LSN order. A frame boundary must not split these.
            for lsn in [3, 1, 2] {
                spool.push(record(key, lsn)).unwrap();
                assert!(spool.records.len() <= FRAME_RECORDS + 2);
            }
        }
        let path = dir.path().join("published");
        let mut log = GarbageLog::open(&path, 1, GarbageLogPosition::default()).unwrap();
        let mut batches = 0;
        spool.append_to(&mut log, |_| batches += 1).unwrap();
        log.sync().unwrap();
        assert!(batches > 1);
        // Each preceding rolled file and the active file survive reopening at the final head.
        let head = log.position();
        drop(log);
        let _log = GarbageLog::open(&path, 1, head).unwrap();
        let mut cursor = GarbageLogPosition::default();
        let mut key = 0;
        while let Some((records, end)) = GarbageLog::read_next(&path, cursor, head).unwrap() {
            assert!(records.len() <= FRAME_RECORDS + 2);
            for group in records.chunks(3) {
                assert_eq!(group, [record(key, 1), record(key, 2), record(key, 3)]);
                key += 1;
            }
            cursor = end;
        }
        assert_eq!(key, keys as u64);
        let staging = spool.directory.path().to_path_buf();
        drop(spool);
        assert!(!staging.exists());
    }

    #[test]
    fn empty_spool_does_not_append_a_frame() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = GarbageSpool::new(dir.path()).unwrap();
        let mut log = GarbageLog::open(
            dir.path().join("published"),
            FILE_BYTES,
            GarbageLogPosition::default(),
        )
        .unwrap();
        spool
            .append_to(&mut log, |_| panic!("empty spool"))
            .unwrap();
        assert!(log.is_at_committed_position(GarbageLogPosition::default()));
    }
}
