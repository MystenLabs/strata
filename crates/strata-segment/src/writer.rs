use std::{
    fs::{File, OpenOptions},
    io::{IoSlice, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use sha2::{Digest, Sha256};
use strata_core::{BlobKey, EncodedRecordParts, PlacementClass, RecordRef, SegmentId, ShardKey};

use crate::{Error, Result, SegmentIoObserver, error::IoResultExt};

/// Result of appending one record to a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOutcome {
    pub record_ref: RecordRef,
    pub record_len: u64,
}

/// Append-only segment writer.
#[derive(Debug)]
pub struct SegmentWriter {
    path: PathBuf,
    file: File,
    segment_id: SegmentId,
    placement_class: PlacementClass,
    write_offset: u64,
    max_size: u64,
    sealed: bool,
    io_observer: Option<Arc<dyn SegmentIoObserver>>,
    sha256: Option<Sha256>,
}

impl SegmentWriter {
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
    ) -> Result<Self> {
        Self::create_inner(path, segment_id, placement_class, max_size, None, false)
    }

    pub fn create_with_io_observer(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
        io_observer: Arc<dyn SegmentIoObserver>,
    ) -> Result<Self> {
        Self::create_inner(
            path,
            segment_id,
            placement_class,
            max_size,
            Some(io_observer),
            false,
        )
    }

    pub fn create_checksummed_with_io_observer(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
        io_observer: Arc<dyn SegmentIoObserver>,
    ) -> Result<Self> {
        Self::create_inner(
            path,
            segment_id,
            placement_class,
            max_size,
            Some(io_observer),
            true,
        )
    }

    fn create_inner(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
        io_observer: Option<Arc<dyn SegmentIoObserver>>,
        checksum_enabled: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .at_path(&path)?;

        Ok(Self {
            path,
            file,
            segment_id,
            placement_class,
            write_offset: 0,
            max_size,
            sealed: false,
            io_observer,
            sha256: checksum_enabled.then(Sha256::new),
        })
    }

    pub fn open_existing(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
    ) -> Result<Self> {
        Self::open_existing_inner(path, segment_id, placement_class, max_size, None)
    }

    pub fn open_existing_with_io_observer(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
        io_observer: Arc<dyn SegmentIoObserver>,
    ) -> Result<Self> {
        Self::open_existing_inner(
            path,
            segment_id,
            placement_class,
            max_size,
            Some(io_observer),
        )
    }

    fn open_existing_inner(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
        io_observer: Option<Arc<dyn SegmentIoObserver>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .at_path(&path)?;
        let write_offset = file.seek(SeekFrom::End(0)).at_path(&path)?;

        Ok(Self {
            path,
            file,
            segment_id,
            placement_class,
            write_offset,
            max_size,
            sealed: false,
            io_observer,
            sha256: None,
        })
    }

    pub fn append(
        &mut self,
        key: &BlobKey,
        generation: u64,
        payload: &[u8],
    ) -> Result<AppendOutcome> {
        self.append_for_shard(key, generation, strata_core::DEFAULT_RECORD_SHARD, payload)
    }

    pub fn append_for_shard(
        &mut self,
        key: &BlobKey,
        generation: u64,
        shard: ShardKey,
        payload: &[u8],
    ) -> Result<AppendOutcome> {
        if self.sealed {
            return Err(Error::SegmentFull {
                max_size: self.write_offset,
                attempted_size: self.write_offset,
            });
        }

        let encoded_record = EncodedRecordParts::new_with_shard(key, generation, shard, payload)?;
        let record_len = encoded_record.record_len;
        let attempted_size = self
            .write_offset
            .checked_add(record_len)
            .ok_or(Error::RangeOverflow)?;
        if attempted_size > self.max_size {
            self.sealed = true;
            return Err(Error::SegmentFull {
                max_size: self.max_size,
                attempted_size,
            });
        }

        let record_offset = self.write_offset;

        let mut slices = [
            IoSlice::new(&encoded_record.header),
            IoSlice::new(encoded_record.payload),
            IoSlice::new(encoded_record.key),
        ];
        if let Err(write_error) = write_all_vectored(&mut self.file, &mut slices) {
            if let Err(rollback_error) = self.rollback_failed_append(record_offset) {
                return Err(Error::AppendRollbackFailed {
                    path: self.path.clone(),
                    offset: record_offset,
                    write_error,
                    rollback_error,
                });
            }
            return Err(Error::Io {
                path: self.path.clone(),
                source: write_error,
            });
        }
        if let Some(sha256) = &mut self.sha256 {
            sha256.update(&encoded_record.header);
            sha256.update(encoded_record.payload);
            sha256.update(encoded_record.key);
        }
        self.write_offset = attempted_size;
        if let Some(observer) = &self.io_observer {
            observer.record_write(record_len);
        }

        Ok(AppendOutcome {
            record_ref: RecordRef {
                segment_id: self.segment_id,
                offset: record_offset,
                len: record_len,
            },
            record_len,
        })
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data().at_path(&self.path)
    }

    /// Clones the active file descriptor for an asynchronous sync task.
    pub fn clone_file_for_sync(&self) -> Result<File> {
        self.file.try_clone().at_path(&self.path)
    }

    /// Checks whether one complete batch fits without changing writer state.
    pub fn ensure_capacity(&self, additional_bytes: u64) -> Result<()> {
        let attempted_size = self
            .write_offset
            .checked_add(additional_bytes)
            .ok_or(Error::RangeOverflow)?;
        if self.sealed || attempted_size > self.max_size {
            return Err(Error::SegmentFull {
                max_size: self.max_size,
                attempted_size,
            });
        }
        Ok(())
    }

    pub fn seal(&mut self) -> Result<u64> {
        self.sync_data()?;
        self.sealed = true;
        Ok(self.write_offset)
    }

    pub fn seal_with_sha256(&mut self) -> Result<(u64, [u8; 32])> {
        let sealed_len = self.seal()?;
        let sha256 = self.sha256.as_ref().ok_or(Error::ChecksumNotEnabled)?;
        Ok((sealed_len, sha256.clone().finalize().into()))
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    pub fn placement_class(&self) -> PlacementClass {
        self.placement_class
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn rollback_failed_append(&mut self, offset: u64) -> std::io::Result<()> {
        self.file.set_len(offset)?;
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

fn write_all_vectored(file: &mut File, mut bufs: &mut [IoSlice<'_>]) -> std::io::Result<()> {
    while !bufs.is_empty() {
        let written = file.write_vectored(bufs)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Seek, Write},
        sync::Arc,
    };

    use sha2::{Digest, Sha256};
    use strata_core::{BlobKey, PlacementClass};
    use tempfile::tempdir;

    use super::SegmentWriter;
    use crate::{Error, SegmentIoObserver};

    #[derive(Debug)]
    struct NoopIoObserver;

    impl SegmentIoObserver for NoopIoObserver {
        fn record_read(&self, _bytes: u64) {}
        fn record_write(&self, _bytes: u64) {}
    }

    #[test]
    fn rollback_failed_append_truncates_and_repositions_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        writer.append(&key, 0, b"first").unwrap();
        let offset = writer.write_offset();
        writer.file.write_all(b"partial-tail").unwrap();
        assert!(writer.file.metadata().unwrap().len() > offset);

        writer.rollback_failed_append(offset).unwrap();

        assert_eq!(writer.file.metadata().unwrap().len(), offset);
        assert_eq!(writer.file.stream_position().unwrap(), offset);
        let outcome = writer.append(&key, 1, b"second").unwrap();
        assert_eq!(outcome.record_ref.offset, offset);
    }

    #[test]
    fn capacity_check_does_not_change_the_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 10).unwrap();

        assert!(matches!(
            writer.ensure_capacity(11),
            Err(Error::SegmentFull { .. })
        ));
        assert_eq!(writer.write_offset(), 0);
    }

    #[test]
    fn checksummed_writer_returns_digest_without_rereading() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let mut writer = SegmentWriter::create_checksummed_with_io_observer(
            &path,
            1,
            PlacementClass::Ingest,
            1 << 20,
            Arc::new(NoopIoObserver),
        )
        .unwrap();

        writer.append(&key, 0, b"first").unwrap();
        writer.append(&key, 1, b"second").unwrap();
        let (sealed_len, actual) = writer.seal_with_sha256().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let expected: [u8; 32] = Sha256::digest(&bytes).into();

        assert_eq!(sealed_len, bytes.len() as u64);
        assert_eq!(actual, expected);
    }
}
