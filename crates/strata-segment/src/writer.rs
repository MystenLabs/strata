use std::{
    fs::{File, OpenOptions},
    io::{IoSlice, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use strata_core::{BlobKey, EncodedRecordParts, PlacementClass, RecordRef, SegmentId, ShardKey};

use crate::{Error, Result, error::IoResultExt};

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
}

impl SegmentWriter {
    pub fn create(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
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
        })
    }

    pub fn open_existing(
        path: impl AsRef<Path>,
        segment_id: SegmentId,
        placement_class: PlacementClass,
        max_size: u64,
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
        self.write_offset = attempted_size;

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

    pub fn seal(&mut self) -> Result<u64> {
        self.sync_data()?;
        self.sealed = true;
        Ok(self.write_offset)
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset
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
    use std::io::{Seek, Write};

    use strata_core::{BlobKey, PlacementClass};
    use tempfile::tempdir;

    use super::SegmentWriter;

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
}
