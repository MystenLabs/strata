use std::{
    fs::{File, OpenOptions},
    io::{IoSlice, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use strata_core::{BlobKey, EncodedRecordParts, PlacementClass, RecordRef, SegmentId};

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
        if self.sealed {
            return Err(Error::SegmentFull {
                max_size: self.write_offset,
                attempted_size: self.write_offset,
            });
        }

        let encoded_record = EncodedRecordParts::new(key, generation, payload)?;
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
        write_all_vectored(&mut self.file, &mut slices).at_path(&self.path)?;
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
