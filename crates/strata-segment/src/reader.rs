use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    ops::Range,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use strata_core::{
    BlobKey, DecodedRecord, FIXED_RECORD_HEADER_LEN, RecordHeader, RecordRef, SegmentId,
};

use crate::{Error, Result, error::IoResultExt};

/// Header and key trailer for one record, without payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    pub header: RecordHeader,
    pub key: BlobKey,
}

/// Temporary per-record read timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentReadProfile {
    pub fixed_header: Duration,
    pub buffer_alloc: Duration,
    pub record_body: Duration,
    pub decode: Duration,
}

/// Options for reading and decoding one full segment record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadOptions {
    pub verify_checksum: bool,
}

impl Default for SegmentReadOptions {
    fn default() -> Self {
        Self {
            verify_checksum: true,
        }
    }
}

/// Blocking stream over a payload range in one segment file.
#[derive(Debug)]
pub struct SegmentPayloadStream {
    path: PathBuf,
    file: File,
    remaining: u64,
}

impl SegmentPayloadStream {
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Read for SegmentPayloadStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }

        let max_read = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buf[..max_read])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "segment payload stream ended with {} bytes remaining in {}",
                    self.remaining,
                    self.path.display()
                ),
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

/// Blocking reader for one segment file.
#[derive(Debug)]
pub struct SegmentReader {
    path: PathBuf,
    file: File,
    segment_id: SegmentId,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>, segment_id: SegmentId) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path).at_path(&path)?;
        advise_random_access(&file, &path)?;
        Ok(Self {
            path,
            file,
            segment_id,
        })
    }

    pub fn read_record(&mut self, record_ref: RecordRef) -> Result<DecodedRecord> {
        self.read_record_with_options(record_ref, SegmentReadOptions::default())
    }

    pub fn read_record_with_options(
        &mut self,
        record_ref: RecordRef,
        options: SegmentReadOptions,
    ) -> Result<DecodedRecord> {
        self.read_record_inner(record_ref, options, None)
    }

    pub fn read_record_profiled(
        &mut self,
        record_ref: RecordRef,
    ) -> Result<(DecodedRecord, SegmentReadProfile)> {
        self.read_record_profiled_with_options(record_ref, SegmentReadOptions::default())
    }

    pub fn read_record_profiled_with_options(
        &mut self,
        record_ref: RecordRef,
        options: SegmentReadOptions,
    ) -> Result<(DecodedRecord, SegmentReadProfile)> {
        let mut profile = SegmentReadProfile::default();
        let record = self.read_record_inner(record_ref, options, Some(&mut profile))?;
        Ok((record, profile))
    }

    fn read_record_inner(
        &mut self,
        record_ref: RecordRef,
        options: SegmentReadOptions,
        mut profile: Option<&mut SegmentReadProfile>,
    ) -> Result<DecodedRecord> {
        if record_ref.segment_id != self.segment_id {
            return Err(Error::WrongSegment {
                expected_segment_id: self.segment_id,
                actual_segment_id: record_ref.segment_id,
            });
        }

        let started = Instant::now();
        let (header, fixed_header) = self.read_fixed_header(record_ref)?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.fixed_header = started.elapsed();
        }

        let started = Instant::now();
        let payload_len = usize::try_from(header.payload_len).map_err(|_| Error::RangeOverflow)?;
        let key_len = header.key_len as usize;
        let body_len = payload_len
            .checked_add(key_len)
            .ok_or(Error::RangeOverflow)?;
        let mut body = vec![0; body_len];
        if let Some(profile) = profile.as_deref_mut() {
            profile.buffer_alloc = started.elapsed();
        }

        let started = Instant::now();
        if !body.is_empty() {
            read_exact_at(
                &mut self.file,
                &self.path,
                header.payload_offset(record_ref.offset)?,
                &mut body,
            )?;
        }
        if let Some(profile) = profile.as_deref_mut() {
            profile.record_body = started.elapsed();
        }

        let key = body.split_off(payload_len);
        let payload = body;

        let started = Instant::now();
        let decoded = DecodedRecord::from_parts_with_checksum_verification(
            header,
            &fixed_header,
            payload,
            key,
            options.verify_checksum,
        )?;
        if let Some(profile) = profile {
            profile.decode = started.elapsed();
        }
        Ok(decoded)
    }

    pub fn read_payload(&mut self, record_ref: RecordRef) -> Result<Vec<u8>> {
        let record = self.read_record(record_ref)?;
        Ok(record.payload)
    }

    /// Reads only the requested payload byte range.
    ///
    /// This validates the fixed header, `RecordRef` length, and range bounds. It does not verify
    /// the full-record checksum because the record checksum covers the whole payload and key trailer.
    pub fn read_payload_range(
        &mut self,
        record_ref: RecordRef,
        payload_range: Range<u64>,
    ) -> Result<Vec<u8>> {
        let header = self.read_header(record_ref)?;
        validate_payload_range(header, payload_range.clone())?;
        let range_len = payload_range.end - payload_range.start;
        let mut payload = vec![0; usize::try_from(range_len).map_err(|_| Error::RangeOverflow)?];
        let offset = payload_range_offset(header, record_ref.offset, payload_range.start)?;
        read_exact_at(&mut self.file, &self.path, offset, &mut payload)?;
        Ok(payload)
    }

    pub fn read_record_metadata(&mut self, record_ref: RecordRef) -> Result<RecordMetadata> {
        let header = self.read_header(record_ref)?;
        let key_len = header.key_len as usize;
        let mut key = vec![0; key_len];
        let key_offset = header.key_offset(record_ref.offset)?;
        read_exact_at(&mut self.file, &self.path, key_offset, &mut key)?;
        Ok(RecordMetadata {
            header,
            key: BlobKey::try_from(key).map_err(strata_core::Error::from)?,
        })
    }

    /// Opens a blocking stream over the requested payload byte range.
    ///
    /// The returned stream reads directly from the segment file after the range has been validated.
    pub fn open_payload_stream(
        &mut self,
        record_ref: RecordRef,
        payload_range: Range<u64>,
    ) -> Result<SegmentPayloadStream> {
        let header = self.read_header(record_ref)?;
        validate_payload_range(header, payload_range.clone())?;
        let offset = payload_range_offset(header, record_ref.offset, payload_range.start)?;
        let mut file = self.file.try_clone().at_path(&self.path)?;
        file.seek(SeekFrom::Start(offset)).at_path(&self.path)?;
        Ok(SegmentPayloadStream {
            path: self.path.clone(),
            file,
            remaining: payload_range.end - payload_range.start,
        })
    }

    /// Opens a blocking stream over the requested payload byte range.
    ///
    /// The returned stream reads directly from the segment file after the range has been validated.
    pub fn into_payload_stream(
        mut self,
        record_ref: RecordRef,
        payload_range: Range<u64>,
    ) -> Result<SegmentPayloadStream> {
        let header = self.read_header(record_ref)?;
        validate_payload_range(header, payload_range.clone())?;
        let offset = payload_range_offset(header, record_ref.offset, payload_range.start)?;
        self.file
            .seek(SeekFrom::Start(offset))
            .at_path(&self.path)?;
        Ok(SegmentPayloadStream {
            path: self.path,
            file: self.file,
            remaining: payload_range.end - payload_range.start,
        })
    }

    pub fn read_header(&mut self, record_ref: RecordRef) -> Result<RecordHeader> {
        self.read_fixed_header(record_ref).map(|(header, _)| header)
    }

    fn read_fixed_header(
        &mut self,
        record_ref: RecordRef,
    ) -> Result<(RecordHeader, [u8; FIXED_RECORD_HEADER_LEN])> {
        if record_ref.segment_id != self.segment_id {
            return Err(Error::WrongSegment {
                expected_segment_id: self.segment_id,
                actual_segment_id: record_ref.segment_id,
            });
        }

        let mut fixed = [0; FIXED_RECORD_HEADER_LEN];
        read_exact_at(&mut self.file, &self.path, record_ref.offset, &mut fixed)?;
        let header = DecodedRecord::peek_fixed_header(&fixed)?;
        validate_record_ref_len(record_ref, header)?;
        Ok((header, fixed))
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(target_os = "linux")]
fn advise_random_access(file: &File, path: &Path) -> Result<()> {
    // SAFETY: `file` owns a valid descriptor for the duration of this call. An offset and length
    // of zero apply the advice to the entire file and do not access userspace memory.
    let error = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
    if error != 0 {
        return Err(Error::Io {
            path: path.to_path_buf(),
            source: io::Error::from_raw_os_error(error),
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn advise_random_access(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

fn validate_record_ref_len(record_ref: RecordRef, header: RecordHeader) -> Result<()> {
    let encoded_record_len = header.encoded_record_len()?;
    if record_ref.len != encoded_record_len {
        return Err(Error::InvalidRecordRefLength {
            record_ref_len: record_ref.len,
            encoded_record_len,
        });
    }
    Ok(())
}

fn validate_payload_range(header: RecordHeader, payload_range: Range<u64>) -> Result<()> {
    if payload_range.start > payload_range.end || payload_range.end > header.payload_len {
        return Err(Error::InvalidPayloadRange {
            payload_len: header.payload_len,
            range_start: payload_range.start,
            range_end: payload_range.end,
        });
    }
    Ok(())
}

fn payload_range_offset(
    header: RecordHeader,
    record_offset: u64,
    payload_range_start: u64,
) -> Result<u64> {
    header
        .payload_offset(record_offset)?
        .checked_add(payload_range_start)
        .ok_or(Error::RangeOverflow)
}

#[cfg(unix)]
fn read_exact_at(file: &mut File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<()> {
    let mut read_total = 0;
    while read_total < buf.len() {
        let read = file
            .read_at(&mut buf[read_total..], offset + read_total as u64)
            .at_path(path)?;
        if read == 0 {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: io::ErrorKind::UnexpectedEof.into(),
            });
        }
        read_total += read;
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &mut File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<()> {
    let mut read_total = 0;
    while read_total < buf.len() {
        let read = file
            .seek_read(&mut buf[read_total..], offset + read_total as u64)
            .at_path(path)?;
        if read == 0 {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: io::ErrorKind::UnexpectedEof.into(),
            });
        }
        read_total += read;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &mut File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).at_path(path)?;
    file.read_exact(buf).at_path(path)
}
