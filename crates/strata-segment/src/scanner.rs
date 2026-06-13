use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use strata_core::{
    BlobKey, DecodedRecord, FIXED_RECORD_HEADER_LEN, RecordHeader, RecordRef, SegmentId,
};

use crate::{Result, error::IoResultExt};

/// One valid record found while scanning a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRecord {
    pub key: BlobKey,
    pub header: RecordHeader,
    pub record_ref: RecordRef,
    pub record_len: u64,
}

/// Valid prefix discovered by a recovery scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidPrefix {
    pub records: Vec<ScannedRecord>,
    pub valid_len: u64,
    pub file_len: u64,
}

/// Sequential scanner for recovery and index rebuild.
#[derive(Debug)]
pub struct SegmentScanner {
    path: PathBuf,
    file: File,
    segment_id: SegmentId,
}

impl SegmentScanner {
    pub fn open(path: impl AsRef<Path>, segment_id: SegmentId) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path).at_path(&path)?;
        Ok(Self {
            path,
            file,
            segment_id,
        })
    }

    /// Scans valid complete records from the start of the segment.
    ///
    /// A partial fixed header, payload, or key trailer at the tail stops the scan and returns the valid
    /// prefix. Corrupt complete records still return an error.
    pub fn scan_valid_prefix(&mut self) -> Result<ValidPrefix> {
        self.scan_valid_prefix_inner(None)
    }

    /// Scans the prefix that can be used after a crash.
    ///
    /// Corruption or partial records beyond `durable_offset` stop the scan and define the recovered
    /// prefix. Damage before `durable_offset` remains an error because the index has already marked
    /// those bytes durable.
    pub fn scan_recoverable_prefix(&mut self, durable_offset: u64) -> Result<ValidPrefix> {
        let prefix = self.scan_valid_prefix_inner(Some(durable_offset))?;
        if prefix.valid_len < durable_offset {
            return Err(crate::Error::InvalidDurableOffset {
                path: self.path.clone(),
                durable_offset,
                valid_len: prefix.valid_len,
            });
        }
        Ok(prefix)
    }

    fn scan_valid_prefix_inner(&mut self, durable_offset: Option<u64>) -> Result<ValidPrefix> {
        let file_len = self.file.metadata().at_path(&self.path)?.len();
        let mut offset = 0;
        let mut records = Vec::new();

        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < FIXED_RECORD_HEADER_LEN as u64 {
                break;
            }

            let mut fixed = vec![0; FIXED_RECORD_HEADER_LEN];
            self.file
                .seek(SeekFrom::Start(offset))
                .at_path(&self.path)?;
            self.file.read_exact(&mut fixed).at_path(&self.path)?;

            let header = match DecodedRecord::peek_fixed_header(&fixed) {
                Ok(header) => header,
                Err(_) if can_stop_recovery_scan(durable_offset, offset) => break,
                Err(error) => return Err(error.into()),
            };
            let record_len = match header.encoded_record_len() {
                Ok(record_len) => record_len,
                Err(_) if can_stop_recovery_scan(durable_offset, offset) => break,
                Err(error) => return Err(error.into()),
            };
            if remaining < record_len {
                break;
            }

            let mut encoded_record = fixed;
            encoded_record.resize(record_len as usize, 0);
            self.file
                .read_exact(&mut encoded_record[FIXED_RECORD_HEADER_LEN..])
                .at_path(&self.path)?;
            let decoded = match DecodedRecord::decode(&encoded_record) {
                Ok(decoded) => decoded,
                Err(_) if can_stop_recovery_scan(durable_offset, offset) => break,
                Err(error) => return Err(error.into()),
            };

            records.push(ScannedRecord {
                key: decoded.key,
                header: decoded.header,
                record_ref: RecordRef {
                    segment_id: self.segment_id,
                    offset,
                    len: record_len,
                },
                record_len,
            });
            offset = offset
                .checked_add(record_len)
                .ok_or(crate::Error::RangeOverflow)?;
        }

        Ok(ValidPrefix {
            records,
            valid_len: offset,
            file_len,
        })
    }
}

fn can_stop_recovery_scan(durable_offset: Option<u64>, offset: u64) -> bool {
    durable_offset.is_some_and(|durable_offset| offset >= durable_offset)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use strata_core::PlacementClass;
    use tempfile::tempdir;

    use super::*;
    use crate::{SegmentReader, SegmentWriter};

    #[test]
    fn appends_and_reads_one_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        let outcome = writer.append(&key, 0, payload).unwrap();
        writer.seal().unwrap();

        let mut reader = SegmentReader::open(&path, 1).unwrap();
        assert_eq!(reader.read_payload(outcome.record_ref).unwrap(), payload);
    }

    #[test]
    fn reads_payload_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        let outcome = writer.append(&key, 0, payload).unwrap();
        writer.seal().unwrap();

        let mut reader = SegmentReader::open(&path, 1).unwrap();
        assert_eq!(
            reader
                .read_payload_range(outcome.record_ref, 6..12)
                .unwrap(),
            b"strata"
        );
    }

    #[test]
    fn streams_payload_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        let outcome = writer.append(&key, 0, payload).unwrap();
        writer.seal().unwrap();

        let reader = SegmentReader::open(&path, 1).unwrap();
        let mut stream = reader
            .into_payload_stream(outcome.record_ref, 0..5)
            .unwrap();
        let mut read = Vec::new();
        stream.read_to_end(&mut read).unwrap();

        assert_eq!(read, b"hello");
        assert_eq!(stream.remaining(), 0);
    }

    #[test]
    fn range_read_rejects_out_of_bounds_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        let outcome = writer.append(&key, 0, payload).unwrap();
        writer.seal().unwrap();

        let mut reader = SegmentReader::open(&path, 1).unwrap();
        let err = reader
            .read_payload_range(outcome.record_ref, 0..(payload.len() as u64 + 1))
            .unwrap_err();

        assert!(matches!(err, crate::Error::InvalidPayloadRange { .. }));
    }

    #[test]
    fn read_record_rejects_record_ref_length_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();

        let outcome = writer.append(&key, 0, payload).unwrap();
        writer.seal().unwrap();

        let mut bad_ref = outcome.record_ref;
        bad_ref.len -= 1;
        let mut reader = SegmentReader::open(&path, 1).unwrap();
        let err = reader.read_record(bad_ref).unwrap_err();

        assert!(matches!(err, crate::Error::InvalidRecordRefLength { .. }));
    }

    #[test]
    fn scans_many_records_and_returns_record_refs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let mut writer = SegmentWriter::create(&path, 7, PlacementClass::Ingest, 1 << 20).unwrap();

        let mut expected = Vec::new();
        for i in 0..10 {
            let key = BlobKey::new(format!("key-{i}").into_bytes()).unwrap();
            let payload = format!("payload-{i}").into_bytes();
            let outcome = writer.append(&key, i, &payload).unwrap();
            expected.push((key, payload, outcome.record_ref));
        }
        writer.seal().unwrap();

        let mut scanner = SegmentScanner::open(&path, 7).unwrap();
        let prefix = scanner.scan_valid_prefix().unwrap();

        assert_eq!(prefix.records.len(), expected.len());
        assert_eq!(prefix.valid_len, prefix.file_len);
        let mut reader = SegmentReader::open(&path, 7).unwrap();
        for (scanned, (key, payload, record_ref)) in prefix.records.iter().zip(expected) {
            assert_eq!(scanned.key, key);
            assert_eq!(scanned.record_ref, record_ref);
            assert_eq!(reader.read_payload(scanned.record_ref).unwrap(), payload);
        }
    }

    #[test]
    fn scan_stops_at_partial_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
        writer.append(&key, 0, payload).unwrap();
        let valid_len = writer.write_offset();
        drop(writer);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);

        let mut scanner = SegmentScanner::open(&path, 1).unwrap();
        let prefix = scanner.scan_valid_prefix().unwrap();

        assert_eq!(prefix.records.len(), 1);
        assert_eq!(prefix.valid_len, valid_len);
        assert!(prefix.file_len > prefix.valid_len);
    }

    #[test]
    fn scan_detects_payload_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let payload = b"hello strata";
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
        let outcome = writer.append(&key, 0, payload).unwrap();
        drop(writer);

        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let payload_offset = outcome.record_ref.offset + FIXED_RECORD_HEADER_LEN as u64;
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(b"X").unwrap();
        drop(file);

        let mut scanner = SegmentScanner::open(&path, 1).unwrap();
        assert!(scanner.scan_valid_prefix().is_err());
    }

    #[test]
    fn append_reports_segment_full() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000001.data");
        let key = BlobKey::new(b"alpha".to_vec()).unwrap();
        let mut writer = SegmentWriter::create(&path, 1, PlacementClass::Ingest, 10).unwrap();

        assert!(writer.append(&key, 0, b"hello").is_err());
    }
}
