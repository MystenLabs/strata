use serde::{Deserialize, Serialize};

use crate::{BlobKey, Error, Generation, Result};

pub const RECORD_MAGIC: u32 = u32::from_le_bytes(*b"STR0");
pub const RECORD_VERSION: u16 = 1;
pub const FIXED_RECORD_HEADER_LEN: usize = 40;
const MAX_PAYLOAD_LEN: u64 = 1 << 40;
const RECORD_CHECKSUM_OFFSET: usize = 36;

/// Logical fields needed to build a v1 Strata record header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeaderFields {
    pub key_len: u32,
    pub logical_end_epoch: u64,
    pub generation: Generation,
    pub payload_len: u64,
}

/// Fixed header fields for a Strata record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
    pub magic: u32,
    pub version: u16,
    pub header_len: u16,
    pub key_len: u32,
    pub logical_end_epoch: u64,
    pub generation: Generation,
    pub payload_len: u64,
    pub record_checksum: u32,
}

impl RecordHeader {
    pub fn new(fields: RecordHeaderFields) -> Result<Self> {
        validate_lengths(fields.key_len as usize, fields.payload_len)?;
        Ok(Self {
            magic: RECORD_MAGIC,
            version: RECORD_VERSION,
            header_len: FIXED_RECORD_HEADER_LEN as u16,
            key_len: fields.key_len,
            logical_end_epoch: fields.logical_end_epoch,
            generation: fields.generation,
            payload_len: fields.payload_len,
            record_checksum: 0,
        })
    }

    /// Encodes the fixed header. Payload and key trailer are not included.
    pub fn encode_fixed(self) -> [u8; FIXED_RECORD_HEADER_LEN] {
        let mut encoded = [0; FIXED_RECORD_HEADER_LEN];
        self.write_fixed(&mut encoded);
        encoded
    }

    pub fn encoded_record_len(self) -> Result<u64> {
        (FIXED_RECORD_HEADER_LEN as u64)
            .checked_add(self.payload_len)
            .and_then(|len| len.checked_add(u64::from(self.key_len)))
            .ok_or(Error::RecordLengthOverflow)
    }

    pub fn payload_offset(record_offset: u64) -> Result<u64> {
        record_offset
            .checked_add(FIXED_RECORD_HEADER_LEN as u64)
            .ok_or(Error::RecordLengthOverflow)
    }

    pub fn key_offset(self, record_offset: u64) -> Result<u64> {
        Self::payload_offset(record_offset)?
            .checked_add(self.payload_len)
            .ok_or(Error::RecordLengthOverflow)
    }

    fn write_fixed(self, output: &mut [u8; FIXED_RECORD_HEADER_LEN]) {
        output[0..4].copy_from_slice(&self.magic.to_le_bytes());
        output[4..6].copy_from_slice(&self.version.to_le_bytes());
        output[6..8].copy_from_slice(&self.header_len.to_le_bytes());
        output[8..12].copy_from_slice(&self.key_len.to_le_bytes());
        output[12..20].copy_from_slice(&self.logical_end_epoch.to_le_bytes());
        output[20..28].copy_from_slice(&self.generation.to_le_bytes());
        output[28..36].copy_from_slice(&self.payload_len.to_le_bytes());
        output[36..40].copy_from_slice(&self.record_checksum.to_le_bytes());
    }
}

/// Borrowed encoded record pieces.
///
/// The checksum is computed over `header(checksum=0) || payload || key`, then patched into the
/// returned fixed header. This avoids copying large payloads into a temporary full-record buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRecordParts<'a> {
    pub header: [u8; FIXED_RECORD_HEADER_LEN],
    pub payload: &'a [u8],
    pub key: &'a [u8],
    pub record_len: u64,
}

impl<'a> EncodedRecordParts<'a> {
    pub fn new(
        key: &'a BlobKey,
        logical_end_epoch: u64,
        generation: Generation,
        payload: &'a [u8],
    ) -> Result<Self> {
        let mut header = RecordHeader::new(RecordHeaderFields {
            key_len: key.len() as u32,
            logical_end_epoch,
            generation,
            payload_len: payload.len() as u64,
        })?;
        let record_len = header.encoded_record_len()?;
        let mut encoded_header = header.encode_fixed();

        header.record_checksum = record_checksum_parts(&encoded_header, payload, key.as_bytes());
        encoded_header[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4]
            .copy_from_slice(&header.record_checksum.to_le_bytes());

        Ok(Self {
            header: encoded_header,
            payload,
            key: key.as_bytes(),
            record_len,
        })
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(self.record_len as usize);
        encoded.extend_from_slice(&self.header);
        encoded.extend_from_slice(self.payload);
        encoded.extend_from_slice(self.key);
        encoded
    }
}

/// Fully decoded and verified record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
    pub key: BlobKey,
}

impl DecodedRecord {
    pub fn encode(
        key: &BlobKey,
        logical_end_epoch: u64,
        generation: Generation,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        Ok(EncodedRecordParts::new(key, logical_end_epoch, generation, payload)?.to_vec())
    }

    pub fn peek_fixed_header(input: &[u8]) -> Result<RecordHeader> {
        if input.len() < FIXED_RECORD_HEADER_LEN {
            return Err(Error::BufferTooShort {
                needed: FIXED_RECORD_HEADER_LEN,
                actual: input.len(),
            });
        }

        let magic = read_u32(input, 0);
        if magic != RECORD_MAGIC {
            return Err(Error::InvalidMagic {
                expected: RECORD_MAGIC,
                actual: magic,
            });
        }

        let version = read_u16(input, 4);
        if version != RECORD_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }

        let header_len = read_u16(input, 6);
        if header_len as usize != FIXED_RECORD_HEADER_LEN {
            return Err(Error::InvalidHeaderLength {
                expected: FIXED_RECORD_HEADER_LEN as u16,
                actual: header_len,
            });
        }

        let header = RecordHeader {
            magic,
            version,
            header_len,
            key_len: read_u32(input, 8),
            logical_end_epoch: read_u64(input, 12),
            generation: read_u64(input, 20),
            payload_len: read_u64(input, 28),
            record_checksum: read_u32(input, RECORD_CHECKSUM_OFFSET),
        };
        validate_lengths(header.key_len as usize, header.payload_len)?;
        Ok(header)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        let header = Self::peek_fixed_header(input)?;
        let record_len = header.encoded_record_len()? as usize;
        if input.len() < record_len {
            return Err(Error::BufferTooShort {
                needed: record_len,
                actual: input.len(),
            });
        }

        let actual = record_checksum(&input[..record_len]);
        if actual != header.record_checksum {
            return Err(Error::RecordChecksumMismatch {
                expected: header.record_checksum,
                actual,
            });
        }

        let payload_start = FIXED_RECORD_HEADER_LEN;
        let payload_end = payload_start + header.payload_len as usize;
        let key_end = payload_end + header.key_len as usize;
        Ok(Self {
            header,
            payload: input[payload_start..payload_end].to_vec(),
            key: BlobKey::try_from(&input[payload_end..key_end])?,
        })
    }
}

fn record_checksum(record: &[u8]) -> u32 {
    let mut header = [0; FIXED_RECORD_HEADER_LEN];
    header.copy_from_slice(&record[..FIXED_RECORD_HEADER_LEN]);
    header[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + 4].fill(0);

    let payload_start = FIXED_RECORD_HEADER_LEN;
    let header_for_lengths = DecodedRecord::peek_fixed_header(record)
        .expect("record checksum is only called after fixed header validation");
    let payload_end = payload_start + header_for_lengths.payload_len as usize;
    let key_end = payload_end + header_for_lengths.key_len as usize;
    record_checksum_parts(
        &header,
        &record[payload_start..payload_end],
        &record[payload_end..key_end],
    )
}

fn record_checksum_parts(header_with_zero_checksum: &[u8], payload: &[u8], key: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header_with_zero_checksum);
    hasher.update(payload);
    hasher.update(key);
    hasher.finalize()
}

fn validate_lengths(key_len: usize, payload_len: u64) -> Result<()> {
    if key_len > crate::key::MAX_BLOB_KEY_LEN {
        return Err(Error::KeyTooLarge(key_len as u32));
    }
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(Error::PayloadTooLarge(payload_len));
    }
    Ok(())
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(input[offset..offset + 2].try_into().expect("slice length"))
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().expect("slice length"))
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().expect("slice length"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_record() -> (BlobKey, Vec<u8>, Vec<u8>) {
        let key = BlobKey::new(b"blob-1".to_vec()).unwrap();
        let payload = b"payload bytes".to_vec();
        let encoded = DecodedRecord::encode(&key, 42, 7, &payload).unwrap();
        (key, payload, encoded)
    }

    #[test]
    fn round_trips_record() {
        let (key, payload, encoded) = encoded_record();
        let decoded = DecodedRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.key, key);
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.header.logical_end_epoch, 42);
        assert_eq!(decoded.header.generation, 7);
        assert_eq!(decoded.header.payload_len, payload.len() as u64);
        assert_eq!(decoded.header.key_len, key.len() as u32);
    }

    #[test]
    fn rejects_partial_fixed_header() {
        let input = vec![0; FIXED_RECORD_HEADER_LEN - 1];
        assert_eq!(
            DecodedRecord::decode(&input),
            Err(Error::BufferTooShort {
                needed: FIXED_RECORD_HEADER_LEN,
                actual: FIXED_RECORD_HEADER_LEN - 1,
            })
        );
    }

    #[test]
    fn rejects_bad_magic() {
        let (_, _, mut encoded) = encoded_record();
        encoded[0] = 0;

        assert!(matches!(
            DecodedRecord::decode(&encoded),
            Err(Error::InvalidMagic { .. })
        ));
    }

    #[test]
    fn rejects_corrupt_header() {
        let (_, _, mut encoded) = encoded_record();
        encoded[12] ^= 0x01;

        assert!(matches!(
            DecodedRecord::decode(&encoded),
            Err(Error::RecordChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_corrupt_payload() {
        let (_, _, mut encoded) = encoded_record();
        encoded[FIXED_RECORD_HEADER_LEN] ^= 0x01;

        assert!(matches!(
            DecodedRecord::decode(&encoded),
            Err(Error::RecordChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_corrupt_key_trailer() {
        let (_, _, mut encoded) = encoded_record();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x01;

        assert!(matches!(
            DecodedRecord::decode(&encoded),
            Err(Error::RecordChecksumMismatch { .. })
        ));
    }
}
