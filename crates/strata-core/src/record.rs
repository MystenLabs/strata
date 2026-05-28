use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3Default;

use crate::{BlobKey, Checksum, ChecksumAlgorithm, Error, Generation, Result};

pub const RECORD_MAGIC: u32 = u32::from_le_bytes(*b"STR0");
pub const RECORD_VERSION: u16 = 2;
pub const FIXED_RECORD_HEADER_LEN: usize = 56;
const MAX_PAYLOAD_LEN: u64 = 1 << 40;
const RECORD_CHECKSUM_OFFSET: usize = 36;
const RECORD_CHECKSUM_LEN: usize = 16;
const RECORD_CHECKSUM_ALGORITHM_OFFSET: usize = RECORD_CHECKSUM_OFFSET + RECORD_CHECKSUM_LEN;

/// Logical fields needed to build a Strata record header.
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
    pub record_checksum: Checksum,
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
            record_checksum: Checksum::xxh3_128_value(0),
        })
    }

    /// Encodes the fixed header. Payload and key trailer are not included.
    pub fn encode_fixed(self) -> [u8; FIXED_RECORD_HEADER_LEN] {
        let mut encoded = [0; FIXED_RECORD_HEADER_LEN];
        self.write_fixed(&mut encoded);
        encoded
    }

    pub fn encoded_record_len(self) -> Result<u64> {
        u64::from(self.header_len)
            .checked_add(self.payload_len)
            .and_then(|len| len.checked_add(u64::from(self.key_len)))
            .ok_or(Error::RecordLengthOverflow)
    }

    pub fn header_len_usize(self) -> usize {
        self.header_len as usize
    }

    pub fn payload_offset(self, record_offset: u64) -> Result<u64> {
        record_offset
            .checked_add(u64::from(self.header_len))
            .ok_or(Error::RecordLengthOverflow)
    }

    pub fn key_offset(self, record_offset: u64) -> Result<u64> {
        self.payload_offset(record_offset)?
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
        output[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + RECORD_CHECKSUM_LEN]
            .copy_from_slice(&self.record_checksum.value.to_le_bytes());
        output[RECORD_CHECKSUM_ALGORITHM_OFFSET..RECORD_CHECKSUM_ALGORITHM_OFFSET + 4]
            .copy_from_slice(&self.record_checksum.algorithm.code().to_le_bytes());
    }
}

/// Borrowed encoded record pieces.
///
/// The checksum is computed over `header(xxh3_128_checksum=0) || payload || key`, then patched into the
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

        header.record_checksum =
            record_checksum_parts_for_header(header, &encoded_header, payload, key.as_bytes());
        encoded_header = header.encode_fixed();

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

        let algorithm_code = read_u32(input, RECORD_CHECKSUM_ALGORITHM_OFFSET);
        let algorithm = ChecksumAlgorithm::from_code(algorithm_code)
            .ok_or(Error::UnsupportedChecksumAlgorithm(algorithm_code))?;
        if algorithm != ChecksumAlgorithm::Xxh3_128 {
            return Err(Error::UnsupportedChecksumAlgorithm(algorithm_code));
        }
        let record_checksum = Checksum::new(algorithm, read_u128(input, RECORD_CHECKSUM_OFFSET));

        let header = RecordHeader {
            magic,
            version,
            header_len,
            key_len: read_u32(input, 8),
            logical_end_epoch: read_u64(input, 12),
            generation: read_u64(input, 20),
            payload_len: read_u64(input, 28),
            record_checksum,
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

        let payload_start = header.header_len_usize();
        let payload_end = payload_start + header.payload_len as usize;
        let key_end = payload_end + header.key_len as usize;
        let actual = record_checksum_parts_for_header(
            header,
            &input[..payload_start],
            &input[payload_start..payload_end],
            &input[payload_end..key_end],
        );
        if actual != header.record_checksum {
            return Err(Error::RecordChecksumMismatch {
                expected: header.record_checksum,
                actual,
            });
        }

        Ok(Self {
            header,
            payload: input[payload_start..payload_end].to_vec(),
            key: BlobKey::try_from(&input[payload_end..key_end])?,
        })
    }

    pub fn from_parts(
        header: RecordHeader,
        fixed_header: &[u8; FIXED_RECORD_HEADER_LEN],
        payload: Vec<u8>,
        key: Vec<u8>,
    ) -> Result<Self> {
        Self::from_parts_with_checksum_verification(header, fixed_header, payload, key, true)
    }

    pub fn from_parts_with_checksum_verification(
        header: RecordHeader,
        fixed_header: &[u8; FIXED_RECORD_HEADER_LEN],
        payload: Vec<u8>,
        key: Vec<u8>,
        verify_checksum: bool,
    ) -> Result<Self> {
        let payload_len =
            usize::try_from(header.payload_len).map_err(|_| Error::RecordLengthOverflow)?;
        if payload.len() != payload_len {
            return Err(Error::BufferTooShort {
                needed: payload_len,
                actual: payload.len(),
            });
        }
        if key.len() != header.key_len as usize {
            return Err(Error::BufferTooShort {
                needed: header.key_len as usize,
                actual: key.len(),
            });
        }

        if verify_checksum {
            let actual = record_checksum_parts_for_header(header, fixed_header, &payload, &key);
            if actual != header.record_checksum {
                return Err(Error::RecordChecksumMismatch {
                    expected: header.record_checksum,
                    actual,
                });
            }
        }

        Ok(Self {
            header,
            payload,
            key: BlobKey::try_from(key)?,
        })
    }
}

fn record_checksum_parts_for_header(
    header: RecordHeader,
    fixed_header: &[u8],
    payload: &[u8],
    key: &[u8],
) -> Checksum {
    let mut header_bytes = [0; FIXED_RECORD_HEADER_LEN];
    header_bytes.copy_from_slice(fixed_header);
    header_bytes[RECORD_CHECKSUM_OFFSET..RECORD_CHECKSUM_OFFSET + RECORD_CHECKSUM_LEN].fill(0);
    record_checksum_parts(
        header.record_checksum.algorithm,
        &header_bytes,
        payload,
        key,
    )
}

fn record_checksum_parts(
    algorithm: ChecksumAlgorithm,
    header_with_zero_checksum: &[u8],
    payload: &[u8],
    key: &[u8],
) -> Checksum {
    match algorithm {
        ChecksumAlgorithm::Xxh3_128 => {
            let mut hasher = Xxh3Default::new();
            hasher.update(header_with_zero_checksum);
            hasher.update(payload);
            hasher.update(key);
            Checksum::xxh3_128_value(hasher.digest128())
        }
    }
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

fn read_u128(input: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(input[offset..offset + 16].try_into().expect("slice length"))
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
        assert_eq!(decoded.header.version, RECORD_VERSION);
        assert_eq!(decoded.header.header_len_usize(), FIXED_RECORD_HEADER_LEN);
        assert_eq!(
            decoded.header.record_checksum.algorithm,
            ChecksumAlgorithm::Xxh3_128
        );
    }

    #[test]
    fn round_trips_record_from_parts_without_payload_copy_source() {
        let (key, payload, encoded) = encoded_record();
        let header = DecodedRecord::peek_fixed_header(&encoded).unwrap();
        let fixed_header: &[u8; FIXED_RECORD_HEADER_LEN] = encoded[..FIXED_RECORD_HEADER_LEN]
            .try_into()
            .expect("fixed header length");
        let payload_start = FIXED_RECORD_HEADER_LEN;
        let payload_end = payload_start + header.payload_len as usize;
        let key_end = payload_end + header.key_len as usize;

        let decoded = DecodedRecord::from_parts(
            header,
            fixed_header,
            encoded[payload_start..payload_end].to_vec(),
            encoded[payload_end..key_end].to_vec(),
        )
        .unwrap();

        assert_eq!(decoded.key, key);
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.header, header);
    }

    #[test]
    fn rejects_partial_fixed_header() {
        let (_, _, encoded) = encoded_record();
        let input = &encoded[..FIXED_RECORD_HEADER_LEN - 1];
        assert_eq!(
            DecodedRecord::decode(input),
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
        let header = DecodedRecord::peek_fixed_header(&encoded).unwrap();
        encoded[header.header_len_usize()] ^= 0x01;

        assert!(matches!(
            DecodedRecord::decode(&encoded),
            Err(Error::RecordChecksumMismatch { .. })
        ));
    }

    #[test]
    fn can_decode_parts_without_checksum_verification() {
        let (key, payload, mut encoded) = encoded_record();
        let header = DecodedRecord::peek_fixed_header(&encoded).unwrap();
        encoded[FIXED_RECORD_HEADER_LEN] ^= 0x01;
        let fixed_header: &[u8; FIXED_RECORD_HEADER_LEN] = encoded[..FIXED_RECORD_HEADER_LEN]
            .try_into()
            .expect("fixed header length");
        let payload_start = FIXED_RECORD_HEADER_LEN;
        let payload_end = payload_start + header.payload_len as usize;
        let key_end = payload_end + header.key_len as usize;

        let decoded = DecodedRecord::from_parts_with_checksum_verification(
            header,
            fixed_header,
            encoded[payload_start..payload_end].to_vec(),
            encoded[payload_end..key_end].to_vec(),
            false,
        )
        .unwrap();

        assert_eq!(decoded.key, key);
        assert_ne!(decoded.payload, payload);
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
