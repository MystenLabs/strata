//! Byte-level encoding, decoding, and structural validation for immutable SST files.

use std::path::Path;

use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::{Error, FORMAT_VERSION, Result, StrataLsn};

pub(crate) const FILE_MAGIC: &[u8; 8] = b"STRLSM01";
pub(crate) const INDEX_MAGIC: &[u8; 8] = b"STRIDX01";
pub(crate) const BLOOM_MAGIC: &[u8; 8] = b"STRBLM01";
pub(crate) const PREFIX_INDEX_MAGIC: &[u8; 8] = b"STRPFXI1";
pub(crate) const PREFIX_BLOOM_MAGIC: &[u8; 8] = b"STRPFXB1";
const FOOTER_MAGIC: &[u8; 8] = b"STRFTR01";
pub(crate) const TRAILER_MAGIC: &[u8; 8] = b"STREND01";
pub(crate) const HEADER_FIXED_LEN: usize = 32;
pub(crate) const TRAILER_LEN: usize = 48;
pub(crate) const CHECKSUM_LEN: usize = 32;
const DATA_BLOCK_HEADER_LEN: usize = 12;
pub(crate) const DATA_BLOCK_OVERHEAD: usize = DATA_BLOCK_HEADER_LEN + CHECKSUM_LEN;
const FOOTER_FIXED_LEN: usize = 112;
pub(crate) const MAX_FORMAT_ID_BYTES: usize = 1024;
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub(crate) const PREFIX_BLOOM_SEED: u64 = 0xbb67_ae85_84ca_a73b;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableKind {
    Base = 0,
    Patch = 1,
}

impl TableKind {
    fn from_byte(byte: u8, path: &Path) -> Result<Self> {
        match byte {
            0 => Ok(Self::Base),
            1 => Ok(Self::Patch),
            _ => Err(corrupt(path, format!("unknown table kind {byte}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockEncoding {
    Plain = 0,
    Prefix = 1,
}

impl BlockEncoding {
    fn from_byte(byte: u8, path: &Path) -> Result<Self> {
        match byte {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Prefix),
            _ => Err(corrupt(path, format!("unknown block encoding {byte}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockMeta {
    pub(crate) encoding: BlockEncoding,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) key_prefix: Vec<u8>,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Footer {
    pub(crate) index: BlockHandle,
    pub(crate) full_bloom: BlockHandle,
    pub(crate) prefix_index: BlockHandle,
    pub(crate) prefix_bloom: BlockHandle,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) min_lsn: Option<StrataLsn>,
    pub(crate) max_lsn: Option<StrataLsn>,
    pub(crate) record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BloomFilter {
    pub(crate) bits: Vec<u8>,
    pub(crate) bit_count: u64,
    pub(crate) hash_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixIndexEntry {
    pub(crate) key_prefix: Vec<u8>,
    pub(crate) block: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) kind: TableKind,
    pub(crate) id: u64,
    pub(crate) partition: u32,
    pub(crate) format_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub(crate) footer_len: u64,
    pub(crate) footer_checksum: [u8; CHECKSUM_LEN],
}

/// The two independent hashes of one key that derive every Bloom probe: probe `i` tests bit
/// `first + i * second` (classic double hashing), so a filter with any `hash_count` needs
/// only this pair per key.
pub(crate) type BloomHashPair = (u64, u64);

pub(crate) fn encode_header(
    kind: TableKind,
    id: u64,
    partition: u32,
    format_id: &str,
) -> Result<Vec<u8>> {
    let format_len = u32::try_from(format_id.len())
        .map_err(|_| Error::InvalidTable("value format identifier is too long".to_owned()))?;
    let mut header = Vec::with_capacity(HEADER_FIXED_LEN + format_id.len());
    header.extend_from_slice(FILE_MAGIC);
    header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    header.push(kind as u8);
    header.extend_from_slice(&[0; 3]);
    header.extend_from_slice(&id.to_le_bytes());
    header.extend_from_slice(&partition.to_le_bytes());
    header.extend_from_slice(&format_len.to_le_bytes());
    header.extend_from_slice(format_id.as_bytes());
    Ok(header)
}

pub(crate) fn decode_header(bytes: &[u8], path: &Path) -> Result<Header> {
    if bytes.len() != HEADER_FIXED_LEN || &bytes[..8] != FILE_MAGIC {
        return Err(corrupt(path, "invalid file header"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed header slice"));
    if version != FORMAT_VERSION {
        return Err(corrupt(
            path,
            format!("format version {version} is not {FORMAT_VERSION}"),
        ));
    }
    if bytes[13..16] != [0; 3] {
        return Err(corrupt(path, "non-zero reserved header bytes"));
    }
    let kind = TableKind::from_byte(bytes[12], path)?;
    let id = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed header slice"));
    let partition = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed header slice"));
    let format_len =
        u32::from_le_bytes(bytes[28..32].try_into().expect("fixed header slice")) as usize;
    if format_len == 0 || format_len > MAX_FORMAT_ID_BYTES {
        return Err(corrupt(path, "invalid value format identifier length"));
    }
    Ok(Header {
        kind,
        id,
        partition,
        format_len,
    })
}

pub(crate) fn decode_trailer(bytes: &[u8], path: &Path) -> Result<Trailer> {
    if bytes.len() != TRAILER_LEN || &bytes[40..] != TRAILER_MAGIC {
        return Err(corrupt(path, "invalid trailer magic"));
    }
    Ok(Trailer {
        footer_len: u64::from_le_bytes(bytes[..8].try_into().expect("trailer slice")),
        footer_checksum: bytes[8..40].try_into().expect("trailer slice"),
    })
}

pub(crate) fn encode_trailer(
    footer_len: u64,
    footer_checksum: [u8; CHECKSUM_LEN],
) -> [u8; TRAILER_LEN] {
    let mut trailer = [0; TRAILER_LEN];
    trailer[..8].copy_from_slice(&footer_len.to_le_bytes());
    trailer[8..40].copy_from_slice(&footer_checksum);
    trailer[40..].copy_from_slice(TRAILER_MAGIC);
    trailer
}

pub(crate) fn encode_footer(footer: &Footer) -> Result<Vec<u8>> {
    let first_key_len = u32::try_from(footer.first_key.len())
        .map_err(|_| Error::InvalidTable("footer key exceeds u32 length".to_owned()))?;
    let last_key_len = u32::try_from(footer.last_key.len())
        .map_err(|_| Error::InvalidTable("footer key exceeds u32 length".to_owned()))?;
    let (has_lsns, min_lsn, max_lsn) = match (footer.min_lsn, footer.max_lsn) {
        (None, None) => (0, 0, 0),
        (Some(min_lsn), Some(max_lsn)) => (1, min_lsn, max_lsn),
        _ => {
            return Err(Error::InvalidTable(
                "footer must contain both lsn bounds or neither".to_owned(),
            ));
        }
    };

    let mut encoded =
        Vec::with_capacity(FOOTER_FIXED_LEN + footer.first_key.len() + footer.last_key.len());
    encoded.extend_from_slice(FOOTER_MAGIC);
    encoded.extend_from_slice(&footer.index.offset.to_le_bytes());
    encoded.extend_from_slice(&footer.index.len.to_le_bytes());
    encoded.extend_from_slice(&footer.full_bloom.offset.to_le_bytes());
    encoded.extend_from_slice(&footer.full_bloom.len.to_le_bytes());
    encoded.extend_from_slice(&footer.prefix_index.offset.to_le_bytes());
    encoded.extend_from_slice(&footer.prefix_index.len.to_le_bytes());
    encoded.extend_from_slice(&footer.prefix_bloom.offset.to_le_bytes());
    encoded.extend_from_slice(&footer.prefix_bloom.len.to_le_bytes());
    encoded.extend_from_slice(&footer.record_count.to_le_bytes());
    encoded.push(has_lsns);
    encoded.extend_from_slice(&[0; 7]);
    encoded.extend_from_slice(&min_lsn.to_le_bytes());
    encoded.extend_from_slice(&max_lsn.to_le_bytes());
    encoded.extend_from_slice(&first_key_len.to_le_bytes());
    encoded.extend_from_slice(&last_key_len.to_le_bytes());
    encoded.extend_from_slice(&footer.first_key);
    encoded.extend_from_slice(&footer.last_key);
    Ok(encoded)
}

pub(crate) fn decode_footer(bytes: &[u8], path: &Path) -> Result<Footer> {
    if bytes.len() < FOOTER_FIXED_LEN || &bytes[..FOOTER_MAGIC.len()] != FOOTER_MAGIC {
        return Err(corrupt(path, "invalid footer header"));
    }
    let mut cursor = FOOTER_MAGIC.len();
    let index = BlockHandle {
        offset: take_u64(bytes, &mut cursor, path)?,
        len: take_u64(bytes, &mut cursor, path)?,
    };
    let full_bloom = BlockHandle {
        offset: take_u64(bytes, &mut cursor, path)?,
        len: take_u64(bytes, &mut cursor, path)?,
    };
    let prefix_index = BlockHandle {
        offset: take_u64(bytes, &mut cursor, path)?,
        len: take_u64(bytes, &mut cursor, path)?,
    };
    let prefix_bloom = BlockHandle {
        offset: take_u64(bytes, &mut cursor, path)?,
        len: take_u64(bytes, &mut cursor, path)?,
    };
    let record_count = take_u64(bytes, &mut cursor, path)?;
    let has_lsns = take(bytes, &mut cursor, 1, path)?[0];
    if take(bytes, &mut cursor, 7, path)? != [0; 7] {
        return Err(corrupt(path, "non-zero reserved footer bytes"));
    }
    let min_lsn = take_u64(bytes, &mut cursor, path)?;
    let max_lsn = take_u64(bytes, &mut cursor, path)?;
    let first_key_len = take_u32(bytes, &mut cursor, path)? as usize;
    let last_key_len = take_u32(bytes, &mut cursor, path)? as usize;
    let first_key = take(bytes, &mut cursor, first_key_len, path)?.to_vec();
    let last_key = take(bytes, &mut cursor, last_key_len, path)?.to_vec();
    if cursor != bytes.len() {
        return Err(corrupt(path, "trailing bytes in footer"));
    }
    let (min_lsn, max_lsn) = match has_lsns {
        0 if min_lsn == 0 && max_lsn == 0 => (None, None),
        0 => return Err(corrupt(path, "lsn-free footer has non-zero lsn bounds")),
        1 => (Some(min_lsn), Some(max_lsn)),
        _ => return Err(corrupt(path, "invalid footer lsn flag")),
    };
    Ok(Footer {
        index,
        full_bloom,
        prefix_index,
        prefix_bloom,
        first_key,
        last_key,
        min_lsn,
        max_lsn,
        record_count,
    })
}

pub(crate) fn encode_row(
    output: &mut Vec<u8>,
    key: &[u8],
    lsn: Option<StrataLsn>,
    value: &[u8],
) -> Result<()> {
    let key_len = u32::try_from(key.len())
        .map_err(|_| Error::InvalidTable("key exceeds u32 length".to_owned()))?;
    let value_len = u32::try_from(value.len())
        .map_err(|_| Error::InvalidTable("value exceeds u32 length".to_owned()))?;
    output.extend_from_slice(&key_len.to_le_bytes());
    output.extend_from_slice(&value_len.to_le_bytes());
    if let Some(lsn) = lsn {
        output.extend_from_slice(&lsn.to_le_bytes());
    }
    output.extend_from_slice(key);
    output.extend_from_slice(value);
    Ok(())
}

pub(crate) fn encode_data_block(
    encoding: BlockEncoding,
    key_prefix: &[u8],
    rows: &[u8],
    record_count: u32,
) -> Result<Vec<u8>> {
    let prefix_len = if encoding == BlockEncoding::Prefix {
        4usize
            .checked_add(key_prefix.len())
            .ok_or_else(|| Error::InvalidTable("block exceeds usize length".to_owned()))?
    } else {
        0
    };
    let payload_len = prefix_len
        .checked_add(rows.len())
        .ok_or_else(|| Error::InvalidTable("block exceeds usize length".to_owned()))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .map_err(|_| Error::InvalidTable("block exceeds u32 length".to_owned()))?;
    let mut encoded = Vec::with_capacity(payload_len + DATA_BLOCK_OVERHEAD);
    encoded.extend_from_slice(&payload_len_u32.to_le_bytes());
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.push(encoding as u8);
    encoded.extend_from_slice(&[0; 3]);
    if encoding == BlockEncoding::Prefix {
        encoded.extend_from_slice(&(key_prefix.len() as u32).to_le_bytes());
        encoded.extend_from_slice(key_prefix);
    }
    encoded.extend_from_slice(rows);
    encoded.extend_from_slice(&checksum(&encoded));
    Ok(encoded)
}

pub(crate) fn visit_data_block(
    encoded: &[u8],
    block: &BlockMeta,
    kind: TableKind,
    path: &Path,
    verify_checksum: bool,
    mut visit: impl FnMut(&[u8], Option<StrataLsn>, &[u8]) -> Result<()>,
) -> Result<()> {
    if encoded.len() < DATA_BLOCK_OVERHEAD {
        return Err(corrupt(path, "block is too short"));
    }
    let checksum_offset = encoded.len() - CHECKSUM_LEN;
    if verify_checksum && checksum(&encoded[..checksum_offset]) != encoded[checksum_offset..] {
        return Err(corrupt(path, "block checksum mismatch"));
    }
    let payload_len =
        u32::from_le_bytes(encoded[..4].try_into().expect("block header slice")) as usize;
    let record_count = u32::from_le_bytes(encoded[4..8].try_into().expect("block header slice"));
    let encoding = BlockEncoding::from_byte(encoded[8], path)?;
    if payload_len.checked_add(DATA_BLOCK_OVERHEAD) != Some(encoded.len())
        || record_count != block.record_count
        || encoding != block.encoding
        || encoded[9..12] != [0; 3]
    {
        return Err(corrupt(path, "invalid block header"));
    }
    let payload = &encoded[DATA_BLOCK_HEADER_LEN..DATA_BLOCK_HEADER_LEN + payload_len];
    let mut cursor = 0;
    let key_prefix = if encoding == BlockEncoding::Prefix {
        let key_prefix_len = take_u32(payload, &mut cursor, path)? as usize;
        let key_prefix = take(payload, &mut cursor, key_prefix_len, path)?;
        if key_prefix != block.key_prefix {
            return Err(corrupt(path, "prefix block does not match sparse index"));
        }
        key_prefix
    } else {
        if !block.key_prefix.is_empty() {
            return Err(corrupt(path, "plain block contains prefixes"));
        }
        &[][..]
    };
    let mut previous_key: Option<Vec<u8>> = None;
    for _ in 0..record_count {
        let key_len = take_u32(payload, &mut cursor, path)? as usize;
        let value_len = take_u32(payload, &mut cursor, path)? as usize;
        let lsn = if kind == TableKind::Patch {
            Some(take_u64(payload, &mut cursor, path)?)
        } else {
            None
        };
        let key_part = take(payload, &mut cursor, key_len, path)?;
        let value_part = take(payload, &mut cursor, value_len, path)?;
        if encoding == BlockEncoding::Plain {
            visit(key_part, lsn, value_part)?;
            continue;
        }
        let key = joined(key_prefix, key_part)?;
        if previous_key
            .as_deref()
            .is_some_and(|previous| key.as_slice() < previous)
        {
            return Err(corrupt(path, "prefix block keys are not ordered"));
        }
        visit(&key, lsn, value_part)?;
        previous_key = Some(key);
    }
    if cursor != payload.len() {
        return Err(corrupt(path, "trailing bytes in data block"));
    }
    Ok(())
}

pub(crate) fn encode_index_block(blocks: &[BlockMeta]) -> Result<Vec<u8>> {
    let block_count = u32::try_from(blocks.len())
        .map_err(|_| Error::InvalidTable("index has too many data blocks".to_owned()))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(INDEX_MAGIC);
    encoded.extend_from_slice(&block_count.to_le_bytes());
    for block in blocks {
        let first_key_len = u32::try_from(block.first_key.len())
            .map_err(|_| Error::InvalidTable("index key exceeds u32 length".to_owned()))?;
        let last_key_len = u32::try_from(block.last_key.len())
            .map_err(|_| Error::InvalidTable("index key exceeds u32 length".to_owned()))?;
        let key_prefix_len = u32::try_from(block.key_prefix.len())
            .map_err(|_| Error::InvalidTable("index prefix exceeds u32 length".to_owned()))?;
        encoded.push(block.encoding as u8);
        encoded.extend_from_slice(&[0; 3]);
        encoded.extend_from_slice(&first_key_len.to_le_bytes());
        encoded.extend_from_slice(&last_key_len.to_le_bytes());
        encoded.extend_from_slice(&key_prefix_len.to_le_bytes());
        encoded.extend_from_slice(&block.offset.to_le_bytes());
        encoded.extend_from_slice(&block.len.to_le_bytes());
        encoded.extend_from_slice(&block.record_count.to_le_bytes());
        encoded.extend_from_slice(&block.first_key);
        encoded.extend_from_slice(&block.last_key);
        encoded.extend_from_slice(&block.key_prefix);
    }
    encoded.extend_from_slice(&checksum(&encoded));
    Ok(encoded)
}

pub(crate) fn decode_index_block(encoded: &[u8], path: &Path) -> Result<Vec<BlockMeta>> {
    let bytes = decode_metadata_block(encoded, path, INDEX_MAGIC, "index")?;
    let mut cursor = 0;
    let block_count = take_u32(bytes, &mut cursor, path)? as usize;
    if block_count > bytes.len() / 36 {
        return Err(corrupt(path, "invalid index block count"));
    }
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        let encoding = BlockEncoding::from_byte(take(bytes, &mut cursor, 1, path)?[0], path)?;
        if take(bytes, &mut cursor, 3, path)? != [0; 3] {
            return Err(corrupt(path, "non-zero reserved index bytes"));
        }
        let first_key_len = take_u32(bytes, &mut cursor, path)? as usize;
        let last_key_len = take_u32(bytes, &mut cursor, path)? as usize;
        let key_prefix_len = take_u32(bytes, &mut cursor, path)? as usize;
        let offset = take_u64(bytes, &mut cursor, path)?;
        let len = take_u64(bytes, &mut cursor, path)?;
        let record_count = take_u32(bytes, &mut cursor, path)?;
        let first_key = take(bytes, &mut cursor, first_key_len, path)?.to_vec();
        let last_key = take(bytes, &mut cursor, last_key_len, path)?.to_vec();
        let key_prefix = take(bytes, &mut cursor, key_prefix_len, path)?.to_vec();
        blocks.push(BlockMeta {
            encoding,
            first_key,
            last_key,
            key_prefix,
            offset,
            len,
            record_count,
        });
    }
    if cursor != bytes.len() {
        return Err(corrupt(path, "trailing bytes in index block"));
    }
    Ok(blocks)
}

pub(crate) fn encode_prefix_index_block(blocks: &[BlockMeta]) -> Result<Vec<u8>> {
    let mut entries = blocks
        .iter()
        .enumerate()
        .filter(|(_, block)| block.encoding == BlockEncoding::Prefix)
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|(left_index, left), (right_index, right)| {
        left.key_prefix
            .cmp(&right.key_prefix)
            .then_with(|| left.first_key.cmp(&right.first_key))
            .then_with(|| left_index.cmp(right_index))
    });
    let count = u32::try_from(entries.len())
        .map_err(|_| Error::InvalidTable("prefix index has too many blocks".to_owned()))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(PREFIX_INDEX_MAGIC);
    encoded.extend_from_slice(&count.to_le_bytes());
    for (_, block) in entries {
        let key_prefix_len = u32::try_from(block.key_prefix.len())
            .map_err(|_| Error::InvalidTable("key prefix exceeds u32 length".to_owned()))?;
        encoded.extend_from_slice(&key_prefix_len.to_le_bytes());
        encoded.extend_from_slice(&block.offset.to_le_bytes());
        encoded.extend_from_slice(&block.len.to_le_bytes());
        encoded.extend_from_slice(&block.key_prefix);
    }
    encoded.extend_from_slice(&checksum(&encoded));
    Ok(encoded)
}

pub(crate) fn decode_prefix_index_block(
    encoded: &[u8],
    blocks: &[BlockMeta],
    path: &Path,
) -> Result<Vec<PrefixIndexEntry>> {
    let bytes = decode_metadata_block(encoded, path, PREFIX_INDEX_MAGIC, "prefix index")?;
    let mut cursor = 0;
    let count = take_u32(bytes, &mut cursor, path)? as usize;
    if count > bytes.len() / 20 {
        return Err(corrupt(path, "invalid prefix index count"));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key_prefix_len = take_u32(bytes, &mut cursor, path)? as usize;
        let offset = take_u64(bytes, &mut cursor, path)?;
        let len = take_u64(bytes, &mut cursor, path)?;
        let key_prefix = take(bytes, &mut cursor, key_prefix_len, path)?.to_vec();
        let block = blocks.partition_point(|block| block.offset < offset);
        let Some(meta) = blocks.get(block).filter(|block| block.offset == offset) else {
            return Err(corrupt(path, "prefix index references an unknown block"));
        };
        if meta.encoding != BlockEncoding::Prefix
            || meta.offset != offset
            || meta.len != len
            || meta.key_prefix != key_prefix
        {
            return Err(corrupt(path, "prefix index does not match sparse index"));
        }
        entries.push(PrefixIndexEntry { key_prefix, block });
    }
    if cursor != bytes.len() {
        return Err(corrupt(path, "trailing bytes in prefix index"));
    }
    Ok(entries)
}

pub(crate) fn encode_bloom_block(magic: &[u8; 8], bloom: &BloomFilter) -> Result<Vec<u8>> {
    let byte_count = u64::try_from(bloom.bits.len())
        .map_err(|_| Error::InvalidTable("Bloom filter exceeds u64 length".to_owned()))?;
    let mut encoded = Vec::with_capacity(bloom.bits.len() + 60);
    encoded.extend_from_slice(magic);
    encoded.extend_from_slice(&bloom.bit_count.to_le_bytes());
    encoded.extend_from_slice(&bloom.hash_count.to_le_bytes());
    encoded.extend_from_slice(&byte_count.to_le_bytes());
    encoded.extend_from_slice(&bloom.bits);
    encoded.extend_from_slice(&checksum(&encoded));
    Ok(encoded)
}

pub(crate) fn decode_bloom_block(
    encoded: &[u8],
    path: &Path,
    magic: &[u8; 8],
    name: &str,
) -> Result<BloomFilter> {
    let bytes = decode_metadata_block(encoded, path, magic, name)?;
    let mut cursor = 0;
    let bit_count = take_u64(bytes, &mut cursor, path)?;
    let hash_count = take_u32(bytes, &mut cursor, path)?;
    let byte_count = usize::try_from(take_u64(bytes, &mut cursor, path)?)
        .map_err(|_| corrupt(path, format!("{name} filter does not fit in memory")))?;
    let bits = take(bytes, &mut cursor, byte_count, path)?.to_vec();
    if cursor != bytes.len() {
        return Err(corrupt(path, format!("trailing bytes in {name} block")));
    }
    Ok(BloomFilter {
        bits,
        bit_count,
        hash_count,
    })
}

fn decode_metadata_block<'a>(
    encoded: &'a [u8],
    path: &Path,
    magic: &[u8; 8],
    name: &str,
) -> Result<&'a [u8]> {
    if encoded.len() < magic.len() + CHECKSUM_LEN || &encoded[..magic.len()] != magic {
        return Err(corrupt(path, format!("invalid {name} block header")));
    }
    let checksum_offset = encoded.len() - CHECKSUM_LEN;
    if checksum(&encoded[..checksum_offset]) != encoded[checksum_offset..] {
        return Err(corrupt(path, format!("{name} block checksum mismatch")));
    }
    Ok(&encoded[magic.len()..checksum_offset])
}

pub(crate) fn validate_metadata_handles(
    path: &Path,
    footer: &Footer,
    header_end: u64,
    footer_offset: u64,
) -> Result<()> {
    let index_end = footer.index.offset.checked_add(footer.index.len);
    let full_bloom_end = footer.full_bloom.offset.checked_add(footer.full_bloom.len);
    let prefix_index_end = footer
        .prefix_index
        .offset
        .checked_add(footer.prefix_index.len);
    let prefix_bloom_end = footer
        .prefix_bloom
        .offset
        .checked_add(footer.prefix_bloom.len);
    if footer.index.offset < header_end
        || footer.index.len < (INDEX_MAGIC.len() + 4 + CHECKSUM_LEN) as u64
        || index_end != Some(footer.full_bloom.offset)
        || footer.full_bloom.len < (BLOOM_MAGIC.len() + 20 + CHECKSUM_LEN) as u64
        || full_bloom_end != Some(footer.prefix_index.offset)
        || footer.prefix_index.len < (PREFIX_INDEX_MAGIC.len() + 4 + CHECKSUM_LEN) as u64
        || prefix_index_end != Some(footer.prefix_bloom.offset)
        || footer.prefix_bloom.len < (PREFIX_BLOOM_MAGIC.len() + 20 + CHECKSUM_LEN) as u64
        || prefix_bloom_end != Some(footer_offset)
    {
        return Err(corrupt(path, "invalid metadata block handles"));
    }
    Ok(())
}

pub(crate) fn validate_footer(
    path: &Path,
    footer: &Footer,
    blocks: &[BlockMeta],
    blooms: [&BloomFilter; 2],
    kind: TableKind,
    header_end: u64,
) -> Result<()> {
    if blocks.is_empty()
        || footer.record_count == 0
        || footer.first_key > footer.last_key
        || blooms.iter().any(|bloom| {
            bloom.bits.is_empty()
                || bloom.bit_count == 0
                || !bloom.bit_count.is_multiple_of(8)
                || bloom.bit_count / 8 != bloom.bits.len() as u64
                || bloom.hash_count == 0
                || bloom.hash_count > 30
        })
    {
        return Err(corrupt(path, "invalid empty footer field"));
    }
    if (kind == TableKind::Base && (footer.min_lsn.is_some() || footer.max_lsn.is_some()))
        || (kind == TableKind::Patch && (footer.min_lsn.is_none() || footer.max_lsn.is_none()))
    {
        return Err(corrupt(path, "lsn bounds do not match table kind"));
    }
    if blocks.first().map(|block| &block.first_key) != Some(&footer.first_key)
        || blocks.last().map(|block| &block.last_key) != Some(&footer.last_key)
    {
        return Err(corrupt(path, "block bounds do not match footer"));
    }
    let mut previous_end = header_end;
    let mut previous_key: Option<&[u8]> = None;
    let mut records = 0_u64;
    for block in blocks {
        if block.first_key > block.last_key
            || block.offset != previous_end
            || block.len < DATA_BLOCK_OVERHEAD as u64
            || block
                .offset
                .checked_add(block.len)
                .is_none_or(|end| end > footer.index.offset)
            || previous_key.is_some_and(|key| key >= block.first_key.as_slice())
            || block.record_count == 0
            || (block.encoding == BlockEncoding::Plain && !block.key_prefix.is_empty())
            || (block.encoding == BlockEncoding::Prefix
                && (block.key_prefix.is_empty()
                    || !block.first_key.starts_with(&block.key_prefix)
                    || !block.last_key.starts_with(&block.key_prefix)))
        {
            return Err(corrupt(path, "invalid sparse block index"));
        }
        previous_end = block.offset + block.len;
        previous_key = Some(&block.last_key);
        records = records
            .checked_add(u64::from(block.record_count))
            .ok_or_else(|| corrupt(path, "block record count overflow"))?;
    }
    if records != footer.record_count {
        return Err(corrupt(path, "block record counts do not match footer"));
    }
    if previous_end != footer.index.offset {
        return Err(corrupt(path, "data blocks do not end at the index"));
    }
    Ok(())
}

pub(crate) fn validate_prefix_metadata(
    path: &Path,
    blocks: &[BlockMeta],
    prefix_index: &[PrefixIndexEntry],
) -> Result<()> {
    let prefix_block_count = blocks
        .iter()
        .filter(|block| block.encoding == BlockEncoding::Prefix)
        .count();
    if prefix_index.len() != prefix_block_count {
        return Err(corrupt(
            path,
            "prefix metadata does not cover prefix blocks",
        ));
    }
    if !prefix_index
        .windows(2)
        .all(|pair| compare_prefix_index_entries(&pair[0], &pair[1], blocks).is_le())
    {
        return Err(corrupt(path, "prefix index is not ordered"));
    }
    let mut indexed = vec![false; blocks.len()];
    for entry in prefix_index {
        if indexed[entry.block] {
            return Err(corrupt(path, "prefix index contains a duplicate block"));
        }
        indexed[entry.block] = true;
    }
    if blocks
        .iter()
        .enumerate()
        .any(|(index, block)| (block.encoding == BlockEncoding::Prefix) != indexed[index])
    {
        return Err(corrupt(path, "prefix index does not cover prefix blocks"));
    }
    Ok(())
}

pub(crate) fn build_bloom(hashes: &[BloomHashPair]) -> Result<BloomFilter> {
    let key_count = u64::try_from(hashes.len())
        .map_err(|_| Error::InvalidTable("too many Bloom filter keys".to_owned()))?;
    let raw_bit_count = key_count
        .checked_mul(BLOOM_BITS_PER_KEY)
        .ok_or_else(|| Error::InvalidTable("Bloom filter size overflow".to_owned()))?
        .max(64);
    let bit_count = raw_bit_count
        .checked_add(7)
        .ok_or_else(|| Error::InvalidTable("Bloom filter size overflow".to_owned()))?
        / 8
        * 8;
    let byte_count = usize::try_from(bit_count / 8)
        .map_err(|_| Error::InvalidTable("Bloom filter does not fit in memory".to_owned()))?;
    let hash_count = ((BLOOM_BITS_PER_KEY * 69 + 50) / 100).clamp(1, 30) as u32;
    let mut bits = vec![0_u8; byte_count];
    for &(first, second) in hashes {
        for index in 0..hash_count {
            let bit = first.wrapping_add(u64::from(index).wrapping_mul(second)) % bit_count;
            bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
    Ok(BloomFilter {
        bits,
        bit_count,
        hash_count,
    })
}

pub(crate) fn bloom_may_contain(key: &[u8], bits: &[u8], bit_count: u64, hash_count: u32) -> bool {
    bloom_may_contain_with_seed(key, bits, bit_count, hash_count, BLOOM_SEED)
}

pub(crate) fn bloom_may_contain_with_seed(
    key: &[u8],
    bits: &[u8],
    bit_count: u64,
    hash_count: u32,
    seed: u64,
) -> bool {
    let (first, second) = hashes_with_seed(key, seed);
    (0..hash_count).all(|index| {
        let bit = first.wrapping_add(u64::from(index).wrapping_mul(second)) % bit_count;
        bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
    })
}

pub(crate) fn bloom_hashes(key: &[u8]) -> BloomHashPair {
    hashes_with_seed(key, BLOOM_SEED)
}

pub(crate) fn prefix_bloom_hashes(key: &[u8]) -> BloomHashPair {
    hashes_with_seed(key, PREFIX_BLOOM_SEED)
}

fn hashes_with_seed(key: &[u8], seed: u64) -> BloomHashPair {
    let first = xxh3_64_with_seed(key, seed);
    let second = xxh3_64_with_seed(key, seed ^ 0x9e37_79b9_7f4a_7c15) | 1;
    (first, second)
}

pub(crate) fn joined(prefix: &[u8], suffix: &[u8]) -> Result<Vec<u8>> {
    let len = prefix
        .len()
        .checked_add(suffix.len())
        .ok_or_else(|| Error::InvalidTable("joined table field is too large".to_owned()))?;
    let mut joined = Vec::with_capacity(len);
    joined.extend_from_slice(prefix);
    joined.extend_from_slice(suffix);
    Ok(joined)
}

fn compare_prefix_index_entries(
    left: &PrefixIndexEntry,
    right: &PrefixIndexEntry,
    blocks: &[BlockMeta],
) -> std::cmp::Ordering {
    left.key_prefix
        .cmp(&right.key_prefix)
        .then_with(|| {
            blocks[left.block]
                .first_key
                .cmp(&blocks[right.block].first_key)
        })
        .then_with(|| left.block.cmp(&right.block))
}

pub(crate) fn checksum(bytes: &[u8]) -> [u8; CHECKSUM_LEN] {
    Sha256::digest(bytes).into()
}

fn take_u32(bytes: &[u8], cursor: &mut usize, path: &Path) -> Result<u32> {
    Ok(u32::from_le_bytes(
        take(bytes, cursor, 4, path)?
            .try_into()
            .expect("four-byte slice"),
    ))
}

fn take_u64(bytes: &[u8], cursor: &mut usize, path: &Path) -> Result<u64> {
    Ok(u64::from_le_bytes(
        take(bytes, cursor, 8, path)?
            .try_into()
            .expect("eight-byte slice"),
    ))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize, path: &Path) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| corrupt(path, "record length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt(path, "record exceeds data block"))?;
    *cursor = end;
    Ok(value)
}

pub(crate) fn corrupt(path: &Path, reason: impl Into<String>) -> Error {
    Error::CorruptTable {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}
