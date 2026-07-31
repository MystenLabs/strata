use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::{Error, FORMAT_VERSION, Result, StrataLsn, TableMeta};

const FILE_MAGIC: &[u8; 8] = b"STRLSM01";
const INDEX_MAGIC: &[u8; 8] = b"STRIDX01";
const BLOOM_MAGIC: &[u8; 8] = b"STRBLM01";
const PREFIX_INDEX_MAGIC: &[u8; 8] = b"STRPFXI1";
const PREFIX_BLOOM_MAGIC: &[u8; 8] = b"STRPFXB1";
const FOOTER_MAGIC: &[u8; 8] = b"STRFTR01";
const TRAILER_MAGIC: &[u8; 8] = b"STREND01";
const HEADER_FIXED_LEN: usize = 32;
const TRAILER_LEN: usize = 48;
const CHECKSUM_LEN: usize = 32;
const DATA_BLOCK_HEADER_LEN: usize = 12;
const DATA_BLOCK_OVERHEAD: usize = DATA_BLOCK_HEADER_LEN + CHECKSUM_LEN;
const FOOTER_FIXED_LEN: usize = 112;
const MAX_FORMAT_ID_BYTES: usize = 1024;
const TARGET_BLOCK_BYTES: usize = 64 * 1024;
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
const PREFIX_BLOOM_SEED: u64 = 0xbb67_ae85_84ca_a73b;
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Point-read data-block cache counters and current occupancy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub entries: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BlockCacheKey {
    table_id: u64,
    offset: u64,
}

struct CachedBlock {
    bytes: Arc<[u8]>,
    referenced: bool,
}

#[derive(Default)]
struct BlockCacheState {
    entries: HashMap<BlockCacheKey, CachedBlock>,
    clock: VecDeque<BlockCacheKey>,
    bytes: usize,
}

pub(crate) struct BlockCache {
    capacity: usize,
    state: Mutex<BlockCacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: AtomicU64,
    evictions: AtomicU64,
}

impl BlockCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(BlockCacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            insertions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn get(&self, key: BlockCacheKey) -> Option<Arc<[u8]>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(block) = state.entries.get_mut(&key) {
            block.referenced = true;
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(Arc::clone(&block.bytes));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn insert(&self, key: BlockCacheKey, bytes: Arc<[u8]>) {
        if self.capacity == 0 || bytes.len() > self.capacity {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(block) = state.entries.get_mut(&key) {
            block.referenced = true;
            return;
        }
        state.bytes += bytes.len();
        state.entries.insert(
            key,
            CachedBlock {
                bytes,
                referenced: true,
            },
        );
        state.clock.push_back(key);
        self.insertions.fetch_add(1, Ordering::Relaxed);

        while state.bytes > self.capacity {
            let Some(candidate) = state.clock.pop_front() else {
                break;
            };
            let Some(block) = state.entries.get_mut(&candidate) else {
                continue;
            };
            if block.referenced {
                block.referenced = false;
                state.clock.push_back(candidate);
                continue;
            }
            let block = state
                .entries
                .remove(&candidate)
                .expect("clock candidate exists");
            state.bytes -= block.bytes.len();
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn remove_table(&self, table_id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed = state
            .entries
            .extract_if(|key, _| key.table_id == table_id)
            .map(|(_, block)| block.bytes.len())
            .sum::<usize>();
        state.bytes = state.bytes.saturating_sub(removed);
        if removed > 0 {
            let mut clock = std::mem::take(&mut state.clock);
            clock.retain(|key| state.entries.contains_key(key));
            state.clock = clock;
        }
    }

    pub(crate) fn stats(&self) -> BlockCacheStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: state.entries.len(),
            bytes: state.bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableKind {
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
enum BlockEncoding {
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
struct BlockMeta {
    encoding: BlockEncoding,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    key_prefix: Vec<u8>,
    offset: u64,
    len: u64,
    record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockHandle {
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Footer {
    index: BlockHandle,
    full_bloom: BlockHandle,
    prefix_index: BlockHandle,
    prefix_bloom: BlockHandle,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    min_lsn: Option<StrataLsn>,
    max_lsn: Option<StrataLsn>,
    record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BloomFilter {
    bits: Vec<u8>,
    bit_count: u64,
    hash_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrefixIndexEntry {
    key_prefix: Vec<u8>,
    block: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockLayout {
    Plain,
    Prefix { key_prefix: Vec<u8> },
}

impl BlockLayout {
    fn encoding(&self) -> BlockEncoding {
        match self {
            Self::Plain => BlockEncoding::Plain,
            Self::Prefix { .. } => BlockEncoding::Prefix,
        }
    }
}

struct PendingBlock {
    layout: BlockLayout,
    rows: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    record_count: u32,
}

impl PendingBlock {
    fn new(layout: BlockLayout, key: &[u8]) -> Self {
        Self {
            layout,
            rows: Vec::new(),
            first_key: key.to_vec(),
            last_key: key.to_vec(),
            record_count: 0,
        }
    }

    fn estimated_len_with(&self, row_len: usize) -> usize {
        DATA_BLOCK_OVERHEAD
            .saturating_add(self.rows.len())
            .saturating_add(row_len)
            .saturating_add(match &self.layout {
                BlockLayout::Plain => 0,
                BlockLayout::Prefix { key_prefix } => 4usize.saturating_add(key_prefix.len()),
            })
    }
}

/// Writes one immutable base or patch SST file.
///
/// Rows must arrive in unsigned lexicographic key order. Patch rows for the same key must also have
/// strictly increasing lsns. `finish` syncs a temporary file, renames it to its final relative
/// path, syncs the parent directory, and returns metadata for that one file.
pub struct TableWriter {
    path: PathBuf,
    tmp_path: PathBuf,
    relative_path: String,
    writer: BufWriter<File>,
    kind: TableKind,
    id: u64,
    partition: u32,
    offset: u64,
    current_key: Option<Vec<u8>>,
    current_lsn: Option<StrataLsn>,
    current_layout: Option<BlockLayout>,
    block: Option<PendingBlock>,
    blocks: Vec<BlockMeta>,
    full_key_hashes: Vec<(u64, u64)>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    min_lsn: Option<StrataLsn>,
    max_lsn: Option<StrataLsn>,
    record_count: u64,
}

impl TableWriter {
    pub fn create_base(
        root: impl AsRef<Path>,
        relative_path: impl Into<String>,
        id: u64,
        partition: u32,
        schema_id: &str,
    ) -> Result<Self> {
        Self::create(
            root.as_ref(),
            relative_path.into(),
            id,
            partition,
            schema_id,
            TableKind::Base,
        )
    }

    pub fn create_patch(
        root: impl AsRef<Path>,
        relative_path: impl Into<String>,
        id: u64,
        partition: u32,
        patch_format_id: &str,
    ) -> Result<Self> {
        Self::create(
            root.as_ref(),
            relative_path.into(),
            id,
            partition,
            patch_format_id,
            TableKind::Patch,
        )
    }

    fn create(
        root: &Path,
        relative_path: String,
        id: u64,
        partition: u32,
        value_format_id: &str,
        kind: TableKind,
    ) -> Result<Self> {
        validate_relative_path(&relative_path)?;
        if value_format_id.is_empty() || value_format_id.len() > MAX_FORMAT_ID_BYTES {
            return Err(Error::InvalidTable(
                "value format identifier must contain 1 to 1024 bytes".to_owned(),
            ));
        }
        let path = root.join(&relative_path);
        if path.exists() {
            return Err(Error::InvalidTable(format!(
                "SST already exists at {}",
                path.display()
            )));
        }
        let parent = path.parent().unwrap_or(root);
        fs::create_dir_all(parent).map_err(|source| Error::Io {
            path: parent.to_path_buf(),
            source,
        })?;
        let mut tmp_name = path.as_os_str().to_os_string();
        tmp_name.push(".tmp");
        let tmp_path = PathBuf::from(tmp_name);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|source| Error::Io {
                path: tmp_path.clone(),
                source,
            })?;
        let mut writer = BufWriter::new(file);
        let header = encode_header(kind, id, partition, value_format_id)?;
        writer.write_all(&header).map_err(|source| Error::Io {
            path: tmp_path.clone(),
            source,
        })?;

        Ok(Self {
            path,
            tmp_path,
            relative_path,
            writer,
            kind,
            id,
            partition,
            offset: header.len() as u64,
            current_key: None,
            current_lsn: None,
            current_layout: None,
            block: None,
            blocks: Vec::new(),
            full_key_hashes: Vec::new(),
            first_key: None,
            last_key: None,
            min_lsn: None,
            max_lsn: None,
            record_count: 0,
        })
    }

    /// Adds one base key/value row.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.add_row(key, None, &[], key, value)
    }

    /// Adds one base row using an explicit key prefix.
    pub fn add_prefix(&mut self, key_prefix: &[u8], key_suffix: &[u8], value: &[u8]) -> Result<()> {
        let key = joined(key_prefix, key_suffix)?;
        self.add_row(&key, None, key_prefix, key_suffix, value)
    }

    /// Adds one versioned patch row.
    pub fn add_patch(&mut self, key: &[u8], lsn: StrataLsn, value: &[u8]) -> Result<()> {
        self.add_row(key, Some(lsn), &[], key, value)
    }

    /// Adds one versioned patch row using an explicit key prefix.
    pub fn add_prefix_patch(
        &mut self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<()> {
        let key = joined(key_prefix, key_suffix)?;
        self.add_row(&key, Some(lsn), key_prefix, key_suffix, value)
    }

    pub(crate) fn estimated_len(&self) -> u64 {
        self.offset.saturating_add(
            self.block
                .as_ref()
                .map_or(0, |block| block.estimated_len_with(0) as u64),
        )
    }

    fn add_row(
        &mut self,
        key: &[u8],
        lsn: Option<StrataLsn>,
        key_prefix: &[u8],
        key_suffix: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.expect_lsn(lsn)?;
        let prefixed = !key_prefix.is_empty();
        if prefixed && key != joined(key_prefix, key_suffix)? {
            return Err(Error::InvalidTable(
                "key prefix and suffix do not reconstruct the full key".to_owned(),
            ));
        }
        for part in [key, key_suffix, value] {
            u32::try_from(part.len())
                .map_err(|_| Error::InvalidTable("table field exceeds u32 length".to_owned()))?;
        }
        let layout = if prefixed {
            BlockLayout::Prefix {
                key_prefix: key_prefix.to_vec(),
            }
        } else {
            BlockLayout::Plain
        };
        let lsn_len = if lsn.is_some() { 8 } else { 0 };
        let row_len = 8usize
            .checked_add(key_suffix.len())
            .and_then(|len| len.checked_add(value.len()))
            .and_then(|len| len.checked_add(lsn_len))
            .ok_or_else(|| Error::InvalidTable("table row is too large".to_owned()))?;

        let new_key = self.start_or_validate_key(key, lsn, &layout)?;
        if new_key {
            let should_flush = self.block.as_ref().is_some_and(|block| {
                block.layout != layout || block.estimated_len_with(row_len) > TARGET_BLOCK_BYTES
            });
            if should_flush {
                self.flush_block()?;
            }
            if self.block.is_none() {
                self.block = Some(PendingBlock::new(layout.clone(), key));
            }
        }

        let block = self.block.as_mut().expect("row has an open block");
        if block.layout != layout {
            return Err(Error::InvalidTable(
                "versions of one key must use one block encoding".to_owned(),
            ));
        }
        encode_lengths(&mut block.rows, key_suffix, value)?;
        if let Some(lsn) = lsn {
            block.rows.extend_from_slice(&lsn.to_le_bytes());
            self.min_lsn = Some(self.min_lsn.map_or(lsn, |current| current.min(lsn)));
            self.max_lsn = Some(self.max_lsn.map_or(lsn, |current| current.max(lsn)));
        }
        block.rows.extend_from_slice(key_suffix);
        block.rows.extend_from_slice(value);
        block.last_key = key.to_vec();
        block.record_count = block
            .record_count
            .checked_add(1)
            .ok_or_else(|| Error::InvalidTable("too many records in one block".to_owned()))?;
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| Error::InvalidTable("table record count overflow".to_owned()))?;
        Ok(())
    }

    fn expect_lsn(&self, lsn: Option<StrataLsn>) -> Result<()> {
        if (self.kind == TableKind::Base) == lsn.is_none() {
            Ok(())
        } else {
            Err(Error::InvalidTable(
                "row lsn does not match table kind".to_owned(),
            ))
        }
    }

    fn start_or_validate_key(
        &mut self,
        key: &[u8],
        lsn: Option<StrataLsn>,
        layout: &BlockLayout,
    ) -> Result<bool> {
        match self.current_key.as_deref() {
            None => {
                self.start_key(key, lsn, layout);
                Ok(true)
            }
            Some(current) if key > current => {
                self.start_key(key, lsn, layout);
                Ok(true)
            }
            Some(current) if key < current => Err(Error::InvalidTable(
                "keys must be added in lexicographic order".to_owned(),
            )),
            Some(_) if self.kind == TableKind::Base => Err(Error::InvalidTable(
                "a base SST cannot contain duplicate keys".to_owned(),
            )),
            Some(_) => {
                let lsn = lsn.expect("patch table validation supplies a lsn");
                if self.current_lsn.is_some_and(|current| lsn <= current) {
                    return Err(Error::InvalidTable(
                        "patch lsns for one key must be strictly increasing".to_owned(),
                    ));
                }
                if self.current_layout.as_ref() != Some(layout) {
                    return Err(Error::InvalidTable(
                        "versions of one key must use one block encoding".to_owned(),
                    ));
                }
                self.current_lsn = Some(lsn);
                Ok(false)
            }
        }
    }

    fn start_key(&mut self, key: &[u8], lsn: Option<StrataLsn>, layout: &BlockLayout) {
        let key = key.to_vec();
        self.first_key.get_or_insert_with(|| key.clone());
        self.last_key = Some(key.clone());
        self.full_key_hashes.push(bloom_hashes(&key));
        self.current_key = Some(key);
        self.current_lsn = lsn;
        self.current_layout = Some(layout.clone());
    }

    fn flush_block(&mut self) -> Result<()> {
        let Some(block) = self.block.take() else {
            return Ok(());
        };
        let key_prefix = match &block.layout {
            BlockLayout::Plain => &[][..],
            BlockLayout::Prefix { key_prefix } => key_prefix.as_slice(),
        };
        let mut payload = Vec::new();
        if block.layout.encoding() == BlockEncoding::Prefix {
            payload.extend_from_slice(&(key_prefix.len() as u32).to_le_bytes());
            payload.extend_from_slice(key_prefix);
        }
        payload.extend_from_slice(&block.rows);
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| Error::InvalidTable("block exceeds u32 length".to_owned()))?;
        let mut encoded = Vec::with_capacity(payload.len() + DATA_BLOCK_OVERHEAD);
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        encoded.extend_from_slice(&block.record_count.to_le_bytes());
        encoded.push(block.layout.encoding() as u8);
        encoded.extend_from_slice(&[0; 3]);
        encoded.append(&mut payload);
        encoded.extend_from_slice(&checksum(&encoded));
        let len = encoded.len() as u64;
        self.writer
            .write_all(&encoded)
            .map_err(|source| Error::Io {
                path: self.tmp_path.clone(),
                source,
            })?;
        let encoding = block.layout.encoding();
        self.blocks.push(BlockMeta {
            encoding,
            first_key: block.first_key,
            last_key: block.last_key,
            key_prefix: key_prefix.to_vec(),
            offset: self.offset,
            len,
            record_count: block.record_count,
        });
        self.offset = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::InvalidTable("table offset overflow".to_owned()))?;
        Ok(())
    }

    fn write_metadata_block(&mut self, bytes: &[u8]) -> Result<BlockHandle> {
        let len = u64::try_from(bytes.len())
            .map_err(|_| Error::InvalidTable("metadata block exceeds u64 length".to_owned()))?;
        let handle = BlockHandle {
            offset: self.offset,
            len,
        };
        self.writer.write_all(bytes).map_err(|source| Error::Io {
            path: self.tmp_path.clone(),
            source,
        })?;
        self.offset = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::InvalidTable("table offset overflow".to_owned()))?;
        Ok(handle)
    }

    /// Finishes, syncs, and publishes the SST file at its final path.
    pub fn finish(mut self) -> Result<TableMeta> {
        if self.record_count == 0 {
            return Err(Error::InvalidTable("an SST must not be empty".to_owned()));
        }
        self.flush_block()?;
        let index_bytes = encode_index_block(&self.blocks)?;
        let index = self.write_metadata_block(&index_bytes)?;
        let full_bloom = build_bloom(&self.full_key_hashes)?;
        let full_bloom_bytes = encode_bloom_block(BLOOM_MAGIC, &full_bloom)?;
        let full_bloom = self.write_metadata_block(&full_bloom_bytes)?;
        let prefix_index = self.write_metadata_block(&encode_prefix_index_block(&self.blocks)?)?;
        let mut prefix_hashes = self
            .blocks
            .iter()
            .filter(|block| block.encoding == BlockEncoding::Prefix)
            .map(|block| prefix_bloom_hashes(&block.key_prefix))
            .collect::<Vec<_>>();
        prefix_hashes.sort_unstable();
        prefix_hashes.dedup();
        let prefix_bloom = build_bloom(&prefix_hashes)?;
        let prefix_bloom_bytes = encode_bloom_block(PREFIX_BLOOM_MAGIC, &prefix_bloom)?;
        let prefix_bloom = self.write_metadata_block(&prefix_bloom_bytes)?;
        let first_key = self
            .first_key
            .take()
            .expect("non-empty table has first key");
        let last_key = self.last_key.take().expect("non-empty table has last key");
        let footer = Footer {
            index,
            full_bloom,
            prefix_index,
            prefix_bloom,
            first_key: first_key.clone(),
            last_key: last_key.clone(),
            min_lsn: self.min_lsn,
            max_lsn: self.max_lsn,
            record_count: self.record_count,
        };
        let footer_bytes = encode_footer(&footer)?;
        let footer_len = u64::try_from(footer_bytes.len())
            .map_err(|_| Error::InvalidTable("footer exceeds u64 length".to_owned()))?;
        let footer_checksum = checksum(&footer_bytes);
        self.writer
            .write_all(&footer_bytes)
            .and_then(|_| self.writer.write_all(&footer_len.to_le_bytes()))
            .and_then(|_| self.writer.write_all(&footer_checksum))
            .and_then(|_| self.writer.write_all(TRAILER_MAGIC))
            .and_then(|_| self.writer.flush())
            .map_err(|source| Error::Io {
                path: self.tmp_path.clone(),
                source,
            })?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|source| Error::Io {
                path: self.tmp_path.clone(),
                source,
            })?;
        let file_len = self
            .offset
            .checked_add(footer_len)
            .and_then(|len| len.checked_add(TRAILER_LEN as u64))
            .ok_or_else(|| Error::InvalidTable("table length overflow".to_owned()))?;
        drop(self.writer);
        fs::rename(&self.tmp_path, &self.path).map_err(|source| Error::Io {
            path: self.path.clone(),
            source,
        })?;
        sync_parent(&self.path)?;

        Ok(TableMeta {
            id: self.id,
            partition: self.partition,
            relative_path: self.relative_path,
            first_key,
            last_key,
            min_lsn: self.min_lsn,
            max_lsn: self.max_lsn,
            record_count: self.record_count,
            file_len,
            checksum: footer_checksum,
        })
    }
}

/// Opens one immutable SST and keeps its sparse index and Bloom filter in memory.
pub struct TableReader {
    id: u64,
    path: PathBuf,
    file: File,
    kind: TableKind,
    footer: Footer,
    blocks: Vec<BlockMeta>,
    full_bloom: BloomFilter,
    prefix_index: Vec<PrefixIndexEntry>,
    prefix_bloom: BloomFilter,
    block_cache: Option<Arc<BlockCache>>,
}

pub(crate) struct TableRow {
    pub key: Vec<u8>,
    pub key_prefix_len: usize,
    pub lsn: Option<StrataLsn>,
    pub value: Vec<u8>,
}

/// A compaction cursor that keeps at most one decoded block in memory.
pub(crate) struct TableCursor {
    reader: TableReader,
    block: usize,
    buffered: VecDeque<TableRow>,
    current: Option<TableRow>,
}

impl TableReader {
    pub fn open_base(root: impl AsRef<Path>, meta: &TableMeta, schema_id: &str) -> Result<Self> {
        Self::open(root.as_ref(), meta, schema_id, TableKind::Base, None)
    }

    pub fn open_patch(
        root: impl AsRef<Path>,
        meta: &TableMeta,
        patch_format_id: &str,
    ) -> Result<Self> {
        Self::open(root.as_ref(), meta, patch_format_id, TableKind::Patch, None)
    }

    pub(crate) fn open_base_cached(
        root: impl AsRef<Path>,
        meta: &TableMeta,
        schema_id: &str,
        block_cache: Arc<BlockCache>,
    ) -> Result<Self> {
        Self::open(
            root.as_ref(),
            meta,
            schema_id,
            TableKind::Base,
            Some(block_cache),
        )
    }

    pub(crate) fn open_patch_cached(
        root: impl AsRef<Path>,
        meta: &TableMeta,
        patch_format_id: &str,
        block_cache: Arc<BlockCache>,
    ) -> Result<Self> {
        Self::open(
            root.as_ref(),
            meta,
            patch_format_id,
            TableKind::Patch,
            Some(block_cache),
        )
    }

    pub(crate) fn into_cursor(self) -> Result<TableCursor> {
        self.into_cursor_from(None)
    }

    pub(crate) fn into_cursor_from(self, start: Option<&[u8]>) -> Result<TableCursor> {
        let block = start.map_or(0, |start| {
            self.blocks
                .partition_point(|block| block.last_key.as_slice() < start)
        });
        let mut cursor = TableCursor {
            reader: self,
            block,
            buffered: VecDeque::new(),
            current: None,
        };
        cursor.advance()?;
        while cursor
            .current()
            .is_some_and(|row| start.is_some_and(|start| row.key.as_slice() < start))
        {
            cursor.advance()?;
        }
        Ok(cursor)
    }

    fn open(
        root: &Path,
        meta: &TableMeta,
        expected_format_id: &str,
        expected_kind: TableKind,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        validate_relative_path(&meta.relative_path)?;
        let path = root.join(&meta.relative_path);
        let file = File::open(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let file_len = file
            .metadata()
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?
            .len();
        if file_len != meta.file_len {
            return Err(corrupt(
                &path,
                format!(
                    "file length {file_len} does not match manifest {}",
                    meta.file_len
                ),
            ));
        }
        if file_len < (HEADER_FIXED_LEN + TRAILER_LEN) as u64 {
            return Err(corrupt(&path, "file is too short"));
        }
        let fixed = read_exact_at(&file, 0, HEADER_FIXED_LEN, &path)?;
        if &fixed[..8] != FILE_MAGIC {
            return Err(corrupt(&path, "invalid file magic"));
        }
        let version = u32::from_le_bytes(fixed[8..12].try_into().expect("fixed header slice"));
        if version != FORMAT_VERSION {
            return Err(corrupt(
                &path,
                format!("format version {version} is not {FORMAT_VERSION}"),
            ));
        }
        if fixed[13..16] != [0; 3] {
            return Err(corrupt(&path, "non-zero reserved header bytes"));
        }
        let kind = TableKind::from_byte(fixed[12], &path)?;
        if kind != expected_kind {
            return Err(corrupt(
                &path,
                "table kind does not match manifest placement",
            ));
        }
        let id = u64::from_le_bytes(fixed[16..24].try_into().expect("fixed header slice"));
        let partition = u32::from_le_bytes(fixed[24..28].try_into().expect("fixed header slice"));
        let format_len =
            u32::from_le_bytes(fixed[28..32].try_into().expect("fixed header slice")) as usize;
        if format_len == 0 || format_len > MAX_FORMAT_ID_BYTES {
            return Err(corrupt(&path, "invalid value format identifier length"));
        }
        if id != meta.id || partition != meta.partition {
            return Err(corrupt(&path, "table identity does not match manifest"));
        }
        let header_end = HEADER_FIXED_LEN
            .checked_add(format_len)
            .ok_or_else(|| corrupt(&path, "header length overflow"))?;
        if header_end as u64 > file_len - TRAILER_LEN as u64 {
            return Err(corrupt(&path, "value format identifier exceeds file"));
        }
        let format_bytes = read_exact_at(&file, HEADER_FIXED_LEN as u64, format_len, &path)?;
        if format_bytes != expected_format_id.as_bytes() {
            return Err(corrupt(&path, "value format identifier mismatch"));
        }

        let trailer_offset = file_len - TRAILER_LEN as u64;
        let trailer = read_exact_at(&file, trailer_offset, TRAILER_LEN, &path)?;
        if &trailer[40..] != TRAILER_MAGIC {
            return Err(corrupt(&path, "invalid trailer magic"));
        }
        let footer_len = u64::from_le_bytes(trailer[..8].try_into().expect("trailer slice"));
        let footer_offset = trailer_offset
            .checked_sub(footer_len)
            .ok_or_else(|| corrupt(&path, "footer length exceeds file"))?;
        if footer_offset < header_end as u64 {
            return Err(corrupt(&path, "footer overlaps table header"));
        }
        let footer_len = usize::try_from(footer_len)
            .map_err(|_| corrupt(&path, "footer does not fit in memory"))?;
        let footer_bytes = read_exact_at(&file, footer_offset, footer_len, &path)?;
        let footer_checksum: [u8; 32] = trailer[8..40].try_into().expect("trailer slice");
        if checksum(&footer_bytes) != footer_checksum || meta.checksum != footer_checksum {
            return Err(corrupt(&path, "footer checksum mismatch"));
        }
        let footer = decode_footer(&footer_bytes, &path)?;
        validate_metadata_handles(&path, &footer, header_end as u64, footer_offset)?;
        let blocks = read_index_block(&file, &footer.index, &path)?;
        let full_bloom = read_bloom_block(
            &file,
            &footer.full_bloom,
            &path,
            BLOOM_MAGIC,
            "full-key Bloom",
        )?;
        let prefix_index = read_prefix_index_block(&file, &footer.prefix_index, &blocks, &path)?;
        let prefix_bloom = read_bloom_block(
            &file,
            &footer.prefix_bloom,
            &path,
            PREFIX_BLOOM_MAGIC,
            "prefix Bloom",
        )?;
        validate_footer(
            &path,
            &footer,
            &blocks,
            [&full_bloom, &prefix_bloom],
            meta,
            kind,
            header_end as u64,
        )?;
        validate_prefix_metadata(&path, &blocks, &prefix_index)?;

        Ok(Self {
            id,
            path,
            file,
            kind,
            footer,
            blocks,
            full_bloom,
            prefix_index,
            prefix_bloom,
            block_cache,
        })
    }

    /// Looks up one base value using table bounds, the Bloom filter, and the sparse block index.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.expect_kind(TableKind::Base)?;
        if !self.may_contain(key) {
            return Ok(None);
        }
        let Some(block) = self.find_block(key) else {
            return Ok(None);
        };
        let mut found = None;
        self.visit_block(block, |record_key, _, value| {
            if record_key == key {
                found = Some(value.to_vec());
            }
            Ok(())
        })?;
        Ok(found)
    }

    /// Looks up every patch for one key in lsn order.
    pub fn get_patches(&self, key: &[u8]) -> Result<Vec<(StrataLsn, Vec<u8>)>> {
        self.expect_kind(TableKind::Patch)?;
        if !self.may_contain(key) {
            return Ok(Vec::new());
        }
        let Some(block) = self.find_block(key) else {
            return Ok(Vec::new());
        };
        let mut found = Vec::new();
        self.visit_block(block, |record_key, lsn, value| {
            if record_key == key {
                found.push((lsn.expect("patch record has lsn"), value.to_vec()));
            }
            Ok(())
        })?;
        Ok(found)
    }

    /// Visits base rows in the half-open range `[start, end)`.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.expect_kind(TableKind::Base)?;
        self.scan_records(start, end, |key, _, value| visit(key, value))
    }

    /// Visits patch rows in the half-open range `[start, end)`.
    pub fn scan_patches(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], StrataLsn, &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.expect_kind(TableKind::Patch)?;
        self.scan_records(start, end, |key, lsn, value| {
            visit(key, lsn.expect("patch record has lsn"), value)
        })
    }

    /// Visits base rows stored under the exact key prefix.
    pub fn scan_key_prefix(
        &self,
        key_prefix: &[u8],
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.expect_kind(TableKind::Base)?;
        if !self.prefix_may_contain(key_prefix) {
            return Ok(());
        }
        let start = self
            .prefix_index
            .partition_point(|entry| entry.key_prefix.as_slice() < key_prefix);
        for entry in self.prefix_index[start..]
            .iter()
            .take_while(|entry| entry.key_prefix == key_prefix)
        {
            self.visit_block(&self.blocks[entry.block], |key, _, value| visit(key, value))?;
        }
        Ok(())
    }

    fn scan_records(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], Option<StrataLsn>, &[u8]) -> Result<()>,
    ) -> Result<()> {
        for block in &self.blocks {
            if start.is_some_and(|start| block.last_key.as_slice() < start) {
                continue;
            }
            if end.is_some_and(|end| block.first_key.as_slice() >= end) {
                break;
            }
            self.visit_block(block, |key, lsn, value| {
                if start.is_none_or(|start| key >= start) && end.is_none_or(|end| key < end) {
                    visit(key, lsn, value)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        if key < self.footer.first_key.as_slice() || key > self.footer.last_key.as_slice() {
            return false;
        }
        bloom_may_contain(
            key,
            &self.full_bloom.bits,
            self.full_bloom.bit_count,
            self.full_bloom.hash_count,
        )
    }

    fn prefix_may_contain(&self, key_prefix: &[u8]) -> bool {
        bloom_may_contain_with_seed(
            key_prefix,
            &self.prefix_bloom.bits,
            self.prefix_bloom.bit_count,
            self.prefix_bloom.hash_count,
            PREFIX_BLOOM_SEED,
        )
    }

    fn find_block(&self, key: &[u8]) -> Option<&BlockMeta> {
        let index = self
            .blocks
            .partition_point(|block| block.last_key.as_slice() < key);
        self.blocks
            .get(index)
            .filter(|block| block.first_key.as_slice() <= key)
    }

    fn visit_block(
        &self,
        block: &BlockMeta,
        mut visit: impl FnMut(&[u8], Option<StrataLsn>, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let len = usize::try_from(block.len)
            .map_err(|_| corrupt(&self.path, "block does not fit in memory"))?;
        let cache_key = BlockCacheKey {
            table_id: self.id,
            offset: block.offset,
        };
        let cached = self
            .block_cache
            .as_ref()
            .and_then(|cache| cache.get(cache_key));
        let was_cached = cached.is_some();
        let encoded: Arc<[u8]> = match cached {
            Some(encoded) => encoded,
            None => read_exact_at(&self.file, block.offset, len, &self.path)?.into(),
        };
        if encoded.len() < DATA_BLOCK_OVERHEAD {
            return Err(corrupt(&self.path, "block is too short"));
        }
        let checksum_offset = encoded.len() - CHECKSUM_LEN;
        if !was_cached && checksum(&encoded[..checksum_offset]) != encoded[checksum_offset..] {
            return Err(corrupt(&self.path, "block checksum mismatch"));
        }
        let payload_len =
            u32::from_le_bytes(encoded[..4].try_into().expect("block header slice")) as usize;
        let record_count =
            u32::from_le_bytes(encoded[4..8].try_into().expect("block header slice"));
        let encoding = BlockEncoding::from_byte(encoded[8], &self.path)?;
        if payload_len.checked_add(DATA_BLOCK_OVERHEAD) != Some(encoded.len())
            || record_count != block.record_count
            || encoding != block.encoding
            || encoded[9..12] != [0; 3]
        {
            return Err(corrupt(&self.path, "invalid block header"));
        }
        let payload = &encoded[DATA_BLOCK_HEADER_LEN..DATA_BLOCK_HEADER_LEN + payload_len];
        let mut cursor = 0;
        let key_prefix = if encoding == BlockEncoding::Prefix {
            let key_prefix_len = take_u32(payload, &mut cursor, &self.path)? as usize;
            let key_prefix = take(payload, &mut cursor, key_prefix_len, &self.path)?;
            if key_prefix != block.key_prefix {
                return Err(corrupt(
                    &self.path,
                    "prefix block does not match sparse index",
                ));
            }
            key_prefix
        } else {
            if !block.key_prefix.is_empty() {
                return Err(corrupt(&self.path, "plain block contains prefixes"));
            }
            &[][..]
        };
        let mut previous_key: Option<Vec<u8>> = None;
        for _ in 0..record_count {
            let key_len = take_u32(payload, &mut cursor, &self.path)? as usize;
            let value_len = take_u32(payload, &mut cursor, &self.path)? as usize;
            let lsn = if self.kind == TableKind::Patch {
                Some(take_u64(payload, &mut cursor, &self.path)?)
            } else {
                None
            };
            let key_part = take(payload, &mut cursor, key_len, &self.path)?;
            let value_part = take(payload, &mut cursor, value_len, &self.path)?;
            if encoding == BlockEncoding::Plain {
                visit(key_part, lsn, value_part)?;
                continue;
            }
            let key = joined(key_prefix, key_part)?;
            if previous_key
                .as_deref()
                .is_some_and(|previous| key.as_slice() < previous)
            {
                return Err(corrupt(&self.path, "prefix block keys are not ordered"));
            }
            visit(&key, lsn, value_part)?;
            previous_key = Some(key);
        }
        if cursor != payload.len() {
            return Err(corrupt(&self.path, "trailing bytes in data block"));
        }
        if !was_cached && let Some(cache) = &self.block_cache {
            cache.insert(cache_key, encoded);
        }
        Ok(())
    }

    fn expect_kind(&self, expected: TableKind) -> Result<()> {
        if self.kind == expected {
            Ok(())
        } else {
            Err(Error::InvalidTable(
                "operation does not match SST kind".to_owned(),
            ))
        }
    }
}

impl TableCursor {
    pub(crate) fn current(&self) -> Option<&TableRow> {
        self.current.as_ref()
    }

    pub(crate) fn advance(&mut self) -> Result<()> {
        self.current = None;
        loop {
            if let Some(row) = self.buffered.pop_front() {
                self.current = Some(row);
                return Ok(());
            }
            let Some(block) = self.reader.blocks.get(self.block) else {
                return Ok(());
            };
            let key_prefix_len = block.key_prefix.len();
            let mut rows = VecDeque::new();
            self.reader.visit_block(block, |key, lsn, value| {
                rows.push_back(TableRow {
                    key: key.to_vec(),
                    key_prefix_len,
                    lsn,
                    value: value.to_vec(),
                });
                Ok(())
            })?;
            self.block += 1;
            self.buffered = rows;
        }
    }
}

fn encode_header(kind: TableKind, id: u64, partition: u32, format_id: &str) -> Result<Vec<u8>> {
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

fn encode_footer(footer: &Footer) -> Result<Vec<u8>> {
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

fn decode_footer(bytes: &[u8], path: &Path) -> Result<Footer> {
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

fn encode_lengths(output: &mut Vec<u8>, key: &[u8], value: &[u8]) -> Result<()> {
    let key_len = u32::try_from(key.len())
        .map_err(|_| Error::InvalidTable("key exceeds u32 length".to_owned()))?;
    let value_len = u32::try_from(value.len())
        .map_err(|_| Error::InvalidTable("value exceeds u32 length".to_owned()))?;
    output.extend_from_slice(&key_len.to_le_bytes());
    output.extend_from_slice(&value_len.to_le_bytes());
    Ok(())
}

fn encode_index_block(blocks: &[BlockMeta]) -> Result<Vec<u8>> {
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

fn read_index_block(file: &File, handle: &BlockHandle, path: &Path) -> Result<Vec<BlockMeta>> {
    let bytes = read_metadata_block(file, handle, path, INDEX_MAGIC, "index")?;
    let mut cursor = 0;
    let block_count = take_u32(&bytes, &mut cursor, path)? as usize;
    if block_count > bytes.len() / 36 {
        return Err(corrupt(path, "invalid index block count"));
    }
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        let encoding = BlockEncoding::from_byte(take(&bytes, &mut cursor, 1, path)?[0], path)?;
        if take(&bytes, &mut cursor, 3, path)? != [0; 3] {
            return Err(corrupt(path, "non-zero reserved index bytes"));
        }
        let first_key_len = take_u32(&bytes, &mut cursor, path)? as usize;
        let last_key_len = take_u32(&bytes, &mut cursor, path)? as usize;
        let key_prefix_len = take_u32(&bytes, &mut cursor, path)? as usize;
        let offset = take_u64(&bytes, &mut cursor, path)?;
        let len = take_u64(&bytes, &mut cursor, path)?;
        let record_count = take_u32(&bytes, &mut cursor, path)?;
        let first_key = take(&bytes, &mut cursor, first_key_len, path)?.to_vec();
        let last_key = take(&bytes, &mut cursor, last_key_len, path)?.to_vec();
        let key_prefix = take(&bytes, &mut cursor, key_prefix_len, path)?.to_vec();
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

fn encode_prefix_index_block(blocks: &[BlockMeta]) -> Result<Vec<u8>> {
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

fn read_prefix_index_block(
    file: &File,
    handle: &BlockHandle,
    blocks: &[BlockMeta],
    path: &Path,
) -> Result<Vec<PrefixIndexEntry>> {
    let bytes = read_metadata_block(file, handle, path, PREFIX_INDEX_MAGIC, "prefix index")?;
    let mut cursor = 0;
    let count = take_u32(&bytes, &mut cursor, path)? as usize;
    if count > bytes.len() / 20 {
        return Err(corrupt(path, "invalid prefix index count"));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key_prefix_len = take_u32(&bytes, &mut cursor, path)? as usize;
        let offset = take_u64(&bytes, &mut cursor, path)?;
        let len = take_u64(&bytes, &mut cursor, path)?;
        let key_prefix = take(&bytes, &mut cursor, key_prefix_len, path)?.to_vec();
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

fn encode_bloom_block(magic: &[u8; 8], bloom: &BloomFilter) -> Result<Vec<u8>> {
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

fn read_bloom_block(
    file: &File,
    handle: &BlockHandle,
    path: &Path,
    magic: &[u8; 8],
    name: &str,
) -> Result<BloomFilter> {
    let bytes = read_metadata_block(file, handle, path, magic, name)?;
    let mut cursor = 0;
    let bit_count = take_u64(&bytes, &mut cursor, path)?;
    let hash_count = take_u32(&bytes, &mut cursor, path)?;
    let byte_count = usize::try_from(take_u64(&bytes, &mut cursor, path)?)
        .map_err(|_| corrupt(path, format!("{name} filter does not fit in memory")))?;
    let bits = take(&bytes, &mut cursor, byte_count, path)?.to_vec();
    if cursor != bytes.len() {
        return Err(corrupt(path, format!("trailing bytes in {name} block")));
    }
    Ok(BloomFilter {
        bits,
        bit_count,
        hash_count,
    })
}

fn read_metadata_block(
    file: &File,
    handle: &BlockHandle,
    path: &Path,
    magic: &[u8; 8],
    name: &str,
) -> Result<Vec<u8>> {
    let len = usize::try_from(handle.len)
        .map_err(|_| corrupt(path, format!("{name} block does not fit in memory")))?;
    let encoded = read_exact_at(file, handle.offset, len, path)?;
    if encoded.len() < magic.len() + CHECKSUM_LEN || &encoded[..magic.len()] != magic {
        return Err(corrupt(path, format!("invalid {name} block header")));
    }
    let checksum_offset = encoded.len() - CHECKSUM_LEN;
    if checksum(&encoded[..checksum_offset]) != encoded[checksum_offset..] {
        return Err(corrupt(path, format!("{name} block checksum mismatch")));
    }
    Ok(encoded[magic.len()..checksum_offset].to_vec())
}

fn validate_metadata_handles(
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

fn validate_footer(
    path: &Path,
    footer: &Footer,
    blocks: &[BlockMeta],
    blooms: [&BloomFilter; 2],
    meta: &TableMeta,
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
    if footer.first_key != meta.first_key
        || footer.last_key != meta.last_key
        || footer.min_lsn != meta.min_lsn
        || footer.max_lsn != meta.max_lsn
        || footer.record_count != meta.record_count
    {
        return Err(corrupt(path, "footer does not match manifest"));
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

fn validate_prefix_metadata(
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

fn build_bloom(hashes: &[(u64, u64)]) -> Result<BloomFilter> {
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

fn bloom_may_contain(key: &[u8], bits: &[u8], bit_count: u64, hash_count: u32) -> bool {
    bloom_may_contain_with_seed(key, bits, bit_count, hash_count, BLOOM_SEED)
}

fn bloom_may_contain_with_seed(
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

fn bloom_hashes(key: &[u8]) -> (u64, u64) {
    hashes_with_seed(key, BLOOM_SEED)
}

fn prefix_bloom_hashes(key: &[u8]) -> (u64, u64) {
    hashes_with_seed(key, PREFIX_BLOOM_SEED)
}

fn hashes_with_seed(key: &[u8], seed: u64) -> (u64, u64) {
    let first = xxh3_64_with_seed(key, seed);
    let second = xxh3_64_with_seed(key, seed ^ 0x9e37_79b9_7f4a_7c15) | 1;
    (first, second)
}

fn joined(prefix: &[u8], suffix: &[u8]) -> Result<Vec<u8>> {
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

fn checksum(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn validate_relative_path(relative_path: &str) -> Result<()> {
    let path = Path::new(relative_path);
    if relative_path.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
                    | Component::CurDir
            )
        })
    {
        return Err(Error::InvalidTable(
            "SST path must be a normalized relative path".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| Error::Io {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(unix)]
fn read_exact_at(file: &File, offset: u64, len: usize, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = vec![0_u8; len];
    let mut read = 0;
    while read < len {
        match file.read_at(&mut bytes[read..], offset + read as u64) {
            Ok(0) => return Err(corrupt(path, "unexpected end of file")),
            Ok(count) => read += count,
            Err(source) => {
                return Err(Error::Io {
                    path: path.to_path_buf(),
                    source,
                });
            }
        }
    }
    Ok(bytes)
}

#[cfg(not(unix))]
compile_error!("strata-lsm table reads currently require positioned Unix file I/O");

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

fn corrupt(path: &Path, reason: impl Into<String>) -> Error {
    Error::CorruptTable {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
    };

    use tempfile::TempDir;

    use super::*;

    fn base_writer(directory: &TempDir) -> TableWriter {
        TableWriter::create_base(directory.path(), "base.sst", 7, 2, "base-v1").unwrap()
    }

    #[test]
    fn base_table_point_reads_and_range_scan() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        writer.add(b"b", b"two").unwrap();
        writer.add(b"c", b"three").unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        assert_eq!(reader.get(b"a").unwrap(), Some(b"one".to_vec()));
        assert_eq!(reader.get(b"missing").unwrap(), None);

        let mut rows = Vec::new();
        reader
            .scan(Some(b"b"), Some(b"d"), |key, value| {
                rows.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (b"b".to_vec(), b"two".to_vec()),
                (b"c".to_vec(), b"three".to_vec())
            ]
        );
    }

    #[test]
    fn one_table_mixes_plain_and_prefix_blocks() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"A", b"plain-a").unwrap();
        writer.add_prefix(b"K1", b"X1", b"V1Y1").unwrap();
        writer.add_prefix(b"K1", b"X2", b"V2Y2").unwrap();
        writer.add_prefix(b"K1", b"X3", b"V1Y3").unwrap();
        writer.add_prefix(b"K2", b"X1", b"V1Y4").unwrap();
        writer.add(b"Z", b"plain-z").unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        assert_eq!(reader.get(b"A").unwrap(), Some(b"plain-a".to_vec()));
        assert_eq!(reader.get(b"K1X2").unwrap(), Some(b"V2Y2".to_vec()));
        assert_eq!(reader.get(b"Z").unwrap(), Some(b"plain-z".to_vec()));
        assert_eq!(
            reader
                .blocks
                .iter()
                .map(|block| block.encoding)
                .collect::<Vec<_>>(),
            vec![
                BlockEncoding::Plain,
                BlockEncoding::Prefix,
                BlockEncoding::Prefix,
                BlockEncoding::Plain,
            ]
        );

        let mut key_prefix = Vec::new();
        reader
            .scan_key_prefix(b"K1", |key, value| {
                key_prefix.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            key_prefix,
            vec![
                (b"K1X1".to_vec(), b"V1Y1".to_vec()),
                (b"K1X2".to_vec(), b"V2Y2".to_vec()),
                (b"K1X3".to_vec(), b"V1Y3".to_vec()),
            ]
        );
    }

    #[test]
    fn prefix_patch_block_preserves_versions() {
        let directory = TempDir::new().unwrap();
        let mut writer =
            TableWriter::create_patch(directory.path(), "prefix-patch.sst", 8, 2, "patch-v1")
                .unwrap();
        writer.add_prefix_patch(b"K1", b"X1", 10, b"V1old").unwrap();
        writer.add_prefix_patch(b"K1", b"X1", 11, b"V1new").unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_patch(directory.path(), &meta, "patch-v1").unwrap();
        assert_eq!(
            reader.get_patches(b"K1X1").unwrap(),
            vec![(10, b"V1old".to_vec()), (11, b"V1new".to_vec()),]
        );
    }

    #[test]
    fn full_key_and_prefix_blooms_route_separate_lookups() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer
            .add_prefix(b"blob-000", b"/shard-0/gen-0/lsn-1", b"first")
            .unwrap();
        writer
            .add_prefix(b"blob-999", b"/shard-9/gen-9/lsn-9", b"last")
            .unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let existing_key = b"blob-999/shard-9/gen-9/lsn-9";
        assert_eq!(reader.get(existing_key).unwrap(), Some(b"last".to_vec()));
        assert!(reader.may_contain(existing_key));
        assert!(reader.prefix_may_contain(b"blob-999"));

        let missing_full_key = (1..999)
            .map(|lsn| format!("blob-000/shard-0/gen-0/lsn-{lsn}"))
            .find(|key| !reader.may_contain(key.as_bytes()))
            .expect("full-key Bloom should have a definite negative");
        assert_eq!(reader.get(missing_full_key.as_bytes()).unwrap(), None);

        let missing_prefix = (1..999)
            .map(|index| format!("blob-{index:03}"))
            .find(|prefix| !reader.prefix_may_contain(prefix.as_bytes()))
            .expect("prefix Bloom should have a definite negative");
        let mut rows = Vec::new();
        reader
            .scan_key_prefix(missing_prefix.as_bytes(), |key, _| {
                rows.push(key.to_vec());
                Ok(())
            })
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn key_prefix_can_span_target_sized_blocks() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        let value = vec![7_u8; 40 * 1024];
        writer.add_prefix(b"K", b"1", &value).unwrap();
        writer.add_prefix(b"K", b"2", &value).unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        assert_eq!(reader.blocks.len(), 2);
        assert_eq!(reader.prefix_index.len(), 2);
        let mut keys = Vec::new();
        reader
            .scan_key_prefix(b"K", |key, _| {
                keys.push(key.to_vec());
                Ok(())
            })
            .unwrap();
        assert_eq!(keys, vec![b"K1".to_vec(), b"K2".to_vec()]);
    }

    #[test]
    fn corrupted_prefix_metadata_is_rejected_on_open() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add_prefix(b"K", b"1", b"V1").unwrap();
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let prefix_index_offset = reader.footer.prefix_index.offset;
        drop(reader);

        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(
            prefix_index_offset + PREFIX_INDEX_MAGIC.len() as u64,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v1"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn patch_table_preserves_lsn_order() {
        let directory = TempDir::new().unwrap();
        let mut writer =
            TableWriter::create_patch(directory.path(), "patch.sst", 8, 2, "patch-v1").unwrap();
        writer.add_patch(b"a", 10, b"first").unwrap();
        writer.add_patch(b"a", 11, b"second").unwrap();
        writer.add_patch(b"b", 12, b"third").unwrap();
        let meta = writer.finish().unwrap();

        let reader = TableReader::open_patch(directory.path(), &meta, "patch-v1").unwrap();
        assert_eq!(
            reader.get_patches(b"a").unwrap(),
            vec![(10, b"first".to_vec()), (11, b"second".to_vec())]
        );
    }

    #[test]
    fn sparse_index_finds_keys_across_multiple_blocks() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        let value = vec![7_u8; 1024];
        for index in 0..200_u32 {
            writer
                .add(format!("key-{index:03}").as_bytes(), &value)
                .unwrap();
        }
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();

        assert!(reader.blocks.len() > 1);
        for index in [0, 63, 127, 199] {
            assert_eq!(
                reader.get(format!("key-{index:03}").as_bytes()).unwrap(),
                Some(value.clone())
            );
        }
    }

    #[test]
    fn one_patch_key_is_not_split_across_blocks() {
        let directory = TempDir::new().unwrap();
        let mut writer =
            TableWriter::create_patch(directory.path(), "large-key.sst", 12, 2, "patch-v1")
                .unwrap();
        let value = vec![3_u8; 1024];
        for lsn in 1..=100 {
            writer.add_patch(b"one-key", lsn, &value).unwrap();
        }
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_patch(directory.path(), &meta, "patch-v1").unwrap();

        assert_eq!(reader.blocks.len(), 1);
        assert_eq!(reader.get_patches(b"one-key").unwrap().len(), 100);
    }

    #[test]
    fn writer_rejects_bad_order_and_duplicate_lsns() {
        let directory = TempDir::new().unwrap();
        let mut base = base_writer(&directory);
        base.add(b"b", b"value").unwrap();
        assert!(base.add(b"a", b"value").is_err());

        let mut patch =
            TableWriter::create_patch(directory.path(), "duplicate-patch.sst", 9, 2, "patch-v1")
                .unwrap();
        patch.add_patch(b"a", 1, b"one").unwrap();
        assert!(patch.add_patch(b"a", 1, b"two").is_err());
    }

    #[test]
    fn corrupted_data_block_fails_closed() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        let meta = writer.finish().unwrap();
        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(
            HEADER_FIXED_LEN as u64 + "base-v1".len() as u64 + 8,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        assert!(matches!(reader.get(b"a"), Err(Error::CorruptTable { .. })));
    }

    #[test]
    fn corrupted_footer_is_rejected_on_open() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        let meta = writer.finish().unwrap();
        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::End(-(TRAILER_LEN as i64) + 8)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v1"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn corrupted_index_block_is_rejected_on_open() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let index_offset = reader.footer.index.offset;
        drop(reader);

        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(index_offset + INDEX_MAGIC.len() as u64))
            .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v1"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn corrupted_bloom_block_is_rejected_on_open() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let bloom_offset = reader.footer.full_bloom.offset;
        drop(reader);

        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(bloom_offset + BLOOM_MAGIC.len() as u64))
            .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v1"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn corrupted_prefix_bloom_block_is_rejected_on_open() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add_prefix(b"blob", b"/shard", b"value").unwrap();
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let prefix_bloom_offset = reader.footer.prefix_bloom.offset;
        drop(reader);

        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(
            prefix_bloom_offset + PREFIX_BLOOM_MAGIC.len() as u64,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v1"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn wrong_value_format_is_rejected() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        writer.add(b"a", b"one").unwrap();
        let meta = writer.finish().unwrap();

        assert!(matches!(
            TableReader::open_base(directory.path(), &meta, "base-v2"),
            Err(Error::CorruptTable { .. })
        ));
    }

    #[test]
    fn bloom_negative_does_not_read_a_corrupt_block() {
        let directory = TempDir::new().unwrap();
        let mut writer = base_writer(&directory);
        for index in 0..100_u32 {
            writer
                .add(format!("key-{index:03}").as_bytes(), b"value")
                .unwrap();
        }
        let meta = writer.finish().unwrap();
        let reader = TableReader::open_base(directory.path(), &meta, "base-v1").unwrap();
        let absent = (0..10_000_u32)
            .map(|index| format!("absent-{index}"))
            .find(|key| !reader.may_contain(key.as_bytes()))
            .expect("Bloom filter should have a definite negative");

        let path = directory.path().join(&meta.relative_path);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(reader.blocks[0].offset + 8))
            .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert_eq!(reader.get(absent.as_bytes()).unwrap(), None);
    }
}
