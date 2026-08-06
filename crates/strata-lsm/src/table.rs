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

use crate::{Error, Result, StrataLsn, TableMeta, table_format::*};

const TARGET_BLOCK_BYTES: usize = 64 * 1024;
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

/// One complete encoded data block — header, rows, and trailing checksum, exactly as stored
/// on disk — shared between a reader and the block cache without copying.
type EncodedBlock = Arc<[u8]>;

struct CachedBlock {
    bytes: EncodedBlock,
    /// Set on insertion and every hit; the clock hand clears it when granting a second chance.
    referenced: bool,
}

/// State for a second-chance (clock) cache.
///
/// `entries` owns the blocks and provides constant-time lookup. `clock` contains the same keys in
/// clock-hand order: its front is the next eviction candidate. A lookup marks an entry referenced
/// without moving it. When space is needed, the hand repeatedly removes the front key. Referenced
/// entries have their bit cleared and move to the back; an already-clear entry is evicted. This
/// approximates LRU without maintaining a linked recency list on every cache hit.
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

    fn get(&self, key: BlockCacheKey) -> Option<EncodedBlock> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(block) = state.entries.get_mut(&key) {
            // The next clock sweep will grant this block one full pass before it can be evicted.
            block.referenced = true;
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(Arc::clone(&block.bytes));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn insert(&self, key: BlockCacheKey, bytes: EncodedBlock) {
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

        // Advance the clock hand until the cache fits. Hot entries get one second chance per
        // observed reference; cold entries are removed when encountered with a clear bit.
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
            // Purge the removed table's clock slots so future sweeps only visit live entries.
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
struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub struct TableWriter {
    path: PathBuf,
    tmp_file: TempFileGuard,
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
    full_key_hashes: Vec<BloomHashPair>,
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
        let tmp_file = TempFileGuard::new(tmp_path);
        let mut writer = BufWriter::new(file);
        let header = encode_header(kind, id, partition, value_format_id)?;
        writer.write_all(&header).map_err(|source| Error::Io {
            path: tmp_file.path.clone(),
            source,
        })?;

        Ok(Self {
            path,
            tmp_file,
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
        encode_row(&mut block.rows, key_suffix, lsn, value)?;
        if let Some(lsn) = lsn {
            self.min_lsn = Some(self.min_lsn.map_or(lsn, |current| current.min(lsn)));
            self.max_lsn = Some(self.max_lsn.map_or(lsn, |current| current.max(lsn)));
        }
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
        let encoded = encode_data_block(
            block.layout.encoding(),
            key_prefix,
            &block.rows,
            block.record_count,
        )?;
        let len = encoded.len() as u64;
        self.writer
            .write_all(&encoded)
            .map_err(|source| Error::Io {
                path: self.tmp_file.path.clone(),
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
            path: self.tmp_file.path.clone(),
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
        let trailer = encode_trailer(footer_len, footer_checksum);
        self.writer
            .write_all(&footer_bytes)
            .and_then(|_| self.writer.write_all(&trailer))
            .and_then(|_| self.writer.flush())
            .map_err(|source| Error::Io {
                path: self.tmp_file.path.clone(),
                source,
            })?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|source| Error::Io {
                path: self.tmp_file.path.clone(),
                source,
            })?;
        let file_len = self
            .offset
            .checked_add(footer_len)
            .and_then(|len| len.checked_add(TRAILER_LEN as u64))
            .ok_or_else(|| Error::InvalidTable("table length overflow".to_owned()))?;
        drop(self.writer);
        fs::rename(&self.tmp_file.path, &self.path).map_err(|source| Error::Io {
            path: self.path.clone(),
            source,
        })?;
        self.tmp_file.disarm();
        sync_parent(&self.path)?;

        Ok(TableMeta {
            id: self.id,
            partition: self.partition,
            relative_path: self.relative_path,
            first_key,
            last_key,
            min_lsn: self.min_lsn,
            max_lsn: self.max_lsn,
            // TableWriter knows the file's rows but not the merge snapshot that produced them.
            // The full-compaction caller stamps this manifest-only field before publication.
            merge_applied_through_lsn: None,
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

/// A sorted cursor over one SST that keeps at most one decoded block in memory.
///
/// The reader is shared so many cursors can reuse one open table: iterators clone the
/// cache-enabled readers their snapshot already holds, while compaction wraps its own
/// uncached reader via [`TableReader::into_cursor`] because it reads every block exactly
/// once and must not evict hot blocks.
pub(crate) struct TableCursor {
    reader: Arc<TableReader>,
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
        TableCursor::new(Arc::new(self), None)
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
        let header = decode_header(&fixed, &path)?;
        if header.kind != expected_kind {
            return Err(corrupt(
                &path,
                "table kind does not match manifest placement",
            ));
        }
        if header.id != meta.id || header.partition != meta.partition {
            return Err(corrupt(&path, "table identity does not match manifest"));
        }
        let header_end = HEADER_FIXED_LEN
            .checked_add(header.format_len)
            .ok_or_else(|| corrupt(&path, "header length overflow"))?;
        if header_end as u64 > file_len - TRAILER_LEN as u64 {
            return Err(corrupt(&path, "value format identifier exceeds file"));
        }
        let format_bytes = read_exact_at(&file, HEADER_FIXED_LEN as u64, header.format_len, &path)?;
        if format_bytes != expected_format_id.as_bytes() {
            return Err(corrupt(&path, "value format identifier mismatch"));
        }

        let trailer_offset = file_len - TRAILER_LEN as u64;
        let trailer_bytes = read_exact_at(&file, trailer_offset, TRAILER_LEN, &path)?;
        let trailer = decode_trailer(&trailer_bytes, &path)?;
        let footer_offset = trailer_offset
            .checked_sub(trailer.footer_len)
            .ok_or_else(|| corrupt(&path, "footer length exceeds file"))?;
        if footer_offset < header_end as u64 {
            return Err(corrupt(&path, "footer overlaps table header"));
        }
        let footer_len = usize::try_from(trailer.footer_len)
            .map_err(|_| corrupt(&path, "footer does not fit in memory"))?;
        let footer_bytes = read_exact_at(&file, footer_offset, footer_len, &path)?;
        if checksum(&footer_bytes) != trailer.footer_checksum
            || meta.checksum != trailer.footer_checksum
        {
            return Err(corrupt(&path, "footer checksum mismatch"));
        }
        let footer = decode_footer(&footer_bytes, &path)?;
        validate_metadata_handles(&path, &footer, header_end as u64, footer_offset)?;
        let index_bytes = read_metadata_block(&file, &footer.index, &path, "index")?;
        let blocks = decode_index_block(&index_bytes, &path)?;
        let full_bloom_bytes =
            read_metadata_block(&file, &footer.full_bloom, &path, "full-key Bloom")?;
        let full_bloom =
            decode_bloom_block(&full_bloom_bytes, &path, BLOOM_MAGIC, "full-key Bloom")?;
        let prefix_index_bytes =
            read_metadata_block(&file, &footer.prefix_index, &path, "prefix index")?;
        let prefix_index = decode_prefix_index_block(&prefix_index_bytes, &blocks, &path)?;
        let prefix_bloom_bytes =
            read_metadata_block(&file, &footer.prefix_bloom, &path, "prefix Bloom")?;
        let prefix_bloom = decode_bloom_block(
            &prefix_bloom_bytes,
            &path,
            PREFIX_BLOOM_MAGIC,
            "prefix Bloom",
        )?;
        validate_footer(
            &path,
            &footer,
            &blocks,
            [&full_bloom, &prefix_bloom],
            header.kind,
            header_end as u64,
        )?;
        validate_manifest_footer(&path, &footer, meta)?;
        validate_prefix_metadata(&path, &blocks, &prefix_index)?;

        Ok(Self {
            id: header.id,
            path,
            file,
            kind: header.kind,
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
        let encoded: EncodedBlock = match cached {
            Some(encoded) => encoded,
            None => read_exact_at(&self.file, block.offset, len, &self.path)?.into(),
        };
        visit_data_block(
            &encoded,
            block,
            self.kind,
            &self.path,
            !was_cached,
            &mut visit,
        )?;
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
    /// Opens a cursor on the first row with key >= `start` (the table's first row for `None`).
    pub(crate) fn new(reader: Arc<TableReader>, start: Option<&[u8]>) -> Result<Self> {
        let mut cursor = Self {
            reader,
            block: 0,
            buffered: VecDeque::new(),
            current: None,
        };
        cursor.seek(start)?;
        Ok(cursor)
    }

    pub(crate) fn current(&self) -> Option<&TableRow> {
        self.current.as_ref()
    }

    /// Repositions `current` to the first row with key >= `target`, in either direction.
    ///
    /// The sparse index stores each block's last key, so `partition_point` names the only
    /// block that could contain `target` and at most that one block is decoded. With blocks
    /// `[a..c] [d..f] [g..i]`, seeking `"e"` decodes the middle block and skips its `"d"`
    /// row; seeking `"z"` exhausts the cursor without reading any block at all.
    pub(crate) fn seek(&mut self, target: Option<&[u8]>) -> Result<()> {
        self.block = target.map_or(0, |target| {
            self.reader
                .blocks
                .partition_point(|block| block.last_key.as_slice() < target)
        });
        self.buffered.clear();
        self.advance()?;
        while self
            .current()
            .is_some_and(|row| target.is_some_and(|target| row.key.as_slice() < target))
        {
            self.advance()?;
        }
        Ok(())
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

fn validate_manifest_footer(path: &Path, footer: &Footer, meta: &TableMeta) -> Result<()> {
    if footer.first_key != meta.first_key
        || footer.last_key != meta.last_key
        || footer.min_lsn != meta.min_lsn
        || footer.max_lsn != meta.max_lsn
        || footer.record_count != meta.record_count
    {
        return Err(corrupt(path, "footer does not match manifest"));
    }
    Ok(())
}

fn read_metadata_block(
    file: &File,
    handle: &BlockHandle,
    path: &Path,
    name: &str,
) -> Result<Vec<u8>> {
    let len = usize::try_from(handle.len)
        .map_err(|_| corrupt(path, format!("{name} block does not fit in memory")))?;
    read_exact_at(file, handle.offset, len, path)
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
    fn dropping_unfinished_writer_removes_its_temp_file() {
        let directory = TempDir::new().unwrap();
        let writer = TableWriter::create_patch(
            directory.path(),
            "patch-00000000000000000001.sst",
            1,
            0,
            "patch-v1",
        )
        .unwrap();
        let temp = directory.path().join("patch-00000000000000000001.sst.tmp");

        assert!(temp.exists());
        drop(writer);
        assert!(!temp.exists());
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
