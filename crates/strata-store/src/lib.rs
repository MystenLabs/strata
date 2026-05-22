//! High-level Strata blob store composed from segment files and the Strata index.
//!
//! This crate coordinates the storage protocol. Segment files hold bytes, the index holds
//! durable metadata, and the store is responsible for keeping both sides consistent across
//! writes, syncs, crashes, recovery, and sealing.
//!
//! Write path:
//!
//! ```text
//! StrataStore::put
//!   -> enqueue write command
//!   -> SegmentWriter::append
//!   -> atomic index batch:
//!        blob_versions[(key, lsn)] = BlobEntry(record_ref)
//!        segment_states[segment_id].write_offset = end_of_record
//!        segment_stats[segment_id] += record bytes
//!        store_state.next_lsn = lsn + 1
//!        pending_lsn_ops[lsn] = key
//! ```
//!
//! Sync path:
//!
//! ```text
//! StrataStore::sync
//!   -> fsync active segment bytes
//!   -> advance segment_states[active].durable_offset
//!   -> advance durable_lsn while pending LSNs are covered by durable bytes
//!   -> fsync RocksDB WAL
//! ```
//!
//! Startup path:
//!
//! ```text
//! open
//!   -> validate config and create ingest directory
//!   -> discard or reject orphan segment files with no index state
//!   -> recover unsealed segments
//!   -> verify sealed segment files according to SealedSegmentIntegrityPolicy
//!   -> choose active segment
//!   -> start writer and sealer workers
//! ```
//!
//! Crash model:
//!
//! - Unsealed segments are scanned from offset 0. The store keeps the longest valid prefix that
//!   is compatible with the recovery policy.
//! - Orphan segment files without index state are ignored by point-in-time recovery by deleting
//!   the file before any active writer is opened.
//! - Lost pending LSNs are rolled back from `blob_versions` and `pending_lsn_ops`.
//! - Sealed segments are expected to be stable. On open, their files must exist and match
//!   indexed length; optional checksum verification recomputes the sealed SHA-256 digest.
//! - `durable_lsn` means every logical operation up to that LSN is recoverable after restart.
//!
//! Read path:
//!
//! ```text
//! get_sliver
//!   -> blob index lookup
//!   -> SegmentReader::read_record
//!   -> verify record key
//!   -> verify full-record checksum unless ReadOptions disables it
//!
//! stream_sliver
//!   -> blob index lookup
//!   -> read record header and key trailer
//!   -> validate requested payload range
//!   -> return a blocking file-range stream
//! ```

mod error;

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs,
    io::Read,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use strata_core::{
    BlobEntry, BlobKey, BlobLifecycle, BlobState, BlobVersionKey, DecodedRecord, Epoch,
    PlacementClass, RecordRef, SegmentFileState, SegmentId, SegmentState, SegmentStats, StrataLsn,
    StrataStoreState,
};
use strata_index::StrataIndex;
use strata_segment::Error as SegmentError;
use strata_segment::{
    RecordMetadata, SegmentPayloadStream, SegmentReadOptions, SegmentReadProfile, SegmentReader,
    SegmentScanner, SegmentWriter,
};
use typed_store::Map;

pub use error::{Error, Result};

const INGEST_DIR: &str = "ingest";
const INDEX_DIR: &str = "index";
const FIRST_SEGMENT_ID: SegmentId = 1;
const SEAL_BACKLOG_WAIT: Duration = Duration::from_millis(10);
pub const DEFAULT_SEGMENT_READER_CACHE_CAPACITY: usize = 64;

/// Runtime configuration for one Strata store namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrataStoreConfig {
    pub root_dir: PathBuf,
    pub namespace: String,
    pub segment_max_bytes: u64,
    pub write_queue_capacity: usize,
    pub max_unsealed_segments: usize,
    pub segment_reader_cache_capacity: usize,
    pub recovery_policy: StrataRecoveryPolicy,
    pub sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy,
}

/// Options for point reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    pub verify_checksum: bool,
}

impl ReadOptions {
    pub const fn verify_checksums() -> Self {
        Self {
            verify_checksum: true,
        }
    }

    pub const fn skip_checksum_verification() -> Self {
        Self {
            verify_checksum: false,
        }
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self::verify_checksums()
    }
}

impl From<ReadOptions> for SegmentReadOptions {
    fn from(options: ReadOptions) -> Self {
        Self {
            verify_checksum: options.verify_checksum,
        }
    }
}

/// Temporary get-path timings for benchmark diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreGetProfile {
    pub record_lookup: Duration,
    pub reader_acquire: Duration,
    pub fixed_header: Duration,
    pub buffer_alloc: Duration,
    pub record_body: Duration,
    pub decode: Duration,
    pub key_validate: Duration,
}

/// Policy used when recovering unsealed ingest segments after a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrataRecoveryPolicy {
    /// Recover the longest globally ordered prefix of unsealed segment data.
    PointInTime,
    /// Fail store open if unsealed segment files do not exactly match indexed offsets.
    AbsoluteConsistency,
}

/// Policy used when verifying sealed segment files during store open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedSegmentIntegrityPolicy {
    /// Verify each sealed segment file exists and has the indexed sealed length.
    MetadataOnly,
    /// Verify metadata and recompute the sealed segment SHA-256 digest.
    Checksum,
}

impl StrataStoreConfig {
    pub fn namespace_dir(&self) -> PathBuf {
        self.root_dir.join(&self.namespace)
    }

    pub fn ingest_dir(&self) -> PathBuf {
        self.namespace_dir().join(INGEST_DIR)
    }

    pub fn standalone_index_dir(&self) -> PathBuf {
        self.namespace_dir().join(INDEX_DIR)
    }

    pub fn index_cf_prefix(&self) -> String {
        format!("strata/{}", self.namespace)
    }
}

/// Single-namespace Strata store.
#[derive(Debug)]
pub struct StrataStore {
    config: StrataStoreConfig,
    index: StrataIndex,
    write_tx: Option<mpsc::SyncSender<WriteCommand>>,
    writer_handle: Option<JoinHandle<()>>,
    seal_tx: Option<mpsc::Sender<SealCommand>>,
    seal_handle: Option<JoinHandle<()>>,
    reader_cache: SegmentReaderCache,
}

impl StrataStore {
    /// Opens a standalone store.
    ///
    /// Segment files are stored at `root_dir/namespace/ingest`, and the index RocksDB lives at
    /// `root_dir/namespace/index`.
    pub fn open_standalone(config: StrataStoreConfig) -> Result<Self> {
        let index =
            StrataIndex::open_path(config.standalone_index_dir(), config.index_cf_prefix())?;
        Self::from_index(config, index)
    }

    /// Opens a store using a caller-provided index.
    ///
    /// This mode creates only segment directories. It does not create a local index directory.
    pub fn from_index(config: StrataStoreConfig, index: StrataIndex) -> Result<Self> {
        validate_config(&config)?;
        ensure_ingest_dir(&config)?;
        reconcile_orphan_ingest_segment_files(&config, &index)?;
        recover_unsealed_segments(&config, &index)?;
        verify_sealed_segments(&config, &index)?;
        let active_segment_id = choose_active_segment_id(&index)?;
        let active_writer = open_active_writer(&config, active_segment_id)?;
        let durable_offset = active_segment_durable_offset(&index, active_writer.segment_id())?;
        let store_state = index.get_store_state()?.unwrap_or_default();
        publish_active_segment_state(&config, &index, &active_writer, durable_offset)?;

        let (seal_tx, seal_rx) = mpsc::channel();
        let seal_worker = SealWorker {
            config: config.clone(),
            index: index.clone(),
            seal_rx,
        };
        let seal_handle = thread::Builder::new()
            .name(format!("strata-sealer-{}", config.namespace))
            .spawn(move || seal_worker.run())
            .map_err(|source| Error::SealThreadSpawn { source })?;
        enqueue_unsealed_segments_for_sealing(&index, active_segment_id, &seal_tx)?;

        let (write_tx, write_rx) = mpsc::sync_channel(config.write_queue_capacity);
        let coordinator = WriteCoordinator {
            config: config.clone(),
            index: index.clone(),
            active_writer,
            durable_offset,
            next_lsn: store_state.next_lsn,
            seal_tx: seal_tx.clone(),
            write_rx,
        };
        let writer_handle = thread::Builder::new()
            .name(format!("strata-writer-{}", config.namespace))
            .spawn(move || coordinator.run())
            .map_err(|source| Error::ThreadSpawn { source })?;

        Ok(Self {
            reader_cache: SegmentReaderCache::new(config.segment_reader_cache_capacity),
            config,
            index,
            write_tx: Some(write_tx),
            writer_handle: Some(writer_handle),
            seal_tx: Some(seal_tx),
            seal_handle: Some(seal_handle),
        })
    }

    pub fn config(&self) -> &StrataStoreConfig {
        &self.config
    }

    pub fn index(&self) -> &StrataIndex {
        &self.index
    }

    pub fn put(
        &self,
        key: &BlobKey,
        lifecycle: BlobLifecycle,
        payload: &[u8],
    ) -> Result<StrataLsn> {
        self.put_arc(key.clone(), lifecycle, Arc::from(payload))
    }

    pub fn put_arc(
        &self,
        key: BlobKey,
        lifecycle: BlobLifecycle,
        payload: Arc<[u8]>,
    ) -> Result<StrataLsn> {
        let (response_tx, response_rx) = mpsc::channel();
        self.write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)?
            .send(WriteCommand::Put(WriteRequest {
                key,
                lifecycle,
                payload,
                response_tx,
            }))
            .map_err(|_| Error::WriteQueueClosed)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    pub fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        self.get_sliver(key)
    }

    pub fn get_with_options(&self, key: &BlobKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.get_sliver_with_options(key, options)
    }

    pub fn get_sliver(&self, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        self.get_sliver_with_options(key, ReadOptions::default())
    }

    pub fn get_sliver_with_options(
        &self,
        key: &BlobKey,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        let Some(record_ref) = self.live_record_ref(key)? else {
            return Ok(None);
        };

        let record =
            self.reader_cache
                .read_record_with_options(&self.config, record_ref, options.into())?;
        if &record.key != key {
            return Err(Error::KeyMismatch {
                requested: key.clone(),
                found: record.key,
            });
        }

        Ok(Some(record.payload))
    }

    pub fn get_sliver_profiled(&self, key: &BlobKey) -> Result<(Option<Vec<u8>>, StoreGetProfile)> {
        self.get_sliver_profiled_with_options(key, ReadOptions::default())
    }

    pub fn get_sliver_profiled_with_options(
        &self,
        key: &BlobKey,
        options: ReadOptions,
    ) -> Result<(Option<Vec<u8>>, StoreGetProfile)> {
        let mut profile = StoreGetProfile::default();

        let started = Instant::now();
        let Some(record_ref) = self.live_record_ref(key)? else {
            profile.record_lookup = started.elapsed();
            return Ok((None, profile));
        };
        profile.record_lookup = started.elapsed();

        let (record, reader_profile) = self.reader_cache.read_record_profiled(
            &self.config,
            record_ref,
            options.into(),
            &mut profile.reader_acquire,
        )?;
        profile.fixed_header = reader_profile.fixed_header;
        profile.buffer_alloc = reader_profile.buffer_alloc;
        profile.record_body = reader_profile.record_body;
        profile.decode = reader_profile.decode;

        let started = Instant::now();
        if &record.key != key {
            return Err(Error::KeyMismatch {
                requested: key.clone(),
                found: record.key,
            });
        }
        profile.key_validate = started.elapsed();

        Ok((Some(record.payload), profile))
    }

    pub fn get_sliver_range(
        &self,
        key: &BlobKey,
        payload_range: Range<u64>,
    ) -> Result<Option<Vec<u8>>> {
        let Some(mut stream) = self.stream_sliver(key, payload_range)? else {
            return Ok(None);
        };

        let mut payload = Vec::new();
        stream
            .read_to_end(&mut payload)
            .map_err(|source| Error::Io {
                path: stream.path().to_path_buf(),
                source,
            })?;
        Ok(Some(payload))
    }

    pub fn stream_sliver(
        &self,
        key: &BlobKey,
        payload_range: Range<u64>,
    ) -> Result<Option<SegmentPayloadStream>> {
        let Some(record_ref) = self.live_record_ref(key)? else {
            return Ok(None);
        };

        let metadata = self
            .reader_cache
            .read_record_metadata(&self.config, record_ref)?;
        if metadata.key != *key {
            return Err(Error::KeyMismatch {
                requested: key.clone(),
                found: metadata.key,
            });
        }

        Ok(Some(self.reader_cache.open_payload_stream(
            &self.config,
            record_ref,
            payload_range,
        )?))
    }

    pub fn contains(&self, key: &BlobKey) -> Result<bool> {
        Ok(self.live_record_ref(key)?.is_some())
    }

    /// Drops the cached file descriptor for a segment.
    ///
    /// Segment cleanup must call this before unlinking or reusing a segment path. The read path
    /// checks indexed segment state before serving refs, so an old cached descriptor cannot bypass
    /// a published `Deleting` or `Deleted` state.
    pub fn evict_segment_reader(&self, segment_id: SegmentId) {
        self.reader_cache.evict(segment_id);
    }

    pub fn tombstone(&self, key: &BlobKey) -> Result<StrataLsn> {
        let (response_tx, response_rx) = mpsc::channel();
        self.write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)?
            .send(WriteCommand::Tombstone(TombstoneRequest {
                key: key.clone(),
                response_tx,
            }))
            .map_err(|_| Error::WriteQueueClosed)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    pub fn extend(&self, key: &BlobKey, new_logical_end_epoch: Epoch) -> Result<Option<StrataLsn>> {
        let (response_tx, response_rx) = mpsc::channel();
        self.write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)?
            .send(WriteCommand::Extend(ExtendRequest {
                key: key.clone(),
                new_logical_end_epoch,
                response_tx,
            }))
            .map_err(|_| Error::WriteQueueClosed)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    pub fn sync(&self) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        self.write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)?
            .send(WriteCommand::Sync(response_tx))
            .map_err(|_| Error::WriteQueueClosed)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    pub fn durable_lsn(&self) -> Result<StrataLsn> {
        Ok(self
            .index
            .get_store_state()?
            .unwrap_or_default()
            .durable_lsn)
    }

    fn live_record_ref(&self, key: &BlobKey) -> Result<Option<RecordRef>> {
        let Some(entry) = self.index.get_blob_entry(key)? else {
            return Ok(None);
        };
        if entry.state == BlobState::Tombstoned {
            return Ok(None);
        }
        let Some(record_ref) = entry.record_ref else {
            return Ok(None);
        };
        if !self.segment_is_readable(record_ref.segment_id)? {
            self.evict_segment_reader(record_ref.segment_id);
            return Ok(None);
        }
        Ok(Some(record_ref))
    }

    fn segment_is_readable(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(self
            .index
            .get_segment_state(segment_id)?
            .is_some_and(|state| segment_state_is_readable(state.state)))
    }

    #[cfg(test)]
    fn reader_cache_len(&self) -> usize {
        self.reader_cache.len()
    }
}

#[derive(Debug)]
struct SegmentReaderCache {
    capacity: usize,
    inner: Mutex<SegmentReaderCacheInner>,
}

#[derive(Debug, Default)]
struct SegmentReaderCacheInner {
    readers: HashMap<SegmentId, Arc<Mutex<SegmentReader>>>,
    lru: VecDeque<SegmentId>,
}

impl SegmentReaderCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(SegmentReaderCacheInner::default()),
        }
    }

    fn read_record_with_options(
        &self,
        config: &StrataStoreConfig,
        record_ref: RecordRef,
        options: SegmentReadOptions,
    ) -> Result<DecodedRecord> {
        self.with_reader(config, record_ref.segment_id, |reader| {
            reader.read_record_with_options(record_ref, options)
        })
    }

    fn read_record_profiled(
        &self,
        config: &StrataStoreConfig,
        record_ref: RecordRef,
        options: SegmentReadOptions,
        reader_acquire: &mut Duration,
    ) -> Result<(DecodedRecord, SegmentReadProfile)> {
        if self.capacity == 0 {
            let path = segment_path(config, record_ref.segment_id);
            let started = Instant::now();
            let mut reader = SegmentReader::open(&path, record_ref.segment_id)?;
            *reader_acquire = started.elapsed();
            return Ok(reader.read_record_profiled_with_options(record_ref, options)?);
        }

        let started = Instant::now();
        let reader = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let reader = if let Some(reader) = inner.readers.get(&record_ref.segment_id) {
                Arc::clone(reader)
            } else {
                let path = segment_path(config, record_ref.segment_id);
                let reader = Arc::new(Mutex::new(SegmentReader::open(
                    &path,
                    record_ref.segment_id,
                )?));
                inner
                    .readers
                    .insert(record_ref.segment_id, Arc::clone(&reader));
                reader
            };
            inner.touch(record_ref.segment_id);
            inner.enforce_capacity(self.capacity);
            reader
        };
        let result = {
            let mut reader = reader
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *reader_acquire = started.elapsed();
            reader.read_record_profiled_with_options(record_ref, options)
        };
        if result.is_err() {
            self.evict(record_ref.segment_id);
        }
        Ok(result?)
    }

    fn read_record_metadata(
        &self,
        config: &StrataStoreConfig,
        record_ref: RecordRef,
    ) -> Result<RecordMetadata> {
        self.with_reader(config, record_ref.segment_id, |reader| {
            reader.read_record_metadata(record_ref)
        })
    }

    fn open_payload_stream(
        &self,
        config: &StrataStoreConfig,
        record_ref: RecordRef,
        payload_range: Range<u64>,
    ) -> Result<SegmentPayloadStream> {
        self.with_reader(config, record_ref.segment_id, |reader| {
            reader.open_payload_stream(record_ref, payload_range)
        })
    }

    fn evict(&self, segment_id: SegmentId) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.remove(segment_id);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .readers
            .len()
    }

    fn with_reader<T>(
        &self,
        config: &StrataStoreConfig,
        segment_id: SegmentId,
        read: impl FnOnce(&mut SegmentReader) -> strata_segment::Result<T>,
    ) -> Result<T> {
        if self.capacity == 0 {
            let path = segment_path(config, segment_id);
            let mut reader = SegmentReader::open(&path, segment_id)?;
            return Ok(read(&mut reader)?);
        }

        let reader = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let reader = if let Some(reader) = inner.readers.get(&segment_id) {
                Arc::clone(reader)
            } else {
                let path = segment_path(config, segment_id);
                let reader = Arc::new(Mutex::new(SegmentReader::open(&path, segment_id)?));
                inner.readers.insert(segment_id, Arc::clone(&reader));
                reader
            };
            inner.touch(segment_id);
            inner.enforce_capacity(self.capacity);
            reader
        };
        let result = {
            let mut reader = reader
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            read(&mut reader)
        };
        if result.is_err() {
            self.evict(segment_id);
        }
        Ok(result?)
    }
}

impl SegmentReaderCacheInner {
    fn touch(&mut self, segment_id: SegmentId) {
        if let Some(position) = self.lru.iter().position(|cached| *cached == segment_id) {
            self.lru.remove(position);
        }
        self.lru.push_back(segment_id);
    }

    fn enforce_capacity(&mut self, capacity: usize) {
        while self.readers.len() > capacity {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            self.readers.remove(&evicted);
        }
    }

    fn remove(&mut self, segment_id: SegmentId) {
        self.readers.remove(&segment_id);
        if let Some(position) = self.lru.iter().position(|cached| *cached == segment_id) {
            self.lru.remove(position);
        }
    }
}

impl Drop for StrataStore {
    fn drop(&mut self) {
        if let Some(write_tx) = self.write_tx.take() {
            let _ = write_tx.send(WriteCommand::Shutdown);
        }
        if let Some(writer_handle) = self.writer_handle.take() {
            let _ = writer_handle.join();
        }
        if let Some(seal_tx) = self.seal_tx.take() {
            let _ = seal_tx.send(SealCommand::Shutdown);
        }
        if let Some(seal_handle) = self.seal_handle.take() {
            let _ = seal_handle.join();
        }
    }
}

#[derive(Debug)]
enum WriteCommand {
    Put(WriteRequest),
    Tombstone(TombstoneRequest),
    Extend(ExtendRequest),
    Sync(mpsc::Sender<Result<()>>),
    Shutdown,
}

#[derive(Debug)]
struct WriteRequest {
    key: BlobKey,
    lifecycle: BlobLifecycle,
    payload: Arc<[u8]>,
    response_tx: mpsc::Sender<Result<StrataLsn>>,
}

#[derive(Debug)]
struct TombstoneRequest {
    key: BlobKey,
    response_tx: mpsc::Sender<Result<StrataLsn>>,
}

#[derive(Debug)]
struct ExtendRequest {
    key: BlobKey,
    new_logical_end_epoch: Epoch,
    response_tx: mpsc::Sender<Result<Option<StrataLsn>>>,
}

#[derive(Debug)]
struct WriteCoordinator {
    config: StrataStoreConfig,
    index: StrataIndex,
    active_writer: SegmentWriter,
    durable_offset: u64,
    next_lsn: StrataLsn,
    seal_tx: mpsc::Sender<SealCommand>,
    write_rx: mpsc::Receiver<WriteCommand>,
}

#[derive(Debug)]
enum SealCommand {
    Seal(SegmentSealTask),
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
struct SegmentSealTask {
    segment_id: SegmentId,
    sealed_len: u64,
}

#[derive(Debug)]
struct SealWorker {
    config: StrataStoreConfig,
    index: StrataIndex,
    seal_rx: mpsc::Receiver<SealCommand>,
}

impl WriteCoordinator {
    fn run(mut self) {
        while let Ok(command) = self.write_rx.recv() {
            match command {
                WriteCommand::Put(request) => {
                    let result =
                        self.process_put(&request.key, request.lifecycle, &request.payload);
                    let _ = request.response_tx.send(result);
                }
                WriteCommand::Tombstone(request) => {
                    let result = self.process_tombstone(&request.key);
                    let _ = request.response_tx.send(result);
                }
                WriteCommand::Extend(request) => {
                    let result = self.process_extend(&request.key, request.new_logical_end_epoch);
                    let _ = request.response_tx.send(result);
                }
                WriteCommand::Sync(response_tx) => {
                    let result = self.sync_data();
                    let _ = response_tx.send(result);
                }
                WriteCommand::Shutdown => break,
            }
        }
    }

    fn process_put(
        &mut self,
        key: &BlobKey,
        lifecycle: BlobLifecycle,
        payload: &[u8],
    ) -> Result<StrataLsn> {
        loop {
            match self.try_process_put(key, lifecycle, payload) {
                Err(Error::Segment(SegmentError::SegmentFull { .. }))
                    if self.active_writer.write_offset() > 0 =>
                {
                    self.rollover_active_segment()?;
                }
                result => return result,
            }
        }
    }

    fn try_process_put(
        &mut self,
        key: &BlobKey,
        lifecycle: BlobLifecycle,
        payload: &[u8],
    ) -> Result<StrataLsn> {
        let lsn = self.next_lsn;
        let generation = lsn;
        let next_lsn = lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        let outcome = self
            .active_writer
            .append(key, lifecycle, generation, payload)?;
        let entry = BlobEntry {
            record_ref: Some(outcome.record_ref),
            lsn,
            generation,
            state: BlobState::Live,
            lifecycle,
        };
        let mut stats = self
            .index
            .get_segment_stats(outcome.record_ref.segment_id)?
            .unwrap_or_default();
        stats.total_bytes = stats.total_bytes.saturating_add(outcome.record_len);
        stats.live_bytes = stats.live_bytes.saturating_add(outcome.record_len);
        stats.live_ref_count = stats.live_ref_count.saturating_add(1);
        add_live_lifecycle_stats(
            &mut stats,
            self.active_writer.placement_class(),
            outcome.record_len,
            lifecycle,
        );

        let existing_state = self
            .index
            .get_segment_state(outcome.record_ref.segment_id)?;
        let state = active_segment_state_with_lsn(
            &self.config,
            &self.active_writer,
            self.durable_offset,
            existing_state.as_ref(),
            Some(lsn),
        );
        let store_state = StrataStoreState {
            next_lsn,
            durable_lsn: self
                .index
                .get_store_state()?
                .unwrap_or_default()
                .durable_lsn,
        };
        let mut batch = self.index.batch();
        self.index.put_blob_version_batch(&mut batch, key, &entry)?;
        batch
            .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
            .map_err(strata_index::Error::from)?;
        batch
            .insert_batch(
                self.index.segment_stats(),
                [(&outcome.record_ref.segment_id, &stats)],
            )
            .map_err(strata_index::Error::from)?;
        batch
            .insert_batch(self.index.store_state(), [((), &store_state)])
            .map_err(strata_index::Error::from)?;
        batch
            .insert_batch(self.index.pending_lsn_ops(), [(&lsn, key)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.next_lsn = next_lsn;

        Ok(lsn)
    }

    fn process_tombstone(&mut self, key: &BlobKey) -> Result<StrataLsn> {
        let lsn = self.next_lsn;
        let generation = lsn;
        let next_lsn = lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        let entry = BlobEntry {
            record_ref: None,
            lsn,
            generation,
            state: BlobState::Tombstoned,
            lifecycle: BlobLifecycle::new(0),
        };
        let store_state = StrataStoreState {
            next_lsn,
            durable_lsn: self
                .index
                .get_store_state()?
                .unwrap_or_default()
                .durable_lsn,
        };

        let mut batch = self.index.batch();
        self.index.put_blob_version_batch(&mut batch, key, &entry)?;
        batch
            .insert_batch(self.index.store_state(), [((), &store_state)])
            .map_err(strata_index::Error::from)?;
        batch
            .insert_batch(self.index.pending_lsn_ops(), [(&lsn, key)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.next_lsn = next_lsn;

        Ok(lsn)
    }

    fn process_extend(
        &mut self,
        key: &BlobKey,
        new_logical_end_epoch: Epoch,
    ) -> Result<Option<StrataLsn>> {
        let Some(existing_entry) = self.index.get_blob_entry(key)? else {
            return Ok(None);
        };
        if existing_entry.state == BlobState::Tombstoned {
            return Ok(None);
        }

        let lsn = self.next_lsn;
        let next_lsn = lsn
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        let mut lifecycle = existing_entry.lifecycle;
        lifecycle.extend_to(new_logical_end_epoch);
        let entry = BlobEntry {
            record_ref: existing_entry.record_ref,
            lsn,
            generation: existing_entry.generation,
            state: BlobState::Live,
            lifecycle,
        };
        let store_state = StrataStoreState {
            next_lsn,
            durable_lsn: self
                .index
                .get_store_state()?
                .unwrap_or_default()
                .durable_lsn,
        };

        let segment_update = if let Some(record_ref) = existing_entry.record_ref {
            let segment_state = self.index.get_segment_state(record_ref.segment_id)?;
            let mut stats = self
                .index
                .get_segment_stats(record_ref.segment_id)?
                .unwrap_or_default();
            update_live_lifecycle_stats(
                &mut stats,
                segment_state
                    .as_ref()
                    .map_or(PlacementClass::Ingest, |state| state.placement_class),
                record_ref.len,
                existing_entry.lifecycle,
                lifecycle,
            );
            Some((record_ref.segment_id, stats))
        } else {
            None
        };

        let mut batch = self.index.batch();
        self.index.put_blob_version_batch(&mut batch, key, &entry)?;
        if let Some((segment_id, stats)) = &segment_update {
            batch
                .insert_batch(self.index.segment_stats(), [(segment_id, stats)])
                .map_err(strata_index::Error::from)?;
        }
        batch
            .insert_batch(self.index.store_state(), [((), &store_state)])
            .map_err(strata_index::Error::from)?;
        batch
            .insert_batch(self.index.pending_lsn_ops(), [(&lsn, key)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.next_lsn = next_lsn;

        Ok(Some(lsn))
    }

    fn rollover_active_segment(&mut self) -> Result<()> {
        self.wait_for_seal_backlog_capacity()?;
        let old_segment_id = self.active_writer.segment_id();
        let old_write_offset = self.active_writer.write_offset();
        let new_segment_id = old_segment_id
            .checked_add(1)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        let new_path = segment_path(&self.config, new_segment_id);
        if new_path.exists() && self.index.get_segment_state(new_segment_id)?.is_none() {
            fs::remove_file(&new_path).map_err(|source| Error::Io {
                path: new_path.clone(),
                source,
            })?;
        }

        let new_writer = SegmentWriter::create(
            &new_path,
            new_segment_id,
            PlacementClass::Ingest,
            self.config.segment_max_bytes,
        )?;
        let new_state = active_segment_state(&self.config, &new_writer, 0);
        let mut old_state = active_segment_state_from_path(
            &self.config,
            old_segment_id,
            old_write_offset,
            self.durable_offset,
        );
        if let Some(existing) = self.index.get_segment_state(old_segment_id)? {
            old_state.volume_id = existing.volume_id;
            old_state.placement_class = existing.placement_class;
            old_state.durable_offset = existing.durable_offset;
            old_state.min_lsn = existing.min_lsn;
            old_state.max_lsn = existing.max_lsn;
        }
        old_state.state = SegmentFileState::Sealing;

        let mut batch = self.index.batch();
        batch
            .insert_batch(
                self.index.segment_states(),
                [
                    (&old_state.segment_id, &old_state),
                    (&new_state.segment_id, &new_state),
                ],
            )
            .map_err(strata_index::Error::from)?;
        if self.index.get_segment_stats(new_segment_id)?.is_none() {
            batch
                .insert_batch(
                    self.index.segment_stats(),
                    [(&new_segment_id, &SegmentStats::default())],
                )
                .map_err(strata_index::Error::from)?;
        }
        batch.write().map_err(strata_index::Error::from)?;

        self.seal_tx
            .send(SealCommand::Seal(SegmentSealTask {
                segment_id: old_segment_id,
                sealed_len: old_write_offset,
            }))
            .map_err(|_| Error::SealQueueClosed)?;
        self.active_writer = new_writer;
        self.durable_offset = 0;
        Ok(())
    }

    fn wait_for_seal_backlog_capacity(&self) -> Result<()> {
        loop {
            if let Some(segment_id) = first_seal_failed_segment(&self.index)? {
                return Err(Error::SealFailed { segment_id });
            }
            if unsealed_ingest_segment_count(&self.index)? < self.config.max_unsealed_segments {
                return Ok(());
            }
            thread::sleep(SEAL_BACKLOG_WAIT);
        }
    }

    fn sync_data(&mut self) -> Result<()> {
        let durable_offset = self.active_writer.write_offset();
        self.active_writer.sync_data()?;

        let existing_state = self
            .index
            .get_segment_state(self.active_writer.segment_id())?;
        let state = active_segment_state_with_lsn(
            &self.config,
            &self.active_writer,
            durable_offset,
            existing_state.as_ref(),
            None,
        );
        let mut batch = self.index.batch();
        batch
            .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
            .map_err(strata_index::Error::from)?;
        let store_state =
            store_state_with_advanced_durable_lsn(&self.index, Some(&state), &mut batch)?;
        batch
            .insert_batch(self.index.store_state(), [((), &store_state)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        self.durable_offset = durable_offset;
        Ok(())
    }
}

impl SealWorker {
    fn run(self) {
        while let Ok(command) = self.seal_rx.recv() {
            match command {
                SealCommand::Seal(task) => {
                    if self.seal_segment(task).is_err() {
                        let _ = self.mark_seal_failed(task.segment_id);
                    }
                }
                SealCommand::Shutdown => break,
            }
        }
    }

    fn seal_segment(&self, task: SegmentSealTask) -> Result<()> {
        if let Some(existing) = self.index.get_segment_state(task.segment_id)?
            && existing.state == SegmentFileState::Sealed
        {
            return Ok(());
        }

        let path = segment_path(&self.config, task.segment_id);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
        file.sync_data().map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let sealed_sha256 = sha256_file_prefix(&path, task.sealed_len)?;

        let mut state = active_segment_state_from_path(
            &self.config,
            task.segment_id,
            task.sealed_len,
            task.sealed_len,
        );
        if let Some(existing) = self.index.get_segment_state(task.segment_id)? {
            state.volume_id = existing.volume_id;
            state.placement_class = existing.placement_class;
            state.min_lsn = existing.min_lsn;
            state.max_lsn = existing.max_lsn;
        }
        state.state = SegmentFileState::Sealed;
        state.sealed_len = Some(task.sealed_len);
        state.sealed_sha256 = Some(sealed_sha256);

        let mut batch = self.index.batch();
        batch
            .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
            .map_err(strata_index::Error::from)?;
        let store_state =
            store_state_with_advanced_durable_lsn(&self.index, Some(&state), &mut batch)?;
        batch
            .insert_batch(self.index.store_state(), [((), &store_state)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        Ok(())
    }

    fn mark_seal_failed(&self, segment_id: SegmentId) -> Result<()> {
        let Some(mut state) = self.index.get_segment_state(segment_id)? else {
            return Ok(());
        };
        if state.state == SegmentFileState::Sealed {
            return Ok(());
        }
        state.state = SegmentFileState::SealFailed;
        let mut batch = self.index.batch();
        batch
            .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        Ok(())
    }
}

fn open_active_writer(
    config: &StrataStoreConfig,
    active_segment_id: SegmentId,
) -> Result<SegmentWriter> {
    ensure_ingest_dir(config)?;

    let active_path = segment_path(config, active_segment_id);
    if active_path.exists() {
        Ok(SegmentWriter::open_existing(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
        )?)
    } else {
        Ok(SegmentWriter::create(
            &active_path,
            active_segment_id,
            PlacementClass::Ingest,
            config.segment_max_bytes,
        )?)
    }
}

fn ensure_ingest_dir(config: &StrataStoreConfig) -> Result<()> {
    fs::create_dir_all(config.ingest_dir()).map_err(|source| Error::Io {
        path: config.ingest_dir(),
        source,
    })
}

fn reconcile_orphan_ingest_segment_files(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    let indexed_segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .collect::<BTreeSet<_>>();
    let ingest_dir = config.ingest_dir();
    let entries = fs::read_dir(&ingest_dir).map_err(|source| Error::Io {
        path: ingest_dir.clone(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: ingest_dir.clone(),
            source,
        })?;
        let path = entry.path();
        let Some(file_name) = path.file_name() else {
            continue;
        };
        let Some(segment_id) = parse_segment_file_name(file_name) else {
            continue;
        };
        if indexed_segment_ids.contains(&segment_id) {
            continue;
        }

        match config.recovery_policy {
            StrataRecoveryPolicy::PointInTime => {
                fs::remove_file(&path).map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            }
            StrataRecoveryPolicy::AbsoluteConsistency => {
                return Err(Error::OrphanSegmentFile { segment_id, path });
            }
        }
    }

    Ok(())
}

fn recover_unsealed_segments(config: &StrataStoreConfig, index: &StrataIndex) -> Result<()> {
    let mut discard_later_segments = false;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if discard_later_segments {
            discard_unsealed_segment(config, index, segment_id)?;
            continue;
        }

        let recovered = recover_unsealed_segment(config, index, segment_id)?;
        if !recovered.is_complete {
            discard_later_segments = true;
        }
    }
    rollback_lost_operations(index)?;
    advance_recovered_durable_lsn(index)?;
    Ok(())
}

fn verify_sealed_segments(config: &StrataStoreConfig, index: &StrataIndex) -> Result<()> {
    for (_, state) in index.iter_segment_states()? {
        if state.state == SegmentFileState::Sealed {
            verify_sealed_segment(config, &state)?;
        }
    }
    Ok(())
}

fn verify_sealed_segment(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    let segment_id = state.segment_id;
    let path = segment_path(config, segment_id);
    let expected_len = state
        .sealed_len
        .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::SealedSegmentMissing { segment_id, path });
        }
        Err(source) => {
            return Err(Error::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let actual_len = metadata.len();
    if actual_len != expected_len {
        return Err(Error::SealedSegmentLengthMismatch {
            segment_id,
            path,
            expected_len,
            actual_len,
        });
    }

    if config.sealed_segment_integrity_policy == SealedSegmentIntegrityPolicy::Checksum {
        let expected = state
            .sealed_sha256
            .ok_or(Error::SealedSegmentMissingChecksum { segment_id })?;
        let actual = sha256_file_prefix(&path, expected_len)?;
        if actual != expected {
            return Err(Error::SealedSegmentChecksumMismatch {
                segment_id,
                path,
                expected,
                actual,
            });
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentRecovery {
    is_complete: bool,
}

fn recover_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
) -> Result<SegmentRecovery> {
    let path = segment_path(config, segment_id);
    let existing_state = index.get_segment_state(segment_id)?;
    let expected_write_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.write_offset);
    let durable_offset = existing_state
        .as_ref()
        .map_or(0, |state| state.durable_offset);
    if !path.exists() {
        if expected_write_offset == 0 {
            return Ok(SegmentRecovery { is_complete: true });
        }
        return recover_missing_unsealed_segment(
            config,
            index,
            segment_id,
            expected_write_offset,
            durable_offset,
        );
    }

    let mut scanner = SegmentScanner::open(&path, segment_id)?;
    let prefix = scanner.scan_recoverable_prefix(durable_offset)?;
    let is_complete = prefix.valid_len >= expected_write_offset;
    let recovered_write_offset = prefix.valid_len.min(expected_write_offset);
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency
        && (prefix.valid_len != expected_write_offset || prefix.file_len != expected_write_offset)
    {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset,
        });
    }

    let recovered_durable_offset = persist_recovered_segment_prefix(
        &path,
        prefix.file_len,
        durable_offset,
        recovered_write_offset,
    )?;

    apply_recovered_segment_prefix(
        config,
        index,
        segment_id,
        existing_state,
        recovered_durable_offset,
        recovered_write_offset,
        &prefix.records,
    )?;
    Ok(SegmentRecovery { is_complete })
}

fn persist_recovered_segment_prefix(
    path: &Path,
    file_len: u64,
    durable_offset: u64,
    recovered_write_offset: u64,
) -> Result<u64> {
    let needs_truncate = file_len != recovered_write_offset;
    let promotes_recovered_bytes = recovered_write_offset > durable_offset;
    if !needs_truncate && !promotes_recovered_bytes {
        return Ok(durable_offset);
    }

    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if needs_truncate {
        file.set_len(recovered_write_offset)
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }
    file.sync_data().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;

    if promotes_recovered_bytes {
        Ok(recovered_write_offset)
    } else {
        Ok(durable_offset)
    }
}

fn recover_missing_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    expected_write_offset: u64,
    durable_offset: u64,
) -> Result<SegmentRecovery> {
    if durable_offset > 0 {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    if config.recovery_policy == StrataRecoveryPolicy::AbsoluteConsistency {
        return Err(Error::RecoveryInconsistent {
            segment_id,
            expected_write_offset,
            recovered_write_offset: 0,
        });
    }
    discard_unsealed_segment(config, index, segment_id)?;
    Ok(SegmentRecovery { is_complete: false })
}

fn apply_recovered_segment_prefix(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
    existing_state: Option<SegmentState>,
    durable_offset: u64,
    recovered_write_offset: u64,
    records: &[strata_segment::ScannedRecord],
) -> Result<()> {
    let mut state =
        active_segment_state_from_path(config, segment_id, recovered_write_offset, durable_offset);
    if let Some(existing) = existing_state {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.state = existing.state;
        state.sealed_len = existing.sealed_len;
    }
    state.min_lsn = None;
    state.max_lsn = None;

    let mut batch = index.batch();
    let mut recovered_stats = SegmentStats {
        total_bytes: recovered_write_offset,
        ..Default::default()
    };

    for record in records {
        let record_end = record
            .record_ref
            .offset
            .checked_add(record.record_len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        if record_end > recovered_write_offset {
            continue;
        }
        let version_key = BlobVersionKey {
            key: record.key.clone(),
            lsn: record.header.generation,
        };
        let Some(entry) = index.get_blob_version(&version_key)? else {
            continue;
        };
        if entry.record_ref != Some(record.record_ref) {
            continue;
        }

        state.min_lsn = Some(
            state
                .min_lsn
                .map_or(entry.lsn, |first| first.min(entry.lsn)),
        );
        state.max_lsn = Some(state.max_lsn.map_or(entry.lsn, |last| last.max(entry.lsn)));
        if let Some((_, latest_entry)) = index.latest_blob_version(&record.key)?
            && latest_entry.state == BlobState::Live
            && latest_entry.record_ref == Some(record.record_ref)
        {
            recovered_stats.live_bytes =
                recovered_stats.live_bytes.saturating_add(record.record_len);
            recovered_stats.live_ref_count = recovered_stats.live_ref_count.saturating_add(1);
            add_live_lifecycle_stats(
                &mut recovered_stats,
                state.placement_class,
                record.record_len,
                latest_entry.lifecycle,
            );
        }
    }

    batch
        .insert_batch(index.segment_states(), [(&state.segment_id, &state)])
        .map_err(strata_index::Error::from)?;
    batch
        .insert_batch(index.segment_stats(), [(&segment_id, &recovered_stats)])
        .map_err(strata_index::Error::from)?;

    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    Ok(())
}

fn discard_unsealed_segment(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    segment_id: SegmentId,
) -> Result<()> {
    let mut state = active_segment_state_from_path(config, segment_id, 0, 0);
    if let Some(existing) = index.get_segment_state(segment_id)? {
        state.volume_id = existing.volume_id;
        state.placement_class = existing.placement_class;
        state.path = existing.path;
    }
    state.state = SegmentFileState::Deleted;
    state.write_offset = 0;
    state.durable_offset = 0;
    state.sealed_len = None;
    state.sealed_sha256 = None;

    let mut batch = index.batch();
    batch
        .insert_batch(index.segment_states(), [(&state.segment_id, &state)])
        .map_err(strata_index::Error::from)?;
    batch
        .insert_batch(
            index.segment_stats(),
            [(&segment_id, &SegmentStats::default())],
        )
        .map_err(strata_index::Error::from)?;
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;

    let path = segment_path(config, segment_id);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: path.clone(),
            source,
        }),
    }
}

fn rollback_lost_operations(index: &StrataIndex) -> Result<()> {
    let store_state = index.get_store_state()?.unwrap_or_default();
    let states = index.iter_segment_states()?;
    let mut rollback_from = None;
    for (lsn, key) in index.iter_pending_lsn_ops()? {
        if lsn <= store_state.durable_lsn || pending_operation_survived(index, lsn, &key, &states)?
        {
            continue;
        }
        rollback_from = Some(rollback_from.map_or(lsn, |current: StrataLsn| current.min(lsn)));
    }

    let Some(rollback_from) = rollback_from else {
        return Ok(());
    };

    let mut entries = index
        .iter_pending_lsn_ops()?
        .into_iter()
        .filter(|(lsn, _)| *lsn >= rollback_from)
        .collect::<Vec<_>>();
    entries.sort_by_key(|(lsn, _)| std::cmp::Reverse(*lsn));

    let mut batch = index.batch();
    let mut hidden_versions = Vec::new();
    for (lsn, key) in entries {
        hidden_versions.push((key, lsn));
        batch
            .delete_batch(index.pending_lsn_ops(), [&lsn])
            .map_err(strata_index::Error::from)?;
    }
    index.remove_blob_versions_batch(&mut batch, &hidden_versions)?;

    let mut store_state = store_state;
    store_state.next_lsn = rollback_from;
    batch
        .insert_batch(index.store_state(), [((), &store_state)])
        .map_err(strata_index::Error::from)?;
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    Ok(())
}

fn advance_recovered_durable_lsn(index: &StrataIndex) -> Result<()> {
    let mut batch = index.batch();
    let store_state = store_state_with_advanced_durable_lsn(index, None, &mut batch)?;
    batch
        .insert_batch(index.store_state(), [((), &store_state)])
        .map_err(strata_index::Error::from)?;
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    Ok(())
}

fn pending_operation_survived(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let Some(entry) = index.get_blob_version(&BlobVersionKey {
        key: key.clone(),
        lsn,
    })?
    else {
        return Ok(false);
    };
    let Some(record_ref) = entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    Ok(states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            !matches!(
                state.state,
                SegmentFileState::SealFailed
                    | SegmentFileState::Deleting
                    | SegmentFileState::Deleted
            ) && state.write_offset >= record_end_offset
        }))
}

fn enqueue_unsealed_segments_for_sealing(
    index: &StrataIndex,
    active_segment_id: SegmentId,
    seal_tx: &mpsc::Sender<SealCommand>,
) -> Result<()> {
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if segment_id < active_segment_id
            && let Some(state) = index.get_segment_state(segment_id)?
        {
            seal_tx
                .send(SealCommand::Seal(SegmentSealTask {
                    segment_id,
                    sealed_len: state.write_offset,
                }))
                .map_err(|_| Error::SealQueueClosed)?;
        }
    }
    Ok(())
}

fn publish_active_segment_state(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> Result<()> {
    let existing = index.get_segment_state(active_writer.segment_id())?;
    let state = active_segment_state_with_lsn(
        config,
        active_writer,
        durable_offset,
        existing.as_ref(),
        None,
    );
    index.put_segment_state(&state)?;
    if index.get_segment_stats(state.segment_id)?.is_none() {
        index.put_segment_stats(state.segment_id, &SegmentStats::default())?;
    }
    Ok(())
}

fn active_segment_state(
    config: &StrataStoreConfig,
    active_writer: &SegmentWriter,
    durable_offset: u64,
) -> SegmentState {
    active_segment_state_with_lsn(config, active_writer, durable_offset, None, None)
}

fn active_segment_state_with_lsn(
    config: &StrataStoreConfig,
    active_writer: &SegmentWriter,
    durable_offset: u64,
    existing: Option<&SegmentState>,
    appended_lsn: Option<StrataLsn>,
) -> SegmentState {
    let mut state = active_segment_state_from_path(
        config,
        active_writer.segment_id(),
        active_writer.write_offset(),
        durable_offset,
    );
    if let Some(existing) = existing {
        state.min_lsn = existing.min_lsn;
        state.max_lsn = existing.max_lsn;
    }
    if let Some(lsn) = appended_lsn {
        state.min_lsn = Some(state.min_lsn.map_or(lsn, |first| first.min(lsn)));
        state.max_lsn = Some(state.max_lsn.map_or(lsn, |last| last.max(lsn)));
    }
    state
}

fn active_segment_state_from_path(
    config: &StrataStoreConfig,
    segment_id: SegmentId,
    write_offset: u64,
    durable_offset: u64,
) -> SegmentState {
    let path = segment_path(config, segment_id);
    SegmentState {
        segment_id,
        volume_id: 0,
        path: path
            .as_path()
            .strip_prefix(config.namespace_dir())
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .into_owned(),
        placement_class: PlacementClass::Ingest,
        state: SegmentFileState::Open,
        write_offset,
        durable_offset,
        min_lsn: None,
        max_lsn: None,
        sealed_len: None,
        sealed_sha256: None,
    }
}

fn sha256_file_prefix(path: &Path, len: u64) -> Result<[u8; 32]> {
    let mut file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut remaining = len;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    while remaining > 0 {
        let to_read = remaining.min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..to_read])
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "segment ended before sealed length",
                ),
            });
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }

    Ok(hasher.finalize().into())
}

fn active_segment_durable_offset(index: &StrataIndex, segment_id: SegmentId) -> Result<u64> {
    Ok(index
        .get_segment_state(segment_id)?
        .map_or(0, |state| state.durable_offset))
}

fn validate_config(config: &StrataStoreConfig) -> Result<()> {
    if config.namespace.trim().is_empty() {
        return Err(Error::InvalidConfig("namespace cannot be empty"));
    }
    if config.namespace.contains('/') {
        return Err(Error::InvalidConfig("namespace cannot contain '/'"));
    }
    if config.segment_max_bytes == 0 {
        return Err(Error::InvalidConfig("segment_max_bytes must be non-zero"));
    }
    if config.write_queue_capacity == 0 {
        return Err(Error::InvalidConfig(
            "write_queue_capacity must be non-zero",
        ));
    }
    if config.max_unsealed_segments < 2 {
        return Err(Error::InvalidConfig(
            "max_unsealed_segments must be at least 2",
        ));
    }
    Ok(())
}

fn choose_active_segment_id(index: &StrataIndex) -> Result<SegmentId> {
    let states = index.iter_segment_states()?;
    if let Some(segment_id) = states
        .iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && state.state == SegmentFileState::Open
        })
        .map(|(segment_id, _)| *segment_id)
        .max()
    {
        return Ok(segment_id);
    }

    Ok(states
        .iter()
        .map(|(segment_id, _)| *segment_id)
        .max()
        .and_then(|segment_id| segment_id.checked_add(1))
        .unwrap_or(FIRST_SEGMENT_ID))
}

fn unsealed_ingest_segment_ids(index: &StrataIndex) -> Result<Vec<SegmentId>> {
    let mut segment_ids = index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest && is_unsealed_state(state.state)
        })
        .map(|(segment_id, _)| segment_id)
        .collect::<Vec<_>>();
    segment_ids.sort_unstable();
    Ok(segment_ids)
}

fn unsealed_ingest_segment_count(index: &StrataIndex) -> Result<usize> {
    Ok(unsealed_ingest_segment_ids(index)?.len())
}

fn store_state_with_advanced_durable_lsn(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
    batch: &mut typed_store::rocks::DBBatch,
) -> Result<StrataStoreState> {
    let mut store_state = index.get_store_state()?.unwrap_or_default();
    store_state.durable_lsn =
        compute_durable_lsn(index, store_state.durable_lsn, override_state, batch)?;
    Ok(store_state)
}

fn compute_durable_lsn(
    index: &StrataIndex,
    current_durable_lsn: StrataLsn,
    override_state: Option<&SegmentState>,
    batch: &mut typed_store::rocks::DBBatch,
) -> Result<StrataLsn> {
    let states = segment_states_with_override(index, override_state)?;
    let mut durable_lsn = current_durable_lsn;

    loop {
        let Some(next_lsn) = durable_lsn.checked_add(1) else {
            break;
        };
        let Some(key) = index
            .pending_lsn_ops()
            .get(&next_lsn)
            .map_err(strata_index::Error::from)?
        else {
            break;
        };
        if !pending_lsn_is_durable(index, next_lsn, &key, &states)? {
            break;
        }
        batch
            .delete_batch(index.pending_lsn_ops(), [&next_lsn])
            .map_err(strata_index::Error::from)?;
        durable_lsn = next_lsn;
    }

    Ok(durable_lsn)
}

fn segment_states_with_override(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
) -> Result<Vec<(SegmentId, SegmentState)>> {
    let mut states = index.iter_segment_states()?;
    if let Some(override_state) = override_state {
        let mut replaced = false;
        for (_, state) in &mut states {
            if state.segment_id == override_state.segment_id {
                *state = override_state.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            states.push((override_state.segment_id, override_state.clone()));
        }
    }
    Ok(states)
}

fn pending_lsn_is_durable(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let Some(entry) = index.get_blob_version(&BlobVersionKey {
        key: key.clone(),
        lsn,
    })?
    else {
        return Ok(false);
    };
    let Some(record_ref) = entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    Ok(states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            !matches!(
                state.state,
                SegmentFileState::SealFailed
                    | SegmentFileState::Deleting
                    | SegmentFileState::Deleted
            ) && state.durable_offset >= record_end_offset
        }))
}

fn first_seal_failed_segment(index: &StrataIndex) -> Result<Option<SegmentId>> {
    Ok(index
        .iter_segment_states()?
        .into_iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest
                && state.state == SegmentFileState::SealFailed
        })
        .map(|(segment_id, _)| segment_id)
        .min())
}

fn is_unsealed_state(state: SegmentFileState) -> bool {
    matches!(
        state,
        SegmentFileState::Open | SegmentFileState::Sealing | SegmentFileState::SealFailed
    )
}

fn segment_state_is_readable(state: SegmentFileState) -> bool {
    !matches!(
        state,
        SegmentFileState::Deleting | SegmentFileState::Deleted
    )
}

fn segment_path(config: &StrataStoreConfig, segment_id: SegmentId) -> PathBuf {
    config.ingest_dir().join(segment_file_name(segment_id))
}

fn segment_file_name(segment_id: SegmentId) -> String {
    format!("{segment_id:012}.data")
}

fn parse_segment_file_name(file_name: &std::ffi::OsStr) -> Option<SegmentId> {
    let stem = file_name.to_str()?.strip_suffix(".data")?;
    if stem.len() != 12 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

fn add_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    lifecycle: BlobLifecycle,
) {
    increment_histogram(
        &mut stats.future_epoch_histogram,
        lifecycle.logical_end_epoch,
    );
    increment_histogram(
        &mut stats.extension_count_histogram,
        lifecycle.extension_count,
    );
    if lifecycle_is_pinned(placement_class, lifecycle) {
        stats.pinned_bytes = stats.pinned_bytes.saturating_add(record_len);
    }
    refresh_live_epoch_bounds(stats);
}

fn update_live_lifecycle_stats(
    stats: &mut SegmentStats,
    placement_class: PlacementClass,
    record_len: u64,
    old_lifecycle: BlobLifecycle,
    new_lifecycle: BlobLifecycle,
) {
    if old_lifecycle.logical_end_epoch != new_lifecycle.logical_end_epoch {
        decrement_histogram(
            &mut stats.future_epoch_histogram,
            old_lifecycle.logical_end_epoch,
        );
        increment_histogram(
            &mut stats.future_epoch_histogram,
            new_lifecycle.logical_end_epoch,
        );
        refresh_live_epoch_bounds(stats);
    }

    if old_lifecycle.extension_count != new_lifecycle.extension_count {
        decrement_histogram(
            &mut stats.extension_count_histogram,
            old_lifecycle.extension_count,
        );
        increment_histogram(
            &mut stats.extension_count_histogram,
            new_lifecycle.extension_count,
        );
    }

    match (
        lifecycle_is_pinned(placement_class, old_lifecycle),
        lifecycle_is_pinned(placement_class, new_lifecycle),
    ) {
        (false, true) => {
            stats.pinned_bytes = stats.pinned_bytes.saturating_add(record_len);
        }
        (true, false) => {
            stats.pinned_bytes = stats.pinned_bytes.saturating_sub(record_len);
        }
        _ => {}
    }
}

fn lifecycle_is_pinned(placement_class: PlacementClass, lifecycle: BlobLifecycle) -> bool {
    match placement_class {
        PlacementClass::ExactEpoch(physical_epoch) => lifecycle.logical_end_epoch > physical_epoch,
        PlacementClass::Ingest | PlacementClass::Spillover => false,
    }
}

fn increment_histogram<K>(histogram: &mut std::collections::BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    *histogram.entry(key).or_default() += 1;
}

fn decrement_histogram<K>(histogram: &mut std::collections::BTreeMap<K, u64>, key: K)
where
    K: Ord,
{
    match histogram.get_mut(&key) {
        Some(count) if *count > 1 => *count -= 1,
        Some(_) => {
            histogram.remove(&key);
        }
        None => {}
    }
}

fn refresh_live_epoch_bounds(stats: &mut SegmentStats) {
    stats.min_live_end_epoch = stats.future_epoch_histogram.keys().next().copied();
    stats.max_live_end_epoch = stats.future_epoch_histogram.keys().next_back().copied();
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        path::Path,
        sync::Once,
        time::{Duration, Instant},
    };

    use strata_core::{BlobLifecycle, FIXED_RECORD_HEADER_LEN};
    use tempfile::tempdir;
    use typed_store::{
        DBMetrics,
        rocks::{MetricConf, open_cf},
    };

    use super::*;

    static INIT_TYPED_STORE_METRICS: Once = Once::new();
    const TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD: u64 = 135;

    fn init_typed_store_metrics() {
        INIT_TYPED_STORE_METRICS.call_once(|| {
            DBMetrics::get();
        });
    }

    fn config(root_dir: &Path, namespace: &str) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: root_dir.to_path_buf(),
            namespace: namespace.to_owned(),
            segment_max_bytes: 1 << 20,
            write_queue_capacity: 128,
            max_unsealed_segments: 8,
            segment_reader_cache_capacity: 16,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
        }
    }

    fn wait_for_segment_state(
        index: &StrataIndex,
        segment_id: SegmentId,
        expected_state: SegmentFileState,
    ) -> SegmentState {
        let started = Instant::now();
        loop {
            let state = index.get_segment_state(segment_id).unwrap().unwrap();
            if state.state == expected_state {
                return state;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "timed out waiting for segment {segment_id} to become {expected_state:?}; current state was {:?}",
                state.state
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn put_test_segment_state(index: &StrataIndex, segment_id: SegmentId, state: SegmentFileState) {
        index
            .put_segment_state(&SegmentState {
                segment_id,
                volume_id: 0,
                path: format!("ingest/{segment_id:012}.data"),
                placement_class: PlacementClass::Ingest,
                state,
                write_offset: 64,
                durable_offset: 0,
                min_lsn: Some(segment_id),
                max_lsn: Some(segment_id),
                sealed_len: None,
                sealed_sha256: None,
            })
            .unwrap();
    }

    fn version_key(key: &BlobKey, lsn: StrataLsn) -> BlobVersionKey {
        BlobVersionKey {
            key: key.clone(),
            lsn,
        }
    }

    fn seal_first_segment(config: &StrataStoreConfig) -> SegmentState {
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config.clone()).unwrap();

        store
            .put(&key_1, BlobLifecycle::new(42), b"payload-a")
            .unwrap();
        store
            .put(&key_2, BlobLifecycle::new(43), b"payload-b")
            .unwrap();

        wait_for_segment_state(store.index(), FIRST_SEGMENT_ID, SegmentFileState::Sealed)
    }

    #[tokio::test]
    async fn standalone_put_get_round_trip() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key, BlobLifecycle::new(42), b"hello strata")
            .unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
        assert!(dir.path().join("default").join("ingest").exists());
        assert!(dir.path().join("default").join("index").exists());
    }

    #[tokio::test]
    async fn get_with_options_can_skip_checksum_verification() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key, BlobLifecycle::new(42), b"hello strata")
            .unwrap();
        store.sync().unwrap();

        let record_ref = store
            .index()
            .get_blob_entry(&key)
            .unwrap()
            .unwrap()
            .record_ref
            .unwrap();
        let mut segment = OpenOptions::new()
            .write(true)
            .open(segment_path(store.config(), record_ref.segment_id))
            .unwrap();
        segment
            .seek(SeekFrom::Start(
                record_ref.offset + FIXED_RECORD_HEADER_LEN as u64,
            ))
            .unwrap();
        segment.write_all(b"H").unwrap();

        let err = store.get(&key).unwrap_err();
        assert!(matches!(
            err,
            Error::Segment(strata_segment::Error::Core(
                strata_core::Error::RecordChecksumMismatch { .. }
            ))
        ));

        assert_eq!(
            store
                .get_with_options(&key, ReadOptions::skip_checksum_verification())
                .unwrap(),
            Some(b"Hello strata".to_vec())
        );
    }

    #[tokio::test]
    async fn get_sliver_range_reads_payload_slice() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key, BlobLifecycle::new(42), b"hello strata")
            .unwrap();

        assert_eq!(
            store.get_sliver_range(&key, 6..12).unwrap(),
            Some(b"strata".to_vec())
        );
    }

    #[tokio::test]
    async fn cached_reader_is_evictable_when_segment_is_deleted() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key, BlobLifecycle::new(42), b"hello strata")
            .unwrap();
        store.sync().unwrap();

        assert_eq!(store.reader_cache_len(), 0);
        assert_eq!(store.get(&key).unwrap(), Some(b"hello strata".to_vec()));
        assert_eq!(store.reader_cache_len(), 1);

        let mut state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        state.state = SegmentFileState::Deleted;
        let mut batch = store.index().batch();
        batch
            .insert_batch(
                store.index().segment_states(),
                [(&state.segment_id, &state)],
            )
            .unwrap();
        batch.write().unwrap();
        store.index().flush_wal(true).unwrap();
        store.evict_segment_reader(FIRST_SEGMENT_ID);

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
        assert_eq!(store.reader_cache_len(), 0);
    }

    #[tokio::test]
    async fn stream_sliver_reads_payload_slice() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key, BlobLifecycle::new(42), b"hello strata")
            .unwrap();

        let mut stream = store.stream_sliver(&key, 0..5).unwrap().unwrap();
        let mut read = Vec::new();
        stream.read_to_end(&mut read).unwrap();

        assert_eq!(read, b"hello");
        assert_eq!(stream.remaining(), 0);
    }

    #[tokio::test]
    async fn read_range_missing_and_tombstone_return_none() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let missing = BlobKey::new(b"missing".to_vec()).unwrap();
        let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        assert_eq!(store.get_sliver_range(&missing, 0..1).unwrap(), None);
        assert!(store.stream_sliver(&missing, 0..1).unwrap().is_none());

        store
            .put(&tombstoned, BlobLifecycle::new(42), b"payload")
            .unwrap();
        store.tombstone(&tombstoned).unwrap();

        assert_eq!(store.get_sliver_range(&tombstoned, 0..1).unwrap(), None);
        assert!(store.stream_sliver(&tombstoned, 0..1).unwrap().is_none());
    }

    #[tokio::test]
    async fn read_range_rejects_out_of_bounds_range() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();

        let err = store.get_sliver_range(&key, 0..8).unwrap_err();

        assert!(matches!(
            err,
            Error::Segment(strata_segment::Error::InvalidPayloadRange { .. })
        ));
    }

    #[tokio::test]
    async fn range_read_rejects_index_record_key_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key_a, BlobLifecycle::new(42), b"payload-a")
            .unwrap();
        store
            .put(&key_b, BlobLifecycle::new(42), b"payload-b")
            .unwrap();

        let mut entry_b = store.index().get_blob_entry(&key_b).unwrap().unwrap();
        entry_b.lsn += 1;
        store.index().put_blob_entry(&key_a, &entry_b).unwrap();

        let err = store.get_sliver_range(&key_a, 0..1).unwrap_err();

        assert!(matches!(
            err,
            Error::KeyMismatch {
                requested,
                found
            } if requested == key_a && found == key_b
        ));
    }

    #[tokio::test]
    async fn from_index_does_not_create_local_index_dir() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let db_dir = tempdir().unwrap();
        let db = open_cf(
            db_dir.path(),
            None,
            MetricConf::new("strata_store_test"),
            &["existing"],
        )
        .unwrap();
        let index = StrataIndex::from_db(db, "strata/shard-99").unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::from_index(config(dir.path(), "shard-99"), index).unwrap();

        store.put(&key, BlobLifecycle::new(7), b"payload").unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        assert!(dir.path().join("shard-99").join("ingest").exists());
        assert!(!dir.path().join("shard-99").join("index").exists());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"missing".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
    }

    #[tokio::test]
    async fn point_in_time_recovery_removes_orphan_segment_file_before_opening_active_writer() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
        fs::create_dir_all(cfg.ingest_dir()).unwrap();
        fs::write(&orphan_path, b"stale bytes").unwrap();

        let store = StrataStore::open_standalone(cfg).unwrap();
        assert_eq!(fs::metadata(&orphan_path).unwrap().len(), 0);

        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();

        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(entry.record_ref.unwrap().offset, 0);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    }

    #[tokio::test]
    async fn absolute_consistency_rejects_orphan_segment_file() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
        let orphan_path = segment_path(&cfg, FIRST_SEGMENT_ID);
        fs::create_dir_all(cfg.ingest_dir()).unwrap();
        fs::write(&orphan_path, b"stale bytes").unwrap();

        let err = StrataStore::open_standalone(cfg).unwrap_err();
        assert!(matches!(
            err,
            Error::OrphanSegmentFile {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn tombstone_hides_payload() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();
        let put_lsn = store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();

        let tombstone_lsn = store.tombstone(&key).unwrap();

        assert_eq!(put_lsn, 1);
        assert_eq!(tombstone_lsn, 2);
        assert_eq!(store.get(&key).unwrap(), None);
        assert!(!store.contains(&key).unwrap());
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key)
                .unwrap()
                .unwrap()
                .record_ref,
            None
        );
        let put_entry = store
            .index()
            .get_blob_version(&version_key(&key, put_lsn))
            .unwrap()
            .unwrap();
        assert_eq!(put_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
        assert_eq!(store.durable_lsn().unwrap(), 0);

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn extend_updates_lifecycle_without_moving_payload() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        let put_lsn = store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
        let before = store.index().get_blob_entry(&key).unwrap().unwrap();

        let extend_lsn = store.extend(&key, 50).unwrap().unwrap();

        let after = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(put_lsn, 1);
        assert_eq!(extend_lsn, 2);
        assert_eq!(after.record_ref, before.record_ref);
        assert_eq!(after.generation, before.generation);
        assert_eq!(after.lifecycle.logical_end_epoch, 50);
        assert_eq!(after.lifecycle.extension_count, 1);
        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));

        let stats = store
            .index()
            .get_segment_stats(before.record_ref.unwrap().segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(stats.future_epoch_histogram.get(&42), None);
        assert_eq!(stats.future_epoch_histogram.get(&50), Some(&1));
        assert_eq!(stats.extension_count_histogram.get(&0), None);
        assert_eq!(stats.extension_count_histogram.get(&1), Some(&1));
        assert_eq!(stats.min_live_end_epoch, Some(50));
        assert_eq!(stats.max_live_end_epoch, Some(50));
        assert_eq!(stats.pinned_bytes, 0);

        store.sync().unwrap();

        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn extend_marks_exact_epoch_segment_bytes_pinned() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        let record_ref = entry.record_ref.unwrap();
        let mut state = store
            .index()
            .get_segment_state(record_ref.segment_id)
            .unwrap()
            .unwrap();
        state.placement_class = PlacementClass::ExactEpoch(42);
        store.index().put_segment_state(&state).unwrap();

        store.extend(&key, 50).unwrap().unwrap();

        let stats = store
            .index()
            .get_segment_stats(record_ref.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(stats.pinned_bytes, record_ref.len);
        assert_eq!(stats.future_epoch_histogram.get(&50), Some(&1));
    }

    #[tokio::test]
    async fn extend_missing_or_tombstoned_sliver_is_noop() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let missing = BlobKey::new(b"missing".to_vec()).unwrap();
        let tombstoned = BlobKey::new(b"tombstoned".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        assert_eq!(store.extend(&missing, 50).unwrap(), None);
        assert_eq!(
            store
                .index()
                .get_store_state()
                .unwrap()
                .unwrap_or_default()
                .next_lsn,
            1
        );

        store
            .put(&tombstoned, BlobLifecycle::new(42), b"payload")
            .unwrap();
        store.tombstone(&tombstoned).unwrap();

        assert_eq!(store.extend(&tombstoned, 50).unwrap(), None);
        assert_eq!(
            store.index().get_store_state().unwrap().unwrap().next_lsn,
            3
        );
    }

    #[tokio::test]
    async fn reopen_reads_existing_data() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();
            store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
            store.sync().unwrap();
        }

        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
    }

    #[tokio::test]
    async fn sync_advances_durable_offset_after_segment_fsync() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();

        let unsynced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(unsynced.durable_offset, 0);
        assert!(unsynced.write_offset > 0);
        assert_eq!(store.durable_lsn().unwrap(), 0);

        store.sync().unwrap();

        let synced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(synced.durable_offset, unsynced.write_offset);
        assert_eq!(synced.write_offset, unsynced.write_offset);
        assert_eq!(store.durable_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn put_after_sync_preserves_durable_offset() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = StrataStore::open_standalone(config(dir.path(), "default")).unwrap();

        store
            .put(&key_1, BlobLifecycle::new(42), b"payload-a")
            .unwrap();
        store.sync().unwrap();
        let synced = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();

        store
            .put(&key_2, BlobLifecycle::new(43), b"payload-b")
            .unwrap();

        let after_put = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert_eq!(after_put.durable_offset, synced.durable_offset);
        assert!(after_put.write_offset > synced.write_offset);
    }

    #[tokio::test]
    async fn recovery_keeps_complete_unsynced_record_that_survived() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        let state = store
            .index()
            .get_segment_state(FIRST_SEGMENT_ID)
            .unwrap()
            .unwrap();
        assert!(state.write_offset > 0);
        assert_eq!(state.durable_offset, state.write_offset);
        assert_eq!(store.durable_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_removes_live_index_entry_when_segment_bytes_are_missing() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(0)
            .unwrap();

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
    }

    #[tokio::test]
    async fn recovery_ignores_segment_record_when_index_version_is_missing() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            let segment_path = segment_path(&cfg, FIRST_SEGMENT_ID);
            let mut segment = SegmentWriter::open_existing(
                &segment_path,
                FIRST_SEGMENT_ID,
                PlacementClass::Ingest,
                1 << 20,
            )
            .unwrap();
            segment
                .append(&key, BlobLifecycle::new(42), 1, b"payload")
                .unwrap();
            let write_offset = segment.write_offset();
            drop(segment);

            let state = active_segment_state_from_path(&cfg, FIRST_SEGMENT_ID, write_offset, 0);
            let mut batch = store.index().batch();
            batch
                .insert_batch(
                    store.index().segment_states(),
                    [(&state.segment_id, &state)],
                )
                .unwrap();
            batch
                .insert_batch(
                    store.index().store_state(),
                    [((), &StrataStoreState::default())],
                )
                .unwrap();
            batch.write().unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
        assert_eq!(
            store.index().get_store_state().unwrap().unwrap().next_lsn,
            1
        );
    }

    #[tokio::test]
    async fn recovery_truncates_partial_tail() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let valid_len;
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
            store.index().flush_wal(true).unwrap();
            valid_len = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .write_offset;
        }

        let path = segment_path(&cfg, FIRST_SEGMENT_ID);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);
        assert!(std::fs::metadata(&path).unwrap().len() > valid_len);

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload".to_vec()));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[tokio::test]
    async fn recovery_removes_tombstone_when_rolled_back_lsn_range_is_lost() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store.put(&key, BlobLifecycle::new(42), b"payload").unwrap();
            store.tombstone(&key).unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(0)
            .unwrap();

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.index().get_blob_entry(&key).unwrap(), None);
        assert_eq!(store.get(&key).unwrap(), None);
        assert_eq!(store.durable_lsn().unwrap(), 0);
    }

    #[tokio::test]
    async fn recovery_rolls_back_lost_overwrite_to_previous_entry() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let first_len;
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store
                .put(&key, BlobLifecycle::new(42), b"payload-a")
                .unwrap();
            store.sync().unwrap();
            first_len = store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .write_offset;
            store
                .put(&key, BlobLifecycle::new(43), b"payload-b")
                .unwrap();
            store.index().flush_wal(true).unwrap();
        }

        std::fs::OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(first_len)
            .unwrap();

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key).unwrap(), Some(b"payload-a".to_vec()));
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();
        assert_eq!(entry.lsn, 1);
        assert_eq!(entry.lifecycle.logical_end_epoch, 42);
        assert_eq!(store.durable_lsn().unwrap(), 1);
    }

    #[tokio::test]
    async fn put_overwrite_keeps_blob_versions_for_old_records() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let store = StrataStore::open_standalone(cfg).unwrap();

        let min_lsn = store
            .put(&key, BlobLifecycle::new(42), b"payload-a")
            .unwrap();
        let second_lsn = store
            .put(&key, BlobLifecycle::new(43), b"payload-b")
            .unwrap();
        let entry = store.index().get_blob_entry(&key).unwrap().unwrap();

        assert_ne!(min_lsn, second_lsn);
        assert_eq!(entry.lsn, second_lsn);
        let first_entry = store
            .index()
            .get_blob_version(&version_key(&key, min_lsn))
            .unwrap()
            .unwrap();
        let second_entry = store
            .index()
            .get_blob_version(&version_key(&key, second_lsn))
            .unwrap()
            .unwrap();
        assert_eq!(first_entry.record_ref.unwrap().segment_id, FIRST_SEGMENT_ID);
        assert_eq!(
            second_entry.record_ref.unwrap().segment_id,
            FIRST_SEGMENT_ID
        );
    }

    #[tokio::test]
    async fn put_assigns_monotonic_lsn_across_reopen() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            let lsn_1 = store
                .put(&key_1, BlobLifecycle::new(42), b"payload-a")
                .unwrap();
            let lsn_2 = store
                .put(&key_2, BlobLifecycle::new(43), b"payload-b")
                .unwrap();

            assert_eq!(lsn_1, 1);
            assert_eq!(lsn_2, 2);
            assert_eq!(
                store
                    .index()
                    .get_blob_entry(&key_1)
                    .unwrap()
                    .unwrap()
                    .record_ref
                    .unwrap()
                    .segment_id,
                1
            );
            assert_eq!(
                store
                    .index()
                    .get_blob_entry(&key_2)
                    .unwrap()
                    .unwrap()
                    .record_ref
                    .unwrap()
                    .segment_id,
                2
            );
            assert_eq!(
                store.index().get_blob_entry(&key_2).unwrap().unwrap().lsn,
                2
            );
            assert_eq!(
                store.index().get_store_state().unwrap().unwrap().next_lsn,
                3
            );
        }

        let store = StrataStore::open_standalone(cfg).unwrap();
        let lsn_3 = store.put(&key_3, BlobLifecycle::new(44), b"x").unwrap();

        assert_eq!(lsn_3, 3);
        assert_eq!(
            store.index().get_store_state().unwrap().unwrap().next_lsn,
            4
        );
        let active_state = store.index().get_segment_state(2).unwrap().unwrap();
        assert_eq!(active_state.min_lsn, Some(2));
        assert_eq!(active_state.max_lsn, Some(3));
    }

    #[tokio::test]
    async fn recovery_keeps_valid_blob_versions_for_old_versions() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let first;
        let second;
        let first_lsn;
        let second_lsn;
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            first_lsn = store
                .put(&key, BlobLifecycle::new(42), b"payload-a")
                .unwrap();
            first = store
                .index()
                .get_blob_entry(&key)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap();
            second_lsn = store
                .put(&key, BlobLifecycle::new(43), b"payload-b")
                .unwrap();
            second = store
                .index()
                .get_blob_entry(&key)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap();
            store.index().flush_wal(true).unwrap();
        }

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_version(&version_key(&key, first_lsn))
                .unwrap()
                .unwrap()
                .record_ref,
            Some(first)
        );
        assert_eq!(
            store
                .index()
                .get_blob_version(&version_key(&key, second_lsn))
                .unwrap()
                .unwrap()
                .record_ref,
            Some(second)
        );
        assert_eq!(store.get(&key).unwrap(), Some(b"payload-b".to_vec()));
    }

    #[tokio::test]
    async fn point_in_time_recovery_discards_higher_segments_after_lower_gap() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_c = BlobKey::new(b"blob-c".to_vec()).unwrap();
        let first_end;
        let second_end;
        {
            let index =
                StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
            fs::create_dir_all(cfg.ingest_dir()).unwrap();

            let segment_1_path = segment_path(&cfg, 1);
            let mut segment_1 =
                SegmentWriter::create(&segment_1_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_a = segment_1
                .append(&key_a, BlobLifecycle::new(42), 1, b"payload-a")
                .unwrap();
            first_end = segment_1.write_offset();
            let out_b = segment_1
                .append(&key_b, BlobLifecycle::new(43), 2, b"payload-b")
                .unwrap();
            second_end = segment_1.write_offset();
            drop(segment_1);
            OpenOptions::new()
                .write(true)
                .open(&segment_1_path)
                .unwrap()
                .set_len(first_end)
                .unwrap();

            let segment_2_path = segment_path(&cfg, 2);
            let mut segment_2 =
                SegmentWriter::create(&segment_2_path, 2, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_c = segment_2
                .append(&key_c, BlobLifecycle::new(44), 3, b"payload-c")
                .unwrap();
            let segment_2_end = segment_2.write_offset();
            drop(segment_2);

            let mut segment_1_state = active_segment_state_from_path(&cfg, 1, second_end, 0);
            segment_1_state.state = SegmentFileState::Sealing;
            let segment_2_state = active_segment_state_from_path(&cfg, 2, segment_2_end, 0);

            let mut batch = index.batch();
            for (key, record_ref, lsn, lifecycle) in [
                (&key_a, out_a.record_ref, 1, BlobLifecycle::new(42)),
                (&key_b, out_b.record_ref, 2, BlobLifecycle::new(43)),
                (&key_c, out_c.record_ref, 3, BlobLifecycle::new(44)),
            ] {
                index
                    .put_blob_version_batch(
                        &mut batch,
                        key,
                        &BlobEntry {
                            record_ref: Some(record_ref),
                            lsn,
                            generation: lsn,
                            state: BlobState::Live,
                            lifecycle,
                        },
                    )
                    .unwrap();
                batch
                    .insert_batch(index.pending_lsn_ops(), [(&lsn, key)])
                    .unwrap();
            }
            batch
                .insert_batch(
                    index.segment_states(),
                    [
                        (&segment_1_state.segment_id, &segment_1_state),
                        (&segment_2_state.segment_id, &segment_2_state),
                    ],
                )
                .unwrap();
            batch.write().unwrap();
            index.flush_wal(true).unwrap();
        }

        let store = StrataStore::open_standalone(cfg).unwrap();

        assert_eq!(store.get(&key_a).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_b).unwrap(), None);
        assert_eq!(store.get(&key_c).unwrap(), None);
        assert_eq!(
            store
                .index()
                .get_segment_state(1)
                .unwrap()
                .unwrap()
                .write_offset,
            first_end
        );
        assert_eq!(
            store.index().get_segment_state(2).unwrap().unwrap().state,
            SegmentFileState::Deleted
        );
        assert_eq!(
            store.index().get_blob_entry(&key_a).unwrap().unwrap().state,
            BlobState::Live
        );
        assert_eq!(store.index().get_blob_entry(&key_b).unwrap(), None);
        assert_eq!(store.index().get_blob_entry(&key_c).unwrap(), None);
    }

    #[tokio::test]
    async fn absolute_consistency_recovery_fails_on_unsealed_gap() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.recovery_policy = StrataRecoveryPolicy::AbsoluteConsistency;
        let key_a = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_b = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let first_end;
        let second_end;
        {
            let index =
                StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
            fs::create_dir_all(cfg.ingest_dir()).unwrap();
            let segment_path = segment_path(&cfg, 1);
            let mut segment =
                SegmentWriter::create(&segment_path, 1, PlacementClass::Ingest, 1 << 20).unwrap();
            let out_a = segment
                .append(&key_a, BlobLifecycle::new(42), 1, b"payload-a")
                .unwrap();
            first_end = segment.write_offset();
            let out_b = segment
                .append(&key_b, BlobLifecycle::new(43), 2, b"payload-b")
                .unwrap();
            second_end = segment.write_offset();
            drop(segment);
            OpenOptions::new()
                .write(true)
                .open(&segment_path)
                .unwrap()
                .set_len(first_end)
                .unwrap();

            let state = active_segment_state_from_path(&cfg, 1, second_end, 0);
            let mut batch = index.batch();
            for (key, record_ref, lsn, lifecycle) in [
                (&key_a, out_a.record_ref, 1, BlobLifecycle::new(42)),
                (&key_b, out_b.record_ref, 2, BlobLifecycle::new(43)),
            ] {
                index
                    .put_blob_version_batch(
                        &mut batch,
                        key,
                        &BlobEntry {
                            record_ref: Some(record_ref),
                            lsn,
                            generation: lsn,
                            state: BlobState::Live,
                            lifecycle,
                        },
                    )
                    .unwrap();
                batch
                    .insert_batch(index.pending_lsn_ops(), [(&lsn, key)])
                    .unwrap();
            }
            batch
                .insert_batch(index.segment_states(), [(&state.segment_id, &state)])
                .unwrap();
            batch.write().unwrap();
            index.flush_wal(true).unwrap();
        }

        let err = StrataStore::open_standalone(cfg).unwrap_err();

        assert!(matches!(
            err,
            Error::RecoveryInconsistent {
                segment_id: 1,
                expected_write_offset,
                recovered_write_offset,
            } if expected_write_offset == second_end && recovered_write_offset == first_end
        ));
    }

    #[tokio::test]
    async fn unsealed_segment_count_includes_open_sealing_and_failed_segments() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(dir.path().join("index"), "strata/default").unwrap();

        put_test_segment_state(&index, 1, SegmentFileState::Open);
        put_test_segment_state(&index, 2, SegmentFileState::Sealing);
        put_test_segment_state(&index, 3, SegmentFileState::SealFailed);
        put_test_segment_state(&index, 4, SegmentFileState::Sealed);

        assert_eq!(unsealed_ingest_segment_ids(&index).unwrap(), vec![1, 2, 3]);
        assert_eq!(unsealed_ingest_segment_count(&index).unwrap(), 3);
        assert_eq!(first_seal_failed_segment(&index).unwrap(), Some(3));
    }

    #[tokio::test]
    async fn reopen_detects_missing_sealed_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        seal_first_segment(&cfg);

        std::fs::remove_file(segment_path(&cfg, FIRST_SEGMENT_ID)).unwrap();

        let err = StrataStore::open_standalone(cfg).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentMissing {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn reopen_detects_sealed_segment_length_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let sealed = seal_first_segment(&cfg);
        let sealed_len = sealed.sealed_len.unwrap();
        assert!(sealed_len > 0);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .set_len(sealed_len - 1)
            .unwrap();

        let err = StrataStore::open_standalone(cfg).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentLengthMismatch {
                segment_id: FIRST_SEGMENT_ID,
                expected_len,
                actual_len,
                ..
            } if expected_len == sealed_len && actual_len == sealed_len - 1
        ));
    }

    #[tokio::test]
    async fn metadata_only_reopen_does_not_hash_sealed_segment_bytes() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::MetadataOnly;
        seal_first_segment(&cfg);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .write_all(b"X")
            .unwrap();

        let store = StrataStore::open_standalone(cfg).unwrap();
        assert_eq!(
            store
                .index()
                .get_segment_state(FIRST_SEGMENT_ID)
                .unwrap()
                .unwrap()
                .state,
            SegmentFileState::Sealed
        );
    }

    #[tokio::test]
    async fn checksum_reopen_detects_sealed_segment_hash_mismatch() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        cfg.sealed_segment_integrity_policy = SealedSegmentIntegrityPolicy::Checksum;
        seal_first_segment(&cfg);

        OpenOptions::new()
            .write(true)
            .open(segment_path(&cfg, FIRST_SEGMENT_ID))
            .unwrap()
            .write_all(b"X")
            .unwrap();

        let err = StrataStore::open_standalone(cfg).unwrap_err();
        assert!(matches!(
            err,
            Error::SealedSegmentChecksumMismatch {
                segment_id: FIRST_SEGMENT_ID,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn seal_worker_marks_segment_failed_on_seal_error() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let cfg = config(dir.path(), "default");
        let index =
            StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).unwrap();
        put_test_segment_state(&index, 1, SegmentFileState::Sealing);
        let (_seal_tx, seal_rx) = mpsc::channel();
        let worker = SealWorker {
            config: cfg,
            index: index.clone(),
            seal_rx,
        };

        assert!(
            worker
                .seal_segment(SegmentSealTask {
                    segment_id: 1,
                    sealed_len: 64,
                })
                .is_err()
        );
        worker.mark_seal_failed(1).unwrap();

        let state = index.get_segment_state(1).unwrap().unwrap();
        assert_eq!(state.state, SegmentFileState::SealFailed);
    }

    #[tokio::test]
    async fn rollover_switches_active_segment_and_seal_worker_seals_old_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let store = StrataStore::open_standalone(cfg).unwrap();

        store
            .put(&key_1, BlobLifecycle::new(42), b"payload-a")
            .unwrap();
        store
            .put(&key_2, BlobLifecycle::new(43), b"payload-b")
            .unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_1)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            1
        );
        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_2)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            2
        );
        assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));

        let sealed = wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
        assert_eq!(sealed.durable_offset, sealed.write_offset);
        assert_eq!(sealed.sealed_len, Some(sealed.write_offset));
        assert_eq!(
            sealed.sealed_sha256,
            Some(
                sha256_file_prefix(&segment_path(store.config(), 1), sealed.write_offset).unwrap()
            )
        );
        assert_eq!(store.durable_lsn().unwrap(), 1);

        let open = store.index().get_segment_state(2).unwrap().unwrap();
        assert_eq!(open.state, SegmentFileState::Open);
        assert_eq!(open.sealed_sha256, None);
        let open_segment_ids = store
            .index()
            .iter_segment_states()
            .unwrap()
            .into_iter()
            .filter(|(_, state)| {
                state.placement_class == PlacementClass::Ingest
                    && state.state == SegmentFileState::Open
            })
            .map(|(segment_id, _)| segment_id)
            .collect::<Vec<_>>();
        assert_eq!(open_segment_ids, vec![2]);

        store.sync().unwrap();
        assert_eq!(store.durable_lsn().unwrap(), 2);
    }

    #[tokio::test]
    async fn reopen_after_rollover_appends_to_highest_open_segment() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let mut cfg = config(dir.path(), "default");
        cfg.segment_max_bytes = TEST_SEGMENT_MAX_BYTES_ONE_FULL_RECORD;
        let key_1 = BlobKey::new(b"blob-a".to_vec()).unwrap();
        let key_2 = BlobKey::new(b"blob-b".to_vec()).unwrap();
        let key_3 = BlobKey::new(b"blob-c".to_vec()).unwrap();
        {
            let store = StrataStore::open_standalone(cfg.clone()).unwrap();
            store
                .put(&key_1, BlobLifecycle::new(42), b"payload-a")
                .unwrap();
            store
                .put(&key_2, BlobLifecycle::new(43), b"payload-b")
                .unwrap();
            wait_for_segment_state(store.index(), 1, SegmentFileState::Sealed);
        }

        let store = StrataStore::open_standalone(cfg).unwrap();
        store.put(&key_3, BlobLifecycle::new(44), b"x").unwrap();

        assert_eq!(
            store
                .index()
                .get_blob_entry(&key_3)
                .unwrap()
                .unwrap()
                .record_ref
                .unwrap()
                .segment_id,
            2
        );
        assert_eq!(store.get(&key_1).unwrap(), Some(b"payload-a".to_vec()));
        assert_eq!(store.get(&key_2).unwrap(), Some(b"payload-b".to_vec()));
        assert_eq!(store.get(&key_3).unwrap(), Some(b"x".to_vec()));
    }
}
