use std::{
    io::{ErrorKind, Read},
    ops::Range,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use core_types::{
    BlobKey, BlobLifecycle, RecordRef, SegmentFileState, SegmentOwner, ShardId, ShardKey, StrataLsn,
};
use segment::{SegmentPayloadStream, SegmentReadOptions};

use lsm::{StoredValue, decode_value};

use crate::{
    Error, Result, STANDALONE_SHARD, StrataStore,
    blob_lsm::{BlobMerge, BlobState as LsmBlobState},
    layout::segment_state_path,
    partition::partition_for_key,
};

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

impl StrataStore {
    pub fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        self.get_blob(key)
    }

    pub fn get_from_shard(&self, shard_id: ShardId, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        let shard = self.readable_shard_key(shard_id)?;
        self.get_blob_for_shard(shard, key, ReadOptions::default())
    }

    pub fn get_with_options(&self, key: &BlobKey, options: ReadOptions) -> Result<Option<Vec<u8>>> {
        self.get_blob_with_options(key, options)
    }

    pub fn get_blob(&self, key: &BlobKey) -> Result<Option<Vec<u8>>> {
        self.get_blob_with_options(key, ReadOptions::default())
    }

    pub fn get_blob_with_options(
        &self,
        key: &BlobKey,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.get_blob_for_shard(STANDALONE_SHARD, key, options)
    }

    fn get_blob_for_shard(
        &self,
        shard: ShardKey,
        key: &BlobKey,
        options: ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        let started = Instant::now();
        let result = self.read_live_record_ref(shard, key, |record_ref, path| {
            let record =
                self.reader_cache
                    .read_record_with_options(path, record_ref, options.into())?;
            if &record.key != key {
                return Err(Error::KeyMismatch {
                    requested: key.clone(),
                    found: record.key,
                });
            }

            Ok(record.payload)
        });
        self.metrics.record_get(
            result
                .as_ref()
                .map(|payload| payload.as_ref().map(|payload| payload.len() as u64))
                .map_err(|_| ()),
            started.elapsed(),
        );
        result
    }

    pub fn get_blob_profiled(&self, key: &BlobKey) -> Result<(Option<Vec<u8>>, StoreGetProfile)> {
        self.get_blob_profiled_with_options(key, ReadOptions::default())
    }

    pub fn get_blob_profiled_with_options(
        &self,
        key: &BlobKey,
        options: ReadOptions,
    ) -> Result<(Option<Vec<u8>>, StoreGetProfile)> {
        let operation_started = Instant::now();
        let mut profile = StoreGetProfile::default();
        let mut retry_used = false;

        loop {
            let started = Instant::now();
            let readable = match self.live_record_ref_once(STANDALONE_SHARD, key)? {
                LiveRecordRef::Readable(readable) => readable,
                LiveRecordRef::Missing => {
                    profile.record_lookup = started.elapsed();
                    self.metrics
                        .record_get(Ok(None), operation_started.elapsed());
                    return Ok((None, profile));
                }
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. GC may have deleted the
                    // resolved source between them, so re-resolve once through its relocation.
                    profile.record_lookup = started.elapsed();
                    retry_used = true;
                    continue;
                }
                LiveRecordRef::Deleted(_) => {
                    profile.record_lookup = started.elapsed();
                    self.metrics
                        .record_get(Ok(None), operation_started.elapsed());
                    return Ok((None, profile));
                }
            };
            profile.record_lookup = started.elapsed();
            let record_ref = readable.record_ref;

            let (record, reader_profile) = match self.reader_cache.read_record_profiled(
                &readable.path,
                record_ref,
                options.into(),
                &mut profile.reader_acquire,
            ) {
                Ok(record) => record,
                Err(error) if !retry_used => {
                    if self.stale_ref_not_found(record_ref, &error)? {
                        retry_used = true;
                        continue;
                    }
                    self.metrics
                        .record_get(Err(()), operation_started.elapsed());
                    return Err(error);
                }
                Err(error) => {
                    self.metrics
                        .record_get(Err(()), operation_started.elapsed());
                    return Err(error);
                }
            };
            profile.fixed_header = reader_profile.fixed_header;
            profile.buffer_alloc = reader_profile.buffer_alloc;
            profile.record_body = reader_profile.record_body;
            profile.decode = reader_profile.decode;

            let started = Instant::now();
            if &record.key != key {
                self.metrics
                    .record_get(Err(()), operation_started.elapsed());
                return Err(Error::KeyMismatch {
                    requested: key.clone(),
                    found: record.key,
                });
            }
            profile.key_validate = started.elapsed();

            self.metrics.record_get(
                Ok(Some(record.payload.len() as u64)),
                operation_started.elapsed(),
            );
            return Ok((Some(record.payload), profile));
        }
    }

    pub fn get_blob_range(
        &self,
        key: &BlobKey,
        payload_range: Range<u64>,
    ) -> Result<Option<Vec<u8>>> {
        let started = Instant::now();
        let result = (|| {
            let Some(mut stream) = self.stream_blob(key, payload_range)? else {
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
        })();
        self.metrics.record_range_read(
            result
                .as_ref()
                .map(|payload| payload.as_ref().map(|payload| payload.len() as u64))
                .map_err(|_| ()),
            started.elapsed(),
        );
        result
    }

    pub fn stream_blob(
        &self,
        key: &BlobKey,
        payload_range: Range<u64>,
    ) -> Result<Option<SegmentPayloadStream>> {
        self.stream_blob_from_shard_key(STANDALONE_SHARD, key, payload_range)
    }

    fn stream_blob_from_shard_key(
        &self,
        shard: ShardKey,
        key: &BlobKey,
        payload_range: Range<u64>,
    ) -> Result<Option<SegmentPayloadStream>> {
        let started = Instant::now();
        let result = self.read_live_record_ref(shard, key, |record_ref, path| {
            let metadata = self.reader_cache.read_record_metadata(path, record_ref)?;
            if metadata.key != *key {
                return Err(Error::KeyMismatch {
                    requested: key.clone(),
                    found: metadata.key,
                });
            }

            self.reader_cache
                .open_payload_stream(path, record_ref, payload_range.clone())
        });
        self.metrics.record_stream(
            result
                .as_ref()
                .map(|stream| stream.is_some())
                .map_err(|_| ()),
            started.elapsed(),
        );
        result
    }

    pub fn contains(&self, key: &BlobKey) -> Result<bool> {
        Ok(self.live_record_ref(STANDALONE_SHARD, key)?.is_some())
    }

    pub(crate) fn live_record_ref(
        &self,
        shard: ShardKey,
        key: &BlobKey,
    ) -> Result<Option<RecordRef>> {
        let mut retry_used = false;
        loop {
            match self.live_record_ref_once(shard, key)? {
                LiveRecordRef::Readable(readable) => return Ok(Some(readable.record_ref)),
                LiveRecordRef::Missing => return Ok(None),
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. GC may have deleted the
                    // resolved source between them, so re-resolve once through its relocation.
                    retry_used = true;
                }
                LiveRecordRef::Deleted(_) => return Ok(None),
            }
        }
    }

    pub(crate) fn read_live_record_ref<T>(
        &self,
        shard: ShardKey,
        key: &BlobKey,
        mut read: impl FnMut(RecordRef, &Path) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut retry_used = false;
        loop {
            let readable = match self.live_record_ref_once(shard, key)? {
                LiveRecordRef::Readable(readable) => readable,
                LiveRecordRef::Missing => return Ok(None),
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. GC may have deleted the
                    // resolved source between them, so re-resolve once through its relocation.
                    retry_used = true;
                    continue;
                }
                LiveRecordRef::Deleted(_) => return Ok(None),
            };
            let record_ref = readable.record_ref;

            match read(record_ref, &readable.path) {
                Ok(value) => return Ok(Some(value)),
                Err(error) if !retry_used => {
                    if self.stale_ref_not_found(record_ref, &error)? {
                        retry_used = true;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn live_record_ref_once(&self, shard: ShardKey, key: &BlobKey) -> Result<LiveRecordRef> {
        let Some(resolved) = resolve_blob_version(self, shard, key)? else {
            return Ok(LiveRecordRef::Missing);
        };
        let mut record_ref = resolved.record_ref;
        let mut state = self.segment_state_for_read(shard, record_ref)?;
        let mut relocation_cache_fill = None;
        if state
            .as_ref()
            .is_some_and(|state| state.state == SegmentFileState::Deleted)
        {
            self.evict_segment_reader(record_ref.segment_id);
            let cached = self.relocation_cache.get(key, shard, resolved.payload_lsn);
            self.metrics
                .record_relocation_cache_lookup(cached.is_some());
            let translated = match cached {
                Some(to) => to,
                None => {
                    let started = Instant::now();
                    let relocation = self.relocations.lookup(key, shard, resolved.payload_lsn);
                    self.metrics.record_relocation_lookup(
                        match &relocation {
                            Ok(Some(_)) => Ok(true),
                            Ok(None) => Ok(false),
                            Err(_) => Err(()),
                        },
                        started.elapsed(),
                    );
                    match relocation? {
                        Some(relocation) => {
                            relocation_cache_fill = Some(relocation.to);
                            relocation.to
                        }
                        None => record_ref,
                    }
                }
            };
            if translated.len != record_ref.len {
                return Err(Error::InvariantViolation {
                    reason: format!(
                        "relocation changes record length from {} to {}",
                        record_ref.len, translated.len
                    ),
                });
            }
            if translated == record_ref {
                return Ok(LiveRecordRef::Deleted(record_ref));
            }
            record_ref = translated;
            state = self.segment_state_for_read(shard, record_ref)?;
        }
        match state {
            Some(state) if segment_state_is_readable(state.state) => {
                if let Some(to) = relocation_cache_fill {
                    self.relocation_cache
                        .insert(key.clone(), shard, resolved.payload_lsn, to);
                }
                Ok(LiveRecordRef::Readable(ReadableRecordRef {
                    record_ref,
                    path: segment_state_path(&self.config, &state),
                }))
            }
            Some(state) if state.state == SegmentFileState::Deleted => {
                self.evict_segment_reader(record_ref.segment_id);
                Ok(LiveRecordRef::Deleted(record_ref))
            }
            Some(_) | None => {
                self.evict_segment_reader(record_ref.segment_id);
                Ok(LiveRecordRef::Missing)
            }
        }
    }

    fn segment_state_for_read(
        &self,
        shard: ShardKey,
        record_ref: RecordRef,
    ) -> Result<Option<core_types::SegmentState>> {
        Ok(self
            .index
            .get_segment_state(record_ref.segment_id)?
            .filter(|state| {
                matches!(state.owner, SegmentOwner::Store)
                    || state.owner == SegmentOwner::Shard(shard)
            }))
    }

    fn stale_ref_not_found(&self, record_ref: RecordRef, error: &Error) -> Result<bool> {
        if !segment_read_not_found(error) {
            return Ok(false);
        }
        // GC may unlink the resolved source before the physical read. Retry only when metadata
        // confirms the segment was deleted; otherwise surface the storage error.
        Ok(self
            .index
            .get_segment_state(record_ref.segment_id)?
            .is_some_and(|state| state.state == SegmentFileState::Deleted))
    }

    fn readable_shard_key(&self, shard_id: ShardId) -> Result<ShardKey> {
        match self.index.get_shard_info(shard_id)? {
            Some(info) if info.is_active() => Ok(info.key(shard_id)),
            Some(info) => Err(Error::ShardUnavailable {
                shard_id,
                generation: info.current_generation,
                current_generation: info.current_generation,
                state: info.state,
            }),
            None => Err(Error::ShardNotFound { shard_id }),
        }
    }

    #[cfg(test)]
    pub(crate) fn reader_cache_len(&self) -> usize {
        self.reader_cache.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiveRecordRef {
    Missing,
    Readable(ReadableRecordRef),
    Deleted(RecordRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadableRecordRef {
    record_ref: RecordRef,
    path: PathBuf,
}

fn segment_read_not_found(error: &Error) -> bool {
    matches!(
        error,
        Error::Segment(segment::Error::Io { source, .. })
            if source.kind() == ErrorKind::NotFound
    )
}

pub(crate) fn segment_state_is_readable(state: SegmentFileState) -> bool {
    state != SegmentFileState::Deleted
}

/// What the read path needs from the index: where the payload bytes live, plus the current
/// blob-level lifecycle when one has been recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBlobVersion {
    pub head_lsn: StrataLsn,
    pub record_ref: core_types::RecordRef,
    pub generation: core_types::Generation,
    pub lifecycle: Option<BlobLifecycle>,
    pub payload_lsn: StrataLsn,
}

/// Resolves a key to its readable payload, or None for missing/tombstoned blobs.
///
/// A live head without a payload ref should not be produced by new writes. If recovery leaves such
/// a head behind, there are no bytes to return, so None is the honest answer.
pub(crate) fn resolve_blob_version(
    store: &StrataStore,
    shard: ShardKey,
    key: &BlobKey,
) -> Result<Option<ResolvedBlobVersion>> {
    let lsm = store.lsm()?;
    let partition = partition_for_key(key.as_bytes(), store.config.lsm_partition_count);
    let Some(encoded) = lsm.get(partition, key.as_bytes(), &BlobMerge)? else {
        return Ok(None);
    };
    let StoredValue::Inline(bytes) = decode_value(&encoded)? else {
        return Err(Error::InvariantViolation {
            reason: format!("materialized blob state for {key:?} is segment-backed"),
        });
    };
    let state = LsmBlobState::decode(bytes)?;
    let current_epoch = store.current_epoch()?;
    let Some((version, lifecycle)) = state.resolve(shard, current_epoch) else {
        return Ok(None);
    };
    Ok(Some(ResolvedBlobVersion {
        head_lsn: version.lsn,
        record_ref: version.record_ref,
        generation: version.lsn,
        lifecycle,
        payload_lsn: version.lsn,
    }))
}
