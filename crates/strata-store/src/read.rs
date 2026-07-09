use std::{
    io::{ErrorKind, Read},
    ops::Range,
    time::{Duration, Instant},
};

use strata_core::{BlobKey, RecordRef, SegmentFileState, ShardId, ShardKey};
use strata_segment::{SegmentPayloadStream, SegmentReadOptions};

use crate::{Error, Result, STANDALONE_SHARD, StrataStore, resolve_blob_version};

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
        let result = self.read_live_record_ref(shard, key, |record_ref| {
            let record = self.reader_cache.read_record_with_options(
                &self.config,
                record_ref,
                options.into(),
            )?;
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
            let record_ref = match self.live_record_ref_once(STANDALONE_SHARD, key)? {
                LiveRecordRef::Readable(record_ref) => record_ref,
                LiveRecordRef::Missing => {
                    profile.record_lookup = started.elapsed();
                    self.metrics
                        .record_get(Ok(None), operation_started.elapsed());
                    return Ok((None, profile));
                }
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. Example: this read saw
                    // `K -> S10`, then GC published `MapRef(K, S10 -> S22)` and EmptyDelete marked
                    // S10 Deleted before this state check. Re-resolve once before reporting a miss.
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

            let (record, reader_profile) = match self.reader_cache.read_record_profiled(
                &self.config,
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
        let result = self.read_live_record_ref(shard, key, |record_ref| {
            let metadata = self
                .reader_cache
                .read_record_metadata(&self.config, record_ref)?;
            if metadata.key != *key {
                return Err(Error::KeyMismatch {
                    requested: key.clone(),
                    found: metadata.key,
                });
            }

            self.reader_cache
                .open_payload_stream(&self.config, record_ref, payload_range.clone())
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
                LiveRecordRef::Readable(record_ref) => return Ok(Some(record_ref)),
                LiveRecordRef::Missing => return Ok(None),
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. Example: this read saw
                    // `K -> S10`, then GC published `MapRef(K, S10 -> S22)` and EmptyDelete marked
                    // S10 Deleted before this state check. Re-resolve once before reporting a miss.
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
        mut read: impl FnMut(RecordRef) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut retry_used = false;
        loop {
            let record_ref = match self.live_record_ref_once(shard, key)? {
                LiveRecordRef::Readable(record_ref) => record_ref,
                LiveRecordRef::Missing => return Ok(None),
                LiveRecordRef::Deleted(_) if !retry_used => {
                    // The blob row and segment state are separate reads. Example: this read saw
                    // `K -> S10`, then GC published `MapRef(K, S10 -> S22)` and EmptyDelete marked
                    // S10 Deleted before this state check. Re resolve once before reporting a miss.
                    retry_used = true;
                    continue;
                }
                LiveRecordRef::Deleted(_) => return Ok(None),
            };

            match read(record_ref) {
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
        let Some(resolved) = resolve_blob_version(&self.index, shard, key)? else {
            return Ok(LiveRecordRef::Missing);
        };
        let record_ref = resolved.record_ref;
        match self.index.get_segment_state(record_ref.segment_id)? {
            Some(state) if segment_state_is_readable(state.state) => {
                Ok(LiveRecordRef::Readable(record_ref))
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

    fn stale_ref_not_found(&self, record_ref: RecordRef, error: &Error) -> Result<bool> {
        if !segment_read_not_found(error) {
            return Ok(false);
        }
        // Example: this read resolved `K -> S10`, then GC published `MapRef(K, S10 -> S22)` and
        // EmptyDelete unlinked S10 before the physical read. Only retry that NotFound when metadata
        // also says S10 is Deleted; if metadata says S10 is readable, surface the storage error.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveRecordRef {
    Missing,
    Readable(RecordRef),
    Deleted(RecordRef),
}

fn segment_read_not_found(error: &Error) -> bool {
    matches!(
        error,
        Error::Segment(strata_segment::Error::Io { source, .. })
            if source.kind() == ErrorKind::NotFound
    )
}

pub(crate) fn segment_state_is_readable(state: SegmentFileState) -> bool {
    state != SegmentFileState::Deleted
}
