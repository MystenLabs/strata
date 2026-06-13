use std::{
    io::Read,
    ops::Range,
    time::{Duration, Instant},
};

use strata_core::{BlobKey, RecordRef, SegmentFileState, SegmentId, ShardId, ShardKey};
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
        let result = (|| {
            let Some(record_ref) = self.live_record_ref(shard, key)? else {
                return Ok(None);
            };

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

            Ok(Some(record.payload))
        })();
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

        let started = Instant::now();
        let Some(record_ref) = self.live_record_ref(STANDALONE_SHARD, key)? else {
            profile.record_lookup = started.elapsed();
            self.metrics
                .record_get(Ok(None), operation_started.elapsed());
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
        Ok((Some(record.payload), profile))
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
        let result = (|| {
            let Some(record_ref) = self.live_record_ref(shard, key)? else {
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
        })();
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
        let Some(resolved) = resolve_blob_version(&self.index, shard, key)? else {
            return Ok(None);
        };
        let record_ref = resolved.record_ref;
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

pub(crate) fn segment_state_is_readable(state: SegmentFileState) -> bool {
    !matches!(
        state,
        SegmentFileState::Deleting | SegmentFileState::Deleted
    )
}
