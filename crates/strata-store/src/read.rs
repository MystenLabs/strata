use std::{
    io::Read,
    ops::Range,
    time::{Duration, Instant},
};

use strata_core::{BlobKey, BlobState, RecordRef, SegmentFileState, SegmentId};
use strata_segment::{SegmentPayloadStream, SegmentReadOptions};

use crate::{Error, Result, StrataStore};

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

    pub(crate) fn live_record_ref(&self, key: &BlobKey) -> Result<Option<RecordRef>> {
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
