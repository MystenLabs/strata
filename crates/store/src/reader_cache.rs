use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use core_types::{DecodedRecord, RecordRef, SegmentId};
use segment::{
    RecordMetadata, SegmentIoObserver, SegmentPayloadStream, SegmentReadOptions,
    SegmentReadProfile, SegmentReader,
};

use crate::Result;

#[derive(Debug)]
pub(crate) struct SegmentReaderCache {
    capacity: usize,
    io_observer: Arc<dyn SegmentIoObserver>,
    inner: Mutex<SegmentReaderCacheInner>,
}

#[derive(Debug, Default)]
struct SegmentReaderCacheInner {
    readers: HashMap<SegmentId, Arc<Mutex<SegmentReader>>>,
    lru: VecDeque<SegmentId>,
}

impl SegmentReaderCache {
    pub(crate) fn new(capacity: usize, io_observer: Arc<dyn SegmentIoObserver>) -> Self {
        Self {
            capacity,
            io_observer,
            inner: Mutex::new(SegmentReaderCacheInner::default()),
        }
    }

    pub(crate) fn read_record_with_options(
        &self,
        path: &Path,
        record_ref: RecordRef,
        options: SegmentReadOptions,
    ) -> Result<DecodedRecord> {
        self.with_reader(path, record_ref.segment_id, |reader| {
            reader.read_record_with_options(record_ref, options)
        })
    }

    pub(crate) fn read_record_profiled(
        &self,
        path: &Path,
        record_ref: RecordRef,
        options: SegmentReadOptions,
        reader_acquire: &mut Duration,
    ) -> Result<(DecodedRecord, SegmentReadProfile)> {
        if self.capacity == 0 {
            let started = Instant::now();
            let mut reader = SegmentReader::open_with_io_observer(
                path,
                record_ref.segment_id,
                Arc::clone(&self.io_observer),
            )?;
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
                let reader = Arc::new(Mutex::new(SegmentReader::open_with_io_observer(
                    path,
                    record_ref.segment_id,
                    Arc::clone(&self.io_observer),
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

    pub(crate) fn read_record_metadata(
        &self,
        path: &Path,
        record_ref: RecordRef,
    ) -> Result<RecordMetadata> {
        self.with_reader(path, record_ref.segment_id, |reader| {
            reader.read_record_metadata(record_ref)
        })
    }

    pub(crate) fn open_payload_stream(
        &self,
        path: &Path,
        record_ref: RecordRef,
        payload_range: Range<u64>,
    ) -> Result<SegmentPayloadStream> {
        self.with_reader(path, record_ref.segment_id, |reader| {
            reader.open_payload_stream(record_ref, payload_range)
        })
    }

    pub(crate) fn evict(&self, segment_id: SegmentId) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.remove(segment_id);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .readers
            .len()
    }

    fn with_reader<T>(
        &self,
        path: &Path,
        segment_id: SegmentId,
        read: impl FnOnce(&mut SegmentReader) -> segment::Result<T>,
    ) -> Result<T> {
        if self.capacity == 0 {
            let mut reader = SegmentReader::open_with_io_observer(
                path,
                segment_id,
                Arc::clone(&self.io_observer),
            )?;
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
                let reader = Arc::new(Mutex::new(SegmentReader::open_with_io_observer(
                    path,
                    segment_id,
                    Arc::clone(&self.io_observer),
                )?));
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
