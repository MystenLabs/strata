use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use strata_core::{PlacementClass, SegmentId};

use crate::{Error, Result, SegmentIoObserver, SegmentWriter};

/// Shared source of store-global segment ids.
#[derive(Debug, Clone)]
pub struct SegmentIdAllocator {
    next: Arc<Mutex<SegmentId>>,
}

impl SegmentIdAllocator {
    pub fn new(next: SegmentId) -> Self {
        Self {
            next: Arc::new(Mutex::new(next)),
        }
    }

    pub fn allocate(&self) -> Result<SegmentId> {
        let mut next = self
            .next
            .lock()
            .expect("segment id allocator lock poisoned");
        let id = *next;
        *next = next.checked_add(1).ok_or(Error::RangeOverflow)?;
        Ok(id)
    }
}

/// Creates active segment writers from a shared id stream.
#[derive(Debug, Clone)]
pub struct SegmentFactory {
    directory: PathBuf,
    ids: SegmentIdAllocator,
    placement_class: PlacementClass,
    max_size: u64,
    io_observer: Option<Arc<dyn SegmentIoObserver>>,
}

impl SegmentFactory {
    pub fn new(
        directory: impl Into<PathBuf>,
        ids: SegmentIdAllocator,
        placement_class: PlacementClass,
        max_size: u64,
    ) -> Self {
        Self {
            directory: directory.into(),
            ids,
            placement_class,
            max_size,
            io_observer: None,
        }
    }

    pub fn with_io_observer(mut self, io_observer: Arc<dyn SegmentIoObserver>) -> Self {
        self.io_observer = Some(io_observer);
        self
    }

    pub fn create(&self) -> Result<SegmentWriter> {
        let id = self.ids.allocate()?;
        let path = self.directory.join(segment_file_name(id));
        match &self.io_observer {
            Some(observer) => SegmentWriter::create_with_io_observer(
                path,
                id,
                self.placement_class,
                self.max_size,
                Arc::clone(observer),
            ),
            None => SegmentWriter::create(path, id, self.placement_class, self.max_size),
        }
    }
}

pub fn segment_file_name(segment_id: SegmentId) -> String {
    format!("{segment_id:012}.data")
}

pub fn segment_path(directory: impl AsRef<Path>, segment_id: SegmentId) -> PathBuf {
    directory.as_ref().join(segment_file_name(segment_id))
}
