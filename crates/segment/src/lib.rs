//! Blocking append/read/scan support for Strata segment files.
//!
//! A segment is an append-only file containing `core` records back to back:
//!
//! ```text
//! segment file
//! +------------+------------+------------+-----------+
//! | record 0   | record 1   | record 2   | tail ...  |
//! +------------+------------+------------+-----------+
//! ^            ^            ^
//! offset 0     RecordRef    RecordRef
//! ```
//!
//! `SegmentWriter` appends complete records, `SegmentReader` resolves record references, and
//! `SegmentScanner` finds the longest valid prefix after a crash. Segment selection, rollover,
//! and commit visibility belong to the engine using this crate.

mod error;
mod factory;
mod io;
mod reader;
mod scanner;
mod writer;

pub use error::{Error, Result};
pub use factory::{SegmentFactory, SegmentIdAllocator, segment_file_name, segment_path};
pub use io::SegmentIoObserver;
pub use reader::{
    RecordMetadata, SegmentPayloadStream, SegmentReadOptions, SegmentReadProfile, SegmentReader,
};
pub use scanner::{ScannedRecord, SegmentScanner, ValidPrefix};
pub use writer::{AppendOutcome, SegmentWriter};
