//! Blocking append/read/scan support for Strata segment files.
//!
//! A segment is an append-only file containing `strata-core` records back to back:
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
//! Responsibilities:
//!
//! - `SegmentWriter` appends one complete encoded record at `write_offset`.
//! - `SegmentReader` reads full records with checksum validation, or validated payload ranges
//!   when the caller needs file-range reads.
//! - `SegmentScanner` walks records from offset 0 and returns the longest valid prefix.
//!
//! Recovery behavior is intentionally conservative:
//!
//! ```text
//! valid record | valid record | partial/corrupt tail
//! <------------- valid prefix ------------->
//! ```
//!
//! A partial tail is expected after a crash and can be truncated by the store layer. Corruption
//! before the durable prefix is reported as an error. This crate does not allocate LSNs, update
//! metadata, decide active segments, or seal segments.

mod error;
mod reader;
mod scanner;
mod writer;

pub use error::{Error, Result};
pub use reader::{RecordMetadata, SegmentPayloadStream, SegmentReader};
pub use scanner::{ScannedRecord, SegmentScanner, ValidPrefix};
pub use writer::{AppendOutcome, SegmentWriter};
