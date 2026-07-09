//! Public facade for the Strata blob store.
//!
//! Most consumers should depend on this crate instead of the internal workspace crates. The
//! lower-level crates remain split out to keep storage vocabulary, segment I/O, metadata indexing,
//! accounting, GC planning, and store orchestration independently testable.

pub use strata_store::*;

/// Shared storage vocabulary and record-format types.
pub mod core {
    pub use strata_core::*;
}

/// Pure garbage-collection planning APIs.
pub mod gc {
    pub use strata_gc::*;
}
