//! Current physical locations for payloads moved by Strata GC.
//!
//! This crate is a typed wrapper over a dedicated [`strata_lsm::Lsm`]. Relocations use the blob key
//! as the explicit key prefix, shard generation and payload LSN as the suffix, and the latest
//! physical reference as the value.

mod error;
mod store;

pub use error::{Error, Result};
pub use store::{Relocation, RelocationEntry, RelocationMerge, RelocationScan, RelocationStore};
