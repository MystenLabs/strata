//! Current physical locations for payloads moved by Strata GC.
//!
//! Relocations live in a dedicated [`lsm::Lsm`]. They use the blob key as the explicit key
//! prefix, shard generation and payload LSN as the suffix, and the latest physical reference as
//! the value. The cache is consulted after a main-LSM lookup resolves a source segment that has
//! already been deleted.

mod activations;
mod cache;
mod store;

pub(crate) use activations::RelocationActivations;
pub use cache::DEFAULT_RELOCATION_CACHE_ENTRIES;
pub(crate) use cache::RelocationCache;
pub(crate) use store::{RelocationEntry, RelocationMerge, RelocationScan, RelocationStore};

#[cfg(test)]
mod tests;
