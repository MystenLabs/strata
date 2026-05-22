# Strata

Strata is an epoch-aware blob storage library built around append-only segment files and RocksDB-backed metadata.

The first crate, `strata-core`, defines the stable storage vocabulary and binary record format shared by the segment, index, GC, and integration crates.
