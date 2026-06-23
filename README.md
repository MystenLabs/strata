# Strata

Strata is an epoch-aware blob storage library built around append-only segment files and RocksDB-backed metadata.

The first crate, `strata-core`, defines the stable storage vocabulary and binary record format shared by the segment, index, GC, and integration crates.

See `design_doc.md` for the current storage design notes, including the ordered blob-version index idea for making accounting a controlled merge/compaction stage instead of a separate random-lookup process.
