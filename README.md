# Strata

Strata is an epoch-aware blob storage library built around append-only segment files and RocksDB-backed metadata.

The workspace is split across shared core types, segment I/O, index/accounting/store crates, and `strata-gc` for pure GC planning and copy selection.

See `design_doc.md` for the current storage design notes, including the ordered blob-version index idea for making accounting a controlled merge/compaction stage instead of a separate random-lookup process.
