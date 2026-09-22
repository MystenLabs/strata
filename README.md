# Strata

Strata is an epoch aware blob storage library built around append-only segment files and RocksDB backed metadata.

Consumers should depend on the top-level `strata` crate. The lower-level crates are internal workspace layers that keep core types, segment I/O, metadata indexing, accounting, GC planning, and store orchestration separately testable.

The workspace is split across shared core types, segment I/O, index/accounting/store crates, and `gc` for pure GC planning and copy selection.

