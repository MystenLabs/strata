# Strata

Strata is an epoch-aware blob storage library built around append-only segment files and RocksDB-backed metadata.

Consumers should depend on the top-level `strata` crate. The lower-level crates are internal workspace layers that keep core types, segment I/O, metadata indexing, accounting, GC planning, and store orchestration separately testable.

The workspace is split across shared core types, segment I/O, index/accounting/store crates, and `gc` for pure GC planning and copy selection.

See `docs/design_doc.md` for the current storage design notes, including the ordered blob-version index idea for making accounting a controlled merge/compaction stage instead of a separate random-lookup process.

## Workspaces

Building or testing Strata never fetches the walrus repository. Nothing in the main workspace
depends on `typed-store`, which lives inside MystenLabs/walrus, so an embedder such as walrus can
depend on `strata` without a circular dependency.

Three crates do still use `typed-store`, and each is its own workspace, excluded from the root:

| crate | purpose | build |
| --- | --- | --- |
| `crates/bench` | RocksDB BlobDB micro-benchmark baselines | `cargo build --release --manifest-path crates/bench/Cargo.toml` |
| `crates/realistic-bench` | paired Strata / BlobDB workload runs (`--engine strata\|blobdb`) | `cargo build --release --manifest-path crates/realistic-bench/Cargo.toml` |
| `crates/port-compat` | checks the storage port against `typed-store` | `cargo test --manifest-path crates/port-compat/Cargo.toml` |

`.cargo/config.toml` points every workspace at one shared `target/` directory, so binaries still land
at `target/release/<name>` regardless of which manifest was built.

These are excluded rather than feature-gated on purpose: an optional dependency still appears in the
lockfile and is still fetched, because Cargo needs its manifest to resolve. Exclusion is what
actually keeps it out. It also means the bench crates can never be pulled in as workspace members by
something vendoring this repository, which would reintroduce the cycle.

CI must run the excluded workspaces as their own steps. `crates/port-compat` is the one that should
never be skipped: it is what guarantees the storage port still reads and writes the exact on-disk
format the index shipped with.
