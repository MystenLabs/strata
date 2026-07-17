# Accounting

Strata accounting turns foreground blob operations into durable, segment-oriented facts used by
garbage collection. The foreground path only appends ordered deltas. The background accounting
worker groups and folds those deltas, then publishes ref events, GC overlay operands, and the global
`accounted_lsn` frontier.

## Vocabulary

- **Accounting worker**: the background thread that receives wakeups and serializes accounting
  passes.
- **Accounting processor**: the stateful pipeline owned by the worker for one open store.
- **Accounting index**: the file-backed run store represented by `AccountingIndex` and its manifest.
- **Store index**: the RocksDB-backed `StrataIndex` that owns the durable accounting manifest,
  cursors, ref events, GC overlays, and frontier.
- **Delta compaction**: groups raw delta runs by key and writes residual patch histories.
- **Major compaction**: folds base state and patch histories into a replacement base run.

## End-to-end flow

```text
foreground write
  -> append AccountingLogEntry to the active accounting log
  -> sync payload, log, and store-index metadata

accounting worker
  -> read the durable active-log range
  -> prepare partitioned delta runs
  -> publish manifest + consumed cursor
  -> delta-compact raw updates into patch runs
  -> major-compact base + patches into a new base run
  -> project compaction RefEvents into segment ref events and GC overlay operands
  -> publish derived rows + manifest + accounted_lsn atomically
```

The accounting index has three physical run levels:

```text
delta-*.run  sorted BlobUpdate records waiting to be grouped
patch-*.run  residual per-key history that still needs older state
base-*.run   MaterializedBlobState for each key
```

Delta compaction is deliberately shallow: it does not read base or existing patch state. Major
compaction is the point where residual history is interpreted against older materialized state.

## Prepare, publish, apply

Every accounting-index mutation follows the same protocol:

1. **Prepare** writes and syncs any new immutable run files and builds a candidate manifest.
2. **Publish** commits the candidate manifest, cursor changes, derived rows, and frontier changes in
   one synced RocksDB batch.
3. **Apply** adopts the already-published manifest in the in-memory accounting index and removes
   obsolete run files.

The order is a crash-safety contract. Publishing before run files are synced could expose missing
files. Advancing the consumed cursor without its manifest could skip deltas after restart. Removing
old runs before the manifest is durable could destroy the current readable state.

Prepared values carry the manifest generation they were based on. Applying stale prepared work is
rejected before obsolete files are removed.

## Accounting passes

All processor entry points execute the same ordered stages with a different `AccountingPassPolicy`:

```text
ingest -> delta compaction -> major compaction -> shard-drop materialization
```

- A writer nudge forces ingest but normally leaves both compaction levels threshold-driven. If
  `durable_lsn - accounted_lsn` reaches `accounting_materialize_lag_threshold`, the worker promotes
  that nudge to a materializing pass. Zero disables this lag trigger.
- A materialization request forces ingest and both compaction levels.
- Periodic maintenance uses thresholds until its wall-clock deadline or pending shard cleanup
  forces a complete pass.

## Source map

The implementation is organized by responsibility:

```text
crates/strata-store/src/accounting/
  requests.rs          request coalescing and priority
  worker.rs            background event loop
  processor.rs         processor lifetime and pass sequencing
  ingest.rs            active-log ingestion and reclamation
  compaction.rs        compaction thresholds and scheduling
  publish.rs           atomic store-index publication and frontier advancement
  event_projection.rs compaction events to GC-facing rows

crates/strata-accounting/src/
  active_log/          active-log writer, reader, and recovery
  state/               accounting model, run records, and reducer
  index/               run storage, queries, prepared changes, and compaction
  events.rs            logical compaction events
  manifest.rs          durable accounting-index root
  run_io.rs            framed immutable run I/O
```

## Core invariants

- The accounting worker serializes publication because live-allocation overlay operands are not
  idempotent.
- The active-log reader never reads beyond the foreground-published durable offset.
- A run file is live only when the durable accounting manifest references it.
- Delta compaction preserves any operation whose meaning depends on older state.
- `accounted_lsn` advances only through a contiguous prefix whose key operations, epoch changes,
  and shard drops have been materialized.
- GC may reclaim bytes only from the durable segment-oriented accounting view.
