# MapRef Relocation LSM Design

Status: proposed

## Summary

Move GC `MapRef` publication out of per-key RocksDB merge operands and per-record relocation rows
into a file-backed relocation LSM.

GC already copies payload bytes into immutable output segments before entering the serialized writer
path. It should also prepare and sync immutable relocation runs outside that path. The writer then
assigns one contiguous LSN range and atomically publishes a small run descriptor together with
output segment state, source fencing, and the global LSN frontier.

The expected writer-path change is:

```text
current GC publish for N copied records:
  N blob_versions MapRef merge operands
  N gc_relocations rows
  N unaccounted_lsn_ops rows
  one O(N) accounting-log frame
  output/source segment-state changes

proposed GC publish for N copied records in R relocation runs:
  R relocation-run descriptors
  R unaccounted LSN-range markers
  R small accounting-log run references
  output/source segment-state changes
```

`R` is bounded by a configured run-size limit and should normally be much smaller than `N`.

The relocation LSM becomes the authoritative online forwarding view for GC-moved physical refs. It
does not decide logical liveness. Blob accounting still applies each MapRef conditionally and emits
segment-liveness transitions only when the mapped source is the physical version actually protected
by the logical key.

## Relationship to Other Designs

This proposal specializes and makes concrete the relocation-run direction in
`design_segment_relocation_manifest.md`. A single immutable relocation manifest is useful for one
GC publication; an LSM adds:

- Multiple immutable runs.
- A versioned live-run manifest.
- Source-key lookup across runs.
- Compaction and relocation-chain collapse.
- Retired/healed mapping tombstones.
- Bounded read amplification.
- Snapshot and consumer pinning.

It is complementary to `design_segment_accounting_lsm.md`:

```text
relocation LSM
  immediate visibility
  foreground read correctness
  exact physical forwarding

segment-accounting LSM
  accounting-frontier visibility
  GC planning and copy selection
  physical liveness, lifetimes, summaries, and changes
```

The same immutable relocation run may be referenced by both pipelines, but the two logical indexes
must retain distinct visibility frontiers.

## Context

GC copies live records from sealed source segments into sealed output segments. For every copied
record, the current publisher assigns a synthetic publish LSN and constructs:

```rust
MapRefOp {
    publish_lsn,
    shard,
    payload_lsn,
    from,
    to,
}
```

The MapRef is conditional. It rewrites a payload only when both the original payload version and the
physical source ref match:

```text
payload_lsn matches the protected version
and current RecordRef == from
```

This prevents GC from corrupting a newer logical version that reused the same physical ref or was
published concurrently with the GC job.

Current GC publication performs the following ordering-sensitive work inside the serialized writer:

1. Read ref events newer than the GC accounting snapshot.
2. Reconcile copied records and discard known stale copies.
3. Assign one publish LSN per surviving record.
4. Publish output segment state.
5. Fence source segments as `GcRelocating`.
6. Add one `gc_relocations` row per source ref.
7. Add one `MapRef` merge operand to `blob_versions` per blob key.
8. Add one `unaccounted_lsn_ops` row per publish LSN.
9. Append all maps to one bulk active-accounting-log frame.
10. Advance `next_lsn`.

Output file rename and fsync are already outside the writer. The remaining record-proportional
metadata work can still occupy the writer queue long enough to increase foreground tail latency.

## Problem Statement

### Record-proportional work in the serialized writer

The serialized writer is the commit-order boundary for user operations and GC publication. A GC copy
with many records creates a large RocksDB batch while later user writes wait in the same queue.

The payloads themselves are not the issue at this point: they were copied and made durable earlier.
The critical section is dominated by encoding, allocating, and committing small per-record metadata
operations.

Chunking the current batch improves fairness but does not remove the work. It also makes one logical
GC move visible in several partial steps and extends the period during which both source and output
metadata must be protected.

### MapRef operands enlarge the foreground blob index

Every MapRef is stored as ordered history inside the packed `blob_versions` value for its key. This
preserves exact read, rollback, and accounting semantics, but physical relocation traffic now enters
the same LSM values that serve foreground logical reads.

The merge operand is small, but it still contributes:

- WAL and memtable bytes.
- Blob-version merge depth.
- Full-merge and compaction work.
- Larger packed values until the compact-safe frontier folds the mapping into a head.

Physical movement is not a logical content update. Keeping it in an independently compacted physical
forwarding index can reduce interference while retaining the same conditional identity.

### Temporary relocation rows duplicate MapRef contents

`gc_relocations[from] -> { publish_lsn, to }` provides accounting-event forwarding while accounting
catches up. Foreground reads currently obtain the mapped ref from `blob_versions`; they do not read
this temporary table. The row is later removed after the MapRef reaches the accounted frontier. The
same source and destination also exist in the blob-version operand and accounting log.

A relocation LSM can be both the durable forwarding view and the input consumed by accounting,
eliminating the per-record temporary RocksDB representation.

### Per-LSN unaccounted rows remain a hidden scaling term

Removing only the `blob_versions` MapRef merges is insufficient. Current GC publication also writes
one `unaccounted_lsn_ops` row for each synthetic MapRef LSN.

Because one relocation run owns a contiguous LSN interval, accounting can track the interval as one
range marker and advance across it once every involved accounting partition has materialized the
run. Without this change, GC publication remains `O(N)` in RocksDB operations.

### The accounting log is batched but still record-sized

The active accounting log already stores GC mappings in one `GcMapRefBatch` frame. That avoids one
frame header and syscall per record, but the writer still serializes and appends every key and
mapping.

If the relocation file is already synced, the accounting log can contain a run reference instead of
a second copy of the entries.

## Goals

- Make serialized GC publication cost proportional to relocation runs and touched segments rather
  than copied records.
- Remove GC MapRef operands from foreground blob-version values.
- Replace per-record `gc_relocations` rows with one authoritative forwarding index.
- Replace per-record unaccounted-LSN rows with contiguous run-range markers.
- Preserve exact `(key, shard, payload_lsn, from) -> to` conditional mapping semantics.
- Preserve the global LSN order observed by blob accounting.
- Keep destination bytes durable before any read can be forwarded to them.
- Allow foreground reads to resolve refs whose original source segment has been deleted.
- Let blob accounting consume relocation entries without copying them into another durable log.
- Bound relocation lookup and chain depth through compaction and caching.
- Preserve crash rollback and deterministic recovery.

## Non-goals

- The relocation LSM does not decide whether a logical key is live, tombstoned, expired, or current.
- It does not replace the blob-accounting LSM.
- It does not directly update segment summaries or liveness overlays.
- It does not allow a source segment to be deleted before durable forwarding is visible.
- It does not require synchronous read-side healing.
- It does not eliminate all RocksDB work from GC publication; small roots, segment states, and global
  frontiers remain in the atomic commit domain.
- It does not require permanent relocation indirection if bounded background healing proves better.

## Proposed Ownership Model

```text
foreground blob index
  logical blob heads, versions, tombstones, lifecycles
  does not receive immediate per-record MapRef merges

relocation LSM
  exact physical source -> destination forwarding
  immediately visible after GC publish
  input to blob accounting

blob-accounting LSM
  folds MapRefs in global LSN order against logical history
  decides whether a mapping applied or was obsolete/pending

segment-accounting LSM
  consumes resolved Mapped/Retired/Live transitions
  updates physical liveness, summaries, and GC history
```

The transition pipeline is:

```text
GC copies bytes
  -> relocation run becomes visible to reads
  -> blob accounting consumes the run in LSN order
  -> conditional MapRefs resolve against logical state
  -> resolved physical transitions enter segment accounting
  -> source bytes eventually become reclaimable
```

## Relocation Identity

The lookup key must identify the exact physical version moved by GC:

```rust
struct RelocationKey {
    shard: ShardKey,
    payload_lsn: StrataLsn,
    from: RecordRef,
}
```

The run entry also stores the blob key or a collision-resistant key digest:

```rust
struct RelocationEntry {
    key: BlobKey,
    identity: RelocationKey,
    to: RecordRef,
    ordinal: u32,
}
```

The full blob key is preferable initially because it supports recovery diagnostics, accounting
partition routing, and exact corruption checks. A compact digest plus a separate key dictionary is
a later format optimization.

`payload_lsn` cannot be replaced with the latest blob-head LSN. Metadata-only operations can advance
the head while the protected payload version remains older. The foreground resolution API must
therefore expose the payload LSN in addition to the current head LSN and record ref.

## Immutable Run Format

### Prepared run

A relocation run is created before its absolute publish LSN is known:

```rust
struct PreparedRelocationRunHeader {
    format_version: u32,
    run_id: RelocationRunId,
    entry_count: u32,
    source_segment_count: u32,
    output_segment_count: u32,
    entries_checksum: [u8; 32],
}

struct PreparedRelocationEntry {
    ordinal: u32,
    key: BlobKey,
    shard: ShardKey,
    payload_lsn: StrataLsn,
    from: RecordRef,
    to: RecordRef,
}
```

Absolute publish LSN is derived after publication:

```text
publish_lsn(entry) = run_descriptor.base_lsn + entry.ordinal
```

This allows the complete record-proportional file to be written and synced without holding the
serialized writer.

### Entry ordering

Entries should be sorted deterministically by:

```text
(from.segment_id, from.offset, from.len, payload_lsn, shard, key)
```

The assigned ordinal follows that order. MapRefs for distinct physical identities commute, so GC
does not require the staging-copy order to become the LSN order. Sorting provides:

- Sequential lookup by source segment and offset.
- Deterministic retry output.
- Straightforward duplicate detection.
- LSN-order accounting consumption by scanning file order.

If future semantics introduce dependencies among entries, the format must retain a separate
LSN-order index rather than assuming physical sort order is safe.

### Run indexes

Each run should contain:

- A sparse index from source segment ID to its first and last entry block.
- A Bloom filter over relocation identity.
- Output segment IDs referenced by the run.
- Accounting partition bitmap or per-partition maximum ordinal.
- File length and checksum.
- Optional per-block checksums and compression.

The accounting partition metadata lets the global accounted frontier determine when every key in a
run has been materialized without writing one RocksDB row per entry.

### Chunking

Large publications are split by a configured entry or byte limit:

```text
max_relocation_run_entries
max_relocation_run_bytes
```

Runs should not split one physical source segment unnecessarily, because source-scoped reads and
cleanup benefit from locality. The writer assigns adjacent LSN intervals to the prepared runs in
deterministic order.

## Manifest and Visibility

The relocation LSM has a versioned live-run manifest. A published descriptor supplies the fields
that were not known during file preparation:

```rust
struct PublishedRelocationRun {
    run_id: RelocationRunId,
    base_lsn: StrataLsn,
    entry_count: u32,
    file_len: u64,
    checksum: [u8; 32],
    state: RelocationRunState,
}

enum RelocationRunState {
    Active,
    Compacting,
    Retired,
}
```

The initial implementation may publish one small descriptor row per run in RocksDB. That is still
`O(R)`, not `O(N)`. A later version can use manifest edit files and publish only a singleton root or
generation number.

The visibility commit must atomically include:

```text
relocation-run descriptors/root
output segment states
source GcRelocating states
next_lsn
unaccounted relocation-run range markers
pending rollover state, when applicable
```

A reader must never observe forwarding to an output segment that the same metadata snapshot does not
recognize as readable.

## GC Publication Protocol

### Phase 1: payload prepublication

As today:

1. Copy selected source records into staging output files.
2. Seal and checksum output files.
3. Rename them to final paths.
4. Sync files and parent directories.
5. Publish protective pending-output segment rows.

No relocation mapping is visible yet.

### Phase 2: reconciliation and run preparation

1. Acquire the accounting publication lock.
2. Reconcile the GC snapshot against newly materialized segment changes.
3. Remove known retired, expired, or obsolete-shard copies.
4. Translate staging refs to final output segment IDs.
5. Sort surviving entries by relocation identity.
6. Write and sync bounded relocation runs with relative ordinals.

The first implementation may hold the accounting lock during run preparation. This delays
accounting but does not occupy the foreground writer queue. It prevents accounting from
materializing a terminal event between reconciliation and forwarding publication without seeing the
new relocation.

A later optimization can prepare a superset run before taking the lock and publish a compact
activation/exclusion structure after reconciliation. That optimization is worthwhile only if
accounting-lock duration becomes a problem.

### Phase 3: serialized publish

The writer:

1. Revalidates source segment and shard-generation eligibility that is ordered against writer
   commands.
2. Reads `next_lsn` as the first run's `base_lsn`.
3. Assigns adjacent LSN ranges to all prepared runs.
4. Builds one small atomic metadata batch containing run descriptors, output/source states, range
   markers, and the new `next_lsn`.
5. Appends accounting-log references to the prepared runs.
6. Commits the metadata batch.

Foreground user writes that were already ahead of the GC command receive lower LSNs. User writes
behind it receive higher LSNs. Exact relocation identity makes a mapping a no-op for a newer or
different payload version.

### Phase 4: post-commit

1. Make the new relocation-manifest generation visible to in-memory lookups.
2. Make active run blocks available through the relocation cache.
3. Wake accounting ingestion.
4. Release the accounting publication lock.

## Accounting Log and Frontier Changes

### Run-reference log entry

Replace the record-sized `GcMapRefBatch` frame with:

```rust
struct GcMapRefRun {
    run_id: RelocationRunId,
    base_lsn: StrataLsn,
    entry_count: u32,
    checksum: [u8; 32],
}
```

Accounting ingestion opens the named immutable file, validates its checksum, derives each entry's
LSN from its ordinal, and routes the entry to the appropriate blob-accounting partition.

The active log record remains small even for a large GC copy. The relocation file is now shared
durable input rather than duplicated bytes.

### Range-based unaccounted tracking

Replace per-entry `unaccounted_lsn_ops` rows for relocation runs with:

```rust
struct UnaccountedRelocationRun {
    run_id: RelocationRunId,
    base_lsn: StrataLsn,
    entry_count: u32,
    accounting_partitions: PartitionBitmap,
}
```

The interval is:

```text
[base_lsn, base_lsn + entry_count)
```

Because no unrelated operation can appear inside this interval, `accounted_lsn` can jump across it
when every partition named by the descriptor has materialized its entries through the relevant
ordinal/LSN. Until then, the entire unresolved suffix remains conservatively unaccounted.

If partition materialization can make only part of a run globally contiguous, the descriptor may
store per-partition ordinal ranges. The design should avoid expanding back to one durable row per
MapRef.

## Foreground Read Path

### Logical resolution first

The read path first resolves logical visibility exactly as today:

```text
blob head
tombstone state
lifecycle and current epoch
shard generation
```

If the logical blob is missing, tombstoned, expired, or belongs to an obsolete shard, relocation
metadata must not make it visible.

Logical resolution returns:

```rust
struct ResolvedBlobVersion {
    head_lsn: StrataLsn,
    payload_lsn: StrataLsn,
    record_ref: RecordRef,
    generation: Generation,
    lifecycle: Option<BlobLifecycle>,
}
```

### Physical translation

After logical resolution:

```text
translate(key, shard, payload_lsn, record_ref) -> final RecordRef
```

Translation repeatedly looks up the exact relocation identity until no mapping remains:

```text
A -> B
B -> C
result: C
```

It validates the stored key and detects cycles. A hard maximum chain depth protects reads from
corrupt or unexpectedly uncompact state.

### Fast path

The relocation LSM should not add disk I/O to every normal read.

One conservative policy is:

```text
source segment readable and not deleted:
  read original source ref

source segment deleted or source read races deletion:
  translate through relocation index
  read destination
```

Reading the old source remains correct because relocation does not change payload contents. Once the
source is deleted, durable forwarding is mandatory. A more eager policy may translate
`GcRelocating` sources immediately to warm the cache and reduce deletion races.

### Cache

Use a source-segment-oriented cache:

```rust
struct RelocationCache {
    by_source_segment: HashMap<SegmentId, Arc<CachedSegmentRelocations>>,
}
```

Entries for deleted source segments are higher priority because reads cannot fall back to source
bytes. Cache misses use run Bloom filters and sparse source indexes to avoid unrelated blocks.

## Conditional MapRef Semantics

The relocation LSM is authoritative for forwarding, but not every published entry necessarily
changes logical state.

Examples:

### Concurrent overwrite before MapRef

```text
put@10 -> source A
GC copies A
put@20 -> source D
MapRef@30 A -> B
```

The current logical payload is `D`; lookup identity does not match `A/payload_lsn=10`, so the
relocation is not applied to the foreground result. Blob accounting eventually marks copied `B` as
retired garbage.

### Metadata-only update

```text
payload put@10 -> A
lifecycle update@20
MapRef@30 payload_lsn=10, A -> B
```

The blob head may be newer than 10, but the protected payload identity is still 10/A. Translation
must apply.

### Reused source ref

```text
put@10 -> A
put@20 -> A
MapRef@30 payload_lsn=10, A -> B
```

The mapping must not rewrite the payload identity from put@20. This is why `payload_lsn` is part of
the relocation key.

## Blob-Accounting Integration

Accounting consumes relocation entries in derived LSN order and applies the existing MapRef reducer:

```text
if current materialized payload matches from:
  replace with to
  emit Mapped(from, to, lifecycle)
else if source may appear from older state:
  retain pending map
else:
  mapping is obsolete for logical state
```

Only a resolved `Mapped` transition retires the source and adds the destination to the
segment-accounting LSM. The relocation run alone is insufficient proof of liveness because an
unaccounted overwrite or tombstone may precede its publish LSN.

The segment-accounting projection may consult active relocation runs when processing an earlier
terminal event. This replaces the temporary `gc_relocations` RocksDB table used today to mark copied
destinations retired when their source became terminal before MapRef materialization.

## Source Deletion Rules

A source segment may be physically deleted only when every protected source ref satisfies one of:

```text
logical version is dead/expired
or exact source identity has durable relocation forwarding
or foreground blob state has been durably healed to a readable destination
```

In addition:

- The relocation manifest generation must be loadable before deletion commits.
- Every referenced destination segment must be readable and durable.
- Accounting must have materialized enough state to prove source liveness is zero.
- A relocation run cannot be removed merely because its source segment was deleted; that is when
  forwarding may be most important.

Source segment state and relocation visibility must change atomically. A reader observing `Deleted`
must either see the corresponding forwarding run in the same metadata generation or retry against a
newer generation.

## Relocation Compaction

### Objectives

- Bound lookup run count.
- Collapse forwarding chains.
- Remove obsolete and healed mappings.
- Group mappings by source segment.
- Preserve rollbackable and accounting-consumer history.

### Safe compaction frontier

Relocation mappings newer than the durable frontier remain rollbackable and must preserve their
individual publish metadata. Runs still referenced by the active accounting log or unaccounted
range markers cannot be removed.

A conservative compaction frontier is:

```text
min(published_lsn, accounted_lsn, minimum_pinned_consumer_lsn)
```

Chain collapsing may occur for read lookup before full retirement, but original run files remain
pinned until recovery and accounting no longer require their exact entries.

### Chain collapse

For one exact logical payload identity:

```text
A -> B @ 100
B -> C @ 200
C -> D @ 300
```

The read-optimized base can materialize:

```text
A -> D
B -> D
C -> D
```

It must retain every source that a foreground blob head may still name. Removing `A -> D` merely
because `A` was deleted would break an unhealed blob row that still points to A.

Conflicting active mappings from the same exact source identity to different destinations are an
invariant violation, not a last-write-wins case to hide during compaction.

### Retiring mappings

A mapping can be dropped when the system durably proves that no readable logical version can resolve
to its source identity. Proof may come from:

- A later overwrite, tombstone, expiry, or shard drop materialized by blob accounting.
- A completed healer update plus a compact-safe blob-version frontier.
- Rebuild/audit showing the source identity is unreachable.

The initial implementation should retain uncertain mappings rather than risk an unreadable live
blob.

## Healing Policy

The relocation LSM can remain permanently authoritative, or mappings can be healed into foreground
blob heads gradually.

### Permanent forwarding

Advantages:

- No eventual per-key MapRef writes.
- Physical movement stays completely separate from logical blob values.
- GC publication and later compaction remain sequential.

Costs:

- Long-lived forwarding metadata.
- Relocation lookup remains part of reads for deleted sources.
- Chain compaction and reachability-based retirement become mandatory.

### Bounded background healing

A low-priority healer:

1. Selects a bounded number of active mappings.
2. Revalidates exact blob identity.
3. Writes the final physical ref into foreground blob state.
4. Records durable healing progress.
5. Retires forwarding only after the blob compact-safe frontier proves the old source cannot
   reappear.

Healing moves work out of the GC publish tail but does not eliminate it. It should yield whenever
foreground queue or sync latency rises.

### Initial recommendation

Make the relocation LSM sufficient for correctness without healing. Implement healing later as a
space/read optimization. This avoids making reclamation depend on healer progress while the new
publication path is being validated.

## Crash Consistency

### Crash before relocation-run fsync

No descriptor is published. The incomplete run and any unused pending output are orphan cleanup
candidates.

### Crash after run fsync but before descriptor publication

The run is a durable orphan and has no effect on reads or accounting. Recovery removes it after
confirming no manifest references it.

### Crash after descriptor publication but before durability frontier

The run is visible but rollbackable by its LSN interval. Recovery validates:

- Run file length and checksum.
- Destination segment state and durable offsets.
- Source fencing state.
- Active-log run reference, when durable.

If the publish tail did not survive, recovery removes the run descriptor, range marker, and
associated hidden metadata, then restores source eligibility as current recovery does for hidden
per-record relocations.

### Crash after durable frontier

The run and destination bytes must survive. Missing or corrupt data is a hard recovery error or an
explicit repair operation; it cannot be treated as an absent mapping if a source segment may already
be deleted.

### Crash during relocation compaction

The old manifest remains authoritative until replacement runs are synced and the new manifest root
is published. Orphan output is discarded. Old runs are deleted only after reader and accounting
pins release them.

## Recovery Order

Before serving foreground reads:

1. Open the main metadata index.
2. Recover or roll back the unpublished LSN tail.
3. Load and validate the current relocation manifest.
4. Validate every active run's destination segment dependencies.
5. Restore source `GcRelocating`/`Deleted` consistency.
6. Initialize the relocation cache metadata.
7. Open blob and segment accounting consumers.
8. Enable reads and background workers.

Serving reads before the forwarding view is available is unsafe when a blob row may name a deleted
source segment.

## Interaction with the Segment-Accounting LSM

The two indexes form a staged materialization pipeline:

```text
prepared relocation run
  |-- published immediately in relocation manifest
  |     -> foreground physical translation
  |
  `-- referenced by accounting log
        -> blob-accounting conditional MapRef fold
        -> Mapped or terminal destination event
        -> segment-accounting delta run
        -> source/destination summaries and overlays
```

One physical relocation file may have several independent pins:

- Current relocation manifest.
- Active accounting-log/run consumer.
- Blob-accounting prepared compaction.
- Recovery/rollback horizon.
- Relocation compaction input.

It can be deleted only after all consumers release it. Sharing the immutable file avoids writing the
same `O(N)` map list once for reads and again for accounting.

## Scheduling and Backpressure

Moving MapRefs out of RocksDB does not remove file I/O. It moves record-proportional work outside the
serialized writer and makes it schedulable.

Use explicit budgets for:

```text
relocation run preparation bytes/sec
relocation compaction bytes/sec
maximum prepared-but-unpublished bytes
maximum active relocation runs
maximum retained unaccounted run bytes
healing records and bytes per interval
```

Run preparation should respond to foreground sync latency and device pressure. If free space is
critical, relocation publication may receive priority over optional accounting or healing work
because completed relocation is required to reclaim source bytes.

## Observability

Export:

- Relocation entries and encoded bytes prepared per GC publish.
- Run count and bytes per publish.
- Time spent reconciling, preparing, syncing, waiting in the writer queue, and committing metadata.
- RocksDB operations and encoded bytes per GC publish.
- Active, compacting, retired, orphan, and pinned run counts/bytes.
- Lookup cache hit rate and lookup latency.
- Reads served from original source versus translated destination.
- Relocation chain depth distribution and maximum.
- Accounting consumer lag by run and LSN.
- Unaccounted range count and oldest age.
- Healing throughput and backlog.
- Foreground p50/p99/p999/p9999 write and sync latency correlated with GC publication.

## Migration Plan

### Phase 0: current-path instrumentation

Measure:

```text
GC publish records
writer critical-section duration
blob-version MapRef operand bytes
gc_relocations rows/bytes
unaccounted rows/bytes
active-log frame bytes and append latency
RocksDB batch operations/bytes
foreground queue and sync latency
```

### Phase 1: shadow relocation runs

Prepare and publish relocation runs while continuing all current per-record RocksDB writes. Compare
LSM translation against `blob_versions.resolve_head()` after MapRef merges.

Runs are not yet required for reads or deletion. Mismatch disables GC deletion and records exact
identity, LSN, and run provenance.

### Phase 2: read-path validation

Resolve logical blob state from the current index, independently translate through the relocation
LSM, and compare final refs. Continue reading through the current resolved MapRef during this phase.

### Phase 3: relocation LSM becomes read-authoritative

Stop depending on immediate MapRef application for foreground reads, but continue shadow MapRef
merges for rollback. Require relocation recovery before serving reads.

### Phase 4: remove per-record temporary rows

Replace `gc_relocations` with relocation-run lookup and replace per-LSN unaccounted rows with range
markers. Change the accounting log to run references.

### Phase 5: stop immediate blob-version MapRef merges

After mixed-version and rollback requirements are satisfied, stop publishing per-key MapRefs into
`blob_versions`. Retain an optional bounded healer rather than synchronous GC writeback.

## Validation Plan

### Semantic equivalence

Generate ordered combinations of:

```text
put
overwrite
tombstone
lifecycle extension
expiration
shard drop/re-add
MapRef
multiple relocation hops
rollback
```

Compare the current packed blob-version MapRef fold with logical resolution followed by relocation
LSM translation.

### Publication races

Exercise user writes:

- Before GC reconciliation.
- After reconciliation but before writer enqueue.
- Ahead of GC in the writer queue.
- Behind GC in the writer queue.
- While accounting is behind.
- While relocation compaction is running.

The exact payload identity must produce the same final readable value as the current implementation.

### Crash injection

Inject failures before and after:

- Output segment fsync.
- Relocation run fsync.
- Active-log run-reference append.
- Descriptor/root publication.
- `next_lsn` publication.
- Durable-frontier advancement.
- Relocation base compaction publication.
- Obsolete-run deletion.

### Performance evaluation

Compare:

```text
current one-large-batch MapRef publish
bounded current batch chunking
single relocation manifest without LSM compaction
relocation LSM
relocation LSM plus segment-accounting LSM
```

Vary:

```text
records per GC copy
record size
source segment count
foreground write concurrency
overwrite/tombstone rate during GC
accounting lag
relocation chain depth
fraction of reads whose source was deleted
device type and free-space pressure
```

Primary outcomes are foreground p99.9/p99.99 latency, writer critical-section time, metadata write
amplification, device write amplification, GC throughput, space amplification, and relocated-read
latency.

## Alternatives Considered

### Chunk the current per-record batch

This is simpler and improves fairness, but retains all record-proportional RocksDB and blob-version
work. It is a useful baseline and fallback.

### One relocation manifest per GC job without compaction

This removes immediate per-key writes but eventually accumulates manifests and lookup sources. It is
an appropriate first implementation stage but needs LSM-style indexing and compaction for sustained
operation.

### Dedicated RocksDB column family

A separate column family isolates memtables and SSTs but still shares the main RocksDB WAL, write
queue, and atomic batch. It also keeps record-proportional insertion in the serialized publish. A
dedicated database improves isolation but creates cross-database atomicity and additional sync
problems.

### Synchronous healing on reads

This follows a known lazy-handle-update pattern but turns read traffic into foreground write traffic
and creates latency outliers on cache misses. Reads may enqueue asynchronous healing hints but should
not block on metadata writeback.

### Put physical refs only in the relocation LSM from initial ingest

This would make the foreground blob index purely logical and use relocation lookup for every payload
read. It is a larger redesign with higher common-case read cost and is not required to remove GC
publication bursts.

## Related Work and Novelty Boundary

This proposal does not claim that multiple LSM trees, key-value separation, or lazy pointer updates
are individually novel.

- [RocksDB column families](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) provide
  separate keyspaces/LSM structures with atomic cross-family batches.
- [WiscKey](https://www.usenix.org/sites/default/files/fast16_full_proceedings_interior.pdf) and
  [HashKV](https://www.usenix.org/system/files/conference/atc18/atc18-chan.pdf) establish key-value
  separation and value-log GC as a major design space.
- [NovKV](https://msstconference.org/MSST-history/2020/Papers/15.NovKV.pdf) passes liveness
  information discovered during key-store compaction into searchable value files and delays value
  handle updates, closely overlapping relocation indirection and lazy healing.
- [SILK](https://www.usenix.org/system/files/atc19-balmau.pdf) and
  [AegonKV](https://www.usenix.org/conference/fast25/presentation/duan) establish background I/O and
  GC interference as tail-latency problems.

The potentially differentiating system contribution is the combined semantic decomposition:

```text
logical blob history LSM
immediately visible exact-version relocation LSM
asynchronously derived segment-liveness LSM
```

All three consume or derive from one globally ordered history but use different keys, visibility
frontiers, retention rules, and compaction policies. Relative-LSN immutable relocation runs allow
record-proportional work to occur before the serialized commit, while a small atomic publication
connects them to the main commit domain. Snapshot-pinned run history provides GC reconciliation
without returning every physical transition to the foreground metadata LSM.

Whether this combination is a publishable contribution depends on demonstrated improvements and a
careful comparison with NovKV-style lazy handle updating, not on the number of LSM trees alone.

## Open Questions

1. Should the durable manifest use one RocksDB descriptor row per run, a singleton manifest value,
   or append-only manifest edits plus periodic checkpoints?
2. Can reconciliation safely move entirely outside the writer while retaining the accounting pause
   invariant?
3. Is holding the accounting lock during relocation-run fsync acceptable, or is a prepared superset
   plus activation bitmap necessary?
4. What run size gives the best writer latency, lookup amplification, and compaction cost?
5. Should entry ordering be source-physical order or accounting-partition order?
6. What exact per-partition progress metadata lets `accounted_lsn` jump a run interval safely?
7. Should the foreground read path translate `GcRelocating` sources eagerly or only after deletion
   or read failure?
8. What memory budget and admission policy should the source-segment relocation cache use?
9. Are permanent forwarding and chain collapse sufficient, or is bounded healing required for
   production space control?
10. How does relocation compaction prove that an original source identity is no longer reachable by
    any foreground blob head?
11. Can relocation and segment-accounting runs share a common file format and block cache without
    coupling their visibility frontiers?
12. How should mixed-version nodes behave when some expect MapRefs inside `blob_versions` and others
    expect relocation-run translation?
13. What maximum chain depth should fail closed versus trigger synchronous metadata repair?
14. Should skipped GC output ranges be recorded in the segment-accounting LSM during the same
    publication or only when accounting consumes the run?

## Recommended Initial Scope

1. Implement immutable source-sorted relocation runs with relative ordinals.
2. Publish one small descriptor per run atomically with segment states and LSN range assignment.
3. Keep current per-record MapRef and relocation rows as shadow truth.
4. Add read-path translation in comparison-only mode.
5. Add run-reference accounting-log entries and range-based unaccounted tracking.
6. Validate conditional MapRef and recovery equivalence.
7. Make the relocation LSM read-authoritative.
8. Stop per-record temporary relocation rows.
9. Stop immediate blob-version MapRef merges only after mixed-version rollback is proven.
10. Add relocation compaction and optional healing after publication-tail benefits are measured.

This sequence tests the central hypothesis—moving `O(N)` physical relocation metadata out of the
serialized writer—before committing to permanent forwarding or complex reachability-based cleanup.
# Historical MapRef LSM Proposal

> This proposal predates the implemented blob and relocation LSMs. References to the separate
> accounting engine are historical. See [`lsm_gc.md`](lsm_gc.md).
