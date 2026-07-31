# Segment Relocation Manifest Design Note

## Context

Strata currently moves live records during GC by copying bytes into new sealed output segments and publishing one `MapRef` per copied record. Each `MapRef` rewrites the logical payload reference from:

```rust
from: RecordRef { segment_id, offset, len }
to:   RecordRef { segment_id, offset, len }
```

The current design is correct and rollbackable because each relocation names the exact payload LSN and source record ref. The weakness is publish-time write pressure: a large GC copy can build and commit one large metadata-index batch containing many `MapRef`s, unaccounted-LSN rows, and relocation rows. The existing code already calls this out as a TODO around chunking the publish path.

The core design problem is that GC moves bytes, but blob-version metadata must preserve ordered version semantics. Writing every moved value pointer back into blob-version metadata immediately can interfere with foreground writes. A segment-level indirection layer can avoid that immediate per-key write-back cost by publishing relocation metadata first and healing blob-version rows later.

## File-Level Indirection Lesson

A storage engine can avoid immediate per-key pointer updates by storing an indirect file or segment identifier in the key index and publishing dependency metadata when GC rewrites live values.

On read:

```text
key -> old file or segment identifier from blob-version metadata
old file or segment identifier -> latest file or segment through relocation metadata
latest file or segment -> value bytes
```

This means value GC does not need to synchronously write one updated pointer per key back into blob-version metadata. Later healing or compaction can collapse the indirection by writing newer physical refs into the canonical blob-version rows.

The tradeoff is read-side indirection and dependency management. If relocation chains become complex, the system needs a rebuild or healing path to simplify them.

## Proposed Strata Direction

Use a durable segment relocation manifest as a fast-publish indirection layer, and keep per-key `MapRef`s as the eventual canonical state.

The high-level split:

```text
Segment relocation manifest = fast GC publish and read correctness
MapRef healing = eventual compaction of indirection
```

Instead of publishing every copied record as an immediate blob-version `MapRef`, GC publish can write a small number of durable relocation-run rows:

```text
segment_relocation/{source_segment_id, run_id} -> RelocationRun
```

Each relocation run maps source record ranges to destination record refs:

```rust
struct RelocationRun {
    source_segment_id: SegmentId,
    run_id: u64,
    publish_lsn: StrataLsn,
    output_segments: Vec<SegmentId>,
    records: Vec<RelocatedRecord>,
    state: RelocationRunState,
}

struct RelocatedRecord {
    source_offset: u64,
    source_len: u64,
    payload_lsn: StrataLsn,
    key: BlobKey,
    shard: ShardKey,
    destination: RecordRef,
}

enum RelocationRunState {
    Active,
    Healing,
    Retired,
}
```

The `records` vector should be sorted by `source_offset` so read-side lookup can binary search:

```text
(source_segment_id, source_offset, source_len) -> Option<RecordRef>
```

## GC Publish Flow

Current flow:

```text
copy live records
prepublish output segment files
publish all per-record MapRefs through the writer
accounting later retires source refs and adds destination refs
```

Proposed fast path:

```text
copy live records
prepublish output segment files
publish relocation run metadata through the writer
mark source segment relocated / protected from unsafe deletion
return quickly
```

This reduces writer-queue occupancy from "one row per moved record" to "one relocation run per source segment or chunk".

The publish still needs the serialized writer path for ordering:

- assign a publish LSN or LSN range
- commit output segment states
- commit relocation run metadata
- commit any accounting metadata needed to make the run visible
- preserve rollback and crash recovery semantics

## Read Path

The read path remains blob-index first:

```text
resolve blob key -> RecordRef
if RecordRef source segment has active relocation:
    translate source ref through relocation cache
read translated destination ref
otherwise:
    read original ref
```

Read translation must not require a metadata-index lookup on every relocated read. Add an in-memory cache:

```rust
struct SegmentRelocationCache {
    by_segment: HashMap<SegmentId, Arc<CachedRelocationRuns>>,
}

struct CachedRelocationRuns {
    runs: Vec<CachedRelocationRun>,
}

struct CachedRelocationRun {
    publish_lsn: StrataLsn,
    records_by_offset: Vec<CachedRelocatedRecord>,
}

struct CachedRelocatedRecord {
    source_offset: u64,
    source_len: u64,
    payload_lsn: StrataLsn,
    destination: RecordRef,
}
```

Lookup:

```rust
fn translate(record_ref: RecordRef, payload_lsn: StrataLsn) -> Option<RecordRef> {
    let runs = cache.by_segment.get(&record_ref.segment_id)?;
    runs.find(record_ref.offset, record_ref.len, payload_lsn)
}
```

The `payload_lsn` check is important. It prevents a relocation of one physical version from accidentally moving a newer logical version that happens to reuse the same physical ref.

## Lazy Healing

Relocation manifests should not be permanent in the common case. A background healer gradually writes ordinary per-key `MapRef`s back into `blob_versions`.

The healer:

```text
scan active relocation runs
select a bounded batch of records
revalidate each record against current blob state
publish normal MapRefs through the serialized writer path
advance healing progress
retire relocation run when fully healed and accounted
```

Foreground reads may enqueue healing hints:

```text
read sees relocated ref
enqueue heal(key, payload_lsn, from, to)
continue serving read from translated destination
```

But foreground reads should not synchronously write `MapRef`s. Synchronous read-side writeback would shift the throughput problem from GC publish to user reads.

## Pressure-Aware Scheduling

The healer should run only when foreground write pressure is low.

Useful signals already exist or are adjacent to existing GC tuning:

- write queue send latency
- foreground sync latency
- seal backlog pressure
- current GC worker/concurrency pressure
- `published_lsn - accounted_lsn`
- metadata-index write latency for recent small metadata batches

Scheduling policy:

```text
if write pressure is high:
    do not heal
else:
    publish a small MapRef batch
```

Healing batch limits should be explicit:

```rust
struct RelocationHealerConfig {
    max_records_per_batch: usize,
    max_bytes_per_batch: u64,
    max_batches_per_interval: usize,
    max_accounting_lag_lsn: Option<StrataLsn>,
}
```

This is stronger than simple GC publish chunking. Chunking makes the current per-record `MapRef` publish fairer. Relocation manifests reduce the amount of immediate per-key metadata work in the first place.

## Accounting Model

Accounting must continue to produce ordered segment-local truth for GC overlays.

There are two possible accounting models:

### Option A: Manifest Is Read-Path Only, MapRefs Are Accounting Truth

The relocation manifest serves reads immediately, but accounting does not retire source refs or add destination refs until the healer publishes per-key `MapRef`s.

Pros:

- preserves the existing accounting model
- less new accounting machinery
- `MapRef` remains the canonical relocation event

Cons:

- source segments remain protected until healed
- GC planning may see source bytes as live longer than necessary
- space reclamation depends on healer progress

### Option B: Manifest Is Also Accounting Truth

Publishing a relocation run creates a bulk accounting event. Accounting retires source ranges and adds destination ranges from the relocation run without waiting for per-key healing.

Pros:

- source segment reclaim can proceed sooner
- segment GC summaries reflect relocation promptly

Cons:

- accounting must consume and reconcile relocation runs
- recovery/rollback has more state to repair
- reads may still point at old refs, so relocation manifests become long-lived correctness metadata until healing completes

Initial recommendation: start with Option A unless space reclamation delay is unacceptable. It is simpler and keeps the current `MapRef` machinery as the canonical source of physical relocation truth.

## Deletion And Lifetime Rules

Relocation manifests change deletion safety.

A source segment file may become physically deletable only when:

```text
all old source refs are either:
  healed into blob_versions as MapRefs, or
  logically dead/tombstoned/expired, or
  covered by durable relocation metadata that remains available to reads
```

If the source file is deleted while blob-version rows still point at it, then the relocation manifest is mandatory for correctness. Therefore:

- source segment deletion must check active relocation-run state
- relocation-run metadata cannot be deleted until all blob refs are healed or dead
- reader cache eviction must consider relocated/deleted source states
- recovery must rebuild or reload relocation cache before serving reads

Lifetime extensions and tombstones still apply at the blob-version layer. A relocation manifest only translates physical refs. It must not revive expired or tombstoned logical blobs.

## Recovery Model

On open:

```text
load active relocation runs
validate output segment states exist and are readable
populate SegmentRelocationCache
rollback unaccounted relocation runs whose output bytes did not survive
resume healer from durable progress
```

Crash cases:

- Output files exist but relocation run did not commit: treat output files as orphan/pending GC output and delete or recover according to existing policy.
- Relocation run committed but cache is empty after restart: reload cache before reads.
- Relocation run committed but output segment missing/truncated: rollback the relocation run or fail open, matching the current `MapRef` survival check philosophy.
- Healer partially published MapRefs: rely on LSN ordering and existing rollbackable `MapRef` behavior.

## Correctness Invariants

1. A relocation lookup must match `segment_id`, `offset`, `len`, and `payload_lsn`.
2. A relocation manifest must never make a tombstoned or expired blob visible.
3. Lazy writeback must publish `MapRef { payload_lsn, from, to }`; it must not blindly overwrite the current blob head.
4. Reads must work even if lazy writeback never runs.
5. Source segment deletion must not break reads whose blob-version rows still point at source refs.
6. Healing must go through the serialized writer path so publish LSNs compose with user puts, deletes, tombstones, and lifetime updates.
7. Relocation-run metadata must be crash recoverable before any source bytes it protects are deleted.

## Performance Expectations

Expected improvements:

- much shorter GC publish critical section
- fewer immediate metadata-index merge operands
- less writer-queue head-of-line blocking during large GC moves
- read path remains direct for non-relocated segments
- relocated read overhead is bounded by an in-memory segment relocation lookup

Expected costs:

- read-side indirection until healing completes
- memory for relocation cache
- more complicated segment deletion rules
- new recovery state
- possible accumulation of relocation runs if write pressure stays high and healing cannot make progress

## Comparison To Existing Strata TODO

The current TODO says to publish large GC copies in bounded chunks because one large batch can block foreground writes.

Chunking only changes:

```text
one huge MapRef batch -> many smaller MapRef batches
```

The relocation-manifest design changes:

```text
immediate per-record MapRefs -> segment-level relocation metadata now, per-record MapRefs later
```

These approaches are compatible. Even with relocation manifests, the healer should publish `MapRef`s in bounded chunks.

## Open Questions

1. Should relocation runs be one per source segment, one per output segment, or fixed-size chunks?
2. Should accounting treat relocation manifests as canonical immediately, or wait for healed `MapRef`s?
3. What is the cache memory budget and eviction policy for relocation runs?
4. If a cached relocation is evicted, should reads synchronously reload it from durable metadata or fail closed?
5. When can a source segment file be deleted if unhealed blob-version rows still point at it?
6. Should relocation manifests support chaining, or should GC refuse to relocate already-relocated destination records until healing catches up?
7. What pressure signals should gate the healer, and what thresholds prevent starvation?
8. Should foreground reads enqueue heal hints, or should the healer scan manifests independently?

## Local Code Pointer

Current Strata GC publish TODO: `crates/strata-store/src/lib.rs`, `submit_gc_publish`.
# Historical Segment Relocation Manifest Proposal

> This proposal predates the implemented relocation LSM. References to the separate accounting
> engine are historical. See [`lsm_gc.md`](lsm_gc.md).
