# Segment Accounting LSM Design

Status: proposed

## Summary

Move segment GC summaries, record-range overlays, and the change history needed by concurrent GC
out of RocksDB and into a file-backed, segment-oriented LSM.

Accounting compaction already discovers the logical transitions that change physical segment
liveness. Instead of projecting those transitions into one RocksDB row per ref event plus one
RocksDB merge operand per touched segment, it should write immutable segment-accounting runs. After
the run files are synced, accounting should atomically publish a small manifest/root update together
with the accounting frontier.

The proposed ownership model is:

```text
blob-accounting LSM     authoritative logical materialized state by blob key
segment-accounting LSM  authoritative GC view by physical segment and record range
segment files           authoritative payload bytes
RocksDB                  durable commit roots, frontiers, and foreground metadata
```

The segment-accounting LSM is authoritative for GC decisions but remains derived and rebuildable.
GC must fail closed if this index is unavailable or corrupt; it must not infer that unindexed bytes
are garbage.

The expected change in the metadata path is:

```text
current:
  accounting compaction
    -> N segment_ref_event RocksDB puts
    -> S segment_gc_overlay RocksDB merge operands containing O(N) range changes
    -> synced RocksDB batch

proposed:
  accounting compaction
    -> sequential immutable segment-accounting run write
    -> fsync run files
    -> small synced RocksDB manifest/frontier publication
```

Here, `N` is the number of materialized physical transitions and `S` is the number of touched
segments.

## Context

Foreground blob operations append ordered entries to the active accounting log. The background
accounting worker ingests those entries into the blob-oriented accounting index, compacts update
history, and emits logical ref transitions such as:

```text
Live
Retired
LifecycleChanged
Mapped
```

The store currently projects those transitions back into the RocksDB-backed store index:

```text
segment_ref_events[(segment_id, lsn, offset)] -> SegmentRefEvent
segment_gc_overlay[segment_id] MERGE Vec<SegmentGcOverlayMergeOp>
```

The overlay contains both a compact planning summary and record-level state:

```text
SegmentGcOverlay {
    summary,
    expired ranges,
    retired ranges,
    lifetime ranges,
}
```

The two RocksDB representations serve different purposes:

- The overlay is the latest materialized per-segment view used for planning and copy selection.
- Ref events are ordered changes retained so a GC job can reconcile mutations that occur after the
  job captures its accounting frontier.

This is correct, but it places a large derived-state workload back into the same general-purpose
metadata engine that serves foreground operations.

## Problem Statement

### RocksDB write contention

One accounting compaction publication inserts one RocksDB row for every projected ref event. It also
adds one merge operand per touched segment, but the contents of those operands still grow with the
number of affected record ranges.

The publication is a synced RocksDB batch because the following state must move atomically:

```text
accounting manifest
accounting consumed cursor, when applicable
segment ref events
segment overlay operands
accounted_lsn
relocation and shard-cleanup state, when applicable
```

When Strata shares a RocksDB instance with application metadata, the batch shares the WAL, write
queue, memtables, flush scheduling, and compaction resources with foreground traffic. With a
dedicated RocksDB instance, it still competes with foreground Strata metadata and payload I/O on the
same device.

Batching reduces the number of write calls and fsyncs, but it makes publication bursty. The likely
first symptom is elevated foreground p99/p999 write or sync latency during accounting catch-up,
forced materialization, epoch changes, or periodic full maintenance.

### RocksDB and merge-operator write amplification

Overlay merge operands avoid a foreground read-modify-write, but they do not remove the work. Every
operand is written to the WAL and memtable, flushed into an SST, and eventually folded into a
materialized overlay during RocksDB compaction or a full merge.

The current partial merge only concatenates operation lists because it cannot safely resolve
retire, expire, and lifetime conflicts without the existing value. It therefore reduces operand
count without semantically reducing record-level work. A full merge must deserialize the previous
overlay, apply all pending operations in order, normalize its range vectors, and serialize the full
value again.

### Summary reads resolve the full overlay

`SegmentGcSummary` is embedded in `SegmentGcOverlay`. Code that needs only a small summary therefore
reads the record-level overlay value as well.

Important examples include:

- GC snapshot construction, which reads a summary for every segment.
- Empty-segment deletion checks.
- Accounting metric-delta calculation for every touched segment before publication.
- Metric initialization and shard cleanup.

A RocksDB `Get` on a key with unresolved merge operands invokes the full merge path. Consequently,
planning cost grows with overlay value size and pending operand history even though the planner only
needs aggregate counters and histograms.

### Ref-event reconciliation scans retained history

GC captures an `accounted_lsn`, copies records optimistically, and then asks for every ref event
newer than that frontier before publishing the copy. The current implementation scans the retained
`segment_ref_events` column family, filters by LSN, and sorts the result. The segment-specific helper
also scans and filters instead of performing a bounded range read.

The reconciliation cost is therefore proportional to all retained event rows, not only to changes
for the selected source segments. Multiple GC workers can repeat this scan, and long-running GC
snapshot pins delay event pruning.

### Duplicate physical projections

The same accounting transition is represented in several forms:

```text
logical RefEvent in CompactionEventBatch
segment_ref_events RocksDB row
SegmentGcOverlayMergeOp inside a RocksDB merge operand
materialized SegmentGcOverlay state
```

Some duplication is required because GC needs both current state and changes since a snapshot.
However, RocksDB rows and merge values are not the only way to retain those two views. Immutable
versioned runs can provide both with less metadata-engine interference.

### Limited scheduling control

RocksDB decides when overlay operands are folded and SSTs are compacted. Strata can tune column
family options, but it cannot directly schedule segment-accounting work around foreground sync
latency, disk pressure, or GC I/O budgets. A Strata-owned file LSM makes that work explicit and
rate-limitable.

## Goals

- Remove record-proportional GC accounting rows and merge operands from RocksDB.
- Make GC planning read compact summaries without resolving record-level overlays.
- Make overlay reads segment-local and sequential.
- Make GC reconciliation proportional to recent changes for selected source segments.
- Preserve the current crash-safety and accounting-frontier invariants.
- Preserve an atomic relationship between blob-accounting materialization and its segment-oriented
  projection.
- Allow segment-accounting compaction to be paused, rate-limited, and scheduled independently.
- Retain a deterministic, auditable rebuild path.

## Non-goals

- This proposal does not change foreground blob-version semantics.
- It does not move payload bytes into the accounting LSM.
- It does not make unaccounted or unindexed bytes reclaimable.
- It does not require immediate removal of the existing RocksDB column families during rollout.
- It does not solve GC relocation publication or per-key `MapRef` write pressure by itself.
- It does not require a generic reusable LSM implementation before the first version can ship,
  although existing accounting-index run and manifest machinery should be reused where practical.

## Proposed Architecture

Introduce a `SegmentAccountingIndex`, physically stored under the existing accounting-index
namespace or a sibling directory:

```text
{namespace}/accounting-index/
  active-delta.log
  blob/
    ... existing blob-oriented runs ...
  segment/
    partition-00000/
      delta-00000000000000000001.run
      base-00000000000000000002.run
      changes-00000000000000000003.run
    partition-00001/
      ...
```

The exact directory names are not part of the format contract. The important properties are:

- Runs are immutable after creation.
- Every live run is named by a durable manifest.
- Runs are partitioned so compaction and reads can be bounded.
- Rows inside a run are ordered by physical segment and record offset.
- Run metadata includes its LSN interval and a sparse per-segment index.

### One accounting commit root

The blob-accounting and segment-accounting roots should be published as one accounting manifest
generation, not as independently visible manifests:

```text
AccountingManifest {
    generation,
    blob_partitions,
    segment_partitions,
    retained_segment_change_runs,
    materialization_frontiers,
    ...
}
```

This is the central correctness decision. Blob-accounting compaction must not become visible, drop
input history, or advance `accounted_lsn` without making the matching segment projection visible.

The manifest may continue to live as a small RocksDB value. RocksDB remains the atomic root of the
publication, but it no longer stores the record-proportional derived state.

## Logical Data Model

### Record state

The materialized overlay table is keyed by a physical range:

```rust
struct SegmentRecordKey {
    segment_id: SegmentId,
    offset: u64,
}

struct SegmentRecordState {
    len: u64,
    last_lsn: StrataLsn,
    state: SegmentRecordLiveness,
}

enum SegmentRecordLiveness {
    Live {
        lifecycle: Option<BlobLifecycle>,
    },
    Retired,
    Expired,
}
```

The first implementation may encode the existing overlay operations directly instead of
materializing this exact Rust shape. The semantic requirements are:

- Operations for one physical range are applied in commit order.
- Retired and expired ranges cannot be revived by a later stale lifetime update.
- A new physical allocation can initialize a range as live, retired, or expired.
- Mapping retires the source range and initializes the destination range atomically in the same
  accounting publication.
- Length is validated consistently for repeated references to the same segment and offset.

The physical key may include `len` if doing so makes corruption detection or range identity safer.
Offsets are unique within a segment under the current append-only segment format.

### Delta mutations

Accounting compaction writes ordered mutations:

```rust
struct SegmentMutation {
    lsn: StrataLsn,
    ordinal: u32,
    range: SegmentGcRecordRange,
    op: SegmentMutationOp,
}

enum SegmentMutationOp {
    AddLive { lifecycle: Option<BlobLifecycle> },
    AddRetired,
    AddExpired,
    Retire,
    Expire,
    SetLifetime { lifecycle: Option<BlobLifecycle> },
}
```

`ordinal` provides a deterministic tie-breaker if more than one physical transition for the same
range can share an LSN. If the accounting model proves that this is impossible, it may be omitted
from the persisted key while retaining a deterministic in-batch order.

Delta rows should be sorted by:

```text
(segment_partition, segment_id, offset, lsn, ordinal)
```

Every run also records `min_lsn` and `max_lsn`, allowing reconciliation to skip runs that cannot
contain changes after a GC snapshot.

### Segment summaries

Summaries are a separate logical table:

```text
segment_summary[segment_id] -> SegmentGcSummary
```

They must not be embedded in record-level overlay values. GC planning should be able to scan the
summary table without opening record-state blocks.

Accounting compaction already knows the old and new logical lifecycles involved in transitions. It
should produce exact signed summary deltas, including:

```text
total/live/retired/expired byte deltas
live and unknown-lifetime ref-count deltas
unknown-lifetime byte deltas
future-epoch histogram bucket deltas
extension-count histogram bucket deltas
```

`min_live_end_epoch` and `max_live_end_epoch` are derived from the non-empty future-epoch
histogram. Summary delta application must reject underflow and inconsistent range lengths rather
than relying on saturating arithmetic to hide projection bugs.

Summary storage can use a small base table plus ordered delta tables. Readers fold only summary
deltas, never record-range state. Segment-accounting compaction periodically materializes a new
summary base.

## Physical Run Types

The initial design needs three logical run roles. They may share one file format.

### Segment delta run

Produced directly from one accounting compaction publication. Contains ordered record mutations and
per-segment summary deltas. It is immutable and synced before manifest publication.

### Segment base run

Produced by segment-accounting compaction. Contains canonical current record state and materialized
segment summaries for its partition. Rows are sorted by `(segment_id, offset)` and should be block
indexed so one segment can be read without decoding unrelated segments.

### Retained change run

Preserves the ordered transitions needed by GC jobs whose captured `accounted_lsn` precedes those
transitions. Initially, a delta run can serve as both a state input and a retained change run.

After its mutations are folded into a base run, the input run may still need to remain referenced as
change history. It can be removed only when no active GC snapshot can request its LSN interval. A
later optimization can rewrite several retained inputs into a compact change-only run.

This retained history is the file-backed replacement for the `segment_ref_events` column family.

## Accounting Publication Protocol

The existing prepare-publish-apply pattern extends naturally.

### Prepare

For a blob-accounting compaction:

1. Produce the new blob-accounting run files.
2. Translate the compaction's logical ref transitions into a segment delta run.
3. Produce exact segment-summary deltas.
4. Sync every new run file.
5. Sync directory entries when new files or directories must survive a crash.
6. Build one candidate accounting manifest that references both blob and segment outputs.

Preparing the segment run must be deterministic from the compaction event batch. Retrying a prepare
may create a new orphan file name, but it must produce equivalent logical contents.

### Publish

Commit one small synced RocksDB batch containing:

```text
new combined accounting manifest/root
active-log consumed cursor, when applicable
accounted_lsn, when the contiguous frontier advances
unaccounted-LSN cleanup
shard-cleanup state transitions
other small commit-domain metadata
```

The batch no longer contains individual segment ref-event rows or segment overlay operands.

The manifest is the visibility boundary. A run file that exists on disk but is not named by the
published manifest is an orphan and must not affect reads.

### Apply

After RocksDB publication succeeds:

1. Install the published manifest in the in-memory blob and segment indexes.
2. Make the new generation available to readers.
3. Mark superseded runs as cleanup candidates.
4. Delete obsolete files only after all manifest and GC snapshot pins release them.

If in-memory apply fails after publication, the worker reopens both indexes from the durable
manifest. It must not attempt to roll back the already-published generation.

## Crash Consistency

The relevant crash points are:

### Crash before run fsync

The old manifest remains authoritative. Incomplete output files are unreferenced and removed during
recovery.

### Crash after run fsync but before manifest publication

The new files are durable orphans. Recovery removes files not reachable from the durable manifest.

### Crash after manifest publication but before in-memory apply

Recovery opens the newly published manifest and its already-synced files. The accounting frontier
and both logical indexes move together.

### Crash during segment-accounting compaction

The old base and delta runs remain authoritative until the replacement manifest is published.
Incomplete output is discarded as an orphan.

### Crash during obsolete-run deletion

Deletion is idempotent. A file named by the durable manifest must never be deleted. Missing
unreferenced files are harmless.

## Snapshot and Pinning Model

A GC accounting snapshot becomes:

```rust
struct SegmentAccountingSnapshot {
    manifest_generation: u64,
    accounted_lsn: StrataLsn,
    current_epoch: Epoch,
}
```

Snapshot acquisition must join the file-manifest root with RocksDB-owned segment state without
leaving a cleanup race:

1. Read the durable accounting manifest generation, `accounted_lsn`, current epoch, and segment
   states from a RocksDB snapshot.
2. Acquire a pin for that manifest generation while holding the same registry lock used by obsolete
   run cleanup.
3. If the generation is no longer pinnable, release the RocksDB snapshot and retry.
4. Release the RocksDB snapshot after constructing the plain planning view.

Publishing a newer manifest need not wait for readers. Only deletion of files reachable from a
pinned generation is blocked.

The guard pins:

- The manifest generation used for planning and overlay reads.
- Every base and delta run reachable from that generation.
- Change runs needed to answer `changes_since(accounted_lsn)`.

Compaction may publish a newer generation while the guard is alive, but cleanup cannot remove pinned
inputs. After process restart there are no surviving in-memory GC jobs, so only the durable current
manifest needs to be retained unless a future GC job protocol itself becomes restartable.

Pins should have observable age and count. A stuck GC job must not retain unbounded run history
silently.

## GC Read Paths

### Planning

GC captures a segment-accounting snapshot and sequentially scans its summary view:

```text
summary base
  + summary deltas visible in snapshot generation
  -> one SegmentGcSummary per segment
  -> GcSnapshot
```

Record-state blocks are not read during planning. This replaces one RocksDB point read and possible
full merge per segment with a compact sequential summary scan.

The existing accounting-coverage gate remains mandatory. A sealed source is not eligible for GC
when its `SegmentState.max_lsn` is newer than the snapshot's `accounted_lsn`. New foreground records
therefore cannot become reclaimable merely because their segment-accounting rows have not been
materialized yet.

### Copy selection

After the planner selects source segment `S`, GC reads only rows for `S`:

```text
base range for S
  + delta ranges for S
  -> ordered canonical overlay for S
```

The overlay reader merges iterators by `(offset, lsn, ordinal)`. It may materialize the canonical
overlay in memory or expose a streaming classifier. A streaming classifier is preferred because GC
already scans source records in offset order.

When the proposed per-segment immutable record index is available, copy selection becomes:

```text
scan S.idx entries in offset order
merge with segment-accounting overlay iterator for S
read only selected payload ranges from S.data
```

Without `S.idx`, GC continues scanning `S.data` and uses the same overlay iterator.

### Reconciliation before publish

Before publishing copied records, GC asks:

```text
changes_since(snapshot.accounted_lsn, copied_source_segment_ids)
```

The segment index:

1. Selects retained change runs with `max_lsn > snapshot.accounted_lsn`.
2. Uses each run's sparse segment index to skip segments not copied by this job.
3. Reads mutations for the selected source segments.
4. Filters by LSN and returns them in deterministic commit order.

This replaces a full scan of all retained RocksDB ref-event rows. Reconciliation work becomes
proportional to recent changes in runs that overlap the GC job's source segments.

Mapped destination changes and relocation forwarding must remain visible when they affect a copied
source. The exact filter contract should be tested against overwrite, tombstone, expiration,
lifecycle extension, shard drop, and relocation races before restricting reconciliation more
aggressively than source-segment scope.

## Segment-Accounting Compaction

Segment-accounting compaction is independent of blob-accounting compaction. Blob compaction creates
new segment delta runs; segment compaction folds them into bases.

Suggested triggers include:

```text
delta run count per partition
delta bytes per partition
summary-delta depth
retained change-run bytes
point/range read amplification
explicit maintenance request
```

Compaction should:

1. Merge the current base and selected delta runs in physical range order.
2. Apply mutations deterministically and reject invalid transitions.
3. Coalesce adjacent retired or expired ranges where safe.
4. Materialize exact per-segment summaries.
5. Write and sync replacement base and summary blocks.
6. Publish a new manifest generation.
7. Retain input change history until the GC snapshot floor permits deletion.

The compactor must have an explicit byte-rate budget and should react to the same foreground
pressure signals as GC:

```text
foreground sync latency
writer queue latency
payload and seal backlog
free-space pressure
recent segment-accounting read amplification
```

Moving work out of RocksDB does not eliminate physical disk contention. It makes the work visible,
sequential, and schedulable.

## Expiration Handling

The first implementation should preserve current semantics: accounting materializes explicit
`Expire` mutations when epochs advance, and those mutations update both record state and summaries.
This minimizes semantic change during migration.

A later optimization can avoid epoch-wide per-record writes by deriving expiration from
`lifecycle.logical_end_epoch <= snapshot.current_epoch`. The future-epoch histogram already makes
summary-level derivation possible. That optimization needs a separate correctness review because
the current model treats expiration as irreversible and ignores later lifetime updates for already
expired ranges.

## Segment and Shard Deletion

When a segment becomes durably deleted:

- Its summary must disappear from the current segment-accounting view.
- Record and change rows may be removed once no active GC snapshot pins them.
- Run compaction should eventually purge the segment's key range.
- Missing overlay state must never be interpreted as proof that a non-deleted segment is empty.

Shard-generation cleanup can publish range tombstones or a manifest-level obsolete-owner marker,
then let compaction remove affected segment rows. The choice should avoid writing one deletion record
per physical range.

## Source-of-Truth and Rebuild Semantics

The segment-accounting LSM is the authoritative online GC view. GC planning, selection, empty-delete
checks, and reconciliation read it rather than RocksDB overlay or ref-event rows.

It remains a derived index. A rebuild can use:

```text
sealed segment record/index files for the physical record universe
blob-accounting materialized state for currently protected RecordRefs and lifecycles
relocation metadata for in-flight or published moves
current epoch for expiration classification
segment state for ownership and deletion status
```

During rebuild, GC is disabled or limited to actions proven safe without segment-accounting state.
The rebuilt index becomes visible only after it reaches a durable accounting frontier and its
manifest is atomically published.

Rebuild should prefer conservative classification. A range that cannot be proven retired or expired
is treated as copy-eligible.

## Failure Handling

- Failure to write or sync a segment delta run aborts the accounting publication. Blob-accounting
  input history remains live and the transition can be retried.
- Failure to publish the combined manifest leaves only orphan output files.
- Corrupt current run files halt GC decisions that depend on them and trigger repair or rebuild.
- Segment-accounting compaction failure leaves the previous manifest readable.
- Summary underflow, overlapping incompatible record lengths, or invalid state transitions are hard
  errors with segment, range, LSN, and run provenance in the diagnostic.
- A missing current-manifest run is corruption, not an empty table.

## Migration Plan

### Phase 0: measurement

Instrument the current implementation before changing ownership:

```text
ref-event rows and bytes per accounting publication
overlay operand count and encoded bytes per publication
synced accounting batch latency and bytes
overlay full-merge count and duration
GC summary-snapshot duration
ref-event rows scanned versus returned
foreground write/sync latency during accounting work
```

### Phase 1: shadow segment index

Write segment-accounting runs while continuing to publish the current RocksDB overlay and ref-event
rows. Compare summaries, canonical overlays, and reconciliation results at the same accounted
frontier. Any mismatch fails closed for GC and records both representations for diagnosis.

This phase temporarily increases write volume and should be used only for validation or limited
canaries.

### Phase 2: LSM reads, RocksDB shadow writes

Switch GC planning, copy selection, and reconciliation to the segment-accounting LSM. Continue
writing the RocksDB representations for rollback and comparison.

### Phase 3: stop record-proportional RocksDB projection

Stop writing `segment_ref_events` and `segment_gc_overlay`. Continue publishing the combined
accounting manifest and frontier in RocksDB. Keep old column families read-only for an upgrade
window.

### Phase 4: remove legacy state

After restart, rollback, and mixed-version requirements are satisfied, drop or offline-clean the
legacy column families. Removal should be an explicit operational step rather than part of normal
store open.

## Performance Model

| Operation | Current representation | Proposed representation |
| --- | --- | --- |
| Accounting publication | `N` event puts + `S` overlay merges in synced RocksDB batch | Sequential run write + small synced manifest batch |
| GC planning | One overlay `Get`/merge per segment | Sequential compact summary scan |
| GC source overlay | Full merge value for selected segment | Segment-local base/delta iterator merge |
| GC reconciliation | Scan/filter retained event CF | Scan newer runs for selected segments |
| Overlay compaction | RocksDB-controlled full merges | Strata-controlled partition compaction |
| History retention | Event rows pruned by LSN pins | Immutable change runs retained by snapshot pins |

The proposal exchanges RocksDB key/value and compaction overhead for explicit sequential files,
manifest management, and a second compactor. It should reduce foreground metadata interference, but
the hypothesis must be validated under realistic disk contention.

## Observability

The segment-accounting index should export:

- Current manifest generation and accounted frontier.
- Delta, base, and retained-change run counts and bytes by partition.
- Bytes written, read, and compacted.
- Prepare, fsync, publish, apply, and compaction latency.
- Summary scan latency and bytes decoded.
- Overlay read latency, runs consulted, and bytes decoded per source segment.
- Reconciliation runs/rows scanned, rows returned, and source segments queried.
- Active snapshot pin count, minimum pinned LSN, and oldest pin age.
- Orphan and obsolete file cleanup counts.
- Rebuild status and frontier.
- Foreground sync latency correlated with segment-accounting writes and compaction.

## Validation Plan

### Model equivalence

Generate random sequences of live allocations, overwrites, tombstones, lifecycle changes, epoch
changes, mappings, and shard drops. Apply them to both the current `SegmentGcOverlay` fold and the
new run reader, then compare canonical record state and every summary field.

### Compaction equivalence

Verify that any valid partitioning of the same ordered delta runs produces the same base state,
summary, and visible change history.

### Snapshot races

Exercise:

```text
overwrite during GC copy
tombstone during GC copy
lifecycle change during GC copy
epoch advancement during GC copy
shard drop during GC copy
relocation during another GC copy
segment-accounting compaction while a GC snapshot is pinned
```

The LSM reconciliation result must match the current ref-event implementation.

### Crash injection

Inject crashes:

- Before and after each run fsync.
- Before and after combined-manifest publication.
- Before and after in-memory apply.
- During base compaction output.
- During orphan and obsolete-file deletion.

Recovery must expose either the old complete generation or the new complete generation, never a
mixture.

### Load testing

Measure foreground throughput and p50/p99/p999 latency under:

```text
steady accounting ingestion
large accounting catch-up
forced materialization
epoch transition
multiple concurrent GC workers
long-running pinned GC snapshot
segment-accounting base compaction
low free-space pressure
```

Compare current shared RocksDB projection, dedicated RocksDB where applicable, and the proposed
file-backed index using the same foreground workload.

## Alternatives Considered

### Keep the current RocksDB representation and tune it

Possible changes include column-family-specific options, operand-depth limits, smaller accounting
batches, and rate-limited publication. These reduce symptoms but retain record-proportional WAL/SST
traffic and full-merge semantics.

### Split summaries into a separate RocksDB column family

This is a useful incremental improvement because planning no longer reads the full overlay. It does
not remove ref-event rows, overlay operand writes, merge compaction, or full-CF reconciliation scans.

### Batch ref events into RocksDB values

Writing one event-batch row per accounting publication reduces key overhead and makes LSN-range
scans possible. It is a credible smaller change, but the bulk history still shares RocksDB's WAL and
compaction pipeline.

### Put overlay column families in a dedicated RocksDB instance

This isolates RocksDB write queues and compaction state, but introduces a second WAL and a cross-DB
publication problem. A manifest-published immutable run store provides a clearer atomicity protocol
and more direct scheduling control.

### Store one immutable overlay sidecar per sealed segment

Sidecars make copy reads simple but are awkward for lifecycle changes, overwrites, and relocations
that arrive after sealing. Versioned delta runs are still needed, which converges on an LSM.

## Open Questions

1. Should segment runs share the existing `AccountingManifest` type directly, or should that
   manifest reference a versioned `SegmentAccountingManifest` root?
2. Should segment partitions use hash partitioning, contiguous segment-ID ranges, or shard/owner
   grouping?
3. What is the target delta-run size and maximum run depth before compaction?
4. Should record state be stored as exact per-record rows, coalesced ranges, or a hybrid?
5. Can one delta run safely serve both overlay input and retained GC change history, or is a compact
   change-only run format worthwhile initially?
6. What exact destination or relocation events must `changes_since` return when the query is scoped
   to copied source segments?
7. Should summary deltas be stored beside record mutations or in independent summary runs?
8. What maximum GC snapshot age should trigger cancellation or operator warning?
9. Can expiration be derived from lifecycle and current epoch without explicit per-record `Expire`
   mutations in a later version?
10. Which compression and block size minimize summary and segment-local scan cost without excessive
    CPU use?
11. How should mixed-version nodes coordinate the transition away from RocksDB overlay and event
    column families?
12. What is the minimum rebuild input that must be retained after blob-accounting major compaction?

## Recommended Initial Scope

The first implementation should deliberately avoid combining every possible optimization:

1. Add segment delta and base runs under the accounting index.
2. Publish one combined blob/segment accounting manifest generation.
3. Store summaries separately from record-level state.
4. Preserve explicit expiration mutations.
5. Retain delta runs as ordered GC change history while snapshots need them.
6. Implement segment-local overlay reads and source-scoped `changes_since`.
7. Dual-write and compare against the current RocksDB implementation.
8. Add rate-limited segment compaction only after correctness equivalence is established.

This scope captures the main benefit—removing record-proportional derived state from RocksDB—while
keeping expiration semantics, accounting ordering, and GC reconciliation recognizable.
