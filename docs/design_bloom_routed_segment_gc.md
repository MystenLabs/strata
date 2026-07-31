# Bloom-Routed Segment Accounting and GC

Status: exploratory design

For a shorter overview without implementation-level data structures, see
`design_bloom_routed_segment_gc_concise.md`.

## Summary

This document proposes replacing Strata's global, blob-key-oriented accounting LSM with a
segment-oriented design built around immutable Bloom filters and exact per-segment reconciliation.

The core observation is that GC does not normally need the latest state of every blob key. It needs
to answer a narrower question:

```text
Which sealed segments might contain records invalidated by this new operation?
```

Each sealed segment can answer the membership half of that question with an immutable Bloom filter.
An overwrite, tombstone, lifetime change, or other keyed operation is routed to the small set of
segments whose filters report a possible match. The routed operations form a segment-local history.
When one segment appears to contain enough garbage, Strata joins that history with the segment's
immutable record index and computes its exact live and garbage bytes.

Bloom filters are never liveness authority. They are only a routing and scheduling accelerator:

- A false positive sends an operation to an unrelated segment and may cause an unnecessary exact
  preflight.
- A missing or corrupt filter must fail closed. It can delay reclamation, but cannot make a live
  record reclaimable.
- Exact record identity, operation ordering, shard identity, lifecycle semantics, and GC relocation
  rules are checked before any byte is reclaimed.

At the target scale of 200 TiB, 1 GiB segments, and 64 KiB records, a flat filter matrix configured
with 20 hashes requires about 11.3 GiB. A flat transposed lookup reads about 500 KiB per key. At
2 GiB/s of 64 KiB operations, that is about 15.6 GiB/s of memory traffic, which is practical on a
modern server but needlessly CPU intensive. A two-level group/segment matrix roughly doubles the
filter memory while reducing the normal lookup to a few KiB per key.

The intended end state is:

```text
segment data and record index       immutable physical truth
segment membership filters         probabilistic routing hints
routed segment-operation runs       exact operation history for candidate segments
segment GC summaries                approximate scheduling state plus verified snapshots
foreground RocksDB index            logical read/write authority and atomic publication root
```

The existing active accounting log remains useful as the ordered, durable input stream and replay
source. This proposal removes the need to repeatedly major-compact all blob-key state merely to
discover physical garbage. Complete removal of the current accounting LSM is possible only after
the lifecycle, shard-drop, and relocation cases described below are proven equivalent. The first
rollout should shadow the current implementation.

## Relationship to Existing Designs

The current blob-LSM garbage path is documented in `lsm_gc.md`; the older design was documented in
`design_segment_accounting_lsm.md`. It uses a blob-key LSM to fold complete key histories and emit
physical `RefEvent`s. Major compaction is the point where a new update meets older materialized
state, so it is also the point where `accounted_lsn` can advance and GC can learn that old physical
ranges are dead.

This proposal changes the direction of the join:

```text
current:
  sort all operations by blob key
    -> join with global blob state
    -> discover old RecordRefs
    -> project results to segments

proposed:
  use immutable segment membership to route operations
    -> accumulate segment-local candidate histories
    -> inspect only a segment that is likely worth reclaiming
    -> join its exact record list with its routed operations
```

This is not a generic LSM with a Bloom filter added to it. The immutable segment is the durable base
state, and old state is materialized only for a segment that GC may actually reclaim. Cold, mostly
live segments never pay repeated global major-compaction cost.

The proposed per-segment record index from `design_segment_record_index.md` is a natural companion:
the Bloom filter answers where an operation might matter, while the record index provides the exact
keys, shards, LSNs, lengths, and offsets needed to prove what can be reclaimed without scanning
payload bytes.

## Goals

- Make accounting work proportional to invalidations and likely GC candidates rather than total
  historical key state.
- Avoid global accounting major compactions whose output grows with the total live key population.
- Keep all probabilistic decisions off the data-safety boundary.
- Select candidate segments without probing every per-segment filter individually.
- Compute exact garbage bytes without querying RocksDB once a segment has been selected.
- Support 200 TiB on one node with approximately 1 GiB GC units.
- Keep the hot membership-routing structure in RAM and bulk metadata on SSD.
- Permit GC and accounting work to be scheduled independently from foreground writes.
- Replace the global `accounted_lsn` GC gate with explicit, segment-relevant coverage.
- Preserve deterministic recovery, auditing, and rebuildability.

## Non-goals

- Bloom membership alone will not declare a record retired, expired, or safe to reclaim.
- This design does not remove the foreground logical index used to serve reads and commit writes.
- It does not make hash collisions an acceptable source of record identity.
- It does not make a whole 8 GiB file independently reclaimable in 1 GiB pieces unless the physical
  format and filesystem support those pieces as independent GC units.
- It does not require size-bucket filters in the first version.
- It does not promise that all accounting history can be discarded immediately after routing.
- It does not make lifecycle and shard semantics optional. A restricted first version may treat
  records with incomplete lifecycle information as live.

## Terminology

- **GC unit**: the independently selectable and reclaimable physical unit. The recommended initial
  unit is one sealed 1 GiB segment.
- **Record seed**: immutable information about one physical record from the segment index.
- **Membership filter**: immutable Bloom filter containing the keys present in one GC unit.
- **Filter group**: a fixed-size collection of segment filter columns used by hierarchical lookup.
- **Routing**: mapping one exact accounting operation to possible source segments.
- **Routed operation**: an unmodified operation stored for one candidate segment. Its presence does
  not prove that the operation affects a record in that segment.
- **Candidate estimate**: intentionally approximate garbage estimate used only for scheduling.
- **Exact preflight**: local merge of record seeds and routed operations that classifies every range
  before GC reads or copies payload bytes.
- **Routing frontier**: greatest contiguous accounting-log LSN durably considered by the router.
- **Segment coverage**: proof that a segment has received all relevant operations through a routing
  frontier, including seal-time backfill.

## High-Level Architecture

```text
                       foreground commit
                              |
                              v
                 ordered active accounting log
                              |
                       routing worker
                              |
              +---------------+----------------+
              |                                |
              v                                v
     in-memory transposed filters       directly addressed events
       keyed operation routing          MapRef / shard / segment events
              |                                |
              +---------------+----------------+
                              |
                              v
                immutable routed-operation runs
                  partitioned by segment id
                              |
                        summary estimator
                              |
                    likely-GC candidate
                              |
                              v
      segment.idx + routed operations + captured global epoch
                       exact preflight
                              |
                 +------------+-------------+
                 |                          |
          below threshold             above threshold
          update estimate             copy exact live set
                                             |
                                    reconcile newer ops
                                             |
                                    atomic GC publication
                                             |
                                      reclaim source unit
```

There are two distinct paths:

1. The frequent routing path is probabilistic, batched, and entirely metadata-oriented.
2. The infrequent reclamation path is exact and operates on one or a few selected segments.

Keeping those paths separate is the main design invariant.

## Persisted Data Model

### Segment record index

Each sealed segment has an immutable record index. The current record-index proposal should be
extended, if needed, so one entry can seed exact local classification:

```rust
struct SegmentRecordSeed {
    key: BlobKey,
    shard: ShardKey,
    // Original logical payload version.
    payload_lsn: StrataLsn,
    // LSN at which this physical range became reachable. This differs from payload_lsn after GC.
    physical_birth_lsn: StrataLsn,
    offset: u64,
    encoded_record_len: u64,
    payload_len: u64,
    owner: SegmentOwner,
    initial_lifecycle: LifecycleSeed,
}

enum LifecycleSeed {
    Known(Option<BlobLifecycle>),
    Unknown,
}
```

The exact encoding can differ. The semantic requirements are:

- Physical range identity is exact and checksummed.
- The index distinguishes records for the same key in different shard generations.
- The record's payload LSN is preserved even when later metadata operations advance the key LSN.
- A relocated record preserves both its original payload LSN and its new physical publication LSN.
- GC can determine whether lifecycle state is known or must be handled conservatively.
- A GC relocation output record is indexed just like a foreground record.

An `Unknown` lifecycle is always treated as live with respect to expiration. It can still be retired
by an exact overwrite, tombstone, shard deletion, or `MapRef` transition.

### Per-segment membership filter

For each sealed segment `S`, construct:

```text
BF[S] = Bloom({ record.key | record is physically present in S })
```

The filter contains physical membership, not current liveness. A key remains in the filter for the
lifetime of the segment even after all of its records become garbage.

The filter header should include:

```rust
struct SegmentFilterHeader {
    format_version: u32,
    segment_id: SegmentId,
    segment_generation: u64,
    record_count: u32,
    bit_count: u64,
    hash_count: u16,
    hash_seed_version: u16,
    record_index_checksum: [u8; 32],
    filter_checksum: [u8; 32],
}
```

Tying the filter to the record-index checksum prevents a valid filter for the wrong segment
generation from being published accidentally.

### Routed operation runs

Routing should not create one tiny file per segment and input batch. Instead, each router batch
writes an immutable run sorted by segment:

```rust
struct RoutedOperationKey {
    segment_id: SegmentId,
    key: BlobKey,
    lsn: StrataLsn,
    ordinal: u32,
}

struct RoutedOperation {
    operation: BlobUpdate,
    route_kind: RouteKind,
}

enum RouteKind {
    BloomCandidate,
    DirectRecordRef,
    ShardScope,
    SealBackfill,
}
```

The persisted value should retain the full key unless an exact collision-resolution scheme exists.
A 64- or 128-bit fingerprint by itself is not sufficient data-safety identity.

Each run contains:

- minimum and maximum LSN;
- a sparse segment directory;
- checksummed compressed blocks;
- the filter manifest generation used for routing;
- the global epoch events or epoch frontier needed to interpret lifetime state;
- a durable batch identifier for replay idempotence.

Runs may be minor-compacted by concatenating and sorting segment-local histories. They do not need a
global materialized blob-key base. Once a source segment is reclaimed and no snapshot references its
history, its routed operations can be dropped.

This is still a log-structured metadata store, but it is not a full blob-state LSM: compaction is
bounded by segment histories and does not repeatedly rewrite the total current key set.

### Segment GC summaries

Each segment has two kinds of summary state:

```rust
struct SegmentGcEstimate {
    candidate_events: u64,
    candidate_bytes_estimate: u64,
    last_routed_lsn: StrataLsn,
    last_exact_preflight_lsn: StrataLsn,
}

struct VerifiedSegmentGcSummary {
    total_bytes: u64,
    live_bytes: u64,
    retired_bytes: u64,
    expired_bytes: u64,
    verified_through_lsn: StrataLsn,
    verified_epoch: Epoch,
}
```

The estimate may double-count repeated updates and Bloom false positives. It can trigger work but
cannot authorize reclamation. The verified summary is produced from exact preflight and is valid
only for its captured operation and epoch frontiers.

An optional per-segment distinct-key sketch can reduce repeated overestimation. Its errors must only
affect scheduling. A second "deleted-key Bloom filter" can serve this purpose, but it must never be
used as proof that a physical range is dead.

### Manifest and frontiers

The durable root names all live filters, filter-matrix checkpoints, routed runs, and segment
coverage:

```rust
struct BloomAccountingManifest {
    generation: u64,
    routing_frontier: StrataLsn,
    active_log_cursor: ActiveDeltaLogReadCursor,
    filter_groups: Vec<FilterGroupState>,
    routed_runs: Vec<RoutedRunState>,
    segment_coverage: Vec<SegmentCoverageRange>,
    min_pinned_routing_frontier: StrataLsn,
}
```

The manifest can remain a small RocksDB value so foreground metadata, GC publication, and the
accounting root can be committed atomically where required.

## Bloom Filter Organization

### Independent per-segment filters

The simplest representation stores one conventional filter per segment. It is compact and easy to
rebuild, but looking up one key requires 20 random bit tests in every segment filter:

```text
O(segment_count * hash_count) random probes per key
```

At 204,800 segments this is not acceptable.

### Flat transposed matrix

All segment filters use the same logical bit count `m` for a size class. Instead of laying them out
one filter after another, transpose the filters:

```text
normal layout:
  segment 0 -> bit[0..m)
  segment 1 -> bit[0..m)
  ...

transposed layout:
  bit position 0 -> bitmap of segment ids
  bit position 1 -> bitmap of segment ids
  ...
```

For a key with Bloom positions `h0..h19`, candidate segments are:

```text
candidates = row[h0] AND row[h1] AND ... AND row[h19] AND live_segment_mask
```

This is the per-bit inverted index discussed in the proposal. We do not need a separate index for
zero bits. A Bloom membership test requires all selected bits to be one; only the one-bit posting
bitmap participates in the intersection.

The transposed representation contains exactly the same number of filter bits as independent
filters. Its advantage is turning millions of small random tests into a small number of sequential
bitmap intersections.

Its drawback is that every selected row spans every segment. With 204,800 segments, one row is
25,600 bytes and a 20-hash lookup reads 500 KiB.

### Two-level filter hierarchy

A group hierarchy trades memory for much lower lookup traffic. For example, group 1,024 segments:

```text
level 0: Bloom filter for the union of keys in each group
         transposed across approximately 200 groups

level 1: one per-segment Bloom filter
         transposed only across the 1,024 segments in its group
```

Lookup proceeds as follows:

1. Intersect 20 level-0 rows to find possible groups.
2. For each possible group, intersect 20 level-1 rows to find possible segments.
3. Mask out retired segment slots and emit exact segment generations.

For one matching group, the bitmap payload is approximately:

```text
20 * (200 / 8) bytes       level 0
+ 20 * (1,024 / 8) bytes  level 1
= 3,060 bytes per key
```

Cache-line fetches, matrix metadata, and multiple true groups make actual traffic higher, but this
is still far below the flat 500 KiB lookup.

The hierarchy stores each key once at the group level and once at the segment level, so the filter
bits are roughly doubled. At the target scale that is approximately 22.6 GiB before alignment,
manifests, masks, and rebuild headroom.

Group filters cannot be formed by blindly OR-ing per-segment filters when the two levels use
different bit counts. Keys should be hashed independently into each level during segment seal or
group rebuild.

### Recommended initial layout

Implement the conventional per-segment on-disk format first and derive either in-memory layout from
it:

1. Begin with the flat transpose to validate routing equivalence and measure actual CPU cost.
2. Add the two-level transpose if flat lookup consumes material CPU or memory bandwidth.
3. Keep group size configurable; benchmark 512, 1,024, and 2,048 segment groups.
4. Persist a matrix checkpoint for fast restart, but retain per-segment filters as the rebuildable
   canonical form.

This separates the durable format decision from an in-memory optimization.

## Filter Construction and Segment Seal

An immutable filter is easy to build after a segment is sealed, but operations can invalidate a
record while its segment is still open. Publishing a filter without covering that interval would
lose routing information.

The seal protocol should be:

1. Stop appending records to segment `S` and capture its maximum durable offset and LSN.
2. Finish and fsync `S.idx`.
3. Build `S.bf` from every record key in the sealed index.
4. Capture the router frontier `R0`.
5. Replay the retained accounting log from `min_record_lsn(S)` through `R0` against `S.bf`.
6. Write routed backfill operations for `S`, including exact direct events.
7. Fsync the filter and backfill run.
8. Atomically publish `S` as filter-visible with coverage through `R0`.
9. Add the segment column to the live transposed matrix.

Operations before an individual record's LSN may be routed during backfill. Exact preflight ignores
them for that record. This deliberately favors a simple contiguous replay range over per-record
backfill bookkeeping.

The global operation log cannot be pruned past a frontier needed to backfill an open or sealing
segment. With 1 GiB segments the interval should remain bounded. Long-lived partially full segments
need an explicit maximum age or an active mutable filter path.

An optional optimization maintains a mutable filter for each open segment and routes operations to
it in memory. Seal-time replay is still required as a correctness backstop.

## Operation Routing

The router consumes the durable accounting log in strict LSN order. It processes a batch as follows:

1. Separate keyed membership-routed operations from directly addressed and global operations.
2. Hash keyed operations in batches and intersect filter matrices.
3. Add one routed row for every candidate live segment generation.
4. Route `MapRef` source and destination ranges directly by `RecordRef`, regardless of Bloom output.
5. Attach shard drops to affected shard-owned segments using the segment/shard directory.
6. Record epoch transitions in the routing batch so exact readers can interpret lifecycle state at
   a prefix.
7. Sort output by segment, key, LSN, and ordinal.
8. Write and fsync immutable run blocks.
9. Publish the runs, active-log cursor, and contiguous routing frontier in one manifest update.

If a routed batch is prepared but not published, it is an orphan and can be deleted on recovery. If
it is published, replay uses its batch id and LSN interval to avoid duplicate estimator updates.
Exact classification is naturally idempotent because it sorts operations by their logical identity.

### Batched lookup

Keys should be processed in batches large enough to amortize matrix and allocation overhead:

- hash each key once using double hashing or another versioned scheme;
- group equal Bloom row requests when possible;
- intersect into reusable aligned bitmaps;
- stop an intersection early when no candidate bits remain;
- enumerate set bits only after applying the live-segment mask;
- shard groups and router workers by NUMA node;
- keep a segment-generation lookup beside each matrix slot.

Matrix memory should be allocated and queried on the same NUMA socket. Summing bandwidth across two
sockets is meaningless if every lookup fetches remote pages from one socket.

## Exact Segment Preflight

When the estimator crosses a configurable threshold, Strata performs an exact metadata-only
preflight before copying payloads:

1. Capture routing snapshot `R` and the epoch visible at `R`.
2. Read the selected segment's record index.
3. Read routed operations for that segment through `R` using sparse run indexes.
4. Sort or merge record seeds and operations by `(key, shard, lsn, ordinal)`.
5. Run the exact reducer for each physical record.
6. Produce exact live, retired, expired, and unknown-lifecycle ranges.
7. Validate lengths and range identities against the sealed segment state.
8. Admit the segment to payload GC only if exact reclaimable bytes cross the threshold.

No RocksDB point query is needed in this path. RocksDB remains involved later for atomic validation
and publication of the relocation, not for calculating the segment's garbage estimate.

The preflight can stream two sorted inputs if the record index is key-sorted. If the index remains
offset-sorted for copy efficiency, either add a compact key-order permutation, externally sort the
approximately 16,384 entries of a 1 GiB/64 KiB segment, or hash the record seeds in memory. This is a
small bounded local operation.

### Why a Bloom hit is insufficient

Suppose segment `S` contains `(K, shard A, payload_lsn 10)` and the router sees a put of `K` at LSN
20 for shard B. `S.bf` reports a match, but the new put does not retire the shard-A record. The exact
reducer must inspect the shard.

Similarly:

- A `SetLifetime` changes metadata but does not itself retire a live record.
- A stale `MapRef` applies only when its exact source `RecordRef` is present.
- An operation older than a physical record cannot retire that record.
- A Bloom false positive may not correspond to any record at all.

Only exact preflight resolves these cases.

## Operation Semantics

### Put

A put is routed by key to all candidate source segments. For one exact record:

- a later put for the same shard retires the older physical record;
- records for other shards remain independently live;
- the new `RecordRef` seeds membership in its destination segment at seal;
- an earlier put cannot affect a later physical record.

Repeated historical puts may route to several still-live source segments. This fanout reflects real
uncollected history, not merely Bloom amplification.

### Tombstone

A tombstone is routed by key and retires older payloads for all applicable shards. A tombstone after
the selected record LSN is strong positive evidence of retirement. A later put does not revive the
old physical range; it creates a new range.

### SetLifetime and epoch changes

Expiration is the hardest part of eliminating global key state because the current foreground put
can say "preserve the previously materialized lifecycle." A segment-local reducer cannot infer a
lifecycle that was set before the record unless one of the following is available:

1. the effective lifecycle is embedded in the record seed;
2. the complete relevant prehistory is routed to the segment; or
3. a smaller lifecycle state service remains temporarily.

The recommended first implementation treats unknown lifecycle as live. Overwrite, tombstone, and
direct relocation garbage can still be reclaimed. Expiration-based reclamation is enabled only for
records with a known lifecycle seed and complete later lifecycle/epoch history.

Exact preflight uses the epoch at its routing prefix, not a newer wall-clock or durable epoch. A
later but unrouted lifetime extension must never be bypassed by interpreting a newer epoch first.

### MapRef and GC relocation

`MapRef` already contains exact source and destination `RecordRef`s. It should bypass probabilistic
routing:

- route `Retire(from)` directly to the source segment;
- route `AddLive(to)` directly to the destination segment;
- preserve the key and lifecycle association required by later operations;
- index the output record and add it to the destination membership filter.

GC publication must remain conditional on the source physical reference still being current for the
logical key. If a foreground update wins the race, the copied output is garbage and must not revive
the old value.

### Shard drop

A shard drop is not a key-routed event. Segments already carry ownership and shard information, so
the event should use a direct segment/shard directory:

- whole shard-owned directories can follow the existing whole-directory deletion path;
- mixed store-owned segments require exact record-index classification by shard;
- the global routing frontier cannot pass the shard-drop LSN until its segment-scope consequence is
  durably represented.

### Multiple physical copies of one key

The same key can appear in several segments due to overwrites, shards, or GC relocation. Routing to
all Bloom matches is intentional. Exact reduction independently determines the fate of each physical
range. A current logical-key lookup is not required to discover the candidate ranges.

## Garbage Estimation and Selection

Bloom routing naturally produces an overestimate:

```text
candidate bytes = sum(size estimate for each routed invalidating event)
```

It can be inflated by:

- filter false positives;
- multiple operations invalidating the same record;
- an operation matching a key in another shard;
- stale group-filter bits;
- operations older than a matching record;
- membership in a segment whose matching record was already known dead.

That is acceptable because selection has two gates:

```text
cheap approximate threshold -> exact metadata preflight -> physical GC threshold
```

After a false admission, persist the verified summary and reset or calibrate the estimator so the
same segment is not repeatedly preflighted without enough new routed evidence. Use hysteresis and a
minimum new-operation count before retrying.

### Size buckets

Size-bucket membership filters can improve byte estimates when blob sizes vary widely:

```text
64 KiB, 256 KiB, 1 MiB, 10 MiB, 100 MiB, 1 GiB+
```

Each physical record belongs to exactly one bucket. The total filter bits remain approximately
proportional to total record count, but a tombstone usually does not know the old record size. It
must query every bucket unless foreground metadata supplies a previous size or `RecordRef`.

Therefore:

- use one filter for fixed 64 KiB-style workloads;
- start variable-size support with one membership filter and a segment size histogram;
- add bucketed filters only if improved scheduling accuracy repays the additional lookup and matrix
  complexity;
- never use a bucket's representative size as the exact reclaimed-byte count.

An exact preflight always obtains the real encoded lengths from `segment.idx`.

## Replacing `accounted_lsn`

Today `accounted_lsn` means a contiguous prefix has passed through the global reducer and its
physical effects have been published. GC uses it as a conservative coverage gate.

The Bloom-routed design should not simply delete that check. It should replace one global notion
with more precise coverage:

```text
routing_frontier
  all log entries through LSN R were durably considered by the router

segment_coverage[S]
  S was filter-visible and received seal backfill plus live routing through R

verified_through[S]
  exact preflight classified S using all routed effects through R
```

A segment may enter exact GC when:

```text
filter/index checksums valid
AND seal backfill complete
AND segment_coverage >= captured routing_frontier
```

It does not need every key in the store to be globally materialized. It needs complete, ordered
evidence relevant to the selected segment.

The router frontier may lag `published_lsn`. GC can still classify at the older prefix because an
unseen overwrite or tombstone can only cause it to copy extra stale data. It cannot use an epoch or
other non-monotonic semantic event newer than the prefix. Before source deletion, relocation
publication must reconcile newer routed events and conditionally validate the source mappings.

This argument must be model-checked against every operation type. Until that proof and the
conditional publication path exist, retain the existing `accounted_lsn` gate in shadow mode.

## GC Copy and Reconciliation

After exact preflight admits segment `S`:

1. Capture `(manifest generation, routing frontier R, epoch at R)`.
2. Claim `S` so another worker cannot concurrently collect it.
3. Read only ranges classified live by the exact preflight.
4. Verify record checksums while copying.
5. Bring routing current to a reconciliation frontier `R2`.
6. Read segment-local routed operations in `(R, R2]`.
7. Remove newly invalidated copies and update lifecycle classifications.
8. Publish destination segments and conditional `MapRef`s atomically.
9. Verify that current foreground mappings either moved from the claimed source or no longer point
   to it.
10. Retire and eventually unlink the source segment.

If reconciliation discovers an operation whose semantics cannot be resolved locally, abort the GC
job and retain the source. Failure costs space and work, not correctness.

Routed history must remain pinned through publication. Runs can be deleted only when no segment or
GC snapshot can ask for their LSN interval.

## Correctness Invariants

The design is safe only if all of these hold:

1. **Bloom filters never prove death.** They only decide where exact operations are copied.
2. **Unknown means live.** Missing history, lifecycle ambiguity, checksum failure, or unsupported
   operation semantics protects the physical record.
3. **Exact range identity.** Reclamation uses `(segment generation, offset, length)` from a validated
   record index, not a key hash.
4. **Ordered prefix.** A routing frontier advances only through a contiguous durable log prefix.
5. **Seal coverage.** A sealed segment is not selectable until backfill closes the interval in which
   it was open but not filter-visible.
6. **No newer epoch with older key history.** Epoch and lifecycle decisions use one routing prefix.
7. **Shard isolation.** A put in one shard does not retire a record for the same key in another.
8. **Relocation is conditional.** GC cannot make a copied stale value current after a foreground
   operation has superseded the source.
9. **Generation-safe matrix slots.** Reusing a segment bitmap slot cannot route an old operation to
   a different physical generation without validation.
10. **Pinned reconciliation.** Routed history visible to a GC snapshot is retained until the job
    commits or aborts.
11. **Physical deletion follows publication.** Source bytes are removed only after the durable
    metadata root no longer requires them.
12. **Filter corruption fails closed.** Rebuild from `segment.idx`; do not silently continue with an
    incomplete filter.

### Effect of Bloom errors

| Condition | Result | Safety |
|---|---|---|
| Normal false positive | Extra routed operation or preflight | Safe |
| Repeated candidate event | Estimate too high | Safe |
| Stale group bit | Extra group/segment work | Safe |
| Construction bug causing false negative | Garbage may remain or be recopied | Safe only because unmatched records default live |
| Corrupt record index accepted as valid | Wrong exact classification | Unsafe; checksum validation is mandatory |
| Missing direct `MapRef` route | Source may remain protected | Safe leak, but must alarm |
| Routing frontier skips an LSN | Local history may be incomplete | Invalid state; stop GC and rebuild/replay |

## Crash Consistency

### Crash while building a segment filter

The filter and backfill are not visible until both are fsynced and named by the durable manifest.
Unpublished files are orphaned. Recovery rebuilds them or deletes them.

### Crash after route-run fsync but before publication

The run is an orphan. The active-log cursor did not move, so recovery replays the batch. Batch ids
prevent duplicate estimator effects if orphan discovery races with cleanup.

### Crash after manifest publication

The route run and frontier are authoritative. Recovery reconstructs or loads the transposed matrix
from canonical segment filters and resumes after the published cursor.

### Crash during matrix update

The in-memory transpose is a cache. Its partial state is discarded. A persisted checkpoint is valid
only when its manifest generation and checksum match; otherwise rebuild it from segment filters.

### Crash during exact preflight

No physical state changed. Discard the result and retry.

### Crash during GC publication

Use the existing prepared-output and atomic-root protocol. Recovery decides from the durable root
whether output or source segments are live. Routed input runs remain pinned until that decision is
resolved.

## Filter Lifecycle and Churn

Standard Bloom filters cannot clear one segment's contribution from an aggregate group filter. The
level-1 live-segment mask removes reclaimed leaf columns, but level-0 group filters will accumulate
stale keys and gradually produce more candidate groups.

Recommended policy:

- keep leaf per-segment filters immutable and canonical;
- track live/retired column density per group;
- rebuild a group filter from live leaf indexes when retired slots or measured false positives cross
  a threshold;
- publish a new group generation atomically;
- reuse leaf slots only with an explicit segment generation;
- periodically rebuild the top-level matrix offline and swap it through the manifest.

A counting Bloom filter would allow deletion but costs much more memory and introduces counter
overflow and update synchronization. Immutable generations and rebuilds fit Strata's segment model
better.

## Capacity Model

Assume:

```text
total physical capacity     200 TiB
GC unit                     1 GiB
average record payload      64 KiB
Bloom hashes                20
optimal bits per key        k / ln(2) = 28.85
```

Then:

```text
segment count               200 * 1024 = 204,800
records per full segment    1 GiB / 64 KiB = 16,384
total record memberships    3,355,443,200
filter false-positive rate  approximately (1/2)^20 = 9.54e-7
```

### Filter memory

```text
per-segment bits     16,384 * 28.85 = 472,678 bits
per-segment bytes    approximately 57.7 KiB
all segment filters  approximately 11.3 GiB
```

The flat transposed representation uses the same approximately 11.3 GiB. A two-level hierarchy
stores group and leaf membership and therefore uses approximately 22.6 GiB, plus masks, alignment,
manifest state, rebuild overlap, and allocator overhead. Budget 32 GiB of dedicated RAM for an
initial two-level implementation.

With 204,800 segments and `p = 9.54e-7`, a key that exists in one segment produces approximately:

```text
204,799 * p = 0.195 unrelated segment candidates
```

The expected output is therefore roughly 1.2 segments per key before historical duplicate copies
and stale aggregate bits.

### Lookup bandwidth

For the flat transpose:

```text
one segment bitmap row      204,800 / 8 = 25,600 bytes
20 rows                     512,000 bytes = 500 KiB/key
2 GiB/s / 64 KiB            32,768 keys/s
matrix read traffic         15.6 GiB/s
```

Routing another 32,768 deletes/s would make the worst-case total approximately 31.2 GiB/s. This is
within modern server DRAM bandwidth, but the access pattern, cache misses, and bitmap instructions
matter more than the raw bandwidth ratio.

For two levels with 1,024 segments per group, the normal bitmap payload is about 3 KiB/key plus
cache-line and false-group overhead. That shifts the bottleneck toward hashing, run construction,
and actual routed fanout.

### Exact record-index storage

At 16,384 records per segment:

```text
64-byte index entry   1 MiB/segment   approximately 200 GiB at full scale
96-byte index entry   1.5 MiB/segment approximately 300 GiB at full scale
```

This is only about 0.1-0.15% of payload capacity but is too large to keep entirely in RAM. Store
record indexes on SSD and read them sequentially only for selected segments. Compression or a
columnar key/shard/LSN layout may reduce both space and preflight I/O.

### Routed-operation volume

At 32,768 operations/s, a 64-byte encoded operation is 2 MiB/s before routing fanout, or roughly
177 GB/day. Equal put and delete streams double that. An expected fanout near 1.2 adds another 20%,
while repeated versions of hot keys can add much more.

This makes bounded retention essential:

- compress run blocks;
- minor-compact segment-local runs;
- discard histories with reclaimed source segments after snapshot pins clear;
- measure actual true and false routing fanout;
- consider storing shared batch payloads once with per-segment posting lists if duplication dominates.

The design avoids rewriting a full global base, but it does not make the incoming history free.

## Physical Placement

Recommended placement for an HDD-backed blob node:

```text
RAM:
  live transposed matrices
  live-segment masks
  hot manifests and estimator state

SSD:
  canonical per-segment Bloom filters
  matrix checkpoints
  segment record indexes
  routed-operation runs
  manifests and recovery metadata

HDD:
  blob segment data
  sequential GC input/output
```

Querying the matrix directly from SSD would turn selected bit rows into latency-sensitive page
faults. The hot matrix should remain resident. Exact preflight reads bounded, mostly sequential
metadata from SSD before issuing HDD payload reads.

The GC/filter unit should align with the independently reclaimable physical unit. If an 8 GiB file
contains eight 1 GiB durability extents but only whole-file deletion is supported, use one filter
and one GC decision for the 8 GiB file. If the design depends on reclaiming individual extents by
hole punching, cloning, or an extent allocator, each 1 GiB extent needs independent record metadata,
coverage, filter membership, and crash semantics. The total Bloom bits remain driven by record count,
but the physical-reclamation design becomes more complex.

## Concurrency and Scheduling

Routing, exact preflight, and physical GC should use separate budgets:

- routing workers optimize ordered log catch-up and matrix throughput;
- preflight workers consume SSD metadata bandwidth;
- copy workers consume HDD read/write bandwidth;
- publication workers perform small atomic metadata commits.

Useful pressure signals include:

```text
foreground put and sync latency
published_lsn - routing_frontier
routed-run bytes and count
matrix lookup CPU and memory bandwidth
exact-preflight queue depth
GC copy bandwidth
free-space reserve
false-admission rate
group stale-bit density
```

GC candidate priority should combine exact or estimated garbage ratio, reclaimable bytes, segment
age, read/copy cost, and free-space pressure. A large approximate estimate should schedule a
preflight, not jump directly ahead of already verified high-value candidates.

## Observability

At minimum expose:

```text
strata_store_bloom_filter_bytes{level}
strata_store_bloom_filter_segments
strata_store_bloom_filter_group_rebuilds_total
strata_store_bloom_filter_checksum_failures_total

strata_store_accounting_routing_frontier
strata_store_accounting_routing_lag_lsn
strata_store_accounting_routed_ops_total{kind}
strata_store_accounting_routed_bytes_total{kind}
strata_store_accounting_route_candidates{quantile or histogram}
strata_store_accounting_route_lookup_seconds
strata_store_accounting_route_matrix_bytes_total

strata_store_gc_candidate_estimated_bytes
strata_store_gc_preflight_seconds
strata_store_gc_preflight_index_bytes_total
strata_store_gc_preflight_routed_bytes_total
strata_store_gc_preflight_verified_bytes{state}
strata_store_gc_false_admissions_total
strata_store_gc_unknown_lifecycle_bytes

strata_store_accounting_routed_run_bytes
strata_store_accounting_routed_run_count
strata_store_accounting_routed_history_oldest_lsn
```

Dashboards should compare:

- candidate bytes versus exact garbage bytes;
- expected versus observed Bloom fanout;
- routing lag versus foreground throughput;
- preflight SSD throughput versus physical GC throughput;
- filter/group age versus false-positive amplification;
- bytes retained due to unknown lifecycle or incomplete coverage.

## Failure Handling and Rebuild

### Filter checksum failure

Remove the segment column from GC eligibility, continue serving payload reads, rebuild the filter
from the validated record index, replay retained operations, and publish a new generation.

### Record-index checksum failure

The segment cannot pass exact preflight. Rebuild the index from the segment data under an explicit
repair path. Do not infer liveness from the Bloom filter or routed operations alone.

### Missing routed run

A manifest referencing a missing or corrupt routed run invalidates the routing frontier. Stop GC,
replay from the last valid active-log cursor if retained, and alarm. If replay input is no longer
available, fall back to the current accounting system or a full foreground-index/segment rebuild.

### Matrix checkpoint mismatch

Discard and rebuild the in-memory cache from canonical filters. This affects startup time, not
correctness.

### Estimator corruption

Discard estimates and rebuild them from routed runs, or schedule conservative exact preflights.
Verified liveness is not derived from estimator state.

## Validation Plan

### Phase 0: trace simulator

Build an offline simulator that consumes real accounting traces and compares:

```text
current global reducer RefEvents
vs.
Bloom routing + segment-local exact reducer
```

Measure filter fanout, repeated-key fanout, operation-run growth, candidate overestimation, and the
percentage of bytes requiring unknown-lifecycle protection.

### Phase 1: filter and router shadow mode

- Build filters and matrices for sealed segments.
- Route operations but retain the existing accounting LSM as authority.
- Compare every emitted candidate segment with current exact `RefEvent.record_ref.segment_id`.
- A current exact target absent from Bloom candidates is a critical error.
- Record CPU, DRAM traffic, SSD writes, and restart rebuild time.

### Phase 2: exact preflight shadow mode

- Run segment-local reducers for candidates selected by the existing GC summaries.
- Compare record-by-record classification with the current `SegmentGcOverlay`.
- Keep the existing overlay on the copy and publication path.
- Exercise puts, tombstones, lifetimes, epoch changes, shard drops, relocations, and repeated GC.

### Phase 3: overwrite/tombstone authority

- Let exact local results authorize only monotonic overwrite and tombstone garbage.
- Treat lifecycle-unknown records as live.
- Keep current accounting for expiration, complex shard cleanup, and audit comparison.
- Retain the existing `accounted_lsn` gate until conditional GC publication is proven.

### Phase 4: segment-coverage frontier

- Introduce seal backfill and durable per-segment coverage.
- Permit selected GC jobs to use the routing frontier instead of global materialization.
- Run crash injection at every prepare/publish/apply boundary.
- Verify that GC with routing lag can only copy extra bytes, never lose a current value.

### Phase 5: full lifecycle and relocation equivalence

- Embed or otherwise provide exact lifecycle seeds.
- Prove epoch-prefix handling and expiration equivalence.
- Route shard and relocation events directly.
- Remove the remaining global accounting-LSM dependency only after shadow disagreement is zero over
  long-running churn and recovery tests.

### Required adversarial tests

- A key overwritten while its old segment is sealing.
- Tombstone before and after seal backfill capture.
- Same key live in multiple shards.
- Put in one shard while another shard's record remains current.
- Lifetime set before put, extended after put, and raced with epoch transition.
- GC `MapRef` raced with overwrite and tombstone.
- Segment slot reused while an old routing batch is in flight.
- Group rebuild concurrent with routing.
- Crash after run fsync and before manifest publication.
- Corrupt filter, index, route run, and matrix checkpoint.
- Hot key present in thousands of historical segments.
- New random keys that match no live segment.
- Disk full during routing, preflight, and GC publication.

## Alternatives Considered

### Keep the global accounting LSM and split base files

Partitioning and bounded major compaction reduce rewrite amplification and are lower-risk changes.
They retain exact global current state but continue maintaining state for cold keys that may never
help select GC. This remains the safest fallback and a useful baseline.

### Store accounting state in RocksDB

RocksDB provides a mature LSM, filters, recovery, and compaction scheduling. It also shares WAL,
flush, and compaction resources with foreground metadata unless isolated, and it still maintains a
global latest-state index. It is a reasonable implementation baseline but does not exploit immutable
segment membership as the primary direction of the join.

### Query every segment Bloom filter

Memory use is minimal, but CPU and random memory probes scale as segment count times hash count. The
transposed or hierarchical matrix is necessary at 200,000 segments.

### Persist only a global deleted-key Bloom filter

A global deleted-key filter cannot identify the physical segments that contain the invalidated
records, cannot remove old keys cleanly, and cannot distinguish shards or versions. It can be a
scheduling sketch but not the accounting design.

### Query the foreground RocksDB index during GC

Scanning one segment and batch-querying every key against the current logical index is conceptually
simple and exact for current mappings. It creates random metadata reads during GC, complicates
snapshot races, and does not directly preserve lifecycle/ref-event history. It is useful as a shadow
validator and emergency rebuild mechanism, but the proposed normal preflight avoids it.

### Store exact per-segment key hash tables

An exact immutable key-to-record sidecar eliminates Bloom false positives but costs substantially
more SSD/RAM and still needs an efficient global routing directory. The segment record index already
provides exact local data when a segment is selected; Bloom filters keep the global directory small.

## Open Questions

1. Can foreground publication cheaply embed the effective lifecycle in each record seed, or should
   expiration remain on the current accounting path?
2. Does conditional `MapRef` publication already prove that a copied source remains current, or does
   the foreground index need a stronger compare-and-map operation?
3. Should routed operations duplicate values per segment or use one shared operation batch plus
   compressed segment posting lists?
4. What is the measured historical-segment fanout for hot keys under the realistic benchmark?
5. Is one flat 11.3 GiB matrix already cheap enough, making the two-level 22.6 GiB hierarchy
   unnecessary?
6. What group size minimizes CPU after accounting for cache lines, stale bits, and multiple true
   groups?
7. Should record indexes be both key-ordered and offset-ordered, or should one order use a compact
   permutation?
8. How much active-log retention is required for seal backfill at minimum write rate and maximum
   segment age?
9. Can per-segment estimator state be rebuilt cheaply enough to remain non-authoritative?
10. Should one 1 GiB durability extent always be a standalone segment, or is sub-file extent
    reclamation worth its crash and allocator complexity?
11. How should routed history for a segment be compacted when it remains mostly live for months?
12. What exact event subset is monotonic enough to permit GC at a routing frontier behind
    `published_lsn`?

## Recommended Decision

Proceed with a shadow prototype, not a direct replacement:

1. Build immutable per-segment filters from the existing record scan or record index.
2. Implement the flat transposed router and measure it at the 200 TiB model dimensions.
3. Persist segment-partitioned routed operation runs.
4. Implement exact preflight for put overwrite and tombstone semantics only.
5. Compare its range classifications and byte summaries with the current GC overlay.
6. Add the two-level matrix only if flat lookup CPU is material.
7. Design lifecycle seeds and conditional relocation publication before removing `accounted_lsn` or
   the global accounting LSM.

This prototype tests the important claim: immutable segment membership can direct accounting work
to the physical segments where it may create reclaimable space, allowing Strata to avoid repeatedly
materializing a full global key state. It does so without placing probabilistic membership on the
reclamation safety boundary.
# Historical Bloom-Routed Segment GC Proposal

> This proposal was superseded by lazy global materialization in blob-LSM compaction. See
> [`lsm_gc.md`](lsm_gc.md) for the implemented design.
