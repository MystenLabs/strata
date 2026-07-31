# Bloom-Routed Segment Accounting and GC

> Historical proposal, superseded by lazy global materialization in blob-LSM compaction. See
> [`lsm_gc.md`](lsm_gc.md) for the implemented design.

Status: exploratory design

## Summary

This proposal uses immutable Bloom filters to identify which blob segments may be affected by an
overwrite or deletion. It allows Strata to direct accounting work toward segments that are likely to
contain garbage instead of continuously maintaining and compacting the complete state of every blob
key in a global accounting LSM.

Each sealed segment has:

- A compact Bloom filter containing the keys physically present in the segment.
- An exact record index, sorted by key, containing each physical record's key, location, length,
  shard, and payload version.
- A segment-local history of later operations that might affect those records.

When a new operation arrives, Strata queries the Bloom filters and routes the operation to candidate
segments. A per-segment HyperLogLog estimates how many distinct keys have been invalidated without
maintaining the latest put LSN for every key. When one segment appears to contain enough garbage,
Strata combines its exact record index with its routed operations to calculate the real live and
garbage bytes. GC runs only if this exact calculation crosses the collection threshold.

Bloom filters are therefore used for routing and scheduling, not correctness. A false positive
causes extra work but cannot cause a live record to be reclaimed.

## Motivation

The current accounting LSM groups operations by blob key and eventually joins them with older key
state during major compaction. This discovers which physical record was overwritten, tombstoned,
expired, or relocated.

This is exact, but major compaction becomes more expensive as the amount of materialized state
grows. Most of that work may not produce immediately collectable space because it processes keys
without considering whether their physical segments are close to the GC threshold.

The proposed design reverses the accounting lookup:

```text
Current approach:
  operation -> global key state -> old record reference -> segment

Proposed approach:
  operation -> possible segments -> exact segment-local classification
```

The segment data itself becomes the base accounting state. We materialize detailed liveness only for
segments that are likely to be worth collecting.

## Core Design

### Segment membership filters

When a segment is sealed, Strata creates a Bloom filter from all keys physically present in it. The
filter describes membership, not current liveness. A key remains in the filter until the segment is
deleted, even after its record becomes garbage.

For every keyed operation, such as a put or tombstone, Strata asks which live segment filters may
contain the key. The operation is then associated with those segments.

The possible outcomes are:

- A true match routes the operation to a segment containing the key.
- A false positive routes it to an unrelated segment and causes some extra metadata work.
- A corrupt or unavailable filter prevents that segment from being collected until the filter is
  rebuilt.

No Bloom-filter result directly marks bytes as garbage.

### Exact segment record index

Sealing also writes a checksummed index tied to the immutable segment generation. It has one entry
per physical record, not merely one entry per unique key, and is sorted for a streaming key join:

```text
(key, shard, payload_lsn, offset, length)
```

`key` establishes exact membership, `payload_lsn` and `shard` establish whether a later operation
actually invalidates that record, and `offset` plus `length` identify and size the reclaimable range.
The index remains on SSD and is read only for selected segments. An offset-order permutation may be
added if sequential payload copying needs a different order.

### Efficient filter lookup

At 200 TiB with 1 GiB segments, there are approximately 204,800 segment filters. Testing every
filter independently for every key would be too expensive.

Instead, filters can be stored in a transposed form. For each Bloom-filter bit position, Strata keeps
a bitmap identifying the segments where that bit is set. Looking up a key means intersecting the
bitmaps for its hash positions. The remaining set bits identify candidate segments.

```text
hash key -> select Bloom bitmaps -> intersect bitmaps -> candidate segments
```

A two-level version can reduce lookup cost further:

1. Find candidate groups of segments.
2. Search only the per-segment filters inside those groups.

Grouping about 1,024 segments together increases filter memory but reduces the normal lookup from
hundreds of KiB to a few KiB per key.

### Routed operation history

The router consumes the durable accounting log in LSN order. It groups candidate operations by
segment and writes the full operations into immutable delta files indexed by segment generation and
ordered by `(segment, key, operation_lsn)`.

These files are not a materialized latest-value database. They are histories used when a particular
segment is examined. Compaction is not required for correctness: exact preflight can range-scan and
merge multiple immutable delta files. Local compaction or packing remains an optional optimization
if file-count or read amplification becomes material. History can be deleted when the corresponding
source segment has been reclaimed and no GC snapshot still references it.

Operations that already identify physical locations, such as GC relocation mappings, should bypass
Bloom lookup and be routed directly to the referenced source and destination segments.

## Garbage Selection

As operations are routed, Strata maintains a HyperLogLog (HLL) for each segment. The HLL receives
the key of every possible invalidation routed to that segment and estimates how many distinct keys
have been invalidated.

Repeated puts do not require a latest-LSN lookup on the ingestion path:

```text
put K -> HLL.add(K)
put K -> HLL.add(K)
put K -> HLL.add(K)

estimated distinct invalidated keys: approximately 1
```

HLL does not tell us whether one particular key was previously present. It handles duplicates only
as part of its aggregate cardinality estimate. It also does not replace the segment Bloom filter:
the Bloom matrix finds candidate segments for `K`, while the HLL estimates distinct invalidated keys
after `K` has been routed.

For fixed-size records with approximately one record per key in a segment, the scheduler can use:

```text
estimated garbage bytes = HLL distinct-key estimate * record size
```

If a segment contains several physical versions of one key, HLL reports one distinct garbage key
even though several records may be garbage. A segment-level average bytes per unique key can improve
the estimate, but exact byte counts still require the record index.

GC selection has two thresholds. The first is approximate and only selects candidates; the second
is exact and authorizes physical work:

```text
routed operations
  -> per-segment HLL distinct-key estimate
  -> likely candidate
  -> exact metadata preflight
  -> physical GC, if the exact threshold is met
```

At a captured routing frontier, exact preflight performs a streaming merge of the candidate
segment's key-sorted record index and its `(segment, key, operation_lsn)` delta rows. This first
removes Bloom false positives, then applies operation order and shard rules to distinguish a key
that merely exists in the segment from an operation that actually invalidates a physical record.
It calculates exact live, retired, and expired bytes from record lengths. The temporary per-key
state is discarded after preflight; it is not maintained as a global latest-value index.

This does not require a RocksDB lookup for every key. The preflight is bounded by one selected
segment and reads only metadata. Payload bytes are read after the segment has passed the exact
threshold.

### HLL checkpoints and error handling

An HLL is scheduling state, not correctness state. Its estimate includes Bloom false positives and
has normal statistical error. Both can trigger an early exact preflight, but neither can authorize
reclamation.

After an exact preflight, Strata records:

- The exact garbage bytes verified through the captured routing frontier.
- A rebuilt HLL containing keys whose records were exactly verified as invalidated through that
  frontier, plus its cardinality estimate.

If exact garbage does not cross the physical collection threshold, Strata atomically publishes the
verified summary, routing frontier, and rebuilt HLL as the new scheduling checkpoint. Future
scheduling adds HLL growth after that checkpoint to the exact garbage baseline. Rebuilding removes
Bloom false positives observed before the checkpoint while retaining verified invalidated keys, so
later operations for already-garbage keys do not inflate the estimate again. Operations routed
after the captured frontier must be accumulated separately during the rebuild and merged when the
new checkpoint is published. If physical GC succeeds, the source segment and its HLL are deleted,
so no rebuild is needed.

New distinct Bloom false positives can still accumulate after every rebuild. For example, at 32,768
distinct routed keys per second and a per-segment false-positive probability of `2^-20`, one cold
segment receives about 2,700 distinct false matches per day. A 10% threshold on a 16,384-record
segment could therefore cause an unsuccessful exact preflight about every 14.6 hours in that
worst-case workload.

The scheduler must use adaptive hysteresis after an unsuccessful preflight. It should record the
exact yield and consecutive false-admission count, require a larger minimum HLL increase, and apply
a per-segment cooldown before retrying. Repeated misses should increase the threshold or cooldown;
a useful exact result should reduce or reset it. Hysteresis affects scheduling latency only and can
never bypass exact preflight. Shadow mode must measure the real distinct-key rate, false-admission
frequency, and metadata bytes read per preflight before choosing these values.

The initial design assumes one shard. With multiple shards, a later extension can route puts using
`(key, shard)` while retaining key-only routing for tombstones. This requires additional filter and
HLL namespaces and should be added only if cross-shard amplification is material.

## Operation Semantics

### Puts and tombstones

A later put retires an older record for the same key and shard. A tombstone retires applicable older
records for the key. These transitions can be established locally by comparing the segment's record
index with later routed operations.

The same key may exist in several segments due to overwrites, multiple shards, or GC relocation.
Routing to all matching segments is intentional. Each segment independently determines whether its
physical copy remains live.

Many blob workloads use a content-addressed hash as the key and are predominantly insert-once, so
true overwrites and deletions may be rare. That reduces the amount of real garbage but does not
automatically reduce Bloom or HLL traffic: if every first-time put is routed as a possible
invalidation, a stream of unique content hashes produces the highest distinct false-positive growth.
If the foreground index or API contract can reliably identify an insert-only put, the router may
exclude it from invalidation routing. This decision must come from exact foreground knowledge or an
immutable-key contract, never from a Bloom result. The shadow prototype should measure total
distinct routed puts, actual overwrites, and tombstones separately.

### Lifetimes and expiration

Expiration is more difficult because a record may inherit lifecycle state that was set before the
record was written. A segment-local history cannot infer that older state unless the effective
lifecycle is stored with the record or remains available elsewhere.

The safest rollout is:

- Initially use Bloom-routed accounting for overwrite and tombstone garbage.
- Treat records with unknown lifecycle state as live.
- Keep the existing lifecycle accounting path until segment indexes include sufficient lifecycle
  information to classify expiration locally.

### GC relocation

Relocation operations contain exact source and destination record locations and do not need Bloom
routing. Publication must remain conditional on the source still being the current physical mapping.
If a foreground overwrite or tombstone wins the race, GC must not make its stale copy current.

### Shard deletion

Shard deletion is also better handled directly. Segment ownership and record-index shard information
can identify affected segments without a key-membership search. Mixed segments require an exact
record-index scan; whole shard-owned directories may continue using whole-directory deletion.

## Segment Sealing

A record can be invalidated while its segment is still open and before its immutable filter exists.
The sealing process must close this gap:

1. Seal and sync the segment and its exact record index.
2. Build and sync the segment Bloom filter.
3. Replay accounting operations that occurred while the segment was open.
4. Route matching operations to the new segment.
5. Publish the filter and its routing coverage atomically.

A segment cannot be selected for GC until this backfill is complete.

This makes 1 GiB segments attractive: they bound durability pauses, filter construction, backfill,
exact preflight, and physical GC work to the same unit.

## Accounting Frontiers

The current `accounted_lsn` represents a global prefix whose key state has been fully materialized.
The proposed design replaces this GC dependency with more specific coverage:

- The routing frontier says how far the ordered accounting log has been processed.
- Segment coverage says that a particular segment has received matching operations through that
  frontier, including seal-time backfill.
- The verified frontier records the prefix used by the segment's last exact preflight.

GC needs complete evidence for the selected segment, not globally materialized state for every key.

The router may lag foreground writes. An unseen overwrite or tombstone causes GC to copy more stale
data, rather than discard a live record, because unknown records default to live. GC must still
reconcile operations that arrive while it is copying and validate relocation mappings before
deleting the source segment.

The existing `accounted_lsn` gate should remain during the shadow rollout until these invariants are
validated for every operation type.

## Capacity Estimate

For 200 TiB of 64 KiB blobs stored in 1 GiB segments:

| Item | Estimate |
|---|---:|
| Segments | 204,800 |
| Records per full segment | 16,384 |
| Total record memberships | 3.36 billion |
| Bloom-filter memory with 20 hashes | 11.3 GiB |
| Expected unrelated matches per lookup | 0.2 segments |
| Flat transposed lookup | 500 KiB per key |
| Lookup traffic at 2 GiB/s of 64 KiB operations | 15.6 GiB/s |

The HLL scheduling state is comparatively small:

| HLL configuration | Approximate error | RAM for 204,800 segments |
|---|---:|---:|
| 1,024 one-byte registers per segment | 3.25% | 200 MiB |
| 4,096 one-byte registers per segment | 1.6% | 800 MiB |

Packed registers can use less memory, and mostly untouched segments can remain in a sparse encoding.
The HLLs are rebuildable scheduling state and can be checkpointed rather than synchronously persisted
on every update.

A two-level group and segment index requires roughly twice the filter memory, approximately
22.6 GiB, but reduces typical lookup traffic to a few KiB per key. Budgeting about 32 GiB of RAM
provides space for filters, live masks, metadata, and rebuild overlap.

Those numbers assume one key-only membership namespace. Adding a separate `(key, shard)` namespace
for precise put routing approximately doubles filter memory again: about 22.6 GiB for a flat index or
45.2 GiB for both levels of a two-level index, before overhead. We should measure cross-shard false
matches before paying that cost everywhere; shard-scoped filters could be enabled only for mixed or
highly shared segments.

The exact record indexes are much larger but do not need to remain in RAM. Depending on their entry
size, they would consume roughly 200-300 GiB of SSD at full scale, which is about 0.1-0.15% of the
blob capacity.

## Storage Placement

For an HDD-backed blob store, the natural placement is:

```text
RAM:
  transposed Bloom-filter index, per-segment HLLs, and hot summaries

SSD:
  canonical Bloom filters, exact record indexes, routed histories, and manifests

HDD:
  blob segments and sequential GC input/output
```

The filter index must remain in RAM. Reading Bloom rows on demand from SSD would make routing
latency-sensitive and unpredictable.

## Correctness Model

The most important rules are:

1. Bloom membership can schedule work but cannot authorize reclamation.
2. HLL cardinality can schedule exact preflight but cannot authorize reclamation.
3. Missing or ambiguous state is treated as live.
4. Exact classification uses complete keys and physical record locations, not short hashes.
5. Segment GC is allowed only after seal-time routing coverage is complete.
6. Shard identity and operation order are checked when matching puts to records.
7. Lifecycle decisions use one consistent operation and epoch frontier.
8. GC reconciles newer operations before publication.
9. Source segments are deleted only after the new metadata root is durable.
10. Filters and record indexes are checksummed and tied to a specific segment generation.

These rules make Bloom false positives a performance problem rather than a data-integrity problem.

## Benefits

- Avoids repeatedly rewriting a global materialized base merely to discover garbage.
- Focuses exact accounting work on segments likely to release useful disk space.
- Makes work bounded by the 1 GiB segment size.
- Uses sequential SSD metadata reads before expensive HDD copying.
- Allows accounting, preflight, and physical GC to have separate resource budgets.
- Keeps filter state relatively small even at 200 TiB.
- Provides a path to remove GC's dependence on global major-compaction progress.

## Costs and Risks

- Routed operation histories can be large and need bounded retention.
- Hot keys present in many historical segments create real routing fanout.
- Unique insert-heavy workloads can create steady Bloom false-positive HLL growth even when real
  overwrites and deletions are rare.
- Group filters accumulate stale bits and require periodic rebuilds.
- Lifecycle inheritance cannot be removed from the existing accounting path without additional
  record metadata.
- Seal-time replay and GC reconciliation introduce new frontier logic.
- A segment-oriented history store still requires recovery and observability. Local history
  compaction may be added for file-count or read-amplification control, but is not required for the
  correctness path.
- The two-level transposed index consumes more memory than independent Bloom filters.

## Rollout Plan

1. Build per-segment filters and route operations in shadow mode.
2. Maintain per-segment HLL estimates, rebuild them after unsuccessful exact preflights, and compare
   predicted distinct garbage keys with exact results.
3. Compare Bloom candidates with the exact segments produced by the current accounting reducer.
4. Implement exact segment preflight and compare every classified range with the current GC overlay.
5. Enable local authority for overwrite and tombstone garbage while treating lifecycle-unknown
   records as live.
6. Add routing and segment-coverage frontiers, initially behind the existing `accounted_lsn` gate.
7. Validate concurrent writes during HLL rebuild, adaptive hysteresis, GC relocation, crashes,
   disk-full recovery, shard deletion, and epoch transitions.
8. Add lifecycle seeds and remove the remaining global accounting dependency only after sustained
   shadow equivalence.

## Recommendation

Build a shadow prototype using 1 GiB segments, one membership filter and HLL per segment, and a flat
transposed in-memory lookup. Use the existing accounting results as the correctness oracle.

The first milestone should answer these questions:

- Does observed routing fanout stay close to the predicted approximately 1.2 segments per key?
- How accurately does HLL growth predict exact garbage bytes without maintaining per-key state?
- Can exact segment-local preflight reproduce overwrite and tombstone garbage without RocksDB
  lookups?
- Is flat lookup already inexpensive enough, or is the two-level index justified?
- Under the expected content-addressed workload, what are the distinct routed-put, true-overwrite,
  tombstone, cold-segment HLL-drift, and false-preflight rates?
- How much SSD bandwidth and CPU does the sorted index/delta merge consume, and what hysteresis
  policy keeps false preflights within budget?

If those results are positive, the design offers a credible way to eliminate global accounting
major compaction from the normal GC path while retaining exact reclamation decisions.
