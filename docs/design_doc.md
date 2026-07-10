# 1. Overview

This document proposes Strata Storage, a segment based sliver store for Walrus storage nodes. Strata uses RocksDB for metadata and indexes, but stores large sliver payloads in append only segment files.

## 1.1 Summary

Walrus slivers should move out of the general purpose RocksDB value path and into a purpose built segment store.

The core idea is:

- Slivers are written into append only segment files.
- Slivers are physically grouped by their expected end epoch.
- The exact current sliver lifetime is stored in metadata, not inferred from the directory name.
- Deletes and extensions update metadata first.
- Background cleanup moves bytes around only when reclaiming space is worth the I/O.

RocksDB remains in the design, but it stores metadata and indexes rather than large sliver payloads.

This gives Walrus the cheap common case: if a blob naturally expires, the node can delete whole segment files or epoch directories. If a blob is extended, the foreground path only updates metadata. The physical bytes can stay where they are until GC decides whether moving them improves the disk budget.

## 1.2 Goals

The design aims to:

- Minimize disk I/O during garbage collection.
- Make natural expiry cheap.
- Keep foreground writes, deletes, and extensions cheap.
- Check whether a sliver exists without reading the payload.
- Stream sliver bytes directly from disk by file range.
- Handle frequent extensions without repeatedly copying the same sliver.
- Support live migration from existing RocksDB/BlobDB shard column families.
- Keep crash recovery understandable.
- Keep the first implementation small enough to review and ship.

## 1.3 Non Goals

This design does not try to:

- Replace RocksDB for all storage node metadata.
- Provide a generic transactional database.

# 2. Motivation

## 2.1 Slivers Are Not Ordinary Key-Value

Sliver data has a different shape from ordinary key value.

- Slivers are large.
- Slivers are immutable.
- Slivers have a known logical lifetime.
- Sliver reads should be streamable from disk by file range.
- Garbage collection should avoid unnecessary disk I/O on HDDs.

RocksDB is excellent for metadata. It is not ideal as the physical layout for large immutable sliver payloads. A generic LSM layout and BlobDB style value management can move live values even when the application already knows those values are mostly waiting for expiry.

Walrus knows each blob’s end epoch. The storage layout should use that signal directly.

## 2.2 End Epoch Is A Storage Signal

Walrus knows each blob’s end epoch. That is stronger information than a generic blob store has.

If a blob naturally expires, the cheapest cleanup path is not compaction. It is deleting whole files or directories. The storage layout should use end epoch as a physical placement and GC signal.

## 2.3 Core Principle

The simple design is:

```
store every sliver under its exact end epoch
delete the end epoch directory or the segment files in that dir when that epoch expire
```

That is excellent when most blobs expireurally. It becomes awkward when blobs are frequently extended, because every extension would imply moving payload bytes to a new epoch directory which creates unnecessary write amplification.

So, what we add on top for blob extensions is:

```
An extension updates metadata. It does not have to move bytes immediately. 
Where and when those bytes are moved is decided based on write amplification.
```

That keeps the extension path cheap. An extension updates metadata. It does not have to move bytes immediately. Where and when those bytes move is a GC policy decision based on write amplification and reclaim efficiency.

The end epoch directory name is a placement hint and a GC hint. It is not the source of truth.

# 3. Storage Architecture

Strata splits sliver storage into two layers:

```
RocksDB:
  sliver indexes
  lifecycle metadata
  segment manifests
  segment accounting
  migration state

Segment files:
  sliver payload records
```

Segments are append only while open and immutable after sealing. Segment files are sealed either periodically or based on size (size being the primary trigger).

## 3.1 Physical Layout

Each local disk is treated as a volume. Each volume has this shape:

```
volume 0/
  shard-99/
    epoch 1042/
      segment 000001.data
      segment 000001.manifest
      segment 000002.data
      segment 000002.manifest

    epoch 1043/
      segment 000010.data
      segment 000010.manifest

    spillover/
      segment 000900.data
      segment 000900.manifest
```

There are two placement classes.

| Placement Class | What Goes There | GC Model |
| --- | --- | --- |
| Exact epoch directories | New slivers under their current end epoch | Delete whole segments/directories when blobs expire naturally |
| Spillover | Repeatedly extended, mixed, or mostly live old data | Compact by garbage ratio and disk pressure |

New slivers are written under their exact end epoch. These directories are the common case GC path. If blobs expire naturally, the node can delete whole segments or whole epoch directories.

## 3.2 Spillover

Spillover is for data where exact epoch placement no longer pays off:

- Repeatedly extended blobs
- Segments whose live data is spread across many future epochs
- Segments that remain mostly live after their original epoch expires
- Data copied by GC where a future exact epoch would likely cause another move

Spillover is cleaned by garbage ratio and disk pressure, not by exact epoch.

## 3.3 Physical File Fanout

Strata uses **one active writer** for foreground sliver writes. This keeps the write path simple and performant as fsync drain throughput worsens with increasing number of files being drained on HDDs.

Because this writer is optimized for ingest, a sealed segment may contain slivs with many different end epochs. After an segment is sealed, it becomes eligible for background re-organization. The organizer reads ingested sealed segments and rewrites their records into retention segments grouped by end epoch. This organization can be delayed during heavy write periods without immediately increasing point read amplification.

```rust
Ingest Layout:
  1 segment
  mixed epochs
  optimized for foreground writes

Retention Layout:
  52 sealed segments organized by epoch
  optimized for shard removal, GC, and long term reads
```

If the disk is busy, Strata can postpone epoch based re-organization. The cost of falling behind is worse physical locality, not immediate read correctness or foreground write failure.

## 3.4 Epoch Locality And Why Pay This Rewrite

The above re-organization introduces a one time rewrite of slivers that move from ingest to retention layout. That is real write amplification. The reason it may be worthwhile is that the rewrite is controlled and purposeful:

- It happens at most once in the normal lifetime of a sliver.
- It is sequential and background.
- It can be paused during heavy writes.
- It improves epoch expiry locality.

This is different from generic compaction repeatedly moving live values because files overlap. The organizer is a deliberate conversion from write optimized layout to retention optimized layout.

# 4. Segment Format And Indexing

## 4.1 Sliver Record Format

Each record in a segment file is self describing:

```rust
struct SliverRecord {
    magic,
    version,
    shard_id,
    sliver_type,
    blob_id,
    generation,
    logical_end_epoch_at_write,
    payload_len,
    header_checksum,
    payload_checksum_or_chunk_checksum_root,
    payload_bytes,
}
```

Repeating the key in the data file costs space, but it makes the system easier to recover and inspect. If the index is lost or suspected to be wrong, the node can scan sealed segments and rebuild or verify metadata.

For full sliver reads, a payload checksum is enough. For streaming and range reads, chunk checksums are better because the node can verify chunks as they are served.

## 4.2 Blob Sliver Index

This is the hot read and blob operation index in RocksDB:

```rust
(blob_id, sliver_type)  -> SliverEntry
```

```rust
struct SliverEntry {
    payload_ref,
    generation,
    state,
    logical_end_epoch,
    extension_count,
    last_extended_epoch,
    checksum_metadata,
}

enum PayloadRef {
    Strata {
        segment_id,
        offset,
        len,
    },
    NotMigrated,
}

enum SliverState {
    Live,
    Tombstoned,
}
```

`NotMigrated` is used during live migration. It means Strata metadata already knows about the sliver, but the payload may still live in the old RocksDB shard column family.

## 4.3 Segment State Index

Each segment has durable metadata:

```rust
struct SegmentState {
    owner: SegmentOwner,
    segment_id,
    volume_id,
    path,
    placement_class,
    physical_epoch,
    state,
    write_offset,
    durable_offset,
    sealed_len,
}

enum SegmentOwner {
    Store,           // mixed ingest segment
    Shard(ShardKey), // one shard generation's retention segment
}

enum SegmentFileState {
    Open,
    Sealed,
    Deleted,
}
```

`write_offset` is how far this process has appended. `durable_offset` is how far the segment is known to be crash safe. For sealed segments, `sealed_len` should match the final durable length.

```rust
segment/state/{owner}/{segment_id} -> SegmentState
```

`SegmentOwner::Store` is not a logical shard. In particular, it is distinct from the default
logical `ShardKey { id: 0, generation: 0 }`, which may own retention segments and may be dropped
like any other shard generation.

## 4.4 Segment GC Accounting Overlay

Each segment tracks enough accounting for GC to make cheap decisions:

```rust
struct SegmentGcSummary {
    total_bytes,
    live_bytes,
    retired_bytes,
    expired_bytes,
    live_ref_count,
    unknown_lifetime_bytes,
    unknown_lifetime_ref_count,
    min_live_end_epoch,
    max_live_end_epoch,
    future_epoch_histogram,
    extension_count_histogram,
}

struct SegmentGcOverlay {
    summary: SegmentGcSummary,
    expired_ranges,
    retired_ranges,
    lifetime_hints,
}
```

```rust
segment_gc_overlay/{segment_id} -> SegmentGcOverlay
```

The most important fields are:

- `live_ref_count`: if zero, the segment can be deleted.
- `retired_bytes` and `expired_bytes`: cheap garbage-ratio inputs without scanning the file.
- `future_epoch_histogram`: helps choose exact future epoch vs spillover when relocation happens.

This avoids scanning payload files just to decide what GC should do next.

# 5. Foreground Operations

## 5.1 Write Path

For a new sliver:

1. Append the sliver record to the active and unsealed segment.
2. Publish metadata in RocksDB.
3. Periodically fsync dirty segment files and advance durable offsets.

## 5.2 Visibility vs Durability

The write path should not fsync every sliver. That would destroy throughput on HDDs. Instead, Strata separates where the bytes are from how far the segment is crash safe.

On write:

```rust
append sliver bytes to segment file

RocksDB write batch:
    update Blob Sliver Index
    update Segment State/Stats Index
```

Reads can find the sliver immediately.

Separately, a background sync task runs:

```rust
periodically or when dirty bytes exceed a threshold:
    fsync dirty segment files

    RocksDB write batch:
        update segment durable_offset
        fsync db wal
```

Example:

```
segment 7 write_offset   = 150 MiB
segment 7 durable_offset = 120 MiB
```

Bytes before 120 MiB are safe after crash. Bytes after 120 MiB may exist now, but recovery must not assume they survived.

## 5.3 Read Path

The read path is index first:

```rust
read_sliver(key):
    entry = blob_sliver_index.get(key)

    if entry is tombstone:
        return not_found

    if entry.payload_ref is Strata:
        return stream_from_segment(entry.segment_id, entry.offset, entry.len)

    if entry.payload_ref is NotMigrated and legacy fallback is enabled:
        return read_from_legacy_rocksdb_cf(key)

    return not_found
```

For normal reads, the caller does not need to know whether the segment is in an exact epoch directory or spillover. Because the index points to a file range, reads can be streamed directly from disk. The storage API can expose both:

```rust
get_sliver(key) -> bytes
stream_sliver(key, range) -> stream
```

The implementation should begin with a portable `pread` thread pool backend. `io_uring` can be added later as a Linux optimization if benchmarks show it improves p99 latency or CPU cost.

## 5.4 Delete Path

Deletes are metadata first.

For each affected sliver:

```rust
RocksDB write batch:
    mark sliver tombstoned
    update GC overlay summary/ranges
```

The payload bytes stay where they are until GC can reclaim them cheaply.

The tombstone is important during migration. If a sliver still exists in the old RocksDB shard CF, the Strata tombstone prevents legacy fallback from returning it and prevents background migration from copying it later.

## 5.5 Extension Path

Extensions are also metadata first.

For each affected sliver:

```rust
RocksDB write batch:
    update logical_end_epoch
    increment extension_count
    update last_extended_epoch
    update GC overlay lifetime summary
    update future epoch histogram
```

The extension path does not move bytes.

This remains true during migration. A sliver may still physically live in the old RocksDB shard CF while Strata metadata already records:

```rust
logical_end_epoch = 1120
extension_count = 1
```

When background migration later copies that sliver, it uses the current lifecycle metadata, not the old lifetime at original write time.

# 6. Background Cleanup

Background cleanup is the system that reclaims disk space and improves physical layout.

The goal is not to keep every segment perfectly organized. The goal is to move bytes only when the reclaim or layout benefit is worth the I/O.

Cleanup has three kinds of actions:

1. Delete files that have no live refs.
2. Rewrite sparse or poorly placed segments when the reclaim tradeoff is good.
3. Reorganize sealed ingest segments into epoch based retention segments.

Foreground deletes and extensions do not move payload bytes. They update metadata and segment accounting. Cleanup handles the physical consequences later.

## 6.1 Cleanup Triggers

Cleanup can be triggered by several signals:

- An epoch boundary is reached.
- A segment’s `live_ref_count` becomes zero.
- A segment’s garbage ratio crosses a threshold.
- Sealed bytes grow beyond a target size or age.
- Disk free space drops below a threshold.

These triggers all feed the same cleanup planner. They are not separate systems.

## 6.2 Empty Segment Cleanup

This is the cheapest cleanup action. If a segment has no live refs, it can be del regardless of its physical epoch.

```rust
if segment.live_ref_count == 0:
    mark segment deleting
    unlink segment file
    mark segment deleted
```

## 6.3 Epoch Cleanup

Epoch cleanup runs when epoch `E` is reached. It looks at segments physically placed under `epoch-E`.

```rust
if segment.live_ref_count == 0:
    delete segment

else if segment.live_bytes is small enough:
    move live refs to exact future epochs or spillover
    delete old segment

else:
    reclassify segment class as spillover
    keep payload bytes in place
```

The last case is important. A mostly live segment should not be rewritten just because its original epoch expired. If the bytes are moved, destination choice for byte movement is expiry aware:

```rust
if sliver has low extension count and useful future end epoch:
  write to epoch_{logical_end_epoch}
else:
  write to spillover
```

## **6.4 Garbage Ratio** Cleanup

Garbage ratio cleanup looks for segments where retired/expired bytes are high enough that rewriting live bytes is worth it.

This applies to:

- future epoch segments with many user deletes
- expired epoch segments with some pinned live data
- segments made sparse by shard removal
- spillover segments with accumulated tombstones

### 6.4.1 Cleaning up sparse live segments

**Targets:** segments with very little live data, regardless of placement class.

```
source segment:
  total: 1000 MiB
  live:    30 MiB
  dead:   970 MiB

action:
  copy 30 MiB live data
  update indexes
  delete source segment
```

Destination choice remains expiry aware:

```rust
if sliver has low extension count and useful future end epoch:
    write to epoch_{logical_end_epoch}
else:
    write to spillover
```

### 6.4.2. Cleaning up moderately live segments

**Targets:** multiple sparse or moderately sparse segments that contain live slivers for the same useful future end epoch.

```
target: epoch 1120
destination segment size: 1000 MiB

donor A has 300 MiB live for epoch 1120
donor B has 250 MiB live for epoch 1120
donor C has 400 MiB live for epoch 1120

action:
  copy 950 MiB into one dense epoch 1120 segment
  update indexes
  delete any donor segment that becomes empty
```

If live data is spread across many future epochs, pack into spillover instead.

### 6.4.3. Packing up almost packed live segments

**Targets:** build dense output segments from one or more donor segments.

```
donor A has 800 MiB live and 200 MiB garbage
donor B has 400 MiB live and 600 MiB garbage

action:
  copy 200 MiB selected live data from B to A
  make donor segments sparser
  make target segments denser
```

This is useful because it can make one output segment full while making source segments better candidates for later sparse drain.

# 7. Live Migration From RocksDB Shard CFs

Migration should be a reusable live migration layer around RocksDB column families, not special logic embedded deep inside Strata.

The rollout:

1. Deploy a binary that can read from Strata and the old RocksDB shard CFs.
2. Switch new sliver writes to Strata.
3. Keep old RocksDB shard CFs available for fallback reads.
4. Migrate old slivers one shard at a time in the background.
5. Verify that a shard is fully served from Strata.
6. Disable fallback reads for that shard.
7. Drop the old RocksDB shard CF later in a throttled cleanup job.

## 7.1 Read During Migration

```rust
read_sliver(key):
    entry = strata_index.get(key)

    if entry is live and has Strata payload location:
        return read_from_strata(entry.location)

    if entry is tombstone:
        return not_found

    if shard still has legacy fallback enabled:
        return read_from_rocksdb_source_cf(key)

    return not_found
```

## 7.2 Write During Migration

After the migration aware binary is enabled, new sliver writes go to Strata. The old RocksDB shard CF should not receive new sliver payloads.

Writes use:

```rust
append payload to segment file

RocksDB write batch:
    publish Strata index updates
    update segment write_offset
    update segment accounting
```

If the node crashes before the RocksDB write batch commits, the new bytes are unreachable and can be cleaned up later. If the batch commits but the file was not fsynced, recovery truncates the open segment to `durable_offset` and removes affected refs.

## 7.3 Background Migration

The migration job scans the old RocksDB source CF slowly:

```rust
for (key, payload) in source_sliver_cf.scan_from(cursor):
    migrate_one_key(key, payload)

migrate_one_key(key, payload):
    entry = strata_index.get(key)

    if entry is tombstone:
        skip

    if entry already has a Strata payload location:
        skip

    lifecycle = current lifecycle from Strata metadata or canonical blob metadata

    write payload to Strata segment

    RocksDB write batch:
        publish Strata payload location
        preserve current lifecycle metadata
        advance migration cursor
        update segment write_offset
```

The migration write is idempotent. It is safe to scan the same source range again after a crash.

## 7.4 Cutover

When the background copy reaches the end of a shard:

1. Mark the shard `backfill_complete`.
2. Verify with counters and sampling.
3. Disable fallback reads for that shard.
4. Keep the old source CF around for a short safety window.
5. Drop the old source CF in a throttled cleanup job.

Dropping the old CF can be expensive on HDDs because RocksDB may need to delete many SST and blob files. Migration should not trigger many CF drops at once.

# 8. Crash Recovery

Recovery has three jobs.

## 8.1 Open Segment Recovery

For each open segment:

```rust
durable = segment.durable_offset
actual = file.len()

if actual > durable:
    truncate file to durable

remove or invalidate refs with offset + len > durable
resume appending at durable
```

But after a **process crash**, bytes beyond durable_offset may still be present in the kernel page cache and visible in the file. If the file contains complete, checksummed records beyond durable_offset, we can scan and recover them.

Potentially, on restart:

```rust
for each open segment:
    scan from durable_offset to file length

    while next record is complete and checksum valid:
        if RocksDB has matching index entry:
            keep it
            advance recovered_offset

        else          stop or mark as orphan, depending on policy

    truncate file to recovered_offset
    update durable_offset or recovered_offset metadata
    remove index refs beyond recovered_offset
```

The invariant is that after a crash, Strata must never serve a reference to bytes beyond the recovered durable offset.

## 8.2 Sealed Segment Recovery

For sealed segments, the manifest and RocksDB segment state must agree. If a sealed segment was published, its records are treated as durable. If a seal was never published, recovery treats the file as an open segment and truncates it to the durable offset.

## 8.3 Index Verification

Because records are self describing, the node can scan sealed segments and rebuild or verify indexes. This should be slow but reliable. It is the fallback for index corruption, partial migration, and operator mistakes.

# **9. Capacity Expansion Options**

Below we discuss two possible but different ways to increase storage capacity of a storage node leveraging this new storage system:

## **9.1 NAS Cold Tier**

New user data is always written to local disk. NAS is only used by background GC as a cold capacity tier. Background cleanup will migrate segment files out to the cold tier.

```rust
write path:
  user sliver  -> local segment  -> local fsync/index publish

cold migration:
  sealed local segment  -> few reads over time (cold) -> copy to NAS  -> update metadata

read path:
  index lookup  -> local segment or NAS cold segment
```

NAS would host immutable sealed segments which gets skipped by most of background GC. Background cleanup would only delete whole NAS segments when they become fully dead. This is operationally simpler because the Walrus node still has:

```rust
one metadata authority
one RocksDB/index set
one GC planner
one routing model
one recovery model
```

Risks:

```rust
cold random reads are slow
NAS availability affects cold reads
clanup must budget NAS copy and/or delete bandwidth
cold tier health must be explicit
```

Mitigations:

```rust
only move sealed, cold, long lived segments
add local nvme cache for cold reads
bound concurrent cold reads
do not put ingest durability on NAS
```

## **9.2 Remote Storage Instances**

The main node shards slivers across multiple storage services. Each storage service owns its own local sliver store:

```rust
storage instance:
  RocksDB indexes
  epoch directories
  spillover
  segment manifests
  GC planner
  recovery
```

Routing would be something like:

```rust
(blob_id, shard_id, sliver_type)
   > virtual storage partition
   > storage_instance_id
```

This gives true horizontal scaling, but it adds route tables, migration protocols, RPC read write paths, per instance health, cross instance rebalancing, and more failure states.

# 10. Summary

The clean design is not exact end epoch directories by themselves. That breaks down when extensions are frequent. The clean design is also not a generic WAL KV store. That leaves too much expiry information unused.

Strata is an epoch aware segment store backed by RocksDB metadata:

- Write new data where it is likely to expire.
- Keep exact expiry in metadata.
- Make extension cheap.
- Keep physical files bounded by epoch/volume, not shard.
- Stream reads directly from segment ranges.
- Let background GC move bytes only when the economics are good.
- Migrate from existing RocksDB shard CFs one shard at a time.

This gives Walrus the cheap common case without forcing the system to copy sliver bytes every time blob lifetimes change.

# Addendum 1: Two WALs And Event Cursor Safety

Today, Walrus sliver writes and the event cursor both live in RocksDB’s durability domain. The current certified-event flow is roughly:

1. Process the certified blob event.
2. Update blob metadata in RocksDB.
3. Sync missing metadata/slivers.
4. Write slivers into RocksDB shard CFs.
5. Mark the event handle complete.
6. Advance the event cursor in RocksDB.

**Because the sliver writes and the event cursor are both backed by RocksDB, the event cursor cannot get ahead of the storage writes it depends on**. This is true even though we have turned off WAL sync in RocksDB because the default WAL recovery mode is **Point in time** which meanthat during a crash we always recover a fully consistent WAL prefix and ignore the whole tail even if some writes might be recoverable. 

Strata changes that. 

With Strata, sliver metadata is still in RocksDB, but payload bytes are in segment files. RocksDB cannot atomically commit its WAL together with regular file appends. That means we need an explicit rule to prevent this failure:

1. Event `E` writes sliver bytes to Strata segment files.
2. RocksDB index is updated.
3. Walrus advances the latest processed event cursor.
4. Node crashes before Strata segment bytes are durable.
5. On restart, event `E` is not replayed because the cursor advanced.
6. But the payload bytes may be missing.

The idea is **gate event cursor update with a Strata based LSN marker.**

Strata will return a logical sequence number for every payload write:

```rust
put_sliver(...) -> strata_lsn // lsn of this write
durable_lsn() -> strata_lsn // latest durable lsn
```

The `strata_lsn` is a global Strata sequence number. All Strata writes with `lsn <= durable_lsn()` are known to be durable. Walrus then uses this rule:

```rust
after processing event E:
    strata_lsn_E = put_sliver()
    pending_event_durability.insert(strata_lsn_E, event_cursor_for_E)

background:
    durable = strata.durable_lsn()
    mark all pending events with lsn <= durable as complete

```

# Addendum 2: Blob-Version LSM As The Accounting Engine

This addendum records an alternative design direction discussed after the initial RocksDB-backed metadata plan. The goal is to avoid making accounting a separate component that repeatedly performs random metadata lookups over blob keys while still preserving the important correctness properties around live refs, GC summaries, L0 organization, and GC.

The short version:

```
Do not ask RocksDB compaction to discover accounting side effects.
Instead, consider making Strata's blob-version index a small Strata-owned LSM.
Then the blob-version LSM merge/compaction stage can be the accounting engine.
```

This is not required for the first implementation, but it is important enough to keep as a design option because it may give a cleaner long-term architecture.

## A2.1 Motivation

The original design keeps foreground payload writes append-only and puts blob metadata in RocksDB. L0 then needs to reorganize sealed ingest segments into retention layout:

```
ingest segment:
  many shards
  many expiration epochs
  optimized for append throughput

retention layout:
  shard / exact_epoch
  shard / spillover
  optimized for expiry, shard removal, and GC
```

The hard part is that a sealed ingest segment alone does not tell L0 whether each physical record is still live or what its latest expiration route is. A naive L0 compactor would scan records and perform one blob metadata lookup per record:

```
for record in S.data:
    entry = blob_index.get(record.key)
    decide whether record is live
    decide shard and lifecycle route
```

That is the wrong shape:

- it duplicates work already done by blob-version/accounting logic
- it turns a sequential compaction into many random metadata lookups
- it couples L0 byte movement to the current blob-index read path
- it makes stats correctness depend on per-record lookups during background work

The current design solves this by having accounting materialize segment-local GC overlay state and
ordered ref events:

```
segment_gc_overlay/{segment_id} -> {
  dead: Vec<SegmentGcRecordRange>,
  lifetimes: Vec<SegmentGcLifetimeRange>,
}
segment_ref_events/{segment_id}/{lsn}/{offset} -> SegmentRefEvent
```

Then L0 can scan the data file and the segment-local overlay mostly sequentially. Ranges in
`dead` are skipped, ranges in `lifetimes` carry exact-epoch routing hints, and ranges absent from
both remain copy-eligible with unknown lifetime.

The newer idea asks whether the separate accounting component can disappear if the blob-version index itself is implemented as a controlled LSM owned by Strata. In that design, accounting is not a separate random lookup pass. It is the side effect of merging ordered blob-version deltas into current blob state.

## A2.2 Why RocksDB Compaction Is Not Enough

RocksDB has compaction filters, merge operators, and event listeners. These are useful, but they are not a clean fit for Strata accounting side effects.

A compaction filter can drop or modify the key/value currently being compacted, but it is not a reliable "the tombstone matched this older put ref, now update that segment stat" callback. If multiple versions of a key exist in a compaction input, the filter is not necessarily invoked on the old versions in a way that exposes the exact logical transition Strata needs.

A merge operator can fold operands for the same key, but it produces a value for that key. It is not a transaction mechanism for updating other metadata keys such as:

```
segment_gc_overlay summary
segment_gc_overlay
segment_ref_events
source segment state
destination segment state
GC eligibility
```

RocksDB event listeners expose file/job level events. They do not expose a durable, application-owned transaction boundary for per-key obsolete-version decisions.

The deeper issue is ownership of the commit protocol. Strata needs transitions like this to be one logical publish:

```
blob K old ref is retired
segment S live_bytes decreases
segment S live_ref_count decreases
segment_gc_overlay for S marks the ref range dead
accounting/manifest progress advances
```

If these are side effects from RocksDB's internal compaction, Strata does not cleanly control whether they commit atomically with the compaction's manifest change. If Strata owns the blob-version LSM, it controls that publish point.

## A2.3 Core Idea

Build a Strata-owned blob-version LSM for blob metadata and lifecycle deltas.

Foreground operations append ordered blob-version deltas:

```rust
enum BlobVersionDelta {
    Put {
        key: BlobKey,
        payload_lsn: StrataLsn,
        payload_ref: PayloadRef,
        lifecycle: Option<BlobLifecycle>,
    },
    Tombstone {
        key: BlobKey,
        tombstone_lsn: StrataLsn,
    },
    SetLifetime {
        key: BlobKey,
        lifetime_lsn: StrataLsn,
        lifecycle: Option<BlobLifecycle>,
    },
    DropShard {
        shard: ShardKey,
        drop_lsn: StrataLsn,
    },
}
```

The blob-version LSM merge/compaction process groups by key and folds deltas into current state:

```rust
old current state for key
+ ordered deltas for key
=> new current state for key
=> ref retire/live/lifecycle side effects
=> segment GC summary deltas
=> segment_gc_overlay and segment_ref_event batches
```

This lets the system discover transitions sequentially:

```
Put(K -> S_ref) followed by Tombstone(K)
  => retire S_ref

Put(K -> S_ref1) followed by Put(K -> S_ref2)
  => retire S_ref1
  => make S_ref2 current live

Put(K -> S_ref) followed by SetLifetime(K, epoch 120)
  => keep S_ref live
  => update lifecycle route for S_ref

DropShard(A)
  => retire every current live ref owned by shard A
```

The important difference from RocksDB compaction is that this is Strata's compaction. Strata chooses the input runs, produces the output run, produces accounting deltas, and publishes all of it under a Strata manifest protocol.

## A2.4 Current Read View Must Stay Up To Date

The blob-version index must answer user reads at all times. That means the logical current read view cannot wait for background compaction.

The LSM read path must merge:

```
base/current run(s)
newer delta run(s)
memtable or foreground delta buffer
```

So when a user reads key `K`, the system sees the latest committed `Put`, `Tombstone`, or lifecycle-relevant state even if background compaction has not folded all deltas into the base run yet.

This is the key distinction:

```
current blob index:
  must be logically up to date for reads

compacted blob-version base:
  may lag behind

segment GC summary/ref events:
  either update synchronously on foreground operations
  or lag if they are derived only by blob-version compaction
```

If GC summaries are also updated synchronously by foreground operations, then GC can trust them immediately. If GC summaries are derived later by LSM compaction, GC needs a way to distinguish known live/retired refs from unresolved refs.

## A2.5 PayloadRef And Ingest Shard Placement

Keep `RecordRef` as a physical byte range:

```rust
struct RecordRef {
    segment_id: SegmentId,
    offset: u64,
    len: u64,
}
```

Placement context belongs in a higher-level payload ref:

```rust
enum PayloadRef {
    Ingest {
        record_ref: RecordRef,
        shard: ShardKey,
    },
    Retention {
        record_ref: RecordRef,
    },
}
```

During ingest, an append-only segment may contain records from many shards. The shard must be available without a per-record blob-index lookup when L0 later scans the segment. There are two acceptable ways to ensure that:

```
record header stores shard
or PayloadRef::Ingest stores shard and L0 has a segment-local state/log that includes it
```

The cleaner physical model is:

```
RecordRef:
  segment_id + offset + len only

PayloadRef::Ingest:
  RecordRef + shard

PayloadRef::Retention:
  RecordRef only
```

After L0 moves a record from ingest to retention, the destination segment placement metadata supplies the shard and expiration route:

```rust
struct SegmentPlacement {
    class: PlacementClass,
}

enum PlacementClass {
    Ingest,
    RetentionExactEpoch {
        shard: ShardKey,
        epoch: Epoch,
    },
    RetentionSpillover {
        shard: ShardKey,
    },
}
```

Do not rely only on a filesystem path to infer placement. Persist it in segment metadata.

## A2.6 Segment GC Overlay In The LSM Model

If shard is stored in the record header or in `PayloadRef::Ingest`, segment-local GC metadata does
not need to repeat shard.

It only needs the facts that can change after the physical put:

```rust
struct SegmentGcOverlay {
    dead: Vec<SegmentGcRecordRange>,
    lifetimes: Vec<SegmentGcLifetimeRange>,
}
```

Unknown lifetime is represented as:

```rust
dead does not contain range
lifetimes does not contain range
```

L0 routes this to spillover:

```
dead contains record range      -> skip
lifetimes contains record range -> shard / exact_epoch
otherwise                       -> shard / spillover
```

Overlay and event updates should be batch-oriented, not one RocksDB row per physical record:

```
segment_gc_overlay/{segment_id}
segment_ref_event_batch/{segment_id}/{lsn_range_or_batch_id}
```

The write path should avoid millions of tiny metadata writes for large segments. Overlay merge
operands can pack:

```
dead range batches
lifetime update batches
```

Event batches can pack changes:

```rust
enum SegmentRefEvent {
    Retired {
        offset: u64,
    },
    LifecycleChanged {
        offset: u64,
        lifecycle: Option<BlobLifecycle>,
    },
    Mapped {
        from: PayloadRef,
        to: PayloadRef,
    },
}
```

The guiding rule:

```
segment_gc_overlay is optimized for L0 sequential reads
segment_ref_event_batch is optimized for append/fold by accounting or LSM compaction
```

## A2.7 Do We Need An Accounted LSN?

Not necessarily as a global gate.

An earlier design had:

```rust
accounted_lsn
```

with the invariant:

```
L0 may compact ingest segment S only when accounted_lsn >= S.max_lsn
```

That is sufficient, but it may be too strong if the blob index is always current and GC is allowed to operate on older or partial views.

The more precise requirement is:

```
GC and L0 must not delete or fully drain a segment while it contains refs whose
live/dead/mapped status is still unknown to the metadata they are using.
```

There are two ways to express that.

Option A: watermark/fence.

```rust
blob_version_view.materialized_through_lsn >= segment.max_lsn
```

This says all mutations up to the segment's max LSN have been folded into the materialized segment-ref/accounting view.

Option B: per-segment pending counts.

```rust
struct SegmentGcSummary {
    total_bytes: u64,
    live_bytes: u64,
    retired_bytes: u64,
    expired_bytes: u64,
    live_ref_count: u64,
    unknown_lifetime_bytes: u64,
    unknown_lifetime_ref_count: u64,
}
```

Foreground writes immediately create physical occupancy:

```
append Put payload to S
S.pending_ref_count += 1
S.pending_bytes += len
```

Blob-version LSM compaction later resolves the pending ref:

```
if Put is current live:
    pending_ref_count -= 1
    live_ref_count += 1
    pending_bytes -= len
    live_bytes += len

if Put was overwritten/tombstoned before materialization:
    pending_ref_count -= 1
    retired_bytes += len
    pending_bytes -= len
```

Whole-segment deletion then requires:

```
live_ref_count == 0
pending_ref_count == 0
mapped_ref_count == 0
no open writer
no staged L0/GC copy references the segment
```

This avoids needing a global `accounted_lsn` as a hard gate. It preserves the same safety property through explicit unknown/pending accounting.

## A2.8 GC Can Run On Older Views

GC does not need to block on the freshest possible blob-version compaction view.

It can safely run on an older committed view if it is conservative:

```
GC may reclaim refs proven dead in its view.
GC may ignore newer garbage it has not seen yet.
GC must not delete refs whose status is unknown.
```

This is normal snapshot-style GC. The danger is only this invalid rule:

```
materialized blob index has no live refs to S
therefore S is deletable
```

That rule is unsafe if the materialized view might not include newer puts into S. It becomes safe only if either:

```
the view covers all refs in S
or S.pending_ref_count == 0 and all GC summaries are current
```

If the current blob index and segment counters are updated synchronously on every foreground mutation, then GC can trust the segment counters. If segment counters are compaction-derived, GC must respect pending counters or a materialized-through watermark.

## A2.9 L0 Organization With A Blob-Version LSM

L0's job is to move sealed ingest records into retention layout without doing per-record blob lookups.

It should still be mostly sequential:

```
scan S.data sequentially
scan/fold segment_gc_overlay for S sequentially
skip retired and expired ranges
route live refs by shard + lifecycle
write destination retention segments
publish precise ref mappings
```

Routing:

```
source shard:
  record header or PayloadRef::Ingest.shard

lifecycle:
  segment_gc_overlay.lifetimes

target:
  Some(lifecycle) -> shard / exact_epoch
  None            -> shard / spillover
```

The move should rewrite a precise historical payload ref:

```rust
struct MapPayloadRef {
    payload_lsn: StrataLsn,
    from: PayloadRef,
    to: PayloadRef,
}
```

For ingest to retention:

```rust
MapPayloadRef {
    payload_lsn,
    from: PayloadRef::Ingest {
        record_ref: source_ref,
        shard,
    },
    to: PayloadRef::Retention {
        record_ref: destination_ref,
    },
}
```

This map operation must not blindly set the current blob head. It must rewrite the exact historical payload ref if that ref is still the one associated with the payload LSN.

In normal publish flow, L0 should prove under the relevant metadata barrier or manifest transaction that all mapped refs still match. If a map op would no-op because the source ref no longer matches, that should be treated as publish failure or metadata invariant failure, not as success.

Stats movement is deterministic only if MapRef preconditions are proven:

```
for each copied ref:
    source live stats -= ref
    destination live stats += ref
    MapPayloadRef(source_ref -> destination_ref)
```

If the storage primitive cannot report whether a conditional map applied, the publish path must rely on its pre-validation. A failed precondition is not a benign no-op for stats.

## A2.10 L0 Copy/Publish Protocol

The copy phase should not block foreground accounting/index updates:

```
claim source S:
  Sealed -> L0Copying(job_id)

copy phase:
  no long metadata barrier
  scan source segment
  scan segment GC overlay
  write staged destination segments
  fsync destination segments
```

The publish phase is short and serialized with whatever mutates blob-version refs and GC overlay state:

```
publish phase:
  acquire short metadata/accounting barrier
  re-read segment_ref_events/state since copy started
  validate copied refs still live and still refer to source
  publish map refs
  publish destination segment states/stats
  update source segment state/stats
  release barrier
```

For the first implementation, use the conservative rule:

```
if any copied ref changed during the copy window:
    discard staged destination segments
    reset source to Sealed
    retry later
```

Later implementations can reconcile individual changes:

```
Retired during copy:
  do not publish the copied ref live

LifecycleChanged during copy:
  reroute to the new target
  or abort if reroute would complicate the current publish
```

Crash recovery:

```
S in L0Copying(job_id):
  reset S to Sealed

destination T in Staging(job_id):
  delete file and staged metadata

published T:
  keep

source marked Compacted/Deleted:
  finish cleanup if needed
```

## A2.11 Staged Manifest Publish For Large Batches

Large segments may produce too many map refs and segment-ref updates for one small metadata batch.

The heavy data should be staged in chunks:

```
l0_move_batch/{job_id}/{batch_id} -> [MapPayloadRef, ...]
segment_ref_event_batch/{job_id}/{batch_id} -> [SegmentRefEvent, ...]
stats_delta_batch/{job_id}/{batch_id} -> StatsDelta
```

The final visibility transition should still be atomic:

```rust
struct L0PublishCommit {
    job_id: JobId,
    source_segment: SegmentId,
    destination_segments: Vec<SegmentId>,
    move_batches: Vec<BatchId>,
    stats_delta_batches: Vec<BatchId>,
}
```

Publish protocol:

```
1. write staged destination segment files
2. fsync destination segment files
3. write staged metadata batches that are not yet visible
4. acquire short metadata/accounting barrier
5. validate source refs against current segment-ref/blob-version state
6. atomically write commit marker + segment state transitions
7. release barrier
8. readers/accounting honor staged batches only after commit marker
```

This keeps write volume batch-oriented without making partially published movement visible.

## A2.12 Foreground-Synchronous GC Summary vs Compaction-Derived GC Summary

There are two viable implementation choices.

### Option A: Foreground Path Maintains Current Blob Index And GC Summary

Every user-visible mutation updates blob state and GC summary immediately:

```
Put new ref:
  blob_index[key] = new_ref
  S.live_ref_count += 1
  S.live_bytes += len

Overwrite old ref:
  blob_index[key] = new_ref
  old_segment.live_ref_count -= 1
  old_segment.live_bytes -= old_len
  new_segment.live_ref_count += 1
  new_segment.live_bytes += new_len

Tombstone:
  blob_index[key] = tombstone
  old_segment.live_ref_count -= 1
  old_segment.live_bytes -= old_len

Lifetime extension:
  blob_index[key].lifecycle = new_lifecycle
  update segment lifecycle histograms or route state
```

Benefits:

- current read view and GC summaries are both up to date
- GC can trust GC summaries immediately
- no global `accounted_lsn` needed for deletion safety

Costs:

- foreground writes do more metadata work
- overwrite/tombstone paths need to find the old current ref
- more random metadata reads may be needed in the foreground path

### Option B: Foreground Path Appends Deltas, LSM Compaction Derives GC Summary

Foreground operations append deltas and possibly update a memtable/current overlay:

```
Put(K -> S_ref)
Tombstone(K)
SetLifetime(K, epoch)
```

The blob-version LSM read path remains current by merging overlays and runs. GC summaries are updated when LSM compaction folds deltas.

Benefits:

- foreground write path can be simpler and more append-oriented
- accounting side effects are derived sequentially by key-order compaction
- fewer random foreground reads for old refs

Costs:

- GC summaries may lag
- GC/L0 need pending counters or materialized view watermarks
- publish and recovery need a Strata-owned manifest protocol

The design conversation leaned toward this being attractive if Strata owns the blob-version LSM. However, it is a larger storage-engine commitment than using RocksDB for metadata.

## A2.13 One-Layer LSM Shape

The simplest custom LSM shape to consider is a one-layer or small-layer design specialized for blob versions:

```
mutable delta buffer
immutable delta runs
base/current run
manifest
```

Writes:

```
append to Strata WAL or delta log
insert into mutable delta buffer
return after required durability policy
```

Flush:

```
mutable delta buffer -> immutable sorted delta run
manifest adds delta run
```

Compaction:

```
base/current run + selected delta runs
  -> new base/current run
  -> segment GC overlay/ref event batches
  -> segment GC summary deltas
  -> new manifest generation
```

Manifest publish:

```rust
struct BlobVersionManifest {
    generation: u64,
    base_run: RunId,
    delta_runs: Vec<RunId>,
    segment_ref_batches: Vec<BatchId>,
    stats_delta_batches: Vec<BatchId>,
    materialized_ranges: Vec<LsnRange>,
}
```

The manifest commit is the atomic transition. Old runs remain valid until the new manifest is durable. Staged runs and staged accounting batches are ignored unless referenced by a committed manifest.

## A2.14 Relationship To Segment Ref Events

Even with a custom blob-version LSM, segment ref events are still useful.

They give L0 and GC a cheap way to reconcile changes that occur during a long copy:

```
copy_start_generation = current blob-version manifest generation
copy S
on publish:
  read segment_ref_events for S after copy_start_generation or copy_start_lsn
  if copied refs changed, abort or reconcile
```

Events should be append/batch-oriented:

```
segment_ref_event_batch/{segment_id}/{manifest_generation}/{batch_id}
```

They can be garbage collected after:

```
source segment is deleted
and no active L0/GC job needs the older generation
and recovery no longer needs the batch for manifest replay
```

## A2.15 Required Invariants

These invariants are the core of the design:

```
The logical blob-version read view is always current for committed user writes.

Every physical payload ref is in exactly one accounting state:
  pending
  live
  retired
  mapped
  deleted/unreachable

Unknown refs are represented explicitly as pending, not inferred from absence.

GC may reclaim only refs proven dead in the view it uses.

Whole segment deletion requires:
  no live refs
  no pending refs
  no mapped refs still requiring the source
  no open writer
  no staged copy job depending on the segment

L0 may copy without blocking foreground metadata updates.

L0 publish must be a short serialized metadata transaction.

MapPayloadRef rewrites a specific historical payload ref.
It does not blindly set the current blob head.

Stats movement during L0 publish is valid only when every MapPayloadRef
precondition has been proven.

Staged metadata is not visible until a manifest or commit marker makes it visible.

Destination retention placement is persisted in segment metadata, not only in
directory names.
```

## A2.16 Failure Cases To Keep In Mind

These are the cases the design must continue to handle:

```
overwrite during L0 copy
tombstone during L0 copy
lifecycle extension during L0 copy
shard drop during L0 copy
crash with shard cleanup PendingAccounting
crash with shard cleanup ReadyForGc
shard re-add before an older generation cleanup completes
crash before destination fsync
crash after destination fsync before metadata publish
crash after metadata publish before source deletion
MapPayloadRef precondition fails during publish
GC running on an older materialized view
segment has pending refs not yet folded into stats
unknown lifetime routes to spillover
future accounting/compaction resolves old refs through mapped retention refs
```

## A2.17 Open Questions

The main open questions are:

```
Do we want to own a blob-version LSM at all, or keep using RocksDB metadata?

If we own it, how much of the foreground path should update GC summaries
synchronously versus leaving summary derivation to LSM compaction?

Is a one-layer LSM enough, or do blob-version deltas need multiple levels?

What is the right unit for segment_gc_overlay merge batches?
  fixed record count
  fixed byte range
  variable compressed block

Should L0 publish write all MapPayloadRef operations directly in the commit batch,
or stage map batches and commit them with a manifest marker?

Can `PayloadRef::Ingest` shard be represented only in blob-version metadata,
or must shard always be duplicated in the segment record header for recovery and
sequential L0 routing?

How much audit/reconciliation machinery is required if a defensive MapPayloadRef
no-op is ever observed?
```

## A2.18 Current Takeaway

The LSM idea changes the shape of accounting:

```
old framing:
  accounting is a separate component that reads blob metadata and publishes
  segment-local truth for L0/GC

new framing:
  blob-version compaction is accounting
  it folds ordered per-key deltas and emits segment-local truth as a side effect
  under a Strata-owned manifest protocol
```

This may be a cleaner long-term architecture if Strata needs to minimize random RocksDB lookups and batch metadata writes heavily. The cost is that Strata would own more storage-engine machinery: delta runs, manifests, recovery, compaction scheduling, and read merging.

The important point is not the name `accounting_lsn`. The important point is explicit completeness:

```
Either GC summaries/ref events are current synchronously,
or unresolved physical refs are represented as pending,
or a materialized-through watermark proves a view covers a segment.
```

As long as unknown refs are never mistaken for retired refs, GC can run on older views and L0 can remain a mostly sequential byte mover.
