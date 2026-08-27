# Strata Storage Engine Design

## Engine Core Architecture Overview

Strata is an append-only blob storage engine with payload bytes outside RocksDB and metadata inside RocksDB.

Core crates:

- `core`: stable record, key, lifecycle, segment, and LSN types.
- `segment`: blocking append/read/scan for segment `.data` files.
- `index`: RocksDB-backed metadata index.
- `store`: high-level store protocol and workers.
- `strata-accounting-index`: file-backed LSM for accounting/GC-derived state.

Physical namespace layout:

```text
{root_dir}/{namespace}/
  ingest/
    000000000001.data
    000000000002.data
  index/
    RocksDB metadata
  accounting-index/
    active-delta.log
    partition-00000/
      delta-00000000000000000001.run
      patch-00000000000000000002.run
      base-00000000000000000003.run
```

Segment files are append-only while open. A record is:

```text
64-byte fixed header || payload bytes || key trailer
```

Header fields include `STR0` magic, record version `4`, key length, generation/LSN, payload length, XXH3-128 checksum, checksum algorithm, shard id, and shard generation. The checksum covers the header with checksum bytes zeroed, the payload, and the key trailer.

Main RocksDB column families:

```text
blob_versions          BlobKey -> BlobVersionState
segment_states         SegmentId -> SegmentState
segment_gc_overlay     SegmentId -> SegmentGcOverlay + SegmentGcSummary
segment_ref_events     SegmentRefEventKey -> SegmentRefEvent
shards                 ShardId -> ShardInfo
store_state            StoreStateKey -> u64
epoch_changes          StrataLsn -> Epoch
unaccounted_lsn_ops    StrataLsn -> BlobKey
accounting_index       AccountingIndexKey -> manifest/cursors/shard cleanup jobs
```

The logical commit domain is a store-global LSN stream. RocksDB metadata is the commit log; segment files are payload logs. Recovery never treats payload records found only in `.data` files as committed operations.

## Why Strata Instead of BlobDB

Strata is not a generic replacement for RocksDB BlobDB. It is a workload-specific design for large immutable blobs whose lifecycle, durability frontier, and garbage-collection policy are known by the application.

The most important distinction is GC scheduling. Integrated BlobDB garbage collection is part of RocksDB compaction: as compaction encounters references to older blob files, it can relocate still-valid blobs and eventually make old blob files obsolete. BlobDB can also force reclamation by scheduling targeted compactions for SSTs that reference garbage-heavy blob files. That means blob reclamation is coupled to LSM compaction selection and compaction volume.

For large blob workloads, this coupling is the wrong control surface. Heavy metadata compaction can become heavy blob-file GC because compaction is also the path that reads old blob references and rewrites live blob values. Conversely, if compaction does not touch the SSTs that reference garbage-heavy blob files, blob space reclamation can lag until force-GC targeted compactions run.

Strata decouples these concerns. RocksDB compaction only manages compact metadata and merge operands. Payload GC/reorganization is driven by Strata-owned segment state, accounting processors, ref events, and GC overlays. The system can choose when to copy, tier, or delete segment bytes based on application-level liveness and disk policy, without making every metadata compaction an implicit blob-GC event.

BlobDB moves large values out of the main LSM value path, but it is still coupled to RocksDB's generic key-value abstraction:

- blob files are managed as generic value storage, not as application-visible segments
- garbage collection is tied to compaction and blob-file liveness rather than independently scheduled from epoch-aware object lifetime
- foreground metadata and blob-value management remain tied to RocksDB compaction behavior
- range streaming still starts from a key-value read path, not from a stable application-level `RecordRef`
- tiering decisions have limited application context unless encoded externally

Strata keeps RocksDB on the path where it is strongest: ordered, durable, compact metadata. Payload bytes move into explicit segment files with store-level lifecycle and durability semantics.

The main advantages for this workload are:

- **Predictable payload layout**: blobs are appended to segment files named and tracked by `SegmentState`; the store owns rollover, sealing, verification, and recovery.
- **Lower foreground write amplification**: payload writes are sequential appends, while RocksDB receives compact metadata and merge operands instead of large values.
- **Explicit durability frontier**: `published_lsn` is computed from fsynced segment offsets, fsynced accounting deltas, and RocksDB WAL state.
- **Application-aware accounting**: `accounted_lsn`, segment ref events, and GC overlays materialize blob lifecycle transitions without scanning payload files on the hot path.
- **Decoupled GC control**: metadata compaction does not automatically drive payload relocation; segment-copy, reclaim, and tiering work can be scheduled by disk pressure, garbage ratio, epoch expiry, or coldness.
- **Cheap payload streaming**: reads resolve `BlobKey -> RecordRef` in RocksDB, then read or stream the payload directly from the segment file.
- **GC policy separation**: metadata can declare which segment ranges are live, retired, expired, pinned, or lifetime-routed before any physical copy/reclaim worker runs.

This makes Strata better suited than BlobDB when payloads are large, immutable, frequently read by byte range, and governed by explicit epochs or retention metadata.

### Cold Tiering Possibility

Strata's segment abstraction naturally supports cold tiering because segment state is already separated from blob identity. A future tiering worker can move sealed segments, or selected copied records, to a colder placement class without changing the logical blob key.

The expected extension point is `SegmentState`:

```text
SegmentState {
  owner, // Store for mixed ingest, Shard(ShardKey) for retention
  segment_id,
  path,
  placement_class,
  state,
  sealed_len,
  sealed_sha256,
  ...
}
```

Possible cold-tier flow:

```text
sealed ingest segment
  -> accounting identifies cold/live ranges
  -> copy whole segment or selected records to cold storage
  -> publish new SegmentState or MapRef metadata
  -> retire old hot ranges through segment_ref_events / segment_gc_overlay
  -> delete hot segment after refs are no longer protected
```

Cold placement could use `PlacementClass::Spillover`, future cold-specific placement classes, or a volume/path convention in `SegmentState`. Since reads resolve through `RecordRef` and segment metadata, the blob API does not need to expose whether bytes live on hot local disk, colder local media, or a remote/object-backed tier. The main requirement is preserving the same publication rule used elsewhere in Strata: cold bytes must be durable before RocksDB metadata points at them.

Cold tiering can also support future promotion back to hot or warm storage. A later policy engine could track read hits per blob, record range, or segment and migrate frequently accessed cold bytes back to a lower-latency tier. The promotion path would mirror cold migration:

```text
cold record or segment receives sustained read hits
  -> copy selected records or whole segment to hot/warm storage
  -> publish new SegmentState or MapRef metadata
  -> keep old cold bytes protected until metadata is durable
  -> retire old cold ranges after readers and accounting no longer need them
```

This is not implemented in the current codebase. The current design keeps the necessary indirection: reads resolve through metadata, so later hot/warm promotion can be added as a background policy without changing the user-facing blob API.

## The Write Path Lifecycle

Foreground writes are serialized through one `WriteCoordinator`.

```text
client
  -> StrataStore::put / StrataBatch::write
  -> bounded sync_channel(write_queue_capacity)
  -> WriteCoordinator
```

For a payload write:

1. `prepare_batch()`
   - read `store_state[NextLsn]`
   - assign contiguous LSNs to every `BatchOp`
   - validate shard generation through the `shards` CF
   - precompute encoded record length

2. `ensure_segment_capacity(record_len)`
   - if active segment overflows, stage rollover
   - records are never split across segment files

3. `SegmentWriter::append_for_shard()`
   - encode header/payload/key trailer
   - write vectored slices to `ingest/{segment_id:012}.data`
   - return `RecordRef { segment_id, offset, len }`

4. Append accounting deltas
   - write `AccountingLogEntry::Blob` or `AccountingLogEntry::Epoch` to `active-delta.log`
   - if later RocksDB commit fails, roll back `active-delta.log` to the saved position

5. `commit_write_batch()`
   - one RocksDB batch:
     - `blob_versions` merge operand
     - `unaccounted_lsn_ops[lsn] = BlobKey`
     - `segment_states[active].write_offset`
     - `epoch_changes` and `CurrentEpoch` if needed
     - `store_state[NextLsn] = next_lsn`
     - pending rollover segment state rows

6. Reply to caller.

Write visibility vs durability:

```text
put returns after:
  segment bytes written to OS
  blob-LSM WAL mutation appended
  RocksDB metadata batch committed

put does not imply:
  segment fsync
  blob-LSM WAL fsync
  RocksDB WAL fsync
```

Durability is published by `StrataStore::sync`:

```text
sync
  -> fsync pending segment files and blob-LSM WAL
  -> write segment durable_offset
  -> write the exact LSM checkpoint
  -> write store_state[PublishedLsn]
  -> RocksDB flush_wal(true)
```

`published_lsn` advances only to the LSN covered by the synced blob-LSM checkpoint. Recovery
promotes a complete committed WAL tail, but rolls an incomplete unpublished tail back to the last
published checkpoint.

Rollover path:

```text
active segment full
  -> wait until unsealed segment count < max_unsealed_segments
  -> create next segment file
  -> stage old segment as Sealing
  -> stage new segment as Open
  -> publish both in next metadata batch
  -> enqueue SealWorker after commit
```

Seal path:

```text
SealWorker
  -> open old segment
  -> sync_data()
  -> compute SHA-256 over sealed_len
  -> write SegmentFileState::Sealed
  -> write sealed_len and sealed_sha256
  -> recompute/publish published_lsn
  -> RocksDB flush_wal(true)
```

## The Read Path Lifecycle

Point lookup is metadata-first and payload-direct.

```text
StrataStore::get_blob(key)
  -> resolve_blob_version(index, shard, key)
  -> reader_cache.read_record_with_options(record_ref)
  -> validate record key
  -> return payload
```

Lookup sequence:

1. RocksDB get `blob_versions[key]`
   - `BlobVersionState { versions: VersionState { heads, tail }, lifecycle: BlobLifecycleState { head, tail } }`

2. Resolve latest shard head
   - start from compacted `heads[ShardKey]`
   - apply unaccounted tail ops ordered by LSN
   - reject tombstoned head or newer lifecycle tombstone

3. Validate segment readability
   - get `segment_states[segment_id]`
   - reject `Deleted`
   - evict cached reader if unreadable

4. Read physical record
   - `SegmentReaderCache` returns cached `SegmentReader`
   - read 64-byte fixed header at `RecordRef.offset`
   - validate magic/version/header length/checksum algorithm
   - validate `RecordRef.len == encoded_record_len(header)`
   - read payload + key trailer
   - optionally verify XXH3-128 full-record checksum
   - validate trailer key equals requested `BlobKey`

Payload byte-range read:

```text
get_blob_range / stream_blob
  -> resolve same RecordRef
  -> read header and key trailer only
  -> validate requested payload range
  -> seek to payload_offset + range.start
  -> read or stream requested bytes
```

Range reads do not verify the full-record checksum because the checksum covers the entire record, not an arbitrary payload slice.

Caching and indexing properties:

- There is no segment-local Bloom filter.
- There is no segment block index.
- There is no Strata row cache or block cache.
- `SegmentReaderCache` is an LRU cache of open segment readers/file handles.
- RocksDB serves the primary key index.
- The OS page cache handles payload locality.
- The store exposes payload byte-range reads, not user-facing ordered key-range scans.

## Background Maintenance & Resource Management

### Historical Accounting Processor (retired)

See [`lsm_gc.md`](lsm_gc.md) for the current implementation, publication protocol, and invariants.
The remainder of this section documents the pre-blob-LSM pipeline.

The foreground writer appends cheap accounting deltas. A background `AccountingWorker` materializes those deltas into GC-facing rows.

```text
foreground writer
  -> active-delta.log

AccountingWorker
  -> read durable active-delta.log range
  -> write partitioned delta-*.run files
  -> publish manifest + consumed cursor
  -> compact deltas into patch-*.run
  -> major-compact base + patches into base-*.run
  -> publish segment ref events/GC overlay
  -> advance accounted_lsn
```

Accounting-index run model:

```text
delta-*.run  raw sorted BlobUpdate records
patch-*.run  residual per-key histories
base-*.run   MaterializedBlobState per key
```

Run files are written, synced, and renamed before they become reachable. The RocksDB `accounting_index` manifest is the root pointer that makes run files live.

`accounted_lsn` means accounting-derived rows are durable through that global LSN. After `accounted_lsn` advances, consumed `unaccounted_lsn_ops` rows are removed.

`blob_versions` compaction follows `published_lsn`, not `accounted_lsn`. The active accounting delta log is the accounting input, so packed per-key tail entries may fold into heads once the corresponding logical operations are durable.

### Asynchronous Shard Drop

`drop_shard` completes after the writer durably commits a generation fence, a `ShardDropped` accounting delta, and a cleanup job keyed by the full `ShardKey`. It does not wait for accounting, GC claims, or filesystem deletion.

```text
PendingAccounting
  -> accounting folds through drop_lsn
  -> remove the generation from every MaterializedBlobState
  -> emit Retired only for SegmentOwner::Store payloads
ReadyForGc
  -> drain claims for SegmentOwner::Shard(generation) segments
  -> evict readers and delete generation metadata/directory
  -> remove cleanup job
```

Shard-owned payloads do not need individual retirement ranges because the complete generation directory is deleted. A persisted cleanup job is the recovery source of truth; in-memory accounting and GC commands are wakeups only.

### GC Metadata

Implemented metadata:

- `SegmentGcSummary`: total/live/retired/expired byte accounting stored inside the overlay.
- `SegmentRefEvent`: ordered physical ref transitions.
- `SegmentGcOverlay`: expired ranges, retired ranges, lifetime hints, and summary counters per segment.
- `VersionMergeOp::MapRef`: metadata operation for relocating a payload ref.

The overlay is stale-tolerant:

```text
retired ranges   permanently skippable
expired ranges   skippable after logical expiry; not revivable by a later extension
lifetime ranges  copy-eligible ranges with routing hints
absent range     eligible, not necessarily freshly proven live
```

### Recovery

Open-time recovery is ordered to avoid publishing metadata that points at missing bytes.

```text
open
  -> validate config and create ingest dir
  -> initialize shard and epoch state
  -> remove or reject orphan segment files
  -> scan unsealed segments in segment-id order
  -> truncate valid prefixes
  -> discard later unsealed segments after first incomplete prefix
  -> rollback lost unaccounted operations
  -> reconcile active-delta.log with RocksDB committed prefix
  -> recompute published_lsn
  -> verify sealed segments
  -> choose active segment
  -> start accounting, sealer, writer
```

Unsealed segment recovery:

```text
SegmentScanner::scan_recoverable_prefix(durable_offset)
  -> scan from offset 0
  -> accept complete checksummed records
  -> stop at partial/corrupt tail after durable_offset
  -> error if corruption is before durable_offset
  -> truncate file to valid prefix
  -> update SegmentState.write_offset/durable_offset
```

Rollback rule:

```text
find first unaccounted LSN whose operation did not survive
  -> remove blob/lifecycle ops at that LSN and after
  -> remove epoch changes at that LSN and after
  -> remove unaccounted_lsn_ops at that LSN and after
  -> rewind store_state[NextLsn]
  -> restore CurrentEpoch from latest surviving epoch row
  -> RocksDB flush_wal(true)
```

Sealed segment recovery is verification-only. A sealed segment must exist with the indexed `sealed_len`; under checksum policy, its SHA-256 must match `sealed_sha256`.
# Historical Design Summary

> This summary describes the retired projection-engine architecture. It is retained for design
> context, not as current operational documentation. See [`lsm_gc.md`](lsm_gc.md).
