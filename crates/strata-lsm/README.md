# strata-lsm

Status: milestone 1.9, rolled global garbage logs are reclaimed

`strata-lsm` is the immutable-file engine reused for blob state, segment references, and relocation
processing. SST keys and values remain byte buffers; its garbage-log boundary is typed.

The crate implements a bounded generational memtable, an operation WAL, concrete immutable SST
writers/readers, point reads over a pinned manifest snapshot, and prepared size-bounded compaction
outputs. It composes value storage from `strata-segment`.

The memtable stores every mutation in append order while its hash index points to the latest entry
for each key. Rollover freezes the byte buffer and index together. Frozen generations can finish
caller-owned log ingestion, write their latest entries to a patch SST, and retain their allocations
for reuse after publication. An optional policy rolls a non-empty generation before insertion when
either its unique-key limit or maximum age is reached.

Plain and explicitly prefixed entries can coexist in one generation. A one-byte tag selects the
entry layout while explicit key/value prefix boundaries still expose contiguous full-key and
full-value views. Rollover and recycling freeze all entry kinds together. One flush writes the
generation to a normal manifest-compatible patch SST.

`strata-segment` owns checksummed value records and the reader, writer, and crash-prefix scanner.
It remains independently usable and is available to the LSM as `strata_lsm::segment`.

`Wal` stores ordered `(LSN, bytes)` batches in independently rolled, checksummed files.
The low-level writer remains payload-agnostic; `Lsm` manually encodes its puts, record references,
and metadata operations before appending them. A WAL is initialized with a `FileSyncSender`; sync
queues the active file and tracks the exact `WalPosition` it covers. Rollover queues the old file,
then immediately opens the next file. Reopening validates the published prefix, truncates later
bytes, and removes later WAL files.

`FileSyncer` consumes `FileSyncTask`s from a bounded channel. Each task owns an already-open file
and a completion action that receives the sync result. Cloned workers share the receiver, release
its lock before syncing, and may complete out of order. The syncer contains no segment, WAL, table,
or publication policy, so those callers decide what becomes visible after a successful sync.

## Read/write façade

`Lsm::from_parts` opens its table directory and installs a caller-supplied active segment, segment
factory, manifest, WAL, last recovered LSN, and optional durable checkpoint. Recovery is
deliberately caller-owned for now. `TableStore` is the shared runtime handle used only when
compaction or cleanup must coordinate with live snapshots.

`Lsm::write` assigns consecutive LSNs, rolls the segment when the batch does not fit, and appends
segment records before internally encoding their key-to-`RecordRef` WAL operations. Capacity,
rollover, LSN assignment, and append share one lock. Plain values use one ordered 24-byte
reference; prefixed values split it into an eight-byte segment-id prefix and a sixteen-byte
offset/length suffix. Metadata mutations consume LSNs and enter the WAL without writing
segment or memtable bytes.

Concurrent callers form a bounded two-stage pipeline. One lane serializes segment and WAL appends;
a ticketed second lane applies complete batches to the memtable in the same order. The next batch
may append while the previous batch updates memory, but a batch becomes visible only after its
memtable stage completes. At most two batches are in flight, so a blocked memtable naturally
backpressures segment and WAL appends.

One batch occupies one WAL frame and one uninterrupted memtable critical section. A failure after
physical mutation begins halts the façade so caller-owned recovery can discard uncommitted tails.

`Lsm::get` captures the current table snapshot and all active and frozen memtable generations
together, then invokes the caller's merge operator once over the combined patches.

`Lsm::flush_one` writes the oldest frozen generation as a patch SST. Its publication callback
durably applies the returned `ManifestEdit` and returns the canonical manifest. The frozen
generation remains readable until that manifest has been opened and installed.

`Lsm::sync` queues the active segment and WAL together, waits for rolled-segment syncs, and returns
an `LsmCheckpoint` only after both file streams are durable and the corresponding memtable batch is
visible. The checkpoint records the durable LSN, WAL position, and active segment position.
Segment and WAL roll independently; `roll_segment` queues the old segment for sync and immediately
installs a replacement from the shared segment factory.

## Minimal ownership boundary

The LSM owns the active segment writer, memtables, immutable SST files, their manifest, reads, and
compaction. `strata-segment` continues to own the segment file format and standalone readers and
scanners. The operation WAL lives here; unified recovery joins it in the next review.

There is no pluggable filesystem, comparator, checksum, cache, compression, or block format. We will
add an abstraction only after more than one real implementation needs it.

## Ordering

Keys use unsigned lexicographic byte ordering. Domain encodings must preserve their intended order;
for example, ordered integers use big-endian bytes.

Patches for one key are ordered by LSN. One mutation owns one unique LSN; conflicting patches with
the same key and LSN are corruption.

## Merge function

The one policy abstraction is `MergeOperator`. It receives one key, an optional base value, and all
visible patches as byte slices. It returns the new base bytes or `None` to delete the key.

The merge function may emit typed `GarbageRecord` values through a callback. Foreground reads
discard them; compaction returns them for durable publication. The reducer must be deterministic and
must not perform its own I/O.

## Manifest and publication

`Manifest` is the materialized live file set. Each partition contains non-overlapping base SST
metadata and possibly overlapping patch SST metadata. `TableMeta` describes exactly one SST file;
the RocksDB manifest row is the durable source for the materialized file set.

Compaction produces a small `ManifestEdit` containing removed paths and added base/patch SST
metadata. `Manifest::apply` validates and applies the entire edit atomically in memory. Disjoint edits
can apply in either order; there is no whole-manifest generation precondition.

`TableStore::reserve_for_compaction` atomically reserves all requested input SSTs or none of them.
Reservations exclude only other compactions: snapshots may continue to read the same files. Both
reservations and snapshots pin files against physical deletion and release their pins on drop. A
base-producing job must reserve every base SST whose range its output replaces.

`strata-index` stores one materialized manifest per LSM in RocksDB. Compaction publication appends a
`ManifestEdit` through the manifest column family's merge operator. Edits for several LSMs can share
one RocksDB batch with segment and frontier changes, so the complete publication receives one atomic
database sequence number. Expensive table construction remains outside RocksDB; only metadata enters
its write path.

`Snapshot` captures an `Arc<Manifest>` and a maximum visible LSN. It pins every SST referenced
by that manifest, so compaction may publish replacement files immediately while cleanup retains the
old files for existing snapshots. `TableStore::remove_if_unpinned` bases physical deletion only on
file liveness, never on LSN bounds.

Point reads binary-search the non-overlapping base files, probe only overlapping patch files, filter
patches above the snapshot LSN, sort them by LSN, and invoke the merge operator. The caller still
chooses the partition and must capture the manifest and LSN as one
coherent view.

Two LSMs may share a patch file only when table format, partition count, key encoding, patch encoding,
and partition function match. One persisted `patch_format_id` names that complete compatibility
contract instead of modeling each component separately.

## SST format

An SST contains a small header, sorted checksummed data blocks, independently checksummed index and
Bloom blocks, and a checksummed footer with a fixed-size trailer. The format uses soft 64 KiB target
blocks, SHA-256 checksums, and XXH3 double-hashed Bloom probes. One key is never split across data
blocks.

Writers use a temporary file and sync-file, rename, sync-directory publication. Readers keep only the
footer's sparse index and Bloom filter in memory and use positioned reads for candidate blocks.

Each data block is either plain or prefixed, and one SST may contain both. A prefixed block contains
one `(key prefix, value prefix)` pair and stores only row suffixes. A pair may recur when prefixes
alternate or a block reaches its target size. The full-key sparse index covers every block. A
secondary index sorted by `(value prefix, key prefix, first full key)` supports scans of every key
pointing to one value prefix as well as exact-pair scans. Per-prefix-block suffix Bloom filters
reject missing suffixes. All encodings are explicit and do not use BCS.

## Garbage log

`write_compaction` returns derived `GarbageRecord` values sorted by `(key, LSN)` beside its
manifest edit. `SegmentKey(segment_id, blob_key)` keeps records for one physical segment together.
The value is `Retired`, `Expired`, or `SetLifecycle`; each variant names the exact `RecordRef` it
affects. The accompanying summary delta is ready for RocksDB's merge operator.

There is no `Live` record. Segment contents are copy-eligible by default. A mapped ref becomes a
`Retired` source record and, when needed, a `SetLifecycle` destination record.

`GarbageLog` stores those records in checksummed frames and rolls to a new file at a caller-supplied
soft size limit. Global files are named `garbage-<id>.glog`. A compaction batch is never split across
frames. Each append is synced before its returned `GarbageLogPosition` can be published in RocksDB.

RocksDB owns the committed position. Reopening the log verifies that position is a frame boundary,
truncates later unpublished bytes, and removes later orphaned rollover files.

`strata-index` stores garbage-log positions separately from LSM manifests. Both can be staged in one
RocksDB batch, including a new log id after rollover, without adding garbage-log concerns to the
generic manifest.

Per-segment GC summaries are also stored separately in RocksDB. The summary merge operator updates
counters and epoch histograms without storing exact ranges in RocksDB.

`StrataIndex::publish_lsm_compaction` appends and syncs one compaction's garbage records, then
commits its manifest edit and the frame's end position in a single RocksDB batch. Its mutable log
borrow keeps append and publication serialized. It deliberately does not update segment summaries:
those become visible only after the sweeper has synced the corresponding segment-local garbage logs.

## Global garbage-log sweeper

`GarbageLog::read_next` reads one complete frame between a sweep cursor and the committed global log
position. `StrataIndex::sweep_garbage_log` routes each typed record through its `SegmentKey` and
groups the records by segment.

Each group is appended as one checksummed frame to the segment's `.glog` file. Segment-local
frames need not be sorted because GC will load the file in memory. After every affected file is
synced, one RocksDB batch publishes their committed offsets, applies the summary deltas, and advances
the global sweep cursor.

If the process stops before that RocksDB batch commits, the next sweep truncates each local file to
its stored offset and replays the same global frame. GC reads only through the stored local offset,
so it never observes a record before the matching summary becomes visible and a retry never counts
the summary twice.

`StrataStore` runs a background sweeper that drains committed frames immediately at startup and
retries periodically. Store shutdown stops and joins the worker. The Store's LSM compactor owns the
global log writer.

After the RocksDB batch advances the sweep cursor into a later rolled file, the sweeper deletes
strictly older global files and syncs the directory. The cursor's file and every newer file remain
untouched. Startup retries the same cleanup, covering a crash after cursor publication but before
physical deletion.

`fold_segment_garbage` sorts the committed records by `(SegmentKey, LSN)` and folds them
into the existing in-memory GC overlay shape. Segment contents are copy-eligible by default;
retirement and expiry cannot be reversed by a later lifecycle event. `strata-index` reads the
authoritative summary and local-log position from one RocksDB snapshot and attaches that summary to
the folded result.

## Next milestone

Remove the legacy segment GC overlay publication path once the LSM-produced segment-local logs are
authoritative.
