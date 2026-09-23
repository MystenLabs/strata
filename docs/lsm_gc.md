# Blob-LSM Garbage Materialization

The blob LSM is the authoritative logical index and the only producer of new logical garbage.
There is no separate accounting log, worker, frontier, run store, or projection index.

## Write and durability path

```text
foreground mutation
  -> append payload bytes when needed
  -> append an ordered blob-LSM WAL mutation
  -> commit segment metadata and next_lsn

sync
  -> fsync pending segment files and blob-LSM data
  -> atomically publish the LSM checkpoint, segment durable offsets, and published_lsn

recovery
  -> validate recoverable segment prefixes
  -> preflight the committed LSM WAL target
  -> promote a complete tail, or roll an incomplete unpublished tail back to published_lsn
  -> publish the recovered LSM checkpoint
```

The sealer makes immutable segment files durable but cannot advance `published_lsn` by itself. A
matching blob-LSM checkpoint is required for that durability promise.

## Lazy garbage discovery

Blob-LSM compaction classifies the keys in its selected input range. It emits terminal garbage
records for:

- overwritten payload versions;
- tombstoned payload versions;
- lifetimes whose end epoch is visible at the compaction snapshot;
- shard generations whose durable drop LSN is visible at the compaction snapshot.

Epoch expiry and mixed ingest-segment shard data are deliberately lazy. Strata does not perform a
global rewrite or point-lookup sweep merely because an epoch advances or a shard is dropped. Cold
keys remain conservatively unreclaimed until normal compaction visits them.

Shard-owned retention segments take the fast path. A durable cleanup job fences the generation,
waits for active readers and outstanding garbage-log work, then removes the generation's complete
directory without inspecting every blob key. The cleanup job remains as the drop-LSN source of truth
for later lazy compactions of mixed ingest data.

## Garbage publication and physical GC

```text
blob-LSM compaction
  -> append terminal records to the global garbage log
  -> sync output tables and garbage records
  -> atomically publish the new manifest

garbage sweeper
  -> fold records into segment-local garbage logs and compact summaries
  -> publish the swept cursor only after those files are durable

GC
  -> plan from segment summaries
  -> copy candidate live records
  -> revalidate them against the current blob LSM
  -> sync relocation and blob-LSM mutations
  -> atomically publish relocation metadata, checkpoints, and published_lsn
```

This ordering keeps untouched or not-yet-swept bytes live by default. A segment becomes reclaimable
only from durable garbage records and summaries.

## Retired families

The projection engine's column families (`blob_versions`, `segment_ref_events`,
`segment_gc_overlay`, `gc_relocations`, `unaccounted_lsn_ops`, `accounting_index`) and its
`accounting-index` run/log directory are gone, along with the retired `strata-accounting` crate,
its runtime APIs, configuration, metrics, and serialized helper types.

Open no longer migrates or drops them: a database written by a version that still had those
families is not readable by this one.
