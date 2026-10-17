# Parallel sweeping and GC activation without global catch-up

GC activation previously held the garbage-publication mutex while sweeping the entire committed
backlog. Compaction could merge SSTs but could not publish them until that drain finished. This
change removes that drain and increases the throughput of the dedicated sweeper.

## Bounded map and reduce

The coordinator locates frame boundaries in log order, up to 256 frames, 32,768 records, or 64 MiB
of encoded data. Limits permit one oversized legacy frame because a committed frame cannot be
split. Up to four workers read, checksum and decode independent frames. The coordinator restores
frame order, follows any GC forwarding routes, and groups records by destination segment.

Up to four reducers process different segment logs concurrently; each segment has exactly one
writer in a batch. Reducers reuse validated writers and folded overlays, apply events in order,
and append once per segment. The existing filesystem sync precedes a single RocksDB commit of
all segment positions, summary deltas, forwarding-route removals and the global cursor. No
worker independently commits a cursor. All workers join on failure; successful partial appends
remain unpublished and normal recovery truncates them on retry. Cached writers are retained only
after successful publication. Completed reducer overlays are capped at 1 GiB combined, in
addition to the existing 1 GiB cache and the overlays currently being processed by workers.

The publication mutex still serializes each bounded sweep with GC activation and compaction
publication. It is released between batches. The sweeper wakes GC and checks shutdown after
bounded groups even if the producer keeps the log nonempty.

## Activation with incomplete garbage knowledge

GC copies and revalidates using the source overlays already available. It may conservatively
activate bytes that a pending event will later identify as garbage. Logical blob reads still
resolve the blob's current version first; publishing a physical relocation does not resurrect a
logical deletion.

Under the publication mutex, activation captures the committed global head H. In the **same
synced batch** as the relocation manifest and output/source states, it stores a route for each
source segment: its copied `(offset, length)` ranges, destination references, and boundary H.

When sweeping an event from a frame ending at or before H, apply it to the old segment and
forward it to each matching new copy. Chained moves are followed in order. Events after H are
not forwarded through that route: notably, GC's own source-retirement frame comes after H and
must never retire the new copy. Compactions publishing after activation retain their existing
in-flight activation routing.

Routes persist across restart. They are removed atomically with a sweep cursor at or beyond H.
The source-retirement frame ensures every route is encountered again even when no older event
names that source. Routes are independent of the relocation LSM's later reference healing.
Monotonically increasing destination segment IDs reject cycles and malformed moves.

Physical source retirement uses `u64::MAX` as its garbage-log sort LSN. It allocates no foreground
LSN and sorts after logical history. Reusing the latest foreground LSN could collide with a
pending lifecycle/expiry event at the same logical position when the source log is rebuilt.
The route boundary is a **log position**, not this sort LSN.

Output lifetimes are seeded only from already swept source knowledge. Reading a newer blob-LSM
lifetime at activation would allow an older pending hint to overwrite it later. Unknown lifetimes
remain conservative. The first relocation-healing compaction supplies the current lifetime even
when that lifetime predates activation; subsequent passes over already-healed refs do not repeat
that hint.

## Remaining constraints

This removes global catch-up from **activation**. Safe physical deletion still requires a durable
relocation, released snapshots, and evidence that the source is empty. Clock-based expiry keeps
its conservative accounting frontier. Neither deletion nor expiry is allowed to guess that an
unswept lifetime extension does not exist.

A new `garbage_forwarding` column family holds pending routes. Existing stores create it on open;
no payload or SST rewrite is needed. Do not downgrade to a writer that ignores these routes while
any remain outstanding. Routes are temporary metadata proportional to copied records with
outstanding sweep work. Activation can retain extra obsolete copies while sweeping lags; this is
a space tradeoff, not a promise that all GC reclamation is independent of sweeping.

## Validation

Regression tests cover parallel event order, rolled files, batch limits, corrupt frames, failed
local appends and retry, forwarding through two moves after restart, preventing GC source
retirement from reaching the destination, and actual GC activation with an unchanged lagging
sweep cursor. A store test checks that a pending expiry at the durable foreground LSN cannot
collide with GC's source-retirement marker.

The ignored `sweep_reducer_throughput` benchmark compares the same pipeline with one and four
workers: 32 segment histories of 20,000 events, then a 32,768-event incoming batch. A local debug
build measured 13.80 s versus 3.99 s cold (3.45x), and 1.17 s versus 0.85 s warm (1.37x).
These are local microbenchmarks, not Oregon HDD throughput or proof of steady-state catch-up.
The cluster must still be measured for sweep rate, retained garbage, compaction publication,
put p99 and completed blob writes under load.
