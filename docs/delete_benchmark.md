# Strata vs BlobDB Delete Benchmark

The delete benchmark has two cases:

- `store-delete`: populate Strata, checkpoint and seal its active ingest tail, issue logical
  tombstones, and optionally observe background reclamation.
- `rocksdb-blobdb-delete`: populate and flush BlobDB, issue point deletes, and optionally observe
  its normal background compactions and blob GC.

There is no forced Strata drain or RocksDB full-range compaction in this harness. Setup is excluded
from foreground delete latency. Both engines perform a final durability sync after the initial
delete loop, and that durable boundary is `t=0` for reclamation samples.

## Reclamation modes

`--delete-reclaim none` exits after the immediate `t=0` sample. Use it to measure foreground
logical deletes without an observation window.

`--delete-reclaim background` keeps the store open for `--reclaim-duration`. It never invokes a GC
or compaction API. Strata's configured accounting/GC workers and RocksDB's configured automatic
compactions continue naturally. Samples are recorded at `--reclaim-sample-at`; `0` and the final
duration are always included.

Background mode supports two post-delete workloads:

- `idle`: issue no operations after the initial durable deletes. This measures autonomous reclaim
  when a tenant deletes data and stops writing.
- `steady`: issue a deterministic, single-threaded, rate-limited mix of puts, payload gets, and
  deletes. Generated deletes target keys created by the steady workload, preventing repeated
  deletion of the initial key set. The default 40/20/40 mix is approximately live-data neutral,
  although early deletes can become puts when no generated live key is available. Reported actual
  operation counts are authoritative.

The steady rate is a target, not a catch-up queue. If an operation or sync takes longer than its
interval, the benchmark does not issue a burst to recover missed operations. `--sync-every`
applies to mutations in the steady phase as well as to the initial delete loop; a final sync makes
any remaining steady mutations durable.

## Recommended idle runs

Run engines one at a time against fresh roots with the same payload, key count, deletion manifest,
sync cadence, duration, and sample times.

```shell
cargo run -p strata-bench --release -- \
  --case store-delete \
  --root /benchmark/strata-delete-idle \
  --ops 100000 \
  --payload-size 1MiB \
  --delete-percent 75 \
  --delete-pattern random \
  --delete-seed 1 \
  --delete-reclaim background \
  --reclaim-duration 60m \
  --reclaim-sample-at 0,1m,5m,10m,15m,30m,60m \
  --post-delete-workload idle \
  --sync-every 1000 \
  --metrics-drain-seconds 0

cargo run -p strata-bench --release -- \
  --case rocksdb-blobdb-delete \
  --root /benchmark/blobdb-delete-idle \
  --ops 100000 \
  --payload-size 1MiB \
  --delete-percent 75 \
  --delete-pattern random \
  --delete-seed 1 \
  --delete-reclaim background \
  --reclaim-duration 60m \
  --reclaim-sample-at 0,1m,5m,10m,15m,30m,60m \
  --post-delete-workload idle \
  --sync-every 1000 \
  --metrics-drain-seconds 0
```

With BlobDB's defaults, `blob_gc_age_cutoff=0.25` makes the oldest quarter of blob files eligible
for relocation during compaction, while `blob_gc_force_threshold=1.0` disables garbage-triggered
targeted compaction. The idle result therefore answers what those defaults actually reclaim without
subsequent tenant writes.

## Recommended steady runs

Add the same steady stream to each command:

```shell
  --post-delete-workload steady \
  --post-delete-ops-per-second 100 \
  --post-delete-workers 4 \
  --post-delete-put-percent 40 \
  --post-delete-get-percent 20 \
  --post-delete-delete-percent 40 \
  --post-delete-seed 2
```

This measures whether reclamation keeps up while BlobDB can piggyback relocation onto natural LSM
compactions and Strata performs background organization/GC. Compare achieved operation counts and
latencies as well as reclaimed space; a store that misses the target operation rate is experiencing
foreground interference.

## Workload controls

- `--ops`: keys loaded before the timed initial delete phase.
- `--delete-percent`: percentage of loaded keys deleted, greater than zero through 100.
- `--delete-pattern sequential`: delete the oldest insertion-order keys.
- `--delete-pattern random`: deterministic partial Fisher-Yates selection using `--delete-seed`.
- `--delete-verify-samples`: number of deleted and surviving initial keys checked at `t=0` and after
  the observation window.
- `--delete-reclaim none|background`: immediate exit or fixed-duration native background behavior.
- `--delete-setup-timeout`: bound Strata's pre-delete sealing/accounting wait.
- `--reclaim-duration`: total background observation time. Supported suffixes are `ms`, `s`, `m`,
  and `h`.
- `--reclaim-sample-at`: comma-separated checkpoints. Times after the duration are rejected when
  this list is explicitly provided.
- `--post-delete-workload idle|steady`: choose quiescent or ongoing foreground traffic.
- `--post-delete-ops-per-second`: aggregate target steady operations per second across all workers.
- `--post-delete-workers`: concurrent steady workload producers. Each owns a disjoint generated-key
  stream; the default is one worker.
- `--post-delete-put-percent`, `--post-delete-get-percent`, and
  `--post-delete-delete-percent`: steady mix; values must sum to 100.
- `--post-delete-seed`: deterministic steady operation selection.
- `--strata-gc-min-garbage-percent` and `--strata-gc-min-reclaim-bytes`: Strata sparse segment
  rewrite policy.
- `--strata-gc-io-bytes-per-sec`: Strata's background copy budget.
- `--rocksdb-blob-gc-age-cutoff` and `--rocksdb-blob-gc-force-threshold`: BlobDB eligibility and
  targeted-compaction scheduling policy.

For a geometry-controlled comparison, set `--segment-max-bytes` and
`--rocksdb-blob-file-size` to the same value and report observed file-size distributions. Also run
a production-configuration comparison; engine geometry is part of real behavior.

## Output interpretation

Important groups are:

- `phase_store_delete_*` / `phase_rocksdb_delete_*`: initial foreground logical-delete latency.
- `delete_final_sync_us`: durability cost after the initial deletes.
- `delete_reclaim_sample_N_target_ms` / `_elapsed_ms`: requested and actual checkpoint time.
- `delete_reclaim_sample_N_*_bytes`: directory, allocation, and payload-file state at a checkpoint.
- `delete_reclaim_sample_N_cumulative_io_*`: Linux process I/O since `t=0`.
- `delete_reclaim_sample_N_post_delete_*`: cumulative steady operations at a checkpoint.
- `phase_post_delete_put_*`, `_get_*`, `_delete_*`, and `_sync_*`: foreground latency while
  background reclamation is active.
- `delete_post_delete_*_payload_bytes`: additional logical traffic generated by steady mode.
- `rocksdb_property_blob_stats` and `rocksdb_property_stats`: final BlobDB garbage and compaction
  detail.

In idle mode, final reclaimed bytes can be divided by the initially deleted logical payload. In
steady mode, directory growth or shrinkage includes new puts and deletes, so use the full timeline,
actual workload counts, and cumulative I/O rather than interpreting final size as reclamation of
only the initial deletes.

Directory-level reclaim can be negative because metadata, WALs, manifests, active Strata segments,
and steady writes may grow while payload files are removed. Use both allocated bytes and payload
file bytes. Process I/O reports `unavailable` on systems without `/proc/self/io`.
