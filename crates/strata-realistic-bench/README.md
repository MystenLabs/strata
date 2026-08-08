# Strata realistic lifecycle benchmark

`strata-realistic-bench` is a standalone, time-bounded comparison harness for Strata and RocksDB
BlobDB. It is intentionally separate from `strata-bench`, whose cases isolate individual storage
operations.

The harness runs four roles concurrently:

- writer threads greedily consume the remaining service budget; an AIMD controller raises their
  admitted concurrency while the read and delete obligations are healthy and cuts it when they are
  not;
- delete threads retire every successfully written key after `--retention`, measured from put
  acknowledgement;
- read threads maintain a fixed aggregate request rate and validate a configured mixture of live
  and deleted keys against the client reference model;
- a sampler measures apparent database bytes, allocated filesystem blocks, file count, free
  filesystem space, and space amplification.

The workload stops at `--duration`. The harness takes a terminal space snapshot, then performs no
more client reads, writes, deletes, or explicit syncs during `--cleanup-grace`. Internal GC and
compaction stay alive, and a second space snapshot measures how much space the engine can recover
without new traffic.

## Fairness rules

Use the same workload flags for both engines and change only the root, metrics address, and
engine-specific configuration. Run on an empty, dedicated filesystem directory. The harness
refuses a non-empty `--root` so old files cannot contaminate the score.

BlobDB uses typed-store, has WAL enabled, and uses RocksDB's normal `sync=false` writes. Strata also
does not invoke `sync()` by default. `--sync-interval 0` is the default for both; choose the same
non-zero interval only when explicit durability checkpoints are part of the experiment.

`qualified=true` requires all of the following at the terminal cutoff:

- no storage or reference-model correctness errors;
- achieved read rate at or above `--read-attainment-percent` of the requested rate;
- overall read p99 at or below `--read-p99-slo`;
- at least `--delete-timely-percent` of completed or already-SLO-violating delete obligations
  acknowledged within `--delete-lag-slo`.

Compare accepted put bytes/second only among qualified runs. For similarly qualified runs, lower
terminal and post-grace allocated bytes wins the space part of the comparison. Filesystem free bytes
are reported as a sanity check, but directory allocated bytes are the comparable measure because
other processes can change filesystem-wide free space.

## Example runs

Build once:

```sh
cargo build --release -p strata-realistic-bench
```

Strata:

```sh
sudo env \
  PATH="$HOME/.cargo/bin:$PATH" \
  HOME="$HOME" \
  RUSTUP_HOME="$HOME/.rustup" \
  CARGO_HOME="$HOME/.cargo" \
  target/release/strata-realistic-bench \
  --engine strata \
  --root /opt/benchmark/realistic-strata \
  --duration 30m \
  --retention 5m \
  --cleanup-grace 10m \
  --payload-size 1MiB \
  --initial-write-workers 1 \
  --min-write-workers 1 \
  --max-write-workers 64 \
  --read-workers 8 \
  --read-ops-per-second 5000 \
  --read-deleted-percent 50 \
  --read-p99-slo 100ms \
  --read-attainment-percent 95 \
  --delete-workers 8 \
  --delete-lag-slo 30s \
  --delete-timely-percent 99 \
  --control-interval 5s \
  --sync-interval 0 \
  --strata-gc true \
  --metrics-listen 0.0.0.0:9184
```

BlobDB with the identical client workload:

```sh
sudo env \
  PATH="$HOME/.cargo/bin:$PATH" \
  HOME="$HOME" \
  RUSTUP_HOME="$HOME/.rustup" \
  CARGO_HOME="$HOME/.cargo" \
  target/release/strata-realistic-bench \
  --engine blobdb \
  --root /opt/benchmark/realistic-blobdb \
  --duration 30m \
  --retention 5m \
  --cleanup-grace 10m \
  --payload-size 1MiB \
  --initial-write-workers 1 \
  --min-write-workers 1 \
  --max-write-workers 64 \
  --read-workers 8 \
  --read-ops-per-second 5000 \
  --read-deleted-percent 50 \
  --read-p99-slo 100ms \
  --read-attainment-percent 95 \
  --delete-workers 8 \
  --delete-lag-slo 30s \
  --delete-timely-percent 99 \
  --control-interval 5s \
  --sync-interval 0 \
  --rocksdb-write-buffer-size 512MiB \
  --rocksdb-db-write-buffer-size 1GiB \
  --rocksdb-max-write-buffer-number 2 \
  --rocksdb-high-pri-threads 4 \
  --rocksdb-low-pri-threads 1 \
  --rocksdb-blob-gc true \
  --metrics-listen 0.0.0.0:9184
```

Do not add `--rocksdb-max-background-flushes` unless that tuning is intentionally part of the
configuration under test. Supplying it sets RocksDB `max_background_jobs` to four times the value,
matching RocksDB's derived flush allocation.

## Prometheus metrics

All client and filesystem metrics start with `strata_realistic_bench_` and carry an `engine` label.
The most important queries are:

```promql
# Accepted logical write MiB/s
rate(strata_realistic_bench_put_payload_bytes_total[$__rate_interval]) / 1024 / 1024

# Logical bytes retired by timely/late tombstone acknowledgement, MiB/s
rate(strata_realistic_bench_retired_payload_bytes_total[$__rate_interval]) / 1024 / 1024

# Client operation rate
sum by (engine, operation) (
  rate(strata_realistic_bench_operation_successes_total[$__rate_interval])
)

# Client-visible p99 by operation
histogram_quantile(0.99,
  sum by (engine, operation, le) (
    rate(strata_realistic_bench_operation_duration_seconds_bucket[$__rate_interval])
  )
)

# Database space actually allocated on disk
strata_realistic_bench_directory_allocated_bytes

# Apparent bytes split into segment, Strata LSM table, RocksDB SST, and RocksDB blob files
strata_realistic_bench_storage_file_apparent_bytes

# Strata segment-file I/O MiB/s, including foreground and GC traffic
rate(strata_store_segment_file_bytes_read_total[$__rate_interval]) / 1024 / 1024
rate(strata_store_segment_file_bytes_written_total[$__rate_interval]) / 1024 / 1024

# Logical/physical space amplification
strata_realistic_bench_space_amplification_ratio
```

Strata exports direct net physical GC reclamation as
`strata_store_gc_reclaimed_bytes_total`. BlobDB does not expose an equivalent counter. Its net
physical reclaim rate can be derived from the blob-file counters exposed by this harness:

```promql
clamp_min(
  rate(strata_realistic_bench_blobdb_blob_file_bytes_written[$__rate_interval])
  - deriv(strata_realistic_bench_blobdb_total_blob_file_bytes[$__rate_interval])
  - rate(strata_realistic_bench_blobdb_gc_bytes_relocated[$__rate_interval]),
  0
)
```

The subtraction removes live bytes copied by GC; those bytes are write amplification, not net
reclamation. The directory allocated-byte graph remains the engine-neutral source of truth for the
terminal result.

Import [`grafana-dashboard.json`](grafana-dashboard.json) for a ready-made dashboard. Run one engine
per process; to display Strata and BlobDB simultaneously, scrape each process with the existing
`job="strata-bench"` and distinct Prometheus targets. The harness's unique metric prefix keeps it
separate from focused `strata-bench` metrics despite sharing the scrape job.
