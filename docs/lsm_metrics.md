# LSM inventory metrics

Stores opened with `StrataStoreMetrics::new` export these Prometheus gauges:

| Metric | Meaning |
| --- | --- |
| `strata_store_lsm_sst_files` | Number of installed SST files |
| `strata_store_lsm_sst_bytes` | Sum of installed SST file lengths in bytes |
| `strata_store_lsm_sst_records` | Sum of physical records in installed SSTs |

Every gauge has `store`, `lsm` (`main` or `relocation`) and `kind` (`base` or
`patch`) labels: twelve series per store. Sum over `kind` for one tree's total.
The main tree indexes blobs; the relocation tree tracks payloads moved by GC.
RocksDB's own metrics describe separate metadata column families.

Counts include flushed patches installed in the current read view even when
they are waiting for durable manifest publication. They exclude memtables,
payload segments, compaction output not yet installed, and obsolete files that
older readers still pin. These are current-tree totals, not a directory's
physical disk usage. There are no series before binding a store or after the
corresponding LSM is dropped; an open empty tree reports zero.

**Records are not unique live keys.** A key can appear in multiple patches and
the base, and records can contain obsolete operands pending compaction. No
deduplication or live-key scan is performed.

Inventory is cached while the existing snapshot construction loop visits table
metadata. It adds three saturating integer additions per table, with no extra
table traversal or file reads. A scrape takes one brief view lock per LSM and
copies six cached integers. It performs no SST reads, directory scans, key
walks, or RocksDB property queries. Reads and writes do not update per-operation
inventory counters. Metrics retain weak LSM references so a registered collector
does not keep a closed store alive.
