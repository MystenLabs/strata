//! Small benchmark harness for comparing Strata paths against RocksDB blob files.
//!
//! It creates a fresh root directory, runs one case, prints machine-readable key/value metrics,
//! and removes default temporary data unless `--keep-data` is set. Get cases can instead reopen a
//! prior put case's root with `--reuse-existing`.
//!
//! Cases:
//!
//! ```text
//! segment-append      SegmentWriter append cost without index work
//! store-put           StrataStore put path, cloning each payload into Arc storage
//! store-put-arc       StrataStore put path with caller-provided Arc payload
//! store-get           StrataStore point reads from indexed segment records
//! store-delete        StrataStore tombstone and optional background reclamation timeline
//! rocksdb-blobdb-put  RocksDB BlobDB put baseline through typed-store
//! rocksdb-blobdb-get  RocksDB BlobDB get baseline through typed-store
//! rocksdb-blobdb-get-pinned
//!                      RocksDB BlobDB pinned get without typed-store value decoding
//! rocksdb-blobdb-delete
//!                      RocksDB BlobDB point delete and optional background reclamation timeline
//! ```
//!
//! The benchmark is not part of the storage protocol. It should keep using public crate APIs so
//! benchmark results reflect what callers can actually exercise.

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    collections::{HashMap, VecDeque},
    env, fs, hint, io,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use prometheus::{Encoder, GaugeVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};
use rocksdb::{DB, Env, PerfContext, PerfStatsLevel, WriteOptions, perf::set_perf_stats};
use serde::{Deserialize, Serialize};
use serde_with::{Bytes, serde_as};
use strata_core::{BlobKey, Epoch, PlacementClass, SegmentFileState};
use strata_segment::SegmentWriter;
use strata_store::{
    DEFAULT_ACCOUNTING_DELTA_RUN_BYTES_THRESHOLD, DEFAULT_ACCOUNTING_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_INTERVAL,
    DEFAULT_ACCOUNTING_MAINTENANCE_INTERVAL, DEFAULT_ACCOUNTING_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_MAJOR_PATCH_COUNT_THRESHOLD, DEFAULT_ACCOUNTING_MATERIALIZE_LAG_THRESHOLD,
    DEFAULT_ACCOUNTING_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN, DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
    DEFAULT_GC_SYNC_IMPACT_THRESHOLD, DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT,
    DEFAULT_SEAL_WORKER_COUNT, DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
    GcPlannerConfig, ReadOptions, SealedSegmentIntegrityPolicy, StoreGetProfile,
    StrataRecoveryPolicy, StrataStore, StrataStoreConfig, StrataStoreMetrics,
};
#[cfg(feature = "internal-profiling")]
use strata_store::{StoreProfileSink, StoreSyncProfile, StoreWriteProfile};
use typed_store::{
    DBMetrics, Map,
    metrics::SamplingInterval,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, be_fix_int_ser, default_db_options},
};

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_PAYLOAD_SIZE: usize = 1 << 20;
const DEFAULT_OPS: usize = 1024;
const DEFAULT_READ_SET_SIZE: usize = 1024;
const DEFAULT_READ_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_UNSEALED_SEGMENTS: usize = 8;
const DEFAULT_READER_CACHE_CAPACITY: usize = strata_store::DEFAULT_SEGMENT_READER_CACHE_CAPACITY;
const DEFAULT_STARTING_EPOCH: Epoch = 1;
const DEFAULT_ROCKSDB_MIN_BLOB_SIZE: u64 = 1;
const DEFAULT_ROCKSDB_BLOB_FILE_SIZE: u64 = 1 << 28;
const DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE: usize = 512 << 20;
const DEFAULT_ROCKSDB_HIGH_PRI_BACKGROUND_THREADS: usize = 4;
const DEFAULT_DELETE_PERCENT: f64 = 50.0;
const DEFAULT_DELETE_SEED: u64 = 0xd1e7_e001_cafe_f00d;
const DEFAULT_DELETE_VERIFY_SAMPLES: usize = 1024;
const DEFAULT_DELETE_SETUP_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_RECLAIM_DURATION: Duration = Duration::from_secs(3600);
const DEFAULT_POST_DELETE_OPS_PER_SECOND: u64 = 10;
const DEFAULT_POST_DELETE_WORKERS: usize = 1;
const DEFAULT_POST_DELETE_PUT_PERCENT: f64 = 40.0;
const DEFAULT_POST_DELETE_GET_PERCENT: f64 = 20.0;
const DEFAULT_POST_DELETE_DELETE_PERCENT: f64 = 40.0;
const DEFAULT_POST_DELETE_SEED: u64 = 0x57ea_d1e7_bacc_600d;
const DEFAULT_ROCKSDB_BLOB_GC_AGE_CUTOFF: f64 = 0.25;
const DEFAULT_ROCKSDB_BLOB_GC_FORCE_THRESHOLD: f64 = 1.0;
const ROCKSDB_BLOBDB_CF_CLASS: &str = "rocksdb_blobdb";
const DEFAULT_METRICS_DRAIN_SECONDS: u64 = 30;
const ROCKSDB_PERF_METRICS_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);
const BENCH_SEGMENT_ID: u64 = 1;

/// Stores blob payloads using Serde's byte-buffer path instead of treating every byte as one
/// sequence element. BCS uses the same length-prefixed wire representation for both paths, so this
/// remains compatible with values previously written as a bare `Vec<u8>`.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlobDbValue(#[serde_as(as = "Bytes")] Vec<u8>);

type BlobDbMap = DBMap<Vec<u8>, BlobDbValue>;

fn main() {
    match Config::parse(env::args().skip(1)) {
        Ok(config) => {
            if let Err(error) = run_with_runtime(config) {
                eprintln!("error: {error}");
                process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!();
            eprintln!("{}", usage());
            process::exit(2);
        }
    }
}

fn run_with_runtime(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move { run(config) })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchCase {
    SegmentAppend,
    StorePut,
    StorePutArc,
    StoreGet,
    StoreDelete,
    RocksDbBlobDbPut,
    RocksDbBlobDbGet,
    RocksDbBlobDbGetPinned,
    RocksDbBlobDbDelete,
}

impl BenchCase {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "segment-append" => Ok(Self::SegmentAppend),
            "store-put" => Ok(Self::StorePut),
            "store-put-arc" => Ok(Self::StorePutArc),
            "store-get" => Ok(Self::StoreGet),
            "store-delete" => Ok(Self::StoreDelete),
            "rocksdb-blobdb-put" => Ok(Self::RocksDbBlobDbPut),
            "rocksdb-blobdb-get" => Ok(Self::RocksDbBlobDbGet),
            "rocksdb-blobdb-get-pinned" => Ok(Self::RocksDbBlobDbGetPinned),
            "rocksdb-blobdb-delete" => Ok(Self::RocksDbBlobDbDelete),
            _ => Err(format!("unknown case '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::SegmentAppend => "segment-append",
            Self::StorePut => "store-put",
            Self::StorePutArc => "store-put-arc",
            Self::StoreGet => "store-get",
            Self::StoreDelete => "store-delete",
            Self::RocksDbBlobDbPut => "rocksdb-blobdb-put",
            Self::RocksDbBlobDbGet => "rocksdb-blobdb-get",
            Self::RocksDbBlobDbGetPinned => "rocksdb-blobdb-get-pinned",
            Self::RocksDbBlobDbDelete => "rocksdb-blobdb-delete",
        }
    }

    fn is_delete(self) -> bool {
        matches!(self, Self::StoreDelete | Self::RocksDbBlobDbDelete)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeletePattern {
    Sequential,
    Random,
}

impl DeletePattern {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "sequential" => Ok(Self::Sequential),
            "random" => Ok(Self::Random),
            _ => Err(format!("unknown delete pattern '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Random => "random",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteReclaimMode {
    None,
    Background,
}

impl DeleteReclaimMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "none" => Ok(Self::None),
            "background" => Ok(Self::Background),
            _ => Err(format!("unknown delete reclaim mode '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostDeleteWorkload {
    Idle,
    Steady,
}

impl PostDeleteWorkload {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "idle" => Ok(Self::Idle),
            "steady" => Ok(Self::Steady),
            _ => Err(format!("unknown post-delete workload '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Steady => "steady",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPattern {
    Sequential,
    Random,
}

impl ReadPattern {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "sequential" => Ok(Self::Sequential),
            "random" => Ok(Self::Random),
            _ => Err(format!("unknown read pattern '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Random => "random",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreGetMode {
    Payload,
    KeyOnly,
}

impl StoreGetMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "payload" => Ok(Self::Payload),
            "key-only" => Ok(Self::KeyOnly),
            _ => Err(format!("unknown store get mode '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Payload => "payload",
            Self::KeyOnly => "key-only",
        }
    }
}

fn parse_sealed_integrity(value: &str) -> Result<SealedSegmentIntegrityPolicy, String> {
    match value {
        "metadata-only" | "metadata" => Ok(SealedSegmentIntegrityPolicy::MetadataOnly),
        "checksum" => Ok(SealedSegmentIntegrityPolicy::Checksum),
        _ => Err(format!("unknown sealed integrity policy '{value}'")),
    }
}

fn sealed_integrity_as_str(policy: SealedSegmentIntegrityPolicy) -> &'static str {
    match policy {
        SealedSegmentIntegrityPolicy::MetadataOnly => "metadata-only",
        SealedSegmentIntegrityPolicy::Checksum => "checksum",
    }
}

#[derive(Debug, Clone)]
struct Config {
    case: BenchCase,
    root_dir: PathBuf,
    namespace: String,
    payload_size: usize,
    ops: usize,
    read_set_size: usize,
    read_pattern: ReadPattern,
    read_seed: u64,
    store_get_mode: StoreGetMode,
    store_get_profile: bool,
    store_get_verify_checksum: bool,
    queue_capacity: usize,
    max_unsealed_segments: usize,
    segment_max_bytes: u64,
    seal_worker_count: usize,
    sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy,
    reader_cache_capacity: usize,
    starting_epoch: Epoch,
    strata_accounting: bool,
    strata_accounting_materialize_lag_threshold: u64,
    strata_gc: bool,
    strata_gc_io_bytes_per_sec: u64,
    strata_gc_min_io_bytes_per_sec: u64,
    strata_gc_min_reclaim_bytes: u64,
    strata_gc_min_garbage_ratio_bps: u16,
    rocksdb_min_blob_size: u64,
    rocksdb_blob_file_size: u64,
    rocksdb_write_buffer_size: usize,
    rocksdb_high_pri_background_threads: usize,
    rocksdb_blob_gc: bool,
    rocksdb_blob_gc_age_cutoff: f64,
    rocksdb_blob_gc_force_threshold: f64,
    rocksdb_get_profile: bool,
    rocksdb_disable_wal: bool,
    rocksdb_disable_auto_compactions: bool,
    sync_every: usize,
    delete_percent: f64,
    delete_pattern: DeletePattern,
    delete_seed: u64,
    delete_verify_samples: usize,
    delete_reclaim_mode: DeleteReclaimMode,
    delete_setup_timeout: Duration,
    reclaim_duration: Duration,
    reclaim_sample_at: Vec<Duration>,
    post_delete_workload: PostDeleteWorkload,
    post_delete_ops_per_second: u64,
    post_delete_workers: usize,
    post_delete_put_percent: f64,
    post_delete_get_percent: f64,
    post_delete_delete_percent: f64,
    post_delete_seed: u64,
    metrics_listen: Option<String>,
    metrics_drain_seconds: u64,
    reuse_existing: bool,
    keep_data: bool,
    root_was_defaulted: bool,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let default_gc_planner = GcPlannerConfig::default();
        let mut config = Self {
            case: BenchCase::StorePut,
            root_dir: default_root_dir(),
            namespace: DEFAULT_NAMESPACE.to_owned(),
            payload_size: DEFAULT_PAYLOAD_SIZE,
            ops: DEFAULT_OPS,
            read_set_size: DEFAULT_READ_SET_SIZE,
            read_pattern: ReadPattern::Sequential,
            read_seed: DEFAULT_READ_SEED,
            store_get_mode: StoreGetMode::Payload,
            store_get_profile: false,
            store_get_verify_checksum: true,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_unsealed_segments: DEFAULT_MAX_UNSEALED_SEGMENTS,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            seal_worker_count: DEFAULT_SEAL_WORKER_COUNT,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            reader_cache_capacity: DEFAULT_READER_CACHE_CAPACITY,
            starting_epoch: DEFAULT_STARTING_EPOCH,
            strata_accounting: true,
            strata_accounting_materialize_lag_threshold:
                DEFAULT_ACCOUNTING_MATERIALIZE_LAG_THRESHOLD,
            strata_gc: true,
            strata_gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            strata_gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            strata_gc_min_reclaim_bytes: default_gc_planner.min_reclaim_bytes,
            strata_gc_min_garbage_ratio_bps: default_gc_planner.min_garbage_ratio_bps,
            rocksdb_min_blob_size: DEFAULT_ROCKSDB_MIN_BLOB_SIZE,
            rocksdb_blob_file_size: DEFAULT_ROCKSDB_BLOB_FILE_SIZE,
            rocksdb_write_buffer_size: DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE,
            rocksdb_high_pri_background_threads: DEFAULT_ROCKSDB_HIGH_PRI_BACKGROUND_THREADS,
            rocksdb_blob_gc: true,
            rocksdb_blob_gc_age_cutoff: DEFAULT_ROCKSDB_BLOB_GC_AGE_CUTOFF,
            rocksdb_blob_gc_force_threshold: DEFAULT_ROCKSDB_BLOB_GC_FORCE_THRESHOLD,
            rocksdb_get_profile: false,
            rocksdb_disable_wal: false,
            rocksdb_disable_auto_compactions: false,
            sync_every: 0,
            delete_percent: DEFAULT_DELETE_PERCENT,
            delete_pattern: DeletePattern::Random,
            delete_seed: DEFAULT_DELETE_SEED,
            delete_verify_samples: DEFAULT_DELETE_VERIFY_SAMPLES,
            delete_reclaim_mode: DeleteReclaimMode::None,
            delete_setup_timeout: DEFAULT_DELETE_SETUP_TIMEOUT,
            reclaim_duration: DEFAULT_RECLAIM_DURATION,
            reclaim_sample_at: default_reclaim_sample_at(),
            post_delete_workload: PostDeleteWorkload::Idle,
            post_delete_ops_per_second: DEFAULT_POST_DELETE_OPS_PER_SECOND,
            post_delete_workers: DEFAULT_POST_DELETE_WORKERS,
            post_delete_put_percent: DEFAULT_POST_DELETE_PUT_PERCENT,
            post_delete_get_percent: DEFAULT_POST_DELETE_GET_PERCENT,
            post_delete_delete_percent: DEFAULT_POST_DELETE_DELETE_PERCENT,
            post_delete_seed: DEFAULT_POST_DELETE_SEED,
            metrics_listen: None,
            metrics_drain_seconds: DEFAULT_METRICS_DRAIN_SECONDS,
            reuse_existing: false,
            keep_data: false,
            root_was_defaulted: true,
        };

        let mut reclaim_sample_at_explicit = false;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Err("help requested".to_owned()),
                "--case" => config.case = BenchCase::parse(&next_value(&mut args, "--case")?)?,
                "--root" => {
                    config.root_dir = PathBuf::from(next_value(&mut args, "--root")?);
                    config.root_was_defaulted = false;
                }
                "--namespace" => config.namespace = next_value(&mut args, "--namespace")?,
                "--payload-size" => {
                    config.payload_size = parse_size(&next_value(&mut args, "--payload-size")?)?
                }
                "--ops" => config.ops = parse_nonzero_usize(&next_value(&mut args, "--ops")?)?,
                "--read-set-size" => {
                    config.read_set_size =
                        parse_nonzero_usize(&next_value(&mut args, "--read-set-size")?)?
                }
                "--read-pattern" => {
                    config.read_pattern =
                        ReadPattern::parse(&next_value(&mut args, "--read-pattern")?)?
                }
                "--read-seed" => {
                    config.read_seed = parse_u64(&next_value(&mut args, "--read-seed")?)?
                }
                "--store-get-mode" => {
                    config.store_get_mode =
                        StoreGetMode::parse(&next_value(&mut args, "--store-get-mode")?)?
                }
                "--store-get-profile" => config.store_get_profile = true,
                "--store-get-verify-checksum" => {
                    config.store_get_verify_checksum =
                        parse_bool(&next_value(&mut args, "--store-get-verify-checksum")?)?
                }
                "--queue-capacity" => {
                    config.queue_capacity =
                        parse_nonzero_usize(&next_value(&mut args, "--queue-capacity")?)?
                }
                "--max-unsealed-segments" => {
                    config.max_unsealed_segments =
                        parse_nonzero_usize(&next_value(&mut args, "--max-unsealed-segments")?)?
                }
                "--segment-max-bytes" => {
                    config.segment_max_bytes =
                        parse_size(&next_value(&mut args, "--segment-max-bytes")?)? as u64
                }
                "--seal-workers" => {
                    config.seal_worker_count =
                        parse_nonzero_usize(&next_value(&mut args, "--seal-workers")?)?
                }
                "--sealed-integrity" => {
                    config.sealed_segment_integrity_policy =
                        parse_sealed_integrity(&next_value(&mut args, "--sealed-integrity")?)?
                }
                "--reader-cache-capacity" => {
                    config.reader_cache_capacity =
                        parse_usize(&next_value(&mut args, "--reader-cache-capacity")?)?
                }
                "--starting-epoch" => {
                    config.starting_epoch = parse_u64(&next_value(&mut args, "--starting-epoch")?)?
                }
                "--strata-accounting" => {
                    config.strata_accounting =
                        parse_bool(&next_value(&mut args, "--strata-accounting")?)?
                }
                "--strata-accounting-materialize-lag-threshold" => {
                    config.strata_accounting_materialize_lag_threshold = parse_u64(&next_value(
                        &mut args,
                        "--strata-accounting-materialize-lag-threshold",
                    )?)?
                }
                "--strata-gc" => {
                    config.strata_gc = parse_bool(&next_value(&mut args, "--strata-gc")?)?
                }
                "--strata-gc-io-bytes-per-sec" => {
                    config.strata_gc_io_bytes_per_sec =
                        parse_size(&next_value(&mut args, "--strata-gc-io-bytes-per-sec")?)? as u64
                }
                "--strata-gc-min-io-bytes-per-sec" => {
                    config.strata_gc_min_io_bytes_per_sec =
                        parse_size(&next_value(&mut args, "--strata-gc-min-io-bytes-per-sec")?)?
                            as u64
                }
                "--strata-gc-min-reclaim-bytes" => {
                    config.strata_gc_min_reclaim_bytes =
                        parse_size(&next_value(&mut args, "--strata-gc-min-reclaim-bytes")?)? as u64
                }
                "--strata-gc-min-garbage-percent" => {
                    config.strata_gc_min_garbage_ratio_bps = percent_to_basis_points(parse_percent(
                        &next_value(&mut args, "--strata-gc-min-garbage-percent")?,
                    )?)
                }
                "--rocksdb-min-blob-size" => {
                    config.rocksdb_min_blob_size =
                        parse_size(&next_value(&mut args, "--rocksdb-min-blob-size")?)? as u64
                }
                "--rocksdb-blob-file-size" => {
                    config.rocksdb_blob_file_size =
                        parse_size(&next_value(&mut args, "--rocksdb-blob-file-size")?)? as u64
                }
                "--rocksdb-write-buffer-size" => {
                    config.rocksdb_write_buffer_size =
                        parse_size(&next_value(&mut args, "--rocksdb-write-buffer-size")?)?
                }
                "--rocksdb-high-pri-background-threads" => {
                    config.rocksdb_high_pri_background_threads = parse_usize(&next_value(
                        &mut args,
                        "--rocksdb-high-pri-background-threads",
                    )?)?
                }
                "--rocksdb-blob-gc" => {
                    config.rocksdb_blob_gc =
                        parse_bool(&next_value(&mut args, "--rocksdb-blob-gc")?)?
                }
                "--rocksdb-blob-gc-age-cutoff" => {
                    config.rocksdb_blob_gc_age_cutoff =
                        parse_fraction(&next_value(&mut args, "--rocksdb-blob-gc-age-cutoff")?)?
                }
                "--rocksdb-blob-gc-force-threshold" => {
                    config.rocksdb_blob_gc_force_threshold = parse_fraction(&next_value(
                        &mut args,
                        "--rocksdb-blob-gc-force-threshold",
                    )?)?
                }
                "--rocksdb-get-profile" => config.rocksdb_get_profile = true,
                "--rocksdb-disable-wal" => {
                    config.rocksdb_disable_wal =
                        parse_bool(&next_value(&mut args, "--rocksdb-disable-wal")?)?
                }
                "--rocksdb-disable-auto-compactions" => {
                    config.rocksdb_disable_auto_compactions = parse_bool(&next_value(
                        &mut args,
                        "--rocksdb-disable-auto-compactions",
                    )?)?
                }
                "--sync-every" => {
                    config.sync_every = parse_usize(&next_value(&mut args, "--sync-every")?)?
                }
                "--delete-percent" => {
                    config.delete_percent =
                        parse_percent(&next_value(&mut args, "--delete-percent")?)?
                }
                "--delete-pattern" => {
                    config.delete_pattern =
                        DeletePattern::parse(&next_value(&mut args, "--delete-pattern")?)?
                }
                "--delete-seed" => {
                    config.delete_seed = parse_u64(&next_value(&mut args, "--delete-seed")?)?
                }
                "--delete-verify-samples" => {
                    config.delete_verify_samples =
                        parse_usize(&next_value(&mut args, "--delete-verify-samples")?)?
                }
                "--delete-reclaim" => {
                    config.delete_reclaim_mode =
                        DeleteReclaimMode::parse(&next_value(&mut args, "--delete-reclaim")?)?
                }
                "--delete-setup-timeout" => {
                    config.delete_setup_timeout =
                        parse_duration(&next_value(&mut args, "--delete-setup-timeout")?)?
                }
                "--reclaim-duration" => {
                    config.reclaim_duration =
                        parse_duration(&next_value(&mut args, "--reclaim-duration")?)?
                }
                "--reclaim-sample-at" => {
                    reclaim_sample_at_explicit = true;
                    config.reclaim_sample_at =
                        parse_duration_list(&next_value(&mut args, "--reclaim-sample-at")?)?
                }
                "--post-delete-workload" => {
                    config.post_delete_workload = PostDeleteWorkload::parse(&next_value(
                        &mut args,
                        "--post-delete-workload",
                    )?)?
                }
                "--post-delete-ops-per-second" => {
                    config.post_delete_ops_per_second =
                        parse_u64(&next_value(&mut args, "--post-delete-ops-per-second")?)?
                }
                "--post-delete-workers" => {
                    config.post_delete_workers =
                        parse_nonzero_usize(&next_value(&mut args, "--post-delete-workers")?)?
                }
                "--post-delete-put-percent" => {
                    config.post_delete_put_percent = parse_percent_inclusive(&next_value(
                        &mut args,
                        "--post-delete-put-percent",
                    )?)?
                }
                "--post-delete-get-percent" => {
                    config.post_delete_get_percent = parse_percent_inclusive(&next_value(
                        &mut args,
                        "--post-delete-get-percent",
                    )?)?
                }
                "--post-delete-delete-percent" => {
                    config.post_delete_delete_percent = parse_percent_inclusive(&next_value(
                        &mut args,
                        "--post-delete-delete-percent",
                    )?)?
                }
                "--post-delete-seed" => {
                    config.post_delete_seed =
                        parse_u64(&next_value(&mut args, "--post-delete-seed")?)?
                }
                "--metrics-listen" => {
                    config.metrics_listen = Some(next_value(&mut args, "--metrics-listen")?)
                }
                "--metrics-drain-seconds" => {
                    config.metrics_drain_seconds =
                        parse_u64(&next_value(&mut args, "--metrics-drain-seconds")?)?
                }
                "--reuse-existing" => config.reuse_existing = true,
                "--keep-data" => config.keep_data = true,
                unknown => return Err(format!("unknown argument '{unknown}'")),
            }
        }

        if config.segment_max_bytes == 0 {
            return Err("segment_max_bytes must be non-zero".to_owned());
        }
        if config.payload_size == 0 {
            return Err("payload_size must be non-zero".to_owned());
        }
        if config.rocksdb_min_blob_size == 0 {
            return Err("rocksdb_min_blob_size must be non-zero".to_owned());
        }
        if config.rocksdb_blob_file_size == 0 {
            return Err("rocksdb_blob_file_size must be non-zero".to_owned());
        }
        if config.rocksdb_write_buffer_size == 0 {
            return Err("rocksdb_write_buffer_size must be non-zero".to_owned());
        }
        if config.rocksdb_high_pri_background_threads > i32::MAX as usize {
            return Err("rocksdb_high_pri_background_threads exceeds i32::MAX".to_owned());
        }
        if config.max_unsealed_segments < 2 {
            return Err("--max-unsealed-segments must be at least 2".to_owned());
        }
        if config.strata_gc && !config.strata_accounting {
            return Err("--strata-gc true requires --strata-accounting true".to_owned());
        }
        if config.strata_gc_io_bytes_per_sec == 0 {
            return Err("--strata-gc-io-bytes-per-sec must be non-zero".to_owned());
        }
        if config.strata_gc_min_io_bytes_per_sec == 0
            || config.strata_gc_min_io_bytes_per_sec > config.strata_gc_io_bytes_per_sec
        {
            return Err(
                "--strata-gc-min-io-bytes-per-sec must be non-zero and no greater than --strata-gc-io-bytes-per-sec"
                    .to_owned(),
            );
        }
        if config.case.is_delete() && config.delete_setup_timeout.is_zero() {
            return Err("--delete-setup-timeout must be non-zero".to_owned());
        }
        if config.delete_reclaim_mode == DeleteReclaimMode::Background
            && config.reclaim_duration.is_zero()
        {
            return Err(
                "--reclaim-duration must be non-zero for background reclamation".to_owned(),
            );
        }
        if config.delete_reclaim_mode != DeleteReclaimMode::Background
            && config.post_delete_workload == PostDeleteWorkload::Steady
        {
            return Err(
                "--post-delete-workload steady requires --delete-reclaim background".to_owned(),
            );
        }
        if config.post_delete_workload == PostDeleteWorkload::Steady
            && config.post_delete_ops_per_second == 0
        {
            return Err(
                "--post-delete-ops-per-second must be non-zero for steady traffic".to_owned(),
            );
        }
        if config.post_delete_ops_per_second > 1_000_000_000 {
            return Err("--post-delete-ops-per-second must be at most 1000000000".to_owned());
        }
        let post_delete_mix_bps = percent_to_basis_points_inclusive(config.post_delete_put_percent)
            + percent_to_basis_points_inclusive(config.post_delete_get_percent)
            + percent_to_basis_points_inclusive(config.post_delete_delete_percent);
        if post_delete_mix_bps != 10_000 {
            return Err(format!(
                "post-delete put/get/delete percentages must sum to 100, got {:.6}",
                config.post_delete_put_percent
                    + config.post_delete_get_percent
                    + config.post_delete_delete_percent
            ));
        }
        if reclaim_sample_at_explicit
            && config
                .reclaim_sample_at
                .iter()
                .any(|sample| *sample > config.reclaim_duration)
        {
            return Err(
                "--reclaim-sample-at cannot contain a time after --reclaim-duration".to_owned(),
            );
        }
        if config.reuse_existing
            && !matches!(
                config.case,
                BenchCase::StoreGet
                    | BenchCase::RocksDbBlobDbGet
                    | BenchCase::RocksDbBlobDbGetPinned
            )
        {
            return Err(
                "--reuse-existing is only supported for store-get, rocksdb-blobdb-get, and rocksdb-blobdb-get-pinned"
                    .to_owned(),
            );
        }
        if config.reuse_existing && config.root_was_defaulted {
            return Err("--reuse-existing requires --root <path>".to_owned());
        }
        if config.rocksdb_get_profile
            && !matches!(
                config.case,
                BenchCase::RocksDbBlobDbGet | BenchCase::RocksDbBlobDbGetPinned
            )
        {
            return Err(
                "--rocksdb-get-profile is only supported for rocksdb-blobdb-get and rocksdb-blobdb-get-pinned"
                    .to_owned(),
            );
        }

        Ok(config)
    }

    fn effective_read_set_size(&self) -> usize {
        if self.reuse_existing {
            self.read_set_size
        } else {
            self.read_set_size.min(self.ops.max(1))
        }
    }

    fn effective_reclaim_sample_at(&self) -> Vec<Duration> {
        if self.delete_reclaim_mode == DeleteReclaimMode::None {
            return vec![Duration::ZERO];
        }

        let mut samples = self.reclaim_sample_at.clone();
        samples.retain(|sample| *sample <= self.reclaim_duration);
        samples.push(Duration::ZERO);
        samples.push(self.reclaim_duration);
        samples.sort_unstable();
        samples.dedup();
        samples
    }

    fn store_config(&self) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: self.root_dir.clone(),
            namespace: self.namespace.clone(),
            segment_max_bytes: self.segment_max_bytes,
            write_queue_capacity: self.queue_capacity,
            max_unsealed_segments: self.max_unsealed_segments,
            seal_worker_count: self.seal_worker_count,
            segment_reader_cache_capacity: self.reader_cache_capacity,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: self.sealed_segment_integrity_policy,
            accounting_worker_enabled: self.strata_accounting,
            accounting_interval: DEFAULT_ACCOUNTING_INTERVAL,
            accounting_unaccounted_threshold: DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
            accounting_materialize_lag_threshold: self.strata_accounting_materialize_lag_threshold,
            accounting_partition_count: DEFAULT_ACCOUNTING_PARTITION_COUNT,
            accounting_maintenance_interval: DEFAULT_ACCOUNTING_MAINTENANCE_INTERVAL,
            accounting_ingest_record_threshold: DEFAULT_ACCOUNTING_INGEST_RECORD_THRESHOLD,
            accounting_delta_run_count_threshold: DEFAULT_ACCOUNTING_DELTA_RUN_COUNT_THRESHOLD,
            accounting_delta_run_bytes_threshold: DEFAULT_ACCOUNTING_DELTA_RUN_BYTES_THRESHOLD,
            accounting_major_patch_count_threshold: DEFAULT_ACCOUNTING_MAJOR_PATCH_COUNT_THRESHOLD,
            accounting_major_patch_bytes_threshold: DEFAULT_ACCOUNTING_MAJOR_PATCH_BYTES_THRESHOLD,
            gc_workers_enabled: self.strata_gc,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: self.strata_gc_io_bytes_per_sec,
            gc_min_io_bytes_per_sec: self.strata_gc_min_io_bytes_per_sec,
            gc_planner_config: self.gc_planner_config(),
            gc_max_accounting_lag_lsn: DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: self.starting_epoch,
        }
    }

    fn gc_planner_config(&self) -> GcPlannerConfig {
        GcPlannerConfig {
            min_reclaim_bytes: self.strata_gc_min_reclaim_bytes,
            min_garbage_ratio_bps: self.strata_gc_min_garbage_ratio_bps,
            ..GcPlannerConfig::default()
        }
    }
}

#[derive(Debug, Default)]
struct StoreGetProfileSummary {
    count: usize,
    op_total: Duration,
    record_lookup: Duration,
    reader_acquire: Duration,
    fixed_header: Duration,
    buffer_alloc: Duration,
    record_body: Duration,
    decode: Duration,
    key_validate: Duration,
}

impl StoreGetProfileSummary {
    fn add(&mut self, op_elapsed: Duration, profile: StoreGetProfile) {
        self.count += 1;
        self.op_total += op_elapsed;
        self.record_lookup += profile.record_lookup;
        self.reader_acquire += profile.reader_acquire;
        self.fixed_header += profile.fixed_header;
        self.buffer_alloc += profile.buffer_alloc;
        self.record_body += profile.record_body;
        self.decode += profile.decode;
        self.key_validate += profile.key_validate;
    }

    fn accounted(&self) -> Duration {
        self.record_lookup
            + self.reader_acquire
            + self.fixed_header
            + self.buffer_alloc
            + self.record_body
            + self.decode
            + self.key_validate
    }
}

#[derive(Debug)]
struct PhaseTimings {
    primary_name: &'static str,
    primary: Vec<Duration>,
    sync: Vec<Duration>,
}

impl PhaseTimings {
    fn new(primary_name: &'static str, capacity: usize) -> Self {
        Self {
            primary_name,
            primary: Vec::with_capacity(capacity),
            sync: Vec::new(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PathSummary {
    bytes: u64,
    allocated_bytes: u64,
    file_count: u64,
    directory_count: u64,
    data_file_count: u64,
    data_file_bytes: u64,
    sst_file_count: u64,
    sst_file_bytes: u64,
    blob_file_count: u64,
    blob_file_bytes: u64,
    log_file_count: u64,
    log_file_bytes: u64,
    manifest_file_count: u64,
}

impl PathSummary {
    fn payload_file_bytes(self) -> u64 {
        self.data_file_bytes.saturating_add(self.blob_file_bytes)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SteadyOperationCounts {
    puts: u64,
    gets: u64,
    deletes: u64,
    syncs: u64,
}

impl SteadyOperationCounts {
    fn operations(self) -> u64 {
        self.puts
            .saturating_add(self.gets)
            .saturating_add(self.deletes)
    }

    fn mutations(self) -> u64 {
        self.puts.saturating_add(self.deletes)
    }
}

#[derive(Debug, Default)]
struct SteadyWorkloadSummary {
    counts: SteadyOperationCounts,
    put_timings: Vec<Duration>,
    get_timings: Vec<Duration>,
    delete_timings: Vec<Duration>,
    sync_timings: Vec<Duration>,
}

impl SteadyWorkloadSummary {
    fn record_action(&mut self, action: SteadyAction, elapsed: Duration) {
        match action {
            SteadyAction::Put(_) => {
                self.counts.puts = self.counts.puts.saturating_add(1);
                self.put_timings.push(elapsed);
            }
            SteadyAction::GetOriginal(_) | SteadyAction::GetSteady(_) => {
                self.counts.gets = self.counts.gets.saturating_add(1);
                self.get_timings.push(elapsed);
            }
            SteadyAction::Delete(_) => {
                self.counts.deletes = self.counts.deletes.saturating_add(1);
                self.delete_timings.push(elapsed);
            }
        }
    }

    fn record_sync(&mut self, elapsed: Duration) {
        self.counts.syncs = self.counts.syncs.saturating_add(1);
        self.sync_timings.push(elapsed);
    }

    fn merge(&mut self, mut other: Self) {
        self.counts.puts = self.counts.puts.saturating_add(other.counts.puts);
        self.counts.gets = self.counts.gets.saturating_add(other.counts.gets);
        self.counts.deletes = self.counts.deletes.saturating_add(other.counts.deletes);
        self.counts.syncs = self.counts.syncs.saturating_add(other.counts.syncs);
        self.put_timings.append(&mut other.put_timings);
        self.get_timings.append(&mut other.get_timings);
        self.delete_timings.append(&mut other.delete_timings);
        self.sync_timings.append(&mut other.sync_timings);
    }
}

#[derive(Debug, Default)]
struct ConcurrentSteadyOperationCounts {
    puts: AtomicU64,
    gets: AtomicU64,
    deletes: AtomicU64,
    syncs: AtomicU64,
    mutations: AtomicU64,
}

impl ConcurrentSteadyOperationCounts {
    fn record_action(&self, action: SteadyAction) -> Option<u64> {
        match action {
            SteadyAction::Put(_) => {
                self.puts.fetch_add(1, Ordering::Relaxed);
                Some(self.mutations.fetch_add(1, Ordering::Relaxed) + 1)
            }
            SteadyAction::GetOriginal(_) | SteadyAction::GetSteady(_) => {
                self.gets.fetch_add(1, Ordering::Relaxed);
                None
            }
            SteadyAction::Delete(_) => {
                self.deletes.fetch_add(1, Ordering::Relaxed);
                Some(self.mutations.fetch_add(1, Ordering::Relaxed) + 1)
            }
        }
    }

    fn record_sync(&self) {
        self.syncs.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> SteadyOperationCounts {
        SteadyOperationCounts {
            puts: self.puts.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            syncs: self.syncs.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct ReclaimSample {
    target: Duration,
    elapsed: Duration,
    path: PathSummary,
    io: Option<ProcessIoSnapshot>,
    workload_counts: SteadyOperationCounts,
    strata_accounting: Option<StrataAccountingSample>,
}

#[derive(Debug, Default, Clone, Copy)]
struct StrataAccountingSample {
    durable_lsn: u64,
    accounted_lsn: u64,
    known_total_bytes: u64,
    known_live_bytes: u64,
    known_retired_bytes: u64,
    known_expired_bytes: u64,
    known_live_ref_count: u64,
}

#[derive(Debug, Default)]
struct ReclaimTimeline {
    samples: Vec<ReclaimSample>,
    workload: SteadyWorkloadSummary,
    final_sync: Duration,
}

struct DeleteReportInputs<'a> {
    config: &'a Config,
    loaded_keys: usize,
    deleted_keys: usize,
    loaded: PathSummary,
    baseline: PathSummary,
    post_delete: PathSummary,
    empty_io: Option<ProcessIoSnapshot>,
    loaded_io: Option<ProcessIoSnapshot>,
    baseline_io: Option<ProcessIoSnapshot>,
    post_delete_io: Option<ProcessIoSnapshot>,
    final_sync: Duration,
    reclaim_timeline: &'a ReclaimTimeline,
}

#[derive(Debug, Clone, Copy)]
struct ProcessIoSnapshot {
    read_bytes: u64,
    write_bytes: u64,
}

impl ProcessIoSnapshot {
    fn saturating_delta(self, earlier: Self) -> Self {
        Self {
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
        }
    }
}

#[cfg(feature = "internal-profiling")]
#[derive(Debug, Default, Clone, Copy)]
struct StoreWriteProfileSummary {
    count: usize,
    queue_send: Duration,
    queue_wait: Duration,
    prepare_batch: Duration,
    segment_capacity: Duration,
    segment_append: Duration,
    accounting_delta_append: Duration,
    index_batch_commit: Duration,
    rollover_post_commit: Duration,
    accounting_nudge: Duration,
    response_send: Duration,
    writer_total: Duration,
}

#[cfg(feature = "internal-profiling")]
impl StoreWriteProfileSummary {
    fn add(&mut self, profile: StoreWriteProfile) {
        self.count += 1;
        self.queue_send += profile.queue_send;
        self.queue_wait += profile.queue_wait;
        self.prepare_batch += profile.prepare_batch;
        self.segment_capacity += profile.segment_capacity;
        self.segment_append += profile.segment_append;
        self.accounting_delta_append += profile.accounting_delta_append;
        self.index_batch_commit += profile.index_batch_commit;
        self.rollover_post_commit += profile.rollover_post_commit;
        self.accounting_nudge += profile.accounting_nudge;
        self.response_send += profile.response_send;
        self.writer_total += profile.writer_total;
    }
}

#[cfg(feature = "internal-profiling")]
#[derive(Debug, Default, Clone, Copy)]
struct StoreSyncProfileSummary {
    count: usize,
    queue_send: Duration,
    queue_wait: Duration,
    segment_sync: Duration,
    accounting_delta_sync: Duration,
    durable_lsn_compute: Duration,
    index_batch_commit: Duration,
    state_update: Duration,
    accounting_nudge: Duration,
    response_send: Duration,
    writer_total: Duration,
}

#[cfg(feature = "internal-profiling")]
impl StoreSyncProfileSummary {
    fn add(&mut self, profile: StoreSyncProfile) {
        self.count += 1;
        self.queue_send += profile.queue_send;
        self.queue_wait += profile.queue_wait;
        self.segment_sync += profile.segment_sync;
        self.accounting_delta_sync += profile.accounting_delta_sync;
        self.durable_lsn_compute += profile.durable_lsn_compute;
        self.index_batch_commit += profile.index_batch_commit;
        self.state_update += profile.state_update;
        self.accounting_nudge += profile.accounting_nudge;
        self.response_send += profile.response_send;
        self.writer_total += profile.writer_total;
    }
}

#[cfg(feature = "internal-profiling")]
#[derive(Debug, Default)]
struct BenchProfileSink {
    write: Mutex<StoreWriteProfileSummary>,
    sync: Mutex<StoreSyncProfileSummary>,
}

#[cfg(feature = "internal-profiling")]
impl BenchProfileSink {
    fn write_summary(&self) -> StoreWriteProfileSummary {
        *self.write.lock().expect("write profile lock poisoned")
    }

    fn sync_summary(&self) -> StoreSyncProfileSummary {
        *self.sync.lock().expect("sync profile lock poisoned")
    }
}

#[cfg(feature = "internal-profiling")]
impl StoreProfileSink for BenchProfileSink {
    fn record_write(&self, profile: StoreWriteProfile) {
        self.write
            .lock()
            .expect("write profile lock poisoned")
            .add(profile);
    }

    fn record_sync(&self, profile: StoreSyncProfile) {
        self.sync
            .lock()
            .expect("sync profile lock poisoned")
            .add(profile);
    }
}

#[cfg(feature = "internal-profiling")]
type ProfileCapture = Arc<BenchProfileSink>;

#[cfg(not(feature = "internal-profiling"))]
#[derive(Debug, Default)]
struct ProfileCapture;

#[cfg(feature = "internal-profiling")]
fn metrics_with_profile_capture(
    metrics: StrataStoreMetrics,
) -> (StrataStoreMetrics, ProfileCapture) {
    let capture = Arc::new(BenchProfileSink::default());
    (metrics.with_profile_sink(capture.clone()), capture)
}

#[cfg(not(feature = "internal-profiling"))]
fn metrics_with_profile_capture(
    metrics: StrataStoreMetrics,
) -> (StrataStoreMetrics, ProfileCapture) {
    (metrics, ProfileCapture)
}

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let bench_metrics = BenchMetrics::start(&config)?;

    if config.reuse_existing {
        if !config.root_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "--reuse-existing root does not exist or is not a directory: {}",
                    config.root_dir.display()
                ),
            )
            .into());
        }
    } else {
        if config.root_dir.exists() {
            fs::remove_dir_all(&config.root_dir)?;
        }
        fs::create_dir_all(&config.root_dir)?;
    }

    let result = match config.case {
        BenchCase::SegmentAppend => run_segment_append(&config),
        BenchCase::StorePut => run_store_put(&config, &bench_metrics),
        BenchCase::StorePutArc => run_store_put_arc(&config, &bench_metrics),
        BenchCase::StoreGet => run_store_get(&config, &bench_metrics),
        BenchCase::StoreDelete => run_store_delete(&config, &bench_metrics),
        BenchCase::RocksDbBlobDbPut => run_rocksdb_blobdb_put(&config),
        BenchCase::RocksDbBlobDbGet => {
            run_rocksdb_blobdb_get(&config, &bench_metrics, RocksDbGetMode::Decoded)
        }
        BenchCase::RocksDbBlobDbGetPinned => {
            run_rocksdb_blobdb_get(&config, &bench_metrics, RocksDbGetMode::Pinned)
        }
        BenchCase::RocksDbBlobDbDelete => run_rocksdb_blobdb_delete(&config),
    };

    if bench_metrics.is_enabled() && config.metrics_drain_seconds != 0 {
        eprintln!("metrics_drain_seconds={}", config.metrics_drain_seconds);
        thread::sleep(Duration::from_secs(config.metrics_drain_seconds));
    }

    if config.root_was_defaulted && !config.keep_data {
        let _ = fs::remove_dir_all(&config.root_dir);
    }

    result
}

fn run_segment_append(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let key_prefix = b"segment-key-";
    let segment_path = config.root_dir.join("segment-append.data");
    let mut writer = SegmentWriter::create(
        &segment_path,
        BENCH_SEGMENT_ID,
        PlacementClass::Ingest,
        config.segment_max_bytes,
    )?;
    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new("append", config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(key_prefix, op)?;
        let op_started = Instant::now();
        let phase_started = Instant::now();
        let record_ref = writer.append(&key, 0, &payload)?.record_ref;
        phases.primary.push(phase_started.elapsed());
        hint::black_box(record_ref);
        if let Some(sync_elapsed) =
            record_sync_timed(config.sync_every, op + 1, || writer.sync_data())?
        {
            phases.sync.push(sync_elapsed);
        }
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: None,
        rocksdb: None,
        profile_capture: None,
    })?;
    Ok(())
}

fn run_store_put(
    config: &Config,
    bench_metrics: &BenchMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store_config = config.store_config();
    let (metrics, profile_capture) =
        metrics_with_profile_capture(bench_metrics.store_metrics(&config.namespace)?);
    let store = StrataStore::open(store_config, metrics)?;
    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new("store_put", config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let phase_started = Instant::now();
        let lsn = store.put(0, &key, &payload)?;
        phases.primary.push(phase_started.elapsed());
        hint::black_box(lsn);
        if should_sync(config.sync_every, op + 1) {
            let phase_started = Instant::now();
            store.sync()?;
            phases.sync.push(phase_started.elapsed());
        }
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: Some(&store),
        rocksdb: None,
        profile_capture: Some(&profile_capture),
    })?;
    Ok(())
}

fn run_store_put_arc(
    config: &Config,
    bench_metrics: &BenchMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Arc<[u8]> = Arc::from(payload(config.payload_size));
    let store_config = config.store_config();
    let (metrics, profile_capture) =
        metrics_with_profile_capture(bench_metrics.store_metrics(&config.namespace)?);
    let store = StrataStore::open(store_config, metrics)?;
    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new("store_put_arc", config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let phase_started = Instant::now();
        let lsn = store.put_arc(0, key, payload.clone())?;
        phases.primary.push(phase_started.elapsed());
        hint::black_box(lsn);
        if should_sync(config.sync_every, op + 1) {
            let phase_started = Instant::now();
            store.sync()?;
            phases.sync.push(phase_started.elapsed());
        }
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: Some(&store),
        rocksdb: None,
        profile_capture: Some(&profile_capture),
    })?;
    Ok(())
}

fn run_store_get(
    config: &Config,
    bench_metrics: &BenchMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store_config = config.store_config();
    let store = StrataStore::open(
        store_config,
        bench_metrics.store_metrics(&config.namespace)?,
    )?;
    let read_set_size = config.effective_read_set_size();
    let key_prefix: &[u8] = if config.reuse_existing {
        b"store-key-"
    } else {
        b"read-key-"
    };
    let keys = (0..read_set_size)
        .map(|op| bench_key(key_prefix, op))
        .collect::<Result<Vec<_>, _>>()?;

    if !config.reuse_existing {
        for key in &keys {
            store.put(0, key, &payload)?;
        }
        store.sync()?;
    }

    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new("store_get", config.ops);
    let mut profile_summary = config
        .store_get_profile
        .then(StoreGetProfileSummary::default);
    let read_options = ReadOptions {
        verify_checksum: config.store_get_verify_checksum,
    };
    let started = Instant::now();
    let key_indexes = ReadKeySequence::new(config.read_pattern, keys.len(), config.read_seed);

    for key_index in key_indexes.take(config.ops) {
        let key = &keys[key_index];
        let op_started = Instant::now();
        let phase_started = Instant::now();
        match config.store_get_mode {
            StoreGetMode::Payload => {
                if let Some(profile_summary) = &mut profile_summary {
                    let (value, profile) =
                        store.get_blob_profiled_with_options(key, read_options)?;
                    verify_reused_key_found(config, value.is_some(), key.as_bytes())?;
                    hint::black_box(value.as_deref());
                    phases.primary.push(phase_started.elapsed());
                    let op_elapsed = op_started.elapsed();
                    profile_summary.add(op_elapsed, profile);
                    timings.push(op_elapsed);
                    continue;
                }

                let value = store.get_with_options(key, read_options)?;
                verify_reused_key_found(config, value.is_some(), key.as_bytes())?;
                hint::black_box(value.as_deref());
            }
            StoreGetMode::KeyOnly => {
                let exists = store.contains(key)?;
                verify_reused_key_found(config, exists, key.as_bytes())?;
                hint::black_box(exists);
            }
        }
        phases.primary.push(phase_started.elapsed());
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: profile_summary.as_ref(),
        phases: Some(&phases),
        store: Some(&store),
        rocksdb: None,
        profile_capture: None,
    })?;
    Ok(())
}

fn run_store_delete(
    config: &Config,
    bench_metrics: &BenchMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store_config = config.store_config();
    let (metrics, profile_capture) =
        metrics_with_profile_capture(bench_metrics.store_metrics(&config.namespace)?);
    let store = StrataStore::open(store_config, metrics)?;
    let keys = (0..config.ops)
        .map(|op| bench_key(b"store-delete-key-", op))
        .collect::<Result<Vec<_>, _>>()?;
    let empty_io = process_io_snapshot()?;

    for key in &keys {
        store.put(0, key, &payload)?;
    }
    store.sync()?;
    store.checkpoint_active_segment()?;
    wait_for_strata_sealing(&store, config.delete_setup_timeout)?;
    wait_for_strata_accounting(&store, config.delete_setup_timeout)?;

    let loaded = summarize_path_if_exists(&config.root_dir)?;
    let loaded_io = process_io_snapshot()?;
    let baseline = loaded;
    let baseline_io = loaded_io;

    let delete_indexes = delete_indexes(
        config.ops,
        config.delete_percent,
        config.delete_pattern,
        config.delete_seed,
    );
    let mut timings = Vec::with_capacity(delete_indexes.len());
    let mut phases = PhaseTimings::new("store_delete", delete_indexes.len());
    let started = Instant::now();
    for (completed, key_index) in delete_indexes.iter().copied().enumerate() {
        let op_started = Instant::now();
        let phase_started = Instant::now();
        let lsn = store.tombstone(&keys[key_index])?;
        phases.primary.push(phase_started.elapsed());
        hint::black_box(lsn);
        if should_sync(config.sync_every, completed + 1) {
            let sync_started = Instant::now();
            store.sync()?;
            phases.sync.push(sync_started.elapsed());
        }
        timings.push(op_started.elapsed());
    }
    let elapsed = started.elapsed();
    let final_sync_started = Instant::now();
    store.sync()?;
    let final_sync = final_sync_started.elapsed();

    verify_strata_delete_samples(&store, &keys, &delete_indexes, config.delete_verify_samples)?;
    let post_delete_io = process_io_snapshot()?;
    let post_delete = summarize_path_if_exists(&config.root_dir)?;
    let live_indexes = live_indexes(keys.len(), &delete_indexes);
    let reclaim_timeline = run_reclaim_timeline(
        config,
        &live_indexes,
        |action| {
            match action {
                SteadyAction::Put(index) => {
                    let key = bench_key(b"store-post-delete-key-", index)?;
                    hint::black_box(store.put(0, &key, &payload)?);
                }
                SteadyAction::GetOriginal(index) => {
                    let value = store.get(&keys[index])?;
                    if value.is_none() {
                        return Err(io::Error::other(format!(
                            "Strata steady get missed original key {index}"
                        ))
                        .into());
                    }
                    hint::black_box(value);
                }
                SteadyAction::GetSteady(index) => {
                    let key = bench_key(b"store-post-delete-key-", index)?;
                    let value = store.get(&key)?;
                    if value.is_none() {
                        return Err(io::Error::other(format!(
                            "Strata steady get missed generated key {index}"
                        ))
                        .into());
                    }
                    hint::black_box(value);
                }
                SteadyAction::Delete(index) => {
                    let key = bench_key(b"store-post-delete-key-", index)?;
                    hint::black_box(store.tombstone(&key)?);
                }
            }
            Ok(())
        },
        || {
            store.sync()?;
            Ok(())
        },
        || strata_accounting_sample(&store).map(Some),
    )?;
    verify_strata_delete_samples(&store, &keys, &delete_indexes, config.delete_verify_samples)?;

    print_delete_report(DeleteReportInputs {
        config,
        loaded_keys: keys.len(),
        deleted_keys: delete_indexes.len(),
        loaded,
        baseline,
        post_delete,
        empty_io,
        loaded_io,
        baseline_io,
        post_delete_io,
        final_sync,
        reclaim_timeline: &reclaim_timeline,
    });
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: Some(&store),
        rocksdb: None,
        profile_capture: Some(&profile_capture),
    })?;
    Ok(())
}

fn run_rocksdb_blobdb_put(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = BlobDbValue(payload(config.payload_size));
    let db = open_typed_rocksdb_blobdb(config)?;
    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new("rocksdb_put", config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"rocksdb-key-", op)?;
        let key = key.as_bytes().to_vec();
        let op_started = Instant::now();
        let phase_started = Instant::now();
        insert_rocksdb_blobdb(&db, &key, &payload, config.rocksdb_disable_wal)?;
        phases.primary.push(phase_started.elapsed());
        if let Some(sync_elapsed) = record_sync_timed(config.sync_every, op + 1, || {
            flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true)
        })? {
            phases.sync.push(sync_elapsed);
        }
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: None,
        rocksdb: Some(db.rocksdb.as_ref()),
        profile_capture: None,
    })?;
    Ok(())
}

fn run_rocksdb_blobdb_delete(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = BlobDbValue(payload(config.payload_size));
    let db = open_typed_rocksdb_blobdb(config)?;
    let keys = (0..config.ops)
        .map(|op| bench_key(b"rocksdb-delete-key-", op))
        .map(|result| result.map(|key| key.as_bytes().to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
    let empty_io = process_io_snapshot()?;

    for key in &keys {
        insert_rocksdb_blobdb(&db, key, &payload, config.rocksdb_disable_wal)?;
    }
    flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true)?;
    db.flush()?;
    let baseline = summarize_path_if_exists(&config.root_dir)?;
    let baseline_io = process_io_snapshot()?;
    let loaded = baseline;
    let loaded_io = baseline_io;

    let delete_indexes = delete_indexes(
        config.ops,
        config.delete_percent,
        config.delete_pattern,
        config.delete_seed,
    );
    let mut timings = Vec::with_capacity(delete_indexes.len());
    let mut phases = PhaseTimings::new("rocksdb_delete", delete_indexes.len());
    let started = Instant::now();
    for (completed, key_index) in delete_indexes.iter().copied().enumerate() {
        let op_started = Instant::now();
        let phase_started = Instant::now();
        delete_rocksdb_blobdb(&db, &keys[key_index], config.rocksdb_disable_wal)?;
        phases.primary.push(phase_started.elapsed());
        if let Some(sync_elapsed) = record_sync_timed(config.sync_every, completed + 1, || {
            flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true)
        })? {
            phases.sync.push(sync_elapsed);
        }
        timings.push(op_started.elapsed());
    }
    let elapsed = started.elapsed();
    let final_sync_started = Instant::now();
    flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true)?;
    let final_sync = final_sync_started.elapsed();

    verify_rocksdb_delete_samples(&db, &keys, &delete_indexes, config.delete_verify_samples)?;
    let post_delete_io = process_io_snapshot()?;
    let post_delete = summarize_path_if_exists(&config.root_dir)?;
    let live_indexes = live_indexes(keys.len(), &delete_indexes);
    let reclaim_timeline = run_reclaim_timeline(
        config,
        &live_indexes,
        |action| {
            match action {
                SteadyAction::Put(index) => {
                    let key = bench_key(b"rocksdb-post-delete-key-", index)?
                        .as_bytes()
                        .to_vec();
                    insert_rocksdb_blobdb(&db, &key, &payload, config.rocksdb_disable_wal)?;
                }
                SteadyAction::GetOriginal(index) => {
                    let value = db.get(&keys[index])?;
                    if value.is_none() {
                        return Err(io::Error::other(format!(
                            "BlobDB steady get missed original key {index}"
                        ))
                        .into());
                    }
                    hint::black_box(value);
                }
                SteadyAction::GetSteady(index) => {
                    let key = bench_key(b"rocksdb-post-delete-key-", index)?
                        .as_bytes()
                        .to_vec();
                    let value = db.get(&key)?;
                    if value.is_none() {
                        return Err(io::Error::other(format!(
                            "BlobDB steady get missed generated key {index}"
                        ))
                        .into());
                    }
                    hint::black_box(value);
                }
                SteadyAction::Delete(index) => {
                    let key = bench_key(b"rocksdb-post-delete-key-", index)?
                        .as_bytes()
                        .to_vec();
                    delete_rocksdb_blobdb(&db, &key, config.rocksdb_disable_wal)?;
                }
            }
            Ok(())
        },
        || flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true),
        || Ok(None),
    )?;
    verify_rocksdb_delete_samples(&db, &keys, &delete_indexes, config.delete_verify_samples)?;

    print_delete_report(DeleteReportInputs {
        config,
        loaded_keys: keys.len(),
        deleted_keys: delete_indexes.len(),
        loaded,
        baseline,
        post_delete,
        empty_io,
        loaded_io,
        baseline_io,
        post_delete_io,
        final_sync,
        reclaim_timeline: &reclaim_timeline,
    });
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: None,
        rocksdb: Some(db.rocksdb.as_ref()),
        profile_capture: None,
    })?;
    Ok(())
}

struct RocksDbPerfCapture {
    context: PerfContext,
    active: bool,
}

impl RocksDbPerfCapture {
    fn start() -> Self {
        let mut context = PerfContext::default();
        context.reset();
        set_perf_stats(PerfStatsLevel::EnableTime);
        Self {
            context,
            active: true,
        }
    }

    fn finish(mut self) -> String {
        set_perf_stats(PerfStatsLevel::Disable);
        self.active = false;
        self.context.report(false)
    }

    fn report(&self) -> String {
        self.context.report(false)
    }
}

impl Drop for RocksDbPerfCapture {
    fn drop(&mut self) {
        if self.active {
            set_perf_stats(PerfStatsLevel::Disable);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RocksDbGetMode {
    Decoded,
    Pinned,
}

impl RocksDbGetMode {
    fn phase_name(self) -> &'static str {
        match self {
            Self::Decoded => "rocksdb_get",
            Self::Pinned => "rocksdb_get_pinned",
        }
    }
}

fn run_rocksdb_blobdb_get(
    config: &Config,
    bench_metrics: &BenchMetrics,
    mode: RocksDbGetMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = BlobDbValue(payload(config.payload_size));
    let db = open_typed_rocksdb_blobdb(config)?;
    let read_set_size = config.effective_read_set_size();
    let key_prefix: &[u8] = if config.reuse_existing {
        b"rocksdb-key-"
    } else {
        b"rocksdb-read-key-"
    };
    let keys = (0..read_set_size)
        .map(|op| bench_key(key_prefix, op))
        .map(|result| result.map(|key| key.as_bytes().to_vec()))
        .collect::<Result<Vec<_>, _>>()?;

    if !config.reuse_existing {
        for key in &keys {
            insert_rocksdb_blobdb(&db, key, &payload, config.rocksdb_disable_wal)?;
        }
        flush_typed_rocksdb_wal(db.rocksdb.as_ref(), true)?;
    }

    let mut timings = Vec::with_capacity(config.ops);
    let mut phases = PhaseTimings::new(mode.phase_name(), config.ops);
    let perf_capture = config.rocksdb_get_profile.then(RocksDbPerfCapture::start);
    let pinned_read_write_options = ReadWriteOptions::default();
    let started = Instant::now();
    let mut last_perf_metrics_publish = started;
    let key_indexes = ReadKeySequence::new(config.read_pattern, keys.len(), config.read_seed);

    for key_index in key_indexes.take(config.ops) {
        let key = &keys[key_index];
        let op_started = Instant::now();
        let phase_started = Instant::now();
        match mode {
            RocksDbGetMode::Decoded => {
                let value = db.get(key)?;
                verify_reused_key_found(config, value.is_some(), key)?;
                phases.primary.push(phase_started.elapsed());
                hint::black_box(value.as_ref().map(|value| value.0.as_slice()));
                timings.push(op_started.elapsed());
            }
            RocksDbGetMode::Pinned => {
                let get_started = Instant::now();
                let key_buf = be_fix_int_ser(key)?;
                let cf_handle = db.cf()?;
                let read_options = pinned_read_write_options.readopts();
                let value = db
                    .rocksdb
                    .get_pinned_cf_opt(&cf_handle, &key_buf, &read_options)?;
                let found = value.is_some();
                let value_len = value.as_deref().map_or(0, <[u8]>::len);
                record_rocksdb_blobdb_get_metrics(get_started, found, key_buf.len(), value_len);
                verify_reused_key_found(config, found, key)?;
                phases.primary.push(phase_started.elapsed());
                hint::black_box(value.as_deref());
                timings.push(op_started.elapsed());
            }
        }

        if bench_metrics.rocksdb_perf_is_enabled()
            && last_perf_metrics_publish.elapsed() >= ROCKSDB_PERF_METRICS_PUBLISH_INTERVAL
        {
            if let Some(perf_capture) = perf_capture.as_ref() {
                bench_metrics.publish_rocksdb_get_profile(&perf_capture.report(), timings.len());
            }
            last_perf_metrics_publish = Instant::now();
        }
    }

    let elapsed = started.elapsed();
    let perf_report = perf_capture.map(RocksDbPerfCapture::finish);
    if let Some(perf_report) = perf_report.as_deref() {
        bench_metrics.publish_rocksdb_get_profile(perf_report, timings.len());
    }
    print_report(ReportInputs {
        config,
        elapsed,
        timings: &timings,
        profile: None,
        phases: Some(&phases),
        store: None,
        rocksdb: Some(db.rocksdb.as_ref()),
        profile_capture: None,
    })?;
    if let Some(perf_report) = perf_report.as_deref() {
        print_rocksdb_get_profile(perf_report, timings.len());
    }
    Ok(())
}

fn record_rocksdb_blobdb_get_metrics(
    started: Instant,
    found: bool,
    key_len: usize,
    value_len: usize,
) {
    let db_metrics = DBMetrics::get();
    let found = found.to_string();
    db_metrics
        .op_metrics
        .rocksdb_get_latency_seconds
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS, &found])
        .observe(started.elapsed().as_secs_f64());
    db_metrics
        .op_metrics
        .rocksdb_get_key_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe(key_len as f64);
    db_metrics
        .op_metrics
        .rocksdb_get_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe((key_len + value_len) as f64);
    db_metrics
        .op_metrics
        .rocksdb_get_value_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe(value_len as f64);
}

fn verify_reused_key_found(config: &Config, found: bool, key: &[u8]) -> io::Result<()> {
    if config.reuse_existing && !found {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "key from reused write set was not found: {}",
                String::from_utf8_lossy(key)
            ),
        ));
    }
    Ok(())
}

fn delete_indexes(
    key_count: usize,
    delete_percent: f64,
    pattern: DeletePattern,
    seed: u64,
) -> Vec<usize> {
    let delete_count =
        (((key_count as f64) * delete_percent / 100.0).round() as usize).clamp(1, key_count);
    match pattern {
        DeletePattern::Sequential => (0..delete_count).collect(),
        DeletePattern::Random => {
            let mut indexes = (0..key_count).collect::<Vec<_>>();
            let mut rng = SplitMix64::new(seed);
            for index in 0..delete_count {
                let remaining = key_count - index;
                let selected = index + (rng.next_u64() as usize % remaining);
                indexes.swap(index, selected);
            }
            indexes.truncate(delete_count);
            indexes
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SteadyAction {
    Put(usize),
    GetOriginal(usize),
    GetSteady(usize),
    Delete(usize),
}

fn run_reclaim_timeline<Perform, SyncFn, SampleAccounting>(
    config: &Config,
    live_original_indexes: &[usize],
    perform: Perform,
    sync: SyncFn,
    mut sample_accounting: SampleAccounting,
) -> Result<ReclaimTimeline, Box<dyn std::error::Error>>
where
    Perform: Fn(SteadyAction) -> Result<(), Box<dyn std::error::Error>> + Sync,
    SyncFn: Fn() -> Result<(), Box<dyn std::error::Error>> + Sync,
    SampleAccounting: FnMut() -> Result<Option<StrataAccountingSample>, Box<dyn std::error::Error>>,
{
    if config.post_delete_workers == 1
        || config.post_delete_workload == PostDeleteWorkload::Idle
        || config.delete_reclaim_mode == DeleteReclaimMode::None
    {
        return run_reclaim_timeline_serial(
            config,
            live_original_indexes,
            &perform,
            &sync,
            &mut sample_accounting,
        );
    }

    run_reclaim_timeline_parallel(
        config,
        live_original_indexes,
        &perform,
        &sync,
        &mut sample_accounting,
    )
}

fn run_reclaim_timeline_serial<Perform, SyncFn, SampleAccounting>(
    config: &Config,
    live_original_indexes: &[usize],
    perform: &Perform,
    sync: &SyncFn,
    sample_accounting: &mut SampleAccounting,
) -> Result<ReclaimTimeline, Box<dyn std::error::Error>>
where
    Perform: Fn(SteadyAction) -> Result<(), Box<dyn std::error::Error>> + Sync,
    SyncFn: Fn() -> Result<(), Box<dyn std::error::Error>> + Sync,
    SampleAccounting: FnMut() -> Result<Option<StrataAccountingSample>, Box<dyn std::error::Error>>,
{
    let sample_at = config.effective_reclaim_sample_at();
    let mut timeline = ReclaimTimeline::default();
    let started = Instant::now();
    let mut next_operation_at = started;
    let operation_interval = Duration::from_nanos(
        1_000_000_000_u64
            .checked_div(config.post_delete_ops_per_second.max(1))
            .unwrap_or(1)
            .max(1),
    );
    let mut rng = SplitMix64::new(config.post_delete_seed);
    let mut next_steady_key = 0_usize;
    let mut steady_live_keys = VecDeque::new();
    let mut last_synced_mutations = 0_u64;

    for target in sample_at {
        let deadline = started
            .checked_add(target)
            .ok_or_else(|| io::Error::other("reclaim sample time exceeds Instant range"))?;

        while Instant::now() < deadline {
            if config.post_delete_workload == PostDeleteWorkload::Idle
                || config.delete_reclaim_mode == DeleteReclaimMode::None
            {
                thread::sleep(deadline.saturating_duration_since(Instant::now()));
                continue;
            }

            let now = Instant::now();
            if now < next_operation_at {
                thread::sleep(
                    next_operation_at
                        .min(deadline)
                        .saturating_duration_since(now),
                );
                continue;
            }

            let action = next_steady_action(
                config,
                live_original_indexes,
                &mut steady_live_keys,
                &mut next_steady_key,
                &mut rng,
                1,
            );
            let operation_started = Instant::now();
            perform(action)?;
            let operation_elapsed = operation_started.elapsed();
            timeline.workload.record_action(action, operation_elapsed);
            if let SteadyAction::Put(key) = action {
                steady_live_keys.push_back(key);
            }

            let mutations = timeline.workload.counts.mutations();
            if config.sync_every != 0
                && mutations != last_synced_mutations
                && mutations.is_multiple_of(config.sync_every as u64)
            {
                let sync_started = Instant::now();
                sync()?;
                timeline.workload.record_sync(sync_started.elapsed());
                last_synced_mutations = mutations;
            }

            next_operation_at = next_operation_at
                .checked_add(operation_interval)
                .unwrap_or_else(Instant::now);
            if next_operation_at < Instant::now() {
                next_operation_at = Instant::now();
            }
        }

        if target == config.reclaim_duration
            && config.post_delete_workload == PostDeleteWorkload::Steady
            && timeline.workload.counts.mutations() != last_synced_mutations
        {
            let sync_started = Instant::now();
            sync()?;
            timeline.final_sync = sync_started.elapsed();
            timeline.workload.record_sync(timeline.final_sync);
        }

        timeline.samples.push(capture_reclaim_sample(
            config,
            target,
            started,
            timeline.workload.counts,
            sample_accounting,
        )?);
    }

    Ok(timeline)
}

fn run_reclaim_timeline_parallel<Perform, SyncFn, SampleAccounting>(
    config: &Config,
    live_original_indexes: &[usize],
    perform: &Perform,
    sync: &SyncFn,
    sample_accounting: &mut SampleAccounting,
) -> Result<ReclaimTimeline, Box<dyn std::error::Error>>
where
    Perform: Fn(SteadyAction) -> Result<(), Box<dyn std::error::Error>> + Sync,
    SyncFn: Fn() -> Result<(), Box<dyn std::error::Error>> + Sync,
    SampleAccounting: FnMut() -> Result<Option<StrataAccountingSample>, Box<dyn std::error::Error>>,
{
    let sample_at = config.effective_reclaim_sample_at();
    let started = Instant::now();
    let mut timeline = ReclaimTimeline::default();
    timeline.samples.push(capture_reclaim_sample(
        config,
        Duration::ZERO,
        started,
        SteadyOperationCounts::default(),
        sample_accounting,
    )?);

    let shared_counts = ConcurrentSteadyOperationCounts::default();
    let last_synced_mutations = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let worker_error = Mutex::new(None::<String>);
    let sync_lock = Mutex::new(());
    let deadline = started
        .checked_add(config.reclaim_duration)
        .ok_or_else(|| io::Error::other("reclaim duration exceeds Instant range"))?;

    let worker_summaries = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(config.post_delete_workers);
        for worker_index in 0..config.post_delete_workers {
            let shared_counts = &shared_counts;
            let last_synced_mutations = &last_synced_mutations;
            let stop = &stop;
            let worker_error = &worker_error;
            let sync_lock = &sync_lock;
            handles.push(scope.spawn(move || {
                run_post_delete_worker(
                    config,
                    live_original_indexes,
                    worker_index,
                    started,
                    deadline,
                    perform,
                    sync,
                    shared_counts,
                    last_synced_mutations,
                    stop,
                    worker_error,
                    sync_lock,
                )
            }));
        }

        let mut main_error: Option<Box<dyn std::error::Error>> = None;
        for target in sample_at
            .iter()
            .copied()
            .filter(|target| !target.is_zero() && *target < config.reclaim_duration)
        {
            let sample_deadline = started
                .checked_add(target)
                .expect("validated reclaim checkpoint should fit in Instant");
            if let Some(error) = wait_for_post_delete_workers(sample_deadline, &worker_error) {
                main_error = Some(io::Error::other(error).into());
                break;
            }
            match capture_reclaim_sample(
                config,
                target,
                started,
                shared_counts.snapshot(),
                sample_accounting,
            ) {
                Ok(sample) => timeline.samples.push(sample),
                Err(error) => {
                    main_error = Some(error);
                    break;
                }
            }
        }

        if main_error.is_none()
            && let Some(error) = wait_for_post_delete_workers(deadline, &worker_error)
        {
            main_error = Some(io::Error::other(error).into());
        }
        stop.store(true, Ordering::Release);

        let mut summaries = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.join() {
                Ok(summary) => summaries.push(summary),
                Err(_) if main_error.is_none() => {
                    main_error =
                        Some(io::Error::other("post-delete workload worker panicked").into());
                }
                Err(_) => {}
            }
        }
        if main_error.is_none() {
            let error = worker_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(error) = error {
                main_error = Some(io::Error::other(error).into());
            }
        }

        match main_error {
            Some(error) => Err(error),
            None => Ok(summaries),
        }
    })?;

    for summary in worker_summaries {
        timeline.workload.merge(summary);
    }
    if timeline.workload.counts.mutations() != last_synced_mutations.load(Ordering::Acquire) {
        let sync_started = Instant::now();
        sync()?;
        timeline.final_sync = sync_started.elapsed();
        timeline.workload.record_sync(timeline.final_sync);
    }
    timeline.samples.push(capture_reclaim_sample(
        config,
        config.reclaim_duration,
        started,
        timeline.workload.counts,
        sample_accounting,
    )?);

    Ok(timeline)
}

#[allow(clippy::too_many_arguments)]
fn run_post_delete_worker<Perform, SyncFn>(
    config: &Config,
    live_original_indexes: &[usize],
    worker_index: usize,
    started: Instant,
    deadline: Instant,
    perform: &Perform,
    sync: &SyncFn,
    shared_counts: &ConcurrentSteadyOperationCounts,
    last_synced_mutations: &AtomicU64,
    stop: &AtomicBool,
    worker_error: &Mutex<Option<String>>,
    sync_lock: &Mutex<()>,
) -> SteadyWorkloadSummary
where
    Perform: Fn(SteadyAction) -> Result<(), Box<dyn std::error::Error>> + Sync,
    SyncFn: Fn() -> Result<(), Box<dyn std::error::Error>> + Sync,
{
    let global_interval_ns = 1_000_000_000_u64
        .checked_div(config.post_delete_ops_per_second)
        .unwrap_or(1)
        .max(1);
    let worker_count = u64::try_from(config.post_delete_workers).unwrap_or(u64::MAX);
    let worker_interval = Duration::from_nanos(global_interval_ns.saturating_mul(worker_count));
    let initial_delay = Duration::from_nanos(
        global_interval_ns.saturating_mul(u64::try_from(worker_index).unwrap_or(u64::MAX)),
    );
    let mut next_operation_at = started.checked_add(initial_delay).unwrap_or(deadline);
    let mut summary = SteadyWorkloadSummary::default();
    let mut rng = SplitMix64::new(
        config
            .post_delete_seed
            .wrapping_add(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(worker_index as u64)),
    );
    let mut next_steady_key = worker_index;
    let mut steady_live_keys = VecDeque::new();

    while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
        let now = Instant::now();
        if now < next_operation_at {
            thread::sleep(
                next_operation_at
                    .min(deadline)
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(100)),
            );
            continue;
        }

        let action = next_steady_action(
            config,
            live_original_indexes,
            &mut steady_live_keys,
            &mut next_steady_key,
            &mut rng,
            config.post_delete_workers,
        );
        let operation_started = Instant::now();
        if let Err(error) = perform(action) {
            publish_post_delete_worker_error(worker_error, stop, error.to_string());
            break;
        }
        let operation_elapsed = operation_started.elapsed();
        summary.record_action(action, operation_elapsed);
        if let SteadyAction::Put(key) = action {
            steady_live_keys.push_back(key);
        }

        let mutation = shared_counts.record_action(action);
        if let Some(mutation) = mutation
            && config.sync_every != 0
            && mutation.is_multiple_of(config.sync_every as u64)
        {
            let _guard = sync_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let sync_started = Instant::now();
            if let Err(error) = sync() {
                publish_post_delete_worker_error(worker_error, stop, error.to_string());
                break;
            }
            let sync_elapsed = sync_started.elapsed();
            summary.record_sync(sync_elapsed);
            shared_counts.record_sync();
            last_synced_mutations.fetch_max(mutation, Ordering::Release);
        }

        next_operation_at = next_operation_at
            .checked_add(worker_interval)
            .unwrap_or_else(Instant::now);
        if next_operation_at < Instant::now() {
            next_operation_at = Instant::now();
        }
    }

    summary
}

fn wait_for_post_delete_workers(
    deadline: Instant,
    worker_error: &Mutex<Option<String>>,
) -> Option<String> {
    loop {
        if let Some(error) = worker_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            return Some(error);
        }
        let now = Instant::now();
        if now >= deadline {
            return None;
        }
        thread::sleep(
            deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(10)),
        );
    }
}

fn publish_post_delete_worker_error(
    worker_error: &Mutex<Option<String>>,
    stop: &AtomicBool,
    error: String,
) {
    let mut first_error = worker_error
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if first_error.is_none() {
        *first_error = Some(error);
    }
    stop.store(true, Ordering::Release);
}

fn capture_reclaim_sample<SampleAccounting>(
    config: &Config,
    target: Duration,
    started: Instant,
    workload_counts: SteadyOperationCounts,
    sample_accounting: &mut SampleAccounting,
) -> Result<ReclaimSample, Box<dyn std::error::Error>>
where
    SampleAccounting: FnMut() -> Result<Option<StrataAccountingSample>, Box<dyn std::error::Error>>,
{
    Ok(ReclaimSample {
        target,
        elapsed: started.elapsed(),
        path: summarize_path_if_exists(&config.root_dir)?,
        io: process_io_snapshot()?,
        workload_counts,
        strata_accounting: sample_accounting()?,
    })
}

fn strata_accounting_sample(
    store: &StrataStore,
) -> Result<StrataAccountingSample, Box<dyn std::error::Error>> {
    let durable_lsn = store.durable_lsn()?;
    let accounted_lsn = store.accounted_lsn()?;
    let mut sample = StrataAccountingSample {
        durable_lsn,
        accounted_lsn,
        ..StrataAccountingSample::default()
    };
    for (segment_id, state) in store.index().iter_segment_states()? {
        if state.state == SegmentFileState::Deleted {
            continue;
        }
        let Some(overlay) = store.index().get_segment_gc_overlay(segment_id)? else {
            continue;
        };
        sample.known_total_bytes = sample
            .known_total_bytes
            .saturating_add(overlay.summary.total_bytes);
        sample.known_live_bytes = sample
            .known_live_bytes
            .saturating_add(overlay.summary.live_bytes);
        sample.known_retired_bytes = sample
            .known_retired_bytes
            .saturating_add(overlay.summary.retired_bytes);
        sample.known_expired_bytes = sample
            .known_expired_bytes
            .saturating_add(overlay.summary.expired_bytes);
        sample.known_live_ref_count = sample
            .known_live_ref_count
            .saturating_add(overlay.summary.live_ref_count);
    }
    Ok(sample)
}

fn next_steady_action(
    config: &Config,
    live_original_indexes: &[usize],
    steady_live_keys: &mut VecDeque<usize>,
    next_steady_key: &mut usize,
    rng: &mut SplitMix64,
    steady_key_stride: usize,
) -> SteadyAction {
    let put_bps = percent_to_basis_points_inclusive(config.post_delete_put_percent) as u64;
    let get_bps = percent_to_basis_points_inclusive(config.post_delete_get_percent) as u64;
    let roll = rng.next_u64() % 10_000;

    if roll < put_bps {
        return next_steady_put(next_steady_key, steady_key_stride);
    }

    if roll < put_bps + get_bps {
        if !live_original_indexes.is_empty()
            && (steady_live_keys.is_empty() || rng.next_u64().is_multiple_of(2))
        {
            let index = (rng.next_u64() as usize) % live_original_indexes.len();
            return SteadyAction::GetOriginal(live_original_indexes[index]);
        }
        if let Some(&key) = steady_live_keys.front() {
            return SteadyAction::GetSteady(key);
        }
        return next_steady_put(next_steady_key, steady_key_stride);
    }

    match steady_live_keys.pop_front() {
        Some(key) => SteadyAction::Delete(key),
        None => next_steady_put(next_steady_key, steady_key_stride),
    }
}

fn next_steady_put(next_steady_key: &mut usize, steady_key_stride: usize) -> SteadyAction {
    let key = *next_steady_key;
    *next_steady_key = next_steady_key.wrapping_add(steady_key_stride);
    SteadyAction::Put(key)
}

fn live_indexes(key_count: usize, deleted_indexes: &[usize]) -> Vec<usize> {
    let deleted = deleted_bitmap(key_count, deleted_indexes);
    (0..key_count).filter(|index| !deleted[*index]).collect()
}

fn wait_for_strata_sealing(
    store: &StrataStore,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    loop {
        let sealing = store
            .index()
            .iter_segment_states()?
            .into_iter()
            .any(|(_, state)| state.state == SegmentFileState::Sealing);
        if !sealing {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for Strata segment sealing",
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_strata_accounting(
    store: &StrataStore,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let target_lsn = store.durable_lsn()?;
    store.request_accounting_materialization(target_lsn)?;
    let started = Instant::now();
    loop {
        if store.accounted_lsn()? >= target_lsn {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for Strata accounting LSN {target_lsn}"),
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn verify_strata_delete_samples(
    store: &StrataStore,
    keys: &[BlobKey],
    deleted_indexes: &[usize],
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if sample_count == 0 {
        return Ok(());
    }
    for &index in deleted_indexes.iter().take(sample_count) {
        if store.contains(&keys[index])? {
            return Err(
                io::Error::other(format!("Strata deleted key {index} remained visible")).into(),
            );
        }
    }
    let deleted = deleted_bitmap(keys.len(), deleted_indexes);
    for index in (0..keys.len())
        .filter(|index| !deleted[*index])
        .take(sample_count)
    {
        if !store.contains(&keys[index])? {
            return Err(
                io::Error::other(format!("Strata live key {index} was not visible")).into(),
            );
        }
    }
    Ok(())
}

fn verify_rocksdb_delete_samples(
    db: &BlobDbMap,
    keys: &[Vec<u8>],
    deleted_indexes: &[usize],
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if sample_count == 0 {
        return Ok(());
    }
    for &index in deleted_indexes.iter().take(sample_count) {
        if db.contains_key(&keys[index])? {
            return Err(
                io::Error::other(format!("BlobDB deleted key {index} remained visible")).into(),
            );
        }
    }
    let deleted = deleted_bitmap(keys.len(), deleted_indexes);
    for index in (0..keys.len())
        .filter(|index| !deleted[*index])
        .take(sample_count)
    {
        if !db.contains_key(&keys[index])? {
            return Err(
                io::Error::other(format!("BlobDB live key {index} was not visible")).into(),
            );
        }
    }
    Ok(())
}

fn deleted_bitmap(key_count: usize, deleted_indexes: &[usize]) -> Vec<bool> {
    let mut deleted = vec![false; key_count];
    for &index in deleted_indexes {
        deleted[index] = true;
    }
    deleted
}

fn process_io_snapshot() -> io::Result<Option<ProcessIoSnapshot>> {
    let path = Path::new("/proc/self/io");
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path)?;
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in contents.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid /proc/self/io value '{value}': {error}"),
            )
        })?;
        match name {
            "read_bytes" => read_bytes = Some(value),
            "write_bytes" => write_bytes = Some(value),
            _ => {}
        }
    }
    match (read_bytes, write_bytes) {
        (Some(read_bytes), Some(write_bytes)) => Ok(Some(ProcessIoSnapshot {
            read_bytes,
            write_bytes,
        })),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "/proc/self/io did not contain read_bytes and write_bytes",
        )),
    }
}

fn delete_rocksdb_blobdb(
    db: &BlobDbMap,
    key: &Vec<u8>,
    disable_wal: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !disable_wal {
        db.remove(key)?;
        return Ok(());
    }

    let key_buf = be_fix_int_ser(key)?;
    let mut write_options = WriteOptions::default();
    write_options.disable_wal(true);
    db.rocksdb.delete_cf(&db.cf()?, key_buf, &write_options)?;
    Ok(())
}

fn insert_rocksdb_blobdb(
    db: &BlobDbMap,
    key: &Vec<u8>,
    value: &BlobDbValue,
    disable_wal: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !disable_wal {
        db.insert(key, value)?;
        return Ok(());
    }

    let db_metrics = DBMetrics::get();
    let timer = db_metrics
        .op_metrics
        .rocksdb_put_latency_seconds
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .start_timer();
    let key_buf = be_fix_int_ser(key)?;
    let value_buf = bcs::to_bytes(value)?;
    db_metrics
        .op_metrics
        .rocksdb_put_key_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe(key_buf.len() as f64);
    db_metrics
        .op_metrics
        .rocksdb_put_value_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe(value_buf.len() as f64);
    db_metrics
        .op_metrics
        .rocksdb_put_bytes
        .with_label_values(&[ROCKSDB_BLOBDB_CF_CLASS])
        .observe(key_buf.len() as f64 + value_buf.len() as f64);

    let mut write_options = WriteOptions::default();
    write_options.disable_wal(true);
    db.rocksdb
        .put_cf(&db.cf()?, key_buf, value_buf, &write_options)?;
    timer.stop_and_record();
    Ok(())
}

fn open_typed_rocksdb_blobdb(config: &Config) -> Result<BlobDbMap, Box<dyn std::error::Error>> {
    let mut options = default_db_options().options;
    let mut env = Env::new()?;
    env.set_high_priority_background_threads(config.rocksdb_high_pri_background_threads as i32);
    options.set_env(&env);
    options.create_if_missing(true);
    options.set_enable_blob_files(true);
    options.set_min_blob_size(config.rocksdb_min_blob_size);
    options.set_blob_file_size(config.rocksdb_blob_file_size);
    options.set_write_buffer_size(config.rocksdb_write_buffer_size);
    options.set_enable_blob_gc(config.rocksdb_blob_gc);
    options.set_blob_gc_age_cutoff(config.rocksdb_blob_gc_age_cutoff);
    options.set_blob_gc_force_threshold(config.rocksdb_blob_gc_force_threshold);
    options.set_disable_auto_compactions(config.rocksdb_disable_auto_compactions);
    options.enable_statistics();
    let metric_conf = if config.rocksdb_get_profile {
        MetricConf::new(ROCKSDB_BLOBDB_CF_CLASS)
            .with_sampling(SamplingInterval::new(Duration::ZERO, u64::MAX - 1))
    } else {
        MetricConf::new(ROCKSDB_BLOBDB_CF_CLASS)
    };
    Ok(DBMap::open(
        config.root_dir.join("rocksdb-blobdb"),
        metric_conf,
        Some(options),
        None,
        Some(ROCKSDB_BLOBDB_CF_CLASS),
        &ReadWriteOptions::default(),
    )?)
}

fn flush_typed_rocksdb_wal(db: &RocksDB, sync: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db = standard_rocksdb(db).ok_or_else(|| {
        io::Error::other("typed-store BlobDB benchmark requires standard RocksDB")
    })?;
    db.flush_wal(sync)?;
    Ok(())
}

fn record_sync_timed<E>(
    sync_every: usize,
    completed_ops: usize,
    sync: impl FnOnce() -> Result<(), E>,
) -> Result<Option<Duration>, E> {
    if sync_every != 0 && completed_ops.is_multiple_of(sync_every) {
        let started = Instant::now();
        sync()?;
        return Ok(Some(started.elapsed()));
    }
    Ok(None)
}

fn should_sync(sync_every: usize, completed_ops: usize) -> bool {
    sync_every != 0 && completed_ops.is_multiple_of(sync_every)
}

struct BenchMetrics {
    registry: Option<Arc<Registry>>,
    rocksdb_perf: Option<RocksDbPerfPrometheusMetrics>,
    _server: Option<MetricsServer>,
}

impl BenchMetrics {
    fn start(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let Some(listen_addr) = config.metrics_listen.as_deref() else {
            return Ok(Self {
                registry: None,
                rocksdb_perf: None,
                _server: None,
            });
        };

        let registry = Arc::new(Registry::new());
        DBMetrics::init(&registry);
        let rocksdb_perf = config
            .rocksdb_get_profile
            .then(|| RocksDbPerfPrometheusMetrics::new(&registry))
            .transpose()?;
        let server = start_metrics_server(listen_addr, Arc::clone(&registry))?;
        Ok(Self {
            registry: Some(registry),
            rocksdb_perf,
            _server: Some(server),
        })
    }

    fn store_metrics(&self, store_label: &str) -> Result<StrataStoreMetrics, prometheus::Error> {
        match &self.registry {
            Some(registry) => StrataStoreMetrics::new(registry, store_label),
            None => Ok(StrataStoreMetrics::default()),
        }
    }

    fn is_enabled(&self) -> bool {
        self.registry.is_some()
    }

    fn publish_rocksdb_get_profile(&self, report: &str, ops: usize) {
        if let Some(metrics) = &self.rocksdb_perf {
            metrics.set(report, ops);
        }
    }

    fn rocksdb_perf_is_enabled(&self) -> bool {
        self.rocksdb_perf.is_some()
    }
}

struct RocksDbPerfPrometheusMetrics {
    profile_ops: IntGauge,
    counts: IntGaugeVec,
    bytes: IntGaugeVec,
    seconds: GaugeVec,
    seconds_per_op: GaugeVec,
}

impl RocksDbPerfPrometheusMetrics {
    fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let profile_ops = IntGauge::new(
            "strata_bench_rocksdb_perf_profile_ops",
            "Number of RocksDB get operations included in the latest native perf snapshot.",
        )?;
        let counts = IntGaugeVec::new(
            Opts::new(
                "strata_bench_rocksdb_perf_count",
                "Latest cumulative RocksDB native perf count by counter.",
            ),
            &["counter"],
        )?;
        let bytes = IntGaugeVec::new(
            Opts::new(
                "strata_bench_rocksdb_perf_bytes",
                "Latest cumulative RocksDB native perf byte count by counter.",
            ),
            &["counter"],
        )?;
        let seconds = GaugeVec::new(
            Opts::new(
                "strata_bench_rocksdb_perf_seconds",
                "Latest cumulative RocksDB native perf time in seconds by counter.",
            ),
            &["counter"],
        )?;
        let seconds_per_op = GaugeVec::new(
            Opts::new(
                "strata_bench_rocksdb_perf_seconds_per_op",
                "Latest average RocksDB native perf time in seconds per profiled operation.",
            ),
            &["counter"],
        )?;

        registry.register(Box::new(profile_ops.clone()))?;
        registry.register(Box::new(counts.clone()))?;
        registry.register(Box::new(bytes.clone()))?;
        registry.register(Box::new(seconds.clone()))?;
        registry.register(Box::new(seconds_per_op.clone()))?;

        let metrics = Self {
            profile_ops,
            counts,
            bytes,
            seconds,
            seconds_per_op,
        };
        metrics.set("", 0);
        Ok(metrics)
    }

    fn set(&self, report: &str, ops: usize) {
        let counters = parse_rocksdb_perf_report(report);
        self.profile_ops.set(saturating_i64(ops as u64));
        for (counter_name, output_name, kind) in ROCKSDB_GET_PERF_METRICS {
            let value = counters.get(counter_name).copied().unwrap_or_default();
            match kind {
                RocksDbPerfMetricKind::Count => self
                    .counts
                    .with_label_values(&[output_name])
                    .set(saturating_i64(value)),
                RocksDbPerfMetricKind::Bytes => self
                    .bytes
                    .with_label_values(&[output_name])
                    .set(saturating_i64(value)),
                RocksDbPerfMetricKind::Nanos => {
                    let seconds = value as f64 / 1_000_000_000.0;
                    let seconds_per_op = if ops == 0 { 0.0 } else { seconds / ops as f64 };
                    self.seconds.with_label_values(&[output_name]).set(seconds);
                    self.seconds_per_op
                        .with_label_values(&[output_name])
                        .set(seconds_per_op);
                }
            }
        }
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

struct MetricsServer {
    _thread: thread::JoinHandle<()>,
}

fn start_metrics_server(listen_addr: &str, registry: Arc<Registry>) -> io::Result<MetricsServer> {
    let listener = TcpListener::bind(listen_addr)?;
    let local_addr = listener.local_addr()?;
    let thread = thread::Builder::new()
        .name("strata-bench-metrics".to_owned())
        .spawn(move || serve_metrics(listener, registry))
        .map_err(io::Error::other)?;
    eprintln!("metrics_listen={local_addr}");
    Ok(MetricsServer { _thread: thread })
}

fn serve_metrics(listener: TcpListener, registry: Arc<Registry>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(error) = handle_metrics_connection(stream, &registry) {
                    eprintln!(
                        "metrics_connection_error={}",
                        sanitize_property_value(&error.to_string())
                    );
                }
            }
            Err(error) => {
                eprintln!(
                    "metrics_accept_error={}",
                    sanitize_property_value(&error.to_string())
                );
                break;
            }
        }
    }
}

fn handle_metrics_connection(mut stream: TcpStream, registry: &Registry) -> io::Result<()> {
    let mut request = [0_u8; 1024];
    let read = stream.read(&mut request)?;
    let path = request_path(&request[..read]).unwrap_or("");

    match path.split_once('?').map_or(path, |(path, _)| path) {
        "/metrics" => {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut body = Vec::new();
            encoder
                .encode(&metric_families, &mut body)
                .map_err(io::Error::other)?;
            write_http_response(&mut stream, "200 OK", encoder.format_type(), &body)
        }
        "/" => write_http_response(
            &mut stream,
            "200 OK",
            "text/plain; charset=utf-8",
            b"strata-bench metrics: GET /metrics\n",
        ),
        _ => write_http_response(
            &mut stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"not found\n",
        ),
    }
}

fn request_path(request: &[u8]) -> Option<&str> {
    let request = std::str::from_utf8(request).ok()?;
    let line = request.lines().next()?;
    let mut parts = line.split_whitespace();
    if parts.next()? != "GET" {
        return None;
    }
    parts.next()
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

struct ReportInputs<'a> {
    config: &'a Config,
    elapsed: Duration,
    timings: &'a [Duration],
    profile: Option<&'a StoreGetProfileSummary>,
    phases: Option<&'a PhaseTimings>,
    store: Option<&'a StrataStore>,
    rocksdb: Option<&'a RocksDB>,
    profile_capture: Option<&'a ProfileCapture>,
}

fn print_delete_report(inputs: DeleteReportInputs<'_>) {
    let DeleteReportInputs {
        config,
        loaded_keys,
        deleted_keys,
        loaded,
        baseline,
        post_delete,
        empty_io,
        loaded_io,
        baseline_io,
        post_delete_io,
        final_sync,
        reclaim_timeline,
    } = inputs;
    let final_sample = reclaim_timeline
        .samples
        .last()
        .expect("reclaim timeline always contains at least the t=0 sample");
    let post_reclaim = final_sample.path;
    let post_reclaim_io = final_sample.io;
    let logical_deleted_payload_bytes =
        (deleted_keys as u128).saturating_mul(config.payload_size as u128);
    let logical_loaded_payload_bytes =
        (loaded_keys as u128).saturating_mul(config.payload_size as u128);
    let post_delete_put_payload_bytes =
        (reclaim_timeline.workload.counts.puts as u128).saturating_mul(config.payload_size as u128);
    let post_delete_deleted_payload_bytes = (reclaim_timeline.workload.counts.deletes as u128)
        .saturating_mul(config.payload_size as u128);
    let reclaimed_bytes = baseline.bytes.saturating_sub(post_reclaim.bytes);
    let reclaimed_allocated_bytes = baseline
        .allocated_bytes
        .saturating_sub(post_reclaim.allocated_bytes);
    let reclaimed_payload_file_bytes = baseline
        .payload_file_bytes()
        .saturating_sub(post_reclaim.payload_file_bytes());

    println!("delete_loaded_keys={loaded_keys}");
    println!("delete_deleted_keys={deleted_keys}");
    println!("delete_percent={:.6}", config.delete_percent);
    println!("delete_pattern={}", config.delete_pattern.as_str());
    println!("delete_seed={}", config.delete_seed);
    println!("delete_verify_samples={}", config.delete_verify_samples);
    println!(
        "delete_reclaim_mode={}",
        config.delete_reclaim_mode.as_str()
    );
    println!(
        "delete_setup_timeout_ms={}",
        config.delete_setup_timeout.as_millis()
    );
    println!(
        "delete_reclaim_duration_ms={}",
        config.reclaim_duration.as_millis()
    );
    println!(
        "delete_post_delete_workload={}",
        config.post_delete_workload.as_str()
    );
    println!(
        "delete_post_delete_target_ops_per_second={}",
        config.post_delete_ops_per_second
    );
    println!("delete_post_delete_workers={}", config.post_delete_workers);
    println!(
        "delete_post_delete_put_percent={:.6}",
        config.post_delete_put_percent
    );
    println!(
        "delete_post_delete_get_percent={:.6}",
        config.post_delete_get_percent
    );
    println!(
        "delete_post_delete_delete_percent={:.6}",
        config.post_delete_delete_percent
    );
    println!("delete_post_delete_seed={}", config.post_delete_seed);
    println!("delete_loaded_payload_bytes={logical_loaded_payload_bytes}");
    println!("delete_logical_payload_bytes={logical_deleted_payload_bytes}");
    println!("delete_final_sync_us={:.3}", duration_us(&final_sync));
    println!(
        "delete_reclaim_final_sync_us={:.3}",
        duration_us(&reclaim_timeline.final_sync)
    );
    println!(
        "delete_post_delete_operations={}",
        reclaim_timeline.workload.counts.operations()
    );
    let post_delete_elapsed_seconds = final_sample.elapsed.as_secs_f64();
    let post_delete_actual_ops_per_second = if post_delete_elapsed_seconds == 0.0 {
        0.0
    } else {
        reclaim_timeline.workload.counts.operations() as f64 / post_delete_elapsed_seconds
    };
    println!("delete_post_delete_actual_ops_per_second={post_delete_actual_ops_per_second:.3}");
    println!(
        "delete_post_delete_puts={}",
        reclaim_timeline.workload.counts.puts
    );
    println!(
        "delete_post_delete_gets={}",
        reclaim_timeline.workload.counts.gets
    );
    println!(
        "delete_post_delete_deletes={}",
        reclaim_timeline.workload.counts.deletes
    );
    println!(
        "delete_post_delete_syncs={}",
        reclaim_timeline.workload.counts.syncs
    );
    println!("delete_post_delete_put_payload_bytes={post_delete_put_payload_bytes}");
    println!("delete_post_delete_deleted_payload_bytes={post_delete_deleted_payload_bytes}");
    println!(
        "delete_post_delete_directory_delta_bytes={}",
        post_delete.bytes as i128 - baseline.bytes as i128
    );
    println!(
        "delete_final_directory_delta_from_baseline_bytes={}",
        post_reclaim.bytes as i128 - baseline.bytes as i128
    );
    println!(
        "delete_final_directory_delta_from_post_delete_bytes={}",
        post_reclaim.bytes as i128 - post_delete.bytes as i128
    );
    println!("delete_reclaimed_directory_bytes={reclaimed_bytes}");
    println!("delete_reclaimed_allocated_bytes={reclaimed_allocated_bytes}");
    println!("delete_reclaimed_payload_file_bytes={reclaimed_payload_file_bytes}");
    print_ratio(
        "delete_directory_reclaim_efficiency",
        reclaimed_bytes,
        logical_deleted_payload_bytes,
    );
    print_ratio(
        "delete_allocated_reclaim_efficiency",
        reclaimed_allocated_bytes,
        logical_deleted_payload_bytes,
    );
    print_ratio(
        "delete_payload_file_reclaim_efficiency",
        reclaimed_payload_file_bytes,
        logical_deleted_payload_bytes,
    );
    print_path_summary("delete_loaded", &loaded);
    print_path_summary("delete_baseline", &baseline);
    print_path_summary("delete_post_delete", &post_delete);
    print_path_summary("delete_post_reclaim", &post_reclaim);

    println!(
        "delete_reclaim_sample_count={}",
        reclaim_timeline.samples.len()
    );
    for (index, sample) in reclaim_timeline.samples.iter().enumerate() {
        let prefix = format!("delete_reclaim_sample_{index}");
        println!("{prefix}_target_ms={}", sample.target.as_millis());
        println!(
            "{prefix}_elapsed_ms={:.3}",
            sample.elapsed.as_secs_f64() * 1000.0
        );
        println!(
            "{prefix}_directory_delta_from_post_delete_bytes={}",
            sample.path.bytes as i128 - post_delete.bytes as i128
        );
        println!(
            "{prefix}_allocated_reclaimed_from_post_delete_bytes={}",
            post_delete
                .allocated_bytes
                .saturating_sub(sample.path.allocated_bytes)
        );
        println!(
            "{prefix}_payload_file_reclaimed_from_post_delete_bytes={}",
            post_delete
                .payload_file_bytes()
                .saturating_sub(sample.path.payload_file_bytes())
        );
        println!("{prefix}_post_delete_puts={}", sample.workload_counts.puts);
        println!("{prefix}_post_delete_gets={}", sample.workload_counts.gets);
        println!(
            "{prefix}_post_delete_deletes={}",
            sample.workload_counts.deletes
        );
        println!(
            "{prefix}_post_delete_syncs={}",
            sample.workload_counts.syncs
        );
        if let Some(accounting) = sample.strata_accounting {
            println!("{prefix}_strata_durable_lsn={}", accounting.durable_lsn);
            println!("{prefix}_strata_accounted_lsn={}", accounting.accounted_lsn);
            println!(
                "{prefix}_strata_accounting_lag_lsn={}",
                accounting
                    .durable_lsn
                    .saturating_sub(accounting.accounted_lsn)
            );
            println!(
                "{prefix}_strata_gc_known_total_bytes={}",
                accounting.known_total_bytes
            );
            println!(
                "{prefix}_strata_gc_known_live_bytes={}",
                accounting.known_live_bytes
            );
            println!(
                "{prefix}_strata_gc_known_retired_bytes={}",
                accounting.known_retired_bytes
            );
            println!(
                "{prefix}_strata_gc_known_expired_bytes={}",
                accounting.known_expired_bytes
            );
            println!(
                "{prefix}_strata_gc_known_live_ref_count={}",
                accounting.known_live_ref_count
            );
        }
        print_path_summary(&prefix, &sample.path);
        if let (Some(sample_io), Some(t0_io)) = (sample.io, post_delete_io) {
            print_process_io_delta(
                &format!("{prefix}_cumulative_io"),
                sample_io.saturating_delta(t0_io),
                logical_deleted_payload_bytes,
            );
        } else {
            println!("{prefix}_cumulative_io_bytes=unavailable");
        }
    }

    print_timing_summary(
        "phase_post_delete_put",
        &reclaim_timeline.workload.put_timings,
    );
    print_timing_summary(
        "phase_post_delete_get",
        &reclaim_timeline.workload.get_timings,
    );
    print_timing_summary(
        "phase_post_delete_delete",
        &reclaim_timeline.workload.delete_timings,
    );
    print_timing_summary(
        "phase_post_delete_sync",
        &reclaim_timeline.workload.sync_timings,
    );

    if let (Some(empty), Some(loaded), Some(baseline), Some(post_delete), Some(post_reclaim)) = (
        empty_io,
        loaded_io,
        baseline_io,
        post_delete_io,
        post_reclaim_io,
    ) {
        let load = loaded.saturating_delta(empty);
        let setup = baseline.saturating_delta(loaded);
        let foreground = post_delete.saturating_delta(baseline);
        let reclaim = post_reclaim.saturating_delta(post_delete);
        let lifetime = post_reclaim.saturating_delta(empty);
        print_process_io_delta("delete_load_io", load, logical_loaded_payload_bytes);
        print_process_io_delta("delete_setup_io", setup, logical_loaded_payload_bytes);
        print_process_io_delta(
            "delete_foreground_io",
            foreground,
            logical_deleted_payload_bytes,
        );
        print_process_io_delta("delete_reclaim_io", reclaim, logical_deleted_payload_bytes);
        print_process_io_delta("delete_lifetime_io", lifetime, logical_loaded_payload_bytes);
    } else {
        println!("delete_process_io_bytes=unavailable");
    }
}

fn print_process_io_delta(prefix: &str, delta: ProcessIoSnapshot, logical_bytes: u128) {
    let total_bytes = delta.read_bytes.saturating_add(delta.write_bytes);
    println!("{prefix}_read_bytes={}", delta.read_bytes);
    println!("{prefix}_write_bytes={}", delta.write_bytes);
    println!("{prefix}_total_bytes={total_bytes}");
    print_ratio(
        &format!("{prefix}_read_amplification"),
        delta.read_bytes,
        logical_bytes,
    );
    print_ratio(
        &format!("{prefix}_write_amplification"),
        delta.write_bytes,
        logical_bytes,
    );
    print_ratio(
        &format!("{prefix}_total_amplification"),
        total_bytes,
        logical_bytes,
    );
}

fn print_report(inputs: ReportInputs<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let ReportInputs {
        config,
        elapsed,
        timings,
        profile,
        phases,
        store,
        rocksdb,
        profile_capture,
    } = inputs;

    let mut sorted = timings.to_vec();
    sorted.sort_unstable();

    let ops = timings.len() as f64;
    let elapsed_secs = elapsed.as_secs_f64();
    let measured_payload_bytes = measured_logical_payload_bytes(config, timings.len());
    let database_payload_bytes = database_logical_payload_bytes(config, timings.len());
    let payload_mib = measured_payload_bytes as f64 / 1_048_576.0;
    let root_summary = summarize_path_if_exists(&config.root_dir)?;

    println!("case={}", config.case.as_str());
    println!("root={}", config.root_dir.display());
    println!("namespace={}", config.namespace);
    println!("ops={}", timings.len());
    println!("payload_bytes={}", config.payload_size);
    println!("read_set_size={}", config.read_set_size);
    println!("read_pattern={}", config.read_pattern.as_str());
    println!("read_seed={}", config.read_seed);
    println!("reuse_existing={}", config.reuse_existing);
    println!("store_get_mode={}", config.store_get_mode.as_str());
    println!("store_get_profile={}", config.store_get_profile);
    println!(
        "store_get_verify_checksum={}",
        config.store_get_verify_checksum
    );
    println!("measured_logical_payload_bytes={measured_payload_bytes}");
    println!("database_logical_payload_bytes={database_payload_bytes}");
    println!("elapsed_ms={:.3}", elapsed_secs * 1000.0);
    println!("ops_per_sec={:.3}", ops / elapsed_secs);
    println!("payload_mib_per_sec={:.3}", payload_mib / elapsed_secs);
    println!("avg_us_per_op={:.3}", elapsed_secs * 1_000_000.0 / ops);
    println!("p50_us={:.3}", percentile_us(&sorted, 50.0));
    println!("p95_us={:.3}", percentile_us(&sorted, 95.0));
    println!("p99_us={:.3}", percentile_us(&sorted, 99.0));
    println!("p999_us={:.3}", percentile_us(&sorted, 99.9));
    println!("max_us={:.3}", sorted.last().map_or(0.0, duration_us));
    println!("sync_every={}", config.sync_every);
    println!("queue_capacity={}", config.queue_capacity);
    println!("max_unsealed_segments={}", config.max_unsealed_segments);
    println!("segment_max_bytes={}", config.segment_max_bytes);
    println!("seal_workers={}", config.seal_worker_count);
    println!(
        "sealed_integrity={}",
        sealed_integrity_as_str(config.sealed_segment_integrity_policy)
    );
    println!("reader_cache_capacity={}", config.reader_cache_capacity);
    println!(
        "strata_accounting_materialize_lag_threshold={}",
        config.strata_accounting_materialize_lag_threshold
    );
    println!(
        "strata_gc_io_bytes_per_sec={}",
        config.strata_gc_io_bytes_per_sec
    );
    println!(
        "strata_gc_min_io_bytes_per_sec={}",
        config.strata_gc_min_io_bytes_per_sec
    );
    println!(
        "strata_gc_min_reclaim_bytes={}",
        config.strata_gc_min_reclaim_bytes
    );
    println!(
        "strata_gc_min_garbage_percent={:.2}",
        config.strata_gc_min_garbage_ratio_bps as f64 / 100.0
    );
    println!("rocksdb_min_blob_size={}", config.rocksdb_min_blob_size);
    println!("rocksdb_blob_file_size={}", config.rocksdb_blob_file_size);
    println!(
        "rocksdb_write_buffer_size={}",
        config.rocksdb_write_buffer_size
    );
    println!(
        "rocksdb_high_pri_background_threads={}",
        config.rocksdb_high_pri_background_threads
    );
    println!("rocksdb_blob_gc={}", config.rocksdb_blob_gc);
    println!(
        "rocksdb_blob_gc_age_cutoff={:.6}",
        config.rocksdb_blob_gc_age_cutoff
    );
    println!(
        "rocksdb_blob_gc_force_threshold={:.6}",
        config.rocksdb_blob_gc_force_threshold
    );
    println!("rocksdb_get_profile={}", config.rocksdb_get_profile);
    println!("rocksdb_disable_wal={}", config.rocksdb_disable_wal);
    println!(
        "rocksdb_disable_auto_compactions={}",
        config.rocksdb_disable_auto_compactions
    );
    println!("final_directory_bytes={}", root_summary.bytes);
    print_ratio(
        "final_bytes_per_database_payload_byte",
        root_summary.bytes,
        database_payload_bytes,
    );
    print_path_summary("filesystem_root", &root_summary);
    print_layout_metrics(config)?;

    if let Some(profile) = profile {
        print_store_get_profile(profile);
    }
    if let Some(phases) = phases {
        print_timing_summary(&format!("phase_{}", phases.primary_name), &phases.primary);
        print_timing_summary("phase_sync", &phases.sync);
    }
    if let Some(store) = store {
        print_strata_store_metrics(store);
    }
    if let Some(db) = rocksdb {
        print_rocksdb_metrics(db);
    }
    if let Some(profile_capture) = profile_capture {
        print_profile_capture(profile_capture);
    }

    Ok(())
}

fn measured_logical_payload_bytes(config: &Config, measured_ops: usize) -> u128 {
    let payload_ops = match config.case {
        BenchCase::SegmentAppend
        | BenchCase::StorePut
        | BenchCase::StorePutArc
        | BenchCase::StoreDelete
        | BenchCase::RocksDbBlobDbPut
        | BenchCase::RocksDbBlobDbDelete
        | BenchCase::RocksDbBlobDbGet
        | BenchCase::RocksDbBlobDbGetPinned => measured_ops,
        BenchCase::StoreGet => match config.store_get_mode {
            StoreGetMode::Payload => measured_ops,
            StoreGetMode::KeyOnly => 0,
        },
    };
    payload_bytes(config, payload_ops)
}

fn database_logical_payload_bytes(config: &Config, measured_ops: usize) -> u128 {
    let stored_payload_ops = match config.case {
        BenchCase::SegmentAppend
        | BenchCase::StorePut
        | BenchCase::StorePutArc
        | BenchCase::RocksDbBlobDbPut => measured_ops,
        BenchCase::StoreDelete | BenchCase::RocksDbBlobDbDelete => config.ops,
        BenchCase::StoreGet | BenchCase::RocksDbBlobDbGet | BenchCase::RocksDbBlobDbGetPinned => {
            config.effective_read_set_size()
        }
    };
    payload_bytes(config, stored_payload_ops)
}

fn payload_bytes(config: &Config, ops: usize) -> u128 {
    config.payload_size as u128 * ops as u128
}

fn print_ratio(name: &str, numerator: u64, denominator: u128) {
    if denominator == 0 {
        println!("{name}=0.000000");
        return;
    }
    println!("{name}={:.6}", numerator as f64 / denominator as f64);
}

fn print_layout_metrics(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let store_config = config.store_config();
    print_path_summary(
        "strata_namespace",
        &summarize_path_if_exists(&store_config.namespace_dir())?,
    );
    print_path_summary(
        "strata_ingest",
        &summarize_path_if_exists(&store_config.ingest_dir())?,
    );
    print_path_summary(
        "strata_index",
        &summarize_path_if_exists(&store_config.standalone_index_dir())?,
    );
    let accounting_summary = summarize_path_if_exists(&store_config.accounting_index_dir())?;
    print_path_summary("strata_accounting_index", &accounting_summary);
    println!(
        "strata_accounting_active_delta_log_bytes={}",
        file_len_if_exists(&store_config.accounting_index_dir().join("active-delta.log"))?
    );
    print_path_summary(
        "rocksdb_blobdb_dir",
        &summarize_path_if_exists(&config.root_dir.join("rocksdb-blobdb"))?,
    );
    Ok(())
}

fn print_path_summary(prefix: &str, summary: &PathSummary) {
    println!("{prefix}_bytes={}", summary.bytes);
    println!("{prefix}_allocated_bytes={}", summary.allocated_bytes);
    println!("{prefix}_file_count={}", summary.file_count);
    println!("{prefix}_directory_count={}", summary.directory_count);
    println!("{prefix}_data_file_count={}", summary.data_file_count);
    println!("{prefix}_data_file_bytes={}", summary.data_file_bytes);
    println!("{prefix}_sst_file_count={}", summary.sst_file_count);
    println!("{prefix}_sst_file_bytes={}", summary.sst_file_bytes);
    println!("{prefix}_blob_file_count={}", summary.blob_file_count);
    println!("{prefix}_blob_file_bytes={}", summary.blob_file_bytes);
    println!("{prefix}_log_file_count={}", summary.log_file_count);
    println!("{prefix}_log_file_bytes={}", summary.log_file_bytes);
    println!(
        "{prefix}_manifest_file_count={}",
        summary.manifest_file_count
    );
}

fn summarize_path_if_exists(path: &Path) -> std::io::Result<PathSummary> {
    if !path.exists() {
        return Ok(PathSummary::default());
    }
    summarize_path(path)
}

fn summarize_path(path: &Path) -> std::io::Result<PathSummary> {
    let metadata = fs::symlink_metadata(path)?;
    let mut summary = PathSummary::default();
    if metadata.is_file() {
        add_file_to_summary(
            path,
            metadata.len(),
            allocated_file_bytes(&metadata),
            &mut summary,
        );
        return Ok(summary);
    }
    if !metadata.is_dir() {
        return Ok(summary);
    }

    summary.directory_count += 1;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child_summary = summarize_path(&entry.path())?;
        summary.bytes = summary.bytes.saturating_add(child_summary.bytes);
        summary.allocated_bytes = summary
            .allocated_bytes
            .saturating_add(child_summary.allocated_bytes);
        summary.file_count = summary.file_count.saturating_add(child_summary.file_count);
        summary.directory_count = summary
            .directory_count
            .saturating_add(child_summary.directory_count);
        summary.data_file_count = summary
            .data_file_count
            .saturating_add(child_summary.data_file_count);
        summary.data_file_bytes = summary
            .data_file_bytes
            .saturating_add(child_summary.data_file_bytes);
        summary.sst_file_count = summary
            .sst_file_count
            .saturating_add(child_summary.sst_file_count);
        summary.sst_file_bytes = summary
            .sst_file_bytes
            .saturating_add(child_summary.sst_file_bytes);
        summary.blob_file_count = summary
            .blob_file_count
            .saturating_add(child_summary.blob_file_count);
        summary.blob_file_bytes = summary
            .blob_file_bytes
            .saturating_add(child_summary.blob_file_bytes);
        summary.log_file_count = summary
            .log_file_count
            .saturating_add(child_summary.log_file_count);
        summary.log_file_bytes = summary
            .log_file_bytes
            .saturating_add(child_summary.log_file_bytes);
        summary.manifest_file_count = summary
            .manifest_file_count
            .saturating_add(child_summary.manifest_file_count);
    }
    Ok(summary)
}

fn add_file_to_summary(path: &Path, len: u64, allocated_bytes: u64, summary: &mut PathSummary) {
    summary.bytes = summary.bytes.saturating_add(len);
    summary.allocated_bytes = summary.allocated_bytes.saturating_add(allocated_bytes);
    summary.file_count += 1;
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("data") => {
            summary.data_file_count += 1;
            summary.data_file_bytes = summary.data_file_bytes.saturating_add(len);
        }
        Some("sst") => {
            summary.sst_file_count += 1;
            summary.sst_file_bytes = summary.sst_file_bytes.saturating_add(len);
        }
        Some("blob") => {
            summary.blob_file_count += 1;
            summary.blob_file_bytes = summary.blob_file_bytes.saturating_add(len);
        }
        Some("log") => {
            summary.log_file_count += 1;
            summary.log_file_bytes = summary.log_file_bytes.saturating_add(len);
        }
        _ => {}
    }
    if path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .is_some_and(|file_name| file_name.starts_with("MANIFEST-"))
    {
        summary.manifest_file_count += 1;
    }
}

#[cfg(unix)]
fn allocated_file_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_file_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

fn file_len_if_exists(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(fs::metadata(path)?.len())
}

#[derive(Debug, Clone, Copy)]
enum RocksDbPerfMetricKind {
    Count,
    Bytes,
    Nanos,
}

const ROCKSDB_GET_PERF_METRICS: &[(&str, &str, RocksDbPerfMetricKind)] = &[
    (
        "user_key_comparison_count",
        "user_key_comparison_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "block_cache_hit_count",
        "block_cache_hit_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "block_read_count",
        "block_read_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "block_read_byte",
        "block_read_bytes",
        RocksDbPerfMetricKind::Bytes,
    ),
    (
        "block_read_time",
        "block_read",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "block_checksum_time",
        "block_checksum",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "block_decompress_time",
        "block_decompress",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "get_read_bytes",
        "get_read_bytes",
        RocksDbPerfMetricKind::Bytes,
    ),
    (
        "blob_cache_hit_count",
        "blob_cache_hit_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "blob_read_count",
        "blob_read_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "blob_read_byte",
        "blob_read_bytes",
        RocksDbPerfMetricKind::Bytes,
    ),
    ("blob_read_time", "blob_read", RocksDbPerfMetricKind::Nanos),
    (
        "blob_checksum_time",
        "blob_checksum",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "blob_decompress_time",
        "blob_decompress",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "get_from_memtable_count",
        "get_from_memtable_count",
        RocksDbPerfMetricKind::Count,
    ),
    (
        "get_from_memtable_time",
        "get_from_memtable",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "get_post_process_time",
        "get_post_process",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "get_from_output_files_time",
        "get_from_output_files",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "read_index_block_nanos",
        "read_index_block",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "read_filter_block_nanos",
        "read_filter_block",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "block_seek_nanos",
        "block_seek",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "find_table_nanos",
        "find_table",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "env_new_random_access_file_nanos",
        "env_new_random_access_file",
        RocksDbPerfMetricKind::Nanos,
    ),
    (
        "db_mutex_lock_nanos",
        "db_mutex_lock",
        RocksDbPerfMetricKind::Nanos,
    ),
];

fn parse_rocksdb_perf_report(report: &str) -> HashMap<&str, u64> {
    report
        .split(',')
        .filter_map(|entry| {
            let (name, value) = entry.trim().split_once(" = ")?;
            Some((name, value.parse().ok()?))
        })
        .collect()
}

fn print_rocksdb_get_profile(report: &str, ops: usize) {
    let counters = parse_rocksdb_perf_report(report);
    println!("rocksdb_perf_profile_ops={ops}");
    for (counter_name, output_name, kind) in ROCKSDB_GET_PERF_METRICS {
        let value = counters.get(counter_name).copied().unwrap_or_default();
        match kind {
            RocksDbPerfMetricKind::Count | RocksDbPerfMetricKind::Bytes => {
                println!("rocksdb_perf_{output_name}={value}");
            }
            RocksDbPerfMetricKind::Nanos => {
                let total_us = value as f64 / 1_000.0;
                let avg_us = if ops == 0 { 0.0 } else { total_us / ops as f64 };
                println!("rocksdb_perf_{output_name}_total_us={total_us:.3}");
                println!("rocksdb_perf_{output_name}_avg_us_per_op={avg_us:.3}");
            }
        }
    }
}

fn print_timing_summary(prefix: &str, timings: &[Duration]) {
    let mut sorted = timings.to_vec();
    sorted.sort_unstable();
    let count = sorted.len();
    let total_us = timings.iter().map(duration_us).sum::<f64>();
    let avg_us = if count == 0 {
        0.0
    } else {
        total_us / count as f64
    };
    println!("{prefix}_count={count}");
    println!("{prefix}_total_us={total_us:.3}");
    println!("{prefix}_avg_us={avg_us:.3}");
    println!("{prefix}_p50_us={:.3}", percentile_us(&sorted, 50.0));
    println!("{prefix}_p95_us={:.3}", percentile_us(&sorted, 95.0));
    println!("{prefix}_p99_us={:.3}", percentile_us(&sorted, 99.0));
    println!("{prefix}_p999_us={:.3}", percentile_us(&sorted, 99.9));
    println!(
        "{prefix}_max_us={:.3}",
        sorted.last().map_or(0.0, duration_us)
    );
}

fn print_strata_store_metrics(store: &StrataStore) {
    let store_config = store.config();
    println!(
        "strata_accounting_enabled={}",
        store_config.accounting_worker_enabled
    );
    println!("strata_accounting_delta_log_enabled=true");
    println!(
        "strata_accounting_materialize_lag_threshold={}",
        store_config.accounting_materialize_lag_threshold
    );
    println!("strata_gc_enabled={}", store_config.gc_workers_enabled);
    println!(
        "strata_accounting_interval_ms={:.3}",
        store_config.accounting_interval.as_secs_f64() * 1000.0
    );
    println!(
        "strata_gc_interval_ms={:.3}",
        store_config.gc_interval.as_secs_f64() * 1000.0
    );
    println!(
        "strata_gc_worker_count={}",
        if store_config.gc_workers_enabled {
            store_config.gc_worker_count
        } else {
            0
        }
    );
    println!(
        "strata_gc_configured_worker_count={}",
        store_config.gc_worker_count
    );
    println!(
        "strata_gc_active_worker_limit={}",
        store.gc_active_worker_limit()
    );
    println!(
        "strata_gc_configured_io_bytes_per_sec={}",
        store_config.gc_io_bytes_per_sec
    );
    println!(
        "strata_gc_active_io_bytes_per_sec={}",
        store.gc_active_io_bytes_per_sec()
    );

    match (store.durable_lsn(), store.accounted_lsn()) {
        (Ok(durable_lsn), Ok(accounted_lsn)) => {
            println!("strata_durable_lsn={durable_lsn}");
            println!("strata_accounted_lsn={accounted_lsn}");
            println!(
                "strata_accounting_lag_lsn={}",
                durable_lsn.saturating_sub(accounted_lsn)
            );
        }
        (durable, accounted) => {
            println!(
                "strata_lsn_metrics_error={}",
                sanitize_property_value(&format!("{durable:?}/{accounted:?}"))
            );
        }
    }

    match store.index().iter_segment_states() {
        Ok(segment_states) => print_strata_segment_state_metrics(&segment_states),
        Err(error) => println!(
            "strata_segment_state_metrics_error={}",
            sanitize_property_value(&error.to_string())
        ),
    }
}

fn print_strata_segment_state_metrics(
    segment_states: &[(strata_core::SegmentId, strata_core::SegmentState)],
) {
    let mut open_count = 0_u64;
    let mut sealing_count = 0_u64;
    let mut sealed_count = 0_u64;
    let mut deleted_count = 0_u64;
    let mut pending_gc_output_count = 0_u64;
    let mut gc_relocating_count = 0_u64;
    let mut write_offset_bytes = 0_u64;
    let mut durable_offset_bytes = 0_u64;
    let mut sealed_len_bytes = 0_u64;
    let mut active_segment_write_offset = 0_u64;

    for (_, state) in segment_states {
        write_offset_bytes = write_offset_bytes.saturating_add(state.write_offset);
        durable_offset_bytes = durable_offset_bytes.saturating_add(state.durable_offset);
        sealed_len_bytes = sealed_len_bytes.saturating_add(state.sealed_len.unwrap_or(0));
        match state.state {
            SegmentFileState::Open => {
                open_count += 1;
                active_segment_write_offset = active_segment_write_offset.max(state.write_offset);
            }
            SegmentFileState::Sealing => sealing_count += 1,
            SegmentFileState::Sealed => sealed_count += 1,
            SegmentFileState::Deleted => deleted_count += 1,
            SegmentFileState::PendingGcOutput => pending_gc_output_count += 1,
            SegmentFileState::GcRelocating => gc_relocating_count += 1,
        }
    }

    println!("strata_segment_state_count={}", segment_states.len());
    println!(
        "strata_segment_rollover_count_estimate={}",
        segment_states.len().saturating_sub(1)
    );
    println!("strata_segment_open_count={open_count}");
    println!("strata_segment_sealing_count={sealing_count}");
    println!("strata_segment_sealed_count={sealed_count}");
    println!("strata_segment_deleted_count={deleted_count}");
    println!("strata_segment_pending_gc_output_count={pending_gc_output_count}");
    println!("strata_segment_gc_relocating_count={gc_relocating_count}");
    println!("strata_segment_write_offset_bytes={write_offset_bytes}");
    println!("strata_segment_durable_offset_bytes={durable_offset_bytes}");
    println!("strata_segment_sealed_len_bytes={sealed_len_bytes}");
    println!("strata_active_segment_write_offset_bytes={active_segment_write_offset}");
}

fn print_rocksdb_metrics(db: &RocksDB) {
    let Some(db) = standard_rocksdb(db) else {
        println!("rocksdb_metrics_error=unsupported_engine");
        return;
    };

    match db.live_files() {
        Ok(live_files) => {
            let live_sst_file_bytes = live_files.iter().map(|file| file.size as u64).sum::<u64>();
            let live_sst_l0_file_count = live_files.iter().filter(|file| file.level == 0).count();
            println!("rocksdb_live_sst_file_count={}", live_files.len());
            println!("rocksdb_live_sst_file_bytes={live_sst_file_bytes}");
            println!("rocksdb_live_sst_l0_file_count={live_sst_l0_file_count}");
        }
        Err(error) => println!(
            "rocksdb_live_files_error={}",
            sanitize_property_value(error.as_ref())
        ),
    }

    for (label, property_name) in ROCKSDB_INT_PROPERTIES {
        print_rocksdb_int_property(db, label, property_name);
    }
    for (label, property_name) in ROCKSDB_STRING_PROPERTIES {
        print_rocksdb_string_property(db, label, property_name);
    }
}

fn print_rocksdb_int_property(db: &DB, label: &str, property_name: &str) {
    match db.property_int_value(property_name) {
        Ok(Some(value)) => println!("{label}={value}"),
        Ok(None) => println!("{label}=unavailable"),
        Err(error) => println!("{label}=error:{}", sanitize_property_value(error.as_ref())),
    }
}

fn print_rocksdb_string_property(db: &DB, label: &str, property_name: &str) {
    match db.property_value(property_name) {
        Ok(Some(value)) => println!("{label}={}", sanitize_property_value(&value)),
        Ok(None) => println!("{label}=unavailable"),
        Err(error) => println!("{label}=error:{}", sanitize_property_value(error.as_ref())),
    }
}

fn standard_rocksdb(db: &RocksDB) -> Option<&DB> {
    match db {
        RocksDB::DB(handle) => Some(&handle.underlying),
        RocksDB::OptimisticTransactionDB(_) => None,
    }
}

fn sanitize_property_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

const ROCKSDB_INT_PROPERTIES: &[(&str, &str)] = &[
    (
        "rocksdb_property_cur_size_active_mem_table",
        "rocksdb.cur-size-active-mem-table",
    ),
    (
        "rocksdb_property_cur_size_all_mem_tables",
        "rocksdb.cur-size-all-mem-tables",
    ),
    (
        "rocksdb_property_size_all_mem_tables",
        "rocksdb.size-all-mem-tables",
    ),
    (
        "rocksdb_property_num_entries_active_mem_table",
        "rocksdb.num-entries-active-mem-table",
    ),
    (
        "rocksdb_property_num_entries_imm_mem_tables",
        "rocksdb.num-entries-imm-mem-tables",
    ),
    (
        "rocksdb_property_num_immutable_mem_table",
        "rocksdb.num-immutable-mem-table",
    ),
    (
        "rocksdb_property_mem_table_flush_pending",
        "rocksdb.mem-table-flush-pending",
    ),
    (
        "rocksdb_property_num_running_flushes",
        "rocksdb.num-running-flushes",
    ),
    (
        "rocksdb_property_compaction_pending",
        "rocksdb.compaction-pending",
    ),
    (
        "rocksdb_property_num_running_compactions",
        "rocksdb.num-running-compactions",
    ),
    (
        "rocksdb_property_estimate_pending_compaction_bytes",
        "rocksdb.estimate-pending-compaction-bytes",
    ),
    (
        "rocksdb_property_num_files_at_level0",
        "rocksdb.num-files-at-level0",
    ),
    (
        "rocksdb_property_num_live_versions",
        "rocksdb.num-live-versions",
    ),
    (
        "rocksdb_property_estimate_num_keys",
        "rocksdb.estimate-num-keys",
    ),
    (
        "rocksdb_property_estimate_live_data_size",
        "rocksdb.estimate-live-data-size",
    ),
    (
        "rocksdb_property_estimate_table_readers_mem",
        "rocksdb.estimate-table-readers-mem",
    ),
    (
        "rocksdb_property_total_sst_files_size",
        "rocksdb.total-sst-files-size",
    ),
    (
        "rocksdb_property_live_sst_files_size",
        "rocksdb.live-sst-files-size",
    ),
    ("rocksdb_property_num_blob_files", "rocksdb.num-blob-files"),
    (
        "rocksdb_property_total_blob_file_size",
        "rocksdb.total-blob-file-size",
    ),
    (
        "rocksdb_property_live_blob_file_size",
        "rocksdb.live-blob-file-size",
    ),
    (
        "rocksdb_property_live_blob_file_garbage_size",
        "rocksdb.live-blob-file-garbage-size",
    ),
];

const ROCKSDB_STRING_PROPERTIES: &[(&str, &str)] = &[
    ("rocksdb_property_blob_stats", "rocksdb.blob-stats"),
    ("rocksdb_property_stats", "rocksdb.stats"),
];

fn print_store_get_profile(profile: &StoreGetProfileSummary) {
    println!("profile_ops={}", profile.count);
    print_profile_duration("profile_op", profile.op_total, profile.count);
    print_profile_duration(
        "profile_record_lookup",
        profile.record_lookup,
        profile.count,
    );
    print_profile_duration(
        "profile_reader_acquire",
        profile.reader_acquire,
        profile.count,
    );
    print_profile_duration("profile_fixed_header", profile.fixed_header, profile.count);
    print_profile_duration("profile_buffer_alloc", profile.buffer_alloc, profile.count);
    print_profile_duration("profile_record_body", profile.record_body, profile.count);
    print_profile_duration("profile_decode", profile.decode, profile.count);
    print_profile_duration("profile_key_validate", profile.key_validate, profile.count);
    let accounted = profile.accounted();
    print_profile_duration("profile_accounted", accounted, profile.count);
    let unaccounted = profile.op_total.checked_sub(accounted).unwrap_or_default();
    print_profile_duration("profile_unaccounted", unaccounted, profile.count);
}

#[cfg(feature = "internal-profiling")]
fn print_profile_capture(capture: &ProfileCapture) {
    let write = capture.write_summary();
    let sync = capture.sync_summary();
    print_store_write_profile(&write);
    print_store_sync_profile(&sync);
}

#[cfg(not(feature = "internal-profiling"))]
fn print_profile_capture(_: &ProfileCapture) {}

#[cfg(feature = "internal-profiling")]
fn print_store_write_profile(profile: &StoreWriteProfileSummary) {
    println!("write_profile_ops={}", profile.count);
    print_profile_duration(
        "write_profile_queue_send",
        profile.queue_send,
        profile.count,
    );
    print_profile_duration(
        "write_profile_queue_wait",
        profile.queue_wait,
        profile.count,
    );
    print_profile_duration(
        "write_profile_prepare_batch",
        profile.prepare_batch,
        profile.count,
    );
    print_profile_duration(
        "write_profile_segment_capacity",
        profile.segment_capacity,
        profile.count,
    );
    print_profile_duration(
        "write_profile_segment_append",
        profile.segment_append,
        profile.count,
    );
    print_profile_duration(
        "write_profile_accounting_delta_append",
        profile.accounting_delta_append,
        profile.count,
    );
    print_profile_duration(
        "write_profile_index_batch_commit",
        profile.index_batch_commit,
        profile.count,
    );
    print_profile_duration(
        "write_profile_rollover_post_commit",
        profile.rollover_post_commit,
        profile.count,
    );
    print_profile_duration(
        "write_profile_accounting_nudge",
        profile.accounting_nudge,
        profile.count,
    );
    print_profile_duration(
        "write_profile_response_send",
        profile.response_send,
        profile.count,
    );
    print_profile_duration("write_profile_writer", profile.writer_total, profile.count);
}

#[cfg(feature = "internal-profiling")]
fn print_store_sync_profile(profile: &StoreSyncProfileSummary) {
    println!("sync_profile_ops={}", profile.count);
    print_profile_duration("sync_profile_queue_send", profile.queue_send, profile.count);
    print_profile_duration("sync_profile_queue_wait", profile.queue_wait, profile.count);
    print_profile_duration(
        "sync_profile_segment_sync",
        profile.segment_sync,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_accounting_delta_sync",
        profile.accounting_delta_sync,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_durable_lsn_compute",
        profile.durable_lsn_compute,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_index_batch_commit",
        profile.index_batch_commit,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_state_update",
        profile.state_update,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_accounting_nudge",
        profile.accounting_nudge,
        profile.count,
    );
    print_profile_duration(
        "sync_profile_response_send",
        profile.response_send,
        profile.count,
    );
    print_profile_duration("sync_profile_writer", profile.writer_total, profile.count);
}

fn print_profile_duration(name: &str, duration: Duration, count: usize) {
    let total_us = duration_us(&duration);
    let avg_us = if count == 0 {
        0.0
    } else {
        total_us / count as f64
    };
    println!("{name}_total_us={total_us:.3}");
    println!("{name}_avg_us={avg_us:.3}");
}

fn percentile_us(sorted: &[Duration], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (percentile / 100.0) * (sorted.len().saturating_sub(1) as f64);
    duration_us(&sorted[rank.round() as usize])
}

fn duration_us(duration: &Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn payload(size: usize) -> Vec<u8> {
    let mut payload = vec![0; size];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    payload
}

fn bench_key(prefix: &[u8], op: usize) -> Result<BlobKey, strata_core::Error> {
    let mut key = Vec::with_capacity(prefix.len() + 20);
    key.extend_from_slice(prefix);
    key.extend_from_slice(op.to_string().as_bytes());
    Ok(BlobKey::new(key)?)
}

struct ReadKeySequence {
    pattern: ReadPattern,
    key_count: usize,
    next_sequential: usize,
    rng: SplitMix64,
}

impl ReadKeySequence {
    fn new(pattern: ReadPattern, key_count: usize, seed: u64) -> Self {
        assert!(
            key_count != 0,
            "read key sequence requires at least one key"
        );
        Self {
            pattern,
            key_count,
            next_sequential: 0,
            rng: SplitMix64::new(seed),
        }
    }
}

impl Iterator for ReadKeySequence {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let index = match self.pattern {
            ReadPattern::Sequential => {
                let index = self.next_sequential % self.key_count;
                self.next_sequential = self.next_sequential.wrapping_add(1);
                index
            }
            ReadPattern::Random => (self.rng.next_u64() as usize) % self.key_count,
        };
        Some(index)
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

fn next_value(
    args: &mut impl Iterator<Item = String>,
    argument: &'static str,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{argument} requires a value"))
}

fn parse_nonzero_usize(value: &str) -> Result<usize, String> {
    let parsed = parse_usize(value)?;
    if parsed == 0 {
        return Err(format!("expected non-zero value, got '{value}'"));
    }
    Ok(parsed)
}

fn parse_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid integer '{value}': {error}"))
}

fn parse_u64(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid integer '{value}': {error}"))
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean '{value}'")),
    }
}

fn parse_percent(value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid percentage '{value}': {error}"))?;
    if !(parsed.is_finite() && 0.0 < parsed && parsed <= 100.0) {
        return Err(format!(
            "percentage must be greater than 0 and at most 100, got '{value}'"
        ));
    }
    Ok(parsed)
}

fn percent_to_basis_points(percent: f64) -> u16 {
    (percent * 100.0).round() as u16
}

fn parse_percent_inclusive(value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid percentage '{value}': {error}"))?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(format!(
            "percentage must be between 0 and 100, got '{value}'"
        ));
    }
    Ok(parsed)
}

fn percent_to_basis_points_inclusive(percent: f64) -> u16 {
    (percent * 100.0).round() as u16
}

fn parse_fraction(value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid fraction '{value}': {error}"))?;
    if !parsed.is_finite() || !(0.0..=1.0).contains(&parsed) {
        return Err(format!("fraction must be between 0 and 1, got '{value}'"));
    }
    Ok(parsed)
}

fn parse_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    let split_at = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let number = value[..split_at]
        .parse::<usize>()
        .map_err(|error| format!("invalid size '{value}': {error}"))?;
    let suffix = value[split_at..].trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return Err(format!("unsupported size suffix in '{value}'")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size '{value}' overflows usize"))
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let split_at = value
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(value.len());
    let amount = value[..split_at]
        .parse::<f64>()
        .map_err(|error| format!("invalid duration '{value}': {error}"))?;
    if !amount.is_finite() || amount < 0.0 {
        return Err(format!(
            "duration must be finite and non-negative, got '{value}'"
        ));
    }
    let multiplier = match value[split_at..].trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" => 1.0,
        "ms" => 0.001,
        "m" | "min" | "mins" => 60.0,
        "h" | "hr" | "hrs" => 3600.0,
        suffix => {
            return Err(format!(
                "unsupported duration suffix '{suffix}' in '{value}'"
            ));
        }
    };
    Duration::try_from_secs_f64(amount * multiplier)
        .map_err(|error| format!("invalid duration '{value}': {error}"))
}

fn parse_duration_list(value: &str) -> Result<Vec<Duration>, String> {
    let mut durations = value
        .split(',')
        .map(str::trim)
        .map(|entry| {
            if entry.is_empty() {
                return Err("duration list contains an empty entry".to_owned());
            }
            parse_duration(entry)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if durations.is_empty() {
        return Err("duration list must not be empty".to_owned());
    }
    durations.sort_unstable();
    durations.dedup();
    Ok(durations)
}

fn default_reclaim_sample_at() -> Vec<Duration> {
    [0, 60, 300, 600, 900, 1800, 3600]
        .into_iter()
        .map(Duration::from_secs)
        .collect()
}

fn default_root_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir().join(format!("strata-bench-{}-{stamp}", process::id()))
}

fn usage() -> &'static str {
    "usage: cargo run -p strata-bench --release -- [options]

options:
  --case <segment-append|store-put|store-put-arc|store-get|store-delete|rocksdb-blobdb-put|rocksdb-blobdb-get|rocksdb-blobdb-get-pinned|rocksdb-blobdb-delete>
  --root <path>
  --namespace <name>
  --payload-size <bytes|KiB|MiB|GiB>
  --ops <count>                         delete cases: number of keys loaded before deletion
  --read-set-size <count>
  --read-pattern <sequential|random>
  --read-seed <u64>
  --reuse-existing                    get: reopen --root and read keys from prior put case
  --store-get-mode <payload|key-only>
  --store-get-profile
  --store-get-verify-checksum <true|false>
  --queue-capacity <count>
  --max-unsealed-segments <count>
  --segment-max-bytes <bytes|KiB|MiB|GiB>
  --seal-workers <count>
  --sealed-integrity <metadata-only|checksum>
  --reader-cache-capacity <count>       cached segment readers; 0 disables
  --starting-epoch <epoch>
  --strata-accounting <true|false>      background accounting worker
  --strata-accounting-materialize-lag-threshold <LSNs>
                                        full accounting pass at this durable/accounted gap; 0 disables
  --strata-gc <true|false>              background GC workers
  --strata-gc-io-bytes-per-sec <size>
  --strata-gc-min-io-bytes-per-sec <size>
  --strata-gc-min-reclaim-bytes <size>
  --strata-gc-min-garbage-percent <0..100>
  --rocksdb-min-blob-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-file-size <bytes|KiB|MiB|GiB>
  --rocksdb-write-buffer-size <bytes|KiB|MiB|GiB>
  --rocksdb-high-pri-background-threads <count>
  --rocksdb-blob-gc <true|false>
  --rocksdb-blob-gc-age-cutoff <0..1>
  --rocksdb-blob-gc-force-threshold <0..1>
  --rocksdb-get-profile               get: report native RocksDB read perf counters
  --rocksdb-disable-wal <true|false>
  --rocksdb-disable-auto-compactions <true|false>
  --sync-every <count>
  --delete-percent <0..100>             default 50
  --delete-pattern <sequential|random>  default random
  --delete-seed <u64>
  --delete-verify-samples <count>        deleted and live samples; 0 disables
  --delete-reclaim <none|background>     background observes engine-native workers over time
  --delete-setup-timeout <duration>      Strata seal/accounting wait; default 5m
  --reclaim-duration <duration>          background observation window; default 60m
  --reclaim-sample-at <times>            comma-separated checkpoints; default 0,1m,5m,10m,15m,30m,60m
  --post-delete-workload <idle|steady>   default idle
  --post-delete-ops-per-second <count>   aggregate steady target rate; default 10
  --post-delete-workers <count>          concurrent steady workers; default 1
  --post-delete-put-percent <0..100>     steady mix; default 40
  --post-delete-get-percent <0..100>     steady mix; default 20
  --post-delete-delete-percent <0..100>  steady mix; default 40; mix must sum to 100
  --post-delete-seed <u64>
  --metrics-listen <addr>              serve Prometheus metrics on /metrics
  --metrics-drain-seconds <seconds>    wait after benchmark when metrics are enabled; default 30
  --keep-data"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimized_blob_db_value_preserves_legacy_bcs_encoding() {
        let payload = (0..4096)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let legacy_bytes = bcs::to_bytes(&payload).expect("legacy Vec should serialize");
        let optimized = BlobDbValue(payload.clone());
        let optimized_bytes = bcs::to_bytes(&optimized).expect("optimized value should serialize");

        assert_eq!(optimized_bytes, legacy_bytes);
        assert_eq!(
            bcs::from_bytes::<BlobDbValue>(&legacy_bytes)
                .expect("optimized value should decode legacy bytes"),
            optimized
        );
        assert_eq!(
            bcs::from_bytes::<Vec<u8>>(&optimized_bytes)
                .expect("legacy Vec should decode optimized bytes"),
            payload
        );
    }

    #[test]
    fn rocksdb_perf_report_parser_reads_native_counter_format() {
        let counters = parse_rocksdb_perf_report(
            "blob_read_count = 7, blob_read_byte = 7340032, blob_read_time = 21000",
        );

        assert_eq!(counters.get("blob_read_count"), Some(&7));
        assert_eq!(counters.get("blob_read_byte"), Some(&7_340_032));
        assert_eq!(counters.get("blob_read_time"), Some(&21_000));
    }

    #[test]
    fn rocksdb_perf_snapshot_is_published_as_prometheus_metrics() {
        let registry = Registry::new();
        let metrics =
            RocksDbPerfPrometheusMetrics::new(&registry).expect("perf metrics should register");

        let mut initial = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut initial)
            .expect("initial perf metrics should encode");
        let initial = String::from_utf8(initial).expect("Prometheus output should be UTF-8");
        assert!(initial.contains("strata_bench_rocksdb_perf_profile_ops 0"));
        assert!(initial.contains("strata_bench_rocksdb_perf_count{counter=\"blob_read_count\"} 0"));
        assert!(
            initial.contains("strata_bench_rocksdb_perf_seconds_per_op{counter=\"blob_read\"} 0")
        );

        metrics.set(
            "blob_read_count = 7, blob_read_byte = 7340032, blob_read_time = 21000",
            3,
        );

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut encoded)
            .expect("perf metrics should encode");
        let encoded = String::from_utf8(encoded).expect("Prometheus output should be UTF-8");

        assert!(encoded.contains("strata_bench_rocksdb_perf_profile_ops 3"));
        assert!(encoded.contains("strata_bench_rocksdb_perf_count{counter=\"blob_read_count\"} 7"));
        assert!(
            encoded
                .contains("strata_bench_rocksdb_perf_bytes{counter=\"blob_read_bytes\"} 7340032")
        );
        assert!(
            encoded.contains("strata_bench_rocksdb_perf_seconds{counter=\"blob_read\"} 0.000021")
        );
        assert!(
            encoded.contains(
                "strata_bench_rocksdb_perf_seconds_per_op{counter=\"blob_read\"} 0.000007"
            )
        );
    }

    #[test]
    fn read_key_sequence_round_robins_for_sequential_pattern() {
        let indexes = ReadKeySequence::new(ReadPattern::Sequential, 3, 99)
            .take(8)
            .collect::<Vec<_>>();

        assert_eq!(indexes, vec![0, 1, 2, 0, 1, 2, 0, 1]);
    }

    #[test]
    fn read_key_sequence_is_deterministic_for_random_pattern() {
        let first = ReadKeySequence::new(ReadPattern::Random, 11, 42)
            .take(16)
            .collect::<Vec<_>>();
        let second = ReadKeySequence::new(ReadPattern::Random, 11, 42)
            .take(16)
            .collect::<Vec<_>>();

        assert_eq!(first, second);
        assert!(first.iter().all(|index| *index < 11));
        assert_ne!(first, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4]);
    }

    #[test]
    fn config_parses_read_pattern_and_seed() {
        let config = Config::parse(
            [
                "--case",
                "store-get",
                "--read-pattern",
                "random",
                "--read-seed",
                "123",
                "--store-get-mode",
                "key-only",
                "--store-get-profile",
                "--store-get-verify-checksum",
                "false",
                "--strata-accounting",
                "false",
                "--strata-accounting-materialize-lag-threshold",
                "12345",
                "--strata-gc",
                "false",
                "--rocksdb-disable-auto-compactions",
                "true",
                "--rocksdb-disable-wal",
                "true",
                "--rocksdb-write-buffer-size",
                "512MiB",
                "--rocksdb-high-pri-background-threads",
                "8",
                "--metrics-listen",
                "127.0.0.1:0",
                "--metrics-drain-seconds",
                "7",
                "--max-unsealed-segments",
                "12",
                "--seal-workers",
                "3",
                "--sealed-integrity",
                "checksum",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("config should parse");

        assert_eq!(config.read_pattern, ReadPattern::Random);
        assert_eq!(config.read_seed, 123);
        assert_eq!(config.store_get_mode, StoreGetMode::KeyOnly);
        assert!(config.store_get_profile);
        assert!(!config.store_get_verify_checksum);
        assert!(!config.strata_accounting);
        assert_eq!(config.strata_accounting_materialize_lag_threshold, 12345);
        assert!(!config.strata_gc);
        assert!(config.rocksdb_disable_auto_compactions);
        assert!(config.rocksdb_disable_wal);
        assert_eq!(config.rocksdb_write_buffer_size, 512 << 20);
        assert_eq!(config.rocksdb_high_pri_background_threads, 8);
        assert_eq!(config.metrics_listen.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(config.metrics_drain_seconds, 7);
        assert_eq!(config.max_unsealed_segments, 12);
        assert_eq!(config.seal_worker_count, 3);
        assert_eq!(config.reader_cache_capacity, 64_000);
        assert_eq!(
            config.sealed_segment_integrity_policy,
            SealedSegmentIntegrityPolicy::Checksum
        );
    }

    #[test]
    fn config_allows_get_to_reuse_full_existing_write_set() {
        let config = Config::parse(
            [
                "--case",
                "store-get",
                "--root",
                "/tmp/strata-bench-existing",
                "--ops",
                "3",
                "--read-set-size",
                "10",
                "--reuse-existing",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("existing get config should parse");

        assert!(config.reuse_existing);
        assert_eq!(config.effective_read_set_size(), 10);
    }

    #[test]
    fn config_allows_native_rocksdb_get_profiling() {
        for case in ["rocksdb-blobdb-get", "rocksdb-blobdb-get-pinned"] {
            let config = Config::parse(
                ["--case", case, "--rocksdb-get-profile"]
                    .into_iter()
                    .map(str::to_owned),
            )
            .expect("BlobDB get profiling should parse");

            assert!(config.rocksdb_get_profile);
        }
    }

    #[test]
    fn config_allows_pinned_get_to_reuse_existing_write_set() {
        let config = Config::parse(
            [
                "--case",
                "rocksdb-blobdb-get-pinned",
                "--root",
                "/tmp/strata-bench-existing",
                "--reuse-existing",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("pinned BlobDB get should reuse an existing write set");

        assert_eq!(config.case, BenchCase::RocksDbBlobDbGetPinned);
        assert!(config.reuse_existing);
    }

    #[test]
    fn config_rejects_native_rocksdb_profiling_for_other_cases() {
        let error = Config::parse(
            ["--case", "store-get", "--rocksdb-get-profile"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("native RocksDB profiling should require BlobDB get");

        assert_eq!(
            error,
            "--rocksdb-get-profile is only supported for rocksdb-blobdb-get and rocksdb-blobdb-get-pinned"
        );
    }

    #[test]
    fn config_requires_explicit_root_when_reusing_existing_data() {
        let error = Config::parse(
            ["--case", "store-get", "--reuse-existing"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("reuse without an explicit root should be rejected");

        assert_eq!(error, "--reuse-existing requires --root <path>");
    }

    #[test]
    fn config_rejects_reuse_for_put_cases() {
        let error = Config::parse(
            [
                "--case",
                "store-put",
                "--root",
                "/tmp/strata-bench-existing",
                "--reuse-existing",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect_err("put cases should not reuse an existing root");

        assert_eq!(
            error,
            "--reuse-existing is only supported for store-get, rocksdb-blobdb-get, and rocksdb-blobdb-get-pinned"
        );
    }

    #[test]
    fn config_rejects_gc_without_accounting() {
        let error = Config::parse(
            ["--strata-accounting", "false"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("GC without accounting should be rejected");

        assert_eq!(error, "--strata-gc true requires --strata-accounting true");
    }

    #[test]
    fn config_parses_delete_workload_controls() {
        let config = Config::parse(
            [
                "--case",
                "rocksdb-blobdb-delete",
                "--delete-percent",
                "62.5",
                "--delete-pattern",
                "sequential",
                "--delete-seed",
                "99",
                "--delete-verify-samples",
                "17",
                "--delete-reclaim",
                "background",
                "--delete-setup-timeout",
                "45s",
                "--reclaim-duration",
                "30m",
                "--reclaim-sample-at",
                "0,5m,15m,30m",
                "--post-delete-workload",
                "steady",
                "--post-delete-ops-per-second",
                "250",
                "--post-delete-workers",
                "4",
                "--post-delete-put-percent",
                "50",
                "--post-delete-get-percent",
                "25",
                "--post-delete-delete-percent",
                "25",
                "--post-delete-seed",
                "1234",
                "--rocksdb-blob-gc-age-cutoff",
                "0.75",
                "--rocksdb-blob-gc-force-threshold",
                "0.9",
                "--strata-gc-io-bytes-per-sec",
                "64MiB",
                "--strata-gc-min-io-bytes-per-sec",
                "8MiB",
                "--strata-gc-min-reclaim-bytes",
                "16MiB",
                "--strata-gc-min-garbage-percent",
                "55.5",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("delete config should parse");

        assert_eq!(config.case, BenchCase::RocksDbBlobDbDelete);
        assert_eq!(config.delete_percent, 62.5);
        assert_eq!(config.delete_pattern, DeletePattern::Sequential);
        assert_eq!(config.delete_seed, 99);
        assert_eq!(config.delete_verify_samples, 17);
        assert_eq!(config.delete_reclaim_mode, DeleteReclaimMode::Background);
        assert_eq!(config.delete_setup_timeout, Duration::from_secs(45));
        assert_eq!(config.reclaim_duration, Duration::from_secs(30 * 60));
        assert_eq!(
            config.reclaim_sample_at,
            vec![
                Duration::ZERO,
                Duration::from_secs(5 * 60),
                Duration::from_secs(15 * 60),
                Duration::from_secs(30 * 60),
            ]
        );
        assert_eq!(config.post_delete_workload, PostDeleteWorkload::Steady);
        assert_eq!(config.post_delete_ops_per_second, 250);
        assert_eq!(config.post_delete_workers, 4);
        assert_eq!(config.post_delete_put_percent, 50.0);
        assert_eq!(config.post_delete_get_percent, 25.0);
        assert_eq!(config.post_delete_delete_percent, 25.0);
        assert_eq!(config.post_delete_seed, 1234);
        assert_eq!(config.rocksdb_blob_gc_age_cutoff, 0.75);
        assert_eq!(config.rocksdb_blob_gc_force_threshold, 0.9);
        assert_eq!(config.strata_gc_io_bytes_per_sec, 64 << 20);
        assert_eq!(config.strata_gc_min_io_bytes_per_sec, 8 << 20);
        assert_eq!(config.strata_gc_min_reclaim_bytes, 16 << 20);
        assert_eq!(config.strata_gc_min_garbage_ratio_bps, 5_550);
    }

    #[test]
    fn post_delete_workers_run_concurrently_with_disjoint_generated_keys() {
        let config = Config::parse(
            [
                "--case",
                "rocksdb-blobdb-delete",
                "--delete-reclaim",
                "background",
                "--reclaim-duration",
                "250ms",
                "--reclaim-sample-at",
                "0,250ms",
                "--post-delete-workload",
                "steady",
                "--post-delete-ops-per-second",
                "400",
                "--post-delete-workers",
                "4",
                "--post-delete-put-percent",
                "100",
                "--post-delete-get-percent",
                "0",
                "--post-delete-delete-percent",
                "0",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("parallel steady workload config should parse");
        let worker_threads = Mutex::new(std::collections::HashSet::new());
        let generated_keys = Mutex::new(std::collections::HashSet::new());
        let syncs = AtomicU64::new(0);

        let timeline = run_reclaim_timeline(
            &config,
            &[],
            |action| {
                worker_threads
                    .lock()
                    .expect("worker thread set should not be poisoned")
                    .insert(thread::current().id());
                let SteadyAction::Put(key) = action else {
                    panic!("100% put mix should only generate puts");
                };
                assert!(
                    generated_keys
                        .lock()
                        .expect("generated key set should not be poisoned")
                        .insert(key),
                    "workers generated a duplicate key {key}"
                );
                Ok::<(), Box<dyn std::error::Error>>(())
            },
            || {
                syncs.fetch_add(1, Ordering::Relaxed);
                Ok::<(), Box<dyn std::error::Error>>(())
            },
            || Ok::<_, Box<dyn std::error::Error>>(None),
        )
        .expect("parallel steady workload should complete");

        assert_eq!(
            worker_threads
                .lock()
                .expect("worker thread set should not be poisoned")
                .len(),
            4
        );
        assert_eq!(
            timeline.workload.counts.puts,
            generated_keys
                .lock()
                .expect("generated key set should not be poisoned")
                .len() as u64
        );
        assert_eq!(timeline.workload.counts.gets, 0);
        assert_eq!(timeline.workload.counts.deletes, 0);
        assert_eq!(syncs.load(Ordering::Relaxed), 1);
        assert_eq!(
            timeline
                .samples
                .last()
                .expect("timeline should include the final sample")
                .workload_counts
                .operations(),
            timeline.workload.counts.operations()
        );
    }

    #[test]
    fn random_delete_selection_is_deterministic_unique_and_sized() {
        let first = delete_indexes(100, 12.5, DeletePattern::Random, 7);
        let second = delete_indexes(100, 12.5, DeletePattern::Random, 7);
        let unique = first
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();

        assert_eq!(first, second);
        assert_eq!(first.len(), 13);
        assert_eq!(unique.len(), first.len());
        assert!(first.iter().all(|index| *index < 100));
    }

    #[test]
    fn duration_parser_accepts_reclaim_checkpoint_units() {
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("1.5m").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(
            parse_duration_list("5m,0,1m,5m").unwrap(),
            vec![
                Duration::ZERO,
                Duration::from_secs(60),
                Duration::from_secs(300),
            ]
        );
    }

    #[test]
    fn shorter_reclaim_duration_trims_default_checkpoints_and_keeps_endpoint() {
        let config = Config::parse(
            [
                "--case",
                "store-delete",
                "--delete-reclaim",
                "background",
                "--reclaim-duration",
                "3m",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("short background duration should parse");

        assert_eq!(
            config.effective_reclaim_sample_at(),
            vec![
                Duration::ZERO,
                Duration::from_secs(60),
                Duration::from_secs(180),
            ]
        );
    }

    #[test]
    fn manual_delete_reclamation_is_no_longer_supported() {
        let error = Config::parse(
            ["--case", "store-delete", "--delete-reclaim", "manual"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("manual compaction mode should be removed");

        assert_eq!(error, "unknown delete reclaim mode 'manual'");
    }
}
