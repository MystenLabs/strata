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
//! rocksdb-blobdb-put  RocksDB BlobDB put baseline through typed-store
//! rocksdb-blobdb-get  RocksDB BlobDB get baseline through typed-store
//! ```
//!
//! The benchmark is not part of the storage protocol. It should keep using public crate APIs so
//! benchmark results reflect what callers can actually exercise.

#[cfg(feature = "internal-profiling")]
use std::sync::Mutex;
use std::{
    env, fs, hint, io,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use prometheus::{Encoder, Registry, TextEncoder};
use rocksdb::{DB, Env, WriteOptions};
use strata_core::{BlobKey, Epoch, PlacementClass, SegmentFileState};
use strata_segment::SegmentWriter;
use strata_store::{
    DEFAULT_ACCOUNTING_INTERVAL, DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
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
const ROCKSDB_BLOBDB_CF_CLASS: &str = "rocksdb_blobdb";
const DEFAULT_METRICS_DRAIN_SECONDS: u64 = 30;
const BENCH_SEGMENT_ID: u64 = 1;
type BlobDbMap = DBMap<Vec<u8>, Vec<u8>>;

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
    RocksDbBlobDbPut,
    RocksDbBlobDbGet,
}

impl BenchCase {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "segment-append" => Ok(Self::SegmentAppend),
            "store-put" => Ok(Self::StorePut),
            "store-put-arc" => Ok(Self::StorePutArc),
            "store-get" => Ok(Self::StoreGet),
            "rocksdb-blobdb-put" => Ok(Self::RocksDbBlobDbPut),
            "rocksdb-blobdb-get" => Ok(Self::RocksDbBlobDbGet),
            _ => Err(format!("unknown case '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::SegmentAppend => "segment-append",
            Self::StorePut => "store-put",
            Self::StorePutArc => "store-put-arc",
            Self::StoreGet => "store-get",
            Self::RocksDbBlobDbPut => "rocksdb-blobdb-put",
            Self::RocksDbBlobDbGet => "rocksdb-blobdb-get",
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
    strata_gc: bool,
    rocksdb_min_blob_size: u64,
    rocksdb_blob_file_size: u64,
    rocksdb_write_buffer_size: usize,
    rocksdb_high_pri_background_threads: usize,
    rocksdb_blob_gc: bool,
    rocksdb_disable_wal: bool,
    rocksdb_disable_auto_compactions: bool,
    sync_every: usize,
    metrics_listen: Option<String>,
    metrics_drain_seconds: u64,
    reuse_existing: bool,
    keep_data: bool,
    root_was_defaulted: bool,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
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
            strata_gc: true,
            rocksdb_min_blob_size: DEFAULT_ROCKSDB_MIN_BLOB_SIZE,
            rocksdb_blob_file_size: DEFAULT_ROCKSDB_BLOB_FILE_SIZE,
            rocksdb_write_buffer_size: DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE,
            rocksdb_high_pri_background_threads: DEFAULT_ROCKSDB_HIGH_PRI_BACKGROUND_THREADS,
            rocksdb_blob_gc: true,
            rocksdb_disable_wal: false,
            rocksdb_disable_auto_compactions: false,
            sync_every: 0,
            metrics_listen: None,
            metrics_drain_seconds: DEFAULT_METRICS_DRAIN_SECONDS,
            reuse_existing: false,
            keep_data: false,
            root_was_defaulted: true,
        };

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
                "--strata-gc" => {
                    config.strata_gc = parse_bool(&next_value(&mut args, "--strata-gc")?)?
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
        if config.reuse_existing
            && !matches!(
                config.case,
                BenchCase::StoreGet | BenchCase::RocksDbBlobDbGet
            )
        {
            return Err(
                "--reuse-existing is only supported for store-get and rocksdb-blobdb-get"
                    .to_owned(),
            );
        }
        if config.reuse_existing && config.root_was_defaulted {
            return Err("--reuse-existing requires --root <path>".to_owned());
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
            accounting_sidecar_partition_count: DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT,
            accounting_sidecar_interval: DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
            accounting_sidecar_ingest_record_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD,
            accounting_sidecar_delta_run_count_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
            accounting_sidecar_delta_run_bytes_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
            accounting_sidecar_major_patch_count_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
            accounting_sidecar_major_patch_bytes_threshold:
                DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
            gc_workers_enabled: self.strata_gc,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config: GcPlannerConfig::default(),
            gc_max_accounting_lag_lsn: DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: self.starting_epoch,
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

#[derive(Debug, Default)]
struct PathSummary {
    bytes: u64,
    file_count: u64,
    directory_count: u64,
    data_file_count: u64,
    sst_file_count: u64,
    blob_file_count: u64,
    log_file_count: u64,
    manifest_file_count: u64,
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
        BenchCase::RocksDbBlobDbPut => run_rocksdb_blobdb_put(&config),
        BenchCase::RocksDbBlobDbGet => run_rocksdb_blobdb_get(&config),
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

fn run_rocksdb_blobdb_put(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
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

fn run_rocksdb_blobdb_get(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
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
    let mut phases = PhaseTimings::new("rocksdb_get", config.ops);
    let started = Instant::now();
    let key_indexes = ReadKeySequence::new(config.read_pattern, keys.len(), config.read_seed);

    for key_index in key_indexes.take(config.ops) {
        let key = &keys[key_index];
        let op_started = Instant::now();
        let phase_started = Instant::now();
        let value = db.get(key)?;
        verify_reused_key_found(config, value.is_some(), key)?;
        phases.primary.push(phase_started.elapsed());
        hint::black_box(value.as_deref());
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

fn insert_rocksdb_blobdb(
    db: &BlobDbMap,
    key: &Vec<u8>,
    value: &Vec<u8>,
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
    options.set_disable_auto_compactions(config.rocksdb_disable_auto_compactions);
    Ok(DBMap::open(
        config.root_dir.join("rocksdb-blobdb"),
        MetricConf::new(ROCKSDB_BLOBDB_CF_CLASS),
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
    _server: Option<MetricsServer>,
}

impl BenchMetrics {
    fn start(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let Some(listen_addr) = config.metrics_listen.as_deref() else {
            return Ok(Self {
                registry: None,
                _server: None,
            });
        };

        let registry = Arc::new(Registry::new());
        DBMetrics::init(&registry);
        let server = start_metrics_server(listen_addr, Arc::clone(&registry))?;
        Ok(Self {
            registry: Some(registry),
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
        | BenchCase::RocksDbBlobDbPut
        | BenchCase::RocksDbBlobDbGet => measured_ops,
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
        BenchCase::StoreGet | BenchCase::RocksDbBlobDbGet => config.effective_read_set_size(),
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
    println!("{prefix}_file_count={}", summary.file_count);
    println!("{prefix}_directory_count={}", summary.directory_count);
    println!("{prefix}_data_file_count={}", summary.data_file_count);
    println!("{prefix}_sst_file_count={}", summary.sst_file_count);
    println!("{prefix}_blob_file_count={}", summary.blob_file_count);
    println!("{prefix}_log_file_count={}", summary.log_file_count);
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
        add_file_to_summary(path, metadata.len(), &mut summary);
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
        summary.file_count = summary.file_count.saturating_add(child_summary.file_count);
        summary.directory_count = summary
            .directory_count
            .saturating_add(child_summary.directory_count);
        summary.data_file_count = summary
            .data_file_count
            .saturating_add(child_summary.data_file_count);
        summary.sst_file_count = summary
            .sst_file_count
            .saturating_add(child_summary.sst_file_count);
        summary.blob_file_count = summary
            .blob_file_count
            .saturating_add(child_summary.blob_file_count);
        summary.log_file_count = summary
            .log_file_count
            .saturating_add(child_summary.log_file_count);
        summary.manifest_file_count = summary
            .manifest_file_count
            .saturating_add(child_summary.manifest_file_count);
    }
    Ok(summary)
}

fn add_file_to_summary(path: &Path, len: u64, summary: &mut PathSummary) {
    summary.bytes = summary.bytes.saturating_add(len);
    summary.file_count += 1;
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("data") => summary.data_file_count += 1,
        Some("sst") => summary.sst_file_count += 1,
        Some("blob") => summary.blob_file_count += 1,
        Some("log") => summary.log_file_count += 1,
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

fn file_len_if_exists(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(fs::metadata(path)?.len())
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

const ROCKSDB_STRING_PROPERTIES: &[(&str, &str)] =
    &[("rocksdb_property_blob_stats", "rocksdb.blob-stats")];

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
  --case <segment-append|store-put|store-put-arc|store-get|rocksdb-blobdb-put|rocksdb-blobdb-get>
  --root <path>
  --namespace <name>
  --payload-size <bytes|KiB|MiB|GiB>
  --ops <count>
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
  --strata-gc <true|false>              background GC workers
  --rocksdb-min-blob-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-file-size <bytes|KiB|MiB|GiB>
  --rocksdb-write-buffer-size <bytes|KiB|MiB|GiB>
  --rocksdb-high-pri-background-threads <count>
  --rocksdb-blob-gc <true|false>
  --rocksdb-disable-wal <true|false>
  --rocksdb-disable-auto-compactions <true|false>
  --sync-every <count>
  --metrics-listen <addr>              serve Prometheus metrics on /metrics
  --metrics-drain-seconds <seconds>    wait after benchmark when metrics are enabled; default 30
  --keep-data"
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!config.strata_gc);
        assert!(config.rocksdb_disable_auto_compactions);
        assert!(config.rocksdb_disable_wal);
        assert_eq!(config.rocksdb_write_buffer_size, 512 << 20);
        assert_eq!(config.rocksdb_high_pri_background_threads, 8);
        assert_eq!(config.metrics_listen.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(config.metrics_drain_seconds, 7);
        assert_eq!(config.max_unsealed_segments, 12);
        assert_eq!(config.seal_worker_count, 3);
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
            "--reuse-existing is only supported for store-get and rocksdb-blobdb-get"
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
}
