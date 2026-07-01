//! Small benchmark harness for comparing Strata paths against RocksDB blob files.
//!
//! It creates a fresh root directory,
//! runs one case, prints machine-readable key/value metrics, and removes default temporary data
//! unless `--keep-data` is set.
//!
//! Cases:
//!
//! ```text
//! segment-append      SegmentWriter append cost without index work
//! store-put           StrataStore put path, cloning each payload into Arc storage
//! store-put-arc       StrataStore put path with caller-provided Arc payload
//! store-get           StrataStore point reads from indexed segment records
//! rocksdb-blobdb-put  RocksDB BlobDB put baseline
//! rocksdb-blobdb-get  RocksDB BlobDB get baseline
//! ```
//!
//! The benchmark is not part of the storage protocol. It should keep using public crate APIs so
//! benchmark results reflect what callers can actually exercise.

use std::{
    env, fs, hint,
    path::PathBuf,
    process,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rocksdb::{DB, Options};
use strata_core::{BlobKey, Epoch, PlacementClass};
use strata_segment::SegmentWriter;
use strata_store::{
    DEFAULT_ACCOUNTING_INTERVAL, DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
    DEFAULT_GC_INTERVAL, DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN, DEFAULT_GC_WORKER_COUNT,
    GcPlannerConfig, ReadOptions, SealedSegmentIntegrityPolicy, StoreGetProfile,
    StrataRecoveryPolicy, StrataStore, StrataStoreConfig, StrataStoreMetrics,
};

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_PAYLOAD_SIZE: usize = 1 << 20;
const DEFAULT_OPS: usize = 1024;
const DEFAULT_READ_SET_SIZE: usize = 1024;
const DEFAULT_READ_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 1 << 40;
const DEFAULT_READER_CACHE_CAPACITY: usize = strata_store::DEFAULT_SEGMENT_READER_CACHE_CAPACITY;
const DEFAULT_STARTING_EPOCH: Epoch = 1;
const DEFAULT_ROCKSDB_MIN_BLOB_SIZE: u64 = 1;
const DEFAULT_ROCKSDB_BLOB_FILE_SIZE: u64 = 1 << 28;
const BENCH_SEGMENT_ID: u64 = 1;

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
    segment_max_bytes: u64,
    reader_cache_capacity: usize,
    starting_epoch: Epoch,
    rocksdb_min_blob_size: u64,
    rocksdb_blob_file_size: u64,
    rocksdb_blob_gc: bool,
    sync_every: usize,
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
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            reader_cache_capacity: DEFAULT_READER_CACHE_CAPACITY,
            starting_epoch: DEFAULT_STARTING_EPOCH,
            rocksdb_min_blob_size: DEFAULT_ROCKSDB_MIN_BLOB_SIZE,
            rocksdb_blob_file_size: DEFAULT_ROCKSDB_BLOB_FILE_SIZE,
            rocksdb_blob_gc: true,
            sync_every: 0,
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
                "--segment-max-bytes" => {
                    config.segment_max_bytes =
                        parse_size(&next_value(&mut args, "--segment-max-bytes")?)? as u64
                }
                "--reader-cache-capacity" => {
                    config.reader_cache_capacity =
                        parse_usize(&next_value(&mut args, "--reader-cache-capacity")?)?
                }
                "--starting-epoch" => {
                    config.starting_epoch = parse_u64(&next_value(&mut args, "--starting-epoch")?)?
                }
                "--rocksdb-min-blob-size" => {
                    config.rocksdb_min_blob_size =
                        parse_size(&next_value(&mut args, "--rocksdb-min-blob-size")?)? as u64
                }
                "--rocksdb-blob-file-size" => {
                    config.rocksdb_blob_file_size =
                        parse_size(&next_value(&mut args, "--rocksdb-blob-file-size")?)? as u64
                }
                "--rocksdb-blob-gc" => {
                    config.rocksdb_blob_gc =
                        parse_bool(&next_value(&mut args, "--rocksdb-blob-gc")?)?
                }
                "--sync-every" => {
                    config.sync_every = parse_usize(&next_value(&mut args, "--sync-every")?)?
                }
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

        Ok(config)
    }

    fn store_config(&self) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: self.root_dir.clone(),
            namespace: self.namespace.clone(),
            segment_max_bytes: self.segment_max_bytes,
            write_queue_capacity: self.queue_capacity,
            max_unsealed_segments: 8,
            segment_reader_cache_capacity: self.reader_cache_capacity,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
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
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_planner_config: GcPlannerConfig::default(),
            gc_max_accounting_lag_lsn: DEFAULT_GC_MAX_ACCOUNTING_LAG_LSN,
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

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    if config.root_dir.exists() {
        fs::remove_dir_all(&config.root_dir)?;
    }
    fs::create_dir_all(&config.root_dir)?;

    let result = match config.case {
        BenchCase::SegmentAppend => run_segment_append(&config),
        BenchCase::StorePut => run_store_put(&config),
        BenchCase::StorePutArc => run_store_put_arc(&config),
        BenchCase::StoreGet => run_store_get(&config),
        BenchCase::RocksDbBlobDbPut => run_rocksdb_blobdb_put(&config),
        BenchCase::RocksDbBlobDbGet => run_rocksdb_blobdb_get(&config),
    };

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
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(key_prefix, op)?;
        let op_started = Instant::now();
        let record_ref = writer.append(&key, 0, &payload)?.record_ref;
        hint::black_box(record_ref);
        record_sync(config.sync_every, op + 1, || writer.sync_data())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, None);
    Ok(())
}

fn run_store_put(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store_config = config.store_config();
    let store = StrataStore::open(store_config, StrataStoreMetrics::default())?;
    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let lsn = store.put(0, &key, &payload)?;
        hint::black_box(lsn);
        record_sync(config.sync_every, op + 1, || store.sync())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, None);
    Ok(())
}

fn run_store_put_arc(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Arc<[u8]> = Arc::from(payload(config.payload_size));
    let store_config = config.store_config();
    let store = StrataStore::open(store_config, StrataStoreMetrics::default())?;
    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let lsn = store.put_arc(0, key, payload.clone())?;
        hint::black_box(lsn);
        record_sync(config.sync_every, op + 1, || store.sync())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, None);
    Ok(())
}

fn run_store_get(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store_config = config.store_config();
    let store = StrataStore::open(store_config, StrataStoreMetrics::default())?;
    let read_set_size = config.read_set_size.min(config.ops.max(1));
    let keys = (0..read_set_size)
        .map(|op| bench_key(b"read-key-", op))
        .collect::<Result<Vec<_>, _>>()?;

    for key in &keys {
        store.put(0, key, &payload)?;
    }
    store.sync()?;

    let mut timings = Vec::with_capacity(config.ops);
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
        match config.store_get_mode {
            StoreGetMode::Payload => {
                if let Some(profile_summary) = &mut profile_summary {
                    let (value, profile) =
                        store.get_blob_profiled_with_options(key, read_options)?;
                    hint::black_box(value.as_deref());
                    let op_elapsed = op_started.elapsed();
                    profile_summary.add(op_elapsed, profile);
                    timings.push(op_elapsed);
                    continue;
                }

                let value = store.get_with_options(key, read_options)?;
                hint::black_box(value.as_deref());
            }
            StoreGetMode::KeyOnly => {
                let exists = store.contains(key)?;
                hint::black_box(exists);
            }
        }
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, profile_summary.as_ref());
    Ok(())
}

fn run_rocksdb_blobdb_put(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let db = open_rocksdb_blobdb(config)?;
    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"rocksdb-key-", op)?;
        let op_started = Instant::now();
        db.put(key.as_bytes(), &payload)?;
        record_sync(config.sync_every, op + 1, || db.flush_wal(true))?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, None);
    Ok(())
}

fn run_rocksdb_blobdb_get(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let db = open_rocksdb_blobdb(config)?;
    let read_set_size = config.read_set_size.min(config.ops.max(1));
    let keys = (0..read_set_size)
        .map(|op| bench_key(b"rocksdb-read-key-", op))
        .collect::<Result<Vec<_>, _>>()?;

    for key in &keys {
        db.put(key.as_bytes(), &payload)?;
    }
    db.flush_wal(true)?;

    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();
    let key_indexes = ReadKeySequence::new(config.read_pattern, keys.len(), config.read_seed);

    for key_index in key_indexes.take(config.ops) {
        let key = &keys[key_index];
        let op_started = Instant::now();
        let value = db.get(key.as_bytes())?;
        hint::black_box(value.as_deref());
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings, None);
    Ok(())
}

fn open_rocksdb_blobdb(config: &Config) -> Result<DB, rocksdb::Error> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_enable_blob_files(true);
    options.set_min_blob_size(config.rocksdb_min_blob_size);
    options.set_blob_file_size(config.rocksdb_blob_file_size);
    options.set_enable_blob_gc(config.rocksdb_blob_gc);
    DB::open(&options, config.root_dir.join("rocksdb-blobdb"))
}

fn record_sync<E>(
    sync_every: usize,
    completed_ops: usize,
    sync: impl FnOnce() -> Result<(), E>,
) -> Result<(), E> {
    if sync_every != 0 && completed_ops.is_multiple_of(sync_every) {
        sync()?;
    }
    Ok(())
}

fn print_report(
    config: &Config,
    elapsed: Duration,
    timings: &[Duration],
    profile: Option<&StoreGetProfileSummary>,
) {
    let mut sorted = timings.to_vec();
    sorted.sort_unstable();

    let ops = timings.len() as f64;
    let elapsed_secs = elapsed.as_secs_f64();
    let payload_mib = (config.payload_size as f64 * ops) / 1_048_576.0;

    println!("case={}", config.case.as_str());
    println!("root={}", config.root_dir.display());
    println!("namespace={}", config.namespace);
    println!("ops={}", timings.len());
    println!("payload_bytes={}", config.payload_size);
    println!("read_set_size={}", config.read_set_size);
    println!("read_pattern={}", config.read_pattern.as_str());
    println!("read_seed={}", config.read_seed);
    println!("store_get_mode={}", config.store_get_mode.as_str());
    println!("store_get_profile={}", config.store_get_profile);
    println!(
        "store_get_verify_checksum={}",
        config.store_get_verify_checksum
    );
    println!("elapsed_ms={:.3}", elapsed_secs * 1000.0);
    println!("ops_per_sec={:.3}", ops / elapsed_secs);
    println!("payload_mib_per_sec={:.3}", payload_mib / elapsed_secs);
    println!("avg_us_per_op={:.3}", elapsed_secs * 1_000_000.0 / ops);
    println!("p50_us={:.3}", percentile_us(&sorted, 50.0));
    println!("p95_us={:.3}", percentile_us(&sorted, 95.0));
    println!("p99_us={:.3}", percentile_us(&sorted, 99.0));
    println!("max_us={:.3}", sorted.last().map_or(0.0, duration_us));
    println!("sync_every={}", config.sync_every);
    println!("queue_capacity={}", config.queue_capacity);
    println!("segment_max_bytes={}", config.segment_max_bytes);
    println!("reader_cache_capacity={}", config.reader_cache_capacity);
    println!("rocksdb_min_blob_size={}", config.rocksdb_min_blob_size);
    println!("rocksdb_blob_file_size={}", config.rocksdb_blob_file_size);
    println!("rocksdb_blob_gc={}", config.rocksdb_blob_gc);

    if let Some(profile) = profile {
        print_store_get_profile(profile);
    }
}

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
  --store-get-mode <payload|key-only>
  --store-get-profile
  --store-get-verify-checksum <true|false>
  --queue-capacity <count>
  --segment-max-bytes <bytes|KiB|MiB|GiB>
  --reader-cache-capacity <count>       cached segment readers; 0 disables
  --starting-epoch <epoch>
  --rocksdb-min-blob-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-file-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-gc <true|false>
  --sync-every <count>
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
    }
}
