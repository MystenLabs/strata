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
use strata_core::{BlobKey, BlobLifecycle, PlacementClass};
use strata_segment::SegmentWriter;
use strata_store::{
    SealedSegmentIntegrityPolicy, StrataRecoveryPolicy, StrataStore, StrataStoreConfig,
};

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_PAYLOAD_SIZE: usize = 1 << 20;
const DEFAULT_OPS: usize = 1024;
const DEFAULT_READ_SET_SIZE: usize = 1024;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 1 << 40;
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

#[derive(Debug, Clone)]
struct Config {
    case: BenchCase,
    root_dir: PathBuf,
    namespace: String,
    payload_size: usize,
    ops: usize,
    read_set_size: usize,
    queue_capacity: usize,
    segment_max_bytes: u64,
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
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
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
                "--queue-capacity" => {
                    config.queue_capacity =
                        parse_nonzero_usize(&next_value(&mut args, "--queue-capacity")?)?
                }
                "--segment-max-bytes" => {
                    config.segment_max_bytes =
                        parse_size(&next_value(&mut args, "--segment-max-bytes")?)? as u64
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
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
        }
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
        let record_ref = writer
            .append(&key, BlobLifecycle::new(42), 0, &payload)?
            .record_ref;
        hint::black_box(record_ref);
        record_sync(config.sync_every, op + 1, || writer.sync_data())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings);
    Ok(())
}

fn run_store_put(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store = StrataStore::open_standalone(config.store_config())?;
    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let lsn = store.put(&key, BlobLifecycle::new(42), &payload)?;
        hint::black_box(lsn);
        record_sync(config.sync_every, op + 1, || store.sync())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings);
    Ok(())
}

fn run_store_put_arc(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload: Arc<[u8]> = Arc::from(payload(config.payload_size));
    let store = StrataStore::open_standalone(config.store_config())?;
    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = bench_key(b"store-key-", op)?;
        let op_started = Instant::now();
        let lsn = store.put_arc(key, BlobLifecycle::new(42), payload.clone())?;
        hint::black_box(lsn);
        record_sync(config.sync_every, op + 1, || store.sync())?;
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings);
    Ok(())
}

fn run_store_get(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let payload = payload(config.payload_size);
    let store = StrataStore::open_standalone(config.store_config())?;
    let read_set_size = config.read_set_size.min(config.ops.max(1));
    let keys = (0..read_set_size)
        .map(|op| bench_key(b"read-key-", op))
        .collect::<Result<Vec<_>, _>>()?;

    for key in &keys {
        store.put(key, BlobLifecycle::new(42), &payload)?;
    }
    store.sync()?;

    let mut timings = Vec::with_capacity(config.ops);
    let started = Instant::now();

    for op in 0..config.ops {
        let key = &keys[op % keys.len()];
        let op_started = Instant::now();
        let value = store.get(key)?;
        hint::black_box(value.as_deref());
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings);
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
    print_report(config, elapsed, &timings);
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

    for op in 0..config.ops {
        let key = &keys[op % keys.len()];
        let op_started = Instant::now();
        let value = db.get(key.as_bytes())?;
        hint::black_box(value.as_deref());
        timings.push(op_started.elapsed());
    }

    let elapsed = started.elapsed();
    print_report(config, elapsed, &timings);
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

fn print_report(config: &Config, elapsed: Duration, timings: &[Duration]) {
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
    println!("rocksdb_min_blob_size={}", config.rocksdb_min_blob_size);
    println!("rocksdb_blob_file_size={}", config.rocksdb_blob_file_size);
    println!("rocksdb_blob_gc={}", config.rocksdb_blob_gc);
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
  --queue-capacity <count>
  --segment-max-bytes <bytes|KiB|MiB|GiB>
  --rocksdb-min-blob-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-file-size <bytes|KiB|MiB|GiB>
  --rocksdb-blob-gc <true|false>
  --sync-every <count>
  --keep-data"
}
