//! Standalone lifecycle benchmark for comparing Strata with RocksDB BlobDB.
//!
//! Unlike the focused `bench` cases, this harness keeps writes, age-based deletes, reads,
//! reclamation, and storage pressure active at the same time. Read and delete service levels are
//! obligations. By default an AIMD controller greedily raises write concurrency while those
//! obligations hold; `--put-ops-per-second` instead holds offered put load at a fixed global rate.

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::{unix::ffi::OsStrExt, unix::fs::MetadataExt};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    env, fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use core_types::{BlobKey, Epoch, SegmentFileState, SegmentId};
use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use rocksdb::{DB, Env, statistics::Ticker};
use serde::{Deserialize, Serialize};
use serde_with::{Bytes, serde_as};
use store::{
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
    DEFAULT_GC_SYNC_IMPACT_THRESHOLD, DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT,
    DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT, GcPlanner, GcPlannerConfig,
    SealedSegmentIntegrityPolicy, StrataRecoveryPolicy, StrataStore, StrataStoreConfig,
    StrataStoreMetrics,
};
use typed_store::{
    DBMetrics, Map,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, default_db_options},
};

const DEFAULT_NAMESPACE: &str = "realistic";
const DEFAULT_DURATION: Duration = Duration::from_secs(30 * 60);
const DEFAULT_RETENTION: Duration = Duration::from_secs(5 * 60);
const DEFAULT_EPOCH_DURATION: Duration = Duration::from_secs(5 * 60);
const DEFAULT_FUTURE_EPOCHS: Epoch = 52;
const DEFAULT_LIFETIME_SEED: u64 = 0x5eed_1eaf_cafe_f00d;
const DEFAULT_CLEANUP_GRACE: Duration = Duration::from_secs(5 * 60);
const DEFAULT_PAYLOAD_SIZE: usize = 1 << 20;
const DEFAULT_INITIAL_WRITE_WORKERS: usize = 1;
const DEFAULT_MIN_WRITE_WORKERS: usize = 1;
const DEFAULT_MAX_WRITE_WORKERS: usize = 64;
const DEFAULT_READ_WORKERS: usize = 4;
const DEFAULT_READ_OPS_PER_SECOND: u64 = 1_000;
const DEFAULT_READ_DELETED_PERCENT: f64 = 50.0;
const DEFAULT_READ_P99_SLO: Duration = Duration::from_millis(100);
const DEFAULT_READ_ATTAINMENT_PERCENT: f64 = 95.0;
const DEFAULT_DELETE_WORKERS: usize = 4;
const DEFAULT_DELETE_LAG_SLO: Duration = Duration::from_secs(30);
const DEFAULT_DELETE_TIMELY_PERCENT: f64 = 99.0;
const DEFAULT_CONTROL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_CONTROLLER_DEBOUNCE_WINDOWS: usize = 3;
const DEFAULT_WRITER_INCREASE_PERCENT: u64 = 25;
const DEFAULT_WRITER_DECREASE_PERCENT: u64 = 25;
const NATIVE_EXPIRATION_CLEANUP_BATCH_SIZE: usize = 256;
const CONTROLLER_UNHEALTHY_REASONS: &[&str] = &[
    "low_read_rate",
    "high_read_latency",
    "overdue_deletes",
    "read_errors",
    "correctness_errors",
    "put_errors",
    "delete_errors",
];
const DEFAULT_SPACE_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const STRATA_GC_IO_BYTES_PER_SEC: u64 = 1 << 30;
const DEFAULT_DELETED_SAMPLE_CAPACITY: usize = 1_000_000;
const DEFAULT_STARTING_EPOCH: Epoch = 1;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_UNSEALED_SEGMENTS: usize = 8;
const DEFAULT_READER_CACHE_CAPACITY: usize = store::DEFAULT_SEGMENT_READER_CACHE_CAPACITY;
const DEFAULT_ROCKSDB_MIN_BLOB_SIZE: u64 = 1;
const DEFAULT_ROCKSDB_BLOB_FILE_SIZE: u64 = 1 << 28;
const DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE: usize = 512 << 20;
const DEFAULT_ROCKSDB_DB_WRITE_BUFFER_SIZE: usize = 1 << 30;
const DEFAULT_ROCKSDB_MAX_WRITE_BUFFER_NUMBER: usize = 2;
const DEFAULT_ROCKSDB_MAX_SUBCOMPACTIONS: usize = 1;
const DEFAULT_ROCKSDB_HIGH_PRI_THREADS: usize = 4;
const DEFAULT_ROCKSDB_LOW_PRI_THREADS: usize = 1;
const DEFAULT_ROCKSDB_BLOB_GC_AGE_CUTOFF: f64 = 0.25;
const DEFAULT_ROCKSDB_BLOB_GC_FORCE_THRESHOLD: f64 = 1.0;
const DEFAULT_RELOCATION_PROFILE_READS: usize = 0;
const DEFAULT_RELOCATION_PROFILE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const ROCKSDB_CF_CLASS: &str = "rocksdb_blobdb";
const KEY_PREFIX: &[u8] = b"realistic-key-";
const STORAGE_FILE_TYPES: [&str; 4] =
    ["segment", "strata_lsm_table", "rocksdb_sst", "rocksdb_blob"];

fn main() {
    match Config::parse(env::args().skip(1)) {
        Ok(config) => {
            let result = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime
                    .block_on(run(config))
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error.to_string()),
            };
            if let Err(error) = result {
                eprintln!("error: {error}");
                process::exit(1);
            }
        }
        Err(error) if error == "help requested" => {
            println!("{}", usage());
        }
        Err(error) => {
            eprintln!("error: {error}\n\n{}", usage());
            process::exit(2);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    Strata,
    BlobDb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifetimeMode {
    Retention,
    Epoch,
}

impl LifetimeMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "retention" | "manual-retention" => Ok(Self::Retention),
            "epoch" | "epochs" | "epoch-lifetime" => Ok(Self::Epoch),
            _ => Err(format!("unknown lifetime mode '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Retention => "retention",
            Self::Epoch => "epoch",
        }
    }
}

impl EngineKind {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "strata" => Ok(Self::Strata),
            "blobdb" | "rocksdb-blobdb" => Ok(Self::BlobDb),
            _ => Err(format!("unknown engine '{value}'")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Strata => "strata",
            Self::BlobDb => "blobdb",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    engine: EngineKind,
    root_dir: PathBuf,
    namespace: String,
    duration: Duration,
    lifetime_mode: LifetimeMode,
    retention: Duration,
    /// Half-width, in percent of `retention`, of the uniform per-key jitter applied to it.
    retention_jitter_percent: u64,
    epoch_duration: Duration,
    future_epochs: Epoch,
    min_future_epoch_offset: Epoch,
    lifetime_seed: u64,
    hashed_keys: bool,
    cleanup_grace: Duration,
    payload_size: usize,
    put_ops_per_second: u64,
    initial_write_workers: usize,
    min_write_workers: usize,
    max_write_workers: usize,
    read_workers: usize,
    read_ops_per_second: u64,
    read_deleted_percent: f64,
    read_p99_slo: Duration,
    read_attainment_percent: f64,
    delete_workers: usize,
    delete_lag_slo: Duration,
    delete_timely_percent: f64,
    control_interval: Duration,
    controller_debounce_windows: usize,
    writer_increase_percent: u64,
    writer_decrease_percent: u64,
    sync_interval: Duration,
    space_sample_interval: Duration,
    deleted_sample_capacity: usize,
    metrics_listen: Option<String>,
    queue_capacity: usize,
    max_unsealed_segments: usize,
    segment_max_bytes: u64,
    strata_gc: bool,
    strata_gc_min_epoch_copy_distance: Option<Epoch>,
    /// Overrides the planner's per-plan copy cap for dead-ref rewrites (bytes).
    strata_gc_max_copy_bytes_per_plan: Option<u64>,
    strata_gc_workers: Option<usize>,
    strata_lsm_partitions: Option<u32>,
    strata_memtable_max_age: Option<Duration>,
    strata_relocation_writeback: Option<usize>,
    relocation_profile_reads: usize,
    relocation_profile_timeout: Duration,
    rocksdb_min_blob_size: u64,
    rocksdb_blob_file_size: u64,
    rocksdb_write_buffer_size: usize,
    rocksdb_db_write_buffer_size: usize,
    rocksdb_max_write_buffer_number: usize,
    rocksdb_max_background_flushes: Option<usize>,
    rocksdb_max_subcompactions: usize,
    rocksdb_high_pri_threads: usize,
    rocksdb_low_pri_threads: usize,
    rocksdb_blob_gc: bool,
    rocksdb_blob_gc_age_cutoff: f64,
    rocksdb_blob_gc_force_threshold: f64,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut engine_was_set = false;
        let mut config = Self {
            engine: EngineKind::Strata,
            root_dir: PathBuf::new(),
            namespace: DEFAULT_NAMESPACE.to_owned(),
            duration: DEFAULT_DURATION,
            lifetime_mode: LifetimeMode::Retention,
            retention: DEFAULT_RETENTION,
            retention_jitter_percent: 0,
            epoch_duration: DEFAULT_EPOCH_DURATION,
            future_epochs: DEFAULT_FUTURE_EPOCHS,
            min_future_epoch_offset: 1,
            lifetime_seed: DEFAULT_LIFETIME_SEED,
            hashed_keys: false,
            cleanup_grace: DEFAULT_CLEANUP_GRACE,
            payload_size: DEFAULT_PAYLOAD_SIZE,
            put_ops_per_second: 0,
            initial_write_workers: DEFAULT_INITIAL_WRITE_WORKERS,
            min_write_workers: DEFAULT_MIN_WRITE_WORKERS,
            max_write_workers: DEFAULT_MAX_WRITE_WORKERS,
            read_workers: DEFAULT_READ_WORKERS,
            read_ops_per_second: DEFAULT_READ_OPS_PER_SECOND,
            read_deleted_percent: DEFAULT_READ_DELETED_PERCENT,
            read_p99_slo: DEFAULT_READ_P99_SLO,
            read_attainment_percent: DEFAULT_READ_ATTAINMENT_PERCENT,
            delete_workers: DEFAULT_DELETE_WORKERS,
            delete_lag_slo: DEFAULT_DELETE_LAG_SLO,
            delete_timely_percent: DEFAULT_DELETE_TIMELY_PERCENT,
            control_interval: DEFAULT_CONTROL_INTERVAL,
            controller_debounce_windows: DEFAULT_CONTROLLER_DEBOUNCE_WINDOWS,
            writer_increase_percent: DEFAULT_WRITER_INCREASE_PERCENT,
            writer_decrease_percent: DEFAULT_WRITER_DECREASE_PERCENT,
            sync_interval: Duration::ZERO,
            space_sample_interval: DEFAULT_SPACE_SAMPLE_INTERVAL,
            deleted_sample_capacity: DEFAULT_DELETED_SAMPLE_CAPACITY,
            metrics_listen: None,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_unsealed_segments: DEFAULT_MAX_UNSEALED_SEGMENTS,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            strata_gc: true,
            strata_gc_min_epoch_copy_distance: None,
            strata_gc_max_copy_bytes_per_plan: None,
            strata_gc_workers: None,
            strata_lsm_partitions: None,
            strata_memtable_max_age: None,
            strata_relocation_writeback: None,
            relocation_profile_reads: DEFAULT_RELOCATION_PROFILE_READS,
            relocation_profile_timeout: DEFAULT_RELOCATION_PROFILE_TIMEOUT,
            rocksdb_min_blob_size: DEFAULT_ROCKSDB_MIN_BLOB_SIZE,
            rocksdb_blob_file_size: DEFAULT_ROCKSDB_BLOB_FILE_SIZE,
            rocksdb_write_buffer_size: DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE,
            rocksdb_db_write_buffer_size: DEFAULT_ROCKSDB_DB_WRITE_BUFFER_SIZE,
            rocksdb_max_write_buffer_number: DEFAULT_ROCKSDB_MAX_WRITE_BUFFER_NUMBER,
            rocksdb_max_background_flushes: None,
            rocksdb_max_subcompactions: DEFAULT_ROCKSDB_MAX_SUBCOMPACTIONS,
            rocksdb_high_pri_threads: DEFAULT_ROCKSDB_HIGH_PRI_THREADS,
            rocksdb_low_pri_threads: DEFAULT_ROCKSDB_LOW_PRI_THREADS,
            rocksdb_blob_gc: true,
            rocksdb_blob_gc_age_cutoff: DEFAULT_ROCKSDB_BLOB_GC_AGE_CUTOFF,
            rocksdb_blob_gc_force_threshold: DEFAULT_ROCKSDB_BLOB_GC_FORCE_THRESHOLD,
        };

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Err("help requested".to_owned()),
                "--engine" => {
                    config.engine = EngineKind::parse(&next_value(&mut args, &arg)?)?;
                    engine_was_set = true;
                }
                "--root" => config.root_dir = PathBuf::from(next_value(&mut args, &arg)?),
                "--namespace" => config.namespace = next_value(&mut args, &arg)?,
                "--duration" => config.duration = parse_duration(&next_value(&mut args, &arg)?)?,
                "--lifetime-mode" => {
                    config.lifetime_mode = LifetimeMode::parse(&next_value(&mut args, &arg)?)?
                }
                "--retention" => config.retention = parse_duration(&next_value(&mut args, &arg)?)?,
                "--retention-jitter-percent" => {
                    config.retention_jitter_percent = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--epoch-duration" => {
                    config.epoch_duration = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--future-epochs" => {
                    config.future_epochs = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--min-future-epoch-offset" => {
                    config.min_future_epoch_offset = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--lifetime-seed" => {
                    config.lifetime_seed = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--hashed-keys" => config.hashed_keys = parse_bool(&next_value(&mut args, &arg)?)?,
                "--cleanup-grace" => {
                    config.cleanup_grace = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--payload-size" => {
                    config.payload_size = parse_size(&next_value(&mut args, &arg)?)?
                }
                "--put-ops-per-second" => {
                    config.put_ops_per_second = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--initial-write-workers" => {
                    config.initial_write_workers =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--min-write-workers" => {
                    config.min_write_workers = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--max-write-workers" => {
                    config.max_write_workers = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--read-workers" => {
                    config.read_workers = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--read-ops-per-second" => {
                    config.read_ops_per_second = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--read-deleted-percent" => {
                    config.read_deleted_percent = parse_percent(&next_value(&mut args, &arg)?)?
                }
                "--read-p99-slo" => {
                    config.read_p99_slo = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--read-attainment-percent" => {
                    config.read_attainment_percent = parse_percent(&next_value(&mut args, &arg)?)?
                }
                "--delete-workers" => {
                    config.delete_workers = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--delete-lag-slo" => {
                    config.delete_lag_slo = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--delete-timely-percent" => {
                    config.delete_timely_percent = parse_percent(&next_value(&mut args, &arg)?)?
                }
                "--control-interval" => {
                    config.control_interval = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--controller-debounce-windows" => {
                    config.controller_debounce_windows =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--writer-increase-percent" => {
                    config.writer_increase_percent = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--writer-decrease-percent" => {
                    config.writer_decrease_percent = parse_u64(&next_value(&mut args, &arg)?)?
                }
                "--sync-interval" => {
                    config.sync_interval = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--space-sample-interval" => {
                    config.space_sample_interval = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--deleted-sample-capacity" => {
                    config.deleted_sample_capacity =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--metrics-listen" => {
                    let value = next_value(&mut args, &arg)?;
                    config.metrics_listen =
                        (!matches!(value.as_str(), "off" | "none")).then_some(value);
                }
                "--queue-capacity" => {
                    config.queue_capacity = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--max-unsealed-segments" => {
                    config.max_unsealed_segments =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--segment-max-bytes" => {
                    config.segment_max_bytes = parse_size(&next_value(&mut args, &arg)?)? as u64
                }
                "--strata-gc" => config.strata_gc = parse_bool(&next_value(&mut args, &arg)?)?,
                "--strata-gc-min-epoch-copy-distance" => {
                    config.strata_gc_min_epoch_copy_distance =
                        Some(parse_u64(&next_value(&mut args, &arg)?)?)
                }
                "--strata-gc-max-copy-bytes-per-plan" => {
                    config.strata_gc_max_copy_bytes_per_plan =
                        Some(parse_size(&next_value(&mut args, &arg)?)? as u64)
                }
                "--strata-gc-workers" => {
                    config.strata_gc_workers =
                        Some(parse_nonzero_usize(&next_value(&mut args, &arg)?)?)
                }
                "--strata-lsm-partitions" => {
                    let count = parse_u64(&next_value(&mut args, &arg)?)?;
                    config.strata_lsm_partitions = Some(
                        u32::try_from(count)
                            .ok()
                            .filter(|count| *count > 0)
                            .ok_or_else(|| {
                                "--strata-lsm-partitions must be between 1 and u32::MAX".to_owned()
                            })?,
                    );
                }
                "--strata-relocation-writeback" => {
                    let value = next_value(&mut args, &arg)?;
                    config.strata_relocation_writeback = if value == "off" {
                        None
                    } else {
                        Some(parse_nonzero_usize(&value)?)
                    };
                }
                "--relocation-profile-reads" => {
                    config.relocation_profile_reads =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--strata-memtable-max-age" => {
                    let age = parse_duration(&next_value(&mut args, &arg)?)?;
                    if age.is_zero() {
                        return Err("--strata-memtable-max-age must be non-zero".to_owned());
                    }
                    config.strata_memtable_max_age = Some(age);
                }
                "--relocation-profile-timeout" => {
                    config.relocation_profile_timeout =
                        parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-min-blob-size" => {
                    config.rocksdb_min_blob_size = parse_size(&next_value(&mut args, &arg)?)? as u64
                }
                "--rocksdb-blob-file-size" => {
                    config.rocksdb_blob_file_size =
                        parse_size(&next_value(&mut args, &arg)?)? as u64
                }
                "--rocksdb-write-buffer-size" => {
                    config.rocksdb_write_buffer_size = parse_size(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-db-write-buffer-size" => {
                    config.rocksdb_db_write_buffer_size = parse_size(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-max-write-buffer-number" => {
                    config.rocksdb_max_write_buffer_number =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-max-background-flushes" => {
                    config.rocksdb_max_background_flushes =
                        Some(parse_nonzero_usize(&next_value(&mut args, &arg)?)?)
                }
                "--rocksdb-max-subcompactions" => {
                    config.rocksdb_max_subcompactions =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-high-pri-threads" => {
                    config.rocksdb_high_pri_threads =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-low-pri-threads" => {
                    config.rocksdb_low_pri_threads =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-blob-gc" => {
                    config.rocksdb_blob_gc = parse_bool(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-blob-gc-age-cutoff" => {
                    config.rocksdb_blob_gc_age_cutoff =
                        parse_fraction(&next_value(&mut args, &arg)?)?
                }
                "--rocksdb-blob-gc-force-threshold" => {
                    config.rocksdb_blob_gc_force_threshold =
                        parse_fraction(&next_value(&mut args, &arg)?)?
                }
                _ => return Err(format!("unknown argument '{arg}'")),
            }
        }

        if !engine_was_set {
            return Err("--engine is required".to_owned());
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.root_dir.as_os_str().is_empty() {
            return Err("--root is required".to_owned());
        }
        for (name, duration) in [
            ("--duration", self.duration),
            ("--read-p99-slo", self.read_p99_slo),
            ("--delete-lag-slo", self.delete_lag_slo),
            ("--control-interval", self.control_interval),
            ("--space-sample-interval", self.space_sample_interval),
        ] {
            if duration.is_zero() {
                return Err(format!("{name} must be non-zero"));
            }
        }
        if self.retention_jitter_percent > 100 {
            return Err("--retention-jitter-percent must not exceed 100".to_owned());
        }
        if self.lifetime_mode == LifetimeMode::Epoch {
            if self.epoch_duration.is_zero() {
                return Err("--epoch-duration must be non-zero in epoch lifetime mode".to_owned());
            }
            if self.future_epochs == 0 {
                return Err("--future-epochs must be non-zero in epoch lifetime mode".to_owned());
            }
            if self.future_epochs > 10_000 {
                return Err("--future-epochs must not exceed 10000".to_owned());
            }
            if self.min_future_epoch_offset == 0 {
                return Err(
                    "--min-future-epoch-offset must be non-zero in epoch lifetime mode".to_owned(),
                );
            }
            if self.min_future_epoch_offset > self.future_epochs {
                return Err("--min-future-epoch-offset must not exceed --future-epochs".to_owned());
            }
            if DEFAULT_STARTING_EPOCH
                .checked_add(self.future_epochs)
                .is_none()
            {
                return Err("--future-epochs overflows the epoch range".to_owned());
            }
        }
        if self.payload_size == 0 {
            return Err("--payload-size must be non-zero".to_owned());
        }
        if self.segment_max_bytes == 0 {
            return Err("--segment-max-bytes must be non-zero".to_owned());
        }
        if self.max_unsealed_segments < 2 {
            return Err("--max-unsealed-segments must be at least 2".to_owned());
        }
        if self.min_write_workers > self.initial_write_workers
            || self.initial_write_workers > self.max_write_workers
        {
            return Err("write worker counts must satisfy min <= initial <= max".to_owned());
        }
        if self.writer_increase_percent == 0 {
            return Err("--writer-increase-percent must be non-zero".to_owned());
        }
        if self.writer_decrease_percent == 0 || self.writer_decrease_percent >= 100 {
            return Err("--writer-decrease-percent must be between 1 and 99".to_owned());
        }
        if self.rocksdb_min_blob_size == 0
            || self.rocksdb_blob_file_size == 0
            || self.rocksdb_write_buffer_size == 0
        {
            return Err("RocksDB blob and write-buffer sizes must be non-zero".to_owned());
        }
        if !(2..=i32::MAX as usize).contains(&self.rocksdb_max_write_buffer_number) {
            return Err(
                "--rocksdb-max-write-buffer-number must be between 2 and i32::MAX".to_owned(),
            );
        }
        if self
            .rocksdb_max_background_flushes
            .is_some_and(|flushes| flushes > i32::MAX as usize / 4)
        {
            return Err("--rocksdb-max-background-flushes must not exceed i32::MAX / 4".to_owned());
        }
        if self.rocksdb_max_subcompactions > u32::MAX as usize {
            return Err("--rocksdb-max-subcompactions must not exceed u32::MAX".to_owned());
        }
        if self.rocksdb_high_pri_threads > i32::MAX as usize
            || self.rocksdb_low_pri_threads > i32::MAX as usize
        {
            return Err("RocksDB background thread counts must not exceed i32::MAX".to_owned());
        }
        if self.relocation_profile_reads > 0 && self.engine != EngineKind::Strata {
            return Err("--relocation-profile-reads requires --engine strata".to_owned());
        }
        if self.strata_gc_min_epoch_copy_distance.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-gc-min-epoch-copy-distance requires --engine strata".to_owned());
        }
        if self.strata_gc_workers.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-gc-workers requires --engine strata".to_owned());
        }
        if self.strata_lsm_partitions.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-lsm-partitions requires --engine strata".to_owned());
        }
        if self.strata_memtable_max_age.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-memtable-max-age requires --engine strata".to_owned());
        }
        if self.strata_gc_max_copy_bytes_per_plan.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-gc-max-copy-bytes-per-plan requires --engine strata".to_owned());
        }
        if self.strata_relocation_writeback.is_some() && self.engine != EngineKind::Strata {
            return Err("--strata-relocation-writeback requires --engine strata".to_owned());
        }
        if self.relocation_profile_reads > 0 && self.relocation_profile_timeout.is_zero() {
            return Err("--relocation-profile-timeout must be non-zero".to_owned());
        }
        Ok(())
    }

    fn store_config(&self) -> StrataStoreConfig {
        let mut gc_planner_config = if self.relocation_profile_reads == 0 {
            GcPlannerConfig::default()
        } else {
            GcPlannerConfig {
                max_copy_bytes_per_plan: u64::MAX,
                max_l0_copy_bytes_per_plan: u64::MAX,
                min_l0_rewrite_epoch_distance: 1,
                min_l0_rewrite_useful_ratio_bps: 1,
                min_reclaim_bytes: 1,
                min_garbage_ratio_bps: 1,
                min_exact_epoch_bucket_bytes: u64::MAX,
                min_exact_epoch_distance: 1,
                max_exact_epoch_extension_count: 1,
                min_join_output_bytes: u64::MAX,
                max_join_sources: 2,
            }
        };
        if let Some(distance) = self.strata_gc_min_epoch_copy_distance {
            gc_planner_config.min_l0_rewrite_epoch_distance = distance;
            gc_planner_config.min_exact_epoch_distance = distance;
        }
        if let Some(cap) = self.strata_gc_max_copy_bytes_per_plan {
            gc_planner_config.max_copy_bytes_per_plan = cap;
        }
        StrataStoreConfig {
            root_dir: self.root_dir.clone(),
            namespace: self.namespace.clone(),
            segment_max_bytes: self.segment_max_bytes,
            write_queue_capacity: self.queue_capacity,
            max_unsealed_segments: self.max_unsealed_segments,
            segment_reader_cache_capacity: DEFAULT_READER_CACHE_CAPACITY,
            lsm_partition_count: self
                .strata_lsm_partitions
                .unwrap_or(store::DEFAULT_LSM_PARTITION_COUNT),
            lsm_compaction_patch_bytes: store::DEFAULT_LSM_COMPACTION_PATCH_BYTES,
            lsm_memtable_max_age: self
                .strata_memtable_max_age
                .unwrap_or(store::DEFAULT_LSM_MEMTABLE_MAX_AGE),
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            gc_workers_enabled: self.strata_gc && self.relocation_profile_reads == 0,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: self.strata_gc_workers.unwrap_or(DEFAULT_GC_WORKER_COUNT),
            gc_initial_worker_count: self
                .strata_gc_workers
                .unwrap_or(DEFAULT_GC_INITIAL_WORKER_COUNT),
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: STRATA_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config,
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: DEFAULT_STARTING_EPOCH,
            relocation_writeback_chunk: self.strata_relocation_writeback,
        }
    }
}

#[derive(Debug)]
struct AtomicLatency {
    buckets: [AtomicU64; 28],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

const LATENCY_BOUNDS_US: [u64; 28] = [
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    30_000_000,
    60_000_000,
    120_000_000,
    300_000_000,
    600_000_000,
    1_200_000_000,
    3_600_000_000,
    u64::MAX,
];

impl Default for AtomicLatency {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl AtomicLatency {
    fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let index = LATENCY_BOUNDS_US.partition_point(|bound| *bound < micros);
        self.buckets[index.min(self.buckets.len() - 1)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    fn quantile(&self, quantile: f64) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let target = ((count as f64 * quantile).ceil() as u64).max(1);
        let mut cumulative = 0_u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
            if cumulative >= target {
                let upper = LATENCY_BOUNDS_US[index];
                return if upper == u64::MAX {
                    Duration::from_micros(self.sum_micros.load(Ordering::Relaxed) / count.max(1))
                } else {
                    Duration::from_micros(upper)
                };
            }
        }
        Duration::ZERO
    }
}

#[derive(Debug, Default)]
struct ControlWindow {
    read_latencies_us: Vec<u64>,
    read_ops: u64,
    read_errors: u64,
    correctness_errors: u64,
    put_errors: u64,
    delete_errors: u64,
}

impl ControlWindow {
    fn record_read(&mut self, elapsed: Duration) {
        self.read_ops = self.read_ops.saturating_add(1);
        self.read_latencies_us
            .push(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    fn p99(&mut self) -> Duration {
        if self.read_latencies_us.is_empty() {
            return Duration::ZERO;
        }
        self.read_latencies_us.sort_unstable();
        let index = ((self.read_latencies_us.len() as f64 * 0.99).ceil() as usize)
            .saturating_sub(1)
            .min(self.read_latencies_us.len() - 1);
        Duration::from_micros(self.read_latencies_us[index])
    }
}

#[derive(Clone)]
struct HarnessMetrics {
    operation_attempts: IntCounterVec,
    operation_successes: IntCounterVec,
    operation_errors: IntCounterVec,
    operation_duration: HistogramVec,
    put_payload_bytes: IntCounter,
    retired_payload_bytes: IntCounter,
    epoch_advances: IntCounter,
    native_expirations: IntCounter,
    read_outcomes: IntCounterVec,
    correctness_errors: IntCounter,
    delete_due: IntCounter,
    delete_timely: IntCounter,
    delete_late: IntCounter,
    delete_lag: prometheus::Histogram,
    live_keys: IntGauge,
    deleted_sample_keys: IntGauge,
    logical_live_bytes: IntGauge,
    active_write_workers: IntGauge,
    target_put_ops_per_second: Gauge,
    client_load_active: IntGauge,
    controller_healthy: IntGauge,
    controller_unhealthy: IntGaugeVec,
    controller_debounce_streak_windows: IntGauge,
    controller_read_p99_seconds: Gauge,
    controller_read_ops_per_second: Gauge,
    overdue_delete_keys: IntGauge,
    oldest_overdue_seconds: Gauge,
    directory_apparent_bytes: IntGauge,
    directory_allocated_bytes: IntGauge,
    directory_files: IntGauge,
    storage_file_apparent_bytes: IntGaugeVec,
    strata_exact_epoch_bytes: IntGauge,
    strata_spillover_bytes: IntGauge,
    strata_epoch_directories: IntGauge,
    filesystem_available_bytes: IntGauge,
    filesystem_total_bytes: IntGauge,
    space_amplification: Gauge,
    space_sample_errors: IntCounter,
    blob_total_bytes: IntGauge,
    blob_garbage_bytes: IntGauge,
    blob_file_bytes_read: IntGauge,
    blob_file_bytes_written: IntGauge,
    blob_gc_bytes_relocated: IntGauge,
    rocksdb_wal_bytes_written: IntGauge,
    put_latency: Arc<AtomicLatency>,
    read_latency: Arc<AtomicLatency>,
    delete_latency: Arc<AtomicLatency>,
    delete_lag_latency: Arc<AtomicLatency>,
    control: Arc<Mutex<ControlWindow>>,
}

impl HarnessMetrics {
    fn new(registry: &Registry, engine: EngineKind) -> Result<Self, prometheus::Error> {
        let operation_attempts = IntCounterVec::new(
            Opts::new(
                "strata_realistic_bench_operation_attempts_total",
                "Client operations attempted by type.",
            )
            .const_label("engine", engine.as_str()),
            &["operation"],
        )?;
        let operation_successes = IntCounterVec::new(
            Opts::new(
                "strata_realistic_bench_operation_successes_total",
                "Client operations completed successfully by type.",
            )
            .const_label("engine", engine.as_str()),
            &["operation"],
        )?;
        let operation_errors = IntCounterVec::new(
            Opts::new(
                "strata_realistic_bench_operation_errors_total",
                "Storage operation errors by type.",
            )
            .const_label("engine", engine.as_str()),
            &["operation"],
        )?;
        let operation_duration = HistogramVec::new(
            HistogramOpts::new(
                "strata_realistic_bench_operation_duration_seconds",
                "Client-visible operation duration.",
            )
            .const_label("engine", engine.as_str())
            .buckets(vec![
                0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002, 0.005,
                0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0,
            ]),
            &["operation"],
        )?;
        let put_payload_bytes = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_put_payload_bytes_total",
                "Logical payload bytes accepted by successful puts.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let retired_payload_bytes = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_retired_payload_bytes_total",
                "Logical payload bytes retired by native expiry or successful due deletes.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let epoch_advances = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_epoch_advances_total",
                "Logical epoch transitions completed by the benchmark.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let native_expirations = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_native_expirations_total",
                "Keys retired by Strata epoch visibility without client tombstones.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let read_outcomes = IntCounterVec::new(
            Opts::new(
                "strata_realistic_bench_read_outcomes_total",
                "Validated read outcomes, including races.",
            )
            .const_label("engine", engine.as_str()),
            &["outcome"],
        )?;
        let correctness_errors = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_correctness_errors_total",
                "Reads that violated the client reference model.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let delete_due = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_delete_due_total",
                "Keys whose retention deadline has arrived.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let delete_timely = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_delete_timely_total",
                "Deletes acknowledged within the configured lag SLO.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let delete_late = IntCounter::with_opts(
            Opts::new(
                "strata_realistic_bench_delete_late_total",
                "Deletes acknowledged after the configured lag SLO.",
            )
            .const_label("engine", engine.as_str()),
        )?;
        let delete_lag = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "strata_realistic_bench_delete_lag_seconds",
                "Time from a key becoming due until delete acknowledgment.",
            )
            .const_label("engine", engine.as_str())
            .buckets(vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
                600.0,
            ]),
        )?;
        let controller_unhealthy = IntGaugeVec::new(
            Opts::new(
                "strata_realistic_bench_controller_unhealthy",
                "One for each failed controller obligation in the latest control window.",
            )
            .const_label("engine", engine.as_str()),
            &["reason"],
        )?;
        for reason in CONTROLLER_UNHEALTHY_REASONS {
            controller_unhealthy.with_label_values(&[*reason]).set(0);
        }
        let storage_file_apparent_bytes = IntGaugeVec::new(
            Opts::new(
                "strata_realistic_bench_storage_file_apparent_bytes",
                "Apparent bytes in benchmark files classified by storage role.",
            )
            .const_label("engine", engine.as_str()),
            &["file_type"],
        )?;
        for file_type in STORAGE_FILE_TYPES {
            storage_file_apparent_bytes
                .with_label_values(&[file_type])
                .set(0);
        }

        macro_rules! int_gauge {
            ($name:literal, $help:literal) => {
                IntGauge::with_opts(Opts::new($name, $help).const_label("engine", engine.as_str()))?
            };
        }
        macro_rules! gauge {
            ($name:literal, $help:literal) => {
                Gauge::with_opts(Opts::new($name, $help).const_label("engine", engine.as_str()))?
            };
        }

        let metrics = Self {
            operation_attempts,
            operation_successes,
            operation_errors,
            operation_duration,
            put_payload_bytes,
            retired_payload_bytes,
            epoch_advances,
            native_expirations,
            read_outcomes,
            correctness_errors,
            delete_due,
            delete_timely,
            delete_late,
            delete_lag,
            live_keys: int_gauge!(
                "strata_realistic_bench_live_keys",
                "Current logical live keys."
            ),
            deleted_sample_keys: int_gauge!(
                "strata_realistic_bench_deleted_sample_keys",
                "Deleted keys retained in the client negative-read sample."
            ),
            logical_live_bytes: int_gauge!(
                "strata_realistic_bench_logical_live_bytes",
                "Current logical live payload bytes."
            ),
            active_write_workers: int_gauge!(
                "strata_realistic_bench_active_write_workers",
                "Active writer concurrency; fixed at the configured maximum when a put rate is set."
            ),
            target_put_ops_per_second: gauge!(
                "strata_realistic_bench_target_put_ops_per_second",
                "Configured global put rate; zero means AIMD-controlled unbounded puts."
            ),
            client_load_active: int_gauge!(
                "strata_realistic_bench_client_load_active",
                "One during the client workload and zero during the cleanup grace period."
            ),
            controller_healthy: int_gauge!(
                "strata_realistic_bench_controller_healthy",
                "One when the latest read/delete service window met its obligations."
            ),
            controller_unhealthy,
            controller_debounce_streak_windows: int_gauge!(
                "strata_realistic_bench_controller_debounce_streak_windows",
                "Consecutive controller windows with the current health result since the last writer-count change."
            ),
            controller_read_p99_seconds: gauge!(
                "strata_realistic_bench_controller_read_p99_seconds",
                "Read p99 observed in the latest controller interval."
            ),
            controller_read_ops_per_second: gauge!(
                "strata_realistic_bench_controller_read_ops_per_second",
                "Reads completed per second in the latest controller interval."
            ),
            overdue_delete_keys: int_gauge!(
                "strata_realistic_bench_overdue_delete_keys",
                "Due keys not yet delete-acknowledged."
            ),
            oldest_overdue_seconds: gauge!(
                "strata_realistic_bench_oldest_overdue_seconds",
                "Age past the retention deadline of the oldest outstanding delete."
            ),
            directory_apparent_bytes: int_gauge!(
                "strata_realistic_bench_directory_apparent_bytes",
                "Apparent bytes under the benchmark database root."
            ),
            directory_allocated_bytes: int_gauge!(
                "strata_realistic_bench_directory_allocated_bytes",
                "Filesystem blocks allocated under the benchmark database root."
            ),
            directory_files: int_gauge!(
                "strata_realistic_bench_directory_files",
                "Files currently present under the benchmark database root."
            ),
            storage_file_apparent_bytes,
            strata_exact_epoch_bytes: int_gauge!(
                "strata_realistic_bench_strata_exact_epoch_bytes",
                "Apparent bytes in Strata retention segments grouped into exact epoch directories."
            ),
            strata_spillover_bytes: int_gauge!(
                "strata_realistic_bench_strata_spillover_bytes",
                "Apparent bytes in Strata spillover retention segments."
            ),
            strata_epoch_directories: int_gauge!(
                "strata_realistic_bench_strata_epoch_directories",
                "Current number of Strata exact-epoch retention directories."
            ),
            filesystem_available_bytes: int_gauge!(
                "strata_realistic_bench_filesystem_available_bytes",
                "Bytes available to an unprivileged process on the root filesystem."
            ),
            filesystem_total_bytes: int_gauge!(
                "strata_realistic_bench_filesystem_total_bytes",
                "Total bytes on the filesystem holding the benchmark root."
            ),
            space_amplification: gauge!(
                "strata_realistic_bench_space_amplification_ratio",
                "Allocated database bytes divided by current logical live payload bytes."
            ),
            space_sample_errors: IntCounter::with_opts(
                Opts::new(
                    "strata_realistic_bench_space_sample_errors_total",
                    "Directory or filesystem space sampling failures.",
                )
                .const_label("engine", engine.as_str()),
            )?,
            blob_total_bytes: int_gauge!(
                "strata_realistic_bench_blobdb_total_blob_file_bytes",
                "RocksDB-reported current BlobDB blob-file bytes."
            ),
            blob_garbage_bytes: int_gauge!(
                "strata_realistic_bench_blobdb_live_garbage_bytes",
                "RocksDB-reported garbage bytes in live blob files."
            ),
            blob_file_bytes_read: int_gauge!(
                "strata_realistic_bench_blobdb_blob_file_bytes_read",
                "Cumulative bytes read from BlobDB blob files."
            ),
            blob_file_bytes_written: int_gauge!(
                "strata_realistic_bench_blobdb_blob_file_bytes_written",
                "Cumulative bytes submitted to BlobDB blob files."
            ),
            blob_gc_bytes_relocated: int_gauge!(
                "strata_realistic_bench_blobdb_gc_bytes_relocated",
                "Cumulative live blob bytes relocated by BlobDB GC."
            ),
            rocksdb_wal_bytes_written: int_gauge!(
                "strata_realistic_bench_rocksdb_wal_bytes_written",
                "Cumulative bytes RocksDB has appended to its WAL."
            ),
            put_latency: Arc::new(AtomicLatency::default()),
            read_latency: Arc::new(AtomicLatency::default()),
            delete_latency: Arc::new(AtomicLatency::default()),
            delete_lag_latency: Arc::new(AtomicLatency::default()),
            control: Arc::new(Mutex::new(ControlWindow::default())),
        };

        registry.register(Box::new(metrics.operation_attempts.clone()))?;
        registry.register(Box::new(metrics.operation_successes.clone()))?;
        registry.register(Box::new(metrics.operation_errors.clone()))?;
        registry.register(Box::new(metrics.operation_duration.clone()))?;
        registry.register(Box::new(metrics.put_payload_bytes.clone()))?;
        registry.register(Box::new(metrics.retired_payload_bytes.clone()))?;
        registry.register(Box::new(metrics.epoch_advances.clone()))?;
        registry.register(Box::new(metrics.native_expirations.clone()))?;
        registry.register(Box::new(metrics.read_outcomes.clone()))?;
        registry.register(Box::new(metrics.correctness_errors.clone()))?;
        registry.register(Box::new(metrics.controller_unhealthy.clone()))?;
        registry.register(Box::new(metrics.delete_due.clone()))?;
        registry.register(Box::new(metrics.delete_timely.clone()))?;
        registry.register(Box::new(metrics.delete_late.clone()))?;
        registry.register(Box::new(metrics.delete_lag.clone()))?;
        for gauge in [
            &metrics.live_keys,
            &metrics.deleted_sample_keys,
            &metrics.logical_live_bytes,
            &metrics.active_write_workers,
            &metrics.client_load_active,
            &metrics.controller_healthy,
            &metrics.controller_debounce_streak_windows,
            &metrics.overdue_delete_keys,
            &metrics.directory_apparent_bytes,
            &metrics.directory_allocated_bytes,
            &metrics.directory_files,
            &metrics.strata_exact_epoch_bytes,
            &metrics.strata_spillover_bytes,
            &metrics.strata_epoch_directories,
            &metrics.filesystem_available_bytes,
            &metrics.filesystem_total_bytes,
            &metrics.blob_total_bytes,
            &metrics.blob_garbage_bytes,
            &metrics.blob_file_bytes_read,
            &metrics.blob_file_bytes_written,
            &metrics.blob_gc_bytes_relocated,
            &metrics.rocksdb_wal_bytes_written,
        ] {
            registry.register(Box::new(gauge.clone()))?;
        }
        for gauge in [
            &metrics.controller_read_p99_seconds,
            &metrics.controller_read_ops_per_second,
            &metrics.target_put_ops_per_second,
            &metrics.oldest_overdue_seconds,
            &metrics.space_amplification,
        ] {
            registry.register(Box::new(gauge.clone()))?;
        }
        registry.register(Box::new(metrics.space_sample_errors.clone()))?;
        registry.register(Box::new(metrics.storage_file_apparent_bytes.clone()))?;
        Ok(metrics)
    }

    fn record_operation(&self, operation: &str, elapsed: Duration, success: bool) {
        self.operation_duration
            .with_label_values(&[operation])
            .observe(elapsed.as_secs_f64());
        if success {
            self.operation_successes
                .with_label_values(&[operation])
                .inc();
        } else {
            self.operation_errors.with_label_values(&[operation]).inc();
        }
    }
}

trait BenchEngine: Send + Sync {
    fn put(&self, key: &BlobKey, logical_end_epoch: Option<Epoch>) -> Result<(), String>;
    fn delete(&self, key: &BlobKey) -> Result<(), String>;
    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, String>;
    fn sync(&self) -> Result<(), String>;
    fn advance_epoch(&self, expected_epoch: Epoch) -> Result<(), String>;
}

struct StrataEngine {
    store: Arc<StrataStore>,
    payload: Arc<[u8]>,
}

impl BenchEngine for StrataEngine {
    fn put(&self, key: &BlobKey, logical_end_epoch: Option<Epoch>) -> Result<(), String> {
        let Some(logical_end_epoch) = logical_end_epoch else {
            return self
                .store
                .put_arc(0, key.clone(), Arc::clone(&self.payload))
                .map(|_| ())
                .map_err(|error| error.to_string());
        };

        // Lifetime first is intentional: the following put inherits this still-live lifetime.
        // Keeping both operations in one writer batch prevents an epoch advance or another
        // mutation of this key from interleaving between the two LSNs.
        let mut batch = self.store.batch();
        batch.set_blob_lifetime(key.clone(), logical_end_epoch).put(
            0,
            key.clone(),
            Arc::clone(&self.payload),
        );
        batch.write().map(|_| ()).map_err(|error| error.to_string())
    }

    fn delete(&self, key: &BlobKey) -> Result<(), String> {
        self.store
            .tombstone(0, key)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, String> {
        self.store.get(key).map_err(|error| error.to_string())
    }

    fn sync(&self) -> Result<(), String> {
        self.store.sync().map_err(|error| error.to_string())
    }

    fn advance_epoch(&self, expected_epoch: Epoch) -> Result<(), String> {
        let (epoch, _) = self
            .store
            .increment_epoch()
            .map_err(|error| error.to_string())?;
        if epoch != expected_epoch {
            return Err(format!(
                "Strata advanced to epoch {epoch}, expected {expected_epoch}"
            ));
        }
        Ok(())
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlobDbValue(#[serde_as(as = "Bytes")] Vec<u8>);

struct BlobDbEngine {
    map: DBMap<BlobKey, BlobDbValue>,
    payload: BlobDbValue,
}

type OpenedEngine = (
    Arc<dyn BenchEngine>,
    Option<Arc<StrataStore>>,
    Option<BlobDbMetricsReporter>,
);

impl BenchEngine for BlobDbEngine {
    fn put(&self, key: &BlobKey, _logical_end_epoch: Option<Epoch>) -> Result<(), String> {
        self.map
            .insert(key, &self.payload)
            .map_err(|error| error.to_string())
    }

    fn delete(&self, key: &BlobKey) -> Result<(), String> {
        self.map.remove(key).map_err(|error| error.to_string())
    }

    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, String> {
        self.map
            .get(key)
            .map(|value| value.map(|value| value.0))
            .map_err(|error| error.to_string())
    }

    fn sync(&self) -> Result<(), String> {
        let db = standard_rocksdb(self.map.rocksdb.as_ref())
            .ok_or_else(|| "BlobDB benchmark requires standard RocksDB".to_owned())?;
        db.flush_wal(true).map_err(|error| error.to_string())
    }

    fn advance_epoch(&self, _expected_epoch: Epoch) -> Result<(), String> {
        // BlobDB has no native epoch primitive. The harness's deterministic expiry queue is its
        // logical clock; the deleters materialize each expiry as a point tombstone.
        Ok(())
    }
}

fn open_engine(
    config: &Config,
    payload: Arc<[u8]>,
    registry: &Registry,
    metrics: HarnessMetrics,
) -> Result<OpenedEngine, Box<dyn std::error::Error>> {
    match config.engine {
        EngineKind::Strata => {
            let metrics = StrataStoreMetrics::new(registry, config.engine.as_str())?;
            let store = Arc::new(StrataStore::open(config.store_config(), metrics)?);
            Ok((
                Arc::new(StrataEngine {
                    store: Arc::clone(&store),
                    payload,
                }),
                Some(store),
                None,
            ))
        }
        EngineKind::BlobDb => {
            DBMetrics::init(registry);
            let mut options = default_db_options().options;
            let mut env = Env::new()?;
            env.set_high_priority_background_threads(config.rocksdb_high_pri_threads as i32);
            env.set_low_priority_background_threads(config.rocksdb_low_pri_threads as i32);
            options.set_env(&env);
            options.create_if_missing(true);
            options.set_enable_blob_files(true);
            options.set_min_blob_size(config.rocksdb_min_blob_size);
            options.set_blob_file_size(config.rocksdb_blob_file_size);
            options.set_write_buffer_size(config.rocksdb_write_buffer_size);
            options.set_db_write_buffer_size(config.rocksdb_db_write_buffer_size);
            options.set_max_write_buffer_number(config.rocksdb_max_write_buffer_number as i32);
            if let Some(max_flushes) = config.rocksdb_max_background_flushes {
                // RocksDB derives the flush limit as max_background_jobs / 4. Setting the modern
                // aggregate option avoids the legacy max-background-flushes compatibility path.
                options.set_max_background_jobs((max_flushes * 4) as i32);
            }
            options.set_max_subcompactions(config.rocksdb_max_subcompactions as u32);
            options.set_enable_blob_gc(config.rocksdb_blob_gc);
            options.set_blob_gc_age_cutoff(config.rocksdb_blob_gc_age_cutoff);
            options.set_blob_gc_force_threshold(config.rocksdb_blob_gc_force_threshold);
            options.enable_statistics();
            let map = DBMap::open(
                config.root_dir.join("rocksdb-blobdb"),
                MetricConf::new(ROCKSDB_CF_CLASS),
                Some(options),
                None,
                Some(ROCKSDB_CF_CLASS),
                &ReadWriteOptions::default(),
            )?;
            let reporter = BlobDbMetricsReporter::start(Arc::clone(&map.rocksdb), metrics)?;
            let engine = Arc::new(BlobDbEngine {
                map,
                payload: BlobDbValue(payload.as_ref().to_vec()),
            });
            Ok((engine, None, Some(reporter)))
        }
    }
}

fn standard_rocksdb(db: &RocksDB) -> Option<&DB> {
    match db {
        RocksDB::DB(handle) => Some(&handle.underlying),
        RocksDB::OptimisticTransactionDB(_) => None,
    }
}

struct BlobDbMetricsReporter {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl BlobDbMetricsReporter {
    fn start(db: Arc<RocksDB>, metrics: HarnessMetrics) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("realistic-blobdb-metrics".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    metrics.blob_file_bytes_read.set(saturating_i64(
                        db.db_options()
                            .get_ticker_count(Ticker::BlobDbBlobFileBytesRead),
                    ));
                    metrics.blob_file_bytes_written.set(saturating_i64(
                        db.db_options()
                            .get_ticker_count(Ticker::BlobDbBlobFileBytesWritten),
                    ));
                    metrics.blob_gc_bytes_relocated.set(saturating_i64(
                        db.db_options()
                            .get_ticker_count(Ticker::BlobDbGcBytesRelocated),
                    ));
                    metrics.rocksdb_wal_bytes_written.set(saturating_i64(
                        db.db_options().get_ticker_count(Ticker::WalFileBytes),
                    ));
                    if let Some(standard) = standard_rocksdb(db.as_ref()) {
                        if let Ok(Some(value)) =
                            standard.property_int_value("rocksdb.total-blob-file-size")
                        {
                            metrics.blob_total_bytes.set(saturating_i64(value));
                        }
                        if let Ok(Some(value)) =
                            standard.property_int_value("rocksdb.live-blob-file-garbage-size")
                        {
                            metrics.blob_garbage_bytes.set(saturating_i64(value));
                        }
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

impl Drop for BlobDbMetricsReporter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

const KEY_LIVE: u8 = 0;
const KEY_DELETING: u8 = 1;
const KEY_DELETED: u8 = 2;

#[derive(Debug)]
struct KeyRecord {
    key: BlobKey,
    written_at: Instant,
    due_at: Instant,
    logical_end_epoch: Option<Epoch>,
    state: AtomicU8,
    due_counted: AtomicBool,
    sample_index: AtomicUsize,
}

impl KeyRecord {
    fn with_retention(key: BlobKey, written_at: Instant, retention: Duration) -> Self {
        Self {
            key,
            written_at,
            due_at: written_at.checked_add(retention).unwrap_or(written_at),
            logical_end_epoch: None,
            state: AtomicU8::new(KEY_LIVE),
            due_counted: AtomicBool::new(false),
            sample_index: AtomicUsize::new(usize::MAX),
        }
    }

    fn with_epoch(
        key: BlobKey,
        written_at: Instant,
        due_at: Instant,
        logical_end_epoch: Epoch,
    ) -> Self {
        Self {
            key,
            written_at,
            due_at,
            logical_end_epoch: Some(logical_end_epoch),
            state: AtomicU8::new(KEY_LIVE),
            due_counted: AtomicBool::new(false),
            sample_index: AtomicUsize::new(usize::MAX),
        }
    }

    fn mark_due(&self, metrics: &HarnessMetrics) {
        if !self.due_counted.swap(true, Ordering::AcqRel) {
            metrics.delete_due.inc();
        }
    }
}

impl PartialEq for KeyRecord {
    fn eq(&self, other: &Self) -> bool {
        self.due_at == other.due_at && self.key == other.key
    }
}

impl Eq for KeyRecord {}

impl PartialOrd for KeyRecord {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyRecord {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.due_at
            .cmp(&other.due_at)
            .then_with(|| self.key.as_bytes().cmp(other.key.as_bytes()))
    }
}

#[derive(Debug, Default)]
struct Model {
    live: BTreeSet<Arc<KeyRecord>>,
    live_sample: Vec<Arc<KeyRecord>>,
    deleting: HashMap<Vec<u8>, Arc<KeyRecord>>,
    deleted: VecDeque<Arc<KeyRecord>>,
    deleted_sample_capacity: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct DeleteBacklog {
    due_keys: u64,
    matured_overdue_keys: u64,
    oldest_lag: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct ModelSnapshot {
    live_keys: u64,
    deleting_keys: u64,
    deleted_sample_keys: u64,
    oldest_live_age: Duration,
}

#[derive(Debug, Clone, Copy)]
enum ReadExpectation {
    Live,
    Deleted,
}

impl Model {
    fn new(deleted_sample_capacity: usize) -> Self {
        Self {
            deleted_sample_capacity,
            ..Self::default()
        }
    }

    fn push_live(&mut self, record: Arc<KeyRecord>) {
        record
            .sample_index
            .store(self.live_sample.len(), Ordering::Relaxed);
        self.live_sample.push(Arc::clone(&record));
        self.live.insert(record);
    }

    fn pop_first_live(&mut self) -> Arc<KeyRecord> {
        let record = self.live.pop_first().expect("live key must remain present");
        self.remove_live_sample(&record);
        record
    }

    fn remove_live_sample(&mut self, record: &Arc<KeyRecord>) {
        let sample_index = record.sample_index.load(Ordering::Relaxed);
        let sampled = self.live_sample.swap_remove(sample_index);
        debug_assert!(Arc::ptr_eq(&record, &sampled));
        record.sample_index.store(usize::MAX, Ordering::Relaxed);
        if let Some(moved) = self.live_sample.get(sample_index) {
            moved.sample_index.store(sample_index, Ordering::Relaxed);
        }
    }

    fn claim_due(
        &mut self,
        now: Instant,
        current_epoch: Option<Epoch>,
        metrics: &HarnessMetrics,
    ) -> Option<Arc<KeyRecord>> {
        let record = self.live.first().filter(|record| {
            record.due_at <= now
                && record
                    .logical_end_epoch
                    .is_none_or(|end_epoch| current_epoch.is_some_and(|epoch| end_epoch <= epoch))
        })?;
        record.mark_due(metrics);
        let record = self.pop_first_live();
        record.state.store(KEY_DELETING, Ordering::Release);
        self.deleting
            .insert(record.key.as_bytes().to_vec(), Arc::clone(&record));
        Some(record)
    }

    /// Detaches the due prefix from the ordered live set without rewriting the sampling vector.
    ///
    /// Epoch writers are fenced while this runs, so no new record at or below `current_epoch` can
    /// appear. The returned records remain in `live_sample` temporarily and are marked deleting by
    /// the caller before the store epoch advances. Readers already holding one therefore classify
    /// a post-advance miss as a deletion race. Removing the sample entries can then be chunked
    /// outside the epoch fence instead of pausing every writer for the whole cleanup.
    fn detach_native_expirations(&mut self, current_epoch: Epoch) -> BTreeSet<Arc<KeyRecord>> {
        let first_retained = self
            .live
            .iter()
            .find(|record| {
                !record
                    .logical_end_epoch
                    .is_some_and(|end_epoch| end_epoch <= current_epoch)
            })
            .cloned();
        let expired = match first_retained {
            Some(first_retained) => {
                let retained = self.live.split_off(&first_retained);
                std::mem::replace(&mut self.live, retained)
            }
            None => std::mem::take(&mut self.live),
        };
        debug_assert!(expired.iter().all(|record| {
            record
                .logical_end_epoch
                .is_some_and(|end_epoch| end_epoch <= current_epoch)
        }));
        expired
    }

    fn finish_native_expirations(&mut self, records: &[Arc<KeyRecord>]) -> usize {
        for record in records {
            self.remove_live_sample(record);
            record.state.store(KEY_DELETED, Ordering::Release);
            self.deleted.push_back(Arc::clone(record));
        }
        while self.deleted.len() > self.deleted_sample_capacity {
            self.deleted.pop_front();
        }
        self.deleted.len()
    }

    fn finish_delete(&mut self, record: Arc<KeyRecord>) {
        self.deleting.remove(record.key.as_bytes());
        record.state.store(KEY_DELETED, Ordering::Release);
        self.deleted.push_back(record);
        while self.deleted.len() > self.deleted_sample_capacity {
            self.deleted.pop_front();
        }
    }

    fn sample(
        &self,
        prefer_deleted: bool,
        rng: &mut SplitMix64,
    ) -> Option<(Arc<KeyRecord>, ReadExpectation)> {
        let choose_live = |rng: &mut SplitMix64| {
            (!self.live_sample.is_empty()).then(|| {
                let index = rng.next_u64() as usize % self.live_sample.len();
                Arc::clone(
                    self.live_sample
                        .get(index)
                        .expect("sample index must be in range"),
                )
            })
        };
        let choose_deleted = |rng: &mut SplitMix64| {
            let records = &self.deleted;
            (!records.is_empty()).then(|| {
                let index = rng.next_u64() as usize % records.len();
                Arc::clone(records.get(index).expect("sample index must be in range"))
            })
        };
        if prefer_deleted {
            choose_deleted(rng)
                .map(|record| (record, ReadExpectation::Deleted))
                .or_else(|| choose_live(rng).map(|record| (record, ReadExpectation::Live)))
        } else {
            choose_live(rng)
                .map(|record| (record, ReadExpectation::Live))
                .or_else(|| choose_deleted(rng).map(|record| (record, ReadExpectation::Deleted)))
        }
    }

    fn refresh_delete_backlog(
        &self,
        now: Instant,
        current_epoch: Option<Epoch>,
        lag_slo: Duration,
        metrics: &HarnessMetrics,
    ) -> DeleteBacklog {
        let mut backlog = DeleteBacklog::default();
        for record in self
            .live
            .iter()
            .take_while(|record| record.due_at <= now)
            .chain(self.deleting.values())
        {
            if record.due_at > now
                || record
                    .logical_end_epoch
                    .is_some_and(|end_epoch| current_epoch.is_none_or(|epoch| end_epoch > epoch))
            {
                continue;
            }
            record.mark_due(metrics);
            let lag = now.saturating_duration_since(record.due_at);
            backlog.due_keys = backlog.due_keys.saturating_add(1);
            backlog.oldest_lag = backlog.oldest_lag.max(lag);
            if lag > lag_slo {
                backlog.matured_overdue_keys = backlog.matured_overdue_keys.saturating_add(1);
            }
        }
        backlog
    }

    fn snapshot(&self, now: Instant) -> ModelSnapshot {
        ModelSnapshot {
            live_keys: self.live.len() as u64,
            deleting_keys: self.deleting.len() as u64,
            deleted_sample_keys: self.deleted.len() as u64,
            oldest_live_age: self
                .live
                .first()
                .map(|record| now.saturating_duration_since(record.written_at))
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Default)]
struct FatalState {
    message: Mutex<Option<String>>,
}

impl FatalState {
    fn set(&self, message: impl Into<String>, stop: &AtomicBool) {
        let mut guard = self
            .message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            *guard = Some(message.into());
        }
        stop.store(true, Ordering::Release);
    }

    fn get(&self) -> Option<String> {
        self.message
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LifetimeAssignment {
    offset: Epoch,
    logical_end_epoch: Epoch,
    due_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct EpochClock {
    current_epoch: Epoch,
    next_transition_at: Instant,
}

impl EpochClock {
    fn assignment(&self, key_id: u64, config: &Config) -> LifetimeAssignment {
        let mut rng = SplitMix64::new(
            config
                .lifetime_seed
                .wrapping_add(key_id.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
        );
        let offset_count = config
            .future_epochs
            .saturating_sub(config.min_future_epoch_offset)
            .saturating_add(1);
        let offset = config.min_future_epoch_offset + rng.next_u64() % offset_count;
        let logical_end_epoch = self.current_epoch.saturating_add(offset);
        let intervals_after_next = u32::try_from(offset.saturating_sub(1))
            .expect("validated future epoch count must fit u32");
        let due_at = config
            .epoch_duration
            .checked_mul(intervals_after_next)
            .and_then(|duration| self.next_transition_at.checked_add(duration))
            .unwrap_or(self.next_transition_at);
        LifetimeAssignment {
            offset,
            logical_end_epoch,
            due_at,
        }
    }
}

#[derive(Debug)]
struct LifetimeOffsetCounts {
    counts: Vec<AtomicU64>,
}

impl LifetimeOffsetCounts {
    fn new(future_epochs: Epoch) -> Self {
        Self {
            counts: (0..future_epochs).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record(&self, offset: Epoch) {
        if let Some(count) = offset
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.counts.get(index))
        {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> Vec<u64> {
        self.counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }
}

/// Sequential keys sort in id order, so a memtable flush overlaps only the newest main-LSM base
/// file. Hashed keys sort like production blob ids: every flush spans the whole key space.
fn make_key(id: u64, hashed: bool) -> Result<BlobKey, String> {
    let mut bytes = Vec::with_capacity(KEY_PREFIX.len() + 20);
    bytes.extend_from_slice(KEY_PREFIX);
    if hashed {
        bytes.extend_from_slice(format!("{:016x}", splitmix64(id)).as_bytes());
    } else {
        bytes.extend_from_slice(id.to_string().as_bytes());
    }
    BlobKey::new(bytes).map_err(|error| error.to_string())
}

/// The retention one key gets: the configured retention, widened by a uniform jitter of up to
/// `retention_jitter_percent` either way, drawn from the lifetime seed and the key id so a run is
/// reproducible. Without jitter every key is deleted in write order and whole segments die
/// together; with it, deletion order decorrelates from write order, segments become partially
/// dead, and GC has to copy the survivors out. That is the workload that exercises relocation
/// under tombstone-discovered garbage.
fn jittered_retention(config: &Config, key_id: u64) -> Duration {
    if config.retention_jitter_percent == 0 {
        return config.retention;
    }
    let mut rng = SplitMix64::new(
        config
            .lifetime_seed
            .wrapping_add(key_id.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
    );
    // Uniform in [-1, 1], scaled to the configured half-width.
    let unit = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0;
    let factor = 1.0 + unit * config.retention_jitter_percent as f64 / 100.0;
    config.retention.mul_f64(factor.max(0.0))
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn make_payload(size: usize) -> Arc<[u8]> {
    let mut payload = vec![0_u8; size];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    Arc::from(payload)
}

struct WorkloadContext {
    config: Arc<Config>,
    engine: Arc<dyn BenchEngine>,
    expected_payload: Arc<[u8]>,
    model: Arc<Mutex<Model>>,
    metrics: HarnessMetrics,
    workload_stop: Arc<AtomicBool>,
    fatal: Arc<FatalState>,
    active_write_workers: Arc<AtomicUsize>,
    next_put_at: Arc<Mutex<Instant>>,
    next_key_id: Arc<AtomicU64>,
    epoch_clock: Arc<RwLock<EpochClock>>,
    current_epoch: Arc<AtomicU64>,
    lifetime_offset_counts: Arc<LifetimeOffsetCounts>,
}

fn run_writer(worker_index: usize, context: Arc<WorkloadContext>) {
    let put_interval = (context.config.put_ops_per_second != 0).then(|| {
        Duration::from_nanos(
            1_000_000_000_u64
                .checked_div(context.config.put_ops_per_second)
                .unwrap_or(0)
                .max(1),
        )
    });
    while !context.workload_stop.load(Ordering::Acquire) {
        if worker_index >= context.active_write_workers.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
            continue;
        }

        if let Some(put_interval) = put_interval {
            let scheduled_at = {
                let mut next_put_at = context
                    .next_put_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let scheduled_at = (*next_put_at).max(Instant::now());
                *next_put_at = scheduled_at
                    .checked_add(put_interval)
                    .unwrap_or(scheduled_at);
                scheduled_at
            };
            if sleep_until_stopped(
                &context.workload_stop,
                scheduled_at.saturating_duration_since(Instant::now()),
            ) {
                break;
            }
        }

        let id = context.next_key_id.fetch_add(1, Ordering::Relaxed);
        let key = match make_key(id, context.config.hashed_keys) {
            Ok(key) => key,
            Err(error) => {
                context.fatal.set(
                    format!("writer {worker_index} key generation failed: {error}"),
                    &context.workload_stop,
                );
                break;
            }
        };
        // The read guard keeps the epoch transition from interleaving between lifetime selection,
        // Strata's lifetime+put batch, and publication into the client reference model.
        let epoch_guard = (context.config.lifetime_mode == LifetimeMode::Epoch).then(|| {
            context
                .epoch_clock
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        });
        let assignment = epoch_guard
            .as_deref()
            .map(|clock| clock.assignment(id, &context.config));
        context
            .metrics
            .operation_attempts
            .with_label_values(&["put"])
            .inc();
        let started = Instant::now();
        match context.engine.put(
            &key,
            assignment.map(|assignment| assignment.logical_end_epoch),
        ) {
            Ok(()) => {
                let acknowledged_at = Instant::now();
                let elapsed = started.elapsed();
                context.metrics.record_operation("put", elapsed, true);
                context.metrics.put_latency.record(elapsed);
                context
                    .metrics
                    .put_payload_bytes
                    .inc_by(context.config.payload_size as u64);
                context.metrics.live_keys.inc();
                context
                    .metrics
                    .logical_live_bytes
                    .add(saturating_i64(context.config.payload_size as u64));
                let record = match assignment {
                    Some(assignment) => {
                        context.lifetime_offset_counts.record(assignment.offset);
                        KeyRecord::with_epoch(
                            key,
                            acknowledged_at,
                            assignment.due_at,
                            assignment.logical_end_epoch,
                        )
                    }
                    None => KeyRecord::with_retention(
                        key,
                        acknowledged_at,
                        jittered_retention(&context.config, id),
                    ),
                };
                context
                    .model
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push_live(Arc::new(record));
            }
            Err(error) => {
                let elapsed = started.elapsed();
                context.metrics.record_operation("put", elapsed, false);
                context.metrics.put_latency.record(elapsed);
                context
                    .metrics
                    .control
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .put_errors += 1;
                context.fatal.set(
                    format!("writer {worker_index} put failed for key {id}: {error}"),
                    &context.workload_stop,
                );
                break;
            }
        }
    }
}

fn run_deleter(worker_index: usize, context: Arc<WorkloadContext>) {
    while !context.workload_stop.load(Ordering::Acquire) {
        let current_epoch = (context.config.lifetime_mode == LifetimeMode::Epoch)
            .then(|| context.current_epoch.load(Ordering::Acquire));
        let record = context
            .model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .claim_due(Instant::now(), current_epoch, &context.metrics);
        let Some(record) = record else {
            thread::sleep(Duration::from_millis(2));
            continue;
        };

        context
            .metrics
            .operation_attempts
            .with_label_values(&["delete"])
            .inc();
        let started = Instant::now();
        match context.engine.delete(&record.key) {
            Ok(()) => {
                let acknowledged_at = Instant::now();
                let elapsed = started.elapsed();
                let lag = acknowledged_at.saturating_duration_since(record.due_at);
                context.metrics.record_operation("delete", elapsed, true);
                context.metrics.delete_latency.record(elapsed);
                context.metrics.delete_lag.observe(lag.as_secs_f64());
                context.metrics.delete_lag_latency.record(lag);
                if lag <= context.config.delete_lag_slo {
                    context.metrics.delete_timely.inc();
                } else {
                    context.metrics.delete_late.inc();
                }
                context
                    .metrics
                    .retired_payload_bytes
                    .inc_by(context.config.payload_size as u64);
                context.metrics.live_keys.dec();
                context
                    .metrics
                    .logical_live_bytes
                    .sub(saturating_i64(context.config.payload_size as u64));
                let deleted_len = {
                    let mut model = context
                        .model
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    model.finish_delete(record);
                    model.deleted.len()
                };
                context
                    .metrics
                    .deleted_sample_keys
                    .set(saturating_i64(deleted_len as u64));
            }
            Err(error) => {
                let elapsed = started.elapsed();
                context.metrics.record_operation("delete", elapsed, false);
                context.metrics.delete_latency.record(elapsed);
                context
                    .metrics
                    .control
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .delete_errors += 1;
                context.fatal.set(
                    format!(
                        "deleter {worker_index} failed for key {:?}: {error}",
                        record.key
                    ),
                    &context.workload_stop,
                );
                break;
            }
        }
    }
}

fn run_reader(worker_index: usize, context: Arc<WorkloadContext>) {
    debug_assert!(context.config.read_ops_per_second > 0);
    let global_interval_ns = 1_000_000_000_u64
        .checked_div(context.config.read_ops_per_second)
        .unwrap_or(1)
        .max(1);
    let worker_interval =
        Duration::from_nanos(global_interval_ns.saturating_mul(context.config.read_workers as u64));
    let mut next_operation_at = Instant::now()
        .checked_add(Duration::from_nanos(
            global_interval_ns.saturating_mul(worker_index as u64),
        ))
        .unwrap_or_else(Instant::now);
    let mut rng = SplitMix64::new(
        0xa11c_e5ed_5eed_u64
            .wrapping_add((worker_index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)),
    );
    let deleted_bps = (context.config.read_deleted_percent * 100.0).round() as u64;

    while !context.workload_stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now < next_operation_at {
            thread::sleep(
                next_operation_at
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(50)),
            );
            continue;
        }
        next_operation_at = next_operation_at
            .checked_add(worker_interval)
            .unwrap_or_else(Instant::now)
            .max(Instant::now());

        let prefer_deleted = rng.next_u64() % 10_000 < deleted_bps;
        let sample = context
            .model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .sample(prefer_deleted, &mut rng);
        let Some((record, expectation)) = sample else {
            continue;
        };

        let operation = match expectation {
            ReadExpectation::Live => "read_live",
            ReadExpectation::Deleted => "read_deleted",
        };
        context
            .metrics
            .operation_attempts
            .with_label_values(&[operation])
            .inc();
        let started = Instant::now();
        let result = context.engine.get(&record.key);
        let elapsed = started.elapsed();
        context.metrics.read_latency.record(elapsed);
        {
            let mut window = context
                .metrics
                .control
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            window.record_read(elapsed);
        }

        match result {
            Err(error) => {
                context.metrics.record_operation(operation, elapsed, false);
                context
                    .metrics
                    .control
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .read_errors += 1;
                context.fatal.set(
                    format!("reader {worker_index} failed for {:?}: {error}", record.key),
                    &context.workload_stop,
                );
                break;
            }
            Ok(value) => {
                context.metrics.record_operation(operation, elapsed, true);
                let state_after = record.state.load(Ordering::Acquire);
                let outcome = match (expectation, value) {
                    (ReadExpectation::Live, Some(value))
                        if value.as_slice() == context.expected_payload.as_ref() =>
                    {
                        Ok("live_hit")
                    }
                    (ReadExpectation::Live, None) if state_after != KEY_LIVE => Ok("delete_race"),
                    (ReadExpectation::Deleted, None) => Ok("deleted_miss"),
                    (ReadExpectation::Live, Some(_)) => Err("live_payload_mismatch"),
                    (ReadExpectation::Live, None) => Err("live_key_missing"),
                    (ReadExpectation::Deleted, Some(_)) => Err("deleted_key_visible"),
                };
                match outcome {
                    Ok(outcome) => context
                        .metrics
                        .read_outcomes
                        .with_label_values(&[outcome])
                        .inc(),
                    Err(outcome) => {
                        context
                            .metrics
                            .read_outcomes
                            .with_label_values(&[outcome])
                            .inc();
                        context.metrics.correctness_errors.inc();
                        context
                            .metrics
                            .control
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .correctness_errors += 1;
                        context.fatal.set(
                            format!(
                                "reader {worker_index} observed {outcome} for {:?}",
                                record.key
                            ),
                            &context.workload_stop,
                        );
                        break;
                    }
                }
            }
        }
    }
}

fn run_epoch_advancer(context: Arc<WorkloadContext>) {
    debug_assert_eq!(context.config.lifetime_mode, LifetimeMode::Epoch);
    while !context.workload_stop.load(Ordering::Acquire) {
        let next_transition_at = context
            .epoch_clock
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next_transition_at;
        if sleep_until_stopped(
            &context.workload_stop,
            next_transition_at.saturating_duration_since(Instant::now()),
        ) {
            break;
        }

        let transition = {
            let mut clock = context
                .epoch_clock
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if Instant::now() < clock.next_transition_at
                || context.workload_stop.load(Ordering::Acquire)
            {
                continue;
            }
            let scheduled_at = clock.next_transition_at;
            let Some(next_epoch) = clock.current_epoch.checked_add(1) else {
                context
                    .fatal
                    .set("epoch counter overflowed", &context.workload_stop);
                return;
            };

            // Fence native-expiring keys before Strata changes visibility. A reader that samples
            // one before the chunked sample cleanup sees KEY_DELETING and treats the miss as an
            // epoch race. BlobDB leaves these records live until its manual deleters acknowledge
            // them.
            let native_expirations = if context.config.engine == EngineKind::Strata {
                context
                    .model
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .detach_native_expirations(next_epoch)
            } else {
                BTreeSet::new()
            };
            for record in &native_expirations {
                record.state.store(KEY_DELETING, Ordering::Release);
            }

            context
                .metrics
                .operation_attempts
                .with_label_values(&["epoch_advance"])
                .inc();
            let started = Instant::now();
            if let Err(error) = context.engine.advance_epoch(next_epoch) {
                context
                    .metrics
                    .record_operation("epoch_advance", started.elapsed(), false);
                context.fatal.set(
                    format!("advancing to epoch {next_epoch} failed: {error}"),
                    &context.workload_stop,
                );
                return;
            }
            context
                .metrics
                .record_operation("epoch_advance", started.elapsed(), true);
            context.metrics.epoch_advances.inc();
            clock.current_epoch = next_epoch;
            context.current_epoch.store(next_epoch, Ordering::Release);
            clock.next_transition_at = scheduled_at
                .checked_add(context.config.epoch_duration)
                .unwrap_or_else(Instant::now);
            (scheduled_at, native_expirations)
        };

        let (scheduled_at, native_expirations) = transition;
        let expired = native_expirations.len() as u64;
        if expired > 0 {
            let native_expirations = native_expirations.into_iter().collect::<Vec<_>>();
            for record in &native_expirations {
                record.mark_due(&context.metrics);
            }
            let mut deleted_sample_keys = 0;
            for records in native_expirations.chunks(NATIVE_EXPIRATION_CLEANUP_BATCH_SIZE) {
                deleted_sample_keys = context
                    .model
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .finish_native_expirations(records);
                thread::yield_now();
            }
            let lag = Instant::now().saturating_duration_since(scheduled_at);
            context.metrics.native_expirations.inc_by(expired);
            context
                .metrics
                .retired_payload_bytes
                .inc_by(expired.saturating_mul(context.config.payload_size as u64));
            context.metrics.live_keys.sub(saturating_i64(expired));
            context.metrics.logical_live_bytes.sub(saturating_i64(
                expired.saturating_mul(context.config.payload_size as u64),
            ));
            context
                .metrics
                .deleted_sample_keys
                .set(saturating_i64(deleted_sample_keys as u64));
            context.metrics.delete_lag.observe(lag.as_secs_f64());
            context.metrics.delete_lag_latency.record(lag);
            if lag <= context.config.delete_lag_slo {
                context.metrics.delete_timely.inc_by(expired);
            } else {
                context.metrics.delete_late.inc_by(expired);
            }
        }
    }
}

fn run_syncer(context: Arc<WorkloadContext>) {
    if context.config.sync_interval.is_zero() {
        return;
    }
    while !context.workload_stop.load(Ordering::Acquire) {
        if sleep_until_stopped(&context.workload_stop, context.config.sync_interval) {
            break;
        }
        context
            .metrics
            .operation_attempts
            .with_label_values(&["sync"])
            .inc();
        let started = Instant::now();
        match context.engine.sync() {
            Ok(()) => context
                .metrics
                .record_operation("sync", started.elapsed(), true),
            Err(error) => {
                context
                    .metrics
                    .record_operation("sync", started.elapsed(), false);
                context.fatal.set(
                    format!("periodic sync failed: {error}"),
                    &context.workload_stop,
                );
                break;
            }
        }
    }
}

fn run_controller(context: Arc<WorkloadContext>) {
    let mut debounce = ControllerDebounce::default();
    while !context.workload_stop.load(Ordering::Acquire) {
        if sleep_until_stopped(&context.workload_stop, context.config.control_interval) {
            break;
        }
        let mut window = {
            let mut guard = context
                .metrics
                .control
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *guard)
        };
        let read_p99 = window.p99();
        let read_rate = window.read_ops as f64 / context.config.control_interval.as_secs_f64();
        let required_read_rate = context.config.read_ops_per_second as f64
            * context.config.read_attainment_percent
            / 100.0;
        let current_epoch = (context.config.lifetime_mode == LifetimeMode::Epoch)
            .then(|| context.current_epoch.load(Ordering::Acquire));
        let backlog = context
            .model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .refresh_delete_backlog(
                Instant::now(),
                current_epoch,
                context.config.delete_lag_slo,
                &context.metrics,
            );
        context
            .metrics
            .controller_read_p99_seconds
            .set(read_p99.as_secs_f64());
        context
            .metrics
            .controller_read_ops_per_second
            .set(read_rate);
        context
            .metrics
            .overdue_delete_keys
            .set(saturating_i64(backlog.due_keys));
        context
            .metrics
            .oldest_overdue_seconds
            .set(backlog.oldest_lag.as_secs_f64());

        let unhealthy = [
            ("low_read_rate", read_rate < required_read_rate),
            ("high_read_latency", read_p99 > context.config.read_p99_slo),
            ("overdue_deletes", backlog.matured_overdue_keys != 0),
            ("read_errors", window.read_errors != 0),
            ("correctness_errors", window.correctness_errors != 0),
            ("put_errors", window.put_errors != 0),
            ("delete_errors", window.delete_errors != 0),
        ];
        for (reason, active) in unhealthy {
            context
                .metrics
                .controller_unhealthy
                .with_label_values(&[reason])
                .set(i64::from(active));
        }
        let healthy = unhealthy.iter().all(|(_, active)| !active);
        context.metrics.controller_healthy.set(i64::from(healthy));
        if context.config.put_ops_per_second != 0 {
            continue;
        }
        let should_adjust = debounce.observe(healthy, context.config.controller_debounce_windows);
        context
            .metrics
            .controller_debounce_streak_windows
            .set(debounce.streak_windows as i64);
        if !should_adjust {
            continue;
        }

        let current = context.active_write_workers.load(Ordering::Acquire);
        let next = next_writer_count(
            current,
            healthy,
            context.config.min_write_workers,
            context.config.max_write_workers,
            context.config.writer_increase_percent,
            context.config.writer_decrease_percent,
        );
        context.active_write_workers.store(next, Ordering::Release);
        context.metrics.active_write_workers.set(next as i64);
        debounce.reset_streak();
    }
}

#[derive(Debug, Default)]
struct ControllerDebounce {
    last_healthy: Option<bool>,
    streak_windows: usize,
}

impl ControllerDebounce {
    fn observe(&mut self, healthy: bool, required_windows: usize) -> bool {
        if self.last_healthy == Some(healthy) {
            self.streak_windows = self.streak_windows.saturating_add(1);
        } else {
            self.last_healthy = Some(healthy);
            self.streak_windows = 1;
        }
        self.streak_windows >= required_windows
    }

    fn reset_streak(&mut self) {
        self.streak_windows = 0;
    }
}

fn next_writer_count(
    current: usize,
    healthy: bool,
    minimum: usize,
    maximum: usize,
    increase_percent: u64,
    decrease_percent: u64,
) -> usize {
    if healthy {
        let increase = (current as u128 * increase_percent as u128)
            .div_ceil(100)
            .max(1)
            .min(usize::MAX as u128) as usize;
        current.saturating_add(increase).clamp(minimum, maximum)
    } else {
        let keep_percent = 100_u64.saturating_sub(decrease_percent);
        let decreased =
            (current as u128 * keep_percent as u128 / 100).min(usize::MAX as u128) as usize;
        decreased.clamp(minimum, maximum)
    }
}

fn sleep_until_stopped(stop: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now()
        .checked_add(duration)
        .unwrap_or_else(Instant::now);
    while !stop.load(Ordering::Acquire) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(100)));
    }
    true
}

#[derive(Debug, Clone, Copy, Default)]
struct SpaceSnapshot {
    apparent_bytes: u64,
    allocated_bytes: u64,
    files: u64,
    filesystem_available_bytes: u64,
    filesystem_total_bytes: u64,
    storage_file_apparent_bytes: [u64; STORAGE_FILE_TYPES.len()],
    strata_exact_epoch_bytes: u64,
    strata_spillover_bytes: u64,
    strata_epoch_directories: u64,
}

impl SpaceSnapshot {
    fn amplification(self, logical_live_bytes: u64) -> f64 {
        if logical_live_bytes == 0 {
            0.0
        } else {
            self.allocated_bytes as f64 / logical_live_bytes as f64
        }
    }
}

struct MetricsServer {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MetricsServer {
    fn start(address: &str, registry: Arc<Registry>) -> io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("realistic-metrics-http".to_owned())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => serve_metrics(stream, &registry),
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(25));
                        }
                        Err(error) => {
                            eprintln!("metrics server accept error: {error}");
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve_metrics(mut stream: TcpStream, registry: &Registry) {
    let mut request = [0_u8; 4096];
    let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
    let read = stream.read(&mut request).unwrap_or_default();
    let metrics_path = request[..read].starts_with(b"GET /metrics ");
    let (status, content_type, body) = if metrics_path {
        let encoder = TextEncoder::new();
        let mut body = Vec::new();
        match encoder.encode(&registry.gather(), &mut body) {
            Ok(()) => ("200 OK", encoder.format_type().to_owned(), body),
            Err(error) => (
                "500 Internal Server Error",
                "text/plain; charset=utf-8".to_owned(),
                error.to_string().into_bytes(),
            ),
        }
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8".to_owned(),
            b"metrics are available at /metrics\n".to_vec(),
        )
    };
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(&body);
}

fn prepare_root(root: &Path) -> io::Result<()> {
    if root.exists() {
        if !root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("benchmark root '{}' is not a directory", root.display()),
            ));
        }
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "benchmark root '{}' is not empty; use a fresh directory",
                    root.display()
                ),
            ));
        }
    } else {
        fs::create_dir_all(root)?;
    }
    Ok(())
}

fn sample_space(root: &Path, metrics: &HarnessMetrics) -> io::Result<SpaceSnapshot> {
    let mut snapshot = SpaceSnapshot::default();
    summarize_path(root, &mut snapshot)?;
    let (available, total) = filesystem_space(root)?;
    snapshot.filesystem_available_bytes = available;
    snapshot.filesystem_total_bytes = total;
    publish_space(snapshot, metrics);
    Ok(snapshot)
}

fn summarize_path(path: &Path, snapshot: &mut SpaceSnapshot) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    snapshot.apparent_bytes = snapshot.apparent_bytes.saturating_add(metadata.len());
    #[cfg(unix)]
    {
        snapshot.allocated_bytes = snapshot
            .allocated_bytes
            .saturating_add(metadata.blocks().saturating_mul(512));
    }
    #[cfg(not(unix))]
    {
        snapshot.allocated_bytes = snapshot.allocated_bytes.saturating_add(metadata.len());
    }

    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        snapshot.files = snapshot.files.saturating_add(1);
        if let Some(file_type) = storage_file_type(path) {
            snapshot.storage_file_apparent_bytes[file_type] =
                snapshot.storage_file_apparent_bytes[file_type].saturating_add(metadata.len());
        }
        if path
            .extension()
            .is_some_and(|extension| extension == "data")
        {
            if path_has_component_prefix(path, "epoch-") {
                snapshot.strata_exact_epoch_bytes = snapshot
                    .strata_exact_epoch_bytes
                    .saturating_add(metadata.len());
            } else if path_has_component(path, "spillover") {
                snapshot.strata_spillover_bytes = snapshot
                    .strata_spillover_bytes
                    .saturating_add(metadata.len());
            }
        }
        return Ok(());
    }
    if metadata.is_dir() {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("epoch-"))
        {
            snapshot.strata_epoch_directories = snapshot.strata_epoch_directories.saturating_add(1);
        }
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            match entry {
                Ok(entry) => summarize_path(&entry.path(), snapshot)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

fn storage_file_type(path: &Path) -> Option<usize> {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("data") => Some(0),
        Some("sst") if is_strata_lsm_table(path) => Some(1),
        Some("sst") => Some(2),
        Some("blob") => Some(3),
        _ => None,
    }
}

fn is_strata_lsm_table(path: &Path) -> bool {
    path.components().any(|component| {
        let component = component.as_os_str();
        component == "lsm" || component == "relocations"
    })
}

fn path_has_component(path: &Path, expected: &str) -> bool {
    path.components()
        .any(|component| component.as_os_str() == expected)
}

fn path_has_component_prefix(path: &Path, prefix: &str) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|component| component.starts_with(prefix))
    })
}

#[cfg(unix)]
fn filesystem_space(path: &Path) -> io::Result<(u64, u64)> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains a NUL byte: '{}'", path.display()),
        )
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated C string and `stats` points to writable memory.
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs initialized `stats` after returning success.
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize;
    Ok((
        (stats.f_bavail as u64).saturating_mul(block_size),
        (stats.f_blocks as u64).saturating_mul(block_size),
    ))
}

#[cfg(not(unix))]
fn filesystem_space(_path: &Path) -> io::Result<(u64, u64)> {
    Ok((0, 0))
}

fn publish_space(snapshot: SpaceSnapshot, metrics: &HarnessMetrics) {
    metrics
        .directory_apparent_bytes
        .set(saturating_i64(snapshot.apparent_bytes));
    metrics
        .directory_allocated_bytes
        .set(saturating_i64(snapshot.allocated_bytes));
    metrics.directory_files.set(saturating_i64(snapshot.files));
    metrics
        .strata_exact_epoch_bytes
        .set(saturating_i64(snapshot.strata_exact_epoch_bytes));
    metrics
        .strata_spillover_bytes
        .set(saturating_i64(snapshot.strata_spillover_bytes));
    metrics
        .strata_epoch_directories
        .set(saturating_i64(snapshot.strata_epoch_directories));
    for (file_type, bytes) in STORAGE_FILE_TYPES
        .iter()
        .zip(snapshot.storage_file_apparent_bytes)
    {
        metrics
            .storage_file_apparent_bytes
            .with_label_values(&[file_type])
            .set(saturating_i64(bytes));
    }
    metrics
        .filesystem_available_bytes
        .set(saturating_i64(snapshot.filesystem_available_bytes));
    metrics
        .filesystem_total_bytes
        .set(saturating_i64(snapshot.filesystem_total_bytes));
    let logical_live_bytes = metrics.logical_live_bytes.get().max(0) as u64;
    metrics
        .space_amplification
        .set(snapshot.amplification(logical_live_bytes));
}

fn start_space_sampler(
    root: PathBuf,
    metrics: HarnessMetrics,
    stop: Arc<AtomicBool>,
    interval: Duration,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("realistic-space-sampler".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if let Err(error) = sample_space(&root, &metrics) {
                    metrics.space_sample_errors.inc();
                    eprintln!("space sampling failed for '{}': {error}", root.display());
                }
                if sleep_until_stopped(&stop, interval) {
                    break;
                }
            }
        })
}

async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    prepare_root(&config.root_dir)?;

    let config = Arc::new(config);
    let registry = Arc::new(Registry::new());
    let metrics = HarnessMetrics::new(&registry, config.engine)?;
    let _metrics_server = config
        .metrics_listen
        .as_deref()
        .map(|address| MetricsServer::start(address, Arc::clone(&registry)))
        .transpose()?;
    let payload = make_payload(config.payload_size);
    let (engine, strata_store, _blobdb_reporter) =
        open_engine(&config, Arc::clone(&payload), &registry, metrics.clone())?;

    println!("engine={}", config.engine.as_str());
    println!("root={}", config.root_dir.display());
    println!("duration_seconds={:.3}", config.duration.as_secs_f64());
    println!("lifetime_mode={}", config.lifetime_mode.as_str());
    match config.lifetime_mode {
        LifetimeMode::Retention => {
            println!("retention_seconds={:.3}", config.retention.as_secs_f64());
            println!(
                "retention_jitter_percent={}",
                config.retention_jitter_percent
            );
        }
        LifetimeMode::Epoch => {
            println!(
                "epoch_duration_seconds={:.3}",
                config.epoch_duration.as_secs_f64()
            );
            println!("future_epochs={}", config.future_epochs);
            println!("min_future_epoch_offset={}", config.min_future_epoch_offset);
            println!("lifetime_seed={}", config.lifetime_seed);
            println!("hashed_keys={}", config.hashed_keys);
        }
    }
    println!(
        "cleanup_grace_seconds={:.3}",
        config.cleanup_grace.as_secs_f64()
    );
    println!("payload_bytes={}", config.payload_size);
    println!("put_target_ops_per_second={}", config.put_ops_per_second);
    println!(
        "write_workers_min_initial_max={},{},{}",
        config.min_write_workers, config.initial_write_workers, config.max_write_workers
    );
    println!("read_workers={}", config.read_workers);
    println!("read_target_ops_per_second={}", config.read_ops_per_second);
    println!("read_deleted_percent={:.3}", config.read_deleted_percent);
    println!(
        "read_p99_slo_seconds={:.6}",
        config.read_p99_slo.as_secs_f64()
    );
    println!(
        "read_attainment_percent={:.3}",
        config.read_attainment_percent
    );
    println!("delete_workers={}", config.delete_workers);
    println!(
        "delete_lag_slo_seconds={:.6}",
        config.delete_lag_slo.as_secs_f64()
    );
    println!(
        "delete_timely_percent_required={:.3}",
        config.delete_timely_percent
    );
    println!(
        "controller_interval_seconds={:.3}",
        config.control_interval.as_secs_f64()
    );
    println!(
        "controller_debounce_windows={}",
        config.controller_debounce_windows
    );
    println!(
        "sync_interval_seconds={:.3}",
        config.sync_interval.as_secs_f64()
    );
    match config.engine {
        EngineKind::Strata => {
            println!("segment_max_bytes={}", config.segment_max_bytes);
            println!("strata_gc={}", config.strata_gc);
            let planner = config.store_config().gc_planner_config;
            println!(
                "strata_gc_workers={}",
                config.store_config().gc_worker_count
            );
            println!(
                "strata_lsm_partitions={}",
                config.store_config().lsm_partition_count
            );
            println!(
                "strata_memtable_max_age={:?}",
                config.store_config().lsm_memtable_max_age
            );
            println!(
                "strata_gc_min_l0_rewrite_epoch_distance={}",
                planner.min_l0_rewrite_epoch_distance
            );
            println!(
                "strata_gc_min_l0_rewrite_useful_ratio_bps={}",
                planner.min_l0_rewrite_useful_ratio_bps
            );
            println!(
                "strata_gc_min_exact_epoch_distance={}",
                planner.min_exact_epoch_distance
            );
            println!(
                "strata_gc_max_copy_bytes_per_plan={}",
                planner.max_copy_bytes_per_plan
            );
            println!(
                "strata_relocation_writeback_chunk={}",
                config
                    .strata_relocation_writeback
                    .map_or("off".to_owned(), |chunk| chunk.to_string())
            );
            println!(
                "relocation_profile_reads={}",
                config.relocation_profile_reads
            );
            println!(
                "relocation_profile_timeout_seconds={:.3}",
                config.relocation_profile_timeout.as_secs_f64()
            );
        }
        EngineKind::BlobDb => {
            println!("rocksdb_wal_enabled=true");
            println!("rocksdb_per_write_sync=false");
            println!(
                "rocksdb_write_buffer_size={}",
                config.rocksdb_write_buffer_size
            );
            println!(
                "rocksdb_db_write_buffer_size={}",
                config.rocksdb_db_write_buffer_size
            );
            println!(
                "rocksdb_max_write_buffer_number={}",
                config.rocksdb_max_write_buffer_number
            );
            println!(
                "rocksdb_max_background_flushes={}",
                config
                    .rocksdb_max_background_flushes
                    .map_or_else(|| "default".to_owned(), |value| value.to_string())
            );
            println!(
                "rocksdb_max_subcompactions={}",
                config.rocksdb_max_subcompactions
            );
            println!(
                "rocksdb_high_low_pri_threads={},{}",
                config.rocksdb_high_pri_threads, config.rocksdb_low_pri_threads
            );
            println!("rocksdb_blob_gc={}", config.rocksdb_blob_gc);
        }
    }
    if let Some(address) = &config.metrics_listen {
        println!("metrics=http://{address}/metrics");
    }

    let workload_stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let fatal = Arc::new(FatalState::default());
    let initial_active_write_workers = if config.put_ops_per_second == 0 {
        config.initial_write_workers
    } else {
        config.max_write_workers
    };
    let active_write_workers = Arc::new(AtomicUsize::new(initial_active_write_workers));
    metrics
        .active_write_workers
        .set(initial_active_write_workers as i64);
    metrics
        .target_put_ops_per_second
        .set(config.put_ops_per_second as f64);
    let workload_started_at = Instant::now();
    let initial_epoch = match (&config.lifetime_mode, strata_store.as_deref()) {
        (LifetimeMode::Epoch, Some(store)) => store.current_epoch()?,
        _ => DEFAULT_STARTING_EPOCH,
    };
    let epoch_clock = Arc::new(RwLock::new(EpochClock {
        current_epoch: initial_epoch,
        next_transition_at: workload_started_at
            .checked_add(config.epoch_duration)
            .unwrap_or(workload_started_at),
    }));
    let current_epoch = Arc::new(AtomicU64::new(initial_epoch));
    let lifetime_offset_counts = Arc::new(LifetimeOffsetCounts::new(config.future_epochs));
    let model = Arc::new(Mutex::new(Model::new(config.deleted_sample_capacity)));
    let context = Arc::new(WorkloadContext {
        config: Arc::clone(&config),
        engine: Arc::clone(&engine),
        expected_payload: Arc::clone(&payload),
        model: Arc::clone(&model),
        metrics: metrics.clone(),
        workload_stop: Arc::clone(&workload_stop),
        fatal: Arc::clone(&fatal),
        active_write_workers: Arc::clone(&active_write_workers),
        next_put_at: Arc::new(Mutex::new(workload_started_at)),
        next_key_id: Arc::new(AtomicU64::new(0)),
        epoch_clock: Arc::clone(&epoch_clock),
        current_epoch: Arc::clone(&current_epoch),
        lifetime_offset_counts: Arc::clone(&lifetime_offset_counts),
    });
    metrics.client_load_active.set(1);

    let sampler = start_space_sampler(
        config.root_dir.clone(),
        metrics.clone(),
        Arc::clone(&sampler_stop),
        config.space_sample_interval,
    )?;
    let mut workers = Vec::new();
    for worker_index in 0..config.max_write_workers {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name(format!("realistic-writer-{worker_index}"))
                .spawn(move || run_writer(worker_index, worker_context))?,
        );
    }
    if config.lifetime_mode == LifetimeMode::Retention || config.engine == EngineKind::BlobDb {
        for worker_index in 0..config.delete_workers {
            let worker_context = Arc::clone(&context);
            workers.push(
                thread::Builder::new()
                    .name(format!("realistic-deleter-{worker_index}"))
                    .spawn(move || run_deleter(worker_index, worker_context))?,
            );
        }
    }
    if config.read_ops_per_second > 0 {
        for worker_index in 0..config.read_workers {
            let worker_context = Arc::clone(&context);
            workers.push(
                thread::Builder::new()
                    .name(format!("realistic-reader-{worker_index}"))
                    .spawn(move || run_reader(worker_index, worker_context))?,
            );
        }
    }
    {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name("realistic-controller".to_owned())
                .spawn(move || run_controller(worker_context))?,
        );
    }
    if config.lifetime_mode == LifetimeMode::Epoch {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name("realistic-epoch-advancer".to_owned())
                .spawn(move || run_epoch_advancer(worker_context))?,
        );
    }
    if !config.sync_interval.is_zero() {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name("realistic-syncer".to_owned())
                .spawn(move || run_syncer(worker_context))?,
        );
    }

    let started_at = workload_started_at;
    let deadline = started_at
        .checked_add(config.duration)
        .unwrap_or_else(Instant::now);
    while Instant::now() < deadline && !workload_stop.load(Ordering::Acquire) {
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100)),
        );
    }
    workload_stop.store(true, Ordering::Release);
    metrics.client_load_active.set(0);
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("workload worker panicked"))?;
    }
    let workload_elapsed = started_at.elapsed();
    let terminal_at = Instant::now();
    let terminal_epoch = (config.lifetime_mode == LifetimeMode::Epoch)
        .then(|| current_epoch.load(Ordering::Acquire));
    let (terminal_model, terminal_backlog) = {
        let model = model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            model.snapshot(terminal_at),
            model.refresh_delete_backlog(
                terminal_at,
                terminal_epoch,
                config.delete_lag_slo,
                &metrics,
            ),
        )
    };
    metrics
        .overdue_delete_keys
        .set(saturating_i64(terminal_backlog.due_keys));
    metrics
        .oldest_overdue_seconds
        .set(terminal_backlog.oldest_lag.as_secs_f64());
    let terminal_space = sample_space(&config.root_dir, &metrics)?;

    if !config.cleanup_grace.is_zero() {
        println!("phase=cleanup_grace");
        let grace_deadline = Instant::now()
            .checked_add(config.cleanup_grace)
            .unwrap_or_else(Instant::now);
        while Instant::now() < grace_deadline {
            thread::sleep(
                grace_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            );
        }
    }
    let grace_space = sample_space(&config.root_dir, &metrics)?;
    sampler_stop.store(true, Ordering::Release);
    sampler
        .join()
        .map_err(|_| io::Error::other("space sampler panicked"))?;

    print_scorecard(
        &config,
        &metrics,
        &fatal,
        workload_elapsed,
        terminal_model,
        terminal_backlog,
        terminal_space,
        grace_space,
        &registry,
        terminal_epoch,
        &lifetime_offset_counts.snapshot(),
    );
    let profile_keys = if config.relocation_profile_reads > 0 && fatal.get().is_none() {
        prepare_relocation_profile(
            strata_store
                .as_deref()
                .expect("relocation profile requires Strata"),
            &registry,
            config.relocation_profile_reads,
            config.relocation_profile_timeout,
        )?
    } else {
        Vec::new()
    };
    drop(context);
    drop(engine);
    drop(strata_store);
    drop(_blobdb_reporter);
    if config.relocation_profile_reads > 0 && fatal.get().is_none() {
        run_relocation_read_profile(&config, &profile_keys, &payload)?;
    }
    if let Some(error) = fatal.get() {
        return Err(error.into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn print_scorecard(
    config: &Config,
    metrics: &HarnessMetrics,
    fatal: &FatalState,
    workload_elapsed: Duration,
    model: ModelSnapshot,
    backlog: DeleteBacklog,
    terminal_space: SpaceSnapshot,
    grace_space: SpaceSnapshot,
    registry: &Registry,
    current_epoch: Option<Epoch>,
    lifetime_offset_counts: &[u64],
) {
    let elapsed_seconds = workload_elapsed.as_secs_f64().max(f64::EPSILON);
    let puts = metrics
        .operation_successes
        .with_label_values(&["put"])
        .get();
    let deletes = metrics
        .operation_successes
        .with_label_values(&["delete"])
        .get();
    let live_reads = metrics
        .operation_successes
        .with_label_values(&["read_live"])
        .get();
    let deleted_reads = metrics
        .operation_successes
        .with_label_values(&["read_deleted"])
        .get();
    let reads = live_reads.saturating_add(deleted_reads);
    let put_bytes = metrics.put_payload_bytes.get();
    let retired_bytes = metrics.retired_payload_bytes.get();
    let timely = metrics.delete_timely.get();
    let late = metrics.delete_late.get();
    // A due key still inside the lag SLO has not failed its obligation at the cutoff. Outstanding
    // keys count against the score only once their lag has crossed that SLO.
    let delete_obligations = timely
        .saturating_add(late)
        .saturating_add(backlog.matured_overdue_keys);
    let timely_percent = if delete_obligations == 0 {
        100.0
    } else {
        timely as f64 * 100.0 / delete_obligations as f64
    };
    let read_rate = reads as f64 / elapsed_seconds;
    let required_read_rate =
        config.read_ops_per_second as f64 * config.read_attainment_percent / 100.0;
    let read_p99 = metrics.read_latency.quantile(0.99);
    let delete_lag_p99 = metrics.delete_lag_latency.quantile(0.99);
    let correctness_errors = metrics.correctness_errors.get();
    let qualified = fatal.get().is_none()
        && correctness_errors == 0
        && read_rate >= required_read_rate
        && read_p99 <= config.read_p99_slo
        && timely_percent >= config.delete_timely_percent;
    let logical_live_bytes = model
        .live_keys
        .saturating_add(model.deleting_keys)
        .saturating_sub(backlog.due_keys)
        .saturating_mul(config.payload_size as u64);

    println!("phase=scorecard");
    println!("qualified={qualified}");
    println!(
        "fatal_error={}",
        fatal.get().unwrap_or_else(|| "none".to_owned())
    );
    println!("workload_elapsed_seconds={elapsed_seconds:.3}");
    println!("put_ops={puts}");
    println!("put_ops_per_second={:.3}", puts as f64 / elapsed_seconds);
    println!("put_bytes={put_bytes}");
    println!(
        "put_bytes_per_second={:.3}",
        put_bytes as f64 / elapsed_seconds
    );
    println!("delete_ops={deletes}");
    println!("manual_tombstone_ops={deletes}");
    println!("native_expiration_ops={}", metrics.native_expirations.get());
    println!("epoch_advances={}", metrics.epoch_advances.get());
    if let Some(current_epoch) = current_epoch {
        let epoch_advances = metrics.epoch_advances.get();
        println!(
            "lifetime_window_filled={}",
            epoch_advances >= config.future_epochs
        );
        println!(
            "steady_state_epoch_transitions={}",
            epoch_advances.saturating_sub(config.future_epochs)
        );
        println!("current_epoch={current_epoch}");
        let assignment_total = lifetime_offset_counts.iter().copied().sum::<u64>();
        let assignment_min = lifetime_offset_counts.iter().copied().min().unwrap_or(0);
        let assignment_max = lifetime_offset_counts.iter().copied().max().unwrap_or(0);
        println!("lifetime_assignments={assignment_total}");
        println!("lifetime_offset_min_assignments={assignment_min}");
        println!("lifetime_offset_max_assignments={assignment_max}");
        println!(
            "lifetime_offset_assignments={}",
            lifetime_offset_counts
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    println!(
        "delete_ops_per_second={:.3}",
        deletes as f64 / elapsed_seconds
    );
    println!("retired_bytes={retired_bytes}");
    println!(
        "retired_bytes_per_second={:.3}",
        retired_bytes as f64 / elapsed_seconds
    );
    println!("read_ops={reads}");
    println!("read_ops_per_second={read_rate:.3}");
    println!("read_live_ops={live_reads}");
    println!("read_deleted_ops={deleted_reads}");
    println!(
        "put_p50_us={}",
        metrics.put_latency.quantile(0.50).as_micros()
    );
    println!(
        "put_p99_us={}",
        metrics.put_latency.quantile(0.99).as_micros()
    );
    println!(
        "read_p50_us={}",
        metrics.read_latency.quantile(0.50).as_micros()
    );
    println!("read_p99_us={}", read_p99.as_micros());
    println!(
        "delete_p50_us={}",
        metrics.delete_latency.quantile(0.50).as_micros()
    );
    println!(
        "delete_p99_us={}",
        metrics.delete_latency.quantile(0.99).as_micros()
    );
    println!("delete_lag_p99_us={}", delete_lag_p99.as_micros());
    println!("delete_due={}", metrics.delete_due.get());
    println!("delete_timely={timely}");
    println!("delete_late={late}");
    println!("delete_outstanding={}", backlog.due_keys);
    println!("delete_timely_percent={timely_percent:.3}");
    println!("correctness_errors={correctness_errors}");
    println!("live_keys={}", model.live_keys);
    println!("deleting_keys={}", model.deleting_keys);
    println!("deleted_sample_keys={}", model.deleted_sample_keys);
    println!(
        "oldest_live_age_seconds={:.3}",
        model.oldest_live_age.as_secs_f64()
    );
    println!("logical_live_bytes={logical_live_bytes}");
    println!(
        "terminal_directory_allocated_bytes={}",
        terminal_space.allocated_bytes
    );
    println!(
        "terminal_directory_apparent_bytes={}",
        terminal_space.apparent_bytes
    );
    println!(
        "terminal_space_amplification={:.6}",
        terminal_space.amplification(logical_live_bytes)
    );
    println!(
        "terminal_strata_exact_epoch_bytes={}",
        terminal_space.strata_exact_epoch_bytes
    );
    println!(
        "terminal_strata_spillover_bytes={}",
        terminal_space.strata_spillover_bytes
    );
    println!(
        "terminal_strata_epoch_directories={}",
        terminal_space.strata_epoch_directories
    );
    println!(
        "post_grace_directory_allocated_bytes={}",
        grace_space.allocated_bytes
    );
    println!(
        "post_grace_directory_apparent_bytes={}",
        grace_space.apparent_bytes
    );
    println!(
        "post_grace_space_amplification={:.6}",
        grace_space.amplification(logical_live_bytes)
    );
    println!(
        "post_grace_strata_exact_epoch_bytes={}",
        grace_space.strata_exact_epoch_bytes
    );
    println!(
        "post_grace_strata_spillover_bytes={}",
        grace_space.strata_spillover_bytes
    );
    println!(
        "post_grace_strata_epoch_directories={}",
        grace_space.strata_epoch_directories
    );
    println!(
        "strata_gc_output_bytes={:.0}",
        counter_metric(registry, "strata_store_gc_output_bytes_total")
    );
    println!(
        "strata_gc_source_deleted_bytes={:.0}",
        counter_metric(registry, "strata_store_gc_source_deleted_bytes_total")
    );
    println!(
        "strata_gc_reclaimed_bytes={:.0}",
        counter_metric(registry, "strata_store_gc_reclaimed_bytes_total")
    );
    println!(
        "strata_segment_file_bytes_read={:.0}",
        counter_metric(registry, "strata_store_segment_file_bytes_read_total")
    );
    println!(
        "strata_segment_file_bytes_written={:.0}",
        counter_metric(registry, "strata_store_segment_file_bytes_written_total")
    );
    println!(
        "blobdb_blob_file_bytes_read={}",
        metrics.blob_file_bytes_read.get()
    );
    println!(
        "blobdb_blob_file_bytes_written={}",
        metrics.blob_file_bytes_written.get()
    );
    println!(
        "blobdb_gc_bytes_relocated={}",
        metrics.blob_gc_bytes_relocated.get()
    );
    println!(
        "post_grace_filesystem_available_bytes={}",
        grace_space.filesystem_available_bytes
    );
}

fn prepare_relocation_profile(
    store: &StrataStore,
    registry: &Registry,
    sample_size: usize,
    timeout: Duration,
) -> Result<Vec<BlobKey>, Box<dyn std::error::Error>> {
    println!("phase=relocation_profile_setup");
    let deadline = Instant::now() + timeout;
    store.sync()?;
    store.sync()?;
    thread::sleep(Duration::from_millis(2_500));

    // Let the normal one-second main-LSM roll/flush/compact loop drain user patches before GC.
    // Relocation publication itself does not add a user-key patch, so moved keys remain stale until
    // the explicit healing phase below.
    loop {
        let sealed = store
            .index()
            .iter_segment_states()?
            .iter()
            .any(|(_, state)| state.state == SegmentFileState::Sealed);
        let patches = store
            .index()
            .get_lsm_manifest("blob")?
            .and_then(|manifest| {
                manifest
                    .partitions
                    .get(&0)
                    .map(|partition| partition.patches.len())
            })
            .unwrap_or_default();
        if patches == 0 {
            if !sealed {
                println!("relocation_profile_status=no_naturally_sealed_segments");
                return Ok(Vec::new());
            }
            break;
        }
        if Instant::now() >= deadline {
            return Err("timed out draining the main LSM before relocation profiling".into());
        }
        thread::sleep(Duration::from_millis(100));
    }

    let main_input_before =
        counter_metric(registry, "strata_store_main_compaction_input_bytes_total");
    let main_output_before =
        counter_metric(registry, "strata_store_main_compaction_output_bytes_total");
    let main_seconds_before =
        histogram_sum(registry, "strata_store_main_compaction_duration_seconds");
    let main_compactions_before =
        histogram_count(registry, "strata_store_main_compaction_duration_seconds");
    let relocation_input_before = counter_metric(
        registry,
        "strata_store_relocation_compaction_input_bytes_total",
    );
    let relocation_output_before = counter_metric(
        registry,
        "strata_store_relocation_compaction_output_bytes_total",
    );
    let relocation_seconds_before = histogram_sum(
        registry,
        "strata_store_relocation_compaction_duration_seconds",
    );
    let relocation_compactions_before = histogram_count(
        registry,
        "strata_store_relocation_compaction_duration_seconds",
    );

    let mut moved = Vec::new();
    let mut seen = HashSet::new();
    let mut move_batches = 0_u64;
    let mut flushed_batches = 0_u64;
    let mut empty_attempts = 0_u32;
    while Instant::now() < deadline && (moved.len() < sample_size || flushed_batches < 8) {
        match store.run_gc_once()? {
            Some(result) if !result.published_records.is_empty() => {
                move_batches += 1;
                empty_attempts = 0;
                for record in result.published_records {
                    if seen.insert(record.source.key.as_bytes().to_vec()) {
                        moved.push((record.source.key, record.source.from.segment_id));
                    }
                }
                store.sync()?;
                thread::sleep(Duration::from_millis(1_100));
                if store.flush_relocation_memtable_if_due()? {
                    flushed_batches += 1;
                }
            }
            Some(_) => {
                empty_attempts = 0;
                thread::sleep(Duration::from_millis(100));
            }
            None => {
                empty_attempts += 1;
                thread::sleep(Duration::from_millis(200));
                if empty_attempts >= 10 {
                    break;
                }
            }
        }
    }

    let sampled_sources = moved
        .iter()
        .take(sample_size)
        .map(|(_, source)| *source)
        .collect::<HashSet<_>>();
    let delete_only = GcPlanner::new(GcPlannerConfig {
        max_copy_bytes_per_plan: 0,
        max_l0_copy_bytes_per_plan: 0,
        min_l0_rewrite_epoch_distance: u64::MAX,
        min_l0_rewrite_useful_ratio_bps: 10_000,
        min_reclaim_bytes: u64::MAX,
        min_garbage_ratio_bps: 10_000,
        min_exact_epoch_bucket_bytes: u64::MAX,
        min_exact_epoch_distance: u64::MAX,
        max_exact_epoch_extension_count: 0,
        min_join_output_bytes: u64::MAX,
        max_join_sources: 2,
    });
    while Instant::now() < deadline {
        let deleted = store
            .index()
            .iter_segment_states()?
            .into_iter()
            .filter_map(|(segment_id, state)| {
                (state.state == SegmentFileState::Deleted).then_some(segment_id)
            })
            .collect::<HashSet<_>>();
        if sampled_sources
            .iter()
            .all(|source| deleted.contains(source))
        {
            break;
        }
        match store.prepare_gc_plan(&delete_only)? {
            Some(plan) => {
                let copied = store.copy_prepared_gc_plan(plan)?;
                store.publish_prepared_gc_copy(copied)?;
                store.sync()?;
            }
            None => thread::sleep(Duration::from_millis(100)),
        }
    }
    store.sync()?;

    let deleted_sources = store
        .index()
        .iter_segment_states()?
        .into_iter()
        .filter_map(|(segment_id, state)| {
            (state.state == SegmentFileState::Deleted).then_some(segment_id)
        })
        .collect::<HashSet<SegmentId>>();
    let mut keys = moved
        .into_iter()
        .filter_map(|(key, source)| deleted_sources.contains(&source).then_some(key))
        .take(sample_size)
        .collect::<Vec<_>>();
    keys.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

    let main_input = counter_metric(registry, "strata_store_main_compaction_input_bytes_total")
        - main_input_before;
    let main_output = counter_metric(registry, "strata_store_main_compaction_output_bytes_total")
        - main_output_before;
    let main_seconds = histogram_sum(registry, "strata_store_main_compaction_duration_seconds")
        - main_seconds_before;
    let main_compactions =
        histogram_count(registry, "strata_store_main_compaction_duration_seconds")
            .saturating_sub(main_compactions_before);
    let relocation_input = counter_metric(
        registry,
        "strata_store_relocation_compaction_input_bytes_total",
    ) - relocation_input_before;
    let relocation_output = counter_metric(
        registry,
        "strata_store_relocation_compaction_output_bytes_total",
    ) - relocation_output_before;
    let relocation_seconds = histogram_sum(
        registry,
        "strata_store_relocation_compaction_duration_seconds",
    ) - relocation_seconds_before;
    let relocation_compactions = histogram_count(
        registry,
        "strata_store_relocation_compaction_duration_seconds",
    )
    .saturating_sub(relocation_compactions_before);

    println!("relocation_gc_move_batches={move_batches}");
    println!("relocation_sst_flush_batches={flushed_batches}");
    println!("relocation_stale_read_keys={}", keys.len());
    print_compaction_profile(
        "main",
        main_input,
        main_output,
        main_seconds,
        main_compactions,
    );
    print_compaction_profile(
        "relocation",
        relocation_input,
        relocation_output,
        relocation_seconds,
        relocation_compactions,
    );
    Ok(keys)
}

fn run_relocation_read_profile(
    config: &Config,
    keys: &[BlobKey],
    expected_payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("phase=relocation_read_profile");
    if keys.is_empty() {
        println!("relocation_read_profile_status=no_deleted_source_references");
        return Ok(());
    }

    let evicted_files = evict_file_pages(&config.store_config().namespace_dir())?;
    println!(
        "relocation_cold_cache_eviction_supported={}",
        evicted_files.is_some()
    );
    println!(
        "relocation_cold_cache_evicted_files={}",
        evicted_files.unwrap_or_default()
    );

    let registry = Registry::new();
    let metrics = StrataStoreMetrics::new(&registry, "relocation-profile")?;
    let store = StrataStore::open(config.store_config(), metrics)?;

    read_relocation_pass("cold", &store, &registry, keys, expected_payload)?;
    store.clear_relocation_cache();
    read_relocation_pass("block_warm", &store, &registry, keys, expected_payload)?;
    read_relocation_pass("cache_warm", &store, &registry, keys, expected_payload)?;

    let healed_before = counter_metric(
        &registry,
        "strata_store_main_compaction_healed_references_total",
    );
    let input_before = counter_metric(&registry, "strata_store_main_compaction_input_bytes_total");
    let output_before =
        counter_metric(&registry, "strata_store_main_compaction_output_bytes_total");
    let seconds_before = histogram_sum(&registry, "strata_store_main_compaction_duration_seconds");
    let compactions_before =
        histogram_count(&registry, "strata_store_main_compaction_duration_seconds");
    let logical_end_epoch = store.current_epoch()?.saturating_add(1_000_000);
    for key in keys {
        store.set_blob_lifetime(key, logical_end_epoch)?;
    }
    store.sync()?;

    let deadline = Instant::now() + config.relocation_profile_timeout;
    loop {
        let healed = counter_metric(
            &registry,
            "strata_store_main_compaction_healed_references_total",
        ) - healed_before;
        let patches = store
            .index()
            .get_lsm_manifest("blob")?
            .and_then(|manifest| {
                manifest
                    .partitions
                    .get(&0)
                    .map(|partition| partition.patches.len())
            })
            .unwrap_or_default();
        if healed >= keys.len() as f64 && patches == 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for main-LSM relocation healing".into());
        }
        thread::sleep(Duration::from_millis(100));
    }

    let input =
        counter_metric(&registry, "strata_store_main_compaction_input_bytes_total") - input_before;
    let output = counter_metric(&registry, "strata_store_main_compaction_output_bytes_total")
        - output_before;
    let seconds =
        histogram_sum(&registry, "strata_store_main_compaction_duration_seconds") - seconds_before;
    let compactions = histogram_count(&registry, "strata_store_main_compaction_duration_seconds")
        .saturating_sub(compactions_before);
    print_compaction_profile("main_healing", input, output, seconds, compactions);
    println!(
        "main_healed_references={:.0}",
        counter_metric(
            &registry,
            "strata_store_main_compaction_healed_references_total"
        ) - healed_before
    );
    read_relocation_pass("healed", &store, &registry, keys, expected_payload)?;
    Ok(())
}

fn read_relocation_pass(
    name: &str,
    store: &StrataStore,
    registry: &Registry,
    keys: &[BlobKey],
    expected_payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let hit_before = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "hit",
    );
    let miss_before = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "miss",
    );
    let error_before = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "error",
    );
    let cache_hit_before = counter_metric_with_label(
        registry,
        "strata_store_relocation_cache_requests_total",
        "result",
        "hit",
    );
    let cache_miss_before = counter_metric_with_label(
        registry,
        "strata_store_relocation_cache_requests_total",
        "result",
        "miss",
    );
    let main_cache_before = store.main_lsm_block_cache_stats()?;
    let relocation_block_cache_before = store.relocation_lsm_block_cache_stats();
    let latency = AtomicLatency::default();
    let started = Instant::now();
    for key in keys {
        let read_started = Instant::now();
        let value = store.get(key)?;
        latency.record(read_started.elapsed());
        if value.as_deref() != Some(expected_payload) {
            return Err(format!(
                "{name} relocation read returned incorrect bytes for {:?}",
                key.as_bytes()
            )
            .into());
        }
    }
    let elapsed = started.elapsed();
    let hits = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "hit",
    ) - hit_before;
    let misses = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "miss",
    ) - miss_before;
    let errors = counter_metric_with_label(
        registry,
        "strata_store_relocation_lookups_total",
        "result",
        "error",
    ) - error_before;
    let cache_hits = counter_metric_with_label(
        registry,
        "strata_store_relocation_cache_requests_total",
        "result",
        "hit",
    ) - cache_hit_before;
    let cache_misses = counter_metric_with_label(
        registry,
        "strata_store_relocation_cache_requests_total",
        "result",
        "miss",
    ) - cache_miss_before;
    let main_cache = store.main_lsm_block_cache_stats()?;
    let relocation_block_cache = store.relocation_lsm_block_cache_stats();
    println!("relocation_{name}_read_ops={}", keys.len());
    println!(
        "relocation_{name}_read_ops_per_second={:.3}",
        keys.len() as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    println!(
        "relocation_{name}_read_p50_us={}",
        latency.quantile(0.50).as_micros()
    );
    println!(
        "relocation_{name}_read_p99_us={}",
        latency.quantile(0.99).as_micros()
    );
    println!("relocation_{name}_lookup_hits={hits:.0}");
    println!("relocation_{name}_lookup_misses={misses:.0}");
    println!("relocation_{name}_lookup_errors={errors:.0}");
    println!("relocation_{name}_cache_hits={cache_hits:.0}");
    println!("relocation_{name}_cache_misses={cache_misses:.0}");
    println!(
        "relocation_{name}_main_block_cache_hits={}",
        main_cache.hits.saturating_sub(main_cache_before.hits)
    );
    println!(
        "relocation_{name}_main_block_cache_misses={}",
        main_cache.misses.saturating_sub(main_cache_before.misses)
    );
    println!(
        "relocation_{name}_relo_block_cache_hits={}",
        relocation_block_cache
            .hits
            .saturating_sub(relocation_block_cache_before.hits)
    );
    println!(
        "relocation_{name}_relo_block_cache_misses={}",
        relocation_block_cache
            .misses
            .saturating_sub(relocation_block_cache_before.misses)
    );
    let expected = keys.len() as f64;
    let path_verified = match name {
        "cold" | "block_warm" => {
            hits == expected
                && misses == 0.0
                && errors == 0.0
                && cache_hits == 0.0
                && cache_misses == expected
        }
        "cache_warm" => {
            hits + misses + errors == 0.0 && cache_hits == expected && cache_misses == 0.0
        }
        "healed" => hits + misses + errors == 0.0 && cache_hits + cache_misses == 0.0,
        _ => false,
    };
    println!("relocation_{name}_path_verified={path_verified}");
    if !path_verified {
        return Err(format!("{name} pass used an unexpected relocation path").into());
    }
    Ok(())
}

fn print_compaction_profile(
    name: &str,
    input_bytes: f64,
    output_bytes: f64,
    elapsed_seconds: f64,
    compactions: u64,
) {
    println!("{name}_compactions={compactions}");
    println!("{name}_compaction_input_bytes={input_bytes:.0}");
    println!("{name}_compaction_output_bytes={output_bytes:.0}");
    println!("{name}_compaction_seconds={elapsed_seconds:.6}");
    println!(
        "{name}_compaction_input_bytes_per_second={:.3}",
        input_bytes / elapsed_seconds.max(f64::EPSILON)
    );
    println!(
        "{name}_compaction_output_input_ratio={:.6}",
        if input_bytes == 0.0 {
            0.0
        } else {
            output_bytes / input_bytes
        }
    );
}

fn counter_metric(registry: &Registry, name: &str) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_counter().value())
        })
        .unwrap_or_default()
}

fn counter_metric_with_label(
    registry: &Registry,
    name: &str,
    label_name: &str,
    label_value: &str,
) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family.get_metric().iter().find_map(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == label_name && label.value() == label_value)
                    .then(|| metric.get_counter().value())
            })
        })
        .unwrap_or_default()
}

fn histogram_sum(registry: &Registry, name: &str) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_histogram().sample_sum())
        })
        .unwrap_or_default()
}

fn histogram_count(registry: &Registry, name: &str) -> u64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == name)
        .and_then(|family| {
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_histogram().sample_count())
        })
        .unwrap_or_default()
}

fn evict_file_pages(root: &Path) -> io::Result<Option<u64>> {
    #[cfg(target_os = "linux")]
    {
        let mut pending = vec![root.to_owned()];
        let mut evicted = 0_u64;
        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    pending.push(entry.path());
                } else if file_type.is_file() {
                    let file = fs::File::open(entry.path())?;
                    let result = unsafe {
                        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                    };
                    if result != 0 {
                        return Err(io::Error::from_raw_os_error(result));
                    }
                    evicted += 1;
                }
            }
        }
        Ok(Some(evicted))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        Ok(None)
    }
}

fn next_value(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_u64(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("invalid integer '{value}': {error}"))
}

fn parse_nonzero_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid integer '{value}': {error}"))?;
    if parsed == 0 {
        return Err(format!("value must be non-zero, got '{value}'"));
    }
    Ok(parsed)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("expected true or false, got '{value}'")),
    }
}

fn parse_percent(value: &str) -> Result<f64, String> {
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
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let number = value[..split_at]
        .parse::<usize>()
        .map_err(|error| format!("invalid size '{value}': {error}"))?;
    let multiplier = match value[split_at..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        suffix => return Err(format!("unsupported size suffix '{suffix}' in '{value}'")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size '{value}' overflows usize"))
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let split_at = value
        .find(|character: char| !(character.is_ascii_digit() || character == '.'))
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
        "h" | "hr" | "hrs" => 3_600.0,
        "d" | "day" | "days" => 86_400.0,
        suffix => {
            return Err(format!(
                "unsupported duration suffix '{suffix}' in '{value}'"
            ));
        }
    };
    Duration::try_from_secs_f64(amount * multiplier)
        .map_err(|error| format!("invalid duration '{value}': {error}"))
}

fn usage() -> &'static str {
    "usage: cargo run --release -p realistic-bench -- [options]

required:
  --engine <strata|blobdb>
  --root <fresh-path>

workload:
  --duration <duration>                  write/read/delete phase; default 30m
  --lifetime-mode <retention|epoch>      default retention
  --retention <duration>                 retention-mode age before tombstone; default 5m
  --epoch-duration <duration>            epoch-mode cadence; default 5m
  --future-epochs <count>                epoch-mode uniform future lifetime window; default 52
  --min-future-epoch-offset <count>      lower bound for the future lifetime window; default 1
  --lifetime-seed <u64>                  deterministic lifetime distribution seed
  --hashed-keys <true|false>             spread keys uniformly over the key space instead of in id order; default false
  --cleanup-grace <duration>             no-traffic reclamation window; default 5m
  --payload-size <bytes|KiB|MiB|GiB>     default 1MiB
  --put-ops-per-second <count>           global put-rate target; 0 keeps AIMD control (default)
  --initial-write-workers <count>        default 1
  --min-write-workers <count>            default 1
  --max-write-workers <count>            default 64
  --read-workers <count>                 default 4
  --read-ops-per-second <count>          aggregate read obligation; 0 disables reads; default 1000
  --read-deleted-percent <0..100>        negative-read share; default 50
  --read-p99-slo <duration>              default 100ms
  --read-attainment-percent <0..100>     required target-rate attainment; default 95
  --delete-workers <count>               default 4
  --delete-lag-slo <duration>            default 30s
  --delete-timely-percent <0..100>       required timely deletes; default 99
  --control-interval <duration>          AIMD window; default 5s
  --controller-debounce-windows <count>  consecutive equal-health windows required per writer change; default 3
  --writer-increase-percent <count>      healthy-window additive step; default 25
  --writer-decrease-percent <count>      unhealthy-window multiplicative cut; default 25
  --sync-interval <duration>             explicit sync cadence; 0 (default) disables
  --deleted-sample-capacity <count>      keys retained for negative reads; default 1000000
  --space-sample-interval <duration>     directory scan cadence; default 10s
  --metrics-listen <addr|off>            Prometheus /metrics endpoint

strata:
  --namespace <name>
  --queue-capacity <count>
  --max-unsealed-segments <count>
  --segment-max-bytes <size>
  --strata-gc <true|false>
  --strata-gc-min-epoch-copy-distance <epochs>
                                           minimum distance considered far for L0 usefulness and exact routing
  --strata-gc-workers <count>            GC workers started and admitted initially; default 1
  --strata-lsm-partitions <count>        hash partitions for the main and relocation LSMs; default 1
  --strata-memtable-max-age <time>       oldest an LSM memtable grows before it flushes; default 1s
  --strata-relocation-writeback <count|off>
                                           evaluation mode: push GC relocations through the foreground writer in chunks of <count>; default off
  --relocation-profile-reads <count>    post-workload HDD relocation profile; disables background GC for deterministic setup
  --relocation-profile-timeout <time>   setup/healing deadline; default 10m

blobdb:
  --rocksdb-min-blob-size <size>
  --rocksdb-blob-file-size <size>
  --rocksdb-write-buffer-size <size>
  --rocksdb-db-write-buffer-size <size>
  --rocksdb-max-write-buffer-number <count>
  --rocksdb-max-background-flushes <count>
  --rocksdb-max-subcompactions <count>  default 1
  --rocksdb-high-pri-threads <count>
  --rocksdb-low-pri-threads <count>
  --rocksdb-blob-gc <true|false>
  --rocksdb-blob-gc-age-cutoff <0..1>
  --rocksdb-blob-gc-force-threshold <0..1>"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_storage_files_for_size_breakdown() {
        assert_eq!(
            storage_file_type(Path::new("realistic/ingest/1.data")),
            Some(0)
        );
        assert_eq!(
            storage_file_type(Path::new("realistic/lsm/tables/patch-1.sst")),
            Some(1)
        );
        assert_eq!(
            storage_file_type(Path::new("realistic/relocations/tables/base-1.sst")),
            Some(1)
        );
        assert_eq!(
            storage_file_type(Path::new("realistic/index/000001.sst")),
            Some(2)
        );
        assert_eq!(
            storage_file_type(Path::new("rocksdb-blobdb/000002.blob")),
            Some(3)
        );
        assert_eq!(storage_file_type(Path::new("realistic/wal/1.log")), None);
    }

    #[test]
    fn parses_minimal_configuration() {
        let config = Config::parse(
            ["--engine", "blobdb", "--root", "/tmp/realistic"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("configuration should parse");

        assert_eq!(config.engine, EngineKind::BlobDb);
        assert_eq!(config.lifetime_mode, LifetimeMode::Retention);
        assert_eq!(config.payload_size, 1 << 20);
        assert!(config.sync_interval.is_zero());
        assert_eq!(config.controller_debounce_windows, 3);
        assert_eq!(config.rocksdb_max_subcompactions, 1);
    }

    /// Jitter is reproducible per key, bounded by the configured half-width, and spread across
    /// it rather than clustered; zero jitter leaves the retention untouched.
    #[test]
    fn retention_jitter_is_bounded_reproducible_and_spread() {
        let mut config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/x",
                "--retention",
                "100s",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("configuration should parse");
        assert_eq!(jittered_retention(&config, 7), Duration::from_secs(100));
        config.retention_jitter_percent = 50;
        let first = jittered_retention(&config, 7);
        assert_eq!(first, jittered_retention(&config, 7));
        let samples = (0..1000u64)
            .map(|id| jittered_retention(&config, id).as_secs_f64())
            .collect::<Vec<_>>();
        assert!(samples.iter().all(|s| (50.0..=150.0).contains(s)));
        let below = samples.iter().filter(|s| **s < 75.0).count();
        let above = samples.iter().filter(|s| **s > 125.0).count();
        assert!(
            below > 150 && above > 150,
            "jitter clustered: {below} below, {above} above"
        );
    }

    #[test]
    fn parses_epoch_lifetime_configuration() {
        let config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/realistic",
                "--lifetime-mode",
                "epoch",
                "--epoch-duration",
                "5m",
                "--future-epochs",
                "52",
                "--min-future-epoch-offset",
                "20",
                "--lifetime-seed",
                "7",
                "--strata-gc-min-epoch-copy-distance",
                "6",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("epoch lifetime configuration should parse");

        assert_eq!(config.lifetime_mode, LifetimeMode::Epoch);
        assert_eq!(config.epoch_duration, Duration::from_secs(5 * 60));
        assert_eq!(config.future_epochs, 52);
        assert_eq!(config.min_future_epoch_offset, 20);
        assert_eq!(config.lifetime_seed, 7);
        assert_eq!(config.strata_gc_min_epoch_copy_distance, Some(6));
        let planner = config.store_config().gc_planner_config;
        assert_eq!(planner.min_l0_rewrite_epoch_distance, 6);
        assert_eq!(planner.min_l0_rewrite_useful_ratio_bps, 6_600);
        assert_eq!(planner.min_exact_epoch_distance, 6);
    }

    #[test]
    fn lifetime_assignment_is_deterministic_and_covers_future_window() {
        let config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/realistic",
                "--lifetime-mode",
                "epoch",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("epoch lifetime configuration should parse");
        let now = Instant::now();
        let clock = EpochClock {
            current_epoch: 10,
            next_transition_at: now,
        };
        let mut counts = vec![0_u64; config.future_epochs as usize];
        for key_id in 0..10_000 {
            let assignment = clock.assignment(key_id, &config);
            assert!((1..=config.future_epochs).contains(&assignment.offset));
            assert_eq!(
                assignment.logical_end_epoch,
                clock.current_epoch + assignment.offset
            );
            assert_eq!(assignment, clock.assignment(key_id, &config));
            counts[assignment.offset as usize - 1] += 1;
        }
        assert!(counts.into_iter().all(|count| count > 0));
    }

    #[test]
    fn lifetime_assignment_can_use_a_bounded_future_window() {
        let config = Config::parse(
            [
                "--engine",
                "blobdb",
                "--root",
                "/tmp/realistic",
                "--lifetime-mode",
                "epoch",
                "--epoch-duration",
                "216s",
                "--future-epochs",
                "50",
                "--min-future-epoch-offset",
                "20",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("fixed epoch lifetime configuration should parse");
        let now = Instant::now();
        let clock = EpochClock {
            current_epoch: 10,
            next_transition_at: now,
        };

        let mut counts = [0_u64; 31];
        for key_id in 0..10_000 {
            let assignment = clock.assignment(key_id, &config);
            assert!((20..=50).contains(&assignment.offset));
            assert_eq!(
                assignment.logical_end_epoch,
                clock.current_epoch + assignment.offset
            );
            assert_eq!(
                assignment.due_at,
                now + Duration::from_secs((assignment.offset - 1) * 216)
            );
            counts[(assignment.offset - 20) as usize] += 1;
        }
        assert!(counts.into_iter().all(|count| count > 0));
    }

    #[test]
    fn minimum_future_offset_must_fit_the_future_window() {
        let error = Config::parse(
            [
                "--engine",
                "blobdb",
                "--root",
                "/tmp/realistic",
                "--lifetime-mode",
                "epoch",
                "--future-epochs",
                "50",
                "--min-future-epoch-offset",
                "51",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect_err("minimum offset outside the configured window should fail");

        assert!(error.contains("must not exceed --future-epochs"));
    }

    #[test]
    fn rocksdb_max_subcompactions_parses() {
        let config = Config::parse(
            [
                "--engine",
                "blobdb",
                "--root",
                "/tmp/realistic",
                "--rocksdb-max-subcompactions",
                "4",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("max subcompactions should parse");

        assert_eq!(config.rocksdb_max_subcompactions, 4);
    }

    #[test]
    fn controller_debounce_requires_consecutive_equal_health_windows() {
        let mut debounce = ControllerDebounce::default();

        assert!(!debounce.observe(true, 3));
        assert!(!debounce.observe(true, 3));
        assert!(!debounce.observe(false, 3));
        assert_eq!(debounce.streak_windows, 1);
        assert!(!debounce.observe(false, 3));
        assert!(debounce.observe(false, 3));

        debounce.reset_streak();
        assert!(!debounce.observe(false, 3));
        assert_eq!(debounce.streak_windows, 1);
    }

    #[test]
    fn zero_read_rate_disables_the_read_obligation() {
        let config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/realistic",
                "--read-ops-per-second",
                "0",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("zero read rate should parse");

        assert_eq!(config.read_ops_per_second, 0);
    }

    #[test]
    fn fixed_put_rate_parses() {
        let config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/realistic",
                "--put-ops-per-second",
                "2000",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("fixed put rate should parse");

        assert_eq!(config.put_ops_per_second, 2_000);
    }

    #[test]
    fn relocation_profile_uses_synchronous_gc() {
        let config = Config::parse(
            [
                "--engine",
                "strata",
                "--root",
                "/tmp/realistic",
                "--relocation-profile-reads",
                "32",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("relocation profile should parse");

        assert_eq!(config.relocation_profile_reads, 32);
        assert!(!config.store_config().gc_workers_enabled);
        assert!(
            Config::parse(
                [
                    "--engine",
                    "blobdb",
                    "--root",
                    "/tmp/realistic",
                    "--relocation-profile-reads",
                    "32",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
    }

    #[test]
    fn aimd_increases_and_decreases_writer_count() {
        assert_eq!(next_writer_count(4, true, 1, 64, 25, 25), 5);
        assert_eq!(next_writer_count(5, true, 1, 64, 25, 25), 7);
        assert_eq!(next_writer_count(8, false, 1, 64, 25, 25), 6);
        assert_eq!(next_writer_count(1, false, 1, 64, 25, 25), 1);
        assert_eq!(next_writer_count(64, true, 1, 64, 25, 25), 64);
    }

    #[test]
    fn model_claims_only_keys_past_retention() {
        let registry = Registry::new();
        let metrics =
            HarnessMetrics::new(&registry, EngineKind::Strata).expect("metrics should register");
        let written_at = Instant::now();
        let mut model = Model::new(10);
        let record = Arc::new(KeyRecord::with_retention(
            make_key(7, false).expect("key should be valid"),
            written_at,
            Duration::from_secs(10),
        ));
        model.push_live(Arc::clone(&record));

        assert!(
            model
                .claim_due(written_at + Duration::from_secs(9), None, &metrics)
                .is_none()
        );
        let claimed = model
            .claim_due(written_at + Duration::from_secs(10), None, &metrics)
            .expect("key should become due");
        assert!(Arc::ptr_eq(&record, &claimed));
        assert_eq!(metrics.delete_due.get(), 1);
        model.finish_delete(claimed);
        assert_eq!(model.deleted.len(), 1);
    }

    #[test]
    fn model_orders_out_of_order_put_acknowledgments() {
        let registry = Registry::new();
        let metrics =
            HarnessMetrics::new(&registry, EngineKind::Strata).expect("metrics should register");
        let written_at = Instant::now();
        let mut model = Model::new(10);
        for (key, retention) in [(1, 10), (3, 30), (2, 20)] {
            model.push_live(Arc::new(KeyRecord::with_retention(
                make_key(key, false).expect("key should be valid"),
                written_at,
                Duration::from_secs(retention),
            )));
        }

        assert_eq!(
            model
                .claim_due(written_at + Duration::from_secs(10), None, &metrics)
                .expect("first key should be due")
                .key,
            make_key(1, false).expect("key should be valid")
        );
        assert!(
            model
                .claim_due(written_at + Duration::from_secs(19), None, &metrics)
                .is_none()
        );
        assert_eq!(
            model
                .claim_due(written_at + Duration::from_secs(20), None, &metrics)
                .expect("second key should be due")
                .key,
            make_key(2, false).expect("key should be valid")
        );
        assert_eq!(model.live.len(), 1);
        assert_eq!(model.live_sample.len(), 1);
        assert_eq!(model.live_sample[0].sample_index.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn native_epoch_expiry_retires_only_reached_epochs() {
        let registry = Registry::new();
        let metrics =
            HarnessMetrics::new(&registry, EngineKind::Strata).expect("metrics should register");
        let written_at = Instant::now();
        let mut model = Model::new(10);
        for (key_id, end_epoch, due_after) in [(1, 2, 10), (2, 3, 20)] {
            model.push_live(Arc::new(KeyRecord::with_epoch(
                make_key(key_id, false).expect("key should be valid"),
                written_at,
                written_at + Duration::from_secs(due_after),
                end_epoch,
            )));
        }

        let expired = model.detach_native_expirations(2);
        for record in &expired {
            record.mark_due(&metrics);
            record.state.store(KEY_DELETING, Ordering::Release);
        }
        assert_eq!(expired.len(), 1);
        assert_eq!(
            expired
                .first()
                .expect("one record should expire")
                .state
                .load(Ordering::Acquire),
            KEY_DELETING
        );
        let expired = expired.into_iter().collect::<Vec<_>>();
        assert_eq!(model.live_sample.len(), 2);
        assert_eq!(model.finish_native_expirations(&expired), 1);
        assert_eq!(model.live.len(), 1);
        assert_eq!(model.live_sample.len(), 1);
        assert_eq!(model.deleted.len(), 1);
        assert_eq!(metrics.delete_due.get(), 1);
    }

    #[test]
    fn parses_sizes_and_durations() {
        assert_eq!(parse_size("512MiB").expect("size should parse"), 512 << 20);
        assert_eq!(
            parse_duration("1.5m").expect("duration should parse"),
            Duration::from_secs(90)
        );
        assert!(parse_duration("-1s").is_err());
    }
}
