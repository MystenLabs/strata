//! Standalone lifecycle benchmark for comparing Strata with RocksDB BlobDB.
//!
//! Unlike the focused `strata-bench` cases, this harness keeps writes, age-based deletes, reads,
//! reclamation, and storage pressure active at the same time. Read and delete service levels are
//! obligations; an AIMD controller greedily raises write concurrency while those obligations hold.

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::{unix::ffi::OsStrExt, unix::fs::MetadataExt};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};
use rocksdb::{DB, Env, statistics::Ticker};
use serde::{Deserialize, Serialize};
use serde_with::{Bytes, serde_as};
use strata_core::{BlobKey, Epoch, SegmentFileState, SegmentId};
use strata_store::{
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_INTERVAL, DEFAULT_GC_IO_BYTES_PER_SEC,
    DEFAULT_GC_MIN_IO_BYTES_PER_SEC, DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
    DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT, DEFAULT_SEAL_WORKER_COUNT,
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
const DEFAULT_SPACE_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_DELETED_SAMPLE_CAPACITY: usize = 1_000_000;
const DEFAULT_STARTING_EPOCH: Epoch = 1;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_UNSEALED_SEGMENTS: usize = 8;
const DEFAULT_READER_CACHE_CAPACITY: usize = strata_store::DEFAULT_SEGMENT_READER_CACHE_CAPACITY;
const DEFAULT_ROCKSDB_MIN_BLOB_SIZE: u64 = 1;
const DEFAULT_ROCKSDB_BLOB_FILE_SIZE: u64 = 1 << 28;
const DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE: usize = 512 << 20;
const DEFAULT_ROCKSDB_DB_WRITE_BUFFER_SIZE: usize = 1 << 30;
const DEFAULT_ROCKSDB_MAX_WRITE_BUFFER_NUMBER: usize = 2;
const DEFAULT_ROCKSDB_HIGH_PRI_THREADS: usize = 4;
const DEFAULT_ROCKSDB_LOW_PRI_THREADS: usize = 1;
const DEFAULT_ROCKSDB_BLOB_GC_AGE_CUTOFF: f64 = 0.25;
const DEFAULT_ROCKSDB_BLOB_GC_FORCE_THRESHOLD: f64 = 1.0;
const DEFAULT_RELOCATION_PROFILE_READS: usize = 0;
const DEFAULT_RELOCATION_PROFILE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const ROCKSDB_CF_CLASS: &str = "rocksdb_blobdb";
const KEY_PREFIX: &[u8] = b"realistic-key-";

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
    retention: Duration,
    cleanup_grace: Duration,
    payload_size: usize,
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
    seal_workers: usize,
    strata_gc: bool,
    relocation_profile_reads: usize,
    relocation_profile_timeout: Duration,
    rocksdb_min_blob_size: u64,
    rocksdb_blob_file_size: u64,
    rocksdb_write_buffer_size: usize,
    rocksdb_db_write_buffer_size: usize,
    rocksdb_max_write_buffer_number: usize,
    rocksdb_max_background_flushes: Option<usize>,
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
            retention: DEFAULT_RETENTION,
            cleanup_grace: DEFAULT_CLEANUP_GRACE,
            payload_size: DEFAULT_PAYLOAD_SIZE,
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
            seal_workers: DEFAULT_SEAL_WORKER_COUNT,
            strata_gc: true,
            relocation_profile_reads: DEFAULT_RELOCATION_PROFILE_READS,
            relocation_profile_timeout: DEFAULT_RELOCATION_PROFILE_TIMEOUT,
            rocksdb_min_blob_size: DEFAULT_ROCKSDB_MIN_BLOB_SIZE,
            rocksdb_blob_file_size: DEFAULT_ROCKSDB_BLOB_FILE_SIZE,
            rocksdb_write_buffer_size: DEFAULT_ROCKSDB_WRITE_BUFFER_SIZE,
            rocksdb_db_write_buffer_size: DEFAULT_ROCKSDB_DB_WRITE_BUFFER_SIZE,
            rocksdb_max_write_buffer_number: DEFAULT_ROCKSDB_MAX_WRITE_BUFFER_NUMBER,
            rocksdb_max_background_flushes: None,
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
                "--retention" => config.retention = parse_duration(&next_value(&mut args, &arg)?)?,
                "--cleanup-grace" => {
                    config.cleanup_grace = parse_duration(&next_value(&mut args, &arg)?)?
                }
                "--payload-size" => {
                    config.payload_size = parse_size(&next_value(&mut args, &arg)?)?
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
                "--seal-workers" => {
                    config.seal_workers = parse_nonzero_usize(&next_value(&mut args, &arg)?)?
                }
                "--strata-gc" => config.strata_gc = parse_bool(&next_value(&mut args, &arg)?)?,
                "--relocation-profile-reads" => {
                    config.relocation_profile_reads =
                        parse_nonzero_usize(&next_value(&mut args, &arg)?)?
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
        if self.rocksdb_high_pri_threads > i32::MAX as usize
            || self.rocksdb_low_pri_threads > i32::MAX as usize
        {
            return Err("RocksDB background thread counts must not exceed i32::MAX".to_owned());
        }
        if self.relocation_profile_reads > 0 && self.engine != EngineKind::Strata {
            return Err("--relocation-profile-reads requires --engine strata".to_owned());
        }
        if self.relocation_profile_reads > 0 && self.relocation_profile_timeout.is_zero() {
            return Err("--relocation-profile-timeout must be non-zero".to_owned());
        }
        Ok(())
    }

    fn store_config(&self) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: self.root_dir.clone(),
            namespace: self.namespace.clone(),
            segment_max_bytes: self.segment_max_bytes,
            write_queue_capacity: self.queue_capacity,
            max_unsealed_segments: self.max_unsealed_segments,
            seal_worker_count: self.seal_workers,
            segment_reader_cache_capacity: DEFAULT_READER_CACHE_CAPACITY,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            gc_workers_enabled: self.strata_gc && self.relocation_profile_reads == 0,
            gc_interval: DEFAULT_GC_INTERVAL,
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config: if self.relocation_profile_reads == 0 {
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
            },
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: DEFAULT_STARTING_EPOCH,
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
    client_load_active: IntGauge,
    controller_healthy: IntGauge,
    controller_debounce_streak_windows: IntGauge,
    controller_read_p99_seconds: Gauge,
    controller_read_ops_per_second: Gauge,
    overdue_delete_keys: IntGauge,
    oldest_overdue_seconds: Gauge,
    directory_apparent_bytes: IntGauge,
    directory_allocated_bytes: IntGauge,
    directory_files: IntGauge,
    filesystem_available_bytes: IntGauge,
    filesystem_total_bytes: IntGauge,
    space_amplification: Gauge,
    space_sample_errors: IntCounter,
    blob_total_bytes: IntGauge,
    blob_garbage_bytes: IntGauge,
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
                "Logical payload bytes retired by successful due deletes.",
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
                "Writer concurrency currently admitted by the controller."
            ),
            client_load_active: int_gauge!(
                "strata_realistic_bench_client_load_active",
                "One during the client workload and zero during the cleanup grace period."
            ),
            controller_healthy: int_gauge!(
                "strata_realistic_bench_controller_healthy",
                "One when the latest read/delete service window met its obligations."
            ),
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
        registry.register(Box::new(metrics.read_outcomes.clone()))?;
        registry.register(Box::new(metrics.correctness_errors.clone()))?;
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
            &metrics.filesystem_available_bytes,
            &metrics.filesystem_total_bytes,
            &metrics.blob_total_bytes,
            &metrics.blob_garbage_bytes,
            &metrics.blob_file_bytes_written,
            &metrics.blob_gc_bytes_relocated,
            &metrics.rocksdb_wal_bytes_written,
        ] {
            registry.register(Box::new(gauge.clone()))?;
        }
        for gauge in [
            &metrics.controller_read_p99_seconds,
            &metrics.controller_read_ops_per_second,
            &metrics.oldest_overdue_seconds,
            &metrics.space_amplification,
        ] {
            registry.register(Box::new(gauge.clone()))?;
        }
        registry.register(Box::new(metrics.space_sample_errors.clone()))?;
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
    fn put(&self, key: &BlobKey) -> Result<(), String>;
    fn delete(&self, key: &BlobKey) -> Result<(), String>;
    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, String>;
    fn sync(&self) -> Result<(), String>;
}

struct StrataEngine {
    store: Arc<StrataStore>,
    payload: Arc<[u8]>,
}

impl BenchEngine for StrataEngine {
    fn put(&self, key: &BlobKey) -> Result<(), String> {
        self.store
            .put_arc(0, key.clone(), Arc::clone(&self.payload))
            .map(|_| ())
            .map_err(|error| error.to_string())
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
    fn put(&self, key: &BlobKey) -> Result<(), String> {
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
    state: AtomicU8,
    due_counted: AtomicBool,
}

impl KeyRecord {
    fn new(key: BlobKey, written_at: Instant, retention: Duration) -> Self {
        Self {
            key,
            written_at,
            due_at: written_at.checked_add(retention).unwrap_or(written_at),
            state: AtomicU8::new(KEY_LIVE),
            due_counted: AtomicBool::new(false),
        }
    }

    fn mark_due(&self, metrics: &HarnessMetrics) {
        if !self.due_counted.swap(true, Ordering::AcqRel) {
            metrics.delete_due.inc();
        }
    }
}

#[derive(Debug, Default)]
struct Model {
    live: VecDeque<Arc<KeyRecord>>,
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
        if self
            .live
            .back()
            .is_none_or(|last| last.due_at <= record.due_at)
        {
            self.live.push_back(record);
            return;
        }
        let position = self
            .live
            .iter()
            .position(|candidate| candidate.due_at > record.due_at)
            .unwrap_or(self.live.len());
        self.live.insert(position, record);
    }

    fn claim_due(&mut self, now: Instant, metrics: &HarnessMetrics) -> Option<Arc<KeyRecord>> {
        let record = self.live.front().filter(|record| record.due_at <= now)?;
        record.mark_due(metrics);
        let record = self.live.pop_front().expect("due key must remain at front");
        record.state.store(KEY_DELETING, Ordering::Release);
        self.deleting
            .insert(record.key.as_bytes().to_vec(), Arc::clone(&record));
        Some(record)
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
        let choose = |records: &VecDeque<Arc<KeyRecord>>, rng: &mut SplitMix64| {
            (!records.is_empty()).then(|| {
                let index = rng.next_u64() as usize % records.len();
                Arc::clone(records.get(index).expect("sample index must be in range"))
            })
        };
        if prefer_deleted {
            choose(&self.deleted, rng)
                .map(|record| (record, ReadExpectation::Deleted))
                .or_else(|| choose(&self.live, rng).map(|record| (record, ReadExpectation::Live)))
        } else {
            choose(&self.live, rng)
                .map(|record| (record, ReadExpectation::Live))
                .or_else(|| {
                    choose(&self.deleted, rng).map(|record| (record, ReadExpectation::Deleted))
                })
        }
    }

    fn refresh_delete_backlog(
        &self,
        now: Instant,
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
            if record.due_at > now {
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
                .front()
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

fn make_key(id: u64) -> Result<BlobKey, String> {
    let mut bytes = Vec::with_capacity(KEY_PREFIX.len() + 20);
    bytes.extend_from_slice(KEY_PREFIX);
    bytes.extend_from_slice(id.to_string().as_bytes());
    BlobKey::new(bytes).map_err(|error| error.to_string())
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
    next_key_id: Arc<AtomicU64>,
}

fn run_writer(worker_index: usize, context: Arc<WorkloadContext>) {
    while !context.workload_stop.load(Ordering::Acquire) {
        if worker_index >= context.active_write_workers.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
            continue;
        }

        let id = context.next_key_id.fetch_add(1, Ordering::Relaxed);
        let key = match make_key(id) {
            Ok(key) => key,
            Err(error) => {
                context.fatal.set(
                    format!("writer {worker_index} key generation failed: {error}"),
                    &context.workload_stop,
                );
                break;
            }
        };
        context
            .metrics
            .operation_attempts
            .with_label_values(&["put"])
            .inc();
        let started = Instant::now();
        match context.engine.put(&key) {
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
                context
                    .model
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push_live(Arc::new(KeyRecord::new(
                        key,
                        acknowledged_at,
                        context.config.retention,
                    )));
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
        let record = context
            .model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .claim_due(Instant::now(), &context.metrics);
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
    context
        .metrics
        .active_write_workers
        .set(context.config.initial_write_workers as i64);
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
        let backlog = context
            .model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .refresh_delete_backlog(
                Instant::now(),
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

        let healthy = read_rate >= required_read_rate
            && read_p99 <= context.config.read_p99_slo
            && backlog.matured_overdue_keys == 0
            && window.read_errors == 0
            && window.correctness_errors == 0
            && window.put_errors == 0
            && window.delete_errors == 0;
        context.metrics.controller_healthy.set(i64::from(healthy));
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
        return Ok(());
    }
    if metadata.is_dir() {
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
    println!("retention_seconds={:.3}", config.retention.as_secs_f64());
    println!(
        "cleanup_grace_seconds={:.3}",
        config.cleanup_grace.as_secs_f64()
    );
    println!("payload_bytes={}", config.payload_size);
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
            println!("seal_workers={}", config.seal_workers);
            println!("strata_gc={}", config.strata_gc);
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
    let active_write_workers = Arc::new(AtomicUsize::new(config.initial_write_workers));
    metrics
        .active_write_workers
        .set(config.initial_write_workers as i64);
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
        next_key_id: Arc::new(AtomicU64::new(0)),
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
    for worker_index in 0..config.delete_workers {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name(format!("realistic-deleter-{worker_index}"))
                .spawn(move || run_deleter(worker_index, worker_context))?,
        );
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
    if !config.sync_interval.is_zero() {
        let worker_context = Arc::clone(&context);
        workers.push(
            thread::Builder::new()
                .name("realistic-syncer".to_owned())
                .spawn(move || run_syncer(worker_context))?,
        );
    }

    let started_at = Instant::now();
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
    let (terminal_model, terminal_backlog) = {
        let model = model
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            model.snapshot(terminal_at),
            model.refresh_delete_backlog(terminal_at, config.delete_lag_slo, &metrics),
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
    store.rollover_active_segment_for_sealing()?;
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
        if sealed && patches == 0 {
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
    "usage: cargo run --release -p strata-realistic-bench -- [options]

required:
  --engine <strata|blobdb>
  --root <fresh-path>

workload:
  --duration <duration>                  write/read/delete phase; default 30m
  --retention <duration>                 delete each key this long after put; default 5m
  --cleanup-grace <duration>             no-traffic reclamation window; default 5m
  --payload-size <bytes|KiB|MiB|GiB>     default 1MiB
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
  --seal-workers <count>
  --strata-gc <true|false>
  --relocation-profile-reads <count>    post-workload HDD relocation profile; disables background GC for deterministic setup
  --relocation-profile-timeout <time>   setup/healing deadline; default 10m

blobdb:
  --rocksdb-min-blob-size <size>
  --rocksdb-blob-file-size <size>
  --rocksdb-write-buffer-size <size>
  --rocksdb-db-write-buffer-size <size>
  --rocksdb-max-write-buffer-number <count>
  --rocksdb-max-background-flushes <count>
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
    fn parses_minimal_configuration() {
        let config = Config::parse(
            ["--engine", "blobdb", "--root", "/tmp/realistic"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("configuration should parse");

        assert_eq!(config.engine, EngineKind::BlobDb);
        assert_eq!(config.payload_size, 1 << 20);
        assert!(config.sync_interval.is_zero());
        assert_eq!(config.controller_debounce_windows, 3);
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
        let record = Arc::new(KeyRecord::new(
            make_key(7).expect("key should be valid"),
            written_at,
            Duration::from_secs(10),
        ));
        model.push_live(Arc::clone(&record));

        assert!(
            model
                .claim_due(written_at + Duration::from_secs(9), &metrics)
                .is_none()
        );
        let claimed = model
            .claim_due(written_at + Duration::from_secs(10), &metrics)
            .expect("key should become due");
        assert!(Arc::ptr_eq(&record, &claimed));
        assert_eq!(metrics.delete_due.get(), 1);
        model.finish_delete(claimed);
        assert_eq!(model.deleted.len(), 1);
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
