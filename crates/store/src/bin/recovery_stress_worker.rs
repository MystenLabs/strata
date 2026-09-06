use std::{
    env,
    io::{self, Write},
    path::PathBuf,
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use core_types::{BlobKey, Epoch};
use store::{
    DEFAULT_GC_INITIAL_WORKER_COUNT, DEFAULT_GC_IO_BYTES_PER_SEC, DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
    DEFAULT_GC_SYNC_IMPACT_THRESHOLD, DEFAULT_GC_TUNING_WINDOW_CYCLES, DEFAULT_GC_WORKER_COUNT,
    DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT, GcPlannerConfig, SealedSegmentIntegrityPolicy,
    StrataRecoveryPolicy, StrataStore, StrataStoreConfig, StrataStoreMetrics,
};
use tokio::runtime::Handle;

#[derive(Debug, Clone)]
struct Config {
    root_dir: PathBuf,
    namespace: String,
    run_id: u64,
    seed: u64,
    writer_threads: usize,
    reader_threads: usize,
    payload_bytes: usize,
    segment_max_bytes: u64,
    write_queue_capacity: usize,
    max_unsealed_segments: usize,
    starting_epoch: Epoch,
}

#[derive(Debug, Clone, Copy)]
struct KeySpec {
    run_id: u64,
    thread_id: usize,
    sequence: u64,
}

fn main() {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to build tokio runtime: {error}");
            process::exit(1);
        }
    };

    if let Err(error) = runtime.block_on(async { run() }) {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::parse()?;
    let store_config = config.store_config();
    let store = Arc::new(
        StrataStore::open(store_config, StrataStoreMetrics::default()).map_err(format_err)?,
    );
    let output = Arc::new(Mutex::new(io::stdout()));
    let published = Arc::new(Mutex::new(Vec::<KeySpec>::new()));
    let next_reader_index = Arc::new(AtomicUsize::new(0));
    let runtime_handle = Handle::current();

    write_line(&output, "READY")?;

    for thread_id in 0..config.writer_threads {
        let store = Arc::clone(&store);
        let output = Arc::clone(&output);
        let published = Arc::clone(&published);
        let config = config.clone();
        let runtime_handle = runtime_handle.clone();
        thread::Builder::new()
            .name(format!("stress-writer-{thread_id}"))
            .spawn(move || {
                let _runtime_guard = runtime_handle.enter();
                writer_loop(store, output, published, config, thread_id);
            })
            .map_err(|error| error.to_string())?;
    }

    for reader_id in 0..config.reader_threads {
        let store = Arc::clone(&store);
        let published = Arc::clone(&published);
        let next_reader_index = Arc::clone(&next_reader_index);
        let config = config.clone();
        let runtime_handle = runtime_handle.clone();
        thread::Builder::new()
            .name(format!("stress-reader-{reader_id}"))
            .spawn(move || {
                let _runtime_guard = runtime_handle.enter();
                reader_loop(store, published, next_reader_index, config, reader_id);
            })
            .map_err(|error| error.to_string())?;
    }

    let sync_count = Arc::new(AtomicU64::new(0));
    {
        let store = Arc::clone(&store);
        let output = Arc::clone(&output);
        let config = config.clone();
        let sync_count = Arc::clone(&sync_count);
        let runtime_handle = runtime_handle.clone();
        thread::Builder::new()
            .name("stress-sync".to_owned())
            .spawn(move || {
                let _runtime_guard = runtime_handle.enter();
                sync_loop(store, output, config, sync_count);
            })
            .map_err(|error| error.to_string())?;
    }

    loop {
        thread::park_timeout(Duration::from_secs(60));
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut root_dir = None;
        let mut namespace = "default".to_owned();
        let mut run_id = None;
        let mut seed = 1;
        let mut writer_threads = 8;
        let mut reader_threads = 4;
        let mut payload_bytes = 128;
        let mut segment_max_bytes = 512;
        let mut write_queue_capacity = 4;
        let mut max_unsealed_segments = 3;
        let mut starting_epoch = 1;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" => root_dir = Some(PathBuf::from(next_arg(&mut args, "--root")?)),
                "--namespace" => namespace = next_arg(&mut args, "--namespace")?,
                "--run-id" => run_id = Some(parse_arg(&mut args, "--run-id")?),
                "--seed" => seed = parse_arg(&mut args, "--seed")?,
                "--writer-threads" => writer_threads = parse_arg(&mut args, "--writer-threads")?,
                "--reader-threads" => reader_threads = parse_arg(&mut args, "--reader-threads")?,
                "--payload-bytes" => payload_bytes = parse_arg(&mut args, "--payload-bytes")?,
                "--segment-max-bytes" => {
                    segment_max_bytes = parse_arg(&mut args, "--segment-max-bytes")?
                }
                "--write-queue-capacity" => {
                    write_queue_capacity = parse_arg(&mut args, "--write-queue-capacity")?
                }
                "--max-unsealed-segments" => {
                    max_unsealed_segments = parse_arg(&mut args, "--max-unsealed-segments")?
                }
                "--starting-epoch" => starting_epoch = parse_arg(&mut args, "--starting-epoch")?,
                _ => return Err(format!("unknown argument {arg}")),
            }
        }

        let root_dir = root_dir.ok_or_else(|| "--root is required".to_owned())?;
        let run_id = run_id.ok_or_else(|| "--run-id is required".to_owned())?;
        if writer_threads == 0 {
            return Err("--writer-threads must be non-zero".to_owned());
        }
        if payload_bytes == 0 {
            return Err("--payload-bytes must be non-zero".to_owned());
        }
        if max_unsealed_segments < 2 {
            return Err("--max-unsealed-segments must be at least 2".to_owned());
        }

        Ok(Self {
            root_dir,
            namespace,
            run_id,
            seed,
            writer_threads,
            reader_threads,
            payload_bytes,
            segment_max_bytes,
            write_queue_capacity,
            max_unsealed_segments,
            starting_epoch,
        })
    }

    fn store_config(&self) -> StrataStoreConfig {
        StrataStoreConfig {
            root_dir: self.root_dir.clone(),
            namespace: self.namespace.clone(),
            segment_max_bytes: self.segment_max_bytes,
            write_queue_capacity: self.write_queue_capacity,
            max_unsealed_segments: self.max_unsealed_segments,
            segment_reader_cache_capacity: 8,
            lsm_partition_count: store::DEFAULT_LSM_PARTITION_COUNT,
            lsm_compaction_patch_bytes: store::DEFAULT_LSM_COMPACTION_PATCH_BYTES,
            lsm_memtable_max_age: store::DEFAULT_LSM_MEMTABLE_MAX_AGE,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            gc_workers_enabled: true,
            gc_interval: Duration::from_secs(3600),
            gc_worker_count: DEFAULT_GC_WORKER_COUNT,
            gc_initial_worker_count: DEFAULT_GC_INITIAL_WORKER_COUNT,
            gc_tuning_window_cycles: DEFAULT_GC_TUNING_WINDOW_CYCLES,
            gc_sync_impact_threshold: DEFAULT_GC_SYNC_IMPACT_THRESHOLD,
            gc_io_bytes_per_sec: DEFAULT_GC_IO_BYTES_PER_SEC,
            gc_min_io_bytes_per_sec: DEFAULT_GC_MIN_IO_BYTES_PER_SEC,
            gc_planner_config: GcPlannerConfig::default(),
            shard_drop_gc_drain_timeout: DEFAULT_SHARD_DROP_GC_DRAIN_TIMEOUT,
            starting_epoch: self.starting_epoch,
        }
    }
}

fn writer_loop(
    store: Arc<StrataStore>,
    output: Arc<Mutex<io::Stdout>>,
    published: Arc<Mutex<Vec<KeySpec>>>,
    config: Config,
    thread_id: usize,
) {
    let mut sequence = 0;
    loop {
        sequence += 1;
        let spec = KeySpec {
            run_id: config.run_id,
            thread_id,
            sequence,
        };
        let key = key_for(spec);
        let payload = payload_for(spec, config.seed, config.payload_bytes);
        let lsn = match store.put(0, &key, &payload) {
            Ok(lsn) => lsn,
            Err(error) => exit_worker(format!("put failed: {error}")),
        };

        {
            let mut published = published
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            published.push(spec);
            if published.len() > 4096 {
                let drain_len = published.len() - 4096;
                published.drain(0..drain_len);
            }
        }

        if let Err(error) = write_line(
            &output,
            &format!(
                "PUT {lsn} {} {} {} {}",
                spec.run_id, spec.thread_id, spec.sequence, config.payload_bytes
            ),
        ) {
            exit_worker(error);
        }

        if sequence % 5 == 0 {
            assert_payload(&store, spec, config.seed, config.payload_bytes);
        }
    }
}

fn reader_loop(
    store: Arc<StrataStore>,
    published: Arc<Mutex<Vec<KeySpec>>>,
    next_reader_index: Arc<AtomicUsize>,
    config: Config,
    reader_id: usize,
) {
    loop {
        let spec = {
            let published = published
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if published.is_empty() {
                drop(published);
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            let index =
                next_reader_index.fetch_add(1 + reader_id, Ordering::Relaxed) % published.len();
            published[index]
        };
        assert_payload(&store, spec, config.seed, config.payload_bytes);
    }
}

fn sync_loop(
    store: Arc<StrataStore>,
    output: Arc<Mutex<io::Stdout>>,
    config: Config,
    sync_count: Arc<AtomicU64>,
) {
    loop {
        let count = sync_count.fetch_add(1, Ordering::Relaxed);
        let delay_ms = 3 + (mix64(config.seed ^ config.run_id ^ count) % 13);
        thread::sleep(Duration::from_millis(delay_ms));

        if let Err(error) = store.sync() {
            exit_worker(format!("sync failed: {error}"));
        }
        let published_lsn = match store.published_lsn() {
            Ok(lsn) => lsn,
            Err(error) => exit_worker(format!("published_lsn failed: {error}")),
        };
        if let Err(error) = write_line(&output, &format!("SYNC {published_lsn}")) {
            exit_worker(error);
        }
    }
}

fn assert_payload(store: &StrataStore, spec: KeySpec, seed: u64, payload_bytes: usize) {
    let key = key_for(spec);
    let expected = payload_for(spec, seed, payload_bytes);
    match store.get(&key) {
        Ok(Some(actual)) if actual == expected => {}
        Ok(Some(_)) => exit_worker(format!("payload mismatch for {:?}", key.as_bytes())),
        Ok(None) => exit_worker(format!("missing payload for {:?}", key.as_bytes())),
        Err(error) => exit_worker(format!("get failed: {error}")),
    }
}

fn key_for(spec: KeySpec) -> BlobKey {
    BlobKey::new(format!(
        "stress:{}:{}:{}",
        spec.run_id, spec.thread_id, spec.sequence
    ))
    .expect("stress key is valid")
}

fn payload_for(spec: KeySpec, seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed
        ^ spec.run_id.rotate_left(17)
        ^ (spec.thread_id as u64).rotate_left(31)
        ^ spec.sequence.rotate_left(43);
    let mut payload = Vec::with_capacity(len);
    while payload.len() < len {
        state = mix64(state);
        payload.extend_from_slice(&state.to_le_bytes());
    }
    payload.truncate(len);
    payload
}

fn write_line(output: &Arc<Mutex<io::Stdout>>, line: &str) -> Result<(), String> {
    let mut output = output
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    writeln!(output, "{line}").map_err(|error| error.to_string())?;
    output.flush().map_err(|error| error.to_string())
}

fn parse_arg<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    let value = next_arg(args, name)?;
    value
        .parse()
        .map_err(|_| format!("invalid value for {name}: {value}"))
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn format_err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn exit_worker(message: String) -> ! {
    eprintln!("{message}");
    process::exit(1);
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
