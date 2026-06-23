use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, Once, mpsc},
    thread,
    time::{Duration, Instant},
};

use strata_core::{BlobKey, BlobState, PlacementClass, SegmentFileState, SegmentId, StrataLsn};
use strata_index::StrataIndex;
use strata_store::{
    DEFAULT_ACCOUNTING_INTERVAL, DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_DELTA_RUN_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_INGEST_RECORD_THRESHOLD, DEFAULT_ACCOUNTING_SIDECAR_INTERVAL,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_BYTES_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_MAJOR_PATCH_COUNT_THRESHOLD,
    DEFAULT_ACCOUNTING_SIDECAR_PARTITION_COUNT, DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
    SealedSegmentIntegrityPolicy, StrataRecoveryPolicy, StrataStore, StrataStoreConfig,
    StrataStoreMetrics,
};
use tempfile::tempdir;
use typed_store::DBMetrics;

static INIT_TYPED_STORE_METRICS: Once = Once::new();

const NAMESPACE: &str = "default";
const SEED: u64 = 0x51ed_5eed_f00d_cafe;
const PAYLOAD_BYTES: usize = 128;

#[derive(Debug, Clone, Copy)]
struct PutEvent {
    lsn: StrataLsn,
    run_id: u64,
    thread_id: usize,
    sequence: u64,
    payload_bytes: usize,
}

#[derive(Debug)]
enum WorkerEvent {
    Ready,
    Put(PutEvent),
    Sync(StrataLsn),
    ParseError(String),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "crash-loop stress test; set STRATA_RECOVERY_STRESS_ITERS for longer runs"]
async fn crash_loop_recovers_synced_puts() {
    init_typed_store_metrics();

    let iterations = env_usize("STRATA_RECOVERY_STRESS_ITERS").unwrap_or(12);
    let dir = tempdir().unwrap();
    let root_dir = dir.path().to_path_buf();
    let mut durable_puts = Vec::new();

    for run_id in 0..iterations as u64 {
        let mut child = spawn_worker(&root_dir, run_id);
        let (events, stderr) = kill_worker_after_ready(&mut child, run_id);
        let mut observed_puts = Vec::new();
        let mut run_synced_lsn = 0;
        for event in events {
            match event {
                WorkerEvent::Ready => {}
                WorkerEvent::Put(put) => observed_puts.push(put),
                WorkerEvent::Sync(lsn) => run_synced_lsn = run_synced_lsn.max(lsn),
                WorkerEvent::ParseError(error) => panic!("{error}"),
            }
        }
        durable_puts.extend(
            observed_puts
                .into_iter()
                .filter(|put| put.lsn <= run_synced_lsn),
        );

        let durable_lsn_floor = durable_puts.iter().map(|put| put.lsn).max().unwrap_or(0);
        let fault = inject_post_crash_fault(&root_dir, run_id, durable_lsn_floor);
        validate_recovered_store(
            &root_dir,
            durable_lsn_floor,
            &durable_puts,
            fault.as_deref(),
            &stderr,
        );
    }
}

fn init_typed_store_metrics() {
    INIT_TYPED_STORE_METRICS.call_once(|| {
        DBMetrics::get();
    });
}

fn spawn_worker(root_dir: &Path, run_id: u64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_recovery_stress_worker"))
        .arg("--root")
        .arg(root_dir)
        .arg("--namespace")
        .arg(NAMESPACE)
        .arg("--run-id")
        .arg(run_id.to_string())
        .arg("--seed")
        .arg(SEED.to_string())
        .arg("--writer-threads")
        .arg("8")
        .arg("--reader-threads")
        .arg("4")
        .arg("--payload-bytes")
        .arg(PAYLOAD_BYTES.to_string())
        .arg("--segment-max-bytes")
        .arg("512")
        .arg("--write-queue-capacity")
        .arg("4")
        .arg("--max-unsealed-segments")
        .arg("3")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn kill_worker_after_ready(child: &mut Child, run_id: u64) -> (Vec<WorkerEvent>, String) {
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let stderr_buffer = Arc::new(Mutex::new(String::new()));

    let stdout_thread = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    let event = parse_worker_line(&line).unwrap_or_else(WorkerEvent::ParseError);
                    if event_tx.send(event).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = event_tx.send(WorkerEvent::ParseError(error.to_string()));
                    break;
                }
            }
        }
    });

    let stderr_thread = {
        let stderr_buffer = Arc::clone(&stderr_buffer);
        thread::spawn(move || {
            let mut stderr = stderr;
            let mut contents = String::new();
            let _ = stderr.read_to_string(&mut contents);
            *stderr_buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = contents;
        })
    };

    let mut events = Vec::new();
    wait_for_ready(child, &event_rx, &mut events);
    thread::sleep(kill_delay(run_id));

    if let Some(status) = child.try_wait().unwrap() {
        stdout_thread.join().unwrap();
        stderr_thread.join().unwrap();
        let stderr = stderr_buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        panic!("stress worker exited before kill with status {status}; stderr:\n{stderr}");
    }

    child.kill().unwrap();
    let _ = child.wait().unwrap();
    stdout_thread.join().unwrap();
    stderr_thread.join().unwrap();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }

    let stderr = stderr_buffer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    (events, stderr)
}

fn wait_for_ready(
    child: &mut Child,
    event_rx: &mpsc::Receiver<WorkerEvent>,
    events: &mut Vec<WorkerEvent>,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match event_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(WorkerEvent::Ready) => {
                events.push(WorkerEvent::Ready);
                return;
            }
            Ok(event) => events.push(event),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("stress worker exited before READY with status {status}");
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for stress worker READY"
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("stress worker stdout closed before READY");
            }
        }
    }
}

fn parse_worker_line(line: &str) -> Result<WorkerEvent, String> {
    let mut fields = line.split_whitespace();
    match fields.next() {
        Some("READY") => Ok(WorkerEvent::Ready),
        Some("SYNC") => Ok(WorkerEvent::Sync(parse_field(
            fields.next(),
            "sync durable_lsn",
        )?)),
        Some("PUT") => Ok(WorkerEvent::Put(PutEvent {
            lsn: parse_field(fields.next(), "put lsn")?,
            run_id: parse_field(fields.next(), "put run_id")?,
            thread_id: parse_field(fields.next(), "put thread_id")?,
            sequence: parse_field(fields.next(), "put sequence")?,
            payload_bytes: parse_field(fields.next(), "put payload_bytes")?,
        })),
        Some(other) => Err(format!("unknown worker event {other:?} in line {line:?}")),
        None => Err("empty worker event line".to_owned()),
    }
}

fn parse_field<T: std::str::FromStr>(field: Option<&str>, name: &str) -> Result<T, String> {
    let field = field.ok_or_else(|| format!("missing {name}"))?;
    field
        .parse()
        .map_err(|_| format!("invalid {name}: {field}"))
}

fn inject_post_crash_fault(
    root_dir: &Path,
    run_id: u64,
    durable_lsn_floor: StrataLsn,
) -> Option<String> {
    let cfg = store_config(root_dir);
    let index = StrataIndex::open_path(cfg.standalone_index_dir(), cfg.index_cf_prefix()).ok()?;
    let states = index.iter_segment_states().ok()?;
    drop(index);

    let unsealed = states
        .iter()
        .filter(|(_, state)| {
            state.placement_class == PlacementClass::Ingest
                && matches!(
                    state.state,
                    SegmentFileState::Open
                        | SegmentFileState::Sealing
                        | SegmentFileState::SealFailed
                )
        })
        .map(|(segment_id, state)| (*segment_id, state.clone()))
        .collect::<Vec<_>>();
    let choice = mix64(SEED ^ run_id) % 4;

    match choice {
        0 => truncate_unsealed_tail(&cfg, &unsealed, run_id),
        1 => append_unsealed_garbage(&cfg, &unsealed, run_id),
        2 => delete_unsealed_without_durable_bytes(&cfg, &unsealed, durable_lsn_floor),
        _ => create_orphan_segment_file(&cfg, &states, run_id),
    }
}

fn truncate_unsealed_tail(
    cfg: &StrataStoreConfig,
    unsealed: &[(SegmentId, strata_core::SegmentState)],
    run_id: u64,
) -> Option<String> {
    let candidates = unsealed
        .iter()
        .filter(|(_, state)| state.write_offset > state.durable_offset)
        .collect::<Vec<_>>();
    let (segment_id, state) = *candidates.first()?;
    let gap = state.write_offset - state.durable_offset;
    let truncate_to = state.durable_offset + (mix64(SEED ^ run_id ^ segment_id) % gap);
    OpenOptions::new()
        .write(true)
        .open(segment_path(cfg, *segment_id))
        .ok()?
        .set_len(truncate_to)
        .ok()?;
    Some(format!(
        "truncated unsealed segment {segment_id} from {} to {truncate_to}",
        state.write_offset
    ))
}

fn append_unsealed_garbage(
    cfg: &StrataStoreConfig,
    unsealed: &[(SegmentId, strata_core::SegmentState)],
    run_id: u64,
) -> Option<String> {
    let (segment_id, _) = unsealed.first()?;
    let path = segment_path(cfg, *segment_id);
    let mut file = OpenOptions::new().append(true).open(&path).ok()?;
    let garbage_len = 1 + (mix64(SEED ^ run_id ^ segment_id) % 31) as usize;
    file.write_all(&vec![0xa5; garbage_len]).ok()?;
    Some(format!(
        "appended {garbage_len} garbage bytes to unsealed segment {segment_id}"
    ))
}

fn delete_unsealed_without_durable_bytes(
    cfg: &StrataStoreConfig,
    unsealed: &[(SegmentId, strata_core::SegmentState)],
    durable_lsn_floor: StrataLsn,
) -> Option<String> {
    let (segment_id, _) = unsealed.iter().find(|(_, state)| {
        state.durable_offset == 0
            && state.write_offset > 0
            && state
                .min_lsn
                .is_some_and(|min_lsn| min_lsn > durable_lsn_floor)
    })?;
    let path = segment_path(cfg, *segment_id);
    fs::remove_file(path).ok()?;
    Some(format!(
        "deleted unsealed segment {segment_id} with no durable bytes"
    ))
}

fn create_orphan_segment_file(
    cfg: &StrataStoreConfig,
    states: &[(SegmentId, strata_core::SegmentState)],
    run_id: u64,
) -> Option<String> {
    let existing = states
        .iter()
        .map(|(segment_id, _)| *segment_id)
        .collect::<BTreeSet<_>>();
    let mut segment_id = 10_000 + run_id;
    while existing.contains(&segment_id) {
        segment_id += 1;
    }
    fs::create_dir_all(cfg.ingest_dir()).ok()?;
    fs::write(segment_path(cfg, segment_id), b"orphan segment bytes").ok()?;
    Some(format!("created orphan segment file {segment_id}"))
}

fn validate_recovered_store(
    root_dir: &Path,
    durable_lsn_floor: StrataLsn,
    durable_puts: &[PutEvent],
    fault: Option<&str>,
    worker_stderr: &str,
) {
    let cfg = store_config(root_dir);
    let store =
        StrataStore::open(cfg.clone(), StrataStoreMetrics::default()).unwrap_or_else(|error| {
            panic!(
                "failed to reopen store after fault {:?}; worker stderr:\n{}; error: {}",
                fault, worker_stderr, error
            )
        });
    let durable_lsn = store.durable_lsn().unwrap();
    assert!(
        durable_lsn >= durable_lsn_floor,
        "durable_lsn regressed after recovery; durable_lsn_floor={durable_lsn_floor} recovered={durable_lsn} fault={fault:?}"
    );

    for put in durable_puts
        .iter()
        .filter(|put| put.payload_bytes == PAYLOAD_BYTES)
    {
        let key = key_for(*put);
        let expected = payload_for(*put, SEED);
        let actual = store.get(&key).unwrap_or_else(|error| {
            panic!(
                "failed reading known durable put lsn={} key={:?}; fault={fault:?}; error={error}",
                put.lsn,
                key.as_bytes()
            )
        });
        assert_eq!(
            actual,
            Some(expected),
            "known durable put missing or corrupted: lsn={} key={:?} fault={fault:?}",
            put.lsn,
            key.as_bytes()
        );
    }

    let store_state = store.index().get_store_state().unwrap().unwrap_or_default();
    assert_eq!(store_state.durable_lsn, durable_lsn);
    assert!(store_state.next_lsn > durable_lsn);

    for (lsn, _) in store.index().iter_unaccounted_lsn_ops().unwrap() {
        assert!(
            lsn < store_state.next_lsn,
            "unaccounted LSN {lsn} remained at or beyond next_lsn {}; fault={fault:?}",
            store_state.next_lsn
        );
    }

    validate_segment_states(&cfg, &store, fault);
    validate_blob_versions(&store, fault);
}

fn validate_segment_states(cfg: &StrataStoreConfig, store: &StrataStore, fault: Option<&str>) {
    for (segment_id, state) in store.index().iter_segment_states().unwrap() {
        assert_eq!(segment_id, state.segment_id);
        assert!(
            state.durable_offset <= state.write_offset,
            "segment {segment_id} durable_offset {} exceeds write_offset {}; fault={fault:?}",
            state.durable_offset,
            state.write_offset
        );
        match state.state {
            SegmentFileState::Deleted => {}
            SegmentFileState::Sealed => {
                let sealed_len = state.sealed_len.expect("sealed segment has sealed_len");
                assert_eq!(state.write_offset, sealed_len);
                assert_eq!(state.durable_offset, sealed_len);
                let file_len = fs::metadata(segment_path(cfg, segment_id)).unwrap().len();
                assert_eq!(
                    file_len, sealed_len,
                    "sealed segment length mismatch after recovery; fault={fault:?}"
                );
            }
            SegmentFileState::Open | SegmentFileState::Sealing | SegmentFileState::SealFailed => {
                if let Ok(metadata) = fs::metadata(segment_path(cfg, segment_id)) {
                    assert!(
                        metadata.len() >= state.write_offset,
                        "segment {segment_id} file is shorter than indexed write_offset after recovery; fault={fault:?}"
                    );
                }
            }
            SegmentFileState::Deleting => {}
        }
    }
}

fn validate_blob_versions(store: &StrataStore, fault: Option<&str>) {
    for (version_key, entry) in store.index().iter_blob_versions().unwrap() {
        if entry.state == BlobState::Tombstoned {
            continue;
        }
        let Some(record_ref) = entry.record_ref else {
            continue;
        };
        let state = store
            .index()
            .get_segment_state(record_ref.segment_id)
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "blob version lsn={} points at missing segment {}; fault={fault:?}",
                    version_key.lsn, record_ref.segment_id
                )
            });
        let record_end = record_ref
            .end_offset()
            .expect("record ref does not overflow");
        assert!(
            record_end <= state.write_offset,
            "blob version lsn={} points beyond segment {} write_offset {}; fault={fault:?}",
            version_key.lsn,
            record_ref.segment_id,
            state.write_offset
        );
        assert!(
            !matches!(
                state.state,
                SegmentFileState::Deleted
                    | SegmentFileState::Deleting
                    | SegmentFileState::SealFailed
            ),
            "blob version lsn={} points at unreadable segment {} state {:?}; fault={fault:?}",
            version_key.lsn,
            record_ref.segment_id,
            state.state
        );
    }
}

fn store_config(root_dir: &Path) -> StrataStoreConfig {
    StrataStoreConfig {
        root_dir: root_dir.to_path_buf(),
        namespace: NAMESPACE.to_owned(),
        segment_max_bytes: 512,
        write_queue_capacity: 4,
        max_unsealed_segments: 3,
        segment_reader_cache_capacity: 8,
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
        starting_epoch: 1,
    }
}

fn key_for(put: PutEvent) -> BlobKey {
    BlobKey::new(format!(
        "stress:{}:{}:{}",
        put.run_id, put.thread_id, put.sequence
    ))
    .unwrap()
}

fn payload_for(put: PutEvent, seed: u64) -> Vec<u8> {
    let mut state = seed
        ^ put.run_id.rotate_left(17)
        ^ (put.thread_id as u64).rotate_left(31)
        ^ put.sequence.rotate_left(43);
    let mut payload = Vec::with_capacity(put.payload_bytes);
    while payload.len() < put.payload_bytes {
        state = mix64(state);
        payload.extend_from_slice(&state.to_le_bytes());
    }
    payload.truncate(put.payload_bytes);
    payload
}

fn segment_path(cfg: &StrataStoreConfig, segment_id: SegmentId) -> PathBuf {
    cfg.ingest_dir().join(format!("{segment_id:012}.data"))
}

fn kill_delay(run_id: u64) -> Duration {
    Duration::from_millis(75 + (mix64(SEED ^ run_id) % 175))
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
