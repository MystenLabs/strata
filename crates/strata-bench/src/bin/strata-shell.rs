//! Interactive shell for opening a Strata store and issuing foreground operations.

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Write},
    path::PathBuf,
    process,
};

use strata_core::{BlobKey, Epoch};
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
    GcPlannerConfig, SealedSegmentIntegrityPolicy, StrataRecoveryPolicy, StrataStore,
    StrataStoreConfig, StrataStoreMetrics,
};

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_UNSEALED_SEGMENTS: usize = 8;
const DEFAULT_READER_CACHE_CAPACITY: usize = strata_store::DEFAULT_SEGMENT_READER_CACHE_CAPACITY;
const DEFAULT_MAX_PRINT_BYTES: usize = 4096;
const DEFAULT_STARTING_EPOCH: Epoch = 42;

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

#[derive(Debug, Clone)]
struct Config {
    root_dir: PathBuf,
    namespace: String,
    queue_capacity: usize,
    segment_max_bytes: u64,
    max_unsealed_segments: usize,
    seal_worker_count: usize,
    reader_cache_capacity: usize,
    recovery_policy: StrataRecoveryPolicy,
    sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy,
    max_print_bytes: usize,
    starting_epoch: Epoch,
    script: Option<PathBuf>,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            root_dir: PathBuf::new(),
            namespace: DEFAULT_NAMESPACE.to_owned(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            max_unsealed_segments: DEFAULT_MAX_UNSEALED_SEGMENTS,
            seal_worker_count: DEFAULT_SEAL_WORKER_COUNT,
            reader_cache_capacity: DEFAULT_READER_CACHE_CAPACITY,
            recovery_policy: StrataRecoveryPolicy::PointInTime,
            sealed_segment_integrity_policy: SealedSegmentIntegrityPolicy::MetadataOnly,
            max_print_bytes: DEFAULT_MAX_PRINT_BYTES,
            starting_epoch: DEFAULT_STARTING_EPOCH,
            script: None,
        };

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Err("help requested".to_owned()),
                "--root" => config.root_dir = PathBuf::from(next_value(&mut args, "--root")?),
                "--namespace" => config.namespace = next_value(&mut args, "--namespace")?,
                "--queue-capacity" => {
                    config.queue_capacity =
                        parse_nonzero_usize(&next_value(&mut args, "--queue-capacity")?)?
                }
                "--segment-max-bytes" => {
                    config.segment_max_bytes =
                        parse_size(&next_value(&mut args, "--segment-max-bytes")?)? as u64
                }
                "--max-unsealed-segments" => {
                    config.max_unsealed_segments =
                        parse_nonzero_usize(&next_value(&mut args, "--max-unsealed-segments")?)?
                }
                "--seal-workers" => {
                    config.seal_worker_count =
                        parse_nonzero_usize(&next_value(&mut args, "--seal-workers")?)?
                }
                "--reader-cache-capacity" => {
                    config.reader_cache_capacity =
                        parse_usize(&next_value(&mut args, "--reader-cache-capacity")?)?
                }
                "--recovery-policy" => {
                    config.recovery_policy =
                        parse_recovery_policy(&next_value(&mut args, "--recovery-policy")?)?
                }
                "--sealed-integrity" => {
                    config.sealed_segment_integrity_policy =
                        parse_sealed_integrity(&next_value(&mut args, "--sealed-integrity")?)?
                }
                "--max-print-bytes" => {
                    config.max_print_bytes =
                        parse_usize(&next_value(&mut args, "--max-print-bytes")?)?
                }
                "--starting-epoch" => {
                    config.starting_epoch =
                        parse_epoch(&next_value(&mut args, "--starting-epoch")?)?
                }
                "--script" => {
                    config.script = Some(PathBuf::from(next_value(&mut args, "--script")?))
                }
                unknown => return Err(format!("unknown argument '{unknown}'")),
            }
        }

        if config.root_dir.as_os_str().is_empty() {
            return Err("--root is required".to_owned());
        }
        if config.segment_max_bytes == 0 {
            return Err("segment_max_bytes must be non-zero".to_owned());
        }

        Ok(config)
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
            recovery_policy: self.recovery_policy,
            sealed_segment_integrity_policy: self.sealed_segment_integrity_policy,
            accounting_worker_enabled: true,
            accounting_interval: DEFAULT_ACCOUNTING_INTERVAL,
            accounting_unaccounted_threshold: DEFAULT_ACCOUNTING_UNACCOUNTED_THRESHOLD,
            accounting_materialize_lag_threshold: DEFAULT_ACCOUNTING_MATERIALIZE_LAG_THRESHOLD,
            accounting_partition_count: DEFAULT_ACCOUNTING_PARTITION_COUNT,
            accounting_maintenance_interval: DEFAULT_ACCOUNTING_MAINTENANCE_INTERVAL,
            accounting_ingest_record_threshold: DEFAULT_ACCOUNTING_INGEST_RECORD_THRESHOLD,
            accounting_delta_run_count_threshold: DEFAULT_ACCOUNTING_DELTA_RUN_COUNT_THRESHOLD,
            accounting_delta_run_bytes_threshold: DEFAULT_ACCOUNTING_DELTA_RUN_BYTES_THRESHOLD,
            accounting_major_patch_count_threshold: DEFAULT_ACCOUNTING_MAJOR_PATCH_COUNT_THRESHOLD,
            accounting_major_patch_bytes_threshold: DEFAULT_ACCOUNTING_MAJOR_PATCH_BYTES_THRESHOLD,
            gc_workers_enabled: true,
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

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let store_config = config.store_config();
    let store = StrataStore::open(store_config, StrataStoreMetrics::default())?;
    println!(
        "opened root={} namespace={}",
        config.root_dir.display(),
        config.namespace
    );

    if let Some(script) = &config.script {
        let file = fs::File::open(script)?;
        run_commands(&store, &config, BufReader::new(file), false)?;
    } else {
        println!("type 'help' for commands; 'quit' to exit");
        let stdin = io::stdin();
        run_commands(&store, &config, stdin.lock(), true)?;
    }

    Ok(())
}

fn run_commands<R: BufRead>(
    store: &StrataStore,
    config: &Config,
    mut input: R,
    interactive: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut line = String::new();

    loop {
        line.clear();
        if interactive {
            print!("strata> ");
            io::stdout().flush()?;
        }

        if input.read_line(&mut line)? == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        match execute_command(store, config, trimmed) {
            Ok(Control::Continue) => {}
            Ok(Control::Quit) => break,
            Err(error) if interactive => eprintln!("error: {error}"),
            Err(error) => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, error).into());
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Continue,
    Quit,
}

fn execute_command(store: &StrataStore, config: &Config, line: &str) -> Result<Control, String> {
    let words = split_words(line)?;
    let Some(command) = words.first().map(String::as_str) else {
        return Ok(Control::Continue);
    };

    match command {
        "help" | "?" => {
            println!("{}", help());
        }
        "quit" | "exit" => return Ok(Control::Quit),
        "put" => {
            expect_arg_count(&words, 3, 3)?;
            let key = parse_key(&words[1])?;
            let payload = parse_bytes_arg(&words[2])?;
            let lsn = store
                .put(0, &key, &payload)
                .map_err(|error| error.to_string())?;
            println!("ok lsn={lsn}");
        }
        "put-hex" => {
            expect_arg_count(&words, 3, 3)?;
            let key = parse_key(&words[1])?;
            let payload = decode_hex(&words[2])?;
            let lsn = store
                .put(0, &key, &payload)
                .map_err(|error| error.to_string())?;
            println!("ok lsn={lsn}");
        }
        "get" => {
            expect_arg_count(&words, 2, 2)?;
            let key = parse_key(&words[1])?;
            match store.get_blob(&key).map_err(|error| error.to_string())? {
                Some(payload) => print_payload(&payload, config, true),
                None => println!("not_found"),
            }
        }
        "get-hex" => {
            expect_arg_count(&words, 2, 2)?;
            let key = parse_key(&words[1])?;
            match store.get_blob(&key).map_err(|error| error.to_string())? {
                Some(payload) => print_payload(&payload, config, false),
                None => println!("not_found"),
            }
        }
        "range" => {
            expect_arg_count(&words, 4, 4)?;
            let key = parse_key(&words[1])?;
            let start = parse_u64(&words[2])?;
            let len = parse_u64(&words[3])?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| "range end overflows u64".to_owned())?;
            match store
                .get_blob_range(&key, start..end)
                .map_err(|error| error.to_string())?
            {
                Some(payload) => print_payload(&payload, config, true),
                None => println!("not_found"),
            }
        }
        "delete" | "tombstone" => {
            expect_arg_count(&words, 2, 2)?;
            let key = parse_key(&words[1])?;
            let lsn = store.tombstone(&key).map_err(|error| error.to_string())?;
            println!("ok lsn={lsn}");
        }
        "extend" | "set-lifetime" => {
            expect_arg_count(&words, 3, 3)?;
            let key = parse_key(&words[1])?;
            let epoch = parse_epoch(&words[2])?;
            let lsn = store
                .set_blob_lifetime(&key, epoch)
                .map_err(|error| error.to_string())?;
            println!("ok lsn={lsn}");
        }
        "contains" => {
            expect_arg_count(&words, 2, 2)?;
            let key = parse_key(&words[1])?;
            let contains = store.contains(&key).map_err(|error| error.to_string())?;
            println!("{contains}");
        }
        "entry" => {
            expect_arg_count(&words, 2, 2)?;
            let key = parse_key(&words[1])?;
            match store
                .index()
                .get_blob_entry(&key)
                .map_err(|error| error.to_string())?
            {
                Some(entry) => println!("{entry:#?}"),
                None => println!("not_found"),
            }
        }
        "segment" => {
            expect_arg_count(&words, 2, 2)?;
            let segment_id = parse_u64(&words[1])?;
            match store
                .index()
                .get_segment_state(segment_id)
                .map_err(|error| error.to_string())?
            {
                Some(state) => println!("{state:#?}"),
                None => println!("not_found"),
            }
        }
        "stats" => {
            expect_arg_count(&words, 2, 2)?;
            let segment_id = parse_u64(&words[1])?;
            match store
                .index()
                .get_segment_gc_overlay(segment_id)
                .map_err(|error| error.to_string())?
            {
                Some(overlay) => println!("{overlay:#?}"),
                None => println!("not_found"),
            }
        }
        "sync" => {
            expect_arg_count(&words, 1, 1)?;
            store.sync().map_err(|error| error.to_string())?;
            println!(
                "ok durable_lsn={}",
                store.durable_lsn().map_err(|error| error.to_string())?
            );
        }
        "durable-lsn" => {
            expect_arg_count(&words, 1, 1)?;
            println!(
                "{}",
                store.durable_lsn().map_err(|error| error.to_string())?
            );
        }
        "config" => {
            expect_arg_count(&words, 1, 1)?;
            println!("root={}", config.root_dir.display());
            println!("namespace={}", config.namespace);
            println!("segment_max_bytes={}", config.segment_max_bytes);
            println!("queue_capacity={}", config.queue_capacity);
            println!("max_unsealed_segments={}", config.max_unsealed_segments);
            println!("seal_workers={}", config.seal_worker_count);
            println!("reader_cache_capacity={}", config.reader_cache_capacity);
            println!("recovery_policy={:?}", config.recovery_policy);
            println!(
                "sealed_integrity={:?}",
                config.sealed_segment_integrity_policy
            );
            println!("max_print_bytes={}", config.max_print_bytes);
        }
        unknown => return Err(format!("unknown command '{unknown}'")),
    }

    Ok(Control::Continue)
}

fn print_payload(payload: &[u8], config: &Config, include_utf8: bool) {
    let print_len = payload.len().min(config.max_print_bytes);
    let prefix = &payload[..print_len];

    println!("bytes={}", payload.len());
    if include_utf8 {
        match std::str::from_utf8(prefix) {
            Ok(text) => println!("utf8_prefix={text:?}"),
            Err(_) => println!("utf8_lossy_prefix={:?}", String::from_utf8_lossy(prefix)),
        }
    }
    println!("hex_prefix={}", encode_hex(prefix));
    if print_len < payload.len() {
        println!("truncated=true omitted_bytes={}", payload.len() - print_len);
    }
}

fn parse_key(input: &str) -> Result<BlobKey, String> {
    let bytes = parse_bytes_arg(input)?;
    BlobKey::new(bytes).map_err(|error| error.to_string())
}

fn parse_bytes_arg(input: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = input.strip_prefix("hex:") {
        decode_hex(hex)
    } else {
        Ok(input.as_bytes().to_vec())
    }
}

fn expect_arg_count(words: &[String], min: usize, max: usize) -> Result<(), String> {
    if words.len() < min || words.len() > max {
        if min == max {
            Err(format!("expected {min} argument(s), got {}", words.len()))
        } else {
            Err(format!(
                "expected {min}..={max} argument(s), got {}",
                words.len()
            ))
        }
    } else {
        Ok(())
    }
}

fn split_words(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut quote = None;

    while let Some(ch) = chars.next() {
        match (quote, ch) {
            (None, '#') if current.is_empty() => break,
            (None, ch) if ch.is_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            (None, '"' | '\'') => quote = Some(ch),
            (Some(expected), ch) if ch == expected => quote = None,
            (_, '\\') => {
                let Some(escaped) = chars.next() else {
                    return Err("trailing escape character".to_owned());
                };
                current.push(match escaped {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                });
            }
            (_, ch) => current.push(ch),
        }
    }

    if let Some(quote) = quote {
        return Err(format!("unterminated quote {quote}"));
    }
    if !current.is_empty() {
        words.push(current);
    }

    Ok(words)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    let input = input.strip_prefix("0x").unwrap_or(input);
    if !input.len().is_multiple_of(2) {
        return Err("hex input must have an even number of digits".to_owned());
    }

    let mut decoded = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit '{}'", byte as char)),
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

fn parse_epoch(value: &str) -> Result<Epoch, String> {
    parse_u64(value)
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

fn parse_recovery_policy(value: &str) -> Result<StrataRecoveryPolicy, String> {
    match value {
        "point-in-time" | "pit" => Ok(StrataRecoveryPolicy::PointInTime),
        "absolute-consistency" | "absolute" => Ok(StrataRecoveryPolicy::AbsoluteConsistency),
        _ => Err(format!("unknown recovery policy '{value}'")),
    }
}

fn parse_sealed_integrity(value: &str) -> Result<SealedSegmentIntegrityPolicy, String> {
    match value {
        "metadata-only" | "metadata" => Ok(SealedSegmentIntegrityPolicy::MetadataOnly),
        "checksum" => Ok(SealedSegmentIntegrityPolicy::Checksum),
        _ => Err(format!("unknown sealed integrity policy '{value}'")),
    }
}

fn usage() -> &'static str {
    "usage: cargo run -p strata-bench --bin strata-shell -- [options]

options:
  --root <path>
  --namespace <name>
  --queue-capacity <count>
  --segment-max-bytes <bytes|KiB|MiB|GiB>
  --max-unsealed-segments <count>
  --seal-workers <count>
  --reader-cache-capacity <count>       cached segment readers; 0 disables
  --recovery-policy <point-in-time|absolute-consistency>
  --sealed-integrity <metadata-only|checksum>
  --starting-epoch <epoch>
  --max-print-bytes <count>
  --script <path>"
}

fn help() -> &'static str {
    "commands:
  put <key> <value>               write UTF-8 bytes; use quotes for spaces
  put-hex <key> <hex>             write hex payload bytes
  get <key>                       read full payload, print UTF-8/hex prefix
  get-hex <key>                   read full payload, print hex prefix
  range <key> <start> <len>       read payload byte range
  contains <key>                  index-only live check
  delete <key>                    write tombstone
  tombstone <key>                 alias for delete
  set-lifetime <key> <epoch>      set blob logical end epoch without moving bytes
  extend <key> <epoch>            alias for set-lifetime
  entry <key>                     print latest blob index entry
  segment <segment_id>            print segment state
  stats <segment_id>              print GC overlay summary/ranges
  sync                            fsync active segment and advance durable LSN
  durable-lsn                     print current durable LSN
  config                          print shell/store config
  help                            show this help
  quit                            exit

keys and values are UTF-8 by default. Use hex:<hex> for binary keys/values."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_quoted_words() {
        assert_eq!(
            split_words("put alpha \"hello world\" 42").unwrap(),
            vec!["put", "alpha", "hello world", "42"]
        );
    }

    #[test]
    fn decodes_hex_with_optional_prefix() {
        assert_eq!(decode_hex("6869").unwrap(), b"hi");
        assert_eq!(decode_hex("0x6869").unwrap(), b"hi");
    }

    #[test]
    fn parses_hex_keys() {
        let key = parse_key("hex:616263").unwrap();
        assert_eq!(key.as_bytes(), b"abc");
    }
}
