//! RocksDB tuning the index opens its own instances with.
//!
//! These values reproduce the defaults the index previously inherited from its storage wrapper.
//! They are kept verbatim rather than re-tuned: an index opened with different compression or
//! block settings than it was written with is still readable, but its performance profile — and
//! every benchmark number recorded against it — silently changes.

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options};

/// Total write-buffer budget across all column families, in MiB.
const DB_WRITE_BUFFER_SIZE_MB: usize = 1024;
/// Write-ahead log size cap, in MiB.
const DB_WAL_SIZE_MB: usize = 1024;
/// Background threads for flushes and compactions.
const DB_PARALLELISM: i32 = 8;
/// Block cache for the default column family.
const DEFAULT_BLOCK_CACHE_BYTES: usize = 128 << 20;
/// Block size for the default column family.
const DEFAULT_BLOCK_SIZE_BYTES: usize = 16 << 10;

/// Environment overrides, kept for parity with the previous wrapper. These are emergency knobs;
/// prefer changing the constants above.
fn size_from_env(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

/// Database-wide options for an index instance Strata opens itself.
pub fn default_db_options() -> Options {
    let mut options = Options::default();

    // The default file-descriptor limit is low enough on macOS to fail tests with "too many open
    // files", so raise it and size the table cache from whatever the OS granted.
    if let Ok(outcome) = fdlimit::raise_fd_limit() {
        let limit = match outcome {
            fdlimit::Outcome::LimitRaised { to, .. } => to,
            fdlimit::Outcome::Unsupported => 1024,
        };
        options.set_max_open_files((limit / 8) as i32);
    }

    // 2^10 table-cache shards; raise if lock contention shows up here.
    options.set_table_cache_num_shard_bits(10);

    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);
    options.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    options.set_db_write_buffer_size(
        size_from_env("DB_WRITE_BUFFER_SIZE_MB").unwrap_or(DB_WRITE_BUFFER_SIZE_MB) * 1024 * 1024,
    );
    options.set_max_total_wal_size(
        size_from_env("DB_WAL_SIZE_MB").unwrap_or(DB_WAL_SIZE_MB) as u64 * 1024 * 1024,
    );
    options.increase_parallelism(
        size_from_env("DB_PARALLELISM").map_or(DB_PARALLELISM, |value| value as i32),
    );
    options.set_enable_pipelined_write(true);
    options.set_block_based_table_factory(&block_options(
        &Cache::new_lru_cache(DEFAULT_BLOCK_CACHE_BYTES),
        Some(DEFAULT_BLOCK_SIZE_BYTES),
        Some(true),
    ));
    options.set_memtable_prefix_bloom_ratio(0.02);

    options
}

/// Block-based table options with a caller-supplied block cache.
///
/// Each column family that wants its own cache must pass its own [`Cache`]; passing the same cache
/// shares it.
pub fn block_options(
    block_cache: &Cache,
    block_size_bytes: Option<usize>,
    pin_l0_filter_and_index_blocks: Option<bool>,
) -> BlockBasedOptions {
    let mut block_options = BlockBasedOptions::default();
    if let Some(block_size_bytes) = block_size_bytes {
        block_options.set_block_size(block_size_bytes);
    }
    block_options.set_block_cache(block_cache);
    block_options.set_cache_index_and_filter_blocks(true);
    // Bloom filter at a 1% false-positive rate.
    block_options.set_bloom_filter(10.0, false);
    if let Some(pin) = pin_l0_filter_and_index_blocks {
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(pin);
    }
    block_options
}
