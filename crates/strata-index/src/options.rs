use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use rocksdb::Cache;
use strata_core::{ShardId, ShardInfo, StrataLsn};
use typed_store::rocks::{default_db_options, get_block_options};

use super::StrataIndexCfNames;
use super::blob::blob_versions_cf_options;
use super::segment::gc_overlay::segment_gc_overlay_cf_options;

const SEGMENT_STATES_BLOCK_CACHE_BYTES: usize = 64 << 20;
const SEGMENT_STATES_BLOCK_SIZE_BYTES: usize = 4 << 10;

pub(crate) fn cf_options(
    cf_names: &StrataIndexCfNames,
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
) -> Vec<(String, rocksdb::Options)> {
    let mut options = Vec::new();
    for cf in cf_names.as_strs() {
        let cf_options = if cf == cf_names.blob_versions {
            blob_versions_cf_options(Arc::clone(&compact_safe_lsn), Arc::clone(&shard_infos))
        } else if cf == cf_names.segment_gc_overlay {
            segment_gc_overlay_cf_options()
        } else if cf == cf_names.segment_states {
            segment_states_cf_options()
        } else {
            default_db_options().options
        };
        options.push((cf.to_owned(), cf_options));
    }
    options
}

fn segment_states_cf_options() -> rocksdb::Options {
    let mut options = default_db_options().options;
    options.set_block_based_table_factory(&get_block_options(
        &Cache::new_lru_cache(SEGMENT_STATES_BLOCK_CACHE_BYTES),
        Some(SEGMENT_STATES_BLOCK_SIZE_BYTES),
        Some(true),
    ));
    options
}
