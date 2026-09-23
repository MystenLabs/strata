use crate::port::options::{block_options, default_db_options};
use rocksdb::Cache;

use super::StrataIndexCfNames;
use super::manifest::lsm_manifests_cf_options;
use super::segment::gc_summary::segment_gc_summaries_cf_options;

const SEGMENT_STATES_BLOCK_CACHE_BYTES: usize = 64 << 20;
const SEGMENT_STATES_BLOCK_SIZE_BYTES: usize = 4 << 10;

pub(crate) fn cf_options(cf_names: &StrataIndexCfNames) -> Vec<(String, rocksdb::Options)> {
    let mut options = Vec::new();
    for cf in cf_names.as_strs() {
        let cf_options = if cf == cf_names.segment_gc_summaries {
            segment_gc_summaries_cf_options()
        } else if cf == cf_names.segment_states {
            segment_states_cf_options()
        } else if cf == cf_names.lsm_manifests {
            lsm_manifests_cf_options()
        } else {
            default_db_options()
        };
        options.push((cf.to_owned(), cf_options));
    }
    options
}

fn segment_states_cf_options() -> rocksdb::Options {
    let mut options = default_db_options();
    options.set_block_based_table_factory(&block_options(
        &Cache::new_lru_cache(SEGMENT_STATES_BLOCK_CACHE_BYTES),
        Some(SEGMENT_STATES_BLOCK_SIZE_BYTES),
        Some(true),
    ));
    options
}
