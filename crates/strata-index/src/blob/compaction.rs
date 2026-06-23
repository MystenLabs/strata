use std::{
    collections::BTreeMap,
    ffi::{CStr, CString},
    sync::{Arc, RwLock},
};

use rocksdb::{
    MergeOperands,
    compaction_filter::{CompactionFilter, Decision},
    compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
};
use strata_core::{BlobVersionState, ShardId, ShardInfo, StrataLsn};
use typed_store::rocks::default_db_options;

use super::merge::{
    full_merge_blob_versions, partial_merge_blob_versions, prune_obsolete_shard_versions,
};

pub(crate) fn blob_versions_cf_options(
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
) -> rocksdb::Options {
    let mut options = default_db_options().options;
    let merge_shard_infos = shard_infos.clone();
    options.set_merge_operator(
        "strata-blob-versions-merge",
        move |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
            full_merge_blob_versions(
                compact_safe_lsn.clone(),
                merge_shard_infos.clone(),
                existing_value,
                operands,
            )
        },
        move |_key: &[u8], _existing_value: Option<&[u8]>, operands: &MergeOperands| {
            partial_merge_blob_versions(operands)
        },
    );
    options.set_compaction_filter_factory(BlobVersionsCompactionFilterFactory {
        shard_infos,
        name: CString::new("strata-blob-versions-compaction-filter").unwrap(),
    });
    options
}

struct BlobVersionsCompactionFilterFactory {
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    name: CString,
}

impl CompactionFilterFactory for BlobVersionsCompactionFilterFactory {
    type Filter = BlobVersionsCompactionFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        BlobVersionsCompactionFilter {
            shard_infos: Arc::clone(&self.shard_infos),
            scratch: Vec::new(),
            name: CString::new("strata-blob-versions-compaction-filter").unwrap(),
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

struct BlobVersionsCompactionFilter {
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    scratch: Vec<u8>,
    name: CString,
}

impl CompactionFilter for BlobVersionsCompactionFilter {
    fn filter<'a>(&'a mut self, _level: u32, _key: &[u8], value: &[u8]) -> Decision<'a> {
        let Ok(mut state) = bcs::from_bytes::<BlobVersionState>(value) else {
            return Decision::Keep;
        };
        let shard_infos = self
            .shard_infos
            .read()
            .expect("shard info cache lock poisoned")
            .clone();
        if !prune_obsolete_shard_versions(&mut state.versions, &shard_infos) {
            return Decision::Keep;
        }
        if state.is_empty() {
            return Decision::Remove;
        }
        match bcs::to_bytes(&state) {
            Ok(bytes) => {
                self.scratch = bytes;
                Decision::Change(self.scratch.as_slice())
            }
            Err(_) => Decision::Keep,
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}
