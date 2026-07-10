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
    let merge_compact_safe_lsn = Arc::clone(&compact_safe_lsn);
    let merge_shard_infos = Arc::clone(&shard_infos);
    options.set_merge_operator(
        "strata-blob-versions-merge",
        move |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
            full_merge_blob_versions(
                Arc::clone(&merge_compact_safe_lsn),
                Arc::clone(&merge_shard_infos),
                existing_value,
                operands,
            )
        },
        move |_key: &[u8], _existing_value: Option<&[u8]>, operands: &MergeOperands| {
            partial_merge_blob_versions(operands)
        },
    );
    options.set_compaction_filter_factory(BlobVersionsCompactionFilterFactory {
        compact_safe_lsn,
        shard_infos,
        name: CString::new("strata-blob-versions-compaction-filter").unwrap(),
    });
    options
}

struct BlobVersionsCompactionFilterFactory {
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    name: CString,
}

impl CompactionFilterFactory for BlobVersionsCompactionFilterFactory {
    type Filter = BlobVersionsCompactionFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        BlobVersionsCompactionFilter {
            compact_safe_lsn: Arc::clone(&self.compact_safe_lsn),
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
    compact_safe_lsn: Arc<RwLock<StrataLsn>>,
    shard_infos: Arc<RwLock<BTreeMap<ShardId, ShardInfo>>>,
    scratch: Vec<u8>,
    name: CString,
}

impl CompactionFilter for BlobVersionsCompactionFilter {
    fn filter(&mut self, _level: u32, _key: &[u8], value: &[u8]) -> Decision {
        let Ok(mut state) = bcs::from_bytes::<BlobVersionState>(value) else {
            return Decision::Keep;
        };
        let compact_safe_lsn = *self
            .compact_safe_lsn
            .read()
            .expect("blob version compaction frontier lock poisoned");
        let compacted_lsn = has_lsn_compaction_work(&state, compact_safe_lsn);
        if compacted_lsn {
            state.versions.compact_through(compact_safe_lsn);
            state.lifecycle.compact_through(compact_safe_lsn);
        }
        let shard_infos = self
            .shard_infos
            .read()
            .expect("shard info cache lock poisoned")
            .clone();
        let pruned_shards = prune_obsolete_shard_versions(&mut state.versions, &shard_infos);
        if !compacted_lsn && !pruned_shards {
            return Decision::Keep;
        }
        if state.is_empty() {
            return Decision::Remove;
        }
        match bcs::to_bytes(&state) {
            Ok(bytes) => self.change_value(bytes),
            Err(_) => Decision::Keep,
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

impl BlobVersionsCompactionFilter {
    fn change_value(&mut self, bytes: Vec<u8>) -> Decision {
        self.scratch = bytes;
        let scratch = self.scratch.as_slice();
        // SAFETY: rocksdb 0.22 incorrectly requires a 'static replacement slice. Its callback
        // forwards this pointer directly to RocksDB, which copies it before another call can
        // mutate this factory-created, single-threaded filter. The scratch buffer is owned by the
        // filter and therefore remains allocated for that entire interval.
        let scratch = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(scratch) };
        Decision::Change(scratch)
    }
}

fn has_lsn_compaction_work(state: &BlobVersionState, compact_safe_lsn: StrataLsn) -> bool {
    state
        .versions
        .tail
        .iter()
        .any(|op| op.lsn() <= compact_safe_lsn)
        || state
            .versions
            .maps
            .iter()
            .any(|op| op.publish_lsn <= compact_safe_lsn && op.payload_lsn <= compact_safe_lsn)
        || state
            .lifecycle
            .tail
            .iter()
            .any(|op| op.lsn() <= compact_safe_lsn)
}
