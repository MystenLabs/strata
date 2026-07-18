use std::sync::Arc;

use strata_core::{
    BlobKey, BlobVersionState, Epoch, GcRelocation, RecordRef, SegmentGcOverlay, SegmentId,
    SegmentRefEvent, SegmentRefEventKey, SegmentState, ShardId, ShardInfo, StoreStateKey,
    StrataLsn,
};
use typed_store::rocks::{DBBatch, DBMap, RocksDB};

use crate::{Error, Result};

use super::{AccountingIndexKey, AccountingIndexValue, StrataIndex, StrataIndexCfNames};

impl StrataIndex {
    pub fn db(&self) -> &Arc<RocksDB> {
        &self.db
    }

    pub fn cf_names(&self) -> &StrataIndexCfNames {
        &self.cf_names
    }

    pub fn batch(&self) -> DBBatch {
        self.blob_versions.batch()
    }

    pub fn blob_versions(&self) -> &DBMap<BlobKey, BlobVersionState> {
        &self.blob_versions
    }

    pub fn segment_states(&self) -> &DBMap<SegmentId, SegmentState> {
        &self.segment_states
    }

    pub fn segment_ref_events(&self) -> &DBMap<SegmentRefEventKey, SegmentRefEvent> {
        &self.segment_ref_events
    }

    pub fn segment_gc_overlay(&self) -> &DBMap<SegmentId, SegmentGcOverlay> {
        &self.segment_gc_overlay
    }

    pub fn gc_relocations(&self) -> &DBMap<RecordRef, GcRelocation> {
        &self.gc_relocations
    }

    pub fn gc_reclaim_pending(&self) -> &DBMap<(SegmentId, StrataLsn), u64> {
        &self.gc_reclaim_pending
    }

    pub fn shards(&self) -> &DBMap<ShardId, ShardInfo> {
        &self.shards
    }

    pub fn store_state(&self) -> &DBMap<StoreStateKey, StrataLsn> {
        &self.store_state
    }

    pub fn epoch_changes(&self) -> &DBMap<StrataLsn, Epoch> {
        &self.epoch_changes
    }

    pub fn unaccounted_lsn_ops(&self) -> &DBMap<StrataLsn, BlobKey> {
        &self.unaccounted_lsn_ops
    }

    pub fn accounting_index(&self) -> &DBMap<AccountingIndexKey, AccountingIndexValue> {
        &self.accounting_index
    }

    pub fn flush_wal(&self, sync: bool) -> Result<()> {
        match self.db.as_ref() {
            RocksDB::DB(db) => db
                .underlying
                .flush_wal(sync)
                .map_err(|error| Error::RocksDb(error.into_string())),
            RocksDB::OptimisticTransactionDB(_) => Err(Error::RocksDb(
                "flush_wal is not supported for optimistic transaction RocksDB".to_owned(),
            )),
        }
    }
}
