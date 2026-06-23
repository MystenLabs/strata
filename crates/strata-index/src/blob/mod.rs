mod compaction;
pub(crate) mod merge;

use std::collections::{BTreeMap, BTreeSet};

use strata_core::{
    BlobEntry, BlobKey, BlobLifecycleHead, BlobLifecycleMergeOp, BlobLifecycleOp,
    BlobLifecycleState, BlobVersionKey, BlobVersionState, ShardHead, ShardKey, StrataLsn,
    VersionMergeOp, VersionOp, VersionState,
};
use typed_store::{Map, rocks::DBBatch};

use crate::Result;

use self::merge::{
    BlobVersionMergeOp, EncodedBlobVersionMergeOperand, encode_blob_version_merge_operand,
    remove_lifecycle_lsns_from_state, remove_version_lsns_from_state,
};
use super::{STANDALONE_SHARD, StrataIndex};

pub(crate) use compaction::blob_versions_cf_options;

impl StrataIndex {
    pub fn set_blob_compact_safe_lsn(&self, lsn: StrataLsn) {
        // This is a one way frontier published after the matching durable_lsn update is WAL synced.
        // Merge operators use it to decide which per blob tails can be folded into heads, moving it
        // backward would make the same RocksDB value compact differently across later merges.
        let mut frontier = self
            .blob_compact_safe_lsn
            .write()
            .expect("blob version compaction frontier lock poisoned");
        *frontier = (*frontier).max(lsn);
    }

    pub fn get_blob_state(&self, key: &BlobKey) -> Result<Option<BlobVersionState>> {
        Ok(self.blob_versions.get(key)?)
    }

    pub fn get_blob_version_state(&self, key: &BlobKey) -> Result<Option<VersionState>> {
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| (!state.versions.is_empty()).then_some(state.versions)))
    }

    pub fn get_blob_lifecycle_state(&self, key: &BlobKey) -> Result<Option<BlobLifecycleState>> {
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| (!state.lifecycle.is_empty()).then_some(state.lifecycle)))
    }

    pub fn resolve_blob_lifecycle_at(
        &self,
        key: &BlobKey,
        max_lsn: StrataLsn,
    ) -> Result<BlobLifecycleHead> {
        Ok(self
            .get_blob_state(key)?
            .map_or_else(BlobLifecycleHead::default, |state| {
                state.lifecycle.resolve_at(max_lsn)
            }))
    }

    pub fn resolve_blob_head(&self, key: &BlobKey, shard: ShardKey) -> Result<Option<ShardHead>> {
        // The cache check is part of stale write prevention for readers as well as writers. If a
        // shard generation was dropped, any packed value that still contains its old head is treated
        // as physically unreachable even before RocksDB compaction has pruned it.
        if self.shard_generation_is_cached_obsolete(shard) {
            return Ok(None);
        }
        Ok(self
            .get_blob_state(key)?
            .and_then(|state| state.versions.resolve_head(shard)))
    }

    pub fn get_blob_entry(&self, key: &BlobKey) -> Result<Option<BlobEntry>> {
        Ok(self.latest_blob_version(key)?.map(|(_, entry)| entry))
    }

    pub fn contains_blob(&self, key: &BlobKey) -> Result<bool> {
        Ok(self.latest_blob_version(key)?.is_some())
    }

    pub fn put_blob_entry(&self, key: &BlobKey, entry: &BlobEntry) -> Result<()> {
        let mut batch = self.batch();
        self.put_blob_entry_batch(&mut batch, key, entry)?;
        batch.write()?;
        Ok(())
    }

    pub fn put_blob_entry_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        self.put_blob_version_batch(batch, key, entry)
    }

    fn put_blob_state_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        state: &BlobVersionState,
    ) -> Result<()> {
        if state.is_empty() {
            batch.delete_batch(&self.blob_versions, [key.clone()])?;
        } else {
            batch.insert_batch(&self.blob_versions, [(key, state)])?;
        }
        Ok(())
    }

    pub fn put_blob_version_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        self.merge_blob_version_batch(batch, key, STANDALONE_SHARD, entry)
    }

    pub fn merge_blob_version_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        shard: ShardKey,
        entry: &BlobEntry,
    ) -> Result<()> {
        let op = VersionMergeOp::Append(VersionOp {
            shard,
            entry: entry.clone(),
        });
        self.apply_blob_version_merge_op_batch(batch, key, op)
    }

    pub fn apply_blob_version_merge_op_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        op: VersionMergeOp,
    ) -> Result<()> {
        let operand = encode_blob_version_merge_operand(EncodedBlobVersionMergeOperand::Op(
            BlobVersionMergeOp::Version(op),
        ))?;
        batch.partial_merge_batch(&self.blob_versions, [(key, operand)])?;
        Ok(())
    }

    pub fn map_blob_ref_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        shard: ShardKey,
        payload_lsn: StrataLsn,
        from: strata_core::RecordRef,
        to: strata_core::RecordRef,
    ) -> Result<()> {
        // MapRef is recorded as ordered history, not resolved here. The index does not know whether
        // `from` is still in the tail, already folded into a head, or hidden behind later lifecycle
        // metadata; resolving it early would couple this write path to the same state-folding logic
        // used by accounting and GC.
        self.apply_blob_version_merge_op_batch(
            batch,
            key,
            VersionMergeOp::MapRef {
                shard,
                payload_lsn,
                from,
                to,
            },
        )
    }

    pub fn apply_blob_lifecycle_merge_op_batch(
        &self,
        batch: &mut DBBatch,
        key: &BlobKey,
        op: BlobLifecycleMergeOp,
    ) -> Result<()> {
        // Lifecycle and tombstone operations are metadata-only, but they must share the same packed
        // value as payload versions. That keeps reads, rollback, and accounting from observing a
        // payload transition without the lifetime/tombstone transition at the same LSN boundary.
        let operand = encode_blob_version_merge_operand(EncodedBlobVersionMergeOperand::Op(
            BlobVersionMergeOp::Lifecycle(op),
        ))?;
        batch.partial_merge_batch(&self.blob_versions, [(key, operand)])?;
        Ok(())
    }

    pub fn blob_version_ops_at_lsn(&self, key: &BlobKey, lsn: StrataLsn) -> Result<Vec<VersionOp>> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok(Vec::new());
        };

        Ok(state.versions.ops_at_lsn(lsn))
    }

    pub fn blob_ops_at_lsn(
        &self,
        key: &BlobKey,
        lsn: StrataLsn,
    ) -> Result<(Vec<VersionOp>, Vec<BlobLifecycleOp>)> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok((Vec::new(), Vec::new()));
        };

        Ok((
            state.versions.ops_at_lsn(lsn),
            state.lifecycle.ops_at_lsn(lsn),
        ))
    }

    pub fn blob_lifecycle_ops_at_lsn(
        &self,
        key: &BlobKey,
        lsn: StrataLsn,
    ) -> Result<Vec<BlobLifecycleOp>> {
        let Some(state) = self.get_blob_state(key)? else {
            return Ok(Vec::new());
        };

        Ok(state.lifecycle.ops_at_lsn(lsn))
    }

    pub fn latest_blob_version(
        &self,
        key: &BlobKey,
    ) -> Result<Option<(BlobVersionKey, BlobEntry)>> {
        let Some(state) = self.get_blob_version_state(key)? else {
            return Ok(None);
        };

        let mut latest = state
            .heads
            .get(&STANDALONE_SHARD)
            .map(|head| (head.head_lsn, head.entry.clone()));

        for op in state.tail.iter().filter(|op| op.shard == STANDALONE_SHARD) {
            if latest
                .as_ref()
                .is_none_or(|(latest_lsn, _)| op.lsn() > *latest_lsn)
            {
                latest = Some((op.lsn(), op.entry.clone()));
            }
        }

        Ok(latest.map(|(lsn, entry)| {
            (
                BlobVersionKey {
                    key: key.clone(),
                    lsn,
                },
                entry,
            )
        }))
    }

    pub fn get_blob_version(&self, key: &BlobVersionKey) -> Result<Option<BlobEntry>> {
        self.get_blob_version_for_shard(key, STANDALONE_SHARD)
    }

    pub fn get_blob_version_for_shard(
        &self,
        key: &BlobVersionKey,
        shard: ShardKey,
    ) -> Result<Option<BlobEntry>> {
        let Some(state) = self.get_blob_version_state(&key.key)? else {
            return Ok(None);
        };

        if let Some(op) = state
            .tail
            .iter()
            .rev()
            .find(|op| op.shard == shard && op.lsn() == key.lsn)
        {
            return Ok(Some(op.entry.clone()));
        }

        Ok(state
            .heads
            .get(&shard)
            .into_iter()
            .find(|head| head.head_lsn == key.lsn)
            .map(|head| head.entry.clone()))
    }

    pub fn remove_blob_versions_batch(
        &self,
        batch: &mut DBBatch,
        hidden_versions: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        self.remove_blob_versions_for_shard_batch(batch, STANDALONE_SHARD, hidden_versions)
    }

    pub fn remove_blob_versions_for_shard_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
        hidden_versions: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        let mut by_key = BTreeMap::<BlobKey, BTreeSet<StrataLsn>>::new();
        for (key, lsn) in hidden_versions {
            by_key.entry(key.clone()).or_default().insert(*lsn);
        }

        // Rollback groups by blob key because packed values are the unit of replacement. The exact
        // LSN filter removes only hidden payload versions; lifecycle/tombstone state is left alone
        // for callers that intentionally roll back payload refs without metadata.
        for (key, lsns) in by_key {
            let Some(mut state) = self.get_blob_state(&key)? else {
                continue;
            };
            state
                .versions
                .tail
                .retain(|op| op.shard != shard || !lsns.contains(&op.lsn()));
            state
                .versions
                .heads
                .retain(|candidate, head| *candidate != shard || !lsns.contains(&head.head_lsn));
            self.put_blob_state_batch(batch, &key, &state)?;
        }

        Ok(())
    }

    pub fn remove_blob_ops_at_lsns_batch(
        &self,
        batch: &mut DBBatch,
        hidden_ops: &[(BlobKey, StrataLsn)],
    ) -> Result<()> {
        let mut by_key = BTreeMap::<BlobKey, BTreeSet<StrataLsn>>::new();
        for (key, lsn) in hidden_ops {
            by_key.entry(key.clone()).or_default().insert(*lsn);
        }

        // This path rolls back complete blob operations. Payload and lifecycle/tombstone histories
        // are edited together because accounting consumed them as one logical LSN stream.
        for (key, lsns) in by_key {
            let Some(mut state) = self.get_blob_state(&key)? else {
                continue;
            };
            remove_version_lsns_from_state(&mut state.versions, &lsns);
            remove_lifecycle_lsns_from_state(&mut state.lifecycle, &lsns);
            self.put_blob_state_batch(batch, &key, &state)?;
        }

        Ok(())
    }

    pub fn iter_blob_versions(&self) -> Result<Vec<(BlobVersionKey, BlobEntry)>> {
        let mut versions = Vec::new();
        for result in self.blob_versions.safe_iter()? {
            let (key, state) = result?;
            for head in state.versions.heads.values() {
                versions.push((
                    BlobVersionKey {
                        key: key.clone(),
                        lsn: head.head_lsn,
                    },
                    head.entry.clone(),
                ));
            }
            for op in state.versions.tail {
                versions.push((
                    BlobVersionKey {
                        key: key.clone(),
                        lsn: op.lsn(),
                    },
                    op.entry.clone(),
                ));
            }
        }
        versions.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(versions)
    }
}
