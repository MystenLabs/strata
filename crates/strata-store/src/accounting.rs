use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use strata_core::{
    BlobKey, BlobLifecycle, BlobLifecycleState, BlobVersionState, Epoch, RecordRef, SegmentId,
    SegmentState, SegmentStats, ShardKey, StrataLsn, VersionOp, VersionState,
};
use strata_index::StrataIndex;

use crate::{
    Error, Result,
    stats::{
        add_live_lifecycle_stats, expire_live_lifecycle_stats_through, lifecycle_is_expired,
        remove_live_lifecycle_stats,
    },
};

#[derive(Debug)]
pub(crate) enum AccountingCommand {
    Run,
    Shutdown,
}

#[derive(Debug)]
pub(crate) struct AccountingWorker {
    pub(crate) index: StrataIndex,
    pub(crate) interval: Duration,
    pub(crate) command_rx: mpsc::Receiver<AccountingCommand>,
    pub(crate) run_lock: Arc<Mutex<()>>,
}

impl AccountingWorker {
    pub(crate) fn run(self) {
        loop {
            match self.command_rx.recv_timeout(self.interval) {
                Ok(AccountingCommand::Run) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    let _ = run_accounting(&self.index);
                }
                Ok(AccountingCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }
}

pub(crate) fn run_accounting(index: &StrataIndex) -> Result<()> {
    let accounted_lsn = index.get_accounted_lsn()?;
    let durable_lsn = index.get_durable_lsn()?;
    if accounted_lsn >= durable_lsn {
        return Ok(());
    }

    let current_epoch = index
        .latest_epoch_at_lsn(accounted_lsn)?
        .map(|(_, epoch)| epoch)
        .ok_or(Error::EpochNotInitialized)?;
    let unaccounted = index
        .iter_unaccounted_lsn_ops_from(accounted_lsn.saturating_add(1))?
        .into_iter()
        .filter(|(lsn, _)| *lsn <= durable_lsn)
        .collect::<BTreeMap<_, _>>();
    let epoch_changes = index
        .iter_epoch_changes_from(accounted_lsn.saturating_add(1))?
        .into_iter()
        .filter(|(lsn, _)| *lsn <= durable_lsn)
        .collect::<BTreeMap<_, _>>();

    let mut context = AccountingContext {
        index,
        current_epoch,
        stats: BTreeMap::new(),
        states: BTreeMap::new(),
    };
    let mut consumed_lsns = Vec::new();
    for lsn in accounted_lsn.saturating_add(1)..=durable_lsn {
        if let Some(epoch) = epoch_changes.get(&lsn).copied() {
            context.current_epoch = epoch;
            context.expire_live_refs(epoch)?;
        }
        if let Some(key) = unaccounted.get(&lsn) {
            context.apply_unaccounted_op(lsn, key)?;
            consumed_lsns.push(lsn);
        }
    }

    let AccountingContext { stats, .. } = context;
    let mut batch = index.batch();
    for (segment_id, stats) in stats {
        index.put_segment_stats_batch(&mut batch, segment_id, &stats)?;
    }
    index.remove_unaccounted_lsn_ops_batch(&mut batch, &consumed_lsns)?;
    index.put_accounted_lsn_batch(&mut batch, durable_lsn)?;
    batch.write().map_err(strata_index::Error::from)?;
    index.flush_wal(true)?;
    index.set_blob_compact_safe_lsn(durable_lsn);
    Ok(())
}

struct AccountingContext<'a> {
    index: &'a StrataIndex,
    current_epoch: Epoch,
    stats: BTreeMap<SegmentId, SegmentStats>,
    states: BTreeMap<SegmentId, SegmentState>,
}

impl AccountingContext<'_> {
    fn apply_unaccounted_op(&mut self, lsn: StrataLsn, key: &BlobKey) -> Result<()> {
        let Some(state) = self.index.get_blob_state(key)? else {
            return Ok(());
        };
        let version_ops = state.versions.ops_at_lsn(lsn);
        if !version_ops.is_empty() {
            self.apply_blob_op(lsn, &state, &version_ops)?;
        }

        let lifecycle_ops = state.lifecycle.ops_at_lsn(lsn);
        if !lifecycle_ops.is_empty() {
            self.apply_lifecycle_op(lsn, &state)?;
        }
        Ok(())
    }

    fn apply_blob_op(
        &mut self,
        lsn: StrataLsn,
        state: &BlobVersionState,
        version_ops: &[VersionOp],
    ) -> Result<()> {
        for shard in version_ops.iter().map(|op| op.shard) {
            let before = resolve_accounted_ref_at(
                &state.versions,
                &state.lifecycle,
                shard,
                lsn.saturating_sub(1),
            );
            let after = resolve_accounted_ref_at(&state.versions, &state.lifecycle, shard, lsn);
            self.apply_transition(before.as_ref(), after.as_ref())?;
        }
        Ok(())
    }

    fn apply_lifecycle_op(&mut self, lsn: StrataLsn, state: &BlobVersionState) -> Result<()> {
        for shard in shards_in_version_state(&state.versions) {
            let before = resolve_accounted_ref_at(
                &state.versions,
                &state.lifecycle,
                shard,
                lsn.saturating_sub(1),
            );
            let after = resolve_accounted_ref_at(&state.versions, &state.lifecycle, shard, lsn);
            self.apply_transition(before.as_ref(), after.as_ref())?;
        }
        Ok(())
    }

    fn apply_transition(
        &mut self,
        before: Option<&AccountedRef>,
        after: Option<&AccountedRef>,
    ) -> Result<()> {
        let before_ref = before.map(|entry| entry.record_ref);
        let after_ref = after.map(|entry| entry.record_ref);

        if before_ref == after_ref {
            if let (Some(record_ref), Some(before), Some(after)) = (before_ref, before, after) {
                self.update_same_ref(record_ref.segment_id, record_ref.len, before, after)?;
            } else if let (Some(record_ref), Some(before), None) = (before_ref, before, after) {
                self.retire_ref(record_ref.segment_id, record_ref.len, before)?;
            }
            return Ok(());
        }

        if let (Some(record_ref), Some(before)) = (before_ref, before) {
            self.retire_ref(record_ref.segment_id, record_ref.len, before)?;
        }
        if let (Some(record_ref), Some(after)) = (after_ref, after) {
            self.add_ref(record_ref.segment_id, record_ref.len, after)?;
        }
        Ok(())
    }

    fn update_same_ref(
        &mut self,
        segment_id: SegmentId,
        record_len: u64,
        before: &AccountedRef,
        after: &AccountedRef,
    ) -> Result<()> {
        if before.lifecycle == after.lifecycle {
            return Ok(());
        }

        let before_expired = lifecycle_is_expired(before.lifecycle, self.current_epoch);
        let after_expired = lifecycle_is_expired(after.lifecycle, self.current_epoch);
        if before_expired && after_expired {
            return Ok(());
        }

        let state = self.segment_state(segment_id)?.clone();
        let stats = self.segment_stats(segment_id)?;
        match (before_expired, after_expired) {
            (false, false) => {
                remove_live_ref(stats, &state, record_len, before.lifecycle);
                add_live_ref(stats, &state, record_len, after.lifecycle);
            }
            (true, false) => {
                stats.expired_bytes = stats.expired_bytes.saturating_sub(record_len);
                add_live_ref(stats, &state, record_len, after.lifecycle);
            }
            (false, true) => {
                remove_live_ref(stats, &state, record_len, before.lifecycle);
                stats.expired_bytes = stats.expired_bytes.saturating_add(record_len);
            }
            (true, true) => {}
        }
        Ok(())
    }

    fn retire_ref(
        &mut self,
        segment_id: SegmentId,
        record_len: u64,
        entry: &AccountedRef,
    ) -> Result<()> {
        if lifecycle_is_expired(entry.lifecycle, self.current_epoch) {
            return Ok(());
        }
        let state = self.segment_state(segment_id)?.clone();
        let stats = self.segment_stats(segment_id)?;
        remove_live_ref(stats, &state, record_len, entry.lifecycle);
        stats.tombstoned_bytes = stats.tombstoned_bytes.saturating_add(record_len);
        Ok(())
    }

    fn add_ref(
        &mut self,
        segment_id: SegmentId,
        record_len: u64,
        entry: &AccountedRef,
    ) -> Result<()> {
        let state = self.segment_state(segment_id)?.clone();
        let is_expired = lifecycle_is_expired(entry.lifecycle, self.current_epoch);
        let stats = self.segment_stats(segment_id)?;
        stats.total_bytes = stats.total_bytes.saturating_add(record_len);
        if is_expired {
            stats.expired_bytes = stats.expired_bytes.saturating_add(record_len);
        } else {
            add_live_ref(stats, &state, record_len, entry.lifecycle);
        }
        Ok(())
    }

    fn expire_live_refs(&mut self, epoch: Epoch) -> Result<()> {
        for (segment_id, state) in self.index.iter_segment_states()? {
            let stats = self.segment_stats(segment_id)?;
            expire_live_lifecycle_stats_through(stats, state.placement_class, epoch);
        }
        Ok(())
    }

    fn segment_state(&mut self, segment_id: SegmentId) -> Result<&SegmentState> {
        if !self.states.contains_key(&segment_id) {
            let state = self
                .index
                .get_segment_state(segment_id)?
                .ok_or(Error::AccountingMissingSegmentState { segment_id })?;
            self.states.insert(segment_id, state);
        }
        Ok(self.states.get(&segment_id).expect("state inserted above"))
    }

    fn segment_stats(&mut self, segment_id: SegmentId) -> Result<&mut SegmentStats> {
        if !self.stats.contains_key(&segment_id) {
            let stats = self
                .index
                .get_segment_stats(segment_id)?
                .unwrap_or_default();
            self.stats.insert(segment_id, stats);
        }
        Ok(self
            .stats
            .get_mut(&segment_id)
            .expect("stats inserted above"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AccountedRef {
    payload_lsn: StrataLsn,
    record_ref: RecordRef,
    lifecycle: Option<BlobLifecycle>,
}

fn resolve_accounted_ref_at(
    state: &VersionState,
    lifecycle_state: &BlobLifecycleState,
    shard: ShardKey,
    max_lsn: StrataLsn,
) -> Option<AccountedRef> {
    let head = resolve_head_at(state, shard, max_lsn)?;
    if head.entry.is_tombstone() {
        return None;
    }
    let record_ref = head.entry.record_ref?;
    let lifecycle = lifecycle_state.resolve_at(max_lsn);
    if lifecycle
        .tombstone_lsn
        .is_some_and(|tombstone_lsn| tombstone_lsn > head.head_lsn)
    {
        return None;
    }
    Some(AccountedRef {
        payload_lsn: head.head_lsn,
        record_ref,
        lifecycle: lifecycle.lifetime.map(|lifetime| lifetime.lifecycle),
    })
}

fn resolve_head_at(
    state: &VersionState,
    shard: ShardKey,
    max_lsn: StrataLsn,
) -> Option<strata_core::ShardHead> {
    let mut view = VersionState::default();
    if let Some(head) = state.heads.get(&shard)
        && head.head_lsn <= max_lsn
    {
        view.heads.insert(shard, head.clone());
    }
    view.tail = state
        .tail
        .iter()
        .filter(|op| op.shard == shard && op.lsn() <= max_lsn)
        .cloned()
        .collect();
    view.resolve_head(shard)
}

fn shards_in_version_state(state: &VersionState) -> Vec<ShardKey> {
    let mut shards = state.heads.keys().copied().collect::<Vec<_>>();
    shards.extend(state.tail.iter().map(|op| op.shard));
    shards.sort();
    shards.dedup();
    shards
}

fn add_live_ref(
    stats: &mut SegmentStats,
    state: &SegmentState,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    add_live_lifecycle_stats(stats, state.placement_class, record_len, lifecycle);
}

fn remove_live_ref(
    stats: &mut SegmentStats,
    state: &SegmentState,
    record_len: u64,
    lifecycle: Option<BlobLifecycle>,
) {
    remove_live_lifecycle_stats(stats, state.placement_class, record_len, lifecycle);
}
