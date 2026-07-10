use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use strata_accounting::{ActiveDeltaLogReadCursor, ActiveDeltaLogState, Manifest};
use strata_core::{
    SegmentRefEvent, SegmentRefEventKey, ShardCleanupJob, ShardKey, StoreStateKey, StrataLsn,
    StrataStoreState,
};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result};

use super::StrataIndex;

pub(crate) const ACCOUNTING_INDEX_MANIFEST_KEY: AccountingIndexKey = AccountingIndexKey::Manifest;
pub const ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_STATE_KEY: AccountingIndexKey =
    AccountingIndexKey::ActiveDeltaLogState;
pub const ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_CONSUMED_CURSOR_KEY: AccountingIndexKey =
    AccountingIndexKey::ActiveDeltaLogConsumedCursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AccountingIndexKey {
    Manifest,
    ActiveDeltaLogState,
    ActiveDeltaLogConsumedCursor,
    ShardCleanup(ShardKey),
}

/// In-memory accounting frontier retained by a long-running GC job.
///
/// This is intentionally not durable. A process crash abandons in-flight GC work, so restart does
/// not need to recover the marker. While the matching guard is alive, ref-event cleanup must retain
/// events with `lsn > accounted_lsn` so the GC publisher can reconcile changes that accounting
/// materialized during the copy phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountingSnapshot {
    /// Last store-global LSN that accounting had folded into GC overlay state when the pin was made.
    ///
    /// GC reconciliation must read segment ref events with `lsn > accounted_lsn` before publishing
    /// moves based on this view.
    pub accounted_lsn: StrataLsn,
}

/// RAII handle that pins segment ref events newer than an accounting frontier.
///
/// Holding this guard does not hold a RocksDB snapshot. It only records the oldest accounting LSN
/// that ref-event cleanup must preserve. Dropping the guard releases the pin.
#[derive(Debug)]
pub struct AccountingSnapshotGuard {
    /// The captured accounting frontier exposed to GC and reconciliation code.
    snapshot: AccountingSnapshot,
    /// Opaque entry in `AccountingSnapshotPins` removed when the guard is dropped.
    pin_id: u64,
    /// Shared in-memory pin set owned by the opened `StrataIndex`.
    pins: Arc<Mutex<AccountingSnapshotPins>>,
}

impl AccountingSnapshotGuard {
    pub fn snapshot(&self) -> AccountingSnapshot {
        self.snapshot
    }

    pub fn accounted_lsn(&self) -> StrataLsn {
        self.snapshot.accounted_lsn
    }
}

impl Drop for AccountingSnapshotGuard {
    fn drop(&mut self) {
        if let Ok(mut pins) = self.pins.lock() {
            pins.remove(self.pin_id);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct AccountingSnapshotPins {
    /// Next opaque pin identifier. Exhausting this would require creating 2^64 live pins.
    next_pin_id: u64,
    /// Active GC accounting frontiers keyed by opaque pin id.
    pins: BTreeMap<u64, StrataLsn>,
}

impl AccountingSnapshotPins {
    fn insert(&mut self, accounted_lsn: StrataLsn) -> u64 {
        let pin_id = self.next_pin_id;
        self.next_pin_id = self
            .next_pin_id
            .checked_add(1)
            .expect("exhausted accounting snapshot pin ids");
        self.pins.insert(pin_id, accounted_lsn);
        pin_id
    }

    fn remove(&mut self, pin_id: u64) {
        self.pins.remove(&pin_id);
    }

    fn min_accounted_lsn(&self) -> Option<StrataLsn> {
        self.pins.values().copied().min()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountingRefEvent {
    /// Segment-local event identity, including physical segment, source LSN, and record offset.
    pub key: SegmentRefEventKey,
    /// Accounting transition observed for that physical record.
    pub event: SegmentRefEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountingIndexValue {
    Manifest(Manifest),
    ActiveDeltaLogState(ActiveDeltaLogState),
    ActiveDeltaLogConsumedCursor(ActiveDeltaLogReadCursor),
    ShardCleanupJob(ShardCleanupJob),
}

impl AccountingIndexValue {
    pub(crate) fn key(&self) -> AccountingIndexKey {
        match self {
            Self::Manifest(_) => AccountingIndexKey::Manifest,
            Self::ActiveDeltaLogState(_) => AccountingIndexKey::ActiveDeltaLogState,
            Self::ActiveDeltaLogConsumedCursor(_) => {
                AccountingIndexKey::ActiveDeltaLogConsumedCursor
            }
            Self::ShardCleanupJob(job) => AccountingIndexKey::ShardCleanup(job.shard),
        }
    }
}

impl StrataIndex {
    pub fn get_accounting_index_value(
        &self,
        key: AccountingIndexKey,
    ) -> Result<Option<AccountingIndexValue>> {
        Ok(self.accounting_index.get(&key)?)
    }

    pub fn put_accounting_index_value_batch(
        &self,
        batch: &mut DBBatch,
        key: AccountingIndexKey,
        value: &AccountingIndexValue,
    ) -> Result<()> {
        // The accounting sidecar stores several singleton rows in one typed column family so the
        // owner can commit manifest, active-log cursor, derived ref events, and GC overlay
        // operands in one RocksDB batch. The key/value shape check prevents a bad caller from
        // publishing a manifest under the cursor key and making recovery skip or replay deltas.
        if value.key() != key {
            return Err(Error::Serialization(format!(
                "accounting index value for {key:?} cannot be stored under {:?}",
                value.key()
            )));
        }
        batch
            .insert_batch(self.accounting_index(), [(&key, value)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn get_accounting_index_manifest(&self) -> Result<Option<Manifest>> {
        match self.get_accounting_index_value(ACCOUNTING_INDEX_MANIFEST_KEY)? {
            Some(AccountingIndexValue::Manifest(manifest)) => Ok(Some(manifest)),
            Some(value) => Err(unexpected_accounting_index_value(
                ACCOUNTING_INDEX_MANIFEST_KEY,
                value,
            )),
            None => Ok(None),
        }
    }

    pub fn put_accounting_index_manifest_batch(
        &self,
        batch: &mut DBBatch,
        manifest: &Manifest,
    ) -> Result<()> {
        // The manifest is the durable root set for sidecar run files. This method only stages the
        // row; callers must place it in the same batch as the derived ref/overlay rows
        // that were produced from that manifest's compaction event batch.
        self.put_accounting_index_value_batch(
            batch,
            ACCOUNTING_INDEX_MANIFEST_KEY,
            &AccountingIndexValue::Manifest(manifest.clone()),
        )
    }

    pub fn put_accounting_index_manifest(&self, manifest: &Manifest) -> Result<()> {
        let mut batch = self.batch();
        self.put_accounting_index_manifest_batch(&mut batch, manifest)?;
        batch.write()?;
        Ok(())
    }

    pub fn get_accounting_active_delta_log_state(&self) -> Result<Option<ActiveDeltaLogState>> {
        match self.get_accounting_index_value(ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_STATE_KEY)? {
            Some(AccountingIndexValue::ActiveDeltaLogState(state)) => Ok(Some(state)),
            Some(value) => Err(unexpected_accounting_index_value(
                ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_STATE_KEY,
                value,
            )),
            None => Ok(None),
        }
    }

    pub fn put_accounting_active_delta_log_state_batch(
        &self,
        batch: &mut DBBatch,
        state: ActiveDeltaLogState,
    ) -> Result<()> {
        self.put_accounting_index_value_batch(
            batch,
            ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_STATE_KEY,
            &AccountingIndexValue::ActiveDeltaLogState(state),
        )
    }

    pub fn get_accounting_active_delta_log_consumed_cursor(
        &self,
    ) -> Result<Option<ActiveDeltaLogReadCursor>> {
        match self
            .get_accounting_index_value(ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_CONSUMED_CURSOR_KEY)?
        {
            Some(AccountingIndexValue::ActiveDeltaLogConsumedCursor(cursor)) => Ok(Some(cursor)),
            Some(value) => Err(unexpected_accounting_index_value(
                ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_CONSUMED_CURSOR_KEY,
                value,
            )),
            None => Ok(None),
        }
    }

    pub fn put_accounting_active_delta_log_consumed_cursor_batch(
        &self,
        batch: &mut DBBatch,
        cursor: ActiveDeltaLogReadCursor,
    ) -> Result<()> {
        // Cursor and manifest move together. Advancing the consumed cursor without the manifest that
        // contains the corresponding delta runs would make recovery skip active-log records whose
        // physical sidecar files are not reachable from the durable root.
        self.put_accounting_index_value_batch(
            batch,
            ACCOUNTING_INDEX_ACTIVE_DELTA_LOG_CONSUMED_CURSOR_KEY,
            &AccountingIndexValue::ActiveDeltaLogConsumedCursor(cursor),
        )
    }

    pub fn get_shard_cleanup_job(&self, shard: ShardKey) -> Result<Option<ShardCleanupJob>> {
        let key = AccountingIndexKey::ShardCleanup(shard);
        match self.get_accounting_index_value(key)? {
            Some(AccountingIndexValue::ShardCleanupJob(job)) => Ok(Some(job)),
            Some(value) => Err(unexpected_accounting_index_value(key, value)),
            None => Ok(None),
        }
    }

    pub fn put_shard_cleanup_job_batch(
        &self,
        batch: &mut DBBatch,
        job: ShardCleanupJob,
    ) -> Result<()> {
        self.put_accounting_index_value_batch(
            batch,
            AccountingIndexKey::ShardCleanup(job.shard),
            &AccountingIndexValue::ShardCleanupJob(job),
        )
    }

    pub fn delete_shard_cleanup_job_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
    ) -> Result<()> {
        batch.delete_batch(
            self.accounting_index(),
            [AccountingIndexKey::ShardCleanup(shard)],
        )?;
        Ok(())
    }

    pub fn iter_shard_cleanup_jobs(&self) -> Result<Vec<ShardCleanupJob>> {
        self.accounting_index
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((
                    AccountingIndexKey::ShardCleanup(_),
                    AccountingIndexValue::ShardCleanupJob(job),
                )) => Some(Ok(job)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    /// Creates an in memory accounting snapshot pin for GC.
    ///
    /// This takes a short lived RocksDB snapshot only to read a consistent `accounted_lsn`, the
    /// returned guard does not retain that RocksDB snapshot. The pin is installed while holding the
    /// same mutex used by ref-event cleanup, which prevents cleanup from deleting events needed by
    /// a newly-created GC guard.
    pub fn create_accounting_snapshot(&self) -> Result<AccountingSnapshotGuard> {
        let mut pins = self
            .accounting_snapshot_pins
            .lock()
            .expect("accounting snapshot pins lock poisoned");
        let db_snapshot = self.db.snapshot();
        let accounted_lsn = self
            .store_state
            .get_with_snapshot(&db_snapshot, &StoreStateKey::AccountedLsn)?
            .unwrap_or_else(|| StrataStoreState::default().accounted_lsn);
        let pin_id = pins.insert(accounted_lsn);
        Ok(AccountingSnapshotGuard {
            snapshot: AccountingSnapshot { accounted_lsn },
            pin_id,
            pins: Arc::clone(&self.accounting_snapshot_pins),
        })
    }

    #[cfg(test)]
    pub(crate) fn min_pinned_accounted_lsn(&self) -> Option<StrataLsn> {
        self.accounting_snapshot_pins
            .lock()
            .expect("accounting snapshot pins lock poisoned")
            .min_accounted_lsn()
    }

    /// Deletes old segment ref events without racing active accounting snapshot creation.
    ///
    /// The caller supplies the largest event LSN it would normally delete, usually the current
    /// accounted LSN. If any GC job is holding an accounting snapshot, this method keeps every event
    /// with `lsn > min_pinned_accounted_lsn` because those are exactly the events the GC publisher
    /// must replay before it can safely publish moved refs.
    ///
    /// Cleanup intentionally holds the pin mutex while it chooses and deletes keys. Otherwise a GC
    /// job could capture an old `accounted_lsn` after cleanup checks the pin set but before cleanup
    /// deletes events needed by that new guard.
    pub fn prune_accounting_ref_events_through_lsn(&self, through_lsn: StrataLsn) -> Result<usize> {
        let pins = self
            .accounting_snapshot_pins
            .lock()
            .expect("accounting snapshot pins lock poisoned");
        let delete_through_lsn = pins
            .min_accounted_lsn()
            .map(|pinned_lsn| pinned_lsn.min(through_lsn))
            .unwrap_or(through_lsn);
        let keys = self
            .segment_ref_events
            .safe_iter()?
            .filter_map(|result| match result {
                Ok((key, _)) if key.lsn <= delete_through_lsn => Some(Ok(key)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        if keys.is_empty() {
            return Ok(0);
        }

        let deleted = keys.len();
        let mut batch = self.batch();
        batch.delete_batch(self.segment_ref_events(), keys)?;
        batch.write()?;
        drop(pins);
        Ok(deleted)
    }

    /// Returns segment ref events published after an active accounting snapshot's frontier.
    pub fn accounting_changes_since(
        &self,
        snapshot: &AccountingSnapshotGuard,
    ) -> Result<Vec<AccountingRefEvent>> {
        self.accounting_changes_since_lsn(snapshot.accounted_lsn())
    }

    /// Returns segment ref events published after `accounted_lsn`.
    ///
    /// Prefer `accounting_changes_since` for GC jobs that hold a guard. This lower-level helper is
    /// useful for tests and callers that already manage ref-event retention.
    pub fn accounting_changes_since_lsn(
        &self,
        accounted_lsn: StrataLsn,
    ) -> Result<Vec<AccountingRefEvent>> {
        let db_snapshot = self.db.snapshot();
        let mut events = self
            .segment_ref_events
            .safe_iter_with_snapshot(&db_snapshot)?
            .filter_map(|result| match result {
                Ok((key, event)) if key.lsn > accounted_lsn => {
                    Some(Ok(AccountingRefEvent { key, event }))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;
        events.sort_by_key(|event| (event.key.lsn, event.key.segment_id, event.key.offset));
        Ok(events)
    }
}

pub(crate) fn unexpected_accounting_index_value(
    expected: AccountingIndexKey,
    actual: AccountingIndexValue,
) -> Error {
    Error::Serialization(format!(
        "accounting index key {expected:?} contained {:?}",
        actual.key()
    ))
}
