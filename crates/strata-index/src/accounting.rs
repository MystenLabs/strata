use serde::{Deserialize, Serialize};
use strata_accounting::{ActiveDeltaLogReadCursor, ActiveDeltaLogState, Manifest};
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountingIndexValue {
    Manifest(Manifest),
    ActiveDeltaLogState(ActiveDeltaLogState),
    ActiveDeltaLogConsumedCursor(ActiveDeltaLogReadCursor),
}

impl AccountingIndexValue {
    pub(crate) fn key(&self) -> AccountingIndexKey {
        match self {
            Self::Manifest(_) => AccountingIndexKey::Manifest,
            Self::ActiveDeltaLogState(_) => AccountingIndexKey::ActiveDeltaLogState,
            Self::ActiveDeltaLogConsumedCursor(_) => {
                AccountingIndexKey::ActiveDeltaLogConsumedCursor
            }
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
        // owner can commit manifest, active-log cursor, derived stats, ref events, and GC overlay
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
        // row; callers must place it in the same batch as the derived segment stats/ref/overlay rows
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
