//! Accounting processor lifetime and ordered pass execution.

use std::time::Instant;

use strata_accounting::{AccountingIndex, AccountingIndexConfig};
use strata_core::ShardCleanupState;
use strata_index::StrataIndex;

use crate::{Result, config::StrataStoreConfig, metrics::StrataStoreMetrics};

use super::{AccountingPassMode, AccountingPassPolicy};

/// Stateful accounting pipeline for one open store.
pub(super) struct AccountingProcessor {
    pub(super) config: StrataStoreConfig,
    pub(super) store_index: StrataIndex,
    pub(super) accounting_index: AccountingIndex,
    pub(super) metrics: StrataStoreMetrics,
    last_forced_run: Instant,
}

impl AccountingProcessor {
    /// Opens the accounting index from the manifest stored in the store index.
    pub(super) fn open(
        config: StrataStoreConfig,
        store_index: StrataIndex,
        metrics: StrataStoreMetrics,
    ) -> Result<Self> {
        let manifest = store_index.get_accounting_index_manifest()?;
        let accounting_index = AccountingIndex::open_with_manifest(
            AccountingIndexConfig::new(
                config.accounting_index_dir(),
                config.accounting_partition_count(),
            ),
            manifest,
        )?;
        Ok(Self {
            config,
            store_index,
            accounting_index,
            metrics,
            last_forced_run: Instant::now(),
        })
    }

    /// Runs one accounting request using a single explicit pass policy.
    pub(super) fn run(&mut self, mode: AccountingPassMode) -> Result<bool> {
        self.refresh_accounting_index()?;
        let (policy, reset_forced_clock) = match mode {
            AccountingPassMode::Nudged => (AccountingPassPolicy::NUDGED, false),
            AccountingPassMode::Materialize => (AccountingPassPolicy::MATERIALIZE, true),
            AccountingPassMode::Maintenance => {
                let pending_shard_drop = self
                    .store_index
                    .iter_shard_cleanup_jobs()?
                    .into_iter()
                    .any(|job| job.state == ShardCleanupState::PendingAccounting);
                let force = pending_shard_drop
                    || self.last_forced_run.elapsed()
                        >= self.config.accounting_maintenance_interval;
                (AccountingPassPolicy::maintenance(force), force)
            }
        };
        let should_nudge_gc = self.run_pass(policy)?;
        if reset_forced_clock {
            self.last_forced_run = Instant::now();
        }
        Ok(should_nudge_gc)
    }

    /// Executes the accounting stages in their durability order.
    pub(super) fn run_pass(&mut self, policy: AccountingPassPolicy) -> Result<bool> {
        let mut should_nudge_gc = self.ingest_active_delta_log(policy.force_ingest)?;
        should_nudge_gc |= self.compact_accounting_index(
            policy.force_delta_compaction,
            policy.force_major_compaction,
        )?;
        if policy.materialize_shard_drops {
            should_nudge_gc |= self.materialize_shard_drops()?;
        }
        Ok(should_nudge_gc)
    }

    /// Refreshes this handle after another serialized publisher advances the durable manifest.
    fn refresh_accounting_index(&mut self) -> Result<()> {
        let Some(manifest) = self.store_index.get_accounting_index_manifest()? else {
            return Ok(());
        };
        if manifest.generation != self.accounting_index.manifest().generation {
            self.accounting_index.apply_manifest(manifest)?;
        }
        Ok(())
    }
}

#[cfg(test)]
/// Runs the processor once from tests without waiting for the background interval.
pub(crate) fn run_accounting_once(
    store_index: &StrataIndex,
    config: &StrataStoreConfig,
    force: bool,
) -> Result<()> {
    let mut processor = AccountingProcessor::open(
        config.clone(),
        store_index.clone(),
        StrataStoreMetrics::default(),
    )?;
    processor
        .run_pass(AccountingPassPolicy::test(force))
        .map(|_| ())
}

#[cfg(test)]
pub(crate) fn run_accounting_nudged_once(
    store_index: &StrataIndex,
    config: &StrataStoreConfig,
) -> Result<()> {
    let mut processor = AccountingProcessor::open(
        config.clone(),
        store_index.clone(),
        StrataStoreMetrics::default(),
    )?;
    processor.run(AccountingPassMode::Nudged).map(|_| ())
}

#[cfg(test)]
pub(crate) fn run_accounting_materializing_once(
    store_index: &StrataIndex,
    config: &StrataStoreConfig,
) -> Result<()> {
    let mut processor = AccountingProcessor::open(
        config.clone(),
        store_index.clone(),
        StrataStoreMetrics::default(),
    )?;
    processor.run(AccountingPassMode::Materialize).map(|_| ())
}
