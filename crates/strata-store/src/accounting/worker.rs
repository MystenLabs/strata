use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64},
        mpsc,
    },
    time::Duration,
};

use strata_core::StrataLsn;
use strata_index::StrataIndex;

use crate::{config::StrataStoreConfig, gc::GcCommand, metrics::StrataStoreMetrics};

use super::{
    AccountingCommand, AccountingPassMode, AccountingProcessor, rearm_materialization,
    take_pending_run_mode,
};

/// Background accounting loop for one store.
///
/// It serializes accounting maintenance over the shared store index: ingest durable active-log
/// deltas, compact accounting runs, then publish derived events back into the store index.
#[derive(Debug)]
pub(crate) struct AccountingWorker {
    pub(crate) config: StrataStoreConfig,
    pub(crate) store_index: StrataIndex,
    pub(crate) interval: Duration,
    pub(crate) command_rx: mpsc::Receiver<AccountingCommand>,
    pub(crate) pending_request: Arc<AtomicU8>,
    pub(crate) pending_materialize_through_lsn: Arc<AtomicU64>,
    pub(crate) run_lock: Arc<Mutex<()>>,
    pub(crate) gc_txs: Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
    pub(crate) metrics: StrataStoreMetrics,
}

impl AccountingWorker {
    /// Runs until shutdown or channel disconnect.
    pub(crate) fn run(self) {
        let mut processor = AccountingProcessor::open(
            self.config.clone(),
            self.store_index.clone(),
            self.metrics.clone(),
        )
        .ok();
        loop {
            match self.command_rx.recv_timeout(self.interval) {
                Ok(AccountingCommand::Wake) => {
                    let Some((mut mode, mut materialize_through_lsn)) = take_pending_run_mode(
                        &self.pending_request,
                        &self.pending_materialize_through_lsn,
                    ) else {
                        continue;
                    };
                    if let Some(through_lsn) = materialize_through_lsn
                        && self
                            .store_index
                            .get_durable_lsn()
                            .is_ok_and(|durable_lsn| durable_lsn < through_lsn)
                    {
                        // The semantic operation is visible but not crash-safe yet. Preserve its
                        // priority for the sync/seal wakeup that makes the target durable, while
                        // keeping this early wakeup on the cheap ingest-only path.
                        rearm_materialization(
                            &self.pending_request,
                            &self.pending_materialize_through_lsn,
                            through_lsn,
                        );
                        mode = AccountingPassMode::Nudged;
                        materialize_through_lsn = None;
                    }
                    if matches!(mode, AccountingPassMode::Nudged)
                        && let (Ok(durable_lsn), Ok(accounted_lsn)) = (
                            self.store_index.get_durable_lsn(),
                            self.store_index.get_accounted_lsn(),
                        )
                        && lag_materialization_target(
                            durable_lsn,
                            accounted_lsn,
                            self.config.accounting_materialize_lag_threshold,
                        )
                        .is_some()
                    {
                        // Promote only after the persisted durability frontier crosses the lag
                        // threshold. Tracking the sampled durable LSN as the target preserves the
                        // existing retry behavior if this materializing pass cannot reach it.
                        mode = AccountingPassMode::Materialize;
                        materialize_through_lsn = Some(durable_lsn);
                    }
                    // One accounting pass at a time: live-allocation overlay operands are not
                    // idempotent, so duplicate application would corrupt GC summary counters.
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    let succeeded = Self::run_processor(
                        &mut processor,
                        &self.config,
                        &self.store_index,
                        &self.gc_txs,
                        &self.metrics,
                        mode,
                    );
                    if let Some(through_lsn) = materialize_through_lsn {
                        let reached_target = succeeded
                            && matches!(
                                self.store_index.get_accounted_lsn(),
                                Ok(accounted_lsn) if accounted_lsn >= through_lsn
                            );
                        if !reached_target {
                            rearm_materialization(
                                &self.pending_request,
                                &self.pending_materialize_through_lsn,
                                through_lsn,
                            );
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _guard = self.run_lock.lock().expect("accounting run lock poisoned");
                    let _ = Self::run_processor(
                        &mut processor,
                        &self.config,
                        &self.store_index,
                        &self.gc_txs,
                        &self.metrics,
                        AccountingPassMode::Maintenance,
                    );
                }
                Ok(AccountingCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    fn run_processor(
        processor: &mut Option<AccountingProcessor>,
        config: &StrataStoreConfig,
        store_index: &StrataIndex,
        gc_txs: &Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>,
        metrics: &StrataStoreMetrics,
        mode: AccountingPassMode,
    ) -> bool {
        if processor.is_none() {
            // Setup can fail transiently while the active log or manifest is initialized. Retry
            // later instead of killing the worker.
            *processor =
                AccountingProcessor::open(config.clone(), store_index.clone(), metrics.clone())
                    .ok();
        }
        let failed = if let Some(current_processor) = processor.as_mut() {
            match current_processor.run(mode) {
                Ok(should_nudge_gc) => {
                    if should_nudge_gc {
                        nudge_gc(gc_txs);
                    }
                    false
                }
                Err(_) => true,
            }
        } else {
            false
        };
        if failed {
            // A pass may have written immutable runs or committed its RocksDB batch before
            // reporting an error. Reopen from durable metadata on the next pass.
            *processor = None;
        }
        !failed
    }
}

fn lag_materialization_target(
    durable_lsn: StrataLsn,
    accounted_lsn: StrataLsn,
    threshold: StrataLsn,
) -> Option<StrataLsn> {
    (threshold != 0 && durable_lsn.saturating_sub(accounted_lsn) >= threshold)
        .then_some(durable_lsn)
}

fn nudge_gc(gc_txs: &Arc<Mutex<Vec<mpsc::Sender<GcCommand>>>>) {
    let gc_txs = gc_txs.lock().expect("gc tx list lock poisoned");
    for gc_tx in gc_txs.iter() {
        let _ = gc_tx.send(GcCommand::Run);
    }
}

#[cfg(test)]
mod tests {
    use super::lag_materialization_target;

    #[test]
    fn accounting_lag_promotes_at_the_configured_threshold() {
        assert_eq!(lag_materialization_target(100, 90, 0), None);
        assert_eq!(lag_materialization_target(100, 91, 10), None);
        assert_eq!(lag_materialization_target(100, 90, 10), Some(100));
        assert_eq!(lag_materialization_target(100, 80, 10), Some(100));
        assert_eq!(lag_materialization_target(90, 100, 10), None);
    }
}
