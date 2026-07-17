use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
    mpsc,
};

use strata_core::StrataLsn;

#[derive(Debug)]
pub(crate) enum AccountingCommand {
    /// Wake the worker so it can consume the highest-priority coalesced request.
    Wake,
    /// Stop the background worker after pending channel work has drained.
    Shutdown,
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum AccountingRequest {
    None = 0,
    Ingest = 1,
    Materialize = 2,
}

/// Non-blocking accounting request handle shared by foreground, seal, and GC paths.
///
/// The wake channel remains bounded to one so repeated ingest requests coalesce. Request priority
/// lives separately: a materialization request promotes an already queued ingest wakeup instead of
/// being dropped behind it or applying backpressure to the caller.
#[derive(Clone, Debug)]
pub(crate) struct AccountingRequestSender {
    command_tx: mpsc::SyncSender<AccountingCommand>,
    pending_request: Arc<AtomicU8>,
    pending_materialize_through_lsn: Arc<AtomicU64>,
}

impl AccountingRequestSender {
    pub(crate) fn request_ingest(&self) {
        self.request(AccountingRequest::Ingest);
    }

    pub(crate) fn request_materialize(&self, through_lsn: StrataLsn) {
        self.pending_materialize_through_lsn
            .fetch_max(through_lsn, Ordering::AcqRel);
        self.request(AccountingRequest::Materialize);
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.command_tx.send(AccountingCommand::Shutdown);
    }

    fn request(&self, request: AccountingRequest) {
        self.pending_request
            .fetch_max(request as u8, Ordering::AcqRel);
        let _ = self.command_tx.try_send(AccountingCommand::Wake);
    }
}

pub(crate) fn accounting_request_channel() -> (
    AccountingRequestSender,
    mpsc::Receiver<AccountingCommand>,
    Arc<AtomicU8>,
    Arc<AtomicU64>,
) {
    let (command_tx, command_rx) = mpsc::sync_channel(1);
    let pending_request = Arc::new(AtomicU8::new(AccountingRequest::None as u8));
    let pending_materialize_through_lsn = Arc::new(AtomicU64::new(0));
    (
        AccountingRequestSender {
            command_tx,
            pending_request: Arc::clone(&pending_request),
            pending_materialize_through_lsn: Arc::clone(&pending_materialize_through_lsn),
        },
        command_rx,
        pending_request,
        pending_materialize_through_lsn,
    )
}

#[derive(Clone, Copy, Debug)]
pub(super) enum AccountingPassMode {
    Nudged,
    Materialize,
    Maintenance,
}

/// Threshold overrides for one ordered accounting pass.
///
/// Keeping these decisions in one value makes the difference between a writer nudge, an explicit
/// materialization request, and periodic maintenance visible without duplicating the pipeline.
#[derive(Clone, Copy, Debug)]
pub(super) struct AccountingPassPolicy {
    pub(super) force_ingest: bool,
    pub(super) force_delta_compaction: bool,
    pub(super) force_major_compaction: bool,
    pub(super) materialize_shard_drops: bool,
}

impl AccountingPassPolicy {
    pub(super) const NUDGED: Self = Self {
        force_ingest: true,
        force_delta_compaction: false,
        force_major_compaction: false,
        materialize_shard_drops: true,
    };

    pub(super) const MATERIALIZE: Self = Self {
        force_ingest: true,
        force_delta_compaction: true,
        force_major_compaction: true,
        materialize_shard_drops: true,
    };

    pub(super) fn maintenance(force: bool) -> Self {
        Self {
            force_ingest: force,
            force_delta_compaction: force,
            force_major_compaction: force,
            materialize_shard_drops: true,
        }
    }

    #[cfg(test)]
    pub(super) fn test(force: bool) -> Self {
        Self {
            force_ingest: force,
            force_delta_compaction: force,
            force_major_compaction: false,
            materialize_shard_drops: force,
        }
    }
}

pub(super) fn take_pending_run_mode(
    pending_request: &AtomicU8,
    pending_materialize_through_lsn: &AtomicU64,
) -> Option<(AccountingPassMode, Option<StrataLsn>)> {
    match pending_request.swap(AccountingRequest::None as u8, Ordering::AcqRel) {
        value if value == AccountingRequest::None as u8 => None,
        value if value == AccountingRequest::Ingest as u8 => {
            Some((AccountingPassMode::Nudged, None))
        }
        value if value == AccountingRequest::Materialize as u8 => {
            let through_lsn = pending_materialize_through_lsn.swap(0, Ordering::AcqRel);
            (through_lsn != 0).then_some((AccountingPassMode::Materialize, Some(through_lsn)))
        }
        value => panic!("unknown accounting request priority {value}"),
    }
}

pub(super) fn rearm_materialization(
    pending_request: &AtomicU8,
    pending_materialize_through_lsn: &AtomicU64,
    through_lsn: StrataLsn,
) {
    pending_materialize_through_lsn.fetch_max(through_lsn, Ordering::AcqRel);
    pending_request.fetch_max(AccountingRequest::Materialize as u8, Ordering::AcqRel);
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::TryRecvError;

    use super::*;

    #[test]
    fn materialize_promotes_an_already_queued_ingest_wakeup() {
        let (sender, command_rx, pending_request, pending_materialize_through_lsn) =
            accounting_request_channel();

        sender.request_ingest();
        sender.request_materialize(7);

        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((AccountingPassMode::Materialize, Some(7)))
        ));
    }

    #[test]
    fn unfinished_materialization_promotes_the_next_ingest_wakeup() {
        let (sender, command_rx, pending_request, pending_materialize_through_lsn) =
            accounting_request_channel();

        sender.request_materialize(9);
        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((AccountingPassMode::Materialize, Some(9)))
        ));

        rearm_materialization(&pending_request, &pending_materialize_through_lsn, 9);
        sender.request_ingest();

        assert!(matches!(command_rx.try_recv(), Ok(AccountingCommand::Wake)));
        assert!(matches!(
            take_pending_run_mode(&pending_request, &pending_materialize_through_lsn),
            Some((AccountingPassMode::Materialize, Some(9)))
        ));
    }
}
