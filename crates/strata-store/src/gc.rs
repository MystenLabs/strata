use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use strata_core::{
    BlobLifecycle, PlacementClass, RecordRef, SegmentFileState, SegmentGcOverlay,
    SegmentGcRecordRange, SegmentId, SegmentState, StrataLsn,
};
use strata_gc::{
    DestinationClass, GcAction, GcCopyRecord, GcCopySelection, GcPlan, GcPlanner, GcSnapshot,
    GcSourceRecord, select_copy_records,
};
use strata_index::{AccountingSnapshotGuard, StrataIndex};
use strata_segment::{SegmentReader, SegmentScanner, SegmentWriter};

use crate::{
    Error, GcPublishRequest, Result, StrataStore, WriteCommand,
    layout::{segment_path, segment_state_path},
    metrics::StrataStoreMetrics,
    seal::sha256_file_prefix,
};

/// Store-local preparation for one GC attempt.
///
/// This object intentionally keeps the accounting snapshot guard alive. Later copy/publish work must
/// use the same guard when it asks accounting for changes that happened during the copy phase.
#[derive(Debug)]
pub struct PreparedGcPlan {
    /// In-memory accounting frontier pin used for both planning and later reconciliation.
    pub accounting_snapshot: AccountingSnapshotGuard,
    /// Aggregate pure-planner recommendation.
    pub plan: GcPlan,
    /// Exact copy records selected from source segment scans.
    ///
    /// `None` means the plan is metadata-only, such as deleting an empty segment or reclassifying a
    /// segment whose pinned bytes are too expensive to copy in this run.
    pub copy_selection: Option<GcCopySelection>,
    /// In-memory source segment claim held until this plan is copied or dropped.
    #[doc(hidden)]
    pub claim: Option<GcSourceClaimGuard>,
}

/// Bytes copied into GC staging files, ready for a later publish/finalize step.
///
/// The output files are not yet durable segment rows and the staged `RecordRef.segment_id` values
/// are local to this object. The publish step must assign real segment ids, install sealed segment
/// state rows, and translate staged offsets into final `MapRef` destinations atomically.
#[derive(Debug)]
pub struct PreparedGcCopy {
    /// In-memory accounting frontier pin used for publish reconciliation.
    pub accounting_snapshot: AccountingSnapshotGuard,
    /// Aggregate plan whose selected bytes were copied.
    pub plan: GcPlan,
    /// Sealed staging files containing copied records.
    pub outputs: Vec<GcStagedOutputSegment>,
    /// Source-to-staged-record mapping for later `MapRef` publication.
    pub copied_records: Vec<GcStagedCopiedRecord>,
    /// In-memory source segment claim held until publish completes or this copy is dropped.
    #[doc(hidden)]
    pub claim: Option<GcSourceClaimGuard>,
}

/// Result of publishing staged GC copies into durable Strata metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPublishResult {
    /// Accounted frontier observed while accounting was paused for publish reconciliation.
    pub reconciled_accounted_lsn: StrataLsn,
    /// Output segment files made visible by this publish.
    pub output_segments: Vec<GcPublishedOutputSegment>,
    /// Source refs that were mapped to replacement refs.
    pub published_records: Vec<GcPublishedRecord>,
    /// Staged copies not mapped because their source changed after the GC accounting snapshot.
    pub skipped_records: Vec<GcStagedCopiedRecord>,
}

/// One staged output file after it receives a real segment id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPublishedOutputSegment {
    /// Local id used by the staging file before publish.
    pub staged_segment_id: SegmentId,
    /// Durable segment id assigned during publish.
    pub segment_id: SegmentId,
    /// Final on-disk path.
    pub path: PathBuf,
    /// Placement class installed in segment state.
    pub placement_class: PlacementClass,
    /// Number of sealed bytes in the file.
    pub sealed_len: u64,
}

/// One source ref successfully rewritten to a replacement segment ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPublishedRecord {
    /// Original copied record.
    pub source: GcCopyRecord,
    /// Final replacement ref. This is the staged offset with the real segment id substituted.
    pub to: RecordRef,
    /// Publish LSN assigned to the MapRef/accounting delta.
    pub publish_lsn: StrataLsn,
}

/// One sealed GC staging file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcStagedOutputSegment {
    /// Local id used only while reading this staging file back before publish.
    pub staged_segment_id: SegmentId,
    /// Routing class this file was created for.
    pub destination_class: DestinationClass,
    /// Final placement class to use when this staged file becomes a real segment.
    pub placement_class: PlacementClass,
    /// Temporary on-disk path.
    pub path: PathBuf,
    /// Number of encoded bytes copied into the file.
    pub sealed_len: u64,
    /// SHA-256 digest of the staged bytes.
    pub sealed_sha256: [u8; 32],
}

/// One copied record and the staged offset where its replacement bytes landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcStagedCopiedRecord {
    /// Record selected from the source segment.
    pub source: GcCopyRecord,
    /// Staged record location. `segment_id` is local to `PreparedGcCopy.outputs`.
    pub staged: RecordRef,
}

/// Current accounting lag observed by GC admission.
///
/// `lag_lsn` is `durable_lsn - accounted_lsn` with saturating arithmetic. A non-zero value is not a
/// correctness problem: GC publish can still use relocation forwarding to reconcile accounting
/// events that were durable before publish but not yet materialized. The lag matters for efficiency,
/// because a stale accounting view can make GC copy bytes that accounting will later discover are
/// dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcAccountingLag {
    /// Highest contiguous LSN whose payload and metadata are crash-safe.
    pub durable_lsn: StrataLsn,
    /// Highest contiguous LSN already materialized into accounting/GC overlays.
    pub accounted_lsn: StrataLsn,
    /// `durable_lsn - accounted_lsn`, clamped at zero for defensive accounting.
    pub lag_lsn: StrataLsn,
    /// Optional configured limit used to decide whether a new GC run should be admitted.
    pub max_lag_lsn: Option<StrataLsn>,
}

impl GcAccountingLag {
    /// Returns true when this lag should defer new GC planning/copy work.
    ///
    /// The check is strict: a lag equal to the limit is still admitted, while a lag above the limit
    /// is deferred. `None` disables the gate.
    pub fn exceeds_configured_limit(&self) -> bool {
        self.max_lag_lsn
            .is_some_and(|max_lag_lsn| self.lag_lsn > max_lag_lsn)
    }
}

/// In-memory ownership table for source segments currently used by GC jobs.
#[derive(Debug, Default)]
pub(crate) struct GcSourceClaims {
    claimed: Mutex<BTreeSet<SegmentId>>,
}

impl GcSourceClaims {
    pub(crate) fn try_claim(
        self: &Arc<Self>,
        segments: BTreeSet<SegmentId>,
    ) -> Option<GcSourceClaimGuard> {
        let mut claimed = self.claimed.lock().expect("gc source claims lock poisoned");
        if segments
            .iter()
            .any(|segment_id| claimed.contains(segment_id))
        {
            return None;
        }
        claimed.extend(segments.iter().copied());
        Some(GcSourceClaimGuard {
            claims: Arc::clone(self),
            segments,
        })
    }

    fn mark_snapshot(&self, snapshot: &mut GcSnapshot) {
        let claimed = self.claimed.lock().expect("gc source claims lock poisoned");
        for segment in &mut snapshot.segments {
            if claimed.contains(&segment.state.segment_id) {
                segment.claimed = true;
            }
        }
    }
}

/// Releases in-memory GC source claims when dropped.
#[derive(Debug)]
pub struct GcSourceClaimGuard {
    claims: Arc<GcSourceClaims>,
    segments: BTreeSet<SegmentId>,
}

impl Drop for GcSourceClaimGuard {
    fn drop(&mut self) {
        let mut claimed = self
            .claims
            .claimed
            .lock()
            .expect("gc source claims lock poisoned");
        for segment_id in &self.segments {
            claimed.remove(segment_id);
        }
    }
}

pub(crate) enum GcCommand {
    /// Ask the background worker to run one GC attempt immediately.
    Run,
    /// Stop the worker during store shutdown.
    Shutdown,
}

pub(crate) struct GcWorker {
    pub(crate) executor: GcExecutor,
    pub(crate) planner: GcPlanner,
    pub(crate) interval: Duration,
    pub(crate) command_rx: mpsc::Receiver<GcCommand>,
}

impl GcWorker {
    pub(crate) fn run(self) {
        loop {
            match self.command_rx.recv_timeout(self.interval) {
                Ok(GcCommand::Run) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = self.executor.run_once(&self.planner);
                }
                Ok(GcCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct GcExecutor {
    pub(crate) config: crate::StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) write_tx: mpsc::SyncSender<WriteCommand>,
    pub(crate) accounting_lock: Arc<Mutex<()>>,
    pub(crate) claims: Arc<GcSourceClaims>,
    pub(crate) metrics: StrataStoreMetrics,
}

impl StrataStore {
    fn gc_executor(&self) -> Result<GcExecutor> {
        Ok(GcExecutor {
            config: self.config.clone(),
            index: self.index.clone(),
            write_tx: self
                .write_tx
                .as_ref()
                .ok_or(Error::WriteQueueClosed)?
                .clone(),
            accounting_lock: Arc::clone(&self.accounting_lock),
            claims: Arc::clone(&self.gc_claims),
            metrics: self.metrics.clone(),
        })
    }

    /// Wakes the production GC worker for one immediate attempt.
    pub fn request_gc(&self) -> Result<()> {
        if self.gc_txs.is_empty() {
            return Err(Error::GcQueueClosed);
        }
        for gc_tx in &self.gc_txs {
            gc_tx
                .send(GcCommand::Run)
                .map_err(|_| Error::GcQueueClosed)?;
        }
        Ok(())
    }

    /// Runs one GC plan synchronously using the store's configured planner policy.
    pub fn run_gc_once(&self) -> Result<Option<GcPublishResult>> {
        let planner = GcPlanner::new(self.config.gc_planner_config.clone());
        self.gc_executor()?.run_once(&planner)
    }

    /// Reports the accounting lag GC would use for admission control.
    pub fn gc_accounting_lag(&self) -> Result<GcAccountingLag> {
        self.gc_executor()?.gc_accounting_lag()
    }

    /// Returns a lag snapshot when the configured GC accounting-lag gate would defer a new run.
    pub fn gc_deferred_by_accounting_lag(&self) -> Result<Option<GcAccountingLag>> {
        self.gc_executor()?.gc_deferred_by_accounting_lag()
    }

    /// Prepares one GC plan using real segment files and the segment GC overlay.
    ///
    /// This is the bridge from pure planning to execution. It creates an accounting snapshot guard,
    /// builds the GC planning view from that guard, asks the planner for one plan, scans every
    /// source segment named by copy actions, applies dead/lifetime overlay ranges, and validates
    /// exact copy records against the aggregate route estimates. It does not copy bytes or publish
    /// metadata.
    pub fn prepare_gc_plan(&self, planner: &GcPlanner) -> Result<Option<PreparedGcPlan>> {
        self.gc_executor()?.prepare_gc_plan(planner)
    }

    /// Copies selected GC records into sealed staging files.
    ///
    /// This consumes a `PreparedGcPlan` so the accounting snapshot guard moves forward with the
    /// copied bytes. The method does not publish `MapRef` operations or create durable segment
    /// metadata for the outputs; that is the next step, after reconciling accounting changes since
    /// `accounting_snapshot`.
    pub fn copy_prepared_gc_plan(&self, prepared: PreparedGcPlan) -> Result<PreparedGcCopy> {
        self.gc_executor()?.copy_prepared_gc_plan(prepared)
    }

    /// Publishes staged GC copies through the serialized writer path.
    ///
    /// This method pauses accounting before it enters the writer queue, so the writer thread never
    /// blocks waiting for a long-running sidecar pass. The writer still assigns the final LSN range
    /// and commits metadata in order with user writes; any user writes that were already ahead of
    /// this command in the queue have lower LSNs and are handled later by relocation forwarding.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        self.gc_executor()?.publish_prepared_gc_copy(copy)
    }
}

impl GcExecutor {
    pub(crate) fn run_once(&self, planner: &GcPlanner) -> Result<Option<GcPublishResult>> {
        let Some(prepared) = self.prepare_gc_plan(planner)? else {
            return Ok(None);
        };
        let copy = self.copy_prepared_gc_plan(prepared)?;
        self.publish_prepared_gc_copy(copy).map(Some)
    }

    fn send_write_command(&self, command: WriteCommand) -> Result<()> {
        let started = Instant::now();
        self.metrics.enqueue_write_command();
        let result = self
            .write_tx
            .send(command)
            .map_err(|_| Error::WriteQueueClosed);
        if result.is_err() {
            self.metrics.dequeue_write_command();
        }
        self.metrics
            .record_write_queue_send(result.is_ok(), started.elapsed());
        result
    }

    /// Reports the accounting lag GC would use for admission control.
    pub(crate) fn gc_accounting_lag(&self) -> Result<GcAccountingLag> {
        let durable_lsn = self.index.get_durable_lsn()?;
        let accounted_lsn = self.index.get_accounted_lsn()?;
        Ok(GcAccountingLag {
            durable_lsn,
            accounted_lsn,
            lag_lsn: durable_lsn.saturating_sub(accounted_lsn),
            max_lag_lsn: self.config.gc_max_accounting_lag_lsn,
        })
    }

    /// Returns a lag snapshot when the configured GC accounting-lag gate would defer a new run.
    pub(crate) fn gc_deferred_by_accounting_lag(&self) -> Result<Option<GcAccountingLag>> {
        let lag = self.gc_accounting_lag()?;
        Ok(lag.exceeds_configured_limit().then_some(lag))
    }

    /// Prepares one GC plan using real segment files and the segment GC overlay.
    ///
    /// This is the bridge from pure planning to execution. It creates an accounting snapshot guard,
    /// builds the GC planning view from that guard, asks the planner for one plan, scans every
    /// source segment named by copy actions, applies dead/lifetime overlay ranges, and validates
    /// exact copy records against the aggregate route estimates. It does not copy bytes or publish
    /// metadata.
    pub(crate) fn prepare_gc_plan(&self, planner: &GcPlanner) -> Result<Option<PreparedGcPlan>> {
        if self.gc_deferred_by_accounting_lag()?.is_some() {
            return Ok(None);
        }

        let accounting_snapshot = self.index.create_accounting_snapshot()?;
        let Some(mut snapshot) = self.index.build_gc_snapshot(&accounting_snapshot)? else {
            return Ok(None);
        };
        self.claims.mark_snapshot(&mut snapshot);
        for plan in planner.plans(&snapshot) {
            let source_segments = gc_plan_source_segment_ids(&plan);
            let Some(claim) = self.claims.try_claim(source_segments) else {
                continue;
            };
            let copy_selection = if plan_has_copy_action(&plan) {
                Some(self.select_gc_copy_records(&plan)?)
            } else {
                None
            };

            return Ok(Some(PreparedGcPlan {
                accounting_snapshot,
                plan,
                copy_selection,
                claim: Some(claim),
            }));
        }
        Ok(None)
    }

    fn select_gc_copy_records(&self, plan: &GcPlan) -> Result<GcCopySelection> {
        let mut records = Vec::new();
        for segment_id in copy_source_segment_ids(plan) {
            records.extend(self.scan_gc_source_segment(segment_id)?);
        }
        select_copy_records(plan, &records).map_err(Error::from)
    }

    fn scan_gc_source_segment(&self, segment_id: SegmentId) -> Result<Vec<GcSourceRecord>> {
        let state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        let overlay = self
            .index
            .get_segment_gc_overlay(segment_id)?
            .unwrap_or_default();
        let path = gc_source_segment_path(&self.config, &state);
        let mut scanner = SegmentScanner::open(&path, segment_id)?;
        let prefix = scanner.scan_valid_prefix()?;

        if state.state == SegmentFileState::Sealed {
            let sealed_len = state
                .sealed_len
                .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
            if prefix.valid_len != sealed_len {
                return Err(Error::GcSourceSegmentInvalidPrefix {
                    segment_id,
                    path,
                    expected_len: sealed_len,
                    valid_len: prefix.valid_len,
                });
            }
        }

        prefix
            .records
            .into_iter()
            .filter_map(|record| {
                let record_ref = record.record_ref;
                let range = SegmentGcRecordRange::from(record_ref);
                match overlay_lifecycle_for_record(segment_id, &overlay, range) {
                    Ok(OverlayRecordState::Skip) => None,
                    Ok(OverlayRecordState::CopyEligible { lifecycle }) => {
                        Some(Ok(GcSourceRecord {
                            key: record.key,
                            shard: record.header.shard,
                            payload_lsn: record.header.generation,
                            record_ref,
                            lifecycle,
                        }))
                    }
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
    }

    /// Copies selected GC records into sealed staging files.
    ///
    /// This consumes a `PreparedGcPlan` so the accounting snapshot guard moves forward with the
    /// copied bytes. The method does not publish `MapRef` operations or create durable segment
    /// metadata for the outputs; that is the next step, after reconciling accounting changes since
    /// `accounting_snapshot`.
    pub fn copy_prepared_gc_plan(&self, prepared: PreparedGcPlan) -> Result<PreparedGcCopy> {
        let PreparedGcPlan {
            accounting_snapshot,
            plan,
            copy_selection,
            claim,
        } = prepared;
        let records = copy_selection
            .as_ref()
            .map(|selection| selection.records.clone())
            .unwrap_or_default();
        if records.is_empty() {
            return Ok(PreparedGcCopy {
                accounting_snapshot,
                plan,
                outputs: Vec::new(),
                copied_records: Vec::new(),
                claim,
            });
        }

        let staging_dir = create_gc_staging_dir(&self.config)?;
        let copy_result = self.copy_gc_records_to_staging(&staging_dir, &records);
        let (outputs, copied_records) = match copy_result {
            Ok(copy) => copy,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging_dir);
                return Err(error);
            }
        };

        Ok(PreparedGcCopy {
            accounting_snapshot,
            plan,
            outputs,
            copied_records,
            claim,
        })
    }

    /// Publishes staged GC copies through the serialized writer path.
    ///
    /// This method pauses accounting before it enters the writer queue, so the writer thread never
    /// blocks waiting for a long-running sidecar pass. The writer still assigns the final LSN range
    /// and commits metadata in order with user writes; any user writes that were already ahead of
    /// this command in the queue have lower LSNs and are handled later by relocation forwarding.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        let _accounting_guard = self
            .accounting_lock
            .lock()
            .expect("accounting run lock poisoned");
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::GcPublish(GcPublishRequest {
            copy,
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    fn copy_gc_records_to_staging(
        &self,
        staging_dir: &std::path::Path,
        records: &[GcCopyRecord],
    ) -> Result<(Vec<GcStagedOutputSegment>, Vec<GcStagedCopiedRecord>)> {
        let mut readers = BTreeMap::new();
        let mut outputs = Vec::new();
        let mut open_outputs = BTreeMap::new();
        let mut copied_records = Vec::with_capacity(records.len());
        let mut next_staged_segment_id = 1;

        for record in records {
            let payload = self.read_gc_source_payload(&mut readers, record.from)?;
            if !open_outputs.contains_key(&record.destination_class) {
                let output = create_staged_output(
                    staging_dir,
                    next_staged_segment_id,
                    record.destination_class,
                    self.config.segment_max_bytes,
                )?;
                next_staged_segment_id = output.next_staged_segment_id;
                open_outputs.insert(record.destination_class, output);
            }
            let output = open_outputs
                .get_mut(&record.destination_class)
                .expect("staged output inserted above");
            let staged = append_gc_record_to_staged_output(
                output,
                &mut outputs,
                staging_dir,
                &mut next_staged_segment_id,
                record,
                &payload,
                self.config.segment_max_bytes,
            )?;
            copied_records.push(GcStagedCopiedRecord {
                source: record.clone(),
                staged,
            });
        }

        for (_, output) in open_outputs {
            outputs.push(output.finish()?);
        }
        outputs.sort_by_key(|output| output.staged_segment_id);
        copied_records
            .sort_by_key(|record| (record.source.from.segment_id, record.source.from.offset));
        Ok((outputs, copied_records))
    }

    fn read_gc_source_payload(
        &self,
        readers: &mut BTreeMap<SegmentId, SegmentReader>,
        record_ref: RecordRef,
    ) -> Result<Vec<u8>> {
        let reader = if let Some(reader) = readers.get_mut(&record_ref.segment_id) {
            reader
        } else {
            let state = self.index.get_segment_state(record_ref.segment_id)?.ok_or(
                Error::GcMissingSourceSegment {
                    segment_id: record_ref.segment_id,
                },
            )?;
            if state.state != SegmentFileState::Sealed {
                return Err(Error::GcSourceSegmentNotSealed {
                    segment_id: record_ref.segment_id,
                    state: state.state,
                });
            }
            let path = gc_source_segment_path(&self.config, &state);
            readers.insert(
                record_ref.segment_id,
                SegmentReader::open(&path, record_ref.segment_id)?,
            );
            readers
                .get_mut(&record_ref.segment_id)
                .expect("reader inserted above")
        };
        Ok(reader.read_payload(record_ref)?)
    }
}

#[derive(Debug)]
struct OpenStagedOutput {
    writer: SegmentWriter,
    destination_class: DestinationClass,
    placement_class: PlacementClass,
    next_staged_segment_id: SegmentId,
}

impl OpenStagedOutput {
    fn finish(mut self) -> Result<GcStagedOutputSegment> {
        let sealed_len = self.writer.seal()?;
        let path = self.writer.path().to_path_buf();
        let sealed_sha256 = sha256_file_prefix(&path, sealed_len)?;
        Ok(GcStagedOutputSegment {
            staged_segment_id: self.writer.segment_id(),
            destination_class: self.destination_class,
            placement_class: self.placement_class,
            path,
            sealed_len,
            sealed_sha256,
        })
    }
}

fn create_staged_output(
    staging_dir: &std::path::Path,
    staged_segment_id: SegmentId,
    destination_class: DestinationClass,
    segment_max_bytes: u64,
) -> Result<OpenStagedOutput> {
    let placement_class = placement_class_for_destination(destination_class);
    let path = staging_dir.join(format!("{staged_segment_id:012}.data"));
    let writer =
        SegmentWriter::create(&path, staged_segment_id, placement_class, segment_max_bytes)?;
    Ok(OpenStagedOutput {
        writer,
        destination_class,
        placement_class,
        next_staged_segment_id: staged_segment_id.saturating_add(1),
    })
}

fn append_gc_record_to_staged_output(
    output: &mut OpenStagedOutput,
    finished_outputs: &mut Vec<GcStagedOutputSegment>,
    staging_dir: &std::path::Path,
    next_staged_segment_id: &mut SegmentId,
    record: &GcCopyRecord,
    payload: &[u8],
    segment_max_bytes: u64,
) -> Result<RecordRef> {
    match output
        .writer
        .append_for_shard(&record.key, record.payload_lsn, record.shard, payload)
    {
        Ok(outcome) => Ok(outcome.record_ref),
        Err(strata_segment::Error::SegmentFull { .. }) => {
            let replacement = create_staged_output(
                staging_dir,
                *next_staged_segment_id,
                record.destination_class,
                segment_max_bytes,
            )?;
            let finished = std::mem::replace(output, replacement).finish()?;
            finished_outputs.push(finished);
            *next_staged_segment_id = output.next_staged_segment_id;
            let outcome = output.writer.append_for_shard(
                &record.key,
                record.payload_lsn,
                record.shard,
                payload,
            )?;
            Ok(outcome.record_ref)
        }
        Err(error) => Err(error.into()),
    }
}

fn create_gc_staging_dir(config: &crate::StrataStoreConfig) -> Result<PathBuf> {
    let root = config.namespace_dir().join("gc-staging");
    fs::create_dir_all(&root).map_err(|source| Error::Io {
        path: root.clone(),
        source,
    })?;
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    for attempt in 0..1024_u64 {
        let path = root.join(format!("{}-{}-{attempt}", std::process::id(), seed));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(Error::Io { path, source }),
        }
    }
    Err(Error::InvalidConfig(
        "could not allocate gc staging directory",
    ))
}

fn placement_class_for_destination(destination_class: DestinationClass) -> PlacementClass {
    match destination_class {
        DestinationClass::ExactEpoch(epoch) => PlacementClass::ExactEpoch(epoch),
        DestinationClass::Spillover => PlacementClass::Spillover,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverlayRecordState {
    Skip,
    CopyEligible { lifecycle: Option<BlobLifecycle> },
}

fn overlay_lifecycle_for_record(
    segment_id: SegmentId,
    overlay: &SegmentGcOverlay,
    record: SegmentGcRecordRange,
) -> Result<OverlayRecordState> {
    for skipped in overlay.expired.iter().chain(overlay.retired.iter()) {
        if range_contains(*skipped, record) {
            return Ok(OverlayRecordState::Skip);
        }
        if ranges_overlap(*skipped, record) {
            return Err(partial_overlay_error(segment_id, record));
        }
    }

    let mut lifecycle = None;
    for lifetime in &overlay.lifetimes {
        if range_contains(lifetime.range, record) {
            lifecycle = Some(lifetime.lifecycle);
            continue;
        }
        if ranges_overlap(lifetime.range, record) {
            return Err(partial_overlay_error(segment_id, record));
        }
    }

    Ok(OverlayRecordState::CopyEligible { lifecycle })
}

fn partial_overlay_error(segment_id: SegmentId, record: SegmentGcRecordRange) -> Error {
    Error::GcOverlayPartialRecordRange {
        segment_id,
        offset: record.offset,
        len: record.len,
    }
}

fn range_contains(container: SegmentGcRecordRange, contained: SegmentGcRecordRange) -> bool {
    container.offset <= contained.offset && range_end(container) >= range_end(contained)
}

fn ranges_overlap(left: SegmentGcRecordRange, right: SegmentGcRecordRange) -> bool {
    left.offset < range_end(right) && right.offset < range_end(left)
}

fn range_end(range: SegmentGcRecordRange) -> u64 {
    range.offset.saturating_add(range.len)
}

fn gc_source_segment_path(
    config: &crate::StrataStoreConfig,
    state: &SegmentState,
) -> std::path::PathBuf {
    if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    }
}

fn plan_has_copy_action(plan: &GcPlan) -> bool {
    plan.actions.iter().any(|action| {
        matches!(
            action,
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. }
        )
    })
}

fn copy_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = BTreeSet::new();
    for action in &plan.actions {
        match action {
            GcAction::MoveLiveBytes {
                source_segment_id, ..
            } => {
                source_ids.insert(*source_segment_id);
            }
            GcAction::MoveEpochBytes { routes, .. } => {
                source_ids.extend(routes.iter().map(|route| route.source_segment_id));
            }
            GcAction::DeleteSegment { .. } | GcAction::ReclassifySegment { .. } => {}
        }
    }
    source_ids
}

fn gc_plan_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = copy_source_segment_ids(plan);
    for action in &plan.actions {
        match action {
            GcAction::DeleteSegment { segment_id }
            | GcAction::ReclassifySegment { segment_id, .. } => {
                source_ids.insert(*segment_id);
            }
            GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
        }
    }
    source_ids
}
