use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use strata_core::{
    BlobLifecycle, DecodedRecord, FIXED_RECORD_HEADER_LEN, PlacementClass, RecordRef,
    SegmentFileState, SegmentGcOverlay, SegmentGcRecordRange, SegmentGcSummary, SegmentId,
    SegmentOwner, SegmentState, ShardCleanupState, ShardKey, StrataLsn,
};
use strata_gc::{
    DestinationClass, GcAction, GcCopyRecord, GcCopySelector, GcPlan, GcPlanner, GcSnapshot,
};
use strata_index::{AccountingSnapshotGuard, StrataIndex};
use strata_segment::SegmentWriter;

use crate::{
    Error, GcIoLimiter, GcPublishRequest, Result, SegmentIdAllocator, StoreHalt, StrataStore,
    WriteCommand,
    layout::{relative_segment_path, retention_segment_path, segment_path, segment_state_path},
    metrics::StrataStoreMetrics,
    prune_empty_retention_dirs,
    reader_cache::SegmentReaderCache,
    seal::sha256_file_prefix,
    shard_gc::{remove_shard_retention_generation, shard_generation_is_obsolete},
    sync_parent_dir,
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
    /// In-memory source segment claim held until this plan is copied or dropped.
    #[doc(hidden)]
    pub claim: Option<GcSourceClaimGuard>,
}

/// Bytes copied into GC staging files, ready for a later publish/finalize step.
///
/// The output files are not yet durable segment rows and the staged `RecordRef.segment_id` values
/// are local to this object. Publishing first preprotects those files as pending output segment
/// rows, then the writer translates staged offsets into final `MapRef` destinations atomically.
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

/// GC copy bundle after output files have durable segment ids and protected segment rows.
#[derive(Debug)]
pub(crate) struct GcPrepublishedCopy {
    /// In-memory accounting frontier pin used for publish reconciliation.
    pub(crate) accounting_snapshot: AccountingSnapshotGuard,
    /// Aggregate plan whose selected bytes were copied.
    pub(crate) plan: GcPlan,
    /// Protected output segments already installed as pending GC output rows.
    pub(crate) outputs: Vec<GcPrepublishedOutputSegment>,
    /// Source-to-staged-record mapping for later `MapRef` publication.
    pub(crate) copied_records: Vec<GcStagedCopiedRecord>,
    /// In-memory source segment claim held until publish completes or this copy is dropped.
    pub(crate) _claim: Option<GcSourceClaimGuard>,
}

/// Result of publishing staged GC copies into durable Strata metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPublishResult {
    /// Accounted frontier observed while accounting was paused for publish reconciliation.
    pub reconciled_accounted_lsn: StrataLsn,
    /// Output segment files finalized by this publish.
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
    /// Shard generation that owns this retention segment.
    pub shard: ShardKey,
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
    /// Shard generation that owns every record in this staged output.
    pub shard: ShardKey,
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

/// One GC output segment after it has been renamed into the segment directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcPrepublishedOutputSegment {
    /// Local id used by staged record refs before final publication.
    pub(crate) staged_segment_id: SegmentId,
    /// Durable segment id assigned before entering the writer queue.
    pub(crate) segment_id: SegmentId,
    /// Shard generation that owns this retention segment.
    pub(crate) shard: ShardKey,
    /// Final on-disk path.
    pub(crate) path: PathBuf,
    /// Placement class to install when the output becomes sealed.
    pub(crate) placement_class: PlacementClass,
    /// Number of sealed bytes in the file.
    pub(crate) sealed_len: u64,
    /// SHA-256 digest of the sealed bytes.
    pub(crate) sealed_sha256: [u8; 32],
}

impl GcPrepublishedOutputSegment {
    pub(crate) fn pending_state(&self, config: &crate::StrataStoreConfig) -> SegmentState {
        self.segment_state(config, SegmentFileState::PendingGcOutput)
    }

    pub(crate) fn sealed_state(&self, config: &crate::StrataStoreConfig) -> SegmentState {
        self.segment_state(config, SegmentFileState::Sealed)
    }

    pub(crate) fn deleted_state(&self, config: &crate::StrataStoreConfig) -> SegmentState {
        self.segment_state(config, SegmentFileState::Deleted)
    }

    pub(crate) fn published_output(&self) -> GcPublishedOutputSegment {
        GcPublishedOutputSegment {
            staged_segment_id: self.staged_segment_id,
            segment_id: self.segment_id,
            shard: self.shard,
            path: self.path.clone(),
            placement_class: self.placement_class,
            sealed_len: self.sealed_len,
        }
    }

    fn segment_state(
        &self,
        config: &crate::StrataStoreConfig,
        state: SegmentFileState,
    ) -> SegmentState {
        SegmentState {
            owner: SegmentOwner::Shard(self.shard),
            segment_id: self.segment_id,
            volume_id: 0,
            path: relative_segment_path(config, self.path.clone()),
            placement_class: self.placement_class,
            state,
            write_offset: self.sealed_len,
            durable_offset: self.sealed_len,
            min_lsn: None,
            max_lsn: None,
            sealed_before_lsn: None,
            sealed_len: Some(self.sealed_len),
            sealed_sha256: Some(self.sealed_sha256),
        }
    }
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

/// Metric value meaning the GC tuner currently sees no foreground pressure.
const GC_TUNER_HEALTHY: i64 = 0;
/// Metric value meaning the GC tuner currently sees foreground pressure.
const GC_TUNER_PRESSURED: i64 = 1;
/// Metric value meaning the GC tuner recently changed active worker concurrency.
const GC_TUNER_COOLDOWN: i64 = 2;
/// Relative EWMA increase that counts as foreground impact.
const GC_IMPACT_FACTOR_BPS: u128 = 12_500;
/// Absolute hard pressure multiplier over the configured latency threshold.
const GC_IMPACT_HARD_FACTOR_BPS: u128 = 20_000;
/// Old-baseline weight when adapting the GC foreground-latency baseline upward.
const GC_BASELINE_DECAY_OLD_WEIGHT: u128 = 63;
/// New-sample weight when adapting the GC foreground-latency baseline upward.
const GC_BASELINE_DECAY_NEW_WEIGHT: u128 = 1;
/// Total weight for upward baseline adaptation.
const GC_BASELINE_DECAY_WEIGHT_TOTAL: u128 =
    GC_BASELINE_DECAY_OLD_WEIGHT + GC_BASELINE_DECAY_NEW_WEIGHT;
/// Relative EWMA decrease that counts as improvement during backoff.
const GC_IMPROVEMENT_FACTOR_BPS: u128 = 9_500;
/// Write-queue send latency above which enqueueing itself is considered pressured.
const GC_WRITE_QUEUE_IMPACT_THRESHOLD: Duration = Duration::from_millis(10);
/// Basis-point denominator used for GC rate budget adjustment factors.
const GC_RATE_FACTOR_DENOMINATOR: u64 = 10_000;
/// Foreground pressure halves the active GC byte budget until the configured floor.
const GC_RATE_DECREASE_FACTOR_BPS: u64 = 5_000;
/// Healthy windows double the active GC byte budget until the configured ceiling.
const GC_RATE_INCREASE_FACTOR_BPS: u64 = 20_000;

/// Runtime GC concurrency-tuning knobs derived from `StrataStoreConfig`.
///
/// `max_workers` is the number of GC worker threads created at open time. The controller never
/// spawns or stops threads; it only changes how many of those workers may enter a GC attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcConcurrencyConfig {
    /// Upper bound for simultaneously admitted GC workers.
    pub(crate) max_workers: usize,
    /// Initial runtime admission limit.
    pub(crate) initial_workers: usize,
    /// Number of admitted GC attempts between normal tuning decisions.
    pub(crate) tuning_window_cycles: u64,
    /// Sync-latency floor for considering foreground durability work impacted.
    pub(crate) sync_impact_threshold: Duration,
    /// Healthy upper bound for store-wide GC disk I/O.
    pub(crate) max_io_bytes_per_sec: u64,
    /// Pressure lower bound for store-wide GC disk I/O.
    pub(crate) min_io_bytes_per_sec: u64,
}

impl GcConcurrencyConfig {
    /// Extracts the GC concurrency subset from the store-wide config.
    pub(crate) fn from_store_config(config: &crate::StrataStoreConfig) -> Self {
        Self {
            max_workers: config.gc_worker_count,
            initial_workers: config.gc_initial_worker_count,
            tuning_window_cycles: config.gc_tuning_window_cycles,
            sync_impact_threshold: config.gc_sync_impact_threshold,
            max_io_bytes_per_sec: config.gc_io_bytes_per_sec,
            min_io_bytes_per_sec: config.gc_min_io_bytes_per_sec,
        }
    }
}

/// Shared runtime admission controller for background GC attempts.
///
/// GC workers call `try_admit` before running planner/copy/publish work. Foreground paths feed
/// sync, write-queue, and seal-backpressure observations into this object. The tuner then performs
/// additive probing while healthy and backs off when foreground pressure is visible.
#[derive(Debug)]
pub(crate) struct GcConcurrencyController {
    /// Immutable tuning bounds and thresholds.
    config: GcConcurrencyConfig,
    /// Metrics updated whenever the active limit or state changes.
    metrics: StrataStoreMetrics,
    /// Mutable admission/tuning state shared by GC and foreground writer threads.
    state: Mutex<GcConcurrencyState>,
}

// Worker count bounds how many GC attempts may run concurrently; the byte budget bounds how much
// aggregate disk I/O those admitted workers may issue. Both are needed because one admitted worker
// can still issue enough sequential copy I/O to affect foreground sync latency.
#[derive(Debug)]
struct GcConcurrencyState {
    /// Current maximum number of workers allowed to run concurrently.
    active_limit: usize,
    /// Current store-wide GC I/O byte budget.
    active_io_bytes_per_sec: u64,
    /// Number of workers currently holding `GcRunPermit`s.
    in_flight: usize,
    /// Admitted attempts completed since the last tuning decision.
    cycles_since_tune: u64,
    /// Normalized foreground sync latency signal.
    sync_latency_nanos: TunedSignal,
    /// Write-queue enqueue latency signal.
    write_queue_send_nanos: TunedSignal,
    /// Whether foreground writes are currently stalled on seal backlog capacity.
    seal_backpressure_current: bool,
    /// Whether a foreground pressure sample overlapped an admitted GC attempt in this tune window.
    pressure_observed_during_gc: bool,
    /// Current probe/backoff phase.
    mode: GcTuningMode,
}

/// Tuner phase used to choose window length and next adjustment direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcTuningMode {
    /// Cautiously increase concurrency after healthy windows.
    Probing,
    /// Decrease concurrency while the pressure signal keeps improving after a prior decrease.
    BackingOff { previous_signal_nanos: u128 },
}

/// One EWMA foreground-health signal and its recent-best latency baseline.
#[derive(Debug, Clone, Copy, Default)]
struct TunedSignal {
    /// Current exponentially weighted moving average.
    ewma_nanos: Option<u128>,
    /// Decaying baseline used to decide whether the current EWMA is unexpectedly slow.
    baseline_ewma_nanos: Option<u128>,
}

impl TunedSignal {
    /// Updates the EWMA with a new nanosecond sample.
    fn observe(&mut self, sample_nanos: u128) {
        let ewma = match self.ewma_nanos {
            // Smooth noisy foreground samples while still letting sustained latency shifts move the
            // signal. This is a 7/8 old + 1/8 new EWMA.
            Some(current) => current.saturating_mul(7).saturating_add(sample_nanos) / 8,
            None => sample_nanos,
        };
        self.ewma_nanos = Some(ewma);
        self.baseline_ewma_nanos = Some(
            self.baseline_ewma_nanos
                .map_or(ewma, |baseline| decayed_latency_baseline(baseline, ewma)),
        );
    }

    /// Returns the current EWMA if at least one sample has been observed.
    fn current(&self) -> Option<u128> {
        self.ewma_nanos
    }

    /// Returns true when this signal is both above the configured floor and worse than baseline.
    fn degraded(&self, threshold: Duration) -> bool {
        let Some(current) = self.ewma_nanos else {
            return false;
        };
        let threshold_nanos = threshold.as_nanos();
        if current <= threshold_nanos {
            return false;
        }
        let Some(baseline) = self.baseline_ewma_nanos else {
            return current.saturating_mul(10_000)
                > threshold_nanos.saturating_mul(GC_IMPACT_HARD_FACTOR_BPS);
        };
        current.saturating_mul(10_000) > baseline.saturating_mul(GC_IMPACT_FACTOR_BPS)
            || current.saturating_mul(10_000)
                > threshold_nanos.saturating_mul(GC_IMPACT_HARD_FACTOR_BPS)
    }
}

impl GcConcurrencyController {
    /// Creates a controller with its active limit set to the configured initial worker count.
    pub(crate) fn new(config: GcConcurrencyConfig, metrics: StrataStoreMetrics) -> Self {
        let active_limit = config.initial_workers.min(config.max_workers).max(1);
        let active_io_bytes_per_sec = config.max_io_bytes_per_sec;
        metrics.initialize_gc_tuner(
            config.max_workers,
            active_limit,
            config.max_io_bytes_per_sec,
            config.min_io_bytes_per_sec,
            active_io_bytes_per_sec,
        );
        Self {
            config,
            metrics,
            state: Mutex::new(GcConcurrencyState {
                active_limit,
                active_io_bytes_per_sec,
                in_flight: 0,
                cycles_since_tune: 0,
                sync_latency_nanos: TunedSignal::default(),
                write_queue_send_nanos: TunedSignal::default(),
                seal_backpressure_current: false,
                pressure_observed_during_gc: false,
                mode: GcTuningMode::Probing,
            }),
        }
    }

    /// Returns the current runtime admission limit.
    pub(crate) fn active_limit(&self) -> usize {
        self.state
            .lock()
            .expect("gc concurrency lock poisoned")
            .active_limit
    }

    /// Returns the current store-wide GC I/O budget chosen by the runtime tuner.
    pub(crate) fn active_io_bytes_per_sec(&self) -> u64 {
        self.state
            .lock()
            .expect("gc concurrency lock poisoned")
            .active_io_bytes_per_sec
    }

    /// Attempts to reserve one active GC worker slot.
    ///
    /// A returned `GcRunPermit` must be held for the whole GC attempt so `in_flight` accurately
    /// reflects active copy/publish pressure. `None` means the current runtime limit is already
    /// full, so the caller should skip this cycle.
    pub(crate) fn try_admit(self: &Arc<Self>) -> Option<GcRunPermit> {
        let mut state = self.state.lock().expect("gc concurrency lock poisoned");
        if state.in_flight >= state.active_limit {
            self.metrics.record_gc_skipped_by_tuner();
            return None;
        }
        state.in_flight += 1;
        self.metrics.record_gc_admitted();
        self.metrics.set_gc_in_flight_workers(state.in_flight);
        Some(GcRunPermit {
            controller: Arc::clone(self),
        })
    }

    /// Records foreground sync latency.
    ///
    /// Large foreground flushes are normalized by MiB so ordinary large writes do not look like GC
    /// interference solely because they took longer in absolute time.
    pub(crate) fn observe_sync(&self, elapsed: Duration, bytes: u64) {
        let mut state = self.state.lock().expect("gc concurrency lock poisoned");
        let sample_nanos = sync_impact_sample_nanos(elapsed, bytes);
        state.sync_latency_nanos.observe(sample_nanos);
        self.record_pressure_overlap_locked(&mut state);
    }

    /// Records time spent enqueueing a command into the writer queue.
    pub(crate) fn observe_write_queue_send(&self, elapsed: Duration) {
        let mut state = self.state.lock().expect("gc concurrency lock poisoned");
        state
            .write_queue_send_nanos
            .observe(elapsed.as_nanos().max(1));
        self.record_pressure_overlap_locked(&mut state);
    }

    /// Records whether foreground writes are currently blocked by seal backlog pressure.
    pub(crate) fn set_seal_backpressure(&self, current: bool) {
        let mut state = self.state.lock().expect("gc concurrency lock poisoned");
        state.seal_backpressure_current = current;
        if current {
            self.record_pressure_overlap_locked(&mut state);
        }
        if current {
            self.metrics.set_gc_tuner_health_state(GC_TUNER_PRESSURED);
        } else if !self.workload_pressured(&state) {
            self.metrics.set_gc_tuner_health_state(GC_TUNER_HEALTHY);
        }
    }

    /// Releases one active slot and performs a tuning decision if the current window ended.
    fn finish_run(&self) {
        let mut state = self.state.lock().expect("gc concurrency lock poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.cycles_since_tune = state.cycles_since_tune.saturating_add(1);
        self.metrics.set_gc_in_flight_workers(state.in_flight);
        let window = tuning_window_cycles(self.config.tuning_window_cycles, state.mode);
        if state.cycles_since_tune >= window {
            state.cycles_since_tune = 0;
            self.tune_locked(&mut state);
        }
    }

    /// Adjusts `active_limit` according to the current pressure state.
    ///
    /// Healthy probing increases by one worker per normal window. A pressured window decreases by
    /// one worker and enters a shorter backoff window. Backoff continues only while the pressure
    /// signal is still improving, which prevents walking concurrency down forever after the useful
    /// improvement has stopped.
    fn tune_locked(&self, state: &mut GcConcurrencyState) {
        let pressured = self.workload_pressured(state);
        let attributed_pressure = pressured && state.pressure_observed_during_gc;
        state.pressure_observed_during_gc = false;
        let signal = current_pressure_signal_nanos(state);
        match state.mode {
            GcTuningMode::Probing => {
                if attributed_pressure {
                    let worker_changed = self.decrease_locked(state);
                    let io_budget_changed = self.decrease_io_budget_locked(state);
                    let changed = worker_changed || io_budget_changed;
                    state.mode = GcTuningMode::BackingOff {
                        previous_signal_nanos: signal.unwrap_or(0),
                    };
                    self.metrics.set_gc_tuner_health_state(if changed {
                        GC_TUNER_COOLDOWN
                    } else {
                        GC_TUNER_PRESSURED
                    });
                } else if pressured {
                    self.metrics.set_gc_tuner_health_state(GC_TUNER_PRESSURED);
                } else {
                    let worker_changed = self.increase_locked(state);
                    let io_budget_changed = self.increase_io_budget_locked(state);
                    let changed = worker_changed || io_budget_changed;
                    self.metrics.set_gc_tuner_health_state(if changed {
                        GC_TUNER_COOLDOWN
                    } else {
                        GC_TUNER_HEALTHY
                    });
                }
            }
            GcTuningMode::BackingOff {
                previous_signal_nanos,
            } => {
                let current_signal = signal.unwrap_or(previous_signal_nanos);
                let can_back_off_more = state.active_limit > 1
                    || state.active_io_bytes_per_sec > self.config.min_io_bytes_per_sec;
                if attributed_pressure
                    && can_back_off_more
                    && signal_improved(current_signal, previous_signal_nanos)
                {
                    let worker_changed = self.decrease_locked(state);
                    let io_budget_changed = self.decrease_io_budget_locked(state);
                    let changed = worker_changed || io_budget_changed;
                    state.mode = GcTuningMode::BackingOff {
                        previous_signal_nanos: current_signal,
                    };
                    self.metrics.set_gc_tuner_health_state(if changed {
                        GC_TUNER_COOLDOWN
                    } else {
                        GC_TUNER_PRESSURED
                    });
                } else {
                    state.mode = GcTuningMode::Probing;
                    self.metrics.set_gc_tuner_health_state(if pressured {
                        GC_TUNER_PRESSURED
                    } else {
                        GC_TUNER_HEALTHY
                    });
                }
            }
        }
    }

    /// Returns true if any foreground signal currently indicates user-visible pressure.
    fn workload_pressured(&self, state: &GcConcurrencyState) -> bool {
        state.seal_backpressure_current
            || state
                .sync_latency_nanos
                .degraded(self.config.sync_impact_threshold)
            || state
                .write_queue_send_nanos
                .degraded(GC_WRITE_QUEUE_IMPACT_THRESHOLD)
    }

    /// Records that foreground pressure was observed while GC was actually active.
    fn record_pressure_overlap_locked(&self, state: &mut GcConcurrencyState) {
        if state.in_flight > 0 && self.workload_pressured(state) {
            state.pressure_observed_during_gc = true;
        }
    }

    /// Raises active GC concurrency by one worker when below the configured max.
    fn increase_locked(&self, state: &mut GcConcurrencyState) -> bool {
        if state.active_limit >= self.config.max_workers {
            return false;
        }
        state.active_limit += 1;
        self.metrics.set_gc_active_worker_limit(state.active_limit);
        self.metrics.record_gc_tuner_increase();
        true
    }

    /// Lowers active GC concurrency by one worker while preserving at least one active worker.
    fn decrease_locked(&self, state: &mut GcConcurrencyState) -> bool {
        if state.active_limit <= 1 {
            return false;
        }
        state.active_limit -= 1;
        self.metrics.set_gc_active_worker_limit(state.active_limit);
        self.metrics.record_gc_tuner_decrease();
        true
    }

    /// Raises active GC I/O budget toward the configured healthy ceiling.
    fn increase_io_budget_locked(&self, state: &mut GcConcurrencyState) -> bool {
        if state.active_io_bytes_per_sec >= self.config.max_io_bytes_per_sec {
            return false;
        }
        let scaled = state
            .active_io_bytes_per_sec
            .saturating_mul(GC_RATE_INCREASE_FACTOR_BPS)
            / GC_RATE_FACTOR_DENOMINATOR;
        let next = scaled
            .max(state.active_io_bytes_per_sec.saturating_add(1))
            .min(self.config.max_io_bytes_per_sec);
        if next == state.active_io_bytes_per_sec {
            return false;
        }
        state.active_io_bytes_per_sec = next;
        self.metrics.set_gc_active_io_bytes_per_sec(next);
        true
    }

    /// Lowers active GC I/O budget toward the configured pressure floor.
    fn decrease_io_budget_locked(&self, state: &mut GcConcurrencyState) -> bool {
        if state.active_io_bytes_per_sec <= self.config.min_io_bytes_per_sec {
            return false;
        }
        let scaled = state
            .active_io_bytes_per_sec
            .saturating_mul(GC_RATE_DECREASE_FACTOR_BPS)
            / GC_RATE_FACTOR_DENOMINATOR;
        let next = scaled
            .max(self.config.min_io_bytes_per_sec)
            .min(state.active_io_bytes_per_sec.saturating_sub(1));
        if next == state.active_io_bytes_per_sec {
            return false;
        }
        state.active_io_bytes_per_sec = next;
        self.metrics.set_gc_active_io_bytes_per_sec(next);
        true
    }
}

/// RAII guard for one admitted background GC attempt.
///
/// Dropping the guard updates the controller's `in_flight` count and may trigger a tuning decision.
#[derive(Debug)]
pub(crate) struct GcRunPermit {
    controller: Arc<GcConcurrencyController>,
}

impl Drop for GcRunPermit {
    fn drop(&mut self) {
        self.controller.finish_run();
    }
}

/// Produces the latency sample used for foreground sync impact detection.
///
/// Small flushes are kept as raw latency. Larger flushes are normalized to latency per MiB so the
/// tuner responds to unusually slow durability work, not merely to high foreground write volume.
fn sync_impact_sample_nanos(elapsed: Duration, bytes: u64) -> u128 {
    let elapsed_nanos = elapsed.as_nanos().max(1);
    const MIB: u128 = 1024 * 1024;
    if bytes >= MIB as u64 {
        elapsed_nanos.saturating_mul(MIB) / bytes as u128
    } else {
        elapsed_nanos
    }
}

/// Updates the latency baseline used by foreground-pressure detection.
///
/// The baseline drops immediately when latency improves, but rises slowly when the workload changes.
/// Example: a quiet store may learn a 50ms sync baseline. If the real foreground workload later makes
/// 120ms syncs normal, an all-time best baseline would blame GC forever because 120ms is more than
/// 50ms * 1.25. This decayed baseline gradually moves toward 120ms. A later GC-induced jump from a
/// recent 120ms baseline to 220ms is still detected as pressure.
fn decayed_latency_baseline(baseline: u128, current: u128) -> u128 {
    if current <= baseline {
        return current;
    }

    let decayed = baseline
        .saturating_mul(GC_BASELINE_DECAY_OLD_WEIGHT)
        .saturating_add(current.saturating_mul(GC_BASELINE_DECAY_NEW_WEIGHT))
        / GC_BASELINE_DECAY_WEIGHT_TOTAL;
    decayed.max(baseline.saturating_add(1)).min(current)
}

/// Chooses the number of completed GC attempts required before the next tuning decision.
fn tuning_window_cycles(normal_window: u64, mode: GcTuningMode) -> u64 {
    match mode {
        GcTuningMode::Probing => normal_window.max(1),
        GcTuningMode::BackingOff { .. } => (normal_window / 2).max(1),
    }
}

/// Returns the primary pressure signal used to compare backoff improvement.
fn current_pressure_signal_nanos(state: &GcConcurrencyState) -> Option<u128> {
    state
        .sync_latency_nanos
        .current()
        .or_else(|| state.write_queue_send_nanos.current())
}

/// Returns true when the current pressure signal improved materially from the previous one.
fn signal_improved(current: u128, previous: u128) -> bool {
    if previous == 0 {
        return false;
    }
    current.saturating_mul(10_000) < previous.saturating_mul(GC_IMPROVEMENT_FACTOR_BPS)
}

#[cfg(test)]
mod tests;

/// In-memory ownership table for source segments currently used by GC jobs.
/// This is needed to prevent multiple GC jobs from accessing the same source segment concurrently.
#[derive(Debug, Default)]
pub(crate) struct GcSourceClaims {
    state: Mutex<GcSourceClaimState>,
    available: Condvar,
}

#[derive(Debug, Default)]
struct GcSourceClaimState {
    claimed: BTreeSet<SegmentId>,
    /// Number of shard-cleanup waiters blocking new GC claims for each segment.
    draining: BTreeMap<SegmentId, usize>,
}

impl GcSourceClaims {
    /// Attempts to claim every source segment in `segments`.
    ///
    /// The claim is all-or-nothing. If any source is already owned by another in-flight GC job,
    /// the planner should try a different plan or skip this cycle.
    pub(crate) fn try_claim(
        self: &Arc<Self>,
        segments: BTreeSet<SegmentId>,
    ) -> Option<GcSourceClaimGuard> {
        let mut state = self.state.lock().expect("gc source claims lock poisoned");
        if segments.iter().any(|segment_id| {
            state.claimed.contains(segment_id) || state.draining.contains_key(segment_id)
        }) {
            return None;
        }
        state.claimed.extend(segments.iter().copied());
        Some(GcSourceClaimGuard {
            claims: Arc::clone(self),
            segments,
        })
    }

    /// Waits until every requested source can be claimed.
    ///
    /// Shard-drop cleanup uses this after publishing the generation fence. New plans no longer see
    /// the dropped shard's segments, while jobs that claimed an older snapshot are allowed to
    /// finish before the generation directory is unlinked beneath them.
    pub(crate) fn claim_when_available(
        self: &Arc<Self>,
        segments: BTreeSet<SegmentId>,
        timeout: Duration,
    ) -> Option<GcSourceClaimGuard> {
        let deadline = Instant::now().checked_add(timeout)?;
        let mut state = self.state.lock().expect("gc source claims lock poisoned");
        for segment_id in &segments {
            *state.draining.entry(*segment_id).or_default() += 1;
        }

        loop {
            let unavailable = segments
                .iter()
                .any(|segment_id| state.claimed.contains(segment_id));
            if !unavailable {
                state.claimed.extend(segments.iter().copied());
                release_draining_reservation(&mut state, &segments);
                return Some(GcSourceClaimGuard {
                    claims: Arc::clone(self),
                    segments,
                });
            }

            let now = Instant::now();
            if now >= deadline {
                release_draining_reservation(&mut state, &segments);
                self.available.notify_all();
                return None;
            }
            let (next_state, wait) = self
                .available
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("gc source claims lock poisoned");
            state = next_state;
            if wait.timed_out()
                && segments
                    .iter()
                    .any(|segment_id| state.claimed.contains(segment_id))
            {
                release_draining_reservation(&mut state, &segments);
                self.available.notify_all();
                return None;
            }
        }
    }

    /// Marks claimed segments inside a planner snapshot so pure planning can skip them.
    fn mark_snapshot(&self, snapshot: &mut GcSnapshot) {
        let state = self.state.lock().expect("gc source claims lock poisoned");
        for segment in &mut snapshot.segments {
            if state.claimed.contains(&segment.state.segment_id)
                || state.draining.contains_key(&segment.state.segment_id)
            {
                segment.claimed = true;
            }
        }
    }
}

/// Releases in-memory GC source claims when dropped.
///
/// Claims are intentionally not durable. They protect only concurrent workers in this process; all
/// correctness-sensitive validation still happens against durable metadata during publish.
#[derive(Debug)]
pub struct GcSourceClaimGuard {
    /// Shared ownership table to update on drop.
    claims: Arc<GcSourceClaims>,
    /// Source segment ids owned by this guard.
    segments: BTreeSet<SegmentId>,
}

impl Drop for GcSourceClaimGuard {
    fn drop(&mut self) {
        let mut state = self
            .claims
            .state
            .lock()
            .expect("gc source claims lock poisoned");
        for segment_id in &self.segments {
            state.claimed.remove(segment_id);
        }
        self.claims.available.notify_all();
    }
}

fn release_draining_reservation(state: &mut GcSourceClaimState, segments: &BTreeSet<SegmentId>) {
    for segment_id in segments {
        let remove = match state.draining.get_mut(segment_id) {
            Some(waiters) => {
                *waiters = waiters.saturating_sub(1);
                *waiters == 0
            }
            None => false,
        };
        if remove {
            state.draining.remove(segment_id);
        }
    }
}

pub(crate) enum GcCommand {
    /// Ask the background worker to run one GC attempt immediately.
    Run,
    /// Stop the worker during store shutdown.
    Shutdown,
}

/// Background GC worker loop.
///
/// Each worker owns one command receiver. Requests are broadcast by sending `GcCommand::Run` to all
/// workers, but the shared concurrency controller decides how many may actually enter a GC attempt.
pub(crate) struct GcWorker {
    /// Store-local executor used for planning, copying, and publishing.
    pub(crate) executor: GcExecutor,
    /// Pure planner policy used by this worker.
    pub(crate) planner: GcPlanner,
    /// Periodic wakeup interval for opportunistic GC.
    pub(crate) interval: Duration,
    /// Control channel for immediate run requests and shutdown.
    pub(crate) command_rx: mpsc::Receiver<GcCommand>,
}

/// Largest failure backoff multiplier is `2^GC_FAILURE_BACKOFF_MAX_EXPONENT` times the configured
/// GC interval. Backoff only stretches the timer tick; explicit `GcCommand::Run` requests still run
/// immediately so an operator or test can force a retry.
const GC_FAILURE_BACKOFF_MAX_EXPONENT: u32 = 4;
/// Maximum number of selected plans one worker drains from a single wakeup.
///
/// Copy plans are still governed by the shared byte limiter, but metadata-only plans can otherwise
/// run without consuming byte tokens. This cap keeps one wake from monopolizing a worker forever.
const GC_MAX_PLANS_PER_WAKE: usize = 64;

impl GcWorker {
    /// Runs the worker until shutdown or channel disconnect.
    pub(crate) fn run(self) {
        // Consecutive failures are tracked per worker so one worker stuck on a persistently
        // failing plan backs off without slowing down other workers. The shared metrics gauge is
        // store-level: any successful attempt resets it, so it only stays elevated when GC as a
        // whole cannot make progress.
        let mut consecutive_failures: u32 = 0;
        loop {
            let wait = gc_failure_backoff(self.interval, consecutive_failures);
            match self.command_rx.recv_timeout(wait) {
                Ok(GcCommand::Run) | Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Some(_permit) = self.executor.gc_concurrency.try_admit() {
                        match self.run_ready_plans() {
                            Ok(_) => {
                                consecutive_failures = 0;
                                self.executor.metrics.record_gc_run_success();
                            }
                            Err(error) => {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                let reason = error.gc_failure_reason();
                                self.executor.metrics.record_gc_run_failure(reason);
                                let current_thread = std::thread::current();
                                let worker = current_thread.name().unwrap_or("unnamed-gc-worker");
                                eprintln!(
                                    "background Strata GC run failed: worker={worker} reason={reason} worker_consecutive_failures={consecutive_failures} error={error:?}"
                                );
                            }
                        }
                    }
                }
                Ok(GcCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn run_ready_plans(&self) -> Result<()> {
        self.executor.cleanup_ready_shard_generations()?;
        for _ in 0..GC_MAX_PLANS_PER_WAKE {
            if self.executor.run_once(&self.planner)?.is_none() {
                break;
            }
        }
        Ok(())
    }
}

/// Stretches the worker's timer tick after consecutive failures.
///
/// A GC attempt that fails deterministically (for example a plan that keeps aborting on a stale
/// view) would otherwise burn a full plan/scan cycle every interval forever. The first failure
/// keeps the normal cadence; each further failure doubles the wait up to the capped exponent.
fn gc_failure_backoff(interval: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures <= 1 {
        return interval;
    }
    let exponent = (consecutive_failures - 1).min(GC_FAILURE_BACKOFF_MAX_EXPONENT);
    interval.saturating_mul(2_u32.saturating_pow(exponent))
}

/// Store-local executor for one GC attempt.
///
/// This object bridges pure planning with real files and durable store metadata. It is cheap to
/// clone because it holds shared handles/channels, not open staging state.
#[derive(Clone)]
pub(crate) struct GcExecutor {
    /// Store configuration snapshot.
    pub(crate) config: crate::StrataStoreConfig,
    /// Metadata/index handle used for snapshots and validation.
    pub(crate) index: StrataIndex,
    /// Writer queue used to serialize GC publish with foreground writes.
    pub(crate) write_tx: mpsc::SyncSender<WriteCommand>,
    /// Accounting run lock used to pause accounting during publish reconciliation.
    pub(crate) accounting_lock: Arc<Mutex<()>>,
    /// Serializes GC output publication with shard-generation directory cleanup.
    pub(crate) publish_cleanup_lock: Arc<Mutex<()>>,
    /// In-process source segment ownership table.
    pub(crate) claims: Arc<GcSourceClaims>,
    /// Runtime GC admission controller shared with foreground paths.
    pub(crate) gc_concurrency: Arc<GcConcurrencyController>,
    /// Store-wide byte limiter for GC scan/copy/checksum I/O.
    pub(crate) gc_io_limiter: Arc<GcIoLimiter>,
    /// Shared monotonic allocator for durable segment ids.
    pub(crate) segment_ids: SegmentIdAllocator,
    /// Open segment readers that must be evicted before generation-directory deletion.
    pub(crate) reader_cache: Arc<SegmentReaderCache>,
    /// Terminal store state shared with foreground writer paths.
    pub(crate) store_halt: StoreHalt,
    /// Store metrics sink.
    pub(crate) metrics: StrataStoreMetrics,
}

impl StrataStore {
    /// Builds an executor view over this store handle.
    pub(crate) fn gc_executor(&self) -> Result<GcExecutor> {
        self.store_halt.check()?;
        Ok(GcExecutor {
            config: self.config.clone(),
            index: self.index.clone(),
            write_tx: self
                .write_tx
                .as_ref()
                .ok_or(Error::WriteQueueClosed)?
                .clone(),
            accounting_lock: Arc::clone(&self.accounting_lock),
            publish_cleanup_lock: Arc::clone(&self.gc_publish_cleanup_lock),
            claims: Arc::clone(&self.gc_claims),
            gc_concurrency: Arc::clone(&self.gc_concurrency),
            gc_io_limiter: Arc::clone(&self.gc_io_limiter),
            segment_ids: self.segment_ids.clone(),
            reader_cache: Arc::clone(&self.reader_cache),
            store_halt: self.store_halt.clone(),
            metrics: self.metrics.clone(),
        })
    }

    /// Wakes the production GC worker for one immediate attempt.
    pub fn request_gc(&self) -> Result<()> {
        self.store_halt.check()?;
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

    /// Prepares one GC plan from the segment GC overlay.
    ///
    /// This is the bridge from pure planning to execution. It creates an accounting snapshot guard,
    /// builds the GC planning view from that guard, asks the planner for one plan, and claims the
    /// source segments named by that plan. It does not read source segment files, copy bytes, or
    /// publish metadata.
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
    /// blocks waiting for a long-running processor pass. The writer still assigns the final LSN range
    /// and commits metadata in order with user writes; any user writes that were already ahead of
    /// this command in the queue have lower LSNs and are handled later by relocation forwarding.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        self.gc_executor()?.publish_prepared_gc_copy(copy)
    }
}

impl GcExecutor {
    /// Runs one complete GC attempt: prepare, copy, then publish.
    ///
    /// `Ok(None)` means no eligible plan was admitted or selected. Errors are operational failures
    /// from file I/O, index access, or writer publication.
    pub(crate) fn run_once(&self, planner: &GcPlanner) -> Result<Option<GcPublishResult>> {
        self.gc_io_limiter
            .set_bytes_per_sec(self.gc_concurrency.active_io_bytes_per_sec());
        let Some(prepared) = self.prepare_gc_plan(planner)? else {
            return Ok(None);
        };
        let copy = self.copy_prepared_gc_plan(prepared)?;
        self.publish_prepared_gc_copy(copy).map(Some)
    }

    pub(crate) fn cleanup_ready_shard_generations(&self) -> Result<usize> {
        let jobs = self.index.iter_shard_cleanup_jobs()?;
        let mut cleaned = 0;
        for job in jobs
            .into_iter()
            .filter(|job| job.state == ShardCleanupState::ReadyForGc)
        {
            let owned_segments = self
                .index
                .iter_segment_states_for_shard(job.shard)?
                .into_iter()
                .map(|(segment_id, _)| segment_id)
                .collect::<BTreeSet<_>>();
            let Some(_claim) = self.claims.claim_when_available(
                owned_segments.clone(),
                self.config.shard_drop_gc_drain_timeout,
            ) else {
                continue;
            };
            let _publish_cleanup_guard = self
                .publish_cleanup_lock
                .lock()
                .expect("gc publish/cleanup lock poisoned");
            let Some(current_job) = self.index.get_shard_cleanup_job(job.shard)? else {
                continue;
            };
            if current_job.state != ShardCleanupState::ReadyForGc {
                continue;
            }
            let mut removed_summary = SegmentGcSummary::default();
            let mut removed_relocating_segments = 0;
            for segment_id in &owned_segments {
                if self
                    .index
                    .get_segment_state(*segment_id)?
                    .is_some_and(|state| state.state == SegmentFileState::GcRelocating)
                {
                    removed_relocating_segments += 1;
                }
                if let Some(overlay) = self.index.get_segment_gc_overlay(*segment_id)? {
                    removed_summary.total_bytes = removed_summary
                        .total_bytes
                        .saturating_add(overlay.summary.total_bytes);
                    removed_summary.live_bytes = removed_summary
                        .live_bytes
                        .saturating_add(overlay.summary.live_bytes);
                    removed_summary.retired_bytes = removed_summary
                        .retired_bytes
                        .saturating_add(overlay.summary.retired_bytes);
                    removed_summary.expired_bytes = removed_summary
                        .expired_bytes
                        .saturating_add(overlay.summary.expired_bytes);
                    removed_summary.live_ref_count = removed_summary
                        .live_ref_count
                        .saturating_add(overlay.summary.live_ref_count);
                }
                self.reader_cache.evict(*segment_id);
                self.metrics.record_reader_cache_eviction();
            }
            remove_shard_retention_generation(&self.config, &self.index, job.shard)?;
            self.metrics.remove_gc_known_summary(&removed_summary);
            self.metrics
                .remove_gc_relocating_segments(removed_relocating_segments);
            let mut batch = self.index.batch();
            self.index
                .delete_shard_cleanup_job_batch(&mut batch, job.shard)?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
            cleaned += 1;
        }
        Ok(cleaned)
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

    /// Prepares one GC plan from the segment GC overlay.
    ///
    /// This is the bridge from pure planning to execution. It creates an accounting snapshot guard,
    /// builds the GC planning view from that guard, asks the planner for one plan, and claims the
    /// source segments named by that plan. It does not read source segment files, copy bytes, or
    /// publish metadata.
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

            return Ok(Some(PreparedGcPlan {
                accounting_snapshot,
                plan,
                claim: Some(claim),
            }));
        }
        Ok(None)
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
            claim,
        } = prepared;
        if !plan_has_copy_action(&plan) {
            return Ok(PreparedGcCopy {
                accounting_snapshot,
                plan,
                outputs: Vec::new(),
                copied_records: Vec::new(),
                claim,
            });
        }

        let staging_dir = create_gc_staging_dir(&self.config)?;
        let copy_result = self.copy_gc_plan_to_staging(&staging_dir, &plan);
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
    /// Output files are first renamed into final segment paths and protected by pending segment
    /// rows outside the writer queue. Accounting is paused only while the writer reconciles the
    /// snapshot and publishes MapRefs.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        // Keep shard cleanup ordered with every phase that can create or remove a retention path.
        // The writer may fence a shard while this guard is held; drop cleanup waits until publish
        // has reconciled that fence and removed any now-unpublished output.
        let _publish_cleanup_guard = self
            .publish_cleanup_lock
            .lock()
            .expect("gc publish/cleanup lock poisoned");
        let copy = self.prepublish_gc_outputs(copy)?;
        let prepublished_outputs = copy.outputs.clone();
        let result = {
            let _accounting_guard = self
                .accounting_lock
                .lock()
                .expect("accounting run lock poisoned");
            self.publish_prepublished_gc_copy(copy)
        };

        match result {
            Ok(result) => {
                remove_unpublished_prepublished_outputs(
                    &self.config,
                    &prepublished_outputs,
                    &result.output_segments,
                )?;
                Ok(result)
            }
            Err(error) => {
                if self.store_halt.error().is_none() {
                    let _ = abandon_prepublished_outputs(
                        &self.config,
                        &self.index,
                        &prepublished_outputs,
                    );
                }
                Err(error)
            }
        }
    }

    fn publish_prepublished_gc_copy(&self, copy: GcPrepublishedCopy) -> Result<GcPublishResult> {
        self.store_halt.check()?;
        let started = Instant::now();
        self.metrics.enqueue_write_command();
        let (response_tx, response_rx) = mpsc::channel();
        let command = WriteCommand::GcPublish(GcPublishRequest { copy, response_tx });
        if let Err(error) = self.write_tx.send(command) {
            self.metrics.dequeue_write_command();
            self.metrics
                .record_write_queue_send(false, started.elapsed());
            return match error.0 {
                WriteCommand::GcPublish(request) => {
                    let _ = abandon_prepublished_outputs(
                        &self.config,
                        &self.index,
                        &request.copy.outputs,
                    );
                    Err(Error::WriteQueueClosed)
                }
                _ => Err(Error::WriteQueueClosed),
            };
        }
        self.metrics
            .record_write_queue_send(true, started.elapsed());
        self.gc_concurrency
            .observe_write_queue_send(started.elapsed());
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    fn prepublish_gc_outputs(&self, copy: PreparedGcCopy) -> Result<GcPrepublishedCopy> {
        let PreparedGcCopy {
            accounting_snapshot,
            plan,
            outputs,
            copied_records,
            claim,
        } = copy;
        let original_outputs = outputs.clone();
        let outputs = (|| {
            let mut publishable = Vec::with_capacity(outputs.len());
            let mut obsolete = Vec::new();
            for output in outputs {
                if shard_generation_is_obsolete(&self.index, output.shard)? {
                    obsolete.push(output);
                } else {
                    publishable.push(output);
                }
            }
            remove_gc_staging_output_files(&obsolete)?;
            self.prepublish_gc_output_segments(publishable)
        })();
        let outputs = match outputs {
            Ok(outputs) => outputs,
            Err(error) => {
                let _ = remove_gc_staging_output_files(&original_outputs);
                return Err(error);
            }
        };
        Ok(GcPrepublishedCopy {
            accounting_snapshot,
            plan,
            outputs,
            copied_records,
            _claim: claim,
        })
    }

    fn prepublish_gc_output_segments(
        &self,
        outputs: Vec<GcStagedOutputSegment>,
    ) -> Result<Vec<GcPrepublishedOutputSegment>> {
        if outputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut prepublished = Vec::with_capacity(outputs.len());
        let result = (|| {
            for output in &outputs {
                let segment_id = self.segment_ids.allocate()?;
                let final_path = retention_segment_path(
                    &self.config,
                    output.shard,
                    output.placement_class,
                    segment_id,
                );
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).map_err(|source| Error::Io {
                        path: parent.to_path_buf(),
                        source,
                    })?;
                }
                if final_path.exists() {
                    if self.index.get_segment_state(segment_id)?.is_none() {
                        fs::remove_file(&final_path).map_err(|source| Error::Io {
                            path: final_path.clone(),
                            source,
                        })?;
                    } else {
                        return Err(Error::GcOutputSegmentExists {
                            segment_id,
                            path: final_path,
                        });
                    }
                }
                fs::rename(&output.path, &final_path).map_err(|source| Error::Io {
                    path: final_path.clone(),
                    source,
                })?;
                sync_parent_dir(&final_path)?;
                if output.path.parent() != final_path.parent() {
                    sync_parent_dir(&output.path)?;
                }
                prepublished.push(GcPrepublishedOutputSegment {
                    staged_segment_id: output.staged_segment_id,
                    segment_id,
                    shard: output.shard,
                    path: final_path,
                    placement_class: output.placement_class,
                    sealed_len: output.sealed_len,
                    sealed_sha256: output.sealed_sha256,
                });
            }

            let mut batch = self.index.batch();
            for output in &prepublished {
                self.index
                    .put_segment_state_batch(&mut batch, &output.pending_state(&self.config))?;
            }
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
            Ok::<_, Error>(())
        })();

        match result {
            Ok(()) => Ok(prepublished),
            Err(error) => {
                let _ = remove_gc_prepublished_output_files(&self.config, &prepublished);
                let _ = remove_gc_staging_output_files(&outputs);
                Err(error)
            }
        }
    }

    /// Copies selected records into temporary sealed segment files grouped by destination class.
    ///
    /// Staging output uses local segment ids starting at one. Publish later assigns durable segment
    /// ids and translates staged refs into final refs while holding the writer ordering domain.
    fn copy_gc_plan_to_staging(
        &self,
        staging_dir: &Path,
        plan: &GcPlan,
    ) -> Result<(Vec<GcStagedOutputSegment>, Vec<GcStagedCopiedRecord>)> {
        let selector = GcCopySelector::new(plan)?;
        let mut copier = GcStagingCopier::new(
            &self.gc_io_limiter,
            staging_dir,
            self.config.segment_max_bytes,
        );

        for segment_id in copy_source_segment_ids(plan) {
            self.copy_gc_source_segment_to_staging(segment_id, &selector, &mut copier)?;
        }

        selector.validate_copied_bytes(copier.copied_bytes())?;
        copier.finish()
    }

    /// Scans one source segment and copies records selected by the aggregate plan routes.
    ///
    /// The scan is offset ordered, so overlay classification advances monotonically through the
    /// segment-local overlay. Expired and retired ranges are skipped. Copy eligible records are
    /// decoded only when their lifecycle bucket is named by the plan.
    fn copy_gc_source_segment_to_staging(
        &self,
        segment_id: SegmentId,
        selector: &GcCopySelector,
        copier: &mut GcStagingCopier<'_>,
    ) -> Result<()> {
        let state = self
            .index
            .get_segment_state(segment_id)?
            .ok_or(Error::GcMissingSourceSegment { segment_id })?;
        if state.state != SegmentFileState::Sealed {
            return Err(Error::GcSourceSegmentNotSealed {
                segment_id,
                state: state.state,
            });
        }
        let sealed_len = state
            .sealed_len
            .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
        let overlay = self
            .index
            .get_segment_gc_overlay(segment_id)?
            .unwrap_or_default();
        let path = gc_source_segment_path(&self.config, &state);
        let file_len = fs::metadata(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?
            .len();
        if file_len != sealed_len {
            return Err(Error::GcSourceSegmentInvalidPrefix {
                segment_id,
                path,
                expected_len: sealed_len,
                valid_len: file_len,
            });
        }

        let mut file = File::open(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let mut classifier = OverlayRecordClassifier::new(segment_id, &overlay);
        let mut offset = 0_u64;

        while offset < sealed_len {
            let remaining = sealed_len - offset;
            if remaining < FIXED_RECORD_HEADER_LEN as u64 {
                return Err(Error::GcSourceSegmentInvalidPrefix {
                    segment_id,
                    path,
                    expected_len: sealed_len,
                    valid_len: offset,
                });
            }

            file.seek(SeekFrom::Start(offset))
                .map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            let mut fixed = [0; FIXED_RECORD_HEADER_LEN];
            self.gc_io_limiter.acquire(FIXED_RECORD_HEADER_LEN as u64);
            file.read_exact(&mut fixed).map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;

            let header =
                DecodedRecord::peek_fixed_header(&fixed).map_err(strata_segment::Error::from)?;
            let record_len = header
                .encoded_record_len()
                .map_err(strata_segment::Error::from)?;
            if remaining < record_len {
                return Err(Error::GcSourceSegmentInvalidPrefix {
                    segment_id,
                    path,
                    expected_len: sealed_len,
                    valid_len: offset,
                });
            }

            let record_ref = RecordRef {
                segment_id,
                offset,
                len: record_len,
            };
            let range = SegmentGcRecordRange::from(record_ref);
            let lifecycle = match classifier.classify(range)? {
                OverlayRecordState::Skip => {
                    offset = offset
                        .checked_add(record_len)
                        .ok_or(strata_segment::Error::RangeOverflow)?;
                    continue;
                }
                OverlayRecordState::CopyEligible { lifecycle } => lifecycle,
            };

            let Some(destination_class) = selector.destination_for(segment_id, lifecycle) else {
                offset = offset
                    .checked_add(record_len)
                    .ok_or(strata_segment::Error::RangeOverflow)?;
                continue;
            };

            let payload_len = usize::try_from(header.payload_len)
                .map_err(|_| strata_segment::Error::RangeOverflow)?;
            let key_len = header.key_len as usize;
            let body_len = payload_len
                .checked_add(key_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
            let mut body = vec![0; body_len];
            self.gc_io_limiter.acquire(body_len as u64);
            if !body.is_empty() {
                file.read_exact(&mut body).map_err(|source| Error::Io {
                    path: path.clone(),
                    source,
                })?;
            }
            let key = body.split_off(payload_len);
            let payload = body;
            let decoded = DecodedRecord::from_parts(header, &fixed, payload, key)
                .map_err(strata_segment::Error::from)?;
            let payload = decoded.payload;
            let record = GcCopyRecord {
                key: decoded.key,
                shard: decoded.header.shard,
                payload_lsn: decoded.header.generation,
                from: record_ref,
                lifecycle,
                destination_class,
            };
            copier.append(record, &payload)?;

            offset = offset
                .checked_add(record_len)
                .ok_or(strata_segment::Error::RangeOverflow)?;
        }

        Ok(())
    }
}

/// Open GC staging state shared across source segment scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct DestinationPlacement {
    shard: ShardKey,
    class: DestinationClass,
}

struct GcStagingCopier<'a> {
    io_limiter: &'a GcIoLimiter,
    staging_dir: &'a Path,
    segment_max_bytes: u64,
    outputs: Vec<GcStagedOutputSegment>,
    open_outputs: BTreeMap<DestinationPlacement, OpenStagedOutput>,
    copied_records: Vec<GcStagedCopiedRecord>,
    copied_bytes: u64,
    next_staged_segment_id: SegmentId,
}

impl<'a> GcStagingCopier<'a> {
    fn new(io_limiter: &'a GcIoLimiter, staging_dir: &'a Path, segment_max_bytes: u64) -> Self {
        Self {
            io_limiter,
            staging_dir,
            segment_max_bytes,
            outputs: Vec::new(),
            open_outputs: BTreeMap::new(),
            copied_records: Vec::new(),
            copied_bytes: 0,
            next_staged_segment_id: 1,
        }
    }

    fn copied_bytes(&self) -> u64 {
        self.copied_bytes
    }

    fn append(&mut self, record: GcCopyRecord, payload: &[u8]) -> Result<()> {
        let destination = DestinationPlacement {
            shard: record.shard,
            class: record.destination_class,
        };
        if !self.open_outputs.contains_key(&destination) {
            let output = create_staged_output(
                self.staging_dir,
                self.next_staged_segment_id,
                destination,
                self.segment_max_bytes,
            )?;
            self.next_staged_segment_id = output.next_staged_segment_id;
            self.open_outputs.insert(destination, output);
        }

        let output = self
            .open_outputs
            .get_mut(&destination)
            .expect("staged output inserted above");
        let mut append_context = GcStagedOutputAppendContext {
            io_limiter: self.io_limiter,
            finished_outputs: &mut self.outputs,
            staging_dir: self.staging_dir,
            next_staged_segment_id: &mut self.next_staged_segment_id,
            segment_max_bytes: self.segment_max_bytes,
        };
        let staged =
            append_gc_record_to_staged_output(&mut append_context, output, &record, payload)?;
        self.copied_bytes = self
            .copied_bytes
            .checked_add(record.from.len)
            .ok_or(strata_segment::Error::RangeOverflow)?;
        self.copied_records.push(GcStagedCopiedRecord {
            source: record,
            staged,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<(Vec<GcStagedOutputSegment>, Vec<GcStagedCopiedRecord>)> {
        for (_, output) in self.open_outputs {
            self.outputs.push(output.finish(self.io_limiter)?);
        }
        self.outputs.sort_by_key(|output| output.staged_segment_id);
        self.copied_records
            .sort_by_key(|record| (record.source.from.segment_id, record.source.from.offset));
        Ok((self.outputs, self.copied_records))
    }
}

/// Writable GC output segment that has not been sealed yet.
#[derive(Debug)]
struct OpenStagedOutput {
    /// Segment writer for the temporary staging file.
    writer: SegmentWriter,
    /// Logical shard and destination class this output accepts.
    destination: DestinationPlacement,
    /// Final placement class to install if this output is published.
    placement_class: PlacementClass,
    /// Next local staging id to allocate after this output.
    next_staged_segment_id: SegmentId,
}

impl OpenStagedOutput {
    /// Seals the temporary file and records the digest needed for final segment metadata.
    fn finish(mut self, io_limiter: &GcIoLimiter) -> Result<GcStagedOutputSegment> {
        let sealed_len = self.writer.seal()?;
        let path = self.writer.path().to_path_buf();
        io_limiter.acquire(sealed_len);
        let sealed_sha256 = sha256_file_prefix(&path, sealed_len)?;
        Ok(GcStagedOutputSegment {
            staged_segment_id: self.writer.segment_id(),
            shard: self.destination.shard,
            destination_class: self.destination.class,
            placement_class: self.placement_class,
            path,
            sealed_len,
            sealed_sha256,
        })
    }
}

/// Creates one open staging segment for a destination class.
fn create_staged_output(
    staging_dir: &std::path::Path,
    staged_segment_id: SegmentId,
    destination: DestinationPlacement,
    segment_max_bytes: u64,
) -> Result<OpenStagedOutput> {
    let placement_class = placement_class_for_destination(destination.class);
    let path = staging_dir.join(format!("{staged_segment_id:012}.data"));
    let writer =
        SegmentWriter::create(&path, staged_segment_id, placement_class, segment_max_bytes)?;
    Ok(OpenStagedOutput {
        writer,
        destination,
        placement_class,
        next_staged_segment_id: staged_segment_id.saturating_add(1),
    })
}

/// Appends one copied record to an open staging output.
///
/// If the current output is full, it is sealed and pushed into `finished_outputs`, then a
/// replacement output for the same destination class is opened before retrying the append.
struct GcStagedOutputAppendContext<'a> {
    io_limiter: &'a GcIoLimiter,
    finished_outputs: &'a mut Vec<GcStagedOutputSegment>,
    staging_dir: &'a Path,
    next_staged_segment_id: &'a mut SegmentId,
    segment_max_bytes: u64,
}

fn append_gc_record_to_staged_output(
    context: &mut GcStagedOutputAppendContext<'_>,
    output: &mut OpenStagedOutput,
    record: &GcCopyRecord,
    payload: &[u8],
) -> Result<RecordRef> {
    context.io_limiter.acquire(record.from.len);
    match output
        .writer
        .append_for_shard(&record.key, record.payload_lsn, record.shard, payload)
    {
        Ok(outcome) => Ok(outcome.record_ref),
        Err(strata_segment::Error::SegmentFull { .. }) => {
            let replacement = create_staged_output(
                context.staging_dir,
                *context.next_staged_segment_id,
                output.destination,
                context.segment_max_bytes,
            )?;
            let finished = std::mem::replace(output, replacement).finish(context.io_limiter)?;
            context.finished_outputs.push(finished);
            *context.next_staged_segment_id = output.next_staged_segment_id;
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

/// Allocates a unique per-attempt staging directory under the store namespace.
///
/// The directory name includes process id, wall-clock seed, and retry counter. It is intentionally
/// not durable metadata; failed copy attempts remove it best-effort, and recovery can discard stale
/// staging directories because they are not referenced by segment state.
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

fn remove_unpublished_prepublished_outputs(
    config: &crate::StrataStoreConfig,
    prepublished: &[GcPrepublishedOutputSegment],
    published: &[GcPublishedOutputSegment],
) -> Result<()> {
    let published_ids = published
        .iter()
        .map(|output| output.segment_id)
        .collect::<BTreeSet<_>>();
    let unpublished = prepublished
        .iter()
        .filter(|output| !published_ids.contains(&output.segment_id))
        .cloned()
        .collect::<Vec<_>>();
    if unpublished.is_empty() {
        return Ok(());
    }
    remove_gc_prepublished_output_files(config, &unpublished)
}

fn abandon_prepublished_outputs(
    config: &crate::StrataStoreConfig,
    index: &StrataIndex,
    outputs: &[GcPrepublishedOutputSegment],
) -> Result<()> {
    if outputs.is_empty() {
        return Ok(());
    }

    let mut batch = index.batch();
    for output in outputs {
        index.put_segment_state_batch(&mut batch, &output.deleted_state(config))?;
    }
    batch
        .write_with_sync(true)
        .map_err(strata_index::Error::from)?;
    remove_gc_prepublished_output_files(config, outputs)
}

fn remove_gc_staging_output_files(outputs: &[GcStagedOutputSegment]) -> Result<()> {
    for output in outputs {
        remove_gc_output_file(&output.path)?;
    }
    Ok(())
}

fn remove_gc_prepublished_output_files(
    config: &crate::StrataStoreConfig,
    outputs: &[GcPrepublishedOutputSegment],
) -> Result<()> {
    for output in outputs {
        remove_gc_output_file(&output.path)?;
        if let Some(parent) = output.path.parent() {
            prune_empty_retention_dirs(config, parent.to_path_buf())?;
        }
    }
    Ok(())
}

fn remove_gc_output_file(path: &std::path::Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent_dir(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Converts a planner destination class into the physical placement class used by segment state.
fn placement_class_for_destination(destination_class: DestinationClass) -> PlacementClass {
    match destination_class {
        DestinationClass::ExactEpoch(epoch) => PlacementClass::ExactEpoch(epoch),
        DestinationClass::Spillover => PlacementClass::Spillover,
    }
}

/// Overlay-derived copy disposition for a single source record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverlayRecordState {
    /// The record is known expired or retired and must not be copied.
    Skip,
    /// The record is live enough to copy, with an optional known lifecycle.
    CopyEligible { lifecycle: Option<BlobLifecycle> },
}

/// Offset ordered classifier for records in one segment GC overlay.
///
/// Overlay ranges must either fully contain a record or not overlap it. Partial overlap is rejected
/// because GC only rewrites whole encoded records and cannot split payload liveness.
struct OverlayRecordClassifier<'a> {
    segment_id: SegmentId,
    overlay: &'a SegmentGcOverlay,
    expired_index: usize,
    retired_index: usize,
    lifetime_index: usize,
}

impl<'a> OverlayRecordClassifier<'a> {
    fn new(segment_id: SegmentId, overlay: &'a SegmentGcOverlay) -> Self {
        Self {
            segment_id,
            overlay,
            expired_index: 0,
            retired_index: 0,
            lifetime_index: 0,
        }
    }

    fn classify(&mut self, record: SegmentGcRecordRange) -> Result<OverlayRecordState> {
        if self.skipped_range_contains_or_overlaps(record)? {
            return Ok(OverlayRecordState::Skip);
        }

        let lifecycle = self.lifecycle_for_record(record)?;
        Ok(OverlayRecordState::CopyEligible { lifecycle })
    }

    fn skipped_range_contains_or_overlaps(&mut self, record: SegmentGcRecordRange) -> Result<bool> {
        if classify_skipped_range(
            self.segment_id,
            &self.overlay.expired,
            &mut self.expired_index,
            record,
        )? {
            return Ok(true);
        }
        classify_skipped_range(
            self.segment_id,
            &self.overlay.retired,
            &mut self.retired_index,
            record,
        )
    }

    fn lifecycle_for_record(
        &mut self,
        record: SegmentGcRecordRange,
    ) -> Result<Option<BlobLifecycle>> {
        advance_range_cursor(
            &self.overlay.lifetimes,
            &mut self.lifetime_index,
            record.offset,
            |entry| entry.range,
        );
        let Some(lifetime) = self.overlay.lifetimes.get(self.lifetime_index) else {
            return Ok(None);
        };
        if range_contains(lifetime.range, record) {
            return Ok(Some(lifetime.lifecycle));
        }
        if ranges_overlap(lifetime.range, record) {
            return Err(partial_overlay_error(self.segment_id, record));
        }
        Ok(None)
    }
}

fn classify_skipped_range(
    segment_id: SegmentId,
    ranges: &[SegmentGcRecordRange],
    index: &mut usize,
    record: SegmentGcRecordRange,
) -> Result<bool> {
    advance_range_cursor(ranges, index, record.offset, |range| *range);
    let Some(range) = ranges.get(*index).copied() else {
        return Ok(false);
    };
    if range_contains(range, record) {
        return Ok(true);
    }
    if ranges_overlap(range, record) {
        return Err(partial_overlay_error(segment_id, record));
    }
    Ok(false)
}

fn advance_range_cursor<T>(
    ranges: &[T],
    index: &mut usize,
    record_offset: u64,
    range_for: impl Fn(&T) -> SegmentGcRecordRange,
) {
    while let Some(range) = ranges.get(*index).map(&range_for) {
        if range_end(range) > record_offset {
            break;
        }
        *index += 1;
    }
}

/// Builds the error returned when an overlay range cuts through a record boundary.
fn partial_overlay_error(segment_id: SegmentId, record: SegmentGcRecordRange) -> Error {
    Error::GcOverlayPartialRecordRange {
        segment_id,
        offset: record.offset,
        len: record.len,
    }
}

/// Returns true when `container` fully covers `contained`.
fn range_contains(container: SegmentGcRecordRange, contained: SegmentGcRecordRange) -> bool {
    container.offset <= contained.offset && range_end(container) >= range_end(contained)
}

/// Returns true when two half-open byte ranges overlap.
fn ranges_overlap(left: SegmentGcRecordRange, right: SegmentGcRecordRange) -> bool {
    left.offset < range_end(right) && right.offset < range_end(left)
}

/// Computes the exclusive end offset for a record range.
fn range_end(range: SegmentGcRecordRange) -> u64 {
    range.offset.saturating_add(range.len)
}

/// Resolves the on-disk path for a GC source segment.
///
/// Older or synthetic test states may not carry `SegmentState.path`; in that case the ingest layout
/// path is derived from the segment id.
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

/// Returns true if any plan action requires source record copying.
fn plan_has_copy_action(plan: &GcPlan) -> bool {
    matches!(
        &plan.action,
        GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. }
    )
}

/// Returns source segment ids that must be scanned and copied for a plan.
fn copy_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = BTreeSet::new();
    match &plan.action {
        GcAction::MoveLiveBytes {
            source_segment_id, ..
        } => {
            source_ids.insert(*source_segment_id);
        }
        GcAction::MoveEpochBytes { routes, .. } => {
            source_ids.extend(routes.iter().map(|route| route.source_segment_id));
        }
        GcAction::DeleteSegment { .. }
        | GcAction::DeleteSegments { .. }
        | GcAction::ReclassifySegment { .. } => {}
    }
    source_ids
}

/// Returns every source segment id a plan must claim before execution.
///
/// This includes metadata-only actions such as delete and reclassify, not just copy sources.
fn gc_plan_source_segment_ids(plan: &GcPlan) -> BTreeSet<SegmentId> {
    let mut source_ids = copy_source_segment_ids(plan);
    match &plan.action {
        GcAction::DeleteSegment { segment_id } => {
            source_ids.insert(*segment_id);
        }
        GcAction::DeleteSegments { segment_ids } => {
            source_ids.extend(segment_ids.iter().copied());
        }
        GcAction::ReclassifySegment { segment_id, .. } => {
            source_ids.insert(*segment_id);
        }
        GcAction::MoveLiveBytes { .. } | GcAction::MoveEpochBytes { .. } => {}
    }
    source_ids
}
