//! Background GC admission, source claims, and worker scheduling.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Condvar, Mutex, mpsc},
    time::{Duration, Instant},
};

use core_types::SegmentId;
use gc_planner::{GcPlanner, GcSnapshot};

use super::GcExecutor;
use crate::{Result, StrataStoreMetrics};

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
pub(super) struct TunedSignal {
    /// Current exponentially weighted moving average.
    ewma_nanos: Option<u128>,
    /// Decaying baseline used to decide whether the current EWMA is unexpectedly slow.
    baseline_ewma_nanos: Option<u128>,
}

impl TunedSignal {
    /// Updates the EWMA with a new nanosecond sample.
    pub(super) fn observe(&mut self, sample_nanos: u128) {
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
    pub(super) fn degraded(&self, threshold: Duration) -> bool {
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
    pub(super) fn mark_snapshot(&self, snapshot: &mut GcSnapshot) {
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

    /// One admitted wake: finish any dropped shard generations first, then drain plans until the
    /// planner has nothing left or the per-wake cap trips.
    ///
    /// Shard cleanup runs first because it is the cheapest way to reclaim the most bytes — a
    /// whole generation directory at once — and because finishing it unblocks the claims it holds
    /// in `draining`. The cap exists so one wake on a deeply fragmented store cannot pin this
    /// worker in a plan loop forever; the next timer tick or sweeper wake simply continues.
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
pub(super) fn gc_failure_backoff(interval: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures <= 1 {
        return interval;
    }
    let exponent = (consecutive_failures - 1).min(GC_FAILURE_BACKOFF_MAX_EXPONENT);
    interval.saturating_mul(2_u32.saturating_pow(exponent))
}
