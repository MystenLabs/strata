use std::{collections::HashMap, sync::Arc, time::Duration};

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};
use strata_core::{Epoch, SegmentId, StrataLsn};

const OPERATION_LATENCY_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001,
    0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
];

#[derive(Clone, Debug, Default)]
pub struct StrataStoreMetrics {
    inner: Option<Arc<PrometheusMetrics>>,
}

#[derive(Debug)]
struct PrometheusMetrics {
    queued_write_commands: IntGauge,
    write_queue_send_duration_seconds: Histogram,
    write_queue_send_errors_total: IntCounter,
    put_calls_total: IntCounter,
    put_errors_total: IntCounter,
    put_duration_seconds: Histogram,
    put_payload_bytes_total: IntCounter,
    put_record_bytes_total: IntCounter,
    sync_calls_total: IntCounter,
    sync_errors_total: IntCounter,
    sync_duration_seconds: Histogram,
    sync_bytes_total: IntCounter,
    get_calls_total: IntCounter,
    get_hits_total: IntCounter,
    get_misses_total: IntCounter,
    get_errors_total: IntCounter,
    get_duration_seconds: Histogram,
    get_payload_bytes_total: IntCounter,
    range_read_calls_total: IntCounter,
    range_read_hits_total: IntCounter,
    range_read_misses_total: IntCounter,
    range_read_errors_total: IntCounter,
    range_read_duration_seconds: Histogram,
    range_read_payload_bytes_total: IntCounter,
    stream_calls_total: IntCounter,
    stream_hits_total: IntCounter,
    stream_misses_total: IntCounter,
    stream_errors_total: IntCounter,
    stream_duration_seconds: Histogram,
    reader_cache_evictions_total: IntCounter,
    orphaned_segment_records_total: IntCounter,
    orphaned_segment_bytes_total: IntCounter,
    active_segment_id: IntGauge,
    active_segment_write_offset: IntGauge,
    active_segment_durable_offset: IntGauge,
    next_lsn: IntGauge,
    durable_lsn: IntGauge,
    current_epoch: IntGauge,
    pending_lsn_count: IntGauge,
    unsealed_segments: IntGauge,
    seal_enqueued_total: IntCounter,
    sealed_segments_total: IntCounter,
    seal_errors_total: IntCounter,
    seal_backpressure_waits_total: IntCounter,
    seal_backpressure_wait_duration_seconds: Histogram,
    seal_backpressure_current: IntGauge,
    recovery_segments_total: IntCounter,
    recovery_complete_segments_total: IntCounter,
    recovery_incomplete_segments_total: IntCounter,
    recovery_records_total: IntCounter,
    recovery_bytes_total: IntCounter,
    recovery_discarded_segments_total: IntCounter,
    recovery_rollback_ops_total: IntCounter,
    recovery_last_rollback_from: IntGauge,
    gc_configured_workers: IntGauge,
    gc_active_worker_limit: IntGauge,
    gc_in_flight_workers: IntGauge,
    gc_admitted_total: IntCounter,
    gc_skipped_by_tuner_total: IntCounter,
    gc_tuner_increases_total: IntCounter,
    gc_tuner_decreases_total: IntCounter,
    gc_tuner_health_state: IntGauge,
}

impl StrataStoreMetrics {
    /// Creates Prometheus-backed metrics for one Strata store.
    ///
    /// `store_label` should be a low-cardinality store identifier, such as a namespace or shard
    /// name. Do not use blob keys or other unbounded values.
    pub fn new(
        registry: &Registry,
        store_label: impl Into<String>,
    ) -> Result<Self, prometheus::Error> {
        let mut labels = HashMap::new();
        labels.insert("store".to_owned(), store_label.into());

        Ok(Self {
            inner: Some(Arc::new(PrometheusMetrics {
                queued_write_commands: register_gauge(
                    registry,
                    &labels,
                    "queued_write_commands",
                    "Number of write commands queued for the Strata writer.",
                )?,
                write_queue_send_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "write_queue_send_duration_seconds",
                    "Time spent sending commands into the bounded Strata write queue.",
                )?,
                write_queue_send_errors_total: register_counter(
                    registry,
                    &labels,
                    "write_queue_send_errors_total",
                    "Total failures while sending commands into the Strata write queue.",
                )?,
                put_calls_total: register_counter(
                    registry,
                    &labels,
                    "put_calls_total",
                    "Total Strata put calls.",
                )?,
                put_errors_total: register_counter(
                    registry,
                    &labels,
                    "put_errors_total",
                    "Total failed Strata put calls.",
                )?,
                put_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "put_duration_seconds",
                    "Strata put latency in seconds.",
                )?,
                put_payload_bytes_total: register_counter(
                    registry,
                    &labels,
                    "put_payload_bytes_total",
                    "Total payload bytes accepted by Strata put calls.",
                )?,
                put_record_bytes_total: register_counter(
                    registry,
                    &labels,
                    "put_record_bytes_total",
                    "Total encoded record bytes written by Strata put calls.",
                )?,
                sync_calls_total: register_counter(
                    registry,
                    &labels,
                    "sync_calls_total",
                    "Total Strata sync calls.",
                )?,
                sync_errors_total: register_counter(
                    registry,
                    &labels,
                    "sync_errors_total",
                    "Total failed Strata sync calls.",
                )?,
                sync_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "sync_duration_seconds",
                    "Strata sync latency in seconds.",
                )?,
                sync_bytes_total: register_counter(
                    registry,
                    &labels,
                    "sync_bytes_total",
                    "Total bytes made durable by Strata sync calls.",
                )?,
                get_calls_total: register_counter(
                    registry,
                    &labels,
                    "get_calls_total",
                    "Total Strata get calls.",
                )?,
                get_hits_total: register_counter(
                    registry,
                    &labels,
                    "get_hits_total",
                    "Total Strata get calls that found a blob.",
                )?,
                get_misses_total: register_counter(
                    registry,
                    &labels,
                    "get_misses_total",
                    "Total Strata get calls that did not find a blob.",
                )?,
                get_errors_total: register_counter(
                    registry,
                    &labels,
                    "get_errors_total",
                    "Total failed Strata get calls.",
                )?,
                get_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "get_duration_seconds",
                    "Strata get latency in seconds.",
                )?,
                get_payload_bytes_total: register_counter(
                    registry,
                    &labels,
                    "get_payload_bytes_total",
                    "Total payload bytes returned by Strata get calls.",
                )?,
                range_read_calls_total: register_counter(
                    registry,
                    &labels,
                    "range_read_calls_total",
                    "Total Strata range read calls.",
                )?,
                range_read_hits_total: register_counter(
                    registry,
                    &labels,
                    "range_read_hits_total",
                    "Total Strata range reads that found a blob.",
                )?,
                range_read_misses_total: register_counter(
                    registry,
                    &labels,
                    "range_read_misses_total",
                    "Total Strata range reads that did not find a blob.",
                )?,
                range_read_errors_total: register_counter(
                    registry,
                    &labels,
                    "range_read_errors_total",
                    "Total failed Strata range reads.",
                )?,
                range_read_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "range_read_duration_seconds",
                    "Strata range read latency in seconds.",
                )?,
                range_read_payload_bytes_total: register_counter(
                    registry,
                    &labels,
                    "range_read_payload_bytes_total",
                    "Total payload bytes returned by Strata range reads.",
                )?,
                stream_calls_total: register_counter(
                    registry,
                    &labels,
                    "stream_calls_total",
                    "Total Strata stream setup calls.",
                )?,
                stream_hits_total: register_counter(
                    registry,
                    &labels,
                    "stream_hits_total",
                    "Total Strata stream setup calls that found a blob.",
                )?,
                stream_misses_total: register_counter(
                    registry,
                    &labels,
                    "stream_misses_total",
                    "Total Strata stream setup calls that did not find a blob.",
                )?,
                stream_errors_total: register_counter(
                    registry,
                    &labels,
                    "stream_errors_total",
                    "Total failed Strata stream setup calls.",
                )?,
                stream_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "stream_duration_seconds",
                    "Strata stream setup latency in seconds.",
                )?,
                reader_cache_evictions_total: register_counter(
                    registry,
                    &labels,
                    "reader_cache_evictions_total",
                    "Total Strata segment reader cache evictions.",
                )?,
                orphaned_segment_records_total: register_counter(
                    registry,
                    &labels,
                    "orphaned_segment_records_total",
                    "Total Strata records appended before their metadata batch failed.",
                )?,
                orphaned_segment_bytes_total: register_counter(
                    registry,
                    &labels,
                    "orphaned_segment_bytes_total",
                    "Total Strata segment bytes appended before their metadata batch failed.",
                )?,
                active_segment_id: register_gauge(
                    registry,
                    &labels,
                    "active_segment_id",
                    "Current active Strata segment id.",
                )?,
                active_segment_write_offset: register_gauge(
                    registry,
                    &labels,
                    "active_segment_write_offset",
                    "Current active Strata segment write offset.",
                )?,
                active_segment_durable_offset: register_gauge(
                    registry,
                    &labels,
                    "active_segment_durable_offset",
                    "Current active Strata segment durable offset.",
                )?,
                next_lsn: register_gauge(
                    registry,
                    &labels,
                    "next_lsn",
                    "Next Strata LSN to assign.",
                )?,
                durable_lsn: register_gauge(
                    registry,
                    &labels,
                    "durable_lsn",
                    "Highest contiguous durable Strata LSN.",
                )?,
                current_epoch: register_gauge(
                    registry,
                    &labels,
                    "current_epoch",
                    "Current Strata epoch.",
                )?,
                pending_lsn_count: register_gauge(
                    registry,
                    &labels,
                    "pending_lsn_count",
                    "Number of assigned Strata LSNs not yet known durable.",
                )?,
                unsealed_segments: register_gauge(
                    registry,
                    &labels,
                    "unsealed_segments",
                    "Number of unsealed Strata segments.",
                )?,
                seal_enqueued_total: register_counter(
                    registry,
                    &labels,
                    "seal_enqueued_total",
                    "Total Strata segments enqueued for sealing.",
                )?,
                sealed_segments_total: register_counter(
                    registry,
                    &labels,
                    "sealed_segments_total",
                    "Total Strata segments sealed.",
                )?,
                seal_errors_total: register_counter(
                    registry,
                    &labels,
                    "seal_errors_total",
                    "Total Strata segment seal failures.",
                )?,
                seal_backpressure_waits_total: register_counter(
                    registry,
                    &labels,
                    "seal_backpressure_waits_total",
                    "Total times Strata writes waited for unsealed segment backlog capacity.",
                )?,
                seal_backpressure_wait_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "seal_backpressure_wait_duration_seconds",
                    "Time Strata writes spent waiting for unsealed segment backlog capacity.",
                )?,
                seal_backpressure_current: register_gauge(
                    registry,
                    &labels,
                    "seal_backpressure_current",
                    "Whether the Strata writer is currently waiting for unsealed segment backlog capacity.",
                )?,
                recovery_segments_total: register_counter(
                    registry,
                    &labels,
                    "recovery_segments_total",
                    "Total Strata unsealed segments processed during recovery.",
                )?,
                recovery_complete_segments_total: register_counter(
                    registry,
                    &labels,
                    "recovery_complete_segments_total",
                    "Total complete Strata unsealed segments recovered.",
                )?,
                recovery_incomplete_segments_total: register_counter(
                    registry,
                    &labels,
                    "recovery_incomplete_segments_total",
                    "Total incomplete Strata unsealed segments recovered.",
                )?,
                recovery_records_total: register_counter(
                    registry,
                    &labels,
                    "recovery_records_total",
                    "Total Strata records recovered from unsealed segment files.",
                )?,
                recovery_bytes_total: register_counter(
                    registry,
                    &labels,
                    "recovery_bytes_total",
                    "Total Strata bytes recovered from unsealed segment files.",
                )?,
                recovery_discarded_segments_total: register_counter(
                    registry,
                    &labels,
                    "recovery_discarded_segments_total",
                    "Total Strata segment files discarded during recovery.",
                )?,
                recovery_rollback_ops_total: register_counter(
                    registry,
                    &labels,
                    "recovery_rollback_ops_total",
                    "Total Strata metadata operations rolled back during recovery.",
                )?,
                recovery_last_rollback_from: register_gauge(
                    registry,
                    &labels,
                    "recovery_last_rollback_from",
                    "Lowest Strata LSN rolled back by the most recent recovery rollback.",
                )?,
                gc_configured_workers: register_gauge(
                    registry,
                    &labels,
                    "gc_configured_workers",
                    "Configured upper bound for background Strata GC workers.",
                )?,
                gc_active_worker_limit: register_gauge(
                    registry,
                    &labels,
                    "gc_active_worker_limit",
                    "Current runtime limit for background Strata GC workers admitted concurrently.",
                )?,
                gc_in_flight_workers: register_gauge(
                    registry,
                    &labels,
                    "gc_in_flight_workers",
                    "Current number of background Strata GC workers running an admitted attempt.",
                )?,
                gc_admitted_total: register_counter(
                    registry,
                    &labels,
                    "gc_admitted_total",
                    "Total background Strata GC attempts admitted by the runtime concurrency tuner.",
                )?,
                gc_skipped_by_tuner_total: register_counter(
                    registry,
                    &labels,
                    "gc_skipped_by_tuner_total",
                    "Total background Strata GC attempts skipped because the runtime concurrency limit was full.",
                )?,
                gc_tuner_increases_total: register_counter(
                    registry,
                    &labels,
                    "gc_tuner_increases_total",
                    "Total times the Strata GC runtime tuner increased active worker concurrency.",
                )?,
                gc_tuner_decreases_total: register_counter(
                    registry,
                    &labels,
                    "gc_tuner_decreases_total",
                    "Total times the Strata GC runtime tuner decreased active worker concurrency.",
                )?,
                gc_tuner_health_state: register_gauge(
                    registry,
                    &labels,
                    "gc_tuner_health_state",
                    "Current Strata GC tuner health state: 0 healthy, 1 pressured, 2 cooldown.",
                )?,
            })),
        })
    }

    pub(crate) fn enqueue_write_command(&self) {
        if let Some(metrics) = &self.inner {
            metrics.queued_write_commands.inc();
        }
    }

    pub(crate) fn dequeue_write_command(&self) {
        if let Some(metrics) = &self.inner {
            metrics.queued_write_commands.dec();
        }
    }

    pub(crate) fn record_write_queue_send(&self, success: bool, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .write_queue_send_duration_seconds
            .observe(duration_seconds(elapsed));
        if !success {
            metrics.write_queue_send_errors_total.inc();
        }
    }

    pub(crate) fn record_put(&self, result: Result<PutMetric, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.put_calls_total.inc();
        metrics
            .put_duration_seconds
            .observe(duration_seconds(elapsed));
        match result {
            Ok(metric) => {
                metrics.put_payload_bytes_total.inc_by(metric.payload_bytes);
                metrics.put_record_bytes_total.inc_by(metric.record_bytes);
            }
            Err(()) => metrics.put_errors_total.inc(),
        }
    }

    pub(crate) fn record_sync(&self, result: Result<u64, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.sync_calls_total.inc();
        metrics
            .sync_duration_seconds
            .observe(duration_seconds(elapsed));
        match result {
            Ok(bytes) => metrics.sync_bytes_total.inc_by(bytes),
            Err(()) => metrics.sync_errors_total.inc(),
        }
    }

    pub(crate) fn record_get(&self, result: Result<Option<u64>, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.get_calls_total.inc();
        metrics
            .get_duration_seconds
            .observe(duration_seconds(elapsed));
        match result {
            Ok(Some(bytes)) => {
                metrics.get_hits_total.inc();
                metrics.get_payload_bytes_total.inc_by(bytes);
            }
            Ok(None) => metrics.get_misses_total.inc(),
            Err(()) => metrics.get_errors_total.inc(),
        }
    }

    pub(crate) fn record_range_read(&self, result: Result<Option<u64>, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.range_read_calls_total.inc();
        metrics
            .range_read_duration_seconds
            .observe(duration_seconds(elapsed));
        match result {
            Ok(Some(bytes)) => {
                metrics.range_read_hits_total.inc();
                metrics.range_read_payload_bytes_total.inc_by(bytes);
            }
            Ok(None) => metrics.range_read_misses_total.inc(),
            Err(()) => metrics.range_read_errors_total.inc(),
        }
    }

    pub(crate) fn record_stream(&self, result: Result<bool, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.stream_calls_total.inc();
        metrics
            .stream_duration_seconds
            .observe(duration_seconds(elapsed));
        match result {
            Ok(true) => metrics.stream_hits_total.inc(),
            Ok(false) => metrics.stream_misses_total.inc(),
            Err(()) => metrics.stream_errors_total.inc(),
        }
    }

    pub(crate) fn record_reader_cache_eviction(&self) {
        if let Some(metrics) = &self.inner {
            metrics.reader_cache_evictions_total.inc();
        }
    }

    pub(crate) fn record_orphaned_segment_bytes(&self, records: u64, bytes: u64) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.orphaned_segment_records_total.inc_by(records);
        metrics.orphaned_segment_bytes_total.inc_by(bytes);
    }

    pub(crate) fn set_active_segment(
        &self,
        segment_id: SegmentId,
        write_offset: u64,
        durable_offset: u64,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.active_segment_id.set(to_i64(segment_id));
        metrics
            .active_segment_write_offset
            .set(to_i64(write_offset));
        metrics
            .active_segment_durable_offset
            .set(to_i64(durable_offset));
    }

    pub(crate) fn set_lsn_state(&self, next_lsn: StrataLsn, durable_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.next_lsn.set(to_i64(next_lsn));
        metrics.durable_lsn.set(to_i64(durable_lsn));
        set_pending_lsn_count(metrics, next_lsn, durable_lsn);
    }

    pub(crate) fn set_next_lsn(&self, next_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.next_lsn.set(to_i64(next_lsn));
        set_pending_lsn_count(metrics, next_lsn, lsn_from_i64(metrics.durable_lsn.get()));
    }

    pub(crate) fn set_durable_lsn(&self, durable_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.durable_lsn.set(to_i64(durable_lsn));
        set_pending_lsn_count(metrics, lsn_from_i64(metrics.next_lsn.get()), durable_lsn);
    }

    pub(crate) fn set_current_epoch(&self, epoch: Epoch) {
        if let Some(metrics) = &self.inner {
            metrics.current_epoch.set(to_i64(epoch));
        }
    }

    pub(crate) fn set_unsealed_segments(&self, count: usize) {
        if let Some(metrics) = &self.inner {
            metrics.unsealed_segments.set(to_i64(count as u64));
        }
    }

    pub(crate) fn record_seal_enqueued(&self) {
        if let Some(metrics) = &self.inner {
            metrics.seal_enqueued_total.inc();
        }
    }

    pub(crate) fn record_segment_sealed(&self) {
        if let Some(metrics) = &self.inner {
            metrics.sealed_segments_total.inc();
        }
    }

    pub(crate) fn record_seal_error(&self) {
        if let Some(metrics) = &self.inner {
            metrics.seal_errors_total.inc();
        }
    }

    pub(crate) fn start_seal_backpressure_wait(&self) {
        if let Some(metrics) = &self.inner {
            metrics.seal_backpressure_waits_total.inc();
            metrics.seal_backpressure_current.set(1);
        }
    }

    pub(crate) fn finish_seal_backpressure_wait(&self, elapsed: Duration) {
        if let Some(metrics) = &self.inner {
            metrics
                .seal_backpressure_wait_duration_seconds
                .observe(duration_seconds(elapsed));
            metrics.seal_backpressure_current.set(0);
        }
    }

    pub(crate) fn record_recovered_segment(&self, is_complete: bool) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.recovery_segments_total.inc();
        if is_complete {
            metrics.recovery_complete_segments_total.inc();
        } else {
            metrics.recovery_incomplete_segments_total.inc();
        }
    }

    pub(crate) fn record_recovered_records(&self, records: u64, bytes: u64) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.recovery_records_total.inc_by(records);
        metrics.recovery_bytes_total.inc_by(bytes);
    }

    pub(crate) fn record_discarded_segment(&self) {
        if let Some(metrics) = &self.inner {
            metrics.recovery_discarded_segments_total.inc();
        }
    }

    pub(crate) fn record_rollback(&self, rollback_from: StrataLsn, ops: u64) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .recovery_last_rollback_from
            .set(to_i64(rollback_from));
        metrics.recovery_rollback_ops_total.inc_by(ops);
    }

    pub(crate) fn initialize_gc_tuner(&self, configured_workers: usize, active_limit: usize) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .gc_configured_workers
            .set(to_i64(configured_workers as u64));
        metrics
            .gc_active_worker_limit
            .set(to_i64(active_limit as u64));
        metrics.gc_in_flight_workers.set(0);
        metrics.gc_tuner_health_state.set(0);
    }

    pub(crate) fn set_gc_active_worker_limit(&self, active_limit: usize) {
        if let Some(metrics) = &self.inner {
            metrics
                .gc_active_worker_limit
                .set(to_i64(active_limit as u64));
        }
    }

    pub(crate) fn set_gc_in_flight_workers(&self, in_flight: usize) {
        if let Some(metrics) = &self.inner {
            metrics.gc_in_flight_workers.set(to_i64(in_flight as u64));
        }
    }

    pub(crate) fn record_gc_admitted(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_admitted_total.inc();
        }
    }

    pub(crate) fn record_gc_skipped_by_tuner(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_skipped_by_tuner_total.inc();
        }
    }

    pub(crate) fn record_gc_tuner_increase(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_tuner_increases_total.inc();
        }
    }

    pub(crate) fn record_gc_tuner_decrease(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_tuner_decreases_total.inc();
        }
    }

    pub(crate) fn set_gc_tuner_health_state(&self, state: i64) {
        if let Some(metrics) = &self.inner {
            metrics.gc_tuner_health_state.set(state);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PutMetric {
    pub payload_bytes: u64,
    pub record_bytes: u64,
}

fn register_counter(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
) -> Result<IntCounter, prometheus::Error> {
    let counter = IntCounter::with_opts(opts(labels, name, help))?;
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn register_gauge(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
) -> Result<IntGauge, prometheus::Error> {
    let gauge = IntGauge::with_opts(opts(labels, name, help))?;
    registry.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

fn register_histogram(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
) -> Result<Histogram, prometheus::Error> {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(metric_name(name), help)
            .const_labels(labels.clone())
            .buckets(OPERATION_LATENCY_BUCKETS.to_vec()),
    )?;
    registry.register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

fn opts(labels: &HashMap<String, String>, name: &str, help: &str) -> Opts {
    Opts::new(metric_name(name), help).const_labels(labels.clone())
}

fn metric_name(name: &str) -> String {
    format!("strata_store_{name}")
}

fn duration_seconds(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64()
}

fn set_pending_lsn_count(metrics: &PrometheusMetrics, next_lsn: StrataLsn, durable_lsn: StrataLsn) {
    metrics.pending_lsn_count.set(to_i64(
        next_lsn.saturating_sub(durable_lsn).saturating_sub(1),
    ));
}

fn lsn_from_i64(value: i64) -> StrataLsn {
    u64::try_from(value).unwrap_or(0)
}

fn to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}
