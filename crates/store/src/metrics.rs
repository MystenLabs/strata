use std::{collections::HashMap, sync::Arc, time::Duration};

use core_types::{Epoch, SegmentGcSummary, SegmentId, StrataLsn};
use gc_planner::{GcAction, GcScenario};
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};
use segment::SegmentIoObserver;

#[cfg(feature = "internal-profiling")]
use crate::StoreProfileSink;
use crate::{StoreSyncProfile, StoreWriteProfile};

const OPERATION_LATENCY_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001,
    0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
];

const SYNC_PHASE_DURATION_BUCKETS: &[f64] = &[
    0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001,
    0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

const COMPACTION_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 1_200.0, 3_600.0,
];

#[derive(Clone, Debug, Default)]
pub struct StrataStoreMetrics {
    inner: Option<Arc<PrometheusMetrics>>,
    #[cfg(feature = "internal-profiling")]
    profile_sink: Option<Arc<dyn StoreProfileSink>>,
}

#[derive(Debug)]
struct PrometheusMetrics {
    queued_write_commands: IntGauge,
    write_queue_send_duration_seconds: Histogram,
    write_queue_send_errors_total: IntCounter,
    requests_per_commit_group: Histogram,
    put_calls_total: IntCounter,
    put_errors_total: IntCounter,
    put_duration_seconds: Histogram,
    put_payload_bytes_total: IntCounter,
    put_record_bytes_total: IntCounter,
    segment_file_bytes_read_total: IntCounter,
    segment_file_bytes_written_total: IntCounter,
    delete_calls_total: IntCounter,
    delete_errors_total: IntCounter,
    delete_duration_seconds: Histogram,
    sync_calls_total: IntCounter,
    sync_errors_total: IntCounter,
    sync_duration_seconds: Histogram,
    sync_phase_duration_seconds: HistogramVec,
    wal_reclaim_phase_duration_seconds: HistogramVec,
    sync_bytes_total: IntCounter,
    durability_wal_bytes_total: IntCounter,
    durability_pending_wal_bytes: IntGauge,
    durability_pending_segment_bytes: IntGauge,
    durability_publish_in_flight: IntGauge,
    get_calls_total: IntCounter,
    get_hits_total: IntCounter,
    get_misses_total: IntCounter,
    get_errors_total: IntCounter,
    get_duration_seconds: Histogram,
    get_payload_bytes_total: IntCounter,
    relocation_cache_requests_total: IntCounterVec,
    relocation_lookups_total: IntCounterVec,
    relocation_lookup_duration_seconds: Histogram,
    main_compaction_healed_references_total: IntCounter,
    main_compaction_duration_seconds: Histogram,
    main_compaction_input_bytes_total: IntCounter,
    main_compaction_output_bytes_total: IntCounter,
    main_minor_compaction_lsn: IntGauge,
    main_full_compaction_lsn: IntGauge,
    relocation_compaction_entries_examined_total: IntCounter,
    relocation_compaction_entries_dropped_total: IntCounter,
    relocation_compaction_duration_seconds: Histogram,
    relocation_compaction_input_bytes_total: IntCounter,
    relocation_compaction_output_bytes_total: IntCounter,
    relocation_compaction_passes_total: IntCounterVec,
    relocation_compaction_pass_input_bytes_total: IntCounterVec,
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
    published_lsn: IntGauge,
    gc_known_total_bytes: IntGauge,
    gc_known_live_bytes: IntGauge,
    gc_known_retired_bytes: IntGauge,
    gc_known_expired_bytes: IntGauge,
    gc_known_live_ref_count: IntGauge,
    gc_relocating_segments: IntGauge,
    gc_output_bytes_total: IntCounter,
    gc_source_deleted_bytes_total: IntCounter,
    gc_reclaimed_bytes_total: IntCounter,
    gc_strategy_selected_total: IntCounterVec,
    gc_strategy_completed_total: IntCounterVec,
    gc_strategy_output_bytes_total: IntCounterVec,
    gc_strategy_source_deleted_bytes_total: IntCounterVec,
    gc_strategy_reclaimed_bytes_total: IntCounterVec,
    current_epoch: IntGauge,
    pending_lsn_count: IntGauge,
    unsealed_segments: IntGauge,
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
    gc_configured_io_bytes_per_sec: IntGauge,
    gc_min_io_bytes_per_sec: IntGauge,
    gc_active_io_bytes_per_sec: IntGauge,
    gc_in_flight_workers: IntGauge,
    gc_admitted_total: IntCounter,
    gc_run_failures_total: IntCounter,
    gc_run_failures_by_reason_total: IntCounterVec,
    gc_consecutive_run_failures: IntGauge,
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
                requests_per_commit_group: register_histogram_with_buckets(
                    registry,
                    &labels,
                    "requests_per_commit_group",
                    "Successfully committed client batch requests per physical writer commit.",
                    vec![1.0, 2.0, 4.0, 8.0, 16.0],
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
                segment_file_bytes_read_total: register_counter(
                    registry,
                    &labels,
                    "segment_file_bytes_read_total",
                    "Total bytes successfully read from Strata segment files.",
                )?,
                segment_file_bytes_written_total: register_counter(
                    registry,
                    &labels,
                    "segment_file_bytes_written_total",
                    "Total bytes successfully appended to Strata segment files.",
                )?,
                delete_calls_total: register_counter(
                    registry,
                    &labels,
                    "delete_calls_total",
                    "Total Strata delete calls.",
                )?,
                delete_errors_total: register_counter(
                    registry,
                    &labels,
                    "delete_errors_total",
                    "Total failed Strata delete calls.",
                )?,
                delete_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "delete_duration_seconds",
                    "Strata delete latency in seconds.",
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
                sync_duration_seconds: register_histogram_with_buckets(
                    registry,
                    &labels,
                    "sync_duration_seconds",
                    "Strata sync latency in seconds.",
                    SYNC_PHASE_DURATION_BUCKETS.to_vec(),
                )?,
                sync_phase_duration_seconds: register_histogram_vec_with_buckets(
                    registry,
                    &labels,
                    "sync_phase_duration_seconds",
                    "Successful Strata durability publication latency by non-overlapping phase.",
                    &["phase"],
                    SYNC_PHASE_DURATION_BUCKETS.to_vec(),
                )?,
                wal_reclaim_phase_duration_seconds: register_histogram_vec_with_buckets(
                    registry,
                    &labels,
                    "wal_reclaim_phase_duration_seconds",
                    "Background store-WAL reclamation latency by phase.",
                    &["phase"],
                    SYNC_PHASE_DURATION_BUCKETS.to_vec(),
                )?,
                sync_bytes_total: register_counter(
                    registry,
                    &labels,
                    "sync_bytes_total",
                    "Total bytes made durable by Strata sync calls.",
                )?,
                durability_wal_bytes_total: register_counter(
                    registry,
                    &labels,
                    "durability_wal_bytes_total",
                    "Total store-WAL bytes covered by successful durability publications.",
                )?,
                durability_pending_wal_bytes: register_gauge(
                    registry,
                    &labels,
                    "durability_pending_wal_bytes",
                    "Store-WAL bytes committed since the latest captured durability boundary.",
                )?,
                durability_pending_segment_bytes: register_gauge(
                    registry,
                    &labels,
                    "durability_pending_segment_bytes",
                    "Encoded segment bytes committed since the latest captured durability boundary.",
                )?,
                durability_publish_in_flight: register_gauge(
                    registry,
                    &labels,
                    "durability_publish_in_flight",
                    "Whether one asynchronous durability publication is currently in flight.",
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
                relocation_cache_requests_total: register_counter_vec(
                    registry,
                    &labels,
                    "relocation_cache_requests_total",
                    "Total relocation-cache requests by result.",
                    &["result"],
                )?,
                relocation_lookups_total: register_counter_vec(
                    registry,
                    &labels,
                    "relocation_lookups_total",
                    "Total relocation LSM point lookups by result.",
                    &["result"],
                )?,
                relocation_lookup_duration_seconds: register_histogram(
                    registry,
                    &labels,
                    "relocation_lookup_duration_seconds",
                    "Relocation LSM point lookup latency in seconds.",
                )?,
                main_compaction_healed_references_total: register_counter(
                    registry,
                    &labels,
                    "main_compaction_healed_references_total",
                    "Total physical references updated by successful main LSM compactions.",
                )?,
                main_compaction_duration_seconds: register_histogram_with_buckets(
                    registry,
                    &labels,
                    "main_compaction_duration_seconds",
                    "Wall-clock duration of successful main LSM compactions.",
                    COMPACTION_DURATION_BUCKETS.to_vec(),
                )?,
                main_compaction_input_bytes_total: register_counter(
                    registry,
                    &labels,
                    "main_compaction_input_bytes_total",
                    "Encoded main SST input bytes consumed by successful compactions; this is not a device I/O counter.",
                )?,
                main_compaction_output_bytes_total: register_counter(
                    registry,
                    &labels,
                    "main_compaction_output_bytes_total",
                    "Encoded main SST output bytes created by successful compactions; this is not a device I/O counter.",
                )?,
                main_minor_compaction_lsn: register_gauge(
                    registry,
                    &labels,
                    "main_minor_compaction_lsn",
                    "Highest durable LSN cutoff processed by a successful minor main LSM compaction in this process.",
                )?,
                main_full_compaction_lsn: register_gauge(
                    registry,
                    &labels,
                    "main_full_compaction_lsn",
                    "Highest durable LSN cutoff processed by a successful full main LSM compaction in this process.",
                )?,
                relocation_compaction_entries_examined_total: register_counter(
                    registry,
                    &labels,
                    "relocation_compaction_entries_examined_total",
                    "Total logical relocation entries examined by successful relocation LSM compactions.",
                )?,
                relocation_compaction_entries_dropped_total: register_counter(
                    registry,
                    &labels,
                    "relocation_compaction_entries_dropped_total",
                    "Total relocation entries targeting deleted segments dropped by successful relocation LSM compactions.",
                )?,
                relocation_compaction_duration_seconds: register_histogram_with_buckets(
                    registry,
                    &labels,
                    "relocation_compaction_duration_seconds",
                    "Wall-clock duration of successful relocation LSM compactions.",
                    COMPACTION_DURATION_BUCKETS.to_vec(),
                )?,
                relocation_compaction_input_bytes_total: register_counter(
                    registry,
                    &labels,
                    "relocation_compaction_input_bytes_total",
                    "Encoded relocation SST input bytes consumed by successful compactions; this is not a device I/O counter.",
                )?,
                relocation_compaction_output_bytes_total: register_counter(
                    registry,
                    &labels,
                    "relocation_compaction_output_bytes_total",
                    "Encoded relocation SST output bytes created by successful compactions; this is not a device I/O counter.",
                )?,
                relocation_compaction_passes_total: register_counter_vec(
                    registry,
                    &labels,
                    "relocation_compaction_passes_total",
                    "Total relocation LSM compaction passes by shape (partial merges a patch tier; full rewrites base tables).",
                    &["kind"],
                )?,
                relocation_compaction_pass_input_bytes_total: register_counter_vec(
                    registry,
                    &labels,
                    "relocation_compaction_pass_input_bytes_total",
                    "Total relocation LSM compaction input bytes by pass shape.",
                    &["kind"],
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
                published_lsn: register_gauge(
                    registry,
                    &labels,
                    "published_lsn",
                    "Highest contiguous durable Strata LSN.",
                )?,
                gc_known_total_bytes: register_gauge(
                    registry,
                    &labels,
                    "gc_known_total_bytes",
                    "Encoded segment bytes classified in Strata GC summaries.",
                )?,
                gc_known_live_bytes: register_gauge(
                    registry,
                    &labels,
                    "gc_known_live_bytes",
                    "Encoded segment bytes currently classified live in Strata GC summaries.",
                )?,
                gc_known_retired_bytes: register_gauge(
                    registry,
                    &labels,
                    "gc_known_retired_bytes",
                    "Encoded segment bytes currently classified retired in Strata GC summaries.",
                )?,
                gc_known_expired_bytes: register_gauge(
                    registry,
                    &labels,
                    "gc_known_expired_bytes",
                    "Encoded segment bytes currently classified expired in Strata GC summaries.",
                )?,
                gc_known_live_ref_count: register_gauge(
                    registry,
                    &labels,
                    "gc_known_live_ref_count",
                    "Physical references currently classified live in Strata GC summaries.",
                )?,
                gc_relocating_segments: register_gauge(
                    registry,
                    &labels,
                    "gc_relocating_segments",
                    "Sealed source segments fenced between GC relocation publish and final deletion.",
                )?,
                gc_output_bytes_total: register_counter(
                    registry,
                    &labels,
                    "gc_output_bytes_total",
                    "Encoded bytes in GC output segments that were successfully published.",
                )?,
                gc_source_deleted_bytes_total: register_counter(
                    registry,
                    &labels,
                    "gc_source_deleted_bytes_total",
                    "Physical bytes in GC source segment files that were successfully unlinked.",
                )?,
                gc_reclaimed_bytes_total: register_counter(
                    registry,
                    &labels,
                    "gc_reclaimed_bytes_total",
                    "Net physical bytes reclaimed by GC after subtracting replacement output bytes from successfully unlinked source bytes.",
                )?,
                gc_strategy_selected_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_strategy_selected_total",
                    "GC plans successfully selected and source-claimed, classified by strategy and action.",
                    &["strategy", "action"],
                )?,
                gc_strategy_completed_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_strategy_completed_total",
                    "GC plans whose metadata or relocation publication completed successfully, classified by strategy and action.",
                    &["strategy", "action"],
                )?,
                gc_strategy_output_bytes_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_strategy_output_bytes_total",
                    "Encoded bytes in successfully published GC output segments, classified by originating strategy.",
                    &["strategy"],
                )?,
                gc_strategy_source_deleted_bytes_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_strategy_source_deleted_bytes_total",
                    "Physical source-file bytes successfully unlinked, attributed to the GC strategy that made them reclaimable.",
                    &["strategy"],
                )?,
                gc_strategy_reclaimed_bytes_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_strategy_reclaimed_bytes_total",
                    "Net physical bytes reclaimed after subtracting replacement output, attributed to the originating GC strategy.",
                    &["strategy"],
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
                gc_configured_io_bytes_per_sec: register_gauge(
                    registry,
                    &labels,
                    "gc_configured_io_bytes_per_sec",
                    "Configured healthy upper bound for background Strata GC disk I/O bytes per second.",
                )?,
                gc_min_io_bytes_per_sec: register_gauge(
                    registry,
                    &labels,
                    "gc_min_io_bytes_per_sec",
                    "Configured lower bound for background Strata GC disk I/O bytes per second under foreground pressure.",
                )?,
                gc_active_io_bytes_per_sec: register_gauge(
                    registry,
                    &labels,
                    "gc_active_io_bytes_per_sec",
                    "Current runtime background Strata GC disk I/O budget in bytes per second.",
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
                gc_run_failures_total: register_counter(
                    registry,
                    &labels,
                    "gc_run_failures_total",
                    "Total admitted background Strata GC attempts that ended in an error.",
                )?,
                gc_run_failures_by_reason_total: register_counter_vec(
                    registry,
                    &labels,
                    "gc_run_failures_by_reason_total",
                    "Total admitted background Strata GC attempts that ended in an error, classified by bounded reason.",
                    &["reason"],
                )?,
                gc_consecutive_run_failures: register_gauge(
                    registry,
                    &labels,
                    "gc_consecutive_run_failures",
                    "Consecutive failed background Strata GC attempts since the last success; alert when this keeps growing.",
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
            #[cfg(feature = "internal-profiling")]
            profile_sink: None,
        })
    }

    #[cfg(feature = "internal-profiling")]
    pub fn with_profile_sink(mut self, sink: Arc<dyn StoreProfileSink>) -> Self {
        self.profile_sink = Some(sink);
        self
    }

    pub(crate) fn internal_profile_enabled(&self) -> bool {
        #[cfg(feature = "internal-profiling")]
        {
            self.profile_sink.is_some()
        }

        #[cfg(not(feature = "internal-profiling"))]
        {
            false
        }
    }

    pub(crate) fn record_write_profile(&self, profile: StoreWriteProfile) {
        #[cfg(feature = "internal-profiling")]
        {
            if let Some(sink) = &self.profile_sink {
                sink.record_write(profile);
            }
        }

        #[cfg(not(feature = "internal-profiling"))]
        {
            let _ = profile;
        }
    }

    pub(crate) fn record_sync_profile(&self, profile: StoreSyncProfile) {
        #[cfg(feature = "internal-profiling")]
        {
            if let Some(sink) = &self.profile_sink {
                sink.record_sync(profile);
            }
        }

        #[cfg(not(feature = "internal-profiling"))]
        {
            let _ = profile;
        }
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

    pub(crate) fn record_commit_group(&self, requests: usize) {
        if let Some(metrics) = &self.inner {
            metrics.requests_per_commit_group.observe(requests as f64);
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

    pub(crate) fn record_delete(&self, success: bool, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.delete_calls_total.inc();
        metrics
            .delete_duration_seconds
            .observe(duration_seconds(elapsed));
        if !success {
            metrics.delete_errors_total.inc();
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

    pub(crate) fn record_sync_phases(&self, profile: &StoreSyncProfile, total: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        let phases = [
            ("capture", profile.capture),
            ("segment_files", profile.segment_file_sync),
            ("wal", profile.wal_sync),
            ("completion_queue", profile.completion_queue_wait),
            ("relocation_lock", profile.relocation_lock_wait),
            ("metadata_build", profile.published_lsn_compute),
            ("index_sync_commit", profile.index_batch_commit),
            ("state_update", profile.state_update),
            ("wal_reclaim", profile.wal_reclaim),
        ];
        let mut accounted = Duration::ZERO;
        for (phase, elapsed) in phases {
            metrics
                .sync_phase_duration_seconds
                .with_label_values(&[phase])
                .observe(duration_seconds(elapsed));
            accounted = accounted.saturating_add(elapsed);
        }
        metrics
            .sync_phase_duration_seconds
            .with_label_values(&["unattributed"])
            .observe(duration_seconds(total.saturating_sub(accounted)));
    }

    pub(crate) fn record_wal_reclaim_phase(&self, phase: &str, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .wal_reclaim_phase_duration_seconds
            .with_label_values(&[phase])
            .observe(duration_seconds(elapsed));
    }

    pub(crate) fn set_durability_pending(
        &self,
        wal_bytes: u64,
        segment_bytes: u64,
        in_flight: bool,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.durability_pending_wal_bytes.set(to_i64(wal_bytes));
        metrics
            .durability_pending_segment_bytes
            .set(to_i64(segment_bytes));
        metrics
            .durability_publish_in_flight
            .set(i64::from(in_flight));
    }

    pub(crate) fn record_durability_wal_bytes(&self, bytes: u64) {
        if let Some(metrics) = &self.inner {
            metrics.durability_wal_bytes_total.inc_by(bytes);
        }
    }

    pub(crate) fn record_segment_file_read(&self, bytes: u64) {
        if let Some(metrics) = &self.inner {
            metrics.segment_file_bytes_read_total.inc_by(bytes);
        }
    }

    pub(crate) fn record_segment_file_write(&self, bytes: u64) {
        if let Some(metrics) = &self.inner {
            metrics.segment_file_bytes_written_total.inc_by(bytes);
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

    pub(crate) fn record_relocation_lookup(&self, result: Result<bool, ()>, elapsed: Duration) {
        let Some(metrics) = &self.inner else {
            return;
        };
        let result = match result {
            Ok(true) => "hit",
            Ok(false) => "miss",
            Err(()) => "error",
        };
        metrics
            .relocation_lookups_total
            .with_label_values(&[result])
            .inc();
        metrics
            .relocation_lookup_duration_seconds
            .observe(duration_seconds(elapsed));
    }

    pub(crate) fn record_relocation_cache_lookup(&self, hit: bool) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .relocation_cache_requests_total
            .with_label_values(&[if hit { "hit" } else { "miss" }])
            .inc();
    }

    pub(crate) fn record_main_compaction(
        &self,
        kind: MainCompactionKind,
        compacted_through_lsn: StrataLsn,
        healed: u64,
        input_bytes: u64,
        output_bytes: u64,
        elapsed: Duration,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .main_compaction_healed_references_total
            .inc_by(healed);
        metrics
            .main_compaction_duration_seconds
            .observe(duration_seconds(elapsed));
        metrics
            .main_compaction_input_bytes_total
            .inc_by(input_bytes);
        metrics
            .main_compaction_output_bytes_total
            .inc_by(output_bytes);
        let lsn = match kind {
            MainCompactionKind::Minor => &metrics.main_minor_compaction_lsn,
            MainCompactionKind::Full => &metrics.main_full_compaction_lsn,
        };
        lsn.set(lsn.get().max(to_i64(compacted_through_lsn)));
    }

    pub(crate) fn record_relocation_compaction(
        &self,
        examined: u64,
        dropped: u64,
        input_bytes: u64,
        output_bytes: u64,
        elapsed: Duration,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .relocation_compaction_entries_examined_total
            .inc_by(examined);
        metrics
            .relocation_compaction_entries_dropped_total
            .inc_by(dropped);
        metrics
            .relocation_compaction_duration_seconds
            .observe(duration_seconds(elapsed));
        metrics
            .relocation_compaction_input_bytes_total
            .inc_by(input_bytes);
        metrics
            .relocation_compaction_output_bytes_total
            .inc_by(output_bytes);
    }

    pub(crate) fn record_relocation_compaction_pass(&self, kind: &str, input_bytes: u64) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .relocation_compaction_passes_total
            .with_label_values(&[kind])
            .inc();
        metrics
            .relocation_compaction_pass_input_bytes_total
            .with_label_values(&[kind])
            .inc_by(input_bytes);
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

    pub(crate) fn set_lsn_state(&self, next_lsn: StrataLsn, published_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.next_lsn.set(to_i64(next_lsn));
        metrics.published_lsn.set(to_i64(published_lsn));
        set_pending_lsn_count(metrics, next_lsn, published_lsn);
    }

    pub(crate) fn set_next_lsn(&self, next_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.next_lsn.set(to_i64(next_lsn));
        set_pending_lsn_count(metrics, next_lsn, lsn_from_i64(metrics.published_lsn.get()));
    }

    pub(crate) fn set_published_lsn(&self, published_lsn: StrataLsn) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.published_lsn.set(to_i64(published_lsn));
        set_pending_lsn_count(metrics, lsn_from_i64(metrics.next_lsn.get()), published_lsn);
    }

    pub(crate) fn initialize_gc_known(&self, summary: &SegmentGcSummary) {
        let Some(metrics) = &self.inner else {
            return;
        };
        set_gc_known_summary(metrics, summary);
    }

    pub(crate) fn apply_gc_known_delta(&self, delta: GcKnownDelta) {
        if let Some(metrics) = &self.inner {
            apply_gc_known_delta(metrics, delta);
        }
    }

    pub(crate) fn remove_gc_known_summary(&self, summary: &SegmentGcSummary) {
        self.apply_gc_known_delta(GcKnownDelta {
            total_bytes: -i128::from(summary.total_bytes),
            live_bytes: -i128::from(summary.live_bytes),
            retired_bytes: -i128::from(summary.retired_bytes),
            expired_bytes: -i128::from(summary.expired_bytes),
            live_ref_count: -i128::from(summary.live_ref_count),
        });
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

    pub(crate) fn initialize_gc_tuner(
        &self,
        configured_workers: usize,
        active_limit: usize,
        configured_io_bytes_per_sec: u64,
        min_io_bytes_per_sec: u64,
        active_io_bytes_per_sec: u64,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics
            .gc_configured_workers
            .set(to_i64(configured_workers as u64));
        metrics
            .gc_active_worker_limit
            .set(to_i64(active_limit as u64));
        metrics
            .gc_configured_io_bytes_per_sec
            .set(to_i64(configured_io_bytes_per_sec));
        metrics
            .gc_min_io_bytes_per_sec
            .set(to_i64(min_io_bytes_per_sec));
        metrics
            .gc_active_io_bytes_per_sec
            .set(to_i64(active_io_bytes_per_sec));
        metrics.gc_in_flight_workers.set(0);
        metrics.gc_tuner_health_state.set(0);
        metrics.gc_consecutive_run_failures.set(0);
    }

    pub(crate) fn set_gc_active_worker_limit(&self, active_limit: usize) {
        if let Some(metrics) = &self.inner {
            metrics
                .gc_active_worker_limit
                .set(to_i64(active_limit as u64));
        }
    }

    pub(crate) fn set_gc_active_io_bytes_per_sec(&self, bytes_per_sec: u64) {
        if let Some(metrics) = &self.inner {
            metrics
                .gc_active_io_bytes_per_sec
                .set(to_i64(bytes_per_sec));
        }
    }

    pub(crate) fn set_gc_in_flight_workers(&self, in_flight: usize) {
        if let Some(metrics) = &self.inner {
            metrics.gc_in_flight_workers.set(to_i64(in_flight as u64));
        }
    }

    pub(crate) fn set_gc_relocating_segments(&self, count: usize) {
        if let Some(metrics) = &self.inner {
            metrics.gc_relocating_segments.set(to_i64(count as u64));
        }
    }

    pub(crate) fn add_gc_relocating_segments(&self, count: usize) {
        if let Some(metrics) = &self.inner {
            apply_gauge_delta(&metrics.gc_relocating_segments, count as i128);
        }
    }

    pub(crate) fn remove_gc_relocating_segments(&self, count: usize) {
        if let Some(metrics) = &self.inner {
            apply_gauge_delta(&metrics.gc_relocating_segments, -(count as i128));
        }
    }

    /// Records physical bytes made visible by a successful GC relocation publication.
    pub(crate) fn record_gc_output_published(&self, strategy: &str, output_bytes: u64) {
        let Some(metrics) = &self.inner else {
            return;
        };
        metrics.gc_output_bytes_total.inc_by(output_bytes);
        metrics
            .gc_strategy_output_bytes_total
            .with_label_values(&[strategy])
            .inc_by(output_bytes);
    }

    /// Records a source file that GC successfully unlinked.
    pub(crate) fn record_gc_source_deleted(
        &self,
        strategy: &str,
        source_bytes: u64,
        copied_bytes: u64,
    ) {
        let Some(metrics) = &self.inner else {
            return;
        };
        let reclaimed_bytes = source_bytes.saturating_sub(copied_bytes);
        metrics.gc_source_deleted_bytes_total.inc_by(source_bytes);
        metrics.gc_reclaimed_bytes_total.inc_by(reclaimed_bytes);
        metrics
            .gc_strategy_source_deleted_bytes_total
            .with_label_values(&[strategy])
            .inc_by(source_bytes);
        metrics
            .gc_strategy_reclaimed_bytes_total
            .with_label_values(&[strategy])
            .inc_by(reclaimed_bytes);
    }

    pub(crate) fn record_gc_strategy_selected(&self, scenario: GcScenario, action: &GcAction) {
        if let Some(metrics) = &self.inner {
            metrics
                .gc_strategy_selected_total
                .with_label_values(&[scenario.metric_label(), action.metric_label()])
                .inc();
        }
    }

    pub(crate) fn record_gc_strategy_completed(&self, scenario: GcScenario, action: &GcAction) {
        if let Some(metrics) = &self.inner {
            metrics
                .gc_strategy_completed_total
                .with_label_values(&[scenario.metric_label(), action.metric_label()])
                .inc();
        }
    }

    pub(crate) fn record_gc_admitted(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_admitted_total.inc();
        }
    }

    pub(crate) fn record_gc_run_failure(&self, reason: &str) {
        if let Some(metrics) = &self.inner {
            metrics.gc_run_failures_total.inc();
            metrics
                .gc_run_failures_by_reason_total
                .with_label_values(&[reason])
                .inc();
            metrics.gc_consecutive_run_failures.inc();
        }
    }

    pub(crate) fn record_gc_run_success(&self) {
        if let Some(metrics) = &self.inner {
            metrics.gc_consecutive_run_failures.set(0);
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

impl SegmentIoObserver for StrataStoreMetrics {
    fn record_read(&self, bytes: u64) {
        self.record_segment_file_read(bytes);
    }

    fn record_write(&self, bytes: u64) {
        self.record_segment_file_write(bytes);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PutMetric {
    pub payload_bytes: u64,
    pub record_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MainCompactionKind {
    Minor,
    Full,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GcKnownDelta {
    pub total_bytes: i128,
    pub live_bytes: i128,
    pub retired_bytes: i128,
    pub expired_bytes: i128,
    pub live_ref_count: i128,
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
    register_histogram_with_buckets(
        registry,
        labels,
        name,
        help,
        OPERATION_LATENCY_BUCKETS.to_vec(),
    )
}

fn register_histogram_with_buckets(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
    buckets: Vec<f64>,
) -> Result<Histogram, prometheus::Error> {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(metric_name(name), help)
            .const_labels(labels.clone())
            .buckets(buckets),
    )?;
    registry.register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

fn register_histogram_vec_with_buckets(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
    variable_labels: &[&str],
    buckets: Vec<f64>,
) -> Result<HistogramVec, prometheus::Error> {
    let histogram = HistogramVec::new(
        HistogramOpts::new(metric_name(name), help)
            .const_labels(labels.clone())
            .buckets(buckets),
        variable_labels,
    )?;
    registry.register(Box::new(histogram.clone()))?;
    Ok(histogram)
}

fn register_counter_vec(
    registry: &Registry,
    labels: &HashMap<String, String>,
    name: &str,
    help: &str,
    variable_labels: &[&str],
) -> Result<IntCounterVec, prometheus::Error> {
    let counter = IntCounterVec::new(opts(labels, name, help), variable_labels)?;
    registry.register(Box::new(counter.clone()))?;
    Ok(counter)
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

fn set_pending_lsn_count(
    metrics: &PrometheusMetrics,
    next_lsn: StrataLsn,
    published_lsn: StrataLsn,
) {
    metrics.pending_lsn_count.set(to_i64(
        next_lsn.saturating_sub(published_lsn).saturating_sub(1),
    ));
}

fn set_gc_known_summary(metrics: &PrometheusMetrics, summary: &SegmentGcSummary) {
    metrics
        .gc_known_total_bytes
        .set(to_i64(summary.total_bytes));
    metrics.gc_known_live_bytes.set(to_i64(summary.live_bytes));
    metrics
        .gc_known_retired_bytes
        .set(to_i64(summary.retired_bytes));
    metrics
        .gc_known_expired_bytes
        .set(to_i64(summary.expired_bytes));
    metrics
        .gc_known_live_ref_count
        .set(to_i64(summary.live_ref_count));
}

fn apply_gc_known_delta(metrics: &PrometheusMetrics, delta: GcKnownDelta) {
    apply_gauge_delta(&metrics.gc_known_total_bytes, delta.total_bytes);
    apply_gauge_delta(&metrics.gc_known_live_bytes, delta.live_bytes);
    apply_gauge_delta(&metrics.gc_known_retired_bytes, delta.retired_bytes);
    apply_gauge_delta(&metrics.gc_known_expired_bytes, delta.expired_bytes);
    apply_gauge_delta(&metrics.gc_known_live_ref_count, delta.live_ref_count);
}

fn apply_gauge_delta(gauge: &IntGauge, delta: i128) {
    let next = i128::from(gauge.get()).saturating_add(delta);
    gauge.set(next.clamp(0, i128::from(i64::MAX)) as i64);
}

fn lsn_from_i64(value: i64) -> StrataLsn {
    u64::try_from(value).unwrap_or(0)
}

fn to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::proto::MetricType;

    fn metric_value(registry: &Registry, name: &str) -> f64 {
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .expect("metric family registered");
        let metric = &family.metric[0];
        match family.type_() {
            MetricType::COUNTER => metric.counter.value(),
            MetricType::GAUGE => metric.gauge.value(),
            other => panic!("unexpected metric type {other:?} for {name}"),
        }
    }

    fn metric_value_with_labels(registry: &Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .expect("metric family registered");
        let metric = family
            .metric
            .iter()
            .find(|metric| {
                labels.iter().all(|(name, value)| {
                    metric
                        .label
                        .iter()
                        .any(|label| label.name() == *name && label.value() == *value)
                })
            })
            .expect("metric with labels registered");
        match family.type_() {
            MetricType::COUNTER => metric.counter.value(),
            MetricType::GAUGE => metric.gauge.value(),
            other => panic!("unexpected metric type {other:?} for {name}"),
        }
    }

    fn histogram_value_with_labels(
        registry: &Registry,
        name: &str,
        labels: &[(&str, &str)],
    ) -> (u64, f64) {
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .expect("metric family registered");
        let metric = family
            .metric
            .iter()
            .find(|metric| {
                labels.iter().all(|(name, value)| {
                    metric
                        .label
                        .iter()
                        .any(|label| label.name() == *name && label.value() == *value)
                })
            })
            .expect("histogram with labels registered");
        assert_eq!(family.type_(), MetricType::HISTOGRAM);
        (
            metric.histogram.sample_count(),
            metric.histogram.sample_sum(),
        )
    }

    #[test]
    fn sync_phase_histograms_record_each_phase_and_residual() {
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "test").unwrap();
        let profile = StoreSyncProfile {
            capture: Duration::from_millis(1),
            segment_file_sync: Duration::from_millis(2),
            wal_sync: Duration::from_millis(3),
            completion_queue_wait: Duration::from_millis(4),
            relocation_lock_wait: Duration::from_millis(5),
            published_lsn_compute: Duration::from_millis(6),
            index_batch_commit: Duration::from_millis(7),
            state_update: Duration::from_millis(8),
            wal_reclaim: Duration::from_millis(9),
            ..StoreSyncProfile::default()
        };

        metrics.record_sync_phases(&profile, Duration::from_millis(50));

        for (phase, expected_seconds) in [
            ("capture", 0.001),
            ("segment_files", 0.002),
            ("wal", 0.003),
            ("completion_queue", 0.004),
            ("relocation_lock", 0.005),
            ("metadata_build", 0.006),
            ("index_sync_commit", 0.007),
            ("state_update", 0.008),
            ("wal_reclaim", 0.009),
            ("unattributed", 0.005),
        ] {
            let (count, sum) = histogram_value_with_labels(
                &registry,
                "strata_store_sync_phase_duration_seconds",
                &[("phase", phase)],
            );
            assert_eq!(count, 1, "phase {phase}");
            assert!(
                (sum - expected_seconds).abs() < f64::EPSILON,
                "phase {phase}"
            );
        }
    }

    #[test]
    fn wal_reclaim_phase_histograms_record_background_work() {
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "test").unwrap();

        metrics.record_wal_reclaim_phase("materialize", Duration::from_millis(7));

        let (count, sum) = histogram_value_with_labels(
            &registry,
            "strata_store_wal_reclaim_phase_duration_seconds",
            &[("phase", "materialize")],
        );
        assert_eq!(count, 1);
        assert!((sum - 0.007).abs() < f64::EPSILON);
    }

    #[test]
    fn gc_run_outcomes_count_failures_and_reset_consecutive_on_success() {
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "test").unwrap();

        metrics.record_gc_run_failure("io");
        metrics.record_gc_run_failure("source_not_sealed");
        assert_eq!(
            metric_value(&registry, "strata_store_gc_run_failures_total"),
            2.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_run_failures_by_reason_total",
                &[("reason", "io")],
            ),
            1.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_run_failures_by_reason_total",
                &[("reason", "source_not_sealed")],
            ),
            1.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_gc_consecutive_run_failures"),
            2.0
        );

        metrics.record_gc_run_success();
        assert_eq!(
            metric_value(&registry, "strata_store_gc_run_failures_total"),
            2.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_gc_consecutive_run_failures"),
            0.0
        );
    }

    #[test]
    fn gc_strategy_metrics_separate_attempts_and_actual_byte_attribution() {
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "test").unwrap();
        let action = GcAction::MoveLiveBytes {
            source_segment_id: 7,
            routes: Vec::new(),
        };

        metrics.record_gc_strategy_selected(GcScenario::L0Compaction, &action);
        metrics.record_gc_strategy_selected(GcScenario::L0Compaction, &action);
        metrics.record_gc_strategy_completed(GcScenario::L0Compaction, &action);
        metrics.record_gc_output_published("l0_compaction", 40);
        metrics.record_gc_source_deleted("l0_compaction", 100, 40);

        let labels = &[("strategy", "l0_compaction"), ("action", "move_live_bytes")];
        assert_eq!(
            metric_value_with_labels(&registry, "strata_store_gc_strategy_selected_total", labels,),
            2.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_strategy_completed_total",
                labels,
            ),
            1.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_strategy_output_bytes_total",
                &[("strategy", "l0_compaction")],
            ),
            40.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_strategy_source_deleted_bytes_total",
                &[("strategy", "l0_compaction")],
            ),
            100.0
        );
        assert_eq!(
            metric_value_with_labels(
                &registry,
                "strata_store_gc_strategy_reclaimed_bytes_total",
                &[("strategy", "l0_compaction")],
            ),
            60.0
        );
    }

    #[test]
    fn relocation_metrics_record_lookup_outcomes_and_compaction_work() {
        let registry = Registry::new();
        let metrics = StrataStoreMetrics::new(&registry, "test").unwrap();

        metrics.record_relocation_lookup(Ok(true), Duration::from_millis(1));
        metrics.record_relocation_lookup(Ok(false), Duration::from_millis(1));
        metrics.record_relocation_lookup(Err(()), Duration::from_millis(1));
        metrics.record_relocation_cache_lookup(true);
        metrics.record_relocation_cache_lookup(false);
        metrics.record_main_compaction(
            MainCompactionKind::Full,
            120,
            4,
            2_000,
            900,
            Duration::from_millis(2),
        );
        metrics.record_main_compaction(MainCompactionKind::Minor, 140, 0, 0, 0, Duration::ZERO);
        metrics.record_main_compaction(MainCompactionKind::Minor, 130, 0, 0, 0, Duration::ZERO);
        metrics.record_relocation_compaction(10, 3, 1_000, 400, Duration::from_millis(3));

        for result in ["hit", "miss", "error"] {
            assert_eq!(
                metric_value_with_labels(
                    &registry,
                    "strata_store_relocation_lookups_total",
                    &[("result", result)],
                ),
                1.0
            );
        }
        for result in ["hit", "miss"] {
            assert_eq!(
                metric_value_with_labels(
                    &registry,
                    "strata_store_relocation_cache_requests_total",
                    &[("result", result)],
                ),
                1.0
            );
        }
        assert_eq!(
            metric_value(
                &registry,
                "strata_store_main_compaction_healed_references_total"
            ),
            4.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_main_compaction_input_bytes_total"),
            2_000.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_main_compaction_output_bytes_total"),
            900.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_main_minor_compaction_lsn"),
            140.0
        );
        assert_eq!(
            metric_value(&registry, "strata_store_main_full_compaction_lsn"),
            120.0
        );
        assert_eq!(
            metric_value(
                &registry,
                "strata_store_relocation_compaction_entries_examined_total"
            ),
            10.0
        );
        assert_eq!(
            metric_value(
                &registry,
                "strata_store_relocation_compaction_entries_dropped_total"
            ),
            3.0
        );
        assert_eq!(
            metric_value(
                &registry,
                "strata_store_relocation_compaction_input_bytes_total"
            ),
            1_000.0
        );
        assert_eq!(
            metric_value(
                &registry,
                "strata_store_relocation_compaction_output_bytes_total"
            ),
            400.0
        );
    }
}
