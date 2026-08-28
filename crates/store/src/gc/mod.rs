//! The store-local half of garbage collection. The pure planner lives in `gc`; this module
//! owns the machinery that turns its plans into files and durable store metadata:
//! - `worker` owns scheduling, admission tuning, and in-process source claims.
//! - `copy` owns snapshot preparation, copying, prepublication, and shard cleanup.
//! - `publish` owns final revalidation and atomic relocation activation.
//! - `output` contains the publication-accounting helpers shared by publication and tests.
//!
//! One GC attempt is a four-stage pipeline, and the running example from `publish.rs` starts
//! here: source segment S7 (sealed, 100 MB, mostly dead per its overlay), records A, B, C, D
//! still live when the plan is made.
//!
//! 1. Prepare (prepare_gc_plan): build a snapshot from published summaries, let the planner rank
//!    plans, claim the first plan's sources in memory, and load their garbage overlays.
//! 2. Copy (copy_prepared_gc_plan): scan S7 offset-by-offset, skip ranges the overlay already
//!    calls dead, and copy live records into temporary staging files grouped by destination —
//!    A, B, C, D all land in staging file T900 (local id, not a real segment yet).
//! 3. Prepublish (prepublish_gc_outputs): give T900 a durable id (S42), rename it into its
//!    retention directory, and write a PendingGcOutput row so a crash from here on has a durable
//!    pointer to clean up by.
//! 4. Publish (`submit_gc_publish`, in `publish.rs`): revalidate every copy against the freshly
//!    drained overlays — this is where B and C are discovered dead — and activate the relocation
//!    table. S7 is deleted much later, by a separate delete plan, once its overlay proves empty
//!    and the activation is durable.
//!
//! Around the pipeline sit the GcConcurrencyController (an additive-increase/backoff tuner that
//! watches foreground sync latency, write-queue latency, and seal backpressure), GcSourceClaims
//! (non-durable ownership that prevents duplicate local work), the GcWorker loop, and GcExecutor
//! (a cheap-to-clone bundle of the handles needed by one attempt). Correctness never depends on
//! the claims: publication always revalidates against durable state.
//!
//! The failure philosophy everywhere: each stage cleans up its own artifacts and aborts cleanly.
//! Staging files are unreferenced by any metadata and are simply removed; prepublished outputs
//! have Pending rows precisely so startup can delete them; nothing durable claims a copy
//! succeeded until publication's atomic batch commits.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicU64, mpsc},
};

use core_types::{
    PlacementClass, RecordRef, SegmentFileState, SegmentGcOverlay, SegmentId, SegmentOwner,
    SegmentState, ShardKey, StrataLsn,
};
use gc_planner::{DestinationClass, GcCopyRecord, GcPlan, GcPlanner};
use index::StrataIndex;
use lsm::LiveSnapshots;

use crate::{
    Error, GcIoLimiter, Result, SegmentIdAllocator, StoreHalt, StrataStore,
    layout::relative_segment_path,
    metrics::StrataStoreMetrics,
    reader_cache::SegmentReaderCache,
    relocation::{RelocationCache, RelocationStore},
};

mod copy;
pub(crate) mod output;
mod publish;
mod worker;

pub(crate) use copy::{OverlayRecordClassifier, OverlayRecordState};
pub use worker::GcSourceClaimGuard;
pub(crate) use worker::{
    GcCommand, GcConcurrencyConfig, GcConcurrencyController, GcSourceClaims, GcWorker,
};
#[cfg(test)]
use worker::{TunedSignal, gc_failure_backoff};

#[cfg(test)]
mod tests;

/// Store-local preparation for one GC attempt.
///
#[derive(Debug)]
pub struct PreparedGcPlan {
    /// Aggregate pure-planner recommendation.
    pub plan: GcPlan,
    /// Source classifications folded from the selected segments' committed local garbage logs.
    source_overlays: BTreeMap<SegmentId, SegmentGcOverlay>,
    /// In-memory source segment claim held until this plan is copied or dropped.
    #[doc(hidden)]
    pub claim: Option<GcSourceClaimGuard>,
}

/// Bytes copied into GC staging files, ready for a later publish/finalize step.
///
/// The output files are not yet durable segment rows and the staged `RecordRef.segment_id` values
/// are local to this object. Publishing first preprotects those files as pending output segment
/// rows, then GC translates staged offsets into final relocation destinations.
#[derive(Debug)]
pub struct PreparedGcCopy {
    /// Aggregate plan whose selected bytes were copied.
    pub plan: GcPlan,
    /// Sealed staging files containing copied records.
    pub outputs: Vec<GcStagedOutputSegment>,
    /// Source-to-staged-record mapping for later relocation publication.
    pub copied_records: Vec<GcStagedCopiedRecord>,
    /// In-memory source segment claim held until publish completes or this copy is dropped.
    #[doc(hidden)]
    pub claim: Option<GcSourceClaimGuard>,
}

/// GC copy bundle after output files have durable segment ids and protected segment rows.
#[derive(Debug)]
pub(crate) struct GcPrepublishedCopy {
    /// Aggregate plan whose selected bytes were copied.
    pub(crate) plan: GcPlan,
    /// Protected output segments already published as pending GC output rows.
    pub(crate) outputs: Vec<GcPrepublishedOutputSegment>,
    /// Reconciled source-to-staged-record mappings.
    pub(crate) copied_records: Vec<GcStagedCopiedRecord>,
    /// In-memory source segment claim held until publish completes or this copy is dropped.
    pub(crate) _claim: Option<GcSourceClaimGuard>,
}

/// Result of publishing staged GC copies into durable Strata metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPublishResult {
    /// Latest visible foreground sequence observed during relocation reconciliation.
    pub reconciled_lsn: StrataLsn,
    /// Output segment files finalized by this publish.
    pub output_segments: Vec<GcPublishedOutputSegment>,
    /// Source refs that were mapped to replacement refs.
    pub published_records: Vec<GcPublishedRecord>,
    /// Staged copies not mapped because their source changed while bytes were copied.
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
    /// Placement class recorded in segment state.
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
    /// Foreground durability frontier used as this relocation's logical publication fence.
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
    /// Durable segment id assigned before relocation activation.
    pub(crate) segment_id: SegmentId,
    /// Shard generation that owns this retention segment.
    pub(crate) shard: ShardKey,
    /// Final on-disk path.
    pub(crate) path: PathBuf,
    /// Placement class to record when the output becomes sealed.
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
    /// Serializes GC output publication with shard-generation directory cleanup.
    pub(crate) publish_cleanup_lock: Arc<Mutex<()>>,
    /// Serializes whole-shard metadata removal with garbage-log publication and sweeping.
    pub(crate) garbage_publish_lock: Arc<Mutex<()>>,
    /// Excludes blob-LSM compaction while one relocation view is reconciled and activated.
    pub(crate) compaction_admission_lock: Arc<RwLock<()>>,
    /// Relocation L0s and cache used directly by the GC publication lane.
    pub(crate) relocations: Arc<RelocationStore>,
    pub(crate) relocation_cache: Arc<RelocationCache>,
    /// Highest relocation activation covered by a completed RocksDB WAL sync.
    pub(crate) durable_relocation_lsn: Arc<AtomicU64>,
    /// Snapshot pins consulted before deleting a relocated source segment.
    pub(crate) live_snapshots: LiveSnapshots,
    /// Wakes the blob compactor so it can heal freshly activated relocations.
    pub(crate) lsm_compact_tx: mpsc::Sender<()>,
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
            publish_cleanup_lock: Arc::clone(&self.gc_publish_cleanup_lock),
            garbage_publish_lock: Arc::clone(&self.garbage_publish_lock),
            compaction_admission_lock: Arc::clone(&self.compaction_admission_lock),
            relocations: Arc::clone(&self.relocations),
            relocation_cache: Arc::clone(&self.relocation_cache),
            durable_relocation_lsn: Arc::clone(&self.durable_relocation_lsn),
            live_snapshots: self.live_snapshots.clone(),
            lsm_compact_tx: self
                .lsm_compact_tx
                .as_ref()
                .ok_or(Error::WriteQueueClosed)?
                .clone(),
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

    /// Prepares one GC plan from RocksDB summaries and segment-local garbage logs.
    ///
    /// This builds one published GC view, asks the planner for a plan, and claims its sources.
    pub fn prepare_gc_plan(&self, planner: &GcPlanner) -> Result<Option<PreparedGcPlan>> {
        self.gc_executor()?.prepare_gc_plan(planner)
    }

    /// Copies selected GC records into sealed staging files.
    ///
    /// This consumes a `PreparedGcPlan`. It does not publish relocations or create durable segment
    /// metadata for the outputs.
    pub fn copy_prepared_gc_plan(&self, prepared: PreparedGcPlan) -> Result<PreparedGcCopy> {
        self.gc_executor()?.copy_prepared_gc_plan(prepared)
    }

    /// Publishes staged GC copies through the independent relocation path.
    ///
    /// GC revalidates copied records, writes a durable relocation L0, and atomically activates it
    /// without entering the foreground writer queue.
    pub fn publish_prepared_gc_copy(&self, copy: PreparedGcCopy) -> Result<GcPublishResult> {
        self.gc_executor()?.publish_prepared_gc_copy(copy)
    }
}
