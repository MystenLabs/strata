use std::collections::BTreeMap;

#[cfg(test)]
use strata_core::SegmentOwner;
use strata_core::{
    Epoch, PlacementClass, SegmentFileState, SegmentGcSummary, SegmentId, SegmentState, StrataLsn,
};

/// Cost and eligibility knobs for pure GC planning.
///
/// These values describe policy, not correctness. Lower thresholds make GC more eager and can
/// increase write amplification; higher thresholds make GC more conservative and can leave sparse
/// files around longer. The executor should still enforce its own byte and time budgets before
/// running a selected plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlannerConfig {
    /// Maximum bytes to write for one selected plan.
    ///
    /// The planner uses this as a hard cap for rewrite style plans so one scheduling decision does
    /// not monopolize the disk with a very large copy.
    pub max_copy_bytes_per_plan: u64,
    /// Maximum bytes to write for one selected L0 ingest compaction plan.
    ///
    /// L0 compaction must be able to drain healthy sealed ingest segments into retention layout.
    /// This cap is intentionally separate from the general rewrite cap so large ingest segments can
    /// be reorganized without allowing every garbage-ratio rewrite to become equally large.
    pub max_l0_copy_bytes_per_plan: u64,
    /// Minimum remaining epoch distance for a known-lifetime bucket to count toward L0 usefulness.
    ///
    /// Buckets expiring sooner than this should usually die in the ingest segment and be reclaimed
    /// by `EmptyDelete` rather than being copied shortly before expiry.
    pub min_l0_rewrite_epoch_distance: Epoch,
    /// Minimum fraction of live bytes that must have useful retention value before L0 rewrites an
    /// ingest segment, in basis points.
    ///
    /// Useful bytes are unknown-lifetime bytes plus known-lifetime buckets that are far enough from
    /// expiry. This is a segment-level gate because current L0 execution full-drains the source.
    pub min_l0_rewrite_useful_ratio_bps: u16,
    /// Minimum garbage bytes before a dead ref rewrite is worth considering.
    ///
    /// This avoids rewriting tiny files or tiny holes where the metadata and fsync overhead can be
    /// larger than the actual space reclaimed.
    pub min_reclaim_bytes: u64,
    /// Minimum garbage ratio, in basis points, before a dead ref rewrite is considered.
    ///
    /// For example, `6000` means 60 percent. The planner checks both this ratio and
    /// `min_reclaim_bytes`, a segment must clear both gates before the DeadRef scenario applies.
    pub min_garbage_ratio_bps: u16,
    /// Minimum bytes in an epoch bucket before exact epoch routing is preferred over spillover.
    ///
    /// Exact epoch placement is useful when it creates meaningful future deletion locality. Very
    /// small buckets are routed to spillover so the system does not create many lightly filled epoch
    /// segments.
    pub min_exact_epoch_bucket_bytes: u64,
    /// Minimum remaining epoch distance for exact epoch placement.
    ///
    /// If a record expires too soon, moving it into an exact epoch segment is usually not worth the
    /// copy, waiting for natural expiry or using spillover is cheaper.
    pub min_exact_epoch_distance: Epoch,
    /// If any live ref in a segment has a higher extension count, route estimates to spillover.
    ///
    /// Extension count is a churn signal. Frequently extended records are poor candidates for exact
    /// epoch placement because they are likely to be moved again.
    pub max_exact_epoch_extension_count: u32,
    /// Minimum joined bytes before JoinMultiple is considered.
    ///
    /// JoinMultiple is a locality improvement more than direct space reclamation. This threshold
    /// keeps it focused on output segments dense enough to matter.
    pub min_join_output_bytes: u64,
    /// Maximum number of sources in one JoinMultiple plan.
    ///
    /// This bounds publish complexity. The executor will eventually need to validate every copied
    /// source ref before making a multi-source move visible.
    pub max_join_sources: usize,
}

impl Default for GcPlannerConfig {
    fn default() -> Self {
        Self {
            max_copy_bytes_per_plan: 256 * 1024 * 1024,
            max_l0_copy_bytes_per_plan: 1024 * 1024 * 1024,
            min_l0_rewrite_epoch_distance: 2,
            min_l0_rewrite_useful_ratio_bps: 7_500,
            min_reclaim_bytes: 64 * 1024 * 1024,
            min_garbage_ratio_bps: 6000,
            min_exact_epoch_bucket_bytes: 8 * 1024 * 1024,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 128 * 1024 * 1024,
            max_join_sources: 8,
        }
    }
}

/// Immutable metadata view supplied to the planner.
///
/// The planner is intentionally snapshot-based. It does not hold locks, mutate metadata, or assume
/// that this view remains current after planning. The executor must revalidate the selected plan
/// before publishing any `MapRef` operations or deleting source files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcSnapshot {
    /// Store epoch at the time the snapshot was built.
    ///
    /// This is used for expiry-sensitive routing and for detecting exact-epoch segments whose
    /// physical epoch has already passed.
    pub current_epoch: Epoch,
    /// Highest LSN for which accounting-derived segment state is known to be materialized.
    ///
    /// A source segment whose `max_lsn` is above this frontier may contain refs that are still
    /// unknown to `SegmentGcSummary` or `SegmentGcOverlay`, so the pure planner skips full-source
    /// plans for that segment.
    pub accounted_lsn: StrataLsn,
    /// Candidate source segments and their accounting summaries.
    pub segments: Vec<SegmentSnapshot>,
}

/// Per-source segment facts used by the planner.
///
/// `SegmentState` says what the file is and where it is placed; `SegmentGcSummary` says what the
/// accounting view currently knows about the bytes inside it. Keeping both together makes tests
/// and future schedulers explicit about the view they are planning from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSnapshot {
    /// Durable segment metadata, including placement class, state, path, and LSN bounds.
    pub state: SegmentState,
    /// GC-facing byte/ref accounting for this segment.
    ///
    /// The planner treats these counters as a conservative planning view. It does not use them as
    /// proof that a copy is still valid at publish time.
    pub summary: SegmentGcSummary,
    /// True when another GC job already owns this segment.
    ///
    /// Claimed segments are skipped so two plans do not race to rewrite or delete the same source.
    pub claimed: bool,
}

impl SegmentSnapshot {
    fn segment_id(&self) -> SegmentId {
        self.state.segment_id
    }

    fn is_sealed(&self) -> bool {
        self.state.state == SegmentFileState::Sealed
    }

    fn is_empty_delete_source(&self) -> bool {
        matches!(
            self.state.state,
            SegmentFileState::Sealed | SegmentFileState::GcRelocating
        )
    }

    fn liveness_complete(&self, accounted_lsn: StrataLsn) -> bool {
        self.state
            .max_lsn
            .is_none_or(|max_lsn| accounted_lsn >= max_lsn)
    }

    fn eligible_source(&self, accounted_lsn: StrataLsn) -> bool {
        self.is_sealed()
            && !self.claimed
            && self.liveness_complete(accounted_lsn)
            && self.summary.total_bytes > 0
    }

    fn eligible_empty_delete_source(&self, accounted_lsn: StrataLsn) -> bool {
        self.is_empty_delete_source()
            && !self.claimed
            && self.liveness_complete(accounted_lsn)
            && self.summary.total_bytes > 0
    }
}

/// Logical destination class selected for a group of live records.
///
/// This is deliberately smaller than `PlacementClass`: GC routes moved live bytes only to retention
/// classes. Ingest remains a foreground write layout and is not a GC destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DestinationClass {
    /// Retention segment physically grouped by the record's logical end epoch.
    ExactEpoch(Epoch),
    /// Mixed retention area for unknown, unstable, or low-density lifetimes.
    Spillover,
}

/// Estimated movement for one source bucket into one destination class.
///
/// A route is still an aggregate estimate. It says how many refs/bytes the executor should expect
/// to move for a lifetime bucket, but it does not identify individual `RecordRef`s. The byte-copy
/// phase will later scan the segment-local GC overlay to enumerate exact records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEstimate {
    /// Source segment that owns the records covered by this route.
    pub source_segment_id: SegmentId,
    /// Logical end epoch for the bucket, or `None` when the lifetime is unknown.
    pub end_epoch: Option<Epoch>,
    /// Placement target chosen by policy.
    pub destination_class: DestinationClass,
    /// Number of live refs expected in this route.
    pub refs: u64,
    /// Encoded-record bytes expected in this route.
    pub bytes: u64,
}

/// High-level reason a plan was selected.
///
/// Scenarios keep scheduling decisions explainable. They also let a future executor apply
/// scenario-specific validation, metrics, and retry behavior without reverse-engineering the action
/// list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcScenario {
    /// Delete a sealed segment whose accounting view shows no live refs.
    EmptyDelete,
    /// Move live records from sealed ingest layout into retention layout.
    L0Compaction,
    /// Drain a sparse retention segment whose garbage bytes justify rewriting remaining live bytes.
    DeadRef,
    /// Combine same-epoch live buckets from multiple source segments into denser output.
    JoinMultiple,
    /// Handle an expired physical epoch segment that still contains future-live pinned bytes.
    PinnedEpochExpiry,
}

/// One operation the executor should perform if it accepts a plan.
///
/// Actions are declarative and side-effect free. They describe intent; the executor is responsible
/// for claiming sources, copying records, fsyncing outputs, validating refs, publishing metadata,
/// and cleaning up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcAction {
    /// Unlink a segment after metadata marks it deleting.
    ///
    /// This action is valid only when the executor revalidates that the segment has no live,
    /// pending, or claimed refs.
    DeleteSegment {
        /// Source segment to delete.
        segment_id: SegmentId,
    },
    /// Unlink several empty segments after metadata marks them deleted in one commit.
    ///
    /// This is the batched form of `DeleteSegment`. It is valid only when the executor revalidates
    /// every named segment before publishing the metadata transition.
    DeleteSegments {
        /// Source segments to delete.
        segment_ids: Vec<SegmentId>,
    },
    /// Move all currently planned live bytes out of one source segment.
    ///
    /// This is used for L0, DeadRef, and small pinned-epoch drains. The exact copied records are
    /// discovered later by scanning the segment-local GC overlay and applying these route decisions.
    MoveLiveBytes {
        /// Source segment to drain.
        source_segment_id: SegmentId,
        /// Aggregate route estimates for the source's live lifetime buckets.
        routes: Vec<RouteEstimate>,
    },
    /// Move same-epoch live buckets from multiple sources.
    ///
    /// JoinMultiple uses this action when the benefit is output density rather than immediate whole
    /// source deletion.
    MoveEpochBytes {
        /// Common end epoch being packed.
        epoch: Epoch,
        /// Per-source movement estimates for the common epoch.
        routes: Vec<RouteEstimate>,
    },
    /// Change placement metadata without copying bytes.
    ///
    /// This is the conservative answer for an expired exact-epoch segment that remains too live to
    /// rewrite economically. Metadata reclassification avoids forcing high write amplification.
    ReclassifySegment {
        /// Segment whose placement class should change.
        segment_id: SegmentId,
        /// Replacement class, currently expected to be `PlacementClass::Spillover`.
        placement_class: PlacementClass,
    },
}

/// Planner output for one scheduling decision.
///
/// A `GcPlan` is not a transaction. It is a recommendation derived from a snapshot. Any executor
/// must treat it as stale until it has reacquired the relevant metadata barriers and revalidated the
/// copied or deleted refs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlan {
    /// Why this plan was chosen.
    pub scenario: GcScenario,
    /// Declarative operations to execute.
    pub action: GcAction,
    /// Estimated bytes the executor would write.
    pub copied_bytes: u64,
    /// Estimated source bytes made reclaimable by the plan.
    ///
    /// For JoinMultiple this can be zero because the initial benefit is denser future placement, not
    /// necessarily immediate source deletion.
    pub expected_reclaim_bytes: u64,
    /// Relative ranking score among candidates in the same snapshot.
    ///
    /// Scores are intentionally local to this planner version. They should be used for ordering, not
    /// persisted as a durable policy contract.
    pub score: i128,
}

/// Stateless pure planner for segment GC.
///
/// The planner holds policy config and computes one best next plan from a snapshot. It performs no
/// I/O and is cheap to unit-test with synthetic metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlanner {
    /// Policy knobs used when filtering and scoring candidates.
    config: GcPlannerConfig,
}

impl GcPlanner {
    pub fn new(config: GcPlannerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &GcPlannerConfig {
        &self.config
    }

    pub fn plan(&self, snapshot: &GcSnapshot) -> Option<GcPlan> {
        self.plans(snapshot).into_iter().next()
    }

    pub fn plans(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        let mut empty_deletes = self.empty_delete_candidates(snapshot);
        empty_deletes.sort_by(|left, right| right.score.cmp(&left.score));

        let mut candidates = Vec::new();
        candidates.extend(self.l0_candidates(snapshot));
        candidates.extend(self.dead_ref_candidates(snapshot));
        candidates.extend(self.join_multiple_candidates(snapshot));
        candidates.extend(self.pinned_epoch_candidates(snapshot));
        candidates.sort_by(|left, right| right.score.cmp(&left.score));

        empty_deletes.extend(candidates);
        empty_deletes
    }

    fn empty_delete_candidates(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        let mut candidates = snapshot
            .segments
            .iter()
            .filter(|segment| segment.eligible_empty_delete_source(snapshot.accounted_lsn))
            .filter(|segment| segment.summary.live_ref_count == 0)
            .map(|segment| {
                (
                    segment.segment_id(),
                    segment.summary.total_bytes,
                    i128::from(segment.summary.total_bytes) * 10_000,
                )
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Vec::new();
        }

        candidates.sort_by_key(|(segment_id, _, _)| *segment_id);
        let expected_reclaim_bytes = candidates
            .iter()
            .map(|(_, reclaim_bytes, _)| *reclaim_bytes)
            .sum();
        let score = candidates.iter().map(|(_, _, score)| *score).sum();
        let segment_ids = candidates
            .into_iter()
            .map(|(segment_id, _, _)| segment_id)
            .collect();

        vec![GcPlan {
            scenario: GcScenario::EmptyDelete,
            action: GcAction::DeleteSegments { segment_ids },
            copied_bytes: 0,
            expected_reclaim_bytes,
            score,
        }]
    }

    fn l0_candidates(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        snapshot
            .segments
            .iter()
            .filter(|segment| segment.eligible_source(snapshot.accounted_lsn))
            .filter(|segment| segment.state.placement_class == PlacementClass::Ingest)
            .filter_map(|segment| {
                let copied_bytes = segment.summary.live_bytes;
                if copied_bytes > self.config.max_l0_copy_bytes_per_plan {
                    return None;
                }
                let useful_bytes = self.l0_rewrite_useful_bytes(snapshot, segment);
                if !self.l0_rewrite_is_useful(copied_bytes, useful_bytes) {
                    return None;
                }
                let routes = self.route_segment_live_bytes(snapshot, segment);
                if copied_bytes > 0 && routes.is_empty() {
                    return None;
                }
                Some(GcPlan {
                    scenario: GcScenario::L0Compaction,
                    action: GcAction::MoveLiveBytes {
                        source_segment_id: segment.segment_id(),
                        routes,
                    },
                    copied_bytes,
                    expected_reclaim_bytes: segment.summary.total_bytes,
                    score: score_rewrite(useful_bytes, copied_bytes, 1_500),
                })
            })
            .collect()
    }

    fn dead_ref_candidates(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        snapshot
            .segments
            .iter()
            .filter(|segment| segment.eligible_source(snapshot.accounted_lsn))
            .filter(|segment| segment.state.placement_class != PlacementClass::Ingest)
            .filter(|segment| segment.summary.garbage_bytes() >= self.config.min_reclaim_bytes)
            .filter(|segment| {
                garbage_ratio_bps(&segment.summary) >= self.config.min_garbage_ratio_bps
            })
            .filter_map(|segment| {
                let copied_bytes = segment.summary.live_bytes;
                if copied_bytes > self.config.max_copy_bytes_per_plan {
                    return None;
                }
                let routes = self.route_segment_live_bytes(snapshot, segment);
                Some(GcPlan {
                    scenario: GcScenario::DeadRef,
                    action: GcAction::MoveLiveBytes {
                        source_segment_id: segment.segment_id(),
                        routes,
                    },
                    copied_bytes,
                    expected_reclaim_bytes: segment.summary.garbage_bytes(),
                    score: score_rewrite(segment.summary.garbage_bytes(), copied_bytes, 2_000),
                })
            })
            .collect()
    }

    fn join_multiple_candidates(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        let mut by_epoch: BTreeMap<Epoch, Vec<RouteEstimate>> = BTreeMap::new();
        for segment in snapshot
            .segments
            .iter()
            .filter(|segment| segment.eligible_source(snapshot.accounted_lsn))
            .filter(|segment| segment.state.placement_class != PlacementClass::Ingest)
        {
            if !self.segment_lifetimes_stable(&segment.summary) {
                continue;
            }
            for (epoch, bucket) in &segment.summary.future_epoch_histogram {
                if !self.exact_epoch_bucket_is_useful(snapshot.current_epoch, *epoch, bucket.bytes)
                {
                    continue;
                }
                by_epoch.entry(*epoch).or_default().push(RouteEstimate {
                    source_segment_id: segment.segment_id(),
                    end_epoch: Some(*epoch),
                    destination_class: DestinationClass::ExactEpoch(*epoch),
                    refs: bucket.refs,
                    bytes: bucket.bytes,
                });
            }
        }

        by_epoch
            .into_iter()
            .filter_map(|(epoch, mut routes)| {
                routes.sort_by_key(|route| std::cmp::Reverse(route.bytes));
                routes.truncate(self.config.max_join_sources);
                let source_count = routes
                    .iter()
                    .map(|route| route.source_segment_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                let copied_bytes = routes.iter().map(|route| route.bytes).sum::<u64>();
                if source_count < 2
                    || copied_bytes < self.config.min_join_output_bytes
                    || copied_bytes > self.config.max_copy_bytes_per_plan
                {
                    return None;
                }
                Some(GcPlan {
                    scenario: GcScenario::JoinMultiple,
                    action: GcAction::MoveEpochBytes { epoch, routes },
                    copied_bytes,
                    expected_reclaim_bytes: 0,
                    score: i128::from(copied_bytes) + i128::from(source_count as u64) * 8_000_000,
                })
            })
            .collect()
    }

    fn pinned_epoch_candidates(&self, snapshot: &GcSnapshot) -> Vec<GcPlan> {
        snapshot
            .segments
            .iter()
            .filter(|segment| segment.eligible_source(snapshot.accounted_lsn))
            .filter(|segment| {
                matches!(
                    segment.state.placement_class,
                    PlacementClass::ExactEpoch(epoch) if epoch <= snapshot.current_epoch
                )
            })
            .filter(|segment| segment.summary.live_bytes > 0)
            .map(|segment| {
                if segment.summary.live_bytes > self.config.max_copy_bytes_per_plan {
                    GcPlan {
                        scenario: GcScenario::PinnedEpochExpiry,
                        action: GcAction::ReclassifySegment {
                            segment_id: segment.segment_id(),
                            placement_class: PlacementClass::Spillover,
                        },
                        copied_bytes: 0,
                        expected_reclaim_bytes: 0,
                        score: i128::from(segment.summary.live_bytes),
                    }
                } else {
                    let routes = self.route_segment_live_bytes(snapshot, segment);
                    GcPlan {
                        scenario: GcScenario::PinnedEpochExpiry,
                        action: GcAction::MoveLiveBytes {
                            source_segment_id: segment.segment_id(),
                            routes,
                        },
                        copied_bytes: segment.summary.live_bytes,
                        expected_reclaim_bytes: segment.summary.garbage_bytes(),
                        score: score_rewrite(
                            segment.summary.garbage_bytes(),
                            segment.summary.live_bytes,
                            1_750,
                        ),
                    }
                }
            })
            .collect()
    }

    fn route_segment_live_bytes(
        &self,
        snapshot: &GcSnapshot,
        segment: &SegmentSnapshot,
    ) -> Vec<RouteEstimate> {
        let mut routes = Vec::new();

        let stable_lifetimes = self.segment_lifetimes_stable(&segment.summary);
        for (epoch, bucket) in &segment.summary.future_epoch_histogram {
            if bucket.bytes == 0 || bucket.refs == 0 {
                continue;
            }
            let destination_class = if stable_lifetimes
                && self.exact_epoch_bucket_is_useful(snapshot.current_epoch, *epoch, bucket.bytes)
            {
                DestinationClass::ExactEpoch(*epoch)
            } else {
                DestinationClass::Spillover
            };
            routes.push(RouteEstimate {
                source_segment_id: segment.segment_id(),
                end_epoch: Some(*epoch),
                destination_class,
                refs: bucket.refs,
                bytes: bucket.bytes,
            });
        }

        if segment.summary.unknown_lifetime_bytes > 0 {
            let destination_class = DestinationClass::Spillover;
            routes.push(RouteEstimate {
                source_segment_id: segment.segment_id(),
                end_epoch: None,
                destination_class,
                refs: segment.summary.unknown_lifetime_ref_count,
                bytes: segment.summary.unknown_lifetime_bytes,
            });
        }

        routes
    }

    fn l0_rewrite_useful_bytes(&self, snapshot: &GcSnapshot, segment: &SegmentSnapshot) -> u64 {
        let known_useful_bytes = segment
            .summary
            .future_epoch_histogram
            .iter()
            .filter(|(epoch, bucket)| {
                bucket.bytes > 0
                    && bucket.refs > 0
                    && self.l0_epoch_bucket_is_useful(snapshot.current_epoch, **epoch)
            })
            .map(|(_, bucket)| bucket.bytes)
            .sum::<u64>();
        known_useful_bytes.saturating_add(segment.summary.unknown_lifetime_bytes)
    }

    fn l0_rewrite_is_useful(&self, live_bytes: u64, useful_bytes: u64) -> bool {
        useful_bytes > 0
            && ratio_bps(useful_bytes, live_bytes) >= self.config.min_l0_rewrite_useful_ratio_bps
    }

    fn l0_epoch_bucket_is_useful(&self, current_epoch: Epoch, end_epoch: Epoch) -> bool {
        end_epoch > current_epoch
            && end_epoch.saturating_sub(current_epoch) >= self.config.min_l0_rewrite_epoch_distance
    }

    fn exact_epoch_bucket_is_useful(
        &self,
        current_epoch: Epoch,
        end_epoch: Epoch,
        bucket_bytes: u64,
    ) -> bool {
        end_epoch > current_epoch
            && end_epoch.saturating_sub(current_epoch) >= self.config.min_exact_epoch_distance
            && bucket_bytes >= self.config.min_exact_epoch_bucket_bytes
    }

    fn segment_lifetimes_stable(&self, summary: &SegmentGcSummary) -> bool {
        summary
            .extension_count_histogram
            .keys()
            .all(|count| *count <= self.config.max_exact_epoch_extension_count)
    }
}

fn garbage_ratio_bps(summary: &SegmentGcSummary) -> u16 {
    ratio_bps(summary.garbage_bytes(), summary.total_bytes)
}

fn ratio_bps(numerator: u64, denominator: u64) -> u16 {
    if denominator == 0 {
        return 0;
    }
    let ratio = numerator.saturating_mul(10_000) / denominator;
    ratio.min(u64::from(u16::MAX)) as u16
}

fn score_rewrite(reclaim_bytes: u64, copied_bytes: u64, weight: i128) -> i128 {
    let copy_penalty = i128::from(copied_bytes.max(1));
    i128::from(reclaim_bytes) * weight / copy_penalty
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::{EpochBucket, ShardKey, VolumeId};

    const SHARD: ShardKey = ShardKey {
        id: 0,
        generation: 0,
    };
    const VOLUME: VolumeId = 0;

    fn planner() -> GcPlanner {
        GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: 1_000,
            max_l0_copy_bytes_per_plan: 1_000,
            min_l0_rewrite_epoch_distance: 2,
            min_l0_rewrite_useful_ratio_bps: 7_500,
            min_reclaim_bytes: 100,
            min_garbage_ratio_bps: 5000,
            min_exact_epoch_bucket_bytes: 50,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 100,
            max_join_sources: 4,
        })
    }

    fn segment_state(
        segment_id: SegmentId,
        placement_class: PlacementClass,
        state: SegmentFileState,
        max_lsn: Option<StrataLsn>,
    ) -> SegmentState {
        SegmentState {
            owner: SegmentOwner::Shard(SHARD),
            segment_id,
            volume_id: VOLUME,
            path: format!("{segment_id}.data"),
            placement_class,
            state,
            write_offset: 1_000,
            durable_offset: 1_000,
            min_lsn: Some(1),
            max_lsn,
            sealed_before_lsn: None,
            sealed_len: Some(1_000),
            sealed_sha256: None,
        }
    }

    fn sealed_segment(
        segment_id: SegmentId,
        placement_class: PlacementClass,
        summary: SegmentGcSummary,
    ) -> SegmentSnapshot {
        SegmentSnapshot {
            state: segment_state(
                segment_id,
                placement_class,
                SegmentFileState::Sealed,
                Some(10),
            ),
            summary,
            claimed: false,
        }
    }

    fn summary(total: u64, live: u64, retired: u64) -> SegmentGcSummary {
        SegmentGcSummary {
            total_bytes: total,
            live_bytes: live,
            retired_bytes: retired,
            live_ref_count: u64::from(live > 0),
            ..SegmentGcSummary::default()
        }
    }

    fn add_epoch_bucket(summary: &mut SegmentGcSummary, epoch: Epoch, bytes: u64, refs: u64) {
        summary
            .future_epoch_histogram
            .insert(epoch, EpochBucket { refs, bytes });
        summary.min_live_end_epoch = summary.future_epoch_histogram.keys().next().copied();
        summary.max_live_end_epoch = summary.future_epoch_histogram.keys().next_back().copied();
        summary.extension_count_histogram.insert(0, refs);
    }

    fn snapshot(segments: Vec<SegmentSnapshot>) -> GcSnapshot {
        GcSnapshot {
            current_epoch: 10,
            accounted_lsn: 10,
            segments,
        }
    }

    #[test]
    fn planner_prefers_empty_delete() {
        let empty = sealed_segment(1, PlacementClass::ExactEpoch(20), summary(500, 0, 500));
        let plan = planner().plan(&snapshot(vec![empty])).unwrap();

        assert_eq!(plan.scenario, GcScenario::EmptyDelete);
        assert_eq!(
            plan.action,
            GcAction::DeleteSegments {
                segment_ids: vec![1]
            }
        );
        assert_eq!(plan.copied_bytes, 0);
        assert_eq!(plan.expected_reclaim_bytes, 500);
    }

    #[test]
    fn planner_batches_all_empty_deletes() {
        let first = sealed_segment(1, PlacementClass::ExactEpoch(20), summary(500, 0, 500));
        let second = sealed_segment(2, PlacementClass::ExactEpoch(20), summary(700, 0, 700));
        let plan = planner().plan(&snapshot(vec![second, first])).unwrap();

        assert_eq!(plan.scenario, GcScenario::EmptyDelete);
        assert_eq!(
            plan.action,
            GcAction::DeleteSegments {
                segment_ids: vec![1, 2]
            }
        );
        assert_eq!(plan.copied_bytes, 0);
        assert_eq!(plan.expected_reclaim_bytes, 1_200);
    }

    #[test]
    fn planner_fences_relocating_source_until_it_is_empty() {
        let mut relocating = sealed_segment(1, PlacementClass::Spillover, summary(1_000, 100, 900));
        relocating.state.state = SegmentFileState::GcRelocating;

        assert!(
            planner()
                .plan(&snapshot(vec![relocating.clone()]))
                .is_none()
        );

        relocating.summary = summary(1_000, 0, 1_000);
        let plan = planner().plan(&snapshot(vec![relocating])).unwrap();
        assert_eq!(plan.scenario, GcScenario::EmptyDelete);
        assert_eq!(
            plan.action,
            GcAction::DeleteSegments {
                segment_ids: vec![1]
            }
        );
    }

    #[test]
    fn dead_ref_routes_known_lifetime_to_exact_epoch_destination_class() {
        let mut segment_summary = summary(1_000, 100, 900);
        add_epoch_bucket(&mut segment_summary, 50, 100, 1);
        let snapshot = snapshot(vec![sealed_segment(
            1,
            PlacementClass::Spillover,
            segment_summary,
        )]);

        let plan = planner().plan(&snapshot).unwrap();

        assert_eq!(plan.scenario, GcScenario::DeadRef);
        let GcAction::MoveLiveBytes { routes, .. } = &plan.action else {
            panic!("expected move action");
        };
        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes[0].destination_class,
            DestinationClass::ExactEpoch(50)
        );
    }

    #[test]
    fn l0_unknown_lifetime_routes_to_spillover() {
        let mut segment_summary = summary(200, 200, 0);
        segment_summary.unknown_lifetime_bytes = 200;
        segment_summary.unknown_lifetime_ref_count = 2;
        let plan = planner()
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::Ingest,
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::L0Compaction);
        let GcAction::MoveLiveBytes { routes, .. } = &plan.action else {
            panic!("expected move action");
        };
        assert_eq!(routes[0].destination_class, DestinationClass::Spillover);
        assert_eq!(routes[0].end_epoch, None);
    }

    #[test]
    fn l0_uses_dedicated_copy_cap() {
        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: 1_000,
            max_l0_copy_bytes_per_plan: 2_000,
            min_l0_rewrite_epoch_distance: 2,
            min_l0_rewrite_useful_ratio_bps: 7_500,
            min_reclaim_bytes: 100,
            min_garbage_ratio_bps: 5000,
            min_exact_epoch_bucket_bytes: 50,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 100,
            max_join_sources: 4,
        });
        let mut segment_summary = summary(2_000, 1_500, 0);
        segment_summary.unknown_lifetime_bytes = 1_500;
        segment_summary.unknown_lifetime_ref_count = 1;
        let plan = planner
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::Ingest,
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::L0Compaction);
        assert_eq!(plan.copied_bytes, 1_500);
    }

    #[test]
    fn l0_skips_segment_when_live_bytes_are_too_close_to_expiry() {
        let mut segment_summary = summary(1_000, 1_000, 0);
        add_epoch_bucket(&mut segment_summary, 11, 900, 9);
        add_epoch_bucket(&mut segment_summary, 20, 100, 1);

        assert!(
            planner()
                .plan(&snapshot(vec![sealed_segment(
                    1,
                    PlacementClass::Ingest,
                    segment_summary,
                )]))
                .is_none()
        );
    }

    #[test]
    fn l0_accepts_segment_when_all_live_bytes_have_retention_value() {
        let mut segment_summary = summary(1_000, 1_000, 0);
        add_epoch_bucket(&mut segment_summary, 20, 1_000, 10);
        let plan = planner()
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::Ingest,
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::L0Compaction);
    }

    #[test]
    fn l0_skips_mixed_segment_to_avoid_moving_near_expiry_bytes() {
        let mut segment_summary = summary(1_000, 1_000, 0);
        add_epoch_bucket(&mut segment_summary, 11, 300, 3);
        add_epoch_bucket(&mut segment_summary, 20, 700, 7);

        assert!(
            planner()
                .plan(&snapshot(vec![sealed_segment(
                    1,
                    PlacementClass::Ingest,
                    segment_summary,
                )]))
                .is_none()
        );
    }

    #[test]
    fn l0_allows_tiny_near_expiry_tail() {
        let mut segment_summary = summary(1_000, 1_000, 0);
        add_epoch_bucket(&mut segment_summary, 11, 1, 1);
        add_epoch_bucket(&mut segment_summary, 20, 999, 999);
        let plan = planner()
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::Ingest,
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::L0Compaction);
    }

    #[test]
    fn l0_counts_unknown_lifetime_bytes_as_useful() {
        let mut segment_summary = summary(1_000, 1_000, 0);
        segment_summary.unknown_lifetime_bytes = 1_000;
        segment_summary.unknown_lifetime_ref_count = 10;
        let plan = planner()
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::Ingest,
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::L0Compaction);
    }

    #[test]
    fn dead_ref_still_uses_general_copy_cap() {
        let planner = GcPlanner::new(GcPlannerConfig {
            max_copy_bytes_per_plan: 1_000,
            max_l0_copy_bytes_per_plan: 2_000,
            min_l0_rewrite_epoch_distance: 2,
            min_l0_rewrite_useful_ratio_bps: 7_500,
            min_reclaim_bytes: 100,
            min_garbage_ratio_bps: 5000,
            min_exact_epoch_bucket_bytes: 50,
            min_exact_epoch_distance: 1,
            max_exact_epoch_extension_count: 1,
            min_join_output_bytes: 100,
            max_join_sources: 4,
        });
        let mut segment_summary = summary(3_000, 1_500, 1_500);
        add_epoch_bucket(&mut segment_summary, 50, 1_500, 1);

        assert!(
            planner
                .plan(&snapshot(vec![sealed_segment(
                    1,
                    PlacementClass::Spillover,
                    segment_summary,
                )]))
                .is_none()
        );
    }

    #[test]
    fn join_multiple_groups_common_epoch() {
        let mut left_summary = summary(300, 100, 0);
        add_epoch_bucket(&mut left_summary, 40, 100, 1);
        let mut right_summary = summary(300, 120, 0);
        add_epoch_bucket(&mut right_summary, 40, 120, 1);

        let plan = planner()
            .plan(&snapshot(vec![
                sealed_segment(1, PlacementClass::ExactEpoch(30), left_summary),
                sealed_segment(2, PlacementClass::ExactEpoch(35), right_summary),
            ]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::JoinMultiple);
        let GcAction::MoveEpochBytes { epoch, routes } = &plan.action else {
            panic!("expected join action");
        };
        assert_eq!(*epoch, 40);
        assert_eq!(routes.len(), 2);
        assert_eq!(plan.copied_bytes, 220);
    }

    #[test]
    fn pinned_epoch_expiry_reclassifies_when_copy_is_too_expensive() {
        let mut segment_summary = summary(2_000, 1_500, 0);
        add_epoch_bucket(&mut segment_summary, 30, 1_500, 3);
        let plan = planner()
            .plan(&snapshot(vec![sealed_segment(
                1,
                PlacementClass::ExactEpoch(9),
                segment_summary,
            )]))
            .unwrap();

        assert_eq!(plan.scenario, GcScenario::PinnedEpochExpiry);
        assert_eq!(
            plan.action,
            GcAction::ReclassifySegment {
                segment_id: 1,
                placement_class: PlacementClass::Spillover,
            }
        );
        assert_eq!(plan.copied_bytes, 0);
    }

    #[test]
    fn incomplete_accounting_blocks_full_source_planning() {
        let mut segment = sealed_segment(1, PlacementClass::ExactEpoch(20), summary(500, 0, 500));
        segment.state.max_lsn = Some(11);
        let snapshot = GcSnapshot {
            current_epoch: 10,
            accounted_lsn: 10,
            segments: vec![segment],
        };

        assert!(planner().plan(&snapshot).is_none());
    }
}
