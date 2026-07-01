use std::{collections::BTreeMap, fmt};

use strata_core::{BlobKey, BlobLifecycle, RecordRef, ShardKey, StrataLsn};

use crate::{DestinationClass, GcAction, GcPlan, GcScenario, RouteEstimate};

/// One scanned source record that remains copy-eligible after applying the segment GC overlay.
///
/// The scanner supplies immutable physical facts from the segment file: key, shard, payload LSN,
/// and `RecordRef`. The index supplies the segment-local overlay before this value is built:
/// expired and retired ranges are skipped, and lifetime ranges become `lifecycle` routing hints.
/// Absence of a lifetime means the record is copy-eligible but should be treated as
/// unknown/spillover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcSourceRecord {
    /// Blob key encoded in the source record trailer.
    pub key: BlobKey,
    /// Logical shard generation encoded in the source record header.
    pub shard: ShardKey,
    /// LSN of the payload write that created this physical record.
    ///
    /// `VersionMergeOp::MapRef` must target this exact payload LSN so a GC move cannot rewrite a
    /// later blob version that happens to reference the same key.
    pub payload_lsn: StrataLsn,
    /// Physical range of the complete encoded source record.
    pub record_ref: RecordRef,
    /// Segment-local lifetime hint used to route this copy.
    pub lifecycle: Option<BlobLifecycle>,
}

/// One exact record that should be copied by the executor.
///
/// This is the bridge from aggregate planning to physical execution. The executor will read `from`,
/// append the payload to the selected destination class, then publish a `MapRef` using `key`,
/// `shard`, `payload_lsn`, and the newly allocated destination `RecordRef`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcCopyRecord {
    /// Blob key whose physical payload reference will be remapped after copy.
    pub key: BlobKey,
    /// Shard generation owning the payload version.
    pub shard: ShardKey,
    /// Exact payload LSN to rewrite during `MapRef` publication.
    pub payload_lsn: StrataLsn,
    /// Source physical record range.
    pub from: RecordRef,
    /// Current lifecycle used for destination routing and later accounting publication.
    pub lifecycle: Option<BlobLifecycle>,
    /// Destination class chosen by the planner for this record's bucket.
    pub destination_class: DestinationClass,
}

/// Exact copy work selected from one aggregate `GcPlan`.
///
/// The selector validates that aggregate route estimates match the exact live records it found. A
/// mismatch means the plan was built from a view that no longer matches the selected record/state
/// inputs; the caller should discard the plan and rebuild or retry later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcCopySelection {
    /// Scenario inherited from the aggregate plan for metrics and executor policy.
    pub scenario: GcScenario,
    /// Exact records to copy, sorted by source segment and offset.
    pub records: Vec<GcCopyRecord>,
    /// Sum of encoded source record bytes in `records`.
    pub copied_bytes: u64,
}

/// Validation failure while turning aggregate routes into exact record copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcSelectionError {
    /// The plan contains no copy action.
    ///
    /// `DeleteSegment` and `ReclassifySegment` plans do not need record selection.
    NoCopyAction,
    /// The plan mixes copy actions with non-copy actions.
    ///
    /// The current selector expects one copy operation per plan. Supporting mixed plans later should
    /// be explicit because publish validation gets more complicated.
    MixedCopyAndNonCopyActions,
    /// Two routes describe the same `(source segment, end epoch)` bucket.
    DuplicateRoute {
        source_segment_id: u64,
        end_epoch: Option<u64>,
    },
    /// Exact selected bytes do not match the aggregate route bytes.
    CopyBytesMismatch { expected: u64, actual: u64 },
}

impl fmt::Display for GcSelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCopyAction => write!(f, "GC plan does not contain a copy action"),
            Self::MixedCopyAndNonCopyActions => {
                write!(f, "GC plan mixes copy and non-copy actions")
            }
            Self::DuplicateRoute {
                source_segment_id,
                end_epoch,
            } => write!(
                f,
                "GC plan has duplicate route for source segment {source_segment_id} and end epoch {end_epoch:?}"
            ),
            Self::CopyBytesMismatch { expected, actual } => write!(
                f,
                "GC selected {actual} copy bytes but plan expected {expected}"
            ),
        }
    }
}

impl std::error::Error for GcSelectionError {}

/// Selects exact live records for a planner-produced copy plan.
///
/// The caller should pass records scanned from every source segment named by the plan after applying
/// `SegmentGcOverlay`: expired/retired ranges must be omitted, and ranges in `lifetimes` should
/// populate `GcSourceRecord::lifecycle`. Records whose lifetime matches a planned route are
/// returned as `GcCopyRecord`s. If selected bytes differ from aggregate route estimates, the caller
/// should discard the plan and rebuild from a fresher view.
pub fn select_copy_records(
    plan: &GcPlan,
    records: &[GcSourceRecord],
) -> Result<GcCopySelection, GcSelectionError> {
    let routes = route_table(plan)?;
    let mut selected = Vec::new();

    for record in records {
        if !routes.source_is_planned(record.record_ref.segment_id) {
            continue;
        }
        let key = RouteKey::from_record(record.record_ref.segment_id, record.lifecycle);
        let Some(route) = routes.by_key.get(&key) else {
            continue;
        };
        selected.push(GcCopyRecord {
            key: record.key.clone(),
            shard: record.shard,
            payload_lsn: record.payload_lsn,
            from: record.record_ref,
            lifecycle: record.lifecycle,
            destination_class: route.destination_class,
        });
    }

    selected.sort_by_key(|record| (record.from.segment_id, record.from.offset));
    let copied_bytes = selected
        .iter()
        .map(|record| record.from.len)
        .try_fold(0_u64, u64::checked_add)
        .ok_or(GcSelectionError::CopyBytesMismatch {
            expected: routes.expected_bytes,
            actual: u64::MAX,
        })?;
    if copied_bytes != routes.expected_bytes {
        return Err(GcSelectionError::CopyBytesMismatch {
            expected: routes.expected_bytes,
            actual: copied_bytes,
        });
    }

    Ok(GcCopySelection {
        scenario: plan.scenario,
        records: selected,
        copied_bytes,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RouteKey {
    source_segment_id: u64,
    end_epoch: Option<u64>,
}

impl RouteKey {
    fn from_route(route: &RouteEstimate) -> Self {
        Self {
            source_segment_id: route.source_segment_id,
            end_epoch: route.end_epoch,
        }
    }

    fn from_record(source_segment_id: u64, lifecycle: Option<BlobLifecycle>) -> Self {
        Self {
            source_segment_id,
            end_epoch: lifecycle.map(|lifecycle| lifecycle.logical_end_epoch),
        }
    }
}

#[derive(Debug)]
struct RouteTable {
    by_key: BTreeMap<RouteKey, RouteEstimate>,
    expected_bytes: u64,
}

impl RouteTable {
    fn source_is_planned(&self, source_segment_id: u64) -> bool {
        self.by_key
            .keys()
            .any(|key| key.source_segment_id == source_segment_id)
    }
}

fn route_table(plan: &GcPlan) -> Result<RouteTable, GcSelectionError> {
    let mut saw_copy = false;
    let mut routes = BTreeMap::new();
    let mut expected_bytes = 0_u64;

    for action in &plan.actions {
        match action {
            GcAction::MoveLiveBytes {
                routes: action_routes,
                ..
            }
            | GcAction::MoveEpochBytes {
                routes: action_routes,
                ..
            } => {
                saw_copy = true;
                for route in action_routes {
                    let key = RouteKey::from_route(route);
                    if routes.insert(key, route.clone()).is_some() {
                        return Err(GcSelectionError::DuplicateRoute {
                            source_segment_id: key.source_segment_id,
                            end_epoch: key.end_epoch,
                        });
                    }
                    expected_bytes = expected_bytes.saturating_add(route.bytes);
                }
            }
            GcAction::DeleteSegment { .. } | GcAction::ReclassifySegment { .. } => {
                if saw_copy || plan.actions.len() > 1 {
                    return Err(GcSelectionError::MixedCopyAndNonCopyActions);
                }
                return Err(GcSelectionError::NoCopyAction);
            }
        }
    }

    if !saw_copy {
        return Err(GcSelectionError::NoCopyAction);
    }
    Ok(RouteTable {
        by_key: routes,
        expected_bytes,
    })
}

#[cfg(test)]
mod tests {
    use strata_core::{BlobKey, ShardKey};

    use super::*;
    use crate::{DestinationClass, GcAction, GcPlan, GcScenario};

    const SHARD: ShardKey = ShardKey {
        id: 7,
        generation: 3,
    };

    fn key(name: &str) -> BlobKey {
        BlobKey::new(name.as_bytes().to_vec()).unwrap()
    }

    fn record_ref(segment_id: u64, offset: u64, len: u64) -> RecordRef {
        RecordRef {
            segment_id,
            offset,
            len,
        }
    }

    fn lifecycle(epoch: u64) -> BlobLifecycle {
        BlobLifecycle {
            logical_end_epoch: epoch,
            extension_count: 0,
        }
    }

    fn source_record(
        key: &str,
        payload_lsn: u64,
        record_ref: RecordRef,
        lifecycle: Option<BlobLifecycle>,
    ) -> GcSourceRecord {
        GcSourceRecord {
            key: self::key(key),
            shard: SHARD,
            payload_lsn,
            record_ref,
            lifecycle,
        }
    }

    fn move_plan(routes: Vec<RouteEstimate>) -> GcPlan {
        let copied_bytes = routes.iter().map(|route| route.bytes).sum();
        GcPlan {
            scenario: GcScenario::DeadRef,
            actions: vec![GcAction::MoveLiveBytes {
                source_segment_id: 1,
                routes,
            }],
            copied_bytes,
            expected_reclaim_bytes: 900,
            score: 1,
        }
    }

    fn route(source_segment_id: u64, end_epoch: Option<u64>, bytes: u64) -> RouteEstimate {
        RouteEstimate {
            source_segment_id,
            end_epoch,
            destination_class: end_epoch
                .map(DestinationClass::ExactEpoch)
                .unwrap_or(DestinationClass::Spillover),
            refs: 1,
            bytes,
        }
    }

    #[test]
    fn selects_live_records_and_preserves_map_ref_identity() {
        let from = record_ref(1, 10, 100);
        let plan = move_plan(vec![route(1, Some(50), 100)]);
        let records = vec![source_record("blob-a", 42, from, Some(lifecycle(50)))];

        let selection = select_copy_records(&plan, &records).unwrap();

        assert_eq!(selection.scenario, GcScenario::DeadRef);
        assert_eq!(selection.copied_bytes, 100);
        assert_eq!(
            selection.records,
            vec![GcCopyRecord {
                key: key("blob-a"),
                shard: SHARD,
                payload_lsn: 42,
                from,
                lifecycle: Some(lifecycle(50)),
                destination_class: DestinationClass::ExactEpoch(50),
            }]
        );
    }

    #[test]
    fn selects_unknown_lifetime_spillover_after_overlay_skipped_records_are_filtered() {
        let plan = move_plan(vec![route(1, None, 80)]);
        let records = vec![source_record("unknown", 2, record_ref(1, 20, 80), None)];

        let selection = select_copy_records(&plan, &records).unwrap();

        assert_eq!(selection.copied_bytes, 80);
        assert_eq!(selection.records.len(), 1);
        assert_eq!(selection.records[0].key, key("unknown"));
        assert_eq!(
            selection.records[0].destination_class,
            DestinationClass::Spillover
        );
    }

    #[test]
    fn errors_when_selected_bytes_do_not_match_route_estimate() {
        let plan = move_plan(vec![route(1, Some(50), 120)]);
        let records = vec![source_record(
            "blob-a",
            42,
            record_ref(1, 10, 100),
            Some(lifecycle(50)),
        )];

        assert_eq!(
            select_copy_records(&plan, &records).unwrap_err(),
            GcSelectionError::CopyBytesMismatch {
                expected: 120,
                actual: 100,
            }
        );
    }

    #[test]
    fn join_multiple_selects_matching_epoch_from_each_source() {
        let routes = vec![route(1, Some(50), 100), route(2, Some(50), 120)];
        let plan = GcPlan {
            scenario: GcScenario::JoinMultiple,
            actions: vec![GcAction::MoveEpochBytes { epoch: 50, routes }],
            copied_bytes: 220,
            expected_reclaim_bytes: 0,
            score: 1,
        };
        let records = vec![
            source_record("left", 10, record_ref(1, 0, 100), Some(lifecycle(50))),
            source_record("right", 11, record_ref(2, 0, 120), Some(lifecycle(50))),
            source_record(
                "other-epoch",
                12,
                record_ref(2, 120, 70),
                Some(lifecycle(60)),
            ),
        ];

        let selection = select_copy_records(&plan, &records).unwrap();

        assert_eq!(selection.copied_bytes, 220);
        assert_eq!(selection.records.len(), 2);
        assert_eq!(selection.records[0].key, key("left"));
        assert_eq!(selection.records[1].key, key("right"));
    }
}
