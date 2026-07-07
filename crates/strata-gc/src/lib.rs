//! Pure garbage-collection planning for Strata segment files.
//!
//! This crate does not copy bytes or publish metadata. It consumes a snapshot of segment metadata
//! and returns the best next GC plan under a configurable policy. Execution belongs in a later
//! layer that can claim jobs, copy records, publish `MapRef` operations, and clean up files.

mod control;
mod planner;
mod selector;

pub use control::{
    GcControlDecision, GcControlInputs, GcControlPolicy, GcDiskSpace, GcFreeSpacePolicy,
    GcFreeSpaceThreshold, GcIoBudgetPolicy, GcPlannerPressurePolicy, GcPlannerPressureThresholds,
    GcPressureLevel,
};
pub use planner::{
    DestinationClass, GcAction, GcPlan, GcPlanner, GcPlannerConfig, GcScenario, GcSnapshot,
    RouteEstimate, SegmentSnapshot,
};
pub use selector::{
    GcCopyRecord, GcCopySelection, GcSelectionError, GcSourceRecord, select_copy_records,
};
