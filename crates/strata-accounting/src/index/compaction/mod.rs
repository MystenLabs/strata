//! Accounting-index compaction by level.
//!
//! Delta compaction is shallow and writes residual patch history. Major compaction folds base and
//! patch runs into materialized state. Shard-drop compaction rewrites fully materialized bases.

mod delta;
mod delta_updates;
mod major;
mod merge;
mod shard_drop;
