//! Public accounting state model.
//!
//! Serialized base/patch records live in `records`; all state-machine transitions and logical ref
//! event emission live in `reducer`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strata_core::{BlobKey, BlobLifecycle, Epoch, RecordRef, SegmentOwner, ShardKey, StrataLsn};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobUpdate {
    Put {
        lsn: StrataLsn,
        key: BlobKey,
        shard: ShardKey,
        record_ref: RecordRef,
        current_epoch: Epoch,
        lifecycle: Option<BlobLifecycle>,
    },
    Tombstone {
        lsn: StrataLsn,
        key: BlobKey,
    },
    SetLifetime {
        lsn: StrataLsn,
        key: BlobKey,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
    },
    MapRef {
        lsn: StrataLsn,
        key: BlobKey,
        from: RecordRef,
        to: RecordRef,
    },
}

impl BlobUpdate {
    pub fn lsn(&self) -> StrataLsn {
        match self {
            Self::Put { lsn, .. }
            | Self::Tombstone { lsn, .. }
            | Self::SetLifetime { lsn, .. }
            | Self::MapRef { lsn, .. } => *lsn,
        }
    }

    pub fn key(&self) -> &BlobKey {
        match self {
            Self::Put { key, .. }
            | Self::Tombstone { key, .. }
            | Self::SetLifetime { key, .. }
            | Self::MapRef { key, .. } => key,
        }
    }
}

/// Physical payload selected by the folded state for one blob key.
///
/// This is not the logical blob value; it is the storage address whose segment bytes are currently
/// protected by the accounting index. It is born when a `Put` survives into the folded state, is
/// rewritten by `MapRef`, and is retired when a later put or tombstone removes this physical range
/// from the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivePayload {
    /// The payload's own write LSN is kept separately from `head_lsn` because later metadata-only
    /// operations can advance the folded state without changing which physical record range is live.
    pub payload_lsn: StrataLsn,
    /// Shard identity travels with the live physical ref so a future `Live` event can be reconstructed
    /// from folded state without consulting the original delta log.
    pub shard: ShardKey,
    /// Physical ownership determines whether shard drop needs a range retirement or can rely on
    /// whole-directory deletion.
    pub owner: SegmentOwner,
    /// This is the byte range that segment accounting and the GC overlay protect. The reducer treats
    /// changes to this field as physical storage transitions, not merely logical metadata updates.
    pub record_ref: RecordRef,
}

/// Deferred or applied physical rewrite for a live payload range.
///
/// `MapRef` models storage relocation: the logical key is unchanged, but GC must retire one segment
/// range and optionally install lifecycle metadata on another. It can be born before the source
/// payload is visible in the current fold, so the state keeps unresolved maps until the matching
/// physical ref appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapRef {
    /// The rewrite's LSN determines the retire/live ordering used for event publication once the
    /// source range is found.
    pub lsn: StrataLsn,
    /// Source range that must still be protected until the rewrite is materialized.
    pub from: RecordRef,
    /// Replacement range that inherits the key's lifecycle when the rewrite is applied.
    pub to: RecordRef,
}

/// Logical delete marker that shadows older physical payload state.
///
/// A tombstone is durable state, not an immediate absence. It is born when a delete is folded and can
/// outlive the compaction level that first saw it so that a later major compaction can retire an
/// older base payload at the correct LSN. It is cleared only by a later `Put`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The delete LSN is the point at which an older physical record becomes tombstoned for GC
    /// accounting, even if that older record is discovered in a later compaction layer.
    pub lsn: StrataLsn,
}

/// Key-level lifecycle metadata carried across payload changes.
///
/// Lifecycle is stored independently from `LivePayload` because lifetime extensions can arrive
/// before a payload exists, after a payload exists, or between physical rewrites. The value is born
/// from `Put` lifecycle metadata or `SetLifetime`, follows future puts/maps, and can be cleared by a
/// lifecycle change that removes overlay metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleChange {
    /// This LSN lets metadata-only changes advance the folded state without pretending a new payload
    /// record was written.
    pub lsn: StrataLsn,
    /// `None` is meaningful: it represents clearing lifecycle overlay state for the current physical
    /// range rather than the absence of a recorded change.
    pub new_lifecycle: Option<BlobLifecycle>,
}

/// Durable per-key image stored in base runs and reused as the reducer's transient state.
///
/// This struct is the point where logical blob history becomes physical storage truth. In a base run,
/// one `MaterializedBlobState` row represents all updates for a key that have been absorbed by major
/// compaction. During delta and patch folding, the same shape is used in memory so event generation
/// and persisted state follow one state machine. A persisted row is born when major compaction writes
/// a base run, is superseded by the next base run for the partition, and may contain no live payload
/// when the important durable fact is a tombstone, lifecycle marker, or unresolved map.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MaterializedBlobState {
    /// The folded state needs a causality watermark even when the latest update did not create a live
    /// payload. This is what lets base-run metadata advertise how far compaction has materialized a
    /// key whose last operation may have been a tombstone, lifecycle change, or pending map.
    pub head_lsn: StrataLsn,
    /// One live physical range per shard generation. The foreground index permits the same blob key
    /// to be live in multiple shards, so accounting must protect those ranges independently.
    /// An empty map does not make the row disposable because tombstones and pending maps can still
    /// have future physical effects.
    pub payloads: BTreeMap<ShardKey, LivePayload>,
    /// Lifecycle is key-level intent carried until there is a physical range to annotate. Keeping it
    /// beside, rather than inside, the payload lets a later put inherit the lifetime and lets a map
    /// transfer that lifetime to the replacement segment range.
    pub lifecycle: Option<LifecycleChange>,
    /// This marker preserves a delete across LSM levels. It lets shallow compaction record "an older
    /// payload must die at this LSN" without seeing that older payload yet, which is why tombstoned
    /// rows may still need to be materialized into the next base run.
    pub tombstone: Option<Tombstone>,
    /// Pending maps are delayed physical rewrites. They are kept in the state row because the source
    /// ref may live in an older base row or may be introduced by a later patch; dropping them would
    /// leak or prematurely retire segment ranges.
    pub pending_maps: Vec<MapRef>,
}

impl MaterializedBlobState {
    pub fn is_live(&self) -> bool {
        !self.payloads.is_empty()
    }

    pub fn lifecycle_value(&self) -> Option<BlobLifecycle> {
        self.lifecycle.and_then(|change| change.new_lifecycle)
    }
}

mod records;
mod reducer;

pub(crate) use records::{PatchRecord, PatchUpdate, RecordLsn, StateRecord, sort_updates};
pub(crate) use reducer::{fold_patch_update, fold_update};
