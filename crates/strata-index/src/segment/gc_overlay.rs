use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use strata_core::{SegmentGcOverlay, SegmentGcOverlayMergeOp, SegmentId};
use typed_store::{
    Map,
    rocks::{DBBatch, default_db_options},
};

use crate::{Error, Result};

use super::super::StrataIndex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EncodedSegmentGcOverlayMergeOperand {
    Op(SegmentGcOverlayMergeOp),
    Ops(Vec<SegmentGcOverlayMergeOp>),
}

impl EncodedSegmentGcOverlayMergeOperand {
    fn into_ops(self) -> Vec<SegmentGcOverlayMergeOp> {
        match self {
            Self::Op(op) => vec![op],
            Self::Ops(ops) => ops,
        }
    }
}

impl StrataIndex {
    pub fn get_segment_gc_overlay(
        &self,
        segment_id: SegmentId,
    ) -> Result<Option<SegmentGcOverlay>> {
        Ok(self.segment_gc_overlay.get(&segment_id)?)
    }

    pub fn merge_segment_gc_overlay_batch(
        &self,
        batch: &mut DBBatch,
        segment_id: SegmentId,
        ops: Vec<SegmentGcOverlayMergeOp>,
    ) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Use a RocksDB merge operand rather than read-modify-write. GC overlay updates are
        // published beside manifest/accounting rows, and several independent compactions can append
        // range facts for the same segment. The merge operator is the physical serialization point
        // that turns those append-only facts into one canonical per-segment overlay.
        let operand =
            encode_segment_gc_overlay_merge_operand(EncodedSegmentGcOverlayMergeOperand::Ops(ops))?;
        batch.partial_merge_batch(self.segment_gc_overlay(), [(&segment_id, operand)])?;
        Ok(())
    }
}

pub(crate) fn segment_gc_overlay_cf_options() -> rocksdb::Options {
    let mut options = default_db_options().options;
    options.set_merge_operator(
        "strata-segment-gc-overlay-merge",
        move |_key: &[u8], existing_value: Option<&[u8]>, operands: &MergeOperands| {
            full_merge_segment_gc_overlay(existing_value, operands)
        },
        move |_key: &[u8], _existing_value: Option<&[u8]>, operands: &MergeOperands| {
            partial_merge_segment_gc_overlay(operands)
        },
    );
    options
}

fn full_merge_segment_gc_overlay(
    existing_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // RocksDB may compact many delayed operands into an existing materialized overlay. The operands
    // are physical ref transitions in commit order; the stored value is just the latest normalized
    // summary. Keeping the fold deterministic matters because retired/expired ranges and lifetime
    // hints are not independent sets: retiring or expiring a range removes any hint for that range,
    // and later lifetime updates must not revive expired bytes.
    let mut state = match existing_value {
        Some(value) => bcs::from_bytes::<SegmentGcOverlay>(value).ok()?,
        None => SegmentGcOverlay::default(),
    };

    for operand in operands {
        let operand = decode_segment_gc_overlay_merge_operand(operand).ok()?;
        state.apply_merge_ops(operand.into_ops());
    }

    bcs::to_bytes(&state).ok()
}

pub(crate) fn partial_merge_segment_gc_overlay(operands: &MergeOperands) -> Option<Vec<u8>> {
    // Partial merge cannot inspect the existing overlay value, so it must not try to resolve
    // retire-vs-lifetime conflicts. It only batches operands into one encoded list; full merge is
    // the point where current state and all pending operands are folded together.
    let mut ops = Vec::new();
    for operand in operands {
        let operand = decode_segment_gc_overlay_merge_operand(operand).ok()?;
        ops.extend(operand.into_ops());
    }
    bcs::to_bytes(&EncodedSegmentGcOverlayMergeOperand::Ops(ops)).ok()
}

pub(crate) fn encode_segment_gc_overlay_merge_operand(
    operand: EncodedSegmentGcOverlayMergeOperand,
) -> Result<Vec<u8>> {
    bcs::to_bytes(&operand).map_err(|err| Error::Serialization(err.to_string()))
}

fn decode_segment_gc_overlay_merge_operand(
    data: &[u8],
) -> Result<EncodedSegmentGcOverlayMergeOperand> {
    bcs::from_bytes(data).map_err(|err| Error::Serialization(err.to_string()))
}
