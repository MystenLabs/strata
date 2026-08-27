use core_types::{BlobKey, GarbageEvent, SegmentGcSummaryDelta, SegmentKey, StrataLsn};

use crate::{GarbageRecord, Result, StoredValue, decode_value};

/// Domain specific state transition used by point reads and compaction.
///
/// The engine supplies patches for one key in strictly increasing LSN order.
/// Implementations decode their own bytes and must be deterministic. Reads discard emitted
/// garbage records; compaction returns them for durable publication.
///
/// Returning `None` deletes the materialized key. Returning `Some(bytes)` keeps those bytes as its
/// new base value.
pub trait MergeOperator: Send + Sync + 'static {
    /// Whether base rows without patches must still pass through [`Self::merge`].
    ///
    /// Most operators depend only on stored operands, so an unchanged base can be copied directly.
    /// Operators that consult external state must return true.
    fn merge_base(&self) -> bool {
        false
    }

    /// Folds one key's complete visible patch group.
    ///
    /// `patches` is strictly ordered by lsn and contains no conflicting duplicate lsns.
    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>>;

    /// Optionally replaces consecutive operands with one equivalent operand.
    ///
    /// Returning `None` leaves the operands unchanged.
    fn partial_merge(
        &self,
        _key: &[u8],
        _patches: &[(StrataLsn, &[u8])],
        _emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// Ordinary last-write-wins semantics for [`crate::Lsm::put`] and [`crate::Lsm::put_blob`].
pub struct Replace;

impl MergeOperator for Replace {
    fn merge(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut value = base;
        for (lsn, next) in patches {
            retire_blob(key, *lsn, value, emit)?;
            value = Some(next);
        }
        Ok(value.map(<[u8]>::to_vec))
    }

    fn partial_merge(
        &self,
        key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        for pair in patches.windows(2) {
            retire_blob(key, pair[1].0, Some(pair[0].1), emit)?;
        }
        Ok(patches.last().map(|(_, value)| value.to_vec()))
    }
}

fn retire_blob(
    key: &[u8],
    lsn: StrataLsn,
    value: Option<&[u8]>,
    emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    let StoredValue::Blob { record_ref, .. } = decode_value(value)? else {
        return Ok(());
    };
    let bytes = i128::from(record_ref.len);
    emit(GarbageRecord {
        key: SegmentKey {
            segment_id: record_ref.segment_id,
            blob_key: BlobKey::new(key.to_vec())
                .map_err(|error| crate::Error::Merge(error.to_string()))?,
        },
        lsn,
        event: GarbageEvent::Retired { record: record_ref },
        summary_delta: SegmentGcSummaryDelta {
            live_bytes: -bytes,
            retired_bytes: bytes,
            live_ref_count: -1,
            unknown_lifetime_bytes: -bytes,
            unknown_lifetime_ref_count: -1,
            ..SegmentGcSummaryDelta::default()
        },
    })
}

#[cfg(test)]
mod tests {
    use core_types::RecordRef;

    use super::{MergeOperator, Replace};
    use crate::{
        StoredValue, decode_value,
        engine::{encode_blob_value, encode_inline_value},
    };

    #[test]
    fn replace_handles_inline_and_blob_values() {
        let first = RecordRef {
            segment_id: 1,
            offset: 10,
            len: 20,
        };
        let second_ref = RecordRef {
            segment_id: 2,
            offset: 30,
            len: 40,
        };
        let first = encode_blob_value(&[], first);
        let inline = encode_inline_value(b"inline");
        let second = encode_blob_value(&[], second_ref);
        let patches = [
            (1, first.as_slice()),
            (2, inline.as_slice()),
            (3, second.as_slice()),
        ];
        let mut garbage = Vec::new();

        let value = Replace
            .merge(b"key", None, &patches, &mut |record| {
                garbage.push(record);
                Ok(())
            })
            .unwrap()
            .unwrap();

        assert_eq!(
            decode_value(&value).unwrap(),
            StoredValue::Blob {
                metadata: &[],
                record_ref: second_ref,
            }
        );
        assert_eq!(garbage.len(), 1);
        assert_eq!(garbage[0].event.record().segment_id, 1);
        assert_eq!(garbage[0].lsn, 2);
    }
}
