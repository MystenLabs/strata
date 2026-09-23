//! Durable key and value encoding for index column families.
//!
//! This is an on-disk format, not an implementation detail: changing either function orphans every
//! existing index. Both encodings are chosen to match what the index was originally written with,
//! so a database written before the storage port existed reads back unchanged.
//!
//! * Keys use bincode with **big-endian, fixed-width integers**. Big-endian matters: RocksDB
//!   compares keys as byte strings, so only a big-endian fixed-width encoding makes numeric key
//!   order agree with iteration order. Segment IDs and LSNs are scanned in order and depend on it.
//! * Values use BCS, which is canonical and already Strata's encoding for records elsewhere.

use serde::{Serialize, de::DeserializeOwned};

use crate::{Error, Result};

/// Encodes a key so that RocksDB's lexicographic ordering matches the key's numeric ordering.
pub fn encode_key<K>(key: &K) -> Result<Vec<u8>>
where
    K: ?Sized + Serialize,
{
    use bincode::Options as _;

    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(key)
        .map_err(|error| Error::Serialization(error.to_string()))
}

/// Decodes a key written by [`encode_key`].
pub fn decode_key<K>(bytes: &[u8]) -> Result<K>
where
    K: DeserializeOwned,
{
    use bincode::Options as _;

    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)
        .map_err(|error| Error::Serialization(error.to_string()))
}

/// Encodes a value.
pub fn encode_value<V>(value: &V) -> Result<Vec<u8>>
where
    V: ?Sized + Serialize,
{
    bcs::to_bytes(value).map_err(|error| Error::Serialization(error.to_string()))
}

/// Decodes a value written by [`encode_value`].
pub fn decode_value<V>(bytes: &[u8]) -> Result<V>
where
    V: DeserializeOwned,
{
    bcs::from_bytes(bytes).map_err(|error| Error::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use core_types::{SegmentId, StrataLsn};

    use super::*;

    /// The port must produce byte-identical keys to the encoding the index shipped with, or every
    /// existing database becomes unreadable. This pins the encoding against typed-store's
    /// `be_fix_int_ser`, which wrote every key currently on disk.
    #[test]
    fn keys_match_the_encoding_already_on_disk() {
        fn assert_same<K: Serialize>(key: &K) {
            let ours = encode_key(key).unwrap();
            let theirs = typed_store::rocks::be_fix_int_ser(key).unwrap();
            assert_eq!(ours, theirs);
        }

        assert_same(&0u64);
        assert_same(&1u64);
        assert_same(&u64::MAX);
        assert_same(&(7u64, 9u64));
        assert_same(&"lsm-manifest-name".to_owned());
        assert_same(&(42 as SegmentId, 1234 as StrataLsn));
    }

    /// Big-endian fixed-width keys must sort numerically under RocksDB's byte comparison, which is
    /// what the segment and LSN scans rely on.
    #[test]
    fn key_bytes_sort_in_numeric_order() {
        let mut encoded: Vec<Vec<u8>> = [1u64, 256, 2, u64::MAX, 0]
            .iter()
            .map(|key| encode_key(key).unwrap())
            .collect();
        encoded.sort();
        let decoded: Vec<u64> = encoded
            .iter()
            .map(|bytes| decode_key(bytes).unwrap())
            .collect();
        assert_eq!(decoded, vec![0, 1, 2, 256, u64::MAX]);
    }

    #[test]
    fn keys_and_values_round_trip() {
        let key: (SegmentId, StrataLsn) = (3, 4);
        assert_eq!(
            decode_key::<(SegmentId, StrataLsn)>(&encode_key(&key).unwrap()).unwrap(),
            key
        );
        let value = "relocation".to_owned();
        assert_eq!(
            decode_value::<String>(&encode_value(&value).unwrap()).unwrap(),
            value
        );
    }
}
