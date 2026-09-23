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

    /// The encoding is a durable format, so it is pinned to exact bytes here rather than only
    /// round-tripped. These vectors were produced by the encoding that wrote every key currently on
    /// disk; `port-compat` additionally cross-checks them against that implementation directly.
    #[test]
    fn keys_encode_to_their_documented_bytes() {
        fn assert_bytes<K: Serialize>(key: &K, expected: &str) {
            assert_eq!(hex(&encode_key(key).unwrap()), expected);
        }

        assert_bytes(&0u64, "0000000000000000");
        assert_bytes(&1u64, "0000000000000001");
        assert_bytes(&u64::MAX, "ffffffffffffffff");
        assert_bytes(&(7u64, 9u64), "00000000000000070000000000000009");
        assert_bytes(
            &"lsm-manifest-name".to_owned(),
            "00000000000000116c736d2d6d616e69666573742d6e616d65",
        );
        assert_bytes(
            &(42 as SegmentId, 1234 as StrataLsn),
            "000000000000002a00000000000004d2",
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
