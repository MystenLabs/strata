//! Shared key routing for the main and relocation LSMs.

use xxhash_rust::xxh3::xxh3_64;

/// Routes one logical blob key to its stable on disk hash partition.
///
/// Both LSMs must hash exactly these bytes with this function. In particular, relocation only
/// suffixes such as shard and payload LSN are not part of routing, so main partition `p` can join
/// only relocation partition `p` during compaction.
pub(crate) fn partition_for_key(key: &[u8], partition_count: u32) -> u32 {
    debug_assert!(partition_count != 0);
    (xxh3_64(key) % u64::from(partition_count)) as u32
}

#[cfg(test)]
mod tests {
    use super::partition_for_key;

    #[test]
    fn one_partition_always_routes_to_zero() {
        for key in [b"a".as_slice(), b"blob", b"another-key"] {
            assert_eq!(partition_for_key(key, 1), 0);
        }
    }

    #[test]
    fn routing_is_bounded_and_uses_multiple_partitions() {
        let routed = (0..100)
            .map(|key| partition_for_key(key.to_string().as_bytes(), 4))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(routed, [0, 1, 2, 3].into_iter().collect());
    }
}
