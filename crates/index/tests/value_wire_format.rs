//! Pins the wire names of every stored value type, and the schema evolution they exist to allow.
//!
//! Values are MessagePack with named fields, and the names are short because they repeat in every
//! stored row. That makes them a durable registry: a name must never change, and a retired name must
//! never be reused, or previously written rows decode into the wrong fields.
//!
//! Nothing in the compiler enforces that, so these tests do. Adding a field makes the relevant
//! assertion fail, which is the moment to choose its wire name deliberately rather than by accident.

use std::collections::BTreeMap;

use core_types::{
    EpochBucket, PlacementClass, SegmentFileState, SegmentGcSummary, SegmentGcSummaryDelta,
    SegmentOwner, SegmentState, ShardCleanupJob, ShardCleanupState, ShardInfo, ShardKey,
    ShardState,
};
use index::port::codec::{decode_value, encode_value};
use lsm::{GarbageLogPosition, Manifest, ManifestEdit, OperandFloor, PartitionManifest, TableMeta};
use serde::Serialize;

/// The wire names a value type writes, sorted.
fn wire_names<T: Serialize>(value: &T) -> Vec<String> {
    let encoded = encode_value(value).expect("encode");
    let decoded: rmpv::Value = rmp_serde::from_slice(&encoded).expect("decode as generic msgpack");
    let map = decoded.as_map().expect("values encode as maps, not arrays");
    let mut names = map
        .iter()
        .map(|(key, _)| key.as_str().expect("map keys are field names").to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn assert_wire_names<T: Serialize>(value: &T, expected: &[&str]) {
    let mut expected = expected
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        wire_names(value),
        expected,
        "wire names changed. Renaming or reusing a name makes already-written rows decode into the \
         wrong fields; adding a field means choosing a new name that has never been used before."
    );
}

fn segment_state() -> SegmentState {
    SegmentState {
        owner: SegmentOwner::Shard(ShardKey {
            id: 3,
            generation: 1,
        }),
        segment_id: 4242,
        volume_id: 0,
        path: "000123.data".to_owned(),
        placement_class: PlacementClass::ExactEpoch(50),
        state: SegmentFileState::Sealed,
        write_offset: 1 << 20,
        durable_offset: 1 << 20,
        min_lsn: Some(1),
        max_lsn: Some(9999),
        sealed_before_lsn: Some(10000),
        sealed_len: Some(1 << 20),
        sealed_sha256: Some([7u8; 32]),
    }
}

fn table_meta() -> TableMeta {
    TableMeta {
        id: 1,
        partition: 0,
        relative_path: "p0/t000001.sst".to_owned(),
        first_key: b"blob-00000000".to_vec(),
        last_key: b"blob-00001000".to_vec(),
        min_lsn: Some(1000),
        max_lsn: Some(1999),
        merge_applied_through_lsn: Some(1500),
        global_operand_floor: OperandFloor::At(1010),
        record_count: 10_000,
        file_len: 1 << 22,
        checksum: [9u8; 32],
    }
}

#[test]
fn segment_value_wire_names_are_pinned() {
    assert_wire_names(
        &segment_state(),
        &[
            "ow", "sid", "vid", "p", "pc", "st", "wo", "do", "ml", "xl", "sb", "sl", "sh",
        ],
    );
    assert_wire_names(
        &SegmentGcSummary::default(),
        &[
            "tb", "lb", "rb", "eb", "lr", "ub", "ur", "mn", "mx", "fh", "xh",
        ],
    );
    assert_wire_names(
        &SegmentGcSummaryDelta::default(),
        &["tb", "lb", "rb", "eb", "lr", "ub", "ur", "eby", "erf", "xc"],
    );
    assert_wire_names(&EpochBucket::default(), &["r", "b"]);
}

#[test]
fn shard_value_wire_names_are_pinned() {
    assert_wire_names(
        &ShardInfo {
            current_generation: 1,
            state: ShardState::Active,
        },
        &["g", "s"],
    );
    assert_wire_names(
        &ShardKey {
            id: 1,
            generation: 2,
        },
        &["i", "g"],
    );
    assert_wire_names(
        &ShardCleanupJob {
            shard: ShardKey {
                id: 17,
                generation: 4,
            },
            drop_lsn: 42,
            state: ShardCleanupState::ReadyForGc,
        },
        &["sh", "dl", "st"],
    );
}

#[test]
fn lsm_value_wire_names_are_pinned() {
    assert_wire_names(
        &table_meta(),
        &[
            "i", "pt", "rp", "fk", "lk", "ml", "xl", "ma", "gf", "rc", "fl", "ck",
        ],
    );
    assert_wire_names(&PartitionManifest::default(), &["b", "p"]);
    assert_wire_names(
        &Manifest {
            format_version: 1,
            generation: 7,
            schema_id: "s".to_owned(),
            patch_format_id: "p".to_owned(),
            partition_count: 4,
            next_table_id: 8,
            materialized_through: Some(1),
            wal_retained_from: 0,
            partitions: BTreeMap::new(),
        },
        &["fv", "g", "si", "pf", "pc", "nt", "mt", "wr", "ps"],
    );
    assert_wire_names(
        &ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: Vec::new(),
            materialized_through: None,
            wal_retained_from: None,
        },
        &["rm", "ab", "ap", "mt", "wr"],
    );
    assert_wire_names(&GarbageLogPosition::default(), &["l", "o"]);
}

/// The property the whole encoding exists for: a value type can gain a field without orphaning rows
/// already on disk, and an older reader tolerates rows written by a newer one.
#[test]
fn a_value_type_can_gain_a_field_without_orphaning_stored_rows() {
    /// `SegmentState` as it would look after gaining a field.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Extended {
        #[serde(rename = "ow")]
        owner: SegmentOwner,
        #[serde(rename = "sid")]
        segment_id: u64,
        #[serde(rename = "vid")]
        volume_id: u64,
        #[serde(rename = "p")]
        path: String,
        #[serde(rename = "pc")]
        placement_class: PlacementClass,
        #[serde(rename = "st")]
        state: SegmentFileState,
        #[serde(rename = "wo")]
        write_offset: u64,
        #[serde(rename = "do")]
        durable_offset: u64,
        #[serde(rename = "ml")]
        min_lsn: Option<u64>,
        #[serde(rename = "xl")]
        max_lsn: Option<u64>,
        #[serde(rename = "sb")]
        sealed_before_lsn: Option<u64>,
        #[serde(rename = "sl")]
        sealed_len: Option<u64>,
        #[serde(rename = "sh")]
        sealed_sha256: Option<[u8; 32]>,
        /// New, and therefore defaulted: rows written before it exists carry no such key.
        #[serde(default, rename = "tr")]
        tier: u8,
    }

    let stored = encode_value(&segment_state()).unwrap();

    // Forwards: a new reader decodes an old row, filling the new field from its default.
    let read_by_new: Extended = decode_value(&stored).unwrap();
    assert_eq!(read_by_new.tier, 0);
    assert_eq!(read_by_new.segment_id, 4242);

    // Backwards: an old reader decodes a row written by a new writer, ignoring what it cannot name.
    let written_by_new = encode_value(&Extended {
        tier: 3,
        ..read_by_new
    })
    .unwrap();
    let read_by_old: SegmentState = decode_value(&written_by_new).unwrap();
    assert_eq!(read_by_old, segment_state());
}

/// Without `#[serde(default)]` a new field is a hard requirement, and old rows stop decoding. This
/// pins that so the requirement is a known cost of the encoding rather than a surprise.
#[test]
fn a_new_field_without_a_default_rejects_older_rows() {
    #[derive(Debug, serde::Deserialize)]
    struct Strict {
        #[serde(rename = "sid")]
        _segment_id: u64,
        #[serde(rename = "tr")]
        _tier: u8,
    }

    let stored = encode_value(&segment_state()).unwrap();
    let error = decode_value::<Strict>(&stored).expect_err("a required new field must not decode");
    assert!(
        error.to_string().contains("missing field"),
        "unexpected error: {error}"
    );
}
