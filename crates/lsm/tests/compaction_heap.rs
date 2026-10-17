use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc, time::Instant};

use lsm::{
    GarbageRecord, Manifest, ManifestEdit, MergeOperator, Result, StrataLsn, TableReader,
    TableStore, TableTarget, TableWriter, merge_compaction, select_compaction_inputs,
    select_patch_compaction_inputs, write_patch_compaction,
};
use tempfile::TempDir;

struct Append {
    partial: bool,
}

impl MergeOperator for Append {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        patches: &[(StrataLsn, &[u8])],
        _emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        let mut value = base.unwrap_or_default().to_vec();
        for (_, patch) in patches {
            value.extend_from_slice(patch);
        }
        Ok(Some(value))
    }

    fn partial_merge(
        &self,
        key: &[u8],
        patches: &[(StrataLsn, &[u8])],
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<Option<Vec<u8>>> {
        if self.partial {
            self.merge(key, None, patches, emit)
        } else {
            Ok(None)
        }
    }
}

#[test]
fn many_inputs_preserve_key_order_versions_and_partial_merge_semantics() {
    check_merges(64, 8);
}

#[test]
#[ignore = "manual high-fan-in compaction benchmark"]
fn benchmark_many_compaction_inputs() {
    check_merges(2048, 32);
}

fn check_merges(input_count: usize, keys_per_input: usize) {
    let directory = TempDir::new().unwrap();
    let mut patches = Vec::new();
    let mut expected = BTreeMap::<Vec<u8>, Vec<(u64, Vec<u8>)>>::new();
    // Every file shares the first key; the other keys interleave. Reverse file order so
    // input order cannot accidentally stand in for LSN order. Each file has two versions
    // of each key, exercising cursor advancement without losing a same-key operand.
    for input in (0..input_count).rev() {
        let mut writer = TableWriter::create_patch(
            directory.path(),
            format!("input-{input}.sst"),
            input as u64 + 1,
            0,
            "patch-v1",
        )
        .unwrap();
        for ordinal in 0..keys_per_input {
            let suffix = if ordinal == 0 {
                0
            } else {
                ordinal * input_count + input
            };
            let key = format!("k{suffix:08}").into_bytes();
            for version in 0..2 {
                let lsn = (input * 2 + version + 1) as u64;
                let value = lsn.to_be_bytes().to_vec();
                writer
                    .add_prefix_patch(&key[..1], &key[1..], lsn, &value)
                    .unwrap();
                expected.entry(key.clone()).or_default().push((lsn, value));
            }
        }
        patches.push(writer.finish().unwrap());
    }
    for versions in expected.values_mut() {
        versions.sort_unstable_by_key(|(lsn, _)| *lsn);
    }
    let mut base = TableWriter::create_base(
        directory.path(),
        "base.sst",
        input_count as u64 + 1,
        0,
        "base-v1",
    )
    .unwrap();
    base.add_prefix(b"k", b"00000000", b"base").unwrap();
    base.add_prefix(b"z", b"-base-only", b"cold").unwrap();
    let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
    manifest
        .apply(&ManifestEdit {
            add_base: vec![base.finish().unwrap()],
            add_patches: patches.clone(),
            remove: Vec::new(),
            materialized_through: None,
            wal_retained_from: None,
        })
        .unwrap();
    let tables = Arc::new(TableStore::new(directory.path()));
    let inputs = select_compaction_inputs(&manifest, &tables, 0, &patches[0])
        .unwrap()
        .unwrap();
    let start = Instant::now();
    let mut actual = BTreeMap::new();
    let mut previous = Vec::new();
    merge_compaction(
        &inputs,
        &Append { partial: false },
        |key, prefix, value| {
            assert!(previous.as_slice() < key);
            assert_eq!(prefix, 1);
            previous = key.to_vec();
            actual.insert(key.to_vec(), value.to_vec());
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    eprintln!("full merge: {input_count} inputs, {:?}", start.elapsed());
    let mut expected_full: BTreeMap<_, Vec<_>> = expected
        .iter()
        .map(|(key, versions)| {
            (
                key.clone(),
                versions
                    .iter()
                    .flat_map(|(_, value)| value.iter().copied())
                    .collect(),
            )
        })
        .collect();
    expected_full
        .get_mut(b"k00000000".as_slice())
        .unwrap()
        .splice(0..0, b"base".iter().copied());
    expected_full.insert(b"z-base-only".to_vec(), b"cold".to_vec());
    assert_eq!(actual, expected_full);
    drop(inputs);

    for partial in [false, true] {
        let inputs = select_patch_compaction_inputs(&manifest, &tables, 0, &patches[..1])
            .unwrap()
            .unwrap();
        let mut next_id = input_count as u64 + if partial { 100_000 } else { 200_000 };
        let start = Instant::now();
        let (edit, _) = write_patch_compaction(&inputs, &Append { partial }, 1 << 20, || {
            next_id += 1;
            Ok(TableTarget::patch(next_id))
        })
        .unwrap();
        eprintln!(
            "partial={partial}: {input_count} inputs, {:?}",
            start.elapsed()
        );
        let mut actual = BTreeMap::<_, Vec<_>>::new();
        for table in edit.add_patches {
            TableReader::open_patch(directory.path(), &table, "patch-v1")
                .unwrap()
                .scan_patches(None, None, |key, lsn, value| {
                    actual
                        .entry(key.to_vec())
                        .or_default()
                        .push((lsn, value.to_vec()));
                    Ok(())
                })
                .unwrap();
        }
        if partial {
            let expected: BTreeMap<_, _> = expected
                .iter()
                .map(|(key, versions)| {
                    (
                        key.clone(),
                        vec![(
                            versions.last().unwrap().0,
                            versions
                                .iter()
                                .flat_map(|(_, value)| value.iter().copied())
                                .collect::<Vec<_>>(),
                        )],
                    )
                })
                .collect();
            assert_eq!(actual, expected);
        } else {
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn heap_merge_rejects_conflicting_prefixes_and_duplicate_lsns() {
    for conflicting_prefixes in [false, true] {
        let directory = TempDir::new().unwrap();
        let mut patches = Vec::new();
        for id in 1..=2 {
            let mut writer =
                TableWriter::create_patch(directory.path(), format!("{id}.sst"), id, 0, "patch-v1")
                    .unwrap();
            let prefix_len = if conflicting_prefixes { id as usize } else { 1 };
            let lsn = if conflicting_prefixes { id } else { 1 };
            writer
                .add_prefix_patch(&b"key"[..prefix_len], &b"key"[prefix_len..], lsn, b"value")
                .unwrap();
            patches.push(writer.finish().unwrap());
        }
        let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
        manifest
            .apply(&ManifestEdit {
                remove: Vec::new(),
                add_base: Vec::new(),
                add_patches: patches.clone(),
                materialized_through: None,
                wal_retained_from: None,
            })
            .unwrap();
        let tables = Arc::new(TableStore::new(directory.path()));
        let inputs = select_patch_compaction_inputs(&manifest, &tables, 0, &patches)
            .unwrap()
            .unwrap();
        let expected = if conflicting_prefixes {
            "conflicting prefixes"
        } else {
            "duplicate patch lsn"
        };
        let error = merge_compaction(
            &inputs,
            &Append { partial: true },
            |_, _, _| Ok(()),
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        let error = write_patch_compaction(&inputs, &Append { partial: true }, u64::MAX, || {
            Ok(TableTarget::patch(3))
        })
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}
