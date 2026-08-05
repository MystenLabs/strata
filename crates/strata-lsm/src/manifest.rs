use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroU32,
};

use serde::{Deserialize, Serialize};

use crate::{Error, FORMAT_VERSION, Result, StrataLsn, table::validate_relative_path};

/// Metadata for one immutable SST file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
    pub id: u64,
    pub partition: u32,
    pub relative_path: String,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub min_lsn: Option<StrataLsn>,
    pub max_lsn: Option<StrataLsn>,
    /// Highest external LSN whose merge-time global state was applied to every row in this base.
    ///
    /// Patch tables leave this unset because they still contain ordered operands rather than a
    /// resolved value. A base produced while the merge snapshot covered LSN 120 stores `Some(120)`:
    /// a caller can then prove that a global event at LSN 120, such as an epoch transition, was
    /// considered for every key in the table. The field is manifest-only; it describes how the
    /// table was produced and is not part of the immutable SST byte format.
    #[serde(default)]
    pub merge_applied_through_lsn: Option<StrataLsn>,
    pub record_count: u64,
    pub file_len: u64,
    pub checksum: [u8; 32],
}

/// Live SST files for one hash partition.
///
/// Base files are sorted and have non-overlapping key ranges. Patch files may overlap and are merged
/// by each record's `StrataLsn`, never by their order in this vector.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub base: Vec<TableMeta>,
    pub patches: Vec<TableMeta>,
}

/// Materialized live SST set for one LSM instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub generation: u64,
    /// Identifies the complete logical meaning of this LSM, including base bytes and merge policy.
    pub schema_id: String,
    /// Identifies key bytes, patch bytes, and partitioning as one adoption-compatibility contract.
    pub patch_format_id: String,
    pub partition_count: u32,
    pub next_table_id: u64,
    /// Highest contiguous caller LSN fully represented by this manifest.
    pub materialized_through: Option<StrataLsn>,
    /// Legacy store-WAL retention field, kept so existing manifests remain readable.
    /// New stores persist this boundary as store state in RocksDB; the LSM does not update it.
    pub wal_retained_from: u64,
    pub partitions: BTreeMap<u32, PartitionManifest>,
}

/// One appendable change to a manifest.
///
/// SST paths are their identity inside a manifest. An edit removes live paths and adds fully synced
/// immutable files. Applying an edit is atomic: either the complete resulting manifest is valid or
/// the original manifest is left unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEdit {
    pub remove: Vec<String>,
    pub add_base: Vec<TableMeta>,
    pub add_patches: Vec<TableMeta>,
    /// Advances the contiguous caller-LSN prefix represented by the resulting manifest.
    pub materialized_through: Option<StrataLsn>,
    /// Advances the first caller-log file required by recovery.
    pub wal_retained_from: Option<u64>,
}

impl ManifestEdit {
    /// Validates this edit without applying it to a particular manifest.
    pub fn validate(&self) -> Result<()> {
        validate_edit(self)
    }
}

impl Manifest {
    pub fn empty(
        schema_id: impl Into<String>,
        patch_format_id: impl Into<String>,
        partition_count: NonZeroU32,
    ) -> Self {
        let partitions = (0..partition_count.get())
            .map(|partition| (partition, PartitionManifest::default()))
            .collect();
        Self {
            format_version: FORMAT_VERSION,
            generation: 0,
            schema_id: schema_id.into(),
            patch_format_id: patch_format_id.into(),
            partition_count: partition_count.get(),
            next_table_id: 1,
            materialized_through: None,
            wal_retained_from: 1,
            partitions,
        }
    }

    /// Whether this manifest can adopt the other manifest's patch SST files without rewriting.
    pub fn accepts_patch_files_from(&self, other: &Self) -> bool {
        self.format_version == other.format_version
            && self.partition_count == other.partition_count
            && self.patch_format_id == other.patch_format_id
    }

    /// Returns the largest contiguous external-LSN prefix resolved into every live base range.
    ///
    /// Three independent bounds meet here. `materialized_through` proves no earlier mutation is
    /// hiding in a memtable. Every base must have been merged with a snapshot at least this new.
    /// Finally, a live patch beginning at LSN 115 caps the result at 114 because it may contain a
    /// pre-transition lifetime extension not yet folded into its base. For example:
    ///
    /// ```text
    /// materialized through:       140
    /// oldest base merge frontier: 130
    /// oldest patch min LSN:        125
    /// result:                      124
    /// ```
    ///
    /// Empty sides do not impose a bound: an LSM with no bases or patches has no keys whose merge
    /// state can lag its materialized frontier.
    pub fn merge_applied_through_lsn(&self) -> StrataLsn {
        let mut applied = self.materialized_through.unwrap_or_default();
        for tables in self.partitions.values() {
            for base in &tables.base {
                applied = applied.min(base.merge_applied_through_lsn.unwrap_or_default());
            }
            for patch in &tables.patches {
                let before_patch = patch
                    .min_lsn
                    .expect("validated patch tables have a minimum LSN")
                    .saturating_sub(1);
                applied = applied.min(before_patch);
            }
        }
        applied
    }

    /// Applies one file level edit without requiring the manifest generation captured by its
    /// producer to still be current.
    ///
    /// This permits independently prepared edits to commit in either order. Removal still fails if
    /// an input path is no longer live. The returned metadata identifies files made obsolete by the
    /// edit; physical deletion remains subject to snapshot and compaction pins.
    pub fn apply(&mut self, edit: &ManifestEdit) -> Result<Vec<TableMeta>> {
        self.validate()?;
        edit.validate()?;

        let remove: HashSet<_> = edit.remove.iter().map(String::as_str).collect();
        let mut candidate = self.clone();
        let mut obsolete = Vec::with_capacity(remove.len());
        for partition in candidate.partitions.values_mut() {
            partition.base.retain(|table| {
                if remove.contains(table.relative_path.as_str()) {
                    obsolete.push(table.clone());
                    false
                } else {
                    true
                }
            });
            partition.patches.retain(|table| {
                if remove.contains(table.relative_path.as_str()) {
                    obsolete.push(table.clone());
                    false
                } else {
                    true
                }
            });
        }
        if obsolete.len() != remove.len() {
            let removed: HashSet<_> = obsolete
                .iter()
                .map(|table| table.relative_path.as_str())
                .collect();
            let missing = edit
                .remove
                .iter()
                .find(|path| !removed.contains(path.as_str()))
                .expect("different removal counts imply a missing path");
            return invalid_manifest(format!("cannot remove non-live SST {missing}"));
        }

        for table in &edit.add_base {
            add_table(&mut candidate, table.clone(), false)?;
        }
        for table in &edit.add_patches {
            add_table(&mut candidate, table.clone(), true)?;
        }
        for partition in candidate.partitions.values_mut() {
            partition.base.sort_by(|left, right| {
                left.first_key
                    .cmp(&right.first_key)
                    .then_with(|| left.relative_path.cmp(&right.relative_path))
            });
            partition
                .patches
                .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        }
        if let Some(materialized) = edit.materialized_through {
            if candidate
                .materialized_through
                .is_some_and(|current| materialized < current)
            {
                return invalid_manifest(format!(
                    "materialized WAL frontier {materialized:?} precedes current frontier {:?}",
                    candidate.materialized_through
                ));
            }
            candidate.materialized_through = Some(materialized);
        }
        if let Some(retained_from) = edit.wal_retained_from {
            if retained_from < candidate.wal_retained_from {
                return invalid_manifest(format!(
                    "retained WAL file {retained_from} precedes current file {}",
                    candidate.wal_retained_from
                ));
            }
            candidate.wal_retained_from = retained_from;
        }
        candidate.generation =
            candidate
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::InvalidManifest {
                    reason: "manifest generation overflow".to_owned(),
                })?;
        candidate.validate()?;

        *self = candidate;
        Ok(obsolete)
    }

    /// Validates the complete materialized manifest.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != FORMAT_VERSION {
            return invalid_manifest(format!(
                "format version {} is not {FORMAT_VERSION}",
                self.format_version
            ));
        }
        if self.partition_count == 0 {
            return invalid_manifest("partition count must be non-zero");
        }
        if self.wal_retained_from == 0 {
            return invalid_manifest("first retained WAL file must be non-zero");
        }
        if self.wal_retained_from > 1 && self.materialized_through.is_none() {
            return invalid_manifest("a reclaimed WAL prefix needs a materialized frontier");
        }
        if self.schema_id.is_empty() || self.patch_format_id.is_empty() {
            return invalid_manifest("schema and patch format identifiers must be non-empty");
        }
        if self.partitions.len() != self.partition_count as usize {
            return invalid_manifest("partition map does not match partition count");
        }

        let mut paths = HashSet::new();
        for partition in 0..self.partition_count {
            let tables = self
                .partitions
                .get(&partition)
                .ok_or_else(|| Error::InvalidManifest {
                    reason: format!("partition {partition} is missing"),
                })?;
            let mut previous_last: Option<&[u8]> = None;
            for table in &tables.base {
                validate_table(table, partition, false, &mut paths)?;
                if previous_last.is_some_and(|last| last >= table.first_key.as_slice()) {
                    return invalid_manifest(format!(
                        "base SST key ranges overlap or are unsorted in partition {partition}"
                    ));
                }
                previous_last = Some(&table.last_key);
            }
            for table in &tables.patches {
                validate_table(table, partition, true, &mut paths)?;
            }
        }
        Ok(())
    }
}

fn validate_edit(edit: &ManifestEdit) -> Result<()> {
    if edit.remove.is_empty()
        && edit.add_base.is_empty()
        && edit.add_patches.is_empty()
        && edit.materialized_through.is_none()
        && edit.wal_retained_from.is_none()
    {
        return invalid_manifest("manifest edit must not be empty");
    }

    let mut remove = HashSet::new();
    for path in &edit.remove {
        validate_relative_path(path)?;
        if !remove.insert(path.as_str()) {
            return invalid_manifest(format!("manifest edit removes SST {path} more than once"));
        }
    }

    let mut add = HashSet::new();
    for table in &edit.add_base {
        validate_added_table(table, false)?;
        validate_added_path(table, &remove, &mut add)?;
    }
    for table in &edit.add_patches {
        validate_added_table(table, true)?;
        validate_added_path(table, &remove, &mut add)?;
    }
    Ok(())
}

fn validate_added_path<'a>(
    table: &'a TableMeta,
    remove: &HashSet<&str>,
    add: &mut HashSet<&'a str>,
) -> Result<()> {
    validate_relative_path(&table.relative_path)?;
    if remove.contains(table.relative_path.as_str()) {
        return invalid_manifest(format!(
            "manifest edit cannot replace SST {} at the same path",
            table.relative_path
        ));
    }
    if !add.insert(table.relative_path.as_str()) {
        return invalid_manifest(format!(
            "manifest edit adds SST {} more than once",
            table.relative_path
        ));
    }
    Ok(())
}

fn validate_added_table(table: &TableMeta, patch: bool) -> Result<()> {
    if table.first_key > table.last_key || table.record_count == 0 || table.file_len == 0 {
        return invalid_manifest(format!(
            "SST {} has invalid bounds or size",
            table.relative_path
        ));
    }
    if table.id == u64::MAX {
        return invalid_manifest(format!("SST {} has an overflowing ID", table.relative_path));
    }
    match (
        patch,
        table.min_lsn,
        table.max_lsn,
        table.merge_applied_through_lsn,
    ) {
        (false, None, None, _) => Ok(()),
        (true, Some(min), Some(max), None) if min <= max => Ok(()),
        _ => invalid_manifest(format!(
            "SST {} has invalid lsn bounds or merge frontier",
            table.relative_path
        )),
    }
}

fn add_table(manifest: &mut Manifest, table: TableMeta, patch: bool) -> Result<()> {
    let next_table_id = table
        .id
        .checked_add(1)
        .ok_or_else(|| Error::InvalidManifest {
            reason: format!("SST {} has an overflowing ID", table.relative_path),
        })?;
    manifest.next_table_id = manifest.next_table_id.max(next_table_id);
    let partition =
        manifest
            .partitions
            .get_mut(&table.partition)
            .ok_or(Error::InvalidPartition {
                partition: table.partition,
                partition_count: manifest.partition_count,
            })?;
    if patch {
        partition.patches.push(table);
    } else {
        partition.base.push(table);
    }
    Ok(())
}

fn validate_table<'a>(
    table: &'a TableMeta,
    partition: u32,
    patch: bool,
    paths: &mut HashSet<&'a str>,
) -> Result<()> {
    validate_relative_path(&table.relative_path)?;
    if table.partition != partition {
        return invalid_manifest(format!(
            "SST {} belongs to partition {}, not {partition}",
            table.relative_path, table.partition
        ));
    }
    if table.first_key > table.last_key || table.record_count == 0 || table.file_len == 0 {
        return invalid_manifest(format!(
            "SST {} has invalid bounds or size",
            table.relative_path
        ));
    }
    if !paths.insert(&table.relative_path) {
        return invalid_manifest(format!(
            "SST {} appears more than once",
            table.relative_path
        ));
    }
    match (
        patch,
        table.min_lsn,
        table.max_lsn,
        table.merge_applied_through_lsn,
    ) {
        (false, None, None, _) => {}
        (true, Some(min), Some(max), None) if min <= max => {}
        _ => {
            return invalid_manifest(format!(
                "SST {} has invalid lsn bounds or merge frontier",
                table.relative_path
            ));
        }
    }
    Ok(())
}

fn invalid_manifest<T>(reason: impl Into<String>) -> Result<T> {
    Err(Error::InvalidManifest {
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::{Manifest, ManifestEdit, TableMeta};
    fn base(id: u64, path: &str, first: &[u8], last: &[u8]) -> TableMeta {
        TableMeta {
            id,
            partition: 0,
            relative_path: path.to_owned(),
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            min_lsn: None,
            max_lsn: None,
            merge_applied_through_lsn: None,
            record_count: 1,
            file_len: 1,
            checksum: [0; 32],
        }
    }

    fn patch(id: u64, path: &str, first: &[u8], last: &[u8]) -> TableMeta {
        TableMeta {
            min_lsn: Some(1),
            max_lsn: Some(1),
            ..base(id, path, first, last)
        }
    }

    fn edit(
        remove: &[&str],
        add_base: Vec<TableMeta>,
        add_patches: Vec<TableMeta>,
    ) -> ManifestEdit {
        ManifestEdit {
            remove: remove.iter().map(|path| (*path).to_owned()).collect(),
            add_base,
            add_patches,
            materialized_through: None,
            wal_retained_from: None,
        }
    }

    fn four_base_files() -> Manifest {
        let mut manifest =
            Manifest::empty("test-v1", "test-patches-v1", NonZeroU32::new(1).unwrap());
        manifest.partitions.get_mut(&0).unwrap().base = vec![
            base(1, "a.sst", b"a", b"b"),
            base(2, "b.sst", b"c", b"d"),
            base(3, "c.sst", b"e", b"f"),
            base(4, "d.sst", b"g", b"h"),
        ];
        manifest.next_table_id = 5;
        manifest
    }

    #[test]
    fn patch_compatibility_does_not_require_the_same_base_schema() {
        let partitions = NonZeroU32::new(4).unwrap();
        let left = Manifest::empty("left-v1", "shared-patches-v1", partitions);
        let right = Manifest::empty("right-v1", "shared-patches-v1", partitions);
        assert!(left.accepts_patch_files_from(&right));
    }

    #[test]
    fn empty_manifest_contains_every_partition() {
        let manifest = Manifest::empty("test-v1", "test-patches-v1", NonZeroU32::new(3).unwrap());
        assert_eq!(manifest.partitions.len(), 3);
        assert_eq!(manifest.partition_count, 3);
        assert_eq!(manifest.materialized_through, None);
        assert_eq!(manifest.wal_retained_from, 1);
    }

    #[test]
    fn materialized_frontier_cannot_regress() {
        let mut manifest =
            Manifest::empty("test-v1", "test-patches-v1", NonZeroU32::new(1).unwrap());
        let mut first = edit(&[], Vec::new(), vec![patch(1, "a.sst", b"a", b"a")]);
        first.materialized_through = Some(2);
        manifest.apply(&first).unwrap();

        let mut stale = edit(&[], Vec::new(), vec![patch(2, "b.sst", b"b", b"b")]);
        stale.materialized_through = Some(1);
        assert!(matches!(
            manifest.apply(&stale),
            Err(crate::Error::InvalidManifest { .. })
        ));
        assert_eq!(manifest.materialized_through, Some(2));
    }

    #[test]
    fn merge_frontier_is_bounded_by_the_oldest_base_and_patch() {
        let mut first_base = base(1, "a.sst", b"a", b"m");
        first_base.merge_applied_through_lsn = Some(130);
        let mut second_base = base(2, "n.sst", b"n", b"z");
        second_base.merge_applied_through_lsn = Some(135);
        let mut old_patch = patch(3, "p.sst", b"b", b"b");
        old_patch.min_lsn = Some(125);
        old_patch.max_lsn = Some(140);

        let mut manifest =
            Manifest::empty("test-v1", "test-patches-v1", NonZeroU32::new(1).unwrap());
        let mut initial = edit(&[], vec![first_base, second_base], vec![old_patch]);
        initial.materialized_through = Some(140);
        manifest.apply(&initial).unwrap();

        // Even though the patch extends to LSN 140, its LSN-125 operand may be a lifetime
        // extension that must be folded before an epoch transition at 125 can be called applied.
        assert_eq!(manifest.merge_applied_through_lsn(), 124);

        manifest
            .apply(&edit(&["p.sst"], Vec::new(), Vec::new()))
            .unwrap();
        assert_eq!(manifest.merge_applied_through_lsn(), 130);
    }

    #[test]
    fn disjoint_edits_apply_in_either_order() {
        let first = edit(
            &["a.sst", "b.sst"],
            vec![base(5, "x.sst", b"a", b"d")],
            Vec::new(),
        );
        let second = edit(
            &["c.sst", "d.sst"],
            vec![base(6, "y.sst", b"e", b"h")],
            Vec::new(),
        );

        let mut first_then_second = four_base_files();
        first_then_second.apply(&first).unwrap();
        first_then_second.apply(&second).unwrap();

        let mut second_then_first = four_base_files();
        second_then_first.apply(&second).unwrap();
        second_then_first.apply(&first).unwrap();

        assert_eq!(first_then_second, second_then_first);
        assert_eq!(first_then_second.generation, 2);
        assert_eq!(
            first_then_second.partitions[&0]
                .base
                .iter()
                .map(|table| table.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["x.sst", "y.sst"]
        );
    }

    #[test]
    fn apply_returns_obsolete_metadata() {
        let mut manifest = four_base_files();
        let obsolete = manifest
            .apply(&edit(
                &["a.sst", "b.sst"],
                vec![base(5, "x.sst", b"a", b"d")],
                Vec::new(),
            ))
            .unwrap();

        assert_eq!(
            obsolete
                .iter()
                .map(|table| table.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["a.sst", "b.sst"]
        );
    }

    #[test]
    fn failed_edit_leaves_manifest_unchanged() {
        let mut manifest = four_base_files();
        let original = manifest.clone();
        let overlapping = edit(&["a.sst"], vec![base(5, "x.sst", b"a", b"f")], Vec::new());

        assert!(manifest.apply(&overlapping).is_err());
        assert_eq!(manifest, original);

        assert!(
            manifest
                .apply(&edit(&["missing.sst"], Vec::new(), Vec::new()))
                .is_err()
        );
        assert_eq!(manifest, original);
    }

    #[test]
    fn overlapping_patch_additions_are_valid() {
        let mut manifest = four_base_files();
        manifest
            .apply(&edit(
                &[],
                Vec::new(),
                vec![
                    patch(5, "p2.sst", b"b", b"g"),
                    patch(6, "p1.sst", b"a", b"h"),
                ],
            ))
            .unwrap();

        assert_eq!(
            manifest.partitions[&0]
                .patches
                .iter()
                .map(|table| table.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["p1.sst", "p2.sst"]
        );
    }
}
