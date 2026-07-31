use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    BlockCacheStats, Error, Manifest, MergeOperator, Result, StrataLsn, TableMeta, TableReader,
    table::{
        BlockCache, DEFAULT_BLOCK_CACHE_BYTES, TableCursor, sync_parent, validate_relative_path,
    },
};

type ReadParts = (Option<Vec<u8>>, Vec<(StrataLsn, Vec<u8>)>);

/// Live immutable read views, indexed by their maximum visible lsn.
#[derive(Debug, Clone, Default)]
pub struct LiveSnapshots {
    lsns: Arc<Mutex<BTreeMap<u64, usize>>>,
}

impl LiveSnapshots {
    pub fn pin(&self, lsn: u64) -> SnapshotPin {
        *self
            .lsns
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(lsn)
            .or_default() += 1;
        SnapshotPin {
            lsn,
            snapshots: self.clone(),
        }
    }

    /// Returns whether a live snapshot could reference a segment published at `lsn`.
    pub fn protects(&self, lsn: u64) -> bool {
        self.lsns
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last_key_value()
            .is_some_and(|(&snapshot, _)| snapshot >= lsn)
    }
}

/// One live snapshot registration.
#[derive(Debug)]
pub struct SnapshotPin {
    lsn: u64,
    snapshots: LiveSnapshots,
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        let mut lsns = self
            .snapshots
            .lsns
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let count = lsns
            .get_mut(&self.lsn)
            .expect("snapshot releases a lsn it previously pinned");
        *count -= 1;
        if *count == 0 {
            lsns.remove(&self.lsn);
        }
    }
}

#[derive(Default)]
struct TableUse {
    snapshot_count: usize,
    reserved_for_compaction: bool,
}

impl TableUse {
    fn is_unused(&self) -> bool {
        self.snapshot_count == 0 && !self.reserved_for_compaction
    }
}

/// Immutable-table directory and runtime file-lifetime coordination.
///
/// Snapshot pins do not conflict with compaction. A compaction reservation is exclusive against
/// other compactions and also pins its inputs against physical deletion. Cleanup calls
/// [`TableStore::remove_if_unpinned`] only after a file becomes obsolete.
pub struct TableStore {
    root: PathBuf,
    uses: Mutex<HashMap<String, TableUse>>,
    live_snapshots: LiveSnapshots,
    block_cache: Arc<BlockCache>,
}

impl TableStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_block_cache_capacity(root, DEFAULT_BLOCK_CACHE_BYTES)
    }

    pub fn with_block_cache_capacity(root: impl Into<PathBuf>, capacity: usize) -> Self {
        Self {
            root: root.into(),
            uses: Mutex::new(HashMap::new()),
            live_snapshots: LiveSnapshots::default(),
            block_cache: Arc::new(BlockCache::new(capacity)),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn live_snapshots(&self) -> LiveSnapshots {
        self.live_snapshots.clone()
    }

    pub fn block_cache_stats(&self) -> BlockCacheStats {
        self.block_cache.stats()
    }

    pub fn is_pinned(&self, table: &TableMeta) -> bool {
        self.uses()
            .get(&table.relative_path)
            .is_some_and(|table_use| !table_use.is_unused())
    }

    /// Attempts to reserve all input SSTs for one compaction.
    ///
    /// Returns `Ok(None)` without reserving anything when another compaction holds any requested
    /// file. Snapshots do not prevent a reservation. The caller must select live SST metadata from
    /// the current manifest; the future manifest owner will enforce that boundary.
    pub fn reserve_for_compaction(
        self: &Arc<Self>,
        tables: &[TableMeta],
    ) -> Result<Option<CompactionReservation>> {
        if tables.is_empty() {
            return Err(Error::InvalidTable(
                "a compaction reservation must contain at least one SST".to_owned(),
            ));
        }

        let mut unique = HashSet::new();
        for table in tables {
            validate_relative_path(&table.relative_path)?;
            if !unique.insert(table.relative_path.as_str()) {
                return Err(Error::InvalidTable(format!(
                    "compaction reservation contains SST {} more than once",
                    table.relative_path
                )));
            }
        }

        let paths: Vec<_> = tables
            .iter()
            .map(|table| table.relative_path.clone())
            .collect();
        let mut uses = self.uses();
        if paths.iter().any(|path| {
            uses.get(path)
                .is_some_and(|table_use| table_use.reserved_for_compaction)
        }) {
            return Ok(None);
        }
        for path in &paths {
            uses.entry(path.clone())
                .or_default()
                .reserved_for_compaction = true;
        }
        drop(uses);

        Ok(Some(CompactionReservation {
            store: Arc::clone(self),
            paths,
        }))
    }

    /// Removes an obsolete SST when no snapshot or compaction pins it.
    ///
    /// Returns `false` when the file is pinned. Returns `true` when the file was removed or was
    /// already absent. The caller must ensure the table is no longer in the current manifest.
    pub fn remove_if_unpinned(&self, table: &TableMeta) -> Result<bool> {
        validate_relative_path(&table.relative_path)?;
        let uses = self.uses();
        if uses
            .get(&table.relative_path)
            .is_some_and(|table_use| !table_use.is_unused())
        {
            return Ok(false);
        }

        let path = self.root.join(&table.relative_path);
        match fs::remove_file(&path) {
            Ok(()) => sync_parent(&path)?,
            Err(source) if source.kind() == ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
        self.block_cache.remove_table(table.id);
        Ok(true)
    }

    fn pin(self: &Arc<Self>, tables: impl Iterator<Item = String>) -> TablePinGuard {
        let tables: Vec<_> = tables.collect();
        {
            let mut uses = self.uses();
            for table in &tables {
                uses.entry(table.clone()).or_default().snapshot_count += 1;
            }
        }
        TablePinGuard {
            store: Arc::clone(self),
            tables,
        }
    }

    fn uses(&self) -> MutexGuard<'_, HashMap<String, TableUse>> {
        self.uses.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Exclusive input-SST reservation held for the lifetime of one compaction job.
///
/// Dropping the reservation releases every input atomically. The files may still remain pinned by
/// snapshots. This guard is intentionally not cloneable.
pub struct CompactionReservation {
    store: Arc<TableStore>,
    paths: Vec<String>,
}

impl CompactionReservation {
    pub fn paths(&self) -> &[String] {
        &self.paths
    }

    pub(crate) fn root(&self) -> &Path {
        self.store.root()
    }
}

impl Drop for CompactionReservation {
    fn drop(&mut self) {
        let mut uses = self.store.uses();
        for path in &self.paths {
            let remove = {
                let table_use = uses
                    .get_mut(path)
                    .expect("compaction releases an SST it previously reserved");
                debug_assert!(table_use.reserved_for_compaction);
                table_use.reserved_for_compaction = false;
                table_use.is_unused()
            };
            if remove {
                uses.remove(path);
            }
        }
    }
}

struct TablePinGuard {
    store: Arc<TableStore>,
    tables: Vec<String>,
}

impl Drop for TablePinGuard {
    fn drop(&mut self) {
        let mut uses = self.store.uses();
        for table in &self.tables {
            let remove = {
                let table_use = uses
                    .get_mut(table)
                    .expect("snapshot releases a table it previously pinned");
                table_use.snapshot_count -= 1;
                table_use.is_unused()
            };
            if remove {
                uses.remove(table);
            }
        }
    }
}

struct PartitionReaders {
    base: Vec<TableReader>,
    patches: Vec<TableReader>,
}

// Why pin a physical file set instead of remembering only a lsn number?
//
// Suppose the base says `x = 0`, a patch at lsn 10 says `x = 1`, and a snapshot is taken at
// lsn 5. A later compaction can fold the patch into an unversioned `x = 1` base. If the
// snapshot followed the current manifest, its lsn alone could no longer recover `x = 0`.
// Sequence-only engines such as RocksDB solve this by keeping per-key versions during compaction;
// this lets them delete old input files, but an old snapshot may read files created after it.
//
// This LSM chooses the smaller contract: a snapshot keeps the manifest shape and pins its files.
// Compaction may still publish new files immediately, but the captured view never searches them and
// obsolete inputs remain on disk until its snapshots finish. We trade temporary disk space for
// unversioned base values, predictable reads, and a much simpler merge contract.

/// A coherent, immutable read view over one manifest generation.
///
/// The caller must capture `manifest` and `max_lsn` atomically. Base values belong to that
/// captured manifest. Patch records with a lsn greater than `max_lsn` are ignored.
/// Creating the snapshot pins every SST in the manifest; dropping it releases those pins.
pub struct Snapshot {
    full_manifest: Arc<Manifest>,
    max_lsn: u64,
    readers: BTreeMap<u32, PartitionReaders>,
    _table_pin: TablePinGuard,
    _lsn_pin: Option<SnapshotPin>,
}

/// A pinned, sorted scan over one LSM partition.
pub struct LsmScan {
    base: Vec<TableCursor>,
    patches: Vec<TableCursor>,
    memory: VecDeque<(Vec<u8>, StrataLsn, Vec<u8>)>,
    max_lsn: StrataLsn,
    end: Option<Vec<u8>>,
    _snapshot: Arc<Snapshot>,
}

impl LsmScan {
    pub(crate) fn new(
        snapshot: Arc<Snapshot>,
        tables: &TableStore,
        partition: u32,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        memory: Vec<(Vec<u8>, StrataLsn, Vec<u8>)>,
        max_lsn: StrataLsn,
    ) -> Result<Self> {
        let partition_manifest =
            snapshot
                .full_manifest
                .partitions
                .get(&partition)
                .ok_or(Error::InvalidPartition {
                    partition,
                    partition_count: snapshot.full_manifest.partition_count,
                })?;
        let overlaps = |table: &&TableMeta| {
            start.is_none_or(|start| table.last_key.as_slice() >= start)
                && end.is_none_or(|end| table.first_key.as_slice() < end)
        };
        let base = partition_manifest
            .base
            .iter()
            .filter(overlaps)
            .map(|table| {
                TableReader::open_base(tables.root(), table, &snapshot.full_manifest.schema_id)?
                    .into_cursor_from(start)
            })
            .collect::<Result<Vec<_>>>()?;
        let patches = partition_manifest
            .patches
            .iter()
            .filter(overlaps)
            .filter(|table| table.min_lsn.is_none_or(|lsn| lsn <= max_lsn))
            .map(|table| {
                TableReader::open_patch(
                    tables.root(),
                    table,
                    &snapshot.full_manifest.patch_format_id,
                )?
                .into_cursor_from(start)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            base,
            patches,
            memory: memory.into(),
            max_lsn,
            end: end.map(<[u8]>::to_vec),
            _snapshot: snapshot,
        })
    }

    /// Returns the next materialized key/value pair in unsigned lexicographic key order.
    pub fn next(&mut self, merge: &dyn MergeOperator) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            let Some(key) = self
                .base
                .iter()
                .chain(&self.patches)
                .filter_map(TableCursor::current)
                .map(|row| row.key.as_slice())
                .chain(self.memory.front().map(|(key, _, _)| key.as_slice()))
                .min()
                .map(<[u8]>::to_vec)
            else {
                return Ok(None);
            };
            if self.end.as_deref().is_some_and(|end| key.as_slice() >= end) {
                return Ok(None);
            }

            let mut base_value = None;
            for cursor in &mut self.base {
                while cursor
                    .current()
                    .is_some_and(|row| row.key.as_slice() == key)
                {
                    if base_value.is_some() {
                        return Err(Error::InvalidManifest {
                            reason: "scan found multiple base values for one key".to_owned(),
                        });
                    }
                    base_value = Some(cursor.current().expect("checked current row").value.clone());
                    cursor.advance()?;
                }
            }

            let mut patches = Vec::new();
            for cursor in &mut self.patches {
                while cursor
                    .current()
                    .is_some_and(|row| row.key.as_slice() == key)
                {
                    let row = cursor.current().expect("checked current row");
                    let lsn = row.lsn.expect("patch cursor has lsned rows");
                    if lsn <= self.max_lsn {
                        patches.push((lsn, row.value.clone()));
                    }
                    cursor.advance()?;
                }
            }
            while self
                .memory
                .front()
                .is_some_and(|(memory_key, _, _)| memory_key == &key)
            {
                let (_, lsn, value) = self.memory.pop_front().expect("checked memory row");
                patches.push((lsn, value));
            }

            if patches.is_empty() {
                if let Some(value) = base_value {
                    return Ok(Some((key, value)));
                }
                continue;
            }
            patches.sort_unstable_by_key(|(lsn, _)| *lsn);
            if let Some(lsn) = patches
                .windows(2)
                .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
            {
                return Err(Error::InvalidManifest {
                    reason: format!("scan found duplicate patch lsn {lsn:?} for one key"),
                });
            }
            let patch_refs = patches
                .iter()
                .map(|(lsn, value)| (*lsn, value.as_slice()))
                .collect::<Vec<_>>();
            let mut discard = |_| Ok(());
            if let Some(value) =
                merge.merge(&key, base_value.as_deref(), &patch_refs, &mut discard)?
            {
                return Ok(Some((key, value)));
            }
        }
    }
}

impl Snapshot {
    pub fn new(tables: Arc<TableStore>, manifest: Arc<Manifest>, max_lsn: u64) -> Result<Self> {
        Self::open(tables, manifest, max_lsn, true)
    }

    pub(crate) fn new_current(
        tables: Arc<TableStore>,
        manifest: Arc<Manifest>,
        max_lsn: u64,
    ) -> Result<Self> {
        Self::open(tables, manifest, max_lsn, false)
    }

    fn open(
        tables: Arc<TableStore>,
        manifest: Arc<Manifest>,
        max_lsn: u64,
        track_lsn: bool,
    ) -> Result<Self> {
        manifest.validate()?;
        let lsn_pin = track_lsn.then(|| tables.live_snapshots.pin(max_lsn));

        // Pin before opening so obsolete-file cleanup cannot remove a referenced table between the
        // two operations. The manifest owner must still make capture and publication coherent.
        let table_pin = tables.pin(
            manifest
                .partitions
                .values()
                .flat_map(|partition| partition.base.iter().chain(&partition.patches))
                .map(|table| table.relative_path.clone()),
        );

        let mut readers = BTreeMap::new();
        for (&partition, partition_tables) in &manifest.partitions {
            let base = partition_tables
                .base
                .iter()
                .map(|table| {
                    TableReader::open_base_cached(
                        tables.root(),
                        table,
                        &manifest.schema_id,
                        Arc::clone(&tables.block_cache),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let patches = partition_tables
                .patches
                .iter()
                .map(|table| {
                    TableReader::open_patch_cached(
                        tables.root(),
                        table,
                        &manifest.patch_format_id,
                        Arc::clone(&tables.block_cache),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            readers.insert(partition, PartitionReaders { base, patches });
        }

        Ok(Self {
            full_manifest: manifest,
            max_lsn,
            readers,
            _table_pin: table_pin,
            _lsn_pin: lsn_pin,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.full_manifest
    }

    pub fn max_lsn(&self) -> u64 {
        self.max_lsn
    }

    /// Resolves one key from its base value and every visible patch in lsn order.
    ///
    /// Partitioning remains the caller's responsibility. Garbage records from the merge operator
    /// are intentionally discarded on reads.
    pub fn get(
        &self,
        partition: u32,
        key: &[u8],
        merge: &dyn MergeOperator,
    ) -> Result<Option<Vec<u8>>> {
        let (base, patches) = self.get_parts(partition, key)?;
        if patches.is_empty() {
            return Ok(base);
        }
        let patch_refs: Vec<_> = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect();
        let mut discard_event = |_| Ok(());
        merge.merge(key, base.as_deref(), &patch_refs, &mut discard_event)
    }

    pub(crate) fn get_parts(&self, partition: u32, key: &[u8]) -> Result<ReadParts> {
        let partition_manifest =
            self.full_manifest
                .partitions
                .get(&partition)
                .ok_or(Error::InvalidPartition {
                    partition,
                    partition_count: self.full_manifest.partition_count,
                })?;
        let readers = self
            .readers
            .get(&partition)
            .expect("validated manifest has readers for every partition");

        let base_index = partition_manifest
            .base
            .partition_point(|table| table.last_key.as_slice() < key);
        let base = match partition_manifest.base.get(base_index) {
            Some(table) if table.first_key.as_slice() <= key => {
                readers.base[base_index].get(key)?
            }
            _ => None,
        };

        let mut patches = Vec::new();
        for (table, reader) in partition_manifest.patches.iter().zip(&readers.patches) {
            if key < table.first_key.as_slice()
                || key > table.last_key.as_slice()
                || table.min_lsn.is_some_and(|lsn| lsn > self.max_lsn)
            {
                continue;
            }
            patches.extend(
                reader
                    .get_patches(key)?
                    .into_iter()
                    .filter(|(lsn, _)| *lsn <= self.max_lsn),
            );
        }

        if patches.is_empty() {
            return Ok((base, patches));
        }

        patches.sort_unstable_by_key(|(lsn, _)| *lsn);
        if let Some(lsn) = patches
            .windows(2)
            .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
        {
            return Err(Error::InvalidManifest {
                reason: format!(
                    "partition {partition} has duplicate patch lsn {lsn:?} for one key"
                ),
            });
        }
        Ok((base, patches))
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, sync::Arc};

    use tempfile::TempDir;

    use super::*;
    use crate::{PartitionManifest, StrataLsn, TableWriter};

    struct Append;

    impl MergeOperator for Append {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            patches: &[(StrataLsn, &[u8])],
            _emit: &mut dyn FnMut(crate::GarbageRecord) -> Result<()>,
        ) -> Result<Option<Vec<u8>>> {
            let mut value = base.unwrap_or_default().to_vec();
            for (_, patch) in patches {
                value.extend_from_slice(patch);
            }
            Ok(Some(value))
        }
    }

    fn manifest(base: Vec<TableMeta>, patches: Vec<TableMeta>) -> Arc<Manifest> {
        let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
        manifest.generation = 7;
        manifest
            .partitions
            .insert(0, PartitionManifest { base, patches });
        Arc::new(manifest)
    }

    fn write_base(directory: &TempDir) -> TableMeta {
        let mut writer =
            TableWriter::create_base(directory.path(), "base.sst", 1, 0, "base-v1").unwrap();
        writer.add(b"a", b"base").unwrap();
        writer.add(b"c", b"alone").unwrap();
        writer.finish().unwrap()
    }

    fn write_patch(
        directory: &TempDir,
        path: &str,
        id: u64,
        lsn: StrataLsn,
        value: &[u8],
    ) -> TableMeta {
        let mut writer =
            TableWriter::create_patch(directory.path(), path, id, 0, "patch-v1").unwrap();
        writer.add_patch(b"a", lsn, value).unwrap();
        writer.finish().unwrap()
    }

    #[test]
    fn point_read_merges_only_visible_patches_in_lsn_order() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let early = write_patch(&directory, "early.sst", 2, 10, b"-early");
        let late = write_patch(&directory, "late.sst", 3, 20, b"-late");
        let files = Arc::new(TableStore::new(directory.path()));
        // Reverse manifest order to establish that lsns, not file order, define history.
        let manifest = manifest(vec![base], vec![late, early]);

        let at_15 = Snapshot::new(Arc::clone(&files), Arc::clone(&manifest), 15).unwrap();
        let at_25 = Snapshot::new(files, manifest, 25).unwrap();

        assert_eq!(
            at_15.get(0, b"a", &Append).unwrap(),
            Some(b"base-early".to_vec())
        );
        assert_eq!(
            at_25.get(0, b"a", &Append).unwrap(),
            Some(b"base-early-late".to_vec())
        );
        assert_eq!(
            at_15.get(0, b"c", &Append).unwrap(),
            Some(b"alone".to_vec())
        );
        assert_eq!(at_15.get(0, b"missing", &Append).unwrap(), None);
    }

    #[test]
    fn point_reads_share_verified_data_blocks() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let files = Arc::new(TableStore::with_block_cache_capacity(
            directory.path(),
            1024 * 1024,
        ));
        let snapshot =
            Snapshot::new(Arc::clone(&files), manifest(vec![base], Vec::new()), 0).unwrap();

        assert_eq!(
            snapshot.get(0, b"a", &Append).unwrap(),
            Some(b"base".to_vec())
        );
        assert_eq!(
            snapshot.get(0, b"c", &Append).unwrap(),
            Some(b"alone".to_vec())
        );

        let stats = files.block_cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.insertions, 1);
        assert_eq!(stats.entries, 1);
        assert!(stats.bytes > 0);
    }

    #[test]
    fn duplicate_patch_lsns_across_files_are_rejected_on_read() {
        let directory = TempDir::new().unwrap();
        let first = write_patch(&directory, "first.sst", 1, 10, b"first");
        let second = write_patch(&directory, "second.sst", 2, 10, b"second");
        let files = Arc::new(TableStore::new(directory.path()));
        let snapshot = Snapshot::new(files, manifest(Vec::new(), vec![first, second]), 10).unwrap();

        assert!(matches!(
            snapshot.get(0, b"a", &Append),
            Err(Error::InvalidManifest { .. })
        ));
    }

    #[test]
    fn snapshot_pins_every_referenced_sst_until_drop() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let patch = write_patch(&directory, "patch.sst", 2, 10, b"patch");
        let files = Arc::new(TableStore::new(directory.path()));
        let first_snapshot = Snapshot::new(
            Arc::clone(&files),
            manifest(vec![base.clone()], vec![patch.clone()]),
            10,
        )
        .unwrap();
        let second_snapshot = Snapshot::new(
            Arc::clone(&files),
            manifest(vec![base.clone()], vec![patch.clone()]),
            10,
        )
        .unwrap();

        assert!(files.is_pinned(&base));
        assert!(files.is_pinned(&patch));
        assert!(!files.remove_if_unpinned(&base).unwrap());
        assert!(directory.path().join(&base.relative_path).exists());

        drop(first_snapshot);
        assert!(files.is_pinned(&base));
        assert!(!files.remove_if_unpinned(&base).unwrap());

        drop(second_snapshot);
        assert!(!files.is_pinned(&base));
        assert!(!files.is_pinned(&patch));
        assert!(files.remove_if_unpinned(&base).unwrap());
        assert!(!directory.path().join(&base.relative_path).exists());
    }

    #[test]
    fn snapshot_lsns_are_reference_counted() {
        let directory = TempDir::new().unwrap();
        let files = Arc::new(TableStore::new(directory.path()));
        let live = files.live_snapshots();

        let at_10 =
            Snapshot::new(Arc::clone(&files), manifest(Vec::new(), Vec::new()), 10).unwrap();
        let another_at_10 =
            Snapshot::new(Arc::clone(&files), manifest(Vec::new(), Vec::new()), 10).unwrap();
        let at_20 = Snapshot::new(files, manifest(Vec::new(), Vec::new()), 20).unwrap();

        assert!(live.protects(20));
        assert!(!live.protects(21));
        drop(at_20);
        assert!(live.protects(10));
        assert!(!live.protects(11));
        drop(at_10);
        assert!(live.protects(10));
        drop(another_at_10);
        assert!(!live.protects(0));
    }

    #[test]
    fn replaceable_current_view_does_not_hold_the_snapshot_fence() {
        let directory = TempDir::new().unwrap();
        let files = Arc::new(TableStore::new(directory.path()));
        let live = files.live_snapshots();
        let current = Snapshot::new_current(files, manifest(Vec::new(), Vec::new()), 20).unwrap();

        assert!(!live.protects(0));
        drop(current);
    }

    #[test]
    fn failed_snapshot_open_releases_its_pins() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        fs::remove_file(directory.path().join(&base.relative_path)).unwrap();
        let files = Arc::new(TableStore::new(directory.path()));

        assert!(
            Snapshot::new(
                Arc::clone(&files),
                manifest(vec![base.clone()], Vec::new()),
                0
            )
            .is_err()
        );
        assert!(!files.is_pinned(&base));
    }

    #[test]
    fn invalid_partition_and_overlapping_base_ranges_are_rejected() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let files = Arc::new(TableStore::new(directory.path()));
        let snapshot = Snapshot::new(
            Arc::clone(&files),
            manifest(vec![base.clone()], Vec::new()),
            0,
        )
        .unwrap();
        assert!(matches!(
            snapshot.get(1, b"a", &Append),
            Err(Error::InvalidPartition { .. })
        ));
        drop(snapshot);

        let mut duplicate = base.clone();
        duplicate.relative_path = "other.sst".to_owned();
        assert!(matches!(
            Snapshot::new(files, manifest(vec![base, duplicate], Vec::new()), 0),
            Err(Error::InvalidManifest { .. })
        ));
    }

    #[test]
    fn compaction_reservations_are_exclusive_and_atomic() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let first_patch = write_patch(&directory, "first-patch.sst", 2, 10, b"first");
        let second_patch = write_patch(&directory, "second-patch.sst", 3, 20, b"second");
        let files = Arc::new(TableStore::new(directory.path()));

        let first = files
            .reserve_for_compaction(&[base.clone(), first_patch.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(first.paths(), &["base.sst", "first-patch.sst"]);

        // The overlap rejects the complete request; second-patch.sst remains available.
        assert!(
            files
                .reserve_for_compaction(&[first_patch.clone(), second_patch.clone()])
                .unwrap()
                .is_none()
        );
        let disjoint = files
            .reserve_for_compaction(std::slice::from_ref(&second_patch))
            .unwrap()
            .unwrap();

        drop(first);
        assert!(
            files
                .reserve_for_compaction(std::slice::from_ref(&base))
                .unwrap()
                .is_some()
        );
        drop(disjoint);
    }

    #[test]
    fn snapshots_and_compactions_pin_the_same_file_independently() {
        let directory = TempDir::new().unwrap();
        let base = write_base(&directory);
        let files = Arc::new(TableStore::new(directory.path()));
        let reservation = files
            .reserve_for_compaction(std::slice::from_ref(&base))
            .unwrap()
            .unwrap();
        let snapshot = Snapshot::new(
            Arc::clone(&files),
            manifest(vec![base.clone()], Vec::new()),
            0,
        )
        .unwrap();

        assert_eq!(
            snapshot.get(0, b"a", &Append).unwrap(),
            Some(b"base".to_vec())
        );
        assert!(!files.remove_if_unpinned(&base).unwrap());

        drop(reservation);
        assert!(files.is_pinned(&base));
        assert!(!files.remove_if_unpinned(&base).unwrap());

        drop(snapshot);
        assert!(!files.is_pinned(&base));
        assert!(files.remove_if_unpinned(&base).unwrap());
    }
}
