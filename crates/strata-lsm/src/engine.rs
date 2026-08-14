use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs, mem,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use strata_core::RecordRef;

use crate::{
    BlockCacheStats, DEFAULT_BLOCK_CACHE_BYTES, Error, FrozenMemtable, LsmIter, Manifest,
    ManifestEdit, Memtable, MemtableRolloverPolicy, MergeOperator, Result, Snapshot, StrataLsn,
    TableMeta, TableStore, memtable::VERSION_BYTES, table::sync_parent,
};

const RECORD_REF_BYTES: usize = 3 * mem::size_of::<u64>();
const VALUE_INLINE: u8 = 0;
const VALUE_BLOB: u8 = 1;

/// In-memory generation settings for each partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmOptions {
    pub memtable_capacity: usize,
    pub rollover_policy: Option<MemtableRolloverPolicy>,
    pub first_memtable_generation: u64,
    pub max_frozen_generations: NonZeroUsize,
    pub block_cache_capacity_bytes: usize,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_capacity: crate::DEFAULT_MEMTABLE_BUFFER_BYTES,
            rollover_policy: None,
            first_memtable_generation: 1,
            max_frozen_generations: NonZeroUsize::MIN,
            block_cache_capacity_bytes: DEFAULT_BLOCK_CACHE_BYTES,
        }
    }
}

/// One keyed LSM write at an LSN assigned by the caller.
///
/// The LSM deliberately knows nothing about the store WAL or payload segment. For example, a blob
/// write follows this order at the store boundary:
///
/// 1. append the payload to the active segment and obtain its [`RecordRef`];
/// 2. append `PutBlob { record_ref, .. }` to the store WAL at LSN 42;
/// 3. call `lsm.write(42, mutation)` to make the key visible.
///
/// Epoch changes and shard drops never become `Mutation`s because they have no LSM key; the store
/// WAL routes those directly to RocksDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Put {
        partition: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutPrefix {
        partition: u32,
        key_prefix: Vec<u8>,
        key_suffix: Vec<u8>,
        value: Vec<u8>,
    },
    PutBlob {
        partition: u32,
        key: Vec<u8>,
        metadata: Vec<u8>,
        record_ref: RecordRef,
    },
    PutBlobPrefix {
        partition: u32,
        key_prefix: Vec<u8>,
        key_suffix: Vec<u8>,
        metadata: Vec<u8>,
        record_ref: RecordRef,
    },
}

/// Memtable generation made immutable by one write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RolledMemtable {
    pub partition: u32,
    pub generation: u64,
}

/// One allocator-owned immutable-table identity and its canonical relative path.
///
/// Keeping these together prevents callers from reserving an ID and independently constructing a
/// different or duplicate filesystem path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTarget {
    kind: TableTargetKind,
    id: u64,
    relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableTargetKind {
    Base,
    Patch,
}

impl TableTarget {
    /// Creates the canonical target for a base table ID.
    pub fn base(id: u64) -> Self {
        Self {
            kind: TableTargetKind::Base,
            id,
            relative_path: format!("base-{id:020}.sst"),
        }
    }

    /// Creates the canonical target for a patch table ID.
    pub fn patch(id: u64) -> Self {
        Self {
            kind: TableTargetKind::Patch,
            id,
            relative_path: format!("patch-{id:020}.sst"),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn into_base_parts(self) -> Result<(u64, String)> {
        self.into_parts(TableTargetKind::Base)
    }

    pub fn into_patch_parts(self) -> Result<(u64, String)> {
        self.into_parts(TableTargetKind::Patch)
    }

    fn into_parts(self, expected: TableTargetKind) -> Result<(u64, String)> {
        if self.kind != expected {
            return Err(Error::InvalidTable(format!(
                "table target kind mismatch: expected {expected:?}, got {:?}",
                self.kind
            )));
        }
        Ok((self.id, self.relative_path))
    }
}

/// Result of one visible logical write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub lsn: StrataLsn,
    pub rolled_memtable: Option<RolledMemtable>,
}

/// Result of one ordered logical batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatchResult {
    pub lsns: Vec<StrataLsn>,
    pub rolled_memtables: Vec<RolledMemtable>,
}

struct PartitionState {
    active: Memtable,
    frozen: VecDeque<Arc<FrozenMemtable>>,
}

struct MemoryState {
    manifest: Arc<Manifest>,
    /// Synced SSTs used by readers but not yet safe to put in the durable manifest.
    /// Pending patches are flushed to disk but becuase they are not a part of Manifest
    /// they are not persisted in RocksDB index which means upon restart, crash recovery
    /// is unaware of them and would remove them as orphan files.
    pending_patches: Vec<TableMeta>,
    snapshot: Arc<Snapshot>,
    partitions: BTreeMap<u32, PartitionState>,
    last_visible_lsn: Option<StrataLsn>,
}

struct WriteState {
    partition_count: u32,
    memtable_capacity: usize,
    last_lsn: Option<StrataLsn>,
}

struct PendingBatch {
    mutations: Vec<Mutation>,
    lsns: Vec<StrataLsn>,
}

/// An unlogged LSM: memtables, immutable tables, and merge/compaction machinery.
///
/// The caller owns sequencing and recovery. `from_parts` accepts decoded store-WAL mutations that
/// are newer than the durable tables; normal writes use the same caller-assigned LSN API.
pub struct Lsm {
    tables: Arc<TableStore>,
    writes: Mutex<WriteState>,
    memory: Mutex<MemoryState>,
    memory_changed: Condvar,
    max_frozen_generations: usize,
    halted: Mutex<Option<String>>,
    flush_lock: Mutex<()>,
    next_table_id: AtomicU64,
}

impl Lsm {
    pub fn from_parts(
        table_root: impl Into<PathBuf>,
        manifest: Arc<Manifest>,
        recovered: Vec<(StrataLsn, Mutation)>,
        last_lsn: Option<StrataLsn>,
        options: LsmOptions,
    ) -> Result<Self> {
        let table_root = table_root.into();
        fs::create_dir_all(&table_root).map_err(|source| Error::Io {
            path: table_root.clone(),
            source,
        })?;
        manifest.validate()?;
        remove_orphan_tables(&table_root, &manifest)?;
        let materialized_through = manifest
            .partitions
            .iter()
            .map(|(&id, partition)| {
                (
                    id,
                    partition
                        .patches
                        .iter()
                        .filter_map(|table| table.max_lsn)
                        .max(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let manifest_lsn = materialized_through
            .values()
            .copied()
            .flatten()
            .chain(manifest.materialized_through)
            .max();
        if let (Some(last), Some(table)) = (last_lsn, manifest_lsn)
            && last < table
        {
            return Err(Error::InvalidManifest {
                reason: format!("last recovered lsn {last:?} precedes manifest lsn {table:?}"),
            });
        }
        let last_lsn = last_lsn.or(manifest_lsn);
        let tables = Arc::new(TableStore::with_block_cache_capacity(
            table_root,
            options.block_cache_capacity_bytes,
        ));
        let snapshot = Arc::new(Snapshot::new_current(
            Arc::clone(&tables),
            Arc::clone(&manifest),
            last_lsn.unwrap_or_default(),
        )?);
        let mut partitions = (0..manifest.partition_count)
            .map(|partition| {
                let active = match options.rollover_policy {
                    Some(policy) => Memtable::with_rollover_policy(
                        options.first_memtable_generation,
                        options.memtable_capacity,
                        policy,
                    ),
                    None => {
                        Memtable::new(options.first_memtable_generation, options.memtable_capacity)
                    }
                };
                (
                    partition,
                    PartitionState {
                        active,
                        frozen: VecDeque::new(),
                    },
                )
            })
            .collect();
        replay_mutations(
            recovered,
            manifest.materialized_through,
            &materialized_through,
            &mut partitions,
        )?;

        let next_table_id = manifest.next_table_id;
        Ok(Self {
            tables,
            writes: Mutex::new(WriteState {
                partition_count: manifest.partition_count,
                memtable_capacity: options.memtable_capacity,
                last_lsn,
            }),
            memory: Mutex::new(MemoryState {
                manifest,
                pending_patches: Vec::new(),
                snapshot,
                partitions,
                last_visible_lsn: last_lsn,
            }),
            memory_changed: Condvar::new(),
            max_frozen_generations: options.max_frozen_generations.get(),
            halted: Mutex::new(None),
            flush_lock: Mutex::new(()),
            next_table_id: AtomicU64::new(next_table_id),
        })
    }

    /// Applies one caller-sequenced mutation to the memtable.
    pub fn write(&self, lsn: StrataLsn, mutation: Mutation) -> Result<WriteResult> {
        let result = self.write_batch(vec![(lsn, mutation)])?;
        Ok(WriteResult {
            lsn: result.lsns[0],
            rolled_memtable: result.rolled_memtables.into_iter().next(),
        })
    }

    /// Stores a value in the memtable at a caller-assigned LSN.
    pub fn put(
        &self,
        lsn: StrataLsn,
        partition: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<WriteResult> {
        self.write(
            lsn,
            Mutation::Put {
                partition,
                key,
                value,
            },
        )
    }

    /// Stores a caller-created payload reference in the memtable.
    pub fn put_blob(
        &self,
        lsn: StrataLsn,
        partition: u32,
        key: Vec<u8>,
        record_ref: RecordRef,
    ) -> Result<WriteResult> {
        self.write(
            lsn,
            Mutation::PutBlob {
                partition,
                key,
                metadata: Vec::new(),
                record_ref,
            },
        )
    }

    /// Applies one batch atomically to the visible memtable view.
    ///
    /// LSNs must be strictly increasing but need not be contiguous. Gaps represent store mutations
    /// routed elsewhere. For example, `[10, 13]` is valid when LSNs 11 and 12 are RocksDB-only.
    pub fn write_batch(&self, writes: Vec<(StrataLsn, Mutation)>) -> Result<WriteBatchResult> {
        if writes.is_empty() {
            self.check_running()?;
            return Ok(WriteBatchResult {
                lsns: Vec::new(),
                rolled_memtables: Vec::new(),
            });
        }

        let pending = {
            let mut state = lock(&self.writes);
            self.check_running()?;
            prepare_batch(&mut state, writes)?
        };
        self.apply_pending_batch(pending)
    }

    /// Resolves one key across the active memtable, frozen generations, and captured SSTs.
    /// Mutations newer than the last completely applied batch remain invisible in every source.
    pub fn get(
        &self,
        partition: u32,
        key: &[u8],
        merge: &dyn MergeOperator,
    ) -> Result<Option<Vec<u8>>> {
        let (snapshot, memory_patches, visible_lsn) = {
            self.check_running()?;
            let state = lock(&self.memory);
            let partition = state.partition(partition)?;
            let visible_lsn = state.last_visible_lsn.unwrap_or_default();
            let mut patches = Vec::new();
            for frozen in &partition.frozen {
                for entry in frozen.get_all(key) {
                    if entry.lsn <= visible_lsn {
                        patches.push((entry.lsn, entry.value.to_vec()));
                    }
                }
            }
            for entry in partition.active.get_all(key) {
                if entry.lsn <= visible_lsn {
                    patches.push((entry.lsn, entry.value.to_vec()));
                }
            }
            (Arc::clone(&state.snapshot), patches, visible_lsn)
        };

        let (base, mut patches) = snapshot.get_parts_through(partition, key, visible_lsn)?;
        patches.extend(memory_patches);
        if patches.is_empty() {
            return Ok(base);
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
        let patch_refs = patches
            .iter()
            .map(|(lsn, value)| (*lsn, value.as_slice()))
            .collect::<Vec<_>>();
        let mut discard_event = |_| Ok(());
        merge.merge(key, base.as_deref(), &patch_refs, &mut discard_event)
    }

    /// Captures a pinned, sorted iterator over `[start, end)` of one partition.
    ///
    /// The iterator must be created here rather than on [`Snapshot`] because a snapshot holds
    /// only flushed SSTs: rows at or below `max_lsn` that still live in the memtables are
    /// captured under the memory lock, atomically with the snapshot reference. The requested
    /// `max_lsn` is also capped at the last completely applied batch.
    pub fn iter(
        &self,
        partition: u32,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        max_lsn: StrataLsn,
    ) -> Result<LsmIter> {
        self.check_running()?;
        let (snapshot, frozen, active, visible_lsn) = {
            let state = lock(&self.memory);
            let partition_state = state.partition(partition)?;
            let visible_lsn = max_lsn.min(state.last_visible_lsn.unwrap_or_default());
            let in_range = |key: &[u8]| {
                start.is_none_or(|start| key >= start) && end.is_none_or(|end| key < end)
            };
            // Frozen generations are immutable, so the iterator pins them and reads their rows
            // in place. Only the still-mutable active memtable is copied, and only its visible
            // in-range rows, which also keeps this lock hold short.
            let frozen = partition_state
                .frozen
                .iter()
                .map(Arc::clone)
                .collect::<Vec<_>>();
            let mut active = Vec::new();
            for entry in partition_state.active.entries() {
                if entry.lsn <= visible_lsn && in_range(entry.key) {
                    active.push((entry.key.to_vec(), entry.lsn, entry.value.to_vec()));
                }
            }
            (Arc::clone(&state.snapshot), frozen, active, visible_lsn)
        };
        LsmIter::new(snapshot, partition, start, end, frozen, active, visible_lsn)
    }

    /// Last caller-assigned LSN applied to this LSM. This is a visibility value, not a durability
    /// frontier; durability belongs to the store WAL.
    pub fn last_lsn(&self) -> Result<Option<StrataLsn>> {
        self.check_running()?;
        Ok(lock(&self.writes).last_lsn)
    }

    pub fn manifest(&self) -> Arc<Manifest> {
        Arc::clone(&lock(&self.memory).manifest)
    }

    /// Allocates one patch-table identity and its canonical filesystem path.
    pub fn allocate_patch_target(&self) -> Result<TableTarget> {
        self.allocate_table_target(TableTargetKind::Patch)
    }

    /// Allocates one base-table identity and its canonical filesystem path.
    pub fn allocate_base_target(&self) -> Result<TableTarget> {
        self.allocate_table_target(TableTargetKind::Base)
    }

    fn allocate_table_target(&self, kind: TableTargetKind) -> Result<TableTarget> {
        let manifest_floor = self.manifest().next_table_id;
        let mut current = self.next_table_id.load(Ordering::Relaxed);
        loop {
            let id = current.max(manifest_floor);
            let next = id.checked_add(1).ok_or(Error::TableIdOverflow)?;
            match self.next_table_id.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(match kind {
                        TableTargetKind::Base => TableTarget::base(id),
                        TableTargetKind::Patch => TableTarget::patch(id),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Shared table store for compaction and obsolete-file cleanup.
    pub fn table_store(&self) -> Arc<TableStore> {
        Arc::clone(&self.tables)
    }

    pub fn block_cache_stats(&self) -> BlockCacheStats {
        self.tables.block_cache_stats()
    }

    /// Live caller-held snapshots. The engine's replaceable current view is not registered here.
    pub fn live_snapshots(&self) -> crate::LiveSnapshots {
        self.tables.live_snapshots()
    }

    pub fn frozen_generations(&self, partition: u32) -> Result<Vec<u64>> {
        self.check_running()?;
        Ok(lock(&self.memory)
            .partition(partition)?
            .frozen
            .iter()
            .map(|memtable| memtable.generation())
            .collect())
    }

    /// Rolls a non-empty active memtable when its configured age or key limit is due.
    ///
    /// A full frozen queue is left unchanged; the background flusher will drain it and retry on its
    /// next tick.
    pub fn roll_memtable_if_due(&self, partition: u32) -> Result<Option<RolledMemtable>> {
        self.check_running()?;
        let mut state = lock(&self.memory);
        let partition_state = state.partition_mut(partition)?;
        if !partition_state.active.rollover_due()
            || partition_state.frozen.len() >= self.max_frozen_generations
        {
            return Ok(None);
        }
        let rolled = roll_active(partition, partition_state)?;
        self.memory_changed.notify_all();
        Ok(Some(rolled))
    }

    /// Flushes the oldest frozen generation into a synced SST.
    ///
    /// The SST becomes readable immediately. It enters the durable manifest only when all of its
    /// rows are covered by `publish_through`; otherwise it stays pending until
    /// [`Self::publish_pending_through`] is called. Store-WAL reclamation is deliberately outside
    /// this method.
    pub fn flush_one(
        &self,
        partition: u32,
        target: TableTarget,
        publish_through: StrataLsn,
        publish: impl FnOnce(&ManifestEdit) -> Result<Manifest>,
    ) -> Result<Option<TableMeta>> {
        let _flush = lock(&self.flush_lock);
        let (frozen, manifest, pending_patches, max_lsn) = {
            self.check_running()?;
            let state = lock(&self.memory);
            let Some(frozen) = state.partition(partition)?.frozen.front() else {
                return Ok(None);
            };
            (
                Arc::clone(frozen),
                Arc::clone(&state.manifest),
                state.pending_patches.clone(),
                state.last_visible_lsn.unwrap_or_default(),
            )
        };
        let (id, relative_path) = target.into_patch_parts()?;
        let meta = frozen.flush(
            self.tables.root(),
            relative_path,
            id,
            partition,
            &manifest.patch_format_id,
        )?;
        let (mut ready, mut remaining) =
            split_publishable_pending(&pending_patches, publish_through);
        let older_partition_table_remains = remaining
            .iter()
            .any(|table| table.partition == meta.partition);
        if meta
            .max_lsn
            .is_some_and(|max_lsn| max_lsn <= publish_through)
            && !older_partition_table_remains
        {
            ready.push(meta.clone());
        } else {
            remaining.push(meta.clone());
        }
        let manifest = if ready.is_empty() {
            manifest
        } else {
            let published = publish(&add_patch_edit(ready.clone()))?;
            for table in &ready {
                require_manifest_patch(&published, table)?;
            }
            Arc::new(published)
        };
        let pending_patches = remaining;
        let snapshot = snapshot_with_pending_patches(
            Arc::clone(&self.tables),
            Arc::clone(&manifest),
            &pending_patches,
            max_lsn,
        )?;

        let mut state = lock(&self.memory);
        self.check_running()?;
        let partition_state = state.partition_mut(partition)?;
        let current = partition_state
            .frozen
            .front()
            .expect("serialized flush retains its frozen generation");
        if current.generation() != frozen.generation() {
            return Err(Error::InvalidManifest {
                reason: "frozen memtable changed during serialized flush".to_owned(),
            });
        }
        partition_state.frozen.pop_front();
        state.manifest = manifest;
        state.pending_patches = pending_patches;
        state.snapshot = snapshot;
        self.memory_changed.notify_all();
        Ok(Some(meta))
    }

    /// Publishes the covered prefix of pending SSTs in every partition.
    ///
    /// Different partitions may advance independently. Within one partition, publication stops at
    /// the first uncovered table so recovery can safely treat its highest published patch LSN as a
    /// contiguous replay frontier.
    pub fn publish_pending_through(
        &self,
        publish_through: StrataLsn,
        publish: impl FnOnce(&ManifestEdit) -> Result<Manifest>,
    ) -> Result<bool> {
        let _flush = lock(&self.flush_lock);
        self.check_running()?;
        let (ready, remaining, max_lsn) = {
            let state = lock(&self.memory);
            let (ready, remaining) =
                split_publishable_pending(&state.pending_patches, publish_through);
            (ready, remaining, state.last_visible_lsn.unwrap_or_default())
        };
        if ready.is_empty() {
            return Ok(false);
        }

        let published = publish(&add_patch_edit(ready.clone()))?;
        for table in &ready {
            require_manifest_patch(&published, table)?;
        }
        let published = Arc::new(published);
        let snapshot = snapshot_with_pending_patches(
            Arc::clone(&self.tables),
            Arc::clone(&published),
            &remaining,
            max_lsn,
        )?;

        let mut state = lock(&self.memory);
        self.check_running()?;
        for table in &ready {
            if !state
                .pending_patches
                .iter()
                .any(|pending| pending.relative_path == table.relative_path)
            {
                return Err(Error::InvalidManifest {
                    reason: format!("pending SST {} disappeared", table.relative_path),
                });
            }
        }
        state.manifest = published;
        state.pending_patches = remaining;
        state.snapshot = snapshot;
        Ok(true)
    }

    /// Advances the store-wide materialized frontier without inventing LSM metadata rows.
    ///
    /// The requested frontier is capped immediately before the first mutation not yet present in
    /// the durable manifest, whether it is in a memtable or a pending SST. Example: if the store
    /// has committed through LSN 20 and the oldest unpublished LSM row is LSN 17, this publishes at
    /// most 16. RocksDB-only LSNs in that range need no SST row.
    pub fn materialize_through(
        &self,
        requested: StrataLsn,
        publish: impl FnOnce(&ManifestEdit) -> Result<Manifest>,
    ) -> Result<Option<StrataLsn>> {
        let _flush = lock(&self.flush_lock);
        self.check_running()?;
        let (manifest, pending_patches, max_lsn, first_unflushed) = {
            let state = lock(&self.memory);
            (
                Arc::clone(&state.manifest),
                state.pending_patches.clone(),
                state.last_visible_lsn.unwrap_or_default(),
                state
                    .partitions
                    .values()
                    .flat_map(|partition| {
                        partition
                            .frozen
                            .iter()
                            .flat_map(|memtable| memtable.entries())
                            .chain(partition.active.entries())
                    })
                    .map(|entry| entry.lsn)
                    .chain(
                        state
                            .pending_patches
                            .iter()
                            .filter_map(|table| table.min_lsn),
                    )
                    .min(),
            )
        };
        let materialized_through = first_unflushed
            .map(|lsn| requested.min(lsn.saturating_sub(1)))
            .unwrap_or(requested);
        if manifest
            .materialized_through
            .is_some_and(|published| published >= materialized_through)
        {
            return Ok(None);
        }
        let edit = ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: Vec::new(),
            materialized_through: Some(materialized_through),
            wal_retained_from: None,
        };
        let published = publish(&edit)?;
        published.validate()?;
        if published
            .materialized_through
            .is_none_or(|frontier| frontier < materialized_through)
        {
            return Err(Error::InvalidManifest {
                reason: "published manifest did not advance its materialized frontier".to_owned(),
            });
        }

        let published = Arc::new(published);
        let snapshot = snapshot_with_pending_patches(
            Arc::clone(&self.tables),
            Arc::clone(&published),
            &pending_patches,
            max_lsn,
        )?;
        let mut state = lock(&self.memory);
        self.check_running()?;
        state.manifest = published;
        state.snapshot = snapshot;
        Ok(Some(materialized_through))
    }

    /// Installs a separately published manifest, for example after compaction.
    pub fn install_manifest(&self, manifest: Manifest) -> Result<()> {
        let _flush = lock(&self.flush_lock);
        manifest.validate()?;
        let manifest_lsn = manifest
            .partitions
            .values()
            .flat_map(|partition| partition.base.iter().chain(&partition.patches))
            .filter_map(|table| table.max_lsn)
            .chain(manifest.materialized_through)
            .max();
        let manifest = Arc::new(manifest);
        self.check_running()?;
        if let Some(manifest_lsn) = manifest_lsn {
            let mut writes = lock(&self.writes);
            writes.last_lsn = Some(
                writes
                    .last_lsn
                    .map_or(manifest_lsn, |current| current.max(manifest_lsn)),
            );
        }
        let mut state = lock(&self.memory);
        if let Some(manifest_lsn) = manifest_lsn {
            state.last_visible_lsn = Some(
                state
                    .last_visible_lsn
                    .map_or(manifest_lsn, |current| current.max(manifest_lsn)),
            );
        }
        let snapshot = snapshot_with_pending_patches(
            Arc::clone(&self.tables),
            Arc::clone(&manifest),
            &state.pending_patches,
            state.last_visible_lsn.unwrap_or_default(),
        )?;
        state.manifest = manifest;
        state.snapshot = snapshot;
        Ok(())
    }

    fn apply_pending_batch(&self, pending: PendingBatch) -> Result<WriteBatchResult> {
        let PendingBatch { mutations, lsns } = pending;
        let result = (|| {
            let mut state = lock(&self.memory);
            self.check_running()?;

            let mut rolled = Vec::new();
            for (index, mutation) in mutations.into_iter().enumerate() {
                let lsn = lsns[index];
                match mutation {
                    Mutation::Put {
                        partition,
                        mut key,
                        value,
                    } => {
                        let mut value = encode_inline_value_owned(value);
                        state = self.insert_memtable(state, partition, &mut rolled, |active| {
                            active.insert_active_owned(&mut key, lsn, &mut value)
                        })?;
                    }
                    Mutation::PutPrefix {
                        partition,
                        mut key_prefix,
                        key_suffix,
                        value,
                    } => {
                        let key_prefix_len = key_prefix.len();
                        key_prefix.reserve(key_suffix.len());
                        key_prefix.extend_from_slice(&key_suffix);
                        let mut value = encode_inline_value_owned(value);
                        state = self.insert_memtable(state, partition, &mut rolled, |active| {
                            active.insert_prefix_active_owned(
                                &mut key_prefix,
                                key_prefix_len,
                                lsn,
                                &mut value,
                            )
                        })?;
                    }
                    Mutation::PutBlob {
                        partition,
                        mut key,
                        metadata,
                        record_ref,
                    } => {
                        let mut value = encode_blob_value_owned(metadata, record_ref);
                        state = self.insert_memtable(state, partition, &mut rolled, |active| {
                            active.insert_active_owned(&mut key, lsn, &mut value)
                        })?;
                    }
                    Mutation::PutBlobPrefix {
                        partition,
                        mut key_prefix,
                        key_suffix,
                        metadata,
                        record_ref,
                    } => {
                        let key_prefix_len = key_prefix.len();
                        key_prefix.reserve(key_suffix.len());
                        key_prefix.extend_from_slice(&key_suffix);
                        let mut value = encode_blob_value_owned(metadata, record_ref);
                        state = self.insert_memtable(state, partition, &mut rolled, |active| {
                            active.insert_prefix_active_owned(
                                &mut key_prefix,
                                key_prefix_len,
                                lsn,
                                &mut value,
                            )
                        })?;
                    }
                }
            }
            state.last_visible_lsn = lsns.last().copied();
            self.memory_changed.notify_all();
            Ok(WriteBatchResult {
                lsns,
                rolled_memtables: rolled,
            })
        })();

        if let Err(error) = &result {
            self.halt(format!("memtable apply failed: {error}"));
        }
        result
    }

    fn insert_memtable<'a>(
        &self,
        mut state: MutexGuard<'a, MemoryState>,
        partition: u32,
        rolled: &mut Vec<RolledMemtable>,
        mut insert: impl FnMut(&mut Memtable) -> Result<()>,
    ) -> Result<MutexGuard<'a, MemoryState>> {
        loop {
            let mut inserted = false;
            let wait_for_flush = {
                let partition_state = state.partition_mut(partition)?;
                if partition_state.active.rollover_due() {
                    if partition_state.frozen.len() >= self.max_frozen_generations {
                        true
                    } else {
                        rolled.push(roll_active(partition, partition_state)?);
                        false
                    }
                } else {
                    match insert(&mut partition_state.active) {
                        Ok(()) => {
                            inserted = true;
                            false
                        }
                        Err(Error::MemtableFull { .. }) if !partition_state.active.is_empty() => {
                            if partition_state.frozen.len() >= self.max_frozen_generations {
                                true
                            } else {
                                rolled.push(roll_active(partition, partition_state)?);
                                false
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            if inserted {
                return Ok(state);
            }
            if wait_for_flush {
                self.check_running()?;
                state = wait(&self.memory_changed, state);
            }
        }
    }
    fn check_running(&self) -> Result<()> {
        match &*lock(&self.halted) {
            Some(reason) => Err(Error::LsmHalted {
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }

    /// Stops writes and wakes callers blocked on pipeline or memtable backpressure.
    pub fn halt(&self, reason: String) {
        lock(&self.halted).get_or_insert(reason);
        self.memory_changed.notify_all();
    }
}

fn add_patch_edit(tables: Vec<TableMeta>) -> ManifestEdit {
    ManifestEdit {
        remove: Vec::new(),
        add_base: Vec::new(),
        add_patches: tables,
        materialized_through: None,
        wal_retained_from: None,
    }
}

/// Splits pending patch SSTs into publishable per-partition prefixes.
///
/// Pending tables are appended in serialized flush order. Once one table in a partition is not
/// covered by `publish_through`, every later table in that partition must remain pending too. This
/// keeps the maximum published patch LSN for each partition a true contiguous replay frontier.
fn split_publishable_pending(
    pending: &[TableMeta],
    publish_through: StrataLsn,
) -> (Vec<TableMeta>, Vec<TableMeta>) {
    let mut blocked_partitions = HashSet::new();
    let mut ready = Vec::new();
    let mut remaining = Vec::new();
    for table in pending {
        let covered = table
            .max_lsn
            .is_some_and(|max_lsn| max_lsn <= publish_through);
        if blocked_partitions.contains(&table.partition) || !covered {
            blocked_partitions.insert(table.partition);
            remaining.push(table.clone());
        } else {
            ready.push(table.clone());
        }
    }
    (ready, remaining)
}

fn require_manifest_patch(manifest: &Manifest, table: &TableMeta) -> Result<()> {
    let visible = manifest
        .partitions
        .get(&table.partition)
        .is_some_and(|partition| {
            partition
                .patches
                .iter()
                .any(|patch| patch.relative_path == table.relative_path)
        });
    if visible {
        Ok(())
    } else {
        Err(Error::InvalidManifest {
            reason: format!(
                "published manifest does not contain flushed SST {}",
                table.relative_path
            ),
        })
    }
}

fn snapshot_with_pending_patches(
    tables: Arc<TableStore>,
    manifest: Arc<Manifest>,
    pending_patches: &[TableMeta],
    max_lsn: StrataLsn,
) -> Result<Arc<Snapshot>> {
    let visible_manifest = if pending_patches.is_empty() {
        manifest
    } else {
        let mut visible = (*manifest).clone();
        visible.apply(&add_patch_edit(pending_patches.to_vec()))?;
        Arc::new(visible)
    };
    Ok(Arc::new(Snapshot::new_current(
        tables,
        visible_manifest,
        max_lsn,
    )?))
}

impl MemoryState {
    fn partition(&self, partition: u32) -> Result<&PartitionState> {
        self.partitions
            .get(&partition)
            .ok_or(Error::InvalidPartition {
                partition,
                partition_count: self.manifest.partition_count,
            })
    }

    fn partition_mut(&mut self, partition: u32) -> Result<&mut PartitionState> {
        self.partitions
            .get_mut(&partition)
            .ok_or(Error::InvalidPartition {
                partition,
                partition_count: self.manifest.partition_count,
            })
    }
}

fn roll_active(partition: u32, state: &mut PartitionState) -> Result<RolledMemtable> {
    let next_generation =
        state
            .active
            .generation()
            .checked_add(1)
            .ok_or(Error::MemtableGenerationOverflow {
                generation: state.active.generation(),
            })?;
    let frozen = state.active.rollover(next_generation)?;
    let rolled = RolledMemtable {
        partition,
        generation: frozen.generation(),
    };
    state.frozen.push_back(Arc::new(frozen));
    Ok(rolled)
}

fn replay_mutations(
    mutations: Vec<(StrataLsn, Mutation)>,
    manifest_frontier: Option<StrataLsn>,
    materialized_through: &BTreeMap<u32, Option<StrataLsn>>,
    partitions: &mut BTreeMap<u32, PartitionState>,
) -> Result<()> {
    for (lsn, mutation) in mutations {
        match mutation {
            Mutation::Put {
                partition,
                key,
                value,
            } => {
                if is_materialized(manifest_frontier, materialized_through, partition, lsn) {
                    continue;
                }
                replay_plain(
                    partitions,
                    partition,
                    &key,
                    lsn,
                    &encode_inline_value(&value),
                )?;
            }
            Mutation::PutPrefix {
                partition,
                key_prefix,
                key_suffix,
                value,
            } => {
                if is_materialized(manifest_frontier, materialized_through, partition, lsn) {
                    continue;
                }
                replay_prefix(
                    partitions,
                    partition,
                    &key_prefix,
                    &key_suffix,
                    lsn,
                    &encode_inline_value(&value),
                )?;
            }
            Mutation::PutBlob {
                partition,
                key,
                metadata,
                record_ref,
            } => {
                if is_materialized(manifest_frontier, materialized_through, partition, lsn) {
                    continue;
                }
                replay_plain(
                    partitions,
                    partition,
                    &key,
                    lsn,
                    &encode_blob_value(&metadata, record_ref),
                )?;
            }
            Mutation::PutBlobPrefix {
                partition,
                key_prefix,
                key_suffix,
                metadata,
                record_ref,
            } => {
                if is_materialized(manifest_frontier, materialized_through, partition, lsn) {
                    continue;
                }
                replay_prefix(
                    partitions,
                    partition,
                    &key_prefix,
                    &key_suffix,
                    lsn,
                    &encode_blob_value(&metadata, record_ref),
                )?;
            }
        }
    }
    Ok(())
}

fn is_materialized(
    manifest_frontier: Option<StrataLsn>,
    materialized_through: &BTreeMap<u32, Option<StrataLsn>>,
    partition: u32,
    lsn: StrataLsn,
) -> bool {
    manifest_frontier.is_some_and(|materialized| lsn <= materialized)
        || materialized_through
            .get(&partition)
            .copied()
            .flatten()
            .is_some_and(|materialized| lsn <= materialized)
}

fn replay_plain(
    partitions: &mut BTreeMap<u32, PartitionState>,
    partition: u32,
    key: &[u8],
    lsn: StrataLsn,
    value: &[u8],
) -> Result<()> {
    let state = replay_partition(partitions, partition)?;
    loop {
        match state.active.insert_active(key, lsn, value) {
            Ok(()) => return Ok(()),
            Err(Error::MemtableFull { .. }) if !state.active.is_empty() => {
                roll_active(partition, state)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn replay_prefix(
    partitions: &mut BTreeMap<u32, PartitionState>,
    partition: u32,
    key_prefix: &[u8],
    key_suffix: &[u8],
    lsn: StrataLsn,
    value: &[u8],
) -> Result<()> {
    let state = replay_partition(partitions, partition)?;
    loop {
        match state
            .active
            .insert_prefix_active(key_prefix, key_suffix, lsn, value)
        {
            Ok(()) => return Ok(()),
            Err(Error::MemtableFull { .. }) if !state.active.is_empty() => {
                roll_active(partition, state)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn replay_partition(
    partitions: &mut BTreeMap<u32, PartitionState>,
    partition: u32,
) -> Result<&mut PartitionState> {
    let partition_count = partitions.len() as u32;
    partitions
        .get_mut(&partition)
        .ok_or(Error::InvalidPartition {
            partition,
            partition_count,
        })
}

fn prepare_batch(
    state: &mut WriteState,
    writes: Vec<(StrataLsn, Mutation)>,
) -> Result<PendingBatch> {
    let mut previous = state.last_lsn;
    let mut lsns = Vec::with_capacity(writes.len());
    let mut mutations = Vec::with_capacity(writes.len());
    for (lsn, mutation) in writes {
        if previous.is_some_and(|previous| lsn <= previous) {
            return Err(Error::MemtableLsnOutOfOrder {
                previous: previous.expect("checked above"),
                next: lsn,
            });
        }
        validate_mutation(&mutation, state.partition_count, state.memtable_capacity)?;
        previous = Some(lsn);
        lsns.push(lsn);
        mutations.push(mutation);
    }
    state.last_lsn = previous;
    Ok(PendingBatch { mutations, lsns })
}

fn validate_mutation(
    mutation: &Mutation,
    partition_count: u32,
    memtable_capacity: usize,
) -> Result<()> {
    match mutation {
        Mutation::Put {
            partition,
            key,
            value,
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key", key.len())?;
            validate_u32_len("value", value.len())?;
            validate_memtable_size(
                memtable_capacity,
                VERSION_BYTES,
                &[key.len(), 1, value.len()],
            )
        }
        Mutation::PutPrefix {
            partition,
            key_prefix,
            key_suffix,
            value,
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key prefix", key_prefix.len())?;
            validate_u32_len("key suffix", key_suffix.len())?;
            validate_u32_len("value", value.len())?;
            validate_memtable_size(
                memtable_capacity,
                VERSION_BYTES,
                &[key_prefix.len(), key_suffix.len(), 1, value.len()],
            )
        }
        Mutation::PutBlob {
            partition,
            key,
            metadata,
            ..
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key", key.len())?;
            validate_u32_len("blob metadata", metadata.len())?;
            validate_memtable_size(
                memtable_capacity,
                VERSION_BYTES,
                &[key.len(), 1, 4, metadata.len(), RECORD_REF_BYTES],
            )
        }
        Mutation::PutBlobPrefix {
            partition,
            key_prefix,
            key_suffix,
            metadata,
            ..
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key prefix", key_prefix.len())?;
            validate_u32_len("key suffix", key_suffix.len())?;
            validate_u32_len("blob metadata", metadata.len())?;
            validate_memtable_size(
                memtable_capacity,
                VERSION_BYTES,
                &[
                    key_prefix.len(),
                    key_suffix.len(),
                    1,
                    4,
                    metadata.len(),
                    RECORD_REF_BYTES,
                ],
            )
        }
    }
}

fn validate_partition(partition: u32, partition_count: u32) -> Result<()> {
    if partition >= partition_count {
        return Err(Error::InvalidPartition {
            partition,
            partition_count,
        });
    }
    Ok(())
}

fn validate_u32_len(name: &str, len: usize) -> Result<()> {
    u32::try_from(len)
        .map(|_| ())
        .map_err(|_| Error::Serialization(format!("{name} exceeds u32::MAX bytes")))
}

fn validate_memtable_size(capacity: usize, header: usize, fields: &[usize]) -> Result<()> {
    let required = fields
        .iter()
        .try_fold(header, |size, field| size.checked_add(*field))
        .ok_or_else(|| Error::Serialization("memtable entry length overflow".to_owned()))?;
    if required > capacity {
        return Err(Error::MemtableEntryTooLarge { capacity, required });
    }
    Ok(())
}

pub fn encode_inline_value(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + value.len());
    encoded.push(VALUE_INLINE);
    encoded.extend_from_slice(value);
    encoded
}

fn encode_inline_value_owned(mut value: Vec<u8>) -> Vec<u8> {
    value.reserve(1);
    value.push(VALUE_INLINE);
    value.rotate_right(1);
    value
}

pub fn encode_blob_value(metadata: &[u8], record_ref: RecordRef) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(5 + metadata.len() + RECORD_REF_BYTES);
    encoded.push(VALUE_BLOB);
    encoded.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    encoded.extend_from_slice(metadata);
    encoded.extend_from_slice(&encode_record_ref(record_ref));
    encoded
}

fn encode_blob_value_owned(mut metadata: Vec<u8>, record_ref: RecordRef) -> Vec<u8> {
    let metadata_len = metadata.len();
    metadata.reserve(5 + RECORD_REF_BYTES);
    metadata.resize(metadata_len + 5 + RECORD_REF_BYTES, 0);
    metadata.copy_within(0..metadata_len, 5);
    metadata[0] = VALUE_BLOB;
    metadata[1..5].copy_from_slice(&(metadata_len as u32).to_le_bytes());
    metadata[5 + metadata_len..].copy_from_slice(&encode_record_ref(record_ref));
    metadata
}

/// One value stored directly in the tree or indirectly in a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredValue<'a> {
    Inline(&'a [u8]),
    Blob {
        metadata: &'a [u8],
        record_ref: RecordRef,
    },
}

/// Decodes a value returned to a merge operator or by [`Lsm::get`].
pub fn decode_value(encoded: &[u8]) -> Result<StoredValue<'_>> {
    let Some((&kind, value)) = encoded.split_first() else {
        return Err(Error::Serialization("empty encoded LSM value".to_owned()));
    };
    match kind {
        VALUE_INLINE => Ok(StoredValue::Inline(value)),
        VALUE_BLOB => {
            if value.len() < 4 {
                return Err(Error::Serialization(
                    "encoded blob value has no metadata length".to_owned(),
                ));
            }
            let metadata_len = u32::from_le_bytes(value[..4].try_into().unwrap()) as usize;
            let metadata_end = 4usize
                .checked_add(metadata_len)
                .ok_or_else(|| Error::Serialization("blob metadata length overflow".to_owned()))?;
            let expected_len = metadata_end.checked_add(RECORD_REF_BYTES).ok_or_else(|| {
                Error::Serialization("encoded blob value length overflow".to_owned())
            })?;
            if value.len() != expected_len {
                return Err(Error::Serialization(
                    "encoded blob value length does not match its metadata length".to_owned(),
                ));
            }
            Ok(StoredValue::Blob {
                metadata: &value[4..metadata_end],
                record_ref: decode_record_ref(&value[metadata_end..])?,
            })
        }
        other => Err(Error::Serialization(format!(
            "unknown encoded LSM value kind {other}"
        ))),
    }
}

/// Encodes a segment reference in ordered fixed-width form.
pub fn encode_record_ref(record_ref: RecordRef) -> [u8; RECORD_REF_BYTES] {
    let mut encoded = [0; RECORD_REF_BYTES];
    encoded[..8].copy_from_slice(&record_ref.segment_id.to_be_bytes());
    encoded[8..16].copy_from_slice(&record_ref.offset.to_be_bytes());
    encoded[16..].copy_from_slice(&record_ref.len.to_be_bytes());
    encoded
}

/// Decodes a segment reference produced by [`encode_record_ref`].
pub fn decode_record_ref(encoded: &[u8]) -> Result<RecordRef> {
    if encoded.len() != RECORD_REF_BYTES {
        return Err(Error::InvalidRecordRefEncoding {
            expected: RECORD_REF_BYTES,
            actual: encoded.len(),
        });
    }
    Ok(RecordRef {
        segment_id: u64::from_be_bytes(encoded[..8].try_into().unwrap()),
        offset: u64::from_be_bytes(encoded[8..16].try_into().unwrap()),
        len: u64::from_be_bytes(encoded[16..].try_into().unwrap()),
    })
}

fn remove_orphan_tables(root: &Path, manifest: &Manifest) -> Result<()> {
    let live = manifest
        .partitions
        .values()
        .flat_map(|partition| partition.base.iter().chain(&partition.patches))
        .map(|table| root.join(&table.relative_path))
        .collect::<HashSet<_>>();
    remove_orphan_tables_in(root, &live)
}

fn remove_orphan_tables_in(directory: &Path, live: &HashSet<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory).map_err(|source| Error::Io {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| Error::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        if file_type.is_dir() {
            remove_orphan_tables_in(&path, live)?;
            continue;
        }
        let is_table = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".sst") || name.ends_with(".sst.tmp"));
        if file_type.is_file() && is_table && !live.contains(&path) {
            fs::remove_file(&path).map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
            sync_parent(&path)?;
        }
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        num::{NonZeroU32, NonZeroUsize},
        sync::{Arc, Barrier},
        thread,
        time::{Duration, Instant},
    };

    use strata_core::RecordRef;
    use tempfile::TempDir;

    use super::*;
    use crate::{Manifest, Replace};

    fn open(directory: &TempDir, recovered: Vec<(StrataLsn, Mutation)>) -> Arc<Lsm> {
        let last_lsn = recovered.last().map(|(lsn, _)| *lsn);
        Arc::new(
            Lsm::from_parts(
                directory.path(),
                Arc::new(Manifest::empty("test-base", "test-patch", NonZeroU32::MIN)),
                recovered,
                last_lsn,
                LsmOptions::default(),
            )
            .unwrap(),
        )
    }

    fn put(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Put {
            partition: 0,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn open_with_rollover(directory: &TempDir) -> Arc<Lsm> {
        Arc::new(
            Lsm::from_parts(
                directory.path(),
                Arc::new(Manifest::empty("test-base", "test-patch", NonZeroU32::MIN)),
                Vec::new(),
                None,
                LsmOptions {
                    rollover_policy: Some(MemtableRolloverPolicy::new(
                        NonZeroUsize::MIN,
                        Duration::from_secs(60),
                    )),
                    ..LsmOptions::default()
                },
            )
            .unwrap(),
        )
    }

    #[test]
    fn one_lsm_allocates_unique_canonical_table_targets() {
        let directory = TempDir::new().unwrap();
        let lsm = open(&directory, Vec::new());
        let barrier = Arc::new(Barrier::new(3));
        let mut allocators = Vec::new();

        for patch in [true, false] {
            let lsm = Arc::clone(&lsm);
            let barrier = Arc::clone(&barrier);
            allocators.push(thread::spawn(move || {
                barrier.wait();
                (0..1_000)
                    .map(|_| {
                        if patch {
                            lsm.allocate_patch_target().unwrap()
                        } else {
                            lsm.allocate_base_target().unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
            }));
        }

        barrier.wait();
        let targets = allocators
            .into_iter()
            .flat_map(|allocator| allocator.join().unwrap())
            .collect::<Vec<_>>();
        let ids = targets.iter().map(TableTarget::id).collect::<HashSet<_>>();

        assert_eq!(ids.len(), 2_000);
        assert_eq!(ids.iter().copied().min(), Some(1));
        assert_eq!(ids.iter().copied().max(), Some(2_000));
        assert!(targets.iter().all(|target| {
            target.relative_path() == format!("patch-{:020}.sst", target.id())
                || target.relative_path() == format!("base-{:020}.sst", target.id())
        }));
    }

    #[test]
    fn table_targets_reject_the_wrong_writer_kind() {
        assert!(TableTarget::patch(1).into_base_parts().is_err());
        assert!(TableTarget::base(2).into_patch_parts().is_err());
    }

    #[test]
    fn caller_assigns_lsns_and_gaps_are_valid() {
        let directory = TempDir::new().unwrap();
        let lsm = open(&directory, Vec::new());

        let written = lsm
            .write_batch(vec![(10, put(b"a", b"one")), (13, put(b"b", b"two"))])
            .unwrap();

        assert_eq!(written.lsns, vec![10, 13]);
        assert_eq!(lsm.last_lsn().unwrap(), Some(13));
        assert_eq!(
            lsm.get(0, b"b", &Replace).unwrap(),
            Some(encode_inline_value(b"two"))
        );
    }

    #[test]
    fn reads_hide_a_batch_prefix_while_memtable_backpressure_releases_the_lock() {
        let directory = TempDir::new().unwrap();
        let lsm = open_with_rollover(&directory);
        lsm.write(1, put(b"a", b"one")).unwrap();
        lsm.write(2, put(b"b", b"two")).unwrap();
        assert_eq!(lsm.frozen_generations(0).unwrap(), [1]);

        let writer_lsm = Arc::clone(&lsm);
        let writer = thread::spawn(move || {
            writer_lsm.write_batch(vec![
                (3, put(b"c", b"three")),
                (4, put(b"d", b"four")),
                (5, put(b"e", b"five")),
            ])
        });

        let publish = |edit: &ManifestEdit| {
            let mut manifest = (*lsm.manifest()).clone();
            manifest.apply(edit)?;
            Ok(manifest)
        };
        lsm.flush_one(0, lsm.allocate_patch_target().unwrap(), 2, publish)
            .unwrap()
            .unwrap();

        let wait_for_partial_active = |key: &[u8]| {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let reached = {
                    let state = lock(&lsm.memory);
                    state.last_visible_lsn == Some(2)
                        && !state.partition(0).unwrap().active.get_all(key).is_empty()
                };
                if reached {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "writer did not reach the expected partial batch state"
                );
                thread::yield_now();
            }
        };
        wait_for_partial_active(b"c");

        assert_eq!(lsm.get(0, b"c", &Replace).unwrap(), None);
        let mut partial = lsm.iter(0, None, None, StrataLsn::MAX).unwrap();
        let mut partial_keys = Vec::new();
        while let Some((key, _)) = partial.next(&Replace).unwrap() {
            partial_keys.push(key);
        }
        assert_eq!(partial_keys, [b"a".to_vec(), b"b".to_vec()]);

        lsm.flush_one(0, lsm.allocate_patch_target().unwrap(), 2, publish)
            .unwrap()
            .unwrap();
        wait_for_partial_active(b"d");
        assert_eq!(lsm.get(0, b"c", &Replace).unwrap(), None);

        // This flush moves part of the still-invisible batch into a pending SST. The stored SST
        // snapshot retains max_lsn=2; current reads use the separately captured batch frontier.
        lsm.flush_one(0, lsm.allocate_patch_target().unwrap(), 2, |edit| {
            panic!("the partial batch SST must remain beyond the durable frontier: {edit:?}")
        })
        .unwrap()
        .unwrap();
        writer.join().unwrap().unwrap();

        assert_eq!(lock(&lsm.memory).snapshot.max_lsn(), 2);
        assert_eq!(
            lsm.get(0, b"c", &Replace).unwrap(),
            Some(encode_inline_value(b"three"))
        );
        let mut complete = lsm.iter(0, None, None, StrataLsn::MAX).unwrap();
        let mut complete_keys = Vec::new();
        while let Some((key, _)) = complete.next(&Replace).unwrap() {
            complete_keys.push(key);
        }
        assert_eq!(
            complete_keys,
            [
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
            ]
        );
    }

    #[test]
    fn lsn_must_increase_across_batches() {
        let directory = TempDir::new().unwrap();
        let lsm = open(&directory, Vec::new());
        lsm.write(5, put(b"a", b"one")).unwrap();

        assert!(matches!(
            lsm.write(5, put(b"b", b"two")),
            Err(Error::MemtableLsnOutOfOrder {
                previous: 5,
                next: 5
            })
        ));
    }

    #[test]
    fn blob_mutation_stores_the_callers_segment_reference() {
        let directory = TempDir::new().unwrap();
        let lsm = open(&directory, Vec::new());
        let record_ref = RecordRef {
            segment_id: 7,
            offset: 100,
            len: 25,
        };
        lsm.write(
            9,
            Mutation::PutBlob {
                partition: 0,
                key: b"blob".to_vec(),
                metadata: b"epoch=3".to_vec(),
                record_ref,
            },
        )
        .unwrap();

        let encoded = lsm.get(0, b"blob", &Replace).unwrap().unwrap();
        assert_eq!(
            decode_value(&encoded).unwrap(),
            StoredValue::Blob {
                metadata: b"epoch=3",
                record_ref,
            }
        );
    }

    #[test]
    fn recovery_uses_decoded_store_wal_mutations() {
        let directory = TempDir::new().unwrap();
        let lsm = open(
            &directory,
            vec![(2, put(b"a", b"old")), (8, put(b"a", b"new"))],
        );

        assert_eq!(
            lsm.get(0, b"a", &Replace).unwrap(),
            Some(encode_inline_value(b"new"))
        );
    }

    #[test]
    fn materialization_stops_before_an_unflushed_keyed_mutation() {
        let directory = TempDir::new().unwrap();
        let lsm = open(&directory, Vec::new());
        lsm.write(17, put(b"a", b"one")).unwrap();

        let advanced = lsm
            .materialize_through(20, |edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap();

        // LSNs 1..=16 may be RocksDB-only. LSN 17 cannot be crossed until its row is in an SST.
        assert_eq!(advanced, Some(16));
        assert_eq!(lsm.manifest().materialized_through, Some(16));
    }

    #[test]
    fn flushed_sst_waits_for_its_wal_frontier_before_manifest_publication() {
        let directory = TempDir::new().unwrap();
        let lsm = open_with_rollover(&directory);
        lsm.write(1, put(b"a", b"one")).unwrap();
        lsm.write(2, put(b"b", b"two")).unwrap();

        let table = lsm
            .flush_one(0, lsm.allocate_patch_target().unwrap(), 0, |_| {
                panic!("an SST beyond the WAL frontier must not be published")
            })
            .unwrap()
            .unwrap();

        assert!(lsm.frozen_generations(0).unwrap().is_empty());
        assert!(lsm.manifest().partitions[&0].patches.is_empty());
        assert_eq!(
            lsm.get(0, b"a", &Replace).unwrap(),
            Some(encode_inline_value(b"one"))
        );
        assert!(
            !lsm.publish_pending_through(0, |_| panic!("nothing is publishable"))
                .unwrap()
        );
        let materialized = lsm
            .materialize_through(1, |edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap();
        assert_eq!(materialized, Some(0));

        assert!(
            lsm.publish_pending_through(1, |edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap()
        );
        assert_eq!(lsm.manifest().partitions[&0].patches, [table]);
        let materialized = lsm
            .materialize_through(1, |edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap();
        assert_eq!(materialized, Some(1));
    }

    #[test]
    fn newer_flush_publishes_covered_pending_predecessor_in_the_same_edit() {
        let directory = TempDir::new().unwrap();
        let lsm = open_with_rollover(&directory);
        lsm.write(1, put(b"a", b"one")).unwrap();
        lsm.write(2, put(b"b", b"two")).unwrap();

        let older = lsm
            .flush_one(0, lsm.allocate_patch_target().unwrap(), 0, |_| {
                panic!("the older SST is ahead of the initial WAL frontier")
            })
            .unwrap()
            .unwrap();
        assert!(lsm.manifest().partitions[&0].patches.is_empty());

        lsm.write(3, put(b"c", b"three")).unwrap();
        let newer_target = lsm.allocate_patch_target().unwrap();
        let newer_path = newer_target.relative_path().to_owned();
        let newer = lsm
            .flush_one(0, newer_target, 2, |edit| {
                let paths = edit
                    .add_patches
                    .iter()
                    .map(|table| table.relative_path.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(paths, [older.relative_path.as_str(), newer_path.as_str()]);
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap()
            .unwrap();

        assert_eq!(lsm.manifest().partitions[&0].patches, [older, newer]);

        // A crash at this point must not let the newer table's max LSN suppress replay of an
        // older table that never reached the manifest.
        let manifest = lsm.manifest();
        drop(lsm);
        let recovered = Lsm::from_parts(
            directory.path(),
            manifest,
            vec![
                (1, put(b"a", b"one")),
                (2, put(b"b", b"two")),
                (3, put(b"c", b"three")),
            ],
            Some(3),
            LsmOptions::default(),
        )
        .unwrap();
        assert_eq!(
            recovered.get(0, b"a", &Replace).unwrap(),
            Some(encode_inline_value(b"one"))
        );
        assert_eq!(
            recovered.get(0, b"b", &Replace).unwrap(),
            Some(encode_inline_value(b"two"))
        );
        assert_eq!(
            recovered.get(0, b"c", &Replace).unwrap(),
            Some(encode_inline_value(b"three"))
        );
    }

    #[test]
    fn recovery_removes_an_sst_that_never_reached_the_manifest() {
        let directory = TempDir::new().unwrap();
        let lsm = open_with_rollover(&directory);
        lsm.write(1, put(b"a", b"one")).unwrap();
        lsm.write(2, put(b"b", b"two")).unwrap();
        let table = lsm
            .flush_one(0, lsm.allocate_patch_target().unwrap(), 0, |_| {
                panic!("an SST beyond the WAL frontier must not be published")
            })
            .unwrap()
            .unwrap();
        let table_path = directory.path().join(&table.relative_path);
        let manifest = lsm.manifest();
        assert!(table_path.exists());
        drop(lsm);

        let recovered = Lsm::from_parts(
            directory.path(),
            manifest,
            vec![(1, put(b"a", b"one")), (2, put(b"b", b"two"))],
            Some(2),
            LsmOptions::default(),
        )
        .unwrap();

        assert!(!table_path.exists());
        assert_eq!(
            recovered.get(0, b"a", &Replace).unwrap(),
            Some(encode_inline_value(b"one"))
        );
    }
}
