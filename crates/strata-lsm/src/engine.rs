use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs, mem,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        mpsc::{self, Receiver},
    },
};

use strata_core::{BlobKey, RecordRef, SegmentId, ShardKey, encoded_record_len};
use strata_segment::{SegmentFactory, SegmentWriter};

use crate::{
    BlockCacheStats, DEFAULT_BLOCK_CACHE_BYTES, Error, FileSyncSender, FileSyncTask,
    FrozenMemtable, LsmScan, Manifest, ManifestEdit, Memtable, MemtableRolloverPolicy,
    MergeOperator, Result, Snapshot, StrataLsn, TableMeta, TableStore, Wal, WalEntry, WalPosition,
    memtable::{ENTRY_HEADER_BYTES, PREFIX_ENTRY_HEADER_BYTES},
    table::sync_parent,
};

const PIPELINE_DEPTH: u64 = 2;
const RECORD_REF_BYTES: usize = 3 * mem::size_of::<u64>();
const WAL_PUT_BLOB: u8 = 0;
const WAL_PUT_BLOB_PREFIX: u8 = 1;
const WAL_METADATA: u8 = 2;
const WAL_PUT: u8 = 3;
const WAL_PUT_PREFIX: u8 = 4;
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

/// Value bytes and segment metadata for one put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub key: BlobKey,
    pub shard: ShardKey,
    pub payload: Arc<[u8]>,
}

/// One logical write. Sequence numbers, segment references, and WAL bytes are assigned internally.
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
        record: SegmentRecord,
    },
    PutBlobPrefix {
        partition: u32,
        key_prefix: Vec<u8>,
        key_suffix: Vec<u8>,
        metadata: Vec<u8>,
        record: SegmentRecord,
    },
    /// Ordered caller-defined metadata with no segment value or memtable row.
    Metadata { payload: Vec<u8> },
}

/// Memtable generation made immutable by one write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RolledMemtable {
    pub partition: u32,
    pub generation: u64,
}

/// Segment replaced independently from WAL rollover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RolledSegment {
    pub sealed_segment_id: u64,
    pub sealed_length: u64,
    pub active_segment_id: u64,
    pub sealed_before_lsn: StrataLsn,
}

/// Exact segment and WAL prefix covered by one completed durability barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsmCheckpoint {
    pub durable_lsn: Option<StrataLsn>,
    pub wal_position: WalPosition,
    pub active_segment_id: SegmentId,
    pub active_segment_offset: u64,
}

/// Result of one visible logical write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub lsn: StrataLsn,
    pub record_ref: Option<RecordRef>,
    pub wal_position: WalPosition,
    pub rolled_memtable: Option<RolledMemtable>,
    pub rolled_segment: Option<RolledSegment>,
}

/// Result of one ordered logical batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatchResult {
    pub lsns: Vec<StrataLsn>,
    pub record_refs: Vec<Option<RecordRef>>,
    pub wal_position: WalPosition,
    pub rolled_memtables: Vec<RolledMemtable>,
    pub rolled_segment: Option<RolledSegment>,
}

struct PartitionState {
    active: Memtable,
    frozen: VecDeque<Arc<FrozenMemtable>>,
}

struct MemoryState {
    manifest: Arc<Manifest>,
    snapshot: Arc<Snapshot>,
    partitions: BTreeMap<u32, PartitionState>,
    next_ticket: u64,
    last_visible_lsn: Option<StrataLsn>,
}

struct AppendState {
    segment: SegmentWriter,
    segment_factory: SegmentFactory,
    wal: Wal,
    file_sync_tx: FileSyncSender,
    partition_count: u32,
    memtable_capacity: usize,
    last_lsn: Option<StrataLsn>,
    durable_lsn: Option<StrataLsn>,
    next_ticket: u64,
    pending_segment_syncs: Vec<Receiver<Result<()>>>,
}

struct PendingBatch {
    ticket: u64,
    mutations: Vec<Mutation>,
    lsns: Vec<StrataLsn>,
    record_refs: Vec<Option<RecordRef>>,
    wal_position: WalPosition,
    rolled_segment: Option<RolledSegment>,
}

struct PreparedAppend {
    lsns: Vec<StrataLsn>,
    segment_bytes: u64,
}

/// Owning write façade over the active segment, WAL, memtables, and immutable tables.
///
/// `from_parts` expects one coherent recovered segment, segment factory, WAL, manifest, and last
/// lsn, then rebuilds the mutable view from WAL entries newer than its immutable patch tables.
pub struct Lsm {
    tables: Arc<TableStore>,
    append: Mutex<AppendState>,
    memory: Mutex<MemoryState>,
    memory_changed: Condvar,
    visible_ticket: Mutex<u64>,
    append_changed: Condvar,
    max_frozen_generations: usize,
    halted: Mutex<Option<String>>,
    flush_lock: Mutex<()>,
}

impl Lsm {
    pub fn from_parts(
        table_root: impl Into<PathBuf>,
        manifest: Arc<Manifest>,
        segment: SegmentWriter,
        segment_factory: SegmentFactory,
        wal: Wal,
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
        if wal.last_lsn() != last_lsn {
            return Err(Error::InvalidWal(format!(
                "WAL ends at {:?}, recovered store ends at {last_lsn:?}",
                wal.last_lsn()
            )));
        }
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
        replay_wal(
            &wal,
            manifest.materialized_through,
            &materialized_through,
            &mut partitions,
        )?;
        let file_sync_tx = wal.file_sync_sender();

        Ok(Self {
            tables,
            append: Mutex::new(AppendState {
                segment,
                segment_factory,
                wal,
                file_sync_tx,
                partition_count: manifest.partition_count,
                memtable_capacity: options.memtable_capacity,
                last_lsn,
                durable_lsn: last_lsn,
                next_ticket: 0,
                pending_segment_syncs: Vec::new(),
            }),
            memory: Mutex::new(MemoryState {
                manifest,
                snapshot,
                partitions,
                next_ticket: 0,
                last_visible_lsn: last_lsn,
            }),
            memory_changed: Condvar::new(),
            visible_ticket: Mutex::new(0),
            append_changed: Condvar::new(),
            max_frozen_generations: options.max_frozen_generations.get(),
            halted: Mutex::new(None),
            flush_lock: Mutex::new(()),
        })
    }

    /// Appends one operation to the ordered segment/WAL lane, then the ordered memtable lane.
    pub fn write(&self, mutation: Mutation) -> Result<WriteResult> {
        let result = self.write_batch(vec![mutation])?;
        Ok(WriteResult {
            lsn: result.lsns[0],
            record_ref: result.record_refs[0],
            wal_position: result.wal_position,
            rolled_memtable: result.rolled_memtables.into_iter().next(),
            rolled_segment: result.rolled_segment,
        })
    }

    /// Stores a value directly in the WAL, memtable, and SSTs.
    pub fn put(&self, partition: u32, key: Vec<u8>, value: Vec<u8>) -> Result<WriteResult> {
        self.write(Mutation::Put {
            partition,
            key,
            value,
        })
    }

    /// Stores payload bytes in the segment and its reference in the LSM tree.
    pub fn put_blob(
        &self,
        partition: u32,
        key: Vec<u8>,
        record: SegmentRecord,
    ) -> Result<WriteResult> {
        self.write(Mutation::PutBlob {
            partition,
            key,
            metadata: Vec::new(),
            record,
        })
    }

    /// Applies one batch atomically to the visible memtable view.
    ///
    /// Concurrent callers pipeline segment/WAL work for the next batch with memtable work for the
    /// previous batch. Both stages remain FIFO.
    pub fn write_batch(&self, mutations: Vec<Mutation>) -> Result<WriteBatchResult> {
        if mutations.is_empty() {
            self.check_running()?;
            return Ok(WriteBatchResult {
                lsns: Vec::new(),
                record_refs: Vec::new(),
                wal_position: lock(&self.append).wal.position(),
                rolled_memtables: Vec::new(),
                rolled_segment: None,
            });
        }

        let pending = {
            let mut append = lock(&self.append);
            self.check_running()?;

            let mut visible_ticket = lock(&self.visible_ticket);
            while append.next_ticket.saturating_sub(*visible_ticket) >= PIPELINE_DEPTH {
                self.check_running()?;
                visible_ticket = wait(&self.append_changed, visible_ticket);
            }
            drop(visible_ticket);

            let prepared = prepare_batch(&append, &mutations)?;
            if prepared.segment_bytes > append.segment.max_size() {
                return Err(strata_segment::Error::SegmentFull {
                    max_size: append.segment.max_size(),
                    attempted_size: prepared.segment_bytes,
                }
                .into());
            }
            let rolled_segment = match ensure_segment_capacity(
                &mut append,
                prepared.segment_bytes,
                prepared.lsns[0],
            ) {
                Ok(rolled) => rolled,
                Err(error) => {
                    self.halt(format!("segment rollover failed: {error}"));
                    return Err(error);
                }
            };
            match append_batch(&mut append, mutations, prepared.lsns, rolled_segment) {
                Ok(batch) => batch,
                Err(error) => {
                    self.halt(format!("segment/WAL append failed: {error}"));
                    return Err(error);
                }
            }
        };
        self.apply_pending_batch(pending)
    }

    /// Resolves one key across the active memtable, frozen generations, and captured SSTs.
    pub fn get(
        &self,
        partition: u32,
        key: &[u8],
        merge: &dyn MergeOperator,
    ) -> Result<Option<Vec<u8>>> {
        let (snapshot, memory_patches) = {
            self.check_running()?;
            let state = lock(&self.memory);
            let partition = state.partition(partition)?;
            let mut patches = Vec::new();
            for frozen in &partition.frozen {
                for entry in frozen.get_all(key) {
                    patches.push((entry.lsn, entry.value.to_vec()));
                }
            }
            for entry in partition.active.get_all(key) {
                patches.push((entry.lsn, entry.value.to_vec()));
            }
            (Arc::clone(&state.snapshot), patches)
        };

        let (base, mut patches) = snapshot.get_parts(partition, key)?;
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

    /// Captures a pinned, sorted partition scan over `[start, end)`.
    pub fn scan(
        &self,
        partition: u32,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        max_lsn: StrataLsn,
    ) -> Result<LsmScan> {
        self.check_running()?;
        let (snapshot, mut memory) = {
            let state = lock(&self.memory);
            let partition_state = state.partition(partition)?;
            let in_range = |key: &[u8]| {
                start.is_none_or(|start| key >= start) && end.is_none_or(|end| key < end)
            };
            let mut memory = Vec::new();
            for entry in partition_state
                .frozen
                .iter()
                .flat_map(|memtable| memtable.entries())
                .chain(partition_state.active.entries())
            {
                if entry.lsn <= max_lsn && in_range(entry.key) {
                    memory.push((entry.key.to_vec(), entry.lsn, entry.value.to_vec()));
                }
            }
            (Arc::clone(&state.snapshot), memory)
        };
        memory.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        LsmScan::new(
            snapshot,
            &self.tables,
            partition,
            start,
            end,
            memory,
            max_lsn,
        )
    }

    /// Syncs every segment and WAL append accepted before this call.
    ///
    /// Segment and WAL file syncs are queued together. The durable lsn advances only after both
    /// complete and the matching memtable batch is visible.
    pub fn sync(&self) -> Result<LsmCheckpoint> {
        let result = (|| {
            let mut append = lock(&self.append);
            self.check_running()?;
            let target_lsn = append.last_lsn;
            let target_ticket = append.next_ticket;
            let active_sync = queue_segment_sync(&append)?;
            let wal_position = append.wal.sync()?;
            let mut segment_syncs = mem::take(&mut append.pending_segment_syncs);
            segment_syncs.push(active_sync);

            for sync in segment_syncs {
                sync.recv().map_err(|_| Error::FileSyncQueueClosed)??;
            }
            append.wal.wait_for_sync(wal_position)?;
            self.wait_until_visible(target_ticket)?;
            append.durable_lsn = target_lsn;
            Ok(LsmCheckpoint {
                durable_lsn: target_lsn,
                wal_position,
                active_segment_id: append.segment.segment_id(),
                active_segment_offset: append.segment.write_offset(),
            })
        })();

        if let Err(error) = &result {
            self.halt(format!("durability barrier failed: {error}"));
        }
        result
    }

    /// Queues the active segment for sync and immediately installs a fresh one.
    pub fn roll_segment(&self) -> Result<RolledSegment> {
        let mut append = lock(&self.append);
        self.check_running()?;
        let sealed_before_lsn = next_lsn(&append)?;
        roll_segment(&mut append, sealed_before_lsn)
    }

    pub fn wal_position(&self) -> Result<WalPosition> {
        self.check_running()?;
        Ok(lock(&self.append).wal.position())
    }

    pub fn committed_wal_position(&self) -> Result<WalPosition> {
        self.check_running()?;
        lock(&self.append).wal.committed_position()
    }

    pub fn durable_lsn(&self) -> Result<Option<StrataLsn>> {
        self.check_running()?;
        Ok(lock(&self.append).durable_lsn)
    }

    pub fn manifest(&self) -> Arc<Manifest> {
        Arc::clone(&lock(&self.memory).manifest)
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

    /// Flushes the oldest frozen generation and asks the caller to durably publish its manifest
    /// edit. A successful callback may make complete rolled WAL files immediately reclaimable.
    pub fn flush_one(
        &self,
        partition: u32,
        id: u64,
        relative_path: impl Into<String>,
        publish: impl FnOnce(&ManifestEdit) -> Result<Manifest>,
    ) -> Result<Option<TableMeta>> {
        let _flush = lock(&self.flush_lock);
        let (frozen, manifest, max_lsn) = {
            self.check_running()?;
            let state = lock(&self.memory);
            let Some(frozen) = state.partition(partition)?.frozen.front() else {
                return Ok(None);
            };
            (
                Arc::clone(frozen),
                Arc::clone(&state.manifest),
                state.last_visible_lsn.unwrap_or_default(),
            )
        };
        let meta = frozen.flush(
            self.tables.root(),
            relative_path,
            id,
            partition,
            &manifest.patch_format_id,
        )?;
        let (materialized_through, wal_retained_from) = {
            let append = lock(&self.append);
            let materialized = next_materialized_frontier(&append.wal, &manifest, Some(&meta))?;
            let retained_from = match materialized {
                Some(materialized) => append.wal.retained_from_after(materialized)?,
                None => manifest.wal_retained_from,
            };
            (materialized, retained_from)
        };
        let edit = ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: vec![meta.clone()],
            materialized_through,
            wal_retained_from: Some(wal_retained_from),
        };
        let published = publish(&edit)?;
        published.validate()?;
        let visible = published.partitions.get(&partition).is_some_and(|tables| {
            tables
                .patches
                .iter()
                .any(|table| table.relative_path == meta.relative_path)
        });
        if !visible {
            return Err(Error::InvalidManifest {
                reason: format!(
                    "published manifest does not contain flushed SST {}",
                    meta.relative_path
                ),
            });
        }
        if edit.materialized_through.is_some_and(|frontier| {
            published
                .materialized_through
                .is_none_or(|published| published < frontier)
        }) {
            return Err(Error::InvalidManifest {
                reason: "published manifest did not advance its materialized WAL frontier"
                    .to_owned(),
            });
        }
        if published.wal_retained_from < wal_retained_from {
            return Err(Error::InvalidManifest {
                reason: "published manifest did not advance its retained WAL file".to_owned(),
            });
        }
        let published = Arc::new(published);
        let snapshot = Arc::new(Snapshot::new_current(
            Arc::clone(&self.tables),
            Arc::clone(&published),
            max_lsn,
        )?);

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
        state.manifest = published;
        state.snapshot = snapshot;
        self.memory_changed.notify_all();
        let materialized_through = state.manifest.materialized_through;
        drop(state);
        if let Some(materialized) = materialized_through {
            lock(&self.append).wal.reclaim_through(materialized)?;
        }
        Ok(Some(meta))
    }

    /// Publishes a WAL frontier when the next unmaterialized entries are metadata-only.
    pub fn materialize_metadata(
        &self,
        publish: impl FnOnce(&ManifestEdit) -> Result<Manifest>,
    ) -> Result<Option<StrataLsn>> {
        let _flush = lock(&self.flush_lock);
        self.check_running()?;
        let (manifest, max_lsn) = {
            let state = lock(&self.memory);
            (
                Arc::clone(&state.manifest),
                state.last_visible_lsn.unwrap_or_default(),
            )
        };
        let (materialized_through, wal_retained_from) = {
            let append = lock(&self.append);
            let materialized = next_materialized_frontier(&append.wal, &manifest, None)?;
            if materialized <= manifest.materialized_through {
                return Ok(None);
            }
            (
                materialized.expect("materialized frontier advanced"),
                append
                    .wal
                    .retained_from_after(materialized.expect("frontier is present"))?,
            )
        };
        let edit = ManifestEdit {
            remove: Vec::new(),
            add_base: Vec::new(),
            add_patches: Vec::new(),
            materialized_through: Some(materialized_through),
            wal_retained_from: Some(wal_retained_from),
        };
        let published = publish(&edit)?;
        published.validate()?;
        if published
            .materialized_through
            .is_none_or(|frontier| frontier < materialized_through)
        {
            return Err(Error::InvalidManifest {
                reason: "published manifest did not advance its metadata WAL frontier".to_owned(),
            });
        }
        if published.wal_retained_from < wal_retained_from {
            return Err(Error::InvalidManifest {
                reason: "published manifest did not advance its retained WAL file".to_owned(),
            });
        }

        let published = Arc::new(published);
        let snapshot = Arc::new(Snapshot::new_current(
            Arc::clone(&self.tables),
            Arc::clone(&published),
            max_lsn,
        )?);
        let mut state = lock(&self.memory);
        self.check_running()?;
        state.manifest = published;
        state.snapshot = snapshot;
        drop(state);
        lock(&self.append)
            .wal
            .reclaim_through(materialized_through)?;
        Ok(Some(materialized_through))
    }

    /// Installs a separately published manifest, for example after compaction.
    pub fn install_manifest(&self, manifest: Manifest) -> Result<()> {
        let _flush = lock(&self.flush_lock);
        manifest.validate()?;
        let manifest = Arc::new(manifest);
        self.check_running()?;
        let mut state = lock(&self.memory);
        let snapshot = Arc::new(Snapshot::new_current(
            Arc::clone(&self.tables),
            Arc::clone(&manifest),
            state.last_visible_lsn.unwrap_or_default(),
        )?);
        state.manifest = manifest;
        state.snapshot = snapshot;
        Ok(())
    }

    fn apply_pending_batch(&self, pending: PendingBatch) -> Result<WriteBatchResult> {
        let PendingBatch {
            ticket,
            mutations,
            lsns,
            record_refs,
            wal_position,
            rolled_segment,
        } = pending;
        let result = (|| {
            let mut state = lock(&self.memory);
            while state.next_ticket != ticket {
                self.check_running()?;
                state = wait(&self.memory_changed, state);
            }
            self.check_running()?;

            let mut rolled = Vec::new();
            for (index, mutation) in mutations.iter().enumerate() {
                let lsn = lsns[index];
                match mutation {
                    Mutation::Put {
                        partition,
                        key,
                        value,
                    } => {
                        let value = encode_inline_value(value);
                        state = self.insert_memtable(state, *partition, &mut rolled, |active| {
                            active.insert_active(key, lsn, &value)
                        })?;
                    }
                    Mutation::PutPrefix {
                        partition,
                        key_prefix,
                        key_suffix,
                        value,
                    } => {
                        let value = encode_inline_value(value);
                        state = self.insert_memtable(state, *partition, &mut rolled, |active| {
                            active.insert_prefix_active(key_prefix, key_suffix, lsn, &value)
                        })?;
                    }
                    Mutation::PutBlob {
                        partition,
                        key,
                        metadata,
                        ..
                    } => {
                        let value = encode_blob_value(
                            metadata,
                            record_refs[index].expect("put has a segment record reference"),
                        );
                        state = self.insert_memtable(state, *partition, &mut rolled, |active| {
                            active.insert_active(key, lsn, &value)
                        })?;
                    }
                    Mutation::PutBlobPrefix {
                        partition,
                        key_prefix,
                        key_suffix,
                        metadata,
                        ..
                    } => {
                        let record_ref =
                            record_refs[index].expect("prefix put has a segment record reference");
                        let value = encode_blob_value(metadata, record_ref);
                        state = self.insert_memtable(state, *partition, &mut rolled, |active| {
                            active.insert_prefix_active(key_prefix, key_suffix, lsn, &value)
                        })?;
                    }
                    Mutation::Metadata { .. } => {}
                }
            }
            state.last_visible_lsn = lsns.last().copied();
            state.next_ticket = state.next_ticket.checked_add(1).ok_or(Error::LsnOverflow)?;
            *lock(&self.visible_ticket) = state.next_ticket;
            self.memory_changed.notify_all();
            self.append_changed.notify_all();
            Ok(WriteBatchResult {
                lsns,
                record_refs,
                wal_position,
                rolled_memtables: rolled,
                rolled_segment,
            })
        })();

        if let Err(error) = &result {
            self.halt(format!("memtable apply failed after WAL append: {error}"));
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

    fn wait_until_visible(&self, target_ticket: u64) -> Result<()> {
        let mut state = lock(&self.memory);
        while state.next_ticket < target_ticket {
            self.check_running()?;
            state = wait(&self.memory_changed, state);
        }
        self.check_running()
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
        let _visible_ticket = lock(&self.visible_ticket);
        lock(&self.halted).get_or_insert(reason);
        self.memory_changed.notify_all();
        self.append_changed.notify_all();
    }
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

fn next_materialized_frontier(
    wal: &Wal,
    manifest: &Manifest,
    flushed: Option<&TableMeta>,
) -> Result<Option<StrataLsn>> {
    let mut covered = manifest
        .partitions
        .iter()
        .map(|(&partition, tables)| {
            (
                partition,
                tables
                    .patches
                    .iter()
                    .filter_map(|table| table.max_lsn)
                    .max(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(flushed) = flushed {
        let flushed_lsn = flushed.max_lsn.ok_or_else(|| {
            Error::InvalidTable(format!(
                "flushed patch SST {} has no maximum lsn",
                flushed.relative_path
            ))
        })?;
        covered
            .entry(flushed.partition)
            .and_modify(|lsn| *lsn = Some(lsn.map_or(flushed_lsn, |old| old.max(flushed_lsn))))
            .or_insert(Some(flushed_lsn));
    }

    let mut frontier = manifest.materialized_through;
    let mut blocked = false;
    wal.replay(|entry| {
        let partition = wal_entry_partition(entry)?;
        if frontier.is_some_and(|frontier| entry.lsn <= frontier) || blocked {
            return Ok(());
        }
        let materialized = partition.is_none_or(|partition| {
            covered
                .get(&partition)
                .copied()
                .flatten()
                .is_some_and(|lsn| entry.lsn <= lsn)
        });
        if materialized {
            frontier = Some(entry.lsn);
        } else {
            blocked = true;
        }
        Ok(())
    })?;
    Ok(frontier)
}

fn wal_entry_partition(entry: &WalEntry) -> Result<Option<u32>> {
    let Some((&kind, payload)) = entry.payload.split_first() else {
        return Err(Error::InvalidWal("empty mutation payload".to_owned()));
    };
    match kind {
        WAL_PUT | WAL_PUT_PREFIX | WAL_PUT_BLOB | WAL_PUT_BLOB_PREFIX => {
            Ok(Some(WalDecoder::new(payload).u32()?))
        }
        WAL_METADATA => Ok(None),
        other => Err(Error::InvalidWal(format!(
            "unknown mutation payload kind {other}"
        ))),
    }
}

fn replay_wal(
    wal: &Wal,
    manifest_frontier: Option<StrataLsn>,
    materialized_through: &BTreeMap<u32, Option<StrataLsn>>,
    partitions: &mut BTreeMap<u32, PartitionState>,
) -> Result<()> {
    wal.replay(|entry| {
        let Some((&kind, payload)) = entry.payload.split_first() else {
            return Err(Error::InvalidWal("empty mutation payload".to_owned()));
        };
        let mut decoder = WalDecoder::new(payload);
        match kind {
            WAL_PUT => {
                let partition = decoder.u32()?;
                let key_len = decoder.u32()? as usize;
                let value_len = decoder.u32()? as usize;
                let key = decoder.take(key_len)?;
                let value = encode_inline_value(decoder.take(value_len)?);
                decoder.finish()?;
                if is_materialized(
                    manifest_frontier,
                    materialized_through,
                    partition,
                    entry.lsn,
                ) {
                    return Ok(());
                }
                replay_plain(partitions, partition, key, entry.lsn, &value)
            }
            WAL_PUT_PREFIX => {
                let partition = decoder.u32()?;
                let key_prefix_len = decoder.u32()? as usize;
                let key_suffix_len = decoder.u32()? as usize;
                let value_len = decoder.u32()? as usize;
                let key_prefix = decoder.take(key_prefix_len)?;
                let key_suffix = decoder.take(key_suffix_len)?;
                let value = encode_inline_value(decoder.take(value_len)?);
                decoder.finish()?;
                if is_materialized(
                    manifest_frontier,
                    materialized_through,
                    partition,
                    entry.lsn,
                ) {
                    return Ok(());
                }
                replay_prefix(
                    partitions, partition, key_prefix, key_suffix, entry.lsn, &value,
                )
            }
            WAL_PUT_BLOB => {
                let partition = decoder.u32()?;
                let key_len = decoder.u32()? as usize;
                let metadata_len = decoder.u32()? as usize;
                let key = decoder.take(key_len)?;
                let metadata = decoder.take(metadata_len)?;
                let value = encode_blob_value(metadata, decoder.record_ref()?);
                decoder.finish()?;
                if is_materialized(
                    manifest_frontier,
                    materialized_through,
                    partition,
                    entry.lsn,
                ) {
                    return Ok(());
                }
                replay_plain(partitions, partition, key, entry.lsn, &value)
            }
            WAL_PUT_BLOB_PREFIX => {
                let partition = decoder.u32()?;
                let key_prefix_len = decoder.u32()? as usize;
                let key_suffix_len = decoder.u32()? as usize;
                let metadata_len = decoder.u32()? as usize;
                let key_prefix = decoder.take(key_prefix_len)?;
                let key_suffix = decoder.take(key_suffix_len)?;
                let metadata = decoder.take(metadata_len)?;
                let record_ref = decoder.record_ref()?;
                decoder.finish()?;
                if is_materialized(
                    manifest_frontier,
                    materialized_through,
                    partition,
                    entry.lsn,
                ) {
                    return Ok(());
                }

                let value = encode_blob_value(metadata, record_ref);
                replay_prefix(
                    partitions, partition, key_prefix, key_suffix, entry.lsn, &value,
                )
            }
            WAL_METADATA => Ok(()),
            other => Err(Error::InvalidWal(format!(
                "unknown mutation payload kind {other}"
            ))),
        }
    })
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

struct WalDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WalDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::InvalidWal("mutation length overflow".to_owned()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::InvalidWal("truncated mutation payload".to_owned()))?;
        self.offset = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn record_ref(&mut self) -> Result<RecordRef> {
        decode_record_ref(self.take(RECORD_REF_BYTES)?)
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(Error::InvalidWal(
                "mutation payload has trailing bytes".to_owned(),
            ));
        }
        Ok(())
    }
}

fn prepare_batch(state: &AppendState, mutations: &[Mutation]) -> Result<PreparedAppend> {
    let count = u64::try_from(mutations.len()).map_err(|_| Error::LsnOverflow)?;
    let first_lsn = next_lsn(state)?;
    first_lsn.checked_add(count - 1).ok_or(Error::LsnOverflow)?;

    let mut segment_bytes = 0_u64;
    let mut lsns = Vec::with_capacity(mutations.len());
    for (offset, mutation) in mutations.iter().enumerate() {
        validate_mutation(mutation, state.partition_count, state.memtable_capacity)?;
        if let Some(record) = mutation.record() {
            segment_bytes = segment_bytes
                .checked_add(encoded_record_len(&record.key, record.payload.len())?)
                .ok_or(Error::LsnOverflow)?;
        }
        let lsn = first_lsn
            .checked_add(u64::try_from(offset).map_err(|_| Error::LsnOverflow)?)
            .ok_or(Error::LsnOverflow)?;
        lsns.push(lsn);
    }
    Ok(PreparedAppend {
        lsns,
        segment_bytes,
    })
}

fn append_batch(
    state: &mut AppendState,
    mutations: Vec<Mutation>,
    lsns: Vec<StrataLsn>,
    rolled_segment: Option<RolledSegment>,
) -> Result<PendingBatch> {
    let mut entries = Vec::with_capacity(mutations.len());
    let mut record_refs = Vec::with_capacity(mutations.len());

    for (mutation, lsn) in mutations.iter().zip(lsns.iter().copied()) {
        let (record_ref, payload) = match mutation {
            Mutation::Put {
                partition,
                key,
                value,
            } => (None, encode_put_wal(*partition, key, value)),
            Mutation::PutPrefix {
                partition,
                key_prefix,
                key_suffix,
                value,
            } => (
                None,
                encode_inline_prefix_wal(*partition, key_prefix, key_suffix, value),
            ),
            Mutation::PutBlob {
                partition,
                key,
                metadata,
                record,
            } => {
                let outcome = state.segment.append_for_shard(
                    &record.key,
                    lsn,
                    record.shard,
                    &record.payload,
                )?;
                let payload = encode_blob_wal(*partition, key, metadata, outcome.record_ref);
                (Some(outcome.record_ref), payload)
            }
            Mutation::PutBlobPrefix {
                partition,
                key_prefix,
                key_suffix,
                metadata,
                record,
            } => {
                let outcome = state.segment.append_for_shard(
                    &record.key,
                    lsn,
                    record.shard,
                    &record.payload,
                )?;
                let payload = encode_prefix_wal(
                    *partition,
                    key_prefix,
                    key_suffix,
                    metadata,
                    outcome.record_ref,
                );
                (Some(outcome.record_ref), payload)
            }
            Mutation::Metadata { payload } => (None, encode_metadata_wal(payload)),
        };
        record_refs.push(record_ref);
        entries.push(WalEntry { lsn, payload });
    }

    let wal_position = state.wal.append(&entries)?;
    let ticket = state.next_ticket;
    state.next_ticket = state.next_ticket.checked_add(1).ok_or(Error::LsnOverflow)?;
    state.last_lsn = lsns.last().copied();
    Ok(PendingBatch {
        ticket,
        mutations,
        lsns,
        record_refs,
        wal_position,
        rolled_segment,
    })
}

fn next_lsn(state: &AppendState) -> Result<StrataLsn> {
    state
        .last_lsn
        .map_or(Ok(1), |lsn| lsn.checked_add(1).ok_or(Error::LsnOverflow))
}

fn ensure_segment_capacity(
    state: &mut AppendState,
    additional_bytes: u64,
    sealed_before_lsn: StrataLsn,
) -> Result<Option<RolledSegment>> {
    if additional_bytes == 0 {
        return Ok(None);
    }
    match state.segment.ensure_capacity(additional_bytes) {
        Ok(()) => Ok(None),
        Err(strata_segment::Error::SegmentFull { .. }) => {
            let rolled = roll_segment(state, sealed_before_lsn)?;
            state.segment.ensure_capacity(additional_bytes)?;
            Ok(Some(rolled))
        }
        Err(error) => Err(error.into()),
    }
}

fn roll_segment(state: &mut AppendState, sealed_before_lsn: StrataLsn) -> Result<RolledSegment> {
    let current_id = state.segment.segment_id();
    let next = state.segment_factory.create()?;
    if next.segment_id() <= current_id {
        return Err(Error::SegmentOutOfOrder {
            current: current_id,
            next: next.segment_id(),
        });
    }
    let sync = queue_segment_sync(state)?;
    let rolled = RolledSegment {
        sealed_segment_id: current_id,
        sealed_length: state.segment.write_offset(),
        active_segment_id: next.segment_id(),
        sealed_before_lsn,
    };
    state.pending_segment_syncs.push(sync);
    state.segment = next;
    Ok(rolled)
}

impl Mutation {
    fn record(&self) -> Option<&SegmentRecord> {
        match self {
            Self::PutBlob { record, .. } | Self::PutBlobPrefix { record, .. } => Some(record),
            Self::Put { .. } | Self::PutPrefix { .. } | Self::Metadata { .. } => None,
        }
    }
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
            validate_encoded_len("encoded WAL put", &[1, 4, 4, 4, key.len(), value.len()])?;
            validate_memtable_size(
                memtable_capacity,
                ENTRY_HEADER_BYTES,
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
            validate_encoded_len(
                "encoded WAL prefix put",
                &[
                    1,
                    4,
                    4,
                    4,
                    4,
                    key_prefix.len(),
                    key_suffix.len(),
                    value.len(),
                ],
            )?;
            validate_memtable_size(
                memtable_capacity,
                PREFIX_ENTRY_HEADER_BYTES,
                &[key_prefix.len(), key_suffix.len(), 1, value.len()],
            )
        }
        Mutation::PutBlob {
            partition,
            key,
            metadata,
            record,
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key", key.len())?;
            validate_u32_len("blob metadata", metadata.len())?;
            if key.as_slice() != record.key.as_bytes() {
                return Err(Error::Serialization(
                    "LSM key and segment record key differ".to_owned(),
                ));
            }
            validate_encoded_len(
                "encoded WAL blob put",
                &[1, 4, 4, 4, key.len(), metadata.len(), RECORD_REF_BYTES],
            )?;
            validate_memtable_size(
                memtable_capacity,
                ENTRY_HEADER_BYTES,
                &[key.len(), 1, 4, metadata.len(), RECORD_REF_BYTES],
            )
        }
        Mutation::PutBlobPrefix {
            partition,
            key_prefix,
            key_suffix,
            metadata,
            record,
        } => {
            validate_partition(*partition, partition_count)?;
            validate_u32_len("key prefix", key_prefix.len())?;
            validate_u32_len("key suffix", key_suffix.len())?;
            validate_u32_len("blob metadata", metadata.len())?;
            if record.key.len() != key_prefix.len() + key_suffix.len()
                || !record.key.as_bytes().starts_with(key_prefix)
                || !record.key.as_bytes().ends_with(key_suffix)
            {
                return Err(Error::Serialization(
                    "LSM prefix key and segment record key differ".to_owned(),
                ));
            }
            validate_encoded_len(
                "encoded WAL prefix put",
                &[
                    1,
                    4,
                    4,
                    4,
                    4,
                    key_prefix.len(),
                    key_suffix.len(),
                    metadata.len(),
                    RECORD_REF_BYTES,
                ],
            )?;
            validate_memtable_size(
                memtable_capacity,
                PREFIX_ENTRY_HEADER_BYTES,
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
        Mutation::Metadata { payload } => {
            validate_u32_len("metadata payload", payload.len())?;
            validate_encoded_len("encoded WAL metadata", &[1, payload.len()])
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

fn validate_encoded_len(name: &str, fields: &[usize]) -> Result<()> {
    let len = fields
        .iter()
        .try_fold(0usize, |len, field| len.checked_add(*field))
        .ok_or_else(|| Error::Serialization(format!("{name} length overflow")))?;
    validate_u32_len(name, len)
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

fn encode_put_wal(partition: u32, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(13 + key.len() + value.len());
    payload.push(WAL_PUT);
    payload.extend_from_slice(&partition.to_le_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);
    payload
}

fn encode_inline_prefix_wal(
    partition: u32,
    key_prefix: &[u8],
    key_suffix: &[u8],
    value: &[u8],
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(17 + key_prefix.len() + key_suffix.len() + value.len());
    payload.push(WAL_PUT_PREFIX);
    payload.extend_from_slice(&partition.to_le_bytes());
    payload.extend_from_slice(&(key_prefix.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(key_suffix.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(key_prefix);
    payload.extend_from_slice(key_suffix);
    payload.extend_from_slice(value);
    payload
}

fn encode_blob_wal(partition: u32, key: &[u8], metadata: &[u8], record_ref: RecordRef) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 12 + key.len() + metadata.len() + RECORD_REF_BYTES);
    payload.push(WAL_PUT_BLOB);
    payload.extend_from_slice(&partition.to_le_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(metadata);
    payload.extend_from_slice(&encode_record_ref(record_ref));
    payload
}

fn encode_prefix_wal(
    partition: u32,
    key_prefix: &[u8],
    key_suffix: &[u8],
    metadata: &[u8],
    record_ref: RecordRef,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        1 + 16 + key_prefix.len() + key_suffix.len() + metadata.len() + RECORD_REF_BYTES,
    );
    payload.push(WAL_PUT_BLOB_PREFIX);
    payload.extend_from_slice(&partition.to_le_bytes());
    payload.extend_from_slice(&(key_prefix.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(key_suffix.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    payload.extend_from_slice(key_prefix);
    payload.extend_from_slice(key_suffix);
    payload.extend_from_slice(metadata);
    payload.extend_from_slice(&encode_record_ref(record_ref));
    payload
}

fn encode_metadata_wal(metadata: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + metadata.len());
    payload.push(WAL_METADATA);
    payload.extend_from_slice(metadata);
    payload
}

pub fn encode_inline_value(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + value.len());
    encoded.push(VALUE_INLINE);
    encoded.extend_from_slice(value);
    encoded
}

pub fn encode_blob_value(metadata: &[u8], record_ref: RecordRef) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(5 + metadata.len() + RECORD_REF_BYTES);
    encoded.push(VALUE_BLOB);
    encoded.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    encoded.extend_from_slice(metadata);
    encoded.extend_from_slice(&encode_record_ref(record_ref));
    encoded
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

fn queue_segment_sync(state: &AppendState) -> Result<Receiver<Result<()>>> {
    let path = state.segment.path().to_path_buf();
    let file = state.segment.clone_file_for_sync()?;
    let (completion_tx, completion_rx) = mpsc::sync_channel(1);
    state
        .file_sync_tx
        .send(FileSyncTask::new(path, file, move |result| {
            let _ = completion_tx.send(result);
        }))
        .map_err(|_| Error::FileSyncQueueClosed)?;
    Ok(completion_rx)
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
        num::{NonZeroU32, NonZeroUsize},
        sync::{Arc, mpsc as test_mpsc},
        thread,
        time::Duration,
    };

    use strata_core::{PlacementClass, ShardKey};
    use tempfile::TempDir;

    use super::*;
    use crate::{GarbageRecord, TableWriter, file_sync_channel};

    struct LastWriteWins;

    impl MergeOperator for LastWriteWins {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            patches: &[(StrataLsn, &[u8])],
            _emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
        ) -> Result<Option<Vec<u8>>> {
            Ok(patches
                .last()
                .map(|(_, value)| value.to_vec())
                .or_else(|| base.map(<[u8]>::to_vec)))
        }
    }

    fn lsn(sequence: u64) -> StrataLsn {
        sequence
    }

    fn engine(
        directory: &TempDir,
        rollover_policy: Option<MemtableRolloverPolicy>,
    ) -> (Arc<Lsm>, Vec<thread::JoinHandle<()>>) {
        engine_with_segment_max(directory, rollover_policy, 1 << 20)
    }

    fn engine_with_segment_max(
        directory: &TempDir,
        rollover_policy: Option<MemtableRolloverPolicy>,
        segment_max: u64,
    ) -> (Arc<Lsm>, Vec<thread::JoinHandle<()>>) {
        let table_root = directory.path().join("tables");
        let segment_root = directory.path().join("segments");
        fs::create_dir_all(&segment_root).unwrap();
        let manifest = Arc::new(Manifest::empty(
            "base-v1",
            "patch-v1",
            NonZeroU32::new(1).unwrap(),
        ));
        let (sync_tx, syncer) = file_sync_channel(8);
        let workers = (0..2)
            .map(|_| {
                let syncer = syncer.clone();
                thread::spawn(move || syncer.run())
            })
            .collect();
        drop(syncer);
        let wal = Wal::open(
            directory.path().join("wal"),
            1 << 20,
            WalPosition::default(),
            sync_tx,
        )
        .unwrap();
        let segment = SegmentWriter::create(
            strata_segment::segment_path(&segment_root, 1),
            1,
            PlacementClass::Ingest,
            segment_max,
        )
        .unwrap();
        let segment_factory = SegmentFactory::new(
            &segment_root,
            strata_segment::SegmentIdAllocator::new(2),
            PlacementClass::Ingest,
            segment_max,
        );
        let lsm = Lsm::from_parts(
            table_root,
            manifest,
            segment,
            segment_factory,
            wal,
            None,
            LsmOptions {
                memtable_capacity: 4096,
                rollover_policy,
                first_memtable_generation: 1,
                max_frozen_generations: NonZeroUsize::MIN,
                block_cache_capacity_bytes: DEFAULT_BLOCK_CACHE_BYTES,
            },
        )
        .unwrap();
        (Arc::new(lsm), workers)
    }

    fn put(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::PutBlob {
            partition: 0,
            key: key.to_vec(),
            metadata: Vec::new(),
            record: SegmentRecord {
                key: BlobKey::new(key.to_vec()).unwrap(),
                shard: ShardKey {
                    id: 1,
                    generation: 1,
                },
                payload: Arc::from(value),
            },
        }
    }

    fn blob_ref(value: &[u8]) -> RecordRef {
        match decode_value(value).unwrap() {
            StoredValue::Blob { record_ref, .. } => record_ref,
            StoredValue::Inline(_) => panic!("expected blob value"),
        }
    }

    fn finish(lsm: Arc<Lsm>, workers: Vec<thread::JoinHandle<()>>) {
        drop(lsm);
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn writes_assign_lsns_and_log_record_refs() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);

        let result = lsm.write(put(b"key", b"value")).unwrap();

        assert_eq!(result.lsn, lsn(1));
        assert_eq!(
            blob_ref(&lsm.get(0, b"key", &LastWriteWins).unwrap().unwrap()),
            result.record_ref.unwrap()
        );
        assert_eq!(result.wal_position, lsm.wal_position().unwrap());
        finish(lsm, workers);
    }

    #[test]
    fn lsm_encodes_the_wal_record_from_the_segment_outcome() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let result = lsm.write(put(b"key", b"value")).unwrap();
        let entries = {
            let append = lock(&lsm.append);
            let mut entries = Vec::new();
            append
                .wal
                .replay(|entry| {
                    entries.push(entry.clone());
                    Ok(())
                })
                .unwrap();
            entries
        };

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].lsn, result.lsn);
        assert_eq!(entries[0].payload[0], WAL_PUT_BLOB);
        assert_eq!(
            decode_record_ref(&entries[0].payload[entries[0].payload.len() - RECORD_REF_BYTES..])
                .unwrap(),
            result.record_ref.unwrap()
        );
        finish(lsm, workers);
    }

    #[test]
    fn inline_put_stays_out_of_the_segment() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let segment_offset = lock(&lsm.append).segment.write_offset();

        let result = lsm.put(0, b"key".to_vec(), b"value".to_vec()).unwrap();

        assert_eq!(result.lsn, lsn(1));
        assert_eq!(result.record_ref, None);
        assert_eq!(lock(&lsm.append).segment.write_offset(), segment_offset);
        let value = lsm.get(0, b"key", &LastWriteWins).unwrap().unwrap();
        assert_eq!(decode_value(&value).unwrap(), StoredValue::Inline(b"value"));

        let mut entries = Vec::new();
        lock(&lsm.append)
            .wal
            .replay(|entry| {
                entries.push(entry.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload[0], WAL_PUT);
        assert!(entries[0].payload.ends_with(b"keyvalue"));
        finish(lsm, workers);
    }

    #[test]
    fn plain_prefix_and_metadata_mutations_share_one_order() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let plain = lsm.write(put(b"A", b"plain")).unwrap();
        let prefix = lsm
            .write(Mutation::PutPrefix {
                partition: 0,
                key_prefix: b"K".to_vec(),
                key_suffix: b"X".to_vec(),
                value: b"prefix".to_vec(),
            })
            .unwrap();
        let metadata = lsm
            .write(Mutation::Metadata {
                payload: b"metadata".to_vec(),
            })
            .unwrap();

        assert_eq!(plain.lsn, lsn(1));
        assert_eq!(prefix.lsn, lsn(2));
        assert_eq!(prefix.record_ref, None);
        assert_eq!(metadata.lsn, lsn(3));
        assert_eq!(
            decode_value(&lsm.get(0, b"KX", &LastWriteWins).unwrap().unwrap()).unwrap(),
            StoredValue::Inline(b"prefix")
        );
        finish(lsm, workers);
    }

    #[test]
    fn sorted_scan_merges_tables_and_memtables() {
        let directory = TempDir::new().unwrap();
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::MIN, Duration::MAX);
        let (lsm, workers) = engine(&directory, Some(policy));
        let prefix = |suffix: &[u8], value: &[u8]| Mutation::PutPrefix {
            partition: 0,
            key_prefix: b"K".to_vec(),
            key_suffix: suffix.to_vec(),
            value: value.to_vec(),
        };
        lsm.write(prefix(b"a", b"old")).unwrap();
        lsm.write(prefix(b"b", b"second")).unwrap();
        lsm.flush_one(0, 1, "patch-1.sst", |edit| {
            let mut manifest = (*lsm.manifest()).clone();
            manifest.apply(edit)?;
            Ok(manifest)
        })
        .unwrap();
        let latest = lsm.write(prefix(b"a", b"new")).unwrap();

        let mut scan = lsm.scan(0, Some(b"K"), Some(b"L"), latest.lsn).unwrap();
        let mut rows = Vec::new();
        while let Some((key, value)) = scan.next(&crate::Replace).unwrap() {
            let StoredValue::Inline(value) = decode_value(&value).unwrap() else {
                panic!("inline writes must remain inline");
            };
            rows.push((key, value.to_vec()));
        }
        assert_eq!(
            rows,
            [
                (b"Ka".to_vec(), b"new".to_vec()),
                (b"Kb".to_vec(), b"second".to_vec()),
            ]
        );
        finish(lsm, workers);
    }

    #[test]
    fn metadata_only_frontier_does_not_require_an_sst() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let metadata = lsm
            .write(Mutation::Metadata {
                payload: b"metadata".to_vec(),
            })
            .unwrap();

        assert_eq!(
            lsm.materialize_metadata(|edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap(),
            Some(metadata.lsn)
        );
        assert_eq!(lsm.manifest().materialized_through, Some(metadata.lsn));

        lsm.put(0, b"key".to_vec(), b"value".to_vec()).unwrap();
        lsm.write(Mutation::Metadata {
            payload: b"blocked".to_vec(),
        })
        .unwrap();
        assert_eq!(
            lsm.materialize_metadata(|_| unreachable!()).unwrap(),
            None,
            "an unflushed data row must block the following metadata"
        );
        finish(lsm, workers);
    }

    #[test]
    fn batch_receives_consecutive_lsns_and_one_wal_frame() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);

        let result = lsm
            .write_batch(vec![
                put(b"a", b"one"),
                Mutation::Metadata {
                    payload: b"metadata".to_vec(),
                },
                put(b"b", b"two"),
            ])
            .unwrap();

        assert_eq!(result.lsns, vec![lsn(1), lsn(2), lsn(3)]);
        assert_eq!(result.wal_position, lsm.wal_position().unwrap());
        assert_eq!(result.record_refs.len(), 3);
        assert!(result.record_refs[1].is_none());
        finish(lsm, workers);
    }

    #[test]
    fn next_append_overlaps_the_previous_memtable_stage() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let memory = lock(&lsm.memory);
        let initial = lsm.wal_position().unwrap();

        let first_lsm = Arc::clone(&lsm);
        let first = thread::spawn(move || first_lsm.write(put(b"key", b"one")));
        while lsm.wal_position().unwrap() == initial {
            thread::yield_now();
        }
        let after_first = lsm.wal_position().unwrap();

        let second_lsm = Arc::clone(&lsm);
        let second = thread::spawn(move || second_lsm.write(put(b"key", b"two")));
        while lsm.wal_position().unwrap() == after_first {
            thread::yield_now();
        }
        drop(memory);

        assert_eq!(first.join().unwrap().unwrap().lsn, lsn(1));
        let second = second.join().unwrap().unwrap();
        assert_eq!(second.lsn, lsn(2));
        assert_eq!(
            blob_ref(&lsm.get(0, b"key", &LastWriteWins).unwrap().unwrap()),
            second.record_ref.unwrap()
        );
        finish(lsm, workers);
    }

    #[test]
    fn sync_covers_segment_wal_and_visible_lsn() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let result = lsm.write(put(b"key", b"value")).unwrap();

        let checkpoint = lsm.sync().unwrap();
        assert_eq!(checkpoint.durable_lsn, Some(result.lsn));
        assert_eq!(checkpoint.wal_position, result.wal_position);
        assert_eq!(checkpoint.active_segment_id, 1);
        assert_eq!(
            checkpoint.active_segment_offset,
            result.record_ref.unwrap().len
        );
        assert_eq!(lsm.durable_lsn().unwrap(), Some(result.lsn));
        assert_eq!(
            lsm.committed_wal_position().unwrap(),
            lsm.wal_position().unwrap()
        );
        finish(lsm, workers);
    }

    #[test]
    fn segment_roll_is_independent_from_wal_roll() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        lsm.write(put(b"a", b"one")).unwrap();

        let rolled = lsm.roll_segment().unwrap();
        let second = lsm.write(put(b"b", b"two")).unwrap();

        assert_eq!(rolled.sealed_segment_id, 1);
        assert_eq!(rolled.active_segment_id, 2);
        assert_eq!(rolled.sealed_before_lsn, lsn(2));
        assert_eq!(second.record_ref.unwrap().segment_id, 2);
        lsm.sync().unwrap();
        finish(lsm, workers);
    }

    #[test]
    fn capacity_roll_happens_inside_the_append_order() {
        let directory = TempDir::new().unwrap();
        let segment_max =
            strata_core::encoded_record_len(&BlobKey::new(b"a".to_vec()).unwrap(), 3).unwrap();
        let (lsm, workers) = engine_with_segment_max(&directory, None, segment_max);

        let first = lsm.write(put(b"a", b"one")).unwrap();
        let second = lsm.write(put(b"b", b"two")).unwrap();
        let rolled = second.rolled_segment.unwrap();

        assert!(first.rolled_segment.is_none());
        assert_eq!(rolled.sealed_segment_id, 1);
        assert_eq!(rolled.sealed_length, first.record_ref.unwrap().len);
        assert_eq!(rolled.active_segment_id, 2);
        assert_eq!(rolled.sealed_before_lsn, second.lsn);
        assert_eq!(second.record_ref.unwrap().segment_id, 2);
        lsm.sync().unwrap();
        finish(lsm, workers);
    }

    #[test]
    fn oversized_batch_is_rejected_before_rollover() {
        let directory = TempDir::new().unwrap();
        let segment_max =
            strata_core::encoded_record_len(&BlobKey::new(b"a".to_vec()).unwrap(), 3).unwrap();
        let (lsm, workers) = engine_with_segment_max(&directory, None, segment_max);

        assert!(matches!(
            lsm.write_batch(vec![put(b"a", b"one"), put(b"b", b"two")]),
            Err(Error::Segment(strata_segment::Error::SegmentFull { .. }))
        ));
        let written = lsm.write(put(b"a", b"one")).unwrap();
        assert_eq!(written.lsn, lsn(1));
        assert_eq!(written.record_ref.unwrap().segment_id, 1);
        assert!(written.rolled_segment.is_none());
        finish(lsm, workers);
    }

    #[test]
    fn flushed_generation_remains_visible_through_the_installed_manifest() {
        let directory = TempDir::new().unwrap();
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::new(1).unwrap(), Duration::MAX);
        let (lsm, workers) = engine(&directory, Some(policy));
        let first = lsm.write(put(b"a", b"one")).unwrap();
        let metadata = lsm
            .write(Mutation::Metadata {
                payload: b"metadata".to_vec(),
            })
            .unwrap();
        let rolled = lsm
            .write(put(b"b", b"two"))
            .unwrap()
            .rolled_memtable
            .unwrap();
        assert_eq!(rolled.generation, 1);

        let meta = lsm
            .flush_one(0, 1, "patch-1.sst", |edit| {
                let mut manifest = (*lsm.manifest()).clone();
                manifest.apply(edit)?;
                Ok(manifest)
            })
            .unwrap()
            .unwrap();

        assert_eq!(meta.record_count, 1);
        assert!(lsm.frozen_generations(0).unwrap().is_empty());
        assert_eq!(
            lsm.manifest().materialized_through,
            Some(metadata.lsn),
            "metadata immediately following a materialized row needs no replay"
        );
        assert_eq!(
            blob_ref(&lsm.get(0, b"a", &LastWriteWins).unwrap().unwrap()),
            first.record_ref.unwrap()
        );
        finish(lsm, workers);
    }

    #[test]
    fn failed_manifest_publication_does_not_advance_materialization() {
        let directory = TempDir::new().unwrap();
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::new(1).unwrap(), Duration::MAX);
        let (lsm, workers) = engine(&directory, Some(policy));
        lsm.write(put(b"a", b"one")).unwrap();
        lsm.write(put(b"b", b"two")).unwrap();

        let error = lsm
            .flush_one(0, 1, "orphan.sst", |_| {
                Err(Error::InvalidManifest {
                    reason: "injected publication failure".to_owned(),
                })
            })
            .unwrap_err();

        assert!(matches!(error, Error::InvalidManifest { .. }));
        assert_eq!(lsm.manifest().materialized_through, None);
        assert_eq!(lsm.frozen_generations(0).unwrap(), vec![1]);
        finish(lsm, workers);
    }

    #[test]
    fn startup_removes_unpublished_sst_outputs() {
        let directory = TempDir::new().unwrap();
        let table_root = directory.path().join("tables");
        let mut orphan =
            TableWriter::create_base(&table_root, "nested/orphan.sst", 1, 0, "base-v1").unwrap();
        orphan.add(b"key", b"value").unwrap();
        orphan.finish().unwrap();
        fs::write(table_root.join("abandoned.sst.tmp"), b"partial").unwrap();

        let (lsm, workers) = engine(&directory, None);

        assert!(!table_root.join("nested/orphan.sst").exists());
        assert!(!table_root.join("abandoned.sst.tmp").exists());
        finish(lsm, workers);
    }

    #[test]
    fn frozen_generation_limit_backpressures_the_pipeline_until_flush() {
        let directory = TempDir::new().unwrap();
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::new(1).unwrap(), Duration::MAX);
        let (lsm, workers) = engine(&directory, Some(policy));
        lsm.write(put(b"a", b"one")).unwrap();
        lsm.write(put(b"b", b"two")).unwrap();
        let before = lsm.wal_position().unwrap();
        let (result_tx, result_rx) = test_mpsc::channel();
        let writer = {
            let lsm = Arc::clone(&lsm);
            thread::spawn(move || {
                result_tx.send(lsm.write(put(b"c", b"three"))).unwrap();
            })
        };
        while lsm.wal_position().unwrap() == before {
            thread::yield_now();
        }
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(20)),
            Err(test_mpsc::RecvTimeoutError::Timeout)
        ));

        lsm.flush_one(0, 1, "patch-1.sst", |edit| {
            let mut manifest = (*lsm.manifest()).clone();
            manifest.apply(edit)?;
            Ok(manifest)
        })
        .unwrap()
        .unwrap();

        assert_eq!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap()
                .lsn,
            lsn(3)
        );
        writer.join().unwrap();
        finish(lsm, workers);
    }

    #[test]
    fn invalid_partition_is_rejected_before_physical_mutation() {
        let directory = TempDir::new().unwrap();
        let (lsm, workers) = engine(&directory, None);
        let mut mutation = put(b"a", b"one");
        let Mutation::PutBlob { partition, .. } = &mut mutation else {
            unreachable!()
        };
        *partition = 1;
        let wal_before = lsm.wal_position().unwrap();

        assert!(matches!(
            lsm.write(mutation),
            Err(Error::InvalidPartition { .. })
        ));
        assert_eq!(lsm.wal_position().unwrap(), wal_before);
        finish(lsm, workers);
    }
}
