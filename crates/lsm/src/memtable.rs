//! Bounded memtable keeping each generation's key and value bytes in a few large blocks.
//!
//! A generation holds one row per key, in insertion order, and finds a key through a table from
//! the key's hash to its row. Key and value bytes are copied into the generation's byte blocks.
//! The first version of a key is embedded in its row; a key allocates a history vector only after
//! its second mutation. Rollover freezes the complete generation and installs a fresh one.
//!
//! The bytes are not kept as one heap allocation per key and per value because of who frees them.
//! A generation is filled by the serialized writer and dropped by the flush thread once its table
//! is published. With one allocation each, that drop returns tens of thousands of small chunks to
//! the writer's allocator arena from another thread every few seconds. glibc parks such chunks
//! unmerged on the arena's fastbins, and the writer's next allocation of 1 KiB or more merges all
//! of them at once (`malloc_consolidate`), a pause of up to several milliseconds inside a put.
//! Dropping a generation now frees a handful of blocks and its row vector.

use std::{
    collections::{HashMap, hash_map},
    hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState},
    mem,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use crate::{Error, OperandFloor, OperandFloorFn, Result, StrataLsn, TableMeta, TableWriter};

/// Default logical byte capacity of one memtable generation.
pub const DEFAULT_MEMTABLE_BUFFER_BYTES: usize = 1 << 30;

// Logical accounting includes the directly stored version descriptor and the version's value
// bytes. A key's bytes are charged only for its first version. The key table, the row vector and
// the unused tail of a byte block remain implementation overhead, just as the old buffer-backed
// representation did not charge its index.
pub(crate) const VERSION_BYTES: usize = mem::size_of::<Version>();

/// Capacity of one byte block. Keys and values of the index are a few dozen bytes each, so a
/// block takes a few thousand of them; a key or value larger than this gets a block of its own.
const BLOCK_BYTES: usize = 256 * 1024;

/// Marks the end of a chain of rows whose keys share one hash.
const NO_ROW: usize = usize::MAX;

/// Rolls a non-empty active generation when either limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemtableRolloverPolicy {
    max_index_keys: NonZeroUsize,
    max_age: Duration,
}

impl MemtableRolloverPolicy {
    pub const fn new(max_index_keys: NonZeroUsize, max_age: Duration) -> Self {
        Self {
            max_index_keys,
            max_age,
        }
    }

    pub const fn max_index_keys(self) -> NonZeroUsize {
        self.max_index_keys
    }

    pub const fn max_age(self) -> Duration {
        self.max_age
    }

    pub fn should_rollover(self, index_keys: usize, elapsed: Duration) -> bool {
        index_keys >= self.max_index_keys.get() || elapsed >= self.max_age
    }
}

/// One entry borrowed directly from a memtable generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemtableEntry<'a> {
    pub lsn: StrataLsn,
    pub key: &'a [u8],
    pub value: &'a [u8],
    key_prefix_len: usize,
}

impl<'a> MemtableEntry<'a> {
    pub fn key_prefix(self) -> &'a [u8] {
        &self.key[..self.key_prefix_len]
    }

    pub fn key_suffix(self) -> &'a [u8] {
        &self.key[self.key_prefix_len..]
    }
}

/// Mutable memtable generation.
///
/// This type deliberately contains no internal synchronization. Strata's serialized writer owns
/// mutation order; callers may place the memtable behind their read-view synchronization.
#[derive(Debug)]
pub struct Memtable {
    generation: Generation,
    rollover_policy: Option<MemtableRolloverPolicy>,
}

/// Immutable generation produced by one rollover.
#[derive(Debug)]
pub struct FrozenMemtable {
    generation: Generation,
}

#[derive(Debug)]
struct Generation {
    id: u64,
    capacity: usize,
    // From a key's hash to the first row with that hash. The hash is computed once per lookup
    // with `hasher`, which is keyed at random like a `HashMap`'s, and the table uses it as is.
    index: HashMap<u64, usize, BuildHasherDefault<PrecomputedHash>>,
    hasher: RandomState,
    // One row per key, in the order the keys first arrived.
    rows: Vec<Row>,
    bytes: ByteBlocks,
    used_bytes: usize,
    entry_count: usize,
    first_lsn: Option<StrataLsn>,
    last_lsn: Option<StrataLsn>,
    started_at: Instant,
    // Gives every key the same hash, so that tests can exercise the chains of colliding rows.
    #[cfg(test)]
    collide_all_keys: bool,
}

#[derive(Debug)]
struct Row {
    key: Span,
    // The next row whose key has the same 64-bit hash, or `NO_ROW`. Two different keys with one
    // hash are not expected in practice; the chain keeps them correct if it happens.
    next_with_hash: usize,
    // Keeping the common single-version case inline avoids one Vec allocation per unique key.
    first: Version,
    rest: Vec<Version>,
}

#[derive(Debug, Clone, Copy)]
struct Version {
    lsn: StrataLsn,
    value: Span,
    key_prefix_len: usize,
}

/// Where a run of bytes lives in a generation's byte blocks.
#[derive(Debug, Clone, Copy)]
struct Span {
    block: usize,
    start: usize,
    len: usize,
}

/// Append-only storage for a generation's key and value bytes.
#[derive(Debug, Default)]
struct ByteBlocks {
    blocks: Vec<Vec<u8>>,
    // The block that takes the next bytes. Blocks before it are closed; blocks after it are empty
    // and were kept from a recycled generation.
    current: usize,
}

impl ByteBlocks {
    /// Copies `bytes` into a block and returns where they are.
    ///
    /// A block never reallocates: bytes go into the current block only if they fit in its spare
    /// capacity, and otherwise into the next one. No insert therefore pays for copying earlier
    /// bytes, and the cost of a new block is one allocation.
    fn push(&mut self, bytes: &[u8]) -> Span {
        if bytes.is_empty() {
            return Span {
                block: 0,
                start: 0,
                len: 0,
            };
        }
        loop {
            match self.blocks.get_mut(self.current) {
                Some(block) if block.capacity() - block.len() >= bytes.len() => {
                    let start = block.len();
                    block.extend_from_slice(bytes);
                    return Span {
                        block: self.current,
                        start,
                        len: bytes.len(),
                    };
                }
                Some(_) => self.current += 1,
                None => self
                    .blocks
                    .push(Vec::with_capacity(bytes.len().max(BLOCK_BYTES))),
            }
        }
    }

    fn get(&self, span: Span) -> &[u8] {
        if span.len == 0 {
            return &[];
        }
        &self.blocks[span.block][span.start..span.start + span.len]
    }

    /// Empties every block and keeps their allocations.
    fn clear(&mut self) {
        for block in &mut self.blocks {
            block.clear();
        }
        self.current = 0;
    }
}

/// Hasher for a table whose keys already are hashes.
#[derive(Debug, Default, Clone, Copy)]
struct PrecomputedHash(u64);

impl Hasher for PrecomputedHash {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // The table is keyed by `u64`, which arrives through `write_u64`. Anything else is folded
        // in rather than rejected, so the type stays a valid hasher.
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

impl Row {
    fn get(&self, index: usize) -> Option<&Version> {
        if index == 0 {
            Some(&self.first)
        } else {
            self.rest.get(index - 1)
        }
    }

    fn latest(&self) -> &Version {
        self.rest.last().unwrap_or(&self.first)
    }

    fn versions(&self) -> impl Iterator<Item = &Version> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

impl Memtable {
    pub fn new(generation: u64, capacity: usize) -> Self {
        Self {
            generation: Generation::new(generation, capacity),
            rollover_policy: None,
        }
    }

    pub fn with_rollover_policy(
        generation: u64,
        capacity: usize,
        rollover_policy: MemtableRolloverPolicy,
    ) -> Self {
        Self {
            generation: Generation::new(generation, capacity),
            rollover_policy: Some(rollover_policy),
        }
    }

    pub fn rollover_policy(&self) -> Option<MemtableRolloverPolicy> {
        self.rollover_policy
    }

    pub(crate) fn rollover_due(&self) -> bool {
        !self.is_empty()
            && self.rollover_policy.is_some_and(|policy| {
                policy.should_rollover(self.key_count(), self.generation.started_at.elapsed())
            })
    }

    pub fn generation(&self) -> u64 {
        self.generation.id
    }

    pub fn capacity(&self) -> usize {
        self.generation.capacity
    }

    pub fn used_bytes(&self) -> usize {
        self.generation.used_bytes
    }

    pub fn remaining_bytes(&self) -> usize {
        self.capacity().saturating_sub(self.used_bytes())
    }

    pub fn entry_count(&self) -> usize {
        self.generation.entry_count
    }

    pub fn key_count(&self) -> usize {
        self.generation.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.generation.entry_count == 0
    }

    /// LSN of the first mutation inserted, which is also the smallest in the generation:
    /// every insert must carry an LSN above the previous one.
    pub fn first_lsn(&self) -> Option<StrataLsn> {
        self.generation.first_lsn
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.generation.last_lsn
    }

    /// Stores one mutation and makes it the latest value for `key`.
    ///
    /// The configured rollover policy is evaluated before validation or mutation. When it fires,
    /// the old generation is returned and the entry is inserted into the new active
    /// generation. LSNs must increase across the rollover boundary.
    ///
    /// Without a configured policy, a full generation is left unchanged and reports
    /// [`Error::MemtableFull`].
    pub fn insert(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<Option<FrozenMemtable>> {
        let should_rollover = self.rollover_due();

        let mut required = self.generation.validate_insert(key, lsn, value)?;
        if should_rollover {
            required = self.generation.required_bytes_for_new_key(key, value)?;
        }
        let frozen = if should_rollover {
            let next_generation =
                self.generation
                    .id
                    .checked_add(1)
                    .ok_or(Error::MemtableGenerationOverflow {
                        generation: self.generation.id,
                    })?;
            Some(self.rollover(next_generation)?)
        } else {
            self.generation.ensure_available(required)?;
            None
        };
        self.generation.insert_bytes(key, lsn, value, 0, required);
        Ok(frozen)
    }

    pub(crate) fn insert_active(&mut self, key: &[u8], lsn: StrataLsn, value: &[u8]) -> Result<()> {
        let required = self.generation.validate_insert(key, lsn, value)?;
        self.generation.ensure_available(required)?;
        self.generation.insert_bytes(key, lsn, value, 0, required);
        Ok(())
    }

    /// Inserts engine-owned bytes. They are copied into the generation's byte blocks and the two
    /// vectors are left empty, with their allocations, for the caller to reuse or drop.
    ///
    /// On `MemtableFull`, both vectors remain untouched so the engine can roll and retry.
    pub(crate) fn insert_active_owned(
        &mut self,
        key: &mut Vec<u8>,
        lsn: StrataLsn,
        value: &mut Vec<u8>,
    ) -> Result<()> {
        let required = self.generation.validate_insert(key, lsn, value)?;
        self.generation.ensure_available(required)?;
        self.generation.insert_owned(key, lsn, value, 0, required);
        Ok(())
    }

    /// Appends one explicitly prefixed mutation.
    pub fn insert_prefix(
        &mut self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<Option<FrozenMemtable>> {
        let should_rollover = self.rollover_due();
        let (mut required, full_key) = self
            .generation
            .validate_prefix_insert(key_prefix, key_suffix, lsn, value)?;
        if should_rollover {
            required = self
                .generation
                .required_bytes_for_new_key(&full_key, value)?;
        }
        let frozen = if should_rollover {
            let next_generation =
                self.generation
                    .id
                    .checked_add(1)
                    .ok_or(Error::MemtableGenerationOverflow {
                        generation: self.generation.id,
                    })?;
            Some(self.rollover(next_generation)?)
        } else {
            self.generation.ensure_available(required)?;
            None
        };
        self.generation
            .insert_bytes(&full_key, lsn, value, key_prefix.len(), required);
        Ok(frozen)
    }

    pub(crate) fn insert_prefix_active(
        &mut self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<()> {
        let (required, full_key) = self
            .generation
            .validate_prefix_insert(key_prefix, key_suffix, lsn, value)?;
        self.generation.ensure_available(required)?;
        self.generation
            .insert_bytes(&full_key, lsn, value, key_prefix.len(), required);
        Ok(())
    }

    /// Inserts an engine-owned full key and value while retaining the key's explicit prefix split.
    /// The bytes are copied and the vectors left empty, as in [`Self::insert_active_owned`].
    /// On `MemtableFull`, both vectors remain untouched for a rollover retry.
    pub(crate) fn insert_prefix_active_owned(
        &mut self,
        full_key: &mut Vec<u8>,
        key_prefix_len: usize,
        lsn: StrataLsn,
        value: &mut Vec<u8>,
    ) -> Result<()> {
        let required =
            self.generation
                .validate_owned_prefix_insert(full_key, key_prefix_len, lsn, value)?;
        self.generation.ensure_available(required)?;
        self.generation
            .insert_owned(full_key, lsn, value, key_prefix_len, required);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        self.generation.get(key)
    }

    pub fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        self.generation.get_all(key)
    }

    /// Iterates every mutation, grouped by key and then by increasing LSN.
    pub fn entries(&self) -> MemtableEntries<'_> {
        self.generation.entries()
    }

    /// Freezes the active generation and installs an empty one.
    ///
    /// The new generation's key table and row vector are sized for as many keys as the outgoing
    /// one holds. A table that starts empty grows by doubling, and every doubling moves all of its
    /// entries inside the insert that triggers it: with a few thousand keys per generation that
    /// is a pause of several hundred microseconds on the writer, a few times per generation.
    /// Consecutive generations of a partition hold about the same number of keys, so sizing the
    /// next one from the last removes those pauses.
    pub fn rollover(&mut self, next_generation: u64) -> Result<FrozenMemtable> {
        let replacement = Self {
            generation: Generation::with_key_capacity(
                next_generation,
                self.capacity(),
                self.key_count(),
            ),
            rollover_policy: None,
        };
        self.rollover_into(replacement)
    }

    /// Freezes the active generation and installs an empty, possibly recycled replacement.
    ///
    /// A small caller-owned pool can recycle published generations and pass them here, retaining
    /// the key table, the row vector and the byte blocks instead of allocating them on every
    /// rollover.
    pub fn rollover_into(&mut self, mut replacement: Memtable) -> Result<FrozenMemtable> {
        if replacement.generation.id <= self.generation.id {
            return Err(Error::MemtableGenerationOutOfOrder {
                current: self.generation.id,
                next: replacement.generation.id,
            });
        }
        if !replacement.is_empty() {
            return Err(Error::MemtableReplacementNotEmpty {
                generation: replacement.generation.id,
            });
        }
        replacement.generation.started_at = Instant::now();
        Ok(FrozenMemtable {
            generation: mem::replace(&mut self.generation, replacement.generation),
        })
    }
}

impl FrozenMemtable {
    pub fn generation(&self) -> u64 {
        self.generation.id
    }

    pub fn capacity(&self) -> usize {
        self.generation.capacity
    }

    pub fn used_bytes(&self) -> usize {
        self.generation.used_bytes
    }

    pub fn entry_count(&self) -> usize {
        self.generation.entry_count
    }

    pub fn key_count(&self) -> usize {
        self.generation.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.generation.entry_count == 0
    }

    /// LSN of the first mutation inserted, which is also the smallest in the generation:
    /// every insert must carry an LSN above the previous one.
    pub fn first_lsn(&self) -> Option<StrataLsn> {
        self.generation.first_lsn
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.generation.last_lsn
    }

    pub fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        self.generation.get(key)
    }

    pub fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        self.generation.get_all(key)
    }

    /// Iterates every mutation, grouped by key and then by increasing LSN.
    pub fn entries(&self) -> MemtableEntries<'_> {
        self.generation.entries()
    }

    /// Resolves one version in a frozen key history. Used by a pinned range iterator without
    /// copying the version's value bytes.
    pub(crate) fn entry_at(&self, key: &[u8], version: usize) -> MemtableEntry<'_> {
        self.generation
            .entry_at(key, version)
            .expect("frozen memtable address remains valid")
    }

    pub(crate) fn indexed_entries(&self) -> impl Iterator<Item = (&[u8], usize, StrataLsn)> {
        let generation = &self.generation;
        generation.rows.iter().flat_map(move |row| {
            let key = generation.bytes.get(row.key);
            row.versions()
                .enumerate()
                .map(move |(version, value)| (key, version, value.lsn))
        })
    }

    /// Returns the latest mutation for every key in unsigned lexicographic key order.
    pub fn latest_entries_sorted(&self) -> Vec<MemtableEntry<'_>> {
        self.generation.latest_entries_sorted()
    }

    /// Writes every ordered patch to one immutable patch table.
    ///
    /// The table is fully synced and renamed by [`TableWriter::finish`]. Publication remains the
    /// caller's responsibility, and this frozen generation must remain readable until that
    /// publication becomes visible.
    pub fn flush(
        &self,
        root: impl AsRef<Path>,
        relative_path: impl Into<String>,
        id: u64,
        partition: u32,
        patch_format_id: &str,
        classify_operands: Option<&OperandFloorFn>,
    ) -> Result<TableMeta> {
        if self.is_empty() {
            return Err(Error::InvalidTable(
                "cannot flush an empty memtable".to_owned(),
            ));
        }
        let mut writer =
            TableWriter::create_patch(root, relative_path, id, partition, patch_format_id)?;
        let generation = &self.generation;
        let mut rows = generation
            .rows
            .iter()
            .map(|row| (generation.bytes.get(row.key), row))
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut floor = OperandFloor::None;
        for (key, row) in rows {
            if let Some(classify) = classify_operands {
                let operands = row
                    .versions()
                    .map(|version| (version.lsn, generation.bytes.get(version.value)))
                    .collect::<Vec<_>>();
                if let Some(lsn) = classify(&operands)? {
                    floor = floor.merge(OperandFloor::At(lsn));
                }
            }
            for version in row.versions() {
                let value = generation.bytes.get(version.value);
                if version.key_prefix_len == 0 {
                    writer.add_patch(key, version.lsn, value)?;
                } else {
                    writer.add_prefix_patch(
                        &key[..version.key_prefix_len],
                        &key[version.key_prefix_len..],
                        version.lsn,
                        value,
                    )?;
                }
            }
        }
        if classify_operands.is_some() {
            writer.set_operand_floor(floor)?;
        }
        writer.finish()
    }

    /// Clears a retired generation while retaining its allocations for reuse.
    ///
    /// Callers must invoke this only after the corresponding immutable tables are published and no
    /// reader can still hold a reference to the frozen generation.
    pub fn recycle(mut self, next_generation: u64) -> Result<Memtable> {
        if next_generation <= self.generation.id {
            return Err(Error::MemtableGenerationOutOfOrder {
                current: self.generation.id,
                next: next_generation,
            });
        }
        self.generation.reset(next_generation);
        Ok(Memtable {
            generation: self.generation,
            rollover_policy: None,
        })
    }
}

impl Generation {
    fn new(id: u64, capacity: usize) -> Self {
        Self::with_key_capacity(id, capacity, 0)
    }

    /// A generation whose key table and row vector can take `keys` keys before they have to
    /// grow. Zero allocates nothing.
    fn with_key_capacity(id: u64, capacity: usize, keys: usize) -> Self {
        Self {
            id,
            capacity,
            index: HashMap::with_capacity_and_hasher(keys, BuildHasherDefault::default()),
            hasher: RandomState::new(),
            rows: Vec::with_capacity(keys),
            bytes: ByteBlocks::default(),
            used_bytes: 0,
            entry_count: 0,
            first_lsn: None,
            last_lsn: None,
            started_at: Instant::now(),
            #[cfg(test)]
            collide_all_keys: false,
        }
    }

    fn reset(&mut self, id: u64) {
        self.id = id;
        // Keeps the key table's buckets, the row vector and the byte blocks for the next
        // generation. Only the history vectors of keys with several versions are freed.
        self.index.clear();
        self.rows.clear();
        self.bytes.clear();
        self.used_bytes = 0;
        self.entry_count = 0;
        self.first_lsn = None;
        self.last_lsn = None;
        self.started_at = Instant::now();
    }

    fn key_hash(&self, key: &[u8]) -> u64 {
        #[cfg(test)]
        if self.collide_all_keys {
            return 0;
        }
        self.hasher.hash_one(key)
    }

    /// The row of `key`, if the generation has one.
    fn find(&self, key: &[u8]) -> Option<usize> {
        let mut row = *self.index.get(&self.key_hash(key))?;
        loop {
            let candidate = &self.rows[row];
            if self.bytes.get(candidate.key) == key {
                return Some(row);
            }
            if candidate.next_with_hash == NO_ROW {
                return None;
            }
            row = candidate.next_with_hash;
        }
    }

    fn validate_insert(&self, key: &[u8], lsn: StrataLsn, value: &[u8]) -> Result<usize> {
        self.validate_lsn(lsn)?;

        u32::try_from(key.len()).map_err(|_| Error::MemtableEntryTooLarge {
            capacity: self.capacity,
            required: usize::MAX,
        })?;
        u32::try_from(value.len()).map_err(|_| Error::MemtableEntryTooLarge {
            capacity: self.capacity,
            required: usize::MAX,
        })?;
        let key_bytes = if self.find(key).is_some() {
            0
        } else {
            key.len()
        };
        let required = VERSION_BYTES
            .checked_add(key_bytes)
            .and_then(|len| len.checked_add(value.len()))
            .ok_or(Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required: usize::MAX,
            })?;
        if required > self.capacity {
            return Err(Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required,
            });
        }
        Ok(required)
    }

    fn validate_prefix_insert(
        &self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<(usize, Vec<u8>)> {
        self.validate_lsn(lsn)?;
        if key_prefix.is_empty() {
            return Err(Error::InvalidTable(
                "prefix memtable requires a non-empty key prefix".to_owned(),
            ));
        }
        for part in [key_prefix, key_suffix, value] {
            u32::try_from(part.len()).map_err(|_| Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required: usize::MAX,
            })?;
        }
        let key_len =
            key_prefix
                .len()
                .checked_add(key_suffix.len())
                .ok_or(Error::MemtableEntryTooLarge {
                    capacity: self.capacity,
                    required: usize::MAX,
                })?;
        let mut full_key = Vec::with_capacity(key_len);
        full_key.extend_from_slice(key_prefix);
        full_key.extend_from_slice(key_suffix);
        let required = self.required_bytes(&full_key, value)?;
        Ok((required, full_key))
    }

    fn validate_owned_prefix_insert(
        &self,
        full_key: &[u8],
        key_prefix_len: usize,
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<usize> {
        self.validate_lsn(lsn)?;
        if key_prefix_len == 0 || key_prefix_len > full_key.len() {
            return Err(Error::InvalidTable(
                "prefix memtable requires a non-empty key prefix within the full key".to_owned(),
            ));
        }
        for part in [full_key, value] {
            u32::try_from(part.len()).map_err(|_| Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required: usize::MAX,
            })?;
        }
        self.required_bytes(full_key, value)
    }

    fn required_bytes(&self, key: &[u8], value: &[u8]) -> Result<usize> {
        let key_bytes = if self.find(key).is_some() {
            0
        } else {
            key.len()
        };
        self.required_bytes_with_key_bytes(key_bytes, value)
    }

    fn required_bytes_for_new_key(&self, key: &[u8], value: &[u8]) -> Result<usize> {
        self.required_bytes_with_key_bytes(key.len(), value)
    }

    fn required_bytes_with_key_bytes(&self, key_bytes: usize, value: &[u8]) -> Result<usize> {
        let required = VERSION_BYTES
            .checked_add(key_bytes)
            .and_then(|len| len.checked_add(value.len()))
            .ok_or(Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required: usize::MAX,
            })?;
        if required > self.capacity {
            return Err(Error::MemtableEntryTooLarge {
                capacity: self.capacity,
                required,
            });
        }
        Ok(required)
    }

    fn validate_lsn(&self, lsn: StrataLsn) -> Result<()> {
        if let Some(previous) = self.last_lsn
            && lsn <= previous
        {
            return Err(Error::MemtableLsnOutOfOrder {
                previous,
                next: lsn,
            });
        }
        Ok(())
    }

    fn ensure_available(&self, required: usize) -> Result<()> {
        if self.used_bytes.saturating_add(required) > self.capacity {
            return Err(Error::MemtableFull {
                generation: self.id,
                capacity: self.capacity,
                used: self.used_bytes,
                required,
            });
        }
        Ok(())
    }

    /// Stores caller-owned bytes and leaves the caller's vectors empty.
    fn insert_owned(
        &mut self,
        key: &mut Vec<u8>,
        lsn: StrataLsn,
        value: &mut Vec<u8>,
        key_prefix_len: usize,
        required: usize,
    ) {
        self.insert_bytes(key, lsn, value, key_prefix_len, required);
        key.clear();
        value.clear();
    }

    fn insert_bytes(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        value: &[u8],
        key_prefix_len: usize,
        required: usize,
    ) {
        let hash = self.key_hash(key);
        let version = Version {
            lsn,
            value: self.bytes.push(value),
            key_prefix_len,
        };
        match self.index.entry(hash) {
            hash_map::Entry::Vacant(slot) => {
                slot.insert(self.rows.len());
                self.rows.push(Row {
                    key: self.bytes.push(key),
                    next_with_hash: NO_ROW,
                    first: version,
                    rest: Vec::new(),
                });
            }
            hash_map::Entry::Occupied(slot) => {
                let mut row = *slot.get();
                loop {
                    if self.bytes.get(self.rows[row].key) == key {
                        self.rows[row].rest.push(version);
                        break;
                    }
                    let next = self.rows[row].next_with_hash;
                    if next == NO_ROW {
                        self.rows[row].next_with_hash = self.rows.len();
                        self.rows.push(Row {
                            key: self.bytes.push(key),
                            next_with_hash: NO_ROW,
                            first: version,
                            rest: Vec::new(),
                        });
                        break;
                    }
                    row = next;
                }
            }
        }
        self.used_bytes += required;
        self.entry_count += 1;
        self.first_lsn.get_or_insert(lsn);
        self.last_lsn = Some(lsn);
    }

    fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        let row = &self.rows[self.find(key)?];
        Some(self.entry(row, row.latest()))
    }

    fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        self.find(key).map_or_else(Vec::new, |row| {
            let row = &self.rows[row];
            row.versions()
                .map(|version| self.entry(row, version))
                .collect()
        })
    }

    fn entries(&self) -> MemtableEntries<'_> {
        MemtableEntries {
            generation: self,
            row: 0,
            version: 0,
        }
    }

    fn latest_entries_sorted(&self) -> Vec<MemtableEntry<'_>> {
        let mut entries = self
            .rows
            .iter()
            .map(|row| self.entry(row, row.latest()))
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.key.cmp(right.key));
        entries
    }

    fn entry_at(&self, key: &[u8], version: usize) -> Option<MemtableEntry<'_>> {
        let row = &self.rows[self.find(key)?];
        Some(self.entry(row, row.get(version)?))
    }

    fn entry(&self, row: &Row, version: &Version) -> MemtableEntry<'_> {
        MemtableEntry {
            lsn: version.lsn,
            key: self.bytes.get(row.key),
            value: self.bytes.get(version.value),
            key_prefix_len: version.key_prefix_len,
        }
    }
}

/// Borrowed iterator over one stable memtable generation.
pub struct MemtableEntries<'a> {
    generation: &'a Generation,
    row: usize,
    version: usize,
}

impl<'a> Iterator for MemtableEntries<'a> {
    type Item = MemtableEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = self.generation.rows.get(self.row)?;
            if let Some(version) = row.get(self.version) {
                self.version += 1;
                return Some(self.generation.entry(row, version));
            }
            self.row += 1;
            self.version = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use crate::{Error, OperandFloor, OperandFloorFn, StrataLsn, TableReader};

    use super::{BLOCK_BYTES, Memtable, MemtableRolloverPolicy, VERSION_BYTES};

    fn lsn(sequence: u64) -> StrataLsn {
        sequence
    }

    fn insert(memtable: &mut Memtable, key: &[u8], sequence: u64, value: &[u8]) {
        assert!(
            memtable
                .insert(key, lsn(sequence), value)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn first_lsn_is_the_smallest_lsn_of_each_generation() {
        let smallest = |entries: super::MemtableEntries<'_>| entries.map(|entry| entry.lsn).min();

        let mut memtable = Memtable::new(1, 1024);
        assert_eq!(memtable.first_lsn(), None);
        // Later versions of an earlier key, and keys that hash anywhere, never lower it.
        insert(&mut memtable, b"m", 5, b"one");
        insert(&mut memtable, b"a", 9, b"two");
        insert(&mut memtable, b"m", 12, b"three");
        insert(&mut memtable, b"z", 30, b"four");
        assert_eq!(memtable.first_lsn(), Some(lsn(5)));
        assert_eq!(memtable.first_lsn(), smallest(memtable.entries()));
        // An out-of-order LSN is rejected, so nothing below the first can arrive later.
        assert!(memtable.insert(b"b", lsn(4), b"late").is_err());
        assert_eq!(memtable.first_lsn(), Some(lsn(5)));

        let frozen = memtable.rollover(2).unwrap();
        assert_eq!(frozen.first_lsn(), Some(lsn(5)));
        assert_eq!(frozen.first_lsn(), smallest(frozen.entries()));
        assert_eq!(memtable.first_lsn(), None);
        insert(&mut memtable, b"a", 31, b"five");
        insert(&mut memtable, b"c", 40, b"six");
        assert_eq!(memtable.first_lsn(), Some(lsn(31)));
        assert_eq!(memtable.first_lsn(), smallest(memtable.entries()));

        // A recycled generation starts over.
        let mut recycled = frozen.recycle(3).unwrap();
        assert_eq!(recycled.first_lsn(), None);
        insert(&mut recycled, b"q", 50, b"seven");
        assert_eq!(recycled.first_lsn(), Some(lsn(50)));
    }

    #[test]
    fn map_stores_each_keys_versions_directly() {
        let mut memtable = Memtable::new(7, 1024);
        insert(&mut memtable, b"beta", 1, b"one");
        insert(&mut memtable, b"alpha", 2, b"two");
        insert(&mut memtable, b"beta", 3, b"three");

        assert_eq!(memtable.entry_count(), 3);
        assert_eq!(memtable.key_count(), 2);
        assert_eq!(memtable.get(b"beta").unwrap().value, b"three");
        assert_eq!(
            memtable
                .get_all(b"beta")
                .into_iter()
                .map(|entry| (entry.lsn, entry.value))
                .collect::<Vec<_>>(),
            vec![(1, b"one".as_slice()), (3, b"three".as_slice())]
        );
        let mut entries = memtable
            .entries()
            .map(|entry| (entry.lsn, entry.key, entry.value))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.0);
        assert_eq!(
            entries,
            vec![
                (1, b"beta".as_slice(), b"one".as_slice()),
                (2, b"alpha".as_slice(), b"two".as_slice()),
                (3, b"beta".as_slice(), b"three".as_slice()),
            ]
        );
    }

    #[test]
    fn first_version_is_inline_and_owned_bytes_are_copied_into_one_block() {
        let mut memtable = Memtable::new(1, 1024);
        let mut key = b"key".to_vec();
        let mut value = b"value".to_vec();
        let key_capacity = key.capacity();
        let value_capacity = value.capacity();
        memtable
            .insert_active_owned(&mut key, lsn(7), &mut value)
            .unwrap();

        assert_eq!(
            memtable.used_bytes(),
            VERSION_BYTES + b"key".len() + b"value".len()
        );
        // The caller's vectors come back empty and keep their allocations.
        assert!(key.is_empty() && value.is_empty());
        assert_eq!(key.capacity(), key_capacity);
        assert_eq!(value.capacity(), value_capacity);
        let entry = memtable.get(b"key").unwrap();
        assert_eq!(
            (entry.key, entry.value),
            (b"key".as_slice(), b"value".as_slice())
        );
        // Key and value share one block, and a single version allocates no history vector.
        let generation = &memtable.generation;
        assert_eq!(generation.bytes.blocks.len(), 1);
        assert_eq!(generation.bytes.blocks[0], b"valuekey");
        assert_eq!(generation.rows.len(), 1);
        assert_eq!(generation.rows[0].rest.capacity(), 0);
    }

    #[test]
    fn version_descriptor_charge_matches_the_stored_record() {
        // Byte accounting charges every version this much on top of its bytes. The record holds
        // an LSN, the place of the value and the prefix length.
        assert_eq!(VERSION_BYTES, 40);
    }

    #[test]
    fn repeated_versions_reuse_the_key_and_allocate_history_lazily() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"key", 1, b"first");
        let key_pointer = memtable.get(b"key").unwrap().key.as_ptr();

        insert(&mut memtable, b"key", 2, b"second");

        assert_eq!(memtable.get(b"key").unwrap().key.as_ptr(), key_pointer);
        assert_eq!(memtable.generation.rows.len(), 1);
        assert_eq!(1 + memtable.generation.rows[0].rest.len(), 2);
        // The key's bytes were stored once.
        assert_eq!(memtable.generation.bytes.blocks[0], b"firstkeysecond");
        assert_eq!(memtable.key_count(), 1);
        assert_eq!(
            memtable.used_bytes(),
            VERSION_BYTES + b"key".len() + b"first".len() + VERSION_BYTES + b"second".len()
        );
    }

    #[test]
    fn prefix_entries_retain_components_and_full_views() {
        let mut memtable = Memtable::new(1, 1024);
        memtable
            .insert_prefix(b"K1", b"X1", lsn(1), b"V1Y1")
            .unwrap();

        assert_eq!(
            memtable.used_bytes(),
            VERSION_BYTES + b"K1X1".len() + b"V1Y1".len()
        );
        let entry = memtable.get(b"K1X1").unwrap();
        assert_eq!(entry.key, b"K1X1");
        assert_eq!(entry.value, b"V1Y1");
        assert_eq!(entry.key_prefix(), b"K1");
        assert_eq!(entry.key_suffix(), b"X1");
    }

    #[test]
    fn plain_and_prefix_entries_can_mix() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"A", 1, b"plain");
        memtable.insert_prefix(b"K", b"X", lsn(2), b"VY").unwrap();

        let plain = memtable.get(b"A").unwrap();
        assert_eq!(plain.value, b"plain");
        assert!(plain.key_prefix().is_empty());
        let prefixed = memtable.get(b"KX").unwrap();
        assert_eq!(prefixed.value, b"VY");
        assert_eq!(prefixed.key_prefix(), b"K");
        assert!(matches!(
            memtable.insert_prefix(b"", b"KX", lsn(3), b"VY"),
            Err(Error::InvalidTable(_))
        ));
    }

    #[test]
    fn latest_insert_chooses_the_keys_flush_encoding() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"KX", 1, b"plain");
        memtable
            .insert_prefix(b"K", b"X", lsn(2), b"Vprefixed")
            .unwrap();
        let entry = memtable.get(b"KX").unwrap();
        assert_eq!(entry.key_prefix(), b"K");
        assert_eq!(entry.value, b"Vprefixed");

        insert(&mut memtable, b"KX", 3, b"plain-again");
        let entry = memtable.get(b"KX").unwrap();
        assert!(entry.key_prefix().is_empty());
        assert_eq!(entry.value, b"plain-again");
    }

    #[test]
    fn rollover_and_recycling_preserve_mixed_entries() {
        let mut active = Memtable::new(1, 1024);
        insert(&mut active, b"A", 1, b"plain");
        active.insert_prefix(b"K1", b"X1", lsn(2), b"V1Y1").unwrap();
        let frozen = active.rollover(2).unwrap();
        assert_eq!(frozen.get(b"A").unwrap().value, b"plain");
        assert_eq!(frozen.get(b"K1X1").unwrap().value, b"V1Y1");

        let recycled = frozen.recycle(3).unwrap();
        assert!(recycled.is_empty());
        active.rollover_into(recycled).unwrap();
        insert(&mut active, b"B", 3, b"next");
    }

    #[test]
    fn mixed_generation_has_one_flush() {
        let directory = tempfile::tempdir().unwrap();
        let mut active = Memtable::new(1, 2048);
        insert(&mut active, b"A", 1, b"plain");
        active.insert_prefix(b"K1", b"X2", lsn(2), b"V2Y2").unwrap();
        active.insert_prefix(b"K1", b"X1", lsn(3), b"V1Y1").unwrap();
        let frozen = active.rollover(2).unwrap();

        let meta = frozen
            .flush(directory.path(), "mixed.sst", 8, 0, "patch-v1", None)
            .unwrap();
        let reader = TableReader::open_patch(directory.path(), &meta, "patch-v1").unwrap();
        assert_eq!(
            reader.get_patches(b"A").unwrap(),
            vec![(lsn(1), b"plain".to_vec())]
        );
        assert_eq!(
            reader.get_patches(b"K1X1").unwrap(),
            vec![(lsn(3), b"V1Y1".to_vec())]
        );
        assert_eq!(
            reader.get_patches(b"K1X2").unwrap(),
            vec![(lsn(2), b"V2Y2".to_vec())]
        );
    }

    #[test]
    fn flush_records_the_global_operand_floor() {
        let directory = tempfile::tempdir().unwrap();
        // Operands whose value starts with `L` depend on global state at their own LSN, unless
        // the same key also holds a `P` operand in this patch.
        let classify: OperandFloorFn = Arc::new(|operands: &[(StrataLsn, &[u8])]| {
            if operands
                .iter()
                .any(|(_, value)| value.first() == Some(&b'P'))
            {
                return Ok(None);
            }
            Ok(operands
                .iter()
                .filter(|(_, value)| value.first() == Some(&b'L'))
                .map(|(lsn, _)| *lsn)
                .min())
        });

        let mut active = Memtable::new(1, 2048);
        insert(&mut active, b"A", 1, b"plain");
        insert(&mut active, b"B", 2, b"L-extend");
        insert(&mut active, b"C", 3, b"L-extend");
        insert(&mut active, b"C", 4, b"P-put");
        let frozen = active.rollover(2).unwrap();
        let meta = frozen
            .flush(
                directory.path(),
                "mixed.sst",
                8,
                0,
                "patch-v1",
                Some(&classify),
            )
            .unwrap();
        assert_eq!(meta.min_lsn, Some(lsn(1)));
        // B's extension counts; C's does not because C is put in the same patch.
        assert_eq!(meta.global_operand_floor, OperandFloor::At(lsn(2)));

        let mut active = Memtable::new(3, 2048);
        insert(&mut active, b"A", 5, b"plain");
        insert(&mut active, b"C", 6, b"L-extend");
        insert(&mut active, b"C", 7, b"P-put");
        let frozen = active.rollover(4).unwrap();
        let meta = frozen
            .flush(
                directory.path(),
                "plain.sst",
                9,
                0,
                "patch-v1",
                Some(&classify),
            )
            .unwrap();
        assert_eq!(meta.global_operand_floor, OperandFloor::None);

        let mut active = Memtable::new(5, 2048);
        insert(&mut active, b"B", 8, b"L-extend");
        let frozen = active.rollover(6).unwrap();
        let meta = frozen
            .flush(
                directory.path(),
                "unclassified.sst",
                10,
                0,
                "patch-v1",
                None,
            )
            .unwrap();
        assert_eq!(meta.global_operand_floor, OperandFloor::Unknown);
    }

    #[test]
    fn rollover_freezes_the_key_version_map() {
        let mut active = Memtable::new(10, 1024);
        insert(&mut active, b"key", 4, b"value");

        let frozen = active.rollover(11).unwrap();

        assert_eq!(active.generation(), 11);
        assert!(active.is_empty());
        assert_eq!(frozen.generation(), 10);
        assert_eq!(frozen.get(b"key").unwrap().value, b"value");
    }

    #[test]
    fn frozen_latest_entries_are_sorted_and_deduplicated() {
        let mut active = Memtable::new(1, 1024);
        insert(&mut active, b"z", 1, b"old");
        insert(&mut active, b"a", 2, b"first");
        insert(&mut active, b"z", 3, b"new");
        let frozen = active.rollover(2).unwrap();

        assert_eq!(
            frozen
                .latest_entries_sorted()
                .into_iter()
                .map(|entry| (entry.key, entry.value, entry.lsn))
                .collect::<Vec<_>>(),
            vec![
                (b"a".as_slice(), b"first".as_slice(), 2),
                (b"z".as_slice(), b"new".as_slice(), 3),
            ]
        );
    }

    #[test]
    fn frozen_generation_preserves_every_patch_per_key() {
        let directory = tempfile::tempdir().unwrap();
        let mut active = Memtable::new(1, 1024);
        insert(&mut active, b"z", 1, b"old");
        insert(&mut active, b"a", 2, b"first");
        insert(&mut active, b"z", 3, b"new");
        let frozen = active.rollover(2).unwrap();

        let meta = frozen
            .flush(directory.path(), "memtable.sst", 9, 0, "key-ref-v1", None)
            .unwrap();
        let reader = TableReader::open_patch(directory.path(), &meta, "key-ref-v1").unwrap();

        assert_eq!(
            reader.get_patches(b"a").unwrap(),
            vec![(lsn(2), b"first".to_vec())]
        );
        assert_eq!(
            reader.get_patches(b"z").unwrap(),
            vec![(lsn(1), b"old".to_vec()), (lsn(3), b"new".to_vec()),]
        );
    }

    #[test]
    fn full_generation_is_unchanged_and_reports_backpressure() {
        let first_len = VERSION_BYTES + b"a".len() + b"one".len();
        let mut memtable = Memtable::new(3, first_len);
        insert(&mut memtable, b"a", 1, b"one");

        let error = memtable.insert(b"b", lsn(2), b"two").unwrap_err();

        assert!(matches!(
            error,
            Error::MemtableFull {
                generation: 3,
                used,
                ..
            } if used == first_len
        ));
        assert_eq!(memtable.entry_count(), 1);
        assert!(memtable.get(b"b").is_none());
    }

    #[test]
    fn oversized_entry_is_distinct_from_a_full_generation() {
        let mut memtable = Memtable::new(3, VERSION_BYTES);

        assert!(matches!(
            memtable.insert(b"a", lsn(1), b"value"),
            Err(Error::MemtableEntryTooLarge { .. })
        ));
        assert!(memtable.is_empty());
    }

    #[test]
    fn lsns_must_increase_in_write_order() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"a", 2, b"value");

        assert!(matches!(
            memtable.insert(b"b", lsn(2), b"value"),
            Err(Error::MemtableLsnOutOfOrder { .. })
        ));
        assert!(matches!(
            memtable.insert(b"b", lsn(1), b"value"),
            Err(Error::MemtableLsnOutOfOrder { .. })
        ));
    }

    #[test]
    fn empty_value_can_encode_a_caller_defined_tombstone() {
        let mut memtable = Memtable::new(1, 1024);

        insert(&mut memtable, b"deleted", 1, b"");

        assert_eq!(memtable.get(b"deleted").unwrap().value, b"");
    }

    #[test]
    fn rollover_sizes_the_next_key_map_for_the_outgoing_key_count() {
        let mut active = Memtable::new(1, 1 << 20);
        assert_eq!(active.generation.index.capacity(), 0);
        assert_eq!(active.generation.rows.capacity(), 0);
        for index in 0..1000_u64 {
            insert(
                &mut active,
                format!("key-{index:04}").as_bytes(),
                index + 1,
                b"value",
            );
        }

        let frozen = active.rollover(2).unwrap();
        assert_eq!(frozen.key_count(), 1000);
        let sized = active.generation.index.capacity();
        let rows_sized = active.generation.rows.capacity();
        assert!(
            sized >= 1000 && rows_sized >= 1000,
            "the next generation was sized for {sized} keys and {rows_sized} rows"
        );

        // Filling the next generation to the same key count grows neither the key table nor the
        // row vector, so no insert pays for moving the earlier entries.
        for index in 0..1000_u64 {
            insert(
                &mut active,
                format!("next-{index:04}").as_bytes(),
                2000 + index,
                b"value",
            );
            assert_eq!(active.generation.index.capacity(), sized);
            assert_eq!(active.generation.rows.capacity(), rows_sized);
        }

        // An empty outgoing generation asks for nothing.
        let mut empty = Memtable::new(1, 1024);
        empty.rollover(2).unwrap();
        assert_eq!(empty.generation.index.capacity(), 0);
        assert_eq!(empty.generation.rows.capacity(), 0);
    }

    #[test]
    fn a_generation_keeps_its_bytes_in_a_few_blocks() {
        // 20,000 keys with a value each: 40,000 byte strings, about 0.5 MiB.
        let mut memtable = Memtable::new(1, 1 << 30);
        for index in 0..20_000_u64 {
            insert(
                &mut memtable,
                format!("key-{index:08}").as_bytes(),
                index + 1,
                format!("value-{index:08}").as_bytes(),
            );
        }
        let blocks = &memtable.generation.bytes.blocks;
        assert!(blocks.len() <= 3, "{} blocks", blocks.len());
        // No block grew past the capacity it was created with, so nothing was ever moved.
        assert!(blocks.iter().all(|block| block.capacity() == BLOCK_BYTES));
        for index in (0..20_000_u64).step_by(997) {
            let entry = memtable.get(format!("key-{index:08}").as_bytes()).unwrap();
            assert_eq!(entry.value, format!("value-{index:08}").as_bytes());
            assert_eq!(entry.lsn, index + 1);
        }
        assert_eq!(memtable.entries().count(), 20_000);
    }

    #[test]
    fn bytes_larger_than_a_block_get_their_own_block() {
        let mut memtable = Memtable::new(1, 1 << 30);
        let large = vec![7_u8; BLOCK_BYTES + 1];
        insert(&mut memtable, b"before", 1, b"small");
        insert(&mut memtable, b"large", 2, &large);
        insert(&mut memtable, b"after", 3, b"small again");

        assert_eq!(memtable.get(b"before").unwrap().value, b"small");
        assert_eq!(memtable.get(b"large").unwrap().value, large.as_slice());
        assert_eq!(memtable.get(b"after").unwrap().value, b"small again");
        let blocks = &memtable.generation.bytes.blocks;
        assert!(blocks.iter().any(|block| block.len() == BLOCK_BYTES + 1));
        // Earlier blocks are not reallocated to make room.
        assert!(blocks.iter().all(|block| block.len() <= block.capacity()));
        assert_eq!(blocks[0].capacity(), BLOCK_BYTES);
    }

    #[test]
    fn keys_with_the_same_hash_stay_separate() {
        let mut memtable = Memtable::new(1, 1 << 20);
        memtable.generation.collide_all_keys = true;
        insert(&mut memtable, b"alpha", 1, b"a1");
        insert(&mut memtable, b"beta", 2, b"b1");
        insert(&mut memtable, b"gamma", 3, b"g1");
        insert(&mut memtable, b"beta", 4, b"b2");
        insert(&mut memtable, b"alpha", 5, b"a2");
        insert(&mut memtable, b"gamma", 6, b"g2");
        insert(&mut memtable, b"", 7, b"empty key");

        // One hash, one table entry, four rows chained behind it.
        assert_eq!(memtable.generation.index.len(), 1);
        assert_eq!(memtable.key_count(), 4);
        assert_eq!(memtable.entry_count(), 7);
        assert_eq!(memtable.get(b"alpha").unwrap().value, b"a2");
        assert_eq!(memtable.get(b"beta").unwrap().value, b"b2");
        assert_eq!(memtable.get(b"gamma").unwrap().value, b"g2");
        assert_eq!(memtable.get(b"").unwrap().value, b"empty key");
        assert!(memtable.get(b"delta").is_none());
        assert!(memtable.get_all(b"delta").is_empty());
        assert_eq!(
            memtable
                .get_all(b"beta")
                .into_iter()
                .map(|entry| (entry.lsn, entry.value))
                .collect::<Vec<_>>(),
            vec![(2, b"b1".as_slice()), (4, b"b2".as_slice())]
        );
        // A key that is new to the generation is charged for its bytes, a known one is not.
        assert_eq!(
            memtable.used_bytes(),
            7 * VERSION_BYTES + b"alphabetagamma".len() + 6 * 2 + b"empty key".len()
        );

        let frozen = memtable.rollover(2).unwrap();
        assert_eq!(frozen.entry_at(b"gamma", 0).value, b"g1");
        assert_eq!(frozen.entry_at(b"gamma", 1).value, b"g2");
        assert_eq!(
            frozen
                .latest_entries_sorted()
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (b"".as_slice(), b"empty key".as_slice()),
                (b"alpha".as_slice(), b"a2".as_slice()),
                (b"beta".as_slice(), b"b2".as_slice()),
                (b"gamma".as_slice(), b"g2".as_slice()),
            ]
        );
        let mut indexed = frozen
            .indexed_entries()
            .map(|(key, version, lsn)| (lsn, key.to_vec(), version))
            .collect::<Vec<_>>();
        indexed.sort_unstable();
        assert_eq!(
            indexed,
            vec![
                (1, b"alpha".to_vec(), 0),
                (2, b"beta".to_vec(), 0),
                (3, b"gamma".to_vec(), 0),
                (4, b"beta".to_vec(), 1),
                (5, b"alpha".to_vec(), 1),
                (6, b"gamma".to_vec(), 1),
                (7, b"".to_vec(), 0),
            ]
        );
    }

    #[test]
    fn recycled_generation_reuses_its_byte_blocks() {
        let mut active = Memtable::new(1, 1 << 30);
        let value = vec![1_u8; 1000];
        for index in 0..600_u64 {
            insert(
                &mut active,
                format!("k{index}").as_bytes(),
                index + 1,
                &value,
            );
        }
        let frozen = active.rollover(2).unwrap();
        let blocks = frozen.generation.bytes.blocks.len();
        assert!(blocks >= 2, "{blocks} blocks");
        let first_block = frozen.generation.bytes.blocks[0].as_ptr();

        let mut recycled = frozen.recycle(3).unwrap();
        assert!(recycled.is_empty());
        assert_eq!(recycled.generation.bytes.blocks.len(), blocks);
        assert!(recycled.generation.bytes.blocks.iter().all(Vec::is_empty));
        for index in 0..600_u64 {
            insert(
                &mut recycled,
                format!("n{index}").as_bytes(),
                1000 + index,
                &value,
            );
        }
        // The same blocks were filled again, in order, and no new one was needed.
        assert_eq!(recycled.generation.bytes.blocks.len(), blocks);
        assert_eq!(recycled.generation.bytes.blocks[0].as_ptr(), first_block);
        assert!(recycled.get(b"k0").is_none());
        assert_eq!(recycled.get(b"n599").unwrap().value, value.as_slice());
    }

    #[test]
    fn retired_generation_can_be_recycled_without_retaining_state() {
        let mut active = Memtable::new(1, 1024);
        insert(&mut active, b"old", 1, b"value");
        let frozen = active.rollover(2).unwrap();

        let recycled = frozen.recycle(3).unwrap();

        assert_eq!(recycled.generation(), 3);
        assert_eq!(recycled.capacity(), 1024);
        assert!(recycled.is_empty());
        assert!(recycled.get(b"old").is_none());

        let frozen = active.rollover_into(recycled).unwrap();
        assert_eq!(active.generation(), 3);
        insert(&mut active, b"new", 2, b"value");
        assert_eq!(active.get(b"new").unwrap().value, b"value");
        assert_eq!(frozen.generation(), 2);
    }

    #[test]
    fn rollover_policy_uses_key_count_or_elapsed_time() {
        let policy =
            MemtableRolloverPolicy::new(NonZeroUsize::new(2).unwrap(), Duration::from_secs(10));

        assert!(!policy.should_rollover(1, Duration::from_secs(9)));
        assert!(policy.should_rollover(2, Duration::from_secs(9)));
        assert!(policy.should_rollover(1, Duration::from_secs(10)));
    }

    #[test]
    fn insert_rolls_before_mutation_when_key_limit_is_reached() {
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::new(2).unwrap(), Duration::MAX);
        let mut memtable = Memtable::with_rollover_policy(4, 1024, policy);
        insert(&mut memtable, b"a", 1, b"one");
        insert(&mut memtable, b"b", 2, b"two");

        let frozen = memtable
            .insert(b"c", lsn(3), b"three")
            .unwrap()
            .expect("key limit should roll the active generation");

        assert_eq!(frozen.generation(), 4);
        assert_eq!(frozen.key_count(), 2);
        assert!(frozen.get(b"c").is_none());
        assert_eq!(memtable.generation(), 5);
        assert_eq!(memtable.key_count(), 1);
        assert_eq!(memtable.get(b"c").unwrap().value, b"three");
    }

    #[test]
    fn policy_rollover_charges_an_existing_key_again_in_the_new_generation() {
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::MIN, Duration::MAX);
        let next_len = VERSION_BYTES + b"key".len() + b"second".len();
        let mut memtable = Memtable::with_rollover_policy(4, next_len, policy);
        insert(&mut memtable, b"key", 1, b"first");

        let frozen = memtable
            .insert(b"key", lsn(2), b"second")
            .unwrap()
            .expect("key policy should roll the generation");

        assert_eq!(frozen.get(b"key").unwrap().value, b"first");
        assert_eq!(memtable.get(b"key").unwrap().value, b"second");
        assert_eq!(memtable.used_bytes(), next_len);
    }

    #[test]
    fn age_policy_does_not_roll_an_empty_generation() {
        let policy =
            MemtableRolloverPolicy::new(NonZeroUsize::new(usize::MAX).unwrap(), Duration::ZERO);
        let mut memtable = Memtable::with_rollover_policy(8, 1024, policy);

        assert!(memtable.insert(b"a", lsn(1), b"one").unwrap().is_none());
        let frozen = memtable
            .insert(b"b", lsn(2), b"two")
            .unwrap()
            .expect("elapsed-time limit should roll a non-empty generation");

        assert_eq!(frozen.generation(), 8);
        assert_eq!(memtable.generation(), 9);
        assert_eq!(memtable.get(b"b").unwrap().value, b"two");
    }

    #[test]
    fn rejected_insert_does_not_apply_a_pending_policy_rollover() {
        let policy = MemtableRolloverPolicy::new(NonZeroUsize::new(1).unwrap(), Duration::MAX);
        let mut memtable = Memtable::with_rollover_policy(2, 1024, policy);
        insert(&mut memtable, b"a", 2, b"value");

        assert!(matches!(
            memtable.insert(b"b", lsn(1), b"value"),
            Err(Error::MemtableLsnOutOfOrder { .. })
        ));
        assert_eq!(memtable.generation(), 2);
        assert_eq!(memtable.key_count(), 1);
    }
}
