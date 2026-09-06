//! Bounded memtable storing each key's versions directly in a hash map.
//!
//! The first version is embedded in the map value. A key allocates a history vector only after its
//! second mutation, and values already owned by the write pipeline move into the generation without
//! another byte allocation or copy. Rollover freezes the complete map and installs a fresh one.

use std::{
    collections::{HashMap, hash_map},
    mem,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use crate::{Error, OperandFloor, OperandFloorFn, Result, StrataLsn, TableMeta, TableWriter};

/// Default logical byte capacity of one memtable generation.
pub const DEFAULT_MEMTABLE_BUFFER_BYTES: usize = 1 << 30;

// Logical accounting includes the directly stored version descriptor and its owned bytes. A key's
// bytes are charged only for its first version. Hash-table buckets and allocator rounding remain
// implementation overhead, just as the old buffer-backed representation did not charge its index.
pub(crate) const VERSION_BYTES: usize = mem::size_of::<Version>();

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

/// Immutable key/version map produced by one rollover.
#[derive(Debug)]
pub struct FrozenMemtable {
    generation: Generation,
}

#[derive(Debug)]
struct Generation {
    id: u64,
    capacity: usize,
    rows: HashMap<Vec<u8>, KeyHistory>,
    used_bytes: usize,
    entry_count: usize,
    first_lsn: Option<StrataLsn>,
    last_lsn: Option<StrataLsn>,
    started_at: Instant,
}

#[derive(Debug)]
struct KeyHistory {
    // Keeping the common single-version case inline avoids one Vec allocation per unique key.
    first: Version,
    rest: Vec<Version>,
}

#[derive(Debug)]
struct Version {
    lsn: StrataLsn,
    value: Vec<u8>,
    key_prefix_len: usize,
}

impl KeyHistory {
    fn new(first: Version) -> Self {
        Self {
            first,
            rest: Vec::new(),
        }
    }

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

    fn push(&mut self, version: Version) {
        self.rest.push(version);
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

    pub fn first_lsn(&self) -> Option<StrataLsn> {
        self.generation.first_lsn
    }

    pub fn last_lsn(&self) -> Option<StrataLsn> {
        self.generation.last_lsn
    }

    /// Stores one mutation and makes it the latest value for `key`.
    ///
    /// The configured rollover policy is evaluated before validation or mutation. When it fires,
    /// the old map is returned and the entry is inserted into the new active
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

        let mut key = key.to_vec();
        let mut value = value.to_vec();
        let mut required = self.generation.validate_insert(&key, lsn, &value)?;
        if should_rollover {
            required = self.generation.required_bytes_for_new_key(&key, &value)?;
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
            .insert_owned(&mut key, lsn, &mut value, 0, required);
        Ok(frozen)
    }

    pub(crate) fn insert_active(&mut self, key: &[u8], lsn: StrataLsn, value: &[u8]) -> Result<()> {
        let mut key = key.to_vec();
        let mut value = value.to_vec();
        self.insert_active_owned(&mut key, lsn, &mut value)
    }

    /// Inserts engine-owned bytes without allocating or copying their key or value again.
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
        let (mut required, mut full_key) = self
            .generation
            .validate_prefix_insert(key_prefix, key_suffix, lsn, value)?;
        let mut value = value.to_vec();
        if should_rollover {
            required = self
                .generation
                .required_bytes_for_new_key(&full_key, &value)?;
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
            .insert_owned(&mut full_key, lsn, &mut value, key_prefix.len(), required);
        Ok(frozen)
    }

    pub(crate) fn insert_prefix_active(
        &mut self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
    ) -> Result<()> {
        let mut full_key = Vec::with_capacity(key_prefix.len().saturating_add(key_suffix.len()));
        full_key.extend_from_slice(key_prefix);
        full_key.extend_from_slice(key_suffix);
        let mut value = value.to_vec();
        self.insert_prefix_active_owned(&mut full_key, key_prefix.len(), lsn, &mut value)
    }

    /// Inserts an engine-owned full key and value while retaining the key's explicit prefix split.
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

    /// Iterates every mutation, grouped by hash-map key and then by increasing LSN.
    pub fn entries(&self) -> MemtableEntries<'_> {
        self.generation.entries()
    }

    /// Freezes the active key/version map and installs an empty generation.
    pub fn rollover(&mut self, next_generation: u64) -> Result<FrozenMemtable> {
        self.rollover_into(Self::new(next_generation, self.capacity()))
    }

    /// Freezes the active generation and installs an empty, possibly recycled replacement.
    ///
    /// A small caller-owned pool can recycle published generations and pass them here, retaining
    /// the hash-table allocation instead of allocating it on every rollover.
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

    /// Iterates every mutation, grouped by hash-map key and then by increasing LSN.
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
        self.generation.rows.iter().flat_map(|(key, history)| {
            history
                .versions()
                .enumerate()
                .map(move |(version, value)| (key.as_slice(), version, value.lsn))
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
        let mut rows = self.generation.rows.iter().collect::<Vec<_>>();
        rows.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut floor = OperandFloor::None;
        for (key, history) in rows {
            if let Some(classify) = classify_operands {
                let operands = history
                    .versions()
                    .map(|version| (version.lsn, version.value.as_slice()))
                    .collect::<Vec<_>>();
                if let Some(lsn) = classify(&operands)? {
                    floor = floor.merge(OperandFloor::At(lsn));
                }
            }
            for version in history.versions() {
                if version.key_prefix_len == 0 {
                    writer.add_patch(key, version.lsn, &version.value)?;
                } else {
                    writer.add_prefix_patch(
                        &key[..version.key_prefix_len],
                        &key[version.key_prefix_len..],
                        version.lsn,
                        &version.value,
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
        Self {
            id,
            capacity,
            rows: HashMap::new(),
            used_bytes: 0,
            entry_count: 0,
            first_lsn: None,
            last_lsn: None,
            started_at: Instant::now(),
        }
    }

    fn reset(&mut self, id: u64) {
        self.id = id;
        // `clear` drops key/value allocations but retains the hash-table buckets for the next
        // generation. Values on the engine path were already allocated before insertion.
        self.rows.clear();
        self.used_bytes = 0;
        self.entry_count = 0;
        self.first_lsn = None;
        self.last_lsn = None;
        self.started_at = Instant::now();
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
        let key_bytes = if self.rows.contains_key(key) {
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
        let key_bytes = if self.rows.contains_key(key) {
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

    fn insert_owned(
        &mut self,
        key: &mut Vec<u8>,
        lsn: StrataLsn,
        value: &mut Vec<u8>,
        key_prefix_len: usize,
        required: usize,
    ) {
        let version = Version {
            lsn,
            value: mem::take(value),
            key_prefix_len,
        };
        if let Some(history) = self.rows.get_mut(key.as_slice()) {
            history.push(version);
        } else {
            self.rows.insert(mem::take(key), KeyHistory::new(version));
        }
        self.used_bytes += required;
        self.entry_count += 1;
        self.first_lsn.get_or_insert(lsn);
        self.last_lsn = Some(lsn);
    }

    fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        let (stored_key, history) = self.rows.get_key_value(key)?;
        Some(Self::entry(stored_key, history.latest()))
    }

    fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        self.rows
            .get_key_value(key)
            .map_or_else(Vec::new, |(stored_key, history)| {
                history
                    .versions()
                    .map(|version| Self::entry(stored_key, version))
                    .collect()
            })
    }

    fn entries(&self) -> MemtableEntries<'_> {
        MemtableEntries {
            rows: self.rows.iter(),
            current: None,
        }
    }

    fn latest_entries_sorted(&self) -> Vec<MemtableEntry<'_>> {
        let mut entries = self
            .rows
            .iter()
            .map(|(key, history)| Self::entry(key, history.latest()))
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.key.cmp(right.key));
        entries
    }

    fn entry_at(&self, key: &[u8], version: usize) -> Option<MemtableEntry<'_>> {
        let (stored_key, history) = self.rows.get_key_value(key)?;
        Some(Self::entry(stored_key, history.get(version)?))
    }

    fn entry<'a>(key: &'a [u8], version: &'a Version) -> MemtableEntry<'a> {
        MemtableEntry {
            lsn: version.lsn,
            key,
            value: &version.value,
            key_prefix_len: version.key_prefix_len,
        }
    }
}

struct KeyVersions<'a> {
    key: &'a [u8],
    history: &'a KeyHistory,
    next: usize,
}

/// Borrowed iterator over one stable memtable generation.
pub struct MemtableEntries<'a> {
    rows: hash_map::Iter<'a, Vec<u8>, KeyHistory>,
    current: Option<KeyVersions<'a>>,
}

impl<'a> Iterator for MemtableEntries<'a> {
    type Item = MemtableEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current
                && let Some(version) = current.history.get(current.next)
            {
                current.next += 1;
                return Some(Generation::entry(current.key, version));
            }
            let (key, history) = self.rows.next()?;
            self.current = Some(KeyVersions {
                key,
                history,
                next: 0,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use crate::{Error, OperandFloor, OperandFloorFn, StrataLsn, TableReader};

    use super::{Memtable, MemtableRolloverPolicy, VERSION_BYTES};

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
    fn first_version_is_inline_and_owned_bytes_move_without_a_copy() {
        let mut memtable = Memtable::new(1, 1024);
        let mut key = b"key".to_vec();
        let mut value = b"value".to_vec();
        let key_pointer = key.as_ptr();
        let value_pointer = value.as_ptr();
        memtable
            .insert_active_owned(&mut key, lsn(7), &mut value)
            .unwrap();

        assert_eq!(
            memtable.used_bytes(),
            VERSION_BYTES + b"key".len() + b"value".len()
        );
        let (stored_key, history) = memtable
            .generation
            .rows
            .get_key_value(b"key".as_slice())
            .unwrap();
        assert_eq!(stored_key.as_ptr(), key_pointer);
        assert_eq!(history.first.value.as_ptr(), value_pointer);
        assert_eq!(history.rest.capacity(), 0);
    }

    #[test]
    fn repeated_versions_reuse_the_key_and_allocate_history_lazily() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"key", 1, b"first");
        let key_pointer = memtable
            .generation
            .rows
            .get_key_value(b"key".as_slice())
            .unwrap()
            .0
            .as_ptr();

        insert(&mut memtable, b"key", 2, b"second");

        let (stored_key, history) = memtable
            .generation
            .rows
            .get_key_value(b"key".as_slice())
            .unwrap();
        assert_eq!(stored_key.as_ptr(), key_pointer);
        assert_eq!(1 + history.rest.len(), 2);
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
