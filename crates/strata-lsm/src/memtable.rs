//! Bounded memtable backed by one append-only byte buffer.
//!
//! The active buffer and its key index form one generation. Rollover moves both into a
//! [`FrozenMemtable`] and installs a fresh generation, so an index offset can never outlive the
//! bytes it names. Each buffered entry links to the previous mutation for its key, so merge reads
//! walk only that key's history while the index still stores one offset per key.

use std::{
    collections::HashMap,
    mem,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use crate::{Error, Result, StrataLsn, TableMeta, TableWriter};

/// Default logical byte capacity of one memtable generation.
pub const DEFAULT_MEMTABLE_BUFFER_BYTES: usize = 1 << 30;

pub(crate) const ENTRY_HEADER_BYTES: usize =
    1 + 2 * mem::size_of::<u64>() + 2 * mem::size_of::<u32>();
pub(crate) const PREFIX_ENTRY_HEADER_BYTES: usize =
    1 + 2 * mem::size_of::<u64>() + 3 * mem::size_of::<u32>();
const NO_PREVIOUS_OFFSET: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryEncoding {
    Plain = 0,
    Prefix = 1,
}

impl EntryEncoding {
    fn from_byte(byte: u8) -> Self {
        match byte {
            0 => Self::Plain,
            1 => Self::Prefix,
            _ => unreachable!("memtable contains only internally encoded entries"),
        }
    }
}

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
    previous_offset: Option<usize>,
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

/// Stable append position used by a caller-owned log consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemtableCursor {
    generation: u64,
    offset: usize,
}

impl MemtableCursor {
    pub fn generation(self) -> u64 {
        self.generation
    }

    pub fn offset(self) -> usize {
        self.offset
    }
}

/// Mutable append-only memtable generation.
///
/// This type deliberately contains no internal synchronization. Strata's serialized writer owns
/// mutation order; callers may place the memtable behind their read-view synchronization.
#[derive(Debug)]
pub struct Memtable {
    generation: Generation,
    rollover_policy: Option<MemtableRolloverPolicy>,
}

/// Immutable buffer and latest-key index produced by one rollover.
#[derive(Debug)]
pub struct FrozenMemtable {
    generation: Generation,
}

#[derive(Debug)]
struct Generation {
    id: u64,
    capacity: usize,
    buffer: Vec<u8>,
    index: HashMap<Vec<u8>, usize>,
    entry_count: usize,
    first_lsn: Option<StrataLsn>,
    last_lsn: Option<StrataLsn>,
    started_at: Instant,
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
        self.generation.buffer.len()
    }

    pub fn remaining_bytes(&self) -> usize {
        self.capacity().saturating_sub(self.used_bytes())
    }

    pub fn entry_count(&self) -> usize {
        self.generation.entry_count
    }

    pub fn key_count(&self) -> usize {
        self.generation.index.len()
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

    /// Appends one mutation and makes it the latest value for `key`.
    ///
    /// The configured rollover policy is evaluated before validation or mutation. When it fires,
    /// the old buffer and index are returned together and the entry is appended to the new active
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

        let required = self.generation.validate_insert(key, lsn, value)?;
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
        self.generation.append(key, lsn, value, required);
        Ok(frozen)
    }

    pub(crate) fn insert_active(&mut self, key: &[u8], lsn: StrataLsn, value: &[u8]) -> Result<()> {
        let required = self.generation.validate_insert(key, lsn, value)?;
        self.generation.ensure_available(required)?;
        self.generation.append(key, lsn, value, required);
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
        let (required, full_key) = self
            .generation
            .validate_prefix_insert(key_prefix, key_suffix, lsn, value)?;
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
            .append_prefix(key_prefix, key_suffix, lsn, value, &full_key, required);
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
            .append_prefix(key_prefix, key_suffix, lsn, value, &full_key, required);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        self.generation.get(key)
    }

    pub fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        self.generation.get_all(key)
    }

    /// Iterates every mutation in append order for caller-owned log ingestion.
    pub fn entries(&self) -> MemtableEntries<'_> {
        self.generation.entries()
    }

    /// Continues append-order iteration from a cursor previously returned for this generation.
    pub fn entries_from(&self, cursor: MemtableCursor) -> Result<MemtableEntries<'_>> {
        self.generation.entries_from(cursor)
    }

    /// Freezes the active buffer and index together and installs an empty generation.
    pub fn rollover(&mut self, next_generation: u64) -> Result<FrozenMemtable> {
        self.rollover_into(Self::new(next_generation, self.capacity()))
    }

    /// Freezes the active generation and installs an empty, possibly recycled replacement.
    ///
    /// A small caller-owned pool can recycle published generations and pass them here, retaining
    /// the large buffer and hash-table allocations instead of allocating them on every rollover.
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
        self.generation.buffer.len()
    }

    pub fn entry_count(&self) -> usize {
        self.generation.entry_count
    }

    pub fn key_count(&self) -> usize {
        self.generation.index.len()
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

    /// Iterates every mutation in append order for finishing log ingestion after rollover.
    pub fn entries(&self) -> MemtableEntries<'_> {
        self.generation.entries()
    }

    /// Finishes append-order log ingestion from a cursor captured before rollover.
    pub fn entries_from(&self, cursor: MemtableCursor) -> Result<MemtableEntries<'_>> {
        self.generation.entries_from(cursor)
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
    ) -> Result<TableMeta> {
        if self.is_empty() {
            return Err(Error::InvalidTable(
                "cannot flush an empty memtable".to_owned(),
            ));
        }
        let mut writer =
            TableWriter::create_patch(root, relative_path, id, partition, patch_format_id)?;
        let mut entries = self.entries().collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| {
            left.key
                .cmp(right.key)
                .then_with(|| left.lsn.cmp(&right.lsn))
        });
        for entry in entries {
            if entry.key_prefix().is_empty() {
                writer.add_patch(entry.key, entry.lsn, entry.value)?;
            } else {
                writer.add_prefix_patch(
                    entry.key_prefix(),
                    entry.key_suffix(),
                    entry.lsn,
                    entry.value,
                )?;
            }
        }
        writer.finish()
    }

    /// Clears a retired generation while retaining its allocations for reuse.
    ///
    /// Callers must invoke this only after the corresponding immutable tables are published and no
    /// reader or log consumer can still hold a reference to the frozen generation.
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
            buffer: Vec::new(),
            index: HashMap::new(),
            entry_count: 0,
            first_lsn: None,
            last_lsn: None,
            started_at: Instant::now(),
        }
    }

    fn reset(&mut self, id: u64) {
        self.id = id;
        self.buffer.clear();
        self.index.clear();
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
        let required = ENTRY_HEADER_BYTES
            .checked_add(key.len())
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
        let required = PREFIX_ENTRY_HEADER_BYTES
            .checked_add(key_prefix.len())
            .and_then(|len| len.checked_add(key_suffix.len()))
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
        Ok((required, full_key))
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
        if self.buffer.len().saturating_add(required) > self.capacity {
            return Err(Error::MemtableFull {
                generation: self.id,
                capacity: self.capacity,
                used: self.buffer.len(),
                required,
            });
        }
        Ok(())
    }

    fn append(&mut self, key: &[u8], lsn: StrataLsn, value: &[u8], required: usize) {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        debug_assert_eq!(required, ENTRY_HEADER_BYTES + key.len() + value.len());
        let offset = self.buffer.len();
        let previous_offset = self
            .index
            .get(key)
            .map_or(NO_PREVIOUS_OFFSET, |offset| *offset as u64);
        self.buffer.push(EntryEncoding::Plain as u8);
        self.buffer.extend_from_slice(&lsn.to_le_bytes());
        self.buffer
            .extend_from_slice(&previous_offset.to_le_bytes());
        self.buffer.extend_from_slice(&key_len.to_le_bytes());
        self.buffer.extend_from_slice(&value_len.to_le_bytes());
        self.buffer.extend_from_slice(key);
        self.buffer.extend_from_slice(value);
        self.index.insert(key.to_vec(), offset);
        self.entry_count += 1;
        self.first_lsn.get_or_insert(lsn);
        self.last_lsn = Some(lsn);
    }

    fn append_prefix(
        &mut self,
        key_prefix: &[u8],
        key_suffix: &[u8],
        lsn: StrataLsn,
        value: &[u8],
        full_key: &[u8],
        required: usize,
    ) {
        debug_assert_eq!(
            required,
            PREFIX_ENTRY_HEADER_BYTES + key_prefix.len() + key_suffix.len() + value.len()
        );
        let offset = self.buffer.len();
        let previous_offset = self
            .index
            .get(full_key)
            .map_or(NO_PREVIOUS_OFFSET, |offset| *offset as u64);
        self.buffer.push(EntryEncoding::Prefix as u8);
        self.buffer.extend_from_slice(&lsn.to_le_bytes());
        self.buffer
            .extend_from_slice(&previous_offset.to_le_bytes());
        for part in [key_prefix, key_suffix, value] {
            self.buffer
                .extend_from_slice(&(part.len() as u32).to_le_bytes());
        }
        self.buffer.extend_from_slice(key_prefix);
        self.buffer.extend_from_slice(key_suffix);
        self.buffer.extend_from_slice(value);
        self.index.insert(full_key.to_vec(), offset);
        self.entry_count += 1;
        self.first_lsn.get_or_insert(lsn);
        self.last_lsn = Some(lsn);
    }

    fn get(&self, key: &[u8]) -> Option<MemtableEntry<'_>> {
        self.index.get(key).map(|offset| self.entry_at(*offset))
    }

    fn get_all(&self, key: &[u8]) -> Vec<MemtableEntry<'_>> {
        let mut entries = Vec::new();
        let mut offset = self.index.get(key).copied();
        while let Some(current) = offset {
            let entry = self.entry_at(current);
            offset = entry.previous_offset;
            entries.push(entry);
        }
        entries.reverse();
        entries
    }

    fn entries(&self) -> MemtableEntries<'_> {
        MemtableEntries {
            generation: self,
            offset: 0,
        }
    }

    fn entries_from(&self, cursor: MemtableCursor) -> Result<MemtableEntries<'_>> {
        if cursor.generation != self.id {
            return Err(Error::MemtableCursorGeneration {
                generation: self.id,
                cursor_generation: cursor.generation,
            });
        }
        Ok(MemtableEntries {
            generation: self,
            offset: cursor.offset,
        })
    }

    fn latest_entries_sorted(&self) -> Vec<MemtableEntry<'_>> {
        let mut entries = self
            .index
            .values()
            .map(|offset| self.entry_at(*offset))
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.key.cmp(right.key));
        entries
    }

    fn entry_at(&self, offset: usize) -> MemtableEntry<'_> {
        let encoding = EntryEncoding::from_byte(self.buffer[offset]);
        let header_len = Self::header_bytes(encoding);
        let header = &self.buffer[offset..offset + header_len];
        let lsn = u64::from_le_bytes(header[1..9].try_into().unwrap());
        let previous_offset = u64::from_le_bytes(header[9..17].try_into().unwrap());
        let previous_offset = (previous_offset != NO_PREVIOUS_OFFSET)
            .then(|| usize::try_from(previous_offset).expect("memtable offset fits usize"));
        let (key_prefix_len, key_suffix_len, value_len) = match encoding {
            EntryEncoding::Plain => (
                0,
                u32::from_le_bytes(header[17..21].try_into().unwrap()) as usize,
                u32::from_le_bytes(header[21..25].try_into().unwrap()) as usize,
            ),
            EntryEncoding::Prefix => (
                u32::from_le_bytes(header[17..21].try_into().unwrap()) as usize,
                u32::from_le_bytes(header[21..25].try_into().unwrap()) as usize,
                u32::from_le_bytes(header[25..29].try_into().unwrap()) as usize,
            ),
        };
        let key_len = key_prefix_len + key_suffix_len;
        let key_start = offset + header_len;
        let key_end = key_start + key_len;
        let value_end = key_end + value_len;
        MemtableEntry {
            lsn,
            key: &self.buffer[key_start..key_end],
            value: &self.buffer[key_end..value_end],
            previous_offset,
            key_prefix_len,
        }
    }

    fn header_bytes(encoding: EntryEncoding) -> usize {
        match encoding {
            EntryEncoding::Plain => ENTRY_HEADER_BYTES,
            EntryEncoding::Prefix => PREFIX_ENTRY_HEADER_BYTES,
        }
    }

    fn header_bytes_at(&self, offset: usize) -> usize {
        Self::header_bytes(EntryEncoding::from_byte(self.buffer[offset]))
    }
}

/// Borrowed append-order iterator over one stable memtable generation.
pub struct MemtableEntries<'a> {
    generation: &'a Generation,
    offset: usize,
}

impl MemtableEntries<'_> {
    /// Position immediately after the last entry returned by this iterator.
    pub fn cursor(&self) -> MemtableCursor {
        MemtableCursor {
            generation: self.generation.id,
            offset: self.offset,
        }
    }
}

impl<'a> Iterator for MemtableEntries<'a> {
    type Item = MemtableEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.generation.buffer.len() {
            return None;
        }
        let entry = self.generation.entry_at(self.offset);
        self.offset +=
            self.generation.header_bytes_at(self.offset) + entry.key.len() + entry.value.len();
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use crate::{Error, StrataLsn, TableReader};

    use super::{ENTRY_HEADER_BYTES, Memtable, MemtableRolloverPolicy, PREFIX_ENTRY_HEADER_BYTES};

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
    fn append_stream_retains_mutations_while_index_tracks_latest() {
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
        assert_eq!(
            memtable
                .entries()
                .map(|entry| (entry.lsn, entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (1, b"beta".as_slice(), b"one".as_slice()),
                (2, b"alpha".as_slice(), b"two".as_slice()),
                (3, b"beta".as_slice(), b"three".as_slice()),
            ]
        );
    }

    #[test]
    fn plain_entry_has_one_byte_encoding_tag() {
        let mut memtable = Memtable::new(1, 1024);
        insert(&mut memtable, b"key", 7, b"value");

        assert_eq!(
            memtable.used_bytes(),
            ENTRY_HEADER_BYTES + b"key".len() + b"value".len()
        );
        assert_eq!(
            &memtable.generation.buffer[..ENTRY_HEADER_BYTES],
            &[
                0, // plain encoding
                7, 0, 0, 0, 0, 0, 0, 0, // LSN
                255, 255, 255, 255, 255, 255, 255, 255, // no previous offset
                3, 0, 0, 0, // key length
                5, 0, 0, 0, // value length
            ]
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
            PREFIX_ENTRY_HEADER_BYTES + b"K1X1".len() + b"V1Y1".len()
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

        let entries = memtable.entries().collect::<Vec<_>>();
        assert_eq!(entries[0].key, b"A");
        assert!(entries[0].key_prefix().is_empty());
        assert_eq!(entries[1].key, b"KX");
        assert_eq!(entries[1].key_prefix(), b"K");
        assert_eq!(memtable.get(b"A").unwrap().value, b"plain");
        assert_eq!(memtable.get(b"KX").unwrap().value, b"VY");
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
            .flush(directory.path(), "mixed.sst", 8, 0, "patch-v1")
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
    fn log_scan_resumes_from_a_generation_cursor() {
        let mut memtable = Memtable::new(7, 1024);
        insert(&mut memtable, b"a", 1, b"one");
        let cursor = {
            let mut scan = memtable.entries();
            assert_eq!(scan.next().unwrap().key, b"a");
            scan.cursor()
        };

        insert(&mut memtable, b"b", 2, b"two");

        assert_eq!(
            memtable
                .entries_from(cursor)
                .unwrap()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            vec![b"b".as_slice()]
        );
    }

    #[test]
    fn rollover_freezes_buffer_and_index_together() {
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
            .flush(directory.path(), "memtable.sst", 9, 0, "key-ref-v1")
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
        let first_len = ENTRY_HEADER_BYTES + b"a".len() + b"one".len();
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
        let mut memtable = Memtable::new(3, ENTRY_HEADER_BYTES);

        assert!(matches!(
            memtable.insert(b"a", lsn(1), b"value"),
            Err(Error::MemtableEntryTooLarge { .. })
        ));
        assert!(memtable.is_empty());
    }

    #[test]
    fn lsns_must_increase_in_append_order() {
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
