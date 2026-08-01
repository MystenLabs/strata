//! The merged, seekable read iterator over one LSM partition.

use std::sync::Arc;

use crate::{
    Error, FrozenMemtable, MergeOperator, Result, Snapshot, StrataLsn, TableMeta,
    table::TableCursor,
};

/// One row copied out of the active memtable: `(key, lsn, encoded value)`.
type ActiveRow = (Vec<u8>, StrataLsn, Vec<u8>);

/// Address of one memtable version. Frozen values stay in their pinned per-key histories; active
/// values are copied while the engine's memory lock is held.
#[derive(Clone, Copy)]
enum RowAddress {
    Frozen { source: usize, version: usize },
    Active { row: usize },
}

/// A pinned, sorted iterator over one LSM partition, bounded to `[start, end)` at creation.
///
/// Every key is materialized from three sources holding rows for it: the snapshot's base
/// SSTs, its patch SSTs, and the memtable rows captured when the iterator was created. For a
/// base row `a = 1`, an SST patch `a += 2` at lsn 4, and a memtable row `a += 3` at lsn 9,
/// `next` returns `a` exactly once, merged from all three parts in lsn order.
///
/// Cursors share the snapshot's open readers, so SST blocks are fetched through the shared
/// block cache instead of reopening files. The snapshot and the frozen memtable generations
/// are pinned for the iterator's lifetime: flushing continues normally, and a generation
/// flushed while an iterator references it stays in memory until the iterator drops.
pub struct LsmIter {
    base: Vec<TableCursor>,
    patches: Vec<TableCursor>,
    // Pinned frozen generations, plus an owned copy of the active memtable's in-range rows.
    // Frozen generations are immutable, so their rows are read in place; the active memtable
    // keeps mutating after capture, so its rows must be copied out while the engine lock is held.
    frozen: Vec<Arc<FrozenMemtable>>,
    active: Vec<ActiveRow>,
    // Keys are copied once per iterator, not once per version. The flat row index refers to those
    // keys and to version bytes in a pinned frozen generation or the owned active capture.
    memory_keys: Vec<Vec<u8>>,
    memory_index: Vec<(usize, RowAddress)>,
    memory_next: usize,
    max_lsn: StrataLsn,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    _snapshot: Arc<Snapshot>,
}

impl LsmIter {
    pub(crate) fn new(
        snapshot: Arc<Snapshot>,
        partition: u32,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        frozen: Vec<Arc<FrozenMemtable>>,
        active: Vec<ActiveRow>,
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
        let readers = snapshot
            .readers
            .get(&partition)
            .expect("validated manifest has readers for every partition");
        let overlaps = |table: &TableMeta| {
            start.is_none_or(|start| table.last_key.as_slice() >= start)
                && end.is_none_or(|end| table.first_key.as_slice() < end)
        };
        let base = partition_manifest
            .base
            .iter()
            .zip(&readers.base)
            .filter(|(table, _)| overlaps(table))
            .map(|(_, reader)| TableCursor::new(Arc::clone(reader), start))
            .collect::<Result<Vec<_>>>()?;
        let patches = partition_manifest
            .patches
            .iter()
            .zip(&readers.patches)
            .filter(|(table, _)| overlaps(table))
            .filter(|(table, _)| table.min_lsn.is_none_or(|lsn| lsn <= max_lsn))
            .map(|(_, reader)| TableCursor::new(Arc::clone(reader), start))
            .collect::<Result<Vec<_>>>()?;

        // Hash memtables are not sorted structures, so the merge needs one sorted index over every
        // visible version. Frozen rows contribute only their address; values stay in the pinned
        // maps. Building the index here, after capture, keeps this work off the engine memory lock.
        let in_range =
            |key: &[u8]| start.is_none_or(|start| key >= start) && end.is_none_or(|end| key < end);
        let mut rows = Vec::new();
        for (source, generation) in frozen.iter().enumerate() {
            for (key, version, lsn) in generation.indexed_entries() {
                if lsn <= max_lsn && in_range(key) {
                    rows.push((key, lsn, RowAddress::Frozen { source, version }));
                }
            }
        }
        for (offset, (key, lsn, _)) in active.iter().enumerate() {
            rows.push((key.as_slice(), *lsn, RowAddress::Active { row: offset }));
        }
        rows.sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
        let mut memory_keys: Vec<Vec<u8>> = Vec::new();
        let mut memory_index = Vec::with_capacity(rows.len());
        for (key, _, address) in rows {
            let key_index = match memory_keys.last() {
                Some(previous) if previous.as_slice() == key => memory_keys.len() - 1,
                _ => {
                    memory_keys.push(key.to_vec());
                    memory_keys.len() - 1
                }
            };
            memory_index.push((key_index, address));
        }

        Ok(Self {
            base,
            patches,
            frozen,
            active,
            memory_keys,
            memory_index,
            memory_next: 0,
            max_lsn,
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            _snapshot: snapshot,
        })
    }

    // Resolves one indexed memtable row to its borrowed key, lsn, and value.
    fn memory_row(&self, (key_index, address): (usize, RowAddress)) -> (&[u8], StrataLsn, &[u8]) {
        let key = self.memory_keys[key_index].as_slice();
        match address {
            RowAddress::Frozen { source, version } => {
                let entry = self.frozen[source].entry_at(key, version);
                (entry.key, entry.lsn, entry.value)
            }
            RowAddress::Active { row } => {
                let (key, lsn, value) = &self.active[row];
                (key, *lsn, value)
            }
        }
    }

    /// Repositions so the next `next` call returns the first key at or after `target`.
    ///
    /// The target is clamped to the iterator's range: seeking before `start` lands on
    /// `start`, and seeking at or past `end` leaves `next` returning `None`. Backward seeks
    /// are allowed and re-read at most one block per SST, usually straight from the block
    /// cache. With keys `a..z` and an iterator over `[c, x)`:
    ///
    /// - `seek(b"f")` then `next` returns `f`;
    /// - `seek(b"a")` then `next` returns `c`;
    /// - `seek(b"y")` then `next` returns `None`.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        let target = self
            .start
            .as_deref()
            .filter(|start| *start > target)
            .unwrap_or(target)
            .to_vec();
        for cursor in self.base.iter_mut().chain(&mut self.patches) {
            cursor.seek(Some(&target))?;
        }
        self.memory_next = self
            .memory_index
            .partition_point(|&address| self.memory_row(address).0 < target.as_slice());
        Ok(())
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
                .chain(
                    self.memory_index
                        .get(self.memory_next)
                        .map(|&address| self.memory_row(address).0),
                )
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
            while let Some(&address) = self.memory_index.get(self.memory_next) {
                let (memory_key, lsn, value) = self.memory_row(address);
                if memory_key != key.as_slice() {
                    break;
                }
                patches.push((lsn, value.to_vec()));
                self.memory_next += 1;
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
