use std::{cmp::Ordering, collections::BinaryHeap};

use strata_core::{BlobKey, StrataLsn};

use crate::Result;
use crate::state::{BlobUpdate, MaterializedBlobState, PatchRecord, PatchUpdate, StateRecord};

pub(super) struct DeltaKeyGroup {
    pub key: BlobKey,
    pub updates: Vec<BlobUpdate>,
}

pub(super) struct DeltaRunMerger<I>
where
    I: Iterator<Item = Result<BlobUpdate>>,
{
    sources: Vec<DeltaSource<I>>,
    heap: BinaryHeap<DeltaHeapItem>,
}

struct DeltaSource<I> {
    records: I,
    current: Option<BlobUpdate>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct DeltaHeapItem {
    key: BlobKey,
    lsn: StrataLsn,
    source: usize,
}

pub(super) struct PatchRunInput<I> {
    pub precedence: usize,
    pub records: I,
}

pub(super) struct MergedKey {
    pub key: BlobKey,
    pub base_state: Option<MaterializedBlobState>,
    pub patch_updates: Vec<PatchUpdate>,
}

pub(super) struct BasePatchMerger<B, P>
where
    B: Iterator<Item = Result<StateRecord>>,
    P: Iterator<Item = Result<PatchRecord>>,
{
    base: Option<BaseSource<B>>,
    patches: Vec<PatchSource<P>>,
    heap: BinaryHeap<HeapItem>,
}

struct BaseSource<I> {
    records: I,
    current: Option<StateRecord>,
}

struct PatchSource<I> {
    precedence: usize,
    records: I,
    current: Option<PatchRecord>,
}

impl<I> DeltaRunMerger<I>
where
    I: Iterator<Item = Result<BlobUpdate>>,
{
    pub fn new(runs: impl IntoIterator<Item = I>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        let mut sources = Vec::new();
        for mut records in runs {
            let Some(current) = records.next().transpose()? else {
                continue;
            };
            let source = sources.len();
            heap.push(DeltaHeapItem {
                key: current.key().clone(),
                lsn: current.lsn(),
                source,
            });
            sources.push(DeltaSource {
                records,
                current: Some(current),
            });
        }

        Ok(Self { sources, heap })
    }

    fn next_key(&mut self) -> Result<Option<DeltaKeyGroup>> {
        let Some(first) = self.heap.pop() else {
            return Ok(None);
        };

        // Each delta run is already sorted by (key, lsn). The heap performs the k-way merge across
        // runs and drains all rows for one key before yielding. That gives delta compaction a
        // complete, LSN-ordered slice for the key without loading unrelated partitions or scanning
        // older base/patch state.
        let key = first.key.clone();
        let mut updates = Vec::new();
        self.consume_item(first, &mut updates)?;
        while self.heap.peek().is_some_and(|item| item.key == key) {
            let item = self.heap.pop().expect("peeked heap item must exist");
            self.consume_item(item, &mut updates)?;
        }

        Ok(Some(DeltaKeyGroup { key, updates }))
    }

    fn consume_item(&mut self, item: DeltaHeapItem, updates: &mut Vec<BlobUpdate>) -> Result<()> {
        let source = &mut self.sources[item.source];
        let update = source
            .current
            .take()
            .expect("delta heap item must point at a loaded update");
        updates.push(update);
        if let Some(next) = source.records.next().transpose()? {
            self.heap.push(DeltaHeapItem {
                key: next.key().clone(),
                lsn: next.lsn(),
                source: item.source,
            });
            source.current = Some(next);
        }
        Ok(())
    }
}

impl<I> Iterator for DeltaRunMerger<I>
where
    I: Iterator<Item = Result<BlobUpdate>>,
{
    type Item = Result<DeltaKeyGroup>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_key().transpose()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SourceRef {
    Base,
    Patch(usize),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapItem {
    key: BlobKey,
    source: SourceRef,
}

impl<B, P> BasePatchMerger<B, P>
where
    B: Iterator<Item = Result<StateRecord>>,
    P: Iterator<Item = Result<PatchRecord>>,
{
    pub fn new(
        base: Option<B>,
        patches: impl IntoIterator<Item = PatchRunInput<P>>,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        let base = if let Some(mut records) = base {
            let current = records.next().transpose()?;
            if let Some(record) = &current {
                heap.push(HeapItem {
                    key: record.key.clone(),
                    source: SourceRef::Base,
                });
            }
            Some(BaseSource { records, current })
        } else {
            None
        };

        let mut patch_sources = Vec::new();
        for input in patches {
            let mut records = input.records;
            let Some(current) = records.next().transpose()? else {
                continue;
            };
            let source_index = patch_sources.len();
            heap.push(HeapItem {
                key: current.key.clone(),
                source: SourceRef::Patch(source_index),
            });
            patch_sources.push(PatchSource {
                precedence: input.precedence,
                records,
                current: Some(current),
            });
        }

        Ok(Self {
            base,
            patches: patch_sources,
            heap,
        })
    }

    fn next_key(&mut self) -> Result<Option<MergedKey>> {
        let Some(first) = self.heap.pop() else {
            return Ok(None);
        };

        // Major compaction needs the whole vertical stack for one key: optional base state plus
        // every patch record that may rewrite it. The heap groups by key; patch precedence is sorted
        // after grouping because manifest order, not file interleaving, defines the history from
        // oldest patch to newest patch.
        let key = first.key.clone();
        let mut base_state = None;
        let mut patch_records = Vec::new();
        self.consume_item(first, &mut base_state, &mut patch_records)?;
        while self.heap.peek().is_some_and(|item| item.key == key) {
            let item = self.heap.pop().expect("peeked heap item must exist");
            self.consume_item(item, &mut base_state, &mut patch_records)?;
        }

        patch_records.sort_by_key(|(precedence, _)| *precedence);
        let patch_updates = patch_records
            .into_iter()
            .flat_map(|(_, record)| record.updates)
            .collect();

        Ok(Some(MergedKey {
            key,
            base_state,
            patch_updates,
        }))
    }

    fn consume_item(
        &mut self,
        item: HeapItem,
        base_state: &mut Option<MaterializedBlobState>,
        patch_records: &mut Vec<(usize, PatchRecord)>,
    ) -> Result<()> {
        match item.source {
            SourceRef::Base => {
                let base = self
                    .base
                    .as_mut()
                    .expect("base heap item needs base source");
                let record = base
                    .current
                    .take()
                    .expect("base heap item must point at a loaded record");
                *base_state = Some(record.state);
                if let Some(next) = base.records.next().transpose()? {
                    self.heap.push(HeapItem {
                        key: next.key.clone(),
                        source: SourceRef::Base,
                    });
                    base.current = Some(next);
                }
            }
            SourceRef::Patch(index) => {
                let patch = &mut self.patches[index];
                let record = patch
                    .current
                    .take()
                    .expect("patch heap item must point at a loaded record");
                patch_records.push((patch.precedence, record));
                if let Some(next) = patch.records.next().transpose()? {
                    self.heap.push(HeapItem {
                        key: next.key.clone(),
                        source: SourceRef::Patch(index),
                    });
                    patch.current = Some(next);
                }
            }
        }
        Ok(())
    }
}

impl<B, P> Iterator for BasePatchMerger<B, P>
where
    B: Iterator<Item = Result<StateRecord>>,
    P: Iterator<Item = Result<PatchRecord>>,
{
    type Item = Result<MergedKey>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_key().transpose()
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so invert comparisons to pop the smallest key first. The source
        // tie-breaker keeps base before patches while draining equal keys; patch records are still
        // explicitly sorted by manifest precedence before folding.
        other
            .key
            .cmp(&self.key)
            .then_with(|| source_order(other.source).cmp(&source_order(self.source)))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn source_order(source: SourceRef) -> usize {
    match source {
        SourceRef::Base => 0,
        SourceRef::Patch(index) => index + 1,
    }
}

impl Ord for DeltaHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Keep the merged delta stream deterministic by key, then LSN, then source. The LSN order is
        // what lets compact_delta_updates reason about first Put, terminal update, and transient
        // payload lifetimes using simple slice boundaries.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.lsn.cmp(&self.lsn))
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for DeltaHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
