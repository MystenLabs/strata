use std::collections::BTreeMap;

use crate::port::{
    IndexSnapshot, TypedMap,
    codec::{decode_key, decode_value, encode_key, encode_value},
    map::IndexBatch,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{Result, StrataIndex};

/// Atomic RocksDB batch with point read-your-writes semantics.
///
/// Reads use the snapshot captured when the batch was created, then overlay staged puts and
/// deletes. All writes must go through this type or they will not be visible to [`Self::get`].
pub struct IndexedBatch<'a> {
    snapshot: Box<dyn IndexSnapshot + 'a>,
    batch: IndexBatch,
    staged: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>>,
}

impl StrataIndex {
    pub fn indexed_batch(&self) -> Result<IndexedBatch<'_>> {
        Ok(IndexedBatch {
            snapshot: self.db.snapshot()?,
            batch: self.batch(),
            staged: BTreeMap::new(),
        })
    }
}

impl IndexedBatch<'_> {
    pub(crate) fn raw_batch_mut(&mut self) -> &mut IndexBatch {
        &mut self.batch
    }

    pub fn get<K, V>(&self, map: &TypedMap<K, V>, key: &K) -> Result<Option<V>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let index_key = index_key(map, key)?;
        if let Some(value) = self.staged.get(&index_key) {
            return value.as_deref().map(decode_value).transpose();
        }
        map.get_with_snapshot(self.snapshot.as_ref(), key)
    }

    pub fn put<K, V>(&mut self, map: &TypedMap<K, V>, key: &K, value: &V) -> Result<()>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let index_key = index_key(map, key)?;
        let encoded = encode_value(value)?;
        self.batch.insert_batch(map, [(key, value)])?;
        self.staged.insert(index_key, Some(encoded));
        Ok(())
    }

    pub fn delete<K, V>(&mut self, map: &TypedMap<K, V>, key: &K) -> Result<()>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let index_key = index_key(map, key)?;
        self.batch.delete_batch(map, [key])?;
        self.staged.insert(index_key, None);
        Ok(())
    }

    pub fn update<K, V>(
        &mut self,
        map: &TypedMap<K, V>,
        key: &K,
        update: impl FnOnce(Option<V>) -> Result<Option<V>>,
    ) -> Result<()>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        match update(self.get(map, key)?)? {
            Some(value) => self.put(map, key, &value),
            None => self.delete(map, key),
        }
    }

    pub fn touched_keys<K, V>(&self, map: &TypedMap<K, V>) -> Result<Vec<K>>
    where
        K: Serialize + DeserializeOwned,
    {
        self.staged
            .iter()
            .filter(|((cf, _), _)| cf == map.cf_name())
            .map(|((_, key), _)| decode_key(key))
            .collect()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }

    pub fn write(self) -> Result<()> {
        self.batch.write()
    }

    pub fn write_with_sync(self, sync: bool) -> Result<()> {
        self.batch.write_with_sync(sync)
    }
}

fn index_key<K: Serialize, V>(map: &TypedMap<K, V>, key: &K) -> Result<(String, Vec<u8>)> {
    Ok((map.cf_name().to_owned(), encode_key(key)?))
}

#[cfg(test)]
mod tests {
    use core_types::SegmentGcSummary;
    use tempfile::tempdir;

    use crate::StrataIndex;

    #[test]
    fn reads_staged_writes_and_commits_across_maps() {
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(
            dir.path(),
            "indexed-batch",
            dir.path().display().to_string(),
        )
        .unwrap();
        let first = SegmentGcSummary {
            total_bytes: 100,
            live_bytes: 100,
            live_ref_count: 1,
            ..Default::default()
        };
        let second = SegmentGcSummary {
            total_bytes: 200,
            live_bytes: 200,
            live_ref_count: 2,
            ..Default::default()
        };
        index.segment_gc_summaries().insert(&1, &first).unwrap();
        index.segment_gc_summaries().insert(&2, &second).unwrap();

        let mut batch = index.indexed_batch().unwrap();
        batch
            .update(index.segment_gc_summaries(), &1, |summary| {
                let mut summary = summary.unwrap();
                summary.live_bytes = 40;
                Ok(Some(summary))
            })
            .unwrap();
        batch.delete(index.segment_gc_summaries(), &2).unwrap();
        batch
            .put(index.segment_garbage_log_positions(), &1, &123)
            .unwrap();

        assert_eq!(
            batch
                .get(index.segment_gc_summaries(), &1)
                .unwrap()
                .unwrap()
                .live_bytes,
            40
        );
        assert_eq!(batch.get(index.segment_gc_summaries(), &2).unwrap(), None);
        assert_eq!(
            batch
                .get(index.segment_garbage_log_positions(), &1)
                .unwrap(),
            Some(123)
        );
        assert_eq!(
            batch.touched_keys(index.segment_gc_summaries()).unwrap(),
            vec![1, 2]
        );
        assert!(batch.size_in_bytes() > 0);

        batch.write_with_sync(true).unwrap();
        assert_eq!(
            index
                .segment_gc_summaries()
                .get(&1)
                .unwrap()
                .unwrap()
                .live_bytes,
            40
        );
        assert_eq!(index.segment_gc_summaries().get(&2).unwrap(), None);
        assert_eq!(
            index.segment_garbage_log_positions().get(&1).unwrap(),
            Some(123)
        );
    }

    #[test]
    fn reads_untouched_values_from_its_creation_snapshot() {
        let dir = tempdir().unwrap();
        let index = StrataIndex::open_path(
            dir.path(),
            "indexed-snapshot",
            dir.path().display().to_string(),
        )
        .unwrap();
        let original = SegmentGcSummary {
            total_bytes: 100,
            ..Default::default()
        };
        let later = SegmentGcSummary {
            total_bytes: 200,
            ..Default::default()
        };
        index.segment_gc_summaries().insert(&1, &original).unwrap();

        let batch = index.indexed_batch().unwrap();
        index.segment_gc_summaries().insert(&1, &later).unwrap();

        assert_eq!(
            batch.get(index.segment_gc_summaries(), &1).unwrap(),
            Some(original)
        );
    }
}
