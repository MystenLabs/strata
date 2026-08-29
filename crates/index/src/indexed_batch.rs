use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use typed_store::{
    Map,
    rocks::{DBBatch, DBMap, RocksDBSnapshot},
};

use crate::{Error, Result, StrataIndex};

/// Atomic RocksDB batch with point read-your-writes semantics.
///
/// Reads use the snapshot captured when the batch was created, then overlay staged puts and
/// deletes. All writes must go through this type or they will not be visible to [`Self::get`].
pub struct IndexedBatch<'a> {
    snapshot: RocksDBSnapshot<'a>,
    batch: DBBatch,
    staged: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>>,
}

impl StrataIndex {
    pub fn indexed_batch(&self) -> IndexedBatch<'_> {
        IndexedBatch {
            snapshot: self.db.snapshot(),
            batch: self.batch(),
            staged: BTreeMap::new(),
        }
    }
}

impl IndexedBatch<'_> {
    pub(crate) fn raw_batch_mut(&mut self) -> &mut DBBatch {
        &mut self.batch
    }

    pub fn get<K, V>(&self, map: &DBMap<K, V>, key: &K) -> Result<Option<V>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let index_key = index_key(map, key)?;
        if let Some(value) = self.staged.get(&index_key) {
            return value.as_deref().map(decode).transpose();
        }
        Ok(map.get_with_snapshot(&self.snapshot, key)?)
    }

    pub fn put<K, V>(&mut self, map: &DBMap<K, V>, key: &K, value: &V) -> Result<()>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let index_key = index_key(map, key)?;
        let encoded = encode(value)?;
        self.batch.insert_batch(map, [(key, value)])?;
        self.staged.insert(index_key, Some(encoded));
        Ok(())
    }

    pub fn delete<K, V>(&mut self, map: &DBMap<K, V>, key: &K) -> Result<()>
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
        map: &DBMap<K, V>,
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

    pub fn touched_keys<K, V>(&self, map: &DBMap<K, V>) -> Result<Vec<K>>
    where
        K: Serialize + DeserializeOwned,
    {
        self.staged
            .iter()
            .filter(|((cf, _), _)| cf == map.cf_name())
            .map(|((_, key), _)| decode(key))
            .collect()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }

    pub fn write(self) -> Result<()> {
        self.batch.write().map_err(Error::from)
    }

    pub fn write_with_sync(self, sync: bool) -> Result<()> {
        self.batch.write_with_sync(sync).map_err(Error::from)
    }
}

fn index_key<K: Serialize, V>(map: &DBMap<K, V>, key: &K) -> Result<(String, Vec<u8>)> {
    Ok((map.cf_name().to_owned(), encode(key)?))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bcs::to_bytes(value).map_err(|error| Error::Serialization(error.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bcs::from_bytes(bytes).map_err(|error| Error::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use core_types::SegmentGcSummary;
    use tempfile::tempdir;
    use typed_store::Map;

    use crate::{StrataIndex, init_typed_store_metrics};

    #[tokio::test]
    async fn reads_staged_writes_and_commits_across_maps() {
        init_typed_store_metrics();
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

        let mut batch = index.indexed_batch();
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

    #[tokio::test]
    async fn reads_untouched_values_from_its_creation_snapshot() {
        init_typed_store_metrics();
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

        let batch = index.indexed_batch();
        index.segment_gc_summaries().insert(&1, &later).unwrap();

        assert_eq!(
            batch.get(index.segment_gc_summaries(), &1).unwrap(),
            Some(original)
        );
    }
}
