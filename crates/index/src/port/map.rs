//! Typed view over one column family.
//!
//! [`TypedMap`] is the port's replacement for the wrapper-provided typed map. Method names
//! deliberately match the ones the index already calls, so adopting the port is a type swap at
//! call sites rather than a rewrite.

use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};

use crate::Result;

use super::{
    IndexDb, IndexSnapshot, IndexWriteBatch,
    codec::{decode_key, decode_value, encode_key, encode_value},
};

/// A single column family, typed by its key and value.
///
/// Cloning is cheap: every clone shares one backend handle.
#[derive(Debug)]
pub struct TypedMap<K, V> {
    db: Arc<dyn IndexDb>,
    cf: String,
    _marker: PhantomData<fn(K) -> V>,
}

impl<K, V> Clone for TypedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            cf: self.cf.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> TypedMap<K, V> {
    /// Binds a typed view to an already-open column family.
    pub fn new(db: Arc<dyn IndexDb>, cf: impl AsRef<str>) -> Self {
        Self {
            db,
            cf: cf.as_ref().to_owned(),
            _marker: PhantomData,
        }
    }

    /// The underlying column-family name.
    pub fn cf_name(&self) -> &str {
        &self.cf
    }

    /// Opens a write batch. The batch may span any map on the same backend.
    pub fn batch(&self) -> IndexBatch {
        IndexBatch {
            inner: self.db.write_batch(),
        }
    }
}

impl<K, V> TypedMap<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// Reads one key.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let key = encode_key(key)?;
        self.db
            .get(&self.cf, &key)?
            .as_deref()
            .map(decode_value)
            .transpose()
    }

    /// Reads one key as of a captured snapshot.
    pub fn get_with_snapshot(&self, snapshot: &dyn IndexSnapshot, key: &K) -> Result<Option<V>> {
        let key = encode_key(key)?;
        snapshot
            .get(&self.cf, &key)?
            .as_deref()
            .map(decode_value)
            .transpose()
    }

    /// Reports whether a key is present.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        let key = encode_key(key)?;
        self.db.contains_key(&self.cf, &key)
    }

    /// Writes one key, committing immediately.
    pub fn insert(&self, key: &K, value: &V) -> Result<()> {
        let key = encode_key(key)?;
        let value = encode_value(value)?;
        self.db.put(&self.cf, &key, &value)
    }

    /// Deletes one key, committing immediately.
    pub fn remove(&self, key: &K) -> Result<()> {
        let key = encode_key(key)?;
        self.db.delete(&self.cf, &key)
    }

    /// Scans the whole family in key order.
    ///
    /// Keys sort numerically because [`encode_key`] is big-endian and fixed-width.
    pub fn safe_iter(&self) -> Result<impl Iterator<Item = Result<(K, V)>> + '_> {
        Ok(self.db.iter(&self.cf)?.map(|row| {
            let (key, value) = row?;
            Ok((decode_key(&key)?, decode_value(&value)?))
        }))
    }

    /// Scans the whole family in key order, as of a captured snapshot.
    pub fn safe_iter_with_snapshot<'a>(
        &'a self,
        snapshot: &'a dyn IndexSnapshot,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a> {
        Ok(snapshot.iter(&self.cf)?.map(|row| {
            let (key, value) = row?;
            Ok((decode_key(&key)?, decode_value(&value)?))
        }))
    }

    /// Reports whether the family holds no rows.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.db.iter(&self.cf)?.next().is_none())
    }
}

/// An atomic batch spanning any number of maps on one backend.
///
/// Nothing is durable until [`IndexBatch::write`] or [`IndexBatch::write_with_sync`] returns.
pub struct IndexBatch {
    inner: Box<dyn IndexWriteBatch>,
}

impl std::fmt::Debug for IndexBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexBatch")
            .field("size_in_bytes", &self.inner.size_in_bytes())
            .finish()
    }
}

impl IndexBatch {
    /// Stages writes into one map.
    pub fn insert_batch<J, U, K, V>(
        &mut self,
        map: &TypedMap<K, V>,
        rows: impl IntoIterator<Item = (J, U)>,
    ) -> Result<()>
    where
        J: Borrow<K>,
        U: Borrow<V>,
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        for (key, value) in rows {
            self.inner.put(
                map.cf_name(),
                &encode_key(key.borrow())?,
                &encode_value(value.borrow())?,
            )?;
        }
        Ok(())
    }

    /// Stages deletes from one map.
    pub fn delete_batch<J, K, V>(
        &mut self,
        map: &TypedMap<K, V>,
        keys: impl IntoIterator<Item = J>,
    ) -> Result<()>
    where
        J: Borrow<K>,
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        for key in keys {
            self.inner
                .delete(map.cf_name(), &encode_key(key.borrow())?)?;
        }
        Ok(())
    }

    /// Stages merge operands, resolved by the family's merge operator.
    ///
    /// Operands are already-encoded bytes because the merge operator, not this layer, defines how
    /// an operand folds into a value.
    pub fn partial_merge_batch<J, K, V>(
        &mut self,
        map: &TypedMap<K, V>,
        rows: impl IntoIterator<Item = (J, Vec<u8>)>,
    ) -> Result<()>
    where
        J: Borrow<K>,
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        for (key, operand) in rows {
            self.inner
                .merge(map.cf_name(), &encode_key(key.borrow())?, &operand)?;
        }
        Ok(())
    }

    /// Size of the staged batch.
    pub fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }

    /// Commits without fsyncing the write-ahead log.
    pub fn write(self) -> Result<()> {
        self.inner.write(false)
    }

    /// Commits, fsyncing the write-ahead log when `sync` is set.
    pub fn write_with_sync(self, sync: bool) -> Result<()> {
        self.inner.write(sync)
    }
}
