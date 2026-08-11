//! Database traits for compatibility.

use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
    future::Future,
};

pub trait KeyT: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static {}
pub trait ValueT: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static {}

impl<K: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static> KeyT for K {}
impl<V: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static> ValueT for V {}

/// Emit a `walk progress` log every `WALK_PROGRESS_LOG_EVERY` reads when
/// instrumenting a long DB iteration.
pub const WALK_PROGRESS_LOG_EVERY: u64 = 1000;

pub trait Table: Send + Sync + Debug + 'static {
    type Key: KeyT;
    type Value: ValueT;

    const NAME: &'static str;
}

/// Interface to a DB read transaction.
pub trait DbTx {
    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Returns the stored value for the given key as its raw on-disk bytes, if it exists.
    ///
    /// The bytes are the same canonical value encoding [`get`](Self::get) decodes, so a caller
    /// that only relocates a payload (cold archival) skips the decode/re-encode round trip.
    /// Backends that can lend the bytes from their backing store return `Cow::Borrowed` (valid for
    /// the transaction); the default re-encodes the decoded value, which any backend can satisfy.
    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(self.get::<T>(key)?.map(|value| Cow::Owned(crate::encode(&value))))
    }

    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.get::<T>(key)?.is_some())
    }

    /// Returns an iterator over all key-value pairs in the table.
    fn iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Returns an iterator over all key-value pairs in the table as raw bytes.
    fn raw_iter<T: Table>(&self) -> DBRawIter<'_>;

    /// Skips to the first key >= the provided key and iterates from there.
    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;

    /// Skips to the first key >= the provided key and iterates from there as raw bytes.
    ///
    /// The default filters a full [`raw_iter`](Self::raw_iter) scan, correct for any backend
    /// whose raw iteration is ordered by encoded key; seekable backends override it to position
    /// a cursor directly, skipping the leading scan entirely.
    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let target = crate::encode_key(key);
        Ok(Box::new(self.raw_iter::<T>().skip_while(move |(k, _)| k.as_ref() < target.as_slice())))
    }

    /// Returns an iterator over all key-value pairs in reverse order.
    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Returns an iterator over all key-value pairs in reverse order as raw bytes.
    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_>;

    /// Returns the last key-value pair in the table.
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;

    /// Returns the record prior to the given key.
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;
}

/// Interface to a DB write transaction.
pub trait DbTxMut: DbTx {
    /// Insert the given key/value into the table.
    /// If key already exists it should replace it.
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()>;

    /// Removes many keys from the durable store as one operation, bypassing any in-memory cache.
    ///
    /// Layered backends override it to hard-delete (no tombstone, which would shadow a
    /// fall-through tier) and to hand the set to the writer as one unit; the default is per-key
    /// removes.
    fn evict_persistent_batch<T: Table>(&mut self, keys: &[T::Key]) -> eyre::Result<()> {
        for key in keys {
            self.remove::<T>(key)?;
        }
        Ok(())
    }

    /// Removes every key-value pair from the table.
    fn clear_table<T: Table>(&mut self) -> eyre::Result<()>;

    /// Serialize this transaction's reads of `table` against other writers that also lock it.
    ///
    /// The lock is held until the transaction is committed or dropped. Backends without the
    /// concept (MemDatabase, bare Mdbx/redb) no-op; `LayeredDatabase` serializes via its
    /// [`WriteLockManager`](crate::write_lock::WriteLockManager).
    ///
    /// Required for read-then-write sequences: without it, the read and the write run in
    /// separate transactions and two writers can interleave on the same key.
    fn lock_table(&mut self, table_name: &'static str) -> eyre::Result<()> {
        let _ = table_name;
        Ok(())
    }

    /// Commit data to durable storage.
    fn commit(self) -> eyre::Result<()>;
}

pub type DBIter<'i, T> = Box<dyn Iterator<Item = (<T as Table>::Key, <T as Table>::Value)> + 'i>;

/// Raw key/value iteration yielding borrowed bytes where the backend allows it.
///
/// The `Cow` is `Borrowed` straight into the backing store (e.g. an mdbx
/// read-txn mmap page, valid for the whole transaction) for zero-copy walks,
/// and `Owned` only where the backend cannot lend the bytes for `'i` (the
/// redb path, and the self-referential `Database::raw_iter` variant).
pub type DBRawIter<'i> = Box<dyn Iterator<Item = (Cow<'i, [u8]>, Cow<'i, [u8]>)> + 'i>;

pub trait Database: Send + Sync + Clone + Unpin + 'static {
    type TX<'txn>: DbTx + Debug + 'txn
    where
        Self: 'txn;
    type TXMut<'txn>: DbTxMut + Debug + 'txn
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()>;

    /// Return a read txn object.
    fn read_txn(&self) -> eyre::Result<Self::TX<'_>>;

    /// Return a write txn object.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>>;

    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;

    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Inserts the given key-value pair into the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// inserts.
    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// removes.
    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()>;

    /// Removes every key-value pair from the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// table clears.
    fn clear_table<T: Table>(&self) -> eyre::Result<()>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty<T: Table>(&self) -> bool;

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// If this is backed by storage an underlying error will most likely end the iterator early.
    fn iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Returns an unbounded iterator visiting each key-value pair in the map as raw bytes.
    fn raw_iter<T: Table>(&self) -> DBRawIter<'_>;

    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;

    /// Iterates over all the keys in reverse.
    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Iterates over all the keys in reverse, returning raw bytes.
    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_>;

    /// Returns the record prior to key if it exists or the first record that is sorted before if it
    /// does not exist.
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;

    /// Returns the last (key, value) in the database.
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;

    /// Returns a vector of values corresponding to the keys provided.
    fn multi_get<'a, T: Table>(
        &'a self,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        self.with_read_txn(|tx| keys.into_iter().map(|key| tx.get::<T>(key.borrow())).collect())
    }

    /// Returns a vector of values corresponding to the keys provided.
    fn multi_get_with_tx<'a, T: Table>(
        &'a self,
        txn: &Self::TX<'_>,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        keys.into_iter().map(|key| txn.get::<T>(key.borrow())).collect()
    }

    /// Execute a read operation with proper transaction scoping.
    fn with_read_txn<F, R>(&self, f: F) -> eyre::Result<R>
    where
        F: FnOnce(&Self::TX<'_>) -> eyre::Result<R>,
    {
        let tx = self.read_txn()?;
        f(&tx)
    }

    /// Execute a write operation with automatic commit/abort.
    fn with_write_txn<F, R>(&self, f: F) -> eyre::Result<R>
    where
        F: FnOnce(&mut Self::TXMut<'_>) -> eyre::Result<R>,
    {
        let mut tx = self.write_txn()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// If the underlying DB needs to be manually compacted (looking at redb here) then this can be
    /// overwritten to allow this.  No-op for most backends.
    fn compact(&self) -> eyre::Result<()> {
        Ok(())
    }

    /// Wait for enqueued background writes to commit, returning an error if any failed.
    ///
    /// A successful result means committed, not fsync'd to disk; deferred-sync backends sync later.
    fn persist(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        std::future::ready(Ok(()))
    }
    /// Sync version of persist- useful for test not for prod code.
    fn sync_persist(&self) {}
}
