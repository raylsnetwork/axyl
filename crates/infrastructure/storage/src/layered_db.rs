use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::BinaryHeap,
    fmt::Debug,
    future::Future,
    iter::Peekable,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        mpsc::{self, Receiver, Sender},
        Arc, OnceLock,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(feature = "cold-storage")]
use crate::{
    cold::{ColdStore, ColdTx},
    tables::ColdBatchLocations,
};

use crate::mem_db::{EvictionHeap, EvictionStats, MemDatabase, MemDbTx, MemDbTxMut};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};
use tokio::sync::oneshot::{self, error::TryRecvError};

/// Streaming merge-join iterator for LayeredDB.
/// Merges sorted iterators from the persistent DB and in-memory cache,
/// with mem entries taking precedence on key conflicts.
/// Entries tombstoned in mem (deleted but not yet removed from persistent DB)
/// are filtered out via the `is_tombstoned` closure.
struct MergeJoinIter<'a, T: Table> {
    db_iter: Peekable<DBIter<'a, T>>,
    mem_iter: Peekable<DBIter<'a, T>>,
    is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    reverse: bool,
}

impl<'a, T: Table> MergeJoinIter<'a, T> {
    fn forward(
        db_iter: DBIter<'a, T>,
        mem_iter: DBIter<'a, T>,
        is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    ) -> Self {
        Self {
            db_iter: db_iter.peekable(),
            mem_iter: mem_iter.peekable(),
            is_tombstoned,
            reverse: false,
        }
    }

    fn reverse(
        db_iter: DBIter<'a, T>,
        mem_iter: DBIter<'a, T>,
        is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    ) -> Self {
        Self {
            db_iter: db_iter.peekable(),
            mem_iter: mem_iter.peekable(),
            is_tombstoned,
            reverse: true,
        }
    }
}

impl<'a, T: Table> Iterator for MergeJoinIter<'a, T> {
    type Item = (T::Key, T::Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.db_iter.peek(), self.mem_iter.peek()) {
                (Some((db_key, _)), Some((mem_key, _))) => {
                    // in reverse mode, flip comparison so Greater means "db first"
                    let cmp = db_key.cmp(mem_key);
                    let cmp = if self.reverse { cmp.reverse() } else { cmp };
                    match cmp {
                        Ordering::Less => {
                            // db key comes first in iteration order
                            let (key, value) = self.db_iter.next().unwrap();
                            if (self.is_tombstoned)(&key) {
                                continue;
                            }
                            return Some((key, value));
                        }
                        Ordering::Equal => {
                            self.db_iter.next(); // skip db, prefer mem
                            return self.mem_iter.next();
                        }
                        Ordering::Greater => {
                            // mem key comes first in iteration order
                            return self.mem_iter.next();
                        }
                    }
                }
                (Some(_), None) => {
                    let (key, value) = self.db_iter.next().unwrap();
                    if (self.is_tombstoned)(&key) {
                        continue;
                    }
                    return Some((key, value));
                }
                (None, Some(_)) => return self.mem_iter.next(),
                (None, None) => return None,
            }
        }
    }
}

/// Streaming merge-join iterator for LayeredDB returning raw bytes.
struct MergeJoinRawIter<'a, T: Table> {
    db_iter: Peekable<DBRawIter<'a>>,
    mem_iter: Peekable<DBRawIter<'a>>,
    is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    reverse: bool,
}

impl<'a, T: Table> MergeJoinRawIter<'a, T> {
    fn forward(
        db_iter: DBRawIter<'a>,
        mem_iter: DBRawIter<'a>,
        is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    ) -> Self {
        Self {
            db_iter: db_iter.peekable(),
            mem_iter: mem_iter.peekable(),
            is_tombstoned,
            reverse: false,
        }
    }

    fn reverse(
        db_iter: DBRawIter<'a>,
        mem_iter: DBRawIter<'a>,
        is_tombstoned: Box<dyn Fn(&T::Key) -> bool + 'a>,
    ) -> Self {
        Self {
            db_iter: db_iter.peekable(),
            mem_iter: mem_iter.peekable(),
            is_tombstoned,
            reverse: true,
        }
    }
}

impl<'a, T: Table> Iterator for MergeJoinRawIter<'a, T> {
    type Item = (Cow<'a, [u8]>, Cow<'a, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.db_iter.peek(), self.mem_iter.peek()) {
                (Some((db_key, _)), Some((mem_key, _))) => {
                    let cmp = db_key.cmp(mem_key);
                    let cmp = if self.reverse { cmp.reverse() } else { cmp };
                    match cmp {
                        Ordering::Less => {
                            let (key, value) = self.db_iter.next().unwrap();
                            if (self.is_tombstoned)(&decode_key::<T::Key>(&key)) {
                                continue;
                            }
                            return Some((key, value));
                        }
                        Ordering::Equal => {
                            self.db_iter.next();
                            return self.mem_iter.next();
                        }
                        Ordering::Greater => {
                            return self.mem_iter.next();
                        }
                    }
                }
                (Some(_), None) => {
                    let (key, value) = self.db_iter.next().unwrap();
                    if (self.is_tombstoned)(&decode_key::<T::Key>(&key)) {
                        continue;
                    }
                    return Some((key, value));
                }
                (None, Some(_)) => return self.mem_iter.next(),
                (None, None) => return None,
            }
        }
    }
}

/// Merges the cold-ordered stream for `T` beneath `hot`; hot wins on an equal key, so a
/// sealed-but-unpruned row surfaces once. A passthrough when no cold layer is attached or `T`
/// has no cold key order. A hot tombstone does not hide the cold copy, matching `get`.
///
/// A cold fault ends the whole merged stream, not just the cold side: draining the hot tail past
/// a corrupt jar would present the fault as a gap in history instead of a truncation at it.
#[cfg(feature = "cold-storage")]
fn merge_cold<'i, T: Table>(
    cold: Option<ColdTx<'i>>,
    hot: DBIter<'i, T>,
    from: Option<&T::Key>,
    reverse: bool,
) -> DBIter<'i, T> {
    let Some(tx) = cold else { return hot };
    let Some(cold_side) = tx.scan::<T>(from, reverse) else { return hot };
    let faulted = tx.faulted();
    let never: Box<dyn Fn(&T::Key) -> bool + 'i> = Box::new(|_| false);
    let merged: DBIter<'i, T> = if reverse {
        Box::new(MergeJoinIter::<T>::reverse(cold_side, hot, never))
    } else {
        Box::new(MergeJoinIter::<T>::forward(cold_side, hot, never))
    };
    Box::new(merged.take_while(move |_| !faulted.get()))
}

/// Raw-bytes twin of [`merge_cold`].
#[cfg(feature = "cold-storage")]
fn merge_cold_raw<'i, T: Table>(
    cold: Option<ColdTx<'i>>,
    hot: DBRawIter<'i>,
    from: Option<&T::Key>,
    reverse: bool,
) -> DBRawIter<'i> {
    let Some(tx) = cold else { return hot };
    let Some(cold_side) = tx.raw_scan::<T>(from, reverse) else { return hot };
    let faulted = tx.faulted();
    let never: Box<dyn Fn(&T::Key) -> bool + 'i> = Box::new(|_| false);
    let merged: DBRawIter<'i> = if reverse {
        Box::new(MergeJoinRawIter::<T>::reverse(cold_side, hot, never))
    } else {
        Box::new(MergeJoinRawIter::<T>::forward(cold_side, hot, never))
    };
    Box::new(merged.take_while(move |_| !faulted.get()))
}

/// Default cap on total cached rows (live and tombstoned) before the writer evicts settled keys.
const DEFAULT_MAX_CACHE_SIZE: usize = 10000;

/// Eviction policy for the mem cache.
#[derive(Clone, Copy, Debug)]
pub struct CacheConfig {
    /// Total cached rows (live and tombstoned) allowed before the writer evicts settled keys
    /// (in-flight == 0) in recency order. Small in tests, the default in production.
    pub max_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { max_size: DEFAULT_MAX_CACHE_SIZE }
    }
}

pub struct LayeredDbTx<'a, DB: Database> {
    mem_db: MemDbTx<'a>,
    db: DB::TX<'a>,
    /// The cold tier reads fall through to on a hot miss, when attached.
    #[cfg(feature = "cold-storage")]
    cold: Option<&'a ColdStore>,
}

impl<'a, DB: Database> LayeredDbTx<'a, DB> {
    /// Opens a cold read transaction over the attached tier, resolving the auxiliary index on
    /// this transaction's own hot snapshot.
    #[cfg(feature = "cold-storage")]
    fn cold_tx(&self) -> Option<ColdTx<'_>> {
        let cold = self.cold?;
        Some(ColdTx::new(cold, |digest| self.get::<ColdBatchLocations>(digest)))
    }

    /// Serves `key` from the cold tier after a hot miss; `None` when no cold layer is attached.
    #[cfg(feature = "cold-storage")]
    fn cold_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        match self.cold_tx() {
            Some(tx) => tx.get::<T>(key),
            None => Ok(None),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(None)
    }

    /// Serves `key`'s raw jar bytes from the cold tier after a hot miss.
    #[cfg(feature = "cold-storage")]
    fn cold_raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        match self.cold_tx() {
            Some(tx) => Ok(tx.raw_get::<T>(key)?.map(|bytes| Cow::Owned(bytes.into_owned()))),
            None => Ok(None),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_raw_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(None)
    }

    /// Iterates key-descending from the largest key at or below `key` (the reverse of
    /// `skip_to`), merging the cold span beneath the hot tiers.
    ///
    /// Inherent rather than a `DbTx` method: the transaction contract deliberately stays fixed,
    /// so walk-back lives on the concrete layered types that can serve it efficiently. Each hot
    /// side starts at the floor by positioned lookup and steps backwards one seek per row
    /// (`record_prior_to`), so a deep floor never pays for the rows above it.
    pub fn reverse_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db = &self.db;
        let db_first = if self.mem_db.is_clearing::<T>() {
            None
        } else {
            db.get::<T>(key)?
                .map(|value| (key.clone(), value))
                .or_else(|| db.record_prior_to::<T>(key))
        };
        let db_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(db_first, move |(k, _)| db.record_prior_to::<T>(k)));

        let mem = &self.mem_db;
        let mem_first = if mem.is_tombstoned::<T>(key) {
            None
        } else {
            mem.get_no_marked_check::<T>(key).map(|(_, value)| (key.clone(), value))
        }
        .or_else(|| mem.record_prior_to::<T>(key));
        let mem_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(mem_first, move |(k, _)| mem.record_prior_to::<T>(k)));

        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), true))
    }

    /// Chains the cold-ordered stream for `T` beneath `hot` (see [`merge_cold`]).
    #[cfg(feature = "cold-storage")]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> DBIter<'i, T> {
        merge_cold::<T>(self.cold_tx(), hot, from, reverse)
    }

    /// Cold storage compiled out: iteration is hot-only.
    #[cfg(not(feature = "cold-storage"))]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        _from: Option<&T::Key>,
        _reverse: bool,
    ) -> DBIter<'i, T> {
        hot
    }

    /// Chains the cold-ordered raw stream for `T` beneath `hot` (see [`merge_cold_raw`]).
    #[cfg(feature = "cold-storage")]
    fn chain_cold_raw<'i, T: Table>(
        &'i self,
        hot: DBRawIter<'i>,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> DBRawIter<'i> {
        merge_cold_raw::<T>(self.cold_tx(), hot, from, reverse)
    }

    /// Cold storage compiled out: iteration is hot-only.
    #[cfg(not(feature = "cold-storage"))]
    fn chain_cold_raw<'i, T: Table>(
        &'i self,
        hot: DBRawIter<'i>,
        _from: Option<&T::Key>,
        _reverse: bool,
    ) -> DBRawIter<'i> {
        hot
    }
}

impl<'a, DB: Database> Debug for LayeredDbTx<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTx")
    }
}

impl<'a, DB: Database> DbTx for LayeredDbTx<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // The hot answer (tombstone-aware mem snapshot, then the persistent tier) wins; only a
        // full hot miss consults the cold tier, so a row removed from hot still serves its
        // archived copy.
        let hot = if self.mem_db.is_tombstoned::<T>(key) {
            None
        } else if let Some((_, val)) = self.mem_db.get_no_marked_check::<T>(key) {
            Some(val)
        } else if self.mem_db.is_clearing::<T>() {
            // Pending clear: keys not live in the cache are gone, not stale.
            None
        } else {
            self.db.get::<T>(key)?
        };
        match hot {
            Some(val) => Ok(Some(val)),
            None => self.cold_get::<T>(key),
        }
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        if self.mem_db.is_tombstoned::<T>(key) {
            return self.cold_raw_get::<T>(key);
        }
        // A mem-overlay hit holds the typed value, so it must re-encode; the common archival path
        // (old rows already evicted from the overlay) falls through to the inner backend's
        // zero-copy raw read.
        if let Some((_, val)) = self.mem_db.get_no_marked_check::<T>(key) {
            return Ok(Some(Cow::Owned(encode(&val))));
        }
        if self.mem_db.is_clearing::<T>() {
            return self.cold_raw_get::<T>(key);
        }
        match self.db.raw_get::<T>(key)? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.cold_raw_get::<T>(key),
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.iter::<T>()
        };
        let mem_iter = self.mem_db.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, false)
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.raw_iter::<T>()
        };
        let mem_iter = self.mem_db.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, false)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.skip_to::<T>(key)?
        };
        let mem_iter = self.mem_db.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), false))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.raw_skip_to::<T>(key)?
        };
        let mem_iter = self.mem_db.raw_skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold_raw::<T>(hot, Some(key), false))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.reverse_iter::<T>()
        };
        let mem_iter = self.mem_db.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, true)
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.reverse_raw_iter::<T>()
        };
        let mem_iter = self.mem_db.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, true)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn disable_long_read_safety(&self) {
        // only the mdbx layer enforces a read-txn timeout; forward to the held
        // inner txn so every cursor derived from it runs exempt. mem has none.
        self.db.disable_long_read_safety();
    }
}

pub struct LayeredDbTxMut<'a, DB: Database> {
    mem_db: MemDbTxMut<'a>,
    _db: DB,
    tx: QueueSender<DB>,
}

impl<'a, DB: Database> Debug for LayeredDbTxMut<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTxMut")
    }
}

impl<'a, DB: Database> DbTx for LayeredDbTxMut<'a, DB> {
    fn get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        panic!("DbTx get() should not be called on a DbTxMut!");
    }

    fn raw_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        panic!("DbTx raw_get() should not be called on a DbTxMut!");
    }

    fn raw_skip_to<T: Table>(&self, _key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        panic!("DbTx raw_skip_to() should not be called on a DbTxMut!");
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx iter() should not be called on a DbTxMut!");
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx raw_iter() should not be called on a DbTxMut!");
    }

    fn skip_to<T: Table>(&self, _key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        panic!("DbTx skip_to() should not be called on a DbTxMut!");
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx reverse_iter() should not be called on a DbTxMut!");
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx reverse_raw_iter() should not be called on a DbTxMut!");
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        panic!("DbTx last_record() should not be called on a DbTxMut!");
    }

    fn record_prior_to<T: Table>(&self, _key: &T::Key) -> Option<(T::Key, T::Value)> {
        panic!("DbTx record_prior_to() should not be called on a DbTxMut!");
    }

    fn disable_long_read_safety(&self) {
        panic!("DbTx disable_long_read_safety() should not be called on a DbTxMut!");
    }
}

impl<'a, DB: Database> DbTxMut for LayeredDbTxMut<'a, DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        // The txn producer already holds the mem write guard for its whole lifetime, so the
        // mutation, the in-flight increment and the enqueue are one critical section by
        // construction.
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn evict_persistent_batch<T: Table>(&mut self, keys: &[T::Key]) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        if keys.is_empty() {
            return Ok(());
        }
        // Hard-delete (no tombstone): a tombstone would shadow the cold fall-through. The whole
        // set is ONE writer message; a per-row message on a whole-epoch prune is pure overhead.
        for key in keys {
            self.mem_db.hard_delete::<T>(key);
        }
        let rm = Box::new(KeyRemoveBatch::<T> { keys: keys.to_vec() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        let keys = self.mem_db.raw_keys::<T>();
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _marker: PhantomData, keys });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        self.mem_db.commit()?;
        self.tx.send(DBMessage::CommitTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }
}

/// An op applied into a (possibly shared) write txn; its in-flight release must wait for the
/// final commit to succeed.
enum DeferredOp<DB: Database> {
    Insert(Box<dyn InsertTrait<DB>>),
    Remove(Box<dyn RemoveTrait<DB>>),
    Clear(Box<dyn ClearTrait<DB>>),
}

impl<DB: Database> DeferredOp<DB> {
    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap) {
        match self {
            DeferredOp::Insert(op) => op.on_applied(mem_db, heap),
            DeferredOp::Remove(op) => op.on_applied(mem_db, heap),
            DeferredOp::Clear(op) => op.on_applied(mem_db, heap),
        }
    }
}

/// Depth at which the writer queue is treated as a backlog: `db_run` warns (rate-limited) and
/// data-plane enqueues start pacing, so the imbalance never surfaces as a multi-minute `persist`
/// drain at the next epoch boundary.
const QUEUE_HIGH_WATER_MARK: usize = 10_000;
const QUEUE_LAG_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Cadence of the mem cache occupancy heartbeat in `db_run`: one `info` line per interval with
/// the cache size, its cap, the writer queue depth and the open write txn count.
const OCCUPANCY_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Pause paid by each insert/remove/clear enqueue while the queue is above
/// [`QUEUE_HIGH_WATER_MARK`].
///
/// Soft backpressure: a slow inner DB surfaces as gradual producer slowdown instead of a
/// network-wide stall at the deterministic boundary `persist`. Pacing can lag the node into
/// demotion (recoverable; the boundary stall is not), and the sleep may land on an async worker
/// thread (the `Database` write API is sync) - the accepted cost in an already-degraded regime.
/// Control messages (txn markers, persist barrier, shutdown) never pace.
const QUEUE_PACE_SLEEP: Duration = Duration::from_millis(1);

/// Drain time past which a `persist`/`sync_persist` is logged at `warn` rather than `debug`.
const PERSIST_SLOW_WARN: Duration = Duration::from_secs(1);

/// A [`DBMessage`] sender that tracks the writer queue's depth: every enqueue bumps a shared
/// counter `db_run` decrements as it drains, feeding the depth gauge and the
/// [`QUEUE_HIGH_WATER_MARK`] pacing. It also carries the poison latch: once the writer observes
/// a fatal failure, every producer write path fails fast and nothing further is applied.
struct QueueSender<DB: Database> {
    tx: Sender<DBMessage<DB>>,
    depth: Arc<AtomicUsize>,
    /// The first fatal writer failure, if any. Once set the DB is poisoned: the writer applies
    /// nothing further and every write attempt fails fast with this error.
    fatal: Arc<OnceLock<String>>,
}

impl<DB: Database> Clone for QueueSender<DB> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone(), depth: Arc::clone(&self.depth), fatal: Arc::clone(&self.fatal) }
    }
}

impl<DB: Database> Debug for QueueSender<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueueSender(depth: {}, poisoned: {})", self.depth(), self.fatal.get().is_some())
    }
}

impl<DB: Database> QueueSender<DB> {
    /// Enqueues a writer message; data-plane messages pace once the depth is past
    /// [`QUEUE_HIGH_WATER_MARK`] (see [`QUEUE_PACE_SLEEP`]).
    fn send(&self, msg: DBMessage<DB>) -> Result<(), mpsc::SendError<DBMessage<DB>>> {
        if matches!(msg, DBMessage::Insert(_) | DBMessage::Remove(_) | DBMessage::Clear(_))
            && self.depth() > QUEUE_HIGH_WATER_MARK
        {
            std::thread::sleep(QUEUE_PACE_SLEEP);
        }
        // Bump before the send so the depth never reads low mid-flight. Relaxed: an advisory
        // gauge, not a synchronization point.
        self.depth.fetch_add(1, AtomicOrdering::Relaxed);
        self.tx.send(msg)
    }

    /// Returns the writer messages enqueued but not yet applied by the background thread.
    fn depth(&self) -> usize {
        self.depth.load(AtomicOrdering::Relaxed)
    }

    /// The first fatal writer failure, if any: once set, no further writes are applied and every
    /// producer write path fails fast.
    fn fatal(&self) -> Option<&String> {
        self.fatal.get()
    }
}

/// Builds the fail-fast error every write path returns once the DB is poisoned, carrying the
/// original writer failure.
fn poisoned_error(fatal: Option<&String>) -> Option<eyre::Error> {
    fatal.map(|e| eyre::eyre!("consensus DB poisoned: no further writes will be attempted: {e}"))
}

/// Registers a metric on the process scrape registry, falling back to a private unscraped one on
/// double registration (a second stack in one process, as tests build).
pub(crate) fn register_metric_or_unscraped<T>(
    register: impl Fn(&Registry) -> Result<T, prometheus::Error>,
) -> T {
    register(default_registry())
        .unwrap_or_else(|_| register(&Registry::new()).expect("metric on a fresh registry"))
}

/// Returns the writer-queue-depth gauge; `db_run` samples it on every dequeue, so a dashboard
/// shows the backlog ramp before it crosses the warn threshold.
fn writer_queue_depth_gauge() -> IntGauge {
    register_metric_or_unscraped(|registry| {
        register_int_gauge_with_registry!(
            "layered_db_writer_queue_depth",
            "Consensus DB layered writer messages enqueued but not yet applied.",
            registry,
        )
    })
}

/// Logs a `persist`/`sync_persist` drain: `warn` past [`PERSIST_SLOW_WARN`] (a backlog that stalled
/// the barrier), `debug` otherwise. `depth` is the queue depth observed when the barrier enqueued.
fn log_persist_latency(elapsed: Duration, depth: usize) {
    if elapsed >= PERSIST_SLOW_WARN {
        tracing::warn!(
            target: "storage",
            ?elapsed,
            depth,
            "consensus DB persist drained a slow writer backlog"
        );
    } else {
        tracing::debug!(target: "storage", ?elapsed, depth, "consensus DB persist");
    }
}

/// Runs one eviction pass and logs its outcome at `info`: the cache size before and after and
/// the number of rows evicted, plus the open write transactions observed at that moment.
/// Eviction only runs once no producer txn is open, so `open_txns` is normally 0 except at a
/// `CaughtUp` barrier.
fn evict_and_log(mem_db: &MemDatabase, heap: &mut EvictionHeap, max_size: usize, open_txns: usize) {
    let EvictionStats { before, after, evicted } = mem_db.evict_if_needed(heap, max_size);
    if evicted > 0 {
        tracing::debug!(
            target: "storage",
            before,
            after,
            evicted,
            open_txns,
            "mem cache evicted"
        );
    }
}

/// The background writer loop.
///
/// A write/commit failure POISONS the DB: the failure is latched (first failure wins), the
/// optional node fatal-error channel is signaled, and from then on this loop never touches the
/// database again — it discards every `StartTxn`/`CommitTxn`/`Insert`/`Remove`/`Clear` message,
/// rejects each `CaughtUp` barrier with the stored error, and waits for `Shutdown`. Producers
/// independently fail fast on the same latch, so the node winds down instead of continuing to
/// write into a failing tier. The failed rows stay pinned in mem (their in-flight counts never
/// settle) and die with the process; the cache rebuilds from the durable tier on the next start.
/// Only `compact` failures stay advisory.
fn db_run<DB: Database>(
    db: DB,
    mem_db: MemDatabase,
    rx: Receiver<DBMessage<DB>>,
    depth: Arc<AtomicUsize>,
    max_size: usize,
    fatal: Arc<OnceLock<String>>,
    fatal_signal: Arc<OnceLock<tokio::sync::watch::Sender<Option<String>>>>,
) {
    let mut txn = None;
    let mut last_compact = Instant::now();
    let mut last_occupancy_log = Instant::now();
    let queue_depth_gauge = writer_queue_depth_gauge();
    let mut last_lag_warn: Option<Instant> = None;

    let mut eviction_heap: EvictionHeap = BinaryHeap::new();
    let mut committed_ops: Vec<DeferredOp<DB>> = Vec::with_capacity(1000);
    // Latch the first fatal failure, then signal the node's fatal-error channel (when wired) so
    // the wind-down starts immediately; both `send` results are advisory and ignored.
    // The watch preserves the first error (root cause): a later `WriteFailed` from cold archival
    // must not overwrite the original writer failure that `core.rs` logs.
    let trip = |msg: String| {
        let _ = fatal.set(msg.clone());
        if let Some(signal) = fatal_signal.get() {
            let _ = signal.send_if_modified(|cur| {
                if cur.is_none() {
                    *cur = Some(msg.clone());
                    true
                } else {
                    false
                }
            });
        }
    };
    if let Err(e) = db.compact() {
        tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB on startup (background): {e}");
    }
    while let Ok(msg) = rx.recv() {
        // Depth after taking this message off the queue (fetch_sub returns the count including it).
        let queued = depth.fetch_sub(1, AtomicOrdering::Relaxed).saturating_sub(1);
        queue_depth_gauge.set(queued as i64);
        if queued > QUEUE_HIGH_WATER_MARK
            && last_lag_warn.is_none_or(|at| at.elapsed() >= QUEUE_LAG_WARN_INTERVAL)
        {
            last_lag_warn = Some(Instant::now());
            tracing::warn!(target: "storage", depth = queued, "layered DB writer queue backlog");
        }
        // Poisoned: the DB is no longer writable. Apply nothing further (no DB access at all,
        // including eviction and compact), reject the barriers with the stored error, and wait
        // for shutdown so the producer-side fail-fast and the node wind-down run to completion.
        if let Some(err) = fatal.get() {
            match msg {
                DBMessage::CaughtUp(tx) => {
                    let _ = tx.send(Err(err.clone()));
                }
                DBMessage::Shutdown => break,
                DBMessage::StartTxn
                | DBMessage::CommitTxn
                | DBMessage::Insert(_)
                | DBMessage::Remove(_)
                | DBMessage::Clear(_) => {}
            }
            continue;
        }
        match msg {
            DBMessage::StartTxn => {
                if let Some((_txn, count)) = &mut txn {
                    *count += 1;
                } else {
                    match db.write_txn() {
                        Ok(ntxn) => txn = Some((ntxn, 1)),
                        Err(e) => {
                            tracing::error!(target: "layered_db_runner", "DB ERROR getting write txn (background); the DB is poisoned: {e}");
                            trip(format!("write_txn: {e}"));
                        }
                    }
                }
            }
            DBMessage::CommitTxn => {
                if let Some((current_txn, count)) = txn.take() {
                    if count <= 1 {
                        match current_txn.commit() {
                            Ok(()) => {
                                for op in committed_ops.drain(..) {
                                    op.on_applied(&mem_db, &mut eviction_heap);
                                }
                                evict_and_log(
                                    &mem_db,
                                    &mut eviction_heap,
                                    max_size,
                                    txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                                );
                            }
                            // Poison the DB: nothing further is applied, producers fail fast, and
                            // the next barrier rejects with this error. The txn's rows stay in mem
                            // with their in-flight counts (retained, not lost, not evictable); the
                            // counts never settle, the pinned rows die with the process, and the
                            // cache rebuilds from the durable tier on the next start.
                            Err(e) => {
                                committed_ops.clear();
                                tracing::error!(target: "layered_db_runner", "consensus DB commit failed; the DB is poisoned: {e}");
                                trip(format!("commit: {e}"));
                            }
                        }
                    } else {
                        txn = Some((current_txn, count - 1));
                    }
                }
            }
            DBMessage::Insert(ins) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = ins.insert_txn(txn) {
                        // keep the failed row in mem (not evictable) so it is not lost
                        tracing::error!(target: "layered_db_runner", "DB TXN Insert failed; the DB is poisoned: {e}");
                        trip(format!("insert: {e}"));
                    } else {
                        committed_ops.push(DeferredOp::Insert(ins));
                    }
                } else if let Err(e) = ins.insert(&db) {
                    // keep the failed row in mem (with its in-flight count, so not evictable)
                    tracing::error!(target: "layered_db_runner", "DB Insert failed; the DB is poisoned: {e}");
                    trip(format!("insert: {e}"));
                } else {
                    ins.on_applied(&mem_db, &mut eviction_heap);
                    evict_and_log(
                        &mem_db,
                        &mut eviction_heap,
                        max_size,
                        txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                    );
                }
            }
            DBMessage::Remove(rm) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = rm.remove_txn(txn, &mem_db) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Remove failed; the DB is poisoned: {e}");
                        trip(format!("remove: {e}"));
                    } else {
                        committed_ops.push(DeferredOp::Remove(rm));
                    }
                } else if let Err(e) = rm.remove(&db, &mem_db) {
                    tracing::error!(target: "layered_db_runner", "DB Remove failed; the DB is poisoned: {e}");
                    trip(format!("remove: {e}"));
                } else {
                    rm.on_applied(&mem_db, &mut eviction_heap);
                    evict_and_log(
                        &mem_db,
                        &mut eviction_heap,
                        max_size,
                        txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                    );
                }
            }
            DBMessage::Clear(clr) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = clr.clear_table_txn(txn, &mem_db) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Clear table failed; the DB is poisoned: {e}");
                        trip(format!("clear: {e}"));
                    } else {
                        committed_ops.push(DeferredOp::Clear(clr));
                    }
                } else if let Err(e) = clr.clear_table(&db, &mem_db) {
                    tracing::error!(target: "layered_db_runner", "DB Clear table failed; the DB is poisoned: {e}");
                    trip(format!("clear: {e}"));
                } else {
                    clr.on_applied(&mem_db, &mut eviction_heap);
                    evict_and_log(
                        &mem_db,
                        &mut eviction_heap,
                        max_size,
                        txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                    );
                }
            }
            // NOTE: proves prior messages were applied, not that an open shared txn committed.
            // Safe at shutdown because consensus writers are torn down before persist runs.
            // A poisoned DB never reaches this arm: its barriers are rejected above with the
            // stored error, so a successful reply here means every write so far landed.
            DBMessage::CaughtUp(tx) => {
                evict_and_log(
                    &mem_db,
                    &mut eviction_heap,
                    max_size,
                    txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                );
                let _ = tx.send(Ok(()));
            }
            DBMessage::Shutdown => break,
        }
        if last_occupancy_log.elapsed() >= OCCUPANCY_LOG_INTERVAL {
            last_occupancy_log = Instant::now();
            tracing::info!(
                target: "storage",
                occupancy = mem_db.mem_size(),
                max = max_size,
                queue = queued,
                open_txns = txn.as_ref().map(|(_, count)| *count).unwrap_or(0),
                "mem cache occupancy"
            );
        }
        if last_compact.elapsed() > Duration::from_secs(86_400) {
            last_compact = Instant::now();
            if let Err(e) = db.compact() {
                tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB (background): {e}");
            }
        }
    }
    tracing::info!(target: "layered_db_runner", "Layered DB thread Shutdown complete");
}

/// In-memory cache layer over a persistent database with background writes.
///
/// A fatal writer failure poisons the DB: the writer applies nothing further, every write
/// attempt fails fast with the stored error, and (when [`Self::with_fatal_signal`] is set) the
/// node's fatal-error channel is signaled so the process winds down instead of continuing to
/// write into a failing tier. Reads keep serving from the mem cache and the durable tier.
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: QueueSender<DB>,
    thread: Option<Arc<JoinHandle<()>>>,
    /// Slot for the node's fatal-error channel, shared with the writer thread: `with_fatal_signal`
    /// sets it, `db_run` signals it on the first fatal failure.
    fatal_signal: Arc<OnceLock<tokio::sync::watch::Sender<Option<String>>>>,
    /// The cold tier point reads fall through to on a hot miss, when attached.
    #[cfg(feature = "cold-storage")]
    cold: Option<Arc<ColdStore>>,
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!(target: "layered_db", "LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(DBMessage::Shutdown) {
                tracing::error!(target: "layered_db", "Error while trying to send shutdown to layered DB thread {e}");
                return; // The thread may not shutdown so don't try to join...
            }
            if let Err(e) =
                Arc::into_inner(self.thread.take().expect("thread handle required to be here"))
                    .expect("only one strong `Arc` reference")
                    .join()
            {
                tracing::error!(target: "layered_db", "Error while waiting for shutdown of layered DB thread {e:?}");
            } else {
                tracing::info!(target: "layered_db", "LayeredDatabase Dropped, DB thread is shutdown");
            }
        }
    }
}

impl<DB: Database> LayeredDatabase<DB> {
    pub fn open(db: DB) -> Self {
        Self::open_with_config(db, CacheConfig::default())
    }

    /// Opens the layered DB with a custom eviction policy: small caches in tests, the default in
    /// production. The DB starts unpoisoned; a fatal writer failure latches the poison
    /// (see [`Self::with_fatal_signal`] and [`db_run`]).
    pub fn open_with_config(db: DB, config: CacheConfig) -> Self {
        // This channel must always remain unbounded. This is necessary for the atomicity
        // guarantee (mem mutation + enqueue in one critical section). It is safe today because
        // mpsc::channel() is unbounded and send() never blocks. However, if the channel is ever
        // changed to a bounded sync_channel (for backpressure), the producer would block with the
        // write lock held while the writer thread is blocked trying to acquire the same lock to
        // process the ops — a classic deadlock.
        let (tx, rx) = mpsc::channel();
        let depth = Arc::new(AtomicUsize::new(0));
        let fatal = Arc::new(OnceLock::new());
        let fatal_signal = Arc::new(OnceLock::new());
        let db_cloned = db.clone();
        let mem_db = MemDatabase::new();
        let mem_db_clone = mem_db.clone();
        let queue_depth = Arc::clone(&depth);
        let fatal_for_thread = Arc::clone(&fatal);
        let signal_for_thread = Arc::clone(&fatal_signal);
        let thread = Some(Arc::new(std::thread::spawn(move || {
            db_run(
                db_cloned,
                mem_db_clone,
                rx,
                queue_depth,
                config.max_size,
                fatal_for_thread,
                signal_for_thread,
            )
        })));
        Self {
            mem_db,
            db,
            tx: QueueSender { tx, depth, fatal },
            thread,
            fatal_signal,
            #[cfg(feature = "cold-storage")]
            cold: None,
        }
    }

    /// Wires the node's fatal-error channel: when the background writer observes a fatal write
    /// failure it signals this channel with the stored error, requesting the node wind-down
    /// (see [`db_run`]). Call before starting writes; the first failure signals it once, and
    /// every barrier and write attempt then rejects with the stored error.
    pub fn with_fatal_signal(self, signal: tokio::sync::watch::Sender<Option<String>>) -> Self {
        let _ = self.fatal_signal.set(signal);
        self
    }

    /// Attaches the cold tier point reads fall through to on a hot miss.
    ///
    /// Reads resolve mem -> db -> cold; writes and iteration never touch the cold tier.
    #[cfg(feature = "cold-storage")]
    pub fn with_cold(mut self, cold: Arc<ColdStore>) -> Self {
        self.cold = Some(cold);
        self
    }

    /// Returns a hot-only view of this database: the same mem cache and writer, no cold layer.
    ///
    /// The archival producer reads and deletes through this view, so "is this row still hot?" is
    /// answered by the hot tier alone and never by the cold copy it is about to create.
    #[cfg(feature = "cold-storage")]
    pub fn without_cold(&self) -> Self {
        let mut view = self.clone();
        view.cold = None;
        view
    }

    /// Returns the attached cold tier, or `None` for a hot-only handle.
    #[cfg(feature = "cold-storage")]
    pub fn cold(&self) -> Option<&Arc<ColdStore>> {
        self.cold.as_ref()
    }

    /// Opens a cold read transaction over the attached tier, resolving the auxiliary index on
    /// this handle's own hot view.
    #[cfg(feature = "cold-storage")]
    fn cold_tx(&self) -> Option<ColdTx<'_>> {
        let cold = self.cold.as_deref()?;
        Some(ColdTx::new(cold, |digest| self.get::<ColdBatchLocations>(digest)))
    }

    /// Serves `key` from the cold tier after a hot miss; `None` when no cold layer is attached.
    #[cfg(feature = "cold-storage")]
    fn cold_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        match self.cold_tx() {
            Some(tx) => tx.get::<T>(key),
            None => Ok(None),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(None)
    }

    /// Answers `contains_key` from the cold tier after a hot miss; false when no cold layer is
    /// attached.
    #[cfg(feature = "cold-storage")]
    fn cold_has<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        match self.cold_tx() {
            Some(tx) => tx.contains_key::<T>(key),
            None => Ok(false),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_has<T: Table>(&self, _key: &T::Key) -> eyre::Result<bool> {
        Ok(false)
    }

    /// Iterates key-descending from the largest key at or below `key` (the reverse of
    /// `skip_to`), merging the cold span beneath the hot tiers.
    ///
    /// Inherent rather than a `Database` method: the storage contract deliberately stays fixed,
    /// so walk-back lives on the concrete layered types that can serve it efficiently. Each hot
    /// side starts at the floor by positioned lookup and steps backwards one seek per row
    /// (`record_prior_to`), so a deep floor never pays for the rows above it.
    pub fn reverse_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db = &self.db;
        let db_first = if self.mem_db.is_clearing::<T>() {
            None
        } else {
            db.get::<T>(key)?
                .map(|value| (key.clone(), value))
                .or_else(|| db.record_prior_to::<T>(key))
        };
        let db_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(db_first, move |(k, _)| db.record_prior_to::<T>(k)));

        let mem = &self.mem_db;
        let mem_first = if mem.is_tombstoned::<T>(key) {
            None
        } else {
            mem.get_marked::<T>(key)?.map(|(_, value)| (key.clone(), value))
        }
        .or_else(|| mem.record_prior_to::<T>(key));
        let mem_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(mem_first, move |(k, _)| mem.record_prior_to::<T>(k)));

        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), true))
    }

    /// Chains the cold-ordered stream for `T` beneath `hot` (see [`merge_cold`]).
    #[cfg(feature = "cold-storage")]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> DBIter<'i, T> {
        merge_cold::<T>(self.cold_tx(), hot, from, reverse)
    }

    /// Cold storage compiled out: iteration is hot-only.
    #[cfg(not(feature = "cold-storage"))]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        _from: Option<&T::Key>,
        _reverse: bool,
    ) -> DBIter<'i, T> {
        hot
    }

    /// Chains the cold-ordered raw stream for `T` beneath `hot` (see [`merge_cold_raw`]).
    #[cfg(feature = "cold-storage")]
    fn chain_cold_raw<'i, T: Table>(
        &'i self,
        hot: DBRawIter<'i>,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> DBRawIter<'i> {
        merge_cold_raw::<T>(self.cold_tx(), hot, from, reverse)
    }

    /// Cold storage compiled out: iteration is hot-only.
    #[cfg(not(feature = "cold-storage"))]
    fn chain_cold_raw<'i, T: Table>(
        &'i self,
        hot: DBRawIter<'i>,
        _from: Option<&T::Key>,
        _reverse: bool,
    ) -> DBRawIter<'i> {
        hot
    }

    /// Returns the wrapped inner database: archival operates beneath the mem cache so its deletes
    /// leave no tombstones that would shadow the cold store.
    pub fn inner(&self) -> &DB {
        &self.db
    }

    /// Returns the writer messages enqueued but not yet applied (advisory, relaxed load).
    pub fn queue_depth(&self) -> usize {
        self.tx.depth()
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn>
        = LayeredDbTx<'txn, DB>
    where
        Self: 'txn;

    type TXMut<'txn>
        = LayeredDbTxMut<'txn, DB>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.open_table::<T>()?;
        self.db.open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx {
            mem_db: self.mem_db.read_txn()?,
            db: self.db.read_txn()?,
            #[cfg(feature = "cold-storage")]
            cold: self.cold.as_deref(),
        })
    }

    /// Write transactions overlap and commit when the last one completes.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        self.tx.send(DBMessage::StartTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(LayeredDbTxMut {
            mem_db: self.mem_db.write_txn()?,
            _db: self.db.clone(),
            tx: self.tx.clone(),
        })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if self.mem_db.is_tombstoned::<T>(key) {
            return Ok(false);
        }
        if self.mem_db.contains_key::<T>(key)? {
            return Ok(true);
        }
        if self.mem_db.is_clearing::<T>() {
            // The table's clear is pending: keys not live in the cache are gone, not stale.
            return self.cold_has::<T>(key);
        }
        if self.db.contains_key::<T>(key)? {
            return Ok(true);
        }
        self.cold_has::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // The hot answer (tombstone-aware mem overlay, then the persistent tier) wins; only a
        // full hot miss consults the cold tier, so a row removed from hot still serves its
        // archived copy.
        let hot = if self.mem_db.is_tombstoned::<T>(key) {
            None
        } else if let Some((_, val)) = self.mem_db.get_marked::<T>(key)? {
            Some(val)
        } else if self.mem_db.is_clearing::<T>() {
            // The table's clear is pending: keys evicted from the cache (or never cached, e.g.
            // right after startup) are gone, not stale - do not fall through to the persistent
            // tier.
            None
        } else {
            self.db.get::<T>(key)?
        };
        match hot {
            Some(val) => Ok(Some(val)),
            None => self.cold_get::<T>(key),
        }
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        // Checked before the mem mutation: `insert_queued` has no rollback, so a poisoned DB
        // must not touch the cache at all.
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        // The mem mutation, the in-flight increment and the enqueue share one critical section:
        // channel order equals mem order, so a zero in-flight count is a sound "no queued ops".
        self.mem_db.insert_queued::<T>(key, value, || {
            let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
            self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
        })
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        self.mem_db.remove_queued::<T>(key, || {
            let rm = Box::new(KeyRemove::<T> { key: key.clone() });
            self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
        })
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        // The clear tombstones every row and bumps each in-flight count under the same lock that
        // captures the key set for the writer, so no tombstone is evicted before the clear lands.
        self.mem_db.clear_table_queued::<T>(|keys| {
            let clr = Box::new(ClearTable::<T> { _marker: PhantomData, keys });
            self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
        })
    }

    fn is_empty<T: Table>(&self) -> bool {
        if !self.mem_db.is_empty::<T>() {
            return false;
        }
        // merged iterator respects tombstones
        self.iter::<T>().next().is_none()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            // Pending clear: the persistent tier is stale, so the merge must not see its rows.
            Box::new(std::iter::empty())
        } else {
            self.db.iter::<T>()
        };
        let mem_iter = self.mem_db.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, false)
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.raw_iter::<T>()
        };
        let mem_iter = self.mem_db.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, false)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.skip_to::<T>(key)?
        };
        let mem_iter = self.mem_db.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), false))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.reverse_iter::<T>()
        };
        let mem_iter = self.mem_db.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, true)
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = if self.mem_db.is_clearing::<T>() {
            Box::new(std::iter::empty())
        } else {
            self.db.reverse_raw_iter::<T>()
        };
        let mem_iter = self.mem_db.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, true)
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn persist(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        // A poisoned DB fails immediately with the stored error (the writer would reject the
        // barrier with the same error anyway) — without enqueuing a CaughtUp, so a loop
        // retrying persist() on a poisoned DB does not spam the writer queue (see
        // sync_persist() which also short-circuits before send).
        let tx = self.tx.clone();
        let poisoned = poisoned_error(self.tx.fatal());
        async move {
            if let Some(e) = poisoned {
                return Err(e);
            }
            let (ca_tx, ca_rx) = oneshot::channel();
            let depth_at_send = tx.depth();
            let started = Instant::now();
            let send_result = tx.send(DBMessage::CaughtUp(ca_tx));
            match send_result {
                Ok(()) => match ca_rx.await {
                    Ok(Ok(())) => {
                        log_persist_latency(started.elapsed(), depth_at_send);
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        tracing::error!(target: "storage", "consensus DB persist: write failed since last flush: {e}");
                        Err(eyre::eyre!("consensus DB persist: {e}"))
                    }
                    Err(_) => {
                        tracing::error!(target: "storage", "consensus DB persist: caught-up reply dropped before completion");
                        Err(eyre::eyre!("consensus DB persist: caught-up reply dropped"))
                    }
                },
                Err(_) => {
                    tracing::error!(target: "storage", "consensus DB persist: writer thread gone, in-flight writes not flushed");
                    Err(eyre::eyre!("consensus DB persist: writer thread gone"))
                }
            }
        }
    }

    /// Blocks the calling thread until the writer has applied all queued messages; never call it
    /// on an async runtime worker.
    ///
    /// Returns any write/commit failure the writer observed since the previous barrier, so a
    /// caller cannot mistake a failed flush for durability. Callers must treat the error as
    /// fatal (fault the node): a failed flush poisons the DB — the writer applies nothing
    /// further, every write method returns the stored error immediately, and the failed rows
    /// stay pinned in the mem cache (in-flight counts never settle) until a restart rebuilds
    /// it, so an ignored error leaks them unboundedly.
    fn sync_persist(&self) -> eyre::Result<()> {
        if let Some(e) = poisoned_error(self.tx.fatal()) {
            return Err(e);
        }
        let (tx, mut rx) = oneshot::channel();
        let depth_at_send = self.tx.depth();
        let started = Instant::now();
        let r = self
            .tx
            .send(DBMessage::CaughtUp(tx))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"));

        if r.is_ok() {
            loop {
                match rx.try_recv() {
                    Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(100)),
                    Err(TryRecvError::Closed) => {
                        return Err(eyre::eyre!("consensus DB sync_persist: reply dropped"));
                    }
                    Ok(Ok(())) => {
                        log_persist_latency(started.elapsed(), depth_at_send);
                        return Ok(());
                    }
                    Ok(Err(e)) => {
                        tracing::error!(target: "storage", "consensus DB sync_persist: write failed: {e}");
                        return Err(eyre::eyre!("consensus DB sync_persist: {e}"));
                    }
                }
            }
        }
        r
    }
}

trait InsertTrait<DB: Database>: Send + 'static {
    fn insert(&self, db: &DB) -> eyre::Result<()>;
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
    /// Release the in-flight op for the inserted key once the write is durably applied; at zero
    /// the key becomes an eviction candidate.
    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap);
}

trait RemoveTrait<DB: Database>: Send + 'static {
    fn remove(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()>;
    fn remove_txn(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()>;
    /// Release the in-flight op for the removed key(s) once the delete is durably applied.
    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap);
}

trait ClearTrait<DB: Database>: Send + 'static {
    fn clear_table(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()>;
    fn clear_table_txn(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()>;
    /// Release the in-flight op of every tombstoned key once the clear is durably applied.
    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap);
}

struct KeyValueInsert<T: Table> {
    key: T::Key,
    value: T::Value,
}

struct KeyRemove<T: Table> {
    key: T::Key,
}

struct KeyRemoveBatch<T: Table> {
    keys: Vec<T::Key>,
}

struct ClearTable<T: Table> {
    /// Raw keys tombstoned by the producer's clear: each must release its in-flight op when the
    /// persistent clear applies, so no tombstone is evicted before then.
    keys: Vec<Vec<u8>>,
    _marker: PhantomData<T>,
}

impl<T: Table, DB: Database> InsertTrait<DB> for KeyValueInsert<T> {
    fn insert(&self, db: &DB) -> eyre::Result<()> {
        db.insert::<T>(&self.key, &self.value)
    }
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }
    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap) {
        mem_db.on_op_applied(T::NAME, &encode_key(&self.key), heap);
    }
}

// Tombstones are NOT eagerly cleared from mem after persistent delete;
// doing so races with the main thread's reads (MDBX write not yet visible).
// They are released by the eviction heap once their in-flight count settles.
impl<T: Table, DB: Database> RemoveTrait<DB> for KeyRemove<T> {
    fn remove(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()> {
        // skip if key was re-inserted after the remove was queued
        if mem_db.contains_key::<T>(&self.key)? {
            return Ok(());
        }
        db.remove::<T>(&self.key)
    }

    fn remove_txn(
        &self,
        txn: &mut <DB as Database>::TXMut<'_>,
        mem_db: &MemDatabase,
    ) -> eyre::Result<()> {
        if mem_db.contains_key::<T>(&self.key)? {
            return Ok(());
        }
        txn.remove::<T>(&self.key)
    }

    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap) {
        mem_db.on_op_applied(T::NAME, &encode_key(&self.key), heap);
    }
}

// Batched `KeyRemove`: same per-key re-insert guard, one message for a whole-epoch prune (see
// [`LayeredDbTxMut::evict_persistent_batch`]).
impl<T: Table, DB: Database> RemoveTrait<DB> for KeyRemoveBatch<T> {
    fn remove(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()> {
        for key in &self.keys {
            // skip if the key was re-inserted after the remove was queued
            if mem_db.contains_key::<T>(key)? {
                continue;
            }
            db.remove::<T>(key)?;
        }
        Ok(())
    }

    fn remove_txn(
        &self,
        txn: &mut <DB as Database>::TXMut<'_>,
        mem_db: &MemDatabase,
    ) -> eyre::Result<()> {
        for key in &self.keys {
            if mem_db.contains_key::<T>(key)? {
                continue;
            }
            txn.remove::<T>(key)?;
        }
        Ok(())
    }

    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap) {
        for key in &self.keys {
            mem_db.on_op_applied(T::NAME, &encode_key(key), heap);
        }
    }
}

impl<T: Table, DB: Database> ClearTrait<DB> for ClearTable<T> {
    fn clear_table(&self, db: &DB, _mem_db: &MemDatabase) -> eyre::Result<()> {
        // mem_db already cleared by main thread; re-clearing here would race with new inserts
        db.clear_table::<T>()
    }

    fn clear_table_txn(
        &self,
        txn: &mut <DB as Database>::TXMut<'_>,
        _mem_db: &MemDatabase,
    ) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }

    fn on_applied(&self, mem_db: &MemDatabase, heap: &mut EvictionHeap) {
        mem_db.on_clear_applied::<T>(&self.keys, heap);
    }
}

enum DBMessage<DB: Database> {
    StartTxn,
    CommitTxn,
    Insert(Box<dyn InsertTrait<DB>>),
    Remove(Box<dyn RemoveTrait<DB>>),
    Clear(Box<dyn ClearTrait<DB>>),
    CaughtUp(oneshot::Sender<Result<(), String>>),
    Shutdown,
}

impl<DB: Database> Debug for DBMessage<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBMessage::StartTxn => write!(f, "StartTxn"),
            DBMessage::CommitTxn => write!(f, "CommitTxn"),
            DBMessage::Insert(_) => write!(f, "Insert"),
            DBMessage::Remove(_) => write!(f, "Remove"),
            DBMessage::Clear(_) => write!(f, "Clear"),
            DBMessage::CaughtUp(_) => write!(f, "CaughtUp"),
            DBMessage::Shutdown => write!(f, "Shutdown"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        CacheConfig, DBMessage, EvictionHeap, InsertTrait, KeyValueInsert, LayeredDatabase,
        QUEUE_HIGH_WATER_MARK, QUEUE_PACE_SLEEP,
    };
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{
        mdbx::{MdbxConfig, MdbxDatabase},
        mem_db::MemDatabase,
        test::*,
    };
    use rayls_infrastructure_types::{DBIter, DBRawIter, Database, DbTxMut, Table};
    use std::{path::Path, sync::Arc, time::Instant};
    use tempfile::tempdir;

    #[cfg(feature = "redb")]
    fn open_redb(path: &Path) -> LayeredDatabase<ReDB> {
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    fn open_mdbx(path: &Path) -> LayeredDatabase<MdbxDatabase> {
        let db = MdbxDatabase::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    /// A [`MemDatabase`] whose `clear_table` parks until released: lets a test hold the
    /// producer's clear window open deterministically while the writer is parked mid-apply.
    #[derive(Clone, Debug)]
    struct BlockingClearDb {
        inner: MemDatabase,
        state: Arc<parking_lot::Mutex<BlockState>>,
        cv: Arc<parking_lot::Condvar>,
    }

    #[derive(Debug, Default)]
    struct BlockState {
        /// The next `clear_table` parks until `release_clear` is called.
        block: bool,
        /// The writer is parked inside `clear_table`.
        entered: bool,
    }

    impl BlockingClearDb {
        fn new() -> Self {
            Self {
                inner: MemDatabase::new(),
                state: Arc::new(parking_lot::Mutex::new(BlockState::default())),
                cv: Arc::new(parking_lot::Condvar::new()),
            }
        }

        /// Park the writer's next `clear_table` until [`Self::release_clear`].
        fn block_clear(&self) {
            self.state.lock().block = true;
        }

        /// Signal the parked writer to proceed with the persistent clear.
        fn release_clear(&self) {
            self.state.lock().block = false;
            self.cv.notify_all();
        }

        /// Blocks until the writer is parked inside `clear_table`: at that point the producer's
        /// clear window is provably still open.
        fn wait_clear_entered(&self) {
            let mut state = self.state.lock();
            while !state.entered {
                self.cv.wait(&mut state);
            }
        }
    }

    impl Database for BlockingClearDb {
        type TX<'txn>
            = crate::mem_db::MemDbTx<'txn>
        where
            Self: 'txn;

        type TXMut<'txn>
            = crate::mem_db::MemDbTxMut<'txn>
        where
            Self: 'txn;

        fn open_table<T: Table>(&self) -> eyre::Result<()> {
            self.inner.open_table::<T>()
        }
        fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
            self.inner.read_txn()
        }
        fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
            self.inner.write_txn()
        }
        fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
            self.inner.contains_key::<T>(key)
        }
        fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
            self.inner.get::<T>(key)
        }
        fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
            self.inner.insert::<T>(key, value)
        }
        fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
            self.inner.remove::<T>(key)
        }
        fn clear_table<T: Table>(&self) -> eyre::Result<()> {
            {
                let mut state = self.state.lock();
                if state.block {
                    state.entered = true;
                    self.cv.notify_all();
                    while state.block {
                        self.cv.wait(&mut state);
                    }
                }
            }
            self.inner.clear_table::<T>()
        }
        fn is_empty<T: Table>(&self) -> bool {
            self.inner.is_empty::<T>()
        }
        fn iter<T: Table>(&self) -> DBIter<'_, T> {
            self.inner.iter::<T>()
        }
        fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
            self.inner.raw_iter::<T>()
        }
        fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
            self.inner.skip_to::<T>(key)
        }
        fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
            self.inner.reverse_iter::<T>()
        }
        fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
            self.inner.reverse_raw_iter::<T>()
        }
        fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
            self.inner.record_prior_to::<T>(key)
        }
        fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
            self.inner.last_record::<T>()
        }
    }

    /// Releases the writer's parked clear on drop, so a panicked assertion cannot deadlock the
    /// writer join at test teardown.
    struct ClearRelease<'a>(&'a BlockingClearDb);

    impl Drop for ClearRelease<'_> {
        fn drop(&mut self) {
            self.0.release_clear();
        }
    }

    /// `clear_table` tombstones only the cached rows, so keys already evicted from the cache have
    /// nothing to hide behind: without the pending-clear gate they read stale pre-clear values
    /// from the persistent tier until the writer applies the clear. The pending-clear flag must
    /// close that window for `get`, `contains_key` and every iterator.
    #[test]
    fn clear_hides_evicted_keys_until_the_clear_applies() {
        let inner = BlockingClearDb::new();
        let db = LayeredDatabase::open_with_config(inner.clone(), CacheConfig { max_size: 2 });
        db.open_table::<TestTable>().expect("open layered table");

        db.insert::<TestTable>(&1, &"one".to_string()).expect("insert");
        db.insert::<TestTable>(&2, &"two".to_string()).expect("insert");
        db.insert::<TestTable>(&3, &"three".to_string()).expect("insert");
        db.sync_persist().expect("persist");
        // Key 1 settled first and was evicted: it is now live only in the persistent tier.
        assert_eq!(db.mem_db.mem_size(), 2, "key 1 must be evicted");
        assert!(!db.mem_db.contains_key::<TestTable>(&1).unwrap());

        inner.block_clear();
        let _release = ClearRelease(&inner);
        db.clear_table::<TestTable>().expect("clear");
        inner.wait_clear_entered();
        // The writer is parked inside the persistent clear: the producer's window is open.
        assert!(db.mem_db.is_clearing::<TestTable>());

        // The evicted key must not leak from the persistent tier while the clear is pending.
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "evicted key must read as cleared");
        assert_eq!(db.get::<TestTable>(&2).unwrap(), None, "cached key must read as cleared");
        assert_eq!(db.get::<TestTable>(&3).unwrap(), None, "cached key must read as cleared");
        assert!(!db.contains_key::<TestTable>(&1).unwrap(), "evicted key must not contain");
        assert!(
            db.iter::<TestTable>().collect::<Vec<_>>().is_empty(),
            "iter must not leak stale rows"
        );
        assert!(db.is_empty::<TestTable>(), "table must read empty while the clear is pending");
        // Writes during the window still read through the mem overlay.
        db.insert::<TestTable>(&4, &"four".to_string()).expect("insert");
        assert_eq!(db.get::<TestTable>(&4).unwrap(), Some("four".to_string()));

        inner.release_clear();
        db.sync_persist().expect("persist");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "clear must land");
        assert!(!db.mem_db.is_clearing::<TestTable>(), "pending-clear flag must settle");

        db.insert::<TestTable>(&5, &"five".to_string()).expect("insert");
        db.sync_persist().expect("persist");
        assert_eq!(db.get::<TestTable>(&5).unwrap(), Some("five".to_string()));
    }

    /// `evict_persistent_batch` must drop exactly the given keys from the durable layer and leave
    /// every other key intact, matching a per-key remove loop. Guards the batched
    /// cold-prune path that collapses a whole epoch's row deletes into one writer message.
    #[test]
    fn evict_persistent_batch_removes_exactly_the_given_keys() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx(temp_dir.path());

        db.with_write_txn(|txn| {
            for i in 0..10u64 {
                txn.insert::<TestTable>(&i, &format!("v{i}"))?;
            }
            Ok(())
        })
        .expect("seed");
        db.sync_persist().expect("persist");

        // An empty batch is a no-op: nothing is removed.
        db.with_write_txn(|txn| txn.evict_persistent_batch::<TestTable>(&[])).expect("empty batch");
        db.sync_persist().expect("persist");
        for i in 0..10u64 {
            assert!(db.contains_key::<TestTable>(&i).unwrap(), "empty batch removed key {i}");
        }

        let evicted = [1u64, 3, 5, 7];
        db.with_write_txn(|txn| txn.evict_persistent_batch::<TestTable>(&evicted))
            .expect("batch evict");
        db.sync_persist().expect("persist");

        for i in 0..10u64 {
            let present = db.contains_key::<TestTable>(&i).unwrap();
            if evicted.contains(&i) {
                assert!(!present, "evicted key {i} must be gone from the durable layer");
            } else {
                assert!(present, "unlisted key {i} must remain");
            }
        }
    }

    /// A write the backend rejects (MAP_FULL here, as on a full disk) must surface through
    /// `persist`, not be reported as a successful flush.
    #[tokio::test]
    async fn test_failed_write_is_surfaced_by_persist() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let cfg = MdbxConfig::default().with_max_db_size(1024 * 1024).with_growth_step(256 * 1024);
        let mdbx = MdbxDatabase::open_with_config(temp_dir.path(), cfg).expect("open mdbx");
        mdbx.open_table::<TestTable>().expect("open mdbx table");
        let db = LayeredDatabase::open(mdbx);
        db.open_table::<TestTable>().expect("open layered table");

        // Queue far more data than the map can hold so the background writer hits MAP_FULL.
        let big = "x".repeat(4096);
        let _ = db.with_write_txn(|txn| {
            for i in 0..4_000u64 {
                txn.insert::<TestTable>(&i, &big)?;
            }
            Ok(())
        });

        // The failure is only observable at the durability barrier; it must not be reported Ok.
        assert!(
            db.persist().await.is_err(),
            "persist must surface the write failure instead of reporting success"
        );
    }

    /// The blocking sibling of `test_failed_write_is_surfaced_by_persist`: `sync_persist` must
    /// also surface a failed commit, so blocking callers (cold archival) cannot mistake a failed
    /// flush for durability and proceed to prune rows the index never durably landed.
    #[test]
    fn test_failed_commit_is_surfaced_by_sync_persist() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let cfg = MdbxConfig::default().with_max_db_size(1024 * 1024).with_growth_step(256 * 1024);
        let mdbx = MdbxDatabase::open_with_config(temp_dir.path(), cfg).expect("open mdbx");
        mdbx.open_table::<TestTable>().expect("open mdbx table");
        let db = LayeredDatabase::open(mdbx);
        db.open_table::<TestTable>().expect("open layered table");

        // Queue far more data than the map can hold so the background writer hits MAP_FULL.
        let big = "x".repeat(4096);
        let _ = db.with_write_txn(|txn| {
            for i in 0..4_000u64 {
                txn.insert::<TestTable>(&i, &big)?;
            }
            Ok(())
        });

        assert!(
            db.sync_persist().is_err(),
            "sync_persist must surface the failed commit instead of reporting success"
        );
    }

    /// A layered MDBX whose map is sized so the background writer hits MAP_FULL, as on a full
    /// disk: the first failing write poisons the DB.
    fn open_poisonable_mdbx(path: &Path) -> LayeredDatabase<MdbxDatabase> {
        let cfg = MdbxConfig::default().with_max_db_size(1024 * 1024).with_growth_step(256 * 1024);
        let mdbx = MdbxDatabase::open_with_config(path, cfg).expect("open mdbx");
        mdbx.open_table::<TestTable>().expect("open mdbx table");
        let db = LayeredDatabase::open(mdbx);
        db.open_table::<TestTable>().expect("open layered table");
        db
    }

    /// Queue far more data than the map can hold so the background writer's next commit fails and
    /// poisons the DB. The failure is only observable at the next durability barrier.
    fn queue_poisoning_writes(db: &LayeredDatabase<MdbxDatabase>) {
        let big = "x".repeat(4096);
        let _ = db.with_write_txn(|txn| {
            for i in 0..4_000u64 {
                txn.insert::<TestTable>(&i, &big)?;
            }
            Ok(())
        });
    }

    fn assert_poisoned(err: &eyre::Error, what: &str) {
        assert!(
            err.to_string().contains("poisoned"),
            "{what} on a poisoned DB must fail fast with the poison reason: {err}"
        );
    }

    /// Once a write failure has been observed, every write path fails fast with the stored error
    /// and the DB stays poisoned for its lifetime: nothing is retried, queued, or applied.
    #[tokio::test]
    async fn test_poisoned_db_fails_further_writes_fast() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_poisonable_mdbx(temp_dir.path());
        queue_poisoning_writes(&db);
        assert!(db.persist().await.is_err(), "persist must surface the write failure");

        let big = "x".repeat(4096);
        let err = db.insert::<TestTable>(&7, &big).expect_err("insert must fail fast");
        assert_poisoned(&err, "insert");
        let err = db.remove::<TestTable>(&7).expect_err("remove must fail fast");
        assert_poisoned(&err, "remove");
        let err = db.clear_table::<TestTable>().expect_err("clear_table must fail fast");
        assert_poisoned(&err, "clear_table");
        let err = db.write_txn().map(|_| ()).expect_err("write_txn must fail fast");
        assert_poisoned(&err, "write_txn");
        let err = db.persist().await.expect_err("persist must fail fast");
        assert_poisoned(&err, "persist");
        let err = db.sync_persist().expect_err("sync_persist must fail fast");
        assert_poisoned(&err, "sync_persist");
    }

    /// After the poison latches, the writer applies nothing further — not even a message a
    /// producer enqueued before it could fail fast — and reads keep serving from the cache and
    /// the durable tier.
    #[test]
    fn test_poisoned_writer_applies_nothing_further() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_poisonable_mdbx(temp_dir.path());

        // A row that lands before the failure, so reads have something to serve afterwards.
        // Key 9000 sits outside the poisoning batch's range (0..4000), so the batch cannot
        // clobber it in the mem cache.
        db.insert::<TestTable>(&9000, &"pre-failure".to_string()).expect("pre-failure insert");
        db.sync_persist().expect("pre-failure persist");

        queue_poisoning_writes(&db);
        assert!(db.sync_persist().is_err(), "sync_persist must surface the write failure");

        // A message enqueued directly, as if by a producer that slipped past the fail-fast check:
        // the poisoned writer must discard it, not apply it.
        let ins = Box::new(KeyValueInsert::<TestTable> { key: 99, value: "late".to_string() });
        db.tx.send(DBMessage::Insert(ins)).expect("the poisoned writer still drains messages");

        // The depth gauge is decremented per message the writer takes off the queue (applied or
        // discarded), so zero means the writer has drained past the late insert.
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while db.tx.depth() > 0 {
            assert!(
                Instant::now() < deadline,
                "the writer did not drain the queue after the poison"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert!(
            db.db.get::<TestTable>(&99).expect("durable read").is_none(),
            "a message enqueued after the poison must not be applied to the durable tier"
        );
        assert_eq!(
            db.get::<TestTable>(&9000).expect("cache read").as_deref(),
            Some("pre-failure"),
            "reads must keep serving after the poison"
        );
    }

    /// `with_fatal_signal` wires the node's wind-down channel: the first fatal writer failure
    /// signals it with the stored error, so the process stops instead of writing further.
    #[tokio::test]
    async fn test_fatal_signal_fires_on_writer_failure() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let (signal, rx) = tokio::sync::watch::channel(None);
        let db = open_poisonable_mdbx(temp_dir.path()).with_fatal_signal(signal);

        queue_poisoning_writes(&db);
        assert!(db.persist().await.is_err(), "persist must surface the write failure");

        assert!(
            rx.borrow().is_some(),
            "the node fatal-error channel must be signaled on the first fatal failure"
        );
    }

    /// Writer message that parks `db_run` until the paired sender is dropped, so a test can pin a
    /// real enqueued backlog behind a stalled writer. It signals `reached` just before parking, so
    /// a test can wait deterministically for the writer to have drained up to the gate instead
    /// of sleeping on a fixed timeout.
    struct WriterGate {
        park: std::sync::mpsc::Receiver<()>,
        reached: std::sync::mpsc::Sender<()>,
    }

    impl<DB: Database> InsertTrait<DB> for WriterGate {
        fn insert(&self, _db: &DB) -> eyre::Result<()> {
            let _ = self.reached.send(());
            let _ = self.park.recv();
            Ok(())
        }
        fn insert_txn(&self, _txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
            let _ = self.reached.send(());
            let _ = self.park.recv();
            Ok(())
        }
        fn on_applied(&self, _mem_db: &MemDatabase, _heap: &mut EvictionHeap) {}
    }

    /// A `(release, reached, gate)` triple: dropping `release` lets the writer pass the gate, and
    /// `reached` fires once the writer has drained up to and parked at the gate.
    fn writer_gate() -> (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>, WriterGate) {
        let (release, park) = std::sync::mpsc::channel::<()>();
        let (reached, reached_rx) = std::sync::mpsc::channel::<()>();
        (release, reached_rx, WriterGate { park, reached })
    }

    /// A producer bursting into a seeded writer backlog must be paced above the high-water mark,
    /// so the backlog a boundary `persist` drains grows at the pace rate, not the burst rate; the
    /// same producer below the mark must run unpaced.
    #[test]
    fn writer_backlog_paces_producers_above_the_high_water_mark() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open(inner);
        db.open_table::<TestTable>().expect("open layered table");

        // Park the writer behind a gate so the seeded backlog cannot drain mid-measurement.
        let (release, _reached, gate) = writer_gate();
        db.tx.send(DBMessage::Insert(Box::new(gate))).expect("gate enqueue");

        // Seed one message past the mark: every enqueue here is at or below it, hence unpaced.
        let value = "v".to_string();
        let seed_started = Instant::now();
        for key in 0..=(QUEUE_HIGH_WATER_MARK as u64) {
            db.insert::<TestTable>(&key, &value).expect("seed insert");
        }
        let seed_elapsed = seed_started.elapsed();

        // With the queue pinned above the mark, every further enqueue pays the pace sleep.
        const PACED: u32 = 50;
        let paced_started = Instant::now();
        for key in 0..PACED as u64 {
            db.insert::<TestTable>(&(u64::MAX - key), &value).expect("paced insert");
        }
        let paced_elapsed = paced_started.elapsed();

        // Unblock the writer before asserting so a failure cannot hang the drop-side join.
        drop(release);

        assert!(
            paced_elapsed >= QUEUE_PACE_SLEEP * PACED,
            "{PACED} enqueues above the high-water mark finished in {paced_elapsed:?}: \
             producers into a writer backlog must be paced",
        );
        // Half the would-be pace time is far above any real unpaced burst, so a false failure
        // needs the machine frozen for seconds, while wrongly-paced seeding deterministically
        // exceeds it.
        assert!(
            seed_elapsed < QUEUE_PACE_SLEEP * (QUEUE_HIGH_WATER_MARK as u32 / 2),
            "seeding below the high-water mark took {seed_elapsed:?}: enqueues under the high-water mark \
             must not be paced",
        );
    }

    /// Insert a burst of keys into a tiny cache and confirm the writer evicts settled keys in
    /// recency order until the cache fits `max_size`, with the newest rows surviving in mem.
    #[test]
    fn eviction_keeps_cache_at_max_size_and_evicts_oldest_settled() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 3 });
        db.open_table::<TestTable>().expect("open layered table");

        for i in 0..10u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.sync_persist().expect("persist");

        assert_eq!(db.mem_db.mem_size(), 3, "cache must be trimmed to max_size");
        // The newest rows survive hot; older ones fall through to the persistent tier.
        for i in 0..10u64 {
            assert_eq!(
                db.get::<TestTable>(&i).unwrap().as_deref(),
                Some(format!("v{i}").as_str()),
                "every key must still be readable after eviction"
            );
        }
        for i in 7..10u64 {
            assert!(
                db.mem_db.contains_key::<TestTable>(&i).unwrap(),
                "newest key {i} must stay hot"
            );
        }
        for i in 0..7u64 {
            assert!(
                !db.mem_db.contains_key::<TestTable>(&i).unwrap(),
                "settled key {i} must be evicted"
            );
        }
    }

    /// A tombstone whose remove is still queued must not be evicted, even when the cache is over
    /// its cap: the row shields reads from the persistent tier until the delete lands. Once the
    /// remove applies, the tombstone settles and becomes an eviction candidate.
    #[test]
    fn eviction_waits_for_in_flight_ops() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 1 });
        db.open_table::<TestTable>().expect("open layered table");

        // Two gates park the writer mid-drain so the in-flight window is observable.
        let (release, gate1_reached, gate1) = writer_gate();
        let (release2, gate2_reached, gate2) = writer_gate();
        db.tx.send(DBMessage::Insert(Box::new(gate1))).expect("gate1 enqueue");
        db.insert::<TestTable>(&1, &"one".to_string()).expect("insert k1");
        db.tx.send(DBMessage::Insert(Box::new(gate2))).expect("gate2 enqueue");
        db.remove::<TestTable>(&1).expect("remove k1");
        db.insert::<TestTable>(&2, &"two".to_string()).expect("insert k2");

        // Let the writer drain past K1's insert, then wait until it parks at gate2: K1's tombstone
        // and K2's row are both in flight (count > 0), so eviction (run at every apply) must not
        // drop anything even though the cache holds 2 rows against a cap of 1. Gate messages are
        // processed in enqueue order, so gate2 being reached proves gate1 was drained.
        drop(release);
        let _ = gate1_reached.recv().expect("writer drains past gate1");
        let _ = gate2_reached.recv().expect("writer parks at gate2");
        assert_eq!(
            db.mem_db.mem_size(),
            2,
            "in-flight rows must not be evicted while ops are queued"
        );
        assert!(
            db.mem_db.is_tombstoned::<TestTable>(&1),
            "K1's tombstone must survive while queued"
        );
        drop(release2);
        db.sync_persist().expect("persist");

        // The remove applied, so K1's settled tombstone is now evictable; only K2 stays hot.
        assert_eq!(db.mem_db.mem_size(), 1, "settled rows must be trimmed to max_size");
        assert!(
            !db.mem_db.is_tombstoned::<TestTable>(&1),
            "K1 tombstone must be evicted post-apply"
        );
        assert!(db.mem_db.contains_key::<TestTable>(&2).unwrap(), "K2 must stay hot");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "K1 is durably removed");
        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
    }

    /// A tombstone for a key that was never in the cache (deleted from the persistent tier only)
    /// must not leak: it settles when the remove applies and is evicted like any other row.
    #[test]
    fn removed_never_inserted_tombstone_is_evicted() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 1 });
        db.open_table::<TestTable>().expect("open layered table");

        db.remove::<TestTable>(&999).expect("remove never-inserted key");
        db.insert::<TestTable>(&1, &"one".to_string()).expect("insert");
        db.insert::<TestTable>(&2, &"two".to_string()).expect("insert");
        db.sync_persist().expect("persist");

        assert_eq!(db.get::<TestTable>(&999).unwrap(), None);
        assert!(!db.mem_db.is_tombstoned::<TestTable>(&999), "stray tombstone must be evicted");
        assert_eq!(db.mem_db.mem_size(), 1, "cache must be trimmed");
    }

    /// A `clear_table` tombstones every cached row and holds them (in flight) until the persistent
    /// clear lands; only then are the tombstones released and evicted.
    #[test]
    fn clear_tombstones_are_held_until_the_persistent_clear_lands() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 2 });
        db.open_table::<TestTable>().expect("open layered table");

        db.insert::<TestTable>(&1, &"one".to_string()).expect("insert");
        db.insert::<TestTable>(&2, &"two".to_string()).expect("insert");
        db.insert::<TestTable>(&3, &"three".to_string()).expect("insert");
        db.clear_table::<TestTable>().expect("clear");
        db.insert::<TestTable>(&4, &"four".to_string()).expect("insert");
        db.sync_persist().expect("persist");

        assert_eq!(db.get::<TestTable>(&1).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&2).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&3).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&4).unwrap(), Some("four".to_string()));
        assert_eq!(db.mem_db.mem_size(), 2, "cleared tombstones settle and are evicted");
    }

    /// Reading a hot key refreshes its recency (lock-free, throttled), so it survives eviction over
    /// a sibling that settled at the same time but was never read since.
    #[test]
    fn read_recency_protects_a_hot_key() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 2 });
        db.open_table::<TestTable>().expect("open layered table");

        for i in 1..=3u64 {
            db.insert::<TestTable>(&i, &format!("v{i}")).expect("insert");
        }
        db.sync_persist().expect("persist");
        // 1 settled first and was evicted; 2 and 3 are hot.
        assert!(db.mem_db.contains_key::<TestTable>(&2).unwrap());
        assert!(db.mem_db.contains_key::<TestTable>(&3).unwrap());

        // Cross the recency throttle window, then read key 2: its clock bumps.
        std::thread::sleep(std::time::Duration::from_millis(150));
        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("v2".to_string()));

        // Overflow the cache: without the read bump, key 2 (settled first) would be evicted.
        db.insert::<TestTable>(&4, &"v4".to_string()).expect("insert");
        db.sync_persist().expect("persist");

        assert_eq!(db.mem_db.mem_size(), 2);
        assert!(db.mem_db.contains_key::<TestTable>(&2).unwrap(), "read key must stay hot");
        assert!(db.mem_db.contains_key::<TestTable>(&4).unwrap(), "newest key must stay hot");
        assert!(
            !db.mem_db.contains_key::<TestTable>(&3).unwrap(),
            "unread sibling must be evicted"
        );
    }

    /// Remove-then-reinsert of the same key must not leak its in-flight count: the remove's apply
    /// is skipped by the re-insert guard, but it still settles, so the row stays evictable.
    #[test]
    fn guard_skipped_remove_still_settles() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open_with_config(inner, CacheConfig { max_size: 1 });
        db.open_table::<TestTable>().expect("open layered table");

        db.insert::<TestTable>(&1, &"v1".to_string()).expect("insert");
        db.remove::<TestTable>(&1).expect("remove");
        db.insert::<TestTable>(&1, &"v3".to_string()).expect("reinsert");
        db.insert::<TestTable>(&2, &"v2".to_string()).expect("insert");
        db.insert::<TestTable>(&3, &"v3".to_string()).expect("insert");
        db.sync_persist().expect("persist");

        assert_eq!(db.get::<TestTable>(&1).unwrap(), Some("v3".to_string()));
        assert_eq!(db.mem_db.mem_size(), 1, "no in-flight leak: the row is evictable");
    }

    #[test]
    fn test_layereddb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_contains_key(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_contains_key(db);
    }

    #[test]
    fn test_layereddb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_get(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_get(db);
    }

    #[test]
    fn test_layereddb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_get(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_get(db);
    }

    #[test]
    fn test_layereddb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_skip(db);
    }

    #[test]
    fn test_layereddb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip_to_previous_simple(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_skip_to_previous_simple(db);
    }

    #[test]
    fn test_layereddb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_skip_to_previous_gap(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter_skip_to_previous_gap(db);
    }

    #[test]
    fn test_layereddb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_remove(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_remove(db);
    }

    #[test]
    fn test_layereddb_remove_then_insert_new() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_remove_then_insert_new(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_remove_then_insert_new(db);
    }

    #[test]
    fn test_layereddb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter(db);
    }

    #[test]
    fn test_layereddb_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_reverse(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter_reverse(db);
    }

    #[test]
    fn test_layereddb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_clear(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_clear(db);
    }

    #[test]
    fn test_layereddb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_is_empty(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_is_empty(db);
    }

    #[test]
    fn test_layereddb_clear_then_insert() {
        // Regression test for race condition fix in clear_table.
        // Tests that clear_table followed by inserts works correctly,
        // verifying that the background thread's clear operation doesn't
        // mark subsequently inserted items as deleted.
        use rayls_infrastructure_types::{Database, DbTxMut};
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx(temp_dir.path());

        // Clear first (empty table), then insert
        let _ = db.clear_table::<TestTable>();

        // Insert items after clear
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i as u64, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to insert");
        }
        txn.commit().unwrap();
        db.sync_persist().expect("persist");

        // Verify all items are accessible via the layered iterator
        let count = db.iter::<TestTable>().count();
        assert_eq!(count, 101, "Expected 101 items after clear+insert, got {}", count);
    }

    #[test]
    fn test_layereddb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_insert(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_insert(db);
    }

    #[test]
    fn test_layereddb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_remove(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_remove(db);
    }

    #[test]
    fn test_layereddb_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            db_simp_bench(db, "LayeredDB<ReDB>");
        }
        let db = open_mdbx(temp_dir.path());
        db_simp_bench(db, "LayeredDB<MdbxDatabase>");
    }

    /// Helper: pre-populate persistent DB directly, then open as LayeredDatabase.
    /// The returned LayeredDatabase has data ONLY in the persistent layer (mem is empty).
    fn open_mdbx_prepopulated(
        path: &Path,
        entries: &[(u64, &str)],
    ) -> LayeredDatabase<MdbxDatabase> {
        {
            let db = MdbxDatabase::open(path).expect("Cannot open database");
            db.open_table::<TestTable>().expect("failed to open table!");
            for (k, v) in entries {
                db.insert::<TestTable>(k, &v.to_string()).unwrap();
            }
        }
        open_mdbx(path)
    }

    #[test]
    fn test_layereddb_persistent_only_data() {
        // Data exists only in persistent DB (simulates post-eviction or restart).
        // Exercises the db-only branches of MergeJoinIter.
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (3, "three"), (5, "five")]);

        // get
        assert_eq!(db.get::<TestTable>(&1).unwrap(), Some("one".to_string()));
        assert_eq!(db.get::<TestTable>(&3).unwrap(), Some("three".to_string()));
        assert_eq!(db.get::<TestTable>(&5).unwrap(), Some("five".to_string()));
        assert_eq!(db.get::<TestTable>(&2).unwrap(), None);

        // forward iter
        let items: Vec<_> = db.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(1, "one".to_string()), (3, "three".to_string()), (5, "five".to_string()),]
        );

        // reverse_iter
        let items: Vec<_> = db.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(5, "five".to_string()), (3, "three".to_string()), (1, "one".to_string()),]
        );

        // last_record
        assert_eq!(db.last_record::<TestTable>(), Some((5, "five".to_string())));

        // record_prior_to
        assert_eq!(db.record_prior_to::<TestTable>(&4), Some((3, "three".to_string())));
        assert_eq!(db.record_prior_to::<TestTable>(&1), None);

        // skip_to
        let items: Vec<_> = db.skip_to::<TestTable>(&3).unwrap().collect();
        assert_eq!(items, vec![(3, "three".to_string()), (5, "five".to_string())]);
    }

    #[test]
    fn test_layereddb_merged_different_keys() {
        // Interleaved keys across layers: odd in persistent, even in mem.
        // Exercises the Less/Greater branches of MergeJoinIter.
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (3, "three"), (5, "five")]);

        // insert even keys into mem layer only
        db.insert::<TestTable>(&2, &"two".to_string()).unwrap();
        db.insert::<TestTable>(&4, &"four".to_string()).unwrap();
        db.insert::<TestTable>(&6, &"six".to_string()).unwrap();

        // forward iter merges both layers
        let items: Vec<_> = db.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string()),
                (4, "four".to_string()),
                (5, "five".to_string()),
                (6, "six".to_string()),
            ]
        );

        // reverse iter merges both layers in descending order
        let items: Vec<_> = db.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (6, "six".to_string()),
                (5, "five".to_string()),
                (4, "four".to_string()),
                (3, "three".to_string()),
                (2, "two".to_string()),
                (1, "one".to_string()),
            ]
        );

        // last_record returns 6 (from mem)
        assert_eq!(db.last_record::<TestTable>(), Some((6, "six".to_string())));

        // record_prior_to crosses layers
        assert_eq!(db.record_prior_to::<TestTable>(&4), Some((3, "three".to_string())));
        assert_eq!(db.record_prior_to::<TestTable>(&3), Some((2, "two".to_string())));

        // skip_to merges from starting point
        let items: Vec<_> = db.skip_to::<TestTable>(&3).unwrap().collect();
        assert_eq!(
            items,
            vec![
                (3, "three".to_string()),
                (4, "four".to_string()),
                (5, "five".to_string()),
                (6, "six".to_string()),
            ]
        );
    }

    #[test]
    fn test_layereddb_mem_overrides_persistent() {
        // Same key exists in both layers, mem value wins (Equal branch).
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(
            temp_dir.path(),
            &[(1, "old_one"), (2, "old_two"), (3, "old_three")],
        );

        // update key 2 through LayeredDatabase (mem overrides persistent)
        db.insert::<TestTable>(&2, &"new_two".to_string()).unwrap();

        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("new_two".to_string()));

        let items: Vec<_> = db.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (1, "old_one".to_string()),
                (2, "new_two".to_string()),
                (3, "old_three".to_string()),
            ]
        );

        let items: Vec<_> = db.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (3, "old_three".to_string()),
                (2, "new_two".to_string()),
                (1, "old_one".to_string()),
            ]
        );

        assert_eq!(db.record_prior_to::<TestTable>(&3), Some((2, "new_two".to_string())));
    }

    #[test]
    fn test_layereddb_tombstone_all_methods() {
        // Tombstoned keys (deleted in mem, still in persistent) are hidden
        // across all read methods: iter, reverse_iter, skip_to, last_record,
        // record_prior_to.
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(
            temp_dir.path(),
            &[(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")],
        );

        // tombstone keys 2 and 4 (do NOT sync, persistent still has them)
        db.remove::<TestTable>(&2).unwrap();
        db.remove::<TestTable>(&4).unwrap();

        // get respects tombstones
        assert_eq!(db.get::<TestTable>(&2).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&4).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&3).unwrap(), Some("three".to_string()));

        // contains_key respects tombstones
        assert!(!db.contains_key::<TestTable>(&2).unwrap());
        assert!(db.contains_key::<TestTable>(&3).unwrap());

        // forward iter skips tombstoned keys
        let items: Vec<_> = db.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(1, "one".to_string()), (3, "three".to_string()), (5, "five".to_string()),]
        );

        // reverse_iter skips tombstoned keys
        let items: Vec<_> = db.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(5, "five".to_string()), (3, "three".to_string()), (1, "one".to_string()),]
        );

        // last_record returns 5 (not tombstoned)
        assert_eq!(db.last_record::<TestTable>(), Some((5, "five".to_string())));

        // record_prior_to skips tombstoned keys
        // prior to 4 should be 3 (not 3→skip 4→...); prior to 5 should be 3 (skips 4)
        assert_eq!(db.record_prior_to::<TestTable>(&4), Some((3, "three".to_string())));
        assert_eq!(db.record_prior_to::<TestTable>(&5), Some((3, "three".to_string())));

        // skip_to skips tombstoned keys
        let items: Vec<_> = db.skip_to::<TestTable>(&2).unwrap().collect();
        assert_eq!(items, vec![(3, "three".to_string()), (5, "five".to_string())]);
    }

    #[test]
    fn test_layereddb_tombstone_last_key() {
        // When the last key in persistent DB is tombstoned,
        // last_record should return the second-to-last.
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (2, "two"), (3, "three")]);

        db.remove::<TestTable>(&3).unwrap();

        assert_eq!(db.last_record::<TestTable>(), Some((2, "two".to_string())));

        // tombstone all keys
        db.remove::<TestTable>(&1).unwrap();
        db.remove::<TestTable>(&2).unwrap();

        assert_eq!(db.last_record::<TestTable>(), None);
        assert!(db.iter::<TestTable>().next().is_none());
        assert!(db.reverse_iter::<TestTable>().next().is_none());
    }

    #[test]
    fn test_layereddb_reverse_iter_ordering() {
        // Dedicated test for reverse_iter correctness
        // (test_iter_reverse in shared helpers actually tests forward iter).
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx(temp_dir.path());
        db.insert::<TestTable>(&1, &"one".to_string()).unwrap();
        db.insert::<TestTable>(&2, &"two".to_string()).unwrap();
        db.insert::<TestTable>(&3, &"three".to_string()).unwrap();

        let items: Vec<_> = db.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(3, "three".to_string()), (2, "two".to_string()), (1, "one".to_string()),]
        );
    }

    #[test]
    fn test_layereddb_read_txn_layered() {
        // Tests LayeredDbTx (read transaction) with merged layers + tombstones.
        use rayls_infrastructure_types::DbTx;

        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (3, "three"), (5, "five")]);

        // insert even keys (mem only) and tombstone key 3
        db.insert::<TestTable>(&2, &"two".to_string()).unwrap();
        db.insert::<TestTable>(&4, &"four".to_string()).unwrap();
        db.remove::<TestTable>(&3).unwrap();

        let txn = db.read_txn().unwrap();

        // get
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), Some("one".to_string()));
        assert_eq!(txn.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
        assert_eq!(txn.get::<TestTable>(&3).unwrap(), None); // tombstoned
        assert_eq!(txn.get::<TestTable>(&4).unwrap(), Some("four".to_string()));
        assert_eq!(txn.get::<TestTable>(&5).unwrap(), Some("five".to_string()));

        // forward iter: 1, 2, 4, 5 (key 3 tombstoned)
        let items: Vec<_> = txn.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (4, "four".to_string()),
                (5, "five".to_string()),
            ]
        );

        // reverse_iter
        let items: Vec<_> = txn.reverse_iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![
                (5, "five".to_string()),
                (4, "four".to_string()),
                (2, "two".to_string()),
                (1, "one".to_string()),
            ]
        );

        // last_record
        assert_eq!(txn.last_record::<TestTable>(), Some((5, "five".to_string())));

        // record_prior_to (key 3 tombstoned → prior to 4 is 2)
        assert_eq!(txn.record_prior_to::<TestTable>(&4), Some((2, "two".to_string())));

        // skip_to
        let items: Vec<_> = txn.skip_to::<TestTable>(&2).unwrap().collect();
        assert_eq!(
            items,
            vec![(2, "two".to_string()), (4, "four".to_string()), (5, "five".to_string()),]
        );
    }

    /// Open a `LayeredDatabase<MdbxDatabase>` holding `rows` on the disk layer
    /// only, with a custom read-txn timeout.
    fn open_layered_mdbx_disk_rows(
        path: &Path,
        rows: u64,
        max_read: std::time::Duration,
    ) -> LayeredDatabase<MdbxDatabase> {
        use crate::mdbx::MdbxConfig;
        let cfg = MdbxConfig::default().with_max_read_transaction_duration(Some(max_read));
        // write straight to a bare mdbx db, then reopen behind a fresh (empty)
        // mem layer so the walk is served entirely from the disk-side read txn.
        {
            let db = MdbxDatabase::open_with_config(path, cfg.clone()).expect("open mdbx");
            db.open_table::<TestTable>().expect("open table");
            for i in 1..=rows {
                db.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        let db = MdbxDatabase::open_with_config(path, cfg).expect("reopen mdbx");
        db.open_table::<TestTable>().expect("open table");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>().expect("open table");
        db
    }

    /// `raw_get` returns the stored value's canonical bytes across both layers, so the cold
    /// archiver can relocate a payload without a decode/re-encode round trip.
    ///
    /// Covers the disk layer (the path archival actually hits, served zero-copy from the inner
    /// mdbx), the mem overlay (a typed value re-encoded), and the tombstone/absent `None` cases.
    #[test]
    fn test_layereddb_raw_get_matches_encoded_value() {
        use rayls_infrastructure_types::{encode, DbTx};

        let temp_dir = tempdir().expect("failed to create temp dir");
        // Key 1 lives only on the persistent layer (mem empty), modelling an evicted/archivable
        // row.
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "disk")]);
        // Key 2 lives in the mem overlay only; key 3 is tombstoned (deleted in mem).
        db.insert::<TestTable>(&2, &"mem".to_string()).unwrap();
        db.remove::<TestTable>(&3).unwrap();

        let txn = db.read_txn().unwrap();
        let owned = |k: &u64| txn.raw_get::<TestTable>(k).unwrap().map(|c| c.into_owned());

        // Both layers yield exactly the bytes the value codec would produce.
        assert_eq!(owned(&1), Some(encode(&"disk".to_string())), "disk-layer raw bytes");
        assert_eq!(owned(&2), Some(encode(&"mem".to_string())), "mem-overlay raw bytes");
        // A tombstoned key and an absent key are both `None`.
        assert_eq!(owned(&3), None, "tombstoned key must be None");
        assert_eq!(owned(&9), None, "absent key must be None");
    }

    /// `raw_skip_to` seeks across both layers with tombstones honored, so a seeked raw walk sees
    /// exactly what a full merged scan would from that key on.
    #[test]
    fn test_layereddb_raw_skip_to_merges_layers() {
        use rayls_infrastructure_types::{decode_key, DbTx};

        let temp_dir = tempdir().expect("failed to create temp dir");
        // Keys 1 and 3 on the persistent layer; 2 in the mem overlay; 3 tombstoned in mem.
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "disk"), (3, "gone")]);
        db.insert::<TestTable>(&2, &"mem".to_string()).unwrap();
        db.remove::<TestTable>(&3).unwrap();

        let txn = db.read_txn().unwrap();
        let keys_from = |k: u64| -> Vec<u64> {
            txn.raw_skip_to::<TestTable>(&k)
                .unwrap()
                .map(|(key, _)| decode_key::<u64>(&key))
                .collect()
        };
        assert_eq!(keys_from(0), vec![1, 2], "merged walk: disk + mem, tombstone hidden");
        assert_eq!(keys_from(2), vec![2], "seek lands on the mem-overlay row");
        assert_eq!(keys_from(3), Vec::<u64>::new(), "the tombstoned tail is empty");
    }

    /// Prove `disable_long_read_safety` reaches the inner mdbx txn so a walk
    /// straddling the read-txn timeout is not silently truncated.
    ///
    /// Regression for the leader-count undercount fork: the exemption was a no-op
    /// on `LayeredDatabase`, so the monitor reset the walk's txn mid-scan and the
    /// iterator stopped early. Slow (~3s): drives the real mdbx timeout monitor.
    #[test]
    fn test_layereddb_disable_long_read_safety_survives_midwalk_timeout() {
        use rayls_infrastructure_types::DbTx;
        use std::time::Duration;

        const ROWS: u64 = 64;
        let max_read = Duration::from_secs(1);
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_layered_mdbx_disk_rows(temp_dir.path(), ROWS, max_read);

        // exempt walker: opt out of the timeout, then start iterating so the read
        // snapshot is live before we straddle the deadline.
        let exempt = db.read_txn().unwrap();
        exempt.disable_long_read_safety();
        let mut walk = exempt.reverse_iter::<TestTable>();
        let first = walk.next().expect("first row before timeout");

        // control walker: identical but not exempted, proving the timeout fires.
        let control = db.read_txn().unwrap();

        // hold both open past max_read so the monitor resets every active,
        // non-exempt read txn mid-flight, exactly like the ~30s tally walk.
        std::thread::sleep(max_read + Duration::from_secs(2));

        // fix proof: the exempt walk finishes in full across the timeout boundary.
        let rest = walk.count() as u64;
        assert_eq!(rest + 1, ROWS, "exempt walk must not truncate across the timeout");
        assert_eq!(first.0, ROWS);

        // control: the un-exempt txn was reset, so a disk read now errors. without
        // this a green exempt case could be a false positive (monitor never ran).
        assert!(
            control.get::<TestTable>(&ROWS).is_err(),
            "un-exempt read txn must be reset by the timeout monitor",
        );
    }
}
