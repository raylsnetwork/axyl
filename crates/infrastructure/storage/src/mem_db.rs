//! Impermanent storage in memory - useful for tests.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    marker::PhantomData,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    time::Duration,
};

use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};

use crate::open_default_tables;

type StoreTableValueType = (bool, Vec<u8>);
type StoreTableType = BTreeMap<Vec<u8>, StoreTableValueType>;
type StoreType = HashMap<&'static str, StoreTableType>;

fn get_with_marked_check<T: Table>(store: &StoreType, key: &T::Key) -> Option<T::Value> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode_key(key);
        if let Some((removed, val_bytes)) = table.get(&key_bytes) {
            if !*removed {
                let val = decode(val_bytes);
                return Some(val);
            }
        }
    }
    None
}

#[derive(Debug)]
pub struct MemDbTx<'a> {
    store: RwLockReadGuard<'a, StoreType>,
}

impl<'a> MemDbTx<'a> {
    pub fn get_no_marked_check<T: Table>(&self, key: &T::Key) -> Option<(bool, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map(|(removed, val_bytes)| {
                let val = decode(val_bytes);
                (*removed, val)
            });
        }
        None
    }

    /// Check if a key is tombstoned (marked for deletion) without deserializing the value.
    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map_or(false, |(removed, _)| *removed);
        }
        false
    }
}

impl<'a> DbTx for MemDbTx<'a> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(get_with_marked_check::<T>(&self.store, key))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Box::new(items.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.get(T::NAME) {
            // The read guard is held by `self`, so we can borrow the stored
            // bytes for the iterator's lifetime instead of cloning them.
            Box::new(
                table
                    .iter()
                    .filter(|(_, (removed, _))| !*removed)
                    .map(|(k, (_, v))| (Cow::Borrowed(k.as_slice()), Cow::Borrowed(v.as_slice()))),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .skip_while(|(k, _)| **k < key_bytes)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Ok(Box::new(items.into_iter()))
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .rev()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Box::new(items.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                table
                    .iter()
                    .rev()
                    .filter(|(_, (removed, _))| !*removed)
                    .map(|(k, (_, v))| (Cow::Borrowed(k.as_slice()), Cow::Borrowed(v.as_slice()))),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            for (key_bytes, (removed, value_bytes)) in table.iter().rev() {
                if !*removed {
                    return Some((decode_key(key_bytes), decode(value_bytes)));
                }
            }
            None
        } else {
            None
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            table
                .range(..key_bytes)
                .rev()
                .find(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key(k), decode(v)))
        } else {
            None
        }
    }

    fn disable_long_read_safety(&self) {}
}

#[derive(Debug)]
pub struct MemDbTxMut<'a> {
    store: RwLockWriteGuard<'a, StoreType>,
}

impl<'a> MemDbTxMut<'a> {
    fn mark_value_for_deletion(value: &mut StoreTableValueType) {
        // mark for actual deletion once tx committed
        value.0 = true;
    }

    /// Hard-removes `key` from the in-memory store, leaving no tombstone.
    ///
    /// Persistent eviction archives a row permanently (never re-inserted), so the cache entry is
    /// dropped outright rather than tombstoned: this frees the cache and lets reads fall through to
    /// a lower tier, whereas a tombstone would shadow it and never be reclaimed.
    pub fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        if let Some(table) = self.store.get_mut(T::NAME) {
            table.remove(&encode_key(key));
        }
    }
}

impl<'a> DbTx for MemDbTxMut<'a> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        //if not in cache check store
        Ok(get_with_marked_check::<T>(&self.store, key))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // if let Some(table) = self.store.get(T::NAME) {
        //     let items: Vec<_> = table
        //         .read()
        //         .iter()
        //         .filter(|(_, (removed, _))| !*removed)
        //         .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
        //         .collect();
        //     Box::new(items.into_iter())
        // } else {
        //     Box::new(std::iter::empty())
        // }
        //To implement this we need to merge results from cache and store in a temporary vector and
        // return iterator over that. This is not expected to used in a transaction, so
        // should be safe.
        panic!("Should not be called on a tx mut!");
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("Should not be called on a tx mut!");
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .skip_while(|(k, _)| **k < key_bytes)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Ok(Box::new(items.into_iter()))
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .rev()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Box::new(items.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                table
                    .iter()
                    .rev()
                    .filter(|(_, (removed, _))| !*removed)
                    .map(|(k, (_, v))| (Cow::Borrowed(k.as_slice()), Cow::Borrowed(v.as_slice()))),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            for (key_bytes, (removed, value_bytes)) in table.iter().rev() {
                if !*removed {
                    return Some((decode_key(key_bytes), decode(value_bytes)));
                }
            }
            None
        } else {
            None
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            table
                .range(..key_bytes)
                .rev()
                .find(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key(k), decode(v)))
        } else {
            None
        }
    }

    fn disable_long_read_safety(&self) {}
}

impl<'a> DbTxMut for MemDbTxMut<'a> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            let value_bytes = encode(value);
            table.insert(key_bytes, (false, value_bytes));

            Ok(())
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some(value) = table.get_mut(&key_bytes) {
                Self::mark_value_for_deletion(value);
            } else {
                // tombstone for keys that only exist in the persistent layer
                table.insert(key_bytes, (true, Vec::new()));
            }
            Ok(())
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        if let Some(table) = self.store.get_mut(T::NAME) {
            for value in table.values_mut() {
                Self::mark_value_for_deletion(value);
            }
            Ok(())
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn commit(self) -> eyre::Result<()> {
        // no need to do anything, the lock finishes with the tx drop
        Ok(())
    }
}

/// Implement the Database trait with an in-memory store.
/// This means no persistance.
/// This DB also plays loose with transactions, but since it is in-memory and we do not do
/// roll-backs this should be fine.
#[derive(Clone, Debug)]
pub struct MemDatabase {
    store: Arc<RwLock<StoreType>>,
    metrics: Arc<RwLock<MemDBMetrics>>,
    shutdown_tx: Arc<SyncSender<()>>,
}

impl MemDatabase {
    pub fn new() -> Self {
        let store: Arc<RwLock<StoreType>> = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(RwLock::new(MemDBMetrics::default()));
        let (shutdown_tx, rx) = mpsc::sync_channel::<()>(0);

        let store_cloned: Arc<RwLock<StoreType>> = Arc::clone(&store);
        let metrics_cloned = metrics.clone();

        // Spawn thread to update metrics from MemDB stats every 30 seconds.
        std::thread::spawn(move || {
            tracing::info!(target: "rayls::memdb", "Starting MemDB metrics thread");
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                rx.recv_timeout(Duration::from_secs(30))
            {
                let read_guard = store_cloned.read();
                for (key, table) in read_guard.iter() {
                    if let Some(m) = metrics_cloned.read().table_counts.get(key) {
                        m.set(table.len().try_into().unwrap_or(-1));
                    }
                }
            }
            tracing::info!(target: "rayls::memdb", "Ending MemDB metrics thread");
        });

        Self { store, metrics, shutdown_tx: Arc::new(shutdown_tx) }
    }

    /// Infallible read transaction; [`Database::read_txn`] wraps this.
    fn read_txn_impl(&self) -> MemDbTx<'_> {
        MemDbTx { store: self.store.read() }
    }

    /// Infallible write transaction; [`Database::write_txn`] wraps this.
    fn write_txn_impl(&self) -> MemDbTxMut<'_> {
        MemDbTxMut { store: self.store.write() }
    }

    // gets the value with the marking for delete flag
    pub fn get_marked<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<(bool, T::Value)>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((tombstoned, val_bytes)) = table.get(&key_bytes) {
                let val = decode(val_bytes);
                return Ok(Some((*tombstoned, val)));
            }
        }

        Ok(None)
    }

    /// Check if a key is tombstoned (marked for deletion) without deserializing the value.
    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map_or(false, |(tombstoned, _)| *tombstoned);
        }
        false
    }

    pub fn delete_tombstoned<T: Table>(
        &self,
        key: &T::Key,
        require_marked: bool,
    ) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((tombstoned, _)) = table.get(&key_bytes) {
                if *tombstoned || !require_marked {
                    table.remove(&key_bytes);
                }
            }
        }
        Ok(())
    }

    /// Returns keys marked for deletion in the given table.
    pub fn get_deleted_keys<T: Table>(&self) -> std::collections::HashSet<Vec<u8>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            table
                .iter()
                .filter(|(_, (tombstoned, _))| *tombstoned)
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            std::collections::HashSet::new()
        }
    }
}

impl Drop for MemDatabase {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shutdown_tx) > 1 {
            return;
        }

        tracing::info!(target: "rayls::memdb", "MemDatabase Dropping, shutting down metrics thread");
        // shutdown_tx is a sync sender with no buffer so this should block until the thread
        // reads it and shuts down.
        if let Err(e) = self.shutdown_tx.send(()) {
            tracing::error!(target: "rayls::memdb", "Error while trying to send shutdown to MemDatabase metrics thread {e}"  );
        }
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        let mut db = Self::new();

        open_default_tables(&mut db).expect("failed to open default tables in MemDatabase");

        db
    }
}

impl Database for MemDatabase {
    type TX<'txn>
        = MemDbTx<'txn>
    where
        Self: 'txn;

    type TXMut<'txn>
        = MemDbTxMut<'txn>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.store.write().insert(T::NAME, BTreeMap::new());
        match register_int_gauge_with_registry!(
            format!("memdb_{}_count", T::NAME),
            format!("Entries in the {} memory table.", T::NAME),
            default_registry(),
        ) {
            Ok(m) => {
                self.metrics.write().table_counts.insert(T::NAME, m);
            }
            Err(e) => {
                // This will happen for tests.  Nothing really to do, if the guage is missing then
                // the metrics thread will just not update it... Log at debug level
                // in case something else is going on and someone is debugging.
                tracing::debug!(target: "rayls::memdb", "Error adding metrics for table {}: {e}", T::NAME)
            }
        }
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(self.read_txn_impl())
    }

    fn write_txn(&self) -> eyre::Result<MemDbTxMut<'_>> {
        Ok(self.write_txn_impl())
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, _)) = table.get(&key_bytes) {
                return Ok(!*removed);
            }
        }
        Ok(false)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.read_txn_impl().get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.write_txn_impl().insert::<T>(key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.write_txn_impl().remove::<T>(key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.write_txn_impl().clear_table::<T>()
    }

    fn is_empty<T: Table>(&self) -> bool {
        if let Some(table) = self.store.read().get(T::NAME) {
            // iterate table values and see if any are not marked for deletion
            let guard = table;
            for (removed, _) in guard.values() {
                if !*removed {
                    return false;
                }
            }

            true
        } else {
            true
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Box::new(items.into_iter())
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        // The guard is a temporary here (not held by `self`), so the bytes must
        // be owned rather than borrowed for the iterator's lifetime.
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<(Cow<'_, [u8]>, Cow<'_, [u8]>)> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                .collect();
            Box::new(items.into_iter())
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .skip_while(|(k, _)| **k < key_bytes)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Ok(Box::new(items.into_iter()))
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .rev()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (decode_key::<T::Key>(k), decode::<T::Value>(v)))
                .collect();
            Box::new(items.into_iter())
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<(Cow<'_, [u8]>, Cow<'_, [u8]>)> = table
                .iter()
                .rev()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                .collect();
            Box::new(items.into_iter())
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            table
                .range(..key_bytes)
                .rev()
                .find(|(_, v)| !v.0)
                .map(|(k, v)| (decode_key(k), decode(&v.1)))
        } else {
            None
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            //redo with reverse iter
            for (key_bytes, marked_value_bytes) in table.iter().rev() {
                if marked_value_bytes.0 == false {
                    let key = decode_key(key_bytes);
                    let value = decode(&marked_value_bytes.1);
                    return Some((key, value));
                }
            }
            None
        } else {
            None
        }
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
}

#[self_referencing]
struct TabAndGuard<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    #[borrows(table)]
    #[covariant]
    guard: RwLockReadGuard<'this, BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[self_referencing]
pub struct MemDBIter<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: TabAndGuard<T>,
    #[borrows(table)]
    #[not_covariant]
    iter: Box<dyn Iterator<Item = (&'this Vec<u8>, &'this Vec<u8>)> + 'this>,
}

impl<T: Table> Iterator for MemDBIter<T> {
    type Item = (T::Key, T::Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| {
            fields.iter.next().map(|(key_bytes, value_bytes)| {
                let key = decode_key(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        })
    }
}

#[derive(Debug)]
struct MemDBMetrics {
    table_counts: HashMap<&'static str, IntGauge>,
}

impl MemDBMetrics {
    fn try_new(_registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self { table_counts: HashMap::default() })
    }
}

impl Default for MemDBMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => {
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rayls_infrastructure_types::{Database, DbTx, DbTxMut};

    use crate::{mem_db::MemDatabase, test::*};

    fn open_db() -> MemDatabase {
        let db = MemDatabase::new();
        db.open_table::<TestTable>().expect("failed to open table");
        db
    }

    #[test]
    fn test_memdb_contains_key() {
        let db = open_db();
        test_contains_key(db)
    }

    #[test]
    fn test_memdb_get() {
        let db = open_db();
        test_get(db)
    }

    #[test]
    fn test_memdb_multi_get() {
        let db = open_db();
        test_multi_get(db)
    }

    #[test]
    fn test_memdb_skip() {
        let db = open_db();
        test_skip(db)
    }

    #[test]
    fn test_memdb_skip_to_previous_simple() {
        let db = open_db();
        test_skip_to_previous_simple(db)
    }

    #[test]
    fn test_memdb_iter_skip_to_previous_gap() {
        let db = open_db();
        test_iter_skip_to_previous_gap(db)
    }

    #[test]
    fn test_memdb_remove() {
        let db = open_db();
        test_remove(db)
    }

    #[test]
    fn test_memdb_iter() {
        let db = open_db();
        test_iter(db)
    }

    #[test]
    fn test_memdb_iter_reverse() {
        let db = open_db();
        test_iter_reverse(db)
    }

    #[test]
    fn test_memdb_clear() {
        let db = open_db();
        test_clear(db)
    }

    #[test]
    fn test_memdb_is_empty() {
        let db = open_db();
        test_is_empty(db)
    }

    #[test]
    fn test_memdb_multi_insert() {
        // Init a DB
        let db = open_db();
        test_multi_insert(db)
    }

    #[test]
    fn test_memdb_multi_remove() {
        // Init a DB
        let db = open_db();
        test_multi_remove(db)
    }

    #[test]
    fn test_memdb_dbsimpbench() {
        // Init a DB
        let db = open_db();
        db_simp_bench(db, "MemDb");
    }

    #[test]
    fn test_memdb_tx_commit() {
        let db = open_db();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }

        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            let v = txn.get::<TestTable>(&key).unwrap();
            assert!(v.is_some(), "Value should be present within the transaction before commit");
            assert_eq!(
                v.unwrap(),
                val,
                "Value should match inserted value within the transaction before commit"
            );
        }

        drop(txn);

        // values should be present after commit
        assert!(!db.is_empty::<TestTable>(), "Table should not be empty after commit");

        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            let v = db.get::<TestTable>(&key).unwrap();
            assert!(v.is_some(), "Value should be present within the transaction before commit");
            assert_eq!(
                v.unwrap(),
                val,
                "Value should match inserted value within the transaction before commit"
            );
        }

        // test deleting non-existent key — logically a no-op
        let mut txn2 = db.write_txn().unwrap();
        txn2.remove::<TestTable>(&999).expect("Failed to remove non-existent key");
        drop(txn2);

        // key 999 was never inserted, so get should return None
        assert!(
            db.get::<TestTable>(&999).unwrap().is_none(),
            "Removed non-existent key should not appear"
        );
        // original data should still be present
        assert!(
            !db.is_empty::<TestTable>(),
            "Table should not be empty after removing non-existent key"
        );

        ////////////////////////////////////////////////////////////////////////
        // check value availability within the same transaction
        ////////////////////////////////////////////////////////////////////////

        let mut txn3 = db.write_txn().unwrap();
        txn3.insert::<TestTable>(&200, &"two hundred".to_string())
            .expect("Failed to insert key 200");
        let val_in_txn = txn3.get::<TestTable>(&200).unwrap();
        assert!(
            val_in_txn.is_some(),
            "Value for key 200 should be available within the same transaction"
        );

        // test removing it as well
        txn3.remove::<TestTable>(&200).expect("Failed to remove key 200");
        let val_after_removal_in_txn = txn3.get::<TestTable>(&200).unwrap();
        assert!(
            val_after_removal_in_txn.is_none(),
            "Value for key 200 should not be available within the same transaction after removal"
        );
        drop(txn3);

        ////////////////////////////////////////////////////////////////////////
        // check insert after remove of same value within the same transaction
        ////////////////////////////////////////////////////////////////////////
        let mut txn4 = db.write_txn().unwrap();
        txn4.remove::<TestTable>(&50).expect("Failed to remove key 50");
        let val_after_removal = txn4.get::<TestTable>(&50).unwrap();
        assert!(
            val_after_removal.is_none(),
            "Value for key 50 should not be available within the same transaction after removal"
        );
        txn4.insert::<TestTable>(&50, &"fifty".to_string()).expect("Failed to insert key 50");
        let val_after_reinsertion = txn4.get::<TestTable>(&50).unwrap();
        assert!(
            val_after_reinsertion.is_some(),
            "Value for key 50 should be available within the same transaction after reinsertion"
        );
        assert_eq!(
            val_after_reinsertion.unwrap(),
            "fifty".to_string(),
            "Value for key 50 should match reinserted value within the same transaction"
        );
        drop(txn4);

        //also there after commit
        let val_after_commit = db.get::<TestTable>(&50).unwrap();
        assert!(val_after_commit.is_some(), "Value for key 50 should be available after commit");
        assert_eq!(
            val_after_commit.unwrap(),
            "fifty".to_string(),
            "Value for key 50 should match reinserted value after commit"
        );
    }
}
