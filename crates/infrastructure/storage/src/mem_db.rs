//! Impermanent storage in memory - useful for tests.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    time::Duration,
};

use parking_lot::RwLock;
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};

use crate::{open_default_tables, write_buffer::{WriteBuffer, WriteOp}};

type StoreTableValueType = (bool, Vec<u8>);
type StoreTableType = BTreeMap<Vec<u8>, StoreTableValueType>;

/// Map from table name → table data.
/// `pub` so `write_buffer.rs` and `layered_db.rs` can import it.
pub type StoreType = HashMap<&'static str, StoreTableType>;

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

/// Unified transaction type for MemDatabase.
/// Holds an Arc clone of the shared store (snapshot isolation) + private buffer.
#[derive(Debug)]
pub struct MemTxn<'a> {
    store: Arc<RwLock<StoreType>>,
    buffer: WriteBuffer,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> MemTxn<'a> {
    /// Read-only: check buffer first, then fall through to shared store.
    /// A buffered remove (tombstone) shadows the shared store.
    fn get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        // 1. Check buffer (read-after-write consistency)
        if let Some(val) = self.buffer_get::<T>(key) {
            return Some(val);
        }
        // 2. A buffered remove stops the fallthrough — uncommitted removes are visible
        //    within the transaction but must not resurrect the shared-store value.
        if self.buffer_is_tombstoned::<T>(key) == Some(true) {
            return None;
        }
        // 3. Fall through to shared store
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, val_bytes)) = table.get(&key_bytes) {
                if !*removed {
                    return Some(decode(val_bytes));
                }
            }
        }
        None
    }

    /// Check only the private buffer (for layered get to implement 4-tier fallthrough).
    pub(crate) fn buffer_get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        self.buffer.get::<T>(key)
    }

    /// Raw check only the private buffer (for layered raw_get).
    pub(crate) fn buffer_raw_get<T: Table>(&self, key: &T::Key) -> Option<Vec<u8>> {
        self.buffer.raw_get::<T>(key)
    }

    /// Check only the private buffer for tombstone status.
    /// Returns `Some(true)` if tombstoned, `Some(false)` if inserted, `None` if not in buffer.
    /// Used by `WriteTxn::get` to implement correct read-after-write semantics.
    pub(crate) fn buffer_is_tombstoned<T: Table>(&self, key: &T::Key) -> Option<bool> {
        self.buffer.is_tombstoned::<T>(key)
    }

    /// Get raw value from shared store without tombstone check (for layered raw_get).
    /// Returns (is_tombstoned, raw_bytes) so the caller avoids decode/encode round-trip.
    pub(crate) fn get_raw<T: Table>(&self, key: &T::Key) -> Option<(bool, Vec<u8>)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map(|(removed, val_bytes)| {
                (*removed, val_bytes.clone())
            });
        }
        None
    }

    pub(crate) fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        // Check buffer for tombstone
        if let Some(tombstoned) = self.buffer.is_tombstoned::<T>(key) {
            return tombstoned;
        }
        // Fall through to shared store
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map_or(false, |(removed, _)| *removed);
        }
        false
    }

    /// Hard-delete a key from the mem overlay without tombstoning.
    /// Used by the cold archival producer: a tombstone would shadow the cold fall-through,
    /// so the archived row must be hard-deleted from the hot tier.
    pub(crate) fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        // Remove from buffer if present
        self.buffer.hard_delete::<T>(key);
        // Remove from shared store
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            table.remove(&key_bytes);
        }
    }
}

impl<'a> DbTx for MemTxn<'a> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(self.get::<T>(key))
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Avoid decode/encode round-trip by returning raw bytes directly.
        // A buffered remove shadows the shared store (read-after-write).
        if let Some(bytes) = self.buffer_raw_get::<T>(key) {
            return Ok(Some(Cow::Owned(bytes)));
        }
        if self.buffer_is_tombstoned::<T>(key) == Some(true) {
            return Ok(None);
        }
        if let Some((removed, raw)) = self.get_raw::<T>(key) {
            if !removed {
                return Ok(Some(Cow::Owned(raw)));
            }
        }
        Ok(None)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // Merge buffer + shared store, respect tombstones: start from the store
        // snapshot and replay the buffered ops in order (last write wins).
        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            let shared = self.store.read();
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = shared
                .get(T::NAME)
                .into_iter()
                .flat_map(|t| {
                    t.iter()
                        .filter(|(_, (removed, _))| !*removed)
                        .map(|(k, (_, v))| (k.clone(), v.clone()))
                })
                .collect();
            if let Some(ops) = self.buffer.ops_for(T::NAME) {
                for op in ops {
                    match op {
                        WriteOp::Insert { key, value } => {
                            entries.retain(|(k, _)| k != key);
                            entries.push((key.clone(), value.clone()));
                        }
                        WriteOp::Remove { key } => {
                            entries.retain(|(k, _)| k != key);
                        }
                        WriteOp::ClearTable => entries.clear(),
                    }
                }
            }
            // Buffered inserts can land out of order; restore key order.
            entries.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
            entries
        };
        Box::new(items.into_iter().map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v))))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        // Merge buffer + shared store as raw bytes, respect tombstones.
        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            let shared = self.store.read();
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = shared
                .get(T::NAME)
                .into_iter()
                .flat_map(|t| {
                    t.iter()
                        .filter(|(_, (removed, _))| !*removed)
                        .map(|(k, (_, v))| (k.clone(), v.clone()))
                })
                .collect();
            if let Some(ops) = self.buffer.ops_for(T::NAME) {
                for op in ops {
                    match op {
                        WriteOp::Insert { key, value } => {
                            entries.retain(|(k, _)| k != key);
                            entries.push((key.clone(), value.clone()));
                        }
                        WriteOp::Remove { key } => {
                            entries.retain(|(k, _)| k != key);
                        }
                        WriteOp::ClearTable => entries.clear(),
                    }
                }
            }
            // Buffered inserts can land out of order; restore key order.
            entries.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
            entries
        };
        Box::new(items.into_iter().map(|(k, v)| (Cow::Owned(k), Cow::Owned(v))))
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let key = key.clone();
        Ok(Box::new(self.iter::<T>().skip_while(move |(k, _)| k < &key)))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let items: Vec<_> = self.iter::<T>().collect();
        Box::new(items.into_iter().rev())
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let items: Vec<_> = self.raw_iter::<T>().collect();
        Box::new(items.into_iter().rev())
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }
}

impl<'a> DbTxMut for MemTxn<'a> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.buffer.insert::<T>(key, value);
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.buffer.remove::<T>(key);
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.buffer.clear_table::<T>();
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        // Commit: merge buffer into shared store.
        // Consumes `self`; the `store` Arc outlives the txn, so the shared store persists.
        let Self { store, buffer, _marker } = self;
        buffer.apply_to_mem(&store);
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
    // gets the value with the marking for delete flag
    pub fn get_marked<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<(bool, T::Value)>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, val_bytes)) = table.get(&key_bytes) {
                let val = decode(val_bytes);
                return Ok(Some((*removed, val)));
            }
        }

        Ok(None)
    }

    /// Check if a key is tombstoned (marked for deletion) without deserializing the value.
    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map_or(false, |(removed, _)| *removed);
        }
        false
    }

    pub fn delete_removed<T: Table>(&self, key: &T::Key, require_marked: bool) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, _)) = table.get(&key_bytes) {
                if !*removed && require_marked {
                    // Value was re-inserted after the remove was queued — keep it.
                    return Ok(());
                }

                table.remove(&key_bytes);
            }
        }
        Ok(())
    }

    /// Hard-delete a key from the mem overlay without tombstoning.
    /// Used by the cold archival producer: a tombstone would shadow the cold fall-through,
    /// so the archived row must be hard-deleted from the hot tier.
    pub fn hard_delete<T: Table>(&self, key: &T::Key) {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            table.remove(&key_bytes);
        }
    }

    /// Returns keys marked for deletion in the given table.
    pub fn get_deleted_keys<T: Table>(&self) -> std::collections::HashSet<Vec<u8>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            table.iter().filter(|(_, (removed, _))| *removed).map(|(k, _)| k.clone()).collect()
        } else {
            std::collections::HashSet::new()
        }
    }
}

impl Drop for MemDatabase {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shutdown_tx) <= 1 {
            tracing::info!(target: "rayls::memdb", "MemDatabase Dropping, shutting down metrics thread");
            // shutdown_tx is a sync sender with no buffer so this should block until the thread
            // reads it and shuts down.
            if let Err(e) = self.shutdown_tx.send(()) {
                tracing::error!(target: "rayls::memdb",
                    "Error while trying to send shutdown to MemDatabase metrics thread {e}"
                );
            }
        }
    }
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
        = MemTxn<'txn>
    where
        Self: 'txn;

    type TXMut<'txn>
        = MemTxn<'txn>
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
        Ok(MemTxn {
            store: Arc::clone(&self.store),
            buffer: WriteBuffer::default(),
            _marker: std::marker::PhantomData,
        })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MemTxn {
            store: Arc::clone(&self.store),
            buffer: WriteBuffer::default(),
            _marker: std::marker::PhantomData,
        })
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
        Ok(get_with_marked_check::<T>(&self.store.read(), key))
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            let value_bytes = encode(value);
            table.insert(key_bytes, (false, value_bytes));
        }
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some(value) = table.get_mut(&key_bytes) {
                value.0 = true;
            } else {
                // tombstone for keys that only exist in the persistent layer
                table.insert(key_bytes, (true, Vec::new()));
            }
        }
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            //mark all for deletion
            for value in table.values_mut() {
                value.0 = true;
            }
        }
        Ok(())
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
            assert_eq!(
                v,
                val,
                "Value should match inserted value within the transaction before commit"
            );
        }

        txn.commit().expect("Failed to commit write txn");

        // values should be present after commit
        assert!(!db.is_empty::<TestTable>(), "Table should not be empty after commit");

        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            let v = db.get::<TestTable>(&key).unwrap();
            assert!(v.is_some(), "Value should be present after commit");
            assert_eq!(
                v.unwrap(),
                val,
                "Value should match inserted value after commit"
            );
        }

        // test deleting non-existent key — logically a no-op
        let mut txn2 = db.write_txn().unwrap();
        txn2.remove::<TestTable>(&999).expect("Failed to remove non-existent key");
        txn2.commit().expect("Failed to commit write txn");

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
        assert_eq!(
            val_in_txn,
            "two hundred".to_string(),
            "Value for key 200 should be available within the same transaction"
        );

        // test removing it as well
        txn3.remove::<TestTable>(&200).expect("Failed to remove key 200");
        let val_after_removal_in_txn = DbTx::get::<TestTable>(&txn3, &200).unwrap();
        assert!(
            val_after_removal_in_txn.is_none(),
            "Value for key 200 should not be available within the same transaction after removal"
        );
        txn3.commit().expect("Failed to commit write txn");

        ////////////////////////////////////////////////////////////////////////
        // check insert after remove of same value within the same transaction
        ////////////////////////////////////////////////////////////////////////
        let mut txn4 = db.write_txn().unwrap();
        txn4.remove::<TestTable>(&50).expect("Failed to remove key 50");
        let val_after_removal = DbTx::get::<TestTable>(&txn4, &50).unwrap();
        assert!(
            val_after_removal.is_none(),
            "Value for key 50 should not be available within the same transaction after removal"
        );
        txn4.insert::<TestTable>(&50, &"fifty".to_string()).expect("Failed to insert key 50");
        let val_after_reinsertion = txn4.get::<TestTable>(&50).unwrap();
        assert_eq!(
            val_after_reinsertion,
            "fifty".to_string(),
            "Value for key 50 should be available within the same transaction after reinsertion"
        );
        txn4.commit().expect("Failed to commit write txn");

        //also there after commit
        let val_after_commit = db.get::<TestTable>(&50).unwrap();
        assert!(val_after_commit.is_some(), "Value for key 50 should be available after commit");
        assert_eq!(
            val_after_commit.unwrap(),
            "fifty".to_string(),
            "Value for key 50 should match reinserted value after commit"
        );
    }

    #[test]
    fn mem_txn_buffer_is_private_until_commit() {
        let db = open_db();
        db.insert::<TestTable>(&1, &"old".to_string()).expect("insert");

        let mut txn_a = db.write_txn().unwrap();
        txn_a.insert::<TestTable>(&1, &"new".to_string()).expect("insert");
        let txn_b = db.read_txn().unwrap();
        assert_eq!(
            txn_b.get::<TestTable>(&1).unwrap(),
            "old".to_string(),
            "txnA's write must be invisible to txnB"
        );
        assert_eq!(
            txn_a.get::<TestTable>(&1).unwrap(),
            "new".to_string(),
            "read-after-write must see the buffer"
        );
        txn_a.commit().expect("commit");
        let txn_b2 = db.read_txn().unwrap();
        assert_eq!(
            txn_b2.get::<TestTable>(&1).unwrap(),
            "new".to_string(),
            "write must be visible after commit"
        );
    }

    #[test]
    fn mem_txn_buffer_discarded_on_drop() {
        let db = open_db();
        db.insert::<TestTable>(&1, &"original".to_string()).expect("insert");

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&1, &"modified".to_string()).expect("insert");
        drop(txn);

        let txn2 = db.read_txn().unwrap();
        assert_eq!(
            txn2.get::<TestTable>(&1).unwrap(),
            "original".to_string(),
            "uncommitted write must be discarded on drop"
        );
    }

    #[test]
    fn mem_txn_insert_remove_insert_sequence() {
        let db = open_db();
        db.insert::<TestTable>(&1, &"v1".to_string()).expect("insert");

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&1, &"v2".to_string()).expect("insert");
        txn.remove::<TestTable>(&1).expect("remove");
        txn.insert::<TestTable>(&1, &"v3".to_string()).expect("insert");
        assert_eq!(
            txn.get::<TestTable>(&1).unwrap(),
            "v3".to_string(),
            "last operation wins in buffer"
        );
        txn.commit().expect("commit");
        assert_eq!(
            db.get::<TestTable>(&1).unwrap(),
            Some("v3".to_string()),
            "last operation wins after commit"
        );
    }

    #[test]
    fn mem_txn_remove_nonexistent_creates_tombstone() {
        let db = open_db();

        let mut txn = db.write_txn().unwrap();
        txn.remove::<TestTable>(&42).expect("remove");
        assert_eq!(
            DbTx::get::<TestTable>(&txn, &42).unwrap(),
            None,
            "buffer tombstone must shadow the store"
        );
        assert!(
            txn.is_tombstoned::<TestTable>(&42),
            "buffer tombstone must be visible via is_tombstoned"
        );
        txn.commit().expect("commit");
        assert!(
            db.is_tombstoned::<TestTable>(&42),
            "tombstone must be merged to the shared store"
        );
    }

    #[test]
    fn mem_txn_iter_merges_buffer_and_store() {
        let db = open_db();
        db.insert::<TestTable>(&1, &"a".to_string()).expect("insert");
        db.insert::<TestTable>(&3, &"c".to_string()).expect("insert");

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&2, &"b".to_string()).expect("insert");
        txn.remove::<TestTable>(&1).expect("remove");

        let items: Vec<_> = txn.iter::<TestTable>().collect();
        assert_eq!(
            items,
            vec![(2, "b".to_string()), (3, "c".to_string())],
            "buffer merge must add 2 and skip tombstoned 1"
        );
    }
}
