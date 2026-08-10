//! Impl db traits for redb

use std::{
    borrow::Cow,
    fmt::Debug,
    path::Path,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    time::Duration,
};

use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard};
use redb::{
    Database as ReDatabase, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata,
    TableDefinition, WriteTransaction,
};

use rayls_infrastructure_types::{
    encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, KeyT, Table, ValueT,
};

use super::{
    metrics::ReDbMetrics,
    wraps::{KeyWrap, ValWrap},
};

#[derive(Debug)]
pub struct ReDbTx {
    tx: ReadTransaction,
}

impl DbTx for ReDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Ok(self.tx.open_table(td)?.get(key)?.map(|v| v.value().clone()))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| (k.value().clone(), v.value().clone()))
                    .collect();
                Box::new(items.into_iter())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| {
                        (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value())))
                    })
                    .collect();
                Box::new(items.into_iter())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let table = self.tx.open_table(td)?;
        let key = key.clone();
        let items: Vec<_> = table
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value().clone(), v.value().clone()))
            .skip_while(move |(k, _)| k < &key)
            .collect();
        Ok(Box::new(items.into_iter()))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| (k.value().clone(), v.value().clone()))
                    .collect();
                Box::new(items.into_iter().rev())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| {
                        (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value())))
                    })
                    .collect();
                Box::new(items.into_iter().rev())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx
            .open_table(td)
            .ok()?
            .last()
            .ok()
            .flatten()
            .map(|(k, v)| (k.value().clone(), v.value().clone()))
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let table = self.tx.open_table(td).ok()?;
        let mut last = None;
        for r in table.iter().ok()?.filter_map(|r| r.ok()) {
            let (k, v) = (r.0.value().clone(), r.1.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last
    }
}

pub struct ReDbTxMut {
    tx: WriteTransaction,
}

impl Debug for ReDbTxMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReDbTxMut")
    }
}

impl DbTx for ReDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Ok(self.tx.open_table(td)?.get(key)?.map(|v| v.value().clone()))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| (k.value().clone(), v.value().clone()))
                    .collect();
                Box::new(items.into_iter())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| {
                        (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value())))
                    })
                    .collect();
                Box::new(items.into_iter())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let table = self.tx.open_table(td)?;
        let key = key.clone();
        let items: Vec<_> = table
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value().clone(), v.value().clone()))
            .skip_while(move |(k, _)| k < &key)
            .collect();
        Ok(Box::new(items.into_iter()))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| (k.value().clone(), v.value().clone()))
                    .collect();
                Box::new(items.into_iter().rev())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        match self.tx.open_table(td) {
            Ok(table) => {
                let items: Vec<_> = table
                    .iter()
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| {
                        (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value())))
                    })
                    .collect();
                Box::new(items.into_iter().rev())
            }
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx
            .open_table(td)
            .ok()?
            .last()
            .ok()
            .flatten()
            .map(|(k, v)| (k.value().clone(), v.value().clone()))
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let table = self.tx.open_table(td).ok()?;
        let mut last = None;
        for r in table.iter().ok()?.filter_map(|r| r.ok()) {
            let (k, v) = (r.0.value().clone(), r.1.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last
    }
}

impl DbTxMut for ReDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.open_table(td)?.insert(key, value)?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.open_table(td)?.remove(key)?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        self.tx.delete_table(td)?;
        self.tx.open_table(td)?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.tx.commit()?;
        Ok(())
    }
}

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is
/// useful.
#[derive(Clone, Debug)]
pub struct ReDB {
    db: Arc<RwLock<ReDatabase>>,
    shutdown_tx: SyncSender<()>,
}

impl Drop for ReDB {
    fn drop(&mut self) {
        if Arc::strong_count(&self.db) <= 2 {
            tracing::info!(target: "rayls::redb", "ReDb Dropping, shutting down metrics thread");
            // shutdown_tx is a sync sender with no buffer so this should block until the thread
            // reads it and shutsdown.
            if let Err(e) = self.shutdown_tx.send(()) {
                tracing::error!(target: "rayls::redb", "Error while trying to send shutdown to redb metrics thread {e}");
            }
        }
    }
}

impl ReDB {
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<ReDB> {
        let db = Arc::new(RwLock::new(ReDatabase::create(path.as_ref().join("redb"))?));
        let db_cloned = Arc::clone(&db);
        let (shutdown_tx, rx) = mpsc::sync_channel::<()>(0);

        // Spawn thread to update metrics from ReDB stats every 2 seconds.
        std::thread::spawn(move || {
            tracing::info!(target: "rayls::redb", "Starting ReDb metrics thread");
            let metrics = ReDbMetrics::default();
            while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(2))
            {
                match db_cloned.read().begin_write() {
                    Ok(txn) => match txn.stats() {
                        Ok(status) => {
                            tracing::trace!(target: "rayls::redb", "ReDb metrics thread {status:?}");
                            metrics.tree_height.set(status.tree_height() as i64);
                            metrics
                                .allocated_pages
                                .set(status.allocated_pages().try_into().unwrap_or(-1));
                            metrics.leaf_pages.set(status.leaf_pages().try_into().unwrap_or(-1));
                            metrics
                                .branch_pages
                                .set(status.branch_pages().try_into().unwrap_or(-1));
                            metrics
                                .stored_bytes
                                .set(status.stored_bytes().try_into().unwrap_or(-1));
                            metrics
                                .metadata_bytes
                                .set(status.metadata_bytes().try_into().unwrap_or(-1));
                            metrics
                                .fragmented_bytes
                                .set(status.fragmented_bytes().try_into().unwrap_or(-1));
                            metrics.page_size.set(status.page_size().try_into().unwrap_or(-1));
                        }
                        Err(e) => {
                            tracing::error!(target: "rayls::redb", "Error while trying to get redb status: {e}");
                        }
                    },
                    Err(e) => {
                        tracing::error!(target: "rayls::redb", "Error while trying to get redb status: {e}");
                    }
                }
            }
            tracing::info!(target: "rayls::redb", "Ending ReDb metrics thread");
        });

        Ok(ReDB { db, shutdown_tx })
    }
}

impl Database for ReDB {
    type TX<'txn> = ReDbTx;
    type TXMut<'txn> = ReDbTxMut;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        let txn = self.db.read().begin_write()?;
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        txn.open_table(td)?;
        txn.commit()?;
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        let tx = self.db.read().begin_read()?;
        Ok(ReDbTx { tx })
    }

    /// ReDb can only allows one write txn at a time.  Calling this with an existing transaction
    /// open will block until it closes.  This can be problematic when used directly in async
    /// code.  Note that the LayeredDatabase handles this issues.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        let tx = self.db.read().begin_write()?;
        Ok(ReDbTxMut { tx })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.with_read_txn(|tx| Ok(tx.get::<T>(key)?.is_some()))
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.with_read_txn(|tx| tx.get::<T>(key))
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.with_write_txn(|txn| {
            txn.insert::<T>(key, value)?;
            Ok(())
        })
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.with_write_txn(|txn| {
            txn.remove::<T>(key)?;
            Ok(())
        })
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.with_write_txn(|txn| {
            txn.clear_table::<T>()?;
            Ok(())
        })
    }

    fn is_empty<T: Table>(&self) -> bool {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        if let Ok(txn) = self.read_txn() {
            if let Ok(table) = txn.tx.open_table(td) {
                return table.is_empty().unwrap_or_default();
            }
        }
        false
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let guard = self.db.read();
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table.iter().expect("Unable to get a DB iter").filter(|r| r.is_ok()).map(
                            |r| {
                                let (k, v) = r.expect("row is okay");
                                (k.value().clone(), v.value().clone())
                            },
                        ),
                    )
                },
            }
            .build(),
        )
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read();
        let tx = guard.begin_read().expect("Failed to get read txn, DB broken");
        let table = tx.open_table(td).expect("Missing table, DB not configured/opened correctly");
        let items: Vec<_> = table
            .iter()
            .expect("Unable to get a DB iter")
            .filter(|r| r.is_ok())
            .filter_map(|r| r.ok())
            .map(|(k, v)| (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value()))))
            .collect();
        Box::new(items.into_iter())
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read();
        let key = key.clone();
        Ok(Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table
                            .iter()
                            .expect("Unable to get a DB iter")
                            .filter(|r| r.is_ok())
                            .map(|r| {
                                let (k, v) = r.expect("row is okay");
                                (k.value().clone(), v.value().clone())
                            })
                            .skip_while(move |(k, _)| k < &key),
                    )
                },
            }
            .build(),
        ))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read();
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, ReDatabase>| {
                    guard
                        .begin_read()
                        .expect("Failed to get read txn, DB broken")
                        .open_table(td)
                        .expect("Missing table, DB not configured/opened correctly")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<T::Key>, ValWrap<T::Value>>| {
                    Box::new(
                        table
                            .iter()
                            .expect("Unable to get a DB iter")
                            .rev()
                            .filter(|r| r.is_ok())
                            .map(|r| {
                                let (k, v) = r.expect("row is okay");
                                (k.value().clone(), v.value().clone())
                            }),
                    )
                },
            }
            .build(),
        )
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let guard = self.db.read();
        let tx = guard.begin_read().expect("Failed to get read txn, DB broken");
        let table = tx.open_table(td).expect("Missing table, DB not configured/opened correctly");
        let items: Vec<_> = table
            .iter()
            .expect("Unable to get a DB iter")
            .rev()
            .filter(|r| r.is_ok())
            .filter_map(|r| r.ok())
            .map(|(k, v)| (Cow::Owned(encode_key(&k.value())), Cow::Owned(encode(&v.value()))))
            .collect();
        Box::new(items.into_iter())
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let read_table = self.db.read().begin_read().ok()?.open_table(td).ok()?;
        let mut last = None;
        for (k, v) in read_table.iter().ok()?.flatten() {
            let (k, v) = (k.value().clone(), v.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last.map(|(k, v)| (k.clone(), v.clone()))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        let td = TableDefinition::<KeyWrap<T::Key>, ValWrap<T::Value>>::new(T::NAME);
        let read_table = self.db.read().begin_read().ok()?.open_table(td).ok()?;
        read_table.last().ok().flatten().map(|(k, v)| (k.value().clone(), v.value().clone()))
    }

    fn compact(&self) -> eyre::Result<()> {
        self.db.write().compact()?;
        Ok(())
    }
}

#[self_referencing(pub_extras)]
pub struct ReDBIter<'a, K, V>
where
    K: KeyT,
    V: ValueT,
{
    guard: RwLockReadGuard<'a, ReDatabase>,
    #[borrows(mut guard)]
    table: ReadOnlyTable<KeyWrap<K>, ValWrap<V>>,
    #[borrows(table)]
    #[covariant]
    iter: Box<dyn Iterator<Item = (K, V)> + 'this>,
}

impl<K, V> Iterator for ReDBIter<'_, K, V>
where
    K: KeyT,
    V: ValueT,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| fields.iter.next())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::test::{db_simp_bench, TestTable};

    use rayls_infrastructure_types::{Database, DbTxMut};

    use super::ReDB;

    fn open_db(path: &Path) -> ReDB {
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    #[test]
    fn test_redb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_redb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_redb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result =
            db.multi_get::<TestTable>([123, 456, 789].iter()).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_redb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to::<TestTable>(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to::<TestTable>(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record::<TestTable>(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to::<TestTable>(&000).expect("Skip failed").count(), 3);
    }

    #[test]
    fn test_redb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    #[test]
    fn test_redb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    #[test]
    fn test_redb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_redb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_redb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    #[test]
    fn test_redb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    #[test]
    fn test_redb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_redb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

        // Create kv pairs
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check insertion
        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        let mut txn = db.write_txn().unwrap();
        for (key, _val) in (0..101).map(|i| (i, i.to_string())).take(50) {
            txn.remove::<TestTable>(&key).expect("Failed to batch remove");
        }
        txn.commit().unwrap();
        assert_eq!(db.iter::<TestTable>().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_redb_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db_simp_bench(db, "ReDb");
    }
}
