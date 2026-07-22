//! An observable in-memory backend for tests that assert on transaction shape.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use parking_lot::Mutex;
use rayls_infrastructure_types::{DBIter, DBRawIter, Database, Table};

use super::ARCHIVE_HIGH_WATER_MARK_KEY;
use crate::{
    mem_db::{MemDatabase, MemDbTx, MemDbTxMut},
    tables::{ColdArchiveHighWaterMark, ColdBatchLocations},
};

/// State observed at the start of one write transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxnStart {
    /// Auxiliary-index rows already committed by earlier transactions.
    pub(crate) locations: usize,
    /// Whether the high-water mark was already committed by an earlier transaction.
    pub(crate) high_water_mark: bool,
}

/// A [`MemDatabase`] that counts read transactions, snapshots what each write transaction found
/// already committed, and can fail every read.
///
/// Transaction boundaries belong to the generic commit path rather than to any backend, so an
/// in-memory probe pins the same ordering the node gets, exactly: writes apply synchronously.
#[derive(Clone, Debug)]
pub(crate) struct ProbeDb {
    /// The backend every operation delegates to.
    inner: MemDatabase,
    /// Stands in for the MDBX faults a read can hit (txn open error, map resized, poisoned dbi).
    read_fault: bool,
    /// Read transactions opened so far, shared across clones.
    read_txns: Arc<AtomicUsize>,
    /// One snapshot per write transaction, taken when it opens rather than when it commits.
    write_starts: Arc<Mutex<Vec<TxnStart>>>,
}

impl ProbeDb {
    /// Builds a probe over a fresh backend with the node's tables opened.
    pub(crate) fn new() -> Self {
        let mut db = Self {
            inner: MemDatabase::new(),
            read_fault: false,
            read_txns: Arc::new(AtomicUsize::new(0)),
            write_starts: Arc::new(Mutex::new(Vec::new())),
        };
        // Opened through the node's own table list so a table added later is not missed here.
        crate::open_default_tables(&mut db.inner).expect("open tables");
        db
    }

    /// Builds a probe whose every `read_txn` fails.
    pub(crate) fn failing_reads() -> Self {
        Self { read_fault: true, ..Self::new() }
    }

    /// Returns how many read transactions have been opened.
    pub(crate) fn read_txns(&self) -> usize {
        self.read_txns.load(Ordering::SeqCst)
    }

    /// Returns one entry per write transaction, in the order they were opened.
    pub(crate) fn write_starts(&self) -> Vec<TxnStart> {
        self.write_starts.lock().clone()
    }
}

impl Database for ProbeDb {
    type TX<'txn>
        = MemDbTx<'txn>
    where
        Self: 'txn;

    type TXMut<'txn>
        = MemDbTxMut<'txn>
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        self.read_txns.fetch_add(1, Ordering::SeqCst);
        if self.read_fault {
            eyre::bail!("hot read transaction unavailable");
        }
        self.inner.read_txn()
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.write_starts.lock().push(TxnStart {
            locations: self.inner.iter::<ColdBatchLocations>().count(),
            high_water_mark: self
                .inner
                .contains_key::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)
                .expect("probe high-water mark"),
        });
        self.inner.write_txn()
    }

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.inner.open_table::<T>()
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
