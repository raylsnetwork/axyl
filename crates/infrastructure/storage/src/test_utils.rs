//! The go-to test [`Database`], so a test never re-implements the trait.
//!
//! [`TestDb`] wraps a [`MemDatabase`] and bundles every capability the node's tests have needed:
//! read/write fault injection, a write gate that parks a write until released, counters for read
//! transactions, reverse scans, and writer barriers, and (under `cold-storage`) a snapshot of what
//! each write transaction found already committed. Prefer it over a bespoke `Database` impl.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};

use rayls_infrastructure_types::{DBIter, DBRawIter, Database, Table};

use crate::mem_db::{MemDatabase, MemDbTx, MemDbTxMut};

/// State observed at the start of one write transaction (populated only under `cold-storage`).
#[cfg(feature = "cold-storage")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnStart {
    /// Auxiliary-index rows already committed by earlier transactions.
    pub locations: usize,
    /// Whether the high-water mark was already committed by an earlier transaction.
    pub high_water_mark: bool,
}

/// A write gate: while armed, a write transaction parks until [`TestDb::release`].
#[derive(Debug, Default)]
struct WriteGate {
    /// Once set, every write transaction parks at the gate.
    armed: AtomicBool,
    /// Set while a write transaction is parked, so a test can wait for it deterministically.
    blocked: AtomicBool,
    /// `(released, condvar)`: the write waits until `released` is set and it is notified.
    lock: Mutex<bool>,
    /// Notified on release.
    cv: Condvar,
}

impl WriteGate {
    /// Parks the calling write until released, if the gate is armed.
    fn wait(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.blocked.store(true, Ordering::SeqCst);
        let mut released = self.lock.lock().expect("gate");
        while !*released {
            released = self.cv.wait(released).expect("gate");
        }
    }
}

/// A [`MemDatabase`] with fault injection, a write-stall gate, access counters, and (under
/// `cold-storage`) per-write snapshots.
///
/// Transaction boundaries belong to the generic commit path rather than to any backend, so an
/// in-memory probe pins the same ordering the node gets, exactly: writes apply synchronously.
#[derive(Clone, Debug, Default)]
pub struct TestDb {
    /// The backend every operation delegates to.
    inner: MemDatabase,
    /// Stands in for the MDBX faults a read can hit (txn open error, map resized, poisoned dbi).
    read_fault: bool,
    /// Stands in for a hot tier rejecting writes outright (e.g. its map is full).
    write_fault: bool,
    /// Read transactions opened so far, shared across clones.
    read_txns: Arc<AtomicUsize>,
    /// Full reverse scans requested so far, shared across clones.
    reverse_iters: Arc<AtomicUsize>,
    /// Writer barriers (`sync_persist`) requested so far, shared across clones.
    barriers: Arc<AtomicUsize>,
    /// The write-stall gate, shared across clones.
    gate: Arc<WriteGate>,
    /// One snapshot per write transaction, taken when it opens rather than when it commits.
    #[cfg(feature = "cold-storage")]
    write_starts: Arc<Mutex<Vec<TxnStart>>>,
}

impl TestDb {
    /// Builds a test DB over a fresh backend with the node's tables opened.
    pub fn new() -> Self {
        let mut db = Self::default();
        // Opened through the node's own table list so a table added later is not missed here.
        crate::open_default_tables(&mut db.inner).expect("open tables");
        db
    }

    /// Builds a test DB whose every `read_txn` fails.
    pub fn failing_reads() -> Self {
        Self { read_fault: true, ..Self::new() }
    }

    /// Builds a test DB whose every `write_txn` fails: a hot tier rejecting writes outright.
    pub fn failing_writes() -> Self {
        Self { write_fault: true, ..Self::new() }
    }

    /// Arms the write gate: from now on every write transaction parks until [`Self::release`].
    pub fn arm(&self) {
        self.gate.armed.store(true, Ordering::SeqCst);
    }

    /// Whether a write transaction is currently parked at the gate.
    pub fn blocked(&self) -> bool {
        self.gate.blocked.load(Ordering::SeqCst)
    }

    /// Lets every parked and future write transaction through.
    pub fn release(&self) {
        *self.gate.lock.lock().expect("gate") = true;
        self.gate.cv.notify_all();
    }

    /// Returns how many read transactions have been opened.
    pub fn read_txns(&self) -> usize {
        self.read_txns.load(Ordering::SeqCst)
    }

    /// Returns how many full reverse scans have been requested.
    pub fn reverse_iters(&self) -> usize {
        self.reverse_iters.load(Ordering::SeqCst)
    }

    /// Returns how many writer barriers (`sync_persist`) have been requested.
    pub fn barriers(&self) -> usize {
        self.barriers.load(Ordering::SeqCst)
    }

    /// Returns one entry per write transaction, in the order they were opened.
    #[cfg(feature = "cold-storage")]
    pub fn write_starts(&self) -> Vec<TxnStart> {
        self.write_starts.lock().expect("write starts").clone()
    }

    /// Snapshots what earlier write transactions have already committed.
    #[cfg(feature = "cold-storage")]
    fn snapshot_write_start(&self) {
        use crate::{
            cold::ARCHIVE_HIGH_WATER_MARK_KEY,
            tables::{ColdArchiveHighWaterMark, ColdBatchLocations},
        };
        self.write_starts.lock().expect("write starts").push(TxnStart {
            locations: self.inner.iter::<ColdBatchLocations>().count(),
            high_water_mark: self
                .inner
                .contains_key::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)
                .expect("probe high-water mark"),
        });
    }
}

impl Database for TestDb {
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
        if self.write_fault {
            eyre::bail!("hot tier rejecting writes");
        }
        self.gate.wait();
        #[cfg(feature = "cold-storage")]
        self.snapshot_write_start();
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
        self.reverse_iters.fetch_add(1, Ordering::SeqCst);
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
    fn sync_persist(&self) -> eyre::Result<()> {
        self.barriers.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_persist()
    }
}
