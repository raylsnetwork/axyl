//! Impermanent storage in memory - useful for tests.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashMap},
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        mpsc::{self, SyncSender},
        Arc, LazyLock,
    },
    time::{Duration, Instant},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};

use crate::open_default_tables;

/// Bit 0 of a row's packed word: the row is a tombstone (deleted in mem, pending/durable removal).
const TOMBSTONE_BIT: u32 = 1;

/// One in-flight op occupies bit 1 onward, i.e. the count field starts at bit 1, so each count
/// increment is `1 << TOMBSTONE_BIT` (2) on the packed word. It is even on purpose: adding or
/// subtracting it can never touch bit 0, keeping the tombstone flag intact through arithmetic.
const IN_FLIGHT_UNIT: u32 = 1 << TOMBSTONE_BIT;

/// Writes to a hot row's recency clock are throttled to one per window: reads only need a coarse
/// ordering, and every store lands on the same cache line while the read lock is held.
const RECENCY_BUMP_THRESHOLD: Duration = Duration::from_millis(100);

static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Monotonic nanos since process start; cheap (vDSO) and lock-free.
fn now_nanos() -> u64 {
    PROCESS_START.elapsed().as_nanos() as u64
}

/// A single cached row: the encoded value plus a packed `(tombstoned, in_flight)` word and a
/// lock-free recency clock.
///
/// `packed` bit 0 is the tombstone flag; bits 1..=31 are the number of writer ops queued for
/// this key (the "in flight" count). It is a plain `u32` because every access happens under the
/// store `RwLock`: producers increment under the write lock, the writer thread decrements under
/// the write lock, and reads peek at the flag under the read lock. A count of zero means no
/// queued op remains for the key, so the mem row exactly matches the persistent tier and the
/// key is safe to evict.
///
/// `last_used` is an atomic so a hot read can refresh recency while holding only the read lock;
/// writes are throttled to [`RECENCY_BUMP_THRESHOLD`] to keep the atomic cache line quiet.
#[derive(Debug)]
struct StoreEntry {
    value: Vec<u8>,
    packed: u32,
    last_used: AtomicU64,
}

impl StoreEntry {
    fn new(value: Vec<u8>) -> Self {
        Self { value, packed: 0, last_used: AtomicU64::new(now_nanos()) }
    }

    fn tombstoned(&self) -> bool {
        self.packed & TOMBSTONE_BIT != 0
    }

    fn in_flight(&self) -> u32 {
        self.packed >> TOMBSTONE_BIT
    }

    fn mark_tombstone(&mut self) {
        self.packed |= TOMBSTONE_BIT;
    }

    fn clear_tombstone(&mut self) {
        self.packed &= !TOMBSTONE_BIT;
    }

    fn add_in_flight(&mut self) {
        debug_assert!(
            self.in_flight() < u32::MAX >> TOMBSTONE_BIT,
            "in-flight op count overflow for a single key"
        );
        self.packed += IN_FLIGHT_UNIT;
    }

    fn dec_in_flight(&mut self) {
        debug_assert!(self.in_flight() != 0, "in-flight op count is 0 before decrementing it");
        self.packed -= IN_FLIGHT_UNIT;
    }

    /// Refreshes the recency clock if the previous bump is older than the throttle window.
    fn touch_approximately(&self) {
        let now = now_nanos();
        let last = self.last_used.load(AtomicOrdering::Relaxed);
        if now.saturating_sub(last) >= RECENCY_BUMP_THRESHOLD.as_nanos() as u64 {
            self.last_used.store(now, AtomicOrdering::Relaxed);
        }
    }

    /// Unconditional recency refresh; used on writes, which already serialize on the write lock.
    fn touch(&mut self) {
        self.last_used.store(now_nanos(), AtomicOrdering::Relaxed);
    }
}

/// Writer-owned min-heap of evictable keys ordered by recency: `Reverse<(last_used, table, key)>`.
///
/// Only the background writer pushes (when a key's in-flight count settles to zero) and pops
/// (during eviction), so no lock guards it. Entries go stale when the key is re-inserted or the
/// table is cleared; they are validated at pop under the store write lock and skipped.
pub(crate) type EvictionHeap = BinaryHeap<Reverse<(u64, &'static str, Vec<u8>)>>;

/// Outcome of one eviction pass, for the layered writer's cache-pressure logs.
#[derive(Debug, Clone, Copy, Default)]
pub struct EvictionStats {
    /// Total cached rows (live and tombstoned) when the pass started.
    pub before: usize,
    /// Total cached rows after the pass.
    pub after: usize,
    /// Rows removed during the pass.
    pub evicted: usize,
}

/// One cached table: the rows plus a flag set while the producer's clear is queued but not yet
/// applied to the persistent tier. `clearing` is set under the same write lock that enqueues the
/// `Clear` message, so any reader that sees it is guaranteed the clear will land after all
/// previously queued ops; it is cleared by the writer once the persistent clear applied.
#[derive(Debug)]
struct StoreTable {
    rows: BTreeMap<Vec<u8>, StoreEntry>,
    clearing: bool,
}

impl StoreTable {
    fn new() -> Self {
        Self { rows: BTreeMap::new(), clearing: false }
    }
}

type StoreType = HashMap<&'static str, StoreTable>;

#[derive(Debug)]
pub struct MemDbTx<'a> {
    store: RwLockReadGuard<'a, StoreType>,
}

impl<'a> MemDbTx<'a> {
    pub fn get_no_marked_check<T: Table>(&self, key: &T::Key) -> Option<(bool, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some(entry) = table.rows.get(&key_bytes) {
                if !entry.tombstoned() {
                    entry.touch_approximately();
                }
                let val = decode(&entry.value);
                return Some((entry.tombstoned(), val));
            }
        }
        None
    }

    /// Check if a key is tombstoned (marked for deletion) without deserializing the value.
    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.rows.get(&key_bytes).is_some_and(|entry| entry.tombstoned());
        }
        false
    }

    /// Whether the table's clear is queued but not yet applied to the persistent tier (see
    /// [`MemDatabase::is_clearing`]).
    pub fn is_clearing<T: Table>(&self) -> bool {
        self.store.get(T::NAME).is_some_and(|table| table.clearing)
    }
}

impl<'a> DbTx for MemDbTx<'a> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(get_with_marked_check::<T>(&self.store, key))
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(contains_key_impl::<T>(&self.store, key))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        iter_borrowed_impl::<T>(&self.store).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match raw_iter_borrowed_impl::<T>(&self.store) {
            Some(iter) => iter,
            None => Box::new(std::iter::empty()),
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        skip_to_borrowed_impl::<T>(&self.store, key)
            .ok_or_else(|| eyre::eyre!("Invalid table {}", T::NAME))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        reverse_iter_borrowed_impl::<T>(&self.store).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match reverse_raw_iter_borrowed_impl::<T>(&self.store) {
            Some(iter) => iter,
            None => Box::new(std::iter::empty()),
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        last_record_impl::<T>(&self.store)
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        record_prior_to_impl::<T>(&self.store, key)
    }

    fn disable_long_read_safety(&self) {}
}

#[derive(Debug)]
pub struct MemDbTxMut<'a> {
    store: RwLockWriteGuard<'a, StoreType>,
}

impl<'a> MemDbTxMut<'a> {
    /// Hard-removes `key` from the in-memory store, leaving no tombstone.
    ///
    /// Persistent eviction archives a row permanently (never re-inserted), so the cache entry is
    /// dropped outright rather than tombstoned: this frees the cache and lets reads fall through to
    /// a lower tier, whereas a tombstone would shadow it and never be reclaimed.
    pub fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        if let Some(table) = self.store.get_mut(T::NAME) {
            table.rows.remove(&encode_key(key));
        }
    }

    /// Raw keys of the table, for a `Clear` message: the writer needs the exact set that was
    /// tombstoned (and counted) by this clear to release each key's in-flight op at apply time.
    pub fn raw_keys<T: Table>(&self) -> Vec<Vec<u8>> {
        self.store
            .get(T::NAME)
            .map(|table| table.rows.keys().cloned().collect())
            .unwrap_or_default()
    }
}

impl<'a> DbTx for MemDbTxMut<'a> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        //if not in cache check store
        Ok(get_with_marked_check::<T>(&self.store, key))
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(contains_key_impl::<T>(&self.store, key))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // To implement this we need to merge results from cache and store in a temporary vector and
        // return iterator over that. This is not expected to used in a transaction, so
        // should be safe.
        panic!("Should not be called on a tx mut!");
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("Should not be called on a tx mut!");
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        skip_to_borrowed_impl::<T>(&self.store, key)
            .ok_or_else(|| eyre::eyre!("Invalid table {}", T::NAME))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        reverse_iter_borrowed_impl::<T>(&self.store).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match reverse_raw_iter_borrowed_impl::<T>(&self.store) {
            Some(iter) => iter,
            None => Box::new(std::iter::empty()),
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        last_record_impl::<T>(&self.store)
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        record_prior_to_impl::<T>(&self.store, key)
    }

    fn disable_long_read_safety(&self) {}
}

impl<'a> DbTxMut for MemDbTxMut<'a> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        insert_impl::<T>(&mut self.store, key, value)
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        remove_impl::<T>(&mut self.store, key)
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        clear_table_impl::<T>(&mut self.store).map(|_| ())
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
                        m.set(table.rows.len().try_into().unwrap_or(-1));
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
        Ok(self.read_txn_impl().get_no_marked_check::<T>(key))
    }

    /// Check if a key is tombstoned (marked for deletion) without deserializing the value.
    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        self.read_txn_impl().is_tombstoned::<T>(key)
    }

    /// Enqueue a mem mutation and its writer message in one critical section: the in-flight count
    /// is bumped under the same write lock that mutates the cache, and `on_queued` (the `send`)
    /// runs before the lock is released, so the channel order matches the mem mutation order.
    pub fn insert_queued<T: Table>(
        &self,
        key: &T::Key,
        value: &T::Value,
        on_queued: impl FnOnce() -> eyre::Result<()>,
    ) -> eyre::Result<()> {
        let mut store = self.store.write();
        insert_impl::<T>(&mut store, key, value)?;
        on_queued()
    }

    pub fn remove_queued<T: Table>(
        &self,
        key: &T::Key,
        on_queued: impl FnOnce() -> eyre::Result<()>,
    ) -> eyre::Result<()> {
        let mut store = self.store.write();
        remove_impl::<T>(&mut store, key)?;
        on_queued()
    }

    pub fn clear_table_queued<T: Table>(
        &self,
        on_queued: impl FnOnce(Vec<Vec<u8>>) -> eyre::Result<()>,
    ) -> eyre::Result<()> {
        let mut store = self.store.write();
        let keys = clear_table_impl::<T>(&mut store)?;
        on_queued(keys)
    }

    /// Releases one in-flight op for `key` after its writer message applied (or the txn
    /// committed). When the count reaches zero the key is pushed onto the eviction heap: no
    /// queued op remains, so the mem row equals the persistent tier and eviction cannot expose
    /// a stale value.
    pub fn on_op_applied(&self, table: &'static str, key_bytes: &[u8], heap: &mut EvictionHeap) {
        let mut store = self.store.write();
        let Some(table_map) = store.get_mut(table) else { return };
        let Some(entry) = table_map.rows.get_mut(key_bytes) else { return };
        entry.dec_in_flight();
        if entry.in_flight() == 0 {
            heap.push(Reverse((
                entry.last_used.load(AtomicOrdering::Relaxed),
                table,
                key_bytes.to_vec(),
            )));
        }
    }

    /// Whether the table's clear is queued but not yet applied to the persistent tier.
    ///
    /// While set, layered reads must treat every key that is not live in the cache as deleted:
    /// keys evicted before the clear (or never cached, e.g. right after startup) would otherwise
    /// fall through to the persistent tier and surface stale pre-clear values. The flag is set
    /// under the same write lock that enqueues the `Clear` message and cleared by the writer once
    /// the persistent clear applied, so it is only ever observed while a clear is genuinely
    /// pending.
    pub fn is_clearing<T: Table>(&self) -> bool {
        self.store.read().get(T::NAME).is_some_and(|table| table.clearing)
    }

    /// Clears the pending-clear flag and releases the in-flight op of every key the clear
    /// tombstoned, in one critical section: the writer calls this after the persistent clear
    /// applied, so once it returns the cache matches the (now empty) persistent tier again.
    ///
    /// If the persistent clear failed, this is never called: the flag stays set and the table
    /// keeps reading as empty until a later clear retries and lands (same philosophy as rows
    /// retained in mem on failure, surfaced by the next persist).
    pub fn on_clear_applied<T: Table>(&self, keys: &[Vec<u8>], heap: &mut EvictionHeap) {
        let mut store = self.store.write();
        let Some(table) = store.get_mut(T::NAME) else { return };
        table.clearing = false;
        for key in keys {
            let Some(entry) = table.rows.get_mut(key) else { continue };
            entry.dec_in_flight();
            if entry.in_flight() == 0 {
                heap.push(Reverse((
                    entry.last_used.load(AtomicOrdering::Relaxed),
                    T::NAME,
                    key.clone(),
                )));
            }
        }
    }

    /// Evicts settled keys (in-flight == 0) in recency order until the cache fits `max_size`.
    /// Candidates are validated at pop: a key re-inserted since its settle, or a row cleared
    /// away, is skipped. A key whose recency was refreshed by a read since it settled is
    /// re-pushed with the fresh clock so hot keys survive eviction. The heap stays writer-owned;
    /// producers never touch it. Every pop removes one entry, so the loop always terminates.
    pub fn evict_if_needed(&self, heap: &mut EvictionHeap, max_size: usize) -> EvictionStats {
        let mut store = self.store.write();
        let mut total: usize = store.values().map(|table| table.rows.len()).sum();
        if total <= max_size {
            return EvictionStats { before: total, after: total, evicted: 0 };
        }
        let mut evicted = 0usize;
        while total > max_size {
            let Some(Reverse((heap_last_used, table, key))) = heap.pop() else { break };
            let Some(table_map) = store.get_mut(table) else { continue };
            let Some(entry) = table_map.rows.get(&key) else { continue };
            if entry.in_flight() != 0 {
                continue;
            }
            let current_last_used = entry.last_used.load(AtomicOrdering::Relaxed);
            if current_last_used != heap_last_used {
                // A read refreshed the key after it settled: it is no longer least-recently
                // used. Re-push with the fresh clock; recency bumps are throttled (~100ms), so
                // each hot key pays at most ~10 re-pushes per second.
                heap.push(Reverse((current_last_used, table, key)));
                continue;
            }
            table_map.rows.remove(&key);
            total -= 1;
            evicted += 1;
        }
        EvictionStats { before: total + evicted, after: total, evicted }
    }

    /// Total rows (live and tombstoned) held in the cache; the writer keeps this at or below the
    /// configured max size.
    pub fn mem_size(&self) -> usize {
        self.store.read().values().map(|table| table.rows.len()).sum()
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
            tracing::error!(target: "rayls::memdb", "Error while trying to send shutdown to MemDatabase metrics thread {e}");
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
        self.store.write().insert(T::NAME, StoreTable::new());
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
        self.read_txn_impl().contains_key::<T>(key)
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
        self.store
            .read()
            .get(T::NAME)
            .is_none_or(|table| table.rows.values().all(|entry| entry.tombstoned()))
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        match iter_owned_impl::<T>(&self.store.read()) {
            Some(items) => Box::new(items.into_iter()),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        // The guard is a temporary here (not held by `self`), so the bytes must
        // be owned rather than borrowed for the iterator's lifetime.
        match raw_iter_owned_impl::<T>(&self.store.read()) {
            Some(items) => Box::new(items.into_iter()),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        match skip_to_owned_impl::<T>(&self.store.read(), key) {
            Some(items) => Ok(Box::new(items.into_iter())),
            None => Err(eyre::eyre!("Invalid table {}", T::NAME)),
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        match reverse_iter_owned_impl::<T>(&self.store.read()) {
            Some(items) => Box::new(items.into_iter()),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match reverse_raw_iter_owned_impl::<T>(&self.store.read()) {
            Some(items) => Box::new(items.into_iter()),
            None => panic!("Invalid table {}", T::NAME),
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.read_txn_impl().last_record::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.read_txn_impl().record_prior_to::<T>(key)
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

/// Mutate + count a row: value replaced, tombstone cleared, one in-flight op registered, recency
/// touched. Shared by the txn producers (guard already held) and the `*_queued` entry points.
fn insert_impl<T: Table>(
    store: &mut StoreType,
    key: &T::Key,
    value: &T::Value,
) -> eyre::Result<()> {
    let table = store.get_mut(T::NAME).ok_or_else(|| eyre::eyre!("Invalid table {}", T::NAME))?;
    let key_bytes = encode_key(key);
    match table.rows.get_mut(&key_bytes) {
        Some(entry) => {
            entry.value = encode(value);
            entry.clear_tombstone();
            entry.add_in_flight();
            entry.touch();
        }
        None => {
            let mut entry = StoreEntry::new(encode(value));
            entry.add_in_flight();
            table.rows.insert(key_bytes, entry);
        }
    }
    Ok(())
}

/// Tombstone + count a row: the row is hidden from reads immediately, and its in-flight op keeps
/// it in the cache until the persistent remove applies. A tombstone is inserted for keys that
/// only exist in the persistent layer.
fn remove_impl<T: Table>(store: &mut StoreType, key: &T::Key) -> eyre::Result<()> {
    let table = store.get_mut(T::NAME).ok_or_else(|| eyre::eyre!("Invalid table {}", T::NAME))?;
    let key_bytes = encode_key(key);
    match table.rows.get_mut(&key_bytes) {
        Some(entry) => {
            entry.mark_tombstone();
            entry.add_in_flight();
            entry.touch();
        }
        None => {
            let mut entry = StoreEntry::new(Vec::new());
            entry.mark_tombstone();
            entry.add_in_flight();
            table.rows.insert(key_bytes, entry);
        }
    }
    Ok(())
}

/// Tombstone every row of the table (the persistent clear is deferred) and bump each in-flight
/// count so no tombstone is evicted before the clear applies. Returns the raw keys, which the
/// writer needs to release each count once the clear lands.
///
/// Also raises the table's `clearing` flag: both callers enqueue the `Clear` message while still
/// holding the write lock, so a reader that sees the flag can rely on the persistent tier being
/// wiped shortly; until the writer applies the clear, reads must not fall through to it (keys
/// evicted from the cache would otherwise surface stale pre-clear values).
fn clear_table_impl<T: Table>(store: &mut StoreType) -> eyre::Result<Vec<Vec<u8>>> {
    let table = store.get_mut(T::NAME).ok_or_else(|| eyre::eyre!("Invalid table {}", T::NAME))?;
    let keys: Vec<Vec<u8>> = table.rows.keys().cloned().collect();
    for entry in table.rows.values_mut() {
        entry.mark_tombstone();
        entry.add_in_flight();
    }
    table.clearing = true;
    Ok(keys)
}

fn get_with_marked_check<T: Table>(store: &StoreType, key: &T::Key) -> Option<T::Value> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode_key(key);
        if let Some(entry) = table.rows.get(&key_bytes) {
            if !entry.tombstoned() {
                entry.touch_approximately();
                let val = decode(&entry.value);
                return Some(val);
            }
        }
    }
    None
}

fn contains_key_impl<T: Table>(store: &StoreType, key: &T::Key) -> bool {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode_key(key);
        if let Some(entry) = table.rows.get(&key_bytes) {
            if entry.tombstoned() {
                return false;
            }
            entry.touch_approximately();
            return true;
        }
    }
    false
}

/// Decodes the live (non-tombstoned) rows of `iter` on demand.
///
/// Lazy rather than collected: a caller that stops after a handful of rows never pays to decode
/// the rest of the table, which for a table of certificates or headers dwarfs the walk itself.
fn decode_typed<'t, T: Table, I>(iter: I) -> DBIter<'t, T>
where
    I: Iterator<Item = (&'t Vec<u8>, &'t StoreEntry)> + 't,
{
    Box::new(
        iter.filter(|(_, entry)| !entry.tombstoned())
            .map(|(k, entry)| (decode_key::<T::Key>(k), decode::<T::Value>(&entry.value))),
    )
}

fn collect_raw_borrowed<'t, I>(iter: I) -> DBRawIter<'t>
where
    I: Iterator<Item = (&'t Vec<u8>, &'t StoreEntry)> + 't,
{
    Box::new(
        iter.filter(|(_, entry)| !entry.tombstoned())
            .map(|(k, entry)| (Cow::Borrowed(k.as_slice()), Cow::Borrowed(entry.value.as_slice()))),
    )
}

fn collect_raw_owned<'t, I>(iter: I) -> Vec<(Cow<'static, [u8]>, Cow<'static, [u8]>)>
where
    I: Iterator<Item = (&'t Vec<u8>, &'t StoreEntry)>,
{
    iter.filter(|(_, entry)| !entry.tombstoned())
        .map(|(k, entry)| (Cow::Owned(k.clone()), Cow::Owned(entry.value.clone())))
        .collect()
}

/// Snapshots the table for a caller whose read guard is a temporary (see [`raw_iter_owned_impl`]).
fn iter_owned_impl<T: Table>(store: &StoreType) -> Option<Vec<(T::Key, T::Value)>> {
    Some(iter_borrowed_impl::<T>(store)?.collect())
}

fn iter_borrowed_impl<T: Table>(store: &StoreType) -> Option<DBIter<'_, T>> {
    let table = store.get(T::NAME)?;
    Some(decode_typed::<T, _>(table.rows.iter()))
}

fn raw_iter_borrowed_impl<T: Table>(store: &StoreType) -> Option<DBRawIter<'_>> {
    let table = store.get(T::NAME)?;
    Some(collect_raw_borrowed(table.rows.iter()))
}

fn raw_iter_owned_impl<T: Table>(
    store: &StoreType,
) -> Option<Vec<(Cow<'static, [u8]>, Cow<'static, [u8]>)>> {
    let table = store.get(T::NAME)?;
    Some(collect_raw_owned(table.rows.iter()))
}

fn skip_to_owned_impl<T: Table>(
    store: &StoreType,
    key: &T::Key,
) -> Option<Vec<(T::Key, T::Value)>> {
    Some(skip_to_borrowed_impl::<T>(store, key)?.collect())
}

fn skip_to_borrowed_impl<'s, T: Table>(
    store: &'s StoreType,
    key: &T::Key,
) -> Option<DBIter<'s, T>> {
    let table = store.get(T::NAME)?;
    Some(decode_typed::<T, _>(table.rows.range(encode_key(key)..)))
}

fn reverse_iter_owned_impl<T: Table>(store: &StoreType) -> Option<Vec<(T::Key, T::Value)>> {
    Some(reverse_iter_borrowed_impl::<T>(store)?.collect())
}

fn reverse_iter_borrowed_impl<T: Table>(store: &StoreType) -> Option<DBIter<'_, T>> {
    let table = store.get(T::NAME)?;
    Some(decode_typed::<T, _>(table.rows.iter().rev()))
}

fn reverse_raw_iter_borrowed_impl<T: Table>(store: &StoreType) -> Option<DBRawIter<'_>> {
    let table = store.get(T::NAME)?;
    Some(collect_raw_borrowed(table.rows.iter().rev()))
}

fn reverse_raw_iter_owned_impl<T: Table>(
    store: &StoreType,
) -> Option<Vec<(Cow<'static, [u8]>, Cow<'static, [u8]>)>> {
    let table = store.get(T::NAME)?;
    Some(collect_raw_owned(table.rows.iter().rev()))
}

fn last_record_impl<T: Table>(store: &StoreType) -> Option<(T::Key, T::Value)> {
    let table = store.get(T::NAME)?;
    for (key_bytes, entry) in table.rows.iter().rev() {
        if !entry.tombstoned() {
            return Some((decode_key(key_bytes), decode(&entry.value)));
        }
    }
    None
}

fn record_prior_to_impl<T: Table>(store: &StoreType, key: &T::Key) -> Option<(T::Key, T::Value)> {
    let table = store.get(T::NAME)?;
    let key_bytes = encode_key(key);
    table
        .rows
        .range(..key_bytes)
        .rev()
        .find(|(_, entry)| !entry.tombstoned())
        .map(|(k, entry)| (decode_key(k), decode(&entry.value)))
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
    fn test_memdb_txn_iter_order() {
        let db = open_db();
        test_txn_iter_order(db)
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
