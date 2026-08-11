use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::Debug,
    future::Future,
    iter::Peekable,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(feature = "cold-storage")]
use crate::{
    cold::{ColdStore, ColdTx},
    tables::ColdBatchLocations,
};

use crate::{
    mem_db::{MemDatabase, MemTxn},
    write_buffer::{PersistClear, PersistInsert, PersistOp, PersistRemove, PersistRemoveBatch},
    write_lock::{WriteLockGuard, WriteLockManager},
};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
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

const CACHE_KEEP_TIME_SECS: u64 = 60;
const MAX_CACHE_SIZE: usize = 10000;

/// Manage the persistent DB in a background thread with daily compaction.
/// Drop the mem overlay for committed inserts older than `CACHE_KEEP_TIME_SECS` or beyond
/// `MAX_CACHE_SIZE`. Only safe for committed rows: it removes them from the mem layer.
fn evict_committed<DB: Database>(
    committed_inserts: &mut Vec<(Instant, Box<dyn PersistOp<DB>>)>,
    mem_db: &MemDatabase,
) {
    let total_count = committed_inserts.len();
    let mut remove_count: usize = 0;
    for (instant, insert) in committed_inserts.iter() {
        if instant.elapsed() > Duration::from_secs(CACHE_KEEP_TIME_SECS)
            || total_count - remove_count > MAX_CACHE_SIZE
        {
            insert.clear_mem(mem_db);
            remove_count += 1;
            continue;
        }
        break;
    }
    committed_inserts.drain(..remove_count);
}

/// Depth at which the writer queue is treated as a backlog: `db_run` warns (rate-limited) and
/// data-plane enqueues start pacing, so the imbalance never surfaces as a multi-minute `persist`
/// drain at the next epoch boundary.
const QUEUE_HIGH_WATER_MARK: usize = 10_000;
const QUEUE_LAG_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Pause paid by each insert/remove/clear enqueue while the queue is above
/// [`QUEUE_HIGH_WATER_MARK`].
///
/// Soft backpressure: a slow inner DB surfaces as gradual producer slowdown instead of a
/// network-wide stall at the deterministic boundary `persist`. Pacing can lag the node into
/// demotion (recoverable; the boundary stall is not), and the sleep may land on an async worker
/// thread (the `Database` write API is sync) - the accepted cost in an already-degraded regime.
/// Control messages (persist barrier, shutdown) never pace.
const QUEUE_PACE_SLEEP: Duration = Duration::from_millis(1);

/// Drain time past which a `persist`/`sync_persist` is logged at `warn` rather than `debug`.
const PERSIST_SLOW_WARN: Duration = Duration::from_secs(1);

/// A [`CommitTxn`] sender that tracks the writer queue's depth: every enqueue bumps a shared
/// counter `db_run` decrements as it drains, feeding the depth gauge and the
/// [`QUEUE_HIGH_WATER_MARK`] pacing.
struct QueueSender<DB: Database> {
    tx: Sender<CommitTxn<DB>>,
    depth: Arc<AtomicUsize>,
}

impl<DB: Database> Clone for QueueSender<DB> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone(), depth: Arc::clone(&self.depth) }
    }
}

impl<DB: Database> Debug for QueueSender<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueueSender(depth: {})", self.depth())
    }
}

impl<DB: Database> QueueSender<DB> {
    /// Enqueues a writer message; data-plane messages pace once the depth is past
    /// [`QUEUE_HIGH_WATER_MARK`] (see [`QUEUE_PACE_SLEEP`]).
    fn send(&self, msg: CommitTxn<DB>) -> Result<(), mpsc::SendError<CommitTxn<DB>>> {
        if matches!(msg, CommitTxn::Batch(_)) && self.depth() > QUEUE_HIGH_WATER_MARK {
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

/// Applies commit batches to the persistent backend, one short-lived write transaction per
/// batch, so a commit's ops land atomically and a failed batch can never poison a later one.
///
/// Failed batches stay in the mem overlay (the mem layer is the source of truth until the batch
/// applies) and surface through the next `persist` barrier, exactly like a failed message did.
fn db_run<DB: Database>(
    db: DB,
    mem_db: MemDatabase,
    rx: Receiver<CommitTxn<DB>>,
    depth: Arc<AtomicUsize>,
) {
    let mut last_compact = Instant::now();
    let queue_depth_gauge = writer_queue_depth_gauge();
    let mut last_lag_warn: Option<Instant> = None;

    let mut committed_inserts: Vec<(Instant, Box<dyn PersistOp<DB>>)> = Vec::with_capacity(1000);
    // last write/commit failure since the previous CaughtUp, reported by the next persist
    let mut pending_write_error: Option<String> = None;
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
        match msg {
            CommitTxn::Batch(ops) => {
                if ops.is_empty() {
                    continue;
                }
                let mut txn = match db.write_txn() {
                    Ok(txn) => txn,
                    Err(e) => {
                        tracing::error!(target: "layered_db_runner", "DB ERROR getting write txn (background): {e}");
                        pending_write_error = Some(format!("write txn: {e}"));
                        continue;
                    }
                };
                let applied = ops.iter().try_for_each(|op| op.apply(&mut txn, &mem_db));
                match applied {
                    Ok(()) => match txn.commit() {
                        Ok(()) => {
                            for op in ops {
                                committed_inserts.push((Instant::now(), op));
                            }
                            // Rayls: limit layer growth between commits
                            if committed_inserts.len() > MAX_CACHE_SIZE * 2 {
                                evict_committed(&mut committed_inserts, &mem_db);
                            }
                        }
                        // surface via persist instead of aborting; rows stay in mem, not lost
                        Err(e) => {
                            tracing::error!(target: "layered_db_runner", "consensus DB commit failed: {e}");
                            pending_write_error = Some(format!("commit: {e}"));
                        }
                    },
                    // the batch is abandoned: rows stay in mem (not the eviction cache) so they
                    // are not lost, and the error is surfaced by the next persist
                    Err(e) => {
                        tracing::error!(target: "layered_db_runner", "consensus DB batch failed: {e}");
                        pending_write_error = Some(format!("batch: {e}"));
                    }
                }
            }
            // NOTE: proves prior messages were applied, not that an open shared txn committed.
            // Safe at shutdown because consensus writers are torn down before persist runs.
            CommitTxn::CaughtUp(tx) => {
                let reply: Result<(), String> = match pending_write_error.take() {
                    Some(e) => Err(e),
                    None => Ok(()),
                };
                let _ = tx.send(reply);
            }
            CommitTxn::Shutdown => break,
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

/// A write transaction over the layered database.
///
/// Writes go to a private buffer (invisible to every other transaction) until `commit` merges
/// them into the shared mem overlay and hands the batch to the background writer. Reads walk
/// buffer -> mem overlay -> persistent snapshot -> cold tier, so a transaction sees its own
/// uncommitted writes and every earlier commit, but never an in-flight peer's buffer.
pub struct WriteTxn<'a, DB: Database> {
    mem_txn: MemTxn<'a>,
    /// The typed ops of this transaction, applied to the persistent backend as one batch.
    pending: Vec<Box<dyn PersistOp<DB>>>,
    persistent_snapshot: DB::TX<'a>,
    locks: Vec<WriteLockGuard>,
    lock_manager: Arc<WriteLockManager>,
    tx: QueueSender<DB>,
    /// The cold tier reads fall through to on a hot miss, when attached.
    #[cfg(feature = "cold-storage")]
    cold: Option<&'a ColdStore>,
}

impl<'a, DB: Database> Debug for WriteTxn<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteTxn")
    }
}

impl<'a, DB: Database> WriteTxn<'a, DB> {
    /// Locks the given table, so this transaction's reads of it cannot be interleaved by another
    /// writer's reads of it. Held until this transaction is dropped or commits.
    pub(crate) fn lock(&mut self, table_name: &'static str) -> WriteLockGuard {
        self.locks.push(self.lock_manager.lock(table_name));
        self.locks.last().unwrap().clone()
    }

    /// Opens a cold read transaction over the attached tier, resolving the auxiliary index on
    /// this transaction's own hot view (so an index row written this session is visible).
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

impl<'a, DB: Database> DbTx for WriteTxn<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // 1. private buffer (read-after-write): a buffered remove shadows every tier
        if let Some(val) = self.mem_txn.buffer_get::<T>(key) {
            return Ok(Some(val));
        }
        if self.mem_txn.buffer_is_tombstoned::<T>(key) == Some(true) {
            return Ok(None);
        }
        // 2. shared mem overlay (tombstone-aware): a committed remove shadows the persistent
        //    tier, and only the archived copy may still serve the row.
        if let Some(val) = self.mem_txn.get::<T>(key)? {
            return Ok(Some(val));
        }
        if self.mem_txn.is_tombstoned::<T>(key) {
            return self.cold_get::<T>(key);
        }
        // 3. persistent snapshot
        if let Some(val) = self.persistent_snapshot.get::<T>(key)? {
            return Ok(Some(val));
        }
        // 4. cold tier
        self.cold_get::<T>(key)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Same fallthrough as `get`, but raw bytes: the shared mem overlay stores them as-is, so
        // the archival path skips the decode/re-encode round trip.
        if let Some(bytes) = self.mem_txn.buffer_raw_get::<T>(key) {
            return Ok(Some(Cow::Owned(bytes)));
        }
        if self.mem_txn.buffer_is_tombstoned::<T>(key) == Some(true) {
            return Ok(None);
        }
        if let Some((removed, raw)) = self.mem_txn.get_raw::<T>(key) {
            if !removed {
                return Ok(Some(Cow::Owned(raw)));
            }
            return self.cold_raw_get::<T>(key);
        }
        match self.persistent_snapshot.raw_get::<T>(key)? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.cold_raw_get::<T>(key),
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.persistent_snapshot.iter::<T>();
        let mem_iter = self.mem_txn.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, false)
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.persistent_snapshot.raw_iter::<T>();
        let mem_iter = self.mem_txn.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, false)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = self.persistent_snapshot.skip_to::<T>(key)?;
        let mem_iter = self.mem_txn.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), false))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let db_iter = self.persistent_snapshot.raw_skip_to::<T>(key)?;
        let mem_iter = self.mem_txn.raw_skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold_raw::<T>(hot, Some(key), false))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.persistent_snapshot.reverse_iter::<T>();
        let mem_iter = self.mem_txn.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, true)
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.persistent_snapshot.reverse_raw_iter::<T>();
        let mem_iter = self.mem_txn.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
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
}

impl<'a, DB: Database> DbTxMut for WriteTxn<'a, DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_txn.insert::<T>(key, value)?;
        self.pending.push(Box::new(PersistInsert::<T> { key: key.clone(), value: value.clone() }));
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.mem_txn.remove::<T>(key)?;
        self.pending.push(Box::new(PersistRemove::<T> { key: key.clone() }));
        Ok(())
    }

    fn evict_persistent_batch<T: Table>(&mut self, keys: &[T::Key]) -> eyre::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // Hard-delete (no tombstone): a tombstone would shadow the cold fall-through. The whole
        // set is ONE writer message; a per-row message on a whole-epoch prune is pure overhead.
        for key in keys {
            self.mem_txn.hard_delete::<T>(key);
        }
        self.pending.push(Box::new(PersistRemoveBatch::<T> { keys: keys.to_vec() }));
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.mem_txn.clear_table::<T>()?;
        self.pending.push(Box::new(PersistClear::<T> { _phantom: std::marker::PhantomData }));
        Ok(())
    }

    fn lock_table(&mut self, table_name: &'static str) -> eyre::Result<()> {
        self.lock(table_name);
        Ok(())
    }

    /// Applies the private buffer to the shared mem overlay, then hands the batch to the
    /// background writer. The persistent snapshot is dropped before the locks so the backing
    /// store's read slot is released first; locks release with `self` after the snapshot.
    fn commit(mut self) -> eyre::Result<()> {
        let batch = std::mem::take(&mut self.pending);
        self.mem_txn.commit()?;
        drop(self.persistent_snapshot);
        self.locks.clear();
        if batch.is_empty() {
            return Ok(());
        }
        self.tx.send(CommitTxn::Batch(batch)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
    }
}

/// A read transaction over the layered database: a snapshot of the shared mem overlay plus a
/// persistent-backend read txn, with the cold tier falling through on a hot miss. Reads are
/// tombstone-aware: a committed remove hides the persistent row, while the archived copy may
/// still serve.
pub struct LayeredDbTx<'a, DB: Database> {
    mem_txn: MemTxn<'a>,
    persistent_snapshot: DB::TX<'a>,
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
        let db = &self.persistent_snapshot;
        let db_first = db
            .get::<T>(key)?
            .map(|value| (key.clone(), value))
            .or_else(|| db.record_prior_to::<T>(key));
        let db_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(db_first, move |(k, _)| db.record_prior_to::<T>(k)));

        let mem = &self.mem_txn;
        let mem_first = if mem.is_tombstoned::<T>(key) {
            None
        } else {
            mem.get::<T>(key)?.map(|value| (key.clone(), value))
        }
        .or_else(|| mem.record_prior_to::<T>(key));
        let mem_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(mem_first, move |(k, _)| mem.record_prior_to::<T>(k)));

        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
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
        // archived copy. A committed tombstone hides the persistent tier: the snapshot must NOT
        // resurrect the row below it.
        if let Some(val) = self.mem_txn.get::<T>(key)? {
            return Ok(Some(val));
        }
        if self.mem_txn.is_tombstoned::<T>(key) {
            return self.cold_get::<T>(key);
        }
        if let Some(val) = self.persistent_snapshot.get::<T>(key)? {
            return Ok(Some(val));
        }
        self.cold_get::<T>(key)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // A mem hit holds the raw bytes (no re-encode); a mem tombstone hides the persistent
        // tier, the archived copy may still serve.
        if let Some((removed, raw)) = self.mem_txn.get_raw::<T>(key) {
            if !removed {
                return Ok(Some(Cow::Owned(raw)));
            }
            return self.cold_raw_get::<T>(key);
        }
        match self.persistent_snapshot.raw_get::<T>(key)? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.cold_raw_get::<T>(key),
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.persistent_snapshot.iter::<T>();
        let mem_iter = self.mem_txn.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, false)
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.persistent_snapshot.raw_iter::<T>();
        let mem_iter = self.mem_txn.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, false)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = self.persistent_snapshot.skip_to::<T>(key)?;
        let mem_iter = self.mem_txn.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), false))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let db_iter = self.persistent_snapshot.raw_skip_to::<T>(key)?;
        let mem_iter = self.mem_txn.raw_skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold_raw::<T>(hot, Some(key), false))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.persistent_snapshot.reverse_iter::<T>();
        let mem_iter = self.mem_txn.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, true)
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.persistent_snapshot.reverse_raw_iter::<T>();
        let mem_iter = self.mem_txn.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
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
}

/// Messages handled by the background writer thread.
enum CommitTxn<DB: Database> {
    /// One commit's operations, applied to the persistent backend in a short-lived write
    /// transaction so they land atomically.
    Batch(Vec<Box<dyn PersistOp<DB>>>),
    /// Durability barrier: replies after every earlier batch has been applied (or failed).
    CaughtUp(oneshot::Sender<Result<(), String>>),
    Shutdown,
}

impl<DB: Database> Debug for CommitTxn<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitTxn::Batch(ops) => write!(f, "Batch({} ops)", ops.len()),
            CommitTxn::CaughtUp(_) => write!(f, "CaughtUp"),
            CommitTxn::Shutdown => write!(f, "Shutdown"),
        }
    }
}

/// In-memory cache layer over a persistent database with background writes.
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: QueueSender<DB>,
    thread: Option<Arc<JoinHandle<()>>>,
    lock_manager: Arc<WriteLockManager>,
    /// The cold tier point reads fall through to on a hot miss, when attached.
    #[cfg(feature = "cold-storage")]
    cold: Option<Arc<ColdStore>>,
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!(target: "layered_db", "LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(CommitTxn::Shutdown) {
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
        let (tx, rx) = mpsc::channel();
        let depth = Arc::new(AtomicUsize::new(0));
        let db_cloned = db.clone();
        let mem_db = MemDatabase::new();
        let mem_db_clone = mem_db.clone();
        let queue_depth = Arc::clone(&depth);
        let thread = Some(Arc::new(std::thread::spawn(move || {
            db_run(db_cloned, mem_db_clone, rx, queue_depth)
        })));
        Self {
            mem_db,
            db,
            tx: QueueSender { tx, depth },
            thread,
            lock_manager: Arc::new(WriteLockManager::default()),
            #[cfg(feature = "cold-storage")]
            cold: None,
        }
    }

    /// Open a buffered write transaction, optionally serializing reads against other writers
    /// via [`DbTxMut::lock_table`].
    ///
    /// Reads see this transaction's own writes immediately (private buffer); nothing is visible
    /// to other transactions until [`DbTxMut::commit`]. Equivalent to the [`Database::write_txn`]
    /// entry point; named after the read-then-write workflow that motivates explicit locking.
    pub fn start_write_txn(&self) -> eyre::Result<WriteTxn<'_, DB>> {
        <Self as Database>::write_txn(self)
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
        let db_first = db
            .get::<T>(key)?
            .map(|value| (key.clone(), value))
            .or_else(|| db.record_prior_to::<T>(key));
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
        = WriteTxn<'txn, DB>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.open_table::<T>()?;
        self.db.open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx {
            mem_txn: self.mem_db.read_txn()?,
            persistent_snapshot: self.db.read_txn()?,
            #[cfg(feature = "cold-storage")]
            cold: self.cold.as_deref(),
        })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(WriteTxn {
            mem_txn: self.mem_db.write_txn()?,
            pending: Vec::new(),
            persistent_snapshot: self.db.read_txn()?,
            locks: Vec::new(),
            lock_manager: Arc::clone(&self.lock_manager),
            tx: self.tx.clone(),
            #[cfg(feature = "cold-storage")]
            cold: self.cold.as_deref(),
        })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        let hot = if self.mem_db.is_tombstoned::<T>(key) {
            false
        } else {
            self.mem_db.contains_key::<T>(key)? || self.db.contains_key::<T>(key)?
        };
        if hot {
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
        } else {
            self.db.get::<T>(key)?
        };
        match hot {
            Some(val) => Ok(Some(val)),
            None => self.cold_get::<T>(key),
        }
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(PersistInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx
            .send(CommitTxn::Batch(vec![ins]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(PersistRemove::<T> { key: key.clone() });
        self.tx
            .send(CommitTxn::Batch(vec![rm]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(PersistClear::<T> { _phantom: std::marker::PhantomData });
        self.tx
            .send(CommitTxn::Batch(vec![clr]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))
    }

    fn is_empty<T: Table>(&self) -> bool {
        if !self.mem_db.is_empty::<T>() {
            return false;
        }
        // merged iterator respects tombstones
        self.iter::<T>().next().is_none()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.db.iter::<T>();
        let mem_iter = self.mem_db.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, false)
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.raw_iter::<T>();
        let mem_iter = self.mem_db.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        self.chain_cold_raw::<T>(hot, None, false)
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = self.db.skip_to::<T>(key)?;
        let mem_iter = self.mem_db.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), false))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.db.reverse_iter::<T>();
        let mem_iter = self.mem_db.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        self.chain_cold::<T>(hot, None, true)
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.reverse_raw_iter::<T>();
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
        let (tx, rx) = oneshot::channel();
        let depth_at_send = self.tx.depth();
        let started = Instant::now();
        let send_result = self.tx.send(CommitTxn::CaughtUp(tx));
        async move {
            match send_result {
                Ok(()) => match rx.await {
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
    fn sync_persist(&self) {
        let (tx, mut rx) = oneshot::channel();
        let depth_at_send = self.tx.depth();
        let started = Instant::now();
        let r = self
            .tx
            .send(CommitTxn::CaughtUp(tx))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"));

        if r.is_ok() {
            loop {
                match rx.try_recv() {
                    Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(100)),
                    Err(TryRecvError::Closed) => break,
                    Ok(Ok(())) => {
                        log_persist_latency(started.elapsed(), depth_at_send);
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::error!(target: "storage", "consensus DB sync_persist: write failed: {e}");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{CommitTxn, PersistOp, LayeredDatabase, QUEUE_HIGH_WATER_MARK, QUEUE_PACE_SLEEP};
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{
        mdbx::{MdbxConfig, MdbxDatabase},
        mem_db::MemDatabase,
        test::*,
    };
    use rayls_infrastructure_types::{Database, DbTx, DbTxMut, Table};
    use std::{path::Path, time::Duration, time::Instant};
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
        db.sync_persist();

        // An empty batch is a no-op: nothing is removed.
        db.with_write_txn(|txn| txn.evict_persistent_batch::<TestTable>(&[])).expect("empty batch");
        db.sync_persist();
        for i in 0..10u64 {
            assert!(db.contains_key::<TestTable>(&i).unwrap(), "empty batch removed key {i}");
        }

        let evicted = [1u64, 3, 5, 7];
        db.with_write_txn(|txn| txn.evict_persistent_batch::<TestTable>(&evicted))
            .expect("batch evict");
        db.sync_persist();

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

    /// Writer message that parks `db_run` until the paired sender is dropped, so a test can pin a
    /// real enqueued backlog behind a stalled writer.
    struct WriterGate(std::sync::mpsc::Receiver<()>);

    impl<DB: Database> PersistOp<DB> for WriterGate {
        fn apply(&self, _txn: &mut DB::TXMut<'_>, _mem_db: &MemDatabase) -> eyre::Result<()> {
            let _ = self.0.recv();
            Ok(())
        }
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
        let (release, gate) = std::sync::mpsc::channel::<()>();
        db.tx.send(CommitTxn::Batch(vec![Box::new(WriterGate(gate))])).expect("gate enqueue");

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
        db.sync_persist();

        // Verify all items are accessible via the layered iterator
        let count = db.iter::<TestTable>().count();
        assert_eq!(count, 101, "Expected 101 items after clear+insert, got {}", count);

        // Verify no items are incorrectly marked as deleted
        let deleted_keys = db.mem_db.get_deleted_keys::<TestTable>();
        assert!(deleted_keys.is_empty(), "Expected no deleted keys, found {}", deleted_keys.len());
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

    /// `raw_get` returns the stored value's canonical bytes across both layers, so the cold
    /// archiver can relocate a payload without a decode/re-encode round trip.
    ///
    /// Covers the disk layer (the path archival actually hits, served zero-copy from the inner
    /// mdbx), the mem overlay (a typed value re-encoded), and the tombstone/absent `None` cases.
    #[test]
    fn test_layereddb_raw_get_matches_encoded_value() {
        use rayls_infrastructure_types::encode;

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
        use rayls_infrastructure_types::decode_key;

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

    /// A write transaction's buffer is private: an uncommitted insert is invisible to every other
    /// transaction, then appears everywhere once committed.
    #[test]
    fn write_txn_buffer_is_private_until_commit() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&1, &"one".to_string()).expect("buffered insert");

        // A concurrent reader (and the handle's one-shot reads) must not see the buffer.
        let reader = db.read_txn().unwrap();
        assert_eq!(reader.get::<TestTable>(&1).unwrap(), None, "uncommitted write leaked");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "uncommitted write leaked");

        txn.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), Some("one".to_string()));
    }

    /// Dropping a write transaction discards its buffer: nothing is applied and nothing is sent
    /// to the background writer.
    #[test]
    fn write_txn_buffer_discarded_on_drop() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx(temp_dir.path());

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&1, &"one".to_string()).expect("buffered insert");
        drop(txn);

        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "dropped txn applied its buffer");
        db.sync_persist();
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "dropped txn reached the writer");
    }

    /// A write transaction reads its own uncommitted writes: insert-then-get and
    /// remove-then-get resolve against the private buffer.
    #[test]
    fn write_txn_read_after_write() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "old")]);

        let mut txn = db.write_txn().unwrap();
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), Some("old".to_string()));

        txn.insert::<TestTable>(&2, &"two".to_string()).expect("insert");
        assert_eq!(txn.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
        assert!(txn.contains_key::<TestTable>(&2).unwrap());

        txn.remove::<TestTable>(&1).expect("remove");
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), None, "buffered remove must shadow");
        assert_eq!(txn.raw_get::<TestTable>(&1).unwrap(), None, "buffered remove must shadow raw");

        // The commit keeps both effects: key 1 gone, key 2 present.
        txn.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
    }

    /// A committed remove tombstones the key for every transaction: the layered read paths must
    /// not resurrect the persistent row the tombstone hides (regression: the old snapshots did).
    #[test]
    fn committed_tombstone_shadows_persistent_everywhere() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        // Persistent layer holds the row; the remove below is never flushed, so the persistent
        // copy survives on disk for the whole test.
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "old"), (2, "two")]);

        db.remove::<TestTable>(&1).expect("committed remove (not yet durable)");

        assert_eq!(db.get::<TestTable>(&1).unwrap(), None, "handle read resurrected the row");
        assert!(!db.contains_key::<TestTable>(&1).unwrap());

        let txn = db.read_txn().unwrap();
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), None, "read txn resurrected the row");
        assert_eq!(txn.raw_get::<TestTable>(&1).unwrap(), None, "read txn raw-resurrected the row");
        assert_eq!(
            txn.iter::<TestTable>().collect::<Vec<_>>(),
            vec![(2, "two".to_string())],
            "iter resurrected the row",
        );

        let mut wt = db.write_txn().unwrap();
        assert_eq!(wt.get::<TestTable>(&1).unwrap(), None, "write txn resurrected the row");
        assert_eq!(wt.raw_get::<TestTable>(&1).unwrap(), None, "write txn raw-resurrected the row");
        // A later txn can still bring the key back.
        wt.insert::<TestTable>(&1, &"new".to_string()).expect("re-insert");
        assert_eq!(wt.get::<TestTable>(&1).unwrap(), Some("new".to_string()));
        wt.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), Some("new".to_string()));
    }

    /// A remove inside a write transaction only shadows reads once committed; outside readers
    /// keep seeing the persistent row until the tombstone lands.
    #[test]
    fn write_txn_remove_is_deferred_until_commit() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one")]);

        let mut txn = db.write_txn().unwrap();
        txn.remove::<TestTable>(&1).expect("buffered remove");
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), None, "own remove must be visible");

        let reader = db.read_txn().unwrap();
        assert_eq!(
            reader.get::<TestTable>(&1).unwrap(),
            Some("one".to_string()),
            "uncommitted remove leaked to another txn",
        );

        txn.commit().expect("commit");
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None);
    }

    /// Clearing a table inside a write transaction is deferred until commit, and is
    /// read-after-write visible to the transaction itself.
    #[test]
    fn write_txn_clear_table_is_deferred_until_commit() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (2, "two")]);

        let mut txn = db.write_txn().unwrap();
        txn.clear_table::<TestTable>().expect("buffered clear");
        assert_eq!(txn.get::<TestTable>(&1).unwrap(), None);
        assert_eq!(txn.iter::<TestTable>().count(), 0);

        let reader = db.read_txn().unwrap();
        assert_eq!(reader.iter::<TestTable>().count(), 2, "uncommitted clear leaked");

        txn.commit().expect("commit");
        db.sync_persist();
        assert!(db.is_empty::<TestTable>());
    }

    /// `lock` serializes read-then-write sequences on the same table: a second writer's `lock`
    /// blocks until the first transaction drops or commits.
    #[test]
    fn write_lock_serializes_writers() {
        let inner = MemDatabase::new();
        inner.open_table::<TestTable>().expect("open inner table");
        let db = LayeredDatabase::open(inner);
        db.open_table::<TestTable>().expect("open layered table");

        let mut t1 = db.write_txn().unwrap();
        let _g1 = t1.lock(TestTable::NAME);

        let db2 = db.clone();
        let (waited, done) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let mut t2 = db2.write_txn().unwrap();
            let _g2 = t2.lock(TestTable::NAME);
            waited.send(()).unwrap();
        });

        // The contender must be blocked on the held lock.
        assert!(
            done.recv_timeout(Duration::from_millis(200)).is_err(),
            "second writer acquired a held lock",
        );

        // Releasing the first transaction unblocks the contender.
        drop(t1);
        assert!(done.recv_timeout(Duration::from_secs(5)).is_ok());
        thread.join().expect("contender thread");
    }

    /// An `evict_persistent_batch` inside a write transaction hard-deletes from the mem overlay
    /// immediately (no tombstone, so cold fall-through stays open); only the durable removal is
    /// deferred until commit.
    #[test]
    fn write_txn_evict_batch_is_deferred_until_commit() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_mdbx_prepopulated(temp_dir.path(), &[(1, "one"), (2, "two")]);

        let mut txn = db.write_txn().unwrap();
        txn.evict_persistent_batch::<TestTable>(&[1]).expect("buffered evict");
        txn.commit().expect("commit");

        // The durable copy is still in place until the writer catches up, and the mem overlay was
        // hard-cleared, so the row is visible again through the persistent layer.
        assert_eq!(db.get::<TestTable>(&1).unwrap(), Some("one".to_string()));
        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
        db.sync_persist();
        assert_eq!(db.get::<TestTable>(&1).unwrap(), None);
        assert_eq!(db.get::<TestTable>(&2).unwrap(), Some("two".to_string()));
    }
}
