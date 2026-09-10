//! Impl db traits for mdbx.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    ffi::CString,
    marker::PhantomData,
    path::Path,
    sync::{
        // Disabled with the MDBX metrics thread (removed in #54, f243308):
        // mpsc::{self, SyncSender},
        Arc,
        OnceLock,
        RwLock,
    },
    time::{Duration, Instant},
};

use prometheus::{Histogram, IntCounter, IntGauge};
use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, KeyT,
    Table, ValueT,
};
use reth_libmdbx::{
    ffi, ffi::MDBX_dbi, Cursor, DatabaseFlags, Environment, EnvironmentFlags, Geometry,
    HandleSlowReadersReturnCode, MaxReadTransactionDuration, Mode, PageSize, SyncMode, Transaction,
    TransactionKind, WriteFlags, RO, RW,
};
use tracing::{debug, warn};

/// Reader-table telemetry for the consensus MDBX env — the signals that expose the "read
/// starvation" failure mode: concurrent reader slots held vs the cap, how often a read is
/// rejected for a full reader table (`ReadersFull`) or environment contention (`Busy`), and how
/// long read transactions stay open against the `max_read_transaction_duration` safety limit.
///
/// Sampled on the read path (no background thread, matching the post-#54 design). Each accessor
/// registers once via [`crate::layered_db::register_metric_or_unscraped`], which falls back to a
/// private unscraped registry when a second stack in one process (tests) collides on the name.
fn readers_active_gauge() -> &'static IntGauge {
    static GAUGE: OnceLock<IntGauge> = OnceLock::new();
    GAUGE.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            prometheus::register_int_gauge_with_registry!(
                "mdbx_readers_active",
                "Consensus MDBX reader slots currently bound (concurrent read transactions).",
                registry,
            )
        })
    })
}

fn readers_max_gauge() -> &'static IntGauge {
    static GAUGE: OnceLock<IntGauge> = OnceLock::new();
    GAUGE.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            prometheus::register_int_gauge_with_registry!(
                "mdbx_readers_max",
                "Consensus MDBX reader-slot cap (max_readers).",
                registry,
            )
        })
    })
}

fn readers_full_total() -> &'static IntCounter {
    static COUNTER: OnceLock<IntCounter> = OnceLock::new();
    COUNTER.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            prometheus::register_int_counter_with_registry!(
                "mdbx_readers_full_total",
                "Read transactions rejected because the MDBX reader table was full (ReadersFull).",
                registry,
            )
        })
    })
}

fn busy_total() -> &'static IntCounter {
    static COUNTER: OnceLock<IntCounter> = OnceLock::new();
    COUNTER.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            prometheus::register_int_counter_with_registry!(
                "mdbx_busy_total",
                "Read transactions rejected with MDBX_BUSY (writer/environment contention).",
                registry,
            )
        })
    })
}

fn read_txn_open_seconds() -> &'static Histogram {
    static HISTO: OnceLock<Histogram> = OnceLock::new();
    HISTO.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            prometheus::register_histogram_with_registry!(
                "mdbx_read_txn_open_seconds",
                "How long an MDBX read transaction stayed open (handle lifetime), against the read-txn duration limit.",
                // buckets out to the 30s default read-txn limit, then coarse for stragglers
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0],
                registry,
            )
        })
    })
}

/// Maximum space (in bytes) that a slow reader can hold before triggering a warning.
/// 50MB threshold for investigation purposes.
const MAX_SAFE_READER_SPACE: usize = 50 * 1024 * 1024;

/// Handle slow readers callback for MDBX.
/// Logs warnings when read transactions hold significant reclaimable space.
extern "C" fn handle_slow_readers(
    _env: *const ffi::MDBX_env,
    _txn: *const ffi::MDBX_txn,
    process_id: ffi::mdbx_pid_t,
    thread_id: ffi::mdbx_tid_t,
    read_txn_id: u64,
    gap: std::ffi::c_uint,
    space: usize,
    retry: std::ffi::c_int,
) -> HandleSlowReadersReturnCode {
    if space > MAX_SAFE_READER_SPACE {
        let space_mb = space / (1024 * 1024);
        tracing::warn!(
            target: "storage::mdbx::slow_reader",
            ?process_id,
            ?thread_id,
            ?read_txn_id,
            ?gap,
            space_mb,
            ?retry,
            "Slow reader detected - holding {}MB of reclaimable space",
            space_mb
        );
    }
    HandleSlowReadersReturnCode::ProceedWithoutKillingReader
}

/// Cached MDBX database handles.
pub type DbiCache = Arc<RwLock<HashMap<&'static str, MDBX_dbi>>>;

/// Wrapper for the libmdbx transaction.
#[derive(Debug)]
pub struct MdbxTx {
    /// Libmdbx-sys transaction.
    inner: Transaction<RO>,
    /// Cached MDBX DBIs.
    dbis: DbiCache,
    /// When the read transaction was created, for the open-duration metric.
    started: Instant,
}

impl MdbxTx {
    /// Get a table database handle, using cache if available.
    fn get_dbi<T: Table>(&self) -> eyre::Result<MDBX_dbi> {
        // Try cache first (read lock)
        if let Some(&dbi) = self.dbis.read().unwrap_or_else(|e| e.into_inner()).get(T::NAME) {
            return Ok(dbi);
        }
        // Cache miss - open db and cache the result
        let dbi = self.inner.open_db(Some(T::NAME)).map(|db| db.dbi())?;
        self.dbis.write().unwrap_or_else(|e| e.into_inner()).insert(T::NAME, dbi);
        Ok(dbi)
    }

    fn cursor<T: Table>(&self) -> eyre::Result<Cursor<RO>> {
        Ok(self.inner.cursor_with_dbi(self.get_dbi::<T>()?)?)
    }
}

impl Drop for MdbxTx {
    /// Record how long the read transaction stayed open. Exact for point reads and the
    /// `with_read_txn` closures; cursor-backed walks are timed to the handle (the cursor holds
    /// the slot longer), which the reader-occupancy gauge captures instead.
    fn drop(&mut self) {
        read_txn_open_seconds().observe(self.started.elapsed().as_secs_f64());
    }
}

fn get<T: Table, R: TransactionKind>(
    tx: &Transaction<R>,
    dbi: MDBX_dbi,
    key: &T::Key,
) -> eyre::Result<Option<T::Value>> {
    let key_buf = encode_key(key);

    let a = tx
        .get::<Vec<u8>>(dbi, &key_buf[..])
        .map(|res| res.map(|bytes| decode::<T::Value>(&bytes)))?;

    Ok(a)
}

/// Seeks `cursor` to `key` and decodes its value, or `None` if absent.
///
/// `set` (MDBX_SET) avoids re-decoding the key; the `Cow` borrows the value from the read-txn
/// mmap page (zero-copy). Seek errors other than `NotFound` are logged.
fn cursor_get<T: Table>(cursor: &mut Cursor<RO>, key: &T::Key) -> Option<T::Value> {
    let key_buf = encode_key(key);
    match cursor.set::<Cow<'_, [u8]>>(&key_buf) {
        Ok(Some(v)) => Some(decode::<T::Value>(&v)),
        Ok(None) => None,
        Err(e) => {
            if !matches!(e, reth_libmdbx::Error::NotFound) {
                tracing::warn!(
                    target: "rayls::mdbx",
                    "cursor seek error for table {}: {}",
                    T::NAME, e
                );
            }
            None
        }
    }
}

impl DbTx for MdbxTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T, RO>(&self.inner, self.get_dbi::<T>()?, key)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Borrow the value straight out of the read-txn mmap page (zero-copy on a read txn), with
        // no value decode; the caller copies it owned only if it must outlive the transaction.
        let key_buf = encode_key(key);
        let value: Option<Cow<'_, [u8]>> = self.inner.get(self.get_dbi::<T>()?, &key_buf)?;
        Ok(value)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRawIter::<'_, T::Key, T::Value, RO>::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let cursor = self.cursor::<T>()?;
        let key_bytes = encode_key(key);
        let iter = MdbxSeekedIter::new(cursor, &key_bytes)?;
        Ok(Box::new(iter))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let cursor = self.cursor::<T>()?;
        let key_bytes = encode_key(key);
        Ok(Box::new(MdbxSeekedRawIter::new(cursor, &key_bytes)?))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRevIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRevRawIter::<'_, T::Key, T::Value, RO>::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.cursor::<T>()
            .ok()?
            .last::<Vec<u8>, Vec<u8>>()
            .ok()?
            .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let mut cursor = self.cursor::<T>().ok()?;
        let key_bytes = encode_key(key);

        match cursor.set_range::<Vec<u8>, Vec<u8>>(&key_bytes) {
            Ok(Some(_)) => {
                // Found key >= target, go to previous entry
                cursor
                    .prev::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
            Ok(None) | Err(_) => {
                // No key >= target exists, return last entry in table
                cursor
                    .last::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
        }
    }

    fn disable_long_read_safety(&self) {
        debug!(target: "storage::mdbx", "disabling long read safety for database transaction");
        self.inner.disable_timeout();
    }
}

/// Wrapper for the libmdbx transaction.
#[derive(Debug)]
pub struct MdbxTxMut {
    /// Libmdbx-sys transaction.
    inner: Transaction<RW>,
    /// Cached MDBX DBIs.
    dbis: DbiCache,
}

impl MdbxTxMut {
    /// Get a table database handle, using cache if available.
    fn get_dbi<T: Table>(&self) -> eyre::Result<MDBX_dbi> {
        // Try cache first (read lock)
        if let Some(&dbi) = self.dbis.read().unwrap_or_else(|e| e.into_inner()).get(T::NAME) {
            return Ok(dbi);
        }
        // Cache miss - open db and cache the result
        let dbi = self.inner.open_db(Some(T::NAME)).map(|db| db.dbi())?;
        self.dbis.write().unwrap_or_else(|e| e.into_inner()).insert(T::NAME, dbi);
        Ok(dbi)
    }

    fn cursor<T: Table>(&self) -> eyre::Result<Cursor<RW>> {
        Ok(self.inner.cursor_with_dbi(self.get_dbi::<T>()?)?)
    }
}

impl DbTx for MdbxTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T, RW>(&self.inner, self.get_dbi::<T>()?, key)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRawIter::<'_, T::Key, T::Value, RW>::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let cursor = self.cursor::<T>()?;
        let key_bytes = encode_key(key);
        let iter = MdbxSeekedIter::new(cursor, &key_bytes)?;
        Ok(Box::new(iter))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let cursor = self.cursor::<T>()?;
        let key_bytes = encode_key(key);
        Ok(Box::new(MdbxSeekedRawIter::new(cursor, &key_bytes)?))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRevIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.cursor::<T>() {
            Ok(cursor) => Box::new(MdbxRevRawIter::<'_, T::Key, T::Value, RW>::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.cursor::<T>()
            .ok()?
            .last::<Vec<u8>, Vec<u8>>()
            .ok()?
            .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let mut cursor = self.cursor::<T>().ok()?;
        let key_bytes = encode_key(key);

        match cursor.set_range::<Vec<u8>, Vec<u8>>(&key_bytes) {
            Ok(Some(_)) => {
                // Found key >= target, go to previous entry
                cursor
                    .prev::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
            Ok(None) | Err(_) => {
                // No key >= target exists, return last entry in table
                cursor
                    .last::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
        }
    }

    fn disable_long_read_safety(&self) {
        debug!(target: "storage::mdbx", "disabling long read safety for database transaction");

        self.inner.disable_timeout();
    }
}

impl DbTxMut for MdbxTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let key_buf = encode_key(key);
        let value_buf = encode(value);
        self.inner.put(self.get_dbi::<T>()?, key_buf, value_buf, WriteFlags::UPSERT)?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let key_buf = encode_key(key);
        self.inner.del(self.get_dbi::<T>()?, key_buf, None)?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        Ok(self.inner.clear_db(self.get_dbi::<T>()?)?)
    }

    fn commit(self) -> eyre::Result<()> {
        self.inner.commit()?;
        Ok(())
    }
}

/// Wrapper for the libmdbx environment: [Environment]
#[derive(Debug, Clone)]
pub struct MdbxDatabase {
    /// Libmdbx-sys environment.
    inner: Environment,
    // Disabled: metrics-thread shutdown channel, leftover after the thread was removed in #54.
    // shutdown_tx: Arc<SyncSender<()>>,
    /// Cached MDBX DBIs.
    dbis: DbiCache,
}

impl Drop for MdbxDatabase {
    fn drop(&mut self) {
        // Disabled: the MDBX metrics thread was removed in #54 (f243308) but its shutdown channel
        // was left behind. With the channel gone there is nothing to signal; the send below hit an
        // already-closed channel and logged a spurious "sending on a closed channel" error on every
        // shutdown. Kept as a no-op Drop. Do NOT re-enable as-is, and do NOT retain the rx to
        // "fix" it: `sync_channel(0).send()` would then block forever on drop with no reader.
        // if Arc::strong_count(&self.shutdown_tx) <= 1 {
        //     tracing::info!(target: "rayls::mdbx", "MDBX Dropping, shutting down metrics thread");
        //     if let Err(e) = self.shutdown_tx.send(()) {
        //         tracing::error!(target: "rayls::mdbx", "Error while trying to send shutdown to
        // MDBX metrics thread {e}");     }
        // }
    }
}

pub const KILOBYTE: usize = 1024;
pub const MEGABYTE: usize = KILOBYTE * 1024;
pub const GIGABYTE: usize = MEGABYTE * 1024;
pub const TERABYTE: usize = GIGABYTE * 1024;

/// Rayls: Default max read transaction duration in seconds.
const DEFAULT_MAX_READ_TXN_DURATION_SECS: u64 = 30;
const DEFAULT_MAX_READERS: u32 = 256;

/// Auto-sync cadence for the `SafeNoSync` write map.
///
/// Bounds how many dirty pages accumulate before a sync, so the flush at environment close stays
/// small instead of growing with uptime. Lower also tightens the power-loss window, at the cost of
/// more frequent background syncs.
const SYNC_PERIOD: Duration = Duration::from_secs(5);

/// Configuration for MDBX database initialization.
#[derive(Debug, Clone)]
pub struct MdbxConfig {
    /// Maximum duration for read transactions. None for unbounded.
    pub max_read_transaction_duration: Option<Duration>,
    /// Maximum number of concurrent readers.
    pub max_readers: u32,
    /// Maximum database size in bytes.
    pub max_db_size: usize,
    /// Database growth step in bytes.
    pub growth_step: usize,
}

impl Default for MdbxConfig {
    fn default() -> Self {
        Self {
            max_read_transaction_duration: Some(Duration::from_secs(
                DEFAULT_MAX_READ_TXN_DURATION_SECS,
            )),
            max_readers: DEFAULT_MAX_READERS,
            max_db_size: 100 * GIGABYTE,
            growth_step: GIGABYTE,
        }
    }
}

impl MdbxConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum duration for read transactions.
    pub fn with_max_read_transaction_duration(mut self, duration: Option<Duration>) -> Self {
        self.max_read_transaction_duration = duration;
        self
    }

    /// Set the maximum number of concurrent readers.
    pub fn with_max_readers(mut self, max_readers: u32) -> Self {
        self.max_readers = max_readers;
        self
    }

    /// Set the maximum database size in bytes.
    pub fn with_max_db_size(mut self, max_db_size: usize) -> Self {
        self.max_db_size = max_db_size;
        self
    }

    /// Set the database growth step in bytes.
    pub fn with_growth_step(mut self, growth_step: usize) -> Self {
        self.growth_step = growth_step;
        self
    }
}

/// Returns the default page size that can be used in this OS.
fn default_page_size() -> usize {
    let os_page_size = page_size::get();

    // source: https://gitflic.ru/project/erthink/libmdbx/blob?file=mdbx.h#line-num-821
    let libmdbx_max_page_size = 0x10000;

    // May lead to errors if it's reduced further because of the potential size of the
    // data.
    let min_page_size = 4096;

    os_page_size.clamp(min_page_size, libmdbx_max_page_size)
}

impl MdbxDatabase {
    /// Create a new database at the specified path with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        Self::open_with_config(path, MdbxConfig::default())
    }

    /// Create a new database at the specified path with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: MdbxConfig) -> eyre::Result<Self> {
        let flags = EnvironmentFlags {
            mode: Mode::ReadWrite { sync_mode: SyncMode::SafeNoSync },
            liforeclaim: true,
            no_rdahead: true,
            coalesce: true,
            ..Default::default()
        };

        // Convert config to MDBX settings
        let max_read_txn_duration = match config.max_read_transaction_duration {
            Some(duration) => MaxReadTransactionDuration::Set(duration),
            None => MaxReadTransactionDuration::Unbounded,
        };

        tracing::info!(
            target: "rayls::mdbx",
            "Opening MDBX database with config: max_read_txn_duration={:?}, max_readers={}, max_size={}GB",
            config.max_read_transaction_duration,
            config.max_readers,
            config.max_db_size / GIGABYTE
        );

        let env = Environment::builder()
            .set_max_dbs(32)
            .set_flags(flags)
            .set_geometry(Geometry {
                size: Some(0..config.max_db_size),
                growth_step: Some(config.growth_step as isize),
                // The database never shrinks
                shrink_threshold: Some((2 * config.growth_step) as isize),
                page_size: Some(PageSize::Set(default_page_size())),
            })
            .write_map()
            .set_dp_reserve_limit(512)
            .set_txn_dp_limit(131072)
            .set_rp_augment_limit(1024 * 1024)
            // MDBX syncs lazily on the first commit past the period (see SYNC_PERIOD)
            .set_sync_period(SYNC_PERIOD)
            // Prevent writer starvation from long-held read transactions which can cause
            // consensus delays. Configurable via MdbxConfig.
            .set_max_read_transaction_duration(max_read_txn_duration)
            // Configurable concurrent readers limit for high-throughput consensus operations
            .set_max_readers(config.max_readers.into())
            // Detect slow readers that may be causing memory growth by holding pages
            .set_handle_slow_readers(handle_slow_readers)
            .open(path.as_ref())?;

        // Startup corruption detection
        // Check database integrity immediately after opening to catch corruption early
        // before node starts processing, preventing crashes during operation
        match env.stat() {
            Ok(_status) => {
                tracing::info!(target: "rayls::mdbx", "MDBX database integrity check passed");
            }
            Err(e) => {
                tracing::error!(
                    target: "rayls::mdbx",
                    "CRITICAL: MDBX database corruption detected at startup: {}",
                    e
                );
                tracing::error!(
                    target: "rayls::mdbx",
                    "Recovery instructions:\n\
                     1. Stop all nodes using this database\n\
                     2. Backup the corrupted database directory: {:?}\n\
                     3. Remove the corrupted database directory\n\
                     4. Restart the node - it will sync from network\n\
                     5. Alternative: Restore from a recent backup if available",
                    path.as_ref()
                );
                return Err(eyre::eyre!(
                    "Database corruption detected at startup. \
                     The database at {:?} is corrupted and cannot be used. \
                     See logs for recovery instructions.",
                    path.as_ref()
                ));
            }
        }

        // Metrics-thread shutdown channel disabled (thread removed in #54, f243308):
        // let (shutdown_tx, _rx) = mpsc::sync_channel::<()>(0);

        readers_max_gauge().set(config.max_readers as i64);

        Ok(MdbxDatabase {
            inner: env,
            // shutdown_tx: Arc::new(shutdown_tx),
            dbis: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Copy-compacts the environment into the `dest` file, writing only live pages.
    ///
    /// Runs against a consistent read snapshot, so the source may stay open; the copy omits
    /// freelist pages, shrinking a heavily pruned datafile to its live size. `dest` must not
    /// already exist.
    fn compact_to<P: AsRef<Path>>(&self, dest: P) -> eyre::Result<()> {
        let dest = dest.as_ref();
        let dest_c = CString::new(
            dest.to_str().ok_or_else(|| eyre::eyre!("non-UTF-8 destination path: {dest:?}"))?,
        )?;
        // SAFETY: `with_raw_env_ptr` keeps the env pointer valid for the closure's duration, and
        // `dest_c` is a NUL-terminated path that outlives the call.
        let rc = self.inner.with_raw_env_ptr(|env| unsafe {
            ffi::mdbx_env_copy(env, dest_c.as_ptr(), ffi::MDBX_CP_COMPACT)
        });
        if rc != ffi::MDBX_SUCCESS {
            eyre::bail!(
                "mdbx_env_copy to {dest:?} failed: {}",
                reth_libmdbx::Error::from_err_code(rc)
            );
        }
        Ok(())
    }
}

/// The MDBX datafile name inside an environment directory.
const MDBX_DAT: &str = "mdbx.dat";

/// Datafile sizes measured around an offline [`compact_in_place`] pass.
#[derive(Debug, Clone, Copy)]
pub struct CompactionStats {
    /// Datafile bytes before compaction.
    pub before_bytes: u64,
    /// Datafile bytes after compaction.
    pub after_bytes: u64,
    /// Named tables whose entry counts were verified identical.
    pub tables_verified: usize,
}

/// Compacts an offline MDBX environment in place, reclaiming freelist space.
///
/// Copy-compacts into a sibling temp dir, verifies every named table's entry count, then
/// atomically replaces `mdbx.dat` via `rename(2)`: a crash leaves the old or the new datafile,
/// never neither. The environment must not be open in any other process NOR by any live handle
/// in this process (every `MdbxDatabase` clone dropped, including the layered writer's).
pub fn compact_in_place(store_path: &Path, config: &MdbxConfig) -> eyre::Result<CompactionStats> {
    let dat = store_path.join(MDBX_DAT);
    let before_bytes = std::fs::metadata(&dat)?.len();

    // a leftover directory from an interrupted pass is stale; rebuild it
    let tmp_dir = store_path.join("mdbx.compact.tmp");
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }
    std::fs::create_dir_all(&tmp_dir)?;

    let source_counts = {
        let db = MdbxDatabase::open_with_config(store_path, config.clone())?;
        let counts = table_entry_counts(&db.inner)?;
        db.compact_to(tmp_dir.join(MDBX_DAT))?;
        counts
    };
    let compacted_counts = {
        let db = MdbxDatabase::open_with_config(&tmp_dir, config.clone())?;
        table_entry_counts(&db.inner)?
    };
    eyre::ensure!(
        source_counts == compacted_counts,
        "compacted copy diverges from source: {source_counts:?} vs {compacted_counts:?}",
    );

    std::fs::rename(tmp_dir.join(MDBX_DAT), &dat)?;
    // the reader-lock file belongs to the replaced datafile; a fresh open recreates it
    let _ = std::fs::remove_file(store_path.join("mdbx.lck"));
    let _ = std::fs::remove_dir_all(&tmp_dir);

    let after_bytes = std::fs::metadata(&dat)?.len();
    Ok(CompactionStats { before_bytes, after_bytes, tables_verified: source_counts.len() })
}

/// Returns every named table's entry count, keyed by table name.
fn table_entry_counts(env: &Environment) -> eyre::Result<BTreeMap<String, usize>> {
    let txn = env.begin_ro_txn()?;
    let main_dbi = txn.open_db(None)?.dbi();

    // the MAIN db's keys are the names of every named sub-database
    let mut names = Vec::new();
    let mut cursor = txn.cursor_with_dbi(main_dbi)?;
    let mut row = cursor.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((key, _)) = row {
        names.push(String::from_utf8_lossy(&key).into_owned());
        row = cursor.next::<Vec<u8>, Vec<u8>>()?;
    }

    let mut counts = BTreeMap::new();
    for name in names {
        let dbi = txn.open_db(Some(&name))?.dbi();
        counts.insert(name, txn.db_stat(dbi)?.entries());
    }
    Ok(counts)
}

impl Database for MdbxDatabase {
    type TX<'txn>
        = MdbxTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = MdbxTxMut
    where
        Self: 'txn;

    /// Open or create a table and cache its DBI.
    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        let txn = self.inner.begin_rw_txn()?;
        let db = txn.create_db(Some(T::NAME), DatabaseFlags::default())?;
        let dbi = db.dbi();
        txn.commit()?;

        // Cache the DBI for future transactions
        self.dbis.write().unwrap_or_else(|e| e.into_inner()).insert(T::NAME, dbi);
        tracing::trace!(target: "rayls::mdbx", table = T::NAME, "Cached DBI");
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        let inner = match self.inner.begin_ro_txn() {
            Ok(txn) => {
                // Sample occupancy after binding so the gauge reflects this reader too.
                if let Ok(info) = self.inner.info() {
                    readers_active_gauge().set(info.num_readers() as i64);
                }
                txn
            }
            Err(e) => {
                if matches!(e, reth_libmdbx::Error::ReadersFull) {
                    readers_full_total().inc();
                    let (active, max) = self
                        .inner
                        .info()
                        .map(|i| (i.num_readers(), i.max_readers()))
                        .unwrap_or((0, 0));
                    warn!(
                        target: "rayls::mdbx",
                        ?active,
                        ?max,
                        "MDBX read rejected: reader table full (read starvation)"
                    );
                } else if matches!(e, reth_libmdbx::Error::Busy) {
                    busy_total().inc();
                    warn!(
                        target: "rayls::mdbx",
                        "MDBX read rejected: busy (writer/environment contention)"
                    );
                }
                return Err(e.into());
            }
        };
        Ok(MdbxTx { inner, dbis: Arc::clone(&self.dbis), started: Instant::now() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MdbxTxMut { inner: self.inner.begin_rw_txn()?, dbis: Arc::clone(&self.dbis) })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.with_read_txn(|tx| Ok(tx.get::<T>(key)?.is_some()))
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.with_read_txn(|tx| tx.get::<T>(key))
    }

    /// Batch get using one cursor for the whole key set.
    fn multi_get<'a, T: Table>(
        &'a self,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        self.with_read_txn(|tx| {
            let mut cursor = tx.cursor::<T>()?;
            Ok(keys.into_iter().map(|key| cursor_get::<T>(&mut cursor, key)).collect())
        })
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
        self.iter::<T>().next().is_none()
    }

    // SAFETY: Cursor owns cloned Transaction via Arc, safe after MdbxTx drop.
    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.read_txn().and_then(|tx| tx.cursor::<T>()) {
            Ok(cursor) => Box::new(MdbxIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.read_txn().and_then(|tx| tx.cursor::<T>()) {
            Ok(cursor) => {
                Box::new(MdbxRawIter::<'_, T::Key, T::Value, RO>::new(cursor).map(into_owned_pair))
            }
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        match self.read_txn().and_then(|tx| tx.cursor::<T>()) {
            Ok(cursor) => {
                let key_bytes = encode_key(key);
                let iter = MdbxSeekedIter::new(cursor, &key_bytes)?;
                Ok(Box::new(iter))
            }
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Ok(Box::new(std::iter::empty()))
            }
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        match self.read_txn().and_then(|tx| tx.cursor::<T>()) {
            Ok(cursor) => Box::new(MdbxRevIter::new(cursor)),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        match self.read_txn().and_then(|tx| tx.cursor::<T>()) {
            Ok(cursor) => Box::new(
                MdbxRevRawIter::<'_, T::Key, T::Value, RO>::new(cursor).map(into_owned_pair),
            ),
            Err(e) => {
                tracing::error!(target: "rayls::mdbx", table = T::NAME, "Failed to create iterator: {e}");
                Box::new(std::iter::empty())
            }
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        let tx = self.read_txn().ok()?;
        let mut cursor = tx.cursor::<T>().ok()?;
        let key_bytes = encode_key(key);

        match cursor.set_range::<Vec<u8>, Vec<u8>>(&key_bytes) {
            Ok(Some(_)) => {
                // Found key >= target, go to previous entry
                cursor
                    .prev::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
            Ok(None) | Err(_) => {
                // No key >= target exists, return last entry in table
                cursor
                    .last::<Vec<u8>, Vec<u8>>()
                    .ok()?
                    .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
            }
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.read_txn()
            .ok()?
            .cursor::<T>()
            .ok()?
            .last::<Vec<u8>, Vec<u8>>()
            .ok()?
            .map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))
    }
}

/// Forward iterator over MDBX key-value pairs.
#[derive(Debug)]
pub struct MdbxIter<K, V, TK = RO>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K: KeyT, V: ValueT, TK: TransactionKind> MdbxIter<K, V, TK> {
    fn new(cursor: Cursor<TK>) -> Self {
        Self { cursor, _key: PhantomData, _val: PhantomData }
    }
}

impl<K, V, TK> Iterator for MdbxIter<K, V, TK>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(result) = self.cursor.next::<Vec<u8>, Vec<u8>>() {
            result.map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)))
        } else {
            None
        }
    }
}

/// Copy a borrowed raw key/value pair into owned bytes, severing it from the
/// cursor's mmap.
///
/// The `Database::raw_iter`/`reverse_raw_iter` variants own their read
/// transaction *inside* the returned iterator, so a borrow into the mmap would
/// dangle if the boxed iterator were dropped while a yielded item is still
/// held. The `DbTx` variants don't need this — their transaction outlives the
/// iterator — so they yield the borrow directly.
fn into_owned_pair<'i>((k, v): (Cow<'_, [u8]>, Cow<'_, [u8]>)) -> (Cow<'i, [u8]>, Cow<'i, [u8]>) {
    (Cow::Owned(k.into_owned()), Cow::Owned(v.into_owned()))
}

/// Forward iterator over MDBX key-value pairs returning raw bytes.
///
/// `'i` is the lifetime for which the borrowed bytes are valid. The cursor owns
/// an `Arc` clone of the read transaction, so its mmap pages stay mapped until
/// the transaction ends; on a read txn `Cow` values come back `Borrowed`
/// straight into those pages (zero-copy). For the `DbTx` path the owning
/// transaction is held for `'i` regardless of this iterator, so the borrow is
/// sound; the self-referential `Database` path materializes the bytes to owned
/// before yielding (see `MdbxDatabase::raw_iter`).
#[derive(Debug)]
pub struct MdbxRawIter<'i, K, V, TK = RO>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
    _tx: PhantomData<&'i ()>,
}

impl<'i, K: KeyT, V: ValueT, TK: TransactionKind> MdbxRawIter<'i, K, V, TK> {
    fn new(cursor: Cursor<TK>) -> Self {
        Self { cursor, _key: PhantomData, _val: PhantomData, _tx: PhantomData }
    }
}

impl<'i, K, V, TK> Iterator for MdbxRawIter<'i, K, V, TK>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    type Item = (Cow<'i, [u8]>, Cow<'i, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next::<Cow<'i, [u8]>, Cow<'i, [u8]>>().ok().flatten()
    }
}

/// Forward iterator seeded at the first key >= the given target via MDBX `set_range`.
#[derive(Debug)]
pub struct MdbxSeekedIter<K, V, TK = RO>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    /// First row returned by `set_range`, yielded on the first call to `next`.
    pending: Option<(Vec<u8>, Vec<u8>)>,
    /// True once end-of-table reached; prevents cursor wraparound.
    exhausted: bool,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K: KeyT, V: ValueT, TK: TransactionKind> MdbxSeekedIter<K, V, TK> {
    fn new(mut cursor: Cursor<TK>, key_bytes: &[u8]) -> eyre::Result<Self> {
        let pending = cursor.set_range::<Vec<u8>, Vec<u8>>(key_bytes)?;
        Ok(Self {
            exhausted: pending.is_none(),
            cursor,
            pending,
            _key: PhantomData,
            _val: PhantomData,
        })
    }
}

impl<K, V, TK> Iterator for MdbxSeekedIter<K, V, TK>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        if let Some((k, v)) = self.pending.take() {
            return Some((decode_key::<K>(&k), decode::<V>(&v)));
        }
        match self.cursor.next::<Vec<u8>, Vec<u8>>() {
            Ok(Some((k, v))) => Some((decode_key::<K>(&k), decode::<V>(&v))),
            _ => {
                self.exhausted = true;
                None
            }
        }
    }
}

/// Forward raw iterator seeded at the first key >= the given target via MDBX `set_range`.
///
/// The raw twin of [`MdbxSeekedIter`]: same seek semantics, but yields the stored bytes without
/// decoding, borrowed from the read transaction's mmap pages (see [`MdbxRawIter`] for the
/// lifetime argument).
#[derive(Debug)]
pub struct MdbxSeekedRawIter<'i, TK = RO>
where
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    /// First row returned by `set_range`, yielded on the first call to `next`.
    pending: Option<(Cow<'i, [u8]>, Cow<'i, [u8]>)>,
    /// True once end-of-table reached; prevents cursor wraparound.
    exhausted: bool,
}

impl<'i, TK: TransactionKind> MdbxSeekedRawIter<'i, TK> {
    fn new(mut cursor: Cursor<TK>, key_bytes: &[u8]) -> eyre::Result<Self> {
        let pending = cursor.set_range::<Cow<'i, [u8]>, Cow<'i, [u8]>>(key_bytes)?;
        Ok(Self { exhausted: pending.is_none(), cursor, pending })
    }
}

impl<'i, TK: TransactionKind> Iterator for MdbxSeekedRawIter<'i, TK> {
    type Item = (Cow<'i, [u8]>, Cow<'i, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        if let Some(row) = self.pending.take() {
            return Some(row);
        }
        match self.cursor.next::<Cow<'i, [u8]>, Cow<'i, [u8]>>() {
            Ok(Some(row)) => Some(row),
            _ => {
                self.exhausted = true;
                None
            }
        }
    }
}

/// Reverse iterator over MDBX key-value pairs.
#[derive(Debug)]
pub struct MdbxRevIter<K, V, TK = RO>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    started: bool,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K: KeyT, V: ValueT, TK: TransactionKind> MdbxRevIter<K, V, TK> {
    fn new(cursor: Cursor<TK>) -> Self {
        Self { cursor, started: false, _key: PhantomData, _val: PhantomData }
    }
}

impl<K, V, TK> Iterator for MdbxRevIter<K, V, TK>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            return self
                .cursor
                .last::<Vec<u8>, Vec<u8>>()
                .ok()?
                .map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)));
        }
        if let Ok(result) = self.cursor.prev::<Vec<u8>, Vec<u8>>() {
            result.map(|(k, v)| (decode_key::<K>(&k), decode::<V>(&v)))
        } else {
            None
        }
    }
}

/// Reverse iterator over MDBX key-value pairs returning raw bytes.
///
/// See [`MdbxRawIter`] for the meaning of `'i` and the zero-copy borrow rules.
#[derive(Debug)]
pub struct MdbxRevRawIter<'i, K, V, TK = RO>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    cursor: Cursor<TK>,
    started: bool,
    _key: PhantomData<K>,
    _val: PhantomData<V>,
    _tx: PhantomData<&'i ()>,
}

impl<'i, K: KeyT, V: ValueT, TK: TransactionKind> MdbxRevRawIter<'i, K, V, TK> {
    fn new(cursor: Cursor<TK>) -> Self {
        Self { cursor, started: false, _key: PhantomData, _val: PhantomData, _tx: PhantomData }
    }
}

impl<'i, K, V, TK> Iterator for MdbxRevRawIter<'i, K, V, TK>
where
    K: KeyT,
    V: ValueT,
    TK: TransactionKind,
{
    type Item = (Cow<'i, [u8]>, Cow<'i, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            return self.cursor.last::<Cow<'i, [u8]>, Cow<'i, [u8]>>().ok().flatten();
        }
        self.cursor.prev::<Cow<'i, [u8]>, Cow<'i, [u8]>>().ok().flatten()
    }
}

#[cfg(test)]
mod test {
    use super::{compact_in_place, MdbxConfig, MdbxDatabase};
    use crate::{layered_db::LayeredDatabase, test::*};
    use rayls_infrastructure_types::{Database as _, DbTxMut as _};
    use std::path::Path;
    use tempfile::tempdir;

    fn open_db(path: &Path) -> MdbxDatabase {
        let db = MdbxDatabase::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    /// Seeds a table, prunes most of it, then compacts in place: the survivors must be
    /// intact through a fresh open and the datafile must shrink to its live size.
    #[test]
    fn compact_in_place_preserves_rows_and_shrinks_file() {
        const ROWS: u64 = 4096;
        const SURVIVORS: u64 = 96;

        let temp_dir = tempdir().expect("failed to create temp dir");
        // the compacted datafile is sized in growth_step granules, so shrinkage is only
        // observable when the step is far below the seeded data volume
        let cfg = MdbxConfig::default().with_growth_step(super::MEGABYTE);
        let blob = "x".repeat(4096);

        let open = |path: &Path| {
            let db = MdbxDatabase::open_with_config(path, cfg.clone()).expect("open database");
            db.open_table::<TestTable>().expect("open table");
            db
        };

        {
            let db = open(temp_dir.path());
            db.with_write_txn(|txn| {
                for n in 0..ROWS {
                    txn.insert::<TestTable>(&n, &blob)?;
                }
                Ok(())
            })
            .expect("seed rows");
            db.with_write_txn(|txn| {
                for n in 0..ROWS - SURVIVORS {
                    txn.remove::<TestTable>(&n)?;
                }
                Ok(())
            })
            .expect("prune rows");
        }

        let stats = compact_in_place(temp_dir.path(), &cfg).expect("compact in place");
        assert!(stats.tables_verified >= 1, "no tables verified: {stats:?}");
        assert!(stats.after_bytes < stats.before_bytes / 4, "datafile did not shrink: {stats:?}",);

        // survivors intact, pruned rows gone, through a fresh open of the swapped file
        let db = open(temp_dir.path());
        for n in ROWS - SURVIVORS..ROWS {
            assert_eq!(db.get::<TestTable>(&n).expect("get survivor"), Some(blob.clone()));
        }
        assert_eq!(db.get::<TestTable>(&0).expect("get pruned"), None);
    }

    /// The offline migrate-then-compact sequence: once every `LayeredDatabase` clone drops (which
    /// joins the writer thread and closes its env handle), an in-place compaction of the same
    /// directory must succeed with the rows intact.
    #[test]
    fn compact_in_place_after_layered_stack_drop() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let cfg = MdbxConfig::default().with_growth_step(super::MEGABYTE);

        {
            let db = LayeredDatabase::open(
                MdbxDatabase::open_with_config(temp_dir.path(), cfg.clone())
                    .expect("open database"),
            );
            db.open_table::<TestTable>().expect("open table");
            for n in 0..8u64 {
                db.insert::<TestTable>(&n, &format!("row-{n}")).expect("insert");
            }
            db.sync_persist();
        }

        let stats = compact_in_place(temp_dir.path(), &cfg).expect("compact after stack drop");
        assert!(stats.tables_verified >= 1, "no tables verified: {stats:?}");

        let db = MdbxDatabase::open_with_config(temp_dir.path(), cfg).expect("reopen");
        db.open_table::<TestTable>().expect("open table");
        for n in 0..8u64 {
            assert_eq!(db.get::<TestTable>(&n).expect("get row"), Some(format!("row-{n}")));
        }
    }

    #[test]
    fn test_mdbx_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_contains_key(db)
    }

    #[test]
    fn test_mdbx_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_get(db)
    }

    #[test]
    fn test_mdbx_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_get(db)
    }

    #[test]
    fn test_mdbx_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_skip(db)
    }

    #[test]
    fn test_mdbx_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_skip_to_previous_simple(db)
    }

    #[test]
    fn test_mdbx_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter_skip_to_previous_gap(db)
    }

    #[test]
    fn test_mdbx_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_remove(db)
    }

    #[test]
    fn test_mdbx_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter(db)
    }

    #[test]
    fn test_mdbx_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_iter_reverse(db)
    }

    #[test]
    fn test_mdbx_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_clear(db)
    }

    #[test]
    fn test_mdbx_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_is_empty(db)
    }

    #[test]
    fn test_mdbx_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_insert(db)
    }

    #[test]
    fn test_mdbx_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        test_multi_remove(db)
    }

    #[test]
    fn test_mdbx_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db_simp_bench(db, "MDBX");
    }

    /// Reproduction of the validator "MDBX read starvation" incident.
    ///
    /// The consensus MDBX env caps concurrent readers (256 by default). Once that many read
    /// transactions are held open concurrently (an inbound-request burst / long walks), any
    /// further read is rejected with `ReadersFull`. Point reads surface that as an error, but the
    /// scan/lookup helpers in this module swallow the same error and report an EMPTY table,
    /// silently losing every row.
    ///
    /// The test requests a small reader cap, then holds the effective number of read txns open at
    /// once to saturate the table and show the exhaustion. libmdbx sizes the reader table from the
    /// page-rounded lockfile (a requested cap of 4 comes back larger), so the effective cap is read
    /// back from `env.info()` rather than assumed.
    #[test]
    fn test_readers_full_exhausts_readers_and_scan_paths_silently_fail() {
        const ROWS: u64 = 16;

        let temp_dir = tempdir().expect("failed to create temp dir");
        // Request a small reader table; the 30s read-txn timeout is kept (the test finishes long
        // before the monitor thread could reset any reader).
        let cfg = MdbxConfig::default().with_max_readers(4);
        let db = MdbxDatabase::open_with_config(temp_dir.path(), cfg).expect("open mdbx");
        db.open_table::<TestTable>().expect("open table");
        for i in 0..ROWS {
            db.insert::<TestTable>(&i, &i.to_string()).expect("seed row");
        }

        // Baseline: with no readers held, reads work and the table is not empty.
        let key: u64 = 0;
        assert_eq!(db.get::<TestTable>(&key).expect("baseline get"), Some("0".to_string()));
        assert_eq!(db.iter::<TestTable>().count(), ROWS as usize, "baseline row count");

        // The effective reader-table size, as libmdbx actually provisioned it (page-rounded).
        let cap = db.inner.info().expect("env info").max_readers();
        assert!(cap >= 4, "expected at least 4 reader slots, got {cap}");

        // Saturate the reader table: hold `cap` read txns open at once (one per concurrent
        // reader). With no sticky-thread config each read txn binds its own slot, so `cap` live
        // txns consume every slot.
        let holders =
            (0..cap).map(|_| db.read_txn().expect("bind reader slot")).collect::<Vec<_>>();

        // --- The reader table is now full. ---
        // Point reads surface the exhaustion as an error (the "failed to read" in the logs).
        let err = db.read_txn().expect_err("reader table is full");
        assert!(
            err.downcast_ref::<reth_libmdbx::Error>()
                .is_some_and(|e| matches!(e, reth_libmdbx::Error::ReadersFull)),
            "read_txn must fail with ReadersFull, got: {err:?}",
        );
        assert!(db.get::<TestTable>(&key).is_err(), "get must fail while the reader table is full");
        assert!(
            db.contains_key::<TestTable>(&key).is_err(),
            "contains_key must fail while the reader table is full"
        );
        assert!(
            db.multi_get::<TestTable>(std::iter::once(&key)).is_err(),
            "multi_get must fail while the reader table is full"
        );

        // The scan/lookup paths swallow the SAME error and report an EMPTY table even though
        // ROWS rows exist. This is the data-loss / wrong-decision failure mode.
        assert_eq!(db.iter::<TestTable>().count(), 0, "iter must be silently truncated to empty");
        assert_eq!(
            db.reverse_iter::<TestTable>().next(),
            None,
            "reverse_iter must be silently empty"
        );
        assert_eq!(db.last_record::<TestTable>(), None, "last_record must be silently None");
        assert_eq!(
            db.record_prior_to::<TestTable>(&key),
            None,
            "record_prior_to must be silently None"
        );
        assert!(db.is_empty::<TestTable>(), "is_empty must be silently true");

        // Release the readers; the slot table drains and reads work again (exhaustion, not
        // corruption).
        drop(holders);

        assert_eq!(db.get::<TestTable>(&key).expect("post-release get"), Some("0".to_string()));
        assert_eq!(
            db.iter::<TestTable>().count(),
            ROWS as usize,
            "all rows visible again after release"
        );
    }
}
