//! Impl db traits for mdbx.

use std::{
    borrow::Cow,
    collections::HashMap,
    marker::PhantomData,
    path::Path,
    sync::{
        // Disabled with the MDBX metrics thread (removed in #54, f243308):
        // mpsc::{self, SyncSender},
        Arc,
        RwLock,
    },
    time::Duration,
};

use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, KeyT,
    Table, ValueT,
};
use reth_libmdbx::{
    ffi, ffi::MDBX_dbi, Cursor, DatabaseFlags, Environment, EnvironmentFlags, Geometry,
    HandleSlowReadersReturnCode, MaxReadTransactionDuration, Mode, PageSize, SyncMode, Transaction,
    TransactionKind, WriteFlags, RO, RW,
};
use tracing::warn;

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
        warn!(target: "storage::mdbx", "Disabling long read safety for database transaction");
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
        warn!(target: "storage::mdbx", "Disabling long read safety for database transaction");

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

        Ok(MdbxDatabase {
            inner: env,
            // shutdown_tx: Arc::new(shutdown_tx),
            dbis: Arc::new(RwLock::new(HashMap::new())),
        })
    }
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
        Ok(MdbxTx { inner: self.inner.begin_ro_txn()?, dbis: Arc::clone(&self.dbis) })
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
    use super::MdbxDatabase;
    use crate::test::*;
    use rayls_infrastructure_types::Database as _;
    use std::path::Path;
    use tempfile::tempdir;

    fn open_db(path: &Path) -> MdbxDatabase {
        let db = MdbxDatabase::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        db
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
}
