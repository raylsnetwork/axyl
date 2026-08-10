# Database Refactoring — Detailed Implementation Plan

## 1. Problem Statement

The current storage layer has three fundamental problems:

1. **MemDB transactions are not real transactions.** `MemDbTx`/`MemDbTxMut` hold `RwLockGuard`s directly on the shared store. Writes go immediately to shared memory — there is no private buffer, no rollback, and `commit()` is a no-op.

2. **LayeredDB does not orchestrate — it bypasses.** `LayeredDbTxMut::insert()` writes directly to mem_db's shared store AND sends each operation individually to the background thread. There is no write buffer, no batching, and no atomicity guarantee.

3. **Tombstone visibility is immediate, not transactional.** A `remove()` call marks a key as tombstoned in the shared store, instantly visible to all readers. There is no concept of a transaction seeing its own writes before commit.

## 2. Target Architecture

### 2.1 Layer Responsibilities

```
Application
    │
    ▼
WriteTxn (per-txn state)
    ├── MemTxn          → buffered writes, Arc<StoreType> snapshot
    ├── PersistBuffer   → batched ops for background thread
    ├── LockManager     → opt-in table-level locks
    ├── PersistentTx    → MDBX/ReDB read snapshot
    └── ColdTx (opt.)   → cold-tier read fallthrough (cold-storage feature)
    │
    ▼
LayeredDatabase (orchestrator)
    ├── MemDatabase     → in-memory cache (shared store)
    ├── DB: MdbxDatabase|ReDB → persistent backend
    ├── ColdStore (opt.) → append-only nippy-jar cold archive (cold-storage feature)
    ├── BackgroundThread → applies persist_buffer to persistent DB
    └── WriteLockManager → per-table mutexes
```

**Read resolution order (3-tier):**
```
buffer (private) → mem shared store → persistent DB → cold archive
```

**Cold tier properties:**
- Feature-gated behind `#[cfg(feature = "cold-storage")]`
- Append-only: `remove`, `clear_table`, `evict_persistent_batch` never target cold
- Only `ConsensusBlocks` (by block number) and `Batches` (via `ColdBatchLocations` index) are archived
- `without_cold()` returns a hot-only view for the archival producer

### 2.2 Layer Interaction Flow

```
Application:
    txn = db.start_write_txn()     → creates WriteTxn
    txn.lock("table")?             → acquires table lock
    txn.begin()                    → opens persistent snapshot + cold snapshot (opt.)
    txn.insert(key, value)         → writes to mem buffer + persist buffer
    txn.get(key)                   → buffer → mem → persistent → cold (3-tier fallthrough)
    txn.commit()                   →
        1. MemTxn commits (merge mem buffer → shared store)
        2. PersistBuffer sent to bg thread as single batch
        3. Locks released
    db.persist()                   → waits for bg thread to flush
```

### 2.3 Visibility Rules

- **Uncommitted writes** are invisible to all other txns (private buffers only)
- **Committed writes** are immediately visible to all readers (mem buffer merged into shared store)
- **Each txn sees its own buffered writes** (read-after-write consistency via buffer lookup)
- **Reads follow priority**: buffer → mem shared store → persistent snapshot

---

## 2.4 Per-File Imports

Every code block below assumes these imports at the top of the respective file.
Omitting these is the #1 reason compilation fails — they are listed here for
zero-context implementation.

### `write_buffer.rs`
```rust
use std::collections::HashMap;
use rayls_infrastructure_types::{decode, encode, encode_key, Table};
```

### `write_lock.rs`
```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
```

### `mem_db.rs`
```rust
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    marker::PhantomData,
    sync::mpsc::{self, SyncSender},
    sync::Arc,
    time::Duration,
};
use parking_lot::RwLock;
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode, decode_key, encode, encode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};
```

### `layered_db.rs`
```rust
use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::Debug,
    future::Future,
    iter::Peekable,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        mpsc::{self, Receiver, SendError, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use crate::mem_db::{MemDatabase, StoreType};
use crate::write_buffer::{WriteBuffer, WriteOp, PersistOp, PersistInsert, PersistRemove, PersistRemoveBatch, PersistClear};
use crate::write_lock::{WriteLockManager, WriteLockGuard};
use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};
use rayls_infrastructure_types::{
    decode_key, encode, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table,
};
use tokio::sync::oneshot::{self, error::TryRecvError};

#[cfg(feature = "cold-storage")]
use crate::{
    cold::{ColdStore, ColdTx},
    tables::ColdBatchLocations,
};
```

---

## 3. Data Structures

### 3.1 Write Buffer (NEW: `write_buffer.rs`)

```rust
/// A single write operation stored in a transaction's private buffer.
enum WriteOp {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    ClearTable,
}

/// Per-transaction write buffer.
/// LayeredDB owns this buffer — it is NOT delegated to MemDB.
///
/// Note: `StoreType` must be made `pub` in `mem_db.rs` so this module can import it.
/// `StoreType` uses `parking_lot::RwLock` (not std sync RwLock).
/// The guard API differs: `parking_lot` returns guards directly (no Result/unwrap).
struct WriteBuffer {
    /// Operations grouped by table name.
    ops: HashMap<&'static str, Vec<WriteOp>>,
}

impl WriteBuffer {
    fn new() -> Self {
        Self {
            ops: HashMap::new(),
        }
    }

    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::Insert {
            key: encode_key(key),
            value: encode(value),
        });
    }

    fn remove<T: Table>(&mut self, key: &T::Key) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::Remove {
            key: encode_key(key),
        });
    }

    fn clear_table<T: Table>(&mut self) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::ClearTable);
    }

    /// Hard-delete: remove ALL ops for the key from buffer, no tombstone.
    fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        if let Some(ops) = self.ops.get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            ops.retain(|op| {
                match op {
                    WriteOp::Insert { key: k } | WriteOp::Remove { key: k } => k != &key_bytes,
                    WriteOp::ClearTable => true,
                }
            });
        }
    }

    /// Iterate all Insert ops for a table (for merging into iter).
    fn iter_inserts<T: Table>(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.ops.get(T::NAME)
            .into_iter()
            .flat_map(|ops| {
                // Walk in reverse; last insert wins per key
                let mut seen = Vec::new();
                let mut result = Vec::new();
                for op in ops.iter().rev() {
                    if let WriteOp::Insert { key, value } = op {
                        if !seen.contains(key) {
                            seen.push(key.clone());
                            result.push((key.clone(), value.clone()));
                        }
                    }
                }
                result
            })
            .collect()
    }

    /// Iterate all Remove keys for a table (for filtering in iter).
    fn iter_removes<T: Table>(&self) -> Vec<Vec<u8>> {
        self.ops.get(T::NAME)
            .into_iter()
            .flat_map(|ops| {
                let mut seen = Vec::new();
                let mut result = Vec::new();
                for op in ops.iter().rev() {
                    match op {
                        WriteOp::Remove { key } => {
                            if !seen.contains(key) {
                                seen.push(key.clone());
                                result.push(key.clone());
                            }
                        }
                        WriteOp::ClearTable => {
                            // ClearTable marks everything as removed; return all known keys
                            return ops.iter()
                                .filter_map(|o| match o {
                                    WriteOp::Insert { key } | WriteOp::Remove { key } => Some(key.clone()),
                                    _ => None,
                                })
                                .collect();
                        }
                        _ => {}
                    }
                }
                result
            })
            .collect()
    }

    /// Check buffer for a value (handles insert/remove/clear precedence).
    fn get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        if let Some(ops) = self.ops.get(T::NAME) {
            let key_bytes = encode_key(key);
            // Walk ops in reverse; last matching write wins
            for op in ops.iter().rev() {
                match op {
                    WriteOp::Insert { key: k, value } if k == &key_bytes => return Some(decode(value)),
                    WriteOp::Remove { key: k } if k == &key_bytes => return None,
                    WriteOp::ClearTable => return None,
                    _ => continue,
                }
            }
        }
        None
    }

    /// Check buffer for raw bytes.
    fn raw_get<T: Table>(&self, key: &T::Key) -> Option<Vec<u8>> {
        if let Some(ops) = self.ops.get(T::NAME) {
            let key_bytes = encode_key(key);
            for op in ops.iter().rev() {
                match op {
                    WriteOp::Insert { key: k, value } if k == &key_bytes => return Some(value.clone()),
                    WriteOp::Remove { key: k } if k == &key_bytes => return None,
                    WriteOp::ClearTable => return None,
                    _ => continue,
                }
            }
        }
        None
    }

    /// Check buffer for tombstone.
    fn is_tombstoned<T: Table>(&self, key: &T::Key) -> Option<bool> {
        if let Some(ops) = self.ops.get(T::NAME) {
            let key_bytes = encode_key(key);
            for op in ops.iter().rev() {
                match op {
                    WriteOp::Insert { key: k } if k == &key_bytes => return Some(false),
                    WriteOp::Remove { key: k } if k == &key_bytes => return Some(true),
                    WriteOp::ClearTable => return Some(true),
                    _ => continue,
                }
            }
        }
        None
    }

    /// Apply all buffered operations to the shared store on commit.
    /// Note: `store` is `&parking_lot::RwLock<StoreType>` — `.write()` returns guard directly.
    fn apply_to_mem(self, store: &parking_lot::RwLock<StoreType>) {
        let mut shared = store.write();
        for (table_name, ops) in self.ops {
            let table = shared.entry(table_name).or_insert_with(BTreeMap::new);
            for op in ops {
                match op {
                    WriteOp::Insert { key, value } => {
                        table.insert(key, (false, value));
                    }
                    WriteOp::Remove { key } => {
                        // Tombstone: mark for deletion. If key doesn't exist
                        // in mem, insert a tombstone (key may exist only in persistent).
                        table.entry(key).or_insert((true, Vec::new()));
                    }
                    WriteOp::ClearTable => {
                        for value in table.values_mut() {
                            value.0 = true;  // mark tombstoned
                        }
                    }
                }
            }
        }
    }
}
```

### 3.2 MemTxn (REPLACES `MemDbTx` + `MemDbTxMut`)

```rust
/// Unified transaction type for MemDatabase.
/// Holds an Arc clone of the shared store (snapshot isolation) + private buffer.
/// Note: `store` uses `parking_lot::RwLock` (not std sync RwLock).
struct MemTxn<'a> {
    store: Arc<parking_lot::RwLock<StoreType>>,
    buffer: WriteBuffer,
}

impl MemTxn<'_> {
    /// Read-only: check buffer first, then fall through to shared store.
    fn get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        // 1. Check buffer (read-after-write consistency)
        if let Some(val) = self.buffer_get::<T>(key) {
            return Some(val);
        }
        // 2. Fall through to shared store
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
    fn buffer_get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        self.buffer.get::<T>(key)
    }

    /// Raw check only the private buffer (for layered raw_get).
    fn buffer_raw_get<T: Table>(&self, key: &T::Key) -> Option<Vec<u8>> {
        self.buffer.raw_get::<T>(key)
    }

    /// Check only the private buffer for tombstone status.
    /// Returns `Some(true)` if tombstoned, `Some(false)` if inserted, `None` if not in buffer.
    /// Used by `WriteTxn::get` to implement correct read-after-write semantics.
    fn buffer_is_tombstoned<T: Table>(&self, key: &T::Key) -> Option<bool> {
        self.buffer.is_tombstoned::<T>(key)
    }

    /// Get raw value from shared store without tombstone check (for layered raw_get).
    /// Returns (is_tombstoned, raw_bytes) so the caller avoids decode/encode round-trip.
    fn get_raw<T: Table>(&self, key: &T::Key) -> Option<(bool, Vec<u8>)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map(|(removed, val_bytes)| {
                (*removed, val_bytes.clone())
            });
        }
        None
    }

    fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
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

    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) {
        self.buffer.insert(key, value);
    }

    fn remove<T: Table>(&mut self, key: &T::Key) {
        self.buffer.remove(key);
    }

    fn clear_table<T: Table>(&mut self) {
        self.buffer.clear_table::<T>();
    }

    /// Hard-delete a key from the mem overlay without tombstoning.
    /// Used by the cold archival producer: a tombstone would shadow the cold fall-through,
    /// so the archived row must be hard-deleted from the hot tier.
    fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        // Remove from buffer if present
        self.buffer.hard_delete::<T>(key);
        // Remove from shared store
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            table.remove(&key_bytes);
        }
    }

    /// Commit: merge buffer into shared store.
    /// Consumes `self`; the `store` Arc outlives the txn, so the shared store persists.
    fn commit(self) {
        let Self { store, buffer } = self;
        buffer.apply_to_mem(&store);
    }
}

impl DbTx for MemTxn<'_> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(self.get::<T>(key))
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Avoid decode/encode round-trip by returning raw bytes directly
        if let Some(bytes) = self.buffer_raw_get::<T>(key) {
            return Ok(Some(Cow::Owned(bytes)));
        }
        if let Some((removed, raw)) = self.get_raw::<T>(key) {
            if !removed {
                return Ok(Some(Cow::Owned(raw)));
            }
        }
        Ok(None)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // Merge buffer + shared store, respect tombstones
        let items: Vec<_> = {
            let shared = self.store.read();
            let table = shared.get(T::NAME);
            // Collect from shared store
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = table
                .into_iter()
                .flat_map(|t| t.iter()
                    .filter(|(_, (removed, _))| !**removed)
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                )
                .collect();
            // Apply buffer overrides (inserts/updates)
            for (key, value) in self.buffer.iter_inserts::<T>() {
                // Insert or update
                entries.retain(|(k, _)| k != &key);
                entries.push((key, value));
            }
            // Remove tombstoned entries
            for key in self.buffer.iter_removes::<T>() {
                entries.retain(|(k, _)| k != &key);
            }
            entries
        };
        Box::new(items.into_iter().map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v))))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        // Merge buffer + shared store as raw bytes, respect tombstones
        let items: Vec<_> = {
            let shared = self.store.read();
            let table = shared.get(T::NAME);
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = table
                .into_iter()
                .flat_map(|t| t.iter()
                    .filter(|(_, (removed, _))| !**removed)
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                )
                .collect();
            // Apply buffer inserts
            for (key, value) in self.buffer.iter_inserts::<T>() {
                entries.retain(|(k, _)| k != &key);
                entries.push((key, value));
            }
            // Apply buffer removes
            for key in self.buffer.iter_removes::<T>() {
                entries.retain(|(k, _)| k != &key);
            }
            entries
        };
        Box::new(items.into_iter().map(|(k, v)| (Cow::Owned(k), Cow::Owned(v))))
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let key_bytes = encode_key(key);
        let items: Vec<_> = {
            let shared = self.store.read();
            let table = shared.get(T::NAME);
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = table
                .into_iter()
                .flat_map(|t| t.iter()
                    .filter(|(_, (removed, _))| !**removed)
                    .skip_while(|(k, _)| **k < key_bytes)
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                )
                .collect();
            for (key, value) in self.buffer.iter_inserts::<T>() {
                entries.retain(|(k, _)| k != &key);
                if key >= key_bytes {
                    entries.push((key, value));
                }
            }
            for key in self.buffer.iter_removes::<T>() {
                entries.retain(|(k, _)| k != &key);
            }
            entries
        };
        Ok(Box::new(items.into_iter().map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v)))))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let items: Vec<_> = {
            let shared = self.store.read();
            let table = shared.get(T::NAME);
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = table
                .into_iter()
                .flat_map(|t| t.iter()
                    .filter(|(_, (removed, _))| !**removed)
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                )
                .collect();
            for (key, value) in self.buffer.iter_inserts::<T>() {
                entries.retain(|(k, _)| k != &key);
                entries.push((key, value));
            }
            for key in self.buffer.iter_removes::<T>() {
                entries.retain(|(k, _)| k != &key);
            }
            entries
        };
        Box::new(items.into_iter().rev().map(|(k, v)| (decode_key::<T::Key>(&k), decode::<T::Value>(&v))))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let items: Vec<_> = {
            let shared = self.store.read();
            let table = shared.get(T::NAME);
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = table
                .into_iter()
                .flat_map(|t| t.iter()
                    .filter(|(_, (removed, _))| !**removed)
                    .map(|(k, (_, v))| (k.clone(), v.clone()))
                )
                .collect();
            for (key, value) in self.buffer.iter_inserts::<T>() {
                entries.retain(|(k, _)| k != &key);
                entries.push((key, value));
            }
            for key in self.buffer.iter_removes::<T>() {
                entries.retain(|(k, _)| k != &key);
            }
            entries
        };
        Box::new(items.into_iter().rev().map(|(k, v)| (Cow::Owned(k), Cow::Owned(v))))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }
}

impl DbTxMut for MemTxn<'_> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.remove(key);
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.clear_table::<T>();
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.commit();
        Ok(())
    }
}
```

### 3.3 WriteLockManager (NEW: `write_lock.rs`)

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

struct WriteLockManager {
    /// Per-table mutexes stored as Arc so the guard can keep the mutex alive.
    locks: RwLock<HashMap<&'static str, Arc<std::sync::Mutex<()>>>>,
}

impl WriteLockManager {
    fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire a write lock on the given table.
    /// Blocks until the lock is available, then returns a guard that holds it.
    fn lock(&self, table_name: &'static str) -> WriteLockGuard {
        // First try to get existing mutex under read lock
        let mutex = {
            let locks = self.locks.read().unwrap();
            locks.get(table_name).cloned()
        };

        let mutex = match mutex {
            Some(m) => m,
            None => {
                let mut locks = self.locks.write().unwrap();
                locks.entry(table_name)
                    .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
                    .clone()
            }
        };

        // Lock the mutex; the guard keeps the lock held until dropped.
        // Safety: the Arc keeps the Mutex alive for the guard's lifetime.
        // We use a raw pointer to avoid the MutexGuard<'_> lifetime being tied
        // to the temporary `&*mutex` deref. The flag tracks lock state.
        let ptr = Arc::as_ptr(&mutex);
        let guard = unsafe { &*ptr }.lock().unwrap();
        // The lock is now held; we suppress the guard's drop by forgetting it.
        // The lock will be released in WriteLockGuard::drop().
        std::mem::forget(guard);

        WriteLockGuard {
            _mutex: mutex,
            locked: true,
        }
    }
}

/// Guard that holds a table-level write lock.
/// Dropping the guard releases the mutex.
pub struct WriteLockGuard {
    /// Keeps the mutex alive for the guard's lifetime.
    _mutex: Arc<std::sync::Mutex<()>>,
    /// Whether the mutex is currently locked by this guard.
    locked: bool,
}

impl WriteLockGuard {
    /// No-op lock for backends without locking (e.g. MemDatabase direct writes).
    pub fn no_op() -> Self {
        Self {
            _mutex: Arc::new(std::sync::Mutex::new(())),
            locked: false,
        }
    }
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        if self.locked {
            // Release the mutex lock. Safety: we hold the Arc, so the Mutex is alive.
            // We locked it in WriteLockManager::lock() and haven't unlocked it yet.
            let ptr = Arc::as_ptr(&self._mutex);
            unsafe { &*ptr }.unlock();
            self.locked = false;
        }
    }
}
```

### 3.4 WriteTxn (REPLACES `LayeredDbTxMut`)

```rust
/// Write transaction for LayeredDatabase.
/// Orchestrates MemTxn + persistent snapshot + write buffer + cold fallthrough.
struct WriteTxn<'a, DB: Database> {
    /// Mem layer transaction (buffered).
    mem_txn: MemTxn<'a>,

    /// Persistent read snapshot (for reads to fall through to).
    persistent_snapshot: DB::TX<'a>,

    /// Operations buffered for the background thread.
    persist_buffer: Vec<Box<dyn PersistOp<DB>>>,

    /// Lock manager reference for acquiring table locks.
    lock_manager: Arc<WriteLockManager>,

    /// Locks held by this transaction (released on drop).
    locks: Vec<WriteLockGuard>,

    /// Channel to send committed buffer to background thread.
    tx: QueueSender<DB>,

    /// Cold tier read fallthrough (feature-gated, append-only reads only).
    #[cfg(feature = "cold-storage")]
    cold: Option<&'a ColdStore>,
}

impl<'a, DB: Database> Debug for WriteTxn<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteTxn")
    }
}

impl<'a, DB: Database> WriteTxn<'a, DB> {
    /// Opens a cold read transaction over the attached tier.
    #[cfg(feature = "cold-storage")]
    fn cold_tx(&self) -> Option<ColdTx<'_>> {
        let cold = self.cold?;
        // Resolve ColdBatchLocations from the hot snapshot.
        Some(ColdTx::new(cold, |digest| {
            self.persistent_snapshot.get::<ColdBatchLocations>(digest)
        }))
    }

    /// Serves `key` from cold after a hot miss.
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

    /// Serves `key`'s raw jar bytes from cold after a hot miss.
    #[cfg(feature = "cold-storage")]
    fn cold_raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        match self.cold_tx() {
            Some(tx) => Ok(tx.raw_get::<T>(key)?.map(|b| Cow::Owned(b.into_owned()))),
            None => Ok(None),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_raw_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(None)
    }

    /// Chains the cold-ordered stream beneath `hot` iterator.
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

    /// Chains the cold-ordered raw stream beneath `hot` iterator.
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

    /// Acquire a write lock on the given table before read-then-write.
    pub fn lock(&mut self, table_name: &'static str) -> eyre::Result<()> {
        let guard = self.lock_manager.lock(table_name);
        self.locks.push(guard);
        Ok(())
    }

    /// Iterates key-descending from the largest key at or below `key`.
    pub fn reverse_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_first = self.persistent_snapshot
            .get::<T>(key)?
            .map(|value| (key.clone(), value))
            .or_else(|| self.persistent_snapshot.record_prior_to::<T>(key));
        let db_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(db_first, move |(k, _)| self.persistent_snapshot.record_prior_to::<T>(k)));

        let mem_first = if self.mem_txn.is_tombstoned::<T>(key) {
            None
        } else {
            self.mem_txn.get::<T>(key).map(|value| (key.clone(), value))
                .or_else(|| self.mem_txn.record_prior_to::<T>(key))
        };
        let mem_iter: DBIter<'_, T> =
            Box::new(std::iter::successors(mem_first, move |(k, _)| self.mem_txn.record_prior_to::<T>(k)));

        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBIter<'_, T> =
            Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold::<T>(hot, Some(key), true))
    }
}

impl<'a, DB: Database> DbTx for WriteTxn<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // 1. Check mem buffer (read-after-write consistency)
        //    If tombstoned in buffer, stop here — write txn must not see lower tiers.
        //    If not in buffer at all, fall through to lower tiers.
        match self.mem_txn.buffer_is_tombstoned::<T>(key) {
            Some(true) => return Ok(None),           // tombstoned — hide from lower tiers
            Some(false) => return Ok(self.mem_txn.buffer_get::<T>(key)), // inserted
            None => {}                               // not in buffer — fall through
        }
        // 2. Check mem_db shared store (tombstone-aware)
        if let Some(val) = self.mem_txn.get::<T>(key) {
            return Ok(Some(val));
        }
        // 3. Check persistent snapshot
        if let Some(val) = self.persistent_snapshot.get::<T>(key)? {
            return Ok(Some(val));
        }
        // 4. Check cold tier (feature-gated fallthrough)
        self.cold_get::<T>(key)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Same tombstone-aware logic as get: buffer tombstone blocks fallthrough
        match self.mem_txn.buffer_is_tombstoned::<T>(key) {
            Some(true) => return Ok(None),
            Some(false) => {
                if let Some(bytes) = self.mem_txn.buffer_raw_get::<T>(key) {
                    return Ok(Some(Cow::Owned(bytes)));
                }
            }
            None => {}
        }
        if let Some((_, raw)) = self.mem_txn.get_raw::<T>(key) {
            return Ok(Some(Cow::Owned(raw)));
        }
        match self.persistent_snapshot.raw_get::<T>(key)? {
            Some(bytes) => return Ok(Some(bytes)),
            None => {}
        }
        self.cold_raw_get::<T>(key)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // Merge: mem buffer → mem shared store → persistent → cold
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
        // ORCHESTRATE: write to BOTH buffers (cold is append-only, never targeted)
        self.mem_txn.insert(key, value);
        self.persist_buffer.push(Box::new(PersistInsert { key: key.clone(), value: value.clone() }));
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.mem_txn.remove(key);
        self.persist_buffer.push(Box::new(PersistRemove { key: key.clone() }));
        Ok(())
    }

    fn evict_persistent_batch<T: Table>(&mut self, keys: &[T::Key]) -> eyre::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // Hard-delete from mem (no tombstone: would shadow cold fallthrough)
        for key in keys {
            self.mem_txn.hard_delete::<T>(key);
        }
        // Single batch message to persistent backend
        self.persist_buffer.push(Box::new(PersistRemoveBatch { keys: keys.to_vec() }));
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.mem_txn.clear_table::<T>();
        self.persist_buffer.push(Box::new(PersistClear::<T> { _phantom: PhantomData }));
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        // Destructure to consume self; fields are accessed in order of use.
        let Self {
            mem_txn,
            persistent_snapshot: _,   // dropped, releases persistent read snapshot
            persist_buffer,
            lock_manager: _,          // dropped after locks vec is consumed
            locks,                    // dropped after commit, releasing table locks
            tx,
            #[cfg(feature = "cold-storage")]
            cold: _,
        } = self;

        // 1. Commit mem layer: merge buffer → shared store
        mem_txn.commit();

        // 2. Send persistent buffer to background thread as single batch
        tx.send(CommitTxn::Batch(persist_buffer))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;

        // 3. `locks` dropped here, releasing table write locks
        // 4. `persistent_snapshot` already dropped above, releasing persistent read txn
        Ok(())
    }
}

/// **Drop behavior:** `WriteTxn` does NOT need a custom `Drop` impl.
/// Rust's field drop order handles cleanup automatically:
/// - `locks: Vec<WriteLockGuard>` — each guard's `Drop` releases its mutex
/// - `persistent_snapshot` — drops the persistent read txn
/// - `mem_txn.buffer` — discarded (uncommitted writes are lost, as expected)
/// - `mem_txn.store` — Arc decrement (shared store lives on)
```

**Cold storage integration notes:**
- Reads follow 4-tier fallthrough: `buffer → mem → persistent → cold`
- Writes never target cold (append-only)
- `evict_persistent_batch` hard-deletes from mem (no tombstone) so cold fallthrough isn't shadowed
- All cold methods are `#[cfg(feature = "cold-storage")]` — stubbed to no-op when the feature is off
- `merge_cold` / `merge_cold_raw` functions chain the cold iterator beneath the hot merged iterator
- The cold auxiliary index (`ColdBatchLocations`) is resolved from the hot persistent snapshot

### 3.5 Read Transaction (REPLACES `LayeredDbTx`)

```rust
/// Read transaction for LayeredDatabase.
/// Merges mem_db snapshot + persistent snapshot + cold tier (feature-gated).
struct LayeredDbTx<'a, DB: Database> {
    mem_txn: MemTxn<'a>,
    persistent_snapshot: DB::TX<'a>,
    /// Cold tier read fallthrough (feature-gated, append-only reads).
    #[cfg(feature = "cold-storage")]
    cold: Option<&'a ColdStore>,
}

impl<'a, DB: Database> Debug for LayeredDbTx<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTx")
    }
}

impl<'a, DB: Database> LayeredDbTx<'a, DB> {
    /// Opens a cold read transaction over the attached tier.
    #[cfg(feature = "cold-storage")]
    fn cold_tx(&self) -> Option<ColdTx<'_>> {
        let cold = self.cold?;
        Some(ColdTx::new(cold, |digest| {
            self.persistent_snapshot.get::<ColdBatchLocations>(digest)
        }))
    }

    #[cfg(feature = "cold-storage")]
    fn cold_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        match self.cold_tx() {
            Some(tx) => tx.get::<T>(key),
            None => Ok(None),
        }
    }

    #[cfg(not(feature = "cold-storage"))]
    fn cold_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(None)
    }

    /// Serves `key`'s raw jar bytes from cold after a hot miss.
    #[cfg(feature = "cold-storage")]
    fn cold_raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        match self.cold_tx() {
            Some(tx) => Ok(tx.raw_get::<T>(key)?.map(|b| Cow::Owned(b.into_owned()))),
            None => Ok(None),
        }
    }

    /// Cold storage compiled out: a hot miss is final.
    #[cfg(not(feature = "cold-storage"))]
    fn cold_raw_get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(None)
    }

    #[cfg(feature = "cold-storage")]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> DBIter<'i, T> {
        merge_cold::<T>(self.cold_tx(), hot, from, reverse)
    }

    #[cfg(not(feature = "cold-storage"))]
    fn chain_cold<'i, T: Table>(
        &'i self,
        hot: DBIter<'i, T>,
        _from: Option<&T::Key>,
        _reverse: bool,
    ) -> DBIter<'i, T> {
        hot
    }

    /// Chains the cold-ordered raw stream beneath `hot` iterator.
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

impl<'a, DB: Database> DbTx for LayeredDbTx<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // 1. Check mem shared store (tombstone-aware)
        if !self.mem_txn.is_tombstoned::<T>(key) {
            if let Some(val) = self.mem_txn.get::<T>(key) {
                return Ok(Some(val));
            }
        }
        // 2. Check persistent snapshot
        if let Some(val) = self.persistent_snapshot.get::<T>(key)? {
            return Ok(Some(val));
        }
        // 3. Check cold tier (feature-gated fallthrough)
        self.cold_get::<T>(key)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        // Same 3-tier fallthrough with cold
        if self.mem_txn.is_tombstoned::<T>(key) {
            return self.cold_raw_get::<T>(key);
        }
        if let Some((_, raw)) = self.mem_txn.get_raw::<T>(key) {
            return Ok(Some(Cow::Owned(raw)));
        }
        match self.persistent_snapshot.raw_get::<T>(key)? {
            Some(bytes) => return Ok(Some(bytes)),
            None => {}
        }
        self.cold_raw_get::<T>(key)
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

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let db_iter = self.persistent_snapshot.raw_skip_to::<T>(key)?;
        let mem_iter = self.mem_txn.raw_skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_txn.is_tombstoned::<T>(k));
        let hot: DBRawIter<'_> =
            Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned));
        Ok(self.chain_cold_raw::<T>(hot, Some(key), false))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }
}
```

### 3.6 Background Thread — Simplified

```rust
/// Message sent to background thread.
/// Replaces the current StartTxn/CommitTxn refcount protocol + per-op messages.
enum CommitTxn<DB: Database> {
    /// Batch of trait-object operations from a committed WriteTxn.
    Batch(Vec<Box<dyn PersistOp<DB>>>),
    /// Durability barrier: "have you flushed everything?"
    CaughtUp(oneshot::Sender<Result<(), String>>),
    /// Shutdown signal.
    Shutdown,
}

/// A sender that tracks the writer queue's depth and applies
/// soft backpressure when the queue exceeds `QUEUE_HIGH_WATER_MARK`.
struct QueueSender<DB: Database> {
    tx: Sender<CommitTxn<DB>>,
    depth: Arc<AtomicUsize>,
}

impl<DB: Database> Clone for QueueSender<DB> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone(), depth: Arc::clone(&self.depth) }
    }
}

const QUEUE_HIGH_WATER_MARK: usize = 10_000;
const QUEUE_PACE_SLEEP: Duration = Duration::from_millis(1);

impl<DB: Database> QueueSender<DB> {
    fn send(&self, msg: CommitTxn<DB>) -> Result<(), SendError<CommitTxn<DB>>> {
        // Data-plane messages pace when queue is deep; control messages never pace.
        if !matches!(msg, CommitTxn::CaughtUp(_) | CommitTxn::Shutdown)
            && self.depth.load(AtomicOrdering::Relaxed) > QUEUE_HIGH_WATER_MARK
        {
            std::thread::sleep(QUEUE_PACE_SLEEP);
        }
        self.depth.fetch_add(1, AtomicOrdering::Relaxed);
        self.tx.send(msg)
    }
}

fn db_run<DB: Database>(
    db: DB,
    mem_db: MemDatabase,
    rx: Receiver<CommitTxn<DB>>,
    queue_depth: Arc<AtomicUsize>,
) {
    let mut pending_write_error: Option<String> = None;
    let mut last_compact = Instant::now();

    while let Ok(msg) = rx.recv() {
        match msg {
            CommitTxn::Batch(ops) => {
                match db.write_txn() {
                    Ok(mut txn) => {
                        for op in ops {
                            if let Err(e) = op.apply(&mut txn) {
                                tracing::error!(target: "layered_db_runner", "DB TXN op failed: {e}");
                                pending_write_error = Some(format!("op: {e}"));
                            }
                        }
                        if let Err(e) = txn.commit() {
                            tracing::error!(target: "layered_db_runner", "DB TXN Commit: {e}");
                            pending_write_error = Some(format!("commit: {e}"));
                        }
                    }
                    Err(e) => {
                        tracing::error!(target: "layered_db_runner", "DB ERROR getting write txn (background): {e}");
                        pending_write_error = Some(format!("txn: {e}"));
                    }
                }
            }
            CommitTxn::CaughtUp(tx) => {
                let reply = pending_write_error.take().map_err(|e| e).unwrap_or_else(|| Ok(()));
                let _ = tx.send(reply);
            }
            CommitTxn::Shutdown => break,
        }
        // Decrement queue depth on every dequeue.
        queue_depth.fetch_sub(1, AtomicOrdering::Relaxed);

        if last_compact.elapsed() > Duration::from_secs(86_400) {
            last_compact = Instant::now();
            if let Err(e) = db.compact() {
                tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB (background): {e}");
            }
        }
    }
    tracing::info!(target: "layered_db_runner", "Layered DB thread Shutdown complete");
}
```

**QueueSender backpressure:** Retained from current architecture. Data-plane messages (Batch) pace with a 1ms sleep when queue depth exceeds 10,000. Control messages (CaughtUp, Shutdown) never pace. This prevents a slow inner DB from causing a multi-minute `persist` drain at epoch boundaries.

### 3.7 Background Thread Dispatch — Trait Objects (Option A)

The current code uses trait objects (`InsertTrait`, `RemoveTrait`, `ClearTrait`) to handle typed operations in the bg thread. We keep the same pattern, but batch per-txn instead of per-op.

```rust
/// Per-operation trait for bg thread dispatch.
///
/// Object-safe: the concrete `Table` type is erased into the box, and `DB` is the
/// persistent backend type. The `TXMut` lifetime is resolved at each `apply` call site.
trait PersistOp<DB: Database>: Send + 'static {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

struct PersistInsert<T: Table> {
    key: T::Key,
    value: T::Value,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistInsert<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }
}

struct PersistRemove<T: Table> {
    key: T::Key,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistRemove<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.remove::<T>(&self.key)
    }
}

/// Hard-delete batch: removes keys from the persistent backend without tombstoning mem.
/// Used by the cold archival producer to prune hot rows that have been archived.
struct PersistRemoveBatch<T: Table> {
    keys: Vec<T::Key>,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistRemoveBatch<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        for key in &self.keys {
            txn.remove::<T>(key)?;
        }
        Ok(())
    }
}

struct PersistClear<T: Table> {
    _phantom: PhantomData<T>,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistClear<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }
}
```

**Why trait objects:** The `WriteTxn` knows the concrete `Table` type at compile time for each operation. Boxing into `PersistOp<DB>` preserves type safety while allowing heterogeneous batches. The bg thread simply iterates the batch and calls `apply()` on each — no string dispatch, no registry, no runtime type mismatch.

### 3.9 LayeredDatabase — Simplified

```rust
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: QueueSender<DB>,
    lock_manager: Arc<WriteLockManager>,
    thread: Option<Arc<JoinHandle<()>>>,
    /// Cold tier point reads fall through to on hot miss (feature-gated).
    #[cfg(feature = "cold-storage")]
    cold: Option<Arc<ColdStore>>,
}

impl<DB: Database> LayeredDatabase<DB> {
    pub fn open(db: DB) -> Self {
        let (tx, rx) = mpsc::channel();
        let depth = Arc::new(AtomicUsize::new(0));
        let db_cloned = db.clone();
        let mem_db = MemDatabase::new();
        let mem_db_clone = mem_db.clone();
        let lock_manager = Arc::new(WriteLockManager::new());
        let queue_depth = Arc::clone(&depth);
        let thread = Some(Arc::new(std::thread::spawn(move || {
            db_run(db_cloned, mem_db_clone, rx, queue_depth)
        })));
        Self {
            mem_db,
            db,
            tx: QueueSender { tx, depth },
            lock_manager,
            thread,
            #[cfg(feature = "cold-storage")]
            cold: None,
        }
    }

    /// Attaches the cold tier for point-read fallthrough on hot miss.
    #[cfg(feature = "cold-storage")]
    pub fn with_cold(mut self, cold: Arc<ColdStore>) -> Self {
        self.cold = Some(cold);
        self
    }

    /// Returns a hot-only view (no cold layer) for the archival producer.
    /// Ensures "is this row still hot?" is answered by the hot tier alone.
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

    /// Check cold tier for key existence on hot miss.
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

    /// Start a buffered write transaction with lock support.
    /// Inherent method only — NOT on the Database trait.
    pub fn start_write_txn(&self) -> eyre::Result<WriteTxn<'_, DB>> {
        Ok(WriteTxn {
            mem_txn: self.mem_db.write_txn()?,
            persistent_snapshot: self.db.read_txn()?,
            persist_buffer: Vec::new(),
            lock_manager: Arc::clone(&self.lock_manager),
            locks: Vec::new(),
            tx: self.tx.clone(),
            #[cfg(feature = "cold-storage")]
            cold: self.cold.as_deref(),
        })
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn> = LayeredDbTx<'txn, DB> where Self: 'txn;
    type TXMut<'txn> = WriteTxn<'txn, DB> where Self: 'txn;

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

    // All other Database methods delegate to mem_db + db with cold fallthrough for reads
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        // 3-tier: mem → persistent → cold
        let hot = if self.mem_db.is_tombstoned::<T>(key) {
            false
        } else {
            self.mem_db.contains_key::<T>(key)? || self.db.contains_key::<T>(key)?
        };
        if hot {
            return Ok(true);
        }
        // Cold fallthrough on hot miss
        self.cold_has::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // 3-tier: mem → persistent → cold
        let hot = if self.mem_db.is_tombstoned::<T>(key) {
            None
        } else if let Some(val) = self.mem_db.get::<T>(key)? {
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
        self.tx.send(CommitTxn::Batch(vec![Box::new(PersistInsert::<T> { key: key.clone(), value: value.clone() })]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        self.tx.send(CommitTxn::Batch(vec![Box::new(PersistRemove::<T> { key: key.clone() })]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        self.tx.send(CommitTxn::Batch(vec![Box::new(PersistClear::<T> { _phantom: PhantomData })]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.iter::<T>().next().is_none()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let txn = self.read_txn().unwrap(); // direct methods open a snapshot
        txn.iter::<T>()
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let txn = self.read_txn().unwrap();
        txn.raw_iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let txn = self.read_txn()?;
        txn.skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let txn = self.read_txn().unwrap();
        txn.reverse_iter::<T>()
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let txn = self.read_txn().unwrap();
        txn.reverse_raw_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    // multi_get, with_read_txn, with_write_txn: use Database trait default impls
    // compact: use Database trait default impl (no-op)
    // persist, sync_persist: see Appendix A.10 (identical to current, but use CommitTxn::CaughtUp)
}
```

### 3.10 MemDatabase — Simplified

```rust
#[derive(Clone, Debug)]
pub struct MemDatabase {
    store: Arc<parking_lot::RwLock<StoreType>>,
    metrics: Arc<parking_lot::RwLock<MemDBMetrics>>,
    shutdown_tx: Arc<SyncSender<()>>,
}

impl MemDatabase {
    pub fn new() -> Self {
        let store: Arc<parking_lot::RwLock<StoreType>> = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let metrics = Arc::new(parking_lot::RwLock::new(MemDBMetrics::default()));
        let (shutdown_tx, rx) = mpsc::sync_channel::<()>(0);

        // Metrics thread unchanged
        let store_cloned = Arc::clone(&store);
        let metrics_cloned = metrics.clone();
        std::thread::spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(30)) {
                let read_guard = store_cloned.read();
                for (key, table) in read_guard.iter() {
                    if let Some(m) = metrics_cloned.read().table_counts.get(key) {
                        m.set(table.len().try_into().unwrap_or(-1));
                    }
                }
            }
        });

        Self { store, metrics, shutdown_tx: Arc::new(shutdown_tx) }
    }

    pub fn read_txn(&self) -> eyre::Result<MemTxn<'_>> {
        Ok(MemTxn {
            store: Arc::clone(&self.store),
            buffer: WriteBuffer::new(),
        })
    }

    pub fn write_txn(&self) -> eyre::Result<MemTxn<'_>> {
        Ok(MemTxn {
            store: Arc::clone(&self.store),
            buffer: WriteBuffer::new(),
        })
    }

    // Direct (non-txn) methods still work — they write to shared store immediately
    pub fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let key_bytes = encode_key(key);
        let value_bytes = encode(value);
        self.store.write().entry(T::NAME).or_insert_with(BTreeMap::new)
            .insert(key_bytes, (false, value_bytes));
        Ok(())
    }

    pub fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        let key_bytes = encode_key(key);
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            if let Some(value) = table.get_mut(&key_bytes) {
                value.0 = true;  // tombstone
            } else {
                table.insert(key_bytes, (true, Vec::new()));  // tombstone for persistent-only keys
            }
        }
        Ok(())
    }

    pub fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            for value in table.values_mut() {
                value.0 = true;
            }
        }
        Ok(())
    }

    // Helper methods for eviction and cold archival
    pub fn delete_removed<T: Table>(&self, key: &T::Key, require_marked: bool) -> eyre::Result<()> {
        if let Some(table) = self.store.write().get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, _)) = table.get(&key_bytes) {
                if !*removed && require_marked {
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

    /// Gets the value without checking the tombstone flag.
    /// Returns (is_tombstoned, decoded_value). Used by the layered read path which checks
    /// tombstones separately. Also exposed for debug/test inspection of raw mem state.
    pub fn get_no_marked_check<T: Table>(&self, key: &T::Key) -> Option<(bool, T::Value)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map(|(removed, val_bytes)| {
                (*removed, decode(val_bytes))
            });
        }
        None
    }

    pub fn is_tombstoned<T: Table>(&self, key: &T::Key) -> bool {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            return table.get(&key_bytes).map_or(false, |(removed, _)| *removed);
        }
        false
    }

    /// Gets the value with the marking for delete flag.
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

    /// Returns keys marked for deletion in the given table.
    pub fn get_deleted_keys<T: Table>(&self) -> std::collections::HashSet<Vec<u8>> {
        if let Some(table) = self.store.read().get(T::NAME) {
            table.iter().filter(|(_, (removed, _))| *removed).map(|(k, _)| k.clone()).collect()
        } else {
            std::collections::HashSet::new()
        }
    }
}

impl Database for MemDatabase {
    type TX<'txn> = MemTxn<'txn> where Self: 'txn;
    type TXMut<'txn> = MemTxn<'txn> where Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.store.write().insert(T::NAME, BTreeMap::new());
        // Metrics registration unchanged
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        self.read_txn()
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.write_txn()
    }

    // Delegate to direct (non-txn) methods — same as current implementation
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.get::<T>(key)?.is_some())
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if self.is_tombstoned::<T>(key) {
            return Ok(None);
        }
        Ok(self.store.read().get(T::NAME).and_then(|table| {
            let key_bytes = encode_key(key);
            table.get(&key_bytes).and_then(|(removed, val_bytes)| {
                if !*removed { Some(decode(val_bytes)) } else { None }
            })
        }))
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.insert::<T>(key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.remove::<T>(key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.clear_table::<T>()
    }

    fn is_empty<T: Table>(&self) -> bool {
        if let Some(table) = self.store.read().get(T::NAME) {
            for (removed, _) in table.values() {
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
            Box::new(std::iter::empty())
        }
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                .collect();
            Box::new(items.into_iter())
        } else {
            Box::new(std::iter::empty())
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
            Ok(Box::new(std::iter::empty()))
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
            Box::new(std::iter::empty())
        }
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let items: Vec<_> = table
                .iter()
                .rev()
                .filter(|(_, (removed, _))| !*removed)
                .map(|(k, (_, v))| (Cow::Owned(k.clone()), Cow::Owned(v.clone())))
                .collect();
            Box::new(items.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }
}
```

---

## 4. Trait Changes

### 4.1 `Database` trait — No `start_write_txn` on trait

`start_write_txn()` is an **inherent method on `LayeredDatabase` only**, NOT on the `Database` trait. The `Database` trait retains `write_txn()` for the general write transaction API. Callers that need explicit locking or buffered writes use `LayeredDatabase::start_write_txn()` directly.

**Rationale:** `start_write_txn` returns `WriteTxn<'_, DB>` which is specific to `LayeredDatabase<DB>`. Other backends (MdbxDatabase, ReDB, MemDatabase) don't have this concept. Adding it to the trait would require a second associated type or a default no-op impl, adding complexity for no benefit.

**Migration pattern for callers:**
```rust
// Simple writes (no locking needed) — use existing trait method:
let txn = db.write_txn()?;  // trait method, works on any Database

// Read-then-write with locks — cast to concrete type and use inherent method:
let txn = layered_db.start_write_txn()?;  // inherent on LayeredDatabase only
txn.lock(T::NAME)?;
// ... reads and writes ...
txn.commit()?;
```

**No changes needed to the `Database` trait itself** for `start_write_txn` or `lock_table`.

### 4.1a `with_write_txn()` Closure Pattern

The `Database` trait's default `with_write_txn()` method calls `write_txn()`, which still works for simple fire-and-forget writes. For callers that need **buffered writes with read-after-write consistency** or **explicit locking**, use `start_write_txn()` directly:

```rust
// Simple writes: closure pattern still works (uses write_txn internally)
db.with_write_txn(|txn| {
    txn.insert::<MyTable>(&key, &value)?;
    Ok(())
})?;

// Buffered + locked: explicit pattern
let mut txn = layered_db.start_write_txn()?;
 txn.lock(MyTable::NAME)?;
if txn.get::<MyTable>(&key)?.is_none() {
    txn.insert::<MyTable>(&key, &value)?;
}
txn.commit()?;
```

The `with_write_txn` default impl on the `Database` trait requires no changes. It delegates to `write_txn()`, which each backend implements.

### 4.2 `Database` trait — Remove `disable_long_read_safety()`

Remove from `DbTx` trait and all implementations. 4 call sites to delete.

### 4.3 `ReadTimeout` enum — Remove

Remove from `database_traits.rs`, all imports, and all usages. 5 call sites to delete.

---

## 5. File-by-File Changes

### 5.1 `crates/infrastructure/types/src/database_traits.rs`

**Changes:**
- Remove `ReadTimeout` enum (lines 27-41)
- Remove `disable_long_read_safety()` from `DbTx` trait (line 75)
- No other changes to the `Database` trait (see section 4.1: `start_write_txn` is inherent on `LayeredDatabase` only)

**Before:**
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTimeout { Enforced, Exempt }

pub trait DbTx {
    // ...
    fn disable_long_read_safety(&self);
}
```

**After:**
```rust
pub trait DbTx {
    // ...
    // disable_long_read_safety removed
}
```

### 5.2 `crates/infrastructure/storage/src/lib.rs`

**Changes:**
- Remove `ReadTimeout` re-export (line 27)
- Add `write_buffer` and `write_lock` module declarations
- Update `open_db()` to use new `start_write_txn()` if needed

**Before:**
```rust
pub use rayls_infrastructure_types::{error::StoreError, ReadTimeout};
pub mod layered_db;
pub mod mdbx;
pub mod mem_db;
pub mod redb;
```

**After:**
```rust
pub use rayls_infrastructure_types::error::StoreError;
mod write_buffer;
mod write_lock;
pub mod layered_db;
pub mod mdbx;
pub mod mem_db;
pub mod redb;
```

### 5.3 `crates/infrastructure/storage/src/mem_db.rs` — REWRITE

**Changes:**
- Remove `MemDbTx` struct
- Remove `MemDbTxMut` struct
- Add `MemTxn` struct (unified buffered transaction)
- Add `WriteBuffer` struct
- Update `MemDatabase` to return `MemTxn` from `read_txn()`/`write_txn()`
- Keep `MemDatabase` direct methods (`insert`, `remove`, `clear_table`, `delete_removed`, `is_tombstoned`, `get_deleted_keys`, `get_marked`)
- Update `Database` impl to use `MemTxn`

**What stays the same:**
- `StoreType`, `StoreTableType`, `StoreTableValueType` type aliases
- `MemDatabase` struct + `new()` + metrics thread
- `open_default_tables()` integration
- `delete_removed()`, `is_tombstoned()`, `get_deleted_keys()`, `get_marked()` helper methods
- All tests

**What changes:**
- `MemDbTx` → `MemTxn` (Arc clone + buffer, not lock guard)
- `MemDbTxMut` → merged into `MemTxn`
- `commit()` is no longer a no-op — it merges buffer into shared store
- `iter()` no longer panics — it merges buffer + store

### 5.4 `crates/infrastructure/storage/src/write_buffer.rs` — NEW

**Contents:**
- `WriteOp` enum (Insert, Remove, ClearTable)
- `WriteBuffer` struct (HashMap<table_name, Vec<WriteOp>>)
- `PersistInsert<T>`, `PersistRemove<T>`, `PersistClear<T>` trait objects for bg thread
- Helper methods: `insert()`, `remove()`, `clear_table()`, `apply_to_mem()`

### 5.5 `crates/infrastructure/storage/src/write_lock.rs` — NEW

**Contents:**
- `WriteLockManager` struct (HashMap<table_name, Mutex<()>>)
- `WriteLockGuard` struct (holds MutexGuard, releases on drop)
- `WriteLockGuard::no_op()` for backends without locking

### 5.6 `crates/infrastructure/storage/src/layered_db.rs` — REWRITE

**Changes:**
- Remove `LayeredDbTx` (replace with new version using `MemTxn` + cold fallthrough)
- Remove `LayeredDbTxMut` (replace with `WriteTxn`)
- Remove `DBMessage` enum (replace with `CommitTxn`)
- Remove `InsertTrait`, `RemoveTrait`, `ClearTrait` (replace with `PersistOp`)
- Remove `KeyValueInsert`, `KeyRemove`, `KeyRemoveBatch`, `ClearTable` structs (replace with `PersistInsert<T>`, etc.)
- Rewrite `db_run()` to use `CommitTxn::Batch` instead of `StartTxn`/`CommitTxn` refcount
- Remove `evict_committed()` — no longer needed (no per-op eviction cache)
- Remove `CACHE_KEEP_TIME_SECS` and `MAX_CACHE_SIZE` constants
- Remove `StartTxn`/`CommitTxn` message refcount protocol

**What stays the same:**
- `MergeJoinIter` and `MergeJoinRawIter` (unchanged)
- `merge_cold` and `merge_cold_raw` helper functions (unchanged)
- `QueueSender` struct with backpressure (retained, updated for new message type)
- `QUEUE_HIGH_WATER_MARK`, `QUEUE_PACE_SLEEP` constants (retained)
- `LayeredDatabase` struct (with cold field, `with_cold()`, `without_cold()`, `cold()`)
- `persist()` and `sync_persist()` methods
- `reverse_skip_to()` inherent method on both txn and db
- `Drop` impl for `LayeredDatabase`
- All cold-storage integration (`#[cfg(feature = "cold-storage")]` gates)
- All tests

**Key behavioral changes:**
- `with_write_txn()` now uses `start_write_txn()` internally → writes are buffered until commit
- Per-op sends to bg thread replaced by single batch send on commit
- No more giant shared txn in bg thread — each commit gets its own short-lived txn
- No more `evict_committed()` — the mem buffer is per-txn, not a global cache
- Read path is 4-tier: `buffer → mem → persistent → cold` (was 3-tier: `mem → persistent → cold`)
- `WriteTxn` supports read-after-write (was panic on all read methods)
- `evict_persistent_batch` hard-deletes from mem (no tombstone) for cold archival

### 5.6a `crates/infrastructure/storage/src/cold/` — UNCHANGED

The cold storage module (`cold/mod.rs`, `cold/tx.rs`, `cold/jar.rs`, `cold/archiver.rs`, `cold/producer.rs`, `cold/reconcile.rs`) requires **no changes**. The cold tier is consumed through:
- `ColdTx` / `ColdTxMut` — already implement `DbTx` / `DbTxMut`
- `merge_cold` / `merge_cold_raw` — already chain cold iterators beneath hot
- `ColdStore` — already feature-gated with `#[cfg(feature = "cold-storage")]`

The only integration point is that `WriteTxn` and `LayeredDbTx` must carry the `cold: Option<&'a ColdStore>` field and call `cold_get`, `chain_cold`, `chain_cold_raw` in the same pattern as the current `LayeredDbTx`.

### 5.7 `crates/infrastructure/storage/src/mdbx/database.rs`

**Changes:**
- Remove `DEFAULT_MAX_READ_TXN_DURATION_SECS` constant
- Remove `disable_long_read_safety()` from `MdbxTx` and `MdbxTxMut`
- Remove `DEFAULT_MAX_READ_TXN_DURATION_SECS` from `MdbxConfig`
- Keep all other functionality unchanged

### 5.8 `crates/infrastructure/storage/src/redb/database.rs`

**Changes:**
- Remove `disable_long_read_safety()` from `ReDbTx` and `ReDbTxMut`
- Keep all other functionality unchanged

### 5.9 `crates/infrastructure/storage/src/stores/certificate_store.rs`

**Changes:**
- Remove `ReadTimeout` import (line 11)
- Remove `ReadTimeout` parameter from `after_round()` (line 81)
- Remove `ReadTimeout::Exempt` usage in `after_round()` (line 248)
- Remove `txn.disable_long_read_safety()` call (line 251)

**Before:**
```rust
fn after_round(&self, round: Round, timeout: ReadTimeout) -> StoreResult<Vec<Certificate>> {
    self.with_read_txn(|txn| {
        if timeout == ReadTimeout::Exempt {
            txn.disable_long_read_safety();
        }
        // ...
    })
}
```

**After:**
```rust
fn after_round(&self, round: Round) -> StoreResult<Vec<Certificate>> {
    self.with_read_txn(|txn| {
        // No timeout — removed from architecture
        // ...
    })
}
```

### 5.10 `crates/infrastructure/storage/src/stores/consensus_store.rs`

**Changes:**
- No changes needed (doesn't use `disable_long_read_safety()` or `ReadTimeout`)

### 5.11 `crates/infrastructure/storage/src/stores/epoch_store.rs`

**Changes:**
- No changes needed (doesn't use `disable_long_read_safety()` or `ReadTimeout`)

### 5.12 `crates/middleware/rewards/src/lib.rs`

**Changes:**
- Remove `txn.disable_long_read_safety()` call (line 82)

### 5.13 `crates/middleware/orchestrator/src/epoch_manager/utils.rs`

**Changes:**
- Remove `txn.disable_long_read_safety()` calls (lines 63, 210)

### 5.14 `crates/consensus/primary/src/consensus/state.rs`

**Changes:**
- Remove `ReadTimeout` import (line 9)
- Remove `ReadTimeout::Exempt` usage (line 110)

### 5.15 `crates/consensus/state-sync/src/lib.rs`

**Changes:**
- Remove `ReadTimeout` import (line 14)
- Remove `ReadTimeout::Exempt` usage (line 91)

### 5.16 `crates/consensus/primary/tests/it/storage_tests.rs`

**Changes:**
- Remove `ReadTimeout` import (line 14)
- Remove `ReadTimeout::Enforced` usage (line 383)

### 5.17 `crates/infrastructure/storage/src/stores/certificate_store.rs` — `after_round` callers

**Changes:**
- Update all callers of `after_round()` to remove the `ReadTimeout` parameter
- `certificate_store.rs:248` — remove `ReadTimeout::Exempt` argument
- Any external callers (search for `after_round(`) — remove second argument

### 5.18 `network-cli/src/args/consensus_database.rs`

**Changes:**
- Remove `--consensus-db.read-transaction-timeout` CLI argument (if present)

---

## 6. Tombstone Logic — Changes

### 6.1 Current Behavior

```
remove(key):
  → tombstone in shared store (immediately visible)
  → bg thread sends remove op
  → bg thread applies remove to persistent DB
  → later: evict_committed() removes from mem cache
```

### 6.2 New Behavior

```
txn.remove(key):
  → tombstone in private buffer (only visible after commit)
  → remove op added to persist_buffer

txn.commit():
  → mem_txn.commit(): tombstone merged into shared store
  → persist_buffer sent to bg thread as batch
  → bg thread applies remove to persistent DB

Later:
  → No evict_committed() — no per-op cache
  → mem_db is a cache, not a write-through store
  → eviction is simpler: mem_db stores committed writes temporarily
```

### 6.3 Eviction Simplification

The current `evict_committed()` function tracks individual `InsertTrait` ops in a global `committed_inserts` vec, evicting them from mem after `CACHE_KEEP_TIME_SECS` or when `MAX_CACHE_SIZE` is exceeded.

With the new architecture:
- Writes are buffered per-txn, not globally
- On commit, the mem buffer is merged into the shared store immediately
- The persistent layer gets a batch of ops to apply
- **No eviction is needed** — mem_db is always in sync with committed state

The `delete_removed()` helper on `MemDatabase` is still needed for the case where the bg thread successfully persists a write — it tells mem_db it can clean up. But in the new architecture, this is simpler: after a `CommitTxn::Batch` succeeds, mem_db already has the committed state (it was merged on `WriteTxn::commit()`). No cleanup needed.

### 6.4 Tombstone Visibility Timeline

```
T0: txn1.remove(key=5)        → tombstone in txn1's buffer
T1: txn2.get(key=5)           → txn2 doesn't see txn1's buffer → checks shared store → finds value (not yet tombstoned)
T2: txn1.commit()             → tombstone merged into shared store
T3: txn3.get(key=5)           → checks shared store → finds tombstone → returns None
```

This is correct: readers only see committed state. The only difference from current behavior is that `remove()` is no longer instantly visible — but this is the desired transactional semantics.

### 6.5 Merge-Join Iterator Changes

The `MergeJoinIter` and `MergeJoinRawIter` are already correct — they filter tombstoned keys via a closure. The closure captures `is_tombstoned` from the transaction, which checks both buffer and shared store. No changes needed.

### 6.6 Cold Storage Integration

The cold tier (`cold/` module) is **unchanged**. The integration is in the layered transaction types:

**Read fallthrough:** `buffer → mem → persistent → cold` (4-tier, was 3-tier)
- `WriteTxn::get()` / `LayeredDbTx::get()` fall through to `cold_get()` on hot miss
- `iter()` / `raw_iter()` chain cold beneath hot via `merge_cold()` / `merge_cold_raw()`
- `skip_to()` / `reverse_iter()` pass the key anchor to cold for correct boundary

**Write exclusions:** Cold is append-only — `remove`, `clear_table`, `evict_persistent_batch` never target cold.

**`evict_persistent_batch`:** Hard-deletes from mem (no tombstone) + removes from persistent. A tombstone would shadow the cold fallthrough, so the archived row must be hard-deleted.

**Feature gates:** All cold methods use `#[cfg(feature = "cold-storage")]` with no-op stubs when the feature is off. The `merge_cold` / `merge_cold_raw` functions are already feature-gated.

**Archival producer:** Uses `without_cold()` to get a hot-only view, ensuring "is this row still hot?" is answered by the hot tier alone.

**Auxiliary index:** `ColdBatchLocations` (hot table) maps batch digest → `(epoch, row)`. The cold transaction resolves digests through this index on the caller's hot snapshot.

---

## 7. Migration Strategy

### Phase 1: Foundation (no behavior change for callers)

1. Create `write_buffer.rs` — `WriteOp`, `WriteBuffer`
2. Create `write_lock.rs` — `WriteLockManager`, `WriteLockGuard`
3. Rewrite `mem_db.rs` — `MemTxn` with Arc + buffer
4. Update `mdbx/database.rs` — remove timeout logic
5. Update `redb/database.rs` — remove timeout logic
6. Update `database_traits.rs` — remove `ReadTimeout`, `disable_long_read_safety()`

### Phase 2: LayeredDB Rewrite

7. Rewrite `layered_db.rs` — `WriteTxn`, simplified bg thread, cold integration
8. Update `lib.rs` — add new modules, remove `ReadTimeout` re-export
9. **Verify cold storage integration:**
   - `WriteTxn` and `LayeredDbTx` carry `cold: Option<&'a ColdStore>` field
   - All read methods fall through to cold (`cold_get`, `chain_cold`, `chain_cold_raw`)
   - `with_cold()`, `without_cold()`, `cold()` on `LayeredDatabase` continue to work
   - `evict_persistent_batch` hard-deletes from mem (no tombstone)
   - `merge_cold` / `merge_cold_raw` chain cold beneath hot iterators
   - Feature gates (`#[cfg(feature = "cold-storage")]`) compile correctly both ways

### Phase 3: Call Site Updates

9. Update `certificate_store.rs` — remove `ReadTimeout` param
10. Update `consensus_store.rs` — none needed
11. Update `epoch_store.rs` — none needed
12. Update `rewards/lib.rs` — remove `disable_long_read_safety()`
13. Update `orchestrator/utils.rs` — remove `disable_long_read_safety()`
14. Update `consensus/state.rs` — remove `ReadTimeout`
15. Update `state-sync/lib.rs` — remove `ReadTimeout`
16. Update `storage_tests.rs` — remove `ReadTimeout`
17. Update CLI args — remove `--consensus-db.read-transaction-timeout`

### Phase 4: Business Logic (read-then-write patterns)

18. Update `cert_validator.rs` — switch to `start_write_txn()` + `lock()`
19. Update `handler.rs` — switch to `start_write_txn()` + `lock()`
20. Update `kad.rs` — switch to `start_write_txn()` + `lock()`
21. Update `batch_fetcher.rs` — switch to `start_write_txn()` + `lock()`
22. Update `state-sync/lib.rs` save_consensus — switch to `start_write_txn()` + `lock()`
23. Update `orchestrator/state.rs` — switch to `start_write_txn()` + `lock()`
24. Update `orchestrator/transition.rs` — epoch transition pipeline

### Phase 5: Testing

25. Run all existing tests — should pass with behavioral changes
26. Update tests that depend on immediate write visibility
27. Add tests for:
    - Buffer priority (buffer → store → persistent)
    - Cross-txn isolation (uncommitted writes invisible)
    - Read-after-write consistency
    - Tombstone timing
    - Lock serialization
    - Batch commit atomicity

---

## 8. Behavior Changes Summary

### 8.1 `with_write_txn()` — Writes Buffered Until Commit

**Before:**
```rust
db.with_write_txn(|txn| {
    txn.insert::<Table>(&key, &value)?;  // ← immediately visible to other txns
    txn.insert::<Table>(&key2, &value2)?;  // ← immediately visible
})
// Both writes now visible everywhere
```

**After:**
```rust
db.with_write_txn(|txn| {
    txn.insert::<Table>(&key, &value)?;  // ← invisible to others
    txn.insert::<Table>(&key2, &value2)?;  // ← invisible
})
// Nothing visible until commit
```

This is a **correctness improvement** — the invariant "no uncommitted data visible" is now enforced.

### 8.2 Background Thread — Batched, Not Per-Op

**Before:** Each `insert()`/`remove()`/`clear_table()` sends a separate message to the bg thread. The bg thread accumulates them in a giant shared txn via `StartTxn`/`CommitTxn` refcount.

**After:** Each `WriteTxn::commit()` sends a single batched message. The bg thread opens a short-lived MDBX txn, applies all ops, commits immediately.

### 8.3 Read-Then-Write — Now Possible

**Before:** Not possible within a single `with_write_txn()` closure (reads panic on `DbTxMut`).

**After:** `start_write_txn()` returns a `WriteTxn` that implements both `DbTx` and `DbTxMut`.

```rust
let txn = db.start_write_txn()?;
txn.lock("table")?;
if txn.get::<Table>(&key)?.is_some() {
    txn.remove::<Table>(&key)?;
}
txn.commit()?;
```

### 8.4 `persist()` — Still Works

**Before:** `persist()` sends `CaughtUp` message, waits for bg thread to process all pending messages.

**After:** Same mechanism. `persist()` sends `CommitTxn::CaughtUp`, waits for bg thread to process all pending `Batch` messages. The `pending_write_error` tracking is unchanged.

---

## 9. Risk Assessment

### 9.1 High Risk

1. **`WriteTxn::get()` buffer lookup** — needs to correctly handle insert→remove→insert sequences within the same buffer. Must track per-key operations in order.

2. **Bg thread dispatch** — trait objects (`PersistOp<DB>`) must correctly type-dispatch for all 22 tables. Any mismatch causes runtime errors.

3. **Tombstone timing** — tests that depend on immediate write visibility will fail. Need to identify and update all such tests.

4. **Cold storage integration** — `WriteTxn` and `LayeredDbTx` must correctly chain the cold tier beneath hot iterators. The `merge_cold` / `merge_cold_raw` functions must handle the fault flag from cold scans. The `ColdBatchLocations` auxiliary index must resolve from the hot snapshot.

### 9.2 Medium Risk

5. **`with_write_txn()` visibility change** — code that depends on writes being visible to other concurrent txns mid-transaction will break. This is unlikely in practice (the current code doesn't rely on this), but worth verifying.

6. **Lock contention** — added `WriteLockManager` may change contention patterns. Need to verify no deadlocks.

7. **`evict_persistent_batch` hard-delete** — must ensure hard-delete (no tombstone) is used for cold archival, otherwise tombstones shadow the cold fallthrough.

### 9.3 Low Risk

8. **`ReadTimeout`/`disable_long_read_safety()` removal** — straightforward removal, no behavioral impact.

9. **`evict_committed()` removal** — simplification, not a behavior change.

10. **QueueSender backpressure** — retained from current architecture; no behavioral change.

---

## 10. Implementation Order

```
Phase 1: Foundation
  write_buffer.rs          (NEW)
  write_lock.rs            (NEW)
  mem_db.rs                (REWRITE)
  database_traits.rs       (TRIM)
  mdbx/database.rs         (TRIM)
  redb/database.rs         (TRIM)

Phase 2: LayeredDB (+ cold integration)
  layered_db.rs            (REWRITE — WriteTxn, LayeredDbTx, bg thread, cold)
  lib.rs                   (UPDATE)
  cold/ (all files)        (UNCHANGED — verify integration points)

Phase 3: Call Sites
  certificate_store.rs     (TRIM)
  rewards/lib.rs           (TRIM)
  orchestrator/utils.rs    (TRIM)
  consensus/state.rs       (TRIM)
  state-sync/lib.rs        (TRIM)
  storage_tests.rs         (TRIM)

Phase 4: Business Logic
  cert_validator.rs        (UPDATE)
  handler.rs               (UPDATE)
  kad.rs                   (UPDATE)
  batch_fetcher.rs         (UPDATE)
  state-sync/lib.rs        (UPDATE)
  orchestrator/state.rs    (UPDATE)
  orchestrator/transition.rs (UPDATE)

Phase 5: Tests
   All test suites
   Cold storage tests (with and without feature flag)
   New transactional behavior tests (section 12 below)
```

---

## 11. Key Invariants (Each Has Corresponding Test in Section 12)

1. **No uncommitted data visible** — 12.1, 12.2
2. **No giant txns** — 12.6
3. **Short-lived snapshots** — (inherited from MDBX tests)
4. **Accurate `persist()`** — 12.9
5. **Opt-in locking** — 12.5, 12.10
6. **Read priority (4-tier)** — 12.7
7. **Tombstone semantics** — 12.3
8. **Cold is append-only** — (existing cold tests)
9. **Cold feature gates** — 12.8

---

## 12. Missing Test Cases

### 12.1 MemTxn Private Buffer Isolation

**File:** `mem_db.rs` test module

**Test: `mem_txn_buffer_is_private_until_commit`**
```
Given: MemDatabase with existing key=1, value="old"
When:  txnA = mem_db.write_txn(); txnA.insert(1, "new")
And:   txnB = mem_db.read_txn()
Then:  txnB.get(1) == "old"  (txnA's write is invisible)
And:   txnA.get(1) == "new"  (read-after-write via buffer)
When:  txnA.commit()
Then:  txnB2 = mem_db.read_txn(); txnB2.get(1) == "new"  (now visible)
```

**Test: `mem_txn_buffer_discarded_on_drop`**
```
Given: MemDatabase with key=1, value="original"
When:  txn = mem_db.write_txn(); txn.insert(1, "modified")
And:   drop(txn)  // no commit
Then:  txn2 = mem_db.read_txn(); txn2.get(1) == "original"
```

**Test: `mem_txn_insert_remove_insert_sequence`**
```
Given: MemDatabase with key=1, value="v1"
When:  txn = mem_db.write_txn()
And:   txn.insert(1, "v2")
And:   txn.remove(1)
And:   txn.insert(1, "v3")
Then:  txn.get(1) == "v3"  (last operation wins in buffer)
When:  txn.commit()
Then:  mem_db.get(1) == "v3"
```

**Test: `mem_txn_remove_nonexistent_creates_tombstone`**
```
Given: MemDatabase with no data
When:  txn = mem_db.write_txn(); txn.remove::<Table>(&key)
Then:  txn.get(&key) == None  (buffer tombstone)
And:   txn.is_tombstoned(&key) == true
When:  txn.commit()
Then:  mem_db.is_tombstoned(&key) == true  (tombstone merged to shared store)
```

**Test: `mem_txn_iter_merges_buffer_and_store`**
```
Given: MemDatabase with key=1:"a", key=3:"c"
When:  txn = mem_db.write_txn()
And:   txn.insert(2, "b")     (buffer only)
And:   txn.remove(1)          (buffer tombstone)
Then:  txn.iter() yields [(2,"b"), (3,"c")]  (buffer merge: add 2, skip tombstoned 1)
```

---

### 12.2 Cross-Transaction Isolation

**File:** `layered_db.rs` test module

**Test: `write_txn_isolation_uncommitted_invisible_to_read_txn`**
```
Given: LayeredDatabase with key=1, value="original"
When:  writeTxn = db.start_write_txn()
And:   writeTxn.insert(1, "new")
And:   readTxn = db.read_txn()
Then:  readTxn.get(1) == "original"  (uncommitted write invisible)
When:  writeTxn.commit()
Then:  readTxn2 = db.read_txn(); readTxn2.get(1) == "new"
```

**Test: `write_txn_isolation_two_writers`**
```
Given: LayeredDatabase with key=1, value="original"
When:  txnA = db.start_write_txn(); txnA.lock("table")
And:   txnA.insert(1, "fromA")
And:   txnB = db.start_write_txn()
And:   txnB.lock("table")  // blocks until txnA commits or drops
Then:  (txnB is serialized behind txnA via lock)
When:  txnA.commit(); txnB.insert(1, "fromB"); txnB.commit()
Then:  db.get(1) == "fromB"
```

**Test: `write_txn_sees_own_writes_not_others`**
```
Given: LayeredDatabase with key=1:"A", key=2:"B"
When:  txnA = db.start_write_txn(); txnA.insert(1, "A_new")
And:   txnB = db.start_write_txn(); txnB.insert(2, "B_new")
Then:  txnA.get(1) == "A_new"  (sees own buffer)
And:   txnA.get(2) == "B"      (does NOT see txnB's buffer)
And:   txnB.get(1) == "A"      (does NOT see txnA's buffer)
And:   txnB.get(2) == "B_new"  (sees own buffer)
```

---

### 12.3 Deferred Tombstone Visibility

**File:** `layered_db.rs` test module

**Test: `tombstone_not_visible_before_commit`**
```
Given: LayeredDatabase with key=1, value="present"
When:  txnA = db.start_write_txn(); txnA.remove(1)
And:   txnB = db.read_txn()
Then:  txnB.get(1) == "present"  (tombstone not yet committed)
When:  txnA.commit()
Then:  txnC = db.read_txn(); txnC.get(1) == None  (tombstone now visible)
```

**Test: `tombstone_iter_filtered_after_commit`**
```
Given: LayeredDatabase with key=1:"a", key=2:"b", key=3:"c"
When:  txn = db.start_write_txn(); txn.remove(2)
Then:  txn.iter() yields [(1,"a"), (3,"c")]  (buffer tombstone filtered)
When:  txn.commit()
Then:  db.iter() yields [(1,"a"), (3,"c")]  (merged tombstone filtered)
```

**Test: `tombstone_shadows_persistent_not_cold`**
```
Given: LayeredDatabase with cold attached, key=1 archived to cold, removed from hot
When:  txn = db.start_write_txn(); txn.remove(1)  (tombstone in hot mem)
Then:  txn.get(1) == None  (tombstone shadows cold)
When:  txn.commit()
Then:  db.get(1) == None  (tombstone persists in mem, shadows cold)
```

**Test: `evict_hard_delete_does_not_shadow_cold`**
```
Given: LayeredDatabase with cold attached, key=1 in hot + cold
When:  txn = db.start_write_txn(); txn.evict_persistent_batch::<Table>(&[1])
Then:  txn.get(1) == cold_value  (hard-delete, no tombstone, falls through to cold)
```

---

### 12.4 WriteTxn Read Methods

**File:** `layered_db.rs` test module

**Test: `write_txn_get_reads_from_buffer`**
```
Given: LayeredDatabase with key=1, value="persistent"
When:  txn = db.start_write_txn()
And:   txn.insert(1, "buffered")
Then:  txn.get(1) == "buffered"  (buffer wins over persistent)
```

**Test: `write_txn_get_falls_through_all_tiers`**
```
Given: LayeredDatabase with cold, key=1 in cold only, key=2 in persistent, key=3 in mem
When:  txn = db.start_write_txn()
Then:  txn.get(3) == mem_value      (mem tier)
And:   txn.get(2) == persistent_val (persistent tier)
And:   txn.get(1) == cold_value     (cold tier)
```

**Test: `write_txn_iter_merges_buffer_and_tiers`**
```
Given: LayeredDatabase with key=1:"a", key=3:"c"
When:  txn = db.start_write_txn()
And:   txn.insert(2, "b")
Then:  txn.iter() yields [(1,"a"), (2,"b"), (3,"c")]
```

**Test: `write_txn_skip_to_respects_buffer`**
```
Given: LayeredDatabase with key=1:"a", key=5:"e"
When:  txn = db.start_write_txn()
And:   txn.insert(3, "c")
Then:  txn.skip_to(3) yields [(3,"c"), (5,"e")]
```

**Test: `write_txn_reverse_iter_respects_buffer`**
```
Given: LayeredDatabase with key=1:"a", key=3:"c"
When:  txn = db.start_write_txn()
And:   txn.insert(2, "b")
Then:  txn.reverse_iter() yields [(3,"c"), (2,"b"), (1,"a")]
```

**Test: `write_txn_raw_get_falls_through_tiers`**
```
Given: LayeredDatabase with key=1 in persistent
When:  txn = db.start_write_txn()
And:   txn.insert(1, new_value)
Then:  txn.raw_get(1) returns encoded new_value (buffer wins)
And:   txn.raw_get(2) returns None (not in any tier)
```

---

### 12.5 WriteLockManager

**File:** `write_lock.rs` test module (new file)

**Test: `lock_acquire_and_release`**
```
Given: WriteLockManager
When:  guard = manager.lock("table1")
Then:  lock is held
When:  drop(guard)
Then:  lock is released
```

**Test: `lock_serializes_concurrent_writers`**
```
Given: WriteLockManager
When:  threadA acquires lock("table1")
And:   threadB tries to acquire lock("table1")
Then:  threadB blocks until threadA releases
```

**Test: `lock_different_tables_independent`**
```
Given: WriteLockManager
When:  threadA acquires lock("table1")
And:   threadB acquires lock("table2")
Then:  both acquire immediately (different tables, no contention)
```

**Test: `lock_released_on_commit`**
```
Given: WriteTxn with lock("table1")
When:  txn.commit()
Then:  lock is released (WriteTxn dropped after commit)
```

**Test: `lock_released_on_drop_without_commit`**
```
Given: WriteTxn with lock("table1")
When:  drop(txn)  // no commit
Then:  lock is released
```

**Test: `no_op_lock_is_instant`**
```
Given: WriteLockGuard::no_op()
Then:  acquire/release is zero-cost (no mutex involved)
```

---

### 12.6 PersistOp Batch Commit Atomicity

**File:** `layered_db.rs` test module

**Test: `batch_commit_all_ops_in_one_txn`**
```
Given: LayeredDatabase
When:  txn = db.start_write_txn()
And:   txn.insert(key1, val1)
And:   txn.insert(key2, val2)
And:   txn.remove(key3)
And:   txn.commit()
Then:  bg thread applies all 3 ops in a single MDBX write txn
And:   db.persist() succeeds
```

**Test: `batch_commit_send_once_not_per_op`**
```
Given: LayeredDatabase with message counter on bg thread
When:  txn = db.start_write_txn()
And:   txn.insert(key1, val1)
And:   txn.insert(key2, val2)
And:   txn.commit()
Then:  bg thread received exactly 1 CommitTxn::Batch message (not 2)
```

**Test: `batch_empty_commit_is_noop`**
```
Given: LayeredDatabase
When:  txn = db.start_write_txn(); txn.commit()  (no writes)
Then:  bg thread receives CommitTxn::Batch([]) — no error
```

**Test: `batch_fail_fast_one_op_fails_others_still_apply`**
```
Given: LayeredDatabase with key1 (valid), key2 (will fail, e.g. MAP_FULL)
When:  txn = db.start_write_txn()
And:   txn.insert(key1, val1)
And:   txn.insert(key2, val2)  // will fail
And:   txn.commit()
Then:  bg thread logs error for key2
And:   db.persist() returns Err  (error surfaced)
```

---

### 12.7 4-Tier Read Resolution in WriteTxn

**File:** `layered_db.rs` test module (with cold-storage feature)

**Test: `write_txn_read_buffer_shadows_mem`**
```
Given: LayeredDatabase with key=1 in mem = "mem_val"
When:  txn = db.start_write_txn()
And:   txn.insert(1, "buffer_val")
Then:  txn.get(1) == "buffer_val"  (buffer wins over mem)
```

**Test: `write_txn_read_buffer_shadows_persistent`**
```
Given: LayeredDatabase with key=1 in persistent = "persist_val"
When:  txn = db.start_write_txn()
And:   txn.insert(1, "buffer_val")
Then:  txn.get(1) == "buffer_val"  (buffer wins over persistent)
```

**Test: `write_txn_read_buffer_shadows_cold`**
```
Given: LayeredDatabase with cold, key=1 in cold = "cold_val"
When:  txn = db.start_write_txn()
And:   txn.insert(1, "buffer_val")
Then:  txn.get(1) == "buffer_val"  (buffer wins over cold)
```

**Test: `write_txn_tombstone_shadows_cold`**
```
Given: LayeredDatabase with cold, key=1 in cold = "cold_val", key=1 in persistent
When:  txn = db.start_write_txn()
And:   txn.remove(1)
Then:  txn.get(1) == None  (buffer tombstone shadows all tiers including cold)
```

**Test: `write_txn_iter_includes_cold_tier`**
```
Given: LayeredDatabase with cold, key=1 in cold, key=5 in persistent
When:  txn = db.start_write_txn()
Then:  txn.iter() yields [(1, cold_val), (5, persist_val)]  (cold + persistent merged)
```

---

### 12.8 Cold Feature Gate Compilation

**File:** CI / build test

**Test: `build_without_cold_storage_feature`**
```
Given: Cargo workspace
When:  cargo build --no-default-features --features reth-libmdbx
Then:  compilation succeeds (cold-storage feature disabled)
And:   LayeredDatabase has no `cold` field
And:   cold_get, chain_cold, chain_cold_raw are no-op stubs
```

**Test: `layered_db_without_cold_returns_none`**
```
Given: LayeredDatabase without cold attached (feature disabled or not set)
When:  db.get(key) where key exists only in cold
Then:  returns None (no cold fallthrough)
```

---

### 12.9 Background Thread Error Recovery

**File:** `layered_db.rs` test module

**Test: `bg_thread_error_surfaced_by_persist`**
```
Given: LayeredDatabase where bg thread write fails (e.g. disk full)
When:  txn = db.start_write_txn(); txn.insert(key, val); txn.commit()
Then:  mem layer has the write (committed to shared store)
And:   db.persist() returns Err  (bg thread error surfaced)
```

**Test: `bg_thread_continues_after_one_failed_batch`**
```
Given: LayeredDatabase
When:  txn1 fails (e.g. MAP_FULL)
And:   txn2 = db.start_write_txn(); txn2.insert(valid_key, val); txn2.commit()
Then:  bg thread still processes txn2 after txn1 failure
And:   db.persist() returns Err (earliest error)
```

---

### 12.10 Concurrent Write Serialization

**File:** `layered_db.rs` test module

**Test: `concurrent_writers_same_table_serialized`**
```
Given: LayeredDatabase with key=1, value=0
When:  10 threads concurrently: txn.lock("table")?; txn.get(1); txn.insert(1, get+1); txn.commit()
Then:  final value of key=1 == 10  (no lost updates)
```

**Test: `concurrent_writers_different_tables_parallel`**
```
Given: LayeredDatabase with keyA=0, keyB=0
When:  threadA locks "tableA" and writes keyA
And:   threadB locks "tableB" and writes keyB
Then:  both complete without blocking each other
```

---

## Appendix A: Unchanged Types & Helpers (Full Definitions)

These types are referenced throughout the spec but are **unchanged** from the
existing codebase. They are reproduced here so the spec is fully self-contained
for zero-context implementation. Copy them verbatim into the target files.

### A.1 Type Aliases (`mem_db.rs`)

```rust
/// (bool = is_tombstoned, Vec<u8> = bcs-encoded value)
type StoreTableValueType = (bool, Vec<u8>);

/// Ordered table: encoded key → (tombstone flag, encoded value)
type StoreTableType = BTreeMap<Vec<u8>, StoreTableValueType>;

/// Map from table name → table data
pub type StoreType = HashMap<&'static str, StoreTableType>;
```

**Note:** `StoreType` must be `pub` so that `write_buffer.rs` and `layered_db.rs`
can import it. The `pub` visibility is a new requirement of this refactor.

### A.2 MemDBMetrics (`mem_db.rs`)

```rust
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
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => Self::try_new(&Registry::new())
                .expect("Prometheus error, are you using it wrong?"),
        }
    }
}
```

### A.3 MergeJoinIter (`layered_db.rs`)

```rust
/// Streaming merge-join iterator for LayeredDB.
/// Merges sorted iterators from the persistent DB and in-memory cache,
/// with mem entries taking precedence on key conflicts.
/// Entries tombstoned in mem are filtered out via the `is_tombstoned` closure.
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
                    let cmp = db_key.cmp(mem_key);
                    let cmp = if self.reverse { cmp.reverse() } else { cmp };
                    match cmp {
                        Ordering::Less => {
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
```

### A.4 MergeJoinRawIter (`layered_db.rs`)

```rust
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
```

### A.5 merge_cold & merge_cold_raw (`layered_db.rs`)

```rust
/// Merges the cold-ordered stream for `T` beneath `hot`; hot wins on an equal key.
/// Passthrough when no cold layer is attached or `T` has no cold key order.
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
```

### A.6 Encode/Decode Functions (from `rayls_infrastructure_types::codec`)

These are **not** defined in this crate — they are imported from
`rayls_infrastructure_types`. The import statement is:

```rust
use rayls_infrastructure_types::{decode, decode_key, encode, encode_key};
```

Signatures for reference:
```rust
pub fn decode_key<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T;  // panics on failure
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T;       // panics on failure
pub fn encode_key<T: Serialize>(obj: &T) -> Vec<u8>;               // panics on failure
pub fn encode<T: Serialize>(obj: &T) -> Vec<u8>;                   // panics on failure
```

### A.7 PersistOp Lifetime Resolution

The `PersistOp<DB>` trait uses a GAT (`DB::TXMut<'_>`) with an elided lifetime:

```rust
trait PersistOp<DB: Database>: Send + 'static {
    fn apply(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}
```

**Why this is object-safe:** The concrete `Table` type is erased into the `Box`,
and `DB` is the persistent backend type parameter. The `TXMut<'_>` lifetime is
resolved at each `apply` call site — the caller provides a fresh `DB::TXMut<'a>`
and the trait object borrows it for `'a`. No self-referential structs are involved.

In `db_run`, each batch creates a new short-lived write txn, calls `op.apply(&mut txn)`
for every op in the batch, then commits and drops the txn. The lifetime of the txn
outlives all `apply` calls within the batch loop.

### A.8 LayeredDatabase Drop (`layered_db.rs`)

```rust
impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!(target: "layered_db", "LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(CommitTxn::Shutdown) {
                tracing::error!(target: "layered_db", "Error while trying to send shutdown to layered DB thread {e}");
                return;
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
```

### A.9 QueueSender depth method (`layered_db.rs`)

```rust
/// Returns the writer messages enqueued but not yet applied by the background thread.
fn depth(&self) -> usize {
    self.depth.load(AtomicOrdering::Relaxed)
}
```

### A.10 persist() and sync_persist() (`layered_db.rs`)

These methods are **unchanged** from the current implementation. They are part of
the `Database` trait default impls for `LayeredDatabase`. The only difference is
the message type changed from `DBMessage::CaughtUp` to `CommitTxn::CaughtUp`.

```rust
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

fn sync_persist(&self) {
    let (tx, mut rx) = oneshot::channel();
    let depth_at_send = self.tx.depth();
    let started = Instant::now();
    let r = self.tx.send(CommitTxn::CaughtUp(tx))
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
```

### A.11 Helper functions (`layered_db.rs`)

```rust
const PERSIST_SLOW_WARN: Duration = Duration::from_secs(1);

pub(crate) fn register_metric_or_unscraped<T>(
    register: impl Fn(&Registry) -> Result<T, prometheus::Error>,
) -> T {
    register(default_registry())
        .unwrap_or_else(|_| register(&Registry::new()).expect("metric on a fresh registry"))
}

fn writer_queue_depth_gauge() -> IntGauge {
    register_metric_or_unscraped(|registry| {
        register_int_gauge_with_registry!(
            "layered_db_writer_queue_depth",
            "Consensus DB layered writer messages enqueued but not yet applied.",
            registry,
        )
    })
}

fn log_persist_latency(elapsed: Duration, depth: usize) {
    if elapsed >= PERSIST_SLOW_WARN {
        tracing::warn!(
            target: "storage",
            ?elapsed,
            depth,
            "consensus DB persist drained a slow writer backlog"
        );
    } else {
        tracing::debug!(
            target: "storage",
            ?elapsed,
            depth,
            "consensus DB persist flushed"
        );
    }
}
```

### A.12 open_default_tables (`lib.rs`)

This function is **unchanged**. It opens all consensus tables on a given `Database`.

```rust
/// Opens one table on `db`, folding [`rayls_infrastructure_types::Table::NAME`] into the error.
fn open_one<T: rayls_infrastructure_types::Table>(db: &mut impl Database) -> eyre::Result<()> {
    db.open_table::<T>().map_err(|e| eyre::eyre!("failed to open {} table: {e}", T::NAME))
}

fn open_default_tables<DB: Database>(db: &mut DB) -> eyre::Result<()> {
    open_one::<LastProposed>(db)?;
    open_one::<LastProposedByAuthority>(db)?;
    open_one::<Votes>(db)?;
    open_one::<Certificates>(db)?;
    open_one::<CertificateDigestByRound>(db)?;
    open_one::<CertificateDigestByOrigin>(db)?;
    open_one::<Payload>(db)?;
    open_one::<Batches>(db)?;
    open_one::<ConsensusBlocks>(db)?;
    open_one::<ConsensusBlockNumbersByDigest>(db)?;
    open_one::<ConsensusBlocksCache>(db)?;
    open_one::<NodeBatchesCache>(db)?;
    open_one::<EpochRecords>(db)?;
    open_one::<EpochCerts>(db)?;
    open_one::<EpochRecordsIndex>(db)?;
    open_one::<EpochTransitionCheckpoints>(db)?;
    open_one::<KadRecords>(db)?;
    open_one::<KadProviderRecords>(db)?;
    open_one::<KadWorkerRecords>(db)?;
    open_one::<KadWorkerProviderRecords>(db)?;
    open_one::<BatchSeqCounter>(db)?;
    open_one::<NodeIdentity>(db)?;
    open_one::<BatchOrderingState>(db)?;
    #[cfg(feature = "cold-storage")]
    {
        open_one::<ColdBatchLocations>(db)?;
        open_one::<ColdArchiveHighWaterMark>(db)?;
    }
    Ok(())
}
```

### A.13 Database, DbTx, DbTxMut Traits (from `rayls_infrastructure_types`)

These traits are **not** defined in this crate — they are imported from
`rayls_infrastructure_types`. The import statement is:
```rust
use rayls_infrastructure_types::{Database, DbTx, DbTxMut, Table, DBIter, DBRawIter};
```

**Full trait definitions for reference:**
```rust
pub trait KeyT: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static {}
pub trait ValueT: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static {}

impl<K: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static> KeyT for K {}
impl<V: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static> ValueT for V {}

pub trait Table: Send + Sync + Debug + 'static {
    type Key: KeyT;
    type Value: ValueT;
    const NAME: &'static str;
}

pub type DBIter<'i, T> = Box<dyn Iterator<Item = (<T as Table>::Key, <T as Table>::Value)> + 'i>;
pub type DBRawIter<'i> = Box<dyn Iterator<Item = (Cow<'i, [u8]>, Cow<'i, [u8]>)> + 'i>;

pub trait DbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(self.get::<T>(key)?.map(|value| Cow::Owned(crate::encode(&value))))
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.get::<T>(key)?.is_some())
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T>;
    fn raw_iter<T: Table>(&self) -> DBRawIter<'_>;

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        let target = crate::encode_key(key);
        Ok(Box::new(self.raw_iter::<T>().skip_while(move |(k, _)| k.as_ref() < target.as_slice())))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;
    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_>;
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;
    fn disable_long_read_safety(&self);
}

pub trait DbTxMut: DbTx {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;
    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()>;

    fn evict_persistent_batch<T: Table>(&mut self, keys: &[T::Key]) -> eyre::Result<()> {
        for key in keys {
            self.remove::<T>(key)?;
        }
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()>;
    fn commit(self) -> eyre::Result<()>;
}

pub trait Database: Send + Sync + Clone + Unpin + 'static {
    type TX<'txn>: DbTx + Debug + 'txn where Self: 'txn;
    type TXMut<'txn>: DbTxMut + Debug + 'txn where Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()>;
    fn read_txn(&self) -> eyre::Result<Self::TX<'_>>;
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>>;
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;
    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;
    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()>;
    fn clear_table<T: Table>(&self) -> eyre::Result<()>;
    fn is_empty<T: Table>(&self) -> bool;
    fn iter<T: Table>(&self) -> DBIter<'_, T>;
    fn raw_iter<T: Table>(&self) -> DBRawIter<'_>;
    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;
    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;
    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_>;
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;

    // Default methods:
    fn multi_get<'a, T: Table>(
        &'a self,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        self.with_read_txn(|tx| keys.into_iter().map(|key| tx.get::<T>(key.borrow())).collect())
    }

    fn with_read_txn<F, R>(&self, f: F) -> eyre::Result<R>
    where
        F: FnOnce(&Self::TX<'_>) -> eyre::Result<R>,
    {
        let tx = self.read_txn()?;
        f(&tx)
    }

    fn with_write_txn<F, R>(&self, f: F) -> eyre::Result<R>
    where
        F: FnOnce(&mut Self::TXMut<'_>) -> eyre::Result<R>,
    {
        let mut tx = self.write_txn()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }

    fn compact(&self) -> eyre::Result<()> { Ok(()) }

    fn persist(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn sync_persist(&self) {}
}
```

**Important:** `MemTxn::raw_skip_to` is NOT overridden — it uses the `DbTx` trait default
implementation, which filters a full `raw_iter` scan. This is correct for the in-memory backend.

### A.14 ColdStore & ColdTx (from `crate::cold`, `#[cfg(feature = "cold-storage")]`)

These types are **unchanged** and defined in the `cold` submodule. They are imported in
`layered_db.rs` via:
```rust
#[cfg(feature = "cold-storage")]
use crate::{
    cold::{ColdStore, ColdTx},
    tables::ColdBatchLocations,
};
```

**ColdStore struct:**
```rust
#[derive(Debug)]
pub struct ColdStore {
    consensus_blocks: ColdSegment,
    batches: ColdSegment,
}
```

**ColdTx struct:**
```rust
pub struct ColdTx<'c> {
    cold: &'c ColdStore,
    index: Box<dyn Fn(&BlockHash) -> eyre::Result<Option<ColdLocation>> + 'c>,
    faulted: Rc<Cell<bool>>,
}

impl<'c> ColdTx<'c> {
    pub fn new(
        cold: &'c ColdStore,
        index: impl Fn(&BlockHash) -> eyre::Result<Option<ColdLocation>> + 'c,
    ) -> Self;

    pub fn faulted(&self) -> Rc<Cell<bool>>;

    pub(crate) fn scan<T: Table>(
        &self,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> Option<DBIter<'c, T>>;

    pub(crate) fn raw_scan<T: Table>(
        &self,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> Option<DBRawIter<'c>>;
}

impl DbTx for ColdTx<'_> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;
    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>>;
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;
    fn iter<T: Table>(&self) -> DBIter<'_, T>;
    fn raw_iter<T: Table>(&self) -> DBRawIter<'_>;
    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;
    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>>;
    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;
    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_>;
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;
    fn disable_long_read_safety(&self);
}
```

**ColdLocation type:**
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdLocation {
    pub epoch: Epoch,
    pub row: u64,
}
```

### A.15 Cold Tier Tables (`#[cfg(feature = "cold-storage")]`)

Defined via the `tables!` macro in `lib.rs`. Expanded forms:

```rust
#[cfg(feature = "cold-storage")]
#[derive(Debug)]
pub struct ColdBatchLocations {}
#[cfg(feature = "cold-storage")]
impl rayls_infrastructure_types::Table for ColdBatchLocations {
    type Key = BlockHash;       // B256 (alias for [u8; 32])
    type Value = ColdLocation;  // { epoch: Epoch, row: u64 }
    const NAME: &'static str = "cold_batch_locations";
}

#[cfg(feature = "cold-storage")]
#[derive(Debug)]
pub struct ColdArchiveHighWaterMark {}
#[cfg(feature = "cold-storage")]
impl rayls_infrastructure_types::Table for ColdArchiveHighWaterMark {
    type Key = u8;              // sentinel key (always 0)
    type Value = Epoch;         // u64 alias for epoch number
    const NAME: &'static str = "cold_archive_high_water_mark";
}
```

**Note:** `ColdBatchLocations` maps a batch digest (`BlockHash`) to its cold jar location
(`ColdLocation`). It is rebuildable from the jars and serves as the auxiliary index that
`ColdTx::new` resolves via the `index` closure.

**Note:** `ColdArchiveHighWaterMark` has at most one row with key `0` (sentinel
`ARCHIVE_HIGH_WATER_MARK_KEY`). The value is the last fully-archived epoch number.
```
