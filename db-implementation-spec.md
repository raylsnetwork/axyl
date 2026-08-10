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
    txn.lock("table")              → acquires table lock
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
struct WriteBuffer {
    /// Operations grouped by table name.
    ops: HashMap<&'static str, Vec<WriteOp>>,
}

impl WriteBuffer {
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

    /// Apply all buffered operations to a MemTxn's commit target.
    fn apply_to_mem(self, store: &RwLock<StoreType>) {
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
struct MemTxn<'a> {
    store: Arc<RwLock<StoreType>>,
    buffer: WriteBuffer,
}

impl MemTxn<'_> {
    /// Read: check buffer first, then fall through to shared store.
    fn get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
        // 1. Check buffer (read-after-write consistency)
        if let Some(val) = self.buffer.get::<T>(key) {
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

    /// Commit: merge buffer into shared store.
    fn commit(self) {
        self.buffer.apply_to_mem(&self.store);
    }
}

impl DbTx for MemTxn<'_> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        Ok(self.get::<T>(key))
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

    // ... other DbTx methods similarly merge buffer + shared store
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
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::RwLock;

struct WriteLockManager {
    locks: RwLock<HashMap<&'static str, Mutex<()>>>,
}

/// Guard that holds a table-level write lock.
/// Dropping the guard releases the mutex.
pub struct WriteLockGuard {
    _lock: Option<parking_lot::MutexGuard<'static, ()>>,
}

impl WriteLockManager {
    fn lock(&self, table_name: &'static str) -> WriteLockGuard {
        let mut locks = self.locks.write().unwrap();
        let mutex = locks.entry(table_name).or_insert_with(|| Mutex::new(()));
        WriteLockGuard {
            _lock: Some(mutex.lock()),
        }
    }
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        self._lock.take();
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

    /// Locks held by this transaction.
    locks: Vec<WriteLockGuard>,

    /// Channel to send committed buffer to background thread.
    tx: QueueSender<CommitTxn<DB>>,

    /// Cold tier read transaction (feature-gated, append-only reads only).
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
}

impl<'a, DB: Database> DbTx for WriteTxn<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        // 1. Check mem buffer (read-after-write)
        if let Some(val) = self.mem_txn.buffer.get::<T>(key) {
            return Ok(Some(val));
        }
        // 2. Check mem_db shared store
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
        // Same 4-tier fallthrough: buffer → mem → persistent → cold
        if let Some(val) = self.mem_txn.buffer.get::<T>(key) {
            return Ok(Some(Cow::Owned(encode(&val))));
        }
        if let Some((_, val)) = self.mem_txn.get_raw::<T>(key) {
            return Ok(Some(Cow::Owned(encode(&val))));
        }
        match self.persistent_snapshot.raw_get::<T>(key)? {
            Some(bytes) => return Ok(Some(bytes)),
            None => {}
        }
        // Cold raw fallthrough
        #[cfg(feature = "cold-storage")]
        if let Some(tx) = self.cold_tx() {
            if let Some(bytes) = tx.raw_get::<T>(key)? {
                return Ok(Some(Cow::Owned(bytes.into_owned())));
            }
        }
        Ok(None)
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
        // 1. Commit mem layer: merge buffer → shared store
        self.mem_txn.commit();

        // 2. Send persistent buffer to background thread as single batch
        self.tx.send(CommitTxn::Batch(self.persist_buffer))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;

        // 3. Locks released automatically when WriteTxn is dropped
        Ok(())
    }
}
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
        if let Some((_, val)) = self.mem_txn.get_raw::<T>(key) {
            return Ok(Some(Cow::Owned(encode(&val))));
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

    // ... same pattern for reverse_raw_iter, raw_skip_to, last_record, record_prior_to
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

/// A [`DBMessage`] sender that tracks the writer queue's depth and applies
/// soft backpressure when the queue exceeds `QUEUE_HIGH_WATER_MARK`.
struct QueueSender<DB: Database> {
    tx: Sender<CommitTxn<DB>>,
    depth: Arc<AtomicUsize>,
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
    tx: QueueSender<CommitTxn<DB>>,
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
        })
    }

    fn start_write_txn(&self) -> eyre::Result<WriteTxn<'_, DB>> {
        Ok(WriteTxn {
            mem_txn: self.mem_db.write_txn()?,
            persistent_snapshot: self.db.read_txn()?,
            persist_ops: Vec::new(),
            locks: Vec::new(),
            tx: self.tx.clone(),
        })
    }

    // All other Database methods delegate to mem_db + db
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if self.mem_db.is_tombstoned::<T>(key) {
            return Ok(false);
        }
        if self.mem_db.contains_key::<T>(key)? {
            return Ok(true);
        }
        self.db.contains_key::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if self.mem_db.is_tombstoned::<T>(key) {
            return Ok(None);
        }
        if let Some(val) = self.mem_db.get::<T>(key)? {
            return Ok(Some(val));
        }
        self.db.get::<T>(key)
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
        self.tx.send(CommitTxn::Batch(vec![Box::new(PersistClear::<T> { _casper: PhantomData })]))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    // ... iter, reverse_iter, skip_to, raw_iter, reverse_raw_iter
    // ... last_record, record_prior_to, is_empty, multi_get, with_read_txn, with_write_txn
    // ... persist, sync_persist, compact
}
```

### 3.10 MemDatabase — Simplified

```rust
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
    /// Used by the layered read path which checks tombstones separately.
    pub fn get_no_marked_check<T: Table>(&self, key: &T::Key) -> Option<(u64, T::Value)> {
        if let Some(table) = self.store.read().get(T::NAME) {
            let key_bytes = encode_key(key);
            if let Some((removed, val_bytes)) = table.get(&key_bytes) {
                if !*removed {
                    return Some(/* access count, */ decode(val_bytes));
                }
            }
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

    // Delegate to direct methods
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> { ... }
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> { ... }
    fn iter<T: Table>(&self) -> DBIter<'_, T> { ... }
    // ... other methods
}
```

---

## 4. Trait Changes

### 4.1 `Database` trait — Add `start_write_txn()`

```rust
pub trait Database: Send + Sync + Clone + Unpin + 'static {
    // ... existing methods ...

    /// Start a write transaction with buffering.
    /// Returns a WriteTxn that supports read-then-write patterns with lock() calls.
    fn start_write_txn(&self) -> eyre::Result<WriteTxn<'_, Self>>
    where
        Self: Sized;

    /// Acquire a write lock on a table (must be called before read-then-write).
    fn lock_table(&self, table_name: &'static str) -> WriteLockGuard;
}
```

Default impl for backends that don't need locking:
```rust
impl Database for MemDatabase {
    fn start_write_txn(&self) -> eyre::Result<WriteTxn<'_, Self>> {
        Ok(WriteTxn::from_mem(self))
    }

    fn lock_table(&self, _table_name: &'static str) -> WriteLockGuard {
        // No-op lock for backends without locking
        WriteLockGuard::no_op()
    }
}
```

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
- Add `start_write_txn()` and `lock_table()` to `Database` trait (optional — can be added later)

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
```

---

## 11. Key Invariants (Reiterated)

1. **No uncommitted data visible** — each transaction's buffer is private until commit
2. **No giant txns** — each `WriteTxn` flushes independently as a batch
3. **Short-lived snapshots** — MDBX snapshots live only for the read duration
4. **Accurate `persist()`** — waits for all committed txns to flush
5. **Opt-in locking** — caller decides which tables to lock; database provides the mechanism
6. **Read priority (4-tier)** — buffer → mem shared store → persistent snapshot → cold archive
7. **Tombstone semantics** — tombstones visible only after commit; merge-join iterators filter them
8. **Cold is append-only** — writes never target cold; `evict_persistent_batch` hard-deletes from hot
9. **Cold feature gates** — all cold methods compile with no-op stubs when `cold-storage` is disabled
