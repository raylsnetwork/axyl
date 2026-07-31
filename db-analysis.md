# Database Architecture Analysis

## MemDatabase

### Responsibility

In-memory cache over the persistent database. Provides fast, immediate visibility of committed writes and tombstone-based logical deletes.

### Life Cycles

**MemDatabase (shared store)**
- Created once at node startup
- Lives for the entire node lifetime
- Holds a shared, thread-safe store (HashMap of tables)

**MemTxn (transaction)**
- Created by `MemDatabase::read_txn()` or `write_txn()`
- Holds a reference to the shared store + a private write buffer
- **Reads:** checks private buffer first, then falls through to shared store
- **Writes:** go to private buffer only (invisible to other txns)
- **Commit:** atomically merges private buffer into shared store
- **Drop (without commit):** buffer discarded, no changes to shared store
- Lives until `commit()` or `drop()`

**Visibility Rules**
- Uncommitted writes are invisible to all other txns
- Committed writes are immediately visible to all readers
- Each txn sees its own buffered writes (read-after-write consistency)

---

## MDBX Layer

### Responsibility

Persistent key-value storage on disk. Provides MVCC snapshots for reads and short-lived write transactions for commits.

### Life Cycles

**MdbxDatabase (environment)**
- Created once at node startup
- Lives for the entire node lifetime
- Manages the MDBX environment (file handles, geometry, readers)

**MdbxTx (read-only transaction)**
- Created by `MdbxDatabase::read_txn()`
- Opens an MDBX MVCC snapshot at creation time
- **Reads:** query the snapshot (get, iter, skip_to, etc.)
- **Commit:** no-op (read-only txns are read-only)
- **Drop:** releases the snapshot, frees MVCC resources
- Lives for the duration of the read operation

**MdbxTxMut (write transaction)**
- Created by `MdbxDatabase::write_txn()`
- Opens a short-lived write transaction
- **Writes:** buffered in the txn (insert, remove, clear)
- **Commit:** applies all writes to disk atomically
- **Drop (without commit):** aborts, discards all writes
- Lives only for the duration of a single commit (milliseconds)

---

## LayeredDatabase

### Responsibility

Orchestrates MemDB and the persistent DB. Provides per-txn write buffering, opt-in locking, async flush, and a durability barrier.

### Life Cycles

**LayeredDatabase (top-level handle)**
- Created once at node startup
- Lives for the entire node lifetime
- Owns MemDatabase + Persistent DB + background thread + lock manager

**WriteTxn (per-txn state)**
- Created by `LayeredDatabase::start_write_txn()`
- Holds: MemTxn + write buffer + lock manager reference
- **Lock:** acquires table-level mutex (caller decides which tables)
- **Begin:** opens persistent read snapshot (MDBX)
- **Reads:** MemTxn buffer → persistent snapshot
- **Writes:** MemTxn buffer (in-memory) + persistent write buffer (for flush)
- **Commit:**
  1. MemTxn commits (merge in-memory buffer)
  2. Persistent write buffer sent to background thread (async)
  3. Locks released
  4. Returns immediately
- **Drop (without commit):** both buffers discarded, locks released
- Lives until `commit()` or `drop()`

**WriteLockManager**
- Owned by LayeredDatabase
- Manages per-table mutexes
- `lock(table)` → acquires write lock (blocks other writers to that table)
- Lock released when WriteTxn is dropped or committed

**Background Thread**
- Created once at LayeredDatabase open
- Lives for the entire node lifetime
- Receives `CommitTxn` messages from write txns
- For each message: opens short-lived MDBX write txn, applies buffered writes, commits
- Handles `CaughtUp` messages for `persist()`
- Handles `Shutdown` on node shutdown

**Persist (durability barrier)**
- Called by the application at epoch transitions, shutdown, etc.
- Sends `CaughtUp` to background thread, waits for reply
- Returns `Ok(())` if all pending writes flushed successfully
- Returns `Err(e)` if any write failed

---

## Layer Interaction Flow

```
Application:
    txn = db.start_write_txn()     → creates WriteTxn
    txn.lock("table")              → acquires table lock
    txn.begin()                    → opens persistent snapshot
    txn.insert(key, value)         → writes to both in-memory + persistent buffer
    txn.commit()                   → merges in-memory, sends persistent buffer to background
                                     (returns immediately)
    db.persist()                   → waits for background to flush
                                     (returns when all committed txns flushed)
```

---

## What Each Layer Does NOT Do

| Layer | Does Not Do |
|---|---|
| **MemDB** | Persistence, MVCC snapshots, error recovery |
| **MDBX** | Write buffering, lock management, tombstone tracking |
| **LayeredDB** | Persistence, MVCC snapshots, in-memory caching |

---

## Key Invariants

1. **No uncommitted data visible** — each layer's txn buffer is private until commit
2. **No giant txns** — each WriteTxn flushes independently; no shared write txn
3. **Short-lived snapshots** — MDBX snapshots live only for the read duration
4. **Accurate persist()** — `persist()` waits for all committed txns to flush
5. **Opt-in locking** — caller decides which tables to lock; database provides the mechanism
