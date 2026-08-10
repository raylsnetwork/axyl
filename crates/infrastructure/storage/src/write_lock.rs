//! Opt-in per-table write locks.
//!
//! [`WriteTxn`] may acquire a table lock before a read-then-write sequence so
//! concurrent writers on the same table are serialized. Locks are held for the
//! lifetime of the transaction and released on drop.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

/// Per-table mutexes stored as `Arc` so a guard can keep its mutex alive even if
/// the manager's map entry is evicted or the manager itself is dropped.
#[derive(Clone, Default)]
pub(crate) struct WriteLockManager {
    locks: Arc<RwLock<HashMap<&'static str, Arc<Mutex<()>>>>>,
}

impl WriteLockManager {
    /// Acquire a write lock on the given table.
    /// Blocks until the lock is available, then returns a guard that holds it.
    /// Not yet wired into consensus logic (Phase 4); exercised by tests.
    #[allow(dead_code)]
    pub(crate) fn lock(&self, table_name: &'static str) -> WriteLockGuard {
        // First try to get existing mutex under read lock
        let mutex = { self.locks.read().unwrap().get(table_name).cloned() };

        let mutex = match mutex {
            Some(m) => m,
            None => {
                let mut locks = self.locks.write().unwrap();
                locks
                    .entry(table_name)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }
        };

        // Lock the mutex; the guard keeps the lock held until dropped.
        // Safety: the Arc keeps the Mutex alive for as long as the guard exists,
        // so extending the borrow to 'static is sound — the guard never moves the
        // underlying mutex, and its drop (via the Option field) releases the lock.
        let guard = mutex.lock().unwrap();
        let guard: MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };

        WriteLockGuard { _mutex: mutex, _guard: Some(guard) }
    }
}

impl std::fmt::Debug for WriteLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteLockManager")
    }
}

/// Guard that holds a table-level write lock.
/// Dropping the guard releases the mutex.
#[derive(Debug)]
pub(crate) struct WriteLockGuard {
    /// Keeps the mutex alive for the guard's lifetime.
    _mutex: Arc<Mutex<()>>,
    /// The held lock; dropping it (on guard drop) releases the mutex.
    _guard: Option<MutexGuard<'static, ()>>,
}

impl Clone for WriteLockGuard {
    /// A clone is a handle, not a second acquisition: only the guard stored inside the
    /// transaction releases the lock on drop, so cloned handles never double-unlock.
    fn clone(&self) -> Self {
        Self { _mutex: Arc::clone(&self._mutex), _guard: None }
    }
}
