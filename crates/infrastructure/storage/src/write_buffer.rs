//! Per-transaction write buffering.
//!
//! Writes inside a transaction go into a private [`WriteBuffer`] first, so no
//! uncommitted data is visible to other transactions. On commit the buffer is
//! merged into the shared in-memory store and the buffered operations are sent
//! to the background thread as a single typed batch.

use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap},
    marker::PhantomData,
};

use parking_lot::RwLock;
use rayls_infrastructure_types::{decode, encode, encode_key, Database, DbTxMut, Table};

use crate::mem_db::{MemDatabase, StoreType};

/// A single write operation stored in a transaction's private buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteOp {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    ClearTable,
}

/// Per-transaction write buffer.
///
/// LayeredDB owns this buffer — it is NOT delegated to MemDB.
#[derive(Debug, Default)]
pub(crate) struct WriteBuffer {
    /// Operations grouped by table name.
    ops: HashMap<&'static str, Vec<WriteOp>>,
}

impl WriteBuffer {
    pub(crate) fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::Insert {
            key: encode_key(key),
            value: encode(value),
        });
    }

    pub(crate) fn remove<T: Table>(&mut self, key: &T::Key) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::Remove {
            key: encode_key(key),
        });
    }

    pub(crate) fn clear_table<T: Table>(&mut self) {
        self.ops.entry(T::NAME).or_default().push(WriteOp::ClearTable);
    }

    /// Hard-delete: remove ALL ops for the key from buffer, no tombstone.
    pub(crate) fn hard_delete<T: Table>(&mut self, key: &T::Key) {
        if let Some(ops) = self.ops.get_mut(T::NAME) {
            let key_bytes = encode_key(key);
            ops.retain(|op| {
                match op {
                    WriteOp::Insert { key: k, .. } | WriteOp::Remove { key: k } => k != &key_bytes,
                    WriteOp::ClearTable => true,
                }
            });
        }
    }

    /// Check buffer for a value (handles insert/remove/clear precedence).
    pub(crate) fn get<T: Table>(&self, key: &T::Key) -> Option<T::Value> {
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
    pub(crate) fn raw_get<T: Table>(&self, key: &T::Key) -> Option<Vec<u8>> {
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
    /// Returns `Some(true)` if tombstoned, `Some(false)` if inserted, `None` if not in buffer.
    pub(crate) fn is_tombstoned<T: Table>(&self, key: &T::Key) -> Option<bool> {
        if let Some(ops) = self.ops.get(T::NAME) {
            let key_bytes = encode_key(key);
            for op in ops.iter().rev() {
                match op {
                    WriteOp::Insert { key: k, .. } if k == &key_bytes => return Some(false),
                    WriteOp::Remove { key: k } if k == &key_bytes => return Some(true),
                    WriteOp::ClearTable => return Some(true),
                    _ => continue,
                }
            }
        }
        None
    }

    /// Raw operations recorded for `table`, if any (used to replay the buffer
    /// over a store snapshot in order for iterators).
    pub(crate) fn ops_for(&self, table: &'static str) -> Option<&Vec<WriteOp>> {
        self.ops.get(table)
    }

    /// Apply all buffered operations to the shared store on commit.
    /// Note: `store` is `&parking_lot::RwLock<StoreType>` — `.write()` returns guard directly.
    pub(crate) fn apply_to_mem(self, store: &RwLock<StoreType>) {
        let mut shared = store.write();
        for (table_name, ops) in self.ops {
            let table = shared.entry(table_name).or_insert_with(BTreeMap::new);
            for op in ops {
                match op {
                    WriteOp::Insert { key, value } => {
                        table.insert(key, (false, value));
                    }
                    WriteOp::Remove { key } => match table.entry(key) {
                        Entry::Occupied(mut entry) => entry.get_mut().0 = true,
                        Entry::Vacant(entry) => {
                            // Tombstone for keys that only exist in the persistent layer.
                            entry.insert((true, Vec::new()));
                        }
                    },
                    WriteOp::ClearTable => {
                        for value in table.values_mut() {
                            value.0 = true; // mark tombstoned
                        }
                    }
                }
            }
        }
    }
}

/// Per-operation trait for the background-thread dispatch.
///
/// Object-safe: the concrete `Table` type is erased into the box, and `DB` is the
/// persistent backend type. The `TXMut` lifetime is resolved at each `apply` call site.
/// `mem_db` lets a remove skip a key that was re-inserted after its removal was queued,
/// and `clear_mem` lets a persisted insert drop its row from the mem overlay.
pub(crate) trait PersistOp<DB: Database>: Send + 'static {
    fn apply(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()>;

    /// Clear the persisted row from the mem overlay once durable (default: no-op).
    fn clear_mem(&self, _mem_db: &MemDatabase) {}
}

pub(crate) struct PersistInsert<T: Table> {
    pub(crate) key: T::Key,
    pub(crate) value: T::Value,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistInsert<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>, _mem_db: &MemDatabase) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }

    fn clear_mem(&self, mem_db: &MemDatabase) {
        let _ = mem_db.delete_removed::<T>(&self.key, false);
    }
}

pub(crate) struct PersistRemove<T: Table> {
    pub(crate) key: T::Key,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistRemove<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()> {
        // skip if the key was re-inserted after the remove was queued
        if mem_db.contains_key::<T>(&self.key)? {
            return Ok(());
        }
        txn.remove::<T>(&self.key)
    }
}

/// Hard-delete batch: removes keys from the persistent backend without tombstoning mem.
/// Used by the cold archival producer to prune hot rows that have been archived.
pub(crate) struct PersistRemoveBatch<T: Table> {
    pub(crate) keys: Vec<T::Key>,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistRemoveBatch<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()> {
        for key in &self.keys {
            // skip if the key was re-inserted after the remove was queued
            if mem_db.contains_key::<T>(key)? {
                continue;
            }
            txn.remove::<T>(key)?;
        }
        Ok(())
    }
}

pub(crate) struct PersistClear<T: Table> {
    pub(crate) _phantom: PhantomData<T>,
}

impl<T: Table, DB: Database> PersistOp<DB> for PersistClear<T> {
    fn apply(&self, txn: &mut DB::TXMut<'_>, _mem_db: &MemDatabase) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }
}
