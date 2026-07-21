use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::Debug,
    future::Future,
    iter::Peekable,
    marker::PhantomData,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::mem_db::{MemDatabase, MemDbTx, MemDbTxMut};
use rayls_infrastructure_types::{decode_key, DBIter, DBRawIter, Database, DbTx, DbTxMut, Table};
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

const CACHE_KEEP_TIME_SECS: u64 = 60;
const MAX_CACHE_SIZE: usize = 10000;

pub struct LayeredDbTx<'a, DB: Database> {
    mem_db: MemDbTx<'a>,
    db: DB::TX<'a>,
}

impl<'a, DB: Database> Debug for LayeredDbTx<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTx")
    }
}

impl<'a, DB: Database> DbTx for LayeredDbTx<'a, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if self.mem_db.is_tombstoned::<T>(key) {
            return Ok(None);
        }
        if let Some((_, val)) = self.mem_db.get_no_marked_check::<T>(key) {
            Ok(Some(val))
        } else {
            self.db.get::<T>(key)
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.db.iter::<T>();
        let mem_iter = self.mem_db.iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.raw_iter::<T>();
        let mem_iter = self.mem_db.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned))
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = self.db.skip_to::<T>(key)?;
        let mem_iter = self.mem_db.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Ok(Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned)))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.db.reverse_iter::<T>();
        let mem_iter = self.mem_db.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.reverse_raw_iter::<T>();
        let mem_iter = self.mem_db.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinRawIter::<T>::reverse(db_iter, mem_iter, is_tombstoned))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn disable_long_read_safety(&self) {
        // only the mdbx layer enforces a read-txn timeout; forward to the held
        // inner txn so every cursor derived from it runs exempt. mem has none.
        self.db.disable_long_read_safety();
    }
}

pub struct LayeredDbTxMut<'a, DB: Database> {
    mem_db: MemDbTxMut<'a>,
    _db: DB,
    tx: Sender<DBMessage<DB>>,
}

impl<'a, DB: Database> Debug for LayeredDbTxMut<'a, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTxMut")
    }
}

impl<'a, DB: Database> DbTx for LayeredDbTxMut<'a, DB> {
    fn get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        panic!("DbTx get() should not be called on a DbTxMut!");
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx iter() should not be called on a DbTxMut!");
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx raw_iter() should not be called on a DbTxMut!");
    }

    fn skip_to<T: Table>(&self, _key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        panic!("DbTx skip_to() should not be called on a DbTxMut!");
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx reverse_iter() should not be called on a DbTxMut!");
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx reverse_raw_iter() should not be called on a DbTxMut!");
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        panic!("DbTx last_record() should not be called on a DbTxMut!");
    }

    fn record_prior_to<T: Table>(&self, _key: &T::Key) -> Option<(T::Key, T::Value)> {
        panic!("DbTx record_prior_to() should not be called on a DbTxMut!");
    }

    fn disable_long_read_safety(&self) {
        panic!("DbTx disable_long_read_safety() should not be called on a DbTxMut!");
    }
}

impl<'a, DB: Database> DbTxMut for LayeredDbTxMut<'a, DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.mem_db.commit()?;
        self.tx.send(DBMessage::CommitTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }
}

/// Manage the persistent DB in a background thread with daily compaction.
/// Drop the mem overlay for committed inserts older than `CACHE_KEEP_TIME_SECS` or beyond
/// `MAX_CACHE_SIZE`. Only safe for committed rows: it removes them from the mem layer.
fn evict_committed<DB: Database>(
    committed_inserts: &mut Vec<(Instant, Box<dyn InsertTrait<DB>>)>,
    mem_db: &MemDatabase,
) {
    let total_count = committed_inserts.len();
    let mut remove_count: usize = 0;
    for (instant, insert) in committed_inserts.iter() {
        if instant.elapsed() > Duration::from_secs(CACHE_KEEP_TIME_SECS)
            || total_count - remove_count > MAX_CACHE_SIZE
        {
            insert.clear_insert_mem(mem_db);
            remove_count += 1;
            continue;
        }
        break;
    }
    committed_inserts.drain(..remove_count);
}

fn db_run<DB: Database>(db: DB, mem_db: MemDatabase, rx: Receiver<DBMessage<DB>>) {
    let mut txn = None;
    let mut last_compact = Instant::now();

    let mut committed_inserts: Vec<(Instant, Box<dyn InsertTrait<DB>>)> = Vec::with_capacity(1000);
    // last write/commit failure since the previous CaughtUp, reported by the next persist
    let mut pending_write_error: Option<String> = None;
    if let Err(e) = db.compact() {
        tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB on startup (background): {e}");
    }
    while let Ok(msg) = rx.recv() {
        match msg {
            DBMessage::StartTxn => {
                if let Some((_txn, count)) = &mut txn {
                    *count += 1;
                } else {
                    match db.write_txn() {
                        Ok(ntxn) => txn = Some((ntxn, 1)),
                        Err(e) => {
                            tracing::error!(target: "layered_db_runner", "DB ERROR getting write txn (background): {e}")
                        }
                    }
                }
            }
            DBMessage::CommitTxn => {
                if let Some((current_txn, count)) = txn.take() {
                    if count <= 1 {
                        match current_txn.commit() {
                            Ok(()) => evict_committed(&mut committed_inserts, &mem_db),
                            // surface via persist instead of aborting; rows stay in mem, not lost
                            Err(e) => {
                                tracing::error!(target: "layered_db_runner", "consensus DB commit failed: {e}");
                                pending_write_error = Some(format!("commit: {e}"));
                            }
                        }
                    } else {
                        txn = Some((current_txn, count - 1));
                    }
                }
            }
            DBMessage::Insert(ins) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = ins.insert_txn(txn) {
                        // keep the failed row in mem (not the eviction cache) so it is not lost
                        tracing::error!(target: "layered_db_runner", "DB TXN Insert: {e}");
                        pending_write_error = Some(format!("insert: {e}"));
                    } else {
                        committed_inserts.push((Instant::now(), ins));

                        // Rayls: limit layer growth between commits
                        if committed_inserts.len() > MAX_CACHE_SIZE * 2 {
                            evict_committed(&mut committed_inserts, &mem_db);
                        }
                    }
                } else if let Err(e) = ins.insert(&db) {
                    tracing::error!(target: "layered_db_runner", "DB Insert: {e}");
                    ins.clear_insert_mem(&mem_db);
                    pending_write_error = Some(format!("insert: {e}"));
                }
            }
            DBMessage::Remove(rm) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = rm.remove_txn(txn, &mem_db) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Remove: {e}");
                        pending_write_error = Some(format!("remove: {e}"));
                    }
                } else if let Err(e) = rm.remove(&db, &mem_db) {
                    tracing::error!(target: "layered_db_runner", "DB Remove: {e}");
                    pending_write_error = Some(format!("remove: {e}"));
                }
            }
            DBMessage::Clear(clr) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = clr.clear_table_txn(txn, &mem_db) {
                        tracing::error!("DB TXN Clear table: {e}");
                        pending_write_error = Some(format!("clear: {e}"));
                    }
                } else if let Err(e) = clr.clear_table(&db, &mem_db) {
                    tracing::error!("DB Clear: {e}");
                    pending_write_error = Some(format!("clear: {e}"));
                }
            }
            // NOTE: proves prior messages were applied, not that an open shared txn committed.
            // Safe at shutdown because consensus writers are torn down before persist runs.
            DBMessage::CaughtUp(tx) => {
                let reply: Result<(), String> = match pending_write_error.take() {
                    Some(e) => Err(e),
                    None => Ok(()),
                };
                let _ = tx.send(reply);
            }
            DBMessage::Shutdown => break,
        }
        if last_compact.elapsed() > Duration::from_secs(86_400) {
            last_compact = Instant::now();
            if let Err(e) = db.compact() {
                tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB (background): {e}");
            }
        }
    }
    tracing::info!(target: "layered_db_runner", "Layerd DB thread Shutdown complete");
}

/// In-memory cache layer over a persistent database with background writes.
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: Sender<DBMessage<DB>>,
    thread: Option<Arc<JoinHandle<()>>>,
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!(target: "layered_db", "LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(DBMessage::Shutdown) {
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
        let db_cloned = db.clone();
        let mem_db = MemDatabase::new();
        let mem_db_clone = mem_db.clone();
        let thread =
            Some(Arc::new(std::thread::spawn(move || db_run(db_cloned, mem_db_clone, rx))));
        Self { mem_db, db, tx, thread }
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn>
        = LayeredDbTx<'txn, DB>
    where
        Self: 'txn;

    type TXMut<'txn>
        = LayeredDbTxMut<'txn, DB>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.open_table::<T>()?;
        self.db.open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx { mem_db: self.mem_db.read_txn()?, db: self.db.read_txn()? })
    }

    /// Write transactions overlap and commit when the last one completes.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.tx.send(DBMessage::StartTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(LayeredDbTxMut {
            mem_db: self.mem_db.write_txn()?,
            _db: self.db.clone(),
            tx: self.tx.clone(),
        })
    }

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
        if let Some((_, val)) = self.mem_db.get_marked::<T>(key)? {
            Ok(Some(val))
        } else {
            self.db.get::<T>(key)
        }
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
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
        Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.raw_iter::<T>();
        let mem_iter = self.mem_db.raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinRawIter::<T>::forward(db_iter, mem_iter, is_tombstoned))
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        let db_iter = self.db.skip_to::<T>(key)?;
        let mem_iter = self.mem_db.skip_to::<T>(key)?;
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Ok(Box::new(MergeJoinIter::<T>::forward(db_iter, mem_iter, is_tombstoned)))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        let db_iter = self.db.reverse_iter::<T>();
        let mem_iter = self.mem_db.reverse_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinIter::<T>::reverse(db_iter, mem_iter, is_tombstoned))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        let db_iter = self.db.reverse_raw_iter::<T>();
        let mem_iter = self.mem_db.reverse_raw_iter::<T>();
        let is_tombstoned: Box<dyn Fn(&T::Key) -> bool + '_> =
            Box::new(|k| self.mem_db.is_tombstoned::<T>(k));
        Box::new(MergeJoinRawIter::<T>::reverse(db_iter, mem_iter, is_tombstoned))
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.iter::<T>().take_while(|(k, _)| k < key).last()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn persist(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        let (tx, rx) = oneshot::channel();
        let send_result = self.tx.send(DBMessage::CaughtUp(tx));
        async move {
            match send_result {
                Ok(()) => match rx.await {
                    Ok(Ok(())) => Ok(()),
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
        let r = self
            .tx
            .send(DBMessage::CaughtUp(tx))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"));

        if r.is_ok() {
            loop {
                match rx.try_recv() {
                    Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(100)),
                    Err(TryRecvError::Closed) => break,
                    Ok(Ok(())) => break,
                    Ok(Err(e)) => {
                        tracing::error!(target: "storage", "consensus DB sync_persist: write failed: {e}");
                        break;
                    }
                }
            }
        }
    }
}

trait InsertTrait<DB: Database>: Send + 'static {
    fn insert(&self, db: &DB) -> eyre::Result<()>;
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
    /// Clear the inserted data from the memdb.
    fn clear_insert_mem(&self, mem_db: &MemDatabase);
}

trait RemoveTrait<DB: Database>: Send + 'static {
    fn remove(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()>;
    fn remove_txn(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()>;
}

trait ClearTrait<DB: Database>: Send + 'static {
    fn clear_table(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()>;
    fn clear_table_txn(&self, txn: &mut DB::TXMut<'_>, mem_db: &MemDatabase) -> eyre::Result<()>;
}

struct KeyValueInsert<T: Table> {
    key: T::Key,
    value: T::Value,
}

struct KeyRemove<T: Table> {
    key: T::Key,
}

struct ClearTable<T: Table> {
    _casper: PhantomData<T>,
}

impl<T: Table, DB: Database> InsertTrait<DB> for KeyValueInsert<T> {
    fn insert(&self, db: &DB) -> eyre::Result<()> {
        db.insert::<T>(&self.key, &self.value)
    }
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }
    fn clear_insert_mem(&self, mem_db: &MemDatabase) {
        let _ = mem_db.delete_removed::<T>(&self.key, false);
    }
}

// Tombstones are NOT eagerly cleared from mem after persistent delete;
// doing so races with the main thread's reads (MDBX write not yet visible).
impl<T: Table, DB: Database> RemoveTrait<DB> for KeyRemove<T> {
    fn remove(&self, db: &DB, mem_db: &MemDatabase) -> eyre::Result<()> {
        // skip if key was re-inserted after the remove was queued
        if mem_db.contains_key::<T>(&self.key)? {
            return Ok(());
        }
        db.remove::<T>(&self.key)
    }

    fn remove_txn(
        &self,
        txn: &mut <DB as Database>::TXMut<'_>,
        mem_db: &MemDatabase,
    ) -> eyre::Result<()> {
        if mem_db.contains_key::<T>(&self.key)? {
            return Ok(());
        }
        txn.remove::<T>(&self.key)
    }
}

impl<T: Table, DB: Database> ClearTrait<DB> for ClearTable<T> {
    fn clear_table(&self, db: &DB, _mem_db: &MemDatabase) -> eyre::Result<()> {
        // mem_db already cleared by main thread; re-clearing here would race with new inserts
        db.clear_table::<T>()
    }

    fn clear_table_txn(
        &self,
        txn: &mut <DB as Database>::TXMut<'_>,
        _mem_db: &MemDatabase,
    ) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }
}

enum DBMessage<DB: Database> {
    StartTxn,
    CommitTxn,
    Insert(Box<dyn InsertTrait<DB>>),
    Remove(Box<dyn RemoveTrait<DB>>),
    Clear(Box<dyn ClearTrait<DB>>),
    CaughtUp(oneshot::Sender<Result<(), String>>),
    Shutdown,
}

impl<DB: Database> Debug for DBMessage<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBMessage::StartTxn => write!(f, "StartTxn"),
            DBMessage::CommitTxn => write!(f, "CommitTxn"),
            DBMessage::Insert(_) => write!(f, "Insert"),
            DBMessage::Remove(_) => write!(f, "Remove"),
            DBMessage::Clear(_) => write!(f, "Clear"),
            DBMessage::CaughtUp(_) => write!(f, "CaughtUp"),
            DBMessage::Shutdown => write!(f, "Shutdown"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::LayeredDatabase;
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{
        mdbx::{MdbxConfig, MdbxDatabase},
        test::*,
    };
    use rayls_infrastructure_types::{Database, DbTxMut};
    use std::path::Path;
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
        use rayls_infrastructure_types::{Database, DbTxMut};
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
        use rayls_infrastructure_types::DbTx;

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

    /// Open a `LayeredDatabase<MdbxDatabase>` holding `rows` on the disk layer
    /// only, with a custom read-txn timeout.
    fn open_layered_mdbx_disk_rows(
        path: &Path,
        rows: u64,
        max_read: std::time::Duration,
    ) -> LayeredDatabase<MdbxDatabase> {
        use crate::mdbx::MdbxConfig;
        let cfg = MdbxConfig::default().with_max_read_transaction_duration(Some(max_read));
        // write straight to a bare mdbx db, then reopen behind a fresh (empty)
        // mem layer so the walk is served entirely from the disk-side read txn.
        {
            let db = MdbxDatabase::open_with_config(path, cfg.clone()).expect("open mdbx");
            db.open_table::<TestTable>().expect("open table");
            for i in 1..=rows {
                db.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        let db = MdbxDatabase::open_with_config(path, cfg).expect("reopen mdbx");
        db.open_table::<TestTable>().expect("open table");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>().expect("open table");
        db
    }

    /// Prove `disable_long_read_safety` reaches the inner mdbx txn so a walk
    /// straddling the read-txn timeout is not silently truncated.
    ///
    /// Regression for the leader-count undercount fork: the exemption was a no-op
    /// on `LayeredDatabase`, so the monitor reset the walk's txn mid-scan and the
    /// iterator stopped early. Slow (~3s): drives the real mdbx timeout monitor.
    #[test]
    fn test_layereddb_disable_long_read_safety_survives_midwalk_timeout() {
        use rayls_infrastructure_types::DbTx;
        use std::time::Duration;

        const ROWS: u64 = 64;
        let max_read = Duration::from_secs(1);
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_layered_mdbx_disk_rows(temp_dir.path(), ROWS, max_read);

        // exempt walker: opt out of the timeout, then start iterating so the read
        // snapshot is live before we straddle the deadline.
        let exempt = db.read_txn().unwrap();
        exempt.disable_long_read_safety();
        let mut walk = exempt.reverse_iter::<TestTable>();
        let first = walk.next().expect("first row before timeout");

        // control walker: identical but not exempted, proving the timeout fires.
        let control = db.read_txn().unwrap();

        // hold both open past max_read so the monitor resets every active,
        // non-exempt read txn mid-flight, exactly like the ~30s tally walk.
        std::thread::sleep(max_read + Duration::from_secs(2));

        // fix proof: the exempt walk finishes in full across the timeout boundary.
        let rest = walk.count() as u64;
        assert_eq!(rest + 1, ROWS, "exempt walk must not truncate across the timeout");
        assert_eq!(first.0, ROWS);

        // control: the un-exempt txn was reset, so a disk read now errors. without
        // this a green exempt case could be a false positive (monitor never ran).
        assert!(
            control.get::<TestTable>(&ROWS).is_err(),
            "un-exempt read txn must be reset by the timeout monitor",
        );
    }
}
