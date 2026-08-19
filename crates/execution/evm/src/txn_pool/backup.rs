//! On-disk snapshot of a worker pool across a graceful restart.
//!
//! On the sharded mempool each transaction is owned by exactly one worker, so a restart that
//! dropped the pool would lose sealed-but-uncommitted transactions no peer can re-supply.
//! Transactions and their in-flight marks are written as sibling JSON files on shutdown and
//! replayed through normal validation on the next boot.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use futures::{stream::FuturesUnordered, StreamExt as _};
use rayls_infrastructure_types::Encodable2718 as _;
use reth_transaction_pool::{maintain::TxBackup, TransactionPool as _};
use serde::{
    de::{self, DeserializeSeed, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer as _},
};
use tracing::{error, info, warn};

use super::{bytes_to_txn, WorkerTxPool};
use crate::in_flight::{MarkBackup, MarkRole, MARK_BACKUP_VERSION};

/// Concurrent add-transaction validations while reloading the pool backup on boot.
///
/// Bounds the validation futures held at once, so a large backup does not materialize the whole
/// pool in memory during reload.
const BACKUP_RELOAD_CONCURRENCY: usize = 1024;

/// Deserialized rows allowed to wait for validation; with [`BACKUP_RELOAD_CONCURRENCY`] this
/// bounds reload memory.
const BACKUP_RELOAD_BUFFER: usize = BACKUP_RELOAD_CONCURRENCY;

impl WorkerTxPool {
    /// Saves every pending and queued transaction (all origins) to the backup file, returning the
    /// count.
    ///
    /// Runs on graceful shutdown only; transactions admitted after the last completed snapshot
    /// are lost on kill -9 or panic. The write is tmp-then-rename so an interrupted write cannot
    /// leave a truncated file, and an empty pool removes any stale file so an old snapshot
    /// cannot replay over it. Errors are logged and swallowed: persistence must never block
    /// shutdown.
    pub fn save_backup(&self) -> usize {
        let started = Instant::now();
        // The upstream pool API returns snapshots as Vecs. Keep only those Arc snapshots; do
        // not also materialize a Vec<TxBackup> containing a second copy of every RLP payload.
        let pending = self.pending_transactions();
        let queued = self.queued_transactions();
        let total = pending.len() + queued.len();
        let path = self.backup_path.as_ref();

        if total == 0 {
            remove_backup_file(path);
            return 0;
        }

        let write_result = write_atomically(path, |writer| {
            let mut serializer = serde_json::Serializer::new(writer);
            let mut sequence = serializer.serialize_seq(Some(total)).map_err(io::Error::other)?;
            for tx in pending.iter().chain(queued.iter()) {
                sequence
                    .serialize_element(&TxBackup {
                        rlp: tx.transaction.transaction().encoded_2718().into(),
                        origin: tx.origin,
                    })
                    .map_err(io::Error::other)?;
            }
            sequence.end().map_err(io::Error::other)
        });
        if let Err(err) = write_result {
            warn!(target: "rayls::txpool", %err, ?path, "failed to write txpool backup");
            return 0;
        }
        let bytes = std::fs::metadata(path).map(|metadata| metadata.len()).ok();
        info!(target: "rayls::txpool", txs = total, ?bytes, elapsed_ms = started.elapsed().as_millis(), ?path, "saved txpool backup");
        total
    }

    /// Reloads the backup written by the previous graceful shutdown, then deletes the file;
    /// returns the number of transactions accepted back into the pool.
    ///
    /// Every transaction re-enters through the normal validation path with its saved origin, so
    /// stale entries (already executed, underfunded) are rejected instead of trusted. Deleting
    /// after the reload makes replay at-most-once; a crash in between replays in full next boot,
    /// where duplicates die on the same validation. A corrupt or unreadable file is logged and
    /// deleted so it cannot wedge boot, with undecodable rows skipped individually.
    pub async fn load_backup(&self) -> usize {
        let started = Instant::now();
        // JSON parsing and disk I/O stay on a blocking worker; the bounded channel backpressures
        // parsing against validation so neither the file bytes nor all rows are resident at once.
        let path = self.backup_path.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(BACKUP_RELOAD_BUFFER);
        let reader = tokio::task::spawn_blocking(move || stream_backup_file(&path, sender));
        let mut adds = FuturesUnordered::new();
        let mut receiver_open = true;
        let mut accepted = 0;
        let mut decoded = 0;
        let mut decode_failed = 0;

        while receiver_open || !adds.is_empty() {
            tokio::select! {
                row = receiver.recv(), if receiver_open && adds.len() < BACKUP_RELOAD_CONCURRENCY => {
                    match row {
                        Some(backup) => match bytes_to_txn(&backup.rlp) {
                            Ok(tx) => {
                                decoded += 1;
                                adds.push(self.pool.add_transaction(backup.origin, tx));
                            }
                            Err(_) => decode_failed += 1,
                        },
                        None => receiver_open = false,
                    }
                }
                result = adds.next(), if !adds.is_empty() => {
                    if result.expect("non-empty futures set").is_ok() {
                        accepted += 1;
                    }
                }
            }
        }

        let read = match reader.await {
            Ok(Ok(Some(read))) => read,
            Ok(Ok(None)) => return 0,
            Ok(Err(err)) => {
                warn!(target: "rayls::txpool", %err, ?self.backup_path, "corrupt or unreadable txpool backup; discarding");
                remove_backup_file(&self.backup_path);
                return accepted;
            }
            Err(err) => {
                error!(target: "rayls::txpool", %err, "txpool backup read task panicked");
                return accepted;
            }
        };
        let path = self.backup_path.as_ref();
        info!(target: "rayls::txpool", total = read.rows, bytes = read.bytes, decoded, decode_failed, accepted, elapsed_ms = started.elapsed().as_millis(), ?path, "reloaded txpool backup");
        remove_backup_file(path);
        accepted
    }
}

impl WorkerTxPool {
    /// Returns the mark backup's path, a sibling of the transaction backup.
    fn mark_backup_path(&self) -> PathBuf {
        self.backup_path.with_file_name("txpool-in-flight.json")
    }

    /// Writes the in-flight mark backup next to the transaction backup, returning the count.
    ///
    /// Only a forwarding snapshot is persisted; a sealing snapshot is discarded on purpose:
    /// the proposer queue is not persistent, so a batch still queued at shutdown can never
    /// commit after reboot, and restoring its marks would suppress exactly the head txs the
    /// committed-state seq recovery re-seals, wedging the builder into park/force-drain cycles.
    /// Those txs re-seal on reboot and duplicate absorption bounds the churn. Same contract as
    /// [`Self::save_backup`]: graceful shutdown only, tmp-then-rename, errors logged and
    /// swallowed.
    pub fn save_mark_backup(&self) -> usize {
        let path = self.mark_backup_path();
        let backup = match self.in_flight_tracker.snapshot() {
            Some(backup) if backup.role == MarkRole::Forwarding && !backup.marks.is_empty() => {
                backup
            }
            _ => {
                remove_backup_file(&path);
                return 0;
            }
        };
        if let Err(err) = write_atomically(&path, |writer| {
            serde_json::to_writer(writer, &backup).map_err(io::Error::other)
        }) {
            warn!(target: "rayls::txpool", %err, ?path, "failed to write mark backup");
            return 0;
        }
        info!(
            target: "rayls::txpool",
            marks = backup.marks.len(),
            role = ?backup.role,
            ?path,
            "saved in-flight mark backup"
        );
        backup.marks.len()
    }

    /// Reloads the mark backup written by the previous graceful shutdown, then deletes it;
    /// returns the number of marks stashed.
    ///
    /// The marks are stashed on the tracker, not installed: the first arming of the matching
    /// role consumes them (see [`InFlightTracker::stash_restore`]). Deleting after the reload
    /// makes replay at-most-once; an unknown version or corrupt file is logged and deleted so
    /// it cannot wedge boot. Marks whose txs the pool reload rejected self-heal through the
    /// membership reconcile.
    pub async fn load_mark_backup(&self) -> usize {
        let path = self.mark_backup_path();
        let read = tokio::task::spawn_blocking(move || read_mark_backup_file(&path)).await;
        let backup = match read {
            Ok(Some(backup)) => backup,
            Ok(None) => return 0,
            Err(err) => {
                error!(target: "rayls::txpool", %err, "mark backup read task panicked");
                return 0;
            }
        };
        let total = backup.marks.len();
        info!(target: "rayls::txpool", total, role = ?backup.role, "stashed in-flight mark backup");
        self.in_flight_tracker.stash_restore(backup);
        remove_backup_file(&self.mark_backup_path());
        total
    }
}

/// Reads and parses the mark backup, deleting it on any malformation so boot cannot wedge.
fn read_mark_backup_file(path: &Path) -> Option<MarkBackup> {
    if !path.exists() {
        return None;
    }
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(err) => {
            warn!(target: "rayls::txpool", %err, ?path, "failed to read mark backup");
            remove_backup_file(path);
            return None;
        }
    };
    let backup = match serde_json::from_slice::<MarkBackup>(&data) {
        Ok(backup) => backup,
        Err(err) => {
            warn!(target: "rayls::txpool", %err, ?path, "failed to decode mark backup; discarding");
            remove_backup_file(path);
            return None;
        }
    };
    if backup.version != MARK_BACKUP_VERSION {
        warn!(
            target: "rayls::txpool",
            version = backup.version,
            ?path,
            "unknown mark backup version; discarding"
        );
        remove_backup_file(path);
        return None;
    }
    Some(backup)
}

/// Deletes a backup file, treating an already-missing file as success.
fn remove_backup_file(path: &Path) {
    if let Err(err) = std::fs::remove_file(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            warn!(target: "rayls::txpool", %err, ?path, "failed to remove txpool backup");
        }
    }
}

/// Writes to a uniquely named sibling temporary file and renames it over `path`.
///
/// Syncing both the file and (on Unix) its directory lets a shutdown snapshot survive a power
/// loss right after the rename.
fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        write(&mut writer)?;
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|err| err.error)?;
    sync_parent_directory(parent)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_: &Path) -> io::Result<()> {
    Ok(())
}

/// Size of a backup file that was read to the end.
struct BackupRead {
    /// Rows decoded from the JSON array.
    rows: usize,
    /// File size in bytes.
    bytes: u64,
}

/// Streams the backup's JSON array row by row into the bounded reload channel.
///
/// `Ok(None)` means no backup file exists; a decode error mid-file is reported after the rows
/// before it were already sent.
fn stream_backup_file(
    path: &Path,
    sender: tokio::sync::mpsc::Sender<TxBackup>,
) -> Result<Option<BackupRead>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to open {}: {err}", path.display())),
    };
    let bytes = file.metadata().map(|metadata| metadata.len()).unwrap_or_default();
    let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
    BackupSequence { sender, rows: 0 }
        .deserialize(&mut deserializer)
        .map(|rows| Some(BackupRead { rows, bytes }))
        .map_err(|err| format!("failed to decode {}: {err}", path.display()))
}

/// A serde seed that forwards each decoded row to the reload channel instead of collecting.
struct BackupSequence {
    sender: tokio::sync::mpsc::Sender<TxBackup>,
    rows: usize,
}

impl<'de> Visitor<'de> for BackupSequence {
    type Value = usize;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON array of transaction backups")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(backup) = sequence.next_element::<TxBackup>()? {
            self.sender
                .blocking_send(backup)
                .map_err(|_| de::Error::custom("backup reload receiver dropped"))?;
            self.rows += 1;
        }
        Ok(self.rows)
    }
}

impl<'de> DeserializeSeed<'de> for BackupSequence {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_existing_file() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("txpool.json");

        write_atomically(&path, |writer| writer.write_all(b"first")).expect("first atomic write");
        write_atomically(&path, |writer| writer.write_all(b"second"))
            .expect("replacement atomic write");

        assert_eq!(std::fs::read(&path).expect("read replacement"), b"second");
        assert!(directory
            .path()
            .read_dir()
            .expect("read directory")
            .all(|entry| entry.expect("directory entry").path() == path));
    }

    #[test]
    fn streaming_reader_accepts_legacy_json_array() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("txpool.json");
        std::fs::write(&path, b"[]").expect("write legacy backup");
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        let read =
            stream_backup_file(&path, sender).expect("read legacy backup").expect("backup exists");
        assert_eq!(read.rows, 0);
        assert_eq!(read.bytes, 2);
        assert!(receiver.try_recv().is_err());
    }
}
