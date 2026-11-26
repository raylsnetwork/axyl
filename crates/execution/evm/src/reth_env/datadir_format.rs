//! Refuse an execution database in the storage format of reth 1.x.
//!
//! reth 2.0 changed how some tables are stored: the trie tables use packed keys, and the storage
//! change sets and the storage history use plain slot keys. reth has no marker for this in the
//! datadir. Code from reth 2.0 opens a reth 1.x datadir without an error and reads those tables
//! wrong. So the node must find out by itself, before it uses the datadir.
//!
//! The sign is the key length in the `AccountsTrie` table:
//! - reth 2.0: every key is a packed path, always [`PACKED_TRIE_KEY_LEN`] bytes
//! - reth 1.x: one byte per step of the path, so the length varies; the first rows are nodes near
//!   the top of the trie, with keys of one or two bytes
//!
//! The check runs in [`RethEnv::new_database`](crate::reth_env::RethEnv::new_database), the one
//! place that opens the execution database for writing. That covers the node start, the schedule
//! gate, the output of `rayls-replay` and the cold migration.

use eyre::eyre;
use reth_db::{
    cursor::DbCursorRO as _,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    open_db_read_only, tables,
    transaction::DbTx as _,
    Database as _, DatabaseEnv, RawTable,
};
use std::path::Path;
use tracing::debug;

/// Length of every `AccountsTrie` key in the storage format of reth 2.0: 32 bytes of packed path
/// and one length byte.
const PACKED_TRIE_KEY_LEN: usize = 33;

/// How many rows the check reads. In the old format the first row already has another length
/// (a real reth 1.11 datadir starts with a key of one byte). A few more rows cost nothing.
const KEYS_TO_CHECK: usize = 16;

/// Check the datafile at `db_path` before it is opened for writing, with a read-only open.
///
/// The normal open (`init_db`) already writes: it records the client version and drops tables
/// that reth 2.0 no longer has. An old datadir must be refused before that, so that it stays as
/// it was.
///
/// Returns `Ok(true)` when the format was checked and is fine, and `Ok(false)` when the check
/// could not run here: there is no datafile yet, or the read-only open or the table read failed
/// (the database is in use, needs a recovery, or has no tables yet). In that case the caller
/// goes on with the normal open and calls [`check_open_database`] right after it. A node start
/// is never blocked only because this early check could not run.
///
/// An old datadir gives an error.
pub(crate) fn check_before_open(db_path: &Path, page_size: Option<usize>) -> eyre::Result<bool> {
    let dat = db_path.join(rayls_infrastructure_utils::mdbx::MDBX_DAT);
    if std::fs::metadata(&dat).map(|meta| meta.len()).unwrap_or(0) == 0 {
        return Ok(false);
    }

    // exclusive, like the reader of `rayls-replay`: data files are not touched (MDBX only
    // sets the length of its own lock file)
    let args = DatabaseArguments::default()
        .with_geometry_page_size(page_size)
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded))
        .with_exclusive(Some(true));
    let db = match open_db_read_only(db_path, args) {
        Ok(db) => db,
        Err(error) => {
            debug!(target: "rayls::reth", path = ?db_path, %error, "read-only format check skipped, the database did not open; checking after the normal open");
            return Ok(false);
        }
    };
    match old_format_key_len(&db) {
        Ok(None) => Ok(true),
        Ok(Some(key_len)) => Err(old_format_error(db_path, key_len, false)),
        Err(error) => {
            debug!(target: "rayls::reth", path = ?db_path, %error, "read-only format check skipped, the trie table could not be read; checking after the normal open");
            Ok(false)
        }
    }
}

/// Check a database that is already open. Used when [`check_before_open`] could not run.
///
/// At this point the normal open has made its small writes, but nothing has read the trie
/// tables yet, and the consistency check, the heal and the unwind have not run.
pub(crate) fn check_open_database(db: &DatabaseEnv, db_path: &Path) -> eyre::Result<()> {
    match old_format_key_len(db)? {
        None => Ok(()),
        Some(key_len) => Err(old_format_error(db_path, key_len, true)),
    }
}

/// The length of the first `AccountsTrie` key that is not in the reth 2.0 format, among the
/// first [`KEYS_TO_CHECK`] rows. `None` when all of them are, and for an empty table.
fn old_format_key_len(db: &DatabaseEnv) -> eyre::Result<Option<usize>> {
    let tx = db.tx()?;
    let mut cursor = tx.cursor_read::<RawTable<tables::AccountsTrie>>()?;
    let mut row = cursor.first()?;
    for _ in 0..KEYS_TO_CHECK {
        let Some((key, _)) = row else { break };
        let key_len = key.raw_key().len();
        if key_len != PACKED_TRIE_KEY_LEN {
            return Ok(Some(key_len));
        }
        row = cursor.next()?;
    }
    Ok(None)
}

/// The error for a datadir in the old format. `opened_for_writing` says whether the normal open
/// already ran.
fn old_format_error(db_path: &Path, key_len: usize, opened_for_writing: bool) -> eyre::Report {
    let state = if opened_for_writing {
        "The database was opened, but nothing in it was repaired or unwound."
    } else {
        "The datadir was not changed."
    };
    eyre!(
        "{} is in the storage format of reth 1.x: it was written by an older version of this \
         node. This version runs on reth 2.0, which stores the trie tables in another format \
         and would read this datadir wrong. {state} Build a new datadir from it with \
         `rayls-replay`, which only reads the old one, or sync this node again. (An \
         AccountsTrie key has {key_len} bytes, the reth 2.0 format has \
         {PACKED_TRIE_KEY_LEN}.)",
        db_path.display()
    )
}
