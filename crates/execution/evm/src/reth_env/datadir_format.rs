//! Refuse an execution database that was written by reth 1.x.
//!
//! reth 2.0 changed how some tables are stored: the trie tables use packed keys, and the storage
//! change sets and the storage history use plain slot keys. Code from reth 2.0 opens a reth 1.x
//! datadir without an error and reads those tables wrong. So the node must find out by itself,
//! before it uses the datadir.
//!
//! The sign is reth's own `VersionHistory` table. Every open for writing records the reth
//! version there when it differs from the last row. A datadir is in the old format when ANY row
//! names a reth version before [`FIRST_SUPPORTED_RETH_MAJOR`]:
//! - There is no conversion in place. A datadir that reth 1.x once wrote stays in the old format.
//! - The last row alone is not enough. A reth 2.0 binary that opens an old datadir for writing adds
//!   its own row, and from then on the last row names reth 2.0.
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
    Database as _, DatabaseEnv,
};
use std::path::Path;
use tracing::debug;

/// The first reth major version that writes the storage format this node reads.
const FIRST_SUPPORTED_RETH_MAJOR: u64 = 2;

/// Check the datafile at `db_path` before it is opened for writing, with a read-only open.
///
/// The normal open (`init_db`) already writes: it records the client version and drops tables
/// that reth 2.0 no longer has. An old datadir must be refused before that, so that it stays as
/// it was.
///
/// Returns `Ok(true)` when the check ran and found no old version, and `Ok(false)` when it
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
    match old_reth_version(&db) {
        Ok(None) => Ok(true),
        Ok(Some(version)) => Err(old_format_error(db_path, &version, false)),
        Err(error) => {
            debug!(target: "rayls::reth", path = ?db_path, %error, "read-only format check skipped, the version table could not be read; checking after the normal open");
            Ok(false)
        }
    }
}

/// Check a database that is already open. Used when [`check_before_open`] could not run.
///
/// At this point the normal open has made its small writes, one of them a `VersionHistory` row
/// for this reth version. That does not hide an old datadir, because any older row counts.
/// Nothing has read the trie tables yet, and the consistency check, the heal and the unwind
/// have not run.
pub(crate) fn check_open_database(db: &DatabaseEnv, db_path: &Path) -> eyre::Result<()> {
    match old_reth_version(db)? {
        None => Ok(()),
        Some(version) => Err(old_format_error(db_path, &version, true)),
    }
}

/// The first `VersionHistory` row with a reth version before [`FIRST_SUPPORTED_RETH_MAJOR`].
/// `None` when there is none. A database without rows cannot be judged and gives `None` too.
fn old_reth_version(db: &DatabaseEnv) -> eyre::Result<Option<String>> {
    let tx = db.tx()?;
    let mut cursor = tx.cursor_read::<tables::VersionHistory>()?;
    let mut row = cursor.first()?;
    while let Some((_, client)) = row {
        if major_version(&client.version).is_some_and(|major| major < FIRST_SUPPORTED_RETH_MAJOR) {
            return Ok(Some(client.version));
        }
        row = cursor.next()?;
    }
    Ok(None)
}

/// The major number of a version like `1.11.0` or `v2.0.0-rc.1`. `None` when the text does not
/// start with a number; such a row is not counted as old.
fn major_version(version: &str) -> Option<u64> {
    let version = version.trim().trim_start_matches('v');
    let digits = version.split(|c: char| !c.is_ascii_digit()).next()?;
    digits.parse().ok()
}

/// The error for a datadir in the old format. `opened_for_writing` says whether the normal open
/// already ran.
fn old_format_error(db_path: &Path, version: &str, opened_for_writing: bool) -> eyre::Report {
    let state = if opened_for_writing {
        "The database was opened, but nothing in it was repaired or unwound."
    } else {
        "The datadir was not changed."
    };
    eyre!(
        "{} was written by reth {version}, so it is in the storage format of reth 1.x. This \
         version runs on reth {FIRST_SUPPORTED_RETH_MAJOR}.0, which stores the trie tables in \
         another format and would read this datadir wrong. {state} Build a new datadir from it \
         with `rayls-replay`, which only reads the old one, or sync this node again.",
        db_path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::major_version;

    #[test]
    fn major_version_reads_the_first_number() {
        assert_eq!(major_version("1.11.0"), Some(1));
        assert_eq!(major_version("2.0.0"), Some(2));
        assert_eq!(major_version("v2.1.3-rc.1"), Some(2));
        assert_eq!(major_version(" 10.4.0 "), Some(10));
        assert_eq!(major_version("0.9"), Some(0));
        assert_eq!(major_version(""), None);
        assert_eq!(major_version("reth"), None);
    }
}
