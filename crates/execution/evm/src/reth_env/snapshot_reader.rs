//! Read-only access to the execution data of a source datadir, for `rayls-replay`.
//!
//! The replay tool takes the plan from a source datadir and rebuilds the chain in a new one. The
//! source may come from an older reth version. The normal node start (`RethEnv::new`) must not
//! run on it: it opens everything for writing, runs reth's consistency check, the history heal
//! and, if needed, an unwind. With code from a newer reth those steps can damage an old datadir.
//!
//! [`SnapshotReader`] opens only what the replay reads, and opens it read-only:
//! - `db/` (MDBX) with `MDBX_RDONLY` and `MDBX_EXCLUSIVE`
//! - `static_files/` with reth's read-only static file provider
//!
//! It does not open RocksDB. reth's read-only RocksDB is a "secondary instance" that creates a
//! temp directory inside the datadir, and none of the reads below need RocksDB.
//!
//! What it reads has the same layout in reth 1.11 and reth 2.0: headers (static files), the
//! `BlockWithdrawals` table and the `Finish` stage checkpoint.

use crate::{error::RaylsRethResult, traits::RaylsPrimitives};
use alloy::primitives::Address;
use eyre::{eyre, WrapErr as _};
use rayls_infrastructure_types::{BlockNumber, ExecHeader, SealedHeader};
use reth_chainspec::{ChainSpec as RethChainSpec, EthereumHardforks as _};
use reth_db::{
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    open_db_read_only,
    tables::{BlockWithdrawals, StageCheckpoints},
    transaction::DbTx as _,
    Database as _, DatabaseEnv,
};
use reth_provider::{
    providers::StaticFileProvider, static_file::StaticFileSegment, HeaderProvider as _,
};
use reth_stages::StageId;
use std::{ops::RangeInclusive, path::Path, sync::Arc};
use tracing::{info, warn};

/// Read-only view of a source datadir. Cheap to clone; clones share the open files.
#[derive(Clone, Debug)]
pub struct SnapshotReader {
    /// The source `db/`, opened read-only and exclusive.
    db: Arc<DatabaseEnv>,
    /// The source `static_files/`, opened read-only.
    static_files: StaticFileProvider<RaylsPrimitives>,
    /// Only used for the Shanghai rule in [`Self::block_withdrawals`].
    chain: Arc<RethChainSpec>,
    /// The last block the source fully saved: its `Finish` stage checkpoint.
    tip: BlockNumber,
}

impl SnapshotReader {
    /// Open the source at `rayls_datadir` (the directory that holds `db/` and `static_files/`).
    ///
    /// Nothing is repaired. A source that cannot be read as it is gives an error:
    /// - another process has the database open (a running node)
    /// - MDBX needs a recovery (the node was killed, or the files were copied from a running node)
    /// - there is no `Finish` checkpoint, or the headers end before it
    ///
    /// The tip is the `Finish` checkpoint. reth writes the header static files before it commits
    /// the MDBX transaction of the same blocks, so after a kill the headers can be ahead of the
    /// checkpoint. A node start cuts them back to the checkpoint. The reader gets the same result
    /// without a write: it does not serve headers above the checkpoint.
    pub fn open<P: AsRef<Path>>(rayls_datadir: P, chain: Arc<RethChainSpec>) -> eyre::Result<Self> {
        let rayls_datadir = rayls_datadir.as_ref();
        let db_path = rayls_datadir.join("db");
        let static_files_path = rayls_datadir.join("static_files");
        if !db_path.join("mdbx.dat").is_file() {
            return Err(eyre!(
                "no execution database at {} (expected mdbx.dat); pass the node datadir, the \
                 directory that holds db/, static_files/ and consensus-db/",
                db_path.display()
            ));
        }
        if !static_files_path.is_dir() {
            return Err(eyre!("no static files directory at {}", static_files_path.display()));
        }

        // Exclusive: a database that a running node holds is refused (MDBX_BUSY), so the replay
        // never reads a source that still changes.
        // Unbounded: reth's 5 minute limit for read transactions protects a writer in the same
        // process. There is none here.
        let args = DatabaseArguments::default()
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded))
            .with_exclusive(Some(true));
        let db = open_db_read_only(&db_path, args)
            .map_err(|e| explain_read_only_open_error(&db_path, e))?;
        let static_files = StaticFileProvider::<RaylsPrimitives>::read_only(&static_files_path)
            .wrap_err_with(|| {
                format!("open static files read-only at {}", static_files_path.display())
            })?;

        let finish = db
            .tx()?
            .get::<StageCheckpoints>(StageId::Finish.as_str().to_string())?
            .map(|checkpoint| checkpoint.block_number)
            .ok_or_else(|| {
                eyre!(
                    "{} has no Finish stage checkpoint, so its last saved block is unknown",
                    db_path.display()
                )
            })?;
        let highest_header = static_files
            .get_highest_static_file_block(StaticFileSegment::Headers)
            .ok_or_else(|| eyre!("no header static files in {}", static_files_path.display()))?;
        if highest_header < finish {
            return Err(eyre!(
                "the source is not complete: the database says the last saved block is {finish}, \
                 but the header static files in {} end at block {highest_header}",
                static_files_path.display()
            ));
        }
        if highest_header > finish {
            warn!(
                target: "rayls::reth",
                finish,
                highest_header,
                "the source has headers above its last saved block (the node was stopped in the \
                 middle of a save); they are ignored"
            );
        }
        if static_files.sealed_header(finish)?.is_none() {
            return Err(eyre!(
                "the header of the last saved block {finish} cannot be read from {}",
                static_files_path.display()
            ));
        }

        info!(
            target: "rayls::reth",
            path = %rayls_datadir.display(),
            tip = finish,
            "opened the source datadir read-only"
        );
        Ok(Self { db: Arc::new(db), static_files, chain, tip: finish })
    }

    /// The last block of the source: its `Finish` stage checkpoint.
    pub fn last_block_number(&self) -> BlockNumber {
        self.tip
    }

    /// The sealed header of block `number`. `None` above the tip.
    pub fn sealed_header_by_number(
        &self,
        number: BlockNumber,
    ) -> RaylsRethResult<Option<SealedHeader>> {
        if number > self.tip {
            return Ok(None);
        }
        Ok(self.static_files.sealed_header(number)?)
    }

    /// The header of block `number`. `None` above the tip.
    pub fn header_by_number(&self, number: BlockNumber) -> RaylsRethResult<Option<ExecHeader>> {
        if number > self.tip {
            return Ok(None);
        }
        Ok(self.static_files.header_by_number(number)?)
    }

    /// The sealed headers of `range`, in order. The range is cut at the tip, so the result can
    /// be shorter than the range, or empty.
    pub fn blocks_for_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> RaylsRethResult<Vec<SealedHeader>> {
        let (start, end) = (*range.start(), (*range.end()).min(self.tip));
        if start > end {
            return Ok(Vec::new());
        }
        Ok(self.static_files.sealed_headers_range(start..=end)?)
    }

    /// The withdrawals of block `number` as `(address, amount)` pairs. `None` if the block is not
    /// in the source, an empty list if the block has no withdrawals.
    ///
    /// This gives the same values as reading the whole block through reth (`EthStorage`): reth
    /// keeps withdrawals in the `BlockWithdrawals` table, writes a row only for a block that has
    /// some, and reads the table only for a block at or after Shanghai.
    pub fn block_withdrawals(
        &self,
        number: BlockNumber,
    ) -> RaylsRethResult<Option<Vec<(Address, u64)>>> {
        let Some(header) = self.header_by_number(number)? else {
            return Ok(None);
        };
        if !self.chain.is_shanghai_active_at_timestamp(header.timestamp) {
            return Ok(Some(Vec::new()));
        }
        let tx = self.db.tx().map_err(reth_provider::ProviderError::from)?;
        let row = tx.get::<BlockWithdrawals>(number).map_err(reth_provider::ProviderError::from)?;
        Ok(Some(
            row.map(|stored| {
                stored.withdrawals.iter().map(|w| (w.address, w.amount)).collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        ))
    }
}

/// Turn the error of a read-only MDBX open into one that says what to do.
fn explain_read_only_open_error(db_path: &Path, error: eyre::Report) -> eyre::Report {
    let text = format!("{error:#}");
    match rayls_infrastructure_utils::mdbx::read_only_open_problem(&text) {
        Some(problem) => eyre!("{}: {problem}", db_path.display()),
        None => error.wrap_err(format!("open {} read-only", db_path.display())),
    }
}
