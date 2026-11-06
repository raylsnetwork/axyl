// SPDX-License-Identifier: BUSL-1.1
//! `state-root-check`: rebuild the execution state root from the hashed leaves alone and
//! compare it with the root sealed in the last persisted header.
//!
//! The node computes roots incrementally, reusing the branch nodes it stored for earlier
//! blocks. A walk that trusts a stored node it should have looked past seals a root that no
//! longer describes the leaves (reth PR 28264 fixed one such walk). Rebuilding from the leaves
//! with a no-op trie cursor reads no stored node at all, so it is the one computation such a
//! walk cannot influence: if the two roots differ, the sealed header lies about the state.
//!
//! This opens the node's reth datadir (`db/` and `static_files/`), not its consensus-db.

use crate::{
    node_db::{default_label, split_label},
    report::{code, Verdict},
};
use alloy::primitives::B256;
use eyre::{eyre, WrapErr as _};
use reth_db::{
    mdbx::DatabaseArguments, open_db_read_only, tables, transaction::DbTx as _, Database as _,
};
use reth_ethereum_primitives::EthPrimitives;
use reth_provider::{providers::StaticFileProvider, HeaderProvider as _, StaticFileSegment};
use reth_trie::{trie_cursor::noop::NoopTrieCursorFactory, StateRoot};
use reth_trie_db::DatabaseHashedCursorFactory;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Instant,
};

/// The `Finish` stage checkpoint: the last block whose state the node persisted.
const FINISH_STAGE: &str = "Finish";

#[derive(Debug, Serialize)]
pub struct StateRootReport {
    pub nodes: Vec<StateRootNodeView>,
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub struct StateRootNodeView {
    pub node: String,
    /// The reth datadir that was read (holding `db/` and `static_files/`).
    pub path: String,
    /// The block whose header is compared: the `Finish` stage checkpoint, or the highest header
    /// in static files when no checkpoint is stored.
    pub block_number: u64,
    /// Where `block_number` came from: `checkpoint` or `static-files`.
    pub block_source: &'static str,
    /// `state_root` of that block's sealed header.
    pub header_root: B256,
    /// The root rebuilt from `HashedAccounts` and `HashedStorages` with no stored trie node.
    pub leaves_root: B256,
    pub accounts: usize,
    pub storage_slots: usize,
    pub matches: bool,
    pub elapsed_ms: u64,
}

/// Rebuilds the state root from leaves on every node in `specs` (`[LABEL=]DATADIR`) and compares
/// it with the sealed header's root.
///
/// Fails (rather than reporting) when a datadir cannot be opened or has no header for the
/// persisted block, so a wrong path is never mistaken for a healthy node.
pub fn state_root_check(specs: &[String]) -> eyre::Result<StateRootReport> {
    let mut seen: HashMap<PathBuf, String> = HashMap::new();
    let mut nodes = Vec::with_capacity(specs.len());
    for spec in specs {
        let (label, raw_path) = split_label(spec);
        let datadir = resolve_reth_datadir(Path::new(raw_path))?;
        let label = label.map(str::to_owned).unwrap_or_else(|| default_label(&datadir));
        let canonical = std::fs::canonicalize(&datadir).unwrap_or_else(|_| datadir.clone());
        if let Some(first) = seen.insert(canonical.clone(), label.clone()) {
            return Err(eyre!(
                "datadir {} given twice (as {first} and {label})",
                canonical.display()
            ));
        }
        nodes.push(
            check_node(label.clone(), &datadir)
                .wrap_err_with(|| format!("{label}: state root check at {}", datadir.display()))?,
        );
    }

    let mismatched: Vec<u64> =
        nodes.iter().filter(|n| !n.matches).map(|n| n.block_number).collect();
    let verdict_code = if nodes.is_empty() {
        code::EMPTY
    } else if mismatched.is_empty() {
        code::OK
    } else {
        code::BROKEN
    };
    let verdict = Verdict::new(verdict_code)
        .num("nodes", nodes.len())
        .count("mismatched", mismatched.len())
        .opt("first", mismatched.first().copied());
    Ok(StateRootReport { nodes, verdict })
}

/// Resolves a node datadir (or its `consensus-db` directory) to the directory holding reth's
/// `db/mdbx.dat` and `static_files/`.
fn resolve_reth_datadir(path: &Path) -> eyre::Result<PathBuf> {
    if path.join("db").join("mdbx.dat").is_file() {
        return Ok(path.to_path_buf());
    }
    if path.file_name().is_some_and(|name| name == "consensus-db") {
        if let Some(parent) = path.parent().filter(|p| p.join("db").join("mdbx.dat").is_file()) {
            return Ok(parent.to_path_buf());
        }
    }
    Err(eyre!(
        "no db/mdbx.dat under {}; pass the node datadir (the directory holding db/, \
         static_files/ and consensus-db/)",
        path.display()
    ))
}

fn check_node(label: String, datadir: &Path) -> eyre::Result<StateRootNodeView> {
    let started = Instant::now();
    let db = open_db_read_only(datadir.join("db"), DatabaseArguments::default())
        .wrap_err("open reth db read-only")?;
    let static_files =
        StaticFileProvider::<EthPrimitives>::read_only(datadir.join("static_files"), false)
            .wrap_err("open static files read-only")?;
    let tx = db.tx()?;

    let (block_number, block_source) = match tx
        .get::<tables::StageCheckpoints>(FINISH_STAGE.to_owned())?
        .map(|checkpoint| checkpoint.block_number)
    {
        Some(number) => (number, "checkpoint"),
        None => (
            static_files
                .get_highest_static_file_block(StaticFileSegment::Headers)
                .ok_or_else(|| eyre!("no stage checkpoint and no header in static files"))?,
            "static-files",
        ),
    };
    let header = static_files
        .header_by_number(block_number)?
        .ok_or_else(|| eyre!("header {block_number} is not in static files"))?;

    let accounts = tx.entries::<tables::HashedAccounts>()?;
    let storage_slots = tx.entries::<tables::HashedStorages>()?;
    let leaves_root =
        StateRoot::new(NoopTrieCursorFactory::default(), DatabaseHashedCursorFactory::new(&tx))
            .root()
            .wrap_err("rebuild state root from hashed leaves")?;

    Ok(StateRootNodeView {
        node: label,
        path: datadir.display().to_string(),
        block_number,
        block_source,
        header_root: header.state_root,
        leaves_root,
        accounts,
        storage_slots,
        matches: header.state_root == leaves_root,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}
