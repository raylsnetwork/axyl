// SPDX-License-Identifier: BUSL-1.1
//! `rayls-db-inspect`: read-only inspection of a node's `consensus-db`.
//!
//! Every subcommand opens one or more nodes' consensus databases read-only ([`node_db::NodeDb`]),
//! gathers a per-node view, and folds the views into a network-wide [`report::Verdict`]. The
//! library returns plain report structs; `main.rs` decides between text and JSON rendering, so
//! the same reports are unit-testable without a terminal.

pub mod cli;
pub mod node_db;
pub mod render;
pub mod report;
pub mod view;

use cli::{Cli, Command};
use node_db::{NodeDb, OpenOptions};
use report::Report;

// Used by the binary target only.
use serde_json as _;
use tracing_subscriber as _;

/// Runs `cli` against every requested node and returns the resulting report.
///
/// Fails (rather than reporting) when a database cannot be opened, so a wrong path is never
/// mistaken for a missing row.
pub fn run(cli: &Cli) -> eyre::Result<Report> {
    let opts = OpenOptions { recover: cli.recover };
    let verbose = cli.verbose;
    let dbs = cli.dbs();
    if dbs.is_empty() {
        eyre::bail!("no node given: pass --db <[LABEL=]DATADIR>, before or after the command");
    }
    // Every command opens every node the same way: one column per node, so a label used twice
    // would show one node as two agreeing ones, and MDBX allows one handle per environment per
    // process, so the same database twice would fail on open with an unhelpful error.
    let open_all = || -> eyre::Result<Vec<NodeDb>> {
        let mut labels = std::collections::HashSet::new();
        let mut seen = std::collections::HashMap::new();
        for spec in &dbs {
            let (label, path) = NodeDb::resolve(spec)?;
            if !labels.insert(label.clone()) {
                eyre::bail!("node label {label} used twice; give each node a distinct label");
            }
            let canonical = std::fs::canonicalize(&path).unwrap_or(path);
            if let Some(first) = seen.insert(canonical.clone(), label.clone()) {
                return Err(eyre::eyre!(
                    "database {} given twice (as {first} and {label})",
                    canonical.display()
                ));
            }
        }
        dbs.iter().map(|spec| NodeDb::open(spec, &opts)).collect()
    };

    Ok(match &cli.command {
        Command::Epoch { epoch, .. } => {
            Report::Epoch(report::epoch::epoch(&open_all()?, *epoch, verbose)?)
        }
        Command::Epochs { from, to, all, .. } => {
            // clap guarantees both bounds unless --all; guard direct callers of `run` too
            let range = match (all, from, to) {
                (true, _, _) => None,
                (false, Some(from), Some(to)) => Some((*from, *to)),
                _ => eyre::bail!("epochs: pass FROM_EPOCH and TO_EPOCH, or --all"),
            };
            Report::Epochs(report::epoch::epochs(&open_all()?, range)?)
        }
        Command::EpochCheck { from, to, .. } => {
            Report::EpochCheck(report::epoch::epoch_check(&open_all()?, *from, *to, verbose)?)
        }
        Command::Header { number, .. } => {
            Report::Header(report::header::header(&open_all()?, *number, verbose)?)
        }
        Command::Cert { number, .. } => {
            Report::Cert(report::header::cert(&open_all()?, *number, verbose)?)
        }
        Command::GetBatch { digest, .. } => {
            Report::GetBatch(report::batch::get_batch(&open_all()?, *digest)?)
        }
        Command::GetTx { hash, epoch, .. } => {
            Report::GetTx(report::batch::get_tx(&open_all()?, *hash, *epoch)?)
        }
        Command::HeaderCheck { number, back, .. } => {
            Report::HeaderCheck(report::header::header_check(&open_all()?, *number, *back)?)
        }
        Command::Summary { .. } => Report::Summary(report::summary::summary(&open_all()?)?),
    })
}
