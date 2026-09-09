// SPDX-License-Identifier: BUSL-1.1
//! `rayls-db-inspect`: read-only inspection of a node's `consensus-db`.
//!
//! Every subcommand opens one or more consensus databases read-only (see [`node_db::NodeDb`]),
//! gathers a per-node view, and folds the views into a network-wide [`report::Verdict`]. The
//! library returns plain report structs; `main.rs` decides between text and JSON rendering, so the
//! same reports are unit-testable without a terminal.

pub mod cli;
pub mod node_db;
pub mod render;
pub mod report;
pub mod view;

use cli::{Cli, Command, WalkTarget};
use node_db::{NodeDb, OpenOptions};
use report::Report;

// Used by the binary target only.
use serde_json as _;
use tracing_subscriber as _;

/// Runs `cli` against every requested database and returns the resulting report.
///
/// Fails (rather than reporting) when a database cannot be opened, so a wrong path is never
/// mistaken for a missing row.
pub fn run(cli: &Cli) -> eyre::Result<Report> {
    let opts = OpenOptions {
        exclusive: cli.exclusive,
        require_stopped: cli.require_stopped,
        recover: cli.recover,
    };
    let verbose = cli.verbose;
    let dbs = cli.dbs();
    if dbs.is_empty() {
        eyre::bail!("no database given: pass --db <[LABEL=]DATADIR>, before or after the command");
    }
    let open = |specs: &[String]| -> eyre::Result<Vec<NodeDb>> {
        // MDBX allows one handle per environment per process, so the same database twice would
        // fail on open with an unhelpful error; catch it here instead.
        let mut seen = std::collections::HashMap::new();
        for spec in specs {
            let (label, path) = NodeDb::resolve(spec)?;
            let canonical = std::fs::canonicalize(&path).unwrap_or(path);
            if let Some(first) = seen.insert(canonical.clone(), label.clone()) {
                return Err(eyre::eyre!(
                    "database {} given twice (as {first} and {label})",
                    canonical.display()
                ));
            }
        }
        specs.iter().map(|spec| NodeDb::open(spec, &opts)).collect()
    };

    Ok(match &cli.command {
        Command::Epoch { epoch, .. } => {
            Report::Epoch(report::epoch::epoch(&open(&dbs)?, *epoch, verbose)?)
        }
        Command::Epochs { from, to, all, .. } => {
            // clap guarantees both bounds unless --all; guard direct callers of `run` too
            let range = match (all, from, to) {
                (true, _, _) => None,
                (false, Some(from), Some(to)) => Some((*from, *to)),
                _ => eyre::bail!("epochs: pass FROM_EPOCH and TO_EPOCH, or --all"),
            };
            Report::Epochs(report::epoch::epochs(&open(&dbs)?, range)?)
        }
        Command::ChainCheck { from, to, .. } => {
            Report::ChainCheck(report::epoch::chain_check(&open(&dbs)?, *from, *to)?)
        }
        Command::Header { number, .. } => {
            Report::Header(report::header::header(&open(&dbs)?, *number, verbose)?)
        }
        Command::Cert { number, .. } => {
            Report::Cert(report::header::cert(&open(&dbs)?, *number, verbose)?)
        }
        Command::Walk { target } => match target {
            WalkTarget::Header { number, back, .. } => {
                Report::Walk(report::header::walk(&open(&dbs)?, *number, *back)?)
            }
        },
        Command::Summary { .. } => Report::Summary(report::summary::summary(&open(&dbs)?)?),
    })
}
