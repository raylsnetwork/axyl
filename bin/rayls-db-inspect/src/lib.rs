// SPDX-License-Identifier: BUSL-1.1
//! `rayls-db-inspect`: read-only inspection of a node's `consensus-db`.
//!
//! Every subcommand opens one or more nodes ([`source::Source`]: a consensus database read-only,
//! or a node's RPC endpoint), gathers a per-node view, and folds the views into a network-wide
//! [`report::Verdict`]. The library returns plain report structs; `main.rs` decides between text
//! and JSON rendering, so the same reports are unit-testable without a terminal.

pub mod cli;
pub mod node_db;
pub mod render;
pub mod report;
pub mod source;
pub mod view;

use cli::{Cli, Command};
use node_db::{LiveStatus, NodeDb, OpenOptions};
use report::Report;
use source::Source;

// Used by the binary target only.
use serde_json as _;
use tracing_subscriber as _;

/// Runs `cli` against every requested node and returns the resulting report.
///
/// Fails (rather than reporting) when a database cannot be opened or a command needs a database
/// and got an RPC node, so a wrong path is never mistaken for a missing row.
pub fn run(cli: &Cli) -> eyre::Result<Report> {
    let opts = OpenOptions {
        exclusive: cli.exclusive,
        require_stopped: cli.require_stopped,
        recover: cli.recover,
    };
    let verbose = cli.verbose;
    let dbs = cli.dbs();
    let rpcs = cli.rpcs();
    if dbs.is_empty() && rpcs.is_empty() {
        eyre::bail!(
            "no node given: pass --db <[LABEL=]DATADIR> or --rpc <[LABEL=]URL>, before or after \
             the command"
        );
    }
    if cli.recover && matches!(cli.command, Command::Snapshot { .. }) {
        eyre::bail!("--recover does not combine with snapshot: the source is copied as it is");
    }
    let open_dbs = || -> eyre::Result<Vec<NodeDb>> {
        // MDBX allows one handle per environment per process, so the same database twice would
        // fail on open with an unhelpful error; catch it here instead.
        let mut seen = std::collections::HashMap::new();
        for spec in &dbs {
            let (label, path) = NodeDb::resolve(spec)?;
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
    // commands that read batches or node-local state: databases only
    let open_dbs_only = |what: &str| -> eyre::Result<Vec<NodeDb>> {
        if !rpcs.is_empty() {
            let labels: Vec<String> = rpcs.iter().map(|spec| source::split_label(spec).0).collect();
            eyre::bail!(
                "{what} needs database nodes (--db): {} {} an RPC node, which does not serve \
                 batches or node-local state",
                labels.join(", "),
                if labels.len() == 1 { "is" } else { "are each" }
            );
        }
        open_dbs()
    };
    // one column per node: a label used twice, or one endpoint given twice, would show one node
    // as two agreeing ones
    let check_labels = || -> eyre::Result<()> {
        let mut labels = std::collections::HashSet::new();
        let mut urls = std::collections::HashSet::new();
        for spec in &dbs {
            let (label, _) = NodeDb::resolve(spec)?;
            if !labels.insert(label.clone()) {
                eyre::bail!("node label {label} used twice; give each node a distinct label");
            }
        }
        for spec in &rpcs {
            let (label, url) = source::split_label(spec);
            if !urls.insert(url.to_owned()) {
                eyre::bail!("RPC endpoint {url} given twice");
            }
            if !labels.insert(label.clone()) {
                eyre::bail!("node label {label} used twice; give each node a distinct label");
            }
        }
        Ok(())
    };
    let open_all = || -> eyre::Result<Vec<Source>> {
        check_labels()?;
        let mut sources: Vec<Source> = open_dbs()?.into_iter().map(Source::Db).collect();
        for spec in &rpcs {
            sources.push(Source::open_rpc(spec, cli.rpc_rate)?);
        }
        Ok(sources)
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
        Command::GetBatch { digest, .. } => Report::GetBatch(report::batch::get_batch(
            &open_dbs_only("get-batch")?,
            *digest,
            verbose,
        )?),
        Command::GetTx { hash, epoch, .. } => {
            Report::GetTx(report::batch::get_tx(&open_dbs_only("get-tx")?, *hash, *epoch)?)
        }
        Command::HeaderCheck { number, back, .. } => {
            Report::HeaderCheck(report::header::header_check(&open_all()?, *number, *back)?)
        }
        Command::Summary { .. } => {
            Report::Summary(report::summary::summary(&open_dbs_only("summary")?)?)
        }
        Command::Snapshot { to, .. } => {
            if !rpcs.is_empty() || dbs.len() != 1 {
                eyre::bail!(
                    "snapshot copies one node: give exactly one --db and no --rpc (got {} --db, {} --rpc)",
                    dbs.len(),
                    rpcs.len()
                );
            }
            match NodeDb::open(&dbs[0], &opts) {
                Ok(db) => Report::Snapshot(report::snapshot::snapshot(&db, to)?),
                // a stopped node whose last commit was never synced cannot be read until it is
                // recovered: copy its files as they are and recover the copy instead
                Err(err) if NodeDb::needs_recovery(&err) => {
                    let (label, path) = NodeDb::resolve(&dbs[0])?;
                    if let LiveStatus::Live { .. } = node_db::probe_live(&path) {
                        return Err(err);
                    }
                    eprintln!(
                        "{label}: {} is stopped with an unsynced last commit: copying its files as \
                         they are and recovering the copy",
                        path.display()
                    );
                    Report::Snapshot(report::snapshot::snapshot_stopped_unsynced(
                        &label, &path, to,
                    )?)
                }
                Err(err) => return Err(err),
            }
        }
    })
}
