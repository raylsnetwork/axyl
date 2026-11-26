// SPDX-License-Identifier: BUSL-1.1
//! `rayls-replay`: rebuild a Rayls archive datadir from a pruned snapshot.

#![cfg_attr(test, allow(unused_crate_dependencies))]

use clap::Parser;
use eyre::{eyre, Context};
use rayls_execution_evm::{
    reth_env::{RethEnv, SnapshotReader},
    verify_datadir_chain_id, verify_datadir_schedule_record_at_head, verify_schedule_shape,
    FileSchedule, NetworkProfile, SelectedSchedule,
};
use rayls_infrastructure_config::Parameters;
use rayls_infrastructure_storage::open_db_read_only;
use rayls_infrastructure_types::{
    rewards::RewardsCounter, Address, Genesis, RaylsNetwork, TaskManager,
};
use rayls_replay::{
    rewards::{BoundedHybridWalker, HybridTallySource, SnapshotRewardsBackend, SnapshotTallyStore},
    run_replay, verify_chainspec_compatibility, ReplayConfig,
};
use reth_chainspec::ChainSpec as RethChainSpec;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::watch;
use tracing::{error, info, warn};

use parking_lot as _;
use rayls_middleware_rewards as _;
use thiserror as _;

/// Scripted historical replay from a Rayls snapshot.
///
/// Reads the snapshot's reth datadir as a totally-ordered execution plan and
/// the consensus DB for batch payloads. Re-executes every block on top of a
/// fresh archive datadir, gating correctness on per-block state-root match.
/// The snapshot is opened read-only and is never repaired or changed; it must
/// not be in use by a running node.
/// On completion the archive datadir is ready to boot via `rayls-network node`
/// in Observer mode.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the snapshot's rayls datadir (the directory containing
    /// `db/`, `consensus-db/`, and `genesis/`).
    #[arg(long, value_name = "PATH")]
    snapshot_datadir: PathBuf,

    /// Path to the rayls datadir to rebuild into. On first run it must not
    /// exist yet or must be empty (reth initializes the EVM db from genesis);
    /// re-running with a datadir from an interrupted replay resumes from its tip.
    #[arg(long, value_name = "PATH")]
    archive_out: PathBuf,

    /// Override consensus DB path. Defaults to `<snapshot-datadir>/consensus-db`.
    #[arg(long, value_name = "PATH")]
    consensus_db: Option<PathBuf>,

    /// Override genesis YAML path. Defaults to `<snapshot-datadir>/genesis/genesis.yaml`.
    /// Must exist — there is no embedded fallback.
    #[arg(long, value_name = "PATH")]
    genesis: Option<PathBuf>,

    /// Override parameters YAML path. Defaults to `<snapshot-datadir>/parameters.yaml`.
    /// Must exist — there is no embedded fallback. CRITICAL for
    /// execution-state parity: `basefee_address` must match what live used.
    #[arg(long, value_name = "PATH")]
    parameters: Option<PathBuf>,

    /// Rayls network: `mainnet`, `testnet`, `local`, `devnet` — the same flag as the
    /// node's `--network`. Selects the baked-in hardfork schedule the blocks are rebuilt with;
    /// the network's chain-id must match the snapshot genesis or the boot refuses.
    #[arg(long, value_enum, default_value_t = RaylsNetwork::Mainnet)]
    network: RaylsNetwork,

    /// Network config file (YAML) holding named subnets, each with a `chain_id` and a
    /// `hardforks` schedule (the same format the node takes via `--config-file`).
    /// With `--subnet`, that subnet's schedule replaces the baked-in `--network` profile
    /// for the rebuild. Use it when a network historically
    /// ran a schedule that differs from its baked-in profile. The subnet's
    /// `chain_id` must match the genesis, and a subnet may not declare a baked-in
    /// network's chain-id (mainnet/testnet run on `--network`, never a config file).
    #[arg(long, value_name = "PATH", requires = "subnet", conflicts_with = "network")]
    config_file: Option<PathBuf>,

    /// Subnet name to select inside `--config-file`.
    #[arg(long, value_name = "NAME", requires = "config_file")]
    subnet: Option<String>,

    /// First block to replay (inclusive).
    #[arg(long, default_value_t = 1)]
    from_block: u64,

    /// Last block to replay (inclusive). Defaults to snapshot tip.
    #[arg(long)]
    to_block: Option<u64>,

    /// Unwind the archive datadir down to this block and exit (no replay), then
    /// re-run without this flag to resume from the unwound tip.
    #[arg(long, value_name = "BLOCK")]
    unwind_to: Option<u64>,

    /// Verify state root after every block (slow). Default: epoch boundaries only.
    #[arg(long)]
    verify_every_block: bool,

    /// Progress log frequency.
    #[arg(long, default_value_t = 500)]
    progress_interval: u64,

    /// Storage layout of the archive datadir that is written: v2 (static_files +
    /// RocksDB) or the legacy v1. Default: true, the layout production nodes run
    /// with `--storage.v2`. The snapshot is read the same way for both values.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    storage_v2: bool,

    /// Deferred-persistence flush threshold for the archive, in blocks. Higher
    /// values batch more blocks per MDBX write transaction (fewer write-lock
    /// acquisitions and fsyncs), at the cost of holding more non-persisted blocks
    /// in memory until the next flush. Tune per workload.
    #[arg(long, default_value_t = 512)]
    persistence_threshold: u64,

    /// Path for the full async log file. Defaults to
    /// `<archive_out>/rayls-replay.log`. stdout always shows calm progress only;
    /// this file captures the complete per-block detail (honors `RUST_LOG`).
    #[arg(long, value_name = "PATH")]
    log_file: Option<PathBuf>,
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let log_path = cli.log_file.clone().unwrap_or_else(|| cli.archive_out.join("rayls-replay.log"));
    // Before the log file is created: by default it goes into the archive datadir, so a wrong
    // `--archive-out` would already write into the snapshot here.
    verify_outside_snapshot(&cli.snapshot_datadir, &cli.archive_out, "--archive-out")?;
    verify_outside_snapshot(&cli.snapshot_datadir, &log_path, "the log file")?;
    let _log_guards = init_tracing(&log_path)?;
    info!(
        target: "rayls_replay::main",
        snapshot_datadir = %cli.snapshot_datadir.display(),
        archive_out = %cli.archive_out.display(),
        network = if cli.config_file.is_some() {
            "overridden by --config-file".to_string()
        } else {
            cli.network.to_string()
        },
        config_file = ?cli.config_file,
        subnet = ?cli.subnet,
        "rayls-replay starting"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("build tokio runtime")?;
    rt.block_on(async { run(cli).await })
}

async fn run(cli: Cli) -> eyre::Result<()> {
    let consensus_db =
        cli.consensus_db.clone().unwrap_or_else(|| cli.snapshot_datadir.join("consensus-db"));
    let genesis_path = cli
        .genesis
        .clone()
        .unwrap_or_else(|| cli.snapshot_datadir.join("genesis").join("genesis.yaml"));
    let parameters_path =
        cli.parameters.clone().unwrap_or_else(|| cli.snapshot_datadir.join("parameters.yaml"));

    let base_chain = base_chain_spec(&genesis_path)?;

    // Select and verify the hardfork schedule exactly like the node's boot gate;
    // the archive env below is built from the selected profile.
    let file_schedule = match (&cli.config_file, &cli.subnet) {
        (Some(path), Some(subnet)) => Some(FileSchedule::load(path, subnet)?),
        _ => None,
    };
    let selected = SelectedSchedule::select(file_schedule.as_ref(), Some(cli.network))?; // cli.network defaults to Mainnet; safe because select ignores network when file_schedule is
                                                                                         // Some, and the shape gate keys on profile.chain_id (from the file)
    verify_datadir_chain_id(base_chain.chain().id(), selected.profile.chain_id, &selected.source)?;

    // The snapshot's execution data, read-only: no repair, no unwind, no write. Opened before
    // the schedule check because that check needs the snapshot's last block.
    let snapshot = SnapshotReader::open(&cli.snapshot_datadir, Arc::clone(&base_chain))
        .wrap_err("open snapshot datadir read-only")?;

    verify_snapshot_schedule_record(
        &cli.snapshot_datadir,
        snapshot.last_block_number(),
        &selected.profile,
    )?;

    // The selected schedule must match the relaxed shape of its network
    // family. Mainnet/testnet are excluded inside the gate (frozen in CI and
    // guarded per datadir by the record check above).
    verify_schedule_shape(&selected.profile)?;
    info!(
        target: "rayls_replay::main",
        source = %selected.source,
        chain_id = selected.profile.chain_id,
        hardforks = ?selected.profile.hardforks,
        "hardfork schedule selected"
    );
    let NetworkParams { basefee_address, min_base_fee } = network_params(&parameters_path)?;
    info!(
        target: "rayls_replay::main",
        genesis = %genesis_path.display(),
        parameters = %parameters_path.display(),
        consensus_db = %consensus_db.display(),
        ?basefee_address,
        min_base_fee,
        "loaded network configuration"
    );

    // archive blocks build with the snapshot's committed close-epoch tally,
    // staged into `tally_store` per close block; the snapshot is only read.
    // Hybrid-reward epochs also need the consensus-DB walk, attached to
    // `hybrid_source` once the consensus DB is open below.
    let tally_store = SnapshotTallyStore::default();
    let hybrid_source = HybridTallySource::default();
    let archive_rewards =
        SnapshotRewardsBackend::new(tally_store.clone(), hybrid_source.clone()).into_counter();

    let archive_task_manager = TaskManager::default();

    let archive_evm = RethEnv::new_for_archive_replay(
        Arc::clone(&base_chain),
        &cli.archive_out,
        &archive_task_manager,
        &selected.profile,
        basefee_address,
        Some(min_base_fee),
        cli.storage_v2,
        Some(cli.persistence_threshold),
        archive_rewards.clone(),
    )
    .await
    .wrap_err("open archive reth env")?;

    // unwind exits here: it changes only the archive
    if let Some(target) = cli.unwind_to {
        archive_evm
            .unwind_to(target, archive_rewards.clone())
            .await
            .wrap_err("unwind archive datadir")?;
        info!(
            target: "rayls_replay::main",
            target,
            archive_out = %cli.archive_out.display(),
            "unwind complete; re-run without --unwind-to to resume replay"
        );
        return Ok(());
    }

    // snapshot-only setup: the consensus DB is the replay oracle and is unused by
    // the unwind early-return path above, so open it only once we know we're
    // actually replaying (avoids touching a dummy/bad path in that maintenance mode).
    // Read-only like the execution data: no table is created, nothing is repaired.
    let consensus_store =
        open_db_read_only(&consensus_db).wrap_err("open snapshot consensus DB read-only")?;

    // hybrid-reward close blocks recompute participation rounds over the snapshot's
    // consensus DB with a forward, cursor-bounded walk that credits exactly like the
    // live node's walker but reads each epoch's rows once (see `rewards.rs`; the live
    // reverse walk scans the whole tail of the table per epoch against a snapshot).
    // ORDERING: this attach must precede `run_replay` below, whose first
    // `install_committee_from_contract` forwards the committee to the walker;
    // `set_committee` only reaches a walker that is already attached.
    if !hybrid_source
        .attach(RewardsCounter::from_impl(BoundedHybridWalker::new(consensus_store.clone())))
    {
        return Err(eyre!("hybrid tally source attached twice"));
    }

    verify_chainspec_compatibility(&snapshot, &archive_evm)
        .map_err(|e| eyre!("chainspec compatibility check failed: {e}"))?;

    let config = ReplayConfig {
        from_block: cli.from_block,
        to_block: cli.to_block,
        verify_every_block: cli.verify_every_block,
        progress_interval: cli.progress_interval,
    };

    // SIGTERM (docker stop) / SIGINT (ctrl-c) request a graceful stop: the replay
    // loop finishes the current output group, then we flush and exit resumable.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_shutdown_listener(shutdown_tx);

    // requires `hybrid_source.attach(...)` above to have run: the committee installed
    // here (and after every close block) must reach the hybrid walker
    let last = run_replay(
        &snapshot,
        &consensus_store,
        &archive_evm,
        &archive_rewards,
        &tally_store,
        &config,
        &shutdown_rx,
    )
    .await
    .map_err(|e| eyre!("replay failed: {e}"))?;

    // flush deferred persistence so buffered blocks reach disk (needed on both the
    // completion and the graceful-stop path, so the datadir is always resumable)
    archive_evm.flush_persistence().await.wrap_err("final persistence flush")?;

    // on a graceful stop the archive is incomplete; skip the Observer finalization
    // (artifact copy) and report the resumable tip
    if *shutdown_rx.borrow() {
        info!(
            target: "rayls_replay::main",
            last,
            archive_out = %cli.archive_out.display(),
            "rayls-replay stopped gracefully; flushed to tip, re-run to resume"
        );
        return Ok(());
    }

    // close the DB envs before the copy so no MDBX handle still maps the files;
    // all three are unused past the flush above
    drop(snapshot);
    drop(archive_evm);
    drop(consensus_store);

    // make archive_out self-contained for Observer boot by copying the consensus
    // and config artifacts; the multi-GB copy runs off the runtime thread
    let (snap, cdb, arch) =
        (cli.snapshot_datadir.clone(), consensus_db.clone(), cli.archive_out.clone());
    tokio::task::spawn_blocking(move || copy_observer_artifacts(&snap, &cdb, &arch))
        .await
        .wrap_err("artifact copy task")??;

    info!(
        target: "rayls_replay::main",
        last,
        snapshot_datadir = %cli.snapshot_datadir.display(),
        archive_out = %cli.archive_out.display(),
        "rayls-replay complete; archive datadir ready for Observer boot"
    );
    Ok(())
}

/// Refuse a path the replay writes to if it is the snapshot datadir or lies inside it.
///
/// The replay must not change the snapshot. An archive datadir or a log file at or under the
/// snapshot path would put new files into the snapshot's own directory. `what` names the path
/// in the error (a flag name).
fn verify_outside_snapshot(snapshot: &Path, path: &Path, what: &str) -> eyre::Result<()> {
    let snapshot_abs = std::fs::canonicalize(snapshot)
        .wrap_err_with(|| format!("snapshot datadir {} cannot be read", snapshot.display()))?;
    if resolve_path(path)?.starts_with(&snapshot_abs) {
        return Err(eyre!(
            "{what} ({}) is the snapshot datadir {} or lies inside it; the replay never writes \
             into the snapshot, pass a path outside of it",
            path.display(),
            snapshot.display()
        ));
    }
    Ok(())
}

/// The absolute form of `path` with symlinks resolved. The last parts of the path may not exist
/// yet (the archive datadir of a first run); they are kept as given.
///
/// Two kinds of path cannot be resolved before they are created, and can end up anywhere, also
/// in the snapshot. Both are an error:
/// - a `..` after a part that does not exist yet (`other/new/../../snapshot`)
/// - a symlink whose target does not exist: a file created through it lands at the target
fn resolve_path(path: &Path) -> eyre::Result<PathBuf> {
    let absolute = std::path::absolute(path)
        .wrap_err_with(|| format!("cannot make {} absolute", path.display()))?;
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        if let Ok(resolved) = std::fs::canonicalize(existing) {
            return Ok(missing.iter().rev().fold(resolved, |acc, part| acc.join(part)));
        }
        // `canonicalize` fails on a symlink with a missing target the same way as on a missing
        // path, so check for it before this part is taken as "not there yet"
        if std::fs::symlink_metadata(existing).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return Err(eyre!(
                "{} is, or goes through, a symlink whose target does not exist, so it cannot be \
                 checked against the snapshot datadir; pass the real path",
                path.display()
            ));
        }
        // `file_name` is `None` for a path that ends in `..`
        let (Some(parent), Some(name)) = (existing.parent(), existing.file_name()) else {
            return Err(eyre!(
                "{} has `..` after a directory that does not exist yet, so it cannot be checked \
                 against the snapshot datadir; pass a path without `..`",
                path.display()
            ));
        };
        missing.push(name.to_owned());
        existing = parent;
    }
}

/// Spawn a task that flips `tx` to `true` on the first SIGTERM/SIGINT, so the
/// replay loop can stop at the next output-group boundary and flush a consistent,
/// resumable tip.
fn spawn_shutdown_listener(tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        wait_for_signal().await;
        // ignore send errors: a dropped receiver means replay already finished
        let _ = tx.send(true);
        info!(
            target: "rayls_replay::main",
            "shutdown signal received; stopping after the current output group"
        );
    });
}

/// Resolve when the process receives SIGTERM (`docker stop`) or SIGINT (ctrl-c).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    res = tokio::signal::ctrl_c() => {
                        if let Err(e) = res {
                            error!(target: "rayls_replay::main", %e, "ctrl-c listener failed");
                        }
                    }
                }
            }
            Err(e) => {
                error!(target: "rayls_replay::main", %e, "failed to install SIGTERM handler");
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!(target: "rayls_replay::main", %e, "ctrl-c listener failed");
        }
    }
}

/// Copy the snapshot artifacts an Observer needs (everything except the rebuilt
/// EVM `db/`) into `archive`, so the archive datadir boots without the snapshot.
///
/// `consensus_db` is the resolved consensus path (honoring `--consensus-db`);
/// the remaining artifacts are read from the snapshot datadir root.
fn copy_observer_artifacts(
    snapshot: &Path,
    consensus_db: &Path,
    archive: &Path,
) -> eyre::Result<()> {
    copy_artifact(consensus_db, &archive.join("consensus-db"), "consensus-db")?;
    for name in ["genesis", "parameters.yaml", "node-info.yaml", "node-keys", "network-config"] {
        copy_artifact(&snapshot.join(name), &archive.join(name), name)?;
    }
    Ok(())
}

/// Copy one named artifact (file or directory) if it exists, logging the outcome.
///
/// An already-present destination is left untouched: it either survived a prior
/// completed run (identical content) or belongs to a datadir the operator passed
/// as `--archive-out` by mistake, and overwriting it would destroy data.
fn copy_artifact(src: &Path, dst: &Path, name: &str) -> eyre::Result<()> {
    if !src.exists() {
        info!(target: "rayls_replay::main", artifact = name, "snapshot artifact absent, skipping");
        return Ok(());
    }
    if dst.exists() {
        warn!(
            target: "rayls_replay::main",
            artifact = name,
            dst = %dst.display(),
            "artifact already exists in archive datadir; leaving it in place"
        );
        return Ok(());
    }
    copy_recursive(src, dst).wrap_err_with(|| format!("copy {name} into archive datadir"))?;
    info!(
        target: "rayls_replay::main",
        artifact = name,
        src = %src.display(),
        "copied snapshot artifact into archive"
    );
    Ok(())
}

/// Recursively copy `src` into `dst` (file or directory tree).
fn copy_recursive(src: &Path, dst: &Path) -> eyre::Result<()> {
    if src.is_dir() {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            copy_recursive(&entry.path(), &dst.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src, dst)?;
    }
    Ok(())
}

/// Read the genesis YAML at `genesis_path` into the base reth `ChainSpec`.
///
/// The file must exist (the snapshot's `genesis/genesis.yaml` or an explicit
/// `--genesis`): there is no embedded genesis fallback. Rayls-side hardforks
/// are applied downstream by `new_for_archive_replay` via
/// `RaylsChainSpec::builder().rayls_hardforks`.
fn base_chain_spec(genesis_path: &std::path::Path) -> eyre::Result<Arc<RethChainSpec>> {
    let yaml = std::fs::read_to_string(genesis_path).wrap_err_with(|| {
        format!("read genesis YAML at {} (pass --genesis if it is absent)", genesis_path.display())
    })?;
    let genesis: Genesis = serde_yaml::from_str(&yaml).wrap_err("parse genesis YAML")?;
    Ok(Arc::new(genesis.into()))
}

/// Network parameters that affect EVM execution.
struct NetworkParams {
    basefee_address: Option<Address>,
    min_base_fee: u64,
}

/// Extract `basefee_address` and `min_base_fee` from the parameters YAML at
/// `parameters_path`. The file must exist (the snapshot's `parameters.yaml` or
/// an explicit `--parameters`): there is no embedded fallback. Critical for
/// execution-state parity: the standard node reads from the snapshot's
/// `parameters.yaml`, and `basefee_address` selects where each block's base
/// fee credit lands. A mismatch silently diverges state at the first
/// tx-bearing block.
fn network_params(parameters_path: &std::path::Path) -> eyre::Result<NetworkParams> {
    let params_yaml = std::fs::read_to_string(parameters_path).wrap_err_with(|| {
        format!(
            "read parameters YAML at {} (pass --parameters if it is absent)",
            parameters_path.display()
        )
    })?;

    let params: Parameters =
        serde_yaml::from_str(&params_yaml).wrap_err("parse parameters YAML")?;

    Ok(NetworkParams { basefee_address: params.basefee_address, min_base_fee: params.min_base_fee })
}

/// Initialize layered non-blocking tracing and return the writer guards.
///
/// Both layers write on background workers so logging never lands on the replay
/// hot path; the guards must outlive the process to flush buffered lines. stdout
/// shows only calm `rayls_replay` progress plus warnings; the file at `log_path`
/// captures replay-level detail and honors `RUST_LOG`. The default filter drops
/// the per-block `engine` events (they cost per-block formatting on the hot path);
/// set `RUST_LOG="info,engine=info"` to capture them when debugging.
fn init_tracing(
    log_path: &Path,
) -> eyre::Result<(
    tracing_appender::non_blocking::WorkerGuard,
    tracing_appender::non_blocking::WorkerGuard,
)> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent).wrap_err("create log directory")?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .wrap_err_with(|| format!("open log file {}", log_path.display()))?;

    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let (file_writer, file_guard) = tracing_appender::non_blocking(file);

    let stdout_layer = fmt::layer()
        .with_target(true)
        .with_writer(stdout_writer)
        .with_filter(EnvFilter::new("warn,rayls_replay=info"));
    let file_layer =
        fmt::layer().with_ansi(false).with_target(true).with_writer(file_writer).with_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,engine=warn")),
        );

    tracing_subscriber::registry().with(stdout_layer).with(file_layer).init();
    Ok((stdout_guard, file_guard))
}

/// Verify the selected hardfork schedule against the snapshot's schedule record,
/// when the snapshot carries one.
///
/// A snapshot taken from a node running the schedule-record boot gate carries
/// `schedule-record.yaml`, pinning the schedule its executed blocks ran under.
/// A selection that moves an already-executed fork (or back-dates one into the
/// executed history) would re-interpret the snapshot's blocks and silently
/// diverge state, so this refuses it; future boundary moves are reported.
/// Read-only: replay never writes into the snapshot datadir — the node's gate
/// re-records, and there is no record here to update. A snapshot without a
/// record (taken before the feature) is trusted, like the node's no-record
/// path, minus the write.
///
/// `head` is the snapshot's last block, taken from the read-only reader. The
/// node's gate reads it through a normal database open, which can write.
fn verify_snapshot_schedule_record(
    snapshot_datadir: &Path,
    head: u64,
    profile: &NetworkProfile,
) -> eyre::Result<()> {
    // the verify step is shared with the node's boot gate
    let verification = verify_datadir_schedule_record_at_head(snapshot_datadir, head, profile)?;
    if verification.record.is_none() {
        warn!(
            target: "rayls_replay::main",
            path = tracing::field::debug(&verification.path),
            head = verification.head,
            "snapshot has no schedule record; trusting the selected schedule (the \
             executed-history check is unavailable)"
        );
    } else {
        info!(
            target: "rayls_replay::main",
            path = tracing::field::debug(&verification.path),
            head = verification.head,
            "snapshot schedule record verified against the selected schedule"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(snapshot: &Path, path: &Path) -> eyre::Result<()> {
        verify_outside_snapshot(snapshot, path, "--archive-out")
    }

    /// A path the replay writes to may not be the snapshot datadir or lie inside it, also when
    /// the path does not exist yet or is reached through a symlink.
    #[test]
    fn a_path_inside_the_snapshot_is_refused() {
        let root = tempfile::tempdir().expect("temp dir");
        let snapshot = root.path().join("snapshot");
        std::fs::create_dir_all(snapshot.join("db")).expect("create snapshot");

        // the same directory; the error names the flag
        let error = check(&snapshot, &snapshot).expect_err("the snapshot itself is refused");
        assert!(error.to_string().contains("--archive-out"), "got: {error}");
        // inside it, existing and not yet existing
        assert!(check(&snapshot, &snapshot.join("db")).is_err());
        assert!(check(&snapshot, &snapshot.join("out").join("deeper")).is_err());
        // the same directory through `.`
        assert!(check(&snapshot, &snapshot.join(".")).is_err());
        // through `..` over a directory that exists
        let other = root.path().join("other");
        std::fs::create_dir_all(&other).expect("create other");
        assert!(check(&snapshot, &other.join("..").join("snapshot").join("out")).is_err());
        // through `..` over a directory that does not exist yet: the path cannot be resolved
        // now, and once `new` is created it points into the snapshot
        let error = check(&snapshot, &other.join("new").join("..").join("..").join("snapshot"))
            .expect_err("`..` after a missing directory is refused");
        assert!(error.to_string().contains("`..`"), "got: {error}");

        #[cfg(unix)]
        {
            let link = root.path().join("link-to-snapshot");
            std::os::unix::fs::symlink(&snapshot, &link).expect("symlink");
            assert!(check(&snapshot, &link).is_err());
            assert!(check(&snapshot, &link.join("out")).is_err());

            // a symlink whose target does not exist yet: a file created through the link would
            // land in the snapshot
            let dangling = root.path().join("dangling.log");
            std::os::unix::fs::symlink(snapshot.join("new.log"), &dangling).expect("symlink");
            let error = check(&snapshot, &dangling).expect_err("a dangling symlink is refused");
            assert!(error.to_string().contains("symlink"), "got: {error}");
            assert!(check(&snapshot, &dangling.join("deeper")).is_err());
        }
    }

    /// A path outside the snapshot is accepted, existing or not, also next to the snapshot with
    /// a name that starts the same way.
    #[test]
    fn a_path_outside_the_snapshot_is_accepted() {
        let root = tempfile::tempdir().expect("temp dir");
        let snapshot = root.path().join("snapshot");
        std::fs::create_dir_all(&snapshot).expect("create snapshot");
        let existing = root.path().join("archive");
        std::fs::create_dir_all(&existing).expect("create archive");

        check(&snapshot, &existing).expect("existing archive");
        check(&snapshot, &root.path().join("new").join("archive"))
            .expect("archive that does not exist yet");
        check(&snapshot, &root.path().join("snapshot-archive"))
            .expect("a sibling whose name starts like the snapshot");
    }

    /// A snapshot path that does not exist is an error, not a pass.
    #[test]
    fn missing_snapshot_is_an_error() {
        let root = tempfile::tempdir().expect("temp dir");
        assert!(check(&root.path().join("nope"), root.path()).is_err());
    }
}
