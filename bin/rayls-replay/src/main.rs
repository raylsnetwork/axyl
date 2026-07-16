// SPDX-License-Identifier: BUSL-1.1
//! `rayls-replay`: rebuild a Rayls archive datadir from a pruned snapshot.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use eyre::{eyre, Context};
use rayls_execution_evm::reth_env::RethEnv;
use rayls_infrastructure_config::Parameters;
use rayls_infrastructure_storage::open_db;
use rayls_infrastructure_types::{
    rewards::RewardsCounter, Address, Genesis, RaylsNetwork, TaskManager, MAINNET_GENESIS,
    MAINNET_PARAMETERS, TESTNET_GENESIS, TESTNET_PARAMETERS,
};
use rayls_replay::{
    rewards::{SnapshotRewardsBackend, SnapshotTallyStore},
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
use thiserror as _;

/// Scripted historical replay from a Rayls snapshot.
///
/// Reads the snapshot's reth datadir as a totally-ordered execution plan and
/// the consensus DB for batch payloads. Re-executes every block on top of a
/// fresh archive datadir, gating correctness on per-block state-root match.
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

    /// Override genesis YAML path. Defaults to `<snapshot-datadir>/genesis/genesis.yaml`,
    /// falling back to the embedded network genesis if absent.
    #[arg(long, value_name = "PATH")]
    genesis: Option<PathBuf>,

    /// Override parameters YAML path. Defaults to `<snapshot-datadir>/parameters.yaml`,
    /// falling back to the embedded network parameters if absent. CRITICAL for
    /// execution-state parity: `basefee_address` must match what live used.
    #[arg(long, value_name = "PATH")]
    parameters: Option<PathBuf>,

    /// Chain selector: `mainnet`, `testnet`, `local`, `devnet`.
    #[arg(long, value_enum, default_value_t = RaylsNetwork::Mainnet)]
    chain: RaylsNetwork,

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

    /// Repair genesis history indices on an existing archive, then exit (no
    /// replay). Idempotent; run with the node stopped. Freshly built archives
    /// are repaired automatically at the end of a normal replay, so this flag is
    /// only for archives built before that behaviour existed (e.g. mainnet).
    /// Fixes two v2 archive issues:
    ///
    /// 1. StoragesHistory re-key: reth writes genesis StoragesHistory under plain slots while the
    ///    v2 read looks up by keccak256(slot), so genesis-seeded storage (e.g. the validator set)
    ///    returns 0x0 at historical blocks.
    ///
    /// 2. AccountsHistory seed: IndexAccountHistoryStage clears AccountsHistory on first sync and
    ///    never re-inserts accounts whose code/nonce/balance never change after genesis (immutable
    ///    system contracts). Historical `eth_call` returns empty contract code for those accounts.
    #[arg(long = "fix-genesis-history")]
    fix_genesis_history: bool,

    /// Verify state root after every block (slow). Default: epoch boundaries only.
    #[arg(long)]
    verify_every_block: bool,

    /// Progress log frequency.
    #[arg(long, default_value_t = 500)]
    progress_interval: u64,

    /// Use the v2 storage layout (static_files + RocksDB). Default: true,
    /// matching production snapshots produced with `--storage.v2`. Set to
    /// false only if rebuilding from a legacy v1 snapshot.
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
    let _log_guards = init_tracing(&log_path)?;
    info!(
        target: "rayls_replay::main",
        snapshot_datadir = %cli.snapshot_datadir.display(),
        archive_out = %cli.archive_out.display(),
        chain = %cli.chain,
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

    let base_chain = base_chain_spec(cli.chain, &genesis_path)?;
    let NetworkParams { basefee_address, min_base_fee } =
        network_params(cli.chain, &parameters_path)?;
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
    // staged into `tally_store` per close block; snapshot env never builds.
    let tally_store = SnapshotTallyStore::default();
    let archive_rewards = SnapshotRewardsBackend::new(tally_store.clone()).into_counter();

    let snapshot_task_manager = TaskManager::default();
    let archive_task_manager = TaskManager::default();

    let archive_evm = RethEnv::new_for_archive_replay(
        Arc::clone(&base_chain),
        &cli.archive_out,
        &archive_task_manager,
        cli.chain,
        basefee_address,
        Some(min_base_fee),
        cli.storage_v2,
        Some(cli.persistence_threshold),
        archive_rewards.clone(),
    )
    .await
    .wrap_err("open archive reth env")?;

    // maintenance exit: repair genesis history on an existing archive, then exit
    // before opening the snapshot env (no replay).
    if cli.fix_genesis_history {
        archive_evm.fix_genesis_history()?;
        archive_evm.fix_genesis_account_history()?;
        info!(
            target: "rayls_replay::main",
            archive_out = %cli.archive_out.display(),
            "genesis-history fix complete"
        );
        return Ok(());
    }

    // unwind exits before opening the snapshot env; only the archive is touched
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
    // the fix-genesis and unwind early-return paths above, so open it only once
    // we know we're actually replaying (avoids touching a dummy/bad path in those
    // maintenance modes).
    let consensus_store = open_db(&consensus_db);

    let snapshot_evm = RethEnv::new_for_archive_replay(
        Arc::clone(&base_chain),
        &cli.snapshot_datadir,
        &snapshot_task_manager,
        cli.chain,
        basefee_address,
        Some(min_base_fee),
        cli.storage_v2,
        None,
        RewardsCounter::default(),
    )
    .await
    .wrap_err("open snapshot reth env")?;

    verify_chainspec_compatibility(&snapshot_evm, &archive_evm)
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

    let last = run_replay(
        &snapshot_evm,
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

    // archive is complete: repair the two genesis-history issues so it boots
    // correct without a separate --fix-genesis-history pass. The re-key/seed
    // read-merge with existing history, so running here (after replay) preserves
    // every post-genesis change; a no-op on v1 archives and on any slot/account
    // already carrying the genesis block. Only on normal completion — the
    // graceful-stop path returns above, leaving the archive resumable.
    archive_evm.fix_genesis_history()?;
    archive_evm.fix_genesis_account_history()?;
    info!(
        target: "rayls_replay::main",
        archive_out = %cli.archive_out.display(),
        "genesis-history fix applied to freshly built archive"
    );

    // close the DB envs before the copy so no MDBX handle still maps the files;
    // all three are unused past the flush above
    drop(snapshot_evm);
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

/// Resolve `--chain` + optional genesis file to the base reth `ChainSpec`.
///
/// Prefers `genesis_path` if it exists on disk (so the archive uses the same
/// genesis the snapshot was bootstrapped from), otherwise falls back to the
/// embedded YAML for `network`. Rayls-side hardforks are applied downstream
/// by `new_for_archive_replay` via `RaylsChainSpec::builder().rayls_hardforks`.
fn base_chain_spec(
    network: RaylsNetwork,
    genesis_path: &std::path::Path,
) -> eyre::Result<Arc<RethChainSpec>> {
    let yaml = resolve_config_yaml(network, genesis_path, "genesis", |n| match n {
        RaylsNetwork::Mainnet => Some(MAINNET_GENESIS),
        RaylsNetwork::Testnet => Some(TESTNET_GENESIS),
        RaylsNetwork::Devnet | RaylsNetwork::Local => None,
    })?;
    let genesis: Genesis = serde_yaml::from_str(&yaml).wrap_err("parse genesis YAML")?;
    Ok(Arc::new(genesis.into()))
}

/// Network parameters that affect EVM execution.
struct NetworkParams {
    basefee_address: Option<Address>,
    min_base_fee: u64,
}

/// Extract `basefee_address` and `min_base_fee` from `parameters_path` if it
/// exists, otherwise the embedded YAML for `network`. Critical for
/// execution-state parity: the standard node reads from the snapshot's
/// `parameters.yaml`, and `basefee_address` selects where each block's base
/// fee credit lands. A mismatch silently diverges state at the first
/// tx-bearing block.
fn network_params(
    network: RaylsNetwork,
    parameters_path: &std::path::Path,
) -> eyre::Result<NetworkParams> {
    let params_yaml = resolve_config_yaml(network, parameters_path, "parameters", |n| match n {
        RaylsNetwork::Mainnet => Some(MAINNET_PARAMETERS),
        RaylsNetwork::Testnet => Some(TESTNET_PARAMETERS),
        RaylsNetwork::Devnet | RaylsNetwork::Local => None,
    })?;

    let params: Parameters =
        serde_yaml::from_str(&params_yaml).wrap_err("parse parameters YAML")?;

    Ok(NetworkParams { basefee_address: params.basefee_address, min_base_fee: params.min_base_fee })
}

/// Read YAML from `path` if present, else the embedded constant for `network`.
/// Errors for chains that bake no embedded config (`local`/`devnet`).
fn resolve_config_yaml(
    network: RaylsNetwork,
    path: &Path,
    kind: &str,
    embedded: fn(RaylsNetwork) -> Option<&'static str>,
) -> eyre::Result<String> {
    if path.exists() {
        info!(target: "rayls_replay::main", kind, path = %path.display(), "loading config from snapshot datadir");
        return std::fs::read_to_string(path)
            .wrap_err_with(|| format!("read {kind} at {}", path.display()));
    }
    let embedded = embedded(network).ok_or_else(|| {
        eyre!(
            "chain `{network}` does not bake a {kind} and {} does not exist; pass --{kind} explicitly",
            path.display()
        )
    })?;
    info!(target: "rayls_replay::main", kind, %network, "loading embedded config");
    Ok(embedded.to_string())
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
