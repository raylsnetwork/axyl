// SPDX-License-Identifier: BUSL-1.1
//! Scripted historical replay from a Rayls snapshot.
//!
//! Walks the snapshot's canonical EVM chain block by block, re-executing each on
//! the archive's growing state and verifying the state root against the snapshot.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

// Binary-only deps; markers keep the lib compilation unit's lint quiet.
use clap as _;
use eyre as _;
use rayls_infrastructure_config as _;
use reth_chainspec as _;
use serde_yaml as _;
use tracing_appender as _;
use tracing_subscriber as _;

pub mod epoch;
pub mod error;
pub mod integrity;
pub mod plan;
pub mod replay;
pub mod rewards;

use crate::{
    epoch::install_committee_from_contract,
    error::{ReplayError, ReplayResult},
    integrity::{cross_check_snapshot, load_epoch_anchors, EpochAnchor},
    plan::{derive_plan_window, snapshot_close_epoch_tally, PlanBlock},
    replay::execute_output_group,
    rewards::SnapshotTallyStore,
};
use parking_lot::Mutex;
use rayls_execution_evm::reth_env::RethEnv;
use rayls_infrastructure_types::{rewards::RewardsCounter, Database, SealedHeader, B256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

/// Blocks resolved per windowed snapshot read (one header range + one batch
/// multi_get). Larger windows amortize the per-read MDBX transaction setup over
/// more blocks; the ceiling is the in-flight memory of buffered `Batch` bytes.
const PREFETCH_WINDOW: u64 = 128;
/// Windows the prefetch producer may stage ahead of the executor. Each window is
/// sent as a single `Vec<PlanBlock>`, so this is the channel depth in windows; a
/// few windows let the producer absorb a slow output group without stalling the
/// single reader. The memory ceiling is this many windows of buffered `Batch` bytes.
const PREFETCH_WINDOWS_AHEAD: usize = 4;

/// Configuration for a replay session.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// First block to replay (inclusive). Default 1 (genesis is block 0 and
    /// is established by archive_evm init).
    pub from_block: u64,
    /// Last block to replay (inclusive). `None` means walk to snapshot tip.
    pub to_block: Option<u64>,
    /// Verify the snapshot's `state_root` after every block. When false, only
    /// epoch-boundary blocks are verified.
    pub verify_every_block: bool,
    /// Emit a progress log every `progress_interval` blocks.
    pub progress_interval: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self { from_block: 1, to_block: None, verify_every_block: false, progress_interval: 500 }
    }
}

/// Pre-flight verification that `snapshot_evm` and `archive_evm` agree on
/// genesis, which transitively proves their `RaylsChainSpec` hardfork
/// schedules match. Run this once before `run_replay`.
///
/// Both `RethEnv` instances MUST be initialized with the same `RaylsChainSpec`
/// (same hardfork activation blocks, same per-fork pinned
/// bytecode subdirectories). If they disagree on genesis state, block 1's
/// execution diverges immediately; the explicit check here surfaces the cause
/// up front rather than at the first state-root mismatch deep in replay.
pub fn verify_chainspec_compatibility(
    snapshot_evm: &RethEnv,
    archive_evm: &RethEnv,
) -> ReplayResult<()> {
    let snap_genesis = snapshot_evm
        .sealed_header_by_number(0)
        .map_err(|e| ReplayError::SnapshotEnv(e.to_string()))?
        .ok_or(ReplayError::MissingHeader { block_number: 0 })?;
    let arch_genesis = archive_evm
        .sealed_header_by_number(0)
        .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?
        .ok_or(ReplayError::MissingHeader { block_number: 0 })?;

    if snap_genesis.hash() != arch_genesis.hash() {
        return Err(ReplayError::GenesisHashMismatch {
            ours: arch_genesis.hash(),
            expected: snap_genesis.hash(),
        });
    }
    info!(
        target: "rayls_replay",
        genesis_hash = ?snap_genesis.hash(),
        "chainspec compatibility verified at genesis"
    );
    Ok(())
}

/// Run the replay loop. Walks the plan, executes each block, returns the last
/// successfully executed block number.
///
/// `snapshot_evm` and `archive_evm` MUST be initialized with the same
/// `RaylsChainSpec` (same hardfork schedule). Call
/// [`verify_chainspec_compatibility`] before this to catch a mismatch at
/// genesis rather than during execution.
///
/// When the `shutdown` watch flips to `true` (by a signal handler), the loop
/// stops at the next output-group boundary, leaving a fully flushed, resumable
/// tip. The returned block number is the last committed tip in either case.
pub async fn run_replay<DB: Database>(
    snapshot_evm: &RethEnv,
    consensus_store: &DB,
    archive_evm: &RethEnv,
    rewards_counter: &RewardsCounter,
    tally_store: &SnapshotTallyStore,
    config: &ReplayConfig,
    shutdown: &watch::Receiver<bool>,
) -> ReplayResult<u64> {
    let mut parent = archive_evm.canonical_tip();
    let from = config.from_block.max(parent.number + 1);
    let snapshot_tip =
        snapshot_evm.last_block_number().map_err(|e| ReplayError::SnapshotEnv(e.to_string()))?;
    let to = match config.to_block {
        Some(requested) if requested > snapshot_tip => {
            warn!(
                target: "rayls_replay",
                requested,
                snapshot_tip,
                "--to-block exceeds snapshot tip; clamping to tip"
            );
            snapshot_tip
        }
        Some(requested) => requested,
        None => snapshot_tip,
    };

    info!(
        target: "rayls_replay",
        from,
        to,
        verify_every_block = config.verify_every_block,
        "starting scripted replay"
    );

    // load the consensus DB's BFT-committed epoch anchors and confirm the
    // snapshot's own execution DB agrees before trusting it as the replay oracle.
    let anchors = load_epoch_anchors(consensus_store, from, to);
    let agreement = cross_check_snapshot(snapshot_evm, &anchors);
    if agreement.disagreed > 0 {
        // the snapshot itself committed a fork; replay still runs (locating the
        // divergence is a forensic use case) but every verification downstream
        // is against a provably non-canonical oracle
        warn!(
            target: "rayls_replay",
            disagreed = agreement.disagreed,
            checked = agreement.checked,
            "snapshot's consensus and execution DBs disagree; replaying against an inconsistent oracle"
        );
    }

    let mut last = parent.number;
    if from > to {
        info!(target: "rayls_replay", from, to, "nothing to replay; archive already at target");
        return Ok(last);
    }

    // seed the heal oracle from the plan headers the producer already reads, so
    // the from-leaves self-heal never re-reads the snapshot per block
    let expected_roots: Arc<Mutex<HashMap<u64, B256>>> = Arc::new(Mutex::new(HashMap::new()));
    let oracle_roots = Arc::clone(&expected_roots);
    let oracle_snapshot = snapshot_evm.clone();
    archive_evm.set_canonical_root_oracle(Box::new(move |number| {
        oracle_roots.lock().get(&number).copied().or_else(|| {
            oracle_snapshot.header_by_number(number).ok().flatten().map(|h| h.state_root)
        })
    }));

    // producer derives plan blocks ahead on a blocking thread (pure reads),
    // overlapping the executor's block building; bounded channel = backpressure.
    let (tx, mut rx) = mpsc::channel::<ReplayResult<Vec<PlanBlock>>>(PREFETCH_WINDOWS_AHEAD);
    let producer_snapshot = snapshot_evm.clone();
    let producer_store = consensus_store.clone();
    let producer = tokio::task::spawn_blocking(move || {
        produce_plans(&producer_snapshot, &producer_store, from, to, &tx);
    });

    // mirror the live epoch manager: the committee comes from the on-chain
    // ConsensusRegistry at the archive tip, re-installed after every epoch close
    // so rotations are tracked; resume picks up mid-history membership the same way
    install_committee_from_contract(archive_evm, rewards_counter)?;

    // wall-clock anchor for throughput/ETA; observability only, never replayed state
    let replay_start = std::time::Instant::now();

    // buffer blocks of one consensus output (same `output_digest`) and execute the
    // whole output at once, matching the live orchestrator's per-output finalize.
    let mut divergence_count: u64 = 0;
    let mut group: Vec<PlanBlock> = Vec::with_capacity(PREFETCH_WINDOW as usize);
    let mut group_digest: Option<B256> = None;
    let mut interrupted = false;
    'replay: while let Some(item) = rx.recv().await {
        // one channel message carries a whole prefetch window; iterate it locally
        // so the output-group flush boundary is identical to per-item delivery
        for plan in item? {
            if group_digest.is_some_and(|d| plan.output_digest != d) {
                parent = flush_output_group(
                    archive_evm,
                    snapshot_evm,
                    &group,
                    parent,
                    rewards_counter,
                    tally_store,
                    &anchors,
                    config,
                    &mut divergence_count,
                    &expected_roots,
                )
                .await?;
                last = parent.number;
                if last.is_multiple_of(config.progress_interval) {
                    log_replay_progress(replay_start, from, to, last, divergence_count);
                }
                group.clear();
                // safe point: the completed output is flushed and the next has not
                // started, so a graceful stop here leaves a consistent, resumable tip
                if *shutdown.borrow() {
                    interrupted = true;
                    break 'replay;
                }
            }
            group_digest = Some(plan.output_digest);
            group.push(plan);
        }
    }
    // flush the final complete output only on normal completion; on shutdown the
    // half-buffered next group is dropped (it never executed, so its blocks simply
    // re-derive from the persisted tip on resume)
    if !interrupted && !group.is_empty() {
        parent = flush_output_group(
            archive_evm,
            snapshot_evm,
            &group,
            parent,
            rewards_counter,
            tally_store,
            &anchors,
            config,
            &mut divergence_count,
            &expected_roots,
        )
        .await?;
        last = parent.number;
    }

    // drop the receiver so the producer, if blocked on a full channel, unblocks
    // (its `blocking_send` returns Err) and its task finishes instead of hanging
    // this await on a graceful stop
    drop(rx);
    producer
        .await
        .map_err(|e| ReplayError::SnapshotEnv(format!("plan producer task failed: {e}")))?;

    let elapsed = replay_start.elapsed();
    let blocks = last.saturating_sub(from) + 1;
    let secs = elapsed.as_secs_f64();
    let blocks_per_sec = format!("{:.0}", if secs > 0.0 { blocks as f64 / secs } else { 0.0 });
    let elapsed_str = format_eta(elapsed.as_secs());
    info!(
        target: "rayls_replay",
        last,
        blocks,
        divergences = divergence_count,
        elapsed = %elapsed_str,
        blocks_per_sec = %blocks_per_sec,
        interrupted,
        "scripted replay finished"
    );
    Ok(last)
}

/// Emit a throttled progress line with percent complete, throughput, and ETA.
/// Targets `rayls_replay` so it reaches both the stdout and file sinks.
fn log_replay_progress(start: std::time::Instant, from: u64, to: u64, head: u64, divergences: u64) {
    let done = head.saturating_sub(from) + 1;
    let total = to.saturating_sub(from) + 1;
    let secs = start.elapsed().as_secs_f64();
    let rate = if secs > 0.0 { done as f64 / secs } else { 0.0 };
    let pct = if total > 0 { done as f64 / total as f64 * 100.0 } else { 0.0 };
    let remaining = total.saturating_sub(done);
    let eta = if rate > 0.0 {
        format_eta((remaining as f64 / rate) as u64)
    } else {
        "unknown".to_string()
    };
    let progress = format!("{pct:.1}%");
    let blocks_per_sec = format!("{rate:.0}");
    info!(
        target: "rayls_replay",
        head,
        progress = %progress,
        done,
        total,
        blocks_per_sec = %blocks_per_sec,
        eta = %eta,
        divergences,
        "scripted replay progress"
    );
}

/// Format a second count as a compact `HhMmSs` / `MmSs` / `Ss` duration.
fn format_eta(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}h {m:02}m {s:02}s")
    } else if m > 0 {
        format!("{m}m {s:02}s")
    } else {
        format!("{s}s")
    }
}

/// Execute one buffered consensus output: stage close-epoch tallies, run the group
/// through [`execute_output_group`], drain persistence, and run the persisted-root
/// guard. Returns the new canonical tip.
#[allow(clippy::too_many_arguments)]
async fn flush_output_group(
    archive_evm: &RethEnv,
    snapshot_evm: &RethEnv,
    group: &[PlanBlock],
    parent: SealedHeader,
    rewards_counter: &RewardsCounter,
    tally_store: &SnapshotTallyStore,
    anchors: &BTreeMap<u64, EpochAnchor>,
    config: &ReplayConfig,
    divergence_count: &mut u64,
    expected_roots: &Mutex<HashMap<u64, B256>>,
) -> ReplayResult<SealedHeader> {
    // seed the oracle with this group's canonical roots (already fetched by the
    // producer) so heal reads them instead of re-reading the snapshot per block
    {
        let mut roots = expected_roots.lock();
        roots.clear();
        for plan in group {
            roots.insert(plan.plan_header.number, plan.plan_header.state_root);
        }
    }

    // a close block's committed tally lives in its snapshot withdrawals; stage it
    // so the snapshot-backed RewardsBackend serves it during build
    for plan in group {
        if plan.close_epoch.is_some() {
            let (block_epoch, _) = RethEnv::deconstruct_nonce(plan.plan_header.nonce.into());
            let tally = snapshot_close_epoch_tally(snapshot_evm, plan.plan_header.number)?;
            tally_store.insert(block_epoch, tally);
        }
    }

    let (header, diverged) =
        execute_output_group(archive_evm, group, parent, config.verify_every_block, anchors)
            .await?;
    *divergence_count += diverged;

    // drain completed deferred persistence so periodic persists re-arm and
    // CanonicalInMemoryState does not grow unbounded
    archive_evm.check_persistence_completion();

    // concludeEpoch rotated the registry's committee; flush so the persisted-state
    // registry read sees the close block, then install the new epoch's membership
    if group.iter().any(|plan| plan.close_epoch.is_some()) {
        archive_evm
            .flush_persistence()
            .await
            .map_err(|e| ReplayError::ArchiveEnv(e.to_string()))?;
        install_committee_from_contract(archive_evm, rewards_counter)?;
    }
    Ok(header)
}

/// Derive plan blocks for `from..=to` in ascending order, sending each window
/// into `tx` as one `Vec<PlanBlock>`. Reads in windows to amortize transaction
/// setup; stops early when the consumer drops the receiver or the snapshot tip
/// is reached.
fn produce_plans<DB: Database>(
    snapshot_evm: &RethEnv,
    consensus_store: &DB,
    from: u64,
    to: u64,
    tx: &mpsc::Sender<ReplayResult<Vec<PlanBlock>>>,
) {
    let mut start = from;
    while start <= to {
        let end = start.saturating_add(PREFETCH_WINDOW - 1).min(to);
        match derive_plan_window(snapshot_evm, consensus_store, start, end) {
            Ok(plans) => {
                let requested = end - start + 1;
                let got = plans.len() as u64;
                // send the whole window in one message: ~PREFETCH_WINDOW fewer
                // channel send/recv round-trips per window than per-item delivery
                if !plans.is_empty() && tx.blocking_send(Ok(plans)).is_err() {
                    return;
                }
                if got < requested {
                    return;
                }
            }
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        }
        start = end + 1;
    }
}

/// Crate-root alias for [`ReplayError`].
pub use error::ReplayError as Error;
/// Crate-root alias for [`ReplayResult`].
pub use error::ReplayResult as Result;
