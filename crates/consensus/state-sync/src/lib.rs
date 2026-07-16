// SPDX-License-Identifier: BUSL-1.1
//! Code to sync consensus state between peers.
//! Currently used by nodes that are not participating in consensus
//! to follow along with consensus and execute blocks.

use consensus_metrics::monitored_future;
use rayls_consensus_primary::{
    consensus::ConsensusRound, network::PrimaryNetworkHandle, ConsensusBus, NodeMode,
    EXECUTION_STALL_TIMEOUT,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    tables::{Batches, ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache},
    CertificateStore, CheckpointStore, ConsensusStore, EpochStore, ReadTimeout,
};
use rayls_infrastructure_types::{
    AuthorityIdentifier, ConsensusHeader, ConsensusOutput, Database, DbTx, DbTxMut, Epoch,
    RaylsSender, SealedHeader, TaskSpawner, B256,
};
use tracing::{debug, error, info, trace, warn};

mod epoch;
pub use epoch::{epoch_committee_valid, spawn_epoch_record_collector};
mod consensus;

use consensus::spawn_track_recent_consensus;
pub use consensus::store_consensus_header_in_cache;

/// Populate startup-relevant watch channels on `ConsensusBus` from the DB.
/// Must run before any consensus task spawns.
pub fn prime_consensus<DB: Database>(consensus_bus: &ConsensusBus, config: &ConsensusConfig<DB>) {
    // Seed from the SSOT `executed_anchor` channel (NOT the consensus tip): on restart the engine
    // must re-execute every committed-but-unexecuted output ABOVE this point, so this has to
    // reflect what execution durably reached, not how far consensus committed. The anchor is
    // seeded once at boot from the highest-nonce recent block and advanced live by the engine.
    // `consensus_chain_tip` (the consensus tip) is for header *numbering* only; using it here
    // would let the replay skip outputs lost to a crash/static-file heal and fork the chain
    // (see epoch_manager/core.rs).
    let last_executed_block = consensus_bus.executed_anchor().borrow().clone();
    let epoch = config.epoch();
    let gc_depth = config.parameters().gc_depth;
    let last_number = last_executed_block.number;
    let last_leader_round = last_executed_block.sub_dag.leader_round();
    let last_leader_epoch = last_executed_block.sub_dag.leader_epoch();

    info!(
        target: "rayls-consensus-state-sync",
        epoch,
        last_number,
        last_leader_round,
        last_leader_epoch,
        gc_depth,
        "prime_consensus: begin"
    );

    // Staged derivation: cross-epoch reset, else leader_round if non-zero, else
    // block-number fallback for the degenerate default subdag case.
    let last_subdag = &last_executed_block.sub_dag;
    let (committed_round, derivation) = if last_subdag.leader_epoch() < epoch {
        info!(
            target: "rayls-consensus-state-sync",
            last_leader_epoch, epoch,
            "prime_consensus: cross-epoch reset, committed_round=0"
        );
        (0u32, "cross-epoch-reset")
    } else {
        let round = last_subdag.leader_round();
        if round > 0 {
            (round, "leader-round")
        } else {
            let fallback = last_executed_block.number.min(u32::MAX as u64) as u32;
            if fallback > 0 {
                info!(
                    target: "rayls-consensus-state-sync",
                    block_number = last_executed_block.number,
                    "prime_consensus: leader_round=0, falling back to block number"
                );
            }
            (fallback, "block-number-fallback")
        }
    };

    consensus_bus
        .update_consensus_rounds(ConsensusRound::new_with_gc_depth(committed_round, gc_depth));
    consensus_bus.primary_round_updates().send_replace(committed_round);

    // Seed cert_store_round from the same DB read so the subscriber's
    // rejoin gate and cert_manager's threshold see a consistent view.
    let gc_round = committed_round.saturating_sub(gc_depth);
    let (cert_store_max, cert_count) =
        match config.node_storage().after_round(gc_round.saturating_add(1), ReadTimeout::Exempt) {
            Ok(certs) => {
                let count = certs.len();
                let max = certs.iter().map(|c| c.round()).max();
                (max, count)
            }
            Err(e) => {
                warn!(
                    target: "rayls-consensus-state-sync",
                    err = ?e,
                    "prime_consensus: after_round failed; cert_store_round left at prior value"
                );
                (None, 0)
            }
        };
    let prior_cert_store_round = *consensus_bus.cert_store_round().borrow();
    if let Some(max_round) = cert_store_max {
        consensus_bus.cert_store_round().send_if_modified(|current| {
            if max_round > *current {
                *current = max_round;
                true
            } else {
                false
            }
        });
    }
    let new_cert_store_round = *consensus_bus.cert_store_round().borrow();

    info!(
        target: "rayls-consensus-state-sync",
        epoch,
        committed_round,
        derivation,
        gc_round,
        gc_depth,
        cert_count,
        cert_store_max = ?cert_store_max,
        prior_cert_store_round,
        new_cert_store_round,
        "prime_consensus: complete"
    );
}

/// Spawn the state sync tasks. `last_streamed_number` is the last consensus block number sent by
/// `stream_missing_consensus` so the streaming task starts without overlap.
pub fn spawn_state_sync<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    task_manager: TaskSpawner,
    last_streamed_number: u64,
) {
    let mode = *consensus_bus.node_mode().borrow();
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {}
        NodeMode::CvvInactive | NodeMode::Observer => {
            // If we are not an active CVV then follow latest consensus from peers.
            let (config_clone, consensus_bus_clone) = (config.clone(), consensus_bus.clone());
            // The forward streamer fills gaps by number directly from peers, so both tasks need a
            // handle.
            let network_clone = network.clone();
            task_manager.spawn_task(
                "state sync: track latest consensus header from peers",
                monitored_future!(
                    async move {
                        info!(target: "rayls-consensus-state-sync", "Starting state sync: track latest consensus header from peers");
                        if let Err(e) = spawn_track_recent_consensus(config_clone, consensus_bus_clone, network_clone).await {
                            error!(target: "rayls-consensus-state-sync", "Error tracking latest consensus headers: {e}");
                        }
                    },
                    "StateSyncLatestConsensus"
                ),
            );
            task_manager.spawn_task(
                "state sync: stream consensus headers",
                monitored_future!(
                    async move {
                        info!(target: "rayls-consensus-state-sync", "Starting state sync: stream consensus header from peers");
                        if let Err(e) = spawn_stream_consensus_headers(config, consensus_bus, network, last_streamed_number).await {
                            error!(target: "rayls-consensus-state-sync", "Error streaming consensus headers: {e}");
                        }
                    },
                    "StateSyncStreamConsensusHeaders"
                ),
            );
        }
    }
}
/// Write the consensus header and its component transaction batches to the consensus DB.
///
/// An error here indicates a critical node failure.
/// Note, if this returns an error then the DB could not be written to- this is probably fatal.
pub fn save_consensus<DB: Database>(
    db: &DB,
    consensus_output: ConsensusOutput,
    _authority_id: &Option<AuthorityIdentifier>,
) -> eyre::Result<()> {
    let batches_to_insert: Vec<_> = consensus_output
        .batches
        .iter()
        .flat_map(|certified_batches| certified_batches.batches.iter())
        .map(|batch| (batch.digest(), batch.clone()))
        .collect();

    let header: ConsensusHeader = consensus_output.into();
    let header_digest = header.digest();

    // The ByDigest index is append-only, so a re-commit under a different digest leaves a stale
    // entry. Capture the prior digests at this number before the write overwrites them, to prune
    // below. Read in its own txn: `LayeredDbTxMut::get` panics inside a write txn.
    let priors = db.with_read_txn(|txn| {
        Ok([
            txn.get::<ConsensusBlocks>(&header.number)?,
            txn.get::<ConsensusBlocksCache>(&header.number)?,
        ])
    })?;
    let superseded: Vec<B256> = priors
        .into_iter()
        .flatten()
        .map(|prior| prior.digest())
        .filter(|&digest| digest != header_digest)
        .collect();

    db.with_write_txn(|txn| {
        for (digest, batch) in &batches_to_insert {
            if let Err(e) = txn.insert::<Batches>(digest, batch) {
                error!(target: "state-sync", ?e, "error saving a batch to persistent storage!");
                return Err(e);
            }
        }
        if let Err(e) = txn.insert::<ConsensusBlocks>(&header.number, &header) {
            error!(target: "rayls-consensus-state-sync", ?e, "error saving a consensus header to persistent storage!");
            return Err(e);
        }
        if let Err(e) = txn.insert::<ConsensusBlockNumbersByDigest>(&header_digest, &header.number)
        {
            error!(target: "rayls-consensus-state-sync", ?e, "error saving a consensus header number to persistent storage!");
            return Err(e);
        }
        // promote: remove from cache now that it lives in ConsensusBlocks.
        // batch cleanup of old cache entries was removed because scanning
        // the table was slowing down recovery processing. each entry is
        // cleaned 1:1 as it is promoted through this path.
        let _ = txn.remove::<ConsensusBlocksCache>(&header.number);

        // Prune the digests this header supersedes at its number; this only fixes the common 1:1
        // re-commit, so by-hash readers must still re-check digest == hash (older orphans persist).
        for stale_digest in &superseded {
            let _ = txn.remove::<ConsensusBlockNumbersByDigest>(stale_digest);
        }

        // Do not clean NodeBatchesCache table here. It must be clean in `process_committed_headers` in the proposer`
        Ok(())
    })?;

    Ok(())
}

/// The canonical consensus-chain tip, used to seed the live subscriber's header *numbering* so a
/// number the network already consumed is never reused.
///
/// Returns the highest `ConsensusBlocks` header on the canonical chain. The raw table tip is not
/// always canonical: a drain race at an epoch boundary can leave prior-epoch outputs saved above
/// the certified checkpoint (a "post-boundary leak"). Seeding numbering from such a leak offsets
/// every new-epoch header by the leak count and forks the chain via a divergent `ConsensusHeader`
/// digest (which feeds the block's `mix_hash` / `parent_beacon_block_root`). When the tip is a
/// prior-epoch header above the certified checkpoint, the certified (committee-signed) checkpoint
/// is returned instead, so the new epoch's first number is the one every validator agrees on.
///
/// A current-epoch tip is returned directly, so numbering still sits >= the execution anchor when
/// execution lags the commit (a crash/static-file heal must not regress numbering and reuse a
/// number). IMPORTANT: use this ONLY for numbering. Do NOT anchor re-execution/replay on it - that
/// must stay on the SSOT `executed_anchor`, or committed-but-unexecuted outputs get skipped on
/// restart and the chain forks. See the note in `epoch_manager/core.rs`.
///
/// Returns `None` only when the consensus DB is empty (fresh boot); execution is at 0 then too,
/// since `save_consensus` persists each header before it executes.
pub fn consensus_chain_tip<DB: Database>(config: &ConsensusConfig<DB>) -> Option<ConsensusHeader> {
    let db = config.node_storage();
    let (_, tip) = db.last_record::<ConsensusBlocks>()?;

    let epoch = config.epoch();
    // A current-epoch tip is canonical; return it directly. This also keeps numbering >= the
    // execution anchor when execution lags the commit within an epoch.
    if tip.sub_dag.leader_epoch() >= epoch {
        return Some(tip);
    }

    // A prior-epoch tip above the certified checkpoint is a post-boundary leak (saved during the
    // drain race, not on the canonical chain): re-anchor numbering to the certified checkpoint -
    // the epoch-end output every validator signed - so the new epoch's first number cannot
    // fork. Without a certified checkpoint we cannot prove a leak, so trust the tip.
    match certified_consensus_checkpoint(db, epoch) {
        Some(checkpoint) if tip.number > checkpoint => db.get_consensus_by_number(checkpoint),
        _ => Some(tip),
    }
}

/// Resolve the consensus header an executed EL block anchors to, via its
/// `parent_beacon_block_root`. Returns `None` for a genesis/zero anchor or an absent header.
///
/// Use at startup, before `recently_executed_blocks` is primed, when the EL canonical tip is the
/// only source of the last executed consensus header.
pub fn last_executed_consensus_from_anchor<DB: Database>(
    parent_beacon_block_root: Option<B256>,
    db: &DB,
) -> Option<ConsensusHeader> {
    let hash = parent_beacon_block_root.filter(|h| !h.is_zero())?;
    db.get_canonical_consensus_by_hash(hash)
}

/// Pick the consensus-header anchor (`parent_beacon_block_root`) of the highest-nonce header in a
/// recent EVM block window. Returns `None` for an empty window.
///
/// The canonical tip is NOT a reliable anchor: a drained parked (out-of-order seq) batch's block is
/// stamped with its ORIGIN output's lower nonce and that output's digest as
/// `parent_beacon_block_root`, yet it lands AFTER the in-order filler and becomes the tip. So the
/// tip can anchor to a PREVIOUS output and regress the restart watermark below the true highest
/// executed output. The EVM nonce packs `(epoch << 32) | round`, so plain `u64` ordering matches
/// output ordering, and the max-nonce header in the window identifies the true highest executed
/// output's consensus-header digest.
pub fn highest_executed_anchor(headers: &[SealedHeader]) -> Option<B256> {
    headers
        .iter()
        .max_by_key(|header| u64::from(header.nonce))
        .and_then(|header| header.parent_beacon_block_root)
}

/// Send any consensus headers that were not executed before last shutdown to the consensus header
/// channel. Returns the last consensus block number that was streamed (or the last executed number
/// if none were missing).
pub async fn stream_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<u64> {
    // Load our last executed consensus block from the SSOT `executed_anchor` channel.
    let last_executed_block = consensus_bus.executed_anchor().borrow().clone();

    if last_executed_block.number == 0 {
        trace!(
            target: "rayls-consensus-state-sync",
            "stream_missing_consensus using DEFAULT consensus block (number 0) - this may cause re-execution"
        );
    }

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let db = config.node_storage();
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));

    debug!(target: "rayls-consensus-state-sync", ?last_executed_block, ?last_db_block, "comparing last executed block and last recorded consensus block");

    let mut last_streamed_number = last_executed_block.number;

    // if the last recorded consensus block is larger than the last executed block,
    // forward the stored consensus block to engine for execution
    let gap = last_db_block.number.saturating_sub(last_executed_block.number);
    if gap > 0 {
        info!(
            target: "rayls-consensus-state-sync",
            last_executed = last_executed_block.number,
            last_db = last_db_block.number,
            gap,
            "stream_missing_consensus: streaming unexecuted blocks from DB"
        );
        for consensus_header in collect_replayable_headers(
            db,
            config.epoch(),
            last_executed_block.number,
            last_db_block.number,
        ) {
            let number = consensus_header.number;
            consensus_bus.consensus_header().send(consensus_header).await?;
            last_streamed_number = number;
        }
        info!(
            target: "rayls-consensus-state-sync",
            last_streamed_number,
            "stream_missing_consensus: done streaming"
        );
    } else {
        info!(
            target: "rayls-consensus-state-sync",
            last_executed = last_executed_block.number,
            last_db = last_db_block.number,
            "stream_missing_consensus: no gap, nothing to stream"
        );
    }

    Ok(last_streamed_number)
}

/// Resolve the signed last-consensus-block number of the epoch before `epoch`, from its certified
/// [`EpochRecord`]. Requires the certificate (an uncertified local record is not trusted) and
/// returns `None` on a fresh node, leaving the replay guard inert.
fn certified_consensus_checkpoint<DB: Database>(db: &DB, epoch: Epoch) -> Option<u64> {
    let prev = epoch.checked_sub(1)?;
    let (record, cert) = db.get_epoch_by_number(prev)?;
    cert?;
    db.get_canonical_consensus_by_hash(record.parent_consensus).map(|header| header.number)
}

/// Collect saved headers in `anchor_number+1..=to_number` to replay, stopping at the first prior-
/// epoch output (`leader_epoch < epoch`): a leak past the boundary that must not re-execute.
///
/// The certified `parent_consensus` only logs divergence; it is not a ceiling, since a within-epoch
/// restart legitimately replays current-epoch outputs above it.
fn collect_replayable_headers<DB: Database>(
    db: &DB,
    epoch: Epoch,
    anchor_number: u64,
    to_number: u64,
) -> Vec<ConsensusHeader> {
    let certified_checkpoint = certified_consensus_checkpoint(db, epoch);

    // An execution anchor above the signed checkpoint means the node already diverged.
    if let Some(checkpoint) = certified_checkpoint {
        if anchor_number > checkpoint {
            error!(
                target: "rayls-consensus-state-sync",
                anchor_number,
                certified_checkpoint = checkpoint,
                "execution anchor advanced past the certified epoch checkpoint - node diverged from its signed EpochRecord"
            );
        }
    }

    let mut result = Vec::new();
    for number in anchor_number + 1..=to_number {
        let Some(consensus_header) = db.get_consensus_by_number(number) else { continue };
        let leader_epoch = consensus_header.sub_dag.leader_epoch();

        if leader_epoch < epoch {
            warn!(
                target: "rayls-consensus-state-sync",
                number,
                leader_epoch,
                epoch,
                ?certified_checkpoint,
                "halting catch-up replay: saved output belongs to a closed epoch (post-boundary leak)"
            );
            break;
        }

        debug!(target: "rayls-consensus-state-sync", ?consensus_header, "collecting unexecuted consensus header");
        result.push(consensus_header);
    }
    result
}

/// Collect and return any consensus headers that were not executed before last shutdown.
/// This will be consensus that was reached but had not executed before a shutdown.
///
/// ## Interaction with checkpoint-based crash recovery
///
/// The manager's `recover_partial_transition()` runs BEFORE this function is called.
/// That method checks for incomplete epoch transitions (via a DB checkpoint) and either
/// completes the remaining phases or clears a stale checkpoint. By the time this
/// function executes, the checkpoint system guarantees that `recently_executed_blocks` accurately
/// reflects the last executed block.
///
/// If a checkpoint still exists when this function runs, it indicates an unexpected
/// ordering issue -- recovery should have handled it already. We log a warning but
/// proceed, as the worst case is a harmless replay: the `ExecutorEngine` drops
/// duplicate/out-of-order outputs via `last_seen_output_number` (processor `<=` check).
pub async fn get_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    let db = config.node_storage();

    // Defensive: verify no epoch transition checkpoint is active. The manager's
    // recover_partial_transition() should clear or complete it before we run.
    let epoch = config.epoch();
    if let Ok(Some(checkpoint)) = db.load_checkpoint(epoch) {
        warn!(
            target: "rayls-consensus-state-sync",
            ?epoch,
            phase = ?checkpoint.completed_phase,
            "get_missing_consensus called with an active transition checkpoint - \
             recover_partial_transition() should have handled this first"
        );
    }

    // Load our last executed consensus block from the SSOT `executed_anchor` channel.
    let last_executed_block = consensus_bus.executed_anchor().borrow().clone();

    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));

    debug!(target: "rayls-consensus-state-sync", ?last_executed_block, ?last_db_block, "comparing last executed block and last recorded consensus block");

    // if the last recorded consensus block is larger than the last executed block,
    // forward the stored consensus block to engine for execution
    if last_db_block.number > last_executed_block.number {
        result =
            collect_replayable_headers(db, epoch, last_executed_block.number, last_db_block.number);
    }

    debug!(target: "rayls-consensus-state-sync", ?result, "missing consensus headers that need execution:");
    Ok(result)
}

/// Retry interval for incomplete catch-up attempts.
const CATCH_UP_RETRY_INTERVAL_MS: u64 = 500;

/// Spawn task to stream consensus headers from last saved to current.
///
/// Only use when NOT participating in active consensus. Includes periodic retry
/// for incomplete catch-up attempts.
/// `last_streamed_number` is the last consensus block already sent by `stream_missing_consensus`.
async fn spawn_stream_consensus_headers<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    last_streamed_number: u64,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();

    let mut rx_last_consensus_header = consensus_bus.last_consensus_header().subscribe();
    // Anchor the digest chain on the executed-anchor SSOT: it is canonical and monotonic, so it can
    // never be a stale cache row, and the streamer chains every fetched header onto its digest.
    let mut last_consensus_header = consensus_bus.executed_anchor().borrow().clone();
    let mut last_consensus_height = last_consensus_header.number;

    let mut pending_catch_up_target: Option<ConsensusHeader> = None;

    info!(
        target: "rayls-consensus-state-sync",
        last_streamed_number,
        last_consensus_height,
        "stream handoff: starting forward streamer"
    );

    let mut idle_intervals = 0u64;

    loop {
        tokio::select! {
            biased;

            _ = &rx_shutdown => {
                return Ok(())
            }

            _ = rx_last_consensus_header.changed() => {
                idle_intervals = 0;
                let header = rx_last_consensus_header.borrow_and_update().clone();
                debug!(
                    target: "rayls-consensus-state-sync",
                    new_header = header.number,
                    current = last_consensus_height,
                    gap = header.number.saturating_sub(last_consensus_height),
                    "forward streamer: watch triggered"
                );

                if header.number > last_consensus_height {
                    pending_catch_up_target = attempt_catch_up(
                        &config, &consensus_bus, &network,
                        &mut last_consensus_header, &mut last_consensus_height,
                        header, "forward streamer",
                    ).await;
                }
            }

            // retry catch-up after interval if there's a pending target
            _ = tokio::time::sleep(std::time::Duration::from_millis(CATCH_UP_RETRY_INTERVAL_MS)),
                if pending_catch_up_target.is_some() => {
                if let Some(target) = pending_catch_up_target.clone() {
                    if target.number > last_consensus_height {
                        pending_catch_up_target = attempt_catch_up(
                            &config, &consensus_bus, &network,
                            &mut last_consensus_header, &mut last_consensus_height,
                            target, "forward streamer retry",
                        ).await;
                    } else {
                        pending_catch_up_target = None;
                    }
                }
            }

            // idle timeout - no gossip-driven catch-up trigger
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)),
                if pending_catch_up_target.is_none() => {
                idle_intervals += 1;
                info!(
                    target: "rayls-consensus-state-sync",
                    current = last_consensus_height,
                    idle_secs = idle_intervals * 30,
                    "forward streamer: no new consensus headers received, catch-up idle"
                );
            }

        }
    }
}

/// Run catch-up and update state. Return the new `pending_catch_up_target`.
async fn attempt_catch_up<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    last_header: &mut ConsensusHeader,
    last_height: &mut u64,
    target: ConsensusHeader,
    label: &str,
) -> Option<ConsensusHeader> {
    match catch_up_consensus_from_to(
        config,
        consensus_bus,
        network,
        last_header.clone(),
        target.clone(),
    )
    .await
    {
        Ok(result) => {
            let pending = if result.number < target.number {
                debug!(
                    target: "rayls-consensus-state-sync",
                    caught_up_to = result.number,
                    target = target.number,
                    remaining = target.number - result.number,
                    "{label}: incomplete catch-up, scheduling retry"
                );
                Some(target)
            } else {
                info!(
                    target: "rayls-consensus-state-sync",
                    result = result.number,
                    "{label}: catch-up complete"
                );
                None
            };
            *last_header = result;
            *last_height = last_header.number;
            pending
        }
        Err(e) => {
            warn!(
                target: "rayls-consensus-state-sync",
                current = *last_height,
                target = target.number,
                "{label}: catch-up failed (will retry): {e}"
            );
            Some(target)
        }
    }
}

/// Applies consensus output from (exclusive) to max_consensus_height (inclusive).
/// Returns the last applied ConsensusHeader.
async fn catch_up_consensus_from_to<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    from: ConsensusHeader,
    max_consensus: ConsensusHeader,
) -> eyre::Result<ConsensusHeader> {
    // `from` is the streamer's anchor, seeded from the executed-anchor SSOT and only ever advanced
    // to digest-verified headers, so its digest is the true parent of header `from.number + 1`.
    let mut last_parent = from.digest();

    let last_consensus_height = from.number;
    let max_consensus_height = max_consensus.number;
    if last_consensus_height >= max_consensus_height {
        return Ok(from);
    }
    let total = max_consensus_height - last_consensus_height;
    if total > 1 {
        info!(
            target: "rayls-consensus-state-sync",
            from = last_consensus_height,
            to = max_consensus_height,
            total,
            "catch_up_from_to: starting"
        );
    }
    let db = config.node_storage();
    let mut result_header = from;
    let mut streamed = 0u64;
    for number in last_consensus_height + 1..=max_consensus_height {
        // A by-number gap fetch is unverified (only the number is checked), so it is cached only
        // after the digest-chain guard below links it onto the verified parent; caching earlier
        // poisons every retry until the backwards walk overwrites it.
        let mut from_gap_fetch = false;
        let consensus_header = if number == max_consensus_height {
            max_consensus.clone()
        } else if let Some(block) = db.get_consensus_by_number(number) {
            block
        } else {
            // The backwards walk has not refilled this number yet; fetch it by number rather than
            // wedge behind one slow hash. Safety rests on the later gates (the digest-chain guard
            // below, and the subscriber's BLS re-verify before execution), not this fetch.
            match network.request_consensus(Some(number), None).await {
                Ok(header) => {
                    from_gap_fetch = true;
                    header
                }
                Err(_) => {
                    warn!(
                        target: "rayls-consensus-state-sync",
                        number,
                        streamed,
                        remaining = max_consensus_height - number + 1,
                        "catch_up_from_to: header not in DB and by-number fetch failed, returning early (awaiting backwards walk)"
                    );
                    return Ok(result_header);
                }
            }
        };
        let parent_hash = last_parent;
        last_parent =
            ConsensusHeader::digest_from_parts(parent_hash, &consensus_header.sub_dag, number);
        if last_parent != consensus_header.digest() {
            // `number` does not chain onto the row at `number - 1`. A gap fetch proves only the
            // reply's parent_hash and number, not its sub_dag, so a bad peer can plant a row the
            // next real header cannot link to; drop that poisoned non-canonical row to force a
            // re-fetch. Never drop a canonical row: a mismatch there is a real fork (needs an
            // operator restore), and removing history the walk cannot rebuild loses state silently.
            let poisoned_parent_number = number - 1;
            if poisoned_parent_number > last_consensus_height
                && db
                    .get::<ConsensusBlocks>(&poisoned_parent_number)
                    .ok()
                    .flatten()
                    .map(|h| h.digest())
                    != Some(parent_hash)
            {
                let _ = db.with_write_txn(|txn| {
                    let _ = txn.remove::<ConsensusBlocksCache>(&poisoned_parent_number);
                    let _ = txn.remove::<ConsensusBlockNumbersByDigest>(&parent_hash);
                    Ok(())
                });
            }
            warn!(
                target: "rayls-consensus-state-sync",
                number,
                expected = ?last_parent,
                actual = ?consensus_header.digest(),
                "consensus header digest mismatch - retrying (a non-canonical poison row is cleared to self-heal; a canonical fork needs a DB restore)"
            );
            return Err(eyre::eyre!("consensus header digest mismatch at number {number}"));
        }

        // Guard passed; persist a gap-fetched header now (DB-hit and max_consensus are stored).
        if from_gap_fetch {
            store_consensus_header_in_cache(db, &consensus_header);
        }

        let base_execution_block = consensus_header.sub_dag.leader.header().latest_execution_block;
        if streamed.is_multiple_of(100) {
            let recent_latest =
                consensus_bus.recently_executed_blocks().borrow().latest_block_num_hash();
            info!(
                target: "rayls-consensus-state-sync",
                consensus_number = number,
                exec_current = recent_latest.number,
                streamed,
                leader_epoch = consensus_header.sub_dag.leader_epoch(),
                "catch_up_from_to: streaming progress"
            );
        }
        // Gate streaming on execution so we never outrun it and miss a fork. Bounded so a target
        // that never executes errors out instead of parking forever; the error is surfaced to
        // retry.
        if let Err(wait_err) = consensus_bus
            .wait_for_execution_bounded(base_execution_block, EXECUTION_STALL_TIMEOUT)
            .await
        {
            // A genuine `Forked` recurs and needs an operator DB restore: catch-up cannot rebuild
            // forked committed history.
            let recent = consensus_bus.recently_executed_blocks().borrow();
            let recent_oldest = recent.oldest_block_number();
            let recent_latest = recent.latest_block_num_hash();
            let recent_len = recent.len();
            drop(recent);

            error!(
                target: "rayls-consensus-state-sync",
                consensus_header_number = number,
                consensus_leader_round = consensus_header.sub_dag.leader_round(),
                base_exec_block_number = base_execution_block.number,
                base_exec_block_hash = ?base_execution_block.hash,
                recently_executed_blocks_oldest = recent_oldest,
                recently_executed_blocks_latest_number = recent_latest.number,
                recently_executed_blocks_latest_hash = ?recent_latest.hash,
                recently_executed_blocks_len = recent_len,
                %wait_err,
                "catch-up could not confirm execution reached the referenced block"
            );

            return Err(eyre::eyre!(
                "catch-up could not confirm execution of consensus header {} \
                 (references execution block {}, hash {:?}): {wait_err}. \
                 recently_executed_blocks range is [{}, {}] with {} blocks; the streamer will retry.",
                number,
                base_execution_block.number,
                base_execution_block.hash,
                recent_oldest,
                recent_latest.number,
                recent_len
            ));
        }
        consensus_bus.consensus_header().send(consensus_header.clone()).await?;
        streamed += 1;
        result_header = consensus_header;
    }
    info!(
        target: "rayls-consensus-state-sync",
        from = last_consensus_height,
        to = max_consensus_height,
        streamed,
        "catch_up_from_to: finished"
    );
    Ok(result_header)
}

#[cfg(test)]
mod tests;
