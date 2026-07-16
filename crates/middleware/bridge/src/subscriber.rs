//! Subscriber handles consensus output.

use crate::{errors::SubscriberResult, SubscriberError};
use consensus_metrics::monitored_future;
use futures::{future::BoxFuture, stream::FuturesOrdered, StreamExt};
use rayls_consensus_primary::{
    consensus::ConsensusRound, network::PrimaryNetworkHandle, ConsensusBus, NodeMode,
};
// production-only: consensus-result hashing for gossip signatures
#[cfg(not(feature = "dev-single-node-setup"))]
use rayls_consensus_primary::network::ConsensusResult;
use rayls_consensus_state_sync::{
    consensus_chain_tip, get_missing_consensus, save_consensus, spawn_state_sync,
    store_consensus_header_in_cache, stream_missing_consensus,
};
use rayls_infrastructure_config::ConsensusConfig;
// production-only: consensus-output gossip (dev single-node has no peers to gossip to)
#[cfg(not(feature = "dev-single-node-setup"))]
use rayls_infrastructure_config::LibP2pConfig;
use rayls_infrastructure_network_types::{local::LocalNetwork, PrimaryToWorkerClient};
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{
    Address, AuthorityIdentifier, Batch, BlockHash, CameFrom, CertifiedBatch, CommittedSubDag,
    Committee, ConsensusHeader, ConsensusOutput, Database, Epoch, Hash as _, Noticer,
    RaylsReceiver, RaylsSender, Round, TaskKind, TaskManager, TaskSpawner, Timestamp, TimestampSec,
    B256,
};
// production-only: signing the consensus result for gossip
#[cfg(not(feature = "dev-single-node-setup"))]
use rayls_infrastructure_types::{encode, to_intent_message, BlsSigner as _};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
#[derive(Clone, Debug)]
pub struct Subscriber<DB> {
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBus,
    /// Consensus configuration (contains the consensus DB)
    config: ConsensusConfig<DB>,
    /// The handle to the network.
    network_handle: PrimaryNetworkHandle,
    /// Inner state.
    inner: Arc<Inner>,
}

/// Inner subscriber type.
#[derive(Debug)]
struct Inner {
    /// The identifier for the authority.
    ///
    /// Used for logging, None if we are not a validator.
    authority_id: Option<AuthorityIdentifier>,
    /// The committee for the epoch.
    committee: Committee,
    /// The client to request worker batches and build consensus output.
    client: LocalNetwork,
}

/// Spawn the subscriber in the correct mode based on the validator status for the current epoch.
pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    task_manager: &TaskManager,
    network_handle: PrimaryNetworkHandle,
    to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
    execution_replay_completed: tokio::sync::watch::Sender<()>,
) {
    let authority_id = config.authority_id();
    let committee = config.committee().clone();
    let client = config.local_network().clone();
    let mode = *consensus_bus.node_mode().borrow();
    let subscriber = Subscriber {
        consensus_bus,
        config,
        network_handle,
        inner: Arc::new(Inner { authority_id, committee, client }),
    };
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {
            task_manager.spawn_classified_task(
                "subscriber consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: CVV");
                        match subscriber
                            .run(rx_shutdown, to_engine.clone(), execution_replay_completed)
                            .await
                        {
                            Ok(()) => {
                                info!(target: "subscriber", "subscriber consensus exited normally")
                            }
                            Err(e) => panic!("subscriber consensus failed fatally: {e}"),
                        }
                    },
                    "SubscriberTask"
                ),
                TaskKind::Drainable,
            );
        }
        NodeMode::CvvInactive => {
            let clone = task_manager.get_spawner();
            // If we are not active but are a CVV then catch up and rejoin.
            task_manager.spawn_classified_task(
                "subscriber catch up and rejoin consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: Catch up and rejoin");
                        match subscriber.catch_up_rejoin_consensus(clone, rx_shutdown).await {
                            Ok(()) => {
                                info!(target: "subscriber", "subscriber catch-up exited normally")
                            }
                            Err(e) => panic!("subscriber catch-up failed fatally: {e}"),
                        }
                    },
                    "SubscriberFollowTask"
                ),
                TaskKind::Drainable,
            );
        }
        NodeMode::Observer => {
            let clone = task_manager.get_spawner();
            // If we are not active then just follow consensus.
            task_manager.spawn_classified_task(
                "subscriber follow consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: Follower");
                        match subscriber.follow_consensus(clone, rx_shutdown).await {
                            Ok(()) => {
                                info!(target: "subscriber", "subscriber follow exited normally")
                            }
                            Err(e) => panic!("subscriber follow consensus failed fatally: {e}"),
                        }
                    },
                    "SubscriberFollowTask"
                ),
                TaskKind::Drainable,
            );
        }
    }
}

/// Returns true if `number` is the next consensus header to process.
///
/// Pure predicate: does not advance the watermark, so a header that later fails verification
/// cannot skip the honest header at the same number.
fn should_process_consensus_header(number: u64, last_processed: u64) -> bool {
    if number <= last_processed {
        debug!(target: "subscriber", "skipping duplicate consensus header {number}, last processed: {last_processed}");
        return false;
    }
    if number != last_processed + 1 {
        warn!(
            target: "subscriber",
            expected = last_processed + 1,
            got = number,
            gap = number - last_processed - 1,
            "skipping out-of-order consensus header (forward streamer will deliver sequentially)"
        );
        return false;
    }
    true
}

/// The `(epoch, leader round)` of the most recent commit a follower admitted for execution.
///
/// A streamed header must advance this to be a legitimate successor; one that does not is a commit
/// replayed at a wrong number, rejected before execution. The digest-chain guard cannot catch this
/// (it proves parent linkage, not placement). Exposes only `advance`, so the position cannot
/// regress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AcceptedPosition {
    /// Leader epoch, ordered before `round` so a higher epoch advances despite the boundary
    /// reseeding the round low.
    epoch: Epoch,
    /// Leader round within `epoch`; must strictly increase for a same-epoch successor.
    round: Round,
}

impl AcceptedPosition {
    /// Seeds the position from an executed-anchor sub-dag (a genesis anchor yields `(0, 0)`).
    fn from_sub_dag(sub_dag: &CommittedSubDag) -> Self {
        Self { epoch: sub_dag.leader_epoch(), round: sub_dag.leader_round() }
    }

    /// Advances to `sub_dag`'s `(epoch, round)` if strictly greater (lexicographic, derived `Ord`),
    /// returning whether it advanced. A non-advancing sub-dag leaves the position untouched.
    fn advance(&mut self, sub_dag: &CommittedSubDag) -> bool {
        let next = Self::from_sub_dag(sub_dag);
        let advanced = next > *self;
        if advanced {
            *self = next;
        }
        advanced
    }
}

/// What a committer should do with a consensus output, per the [`EpochCut`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutAction {
    /// Persist and forward the output; it belongs to the current epoch.
    Keep,
    /// Persist and forward the output; it is the epoch-closing block, so nothing after it is kept.
    Close,
    /// Drop the output; it is past the boundary and belongs to the next epoch.
    Drop,
}

/// The deterministic epoch-boundary cut over a committer's output stream.
///
/// Both the live and catch-up committers drive one of these so they stop persisting at the same
/// subdag: the first output reaching the on-chain `epoch_boundary` timestamp is the epoch-closing
/// block ([`CutAction::Close`]), and every output after it belongs to the next epoch
/// ([`CutAction::Drop`]). Persisting a next-epoch output would inflate the consensus tip and reseed
/// the next epoch's numbering above the certified checkpoint, forking via a divergent
/// `ConsensusHeader` digest.
#[derive(Debug, Default)]
struct EpochCut {
    crossed: bool,
}

impl EpochCut {
    /// Classify `output` against the boundary, latching once the epoch-closing block is seen.
    ///
    /// `epoch_boundary` is `u64::MAX` until the boundary is known, so this never cuts early.
    fn classify(&mut self, output: &ConsensusOutput, epoch_boundary: TimestampSec) -> CutAction {
        if self.crossed {
            return CutAction::Drop;
        }
        if output.reaches_epoch_boundary(epoch_boundary) {
            self.crossed = true;
            return CutAction::Close;
        }
        CutAction::Keep
    }
}

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Max concurrent batch fetches during catchup.
    const MAX_CATCHUP_PIPELINE: usize = 16;

    /// Maximum wait time for committee keys before giving up on a header.
    const COMMITTEE_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Interval between retries when waiting for committee keys.
    const COMMITTEE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

    /// Throttle `try_rejoin_consensus` waiting-gate logs to once per this many headers.
    const REJOIN_LOG_THROTTLE: u64 = 100;

    /// Verify BLS signatures on the consensus header's certificates.
    ///
    /// Wait for committee keys if not yet available (triggers epoch record collector).
    /// Returns the verified header on success, or None if it should be discarded.
    async fn verify_consensus_certificates(
        &self,
        consensus_header: ConsensusHeader,
        shutdown: &Noticer,
    ) -> Option<ConsensusHeader> {
        let leader_epoch = consensus_header.sub_dag.leader_epoch();
        let number = consensus_header.number;

        // wait for committee keys, polling until available or timeout
        let deadline = tokio::time::Instant::now() + Self::COMMITTEE_WAIT_TIMEOUT;
        let keys = loop {
            if let Some(keys) = self.config.get_committee_keys_for_epoch(leader_epoch) {
                break keys;
            }
            if shutdown.noticed() {
                return None;
            }
            // trigger epoch record collector for the needed epoch
            self.consensus_bus.requested_missing_epoch().send_if_modified(|current| {
                if leader_epoch > *current {
                    *current = leader_epoch;
                    true
                } else {
                    false
                }
            });
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    target: "subscriber",
                    number,
                    epoch = leader_epoch,
                    "timed out waiting for committee keys, discarding header"
                );
                return None;
            }
            tokio::time::sleep(Self::COMMITTEE_POLL_INTERVAL).await;
        };

        consensus_header
            .verify_header_with_keys(&keys)
            .inspect_err(|e| {
                warn!(
                    target: "subscriber",
                    number,
                    epoch = leader_epoch,
                    "BLS verification failed, discarding header: {e}"
                );
            })
            .ok()
    }

    /// Catch up to current consensus and then try to rejoin as an active CVV.
    async fn catch_up_rejoin_consensus(
        &self,
        tasks: TaskSpawner,
        rx_shutdown: Noticer,
    ) -> SubscriberResult<()> {
        self.stream_and_follow(tasks, true, rx_shutdown).await
    }

    /// Follow along with consensus output but do not try to join consensus.
    async fn follow_consensus(
        &self,
        tasks: TaskSpawner,
        rx_shutdown: Noticer,
    ) -> SubscriberResult<()> {
        self.stream_and_follow(tasks, false, rx_shutdown).await
    }

    /// Schedule a pipelined batch fetch with retry logic for catchup.
    fn schedule_catchup_fetch(
        &self,
        verified: ConsensusHeader,
    ) -> BoxFuture<'static, SubscriberResult<(ConsensusOutput, u64)>> {
        let sub = self.clone();
        let shutdown = self.config.shutdown().clone();
        let sub_dag = verified.sub_dag.clone();
        let parent_hash = verified.parent_hash;
        let number = verified.number;
        Box::pin(async move {
            let mut retry_count = 0u32;
            loop {
                match sub.fetch_batches(sub_dag.clone(), parent_hash, number).await {
                    Ok(output) => return Ok((output, number)),
                    Err(e) if e.is_batch_fetch_error() && retry_count < 5 => {
                        retry_count += 1;
                        let delay = Duration::from_secs(2).min(
                            Duration::from_millis(500) * 2u32.saturating_pow(retry_count.min(5)),
                        );
                        warn!(
                            target: "subscriber",
                            header_number = number,
                            retry_count,
                            delay_ms = delay.as_millis(),
                            "catchup pipeline: batch fetch failed, retrying: {e}"
                        );
                        let rx_shutdown = shutdown.subscribe();
                        tokio::select! {
                            biased;
                            _ = rx_shutdown => {
                                return Err(SubscriberError::ClosedChannel("shutdown during retry".to_string()));
                            }
                            _ = tokio::time::sleep(delay) => continue,
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }

    /// Persist a fetched consensus output and forward it to the engine.
    async fn commit_catchup_output(&self, output: ConsensusOutput) -> SubscriberResult<()> {
        // persist individual certificates for consensus rejoin (BLS-verified before scheduling)
        if let Err(e) = self.config.node_storage().write(output.sub_dag.leader.clone()) {
            warn!(target: "subscriber", ?e, "failed to write leader cert to store");
        }
        if let Err(e) = self.config.node_storage().write_all(output.sub_dag.certificates.clone()) {
            warn!(target: "subscriber", ?e, "failed to write subdag certs to store");
        }

        // advance cert_store_round for try_rejoin_consensus
        let highest_cert_round = output
            .sub_dag
            .certificates
            .iter()
            .map(|c| c.round())
            .max()
            .unwrap_or_else(|| output.sub_dag.leader.round());

        // promote to canonical ConsensusBlocks table
        save_consensus(self.config.node_storage(), output.clone(), &self.inner.authority_id)?;

        let last_round = output.leader_round();

        trace!(
            target: "subscriber",
            output_number = output.number,
            output_leader_round = last_round,
            highest_cert_round,
            num_batches = output.batches.len(),
            "committing catchup output for execution"
        );

        // update consensus round watches before sending
        self.consensus_bus.update_consensus_rounds(ConsensusRound::new_with_gc_depth(
            last_round,
            self.config.parameters().gc_depth,
        ));
        let _ = self.consensus_bus.primary_round_updates().send_replace(last_round);
        self.consensus_bus.cert_store_round().send_if_modified(|current| {
            if highest_cert_round > *current {
                *current = highest_cert_round;
                true
            } else {
                false
            }
        });

        if let Err(e) = self.consensus_bus.consensus_output().send(output).await {
            error!(target: "subscriber", "error broadcasting consensus output: {e}");
            return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
        }
        Ok(())
    }

    /// Return true and trigger CvvActive when caught up to the peer-derived
    /// network head and all rejoin gates clear.
    fn try_rejoin_consensus(&self, consensus_header_number: u64) -> bool {
        //TODO: This IF here works but it is worth checking if this condition can be moved before
        // even invoking the try_rejoin_consensus
        if self.consensus_bus.node_mode().borrow().is_observer() {
            return false;
        }

        let (gossip_number, _) = *self.consensus_bus.last_published_consensus_num_hash().borrow();
        let probed_number = self.consensus_bus.last_consensus_header().borrow().number;
        let network_head = gossip_number.max(probed_number);

        if network_head == 0 {
            // no peer-derived signal yet - wait for gossip or probe
            if consensus_header_number % Self::REJOIN_LOG_THROTTLE == 0 {
                info!(
                    target: "subscriber",
                    consensus_header_number,
                    "try_rejoin_consensus: waiting for network-head signal (no gossip, no probe yet)"
                );
            }
            return false;
        }
        if consensus_header_number < network_head {
            if consensus_header_number % Self::REJOIN_LOG_THROTTLE == 0 {
                info!(
                    target: "subscriber",
                    consensus_header_number,
                    gossip_number,
                    probed_number,
                    network_head,
                    "try_rejoin_consensus: behind network head, still catching up"
                );
            }
            return false;
        }

        // Durable-seed barrier: CvvActive `run()` seeds its number counter from the durable
        // consensus-chain tip (`consensus_chain_tip` -> `ConsensusBlocks`). The in-memory
        // `consensus_header_number` can advance before `save_consensus` flushes the matching
        // header, so gate the handoff on the durable tip itself - not the in-memory counter.
        // If we hand off early, the live counter starts below the catch-up tip and
        // reissues a number the network already consumed, forking via a divergent
        // ConsensusHeader digest (which feeds `mix_hash` / `parent_beacon_block_root`).
        let durable_tip = consensus_chain_tip(&self.config).map_or(0, |h| h.number);
        if durable_tip < network_head {
            if consensus_header_number % Self::REJOIN_LOG_THROTTLE == 0 {
                info!(
                    target: "subscriber",
                    consensus_header_number,
                    durable_tip,
                    network_head,
                    "try_rejoin_consensus: durable consensus-chain tip behind network head - waiting for persist"
                );
            }
            return false;
        }

        // Cert store must cover committed_round or DAG reconstruction at
        // CvvActive startup will be sparse and fork.
        let committed_round = *self.consensus_bus.committed_round_updates().borrow();
        let cert_store_round = *self.consensus_bus.cert_store_round().borrow();
        if cert_store_round < committed_round {
            info!(
                target: "subscriber",
                consensus_header_number,
                committed_round,
                cert_store_round,
                "try_rejoin_consensus: execution caught up but cert store behind committed round - waiting"
            );
            return false;
        }

        // Check promotion barrier from a prior DAG-behind demotion. Clone out of the watch
        // borrow before send_replace (copy-out-under-lock): send_replace needs the write side.
        let barrier = self.consensus_bus.promotion_barrier().borrow().clone();
        if let Some(barrier) = barrier {
            let cleared = barrier.is_cleared(self.inner.committee.epoch(), committed_round, |d| {
                self.config.node_storage().contains(d).unwrap_or(false)
            });
            if !cleared {
                info!(
                    target: "subscriber",
                    consensus_header_number,
                    committed_round,
                    barrier_round = barrier.round,
                    "try_rejoin_consensus: blocked by promotion barrier - waiting for catch-up"
                );
                return false;
            }
            self.consensus_bus.promotion_barrier().send_replace(None);
            info!(
                target: "subscriber",
                consensus_header_number,
                barrier_epoch = barrier.epoch,
                barrier_round = barrier.round,
                "try_rejoin_consensus: promotion barrier cleared, lifting"
            );
        }

        info!(
            target: "subscriber",
            consensus_header_number,
            gossip_number,
            probed_number,
            network_head,
            committed_round,
            cert_store_round,
            "try_rejoin_consensus: caught up to network head; requesting CvvActive via mode_transition"
        );
        // request_mode_transition latches the rejoin (a mode change, not an epoch crossing);
        // the epoch manager drives the controlled transition back to CvvActive from here.
        self.consensus_bus.request_mode_transition(NodeMode::CvvActive);
        true
    }

    /// Stream missing headers, spawn state sync, and process incoming consensus headers.
    /// Batch fetches run concurrently via FuturesOrdered; commits happen in-order.
    async fn stream_and_follow(
        &self,
        tasks: TaskSpawner,
        rejoin: bool,
        rx_shutdown: Noticer,
    ) -> SubscriberResult<()> {
        let mut rx_consensus_headers = self.consensus_bus.consensus_header().subscribe();
        let last_streamed_number =
            stream_missing_consensus(&self.config, &self.consensus_bus).await?;
        info!(
            target: "subscriber",
            last_streamed_number,
            rejoin,
            "stream_and_follow: spawning state sync"
        );
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            self.network_handle.clone(),
            tasks,
            last_streamed_number,
        );
        // Re-execution anchor: the EVM-execution anchor, NOT the consensus tip. Seeding at the tip
        // makes `should_process_consensus_header` skip committed-but-unexecuted outputs on replay
        // (`number <= tip`), so blocks lost to a crash/static-file heal never rebuild and the chain
        // forks. `consensus_chain_tip` is for header numbering only.
        //
        // Reads the SSOT `executed_anchor` channel (seeded once at boot from the highest-nonce
        // recent block, advanced live by the engine) instead of re-deriving from
        // `recently_executed_blocks().latest_block()`, whose tip can regress to a PREVIOUS output's
        // anchor after a drained parked (out-of-order seq) batch. Number 0 means nothing
        // executed yet, so fall back to `last_streamed_number`.
        let (anchor_number, mut accepted_position) = {
            let anchor = self.consensus_bus.executed_anchor().borrow();
            (anchor.number, AcceptedPosition::from_sub_dag(&anchor.sub_dag))
        };
        let mut last_processed_number =
            if anchor_number == 0 { last_streamed_number } else { anchor_number };
        info!(
            target: "subscriber",
            last_processed_number,
            last_streamed_number,
            "stream_and_follow: entering select loop"
        );

        let mut waiting: FuturesOrdered<
            BoxFuture<'static, SubscriberResult<(ConsensusOutput, u64)>>,
        > = FuturesOrdered::new();
        let mut channel_closed = false;
        // Deterministic epoch cut: stops the catch-up persisting outputs that belong to the next
        // epoch, the same tracker the live committer drives.
        let mut epoch_cut = EpochCut::default();
        let mut last_committed_number = last_processed_number;
        // Arm immediately on post-demote re-entry: Bullshark demoted on
        // MissingParent, local tip already matches last_streamed, and no new
        // headers will arrive. Without this the node stalls waiting for
        // cert_store backfill that never triggers try_rejoin_consensus.
        let mut waiting_for_cert_store = rejoin && last_processed_number == last_streamed_number;
        let mut cert_store_rx = self.consensus_bus.cert_store_round().subscribe();

        // sync first-check: if all rejoin signals are already satisfied before we enter
        // the select, return without waiting up to 30s for the next channel change
        if waiting_for_cert_store && self.try_rejoin_consensus(last_committed_number) {
            return Ok(());
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                biased;

                _ = &rx_shutdown => {
                    info!(target: "subscriber", "shutdown signal received, exiting stream_and_follow");
                    return Ok(());
                }

                _ = ticker.tick(), if rejoin => {
                    if self.try_rejoin_consensus(last_committed_number) {
                        return Ok(());
                    }
                }

                // deliver completed fetches in order: persist, forward to engine
                Some(result) = waiting.next(), if !waiting.is_empty() => {
                    let (output, number) = result?;
                    // Deterministic epoch cut, same tracker as the live committer: stop persisting at
                    // the epoch-closing block so the catch-up never writes a next-epoch output to
                    // ConsensusBlocks (which would inflate the consensus tip and fork the next epoch's
                    // numbering). A dropped output must not advance the watermark or trigger rejoin -
                    // the transition starts the next epoch fresh.
                    let epoch_boundary = self.config.epoch_boundary();
                    match epoch_cut.classify(&output, epoch_boundary) {
                        CutAction::Keep | CutAction::Close => {
                            self.commit_catchup_output(output).await?;
                            last_committed_number = number;
                            if rejoin && self.try_rejoin_consensus(number) {
                                return Ok(());
                            }
                        }
                        CutAction::Drop => {
                            info!(
                                target: "subscriber",
                                output_number = output.number,
                                output_round = output.leader_round(),
                                "catch-up: dropping output past epoch boundary (belongs to next epoch)"
                            );
                        }
                    }
                    if channel_closed && waiting.is_empty() {
                        if !rejoin {
                            return Ok(());
                        }
                        waiting_for_cert_store = true;
                    }
                }

                // cert manager wrote new certs to the store - retry rejoin
                Ok(_) = cert_store_rx.changed(), if waiting_for_cert_store => {
                    if self.try_rejoin_consensus(last_committed_number) {
                        return Ok(());
                    }
                }

                // periodic timeout while waiting for cert store to prevent silent stalls
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)), if waiting_for_cert_store => {
                    let committed_round = *self.consensus_bus.committed_round_updates().borrow();
                    let cert_store_round = *self.consensus_bus.cert_store_round().borrow();
                    warn!(
                        target: "subscriber",
                        last_committed_number,
                        committed_round,
                        cert_store_round,
                        "still waiting for cert store to reach committed round"
                    );
                    // re-check in case we missed a notification
                    if self.try_rejoin_consensus(last_committed_number) {
                        return Ok(());
                    }
                }

                // accept new headers when pipeline has capacity
                result = rx_consensus_headers.recv(), if !channel_closed && waiting.len() < Self::MAX_CATCHUP_PIPELINE => {
                    match result {
                        Some(consensus_header) => {
                            let number = consensus_header.number;
                            // dedup before verifying; the predicate cannot advance the watermark,
                            // so a header that fails verification cannot skip the honest one.
                            if !should_process_consensus_header(number, last_processed_number) {
                                continue;
                            }
                            debug!(
                                target: "subscriber",
                                number,
                                pipeline = waiting.len(),
                                epoch = consensus_header.sub_dag.leader_epoch(),
                                "stream_and_follow: received header, verifying"
                            );
                            let Some(verified) = self
                                .verify_consensus_certificates(consensus_header, &rx_shutdown)
                                .await
                            else {
                                continue;
                            };
                            // bind position: a commit replayed at a wrong number does not advance,
                            // so reject it before execution. advance the watermark only on accept.
                            if !accepted_position.advance(&verified.sub_dag) {
                                warn!(
                                    target: "subscriber",
                                    number,
                                    epoch = verified.sub_dag.leader_epoch(),
                                    round = verified.sub_dag.leader_round(),
                                    "rejecting misplaced consensus sub-dag before execution"
                                );
                                continue;
                            }
                            last_processed_number = number;
                            waiting.push_back(self.schedule_catchup_fetch(verified));
                        }
                        None => {
                            info!(target: "subscriber", "stream_and_follow: channel closed");
                            channel_closed = true;
                            if waiting.is_empty() {
                                if !rejoin {
                                    return Ok(());
                                }
                                waiting_for_cert_store = true;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Return the `(digest, number)` seed for the live consensus-header chain - the parent the
    /// next committed `ConsensusHeader` chains from, read once on startup before `run()`'s loop.
    ///
    /// Uses [`consensus_chain_tip`] so numbering never regresses below the catch-up tip;
    /// see its docs for why the EVM-execution anchor must not be used here.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(BlockHash, u64)> {
        let tip = consensus_chain_tip(&self.config).unwrap_or_default();
        trace!(
            target: "subscriber",
            last_number = tip.number,
            last_leader_round = tip.sub_dag.leader_round(),
            "restoring consensus-header seed from consensus-chain tip"
        );
        Ok((tip.digest(), tip.number))
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(
        self,
        rx_shutdown: Noticer,
        to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
        execution_replay_done: tokio::sync::watch::Sender<()>,
    ) -> SubscriberResult<()> {
        // Make sure any old consensus that was not executed gets executed.
        // Note, "missing" in this context is consensus that was reached but not executed
        // before the last shutdown.  We need to execute it now so that everything will be
        // in sync, otherwise we could get out of order execution racing with Bullshark.
        let missing = get_missing_consensus(&self.config, &self.consensus_bus).await?;
        // Every output now produces at least one block, so replay waits solely on
        // recently_executed_blocks, which ticks whenever a block is produced.
        let mut recently_executed_blocks_sub =
            self.consensus_bus.recently_executed_blocks().subscribe();

        if !missing.is_empty() {
            info!(
                target: "subscriber",
                missing_count = missing.len(),
                "found missing consensus headers to catch up before starting main loop"
            );
        }

        for consensus_header in missing.into_iter() {
            let consensus_output = self
                .fetch_batches(
                    consensus_header.sub_dag.clone(),
                    consensus_header.parent_hash,
                    consensus_header.number,
                )
                .await?;

            let epoch_boundary = self.config.epoch_boundary();
            if consensus_output.reaches_epoch_boundary(epoch_boundary) {
                warn!(
                    target: "subscriber",
                    consensus_output_number = consensus_output.number,
                    consensus_output_round = consensus_output.leader_round(),
                    epoch_boundary,
                    "fetched missing consensus output beyond epoch boundary"
                );

                // send through the channel so it is received by detect_epoch_boundary;
                // continue with setup without waiting
                if let Err(e) = self.consensus_bus.consensus_output().send(consensus_output).await {
                    error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
                    return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
                }

                break;
            }

            if let Err(e) = to_engine.send((CameFrom::GetMissingConsensus, consensus_output)).await
            {
                error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
                return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
            }
            // wait until execution advances: a block landed (recently_executed_blocks ticks),
            // or shutdown fires, in which case we bail.
            tokio::select! {
                _ = recently_executed_blocks_sub.changed() => {}
                _ = &rx_shutdown => return Ok(()),
            }
        }

        // get_missing_consensus done - tell EpochManager it can go on with detect_epoch_boundary,
        // so catch-up replay is serialized BEFORE the live relay starts (no dual delivery).
        execution_replay_done.send_replace(());

        // Signal that execution replay is complete. The proposer waits for this before
        // creating headers, ensuring recently_executed_blocks contains up-to-date execution state
        // rather than stale MDBX data from before replay. (FIX: XL-C3)
        self.consensus_bus.execution_replay_complete().send_replace(true);

        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_sequence. So it doesn't
        // matter if we somehow managed to fetch the blocks from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        let (mut last_parent, mut last_number) = self.get_last_executed_consensus().await?;

        let mut rx_sequence = self.consensus_bus.sequence().subscribe();

        // Drain protocol: ack sender + drain signal. The signal only coordinates graceful shutdown;
        // the epoch cut is the `epoch_cut` tracker below, not the signal's payload.
        let mut drain_ack_tx = self.consensus_bus.take_drain_ack_tx();
        let mut drain_rx = self.consensus_bus.drain_signal().subscribe();
        let mut draining = false;
        let mut sequence_closed = false;
        // Cuts the epoch at the first output reaching the on-chain epoch_boundary (commit
        // timestamp, not drain timing), so all validators cut alike.
        let mut epoch_cut = EpochCut::default();

        // Listen to sequenced consensus message and process them.
        //
        // Use `biased` select to guarantee that the drain signal is always processed
        // before consuming new subdags from rx_sequence.
        // Without `biased`, tokio::select! randomly picks between ready branches,
        // creating a race where some validators consume an extra subdag before seeing
        // the drain signal.
        loop {
            tokio::select! {
                biased;

                // 1st priority: Drain signal from manager - must beat rx_sequence to
                // prevent consuming post-boundary subdags on some validators but not others.
                Ok(_) = drain_rx.changed(), if !draining => {
                    if drain_rx.borrow().is_some() {
                        draining = true;
                        info!(
                            target: "subscriber",
                            in_flight = waiting.len(),
                            "drain signal received, finishing in-flight work"
                        );
                        if waiting.is_empty() {
                            info!(target: "subscriber", "drain complete: no in-flight work");
                            let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                            return Ok(());
                        }
                    }
                },

                // 2nd priority: Process in-flight work (critical during drain).
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(output) = waiting.next() => {
                    let output: SubscriberResult<ConsensusOutput> = output;
                    match output {
                        Ok(output) => {
                            // Deterministic epoch cut (same tracker as the catch-up committer): keep
                            // the first output reaching the boundary (the epoch-closing block), drop
                            // the rest - persisting a later output forks the node past the boundary
                            // it signed.
                            let epoch_boundary = self.config.epoch_boundary();
                            match epoch_cut.classify(&output, epoch_boundary) {
                                CutAction::Drop => {
                                    info!(
                                        target: "subscriber",
                                        output_number = output.number,
                                        output_round = output.leader_round(),
                                        "dropping output past epoch boundary (belongs to next epoch)"
                                    );
                                    if draining && waiting.is_empty() {
                                        info!(target: "subscriber", "drain complete: all in-flight work processed (boundary cut), sending ack");
                                        let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                                        return Ok(());
                                    }
                                    continue;
                                }
                                CutAction::Close => {
                                    info!(
                                        target: "subscriber",
                                        output_number = output.number,
                                        output_round = output.leader_round(),
                                        epoch_boundary,
                                        "epoch boundary crossed at save point; this is the epoch-closing block"
                                    );
                                }
                                CutAction::Keep => {}
                            }

                            debug!(target: "subscriber", output=?output.digest(), "saving next output");
                            save_consensus(self.config.node_storage(), output.clone(), &self.inner.authority_id)?;
                            {
                                let digests: Vec<_> = output.batch_digests.iter().copied().collect();
                                self.consensus_bus.batch_tracker().output_broadcast(output.number, &digests);
                            }
                            debug!(target: "subscriber", "broadcasting output...");
                            if let Err(e) = self.consensus_bus.consensus_output().send(output).await {
                                error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
                                return Err(SubscriberError::ClosedChannel("failed to broadcast consensus output".to_string()));
                            }
                            debug!(target: "subscriber", "output broadcast successfully");

                            // If draining and all in-flight work is done, acknowledge and exit.
                            if draining && waiting.is_empty() {
                                info!(target: "subscriber", "drain complete: all in-flight work processed, sending ack");
                                let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            error!(target: "subscriber", "error fetching batches: {e}");
                            // Failure to fetch batches is a fatal condition, return an error which will trigger node shutdown.
                            return Err(e);
                        }
                    }
                },

                // 3rd priority: Accept new subdags from consensus (disabled when draining).
                result = rx_sequence.recv(), if !sequence_closed && !draining && waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                  match result {
                    Some(sub_dag) => {
                    debug!(target: "subscriber", subdag=?sub_dag.digest(), round=?sub_dag.leader_round(), "received committed subdag from consensus");
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    let parent_hash = last_parent;
                    let number = last_number + 1;
                    last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);

                    // NOTE: last_consensus_header must stay peer-derived only;
                    // local writes cause premature rejoin in try_rejoin_consensus.

                    // epoch/round/hash/sig feed production gossip only; a dev single-node
                    // has no peers to publish to, so they are compiled out there.
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    let epoch = sub_dag.leader_epoch();
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    let round = sub_dag.leader_round();
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    let consensus_result_hash = ConsensusResult::digest_data(epoch, round, number, last_parent);
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    let sig =
                        self.config.key_config().request_signature_direct(&encode(&to_intent_message(consensus_result_hash)));

                    // pre-publish gossipsub diagnostics (production only — dev has no peers)
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    {
                        let consensus_output_topic = LibP2pConfig::consensus_output_topic();
                        let mesh_peers = self.network_handle.inner_handle()
                            .mesh_peers(consensus_output_topic.clone())
                            .await
                            .unwrap_or_default();
                        let connected_count = self.network_handle.inner_handle()
                            .connected_peer_count()
                            .await
                            .unwrap_or(0);
                        if mesh_peers.is_empty() {
                            warn!(
                                target: "subscriber::gossipsub",
                                authority = ?self.inner.authority_id,
                                epoch,
                                round,
                                number,
                                connected_count,
                                "NO mesh peers for consensus output topic before publish - expect NoPeersSubscribedToTopic"
                            );
                            // also log all peers and their topics for debugging
                            if let Ok(all_peers) = self.network_handle.inner_handle().all_peers().await {
                                for (peer_id, topics) in &all_peers {
                                    let topic_names: Vec<_> = topics.iter().map(|t| t.to_string()).collect();
                                    warn!(
                                        target: "subscriber::gossipsub",
                                        ?peer_id,
                                        ?topic_names,
                                        "peer topic subscriptions at publish time"
                                    );
                                }
                            }
                        } else {
                            debug!(
                                target: "subscriber::gossipsub",
                                epoch,
                                round,
                                number,
                                mesh_peer_count = mesh_peers.len(),
                                connected_count,
                                "publishing consensus output"
                            );
                        }
                    }

                    // cache the header BEFORE gossip so peers can fetch it immediately.
                    // Runs in ALL builds: a dev single-node has no peers to gossip to but
                    // its local head must still advance, so the cache write is never gated.
                    let header_for_cache = ConsensusHeader { parent_hash, sub_dag: sub_dag.clone(), number, extra: B256::default() };
                    store_consensus_header_in_cache(self.config.node_storage(), &header_for_cache);

                    // Dev (single-node): no peers — skip gossip entirely.
                    #[cfg(not(feature = "dev-single-node-setup"))]
                    if let Err(e) = self.network_handle.publish_consensus(epoch, round, number, last_parent, self.config.key_config().public_key(), sig).await {
                        error!(target: "subscriber", "error publishing latest consensus to network {:?}: {}", self.inner.authority_id, e);
                    }
                    {
                        let digests: Vec<_> = sub_dag.certificates.iter()
                            .flat_map(|c| c.header().payload().keys().copied())
                            .collect();
                        self.consensus_bus.batch_tracker().subdag_committed(number, &digests);
                    }
                    last_number += 1;
                    waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number));
                    }
                    None => {
                        // channel closed - consensus was shut down, no more subdags
                        sequence_closed = true;
                        info!(target: "subscriber", "sequence channel closed (consensus shut down)");
                        if waiting.is_empty() {
                            info!(target: "subscriber", "drain complete: sequence closed and no in-flight work");
                            let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                            return Ok(());
                        }
                    }
                  }
                },

                // 4th priority: Fallback shutdown.
                _ = &rx_shutdown => {
                    if !draining {
                        // Check if drain was signaled but we hit shutdown first.
                        if drain_rx.borrow_and_update().is_some() {
                            // Drain was signaled but we hit shutdown first - honor the drain.
                            draining = true;
                            info!(
                                target: "subscriber",
                                in_flight = waiting.len(),
                                "shutdown received but drain was signaled, entering drain mode"
                            );
                            if waiting.is_empty() {
                                let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                                return Ok(());
                            }
                            // Continue the loop to finish in-flight work.
                        } else {
                            warn!(
                                target: "subscriber",
                                in_flight = waiting.len(),
                                "received shutdown without drain signal, {} in-flight items may be lost",
                                waiting.len()
                            );
                            return Ok(());
                        }
                    } else {
                        // Already draining - if no work remains, ack and exit.
                        // With biased select, rx_shutdown is only reached when all
                        // higher-priority branches are disabled (drain_rx guarded off,
                        // waiting empty, rx_sequence guarded off).
                        if waiting.is_empty() {
                            info!(target: "subscriber", "drain complete during shutdown: no in-flight work remains");
                            let _ = drain_ack_tx.take().map(|tx| tx.send(()));
                            return Ok(());
                        }
                        // Still has in-flight work: yield to let other select branches run.
                        tokio::task::yield_now().await;
                    }
                }

            }

            self.consensus_bus
                .executor_metrics()
                .waiting_elements_subscriber
                .set(waiting.len() as i64);
        }
    }

    /// Helper function to obtain authority's execution address based on their
    /// `AuthorityIdentifier`. This address is used as the beneficiary during batch execution.
    /// A fatal error is returned if the authority is missing from the committee.
    fn authority_execution_address(
        &self,
        authority_id: &AuthorityIdentifier,
    ) -> SubscriberResult<Address> {
        self.inner
            .committee
            .authority(authority_id)
            .map(|a| a.execution_address())
            .ok_or(SubscriberError::UnexpectedAuthority(authority_id.clone()))
            .inspect_err(|_| {
                error!(target: "subscriber", ?authority_id, "Authority missing from committee");
            })
    }

    /// Turn a CommittedSubDag with consensus header info into ConsensusOutput.
    /// It will retrieve any missing Batches so the ConsensusOutput will be ready
    /// to execute.
    /// Note, an error here is BAD and will most likely cause node shutdown (clean).  Do
    /// not provide a bogus sub dag...
    async fn fetch_batches(
        &self,
        deliver: CommittedSubDag,
        parent_hash: B256,
        number: u64,
    ) -> SubscriberResult<ConsensusOutput> {
        let num_blocks = deliver.num_primary_blocks();
        let num_certs = deliver.len();

        if num_blocks == 0 {
            debug!(target: "subscriber", "No blocks to fetch, payload is empty");
            return Ok(ConsensusOutput {
                sub_dag: Arc::new(deliver),
                parent_hash,
                number,
                ..Default::default()
            });
        }

        let sub_dag = Arc::new(deliver);
        let mut consensus_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            batches: Vec::with_capacity(num_certs),
            parent_hash,
            number,
            ..Default::default()
        };

        let mut batch_set: HashSet<BlockHash> = HashSet::new();

        for cert in &sub_dag.certificates {
            for (digest, _) in cert.header().payload().iter() {
                batch_set.insert(*digest);
                consensus_output.batch_digests.push_back(*digest);
            }
        }

        let fetched_batches_timer = self
            .consensus_bus
            .executor_metrics()
            .block_fetch_for_committed_subdag_total_latency
            .start_timer();
        self.consensus_bus
            .executor_metrics()
            .committed_subdag_block_count
            .observe(num_blocks as f64);
        let fetched_batches = self.fetch_batches_from_peers(batch_set).await?;
        drop(fetched_batches_timer);

        // map all fetched batches to their respective certificates for applying block rewards
        for cert in &sub_dag.certificates {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(cert.header().payload().len());

            self.consensus_bus.executor_metrics().subscriber_current_round.set(cert.round() as i64);
            self.consensus_bus
                .executor_metrics()
                .subscriber_certificate_latency
                .observe(cert.created_at().elapsed().as_secs_f64());

            // retrieve fetched batch by digest
            for digest in cert.header().payload().keys() {
                self.consensus_bus.executor_metrics().subscriber_processed_blocks.inc();
                let batch = fetched_batches.get(digest).ok_or(SubscriberError::MissingFetchedBatch(*digest)).inspect_err(|_| {
                    error!(target: "subscriber", "[Protocol violation] Batch not found in fetched batches from workers of certificate signers");
                })?;

                debug!(
                    target: "subscriber",
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                cert_batches.push(batch.clone());
            }

            // main collection for execution
            consensus_output.batches.push(CertifiedBatch {
                address: self.authority_execution_address(cert.origin())?,
                batches: cert_batches,
            });
        }
        debug!(target: "subscriber", "returning output to subscriber");
        Ok(consensus_output)
    }

    async fn fetch_batches_from_peers(
        &self,
        batch_digests: HashSet<BlockHash>,
    ) -> SubscriberResult<HashMap<BlockHash, Batch>> {
        let mut fetched_blocks = HashMap::new();

        debug!(target: "subscriber", "Attempting to fetch {} digests peers", batch_digests.len(),);
        let blocks = match self.inner.client.fetch_batches(batch_digests.clone()).await {
            Ok(resp) => resp,
            Err(e) => {
                error!(target: "subscriber", "Failed to fetch batches from peers: {e:?}");
                return Err(SubscriberError::ClientRequestsFailed);
            }
        };
        for (digest, block) in blocks.batches.into_iter() {
            self.record_fetched_batch_metrics(&block, &digest);
            fetched_blocks.insert(digest, block);
        }

        Ok(fetched_blocks)
    }

    fn record_fetched_batch_metrics(&self, batch: &Batch, digest: &BlockHash) {
        if let Some(received_at) = batch.received_at() {
            let remote_duration = received_at.elapsed().as_secs_f64();
            debug!(
                target: "subscriber",
                "Batch was fetched for execution after being received from another worker {}s ago.",
                remote_duration
            );
            self.consensus_bus
                .executor_metrics()
                .block_execution_local_latency
                .with_label_values(&["other"])
                .observe(remote_duration);
            self.consensus_bus.executor_metrics().block_execution_latency.observe(remote_duration);
            debug!(
                target: "subscriber",
                "Block {:?} took {} seconds since it has been created to when it has been fetched for execution",
                digest,
                remote_duration,
            );
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_headers_accepted() {
        assert!(should_process_consensus_header(1, 0));
        assert!(should_process_consensus_header(2, 1));
        assert!(should_process_consensus_header(3, 2));
    }

    #[test]
    fn test_duplicate_header_rejected() {
        assert!(should_process_consensus_header(2, 1));
        assert!(!should_process_consensus_header(2, 2));
        assert!(should_process_consensus_header(3, 2));
    }

    #[test]
    fn test_out_of_order_header_rejected() {
        assert!(should_process_consensus_header(1, 0));
        assert!(!should_process_consensus_header(3, 1));
    }

    #[test]
    fn test_backward_header_rejected() {
        assert!(!should_process_consensus_header(3, 5));
    }

    #[test]
    fn test_recovery_starting_from_high_number() {
        assert!(!should_process_consensus_header(528336, 544719));
        assert!(should_process_consensus_header(544720, 544719));
    }

    /// Regression for the validator-2 chaos fork: after a crash/static-file heal the EVM tip can
    /// sit one output BELOW the consensus tip (round 418's blocks lost to the heal; consensus still
    /// committed round 418). The catch-up replay re-streams that gap output and must re-execute it,
    /// so the dedup tracker has to be seeded from the EVM-execution anchor (3429), NOT the
    /// consensus tip (3430). Seeding at the tip makes the gap output look like a duplicate,
    /// silently drops it, and the lost block never rebuilds -> the chain forks. (Chaos-log
    /// values; round 416 -> 3429, round 418 -> 3430.)
    #[test]
    fn test_replay_admits_gap_only_when_seeded_at_execution_anchor() {
        // FIX: seeded at the execution anchor -> the committed-but-unexecuted gap output is
        // replayed.
        assert!(
            should_process_consensus_header(3430, 3429),
            "gap output must be re-executed when the replay tracker is the execution anchor"
        );

        // BUG (the fork): seeded at the consensus tip -> the gap output is dropped as a duplicate.
        assert!(
            !should_process_consensus_header(3430, 3430),
            "seeding the replay tracker at the consensus tip drops the gap output and forks the chain"
        );
    }

    /// Build a committed sub-dag whose leader commits at `(epoch, round)`.
    fn subdag_at(epoch: Epoch, round: Round) -> CommittedSubDag {
        use rayls_infrastructure_types::{Certificate, ReputationScores};
        let mut leader = Certificate::default();
        leader.header.epoch = epoch;
        leader.header.round = round;
        CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None)
    }

    #[test]
    fn accepted_position_accepts_strictly_increasing_round() {
        let mut pos = AcceptedPosition { epoch: 5, round: 10 };
        assert!(pos.advance(&subdag_at(5, 11)));
        assert_eq!(pos, AcceptedPosition { epoch: 5, round: 11 });
        // a gap larger than 2 (a skipped leader) is legal: the check is strict `>`, not `+2`
        assert!(pos.advance(&subdag_at(5, 16)));
        assert_eq!(pos, AcceptedPosition { epoch: 5, round: 16 });
    }

    #[test]
    fn accepted_position_rejects_equal_or_lower_round() {
        let mut pos = AcceptedPosition { epoch: 5, round: 11 };
        assert!(!pos.advance(&subdag_at(5, 11)), "an equal round must not advance");
        assert!(!pos.advance(&subdag_at(5, 9)), "a lower round must not advance");
        assert_eq!(pos, AcceptedPosition { epoch: 5, round: 11 }, "a rejection must not mutate");
    }

    #[test]
    fn accepted_position_accepts_epoch_increase_resetting_round() {
        // the epoch boundary reseeds the round counter low; the epoch key still advances
        let mut pos = AcceptedPosition { epoch: 5, round: 400 };
        assert!(pos.advance(&subdag_at(6, 2)));
        assert_eq!(pos, AcceptedPosition { epoch: 6, round: 2 });
    }

    #[test]
    fn accepted_position_rejects_epoch_regression() {
        let mut pos = AcceptedPosition { epoch: 6, round: 2 };
        assert!(!pos.advance(&subdag_at(5, 500)), "a lower epoch must not advance");
        assert_eq!(pos, AcceptedPosition { epoch: 6, round: 2 }, "a rejection must not mutate");
    }

    #[test]
    fn accepted_position_genesis_baseline_accepts_first_real_round() {
        let mut pos = AcceptedPosition::from_sub_dag(&subdag_at(0, 0));
        assert_eq!(pos, AcceptedPosition { epoch: 0, round: 0 });
        assert!(pos.advance(&subdag_at(0, 1)));
    }

    #[test]
    fn accepted_position_blockless_output_advances() {
        // a blockless output still carries a leader at a round, so it advances like any other
        let mut pos = AcceptedPosition { epoch: 5, round: 11 };
        assert!(pos.advance(&subdag_at(5, 12)));
        assert_eq!(pos, AcceptedPosition { epoch: 5, round: 12 });
    }

    #[test]
    fn test_is_batch_fetch_error() {
        assert!(SubscriberError::MissingFetchedBatch(BlockHash::default()).is_batch_fetch_error());
        assert!(SubscriberError::ClientRequestsFailed.is_batch_fetch_error());
        assert!(!SubscriberError::ClosedChannel("test".into()).is_batch_fetch_error());
        assert!(!SubscriberError::ExecutorConnectionDropped.is_batch_fetch_error());
    }

    /// Build a consensus output whose subdag commits at timestamp `committed_at`.
    fn output_committed_at(committed_at: u64) -> ConsensusOutput {
        use rayls_infrastructure_types::{Certificate, ReputationScores};
        use std::collections::VecDeque;
        let mut leader = Certificate::default();
        leader.header.created_at = committed_at;
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusOutput {
            sub_dag: Arc::new(sub_dag),
            batches: vec![],
            batch_digests: VecDeque::new(),
            parent_hash: B256::default(),
            number: 0,
            extra: B256::default(),
            close_epoch: false,
        }
    }

    /// The epoch cut keeps the first output reaching the boundary (the epoch-closing block) and
    /// drops every output after it. This is the tracker both committers now drive; in the catch-up
    /// committer its absence let a follower persist next-epoch outputs into `ConsensusBlocks`,
    /// inflating the consensus tip and forking the next epoch's numbering (validator-2's
    /// post-boundary leak).
    #[test]
    fn test_epoch_cut_keeps_closing_block_then_drops_rest() {
        let boundary: TimestampSec = 1_000;
        let mut cut = EpochCut::default();

        // before the boundary: kept
        assert_eq!(cut.classify(&output_committed_at(900), boundary), CutAction::Keep);
        // first output at/after the boundary: the epoch-closing block (kept, latches the cut)
        assert_eq!(cut.classify(&output_committed_at(1_000), boundary), CutAction::Close);
        // every output after the cut is dropped - even one whose timestamp dips back below it
        assert_eq!(cut.classify(&output_committed_at(1_001), boundary), CutAction::Drop);
        assert_eq!(cut.classify(&output_committed_at(900), boundary), CutAction::Drop);
    }

    /// `epoch_boundary` is `u64::MAX` until the boundary is known, so the cut never fires early.
    #[test]
    fn test_epoch_cut_inert_until_boundary_known() {
        let mut cut = EpochCut::default();
        let out = output_committed_at(u64::MAX - 1);
        assert_eq!(cut.classify(&out, u64::MAX), CutAction::Keep);
    }
}
