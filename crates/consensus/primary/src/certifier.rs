//! Certifier publishes headers and certificates for this primary.

use crate::{
    aggregators::HeaderVotesAggregator,
    network::{PrimaryNetworkHandle, RequestVoteResult},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::monitored_future;
use rayls_consensus_network::error::NetworkError;
use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_config::{ConsensusConfig, KeyConfig};
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{
    ensure,
    error::{DagError, DagResult},
    AuthorityIdentifier, BlsPublicKey, Certificate, CertificateDigest, Committee, Database, Header,
    HeaderDigest, NodeMode, Noticer, Notifier, RaylsReceiver, RaylsSender, TaskManager,
    TaskSpawner, Vote,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{debug, enabled, error, info, warn};

#[cfg(test)]
#[path = "tests/certifier_tests.rs"]
mod certifier_tests;

use crate::vote_failure_tracker::{RejectionOutcome, VoteFailureTracker};

/// Result of evaluating a vote error in `handle_vote_error`.
enum VoteErrorAction {
    /// Abort the proposal and return this error.
    Abort(DagError),
    /// Non-fatal - continue collecting votes.
    Continue,
}

/// Header proposer that collects peer votes and certifies headers into certificates.
///
/// It receives headers to propose from Proposer via `rx_headers`, and publishes certificates to
/// gossip network.
#[derive(Clone)]
pub(crate) struct Certifier<DB> {
    /// The identifier of this primary.
    authority_id: AuthorityIdentifier,
    /// The committee information.
    committee: Committee,
    /// The persistent storage keyed to certificates.
    certificate_store: DB,
    /// Handles synchronization with other nodes and our workers.
    rayls_consensus_state_sync: StateSynchronizer<DB>,
    /// Service to sign headers.
    signature_service: KeyConfig,
    /// Consensus config to subscribe to shutdown.
    config: ConsensusConfig<DB>,
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// A network sender to send the batches to the other workers.
    network: PrimaryNetworkHandle,
    /// Metrics handler.
    metrics: Arc<PrimaryMetrics>,
    /// Spawn epoch-related tasks.
    task_spawner: TaskSpawner,
    /// Notifier to cancel pending proposals and vote requests if new header is received.
    new_proposal: Notifier,
    /// Shared vote rejection tracker - accumulates across proposals.
    vote_failures: VoteFailureTracker,
}

impl<DB: Database> Certifier<DB> {
    /// Spawn the long-running certifier task.
    pub(crate) fn spawn(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
        primary_network: PrimaryNetworkHandle,
        task_manager: &TaskManager,
    ) {
        // return early if not CVV
        let Some(authority_id) = config.authority_id() else {
            // If we don't have an authority id then we are not a validator and should not be
            // proposing anything...
            return;
        };

        let primary_metrics = consensus_bus.primary_metrics().node_metrics.clone();

        // spawn long-running task to gossip own certificates
        let task_spawner = task_manager.get_spawner();
        task_manager.spawn_critical_task("certifier task", monitored_future!(
            async move {
                let highest_created_certificate = config.node_storage().last_round(&authority_id).expect("certificate store available");
                debug!(
                    target: "epoch-manager",
                    ?highest_created_certificate,
                    "restoring certifier with highest created certificate for epoch {}",
                    config.epoch(),
                );

                // publish last certificate on startup
                if let Some(cert) = highest_created_certificate {
                    if let Err(e) = primary_network.publish_certificate(cert).await {
                        error!(target: "primary::certifier", ?e, "failed to publish highest created certificate gossip during startup");
                    }
                }

                // skip grace on initial startup (no prior state to sync from);
                // allow 30s otherwise for cert fetcher to catch up.
                let grace_deadline = if config.is_initial_epoch() {
                    None
                } else {
                    Some(Instant::now() + Duration::from_secs(30))
                };
                let committee_size = config.committee().size();
                Self {
                    authority_id: authority_id.clone(),
                    committee: config.committee().clone(),
                    certificate_store: config.node_storage().clone(),
                    rayls_consensus_state_sync,
                    signature_service: config.key_config().clone(),
                    config,
                    consensus_bus,
                    network: primary_network,
                    metrics: primary_metrics,
                    task_spawner,
                    new_proposal: Notifier::new(),
                    vote_failures: VoteFailureTracker::new(committee_size, grace_deadline),
                }
                .run()
                .await;
                info!(target: "primary::certifier", "Certifier on node {} has shutdown.", authority_id);
            },
            "CertifierTask"
        ));
    }

    /// Rayls: Maximum vote request attempts before giving up.
    const MAX_VOTE_REQUEST_ATTEMPTS: u32 = 30;

    /// Rayls: How long the committed round may stall - while we already hold the certs for a peer's
    /// limit round - before a "too old" rejection counts toward demotion anyway. Below this, the
    /// lag is treated as a transient proposer stall (certs arriving, not yet usable as parents)
    /// and the rejection is ignored; beyond it, the proposer is considered wedged and allowed
    /// to demote. Far above the normal sub-second commit interval, so transient gaps never trip
    /// it.
    const CERT_COVERED_WEDGE_WINDOW: Duration = Duration::from_secs(30);

    /// Rayls: Request a vote for a header, retrying up to MAX_VOTE_REQUEST_ATTEMPTS times.
    async fn request_vote(
        authority: AuthorityIdentifier,
        header: Header,
        peer_id: BlsPublicKey,
        certificate_store: DB,
        network: PrimaryNetworkHandle,
        committee: Committee,
        cancel_proposal: Noticer,
        quorum_reached: Noticer,
    ) -> DagResult<Vote> {
        let mut missing_parents: Option<Vec<CertificateDigest>> = None;
        let mut attempt: u32 = 0;
        debug!(target: "primary::certifier", ?authority, ?header, "requesting vote for header...");

        // loop until vote received
        let vote: Vote = loop {
            // increase attempt count
            attempt += 1;

            // peers may respond to a vote requesting missing parents
            let parents = missing_parents
                .map(|missing_parents| {
                    // collect missing parents requested by peer in order to vote for this header

                    let requested_count = missing_parents.len();
                    // only provide certs that are parents for the requested vote
                    let filtered: Vec<_> = missing_parents
                        .into_iter()
                        .filter(|parent| header.parents().contains(parent))
                        .collect();
                    let filtered_count = filtered.len();

                    if filtered_count != requested_count {
                        warn!(
                            target: "primary::certifier",
                            requested_count,
                            filtered_count,
                            header_parents = header.parents().len(),
                            header_round = header.round(),
                            "peer requested parent digests not in our header"
                        );
                    }

                    let read_results = certificate_store.read_all(filtered.iter().copied())?;
                    let mut found = 0usize;
                    let mut missing_digests = Vec::new();
                    let parents: Vec<_> = filtered
                        .iter()
                        .zip(read_results.into_iter())
                        .filter_map(|(digest, opt)| {
                            if let Some(cert) = opt {
                                found += 1;
                                Some(cert)
                            } else {
                                missing_digests.push(*digest);
                                None
                            }
                        })
                        .collect();

                    if found != filtered_count {
                        error!(
                            target: "primary::certifier",
                            requested_count,
                            filtered_count,
                            found,
                            header_round = header.round(),
                            header_epoch = header.epoch(),
                            header_parents = ?header.parents(),
                            ?missing_digests,
                            "missing parent certificates in local store"
                        );
                        return Err(DagError::ProposedHeaderMissingCertificates);
                    }
                    Ok(parents)
                })
                .unwrap_or(Ok(vec![]))?;

            // listen for requests from peers
            tokio::select! {
                vote_result = network.request_vote(peer_id, header.clone(), parents) => {
                    // process response from peer
                    match vote_result {
                        Ok(RequestVoteResult::Vote(vote)) => {
                            debug!(target: "primary::certifier", ?authority, ?vote, "Ok response received after request vote");
                            // happy path - vote recieved
                            break vote;
                        }
                        Ok(RequestVoteResult::MissingParents(parents)) => {
                            debug!(target: "primary::certifier", ?authority, ?parents, "Ok missing parents response received after request vote");
                            // retrieve missing parents so peer can vote
                            missing_parents = Some(parents);
                        }
                        Ok(RequestVoteResult::TooOld { header_round, limit_round }) => {
                            return Err(DagError::TooOldRejectedByPeers {
                                peer_id: authority,
                                header_round,
                                limit_round,
                            });
                        }
                        Ok(RequestVoteResult::EpochMismatch { expected, received }) => {
                            return Err(DagError::EpochRejectedByPeer {
                                peer_id: authority,
                                peer_epoch: expected,
                                our_epoch: received,
                            });
                        }
                        Err(error) => {
                            if let NetworkError::RPCError(ref error_msg) = error {
                                if error_msg.contains("Invalid epoch") {
                                    error!(
                                        target: "primary::certifier",
                                        ?authority, ?header,
                                        "epoch mismatch detected, stopping vote requests for stale header: {error_msg}"
                                    );
                                } else if error_msg.contains("too old") {
                                    // legacy format from older peers
                                    return Err(DagError::TooOldRejectedByPeers {
                                        peer_id: authority,
                                        header_round: header.round(),
                                        limit_round: 0,
                                    });
                                } else {
                                    error!(target: "primary::certifier", ?authority, error=?error_msg, ?header, "fatal request for requested vote");
                                }
                                return Err(DagError::NetworkError(format!(
                                    "irrecoverable error requesting vote for {header}: {error_msg}"
                                )));
                            } else {
                                error!(target: "primary::certifier", ?authority, ?error, ?header, "network error requesting vote");
                            }

                            missing_parents = None;
                        }
                    }
                }

                // cancel vote request on new proposal or shutdown
                _ = &cancel_proposal => {
                    return Err(DagError::Canceled);
                }

                // cancel vote request when quorum already reached from other peers
                _ = &quorum_reached => {
                    debug!(target: "primary::certifier", ?authority, ?header, "quorum reached, cancelling remaining vote request");
                    return Err(DagError::Canceled);
                }
            }

            // Check if we've exceeded the maximum number of attempts
            if attempt >= Self::MAX_VOTE_REQUEST_ATTEMPTS {
                error!(
                    target: "primary::certifier",
                    ?authority,
                    ?header,
                    attempt,
                    "exceeded maximum vote request attempts"
                );
                return Err(DagError::NetworkError(format!(
                    "exceeded maximum vote request attempts ({}) for header {}",
                    Self::MAX_VOTE_REQUEST_ATTEMPTS,
                    header.digest()
                )));
            }

            // Retry delay with cancellation support. Using custom values here because pure
            // exponential backoff is hard to configure without it being either too aggressive or
            // too slow. We want the first retry to be instantaneous, next couple to be fast, and
            // to slow quickly thereafter.
            let delay = Duration::from_millis(match attempt {
                1 => 0,
                2 => 100,
                3 => 500,
                4 => 1_000,
                5 => 2_000,
                6 => 5_000,
                _ => 10_000,
            });
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = &cancel_proposal => {
                    return Err(DagError::Canceled);
                }
                _ = &quorum_reached => {
                    debug!(target: "primary::certifier", ?authority, ?header, "quorum reached during retry delay, cancelling");
                    return Err(DagError::Canceled);
                }
            }
        };

        // verify the vote (bls signature over header digest)
        ensure!(
            vote.header_digest() == header.digest()
                && vote.origin() == header.author()
                && vote.author() == &authority,
            DagError::UnexpectedVote(vote.header_digest())
        );

        // possible equivocations.
        ensure!(
            header.epoch() == vote.epoch(),
            DagError::InvalidEpoch { expected: header.epoch(), received: vote.epoch() }
        );
        ensure!(
            header.round() == vote.round(),
            DagError::InvalidRound { expected: header.round(), received: vote.round() }
        );

        // ensure the vote is from the correct epoch
        ensure!(
            vote.epoch() == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: vote.epoch() }
        );

        // ensure the authority has voting rights
        ensure!(
            committee.voting_power_by_id(vote.author()) > 0,
            DagError::UnknownAuthority(vote.author().to_string())
        );

        Ok(vote)
    }

    /// Evaluate a vote error and decide whether to abort the proposal.
    fn handle_vote_error(&self, error: &DagError, header: &Header) -> VoteErrorAction {
        match error {
            DagError::TooOldRejectedByPeers { peer_id, header_round, limit_round } => {
                // A "too old" rejection means our PROPOSER is lagging in rounds - not that we lack
                // data. If our cert store already covers the peer's limit round, we hold every cert
                // CvvInactive would sync; the lag is transient (those certs are suspended on
                // missing grandparents and not yet usable as parents, but they are
                // arriving). Demoting then is a spurious flap that instantly
                // rejoins. Skip while the DAG is still making progress (committed
                // round advancing); only if it stalls for CERT_COVERED_WEDGE_WINDOW
                // is the proposer genuinely wedged and allowed to demote.
                // Mirrors `should_count_epoch_rejection`, which drops rejections carrying no
                // liveness signal.
                // `limit_round > 0` excludes the legacy string-based path (set to 0 at the
                // request site), where we don't know the peer's real limit - those count normally
                // toward demotion rather than always satisfying `cert_store_round >= 0`.
                let cert_store_round = *self.consensus_bus.cert_store_round().borrow();
                if *limit_round > 0 && cert_store_round >= *limit_round {
                    let committed_round = *self.consensus_bus.committed_round_updates().borrow();
                    if self
                        .vote_failures
                        .skip_cert_covered(committed_round, Self::CERT_COVERED_WEDGE_WINDOW)
                    {
                        warn!(
                            target: "primary::certifier",
                            auth=?self.authority_id,
                            peer=?peer_id,
                            header_round, limit_round, cert_store_round, committed_round,
                            "ignoring too-old rejection: cert store covers limit round and DAG still progressing (transient proposer lag)"
                        );
                        return VoteErrorAction::Continue;
                    }
                    warn!(
                        target: "primary::certifier",
                        auth=?self.authority_id,
                        peer=?peer_id,
                        header_round, limit_round, cert_store_round, committed_round,
                        "too-old rejection with certs but committed round wedged; counting toward demotion"
                    );
                }
                let outcome = self.vote_failures.record_too_old(peer_id.clone());
                warn!(
                    target: "primary::certifier",
                    auth=?self.authority_id,
                    peer=?peer_id,
                    header_round, limit_round,
                    count = self.vote_failures.too_old_count(),
                    threshold = self.vote_failures.threshold(),
                    "peer rejected header as too old"
                );
                match outcome {
                    RejectionOutcome::BelowThreshold => VoteErrorAction::Continue,
                    RejectionOutcome::GracePeriod | RejectionOutcome::TransitionToInactive => {
                        if matches!(outcome, RejectionOutcome::TransitionToInactive) {
                            self.request_cvv_inactive();
                        }
                        VoteErrorAction::Abort(DagError::TooOldRejectedByPeers {
                            peer_id: peer_id.clone(),
                            header_round: *header_round,
                            limit_round: *limit_round,
                        })
                    }
                }
            }
            DagError::EpochRejectedByPeer { peer_id, peer_epoch, our_epoch } => {
                // A peer at an older epoch is itself stale; ignore its rejection
                // so one lagging validator cannot demote the honest majority.
                if !VoteFailureTracker::should_count_epoch_rejection(*peer_epoch, *our_epoch) {
                    warn!(
                        target: "primary::certifier",
                        auth=?self.authority_id,
                        peer=?peer_id,
                        peer_epoch, our_epoch,
                        "ignoring epoch rejection from stale peer"
                    );
                    return VoteErrorAction::Continue;
                }

                let outcome = self.vote_failures.record_epoch_mismatch(peer_id.clone());
                warn!(
                    target: "primary::certifier",
                    auth=?self.authority_id,
                    peer=?peer_id,
                    peer_epoch, our_epoch,
                    count = self.vote_failures.epoch_mismatch_count(),
                    threshold = self.vote_failures.threshold(),
                    "peer rejected header as wrong epoch"
                );
                if matches!(outcome, RejectionOutcome::TransitionToInactive) {
                    self.request_cvv_inactive();
                }
                VoteErrorAction::Abort(DagError::EpochRejectedByPeer {
                    peer_id: peer_id.clone(),
                    peer_epoch: *peer_epoch,
                    our_epoch: *our_epoch,
                })
            }
            DagError::InvalidEpoch { expected, received } => {
                // Bad vote payload from a peer (signed wrong epoch). Log and
                // continue - this is not a majority signal.
                warn!(
                    target: "primary::certifier",
                    auth=?self.authority_id,
                    header_epoch = header.epoch(),
                    expected,
                    received,
                    "received vote with mismatched epoch"
                );
                VoteErrorAction::Continue
            }
            e => {
                error!(
                    target: "primary::certifier",
                    auth=?self.authority_id,
                    "failed to get vote for header {header:?}: {e:?}"
                );
                VoteErrorAction::Continue
            }
        }
    }

    /// Signal the epoch manager to transition this node to CvvInactive.
    fn request_cvv_inactive(&self) {
        info!(
            target: "primary::certifier",
            auth=?self.authority_id,
            "vote rejection majority reached, requesting CvvInactive"
        );
        // request_mode_transition latches the mode change; the controlled shutdown that follows
        // drains this node out of the committee. No other signal is needed here.
        self.consensus_bus.request_mode_transition(NodeMode::CvvInactive);
    }

    /// Propose a header produced by this authority.
    async fn propose_header(&self, header: Header) -> DagResult<Certificate> {
        debug!(target: "primary::certifier", auth=?self.authority_id, "proposing header");

        // only propose headers in current epoch
        if header.epoch() != self.committee.epoch() {
            error!(
                target: "primary::certifier",
                "Certifier received mismatched header proposal for epoch {}, currently at epoch {}",
                header.epoch(),
                self.committee.epoch()
            );
            return Err(DagError::InvalidEpoch {
                expected: self.committee.epoch(),
                received: header.epoch(),
            });
        }

        self.metrics.proposed_header_round.set(header.round() as i64);

        // subscribe early for shutdown notifications
        let cancel_proposal = self.new_proposal.subscribe();

        // notifier to cancel lingering vote tasks once quorum is reached
        let quorum_notifier = Notifier::new();

        // reset the votes aggregator and sign own header
        let mut votes_aggregator =
            HeaderVotesAggregator::new(self.metrics.clone(), &self.committee);
        let vote = Vote::new(&header, self.authority_id.clone(), &self.signature_service);
        let mut certificate = votes_aggregator.append(vote, &self.committee, &header)?;

        // create a bounded channel for receiving votes from peers
        // capacity is committee size since we need at most one vote per peer
        let (tx_votes, mut rx_votes) = tokio::sync::mpsc::channel(self.committee.size().max(16));

        // create network requests for votes from peers
        let header_digest = header.digest();
        let peers = self.committee.others_primaries_by_id(Some(&self.authority_id)).into_iter();
        for (name, target) in peers {
            let header_clone = header.clone();
            let tx_votes = tx_votes.clone();
            let network = self.network.clone();
            let certificate_store = self.certificate_store.clone();
            let committee = self.committee.clone();
            let cancel_proposal = self.new_proposal.subscribe();
            let quorum_reached = quorum_notifier.subscribe();
            let task_name = format!("vote-{header_digest}-{name}");

            self.task_spawner.spawn_task(task_name, async move {
                // process request for vote
                let _ = tx_votes
                    .send(
                        // this will exit early on cancel_proposal or quorum_reached
                        Self::request_vote(
                            name,
                            header_clone,
                            target,
                            certificate_store,
                            network,
                            committee,
                            cancel_proposal,
                            quorum_reached,
                        )
                        .await,
                    )
                    .await;
            });
        }

        // drop sender so channel closes when all vote tasks complete
        drop(tx_votes);

        // loop through requests until complete or cancelled
        loop {
            // certificate created - no more votes needed
            if certificate.is_some() {
                // signal lingering vote tasks to stop (e.g. retrying dead peers)
                quorum_notifier.notify();
                break;
            }

            // receive votes or exit early if new proposal replaces this header before certification
            tokio::select! {
                result = rx_votes.recv() => {
                    debug!(target: "primary::certifier", auth=?self.authority_id, ?result, "next request in unordered futures");

                    match result {
                        // happy path
                        Some(Ok(vote)) => {
                            let authority_id = vote.author.clone();
                            // prevent invalid votes from derailing certification process
                            certificate = votes_aggregator
                                 .append(vote, &self.committee, &header)
                                 .unwrap_or_else(|e| {
                                     error!(target: "primary::certifier", "received an invalid vote from {authority_id:?}: {e:?}");
                                     None
                                 });
                        },

                        Some(Err(e)) => {
                            match self.handle_vote_error(&e, &header) {
                                VoteErrorAction::Abort(err) => {
                                    quorum_notifier.notify();
                                    return Err(err);
                                }
                                VoteErrorAction::Continue => {}
                            }
                        }

                        // all sending channels have dropped
                        None => {
                            break;
                        }
                    }
                },

                // exit early when cancel notification received
                _ = &cancel_proposal => {
                    debug!(target: "primary::certifier", "new proposal received - aborting proposal...");
                    return Err(DagError::Canceled);
                }
            }
        }

        // log detailed header info if we failed to form a certificate
        let certificate = certificate.ok_or_else(|| {
            if enabled!(tracing::Level::WARN) {
                let mut msg = format!(
                    "Failed to form certificate from header {header:#?} with parent certificates:"
                );
                for parent_digest in header.parents().iter() {
                    let parent_msg = match self.certificate_store.read(*parent_digest) {
                        Ok(Some(cert)) => format!("{cert:#?}\n"),
                        Ok(None) => {
                            format!("missing certificate for digest {parent_digest:?}")
                        }
                        Err(e) => format!(
                            "error retrieving certificate for digest {parent_digest:?}: {e:?}"
                        ),
                    };
                    msg.push_str(&parent_msg);
                }
                error!(target: "primary::certifier", auth=?self.authority_id, msg, "inside propose_header");
            }
            DagError::CouldNotFormCertificate(header.digest())
        })?;

        debug!(target: "primary::certifier", auth=?self.authority_id, "Assembled {certificate:?}");

        Ok(certificate)
    }

    /// The method to spawn tasks related to a header proposal.
    ///
    /// This listens for new proposal notifications to exit early.
    /// The method returns once enough votes are processed to certify the proposal,
    /// or if a new proposal arrives.
    async fn spawn_header_proposal(self, header: Header) {
        tokio::select! {
            // listen for new_proposal notification to exit
            // NOTE: sub here is okay bc no loop
            _ = self.new_proposal.subscribe() => {
                debug!(target: "primary::certifier", "new proposal notification received");
            },

            // receive enough votes for certification (or exit early)
            proposal_result = self.propose_header(header) => {
                match proposal_result {
                    Ok(certificate) => {
                        // successful certification proves peers accept our headers
                        self.vote_failures.clear_counters();

                        if let Err(e) = self.rayls_consensus_state_sync.process_own_certificate(certificate.clone()).await {
                            error!(target: "primary::certifier", "error accepting own certificate: {e}");
                            return;
                        }

                        // try to publish the certificate on gossip network.
                        // Dev (single-node): no peers to gossip to - we already processed our
                        // own certificate above, and publishing would only fail every round
                        // with NoPeersSubscribedToTopic - so skip it entirely.
                        #[cfg(not(feature = "dev-single-node-setup"))]
                        if let Err(e) = self.network.publish_certificate(certificate).await {
                            error!(target: "primary::certifier", ?e, "failed to gossip certificate");
                        }
                    }

                    Err(e) => {
                        match e {
                            // ignore cancelled proposal errors
                            DagError::Canceled => {
                                debug!(
                                    target: "primary::certifier",
                                    auth=?self.authority_id,
                                    "certifier cancelled proposed header task"
                                );
                            }
                            // log other errors loudly
                            e =>  {
                                error!(
                                    target: "primary::certifier",
                                    auth=?self.authority_id,
                                    "Certifier error on proposed header task: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Execute the main certification task until shutdown is signalled.
    ///
    /// Exiting outside of shutdown logs an error and triggers a node shutdown.
    async fn run(mut self) {
        info!(target: "primary::certifier", "Certifier on node {} has started successfully.", &self.authority_id);
        let mut rx_headers = self.consensus_bus.headers().subscribe();
        let shutdown = &self.config.shutdown().subscribe();
        // dedup re-proposes of same digest to avoid cancelling in-flight retries
        let mut in_flight: Option<(HeaderDigest, tokio::task::AbortHandle)> = None;
        loop {
            tokio::select! {
                // shutdown wins: certifying a further header during teardown wastes a vote
                // round trip the epoch is about to discard
                biased;

                _ = shutdown => {
                    debug!(target: "primary::certifier", "Certifier received shutdown signal");
                    // cancel any outstanding proposals and vote requests
                    self.new_proposal.notify();
                    if let Some((_, h)) = in_flight.take() {
                        h.abort();
                    }
                    break;
                }
                Some(header) = rx_headers.recv() => {
                    let digest = header.digest();
                    let round = header.round();
                    // Rounds restart every epoch, so a round number alone does not identify a
                    // proposal in a log. Both dedup paths and the store-error path carry it.
                    let epoch = header.epoch();

                    debug!(target: "primary::certifier", ?header, "{:?} received header!", &self.authority_id);

                    if let Some((d, h)) = &in_flight {
                        if *d == digest && !h.is_finished() {
                            debug!(
                                target: "primary::certifier",
                                ?digest, round, epoch,
                                "re-propose dedup: in-flight proposal for same digest, skipping",
                            );
                            continue;
                        }
                    }

                    // The guard above only covers a *running* attempt. An attempt that finished
                    // successfully leaves `is_finished() == true`, and the proposer keeps
                    // re-proposing until it observes its own certificate come back round - a
                    // window that is milliseconds normally but seconds wide under load. Falling
                    // through there mints a second certificate for the same header from whatever
                    // quorum answers this time: same digest, different signer set, different
                    // aggregate signature.
                    //
                    // Consensus cannot tell the two apart (`Certificate::digest()` is the header
                    // digest) and receivers silently drop whichever arrives second, so nodes end
                    // up holding different bytes for the same certificate. Harmless in the DAG,
                    // but the epoch-closing block hashes that signature into `extra_data`, so the
                    // two nodes seal different block hashes and the chain forks with consensus
                    // none the wiser - see 2026-08-16, where a ~10s execution stall held round 408
                    // uncertified until both attempts completed within the same flurry.
                    //
                    // LOAD-BEARING ORDERING, not visible from this file: the store lookup is only
                    // a sufficient guard because a finished task is guaranteed to have already
                    // written its certificate. `spawn_header_proposal` awaits
                    // `process_own_certificate`, which awaits the certificate manager's reply
                    // before returning, so the task cannot report `is_finished()` until the write
                    // has landed. If that await is ever removed, batched, or the own-certificate
                    // path starts going through `try_accept_certificate`'s pending branch (which
                    // returns without storing), this check silently stops covering the window and
                    // duplicate certificates become possible again.
                    let cert_digest = CertificateDigest::new(digest.into());
                    match self.certificate_store.contains(&cert_digest) {
                        // `info!`, unlike the in-flight case above: reaching here means a
                        // re-propose arrived for a header that had already certified, which is
                        // exactly the condition that used to mint a second certificate. It is
                        // worth seeing without enabling debug, and it is not extra noise - the
                        // proposer already emits a `warn!` for every re-propose, so this only
                        // ever appears alongside a louder line for the same event. A rising rate
                        // here is itself the signal that certification is racing the proposer.
                        Ok(true) => {
                            info!(
                                target: "primary::certifier",
                                ?digest, round, epoch,
                                "re-propose dedup: header already certified, skipping",
                            );
                            continue;
                        }
                        Ok(false) => {}
                        // Fail closed. The lookup is the only thing standing between a finished
                        // attempt and a second certificate for the same header, so re-certifying
                        // when we cannot answer "did this already certify?" reintroduces exactly
                        // the fork this guard exists to prevent - and a fork is silent, survives
                        // restarts, and killed a validator for 20 hours the one time it happened.
                        //
                        // The cost is real and deliberate: this skips *all* proposals while the
                        // error persists, not just re-proposals, so a node with a broken
                        // certificate store stops contributing certificates entirely. That is
                        // the safer failure - a stalled node is visible in this log line and
                        // recoverable by restarting it, whereas a forked node looks healthy until
                        // an epoch boundary and then cannot rejoin at all.
                        Err(e) => {
                            error!(
                                target: "primary::certifier",
                                auth=?self.authority_id,
                                ?digest, round, epoch,
                                "certificate store lookup failed, refusing to certify: {e}",
                            );
                            continue;
                        }
                    }

                    // cancel prior in-flight proposal; abort() as safety net
                    self.new_proposal.notify();
                    self.new_proposal = Notifier::new();
                    if let Some((_, h)) = in_flight.take() {
                        h.abort();
                    }

                    info!(
                        target: "primary::certifier",
                        auth = ?self.authority_id,
                        ?digest,
                        round,
                        epoch,
                        "spawning proposal task"
                    );

                    let certifier = self.clone();
                    let abort = self.task_spawner.spawn_abortable_task(
                        format!("propose-header-{digest:?}"),
                        async move { certifier.spawn_header_proposal(header).await },
                    );
                    in_flight = Some((digest, abort));
                },

            }
        }
    }
}
