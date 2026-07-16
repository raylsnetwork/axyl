//! Handle specific request types received from the network.

use super::{
    message::{ConsensusResult, MissingCertificatesRequest, PrimaryRPCError},
    AuthEquivocationMap, PrimaryResponse,
};
use crate::{
    error::{CertManagerError, PrimaryNetworkError, PrimaryNetworkResult},
    network::{
        message::PrimaryGossip,
        state::{AuthVoteState, InFlightGuard, IN_FLIGHT_TTL},
    },
    state_sync::{CertificateCollector, StateSynchronizer},
    ConsensusBus, NodeMode, RecentlyExecutedBlocks,
};
use parking_lot::Mutex;
use rayls_consensus_network::GossipMessage;
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_storage::{
    tables::ConsensusBlocks, ConsensusStore, EpochStore, ProposerStore, VoteDigestStore,
};
use rayls_infrastructure_types::{
    ensure,
    error::{CertificateError, HeaderError, HeaderResult},
    now, to_intent_message, try_decode, AuthorityIdentifier, BlockHash, BlockNumHash, BlsPublicKey,
    Certificate, CertificateDigest, ConsensusHeader, Database, Epoch, EpochCertificate,
    EpochRecord, Hash as _, Header, ProtocolSignature, RaylsSender as _, Round,
    SignatureVerificationState, Vote, VotesAggregator,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};
use tracing::{debug, error, info, trace, warn};

const MAX_AUTH_LAST_VOTE_ENTRIES: usize = 1000;
const MAX_CONSENSUS_CERTS_ENTRIES: usize = 100;
/// How long `committed_round` must stay flat while we keep looking behind before we demote to
/// `CvvInactive`. Any commit advance resets the clock, so a node that's catching up (or mid
/// epoch/mode transition) never trips it — only one genuinely stuck for the whole window does.
///
/// We anchor the stall on wall-clock, NOT on a verdict/round count, on purpose. The verdicts come
/// from incoming gossip and `primary_round` is bumped to the highest *received* round, so both are
/// peer-arrival-driven — a burst of ahead-certs would jump them while commit legitimately lags,
/// faking a stall. The clock is the only "how long" a burst can't accelerate. ~5s ≈ 5 rounds under
/// load (the only regime where we're actually behind; idle means no traffic, so not behind), which
/// is a sane "we produced rounds but committed nothing" threshold without depending on the (chain-
/// varying, idle-ceiling) `max_header_delay`.
const BEHIND_STALL_WINDOW: Duration = Duration::from_secs(5);

/// Tracks consensus-commit progress across `behind_consensus` "behind" verdicts, so we demote only
/// when genuinely stuck (`committed_round` flat for a whole window) rather than merely mid-
/// transition (commits still advancing). We track the DAG commit watermark, not the executed tip:
/// execution lags commit, and a node that's committing fine but executing slowly is still
/// participating — gating on the executed tip is exactly what produced the transition false
/// positives. Shared across `RequestHandler` clones, hence behind a lock.
#[derive(Debug)]
struct BehindTracker {
    /// Highest `committed_round` observed at a behind-verdict.
    last_committed_round: Round,
    /// When `committed_round` last advanced (or the tracker was last reset). Demotion fires only
    /// once commit has stayed flat past `BEHIND_STALL_WINDOW` measured from this instant.
    last_progress_at: Instant,
}

impl Default for BehindTracker {
    fn default() -> Self {
        Self { last_committed_round: 0, last_progress_at: Instant::now() }
    }
}

impl BehindTracker {
    /// Record a behind-verdict at the given `committed_round`. Returns true once commit has stayed
    /// flat for at least `window` (i.e. we're genuinely stuck, not merely transitioning). Any
    /// commit advance resets the stall clock, so a catching-up node never trips it. On firing the
    /// clock is re-armed, so a still-stuck node re-requests demotion at most once per window rather
    /// than on every subsequent behind-verdict.
    fn record_behind(&mut self, committed_round: Round, window: Duration) -> bool {
        if committed_round > self.last_committed_round {
            self.last_committed_round = committed_round;
            self.last_progress_at = Instant::now();
            return false;
        }
        if self.last_progress_at.elapsed() >= window {
            self.last_progress_at = Instant::now();
            return true;
        }
        false
    }

    /// Caught up (or no longer an active CVV): treat as progress and re-arm the stall clock.
    fn reset(&mut self, committed_round: Round) {
        self.last_committed_round = committed_round;
        self.last_progress_at = Instant::now();
    }
}

/// Handle request types received from peers.
#[derive(Clone, Debug)]
pub(crate) struct RequestHandler<DB> {
    consensus_config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    rayls_consensus_state_sync: StateSynchronizer<DB>,
    /// In-flight parent requests: (round, digest) -> requesting authority.
    requested_parents: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    /// Per-authority vote state for equivocation detection and caching.
    auth_last_vote: Arc<Mutex<AuthEquivocationMap>>,
    /// Consensus result signers per digest.
    consensus_certs: Arc<Mutex<HashMap<BlockHash, VotesAggregator<ConsensusResult>>>>,
    /// Commit-progress tracker gating demotion to `CvvInactive` (see [`behind_consensus`]).
    behind_tracker: Arc<Mutex<BehindTracker>>,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
    ) -> Self {
        Self {
            consensus_config,
            consensus_bus,
            rayls_consensus_state_sync,
            requested_parents: Default::default(),
            auth_last_vote: Default::default(),
            consensus_certs: Default::default(),
            behind_tracker: Default::default(),
        }
    }

    fn cleanup_auth_last_vote(&self) {
        let mut cache = self.auth_last_vote.lock();
        evict_expired_inflight(&mut cache);
        evict_oldest_completed(&mut cache, MAX_AUTH_LAST_VOTE_ENTRIES);
    }

    /// Return true and request CvvInactive if the peer's cert shows we are *stuck* behind.
    ///
    /// "Behind" alone is not enough to demote: a node mid epoch/mode transition — or simply
    /// syncing — is briefly behind by construction yet keeps committing and self-heals.
    /// We demote only when behind AND not making progress: `committed_round` stays flat for the
    /// whole `BEHIND_STALL_WINDOW`. This replaces the old `is_transitioning()` suppressor, which
    /// was racy (TOCTOU) and, worse, blind to the window before we detect our own boundary —
    /// exactly when a lagging node would wrongly self-demote.
    async fn behind_consensus(&self, epoch: Epoch, round: Round, number: Option<u64>) -> bool {
        if self.consensus_config.shutdown().was_notified() {
            return false;
        }

        let (exec_number, exec_epoch, exec_round) = self
            .consensus_config
            .node_storage()
            .last_record::<ConsensusBlocks>()
            .map(|(n, h)| (n, h.sub_dag.leader_epoch(), h.sub_dag.leader_round()))
            .unwrap_or((0, 0, 0));

        let relative_gc_depth = self.consensus_config.parameters().gc_depth / 4;
        let active_cvv = self.consensus_bus.node_mode().borrow().is_active_cvv();
        let our_epoch = self.consensus_config.committee().epoch();
        // DAG commit watermark — the progress signal we gate the stall on. Advances on our own
        // commits (self-paced), so a gossip burst can't accelerate it the way it does the executed
        // tip or `primary_round`.
        let committed_round = *self.consensus_bus.committed_round_updates().borrow();

        let comparable_round = if epoch == exec_epoch {
            Some(exec_round)
        } else if epoch == our_epoch && exec_epoch < our_epoch {
            // new epoch, no commits yet - fall back to proposer round
            Some(*self.consensus_bus.primary_round_updates().borrow())
        } else {
            None
        };
        let outside_gc_window = comparable_round.is_some_and(|r| r + relative_gc_depth < round);

        // use configured epoch, not exec_epoch which lags at epoch start
        let epoch_behind = if epoch <= our_epoch {
            false
        } else if let Some(number) = number {
            exec_number + 1 < number
        } else if exec_epoch + 1 == epoch {
            round > relative_gc_depth.max(6)
        } else {
            epoch > exec_epoch
        };

        let behind = active_cvv && (outside_gc_window || epoch_behind);
        if !behind {
            // Caught up (or not an active CVV): clear the stall tracker.
            self.behind_tracker.lock().reset(committed_round);
            return false;
        }

        // We look behind. Demote only if commit has been flat for the whole stall window: a
        // transitioning or syncing node keeps committing and resets the clock, so it never demotes
        // — only one stuck for the full window does. record_behind re-arms the clock when it fires,
        // so a concurrent clone (they share the Arc<Mutex>) can't re-trigger demotion in a loop.
        let stalled =
            self.behind_tracker.lock().record_behind(committed_round, BEHIND_STALL_WINDOW);

        if stalled {
            debug!(
                target: "primary",
                epoch, round, our_epoch, exec_epoch, exec_number, committed_round,
                "behind and commit stalled - requesting CvvInactive"
            );
            self.consensus_bus.request_mode_transition(NodeMode::CvvInactive);
            true
        } else {
            // Behind but still progressing: don't demote; process the cert (it helps us catch up).
            false
        }
    }

    fn get_committee(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>> {
        self.consensus_config.get_committee_keys_for_epoch(epoch)
    }

    /// Process gossip from the committee.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> PrimaryNetworkResult<()> {
        let GossipMessage { data, topic, .. } = msg;
        let gossip = try_decode(data)?;

        match gossip {
            PrimaryGossip::Certificate(cert) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::primary_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                let unverified_cert = cert.validate_received().map_err(CertManagerError::from)?;

                let epoch = unverified_cert.header().epoch;
                // verify early so behind_consensus can act; verification is cached on the cert
                if let Some(committee) = self.get_committee(epoch) {
                    match unverified_cert.verify_cert(&committee) {
                        Ok(cert) => {
                            if self.behind_consensus(epoch, cert.header().round, None).await {
                                warn!(target: "primary", "certificate indicates we are behind, go to catchup mode!");
                                return Ok(());
                            }
                            self.rayls_consensus_state_sync.process_peer_certificate(cert).await?;
                        }
                        Err(e) => warn!(target: "primary", "Received invalid cert {e}"),
                    }
                } else {
                    // ignore unverifiable cert: going inactive without committee is unsafe
                    warn!(target: "primary", "failed to get committee for epoch {epoch}, ignoring certificate!", );
                }
            }
            PrimaryGossip::Consensus(result) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::consensus_output_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                let consensus_result_hash = result.digest();
                let ConsensusResult { epoch, round, number, hash, validator: key, signature } =
                    *result;
                let (old_number, old_hash) =
                    *self.consensus_bus.last_published_consensus_num_hash().borrow();
                if hash == old_hash || old_number >= number {
                    // We have already dealt with this hash or we are past this output.
                    return Ok(());
                }
                if let Some(committee) = self.get_committee(epoch) {
                    ensure!(
                        committee.contains(&key),
                        PrimaryNetworkError::PeerNotInCommittee(Box::new(key))
                    );
                    ensure!(
                        signature.verify_secure(&to_intent_message(consensus_result_hash), &key),
                        PrimaryNetworkError::UnknownConsensusHeaderCert(hash)
                    );
                    // per-digest signer tracking prevents single-validator inflation
                    let enough_sigs = (committee.len() / 3) + 1;
                    let quorum = {
                        let mut cache = self.consensus_certs.lock();
                        if cache.len() > MAX_CONSENSUS_CERTS_ENTRIES
                            && !cache.contains_key(&consensus_result_hash)
                        {
                            cache.clear();
                        }
                        let aggregator = cache
                            .entry(consensus_result_hash)
                            .or_insert_with(|| VotesAggregator::new(enough_sigs as u64));
                        match aggregator.append(*result, 1) {
                            Ok(reached) => reached,
                            Err(_) => return Ok(()),
                        }
                    };
                    if quorum {
                        if self.behind_consensus(epoch, round, Some(number)).await {
                            self.consensus_certs.lock().clear();
                            return Ok(());
                        }
                        self.consensus_bus
                            .last_published_consensus_num_hash()
                            .send_replace((number, hash));
                        self.consensus_certs.lock().clear();
                    }
                } else {
                    self.consensus_bus.requested_missing_epoch().send_if_modified(|current| {
                        if epoch > *current {
                            *current = epoch;
                            true
                        } else {
                            false
                        }
                    });
                    warn!(
                        target: "primary::consensus_result",
                        epoch,
                        round,
                        number,
                        "no committee found for epoch, triggered epoch collector, dropping consensus result"
                    );
                }
            }
            PrimaryGossip::EpochVote(vote) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::epoch_vote_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                let (tx, rx) = oneshot::channel();
                let _ = self.consensus_bus.new_epoch_votes().send((*vote, tx)).await;
                match rx.await {
                    Ok(res) => res?,
                    // do not punish peer for an internal channel issue
                    Err(e) => error!(target: "primary", "error waiting on epoch vote result: {e}"),
                }
            }
        }

        Ok(())
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    pub(crate) async fn vote(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        // peer must be the header author to prevent vote cache poisoning
        let committee_peer = header.author.clone();
        let auth_id: AuthorityIdentifier = peer.into();
        if let Some(auth) = self.consensus_config.committee().authority(&committee_peer) {
            ensure!(auth_id == auth.id(), HeaderError::PeerNotAuthor.into());
        } else {
            return Err(HeaderError::UnknownAuthority(committee_peer.to_string()).into());
        }
        if let Some(result) = self.check_equivocation(&header, &parents) {
            return result;
        }

        // InFlight -> Completed on all exit paths (panic, abort, error)
        let guard = InFlightGuard::new(
            self.auth_last_vote.clone(),
            header.author().clone(),
            header.epoch(),
            header.round(),
            header.digest(),
        );

        let res = self.vote_inner(header, parents).await;

        let cached_res: PrimaryResponse = match &res {
            Ok(msg) => msg.clone(),
            Err(e) => PrimaryResponse::into_error_ref(e),
        };
        guard.complete(cached_res);

        self.cleanup_auth_last_vote();

        res
    }

    /// Check for equivocation or duplicate vote and mark the author as in-flight.
    /// Return `None` to proceed to `vote_inner`; `Some` short-circuits.
    fn check_equivocation(
        &self,
        header: &Header,
        parents: &[Certificate],
    ) -> Option<PrimaryNetworkResult<PrimaryResponse>> {
        let mut cache = self.auth_last_vote.lock();
        let author = header.author();
        let in_flight = AuthVoteState::InFlight {
            epoch: header.epoch(),
            round: header.round(),
            digest: header.digest(),
            created_at: Instant::now(),
        };

        let Some(state) = cache.get(author) else {
            cache.insert(author.clone(), in_flight);
            return None;
        };

        match state {
            AuthVoteState::InFlight { round: inflight_round, created_at, .. } => {
                if header.round() > *inflight_round {
                    // stale in-flight from a cancelled or crashed task, supersede it
                } else if created_at.elapsed() > IN_FLIGHT_TTL {
                    // TTL expired - vote task likely hung or leaked, allow re-processing
                    warn!(
                        target: "primary::handler",
                        author = %header.author(),
                        round = header.round(),
                        elapsed_secs = created_at.elapsed().as_secs(),
                        "InFlight entry expired, allowing re-processing"
                    );
                } else {
                    return Some(Ok(PrimaryResponse::RecoverableError(PrimaryRPCError(
                        "vote already in flight for this authority".into(),
                    ))));
                }
            }
            AuthVoteState::Completed {
                epoch: last_epoch,
                round: last_round,
                digest: last_digest,
                response,
            } => {
                // same header digest - serve from cache or re-process
                if *last_digest == header.digest() {
                    match response {
                        None | Some(PrimaryResponse::RecoverableError(_)) => {
                            // no definitive result cached - re-process
                        }
                        Some(PrimaryResponse::MissingParents(missing)) => {
                            if parents.is_empty() {
                                debug!(
                                    target: "primary::handler",
                                    ?header,
                                    missing_count = missing.len(),
                                    "vote retry with 0 parents after MissingParents, re-issuing"
                                );
                                return Some(Ok(PrimaryResponse::MissingParents(missing.clone())));
                            }
                            if parents.len() != missing.len() {
                                return Some(Err(HeaderError::WrongNumberOfParents(
                                    missing.len(),
                                    parents.len(),
                                )
                                .into()));
                            }
                            for digest in parents.iter().map(|p| p.digest()) {
                                if !missing.contains(&digest) {
                                    return Some(Err(HeaderError::InvalidParents.into()));
                                }
                            }
                            // all parents match - re-process
                        }
                        Some(res) => return Some(Ok(res.clone())),
                    }
                } else if header.epoch() < *last_epoch
                    || (*last_epoch == header.epoch() && *last_round >= header.round())
                {
                    // different digest, same or older round - equivocation
                    return Some(Err(HeaderError::AlreadyVotedForLaterRound {
                        theirs: header.round(),
                        ours: *last_round,
                    }
                    .into()));
                }
            }
        }

        // early-return paths leave the existing entry intact;
        // fall-through means proceed - replace with in-flight
        cache.insert(author.clone(), in_flight);
        None
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    async fn vote_inner(
        &self,
        header: Header,
        parents: Vec<Certificate>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        // current committee
        let committee = self.consensus_config.committee();

        // validate header
        header.validate(committee)?;
        let max_round = *self.consensus_bus.committed_round_updates().borrow()
            + self.consensus_config.parameters().gc_depth;
        // Make sure the header is not unreasonable in the future.
        ensure!(
            header.round() <= max_round,
            HeaderError::TooNew {
                digest: header.digest(),
                header_round: header.round(),
                max_round,
            }
            .into()
        );

        // validate parents
        let num_parents = parents.len();
        ensure!(
            num_parents <= committee.size(),
            HeaderError::TooManyParents(num_parents, committee.size()).into()
        );
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_in_votes
            .inc_by(num_parents as u64);

        // A vote attests to data availability and structure, not execution state, so it does not
        // wait for execution (waiting only stalls a behind node; fork safety holds because the
        // commit path re-checks execution and the subscriber BLS-verifies before execution). The
        // one kept check is a non-blocking breaker: refuse to vote if our own executed block at the
        // anchor already diverges, so a quorum cannot certify a forked anchor.
        let target = header.latest_execution_block;
        let anchor_forked = anchor_diverges_from_executed(
            &self.consensus_bus.recently_executed_blocks().borrow(),
            target,
        );
        if anchor_forked {
            warn!(
                target: "primary::handler",
                author = %header.author(),
                round = header.round(),
                target_block = target.number,
                "vote_inner: header anchor diverges from our executed chain, refusing to vote"
            );
            return Err(HeaderError::UnknownExecutionResult(target).into());
        }

        if parents.is_empty() {
            let missing_parents = self.check_for_missing_parents(&header).await?;
            if !missing_parents.is_empty() {
                debug!(
                    target: "primary::handler",
                    author = %header.author(),
                    round = header.round(),
                    epoch = header.epoch(),
                    header_parent_count = header.parents().len(),
                    missing_count = missing_parents.len(),
                    ?missing_parents,
                    header_parents = ?header.parents(),
                    "responding with MissingParents for vote request"
                );
                return Ok(PrimaryResponse::MissingParents(missing_parents));
            }
        } else {
            let verified = parents
                .into_iter()
                .map(|mut cert| {
                    let sig =
                        cert.aggregated_signature().ok_or(HeaderError::ParentMissingSignature)?;
                    cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                        sig,
                    ));
                    Ok(cert)
                })
                .collect::<HeaderResult<Vec<Certificate>>>()?;

            self.try_accept_unknown_certs(&header, verified).await?;
        }

        // blocks until every parent is stored, times out on missing certs
        let parents =
            self.rayls_consensus_state_sync.notify_read_parent_certificates(&header).await?;

        // parents: previous round, precede header, unique authorities, staked quorum
        let mut parent_authorities = BTreeSet::new();
        let mut stake = 0;
        for parent in parents.iter() {
            ensure!(
                parent.epoch() == header.epoch(),
                HeaderError::InvalidEpoch { theirs: parent.epoch(), ours: header.epoch() }.into()
            );
            ensure!(parent.round() + 1 == header.round(), HeaderError::InvalidParentRound.into());

            // strict >: allows sub-second block production (created_at is unix seconds)
            ensure!(
                header.created_at() >= parent.header().created_at(),
                HeaderError::InvalidParentTimestamp {
                    header: *header.created_at(),
                    parent: *parent.header().created_at()
                }
                .into()
            );

            ensure!(
                parent_authorities.insert(parent.header().author()),
                HeaderError::DuplicateParents.into()
            );

            stake += committee.voting_power_by_id(parent.origin());
        }

        let threshold = committee.quorum_threshold();
        ensure!(
            stake >= threshold,
            CertManagerError::from(CertificateError::Inquorate { stake, threshold }).into()
        );

        // blocks until batches become available
        self.rayls_consensus_state_sync.sync_header_batches(&header, false, 0).await?;

        let now = now();
        if &now < header.created_at() {
            if *header.created_at() - now
                <= self
                    .consensus_config
                    .network_config()
                    .sync_config()
                    .max_header_time_drift_tolerance
            {
                tokio::time::sleep(Duration::from_secs(*header.created_at() - now)).await;
            } else {
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {now}",
                    header,
                    *header.created_at()
                );

                return Err(HeaderError::InvalidTimestamp {
                    created: *header.created_at(),
                    received: now,
                }
                .into());
            }
        }

        // Fail closed: an Observer (non-validator) has no authority_id and cannot vote. A byzantine
        // peer can deliver a Vote request to such a node, and under panic=abort an `expect` here
        // would turn that into a node abort, so return gracefully instead.
        let authority_id = match self.consensus_config.authority_id() {
            Some(id) => id,
            None => {
                return Err(PrimaryNetworkError::Internal(
                    "node is not a validator; cannot vote".to_string(),
                ))
            }
        };

        let previous_vote = self
            .consensus_config
            .node_storage()
            .read_vote_info(header.author())
            .map_err(HeaderError::Storage)?;
        if let Some(vote_info) = previous_vote {
            ensure!(
                header.epoch() == vote_info.epoch(),
                HeaderError::InvalidEpoch { theirs: header.epoch(), ours: vote_info.epoch() }
                    .into()
            );
            ensure!(
                header.round() >= vote_info.round(),
                HeaderError::AlreadyVotedForLaterRound {
                    theirs: header.round(),
                    ours: vote_info.round()
                }
                .into()
            );
            if header.round() == vote_info.round() {
                // do not vote twice for the same authority in the same epoch/round
                let vote =
                    Vote::new(&header, authority_id.clone(), self.consensus_config.key_config());

                if vote.digest() != vote_info.vote_digest() {
                    warn!(
                        "Authority {} submitted different header {:?} for voting",
                        header.author(),
                        header,
                    );

                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .votes_dropped_equivocation_protection
                        .inc();

                    return Err(HeaderError::AlreadyVoted(header.digest(), header.round()).into());
                }

                return Ok(PrimaryResponse::Vote(vote));
            }
        }

        let vote = Vote::new(&header, authority_id, self.consensus_config.key_config());

        self.consensus_config.node_storage().write_vote(&vote)?;

        self.consensus_config
            .node_storage()
            .write_last_proposed_by_authority(header.author.clone(), &header)?;

        Ok(PrimaryResponse::Vote(vote))
    }

    /// Identify parents not in local storage, pending, or already requested.
    async fn check_for_missing_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        let mut unknown_certs =
            self.rayls_consensus_state_sync.identify_unknown_parents(header).await?;

        let limit = self.consensus_bus.primary_round_updates().borrow().saturating_sub(
            self.consensus_config.network_config().sync_config().max_proposed_header_age_limit,
        );

        // age limit only applies when parents are missing
        if !unknown_certs.is_empty() {
            ensure!(
                limit <= header.round(),
                HeaderError::TooOld {
                    digest: header.digest(),
                    header_round: header.round(),
                    max_round: limit,
                }
            );
        }

        // hold lock across gc and retain to keep limit_round consistent with requested_parents
        let mut current_requests = self.requested_parents.lock();

        // minimum parent round is limit - 1
        while let Some(((round, _), _)) = current_requests.first_key_value() {
            if round < &limit.saturating_sub(1) {
                current_requests.pop_first();
            } else {
                break;
            }
        }

        unknown_certs.retain(|digest| {
            let key = (header.round() - 1, *digest);
            if let std::collections::btree_map::Entry::Vacant(e) = current_requests.entry(key) {
                e.insert(header.author().clone());
                true
            } else {
                false
            }
        });

        Ok(unknown_certs)
    }

    /// Accept parent certs included with a vote if previously requested.
    async fn try_accept_unknown_certs(
        &self,
        header: &Header,
        mut parents: Vec<Certificate>,
    ) -> PrimaryNetworkResult<()> {
        let mut keys_to_process = Vec::new();
        {
            let requested_parents = self.requested_parents.lock();
            parents.retain(|cert| {
                let req = (cert.round(), cert.digest());
                if let Some(authority) = requested_parents.get(&req) {
                    if authority == header.author() {
                        keys_to_process.push(req);
                        return true;
                    }
                }
                false
            });
        }

        for parent in parents {
            let key = (parent.round(), parent.digest());
            match self.rayls_consensus_state_sync.process_peer_certificate(parent).await {
                Ok(()) => {
                    self.requested_parents.lock().remove(&key);
                }
                // Pending is expected during startup/sync; keep it in requested_parents
                Err(CertManagerError::Pending(digest)) => {
                    debug!(
                        target: "primary::handler",
                        ?digest,
                        "parent certificate is pending (waiting for its own parents)"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Retrieve missing certificates, bounded by time and chunk size.
    /// MDBX reads are offloaded to `spawn_blocking`.
    pub(crate) async fn retrieve_missing_certs(
        &self,
        request: MissingCertificatesRequest,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let consensus_config = self.consensus_config.clone();
        tokio::task::spawn_blocking(move || {
            collect_missing_certs_blocking(request, consensus_config)
        })
        .await
        .map_err(|e| PrimaryNetworkError::Internal(format!("cert-collector join error: {e}")))?
    }

    /// Retrieve a consensus header from local storage.
    pub(super) async fn retrieve_consensus_header(
        &self,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let header = match (number, hash) {
            (_, Some(hash)) => self.get_header_by_hash(hash)?,
            (Some(number), _) => self.get_header_by_number(number)?,
            (None, None) => self.get_latest_output()?,
        };

        Ok(PrimaryResponse::ConsensusHeader(Arc::new(header)))
    }

    /// Retrieve an epoch record from local storage.
    pub(super) async fn retrieve_epoch_record(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let (record, certificate) = match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash).await?,
            (Some(epoch), _) => self.get_epoch_by_number(epoch).await?,
            (None, None) => return Err(PrimaryNetworkError::InvalidEpochRequest),
        };

        Ok(PrimaryResponse::EpochRecord { record, certificate })
    }

    /// Retrieve the consensus header by number.
    fn get_header_by_number(&self, number: u64) -> PrimaryNetworkResult<ConsensusHeader> {
        match self.consensus_config.node_storage().get_consensus_by_number(number) {
            Some(header) => Ok(header),
            None => Err(PrimaryNetworkError::UnknownConsensusHeaderNumber(number)),
        }
    }

    /// Retrieve the consensus header by hash
    fn get_header_by_hash(&self, hash: BlockHash) -> PrimaryNetworkResult<ConsensusHeader> {
        match self.consensus_config.node_storage().get_consensus_by_hash(hash) {
            Some(header) => Ok(header),
            None => Err(PrimaryNetworkError::UnknownConsensusHeaderDigest(hash)),
        }
    }

    /// Return the highest consensus header known to this node.
    fn get_latest_output(&self) -> PrimaryNetworkResult<ConsensusHeader> {
        let executed = self
            .consensus_config
            .node_storage()
            .last_record::<ConsensusBlocks>()
            .map(|(_, header)| header);
        let executed_number = executed.as_ref().map(|h| h.number).unwrap_or(0);

        let (gossip_number, gossip_hash) =
            *self.consensus_bus.last_published_consensus_num_hash().borrow();
        if gossip_number > executed_number {
            if let Some(header) =
                self.consensus_config.node_storage().get_consensus_by_hash(gossip_hash)
            {
                return Ok(header);
            }
        }

        executed.ok_or(PrimaryNetworkError::Internal("Consensus headers unavailable".to_string()))
    }

    /// Retrieve the consensus header by number.
    async fn get_epoch_by_number(
        &self,
        epoch: Epoch,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_config.node_storage().get_epoch_by_number(epoch) {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_config.node_storage().get_epoch_by_number(epoch)
                    {
                        return Ok((record, cert));
                    }
                }
                Err(PrimaryNetworkError::UnavailableEpoch(epoch))
            }
            None => Err(PrimaryNetworkError::UnavailableEpoch(epoch)),
        }
    }

    /// Retrieve the consensus header by hash
    async fn get_epoch_by_hash(
        &self,
        hash: BlockHash,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_config.node_storage().get_epoch_by_hash(hash) {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_config.node_storage().get_epoch_by_hash(hash)
                    {
                        return Ok((record, cert));
                    }
                }
                Err(PrimaryNetworkError::UnavailableEpochDigest(hash))
            }
            None => Err(PrimaryNetworkError::UnavailableEpochDigest(hash)),
        }
    }
}

/// Run the cert-collector synchronously inside `spawn_blocking`.
fn collect_missing_certs_blocking<DB: Database>(
    request: MissingCertificatesRequest,
    consensus_config: ConsensusConfig<DB>,
) -> PrimaryNetworkResult<PrimaryResponse> {
    let mut collector = CertificateCollector::new(request, consensus_config)?;

    let mut missing = Vec::new();
    for cert in collector.by_ref() {
        missing.push(cert?);
    }

    let min_round = missing.iter().map(|c| c.round()).min().unwrap_or(0);
    let max_round = missing.iter().map(|c| c.round()).max().unwrap_or(0);
    let unique_authors = missing.iter().map(|c| c.header().author()).collect::<BTreeSet<_>>().len();
    info!(
        target: "cert-collector",
        count = missing.len(),
        min_round,
        max_round,
        unique_authors,
        "Collected certificates in {}ms",
        collector.start_time().elapsed().as_millis(),
    );

    Ok(PrimaryResponse::RequestedCertificates(missing))
}

/// Returns true when local execution has a block at `target.number` whose hash diverges from
/// `target.hash` - evidence the header's execution anchor is forked.
///
/// Evidence-only: a target the node has not executed returns false, so a behind node still votes;
/// only a node whose own executed state contradicts the anchor rejects.
fn anchor_diverges_from_executed(recent: &RecentlyExecutedBlocks, target: BlockNumHash) -> bool {
    recent.block_at_number(target.number).is_some_and(|block| block.hash() != target.hash)
}

/// Remove InFlight entries that outlived their TTL (safety net for leaked guards).
fn evict_expired_inflight(cache: &mut AuthEquivocationMap) {
    let expired: Vec<_> = cache
        .iter()
        .filter(|(_, s)| {
            matches!(s, AuthVoteState::InFlight { created_at, .. } if created_at.elapsed() > IN_FLIGHT_TTL)
        })
        .map(|(k, _)| k.clone())
        .collect();
    if !expired.is_empty() {
        warn!(target: "primary::handler", count = expired.len(), "evicting expired InFlight entries");
        for key in &expired {
            cache.remove(key);
        }
    }
}

/// Evict oldest Completed entries when cache exceeds `max_entries`.
fn evict_oldest_completed(cache: &mut AuthEquivocationMap, max_entries: usize) {
    if cache.len() <= max_entries {
        return;
    }
    let min_round = cache.values().map(|s| s.round()).min().unwrap_or(0);
    let to_remove = cache.len() - max_entries;
    let keys: Vec<_> = cache
        .iter()
        .filter(|(_, s)| {
            matches!(s, AuthVoteState::Completed { .. }) && s.round() <= min_round + 10
        })
        .take(to_remove)
        .map(|(k, _)| k.clone())
        .collect();
    for key in &keys {
        cache.remove(key);
    }
    trace!(target: "primary::handler", removed = keys.len(), remaining = cache.len(), "evicted old vote cache entries");
}

#[cfg(test)]
mod tests {
    use super::{anchor_diverges_from_executed, BehindTracker};
    use crate::RecentlyExecutedBlocks;
    use rayls_infrastructure_types::{BlockNumHash, ExecHeader, SealedHeader, B256};
    use std::time::Duration;

    fn sealed_at(number: u64) -> SealedHeader {
        SealedHeader::new(
            ExecHeader { number, ..Default::default() },
            B256::repeat_byte(number as u8),
        )
    }

    /// The non-blocking fork circuit-breaker: a divergent already-executed anchor is rejected, a
    /// matching one is accepted, and a not-yet-executed (ahead) anchor is accepted - a behind node
    /// votes on data availability and structure rather than waiting for execution to catch up.
    #[test]
    fn anchor_divergence_is_evidence_only() {
        let mut recent = RecentlyExecutedBlocks::new(8);
        recent.push_latest(sealed_at(5)); // executed block 5, hash repeat_byte(5)

        // matching anchor at an executed height: no divergence, vote.
        assert!(!anchor_diverges_from_executed(&recent, sealed_at(5).num_hash()));

        // divergent anchor at an executed height: fork evidence, reject.
        let forked = BlockNumHash::new(5, B256::repeat_byte(0xEE));
        assert!(anchor_diverges_from_executed(&recent, forked));

        // anchor ahead of our execution (block 9 not executed): not evidence, vote without waiting.
        assert!(!anchor_diverges_from_executed(&recent, sealed_at(9).num_hash()));
    }

    // Tests drive the window argument rather than a real clock: a zero window means "any flat
    // verdict is already past the window" → demote now; a large window means "the stall hasn't
    // lasted long enough" → never demote. That keeps them deterministic without sleeping.
    #[test]
    fn flat_within_window_does_not_demote() {
        let mut t = BehindTracker::default();
        t.reset(10);
        // Commit flat but the window hasn't elapsed: never demotes.
        assert!(!t.record_behind(10, Duration::from_secs(3600)));
        assert!(!t.record_behind(10, Duration::from_secs(3600)));
    }

    #[test]
    fn flat_past_window_demotes_then_rearms() {
        let mut t = BehindTracker::default();
        t.reset(10);
        // Commit flat past the (zero) window → demote.
        assert!(t.record_behind(10, Duration::ZERO));
        // Re-armed on firing: with a real window the next flat verdict does NOT double-demote.
        assert!(!t.record_behind(10, Duration::from_secs(3600)));
    }

    #[test]
    fn commit_advance_resets_window() {
        let mut t = BehindTracker::default();
        t.reset(10);
        // Even past a zero window, an advancing committed_round resets instead of demoting: a
        // catching-up node never trips it.
        assert!(!t.record_behind(11, Duration::ZERO));
        assert_eq!(t.last_committed_round, 11);
        // The clock is fresh from that advance, so a flat verdict within a real window won't
        // demote.
        assert!(!t.record_behind(11, Duration::from_secs(3600)));
    }

    #[test]
    fn reset_rearms_after_stall() {
        let mut t = BehindTracker::default();
        t.reset(10);
        // Would demote (flat, past zero window)...
        assert!(t.record_behind(10, Duration::ZERO));
        // ...but a not-behind verdict moves the baseline and re-arms the clock, so a fresh flat
        // verdict within a real window does not demote.
        t.reset(15);
        assert_eq!(t.last_committed_round, 15);
        assert!(!t.record_behind(15, Duration::from_secs(3600)));
    }
}
