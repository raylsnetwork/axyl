//! Fetch missing certificates from peers and verify them.

use crate::{
    error::{CertManagerError, CertManagerResult},
    network::{MissingCertificatesRequest, PrimaryNetworkHandle},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::{monitored_future, monitored_scope};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::{rngs::ThreadRng, seq::SliceRandom};
use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_network_types::FetchCertificatesResponse;
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{
    validate_received_certificate, AuthorityIdentifier, BlsPublicKey, Certificate, Committee,
    Database, Epoch, Hash as _, Noticer, RaylsReceiver, RaylsSender, Round, TaskManager,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    task::JoinSet,
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/certificate_fetcher_tests.rs"]
mod certificate_fetcher_tests;

/// Interval between periodic probes when catching up or have known gaps.
const CATCHUP_PROBE_INTERVAL: Duration = Duration::from_secs(10);
/// Probe interval for synced CvvActive nodes with no targets.
const IDLE_PROBE_INTERVAL: Duration = Duration::from_secs(60);
/// Seconds to wait for a response before issuing another parallel fetch request.
const PARALLEL_FETCH_REQUEST_INTERVAL_SECS: Duration = Duration::from_secs(5);
/// The timeout for an iteration of parallel fetch requests over all peers would be
/// num peers * PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
const PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT: Duration = Duration::from_secs(15);
/// Clear skip_rounds after this many consecutive failures.
const STALE_SKIP_ROUNDS_FALLBACK_THRESHOLD: u32 = 5;
/// Lower threshold for CvvInactive mode where speed matters more.
const STALE_SKIP_ROUNDS_CATCHUP_THRESHOLD: u32 = 2;
/// Backoff when all fetched certs are from a future epoch.
/// The node cannot make progress until epoch transition completes,
/// so polling aggressively wastes bandwidth. 30s balances responsiveness
/// with not hammering peers at ~700 req/s.
const EPOCH_MISMATCH_BACKOFF: Duration = Duration::from_secs(30);

/// Outcome of a certificate fetch task for backoff decisions.
#[derive(Debug, Clone, Copy)]
enum FetchOutcome {
    /// All certificates accepted successfully.
    Success,
    /// Hard failure (network error, empty response, etc.).
    HardFailure,
    /// Some certificates accepted, others pending parents.
    PartialProgress,
    /// All certificates from a non-matching epoch.
    EpochMismatch,
}

#[derive(Clone, Debug)]
pub enum CertificateFetcherCommand {
    /// Fetch the certificate and its ancestors.
    Ancestors(Arc<Certificate>),
    /// Fetch once from a random primary.
    Kick,
}

/// The CertificateFetcher is responsible for fetching certificates that this primary is missing
/// from peers. It operates a loop which listens for commands to fetch a specific certificate's
/// ancestors, or just to start one fetch attempt.
///
/// In each fetch, the CertificateFetcher first scans locally available certificates. Then it sends
/// this information to a random peer. The peer would reply with the missing certificates that can
/// be accepted by this primary. After a fetch completes, another one will start immediately if
/// there are more certificates missing ancestors.
pub(crate) struct CertificateFetcher<DB> {
    /// Internal state of CertificateFetcher.
    state: Arc<CertificateFetcherState<DB>>,
    /// The committee information.
    committee: Committee,
    /// Persistent storage for certificates. Read-only usage.
    certificate_store: DB,
    /// Used to get Receiver for signal of round changes.
    consensus_bus: ConsensusBus,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Map of validator to target rounds that local store must catch up to.
    /// The targets are updated with each certificate missing parents sent from the core.
    /// Each fetch task may satisfy some / all / none of the targets.
    targets: BTreeMap<AuthorityIdentifier, Round>,
    /// Keeps the handle to the (at most one) inflight fetch certificates task.
    fetch_certificates_task: JoinSet<FetchOutcome>,
    /// The max allowable RPC message size shared with peers (in bytes).
    /// This value should match the `request_response` codec's "max_rpc_message_size".
    max_rpc_message_size: usize,
    /// Track consecutive fetch failures for exponential backoff.
    consecutive_failures: u32,
    /// Last fetch attempt time for rate limiting.
    last_fetch_attempt: Option<Instant>,
    /// Suppresses probes until this instant after an epoch-mismatch outcome.
    /// Prevents tight spin when all peers are on a future epoch.
    epoch_mismatch_until: Option<Instant>,
}

/// Thread-safe internal state of CertificateFetcher shared with its fetch task.
struct CertificateFetcherState<DB> {
    /// Identity of the current authority.
    authority_id: Option<AuthorityIdentifier>,
    /// Network client to fetch certificates from other primaries.
    network: PrimaryNetworkHandle,
    /// Accepts Certificates into local storage.
    rayls_consensus_state_sync: StateSynchronizer<DB>,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
}

impl<DB: Database> CertificateFetcher<DB> {
    pub(crate) fn spawn(
        config: ConsensusConfig<DB>,
        network: PrimaryNetworkHandle,
        consensus_bus: ConsensusBus,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
        task_manager: &TaskManager,
    ) {
        let authority_id = config.authority_id();
        let committee = config.committee().clone();
        let certificate_store = config.node_storage().clone();
        let rx_shutdown = config.shutdown().subscribe();
        let state = Arc::new(CertificateFetcherState {
            authority_id,
            network,
            rayls_consensus_state_sync,
            metrics: consensus_bus.primary_metrics().node_metrics.clone(),
        });
        let max_rpc_message_size = config.network_config().libp2p_config().max_rpc_message_size;

        task_manager.spawn_critical_task(
            "certificate fetcher task",
            monitored_future!(
                async move {
                    Self {
                        state,
                        committee,
                        certificate_store,
                        consensus_bus,
                        rx_shutdown,
                        targets: BTreeMap::new(),
                        fetch_certificates_task: JoinSet::new(),
                        max_rpc_message_size,
                        consecutive_failures: 0,
                        last_fetch_attempt: None,
                        epoch_mismatch_until: None,
                    }
                    .run()
                    .await
                },
                "CertificateFetcherTask"
            ),
        );
    }

    async fn run(&mut self) {
        let cb_clone = self.consensus_bus.clone();
        let mut rx_certificate_fetcher = cb_clone.certificate_fetcher().subscribe();

        // NOTE: probe at boot so a rejoining CvvActive node fetches live parents
        // without waiting for CATCHUP_PROBE_INTERVAL.
        info!(target: "primary::cert_fetcher", "warm-start probe on cert_fetcher boot");
        self.probe().await;

        loop {
            let probe_interval = self.probe_interval();
            tokio::select! {
                Some(command) = rx_certificate_fetcher.recv() => {
                    let certificate = match command {
                        CertificateFetcherCommand::Ancestors(certificate) => certificate,
                        CertificateFetcherCommand::Kick => {
                            // Kick start a fetch task if there is no other task running.
                            if self.fetch_certificates_task.is_empty() {
                                info!(
                                    target: "primary::cert_fetcher",
                                    targets = self.targets.len(),
                                    "GC kick received, attempting kickstart"
                                );
                                self.kickstart().await;
                            } else {
                                info!(
                                    target: "primary::cert_fetcher",
                                    fetch_tasks = self.fetch_certificates_task.len(),
                                    "GC kick received but fetch task already running"
                                );
                            }
                            continue;
                        }
                    };
                    let header = &certificate.header();
                    if header.epoch() != self.committee.epoch() {
                        continue;
                    }
                    // Unnecessary to validate the header and certificate further, since it has
                    // already been validated.

                    if let Some(r) = self.targets.get(header.author()) {
                        if header.round() <= *r {
                            // Ignore fetch request when we already need to sync to a later
                            // certificate from the same authority. Although this certificate may
                            // not be the parent of the later certificate, this should be ok
                            // because eventually a child of this certificate will miss parents and
                            // get inserted into the targets.
                            //
                            // Basically, it is ok to stop fetching without this certificate.
                            // If this certificate becomes a parent of other certificates, another
                            // fetch will be triggered eventually because of missing certificates.
                            continue;
                        }
                    }

                    // The header should have been verified as part of the certificate.
                    match self.certificate_store.last_round_number(header.author()) {
                        Ok(r) => {
                            if header.round() <= r.unwrap_or(0) {
                                // Ignore fetch request. Possibly the certificate was processed
                                // while the message is in the queue.
                                continue;
                            }
                            // Otherwise, continue to update fetch targets.
                        }
                        Err(e) => {
                            // If this happens, it is most likely due to serialization error.
                            error!("Failed to read latest round for {}: {}", header.author(), e);
                            continue;
                        }
                    };

                    // Update the target rounds for the authority.
                    self.targets.insert(header.author().clone(), header.round());

                    // Kick start a fetch task if there is no other task running.
                    if self.fetch_certificates_task.is_empty() {
                        self.kickstart().await;
                    }
                },
                Some(result) = self.fetch_certificates_task.join_next(), if !self.fetch_certificates_task.is_empty() => {
                    match result {
                        Ok(outcome) => {
                            match outcome {
                                FetchOutcome::Success => {
                                    self.consecutive_failures = 0;
                                    self.epoch_mismatch_until = None;
                                }
                                FetchOutcome::HardFailure => {
                                    self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                                }
                                FetchOutcome::PartialProgress => {
                                    // not a real failure - don't count toward stale-session fallback
                                }
                                FetchOutcome::EpochMismatch => {
                                    // suppress probes while peers return future-epoch certs;
                                    self.epoch_mismatch_until = Some(Instant::now() + EPOCH_MISMATCH_BACKOFF);
                                    info!(
                                        target: "primary::cert_fetcher",
                                        backoff_secs = EPOCH_MISMATCH_BACKOFF.as_secs(),
                                        "epoch mismatch backoff - suppressing probes"
                                    );
                                }
                            }
                        },
                        Err(e) => {
                            if e.is_cancelled() {
                                // avoid crashing on ungraceful shutdown
                            } else if e.is_panic() {
                                // propagate panics.
                                std::panic::resume_unwind(e.into_panic());
                            } else {
                                panic!("fetch certificates task failed: {e}");
                            }
                        },
                    };

                    // Kick start another fetch task after the previous one terminates.
                    // If all targets have been fetched, the new task will clean up the targets and exit.
                    if self.fetch_certificates_task.is_empty() {
                        self.kickstart().await;
                    }
                },
                // periodic probe when idle; interval adapts per probe_interval()
                _ = sleep(probe_interval), if self.fetch_certificates_task.is_empty() => {
                    info!(
                        target: "primary::cert_fetcher",
                        "periodic probe to discover missing certificates"
                    );
                    self.probe().await;
                },
                _ = &self.rx_shutdown => {
                    return
                }
            }
        }
    }

    // Starts a task to fetch missing certificates from other primaries.
    // A call to kickstart() can be triggered by a certificate with missing parents or the end of a
    // fetch task. Each iteration of kickstart() updates the target rounds, and iterations will
    // continue until there are no more target rounds to catch up to.
    /// Kickstart a fetch for a queued target; no-op when synced.
    #[allow(clippy::mutable_key_type)]
    async fn kickstart(&mut self) {
        let is_active = self.consensus_bus.node_mode().borrow().is_active_cvv();
        if is_active && self.targets.is_empty() {
            self.consecutive_failures = 0;
            info!(target: "primary::cert_fetcher", gc_round = self.gc_round(), "targets empty and active - nothing to fetch");
            return;
        }
        self.probe().await;
    }

    /// Probe peers for missing certificates, even with no queued targets.
    async fn probe(&mut self) {
        // epoch-mismatch suppression: all peers are on a future epoch so
        // fetching is futile until our epoch advances. sleep the remaining
        // backoff rather than spinning.
        if let Some(until) = self.epoch_mismatch_until {
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining > Duration::ZERO {
                info!(
                    target: "primary::cert_fetcher",
                    remaining_secs = remaining.as_secs(),
                    "epoch mismatch backoff active - sleeping before next probe"
                );
                sleep(remaining).await;
                self.epoch_mismatch_until = None;
            }
        }

        // NOTE: skip backoff in CvvInactive mode for fast catch-up
        let is_active = self.consensus_bus.node_mode().borrow().is_active_cvv();

        // exponential backoff on consecutive failures (1.5x multiplier)
        let backoff = self.calculate_backoff();
        if backoff > Duration::ZERO {
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .certificate_fetcher_backoff_ms
                .set(backoff.as_millis() as i64);
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .certificate_fetcher_consecutive_failures
                .set(self.consecutive_failures as i64);

            // only enforce backoff in CvvActive mode
            if is_active {
                if let Some(last_attempt) = self.last_fetch_attempt {
                    let elapsed = last_attempt.elapsed();
                    if elapsed < backoff {
                        trace!(
                            target: "primary::cert_fetcher",
                            failures = self.consecutive_failures,
                            "Backoff active, skipping fetch. Wait {:?} more",
                            backoff.saturating_sub(elapsed)
                        );
                        return;
                    }
                }
            } else {
                trace!(
                    target: "primary::cert_fetcher",
                    failures = self.consecutive_failures,
                    "Skipping backoff - node in catch-up mode (CvvInactive)"
                );
            }
        }
        self.last_fetch_attempt = Some(Instant::now());

        // Skip fetching certificates at or below the gc round.
        let gc_round = self.gc_round();
        // Skip fetching certificates that already exist locally.
        let mut written_rounds = BTreeMap::<AuthorityIdentifier, BTreeSet<Round>>::new();
        for authority in self.committee.authorities() {
            // Initialize written_rounds for all authorities, because the handler only sends back
            // certificates for the set of authorities here.
            written_rounds.insert(authority.id(), BTreeSet::new());
        }
        // NOTE: origins_after_round() is inclusive.
        match self.certificate_store.origins_after_round(gc_round + 1) {
            Ok(origins) => {
                for (round, origins) in origins {
                    for origin in origins {
                        written_rounds.entry(origin).or_default().insert(round);
                    }
                }
            }
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "failed to read from certificate store");
                return;
            }
        };

        // stale-session fallback: clear skip_rounds so the peer sends everything
        // above gc_round, replacing stale cert store entries from a prior session.
        // use a lower threshold in catch-up mode where speed matters more.
        let is_active = self.consensus_bus.node_mode().borrow().is_active_cvv();
        let stale_threshold = if is_active {
            STALE_SKIP_ROUNDS_FALLBACK_THRESHOLD
        } else {
            STALE_SKIP_ROUNDS_CATCHUP_THRESHOLD
        };
        if self.consecutive_failures >= stale_threshold {
            warn!(
                target: "primary::cert_fetcher",
                failures = self.consecutive_failures,
                gc_round,
                "clearing skip_rounds - stale session fallback after repeated failures"
            );
            for rounds in written_rounds.values_mut() {
                rounds.clear();
            }
        }

        self.targets.retain(|origin, target_round| {
            let last_written_round = written_rounds
                .get(origin)
                .map_or(gc_round, |rounds| rounds.last().unwrap_or(&gc_round).to_owned());
            // Drop sync target when cert store already has an equal or higher round for the origin.
            // This applies GC to targets as well.
            //
            // NOTE: even if the store actually does not have target_round for the origin,
            // it is ok to stop fetching without this certificate.
            // If this certificate becomes a parent of other certificates, another
            // fetch will be triggered eventually because of missing certificates.
            last_written_round < *target_round
        });
        if self.targets.is_empty() {
            info!(
                target: "primary::cert_fetcher",
                gc_round,
                is_active,
                "targets empty - probing peers for missing certificates"
            );
        }

        let state = self.state.clone();
        let committee = self.committee.clone();
        let max_response_size = self.max_rpc_message_size;

        debug!(
            target: "primary::cert_fetcher",
            max_target = self.targets.values().max().unwrap_or(&0),
            gc_round,
            is_active,
            "starting certificate fetch from peers"
        );

        // in catch-up mode, query all peers and merge responses to get the
        // most complete cert set (different peers may hold different subsets)
        let merge_all_peers = !is_active;

        self.fetch_certificates_task.spawn(monitored_future!(
            Self::run_fetch_and_report(
                state,
                committee,
                gc_round,
                written_rounds,
                max_response_size,
                merge_all_peers
            ),
            "CertificatesFetching"
        ));
    }

    /// Runs the fetch task and reports outcome for backoff handling.
    async fn run_fetch_and_report(
        state: Arc<CertificateFetcherState<DB>>,
        committee: Committee,
        gc_round: Round,
        written_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
        max_response_size: usize,
        merge_all_peers: bool,
    ) -> FetchOutcome {
        let _scope = monitored_scope("CertificatesFetching");
        state.metrics.certificate_fetcher_inflight_fetch.inc();

        let now = Instant::now();
        let outcome = match run_fetch_task(
            state.clone(),
            committee,
            gc_round,
            written_rounds,
            max_response_size,
            merge_all_peers,
        )
        .await
        {
            Ok(_) => {
                debug!(target: "primary::cert_fetcher",
                    "Finished task to fetch certificates successfully, elapsed = {}s",
                    now.elapsed().as_secs_f64()
                );
                FetchOutcome::Success
            }
            Err(CertManagerError::Pending(_)) => {
                info!(
                    target: "primary::cert_fetcher",
                    "Certificates pending parent acceptance - partial progress, elapsed = {}s",
                    now.elapsed().as_secs_f64()
                );
                FetchOutcome::PartialProgress
            }
            Err(CertManagerError::FutureEpoch { ours, theirs, count }) => {
                warn!(
                    target: "primary::cert_fetcher",
                    ours,
                    theirs,
                    count,
                    "all fetched certificates from non-matching epoch - \
                     waiting for epoch transition via forward streamer"
                );
                FetchOutcome::EpochMismatch
            }
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "Error from fetch certificates task");
                FetchOutcome::HardFailure
            }
        };

        state.metrics.certificate_fetcher_inflight_fetch.dec();
        outcome
    }

    fn gc_round(&self) -> Round {
        *self.consensus_bus.gc_round_updates().borrow()
    }

    /// Return probe interval: 10s when catching up, 60s when synced.
    fn probe_interval(&self) -> Duration {
        let is_active = self.consensus_bus.node_mode().borrow().is_active_cvv();
        if !is_active || !self.targets.is_empty() {
            CATCHUP_PROBE_INTERVAL
        } else {
            IDLE_PROBE_INTERVAL
        }
    }

    /// Calculate exponential backoff (1.5x multiplier, 100ms base, 60s cap).
    fn calculate_backoff(&self) -> Duration {
        const BASE_TIMEOUT: Duration = Duration::from_millis(100);
        const MAX_TIMEOUT: Duration = Duration::from_secs(60);

        if self.consecutive_failures == 0 {
            return Duration::ZERO;
        }

        let mut timeout = BASE_TIMEOUT;
        for _ in 0..self.consecutive_failures.min(20) {
            timeout = timeout + timeout / 2; // 1.5x multiplier
        }
        timeout.min(MAX_TIMEOUT)
    }
}

#[allow(clippy::mutable_key_type)]
#[instrument(level = "debug", skip_all)]
async fn run_fetch_task<DB: Database>(
    state: Arc<CertificateFetcherState<DB>>,
    committee: Committee,
    gc_round: Round,
    written_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    max_response_size: usize,
    merge_all_peers: bool,
) -> CertManagerResult<()> {
    // tighten lower bound past contiguous prefix to reduce server scan work;
    // prune written_rounds below the new floor to avoid delta underflow.
    let effective_lower_bound = tighten_lower_bound(gc_round, &written_rounds);
    let pruned_written_rounds: BTreeMap<_, _> = written_rounds
        .into_iter()
        .map(|(authority, rounds)| {
            let rounds = rounds.into_iter().filter(|r| *r > effective_lower_bound).collect();
            (authority, rounds)
        })
        .collect();

    // Send request to fetch certificates.
    let request = MissingCertificatesRequest::default()
        .set_bounds(effective_lower_bound, pruned_written_rounds)
        .map_err(|e| CertManagerError::RequestBounds(e.to_string()))?
        .set_max_response_size(max_response_size);
    let Some(response) = fetch_certificates_helper(
        state.authority_id.as_ref(),
        state.network.clone(),
        &committee,
        request,
        merge_all_peers,
    )
    .await
    else {
        error!(target: "primary::cert_fetcher", "error awaiting fetch_certificates_helper");
        return Err(CertManagerError::NoCertificateFetched);
    };

    // filter out certificates from future epochs before verification.
    // mirrors Sui's Anemo epoch filter which rejects cross-epoch responses
    // at the network layer. without this, the cert validator aborts the
    // entire batch on the first epoch-mismatched cert.
    let total = response.certificates.len();
    let certificates = filter_future_epoch_certs(response.certificates, committee.epoch())?;
    let response = FetchCertificatesResponse { certificates };

    // Process and store fetched certificates.
    let num_certs_fetched = response.certificates.len();
    info!(target: "primary::cert_fetcher", "processing {num_certs_fetched} fetched certificates (filtered from {total})");
    process_certificates_helper(response, &state.rayls_consensus_state_sync, state.metrics.clone())
        .await?;
    state.metrics.certificate_fetcher_num_certificates_processed.inc_by(num_certs_fetched as u64);

    debug!(target: "primary::cert_fetcher", "successfully processed {num_certs_fetched} certificates");
    Ok(())
}

/// Tighten `exclusive_lower_bound` by finding the min contiguous prefix length
/// above `gc_round` across all authorities. Rounds below the prefix are gap-free
/// and need not be requested.
fn tighten_lower_bound(
    gc_round: Round,
    written_rounds: &BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
) -> Round {
    if written_rounds.is_empty() {
        return gc_round;
    }
    written_rounds
        .values()
        .map(|rounds| {
            let mut expected = gc_round + 1;
            for &r in rounds.iter() {
                if r == expected {
                    expected = r + 1;
                } else {
                    break;
                }
            }
            // `expected - 1` is the last contiguous round this authority has.
            expected.saturating_sub(1)
        })
        .min()
        .unwrap_or(gc_round)
        .max(gc_round)
}

/// Filter certs to `our_epoch`. Return `Err(FutureEpoch)` if all are foreign.
fn filter_future_epoch_certs(
    certs: Vec<Certificate>,
    our_epoch: Epoch,
) -> CertManagerResult<Vec<Certificate>> {
    let total = certs.len();
    let mut current_epoch = Vec::with_capacity(total);
    let mut max_peer_epoch: Epoch = 0;
    let mut filtered_count = 0usize;
    for cert in certs {
        if cert.epoch() == our_epoch {
            current_epoch.push(cert);
        } else {
            max_peer_epoch = max_peer_epoch.max(cert.epoch());
            filtered_count += 1;
        }
    }
    if filtered_count > 0 {
        warn!(
            target: "primary::cert_fetcher",
            our_epoch,
            filtered_count,
            max_peer_epoch,
            remaining = current_epoch.len(),
            "filtered out certificates from non-matching epochs"
        );
    }
    if current_epoch.is_empty() {
        return Err(CertManagerError::FutureEpoch {
            ours: our_epoch,
            theirs: max_peer_epoch,
            count: total,
        });
    }
    Ok(current_epoch)
}

/// Fetch certificates from peers. In single-peer mode (CvvActive), return
/// first non-empty response. In multi-peer mode (catch-up), query all peers
/// and merge by digest dedup.
#[instrument(level = "debug", skip_all)]
async fn fetch_certificates_helper(
    name: Option<&AuthorityIdentifier>,
    network: PrimaryNetworkHandle,
    committee: &Committee,
    request: MissingCertificatesRequest,
    merge_all_peers: bool,
) -> Option<FetchCertificatesResponse> {
    let _scope = monitored_scope("FetchingCertificatesFromPeers");
    trace!(target: "primary::cert_fetcher", "Start sending fetch certificates requests");
    let request_interval = PARALLEL_FETCH_REQUEST_INTERVAL_SECS;
    let mut peers: Vec<BlsPublicKey> =
        committee.others_primaries_by_id(name).into_iter().map(|(_, key)| key).collect();
    peers.shuffle(&mut ThreadRng::default());
    // in multi-peer mode all requests fire simultaneously, so a single
    // round-trip window suffices. staggered mode needs per-peer intervals.
    let fetch_timeout = if merge_all_peers {
        PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
    } else {
        PARALLEL_FETCH_REQUEST_INTERVAL_SECS
            * peers.len().try_into().expect("usize into secs duration")
            + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
    };
    let fetch_callback = async move {
        debug!(target: "primary::cert_fetcher", "Starting to fetch certificates");

        if merge_all_peers {
            // fire requests to ALL peers immediately in parallel
            let mut multi_fut = FuturesUnordered::new();
            for peer in &peers {
                let request_clone = request.clone();
                let network_clone = network.clone();
                let peer = *peer;
                multi_fut.push(async move {
                    debug!(target: "primary::cert_fetcher", "sending fetch request to {peer}");
                    let result = network_clone.fetch_certificates(peer, request_clone).await;
                    match &result {
                        Ok(certs) => info!(target: "primary::cert_fetcher", "peer {peer} returned {} certificates", certs.len()),
                        Err(e) => warn!(target: "primary::cert_fetcher", "peer {peer} fetch error: {e}"),
                    }
                    result
                });
            }

            // collect all responses and merge by digest
            let mut seen = std::collections::BTreeSet::new();
            let mut merged = Vec::new();
            while let Some(result) = multi_fut.next().await {
                if let Ok(certs) = result {
                    for cert in certs {
                        let digest = cert.digest();
                        if seen.insert(digest) {
                            merged.push(cert);
                        }
                    }
                }
            }
            if merged.is_empty() {
                warn!(target: "primary::cert_fetcher", "all peers exhausted (multi-peer), no certificates fetched");
                sleep(request_interval).await;
                return None;
            }
            // sort by round for causal processing order
            merged.sort_by_key(|c| c.round());
            info!(
                target: "primary::cert_fetcher",
                total = merged.len(),
                unique_rounds = merged.iter().map(|c| c.round()).collect::<std::collections::BTreeSet<_>>().len(),
                "merged certificates from all peers"
            );
            return Some(FetchCertificatesResponse { certificates: merged });
        }

        // single-peer mode: return first non-empty response (original behavior)
        let mut fut = FuturesUnordered::new();
        loop {
            if let Some(peer) = peers.pop() {
                let request_clone = request.clone();
                let network_clone = network.clone();
                fut.push(monitored_future!(async move {
                    debug!(target: "primary::cert_fetcher", "sending fetch request to {peer}");
                    let result = network_clone.fetch_certificates(peer, request_clone).await;
                    match &result {
                        Ok(certificates) => {
                            info!(target: "primary::cert_fetcher", "peer {peer} returned {} certificates", certificates.len());
                        }
                        Err(e) => {
                            warn!(target: "primary::cert_fetcher", "peer {peer} fetch error: {e}");
                        }
                    }
                    result
                }));
            }
            let interval = sleep(request_interval);
            tokio::pin!(interval);
            tokio::select! {
                res = fut.next() => match res {
                    Some(Ok(certificates)) => {
                        if certificates.is_empty() {
                            info!(target: "primary::cert_fetcher", "peer returned empty certificate list, trying next");
                            continue;
                        }
                        info!(target: "primary::cert_fetcher", "received {} certificates from peer", certificates.len());
                        return Some(FetchCertificatesResponse { certificates });
                    }
                    Some(Err(e)) => {
                        warn!(target: "primary::cert_fetcher", "Failed to fetch certificates: {e}");
                        // Issue request to another primary immediately.
                        continue;
                    }
                    None => {
                        warn!(target: "primary::cert_fetcher", "all peers exhausted, no certificates fetched");
                        // Last or all requests to peers may have failed immediately, so wait
                        // before returning to avoid retrying fetching immediately.
                        sleep(request_interval).await;
                        return None;
                    }
                },
                _ = &mut interval => {
                    // No response received in the last interval. Send out another fetch request
                    // in parallel if there is a peer that has not been sent to.
                }
            }
        }
    };
    timeout(fetch_timeout, fetch_callback).await.unwrap_or_else(|e| {
        debug!(target: "primary::cert_fetcher", "Timed out fetching certificates: {e}");
        None
    })
}

#[instrument(level = "debug", skip_all)]
async fn process_certificates_helper<DB: Database>(
    response: FetchCertificatesResponse,
    rayls_consensus_state_sync: &StateSynchronizer<DB>,
    _metrics: Arc<PrimaryMetrics>,
) -> CertManagerResult<()> {
    trace!(target: "primary::cert_fetcher", "Start sending fetched certificates to processing");

    // We should not be getting mixed versions of certificates from a
    // validator, so any individual certificate with mismatched versions
    // should cancel processing for the entire batch of fetched certificates.
    let certificates = response
        .certificates
        .into_iter()
        .map(|cert| {
            let res = validate_received_certificate(cert).inspect_err(|err| {
                error!(target: "primary::cert_fetcher", "fetched certficate processing error: {err}");
            });
            Ok(res?)
        })
        .collect::<CertManagerResult<Vec<Certificate>>>()?;

    // In PrimaryReceiverHandler, certificates already in storage are ignored.
    // The check is unnecessary here, because there is no concurrent processing of older
    // certificates. For byzantine failures, the check will not be effective anyway.
    let _scope = monitored_scope("ProcessingFetchedCertificates");

    rayls_consensus_state_sync.process_fetched_certificates_in_parallel(certificates).await?;

    trace!(target: "primary::cert_fetcher", "Fetched certificates have been processed");

    Ok(())
}
