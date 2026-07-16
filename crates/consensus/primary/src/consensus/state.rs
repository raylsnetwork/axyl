//! The state of consensus

use crate::{
    consensus::{bullshark::Bullshark, utils::gc_round, ConsensusError, ConsensusMetrics},
    ConsensusBus, NodeMode,
};
use consensus_metrics::monitored_future;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{CertificateStore, ConsensusStore, ReadTimeout};
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, CertificateDigest, CommittedSubDag, Committee, Database,
    Epoch, Hash as _, Noticer, RaylsReceiver, RaylsSender, Round, TaskManager, Timestamp,
};
use std::{
    cmp::{max, Ordering},
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
    sync::Arc,
};
use tracing::{debug, info, instrument, warn};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod consensus_tests;

/// In-memory DAG; `BTreeMap` ensures deterministic iteration.
pub type Dag = BTreeMap<Round, BTreeMap<AuthorityIdentifier, (CertificateDigest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
#[derive(Debug)]
pub struct ConsensusState {
    /// The information about the last committed round and corresponding GC round.
    pub last_round: ConsensusRound,
    /// The chosen gc_depth
    pub gc_depth: Round,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    pub last_committed: HashMap<AuthorityIdentifier, Round>,
    /// The last committed sub dag. If value is None, it means that we haven't committed any sub
    /// dag yet.
    pub last_committed_sub_dag: Option<CommittedSubDag>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything
    /// older must be regularly cleaned up through the function `update`.
    pub dag: Dag,
    /// Metrics handler
    pub metrics: Arc<ConsensusMetrics>,
}

impl ConsensusState {
    /// Create a new empty ConsensusState.  Used for tests.
    pub fn new(metrics: Arc<ConsensusMetrics>, gc_depth: Round) -> Self {
        Self {
            last_round: ConsensusRound::default(),
            gc_depth,
            last_committed: Default::default(),
            dag: Default::default(),
            last_committed_sub_dag: None,
            metrics,
        }
    }

    fn new_from_store<DB: Database>(
        metrics: Arc<ConsensusMetrics>,
        last_committed_round: Round,
        gc_depth: Round,
        recovered_last_committed: HashMap<AuthorityIdentifier, Round>,
        latest_sub_dag: Option<CommittedSubDag>,
        cert_store: DB,
        current_epoch: Epoch,
    ) -> Self {
        let last_round = ConsensusRound::new_with_gc_depth(last_committed_round, gc_depth);

        let dag = Self::construct_dag_from_cert_store(
            &cert_store,
            &recovered_last_committed,
            last_round.gc_round,
            current_epoch,
        )
        .expect("error when recovering DAG from store");
        metrics.recovered_consensus_state.inc();

        let last_committed_sub_dag = latest_sub_dag.clone();

        Self {
            gc_depth,
            last_round,
            last_committed: recovered_last_committed,
            last_committed_sub_dag,
            dag,
            metrics,
        }
    }

    #[instrument(level = "info", skip_all)]
    fn construct_dag_from_cert_store<DB: CertificateStore>(
        cert_store: &DB,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
        current_epoch: Epoch,
    ) -> Result<Dag, ConsensusError> {
        let mut dag: Dag = BTreeMap::new();

        info!("Recreating dag from last GC round: {}", gc_round);

        // Ascending round order + check_parents=true makes orphans self-reject
        // at insert time, yielding a sound-by-construction DAG.
        // Exempt from the read-txn timeout: a transient I/O stall must not turn this bounded
        // recovery scan into a panic (which would kill the node mid-write and risk DB corruption).
        let mut certificates =
            cert_store.after_round(gc_round + 1, ReadTimeout::Exempt).expect("database available");
        certificates.sort_by_key(|c| c.round());

        let mut num_certs = 0;
        let mut num_skipped_epoch = 0usize;
        let mut num_orphans = 0usize;
        for cert in &certificates {
            // prior-epoch parents would corrupt leader election
            if cert.epoch() != current_epoch {
                num_skipped_epoch += 1;
                continue;
            }
            match Self::try_insert_in_dag(&mut dag, last_committed, gc_round, cert, true) {
                Ok(true) => {
                    num_certs += 1;
                }
                Ok(false) => {}
                Err(ConsensusError::MissingParent(_, _))
                | Err(ConsensusError::MissingParentRound(_)) => {
                    // sparse cert store on rejoin: ancestor not persisted. Skip.
                    num_orphans += 1;
                }
                Err(e) => return Err(e),
            }
        }

        let total_dag_entries: usize = dag.values().map(|round_map| round_map.len()).sum();
        info!(
            "Dag restored: {} certs above last_committed, {} total entries across {} rounds ({} orphans skipped, {} prior-epoch skipped)",
            num_certs,
            total_dag_entries,
            dag.len(),
            num_orphans,
            num_skipped_epoch,
        );

        Ok(dag)
    }

    /// Returns true if certificate is inserted in the dag.
    pub fn try_insert(&mut self, certificate: &Certificate) -> Result<bool, ConsensusError> {
        Self::try_insert_in_dag(
            &mut self.dag,
            &self.last_committed,
            self.last_round.gc_round,
            certificate,
            true,
        )
    }

    /// Returns true if certificate is inserted in the dag.
    fn try_insert_in_dag(
        dag: &mut Dag,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
        certificate: &Certificate,
        check_parents: bool,
    ) -> Result<bool, ConsensusError> {
        if certificate.round() <= gc_round {
            debug!(target: "rayls::consensus_state",
                "Ignoring certificate {:?} as it is at or before gc round {}",
                certificate, gc_round
            );
            return Ok(false);
        }
        if check_parents {
            Self::check_parents(certificate, dag, gc_round)?;
        }

        // Always insert the certificate even if it is below last committed round of its origin,
        // to allow verifying parent existence.
        if let Some((_, existing_certificate)) = dag
            .entry(certificate.round())
            .or_default()
            .insert(certificate.origin().clone(), (certificate.digest(), certificate.clone()))
        {
            // we want to error only if we try to insert a different certificate in the dag
            if existing_certificate.digest() != certificate.digest() {
                return Err(ConsensusError::CertificateEquivocation(
                    Box::new(certificate.clone()),
                    Box::new(existing_certificate),
                ));
            }
        }

        Ok(certificate.round()
            > last_committed.get(certificate.origin()).cloned().unwrap_or_default())
    }

    /// Update and clean up internal state after committing a certificate.
    pub fn update(&mut self, certificate: &Certificate) {
        self.last_committed
            .entry(certificate.origin().clone())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());
        self.last_round = self.last_round.update(certificate.round(), self.gc_depth);

        self.metrics
            .last_committed_round
            .with_label_values(&[])
            .set(self.last_round.committed_round as i64);
        let elapsed = certificate.created_at().elapsed().as_secs_f64();
        self.metrics
            .certificate_commit_latency
            .observe(certificate.created_at().elapsed().as_secs_f64());

        // NOTE: This log entry is used to compute performance.
        tracing::debug!(target: "rayls::consensus_state",
            "Certificate {:?} took {} seconds to be committed at round {}",
            certificate.digest(),
            elapsed,
            certificate.round(),
        );

        // Purge all certificates past the gc depth.
        self.dag.retain(|r, _| *r > self.last_round.gc_round);
    }

    /// Rayls: Prune the DAG based on highest seen round, returning rounds pruned.
    pub fn proactive_gc(&mut self, highest_seen_round: Round) -> usize {
        if highest_seen_round <= self.gc_depth {
            return 0;
        }

        // Calculate GC round based on highest seen, not committed
        let proactive_gc_round = highest_seen_round.saturating_sub(self.gc_depth);

        // Only prune if proactive GC would remove more than current GC
        if proactive_gc_round <= self.last_round.gc_round {
            return 0;
        }

        let old_len = self.dag.len();
        self.dag.retain(|r, _| *r > proactive_gc_round);
        let pruned = old_len - self.dag.len();

        if pruned > 0 {
            tracing::debug!(
                target: "rayls::consensus_state",
                highest_seen_round,
                proactive_gc_round,
                committed_gc_round = self.last_round.gc_round,
                pruned,
                remaining = self.dag.len(),
                "Proactive DAG pruning"
            );
        }

        pruned
    }

    // Checks that the provided certificate's parents exist return an error if they do not.
    fn check_parents(
        certificate: &Certificate,
        dag: &Dag,
        gc_round: Round,
    ) -> Result<(), ConsensusError> {
        let round = certificate.round();
        // Skip checking parents if they are GC'ed.
        // Also not checking genesis parents for simplicity.
        if round <= gc_round + 1 {
            return Ok(());
        }
        if let Some(round_table) = dag.get(&(round - 1)) {
            let store_parents: BTreeSet<&CertificateDigest> =
                round_table.iter().map(|(_, (digest, _))| digest).collect();
            for parent_digest in certificate.header().parents() {
                if !store_parents.contains(parent_digest) {
                    return Err(ConsensusError::MissingParent(
                        *parent_digest,
                        Box::new(certificate.clone()),
                    ));
                }
            }
        } else {
            tracing::error!(target: "rayls::consensus_state", "Parent round not found in DAG for {certificate:?}!");
            return Err(ConsensusError::MissingParentRound(Box::new(certificate.clone())));
        }
        Ok(())
    }
}

/// Holds information about a committed round in consensus.
///
/// When a certificate gets committed then
/// the corresponding certificate's round is considered a "committed" round. It bears both the
/// committed round and the corresponding garbage collection round.
#[derive(Debug, Default, Copy, Clone)]
pub struct ConsensusRound {
    pub committed_round: Round,
    pub gc_round: Round,
}

impl ConsensusRound {
    pub fn new(committed_round: Round, gc_round: Round) -> Self {
        Self { committed_round, gc_round }
    }

    pub fn new_with_gc_depth(committed_round: Round, gc_depth: Round) -> Self {
        let gc_round = gc_round(committed_round, gc_depth);

        Self { committed_round, gc_round }
    }

    /// Calculates the latest CommittedRound by providing a new committed round and the gc_depth.
    /// The method will compare against the existing committed round and return
    /// the updated instance.
    fn update(&self, new_committed_round: Round, gc_depth: Round) -> Self {
        let last_committed_round = max(self.committed_round, new_committed_round);
        let last_gc_round = gc_round(last_committed_round, gc_depth);

        ConsensusRound { committed_round: last_committed_round, gc_round: last_gc_round }
    }
}

/// Rayls Network consensus.
#[derive(Debug)]
pub struct Consensus<DB> {
    /// The committee information.
    committee: Committee,
    /// The chanell "bus" for consensus (container for consesus channel and watches).
    consensus_bus: ConsensusBus,

    /// Consensus config for the app, used to shutdown an epoch for mode switching.
    consensus_config: ConsensusConfig<DB>,

    /// Receiver for shutdown.
    rx_shutdown: Noticer,

    /// The consensus protocol to run.
    protocol: Bullshark,

    /// Metrics handler
    metrics: Arc<ConsensusMetrics>,

    /// Inner state
    state: ConsensusState,

    /// Are we an active CVV?
    /// An active CVV is participating in consensus (not catching up or following as an NVV).
    active: bool,
}

impl<DB: Database> Consensus<DB> {
    pub fn spawn(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        protocol: Bullshark,
        task_manager: &TaskManager,
    ) {
        let metrics = consensus_bus.consensus_metrics();
        let rx_shutdown = consensus_config.shutdown().subscribe();
        // The consensus state (everything else is immutable).
        let current_epoch = consensus_config.epoch();
        let recovered_last_committed =
            consensus_config.node_storage().read_last_committed(current_epoch);

        debug!(target: "epoch-manager", ?recovered_last_committed, "recovered last committed for epoch {}", current_epoch);

        // use primed round from prime_consensus, falling back to DB calculation
        let primed_round = *consensus_bus.committed_round_updates().borrow();
        let db_round = recovered_last_committed.values().copied().max().unwrap_or(0);
        let last_committed_round = primed_round.max(db_round);

        debug!(
            target: "epoch-manager",
            primed_round,
            db_round,
            last_committed_round,
            "using max of primed and db rounds for consensus state"
        );

        // ignore previous epochs
        let latest_sub_dag = consensus_config
            .node_storage()
            .get_latest_sub_dag()
            .filter(|subdag| subdag.leader_epoch() >= current_epoch);

        debug!(target: "epoch-manager", ?latest_sub_dag, "recovered latest subdag:");
        if let Some(sub_dag) = &latest_sub_dag {
            assert!(
                last_committed_round >= sub_dag.leader_round(),
                "last_committed_round {} is behind subdag leader round {}",
                last_committed_round,
                sub_dag.leader_round(),
            );
            if last_committed_round != sub_dag.leader_round() {
                warn!(
                    target: "epoch-manager",
                    last_committed_round,
                    leader_round = sub_dag.leader_round(),
                    "last_committed_round exceeds subdag leader round, likely from follower certs in db"
                );
            }
        }

        // restore local dag
        let state = ConsensusState::new_from_store(
            metrics.clone(),
            last_committed_round,
            consensus_config.parameters().gc_depth,
            recovered_last_committed,
            latest_sub_dag,
            consensus_config.node_storage().clone(),
            current_epoch,
        );

        // only update if calculated round exceeds the primed value
        if state.last_round.committed_round > primed_round {
            consensus_bus.update_consensus_rounds(state.last_round);
        }

        let s = Self {
            committee: consensus_config.committee().clone(),
            consensus_bus: consensus_bus.clone(),
            consensus_config: consensus_config.clone(),
            rx_shutdown,
            protocol,
            metrics,
            state,
            active: false,
        };

        // Sound-by-construction reconstruction guarantees DAG consistency; live
        // certs referencing pruned ancestors are caught by the MissingParent[Round]
        // arm in `new_certificate`, which demotes to CvvInactive.
        if consensus_bus.node_mode().borrow().is_active_cvv() {
            task_manager.spawn_critical_result_task(
                "consensus",
                monitored_future!(s.run(), "Consensus", INFO),
            );
        } else {
            // drain so cert_manager is not blocked by backpressure when Bullshark is idle
            let mut rx = consensus_bus.new_certificates().subscribe();
            task_manager.spawn_task("drain new_certificates for non-active", async move {
                while rx.recv().await.is_some() {}
            });
        }
    }

    async fn run(mut self) -> Result<(), ConsensusError> {
        // Clone the bus or the borrow checker will yell at us...
        let bus_clone = self.consensus_bus.clone();
        let mut rx_new_certificates = bus_clone.new_certificates().subscribe();
        self.active = bus_clone.node_mode().borrow().is_active_cvv();

        // Track highest seen round for proactive GC
        let mut highest_seen_round: Round = 0;

        // Proactive GC interval - run every 30 seconds to catch stalled consensus
        let mut gc_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        // Listen to incoming certificates.
        loop {
            tokio::select! {
                _ = &self.rx_shutdown => {
                    return Ok(())
                }

                Some(certificate) = rx_new_certificates.recv() => {
                    highest_seen_round = highest_seen_round.max(certificate.round());
                    match self.new_certificate(certificate).await {
                        Ok(()) => {}
                        Err(ConsensusError::ShuttingDown) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                },

                _ = gc_interval.tick() => {
                    // Proactive DAG pruning to prevent unbounded growth during stalls
                    if highest_seen_round > 0 {
                        self.state.proactive_gc(highest_seen_round);
                    }
                }
            }
        }
    }

    /// Process a new certificate.
    async fn new_certificate(&mut self, certificate: Certificate) -> Result<(), ConsensusError> {
        match certificate.epoch().cmp(&self.committee.epoch()) {
            Ordering::Equal => {
                // we can proceed.
            }
            _ => {
                tracing::debug!(target: "rayls::consensus_state", "Already moved to the next epoch");
                return Ok(());
            }
        }
        // demote: local rebuild would diverge from peers' DAG
        let (outcome, committed_sub_dags) = match self
            .protocol
            .process_certificate(&mut self.state, certificate)
        {
            Ok(pair) => pair,
            Err(e @ ConsensusError::MissingParent(..))
            | Err(e @ ConsensusError::MissingParentRound(..)) => {
                // fetch_max keeps the round barrier monotonic across repeated demotes
                let failing_cert = match &e {
                    ConsensusError::MissingParent(_, c) | ConsensusError::MissingParentRound(c) => {
                        c.clone()
                    }
                    _ => unreachable!("arm is gated on MissingParent[Round]"),
                };
                let failing_round = failing_cert.round();
                let missing_digest =
                    if let ConsensusError::MissingParent(d, _) = &e { Some(*d) } else { None };
                let epoch = self.committee.epoch();
                self.consensus_bus.promotion_barrier().send_if_modified(|current| {
                    // monotonic within an epoch (fetch_max keeps the round climbing across
                    // repeated demotes); a prior-epoch barrier is stale, so always replace it.
                    let supersedes = current
                        .as_ref()
                        .is_none_or(|b| b.epoch != epoch || failing_round > b.round);
                    if supersedes {
                        *current = Some(crate::PromotionBarrier {
                            epoch,
                            round: failing_round,
                            digest: missing_digest,
                        });
                    }
                    supersedes
                });
                let _ = self.consensus_bus.certificate_fetcher().try_send(
                    crate::certificate_fetcher::CertificateFetcherCommand::Ancestors(Arc::new(
                        (*failing_cert).clone(),
                    )),
                );
                tracing::warn!(
                    target: "rayls::consensus_state",
                    ?e,
                    failing_round,
                    ?missing_digest,
                    "DAG behind live network; demoting to CvvInactive, promotion barrier raised",
                );
                self.consensus_bus.request_mode_transition(NodeMode::CvvInactive);
                self.consensus_config.shutdown().notify();
                return Err(ConsensusError::ShuttingDown);
            }
            Err(e) => return Err(e),
        };
        if self.active {
            // We extract a list of headers from this specific validator that
            // have been agreed upon, and signal this back to the narwhal sub-system
            // to be used to re-send batches that have not made it to a commit.
            let mut committed_certificates = Vec::new();

            // Each cert is tagged with whether its subdag reaches the epoch boundary: the
            // subscriber drops those post-boundary outputs, so the proposer must keep
            // (not clean) their batches for rescue. Computed here where both the subdag
            // commit_timestamp and the epoch_boundary are known; carried per-cert to
            // the proposer (no shared transition flag).
            let epoch_boundary = self.consensus_config.epoch_boundary();

            // Output the sequence in the right order.
            let csd_len = committed_sub_dags.len();
            for (i, committed_sub_dag) in committed_sub_dags.into_iter().enumerate() {
                // We need to make sure execution has caught up so we can verify we have not forked.
                // This will force the follow function to not outrun execution...  this is probably
                // fine. Also once we can follow gossiped consensus output this will not really be
                // an issue (except during initial catch up).
                let base_execution_block = committed_sub_dag.leader.header.latest_execution_block;
                if self.consensus_bus.wait_for_execution(base_execution_block).await.is_err() {
                    // This seems to be a bogus sub dag, we are out of sync...
                    tracing::error!(target: "rayls::consensus_state", "Got a bogus sub dag from bullshark, we are out of sync and probably can not recover!");
                    // route through mode_transition; direct writes skip shutdown
                    self.consensus_bus.request_mode_transition(NodeMode::CvvInactive);
                    self.consensus_config.shutdown().notify();
                    tracing::error!(target: "rayls::consensus_state", ?base_execution_block, ?outcome, "commit {i} of {csd_len} subdags");
                    return Ok(());
                }

                tracing::debug!(target: "rayls::consensus_state", "Commit in Sequence {:?}", committed_sub_dag.leader.nonce());

                let dropped = committed_sub_dag.reaches_epoch_boundary(epoch_boundary);

                for certificate in &committed_sub_dag.certificates {
                    committed_certificates.push((certificate.clone(), dropped));
                }

                // NOTE: The size of the sub-dag can be arbitrarily large (depending on the network
                // condition and Byzantine leaders).
                self.consensus_bus
                    .sequence()
                    .send(committed_sub_dag)
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;
            }

            if !committed_certificates.is_empty() {
                // Highest committed certificate round is the leader round / commit round
                // expected by primary.
                let leader_commit_round = committed_certificates
                    .iter()
                    .map(|(c, _)| c.round())
                    .max()
                    .expect("committed_certificates isn't empty");

                self.consensus_bus
                    .committed_certificates()
                    .send((leader_commit_round, committed_certificates))
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;

                assert_eq!(self.state.last_round.committed_round, leader_commit_round);

                self.consensus_bus.update_consensus_rounds(self.state.last_round);
            }

            self.metrics
                .consensus_dag_rounds
                .with_label_values(&[])
                .set(self.state.dag.len() as i64);
        }
        Ok(())
    }
}
