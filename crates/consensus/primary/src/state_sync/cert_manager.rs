//! Process standalone validated and verified certificates.
//!
//! This module is responsible for checking certificate parents, managing pending certificates, and
//! accepting certificates that become unlocked.

use super::{gc::GarbageCollector, pending_cert_manager::PendingCertificateManager, AtomicRound};
use crate::{
    aggregators::certificates::CertificatesAggregatorManager,
    certificate_fetcher::CertificateFetcherCommand,
    error::{CertManagerError, CertManagerResult, GarbageCollectorError},
    state_sync::cert_validator::certificate_source,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    tables::{CertificateDigestByOrigin, CertificateDigestByRound, Certificates},
    save_cert, CertificateStore,
};
use rayls_infrastructure_types::{
    error::{CertificateError, HeaderError},
    Certificate, CertificateDigest, Database, DbTxMut, Hash as _, Noticer, RaylsReceiver as _,
    RaylsSender as _, Table,
};
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

/// Pending count threshold for elevated warnings.
const PENDING_COUNT_WARNING_THRESHOLD: usize = 50;
/// Log a cascade-risk warning every N admissions above the threshold.
const PENDING_COUNT_WARNING_INTERVAL: usize = 50;
/// Interval between periodic stuck-state summary warnings.
const STUCK_STATE_SUMMARY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(test)]
#[path = "../tests/cert_manager_tests.rs"]
mod cert_manager_tests;

/// Process validated certificates.
///
/// Long-running task to manage pending certificate requests and accept verified certificates.
#[derive(Debug)]
pub(super) struct CertificateManager<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// State for pending certificate.
    pending: PendingCertificateManager,
    /// Collection of parents to advance the round.
    ///
    /// This is shared with the `GarbageCollector`.
    parents: CertificatesAggregatorManager,
    /// The task responsible for managing garbage collection.
    garbage_collector: GarbageCollector<DB>,
    /// Highest garbage collection round.
    ///
    /// This is managed by GarbageCollector and shared with CertificateValidator.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Certificates accepted since last reset. Gates cert_store_round signaling.
    certs_accepted: u32,
}

impl<DB> CertificateManager<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(super) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
    ) -> Self {
        let parents =
            CertificatesAggregatorManager::new(consensus_bus.clone(), config.parameters().gc_depth);
        let pending = PendingCertificateManager::new(consensus_bus.clone());
        let garbage_collector =
            GarbageCollector::new(config.clone(), consensus_bus.clone(), gc_round.clone());

        Self {
            consensus_bus,
            config,
            pending,
            parents,
            garbage_collector,
            gc_round,
            highest_processed_round,
            certs_accepted: 0,
        }
    }

    /// Process verified certificate.
    ///
    /// Returns an error if a certificate is unverified. This will accept certificate or mark it as
    /// pending if parents are missing.
    async fn process_verified_certificates(
        &mut self,
        mut certs: Vec<Certificate>,
        shutdown_rx: &Noticer,
    ) -> CertManagerResult<()> {
        // Process parents before children so fetched batches do not get
        // suspended on transiently-missing parents within the same batch.
        certs.sort_by_key(|c| c.round());

        // process entire collection of certificates
        //
        // these can be single, fetched from certificate fetcher or unlocked pending
        // if any are pending, return the pending error
        let mut result = Ok(());

        // collect results
        for cert in certs {
            let digest = cert.digest();

            // guarantee certificate is verified before storing in pending
            // NOTE: this is the only time this is checked
            if !cert.is_verified() {
                // stop processing certs if unverified
                return Err(CertManagerError::UnverifiedSignature(digest));
            }

            // check pending status
            if self.pending.is_pending(&digest) {
                // metrics
                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup_locked"])
                    .inc();

                warn!(
                    target: "primary::cert_manager",
                    round = cert.round(),
                    %digest,
                    pending_total = self.pending.num_pending(),
                    "certificate already pending from prior fetch"
                );

                // track error for caller that at least one cert is pending
                // and continue processing other certs
                result = Err(CertManagerError::Pending(digest));
                continue;
            }

            // parent-check gate uses real gc_round, not max(gc, committed): elevating it
            // would suppress the Ancestors targets cert_fetcher needs during catch-up
            let gc = self.gc_round.load();
            if cert.round() > gc + 1 {
                let missing_parents = self.get_missing_parents(&cert).await?;
                if !missing_parents.is_empty() {
                    warn!(
                        target: "primary::cert_manager",
                        round = cert.round(),
                        author = %cert.header().author(),
                        digest = %digest,
                        gc_round = gc,
                        missing_count = missing_parents.len(),
                        missing = ?missing_parents,
                        "certificate suspended - missing parents"
                    );
                    self.pending.insert_pending(cert, missing_parents)?;
                    // metrics
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_currently_suspended
                        .set(self.pending.num_pending() as i64);

                    // Cascade detection: warn when pending queue is growing large
                    let pending_count = self.pending.num_pending();
                    if pending_count > PENDING_COUNT_WARNING_THRESHOLD
                        && pending_count.is_multiple_of(PENDING_COUNT_WARNING_INTERVAL)
                    {
                        warn!(
                            target: "primary::cert_manager",
                            pending_count,
                            gc_round = gc,
                            "Pending certificate count elevated - potential cascade risk"
                        );
                    }

                    // track error for caller that at least one cert is pending
                    // and continue processing other certs
                    result = Err(CertManagerError::Pending(digest));
                    continue;
                }
            }

            // no missing parents - update pending state and
            let mut unlocked = self.pending.update_pending(cert.round(), digest)?;
            // append cert and process all certs in causal order
            unlocked.push_front(cert);
            self.accept_verified_certificates(unlocked, shutdown_rx).await?;
        }

        result
    }

    /// Check that certificate's parents are in storage. Returns the digests of any parents that are
    /// missing.
    async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> CertManagerResult<HashSet<CertificateDigest>> {
        let _scope = monitored_scope("primary::rayls-consensus-state-sync::get_missing_parents");

        // handle genesis cert
        if certificate.round() == 1 {
            debug!(target: "primary::cert_manager", ?certificate, "cert round 1");
            for digest in certificate.header().parents() {
                if !self.config.genesis().contains_key(digest) {
                    return Err(
                        CertificateError::from(HeaderError::InvalidGenesisParent(*digest)).into()
                    );
                }
            }
            return Ok(HashSet::new());
        }

        // check storage
        let existence =
            self.config.node_storage().multi_contains(certificate.header().parents().iter())?;
        let missing_parents: HashSet<_> = certificate
            .header()
            .parents()
            .iter()
            .zip(existence.iter())
            .filter(|(_, exists)| !*exists)
            .map(|(digest, _)| *digest)
            .collect();

        // send request to start fetching parents
        if !missing_parents.is_empty() {
            debug!(target: "primary::cert_manager", ?certificate, "missing {} parents", missing_parents.len());
            // metrics
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .certificates_suspended
                .with_label_values(&["missing_parents"])
                .inc();

            // start fetching parents
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(Arc::new(certificate.clone())))
                .await?;
        }

        Ok(missing_parents)
    }

    /// Try to accept the verified certificate.
    ///
    /// The certificate's state must be verified. This method writes to storage and returns the
    /// result to caller.
    ///
    /// NOTE: `self::process_verified_certificates` checks the verification status, so all
    /// certificates managed here are verified.
    // synchronizer::accept_certificate_internal
    async fn accept_verified_certificates(
        &mut self,
        certificates: VecDeque<Certificate>,
        shutdown_rx: &Noticer,
    ) -> CertManagerResult<()> {
        let _scope = monitored_scope("primary::cert_manager::accept_certificate");
        debug!(target: "primary::cert_manager", ?certificates, "accepting {:?} certificates", certificates.len());

        // persist first so downstream failures still leave the store authoritative
        self.persist_and_advance_high_water_mark(&certificates)?;

        for cert in certificates.into_iter() {
            self.record_accepted_metrics(&cert);

            // append+forward error is fatal except during shutdown; certs are persisted
            // above, so the DAG rebuilds from storage at the next epoch start
            if shutdown_rx.noticed() {
                warn!(target: "primary::cert_manager", "shutdown in progress, skipping DAG forwarding for persisted certificate");
                continue;
            }

            if let Err(e) = self.forward_to_dag(cert).await {
                // Shutdown can race between the noticed() check and the forward.
                if shutdown_rx.noticed() {
                    warn!(target: "primary::cert_manager", ?e, "shutdown raced with forward");
                    return Ok(());
                }
                return Err(e);
            }
        }

        Ok(())
    }

    /// Persist certs and advance `cert_store_round` without forwarding to the DAG.
    fn persist_and_advance_high_water_mark(
        &mut self,
        certificates: &VecDeque<Certificate>,
    ) -> CertManagerResult<()> {
        // Certificates + both secondary indexes must be written atomically w.r.t.
        // the epoch-boundary clear (which takes the same locks).
        let storage = self.config.node_storage();
        let mut txn = storage.write_txn()?;
        txn.lock_table(Certificates::NAME)?;
        txn.lock_table(CertificateDigestByRound::NAME)?;
        txn.lock_table(CertificateDigestByOrigin::NAME)?;
        for certificate in certificates {
            let digest = certificate.digest();
            if let Err(e) = save_cert(&mut txn, digest, certificate.clone()) {
                error!(target: "primary::cert_manager", "Failed to write certificate for {digest} due to error {e}.");
                return Err(e.into());
            }
        }
        txn.commit()?;

        self.certs_accepted = self.certs_accepted.saturating_add(certificates.len() as u32);
        let gc_depth = self.config.parameters().gc_depth;
        if self.certs_accepted >= gc_depth {
            if let Some(max_round) = certificates.iter().map(|c| c.round()).max() {
                self.consensus_bus.cert_store_round().send_if_modified(|current| {
                    if max_round > *current {
                        *current = max_round;
                        true
                    } else {
                        false
                    }
                });
            }
        }
        Ok(())
    }

    /// Update per-cert acceptance metrics.
    fn record_accepted_metrics(&self, cert: &Certificate) {
        let highest_processed_round =
            self.highest_processed_round.fetch_max(cert.round()).max(cert.round());
        let certificate_source = certificate_source(&self.config, cert);
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_processed_round
            .with_label_values(&[certificate_source])
            .set(highest_processed_round as i64);
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_processed
            .with_label_values(&[certificate_source])
            .inc();
    }

    /// Stitch a cert's ancestor edges into `parents` and hand it to Bullshark.
    /// Caller must ensure downstream consumers are alive.
    async fn forward_to_dag(&mut self, cert: Certificate) -> CertManagerResult<()> {
        self.parents
            .append_certificate(cert.clone(), self.config.committee())
            .await
            .inspect_err(|e| {
                error!(target: "primary::cert_manager", ?e, "failed to append cert");
            })
            .map_err(|_| CertManagerError::FatalAppendParent)?;

        if let Err(e) = self.consensus_bus.new_certificates().send(cert).await {
            error!(target: "primary::cert_manager", ?e, "failed to forward accepted certificate to consensus");
            return Err(CertManagerError::FatalForwardAcceptedCertificate);
        }
        Ok(())
    }

    /// Drain pending certs to DAG on shutdown. Uses aggressive gc bound
    /// (orphan pruning at next epoch start catches any over-accepts).
    async fn drain_pending(&mut self) -> CertManagerResult<(usize, usize)> {
        let _scope = monitored_scope("primary::cert_manager::drain_pending");

        let gc_round = self.gc_round.load();
        let committed = *self.consensus_bus.committed_round_updates().borrow();
        let effective_gc = gc_round.max(committed);

        let mut forwarded_to_dag: usize = 0;

        while let Some((round, digest)) = self.pending.next_for_gc_round(effective_gc) {
            let unlocked = self.pending.update_pending(round, digest)?;
            if unlocked.is_empty() {
                continue;
            }
            self.persist_and_advance_high_water_mark(&unlocked)?;
            for cert in unlocked {
                self.record_accepted_metrics(&cert);
                self.forward_to_dag(cert).await?;
                forwarded_to_dag += 1;
            }
        }

        Ok((forwarded_to_dag, self.pending.num_pending()))
    }

    /// Advance pending state to current GC round, extending to
    /// `committed_round` during catch-up.
    async fn process_gc_round(&mut self, shutdown_rx: &Noticer) -> CertManagerResult<()> {
        // load latest gc round
        let gc_round = self.gc_round.load();

        // same catch-up detection as process_verified_certificates
        let committed = *self.consensus_bus.committed_round_updates().borrow();
        let gc_depth = self.config.parameters().gc_depth;
        let gc_stale = committed.saturating_sub(gc_round) > gc_depth;
        let is_catching_up = !self.consensus_bus.node_mode().borrow().is_active_cvv() || gc_stale;
        let effective_gc = if is_catching_up { gc_round.max(committed) } else { gc_round };

        // clear certificate aggregators for expired rounds
        self.parents.garbage_collect(&gc_round);

        // iterate one round at a time to preserve causal order
        while let Some((round, digest)) = self.pending.next_for_gc_round(effective_gc) {
            let unlocked = self.pending.update_pending(round, digest)?;
            self.accept_verified_certificates(unlocked, shutdown_rx).await?;
        }

        Ok(())
    }

    /// Feed parent certificates to the aggregator so the proposer can start.
    /// On fresh epoch start (empty cert store), sends genesis certificates.
    /// On rejoin, sends real certificates from the last two rounds.
    async fn recover_state(&mut self) -> CertManagerResult<()> {
        let stored = self
            .config
            .node_storage()
            .last_two_rounds_certs()
            .expect("Failed recovering certificates in primary core");

        let certificates = if stored.is_empty() {
            let genesis = Certificate::genesis(self.config.committee());
            info!(
                target: "primary::cert_manager",
                count = genesis.len(),
                "recover_state: cert store empty, feeding genesis to aggregator"
            );
            genesis
        } else {
            info!(
                target: "primary::cert_manager",
                count = stored.len(),
                rounds = ?stored.iter().map(|c| c.round()).collect::<std::collections::BTreeSet<_>>(),
                "recover_state: feeding last_two_rounds_certs to aggregator"
            );
            stored
        };

        for certificate in certificates {
            self.parents.append_certificate(certificate, self.config.committee()).await?;
        }

        Ok(())
    }

    /// Long running task to manage verified certificates.
    ///
    /// Certificate signature states are first verified, then parents are checked. If certificate
    /// parents are missing, the manager tracks them as pending. As parents become available or are
    /// removed through garbage collection, the certificate manager will update pending state and
    /// try to accept all known certificates.
    pub(crate) async fn run(mut self) -> CertManagerResult<()> {
        let shutdown_rx = self.config.shutdown().subscribe();
        let mut certificate_manager_rx = self.consensus_bus.certificate_manager().subscribe();

        // cert_manager is re-spawned per epoch; reset per-epoch state so the
        // cert_store_round gate re-engages instead of staying open from epoch 2 onward
        self.certs_accepted = 0;

        // recover state
        self.recover_state().await?;

        // Periodic stuck-state summary: emits a single aggregate warn line when
        // pending has grown and committed_round has not advanced since the last
        // tick. Complements the per-cert "certificate suspended" warns with an
        // observable signal an operator can grep for.
        let mut stuck_tick = tokio::time::interval(STUCK_STATE_SUMMARY_INTERVAL);
        stuck_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_committed_round = *self.consensus_bus.committed_round_updates().borrow();

        // process certificates until shutdown
        loop {
            tokio::select! {
                // update state
                Some(command) = certificate_manager_rx.recv() => {
                    match command {
                        CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply } => {
                            let result = self.process_verified_certificates(certificates, &shutdown_rx).await;

                            match result{
                                // return fatal errors immediately to force shutdown
                                Err(CertManagerError::FatalAppendParent)
                                | Err(CertManagerError::FatalForwardAcceptedCertificate) => {
                                    error!(target: "primary::cert_manager", ?result, "fatal error. shutting down...");
                                    return result;
                                }

                                non_fatal_results => {
                                    let _ = reply.send(non_fatal_results);
                                }
                            }
                        }

                        CertificateManagerCommand::FilterUnknownDigests { mut unknown, reply } => {
                            self.pending.filter_unknown_digests(&mut unknown);
                            let _ = reply.send(unknown);
                        },
                    }
                }

                result = self.garbage_collector.ready() => {
                    match result {
                        Ok(()) => self.process_gc_round(&shutdown_rx).await?,
                        Err(GarbageCollectorError::Timeout) => (), // ignore non-fatal
                        _ => result? // return fatal error
                    }
                }

                _ = stuck_tick.tick() => {
                    let pending_count = self.pending.num_pending();
                    let committed = *self.consensus_bus.committed_round_updates().borrow();
                    if pending_count >= PENDING_COUNT_WARNING_THRESHOLD
                        && committed == last_committed_round
                    {
                        warn!(
                            target: "primary::cert_manager",
                            pending_count,
                            gc_round = self.gc_round.load(),
                            committed_round = committed,
                            node_mode = ?*self.consensus_bus.node_mode().borrow(),
                            "pending queue stuck - committed_round unchanged since last tick",
                        );
                    }
                    last_committed_round = committed;
                }

                // drain pending certs before exit (5s bounded)
                _ = &shutdown_rx => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        self.drain_pending(),
                    ).await {
                        Ok(Ok((forwarded_to_dag, still_pending))) => info!(
                            target: "primary::cert_manager",
                            forwarded_to_dag,
                            still_pending,
                            "drain_pending complete on shutdown",
                        ),
                        Ok(Err(e)) => warn!(
                            target: "primary::cert_manager",
                            ?e,
                            "drain_pending errored on shutdown",
                        ),
                        Err(_) => warn!(
                            target: "primary::cert_manager",
                            "drain_pending timed out on shutdown",
                        ),
                    }

                    let mut drained = 0usize;
                    while let Ok(command) = certificate_manager_rx.try_recv() {
                        if let CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply } = command {
                            let count = certificates.len();
                            let result = self.process_verified_certificates(certificates, &shutdown_rx).await;
                            let _ = reply.send(result);
                            drained += count;
                        }
                    }
                    if drained > 0 {
                        info!(
                            target: "primary::cert_manager",
                            drained,
                            "drained remaining certificates before shutdown"
                        );
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// Commands for the [CertficateManager].
#[derive(Debug)]
pub(crate) enum CertificateManagerCommand {
    /// Message from CertificateValidator.
    ProcessVerifiedCertificates {
        /// The certificate that was verified.
        ///
        /// Try to accept this certificate. If it has missing parents, track the certificate as
        /// pending and return an error.
        certificates: Vec<Certificate>,
        /// Return the result to the certificate validator.
        reply: oneshot::Sender<CertManagerResult<()>>,
    },
    /// Filter certificate digests that are not in local storage.
    ///
    /// Remove digests that are already tracked by `Pending`.
    /// This is used to vote on headers.
    FilterUnknownDigests {
        /// The collection of digests not found in local storage.
        unknown: Vec<CertificateDigest>,
        /// Return the result to the header validator.
        reply: oneshot::Sender<Vec<CertificateDigest>>,
    },
}
