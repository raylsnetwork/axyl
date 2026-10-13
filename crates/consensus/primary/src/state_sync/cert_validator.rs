//! Validate certificates received from peers.

use super::{cert_manager::CertificateManager, AtomicRound, HeaderValidator};
use crate::{
    error::{CertManagerError, CertManagerResult},
    state_sync::CertificateManagerCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{
    error::{CertificateError, HeaderError},
    Certificate, Database, Hash as _, Header, RaylsSender as _, Round, TaskSpawner,
};

/// Number of headers to batch together for sync tasks.
/// Batching reduces task spawning overhead during catch-up scenarios.
const SYNC_HEADER_BATCH_SIZE: usize = 20;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::{debug, error, trace, warn};

#[cfg(test)]
#[path = "../tests/cert_validator_tests.rs"]
mod cert_validator_tests;

pub(super) fn certificate_source<DB: Database>(
    config: &ConsensusConfig<DB>,
    certificate: &Certificate,
) -> &'static str {
    if let Some(authority_id) = config.authority_id() {
        if authority_id.eq(certificate.origin()) {
            "own"
        } else {
            "other"
        }
    } else {
        "other"
    }
}

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub(super) struct CertificateValidator<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// Highest garbage collection round.
    ///
    /// This is managed by GarbageCollector and shared with CertificateValidator.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicRound,
    /// Spawner for async tasks.
    task_spawner: TaskSpawner,
}

impl<DB> CertificateValidator<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(super) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
        highest_received_round: AtomicRound,
        task_spawner: TaskSpawner,
    ) -> Self {
        Self {
            consensus_bus,
            config,
            gc_round,
            highest_processed_round,
            highest_received_round,
            task_spawner,
        }
    }

    /// Convenience method for obtaining a new [CertificateManager].
    ///
    /// This is useful so the primary can handle new/spawn methods separately.
    /// The cert manager only needs to run during `spawn`.
    pub(super) fn new_cert_manager(&self) -> CertificateManager<DB> {
        CertificateManager::new(
            self.config.clone(),
            self.consensus_bus.clone(),
            self.gc_round.clone(),
            self.highest_processed_round.clone(),
        )
    }

    /// Process a certificate produced by the this node.
    pub(super) async fn process_own_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.process_certificate(certificate, false).await?;

        // The caller broadcasts this certificate to peers after this returns.
        // Wait until it is on disk first.
        // A crash that loses it lets a restart re-certify a different header for this round,
        // because the re-propose guard only trusts the stored certificate. That creates two
        // certificates for one round. Fail closed: on a write error, return the error and
        // broadcast nothing.
        self.config.node_storage().persist().await?;

        Ok(())
    }

    /// Process a certificate received from a peer.
    pub(super) async fn process_peer_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.process_certificate(certificate, true).await
    }

    /// Validate certificate.
    async fn process_certificate(
        &self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertManagerResult<()> {
        // Own certs skip validate_and_verify, so without this gate a late
        // prior-epoch own cert could poison the cert store across boundaries.
        let committee_epoch = self.config.committee().epoch();
        if certificate.epoch() != committee_epoch {
            return Err(CertificateError::Header(HeaderError::InvalidEpoch {
                theirs: certificate.epoch(),
                ours: committee_epoch,
            })
            .into());
        }

        // see if certificate already processed
        let digest = certificate.digest();
        if self.config.node_storage().contains(&digest)? {
            trace!(target: "primary::cert_validator", "Certificate {digest:?} has already been processed. Skip processing.");
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .duplicate_certificates_processed
                .inc();
            return Ok(());
        }

        // scrutinize certificates received from peers
        if external {
            // update signature verification
            certificate = self.validate_and_verify(certificate)?;
        }

        // update metrics
        debug!(target: "primary::cert_validator", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source = certificate_source(&self.config, &certificate);
        self.forward_verified_certs(certificate_source, certificate.round(), vec![certificate])
            .await
    }

    /// Validate and verify the certificate.
    ///
    /// This method validates the certificate and verifies signatures.
    fn validate_and_verify(&self, certificate: Certificate) -> CertManagerResult<Certificate> {
        // certificates outside gc can never be included in the DAG
        let gc_round = self.gc_round.load();

        if certificate.round() < gc_round {
            return Err(CertificateError::TooOld(
                certificate.digest(),
                certificate.round(),
                gc_round,
            )
            .into());
        }

        // validate certificate and verify signatures
        let verified_cert = certificate.validate_and_verify(self.config.committee())?;
        Ok(verified_cert)
    }

    /// Update metrics and send to Certificate Manager for final processing.
    async fn forward_verified_certs(
        &self,
        certificate_source: &str,
        highest_round: Round,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
        let highest_received_round =
            self.highest_received_round.fetch_max(highest_round).max(highest_round);

        // highest received round metric
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_received_round
            .with_label_values(&[certificate_source])
            .set(highest_received_round as i64);

        if !self.consensus_bus.node_mode().borrow().is_active_cvv() {
            let current_primary_round = *self.consensus_bus.primary_round_updates().borrow();
            if highest_received_round > current_primary_round {
                self.consensus_bus.primary_round_updates().send_replace(highest_received_round);
            }

            let minimal_round_for_parents = highest_received_round.saturating_sub(1);
            self.consensus_bus.parents().send((vec![], minimal_round_for_parents)).await?;
        }

        let max_age = self.config.parameters().gc_depth.saturating_sub(1);

        // highest_processed_round only advances when certs enter the store,
        // but the forward streamer can catch the node up independently via
        // consensus header execution. sync the floor to the committed leader
        // round so the acceptance window reflects the node's actual position.
        let committed_round =
            self.consensus_bus.last_consensus_header().borrow().sub_dag.leader_round();
        self.highest_processed_round.fetch_max(committed_round);

        // Collect headers that need batch syncing
        let mut headers_to_sync: Vec<Header> = Vec::with_capacity(certificates.len());

        let max_diff = self
            .config
            .network_config()
            .sync_config()
            .max_diff_between_external_cert_round_and_highest_local_round;

        for cert in &certificates {
            let highest_processed = self.highest_processed_round.load();

            // slide acceptance window forward if cert is too far ahead -
            // prevents permanent deadlock when peers GC'd intermediate certs
            if highest_processed + max_diff < cert.round() {
                let new_floor = cert.round().saturating_sub(max_diff);
                let prev = self.highest_processed_round.fetch_max(new_floor);
                // only log when the floor actually moved
                if prev < new_floor {
                    let actual_gap = cert.round() - prev;
                    warn!(
                        target: "primary::cert_validator",
                        cert_round = cert.round(),
                        new_floor,
                        actual_gap,
                        "sliding acceptance window forward - node is {} rounds behind peers",
                        actual_gap
                    );
                }
            }

            // Collect header for batch sync
            headers_to_sync.push(cert.header().clone());
        }

        // batch header sync to reduce task spawn overhead
        for chunk in headers_to_sync.chunks(SYNC_HEADER_BATCH_SIZE) {
            let headers: Vec<Header> = chunk.to_vec();
            let config = self.config.clone();
            let bus = self.consensus_bus.clone();

            self.task_spawner.spawn_task("sync header batches", async move {
                let sync_header = HeaderValidator::new(config, bus);
                for header in headers {
                    let res = sync_header.sync_header_batches(&header, true, max_age).await;
                    if let Err(e) = res {
                        error!(target: "primary::cert_validator", ?e, ?header, ?max_age, "error syncing batches for certified header");
                    }
                }
            });
        }

        // forward to certificate manager to check for pending parents and accept
        let (reply, res) = oneshot::channel();
        self.consensus_bus
            .certificate_manager()
            .send(CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply })
            .await?;

        // await response from certificate manager
        res.await.map_err(|_| CertManagerError::CertificateManagerOneshot)?
    }

    //
    //=== Parallel verification methods
    //

    /// Process a large collection of certificates downloaded from peers.
    ///
    /// This partitions the collection to verify certificates in chunks.
    pub(super) async fn process_fetched_certificates_in_parallel(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
        let _scope = monitored_scope("primary::cert_validator");
        let certificates = self.verify_collection(certificates).await?;

        // update metrics
        let highest_round = certificates.iter().map(|c| c.round()).max().unwrap_or(0);
        self.forward_verified_certs("other", highest_round, certificates).await
    }

    /// Main method to subdivide certificates into groups and verify based on causal relationship.
    async fn verify_collection(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<Vec<Certificate>> {
        // Early return for empty input
        if certificates.is_empty() {
            return Ok(certificates);
        }

        let direct_verification_certs = certificates.iter().cloned().enumerate().collect();

        // Below-gc (`TooOld`) certs are dropped during verification (see
        // `verify_certificate_chunk`), so return only the verified survivors instead of
        // failing the whole batch.
        let verified_certs = self.verify_certificate_chunk(direct_verification_certs).await?;

        // Update metrics about verification types (verified < total when stale certs are dropped).
        self.update_fetch_metrics(&certificates, verified_certs.len());

        Ok(verified_certs.into_iter().map(|(_idx, cert)| cert).collect())
    }

    /// Verifies a chunk of certificates in parallel.
    async fn verify_certificate_chunk(
        &self,
        certs_for_verification: Vec<(usize, Certificate)>,
    ) -> CertManagerResult<Vec<(usize, Certificate)>> {
        let verify_tasks: Vec<_> = certs_for_verification
            .chunks(self.config.network_config().sync_config().certificate_verification_chunk_size)
            .map(|chunk| self.spawn_verification_task(chunk.to_vec()))
            .collect();

        let mut verified_certs = Vec::new();
        for task in verify_tasks {
            let group_result = task.await.map_err(|e| {
                error!(target: "primary::cert_validator", ?e, "group verify certs task failed");
                CertManagerError::JoinError
            })??;
            verified_certs.extend(group_result);
        }
        Ok(verified_certs)
    }

    /// Spawns a single verification task for a chunk of certificates
    fn spawn_verification_task(
        &self,
        certs: Vec<(usize, Certificate)>,
    ) -> tokio::task::JoinHandle<CertManagerResult<Vec<(usize, Certificate)>>> {
        let validator = self.clone();
        // Don't have an equivelent on the task spawner.  Since this is a
        // strictly sync task even if we did it would not really do
        // anything special so Ok for now.
        tokio::task::spawn_blocking(move || {
            let now = Instant::now();
            let mut sanitized_certs = Vec::new();

            for (idx, cert) in certs {
                match validator.validate_and_verify(cert) {
                    Ok(verified) => sanitized_certs.push((idx, verified)),
                    // A below-gc cert can never enter the DAG; drop it and keep verifying the rest.
                    // Failing the whole fetch over one stale cert discards the still-needed
                    // above-gc parents (a catch-up fetch routinely straddles
                    // gc), forcing an endless refetch.
                    Err(CertManagerError::Certificate(CertificateError::TooOld(..))) => continue,
                    Err(e) => return Err(e),
                }
            }

            // Update metrics for verification time
            validator
                .consensus_bus
                .primary_metrics()
                .node_metrics
                .certificate_fetcher_total_verification_us
                .inc_by(now.elapsed().as_micros() as u64);

            Ok(sanitized_certs)
        })
    }

    /// Update metrics for fetched certificates.
    fn update_fetch_metrics(&self, certificates: &[Certificate], direct_count: usize) {
        let total_count = certificates.len() as u64;
        let direct_count = direct_count as u64;

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_directly
            .inc_by(direct_count);

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_indirectly
            .inc_by(total_count.saturating_sub(direct_count));
    }
}
