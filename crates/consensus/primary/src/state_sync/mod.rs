//! Modules for synchronizing state between nodes.

use crate::{error::CertManagerResult, ConsensusBus};
use cert_validator::CertificateValidator;
use gc::AtomicRound;
use header_validator::HeaderValidator;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::ConsensusStore;
use rayls_infrastructure_types::{
    error::HeaderResult, Certificate, CertificateDigest, Database, Header, Round, TaskKind,
    TaskManager, TaskSpawner,
};
mod cert_collector;
mod cert_manager;
mod cert_validator;
mod gc;
mod header_validator;
mod pending_cert_manager;
pub(crate) use cert_collector::CertificateCollector;
pub(crate) use cert_manager::CertificateManagerCommand;

#[cfg(test)]
#[path = "../tests/certificate_processing_tests.rs"]
/// Test the entire certificate flow.
mod cert_flow;

/// Process unverified headers and certificates.
#[derive(Debug, Clone)]
pub struct StateSynchronizer<DB> {
    /// The type to validate certificates.
    certificate_validator: CertificateValidator<DB>,
    /// The type to validate headers.
    header_validator: HeaderValidator<DB>,
}

impl<DB> StateSynchronizer<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        task_spawner: TaskSpawner,
    ) -> Self {
        let header_validator = HeaderValidator::new(config.clone(), consensus_bus.clone());
        // load highest round number from the ConsensusBlock table
        let highest_process_round = config
            .node_storage()
            .get_latest_sub_dag()
            // it should be impossible to have a subdag that is greater than the current epoch
            .filter(|subdag| subdag.leader_epoch() >= config.epoch())
            .map(|subdag| subdag.leader_round())
            .unwrap_or(0);

        // preserve round knowledge from CvvInactive to avoid TooOld after rejoin
        let current_round = *consensus_bus.primary_round_updates().borrow();
        // seed the gc round from the primed channel (primed by identify_node_mode before this
        // runs) so the cert parent-check gate never runs with gc=0 and suspends the history
        let gc_round = *consensus_bus.gc_round_updates().borrow();

        let certificate_validator = CertificateValidator::new(
            config,
            consensus_bus,
            AtomicRound::new(gc_round),
            AtomicRound::new(highest_process_round),
            AtomicRound::new(current_round),
            task_spawner,
        );

        Self { certificate_validator, header_validator }
    }

    /// Spawn the certificate manager and synchronize state between peers.
    pub(crate) fn spawn(&self, task_manager: &TaskManager) {
        let certificate_manager = self.certificate_validator.new_cert_manager();
        task_manager.spawn_classified_task(
            "certificate-manager",
            certificate_manager.run(),
            TaskKind::Drainable,
        );
    }

    //
    //=== Certificate API
    //

    /// Process a certificate produced by the this node.
    pub(crate) async fn process_own_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_own_certificate(certificate).await
    }

    /// Process a certificate received from a peer.
    pub(crate) async fn process_peer_certificate(
        &self,
        certificate: Certificate,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_peer_certificate(certificate).await
    }

    /// Process a large collection of certificates downloaded from peers.
    ///
    /// This partitions the collection to verify certificates in chunks.
    pub(crate) async fn process_fetched_certificates_in_parallel(
        &self,
        certificates: Vec<Certificate>,
    ) -> CertManagerResult<()> {
        self.certificate_validator.process_fetched_certificates_in_parallel(certificates).await
    }

    //
    //=== Header API
    //

    /// Returns the parent certificates of the given header, waits for availability if needed.
    pub(crate) async fn notify_read_parent_certificates(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<Certificate>> {
        self.header_validator.notify_read_parent_certificates(header).await
    }

    /// Synchronize batches.
    pub(crate) async fn sync_header_batches(
        &self,
        header: &Header,
        is_certified: bool,
        max_age: Round,
    ) -> HeaderResult<()> {
        self.header_validator.sync_header_batches(header, is_certified, max_age).await
    }

    /// Filter parent digests that do not exist in storage or pending state.
    ///
    /// Returns a collection of missing parent digests.
    pub(crate) async fn identify_unknown_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        self.header_validator.identify_unknown_parents(header).await
    }
}
