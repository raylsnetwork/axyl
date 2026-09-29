//! The Primary type

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    network::{PrimaryNetworkHandle, WorkerReceiverHandler},
    proposer::Proposer,
    state_handler::StateHandler,
    ConsensusBus, StateSynchronizer,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_types::{Database, RaylsReceiver, RaylsSender, TaskManager};
use std::sync::Arc;
use tracing::info;

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
mod primary_tests;

#[derive(Debug)]
/// The main `Primary` struct.
pub struct Primary<DB> {
    /// Handle to the primary network.
    primary_network: PrimaryNetworkHandle,
    ///  State synchronizer.
    rayls_consensus_state_sync: StateSynchronizer<DB>,
}

impl<DB: Database> Primary<DB> {
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        primary_network: PrimaryNetworkHandle,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
    ) -> Self {
        // Write the parameters to the logs.
        config.parameters().tracing();

        // Some info statements
        info!(
            "Boot primary node with public key {:?}",
            config.authority().as_ref().map(|a| a.protocol_key().encode_base58()),
        );

        let worker_receiver_handler =
            WorkerReceiverHandler::new(consensus_bus.clone(), config.node_storage().clone());

        config
            .local_network()
            .set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler));

        Self { primary_network, rayls_consensus_state_sync }
    }

    /// Spawns the primary.
    pub fn spawn(
        &mut self,
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_manager: &TaskManager,
    ) {
        // Probably don't need this for a non-committee member but it keeps channels drained and is
        // not a problem.
        self.rayls_consensus_state_sync.spawn(task_manager);

        Certifier::spawn(
            config.clone(),
            consensus_bus.clone(),
            self.rayls_consensus_state_sync.clone(),
            self.primary_network.clone(),
            task_manager,
        );

        // observers follow via streamed consensus headers and never rebuild the DAG;
        // CvvInactive still needs the fetcher to rejoin consensus
        // Dev (single-node): always a 1-validator committee — no peers to fetch certs from.
        #[cfg(feature = "dev-single-node-setup")]
        let spawn_cert_fetcher = false;
        #[cfg(not(feature = "dev-single-node-setup"))]
        let spawn_cert_fetcher = consensus_bus.node_mode().borrow().is_cvv();
        if spawn_cert_fetcher {
            CertificateFetcher::spawn(
                config.clone(),
                self.primary_network.clone(),
                consensus_bus.clone(),
                self.rayls_consensus_state_sync.clone(),
                task_manager,
            );
        } else {
            // drain the cert_fetcher channel so producers
            // (cert_manager, consensus/state.rs MissingParent paths) do not backpressure
            let mut cert_fetcher_rx = consensus_bus.certificate_fetcher().subscribe();
            task_manager.spawn_critical_task("drain cert_fetcher for non-cvv", async move {
                while cert_fetcher_rx.recv().await.is_some() {}
            });
        }

        // Only run the proposer task if we are a CVV.
        if consensus_bus.node_mode().borrow().is_active_cvv() {
            // When the `StateSynchronizer` collects enough parent certificates, the `Proposer` generates
            // a new header with new block digests from our workers and sends it to the `Certifier`.
            let proposer = Proposer::new(
                config.clone(),
                config.authority_id().expect("CVV has an auth id"),
                consensus_bus.clone(),
                leader_schedule,
                task_manager.get_spawner(),
            );

            proposer.spawn(task_manager);
        } else {
            // drain parents channel for non-cvv; otherwise senders back up and hang
            let mut parents_rx = consensus_bus.parents().subscribe();
            task_manager.spawn_critical_task("Clear parent certs for non-CVV", async move {
                while (parents_rx.recv().await).is_some() {}
            });
        }

        if let Some(authority_id) = config.authority_id() {
            // validator-only: tracks latest consensus round so other tasks can prune state
            StateHandler::spawn(
                authority_id,
                consensus_bus,
                config.shutdown().subscribe(),
                task_manager,
            );
        }

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {:?} successfully booted on {:?}",
            config.authority_id(),
            config.config().node_info.p2p_info.primary.network_address
        );
    }

    /// Return a reference to the Primary's network.
    pub fn network_handle(&self) -> &PrimaryNetworkHandle {
        &self.primary_network
    }

    /// Return a clone of the Primary's [StateSynchronizer].
    pub fn rayls_consensus_state_sync(&self) -> StateSynchronizer<DB> {
        self.rayls_consensus_state_sync.clone()
    }
}
