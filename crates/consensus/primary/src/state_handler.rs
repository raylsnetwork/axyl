//! Filter consensus results to update execution state.

use crate::ConsensusBus;
use consensus_metrics::monitored_future;
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, Noticer, RaylsReceiver, RaylsSender, Round, TaskManager,
};
use tracing::{debug, error, info};

/// Updates Narwhal system state based on certificates received from consensus.
pub(crate) struct StateHandler {
    authority_id: AuthorityIdentifier,

    /// Used for Receives the ordered certificates from consensus.
    consensus_bus: ConsensusBus,
    /// Channel to signal committee changes.
    rx_shutdown: Noticer,
}

impl StateHandler {
    pub(crate) fn spawn(
        authority_id: AuthorityIdentifier,
        consensus_bus: &ConsensusBus,
        rx_shutdown: Noticer,
        task_manager: &TaskManager,
    ) {
        let state_handler =
            Self { authority_id, consensus_bus: consensus_bus.clone(), rx_shutdown };
        task_manager.spawn_critical_task(
            "state handler task",
            monitored_future!(
                async move {
                    state_handler.run().await;
                },
                "StateHandlerTask"
            ),
        );
    }

    async fn handle_sequenced(&mut self, commit_round: Round, certificates: Vec<Certificate>) {
        // report which of this authority's own headers the commit covered
        let own_rounds_committed: Vec<Round> = certificates
            .iter()
            .filter(|cert| cert.header().author() == &self.authority_id)
            .map(|cert| cert.header().round())
            .collect();
        debug!(target: "primary::state_handler", "Own committed rounds {:?} at round {:?}", own_rounds_committed, commit_round);

        if let Err(e) = self
            .consensus_bus
            .committed_own_headers()
            .send((commit_round, own_rounds_committed))
            .await
        {
            error!(target: "primary::state_handler", "error sending commit header: {e}");
        }
    }

    async fn run(mut self) {
        info!(target: "primary::state_handler", "StateHandler on node {} has started successfully.", self.authority_id);
        let mut rx_committed_certificates = self.consensus_bus.committed_certificates().subscribe();
        loop {
            tokio::select! {
                Some((commit_round, certificates)) = rx_committed_certificates.recv() => {
                    self.handle_sequenced(commit_round, certificates).await;
                },

                _ = &self.rx_shutdown => {
                    return;
                }
            }
        }
    }
}
