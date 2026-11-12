//! Hierarchical type to hold tasks spawned for a worker in the network.
use rayls_consensus_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    ConsensusBus, Primary,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_types::{
    CameFrom, ConsensusOutput, Database as ConsensusDatabase, TaskManager,
    CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use rayls_middleware_bridge::{Executor, SubscriberResult};
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
pub(super) struct PrimaryNodeInner<CDB> {
    /// Consensus configuration.
    pub(super) consensus_config: ConsensusConfig<CDB>,
    /// Container for consensus channels.
    pub(super) consensus_bus: ConsensusBus,
    /// The primary struct that holds handles and network.
    pub(super) primary: Primary<CDB>,
}

impl<CDB: ConsensusDatabase> PrimaryNodeInner<CDB> {
    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    pub(super) async fn start(
        &mut self,
        task_manager: &TaskManager,
        to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
        execution_replay_completed: tokio::sync::watch::Sender<()>,
    ) -> eyre::Result<()> {
        // spawn primary and update `self`
        self.spawn_primary(task_manager, to_engine, execution_replay_completed).await?;

        Ok(())
    }

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing
    /// transactions.
    async fn spawn_primary(
        &mut self,
        task_manager: &TaskManager,
        to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
        execution_replay_completed: tokio::sync::watch::Sender<()>,
    ) -> SubscriberResult<()> {
        let leader_schedule = self
            .spawn_consensus(
                &self.consensus_bus,
                task_manager,
                to_engine,
                execution_replay_completed,
            )
            .await?;

        self.primary.spawn(
            self.consensus_config.clone(),
            &self.consensus_bus,
            leader_schedule,
            task_manager,
        );
        Ok(())
    }

    /// Spawn the consensus core and the client executing transactions.
    ///
    /// Pass the sender channel for consensus output and executor metrics.
    async fn spawn_consensus(
        &self,
        consensus_bus: &ConsensusBus,
        task_manager: &TaskManager,
        to_engine: mpsc::Sender<(CameFrom, ConsensusOutput)>,
        execution_replay_completed: tokio::sync::watch::Sender<()>,
    ) -> SubscriberResult<LeaderSchedule> {
        let leader_schedule = LeaderSchedule::from_store(
            self.consensus_config.committee().clone(),
            self.consensus_config.node_storage().clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            self.consensus_config.committee().clone(),
            self.consensus_bus.consensus_metrics().clone(),
            CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS,
            leader_schedule.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );
        Consensus::spawn(
            self.consensus_config.clone(),
            consensus_bus,
            ordering_engine,
            task_manager,
        );

        // Spawn the client executing the transactions.
        // It also synchronizes with the subscriber handler if it missed some transactions.
        let shutdown_notifier = self.consensus_config.shutdown();
        if shutdown_notifier.was_notified() {
            warn!(
                target: "epoch-manager",
                "shutdown notifier already resolved before subscriber subscribe()",
            );
        }
        Executor::spawn(
            self.consensus_config.clone(),
            shutdown_notifier.subscribe(),
            consensus_bus.clone(),
            task_manager,
            self.primary.network_handle().clone(),
            to_engine,
            execution_replay_completed,
        );

        Ok(leader_schedule)
    }
}
