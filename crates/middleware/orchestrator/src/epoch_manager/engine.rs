use crate::{engine::ExecutionNode, epoch_manager::types::EpochManager};
use rayls_execution_evm::{reth_env::RethEnv, CanonStateNotificationStream};
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, Database as ReDatabase, Noticer, TaskManager,
};
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{error, info};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Helper method to create all engine components.
    pub(super) async fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<ExecutionNode> {
        // create execution components (ie - reth env)
        let basefee_address = self.builder.rayls_infrastructure_config.parameters.basefee_address;
        let network = self.builder.rayls_infrastructure_config.parameters.network;
        let min_base_fee = self.builder.rayls_infrastructure_config.parameters.min_base_fee;
        let reth_env = RethEnv::new(
            &self.builder.node_config,
            engine_task_manager,
            self.reth_db.clone(),
            basefee_address,
            gas_accumulator.rewards_counter(),
            &self.builder.build_metadata,
            Some(network),
            Some(min_base_fee),
        )
        .await?;
        let engine = ExecutionNode::new(&self.builder, reth_env)?;

        Ok(engine)
    }

    /// Spawn a node-scoped task to update `ConsensusBus::recently_executed_blocks` every time the
    /// engine produces a new final block. This task must outlive individual epochs because the
    /// engine continues executing queued outputs after epoch shutdown.
    pub(super) fn spawn_engine_update_task(
        &self,
        shutdown_rx: Noticer,
        mut engine_state: CanonStateNotificationStream,
        task_manager: &TaskManager,
    ) {
        let consensus_bus = self.consensus_bus.clone();
        task_manager.spawn_critical_task("latest execution block", async move {
            let mut gap_check_interval = tokio::time::interval(Duration::from_secs(30));
            gap_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select!(
                    _ = &shutdown_rx => {
                        info!(target: "engine", "received node shutdown, stopping recently-executed blocks updater");
                        break;
                    }
                    latest = engine_state.next() => {
                        if let Some(latest) = latest {
                            consensus_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(latest.tip().clone_sealed_header()));
                        } else {
                            error!(target: "engine", "engine state stream ended, node will exit");
                            break;
                        }
                    }
                    _ = gap_check_interval.tick() => {
                        consensus_bus.batch_tracker().check_gaps();
                    }
                )
            }
        });
    }
}
