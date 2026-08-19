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
    /// Builds the execution node for this epoch manager.
    pub(super) async fn create_engine(
        &self,
        engine_task_manager: &TaskManager,
        gas_accumulator: &GasAccumulator,
    ) -> eyre::Result<ExecutionNode> {
        let reth_env = RethEnv::new_from_parameters(
            &self.builder.node_config,
            &self.builder.rayls_infrastructure_config.parameters,
            engine_task_manager,
            self.reth_db.clone(),
            gas_accumulator.rewards_counter(),
            &self.builder.build_metadata,
            false,
        )
        .await?;
        let engine = ExecutionNode::new(&self.builder, reth_env)?;

        Ok(engine)
    }

    /// Spawns the node-scoped task that records each executed block on
    /// `ConsensusBus::recently_executed_blocks` and runs the periodic gap check and in-flight
    /// mark sweep. Node-scoped because the engine keeps executing queued outputs after an epoch
    /// shuts down.
    pub(super) fn spawn_engine_update_task(
        &self,
        shutdown_rx: Noticer,
        mut engine_state: CanonStateNotificationStream,
        engine: ExecutionNode,
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
                        // Pools are fetched per tick: workers are created during the first epoch,
                        // after this node-scoped task starts.
                        let anchor = consensus_bus.executed_anchor().borrow().number;
                        for pool in engine.get_all_worker_transaction_pools().await {
                            // Seal marks age out via the TTL sweep. Forward marks have no sweep,
                            // so the O(pending) membership reconcile runs here too: its only other
                            // driver is the canonical-stream task, which a burst can starve.
                            pool.in_flight().sweep_due(anchor);
                            pool.reconcile_in_flight();
                        }
                    }
                )
            }
        });
    }
}
