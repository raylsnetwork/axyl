use crate::{engine::ExecutionNode, epoch_manager::types::EpochManager};
use rayls_consensus_primary::ConsensusBus;
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
    /// Create the reth environment and the [`ExecutionNode`] that wraps it.
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

    /// Spawn the node-scoped [`engine_update_loop`] with the production gap-check cadence.
    ///
    /// The task is node-scoped rather than epoch-scoped because the engine keeps executing queued
    /// outputs after an epoch shuts down, and those blocks must still reach
    /// `ConsensusBus::recently_executed_blocks`.
    pub(super) fn spawn_engine_update_task(
        &self,
        shutdown_rx: Noticer,
        engine_state: CanonStateNotificationStream,
        engine: ExecutionNode,
        task_manager: &TaskManager,
    ) {
        let consensus_bus = self.consensus_bus.clone();
        task_manager.spawn_critical_task(
            "latest execution block",
            engine_update_loop(
                shutdown_rx,
                engine_state,
                consensus_bus,
                engine,
                Duration::from_secs(30),
            ),
        );
    }
}

/// Run the engine-update loop until node shutdown or the end of the canonical-state stream.
///
/// Each canonical tip is pushed into `ConsensusBus::recently_executed_blocks`; each
/// `gap_check_interval` tick checks batch-tracker gaps and sweeps every worker pool's in-flight
/// marks. The interval is a parameter so a test can drive one maintenance tick without the
/// production cadence. The sweep policy is not: each tracker applies the policy of the role the
/// current epoch armed it for, so this node-scoped loop cannot apply a stale or wrong-role policy
/// (unarmed and forwarding-armed trackers sweep nothing).
pub(crate) async fn engine_update_loop(
    shutdown_rx: Noticer,
    mut engine_state: CanonStateNotificationStream,
    consensus_bus: ConsensusBus,
    engine: ExecutionNode,
    gap_check_interval: Duration,
) {
    let mut gap_check_interval = tokio::time::interval(gap_check_interval);
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
                // pools are fetched per tick, not captured at spawn: worker components are
                // created during the first epoch, after this node-scoped task starts
                let anchor = consensus_bus.executed_anchor().borrow().number;
                for pool in engine.get_all_worker_transaction_pools().await {
                    // sealing marks age out via the sweep; forwarding marks have no sweep, so drive
                    // the O(pending) membership reconcile here too: its only other trigger is
                    // the canonical-stream task, which a burst can starve. both are idempotent
                    pool.in_flight().sweep_due(anchor);
                    pool.reconcile_in_flight();
                }
            }
        )
    }
}
