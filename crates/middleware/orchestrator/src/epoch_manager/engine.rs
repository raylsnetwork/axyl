use crate::{
    engine::{ExecutionNode, RaylsBuilder},
    epoch_manager::types::EpochManager,
};
use rayls_execution_evm::{
    reth_env::{RethDb, RethEnv},
    CanonStateNotificationStream,
};
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, Database as ReDatabase, Noticer, TaskManager,
};
use rayls_middleware_rewards::RewardsCounter;
use std::time::Duration;
use tokio_stream::StreamExt;
use tracing::{error, info};

/// Opens the boot EL environment with the exact wiring node boot uses (consistency check and
/// unwind included).
///
/// The single construction point for the boot `RethEnv`: node boot (`create_engine`) and the
/// offline cold migration both route through it, so the migration's EL anchor is computed against
/// the same environment a real boot would open. A hand-copied open could silently drift and floor
/// cold archival differently than boot.
pub(crate) async fn open_boot_reth_env(
    builder: &RaylsBuilder,
    task_manager: &TaskManager,
    reth_db: RethDb,
    rewards_counter: RewardsCounter,
    allow_v1: bool,
) -> eyre::Result<RethEnv> {
    let parameters = &builder.rayls_infrastructure_config.parameters;
    RethEnv::new(
        &builder.node_config,
        task_manager,
        reth_db,
        parameters.basefee_address,
        rewards_counter,
        &builder.build_metadata,
        Some(parameters.network),
        Some(parameters.min_base_fee),
        allow_v1,
    )
    .await
}

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
        let reth_env = open_boot_reth_env(
            &self.builder,
            engine_task_manager,
            self.reth_db.clone(),
            gas_accumulator.rewards_counter(),
            false,
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
