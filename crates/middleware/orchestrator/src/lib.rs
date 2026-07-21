// SPDX-License-Identifier: BUSL-1.1
// Library for managing all components used by a full-node in a single process.

use rayls_infrastructure_config::RaylsDirs;
use tokio::runtime::Builder;
use tracing::instrument;
pub mod engine;
pub mod epoch_manager;
pub mod primary;
pub mod types;
pub mod worker;

use crate::{
    engine::RaylsBuilder,
    epoch_manager::{open_consensus_db, EpochManager},
};
#[cfg(feature = "cold-storage")]
use rayls_execution_evm::reth_env::RethEnv;
#[cfg(feature = "cold-storage")]
use rayls_infrastructure_types::TaskManager;
#[cfg(feature = "cold-storage")]
use tracing::{info, warn};

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
#[instrument(level = "info", skip_all)]
pub fn launch_node<P>(
    builder: RaylsBuilder,
    rayls_datadir: P,
    passphrase: String,
) -> eyre::Result<()>
where
    P: RaylsDirs + Clone + 'static,
{
    // Held for the node's lifetime (declared before the runtime, so it drops after it): an
    // offline `cold-migrate` or a second node on the same datadir fails closed instead of
    // interleaving with archival on the shared consensus DB and cold jars.
    #[cfg(feature = "cold-storage")]
    let _consensus_db_lock =
        epoch_manager::acquire_consensus_db_lock(&rayls_datadir.consensus_db_path())?;

    let runtime = Builder::new_multi_thread()
        .thread_name("rayls-network")
        .enable_io()
        .enable_time()
        .build()?;

    let res = runtime.block_on(async move {
        let consensus_db = open_consensus_db(&rayls_datadir, &builder.consensus_db_config)?;
        // One aggregate owns cold archival end to end; inert on backends without a cold tier.
        #[cfg(feature = "cold-storage")]
        let cold_archival = epoch_manager::ColdArchival::new(&consensus_db);
        let mut epoch_manager = EpochManager::new(
            builder,
            rayls_datadir,
            passphrase,
            consensus_db,
            #[cfg(feature = "cold-storage")]
            cold_archival,
        )?;
        epoch_manager.run().await
    });

    // return result after shutdown
    res
}

/// Run the boot-time cold-storage work (crash reconcile + backlog migration) standalone,
/// without starting the node.
///
/// Backs the `cold-migrate` CLI command: operators drain a large cold backlog ahead of a real
/// start, so the node's own boot migration finds the work already done (both passes are
/// idempotent and resumable). Needs no BLS key, consensus, or networking. `compact` then
/// copy-compacts the consensus DB in place, reclaiming freed page space MDBX only recycles.
/// Fails closed: a held consensus-DB lock, a missing execution DB, or a failed pass aborts with
/// an error (no compaction, nonzero exit) rather than reporting success.
#[cfg(feature = "cold-storage")]
#[instrument(level = "info", skip_all)]
pub fn run_cold_migration<P>(
    builder: RaylsBuilder,
    rayls_datadir: P,
    compact: bool,
) -> eyre::Result<()>
where
    P: RaylsDirs + Clone + 'static,
{
    // Exclusive from the first consensus-DB touch through the optional compaction (declared
    // first, so it drops last): a live node holds the same lock for its lifetime, so a migration
    // beside it (or a second migration) fails here instead of interleaving with archival or
    // swapping the datafile under a live environment.
    let _consensus_db_lock =
        epoch_manager::acquire_consensus_db_lock(&rayls_datadir.consensus_db_path())?;

    // The EL anchor floors the archival cutoff, so an absent execution DB must be a hard error:
    // opening it would otherwise create and genesis-initialize a fresh EL here, and the zero
    // anchor would floor every pass to nothing while still reporting success.
    let reth_db_path = rayls_datadir.reth_db_path();
    if !reth_db_path.exists() {
        eyre::bail!(
            "no execution DB at {reth_db_path:?}; restore the EL alongside the consensus DB \
             before cold-migrate (its executed anchor floors the archival cutoff)"
        );
    }

    let runtime = Builder::new_multi_thread()
        .thread_name("rayls-cold-migrate")
        .enable_io()
        .enable_time()
        .build()?;

    let migrated = runtime.block_on(async {
        let consensus_db = open_consensus_db(&rayls_datadir, &builder.consensus_db_config)?;
        let archival = epoch_manager::ColdArchival::new(&consensus_db);
        if !archival.is_active() {
            info!(target: "epoch-manager", "database stack has no cold tier; nothing to migrate");
            return Ok(false);
        }

        archival.reconcile_at_boot().await?;

        // Open the EL through the same helper node boot uses (consistency check and unwind
        // included), so the anchor floor below can never exceed what a real boot would compute.
        // The task manager only hosts reth's idle background tasks (no block is ever executed
        // here); dropping the runtime on return reaps them.
        let task_manager = TaskManager::new("Cold Migration Task Manager");
        let reth_db = RethEnv::new_database(&builder.node_config, rayls_datadir.reth_db_path())?;
        let reth_env = epoch_manager::open_boot_reth_env(
            &builder,
            &task_manager,
            reth_db,
            rayls_middleware_rewards::from_db(consensus_db.clone()),
            false,
        )
        .await?;

        // Floor the cutoff by the EL execution anchor, mirroring the node's boot migration:
        // never seal (and hot-prune) an epoch the EL has not executed.
        let el_anchor_epoch = epoch_manager::recover_executed_anchor(&reth_env, &consensus_db)?
            .unwrap_or_default()
            .sub_dag
            .leader_epoch();
        if el_anchor_epoch == 0 {
            warn!(
                target: "epoch-manager",
                "EL executed anchor is at genesis; the archival cutoff floors to zero and no \
                 epoch is eligible"
            );
        }
        archival.migrate_backlog(el_anchor_epoch).await?;

        info!(target: "epoch-manager", "cold-storage migration complete");
        Ok::<bool, eyre::Report>(true)
    })?;

    // Drop the runtime before compacting: reth's background tasks hold clones of the consensus
    // DB handle, and the in-place swap needs every environment handle closed.
    drop(runtime);

    // No cold tier: nothing was migrated (already logged), so there is nothing to compact.
    if !migrated {
        return Ok(());
    }
    if compact {
        let stats = rayls_infrastructure_storage::mdbx::compact_in_place(
            &rayls_datadir.consensus_db_path(),
            &builder.consensus_db_config,
        )?;
        info!(
            target: "epoch-manager",
            before_mb = stats.before_bytes / (1024 * 1024),
            after_mb = stats.after_bytes / (1024 * 1024),
            tables = stats.tables_verified,
            "consensus DB compacted in place",
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/epoch_transition_tests.rs"]
mod epoch_transition_tests;

#[cfg(test)]
#[path = "tests/batch_seq_gate_tests.rs"]
mod batch_seq_gate_tests;

#[cfg(test)]
mod clippy {
    use rand as _;
    use rayls_infrastructure_network_types as _;
    use rayls_testing_test_utils as _;
}
