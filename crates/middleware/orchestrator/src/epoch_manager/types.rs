//! The epoch manager type.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.

use rayls_consensus_network::types::NetworkEvent;
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, QueChannel};
use rayls_consensus_worker::{WorkerNetworkHandle, WorkerRequest, WorkerResponse};
use rayls_execution_evm::reth_env::RethDb;

#[cfg(feature = "cold-storage")]
use super::cold_archive::ColdArchival;
use rayls_infrastructure_config::KeyConfig;
use rayls_infrastructure_types::{EpochRecord, Notifier};

use crate::engine::RaylsBuilder;

/// The long-running task manager name.
pub(super) const NODE_TASK_MANAGER: &str = "Node Task Manager";

/// The epoch-specific task manager name.
pub(super) const EPOCH_TASK_MANAGER: &str = "Epoch Task Manager";

/// The execution engine task manager name.
pub(super) const ENGINE_TASK_MANAGER: &str = "Engine Task Manager";

/// The worker's base task manager name. This is used by `fn worker_task_manager_name(id)`.
pub(crate) const WORKER_TASK_BASE: &str = "Worker Task";

/// The long-running type that oversees epoch transitions.
#[derive(Debug)]
pub(crate) struct EpochManager<P, DB> {
    /// The builder for node configuration
    pub(super) builder: RaylsBuilder,
    /// The data directory
    pub(super) rayls_datadir: P,
    /// Primary network handle.
    pub(super) primary_network_handle: Option<PrimaryNetworkHandle>,
    /// Worker network handle.
    pub(super) worker_network_handle: Option<WorkerNetworkHandle>,
    /// Key config - loaded once for application lifetime.
    pub(super) key_config: KeyConfig,
    /// The epoch manager's [Notifier] to shutdown all node processes.
    pub(super) node_shutdown: Notifier,
    /// Trigger for a graceful, ordered wind-down of the epoch loop, fired when the node is
    /// going down (SIGTERM/ctrl-c, or a node task exiting). Distinct from
    /// [`Self::node_shutdown`]: firing this drives the current `run_epoch` to its ordered
    /// `controlled_shutdown` WITHOUT waking the tasks that subscribe to `node_shutdown`
    /// directly (engine, network, vote collector), so the epoch teardown stays ordered.
    /// `node_shutdown` is fired only afterward, for the node-level drain + flush.
    pub(super) sigterm_trigger: Notifier,
    /// Reth DB, keep for entire execution.
    pub(super) reth_db: RethDb,
    /// Consensus DB, keep for entire execution.
    pub(super) consensus_db: DB,
    /// ConsensusBus for the application life.
    pub(super) consensus_bus: ConsensusBus,
    /// Persistent event stream for worker network events.
    pub(super) worker_event_stream: QueChannel<NetworkEvent<WorkerRequest, WorkerResponse>>,

    /// The record for a just completed epoch.
    pub(super) epoch_record: Option<EpochRecord>,

    /// The previous epoch's record, retained in memory for the next transition's
    /// `parent_hash`/committee lookup. Set when `epoch_record` is taken and handed
    /// to `collect_epoch_votes`, so the record chain can advance even though the
    /// record is no longer eagerly persisted (it now lands on disk only with its
    /// cert at quorum).
    pub(super) prev_epoch_record: Option<EpochRecord>,

    /// Indicates first epoch since process start
    pub(super) initial_epoch: bool,

    /// Cold-archival aggregate: boot healing, backlog migration, and the background seal actor.
    ///
    /// Runs off the consensus path: reconciled once at boot, then each due epoch is fully
    /// archived by the actor during the live epoch. Inert on backends without a cold tier (redb).
    #[cfg(feature = "cold-storage")]
    pub(super) cold_archival: ColdArchival,
}
