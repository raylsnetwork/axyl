use crate::engine::{
    node_builder::ExecutionNodeBuilder, node_inner::ExecutionNodeInner, rayls_builder::RaylsBuilder,
};

use rayls_consensus_worker::WorkerNetworkHandle;
use rayls_execution_evm::{
    reth_env::RethEnv, system_calls::EpochState, CanonStateNotificationStream, WorkerTxPool,
};
use rayls_execution_rpc::EngineToPrimary;
use rayls_infrastructure_types::{
    batch_tracker::BatchTracker,
    executed_batch_registry::ExecutedBatchRegistry,
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    BatchSender, BatchValidation, BlsPublicKey, CameFrom, ConsensusHeader, ConsensusOutput,
    Database, Epoch, ExecHeader, Noticer, SealedHeader, TaskSpawner, WorkerId, B256,
};
use rayls_middleware_processor::batch::BatchOrdering;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{mpsc, oneshot, watch, RwLock};

/// Wrapper for the inner execution node components.
#[derive(Clone, Debug)]
pub struct ExecutionNode {
    internal: Arc<RwLock<ExecutionNodeInner>>,
}

impl ExecutionNode {
    /// Create a new instance of `Self`.
    pub fn new(rayls_builder: &RaylsBuilder, reth_env: RethEnv) -> eyre::Result<Self> {
        let inner = ExecutionNodeBuilder::new(rayls_builder, reth_env).build()?;

        Ok(ExecutionNode { internal: Arc::new(RwLock::new(inner)) })
    }

    /// Execution engine to produce blocks after consensus.
    pub async fn start_engine<DB: Database>(
        &self,
        rx_output: mpsc::Receiver<(CameFrom, ConsensusOutput)>,
        rx_shutdown: Noticer,
        gas_accumulator: GasAccumulator,
        batch_tracker: Option<Arc<BatchTracker>>,
        batch_ordering: BatchOrdering<DB>,
        executed_anchor_tx: Option<watch::Sender<ConsensusHeader>>,
        engine_idle_tx: Option<watch::Sender<bool>>,
        last_consensus_header: ConsensusHeader,
        engine_done_tx: oneshot::Sender<()>,
        executed_batch_registry: ExecutedBatchRegistry,
    ) -> eyre::Result<()> {
        let mut guard = self.internal.write().await;
        guard
            .start_engine(
                rx_output,
                rx_shutdown,
                gas_accumulator,
                batch_tracker,
                batch_ordering,
                executed_anchor_tx,
                engine_idle_tx,
                last_consensus_header,
                engine_done_tx,
                executed_batch_registry,
            )
            .await
    }

    /// Initialize the worker's transaction pool and public RPC.
    ///
    /// This method should be called on node startup.
    pub async fn initialize_worker_components<EP>(
        &self,
        worker_id: WorkerId,
        network_handle: WorkerNetworkHandle,
        engine_to_primary: EP,
    ) -> eyre::Result<()>
    where
        EP: EngineToPrimary + Send + Sync + 'static,
    {
        let mut guard = self.internal.write().await;
        guard.initialize_worker_components(worker_id, network_handle, engine_to_primary).await
    }

    /// Respawn any tasks on the worker network when we get a new epoch task manager.
    ///
    /// This method should be called on epoch rollover.
    pub async fn respawn_worker_network_tasks(&self, network_handle: WorkerNetworkHandle) {
        let guard = self.internal.write().await;
        guard.respawn_worker_network_tasks(network_handle).await
    }

    /// Spawn the batch builder for one epoch.
    pub async fn start_batch_builder(
        &self,
        worker_id: WorkerId,
        block_provider_sender: BatchSender,
        task_spawner: &TaskSpawner,
        base_fee: BaseFeeContainer,
        epoch: Epoch,
        initial_batch_seq: u64,
        epoch_boundary: u64,
    ) -> eyre::Result<()> {
        let mut guard = self.internal.write().await;
        guard
            .start_batch_builder(
                worker_id,
                block_provider_sender,
                task_spawner,
                base_fee,
                epoch,
                initial_batch_seq,
                epoch_boundary,
            )
            .await
    }

    /// Spawn the observer transaction forwarder for one epoch.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_txn_forwarder(
        &self,
        worker_id: WorkerId,
        network_handle: WorkerNetworkHandle,
        executed_anchor: watch::Receiver<ConsensusHeader>,
        last_seen_header: watch::Receiver<ConsensusHeader>,
        committee: Vec<BlsPublicKey>,
        task_spawner: &TaskSpawner,
        max_gossip_message_size: usize,
    ) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        guard.start_txn_forwarder(
            worker_id,
            network_handle,
            executed_anchor,
            last_seen_header,
            committee,
            task_spawner,
            max_gossip_message_size,
        )
    }

    /// Create the batch validator for one epoch.
    pub async fn new_batch_validator(
        &self,
        worker_id: &WorkerId,
        base_fee: BaseFeeContainer,
        epoch: Epoch,
    ) -> Arc<dyn BatchValidation> {
        let guard = self.internal.read().await;
        guard.new_batch_validator(worker_id, base_fee, epoch)
    }

    /// Retrieve the last executed block from the database to restore consensus.
    pub async fn last_executed_output(&self) -> eyre::Result<B256> {
        let guard = self.internal.read().await;
        guard.last_executed_output()
    }

    /// Return a vector of the last 'number' executed block headers.
    pub async fn last_executed_blocks(&self, number: u64) -> eyre::Result<Vec<ExecHeader>> {
        let guard = self.internal.read().await;
        guard.last_executed_blocks(number)
    }

    /// Return a vector of the last 'number' executed block headers.
    /// These are the execution blocks finalized after consensus output, i.e. it
    /// skips all the "intermediate" blocks and is just the final block from a consensus output.
    pub async fn last_executed_output_blocks(
        &self,
        number: u64,
    ) -> eyre::Result<Vec<SealedHeader>> {
        let guard = self.internal.read().await;
        guard.last_executed_output_blocks(number)
    }

    /// Return a receiver for canonical blocks.
    pub async fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        let guard = self.internal.read().await;
        let reth_env = guard.get_reth_env();
        reth_env.canonical_block_stream()
    }

    /// Return the reth execution env.
    pub async fn get_reth_env(&self) -> RethEnv {
        let guard = self.internal.read().await;
        guard.get_reth_env()
    }

    /// Flush all pending blocks to disk synchronously.
    pub async fn flush_persistence(&self) -> eyre::Result<()> {
        let guard = self.internal.read().await;
        let reth_env = guard.get_reth_env();
        Ok(reth_env.flush_persistence().await?)
    }

    /// Return an HTTP client for submitting transactions to the RPC.
    pub async fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<jsonrpsee::http_client::HttpClient>> {
        let guard = self.internal.read().await;
        guard.worker_http_client(worker_id)
    }

    /// Return an owned instance of the worker's transaction pool.
    pub async fn get_worker_transaction_pool(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<WorkerTxPool> {
        let guard = self.internal.read().await;
        guard.get_worker_transaction_pool(worker_id)
    }

    /// Return an owned instance of all the worker's transaction pools.
    pub async fn get_all_worker_transaction_pools(&self) -> Vec<WorkerTxPool> {
        let guard = self.internal.read().await;
        guard.get_worker_transaction_pools()
    }

    /// Return an HTTP local address for submitting transactions to the RPC.
    pub async fn worker_http_local_address(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<SocketAddr>> {
        let guard = self.internal.read().await;
        guard.worker_http_local_address(worker_id)
    }

    /// Read [EpochState] from the canonical tip.
    pub async fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        let guard = self.internal.read().await;
        guard.epoch_state_from_canonical_tip()
    }

    /// Read committee validator keys for epoch.
    pub async fn validators_for_epoch(&self, epoch: u32) -> eyre::Result<Vec<BlsPublicKey>> {
        let guard = self.internal.read().await;
        guard.validators_for_epoch(epoch)
    }
}
