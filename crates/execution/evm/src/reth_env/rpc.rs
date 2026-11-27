use crate::{
    error::RaylsRethResult,
    reth_env::{RethEnv, RpcServer},
    traits::{RaylsExecution, RaylsPrimitives},
    worker::WorkerNetwork,
    RaylsTransactionPool, WorkerTxPool,
};
use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_engine_primitives::ConsensusEngineEvent;
use reth_rpc_eth_types::EthStateCacheConfig;
use reth_tokio_util::EventSender;
use std::sync::Arc;

impl RethEnv {
    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        other: impl Into<Methods>,
    ) -> RpcServer {
        let transaction_pool: RaylsTransactionPool = transaction_pool.into();
        let rayls_execution = Arc::new(RaylsExecution);
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.blockchain_provider.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network.clone())
            .with_executor(self.reth_runtime.clone())
            .with_evm_config(self.evm_config.clone())
            // .with_events(self.blockchain_provider.clone())
            // .with_block_executor(self.evm_executor.clone())
            .with_consensus(rayls_execution.clone());

        // //.node_configure namespaces
        let modules_config = self.node_config.rpc.transport_rpc_module_config();
        let rpc = &self.node_config.rpc;
        let eth_api = EthApi::builder(
            self.blockchain_provider.clone(),
            transaction_pool,
            network,
            self.evm_config.clone(),
        )
        .eth_state_cache_config(EthStateCacheConfig {
            max_blocks: rpc.rpc_state_cache.max_blocks,
            max_receipts: rpc.rpc_state_cache.max_receipts,
            max_headers: rpc.rpc_state_cache.max_headers,
            max_concurrent_db_requests: rpc.rpc_state_cache.max_concurrent_db_requests,
            max_cached_tx_hashes: rpc.rpc_state_cache.max_cached_tx_hashes,
        })
        // TODO(#416): reth 2.0 changed some defaults (gas oracle, file logging, thread pools).
        // The ones we want to keep are set back in #416.
        .gas_oracle_config(rpc.gas_price_oracle.gas_price_oracle_config())
        .gas_cap(rpc.rpc_gas_cap.into())
        .max_simulate_blocks(rpc.rpc_max_simulate_blocks)
        .eth_proof_window(rpc.rpc_eth_proof_window)
        .proof_permits(rpc.rpc_proof_permits)
        // Without this the builder falls back to `Runtime::test()`, a small test runtime with
        // its own pools.
        .task_spawner(self.reth_runtime.clone())
        .build();

        let engine_events: EventSender<ConsensusEngineEvent<RaylsPrimitives>> = Default::default();
        let mut server = rpc_builder.build(modules_config, eth_api, engine_events);
        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "rayls::execution", "Error merging RL rpc module: {e:?}");
        }

        server
    }

    /// Start running the RPC server for this instance.
    pub async fn start_rpc(&self, server: &RpcServer) -> RaylsRethResult<RpcServerHandle> {
        let server_config = self.node_config.rpc.rpc_server_config();
        Ok(server_config.start(server).await?)
    }
}
