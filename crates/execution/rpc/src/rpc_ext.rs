//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{RaylsNetworkRpcResult, RaylsRpcError},
    EngineToPrimary, NodeStatus,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use rayls_execution_evm::reth_env::ChainSpec;
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, Genesis,
};

/// Rayls Network RPC namespace.
///
/// rayls-specific RPC endpoints.
#[rpc(server, namespace = "rayls")]
pub trait RaylsNetworkRpcExtApi {
    /// Return the latest consensus header.
    #[method(name = "latestHeader")]
    async fn latest_header(&self) -> RaylsNetworkRpcResult<ConsensusHeader>;
    /// Return the chain genesis.
    #[method(name = "genesis")]
    async fn genesis(&self) -> RaylsNetworkRpcResult<Genesis>;
    /// Get the header for epoch if available.
    #[method(name = "epochRecord")]
    async fn epoch_record(
        &self,
        epoch: Epoch,
    ) -> RaylsNetworkRpcResult<(EpochRecord, EpochCertificate)>;
    /// Get the header for epoch by hash if available.
    #[method(name = "epochRecordByHash")]
    async fn epoch_record_by_hash(
        &self,
        hash: BlockHash,
    ) -> RaylsNetworkRpcResult<(EpochRecord, EpochCertificate)>;
    /// Return the local node's role and sync status.
    #[method(name = "nodeStatus")]
    async fn node_status(&self) -> RaylsNetworkRpcResult<NodeStatus>;
}

/// The type that implements `rayls` namespace trait.
#[derive(Debug)]
pub struct RaylsNetworkRpcExt<N: EngineToPrimary> {
    /// The chain id for this node.
    chain: ChainSpec,
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
}

#[async_trait]
impl<N: EngineToPrimary> RaylsNetworkRpcExtApiServer for RaylsNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    async fn latest_header(&self) -> RaylsNetworkRpcResult<ConsensusHeader> {
        Ok(self.inner_node_network.get_latest_consensus_block())
    }

    async fn genesis(&self) -> RaylsNetworkRpcResult<Genesis> {
        Ok(self.chain.genesis().clone())
    }

    async fn epoch_record(
        &self,
        epoch: Epoch,
    ) -> RaylsNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(Some(epoch), None).ok_or(RaylsRpcError::NotFound)
    }

    async fn epoch_record_by_hash(
        &self,
        hash: BlockHash,
    ) -> RaylsNetworkRpcResult<(EpochRecord, EpochCertificate)> {
        self.inner_node_network.epoch(None, Some(hash)).ok_or(RaylsRpcError::NotFound)
    }

    async fn node_status(&self) -> RaylsNetworkRpcResult<NodeStatus> {
        Ok(self.inner_node_network.node_status())
    }
}

impl<N: EngineToPrimary> RaylsNetworkRpcExt<N> {
    /// Create new instance of the Rayls Network RPC extension.
    pub fn new(chain: ChainSpec, inner_node_network: N) -> Self {
        Self { chain, inner_node_network }
    }
}
