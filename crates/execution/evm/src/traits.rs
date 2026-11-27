//! Compatibility types for Rayls Node and reth.
//!
//! These are used to spawn execution components for the node and maintain compatibility with reth's
//! API.

use crate::RaylsChainSpec;
use alloy::rpc::types::engine::ExecutionPayload;
use rayls_infrastructure_types::{
    EthPrimitives, NodePrimitives, RecoveredBlock, SealedBlock, SealedHeader,
};
use reth::{payload::EthBuiltPayload, rpc::types::engine::ExecutionData};
pub use reth_consensus::{Consensus, ConsensusError};
use reth_consensus::{FullConsensus, HeaderValidator, ReceiptRootBloom};
use reth_db::DatabaseEnv;
use reth_engine_primitives::PayloadValidator;
use reth_node_builder::{BuiltPayload, NewPayloadError, NodeTypes, NodeTypesWithDB, PayloadTypes};
use reth_node_ethereum::{engine::EthPayloadAttributes, EthEngineTypes};
use reth_primitives_traits::Block;
use reth_provider::{BlockExecutionResult, EthStorage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Type for primitives.
pub type RaylsPrimitives = EthPrimitives;

/// Empty struct that implements Reth traits to supply GATs and functionality for Reth integration.
#[derive(Clone, Debug)]
pub struct RaylsNode {}

impl NodeTypes for RaylsNode {
    type Primitives = RaylsPrimitives;
    type ChainSpec = RaylsChainSpec;
    type Storage = EthStorage;
    type Payload = EthEngineTypes;
}

impl NodeTypesWithDB for RaylsNode {
    type DB = Arc<DatabaseEnv>;
}

/// Compatibility type to easily integrate with reth.
///
/// This type is used to noop verify all data. It is not used by Rayls Network, but is required to
/// integrate with reth for convenience. RL is mostly EVM/Ethereum types, but with a different
/// consensus. The traits impl on this type are only used beacon engine, which is not used by RL.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RaylsExecution;

impl<H> HeaderValidator<H> for RaylsExecution {
    fn validate_header(&self, _header: &SealedHeader<H>) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader<H>,
        _parent: &SealedHeader<H>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

impl<B: Block> Consensus<B> for RaylsExecution {
    fn validate_body_against_header(
        &self,
        _body: &B::Body,
        _header: &SealedHeader<B::Header>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock<B>) -> Result<(), ConsensusError> {
        Ok(())
    }
}

impl<N: NodePrimitives> FullConsensus<N> for RaylsExecution {
    fn validate_block_post_execution(
        &self,
        _block: &RecoveredBlock<N::Block>,
        _result: &BlockExecutionResult<N::Receipt>,
        _receipt_root_bloom: Option<ReceiptRootBloom>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

// Compatibility noop trait impl.
// This is for the reth rpc build method.
// NOTE: this should never be called because there is no beacon API
impl<Types> PayloadValidator<Types> for RaylsExecution
where
    Types: PayloadTypes<ExecutionData = ExecutionData>,
{
    type Block = rayls_infrastructure_types::Block;

    fn convert_payload_to_block(
        &self,
        _payload: ExecutionData,
    ) -> Result<SealedBlock<Self::Block>, NewPayloadError> {
        Ok(Default::default())
    }
}

/// A default payload type for [`EthEngineTypes`]
///
/// This is required by the `EngineApiTreeHandler` but is never used bc
/// RL doesn't send beacon messages.
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
#[non_exhaustive]
pub struct DefaultEthPayloadTypes;

impl PayloadTypes for DefaultEthPayloadTypes {
    type BuiltPayload = EthBuiltPayload;
    type PayloadAttributes = EthPayloadAttributes;
    type ExecutionData = ExecutionData;

    fn block_to_payload(
        block: SealedBlock<
            <<Self::BuiltPayload as BuiltPayload>::Primitives as NodePrimitives>::Block,
        >,
    ) -> Self::ExecutionData {
        let (payload, sidecar) =
            ExecutionPayload::from_block_unchecked(block.hash(), &block.into_block());
        ExecutionData { payload, sidecar }
    }
}
