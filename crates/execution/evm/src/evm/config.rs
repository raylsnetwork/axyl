//! Provide config and implement traits to bridge protocol extensions to Reth.
//!
//! Inspired by: crates/ethereum/evm/src/lib.rs

use super::{
    RaylsBlockAssembler, RaylsBlockExecutionCtx, RaylsBlockExecutorFactory, RaylsEvmFactory,
};
use crate::{
    chainspec::RaylsHardforks, error::RaylsRethError, traits::RaylsPrimitives, RaylsChainSpec,
};
use alloy::eips::eip7685::EMPTY_REQUESTS_HASH;
use rayls_infrastructure_types::{
    payload::RLPayload, Address, BlockHeader as _, SealedBlock, SealedHeader, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use rayls_middleware_rewards::RewardsCounter;
use reth_chainspec::{EthChainSpec as _, EthereumHardforks as _};
use reth_evm::{ConfigureEvm, EvmEnv, EvmEnvFor, ExecutionCtxFor};
use reth_evm_ethereum::RethReceiptBuilder;
use reth_primitives::{BlockTy, HeaderTy};
use reth_primitives_traits::constants::MAX_TX_GAS_LIMIT_OSAKA;
use reth_revm::{
    context::{BlockEnv, CfgEnv},
    context_interface::block::BlobExcessGasAndPrice,
};
use std::{collections::BTreeMap, sync::Arc};

/// Rayls-related EVM configuration.
#[derive(Debug, Clone)]
pub struct RaylsEvmConfig<ChainSpec = RaylsChainSpec> {
    /// Inner [`RaylsBlockExecutorFactory`].
    pub executor_factory:
        RaylsBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, RaylsEvmFactory>,
    /// EVM factory for creating EVMs with inspectors.
    pub evm_factory: RaylsEvmFactory,
    /// Ethereum block assembler.
    pub block_assembler: RaylsBlockAssembler<ChainSpec>,
    /// Counter that resolves epoch leader counts from the consensus DB.
    pub rewards_counter: RewardsCounter,
}

impl RaylsEvmConfig {
    /// Creates a new Rayls EVM configuration with the given chain spec.
    pub fn new(chain_spec: Arc<RaylsChainSpec>, rewards_counter: RewardsCounter) -> Self {
        let evm_factory = RaylsEvmFactory::default();
        Self {
            block_assembler: RaylsBlockAssembler::new(chain_spec.clone()),
            executor_factory: RaylsBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                evm_factory,
            ),
            evm_factory,
            rewards_counter,
        }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<RaylsChainSpec> {
        self.executor_factory.spec()
    }

    /// Resolve ommers_hash and requests_hash for the given block number and batch digest.
    ///
    /// Pre-BatchDigestV2: batch digest in ommers_hash, requests_hash empty.
    /// Post-BatchDigestV2: batch digest in requests_hash, ommers_hash empty root.
    fn resolve_batch_digest_fields(
        &self,
        block_number: u64,
        batch_digest: B256,
    ) -> (B256, Option<B256>) {
        if self.chain_spec().is_batch_digest_v2_active_at_block(block_number) {
            (EMPTY_OMMER_ROOT_HASH, Some(batch_digest))
        } else {
            (batch_digest, Some(EMPTY_REQUESTS_HASH))
        }
    }
}

// reth-evm
impl ConfigureEvm for RaylsEvmConfig {
    type Primitives = RaylsPrimitives;

    type Error = RaylsRethError;

    type NextBlockEnvCtx = RLPayload;

    type BlockExecutorFactory =
        RaylsBlockExecutorFactory<RethReceiptBuilder, Arc<RaylsChainSpec>, RaylsEvmFactory>;

    type BlockAssembler = RaylsBlockAssembler<RaylsChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &HeaderTy<Self::Primitives>) -> Result<EvmEnv, Self::Error> {
        let spec = reth_evm_ethereum::revm_spec(self.chain_spec(), header);

        // configure evm env based on parent block
        let mut cfg_env = CfgEnv::new()
            .with_chain_id(self.chain_spec().chain().id())
            .with_spec_and_mainnet_gas_params(spec);

        let blob_params = self.chain_spec().blob_params_at_timestamp(header.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg_env.set_max_blobs_per_tx(blob_params.max_blobs_per_tx);
        }

        if self.chain_spec().is_osaka_active_at_timestamp(header.timestamp) {
            cfg_env.tx_gas_limit_cap = Some(MAX_TX_GAS_LIMIT_OSAKA);
        }

        // derive the EIP-4844 blob fees from the header's `excess_blob_gas` and the current
        // blobparams
        let blob_excess_gas_and_price = header
            .excess_blob_gas
            .zip(self.chain_spec().blob_params_at_timestamp(header.timestamp))
            .map(|(excess_blob_gas, params)| {
                let blob_gasprice = params.calc_blob_fee(excess_blob_gas);
                BlobExcessGasAndPrice { excess_blob_gas, blob_gasprice }
            });

        let block_env = BlockEnv {
            number: U256::from(header.number()),
            beneficiary: header.beneficiary(),
            timestamp: U256::from(header.timestamp()),
            difficulty: U256::ZERO,
            prevrandao: header.mix_hash(),
            gas_limit: header.gas_limit(),
            basefee: header.base_fee_per_gas().unwrap_or_default(),
            blob_excess_gas_and_price,
        };

        Ok(EvmEnv { cfg_env, block_env })
    }

    fn next_evm_env(
        &self,
        parent: &HeaderTy<Self::Primitives>,
        payload: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        // ensure we're not missing any timestamp based hardforks
        let spec_id = reth_evm_ethereum::revm_spec_by_timestamp_and_block_number(
            self.chain_spec(),
            payload.timestamp,
            parent.number() + 1,
        );

        // configure evm env based on parent block
        let mut cfg = CfgEnv::new()
            .with_chain_id(self.chain_spec().chain().id())
            .with_spec_and_mainnet_gas_params(spec_id);

        let blob_params = self.chain_spec().blob_params_at_timestamp(payload.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg.set_max_blobs_per_tx(blob_params.max_blobs_per_tx);
        }

        if self.chain_spec().is_osaka_active_at_timestamp(payload.timestamp) {
            cfg.tx_gas_limit_cap = Some(MAX_TX_GAS_LIMIT_OSAKA);
        }

        let block_env = BlockEnv {
            number: U256::from(parent.number + 1),
            beneficiary: payload.beneficiary,
            timestamp: U256::from(payload.timestamp),
            difficulty: U256::from(payload.batch_index),
            prevrandao: Some(payload.prev_randao()),
            gas_limit: payload.gas_limit,
            basefee: payload.base_fee_per_gas,
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,       // no excess gas for blobs
                blob_gasprice: u128::MAX, // eip4844 transactions are ignored
            }),
        };

        let evm_env = EvmEnv::new(cfg, block_env);

        Ok(evm_env)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        // Parse extra_data by length: 32 bytes = epoch close hash, anything else = not a close
        let close_epoch = if block.extra_data.len() == 32 {
            Some(B256::from_slice(block.extra_data.as_ref()))
        } else {
            None
        };
        let nonce: u64 = block.nonce.into();
        let close_epoch_tally = self.compute_close_epoch_tally(close_epoch, nonce)?;
        Ok(RaylsBlockExecutionCtx {
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            nonce,
            ommers_hash: block.ommers_hash,
            requests_hash: block.requests_hash,
            close_epoch,
            difficulty: block.difficulty,
            rewards_counter: self.rewards_counter.clone(),
            close_epoch_tally,
        })
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<HeaderTy<Self::Primitives>>,
        payload: Self::NextBlockEnvCtx,
    ) -> Result<ExecutionCtxFor<'_, Self>, Self::Error> {
        let next_block = parent.number + 1;
        let (ommers_hash, requests_hash) =
            self.resolve_batch_digest_fields(next_block, payload.batch_digest);
        let close_epoch_tally =
            self.compute_close_epoch_tally(payload.close_epoch, payload.nonce)?;

        Ok(RaylsBlockExecutionCtx {
            parent_hash: parent.hash(),
            parent_beacon_block_root: payload.parent_beacon_block_root(),
            nonce: payload.nonce,
            ommers_hash,
            requests_hash,
            close_epoch: payload.close_epoch,
            difficulty: U256::from(payload.batch_index << 16 | payload.worker_id as usize),
            rewards_counter: self.rewards_counter.clone(),
            close_epoch_tally,
        })
    }
}

impl RaylsEvmConfig {
    /// Compute the close-epoch leader tally exactly once per block construction.
    ///
    /// Returns `None` for non-boundary blocks. On boundary blocks, walks the
    /// consensus DB via the calculator and produces the address-keyed map that
    /// will feed both `applyIncentives` and `withdrawals_root`.
    fn compute_close_epoch_tally(
        &self,
        close_epoch: Option<B256>,
        nonce: u64,
    ) -> Result<Option<BTreeMap<Address, u32>>, RaylsRethError> {
        if close_epoch.is_none() {
            return Ok(None);
        }
        let (epoch, last_round) = rayls_infrastructure_types::nonce::unpack_nonce(nonce);
        let tally = self
            .rewards_counter
            .tally(epoch, last_round)
            .map_err(|e| RaylsRethError::EVMCustom(format!("rewards tally: {e}")))?;
        Ok(Some(tally))
    }
}
