//! The [`RaylsChainSpec`] wrapper: a reth [`ChainSpec`] plus Rayls base-fee policy.

use std::sync::Arc;

use alloy::{
    consensus::BlockHeader,
    eips::{
        eip1559::{calc_next_block_base_fee, BaseFeeParams},
        eip7840::BlobParams,
    },
    genesis::Genesis,
    primitives::{B256, U256},
};
use alloy_evm::eth::spec::EthExecutorSpec;
use core::fmt::Display;
use rayls_infrastructure_types::{
    Address, RaylsNetwork, MIN_PROTOCOL_BASE_FEE, MIN_RAYLS_PROTOCOL_BASE_FEE,
};
use reth_chainspec::{
    ChainSpec, DepositContract, EthChainSpec, EthereumHardfork, EthereumHardforks, ForkCondition,
    ForkFilter, ForkId, Hardfork, Hardforks, Head,
};
use reth_network_peers::NodeRecord;

use super::{fork::RaylsHardFork, hardforks::RaylsHardforks, schedule::ScheduledFork};

/// Rayls ChainSpec wrapper with dynamic base fee and custom hardforks.
#[derive(Debug, Clone)]
pub struct RaylsChainSpec {
    inner: Arc<ChainSpec>,
    base_fee_params: BaseFeeParams,
    min_base_fee: u64,
}

impl RaylsChainSpec {
    /// Create a builder from an existing chain spec.
    pub fn builder(chain_spec: Arc<ChainSpec>) -> RaylsChainSpecBuilder {
        RaylsChainSpecBuilder::new(chain_spec)
    }

    /// Wrap a chain spec without dynamic base fee or custom hardforks.
    pub fn new(inner: Arc<ChainSpec>) -> Self {
        Self {
            inner,
            base_fee_params: BaseFeeParams::ethereum(),
            min_base_fee: MIN_RAYLS_PROTOCOL_BASE_FEE,
        }
    }

    /// Return the minimum base fee floor.
    pub fn min_base_fee(&self) -> u64 {
        self.min_base_fee
    }

    /// Return the EIP-1559 base fee parameters.
    pub fn rayls_base_fee_params(&self) -> BaseFeeParams {
        self.base_fee_params
    }

    /// Return a reference to the inner chain spec.
    pub fn inner(&self) -> &Arc<ChainSpec> {
        &self.inner
    }

    /// Compute the next block's base fee from parent header fields.
    pub fn compute_next_base_fee(
        &self,
        parent_gas_used: u64,
        parent_gas_limit: u64,
        parent_base_fee: Option<u64>,
        next_block_number: u64,
    ) -> u64 {
        if self.is_eip1559_active_at_block(next_block_number) {
            let current = parent_base_fee.unwrap_or(self.min_base_fee);
            calc_next_block_base_fee(
                parent_gas_used,
                parent_gas_limit,
                current,
                self.base_fee_params,
            )
            .max(self.min_base_fee)
        } else {
            MIN_PROTOCOL_BASE_FEE
        }
    }
}

/// Builder for [`RaylsChainSpec`].
#[derive(Debug)]
pub struct RaylsChainSpecBuilder {
    inner: ChainSpec,
    base_fee_params: BaseFeeParams,
    min_base_fee: u64,
}

impl RaylsChainSpecBuilder {
    fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            inner: (*chain_spec).clone(),
            base_fee_params: BaseFeeParams::ethereum(),
            min_base_fee: MIN_RAYLS_PROTOCOL_BASE_FEE,
        }
    }

    /// Apply the baked-in hardfork schedule for the given network.
    pub fn add_rayls_hardforks_by_type(mut self, network: RaylsNetwork) -> Self {
        for entry in RaylsHardFork::for_network(network) {
            self.inner.hardforks.insert(entry.fork, entry.condition);
        }
        self
    }

    /// Apply an explicit hardfork schedule, as resolved from an external
    /// network config file. Forks absent from the schedule stay `Never`.
    pub fn add_rayls_hardforks_by_schedule(
        mut self,
        schedule: impl IntoIterator<Item = ScheduledFork>,
    ) -> Self {
        for entry in schedule {
            self.inner.hardforks.insert(entry.fork, entry.condition);
        }
        self
    }

    /// Activate EIP-1559 dynamic base fee at `block`.
    pub fn eip1559(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::Eip1559, ForkCondition::Block(block));
        self
    }

    /// Activate BatchDigestV2 at `block`.
    pub fn batch_digest_v2(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::BatchDigestV2, ForkCondition::Block(block));
        self
    }

    /// Activate EmptyOutputBlock at `block`.
    pub fn empty_output_block(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::EmptyOutputBlock, ForkCondition::Block(block));
        self
    }

    /// Activate AdminTransfer at `block`.
    pub fn admin_transfer(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::AdminTransfer, ForkCondition::Block(block));
        self
    }

    /// Activate PrecompileGasFix at `block`.
    pub fn precompile_gas_fix(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::PrecompileGasFix, ForkCondition::Block(block));
        self
    }

    /// Activate Erc20PrecompileBytecode at `block`.
    pub fn erc20_precompile_bytecode(mut self, block: u64) -> Self {
        self.inner
            .hardforks
            .insert(RaylsHardFork::Erc20PrecompileBytecode, ForkCondition::Block(block));
        self
    }

    /// Activate DynamicCommitteeSizing at `block`.
    pub fn dynamic_committee_sizing(mut self, block: u64) -> Self {
        self.inner
            .hardforks
            .insert(RaylsHardFork::DynamicCommitteeSizing, ForkCondition::Block(block));
        self
    }

    /// Activate HybridRewards at `block` (synthetic schedules / fork-boundary tests).
    pub fn hybrid_rewards(mut self, block: u64) -> Self {
        self.inner.hardforks.insert(RaylsHardFork::HybridRewards, ForkCondition::Block(block));
        self
    }

    /// Activate OutputSeqNormalization at `block`.
    pub fn output_seq_normalization(mut self, block: u64) -> Self {
        self.inner
            .hardforks
            .insert(RaylsHardFork::OutputSeqNormalization, ForkCondition::Block(block));
        self
    }

    /// Set the minimum EIP-1559 base fee floor.
    pub fn min_base_fee(mut self, min_base_fee: u64) -> Self {
        self.min_base_fee = min_base_fee;
        self
    }

    /// Set the EIP-1559 base fee parameters.
    pub fn base_fee_params(mut self, params: BaseFeeParams) -> Self {
        self.base_fee_params = params;
        self
    }

    /// Finalize into a [`RaylsChainSpec`].
    pub fn build(self) -> RaylsChainSpec {
        RaylsChainSpec {
            inner: Arc::new(self.inner),
            base_fee_params: self.base_fee_params,
            min_base_fee: self.min_base_fee,
        }
    }
}

impl core::ops::Deref for RaylsChainSpec {
    type Target = ChainSpec;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl EthChainSpec for RaylsChainSpec {
    type Header = alloy::consensus::Header;

    fn chain(&self) -> reth_chainspec::Chain {
        self.inner.chain
    }

    fn base_fee_params_at_timestamp(&self, timestamp: u64) -> BaseFeeParams {
        self.inner.base_fee_params_at_timestamp(timestamp)
    }

    fn blob_params_at_timestamp(&self, timestamp: u64) -> Option<BlobParams> {
        EthChainSpec::blob_params_at_timestamp(self.inner.as_ref(), timestamp)
    }

    fn deposit_contract(&self) -> Option<&DepositContract> {
        self.inner.deposit_contract.as_ref()
    }

    fn genesis_hash(&self) -> B256 {
        self.inner.genesis_hash()
    }

    fn prune_delete_limit(&self) -> usize {
        self.inner.prune_delete_limit
    }

    fn display_hardforks(&self) -> Box<dyn Display> {
        Box::new(ChainSpec::display_hardforks(&self.inner))
    }

    fn genesis_header(&self) -> &Self::Header {
        self.inner.genesis_header()
    }

    fn genesis(&self) -> &Genesis {
        self.inner.genesis()
    }

    fn bootnodes(&self) -> Option<Vec<NodeRecord>> {
        self.inner.bootnodes()
    }

    fn is_optimism(&self) -> bool {
        // Rayls is a standard EVM chain, not on Optimism's OP stack.
        false
    }

    fn final_paris_total_difficulty(&self) -> Option<U256> {
        self.inner.paris_block_and_final_difficulty.map(|(_, final_difficulty)| final_difficulty)
    }

    /// Compute next block base fee. Post-fork: per-block EIP-1559 from the parent.
    /// Pre-fork: fixed at `MIN_PROTOCOL_BASE_FEE`.
    fn next_block_base_fee(&self, parent: &Self::Header, _target_timestamp: u64) -> Option<u64> {
        Some(self.compute_next_base_fee(
            parent.gas_used(),
            parent.gas_limit(),
            parent.base_fee_per_gas(),
            parent.number() + 1,
        ))
    }
}

impl Hardforks for RaylsChainSpec {
    fn fork<H: Hardfork>(&self, fork: H) -> ForkCondition {
        self.inner.fork(fork)
    }

    fn forks_iter(&self) -> impl Iterator<Item = (&dyn Hardfork, ForkCondition)> {
        self.inner.forks_iter()
    }

    fn fork_id(&self, head: &Head) -> ForkId {
        self.inner.fork_id(head)
    }

    fn latest_fork_id(&self) -> ForkId {
        self.inner.latest_fork_id()
    }

    fn fork_filter(&self, head: Head) -> ForkFilter {
        self.inner.fork_filter(head)
    }
}

impl EthereumHardforks for RaylsChainSpec {
    fn ethereum_fork_activation(&self, fork: EthereumHardfork) -> ForkCondition {
        self.inner.ethereum_fork_activation(fork)
    }
}

impl EthExecutorSpec for RaylsChainSpec {
    fn deposit_contract_address(&self) -> Option<Address> {
        self.inner.deposit_contract.as_ref().map(|deposit_contract| deposit_contract.address)
    }
}
