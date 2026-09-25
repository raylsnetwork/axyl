//! Rayls hardfork queries and the schedule-backed [`RaylsChainHardforks`] implementation.

use std::sync::Arc;

use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::ForkCondition;

use super::{fork::RaylsHardFork, schedule::ScheduledFork, spec::RaylsChainSpec};

/// Sorted hardfork schedule usable without a full [`RaylsChainSpec`].
#[derive(Debug, Clone)]
pub struct RaylsChainHardforks {
    forks: Vec<ScheduledFork>,
}

impl RaylsChainHardforks {
    /// Create from an iterator of schedule entries, sorted by fork.
    pub fn new(forks: impl IntoIterator<Item = ScheduledFork>) -> Self {
        let mut forks = forks.into_iter().collect::<Vec<_>>();
        forks.sort();
        debug_assert!(
            forks.windows(2).all(|w| w[0].fork != w[1].fork),
            "RaylsChainHardforks: schedule contains duplicate fork {:?}",
            forks.windows(2).find(|w| w[0].fork == w[1].fork).map(|w| w[0].fork)
        );
        Self { forks }
    }

    /// Create with devnet schedule.
    pub fn devnet() -> Self {
        Self::new(RaylsHardFork::devnet())
    }

    /// Create with testnet schedule.
    pub fn testnet() -> Self {
        Self::new(RaylsHardFork::testnet())
    }

    /// Create with mainnet schedule.
    pub fn mainnet() -> Self {
        Self::new(RaylsHardFork::mainnet())
    }

    /// Create with local schedule (first four hardforks active at genesis).
    pub fn local() -> Self {
        Self::new(RaylsHardFork::local())
    }

    /// Create with the schedule for the given network.
    pub fn for_network(network: RaylsNetwork) -> Self {
        Self::new(RaylsHardFork::for_network(network))
    }
}

impl RaylsHardforks for RaylsChainHardforks {
    fn rayls_fork_activation(&self, fork: RaylsHardFork) -> ForkCondition {
        // Compares on `fork` only: correct because a schedule never lists the same fork
        // twice (sorting would be ambiguous for duplicates).
        self.forks
            .binary_search_by(|entry| entry.fork.cmp(&fork))
            .ok()
            .map(|idx| self.forks[idx].condition)
            .unwrap_or(ForkCondition::Never)
    }
}

/// Rayls hardfork queries, mirroring [`reth_chainspec::EthereumHardforks`].
pub trait RaylsHardforks {
    /// Return the activation condition for a Rayls hardfork.
    fn rayls_fork_activation(&self, fork: RaylsHardFork) -> ForkCondition;

    /// Return true if `fork` is active at `block_number`.
    fn is_rayls_fork_active_at_block(&self, fork: RaylsHardFork, block_number: u64) -> bool {
        self.rayls_fork_activation(fork).active_at_block(block_number)
    }

    /// Return true if the EIP-1559 fork is active at `block`.
    fn is_eip1559_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::Eip1559, block)
    }

    /// Return true if the BatchDigestV2 fork is active at `block`.
    fn is_batch_digest_v2_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::BatchDigestV2, block)
    }

    /// Return true if the AdminTransfer fork is active at `block`.
    fn is_admin_transfer_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::AdminTransfer, block)
    }

    /// Return true if the PrecompileGasFix fork is active at `block`.
    fn is_precompile_gas_fix_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::PrecompileGasFix, block)
    }

    /// Return true if the Erc20PrecompileBytecode fork is active at `block`.
    fn is_erc20_precompile_bytecode_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::Erc20PrecompileBytecode, block)
    }

    /// Return true if the RlsStorage fork is active at `block`.
    fn is_rls_storage_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::RlsStorage, block)
    }

    /// Return true if the Tokenomics fork is active at `block`.
    fn is_tokenomics_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::Tokenomics, block)
    }

    /// Return true only for the testnet reward-distribution outage window.
    ///
    /// A misconfigured tokenomics activation left rewards off for this block range
    /// on the live testnet, so archive replay skips on-chain reward distribution
    /// here to match canonical state. Mainnet/devnet never match (tokenomics is not
    /// scheduled at the testnet block), so they distribute unconditionally.
    #[cfg(feature = "archive-replay")]
    fn is_tokenomics_outage_block(&self, block: u64) -> bool {
        // Only the live testnet matches: its Tokenomics fork activates at block 1_879_000.
        matches!(
            self.rayls_fork_activation(RaylsHardFork::Tokenomics),
            ForkCondition::Block(1_879_000)
        ) && (
            // First testnet block whose epoch close skipped on-chain reward distribution;
            // the exclusive upper bound is the first block where distribution resumed.
            // Spans the epoch closes the live network produced with rewards disabled;
            // extend if a later epoch close still diverges on re-execution.
            2_879_900..2_949_655
        )
            .contains(&block)
    }

    /// Return true if the UUPS fork is active at `block`.
    fn is_uups_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::Uups, block)
    }

    /// Return true if the TransactionLoadBalancing fork is active at `block`.
    fn is_transaction_load_balancing_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::TransactionLoadBalancing, block)
    }

    /// Return true if the UsdrSupplyCorrection fork is active at `block`.
    fn is_usdr_supply_correction_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::UsdrSupplyCorrection, block)
    }

    /// Return true if the SenderAffinityLoadBalancing fork is active at `block`.
    fn is_sender_affinity_load_balancing_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::SenderAffinityLoadBalancing, block)
    }

    /// Return true if the EmptyOutputBlock fork is active at `block`.
    fn is_empty_output_block_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::EmptyOutputBlock, block)
    }

    /// Return true if the DynamicCommitteeSizing fork is active at `block`.
    fn is_dynamic_committee_sizing_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::DynamicCommitteeSizing, block)
    }

    /// Return true if the HybridRewards fork is active at `block`.
    ///
    /// Gates both the ConsensusRegistry bytecode swap (the migration) and the epoch-close
    /// reward ABI: an epoch whose close block satisfies this uses the 2-arg hybrid
    /// `applyIncentives`; earlier closes use the 1-arg leader-only path.
    fn is_hybrid_rewards_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::HybridRewards, block)
    }

    /// Return true if the OutputSeqNormalization fork is active at `block`.
    fn is_output_seq_normalization_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::OutputSeqNormalization, block)
    }

    /// Return the max version byte among the forks active at `block`, if any.
    fn version_byte_at_block(&self, block: u64) -> Option<u8> {
        RaylsHardFork::VARIANTS
            .iter()
            .filter(|fork| self.rayls_fork_activation(**fork).active_at_block(block))
            .map(|fork| fork.version_byte())
            .max()
    }

    /// Return forks that activated between `prev_block` (exclusive) and `block` (inclusive).
    fn newly_activated_forks(&self, prev_block: u64, block: u64) -> Vec<RaylsHardFork> {
        RaylsHardFork::VARIANTS
            .iter()
            .filter(|fork| {
                let condition = self.rayls_fork_activation(**fork);
                !condition.active_at_block(prev_block) && condition.active_at_block(block)
            })
            .copied()
            .collect()
    }
}

impl RaylsHardforks for RaylsChainSpec {
    fn rayls_fork_activation(&self, fork: RaylsHardFork) -> ForkCondition {
        self.inner().fork(fork)
    }
}

impl<T: RaylsHardforks> RaylsHardforks for &T {
    fn rayls_fork_activation(&self, fork: RaylsHardFork) -> ForkCondition {
        (**self).rayls_fork_activation(fork)
    }
}

impl<T: RaylsHardforks> RaylsHardforks for Arc<T> {
    fn rayls_fork_activation(&self, fork: RaylsHardFork) -> ForkCondition {
        (**self).rayls_fork_activation(fork)
    }
}
