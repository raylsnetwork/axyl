//! Rayls ChainSpec wrapper with dynamic base fee and custom hardforks.

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
use core::fmt::{Debug, Display};
use rayls_infrastructure_types::{
    Address, RaylsNetwork, MIN_PROTOCOL_BASE_FEE, MIN_RAYLS_PROTOCOL_BASE_FEE,
};
use reth_chainspec::{
    hardfork, ChainSpec, DepositContract, EthChainSpec, EthereumHardfork, EthereumHardforks,
    ForkCondition, ForkFilter, ForkId, Hardfork, Hardforks, Head,
};
use reth_network_peers::NodeRecord;
use std::sync::Arc;

pub type RethChainSpec = RaylsChainSpec;

hardfork!(
    /// Rayls Network hardforks.
    RaylsHardFork {
        /// EIP-1559 dynamic base fee activation.
        Eip1559,
        /// Move batch_digest from ommers_hash to requests_hash for go-ethereum compatibility.
        BatchDigestV2,
        /// Transfer admin roles from inaccessible admin to new admin via storage overrides.
        AdminTransfer,
        /// Fix NativeErc20Inspector gas accounting for contract-to-precompile calls.
        PrecompileGasFix,
        /// Deploy ERC1967Proxy bytecode at the RLS token address (missing from testnet genesis).
        RlsStorage,
        /// Enable epoch-end reward distribution via RewardDistributor system call.
        Tokenomics,
        /// Fix contracts upgradability
        Uups,
        /// Seed STOP bytecode at the native ERC-20 precompile to prevent EIP-161 cleanup.
        Erc20PrecompileBytecode,
        /// Hash full tx bytes with FxHasher for committee-slot dispatch, replacing the first-8-bytes-as-u64 prefix.
        TransactionLoadBalancing,
        /// Rebase the USDR precompile's TOTAL_SUPPLY slot to match the true sum of native balances.
        UsdrSupplyCorrection,
        /// Produce a fallback empty block for any consensus output that contributed no block
        /// (no batches, all deduped, or all parked), so every output maps to a block.
        EmptyOutputBlock,
        /// Size the next epoch's committee based on Active validators (Active + PendingActivation)
        /// instead of reusing the current epoch's fixed committee size.
        DynamicCommitteeSizing,
        /// Swap ConsensusRegistry to the hybrid-reward (participation + anchor + stake) bytecode
        /// and switch epoch-close reward distribution to the 2-arg `applyIncentives` ABI.
        HybridRewards,
    }
);

/// EIP-1559 activation block on the Rayls devnet.
pub const DEVNET_EIP1559_BLOCK: u64 = 50;
/// EIP-1559 activation block on the Rayls testnet.
pub const TESTNET_EIP1559_BLOCK: u64 = 281800;
/// EIP-1559 activation block on Rayls mainnet.
pub const MAINNET_EIP1559_BLOCK: u64 = 0;
/// EIP-1559 activation block on local network.
pub const LOCAL_EIP1559_BLOCK: u64 = 0;

/// BatchDigestV2 activation block on Rayls devnet.
pub const DEVNET_BATCH_DIGEST_V2_BLOCK: u64 = 100;
/// BatchDigestV2 activation block on Rayls testnet.
pub const TESTNET_BATCH_DIGEST_V2_BLOCK: u64 = 560539;
/// BatchDigestV2 activation block on Rayls mainnet.
pub const MAINNET_BATCH_DIGEST_V2_BLOCK: u64 = 0;
/// BatchDigestV2 activation block on local network.
pub const LOCAL_BATCH_DIGEST_V2_BLOCK: u64 = 0;

/// AdminTransfer activation block on Rayls devnet.
pub const DEVNET_ADMIN_TRANSFER_BLOCK: u64 = 150;
/// AdminTransfer activation block on Rayls testnet.
pub const TESTNET_ADMIN_TRANSFER_BLOCK: u64 = 560539;
/// AdminTransfer activation block on local network.
pub const LOCAL_ADMIN_TRANSFER_BLOCK: u64 = 0;

/// PrecompileGasFix activation block on Rayls devnet.
pub const DEVNET_PRECOMPILE_GAS_FIX_BLOCK: u64 = 150;
/// PrecompileGasFix activation block on Rayls testnet.
pub const TESTNET_PRECOMPILE_GAS_FIX_BLOCK: u64 = 900000;
/// PrecompileGasFix activation block on Rayls mainnet.
pub const MAINNET_PRECOMPILE_GAS_FIX_BLOCK: u64 = 0;
/// PrecompileGasFix activation block on local network.
pub const LOCAL_PRECOMPILE_GAS_FIX_BLOCK: u64 = 0;

/// RlsProxyDeploy activation block on Rayls devnet.
pub const DEVNET_RLS_STORAGE_BLOCK: u64 = 200;
/// RlsProxyDeploy activation block on Rayls testnet.
pub const TESTNET_RLS_STORAGE_BLOCK: u64 = 900000;

/// Tokenomics activation block on Rayls devnet.
pub const DEVNET_TOKENOMICS_BLOCK: u64 = 250; // TODO: set to actual devnet block before deploy
/// Tokenomics activation block on Rayls testnet.
pub const TESTNET_TOKENOMICS_BLOCK: u64 = 1879000;

/// First testnet block whose epoch close skipped on-chain reward distribution
/// because tokenomics was misconfigured-inactive for this range on the live network.
#[cfg(feature = "archive-replay")]
pub const TESTNET_TOKENOMICS_OUTAGE_START_BLOCK: u64 = 2_879_900;
/// First testnet block where reward distribution resumed (exclusive upper bound).
/// Spans the epoch closes the live network produced with rewards disabled;
/// extend if a later epoch close still diverges on re-execution.
#[cfg(feature = "archive-replay")]
pub const TESTNET_TOKENOMICS_OUTAGE_END_BLOCK: u64 = 2_949_655;

/// UUPS activation block on Rayls devnet.
pub const DEVNET_UUPS_BLOCK: u64 = 2_506_000;
/// UUPS activation block on Rayls testnet.
pub const TESTNET_UUPS_BLOCK: u64 = 2_872_000;

/// Erc20PrecompileBytecode activation block on Rayls devnet.
pub const DEVNET_ERC20_PRECOMPILE_BYTECODE_BLOCK: u64 = 1_542_796;
/// Erc20PrecompileBytecode activation block on Rayls mainnet.
pub const MAINNET_ERC20_PRECOMPILE_BYTECODE_BLOCK: u64 = 893_558;
/// Erc20PrecompileBytecode activation block on local network.
///
/// One-shot migrations fire when the chain transitions *across* their
/// activation block (parent_number → block_number). A fork set to `Block(0)`
/// is treated as already-active at genesis and the migration body is never
/// executed (see the synthetic_schedule(1000) pattern in
/// `hardforks/mod.rs` tests). To make the STOP-bytecode install actually
/// run on a fresh local chain — which is required for the precompile's
/// TOTAL_SUPPLY slot to survive EIP-161 — this MUST be ≥ 1.
pub const LOCAL_ERC20_PRECOMPILE_BYTECODE_BLOCK: u64 = 1;

/// Load Balancing activation block on Rayls devnet.
pub const DEVNET_LOAD_BALANCING_BLOCK: u64 = 1_542_796;
/// Load Balancing activation block on Rayls testnet.
pub const TESTNET_LOAD_BALANCING_BLOCK: u64 = 4_386_290;
/// Load Balancing activation block on Rayls mainnet.
pub const MAINNET_LOAD_BALANCING_BLOCK: u64 = 893_558;
/// Load Balancing activation block on local network.
pub const LOCAL_LOAD_BALANCING_BLOCK: u64 = 0;

// NOTE: UsdrSupplyCorrection is active on local and mainnet; testnet/devnet
// stay `Never` until an activation block is chosen operationally. Flip the
// relevant network entry in the schedule below from `ForkCondition::Never` to
// `ForkCondition::Block(<chosen block>)` when ready. See
// `crates/execution/evm/src/evm/hardforks/usdr_supply_correction.rs`.

/// UsdrSupplyCorrection activation block on the Rayls mainnet.
pub const MAINNET_USDR_SUPPLY_CORRECTION_BLOCK: u64 = 3_569_194;

/// UsdrSupplyCorrection activation block on the local sandbox network.
///
/// Used for manual end-to-end testing of the hardfork (start chain → mint/burn
/// → wait for activation → verify totalSupply). Block 100 hits ~7 s into a
/// fresh local devnet at the 4-validator DAG cadence (≈15 EVM blocks/sec),
/// so the pre-fork window is short but enough to run one mint/burn round;
/// activation lands quickly and post-fork mint/burn can then be tested
/// without a long wait.
pub const LOCAL_USDR_SUPPLY_CORRECTION_BLOCK: u64 = 100;

/// EmptyOutputBlock activation block on the Rayls devnet (active from genesis).
pub const DEVNET_EMPTY_OUTPUT_BLOCK_BLOCK: u64 = 0;

/// EmptyOutputBlock activation block on the Rayls testnet.
pub const TESTNET_EMPTY_OUTPUT_BLOCK_BLOCK: u64 = 6_663_630;

/// EmptyOutputBlock activation block on the Rayls mainnet.
pub const MAINNET_EMPTY_OUTPUT_BLOCK_BLOCK: u64 = 3_569_194;

/// EmptyOutputBlock activation block on the local sandbox network.
pub const LOCAL_EMPTY_OUTPUT_BLOCK_BLOCK: u64 = 0;

/// DynamicCommitteeSizing activation block on the Rayls devnet
pub const DEVNET_DYNAMIC_COMMITTEE_SIZING_BLOCK: u64 = u64::MAX;

/// DynamicCommitteeSizing activation block on the Rayls testnet
pub const TESTNET_DYNAMIC_COMMITTEE_SIZING_BLOCK: u64 = u64::MAX;

/// DynamicCommitteeSizing activation block on the Rayls mainnet
pub const MAINNET_DYNAMIC_COMMITTEE_SIZING_BLOCK: u64 = u64::MAX;

/// DynamicCommitteeSizing activation block on the local sandbox network.
pub const LOCAL_DYNAMIC_COMMITTEE_SIZING_BLOCK: u64 = 0;

/// HybridRewards activation block on the local sandbox network. Block 1 (not 0): genesis
/// deploys the pre-hybrid ConsensusRegistry, and the in-place bytecode-swap migration runs at
/// the first post-genesis block, after which epoch closes use the hybrid `applyIncentives` ABI.
pub const LOCAL_HYBRID_REWARDS_BLOCK: u64 = 1;

impl RaylsHardFork {
    /// Return the protocol version byte for this hardfork.
    pub const fn version_byte(self) -> u8 {
        match self {
            Self::Eip1559 => 0x01,
            Self::BatchDigestV2 => 0x02,
            Self::AdminTransfer => 0x03,
            Self::PrecompileGasFix => 0x04,
            Self::RlsStorage => 0x05,
            Self::Tokenomics => 0x06,
            Self::Uups => 0x07,
            Self::Erc20PrecompileBytecode => 0x08,
            Self::TransactionLoadBalancing => 0x09,
            Self::UsdrSupplyCorrection => 0x0a,
            Self::EmptyOutputBlock => 0x0b,
            Self::DynamicCommitteeSizing => 0x0c,
            Self::HybridRewards => 0x0d,
        }
    }

    /// Devnet hardfork schedule.
    pub const fn devnet() -> [(Self, ForkCondition); 13] {
        [
            (Self::Eip1559, ForkCondition::Block(DEVNET_EIP1559_BLOCK)),
            (Self::BatchDigestV2, ForkCondition::Block(DEVNET_BATCH_DIGEST_V2_BLOCK)),
            (Self::AdminTransfer, ForkCondition::Never),
            (Self::PrecompileGasFix, ForkCondition::Block(DEVNET_PRECOMPILE_GAS_FIX_BLOCK)),
            (Self::RlsStorage, ForkCondition::Never),
            (Self::Tokenomics, ForkCondition::Never),
            (Self::Uups, ForkCondition::Never),
            (
                Self::Erc20PrecompileBytecode,
                ForkCondition::Block(DEVNET_ERC20_PRECOMPILE_BYTECODE_BLOCK),
            ),
            (Self::TransactionLoadBalancing, ForkCondition::Block(DEVNET_LOAD_BALANCING_BLOCK)),
            (Self::UsdrSupplyCorrection, ForkCondition::Never),
            (Self::EmptyOutputBlock, ForkCondition::Block(DEVNET_EMPTY_OUTPUT_BLOCK_BLOCK)),
            (
                Self::DynamicCommitteeSizing,
                ForkCondition::Block(DEVNET_DYNAMIC_COMMITTEE_SIZING_BLOCK),
            ),
            // Never until SRE schedules a concrete devnet activation block.
            (Self::HybridRewards, ForkCondition::Never),
        ]
    }

    /// Testnet hardfork schedule.
    pub const fn testnet() -> [(Self, ForkCondition); 13] {
        [
            (Self::Eip1559, ForkCondition::Block(TESTNET_EIP1559_BLOCK)),
            (Self::BatchDigestV2, ForkCondition::Block(TESTNET_BATCH_DIGEST_V2_BLOCK)),
            (Self::AdminTransfer, ForkCondition::Block(TESTNET_ADMIN_TRANSFER_BLOCK)),
            (Self::PrecompileGasFix, ForkCondition::Block(TESTNET_PRECOMPILE_GAS_FIX_BLOCK)),
            (Self::RlsStorage, ForkCondition::Block(TESTNET_RLS_STORAGE_BLOCK)),
            (Self::Tokenomics, ForkCondition::Block(TESTNET_TOKENOMICS_BLOCK)),
            (Self::Uups, ForkCondition::Block(TESTNET_UUPS_BLOCK)),
            (Self::Erc20PrecompileBytecode, ForkCondition::Never), /* Bytecode is already
                                                                    * present on testnet */
            (Self::TransactionLoadBalancing, ForkCondition::Block(TESTNET_LOAD_BALANCING_BLOCK)),
            (Self::UsdrSupplyCorrection, ForkCondition::Never),
            (Self::EmptyOutputBlock, ForkCondition::Block(TESTNET_EMPTY_OUTPUT_BLOCK_BLOCK)),
            (
                Self::DynamicCommitteeSizing,
                ForkCondition::Block(TESTNET_DYNAMIC_COMMITTEE_SIZING_BLOCK),
            ),
            // Never until SRE schedules a concrete testnet activation block.
            (Self::HybridRewards, ForkCondition::Never),
        ]
    }

    /// Mainnet hardfork schedule.
    pub const fn mainnet() -> [(Self, ForkCondition); 13] {
        [
            (Self::Eip1559, ForkCondition::Block(MAINNET_EIP1559_BLOCK)),
            (Self::BatchDigestV2, ForkCondition::Block(MAINNET_BATCH_DIGEST_V2_BLOCK)),
            (Self::AdminTransfer, ForkCondition::Never),
            (Self::PrecompileGasFix, ForkCondition::Block(MAINNET_PRECOMPILE_GAS_FIX_BLOCK)),
            (Self::RlsStorage, ForkCondition::Never),
            (Self::Tokenomics, ForkCondition::Never),
            (Self::Uups, ForkCondition::Never),
            (
                Self::Erc20PrecompileBytecode,
                ForkCondition::Block(MAINNET_ERC20_PRECOMPILE_BYTECODE_BLOCK),
            ),
            (Self::TransactionLoadBalancing, ForkCondition::Block(MAINNET_LOAD_BALANCING_BLOCK)),
            (
                Self::UsdrSupplyCorrection,
                ForkCondition::Block(MAINNET_USDR_SUPPLY_CORRECTION_BLOCK),
            ),
            (Self::EmptyOutputBlock, ForkCondition::Block(MAINNET_EMPTY_OUTPUT_BLOCK_BLOCK)),
            (
                Self::DynamicCommitteeSizing,
                ForkCondition::Block(MAINNET_DYNAMIC_COMMITTEE_SIZING_BLOCK),
            ),
            // Never until SRE schedules a concrete mainnet activation block (the reward-fairness
            // rollout for #633); the in-place migration re-links BlsG1 from the live contract.
            (Self::HybridRewards, ForkCondition::Never),
        ]
    }

    /// Local network hardfork schedule (first four hardforks active at genesis).
    pub const fn local() -> [(Self, ForkCondition); 13] {
        [
            (Self::Eip1559, ForkCondition::Block(LOCAL_EIP1559_BLOCK)),
            (Self::BatchDigestV2, ForkCondition::Block(LOCAL_BATCH_DIGEST_V2_BLOCK)),
            (Self::AdminTransfer, ForkCondition::Block(LOCAL_ADMIN_TRANSFER_BLOCK)),
            (Self::PrecompileGasFix, ForkCondition::Block(LOCAL_PRECOMPILE_GAS_FIX_BLOCK)),
            (Self::RlsStorage, ForkCondition::Never),
            (Self::Tokenomics, ForkCondition::Never),
            (Self::Uups, ForkCondition::Never),
            (
                Self::Erc20PrecompileBytecode,
                ForkCondition::Block(LOCAL_ERC20_PRECOMPILE_BYTECODE_BLOCK),
            ),
            (Self::TransactionLoadBalancing, ForkCondition::Block(LOCAL_LOAD_BALANCING_BLOCK)),
            (Self::UsdrSupplyCorrection, ForkCondition::Block(LOCAL_USDR_SUPPLY_CORRECTION_BLOCK)),
            (Self::EmptyOutputBlock, ForkCondition::Block(LOCAL_EMPTY_OUTPUT_BLOCK_BLOCK)),
            (
                Self::DynamicCommitteeSizing,
                ForkCondition::Block(LOCAL_DYNAMIC_COMMITTEE_SIZING_BLOCK),
            ),
            (Self::HybridRewards, ForkCondition::Block(LOCAL_HYBRID_REWARDS_BLOCK)),
        ]
    }

    /// Return the hardfork schedule for the given network.
    pub const fn for_network(network: RaylsNetwork) -> [(Self, ForkCondition); 13] {
        match network {
            RaylsNetwork::Devnet => Self::devnet(),
            RaylsNetwork::Testnet => Self::testnet(),
            RaylsNetwork::Mainnet => Self::mainnet(),
            RaylsNetwork::Local => Self::local(),
        }
    }
}

/// Sorted hardfork schedule usable without a full [`RaylsChainSpec`].
#[derive(Debug, Clone)]
pub struct RaylsChainHardforks {
    forks: Vec<(RaylsHardFork, ForkCondition)>,
}

impl RaylsChainHardforks {
    /// Create from an iterator of (fork, condition) pairs.
    pub fn new(forks: impl IntoIterator<Item = (RaylsHardFork, ForkCondition)>) -> Self {
        let mut forks = forks.into_iter().collect::<Vec<_>>();
        forks.sort();
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
        self.forks
            .binary_search_by(|(f, _)| f.cmp(&fork))
            .ok()
            .map(|idx| self.forks[idx].1)
            .unwrap_or(ForkCondition::Never)
    }
}

/// Rayls hardfork queries, mirroring [`EthereumHardforks`].
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

    /// Return true if the PrecompileGasFix fork is active at `block`.
    fn is_precompile_gas_fix_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::PrecompileGasFix, block)
    }

    /// Return true if the Erc20PrecompileBytecode fork is active at `block`.
    fn is_erc20_precompile_bytecode_active_at_block(&self, block: u64) -> bool {
        self.is_rayls_fork_active_at_block(RaylsHardFork::Erc20PrecompileBytecode, block)
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
        matches!(
            self.rayls_fork_activation(RaylsHardFork::Tokenomics),
            ForkCondition::Block(b) if b == TESTNET_TOKENOMICS_BLOCK
        ) && (TESTNET_TOKENOMICS_OUTAGE_START_BLOCK..TESTNET_TOKENOMICS_OUTAGE_END_BLOCK)
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

    /// Return the active version byte at `block`, if any.
    fn version_byte_at_block(&self, block: u64) -> Option<u8> {
        RaylsHardFork::VARIANTS
            .iter()
            .rev()
            .find(|fork| self.rayls_fork_activation(**fork).active_at_block(block))
            .map(|fork| fork.version_byte())
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
        self.inner.fork(fork)
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
    pub fn rayls_hardforks(mut self, network: RaylsNetwork) -> Self {
        for (fork, condition) in RaylsHardFork::for_network(network) {
            self.inner.hardforks.insert(fork, condition);
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
        false
    }

    fn final_paris_total_difficulty(&self) -> Option<U256> {
        self.inner.paris_block_and_final_difficulty.map(|(_, final_difficulty)| final_difficulty)
    }

    /// Compute next block base fee. Post-fork: per-block EIP-1559. Pre-fork: epoch-scoped.
    fn next_block_base_fee(&self, parent: &Self::Header, _target_timestamp: u64) -> Option<u64> {
        let next_block = parent.number() + 1;
        if self.is_eip1559_active_at_block(next_block) {
            let parent_base_fee = parent.base_fee_per_gas().unwrap_or(self.min_base_fee);
            Some(
                calc_next_block_base_fee(
                    parent.gas_used(),
                    parent.gas_limit(),
                    parent_base_fee,
                    self.base_fee_params,
                )
                .max(self.min_base_fee),
            )
        } else {
            Some(MIN_PROTOCOL_BASE_FEE)
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_digest_v2_inactive_before_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        assert!(!hardforks.is_batch_digest_v2_active_at_block(DEVNET_BATCH_DIGEST_V2_BLOCK - 1));
    }

    #[test]
    fn batch_digest_v2_active_at_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        assert!(hardforks.is_batch_digest_v2_active_at_block(DEVNET_BATCH_DIGEST_V2_BLOCK));
    }

    #[test]
    fn batch_digest_v2_active_after_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        assert!(hardforks.is_batch_digest_v2_active_at_block(DEVNET_BATCH_DIGEST_V2_BLOCK + 1));
    }

    #[test]
    fn newly_activated_forks_includes_batch_digest_v2() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let activated = hardforks
            .newly_activated_forks(DEVNET_BATCH_DIGEST_V2_BLOCK - 1, DEVNET_BATCH_DIGEST_V2_BLOCK);
        assert!(activated.contains(&RaylsHardFork::BatchDigestV2));
    }

    #[test]
    fn newly_activated_forks_excludes_batch_digest_v2_when_already_active() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        let activated = hardforks
            .newly_activated_forks(DEVNET_BATCH_DIGEST_V2_BLOCK, DEVNET_BATCH_DIGEST_V2_BLOCK + 1);
        assert!(!activated.contains(&RaylsHardFork::BatchDigestV2));
    }

    // ── AdminTransfer activation tests ─────────────────────────────────

    /// Test Never block
    #[test]
    fn admin_transfer_never_activates_on_devnet() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Devnet);
        for block in [0u64, 1, 1_000, 1_000_000] {
            assert!(!hardforks.is_rayls_fork_active_at_block(RaylsHardFork::AdminTransfer, block,));
        }
    }

    #[test]
    fn admin_transfer_active_after_testnet_activation_block() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        assert!(!hardforks.is_rayls_fork_active_at_block(
            RaylsHardFork::AdminTransfer,
            TESTNET_ADMIN_TRANSFER_BLOCK - 1,
        ));
        assert!(hardforks.is_rayls_fork_active_at_block(
            RaylsHardFork::AdminTransfer,
            TESTNET_ADMIN_TRANSFER_BLOCK + 1,
        ));
    }

    #[test]
    fn newly_activated_forks_on_testnet_includes_admin_transfer() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let activated = hardforks
            .newly_activated_forks(TESTNET_ADMIN_TRANSFER_BLOCK - 1, TESTNET_ADMIN_TRANSFER_BLOCK);
        assert!(activated.contains(&RaylsHardFork::AdminTransfer));
    }

    // ── Local network tests ─────────────────────────────────────────────

    #[test]
    fn local_network_first_four_hardforks_active_at_block_0() {
        let hardforks = RaylsChainHardforks::local();
        // Only first 4 hardforks are active for local network (RlsStorage, Tokenomics, Uups are
        // Never)
        let active_forks = [
            RaylsHardFork::Eip1559,
            RaylsHardFork::BatchDigestV2,
            RaylsHardFork::AdminTransfer,
            RaylsHardFork::PrecompileGasFix,
        ];
        for fork in active_forks {
            assert!(
                hardforks.is_rayls_fork_active_at_block(fork, 0),
                "fork {:?} should be active at block 0",
                fork
            );
        }
    }

    #[test]
    fn local_network_last_three_hardforks_never_activate() {
        let hardforks = RaylsChainHardforks::local();
        // Last 3 hardforks are set to Never for local network
        let never_forks =
            [RaylsHardFork::RlsStorage, RaylsHardFork::Tokenomics, RaylsHardFork::Uups];
        for fork in never_forks {
            assert!(
                !hardforks.is_rayls_fork_active_at_block(fork, 0),
                "fork {:?} should never be active",
                fork
            );
            assert!(
                !hardforks.is_rayls_fork_active_at_block(fork, 1_000_000),
                "fork {:?} should never be active",
                fork
            );
        }
    }

    #[test]
    fn local_network_version_byte_at_block_0() {
        let hardforks = RaylsChainHardforks::local();
        let version = hardforks.version_byte_at_block(0);
        // DynamicCommitteeSizing (0x0c) activates at block 0 on local and is the highest such fork.
        assert_eq!(version, Some(0x0c));
    }

    #[test]
    fn local_network_version_byte_advances_at_hybrid_rewards() {
        let hardforks = RaylsChainHardforks::local();
        assert_eq!(
            hardforks.version_byte_at_block(LOCAL_HYBRID_REWARDS_BLOCK - 1),
            Some(0x0c),
            "the block before HybridRewards must still report DynamicCommitteeSizing"
        );
        assert_eq!(
            hardforks.version_byte_at_block(LOCAL_HYBRID_REWARDS_BLOCK),
            Some(0x0d),
            "HybridRewards must own the version byte from its activation block"
        );
        assert_eq!(hardforks.version_byte_at_block(1_000_000), Some(0x0d));
    }

    // ── Schedule and version tests ──────────────────────────────────────

    #[test]
    fn schedule_contains_both_hardforks_for_all_networks() {
        for network in [
            RaylsNetwork::Devnet,
            RaylsNetwork::Testnet,
            RaylsNetwork::Mainnet,
            RaylsNetwork::Local,
        ] {
            let schedule = RaylsHardFork::for_network(network);
            assert_eq!(schedule.len(), 13, "expected 13 hardforks for {network}");
            assert_eq!(schedule[0].0, RaylsHardFork::Eip1559);
            assert_eq!(schedule[1].0, RaylsHardFork::BatchDigestV2);
            assert_eq!(schedule[2].0, RaylsHardFork::AdminTransfer);
            assert_eq!(schedule[3].0, RaylsHardFork::PrecompileGasFix);
            assert_eq!(schedule[4].0, RaylsHardFork::RlsStorage);
            assert_eq!(schedule[5].0, RaylsHardFork::Tokenomics);
            assert_eq!(schedule[6].0, RaylsHardFork::Uups);
            assert_eq!(schedule[7].0, RaylsHardFork::Erc20PrecompileBytecode);
            assert_eq!(schedule[8].0, RaylsHardFork::TransactionLoadBalancing);
            assert_eq!(schedule[9].0, RaylsHardFork::UsdrSupplyCorrection);
            assert_eq!(schedule[10].0, RaylsHardFork::EmptyOutputBlock);
            assert_eq!(schedule[11].0, RaylsHardFork::DynamicCommitteeSizing);
            assert_eq!(schedule[12].0, RaylsHardFork::HybridRewards);
        }
    }

    #[test]
    fn erc20_precompile_bytecode_is_never_on_testnet() {
        let hardforks = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        assert!(
            !hardforks.is_erc20_precompile_bytecode_active_at_block(u64::MAX),
            "testnet bytecode is already present; migration must stay Never",
        );
    }

    #[test]
    fn builder_overrides_batch_digest_v2_activation() {
        let genesis = rayls_infrastructure_types::test_genesis();
        let chain_spec: ChainSpec = genesis.into();
        let spec = RaylsChainSpec::builder(Arc::new(chain_spec))
            .rayls_hardforks(RaylsNetwork::Devnet)
            .batch_digest_v2(999)
            .build();
        assert!(!spec.is_batch_digest_v2_active_at_block(998));
        assert!(spec.is_batch_digest_v2_active_at_block(999));
    }
}
