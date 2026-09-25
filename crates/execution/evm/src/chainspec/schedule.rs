//! Per-network Rayls hardfork schedules: which fork activates at which block.
//!
//! NOTE: `UsdrSupplyCorrection` is active on local and mainnet; testnet/devnet
//! stay `Never` until an activation block is chosen operationally. Flip the
//! relevant network entry in a schedule below from `ForkCondition::Never` to
//! `ForkCondition::Block(<chosen block>)` when ready. See
//! `crates/execution/evm/src/evm/hardforks/usdr_supply_correction.rs`.

use super::fork::RaylsHardFork;
use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::ForkCondition;

/// One entry of a Rayls hardfork schedule: a fork and its activation condition.
///
/// `Ord` compares fork-first, so sorting a schedule yields fork order
/// regardless of the conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduledFork {
    pub fork: RaylsHardFork,
    pub condition: ForkCondition,
}

impl ScheduledFork {
    pub const fn new(fork: RaylsHardFork, condition: ForkCondition) -> Self {
        Self { fork, condition }
    }

    /// Return true if the fork is active at `block`.
    pub fn is_active_at(&self, block: u64) -> bool {
        self.condition.active_at_block(block)
    }

    /// The activation block; `None` for a fork that never activates.
    ///
    /// Rayls schedules are block-based only; a TTD- or timestamp-based
    /// condition fires the debug assert.
    pub fn block_of(&self) -> Option<u64> {
        debug_assert!(
            matches!(self.condition, ForkCondition::Block(_) | ForkCondition::Never),
            "Rayls schedules are block-based only; extend ScheduleRecord before adding \
             TTD/timestamp forks"
        );
        match self.condition {
            ForkCondition::Block(block) => Some(block),
            ForkCondition::Never | ForkCondition::TTD { .. } | ForkCondition::Timestamp(_) => None,
        }
    }
}

impl RaylsHardFork {
    /// Devnet hardfork schedule.
    pub const fn devnet() -> [ScheduledFork; 15] {
        [
            ScheduledFork::new(Self::Eip1559, ForkCondition::Block(50)),
            ScheduledFork::new(Self::BatchDigestV2, ForkCondition::Block(100)),
            // Planned activation: block 150.
            ScheduledFork::new(Self::AdminTransfer, ForkCondition::Never),
            ScheduledFork::new(Self::PrecompileGasFix, ForkCondition::Block(150)),
            // Planned activation: block 200.
            ScheduledFork::new(Self::RlsStorage, ForkCondition::Never),
            // Planned activation: block 250. TODO: set to actual devnet block before deploy.
            ScheduledFork::new(Self::Tokenomics, ForkCondition::Never),
            // Planned activation: block 2_506_000.
            ScheduledFork::new(Self::Uups, ForkCondition::Never),
            ScheduledFork::new(Self::Erc20PrecompileBytecode, ForkCondition::Block(1_542_796)),
            ScheduledFork::new(Self::TransactionLoadBalancing, ForkCondition::Block(1_542_796)),
            ScheduledFork::new(Self::UsdrSupplyCorrection, ForkCondition::Never),
            // Active from genesis.
            ScheduledFork::new(Self::EmptyOutputBlock, ForkCondition::Block(0)),
            // Never until SRE schedules a concrete devnet activation block.
            ScheduledFork::new(Self::DynamicCommitteeSizing, ForkCondition::Never),
            // Never until SRE schedules a concrete devnet activation block.
            ScheduledFork::new(Self::HybridRewards, ForkCondition::Never),
            ScheduledFork::new(Self::OutputSeqNormalization, ForkCondition::Never),
            // Never until an operational activation block is chosen; the mechanism ships dormant.
            ScheduledFork::new(Self::SenderAffinityLoadBalancing, ForkCondition::Never),
        ]
    }

    /// Testnet hardfork schedule.
    pub const fn testnet() -> [ScheduledFork; 15] {
        [
            ScheduledFork::new(Self::Eip1559, ForkCondition::Block(281_800)),
            ScheduledFork::new(Self::BatchDigestV2, ForkCondition::Block(560_539)),
            ScheduledFork::new(Self::AdminTransfer, ForkCondition::Block(560_539)),
            ScheduledFork::new(Self::PrecompileGasFix, ForkCondition::Block(900_000)),
            ScheduledFork::new(Self::RlsStorage, ForkCondition::Block(900_000)),
            ScheduledFork::new(Self::Tokenomics, ForkCondition::Block(1_879_000)),
            ScheduledFork::new(Self::Uups, ForkCondition::Block(2_872_000)),
            // Bytecode is already present on testnet
            ScheduledFork::new(Self::Erc20PrecompileBytecode, ForkCondition::Never),
            ScheduledFork::new(Self::TransactionLoadBalancing, ForkCondition::Block(4_386_290)),
            ScheduledFork::new(Self::UsdrSupplyCorrection, ForkCondition::Never),
            ScheduledFork::new(Self::EmptyOutputBlock, ForkCondition::Block(6_663_630)),
            ScheduledFork::new(Self::DynamicCommitteeSizing, ForkCondition::Block(10_934_554)),
            // Never until SRE schedules a concrete testnet activation block.
            ScheduledFork::new(Self::HybridRewards, ForkCondition::Never),
            ScheduledFork::new(Self::OutputSeqNormalization, ForkCondition::Never),
            // Never until an operational activation block is chosen; the mechanism ships dormant.
            ScheduledFork::new(Self::SenderAffinityLoadBalancing, ForkCondition::Never),
        ]
    }

    /// Mainnet hardfork schedule.
    pub const fn mainnet() -> [ScheduledFork; 15] {
        [
            ScheduledFork::new(Self::Eip1559, ForkCondition::Block(0)),
            ScheduledFork::new(Self::BatchDigestV2, ForkCondition::Block(0)),
            ScheduledFork::new(Self::AdminTransfer, ForkCondition::Never),
            ScheduledFork::new(Self::PrecompileGasFix, ForkCondition::Block(0)),
            ScheduledFork::new(Self::RlsStorage, ForkCondition::Never),
            ScheduledFork::new(Self::Tokenomics, ForkCondition::Never),
            ScheduledFork::new(Self::Uups, ForkCondition::Never),
            ScheduledFork::new(Self::Erc20PrecompileBytecode, ForkCondition::Block(893_558)),
            ScheduledFork::new(Self::TransactionLoadBalancing, ForkCondition::Block(893_558)),
            ScheduledFork::new(Self::UsdrSupplyCorrection, ForkCondition::Block(3_569_194)),
            ScheduledFork::new(Self::EmptyOutputBlock, ForkCondition::Block(3_569_194)),
            ScheduledFork::new(Self::DynamicCommitteeSizing, ForkCondition::Block(8_291_010)),
            // Never until SRE schedules a concrete mainnet activation block (the reward-fairness
            // rollout for #633); the in-place migration re-links BlsG1 from the live contract.
            ScheduledFork::new(Self::HybridRewards, ForkCondition::Never),
            // Never until SRE schedules a concrete mainnet activation block.
            ScheduledFork::new(Self::OutputSeqNormalization, ForkCondition::Never),
            // Never until an operational activation block is chosen; the mechanism ships dormant.
            ScheduledFork::new(Self::SenderAffinityLoadBalancing, ForkCondition::Never),
        ]
    }

    /// Local network hardfork schedule (first four hardforks active at genesis).
    pub const fn local() -> [ScheduledFork; 15] {
        [
            ScheduledFork::new(Self::Eip1559, ForkCondition::Block(0)),
            ScheduledFork::new(Self::BatchDigestV2, ForkCondition::Block(0)),
            ScheduledFork::new(Self::AdminTransfer, ForkCondition::Block(0)),
            ScheduledFork::new(Self::PrecompileGasFix, ForkCondition::Block(0)),
            ScheduledFork::new(Self::RlsStorage, ForkCondition::Never),
            ScheduledFork::new(Self::Tokenomics, ForkCondition::Never),
            ScheduledFork::new(Self::Uups, ForkCondition::Never),
            // One-shot migrations fire when the chain transitions *across* their activation
            // block (parent_number → block_number). A fork set to `Block(0)` is treated as
            // already-active at genesis and the migration body is never executed (see the
            // synthetic_schedule(1000) pattern in `hardforks/mod.rs` tests). To make the
            // STOP-bytecode install actually run on a fresh local chain - which is required
            // for the precompile's TOTAL_SUPPLY slot to survive EIP-161 - this MUST be ≥ 1.
            ScheduledFork::new(Self::Erc20PrecompileBytecode, ForkCondition::Block(1)),
            ScheduledFork::new(Self::TransactionLoadBalancing, ForkCondition::Block(0)),
            // Manual end-to-end testing of the hardfork (start chain → mint/burn → wait for
            // activation → verify totalSupply). Block 100 hits ~7 s into a fresh local devnet
            // at the 4-validator DAG cadence (≈15 EVM blocks/sec), so the pre-fork window is
            // short but enough to run one mint/burn round.
            ScheduledFork::new(Self::UsdrSupplyCorrection, ForkCondition::Block(100)),
            ScheduledFork::new(Self::EmptyOutputBlock, ForkCondition::Block(0)),
            ScheduledFork::new(Self::DynamicCommitteeSizing, ForkCondition::Block(0)),
            // Block 1 (not 0): genesis deploys the pre-hybrid ConsensusRegistry, and the
            // in-place bytecode-swap migration runs at the first post-genesis block, after
            // which epoch closes use the hybrid `applyIncentives` ABI.
            ScheduledFork::new(Self::HybridRewards, ForkCondition::Block(1)),
            ScheduledFork::new(Self::OutputSeqNormalization, ForkCondition::Block(0)),
            // Real networks stay `Never` until an activation block is chosen operationally.
            ScheduledFork::new(Self::SenderAffinityLoadBalancing, ForkCondition::Block(0)),
        ]
    }

    /// Return the hardfork schedule for the given network.
    pub const fn for_network(network: RaylsNetwork) -> [ScheduledFork; 15] {
        match network {
            RaylsNetwork::Devnet => Self::devnet(),
            RaylsNetwork::Testnet => Self::testnet(),
            RaylsNetwork::Mainnet => Self::mainnet(),
            RaylsNetwork::Local => Self::local(),
        }
    }
}
