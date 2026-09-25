//! Rayls network hardfork definitions.

use reth_chainspec::hardfork;

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
        /// Hash full tx bytes with FxHasher for committee-slot dispatch, replacing the
        /// first-8-bytes-as-u64 prefix.
        TransactionLoadBalancing,
        /// Rebase the USDR precompile's TOTAL_SUPPLY slot to match the true sum of native
        /// balances.
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
        /// Normalize execution to batch seq order: within one output each authority's batches
        /// reorder in place into ascending seq, parked batches still waiting at the epoch
        /// boundary are discarded whole instead of force executed, and an overflow-forced jump
        /// prunes the parked entries it abandons.
        OutputSeqNormalization,
        /// Key committee-slot dispatch on the first transaction's sender, so one validator owns a
        /// sender's whole nonce chain instead of consecutive ranges scattering across pools and
        /// parking nonce-gapped. Also enables live-successor failover for a down slot owner.
        SenderAffinityLoadBalancing,
    }
);

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
            Self::OutputSeqNormalization => 0x0e,
            Self::SenderAffinityLoadBalancing => 0x0f,
        }
    }

    /// Look up a hardfork by name, case-insensitively.
    ///
    /// `None` when the name is not a known Rayls hardfork.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::VARIANTS.iter().find(|fork| fork.name().eq_ignore_ascii_case(name)).copied()
    }
}
