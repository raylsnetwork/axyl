//! Module for solidity interface.
//!
//! These compile into types for interacting with smart contracts through
//! System Calls.

use alloy::{primitives::address, sol};
use rayls_infrastructure_types::{Address, Epoch};

/// The address for consensus registry.
pub use rayls_infrastructure_config::CONSENSUS_REGISTRY_ADDRESS;
/// The address for delegation pool.
pub use rayls_infrastructure_config::DELEGATION_POOL_ADDRESS;
/// The address for fee aggregator.
pub use rayls_infrastructure_config::FEE_AGGREGATOR_ADDRESS;
/// The address for native token controller.
pub use rayls_infrastructure_config::NATIVE_TOKEN_CONTROLLER_ADDRESS;
/// The address for reward distributor.
pub use rayls_infrastructure_config::REWARD_DISTRIBUTOR_ADDRESS;
/// The address for the RLS accumulator.
pub use rayls_infrastructure_config::RLS_ACCUMULATOR_ADDRESS;
/// The address for the RLS ERC-20 token proxy.
pub use rayls_infrastructure_config::RLS_ADDRESS;
/// The address for the RLS ERC-20 token implementation.
pub use rayls_infrastructure_config::RLS_IMPL_ADDRESS;

/// The system address.
pub(super) const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

// ConsensusRegistry interface. See rayls-contracts submodule.
sol!(

    /// Consensus registry.
    #[sol(rpc)]
    contract ConsensusRegistry {
        /// The validator's eligibility status for being
        /// considered in the next committee.
        #[derive(Debug, PartialEq)]
        enum ValidatorStatus {
            /// Undefined status - default value.
            Undefined,
            /// The validator is staked but not eligible for participating
            /// in consensus.
            Staked,
            /// The validator is staked and has indicated it is ready
            /// to participate in committee to earn rewards.
            PendingActivation,
            /// The validator is actively participating in consensus.
            Active,
            /// The validator has indicated interest to exit the protocol.
            PendingExit,
            /// The validator is no longer participating in consensus.
            Exited,
            /// Match any status (also indicates `Retired`)
            Any
        }

        /// The validator's information.
        #[derive(Debug)]
        struct ValidatorInfo {
            /// The BLS12-381 public key.
            bytes blsPubkey;
            /// The address based on ECDSA public key.
            address validatorAddress;
            /// The epoch which the validator's status
            /// become "Active" and eligible to participate
            /// in a committee.
            uint32 activationEpoch;
            /// The epoch that the validator exited the protocol.
            uint32 exitEpoch;
            /// The current status of the validator.
            ValidatorStatus currentStatus;
            /// The validator is permanently disqualified from consensus.
            bool isRetired;
            /// The validator received stake through delegation.
            bool isDelegated;
            /// The configuration for validators stake.
            ///
            /// This supports updating stake amount.
            uint8 stakeVersion;
        }

        /// The epoch info stored on-chain.
        #[derive(PartialEq, Debug)]
        struct EpochInfo {
            /// The committee of validators responsible for the epoch.
            address[] committee;
            /// The execution block height when the epoch started and the
            /// committee became active.
            uint64 blockHeight;
            /// The duration for the epoch (in secs).
            ///
            /// NOTE: this is set at the start of each epoch based on the
            /// current value of the `StakeConfig`.
            uint32 epochDuration;
            /// The stake version to use for rewards calculations.
            uint8 stakeVersion;
        }

        /// The rewards applied right before concluding the epoch.
        /// This is provided by the protocol.
        ///
        /// NOTE: this is part of the StakeManager contract.
        #[derive(Debug)]
        struct RewardInfo {
            /// The validator to receive rewards.
            address validatorAddress;
            /// The number of consensus blocks for which they were the leader.
            uint256 consensusHeaderCount;
        }

        /// Slash information for system calls to decrement outstanding validator balances
        /// Currently disabled during MNO pilot.
        #[derive(Debug)]
        struct Slash {
            /// The validator to slash.
            address validatorAddress;
            /// The amount to slash.
            uint256 amount;
        }

        /// The configuration for consensus.
        #[derive(Debug)]
        struct StakeConfig {
            /// The fixed stake amount.
            uint256 stakeAmount;
            /// The min amount allowed to withdraw.
            uint256 minWithdrawAmount;
            /// The duration for the epoch (in secs).
            uint32 epochDuration;
        }

        /// Represents a proof of possession for a validator's BLS public key
        /// Uses a 192-byte uncompressed public key and 96-byte uncompressed PoP
        #[derive(Debug)]
        struct ProofOfPossession {
            bytes uncompressedPubkey;
            bytes uncompressedSignature;
        }

        /// Initialize the contract.
        #[derive(Debug)]
        constructor(
            /// The RLS ERC-20 token used for staking.
            address rls_,
            /// The configuration for staking.
            StakeConfig memory genesisConfig_,
            /// The initial validators with stake.
            ValidatorInfo[] memory initialValidators_,
            /// The initial validators' uncompressed proofs of possession
            ProofOfPossession[] memory proofsOfPossession,
            /// The address of the owner.
            address owner_
        ) external;

        /// Conclude the current epoch. Caller must pass a new committee of eligible validators.
        function concludeEpoch(address[] calldata newCommittee) external;
        /// Apply incentives for the epoch. This must be called before `concludeEpoch`.
        function applyIncentives(RewardInfo[] calldata rewardInfos) external;
        /// Apply negative incentives for the epoch. This must be called before `concludeEpoch`.
        function applySlashes(Slash[] calldata slashes) external;
        /// Return the current epoch.
        function getCurrentEpoch() public view returns (uint32) ;
        /// Helper function to get the epoch info from the current epoch.
        function getCurrentEpochInfo() external view returns (EpochInfo memory currentEpochInfo);
        /// Return committee epoch info for a specific epoch.
        function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory epochInfo);
        /// Return the validators by status. Pass `0` for status to return all validators.
        function getValidators(uint8 status) public view returns (ValidatorInfo[] memory);
        /// Fetch the committee for a given epoch.
        function getCommitteeValidators(uint32 epoch) external view returns (ValidatorInfo[] memory);
        /// Fetch the `ValidatorInfo` for a give address.
        function getValidator(address validatorAddress) external view returns (ValidatorInfo memory);
        /// Returns the BLS12-381 proof of possession message: `blsPubkey || validatorAddress`
        function proofOfPossessionMessage(
            bytes memory blsPubkey,
            address validatorAddress
        ) external pure returns (bytes memory);

        /// Stake to the consensus registry.
        function stake(bytes calldata blsPubkey, ProofOfPossession calldata proofOfPossession) external override onlyOwner;

        /// Activate node for committee selection.
        /// Normally called by staker after node is synced.
        function activate() external override whenNotPaused;
        /// Initiate exit from protocol.
        function beginExit() external override whenNotPaused;

        /// Retrieve the claimable rewards accrued for a given validator address.
        function getRewards(address validatorAddress) public view virtual returns (uint256);

        /// Add a validator to the allowlist (onlyOwner).
        function allowlistValidator(address validatorAddress) external;
    }

);

// ConsensusRegistry hybrid-reward ABI (active from the HybridRewards fork).
//
// A separate Rust-side binding, not an edit of `ConsensusRegistry` above: it keeps the
// pre-fork 1-arg `applyIncentives` encoding byte-for-byte unchanged for archive replay,
// and avoids a `RewardInfo` symbol collision between the two ABIs. Only the members the
// hybrid close-epoch path encodes are declared here.
sol!(
    /// Consensus registry, hybrid-reward ABI.
    #[sol(rpc)]
    contract ConsensusRegistryV2 {
        /// Per-validator inputs for the hybrid (participation + anchor + stake) blend.
        /// Mirrors `IStakeManager.RewardInfo` in the hybrid contract.
        #[derive(Debug)]
        struct RewardInfo {
            /// The validator to receive rewards.
            address validatorAddress;
            /// Distinct committed rounds this validator's certificate was included in.
            uint256 participationRounds;
            /// Committed rounds this validator led (the Anchor Commit Share signal).
            uint256 anchorRounds;
        }

        /// Apply hybrid incentives for the epoch. Must be called before `concludeEpoch`.
        /// `totalRounds` is the epoch-wide committed-round count (anchor/floor denominator).
        function applyIncentives(RewardInfo[] calldata rewardInfos, uint256 totalRounds) external;
    }
);

// FeeAggregator interface. See rayls-contracts submodule.
// Unified contract for fee collection, swapping (USDr → RLS), and distribution.
sol!(
    /// Fee aggregator for collecting fees, swapping to RLS, and distributing.
    #[sol(rpc)]
    contract FeeAggregator {
        /// Distribution configuration in basis points (1 bps = 0.01%).
        /// Total of all bps must equal 10,000 (100%).
        struct DistributionConfig {
            uint256 validatorPoolBps;
            uint256 ecosystemBps;
            uint256 burnBps;
        }

        /// Initialize the proxy. Grants DEFAULT_ADMIN + KEEPER + PAUSER + UPGRADER to `admin_`.
        function initialize(
            address rlsToken_,
            address algebraRouter_,
            address rewardDistributor_,
            address ecosystemTreasury_,
            address burnAddress_,
            address usdrToken_,
            DistributionConfig memory config_,
            address admin_
        ) external;

        /// Distribute all RLS held by the contract to 3 categories.
        /// Only callable by KEEPER_ROLE.
        function distributeEpochFees() external returns (uint256 rlsDistributed);

        /// Get the pending balance of a stablecoin (including USDr).
        function pendingBalance(address stablecoin) external view returns (uint256);

        /// Get the USDr token address (primary fee token for swaps).
        function usdrToken() external view returns (address);

        /// Get the current distribution configuration.
        function getConfig() external view returns (DistributionConfig memory);

        /// Get the reward distributor address.
        function rewardDistributor() external view returns (address);
    }
);

// RewardDistributor interface. See rayls-contracts submodule.
sol!(
    /// Reward distributor for RLS staking rewards.
    #[sol(rpc)]
    contract RewardDistributor {
        /// Initialize the proxy. Grants DEFAULT_ADMIN + UPGRADER to `admin_`.
        function initialize(
            address rls_,
            address feeAggregator_,
            address consensusRegistry_,
            address delegationPool_,
            address admin_
        ) external;

        /// Wire the RewardDistributor to the RLSAccumulator. Callable by DEFAULT_ADMIN_ROLE.
        function setAccumulator(address newAccumulator) external;

        /// Receive RLS rewards from FeeAggregator.
        /// FeeAggregator must transfer RLS tokens before calling this.
        function receiveRewards(uint256 amount) external;

        /// Distribute all pending rewards to active validators and their delegation pools.
        function distributeRewards() external;

        /// Get pending rewards for a specific validator.
        function getPendingRewards(address validatorAddress) external view returns (uint256);

        /// Get total undistributed rewards held by the contract.
        function totalPendingRewards() external view returns (uint256);

        /// Get the FeeAggregator address.
        function feeAggregator() external view returns (address);

        /// Return the wired RLSAccumulator address.
        function accumulator() external view returns (address);
    }
);

// NativeTokenController interface.
sol!(
    /// UUPS-upgradable controller for the native ERC-20 (USDr) precompile.
    #[sol(rpc)]
    contract NativeTokenController {
        /// Initialize the proxy. Grants DEFAULT_ADMIN + UPGRADER to `admin`.
        function initialize(address admin) external;
    }
);

// RLSAccumulator interface.
sol!(
    /// Accumulator that holds RLS reserves and lets RewardDistributor pull top-ups.
    #[sol(rpc)]
    contract RLSAccumulator {
        /// Initialize the proxy. Grants DEFAULT_ADMIN + UPGRADER to `admin_` and calls
        /// `IERC20(rls_).forceApprove(rewardDistributor_, type(uint256).max)`.
        function initialize(address rls_, address rewardDistributor_, address admin_) external;

        /// Return the configured RLS token address.
        function rlsToken() external view returns (address);

        /// Return the configured RewardDistributor address.
        function rewardDistributor() external view returns (address);
    }
);

// DelegationPool interface.
sol!(
    /// Delegation pool for multi-delegator staking.
    #[sol(rpc)]
    contract DelegationPool {
        /// Per-validator pool configuration and accounting.
        struct ValidatorPool {
            uint256 totalDelegated;
            uint256 commissionBps;
            uint256 rewardPerShareAccum;
            uint256 pendingValidatorRewards;
            bool acceptingDelegations;
            uint256 slashPerShareAccum;
        }

        /// Per-delegator position within a validator's pool.
        struct DelegatorPosition {
            uint256 amount;
            uint256 rewardDebt;
            uint256 pendingRewards;
            uint64 undelegateEpoch;
            uint256 undelegateAmount;
            uint256 slashDebt;
        }

        /// Global delegation configuration.
        struct DelegationConfig {
            uint256 minDelegation;
            uint256 maxDelegation;
            uint256 maxValidatorDelegation;
            uint32 unbondingEpochs;
            uint32 commissionDelayEpochs;
        }

        /// Initialize the proxy. Grants DEFAULT_ADMIN + UPGRADER to `admin_`.
        function initialize(
            address rls_,
            address consensusRegistry_,
            address admin_,
            DelegationConfig memory config_
        ) external;

        /// Wire the DelegationPool to a RewardDistributor. Callable by DEFAULT_ADMIN_ROLE.
        function setRewardDistributor(address newRewardDistributor) external;

        /// Get the total delegated stake for a validator.
        function getTotalDelegatedStake(address validatorAddress) external view returns (uint256);

        /// Distribute epoch rewards to a validator's delegation pool.
        function distributePoolRewards(address validatorAddress) external payable;

        /// Apply a slash to a validator's delegation pool.
        function applyPoolSlash(address validatorAddress, uint256 amount) external;

        /// Get the current delegation configuration.
        function getDelegationConfig() external view returns (DelegationConfig memory);

        /// Get a validator's pool configuration and state.
        function getValidatorPool(address validatorAddress) external view returns (ValidatorPool memory);

        /// Return the wired RewardDistributor address.
        function rewardDistributor() external view returns (address);
    }
);

// RLS ERC-20 token interface. Used for genesis deployment and system interactions.
sol!(
    /// RLS ERC-20 staking token (UUPS upgradeable, IMintableBurnable-compatible).
    #[sol(rpc)]
    contract RLSToken {
        /// Initialize the proxy: mint `initialSupply` to `treasury`, grant roles to `admin`.
        function initialize(address admin, address treasury, uint256 initialSupply) external;
        /// Transfer `amount` tokens to `to`. Returns true on success.
        function transfer(address to, uint256 amount) external returns (bool);
        /// Mint tokens to receiver. Caller must hold MINTER_ROLE.
        function mint(address receiver, uint256 amount) external;
        /// Return the token balance of `account`.
        function balanceOf(address account) external view returns (uint256);
        /// Approve `spender` to transfer up to `amount` tokens on behalf of the caller.
        function approve(address spender, uint256 amount) external returns (bool);
    }
);

/// The state of consensus retrieved from chain.
#[derive(Debug)]
pub struct EpochState {
    /// The epoch number.
    pub epoch: Epoch,
    /// The [EpochInfo].
    pub epoch_info: ConsensusRegistry::EpochInfo,
    /// The collection of validator info.
    pub validators: Vec<ConsensusRegistry::ValidatorInfo>,
    /// The timestamp for when the previous epoch closed.
    ///
    /// This time plus the `EpochInfo::epochDuration` creates the timestamp for the next epoch
    /// boundary.
    pub epoch_start: u64,
}
