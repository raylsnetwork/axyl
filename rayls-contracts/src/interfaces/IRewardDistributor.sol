// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

/**
 * @title IRewardDistributor
 * @notice A Rayls Contract
 *
 * @notice Interface for distributing ERC-20 RLS staking rewards to validators and delegators
 * @dev Receives ERC-20 RLS from FeeAggregator (after USDr → RLS swap) and distributes based on stake
 * @dev RLS is an ERC-20 token; USDr is ERC-20 stablecoin
 */
interface IRewardDistributor {
    /// @dev Retained for storage layout compatibility only. Not used in current version.
    struct DistributionState {
        uint256 totalRewards;
        uint256 totalStake;
        uint256 nextIndex;
        uint256 validatorCount;
        uint256 distributed;
        bool inProgress;
    }

    // errors
    error ZeroAddress();
    error ZeroAmount();
    error OnlyFeeAggregator();
    error NoActiveValidators();
    error NotAuthorized();
    error InsufficientBalance(uint256 requested, uint256 available);
    error InvalidApyBps();

    // events
    event RewardsReceived(uint256 amount);
    event ValidatorRewardDistributed(address indexed validator, uint256 validatorShare, uint256 poolShare);
    event RewardsDistributed(uint256 totalAmount, uint256 validatorCount);
    event PendingRewardsClaimed(address indexed validator, uint256 amount);
    event FeeAggregatorUpdated(address indexed oldAggregator, address indexed newAggregator);
    event ConsensusRegistryUpdated(address indexed oldRegistry, address indexed newRegistry);
    event DelegationPoolUpdated(address indexed oldPool, address indexed newPool);
    event RewardRecipientUpdated(address indexed validator, address indexed oldRecipient, address indexed newRecipient);
    event AccumulatorTopUp(uint256 pullAmount, uint256 targetReward, uint256 totalRewards);
    event AccumulatorTopUpFailed(uint256 pullAmount);
    event AccumulatorUpdated(address indexed oldAccumulator, address indexed newAccumulator);
    event TargetApyBpsUpdated(uint256 oldApyBps, uint256 newApyBps);
    event OpenTierApyBpsUpdated(uint256 oldApyBps, uint256 newApyBps);

    /// @notice Receive ERC-20 RLS rewards from FeeAggregator
    /// @dev Called by FeeAggregator after swapping USDr to RLS
    /// @dev FeeAggregator must transfer RLS tokens before calling this
    /// @param amount The amount of RLS tokens received
    function receiveRewards(uint256 amount) external;

    /// @notice Distribute all pending rewards to active validators and their delegation pools
    /// @dev Calculates distribution based on performance weights or stake
    function distributeRewards() external;

    /// @notice Get pending rewards for a specific validator
    /// @param validatorAddress The validator's address
    /// @return The pending reward amount for the validator
    function getPendingRewards(address validatorAddress) external view returns (uint256);

    /// @notice Get total undistributed rewards held by the contract
    /// @return The total RLS balance pending distribution
    function totalPendingRewards() external view returns (uint256);

    /// @notice Get the RLS token address (ERC-20 staking token)
    function rlsToken() external view returns (address);

    /// @notice Get the FeeAggregator address
    function feeAggregator() external view returns (address);

    /// @notice Get the ConsensusRegistry address
    function consensusRegistry() external view returns (address);

    /// @notice Get the DelegationPool address
    function delegationPool() external view returns (address);

    /// @notice Set the FeeAggregator address
    /// @param newAggregator The new FeeAggregator address
    function setFeeAggregator(address newAggregator) external;

    /// @notice Set the DelegationPool address
    /// @param newPool The new DelegationPool address
    function setDelegationPool(address newPool) external;

    /// @notice Get the reward recipient for a validator
    /// @dev Returns the validator address itself if no custom recipient is set
    function getRewardRecipient(address validatorAddress) external view returns (address);

    /// @notice Set the reward recipient for the calling validator
    /// @param recipient The address to receive rewards (address(0) to reset to self)
    function setRewardRecipient(address recipient) external;

    /// @notice Get the RLS Accumulator address
    function accumulator() external view returns (address);

    /// @notice Set the RLS Accumulator address for APY top-ups
    /// @param newAccumulator The new accumulator address
    function setAccumulator(address newAccumulator) external;

    /// @notice Get the target APY in basis points for whitelisted (Track A) stakers
    function targetApyBps() external view returns (uint256);

    /// @notice Set the target APY in basis points for whitelisted (Track A) stakers
    /// @param newApyBps The new target APY
    function setTargetApyBps(uint256 newApyBps) external;

    /// @notice Get the target APY in basis points for open-tier (Track B) stakers
    function openTierTargetApyBps() external view returns (uint256);

    /// @notice Set the target APY in basis points for open-tier (Track B) stakers
    /// @param newApyBps The new target APY
    function setOpenTierTargetApyBps(uint256 newApyBps) external;
}
