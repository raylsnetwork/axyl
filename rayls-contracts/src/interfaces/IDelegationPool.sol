// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

/**
 * @title IDelegationPool
 * @notice A Rayls Contract
 *
 * @notice Interface for multi-delegator staking pool enabling N:1 delegator-to-validator relationships
 * @dev Deployed by governance; RewardDistributor calls into it for reward distribution
 * @dev Delegations are made with ERC-20 RLS tokens; rewards are distributed in ERC-20 RLS
 * @dev RLS is the ERC-20 staking token; USDr is the native token
 */
interface IDelegationPool {
    /// @notice Per-validator pool configuration and accounting
    struct ValidatorPool {
        uint256 totalDelegated;
        uint256 commissionBps;
        uint256 rewardPerShareAccum;
        uint256 pendingValidatorRewards;
        bool acceptingDelegations;
        uint256 slashPerShareAccum;
        // Track B: open-tier (non-whitelisted) delegators — independent reward stream
        uint256 openTierDelegated;
        uint256 openRewardPerShareAccum;
    }

    /// @notice Per-delegator position within a validator's pool
    struct DelegatorPosition {
        uint256 amount;
        uint256 rewardDebt;
        uint256 pendingRewards;
        uint64 undelegateEpoch;
        uint256 undelegateAmount;
        uint256 slashDebt;
        uint64 lastDelegateEpoch; // epoch of most recent delegation — same-epoch rewards excluded (DP-NEW-002)
        bool openTier; // immutable after entry; true = Track B (open-tier), false = Track A (whitelisted)
    }

    /// @notice Pending commission increase awaiting activation
    struct PendingCommission {
        uint256 newBps;
        uint32 effectiveEpoch;
    }

    /// @notice Global delegation configuration
    struct DelegationConfig {
        uint256 minDelegation;
        uint256 maxDelegation;
        uint256 maxValidatorDelegation;
        uint32 unbondingEpochs;
        uint32 commissionDelayEpochs;
    }

    // errors
    error PoolNotRegistered(address validator);
    error PoolAlreadyRegistered(address validator);
    error PoolNotAcceptingDelegations(address validator);
    error InsufficientDelegation(uint256 amount, uint256 minimum);
    error ExceedsMaxDelegation(uint256 amount, uint256 maximum);
    error ExceedsMaxValidatorDelegation(uint256 total, uint256 maximum);
    error InsufficientBalance(uint256 requested, uint256 available);
    error NothingToUndelegate();
    error UnbondingNotComplete(uint32 currentEpoch, uint64 requiredEpoch);
    error PendingUndelegationExists(uint64 undelegateEpoch);
    error NoPendingRewards();
    error NoCommissionToClaim();
    error InvalidCommission(uint256 bps);
    error InvalidInitialCommission(uint256 bps);
    error CommissionIncreaseExceedsLimit(uint256 increase, uint256 maxIncrease);
    error PendingCommissionExists(uint32 effectiveEpoch);
    error NoPendingCommission();
    error CommissionNotYetEffective(uint32 currentEpoch, uint32 effectiveEpoch);
    error OnlyConsensusRegistry();
    error OnlyRewardSources();
    error NotActiveValidator(address validator);
    error NotAllowlisted(address validator);
    error NotWhitelisted(address delegator);
    error InvalidConfig();
    error ZeroAddress();
    error ZeroAmount();

    // events
    event PoolRegistered(address indexed validator, uint256 commissionBps);
    event CommissionUpdated(
        address indexed validator,
        uint256 oldBps,
        uint256 newBps
    );
    event DelegationsToggled(address indexed validator, bool accepting);
    event Delegated(
        address indexed validator,
        address indexed delegator,
        uint256 amount
    );
    event UndelegationRequested(
        address indexed validator,
        address indexed delegator,
        uint256 amount,
        uint64 completionEpoch
    );
    event UndelegationCompleted(
        address indexed validator,
        address indexed delegator,
        uint256 amount
    );
    event DelegationRewardsClaimed(
        address indexed validator,
        address indexed delegator,
        uint256 amount
    );
    event CommissionClaimed(address indexed validator, uint256 amount);
    event PoolRewardsDistributed(address indexed validator, uint256 amount);
    event PoolSlashed(address indexed validator, uint256 amount);
    event ConfigUpdated(DelegationConfig config);
    event RewardDistributorUpdated(address indexed oldDistributor, address indexed newDistributor);
    event RewardRecipientUpdated(address indexed delegator, address indexed validator, address indexed newRecipient);
    event CommissionRecipientUpdated(address indexed validator, address indexed newRecipient);
    event CommissionUpdateScheduled(
        address indexed validator,
        uint256 currentBps,
        uint256 newBps,
        uint32 effectiveEpoch
    );
    event PendingCommissionCancelled(address indexed validator, uint256 cancelledBps);
    event WhitelistRootUpdated(bytes32 oldRoot, bytes32 newRoot);
    event WhitelistEnabledUpdated(bool enabled);
    event WhitelistVerified(address indexed delegator);

    // === Validator functions ===

    /// @notice Validator registers their delegation pool with a commission rate
    /// @param commissionBps Commission in basis points (0-10000, i.e. 0%-100%)
    function registerPool(uint256 commissionBps) external;

    /// @notice Validator updates their commission rate
    /// @param newCommissionBps New commission in basis points
    function updateCommission(uint256 newCommissionBps) external;

    /// @notice Validator toggles whether their pool accepts new delegations
    /// @param accepting Whether to accept new delegations
    function setAcceptingDelegations(bool accepting) external;

    /// @notice Validator claims accumulated commission rewards
    function claimCommission() external;

    /// @notice Validator sets a custom recipient for commission rewards
    /// @param recipient The address to receive commission (address(0) to reset to self)
    function setCommissionRecipient(address recipient) external;

    /// @notice Get the commission recipient for a validator
    /// @param validatorAddress The validator's address
    /// @return The address that will receive commission when claimed
    function getCommissionRecipient(address validatorAddress) external view returns (address);

    /// @notice Activate a pending commission increase after the delay period
    function activatePendingCommission() external;

    /// @notice Cancel a pending commission increase
    function cancelPendingCommission() external;

    /// @notice Get pending commission for a validator
    /// @param validatorAddress The validator's address
    /// @return The pending commission details (newBps=0 if none pending)
    function getPendingCommission(address validatorAddress) external view returns (PendingCommission memory);

    // === Delegator functions ===

    /// @notice Delegate ERC-20 RLS to a validator's pool
    /// @dev Delegator must approve this contract to spend RLS tokens before calling
    /// @dev When the whitelist gate is enabled, caller must already be verified.
    ///      Use `delegateWithProof` to verify for the first time.
    /// @param validatorAddress The validator to delegate to
    /// @param amount The amount of RLS tokens to delegate
    function delegate(address validatorAddress, uint256 amount) external;

    /// @notice Delegate while submitting a Merkle proof of PrelaunchLockbox inclusion
    /// @dev Verifies the caller against the whitelist Merkle root when the gate is
    ///      enabled and the caller has not yet been verified. After a successful
    ///      verification the result is cached and subsequent delegations can use
    ///      the 2-arg `delegate`. When the gate is disabled or the caller is already
    ///      verified, the proof is ignored.
    /// @param validatorAddress The validator to delegate to
    /// @param amount The amount of RLS tokens to delegate
    /// @param lockboxBalance The caller's balance as encoded in the lockbox snapshot leaf
    /// @param proof Merkle proof of `(msg.sender, lockboxBalance)` against `whitelistRoot`
    function delegateWithProof(
        address validatorAddress,
        uint256 amount,
        uint256 lockboxBalance,
        bytes32[] calldata proof
    ) external;

    /// @notice Request undelegation from a validator (starts unbonding period)
    /// @param validatorAddress The validator to undelegate from
    /// @param amount The amount of RLS to undelegate
    function requestUndelegation(
        address validatorAddress,
        uint256 amount
    ) external;

    /// @notice Complete undelegation after the unbonding period has elapsed
    /// @param validatorAddress The validator to complete undelegation from
    function completeUndelegation(address validatorAddress) external;

    /// @notice Claim accumulated delegation rewards from a validator's pool
    /// @param validatorAddress The validator whose pool to claim from
    function claimDelegationRewards(address validatorAddress) external;

    /// @notice Set a custom recipient for delegation rewards
    /// @param validatorAddress The validator whose pool to set the recipient for
    /// @param recipient The address to receive rewards (address(0) to reset to self)
    function setRewardRecipient(address validatorAddress, address recipient) external;

    /// @notice Get the reward recipient for a delegator in a specific validator's pool
    /// @param validatorAddress The validator's address
    /// @param delegator The delegator's address
    /// @return The address that will receive rewards when claimed
    function getRewardRecipient(address validatorAddress, address delegator) external view returns (address);

    // === RewardDistributor integration ===

    /// @notice Distribute ERC-20 RLS rewards to a validator's delegation pool (legacy single-amount)
    /// @dev Only callable by ConsensusRegistry or RewardDistributor
    /// @dev Caller must transfer RLS tokens before calling this
    /// @dev Splits the amount proportionally between Track A and Track B by stake
    /// @param validatorAddress The validator receiving rewards
    /// @param amount The amount of RLS tokens to distribute
    function distributePoolRewards(
        address validatorAddress,
        uint256 amount
    ) external;

    /// @notice Distribute ERC-20 RLS rewards to a validator's pool with per-track amounts
    /// @dev Only callable by RewardDistributor
    /// @dev RewardDistributor must transfer (trackAAmount + trackBAmount) RLS tokens before calling
    /// @param validatorAddress The validator receiving rewards
    /// @param trackAAmount RLS to distribute to whitelisted (Track A) delegators
    /// @param trackBAmount RLS to distribute to open-tier (Track B) delegators
    function distributePoolRewards(
        address validatorAddress,
        uint256 trackAAmount,
        uint256 trackBAmount
    ) external;

    /// @notice Apply a slash to a validator's delegation pool
    /// @dev Only callable by ConsensusRegistry
    /// @param validatorAddress The validator being slashed
    /// @param amount The slash amount to absorb from the pool
    function applyPoolSlash(
        address validatorAddress,
        uint256 amount
    ) external returns (uint256 effectiveSlash);

    // === View functions ===

    /// @notice Get the RLS token address (ERC-20 staking token)
    function rlsToken() external view returns (address);

    /// @notice Check if a pool is registered for a validator
    function poolRegistered(address validatorAddress) external view returns (bool);

    /// @notice Get the total delegated stake for a validator (Track A + Track B combined)
    /// @dev Intended for Rust consensus to read for future weighted voting power
    function getTotalDelegatedStake(
        address validatorAddress
    ) external view returns (uint256);

    /// @notice Get the open-tier (Track B) delegated stake for a validator
    function getTotalOpenTierDelegatedStake(
        address validatorAddress
    ) external view returns (uint256);

    /// @notice Get a delegator's pending (unclaimed) rewards
    function getPendingRewards(
        address validatorAddress,
        address delegator
    ) external view returns (uint256);

    /// @notice Get a delegator's full position details
    function getDelegatorPosition(
        address validatorAddress,
        address delegator
    ) external view returns (DelegatorPosition memory);

    /// @notice Get a validator's pool configuration and state
    function getValidatorPool(
        address validatorAddress
    ) external view returns (ValidatorPool memory);

    /// @notice Get the current delegation configuration
    function getDelegationConfig()
        external
        view
        returns (DelegationConfig memory);

    /// @notice Current whitelist Merkle root (root of `(address, balance)` leaves from the PrelaunchLockbox snapshot)
    function whitelistRoot() external view returns (bytes32);

    /// @notice Whether the whitelist gate on `delegate` is enabled
    function whitelistEnabled() external view returns (bool);

    /// @notice Whether `account` has already proven Merkle inclusion
    function isWhitelistVerified(address account) external view returns (bool);

    /// @notice Set the whitelist Merkle root and enable the gate
    /// @dev Admin-only. Enables the whitelist as a side effect — a new root
    ///      is always a commitment to gate delegations. Does not invalidate
    ///      previously cached `isWhitelistVerified` entries.
    function setWhitelistRoot(bytes32 newRoot) external;

    /// @notice Kill-switch: disables the whitelist gate on `delegate`
    /// @dev Admin-only. The root and cached verifications are preserved —
    ///      calling `setWhitelistRoot` again re-enables the gate.
    function disableWhitelist() external;

    /// @notice Get a delegator's effective position after pending slashes
    /// @param validatorAddress The validator whose pool to query
    /// @param delegator The delegator address
    /// @return effectiveAmount The delegator's stake after applying pending slashes
    /// @return pendingRewards The delegator's total pending rewards
    function getEffectivePosition(
        address validatorAddress,
        address delegator
    ) external view returns (uint256 effectiveAmount, uint256 pendingRewards);
}
