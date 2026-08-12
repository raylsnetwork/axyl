// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {ReentrancyGuard} from "solady/utils/ReentrancyGuard.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {IRewardDistributor} from "../interfaces/IRewardDistributor.sol";
import {IConsensusRegistry} from "../interfaces/IConsensusRegistry.sol";
import {IStakeManager} from "../interfaces/IStakeManager.sol";
import {IDelegationPool} from "../interfaces/IDelegationPool.sol";
import {SystemCallable} from "../consensus/SystemCallable.sol";

/**
 * @title RewardDistributor
 * @notice A Rayls Contract
 *
 * @notice Distributes ERC-20 RLS staking rewards to validators and delegators
 * @dev Receives ERC-20 RLS from FeeAggregator (after USDr → RLS swap) and distributes
 * @dev Falls back to pure stake-based distribution if no performance data is available
 * @dev UUPS upgradeable with AccessControl
 * @dev RLS is an ERC-20 token; USDr is ERC-20 stablecoin
 */
contract RewardDistributor is
    Initializable,
    UUPSUpgradeable,
    AccessControlUpgradeable,
    ReentrancyGuard,
    SystemCallable,
    IRewardDistributor
{
    using SafeERC20 for IERC20;

    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    /// @custom:storage-location erc7201:rewarddistributor.storage.v1
    struct RewardDistributorStorage {
        /// @notice The RLS token contract (ERC-20 staking token)
        IERC20 rls;
        /// @notice The FeeAggregator contract (only caller allowed to receive rewards)
        address feeAggregator;
        /// @notice The ConsensusRegistry contract
        IConsensusRegistry consensusRegistry;
        /// @notice The DelegationPool contract
        IDelegationPool delegationPool;
        /// @notice Pending rewards per validator (in RLS tokens)
        mapping(address => uint256) pendingValidatorRewards;
        /// @notice Custom reward recipient per validator (if set, rewards go here instead of validator address)
        mapping(address => address) rewardRecipients;
        /// @notice Total undistributed rewards (in RLS tokens)
        uint256 totalPending;
        // -- deprecated fields retained for storage layout compatibility --
        IRewardDistributor.DistributionState _deprecated_distributionState;
        address[] _deprecated_cachedValidators;
        uint256[] _deprecated_cachedStakes;
        uint256[] _deprecated_cachedValidatorStakes;
        // -- end deprecated --
        /// @notice RLS Accumulator for APY top-up subsidies
        address accumulator;
        /// @notice Target APY in basis points (e.g., 5000 = 50%)
        uint256 targetApyBps;
        /// @notice Sum of all pendingValidatorRewards — protects unclaimed rewards from recoverTokens
        uint256 totalUnclaimedRewards;
        /// @notice Target APY in basis points for open-tier (Track B) stakers (e.g., 3000 = 30%)
        uint256 openTierTargetApyBps;
    }

    // keccak256(abi.encode(uint256(keccak256("rewarddistributor.storage.v1")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant REWARD_DISTRIBUTOR_STORAGE_LOCATION =
        0x8a40cc0ccf5a2d030058c860d76601e04104947950ec7475e80ab15a7d69d600;

    function _getRewardDistributorStorage() private pure returns (RewardDistributorStorage storage $) {
        assembly {
            $.slot := REWARD_DISTRIBUTOR_STORAGE_LOCATION
        }
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        address rls_,
        address feeAggregator_,
        address consensusRegistry_,
        address delegationPool_,
        address admin_
    ) external initializer {
        if (rls_ == address(0)) revert ZeroAddress();
        if (consensusRegistry_ == address(0)) revert ZeroAddress();
        if (admin_ == address(0)) revert ZeroAddress();

        __AccessControl_init();
        __UUPSUpgradeable_init();

        _grantRole(DEFAULT_ADMIN_ROLE, admin_);
        _grantRole(UPGRADER_ROLE, admin_);

        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        $.rls = IERC20(rls_);
        $.feeAggregator = feeAggregator_;
        $.consensusRegistry = IConsensusRegistry(consensusRegistry_);
        $.delegationPool = IDelegationPool(delegationPool_);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    /// @inheritdoc IRewardDistributor
    function rlsToken() external view override returns (address) {
        return address(_getRewardDistributorStorage().rls);
    }

    /// @inheritdoc IRewardDistributor
    function feeAggregator() external view override returns (address) {
        return _getRewardDistributorStorage().feeAggregator;
    }

    /// @inheritdoc IRewardDistributor
    function consensusRegistry() external view override returns (address) {
        return address(_getRewardDistributorStorage().consensusRegistry);
    }

    /// @inheritdoc IRewardDistributor
    function delegationPool() external view override returns (address) {
        return address(_getRewardDistributorStorage().delegationPool);
    }

    modifier onlyFeeAggregator() {
        if (msg.sender != _getRewardDistributorStorage().feeAggregator) revert OnlyFeeAggregator();
        _;
    }

    /// @inheritdoc IRewardDistributor
    function receiveRewards(uint256 amount) external override onlyFeeAggregator {
        if (amount == 0) revert ZeroAmount();
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        $.totalPending += amount;
        uint256 balance = $.rls.balanceOf(address(this));
        if (balance < $.totalPending) {
            revert InsufficientBalance($.totalPending, balance);
        }
        emit RewardsReceived(amount);
    }

    // ========== DISTRIBUTION ==========

    /// @dev Per-validator stake snapshot
    struct ValidatorStakes {
        uint256 ownStake;
        uint256 trackADelegated;
        uint256 trackBDelegated;
    }

    /// @inheritdoc IRewardDistributor
    /// @dev Distributes all pending rewards in a single call.
    ///      Every validator's target reward is computed directly from its own stake at the
    ///      configured target APY
    function distributeRewards() external override onlySystemCall nonReentrant {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();

        IConsensusRegistry.ValidatorInfo[] memory activeValidators = $.consensusRegistry.getValidators(
            IConsensusRegistry.ValidatorStatus.Active
        );
        uint256 len = activeValidators.length;
        if (len == 0) revert NoActiveValidators();

        uint256 epochSecs = $.consensusRegistry.getCurrentEpochInfo().epochDuration;
        // Fetch all validator stakes in one pass
        ValidatorStakes[] memory stakes = _fetchAllStakes(activeValidators);

        uint256 totalTarget = _computeTotalTarget(stakes, epochSecs);

        uint256 totalRewards = $.totalPending;
        if (totalTarget > 0) {
            totalRewards = _pullAccumulatorTopUp(totalRewards, totalTarget);
        }

        if (totalRewards == 0) {
            emit RewardsDistributed(0, 0);
            return;
        }

        uint256 distributed = totalTarget > 0
            ? _distributeByTarget(activeValidators, stakes, totalRewards, totalTarget, epochSecs)
            : _distributeByStake(activeValidators, stakes, totalRewards);

        $.totalPending -= totalRewards;
        emit RewardsDistributed(distributed, len);
    }

    /// @dev Fetches ownStake/Track A/Track B for every active validator in one pass.
    function _fetchAllStakes(
        IConsensusRegistry.ValidatorInfo[] memory activeValidators
    ) internal view returns (ValidatorStakes[] memory stakes) {
        uint256 len = activeValidators.length;
        stakes = new ValidatorStakes[](len);
        for (uint256 i; i < len; ++i) {
            (uint256 os, uint256 ta, uint256 tb) = _fetchValidatorStakes(activeValidators[i].validatorAddress);
            stakes[i] = ValidatorStakes(os, ta, tb);
        }
    }

    /// @dev Sums each active validator's target reward (own stake + Track A at targetApyBps,
    ///      Track B at openTierTargetApyBps) for the current epoch.
    function _computeTotalTarget(
        ValidatorStakes[] memory stakes,
        uint256 epochSecs
    ) internal view returns (uint256 totalTarget) {
        uint256 len = stakes.length;
        for (uint256 i; i < len; ++i) {
            (uint256 priorityTarget, uint256 trackBTarget) = _splitTarget(
                stakes[i].ownStake, stakes[i].trackADelegated, stakes[i].trackBDelegated, epochSecs
            );
            totalTarget += priorityTarget + trackBTarget;
        }
    }

    /// @dev Scales each validator's pre-fetched target by totalRewards/totalTarget and
    ///      distributes. 
    function _distributeByTarget(
        IConsensusRegistry.ValidatorInfo[] memory activeValidators,
        ValidatorStakes[] memory stakes,
        uint256 totalRewards,
        uint256 totalTarget,
        uint256 epochSecs
    ) internal returns (uint256 distributed) {
        uint256 len = activeValidators.length;
        for (uint256 i; i < len; ++i) {
            ValidatorStakes memory s = stakes[i];
            (uint256 priorityTarget, uint256 trackBTarget) =
                _splitTarget(s.ownStake, s.trackADelegated, s.trackBDelegated, epochSecs);

            uint256 priorityReward = (totalRewards * priorityTarget) / totalTarget;
            uint256 trackBReward = (totalRewards * trackBTarget) / totalTarget;
            if (priorityReward == 0 && trackBReward == 0) continue;
            distributed += _distributeToValidator(
                activeValidators[i].validatorAddress, priorityReward, trackBReward, s.ownStake, s.trackADelegated
            );
        }
    }

    /// @dev Pre-tier fallback: distributes purely proportional to combined stake (own + Track A +
    ///      Track B), with no APY-based track split, exactly as this contract behaved before
    ///      target APYs existed. Only used when neither tier has a target APY configured.
    function _distributeByStake(
        IConsensusRegistry.ValidatorInfo[] memory activeValidators,
        ValidatorStakes[] memory stakes,
        uint256 totalRewards
    ) internal returns (uint256 distributed) {
        uint256 len = activeValidators.length;
        uint256 totalWeight;
        for (uint256 i; i < len; ++i) {
            totalWeight += stakes[i].ownStake + stakes[i].trackADelegated + stakes[i].trackBDelegated;
        }
        if (totalWeight == 0) return 0;

        for (uint256 i; i < len; ++i) {
            ValidatorStakes memory s = stakes[i];
            uint256 weight = s.ownStake + s.trackADelegated + s.trackBDelegated;
            if (weight == 0) continue;
            uint256 validatorReward = (totalRewards * weight) / totalWeight;
            if (validatorReward == 0) continue;
            distributed += _distributeToValidatorByStake(
                activeValidators[i].validatorAddress, validatorReward, s.ownStake, s.trackADelegated, s.trackBDelegated
            );
        }
    }

    /// @dev Splits a stake-proportional reward between a validator's own claim and its delegated
    ///      pool by combined-stake ratio, then splits the pool share between Track A/B by their
    ///      own stake ratio (no APY weighting — there's no configured rate to honor here).
    function _distributeToValidatorByStake(
        address validatorAddr,
        uint256 validatorReward,
        uint256 ownStake,
        uint256 trackADelegated,
        uint256 trackBDelegated
    ) internal returns (uint256) {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        uint256 totalDelegated = trackADelegated + trackBDelegated;
        uint256 totalValidatorStake = ownStake + totalDelegated;

        uint256 validatorShare = validatorReward;
        uint256 poolShare;
        if (totalDelegated > 0 && totalValidatorStake > 0 && address($.delegationPool) != address(0)) {
            validatorShare = (validatorReward * ownStake) / totalValidatorStake;
            poolShare = validatorReward - validatorShare;
        }

        uint256 trackAShare;
        if (poolShare > 0) {
            trackAShare = trackBDelegated == 0
                ? poolShare
                : (trackADelegated == 0 ? 0 : (poolShare * trackADelegated) / totalDelegated);
            uint256 trackBShare = poolShare - trackAShare;
            $.rls.safeTransfer(address($.delegationPool), poolShare);
            $.delegationPool.distributePoolRewards(validatorAddr, trackAShare, trackBShare);
        }

        if (validatorShare > 0) {
            $.pendingValidatorRewards[validatorAddr] += validatorShare;
            $.totalUnclaimedRewards += validatorShare;
        }
        emit ValidatorRewardDistributed(validatorAddr, validatorShare, poolShare);

        return validatorShare + poolShare;
    }

    /// @dev Splits a validator's stake into its priority (own + Track A) and Track B targets.
    function _splitTarget(
        uint256 ownStake,
        uint256 trackADelegated,
        uint256 trackBDelegated,
        uint256 epochSecs
    ) internal view returns (uint256 priorityTarget, uint256 trackBTarget) {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        priorityTarget = ((ownStake + trackADelegated) * $.targetApyBps * epochSecs) / (365 days * 10_000);
        trackBTarget = (trackBDelegated * $.openTierTargetApyBps * epochSecs) / (365 days * 10_000);
    }

    /// @dev Fetches ownStake, Track A delegated, and Track B delegated for a validator in one call.
    function _fetchValidatorStakes(address validator)
        internal
        view
        returns (uint256 ownStake, uint256 trackA, uint256 trackB)
    {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        (, ownStake, ) = IStakeManager(address($.consensusRegistry)).getBalanceBreakdown(validator);
        if (address($.delegationPool) != address(0)) {
            trackB = $.delegationPool.getTotalOpenTierDelegatedStake(validator);
            trackA = $.delegationPool.getTotalDelegatedStake(validator) - trackB;
        }
    }

    /// @dev Distributes validator's targeted rewards, splitting the priority
    ///      (own stake + Track A) portion between the validator's own claim and Track A
    ///      delegators by stake ratio.
    ///      Track B reward passes through entirely as Track B's own-rate share.
    function _distributeToValidator(
        address validatorAddr,
        uint256 priorityReward,
        uint256 trackBReward,
        uint256 ownStake,
        uint256 trackADelegated
    ) internal returns (uint256) {
        if (priorityReward == 0 && trackBReward == 0) return 0;

        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        uint256 priorityStake = ownStake + trackADelegated;

        uint256 validatorShare = priorityReward;
        uint256 trackAShare;
        if (priorityStake > 0) {
            validatorShare = (priorityReward * ownStake) / priorityStake;
            trackAShare = priorityReward - validatorShare;
        }

        uint256 poolShare = trackAShare + trackBReward;
        if (poolShare > 0 && address($.delegationPool) != address(0)) {
            $.rls.safeTransfer(address($.delegationPool), poolShare);
            $.delegationPool.distributePoolRewards(validatorAddr, trackAShare, trackBReward);
        } else if (poolShare > 0) {
            // no delegation pool wired up: nothing to route the pool share to
            validatorShare += poolShare;
            poolShare = 0;
        }

        if (validatorShare > 0) {
            $.pendingValidatorRewards[validatorAddr] += validatorShare;
            $.totalUnclaimedRewards += validatorShare;
        }
        emit ValidatorRewardDistributed(validatorAddr, validatorShare, poolShare);

        return validatorShare + poolShare;
    }

    // ========== CLAIMS ==========

    /// @notice Claim pending rewards for a validator
    /// @dev Only the validator themselves can claim their rewards
    function claimRewards(address validatorAddress) external nonReentrant {
        if (msg.sender != validatorAddress) revert NotAuthorized();

        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        uint256 amount = $.pendingValidatorRewards[validatorAddress];
        if (amount == 0) revert ZeroAmount();

        $.pendingValidatorRewards[validatorAddress] = 0;
        $.totalUnclaimedRewards -= amount;

        address recipient = $.rewardRecipients[validatorAddress];
        if (recipient == address(0)) {
            recipient = validatorAddress;
        }

        $.rls.safeTransfer(recipient, amount);
        emit PendingRewardsClaimed(validatorAddress, amount);
    }

    /// @inheritdoc IRewardDistributor
    function getPendingRewards(
        address validatorAddress
    ) external view override returns (uint256) {
        return _getRewardDistributorStorage().pendingValidatorRewards[validatorAddress];
    }

    /// @inheritdoc IRewardDistributor
    function totalPendingRewards() external view override returns (uint256) {
        return _getRewardDistributorStorage().totalPending;
    }

    // ========== ADMIN ==========

    /// @inheritdoc IRewardDistributor
    function setFeeAggregator(address newAggregator) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newAggregator == address(0)) revert ZeroAddress();
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        address oldAggregator = $.feeAggregator;
        $.feeAggregator = newAggregator;
        emit FeeAggregatorUpdated(oldAggregator, newAggregator);
    }

    /// @inheritdoc IRewardDistributor
    function setDelegationPool(address newPool) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        address oldPool = address($.delegationPool);
        $.delegationPool = IDelegationPool(newPool);
        emit DelegationPoolUpdated(oldPool, newPool);
    }

    /// @notice Set the ConsensusRegistry address
    function setConsensusRegistry(address newRegistry) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newRegistry == address(0)) revert ZeroAddress();
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        address oldRegistry = address($.consensusRegistry);
        $.consensusRegistry = IConsensusRegistry(newRegistry);
        emit ConsensusRegistryUpdated(oldRegistry, newRegistry);
    }

    /// @inheritdoc IRewardDistributor
    function getRewardRecipient(address validatorAddress) external view override returns (address) {
        address recipient = _getRewardDistributorStorage().rewardRecipients[validatorAddress];
        return recipient == address(0) ? validatorAddress : recipient;
    }

    /// @inheritdoc IRewardDistributor
    function setRewardRecipient(address recipient) external override {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        address oldRecipient = $.rewardRecipients[msg.sender];
        $.rewardRecipients[msg.sender] = recipient;
        emit RewardRecipientUpdated(msg.sender, oldRecipient, recipient);
    }

    // ========== ACCUMULATOR ==========

    /// @inheritdoc IRewardDistributor
    function accumulator() external view override returns (address) {
        return _getRewardDistributorStorage().accumulator;
    }

    /// @inheritdoc IRewardDistributor
    function setAccumulator(address newAccumulator) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        address oldAccumulator = $.accumulator;
        $.accumulator = newAccumulator;
        emit AccumulatorUpdated(oldAccumulator, newAccumulator);
    }

    /// @inheritdoc IRewardDistributor
    function targetApyBps() external view override returns (uint256) {
        return _getRewardDistributorStorage().targetApyBps;
    }

    uint256 public constant MAX_APY_BPS = 10_000; // 100% max

    /// @inheritdoc IRewardDistributor
    function setTargetApyBps(uint256 newApyBps) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newApyBps > MAX_APY_BPS) revert InvalidApyBps();
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        uint256 oldApyBps = $.targetApyBps;
        $.targetApyBps = newApyBps;
        emit TargetApyBpsUpdated(oldApyBps, newApyBps);
    }

    /// @inheritdoc IRewardDistributor
    function openTierTargetApyBps() external view override returns (uint256) {
        return _getRewardDistributorStorage().openTierTargetApyBps;
    }

    /// @inheritdoc IRewardDistributor
    function setOpenTierTargetApyBps(uint256 newApyBps) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newApyBps > MAX_APY_BPS) revert InvalidApyBps();
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        uint256 oldApyBps = $.openTierTargetApyBps;
        $.openTierTargetApyBps = newApyBps;
        emit OpenTierApyBpsUpdated(oldApyBps, newApyBps);
    }

    /// @dev Pull RLS from the RLSAccumulator to cover the shortfall between fees and the per-validator 
    ///      target reward for the current epoch.
    function _pullAccumulatorTopUp(
        uint256 currentRewards,
        uint256 targetReward
    ) internal returns (uint256) {
        RewardDistributorStorage storage $ = _getRewardDistributorStorage();

        if ($.accumulator == address(0)) return currentRewards;

        if (targetReward <= currentRewards) {
            return currentRewards;
        }

        uint256 shortfall = targetReward - currentRewards;
        uint256 available = $.rls.balanceOf($.accumulator);
        uint256 pullAmount = shortfall < available ? shortfall : available;

        if (pullAmount > 0) {
            try IERC20($.rls).transferFrom($.accumulator, address(this), pullAmount) returns (bool ok) {
                if (ok) {
                    $.totalPending += pullAmount;
                    emit AccumulatorTopUp(pullAmount, targetReward, currentRewards + pullAmount);
                    return currentRewards + pullAmount;
                } else {
                    emit AccumulatorTopUpFailed(pullAmount);
                }
            } catch {
                emit AccumulatorTopUpFailed(pullAmount);
            }
        }

        return currentRewards;
    }

    // ========== EMERGENCY ==========

    /// @notice Emergency function to recover stuck ERC-20 tokens
    /// @dev Cannot recover RLS that is pending distribution or unclaimed
    function recoverTokens(address token, address to, uint256 amount) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (to == address(0)) revert ZeroAddress();

        RewardDistributorStorage storage $ = _getRewardDistributorStorage();
        if (token == address($.rls)) {
            uint256 reserved = $.totalPending + $.totalUnclaimedRewards;
            uint256 balance = $.rls.balanceOf(address(this));
            uint256 available = balance > reserved ? balance - reserved : 0;
            if (amount > available) revert InsufficientBalance(amount, available);
        }

        IERC20(token).safeTransfer(to, amount);
    }
}
