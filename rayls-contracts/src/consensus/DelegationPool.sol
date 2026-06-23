// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {ReentrancyGuard} from "solady/utils/ReentrancyGuard.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {MerkleProof} from "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";
import {IDelegationPool} from "../interfaces/IDelegationPool.sol";
import {IConsensusRegistry} from "../interfaces/IConsensusRegistry.sol";

/**
 * @title DelegationPool
 * @notice A Rayls Contract
 *
 * @notice Manages multi-delegator staking pools for Rayls validators
 * @dev Deployed by governance post-genesis; ConsensusRegistry calls into it for reward/slash distribution
 * @dev Uses a share-based accounting model (MasterChef-style) for O(1) reward distribution and slashing
 * @dev UUPS upgradeable with AccessControl
 * @dev Delegations are made with ERC-20 RLS tokens; rewards are distributed in ERC-20 RLS
 */
contract DelegationPool is
    Initializable,
    UUPSUpgradeable,
    AccessControlUpgradeable,
    ReentrancyGuard,
    IDelegationPool
{
    using SafeERC20 for IERC20;
    uint256 public constant PRECISION = 1e18;
    uint256 public constant MAX_COMMISSION_BPS = 10_000;
    uint256 public constant MAX_COMMISSION_INCREASE_BPS = 500; // Max 5% increase per update
    uint256 public constant MAX_INITIAL_COMMISSION_BPS = 2000;  // 20% max

    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    /// @custom:storage-location erc7201:delegationpool.storage.v1
    struct DelegationPoolStorage {
        /// @notice The RLS token contract (ERC-20 staking token)
        IERC20 rls;
        /// @notice The ConsensusRegistry contract
        IConsensusRegistry consensusRegistry;
        /// @notice Configurable delegation parameters
        DelegationConfig config;
        /// @notice Pool state per validator
        mapping(address => ValidatorPool) validatorPools;
        /// @notice Whether a pool has been registered
        mapping(address => bool) poolRegistered;
        /// @notice Delegator positions: validator => delegator => position
        mapping(address => mapping(address => DelegatorPosition)) positions;
        /// @notice Custom reward recipient per delegator per validator
        mapping(address => mapping(address => address)) rewardRecipients;
        /// @notice Custom commission recipient per validator
        mapping(address => address) commissionRecipients;
        /// @notice Pending commission increases per validator
        mapping(address => PendingCommission) pendingCommissions;
        /// @notice Address of the RewardDistributor contract
        address rewardDistributor;
        /// @notice Merkle root of the PrelaunchLockbox snapshot — `(address, balance)` leaves
        bytes32 whitelistRoot;
        /// @notice When true, `delegate` requires prior Merkle-proof verification
        bool whitelistEnabled;
        /// @notice Addresses that have successfully submitted a proof
        mapping(address => bool) whitelistVerified;
    }

    // keccak256(abi.encode(uint256(keccak256("delegationpool.storage.v1")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant DELEGATION_POOL_STORAGE_LOCATION =
        0x88221c9a15d56692c82fe5e6f956bdf53eb61854017aba5bacf6b40976119e00;

    function _getDelegationPoolStorage() private pure returns (DelegationPoolStorage storage $) {
        assembly {
            $.slot := DELEGATION_POOL_STORAGE_LOCATION
        }
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        address rls_,
        address consensusRegistry_,
        address admin_,
        DelegationConfig memory config_
    ) external initializer {
        if (rls_ == address(0)) revert ZeroAddress();
        if (consensusRegistry_ == address(0)) revert ZeroAddress();
        if (admin_ == address(0)) revert ZeroAddress();
        _validateConfig(config_);

        __AccessControl_init();
        __UUPSUpgradeable_init();

        _grantRole(DEFAULT_ADMIN_ROLE, admin_);
        _grantRole(UPGRADER_ROLE, admin_);

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        $.rls = IERC20(rls_);
        $.consensusRegistry = IConsensusRegistry(consensusRegistry_);
        $.config = config_;
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    /// @inheritdoc IDelegationPool
    function rlsToken() external view override returns (address) {
        return address(_getDelegationPoolStorage().rls);
    }

    /// @notice Get the ConsensusRegistry address
    function consensusRegistry() external view returns (address) {
        return address(_getDelegationPoolStorage().consensusRegistry);
    }

    /// @notice Get the RewardDistributor address
    function rewardDistributor() external view returns (address) {
        return _getDelegationPoolStorage().rewardDistributor;
    }

    /// @inheritdoc IDelegationPool
    function poolRegistered(address validatorAddress) external view override returns (bool) {
        return _getDelegationPoolStorage().poolRegistered[validatorAddress];
    }

    modifier onlyConsensusRegistry() {
        if (msg.sender != address(_getDelegationPoolStorage().consensusRegistry))
            revert OnlyConsensusRegistry();
        _;
    }

    modifier onlyRewardSources() {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (msg.sender != address($.consensusRegistry) && msg.sender != $.rewardDistributor)
            revert OnlyRewardSources();
        _;
    }

    // =========================================================================
    //                          Governance
    // =========================================================================

    /// @notice Update delegation configuration parameters
    /// @param newConfig The new configuration to apply
    function updateConfig(
        DelegationConfig calldata newConfig
    ) external onlyRole(DEFAULT_ADMIN_ROLE) {
        _validateConfig(newConfig);
        _getDelegationPoolStorage().config = newConfig;
        emit ConfigUpdated(newConfig);
    }

    /// @notice Set the RewardDistributor contract address
    /// @param newRewardDistributor The new RewardDistributor address
    function setRewardDistributor(address newRewardDistributor) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newRewardDistributor == address(0)) revert ZeroAddress();
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        address oldDistributor = $.rewardDistributor;
        $.rewardDistributor = newRewardDistributor;
        emit RewardDistributorUpdated(oldDistributor, newRewardDistributor);
    }

    /// @inheritdoc IDelegationPool
    function setWhitelistRoot(bytes32 newRoot) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        bytes32 oldRoot = $.whitelistRoot;
        $.whitelistRoot = newRoot;
        emit WhitelistRootUpdated(oldRoot, newRoot);
        if (!$.whitelistEnabled) {
            $.whitelistEnabled = true;
            emit WhitelistEnabledUpdated(true);
        }
    }

    /// @inheritdoc IDelegationPool
    function disableWhitelist() external override onlyRole(DEFAULT_ADMIN_ROLE) {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if ($.whitelistEnabled) {
            $.whitelistEnabled = false;
            emit WhitelistEnabledUpdated(false);
        }
    }

    // =========================================================================
    //                          Validator Functions
    // =========================================================================

    /// @inheritdoc IDelegationPool
    function registerPool(uint256 commissionBps) external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if ($.poolRegistered[msg.sender])
            revert PoolAlreadyRegistered(msg.sender);
        if (commissionBps > MAX_COMMISSION_BPS)
            revert InvalidCommission(commissionBps);

        // verify caller is allowlisted
        if (!$.consensusRegistry.isAllowlisted(msg.sender))
            revert NotAllowlisted(msg.sender);

        // verify initial commission is less than the maximum allowed
        if (commissionBps > MAX_INITIAL_COMMISSION_BPS)
            revert InvalidInitialCommission(commissionBps);

        // verify caller is an active validator
        IConsensusRegistry.ValidatorInfo memory info = $.consensusRegistry.getValidator(msg.sender);
        if (
            info.currentStatus != IConsensusRegistry.ValidatorStatus.Active &&
            info.currentStatus !=
            IConsensusRegistry.ValidatorStatus.PendingActivation
        ) revert NotActiveValidator(msg.sender);

        $.poolRegistered[msg.sender] = true;
        $.validatorPools[msg.sender] = ValidatorPool({
            totalDelegated: 0,
            commissionBps: commissionBps,
            rewardPerShareAccum: 0,
            pendingValidatorRewards: 0,
            acceptingDelegations: true,
            slashPerShareAccum: 0,
            openTierDelegated: 0,
            openRewardPerShareAccum: 0
        });

        emit PoolRegistered(msg.sender, commissionBps);
    }

    /// @inheritdoc IDelegationPool
    /// @dev Increases are queued with a delay; decreases apply immediately and cancel any pending increase
    function updateCommission(uint256 newCommissionBps) external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);
        if (newCommissionBps > MAX_COMMISSION_BPS)
            revert InvalidCommission(newCommissionBps);

        ValidatorPool storage pool = $.validatorPools[msg.sender];
        uint256 oldBps = pool.commissionBps;

        if (newCommissionBps > oldBps) {
            // commission increase: validate and schedule with delay
            uint256 increase = newCommissionBps - oldBps;
            if (increase > MAX_COMMISSION_INCREASE_BPS)
                revert CommissionIncreaseExceedsLimit(increase, MAX_COMMISSION_INCREASE_BPS);

            if ($.pendingCommissions[msg.sender].effectiveEpoch != 0)
                revert PendingCommissionExists($.pendingCommissions[msg.sender].effectiveEpoch);

            uint32 currentEpoch = $.consensusRegistry.getCurrentEpoch();
            uint32 effectiveEpoch = currentEpoch + $.config.commissionDelayEpochs;

            $.pendingCommissions[msg.sender] = PendingCommission({
                newBps: newCommissionBps,
                effectiveEpoch: effectiveEpoch
            });

            emit CommissionUpdateScheduled(msg.sender, oldBps, newCommissionBps, effectiveEpoch);
        } else {
            // commission decrease: apply immediately and cancel any pending increase
            pool.commissionBps = newCommissionBps;

            if ($.pendingCommissions[msg.sender].effectiveEpoch != 0) {
                uint256 cancelledBps = $.pendingCommissions[msg.sender].newBps;
                delete $.pendingCommissions[msg.sender];
                emit PendingCommissionCancelled(msg.sender, cancelledBps);
            }

            emit CommissionUpdated(msg.sender, oldBps, newCommissionBps);
        }
    }

    /// @inheritdoc IDelegationPool
    function activatePendingCommission() external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);

        PendingCommission storage pending = $.pendingCommissions[msg.sender];
        if (pending.effectiveEpoch == 0)
            revert NoPendingCommission();

        uint32 currentEpoch = $.consensusRegistry.getCurrentEpoch();
        if (currentEpoch < pending.effectiveEpoch)
            revert CommissionNotYetEffective(currentEpoch, pending.effectiveEpoch);

        ValidatorPool storage pool = $.validatorPools[msg.sender];
        uint256 oldBps = pool.commissionBps;
        uint256 newBps = pending.newBps;

        pool.commissionBps = newBps;
        delete $.pendingCommissions[msg.sender];

        emit CommissionUpdated(msg.sender, oldBps, newBps);
    }

    /// @inheritdoc IDelegationPool
    function cancelPendingCommission() external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);

        PendingCommission storage pending = $.pendingCommissions[msg.sender];
        if (pending.effectiveEpoch == 0)
            revert NoPendingCommission();

        uint256 cancelledBps = pending.newBps;
        delete $.pendingCommissions[msg.sender];

        emit PendingCommissionCancelled(msg.sender, cancelledBps);
    }

    /// @inheritdoc IDelegationPool
    function setAcceptingDelegations(bool accepting) external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);

        $.validatorPools[msg.sender].acceptingDelegations = accepting;

        emit DelegationsToggled(msg.sender, accepting);
    }

    /// @inheritdoc IDelegationPool
    function claimCommission() external override nonReentrant {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);

        ValidatorPool storage pool = $.validatorPools[msg.sender];
        uint256 commission = pool.pendingValidatorRewards;
        if (commission == 0) revert NoCommissionToClaim();

        pool.pendingValidatorRewards = 0;

        // send to custom recipient if set, otherwise to validator
        address recipient = $.commissionRecipients[msg.sender];
        if (recipient == address(0)) {
            recipient = msg.sender;
        }

        $.rls.safeTransfer(recipient, commission);

        emit CommissionClaimed(msg.sender, commission);
    }

    /// @inheritdoc IDelegationPool
    function setCommissionRecipient(address recipient) external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[msg.sender])
            revert PoolNotRegistered(msg.sender);

        $.commissionRecipients[msg.sender] = recipient;
        emit CommissionRecipientUpdated(msg.sender, recipient);
    }

    /// @inheritdoc IDelegationPool
    function getCommissionRecipient(address validatorAddress) external view override returns (address) {
        address recipient = _getDelegationPoolStorage().commissionRecipients[validatorAddress];
        return recipient == address(0) ? validatorAddress : recipient;
    }

    // =========================================================================
    //                          Delegator Functions
    // =========================================================================

    /// @inheritdoc IDelegationPool
    function delegate(
        address validatorAddress,
        uint256 amount
    ) external override nonReentrant {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        bool isOpenTier = $.whitelistEnabled && !$.whitelistVerified[msg.sender];
        _delegate(validatorAddress, amount, isOpenTier);
    }

    /// @inheritdoc IDelegationPool
    function delegateWithProof(
        address validatorAddress,
        uint256 amount,
        uint256 lockboxBalance,
        bytes32[] calldata proof
    ) external override nonReentrant {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if ($.whitelistEnabled && !$.whitelistVerified[msg.sender]) {
            // Leaf = keccak256(bytes.concat(keccak256(abi.encode(address, uint256))))
            // MUST match StandardMerkleTree.of(rows, ['address', 'uint256']) — OZ canonical.
            bytes32 leaf = keccak256(
                bytes.concat(keccak256(abi.encode(msg.sender, lockboxBalance)))
            );
            if (!MerkleProof.verifyCalldata(proof, $.whitelistRoot, leaf))
                revert NotWhitelisted(msg.sender);
            $.whitelistVerified[msg.sender] = true;
            emit WhitelistVerified(msg.sender);
        }
        // Proof submission always routes to Track A (whitelisted tier)
        _delegate(validatorAddress, amount, false);
    }

    function _delegate(address validatorAddress, uint256 amount, bool isOpenTier) internal {
        if (validatorAddress == address(0)) revert ZeroAddress();
        if (amount == 0) revert ZeroAmount();

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();

        if (!$.poolRegistered[validatorAddress])
            revert PoolNotRegistered(validatorAddress);

        // verify validator is still allowlisted
        if (!$.consensusRegistry.isAllowlisted(validatorAddress))
            revert NotAllowlisted(validatorAddress);

        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        if (!pool.acceptingDelegations)
            revert PoolNotAcceptingDelegations(validatorAddress);
        if (amount < $.config.minDelegation)
            revert InsufficientDelegation(amount, $.config.minDelegation);

        DelegatorPosition storage pos = $.positions[validatorAddress][msg.sender];

        // Existing positions keep their locked tier so open-tier holders can top up
        // even after the whitelist is disabled (when isOpenTier would otherwise be false).
        if (pos.amount > 0) isOpenTier = pos.openTier;

        // settle pending rewards and slashes before changing position
        _settlePosition(pool, pos);

        // check per-delegator max (using post-settlement amount)
        uint256 newDelegatorTotal = pos.amount + amount;
        if (newDelegatorTotal > $.config.maxDelegation)
            revert ExceedsMaxDelegation(newDelegatorTotal, $.config.maxDelegation);

        // check combined pool cap (Track A + Track B together)
        uint256 combinedPoolTotal = pool.totalDelegated + pool.openTierDelegated;
        if (combinedPoolTotal + amount > $.config.maxValidatorDelegation)
            revert ExceedsMaxValidatorDelegation(
                combinedPoolTotal + amount,
                $.config.maxValidatorDelegation
            );

        // transfer RLS tokens from delegator
        $.rls.safeTransferFrom(msg.sender, address(this), amount);

        pos.amount = newDelegatorTotal;
        pos.openTier = isOpenTier;

        uint256 accum = isOpenTier ? pool.openRewardPerShareAccum : pool.rewardPerShareAccum;
        pos.rewardDebt = (pos.amount * accum) / PRECISION;
        // Ceiling division for slashDebt — counterpart to ceiling in _settlePosition
        pos.slashDebt = (pos.amount * pool.slashPerShareAccum + PRECISION - 1) / PRECISION;
        // Record delegation epoch — same-epoch rewards are excluded to prevent
        // sandwich attacks on distributePoolRewards.
        pos.lastDelegateEpoch = uint64($.consensusRegistry.getCurrentEpoch());

        if (isOpenTier) {
            pool.openTierDelegated += amount;
        } else {
            pool.totalDelegated += amount;
        }

        emit Delegated(validatorAddress, msg.sender, amount);
    }

    /// @inheritdoc IDelegationPool
    /// @notice Request undelegation (starts unbonding period based on CURRENT config)
    /// @dev The unbonding period is locked at request time and not affected by future config changes
    /// @dev SLASH PROTECTION: The undelegation amount is removed from pool.totalDelegated immediately,
    ///      so any future slashes (via applyPoolSlash) only affect remaining active delegators.
    ///      The unbonding delegator receives their full undelegateAmount at completion.
    ///      This is an intentional design decision — delegators who have committed to exit
    ///      should not bear slash risk for validator misbehavior after their exit request.
    function requestUndelegation(
        address validatorAddress,
        uint256 amount
    ) external override nonReentrant {
        if (amount == 0) revert ZeroAmount();

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress])
            revert PoolNotRegistered(validatorAddress);

        DelegatorPosition storage pos = $.positions[validatorAddress][
            msg.sender
        ];
        if (pos.undelegateEpoch != 0)
            revert PendingUndelegationExists(pos.undelegateEpoch);

        ValidatorPool storage pool = $.validatorPools[validatorAddress];

        // settle pending rewards and slashes before changing position
        _settlePosition(pool, pos);

        // check balance after settlement (slash may have reduced it)
        if (pos.amount < amount)
            revert InsufficientBalance(amount, pos.amount);

        // update position
        pos.amount -= amount;
        uint256 accum = pos.openTier ? pool.openRewardPerShareAccum : pool.rewardPerShareAccum;
        pos.rewardDebt = (pos.amount * accum) / PRECISION;
        // Ceiling division for slashDebt — counterpart to ceiling in _settlePosition
        pos.slashDebt =
            (pos.amount * pool.slashPerShareAccum + PRECISION - 1) /
            PRECISION;
        pos.undelegateAmount = amount;

        uint32 currentEpoch = $.consensusRegistry.getCurrentEpoch();
        pos.undelegateEpoch = uint64(currentEpoch) + $.config.unbondingEpochs;

        // reduce the correct track's total
        if (pos.openTier) {
            pool.openTierDelegated -= amount;
        } else {
            pool.totalDelegated -= amount;
        }

        emit UndelegationRequested(
            validatorAddress,
            msg.sender,
            amount,
            pos.undelegateEpoch
        );
    }

    /// @inheritdoc IDelegationPool
    /// @dev SLASH PROTECTION: Returns the full undelegateAmount without deducting post-exit slashes.
    ///      Unbonding tokens were removed from pool.totalDelegated at request time, so
    ///      slashPerShareAccum increases during the unbonding period do not affect this amount.
    function completeUndelegation(address validatorAddress) external override nonReentrant {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        DelegatorPosition storage pos = $.positions[validatorAddress][
            msg.sender
        ];
        if (pos.undelegateEpoch == 0) revert NothingToUndelegate();

        uint32 currentEpoch = $.consensusRegistry.getCurrentEpoch();
        if (currentEpoch < pos.undelegateEpoch)
            revert UnbondingNotComplete(currentEpoch, pos.undelegateEpoch);

        uint256 amount = pos.undelegateAmount;

        pos.undelegateEpoch = 0;
        pos.undelegateAmount = 0;

        if (amount > 0) {
            $.rls.safeTransfer(msg.sender, amount);
        }

        emit UndelegationCompleted(validatorAddress, msg.sender, amount);
    }

    /// @inheritdoc IDelegationPool
    function claimDelegationRewards(
        address validatorAddress
    ) external override nonReentrant {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress])
            revert PoolNotRegistered(validatorAddress);

        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        DelegatorPosition storage pos = $.positions[validatorAddress][
            msg.sender
        ];

        // settle pending rewards and slashes
        _settlePosition(pool, pos);

        uint256 totalRewards = pos.pendingRewards;
        if (totalRewards == 0) revert NoPendingRewards();

        pos.pendingRewards = 0;

        // send to custom recipient if set, otherwise to delegator
        address recipient = $.rewardRecipients[validatorAddress][msg.sender];
        if (recipient == address(0)) {
            recipient = msg.sender;
        }

        $.rls.safeTransfer(recipient, totalRewards);

        emit DelegationRewardsClaimed(
            validatorAddress,
            msg.sender,
            totalRewards
        );
    }

    /// @inheritdoc IDelegationPool
    function setRewardRecipient(address validatorAddress, address recipient) external override {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress])
            revert PoolNotRegistered(validatorAddress);

        $.rewardRecipients[validatorAddress][msg.sender] = recipient;
        emit RewardRecipientUpdated(msg.sender, validatorAddress, recipient);
    }

    /// @inheritdoc IDelegationPool
    function getRewardRecipient(
        address validatorAddress,
        address delegator
    ) external view override returns (address) {
        address recipient = _getDelegationPoolStorage().rewardRecipients[validatorAddress][delegator];
        return recipient == address(0) ? delegator : recipient;
    }

    // =========================================================================
    //                     ConsensusRegistry Integration
    // =========================================================================

    /// @inheritdoc IDelegationPool
    /// @dev Caller must transfer RLS tokens to this contract before calling
    /// @dev Splits amount proportionally between Track A and Track B by stake
    function distributePoolRewards(
        address validatorAddress,
        uint256 amount
    ) external override onlyRewardSources {
        if (amount == 0) return;

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress]) return;

        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        uint256 totalAll = pool.totalDelegated + pool.openTierDelegated;

        if (totalAll == 0) {
            pool.pendingValidatorRewards += amount;
            emit PoolRewardsDistributed(validatorAddress, amount);
            return;
        }

        // proportional split by stake
        uint256 trackAAmount = (amount * pool.totalDelegated) / totalAll;
        uint256 trackBAmount = amount - trackAAmount;
        _distributePoolRewardsInternal(validatorAddress, pool, trackAAmount, trackBAmount);
    }

    /// @inheritdoc IDelegationPool
    /// @dev RewardDistributor must transfer (trackAAmount + trackBAmount) RLS before calling
    function distributePoolRewards(
        address validatorAddress,
        uint256 trackAAmount,
        uint256 trackBAmount
    ) external override onlyRewardSources {
        if (trackAAmount == 0 && trackBAmount == 0) return;

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress]) return;

        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        _distributePoolRewardsInternal(validatorAddress, pool, trackAAmount, trackBAmount);
    }

    function _distributePoolRewardsInternal(
        address validatorAddress,
        ValidatorPool storage pool,
        uint256 trackAAmount,
        uint256 trackBAmount
    ) internal {
        uint256 totalAll = pool.totalDelegated + pool.openTierDelegated;

        if (totalAll == 0) {
            pool.pendingValidatorRewards += trackAAmount + trackBAmount;
            emit PoolRewardsDistributed(validatorAddress, trackAAmount + trackBAmount);
            return;
        }

        // Track A: commission + accumulator update
        if (trackAAmount > 0) {
            if (pool.totalDelegated == 0) {
                pool.pendingValidatorRewards += trackAAmount;
            } else {
                uint256 commissionA = (trackAAmount * pool.commissionBps) / MAX_COMMISSION_BPS;
                pool.pendingValidatorRewards += commissionA;
                pool.rewardPerShareAccum +=
                    ((trackAAmount - commissionA) * PRECISION) / pool.totalDelegated;
            }
        }

        // Track B: commission + accumulator update
        if (trackBAmount > 0) {
            if (pool.openTierDelegated == 0) {
                pool.pendingValidatorRewards += trackBAmount;
            } else {
                uint256 commissionB = (trackBAmount * pool.commissionBps) / MAX_COMMISSION_BPS;
                pool.pendingValidatorRewards += commissionB;
                pool.openRewardPerShareAccum +=
                    ((trackBAmount - commissionB) * PRECISION) / pool.openTierDelegated;
            }
        }

        emit PoolRewardsDistributed(validatorAddress, trackAAmount + trackBAmount);
    }

    /// @inheritdoc IDelegationPool
    /// @notice Slashes delegated stake proportionally and transfers slashed tokens to ConsensusRegistry
    function applyPoolSlash(
        address validatorAddress,
        uint256 amount
    ) external override onlyConsensusRegistry returns (uint256 effectiveSlash) {
        if (amount == 0) return 0;

        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        if (!$.poolRegistered[validatorAddress]) return 0;

        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        uint256 totalAll = pool.totalDelegated + pool.openTierDelegated;

        if (totalAll == 0) {
            emit PoolSlashed(validatorAddress, 0);
            return 0;
        }

        // cap slash at combined delegated total
        effectiveSlash = amount > totalAll ? totalAll : amount;

        // Compute per-share slash increment (rounds down due to integer division).
        // Derive actualSlash from the rounded value so the transfer matches what
        // the accumulator records. Dust stays in the contract as a solvency buffer,
        // preventing balance < aggregate claims over many slashes.
        uint256 slashPerShare = (effectiveSlash * PRECISION) / totalAll;
        uint256 actualSlash = (slashPerShare * totalAll) / PRECISION;

        pool.slashPerShareAccum += slashPerShare;

        // Reduce each track proportionally so their totals stay accurate for reward distribution.
        uint256 slashA = (actualSlash * pool.totalDelegated) / totalAll;
        uint256 slashB = actualSlash - slashA;
        pool.totalDelegated -= slashA;
        pool.openTierDelegated -= slashB;

        // transfer only what the accumulator accounts for
        $.rls.safeTransfer(address($.consensusRegistry), actualSlash);

        // return actualSlash so ConsensusRegistry.slashedFunds tracks what was received
        effectiveSlash = actualSlash;

        emit PoolSlashed(validatorAddress, effectiveSlash);
    }

    // =========================================================================
    //                            View Functions
    // =========================================================================

    /// @inheritdoc IDelegationPool
    function getTotalDelegatedStake(
        address validatorAddress
    ) external view override returns (uint256) {
        ValidatorPool storage pool = _getDelegationPoolStorage().validatorPools[validatorAddress];
        return pool.totalDelegated + pool.openTierDelegated;
    }

    /// @inheritdoc IDelegationPool
    function getTotalOpenTierDelegatedStake(
        address validatorAddress
    ) external view override returns (uint256) {
        return _getDelegationPoolStorage().validatorPools[validatorAddress].openTierDelegated;
    }

    /// @inheritdoc IDelegationPool
    function getPendingRewards(
        address validatorAddress,
        address delegator
    ) external view override returns (uint256) {
        (, uint256 rewards) = _getEffectivePosition(validatorAddress, delegator);
        return rewards;
    }

    /// @inheritdoc IDelegationPool
    function getDelegatorPosition(
        address validatorAddress,
        address delegator
    ) external view override returns (DelegatorPosition memory) {
        return _getDelegationPoolStorage().positions[validatorAddress][delegator];
    }

    /// @inheritdoc IDelegationPool
    function getValidatorPool(
        address validatorAddress
    ) external view override returns (ValidatorPool memory) {
        return _getDelegationPoolStorage().validatorPools[validatorAddress];
    }

    /// @inheritdoc IDelegationPool
    function getDelegationConfig()
        external
        view
        override
        returns (DelegationConfig memory)
    {
        return _getDelegationPoolStorage().config;
    }

    /// @inheritdoc IDelegationPool
    function getPendingCommission(
        address validatorAddress
    ) external view override returns (PendingCommission memory) {
        return _getDelegationPoolStorage().pendingCommissions[validatorAddress];
    }

    /// @inheritdoc IDelegationPool
    function getEffectivePosition(
        address validatorAddress,
        address delegator
    ) external view override returns (uint256 effectiveAmount, uint256 pendingRewards) {
        return _getEffectivePosition(validatorAddress, delegator);
    }

    /// @inheritdoc IDelegationPool
    function whitelistRoot() external view override returns (bytes32) {
        return _getDelegationPoolStorage().whitelistRoot;
    }

    /// @inheritdoc IDelegationPool
    function whitelistEnabled() external view override returns (bool) {
        return _getDelegationPoolStorage().whitelistEnabled;
    }

    /// @inheritdoc IDelegationPool
    function isWhitelistVerified(address account) external view override returns (bool) {
        return _getDelegationPoolStorage().whitelistVerified[account];
    }

    // =========================================================================
    //                            Internals
    // =========================================================================

    /// @dev Validates delegation configuration invariants.
    function _validateConfig(DelegationConfig memory config_) internal pure {
        if (config_.minDelegation == 0) revert InvalidConfig();
        if (config_.maxDelegation == 0) revert InvalidConfig();
        if (config_.maxValidatorDelegation == 0) revert InvalidConfig();
        if (config_.maxDelegation > config_.maxValidatorDelegation) revert InvalidConfig();
        if (config_.unbondingEpochs == 0) revert InvalidConfig();
        if (config_.commissionDelayEpochs == 0) revert InvalidConfig();
    }

    /// @dev Pure settlement arithmetic shared by _settlePosition (mutating) and _getEffectivePosition (view).
    /// Slash-first ordering prevents insolvency: rewards are computed on the post-slash amount,
    /// which is consistent with how rewardPerShareAccum was updated on a reduced totalDelegated.
    /// Returns (effectiveAmount, rewardDelta, newRewardDebt, newSlashDebt).
    /// On full slash all four return values are zero.
    function _settleArithmetic(
        ValidatorPool storage pool,
        DelegatorPosition storage pos,
        uint64 currentEpoch
    ) internal view returns (
        uint256 effectiveAmount,
        uint256 rewardDelta,
        uint256 newRewardDebt,
        uint256 newSlashDebt
    ) {
        uint256 accum = pos.openTier ? pool.openRewardPerShareAccum : pool.rewardPerShareAccum;

        // 1. slash — ceiling division so per-delegator sum >= pool's actualSlash (solvency invariant)
        uint256 accumulatedSlash = (pos.amount * pool.slashPerShareAccum + PRECISION - 1) / PRECISION;
        uint256 slashAmount = accumulatedSlash > pos.slashDebt ? accumulatedSlash - pos.slashDebt : 0;

        uint256 scaledRewardDebt = pos.rewardDebt;
        if (slashAmount > 0) {
            if (slashAmount >= pos.amount) return (0, 0, 0, 0);
            effectiveAmount = pos.amount - slashAmount;
            // round UP to prevent overcrediting rewards after slash
            scaledRewardDebt = (pos.rewardDebt * effectiveAmount + pos.amount - 1) / pos.amount;
        } else {
            effectiveAmount = pos.amount;
        }

        // 2. rewards on post-slash amount — skip same-epoch delegations (sandwich protection)
        if (pos.lastDelegateEpoch != currentEpoch) {
            uint256 accumulated = (effectiveAmount * accum) / PRECISION;
            if (accumulated > scaledRewardDebt) {
                rewardDelta = accumulated - scaledRewardDebt;
            }
        }

        // 3. updated debts
        newRewardDebt = (effectiveAmount * accum) / PRECISION;
        newSlashDebt = (effectiveAmount * pool.slashPerShareAccum + PRECISION - 1) / PRECISION;
    }

    function _getEffectivePosition(
        address validatorAddress,
        address delegator
    ) internal view returns (uint256 effectiveAmount, uint256 pendingRewards) {
        DelegationPoolStorage storage $ = _getDelegationPoolStorage();
        ValidatorPool storage pool = $.validatorPools[validatorAddress];
        DelegatorPosition storage pos = $.positions[validatorAddress][delegator];
        if (pos.amount == 0) return (0, pos.pendingRewards);
        uint64 currentEpoch = uint64($.consensusRegistry.getCurrentEpoch());
        uint256 rewardDelta;
        (effectiveAmount, rewardDelta,,) = _settleArithmetic(pool, pos, currentEpoch);
        pendingRewards = pos.pendingRewards + rewardDelta;
    }

    /// @dev Must be called before any position mutation.
    function _settlePosition(
        ValidatorPool storage pool,
        DelegatorPosition storage pos
    ) internal {
        if (pos.amount == 0) return;
        uint64 currentEpoch = uint64(_getDelegationPoolStorage().consensusRegistry.getCurrentEpoch());
        (uint256 effectiveAmount, uint256 rewardDelta, uint256 newRewardDebt, uint256 newSlashDebt) =
            _settleArithmetic(pool, pos, currentEpoch);
        pos.amount = effectiveAmount;
        if (rewardDelta > 0) pos.pendingRewards += rewardDelta;
        pos.rewardDebt = newRewardDebt;
        pos.slashDebt = newSlashDebt;
    }
}
