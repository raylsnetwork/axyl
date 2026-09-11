// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

// OpenZeppelin Upgradeable Contracts
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";

// OpenZeppelin Standard Contracts
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

// Interfaces
import {IFeeAggregator} from "../interfaces/IFeeAggregator.sol";
import {ISwapRouter} from "../interfaces/ISwapRouter.sol";
import {IAlgebraPool} from "../interfaces/IAlgebraPool.sol";
import {IRewardDistributor} from "../interfaces/IRewardDistributor.sol";
import {IOFT} from "../interfaces/IOFT.sol";

/**
 * @title FeeAggregator
 * @notice A Rayls Contract
 *
 * @notice Unified fee collection, swapping, and distribution contract
 * @dev Collects ERC-20 stablecoin fees (USDr, USDT, USDC, etc.)
 * @dev Swaps all fees to RLS via Algebra DEX, distributes to 3 recipients
 * @dev UUPS upgradeable with AccessControl
 *
 * Architecture:
 * - Algebra DEX for stablecoin → RLS swaps
 * - 3-category distribution (validators, ecosystem, burn) with per-category retry
 * - Epoch distribution triggered by keeper
 */
contract FeeAggregator is
    Initializable,
    UUPSUpgradeable,
    AccessControlUpgradeable,
    PausableUpgradeable,
    IFeeAggregator
{
    using SafeERC20 for IERC20;

    // ========== CONSTANTS ==========

    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    uint256 public constant MAX_BPS = 10_000;
    uint256 public constant MAX_STABLECOINS = 10;

    // ========== STORAGE (ERC-7201 Namespaced) ==========

    // WARN: The _deprecated_* fields below kept for testnet! They preserve the
    // exact storage layout used by live testnet state at slot
    // 0x8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39fc00.

    /// @custom:storage-location erc7201:feeaggregator.storage.v1.nonstandard
    struct FeeAggregatorStorage {
        // Tokens
        IERC20 rlsToken;
        address[] stablecoins;
        mapping(address => bool) isStablecoinSupportedMap;
        mapping(address => PoolConfig) poolConfigs;
        // Distribution
        DistributionConfig config;
        // -- DEPRECATED: Live testnet state
        ConfigProposal _deprecated_pendingProposal;
        uint256 _deprecated_configTimelockDelay;
        uint256 _deprecated_minTimelockDelay;
        uint256 _deprecated_maxTimelockDelay;
        // -- end deprecated --
        // Recipients
        address rewardDistributor;
        address ecosystemTreasury;
        // -- DEPRECATED: removed recipients (kept for storage layout compatibility)
        address _deprecated_softwarePartner;
        address _deprecated_operationsTreasury;
        // -- end deprecated --
        // Burn via LayerZero bridge to Ethereum
        address burnAddress;   // destination address on Ethereum
        // Algebra DEX
        ISwapRouter algebraRouter;
        // Safety
        uint256 minSwapAmount;
        uint256 maxSwapAmount;
        // -- DEPRECATED: 
        uint256 _deprecated_maxSlippageBps;
        // -- end deprecated --
        // Statistics
        uint256 totalSwapsExecuted;
        uint256 totalRlsDistributed;
        // Reentrancy
        uint256 reentrancyStatus;
        // Token configuration
        address usdrToken;
        // ── New fields appended at the end (added in tokenomics branch) ──
        // LayerZero OFT bridge config (moved to end to preserve layout)
        IOFT oftBridge;        // LayerZero OFT adapter contract
        uint32 dstEid;         // LayerZero destination endpoint ID (Ethereum)
        // Tracked RLS available for distribution (only from swapToRls)
        uint256 pendingRlsForDistribution;
        // per-category tracking for failed distribution retries
        uint256 pendingValidatorRls;
        uint256 pendingEcosystemRls;
        uint256 pendingBurnRls;
    }

    // WARN: This value does NOT derive from "feeaggregator.storage.v1" via the standard ERC-7201
    // formula. It was set incorrectly before genesis deployment and is now immutable because live
    // state is stored at this slot. The correct ERC-7201 derivation of "feeaggregator.storage.v1"
    // yields 0x97723226cbb2c5eba8cb8d1d9f606a619559178ba469ca1b06b8d9362615b100.
    // DO NOT CHANGE THIS VALUE — it would cause total state loss on the deployed proxy.
    bytes32 private constant STORAGE_LOCATION =
        0x8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39fc00;

    uint256 private constant NOT_ENTERED = 1;
    uint256 private constant ENTERED = 2;

    // ========== MODIFIERS ==========

    modifier nonReentrant() {
        _nonReentrantBefore();
        _;
        _nonReentrantAfter();
    }

    function _nonReentrantBefore() internal {
        FeeAggregatorStorage storage $ = _getStorage();
        if ($.reentrancyStatus == ENTERED) {
            revert Reentrancy();
        }
        $.reentrancyStatus = ENTERED;
    }

    function _nonReentrantAfter() internal {
        FeeAggregatorStorage storage $ = _getStorage();
        $.reentrancyStatus = NOT_ENTERED;
    }

    // ========== CONSTRUCTOR ==========

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    // ========== INITIALIZER ==========

    /**
     * @notice Initialize the contract
     * @param rlsToken_ The RLS token address
     * @param algebraRouter_ The Algebra SwapRouter address
     * @param rewardDistributor_ The RewardDistributor address
     * @param ecosystemTreasury_ The ecosystem treasury address
     * @param burnAddress_ The burn address
     * @param usdrToken_ The USDr token address (primary fee token). Pass address(0) to skip
     *                   and configure later via setUsdrToken — required when no DEX pool exists yet.
     * @param config_ Initial distribution configuration
     * @param admin_ Admin address (receives all roles)
     */
    function initialize(
        address rlsToken_,
        address algebraRouter_,
        address rewardDistributor_,
        address ecosystemTreasury_,
        address burnAddress_,
        address usdrToken_,
        DistributionConfig memory config_,
        address admin_
    ) external initializer {
        if (rlsToken_ == address(0)) revert ZeroAddress();
        if (admin_ == address(0)) revert ZeroAddress();

        __AccessControl_init();
        __Pausable_init();
        __UUPSUpgradeable_init();

        FeeAggregatorStorage storage $ = _getStorage();

        $.rlsToken = IERC20(rlsToken_);
        $.algebraRouter = ISwapRouter(algebraRouter_);
        $.rewardDistributor = rewardDistributor_;
        $.ecosystemTreasury = ecosystemTreasury_;
        $.burnAddress = burnAddress_;
        // Set USDr directly without setUsdrToken's stablecoin/pool validation —
        // at genesis there is no DEX yet. Validation kicks in for later updates.
        $.usdrToken = usdrToken_;

        // Validate and set config
        {
            uint256 total = config_.validatorPoolBps + config_.ecosystemBps + config_.burnBps;
            if (total != MAX_BPS) revert ConfigTotalMismatch(total);
            $.config = config_;
        }

        // Default safety limits
        $.minSwapAmount = 1000e6; // $1,000 minimum (6 decimals)
        $.maxSwapAmount = 100_000e6; // $100,000 maximum

        // Initialize reentrancy guard
        $.reentrancyStatus = NOT_ENTERED;

        // Grant roles
        _grantRole(DEFAULT_ADMIN_ROLE, admin_);
        _grantRole(KEEPER_ROLE, admin_);
        _grantRole(PAUSER_ROLE, admin_);
        _grantRole(UPGRADER_ROLE, admin_);
    }

    // ========== FEE RECEPTION ==========

    /// @inheritdoc IFeeAggregator
    function receiveFee(address stablecoin, uint256 amount) external override whenNotPaused nonReentrant {
        if (amount == 0) revert ZeroAmount();

        FeeAggregatorStorage storage $ = _getStorage();

        if (!$.isStablecoinSupportedMap[stablecoin]) revert UnsupportedStablecoin();

        IERC20(stablecoin).safeTransferFrom(msg.sender, address(this), amount);

        emit FeeReceived(stablecoin, amount, msg.sender);
    }

    /// @inheritdoc IFeeAggregator
    function pendingBalance(address stablecoin) external view override returns (uint256) {
        return IERC20(stablecoin).balanceOf(address(this));
    }

    // ========== TOKEN CONFIGURATION ==========

    /// @inheritdoc IFeeAggregator
    function usdrToken() external view override returns (address) {
        return _getStorage().usdrToken;
    }

    /// @inheritdoc IFeeAggregator
    function setUsdrToken(address newUsdrToken) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newUsdrToken == address(0)) revert ZeroAddress();
        FeeAggregatorStorage storage $ = _getStorage();

        // Validate that the token is a supported stablecoin with a configured pool
        if (!$.isStablecoinSupportedMap[newUsdrToken]) revert UnsupportedStablecoin();
        if ($.poolConfigs[newUsdrToken].pool == address(0)) revert PoolNotConfigured();

        address oldUsdrToken = $.usdrToken;
        $.usdrToken = newUsdrToken;
        emit UsdrTokenUpdated(oldUsdrToken, newUsdrToken);
    }

    // ========== PROCESSING ==========

    /// @inheritdoc IFeeAggregator
    function swapToRls(SwapParams calldata params)
        external
        override
        onlyRole(KEEPER_ROLE)
        whenNotPaused
        nonReentrant
        returns (uint256 rlsReceived)
    {
        if (params.stablecoinAmount == 0) revert ZeroAmount();

        FeeAggregatorStorage storage $ = _getStorage();

        if (!$.isStablecoinSupportedMap[params.stablecoin]) revert UnsupportedStablecoin();

        // Check safety limits (normalize to 6 decimals for comparison)
        uint8 decimals = IERC20Metadata(params.stablecoin).decimals();
        uint256 amountNormalized = _normalizeToUsd6(params.stablecoinAmount, decimals);

        if (amountNormalized < $.minSwapAmount) revert BelowMinimumSwap();
        if (amountNormalized > $.maxSwapAmount) revert ExceedsMaximumSwap();

        // Check balance
        if (IERC20(params.stablecoin).balanceOf(address(this)) < params.stablecoinAmount) {
            revert InsufficientBalance();
        }

        // Swap stablecoin to RLS via Algebra — RLS stays in this contract
        rlsReceived = _executeSwap(params.stablecoin, params.stablecoinAmount, params.minRlsOut);

        // Track RLS available for distribution
        $.pendingRlsForDistribution += rlsReceived;

        // Update stats
        $.totalSwapsExecuted++;
    }

    /// @inheritdoc IFeeAggregator
    function distributeEpochFees()
        external
        override
        onlyRole(KEEPER_ROLE)
        whenNotPaused
        nonReentrant
        returns (uint256 rlsDistributed)
    {
        FeeAggregatorStorage storage $ = _getStorage();

        uint256 rlsToDistribute = $.pendingRlsForDistribution;

        // Split new RLS into per-category buckets so failed legs retry only
        // their own category, not re-split across all three.
        if (rlsToDistribute > 0) {
            uint256 validatorPoolRls = (rlsToDistribute * $.config.validatorPoolBps) / MAX_BPS;
            uint256 ecosystemRls = (rlsToDistribute * $.config.ecosystemBps) / MAX_BPS;
            uint256 burnRls = rlsToDistribute - validatorPoolRls - ecosystemRls;

            $.pendingValidatorRls += validatorPoolRls;
            $.pendingEcosystemRls += ecosystemRls;
            $.pendingBurnRls += burnRls;
            $.pendingRlsForDistribution = 0;
        }

        if ($.pendingValidatorRls == 0 && $.pendingEcosystemRls == 0 && $.pendingBurnRls == 0) {
            emit EpochDistributionSkipped("No RLS to distribute");
            return 0;
        }

        // Attempt each category from its own pending bucket
        rlsDistributed = _distributeCategories();

        // Update stats
        $.totalRlsDistributed += rlsDistributed;

        emit EpochFeesDistributed(rlsToDistribute, rlsDistributed);
    }

    /**
     * @dev Execute swap via Algebra DEX
     */
    function _executeSwap(address stablecoin, uint256 amountIn, uint256 minOut)
        internal
        returns (uint256 rlsReceived)
    {
        FeeAggregatorStorage storage $ = _getStorage();

        PoolConfig memory poolConfig = $.poolConfigs[stablecoin];
        if (poolConfig.pool == address(0)) revert PoolNotConfigured();

        // Record balance before
        uint256 rlsBalanceBefore = $.rlsToken.balanceOf(address(this));

        // Approve router
        IERC20(stablecoin).forceApprove(address($.algebraRouter), amountIn);

        IAlgebraPool pool = IAlgebraPool(poolConfig.pool);

        // Snapshot price before swap (for price impact event only, not for slippage protection).
        // Algebra pool globalState returns (price, tick, lastFee, pluginConfig, communityFee, unlocked).
        (uint160 sqrtPriceX96,,,,,) = pool.globalState();

        // Use extreme limitSqrtPrice values as a pass-through. Reading spot price
        // for limitSqrtPrice provides no additional security since an attacker can
        // manipulate the spot price in the same block (sandwich). Real slippage
        // protection is via amountOutMinimum (minOut) set by the keeper off-chain.
        uint160 limitSqrtPrice;
        if (poolConfig.zeroForOne) {
            limitSqrtPrice = 4295128740; // MIN_SQRT_RATIO + 1
        } else {
            limitSqrtPrice = 1461446703485210103287273052203988822378723970341; // MAX_SQRT_RATIO - 1
        }

        // Execute swap
        ISwapRouter.ExactInputSingleParams memory swapParams = ISwapRouter.ExactInputSingleParams({
            tokenIn: stablecoin,
            tokenOut: address($.rlsToken),
            deployer: poolConfig.deployer,
            recipient: address(this),
            deadline: block.timestamp + 300, // 5 minute deadline
            amountIn: amountIn,
            amountOutMinimum: minOut,
            limitSqrtPrice: limitSqrtPrice
        });

        $.algebraRouter.exactInputSingle(swapParams);

        // Reset approval
        IERC20(stablecoin).forceApprove(address($.algebraRouter), 0);

        // Calculate received amount
        rlsReceived = $.rlsToken.balanceOf(address(this)) - rlsBalanceBefore;

        // price impact for event
        (uint160 sqrtPriceAfter,,,,,) = pool.globalState();
        uint256 priceImpactBps = sqrtPriceX96 > 0
            ? (_absDiff(sqrtPriceAfter, sqrtPriceX96) * MAX_BPS) / sqrtPriceX96
            : 0;

        emit SwapExecuted(stablecoin, amountIn, rlsReceived, priceImpactBps);
    }

    /**
     * @dev Distribute RLS from per-category pending buckets.
     * @dev Each category retries only its own failed amount — no re-splitting.
     * @dev Each recipient transfer is isolated so a single reverting recipient cannot block the others.
     */
    function _distributeCategories() internal returns (uint256 actualDistributed) {
        FeeAggregatorStorage storage $ = _getStorage();

        uint256 validatorDistributed;
        uint256 ecosystemDistributed;
        uint256 burnDistributed;

        // 1. Validator pool: send to RewardDistributor
        if ($.pendingValidatorRls > 0 && $.rewardDistributor != address(0)) {
            uint256 amount = $.pendingValidatorRls;
            if (_tryTransfer($.rlsToken, $.rewardDistributor, amount)) {
                validatorDistributed = amount;
                $.pendingValidatorRls = 0;
                try IRewardDistributor($.rewardDistributor).receiveRewards(amount) {}
                catch {
                    emit RecipientTransferFailed($.rewardDistributor, amount);
                }
            } else {
                emit RecipientTransferFailed($.rewardDistributor, amount);
            }
        }

        // 2. Ecosystem treasury
        if ($.pendingEcosystemRls > 0 && $.ecosystemTreasury != address(0)) {
            uint256 amount = $.pendingEcosystemRls;
            if (_tryTransfer($.rlsToken, $.ecosystemTreasury, amount)) {
                ecosystemDistributed = amount;
                $.pendingEcosystemRls = 0;
            } else {
                emit RecipientTransferFailed($.ecosystemTreasury, amount);
            }
        }

        // 3. Burn: bridge RLS to Ethereum via LayerZero OFT
        if ($.pendingBurnRls > 0 && $.burnAddress != address(0) && address($.oftBridge) != address(0)) {
            uint256 amount = $.pendingBurnRls;
            if (_bridgeBurn(amount)) {
                burnDistributed = amount;
                $.pendingBurnRls = 0;
            }
        }

        actualDistributed = validatorDistributed + ecosystemDistributed + burnDistributed;
        emit FeesDistributed(actualDistributed, validatorDistributed, ecosystemDistributed, burnDistributed);
    }

    /// @dev Attempts an ERC-20 transfer without reverting on failure
    /// @return success True if the transfer succeeded
    function _tryTransfer(IERC20 token, address to, uint256 amount) internal returns (bool success) {
        (bool ok, bytes memory data) = address(token).call(
            abi.encodeCall(IERC20.transfer, (to, amount))
        );
        success = ok && (data.length == 0 || abi.decode(data, (bool)));
    }

    /// @dev Bridge RLS to the burn address on Ethereum via LayerZero OFT.
    ///      Quotes the native fee, approves the OFT adapter, and sends cross-chain.
    ///      Fails gracefully if the bridge call reverts (e.g. insufficient native balance for gas).
    /// @return success True if the bridge send succeeded
    function _bridgeBurn(uint256 amount) internal returns (bool success) {
        FeeAggregatorStorage storage $ = _getStorage();

        IOFT.SendParam memory sendParam = IOFT.SendParam({
            dstEid: $.dstEid,
            to: bytes32(uint256(uint160($.burnAddress))),
            amountLD: amount,
            // Use 0 to tolerate LayerZero OFT shared-decimal dust truncation.
            // The BurnBridged event records the actual bridged amount for off-chain confirmation.
            minAmountLD: 0,
            extraOptions: "",
            composeMsg: "",
            oftCmd: ""
        });

        // Quote the native fee required for the cross-chain message
        IOFT.MessagingFee memory fee;
        try $.oftBridge.quoteSend(sendParam, false) returns (IOFT.MessagingFee memory quoted) {
            fee = quoted;
        } catch {
            emit RecipientTransferFailed($.burnAddress, amount);
            return false;
        }

        // Ensure this contract has enough native balance for the fee
        if (address(this).balance < fee.nativeFee) {
            emit RecipientTransferFailed($.burnAddress, amount);
            return false;
        }

        // Approve OFT adapter to spend RLS
        $.rlsToken.forceApprove(address($.oftBridge), amount);

        // Send cross-chain
        try $.oftBridge.send{value: fee.nativeFee}(sendParam, fee, address(this)) {
            success = true;
            emit BurnBridged(amount, $.burnAddress, $.dstEid);
        } catch {
            emit RecipientTransferFailed($.burnAddress, amount);
        }

        // Always reset approval regardless of success/failure
        $.rlsToken.forceApprove(address($.oftBridge), 0);
    }

    // ========== CONFIGURATION ==========

    /// @inheritdoc IFeeAggregator
    function getConfig() external view override returns (DistributionConfig memory) {
        return _getStorage().config;
    }

    /// @inheritdoc IFeeAggregator
    function setConfig(DistributionConfig calldata newConfig) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        uint256 total = newConfig.validatorPoolBps + newConfig.ecosystemBps + newConfig.burnBps;
        if (total != MAX_BPS) revert ConfigTotalMismatch(total);
        _getStorage().config = newConfig;
        emit ConfigUpdated(newConfig);
    }

    // ========== STABLECOIN MANAGEMENT ==========

    /// @inheritdoc IFeeAggregator
    function addStablecoin(address stablecoin) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (stablecoin == address(0)) revert ZeroAddress();

        FeeAggregatorStorage storage $ = _getStorage();

        if ($.isStablecoinSupportedMap[stablecoin]) revert StablecoinAlreadySupported();
        if ($.stablecoins.length >= MAX_STABLECOINS) revert TooManyStablecoins();

        $.stablecoins.push(stablecoin);
        $.isStablecoinSupportedMap[stablecoin] = true;

        emit StablecoinAdded(stablecoin);
    }

    /// @inheritdoc IFeeAggregator
    function removeStablecoin(address stablecoin) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (stablecoin == address(0)) revert ZeroAddress();

        FeeAggregatorStorage storage $ = _getStorage();

        if (!$.isStablecoinSupportedMap[stablecoin]) revert UnsupportedStablecoin();

        // Prevent removing stablecoin that is currently set as usdrToken
        if (stablecoin == $.usdrToken) revert CannotRemoveUsdrToken();

        // Swap and pop
        uint256 len = $.stablecoins.length;
        for (uint256 i; i < len; ++i) {
            if ($.stablecoins[i] == stablecoin) {
                $.stablecoins[i] = $.stablecoins[len - 1];
                $.stablecoins.pop();
                break;
            }
        }

        delete $.isStablecoinSupportedMap[stablecoin];
        delete $.poolConfigs[stablecoin];

        emit StablecoinRemoved(stablecoin);
    }

    /// @inheritdoc IFeeAggregator
    function isStablecoinSupported(address stablecoin) external view override returns (bool) {
        return _getStorage().isStablecoinSupportedMap[stablecoin];
    }

    /// @inheritdoc IFeeAggregator
    function getSupportedStablecoins() external view override returns (address[] memory) {
        return _getStorage().stablecoins;
    }

    /// @inheritdoc IFeeAggregator
    function setPoolConfig(address stablecoin, address pool, bool zeroForOne, address deployer)
        external
        override
        onlyRole(DEFAULT_ADMIN_ROLE)
    {
        FeeAggregatorStorage storage $ = _getStorage();

        if (!$.isStablecoinSupportedMap[stablecoin]) revert UnsupportedStablecoin();
        if (pool == address(0)) revert ZeroAddress();

        $.poolConfigs[stablecoin] = PoolConfig({pool: pool, zeroForOne: zeroForOne, deployer: deployer});

        emit PoolConfigured(stablecoin, pool, zeroForOne, deployer);
    }

    /// @inheritdoc IFeeAggregator
    function getPoolConfig(address stablecoin) external view override returns (PoolConfig memory) {
        return _getStorage().poolConfigs[stablecoin];
    }

    // ========== SAFETY LIMITS ==========

    /// @notice Set min/max swap amount bounds (in 6-decimal stablecoin units)
    function setSwapLimits(uint256 minSwap, uint256 maxSwap) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (minSwap == 0 || maxSwap <= minSwap) revert InvalidConfig();
        FeeAggregatorStorage storage $ = _getStorage();
        $.minSwapAmount = minSwap;
        $.maxSwapAmount = maxSwap;
    }

    /// @notice Get current swap amount bounds
    function getSwapLimits() external view returns (uint256 minSwap, uint256 maxSwap) {
        FeeAggregatorStorage storage $ = _getStorage();
        return ($.minSwapAmount, $.maxSwapAmount);
    }

    // ========== RECIPIENT MANAGEMENT ==========

    /// @inheritdoc IFeeAggregator
    function rlsToken() external view override returns (address) {
        return address(_getStorage().rlsToken);
    }

    /// @inheritdoc IFeeAggregator
    function algebraRouter() external view override returns (address) {
        return address(_getStorage().algebraRouter);
    }

    /// @inheritdoc IFeeAggregator
    function setAlgebraRouter(address newRouter) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newRouter == address(0)) revert ZeroAddress();
        FeeAggregatorStorage storage $ = _getStorage();
        address oldRouter = address($.algebraRouter);
        $.algebraRouter = ISwapRouter(newRouter);
        emit AlgebraRouterUpdated(oldRouter, newRouter);
    }

    /// @inheritdoc IFeeAggregator
    function rewardDistributor() external view override returns (address) {
        return _getStorage().rewardDistributor;
    }

    /// @inheritdoc IFeeAggregator
    function setRewardDistributor(address newDistributor) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newDistributor == address(0)) revert ZeroAddress();
        FeeAggregatorStorage storage $ = _getStorage();
        address oldDistributor = $.rewardDistributor;
        $.rewardDistributor = newDistributor;
        emit RewardDistributorUpdated(oldDistributor, newDistributor);
    }

    /// @inheritdoc IFeeAggregator
    function ecosystemTreasury() external view override returns (address) {
        return _getStorage().ecosystemTreasury;
    }

    /// @inheritdoc IFeeAggregator
    function setEcosystemTreasury(address newTreasury) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newTreasury == address(0)) revert ZeroAddress();
        FeeAggregatorStorage storage $ = _getStorage();
        address oldTreasury = $.ecosystemTreasury;
        $.ecosystemTreasury = newTreasury;
        emit EcosystemTreasuryUpdated(oldTreasury, newTreasury);
    }

    /// @inheritdoc IFeeAggregator
    function burnAddress() external view override returns (address) {
        return _getStorage().burnAddress;
    }

    /// @inheritdoc IFeeAggregator
    /// @dev Accepts address(0) to intentionally disable burn distribution.
    ///      Pending burn RLS will stay in pendingBurnRls until re-enabled or reset.
    function setBurnAddress(address newBurnAddress) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        FeeAggregatorStorage storage $ = _getStorage();
        address oldBurn = $.burnAddress;
        $.burnAddress = newBurnAddress;
        emit BurnAddressUpdated(oldBurn, newBurnAddress);
    }

    /// @inheritdoc IFeeAggregator
    function oftBridge() external view override returns (address) {
        return address(_getStorage().oftBridge);
    }

    /// @inheritdoc IFeeAggregator
    /// @dev Accepts address(0) to intentionally disable burn bridging.
    function setOftBridge(address newBridge) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        FeeAggregatorStorage storage $ = _getStorage();
        address oldBridge = address($.oftBridge);
        $.oftBridge = IOFT(newBridge);
        emit OftBridgeUpdated(oldBridge, newBridge);
    }

    /// @inheritdoc IFeeAggregator
    function dstEid() external view override returns (uint32) {
        return _getStorage().dstEid;
    }

    /// @inheritdoc IFeeAggregator
    function setDstEid(uint32 newDstEid) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        FeeAggregatorStorage storage $ = _getStorage();
        uint32 oldEid = $.dstEid;
        $.dstEid = newDstEid;
        emit DstEidUpdated(oldEid, newDstEid);
    }

    // ========== STATISTICS ==========

    /// @inheritdoc IFeeAggregator
    function getStats()
        external
        view
        override
        returns (uint256 totalSwapsExecuted_, uint256 totalRlsDistributed_)
    {
        FeeAggregatorStorage storage $ = _getStorage();
        return ($.totalSwapsExecuted, $.totalRlsDistributed);
    }

    /// @notice Returns the per-category pending RLS amounts awaiting distribution.
    function getPendingDistribution()
        external
        view
        returns (uint256 unsplit, uint256 validator, uint256 ecosystem, uint256 burn)
    {
        FeeAggregatorStorage storage $ = _getStorage();
        return ($.pendingRlsForDistribution, $.pendingValidatorRls, $.pendingEcosystemRls, $.pendingBurnRls);
    }

    // ========== EMERGENCY ==========

    /// @inheritdoc IFeeAggregator
    function emergencyWithdraw(address token, address to, uint256 amount)
        external
        override
        onlyRole(DEFAULT_ADMIN_ROLE)
    {
        if (to == address(0)) revert ZeroAddress();
        IERC20(token).safeTransfer(to, amount);
        emit EmergencyWithdraw(token, to, amount);
    }

    /// @inheritdoc IFeeAggregator
    function emergencyWithdrawNative(address to, uint256 amount)
        external
        override
        onlyRole(DEFAULT_ADMIN_ROLE)
    {
        if (to == address(0)) revert ZeroAddress();
        (bool success,) = to.call{value: amount}("");
        if (!success) revert NativeTransferFailed();
        emit EmergencyWithdrawNative(to, amount);
    }

    /// @notice Reset all pending distribution counters after an emergency RLS withdrawal.
    /// @dev Without this, emergencyWithdraw of RLS permanently bricks distribution
    ///      because per-category counters exceed the actual balance.
    function resetPendingDistribution() external onlyRole(DEFAULT_ADMIN_ROLE) {
        FeeAggregatorStorage storage $ = _getStorage();
        uint256 cleared = $.pendingRlsForDistribution + $.pendingValidatorRls
                        + $.pendingEcosystemRls + $.pendingBurnRls;
        $.pendingRlsForDistribution = 0;
        $.pendingValidatorRls = 0;
        $.pendingEcosystemRls = 0;
        $.pendingBurnRls = 0;
        emit PendingDistributionReset(cleared);
    }

    // ========== NATIVE TOKEN RECEPTION ==========

    /// @notice Accept native token transfers (e.g., base fees from transactions, manual top-ups)
    /// @dev Native balance is readable via IERC20(usdrToken).balanceOf(address(this)) when usdrToken = 0x0400
    receive() external payable {
        emit NativeFeeReceived(msg.value, msg.sender);
    }

    // ========== PAUSABLE ==========

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    // ========== HELPERS ==========

    function _absDiff(uint256 a, uint256 b) internal pure returns (uint256) {
        return a >= b ? a - b : b - a;
    }

    function _normalizeToUsd6(uint256 amount, uint8 decimals) internal pure returns (uint256) {
        if (decimals == 6) {
            return amount;
        } else if (decimals > 6) {
            return amount / 10 ** (decimals - 6);
        } else {
            return amount * 10 ** (6 - decimals);
        }
    }

    // ========== VERSION ==========

    function version() external pure returns (string memory) {
        return "1.0.0";
    }

    // ========== STORAGE ACCESS ==========

    function _getStorage() private pure returns (FeeAggregatorStorage storage $) {
        assembly {
            $.slot := STORAGE_LOCATION
        }
    }

    // ========== UUPS ==========

    function _authorizeUpgrade(address newImplementation) internal override onlyRole(UPGRADER_ROLE) {
        // SAFETY: Any new implementation MUST use STORAGE_LOCATION = 0x8b73...fc00
        // or include an explicit storage migration. See audit finding.
    }
}
