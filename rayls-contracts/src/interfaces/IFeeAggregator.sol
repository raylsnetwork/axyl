// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

/**
 * @title IFeeAggregator
 * @notice A Rayls Contract
 *
 * @notice Interface for unified fee collection, swapping, and distribution
 * @dev Collects ERC-20 stablecoin fees (USDr, USDT, USDC, etc.)
 * @dev Swaps fees to RLS via Algebra DEX, distributes to 3 recipients (validators, ecosystem, burn)
 * @dev UUPS upgradeable with AccessControl
 */
interface IFeeAggregator {
    // ========== STRUCTS ==========

    /// @notice Distribution configuration in basis points (1 bps = 0.01%)
    /// @dev Total of all bps must equal 10,000 (100%)
    struct DistributionConfig {
        /// @notice Percentage to validators/delegators (RLS via RewardDistributor)
        uint256 validatorPoolBps;
        /// @notice Percentage to ecosystem development (grants, bug bounties)
        uint256 ecosystemBps;
        /// @notice Percentage to burn (RLS sent to burn address)
        uint256 burnBps;
    }

    /// @notice Pending config proposal with execution timestamp
    struct ConfigProposal {
        DistributionConfig config;
        uint256 executeAfter;
        bool exists;
    }

    /// @notice Pool configuration for a stablecoin
    struct PoolConfig {
        address pool; // Algebra pool address
        bool zeroForOne; // true if stablecoin is token0
        address deployer; // Algebra V1.9 pool deployer; routed as (tokenIn, tokenOut, deployer) in SwapRouter
    }

    /// @notice Parameters for swapToRls (ERC-20 stablecoins)
    struct SwapParams {
        address stablecoin;
        uint256 stablecoinAmount;
        uint256 minRlsOut;
    }

    // ========== ERRORS ==========

    error ZeroAddress();
    error ZeroAmount();
    error InvalidConfig();
    error ConfigTotalMismatch(uint256 total);
    error UnsupportedStablecoin();
    error StablecoinAlreadySupported();
    error TooManyStablecoins();
    error PoolNotConfigured();
    error BelowMinimumSwap();
    error ExceedsMaximumSwap();
    error SlippageExceeded(uint256 received, uint256 minimum);
    error InsufficientBalance();
    error InvalidSwapLimits();
    error SwapFailed();
    error NativeTransferFailed();
    error Reentrancy();
    error OnlySelf();
    error CannotRemoveUsdrToken();

    // ========== EVENTS ==========

    /// @notice Emitted when ERC-20 stablecoin fees are received
    event FeeReceived(address indexed token, uint256 amount, address indexed from);

    /// @notice Emitted when the USDr token address is updated
    event UsdrTokenUpdated(address indexed oldUsdr, address indexed newUsdr);

    /// @notice Emitted when a swap is executed
    event SwapExecuted(
        address indexed stablecoin,
        uint256 amountIn,
        uint256 rlsOut,
        uint256 priceImpactBps
    );

    /// @notice Emitted when fees are distributed (all amounts in RLS)
    event FeesDistributed(
        uint256 totalRlsDistributed,
        uint256 validatorPoolRls,
        uint256 ecosystemRls,
        uint256 burnRls
    );

    /// @notice Emitted when distribution config is updated
    event ConfigUpdated(DistributionConfig config);

    /// @notice Emitted when a stablecoin is added
    event StablecoinAdded(address indexed stablecoin);

    /// @notice Emitted when a stablecoin is removed
    event StablecoinRemoved(address indexed stablecoin);

    /// @notice Emitted when a pool is configured for a stablecoin
    event PoolConfigured(address indexed stablecoin, address indexed pool, bool zeroForOne, address deployer);


    /// @notice Emitted when the reward distributor is updated
    event RewardDistributorUpdated(address indexed oldDistributor, address indexed newDistributor);

    /// @notice Emitted when the ecosystem treasury is updated
    event EcosystemTreasuryUpdated(address indexed oldTreasury, address indexed newTreasury);

    /// @notice Emitted when the burn address is updated
    event BurnAddressUpdated(address indexed oldBurn, address indexed newBurn);

    /// @notice Emitted when the Algebra router is updated
    event AlgebraRouterUpdated(address indexed oldRouter, address indexed newRouter);

    /// @notice Emitted when epoch distribution is triggered with no fees to distribute
    event EpochDistributionSkipped(string reason);

    /// @notice Emitted when epoch distribution swap fails (fees kept for next epoch)
    event EpochDistributionSwapFailed(uint256 usdrAmount, string reason);

    /// @notice Emitted when epoch distribution completes successfully
    event EpochFeesDistributed(uint256 usdrAmount, uint256 rlsDistributed);

    /// @notice Emitted when emergency ERC-20 withdrawal is executed
    event EmergencyWithdraw(address indexed token, address indexed to, uint256 amount);

    /// @notice Emitted when emergency native withdrawal is executed
    event EmergencyWithdrawNative(address indexed to, uint256 amount);

    /// @notice Emitted when pending distribution counters are reset after emergency withdrawal
    event PendingDistributionReset(uint256 clearedAmount);

    /// @notice Emitted when native tokens are received (via receive() function)
    event NativeFeeReceived(uint256 amount, address indexed sender);

    /// @notice Emitted when a distribution recipient transfer fails (does not block other recipients)
    event RecipientTransferFailed(address indexed recipient, uint256 amount);

    /// @notice Emitted when RLS is bridged to Ethereum for burning via LayerZero
    event BurnBridged(uint256 amount, address indexed ethDestination, uint32 dstEid);

    /// @notice Emitted when the OFT bridge contract is updated
    event OftBridgeUpdated(address indexed oldBridge, address indexed newBridge);

    /// @notice Emitted when the LayerZero destination endpoint ID is updated
    event DstEidUpdated(uint32 oldEid, uint32 newEid);

    // ========== FEE RECEPTION ==========

    /// @notice Receive ERC-20 stablecoin fees (direct transfer flow)
    /// @dev Caller must have approved this contract to transfer the stablecoin
    /// @param stablecoin The stablecoin address
    /// @param amount The amount to receive
    function receiveFee(address stablecoin, uint256 amount) external;

    /// @notice Get pending balance of a stablecoin
    /// @param stablecoin The stablecoin address
    /// @return The pending balance
    function pendingBalance(address stablecoin) external view returns (uint256);

    // ========== TOKEN CONFIGURATION ==========

    /// @notice Get the USDr token address (primary fee token for epoch distribution)
    function usdrToken() external view returns (address);

    /// @notice Set the USDr token address
    /// @param newUsdrToken The new USDr token address
    function setUsdrToken(address newUsdrToken) external;

    // ========== PROCESSING ==========

    /// @notice Swap accumulated stablecoin fees to RLS
    /// @dev Only callable by KEEPER_ROLE. RLS stays in this contract for later distribution.
    /// @param params Swap parameters (stablecoin, amount, minRlsOut)
    /// @return rlsReceived Amount of RLS received from the swap
    function swapToRls(SwapParams calldata params)
        external
        returns (uint256 rlsReceived);

    /// @notice Distribute all RLS held by this contract to the 3 categories
    /// @dev Only callable by KEEPER_ROLE
    /// @dev Reads the contract's RLS balance and distributes to validators, ecosystem, and burn
    /// @dev Returns 0 with EpochDistributionSkipped event if no RLS is available
    /// @return rlsDistributed Total RLS distributed (0 if skipped)
    function distributeEpochFees() external returns (uint256 rlsDistributed);

    // ========== CONFIGURATION ==========

    /// @notice Get the current distribution configuration
    function getConfig() external view returns (DistributionConfig memory);

    /// @notice Set the distribution configuration directly
    /// @dev Only callable by DEFAULT_ADMIN_ROLE. BPS must sum to 10,000.
    /// @param newConfig The new configuration
    function setConfig(DistributionConfig calldata newConfig) external;

    // ========== STABLECOIN MANAGEMENT ==========

    /// @notice Add a supported stablecoin
    /// @param stablecoin The stablecoin address
    function addStablecoin(address stablecoin) external;

    /// @notice Remove a supported stablecoin
    /// @param stablecoin The stablecoin address
    function removeStablecoin(address stablecoin) external;

    /// @notice Check if a stablecoin is supported
    /// @param stablecoin The stablecoin address
    function isStablecoinSupported(address stablecoin) external view returns (bool);

    /// @notice Get all supported stablecoins
    function getSupportedStablecoins() external view returns (address[] memory);

    /// @notice Configure the Algebra pool for a stablecoin
    /// @param stablecoin The stablecoin address
    /// @param pool The Algebra pool address
    /// @param zeroForOne True if stablecoin is token0 in the pool
    /// @param deployer The Algebra V1.9 pool deployer (from `IAlgebraPool(pool).deployer()`)
    function setPoolConfig(address stablecoin, address pool, bool zeroForOne, address deployer) external;

    /// @notice Get the pool configuration for a stablecoin
    /// @param stablecoin The stablecoin address
    function getPoolConfig(address stablecoin) external view returns (PoolConfig memory);

    // ========== RECIPIENT MANAGEMENT ==========

    /// @notice Get the RLS token address
    function rlsToken() external view returns (address);

    /// @notice Get the Algebra router address
    function algebraRouter() external view returns (address);

    /// @notice Set the Algebra router address
    function setAlgebraRouter(address newRouter) external;

    /// @notice Get the reward distributor address
    function rewardDistributor() external view returns (address);

    /// @notice Set the reward distributor address
    function setRewardDistributor(address newDistributor) external;

    /// @notice Get the ecosystem treasury address
    function ecosystemTreasury() external view returns (address);

    /// @notice Set the ecosystem treasury address
    function setEcosystemTreasury(address newTreasury) external;


    /// @notice Get the burn address
    function burnAddress() external view returns (address);

    /// @notice Set the burn destination address on Ethereum
    function setBurnAddress(address newBurnAddress) external;

    /// @notice Get the OFT bridge contract address
    function oftBridge() external view returns (address);

    /// @notice Set the OFT bridge contract address (LayerZero OFT adapter)
    function setOftBridge(address newBridge) external;

    /// @notice Get the LayerZero destination endpoint ID
    function dstEid() external view returns (uint32);

    /// @notice Set the LayerZero destination endpoint ID (e.g. Ethereum)
    function setDstEid(uint32 newDstEid) external;

    // ========== STATISTICS ==========

    /// @notice Get aggregated statistics
    /// @return totalSwapsExecuted Number of swaps executed
    /// @return totalRlsDistributed Total RLS distributed
    function getStats()
        external
        view
        returns (uint256 totalSwapsExecuted, uint256 totalRlsDistributed);

    // ========== EMERGENCY ==========

    /// @notice Emergency withdraw stuck ERC-20 tokens
    /// @param token The token to withdraw
    /// @param to The recipient address
    /// @param amount The amount to withdraw
    function emergencyWithdraw(address token, address to, uint256 amount) external;

    /// @notice Emergency withdraw stuck native tokens
    /// @param to The recipient address
    /// @param amount The amount to withdraw
    function emergencyWithdrawNative(address to, uint256 amount) external;
}
