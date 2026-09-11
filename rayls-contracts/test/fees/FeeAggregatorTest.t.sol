// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {FeeAggregator} from "src/fees/FeeAggregator.sol";
import {IFeeAggregator} from "src/interfaces/IFeeAggregator.sol";
import {SystemCallable} from "src/consensus/SystemCallable.sol";
import {ISwapRouter} from "src/interfaces/ISwapRouter.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {MockOFTBridge} from "src/mocks/MockOFTBridge.sol";
import {IOFT} from "src/interfaces/IOFT.sol";
import {IAccessControl} from "@openzeppelin/contracts/access/IAccessControl.sol";

// ============================================================================
//                              Mock Contracts
// ============================================================================

/// @notice Mock ERC20 token for testing
contract MockERC20 is ERC20 {
    uint8 private _decimals;

    constructor(string memory name, string memory symbol, uint8 decimals_) ERC20(name, symbol) {
        _decimals = decimals_;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function burn(uint256 amount) external {
        _burn(msg.sender, amount);
    }

    // Test-only: force `transfer` (the path _tryTransfer uses) to a recipient to fail.
    // Only `transfer`, not `transferFrom`.
    mapping(address => bool) public transferBlocked;

    function setTransferBlocked(address to, bool blocked) external {
        transferBlocked[to] = blocked;
    }

    function transfer(address to, uint256 value) public override returns (bool) {
        require(!transferBlocked[to], "transfer blocked");
        return super.transfer(to, value);
    }

    function decimals() public view override returns (uint8) {
        return _decimals;
    }
}

/// @notice Mock Algebra SwapRouter for testing
contract MockAlgebraRouter {
    MockERC20 public rlsToken;
    uint256 public swapRate; // How many RLS per 1 stablecoin (scaled by 1e18)

    constructor(address _rlsToken) {
        rlsToken = MockERC20(_rlsToken);
        swapRate = 1e18; // 1:1 default
    }

    function setSwapRate(uint256 rate) external {
        swapRate = rate;
    }

    function exactInputSingle(ISwapRouter.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut)
    {
        // Transfer stablecoin from sender
        IERC20(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);

        // Calculate output based on swap rate
        amountOut = (params.amountIn * swapRate) / 1e18;

        // Ensure minimum is met
        require(amountOut >= params.amountOutMinimum, "Too little received");

        // Mint RLS to recipient
        rlsToken.mint(params.recipient, amountOut);
    }
}

/// @notice Mock RewardDistributor for testing
contract MockRewardDistributor {
    IERC20 public rlsToken;
    uint256 public lastReceivedAmount;

    constructor(address _rlsToken) {
        rlsToken = IERC20(_rlsToken);
    }

    function receiveRewards(uint256 amount) external {
        lastReceivedAmount = amount;
    }
}

/// @notice Mock RewardDistributor that reverts on receiveRewards
contract RevertingRewardDistributor {
    function receiveRewards(uint256) external pure {
        revert("DOS");
    }
}

/// @notice Mock Algebra Pool for testing
contract MockAlgebraPool {
    uint160 public sqrtPriceX96 = 79228162514264337593543950336; // ~1:1 price

    function globalState()
        external
        view
        returns (uint160 price, int24 tick, uint16 fee, uint8 timepointIndex, uint8 communityFeeToken0, uint8 communityFeeToken1)
    {
        return (sqrtPriceX96, 0, 0, 0, 0, 0);
    }

    function setSqrtPriceX96(uint160 newPrice) external {
        sqrtPriceX96 = newPrice;
    }
}

/// @notice Mock OFT adapter that reverts on quoteSend (revertOnQuote=true) or on send
///         (false), to exercise FeeAggregator._bridgeBurn's failure branches.
contract RevertingOFTBridge is IOFT {
    bool public revertOnQuote;

    constructor(bool revertOnQuote_) {
        revertOnQuote = revertOnQuote_;
    }

    function quoteSend(SendParam calldata, bool) external view override returns (MessagingFee memory) {
        require(!revertOnQuote, "quote failed");
        return MessagingFee({nativeFee: 0, lzTokenFee: 0});
    }

    function send(SendParam calldata, MessagingFee calldata, address)
        external
        payable
        override
        returns (MessagingReceipt memory, OFTReceipt memory)
    {
        revert("send failed"); // always reverts — drives the send-failure branch (revertOnQuote=false)
    }
}


// ============================================================================
//                              Test Contract
// ============================================================================

contract FeeAggregatorTest is Test {
    FeeAggregator public implementation;
    FeeAggregator public aggregator;

    MockERC20 public rlsToken;
    MockERC20 public usdt;
    MockERC20 public usdc;
    MockERC20 public usdrToken;
    MockAlgebraRouter public router;
    MockRewardDistributor public rewardDistributor;
    MockAlgebraPool public usdtPool;
    MockAlgebraPool public usdcPool;
    MockAlgebraPool public usdrPool;

    address public admin = address(0xAD01);
    address public keeper = address(0xBE02);
    address public pauser = address(0xAA03);
    address public upgrader = address(0xBB04);
    address constant SYSTEM_ADDRESS = address(0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE);

    address public ecosystemTreasury = address(0xEC0);
    address public burnAddress = address(0xdead);

    uint32 public constant ETH_DST_EID = 30101; // LayerZero Ethereum endpoint ID

    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    function setUp() public {
        // Deploy tokens
        rlsToken = new MockERC20("RLS Token", "RLS", 18);
        usdt = new MockERC20("USDT", "USDT", 6);
        usdc = new MockERC20("USDC", "USDC", 6);
        usdrToken = new MockERC20("USDr", "USDr", 18);

        // Deploy mocks
        router = new MockAlgebraRouter(address(rlsToken));
        rewardDistributor = new MockRewardDistributor(address(rlsToken));
        usdtPool = new MockAlgebraPool();
        usdcPool = new MockAlgebraPool();
        usdrPool = new MockAlgebraPool();

        // Deploy implementation
        implementation = new FeeAggregator();

        // Default distribution config (50% validator, 30% ecosystem, 20% burn)
        IFeeAggregator.DistributionConfig memory defaultConfig = IFeeAggregator.DistributionConfig({
            validatorPoolBps: 5000,
            ecosystemBps: 3000,
            burnBps: 2000
        });

        // Deploy proxy
        bytes memory initData = abi.encodeWithSelector(
            FeeAggregator.initialize.selector,
            address(rlsToken),
            address(router),
            address(rewardDistributor),
            ecosystemTreasury,
            burnAddress,
            usdrToken, // usdrToken — set later via setUsdrToken in tests that need it
            defaultConfig,
            admin
        );

        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        aggregator = FeeAggregator(payable(address(proxy)));

        // Grant roles
        vm.startPrank(admin);
        aggregator.grantRole(KEEPER_ROLE, keeper);
        aggregator.grantRole(PAUSER_ROLE, pauser);
        aggregator.grantRole(UPGRADER_ROLE, upgrader);

        // Add stablecoins
        aggregator.addStablecoin(address(usdt));
        aggregator.addStablecoin(address(usdc));
        aggregator.addStablecoin(address(usdrToken));

        // Configure pools with mock pool contracts
        aggregator.setPoolConfig(address(usdt), address(usdtPool), true, address(0));
        aggregator.setPoolConfig(address(usdc), address(usdcPool), true, address(0));
        aggregator.setPoolConfig(address(usdrToken), address(usdrPool), true, address(0));

        // Configure USDr token for epoch distribution
        aggregator.setUsdrToken(address(usdrToken));
        vm.stopPrank();

        // Mint stablecoins for testing
        usdt.mint(address(this), 1_000_000e6);
        usdc.mint(address(this), 1_000_000e6);
        usdrToken.mint(address(this), 1_000_000e18);
        usdrToken.mint(SYSTEM_ADDRESS, 1_000_000e18);
    }

    // =========================================================================
    //                          Initialization Tests
    // =========================================================================

    function test_initialize() public view {
        assertEq(aggregator.rlsToken(), address(rlsToken));
        assertEq(aggregator.algebraRouter(), address(router));
        assertEq(aggregator.rewardDistributor(), address(rewardDistributor));
        assertEq(aggregator.ecosystemTreasury(), ecosystemTreasury);
        assertEq(aggregator.burnAddress(), burnAddress);
    }

    function test_initialize_defaultConfig() public view {
        IFeeAggregator.DistributionConfig memory config = aggregator.getConfig();
        assertEq(config.validatorPoolBps, 5000);
        assertEq(config.ecosystemBps, 3000);
        assertEq(config.burnBps, 2000);
    }

    function test_initialize_defaultSwapLimits() public view {
        (uint256 minSwap, uint256 maxSwap) = aggregator.getSwapLimits();
        assertEq(minSwap, 1_000e6);
        assertEq(maxSwap, 100_000e6);
    }

    function testRevert_initialize_twice() public {
        IFeeAggregator.DistributionConfig memory config = IFeeAggregator.DistributionConfig({
            validatorPoolBps: 5000,
            ecosystemBps: 3000,
            burnBps: 2000
        });

        vm.expectRevert();
        aggregator.initialize(
            address(rlsToken),
            address(router),
            address(rewardDistributor),
            ecosystemTreasury,
            burnAddress,
            address(0),
            config,
            admin
        );
    }

    // =========================================================================
    //                          Fee Reception Tests
    // =========================================================================

    function test_receiveFee() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        assertEq(aggregator.pendingBalance(address(usdt)), amount);
        assertEq(usdt.balanceOf(address(aggregator)), amount);
    }

    function test_receiveFee_multiple() public {
        uint256 amount1 = 10_000e6;
        uint256 amount2 = 20_000e6;

        usdt.approve(address(aggregator), amount1 + amount2);
        aggregator.receiveFee(address(usdt), amount1);
        aggregator.receiveFee(address(usdt), amount2);

        assertEq(aggregator.pendingBalance(address(usdt)), amount1 + amount2);
    }

    function testRevert_receiveFee_unsupportedStablecoin() public {
        MockERC20 randomToken = new MockERC20("Random", "RND", 18);
        randomToken.mint(address(this), 1000e18);
        randomToken.approve(address(aggregator), 1000e18);

        vm.expectRevert(IFeeAggregator.UnsupportedStablecoin.selector);
        aggregator.receiveFee(address(randomToken), 1000e18);
    }

    function testRevert_receiveFee_zeroAmount() public {
        vm.expectRevert(IFeeAggregator.ZeroAmount.selector);
        aggregator.receiveFee(address(usdt), 0);
    }

    // =========================================================================
    //                          Process & Distribute Tests
    // =========================================================================

    function test_swapToRls() public {
        uint256 amount = 10_000e6;

        // Deposit fees
        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        // Swap (keeper role) — RLS stays in aggregator
        vm.prank(keeper);
        uint256 rlsReceived = aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: 0})
        );

        // With 1:1 swap rate, should receive same amount
        assertEq(rlsReceived, amount);

        // Stablecoin balance cleared
        assertEq(aggregator.pendingBalance(address(usdt)), 0);

        // RLS stays in the aggregator (not distributed yet)
        assertEq(rlsToken.balanceOf(address(aggregator)), amount);
    }

    function testRevert_swapToRls_belowMinRlsOut_adversePrice() public {
        uint256 amount = 10_000e6;
        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        // Manipulated/sandwiched pool price: only 0.5x out, below the keeper's floor
        router.setSwapRate(0.5e18);
        uint256 minRlsOut = amount;

        vm.prank(keeper);
        vm.expectRevert(bytes("Too little received")); // router rejects on slippage floor
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: minRlsOut})
        );

        // Fees untouched, no RLS acquired at the bad rate
        assertEq(aggregator.pendingBalance(address(usdt)), amount);
        assertEq(rlsToken.balanceOf(address(aggregator)), 0);
    }

    function test_swapToRls_respectsMinRlsOut_whenSatisfiable() public {
        uint256 amount = 10_000e6;
        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        // Mild adverse price (0.9x); keeper floor set to match → still succeeds
        router.setSwapRate(0.9e18);
        uint256 minRlsOut = 9_000e6;

        vm.prank(keeper);
        uint256 rlsReceived = aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: minRlsOut})
        );

        assertEq(rlsReceived, 9_000e6, "0.9x of 10_000e6 = exactly 9_000e6"); // deterministic, not just >= floor
        assertEq(aggregator.pendingBalance(address(usdt)), 0);
    }

    function testRevert_swapToRls_notKeeper() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        vm.expectRevert();
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: 0})
        );
    }

    function testRevert_swapToRls_belowMinSwap() public {
        uint256 smallAmount = 100e6; // Below $1000 minimum

        usdt.approve(address(aggregator), smallAmount);
        aggregator.receiveFee(address(usdt), smallAmount);

        vm.prank(keeper);
        vm.expectRevert(IFeeAggregator.BelowMinimumSwap.selector);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: smallAmount, minRlsOut: 0})
        );
    }

    function testRevert_swapToRls_aboveMaxSwap() public {
        uint256 largeAmount = 200_000e6; // Above $100k maximum

        usdt.mint(address(this), largeAmount);
        usdt.approve(address(aggregator), largeAmount);
        aggregator.receiveFee(address(usdt), largeAmount);

        vm.prank(keeper);
        vm.expectRevert(IFeeAggregator.ExceedsMaximumSwap.selector);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: largeAmount, minRlsOut: 0})
        );
    }

    function testRevert_swapToRls_insufficientBalance() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        vm.prank(keeper);
        vm.expectRevert(IFeeAggregator.InsufficientBalance.selector);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({
                stablecoin: address(usdt),
                stablecoinAmount: amount + 1, // Request more than available
                minRlsOut: 0
            })
        );
    }

    // =========================================================================
    //                          Stablecoin Management Tests
    // =========================================================================

    function test_addStablecoin() public {
        MockERC20 newStable = new MockERC20("DAI", "DAI", 18);

        vm.prank(admin);
        aggregator.addStablecoin(address(newStable));

        assertTrue(aggregator.isStablecoinSupported(address(newStable)));
    }

    function test_removeStablecoin() public {
        vm.prank(admin);
        aggregator.removeStablecoin(address(usdt));

        assertFalse(aggregator.isStablecoinSupported(address(usdt)));
    }

    function test_getSupportedStablecoins() public view {
        address[] memory stables = aggregator.getSupportedStablecoins();
        assertEq(stables.length, 3); // usdt, usdc, usdrToken
        assertEq(stables[0], address(usdt));
        assertEq(stables[1], address(usdc));
        assertEq(stables[2], address(usdrToken));
    }

    function testRevert_addStablecoin_alreadySupported() public {
        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.StablecoinAlreadySupported.selector);
        aggregator.addStablecoin(address(usdt));
    }

    function testRevert_addStablecoin_notAdmin() public {
        MockERC20 newStable = new MockERC20("DAI", "DAI", 18);

        vm.prank(keeper);
        vm.expectRevert();
        aggregator.addStablecoin(address(newStable));
    }

    // =========================================================================
    //                          Configuration Tests
    // =========================================================================

    function test_updateConfig_viaTimelock() public {
        IFeeAggregator.DistributionConfig memory newConfig = IFeeAggregator.DistributionConfig({
            validatorPoolBps: 4000,
            ecosystemBps: 3500,
            burnBps: 2500
        });

        vm.prank(admin);
        aggregator.setConfig(newConfig);

        IFeeAggregator.DistributionConfig memory config = aggregator.getConfig();
        assertEq(config.validatorPoolBps, 4000);
        assertEq(config.ecosystemBps, 3500);
        assertEq(config.burnBps, 2500);
    }

    function testRevert_setConfig_invalidTotal() public {
        IFeeAggregator.DistributionConfig memory badConfig = IFeeAggregator.DistributionConfig({
            validatorPoolBps: 5000,
            ecosystemBps: 3000,
            burnBps: 3000 // Total = 11000, invalid
        });

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(IFeeAggregator.ConfigTotalMismatch.selector, 11000));
        aggregator.setConfig(badConfig);
    }

    // =========================================================================
    //                          Swap Limits Tests
    // =========================================================================

    function test_setSwapLimits() public {
        vm.prank(admin);
        aggregator.setSwapLimits(500e6, 50_000e6);

        (uint256 minSwap, uint256 maxSwap) = aggregator.getSwapLimits();
        assertEq(minSwap, 500e6);
        assertEq(maxSwap, 50_000e6);
    }

    // =========================================================================
    //                          Recipient Management Tests
    // =========================================================================

    function test_setRewardDistributor() public {
        address newDistributor = address(0x123);

        vm.prank(admin);
        aggregator.setRewardDistributor(newDistributor);

        assertEq(aggregator.rewardDistributor(), newDistributor);
    }

    function test_setEcosystemTreasury() public {
        address newTreasury = address(0x456);

        vm.prank(admin);
        aggregator.setEcosystemTreasury(newTreasury);

        assertEq(aggregator.ecosystemTreasury(), newTreasury);
    }

    function test_setBurnAddress() public {
        address newBurn = address(0x789);

        vm.prank(admin);
        aggregator.setBurnAddress(newBurn);

        assertEq(aggregator.burnAddress(), newBurn);
    }

    function testRevert_setRecipient_zeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.ZeroAddress.selector);
        aggregator.setRewardDistributor(address(0));
    }

    // =========================================================================
    //                          Pause Tests
    // =========================================================================

    function test_pause() public {
        vm.prank(pauser);
        aggregator.pause();

        assertTrue(aggregator.paused());
    }

    function test_unpause() public {
        vm.prank(pauser);
        aggregator.pause();

        vm.prank(pauser);
        aggregator.unpause();

        assertFalse(aggregator.paused());
    }

    function testRevert_swapToRls_whenPaused() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        vm.prank(pauser);
        aggregator.pause();

        vm.prank(keeper);
        vm.expectRevert();
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: 0})
        );
    }

    // =========================================================================
    //                          Emergency Tests
    // =========================================================================

    function test_emergencyWithdraw() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        address recipient = address(0xBAD);

        vm.prank(admin);
        aggregator.emergencyWithdraw(address(usdt), recipient, amount);

        assertEq(usdt.balanceOf(recipient), amount);
    }

    function testRevert_emergencyWithdraw_notAdmin() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        vm.prank(keeper);
        vm.expectRevert();
        aggregator.emergencyWithdraw(address(usdt), address(0xBAD), amount);
    }

    function test_emergencyWithdrawNative() public {
        uint256 amount = 10_000e18;

        // Send native tokens to contract (via vm.deal)
        vm.deal(address(aggregator), amount);

        address recipient = address(0xBAD);
        uint256 recipientBalanceBefore = recipient.balance;

        vm.prank(admin);
        aggregator.emergencyWithdrawNative(recipient, amount);

        assertEq(recipient.balance, recipientBalanceBefore + amount);
    }

    function testRevert_emergencyWithdrawNative_notAdmin() public {
        uint256 amount = 10_000e18;

        vm.deal(address(aggregator), amount);

        vm.prank(keeper);
        vm.expectRevert();
        aggregator.emergencyWithdrawNative(address(0xBAD), amount);
    }

    function testRevert_emergencyWithdrawNative_zeroAddress() public {
        uint256 amount = 10_000e18;

        vm.deal(address(aggregator), amount);

        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.ZeroAddress.selector);
        aggregator.emergencyWithdrawNative(address(0), amount);
    }

    // =========================================================================
    //                          Statistics Tests
    // =========================================================================

    function test_getStats() public {
        uint256 amount = 10_000e6;

        // Swap only — no distribution yet
        _swapToRls(amount);

        (uint256 totalSwaps, uint256 totalDistributed) = aggregator.getStats();
        assertEq(totalSwaps, 1);
        assertEq(totalDistributed, 0); // not distributed yet

        // Distribute
        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();

        (totalSwaps, totalDistributed) = aggregator.getStats();
        assertEq(totalSwaps, 1);
        assertEq(totalDistributed, distributed);
    }

    // =========================================================================
    //                          Protocol Configuration Tests
    // =========================================================================

    function test_setUsdrToken() public {
        // Create a new stablecoin, add it, and configure pool
        MockERC20 newUsdr = new MockERC20("New USDr", "nUSDr", 18);
        MockAlgebraPool newPool = new MockAlgebraPool();

        vm.startPrank(admin);
        aggregator.addStablecoin(address(newUsdr));
        aggregator.setPoolConfig(address(newUsdr), address(newPool), true, address(0));
        aggregator.setUsdrToken(address(newUsdr));
        vm.stopPrank();

        assertEq(aggregator.usdrToken(), address(newUsdr));
    }

    function testRevert_setUsdrToken_zeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.ZeroAddress.selector);
        aggregator.setUsdrToken(address(0));
    }

    function testRevert_setUsdrToken_unsupportedStablecoin() public {
        address randomToken = address(0x8888);

        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.UnsupportedStablecoin.selector);
        aggregator.setUsdrToken(randomToken);
    }

    function testRevert_setUsdrToken_poolNotConfigured() public {
        // Add a stablecoin but don't configure its pool
        MockERC20 newStable = new MockERC20("New Stable", "NSTB", 6);

        vm.startPrank(admin);
        aggregator.addStablecoin(address(newStable));

        vm.expectRevert(IFeeAggregator.PoolNotConfigured.selector);
        aggregator.setUsdrToken(address(newStable));
        vm.stopPrank();
    }

    function testRevert_removeStablecoin_cannotRemoveUsdrToken() public {
        // usdrToken is already set to usdrToken in setUp

        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.CannotRemoveUsdrToken.selector);
        aggregator.removeStablecoin(address(usdrToken));
    }

    function test_removeStablecoin_afterChangingUsdrToken() public {
        // First change usdrToken to a different token
        MockERC20 newUsdr = new MockERC20("New USDr", "nUSDr", 18);
        MockAlgebraPool newPool = new MockAlgebraPool();

        vm.startPrank(admin);
        aggregator.addStablecoin(address(newUsdr));
        aggregator.setPoolConfig(address(newUsdr), address(newPool), true, address(0));
        aggregator.setUsdrToken(address(newUsdr));

        // Now we can remove the old usdrToken
        aggregator.removeStablecoin(address(usdrToken));
        vm.stopPrank();

        assertFalse(aggregator.isStablecoinSupported(address(usdrToken)));
    }

    function testRevert_setEcosystemTreasury_zeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(IFeeAggregator.ZeroAddress.selector);
        aggregator.setEcosystemTreasury(address(0));
    }

    function test_emergencyWithdraw_emitsEvent() public {
        uint256 amount = 10_000e6;

        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        address recipient = address(0xBAD);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.EmergencyWithdraw(address(usdt), recipient, amount);

        vm.prank(admin);
        aggregator.emergencyWithdraw(address(usdt), recipient, amount);
    }

    function test_emergencyWithdrawNative_emitsEvent() public {
        uint256 amount = 10_000e18;

        vm.deal(address(aggregator), amount);

        address recipient = address(0xBAD);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.EmergencyWithdrawNative(recipient, amount);

        vm.prank(admin);
        aggregator.emergencyWithdrawNative(recipient, amount);
    }

    // =========================================================================
    //                          Multiple Stablecoin Integration Test
    // =========================================================================

    function test_multipleStablecoinsProcessed() public {
        // Fee 1: USDT
        uint256 usdtAmount = 10_000e6;
        usdt.approve(address(aggregator), usdtAmount);
        aggregator.receiveFee(address(usdt), usdtAmount);

        // Fee 2: USDr
        uint256 usdrAmount = 10_000e18;
        usdrToken.approve(address(aggregator), usdrAmount);
        aggregator.receiveFee(address(usdrToken), usdrAmount);

        // Process USDT fees
        vm.prank(keeper);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: usdtAmount, minRlsOut: 0})
        );

        // Process USDr fees
        vm.prank(keeper);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdrToken), stablecoinAmount: usdrAmount, minRlsOut: 0})
        );

        // Verify both swaps were processed (distribution happens separately)
        (uint256 totalSwaps, uint256 totalDistributed) = aggregator.getStats();
        assertEq(totalSwaps, 2);
        assertEq(totalDistributed, 0); // not distributed yet, only swapped
    }

    // =========================================================================
    //                     Epoch Distribution Tests
    // =========================================================================

    // Helper: swap USDT to RLS via swapToRls (increments pendingRlsForDistribution)
    function _swapToRls(uint256 usdtAmount) internal returns (uint256 rlsReceived) {
        usdt.mint(address(this), usdtAmount);
        usdt.approve(address(aggregator), usdtAmount);
        aggregator.receiveFee(address(usdt), usdtAmount);
        vm.prank(keeper);
        rlsReceived = aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: usdtAmount, minRlsOut: 0})
        );
    }

    // Expected per-category slices for `rls_`, read from the live config (burn = remainder,
    // matching FeeAggregator._distributeCategories) so tests don't hardcode the setUp() bps.
    function _split(uint256 rls_) internal view returns (uint256 validator, uint256 ecosystem, uint256 burn) {
        IFeeAggregator.DistributionConfig memory cfg = aggregator.getConfig();
        validator = (rls_ * cfg.validatorPoolBps) / 10000;
        ecosystem = (rls_ * cfg.ecosystemBps) / 10000;
        burn = rls_ - validator - ecosystem;
    }

    function test_distributeEpochFees_success() public {
        uint256 amount = 10_000e6;

        // Swap stablecoins to RLS (sets pendingRlsForDistribution)
        uint256 rlsReceived = _swapToRls(amount);

        // Distribute RLS (keeper calls)
        vm.prank(keeper);
        uint256 rlsDistributed = aggregator.distributeEpochFees();

        // actualDistributed = validator (50%) + ecosystem (30%) = 80%
        // burn (20%) skipped — no bridge configured
        uint256 expectedDistributed = (rlsReceived * 8000) / 10000;
        assertEq(rlsDistributed, expectedDistributed);

        // Verify stats updated
        (, uint256 totalDistributed) = aggregator.getStats();
        assertEq(totalDistributed, expectedDistributed);
    }

    function test_distributeEpochFees_zeroBalance_skipsWithEvent() public {
        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.EpochDistributionSkipped("No RLS to distribute");

        vm.prank(keeper);
        uint256 rlsDistributed = aggregator.distributeEpochFees();

        assertEq(rlsDistributed, 0);
    }

    function test_distributeEpochFees_onlyKeeper() public {
        _swapToRls(10_000e6);

        // Non-keeper address should fail
        vm.expectRevert();
        aggregator.distributeEpochFees();

        // System address should fail (no KEEPER_ROLE)
        vm.prank(SYSTEM_ADDRESS);
        vm.expectRevert();
        aggregator.distributeEpochFees();

        // Keeper should succeed
        vm.prank(keeper);
        aggregator.distributeEpochFees();
    }

    function test_distributeEpochFees_multipleRounds() public {
        uint256 amount1 = 10_000e6;
        uint256 amount2 = 15_000e6;

        // First round (80% distributed — burn skipped, no bridge)
        uint256 rls1 = _swapToRls(amount1);
        vm.prank(keeper);
        uint256 distributed1 = aggregator.distributeEpochFees();
        assertEq(distributed1, (rls1 * 8000) / 10000);

        // Second round — pending includes undistributed burn from round 1
        uint256 rls2 = _swapToRls(amount2);
        vm.prank(keeper);
        uint256 distributed2 = aggregator.distributeEpochFees();
        assertGt(distributed2, 0);

        // Verify cumulative stats
        (, uint256 totalDistributed) = aggregator.getStats();
        assertEq(totalDistributed, distributed1 + distributed2);
    }

    // =========================================================================
    //              Pending RLS Tracking (C-1 fix verification)
    // =========================================================================

    function test_pendingRlsForDistribution_trackedBySwap() public {
        uint256 amount = 10_000e6;
        uint256 rlsReceived = _swapToRls(amount);

        // Distribute — 80% goes out (validator+ecosystem), burn skipped (no bridge configured)
        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();
        uint256 expectedDistributed = (rlsReceived * 8000) / 10000;
        assertEq(distributed, expectedDistributed);

        // Burn portion stays in pendingBurnRls (not re-split).
        // Second call with no new RLS and no bridge still retries only burn, which fails again.
        vm.prank(keeper);
        uint256 distributed2 = aggregator.distributeEpochFees();
        // Burn portion stays pending (bridge not configured), nothing else to distribute
        assertEq(distributed2, 0);
    }

    function test_directRlsTransfer_notDistributed() public {
        // Mint RLS and transfer directly (not via swapToRls) — should NOT be distributed
        rlsToken.mint(address(aggregator), 10_000e18);

        // distributeEpochFees should skip — pendingRlsForDistribution is 0
        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();
        assertEq(distributed, 0);

        // RLS still sits in aggregator (recoverable via emergencyWithdraw)
        assertEq(rlsToken.balanceOf(address(aggregator)), 10_000e18);
    }

    function test_failedDistribution_partialAccounting() public {
        // Swap some RLS
        uint256 rlsReceived = _swapToRls(10_000e6);

        // Replace reward distributor with one that reverts on receiveRewards
        RevertingRewardDistributor revertingDistributor = new RevertingRewardDistributor();
        vm.prank(admin);
        aggregator.setRewardDistributor(address(revertingDistributor));

        // Distribute — validator receiveRewards reverts (but RLS was transferred),
        // ecosystem succeeds, burn skipped (no bridge)
        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();

        // Validator transfer succeeded (tokens left the contract) so it must be
        // counted as distributed even though receiveRewards() reverted. The RLS is now
        // in RewardDistributor but uncounted — recoverable as excess via
        // RewardDistributor.recoverTokens (the RecipientTransferFailed event flags it).
        // validator 50% + ecosystem 30% = 80% (burn skipped — no bridge configured)
        uint256 validatorExpected = (rlsReceived * 5000) / 10000;
        uint256 ecosystemExpected = (rlsReceived * 3000) / 10000;
        assertEq(distributed, validatorExpected + ecosystemExpected);

        // Stats reflect everything that actually left the contract
        (, uint256 totalDistributed) = aggregator.getStats();
        assertEq(totalDistributed, validatorExpected + ecosystemExpected);
    }

    // =========================================================================
    //                     Recipient Failure Isolation Tests
    // =========================================================================

    function test_distributeEpochFees_revertingRecipientDoesNotBlock() public {
        RevertingRewardDistributor revertingDistributor = new RevertingRewardDistributor();
        vm.prank(admin);
        aggregator.setRewardDistributor(address(revertingDistributor));

        // Use swap flow to set pendingRlsForDistribution
        uint256 rlsReceived = _swapToRls(10_000e6);

        // Distribution should still succeed despite reverting distributor
        vm.prank(keeper);
        uint256 rlsDistributed = aggregator.distributeEpochFees();

        // actualDistributed includes the validator transfer (tokens left the contract)
        assertGt(rlsDistributed, 0);

        // Ecosystem treasury received its share
        uint256 ecosystemExpected = (rlsReceived * 3000) / 10000;
        assertEq(rlsToken.balanceOf(ecosystemTreasury), ecosystemExpected);
    }

    function test_distributeEpochFees_whenPaused_reverts() public {
        _swapToRls(10_000e6);

        vm.prank(pauser);
        aggregator.pause();

        vm.prank(keeper);
        vm.expectRevert();
        aggregator.distributeEpochFees();
    }

    function test_distributeEpochFees_emitsCorrectEvents() public {
        uint256 rlsReceived = _swapToRls(10_000e6);
        uint256 expectedDistributed = (rlsReceived * 8000) / 10000;

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.EpochFeesDistributed(rlsReceived, expectedDistributed);

        vm.prank(keeper);
        aggregator.distributeEpochFees();
    }

    // =========================================================================
    //   Ecosystem leg failure + retry
    // =========================================================================

    function test_distributeEpochFees_ecosystemLegFails_retainsAndRetries() public {
        uint256 rlsReceived = _swapToRls(10_000e6);
        (uint256 validatorExpected, uint256 ecosystemExpected,) = _split(rlsReceived);

        // Force the RLS transfer to the ecosystem treasury to fail.
        rlsToken.setTransferBlocked(ecosystemTreasury, true);

        // The ecosystem leg fails: event logged, slice retained (validator leg still lands).
        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.RecipientTransferFailed(ecosystemTreasury, ecosystemExpected);

        vm.prank(keeper);
        uint256 distributed1 = aggregator.distributeEpochFees();

        // Only the validator slice left the contract (burn skipped — no bridge).
        assertEq(distributed1, validatorExpected);
        assertEq(rlsToken.balanceOf(ecosystemTreasury), 0);

        // The failed slice is retained in pendingEcosystemRls for retry.
        (, , uint256 pendingEcosystem,) = aggregator.getPendingDistribution();
        assertEq(pendingEcosystem, ecosystemExpected);

        // Fix the treasury and retry — the ecosystem slice now lands.
        rlsToken.setTransferBlocked(ecosystemTreasury, false);
        vm.prank(keeper);
        uint256 distributed2 = aggregator.distributeEpochFees();

        assertEq(distributed2, ecosystemExpected);
        assertEq(rlsToken.balanceOf(ecosystemTreasury), ecosystemExpected);

        // Ecosystem pending cleared after the successful retry.
        (, , uint256 pendingEcosystemAfter,) = aggregator.getPendingDistribution();
        assertEq(pendingEcosystemAfter, 0);
    }

    // =========================================================================
    //  Emergency RLS rescue + pending reset
    // =========================================================================

    // Rescue stuck RLS -> reset stale pending counters -> next cycle resumes (one flow).
    function test_emergencyWithdrawRls_thenResetPending_resumesNextCycle() public {
        _swapToRls(10_000e6);

        // Distribute: validator + ecosystem leave, burn slice (20%) stays (no bridge).
        vm.prank(keeper);
        aggregator.distributeEpochFees();

        // The undistributed burn slice is stuck in the contract.
        uint256 stuckRls = rlsToken.balanceOf(address(aggregator));
        assertGt(stuckRls, 0);

        // Admin rescues the stuck RLS.
        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.EmergencyWithdraw(address(rlsToken), admin, stuckRls);
        vm.prank(admin);
        aggregator.emergencyWithdraw(address(rlsToken), admin, stuckRls);

        assertEq(rlsToken.balanceOf(admin), stuckRls);
        assertEq(rlsToken.balanceOf(address(aggregator)), 0);

        // Reset clears all stale pending counters (the now-orphaned burn slice).
        (uint256 u0, uint256 v0, uint256 e0, uint256 b0) = aggregator.getPendingDistribution();
        uint256 expectedCleared = u0 + v0 + e0 + b0;
        assertGt(expectedCleared, 0);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.PendingDistributionReset(expectedCleared);
        vm.prank(admin);
        aggregator.resetPendingDistribution();

        (uint256 u1, uint256 v1, uint256 e1, uint256 b1) = aggregator.getPendingDistribution();
        assertEq(u1 + v1 + e1 + b1, 0);

        // The next fee cycle works normally after the reset.
        uint256 rls2 = _swapToRls(20_000e6);
        (uint256 v2, uint256 e2,) = _split(rls2);
        vm.prank(keeper);
        uint256 distributed2 = aggregator.distributeEpochFees();
        assertEq(distributed2, v2 + e2); // validator + ecosystem (burn skipped — no bridge)
    }

    function testRevert_resetPendingDistribution_notAdmin() public {
        vm.prank(keeper);
        vm.expectPartialRevert(IAccessControl.AccessControlUnauthorizedAccount.selector);
        aggregator.resetPendingDistribution();
    }

    // =========================================================================
    //   Swap slippage protection
    // =========================================================================

    function testRevert_swapToRls_insufficientSlippage() public {
        uint256 amount = 10_000e6;
        usdt.approve(address(aggregator), amount);
        aggregator.receiveFee(address(usdt), amount);

        // No FA-level slippage error: minRlsOut is forwarded to the router as
        // amountOutMinimum, so the router's revert is what bubbles up.
        vm.prank(keeper);
        vm.expectRevert("Too little received");
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({stablecoin: address(usdt), stablecoinAmount: amount, minRlsOut: amount + 1})
        );
    }

    // =========================================================================
    //   Burn slice bridged via OFT
    // =========================================================================

    function test_distributeEpochFees_burnSliceBridged() public {
        // Wire a mock OFT bridge with zero native fee so the burn leg can send.
        MockOFTBridge bridge = new MockOFTBridge(address(rlsToken), 0);
        vm.startPrank(admin);
        aggregator.setOftBridge(address(bridge));
        aggregator.setDstEid(ETH_DST_EID);
        vm.stopPrank();

        uint256 rlsReceived = _swapToRls(10_000e6);
        (, , uint256 burnExpected) = _split(rlsReceived);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.BurnBridged(burnExpected, burnAddress, ETH_DST_EID);

        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();

        // All three legs landed: validator + ecosystem + burn == 100%.
        assertEq(distributed, rlsReceived);

        // The burn slice was bridged (pulled into the OFT adapter) and pending cleared.
        assertEq(rlsToken.balanceOf(address(bridge)), burnExpected);
        (, , , uint256 pendingBurn) = aggregator.getPendingDistribution();
        assertEq(pendingBurn, 0);
    }

    function test_burnBridge_insufficientNativeFee_retainsAndRetries() public {
        // Bridge quotes a non-zero native fee; the aggregator holds no native, so the send fails.
        MockOFTBridge bridge = new MockOFTBridge(address(rlsToken), 1 ether);
        vm.startPrank(admin);
        aggregator.setOftBridge(address(bridge));
        aggregator.setDstEid(ETH_DST_EID);
        vm.stopPrank();

        uint256 rlsReceived = _swapToRls(10_000e6);
        (, , uint256 burnExpected) = _split(rlsReceived);

        // Burn leg fails (no native for the fee): logged, slice retained, nothing bridged.
        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.RecipientTransferFailed(burnAddress, burnExpected);

        vm.prank(keeper);
        aggregator.distributeEpochFees();

        assertEq(rlsToken.balanceOf(address(bridge)), 0);
        (, , , uint256 pendingBurn) = aggregator.getPendingDistribution();
        assertEq(pendingBurn, burnExpected);

        // Fund the aggregator with native and retry — the burn slice now bridges.
        vm.deal(address(aggregator), 1 ether);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.BurnBridged(burnExpected, burnAddress, ETH_DST_EID);

        vm.prank(keeper);
        aggregator.distributeEpochFees();

        assertEq(rlsToken.balanceOf(address(bridge)), burnExpected);
        (, , , uint256 pendingBurnAfter) = aggregator.getPendingDistribution();
        assertEq(pendingBurnAfter, 0);
    }

    function test_burnBridge_quoteSendReverts_retainsSlice() public {
        // quoteSend reverts — _bridgeBurn bails before approving/sending.
        RevertingOFTBridge bridge = new RevertingOFTBridge(true);
        vm.startPrank(admin);
        aggregator.setOftBridge(address(bridge));
        aggregator.setDstEid(ETH_DST_EID);
        vm.stopPrank();

        uint256 rlsReceived = _swapToRls(10_000e6);
        (, , uint256 burnExpected) = _split(rlsReceived);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.RecipientTransferFailed(burnAddress, burnExpected);

        vm.prank(keeper);
        aggregator.distributeEpochFees();

        assertEq(rlsToken.balanceOf(address(bridge)), 0);
        (, , , uint256 pendingBurn) = aggregator.getPendingDistribution();
        assertEq(pendingBurn, burnExpected);
    }

    function test_burnBridge_sendReverts_retainsSlice() public {
        // quoteSend succeeds with a zero fee, but the cross-chain send reverts inside the try/catch.
        RevertingOFTBridge bridge = new RevertingOFTBridge(false);
        vm.startPrank(admin);
        aggregator.setOftBridge(address(bridge));
        aggregator.setDstEid(ETH_DST_EID);
        vm.stopPrank();

        uint256 rlsReceived = _swapToRls(10_000e6);
        (, , uint256 burnExpected) = _split(rlsReceived);

        vm.expectEmit(true, true, true, true);
        emit IFeeAggregator.RecipientTransferFailed(burnAddress, burnExpected);

        vm.prank(keeper);
        aggregator.distributeEpochFees();

        assertEq(rlsToken.balanceOf(address(bridge)), 0);
        (, , , uint256 pendingBurn) = aggregator.getPendingDistribution();
        assertEq(pendingBurn, burnExpected);
    }
}
