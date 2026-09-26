// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {FeeAggregator} from "src/fees/FeeAggregator.sol";
import {RewardDistributor} from "src/fees/RewardDistributor.sol";
import {DelegationPool} from "src/consensus/DelegationPool.sol";
import {IFeeAggregator} from "src/interfaces/IFeeAggregator.sol";
import {IRewardDistributor} from "src/interfaces/IRewardDistributor.sol";
import {IDelegationPool} from "src/interfaces/IDelegationPool.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {SystemCallable} from "src/consensus/SystemCallable.sol";
import {ISwapRouter} from "src/interfaces/ISwapRouter.sol";
import {IAccessControl} from "@openzeppelin/contracts/access/IAccessControl.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";

// ============================================================================
//                              Mock Contracts
// ============================================================================

contract MockERC20AC is ERC20 {
    uint8 private _dec;

    constructor(string memory name_, string memory symbol_, uint8 decimals_) ERC20(name_, symbol_) {
        _dec = decimals_;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function decimals() public view override returns (uint8) {
        return _dec;
    }
}

contract MockAlgebraRouterAC {
    MockERC20AC public rlsToken;

    constructor(address rls_) {
        rlsToken = MockERC20AC(rls_);
    }

    function exactInputSingle(ISwapRouter.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut)
    {
        IERC20(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);
        amountOut = params.amountIn; // 1:1
        require(amountOut >= params.amountOutMinimum, "Too little");
        rlsToken.mint(params.recipient, amountOut);
    }
}

contract MockAlgebraPoolAC {
    function globalState()
        external
        pure
        returns (uint160, int24, uint16, uint8, uint8, uint8)
    {
        return (79228162514264337593543950336, 0, 0, 0, 0, 0);
    }
}

contract MockConsensusRegistryAC {
    mapping(address => IConsensusRegistry.ValidatorStatus) public validatorStatuses;
    mapping(address => uint256) public balances;
    mapping(address => uint256) public initialStakes;
    IConsensusRegistry.ValidatorInfo[] private _activeValidators;
    IConsensusRegistry.PerformanceWeights private _performanceWeights;
    mapping(address => bool) public allowlisted;
    uint32 private _currentEpoch;

    function setCurrentEpoch(uint32 epoch) external {
        _currentEpoch = epoch;
    }

    function getCurrentEpoch() external view returns (uint32) {
        return _currentEpoch;
    }

    IConsensusRegistry.EpochInfo private _epochInfo;

    function setEpochDuration(uint32 duration) external {
        _epochInfo.epochDuration = duration;
    }

    function getCurrentEpochInfo() external view returns (IConsensusRegistry.EpochInfo memory) {
        return _epochInfo;
    }

    function addActiveValidator(address validator, uint256 balance_) external {
        validatorStatuses[validator] = IConsensusRegistry.ValidatorStatus.Active;
        balances[validator] = balance_;
        initialStakes[validator] = balance_;
        allowlisted[validator] = true;
        _activeValidators.push(IConsensusRegistry.ValidatorInfo({
            blsPubkey: "",
            validatorAddress: validator,
            activationEpoch: 0,
            exitEpoch: 0,
            currentStatus: IConsensusRegistry.ValidatorStatus.Active,
            isRetired: false,
            isDelegated: false,
            stakeVersion: 0
        }));
    }

    function setPerformanceWeights(
        address[] memory validators,
        uint256[] memory weights,
        uint256 totalWeight
    ) external {
        _performanceWeights = IConsensusRegistry.PerformanceWeights({
            validators: validators,
            weights: weights,
            totalWeight: totalWeight
        });
    }

    function getEpochPerformanceWeights() external view returns (IConsensusRegistry.PerformanceWeights memory) {
        return _performanceWeights;
    }

    function getValidator(address validatorAddress) external view returns (IConsensusRegistry.ValidatorInfo memory) {
        return IConsensusRegistry.ValidatorInfo({
            blsPubkey: "",
            validatorAddress: validatorAddress,
            activationEpoch: 0,
            exitEpoch: 0,
            currentStatus: validatorStatuses[validatorAddress],
            isRetired: false,
            isDelegated: false,
            stakeVersion: 0
        });
    }

    function getValidators(uint8) external view returns (IConsensusRegistry.ValidatorInfo[] memory) {
        return _activeValidators;
    }

    function getBalanceBreakdown(address validator) external view returns (uint256, uint256, uint256) {
        uint256 initial = initialStakes[validator];
        if (initial == 0) initial = balances[validator];
        return (balances[validator], initial, 0);
    }

    function isAllowlisted(address addr) external view returns (bool) {
        return allowlisted[addr];
    }

    receive() external payable {}
}

contract MockRewardDistributorAC {
    uint256 public lastReceivedAmount;

    function receiveRewards(uint256 amount) external {
        lastReceivedAmount = amount;
    }
}

/// @notice Mock RewardDistributor that reverts on receiveRewards
contract RevertingRewardDistributorAC {
    function receiveRewards(uint256) external pure {
        revert("DOS");
    }
}

// ============================================================================
//                              Test Contract
// ============================================================================

contract TokenomicsAccessControlTest is Test {
    // Contracts
    FeeAggregator public aggregator;
    RewardDistributor public distributor;
    DelegationPool public delegationPool;

    // Mocks
    MockERC20AC public rls;
    MockERC20AC public usdt;
    MockERC20AC public usdrToken;
    MockAlgebraRouterAC public router;
    MockAlgebraPoolAC public usdtPool;
    MockAlgebraPoolAC public usdrPool;
    MockConsensusRegistryAC public registry;
    MockRewardDistributorAC public mockRewardDistributor;

    // Addresses
    address public admin = address(0xAD01);
    address public keeper = address(0xBE02);
    address public pauser = address(0xAA03);
    address public nobody = address(0xBAD);
    address public ecosystemTreasury = address(0xEC0);
    address public burnAddress = address(0xdead);
    address constant SYSTEM_ADDRESS = address(0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE);

    address public validator1 = address(0x2001);
    address public validator2 = address(0x2002);

    // Role hashes
    bytes32 public constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    function setUp() public {
        // Deploy tokens
        rls = new MockERC20AC("RLS", "RLS", 18);
        usdt = new MockERC20AC("USDT", "USDT", 6);
        usdrToken = new MockERC20AC("USDr", "USDr", 18);

        // Deploy mocks
        router = new MockAlgebraRouterAC(address(rls));
        usdtPool = new MockAlgebraPoolAC();
        usdrPool = new MockAlgebraPoolAC();
        registry = new MockConsensusRegistryAC();
        mockRewardDistributor = new MockRewardDistributorAC();

        // ---- FeeAggregator (via proxy) ----
        FeeAggregator aggImpl = new FeeAggregator();
        IFeeAggregator.DistributionConfig memory config = IFeeAggregator.DistributionConfig({
            validatorPoolBps: 5000,
            ecosystemBps: 3000,
            burnBps: 2000
        });
        bytes memory aggInit = abi.encodeWithSelector(
            FeeAggregator.initialize.selector,
            address(rls),
            address(router),
            address(mockRewardDistributor),
            ecosystemTreasury,
            burnAddress,
            address(0), // usdrToken — configured later in setup
            config,
            admin
        );
        ERC1967Proxy aggProxy = new ERC1967Proxy(address(aggImpl), aggInit);
        aggregator = FeeAggregator(payable(address(aggProxy)));

        // Grant FeeAggregator roles
        vm.startPrank(admin);
        aggregator.grantRole(KEEPER_ROLE, keeper);
        aggregator.grantRole(PAUSER_ROLE, pauser);
        aggregator.addStablecoin(address(usdt));
        aggregator.addStablecoin(address(usdrToken));
        aggregator.setPoolConfig(address(usdt), address(usdtPool), true, address(0));
        aggregator.setPoolConfig(address(usdrToken), address(usdrPool), true, address(0));
        aggregator.setUsdrToken(address(usdrToken));
        vm.stopPrank();

        // ---- RewardDistributor (via proxy) ----
        RewardDistributor rdImpl = new RewardDistributor();
        bytes memory rdInit = abi.encodeCall(
            RewardDistributor.initialize,
            (address(rls), address(aggregator), address(registry), address(0), admin)
        );
        ERC1967Proxy rdProxy = new ERC1967Proxy(address(rdImpl), rdInit);
        distributor = RewardDistributor(address(rdProxy));

        // ---- DelegationPool (via proxy) ----
        DelegationPool dpImpl = new DelegationPool();
        IDelegationPool.DelegationConfig memory dpConfig = IDelegationPool.DelegationConfig({
            minDelegation: 1e18,
            maxDelegation: 1_000_000e18,
            maxValidatorDelegation: 10_000_000e18,
            unbondingEpochs: 2,
            commissionDelayEpochs: 2
        });
        bytes memory dpInit = abi.encodeCall(
            DelegationPool.initialize,
            (address(rls), address(registry), admin, dpConfig)
        );
        ERC1967Proxy dpProxy = new ERC1967Proxy(address(dpImpl), dpInit);
        delegationPool = DelegationPool(address(dpProxy));

        // Wire DelegationPool's rewardDistributor
        vm.prank(admin);
        delegationPool.setRewardDistributor(address(distributor));

        // Wire RewardDistributor's delegationPool
        vm.prank(admin);
        distributor.setDelegationPool(address(delegationPool));

        // Set up validators
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 100e18);

        // Fund for testing
        rls.mint(address(aggregator), 1_000_000e18);
        rls.mint(address(distributor), 1_000_000e18);
        usdt.mint(address(aggregator), 1_000_000e6);
        usdrToken.mint(address(aggregator), 1_000_000e18);
    }

    // =========================================================================
    //  Section 11: Access Control
    // =========================================================================

    // 1. Non-keeper calling swapToRls reverts
    function test_feeAggregator_swapToRls_onlyKeeper() public {
        IFeeAggregator.SwapParams memory params = IFeeAggregator.SwapParams({
            stablecoin: address(usdt),
            stablecoinAmount: 10_000e6,
            minRlsOut: 0
        });
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                KEEPER_ROLE
            )
        );
        aggregator.swapToRls(params);
    }

    // 2. Non-keeper calling distributeEpochFees reverts
    function test_feeAggregator_distributeEpochFees_onlyKeeper() public {
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                KEEPER_ROLE
            )
        );
        aggregator.distributeEpochFees();
    }

    // 3. Non-admin calling emergencyWithdraw reverts
    function test_feeAggregator_emergencyWithdraw_onlyAdmin() public {
        bytes32 defaultAdminRole = 0x00;
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                defaultAdminRole
            )
        );
        aggregator.emergencyWithdraw(address(usdt), nobody, 1000);
    }

    // 4. Non-admin calling addStablecoin reverts
    function test_feeAggregator_addStablecoin_onlyAdmin() public {
        bytes32 defaultAdminRole = 0x00;
        address newStable = address(new MockERC20AC("DAI", "DAI", 18));
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                defaultAdminRole
            )
        );
        aggregator.addStablecoin(newStable);
    }

    // 5. Non-admin calling setAlgebraRouter reverts
    function test_feeAggregator_setAlgebraRouter_onlyAdmin() public {
        bytes32 defaultAdminRole = 0x00;
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                defaultAdminRole
            )
        );
        aggregator.setAlgebraRouter(address(0x123));
    }

    // 6. Non-system address calling distributeRewards reverts
    function test_rewardDistributor_distributeRewards_onlySystemCall() public {
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(SystemCallable.OnlySystemCall.selector, nobody)
        );
        distributor.distributeRewards();
    }

    // 7. Random caller (not feeAggregator) calling receiveRewards reverts
    function test_rewardDistributor_receiveRewards_onlyFeeAggregator() public {
        vm.prank(nobody);
        vm.expectRevert(IRewardDistributor.OnlyFeeAggregator.selector);
        distributor.receiveRewards(1000e18);
    }

    // 8. Non-validator cannot claim another validator's rewards
    function test_rewardDistributor_claimRewards_onlyValidator() public {
        // Give validator1 some pending rewards
        _sendAndReceiveRewards(100e18);
        _distributeAsSystem();

        // nobody tries to claim validator1's rewards
        vm.prank(nobody);
        vm.expectRevert(IRewardDistributor.NotAuthorized.selector);
        distributor.claimRewards(validator1);
    }

    // 9. Random caller calling distributePoolRewards reverts
    function test_delegationPool_distributePoolRewards_onlyRewardSources() public {
        vm.prank(nobody);
        vm.expectRevert(IDelegationPool.OnlyRewardSources.selector);
        delegationPool.distributePoolRewards(validator1, 100e18);
    }

    // 10. Random caller calling applyPoolSlash reverts
    function test_delegationPool_applyPoolSlash_onlyConsensusRegistry() public {
        vm.prank(nobody);
        vm.expectRevert(IDelegationPool.OnlyConsensusRegistry.selector);
        delegationPool.applyPoolSlash(validator1, 50e18);
    }

    // 10b. Non-admin calling setPerformanceWeightBps reverts; admin succeeds; disabled by
    //      default and pausing (tested elsewhere, see #6/#486-514) does not gate this setter.
    function test_rewardDistributor_setPerformanceWeightBps_onlyAdmin() public {
        bytes32 defaultAdminRole = 0x00;
        assertEq(distributor.performanceWeightBps(), 0, "disabled by default");

        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                nobody,
                defaultAdminRole
            )
        );
        distributor.setPerformanceWeightBps(10_000);

        vm.prank(admin);
        distributor.setPerformanceWeightBps(10_000);
        assertEq(distributor.performanceWeightBps(), 10_000);
    }

    // 10c. Setting performanceWeightBps above 100% reverts
    function test_rewardDistributor_setPerformanceWeightBps_boundsCheck() public {
        vm.prank(admin);
        vm.expectRevert(IRewardDistributor.InvalidPerformanceWeightBps.selector);
        distributor.setPerformanceWeightBps(10_001);
    }

    // =========================================================================
    //  Section 12: Emergency & Recovery
    // =========================================================================

    // 11. Admin recovers stuck stablecoins from FeeAggregator
    function test_feeAggregator_emergencyWithdraw_stablecoins() public {
        uint256 stuckAmount = 5_000e6;
        // usdt was already minted to aggregator in setUp
        uint256 adminBalBefore = usdt.balanceOf(admin);

        vm.prank(admin);
        aggregator.emergencyWithdraw(address(usdt), admin, stuckAmount);

        assertEq(usdt.balanceOf(admin), adminBalBefore + stuckAmount);
    }

    // 12. Admin recovers stuck native ETH from FeeAggregator
    function test_feeAggregator_emergencyWithdrawNative() public {
        // Send some ETH to aggregator
        vm.deal(address(aggregator), 10 ether);
        uint256 adminBalBefore = admin.balance;

        vm.prank(admin);
        aggregator.emergencyWithdrawNative(admin, 5 ether);

        assertEq(admin.balance, adminBalBefore + 5 ether);
        assertEq(address(aggregator).balance, 5 ether);
    }

    // 13. Admin recovers excess tokens from RewardDistributor (non-pending)
    function test_rewardDistributor_recoverTokens_nonPending() public {
        // Mint extra RLS directly to distributor (excess, not pending)
        uint256 extraAmount = 500e18;
        rls.mint(address(distributor), extraAmount);

        uint256 adminBalBefore = rls.balanceOf(admin);

        vm.prank(admin);
        distributor.recoverTokens(address(rls), admin, extraAmount);

        assertEq(rls.balanceOf(admin), adminBalBefore + extraAmount);
    }

    // 14. Cannot withdraw pending rewards via recoverTokens
    function test_rewardDistributor_recoverTokens_cannotStealPending() public {
        // Receive rewards to create pending balance
        _sendAndReceiveRewards(100e18);

        uint256 totalPending = distributor.totalPendingRewards();
        assertGt(totalPending, 0);

        // Try to recover more than available (total balance - pending)
        uint256 balance = rls.balanceOf(address(distributor));
        uint256 available = balance - totalPending;
        uint256 tooMuch = available + 1;

        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                IRewardDistributor.InsufficientBalance.selector,
                tooMuch,
                available
            )
        );
        distributor.recoverTokens(address(rls), admin, tooMuch);
    }

    // 15. Pause blocks user ops on FeeAggregator, but system calls on RewardDistributor still work
    function test_feeAggregator_pauseUnpause_systemCallsUnaffected() public {
        // Pause the FeeAggregator
        vm.prank(pauser);
        aggregator.pause();

        // User op: receiveFee should revert with EnforcedPause
        usdt.mint(nobody, 1_000e6);
        vm.startPrank(nobody);
        usdt.approve(address(aggregator), 1_000e6);
        vm.expectRevert(PausableUpgradeable.EnforcedPause.selector);
        aggregator.receiveFee(address(usdt), 1_000e6);
        vm.stopPrank();

        // Keeper op: swapToRls should also revert when paused
        IFeeAggregator.SwapParams memory params = IFeeAggregator.SwapParams({
            stablecoin: address(usdt),
            stablecoinAmount: 10_000e6,
            minRlsOut: 0
        });
        vm.prank(keeper);
        vm.expectRevert(PausableUpgradeable.EnforcedPause.selector);
        aggregator.swapToRls(params);

        // System call: RewardDistributor.distributeRewards still works (separate contract, not paused)
        // Should not revert — the call just distributes 0 with no active pending
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards(); // no revert = success

        // Unpause and verify normal ops resume
        vm.prank(pauser);
        aggregator.unpause();

        // receiveFee should now work
        usdt.mint(nobody, 1_000e6);
        vm.startPrank(nobody);
        usdt.approve(address(aggregator), 1_000e6);
        aggregator.receiveFee(address(usdt), 1_000e6);
        vm.stopPrank();
    }

    // 16. If one distribution recipient fails (reverts), others still succeed
    function test_feeAggregator_distributionRecipientFailure_noBlock() public {
        // Replace the reward distributor with a reverting one
        RevertingRewardDistributorAC revertingRD = new RevertingRewardDistributorAC();
        vm.prank(admin);
        aggregator.setRewardDistributor(address(revertingRD));

        // Swap some stablecoins to RLS first to build pending distribution
        usdt.mint(address(aggregator), 50_000e6);
        vm.prank(keeper);
        aggregator.swapToRls(
            IFeeAggregator.SwapParams({
                stablecoin: address(usdt),
                stablecoinAmount: 10_000e6,
                minRlsOut: 0
            })
        );

        // Record ecosystem treasury balance before distribution
        uint256 ecosystemBefore = rls.balanceOf(ecosystemTreasury);

        // distributeEpochFees should NOT revert even though the reward distributor reverts internally
        vm.prank(keeper);
        uint256 distributed = aggregator.distributeEpochFees();

        // Ecosystem treasury should have received its share despite validator pool failure
        uint256 ecosystemAfter = rls.balanceOf(ecosystemTreasury);
        assertGt(ecosystemAfter, ecosystemBefore, "Ecosystem treasury should receive funds despite validator pool failure");
        assertGt(distributed, 0, "Some distribution should have succeeded");
    }

    // =========================================================================
    //  Section 9: Block Production Non-Blocking
    // =========================================================================

    // 17. System call with 0 pending doesn't revert
    function test_rewardDistributor_distributeRewards_zeroPending_noRevert() public {
        // No rewards have been received — totalPending is 0
        assertEq(distributor.totalPendingRewards(), 0);

        // System call should not revert
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Still 0 pending after
        assertEq(distributor.totalPendingRewards(), 0);
    }

    // 18. Non-system address calling distributeRewards reverts
    function test_systemAddress_concludeEpoch_onlySystem() public {
        vm.prank(nobody);
        vm.expectRevert(
            abi.encodeWithSelector(SystemCallable.OnlySystemCall.selector, nobody)
        );
        distributor.distributeRewards();
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    function _sendAndReceiveRewards(uint256 amount) internal {
        address feeAgg = distributor.feeAggregator();
        vm.startPrank(feeAgg);
        rls.mint(feeAgg, amount);
        rls.mint(address(distributor), amount);
        IERC20(address(rls)).transfer(address(distributor), 0); // noop to keep prank active
        distributor.receiveRewards(amount);
        vm.stopPrank();
    }

    function _distributeAsSystem() internal {
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();
    }
}
