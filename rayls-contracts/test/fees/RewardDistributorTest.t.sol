// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {RewardDistributor} from "src/fees/RewardDistributor.sol";
import {IRewardDistributor} from "src/interfaces/IRewardDistributor.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {IDelegationPool} from "src/interfaces/IDelegationPool.sol";
import {SystemCallable} from "src/consensus/SystemCallable.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {RLSAccumulator} from "src/fees/RLSAccumulator.sol";

/// @notice Mock RLS token (ERC-20 staking token) for testing
contract MockRLS is ERC20 {
    constructor() ERC20("Mock RLS", "RLS") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @notice Mock ConsensusRegistry for testing
contract MockConsensusRegistry {
    mapping(address => IConsensusRegistry.ValidatorStatus) public validatorStatuses;
    mapping(address => uint256) public balances;
    mapping(address => uint256) public initialStakes;
    IConsensusRegistry.ValidatorInfo[] private _activeValidators;
    IConsensusRegistry.PerformanceWeights private _performanceWeights;

    function setValidatorStatus(address validator, IConsensusRegistry.ValidatorStatus status) external {
        validatorStatuses[validator] = status;
    }

    function setBalance(address validator, uint256 balance) external {
        balances[validator] = balance;
    }

    function setInitialStake(address validator, uint256 stake) external {
        initialStakes[validator] = stake;
    }

    function addActiveValidator(address validator, uint256 balance) external {
        validatorStatuses[validator] = IConsensusRegistry.ValidatorStatus.Active;
        balances[validator] = balance;
        initialStakes[validator] = balance; // default: initialStake == balance
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

    function clearValidators() external {
        delete _activeValidators;
        delete _performanceWeights;
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

    function getBalance(address validator) external view returns (uint256) {
        return balances[validator];
    }

    /// @notice IStakeManager.getBalanceBreakdown implementation
    function getBalanceBreakdown(address validator) external view returns (uint256, uint256, uint256) {
        // returns (outstandingBalance, initialStakeAmount, rewards)
        uint256 initial = initialStakes[validator];
        if (initial == 0) initial = balances[validator]; // backward compat
        return (balances[validator], initial, 0);
    }

    IConsensusRegistry.EpochInfo private _epochInfo;

    function setEpochDuration(uint32 duration) external {
        _epochInfo.epochDuration = duration;
    }

    function getCurrentEpochInfo() external view returns (IConsensusRegistry.EpochInfo memory) {
        return _epochInfo;
    }

    receive() external payable {}
}

/// @notice Mock DelegationPool for testing (ERC-20 RLS)
contract MockDelegationPool {
    mapping(address => uint256) public delegatedStakes;
    mapping(address => uint256) public distributedRewards;
    IERC20 public rlsToken;

    function setRlsToken(address rls_) external {
        rlsToken = IERC20(rls_);
    }

    function setDelegatedStake(address validator, uint256 stake) external {
        delegatedStakes[validator] = stake;
    }

    function getTotalDelegatedStake(address validator) external view returns (uint256) {
        return delegatedStakes[validator];
    }

    function getTotalOpenTierDelegatedStake(address) external pure returns (uint256) {
        return 0;
    }

    function distributePoolRewards(address validator, uint256 amount) external {
        distributedRewards[validator] += amount;
    }

    function distributePoolRewards(address validator, uint256 trackAAmount, uint256 trackBAmount) external {
        distributedRewards[validator] += trackAAmount + trackBAmount;
    }
}

contract RewardDistributorTest is Test {
    RewardDistributor public distributor;
    MockRLS public rls;
    MockConsensusRegistry public registry;
    MockDelegationPool public delegationPool;

    address public owner = address(0xABCD);
    address public feeAggregator = address(0x1001);
    address constant SYSTEM_ADDRESS = address(0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE);
    address public validator1 = address(0x2001);
    address public validator2 = address(0x2002);
    address public validator3 = address(0x2003);
    address public user = address(0x3001);

    function setUp() public {
        rls = new MockRLS();
        registry = new MockConsensusRegistry();
        delegationPool = new MockDelegationPool();
        delegationPool.setRlsToken(address(rls));

        RewardDistributor impl = new RewardDistributor();
        bytes memory initData = abi.encodeCall(
            RewardDistributor.initialize,
            (address(rls), feeAggregator, address(registry), address(delegationPool), owner)
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        distributor = RewardDistributor(address(proxy));

        // set up validators
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 100e18);
        registry.addActiveValidator(validator3, 100e18);

        // fund fee distributor with RLS
        rls.mint(feeAggregator, 1_000_000e18);
    }

    // Helper: transfer RLS to distributor and call receiveRewards
    function _receiveRewards(uint256 amount) internal {
        vm.startPrank(feeAggregator);
        rls.transfer(address(distributor), amount);
        distributor.receiveRewards(amount);
        vm.stopPrank();
    }

    // =========================================================================
    //                          Initialize
    // =========================================================================

    function test_initialize() public view {
        assertEq(distributor.rlsToken(), address(rls));
        assertEq(distributor.feeAggregator(), feeAggregator);
        assertEq(address(distributor.consensusRegistry()), address(registry));
        assertEq(address(distributor.delegationPool()), address(delegationPool));
        assertTrue(distributor.hasRole(distributor.DEFAULT_ADMIN_ROLE(), owner));
        assertTrue(distributor.hasRole(distributor.UPGRADER_ROLE(), owner));
    }

    function testRevert_initialize_zeroRls() public {
        RewardDistributor impl2 = new RewardDistributor();
        bytes memory initData = abi.encodeCall(
            RewardDistributor.initialize,
            (address(0), feeAggregator, address(registry), address(delegationPool), owner)
        );
        vm.expectRevert(IRewardDistributor.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_zeroRegistry() public {
        RewardDistributor impl2 = new RewardDistributor();
        bytes memory initData = abi.encodeCall(
            RewardDistributor.initialize,
            (address(rls), feeAggregator, address(0), address(delegationPool), owner)
        );
        vm.expectRevert(IRewardDistributor.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    function testRevert_initialize_zeroAdmin() public {
        RewardDistributor impl2 = new RewardDistributor();
        bytes memory initData = abi.encodeCall(
            RewardDistributor.initialize,
            (address(rls), feeAggregator, address(registry), address(delegationPool), address(0))
        );
        vm.expectRevert(IRewardDistributor.ZeroAddress.selector);
        new ERC1967Proxy(address(impl2), initData);
    }

    // =========================================================================
    //                          Receive Rewards
    // =========================================================================

    function test_receiveRewards() public {
        uint256 amount = 1000e18;

        _receiveRewards(amount);

        assertEq(distributor.totalPendingRewards(), amount);
        assertEq(rls.balanceOf(address(distributor)), amount);
    }

    function test_receiveRewards_multiple() public {
        _receiveRewards(500e18);
        _receiveRewards(500e18);

        assertEq(distributor.totalPendingRewards(), 1000e18);
    }

    function testRevert_receiveRewards_notFeeDistributor() public {
        rls.mint(user, 100e18);
        vm.startPrank(user);
        rls.transfer(address(distributor), 100e18);
        vm.expectRevert(IRewardDistributor.OnlyFeeAggregator.selector);
        distributor.receiveRewards(100e18);
        vm.stopPrank();
    }

    function testRevert_receiveRewards_zeroAmount() public {
        vm.prank(feeAggregator);
        vm.expectRevert(IRewardDistributor.ZeroAmount.selector);
        distributor.receiveRewards(0);
    }

    // =========================================================================
    //                          Distribute Rewards
    // =========================================================================

    function test_distributeRewards_equalStake() public {
        _receiveRewards(300e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // each validator has 100e18 stake, so each gets 100e18
        assertEq(distributor.getPendingRewards(validator1), 100e18);
        assertEq(distributor.getPendingRewards(validator2), 100e18);
        assertEq(distributor.getPendingRewards(validator3), 100e18);
    }

    function test_distributeRewards_unequalStake() public {
        // reset validators
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 200e18);
        registry.addActiveValidator(validator3, 300e18);

        _receiveRewards(600e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // total stake = 600e18, total rewards = 600e18
        // validator1: 100/600 * 600 = 100
        // validator2: 200/600 * 600 = 200
        // validator3: 300/600 * 600 = 300
        assertEq(distributor.getPendingRewards(validator1), 100e18);
        assertEq(distributor.getPendingRewards(validator2), 200e18);
        assertEq(distributor.getPendingRewards(validator3), 300e18);
    }

    function test_distributeRewards_withDelegations() public {
        // set up delegations
        delegationPool.setDelegatedStake(validator1, 100e18); // 50% delegated
        delegationPool.setDelegatedStake(validator2, 0);       // no delegations
        delegationPool.setDelegatedStake(validator3, 300e18); // 75% delegated

        _receiveRewards(600e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // total stakes: v1=200, v2=100, v3=400, total=700
        // v1 reward = 600 * 200/700 = 171.43e18
        //   - validator share = 171.43 * 100/200 = 85.71e18
        //   - pool share = 171.43 * 100/200 = 85.71e18

        // v2 reward = 600 * 100/700 = 85.71e18 (all to validator)

        // v3 reward = 600 * 400/700 = 342.86e18
        //   - validator share = 342.86 * 100/400 = 85.71e18
        //   - pool share = 342.86 * 300/400 = 257.14e18

        // check pool received correct amounts
        assertGt(delegationPool.distributedRewards(validator1), 0);
        assertGt(delegationPool.distributedRewards(validator3), 0);
        assertEq(delegationPool.distributedRewards(validator2), 0);
    }

    function testRevert_distributeRewards_notSystemCall() public {
        _receiveRewards(300e18);

        vm.prank(user);
        vm.expectRevert(abi.encodeWithSelector(SystemCallable.OnlySystemCall.selector, user));
        distributor.distributeRewards();
    }

    function test_distributeRewards_noRewards_silentReturn() public {
        vm.prank(SYSTEM_ADDRESS);
        vm.expectEmit(true, true, true, true);
        emit IRewardDistributor.RewardsDistributed(0, 0);
        distributor.distributeRewards();
    }

    function test_distributeRewards_noValidators_reverts() public {
        registry.clearValidators();

        _receiveRewards(100e18);

        // Should revert — no active validators to distribute to
        vm.prank(SYSTEM_ADDRESS);
        vm.expectRevert(IRewardDistributor.NoActiveValidators.selector);
        distributor.distributeRewards();
        // Rewards remain pending
        assertEq(distributor.totalPendingRewards(), 100e18);
    }

    // =========================================================================
    //                          Claim Rewards
    // =========================================================================

    function test_claimRewards() public {
        _receiveRewards(300e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        uint256 balanceBefore = rls.balanceOf(validator1);

        vm.prank(validator1);
        distributor.claimRewards(validator1);

        assertEq(rls.balanceOf(validator1) - balanceBefore, 100e18);
        assertEq(distributor.getPendingRewards(validator1), 0);
    }

    function testRevert_claimRewards_noPending() public {
        vm.prank(validator1);
        vm.expectRevert(IRewardDistributor.ZeroAmount.selector);
        distributor.claimRewards(validator1);
    }

    // =========================================================================
    //                          Admin Functions
    // =========================================================================

    function test_setFeeAggregator() public {
        address newDistributor = address(0x9999);

        vm.prank(owner);
        distributor.setFeeAggregator(newDistributor);

        assertEq(distributor.feeAggregator(), newDistributor);
    }

    function testRevert_setFeeAggregator_notOwner() public {
        vm.prank(user);
        vm.expectRevert();
        distributor.setFeeAggregator(address(0x9999));
    }

    function testRevert_setFeeAggregator_zeroAddress() public {
        vm.prank(owner);
        vm.expectRevert(IRewardDistributor.ZeroAddress.selector);
        distributor.setFeeAggregator(address(0));
    }

    function test_setDelegationPool() public {
        address newPool = address(0x8888);

        vm.prank(owner);
        distributor.setDelegationPool(newPool);

        assertEq(address(distributor.delegationPool()), newPool);
    }

    function test_setConsensusRegistry() public {
        address newRegistry = address(0x7777);

        vm.prank(owner);
        distributor.setConsensusRegistry(newRegistry);

        assertEq(address(distributor.consensusRegistry()), newRegistry);
    }

    // =========================================================================
    //                          Recovery
    // =========================================================================

    function test_recoverTokens() public {
        // send extra RLS directly (not through receiveRewards)
        rls.mint(address(distributor), 100e18);

        vm.prank(owner);
        distributor.recoverTokens(address(rls), user, 100e18);

        assertEq(rls.balanceOf(user), 100e18);
    }

    function testRevert_recoverTokens_pendingRewards() public {
        _receiveRewards(100e18);

        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(IRewardDistributor.InsufficientBalance.selector, 100e18, 0));
        distributor.recoverTokens(address(rls), user, 100e18);
    }

    function test_recoverTokens_excessRLS() public {
        // receive rewards
        _receiveRewards(100e18);

        // send extra RLS directly
        rls.mint(address(distributor), 50e18);

        // can recover the extra 50
        vm.prank(owner);
        distributor.recoverTokens(address(rls), user, 50e18);

        assertEq(rls.balanceOf(user), 50e18);
        // pending rewards still intact
        assertEq(distributor.totalPendingRewards(), 100e18);
    }

    // =========================================================================
    //                          Reward Recipient
    // =========================================================================

    function test_setRewardRecipient() public {
        address recipient = address(0x4001);

        vm.prank(validator1);
        distributor.setRewardRecipient(recipient);

        assertEq(distributor.getRewardRecipient(validator1), recipient);
    }

    function test_getRewardRecipient_defaultSelf() public view {
        // when no recipient is set, returns validator address
        assertEq(distributor.getRewardRecipient(validator1), validator1);
    }

    function test_setRewardRecipient_resetToZero() public {
        address recipient = address(0x4001);

        // set custom recipient
        vm.prank(validator1);
        distributor.setRewardRecipient(recipient);
        assertEq(distributor.getRewardRecipient(validator1), recipient);

        // reset to zero (back to self)
        vm.prank(validator1);
        distributor.setRewardRecipient(address(0));
        assertEq(distributor.getRewardRecipient(validator1), validator1);
    }

    // =========================================================================
    //                     Accumulator Top-Up Tests
    // =========================================================================

    function _setupAccumulator(uint256 fundAmount) internal returns (RLSAccumulator acc) {
        RLSAccumulator impl = new RLSAccumulator();
        bytes memory initData = abi.encodeCall(
            RLSAccumulator.initialize,
            (address(rls), address(distributor), owner)
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        acc = RLSAccumulator(address(proxy));

        // Fund accumulator
        rls.mint(address(acc), fundAmount);

        // Configure on RewardDistributor
        vm.startPrank(owner);
        distributor.setAccumulator(address(acc));
        distributor.setTargetApyBps(5000); // 50%
        vm.stopPrank();

        // Set epoch duration (e.g., 1 day = 86400 seconds)
        registry.setEpochDuration(86400);
    }

    function test_accumulatorTopUp_pullsShortfall() public {
        RLSAccumulator acc = _setupAccumulator(1_000_000e18);

        // No fee rewards — entire target should be pulled from accumulator
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // totalStaked = 300e18 (3 validators × 100e18)
        // targetReward = (300e18 * 5000 * 86400) / (365 days * 10000)
        //              = (300e18 * 5000 * 86400) / (31536000 * 10000)
        //              ≈ 0.41e18
        uint256 totalStaked = 300e18;
        uint256 expectedTarget = (totalStaked * 5000 * 86400) / (365 days * 10_000);

        // Validators should have received rewards
        uint256 totalDistributed = distributor.getPendingRewards(validator1)
            + distributor.getPendingRewards(validator2)
            + distributor.getPendingRewards(validator3);

        assertGt(totalDistributed, 0);
        // Accumulator balance should have decreased
        assertLt(rls.balanceOf(address(acc)), 1_000_000e18);
    }

    function test_accumulatorTopUp_noShortfallWhenFeesExceedTarget() public {
        RLSAccumulator acc = _setupAccumulator(1_000_000e18);
        uint256 accBalanceBefore = rls.balanceOf(address(acc));

        // Receive large fee rewards that exceed the APY target
        _receiveRewards(100_000e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Accumulator should not have been touched
        assertEq(rls.balanceOf(address(acc)), accBalanceBefore);
    }

    function test_accumulatorTopUp_partialShortfall() public {
        RLSAccumulator acc = _setupAccumulator(1_000_000e18);
        uint256 accBalanceBefore = rls.balanceOf(address(acc));

        // Small fee rewards — only partial top-up needed
        _receiveRewards(1e15); // tiny amount

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Accumulator pulled some but not all
        uint256 pulled = accBalanceBefore - rls.balanceOf(address(acc));
        assertGt(pulled, 0);
    }

    function test_accumulatorTopUp_emptyAccumulator_noRevert() public {
        // Accumulator with zero balance
        _setupAccumulator(0);

        // Receive some fee rewards
        _receiveRewards(100e18);

        // Should distribute fee rewards without reverting
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Fee rewards distributed normally
        assertGt(distributor.getPendingRewards(validator1), 0);
    }

    function test_accumulatorTopUp_notConfigured_noEffect() public {
        // Don't configure accumulator — default is address(0)
        registry.setEpochDuration(86400);

        // Use 300e18 for clean 3-way split (no rounding dust)
        _receiveRewards(300e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Distributed fee rewards only
        uint256 total = distributor.getPendingRewards(validator1)
            + distributor.getPendingRewards(validator2)
            + distributor.getPendingRewards(validator3);
        assertEq(total, 300e18);
    }

    function test_accumulatorTopUp_zeroApyBps_noEffect() public {
        _setupAccumulator(1_000_000e18);

        // Set APY to 0
        vm.prank(owner);
        distributor.setTargetApyBps(0);

        uint256 accBalanceBefore = rls.balanceOf(address(distributor.accumulator()));

        _receiveRewards(100e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Accumulator untouched
        assertEq(rls.balanceOf(address(distributor.accumulator())), accBalanceBefore);
    }

    function test_setTargetApyBps_cappedAt100Percent() public {
        vm.prank(owner);
        vm.expectRevert(IRewardDistributor.InvalidApyBps.selector);
        distributor.setTargetApyBps(10_001);
    }

    function test_setTargetApyBps_maxAllowed() public {
        vm.prank(owner);
        distributor.setTargetApyBps(10_000); // 100% — should succeed
        assertEq(distributor.targetApyBps(), 10_000);
    }

    function test_setAccumulator() public {
        address newAcc = address(0x5555);
        vm.prank(owner);
        distributor.setAccumulator(newAcc);
        assertEq(distributor.accumulator(), newAcc);
    }

    // =========================================================================
    //                     Graceful Failure Tests
    // =========================================================================

    function test_distributeRewards_inProgress_silentReturn() public {
        vm.prank(SYSTEM_ADDRESS);
        vm.expectEmit(true, true, true, true);
        emit IRewardDistributor.RewardsDistributed(0, 0);
        distributor.distributeRewards();
    }

    function test_distributeRewards_noValidators_silentReturn() public {
        registry.clearValidators();
        _receiveRewards(100e18);

        vm.prank(SYSTEM_ADDRESS);
        vm.expectRevert(IRewardDistributor.NoActiveValidators.selector);
        distributor.distributeRewards();
    }

    // =========================================================================
    //                     Custom Recipient Tests
    // =========================================================================

    function test_claimRewards_toCustomRecipient() public {
        address recipient = address(0x4001);

        // set custom recipient
        vm.prank(validator1);
        distributor.setRewardRecipient(recipient);

        // distribute rewards
        _receiveRewards(300e18);
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // claim - should go to recipient
        uint256 recipientBefore = rls.balanceOf(recipient);
        uint256 validatorBefore = rls.balanceOf(validator1);

        vm.prank(validator1);
        distributor.claimRewards(validator1);

        assertEq(rls.balanceOf(recipient) - recipientBefore, 100e18);
        assertEq(rls.balanceOf(validator1), validatorBefore); // validator balance unchanged
    }

    // =========================================================================
    //                    Split uses initialStake after slash
    // =========================================================================

    function test_distributeRewards_postSlash_splitUsesInitialStake() public {
        // Validator1: initial stake 100e18, slashed to 50e18
        // Delegation pool has 100e18 delegated to validator1
        registry.setInitialStake(validator1, 100e18);
        registry.setBalance(validator1, 50e18); // post-slash

        delegationPool.setDelegatedStake(validator1, 100e18);

        // Set performance weights (uses initial stake = 100e18 * 10 blocks = 1000)
        address[] memory vals = new address[](1);
        vals[0] = validator1;
        uint256[] memory weights = new uint256[](1);
        weights[0] = 1000e18; // only validator
        registry.setPerformanceWeights(vals, weights, 1000e18);

        _receiveRewards(200e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Split uses initialStake (100e18), not outstandingBalance (50e18)
        // validator share = 200e18 * 100e18 / (100e18 + 100e18) = 100e18
        // pool share = 200e18 - 100e18 = 100e18
        uint256 pending = distributor.getPendingRewards(validator1);
        assertEq(pending, 100e18, "validator gets 50% based on initialStake");
        assertEq(delegationPool.distributedRewards(validator1), 100e18, "pool gets 50%");
    }

    // =========================================================================
    //             receiveRewards balance invariant
    // =========================================================================

    function test_receiveRewards_reverts_inflatedAmount() public {
        // Transfer only 50e18 but claim 100e18 — should revert
        vm.startPrank(feeAggregator);
        rls.transfer(address(distributor), 50e18);
        vm.expectRevert(); // InsufficientBalance
        distributor.receiveRewards(100e18);
        vm.stopPrank();
    }

    // =========================================================================
    //              Rounding dust freed from totalPending
    // =========================================================================

    function test_distributeRewards_dustFreedFromTotalPending() public {
        // 3 equal validators, 100e18 rewards → each gets 33e18, dust = 1 wei
        _receiveRewards(100e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // totalPending should be 0 (not 1 wei of dust)
        assertEq(distributor.totalPendingRewards(), 0, "dust freed from totalPending");

        // The 1 wei of dust is in the contract balance, recoverable
        uint256 balance = rls.balanceOf(address(distributor));
        // balance = undistributed validator rewards (pendingRewards) + dust
        uint256 totalClaimed = distributor.getPendingRewards(validator1)
            + distributor.getPendingRewards(validator2)
            + distributor.getPendingRewards(validator3);
        assertGe(balance, totalClaimed, "balance covers all claims");
    }

    // =========================================================================
    //        recoverTokens protects unclaimed validator rewards
    // =========================================================================

    function test_recoverTokens_cannotDrainUnclaimedRewards() public {
        // Distribute 300e18 to 3 validators (100e18 each)
        _receiveRewards(300e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // totalPending is now 0, but 300e18 sits as unclaimed validator rewards
        assertEq(distributor.totalPendingRewards(), 0);

        // Admin tries to recover all RLS — should revert because unclaimed rewards are protected
        uint256 contractBalance = rls.balanceOf(address(distributor));
        vm.prank(owner);
        vm.expectRevert(); // InsufficientBalance
        distributor.recoverTokens(address(rls), owner, contractBalance);

        // Admin can only recover dust (balance - totalPending - totalUnclaimed)
        // With 3 equal validators and 300e18: each gets 100e18, sum = 300e18
        // available = balance - 0 - 300e18 = dust only
        uint256 pending1 = distributor.getPendingRewards(validator1);
        uint256 pending2 = distributor.getPendingRewards(validator2);
        uint256 pending3 = distributor.getPendingRewards(validator3);
        uint256 totalUnclaimed = pending1 + pending2 + pending3;
        uint256 available = contractBalance - totalUnclaimed;

        // Recovering the available dust should succeed
        if (available > 0) {
            vm.prank(owner);
            distributor.recoverTokens(address(rls), owner, available);
        }

        // Validators can still claim their full rewards
        vm.prank(validator1);
        distributor.claimRewards(validator1);
        assertEq(rls.balanceOf(validator1), pending1);

        vm.prank(validator2);
        distributor.claimRewards(validator2);
        assertEq(rls.balanceOf(validator2), pending2);

        vm.prank(validator3);
        distributor.claimRewards(validator3);
        assertEq(rls.balanceOf(validator3), pending3);
    }
}
