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
contract MockRLSExt is ERC20 {
    constructor() ERC20("Mock RLS", "RLS") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @notice Mock ConsensusRegistry for testing
contract MockConsensusRegistryExt {
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
        initialStakes[validator] = balance;
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
        uint256 initial = initialStakes[validator];
        if (initial == 0) initial = balances[validator];
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
contract MockDelegationPoolExt {
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

contract RewardDistributorExtendedTest is Test {
    RewardDistributor public distributor;
    MockRLSExt public rls;
    MockConsensusRegistryExt public registry;
    MockDelegationPoolExt public delegationPool;

    address public owner = address(0xABCD);
    address public feeAggregator = address(0x1001);
    address constant SYSTEM_ADDRESS = address(0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE);
    address public validator1 = address(0x2001);
    address public validator2 = address(0x2002);
    address public validator3 = address(0x2003);
    address public user = address(0x3001);

    function setUp() public {
        rls = new MockRLSExt();
        registry = new MockConsensusRegistryExt();
        delegationPool = new MockDelegationPoolExt();
        delegationPool.setRlsToken(address(rls));

        RewardDistributor impl = new RewardDistributor();
        bytes memory initData = abi.encodeCall(
            RewardDistributor.initialize,
            (address(rls), feeAggregator, address(registry), address(delegationPool), owner)
        );
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        distributor = RewardDistributor(address(proxy));

        // fund fee distributor with RLS
        rls.mint(feeAggregator, 10_000_000e18);
    }

    // Helper: transfer RLS to distributor and call receiveRewards
    function _receiveRewards(uint256 amount) internal {
        vm.startPrank(feeAggregator);
        rls.transfer(address(distributor), amount);
        distributor.receiveRewards(amount);
        vm.stopPrank();
    }

    // Helper: deploy and configure an RLSAccumulator
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

        // Set epoch duration (1 day = 86400 seconds)
        registry.setEpochDuration(86400);
    }

    // =========================================================================
    //  1. Performance-weighted: 2x blocks -> 2x rewards (equal stake)
    // =========================================================================

    function test_distributeRewards_performanceWeighted_2xBlocks() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 100e18);

        // validator1 produces 10 blocks, validator2 produces 20 blocks
        // weight = stake * headerCount => v1: 100e18*10 = 1000e18, v2: 100e18*20 = 2000e18
        address[] memory validators = new address[](2);
        uint256[] memory weights = new uint256[](2);
        validators[0] = validator1;
        validators[1] = validator2;
        weights[0] = 100e18 * 10; // 1000e18
        weights[1] = 100e18 * 20; // 2000e18
        uint256 totalWeight = weights[0] + weights[1]; // 3000e18

        registry.setPerformanceWeights(validators, weights, totalWeight);

        _receiveRewards(3000e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // v1 gets 1000/3000 * 3000 = 1000, v2 gets 2000/3000 * 3000 = 2000
        assertEq(distributor.getPendingRewards(validator1), 1000e18);
        assertEq(distributor.getPendingRewards(validator2), 2000e18);
    }

    // =========================================================================
    //  2. Performance-weighted: 2x stake -> 2x weight (equal blocks)
    // =========================================================================

    function test_distributeRewards_performanceWeighted_2xStake() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 200e18);

        // Both produce 10 blocks each
        // weight = stake * headerCount => v1: 100e18*10 = 1000e18, v2: 200e18*10 = 2000e18
        address[] memory validators = new address[](2);
        uint256[] memory weights = new uint256[](2);
        validators[0] = validator1;
        validators[1] = validator2;
        weights[0] = 100e18 * 10; // 1000e18
        weights[1] = 200e18 * 10; // 2000e18
        uint256 totalWeight = weights[0] + weights[1]; // 3000e18

        registry.setPerformanceWeights(validators, weights, totalWeight);

        _receiveRewards(3000e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // v1: 1000/3000 * 3000 = 1000, v2: 2000/3000 * 3000 = 2000
        assertEq(distributor.getPendingRewards(validator1), 1000e18);
        assertEq(distributor.getPendingRewards(validator2), 2000e18);
    }

    // =========================================================================
    //  3. Multiple epochs: distribute rewards across 3 epochs
    // =========================================================================

    function test_distributeRewards_multipleEpochs() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);
        registry.addActiveValidator(validator2, 100e18);

        // --- Epoch 1 ---
        _receiveRewards(200e18);
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        assertEq(distributor.getPendingRewards(validator1), 100e18);
        assertEq(distributor.getPendingRewards(validator2), 100e18);
        assertEq(distributor.totalPendingRewards(), 0);

        // --- Epoch 2 ---
        _receiveRewards(400e18);
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // cumulative: v1 = 100 + 200 = 300, v2 = 100 + 200 = 300
        assertEq(distributor.getPendingRewards(validator1), 300e18);
        assertEq(distributor.getPendingRewards(validator2), 300e18);
        assertEq(distributor.totalPendingRewards(), 0);

        // --- Epoch 3 ---
        _receiveRewards(600e18);
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // cumulative: v1 = 300 + 300 = 600, v2 = 300 + 300 = 600
        assertEq(distributor.getPendingRewards(validator1), 600e18);
        assertEq(distributor.getPendingRewards(validator2), 600e18);
        assertEq(distributor.totalPendingRewards(), 0);
    }

    // =========================================================================
    //  4. Large validator set: single-call distribution with 12 validators
    // =========================================================================

    function test_distributeRewards_largeValidatorSet() public {
        registry.clearValidators();

        address[] memory vals = new address[](12);
        for (uint256 i; i < 12; ++i) {
            vals[i] = address(uint160(0x5000 + i));
            registry.addActiveValidator(vals[i], 100e18);
        }

        _receiveRewards(1200e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        for (uint256 i; i < 12; ++i) {
            assertEq(distributor.getPendingRewards(vals[i]), 100e18, "validator reward mismatch");
        }

        assertEq(distributor.totalPendingRewards(), 0);
    }

    // =========================================================================
    //  5. Delegations: split between validator and delegation pool
    // =========================================================================

    function test_distributeRewards_withDelegations_splitCorrectly() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);

        // validator1 has 100e18 own stake + 400e18 delegated = 500e18 total
        delegationPool.setDelegatedStake(validator1, 400e18);

        // Fund delegation pool so it can receive RLS transfers
        // (the distributor transfers RLS to the delegation pool contract)

        _receiveRewards(500e18);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Only one validator, so it gets all 500e18
        // Split: validator share = 500 * (100/500) = 100e18
        //        pool share     = 500 * (400/500) = 400e18  (= 500 - 100)
        assertEq(distributor.getPendingRewards(validator1), 100e18);
        assertEq(delegationPool.distributedRewards(validator1), 400e18);
    }

    // =========================================================================
    //  6. Zero pending rewards: no revert
    // =========================================================================

    function test_distributeRewards_zeroPendingRewards_noRevert() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 100e18);

        // Do not receive any rewards — totalPending is 0
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Should complete without reverting
        assertEq(distributor.getPendingRewards(validator1), 0);
        assertEq(distributor.totalPendingRewards(), 0);
    }

    // =========================================================================
    //  7. Accumulator top-up: exact target APY formula verification
    // =========================================================================

    function test_accumulatorTopUp_exactTargetAPYFormula() public {
        registry.clearValidators();
        registry.addActiveValidator(validator1, 500e18);
        registry.addActiveValidator(validator2, 500e18);

        RLSAccumulator acc = _setupAccumulator(1_000_000e18);
        uint256 accBalanceBefore = rls.balanceOf(address(acc));

        // No fee rewards — entire target should be pulled from accumulator
        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Formula: targetReward = (totalStaked * apyBps * epochSecs) / (365 days * 10_000)
        uint256 totalStaked = 1000e18; // 500e18 + 500e18
        uint256 apyBps = 5000;
        uint256 epochSecs = 86400;
        uint256 expectedTarget = (totalStaked * apyBps * epochSecs) / (365 days * 10_000);

        // shortfall = target - 0 (no fee rewards) = target
        // pulled amount should equal the shortfall = expectedTarget
        uint256 pulled = accBalanceBefore - rls.balanceOf(address(acc));
        assertEq(pulled, expectedTarget, "accumulator pull should match exact formula");

        // Total distributed to validators should equal the pulled amount
        uint256 totalDistributed = distributor.getPendingRewards(validator1)
            + distributor.getPendingRewards(validator2);
        assertEq(totalDistributed, expectedTarget, "distributed should match target");

        // Equal stake => equal share
        assertEq(
            distributor.getPendingRewards(validator1),
            distributor.getPendingRewards(validator2),
            "equal stake should yield equal rewards"
        );
    }

    // =========================================================================
    //  8. Fuzz test: sum(distributed) <= totalRewards (no overflow)
    // =========================================================================

    function testFuzz_distributeRewards_noRemainder(uint8 validatorCount, uint128 rewardAmount) public {
        // Bound inputs to reasonable ranges
        uint256 count = bound(uint256(validatorCount), 1, 20);
        uint256 rewards = bound(uint256(rewardAmount), 1e18, 10_000_000e18);

        registry.clearValidators();

        address[] memory vals = new address[](count);
        for (uint256 i; i < count; ++i) {
            vals[i] = address(uint160(0x7000 + i));
            // Give each validator a different stake to exercise rounding
            uint256 stake = (i + 1) * 50e18;
            registry.addActiveValidator(vals[i], stake);
        }

        // Mint extra RLS for the feeAggregator if needed
        uint256 feeBalance = rls.balanceOf(feeAggregator);
        if (feeBalance < rewards) {
            rls.mint(feeAggregator, rewards - feeBalance);
        }

        _receiveRewards(rewards);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        // Sum up all distributed rewards
        uint256 totalDistributed;
        for (uint256 i; i < count; ++i) {
            totalDistributed += distributor.getPendingRewards(vals[i]);
        }

        // Distributed must not exceed total rewards (rounding dust is acceptable)
        assertLe(totalDistributed, rewards, "distributed exceeds total rewards");

        // The rounding dust should be small (at most validatorCount wei per token unit)
        uint256 dust = rewards - totalDistributed;
        assertLe(dust, count, "rounding dust too large");
    }

    /// @notice distributeRewards must pay each validator in PROPORTION to its weight
    ///         (= own stake here, since delegated == 0). Complements
    ///         testFuzz_distributeRewards_noRemainder, which checks only conservation/dust.
    ///         Asserted against the observed total, so any accumulator top-up is irrelevant —
    ///         the split itself must stay proportional.
    function testFuzz_distributeRewards_proportionalToStake(
        uint256 rewardAmount,
        uint256 s1,
        uint256 s2,
        uint256 s3
    ) public {
        s1 = bound(s1, 1e21, 1_000_000e18);
        s2 = bound(s2, 1e21, 1_000_000e18);
        s3 = bound(s3, 1e21, 1_000_000e18);
        uint256 rewards = bound(rewardAmount, 1e18, 10_000_000e18);

        registry.clearValidators();
        address v1 = address(0x7101);
        address v2 = address(0x7102);
        address v3 = address(0x7103);
        registry.addActiveValidator(v1, s1);
        registry.addActiveValidator(v2, s2);
        registry.addActiveValidator(v3, s3);

        uint256 feeBalance = rls.balanceOf(feeAggregator);
        if (feeBalance < rewards) {
            rls.mint(feeAggregator, rewards - feeBalance);
        }
        _receiveRewards(rewards);

        vm.prank(SYSTEM_ADDRESS);
        distributor.distributeRewards();

        uint256 p1 = distributor.getPendingRewards(v1);
        uint256 p2 = distributor.getPendingRewards(v2);
        uint256 p3 = distributor.getPendingRewards(v3);
        uint256 total = p1 + p2 + p3;
        assertGt(total, 0, "a distribution must have occurred");

        uint256 totalStake = s1 + s2 + s3;
        assertApproxEqRel(p1, (total * s1) / totalStake, 1e13, "v1 reward proportional to stake");
        assertApproxEqRel(p2, (total * s2) / totalStake, 1e13, "v2 reward proportional to stake");
        assertApproxEqRel(p3, (total * s3) / totalStake, 1e13, "v3 reward proportional to stake");
    }
}
