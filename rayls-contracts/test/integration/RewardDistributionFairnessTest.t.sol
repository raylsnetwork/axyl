// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {ConsensusRegistry} from "src/consensus/ConsensusRegistry.sol";
import {DelegationPool} from "src/consensus/DelegationPool.sol";
import {RewardDistributor} from "src/fees/RewardDistributor.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {IDelegationPool} from "src/interfaces/IDelegationPool.sol";
import {RewardInfo} from "src/interfaces/IStakeManager.sol";
import {ConsensusRegistryTestUtils} from "../consensus/ConsensusRegistryTestUtils.sol";

/// @notice Integration test wiring the REAL ConsensusRegistry, DelegationPool, and
/// RewardDistributor together (every other test in this repo mocks at least one of the
/// three). It deliberately reproduces the reported bug scenario — a minority of validators
/// winning far fewer leader rounds than their peers, purely a latency/luck artifact, not a
/// stake or misbehavior signal — and proves:
///
///   1. `ConsensusRegistry.applyIncentives` still records the skewed leader counts on-chain
///      (`getEpochPerformanceWeights` reflects them exactly).
///   2. `RewardDistributor.distributeRewards`, wired to that same real registry, produces a
///      split that does NOT correlate with the leader-count skew.
///   3. Every party — each validator's own stake, every Track A delegator, every Track B
///      delegator — receives exactly the amount its stake and the configured tier APY
///      entitles it to, regardless of which validator (fast-majority or slow-minority) it
///      picked.
contract RewardDistributionFairnessTest is ConsensusRegistryTestUtils {
    DelegationPool public pool;
    RewardDistributor public distributor;

    address public admin = address(0xad921);
    address public feeAggregator = address(0xfeeA99);

    // Track A (whitelisted) delegators, one per genesis validator
    address public delA1 = address(0xA1);
    address public delA2 = address(0xA2);
    address public delA3 = address(0xA3);
    address public delA4 = address(0xA4);

    // Track B (open-tier) delegators, one per genesis validator
    address public delB1 = address(0xB1);
    address public delB2 = address(0xB2);
    address public delB3 = address(0xB3);
    address public delB4 = address(0xB4);

    uint256 public constant TARGET_APY_BPS = 5500; // 55%, Track A
    uint256 public constant OPEN_TIER_APY_BPS = 1800; // 18%, Track B
    uint256 public constant BPS = 10_000;

    function setUp() public {
        // Use this test contract itself as the ConsensusRegistry — its constructor (inherited
        // via ConsensusRegistryTestUtils) already ran the real genesis flow: validator1-4 are
        // Active, allowlisted, and staked at stakeAmount_ (1_000_000e18) each.
        consensusRegistry = ConsensusRegistry(address(this));
        sysAddress = consensusRegistry.SYSTEM_ADDRESS();

        pool = new DelegationPool();
        ERC1967Proxy poolProxy = new ERC1967Proxy(
            address(pool),
            abi.encodeCall(
                DelegationPool.initialize,
                (
                    address(mockRLS),
                    address(consensusRegistry),
                    admin,
                    IDelegationPool.DelegationConfig({
                        minDelegation: 1e18,
                        maxDelegation: 10_000_000e18,
                        maxValidatorDelegation: 10_000_000e18,
                        unbondingEpochs: 1,
                        commissionDelayEpochs: 1
                    })
                )
            )
        );
        pool = DelegationPool(address(poolProxy));

        RewardDistributor distributorImpl = new RewardDistributor();
        ERC1967Proxy distributorProxy = new ERC1967Proxy(
            address(distributorImpl),
            abi.encodeCall(
                RewardDistributor.initialize,
                (address(mockRLS), feeAggregator, address(consensusRegistry), address(pool), admin)
            )
        );
        distributor = RewardDistributor(address(distributorProxy));

        vm.startPrank(admin);
        distributor.setTargetApyBps(TARGET_APY_BPS);
        distributor.setOpenTierTargetApyBps(OPEN_TIER_APY_BPS);
        vm.stopPrank();

        vm.prank(crOwner);
        consensusRegistry.setDelegationPool(address(pool));

        vm.prank(admin);
        pool.setRewardDistributor(address(distributor));

        // Register all 4 genesis validators as delegation pools, with distinct commissions
        // (mirrors the real Ohio/Oregon/London/Tokyo mix from the incident report).
        vm.prank(validator1);
        pool.registerPool(1000); // 10%
        vm.prank(validator2);
        pool.registerPool(0); // 0%
        vm.prank(validator3);
        pool.registerPool(2000); // 20%
        vm.prank(validator4);
        pool.registerPool(500); // 5%

        // Whitelist gate stays disabled until _enableOpenTierGate() is called (after Track A
        // delegations, before Track B) — while disabled, plain delegate() always routes to
        // Track A; once enabled, unverified delegators route to Track B.
    }

    /// @dev Track A (priority) delegation via plain delegate() while the open-tier gate is
    /// disabled — isOpenTier is false unconditionally in that state.
    function _delegateTrackA(address validator, address delegator, uint256 amount) internal {
        mockRLS.mint(delegator, amount);
        vm.startPrank(delegator);
        mockRLS.approve(address(pool), amount);
        pool.delegate(validator, amount);
        vm.stopPrank();
    }

    /// @dev Track B (open-tier) delegation: plain delegate() from a delegator who never
    /// submitted a proof, with the whitelist gate enabled (call _enableOpenTierGate() first).
    function _delegateTrackB(address validator, address delegator, uint256 amount) internal {
        mockRLS.mint(delegator, amount);
        vm.startPrank(delegator);
        mockRLS.approve(address(pool), amount);
        pool.delegate(validator, amount);
        vm.stopPrank();
    }

    /// @dev Enables the whitelist gate so subsequent delegate() calls from never-verified
    /// delegators route to Track B. Must be called AFTER any Track A delegations that use plain
    /// delegate(), and BEFORE any Track B delegations.
    function _enableOpenTierGate() internal {
        vm.prank(admin);
        pool.setWhitelistRoot(bytes32(uint256(1)));
    }

    /// @dev Advances exactly one epoch, keeping the same 4 validators as the committee.
    function _advanceOneEpoch() internal {
        address[] memory committee = new address[](4);
        committee[0] = validator1;
        committee[1] = validator2;
        committee[2] = validator3;
        committee[3] = validator4;
        _sortAddresses(committee);
        vm.prank(sysAddress);
        consensusRegistry.concludeEpoch(committee);
    }

    /// @dev Mirrors RewardDistributor._splitTarget's exact formula so expected values are
    /// derived the same way the contract computes them (same integer division order).
    function _expectedPriorityTarget(uint256 ownStake, uint256 trackA, uint256 epochSecs) internal pure returns (uint256) {
        return ((ownStake + trackA) * TARGET_APY_BPS * epochSecs) / (365 days * BPS);
    }

    function _expectedTrackBTarget(uint256 trackB, uint256 epochSecs) internal pure returns (uint256) {
        return (trackB * OPEN_TIER_APY_BPS * epochSecs) / (365 days * BPS);
    }

    // =========================================================================
    //  Deterministic scenario: 3 fast validators + 1 slow minority validator
    // =========================================================================

    /// @notice Reproduces the reported incident directly: 4 validators with IDENTICAL stake
    /// composition (so any reward difference between them can only come from the reward
    /// formula, not from legitimately differing stake), but wildly different leader-round
    /// counts — 3 validators winning 900-1200 rounds this epoch, 1 validator (the "slow
    /// minority") winning only 50. Asserts all three required properties.
    function test_slowMinorityValidator_doesNotSkewRewards() public {
        uint256 trackAAmount = 50_000e18;
        uint256 trackBAmount = 20_000e18;

        _delegateTrackA(validator1, delA1, trackAAmount);
        _delegateTrackA(validator2, delA2, trackAAmount);
        _delegateTrackA(validator3, delA3, trackAAmount);
        _delegateTrackA(validator4, delA4, trackAAmount);

        _enableOpenTierGate();
        _delegateTrackB(validator1, delB1, trackBAmount);
        _delegateTrackB(validator2, delB2, trackBAmount);
        _delegateTrackB(validator3, delB3, trackBAmount);
        _delegateTrackB(validator4, delB4, trackBAmount);

        // Move past the delegation epoch so reward accrual isn't sandwich-guarded.
        _advanceOneEpoch();

        // Fund distributor with EXACTLY the combined target so fundingRatio == 1 and every
        // validator gets exactly its own computed target — makes expected values exact, not
        // approximate.
        _fundExactTarget(trackAAmount, trackBAmount);

        // --- Property 1: leader counts (consensusHeaderCount) are recorded on-chain, wildly
        // skewed — validator4 ("slow minority") wins 50 rounds vs. 900-1200 for the others.
        _applySkewedIncentivesAndAssertStored();

        // --- Property 2 & 3: distribute, then verify every party gets the fair amount,
        // completely independent of the leader-count skew just recorded.
        vm.prank(sysAddress);
        distributor.distributeRewards();

        _assertOwnStakeFairness();
        _assertTrackAFairness(trackAAmount);
        _assertTrackBFairness(trackBAmount);
    }

    function _fundExactTarget(uint256 trackAAmount, uint256 trackBAmount) internal {
        uint256 epochSecs = consensusRegistry.getCurrentEpochInfo().epochDuration;
        uint256 priorityTargetEach = _expectedPriorityTarget(stakeAmount_, trackAAmount, epochSecs);
        uint256 trackBTargetEach = _expectedTrackBTarget(trackBAmount, epochSecs);
        uint256 totalTarget = 4 * (priorityTargetEach + trackBTargetEach);

        mockRLS.mint(feeAggregator, totalTarget);
        vm.startPrank(feeAggregator);
        mockRLS.transfer(address(distributor), totalTarget);
        distributor.receiveRewards(totalTarget);
        vm.stopPrank();
    }

    function _applySkewedIncentivesAndAssertStored() internal {
        RewardInfo[] memory rewardInfos = new RewardInfo[](4);
        rewardInfos[0] = RewardInfo(validator1, 1000);
        rewardInfos[1] = RewardInfo(validator2, 1200);
        rewardInfos[2] = RewardInfo(validator3, 900);
        rewardInfos[3] = RewardInfo(validator4, 50); // ~20x fewer than validator2

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos);

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        assertEq(perf.validators.length, 4, "all 4 validators recorded");
        for (uint256 i; i < perf.validators.length; ++i) {
            uint256 expectedHeaderCount = perf.validators[i] == validator1 ? 1000
                : perf.validators[i] == validator2 ? 1200
                : perf.validators[i] == validator3 ? 900
                : 50;
            assertEq(perf.weights[i], stakeAmount_ * expectedHeaderCount, "weight == stake * headerCount, stored on-chain");
        }
        // The skew is real and large — confirms this isn't a trivial/near-equal case.
        assertTrue(perf.weights[0] >= 18 * perf.weights[3], "deliberately large skew: fast >> slow");
    }

    /// @dev Identical ownStake + identical Track A stake + identical target APY across all 4
    /// validators => identical validatorShare, despite the 20x headerCount gap just recorded.
    function _assertOwnStakeFairness() internal {
        uint256 v1 = distributor.getPendingRewards(validator1);
        uint256 v2 = distributor.getPendingRewards(validator2);
        uint256 v3 = distributor.getPendingRewards(validator3);
        uint256 v4 = distributor.getPendingRewards(validator4);
        assertApproxEqAbs(v1, v2, 10, "validator own-stake reward independent of leader count (1v2)");
        assertApproxEqAbs(v1, v3, 10, "validator own-stake reward independent of leader count (1v3)");
        assertApproxEqAbs(v1, v4, 10, "slow-minority validator gets the SAME own-stake reward as fast majority");
    }

    /// @dev Identical Track A stake, identical target rate, only commission differs (10/0/20/5%)
    /// — the pre-commission pool share must be identical across all 4, and match the formula.
    function _assertTrackAFairness(uint256 trackAAmount) internal {
        uint256 a1Gross = (pool.getPendingRewards(validator1, delA1) * BPS) / (BPS - 1000);
        uint256 a2Gross = (pool.getPendingRewards(validator2, delA2) * BPS) / (BPS - 0);
        uint256 a3Gross = (pool.getPendingRewards(validator3, delA3) * BPS) / (BPS - 2000);
        uint256 a4Gross = (pool.getPendingRewards(validator4, delA4) * BPS) / (BPS - 500);
        assertApproxEqAbs(a1Gross, a2Gross, 1e6, "Track A pre-commission rate equal (1v2)");
        assertApproxEqAbs(a1Gross, a3Gross, 1e6, "Track A pre-commission rate equal (1v3)");
        assertApproxEqAbs(a1Gross, a4Gross, 1e6, "slow-minority validator's Track A delegator gets the SAME pre-commission rate");

        uint256 epochSecs = consensusRegistry.getCurrentEpochInfo().epochDuration;
        uint256 priorityTargetEach = _expectedPriorityTarget(stakeAmount_, trackAAmount, epochSecs);
        uint256 expectedValidatorShare = (priorityTargetEach * stakeAmount_) / (stakeAmount_ + trackAAmount);
        uint256 expectedTrackAShareGross = priorityTargetEach - expectedValidatorShare;
        assertApproxEqAbs(distributor.getPendingRewards(validator1), expectedValidatorShare, 10, "validator reward matches target-APY formula exactly");
        assertApproxEqAbs(a1Gross, expectedTrackAShareGross, 1e6, "Track A gross reward matches target-APY formula exactly");
    }

    /// @dev Same check as Track A, using each validator's own commission on the open-tier rate.
    function _assertTrackBFairness(uint256 trackBAmount) internal {
        uint256 b1Gross = (pool.getPendingRewards(validator1, delB1) * BPS) / (BPS - 1000);
        uint256 b2Gross = (pool.getPendingRewards(validator2, delB2) * BPS) / (BPS - 0);
        uint256 b3Gross = (pool.getPendingRewards(validator3, delB3) * BPS) / (BPS - 2000);
        uint256 b4Gross = (pool.getPendingRewards(validator4, delB4) * BPS) / (BPS - 500);
        assertApproxEqAbs(b1Gross, b2Gross, 1e6, "Track B pre-commission rate equal (1v2)");
        assertApproxEqAbs(b1Gross, b3Gross, 1e6, "Track B pre-commission rate equal (1v3)");
        assertApproxEqAbs(b1Gross, b4Gross, 1e6, "slow-minority validator's Track B delegator gets the SAME pre-commission rate");

        uint256 epochSecs = consensusRegistry.getCurrentEpochInfo().epochDuration;
        uint256 trackBTargetEach = _expectedTrackBTarget(trackBAmount, epochSecs);
        assertApproxEqAbs(b1Gross, trackBTargetEach, 1e6, "Track B gross reward matches open-tier target-APY formula exactly");
    }

    // =========================================================================
    //  Fuzz: excessive stress test across random skew, commission, and stake
    // =========================================================================

    /// @notice Generalizes the deterministic scenario above: random (but bounded) Track A/B
    /// delegation per validator, random commissions, and a DELIBERATELY forced large skew
    /// between the header counts of a random "slow" validator and the rest — across many runs.
    /// Same three properties are asserted, using ratio invariants since stake now varies.
    function testFuzz_headerCountSkew_neverAffectsRewardFairness(
        uint256[4] memory trackA,
        uint256[4] memory trackB,
        uint256[4] memory commissions,
        uint256[4] memory headerCounts
    ) public {
        for (uint256 i; i < 4; ++i) {
            trackA[i] = bound(trackA[i], 1e18, 200_000e18);
            trackB[i] = bound(trackB[i], 1e18, 200_000e18);
            commissions[i] = bound(commissions[i], 0, 2000);
        }
        // Validators 0-2: fast majority (500-2000 leader rounds). Validator 3: slow minority
        // (1-50) — deliberately forced far below the fast range every single run.
        headerCounts[0] = bound(headerCounts[0], 500, 2000);
        headerCounts[1] = bound(headerCounts[1], 500, 2000);
        headerCounts[2] = bound(headerCounts[2], 500, 2000);
        headerCounts[3] = bound(headerCounts[3], 1, 50);

        address[4] memory validators = [validator1, validator2, validator3, validator4];

        _fuzzRedeployPool(validators, commissions);
        _fuzzDelegateAll(validators, trackA, trackB);
        _advanceOneEpoch();
        _fuzzFund(trackA, trackB);

        RewardInfo[] memory rewardInfos = new RewardInfo[](4);
        for (uint256 i; i < 4; ++i) {
            rewardInfos[i] = RewardInfo(validators[i], headerCounts[i]);
        }
        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos);

        // Property 1: on-chain storage still reflects the real (skewed) leader counts.
        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        uint256 totalHeaders = headerCounts[0] + headerCounts[1] + headerCounts[2] + headerCounts[3];
        assertEq(perf.totalWeight, stakeAmount_ * totalHeaders, "skewed leader counts stored on-chain");

        vm.prank(sysAddress);
        distributor.distributeRewards();

        _fuzzAssertFairness(validators, trackA, trackB, commissions);
    }

    /// @dev Redeploys a fresh DelegationPool wired to the shared registry/distributor and
    /// registers all 4 validators with fuzzed commissions (fresh deploy needed since commission
    /// can only increase by up to +500bps/update from setUp's fixed values).
    function _fuzzRedeployPool(address[4] memory validators, uint256[4] memory commissions) internal {
        pool = new DelegationPool();
        ERC1967Proxy poolProxy = new ERC1967Proxy(
            address(pool),
            abi.encodeCall(
                DelegationPool.initialize,
                (
                    address(mockRLS),
                    address(consensusRegistry),
                    admin,
                    IDelegationPool.DelegationConfig({
                        minDelegation: 1e18,
                        maxDelegation: 10_000_000e18,
                        maxValidatorDelegation: 10_000_000e18,
                        unbondingEpochs: 1,
                        commissionDelayEpochs: 1
                    })
                )
            )
        );
        pool = DelegationPool(address(poolProxy));
        vm.prank(crOwner);
        consensusRegistry.setDelegationPool(address(pool));
        vm.prank(admin);
        pool.setRewardDistributor(address(distributor));

        for (uint256 i; i < 4; ++i) {
            vm.prank(validators[i]);
            pool.registerPool(commissions[i]);
        }
    }

    function _fuzzDelegateAll(
        address[4] memory validators,
        uint256[4] memory trackA,
        uint256[4] memory trackB
    ) internal {
        address[4] memory delAs = [delA1, delA2, delA3, delA4];
        for (uint256 i; i < 4; ++i) {
            _delegateTrackA(validators[i], delAs[i], trackA[i]);
        }

        _enableOpenTierGate();

        address[4] memory delBs = [delB1, delB2, delB3, delB4];
        for (uint256 i; i < 4; ++i) {
            _delegateTrackB(validators[i], delBs[i], trackB[i]);
        }
    }

    function _fuzzFund(uint256[4] memory trackA, uint256[4] memory trackB) internal {
        uint256 epochSecs = consensusRegistry.getCurrentEpochInfo().epochDuration;
        uint256 totalTarget;
        for (uint256 i; i < 4; ++i) {
            totalTarget += _expectedPriorityTarget(stakeAmount_, trackA[i], epochSecs);
            totalTarget += _expectedTrackBTarget(trackB[i], epochSecs);
        }

        mockRLS.mint(feeAggregator, totalTarget);
        vm.startPrank(feeAggregator);
        mockRLS.transfer(address(distributor), totalTarget);
        distributor.receiveRewards(totalTarget);
        vm.stopPrank();
    }

    /// @dev ownStake is identical (stakeAmount_) across all 4 genesis validators, so
    /// validatorShare_i = totalRewards * RATE * ownStake / totalTarget is IDENTICAL across all 4
    /// in exact real-number terms, regardless of each validator's own Track A/B size, commission,
    /// or (critically) leader-count skew. Checked between validator index 0 (fast) and 3 (the
    /// deliberately-forced slow minority, as few as 1 leader round against up to 2000 for peers).
    function _fuzzAssertFairness(
        address[4] memory validators,
        uint256[4] memory trackA,
        uint256[4] memory trackB,
        uint256[4] memory commissions
    ) internal {
        uint256 v0 = distributor.getPendingRewards(validators[0]);
        uint256 v3 = distributor.getPendingRewards(validators[3]);
        assertApproxEqAbs(v0, v3, 1000, "own-stake reward equal regardless of leader-count skew");

        address[4] memory delAs = [delA1, delA2, delA3, delA4];
        address[4] memory delBs = [delB1, delB2, delB3, delB4];

        // Undo each validator's own commission, then the pre-commission per-token rate must
        // match across validators 0 and 3 even though their stake and headerCount both differ.
        // Cross-multiply instead of dividing by trackA[i] directly, to avoid a second rounding
        // step: a0Gross/trackA[0] == a3Gross/trackA[3]  <=>  a0Gross*trackA[3] == a3Gross*trackA[0]
        uint256 a0Gross = (pool.getPendingRewards(validators[0], delAs[0]) * BPS) / (BPS - commissions[0]);
        uint256 a3Gross = (pool.getPendingRewards(validators[3], delAs[3]) * BPS) / (BPS - commissions[3]);
        assertApproxEqRel(a0Gross * trackA[3], a3Gross * trackA[0], 1e12, "Track A rate ratio equal regardless of leader-count skew");

        uint256 b0Gross = (pool.getPendingRewards(validators[0], delBs[0]) * BPS) / (BPS - commissions[0]);
        uint256 b3Gross = (pool.getPendingRewards(validators[3], delBs[3]) * BPS) / (BPS - commissions[3]);
        assertApproxEqRel(b0Gross * trackB[3], b3Gross * trackB[0], 1e12, "Track B rate ratio equal regardless of leader-count skew");
    }
}
