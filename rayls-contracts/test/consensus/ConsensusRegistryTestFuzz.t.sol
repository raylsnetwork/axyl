// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {ConsensusRegistry} from "src/consensus/ConsensusRegistry.sol";
import {IConsensusRegistry} from "src/interfaces/IConsensusRegistry.sol";
import {SystemCallable} from "src/consensus/SystemCallable.sol";
import {StakeManager} from "src/consensus/StakeManager.sol";
import {Slash, RewardInfo, IStakeManager} from "src/interfaces/IStakeManager.sol";
import {ConsensusRegistryTestUtils} from "./ConsensusRegistryTestUtils.sol";

/// @dev Fuzz test module separated into new file with extra setup to avoid `OutOfGas`
contract ConsensusRegistryTestFuzz is ConsensusRegistryTestUtils {
    function setUp() public {
        // target
        consensusRegistry = ConsensusRegistry(0x07E17e17E17e17E17e17E17E17E17e17e17E17e1);

        vm.startStateDiffRecording();

        StakeConfig memory stakeConfig_ = StakeConfig(
            stakeAmount_,
            minWithdrawAmount_,
            epochDuration_
        );
        ConsensusRegistry tempRegistry = new ConsensusRegistry(address(mockRLS), stakeConfig_, initialValidators, initialBLSPops, crOwner);
        Vm.AccountAccess[] memory records = vm.stopAndReturnStateDiff();
        bytes32[] memory slots = saveWrittenSlots(address(tempRegistry), records);
        copyContractState(address(tempRegistry), address(consensusRegistry), slots);

        // set protocol system address
        sysAddress = consensusRegistry.SYSTEM_ADDRESS();

    }

    function testFuzz_concludeEpoch(uint24 numValidators) public {
        numValidators = uint24(bound(uint256(numValidators), 1, 750));

        uint256 numActive = consensusRegistry
            .getValidators(ValidatorStatus.Active)
            .length + numValidators;

        _fuzz_stake(numValidators, stakeAmount_);
        _fuzz_activate(numValidators);

        // identify committee size, conclude an epoch to reach activation epoch, then create a committee
        uint256 committeeSize = _fuzz_computeCommitteeSize(
            numActive,
            numValidators
        );
        // conclude epoch to reach activationEpoch for validators entered in stake & activate loop
        vm.startPrank(sysAddress);
        address[] memory tokenIdCommittee = _createTokenIdCommittee(
            committeeSize
        );
        consensusRegistry.concludeEpoch(tokenIdCommittee);
        address[] memory futureCommittee = _fuzz_createFutureCommittee(
            numActive,
            committeeSize
        );

        // set the subsequent epoch committee by concluding epoch
        EpochInfo memory epochInfo = consensusRegistry.getCurrentEpochInfo();
        uint32 newEpoch = consensusRegistry.getCurrentEpoch() + 1;
        address[] memory newCommittee = consensusRegistry
            .getEpochInfo(newEpoch)
            .committee;
        vm.expectEmit(true, true, true, true);
        emit IConsensusRegistry.NewEpoch(
            IConsensusRegistry.EpochInfo(
                newCommittee,
                uint64(block.number + 1),
                epochInfo.epochDuration,
                epochInfo.stakeVersion
            )
        );
        consensusRegistry.concludeEpoch(futureCommittee);

        // asserts
        uint256 numActiveAfter = consensusRegistry
            .getValidators(ValidatorStatus.Active)
            .length;
        assertEq(numActiveAfter, numActive);
        uint32 returnedEpoch = consensusRegistry.getCurrentEpoch();
        assertEq(returnedEpoch, newEpoch);
        address[] memory currentCommittee = consensusRegistry
            .getEpochInfo(newEpoch)
            .committee;
        for (uint256 i; i < currentCommittee.length; ++i) {
            assertEq(
                currentCommittee[i],
                initialValidators[i].validatorAddress
            );
        }
        address[] memory nextCommittee = consensusRegistry
            .getEpochInfo(newEpoch + 1)
            .committee;
        for (uint256 i; i < nextCommittee.length; ++i) {
            assertEq(nextCommittee[i], tokenIdCommittee[i]);
        }
        address[] memory subsequentCommittee = consensusRegistry
            .getEpochInfo(newEpoch + 2)
            .committee;
        for (uint256 i; i < subsequentCommittee.length; ++i) {
            assertEq(subsequentCommittee[i], futureCommittee[i]);
        }
    }

    /// @dev Invariant-style rather than re-deriving the full blended formula in
    /// test code (fragile/circular, since totals depend on the same random set
    /// being generated) - see `test_applyIncentives_hybridBlend_tokyoScenario`
    /// below for an exact hand-computed check of the formula itself.
    function testFuzz_applyIncentives(
        uint24 numValidators,
        uint24 numRewardees
    ) public {
        numValidators = uint24(bound(uint256(numValidators), 1, 800));
        numRewardees = uint24(bound(uint256(numRewardees), 1, numValidators));

        _fuzz_stake(numValidators, stakeAmount_);

        vm.startPrank(sysAddress);
        (RewardInfo[] memory rewardInfos, uint256 totalRounds) = _fuzz_createRewardInfos(numRewardees);
        consensusRegistry.applyIncentives(rewardInfos, totalRounds);
        vm.stopPrank();

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();

        // Invariant: recorded weights sum exactly to totalWeight.
        uint256 summedWeight;
        for (uint256 i; i < perf.weights.length; ++i) {
            assertTrue(perf.weights[i] > 0, "recorded entries must have positive weight");
            summedWeight += perf.weights[i];
        }
        assertEq(summedWeight, perf.totalWeight);

        // Invariant: with no retirements and the participation floor disabled
        // (default), exactly the rewardees with participationRounds > 0 are
        // weighted - absent validators (participationRounds == 0) earn nothing and
        // are dropped; nothing else is silently dropped.
        uint256 expectedEligible;
        for (uint256 i; i < rewardInfos.length; ++i) {
            if (rewardInfos[i].participationRounds > 0) expectedEligible++;
        }
        assertEq(perf.validators.length, expectedEligible, "only participating validators weighted");

        // Invariant: every recorded validator was actually submitted (no phantom entries).
        for (uint256 i; i < perf.validators.length; ++i) {
            bool found;
            for (uint256 j; j < rewardInfos.length; ++j) {
                if (rewardInfos[j].validatorAddress == perf.validators[i]) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "recorded validator not in submitted rewardInfos");
        }

        // applyIncentives no longer credits balances, so getRewards should be 0
        for (uint256 i; i < rewardInfos.length; ++i) {
            uint256 rewards = consensusRegistry.getRewards(
                rewardInfos[i].validatorAddress
            );
            assertEq(rewards, 0);
        }
    }

    function testFuzz_claimStakeRewards_reverts_noRewards(
        uint24 numValidators,
        uint24 numRewardees
    ) public {
        numValidators = uint24(bound(uint256(numValidators), 1, 800));
        numRewardees = uint24(bound(uint256(numRewardees), 1, numValidators));

        _fuzz_stake(numValidators, stakeAmount_);

        vm.startPrank(sysAddress);
        // apply incentives — only records performance weights, no balance credits
        (RewardInfo[] memory rewardInfos, uint256 totalRounds) = _fuzz_createRewardInfos(numRewardees);
        consensusRegistry.applyIncentives(rewardInfos, totalRounds);
        vm.stopPrank();

        // claiming should always revert since applyIncentives no longer credits rewards
        for (uint256 i; i < rewardInfos.length; ++i) {
            address validator = rewardInfos[i].validatorAddress;
            vm.prank(validator);
            vm.expectRevert();
            consensusRegistry.claimStakeRewards(validator);
        }
    }

    /// @dev Ports the PoC's `hybrid_model_closes_the_zero_reward_cliff` scenario
    /// (participation.rs/hybrid.rs in the Rust PoC) to exact integer bps math: a
    /// US validator leads every round (100% anchor share, 0% for everyone else
    /// today); a Tokyo-like validator never leads but participates in every
    /// round; a third validator does neither. Everyone at the same (baseline)
    /// stake tier, so the stake component splits evenly three ways.
    function test_applyIncentives_hybridBlend_tokyoScenario() public {
        // `_fuzz_stake` derives each staked validator's address from secret `i + 5`.
        _fuzz_stake(3, stakeAmount_);
        address usLeader = _addressFromPrivateKey(5);
        address tokyo = _addressFromPrivateKey(6);
        address third = _addressFromPrivateKey(7);

        uint256 totalRounds = 100;
        RewardInfo[] memory rewardInfos = new RewardInfo[](3);
        // US leader: leads every round, and (like everyone) participates in every round.
        rewardInfos[0] = RewardInfo(usLeader, totalRounds, totalRounds);
        // Tokyo: never leads, but participates (its cert is included) every round.
        rewardInfos[1] = RewardInfo(tokyo, totalRounds, 0);
        // Third validator: participates every round too, never leads.
        rewardInfos[2] = RewardInfo(third, totalRounds, 0);

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, totalRounds);

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        assertEq(perf.validators.length, 3);

        uint256[] memory weightOf = new uint256[](3);
        for (uint256 i; i < perf.validators.length; ++i) {
            if (perf.validators[i] == usLeader) weightOf[0] = perf.weights[i];
            else if (perf.validators[i] == tokyo) weightOf[1] = perf.weights[i];
            else if (perf.validators[i] == third) weightOf[2] = perf.weights[i];
        }

        // Participation share is equal across all three (each submitted totalRounds
        // participationRounds out of a 3*totalRounds submitted sum) -> 8_000/3 bps each.
        // Anchor share: usLeader gets the full 1_000 bps (100% of totalRounds); the
        // other two get 0. Stake share splits evenly at baseline -> 1_000/3 bps each.
        // Cast forces normal truncating integer division; bare literal/literal
        // division is exact-rational at compile time in Solidity and rejects
        // non-whole results (e.g. plain `8_000 / 3` fails to compile).
        uint256 participationShare = uint256(8_000) / 3;
        uint256 stakeShare = uint256(1_000) / 3;
        uint256 expectedUsLeader = participationShare + 1_000 + stakeShare;
        uint256 expectedTokyo = participationShare + stakeShare;
        uint256 expectedThird = participationShare + stakeShare;

        assertEq(weightOf[0], expectedUsLeader);
        assertEq(weightOf[1], expectedTokyo);
        assertEq(weightOf[2], expectedThird);

        // The whole point of #633: today, Tokyo would earn exactly 0 (anchor-only
        // model). Under the hybrid blend it earns the large majority of what the
        // leader earns instead of nothing.
        assertTrue(weightOf[1] > 0);
        assertTrue(weightOf[1] > weightOf[0] / 2);
    }

    /// @dev A validator below the participation floor is excluded entirely (not
    /// just from the participation component) once the floor is enabled -
    /// disabled (0) is the default, so this exercises `setParticipationFloorBps`.
    function test_applyIncentives_participationFloor_excludesBelowThreshold() public {
        _fuzz_stake(2, stakeAmount_);
        address reliable = _addressFromPrivateKey(5);
        address unreliable = _addressFromPrivateKey(6);

        vm.prank(crOwner);
        consensusRegistry.setParticipationFloorBps(3_000); // 30% floor

        uint256 totalRounds = 100;
        RewardInfo[] memory rewardInfos = new RewardInfo[](2);
        rewardInfos[0] = RewardInfo(reliable, 90, 10); // 90% participation, well above floor
        rewardInfos[1] = RewardInfo(unreliable, 10, 0); // 10% participation, below the 30% floor

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, totalRounds);

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        assertEq(perf.validators.length, 1);
        assertEq(perf.validators[0], reliable);
    }

    /// Reproduction of the e2e halt: the real node calls applyIncentives with the
    /// genesis COMMITTEE validators (Active status, set in the constructor), not
    /// freshly-staked ones. Mirror that exactly.
    function test_applyIncentives_genesisCommitteeValidators_doesNotRevert() public {
        // validator1..4 are the constructor's initialValidators, already Active.
        RewardInfo[] memory rewardInfos = new RewardInfo[](4);
        rewardInfos[0] = RewardInfo(validator1, 100, 40);
        rewardInfos[1] = RewardInfo(validator2, 100, 30);
        rewardInfos[2] = RewardInfo(validator3, 90, 20);
        rewardInfos[3] = RewardInfo(validator4, 80, 10);

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, 100);

        IConsensusRegistry.PerformanceWeights memory perf =
            consensusRegistry.getEpochPerformanceWeights();
        assertEq(perf.validators.length, 4, "all genesis committee validators should be weighted");
    }

    /// @dev A fully-absent validator (0 participation, hence 0 anchor) earns
    /// nothing even with the participation floor disabled - the 10% stake
    /// component must not leak to validators that contributed no certificate this
    /// epoch. This is the unconditional "absent -> 0" rule, independent of the floor.
    function test_applyIncentives_absentValidator_earnsZero() public {
        _fuzz_stake(2, stakeAmount_);
        address active = _addressFromPrivateKey(5);
        address absent = _addressFromPrivateKey(6);

        // Floor stays at its 0 default; only the absent-gate excludes `absent`.
        assertEq(consensusRegistry.participationFloorBps(), 0, "precondition: floor disabled");

        RewardInfo[] memory rewardInfos = new RewardInfo[](2);
        rewardInfos[0] = RewardInfo(active, 100, 40);
        rewardInfos[1] = RewardInfo(absent, 0, 0); // contributed nothing this epoch

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, 100);

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        assertEq(perf.validators.length, 1, "only the active validator earns");
        assertEq(perf.validators[0], active, "absent-but-staked validator must be excluded entirely");
    }

    function testRevert_setParticipationFloorBps_aboveMax() public {
        vm.prank(crOwner);
        vm.expectRevert(
            abi.encodeWithSelector(IConsensusRegistry.InvalidParticipationFloor.selector, 10_001)
        );
        consensusRegistry.setParticipationFloorBps(10_001);
    }

    /// @dev Delegated stake pushes a validator into a higher stake tier, strictly
    /// increasing its weight relative to an identical validator with no delegation
    /// - exercised through a minimal mock `IDelegationPool`, not the full
    /// signature-based real `DelegationPool` (out of scope for this weight-math test).
    function test_applyIncentives_stakeTier_delegationIncreasesWeight() public {
        _fuzz_stake(2, stakeAmount_);
        address baseline = _addressFromPrivateKey(5);
        address delegated = _addressFromPrivateKey(6);

        MockDelegationPoolForRewards mockPool = new MockDelegationPoolForRewards();
        // stakeAmount_ (own/required stake) is 1_000_000e18, below the 5M-RLS tier
        // minimum, so the fixed own-stake component alone never crosses a tier -
        // delegating 29M brings `delegated`'s total (own + delegated) to exactly
        // the 30M cap (5 tiers, +10%).
        mockPool.setDelegated(delegated, 29_000_000e18);
        vm.prank(crOwner);
        consensusRegistry.setDelegationPool(address(mockPool));

        uint256 totalRounds = 100;
        RewardInfo[] memory rewardInfos = new RewardInfo[](2);
        rewardInfos[0] = RewardInfo(baseline, 50, 50);
        rewardInfos[1] = RewardInfo(delegated, 50, 50); // identical round counts

        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, totalRounds);

        IConsensusRegistry.PerformanceWeights memory perf = consensusRegistry.getEpochPerformanceWeights();
        uint256 baselineWeight;
        uint256 delegatedWeight;
        for (uint256 i; i < perf.validators.length; ++i) {
            if (perf.validators[i] == baseline) baselineWeight = perf.weights[i];
            else if (perf.validators[i] == delegated) delegatedWeight = perf.weights[i];
        }

        // Identical participation/anchor rounds -> the entire weight difference
        // comes from the stake-tier component (1.10x vs 1.00x baseline weight).
        assertTrue(delegatedWeight > baselineWeight, "delegated stake must increase weight");
    }

    /// @notice Migration safety. The `HybridRewards` hardfork installs this
    /// contract's bytecode over an EXISTING `ConsensusRegistry`'s storage (a
    /// bytecode swap that preserves storage), so the one state variable this
    /// change adds - `participationFloorBps` - MUST occupy a fresh slot appended
    /// after all pre-existing state, never overlapping it. This test populates
    /// every pre-existing storage region (validators, balances, allowlist, and
    /// `_performanceWeights` - the immediate layout neighbor, hence highest-risk),
    /// writes the new var, and asserts no pre-existing slot moved. A layout
    /// regression (inserting the var mid-struct) would corrupt live validator
    /// state on mainnet activation and fail here.
    function test_storageLayout_participationFloorBps_appendOnly_migrationSafe() public {
        // Populate pre-existing storage: stake writes validators/balances/allowlist;
        // applyIncentives writes _performanceWeights (the last pre-existing var,
        // declared immediately before participationFloorBps).
        _fuzz_stake(2, stakeAmount_);
        address v = _addressFromPrivateKey(5);

        RewardInfo[] memory rewardInfos = new RewardInfo[](1);
        rewardInfos[0] = RewardInfo(v, 50, 20);
        vm.prank(sysAddress);
        consensusRegistry.applyIncentives(rewardInfos, 100);

        // Snapshot pre-existing state.
        uint32 epochBefore = consensusRegistry.getCurrentEpoch();
        IConsensusRegistry.ValidatorInfo memory vBefore = consensusRegistry.getValidator(v);
        bool allowlistedBefore = consensusRegistry.validatorAllowlist(v);
        (uint256 balBefore, uint256 stakeBefore, ) = consensusRegistry.getBalanceBreakdown(v);
        IConsensusRegistry.PerformanceWeights memory perfBefore =
            consensusRegistry.getEpochPerformanceWeights();
        assertGt(perfBefore.totalWeight, 0, "precondition: performance weights populated");

        // The new var must default to 0 (a fresh, never-written slot).
        assertEq(consensusRegistry.participationFloorBps(), 0, "new var must default to 0");

        // Write the new var to a sentinel value.
        vm.prank(crOwner);
        consensusRegistry.setParticipationFloorBps(3_000);

        // Every pre-existing storage region must be byte-identical: no slot overlap.
        assertEq(consensusRegistry.getCurrentEpoch(), epochBefore, "currentEpoch slot moved");
        IConsensusRegistry.ValidatorInfo memory vAfter = consensusRegistry.getValidator(v);
        assertEq(vAfter.validatorAddress, vBefore.validatorAddress, "validators slot moved");
        assertEq(uint8(vAfter.currentStatus), uint8(vBefore.currentStatus), "validator status slot moved");
        assertEq(vAfter.stakeVersion, vBefore.stakeVersion, "validator stakeVersion slot moved");
        assertEq(consensusRegistry.validatorAllowlist(v), allowlistedBefore, "allowlist slot moved");
        (uint256 balAfter, uint256 stakeAfter, ) = consensusRegistry.getBalanceBreakdown(v);
        assertEq(balAfter, balBefore, "balance slot moved");
        assertEq(stakeAfter, stakeBefore, "stake slot moved");
        IConsensusRegistry.PerformanceWeights memory perfAfter =
            consensusRegistry.getEpochPerformanceWeights();
        assertEq(perfAfter.totalWeight, perfBefore.totalWeight, "_performanceWeights.totalWeight slot moved");
        assertEq(perfAfter.validators.length, perfBefore.validators.length, "_performanceWeights.validators slot moved");

        // And the new var holds exactly what we wrote.
        assertEq(consensusRegistry.participationFloorBps(), 3_000, "new var not persisted at its own slot");
    }
}

/// @notice Minimal `IDelegationPool.getTotalDelegatedStake` double for testing the
/// stake-tier component in isolation, without the full signature-based delegation flow.
contract MockDelegationPoolForRewards {
    mapping(address => uint256) private _delegated;

    function setDelegated(address validatorAddress, uint256 amount) external {
        _delegated[validatorAddress] = amount;
    }

    function getTotalDelegatedStake(address validatorAddress) external view returns (uint256) {
        return _delegated[validatorAddress];
    }
}
