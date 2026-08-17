// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import "forge-std/Test.sol";
import {RewardCurve} from "src/fees/RewardCurve.sol";
import {IRewardCurve} from "src/interfaces/IRewardCurve.sol";
import {IAccessControl} from "@openzeppelin/contracts/access/IAccessControl.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

/// @notice Isolated unit tests for RewardCurve (POC for issue #103) — zero mocks. Proves the
///         self-regulating emission-to-APY mechanism in pure isolation before the integration
///         test in RewardDistributorExtendedTest.t.sol proves it survives the real distribution
///         pipeline.
contract RewardCurveTest is Test {
    RewardCurve public curve;

    address public admin = address(0xA11CE);
    address public reporter = address(0xBEEF);
    address public stranger = address(0x5717);

    function setUp() public {
        RewardCurve impl = new RewardCurve();
        bytes memory initData = abi.encodeCall(RewardCurve.initialize, (admin));
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        curve = RewardCurve(address(proxy));

        vm.prank(admin);
        curve.setRevenueReporter(reporter);
    }

    // =========================================================================
    //  1. Self-regulating APY — the headline #103 claim
    // =========================================================================

    function test_apyCompresses_asStakeGrows_fixedEmission() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(500_000e18);
        vm.prank(reporter);
        curve.recordRevenue(500_000e18);
        // annualEmission = (500_000e18 + 500_000e18) * 12 = 12_000_000e18

        uint256[] memory levels = new uint256[](5);
        levels[0] = 10_000_000e18;
        levels[1] = 100_000_000e18;
        levels[2] = 1_000_000_000e18;
        levels[3] = 2_000_000_000e18;
        levels[4] = 10_000_000_000e18;

        uint256 prevApy = type(uint256).max;
        for (uint256 i; i < levels.length; ++i) {
            uint256 apyBps = curve.getCurrentApyBps(levels[i]);
            assertLt(apyBps, prevApy, "APY must strictly decrease as stake grows");
            prevApy = apyBps;
        }

        // Inverse-proportional: doubling stake should roughly halve APY.
        uint256 apyAt1B = curve.getCurrentApyBps(1_000_000_000e18);
        uint256 apyAt2B = curve.getCurrentApyBps(2_000_000_000e18);
        assertApproxEqRel(apyAt2B * 2, apyAt1B, 0.01e18, "APY should scale as 1/stake");
    }

    function test_apyRises_whenVariableEmissionRises_fixedStake() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(500_000e18);

        uint256 stake = 100_000_000e18;

        vm.prank(reporter);
        curve.recordRevenue(100_000e18);
        // annualEmission = (500_000e18 + 100_000e18) * 12 = 7_200_000e18
        // apyBps = 7_200_000e18 * 10_000 / 100_000_000e18 = 720
        assertEq(curve.getCurrentApyBps(stake), 720);

        vm.prank(reporter);
        curve.recordRevenue(400_000e18);
        // variable is now 500_000e18 -> annualEmission = 12_000_000e18
        // apyBps = 12_000_000e18 * 10_000 / 100_000_000e18 = 1200
        assertEq(curve.getCurrentApyBps(stake), 1200);
    }

    function test_phaseTransition_baseDown_variableUp_preservesApy() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(800_000e18);
        vm.prank(reporter);
        curve.recordRevenue(200_000e18);

        uint256 stake = 500_000_000e18;
        uint256 apyBefore = curve.getCurrentApyBps(stake);
        assertEq(apyBefore, 240); // (800_000+200_000)*12*10_000/500_000_000e18

        // Governance shifts the mix: base down, variable up by the same amount.
        vm.prank(admin);
        curve.setBaseMonthlyEmission(200_000e18);
        vm.prank(reporter);
        curve.recordRevenue(600_000e18); // variable: 200_000e18 -> 800_000e18

        vm.expectEmit(true, true, true, true);
        emit IRewardCurve.PhaseTransitioned(IRewardCurve.Phase.FoundationHeavy, IRewardCurve.Phase.Mixed);
        vm.prank(admin);
        curve.setPhase(IRewardCurve.Phase.Mixed);

        assertEq(curve.getCurrentApyBps(stake), apyBefore, "same total emission must yield same APY");
        assertEq(uint8(curve.currentPhase()), uint8(IRewardCurve.Phase.Mixed));
    }

    function test_apyAt2BStaked_wellBelowFixedApyCeiling() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(500_000e18);
        vm.prank(reporter);
        curve.recordRevenue(500_000e18);

        uint256 apyBps = curve.getCurrentApyBps(2_000_000_000e18);
        assertEq(apyBps, 60); // 12_000_000e18 * 10_000 / 2_000_000_000e18

        // Comparator: this repo's own fixed-rate test default (RewardDistributorExtendedTest
        // ._setupAccumulator sets targetApyBps=5000, i.e. 50%) — not an invented external figure.
        assertLt(apyBps, 5000, "curve-derived APY at >2B staked must sit well below a 50% fixed target");
    }

    // =========================================================================
    //  2. Breakdown / estimator / curve-preview views (the "in scope" UI feeds)
    // =========================================================================

    function test_getApyBreakdown_sumsToOverallWithinRounding() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(333_333e18);
        vm.prank(reporter);
        curve.recordRevenue(777_777e18);

        uint256 stake = 987_654_321e18;
        (uint256 baseApy, uint256 variableApy) = curve.getApyBreakdown(stake);
        uint256 overall = curve.getCurrentApyBps(stake);

        assertApproxEqAbs(baseApy + variableApy, overall, 1);
    }

    function test_estimateYield_matchesApyBpsMath() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(500_000e18);
        vm.prank(reporter);
        curve.recordRevenue(500_000e18);

        uint256 stake = 300_000_000e18;
        uint256 apyBps = curve.getCurrentApyBps(stake);

        uint256[] memory amounts = new uint256[](3);
        amounts[0] = 1_000e18;
        amounts[1] = 50_000e18;
        amounts[2] = 12_345e18;

        for (uint256 i; i < amounts.length; ++i) {
            uint256 expected = (amounts[i] * apyBps) / 10_000;
            assertEq(curve.estimateYield(amounts[i], stake), expected);
        }
    }

    function test_previewCurve_monotonicNonIncreasing() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(1_000_000e18);
        vm.prank(reporter);
        curve.recordRevenue(1_000_000e18);

        uint256[] memory stakeLevels = new uint256[](4);
        stakeLevels[0] = 50_000_000e18;
        stakeLevels[1] = 500_000_000e18;
        stakeLevels[2] = 1_500_000_000e18;
        stakeLevels[3] = 5_000_000_000e18;

        uint256[] memory apys = curve.previewCurve(stakeLevels);
        for (uint256 i = 1; i < apys.length; ++i) {
            assertLe(apys[i], apys[i - 1], "curve preview must be non-increasing in stake");
        }
    }

    function test_previewCurve_zeroStakeLevel_returnsZeroNotRevert() public {
        vm.prank(admin);
        curve.setBaseMonthlyEmission(1_000_000e18);

        uint256[] memory stakeLevels = new uint256[](3);
        stakeLevels[0] = 100_000_000e18;
        stakeLevels[1] = 0;
        stakeLevels[2] = 200_000_000e18;

        uint256[] memory apys = curve.previewCurve(stakeLevels);
        assertEq(apys[1], 0);
    }

    // =========================================================================
    //  3. Governance access control
    // =========================================================================

    function test_recordRevenue_onlyRevenueReporter_reverts() public {
        vm.expectRevert(IRewardCurve.OnlyRevenueReporter.selector);
        vm.prank(stranger);
        curve.recordRevenue(1e18);
    }

    function test_setBaseMonthlyEmission_onlyAdmin_reverts() public {
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, bytes32(0))
        );
        vm.prank(stranger);
        curve.setBaseMonthlyEmission(1e18);
    }

    function test_setPhase_onlyAdmin_reverts() public {
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, bytes32(0))
        );
        vm.prank(stranger);
        curve.setPhase(IRewardCurve.Phase.Mixed);
    }

    function test_resetMonthlyRevenue_onlyAdmin_reverts() public {
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, bytes32(0))
        );
        vm.prank(stranger);
        curve.resetMonthlyRevenue();
    }

    function test_setRevenueReporter_onlyAdmin_reverts() public {
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, bytes32(0))
        );
        vm.prank(stranger);
        curve.setRevenueReporter(stranger);
    }

    function test_setBaseMonthlyEmission_aboveMax_reverts() public {
        // Read the constant into a local BEFORE arming prank/expectRevert — calling
        // curve.MAX_MONTHLY_EMISSION() inline as an argument expression would itself be the
        // "next call" that consumes both cheatcodes, before setBaseMonthlyEmission ever runs.
        uint256 maxEmission = curve.MAX_MONTHLY_EMISSION();
        vm.expectRevert(IRewardCurve.EmissionTooLarge.selector);
        vm.prank(admin);
        curve.setBaseMonthlyEmission(maxEmission + 1);
    }

    function test_recordRevenue_aboveMax_reverts() public {
        uint256 maxEmission = curve.MAX_MONTHLY_EMISSION();
        vm.prank(reporter);
        curve.recordRevenue(maxEmission);

        // Already at the ceiling — one more wei pushes the sum over it.
        vm.expectRevert(IRewardCurve.EmissionTooLarge.selector);
        vm.prank(reporter);
        curve.recordRevenue(1);
    }

    function test_setBaseMonthlyEmission_atMax_succeeds() public {
        uint256 maxEmission = curve.MAX_MONTHLY_EMISSION();
        vm.prank(admin);
        curve.setBaseMonthlyEmission(maxEmission);
        assertEq(curve.baseMonthlyEmission(), maxEmission);
    }

    function test_resetMonthlyRevenue_zeroesAccumulator_emitsEvent() public {
        vm.prank(reporter);
        curve.recordRevenue(42_000e18);
        assertEq(curve.variableMonthlyEmission(), 42_000e18);

        vm.expectEmit(true, true, true, true);
        emit IRewardCurve.MonthlyRevenueReset(42_000e18);
        vm.prank(admin);
        curve.resetMonthlyRevenue();

        assertEq(curve.variableMonthlyEmission(), 0);
    }

    // =========================================================================
    //  4. Fuzz
    // =========================================================================

    function testFuzz_apyBps_nonIncreasing_inStake(uint256 stakeLo, uint256 stakeHi) public {
        stakeLo = bound(stakeLo, 1e18, 1e30);
        stakeHi = bound(stakeHi, stakeLo, 1e30);

        vm.prank(admin);
        curve.setBaseMonthlyEmission(1_000_000e18);
        vm.prank(reporter);
        curve.recordRevenue(500_000e18);

        assertLe(curve.getCurrentApyBps(stakeHi), curve.getCurrentApyBps(stakeLo));
    }
}
