// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {IRewardCurve} from "../interfaces/IRewardCurve.sol";

/**
 * @title RewardCurve
 * @notice A Rayls Contract
 *
 * @notice Self-regulating, revenue-based staking reward curve (POC for issue #103)
 * @dev APY = (baseMonthlyEmission + variableMonthlyEmission) * 12 / rlsStaked, always derived,
 *      never stored. rlsStaked is a caller-supplied parameter — this contract does not read
 *      ConsensusRegistry/DelegationPool itself, keeping it a pure function of emission state and
 *      stake, testable in isolation and pluggable into whatever eventually supplies the total
 *      (a test, a keeper/oracle, or RewardDistributor directly).
 * @dev Does not custody or transfer RLS. Real payout stays in RewardDistributor/RLSAccumulator.
 * @dev UUPS upgradeable with AccessControl
 */
contract RewardCurve is Initializable, UUPSUpgradeable, AccessControlUpgradeable, IRewardCurve {
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    /// @notice Role authorized to call recordRevenue/resetMonthlyRevenue. A standard
    ///         AccessControl role (not a single stored address) so more than one reporter can be
    ///         authorized at once, via the inherited grantRole/revokeRole.
    bytes32 public constant REVENUE_REPORTER_ROLE = keccak256("REVENUE_REPORTER_ROLE");
    uint256 public constant BPS_DENOMINATOR = 10_000;
    uint256 public constant MONTHS_PER_YEAR = 12;
    /// @notice Defensive overflow backstop on individual monthly emission values, not a policy
    ///         cap: 1 trillion RLS/month is orders of magnitude beyond any plausible network
    ///         emission, but bounding it means `(base + variable) * MONTHS_PER_YEAR *
    ///         BPS_DENOMINATOR` can never overflow uint256, so a bad `recordRevenue` call (from
    ///         REVENUE_REPORTER_ROLE, a lower-trust role than DEFAULT_ADMIN_ROLE) can no longer
    ///         permanently revert every view function until an admin calls resetMonthlyRevenue.
    uint256 public constant MAX_MONTHLY_EMISSION = 1_000_000_000_000e18;
    /// @notice Defensive cap on previewCurve's caller-supplied array length — a chart never needs
    ///         more than a few dozen points; this just bounds an otherwise-unbounded loop surface.
    uint256 public constant MAX_PREVIEW_LEVELS = 200;

    /// @custom:storage-location erc7201:rewardcurve.storage.v1
    struct RewardCurveStorage {
        /// @notice Flat monthly RLS emission committed by the Foundation treasury
        uint256 baseMonthlyEmission;
        /// @notice Rolling monthly RLS emission accumulated from reported network revenue
        uint256 variableMonthlyEmission;
        /// @notice Current emission funding phase (observable marker only)
        Phase currentPhase;
    }

    // keccak256(abi.encode(uint256(keccak256("rewardcurve.storage.v1")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant REWARD_CURVE_STORAGE_LOCATION =
        0xc11643142d0559101211416e904d9720f39a4cca3864891ca622f773e0424100;

    function _getRewardCurveStorage() private pure returns (RewardCurveStorage storage $) {
        assembly {
            $.slot := REWARD_CURVE_STORAGE_LOCATION
        }
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address admin_) external initializer {
        if (admin_ == address(0)) revert ZeroAddress();

        __AccessControl_init();

        _grantRole(DEFAULT_ADMIN_ROLE, admin_);
        _grantRole(UPGRADER_ROLE, admin_);
        _grantRole(REVENUE_REPORTER_ROLE, admin_);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    // ========== VIEWS ==========

    /// @inheritdoc IRewardCurve
    function baseMonthlyEmission() external view override returns (uint256) {
        return _getRewardCurveStorage().baseMonthlyEmission;
    }

    /// @inheritdoc IRewardCurve
    function variableMonthlyEmission() external view override returns (uint256) {
        return _getRewardCurveStorage().variableMonthlyEmission;
    }

    /// @inheritdoc IRewardCurve
    function currentPhase() external view override returns (Phase) {
        return _getRewardCurveStorage().currentPhase;
    }

    /// @inheritdoc IRewardCurve
    function getEmissionBreakdown()
        external
        view
        override
        returns (uint256 baseMonthly, uint256 variableMonthly, uint256 annualEmission)
    {
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        baseMonthly = $.baseMonthlyEmission;
        variableMonthly = $.variableMonthlyEmission;
        annualEmission = (baseMonthly + variableMonthly) * MONTHS_PER_YEAR;
    }

    /// @inheritdoc IRewardCurve
    function getCurrentApyBps(uint256 rlsStaked) external view override returns (uint256) {
        return _currentApyBps(rlsStaked);
    }

    /// @inheritdoc IRewardCurve
    function getApyBreakdown(
        uint256 rlsStaked
    ) external view override returns (uint256 baseApyBps, uint256 variableApyBps) {
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        baseApyBps = _apyBps($.baseMonthlyEmission * MONTHS_PER_YEAR, rlsStaked);
        variableApyBps = _apyBps($.variableMonthlyEmission * MONTHS_PER_YEAR, rlsStaked);
    }

    /// @inheritdoc IRewardCurve
    function estimateYield(uint256 amount, uint256 rlsStaked) external view override returns (uint256) {
        return (amount * _currentApyBps(rlsStaked)) / BPS_DENOMINATOR;
    }

    /// @inheritdoc IRewardCurve
    function previewCurve(
        uint256[] calldata stakeLevels
    ) external view override returns (uint256[] memory apyBpsAtLevel) {
        uint256 len = stakeLevels.length;
        if (len > MAX_PREVIEW_LEVELS) revert TooManyStakeLevels();

        RewardCurveStorage storage $ = _getRewardCurveStorage();
        uint256 emissionAnnual = ($.baseMonthlyEmission + $.variableMonthlyEmission) * MONTHS_PER_YEAR;

        apyBpsAtLevel = new uint256[](len);
        for (uint256 i; i < len; ++i) {
            apyBpsAtLevel[i] = _apyBps(emissionAnnual, stakeLevels[i]);
        }
    }

    function _currentApyBps(uint256 rlsStaked) internal view returns (uint256) {
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        return _apyBps(($.baseMonthlyEmission + $.variableMonthlyEmission) * MONTHS_PER_YEAR, rlsStaked);
    }

    /// @dev Multiply-then-divide, matching RewardDistributor._splitTarget's existing idiom, to
    ///      avoid truncating to zero when emission < rlsStaked (true almost everywhere near the
    ///      >2B RLS staked scenario this curve is meant to handle gracefully).
    function _apyBps(uint256 emissionAnnual, uint256 rlsStaked) internal pure returns (uint256) {
        if (rlsStaked == 0) return 0;
        return (emissionAnnual * BPS_DENOMINATOR) / rlsStaked;
    }

    // ========== GOVERNANCE ==========

    /// @inheritdoc IRewardCurve
    function setBaseMonthlyEmission(uint256 newBase) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        if (newBase > MAX_MONTHLY_EMISSION) revert EmissionTooLarge();
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        uint256 oldBase = $.baseMonthlyEmission;
        $.baseMonthlyEmission = newBase;
        emit BaseMonthlyEmissionUpdated(oldBase, newBase);
    }

    /// @inheritdoc IRewardCurve
    function recordRevenue(uint256 amount) external override onlyRole(REVENUE_REPORTER_ROLE) {
        if (amount == 0) revert ZeroAmount();
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        uint256 newVariable = $.variableMonthlyEmission + amount;
        if (newVariable > MAX_MONTHLY_EMISSION) revert EmissionTooLarge();
        $.variableMonthlyEmission = newVariable;
        emit RevenueRecorded(amount, newVariable);
    }

    /// @inheritdoc IRewardCurve
    function resetMonthlyRevenue() external override onlyRole(REVENUE_REPORTER_ROLE) {
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        uint256 cleared = $.variableMonthlyEmission;
        $.variableMonthlyEmission = 0;
        emit MonthlyRevenueReset(cleared);
    }

    /// @inheritdoc IRewardCurve
    function setPhase(Phase newPhase) external override onlyRole(DEFAULT_ADMIN_ROLE) {
        RewardCurveStorage storage $ = _getRewardCurveStorage();
        Phase oldPhase = $.currentPhase;
        $.currentPhase = newPhase;
        emit PhaseTransitioned(oldPhase, newPhase);
    }
}
