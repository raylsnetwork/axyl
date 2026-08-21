// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

/**
 * @title IRewardCurve
 * @notice A Rayls Contract
 *
 * @notice Interface for a self-regulating, revenue-based staking reward curve (POC for issue #103)
 * @dev APY is never stored — it is always derived as (Base_monthly + Variable_monthly) * 12 / RLS_staked.
 *      Only the emission inputs (base + variable) are governance-settable; there is no setter for
 *      APY itself, so a fixed or guaranteed APY cannot be configured by construction.
 * @dev Emission amounts are RLS-denominated (1e18 scale), not rates. Base is a flat monthly amount
 *      committed by the Foundation treasury; Variable is a rolling monthly figure accumulated from
 *      reported network revenue. Neither is minted — both model an existing-reserve / real-revenue
 *      payout budget, mirroring RewardDistributor + RLSAccumulator's existing non-inflationary design.
 * @dev This contract does not custody or transfer RLS tokens — it is pure emission bookkeeping and
 *      APY math. Real payout stays in RewardDistributor/RLSAccumulator.
 */
interface IRewardCurve {
    /// @notice Emission funding phase, per issue #103's roadmap. Purely an observable marker —
    ///         no transition logic is gated on it; phase changes are always an explicit governance
    ///         (admin) action here, matching "each shift requires a governance vote".
    enum Phase {
        /// @notice Phase 1 (today -> ~2y): Foundation-backed bootstrapping. Emission = Base +
        ///         Variable, with Base anchoring early growth while the network is young.
        FoundationHeavy,
        /// @notice Phase 2 (~2-3y): transitional. Base decreases as Variable (real network
        ///         revenue) grows to take over a larger share of emission.
        Mixed,
        /// @notice Phase 3 (long term): Foundation base retired entirely. Emission = Variable
        ///         only — staking rewards are funded purely by network revenue.
        RevenueOnly
    }

    // errors
    error ZeroAddress();
    error ZeroAmount();
    error EmissionTooLarge();
    error TooManyStakeLevels();

    // events
    event BaseMonthlyEmissionUpdated(uint256 oldBase, uint256 newBase);
    event RevenueRecorded(uint256 amount, uint256 newVariableMonthlyEmission);
    event MonthlyRevenueReset(uint256 clearedAmount);
    event PhaseTransitioned(Phase oldPhase, Phase newPhase);

    /// @notice Get the flat monthly RLS emission committed by the Foundation treasury
    function baseMonthlyEmission() external view returns (uint256);

    /// @notice Get the rolling monthly RLS emission accumulated from reported network revenue
    function variableMonthlyEmission() external view returns (uint256);

    /// @notice Get the current emission funding phase
    function currentPhase() external view returns (Phase);

    /// @notice Get the base/variable monthly emission and their annualized sum
    /// @return baseMonthly The flat Foundation-committed monthly emission (RLS)
    /// @return variableMonthly The rolling revenue-derived monthly emission (RLS)
    /// @return annualEmission (baseMonthly + variableMonthly) * 12
    function getEmissionBreakdown()
        external
        view
        returns (uint256 baseMonthly, uint256 variableMonthly, uint256 annualEmission);

    /// @notice Get the derived APY, in basis points, at a given total RLS staked
    /// @param rlsStaked Total RLS staked (1e18 scale); returns 0 if zero
    /// @return apyBps Annualized emission / rlsStaked, expressed in basis points (10_000 = 100%)
    function getCurrentApyBps(uint256 rlsStaked) external view returns (uint256 apyBps);

    /// @notice Get the base-funded and revenue-funded portions of the derived APY separately
    /// @dev Computed independently from base/variable emission, not by splitting the combined
    ///      APY proportionally — the two halves may sum to the whole within +/-1 bps due to
    ///      independent integer rounding.
    function getApyBreakdown(
        uint256 rlsStaked
    ) external view returns (uint256 baseApyBps, uint256 variableApyBps);

    /// @notice Estimate the projected annual RLS return for a given staked amount
    /// @param amount The hypothetical staked amount (1e18 scale)
    /// @param rlsStaked Total RLS staked network-wide, used to derive the current APY
    /// @return projectedAnnualRls amount * getCurrentApyBps(rlsStaked) / 10_000
    function estimateYield(uint256 amount, uint256 rlsStaked) external view returns (uint256 projectedAnnualRls);

    /// @notice Preview the APY curve at a caller-supplied list of hypothetical total-stake levels
    /// @dev Reverts TooManyStakeLevels above MAX_PREVIEW_LEVELS — a caller-controlled array length
    ///      is otherwise an unbounded-loop surface if this is ever called from another contract's
    ///      transaction rather than an off-chain eth_call.
    /// @param stakeLevels Hypothetical total RLS staked values to evaluate, in 1e18 scale
    /// @return apyBpsAtLevel APY in bps at each corresponding stakeLevels entry
    function previewCurve(uint256[] calldata stakeLevels) external view returns (uint256[] memory apyBpsAtLevel);

    /// @notice Set the flat monthly RLS emission committed by the Foundation treasury
    /// @dev onlyRole(DEFAULT_ADMIN_ROLE) — models a governance-approved treasury commitment.
    ///      Reverts EmissionTooLarge above MAX_MONTHLY_EMISSION (a defensive overflow backstop,
    ///      not a policy cap — see MAX_MONTHLY_EMISSION's doc comment).
    function setBaseMonthlyEmission(uint256 newBase) external;

    /// @notice Report network revenue for the current month, adding to variableMonthlyEmission
    /// @dev onlyRole(REVENUE_REPORTER_ROLE) — a standard AccessControl role (grant/revoke via the
    ///      inherited grantRole/revokeRole, admin-gated by DEFAULT_ADMIN_ROLE by default), so more
    ///      than one reporter address can be authorized at once. Mirrors
    ///      RewardDistributor.receiveRewards/onlyFeeAggregator in spirit.
    ///      Reverts EmissionTooLarge if the resulting variableMonthlyEmission would exceed
    ///      MAX_MONTHLY_EMISSION.
    function recordRevenue(uint256 amount) external;

    /// @notice Zero out the rolling variable monthly emission (start of a new reporting month)
    /// @dev onlyRole(REVENUE_REPORTER_ROLE) — paired with recordRevenue under the same role, since
    ///      whoever manages the monthly revenue-reporting cycle also closes it out; deliberately
    ///      manual, no automatic time-based reset
    function resetMonthlyRevenue() external;

    /// @notice Set the current emission funding phase (observable marker only, no gating logic)
    /// @dev onlyRole(DEFAULT_ADMIN_ROLE) — the governance vote itself happens off-chain; this call
    ///      just records its outcome
    function setPhase(Phase newPhase) external;
}
