// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {console2} from "forge-std/Script.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {InteractionScript} from "../_base/InteractionScript.sol";
import {RewardCurve} from "../../../src/fees/RewardCurve.sol";
import {IRewardCurve} from "../../../src/interfaces/IRewardCurve.sol";

/// @title RewardCurveOps
/// @notice Deploy + operate a RewardCurve instance (issue #103). Pick the action with `--sig`.
///
/// RewardCurve has no fixed genesis address and no entry in deployments.json -- it's a brand-new
/// contract not deployed anywhere yet. `deploy()` creates one instance; run it twice (once per
/// track) to get the two addresses RewardDistributor.setRewardCurve (Track A) and
/// .setOpenTierRewardCurve (Track B) expect -- see RewardDistributorOps.s.sol's setConfig().
/// Every action except `deploy()` operates on an existing instance passed via REWARD_CURVE.
///
/// Deploy (once per track):
///   LABEL="Track A" \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "deploy()" \
///     --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
///
/// Read:
///   REWARD_CURVE=0x... [RLS_STAKED=...] \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "status()" \
///     --rpc-url $RPC_URL -vvvv
///
/// Admin (DEFAULT_ADMIN_ROLE):
///   REWARD_CURVE=0x... BASE_MONTHLY_EMISSION=100000000000000000000000 \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "setBaseMonthlyEmission()" \
///     --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
///
///   REWARD_CURVE=0x... PHASE=0 \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "setPhase()" \
///     --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
///
/// Revenue reporter (REVENUE_REPORTER_ROLE):
///   REWARD_CURVE=0x... REVENUE_AMOUNT=5000000000000000000000 \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "recordRevenue()" \
///     --rpc-url $RPC_URL --broadcast --private-key $REPORTER_PK -vvvv
///
///   REWARD_CURVE=0x... \
///     forge script .../RewardCurveOps.s.sol:RewardCurveOps --sig "resetMonthlyRevenue()" \
///     --rpc-url $RPC_URL --broadcast --private-key $REPORTER_PK -vvvv
contract RewardCurveOps is InteractionScript {
    /// @notice Real Rayls mainnet chain-id (RaylsNetwork::Mainnet,
    ///         crates/infrastructure/types/src/network.rs). NOT 487 -- see `_requireNotMainnet`.
    uint256 internal constant MAINNET_CHAIN_ID = 72957;
    /// @notice Real Rayls testnet chain-id (RaylsNetwork::Testnet), for an informational
    ///         (non-blocking) log only -- a local/forked deploy legitimately runs elsewhere.
    uint256 internal constant TESTNET_CHAIN_ID = 7295799;

    RewardCurve rc;

    function _setUp() internal override {
        address existing = vm.envOr("REWARD_CURVE", address(0));
        if (existing != address(0)) {
            rc = RewardCurve(existing);
        }
    }

    function run() public {
        deploy();
    }

    // ── DEPLOY ──────────────────────────────────────────────────────────

    /// @notice Deploy a fresh RewardCurve implementation + UUPS proxy. ADMIN (defaults to
    ///         deployments.admin) receives DEFAULT_ADMIN_ROLE, UPGRADER_ROLE, and
    ///         REVENUE_REPORTER_ROLE via `initialize`. Run this twice for the two-track design
    ///         (Track A / Track B) -- the contract itself has no notion of "track", that only
    ///         exists in how the resulting address is later passed to
    ///         RewardDistributor.setRewardCurve / .setOpenTierRewardCurve. LABEL is log-only.
    /// Env vars: ADMIN (optional, defaults to deployments.admin), LABEL (optional, log-only)
    function deploy() public {
        _requireNotMainnet();

        address admin = vm.envOr("ADMIN", deployments.admin);
        string memory label = vm.envOr("LABEL", string("(unlabeled)"));

        logSection(string.concat("Deploying RewardCurve ", label));
        console2.log("Chain id:", block.chainid);
        console2.log("Admin:   ", admin);

        vm.startBroadcast();
        RewardCurve impl = new RewardCurve();
        bytes memory initData = abi.encodeCall(RewardCurve.initialize, (admin));
        ERC1967Proxy proxy = new ERC1967Proxy(address(impl), initData);
        vm.stopBroadcast();

        rc = RewardCurve(address(proxy));

        require(rc.hasRole(0x00, admin), "admin role not granted");
        require(rc.hasRole(rc.REVENUE_REPORTER_ROLE(), admin), "reporter role not granted");

        console2.log("");
        console2.log("Implementation:", address(impl));
        console2.log("Proxy:         ", address(proxy));
        console2.log("");
        console2.log("Record this proxy address -- it's what RewardDistributor.setRewardCurve /");
        console2.log("setOpenTierRewardCurve needs. Not written to deployments.json: no fixed");
        console2.log("address, and two independent instances are expected for the two tracks.");
    }

    // ── READ ────────────────────────────────────────────────────────────

    /// @notice Print emission state, phase, roles, and APY at RLS_STAKED (if provided).
    /// Env vars: REWARD_CURVE (required), ADMIN (optional), RLS_STAKED (optional, 1e18 scale)
    function status() public {
        require(address(rc) != address(0), "Set REWARD_CURVE");
        address admin = vm.envOr("ADMIN", deployments.admin);

        console2.log("========== RewardCurve Status ==========");
        console2.log("Contract:", address(rc));

        logSection("Emission");
        (uint256 base, uint256 variable, uint256 annual) = rc.getEmissionBreakdown();
        console2.log("Base monthly:    ", base);
        console2.log("Variable monthly:", variable);
        console2.log("Annual emission: ", annual);
        console2.log("Phase (enum idx):", uint256(rc.currentPhase()));

        logSection("Roles");
        console2.log("Admin:", admin);
        console2.log("  DEFAULT_ADMIN:    ", rc.hasRole(0x00, admin));
        console2.log("  UPGRADER:         ", rc.hasRole(rc.UPGRADER_ROLE(), admin));
        console2.log("  REVENUE_REPORTER: ", rc.hasRole(rc.REVENUE_REPORTER_ROLE(), admin));

        uint256 rlsStaked = vm.envOr("RLS_STAKED", uint256(0));
        if (rlsStaked > 0) {
            logSection("APY at RLS_STAKED");
            console2.log("RLS staked:", rlsStaked);
            console2.log("APY bps:   ", rc.getCurrentApyBps(rlsStaked));
            (uint256 baseApy, uint256 variableApy) = rc.getApyBreakdown(rlsStaked);
            console2.log("  base bps:    ", baseApy);
            console2.log("  variable bps:", variableApy);
        }
    }

    // ── ADMIN (DEFAULT_ADMIN_ROLE) ──────────────────────────────────────

    /// @notice Set the flat monthly RLS emission committed by the Foundation treasury.
    /// Env vars: REWARD_CURVE (required), BASE_MONTHLY_EMISSION (required, 1e18 scale)
    function setBaseMonthlyEmission() public {
        require(address(rc) != address(0), "Set REWARD_CURVE");
        uint256 newBase = vm.envUint("BASE_MONTHLY_EMISSION");

        console2.log("Current base monthly emission:", rc.baseMonthlyEmission());
        console2.log("New base monthly emission:    ", newBase);

        vm.startBroadcast();
        rc.setBaseMonthlyEmission(newBase);
        vm.stopBroadcast();

        require(rc.baseMonthlyEmission() == newBase, "base mismatch");
        console2.log("");
        console2.log("Done.");
    }

    /// @notice Set the current emission funding phase (observable marker only, no gating logic).
    /// Env vars: REWARD_CURVE (required), PHASE (required, 0=FoundationHeavy, 1=Mixed, 2=RevenueOnly)
    function setPhase() public {
        require(address(rc) != address(0), "Set REWARD_CURVE");
        uint256 phaseIdx = vm.envUint("PHASE");
        require(phaseIdx <= uint256(IRewardCurve.Phase.RevenueOnly), "PHASE must be 0, 1, or 2");
        IRewardCurve.Phase newPhase = IRewardCurve.Phase(phaseIdx);

        console2.log("Current phase (enum idx):", uint256(rc.currentPhase()));
        console2.log("New phase (enum idx):    ", phaseIdx);

        vm.startBroadcast();
        rc.setPhase(newPhase);
        vm.stopBroadcast();

        require(rc.currentPhase() == newPhase, "phase mismatch");
        console2.log("");
        console2.log("Done.");
    }

    // ── REVENUE REPORTER (REVENUE_REPORTER_ROLE) ────────────────────────

    /// @notice Report network revenue for the current month, adding to variableMonthlyEmission.
    /// Env vars: REWARD_CURVE (required), REVENUE_AMOUNT (required, 1e18 scale)
    function recordRevenue() public {
        require(address(rc) != address(0), "Set REWARD_CURVE");
        uint256 amount = vm.envUint("REVENUE_AMOUNT");
        uint256 before = rc.variableMonthlyEmission();

        console2.log("Variable monthly emission before:", before);
        console2.log("Recording revenue:               ", amount);

        vm.startBroadcast();
        rc.recordRevenue(amount);
        vm.stopBroadcast();

        uint256 afterAmount = rc.variableMonthlyEmission();
        require(afterAmount == before + amount, "revenue not recorded");
        console2.log("Variable monthly emission after: ", afterAmount);
        console2.log("");
        console2.log("Done.");
    }

    /// @notice Zero out the rolling variable monthly emission (start of a new reporting month).
    /// Env vars: REWARD_CURVE (required)
    function resetMonthlyRevenue() public {
        require(address(rc) != address(0), "Set REWARD_CURVE");
        uint256 before = rc.variableMonthlyEmission();
        console2.log("Variable monthly emission before reset:", before);

        vm.startBroadcast();
        rc.resetMonthlyRevenue();
        vm.stopBroadcast();

        require(rc.variableMonthlyEmission() == 0, "reset failed");
        console2.log("Variable monthly emission after reset: ", rc.variableMonthlyEmission());
        console2.log("");
        console2.log("Done.");
    }

    // ── SAFETY ──────────────────────────────────────────────────────────

    /// @dev Refuse to deploy on real Rayls mainnet (chain-id 72957). Deliberately NOT the `487`
    ///      constant _mocks/DeployFeeAggregatorMocks.s.sol calls "MAINNET_CHAIN_ID" -- 487 is
    ///      actually RaylsNetwork::Local's chain-id (crates/infrastructure/types/src/network.rs),
    ///      not mainnet's; that existing guard doesn't block real mainnet at all.
    function _requireNotMainnet() internal view {
        require(block.chainid != MAINNET_CHAIN_ID, "refusing to deploy RewardCurve on mainnet");
        if (block.chainid != TESTNET_CHAIN_ID) {
            console2.log("NOTE: chain id is neither testnet (7295799) nor mainnet:", block.chainid);
        }
    }
}
