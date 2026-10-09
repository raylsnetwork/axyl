// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import {console2} from "forge-std/Script.sol";
import {InteractionScript} from "../_base/InteractionScript.sol";
import {RewardDistributor} from "../../../src/fees/RewardDistributor.sol";

/// @title RewardDistributorOps
/// @notice Unified operations for the RewardDistributor. Pick the action with `--sig`.
///
/// Read:
///   forge script .../RewardDistributorOps.s.sol:RewardDistributorOps --sig "status()" \
///     --rpc-url $RPC_URL -vvvv
///
/// Admin (DEFAULT_ADMIN_ROLE):
///   TARGET_APY_BPS=5000 \
///     forge script .../RewardDistributorOps.s.sol:RewardDistributorOps --sig "setConfig()" \
///     --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
///
///   REWARD_CURVE=0x... OPEN_TIER_REWARD_CURVE=0x... \
///     forge script .../RewardDistributorOps.s.sol:RewardDistributorOps --sig "setConfig()" \
///     --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
///
/// Anyone:
///   VALIDATOR=0x... \
///     forge script .../RewardDistributorOps.s.sol:RewardDistributorOps --sig "claim()" \
///     --rpc-url $RPC_URL --broadcast --private-key $CALLER_PK -vvvv
///
/// Validator:
///   RECIPIENT=0x... \
///     forge script .../RewardDistributorOps.s.sol:RewardDistributorOps --sig "setRecipient()" \
///     --rpc-url $RPC_URL --broadcast --private-key $VALIDATOR_PK -vvvv
contract RewardDistributorOps is InteractionScript {
    RewardDistributor rd;

    function _setUp() internal override {
        rd = RewardDistributor(deployments.RewardDistributor);
    }

    function run() public {
        status();
    }

    // ── READ ────────────────────────────────────────────────────────────

    /// @notice Print wiring, balances, APY, and per-validator pending (if VALIDATOR set).
    function status() public {
        address admin = vm.envOr("ADMIN", deployments.admin);

        console2.log("========== RewardDistributor Status ==========");
        console2.log("Contract:", address(rd));

        logSection("Wiring");
        console2.log("RLS Token:        ", rd.rlsToken());
        console2.log("FeeAggregator:    ", rd.feeAggregator());
        console2.log("ConsensusRegistry:", rd.consensusRegistry());
        console2.log("DelegationPool:   ", rd.delegationPool());
        console2.log("Accumulator:      ", rd.accumulator());

        logSection("Roles");
        console2.log("Admin:", admin);
        console2.log("  DEFAULT_ADMIN:", rd.hasRole(0x00, admin));
        console2.log("  UPGRADER:     ", rd.hasRole(rd.UPGRADER_ROLE(), admin));

        logSection("APY Config");
        console2.log("Target APY BPS (Track A):    ", rd.targetApyBps());
        console2.log("Open-tier APY BPS (Track B):", rd.openTierTargetApyBps());
        console2.log("RewardCurve (Track A):       ", rd.rewardCurve());
        console2.log("Open-tier RewardCurve (B):   ", rd.openTierRewardCurve());

        logSection("Balances");
        console2.log("Native:", address(rd).balance);
        console2.log("RLS:   ", balanceOf(rls, address(rd)));

        logSection("Pending");
        console2.log("Total pending:", rd.totalPendingRewards());

        address validator = vm.envOr("VALIDATOR", address(0));
        if (validator != address(0)) {
            logSection("Validator detail");
            console2.log("Validator:       ", validator);
            console2.log("Pending rewards: ", rd.getPendingRewards(validator));
            console2.log("Reward recipient:", rd.getRewardRecipient(validator));
        }
    }

    // ── ADMIN ───────────────────────────────────────────────────────────

    struct ConfigInputs {
        address feeAggregator;
        address delegationPool;
        address consensusRegistry;
        address accumulator;
        uint256 targetApyBps;
        uint256 openTierTargetApyBps;
        address rewardCurve;
        address openTierRewardCurve;
        bool updateFeeAggregator;
        bool updateDelegationPool;
        bool updateConsensusRegistry;
        bool updateAccumulator;
        bool updateApy;
        bool updateOpenTierApy;
        bool updateRewardCurve;
        bool updateOpenTierRewardCurve;
    }

    /// @notice Update wiring + APY target(s) + RewardCurve(s). Pass any subset of env vars.
    /// Env vars: FEE_AGGREGATOR, DELEGATION_POOL, CONSENSUS_REGISTRY, ACCUMULATOR, TARGET_APY_BPS,
    ///           OPEN_TIER_TARGET_APY_BPS, REWARD_CURVE, OPEN_TIER_REWARD_CURVE
    /// (ACCUMULATOR uses 0xdEaD as "not provided" sentinel since address(0) disables top-ups.
    ///  REWARD_CURVE / OPEN_TIER_REWARD_CURVE: address(0) is a valid input meaning "disable the
    ///  curve, fall back to the static bps" -- pass NOT_SET_ADDR (0xdEaD) to leave unchanged.)
    function setConfig() public {
        ConfigInputs memory inp = _readConfigInputs();
        require(
            inp.updateFeeAggregator || inp.updateDelegationPool || inp.updateConsensusRegistry
                || inp.updateAccumulator || inp.updateApy || inp.updateOpenTierApy
                || inp.updateRewardCurve || inp.updateOpenTierRewardCurve,
            "Set FEE_AGGREGATOR / DELEGATION_POOL / CONSENSUS_REGISTRY / ACCUMULATOR / "
            "TARGET_APY_BPS / OPEN_TIER_TARGET_APY_BPS / REWARD_CURVE / OPEN_TIER_REWARD_CURVE"
        );

        logSection("Current");
        console2.log("FeeAggregator:        ", rd.feeAggregator());
        console2.log("DelegationPool:       ", rd.delegationPool());
        console2.log("ConsensusRegistry:    ", rd.consensusRegistry());
        console2.log("Accumulator:          ", rd.accumulator());
        console2.log("Target APY BPS:       ", rd.targetApyBps());
        console2.log("Open-tier APY BPS:    ", rd.openTierTargetApyBps());
        console2.log("RewardCurve:          ", rd.rewardCurve());
        console2.log("Open-tier RewardCurve:", rd.openTierRewardCurve());

        if (inp.updateFeeAggregator) console2.log("New FeeAggregator:        ", inp.feeAggregator);
        if (inp.updateDelegationPool) console2.log("New DelegationPool:       ", inp.delegationPool);
        if (inp.updateConsensusRegistry) console2.log("New ConsensusRegistry:    ", inp.consensusRegistry);
        if (inp.updateAccumulator) console2.log("New Accumulator:          ", inp.accumulator);
        if (inp.updateApy) console2.log("New Target APY BPS:       ", inp.targetApyBps);
        if (inp.updateOpenTierApy) console2.log("New Open-tier APY BPS:    ", inp.openTierTargetApyBps);
        if (inp.updateRewardCurve) console2.log("New RewardCurve:          ", inp.rewardCurve);
        if (inp.updateOpenTierRewardCurve) console2.log("New Open-tier RewardCurve:", inp.openTierRewardCurve);

        vm.startBroadcast();
        if (inp.updateFeeAggregator) rd.setFeeAggregator(inp.feeAggregator);
        if (inp.updateDelegationPool) rd.setDelegationPool(inp.delegationPool);
        if (inp.updateConsensusRegistry) rd.setConsensusRegistry(inp.consensusRegistry);
        if (inp.updateAccumulator) rd.setAccumulator(inp.accumulator);
        if (inp.updateApy) rd.setTargetApyBps(inp.targetApyBps);
        if (inp.updateOpenTierApy) rd.setOpenTierTargetApyBps(inp.openTierTargetApyBps);
        if (inp.updateRewardCurve) rd.setRewardCurve(inp.rewardCurve);
        if (inp.updateOpenTierRewardCurve) rd.setOpenTierRewardCurve(inp.openTierRewardCurve);
        vm.stopBroadcast();

        if (inp.updateFeeAggregator) require(rd.feeAggregator() == inp.feeAggregator, "fa mismatch");
        if (inp.updateDelegationPool) require(rd.delegationPool() == inp.delegationPool, "dp mismatch");
        if (inp.updateConsensusRegistry) require(rd.consensusRegistry() == inp.consensusRegistry, "cr mismatch");
        if (inp.updateAccumulator) require(rd.accumulator() == inp.accumulator, "acc mismatch");
        if (inp.updateApy) require(rd.targetApyBps() == inp.targetApyBps, "apy mismatch");
        if (inp.updateOpenTierApy) {
            require(rd.openTierTargetApyBps() == inp.openTierTargetApyBps, "open-tier apy mismatch");
        }
        if (inp.updateRewardCurve) require(rd.rewardCurve() == inp.rewardCurve, "reward curve mismatch");
        if (inp.updateOpenTierRewardCurve) {
            require(rd.openTierRewardCurve() == inp.openTierRewardCurve, "open-tier reward curve mismatch");
        }

        console2.log("");
        console2.log("Done.");
    }

    function _readConfigInputs() internal view returns (ConfigInputs memory inp) {
        address NOT_SET_ADDR = 0x000000000000000000000000000000000000dEaD;
        uint256 NOT_SET_UINT = type(uint256).max;

        inp.feeAggregator = vm.envOr("FEE_AGGREGATOR", address(0));
        inp.delegationPool = vm.envOr("DELEGATION_POOL", address(0));
        inp.consensusRegistry = vm.envOr("CONSENSUS_REGISTRY", address(0));
        inp.accumulator = vm.envOr("ACCUMULATOR", NOT_SET_ADDR);
        inp.targetApyBps = vm.envOr("TARGET_APY_BPS", NOT_SET_UINT);
        inp.openTierTargetApyBps = vm.envOr("OPEN_TIER_TARGET_APY_BPS", NOT_SET_UINT);
        inp.rewardCurve = vm.envOr("REWARD_CURVE", NOT_SET_ADDR);
        inp.openTierRewardCurve = vm.envOr("OPEN_TIER_REWARD_CURVE", NOT_SET_ADDR);

        inp.updateFeeAggregator = inp.feeAggregator != address(0);
        inp.updateDelegationPool = inp.delegationPool != address(0);
        inp.updateConsensusRegistry = inp.consensusRegistry != address(0);
        inp.updateAccumulator = inp.accumulator != NOT_SET_ADDR;
        inp.updateApy = inp.targetApyBps != NOT_SET_UINT;
        inp.updateOpenTierApy = inp.openTierTargetApyBps != NOT_SET_UINT;
        inp.updateRewardCurve = inp.rewardCurve != NOT_SET_ADDR;
        inp.updateOpenTierRewardCurve = inp.openTierRewardCurve != NOT_SET_ADDR;
    }

    // ── ANYONE / VALIDATOR ──────────────────────────────────────────────

    /// @notice Claim a validator's pending RLS to its recipient. Anyone can call.
    /// Env vars: VALIDATOR
    function claim() public {
        address validator = vm.envAddress("VALIDATOR");
        address recipient = rd.getRewardRecipient(validator);

        console2.log("Validator:", validator);
        console2.log("Recipient:", recipient);

        uint256 pending = rd.getPendingRewards(validator);
        console2.log("Pending RLS:        ", pending);
        require(pending > 0, "Nothing to claim");
        console2.log("Recipient RLS before:", balanceOf(rls, recipient));

        vm.startBroadcast();
        rd.claimRewards(validator);
        vm.stopBroadcast();

        console2.log("");
        console2.log("Recipient RLS after:", balanceOf(rls, recipient));
    }

    /// @notice Validator sets a custom reward recipient. Pass RECIPIENT=0x0 to clear.
    /// Env vars: RECIPIENT
    function setRecipient() public {
        address recipient = vm.envAddress("RECIPIENT");
        console2.log("New recipient:", recipient);

        vm.startBroadcast();
        rd.setRewardRecipient(recipient);
        vm.stopBroadcast();

        console2.log("");
        console2.log("Effective recipient for", msg.sender, "is now", rd.getRewardRecipient(msg.sender));
    }
}
