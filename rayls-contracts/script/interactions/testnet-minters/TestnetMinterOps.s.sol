// SPDX-License-Identifier: BUSL-1.1
pragma solidity 0.8.26;

import { console2 } from "forge-std/Script.sol";
import { InteractionScript } from "../_base/InteractionScript.sol";
import { RLS } from "../../../src/token/RLS.sol";
import { NativeTokenController } from "../../../src/native/NativeTokenController.sol";

/// @title TestnetMinterOps
/// @notice TESTNET ONLY. Lets a testnet faucet mint USDr and RLS instead of handing out a balance that runs dry.
///         Grants (or revokes) MINTER_ROLE on the RLS token and on the NativeTokenController (USDr) for MINTER.
///         Every action refuses any chain but the Rayls testnet (7295799). Pick the action with `--sig`.
///
/// The two contracts have different admins on the testnet, so each grant is its own broadcast, signed by that
/// contract's DEFAULT_ADMIN_ROLE holder.
///
/// Read:
///   MINTER=0x... forge script .../TestnetMinterOps.s.sol:TestnetMinterOps --sig "status()" --rpc-url $RPC_URL
///
/// Grant (RLS admin, then NativeTokenController admin):
///   MINTER=0x... forge script .../TestnetMinterOps.s.sol:TestnetMinterOps --sig "grantRls()" \
///     --rpc-url $RPC_URL --broadcast --private-key $RLS_ADMIN_PK
///   MINTER=0x... forge script .../TestnetMinterOps.s.sol:TestnetMinterOps --sig "grantUsdr()" \
///     --rpc-url $RPC_URL --broadcast --private-key $NTC_ADMIN_PK
///
/// Revoke: the same with `revokeRls()` / `revokeUsdr()`.
contract TestnetMinterOps is InteractionScript {
    /// @notice Real Rayls testnet chain-id (RaylsNetwork::Testnet). Every action runs here only.
    uint256 internal constant TESTNET_CHAIN_ID = 7_295_799;

    RLS token;
    NativeTokenController ntc;
    address minter;

    function _setUp() internal override {
        token = RLS(deployments.RLS);
        ntc = NativeTokenController(deployments.NativeTokenController);
        minter = vm.envAddress("MINTER");
    }

    function run() public view {
        status();
    }

    /// @notice Whether MINTER can mint each token, and RLS's supply headroom under MAX_SUPPLY.
    function status() public view {
        _requireTestnet();
        console2.log("Minter:", minter);
        logSection("RLS");
        console2.log("Contract:     ", address(token));
        console2.log("MINTER_ROLE:  ", token.hasRole(token.MINTER_ROLE(), minter));
        console2.log("Total supply: ", token.totalSupply());
        console2.log("Headroom:     ", token.MAX_SUPPLY() - token.totalSupply());
        logSection("USDr (NativeTokenController)");
        console2.log("Contract:     ", address(ntc));
        console2.log("MINTER_ROLE:  ", ntc.isMinter(minter));
    }

    /// @notice Grants MINTER_ROLE on RLS. Broadcast by RLS's DEFAULT_ADMIN_ROLE holder.
    function grantRls() public {
        _requireTestnet();
        bytes32 role = token.MINTER_ROLE();
        if (token.hasRole(role, minter)) {
            console2.log("RLS: already a minter", minter);
            return;
        }
        vm.startBroadcast();
        token.grantRole(role, minter);
        vm.stopBroadcast();
        require(token.hasRole(role, minter), "RLS: grant did not take");
        console2.log("RLS: MINTER_ROLE granted to", minter);
    }

    /// @notice Grants MINTER_ROLE on the NativeTokenController (USDr). Broadcast by its DEFAULT_ADMIN_ROLE holder.
    function grantUsdr() public {
        _requireTestnet();
        if (ntc.isMinter(minter)) {
            console2.log("USDr: already a minter", minter);
            return;
        }
        vm.startBroadcast();
        ntc.addMinter(minter);
        vm.stopBroadcast();
        require(ntc.isMinter(minter), "USDr: grant did not take");
        console2.log("USDr: MINTER_ROLE granted to", minter);
    }

    /// @notice Revokes MINTER_ROLE on RLS.
    function revokeRls() public {
        _requireTestnet();
        vm.startBroadcast();
        token.revokeRole(token.MINTER_ROLE(), minter);
        vm.stopBroadcast();
        console2.log("RLS: MINTER_ROLE revoked from", minter);
    }

    /// @notice Revokes MINTER_ROLE on the NativeTokenController (USDr).
    function revokeUsdr() public {
        _requireTestnet();
        vm.startBroadcast();
        ntc.removeMinter(minter);
        vm.stopBroadcast();
        console2.log("USDr: MINTER_ROLE revoked from", minter);
    }

    function _requireTestnet() internal view {
        require(block.chainid == TESTNET_CHAIN_ID, "TESTNET ONLY: refusing to run outside the Rayls testnet (7295799)");
        require(minter != address(0), "MINTER is not set");
    }
}
