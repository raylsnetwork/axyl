// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.20;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {INativeTokenController} from "../interfaces/INativeTokenController.sol";
import {IMintableBurnable} from "../interfaces/IMintableBurnable.sol";

/// @title NativeTokenController
/// @notice Access-controlled gateway for minting and burning the native gas token.
/// @dev Sits between whitelisted accounts and the native ERC-20 precompile at 0x0400.
///      The Rust EVM precompile checks `msg.sender == MINTING_MODULE_ADDRESS`, so this
///      contract's address must be set as MINTING_MODULE_ADDRESS in the Rust code.
///      Implements IMintableBurnable for LayerZero OFT compatibility.
///      UUPS upgradeable with AccessControl.
contract NativeTokenController is
    Initializable,
    UUPSUpgradeable,
    AccessControlUpgradeable,
    IMintableBurnable
{
    // ── Roles ──────────────────────────────────────────────────────────
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    /// @notice The native ERC-20 precompile
    INativeTokenController public constant PRECOMPILE =
        INativeTokenController(0x0000000000000000000000000000000000000400);

    // ── Events ─────────────────────────────────────────────────────────
    event Minted(address indexed caller, address indexed to, uint256 amount);
    event Burned(address indexed caller, address indexed from, uint256 amount);

    // ── Errors ─────────────────────────────────────────────────────────
    error PrecompileMintFailed(address to, uint256 amount);
    error PrecompileBurnFailed(address from, uint256 amount);
    error ZeroAddress();
    error ZeroAmount();

    // ── Constructor (disable initializers) ───────────────────────────
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    // ── Initializer ─────────────────────────────────────────────────
    function initialize(address admin) external initializer {
        if (admin == address(0)) revert ZeroAddress();

        __AccessControl_init();
        __UUPSUpgradeable_init();

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(UPGRADER_ROLE, admin);
    }

    // ── UUPS authorization ──────────────────────────────────────────
    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    // ── IMintableBurnable impl ─────────────────────────────────────────

    /// @notice Mint native tokens to `_to`.
    /// @dev Only addresses with MINTER_ROLE (e.g. the LZ OFT contract) may call.
    function mint(address _to, uint256 _amount)
        external
        override
        onlyRole(MINTER_ROLE)
        returns (bool)
    {
        if (_to == address(0)) revert ZeroAddress();
        if (_amount == 0) revert ZeroAmount();

        bool ok = PRECOMPILE.mint(_to, _amount);
        if (!ok) revert PrecompileMintFailed(_to, _amount);

        emit Minted(msg.sender, _to, _amount);
        return true;
    }

    /// @notice Burn native tokens from `_from`.
    /// @dev Only addresses with MINTER_ROLE may call.
    ///      Uses burnFrom on the precompile — `_from` must have granted allowance
    ///      to this contract (the NativeTokenController) on the precompile at 0x0400.
    function burn(address _from, uint256 _amount)
        external
        override
        onlyRole(MINTER_ROLE)
        returns (bool)
    {
        if (_from == address(0)) revert ZeroAddress();
        if (_amount == 0) revert ZeroAmount();

        bool ok = PRECOMPILE.burnFrom(_from, _amount);
        if (!ok) revert PrecompileBurnFailed(_from, _amount);

        emit Burned(msg.sender, _from, _amount);
        return true;
    }

    // ── Storage gap for upgrade safety ──────────────────────────────────
    // Storage gap reserved for future upgrades.
    // slither-disable-next-line unused-state
    uint256[50] private __gap;

    // ── Role management helpers (callable by DEFAULT_ADMIN_ROLE) ───────

    /// @notice Grant MINTER_ROLE to an address (e.g. the deployed LZ OFT contract).
    function addMinter(address account) external onlyRole(DEFAULT_ADMIN_ROLE) {
        if (account == address(0)) revert ZeroAddress();
        _grantRole(MINTER_ROLE, account);
    }

    /// @notice Revoke MINTER_ROLE from an address.
    function removeMinter(address account) external onlyRole(DEFAULT_ADMIN_ROLE) {
        _revokeRole(MINTER_ROLE, account);
    }

    /// @notice Convenience view — check if an address is an active minter.
    function isMinter(address account) external view returns (bool) {
        return hasRole(MINTER_ROLE, account);
    }
}
