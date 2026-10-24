// SPDX-License-Identifier: BUSL-1.1
// Copyright (C) 2026 Rayls Core Ltd.
pragma solidity 0.8.26;

import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {ERC20Upgradeable} from "@openzeppelin/contracts-upgradeable/token/ERC20/ERC20Upgradeable.sol";
import {ERC20BurnableUpgradeable} from "@openzeppelin/contracts-upgradeable/token/ERC20/extensions/ERC20BurnableUpgradeable.sol";
import {ERC20PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/token/ERC20/extensions/ERC20PausableUpgradeable.sol";
import {AccessControlUpgradeable} from "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import {ERC20PermitUpgradeable} from "@openzeppelin/contracts-upgradeable/token/ERC20/extensions/ERC20PermitUpgradeable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

/**
 * @title RLS (v2)
 * @author Rayls Core Ltd.
 * @notice Rayls ERC-20 governance and staking token
 *
 * @dev Adapted from RaylsTokenBridgedV2 (rayls-token repo) for the Rayls L1 network.
 *      - MINTER_ROLE: mint tokens (bridge inbound, OFT adapter)
 *      - BURNER_ROLE: burn tokens without allowance (bridge outbound, OFT adapter)
 *      - Bridge operations (mint/burn by MINTER_ROLE or BURNER_ROLE) bypass pause
 *      - Regular transfers are blocked when paused
 *      - MAX_SUPPLY hard cap: 10 billion RLS
 *      - UUPS upgradeable with role-based access control
 *      - ERC-20Permit (EIP-2612) for gasless approvals
 *
 * @custom:security-contact secops@parfin.io
 * @custom:upgrade-policy Upgrades require UPGRADER_ROLE with recommended timelock
 */
// RLS is the ERC-20, not the native-coin controller: mint/burn/burnFrom only share names with
// INativeTokenController (return types differ).
// slither-disable-next-line missing-inheritance
contract RLS is
    Initializable,
    ERC20BurnableUpgradeable,
    ERC20PausableUpgradeable,
    AccessControlUpgradeable,
    ERC20PermitUpgradeable,
    UUPSUpgradeable
{
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");
    bytes32 public constant BURNER_ROLE = keccak256("BURNER_ROLE");

    /// @notice Maximum token supply (10 billion tokens with 18 decimals)
    uint256 public constant MAX_SUPPLY = 10_000_000_000 ether;

    /// @notice Whether bridge mint/burn operations are paused independently of regular pause.
    /// @dev Allows halting bridge operations during a bridge compromise without affecting user transfers.
    bool public bridgePaused;

    event TokensMinted(address indexed receiver, uint256 amount);
    event BridgePaused();
    event BridgeUnpaused();

    error InvalidAddress();
    error ZeroAmount();
    error MaxSupplyExceeded();
    error BridgeOperationsPaused();

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    /**
     * @notice V1 initializer — called at genesis. Should not be called again on upgrade.
     * @param admin Address receiving DEFAULT_ADMIN_ROLE and all other roles
     * @param treasury Address receiving the initial token supply
     * @param initialSupply Amount minted to treasury at genesis
     */
    function initialize(address admin, address treasury, uint256 initialSupply) public initializer {
        if (admin == address(0)) revert InvalidAddress();
        if (treasury == address(0)) revert InvalidAddress();

        __UUPSUpgradeable_init();
        __ERC20_init("Rayls", "RLS");
        __ERC20Burnable_init();
        __ERC20Pausable_init();
        __AccessControl_init();
        __ERC20Permit_init("Rayls");

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(PAUSER_ROLE, admin);
        _grantRole(UPGRADER_ROLE, admin);
        _grantRole(MINTER_ROLE, admin);
        _grantRole(BURNER_ROLE, admin);

        if (initialSupply > 0) {
            if (initialSupply > MAX_SUPPLY) revert MaxSupplyExceeded();
            _mint(treasury, initialSupply);
        }
    }

    /// @notice V2 reinitializer — called after upgrade to set new state if needed
    function initializeV2() public reinitializer(2) onlyRole(DEFAULT_ADMIN_ROLE) {
        // No new storage in V2. Pattern is here for future upgrades.
    }

    /**
     * @notice Mint tokens to the receiver address
     * @dev Only callable by MINTER_ROLE. Enforces MAX_SUPPLY cap.
     * @dev Bypasses pause to prevent stuck cross-chain funds.
     * @param receiver Address receiving the minted tokens
     * @param amount Amount of tokens to mint
     */
    function mint(address receiver, uint256 amount) public onlyRole(MINTER_ROLE) {
        if (receiver == address(0)) revert InvalidAddress();
        if (amount == 0) revert ZeroAmount();
        if (totalSupply() + amount > MAX_SUPPLY) revert MaxSupplyExceeded();

        _mint(receiver, amount);
        emit TokensMinted(receiver, amount);
    }

    /**
     * @notice Burns tokens from the caller's account
     * @param value Amount of tokens to burn
     */
    function burn(uint256 value) public override {
        if (value == 0) revert ZeroAmount();
        super.burn(value);
    }

    /**
     * @notice Burns tokens from a specified account
     * @dev MINTER_ROLE and BURNER_ROLE can burn without allowance (bridge operations).
     *      Other callers require standard ERC-20 allowance.
     * @dev Bypasses pause when called by MINTER_ROLE or BURNER_ROLE.
     * @param account Account to burn tokens from
     * @param value Amount of tokens to burn
     */
    function burnFrom(address account, uint256 value) public override {
        if (value == 0) revert ZeroAmount();

        // Bridge roles burn without allowance
        if (hasRole(MINTER_ROLE, msg.sender) || hasRole(BURNER_ROLE, msg.sender)) {
            _burn(account, value);
        } else {
            super.burnFrom(account, value);
        }
    }

    function pause() public onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() public onlyRole(PAUSER_ROLE) {
        _unpause();
    }

    /// @notice Pause bridge mint/burn operations independently of regular transfers (RLS-002).
    function pauseBridge() external onlyRole(PAUSER_ROLE) {
        bridgePaused = true;
        emit BridgePaused();
    }

    /// @notice Unpause bridge operations.
    function unpauseBridge() external onlyRole(PAUSER_ROLE) {
        bridgePaused = false;
        emit BridgeUnpaused();
    }

    function version() public pure returns (string memory) {
        return "2.0.0";
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    /**
     * @dev Internal token update with two independent pause controls:
     *      - paused(): blocks regular user transfers
     *      - bridgePaused: blocks bridge mint/burn operations (RLS-002)
     *      Bridge operations bypass regular pause to prevent stuck cross-chain funds,
     *      but can be halted independently via pauseBridge() during a bridge compromise.
     */
    function _update(address from, address to, uint256 value)
        internal
        override(ERC20Upgradeable, ERC20PausableUpgradeable)
    {
        bool isBridgeMint = (from == address(0) && hasRole(MINTER_ROLE, msg.sender));
        bool isBridgeBurn = (to == address(0) && (hasRole(MINTER_ROLE, msg.sender) || hasRole(BURNER_ROLE, msg.sender)));
        bool isBridgeOperation = isBridgeMint || isBridgeBurn;

        if (isBridgeOperation) {
            if (bridgePaused) revert BridgeOperationsPaused();
        } else if (paused()) {
            revert EnforcedPause();
        }

        ERC20Upgradeable._update(from, to, value);
    }

    /// @dev Resolves nonces diamond conflict between ERC20PermitUpgradeable and IERC20Permit
    function nonces(address owner)
        public
        view
        override(ERC20PermitUpgradeable)
        returns (uint256)
    {
        return super.nonces(owner);
    }

    // Storage gap reserved for future upgrades.
    // slither-disable-next-line unused-state
    uint256[49] private __gap;
}
