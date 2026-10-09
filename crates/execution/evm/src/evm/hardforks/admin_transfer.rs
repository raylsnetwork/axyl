//! AdminTransfer hardfork: transfer AccessControl roles from the old admin to a new admin
//! on NativeTokenController and FeeAggregator via direct storage slot overrides, and migrate
//! DelegationPool and RewardDistributor from non-upgradeable Ownable contracts to UUPS
//! upgradeable proxies with AccessControl.
//!
//! ## Role transfers (NativeTokenController, FeeAggregator)
//!
//! Each role transfer writes two storage slots per contract:
//! 1. Revoke: `_roles[role].members[oldAdmin] = false`
//! 2. Grant:  `_roles[role].members[newAdmin] = true`
//!
//! ## Proxy migration (DelegationPool, RewardDistributor)
//!
//! Each proxy migration:
//! 1. Deploys new implementation bytecode at a new precompile address
//! 2. Replaces the contract bytecode with ERC1967Proxy (minimal delegatecall forwarder)
//! 3. Clears old Ownable storage slots
//! 4. Writes ERC-7201 namespaced storage (contract state, AccessControl roles, ERC1967 impl slot)
//! 5. Marks the contract as initialized (OpenZeppelin Initializable)
//!
//! Storage slots are pre-computed from OpenZeppelin's `AccessControlUpgradeable` ERC-7201
//! namespaced storage layout and verified against testnet state.

use alloy::primitives::address;
use rayls_infrastructure_types::{Address, U256};
use reth_revm::{
    primitives::HashMap,
    state::{Account as RevmAccount, AccountStatus, EvmStorageSlot},
};

/// Old admin address (inaccessible Foundry deployer).
const OLD_ADMIN: Address = address!("c1612C97537c2CC62a11FC4516367AB6F62d4B23");
/// New admin for NativeTokenController and FeeAggregator.
const NEW_ADMIN: Address = address!("8D93de1eC49F212C77E969a5723e18cAc22083EF");
/// Admin for DelegationPool and RewardDistributor (current testnet owner).
const POOL_ADMIN: Address = address!("91ec7A2be07A79D2eAB99135553b26F706099e9D");

// ── Contract addresses ────────────────────────────────────────────────

/// NativeTokenController proxy address.
const NATIVE_TOKEN_CONTROLLER: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e6");
/// FeeAggregator proxy address.
const FEE_AGGREGATOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e3");

/// DelegationPool proxy address (currently non-proxy, will become proxy).
const DELEGATION_POOL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e2");
/// DelegationPool implementation address (new).
const DELEGATION_POOL_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e9");

/// RewardDistributor proxy address (currently non-proxy, will become proxy).
const REWARD_DISTRIBUTOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e5");
/// RewardDistributor implementation address (new).
const REWARD_DISTRIBUTOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e8");

/// RLS ERC-20 token proxy address.
const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
/// RLS ERC-20 token implementation address.
const RLS_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17eb");
/// ConsensusRegistry address.
const CONSENSUS_REGISTRY: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e1");

// ── Common constants ──────────────────────────────────────────────────

/// Storage value for `true` (role granted).
const VALUE_TRUE: U256 = U256::from_limbs([1, 0, 0, 0]);
#[allow(unused)]
/// Storage value for `false` (role revoked).
const VALUE_FALSE: U256 = U256::ZERO;

/// ERC-1967 implementation storage slot.
/// `keccak256("eip1967.proxy.implementation") - 1`
const ERC1967_IMPL_SLOT: U256 = U256::from_be_slice(&[
    0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9, 0x8d,
    0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38, 0x2b, 0xbc,
]);

/// OpenZeppelin Initializable storage slot (ERC-7201 namespace
/// `openzeppelin.storage.Initializable`).
/// `0xf0c57e16840df040f15088dc2f81fe391c3923bec73e23a9662efc9c229c6a00`
const INITIALIZABLE_SLOT: U256 = U256::from_be_slice(&[
    0xf0, 0xc5, 0x7e, 0x16, 0x84, 0x0d, 0xf0, 0x40, 0xf1, 0x50, 0x88, 0xdc, 0x2f, 0x81, 0xfe, 0x39,
    0x1c, 0x39, 0x23, 0xbe, 0xc7, 0x3e, 0x23, 0xa9, 0x66, 0x2e, 0xfc, 0x9c, 0x22, 0x9c, 0x6a, 0x00,
]);

/// Value for `_initialized = 1` (uint64, stored in lower 8 bytes of the slot).
const INITIALIZED_V1: U256 = U256::from_limbs([1, 0, 0, 0]);

// ── AccessControl storage slots ───────────────────────────────────────
//
// Layout (ERC-7201 namespace `openzeppelin.storage.AccessControl`):
//   base = 0x02dd7bc7dec4dceedda775e58dd541e08a116c6c53815c0bd028192f7b626800
//   _roles[role].members[account] = keccak256(abi.encode(account, keccak256(abi.encode(role,
// base))))
//
// All slots verified against testnet storage reads.

// ── Slots for OLD_ADMIN (0xc1612C...) on NativeTokenController & FeeAggregator ──

#[allow(unused)]
// DEFAULT_ADMIN_ROLE: _roles[0x00..00].members[oldAdmin]
const DEFAULT_ADMIN_OLD: U256 = U256::from_be_slice(&[
    0x5d, 0x76, 0xd9, 0xdf, 0xcd, 0xa8, 0x5e, 0xbf, 0x2b, 0x43, 0x79, 0x87, 0x17, 0xd2, 0x1a, 0x7a,
    0x5c, 0x51, 0x44, 0x7d, 0x87, 0xab, 0x25, 0x5d, 0x68, 0x0b, 0xf9, 0xe3, 0x77, 0x2a, 0xe4, 0xae,
]);
// DEFAULT_ADMIN_ROLE: _roles[0x00..00].members[newAdmin (0x8D93de...)]
const DEFAULT_ADMIN_NEW: U256 = U256::from_be_slice(&[
    0x63, 0x52, 0xfa, 0x54, 0x6e, 0xef, 0x3d, 0x71, 0xf4, 0x3f, 0x19, 0x14, 0x94, 0x8e, 0x03, 0x27,
    0xb7, 0x1e, 0xf7, 0xd9, 0x3d, 0xc3, 0xcf, 0x57, 0x5b, 0xdf, 0x96, 0xe9, 0xaf, 0x4d, 0x91, 0x44,
]);

#[allow(unused)]
// UPGRADER_ROLE: _roles[keccak256("UPGRADER_ROLE")].members[oldAdmin]
const UPGRADER_OLD: U256 = U256::from_be_slice(&[
    0x2c, 0xd8, 0x62, 0x7c, 0xce, 0x66, 0x9a, 0xf2, 0xd4, 0xa0, 0x6c, 0xf3, 0x1d, 0x48, 0x69, 0x1d,
    0x6a, 0xec, 0x96, 0x8f, 0x2f, 0x20, 0x5c, 0x6c, 0x1d, 0x2f, 0x47, 0x21, 0x98, 0x37, 0xa8, 0xfa,
]);
// UPGRADER_ROLE: _roles[keccak256("UPGRADER_ROLE")].members[newAdmin (0x8D93de...)]
const UPGRADER_NEW: U256 = U256::from_be_slice(&[
    0x5e, 0x34, 0x1a, 0xa9, 0xc8, 0x56, 0xf4, 0x62, 0xa8, 0xf1, 0xf1, 0x64, 0xa3, 0xd7, 0x8f, 0x4b,
    0x66, 0x49, 0x3d, 0x9d, 0xee, 0x72, 0x94, 0x79, 0x28, 0xaf, 0x85, 0x4a, 0x88, 0xfa, 0x7f, 0xcc,
]);

#[allow(unused)]
// KEEPER_ROLE: _roles[keccak256("KEEPER_ROLE")].members[oldAdmin]
const KEEPER_OLD: U256 = U256::from_be_slice(&[
    0x76, 0xcf, 0x1b, 0x2d, 0x75, 0x5d, 0x0f, 0x7b, 0xe8, 0x43, 0x60, 0x46, 0x95, 0x53, 0x8b, 0x03,
    0xe5, 0x4c, 0x9b, 0xb0, 0xa9, 0xb8, 0xf8, 0xfb, 0x02, 0x7f, 0x01, 0x4d, 0x0c, 0x91, 0xbb, 0x7a,
]);
// KEEPER_ROLE: _roles[keccak256("KEEPER_ROLE")].members[newAdmin]
const KEEPER_NEW: U256 = U256::from_be_slice(&[
    0xd7, 0xde, 0xf7, 0xb8, 0x43, 0xbd, 0xa3, 0xe6, 0xf2, 0xbf, 0x5e, 0x51, 0xa7, 0xc3, 0xc5, 0x76,
    0x71, 0x71, 0x19, 0x49, 0x03, 0x9d, 0x5c, 0xd2, 0xcd, 0x85, 0xaf, 0x90, 0x53, 0x83, 0x86, 0x62,
]);

#[allow(unused)]
// PAUSER_ROLE: _roles[keccak256("PAUSER_ROLE")].members[oldAdmin]
const PAUSER_OLD: U256 = U256::from_be_slice(&[
    0xf5, 0x7c, 0xb1, 0xab, 0x6d, 0x56, 0x8c, 0xd6, 0x68, 0x2d, 0xe8, 0x7c, 0x53, 0xa6, 0x0d, 0x49,
    0xc3, 0xfa, 0x18, 0xa6, 0x9c, 0x44, 0x6c, 0x64, 0x94, 0x11, 0x0c, 0x5d, 0xda, 0xdb, 0xbd, 0x0f,
]);
// PAUSER_ROLE: _roles[keccak256("PAUSER_ROLE")].members[newAdmin]
const PAUSER_NEW: U256 = U256::from_be_slice(&[
    0xc8, 0x77, 0xd6, 0xf0, 0x81, 0xb7, 0xdb, 0x2e, 0x10, 0x78, 0x1a, 0xd5, 0x04, 0x0a, 0xd9, 0x6f,
    0xcb, 0x16, 0xa4, 0xc7, 0x6d, 0x79, 0x5e, 0x1f, 0xbe, 0xb5, 0x10, 0xc9, 0xd9, 0x82, 0xd9, 0x7a,
]);

// ── Slots for POOL_ADMIN (0x91ec7A...) on DelegationPool & RewardDistributor ──

// DEFAULT_ADMIN_ROLE: _roles[0x00..00].members[poolAdmin (0x91ec7A...)]
// keccak256(abi.encode(0x91ec7A2be07A79D2eAB99135553b26F706099e9D, keccak256(abi.encode(0x00,
// OZ_AC_BASE))))
const DEFAULT_ADMIN_POOL: U256 = U256::from_be_slice(&[
    0x18, 0x60, 0x6a, 0xdf, 0x89, 0x45, 0xd7, 0x3b, 0xd9, 0xdd, 0x10, 0xd1, 0x4a, 0xc3, 0xc2, 0x94,
    0x5c, 0x07, 0x8a, 0xb7, 0xd7, 0x2c, 0x86, 0x32, 0xbd, 0xa6, 0x28, 0x34, 0x7c, 0x18, 0x1a, 0xdc,
]);

// UPGRADER_ROLE: _roles[keccak256("UPGRADER_ROLE")].members[poolAdmin (0x91ec7A...)]
// keccak256(abi.encode(0x91ec7A2be07A79D2eAB99135553b26F706099e9D,
// keccak256(abi.encode(UPGRADER_ROLE, OZ_AC_BASE))))
const UPGRADER_POOL: U256 = U256::from_be_slice(&[
    0x46, 0x87, 0xe7, 0xfc, 0xf6, 0xeb, 0x36, 0x25, 0xad, 0xda, 0xcb, 0x3f, 0xb4, 0x0e, 0xdc, 0xb2,
    0x88, 0x03, 0x6d, 0xda, 0x6f, 0x4a, 0x77, 0xcb, 0x31, 0x03, 0x6f, 0xa4, 0x6f, 0x8d, 0x2d, 0x75,
]);

// ── ERC-7201 storage bases ────────────────────────────────────────────

/// DelegationPool ERC-7201 base: `delegationpool.storage.v1`
/// `0x88221c9a15d56692c82fe5e6f956bdf53eb61854017aba5bacf6b40976119e00`
const DP_STORAGE_BASE: U256 = U256::from_be_slice(&[
    0x88, 0x22, 0x1c, 0x9a, 0x15, 0xd5, 0x66, 0x92, 0xc8, 0x2f, 0xe5, 0xe6, 0xf9, 0x56, 0xbd, 0xf5,
    0x3e, 0xb6, 0x18, 0x54, 0x01, 0x7a, 0xba, 0x5b, 0xac, 0xf6, 0xb4, 0x09, 0x76, 0x11, 0x9e, 0x00,
]);

/// RewardDistributor ERC-7201 base: `rewarddistributor.storage.v1`
/// `0x8a40cc0ccf5a2d030058c860d76601e04104947950ec7475e80ab15a7d69d600`
const RD_STORAGE_BASE: U256 = U256::from_be_slice(&[
    0x8a, 0x40, 0xcc, 0x0c, 0xcf, 0x5a, 0x2d, 0x03, 0x00, 0x58, 0xc8, 0x60, 0xd7, 0x66, 0x01, 0xe0,
    0x41, 0x04, 0x94, 0x79, 0x50, 0xec, 0x74, 0x75, 0xe8, 0x0a, 0xb1, 0x5a, 0x7d, 0x69, 0xd6, 0x00,
]);

// ── Bytecodes (loaded from forge build artifacts) ─────────────────────

/// ERC1967Proxy deployed bytecode (minimal delegatecall forwarder).
/// Pinned to the AdminTransfer-introduction commit so historical replay is deterministic.
const ERC1967_PROXY_BYTECODE: &[u8] = include_bytes!("bytecodes/admin_transfer/erc1967_proxy.bin");

/// DelegationPool implementation deployed bytecode.
/// Pinned to the AdminTransfer-introduction commit so historical replay is deterministic.
const DELEGATION_POOL_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/admin_transfer/delegation_pool_impl.bin");

/// RewardDistributor implementation deployed bytecode.
/// Pinned to the AdminTransfer-introduction commit so historical replay is deterministic.
const REWARD_DISTRIBUTOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/admin_transfer/reward_distributor_impl.bin");

// ── Testnet config values for DelegationPool ──────────────────────────
// These are the current values in storage slots 1-4 on testnet.

/// config.minDelegation = 100e18 (100 RLS)
const DP_MIN_DELEGATION: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x6b, 0xc7, 0x5e, 0x2d, 0x63, 0x10, 0x00, 0x00,
]);
/// config.maxDelegation = 10_000_000e18 (10M RLS)
const DP_MAX_DELEGATION: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x45, 0x95, 0x16, 0x14, 0x01, 0x48, 0x4a, 0x00, 0x00, 0x00,
]);
/// config.maxValidatorDelegation = 100_000_000e18 (100M RLS)
const DP_MAX_VALIDATOR_DELEGATION: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00,
]);
/// config.unbondingEpochs (uint32 = 7) + config.commissionDelayEpochs (uint32 = 7) packed
const DP_CONFIG_PACKED: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x07,
]);

/// Helper: left-pad an address to 32 bytes as a U256.
fn addr_to_u256(addr: Address) -> U256 {
    let mut buf = [0u8; 32];
    buf[12..].copy_from_slice(addr.as_slice());
    U256::from_be_bytes(buf)
}

use super::account_with_code;

/// Insert a storage slot override into an account.
fn set_slot(account: &mut RevmAccount, slot: U256, value: U256) {
    account.storage.insert(slot, EvmStorageSlot::new_changed(U256::ZERO, value, 0));
}

/// Clear a storage slot (set to zero from a non-zero previous value).
fn clear_slot(account: &mut RevmAccount, slot: U256, old_value: U256) {
    account.storage.insert(slot, EvmStorageSlot::new_changed(old_value, U256::ZERO, 0));
}

/// Build the revm state map for the admin transfer hardfork, ready to be committed
/// via `self.evm.db_mut().commit(state)`.
///
/// This handles:
/// 1. AccessControl role transfers on NativeTokenController and FeeAggregator
/// 2. Proxy migration of DelegationPool and RewardDistributor
pub(crate) fn admin_transfer_state() -> HashMap<Address, RevmAccount> {
    let mut state: HashMap<Address, RevmAccount> = HashMap::default();

    // ── Part 1: Grant roles to new admin (no revocation of old admin) ──
    //
    // Only grant roles — do not revoke old admin roles.  Old roles remain
    // active so existing operational tooling keeps working until the old
    // admin voluntarily renounces via `renounceRole()`.

    let role_grants: [(Address, &[U256]); 2] = [
        (NATIVE_TOKEN_CONTROLLER, &[DEFAULT_ADMIN_NEW, UPGRADER_NEW]),
        (FEE_AGGREGATOR, &[DEFAULT_ADMIN_NEW, UPGRADER_NEW, KEEPER_NEW, PAUSER_NEW]),
    ];

    for (contract, slots) in role_grants {
        // Explicitly include proxy bytecode so it is preserved in the state transition.
        // Without this, the account info ends up with code=None after cache pre-load,
        // and CacheAccount::change() clears the bytecode.
        let mut account = account_with_code(ERC1967_PROXY_BYTECODE);
        for &slot in slots {
            set_slot(&mut account, slot, VALUE_TRUE);
        }
        state.insert(contract, account);
    }

    // ── Part 2: Deploy implementation bytecodes ───────────────────────

    state.insert(DELEGATION_POOL_IMPL, account_with_code(DELEGATION_POOL_IMPL_BYTECODE));
    state.insert(REWARD_DISTRIBUTOR_IMPL, account_with_code(REWARD_DISTRIBUTOR_IMPL_BYTECODE));

    // ── Part 3: Migrate DelegationPool to proxy ───────────────────────

    {
        let mut dp = account_with_code(ERC1967_PROXY_BYTECODE);

        // Clear old Ownable storage
        clear_slot(&mut dp, U256::ZERO, addr_to_u256(POOL_ADMIN)); // slot 0: _owner
        clear_slot(&mut dp, U256::from(1), DP_MIN_DELEGATION); // slot 1: config.minDelegation
        clear_slot(&mut dp, U256::from(2), DP_MAX_DELEGATION); // slot 2: config.maxDelegation
        clear_slot(&mut dp, U256::from(3), DP_MAX_VALIDATOR_DELEGATION); // slot 3: config.
                                                                         // maxValidatorDelegation
        clear_slot(&mut dp, U256::from(4), DP_CONFIG_PACKED); // slot 4: config packed epochs

        // ERC-1967 implementation slot → DelegationPoolImpl
        set_slot(&mut dp, ERC1967_IMPL_SLOT, addr_to_u256(DELEGATION_POOL_IMPL));

        // Initializable: _initialized = 1
        set_slot(&mut dp, INITIALIZABLE_SLOT, INITIALIZED_V1);

        // AccessControl: grant DEFAULT_ADMIN_ROLE and UPGRADER_ROLE to POOL_ADMIN
        set_slot(&mut dp, DEFAULT_ADMIN_POOL, VALUE_TRUE);
        set_slot(&mut dp, UPGRADER_POOL, VALUE_TRUE);

        // ERC-7201 namespaced storage (base = DP_STORAGE_BASE)
        // +0: rls (address)
        set_slot(&mut dp, DP_STORAGE_BASE, addr_to_u256(RLS_TOKEN));
        // +1: consensusRegistry (address)
        set_slot(&mut dp, DP_STORAGE_BASE + U256::from(1), addr_to_u256(CONSENSUS_REGISTRY));
        // +2: config.minDelegation
        set_slot(&mut dp, DP_STORAGE_BASE + U256::from(2), DP_MIN_DELEGATION);
        // +3: config.maxDelegation
        set_slot(&mut dp, DP_STORAGE_BASE + U256::from(3), DP_MAX_DELEGATION);
        // +4: config.maxValidatorDelegation
        set_slot(&mut dp, DP_STORAGE_BASE + U256::from(4), DP_MAX_VALIDATOR_DELEGATION);
        // +5: config packed (unbondingEpochs + commissionDelayEpochs)
        set_slot(&mut dp, DP_STORAGE_BASE + U256::from(5), DP_CONFIG_PACKED);
        // +6 through +8: mappings (validatorPools, poolRegistered, positions) — zero by default
        // +9: rewardDistributor — zero on testnet

        state.insert(DELEGATION_POOL, dp);
    }

    // ── Part 4: Migrate RewardDistributor to proxy ────────────────────

    {
        let mut rd = account_with_code(ERC1967_PROXY_BYTECODE);

        // Clear old Ownable storage
        clear_slot(&mut rd, U256::ZERO, addr_to_u256(POOL_ADMIN)); // slot 0: _owner
        clear_slot(&mut rd, U256::from(1), addr_to_u256(FEE_AGGREGATOR)); // slot 1: feeAggregator
        clear_slot(&mut rd, U256::from(2), addr_to_u256(CONSENSUS_REGISTRY)); // slot 2: _consensusRegistry
        clear_slot(&mut rd, U256::from(3), addr_to_u256(DELEGATION_POOL)); // slot 3: _delegationPool

        // ERC-1967 implementation slot → RewardDistributorImpl
        set_slot(&mut rd, ERC1967_IMPL_SLOT, addr_to_u256(REWARD_DISTRIBUTOR_IMPL));

        // Initializable: _initialized = 1
        set_slot(&mut rd, INITIALIZABLE_SLOT, INITIALIZED_V1);

        // AccessControl: grant DEFAULT_ADMIN_ROLE and UPGRADER_ROLE to POOL_ADMIN
        set_slot(&mut rd, DEFAULT_ADMIN_POOL, VALUE_TRUE);
        set_slot(&mut rd, UPGRADER_POOL, VALUE_TRUE);

        // ERC-7201 namespaced storage (base = RD_STORAGE_BASE)
        // +0: rls (address)
        set_slot(&mut rd, RD_STORAGE_BASE, addr_to_u256(RLS_TOKEN));
        // +1: feeAggregator (address)
        set_slot(&mut rd, RD_STORAGE_BASE + U256::from(1), addr_to_u256(FEE_AGGREGATOR));
        // +2: consensusRegistry (address)
        set_slot(&mut rd, RD_STORAGE_BASE + U256::from(2), addr_to_u256(CONSENSUS_REGISTRY));
        // +3: delegationPool (address)
        set_slot(&mut rd, RD_STORAGE_BASE + U256::from(3), addr_to_u256(DELEGATION_POOL));
        // +4 through +10: mappings/arrays — zero by default

        state.insert(REWARD_DISTRIBUTOR, rd);
    }

    // ── Part 5: Link RLS proxy to its implementation ────────────────────

    {
        // RLS proxy already has correct bytecode and storage (balances, allowances,
        // name, symbol, totalSupply) from genesis.  Do NOT use account_with_code()
        // here — that causes CacheAccount::change() to treat this as a code
        // replacement, which wipes all existing storage.  A plain account lets
        // the pre-load copy the real AccountInfo from the cache, preserving both
        // bytecode and storage; only the impl slot is written.
        let mut rls = RevmAccount { status: AccountStatus::Touched, ..Default::default() };
        set_slot(&mut rls, ERC1967_IMPL_SLOT, addr_to_u256(RLS_IMPL));
        state.insert(RLS_TOKEN, rls);
    }

    state
}

/// The old admin address (for logging).
pub(crate) const fn old_admin() -> Address {
    OLD_ADMIN
}

/// The new admin address (for logging).
pub(crate) const fn new_admin() -> Address {
    NEW_ADMIN
}

#[cfg(test)]
mod tests {
    #[cfg(test)]
    use super::*;
    #[cfg(test)]
    use alloy::primitives::b256;

    #[test]
    fn admin_transfer_state_contains_all_contracts() {
        let state = admin_transfer_state();
        // Role transfers (with proxy bytecode preserved)
        assert!(state.contains_key(&NATIVE_TOKEN_CONTROLLER));
        assert!(state.contains_key(&FEE_AGGREGATOR));
        // Proxy migrations
        assert!(state.contains_key(&DELEGATION_POOL));
        assert!(state.contains_key(&DELEGATION_POOL_IMPL));
        assert!(state.contains_key(&REWARD_DISTRIBUTOR));
        assert!(state.contains_key(&REWARD_DISTRIBUTOR_IMPL));
        // RLS impl slot fix
        assert!(state.contains_key(&RLS_TOKEN));
    }

    #[test]
    fn impl_accounts_have_bytecode() {
        let state = admin_transfer_state();
        let dp_impl = &state[&DELEGATION_POOL_IMPL];
        assert!(dp_impl.info.code.is_some());
        assert_ne!(
            dp_impl.info.code_hash,
            b256!("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
        ); // not KECCAK_EMPTY

        let rd_impl = &state[&REWARD_DISTRIBUTOR_IMPL];
        assert!(rd_impl.info.code.is_some());
        assert_ne!(
            rd_impl.info.code_hash,
            b256!("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
        );
    }

    #[test]
    fn proxy_accounts_have_erc1967_bytecode_and_impl_slot() {
        let state = admin_transfer_state();

        for (proxy_addr, impl_addr) in
            [(DELEGATION_POOL, DELEGATION_POOL_IMPL), (REWARD_DISTRIBUTOR, REWARD_DISTRIBUTOR_IMPL)]
        {
            let account = &state[&proxy_addr];
            // Should have ERC1967Proxy bytecode
            assert!(account.info.code.is_some());
            assert_eq!(
                account.info.code.as_ref().unwrap().original_byte_slice().len(),
                ERC1967_PROXY_BYTECODE.len()
            );

            // Should have ERC1967 implementation slot pointing to impl
            let impl_slot = &account.storage[&ERC1967_IMPL_SLOT];
            assert_eq!(impl_slot.present_value(), addr_to_u256(impl_addr));
        }
    }

    #[test]
    fn proxy_accounts_have_access_control_roles() {
        let state = admin_transfer_state();

        for proxy_addr in [DELEGATION_POOL, REWARD_DISTRIBUTOR] {
            let account = &state[&proxy_addr];
            // DEFAULT_ADMIN_ROLE granted to POOL_ADMIN
            assert_eq!(account.storage[&DEFAULT_ADMIN_POOL].present_value(), VALUE_TRUE);
            // UPGRADER_ROLE granted to POOL_ADMIN
            assert_eq!(account.storage[&UPGRADER_POOL].present_value(), VALUE_TRUE);
            // Initializable._initialized = 1
            assert_eq!(account.storage[&INITIALIZABLE_SLOT].present_value(), INITIALIZED_V1);
        }
    }

    #[test]
    fn delegation_pool_has_correct_erc7201_storage() {
        let state = admin_transfer_state();
        let dp = &state[&DELEGATION_POOL];

        // rls token
        assert_eq!(dp.storage[&DP_STORAGE_BASE].present_value(), addr_to_u256(RLS_TOKEN));
        // consensusRegistry
        assert_eq!(
            dp.storage[&(DP_STORAGE_BASE + U256::from(1))].present_value(),
            addr_to_u256(CONSENSUS_REGISTRY)
        );
        // config values
        assert_eq!(
            dp.storage[&(DP_STORAGE_BASE + U256::from(2))].present_value(),
            DP_MIN_DELEGATION
        );
        assert_eq!(
            dp.storage[&(DP_STORAGE_BASE + U256::from(3))].present_value(),
            DP_MAX_DELEGATION
        );
        assert_eq!(
            dp.storage[&(DP_STORAGE_BASE + U256::from(4))].present_value(),
            DP_MAX_VALIDATOR_DELEGATION
        );
        assert_eq!(
            dp.storage[&(DP_STORAGE_BASE + U256::from(5))].present_value(),
            DP_CONFIG_PACKED
        );
    }

    #[test]
    fn reward_distributor_has_correct_erc7201_storage() {
        let state = admin_transfer_state();
        let rd = &state[&REWARD_DISTRIBUTOR];

        // rls token
        assert_eq!(rd.storage[&RD_STORAGE_BASE].present_value(), addr_to_u256(RLS_TOKEN));
        // feeAggregator
        assert_eq!(
            rd.storage[&(RD_STORAGE_BASE + U256::from(1))].present_value(),
            addr_to_u256(FEE_AGGREGATOR)
        );
        // consensusRegistry
        assert_eq!(
            rd.storage[&(RD_STORAGE_BASE + U256::from(2))].present_value(),
            addr_to_u256(CONSENSUS_REGISTRY)
        );
        // delegationPool
        assert_eq!(
            rd.storage[&(RD_STORAGE_BASE + U256::from(3))].present_value(),
            addr_to_u256(DELEGATION_POOL)
        );
    }

    #[test]
    fn native_token_controller_role_grants() {
        let state = admin_transfer_state();
        let ntc = &state[&NATIVE_TOKEN_CONTROLLER];

        // Should have ERC1967Proxy bytecode preserved
        assert!(ntc.info.code.is_some());
        assert_eq!(
            ntc.info.code.as_ref().unwrap().original_byte_slice().len(),
            ERC1967_PROXY_BYTECODE.len()
        );

        // New admin roles granted
        assert_eq!(ntc.storage[&DEFAULT_ADMIN_NEW].present_value(), VALUE_TRUE);
        assert_eq!(ntc.storage[&UPGRADER_NEW].present_value(), VALUE_TRUE);
        // Old admin roles are NOT touched (no revocation)
        assert!(!ntc.storage.contains_key(&DEFAULT_ADMIN_OLD));
        assert!(!ntc.storage.contains_key(&UPGRADER_OLD));
        // Exactly 2 storage changes (grants only)
        assert_eq!(ntc.storage.len(), 2);
    }

    #[test]
    fn fee_aggregator_role_grants() {
        let state = admin_transfer_state();
        let fa = &state[&FEE_AGGREGATOR];

        // Should have ERC1967Proxy bytecode preserved
        assert!(fa.info.code.is_some());
        assert_eq!(
            fa.info.code.as_ref().unwrap().original_byte_slice().len(),
            ERC1967_PROXY_BYTECODE.len()
        );

        // New admin roles granted
        assert_eq!(fa.storage[&DEFAULT_ADMIN_NEW].present_value(), VALUE_TRUE);
        assert_eq!(fa.storage[&UPGRADER_NEW].present_value(), VALUE_TRUE);
        assert_eq!(fa.storage[&KEEPER_NEW].present_value(), VALUE_TRUE);
        assert_eq!(fa.storage[&PAUSER_NEW].present_value(), VALUE_TRUE);
        // Old admin roles are NOT touched (no revocation)
        assert!(!fa.storage.contains_key(&DEFAULT_ADMIN_OLD));
        assert!(!fa.storage.contains_key(&UPGRADER_OLD));
        assert!(!fa.storage.contains_key(&KEEPER_OLD));
        assert!(!fa.storage.contains_key(&PAUSER_OLD));
        // Exactly 4 storage changes (grants only)
        assert_eq!(fa.storage.len(), 4);
    }

    #[test]
    fn rls_proxy_has_impl_slot() {
        let state = admin_transfer_state();
        let rls = &state[&RLS_TOKEN];
        let impl_slot = &rls.storage[&ERC1967_IMPL_SLOT];
        assert_eq!(impl_slot.present_value(), addr_to_u256(RLS_IMPL));
    }

    #[test]
    fn role_transfer_slots_are_all_distinct() {
        // Ensure no hash collisions in pre-computed slots.
        let all_slots = [
            DEFAULT_ADMIN_OLD,
            DEFAULT_ADMIN_NEW,
            UPGRADER_OLD,
            UPGRADER_NEW,
            KEEPER_OLD,
            KEEPER_NEW,
            PAUSER_OLD,
            PAUSER_NEW,
            DEFAULT_ADMIN_POOL,
            UPGRADER_POOL,
        ];
        for i in 0..all_slots.len() {
            for j in (i + 1)..all_slots.len() {
                assert_ne!(all_slots[i], all_slots[j], "slot collision at indices {i} and {j}");
            }
        }
    }

    #[test]
    fn old_ownable_slots_are_cleared() {
        let state = admin_transfer_state();

        // DelegationPool: slots 0-4 should be cleared
        let dp = &state[&DELEGATION_POOL];
        for slot in 0..=4u64 {
            let s = &dp.storage[&U256::from(slot)];
            assert_eq!(s.present_value(), U256::ZERO, "DP slot {slot} should be cleared");
        }

        // RewardDistributor: slots 0-3 should be cleared
        let rd = &state[&REWARD_DISTRIBUTOR];
        for slot in 0..=3u64 {
            let s = &rd.storage[&U256::from(slot)];
            assert_eq!(s.present_value(), U256::ZERO, "RD slot {slot} should be cleared");
        }
    }
}
