//! UUPS fix hardfork: patch implementation bytecodes with correct `__self` immutable
//! and write `_disableInitializers()` storage on all implementation contracts.
//!
//! ## Background
//!
//! Genesis/hardfork-placed bytecodes skip constructor execution, leaving two UUPS
//! invariants broken:
//!
//! 1. **`__self` immutable is `address(0)`** — The Solidity compiler emits zero-filled placeholders
//!    for immutables in runtime bytecode; they're normally patched during CREATE/CREATE2. Without
//!    constructor execution, `__self = address(0)`, causing `UUPSUnauthorizedCallContext()` on any
//!    `upgradeToAndCall` invocation.
//!
//! 2. **`_disableInitializers()` never ran** — The OpenZeppelin UUPS constructor calls
//!    `_disableInitializers()` to prevent implementation takeover. Without it, anyone can call
//!    `initialize()` on the implementation contract directly.
//!
//! ## Fix
//!
//! This migration:
//! 1. Replaces all 6 UUPS implementation bytecodes with versions that have `__self` pre-patched to
//!    the correct deployment address (patched at build time via the Forge artifact's
//!    `immutableReferences`).
//! 2. Writes `_initialized = type(uint64).max` to the Initializable storage slot on each
//!    implementation contract (equivalent to `_disableInitializers()`).
//!
//! After this hardfork, UUPS `upgradeToAndCall` works normally and implementations
//! cannot be initialized by external callers.

use alloy::primitives::Bytes;
use rayls_infrastructure_types::{address, Address, U256};
use reth_revm::{
    bytecode::Bytecode,
    primitives::HashMap,
    state::{Account as RevmAccount, AccountInfo, AccountStatus, EvmStorageSlot},
};

// ── Implementation addresses ─────────────────────────────────────────

const FEE_AGGREGATOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e4");
const DELEGATION_POOL_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e9");
const REWARD_DISTRIBUTOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e8");
const NATIVE_TOKEN_CONTROLLER_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e7");
const RLS_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17eb");
const RLS_ACCUMULATOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ed");

// ── Storage constants ────────────────────────────────────────────────

/// OpenZeppelin Initializable storage slot.
/// keccak256(abi.encode(uint256(keccak256("openzeppelin.storage.Initializable")) - 1)) & ~0xff
const INITIALIZABLE_SLOT: U256 = U256::from_be_slice(&[
    0xf0, 0xc5, 0x7e, 0x16, 0x84, 0x0d, 0xf0, 0x40, 0xf1, 0x50, 0x88, 0xdc, 0x2f, 0x81, 0xfe, 0x39,
    0x1c, 0x39, 0x23, 0xbe, 0xc7, 0x3e, 0x23, 0xa9, 0x66, 0x2e, 0xfc, 0x9c, 0x22, 0x9c, 0x6a, 0x00,
]);

/// `type(uint64).max` — the value `_disableInitializers()` writes.
const INITIALIZED_MAX: U256 = U256::from_limbs([u64::MAX, 0, 0, 0]);

// ── Bytecodes (pre-patched with correct __self at build time) ────────

const FEE_AGGREGATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/uups/fee_aggregator_impl.bin");
const DELEGATION_POOL_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/uups/delegation_pool_impl.bin");
const REWARD_DISTRIBUTOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/uups/reward_distributor_impl.bin");
const NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/uups/native_token_controller_impl.bin");
const RLS_IMPL_BYTECODE: &[u8] = include_bytes!("bytecodes/uups/rls_impl.bin");
const RLS_ACCUMULATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/uups/rls_accumulator_impl.bin");

// ── Helpers ──────────────────────────────────────────────────────────

fn impl_with_code_and_initializer_disabled(bytecode: &[u8]) -> RevmAccount {
    let code = Bytecode::new_raw(Bytes::copy_from_slice(bytecode));
    let mut account = RevmAccount {
        info: AccountInfo { code_hash: code.hash_slow(), code: Some(code), ..Default::default() },
        status: AccountStatus::Touched,
        ..Default::default()
    };
    // Equivalent to _disableInitializers() in the constructor
    account
        .storage
        .insert(INITIALIZABLE_SLOT, EvmStorageSlot::new_changed(U256::ZERO, INITIALIZED_MAX, 0));
    account
}

// ── Migration ────────────────────────────────────────────────────────

/// Build the state changes for the UupsFix hardfork.
pub(crate) fn uups_state() -> HashMap<Address, RevmAccount> {
    let mut state = HashMap::default();

    state.insert(
        FEE_AGGREGATOR_IMPL,
        impl_with_code_and_initializer_disabled(FEE_AGGREGATOR_IMPL_BYTECODE),
    );
    state.insert(
        DELEGATION_POOL_IMPL,
        impl_with_code_and_initializer_disabled(DELEGATION_POOL_IMPL_BYTECODE),
    );
    state.insert(
        REWARD_DISTRIBUTOR_IMPL,
        impl_with_code_and_initializer_disabled(REWARD_DISTRIBUTOR_IMPL_BYTECODE),
    );
    state.insert(
        NATIVE_TOKEN_CONTROLLER_IMPL,
        impl_with_code_and_initializer_disabled(NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE),
    );
    state.insert(RLS_IMPL, impl_with_code_and_initializer_disabled(RLS_IMPL_BYTECODE));
    state.insert(
        RLS_ACCUMULATOR_IMPL,
        impl_with_code_and_initializer_disabled(RLS_ACCUMULATOR_IMPL_BYTECODE),
    );

    state
}

/// Genesis-state preconditions: every implementation this migration re-patches
/// must already carry code. The genesis and the Tokenomics/AdminTransfer
/// deployments are what place them; a missing implementation means the schedule
/// fires before the state that carries it (or the genesis was never built with
/// it).
pub(crate) fn preconditions(sim: &crate::network_profile::SimAlloc) -> Vec<String> {
    let mut out = Vec::new();
    for (name, address) in [
        ("FeeAggregatorImpl", FEE_AGGREGATOR_IMPL),
        ("DelegationPoolImpl", DELEGATION_POOL_IMPL),
        ("RewardDistributorImpl", REWARD_DISTRIBUTOR_IMPL),
        ("NativeTokenControllerImpl", NATIVE_TOKEN_CONTROLLER_IMPL),
        ("RLSImpl", RLS_IMPL),
        ("RLSAccumulatorImpl", RLS_ACCUMULATOR_IMPL),
    ] {
        if sim.code(address).is_none() {
            out.push(format!(
                "Uups: {name} at {address} has no code in the simulated state; the migration \
                 re-patches an implementation that was never deployed"
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uups_fix_touches_all_implementations() {
        let state = uups_state();
        assert_eq!(state.len(), 6, "should modify exactly 6 impl accounts");

        for (name, addr) in [
            ("FeeAggregator", FEE_AGGREGATOR_IMPL),
            ("DelegationPool", DELEGATION_POOL_IMPL),
            ("RewardDistributor", REWARD_DISTRIBUTOR_IMPL),
            ("NativeTokenController", NATIVE_TOKEN_CONTROLLER_IMPL),
            ("RLS", RLS_IMPL),
            ("RLSAccumulator", RLS_ACCUMULATOR_IMPL),
        ] {
            let account = state.get(&addr).unwrap_or_else(|| panic!("{name} impl missing"));
            assert!(account.info.code.is_some(), "{name} should have bytecode");
            assert_eq!(
                account.storage[&INITIALIZABLE_SLOT].present_value(),
                INITIALIZED_MAX,
                "{name} should have _initialized = type(uint64).max"
            );
        }
    }

    #[test]
    fn uups_fix_bytecodes_contain_correct_self() {
        // Verify that each bytecode contains its target address (proving __self was patched)
        let cases: &[(&str, Address, &[u8])] = &[
            ("FeeAggregator", FEE_AGGREGATOR_IMPL, FEE_AGGREGATOR_IMPL_BYTECODE),
            ("DelegationPool", DELEGATION_POOL_IMPL, DELEGATION_POOL_IMPL_BYTECODE),
            ("RewardDistributor", REWARD_DISTRIBUTOR_IMPL, REWARD_DISTRIBUTOR_IMPL_BYTECODE),
            (
                "NativeTokenController",
                NATIVE_TOKEN_CONTROLLER_IMPL,
                NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE,
            ),
            ("RLS", RLS_IMPL, RLS_IMPL_BYTECODE),
            ("RLSAccumulator", RLS_ACCUMULATOR_IMPL, RLS_ACCUMULATOR_IMPL_BYTECODE),
        ];

        for (name, addr, bytecode) in cases {
            // Build the 32-byte padded address that should appear in the bytecode
            let mut padded = [0u8; 32];
            padded[12..].copy_from_slice(addr.as_slice());

            // Search for it in the bytecode
            let found = bytecode.windows(32).any(|window| window == padded);

            assert!(found, "{name} bytecode should contain __self = {addr}");
        }
    }
}
