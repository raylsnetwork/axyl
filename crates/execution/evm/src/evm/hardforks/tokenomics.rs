//! Tokenomics hardfork: deploy RLSAccumulator, wire reward distribution contracts,
//! and replace implementation bytecodes for contracts modified in the tokenomics branch.
//!
//! This migration:
//! 1. Deploys RLSAccumulator proxy + implementation
//! 2. Writes ERC-7201 storage (rls token, rewardDistributor)
//! 3. Grants AccessControl roles to admin
//! 4. Sets RLS ERC-20 allowance so RewardDistributor can pull from accumulator
//! 5. Wires DelegationPool.rewardDistributor → RewardDistributor
//! 6. Wires RewardDistributor.accumulator → RLSAccumulator
//! 7. Replaces DelegationPoolImpl bytecode (FIND-007 slash dust fix)
//! 8. Replaces RewardDistributorImpl bytecode (consolidation refactor + accumulator fields)
//! 9. Replaces NativeTokenControllerImpl bytecode (interface alignment)
//! 10. Replaces FeeAggregatorImpl bytecode (LayerZero burn flow + per-category retry)
//!
//! All impl swaps target storage-compatible contracts. New fields appended to existing
//! storage structs auto-zero on first read; live state at preserved slot positions is
//! untouched.

use alloy::primitives::{address, Bytes};
use rayls_infrastructure_types::{Address, U256};
use reth_revm::{
    bytecode::Bytecode,
    primitives::HashMap,
    state::{Account as RevmAccount, EvmStorageSlot},
};

// ── Contract addresses ────────────────────────────────────────────────

/// RLSAccumulator proxy address (new).
const RLS_ACCUMULATOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ec");
/// RLSAccumulator implementation address (new).
const RLS_ACCUMULATOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ed");
/// RewardDistributor proxy address.
const REWARD_DISTRIBUTOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e5");
/// RewardDistributor implementation address.
const REWARD_DISTRIBUTOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e8");
/// DelegationPool proxy address.
const DELEGATION_POOL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e2");
/// DelegationPool implementation address.
const DELEGATION_POOL_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e9");
/// NativeTokenController implementation address.
const NATIVE_TOKEN_CONTROLLER_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e7");
/// FeeAggregator implementation address.
const FEE_AGGREGATOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e4");
/// RLS ERC-20 token proxy address.
const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
/// RLS ERC-20 token implementation address.
const RLS_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17eb");
/// Admin address.
#[allow(dead_code)]
const ADMIN: Address = address!("91ec7A2be07A79D2eAB99135553b26F706099e9D");

// ── Common constants ──────────────────────────────────────────────────

const VALUE_TRUE: U256 = U256::from_limbs([1, 0, 0, 0]);
const VALUE_MAX: U256 = U256::MAX;

/// ERC-1967 implementation storage slot.
const ERC1967_IMPL_SLOT: U256 = U256::from_be_slice(&[
    0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9, 0x8d,
    0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38, 0x2b, 0xbc,
]);

/// OpenZeppelin Initializable slot: _initialized = 1
const INITIALIZABLE_SLOT: U256 = U256::from_be_slice(&[
    0xf0, 0xc5, 0x7e, 0x16, 0x84, 0x0d, 0xf0, 0x40, 0xf1, 0x50, 0x88, 0xdc, 0x2f, 0x81, 0xfe, 0x39,
    0x1c, 0x39, 0x23, 0xbe, 0xc7, 0x3e, 0x23, 0xa9, 0x66, 0x2e, 0xfc, 0x9c, 0x22, 0x9c, 0x6a, 0x00,
]);
const INITIALIZED_V1: U256 = U256::from_limbs([1, 0, 0, 0]);

// ── RLSAccumulator ERC-7201 storage ───────────────────────────────────
// Base = keccak256(abi.encode(uint256(keccak256("rlsaccumulator.storage.v1")) - 1)) & ~0xff
// = 0x1434b6d7a9cd896918fc9e177be6ce103a3f4b47c31c2a0c7a187bcc052a1c00

const ACCUM_STORAGE_BASE: U256 = U256::from_be_slice(&[
    0x14, 0x34, 0xb6, 0xd7, 0xa9, 0xcd, 0x89, 0x69, 0x18, 0xfc, 0x9e, 0x17, 0x7b, 0xe6, 0xce, 0x10,
    0x3a, 0x3f, 0x4b, 0x47, 0xc3, 0x1c, 0x2a, 0x0c, 0x7a, 0x18, 0x7b, 0xcc, 0x05, 0x2a, 0x1c, 0x00,
]);

// ── AccessControl slots for ADMIN on the RLSAccumulator ───────────────
// Computed from: keccak256(abi.encode(ADMIN, keccak256(abi.encode(role, AC_BASE))))
// where AC_BASE = 0x02dd7bc7dec4dceedda775e58dd541e08a116c6c53815c0bd028192f7b626800

/// DEFAULT_ADMIN_ROLE.members[ADMIN]
const AC_DEFAULT_ADMIN: U256 = U256::from_be_slice(&[
    0x18, 0x60, 0x6a, 0xdf, 0x89, 0x45, 0xd7, 0x3b, 0xd9, 0xdd, 0x10, 0xd1, 0x4a, 0xc3, 0xc2, 0x94,
    0x5c, 0x07, 0x8a, 0xb7, 0xd7, 0x2c, 0x86, 0x32, 0xbd, 0xa6, 0x28, 0x34, 0x7c, 0x18, 0x1a, 0xdc,
]);

/// DEPOSITOR_ROLE.members[ADMIN]
/// DEPOSITOR_ROLE = keccak256("DEPOSITOR_ROLE")
const AC_DEPOSITOR: U256 = U256::from_be_slice(&[
    0x08, 0x99, 0x85, 0x3e, 0xe5, 0x56, 0x87, 0x5b, 0xbc, 0x0a, 0x36, 0xdd, 0xd3, 0xfd, 0x6f, 0x80,
    0x22, 0x82, 0x3b, 0x31, 0x74, 0x61, 0xcf, 0x31, 0x74, 0xf1, 0x37, 0xb1, 0x74, 0x6c, 0x15, 0x14,
]);

/// UPGRADER_ROLE.members[ADMIN]
const AC_UPGRADER: U256 = U256::from_be_slice(&[
    0x46, 0x87, 0xe7, 0xfc, 0xf6, 0xeb, 0x36, 0x25, 0xad, 0xda, 0xcb, 0x3f, 0xb4, 0x0e, 0xdc, 0xb2,
    0x88, 0x03, 0x6d, 0xda, 0x6f, 0x4a, 0x77, 0xcb, 0x31, 0x03, 0x6f, 0xa4, 0x6f, 0x8d, 0x2d, 0x75,
]);

// ── Cross-contract wiring slots ───────────────────────────────────────

/// DelegationPool ERC-7201 base + 12: rewardDistributor field
const DP_REWARD_DIST_SLOT: U256 = U256::from_be_slice(&[
    0x88, 0x22, 0x1c, 0x9a, 0x15, 0xd5, 0x66, 0x92, 0xc8, 0x2f, 0xe5, 0xe6, 0xf9, 0x56, 0xbd, 0xf5,
    0x3e, 0xb6, 0x18, 0x54, 0x01, 0x7a, 0xba, 0x5b, 0xac, 0xf6, 0xb4, 0x09, 0x76, 0x11, 0x9e, 0x0c,
]);

/// RewardDistributor ERC-7201 base + 16: accumulator field
const RD_ACCUMULATOR_SLOT: U256 = U256::from_be_slice(&[
    0x8a, 0x40, 0xcc, 0x0c, 0xcf, 0x5a, 0x2d, 0x03, 0x00, 0x58, 0xc8, 0x60, 0xd7, 0x66, 0x01, 0xe0,
    0x41, 0x04, 0x94, 0x79, 0x50, 0xec, 0x74, 0x75, 0xe8, 0x0a, 0xb1, 0x5a, 0x7d, 0x69, 0xd6, 0x10,
]);

/// RLS ERC-20 _allowances[RLSAccumulator][RewardDistributor]
/// = keccak256(abi.encode(spender, keccak256(abi.encode(owner, ERC20_BASE + 1))))
const RLS_ALLOWANCE_SLOT: U256 = U256::from_be_slice(&[
    0xa8, 0x6c, 0x96, 0x62, 0xf5, 0xbb, 0xd1, 0x41, 0x9e, 0x01, 0xb9, 0x20, 0xe7, 0x03, 0x4d, 0xd5,
    0xf6, 0x8f, 0x6d, 0x93, 0xdd, 0x1a, 0x17, 0x52, 0xef, 0xe6, 0xe7, 0x79, 0x17, 0x00, 0x58, 0xba,
]);

// ── Bytecode ──────────────────────────────────────────────────────────

const ERC1967_PROXY_BYTECODE: &[u8] = include_bytes!("bytecodes/tokenomics/erc1967_proxy.bin");
const RLS_ACCUMULATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/tokenomics/rls_accumulator_impl.bin");
const DELEGATION_POOL_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/tokenomics/delegation_pool_impl.bin");
const REWARD_DISTRIBUTOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/tokenomics/reward_distributor_impl.bin");
const NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/tokenomics/native_token_controller_impl.bin");
const FEE_AGGREGATOR_IMPL_BYTECODE: &[u8] =
    include_bytes!("bytecodes/tokenomics/fee_aggregator_impl.bin");
const RLS_IMPL_BYTECODE: &[u8] = include_bytes!("bytecodes/tokenomics/rls_impl.bin");

// ── Helpers ───────────────────────────────────────────────────────────

fn addr_to_u256(addr: Address) -> U256 {
    let mut buf = [0u8; 32];
    buf[12..].copy_from_slice(addr.as_slice());
    U256::from_be_bytes(buf)
}

use super::account_with_code;

fn set_slot(account: &mut RevmAccount, slot: U256, value: U256) {
    account.storage.insert(slot, EvmStorageSlot::new_changed(U256::ZERO, value, 0));
}

/// Ensure an account has the ERC1967 proxy bytecode. On devnet, the AdminTransfer/RlsStorage
/// hardforks may fail to persist bytecodes for accounts that exist in the Rust genesis output
/// (code_hash is set but code is lost by the state DB). This re-asserts the bytecode so the
/// Tokenomics hardfork — which runs last — guarantees the code is present.
fn ensure_proxy_code(account: &mut RevmAccount) {
    if account.info.code.is_none() {
        let code = Bytecode::new_raw(Bytes::copy_from_slice(ERC1967_PROXY_BYTECODE));
        account.info.code_hash = code.hash_slow();
        account.info.code = Some(code);
    }
}

// ── Migration ─────────────────────────────────────────────────────────

/// Build the state changes for the Tokenomics hardfork.
pub(crate) fn tokenomics_state() -> HashMap<Address, RevmAccount> {
    let mut state = HashMap::default();

    // ── Part 1: Deploy RLSAccumulator implementation ──────────────────
    state.insert(RLS_ACCUMULATOR_IMPL, account_with_code(RLS_ACCUMULATOR_IMPL_BYTECODE));

    // ── Part 2: Deploy RLSAccumulator proxy with initialized storage ──
    {
        let mut proxy = account_with_code(ERC1967_PROXY_BYTECODE);

        // ERC-1967 impl slot
        set_slot(&mut proxy, ERC1967_IMPL_SLOT, addr_to_u256(RLS_ACCUMULATOR_IMPL));

        // Initializable: _initialized = 1
        set_slot(&mut proxy, INITIALIZABLE_SLOT, INITIALIZED_V1);

        // AccessControl: grant roles to admin
        set_slot(&mut proxy, AC_DEFAULT_ADMIN, VALUE_TRUE);
        set_slot(&mut proxy, AC_DEPOSITOR, VALUE_TRUE);
        set_slot(&mut proxy, AC_UPGRADER, VALUE_TRUE);

        // ERC-7201 namespaced storage
        // +0: rls (address)
        set_slot(&mut proxy, ACCUM_STORAGE_BASE, addr_to_u256(RLS_TOKEN));
        // +1: rewardDistributor (address)
        set_slot(&mut proxy, ACCUM_STORAGE_BASE + U256::from(1), addr_to_u256(REWARD_DISTRIBUTOR));

        state.insert(RLS_ACCUMULATOR, proxy);
    }

    // ── Part 3: Set RLS allowance (accumulator approves RewardDistributor) ──
    // Also re-assert proxy bytecode — on devnet, prior hardfork bytecode injection
    // may fail to persist for accounts that exist in the Rust genesis output.
    {
        let rls =
            state.entry(RLS_TOKEN).or_insert_with(|| account_with_code(ERC1967_PROXY_BYTECODE));
        ensure_proxy_code(rls);
        set_slot(rls, RLS_ALLOWANCE_SLOT, VALUE_MAX);
    }

    // ── Part 4: Wire DelegationPool.rewardDistributor → RewardDistributor ──
    {
        let dp = state
            .entry(DELEGATION_POOL)
            .or_insert_with(|| account_with_code(ERC1967_PROXY_BYTECODE));
        ensure_proxy_code(dp);
        set_slot(dp, DP_REWARD_DIST_SLOT, addr_to_u256(REWARD_DISTRIBUTOR));
    }

    // ── Part 5: Wire RewardDistributor.accumulator → RLSAccumulator ──
    {
        let rd = state
            .entry(REWARD_DISTRIBUTOR)
            .or_insert_with(|| account_with_code(ERC1967_PROXY_BYTECODE));
        ensure_proxy_code(rd);
        set_slot(rd, RD_ACCUMULATOR_SLOT, addr_to_u256(RLS_ACCUMULATOR));
    }

    // ── Part 6: Replace DelegationPoolImpl bytecode (FIND-007 slash dust fix) ──
    // Implementation contract — no storage to preserve.
    state.insert(DELEGATION_POOL_IMPL, account_with_code(DELEGATION_POOL_IMPL_BYTECODE));

    // ── Part 7: Replace RewardDistributorImpl bytecode (consolidation refactor) ──
    // Storage layout is forward-compatible: new fields (accumulator, targetApyBps,
    // totalUnclaimedRewards) are appended to the end of RewardDistributorStorage and
    // auto-zero on first read. Existing slots (rls, feeAggregator, etc.) untouched.
    state.insert(REWARD_DISTRIBUTOR_IMPL, account_with_code(REWARD_DISTRIBUTOR_IMPL_BYTECODE));

    // ── Part 8: Replace NativeTokenControllerImpl bytecode (interface alignment) ──
    // No prior state vars; only event signature changes and added storage gap.
    state.insert(
        NATIVE_TOKEN_CONTROLLER_IMPL,
        account_with_code(NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE),
    );

    // ── Part 9: Replace FeeAggregatorImpl bytecode (LayerZero burn flow) ──
    // Storage layout preserved via _deprecated_* placeholder fields in FeeAggregatorStorage.
    // New fields (oftBridge, dstEid, pendingRlsForDistribution, etc.) appended at the end.
    state.insert(FEE_AGGREGATOR_IMPL, account_with_code(FEE_AGGREGATOR_IMPL_BYTECODE));

    // ── Part 10: Replace RLS implementation bytecode (bridgePaused, mint/burn refactor) ──
    // The RLS proxy storage uses ERC-7201 namespaced slots inherited from OZ ERC20Upgradeable
    // (name, symbol, totalSupply, balances, allowances, AccessControl roles), all unchanged.
    // The branch adds `bool public bridgePaused` at the contract's first own slot (slot 0),
    // which was unused before — safe to swap.
    state.insert(RLS_IMPL, account_with_code(RLS_IMPL_BYTECODE));

    state
}

/// Genesis-state preconditions: the proxies this migration wires must already
/// carry proxy code (deployed by AdminTransfer/RlsStorage, or present in the
/// genesis). Without it, the migration would write ERC-7201 wiring slots into
/// bare addresses, which the live chains it was built against never carried.
pub(crate) fn preconditions(sim: &crate::network_profile::SimAlloc) -> Vec<String> {
    let mut out = Vec::new();
    for (name, address) in [
        ("DelegationPool", DELEGATION_POOL),
        ("RewardDistributor", REWARD_DISTRIBUTOR),
        ("RLS token", RLS_TOKEN),
    ] {
        if sim.code(address).is_none() {
            out.push(format!(
                "Tokenomics: {name} at {address} has no proxy code in the simulated state; the \
                 migration expects the deployed proxy"
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenomics_state_deploys_accumulator_proxy() {
        let state = tokenomics_state();

        // Impl should have bytecode
        let impl_account = &state[&RLS_ACCUMULATOR_IMPL];
        assert!(impl_account.info.code.is_some(), "impl should have bytecode");

        // Proxy should have bytecode
        let proxy = &state[&RLS_ACCUMULATOR];
        assert!(proxy.info.code.is_some(), "proxy should have bytecode");

        // ERC-1967 impl slot points to impl
        assert_eq!(
            proxy.storage[&ERC1967_IMPL_SLOT].present_value(),
            addr_to_u256(RLS_ACCUMULATOR_IMPL)
        );

        // Initialized
        assert_eq!(proxy.storage[&INITIALIZABLE_SLOT].present_value(), INITIALIZED_V1);

        // AccessControl roles
        assert_eq!(proxy.storage[&AC_DEFAULT_ADMIN].present_value(), VALUE_TRUE);
        assert_eq!(proxy.storage[&AC_DEPOSITOR].present_value(), VALUE_TRUE);
        assert_eq!(proxy.storage[&AC_UPGRADER].present_value(), VALUE_TRUE);

        // ERC-7201 storage: rls token
        assert_eq!(proxy.storage[&ACCUM_STORAGE_BASE].present_value(), addr_to_u256(RLS_TOKEN));
        // ERC-7201 storage: rewardDistributor
        assert_eq!(
            proxy.storage[&(ACCUM_STORAGE_BASE + U256::from(1))].present_value(),
            addr_to_u256(REWARD_DISTRIBUTOR)
        );
    }

    #[test]
    fn tokenomics_state_sets_rls_allowance() {
        let state = tokenomics_state();
        let rls = &state[&RLS_TOKEN];
        assert_eq!(
            rls.storage[&RLS_ALLOWANCE_SLOT].present_value(),
            VALUE_MAX,
            "allowance should be type(uint256).max"
        );
    }

    #[test]
    fn tokenomics_state_wires_delegation_pool() {
        let state = tokenomics_state();
        let dp = &state[&DELEGATION_POOL];
        assert_eq!(
            dp.storage[&DP_REWARD_DIST_SLOT].present_value(),
            addr_to_u256(REWARD_DISTRIBUTOR),
            "DelegationPool.rewardDistributor should be set"
        );
    }

    #[test]
    fn tokenomics_state_wires_reward_distributor() {
        let state = tokenomics_state();
        let rd = &state[&REWARD_DISTRIBUTOR];
        assert_eq!(
            rd.storage[&RD_ACCUMULATOR_SLOT].present_value(),
            addr_to_u256(RLS_ACCUMULATOR),
            "RewardDistributor.accumulator should be set"
        );
    }

    #[test]
    fn tokenomics_state_touches_expected_accounts() {
        let state = tokenomics_state();
        // RLSAccumulator deployment + wiring
        assert!(state.contains_key(&RLS_ACCUMULATOR), "accumulator proxy");
        assert!(state.contains_key(&RLS_ACCUMULATOR_IMPL), "accumulator impl");
        assert!(state.contains_key(&RLS_TOKEN), "RLS token (allowance)");
        assert!(state.contains_key(&DELEGATION_POOL), "DelegationPool (wiring)");
        assert!(state.contains_key(&REWARD_DISTRIBUTOR), "RewardDistributor (wiring)");
        // Implementation bytecode replacements
        assert!(state.contains_key(&DELEGATION_POOL_IMPL), "DelegationPool impl swap");
        assert!(state.contains_key(&REWARD_DISTRIBUTOR_IMPL), "RewardDistributor impl swap");
        assert!(
            state.contains_key(&NATIVE_TOKEN_CONTROLLER_IMPL),
            "NativeTokenController impl swap"
        );
        assert!(state.contains_key(&FEE_AGGREGATOR_IMPL), "FeeAggregator impl swap");
        assert!(state.contains_key(&RLS_IMPL), "RLS impl swap");
        assert_eq!(state.len(), 10, "exactly 10 accounts modified");
    }

    /// Compare the underlying bytecode bytes against the included artifact, ignoring
    /// any analysis padding revm appends to legacy bytecode.
    fn assert_bytecode_matches(code: &Bytecode, expected: &[u8], label: &str) {
        let original = code.original_byte_slice();
        assert_eq!(original, expected, "{label}: bytecode mismatch");
    }

    #[test]
    fn tokenomics_state_replaces_delegation_pool_impl_bytecode() {
        let state = tokenomics_state();
        let dp_impl = &state[&DELEGATION_POOL_IMPL];
        let code = dp_impl.info.code.as_ref().expect("DelegationPoolImpl must have bytecode");
        assert_bytecode_matches(code, DELEGATION_POOL_IMPL_BYTECODE, "DelegationPoolImpl");
    }

    #[test]
    fn tokenomics_state_replaces_reward_distributor_impl_bytecode() {
        let state = tokenomics_state();
        let rd_impl = &state[&REWARD_DISTRIBUTOR_IMPL];
        let code = rd_impl.info.code.as_ref().expect("RewardDistributorImpl must have bytecode");
        assert_bytecode_matches(code, REWARD_DISTRIBUTOR_IMPL_BYTECODE, "RewardDistributorImpl");
    }

    #[test]
    fn tokenomics_state_replaces_native_token_controller_impl_bytecode() {
        let state = tokenomics_state();
        let ntc_impl = &state[&NATIVE_TOKEN_CONTROLLER_IMPL];
        let code =
            ntc_impl.info.code.as_ref().expect("NativeTokenControllerImpl must have bytecode");
        assert_bytecode_matches(
            code,
            NATIVE_TOKEN_CONTROLLER_IMPL_BYTECODE,
            "NativeTokenControllerImpl",
        );
    }

    #[test]
    fn tokenomics_state_replaces_fee_aggregator_impl_bytecode() {
        let state = tokenomics_state();
        let fa_impl = &state[&FEE_AGGREGATOR_IMPL];
        let code = fa_impl.info.code.as_ref().expect("FeeAggregatorImpl must have bytecode");
        assert_bytecode_matches(code, FEE_AGGREGATOR_IMPL_BYTECODE, "FeeAggregatorImpl");
    }

    #[test]
    fn tokenomics_state_replaces_rls_impl_bytecode() {
        let state = tokenomics_state();
        let rls_impl = &state[&RLS_IMPL];
        let code = rls_impl.info.code.as_ref().expect("RLSImpl must have bytecode");
        assert_bytecode_matches(code, RLS_IMPL_BYTECODE, "RLSImpl");
    }

    #[test]
    fn tokenomics_state_proxy_storage_preserved_after_impl_swap() {
        // Verify that DelegationPool/RewardDistributor proxy state is wired but
        // not zeroed during impl swap. The proxy accounts only have storage updates
        // (wiring slots), and the impl swaps target separate addresses.
        let state = tokenomics_state();

        // DelegationPool proxy: should have wiring slot set, not be empty
        let dp = &state[&DELEGATION_POOL];
        assert!(dp.storage.contains_key(&DP_REWARD_DIST_SLOT));

        // RewardDistributor proxy: should have wiring slot set
        let rd = &state[&REWARD_DISTRIBUTOR];
        assert!(rd.storage.contains_key(&RD_ACCUMULATOR_SLOT));

        // Impl addresses are SEPARATE from proxy addresses — no overlap
        assert_ne!(DELEGATION_POOL, DELEGATION_POOL_IMPL);
        assert_ne!(REWARD_DISTRIBUTOR, REWARD_DISTRIBUTOR_IMPL);
    }
}
