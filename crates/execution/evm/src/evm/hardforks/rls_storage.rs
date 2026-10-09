//! RlsStorage hardfork: deploy ERC1967 proxy bytecode and initialize storage for the
//! RLS token contract.
//!
//! ## Problem
//!
//! The AdminTransfer hardfork set the ERC-1967 implementation slot on the RLS proxy but
//! did NOT deploy the proxy bytecode, leaving the address without executable code. All
//! calls to the RLS token (totalSupply, transfer, mint, etc.) fail because there is no
//! bytecode to execute the delegatecall to the implementation.
//!
//! ## Fix
//!
//! This migration:
//! 1. Deploys ERC1967Proxy bytecode to the RLS proxy address
//! 2. Writes ERC-1967 implementation slot pointing to the RLS impl
//! 3. Initializes ERC-20 storage (name = "Rayls", symbol = "RLS")
//! 4. Sets totalSupply and balances (admin + ConsensusRegistry for staked tokens)
//! 5. Grants AccessControl roles to admin (DEFAULT_ADMIN, UPGRADER, PAUSER)
//! 6. Marks the contract as initialized (_initialized = 1)
//!
//! Since all RLS proxy storage is empty on testnet (verified via cast), using
//! account_with_code() is safe — there is nothing to wipe.

use alloy::primitives::address;
use rayls_infrastructure_types::{Address, U256};
use reth_revm::{
    primitives::HashMap,
    state::{Account as RevmAccount, EvmStorageSlot},
};

use super::account_with_code;
// ── Contract addresses ────────────────────────────────────────────────

/// RLS ERC-20 token proxy address.
const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
/// RLS ERC-20 token implementation address.
const RLS_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17eb");
/// ConsensusRegistry address (holds staked RLS).
#[allow(dead_code)] // documents the address for genesis slot layout
const CONSENSUS_REGISTRY: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e1");
/// Admin address.
#[allow(dead_code)] // documents the address for genesis slot layout
const ADMIN: Address = address!("91ec7A2be07A79D2eAB99135553b26F706099e9D");

// ── Common constants ──────────────────────────────────────────────────

const VALUE_TRUE: U256 = U256::from_limbs([1, 0, 0, 0]);

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

// ── ERC-20 storage slots (ERC-7201: openzeppelin.storage.ERC20) ─────
// Base = 0x52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00

/// _name slot (base + 3): "Rayls" as Solidity short string
const ERC20_NAME_SLOT: U256 = U256::from_be_slice(&[
    0x52, 0xc6, 0x32, 0x47, 0xe1, 0xf4, 0x7d, 0xb1, 0x9d, 0x5c, 0xe0, 0x46, 0x00, 0x30, 0xc4, 0x97,
    0xf0, 0x67, 0xca, 0x4c, 0xeb, 0xf7, 0x1b, 0xa9, 0x8e, 0xea, 0xda, 0xbe, 0x20, 0xba, 0xce, 0x03,
]);
const ERC20_NAME_VALUE: U256 = U256::from_be_slice(&[
    0x52, 0x61, 0x79, 0x6c, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
]); // "Rayls" + length*2 = 0x0a

/// _symbol slot (base + 4): "RLS" as Solidity short string
const ERC20_SYMBOL_SLOT: U256 = U256::from_be_slice(&[
    0x52, 0xc6, 0x32, 0x47, 0xe1, 0xf4, 0x7d, 0xb1, 0x9d, 0x5c, 0xe0, 0x46, 0x00, 0x30, 0xc4, 0x97,
    0xf0, 0x67, 0xca, 0x4c, 0xeb, 0xf7, 0x1b, 0xa9, 0x8e, 0xea, 0xda, 0xbe, 0x20, 0xba, 0xce, 0x04,
]);
const ERC20_SYMBOL_VALUE: U256 = U256::from_be_slice(&[
    0x52, 0x4c, 0x53, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
]); // "RLS" + length*2 = 0x06

/// _totalSupply slot (base + 2): 1 billion RLS = 1_000_000_000e18
const ERC20_TOTAL_SUPPLY_SLOT: U256 = U256::from_be_slice(&[
    0x52, 0xc6, 0x32, 0x47, 0xe1, 0xf4, 0x7d, 0xb1, 0x9d, 0x5c, 0xe0, 0x46, 0x00, 0x30, 0xc4, 0x97,
    0xf0, 0x67, 0xca, 0x4c, 0xeb, 0xf7, 0x1b, 0xa9, 0x8e, 0xea, 0xda, 0xbe, 0x20, 0xba, 0xce, 0x02,
]);
// 1_000_000_000e18 = 0x033b2e3c9fd0803ce8000000
const TOTAL_SUPPLY: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x03, 0x3b, 0x2e, 0x3c, 0x9f, 0xd0, 0x80, 0x3c, 0xe8, 0x00, 0x00, 0x00,
]);

// ── ERC-20 balance slots: keccak256(abi.encode(addr, ERC20_BASE)) ───

/// _balances[ConsensusRegistry]: 4M RLS (4 validators × 1M staked)
/// keccak256(abi.encode(0x07E17...e1, ERC20_BASE))
const RLS_BAL_CR: U256 = U256::from_be_slice(&[
    0xdb, 0xac, 0xdc, 0xbe, 0xce, 0xe3, 0x97, 0x69, 0x44, 0x68, 0xa2, 0x07, 0xb9, 0x38, 0x02, 0x3f,
    0xc3, 0x9d, 0x98, 0x34, 0x16, 0xcb, 0x4e, 0x04, 0xfa, 0xbf, 0x87, 0x8a, 0x14, 0xa5, 0x2f, 0x03,
]);
// 4_000_000e18 = 0x34f086f3b33b684000000
const CR_BALANCE: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x4f, 0x08, 0x6f, 0x3b, 0x33, 0xb6, 0x84, 0x00, 0x00, 0x00,
]);

/// _balances[ADMIN]: 996M RLS (1B - 4M staked)
/// keccak256(abi.encode(0x91ec7A..., ERC20_BASE))
const RLS_BAL_ADMIN: U256 = U256::from_be_slice(&[
    0xbe, 0xc4, 0x54, 0x3c, 0xf8, 0xdf, 0xfc, 0x23, 0x0f, 0xdc, 0x1a, 0x0e, 0x7e, 0xbd, 0x59, 0xa8,
    0xe1, 0x29, 0x5d, 0xd4, 0x0c, 0x83, 0x79, 0xa7, 0x75, 0xcb, 0x0c, 0xe0, 0x8c, 0xaa, 0x64, 0xd0,
]);
// 996_000_000e18 = 0x0337df3430954c8664000000
const ADMIN_BALANCE: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x03, 0x37, 0xdf, 0x34, 0x30, 0x95, 0x4c, 0x86, 0x64, 0x00, 0x00, 0x00,
]);

// ── AccessControl slots (ERC-7201: openzeppelin.storage.AccessControl) ──
// Base = 0x02dd7bc7dec4dceedda775e58dd541e08a116c6c53815c0bd028192f7b626800
// Slot = keccak256(abi.encode(account, keccak256(abi.encode(role, base))))

/// DEFAULT_ADMIN_ROLE.members[ADMIN]
const AC_DEFAULT_ADMIN: U256 = U256::from_be_slice(&[
    0x18, 0x60, 0x6a, 0xdf, 0x89, 0x45, 0xd7, 0x3b, 0xd9, 0xdd, 0x10, 0xd1, 0x4a, 0xc3, 0xc2, 0x94,
    0x5c, 0x07, 0x8a, 0xb7, 0xd7, 0x2c, 0x86, 0x32, 0xbd, 0xa6, 0x28, 0x34, 0x7c, 0x18, 0x1a, 0xdc,
]);

/// UPGRADER_ROLE.members[ADMIN]
const AC_UPGRADER: U256 = U256::from_be_slice(&[
    0x46, 0x87, 0xe7, 0xfc, 0xf6, 0xeb, 0x36, 0x25, 0xad, 0xda, 0xcb, 0x3f, 0xb4, 0x0e, 0xdc, 0xb2,
    0x88, 0x03, 0x6d, 0xda, 0x6f, 0x4a, 0x77, 0xcb, 0x31, 0x03, 0x6f, 0xa4, 0x6f, 0x8d, 0x2d, 0x75,
]);

/// PAUSER_ROLE.members[ADMIN]
const AC_PAUSER: U256 = U256::from_be_slice(&[
    0xe9, 0xbe, 0x56, 0x33, 0x17, 0x59, 0xda, 0xb8, 0x55, 0x24, 0xf1, 0xba, 0xcd, 0xa1, 0x2a, 0xd9,
    0xcb, 0xe0, 0xe3, 0x33, 0x7a, 0x00, 0x66, 0xc4, 0x00, 0xc8, 0xf0, 0xbb, 0xa3, 0xd8, 0x2f, 0x33,
]);

/// MINTER_ROLE.members[ADMIN]
const AC_MINTER: U256 = U256::from_be_slice(&[
    0x7c, 0x3b, 0xb6, 0x6a, 0x70, 0x82, 0x8c, 0xa1, 0xca, 0x88, 0x6d, 0x00, 0x02, 0xaa, 0x8b, 0x00,
    0xfb, 0xc2, 0x01, 0xce, 0xb4, 0x04, 0x47, 0x05, 0x0f, 0x91, 0x9d, 0x4b, 0xa6, 0x20, 0x2d, 0xad,
]);

/// BURNER_ROLE.members[ADMIN]
const AC_BURNER: U256 = U256::from_be_slice(&[
    0x69, 0xfa, 0x85, 0xe2, 0xdf, 0x5a, 0x70, 0x77, 0x6b, 0x68, 0x14, 0x9c, 0x11, 0x28, 0x39, 0x0f,
    0x90, 0x55, 0x00, 0x1b, 0xca, 0xba, 0x09, 0x6d, 0x3d, 0x55, 0x87, 0x96, 0x45, 0x58, 0x9e, 0x5c,
]);

// ── ConsensusRegistry stake balance slots ───────────────────────────
// StakeManager.balances mapping at slot 4: keccak256(abi.encode(addr, 4))

/// Stake amount per validator: 1,000,000 RLS = 1e24
#[allow(dead_code)] // documents the per-validator stake amount
const STAKE_AMOUNT: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00,
]);

// ── Bytecode ────────────────────────────────────────────────────────

const ERC1967_PROXY_BYTECODE: &[u8] = include_bytes!("bytecodes/rls_storage/erc1967_proxy.bin");

// ── Helpers ─────────────────────────────────────────────────────────

fn addr_to_u256(addr: Address) -> U256 {
    let mut buf = [0u8; 32];
    buf[12..].copy_from_slice(addr.as_slice());
    U256::from_be_bytes(buf)
}

fn set_slot(account: &mut RevmAccount, slot: U256, value: U256) {
    account.storage.insert(slot, EvmStorageSlot::new_changed(U256::ZERO, value, 0));
}

// ── Migration ───────────────────────────────────────────────────────

/// Build the state changes for the RlsStorage hardfork.
pub(crate) fn rls_storage_state() -> HashMap<Address, RevmAccount> {
    let mut state = HashMap::default();

    // ── RLS proxy: deploy bytecode + initialize storage ─────────────
    {
        let mut rls = account_with_code(ERC1967_PROXY_BYTECODE);

        // ERC-1967 impl slot
        set_slot(&mut rls, ERC1967_IMPL_SLOT, addr_to_u256(RLS_IMPL));

        // Initializable
        set_slot(&mut rls, INITIALIZABLE_SLOT, INITIALIZED_V1);

        // ERC-20 name and symbol
        set_slot(&mut rls, ERC20_NAME_SLOT, ERC20_NAME_VALUE);
        set_slot(&mut rls, ERC20_SYMBOL_SLOT, ERC20_SYMBOL_VALUE);

        // ERC-20 totalSupply and balances
        set_slot(&mut rls, ERC20_TOTAL_SUPPLY_SLOT, TOTAL_SUPPLY);
        set_slot(&mut rls, RLS_BAL_CR, CR_BALANCE);
        set_slot(&mut rls, RLS_BAL_ADMIN, ADMIN_BALANCE);

        // AccessControl roles
        set_slot(&mut rls, AC_DEFAULT_ADMIN, VALUE_TRUE);
        set_slot(&mut rls, AC_UPGRADER, VALUE_TRUE);
        set_slot(&mut rls, AC_PAUSER, VALUE_TRUE);
        set_slot(&mut rls, AC_MINTER, VALUE_TRUE);
        set_slot(&mut rls, AC_BURNER, VALUE_TRUE);

        state.insert(RLS_TOKEN, rls);
    }

    state
}

/// Genesis-state preconditions: the RLS proxy must be pristine — no code (the
/// migration deploys the pinned ERC1967 proxy) and no genesis storage (the
/// migration re-initializes name/symbol/totalSupply/balances/roles). A proxy
/// that already carries code or token state belongs to a chain that already
/// ran this migration, and re-initializing it would overwrite live state.
pub(crate) fn preconditions(sim: &crate::network_profile::SimAlloc) -> Vec<String> {
    let mut out = Vec::new();
    if sim.code(RLS_TOKEN).is_some() {
        out.push(format!(
            "RlsStorage: the RLS proxy at {RLS_TOKEN} already has code in the simulated state; \
             the migration deploys the pinned ERC1967 proxy and would replace it"
        ));
    }
    if sim.genesis_storage(RLS_TOKEN).is_some_and(|storage| !storage.is_empty()) {
        out.push(format!(
            "RlsStorage: the RLS proxy at {RLS_TOKEN} carries genesis storage; the migration \
             re-initializes name/symbol/totalSupply/balances/roles and would overwrite it"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_produces_expected_state() {
        let state = rls_storage_state();

        // RLS proxy should have bytecode
        let rls = &state[&RLS_TOKEN];
        assert!(rls.info.code.is_some(), "RLS should have proxy bytecode");

        // ERC-1967 impl slot
        let impl_slot = rls.storage.get(&ERC1967_IMPL_SLOT).expect("impl slot");
        assert_eq!(impl_slot.present_value(), addr_to_u256(RLS_IMPL));

        // Initialized
        let init = rls.storage.get(&INITIALIZABLE_SLOT).expect("init slot");
        assert_eq!(init.present_value(), INITIALIZED_V1);

        // Name and symbol
        assert_eq!(rls.storage.get(&ERC20_NAME_SLOT).unwrap().present_value(), ERC20_NAME_VALUE);
        assert_eq!(
            rls.storage.get(&ERC20_SYMBOL_SLOT).unwrap().present_value(),
            ERC20_SYMBOL_VALUE
        );

        // Supply and balances
        assert_eq!(
            rls.storage.get(&ERC20_TOTAL_SUPPLY_SLOT).unwrap().present_value(),
            TOTAL_SUPPLY
        );
        assert_eq!(rls.storage.get(&RLS_BAL_CR).unwrap().present_value(), CR_BALANCE);
        assert_eq!(rls.storage.get(&RLS_BAL_ADMIN).unwrap().present_value(), ADMIN_BALANCE);

        // Roles
        assert_eq!(rls.storage.get(&AC_DEFAULT_ADMIN).unwrap().present_value(), VALUE_TRUE);
        assert_eq!(rls.storage.get(&AC_UPGRADER).unwrap().present_value(), VALUE_TRUE);
        assert_eq!(rls.storage.get(&AC_PAUSER).unwrap().present_value(), VALUE_TRUE);
        assert_eq!(rls.storage.get(&AC_MINTER).unwrap().present_value(), VALUE_TRUE);
        assert_eq!(rls.storage.get(&AC_BURNER).unwrap().present_value(), VALUE_TRUE);

        // Verify accounting: CR balance + admin balance == totalSupply
        assert_eq!(CR_BALANCE + ADMIN_BALANCE, TOTAL_SUPPLY);
    }
}
