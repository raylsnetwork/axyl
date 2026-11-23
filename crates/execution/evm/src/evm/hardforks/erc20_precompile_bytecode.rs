//! Erc20PrecompileBytecode hardfork: seed STOP bytecode on the native ERC-20 precompile
//! so EIP-161 end-of-tx cleanup cannot destroy its DynPrecompile-written storage.

use reth_revm::{primitives::AddressMap, state::Account as RevmAccount};

use super::account_with_code;
use crate::native_erc20::ERC20_PRECOMPILE_ADDRESS;

/// Single STOP byte. Enough to set `code_hash != KECCAK_EMPTY`, which keeps
/// EIP-161 end-of-tx cleanup from reaping the precompile account.
///
/// Visible to sibling hardfork modules so they can reassert this exact
/// bytecode when their own migrations touch the precompile account (see
/// `usdr_supply_correction.rs` — its corrective slot write would otherwise
/// drop the code reference through the dispatcher's cached-info merge).
pub(super) const STOP_BYTECODE: &[u8] = &[0x00];

/// Build the revm state map for the Erc20PrecompileBytecode hardfork.
pub(crate) fn erc20_precompile_bytecode_state() -> AddressMap<RevmAccount> {
    AddressMap::from_iter([(ERC20_PRECOMPILE_ADDRESS, account_with_code(STOP_BYTECODE))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::U256;
    use reth_revm::{primitives::keccak256, state::AccountStatus};

    #[test]
    fn migration_touches_only_the_precompile() {
        let state = erc20_precompile_bytecode_state();
        assert_eq!(state.len(), 1, "migration must touch only the precompile");
        assert!(
            state.contains_key(&ERC20_PRECOMPILE_ADDRESS),
            "the single touched account must be the precompile address",
        );
    }

    #[test]
    fn migration_installs_stop_bytecode_and_matching_hash() {
        let state = erc20_precompile_bytecode_state();
        let precompile = &state[&ERC20_PRECOMPILE_ADDRESS];
        let code = precompile.info.code.as_ref().expect("code must be set");
        assert_eq!(code.original_byte_slice(), STOP_BYTECODE);
        assert_eq!(precompile.info.code_hash, keccak256(STOP_BYTECODE));
    }

    /// EIP-161 invariant: an account is "empty" if
    /// `balance == 0 && nonce == 0 && code_hash == KECCAK_EMPTY`. After this migration
    /// the precompile has non-empty code, so end-of-tx cleanup must preserve it.
    #[test]
    fn migration_defeats_eip161_empty_check() {
        let state = erc20_precompile_bytecode_state();
        let precompile = &state[&ERC20_PRECOMPILE_ADDRESS];
        assert_ne!(
            precompile.info.code_hash,
            keccak256([]),
            "code_hash must differ from KECCAK_EMPTY or EIP-161 will destroy the account",
        );
        assert!(precompile.info.code.as_ref().is_some_and(|c| !c.is_empty()));
    }

    #[test]
    fn migration_does_not_touch_balance_nonce_or_storage() {
        let state = erc20_precompile_bytecode_state();
        let precompile = &state[&ERC20_PRECOMPILE_ADDRESS];
        assert_eq!(precompile.info.nonce, 0);
        assert_eq!(precompile.info.balance, U256::ZERO);
        assert!(precompile.storage.is_empty(), "migration must not write storage");
    }

    #[test]
    fn migration_marks_account_touched_for_dispatcher() {
        let state = erc20_precompile_bytecode_state();
        let precompile = &state[&ERC20_PRECOMPILE_ADDRESS];
        assert!(
            precompile.status.contains(AccountStatus::Touched),
            "account must be Touched so apply_activated_migrations records the transition",
        );
    }

    #[test]
    fn migration_is_idempotent() {
        let a = erc20_precompile_bytecode_state();
        let b = erc20_precompile_bytecode_state();
        let pa = &a[&ERC20_PRECOMPILE_ADDRESS];
        let pb = &b[&ERC20_PRECOMPILE_ADDRESS];
        assert_eq!(pa.info.code_hash, pb.info.code_hash);
        assert_eq!(
            pa.info.code.as_ref().map(|c| c.original_byte_slice().to_vec()),
            pb.info.code.as_ref().map(|c| c.original_byte_slice().to_vec()),
        );
    }
}
