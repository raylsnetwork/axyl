//! UsdrSupplyCorrection hardfork: rebase the USDR precompile's `TOTAL_SUPPLY`
//! storage slot to match the true on-chain supply.
//!
//! ## Problem
//!
//! Before [`super::erc20_precompile_bytecode`] (PR #404) installed `STOP`
//! bytecode on the USDR precompile account, EIP-161 state-clear was reaping
//! the account at the end of every transaction, wiping the `TOTAL_SUPPLY`
//! storage slot. `mint()` / `burn()` correctly mutated recipients' native
//! balances every time, but the supply counter was lost between transactions.
//!
//! Post-#404 the counter tracks correctly, but its baseline is wrong: it
//! never accounts for
//!   (a) the chain's genesis pre-allocations (which don't emit Transfer
//!       events and so never touch the slot), plus
//!   (b) pre-#404 mints whose slot writes were reaped.
//!
//! Anyone reading `totalSupply()` therefore sees a number smaller than the
//! sum of all native balances by exactly that historical shortfall.
//!
//! ## Fix
//!
//! At activation, this migration reads the precompile's current
//! `TOTAL_SUPPLY` slot value, adds the constant [`CORRECTION_WEI`] to it,
//! and writes the result back. From the activation block onward,
//! `totalSupply()` equals the true sum of native balances and stays correct
//! incrementally (because [`super::erc20_precompile_bytecode`] guarantees
//! mint/burn slot updates persist).
//!
//! See `etc/state-sum/README.md` for the full audit methodology and a
//! reproducible verification script.

use rayls_infrastructure_types::{Address, U256};
use reth_errors::BlockExecutionError;
use reth_revm::{
    primitives::HashMap,
    state::{Account as RevmAccount, EvmStorageSlot},
    Database, State,
};

use super::account_with_code;
// Reuse the canonical STOP byte from Erc20PrecompileBytecode so the two
// migrations can never silently diverge. We must reassert it here because
// the dispatcher's "no new code" branch (`account.info = cached.account_info()`)
// merges in a cached `AccountInfo` whose `code: None` (only `code_hash` is
// populated), and the bundle commit ends up clearing the bytecode reference —
// re-opening the EIP-161 reaping problem Erc20PrecompileBytecode closes.
use super::erc20_precompile_bytecode::STOP_BYTECODE;
// Re-use the canonical precompile address and the single source of truth for
// the `TOTAL_SUPPLY` storage slot from native_erc20 instead of re-declaring
// `address!(...)` / re-deriving the slot here.
use crate::native_erc20::{total_supply_slot, ERC20_PRECOMPILE_ADDRESS};

/// Value to add to the precompile's `TOTAL_SUPPLY` slot at activation.
///
/// `515_241_259_606_000_000_000_000` wei (= 515,241.259606 USDR).
/// See module-level docs and `etc/state-sum/README.md` for derivation
/// and reproducibility.
pub(super) const CORRECTION_WEI: U256 = U256::from_be_slice(&[
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6d, 0x1b, 0x48, 0xde, 0xe2, 0x77, 0x84, 0x93, 0x60, 0x00,
]);

/// The post-correction `TOTAL_SUPPLY` slot value for a given current value.
pub(crate) fn corrected_total_supply(current: U256) -> Result<U256, BlockExecutionError> {
    current.checked_add(CORRECTION_WEI).ok_or_else(|| {
        BlockExecutionError::msg(format!(
            "UsdrSupplyCorrection: U256 overflow adding correction \
             ({CORRECTION_WEI}) to current slot value ({current})"
        ))
    })
}

/// Build the state delta for the `UsdrSupplyCorrection` hardfork.
///
/// Unlike the other one-shot migrations in this module, the new slot value
/// depends on the current slot value at the activation block — so this
/// function takes `&mut State<DB>` rather than being a pure factory.
pub(crate) fn usdr_supply_correction_state<DB: alloy_evm::Database>(
    db: &mut State<DB>,
) -> Result<HashMap<Address, RevmAccount>, BlockExecutionError>
where
    DB::Error: core::fmt::Display,
{
    let slot = total_supply_slot();
    let current = db.storage(ERC20_PRECOMPILE_ADDRESS, slot).map_err(|e| {
        BlockExecutionError::msg(format!(
            "UsdrSupplyCorrection: failed to read TOTAL_SUPPLY slot: {e}"
        ))
    })?;

    let new_value = corrected_total_supply(current)?;

    // Build the migration's RevmAccount with STOP code explicitly included
    // (see STOP_BYTECODE comment above). The dispatcher will take the
    // "account.info.code.is_some()" branch and carry over nonce/balance
    // from the cached on-chain account, leaving the STOP byte in place.
    let mut account = account_with_code(STOP_BYTECODE);
    account.storage.insert(slot, EvmStorageSlot::new_changed(current, new_value, 0));
    let mut state = HashMap::default();
    state.insert(ERC20_PRECOMPILE_ADDRESS, account);
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_revm::{db::EmptyDBTyped, state::AccountStatus};

    /// Pin the correction value so accidental edits to the byte array trip a test.
    #[test]
    fn correction_constant_matches_audit() {
        let expected = U256::from_str_radix("515241259606000000000000", 10).unwrap();
        assert_eq!(CORRECTION_WEI, expected);
    }

    /// On an empty DB (slot reads as zero), the migration writes exactly
    /// CORRECTION_WEI into the slot.
    #[test]
    fn migration_from_zero_slot_writes_correction_exactly() {
        let mut db = State::builder()
            .with_database(EmptyDBTyped::<core::convert::Infallible>::new())
            .with_bundle_update()
            .build();

        let state = usdr_supply_correction_state(&mut db).expect("migration must succeed");
        let account = state.get(&ERC20_PRECOMPILE_ADDRESS).expect("precompile account");
        let slot = account
            .storage
            .get(&total_supply_slot())
            .expect("TOTAL_SUPPLY slot must be present in the delta");

        assert_eq!(slot.original_value(), U256::ZERO, "previous slot value (empty db)");
        assert_eq!(slot.present_value(), CORRECTION_WEI, "new slot value");
        assert!(slot.is_changed(), "slot must be flagged as changed");
        assert_eq!(account.status, AccountStatus::Touched);

        // The migration must reassert the STOP (0x00) bytecode on the precompile,
        // otherwise the bundle commit clears the code reference and re-opens the
        // EIP-161 reaping problem (see STOP_BYTECODE comment at module top).
        let code = account.info.code.as_ref().expect("STOP code must be set");
        assert_eq!(code.original_byte_slice(), STOP_BYTECODE, "must install STOP (0x00) bytecode");
    }
}
