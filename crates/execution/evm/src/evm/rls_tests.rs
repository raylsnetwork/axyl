//! Unit tests for RLS-flavour pricing (`rls_gas`) and the handler's RLS accounting.

use super::rls_gas;
use crate::{
    chainspec::RaylsChainSpec,
    evm::{initialize_erc20_precompile, RaylsEvmFactory},
    native_erc20::{precompile::Erc20Precompile, Erc20TokenConfig, ERC20_PRECOMPILE_ADDRESS},
    rls_flavour::{self, balance_slot, RLS_FEE_RECIPIENT, RLS_TOKEN},
};
use alloy::primitives::{address, Address, Bytes, U256};
use alloy_evm::Evm as _;
use rayls_infrastructure_types::{test_chain_spec_arc, TxKind};
use reth_chainspec::EthChainSpec;
use reth_evm::EvmEnv;
use reth_revm::{
    bytecode::Bytecode,
    context::{result::ExecutionResult, BlockEnv, CfgEnv, TxEnv},
    context_interface::result::InvalidTransaction,
    db::{CacheDB, EmptyDBTyped},
    primitives::hardfork::SpecId,
    state::AccountInfo,
    Database as _, DatabaseCommit as _, State,
};
use std::{convert::Infallible, sync::Arc};

const CHAIN: u64 = 487;
const USDR_BASE_FEE: u64 = 48_000_000_000;
const USER: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");
const RECIPIENT: Address = address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC");
const REVERTER: Address = address!("00000000000000000000000000000000000dead1");

fn rls_tx(max_fee: u128, tip: Option<u128>, value: U256) -> TxEnv {
    TxEnv {
        tx_type: if tip.is_some() { 2 } else { 0 },
        caller: USER,
        kind: TxKind::Call(RECIPIENT),
        value,
        gas_limit: 21_000,
        gas_price: max_fee,
        gas_priority_fee: tip,
        chain_id: Some(rls_flavour::rls_chain_id(CHAIN)),
        ..Default::default()
    }
}

#[test]
fn usdr_chain_id_is_untouched() {
    let mut tx = rls_tx(1, Some(1), U256::from(5));
    tx.chain_id = Some(CHAIN);
    let before = tx.clone();
    assert!(rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, false).unwrap().is_none());
    assert_eq!(tx, before);
}

#[test]
fn fee_cap_under_rls_base_fee_is_refused_unless_simulating() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut tx = rls_tx(base - 1, Some(0), U256::ZERO);
    assert_eq!(
        rls_gas(&mut tx.clone(), CHAIN, USDR_BASE_FEE, false).unwrap_err(),
        InvalidTransaction::GasPriceLessThanBasefee
    );
    assert!(rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, true).unwrap().is_some());
}

#[test]
fn tip_above_fee_cap_is_refused_even_when_simulating() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    for simulating in [false, true] {
        let mut tx = rls_tx(base, Some(base + 1), U256::ZERO);
        assert_eq!(
            rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, simulating).unwrap_err(),
            InvalidTransaction::PriorityFeeGreaterThanMaxFee
        );
    }
}

#[test]
fn price_and_zeroed_fields() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let value = U256::from(7);
    // type 2: base fee + tip, capped by the max fee
    let mut tx = rls_tx(base * 2, Some(10), value);
    let rls = rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, false).unwrap().unwrap();
    assert_eq!(rls.price, base + 10);
    assert_eq!(rls.value, value);
    assert_eq!((tx.gas_price, tx.gas_priority_fee, tx.value), (0, Some(0), U256::ZERO));
    let mut tx = rls_tx(base + 5, Some(10), value);
    assert_eq!(rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, false).unwrap().unwrap().price, base + 5);
    // legacy: the gas price
    let mut tx = rls_tx(base * 3, None, value);
    let rls = rls_gas(&mut tx, CHAIN, USDR_BASE_FEE, false).unwrap().unwrap();
    assert_eq!(rls.price, base * 3);
    assert_eq!((tx.gas_price, tx.gas_priority_fee, tx.value), (0, None, U256::ZERO));
}

/// RLS balances of every account the handler touches.
fn rls_balances(state: &mut State<CacheDB<EmptyDBTyped<Infallible>>>) -> [U256; 4] {
    [USER, RECIPIENT, RLS_FEE_RECIPIENT, REVERTER]
        .map(|owner| state.storage(RLS_TOKEN, balance_slot(owner)).expect("in-memory db"))
}

/// Runs one RLS-flavour tx through `transact_raw` and the commit step, returning the result and
/// the RLS balances before and after.
fn run(tx: TxEnv) -> (ExecutionResult, [U256; 4], [U256; 4]) {
    let chain_spec = test_chain_spec_arc();
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        chain_spec.chain_id(),
    ));
    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::default());
    // like production, the token has code, so EIP-161 never clears its storage
    db.insert_account_info(
        RLS_TOKEN,
        AccountInfo {
            code: Some(Bytecode::new_raw(Bytes::from_static(&[0x00]))),
            ..Default::default()
        },
    );
    // PUSH1 0 PUSH1 0 REVERT
    db.insert_account_info(
        REVERTER,
        AccountInfo {
            code: Some(Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xfd]))),
            ..Default::default()
        },
    );
    db.insert_account_storage(RLS_TOKEN, balance_slot(USER), U256::from(10u128.pow(21))).unwrap();
    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);
    let before = rls_balances(&mut state);

    let mut cfg = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::PRAGUE);
    cfg.chain_id = CHAIN;
    let env = EvmEnv::new(
        cfg,
        BlockEnv { number: U256::from(1), basefee: USDR_BASE_FEE, ..Default::default() },
    );
    let chain_spec = Arc::new(RaylsChainSpec::builder(chain_spec).build());
    let mut evm =
        RaylsEvmFactory::new(chain_spec).create_evm_with_native_erc20_only(&mut state, env);
    let out = evm.transact_raw(tx).expect("an RLS tx the sender can pay for executes");
    drop(evm);
    state.commit(out.state);
    let after = rls_balances(&mut state);
    (out.result, before, after)
}

fn total(balances: [U256; 4]) -> U256 {
    balances.into_iter().fold(U256::ZERO, |sum, b| sum + b)
}

#[test]
fn successful_send_conserves_rls() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let value = U256::from(10u128.pow(18));
    let (result, before, after) = run(rls_tx(base * 2, Some(1), value));
    let gas_used = result.gas_used();
    assert!(result.is_success());
    assert_eq!(total(before), total(after));
    let fee = U256::from(gas_used) * U256::from(base + 1);
    assert_eq!(after[0], before[0] - value - fee, "sender pays value + gas used × price");
    assert_eq!(after[1], value, "recipient gets the value");
    assert_eq!(after[2], fee, "fee recipient gets the gas actually used");
    // the value and the gas each emit one Transfer
    assert_eq!(result.logs().len(), 2);
}

#[test]
fn reverted_call_still_pays_gas_and_conserves_rls() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut tx = rls_tx(base, Some(0), U256::ZERO);
    tx.kind = TxKind::Call(REVERTER);
    tx.gas_limit = 50_000;
    let (result, before, after) = run(tx);
    assert!(matches!(result, ExecutionResult::Revert { .. }));
    assert_eq!(total(before), total(after));
    let fee = U256::from(result.gas_used()) * U256::from(base);
    assert_eq!(after[0], before[0] - fee, "unused gas is refunded");
    assert_eq!(after[2], fee);
}
