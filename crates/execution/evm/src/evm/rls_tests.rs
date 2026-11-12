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
    context::{
        result::{EVMError, ExecutionResult},
        BlockEnv, CfgEnv, TxEnv,
    },
    context_interface::{
        either::Either,
        result::InvalidTransaction,
        transaction::{Authorization, RecoveredAuthority, RecoveredAuthorization},
    },
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
const CLEARER: Address = address!("00000000000000000000000000000000000dead2");
const USDR_SENDER: Address = address!("90F79bf6EB2c4f870365E785982E1f101E93b906");

fn rls_tx(max_fee: u128, tip: Option<u128>, value: U256) -> TxEnv {
    TxEnv {
        tx_type: if tip.is_some() { 2 } else { 0 },
        caller: USER,
        kind: TxKind::Call(RECIPIENT),
        value,
        gas_limit: 21_000,
        gas_price: max_fee,
        gas_priority_fee: tip,
        chain_id: rls_flavour::rls_chain_id(CHAIN),
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
    assert_eq!(rls.max_fee, base * 2, "the signed max fee is kept for the affordability check");
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

/// An in-memory chain state with the RLS token (with code, like production, so EIP-161 never
/// clears its storage), a contract that reverts, a contract that clears storage slot 0 (set to
/// 1), and `user_rls` RLS for [`USER`].
fn test_state(user_rls: U256) -> State<CacheDB<EmptyDBTyped<Infallible>>> {
    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::default());
    let code = |bytes: &'static [u8]| AccountInfo {
        code: Some(Bytecode::new_raw(Bytes::from_static(bytes))),
        ..Default::default()
    };
    db.insert_account_info(RLS_TOKEN, code(&[0x00]));
    // PUSH1 0 PUSH1 0 REVERT
    db.insert_account_info(REVERTER, code(&[0x60, 0x00, 0x60, 0x00, 0xfd]));
    // PUSH1 0 PUSH1 0 SSTORE STOP: clears slot 0, which earns a gas refund
    db.insert_account_info(CLEARER, code(&[0x60, 0x00, 0x60, 0x00, 0x55, 0x00]));
    db.insert_account_storage(CLEARER, U256::ZERO, U256::from(1)).unwrap();
    db.insert_account_storage(RLS_TOKEN, balance_slot(USER), user_rls).unwrap();
    db.insert_account_info(
        USDR_SENDER,
        AccountInfo { balance: U256::from(10u128.pow(21)), ..Default::default() },
    );
    db.insert_account_storage(RLS_TOKEN, balance_slot(RECIPIENT), U256::from(10u128.pow(21)))
        .unwrap();
    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);
    state
}

/// Runs `txs` in order on one EVM (as a block does), committing each result like
/// `RaylsBlockExecutor::commit_transaction`, and returns each result or error.
fn run_all(
    state: &mut State<CacheDB<EmptyDBTyped<Infallible>>>,
    txs: Vec<TxEnv>,
) -> Vec<Result<ExecutionResult, InvalidTransaction>> {
    let chain_spec = test_chain_spec_arc();
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        chain_spec.chain_id(),
    ));
    let mut cfg = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::PRAGUE);
    cfg.chain_id = CHAIN;
    let env = EvmEnv::new(
        cfg,
        BlockEnv { number: U256::from(1), basefee: USDR_BASE_FEE, ..Default::default() },
    );
    let chain_spec = Arc::new(RaylsChainSpec::builder(chain_spec).build());
    let mut evm = RaylsEvmFactory::new(chain_spec).create_evm_with_native_erc20_only(state, env);
    let mut results = Vec::new();
    for tx in txs {
        match evm.transact_raw(tx) {
            Ok(out) => {
                evm.db_mut().commit(out.state);
                results.push(Ok(out.result));
            }
            Err(EVMError::Transaction(err)) => results.push(Err(err)),
            Err(err) => panic!("unexpected EVM error: {err:?}"),
        }
    }
    results
}

/// Runs one RLS-flavour tx the sender can pay for, returning the result and the RLS balances
/// before and after.
fn run(tx: TxEnv) -> (ExecutionResult, [U256; 4], [U256; 4]) {
    let mut state = test_state(U256::from(10u128.pow(21)));
    let before = rls_balances(&mut state);
    let result =
        run_all(&mut state, vec![tx]).remove(0).expect("an RLS tx the sender can pay for executes");
    let after = rls_balances(&mut state);
    (result, before, after)
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
    assert_eq!(after[1], before[1] + value, "recipient gets the value");
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

#[test]
fn gas_refund_is_paid_back_in_rls() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut tx = rls_tx(base, Some(0), U256::ZERO);
    tx.kind = TxKind::Call(CLEARER);
    tx.gas_limit = 60_000;
    let (result, before, after) = run(tx);
    let ExecutionResult::Success { gas_refunded, gas_used, .. } = result else {
        panic!("clearing a slot succeeds: {result:?}");
    };
    assert!(gas_refunded > 0, "clearing a set slot earns a refund");
    assert_eq!(total(before), total(after));
    // gas_used is net of the refund; the sender pays exactly that, the rest comes back
    let fee = U256::from(gas_used) * U256::from(base);
    assert_eq!(after[0], before[0] - fee);
    assert_eq!(after[2], before[2] + fee);
}

#[test]
fn failed_rls_debit_leaves_no_trace() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    // max fee 21,000 × base, plus 1 RLS of value, against 1 RLS: the debit fails
    let mut state = test_state(U256::from(10u128.pow(18)));
    let before = rls_balances(&mut state);
    let unpayable = rls_tx(base, Some(0), U256::from(10u128.pow(18)));
    // a payable tx after it, from another sender, on the same EVM: if the failed tx left journal
    // entries behind (the nonce bump before the debit), they would be committed with this one
    let mut next = rls_tx(base, Some(0), U256::ZERO);
    next.caller = RECIPIENT;
    next.kind = TxKind::Call(REVERTER);
    next.gas_limit = 50_000;
    let results = run_all(&mut state, vec![unpayable, next]);
    assert!(
        matches!(results[0], Err(InvalidTransaction::LackOfFundForMaxFee { .. })),
        "{:?}",
        results[0]
    );
    assert!(results[1].is_ok());
    let after = rls_balances(&mut state);
    assert_eq!(after[0], before[0], "the failed sender's RLS is untouched");
    assert_eq!(state.basic(USER).unwrap().map_or(0, |a| a.nonce), 0, "its nonce is not bumped");
    assert_eq!(total(before), total(after));
}

#[test]
fn affordability_uses_the_signed_max_fee() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    // max fee 3× the base fee: the sender can pay the effective price (base) but not the max
    let max_fee = base * 3;
    let effective_cost = U256::from(21_000u128 * base);
    let mut state = test_state(effective_cost * U256::from(2));
    let before = rls_balances(&mut state);
    let results = run_all(&mut state, vec![rls_tx(max_fee, Some(0), U256::ZERO)]);
    assert!(
        matches!(results[0], Err(InvalidTransaction::LackOfFundForMaxFee { .. })),
        "{:?}",
        results[0]
    );
    assert_eq!(rls_balances(&mut state), before);
    // with enough for gas limit × max fee it runs, and only the effective price is taken
    let mut state = test_state(U256::from(21_000u128 * max_fee));
    let before = rls_balances(&mut state);
    let results = run_all(&mut state, vec![rls_tx(max_fee, Some(0), U256::ZERO)]);
    assert!(results[0].as_ref().is_ok_and(|r| r.is_success()));
    assert_eq!(rls_balances(&mut state)[0], before[0] - effective_cost);
}

/// Runs `tx` against a funded user and asserts it is refused with `reason`, leaving RLS untouched.
fn assert_refused(mut state: State<CacheDB<EmptyDBTyped<Infallible>>>, tx: TxEnv, reason: &str) {
    let before = rls_balances(&mut state);
    let results = run_all(&mut state, vec![tx]);
    assert!(
        matches!(&results[0], Err(InvalidTransaction::Str(msg)) if msg == reason),
        "{:?}",
        results[0]
    );
    assert_eq!(rls_balances(&mut state), before);
    assert_eq!(state.basic(USER).unwrap().map_or(0, |a| a.nonce), 0);
}

#[test]
fn rls_value_to_a_contract_or_precompile_is_refused() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let reason = "RLS value can only go to an account without code that is not a precompile";
    for to in [REVERTER, address!("0000000000000000000000000000000000000400")] {
        let mut tx = rls_tx(base, Some(0), U256::from(1));
        tx.kind = TxKind::Call(to);
        tx.gas_limit = 50_000;
        assert_refused(test_state(U256::from(10u128.pow(21))), tx, reason);
    }
}

#[test]
fn rls_value_in_a_type_4_tx_is_refused() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut tx = rls_tx(base, Some(0), U256::from(1));
    tx.tx_type = 4;
    tx.gas_limit = 60_000;
    // the recipient delegates itself in this same tx
    tx.authorization_list = vec![Either::Right(RecoveredAuthorization::new_unchecked(
        Authorization {
            chain_id: U256::from(rls_flavour::rls_chain_id(CHAIN).unwrap()),
            address: REVERTER,
            nonce: 0,
        },
        RecoveredAuthority::Valid(RECIPIENT),
    ))];
    assert_refused(
        test_state(U256::from(10u128.pow(21))),
        tx,
        "RLS value cannot be sent in an EIP-7702 transaction",
    );
}

#[test]
fn paused_rls_refuses_rls_flavour_transactions() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut state = test_state(U256::from(10u128.pow(21)));
    state.insert_account_with_storage(
        RLS_TOKEN,
        AccountInfo {
            code: Some(Bytecode::new_raw(Bytes::from_static(&[0x00]))),
            ..Default::default()
        },
        [
            (rls_flavour::PAUSED_SLOT, U256::from(1)),
            (balance_slot(USER), U256::from(10u128.pow(21))),
        ]
        .into_iter()
        .collect(),
    );
    assert_refused(state, rls_tx(base, Some(0), U256::from(1)), "RLS is paused");
}

#[test]
fn usdr_tx_after_an_rls_tx_sees_the_chain_id_and_base_fee_restored() {
    let base = rls_flavour::rls_base_fee(USDR_BASE_FEE);
    let mut state = test_state(U256::from(10u128.pow(21)));
    let usdr = TxEnv {
        tx_type: 2,
        caller: USDR_SENDER,
        kind: TxKind::Call(RECIPIENT),
        value: U256::from(1),
        gas_limit: 21_000,
        gas_price: USDR_BASE_FEE as u128 * 2,
        gas_priority_fee: Some(0),
        chain_id: Some(CHAIN),
        ..Default::default()
    };
    let usdr_before = state.basic(USDR_SENDER).unwrap().unwrap().balance;
    let results = run_all(&mut state, vec![rls_tx(base, Some(0), U256::from(1)), usdr]);
    assert!(results.iter().all(|r| r.as_ref().is_ok_and(|r| r.is_success())), "{results:?}");
    // the USDr tx passed the chain id check (487, not 4871) and paid the USDr base fee, not 0
    let paid = usdr_before - state.basic(USDR_SENDER).unwrap().unwrap().balance;
    assert_eq!(paid, U256::from(21_000u128 * USDR_BASE_FEE as u128 + 1));
}
