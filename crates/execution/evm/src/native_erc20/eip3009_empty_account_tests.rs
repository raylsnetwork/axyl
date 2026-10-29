//! Regression tests for the EIP-3009 "empty account" chain halt.
//!
//! A fresh EOA (nonce 0, no code) that signs away its entire native ERC-20 balance via
//! EIP-3009 ends the transaction as an EIP-161 empty account while still `Loaded` in the
//! state cache. Committing that state panics with:
//!
//! ```text
//! internal error: entered unreachable code: Wrong state transition,
//! touch empty is not possible from Loaded
//! ```
//!
//! `eth_call` never commits, so the bug only shows up in block execution. Each test below
//! runs the production pair: `transact` on the block-execution EVM, then `State::commit`.
//!
//! [`full_drain_of_nonce0_account`] is expected to FAIL on v1.4.1 with the panic above.
//! The other two tests are controls that pass today and must keep passing: a partial drain
//! leaves the victim non-empty, and a nonce-1 account is never EIP-161 empty.

use crate::{
    chainspec::RaylsChainSpec,
    evm::{initialize_erc20_precompile, RaylsEvmFactory},
    native_erc20::{
        abi::eip3009, precompile::Erc20Precompile, storage::authorization_nonce_slot,
        Erc20TokenConfig, ERC20_PRECOMPILE_ADDRESS,
    },
};
use alloy::{
    primitives::{address, Address, Bytes, B256, U256},
    sol_types::{SolCall as _, SolStruct as _},
};
use rayls_infrastructure_types::test_chain_spec_arc;
use reth_evm::EvmEnv;
use reth_revm::{
    context::{
        result::{ExecutionResult, ResultAndState},
        BlockEnv, CfgEnv,
    },
    db::{CacheDB, EmptyDBTyped},
    primitives::hardfork::SpecId,
    state::AccountInfo,
    Database as _, DatabaseCommit as _, State,
};
use secp256k1::{ecdsa::RecoveryId, Message, SecretKey, SECP256K1};
use std::{convert::Infallible, sync::Arc};

/// Fresh EOA that signs the EIP-3009 authorization (hardhat key #1).
const VICTIM: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");
/// VICTIM's private key.
const VICTIM_KEY: &str = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

/// EOA that submits the relayer transaction (hardhat key #2).
const RELAYER: Address = address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC");

/// Authorization nonce used by every signed transfer.
const AUTH_NONCE: B256 = B256::repeat_byte(1);
/// Any block >= 1 (Spurious Dragon / EIP-161 active).
const BLOCK_NUMBER: u64 = 1;

/// VICTIM's whole balance: 5 USDr (the native token *is* the ERC-20 at `0x0400`).
fn full_balance() -> U256 {
    U256::from(5u64) * U256::from(10u64).pow(U256::from(18u64))
}

/// The "works fine" control amount from the incident: 3 of the 5 USDr.
fn partial_amount() -> U256 {
    U256::from(3u64) * U256::from(10u64).pow(U256::from(18u64))
}

/// RELAYER's native balance, well above any gas cost (pre-genesis calls are gas-price 0).
fn relayer_balance() -> U256 {
    U256::from(10u64).pow(U256::from(21u64))
}

fn victim_secret_key() -> SecretKey {
    let mut bytes = [0u8; 32];
    for (i, pair) in VICTIM_KEY.as_bytes().chunks_exact(2).enumerate() {
        bytes[i] =
            u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex pair");
    }
    SecretKey::from_byte_array(bytes).expect("valid secp256k1 scalar")
}

/// EIP-712 domain of the process-global precompile instance registered in [`drain`]
/// (default token config, chain id 1, verifying contract `0x0400`).
fn precompile_domain() -> alloy::sol_types::Eip712Domain {
    let config = Erc20TokenConfig::default();
    alloy::sol_types::Eip712Domain {
        name: Some(config.name.into()),
        version: Some(config.version.into()),
        chain_id: Some(U256::from(1u64)),
        verifying_contract: Some(ERC20_PRECOMPILE_ADDRESS),
        salt: None,
    }
}

/// VICTIM's signed `transferWithAuthorization` moving `value` from VICTIM to RELAYER.
fn transfer_calldata(value: U256) -> Bytes {
    let digest = eip3009::TransferWithAuthorizationStruct {
        from: VICTIM,
        to: RELAYER,
        value,
        validAfter: U256::ZERO,
        validBefore: U256::MAX,
        nonce: AUTH_NONCE,
    }
    .eip712_signing_hash(&precompile_domain());

    // libsecp256k1 signs deterministically (RFC 6979) and normalizes to low-s, so the
    // verifier's malleability check cannot trip.
    let signature =
        SECP256K1.sign_ecdsa_recoverable(Message::from_digest(digest.into()), &victim_secret_key());
    let (recovery_id, compact) = signature.serialize_compact();
    let v = match recovery_id {
        RecoveryId::Zero => 27,
        RecoveryId::One => 28,
        RecoveryId::Two => 29,
        RecoveryId::Three => 30,
    };
    let (r, s) = compact.split_at(32);

    eip3009::TransferWithAuthorizationCall {
        from: VICTIM,
        to: RELAYER,
        value,
        validAfter: U256::ZERO,
        validBefore: U256::MAX,
        nonce: AUTH_NONCE,
        v,
        r: B256::from_slice(r),
        s: B256::from_slice(s),
    }
    .abi_encode()
    .into()
}

/// Post-commit view of one drain transaction.
struct DrainRun {
    result: ExecutionResult,
    victim: Option<AccountInfo>,
    relayer: Option<AccountInfo>,
    auth_nonce_used: U256,
}

/// Run one EIP-3009 drain exactly like the block executor: `transact`, then commit.
///
/// On v1.4.1 the full drain of the nonce-0 victim panics inside `State::commit`; that panic
/// is the bug under test.
fn drain(victim_nonce: u64, amount: U256) -> DrainRun {
    // Process-global; the EIP-712 domain above must match this instance.
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        1,
    ));

    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::new());
    db.insert_account_info(
        VICTIM,
        AccountInfo { balance: full_balance(), nonce: victim_nonce, ..Default::default() },
    );
    db.insert_account_info(
        RELAYER,
        AccountInfo { balance: relayer_balance(), nonce: 0, ..Default::default() },
    );

    // Same construction as the production block executor (`reth_env/execution.rs`), then
    // re-enable state clearing per block (`apply_pre_execution_changes`).
    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);

    let env = EvmEnv::new(
        CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN),
        BlockEnv { number: U256::from(BLOCK_NUMBER), ..Default::default() },
    );
    let chain_spec =
        Arc::new(RaylsChainSpec::builder(test_chain_spec_arc()).precompile_gas_fix(0).build());
    let mut evm =
        RaylsEvmFactory::default().create_evm_with_native_erc20_only(&mut state, env, chain_spec);

    let ResultAndState { result, state: diff } = evm
        .transact_pre_genesis_call(RELAYER, ERC20_PRECOMPILE_ADDRESS, transfer_calldata(amount))
        .expect("transact must succeed: the incident panics at commit, not execution");

    // Production commit step (`RaylsBlockExecutor::commit_transaction`).
    state.commit(diff);

    DrainRun {
        result,
        victim: state.basic(VICTIM).expect("in-memory db read"),
        relayer: state.basic(RELAYER).expect("in-memory db read"),
        auth_nonce_used: state
            .storage(ERC20_PRECOMPILE_ADDRESS, authorization_nonce_slot(VICTIM, AUTH_NONCE))
            .expect("in-memory db read"),
    }
}

/// Draining the whole balance of a fresh nonce-0 EOA.
///
/// RED on v1.4.1: the commit panics. After the fix the transfer must execute and the victim
/// must be removed from state (EIP-161). A fix that merely skips the touch would leave the
/// balance change un-persisted, so the credit and nonce assertions guard against that.
#[test]
fn full_drain_of_nonce0_account() {
    let run = drain(0, full_balance());

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the full drain must execute: {:?}",
        run.result
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + full_balance()),
        "the relayer must be credited the full drained amount"
    );
    assert_eq!(run.auth_nonce_used, U256::ONE, "the authorization nonce must be marked used");
    assert_eq!(
        run.victim, None,
        "the drained nonce-0 account must be removed from state (EIP-161); \
         if it still exists, the balance change was never persisted"
    );
}

/// Control: draining 3 of 5 USDr works today — the victim stays non-empty, so the commit
/// takes the legal `change()` transition. Must keep passing after the fix.
#[test]
fn partial_drain_of_nonce0_account() {
    let run = drain(0, partial_amount());

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the partial drain must succeed: {:?}",
        run.result
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + partial_amount()),
        "the relayer must be credited the partial amount"
    );
    assert_eq!(
        run.victim.map(|info| info.balance),
        Some(full_balance() - partial_amount()),
        "the victim must keep the remainder"
    );
}

/// Control: an account with nonce 1 is not EIP-161 empty even at balance 0. Must keep
/// passing after the fix.
#[test]
fn full_drain_of_nonce1_account() {
    let run = drain(1, full_balance());

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the nonce-1 full drain must succeed: {:?}",
        run.result
    );
    let victim = run.victim.expect("a nonce-1 account stays in state");
    assert_eq!(victim.balance, U256::ZERO, "the victim must be fully drained");
    assert_eq!(victim.nonce, 1, "the token transfer must not change the nonce");
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + full_balance()),
        "the relayer must be credited the full drained amount"
    );
}
