//! Regression tests for the EIP-3009 "empty account" chain halt.
//!
//! A fresh EOA (nonce 0, no code) that signs away its entire native ERC-20 balance via
//! EIP-3009 would end the transaction as an EIP-161 empty account while still `Loaded` in
//! the state cache. Committing that state panicked on v1.4.1 with:
//!
//! ```text
//! internal error: entered unreachable code: Wrong state transition,
//! touch empty is not possible from Loaded
//! ```
//!
//! `eth_call` never commits, so the bug only showed up in block execution. Each test below
//! runs the production pair: `transact` on the block-execution EVM, then `State::commit`.
//!
//! The fix rejects any debit that would leave the account EIP-161 empty, so
//! [`full_drain_of_nonce0_account_is_rejected`] must revert without touching state. The
//! other two tests are controls: a partial drain leaves the victim non-empty, and a nonce-1
//! account is never EIP-161 empty.
//!
//! The reject itself is gated behind [`RaylsHardFork::Eip161EmptyAccountGuard`]. v1.4.1
//! already committed the reviewer's two-transaction shape (fund a brand-new account in tx1,
//! drain it in tx2) because the victim was `InMemoryChange`, not `Loaded`. The
//! `replay_shape_*` pair pins both sides of the fork: inactive leaves history untouched,
//! active rejects the drain.
//!
//! The guard is checked on the debited account in every debit handler, so the rejection is
//! pinned per selector: `receiveWithAuthorization` (the receiver relays, the debited
//! account signs), `transferFrom` (an EOA stands in for the contract spender), and
//! `burnFrom` (called by the whitelisted minting module), each with a full-drain reject
//! and a partial-drain control. The `replay_shape_*_changed_account_*` pair extends the
//! replay shape to a pre-existing victim (credit in tx1, full drain in tx2): pre-fork the
//! emptied account still commits, because its first change promoted it out of `Loaded`;
//! post-fork the guard reverts it.
//!
//! One revm quirk these tests pin: the precompile account (`0x0400`) is an EIP-161 empty
//! account, so its auth-nonce storage writes are dropped when the transaction state is
//! committed (`CacheAccount::touch_empty_eip161`). The nonce is therefore only observable
//! in the transaction's `EvmState` diff (pre-commit), which is why [`DrainRun`] carries
//! both `auth_nonce_in_diff` and the post-commit `auth_nonce_used`.

use crate::{
    chainspec::RaylsChainSpec,
    evm::{initialize_erc20_precompile, RaylsEvmFactory, ERC20_PRECOMPILE_INSTANCE},
    native_erc20::{
        abi::{eip3009, erc20},
        precompile::Erc20Precompile,
        storage::{allowance_slot, authorization_nonce_slot, total_supply_slot},
        Erc20TokenConfig, ERC20_PRECOMPILE_ADDRESS, MINTING_MODULE_ADDRESS,
    },
};
use alloy::{
    primitives::{address, Address, Bytes, B256, U256},
    sol_types::{SolCall as _, SolStruct as _},
};
use alloy_evm::Evm as _;
use rayls_infrastructure_types::{test_chain_spec_arc, TxKind, ETHEREUM_BLOCK_GAS_LIMIT_56BITS};
use reth_chainspec::EthChainSpec;
use reth_evm::EvmEnv;
use reth_revm::{
    bytecode::Bytecode,
    context::{
        result::{ExecutionResult, ResultAndState},
        BlockEnv, CfgEnv, TxEnv,
    },
    db::{CacheDB, EmptyDBTyped},
    primitives::{hardfork::SpecId, StorageKey, StorageValue},
    state::{AccountInfo, EvmState},
    Database, DatabaseCommit as _, State,
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

/// The pre-existing balance seeded in the changed-account replay shape: 1 USDr.
fn base_amount() -> U256 {
    U256::from(10u64).pow(U256::from(18u64))
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

/// EIP-712 domain of whichever precompile instance won the process-wide
/// [`initialize_erc20_precompile`] race (verifying contract `0x0400`).
///
/// `ERC20_PRECOMPILE_INSTANCE` is first-wins, and other tests in this binary (the `reth_env`
/// tests register with the test chain's id) may populate it before any test in this file
/// does. Signing against a domain the registered instance does not verify turns every
/// signed call into an `InvalidAuthorizer` revert, so the domain must be read from the
/// instance that actually won rather than assumed. The read self-registers with the test
/// chain's id first (a first-wins no-op when the instance is already set), so the calldata
/// helpers may sign at the call site — before any entry point's body runs — and still find
/// a guaranteed-set instance.
fn precompile_domain() -> alloy::sol_types::Eip712Domain {
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        test_chain_spec_arc().chain_id(),
    ));
    ERC20_PRECOMPILE_INSTANCE
        .get()
        .expect("the global precompile is registered above")
        .read()
        .domain
        .clone()
}

/// VICTIM's EIP-712 signature over the transfer struct moving `value` from VICTIM to
/// RELAYER, signed with VICTIM's key.
///
/// Valid for both `transferWithAuthorization` and `receiveWithAuthorization`: the
/// precompile's `verify_transfer_authorization` always hashes the *transfer* struct (never
/// the receive struct), so the digest — and hence the signature — is identical for both
/// selectors. No relayer key is needed: the signer is the debited VICTIM.
///
/// libsecp256k1 signs deterministically (RFC 6979) and normalizes to low-s, so the
/// verifier's malleability check cannot trip.
fn eip3009_signature(value: U256) -> (u8, B256, B256) {
    let digest = eip3009::TransferWithAuthorizationStruct {
        from: VICTIM,
        to: RELAYER,
        value,
        validAfter: U256::ZERO,
        validBefore: U256::MAX,
        nonce: AUTH_NONCE,
    }
    .eip712_signing_hash(&precompile_domain());

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
    (v, B256::from_slice(r), B256::from_slice(s))
}

/// VICTIM's signed `transferWithAuthorization` moving `value` from VICTIM to RELAYER.
fn transfer_calldata(value: U256) -> Bytes {
    let (v, r, s) = eip3009_signature(value);
    eip3009::TransferWithAuthorizationCall {
        from: VICTIM,
        to: RELAYER,
        value,
        validAfter: U256::ZERO,
        validBefore: U256::MAX,
        nonce: AUTH_NONCE,
        v,
        r,
        s,
    }
    .abi_encode()
    .into()
}

/// VICTIM's signed `receiveWithAuthorization` moving `value` from VICTIM to RELAYER.
///
/// RELAYER (the receiver) submits this; the EIP-712 signer is VICTIM, the debited account
/// (see [`eip3009_signature`]).
fn receive_calldata(value: U256) -> Bytes {
    let (v, r, s) = eip3009_signature(value);
    eip3009::ReceiveWithAuthorizationCall {
        from: VICTIM,
        to: RELAYER,
        value,
        validAfter: U256::ZERO,
        validBefore: U256::MAX,
        nonce: AUTH_NONCE,
        v,
        r,
        s,
    }
    .abi_encode()
    .into()
}

/// `transferFrom` moving `value` from VICTIM to RELAYER. The tx caller (RELAYER) is the
/// spender, so the seeded `(VICTIM -> RELAYER)` allowance must cover the amount.
fn transfer_from_calldata(value: U256) -> Bytes {
    erc20::transferFromCall { from: VICTIM, to: RELAYER, amount: value }.abi_encode().into()
}

/// `burnFrom` burning `value` from VICTIM. The tx caller must be the whitelisted minting
/// module, which also needs the seeded `(VICTIM -> MINTING_MODULE)` allowance.
fn burn_from_calldata(value: U256) -> Bytes {
    erc20::burnFromCall { account: VICTIM, amount: value }.abi_encode().into()
}

/// Post-commit view of one drain transaction.
struct DrainRun {
    result: ExecutionResult,
    victim: Option<AccountInfo>,
    relayer: Option<AccountInfo>,
    /// The auth nonce slot in the *post-commit* state.
    ///
    /// This is `ZERO` in both fork directions: the precompile account (`0x0400`) is an
    /// EIP-161 empty account, and revm's commit drops the storage writes of an emptied
    /// account (`CacheAccount::touch_empty_eip161`). That is a pre-existing revm
    /// behavior the guard does not change.
    auth_nonce_used: U256,
    /// The auth nonce slot in the transaction's `EvmState` diff (pre-commit).
    ///
    /// This is the fork-discriminating signal: a successful drain consumes the nonce in
    /// the journal, a rejected drain rolls the consumption back.
    auth_nonce_in_diff: Option<U256>,
}

/// Read the auth nonce slot out of a transaction diff (before it is committed), if the
/// slot was actually changed by the transaction. A reverted drain leaves the slot entry
/// behind with its original value, so unchanged slots read as `None`.
fn nonce_slot_in_diff(diff: &EvmState) -> Option<U256> {
    diff.get(&ERC20_PRECOMPILE_ADDRESS)?
        .storage
        .get(&authorization_nonce_slot(VICTIM, AUTH_NONCE))
        .filter(|slot| slot.is_changed())
        .map(|slot| slot.present_value)
}

/// Decode the reason of a precompile revert.
///
/// Precompile errors are encoded like Solidity's `Error(string)` (cf.
/// `encode_revert_reason` in `native_erc20::abi`): selector `0x08c379a0`, then an ABI
/// `string` — offset word, length word, zero-padded data. Returns `None` when the output is
/// not that encoding, so a caller can fail loudly instead of asserting on an unrelated
/// revert (e.g. an `InvalidAuthorizer` from an EIP-712 domain mismatch).
fn revert_reason(output: &Bytes) -> Option<String> {
    const ERROR_STRING_SELECTOR: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];
    let rest = output.get(4..)?;
    if rest.len() < 64 {
        return None;
    }
    let offset = U256::from_be_slice(&rest[0..32]).to::<usize>();
    let data = rest.get(offset..)?;
    if data.len() < 32 {
        return None;
    }
    let len = U256::from_be_slice(&data[0..32]).to::<usize>();
    std::str::from_utf8(data.get(32..32 + len)?).map(str::to_owned).ok()
}

/// Run one precompile call exactly like the block executor: `transact`, then commit.
///
/// `State::commit` must never panic: a call the precompile rejects is returned as
/// [`ExecutionResult::Revert`] and contributes no state changes. VICTIM holds
/// [`full_balance`] at nonce `victim_nonce`; RELAYER is the submitting (relayer) caller.
fn drain(victim_nonce: u64, calldata: Bytes) -> DrainRun {
    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::new());
    db.insert_account_info(
        VICTIM,
        AccountInfo { balance: full_balance(), nonce: victim_nonce, ..Default::default() },
    );
    db.insert_account_info(
        RELAYER,
        AccountInfo { balance: relayer_balance(), nonce: 0, ..Default::default() },
    );
    run_eip3009_call(db, RELAYER, calldata)
}

/// Like [`drain`], for the allowance-gated selectors (`transferFrom`, `burnFrom`).
///
/// Seeds the `(owner -> spender)` allowance and the total supply in the precompile
/// account's storage, and submits `calldata` from `caller` (RELAYER for `transferFrom`,
/// the whitelisted minting module for `burnFrom`).
///
/// The allowance is seeded in the database rather than set by an `approve` transaction:
/// the precompile account (`0x0400`) is an EIP-161 empty account, so revm's commit drops
/// its storage writes (the same pinned quirk as the auth nonce) and an `approve` tx's
/// allowance would not survive. Total supply is seeded for `burnFrom`'s control: the burn
/// is a `checked_sub` against it, and the unseeded slot reads zero.
///
/// The precompile account is seeded with *empty* info (it exists in the db, but is still
/// EIP-161 empty) rather than left absent: `State` treats a *not-existing* account as
/// "storage known" and short-circuits its slot reads to zero without ever consulting the
/// database, so an absent account's seeded slots are invisible. An empty account loads as
/// `LoadedEmptyEIP161` instead, and its slots fall through to the seeded `CacheDB` values.
fn drain_with_allowance(
    caller: Address,
    owner: Address,
    spender: Address,
    calldata: Bytes,
) -> DrainRun {
    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::new());
    db.insert_account_info(
        VICTIM,
        AccountInfo { balance: full_balance(), nonce: 0, ..Default::default() },
    );
    db.insert_account_info(
        RELAYER,
        AccountInfo { balance: relayer_balance(), nonce: 0, ..Default::default() },
    );
    // Exist but stay EIP-161 empty (see the doc comment): this is what makes the seeded
    // slots below readable through `State`.
    db.insert_account_info(ERC20_PRECOMPILE_ADDRESS, AccountInfo::default());
    db.insert_account_storage(
        ERC20_PRECOMPILE_ADDRESS,
        allowance_slot(owner, spender),
        full_balance(),
    )
    .expect("in-memory db write");
    db.insert_account_storage(ERC20_PRECOMPILE_ADDRESS, total_supply_slot(), full_balance())
        .expect("in-memory db write");
    run_eip3009_call(db, caller, calldata)
}

/// Shared tail of [`drain`] and [`drain_with_allowance`]: build the production EVM over
/// the already-seeded `db`, submit `calldata` from `caller`, commit like the block
/// executor, and read the post-commit view.
fn run_eip3009_call(
    db: CacheDB<EmptyDBTyped<Infallible>>,
    caller: Address,
    calldata: Bytes,
) -> DrainRun {
    // Process-global and first-wins: register with the test chain's id (the chain the EVM
    // below runs on) so every test in this binary agrees on the domain `precompile_domain`
    // reads, regardless of which test registers first. `create_evm_with_native_erc20_only`
    // also requires the instance to be set, and unsigned calldata (e.g. `burnFrom`) never
    // triggers the `precompile_domain` self-registration.
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        test_chain_spec_arc().chain_id(),
    ));

    // Same construction as the production block executor (`reth_env/execution.rs`), then
    // re-enable state clearing per block (`apply_pre_execution_changes`).
    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);

    let env = EvmEnv::new(
        CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN),
        BlockEnv { number: U256::from(BLOCK_NUMBER), ..Default::default() },
    );
    // Post-fork spec: the EIP-161 empty-account guard is active, so a full drain of a
    // nonce-0 account is rejected.
    let chain_spec = Arc::new(
        RaylsChainSpec::builder(test_chain_spec_arc())
            .precompile_gas_fix(0)
            .eip161_empty_account_guard(0)
            .build(),
    );
    let mut evm =
        RaylsEvmFactory::new(chain_spec).create_evm_with_native_erc20_only(&mut state, env);

    let ResultAndState { result, state: diff } = evm
        .transact_pre_genesis_call(caller, ERC20_PRECOMPILE_ADDRESS, calldata)
        .expect("transact must not fail; a rejected call returns as a revert result");

    // Production commit step (`RaylsBlockExecutor::commit_transaction`).
    let auth_nonce_in_diff = nonce_slot_in_diff(&diff);
    state.commit(diff);

    DrainRun {
        result,
        victim: state.basic(VICTIM).expect("in-memory db read"),
        relayer: state.basic(RELAYER).expect("in-memory db read"),
        auth_nonce_used: state
            .storage(ERC20_PRECOMPILE_ADDRESS, authorization_nonce_slot(VICTIM, AUTH_NONCE))
            .expect("in-memory db read"),
        auth_nonce_in_diff,
    }
}

/// A full drain of a fresh nonce-0 EOA must be rejected.
///
/// Allowing it would leave the account EIP-161 empty, which revm cannot commit (the v1.4.1
/// chain halt). The revert must leave every piece of state untouched.
#[test]
fn full_drain_of_nonce0_account_is_rejected() {
    let run = drain(0, transfer_calldata(full_balance()));

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("the full drain must revert: {:?}", run.result)
    };
    // The revert must be the guard's, not an unrelated one (e.g. an `InvalidAuthorizer`
    // from an EIP-712 domain mismatch): decode the precompile's `Error(string)` reason.
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "a rejected transfer must not credit the relayer"
    );
    assert_eq!(run.auth_nonce_used, U256::ZERO, "a rejected transfer must not use the nonce");
    let victim = run.victim.expect("a rejected transfer must leave the victim in state");
    assert_eq!(
        victim.balance,
        full_balance(),
        "a rejected transfer must leave the victim's balance unchanged"
    );
    assert_eq!(victim.nonce, 0, "a rejected transfer must leave the victim's nonce at 0");
}

/// Control: draining 3 of 5 USDr works today — the victim stays non-empty, so the commit
/// takes the legal `change()` transition. Must keep passing after the fix.
#[test]
fn partial_drain_of_nonce0_account() {
    let run = drain(0, transfer_calldata(partial_amount()));

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
    let run = drain(1, transfer_calldata(full_balance()));

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

/// `receiveWithAuthorization` full drain of a fresh nonce-0 EOA must be rejected.
///
/// RELAYER (the receiver) submits the call; the EIP-712 signer is VICTIM, the debited
/// account, so the guard rejects exactly as it does for the plain transfer (see
/// [`full_drain_of_nonce0_account_is_rejected`]).
#[test]
fn receive_with_authorization_full_drain_of_nonce0_account_is_rejected() {
    let run = drain(0, receive_calldata(full_balance()));

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("the full receive-drain must revert: {:?}", run.result)
    };
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "a rejected receive must not credit the receiver"
    );
    assert_eq!(run.auth_nonce_used, U256::ZERO, "a rejected receive must not use the nonce");
    let victim = run.victim.expect("a rejected receive must leave the victim in state");
    assert_eq!(
        victim.balance,
        full_balance(),
        "a rejected receive must leave the victim's balance unchanged"
    );
    assert_eq!(victim.nonce, 0, "a rejected receive must leave the victim's nonce at 0");
}

/// Control: a partial `receiveWithAuthorization` leaves the victim non-empty, so the guard
/// passes and the commit takes the legal transition.
#[test]
fn receive_with_authorization_partial_drain_of_nonce0_account() {
    let run = drain(0, receive_calldata(partial_amount()));

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the partial receive must succeed: {:?}",
        run.result
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + partial_amount()),
        "the receiver must be credited the partial amount"
    );
    assert_eq!(
        run.victim.map(|info| info.balance),
        Some(full_balance() - partial_amount()),
        "the victim must keep the remainder"
    );
    assert_eq!(
        run.auth_nonce_in_diff,
        Some(U256::ONE),
        "the successful receive must consume the auth nonce in the journal"
    );
}

/// `transferFrom` full drain of a fresh nonce-0 EOA must be rejected.
///
/// The precompile takes the spender from the tx caller, so RELAYER — a plain EOA — stands
/// in for the contract spender of a real contract-mediated flow. The
/// `(VICTIM -> RELAYER)` allowance is seeded in the database (see
/// [`drain_with_allowance`]).
#[test]
fn transfer_from_full_drain_of_nonce0_account_is_rejected() {
    let run =
        drain_with_allowance(RELAYER, VICTIM, RELAYER, transfer_from_calldata(full_balance()));

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("the full transferFrom drain must revert: {:?}", run.result)
    };
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    let victim = run.victim.expect("a rejected transferFrom must leave the victim in state");
    assert_eq!(
        victim.balance,
        full_balance(),
        "a rejected transferFrom must leave the victim's balance unchanged"
    );
    assert_eq!(victim.nonce, 0, "a rejected transferFrom must leave the victim's nonce at 0");
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "a rejected transferFrom must not credit the receiver"
    );
}

/// Control: a partial `transferFrom` leaves the victim non-empty, so the guard passes.
#[test]
fn transfer_from_partial_drain_of_nonce0_account() {
    let run =
        drain_with_allowance(RELAYER, VICTIM, RELAYER, transfer_from_calldata(partial_amount()));

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the partial transferFrom must succeed: {:?}",
        run.result
    );
    assert_eq!(
        run.victim.map(|info| info.balance),
        Some(full_balance() - partial_amount()),
        "the victim must keep the remainder"
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + partial_amount()),
        "the receiver must be credited the partial amount"
    );
}

/// `burnFrom` full drain of a fresh nonce-0 EOA must be rejected.
///
/// The caller is the whitelisted minting module (`burnFrom` requires it), the owner is
/// VICTIM, and the `(VICTIM -> MINTING_MODULE)` allowance is seeded (see
/// [`drain_with_allowance`]).
#[test]
fn burn_from_full_drain_of_nonce0_account_is_rejected() {
    let run = drain_with_allowance(
        MINTING_MODULE_ADDRESS,
        VICTIM,
        MINTING_MODULE_ADDRESS,
        burn_from_calldata(full_balance()),
    );

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("the full burnFrom drain must revert: {:?}", run.result)
    };
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    let victim = run.victim.expect("a rejected burnFrom must leave the victim in state");
    assert_eq!(
        victim.balance,
        full_balance(),
        "a rejected burnFrom must leave the victim's balance unchanged"
    );
    assert_eq!(victim.nonce, 0, "a rejected burnFrom must leave the victim's nonce at 0");
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "an unrelated relayer must stay untouched by a rejected burnFrom"
    );
}

/// Control: a partial `burnFrom` leaves the victim non-empty, so the guard passes. The
/// burn also decreases the seeded total supply.
#[test]
fn burn_from_partial_drain_of_nonce0_account() {
    let run = drain_with_allowance(
        MINTING_MODULE_ADDRESS,
        VICTIM,
        MINTING_MODULE_ADDRESS,
        burn_from_calldata(partial_amount()),
    );

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "the partial burnFrom must succeed: {:?}",
        run.result
    );
    assert_eq!(
        run.victim.map(|info| info.balance),
        Some(full_balance() - partial_amount()),
        "the victim must keep the remainder"
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "an unrelated relayer must stay untouched"
    );
}

/// A database that wraps a `CacheDB` but reports VICTIM with an *un-normalised* zero code
/// hash on read. `CacheDB` normalises a zero code hash to `KECCAK256_EMPTY` on insert
/// (`insert_contract`), so the standard `drain` helper can never reach the zero-hash case;
/// overriding `basic` on read is what lets the reviewer's "DB backend that doesn't normalise
/// the code hash" scenario survive to the empty-account guard. All other reads delegate to
/// the wrapped `CacheDB`.
#[derive(Debug)]
struct UnnormalisedZeroCodeHashDb {
    inner: CacheDB<EmptyDBTyped<Infallible>>,
}

impl Database for UnnormalisedZeroCodeHashDb {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Infallible> {
        let info = self.inner.basic(address)?;
        Ok(if address == VICTIM {
            info.map(|mut info| {
                info.code_hash = B256::ZERO;
                info.code = None;
                info
            })
        } else {
            info
        })
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Infallible> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: StorageKey) -> Result<StorageValue, Infallible> {
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Infallible> {
        self.inner.block_hash(number)
    }
}

/// Regression: an account whose `code_hash` is the un-normalised zero hash (rather than
/// `KECCAK256_EMPTY`) is still EIP-161 empty by revm's `AccountInfo::is_empty()`, which also
/// treats a zero code hash as empty code. A full drain of such a `Loaded` account must be
/// rejected by the guard (which now evaluates revm's own predicate). Against the pre-fix
/// predicate (`code_hash == KECCAK256_EMPTY`) the drain is let through and `State::commit`
/// hits the `unreachable!` halt.
#[test]
fn full_drain_of_unnormalised_zero_code_hash_account_is_rejected() {
    // Process-global and first-wins: register with the test chain's id (the chain the EVM
    // below runs on) so every test in this binary agrees on the domain `precompile_domain`
    // reads, regardless of which test registers first.
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        test_chain_spec_arc().chain_id(),
    ));

    // Seed a standard `CacheDB` (VICTIM normalised to `KECCAK256_EMPTY` on insert), then wrap
    // it so VICTIM is reported with a zero code hash on read — the case `CacheDB` alone can't
    // reach.
    let mut inner = CacheDB::new(EmptyDBTyped::<Infallible>::new());
    inner.insert_account_info(
        VICTIM,
        AccountInfo { balance: full_balance(), nonce: 0, ..Default::default() },
    );
    inner.insert_account_info(
        RELAYER,
        AccountInfo { balance: relayer_balance(), nonce: 0, ..Default::default() },
    );
    let db = UnnormalisedZeroCodeHashDb { inner };

    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);

    let env = EvmEnv::new(
        CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN),
        BlockEnv { number: U256::from(BLOCK_NUMBER), ..Default::default() },
    );
    // Post-fork spec: the EIP-161 empty-account guard is active, so a full drain of a
    // nonce-0 account is rejected.
    let chain_spec = Arc::new(
        RaylsChainSpec::builder(test_chain_spec_arc())
            .precompile_gas_fix(0)
            .eip161_empty_account_guard(0)
            .build(),
    );
    let mut evm =
        RaylsEvmFactory::new(chain_spec).create_evm_with_native_erc20_only(&mut state, env);

    let ResultAndState { result, state: diff } = evm
        .transact_pre_genesis_call(
            RELAYER,
            ERC20_PRECOMPILE_ADDRESS,
            transfer_calldata(full_balance()),
        )
        .expect("transact must not fail; a rejected transfer returns as a revert result");

    // The guard must reject the drain before any balance write, so this commit must not
    // panic. Against the pre-fix predicate the drain is let through and this line panics.
    state.commit(diff);

    let ExecutionResult::Revert { output, .. } = result else {
        panic!("a full drain of a zero-code-hash account must revert: {result:?}");
    };
    // Same reason check as `full_drain_of_nonce0_account_is_rejected`: the revert must be
    // the guard's, so an unrelated error (e.g. a domain mismatch) cannot mask a missing
    // rejection.
    let reason = revert_reason(&output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    assert_eq!(
        state.basic(VICTIM).expect("in-memory db read").map(|info| info.balance),
        Some(full_balance()),
        "a rejected transfer must leave the victim's balance unchanged"
    );
    assert_eq!(
        state.basic(RELAYER).expect("in-memory db read").map(|info| info.balance),
        Some(relayer_balance()),
        "a rejected transfer must not credit the relayer"
    );
}

/// Replay-shape sequence across one block, exactly as the reviewer described:
///
/// - tx1 credits VICTIM with a real native value transfer.
/// - tx2 drains VICTIM's whole balance via EIP-3009.
///
/// `base_balance` selects the account shape:
///
/// - `None`: VICTIM is *brand new* (not in the db); tx1 creates it in memory, so the
///   account lands `InMemoryChange`, not `Loaded`, in the block state cache.
/// - `Some(base)`: VICTIM *pre-exists* in the db at `base`; tx1 only credits it, so the
///   account's first change promotes it out of `Loaded` (to `InMemoryChange`, as a nonce-0
///   code-less account) before tx2 empties it.
///
/// On v1.4.1 both shapes committed fine (an `InMemoryChange` account clearing is a legal
/// transition), so the `guard_active == false` run must succeed and clear the victim.
/// Once the [`RaylsHardFork::Eip161EmptyAccountGuard`] is active at this block, the same
/// sequence must revert the drain and leave VICTIM's state untouched. This is the
/// pre/post-fork boundary the hardfork exists to pin.
fn replay_full_drain(guard_active: bool, base_balance: Option<U256>) -> DrainRun {
    // Process-global and first-wins: register with the test chain's id (the chain the EVM
    // below runs on) so every test in this binary agrees on the domain `precompile_domain`
    // reads, regardless of which test registers first.
    let _ = initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        test_chain_spec_arc().chain_id(),
    ));

    // `None`: VICTIM is deliberately NOT seeded — tx1 must create it in memory.
    // `Some`: VICTIM pre-exists in the db at the base balance; tx1 only credits it.
    let mut db = CacheDB::new(EmptyDBTyped::<Infallible>::new());
    if let Some(base) = base_balance {
        db.insert_account_info(
            VICTIM,
            AccountInfo { balance: base, nonce: 0, ..Default::default() },
        );
    }
    db.insert_account_info(
        RELAYER,
        AccountInfo { balance: relayer_balance(), nonce: 0, ..Default::default() },
    );

    let mut state =
        State::builder().with_database(db).with_bundle_update().without_state_clear().build();
    state.set_state_clear_flag(true);

    // Guard activates at the block being executed (active) or one block later (pre-fork).
    let guard_block = if guard_active { BLOCK_NUMBER } else { BLOCK_NUMBER + 1 };
    let chain_spec = Arc::new(
        RaylsChainSpec::builder(test_chain_spec_arc())
            .precompile_gas_fix(0)
            .eip161_empty_account_guard(guard_block)
            .build(),
    );

    // A real native transfer needs a non-zero block gas limit to pass the per-tx gas check.
    let env = EvmEnv::new(
        CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN),
        BlockEnv {
            number: U256::from(BLOCK_NUMBER),
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
            ..Default::default()
        },
    );
    let mut evm =
        RaylsEvmFactory::new(chain_spec).create_evm_with_native_erc20_only(&mut state, env);

    // tx1: fund the not-yet-existing VICTIM with a native value transfer.
    let fund_tx = TxEnv {
        caller: RELAYER,
        kind: TxKind::Call(VICTIM),
        nonce: 0,
        gas_limit: ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        value: full_balance(),
        data: Bytes::new(),
        gas_price: 0,
        chain_id: None,
        gas_priority_fee: None,
        access_list: Default::default(),
        blob_hashes: Vec::new(),
        max_fee_per_blob_gas: 0,
        tx_type: 0,
        authorization_list: Default::default(),
    };
    let ResultAndState { result: fund_result, state: fund_diff } =
        evm.transact(fund_tx).expect("funding tx must not fail");
    assert!(
        matches!(fund_result, ExecutionResult::Success { .. }),
        "the funding tx must succeed: {fund_result:?}",
    );
    // Commit through the EVM (production idiom, cf. `evm/block.rs`): the EVM still holds a
    // `&mut state` across both txs, so committing on the outer `state` would double-borrow.
    evm.db_mut().commit(fund_diff);

    // tx2: drain VICTIM's whole balance (base + funded, or just funded) via EIP-3009
    // (same shape as `drain`).
    let ResultAndState { result, state: diff } = evm
        .transact_pre_genesis_call(
            RELAYER,
            ERC20_PRECOMPILE_ADDRESS,
            transfer_calldata(full_balance() + base_balance.unwrap_or_default()),
        )
        .expect("transact must not fail; a rejected transfer returns as a revert result");
    let auth_nonce_in_diff = nonce_slot_in_diff(&diff);
    evm.db_mut().commit(diff);

    DrainRun {
        result,
        victim: state.basic(VICTIM).expect("in-memory db read"),
        relayer: state.basic(RELAYER).expect("in-memory db read"),
        auth_nonce_used: state
            .storage(ERC20_PRECOMPILE_ADDRESS, authorization_nonce_slot(VICTIM, AUTH_NONCE))
            .expect("in-memory db read"),
        auth_nonce_in_diff,
    }
}

/// Pre-fork (v1.4.1 shape): funding a fresh account in tx1 and draining it in tx2 committed
/// fine because the account was `InMemoryChange`, not `Loaded`. The drain must succeed and
/// clear the victim.
#[test]
fn replay_shape_pre_fork_full_drain_commits() {
    let run = replay_full_drain(false, None);

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "pre-fork the full drain of an InMemoryChange account must succeed: {:?}",
        run.result
    );
    assert!(
        run.victim.is_none(),
        "pre-fork the cleared victim is removed from state: {:?}",
        run.victim
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance()),
        "the relayer nets zero: funds the victim in tx1 and is credited back in tx2"
    );
    assert_eq!(
        run.auth_nonce_in_diff,
        Some(U256::ONE),
        "the successful pre-fork drain must consume the auth nonce in the journal"
    );
    // The post-commit slot is still zero: the precompile account (`0x0400`) is EIP-161
    // empty, and revm's commit drops the storage writes of an emptied account. v1.4.1
    // behaved identically; the guard does not change this.
    assert_eq!(
        run.auth_nonce_used,
        U256::ZERO,
        "the emptied precompile account's storage is dropped at commit (pre-existing revm behavior)"
    );
}

/// Post-fork: the same sequence is rejected because the drain would leave VICTIM EIP-161
/// empty. The revert must leave VICTIM's balance, the auth nonce, and the relayer's net
/// balance (only the tx1 funding) all untouched by the drain.
#[test]
fn replay_shape_post_fork_full_drain_reverts() {
    let run = replay_full_drain(true, None);

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("post-fork the full drain must revert: {:?}", run.result)
    };
    // The revert must be the guard's (the EIP-161 empty-account reject), not an unrelated
    // one such as an `InvalidAuthorizer` from an EIP-712 domain mismatch.
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    let victim = run.victim.expect("a rejected drain must leave the victim in state");
    assert_eq!(
        victim.balance,
        full_balance(),
        "a rejected drain must leave the victim's funded balance untouched"
    );
    assert_eq!(victim.nonce, 0, "a rejected drain must leave the victim's nonce at 0");
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() - full_balance()),
        "only the tx1 funding moved; the rejected tx2 must not credit the relayer"
    );
    assert_eq!(
        run.auth_nonce_in_diff, None,
        "the rejected drain must roll the in-journal nonce consumption back"
    );
    assert_eq!(run.auth_nonce_used, U256::ZERO, "a rejected drain must not consume the nonce");
}

/// Pre-fork, changed-account shape: VICTIM pre-exists in the db at [`base_amount`] (nonce
/// 0), tx1 credits it, tx2 drains the whole `base + full_balance()`. The account's first
/// change promotes it out of `Loaded`, so the emptied account commits as a legal
/// `InMemoryChange` -> `Destroyed` transition and is cleared.
#[test]
fn replay_shape_pre_fork_changed_account_full_drain_commits() {
    let run = replay_full_drain(false, Some(base_amount()));

    assert!(
        matches!(&run.result, ExecutionResult::Success { .. }),
        "pre-fork the full drain of a pre-existing account must succeed: {:?}",
        run.result
    );
    assert!(
        run.victim.is_none(),
        "pre-fork the cleared victim is removed from state: {:?}",
        run.victim
    );
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() + base_amount()),
        "the relayer nets the victim's pre-existing base: it funds the victim in tx1 and is credited base + full_balance() back in tx2"
    );
    assert_eq!(
        run.auth_nonce_in_diff,
        Some(U256::ONE),
        "the successful pre-fork drain must consume the auth nonce in the journal"
    );
    // The post-commit slot is still zero: the precompile account (`0x0400`) is EIP-161
    // empty, and revm's commit drops the storage writes of an emptied account.
    assert_eq!(
        run.auth_nonce_used,
        U256::ZERO,
        "the emptied precompile account's storage is dropped at commit (pre-existing revm behavior)"
    );
}

/// Post-fork, changed-account shape: the same sequence is rejected because the drain would
/// leave VICTIM EIP-161 empty. The revert must leave the victim at
/// `base + full_balance()`, the relayer at `relayer_balance() - full_balance()` (only the
/// tx1 funding moved), and the auth nonce untouched.
#[test]
fn replay_shape_post_fork_changed_account_full_drain_reverts() {
    let run = replay_full_drain(true, Some(base_amount()));

    let ExecutionResult::Revert { output, .. } = &run.result else {
        panic!("post-fork the full drain of a pre-existing account must revert: {:?}", run.result)
    };
    let reason = revert_reason(output).unwrap_or_else(|| {
        panic!("the guard's revert must carry the precompile's Error(string) reason: {output:?}")
    });
    assert!(
        reason.contains("Transfer would leave account"),
        "the EIP-161 guard, not another error, must reject the drain: {reason}"
    );
    let victim = run.victim.expect("a rejected drain must leave the victim in state");
    assert_eq!(
        victim.balance,
        base_amount() + full_balance(),
        "a rejected drain must leave the victim's pre-existing plus funded balance untouched"
    );
    assert_eq!(victim.nonce, 0, "a rejected drain must leave the victim's nonce at 0");
    assert_eq!(
        run.relayer.map(|info| info.balance),
        Some(relayer_balance() - full_balance()),
        "only the tx1 funding moved; the rejected tx2 must not credit the relayer"
    );
    assert_eq!(
        run.auth_nonce_in_diff, None,
        "the rejected drain must roll the in-journal nonce consumption back"
    );
    assert_eq!(run.auth_nonce_used, U256::ZERO, "a rejected drain must not consume the nonce");
}
