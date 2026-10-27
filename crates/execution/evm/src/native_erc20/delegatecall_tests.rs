//! Regression tests for issue #319: the native ERC-20 precompile at `0x0400` must only run as a
//! direct `CALL` once the `Erc20PrecompileDirectCall` fork is active.
//!
//! The precompile is dispatched by `bytecode_address` (see `alloy_evm::PrecompilesMap::run`), so a
//! `DELEGATECALL` or `CALLCODE` into `0x0400` reaches it just like a plain `CALL`. Under a delegate
//! frame `target_address` is the calling contract, not `0x0400`, yet the precompile would still
//! execute its privileged ERC-20 logic (`transfer`, `approve`, `mint`, ...) against its own global
//! storage at `0x0400`. The guard lives in the `0x0400` DynPrecompile
//! (`create_erc20_dyn_precompile`), gated by the block number, so every EVM the factory builds
//! enforces it and blocks that predate the fork replay unchanged.
//!
//! These tests drive every EVM the factory builds (the block-execution path
//! `create_evm_with_native_erc20_only`, and the raw `create_evm`/`create_evm_with_inspector` paths
//! that reth uses for `eth_call`/tracing), with the factory's activation set as `RaylsEvmConfig`
//! does. With the fork active a delegate frame into `0x0400` is rejected and commits no state on
//! all of them; before the fork the same frame still executes (history preserved); a direct `CALL`
//! works either way.

use crate::{
    chainspec::{RaylsChainSpec, RaylsHardFork, RaylsHardforks},
    evm::{RaylsEvmConfig, RaylsEvmFactory},
    native_erc20::{
        abi::erc20, precompile::Erc20Precompile, storage::allowance_slot, Erc20TokenConfig,
        ERC20_PRECOMPILE_ADDRESS,
    },
};
use alloy::{primitives::address, sol_types::SolCall as _};
use rayls_infrastructure_types::{test_chain_spec_arc, Address, U256};
use rayls_middleware_rewards::RewardsCounter;
use reth_evm::{EvmEnv, EvmFactory as _};
use reth_revm::{
    bytecode::Bytecode as RevmBytecode,
    context::{
        result::{ExecutionResult, ResultAndState},
        BlockEnv, CfgEnv,
    },
    db::{CacheDB, EmptyDBTyped},
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    state::{AccountInfo, EvmState},
    State,
};
use std::sync::Arc;

/// Which `RaylsEvmFactory` path builds the EVM — each must enforce the fork identically.
#[derive(Clone, Copy)]
enum Path {
    /// `create_evm_with_native_erc20_only`: the block-execution path (with the inspector).
    BlockExec,
    /// `create_evm`: the raw path reth uses for `eth_call`/`eth_estimateGas` (no inspector).
    EthCall,
    /// `create_evm_with_inspector`: the raw path reth uses for tracing, including `create_tracer`.
    Tracing,
}

/// A factory whose `direct_call_activation` matches `spec` (what `RaylsEvmConfig::new` builds).
fn factory_for(spec: &RaylsChainSpec) -> RaylsEvmFactory {
    RaylsEvmFactory::new(spec.rayls_fork_activation(RaylsHardFork::Erc20PrecompileDirectCall))
}

/// EOA that signs the outer transaction.
const CALLER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Contract that forwards its calldata into `0x0400` via a delegate frame.
const FORWARDER: Address = address!("00000000000000000000000000000000000f0f0f");
/// Arbitrary approval spender.
const SPENDER: Address = address!("0000000000000000000000000000000000000abc");
/// Recipient for the forwarded native transfer.
const RECIPIENT: Address = address!("0000000000000000000000000000000000000dad");

/// Block at which the `Erc20PrecompileDirectCall` fork activates in these tests.
const ACTIVATION_BLOCK: u64 = 5;
/// A block comfortably after activation: the guard applies.
const ACTIVE_BLOCK: u64 = 10;
/// The last block before activation: the guard must not apply (lower boundary of the `>=` gate).
const JUST_BEFORE_ACTIVATION: u64 = ACTIVATION_BLOCK - 1;

/// Approval amount the forwarded `approve` tries to write into `0x0400`'s allowance storage.
fn approve_amount() -> U256 {
    U256::from(0x1234_5678u64)
}

/// Runtime bytecode that copies all calldata, forwards it to `0x0400` with the given call opcode,
/// then returns or reverts with the forwarded frame's returndata.
///
/// `DELEGATECALL` (`0xf4`) and `CALLCODE` (`0xf2`) both keep `address(this)` as the forwarder while
/// running `0x0400`'s code, so the frame's `target_address` is the forwarder, not `0x0400`.
fn forwarder_runtime(delegatecall: bool) -> Vec<u8> {
    let hex = if delegatecall {
        // CALLDATACOPY; DELEGATECALL(gas, 0x0400, 0, cds, 0, 0); RETURNDATACOPY; bubble result.
        "366000600037600060003660006104005af43d600060003e601f573d6000fd5b3d6000f3"
    } else {
        // Same, but CALLCODE (takes an extra value=0 argument).
        "3660006000376000600036600060006104005af23d600060003e6021573d6000fd5b3d6000f3"
    };
    alloy::hex::decode(hex).expect("valid hex")
}

/// A chain spec that activates `Erc20PrecompileDirectCall` (and the precompile gas fix, so the
/// direct-call path uses the DynPrecompile) at the given blocks.
fn spec_with_direct_call_at(block: u64) -> Arc<RaylsChainSpec> {
    Arc::new(
        RaylsChainSpec::builder(test_chain_spec_arc())
            .precompile_gas_fix(0)
            .erc20_precompile_direct_call(block)
            .build(),
    )
}

/// A fresh [`State`] with the given forwarder runtime seeded at [`FORWARDER`] and the ERC-20
/// precompile registered globally so the EVM installs the `0x0400` DynPrecompile.
fn seeded_state(delegatecall: bool) -> State<CacheDB<EmptyDBTyped<core::convert::Infallible>>> {
    // Register the precompile instance (ignored if another test already did).
    let _ = crate::evm::initialize_erc20_precompile(Erc20Precompile::new(
        Erc20TokenConfig::default(),
        ERC20_PRECOMPILE_ADDRESS,
        1,
    ));

    let mut cache_db = CacheDB::new(EmptyDBTyped::<core::convert::Infallible>::new());

    let code = RevmBytecode::new_raw(alloy::primitives::Bytes::copy_from_slice(
        &forwarder_runtime(delegatecall),
    ));
    let code_hash = code.hash_slow();
    cache_db.insert_account_info(
        FORWARDER,
        AccountInfo {
            balance: U256::ZERO,
            nonce: 1,
            code_hash,
            account_id: None,
            code: Some(code),
        },
    );

    // The signing EOA, funded so the native balance it would move actually exists.
    cache_db.insert_account_info(
        CALLER,
        AccountInfo { balance: U256::from(10).pow(U256::from(21)), nonce: 0, ..Default::default() },
    );

    State::builder().with_database(cache_db).with_bundle_update().without_state_clear().build()
}

/// Read `allowance(owner, spender)` out of `0x0400`'s storage in a post-transaction state diff.
fn allowance_in_diff(state: &EvmState, owner: Address, spender: Address) -> U256 {
    let slot = allowance_slot(owner, spender);
    state
        .get(&ERC20_PRECOMPILE_ADDRESS)
        .and_then(|acc| acc.storage.get(&slot))
        .map(|s| s.present_value())
        .unwrap_or(U256::ZERO)
}

/// Read an account's native balance out of a post-transaction state diff.
fn balance_in_diff(state: &EvmState, addr: Address) -> U256 {
    state.get(&addr).map(|acc| acc.info.balance).unwrap_or(U256::ZERO)
}

/// The outer transaction's outcome, reduced to what the assertions need.
#[derive(Debug, PartialEq)]
enum Outcome {
    Success,
    /// Reverted, with the revert data.
    Revert(Vec<u8>),
    /// Exceptional halt of the outer transaction (e.g. out of gas).
    Halt,
}

fn outcome<H>(result: &ExecutionResult<H>) -> Outcome {
    match result {
        ExecutionResult::Success { .. } => Outcome::Success,
        ExecutionResult::Revert { output, .. } => Outcome::Revert(output.to_vec()),
        ExecutionResult::Halt { .. } => Outcome::Halt,
    }
}

/// Assert the forwarded call was stopped by the direct-call guard and not by anything else. The
/// guard halts the inner delegate frame with a `PrecompileError`, which returns no data; the
/// forwarder sees the failed call and REVERTs with that (empty) returndata. A handler error would
/// revert with ERC-20 error data instead, and an outer out-of-gas would be a `Halt`.
fn assert_guard_rejected(outcome: &Outcome, msg: &str) {
    assert_eq!(*outcome, Outcome::Revert(Vec::new()), "{msg}");
}

/// Build an EVM via the chosen `path` (factory activation from `chain_spec`) at `block_number`,
/// have `CALLER` call `target` with `calldata`, and return the outcome plus the state
/// diff. The factory is built with `direct_call_activation` set, so the raw paths exercise that
/// field (not a `Default::Never`).
fn run_at(
    state: &mut State<CacheDB<EmptyDBTyped<core::convert::Infallible>>>,
    chain_spec: Arc<RaylsChainSpec>,
    path: Path,
    block_number: u64,
    target: Address,
    calldata: Vec<u8>,
) -> (Outcome, EvmState) {
    let block_env = BlockEnv { number: U256::from(block_number), ..Default::default() };
    let cfg = CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN);
    let env = EvmEnv::new(cfg, block_env);
    let factory = factory_for(&chain_spec);

    let ResultAndState { result, state: diff } = match path {
        Path::BlockExec => factory
            .create_evm_with_native_erc20_only(state, env, chain_spec)
            .transact_pre_genesis_call(CALLER, target, calldata.into()),
        Path::EthCall => factory.create_evm(state, env).transact_pre_genesis_call(
            CALLER,
            target,
            calldata.into(),
        ),
        Path::Tracing => factory
            .create_evm_with_inspector(state, env, NoOpInspector)
            .transact_pre_genesis_call(CALLER, target, calldata.into()),
    }
    .expect("transact");
    (outcome(&result), diff)
}

/// Forward `approve(SPENDER, amount)` through [`FORWARDER`] (which re-emits it into `0x0400`) on
/// the block-execution path.
fn forward_approve(delegatecall: bool, block_number: u64) -> (Outcome, EvmState) {
    let mut state = seeded_state(delegatecall);
    let calldata = erc20::approveCall { spender: SPENDER, amount: approve_amount() }.abi_encode();
    run_at(
        &mut state,
        spec_with_direct_call_at(ACTIVATION_BLOCK),
        Path::BlockExec,
        block_number,
        FORWARDER,
        calldata,
    )
}

/// With the fork active, a `DELEGATECALL` into `0x0400` must be rejected and write no allowance.
#[test]
fn delegatecall_into_precompile_is_rejected() {
    let (outcome, diff) = forward_approve(true, ACTIVE_BLOCK);

    // DELEGATECALL keeps the EOA as msg.sender, so the forwarded `approve` would write an allowance
    // for CALLER. A non-zero value means the precompile ran inside the delegate frame.
    let allowance = allowance_in_diff(&diff, CALLER, SPENDER);
    assert_eq!(
        allowance,
        U256::ZERO,
        "DELEGATECALL into 0x0400 wrote allowance {allowance}; the precompile executed inside a \
         delegate frame"
    );
    assert_guard_rejected(
        &outcome,
        "DELEGATECALL into 0x0400 was accepted; the fork must reject it",
    );
}

/// With the fork active, a `CALLCODE` into `0x0400` must be rejected too.
///
/// `CALLCODE` sets `msg.sender` to the forwarder, so the forwarded `approve` would write an
/// allowance for `FORWARDER` (not `CALLER`); the assertion reads the forwarder's slot.
#[test]
fn callcode_into_precompile_is_rejected() {
    let (outcome, diff) = forward_approve(false, ACTIVE_BLOCK);

    let allowance = allowance_in_diff(&diff, FORWARDER, SPENDER);
    assert_eq!(
        allowance,
        U256::ZERO,
        "CALLCODE into 0x0400 wrote allowance {allowance} for the forwarder; the precompile \
         executed inside a delegate frame"
    );
    assert_guard_rejected(&outcome, "CALLCODE into 0x0400 was accepted; the fork must reject it");
}

/// With the fork active, a `transfer` forwarded through a `DELEGATECALL` must move no native
/// balance (the native-balance variant of the defect).
#[test]
fn delegatecall_transfer_moves_no_native_balance() {
    let mut state = seeded_state(true);
    let amount = U256::from(1_000u64);
    let calldata = erc20::transferCall { to: RECIPIENT, amount }.abi_encode();
    let (outcome, diff) = run_at(
        &mut state,
        spec_with_direct_call_at(ACTIVATION_BLOCK),
        Path::BlockExec,
        ACTIVE_BLOCK,
        FORWARDER,
        calldata,
    );

    assert_eq!(
        balance_in_diff(&diff, RECIPIENT),
        U256::ZERO,
        "DELEGATECALL into 0x0400 credited the recipient; the precompile moved native balance \
         inside a delegate frame"
    );
    assert_guard_rejected(
        &outcome,
        "DELEGATECALL into 0x0400 transfer was accepted; the fork must reject it",
    );
}

/// Positive control for the test above: before activation the identical delegate `transfer`
/// credits the recipient, so that test cannot pass just because `transfer` fails in this setup.
#[test]
fn delegatecall_transfer_just_before_fork_moves_balance() {
    let mut state = seeded_state(true);
    let amount = U256::from(1_000u64);
    let calldata = erc20::transferCall { to: RECIPIENT, amount }.abi_encode();
    let (outcome, diff) = run_at(
        &mut state,
        spec_with_direct_call_at(ACTIVATION_BLOCK),
        Path::BlockExec,
        JUST_BEFORE_ACTIVATION,
        FORWARDER,
        calldata,
    );

    assert_eq!(outcome, Outcome::Success, "before the fork, a delegate transfer must execute");
    assert_eq!(
        balance_in_diff(&diff, RECIPIENT),
        amount,
        "before the fork, the delegate transfer must credit the recipient as it did historically"
    );
}

/// A `DELEGATECALL` at exactly the activation block is rejected (the gate is `>=`, so pinning the
/// boundary catches a `>` off-by-one).
#[test]
fn delegatecall_rejected_at_activation_block() {
    let (outcome, diff) = forward_approve(true, ACTIVATION_BLOCK);

    assert_eq!(
        allowance_in_diff(&diff, CALLER, SPENDER),
        U256::ZERO,
        "at the activation block a DELEGATECALL into 0x0400 must be rejected"
    );
    assert_guard_rejected(
        &outcome,
        "at the activation block the fork must reject the delegate frame",
    );
}

/// One block before activation, the same `DELEGATECALL` still executes, so historical blocks replay
/// unchanged. Together with the test above this pins both sides of the `>=` boundary.
#[test]
fn delegatecall_just_before_fork_still_executes() {
    let (outcome, diff) = forward_approve(true, JUST_BEFORE_ACTIVATION);

    assert_eq!(
        outcome,
        Outcome::Success,
        "before the fork, a DELEGATECALL into 0x0400 must still execute"
    );
    assert_eq!(
        allowance_in_diff(&diff, CALLER, SPENDER),
        approve_amount(),
        "before the fork, the delegate frame must write the allowance as it did historically"
    );
}

/// Symmetric pre-fork coverage for `CALLCODE`: before activation it still executes (writing the
/// forwarder's allowance, since `CALLCODE` makes the forwarder `msg.sender`).
#[test]
fn callcode_just_before_fork_still_executes() {
    let (outcome, diff) = forward_approve(false, JUST_BEFORE_ACTIVATION);

    assert_eq!(
        outcome,
        Outcome::Success,
        "before the fork, a CALLCODE into 0x0400 must still execute"
    );
    assert_eq!(
        allowance_in_diff(&diff, FORWARDER, SPENDER),
        approve_amount(),
        "before the fork, the CALLCODE frame must write the forwarder's allowance as before"
    );
}

/// Control: a direct `CALL` to `0x0400` works with the fork active. This is the legitimate path and
/// must keep recording the allowance for the caller.
#[test]
fn direct_call_into_precompile_still_works() {
    let mut state = seeded_state(true);
    let calldata = erc20::approveCall { spender: SPENDER, amount: approve_amount() }.abi_encode();
    let (outcome, diff) = run_at(
        &mut state,
        spec_with_direct_call_at(ACTIVATION_BLOCK),
        Path::BlockExec,
        ACTIVE_BLOCK,
        ERC20_PRECOMPILE_ADDRESS,
        calldata,
    );

    assert_eq!(outcome, Outcome::Success, "a direct CALL to 0x0400 approve must succeed");
    assert_eq!(
        allowance_in_diff(&diff, CALLER, SPENDER),
        approve_amount(),
        "a direct CALL to 0x0400 approve must record the allowance for the caller"
    );
}

/// The allowance slot a forwarded `approve` would write: `DELEGATECALL` keeps the EOA as
/// `msg.sender`, `CALLCODE` makes the forwarder `msg.sender`.
fn forwarded_owner(delegatecall: bool) -> Address {
    if delegatecall {
        CALLER
    } else {
        FORWARDER
    }
}

/// The regression test for the RPC/trace concern: the raw factory paths that reth uses for
/// `eth_call`/`eth_estimateGas` (`create_evm`) and tracing (`create_evm_with_inspector`) must
/// reject delegate frames (`DELEGATECALL` and `CALLCODE`) too, not just the block-execution path.
/// These build no `NativeErc20Inspector`, so they exercise the gate living in the DynPrecompile
/// keyed on the factory's activation.
#[test]
fn delegate_frames_rejected_on_raw_eth_call_and_tracing_paths() {
    for path in [Path::EthCall, Path::Tracing] {
        for delegatecall in [true, false] {
            let mut state = seeded_state(delegatecall);
            let calldata =
                erc20::approveCall { spender: SPENDER, amount: approve_amount() }.abi_encode();
            let (outcome, diff) = run_at(
                &mut state,
                spec_with_direct_call_at(ACTIVATION_BLOCK),
                path,
                ACTIVE_BLOCK,
                FORWARDER,
                calldata,
            );

            assert_eq!(
                allowance_in_diff(&diff, forwarded_owner(delegatecall), SPENDER),
                U256::ZERO,
                "a raw factory path let a delegate frame (delegatecall={delegatecall}) into \
                 0x0400 write allowance"
            );
            assert_guard_rejected(
                &outcome,
                "a raw factory path accepted a delegate frame into 0x0400",
            );
        }
    }
}

/// And before activation the raw paths still execute the delegate frames, matching block
/// execution (so RPC/tracing of historical blocks stays consistent).
#[test]
fn delegate_frames_execute_on_raw_paths_before_fork() {
    for path in [Path::EthCall, Path::Tracing] {
        for delegatecall in [true, false] {
            let mut state = seeded_state(delegatecall);
            let calldata =
                erc20::approveCall { spender: SPENDER, amount: approve_amount() }.abi_encode();
            let (outcome, diff) = run_at(
                &mut state,
                spec_with_direct_call_at(ACTIVATION_BLOCK),
                path,
                JUST_BEFORE_ACTIVATION,
                FORWARDER,
                calldata,
            );

            assert_eq!(
                outcome,
                Outcome::Success,
                "before the fork, a raw-path delegate frame (delegatecall={delegatecall}) must \
                 still execute"
            );
            assert_eq!(
                allowance_in_diff(&diff, forwarded_owner(delegatecall), SPENDER),
                approve_amount(),
                "before the fork, a raw-path delegate frame must write the allowance as before"
            );
        }
    }
}

/// Pins the production wiring: the factory `RaylsEvmConfig::new` builds must carry the chain's
/// activation. If `config.rs` went back to a guard-off factory, the other tests here (which build
/// the factory through `factory_for`) would still pass; this one would not.
#[test]
fn evm_config_factory_rejects_delegate_frame() {
    let spec = spec_with_direct_call_at(ACTIVATION_BLOCK);
    let config = RaylsEvmConfig::new(spec.clone(), RewardsCounter::default());
    let calldata = erc20::approveCall { spender: SPENDER, amount: approve_amount() }.abi_encode();
    let env = || {
        EvmEnv::new(
            CfgEnv::new().with_spec_and_mainnet_gas_params(SpecId::CANCUN),
            BlockEnv { number: U256::from(ACTIVE_BLOCK), ..Default::default() },
        )
    };

    // eth_call path: only the factory's own activation can gate this one.
    let mut state = seeded_state(true);
    let ResultAndState { result, state: diff } = config
        .evm_factory
        .create_evm(&mut state, env())
        .transact_pre_genesis_call(CALLER, FORWARDER, calldata.clone().into())
        .expect("transact");
    assert_eq!(allowance_in_diff(&diff, CALLER, SPENDER), U256::ZERO);
    assert_guard_rejected(&outcome(&result), "RaylsEvmConfig's factory let eth_call through");

    // Block-execution path through the same factory.
    let mut state = seeded_state(true);
    let ResultAndState { result, state: diff } = config
        .evm_factory
        .create_evm_with_native_erc20_only(&mut state, env(), spec)
        .transact_pre_genesis_call(CALLER, FORWARDER, calldata.into())
        .expect("transact");
    assert_eq!(allowance_in_diff(&diff, CALLER, SPENDER), U256::ZERO);
    assert_guard_rejected(
        &outcome(&result),
        "RaylsEvmConfig's factory let block execution through",
    );
}
