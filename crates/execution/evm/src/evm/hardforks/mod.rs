//! One-shot hardfork state migrations.
//!
//! Each submodule builds a `HashMap<Address, RevmAccount>` that is committed to the EVM
//! database exactly once - the first time the fork's activation block is reached.
//! Continuous behavioral forks (Eip1559, BatchDigestV2) are **not** handled here.

mod admin_transfer;
mod erc20_precompile_bytecode;
mod hybrid_rewards;
mod rls_storage;
mod tokenomics;
mod usdr_supply_correction;
mod uups;

use crate::chainspec::{RaylsHardFork, RaylsHardforks};
use alloy::primitives::Bytes;
use alloy_evm::block::StateChangeSource;
use reth_errors::BlockExecutionError;
use reth_evm::OnStateHook;
use reth_revm::{
    bytecode::Bytecode,
    db::StorageWithOriginalValues,
    state::{Account as RevmAccount, AccountInfo, AccountStatus, EvmState},
    State,
};
use tracing::{info, warn};

/// Build a [`RevmAccount`] holding `bytecode` with `AccountStatus::Touched`.
pub(super) fn account_with_code(bytecode: &[u8]) -> RevmAccount {
    let code = Bytecode::new_raw(Bytes::copy_from_slice(bytecode));
    let code_hash = code.hash_slow();
    RevmAccount {
        info: AccountInfo { code_hash, code: Some(code), ..Default::default() },
        status: AccountStatus::Touched,
        ..Default::default()
    }
}

/// Collect newly activated one-shot migrations and apply them to revm's [`State`].
///
/// This is the path block building uses. It decides what is persisted and hashed into the state
/// root, so it must not change.
///
/// Each migration pre-loads its accounts into the state cache, copies their real `AccountInfo`
/// (keeping new bytecode for code-replacement accounts), notifies the state hook, and applies the
/// change through `CacheAccount::change()` rather than `State::commit()`: commit runs the EIP-161
/// state-clear logic (`apply_account_state`), which panics when a `Loaded` cache account is
/// committed with empty `AccountInfo`, and hardfork migrations are not transaction outputs.
/// `change()` also keeps an account that is still empty after the migration but got storage
/// (testnet, AdminTransfer, RLS proxy), where `commit` would destroy it.
///
/// The block executor cannot call this: alloy-evm 0.30 gives it a generic `StateDB`, not a
/// `State`. `RethEnv::build_block_from_batch_payload` calls it before it creates the executor.
pub(crate) fn apply_activated_migrations<DB: alloy_evm::Database>(
    spec: &impl RaylsHardforks,
    db: &mut State<DB>,
    state_hook: &mut Option<Box<dyn OnStateHook>>,
    receipt_count: usize,
    parent_number: u64,
    block_number: u64,
) -> Result<(), BlockExecutionError>
where
    DB::Error: core::fmt::Display,
{
    run_activated_migrations(
        spec,
        db,
        state_hook,
        receipt_count,
        parent_number,
        block_number,
        |db, address| {
            db.load_cache_account(address)
                .map(|cached| cached.account_info())
                .map_err(|e| e.to_string())
        },
        write_through_cache_account,
    )
}

/// Collect newly activated one-shot migrations and apply them through
/// [`DatabaseCommit::commit`].
///
/// This is the fallback for callers that run the block executor without our block-building
/// code and only have a generic `StateDB`: reth's RPC trace and simulate calls. Their result is
/// returned to the caller and never persisted.
///
/// It is NOT equal to [`apply_activated_migrations`] in one case: `commit` runs the EIP-161
/// state clear, so an account that is still empty after the migration (nonce 0, balance 0, no
/// code) is destroyed together with the storage the migration gave it. Never use this path to
/// build a block.
pub(crate) fn apply_activated_migrations_with_commit<DB: alloy_evm::block::StateDB>(
    spec: &impl RaylsHardforks,
    db: &mut DB,
    state_hook: &mut Option<Box<dyn OnStateHook>>,
    receipt_count: usize,
    parent_number: u64,
    block_number: u64,
) -> Result<(), BlockExecutionError> {
    run_activated_migrations(
        spec,
        db,
        state_hook,
        receipt_count,
        parent_number,
        block_number,
        |db, address| db.basic(address).map_err(|e| e.to_string()),
        |db, state| {
            // `commit` silently skips an account that is not marked as touched.
            debug_assert!(
                state.values().all(|account| account.is_touched()),
                "every migration account must be Touched, or commit drops it"
            );
            // This is the one case where `commit` does not give the state of the exact path.
            // It is fine for a trace. It is a bug if a block built this way is persisted.
            for (address, account) in &state {
                if account.info.is_empty() && account.storage.values().any(|slot| slot.is_changed())
                {
                    warn!(
                        target: "engine",
                        %address,
                        block_number,
                        "hardfork migration applied through commit drops an empty account \
                         with storage; this result must not be persisted"
                    );
                }
            }
            db.commit(state);
        },
    )
}

/// Write a migration to [`State`] without `State::commit()`: register the new bytecode, apply
/// each account through `CacheAccount::change()` and record the transitions.
fn write_through_cache_account<DB: alloy_evm::Database>(db: &mut State<DB>, state: EvmState) {
    // Register new bytecodes in the EVM contracts cache so they can be executed.
    for (_, account) in state.iter() {
        if let Some(code) = &account.info.code {
            db.cache.contracts.insert(account.info.code_hash, code.clone());
        }
    }

    // Apply state changes by calling CacheAccount::change() directly.
    // This bypasses State::commit() / apply_account_state() and its EIP-161 assertions,
    // while still correctly transitioning cache-account statuses and recording transitions
    // in the transition state (so changes are persisted to the DB at block finalization).
    let mut transitions = Vec::with_capacity(state.len());
    for (address, account) in state {
        let storage_changes: StorageWithOriginalValues = account
            .storage
            .into_iter()
            .filter(|(_, slot)| slot.is_changed())
            .map(|(key, slot)| (key, slot.into()))
            .collect();

        let cache_account = db
            .cache
            .accounts
            .get_mut(&address)
            .expect("account should be in cache after pre-loading");

        let transition = cache_account.change(account.info, storage_changes);
        transitions.push((address, transition));
    }

    // Register transitions so block finalization writes all changes to the DB.
    db.apply_transition(transitions.into_iter());
}

/// Shared driver of both entry points: pick the migrations that activate at `block_number`,
/// build their state, fill in the on-chain account info, notify the state hook, and hand the
/// result to `write`.
///
/// `load` reads the current account info and `write` stores the migration. They are the only
/// two steps that differ between revm's `State` and a generic `StateDB`.
#[allow(clippy::too_many_arguments)]
fn run_activated_migrations<DB: alloy_evm::Database>(
    spec: &impl RaylsHardforks,
    db: &mut DB,
    state_hook: &mut Option<Box<dyn OnStateHook>>,
    receipt_count: usize,
    parent_number: u64,
    block_number: u64,
    load: impl Fn(&mut DB, alloy::primitives::Address) -> Result<Option<AccountInfo>, String>,
    write: impl Fn(&mut DB, EvmState),
) -> Result<(), BlockExecutionError> {
    let newly_activated_forks = spec.newly_activated_forks(parent_number, block_number);

    for fork in newly_activated_forks {
        info!(
            target: "engine",
            ?fork,
            block_number,
            "Rayls hardfork activated"
        );

        let mut state = match fork {
            RaylsHardFork::AdminTransfer => {
                info!(
                    target: "engine",
                    old_admin = ?admin_transfer::old_admin(),
                    new_admin = ?admin_transfer::new_admin(),
                    block_number,
                    "Applying AdminTransfer hardfork: transferring admin roles and migrating proxies"
                );
                admin_transfer::admin_transfer_state()
            }
            RaylsHardFork::RlsStorage => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying RlsStorage hardfork: deploying RLS proxy bytecode and initializing storage"
                );
                rls_storage::rls_storage_state()
            }
            RaylsHardFork::Tokenomics => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying Tokenomics hardfork: deploying RLSAccumulator, wiring reward distribution"
                );
                tokenomics::tokenomics_state()
            }

            RaylsHardFork::Uups => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying UUPS hardfork: patching __self immutables and disabling initializers on the 6 UUPS implementation contracts"
                );
                uups::uups_state()
            }
            RaylsHardFork::Erc20PrecompileBytecode => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying Erc20PrecompileBytecode hardfork: installing STOP bytecode on native ERC-20 precompile"
                );
                erc20_precompile_bytecode::erc20_precompile_bytecode_state()
            }
            RaylsHardFork::UsdrSupplyCorrection => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying UsdrSupplyCorrection hardfork: rebasing native ERC-20 precompile TOTAL_SUPPLY slot"
                );
                // Corrective migration: needs db read access to compute
                // `slot + correction` at activation. See module docs.
                usdr_supply_correction::usdr_supply_correction_state(db)?
            }
            RaylsHardFork::HybridRewards => {
                info!(
                    target: "engine",
                    block_number,
                    "Applying HybridRewards hardfork: swapping ConsensusRegistry to hybrid-reward bytecode (re-linking BlsG1 + _rls from the live contract)"
                );
                // Reads the live registry runtime code to carry the per-network BlsG1 address
                // and `_rls` immutable into the new bytecode. See module docs.
                hybrid_rewards::hybrid_rewards_state(db)?
            }
            // Continuous behavioral forks -- not one-shot migrations.
            RaylsHardFork::Eip1559
            | RaylsHardFork::BatchDigestV2
            | RaylsHardFork::PrecompileGasFix
            | RaylsHardFork::TransactionLoadBalancing
            | RaylsHardFork::EmptyOutputBlock
            | RaylsHardFork::DynamicCommitteeSizing
            | RaylsHardFork::OutputSeqNormalization
            | RaylsHardFork::SenderAffinityLoadBalancing
            | RaylsHardFork::Eip161EmptyAccountGuard
            | RaylsHardFork::EpochCloseSeedV2
            | RaylsHardFork::CertificateFetchEpochV2
            | RaylsHardFork::Erc20PrecompileDirectCall
            | RaylsHardFork::CertifiedTipV1 => continue,
        };

        // Pre-load accounts into cache and copy their real AccountInfo.
        //
        // When the hardfork explicitly supplies new bytecode (account.info.code is Some),
        // we must NOT overwrite it with the cached code - otherwise bytecode replacements
        // (e.g. the DelegationPool/RewardDistributor proxy migration) are discarded.
        // We still carry over nonce and balance from the on-chain account.
        for (address, account) in state.iter_mut() {
            let on_chain_info = load(db, *address).map_err(|e| {
                BlockExecutionError::msg(format!(
                    "Hardfork {fork:?}: failed to load account {address} into cache: {e}"
                ))
            })?;
            if let Some(info) = on_chain_info {
                if account.info.code.is_some() {
                    // Hardfork is replacing the bytecode - keep the new code
                    // but carry over nonce/balance from the on-chain account.
                    account.info.nonce = info.nonce;
                    account.info.balance = info.balance;
                } else {
                    account.info = info;
                }
            }
        }

        if let Some(hook) = state_hook {
            hook.on_state(StateChangeSource::Transaction(receipt_count), &state);
        }

        write(db, state);

        info!(target: "engine", ?fork, "Hardfork migration applied successfully");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainspec::{RaylsChainHardforks, ScheduledFork};
    use rayls_infrastructure_types::{address, Address, RaylsNetwork, U256};
    use reth_revm::{db::EmptyDBTyped, State};

    type TestDb = EmptyDBTyped<core::convert::Infallible>;

    fn build_state() -> State<TestDb> {
        State::builder().with_database(EmptyDBTyped::new()).with_bundle_update().build()
    }

    /// The activation block of `fork` on testnet, read from the testnet schedule.
    fn testnet_activation_block(spec: &RaylsChainHardforks, fork: RaylsHardFork) -> u64 {
        spec.rayls_fork_activation(fork).block_number().expect("fork is block-scheduled on testnet")
    }

    /// Addresses from admin_transfer.rs
    const NATIVE_TOKEN_CONTROLLER: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e6");
    const FEE_AGGREGATOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e3");
    const DELEGATION_POOL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e2");
    const DELEGATION_POOL_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e9");
    const REWARD_DISTRIBUTOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e5");
    const REWARD_DISTRIBUTOR_IMPL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e8");

    /// ERC-1967 implementation storage slot.
    const ERC1967_IMPL_SLOT: U256 = U256::from_be_slice(&[
        0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9,
        0x8d, 0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38,
        0x2b, 0xbc,
    ]);

    #[test]
    fn admin_transfer_applies_at_activation_block() {
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("migration should succeed");

        // Verify DelegationPool proxy has ERC1967 impl slot set.
        let dp = db.cache.accounts.get(&DELEGATION_POOL).expect("DP should be in cache");
        let dp_account = dp.account.as_ref().expect("DP should have account info");
        let impl_slot = dp_account.storage.get(&ERC1967_IMPL_SLOT).expect("impl slot");
        let mut buf = [0u8; 32];
        buf[12..].copy_from_slice(DELEGATION_POOL_IMPL.as_slice());
        assert_eq!(*impl_slot, U256::from_be_bytes(buf));

        // Verify RewardDistributor proxy has bytecode replaced (should be short proxy code).
        let rd = db.cache.accounts.get(&REWARD_DISTRIBUTOR).expect("RD should be in cache");
        let rd_info = &rd.account.as_ref().expect("RD account info").info;
        assert!(rd_info.code.is_some(), "RD should have bytecode");

        // Verify impl accounts have bytecode deployed.
        let dp_impl = db.cache.accounts.get(&DELEGATION_POOL_IMPL).expect("DP impl");
        assert!(dp_impl.account.as_ref().unwrap().info.code.is_some());
        let rd_impl = db.cache.accounts.get(&REWARD_DISTRIBUTOR_IMPL).expect("RD impl");
        assert!(rd_impl.account.as_ref().unwrap().info.code.is_some());

        // Verify NativeTokenController has role transfer slots.
        let ntc = db.cache.accounts.get(&NATIVE_TOKEN_CONTROLLER).expect("NTC should be in cache");
        assert!(
            !ntc.account.as_ref().unwrap().storage.is_empty(),
            "NTC should have storage changes from role transfers"
        );

        // Verify FeeAggregator has role transfer slots.
        let fa = db.cache.accounts.get(&FEE_AGGREGATOR).expect("FA should be in cache");
        assert!(
            !fa.account.as_ref().unwrap().storage.is_empty(),
            "FA should have storage changes from role transfers"
        );
    }

    /// The RLS proxy gets only a storage slot from AdminTransfer (no code, no balance). On
    /// testnet the proxy account did not exist at the activation block (560,539), so after the
    /// migration it is an EIP-161-empty account that holds storage.
    ///
    /// The migration must keep that account and its slot. `State::commit` would destroy it
    /// (state clear), which changes the state root of that block. This is why the migration
    /// goes through `CacheAccount::change()` and must be called with revm's `State`.
    #[test]
    fn admin_transfer_keeps_empty_account_that_only_gets_storage() {
        use reth_revm::db::{states::bundle_state::BundleRetention, AccountStatus as DbStatus};

        const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        // empty database: the RLS proxy account does not exist before the migration
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("migration should succeed");

        // cache: the account exists, is empty, and holds the implementation slot
        let cached = db.cache.accounts.get(&RLS_TOKEN).expect("RLS proxy is in the cache");
        let account = cached.account.as_ref().expect("RLS proxy account must be kept");
        assert!(account.info.is_empty(), "the proxy has no code, nonce or balance here");
        assert_ne!(
            account.storage.get(&ERC1967_IMPL_SLOT).copied().unwrap_or_default(),
            U256::ZERO,
            "the implementation slot must be set"
        );

        // bundle: this is what is written to the database and hashed into the state root
        db.merge_transitions(BundleRetention::Reverts);
        let bundled = db.bundle_state.state.get(&RLS_TOKEN).expect("RLS proxy is in the bundle");
        assert!(bundled.info.is_some(), "the empty account must be written, not destroyed");
        assert_ne!(bundled.status, DbStatus::Destroyed);
        assert_ne!(
            bundled.storage.get(&ERC1967_IMPL_SLOT).map(|s| s.present_value).unwrap_or_default(),
            U256::ZERO,
            "the implementation slot must reach the bundle"
        );
    }

    /// The commit fallback (RPC trace and simulate) gives the same accounts as the `State`
    /// path, except for the one case it is documented not to: the empty account that only gets
    /// storage is destroyed by the EIP-161 state clear in `commit`. This test pins both facts,
    /// so the fallback is never taken for an equal replacement of the `State` path.
    #[test]
    fn commit_fallback_matches_state_path_except_for_the_empty_storage_account() {
        use reth_revm::db::states::bundle_state::BundleRetention;

        const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        let mut exact = build_state();
        apply_activated_migrations(&spec, &mut exact, &mut hook, 0, block - 1, block)
            .expect("migration should succeed");
        exact.merge_transitions(BundleRetention::Reverts);

        let mut fallback = build_state();
        apply_activated_migrations_with_commit(
            &spec,
            &mut fallback,
            &mut hook,
            0,
            block - 1,
            block,
        )
        .expect("migration should succeed");
        fallback.merge_transitions(BundleRetention::Reverts);

        let mut differing: Vec<Address> = exact
            .bundle_state
            .state
            .iter()
            .filter(|(address, account)| {
                !fallback
                    .bundle_state
                    .state
                    .get(*address)
                    .is_some_and(|f| f.info == account.info && f.storage == account.storage)
            })
            .map(|(address, _)| *address)
            .collect();
        differing.sort();
        assert_eq!(differing, vec![RLS_TOKEN], "only the RLS proxy may differ");

        let kept = exact.bundle_state.state.get(&RLS_TOKEN).expect("State path keeps the proxy");
        assert!(kept.info.is_some());
        let dropped = fallback.bundle_state.state.get(&RLS_TOKEN);
        assert!(
            dropped.is_none_or(|account| account.info.is_none()),
            "commit destroys the empty account"
        );
    }

    /// A migrated contract must be runnable right away: the EVM reads code through
    /// `Database::basic` and `Database::code_by_hash`, so both must see the new bytecode before
    /// the block is merged.
    #[test]
    fn migrated_code_is_readable_in_the_same_block() {
        use reth_revm::Database as _;

        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("migration should succeed");

        let info = db.basic(DELEGATION_POOL_IMPL).expect("read").expect("impl account exists");
        assert!(!info.is_empty_code_hash(), "impl must have a code hash");
        let code = db.code_by_hash(info.code_hash).expect("read");
        assert!(!code.is_empty(), "new bytecode must be readable by hash in the same block");
    }

    #[test]
    fn admin_transfer_does_not_apply_before_activation() {
        // Use testnet where AdminTransfer activates at a high block,
        // so we can test blocks before activation without underflow.
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 2, block - 1)
            .expect("migration should succeed");

        // No accounts should be in cache - migration didn't run.
        assert!(db.cache.accounts.is_empty(), "AdminTransfer should not apply before activation");
    }

    #[test]
    fn admin_transfer_does_not_reapply_after_activation() {
        // Use testnet where AdminTransfer activates at a high block.
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block, block + 1)
            .expect("should succeed");

        assert!(
            db.cache.accounts.is_empty(),
            "AdminTransfer should not re-apply after activation block"
        );
    }

    #[test]
    fn admin_transfer_preserves_bytecode_for_existing_proxies() {
        // Use testnet where AdminTransfer activates at a non-zero block.
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("migration should succeed");

        // NativeTokenController and FeeAggregator should have proxy bytecode preserved.
        for (name, addr) in [("NTC", NATIVE_TOKEN_CONTROLLER), ("FeeAggregator", FEE_AGGREGATOR)] {
            let ca = db.cache.accounts.get(&addr).expect(&format!("{name} should be in cache"));
            let account = ca.account.as_ref().expect(&format!("{name} should have account info"));
            assert!(account.info.code.is_some(), "{name} should have bytecode");
        }

        // DelegationPool and RewardDistributor should also have proxy bytecode.
        for (name, addr) in
            [("DelegationPool", DELEGATION_POOL), ("RewardDistributor", REWARD_DISTRIBUTOR)]
        {
            let ca = db.cache.accounts.get(&addr).expect(&format!("{name} should be in cache"));
            let account = ca.account.as_ref().expect(&format!("{name} should have account info"));
            assert!(account.info.code.is_some(), "{name} should have bytecode");

            // Verify they share the same bytecode hash (all use ERC1967Proxy)
            let dp_hash = db
                .cache
                .accounts
                .get(&DELEGATION_POOL)
                .unwrap()
                .account
                .as_ref()
                .unwrap()
                .info
                .code_hash;
            assert_eq!(account.info.code_hash, dp_hash, "{name} should have ERC1967Proxy bytecode");
        }
    }

    #[test]
    fn eip1559_activation_produces_no_state_changes() {
        // Use testnet where Eip1559 activates at a non-zero block.
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::Eip1559);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("should succeed");

        assert!(db.cache.accounts.is_empty(), "Eip1559 should not modify any accounts");
    }

    #[test]
    fn multiple_forks_at_same_block_only_applies_state_migrations() {
        // On testnet, BatchDigestV2 and AdminTransfer both activate at the same block.
        let spec = RaylsChainHardforks::for_network(RaylsNetwork::Testnet);
        let block = testnet_activation_block(&spec, RaylsHardFork::AdminTransfer);
        let mut db = build_state();
        let mut hook: Option<Box<dyn reth_evm::OnStateHook>> = None;

        apply_activated_migrations(&spec, &mut db, &mut hook, 0, block - 1, block)
            .expect("should succeed");

        // AdminTransfer should have applied (state migration).
        assert!(
            db.cache.accounts.get(&DELEGATION_POOL).is_some(),
            "AdminTransfer state migration should have applied"
        );
    }

    const ERC20_PRECOMPILE: Address = address!("0000000000000000000000000000000000000400");
    const ACTIVATION_BLOCK: u64 = 1_000;

    /// Build a synthetic test schedule with only Erc20PrecompileBytecode active at `block`.
    fn synthetic_schedule(block: u64) -> RaylsChainHardforks {
        RaylsChainHardforks::new([ScheduledFork::new(
            RaylsHardFork::Erc20PrecompileBytecode,
            reth_chainspec::ForkCondition::Block(block),
        )])
    }

    #[test]
    fn erc20_precompile_bytecode_applies_at_activation_block() {
        let spec = synthetic_schedule(ACTIVATION_BLOCK);
        let mut db = build_state();
        let mut hook: Option<Box<dyn OnStateHook>> = None;

        apply_activated_migrations(
            &spec,
            &mut db,
            &mut hook,
            0,
            ACTIVATION_BLOCK - 1,
            ACTIVATION_BLOCK,
        )
        .expect("migration should succeed");

        let cached = db
            .cache
            .accounts
            .get(&ERC20_PRECOMPILE)
            .expect("precompile should be in cache after migration");
        let info = &cached.account.as_ref().expect("precompile must have account info").info;
        let code = info.code.as_ref().expect("precompile must have code installed");
        assert_eq!(code.original_byte_slice(), &[0x00], "code must be a single STOP byte");
        assert_ne!(
            info.code_hash,
            reth_revm::primitives::keccak256([]),
            "code_hash must not be KECCAK_EMPTY or EIP-161 will destroy the account",
        );
    }

    #[test]
    fn erc20_precompile_bytecode_does_not_apply_before_activation() {
        let spec = synthetic_schedule(ACTIVATION_BLOCK);
        let mut db = build_state();
        let mut hook: Option<Box<dyn OnStateHook>> = None;

        apply_activated_migrations(
            &spec,
            &mut db,
            &mut hook,
            0,
            ACTIVATION_BLOCK - 2,
            ACTIVATION_BLOCK - 1,
        )
        .expect("should succeed");

        assert!(
            db.cache.accounts.is_empty(),
            "Erc20PrecompileBytecode should not apply before activation",
        );
    }

    #[test]
    fn erc20_precompile_bytecode_does_not_reapply_after_activation() {
        let spec = synthetic_schedule(ACTIVATION_BLOCK);
        let mut db = build_state();
        let mut hook: Option<Box<dyn OnStateHook>> = None;

        apply_activated_migrations(
            &spec,
            &mut db,
            &mut hook,
            0,
            ACTIVATION_BLOCK,
            ACTIVATION_BLOCK + 1,
        )
        .expect("should succeed");

        assert!(
            db.cache.accounts.is_empty(),
            "Erc20PrecompileBytecode should not re-apply after activation",
        );
    }
}
