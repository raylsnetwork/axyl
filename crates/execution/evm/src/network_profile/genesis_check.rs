//! Genesis/schedule consistency check.
//!
//! The binary embeds the bytecodes and storage layouts the one-shot hardfork
//! migrations install. The hardfork schedule selected for a boot must agree
//! with the state those migrations expect to find in the chain's genesis: a
//! migration scheduled against a genesis that is missing the contracts it
//! rewrites, or that already carries the post-migration state, would corrupt
//! unrelated state or run client behavior against state the migration never
//! touched.
//!
//! [`verify_schedule_against_genesis`] replays the schedule's one-shot
//! migrations in block order over a [`SimAlloc`] (code + storage per address)
//! built from the genesis alloc, checking each migration's preconditions at
//! the point it would fire, and refuses the boot listing every violation so a
//! misconfigured schedule is fixed in one pass.

use alloy::{
    genesis::Genesis,
    primitives::{Address, Bytes, U256},
};
use reth_revm::state::Account as RevmAccount;
use std::collections::HashMap;
use tracing::info;

use crate::{
    chainspec::RaylsHardFork,
    evm::hardforks::{apply_migration_sim, migration_preconditions},
    native_erc20::ERC20_PRECOMPILE_ADDRESS,
};

use super::{activation::ForkActivation, fork_name::ForkName, profile::NetworkProfile};

/// The one-shot state migrations this check validates, mirroring the dispatch
/// in [`crate::evm::hardforks::apply_activated_migrations`]. Continuous
/// behavioral forks carry no state to validate.
const MIGRATION_FORKS: [RaylsHardFork; 7] = [
    RaylsHardFork::AdminTransfer,
    RaylsHardFork::RlsStorage,
    RaylsHardFork::Tokenomics,
    RaylsHardFork::Uups,
    RaylsHardFork::Erc20PrecompileBytecode,
    RaylsHardFork::UsdrSupplyCorrection,
    RaylsHardFork::HybridRewards,
];

/// A single account in a [`SimAlloc`].
#[derive(Debug, Clone, Default)]
struct SimAccount {
    /// The account's current runtime code; migrations that replace code update it.
    code: Option<Bytes>,
    /// The account's current storage; migrations that write slots update it.
    storage: HashMap<U256, U256>,
    /// The storage the account carried in the genesis alloc, frozen at
    /// construction. Preconditions that must hold for the chain's starting
    /// state (not state accumulated by earlier migrations) read this map.
    genesis_storage: HashMap<U256, U256>,
}

/// A minimal snapshot of the state the one-shot migrations touch: per-address
/// code and storage, built from a genesis alloc.
///
/// This is not an EVM: it tracks only what the migrations read or write
/// (runtime code and storage slots), with the dispatcher's application
/// semantics. A migration account carrying new code replaces the code, an
/// account without new code keeps the current code, and only changed slots
/// write their present value.
#[derive(Debug, Default)]
pub struct SimAlloc {
    accounts: HashMap<Address, SimAccount>,
}

impl SimAlloc {
    /// Build a sim from the alloc of a [`Genesis`].
    pub fn from_genesis(genesis: &Genesis) -> Self {
        let mut accounts = HashMap::with_capacity(genesis.alloc.len());
        for (address, account) in &genesis.alloc {
            let storage = account
                .storage
                .as_ref()
                .map(|map| {
                    map.iter()
                        .map(|(slot, value)| (U256::from_be_bytes(slot.0), U256::from_be_bytes(value.0)))
                        .collect::<HashMap<U256, U256>>()
                })
                .unwrap_or_default();
            accounts.insert(
                *address,
                SimAccount {
                    code: account.code.clone(),
                    genesis_storage: storage.clone(),
                    storage,
                },
            );
        }
        Self { accounts }
    }

    /// The current runtime code at `address`, if any.
    pub fn code(&self, address: Address) -> Option<&Bytes> {
        self.accounts.get(&address).and_then(|account| account.code.as_ref())
    }

    /// The current value of `slot` in `address`'s storage (zero when absent).
    pub fn storage(&self, address: Address, slot: U256) -> U256 {
        self.accounts
            .get(&address)
            .and_then(|account| account.storage.get(&slot))
            .copied()
            .unwrap_or_default()
    }

    /// The storage slots `address` carried in the genesis alloc.
    pub fn genesis_storage(&self, address: Address) -> Option<&HashMap<U256, U256>> {
        self.accounts.get(&address).map(|account| &account.genesis_storage)
    }

    /// Apply a migration's state delta, mirroring
    /// [`crate::evm::hardforks::apply_activated_migrations`].
    pub fn apply<'a>(
        &mut self,
        changes: impl IntoIterator<Item = (&'a Address, &'a RevmAccount)>,
    ) {
        for (address, account) in changes {
            let entry = self.accounts.entry(*address).or_default();
            // A plain touched account carries `Some(empty)` code (revm's
            // `AccountInfo::default`), which the executor treats as "no code":
            // only non-empty code replaces the current bytecode.
            if let Some(code) = &account.info.code {
                if !code.is_empty() {
                    entry.code = Some(code.original_byte_slice().to_vec().into());
                }
            }
            for (slot, evm_slot) in &account.storage {
                if evm_slot.is_changed() {
                    entry.storage.insert(*slot, evm_slot.present_value());
                }
            }
        }
    }
}

/// Verify that the hardfork schedule selected for this boot is consistent with
/// the chain's genesis state, refusing the boot when it is not.
///
/// A one-shot migration scheduled at a post-genesis block requires the state
/// it rewrites to be present and un-migrated at the point it fires; a
/// migration that never runs (`never`, or `block(0)`; both leave the migration
/// body unexecuted) must not leave the chain exposed to EIP-161 reaping or to
/// client behavior gated on the un-migrated state.
pub fn verify_schedule_against_genesis(
    profile: &NetworkProfile,
    genesis: &Genesis,
) -> eyre::Result<()> {
    let mut sim = SimAlloc::from_genesis(genesis);
    let mut violations: Vec<String> = Vec::new();
    let mut pending: Vec<(u64, RaylsHardFork)> = Vec::new();

    for fork in MIGRATION_FORKS {
        let activation = profile
            .hardforks
            .get(&ForkName::from(fork.name()))
            .copied()
            .unwrap_or(ForkActivation::Never);
        match activation {
            // `never` and `block(0)` both leave the migration body unrun:
            // `block(0)` is already active at genesis, and a migration fires
            // only when the chain crosses its activation block.
            ForkActivation::Never | ForkActivation::Block(0) => {
                dormant_violations(fork, activation, &sim, &mut violations);
            }
            ForkActivation::Block(block) => pending.push((block, fork)),
        }
    }

    // Replay in the same order the executor applies a block's newly activated
    // forks: block order, then fork declaration order (VARIANTS order).
    let fork_order = |fork: RaylsHardFork| {
        RaylsHardFork::VARIANTS
            .iter()
            .position(|candidate| *candidate == fork)
            .unwrap_or(usize::MAX)
    };
    pending.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(fork_order(a.1).cmp(&fork_order(b.1))));
    for (block, fork) in pending {
        violations.extend(migration_preconditions(fork, &sim));
        if let Err(error) = apply_migration_sim(fork, &mut sim) {
            violations.push(format!("{fork} (block {block}): {error}"));
        }
    }

    if !violations.is_empty() {
        eyre::bail!(
            "genesis/schedule inconsistencies:\n{}\n\
             The datadir's genesis state is not consistent with the selected hardfork \
             schedule, so the embedded migrations would run against state they were not \
             built for. Refusing to start. Remedy: select the schedule this genesis was \
             built for (`--network <name>` or the matching `--config-file`/`--subnet`), \
             or rebuild the genesis for the selected schedule.",
            violations.join("\n")
        );
    }
    Ok(())
}

/// The rules for a migration fork whose body never runs over this genesis
/// (scheduled `never` or `block(0)`): the genesis must already carry the
/// state the fork's absence implies.
fn dormant_violations(
    fork: RaylsHardFork,
    activation: ForkActivation,
    sim: &SimAlloc,
    violations: &mut Vec<String>,
) {
    match fork {
        // Without the STOP-bytecode install a code-less precompile account is
        // reaped by EIP-161 at the end of the first transaction that touches
        // it, wiping the USDr TOTAL_SUPPLY slot. The genesis must already
        // carry code.
        RaylsHardFork::Erc20PrecompileBytecode if sim.code(ERC20_PRECOMPILE_ADDRESS).is_none() => {
            violations.push(format!(
                "Erc20PrecompileBytecode never runs its migration (scheduled {activation:?}), \
                 but the genesis has no code at {ERC20_PRECOMPILE_ADDRESS}; the native ERC-20 \
                 precompile account is exposed to EIP-161 state clearing"
            ));
        }
        // A migration that is active from genesis (`block(0)`) but never
        // executed cannot be dormant when its activation also gates client
        // behavior: the behavior would run from block 0 against the
        // un-migrated state. `never` is consistent instead — the behavior gate
        // reads the same schedule, so a `never` fork keeps the pre-migration
        // behavior forever, matching the un-migrated state.
        fork
            if matches!(activation, ForkActivation::Block(0)) && behaviorally_gated(fork) =>
        {
            violations.push(format!(
                "{fork} is active from genesis (block 0) but never executes its migration, \
                 while its activation gates client behavior that would run from block 0 \
                 against the un-migrated state"
            ));
        }
        _ => {
            info!(
                target: "rayls::reth",
                ?fork,
                "hardfork is dormant at genesis; the genesis is expected to already carry its state"
            );
        }
    }
}

/// The migration forks whose activation block also gates client behavior that
/// can run before the migration body could have. Today only HybridRewards:
/// its activation switches the epoch-close reward ABI (see `evm/config.rs`),
/// so a `block(0)` schedule would distribute rewards with the hybrid ABI
/// against the pre-hybrid ConsensusRegistry.
fn behaviorally_gated(fork: RaylsHardFork) -> bool {
    matches!(fork, RaylsHardFork::HybridRewards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        evm::hardforks::apply_migration_sim,
        network_profile::NetworkProfile,
        native_erc20::total_supply_slot,
        reth_env::RethEnv,
        system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    };
    use alloy::genesis::GenesisAccount;
    use alloy::primitives::{address, B256, Bytes, U256};
    use rand::{rngs::StdRng, SeedableRng as _};
    use rayls_infrastructure_config::{
        NetworkGenesis, NodeInfo, DELEGATION_POOL_ADDRESS, FEE_AGGREGATOR_ADDRESS,
        NATIVE_TOKEN_CONTROLLER_ADDRESS, REWARD_DISTRIBUTOR_ADDRESS, RLS_ACCUMULATOR_ADDRESS,
    };
    use rayls_infrastructure_types::{
        generate_proof_of_possession_bls, Address, BlsKeypair, NodeP2pInfo, RaylsNetwork,
    };
    use std::collections::BTreeMap;

    const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");
    const NATIVE_TOKEN_CONTROLLER: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e6");
    const FEE_AGGREGATOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e3");
    const DELEGATION_POOL: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e2");
    const REWARD_DISTRIBUTOR: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e5");

    /// A stub contract body: only the presence of code matters to the checks.
    const STUB_CODE: &[u8] = &[0x60, 0x01, 0x00];

    fn code_account(code: &[u8]) -> GenesisAccount {
        GenesisAccount::default().with_code(Some(Bytes::copy_from_slice(code)))
    }

    fn genesis_with(accounts: impl IntoIterator<Item = (Address, GenesisAccount)>) -> Genesis {
        let mut genesis = Genesis::default();
        genesis.alloc = accounts.into_iter().collect();
        genesis
    }

    fn profile(entries: &[(&str, ForkActivation)]) -> NetworkProfile {
        NetworkProfile {
            chain_id: 487,
            hardforks: entries
                .iter()
                .map(|(name, activation)| (ForkName::from(*name), *activation))
                .collect(),
        }
    }

    /// Every migration fork dormant. A genesis with the precompile STOP byte
    /// installed (and nothing else) must pass: `never` is consistent for the
    /// behavior-gated fork, and the dormant migrations expect their state to
    /// already be present.
    fn dormant_profile() -> NetworkProfile {
        profile(&[
            ("AdminTransfer", ForkActivation::Never),
            ("RlsStorage", ForkActivation::Never),
            ("Tokenomics", ForkActivation::Never),
            ("Uups", ForkActivation::Never),
            ("Erc20PrecompileBytecode", ForkActivation::Never),
            ("UsdrSupplyCorrection", ForkActivation::Never),
            ("HybridRewards", ForkActivation::Never),
        ])
    }

    #[test]
    fn dormant_precompile_without_code_is_refused() {
        let err =
            verify_schedule_against_genesis(&dormant_profile(), &Genesis::default())
                .expect_err("a code-less precompile with a dormant install must be refused");
        assert!(err.to_string().contains("EIP-161"), "{err}");
    }

    #[test]
    fn dormant_schedule_with_seeded_precompile_passes() {
        let genesis = genesis_with([(ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00]))]);
        verify_schedule_against_genesis(&dormant_profile(), &genesis)
            .expect("a fully dormant schedule whose genesis carries the state is consistent");
    }

    #[test]
    fn block0_behavior_gated_migration_is_refused() {
        let genesis = genesis_with([(ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00]))]);
        let profile = profile(&[
            ("Erc20PrecompileBytecode", ForkActivation::Never),
            ("HybridRewards", ForkActivation::Block(0)),
        ]);
        let err =
            verify_schedule_against_genesis(&profile, &genesis)
                .expect_err("a block(0) behavior-gated migration must be refused");
        assert!(err.to_string().contains("HybridRewards"), "{err}");
    }

    #[test]
    fn admin_transfer_missing_contracts_is_refused() {
        let profile = profile(&[("AdminTransfer", ForkActivation::Block(10))]);
        let err = verify_schedule_against_genesis(&profile, &Genesis::default())
            .expect_err("AdminTransfer against an empty genesis must be refused");
        for name in ["NativeTokenController", "FeeAggregator", "DelegationPool", "RewardDistributor"] {
            assert!(err.to_string().contains(name), "missing {name} in: {err}");
        }
    }

    #[test]
    fn tokenomics_before_rls_proxy_is_refused() {
        let genesis = genesis_with([
            (NATIVE_TOKEN_CONTROLLER, code_account(STUB_CODE)),
            (FEE_AGGREGATOR, code_account(STUB_CODE)),
            (DELEGATION_POOL, code_account(STUB_CODE)),
            (REWARD_DISTRIBUTOR, code_account(STUB_CODE)),
            (ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00])),
        ]);
        let profile = profile(&[
            ("AdminTransfer", ForkActivation::Block(10)),
            ("Tokenomics", ForkActivation::Block(20)),
        ]);
        let err = verify_schedule_against_genesis(&profile, &genesis)
            .expect_err("Tokenomics before the RLS proxy exists must be refused");
        assert!(err.to_string().contains("Tokenomics"), "{err}");
        assert!(err.to_string().contains("RLS"), "{err}");
    }

    #[test]
    fn in_order_migration_chain_passes() {
        // The RLS proxy is absent from the genesis: only the replay of the
        // earlier migrations (AdminTransfer -> RlsStorage) can place it before
        // Tokenomics fires, so a raw-genesis check would falsely fail.
        let genesis = genesis_with([
            (NATIVE_TOKEN_CONTROLLER, code_account(STUB_CODE)),
            (FEE_AGGREGATOR, code_account(STUB_CODE)),
            (DELEGATION_POOL, code_account(STUB_CODE)),
            (REWARD_DISTRIBUTOR, code_account(STUB_CODE)),
            (ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00])),
        ]);
        let profile = profile(&[
            ("AdminTransfer", ForkActivation::Block(10)),
            ("RlsStorage", ForkActivation::Block(20)),
            ("Tokenomics", ForkActivation::Block(30)),
            ("Uups", ForkActivation::Block(40)),
        ]);
        verify_schedule_against_genesis(&profile, &genesis)
            .expect("an in-order migration chain must be consistent");
    }

    #[test]
    fn rls_storage_with_genesis_storage_is_refused() {
        let slot = B256::left_padding_from(&[0x01]);
        let genesis = genesis_with([(
            RLS_TOKEN,
            GenesisAccount::default().with_storage(Some(BTreeMap::from([(slot, B256::repeat_byte(0x2a))]))),
        )]);
        let profile = profile(&[("RlsStorage", ForkActivation::Block(5))]);
        let err = verify_schedule_against_genesis(&profile, &genesis)
            .expect_err("RlsStorage over a storage-carrying RLS proxy must be refused");
        assert!(err.to_string().contains("genesis storage"), "{err}");
    }

    #[test]
    fn rls_storage_over_deployed_proxy_is_refused() {
        let genesis = genesis_with([(RLS_TOKEN, code_account(STUB_CODE))]);
        let profile = profile(&[("RlsStorage", ForkActivation::Block(5))]);
        let err = verify_schedule_against_genesis(&profile, &genesis)
            .expect_err("RlsStorage over an RLS proxy that already has code must be refused");
        assert!(err.to_string().contains("already has code"), "{err}");
    }

    #[test]
    fn usdr_correction_applies_to_sim() {
        let slot = total_supply_slot();
        let genesis = genesis_with([(
            ERC20_PRECOMPILE_ADDRESS,
            GenesisAccount::default().with_storage(Some(BTreeMap::from([(
                B256::from(slot.to_be_bytes()),
                B256::from(U256::from(7).to_be_bytes()),
            )]))),
        )]);
        let mut sim = SimAlloc::from_genesis(&genesis);
        apply_migration_sim(RaylsHardFork::UsdrSupplyCorrection, &mut sim)
            .expect("UsdrSupplyCorrection sim apply");
        let expected = U256::from_str_radix("515241259606000000000000", 10).unwrap() + U256::from(7);
        assert_eq!(sim.storage(ERC20_PRECOMPILE_ADDRESS, slot), expected);
        let code = sim.code(ERC20_PRECOMPILE_ADDRESS).expect("STOP code re-asserted");
        assert_eq!(code.as_ref(), [0x00u8]);
    }

    #[test]
    fn genesis_storage_is_frozen_across_migrations() {
        let slot = B256::left_padding_from(&[0x01]);
        let genesis = genesis_with([(
            RLS_TOKEN,
            GenesisAccount::default().with_storage(Some(BTreeMap::from([(slot, B256::repeat_byte(0x2a))]))),
        )]);
        let mut sim = SimAlloc::from_genesis(&genesis);
        apply_migration_sim(RaylsHardFork::RlsStorage, &mut sim).expect("RlsStorage sim apply");
        assert!(sim.code(RLS_TOKEN).is_some(), "migration deployed the proxy code");
        assert!(
            sim.genesis_storage(RLS_TOKEN).is_some_and(|storage| !storage.is_empty()),
            "the frozen genesis storage must survive the migration"
        );
    }

    #[test]
    fn hybrid_rewards_missing_registry_is_refused() {
        let genesis = genesis_with([(ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00]))]);
        let profile = profile(&[("HybridRewards", ForkActivation::Block(5))]);
        let err = verify_schedule_against_genesis(&profile, &genesis)
            .expect_err("HybridRewards without a live registry must be refused");
        assert!(err.to_string().contains("no code"), "{err}");
    }

    #[test]
    fn hybrid_rewards_unexpected_registry_is_refused() {
        let genesis = genesis_with([(CONSENSUS_REGISTRY_ADDRESS, code_account(&[0u8; 128]))]);
        let profile = profile(&[("HybridRewards", ForkActivation::Block(5))]);
        let err = verify_schedule_against_genesis(&profile, &genesis)
            .expect_err("HybridRewards over an unexpected registry must be refused");
        assert!(err.to_string().contains("21929"), "{err}");
    }

    #[test]
    fn hybrid_rewards_spliceable_registry_passes() {
        // An all-zero pre-hybrid layout (consistent link/immutable sites) is
        // exactly what the splice validates; the deeper per-site checks are
        // covered by the hybrid_rewards module tests.
        let genesis = genesis_with([
            (CONSENSUS_REGISTRY_ADDRESS, code_account(&vec![0u8; 21_929])),
            (ERC20_PRECOMPILE_ADDRESS, code_account(&[0x00])),
        ]);
        let profile = profile(&[("HybridRewards", ForkActivation::Block(5))]);
        verify_schedule_against_genesis(&profile, &genesis)
            .expect("a spliceable pre-hybrid registry must pass");
    }

    fn deterministic_validators() -> Vec<NodeInfo> {
        (0..4u64)
            .map(|i| {
                let addr = Address::from_slice(&[(i as u8 + 1) * 0x11; 20]);
                let mut rng = StdRng::seed_from_u64(i);
                let bls = BlsKeypair::generate(&mut rng);
                let pop = generate_proof_of_possession_bls(&bls, &addr).expect("pop generation");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls.public(),
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: addr,
                    proof_of_possession: pop,
                }
            })
            .collect()
    }

    /// Lockstep: the `local` schedule must be consistent with a fresh local
    /// genesis assembled exactly the way the ceremony assembles it. This is
    /// the schedule/genesis pair the boot gate meets in dev mode.
    #[test]
    fn local_schedule_is_consistent_with_a_fresh_local_genesis() {
        let genesis = rayls_infrastructure_types::test_genesis();
        let stake = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(5_000_000u64) * U256::from(10u64).pow(U256::from(18)),
            minWithdrawAmount: U256::from(1_000u64) * U256::from(10u64).pow(U256::from(18)),
            epochDuration: 86_400,
        };
        let owner = Address::from_slice(&[0xAA; 20]);
        let with_registry = RethEnv::create_consensus_registry_genesis_accounts(
            deterministic_validators(),
            genesis,
            stake,
            owner,
            owner,
            vec![],
        )
        .expect("pre-genesis sim");
        let precompiles =
            NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompile fetch");
        let sim_proxy_overrides: Vec<(Address, GenesisAccount)> = [
            NATIVE_TOKEN_CONTROLLER_ADDRESS,
            FEE_AGGREGATOR_ADDRESS,
            REWARD_DISTRIBUTOR_ADDRESS,
            DELEGATION_POOL_ADDRESS,
            RLS_ACCUMULATOR_ADDRESS,
        ]
        .iter()
        .filter_map(|addr| with_registry.alloc.get(addr).map(|account| (*addr, account.clone())))
        .collect();
        let mut updated = with_registry.extend_accounts(precompiles);
        crate::reth_env::genesis::apply_greenfield_fixes(&mut updated.alloc);
        for (addr, account) in sim_proxy_overrides {
            updated.alloc.insert(addr, account);
        }

        // The pre-genesis ceremony deploys the RLS proxy itself, so a fresh local
        // genesis is fully post-migration: a schedule re-running RlsStorage must
        // be refused against it.
        let sim = SimAlloc::from_genesis(&updated);
        assert!(sim.code(RLS_TOKEN).is_some(), "the greenfield genesis carries the RLS proxy code");

        verify_schedule_against_genesis(
            &NetworkProfile::from_builtin(RaylsNetwork::Local),
            &updated,
        )
        .expect("the local schedule must be consistent with a fresh local genesis");
    }
}
