use super::*;
use alloy::{primitives::utils::parse_ether, sol_types::SolCall};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_config::{NodeInfo, RLS_IMPL_ADDRESS};
use rayls_infrastructure_types::{
    generate_proof_of_possession_bls, payload::RLPayload, Address, BlsKeypair, BlsSignature,
    Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, GenesisAccount, NodeP2pInfo,
    ReputationScores, SignatureVerificationState, TaskManager, B256, U256,
};
use reth_chainspec::ChainSpec as RethChainSpec;

use crate::{
    reth_env::config::ALL_MODULES,
    system_calls::{
        ConsensusRegistry::{self, ValidatorStatus},
        EpochState, RLSToken, CONSENSUS_REGISTRY_ADDRESS, RLS_ADDRESS,
    },
    test_utils::TransactionFactory,
};
use reth::rpc::builder::RpcModuleSelection;
use reth_chain_state::ExecutedBlock;
use reth_evm::{ConfigureEvm, EvmFactory};
use reth_revm::{cached::CachedReads, database::StateProviderDatabase, State};
use std::sync::Arc;
use tempfile::TempDir;
use tracing::debug;

/// Helper function for creating a consensus output for tests.
fn consensus_output_for_tests(round: u32, epoch: u32, subdag_index: u64) -> ConsensusOutput {
    let mut leader = Certificate::default();
    // set signature for deterministic test results
    leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    leader.header_mut_for_test().created_at = rayls_infrastructure_types::now();
    leader.header.round = round;
    leader.header.epoch = epoch;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;

    ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![leader.clone(), Certificate::default()],
            leader,
            subdag_index,
            reputation_scores,
            previous_sub_dag,
        )
        .into(),
        close_epoch: true,
        batches: Default::default(),       // empty
        batch_digests: Default::default(), // empty
        parent_hash: ConsensusHeader::default().digest(),
        number: subdag_index,
        extra: Default::default(),
    }
}

/// Build a block from RLPayload and transactions.
async fn execute_payload_and_update_canonical_chain(
    reth_env: &RethEnv,
    payload: RLPayload,
    transactions: Vec<Vec<u8>>,
) -> eyre::Result<ExecutedBlock> {
    let (block, _validation_counts) =
        reth_env.build_block_from_batch_payload(payload, &transactions, &[])?;
    // update chain state via finish_executing_output (same as production path)
    reth_env.finish_executing_output(vec![block.clone()])?;
    reth_env.finalize_block(block.recovered_block.sealed_header().clone())?;
    // flush to disk immediately so tests can read DB state
    reth_env.flush_persistence().await?;
    Ok(block)
}

/// `batch_txns_all_pending` underpins restart dedup: an empty block's batch is only
/// re-enabled for retry when its txns are STILL pending (nonce-too-high); txns already
/// mined (nonce-too-low) must read as not-retryable so the batch stays deduped and a
/// restart can't re-execute it and fork.
#[tokio::test]
async fn batch_txns_all_pending_distinguishes_pending_from_mined() -> eyre::Result<()> {
    let chain = rayls_infrastructure_types::test_chain_spec_arc();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None).await?;

    // the default `TransactionFactory` address is funded in `test_genesis` at nonce 0.
    let mut factory = TransactionFactory::new();
    let gas_price = reth_env.get_gas_price()?;
    let tx_at = |nonce: u64, factory: &mut TransactionFactory| {
        factory.set_nonce(nonce);
        factory.create_eip1559_encoded(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            U256::ZERO,
            Default::default(),
        )
    };

    // empty input is never retryable
    assert!(!reth_env.batch_txns_all_pending(&[]));

    // account nonce is 0: a tx at the current nonce and a future-nonce tx are both pending
    let tx_nonce0 = tx_at(0, &mut factory);
    let tx_nonce5 = tx_at(5, &mut factory);
    assert!(reth_env.batch_txns_all_pending(&[tx_nonce0.clone()]), "current-nonce tx is pending");
    assert!(reth_env.batch_txns_all_pending(&[tx_nonce5]), "future-nonce tx is pending");

    // mine the nonce-0 tx so the funded account advances to nonce 1
    let consensus_output = consensus_output_for_tests(2, 0, 1);
    let payload = RLPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
    let block =
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![tx_nonce0.clone()])
            .await?;
    assert_eq!(block.recovered_block.body().transactions.len(), 1, "nonce-0 tx must be included");

    // the same nonce-0 tx is now already mined (nonce-too-low) -> NOT retryable
    assert!(
        !reth_env.batch_txns_all_pending(&[tx_nonce0.clone()]),
        "already-mined tx must read as not retryable so the batch stays deduped"
    );

    // mixed batch: one mined (nonce 0) + one pending (nonce 1) -> NOT retryable (all-or-nothing,
    // matching the live drop condition)
    let tx_nonce1 = tx_at(1, &mut factory);
    assert!(
        !reth_env.batch_txns_all_pending(&[tx_nonce0, tx_nonce1]),
        "a mixed batch is not all-pending"
    );

    Ok(())
}

/// Verify that `create_consensus_registry_genesis_accounts` produces a genesis
/// with correct RLS ERC-20 balances:
///   - totalSupply == stakePerValidator × numValidators + Σ rls_prefunds
///   - Treasury (owner) balance == 0
///   - ConsensusRegistry balance == stakePerValidator × numValidators
///   - Each prefund recipient balance == its specified amount
///   - Individual validators hold zero RLS (their stake is held by the registry)
#[tokio::test]
async fn test_genesis_rls_balances_and_stake() -> eyre::Result<()> {
    let num_validators = 4u64;
    let stake_per_validator = U256::from(parse_ether("5_000_000").unwrap());
    // Create validators
    let validators: Vec<_> = (0..num_validators)
        .map(|i| {
            let addr = Address::from_slice(&[(i as u8 + 1) * 0x11; 20]);
            let mut rng = StdRng::seed_from_u64(i);
            let bls = BlsKeypair::generate(&mut rng);
            let pop = generate_proof_of_possession_bls(&bls, &addr).expect("pop generation failed");
            NodeInfo {
                name: format!("validator-{i}"),
                bls_public_key: *bls.public(),
                p2p_info: NodeP2pInfo::default(),
                execution_address: addr,
                proof_of_possession: pop,
            }
        })
        .collect();

    let owner = Address::from_slice(&[0xAA; 20]);
    let initial_stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: stake_per_validator,
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: 86400,
    };

    // Two prefund recipients with distinct amounts.
    let prefund_a = Address::from_slice(&[0xB1; 20]);
    let prefund_b = Address::from_slice(&[0xB2; 20]);
    let prefund_a_amount = U256::from(parse_ether("1_000_000").unwrap());
    let prefund_b_amount = U256::from(parse_ether("250_000").unwrap());
    let rls_prefunds = vec![(prefund_a, prefund_a_amount), (prefund_b, prefund_b_amount)];

    let total_stake = stake_per_validator * U256::from(num_validators);
    let total_prefund = prefund_a_amount + prefund_b_amount;
    let expected_total_supply = total_stake + total_prefund;

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators.clone(),
        rayls_infrastructure_types::test_genesis(),
        initial_stake_config.clone(),
        owner,
        owner,
        rls_prefunds.clone(),
    )?;

    assert!(genesis.alloc.contains_key(&RLS_ADDRESS), "RLS proxy should be in genesis");
    assert!(genesis.alloc.contains_key(&RLS_IMPL_ADDRESS), "RLS impl should be in genesis");
    assert!(
        genesis.alloc.contains_key(&CONSENSUS_REGISTRY_ADDRESS),
        "ConsensusRegistry should be in genesis"
    );

    let rls_account = genesis.alloc.get(&RLS_ADDRESS).expect("RLS proxy in alloc");
    let rls_storage = rls_account.storage.as_ref().expect("RLS proxy should have storage");

    // OpenZeppelin ERC20Upgradeable (ERC-7201 namespaced storage):
    //   base = 0x52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00
    //   _balances mapping is at base + 0 (first field of ERC20Storage)
    //   Solidity mapping slot: _balances[account] = keccak256(abi.encode(account, base))
    let oz_erc20_balance_slot = |account: Address| -> B256 {
        use alloy::primitives::keccak256;
        let base: B256 =
            "0x52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00".parse().unwrap();
        let mut input = [0u8; 64];
        input[12..32].copy_from_slice(account.as_slice());
        input[32..64].copy_from_slice(base.as_slice());
        keccak256(input)
    };

    let read_balance = |account: Address| -> U256 {
        let slot = oz_erc20_balance_slot(account);
        rls_storage.get(&slot).copied().map(|v| U256::from_be_bytes(v.0)).unwrap_or(U256::ZERO)
    };

    // totalSupply is at base + 2
    let total_supply_slot: B256 =
        "0x52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace02".parse().unwrap();
    let stored_supply = rls_storage.get(&total_supply_slot).map(|v| U256::from_be_bytes(v.0));
    assert_eq!(
        stored_supply,
        Some(expected_total_supply),
        "totalSupply should equal stake×N + Σ prefunds"
    );

    assert_eq!(read_balance(owner), U256::ZERO, "treasury should be fully distributed at genesis");
    assert_eq!(
        read_balance(CONSENSUS_REGISTRY_ADDRESS),
        total_stake,
        "ConsensusRegistry should hold the full validator stake"
    );
    assert_eq!(read_balance(prefund_a), prefund_a_amount, "prefund A balance mismatch");
    assert_eq!(read_balance(prefund_b), prefund_b_amount, "prefund B balance mismatch");

    let registry_account = genesis
        .alloc
        .get(&CONSENSUS_REGISTRY_ADDRESS)
        .expect("ConsensusRegistry should be in genesis alloc");
    assert!(registry_account.code.is_some(), "ConsensusRegistry should have bytecode");
    assert!(
        registry_account.storage.as_ref().is_some_and(|s| !s.is_empty()),
        "ConsensusRegistry should have storage (validator state, epoch info, stake config)"
    );

    for v in &validators {
        assert_eq!(
            read_balance(v.execution_address),
            U256::ZERO,
            "Validator {} should have zero RLS balance (stake held by registry)",
            v.execution_address
        );
    }

    Ok(())
}

/// Verify that every account and storage slot produced by the pre-genesis sim is
/// (a) written by `init_genesis_with_settings` into the Storage V2 canonical
/// tables (`HashedAccounts`, `HashedStorages`, `Bytecodes`) and the trie,
/// (b) NOT written into the legacy `PlainAccountState`/`PlainStorageState`,
/// and (c) readable back through `LatestStateProvider`, so the node can use
/// the state without any post-init backfill.
#[tokio::test]
async fn test_genesis_alloc_hashed_and_readable_in_storage_v2() -> eyre::Result<()> {
    use crate::{reth_env::RethEnv, traits::RaylsNode};
    use alloy::primitives::keccak256;
    use reth_db::{tables, transaction::DbTx};
    use reth_db_common::init::init_genesis_with_settings;
    use reth_primitives_traits::Bytecode as RethBytecode;
    use reth_provider::{
        providers::{RocksDBBuilder, StaticFileProvider},
        AccountReader, BlockNumReader, DatabaseProviderFactory, LatestStateProvider,
        ProviderFactory, StateProvider, StorageSettings, StorageSettingsCache,
    };

    // Build a genesis with the full consensus-registry + precompile sim.
    let num_validators = 3u64;
    let stake = U256::from(parse_ether("1_000_000").unwrap());
    let validators: Vec<_> = (0..num_validators)
        .map(|i| {
            let addr = Address::from_slice(&[(i as u8 + 1) * 0x22; 20]);
            let mut rng = StdRng::seed_from_u64(100 + i);
            let bls = BlsKeypair::generate(&mut rng);
            let pop = generate_proof_of_possession_bls(&bls, &addr).expect("pop");
            NodeInfo {
                name: format!("v{i}"),
                bls_public_key: *bls.public(),
                p2p_info: NodeP2pInfo::default(),
                execution_address: addr,
                proof_of_possession: pop,
            }
        })
        .collect();
    let owner = Address::from_slice(&[0xCC; 20]);
    let stake_cfg = ConsensusRegistry::StakeConfig {
        stakeAmount: stake,
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: 86_400,
    };
    let prefund_x = Address::from_slice(&[0xD1; 20]);
    let prefund_x_amount = U256::from(parse_ether("5_000").unwrap());

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators,
        rayls_infrastructure_types::test_genesis(),
        stake_cfg,
        owner,
        owner,
        vec![(prefund_x, prefund_x_amount)],
    )?;

    // Apply the greenfield overrides (pre-patched impl bytecodes + allowance
    // slot) the real pipeline does before handing the alloc to reth.
    let mut final_alloc: std::collections::BTreeMap<Address, GenesisAccount> =
        genesis.alloc.iter().map(|(a, g)| (*a, g.clone())).collect();
    crate::reth_env::genesis::apply_greenfield_fixes(&mut final_alloc);
    let final_genesis =
        genesis.clone().extend_accounts(final_alloc.iter().map(|(a, g)| (*a, g.clone())));

    assert!(!final_genesis.alloc.is_empty(), "sim must produce at least one alloc entry");

    // construct a ProviderFactory with Storage V2 explicitly, then run the
    // canonical genesis init against the storage stack reth owns.
    let tmp_dir = TempDir::new()?;
    let datadir = tmp_dir.path().to_path_buf();
    let db = Arc::new(reth_db::init_db(
        datadir.join("db"),
        reth_db::mdbx::DatabaseArguments::default(),
    )?);
    let rayls_chain_spec = Arc::new(
        crate::chainspec::RaylsChainSpec::builder(Arc::new(RethChainSpec::from(
            final_genesis.clone(),
        )))
        .build(),
    );
    let rocksdb_provider =
        RocksDBBuilder::new(datadir.join("rocksdb")).with_default_tables().build()?;
    let static_files = StaticFileProvider::read_write(datadir.join("static_files"))?;
    let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
    let provider_factory: ProviderFactory<RaylsNode> = ProviderFactory::new(
        db,
        rayls_chain_spec.clone(),
        static_files,
        rocksdb_provider,
        runtime,
    )?;
    let settings = StorageSettings::v2();
    provider_factory.set_storage_settings_cache(settings);

    let genesis_hash = init_genesis_with_settings(&provider_factory, settings)?;
    assert_eq!(
        genesis_hash,
        rayls_chain_spec.genesis_hash(),
        "init_genesis_with_settings must return the chain-spec genesis hash"
    );

    // ── assertions ──────────────────────────────────────────────────────
    let mut expected_bytecodes: std::collections::BTreeSet<B256> = Default::default();

    // Raw-table assertions use a short-lived ro provider.
    {
        let provider_ro = provider_factory.database_provider_ro()?;
        assert_eq!(provider_ro.best_block_number()?, 0, "genesis block is 0");
        let tx = provider_ro.tx_ref();

        // (a) PlainAccountState / PlainStorageState must be empty (v2 routes
        // canonical reads through HashedAccounts/HashedStorages).
        let plain_accounts_len = tx.entries::<tables::PlainAccountState>()?;
        let plain_storages_len = tx.entries::<tables::PlainStorageState>()?;
        assert_eq!(
            plain_accounts_len, 0,
            "v2: PlainAccountState must be empty, got {plain_accounts_len}"
        );
        assert_eq!(
            plain_storages_len, 0,
            "v2: PlainStorageState must be empty, got {plain_storages_len}"
        );

        // (b) HashedAccounts must contain keccak(addr) for every alloc entry,
        // (c) HashedStorages must hold every non-zero slot under keccak(slot).
        for (addr, alloc_account) in &final_genesis.alloc {
            let hashed_address = keccak256(*addr);

            let hashed_entry = tx
                .get::<tables::HashedAccounts>(hashed_address)?
                .unwrap_or_else(|| panic!("HashedAccounts missing entry for {addr}"));
            assert_eq!(hashed_entry.balance, alloc_account.balance, "balance for {addr}");
            assert_eq!(
                hashed_entry.nonce,
                alloc_account.nonce.unwrap_or_default(),
                "nonce for {addr}",
            );
            let expected_code_hash =
                alloc_account.code.as_ref().map(|c| RethBytecode::new_raw(c.clone()).hash_slow());
            assert_eq!(hashed_entry.bytecode_hash, expected_code_hash, "code_hash for {addr}");
            if let Some(code_hash) = expected_code_hash {
                expected_bytecodes.insert(code_hash);
            }
        }

        // (d) Bytecodes table must hold every distinct alloc bytecode.
        for code_hash in &expected_bytecodes {
            assert!(
                tx.get::<tables::Bytecodes>(*code_hash)?.is_some(),
                "Bytecodes missing entry for code_hash {code_hash}"
            );
        }

        // (e) AccountsTrie must be populated: compute_state_root runs against
        // the hashed tables during init_genesis_with_settings.
        let accounts_trie_len = tx.entries::<tables::AccountsTrie>()?;
        assert!(accounts_trie_len > 0, "AccountsTrie must be populated by compute_state_root");
    }

    // (f) LatestStateProvider must read every alloc account and non-zero
    // slot back correctly (it takes the provider by value, hence a fresh ro).
    let provider_for_state = provider_factory.database_provider_ro()?;
    let state_provider = LatestStateProvider::new(provider_for_state);
    for (addr, alloc_account) in &final_genesis.alloc {
        let via_provider = state_provider
            .basic_account(addr)?
            .unwrap_or_else(|| panic!("LatestStateProvider missing {addr}"));
        assert_eq!(via_provider.balance, alloc_account.balance);
        assert_eq!(via_provider.nonce, alloc_account.nonce.unwrap_or_default());
        let expected_code_hash =
            alloc_account.code.as_ref().map(|c| RethBytecode::new_raw(c.clone()).hash_slow());
        assert_eq!(via_provider.bytecode_hash, expected_code_hash);

        if let Some(storage) = &alloc_account.storage {
            for (slot, value) in storage {
                let u256_val = U256::from_be_bytes(value.0);
                if u256_val.is_zero() {
                    continue;
                }
                let stored_via_provider = state_provider
                    .storage(*addr, *slot)?
                    .unwrap_or_else(|| panic!("provider missing storage {addr} {slot}"));
                assert_eq!(
                    stored_via_provider, u256_val,
                    "provider storage mismatch at {addr} {slot}"
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_close_epochs() -> eyre::Result<()> {
    let validator_1 = Address::from_slice(&[0x11; 20]);
    let validator_3 = Address::from_slice(&[0x33; 20]);
    let validator_4 = Address::from_slice(&[0x44; 20]);
    let validator_5 = Address::from_slice(&[0x55; 20]);

    // create validator wallet for staking later
    let mut new_validator_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

    // create validator wallet for exiting later
    let mut validator_2_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(2));
    let validator_2_address = validator_2_eoa.address();

    // create initial validators for testing
    let all_validators = [
        validator_1,
        validator_2_address,
        validator_3,
        validator_4,
        validator_5,
        new_validator_eoa.address(),
    ];

    // create validator info objects for each address
    let mut validators: Vec<_> = all_validators
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            // use deterministic seed
            let mut rng = StdRng::seed_from_u64(i as u64);
            let bls = BlsKeypair::generate(&mut rng);
            let bls_pubkey = bls.public();
            let pop = generate_proof_of_possession_bls(&bls, addr).expect("pop generation failed");
            NodeInfo {
                name: format!("validator-{i}"),
                bls_public_key: *bls_pubkey,
                p2p_info: NodeP2pInfo::default(),
                execution_address: *addr,
                proof_of_possession: pop,
            }
        })
        .collect();

    debug!(target: "engine", "created validators for consensus registry {:#?}", validators);

    let epoch_duration = 60 * 60 * 24; // 24hrs
    let initial_stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: epoch_duration,
    };

    // create genesis with funded governance safe
    let mut governance_multisig =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let governance = governance_multisig.address();
    let tmp_genesis = rayls_infrastructure_types::test_genesis().extend_accounts([
        (
            governance,
            GenesisAccount::default().with_balance(U256::from((50_000_000 * 10) ^ 18)), // 50mil
        ),
        (
            new_validator_eoa.address(),
            GenesisAccount::default()
                .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
        ),
        (
            validator_2_address,
            GenesisAccount::default()
                .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
        ),
    ]);

    // remove last validator so only 5 form the initial committees
    let new_validator = validators.pop().expect("six validators");

    // Pre-fund governance with one validator's worth of RLS so it can hand it to
    // the new validator below (treasury retains nothing after genesis under the
    // fully-allocated supply model).
    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators.clone(),
        tmp_genesis,
        initial_stake_config.clone(),
        governance,
        governance,
        vec![(governance, initial_stake_config.stakeAmount)],
    )?;

    // update genesis again to include stake for new validator
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // governance allowlists the new validator (onlyOwner)
    let calldata =
        ConsensusRegistry::allowlistValidatorCall { validatorAddress: new_validator_eoa.address() }
            .abi_encode()
            .into();
    let allowlist_tx = governance_multisig.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    // governance transfers RLS to new validator so it can stake
    let calldata = RLSToken::transferCall {
        to: new_validator_eoa.address(),
        amount: initial_stake_config.stakeAmount,
    }
    .abi_encode()
    .into();
    let rls_transfer_tx = governance_multisig.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(RLS_ADDRESS),
        U256::ZERO,
        calldata,
    );

    // new validator approves ConsensusRegistry to pull RLS
    let calldata = RLSToken::approveCall {
        spender: CONSENSUS_REGISTRY_ADDRESS,
        amount: initial_stake_config.stakeAmount,
    }
    .abi_encode()
    .into();
    let rls_approve_tx = new_validator_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(RLS_ADDRESS),
        U256::ZERO,
        calldata,
    );

    let proof = ConsensusRegistry::ProofOfPossession {
        uncompressedPubkey: new_validator.bls_public_key.serialize().into(),
        uncompressedSignature: new_validator.proof_of_possession.serialize().into(),
    };
    let calldata = ConsensusRegistry::stakeCall {
        blsPubkey: new_validator.bls_public_key.to_bytes().into(),
        proofOfPossession: proof,
    }
    .abi_encode()
    .into();
    // stake no longer sends ETH — the ConsensusRegistry pulls ERC-20 RLS via transferFrom
    let stake_tx = new_validator_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );
    let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
    let activate_tx = new_validator_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    // create new env with initialized consensus registry for tests
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None).await?;
    let mut expected_epoch = 0;
    let expected_committee = validators.iter().map(|v| v.execution_address).collect();
    let mut expected_epoch_info = ConsensusRegistry::EpochInfo {
        committee: expected_committee,
        blockHeight: 0,
        epochDuration: epoch_duration,
        stakeVersion: 0,
    };

    // assert epoch state is correct
    let EpochState { epoch, epoch_info, validators: committee, epoch_start } =
        reth_env.epoch_state_from_canonical_tip()?;
    debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "original epoch state from canonical tip in genesis");
    assert_eq!(epoch, expected_epoch);
    assert_eq!(epoch_start, chain.genesis_timestamp());
    assert_eq!(epoch_info, expected_epoch_info);

    // assert committee matches validator args for constructor
    for v in &validators {
        let on_chain = committee
            .iter()
            .find(|info| info.validatorAddress == v.execution_address)
            .expect("validator on-chain");
        assert_eq!(on_chain.blsPubkey.as_ref(), v.bls_public_key.to_bytes());
        assert_eq!(on_chain.activationEpoch, epoch);
        assert_eq!(on_chain.exitEpoch, 0);
        assert!(!on_chain.isRetired);
        assert!(!on_chain.isDelegated);
        assert_eq!(on_chain.stakeVersion, 0);
    }

    // close epoch with deterministic signature as source of randomness
    // and execute the first block with txs for new validator to stake
    let mut consensus_output = consensus_output_for_tests(2, expected_epoch, 1);
    consensus_output.close_epoch = false;
    let payload = RLPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
    let block1 = execute_payload_and_update_canonical_chain(
        &reth_env,
        payload,
        vec![allowlist_tx, rls_transfer_tx, rls_approve_tx, stake_tx, activate_tx],
    )
    .await?;
    let canonical_header = block1.recovered_block.clone_sealed_header();

    // now close the first epoch
    expected_epoch += 1;
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 2);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block2.recovered_block.clone_sealed_header();

    // now close the second epoch so the new validator is active
    expected_epoch += 1;
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 3);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block3.recovered_block.clone_sealed_header();

    // read new epoch state
    let EpochState { epoch, epoch_info, validators: committee, epoch_start } =
        reth_env.epoch_state_from_canonical_tip()?;
    debug!(target: "evm", ?epoch, ?epoch_info, ?committee, ?epoch, "new epoch state from canonical tip");
    // assert epoch info updated
    expected_epoch_info.blockHeight = 4;
    assert_eq!(expected_epoch, epoch);
    assert_eq!(epoch_start, canonical_header.timestamp);
    assert_eq!(epoch_info, expected_epoch_info);

    // create evm to read custom contract call
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db = State::builder()
        .with_database(cached_reads.as_db_mut(state))
        .with_bundle_update()
        .without_state_clear()
        .build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);

    // read new committee (always 2 epochs ahead)
    let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }.abi_encode().into();
    let new_epoch_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut rayls_evm, calldata)?;

    // ensure validators in increasing order by address
    let expected_new_committee = vec![
        validator_1,
        validator_3,
        validator_4,
        validator_2_address,
        new_validator.execution_address,
    ];

    let expected = ConsensusRegistry::EpochInfo {
        committee: expected_new_committee,
        blockHeight: 0,
        // epoch duration set at the start
        epochDuration: Default::default(),
        stakeVersion: 0,
    };

    debug!(target: "engine", "new epoch info:{:#?}", new_epoch_info);
    assert_eq!(new_epoch_info, expected);

    // assert new committee matches validator args for constructor
    // this should be the case for the first 3 epochs
    for v in &validators {
        let on_chain = committee
            .iter()
            .find(|info| info.validatorAddress == v.execution_address)
            .expect("validator on-chain");
        assert_eq!(on_chain.blsPubkey.as_ref(), v.bls_public_key.to_bytes());
        assert_eq!(on_chain.activationEpoch, 0);
        assert_eq!(on_chain.exitEpoch, 0);
        assert!(!on_chain.isRetired);
        assert!(!on_chain.isDelegated);
        assert_eq!(on_chain.stakeVersion, 0);
    }

    // submit validator 2 exit request
    let calldata = ConsensusRegistry::beginExitCall {}.abi_encode().into();
    let begin_exit_tx = validator_2_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );
    expected_epoch += 1;
    let mut consensus_output = consensus_output_for_tests(2, expected_epoch, 4);
    consensus_output.close_epoch = false;
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block4 =
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![begin_exit_tx]).await?;
    let canonical_header = block4.recovered_block.clone_sealed_header();

    // close epoch
    expected_epoch += 1;
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 5);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block5 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block5.recovered_block.clone_sealed_header();

    // create evm to read latest state
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);

    // assert validator 2 is pending exit
    let calldata = ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
        .abi_encode()
        .into();
    let validator_2_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(&mut rayls_evm, calldata)?;
    debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
    assert_eq!(validator_2_info.currentStatus, ValidatorStatus::PendingExit);

    // read all active validators from consensus registry
    let calldata = ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
        .abi_encode()
        .into();
    let eligible_validators = reth_env
        .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
            &mut rayls_evm,
            calldata,
        )?;

    assert_eq!(eligible_validators.len(), 6);

    // check for pending exit status
    let (pending_exit, active_validators): (Vec<_>, Vec<_>) = eligible_validators
        .into_iter()
        .partition(|v| v.currentStatus == ValidatorStatus::PendingExit.into());

    assert_eq!(pending_exit.len(), 1);
    assert_eq!(active_validators.len(), 5);
    assert_eq!(
        pending_exit.first().expect("one pending validator").validatorAddress,
        validator_2_address
    );

    // close epoch again to exit validator
    expected_epoch += 1;
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 6);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block6 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block6.recovered_block.clone_sealed_header();
    // close epoch again
    expected_epoch += 1;
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 7);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block7 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block7.recovered_block.clone_sealed_header();

    // create evm to read latest state
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);

    // assert validator 2 is pending exit
    let calldata = ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
        .abi_encode()
        .into();
    let validator_2_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(&mut rayls_evm, calldata)?;
    debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
    assert_eq!(validator_2_info.currentStatus, ValidatorStatus::Exited);

    // read all active validators from consensus registry
    let calldata = ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
        .abi_encode()
        .into();
    let eligible_validators = reth_env
        .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
            &mut rayls_evm,
            calldata,
        )?;

    assert_eq!(eligible_validators.len(), 5);

    // ensure validator 2 has fully exited
    let (pending_exit, active_validators): (Vec<_>, Vec<_>) = eligible_validators
        .into_iter()
        .partition(|v| v.currentStatus == ValidatorStatus::PendingExit.into());

    assert_eq!(pending_exit.len(), 0);
    assert_eq!(active_validators.len(), 5);
    for v in active_validators {
        assert!(v.validatorAddress != validator_2_address);
    }

    Ok(())
}

#[test]
fn test_rpc_validator() {
    let mut mods: Option<RpcModuleSelection> = None;
    RethConfig::validate_rpc_modules(&mut mods);
    assert!(mods.is_none());
    let mut mods = Some(RpcModuleSelection::All);
    RethConfig::validate_rpc_modules(&mut mods);
    if let Some(RpcModuleSelection::Selection(mods)) = &mut mods {
        for r in ALL_MODULES {
            assert!(mods.remove(&r));
        }
    };
}

/// Full-DB check for `fix_genesis_history`: build a v2 storage stack, run
/// canonical genesis init (which writes genesis storage history under the
/// buggy plain slot), simulate the post-genesis history a replay would append
/// under the hashed slot, then re-key and assert every case is correct.
#[cfg(feature = "archive-replay")]
#[tokio::test]
async fn test_fix_genesis_history_rekeys_and_preserves_post_genesis() -> eyre::Result<()> {
    use crate::{reth_env::RethEnv, traits::RaylsNode};
    use alloy::primitives::keccak256;
    use reth_db::{models::storage_sharded_key::StorageShardedKey, tables, BlockNumberList};
    use reth_db_common::init::init_genesis_with_settings;
    use reth_provider::{
        providers::{RocksDBBuilder, StaticFileProvider},
        ChainSpecProvider, ProviderFactory, RocksDBProviderFactory, StorageSettings,
        StorageSettingsCache,
    };
    use std::collections::BTreeMap;

    // genesis with one account holding three storage slots.
    let addr = Address::from([0x11u8; 20]);
    let slot_a = B256::from(U256::from(1u64)); // immutable: no post-genesis history
    let slot_b = B256::from(U256::from(2u64)); // single-shard mutable
    let slot_c = B256::from(U256::from(3u64)); // multi-shard mutable
    let storage = BTreeMap::from([
        (slot_a, B256::from(U256::from(0xAAu64))),
        (slot_b, B256::from(U256::from(0xBBu64))),
        (slot_c, B256::from(U256::from(0xCCu64))),
    ]);
    let genesis = rayls_infrastructure_types::test_genesis().extend_accounts([(
        addr,
        GenesisAccount {
            balance: U256::from(1_000u64),
            storage: Some(storage),
            ..Default::default()
        },
    )]);

    // v2 storage stack + canonical genesis init (writes plain-key history).
    let tmp_dir = TempDir::new()?;
    let datadir = tmp_dir.path().to_path_buf();
    let db = Arc::new(reth_db::init_db(
        datadir.join("db"),
        reth_db::mdbx::DatabaseArguments::default(),
    )?);
    let rayls_chain_spec = Arc::new(
        crate::chainspec::RaylsChainSpec::builder(Arc::new(RethChainSpec::from(genesis))).build(),
    );
    let rocksdb_provider =
        RocksDBBuilder::new(datadir.join("rocksdb")).with_default_tables().build()?;
    let static_files = StaticFileProvider::read_write(datadir.join("static_files"))?;
    let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
    let provider_factory: ProviderFactory<RaylsNode> =
        ProviderFactory::new(db, rayls_chain_spec, static_files, rocksdb_provider, runtime)?;
    let settings = StorageSettings::v2();
    provider_factory.set_storage_settings_cache(settings);
    init_genesis_with_settings(&provider_factory, settings)?;
    let _ = provider_factory.chain_spec();

    let (ha, hb, hc) = (keccak256(slot_a), keccak256(slot_b), keccak256(slot_c));

    // Precondition (the bug): genesis history is keyed by the PLAIN slot, so the
    // hashed keys the v2 read consults carry no genesis entry yet.
    {
        let rocksdb = provider_factory.rocksdb_provider();
        for h in [ha, hb, hc] {
            let shards = rocksdb.storage_history_shards(addr, h)?;
            assert!(
                shards.iter().all(|(_, l)| !l.contains(0u64)),
                "hashed slot must lack the genesis entry before the fix"
            );
        }
        assert!(
            rocksdb.storage_history_shards(addr, slot_a)?.iter().any(|(_, l)| l.contains(0u64)),
            "genesis writer put the entry under the plain slot (the bug)"
        );
    }

    // Simulate post-genesis history under the HASHED keys, as replay's indexer
    // would append:
    //   slot_b — one open shard [5] (has room → prepend in place)
    //   slot_c — a FULL sealed shard [1..=2000] + a full open shard [2001..=4000],
    //            so prepending block 0 (total 4001) must re-chunk into three
    //            <=2000 shards (the cascade / overflow case).
    {
        let rocksdb = provider_factory.rocksdb_provider();
        let mut batch = rocksdb.batch();
        batch.put::<tables::StoragesHistory>(
            StorageShardedKey::last(addr, hb),
            &BlockNumberList::new([5u64]).unwrap(),
        )?;
        batch.put::<tables::StoragesHistory>(
            StorageShardedKey::new(addr, hc, 2000),
            &BlockNumberList::new(1u64..=2000).unwrap(),
        )?;
        batch.put::<tables::StoragesHistory>(
            StorageShardedKey::last(addr, hc),
            &BlockNumberList::new(2001u64..=4000).unwrap(),
        )?;
        batch.commit()?;
    }

    // Run the fix.
    let fixed = RethEnv::fix_genesis_history_with(&provider_factory, true)?;
    assert!(fixed > 0, "fix must re-key at least the injected slots");

    let rocksdb = provider_factory.rocksdb_provider();

    // Strong outcome check (not a loose count): every genesis alloc storage slot
    // now resolves under its hashed key at block 0. Catches any slot the fix
    // silently missed. Iterates the alloc from the chain spec (genesis was moved).
    for (a, account) in provider_factory.chain_spec().genesis().alloc.iter() {
        let Some(s) = account.storage.as_ref() else { continue };
        for slot in s.keys() {
            let shards = rocksdb.storage_history_shards(*a, keccak256(*slot))?;
            assert!(
                shards.iter().any(|(_, l)| l.contains(0u64)),
                "genesis slot {slot} on {a} must carry block 0 under its hashed key after the fix"
            );
        }
    }
    let blocks = |shards: &[(StorageShardedKey, BlockNumberList)]| -> Vec<u64> {
        shards.iter().flat_map(|(_, l)| l.iter()).collect()
    };

    // slot_a (immutable) -> a fresh open shard [0].
    assert_eq!(
        blocks(&rocksdb.storage_history_shards(addr, ha)?),
        vec![0],
        "immutable genesis slot seeded with block 0"
    );

    // slot_b (single-shard mutable) -> [0, 5]: genesis added, post-genesis kept.
    assert_eq!(
        blocks(&rocksdb.storage_history_shards(addr, hb)?),
        vec![0, 5],
        "block 5 must survive (no clobber)"
    );

    // slot_c (full first shard) -> re-chunked so block 0 lands in the earliest
    // shard, every original block is preserved, and no shard exceeds the limit.
    let c = rocksdb.storage_history_shards(addr, hc)?; // ascending by highest block
    assert_eq!(c.len(), 3, "4001 blocks re-chunk into three shards, got {}", c.len());
    assert!(
        c.iter().all(|(_, l)| (l.len() as usize) <= 2000),
        "no shard may exceed NUM_OF_INDICES_IN_SHARD (2000)"
    );
    assert!(c[0].1.contains(0u64), "genesis block lands in the earliest shard");
    assert_eq!(
        c.last().unwrap().0.sharded_key.highest_block_number,
        u64::MAX,
        "the last shard is the open (u64::MAX) shard"
    );
    assert_eq!(
        c.iter().flat_map(|(_, l)| l.iter()).collect::<Vec<u64>>(),
        (0u64..=4000).collect::<Vec<u64>>(),
        "all blocks preserved, sorted, no duplicates or loss"
    );

    // Idempotency: a second run is a no-op.
    let again = RethEnv::fix_genesis_history_with(&provider_factory, true)?;
    assert_eq!(again, 0, "second run must re-key nothing");
    assert_eq!(blocks(&rocksdb.storage_history_shards(addr, ha)?), vec![0]);
    assert_eq!(blocks(&rocksdb.storage_history_shards(addr, hb)?), vec![0, 5]);
    assert_eq!(
        blocks(&rocksdb.storage_history_shards(addr, hc)?),
        (0u64..=4000).collect::<Vec<u64>>(),
        "re-chunked slot is untouched on the second run"
    );

    Ok(())
}

/// Full-DB check for `fix_genesis_account_history`. Unlike storage, account
/// history has no key mismatch (read and write both use the plain address), so
/// genesis init already seeds it correctly and the fix is a no-op. It only has
/// work to do after an `IndexAccountHistoryStage` first-sync clear, which this
/// test simulates — and multi-shard accounts (post-genesis changesets) are left
/// untouched.
#[cfg(feature = "archive-replay")]
#[tokio::test]
async fn test_fix_genesis_account_history_seeds_after_clear_and_skips_multishard(
) -> eyre::Result<()> {
    use crate::{reth_env::RethEnv, traits::RaylsNode};
    use reth_db::{models::ShardedKey, tables, BlockNumberList};
    use reth_db_common::init::init_genesis_with_settings;
    use reth_provider::{
        providers::{RocksDBBuilder, StaticFileProvider},
        ProviderFactory, RocksDBProviderFactory, StorageSettings, StorageSettingsCache,
    };

    let addr = Address::from([0x21u8; 20]); // immutable genesis account
    let addr2 = Address::from([0x22u8; 20]); // multi-shard (post-genesis changes)
    let genesis = rayls_infrastructure_types::test_genesis().extend_accounts([
        (addr, GenesisAccount { balance: U256::from(1u64), ..Default::default() }),
        (addr2, GenesisAccount { balance: U256::from(2u64), ..Default::default() }),
    ]);

    let tmp_dir = TempDir::new()?;
    let datadir = tmp_dir.path().to_path_buf();
    let db = Arc::new(reth_db::init_db(
        datadir.join("db"),
        reth_db::mdbx::DatabaseArguments::default(),
    )?);
    let rayls_chain_spec = Arc::new(
        crate::chainspec::RaylsChainSpec::builder(Arc::new(RethChainSpec::from(genesis))).build(),
    );
    let rocksdb_provider =
        RocksDBBuilder::new(datadir.join("rocksdb")).with_default_tables().build()?;
    let static_files = StaticFileProvider::read_write(datadir.join("static_files"))?;
    let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
    let provider_factory: ProviderFactory<RaylsNode> =
        ProviderFactory::new(db, rayls_chain_spec, static_files, rocksdb_provider, runtime)?;
    let settings = StorageSettings::v2();
    provider_factory.set_storage_settings_cache(settings);
    init_genesis_with_settings(&provider_factory, settings)?;

    let rocksdb = provider_factory.rocksdb_provider();
    let last = |a: Address| rocksdb.get::<tables::AccountsHistory>(ShardedKey::last(a));

    // Genesis init already seeds account history correctly (plain address, no
    // mismatch), so the fix has nothing to do.
    assert!(
        last(addr)?.is_some_and(|l| l.contains(0u64)),
        "genesis init seeds AccountsHistory[addr] = [0]"
    );
    assert_eq!(
        RethEnv::fix_genesis_account_history_with(&provider_factory, true)?,
        0,
        "account history already correct at genesis; fix is a no-op"
    );

    // Simulate IndexAccountHistoryStage's first-sync clear (what a normally-synced
    // node does), then inject a multi-shard post-genesis account.
    rocksdb.clear::<tables::AccountsHistory>()?;
    {
        let mut batch = rocksdb.batch();
        batch.put::<tables::AccountsHistory>(
            ShardedKey::new(addr2, 400u64),
            &BlockNumberList::new([100u64, 400]).unwrap(),
        )?;
        batch.put::<tables::AccountsHistory>(
            ShardedKey::last(addr2),
            &BlockNumberList::new([900u64]).unwrap(),
        )?;
        batch.commit()?;
    }

    // The fix restores block 0 for the cleared immutable account...
    let seeded = RethEnv::fix_genesis_account_history_with(&provider_factory, true)?;
    assert!(seeded >= 1, "cleared single-shard genesis accounts are re-seeded");
    assert!(last(addr)?.is_some_and(|l| l.contains(0u64)), "addr re-seeded with block 0");

    // ...but leaves the multi-shard account untouched (assumed correctly indexed).
    assert_eq!(
        last(addr2)?.unwrap().iter().collect::<Vec<u64>>(),
        vec![900],
        "multi-shard account's open shard is untouched"
    );
    assert!(
        !last(addr2)?.unwrap().contains(0u64),
        "multi-shard account is not seeded with block 0"
    );

    // Idempotent.
    assert_eq!(RethEnv::fix_genesis_account_history_with(&provider_factory, true)?, 0);

    Ok(())
}
