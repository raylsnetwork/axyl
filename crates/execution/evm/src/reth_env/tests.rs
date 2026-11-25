use super::*;
use alloy::{primitives::utils::parse_ether, sol_types::SolCall};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_config::{NodeInfo, RLS_IMPL_ADDRESS};
use rayls_infrastructure_types::{
    generate_proof_of_possession_bls, payload::RLPayload, Address, BlsKeypair, BlsSignature, Bytes,
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

/// A freshly created execution database uses the 16 KiB default page size, `--db.page-size` wins
/// over it, and a database created with the previous 4 KiB default keeps its page size, geometry
/// header and rows when reopened with the new default.
#[test]
fn test_execution_db_default_page_size() -> eyre::Result<()> {
    use crate::reth_env::RethCommand;
    use clap::Parser as _;
    use rayls_infrastructure_utils::mdbx::DEFAULT_MDBX_PAGE_SIZE;
    use reth::{args::DatabaseArgs, builder::NodeConfig};
    use reth_db::{
        tables,
        transaction::{DbTx as _, DbTxMut as _},
        Database as _,
    };

    let page_size = |db: &RethDb| db.stat().expect("stat").page_size() as usize;
    let geometry_lower = |db: &RethDb| db.info().expect("info").geometry().min();
    let default_config = RethConfig(NodeConfig::default());

    // fresh database
    {
        let tmp_dir = TempDir::new()?;
        let db = RethEnv::new_database(&default_config, tmp_dir.path())?;
        assert_eq!(page_size(&db), DEFAULT_MDBX_PAGE_SIZE);
    }

    // `--db.page-size`, parsed the way the node CLI does, overrides the default
    {
        let cmd = RethCommand::try_parse_from(["rayls", "--db.page-size", "8KB"])?;
        assert_eq!(cmd.db.page_size, Some(8 * 1024));
        let cli_config = RethConfig(NodeConfig { db: cmd.db, ..NodeConfig::default() });
        let tmp_dir = TempDir::new()?;
        let db = RethEnv::new_database(&cli_config, tmp_dir.path())?;
        assert_eq!(page_size(&db), 8 * 1024);
    }

    // legacy 4 KiB database: page size and rows survive a reopen with the default config.
    // Unlike the blocks above, `legacy_dir` is held for the rest of the function on purpose: the
    // datafile has to outlive the first `new_database` call so the reopen below finds it.
    const LEGACY_PAGE_SIZE: usize = 4096;
    let legacy_dir = TempDir::new()?;
    let legacy_config = RethConfig(NodeConfig {
        db: DatabaseArgs { page_size: Some(LEGACY_PAGE_SIZE), ..Default::default() },
        ..NodeConfig::default()
    });
    let legacy_geometry_lower = {
        let db = RethEnv::new_database(&legacy_config, legacy_dir.path())?;
        assert_eq!(page_size(&db), LEGACY_PAGE_SIZE);
        let tx = db.tx_mut()?;
        tx.put::<tables::CanonicalHeaders>(7, B256::repeat_byte(0xab))?;
        tx.commit()?;
        geometry_lower(&db)
    };
    let db = RethEnv::new_database(&default_config, legacy_dir.path())?;
    assert_eq!(page_size(&db), LEGACY_PAGE_SIZE);
    // the datafile's geometry header is left alone
    assert_eq!(geometry_lower(&db), legacy_geometry_lower);
    let tx = db.tx()?;
    assert_eq!(tx.get::<tables::CanonicalHeaders>(7)?, Some(B256::repeat_byte(0xab)));
    Ok(())
}

/// A zeroed meta page 0 still opens: `new_database` detects the page size and reopens.
/// Covers the 16 KiB default and ARM's 64 KiB page size.
#[test]
fn test_execution_db_recovers_zeroed_meta_page_0() -> eyre::Result<()> {
    use rayls_infrastructure_utils::mdbx::DEFAULT_MDBX_PAGE_SIZE;
    use reth::builder::NodeConfig;
    use reth_db::{
        tables,
        transaction::{DbTx as _, DbTxMut as _},
        Database as _,
    };
    use std::io::{Seek as _, SeekFrom, Write as _};

    let default_config = RethConfig(NodeConfig::default());

    for page_size in [DEFAULT_MDBX_PAGE_SIZE, 64 * 1024] {
        let dir = TempDir::new()?;

        // A fresh datafile at this page size with one row.
        {
            let mut node_config = NodeConfig::default();
            node_config.db.page_size = Some(page_size);
            let db = RethEnv::new_database(&RethConfig(node_config), dir.path())?;
            assert_eq!(db.stat().expect("stat").page_size() as usize, page_size);
            let tx = db.tx_mut()?;
            tx.put::<tables::CanonicalHeaders>(7, B256::repeat_byte(0xab))?;
            tx.commit()?;
        }

        // Zero meta page 0, leaving the two backup meta pages intact.
        let mut f = std::fs::OpenOptions::new().write(true).open(dir.path().join("mdbx.dat"))?;
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&vec![0u8; page_size])?;
        f.sync_all()?;
        drop(f);

        // Reopen with no page-size hint: detect the size, recover, and keep the row.
        let db = RethEnv::new_database(&default_config, dir.path())?;
        assert_eq!(db.stat().expect("stat").page_size() as usize, page_size);
        let tx = db.tx()?;
        assert_eq!(tx.get::<tables::CanonicalHeaders>(7)?, Some(B256::repeat_byte(0xab)));
    }
    Ok(())
}

/// `new_database` rejects an unsupported `--db.page-size` before it opens the datafile.
#[test]
fn test_execution_db_rejects_unsupported_page_size() {
    use reth::builder::NodeConfig;
    for page_size in [2048, 12 * 1024, 128 * 1024] {
        let dir = TempDir::new().expect("temp dir");
        let mut node_config = NodeConfig::default();
        node_config.db.page_size = Some(page_size);
        let err = RethEnv::new_database(&RethConfig(node_config), dir.path())
            .expect_err("unsupported page size must be rejected");
        assert!(format!("{err:#}").contains("invalid --db.page-size"), "{page_size}: {err:#}");
        assert!(!dir.path().join("mdbx.dat").exists(), "{page_size}: no datafile is created");
    }
}

/// An existing datafile ignores `--db.page-size`, even an unsupported one.
#[test]
fn test_execution_db_existing_datafile_ignores_unsupported_page_size() -> eyre::Result<()> {
    use rayls_infrastructure_utils::mdbx::DEFAULT_MDBX_PAGE_SIZE;
    use reth::builder::NodeConfig;
    let dir = TempDir::new()?;
    drop(RethEnv::new_database(&RethConfig(NodeConfig::default()), dir.path())?);
    let mut node_config = NodeConfig::default();
    node_config.db.page_size = Some(2048);
    let db = RethEnv::new_database(&RethConfig(node_config), dir.path())?;
    assert_eq!(db.stat().expect("stat").page_size() as usize, DEFAULT_MDBX_PAGE_SIZE);
    Ok(())
}

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
    transactions: Vec<Bytes>,
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

/// A `--config-file` profile handed to the constructor reaches the chain spec:
/// its forks activate at the configured blocks and everything else stays `Never`.
#[tokio::test]
async fn profile_schedule_reaches_chain_spec() -> eyre::Result<()> {
    use crate::{ForkCondition, NetworkProfile, RaylsHardFork, RaylsHardforks};

    // The test genesis carries the local network's chain-id, which the profile must match.
    let chain_id = rayls_infrastructure_types::RaylsNetwork::Local.chain_id();
    let profile: NetworkProfile = serde_yaml::from_str(&format!(
        "chain_id: {chain_id}\nhardforks:\n  TransactionLoadBalancing: 777777\n"
    ))?;

    let chain = rayls_infrastructure_types::test_chain_spec_arc();
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain_with_profile(
        chain,
        tmp_dir.path(),
        &task_manager,
        None,
        &profile,
    )
    .await?;

    let spec = reth_env.rayls_chain_spec();
    assert_eq!(
        spec.rayls_fork_activation(RaylsHardFork::TransactionLoadBalancing),
        ForkCondition::Block(777777)
    );
    // A fork absent from the file's schedule stays `Never`.
    assert_eq!(spec.rayls_fork_activation(RaylsHardFork::Eip1559), ForkCondition::Never);
    Ok(())
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
    let runtime =
        reth_tasks::RuntimeBuilder::new(reth_tasks::RuntimeConfig::default().with_tokio(
            reth_tasks::TokioConfig::existing_handle(tokio::runtime::Handle::current()),
        ))
        .build()?;
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
    // stake no longer sends ETH: the ConsensusRegistry pulls ERC-20 RLS via transferFrom
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

    // the new validator is staked but waits for its activation epoch: already registered
    let registered = reth_env.registered_validators()?;
    assert_eq!(registered.len(), 6, "five genesis validators plus the pending one");
    assert!(
        registered.iter().any(|v| v.validatorAddress == new_validator.execution_address
            && v.currentStatus == ValidatorStatus::PendingActivation),
        "a staked, not yet active validator is registered"
    );

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
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
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

    // validator 2 is leaving but its stake is still bonded: still registered
    let registered = reth_env.registered_validators()?;
    assert_eq!(registered.len(), 6);
    assert!(
        registered.iter().any(|v| v.validatorAddress == validator_2_address
            && v.currentStatus == ValidatorStatus::PendingExit),
        "a validator pending exit is still registered"
    );

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

    // validator 2 has exited and withdrawn: no longer registered, so its node record would no
    // longer be kept in the kad store
    let registered = reth_env.registered_validators()?;
    assert_eq!(registered.len(), 5);
    assert!(
        registered.iter().all(|v| v.validatorAddress != validator_2_address),
        "an exited validator is not registered"
    );

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

/// Same lifecycle as `test_close_epochs` but with `DynamicCommitteeSizing` active from genesis.
/// Verifies three key differences from the pre-fork behaviour:
///   1. Committee is deterministic (sorted, no shuffle).
///   2. Committee grows when a new validator joins (no truncate).
///   3. Committee shrinks when a validator exits.
#[tokio::test]
async fn test_close_epochs_with_dynamic_committee() -> eyre::Result<()> {
    use crate::chainspec::{RaylsChainSpec, RaylsHardforks};

    let validator_1 = Address::from_slice(&[0x11; 20]);
    let validator_3 = Address::from_slice(&[0x33; 20]);
    let validator_4 = Address::from_slice(&[0x44; 20]);
    let validator_5 = Address::from_slice(&[0x55; 20]);

    let mut new_validator_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

    let mut validator_2_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(2));
    let validator_2_address = validator_2_eoa.address();

    let all_validators = [
        validator_1,
        validator_2_address,
        validator_3,
        validator_4,
        validator_5,
        new_validator_eoa.address(),
    ];

    let mut validators: Vec<_> = all_validators
        .iter()
        .enumerate()
        .map(|(i, addr)| {
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

    let epoch_duration = 60 * 60 * 24;
    let initial_stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: epoch_duration,
    };

    let mut governance_multisig =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let governance = governance_multisig.address();
    let tmp_genesis = rayls_infrastructure_types::test_genesis().extend_accounts([
        (governance, GenesisAccount::default().with_balance(U256::from((50_000_000 * 10) ^ 18))),
        (
            new_validator_eoa.address(),
            GenesisAccount::default()
                .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))),
        ),
        (
            validator_2_address,
            GenesisAccount::default()
                .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))),
        ),
    ]);

    let new_validator = validators.pop().expect("six validators");

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators.clone(),
        tmp_genesis,
        initial_stake_config.clone(),
        governance,
        governance,
        vec![(governance, initial_stake_config.stakeAmount)],
    )?;

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // Build a RaylsChainSpec with DynamicCommitteeSizing active from block 0
    let rayls_chain_spec: Arc<RaylsChainSpec> =
        Arc::new(RaylsChainSpec::builder(chain.clone()).dynamic_committee_sizing(0).build());

    // governance allowlists the new validator
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

    // create env with custom RaylsChainSpec
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_chain_spec.clone(),
        tmp_dir.path(),
        &task_manager,
        None,
    )
    .await?;

    // verify fork is active
    assert!(
        rayls_chain_spec.is_dynamic_committee_sizing_active_at_block(0),
        "DynamicCommitteeSizing must be active at block 0"
    );

    let mut expected_epoch = 0;
    let expected_committee = validators.iter().map(|v| v.execution_address).collect();
    let expected_epoch_info = ConsensusRegistry::EpochInfo {
        committee: expected_committee,
        blockHeight: 0,
        epochDuration: epoch_duration,
        stakeVersion: 0,
    };

    // assert genesis epoch state
    let EpochState { epoch, epoch_info, epoch_start, .. } =
        reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(epoch, expected_epoch);
    assert_eq!(epoch_start, chain.genesis_timestamp());
    assert_eq!(epoch_info, expected_epoch_info);

    // ── Block 1: no epoch close, new validator stakes + activates ──
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

    // ── Block 2: close epoch 0 ──
    // With DynamicCommitteeSizing the future committee is deterministic (sorted, no shuffle).
    // new_validator is PendingActivation at this point, so it is included.
    // Future committee = 6 validators sorted by address.
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 2);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block2.recovered_block.clone_sealed_header();

    // Read future committee (3 epochs ahead of the one that is closing)
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);

    let calldata =
        ConsensusRegistry::getEpochInfoCall { epoch: expected_epoch + 3 }.abi_encode().into();
    let future_epoch_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut rayls_evm, calldata)?;
    expected_epoch += 1;

    // Dynamic committee: 6 validators sorted by address (no truncate)
    let mut expected_sorted_6 = vec![
        validator_1,
        validator_3,
        validator_4,
        validator_5,
        validator_2_address,
        new_validator.execution_address,
    ];
    expected_sorted_6.sort();
    assert_eq!(
        future_epoch_info.committee, expected_sorted_6,
        "dynamic committee must include all 6 active validators sorted by address (no truncate)"
    );

    // ── Block 3: close epoch 1 ──
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 3);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let canonical_header = block3.recovered_block.clone_sealed_header();

    // The 6-validator committee is stored for epoch 3 (two epochs ahead at block 2).
    // It becomes current at epoch 3, which is after block 5 closes epoch 2.
    let EpochState { epoch: current_epoch, .. } = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(current_epoch, expected_epoch + 1);
    // For now, check the future epoch (epoch 3) which has the 6-validator committee.
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);
    let calldata =
        ConsensusRegistry::getEpochInfoCall { epoch: current_epoch + 1 }.abi_encode().into();
    let future_epoch_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut rayls_evm, calldata)?;
    assert_eq!(
        future_epoch_info.committee, expected_sorted_6,
        "future epoch (epoch 3) must have 6 validators"
    );
    expected_epoch += 1;

    // ── Block 4: no epoch close, validator 2 begins exit ──
    let calldata = ConsensusRegistry::beginExitCall {}.abi_encode().into();
    let begin_exit_tx = validator_2_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );
    let mut consensus_output = consensus_output_for_tests(2, expected_epoch, 4);
    consensus_output.close_epoch = false;
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block4 =
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![begin_exit_tx]).await?;
    let canonical_header = block4.recovered_block.clone_sealed_header();

    // ── Block 5: close epoch 2 ──
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 5);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block5 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    expected_epoch += 1;
    let canonical_header = block5.recovered_block.clone_sealed_header();

    // ── Block 6: close epoch 3 ──
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 6);
    let payload = RLPayload::new_for_test(canonical_header, &consensus_output);
    let block6 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    expected_epoch += 1;
    let canonical_header = block6.recovered_block.clone_sealed_header();

    // ── Block 7: close epoch 4 ──
    let consensus_output = consensus_output_for_tests(2, expected_epoch, 7);
    let payload = RLPayload::new_for_test(canonical_header.clone(), &consensus_output);
    execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    expected_epoch += 1;

    // validator 2 should now be Exited
    let state = StateProviderDatabase::new(reth_env.latest()?);
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();
    let mut rayls_evm = reth_env
        .evm_config
        .evm_factory()
        .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header())?);

    let calldata = ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
        .abi_encode()
        .into();
    let validator_2_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(&mut rayls_evm, calldata)?;
    assert_eq!(validator_2_info.currentStatus, ValidatorStatus::Exited);

    // Future committee should have shrunk to 5 (validator 2 excluded)
    let calldata =
        ConsensusRegistry::getEpochInfoCall { epoch: expected_epoch + 1 }.abi_encode().into();
    let shrunken_epoch_info = reth_env
        .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut rayls_evm, calldata)?;

    let mut expected_sorted_5 =
        vec![validator_1, validator_3, validator_4, validator_5, new_validator.execution_address];
    expected_sorted_5.sort();
    assert_eq!(
        shrunken_epoch_info.committee, expected_sorted_5,
        "dynamic committee must shrink to 5 after validator 2 exits"
    );

    Ok(())
}

/// A one-shot hardfork migration runs at its activation block for every batch index.
///
/// Each batch of a consensus output becomes its own block, so the activation block can be a
/// block that is not the first batch of its output. Some pre-execution system calls only run on
/// the first batch. The migration must not be tied to them: a skipped migration leaves later
/// execution running against un-migrated state, and the node diverges.
#[tokio::test]
async fn migration_applies_when_activation_block_is_not_first_batch() -> eyre::Result<()> {
    use crate::{chainspec::RaylsChainSpec, native_erc20::ERC20_PRECOMPILE_ADDRESS};
    use reth_provider::StateProvider as _;

    const ACTIVATION_BLOCK: u64 = 2;

    let chain = rayls_infrastructure_types::test_chain_spec_arc();
    let rayls_chain_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone()).erc20_precompile_bytecode(ACTIVATION_BLOCK).build(),
    );
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_chain_spec,
        tmp_dir.path(),
        &task_manager,
        None,
    )
    .await?;

    // block 1: before the activation block, the precompile account has no code
    let mut co = consensus_output_for_tests(2, 0, 1);
    co.close_epoch = false;
    let payload = RLPayload::new_for_test(chain.sealed_genesis_header(), &co);
    let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    assert!(
        reth_env.latest()?.account_code(&ERC20_PRECOMPILE_ADDRESS)?.is_none(),
        "no bytecode before the activation block"
    );

    // block 2: the activation block, built as the second batch of its output
    let mut payload = RLPayload::new_for_test(block1.recovered_block.clone_sealed_header(), &co);
    payload.batch_index = 1;
    execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    assert!(
        reth_env.latest()?.account_code(&ERC20_PRECOMPILE_ADDRESS)?.is_some(),
        "the Erc20PrecompileBytecode migration must install bytecode on a non-first batch"
    );

    Ok(())
}

/// Fork-boundary test for the `HybridRewards` hardfork.
///
/// With a synthetic schedule activating at `FORK_BLOCK`, drive epoch closes on both sides in a
/// single run. Genesis deploys the pre-hybrid contract, so a successful close *before* the fork
/// proves the legacy 1-arg `applyIncentives` matches it, and a successful close *at/after* the
/// fork proves the in-place migration swapped the contract and the hybrid 2-arg `applyIncentives`
/// matches the new one. A wrong ABI on either side (or a bad migration) would revert
/// `applyIncentives` and fail block execution, so reaching `Ok` on both sides is the assertion;
/// the registry code length (21929 -> 23042 bytes) is checked directly as migration proof.
#[tokio::test]
async fn test_hybrid_rewards_fork_boundary() -> eyre::Result<()> {
    use crate::chainspec::{RaylsChainSpec, RaylsHardforks};
    use reth_provider::StateProvider as _;

    const FORK_BLOCK: u64 = 3;
    const OLD_REGISTRY_LEN: usize = 21_929;
    const HYBRID_REGISTRY_LEN: usize = 23_042;

    // Four genesis validators with BLS proofs of possession.
    let validator_addrs = [
        Address::from_slice(&[0x11; 20]),
        Address::from_slice(&[0x22; 20]),
        Address::from_slice(&[0x33; 20]),
        Address::from_slice(&[0x44; 20]),
    ];
    let validators: Vec<NodeInfo> = validator_addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let bls = BlsKeypair::generate(&mut rng);
            let pop = generate_proof_of_possession_bls(&bls, addr).expect("pop generation failed");
            NodeInfo {
                name: format!("validator-{i}"),
                bls_public_key: *bls.public(),
                p2p_info: NodeP2pInfo::default(),
                execution_address: *addr,
                proof_of_possession: pop,
            }
        })
        .collect();

    let epoch_duration = 60 * 60 * 24;
    let stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: epoch_duration,
    };

    let governance_multisig =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let governance = governance_multisig.address();
    let tmp_genesis = rayls_infrastructure_types::test_genesis().extend_accounts([(
        governance,
        GenesisAccount::default().with_balance(U256::from(parse_ether("100_000_000").unwrap())),
    )]);

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators.clone(),
        tmp_genesis,
        stake_config.clone(),
        governance,
        governance,
        vec![(governance, stake_config.stakeAmount)],
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // Synthetic schedule: only HybridRewards, active at FORK_BLOCK.
    let rayls_chain_spec: Arc<RaylsChainSpec> =
        Arc::new(RaylsChainSpec::builder(chain.clone()).hybrid_rewards(FORK_BLOCK).build());
    assert!(
        !rayls_chain_spec.is_hybrid_rewards_active_at_block(FORK_BLOCK - 1),
        "HybridRewards must be inactive before the fork block"
    );
    assert!(
        rayls_chain_spec.is_hybrid_rewards_active_at_block(FORK_BLOCK),
        "HybridRewards must be active at the fork block"
    );

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_chain_spec.clone(),
        tmp_dir.path(),
        &task_manager,
        None,
    )
    .await?;

    let registry_code_len = |env: &RethEnv| -> eyre::Result<usize> {
        Ok(env
            .latest()?
            .account_code(&CONSENSUS_REGISTRY_ADDRESS)?
            .expect("ConsensusRegistry must have code")
            .original_byte_slice()
            .len())
    };

    // Genesis deploys the pre-hybrid ConsensusRegistry.
    assert_eq!(
        registry_code_len(&reth_env)?,
        OLD_REGISTRY_LEN,
        "genesis must be the pre-hybrid contract"
    );

    let mut epoch = 0u32;

    // ── Block 1 (< FORK_BLOCK): no epoch close ──
    let mut co = consensus_output_for_tests(2, epoch, 1);
    co.close_epoch = false;
    let payload = RLPayload::new_for_test(chain.sealed_genesis_header(), &co);
    let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    let mut header = block1.recovered_block.clone_sealed_header();

    // ── Block 2 (< FORK_BLOCK): close epoch 0 via the LEGACY 1-arg ABI ──
    let co = consensus_output_for_tests(2, epoch, 2);
    let payload = RLPayload::new_for_test(header, &co);
    let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    // Revert entries per block in the bundle. A normal block has one. An epoch-close block
    // has two, the second one empty: the transitions are merged once for the epoch close and
    // once at the end of block building. This shape is part of what is persisted, so it must
    // not change without a decision.
    assert_eq!(block1.execution_output.state.reverts.len(), 1, "normal block");
    assert_eq!(block2.execution_output.state.reverts.len(), 2, "epoch-close block");
    assert!(
        block2.execution_output.state.reverts.last().is_some_and(|r| r.is_empty()),
        "the extra revert entry of an epoch-close block is empty"
    );
    header = block2.recovered_block.clone_sealed_header();
    epoch += 1;
    assert_eq!(
        registry_code_len(&reth_env)?,
        OLD_REGISTRY_LEN,
        "contract must still be pre-hybrid before the fork block"
    );

    // ── Block 3 (== FORK_BLOCK): migration swaps the contract, then the HYBRID 2-arg ABI ──
    let co = consensus_output_for_tests(2, epoch, 3);
    let payload = RLPayload::new_for_test(header, &co);
    let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    header = block3.recovered_block.clone_sealed_header();
    epoch += 1;
    assert_eq!(
        registry_code_len(&reth_env)?,
        HYBRID_REGISTRY_LEN,
        "fork block must migrate to the hybrid contract"
    );

    // ── Block 4 (> FORK_BLOCK): another hybrid-path close ──
    let co = consensus_output_for_tests(2, epoch, 4);
    let payload = RLPayload::new_for_test(header, &co);
    execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
    assert_eq!(
        registry_code_len(&reth_env)?,
        HYBRID_REGISTRY_LEN,
        "hybrid contract must persist after the fork block"
    );

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

/// A genesis account for the genesis-history tests: balance, code and one storage slot. None
/// of them changes after genesis, so only the genesis history entry can answer a historical
/// read for it.
const GENESIS_HISTORY_ADDR: Address = Address::new([0x11u8; 20]);
const GENESIS_HISTORY_SLOT: B256 = B256::with_last_byte(1);
const GENESIS_HISTORY_VALUE: U256 = U256::from_limbs([0xAA, 0, 0, 0]);
const GENESIS_HISTORY_CODE: [u8; 2] = [0x60, 0x00];

fn genesis_with_static_account() -> rayls_infrastructure_types::Genesis {
    rayls_infrastructure_types::test_genesis().extend_accounts([(
        GENESIS_HISTORY_ADDR,
        GenesisAccount {
            balance: U256::from(1_000u64),
            code: Some(Bytes::from_static(&GENESIS_HISTORY_CODE)),
            storage: Some(std::collections::BTreeMap::from([(
                GENESIS_HISTORY_SLOT,
                B256::from(GENESIS_HISTORY_VALUE),
            )])),
            ..Default::default()
        },
    )])
}

/// A v2 storage stack with genesis written, without the rest of the node.
fn genesis_storage_stack(
    datadir: &std::path::Path,
) -> eyre::Result<reth_provider::ProviderFactory<crate::traits::RaylsNode>> {
    use reth_db_common::init::init_genesis_with_settings;
    use reth_provider::{
        providers::{RocksDBBuilder, StaticFileProvider},
        ProviderFactory, StorageSettings, StorageSettingsCache,
    };

    let db = Arc::new(reth_db::init_db(
        datadir.join("db"),
        reth_db::mdbx::DatabaseArguments::default(),
    )?);
    let rayls_chain_spec = Arc::new(
        crate::chainspec::RaylsChainSpec::builder(Arc::new(RethChainSpec::from(
            genesis_with_static_account(),
        )))
        .build(),
    );
    let rocksdb_provider =
        RocksDBBuilder::new(datadir.join("rocksdb")).with_default_tables().build()?;
    let static_files = StaticFileProvider::read_write(datadir.join("static_files"))?;
    let runtime =
        reth_tasks::RuntimeBuilder::new(reth_tasks::RuntimeConfig::default().with_tokio(
            reth_tasks::TokioConfig::existing_handle(tokio::runtime::Handle::current()),
        ))
        .build()?;
    let provider_factory: ProviderFactory<crate::traits::RaylsNode> =
        ProviderFactory::new(db, rayls_chain_spec, static_files, rocksdb_provider, runtime)?;
    let settings = StorageSettings::v2();
    provider_factory.set_storage_settings_cache(settings);
    init_genesis_with_settings(&provider_factory, settings)?;
    Ok(provider_factory)
}

/// Genesis state that never changes again is readable at a historical block.
///
/// On reth 1.11 this needed two start-up fixes (`fix_genesis_history` and
/// `fix_genesis_account_history`): the genesis storage history was written under the plain slot
/// but read under the hashed one, and the genesis account history could be lost. reth 2.0 reads
/// and writes the plain slot, and both fixes are removed. This test is the proof that a node
/// without them answers historical reads for genesis-only state, through the real start-up and
/// block-building path.
#[tokio::test]
async fn genesis_only_state_is_readable_at_historical_blocks() -> eyre::Result<()> {
    use reth_provider::{StateProvider as _, StateProviderFactory as _};

    let chain: Arc<RethChainSpec> = Arc::new(genesis_with_static_account().into());
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env =
        RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None).await?;

    // three persisted blocks that do not touch the account
    let mut header = chain.sealed_genesis_header();
    for number in 1..=3u64 {
        let mut co = consensus_output_for_tests(2, 0, number);
        co.close_epoch = false;
        let payload = RLPayload::new_for_test(header, &co);
        let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![]).await?;
        header = block.recovered_block.clone_sealed_header();
    }

    // blocks 0..=2 are historical now (the tip is 3)
    for number in 0..=2u64 {
        let state = reth_env.blockchain_provider.history_by_block_number(number)?;
        assert_eq!(
            state.storage(GENESIS_HISTORY_ADDR, GENESIS_HISTORY_SLOT)?,
            Some(GENESIS_HISTORY_VALUE),
            "genesis storage at block {number}"
        );
        let code = state.account_code(&GENESIS_HISTORY_ADDR)?;
        assert_eq!(
            code.map(|c| c.original_bytes()),
            Some(Bytes::from_static(&GENESIS_HISTORY_CODE)),
            "genesis code at block {number}"
        );
        let account = state.basic_account(&GENESIS_HISTORY_ADDR)?.expect("genesis account");
        assert_eq!(account.balance, U256::from(1_000u64), "genesis balance at block {number}");
    }

    Ok(())
}

/// The genesis history index is right after genesis and stays right over a restart.
///
/// reth clears both history tables on every start while no block is persisted yet (history
/// checkpoint 0). On reth 1.11 it did not write the genesis entries back, which lost them
/// (the reason for `fix_genesis_account_history`). reth 2.0 writes them back. This test deletes
/// the entries and checks that the start-up check brings them back. It also pins the key: the
/// storage entry is under the PLAIN slot, and there is no entry under the hashed slot, which
/// is what the removed `fix_genesis_history` used to add.
#[tokio::test]
async fn genesis_history_index_survives_restart_before_first_block() -> eyre::Result<()> {
    use alloy::primitives::keccak256;
    use reth_provider::RocksDBProviderFactory;

    let tmp_dir = TempDir::new()?;
    let provider_factory = genesis_storage_stack(tmp_dir.path())?;

    let assert_genesis_index = |when: &str| -> eyre::Result<()> {
        let rocksdb = provider_factory.rocksdb_provider();
        assert!(
            rocksdb
                .account_history_shards(GENESIS_HISTORY_ADDR)?
                .iter()
                .any(|(_, blocks)| blocks.contains(0u64)),
            "{when}: genesis account must have block 0 in its history"
        );
        assert!(
            rocksdb
                .storage_history_shards(GENESIS_HISTORY_ADDR, GENESIS_HISTORY_SLOT)?
                .iter()
                .any(|(_, blocks)| blocks.contains(0u64)),
            "{when}: genesis slot must have block 0 under the plain slot key"
        );
        assert!(
            rocksdb
                .storage_history_shards(GENESIS_HISTORY_ADDR, keccak256(GENESIS_HISTORY_SLOT))?
                .is_empty(),
            "{when}: there must be no history under the hashed slot key"
        );
        Ok(())
    };

    assert_genesis_index("after genesis init")?;

    // Lose the genesis entries, as a reth 1.11 restart did. Without this step the test would
    // pass the same way if reth never touched the tables.
    {
        use reth_db::{
            models::{storage_sharded_key::StorageShardedKey, ShardedKey},
            tables,
        };
        let rocksdb = provider_factory.rocksdb_provider();
        let mut batch = rocksdb.batch();
        batch.delete::<tables::AccountsHistory>(ShardedKey::last(GENESIS_HISTORY_ADDR))?;
        batch.delete::<tables::StoragesHistory>(StorageShardedKey::last(
            GENESIS_HISTORY_ADDR,
            GENESIS_HISTORY_SLOT,
        ))?;
        batch.commit()?;
        assert!(
            rocksdb.account_history_shards(GENESIS_HISTORY_ADDR)?.is_empty()
                && rocksdb
                    .storage_history_shards(GENESIS_HISTORY_ADDR, GENESIS_HISTORY_SLOT)?
                    .is_empty(),
            "the genesis entries are gone before the start-up check"
        );
    }

    // what every node start runs; with the checkpoint at 0 it clears and re-seeds the tables
    for restart in 1..=2 {
        provider_factory.check_consistency()?;
        assert_genesis_index(&format!("after restart {restart}"))?;
    }

    Ok(())
}

/// A small chain for the history tests: a contract that stores the current block number in
/// slot 1, and a funded sender that calls it.
struct HistoryChain {
    reth_env: RethEnv,
    chain: Arc<RethChainSpec>,
    factory: TransactionFactory,
    header: rayls_infrastructure_types::SealedHeader,
    next_nonce: u64,
    // keep the node alive for the whole test
    _task_manager: TaskManager,
    _tmp_dir: TempDir,
}

impl HistoryChain {
    const CONTRACT: Address = Address::new([0x33u8; 20]);
    const SLOT: B256 = B256::with_last_byte(1);
    const GENESIS_VALUE: U256 = U256::from_limbs([0xAA, 0, 0, 0]);

    async fn new() -> eyre::Result<Self> {
        // NUMBER PUSH1 1 SSTORE STOP
        let genesis = rayls_infrastructure_types::test_genesis().extend_accounts([(
            Self::CONTRACT,
            GenesisAccount {
                code: Some(Bytes::from_static(&[0x43, 0x60, 0x01, 0x55, 0x00])),
                storage: Some(std::collections::BTreeMap::from([(
                    Self::SLOT,
                    B256::from(Self::GENESIS_VALUE),
                )])),
                ..Default::default()
            },
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None).await?;
        let header = chain.sealed_genesis_header();
        Ok(Self {
            reth_env,
            chain,
            // the default `TransactionFactory` address is funded in `test_genesis` at nonce 0
            factory: TransactionFactory::new(),
            header,
            next_nonce: 0,
            _task_manager: task_manager,
            _tmp_dir: tmp_dir,
        })
    }

    fn sender(&self) -> Address {
        self.factory.address()
    }

    /// Build, persist and flush the next block. With `call_contract` it holds one transaction
    /// from the sender that calls the contract.
    async fn next_block(&mut self, call_contract: bool) -> eyre::Result<u64> {
        let number = self.header.number + 1;
        let txs = if call_contract {
            self.factory.set_nonce(self.next_nonce);
            self.next_nonce += 1;
            vec![self.factory.create_eip1559_encoded(
                self.chain.clone(),
                None,
                self.reth_env.get_gas_price()?,
                Some(Self::CONTRACT),
                U256::ZERO,
                Default::default(),
            )]
        } else {
            vec![]
        };
        let mut co = consensus_output_for_tests(2, 0, number);
        co.close_epoch = false;
        let payload = RLPayload::new_for_test(self.header.clone(), &co);
        let block =
            execute_payload_and_update_canonical_chain(&self.reth_env, payload, txs).await?;
        assert_eq!(
            block.recovered_block.body().transactions.len(),
            usize::from(call_contract),
            "block {number} must include its transaction"
        );
        self.header = block.recovered_block.clone_sealed_header();
        Ok(number)
    }

    /// Slot value and sender nonce at the end of a historical block.
    fn state_at(&self, number: u64) -> eyre::Result<(U256, u64)> {
        use reth_provider::{StateProvider as _, StateProviderFactory as _};
        let state = self.reth_env.blockchain_provider.history_by_block_number(number)?;
        Ok((
            state.storage(Self::CONTRACT, Self::SLOT)?.unwrap_or_default(),
            state.basic_account(&self.sender())?.map(|a| a.nonce).unwrap_or_default(),
        ))
    }
}

/// History written by normal blocks is still there after the start-up check runs again.
///
/// reth's start-up check clears both history tables and re-seeds only genesis while the history
/// stage checkpoint is 0. Saving a block moves that checkpoint (`save_blocks` updates all stage
/// checkpoints), so the clear must not run on a node that has saved blocks. If it did, every
/// restart would drop the history of all blocks. This test builds blocks that change a storage
/// slot and an account, runs the start-up check, and reads the old values again.
///
/// The check is called directly on the open node env. It is the step a node start runs, but it
/// is not a full stop and start of the process.
#[tokio::test]
async fn block_history_survives_the_start_up_check() -> eyre::Result<()> {
    let mut chain = HistoryChain::new().await?;
    // blocks 1 and 3 call the contract, blocks 2 and 4 are empty
    for call_contract in [true, false, true, false] {
        chain.next_block(call_contract).await?;
    }

    // slot value and sender nonce at the end of each historical block (the tip is block 4)
    let expected = [
        (0u64, HistoryChain::GENESIS_VALUE, 0u64),
        (1, U256::from(1u64), 1),
        (2, U256::from(1u64), 1),
        (3, U256::from(3u64), 2),
    ];
    let assert_history = |when: &str| -> eyre::Result<()> {
        for (number, slot_value, nonce) in expected {
            assert_eq!(chain.state_at(number)?, (slot_value, nonce), "{when}: block {number}");
        }
        Ok(())
    };

    assert_history("before the start-up check")?;

    // what every node start runs; it must find nothing to unwind and must not clear history
    let unwind = chain.reth_env.provider_factory.check_consistency()?;
    assert_eq!(unwind, (None, None), "nothing to unwind");

    assert_history("after the start-up check")?;

    Ok(())
}

/// `heal_rocksdb_history_after_snapshot` is still needed on reth 2.0, and it works.
///
/// A datadir copied while the node runs can hold a history index (RocksDB) that already knows
/// blocks the rest of the datadir does not have. The test builds that state on a node with
/// saved blocks and runs the two start-up steps in their real order:
///
/// 1. reth's own check. It only repairs the index when the changeset static files are ahead too.
///    Here they are not, so it must leave the stale entries in place. This is the part that shows
///    our heal is still needed.
/// 2. our heal. It must cut the index back to the tip.
///
/// After that the node must be able to build and save the very block the stale entries pointed
/// to. Without the heal that save fails and the persistence service stops: reth's history
/// append rejects a block number that is already in the list (`UnsortedInput`). Checked by
/// running this test once with the heal call removed.
#[tokio::test]
async fn heal_is_needed_after_reths_check_and_lets_the_next_block_persist() -> eyre::Result<()> {
    use reth_db::{
        models::{storage_sharded_key::StorageShardedKey, ShardedKey},
        tables, BlockNumberList,
    };
    use reth_provider::RocksDBProviderFactory;

    fn blocks_of<K>(shards: Vec<(K, BlockNumberList)>) -> Vec<u64> {
        shards.into_iter().flat_map(|(_, blocks)| blocks.iter().collect::<Vec<_>>()).collect()
    }

    let mut chain = HistoryChain::new().await?;
    for call_contract in [true, false, true, false] {
        chain.next_block(call_contract).await?;
    }
    let (sender, contract, slot) = (chain.sender(), HistoryChain::CONTRACT, HistoryChain::SLOT);
    let tip = 4u64;

    let account_blocks = |chain: &HistoryChain| -> eyre::Result<Vec<u64>> {
        Ok(blocks_of(
            chain.reth_env.provider_factory.rocksdb_provider().account_history_shards(sender)?,
        ))
    };
    let slot_blocks = |chain: &HistoryChain| -> eyre::Result<Vec<u64>> {
        Ok(blocks_of(
            chain
                .reth_env
                .provider_factory
                .rocksdb_provider()
                .storage_history_shards(contract, slot)?,
        ))
    };

    // the real history after 4 blocks
    let real_account_blocks = account_blocks(&chain)?;
    let real_slot_blocks = slot_blocks(&chain)?;
    assert!(real_account_blocks.iter().all(|b| *b <= tip) && real_account_blocks.contains(&3));
    assert!(real_slot_blocks.iter().all(|b| *b <= tip) && real_slot_blocks.contains(&3));

    // Add block 5 to both lists, as a copy would carry it when the index is ahead. Block 5 is
    // the block the node builds next, and it changes this account and this slot.
    {
        let rocksdb = chain.reth_env.provider_factory.rocksdb_provider();
        let with_stale = |real: &[u64]| {
            BlockNumberList::new(real.iter().copied().chain([tip + 1])).expect("sorted")
        };
        let mut batch = rocksdb.batch();
        batch.put::<tables::AccountsHistory>(
            ShardedKey::last(sender),
            &with_stale(&real_account_blocks),
        )?;
        batch.put::<tables::StoragesHistory>(
            StorageShardedKey::last(contract, slot),
            &with_stale(&real_slot_blocks),
        )?;
        batch.commit()?;
    }

    // step 1: reth's own start-up check does not repair this case
    let unwind = chain.reth_env.provider_factory.check_consistency()?;
    assert_eq!(unwind, (None, None), "reth sees nothing to unwind");
    assert!(account_blocks(&chain)?.contains(&(tip + 1)), "reth leaves the stale account entry");
    assert!(slot_blocks(&chain)?.contains(&(tip + 1)), "reth leaves the stale slot entry");

    // step 2: our heal cuts the index back to the tip
    RethEnv::heal_rocksdb_history_after_snapshot(&chain.reth_env.provider_factory)?;
    assert_eq!(account_blocks(&chain)?, real_account_blocks, "account history is back to the tip");
    assert_eq!(slot_blocks(&chain)?, real_slot_blocks, "slot history is back to the tip");

    // the node can now build and save block 5, which touches the healed account and slot
    assert_eq!(chain.next_block(true).await?, tip + 1);
    chain.next_block(false).await?;
    assert_eq!(account_blocks(&chain)?.iter().filter(|b| **b == tip + 1).count(), 1);
    assert_eq!(slot_blocks(&chain)?.iter().filter(|b| **b == tip + 1).count(), 1);
    assert_eq!(chain.state_at(tip + 1)?, (U256::from(tip + 1), 3), "block 5 is readable");

    Ok(())
}

/// The persistence threshold `rayls-replay` uses (512) must not reach reth's engine config.
///
/// reth 2.0 debug-asserts in `EngineArgs::tree_config` that its backpressure threshold (16 by
/// default) is above the persistence threshold. With 512 that assert fails in a debug or test
/// build. `RethEnv::engine_tree_config` builds the config with reth's default persistence
/// threshold.
#[test]
fn engine_tree_config_accepts_a_large_persistence_threshold() {
    use reth_engine_primitives::{
        DEFAULT_PERSISTENCE_BACKPRESSURE_THRESHOLD, DEFAULT_PERSISTENCE_THRESHOLD,
    };
    use reth_node_core::args::EngineArgs;

    let engine = EngineArgs {
        persistence_threshold: 512,
        prewarming_disabled: true,
        cross_block_cache_size: 123,
        ..Default::default()
    };

    // must not panic
    let tree_config = RethEnv::engine_tree_config(&engine);

    // the thresholds reth's engine tree would read keep reth's defaults
    assert_eq!(tree_config.persistence_threshold(), DEFAULT_PERSISTENCE_THRESHOLD);
    assert_eq!(
        tree_config.persistence_backpressure_threshold(),
        DEFAULT_PERSISTENCE_BACKPRESSURE_THRESHOLD
    );
    // every other engine setting is passed on (the cache size is given in MB, kept in bytes)
    assert!(tree_config.disable_prewarming());
    assert_eq!(tree_config.cross_block_cache_size(), 123 * 1024 * 1024);
}

/// The env `rayls-replay` builds, with its threshold of 512, starts in a debug build, and our
/// own persistence state gets the 512.
///
/// This is the real path of the fix above: before it, building this env hit reth's
/// debug-assert.
#[cfg(feature = "archive-replay")]
#[tokio::test]
async fn replay_env_keeps_a_large_persistence_threshold() -> eyre::Result<()> {
    use crate::NetworkProfile;
    use reth_chainspec::EthChainSpec as _;

    let chain = rayls_infrastructure_types::test_chain_spec_arc();
    let profile = NetworkProfile { chain_id: chain.chain().id(), hardforks: Default::default() };
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_archive_replay(
        chain,
        tmp_dir.path(),
        &task_manager,
        &profile,
        None,
        None,
        true,
        Some(512),
        rayls_middleware_rewards::RewardsCounter::default(),
    )
    .await?;

    assert_eq!(reth_env.persistence_state.lock().persistence_threshold, 512);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Golden values (#417)
//
// Every other test only checks that the node agrees with itself. This one compares a genesis and
// four blocks with values written down here. If a dependency upgrade or a code change alters what
// the node computes, the hashes change and the test fails.
//
// The same test with the same values passes on reth 1.11 (v1.5.3) and on reth 2.0. The values
// may only change together with a hardfork, or with a change of the inputs. The inputs are the
// values below and three shared fixtures: `test_genesis()` with `TEST_GENESIS_YAML`, the contract
// bytecodes that the genesis build and the migrations deploy, and the key of
// `TransactionFactory::new()`.
// ---------------------------------------------------------------------------------------------

/// Fixed genesis time for the golden chain. Block `n` has timestamp `GOLDEN_GENESIS_TIME + n`.
const GOLDEN_GENESIS_TIME: u64 = 1_750_000_000;

/// The golden genesis: the test genesis with a fixed timestamp and four validators, built by the
/// same two steps the `genesis` command runs (the consensus registry simulation, then the
/// greenfield overrides).
///
/// The validator keys are built from fixed secret bytes (the scalars 1 to 4), not from a random
/// generator: `StdRng` is not promised to give the same numbers in a later `rand` version, and
/// the keys go into the genesis state.
fn golden_genesis() -> eyre::Result<rayls_infrastructure_types::Genesis> {
    let validators: Vec<NodeInfo> = (0u8..4)
        .map(|i| {
            let addr = Address::from_slice(&[(i + 1) * 0x11; 20]);
            let mut secret = [0u8; 32];
            secret[31] = i + 1;
            let bls = BlsKeypair::from_bytes(&secret).expect("a small scalar is a valid key");
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
    let stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochDuration: 60 * 60 * 24,
    };
    let governance = Address::from_slice(&[0x60; 20]);

    let mut base = rayls_infrastructure_types::test_genesis();
    base.timestamp = GOLDEN_GENESIS_TIME;
    let base = base.extend_accounts([(
        governance,
        GenesisAccount::default().with_balance(U256::from(parse_ether("100_000_000").unwrap())),
    )]);

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        validators,
        base,
        stake_config.clone(),
        governance,
        governance,
        vec![(governance, stake_config.stakeAmount)],
    )?;
    let mut alloc: std::collections::BTreeMap<Address, GenesisAccount> =
        genesis.alloc.iter().map(|(a, g)| (*a, g.clone())).collect();
    crate::reth_env::genesis::apply_greenfield_fixes(&mut alloc);
    Ok(genesis.extend_accounts(alloc))
}

/// A payload with no random and no clock input.
fn golden_payload(
    parent: rayls_infrastructure_types::SealedHeader,
    epoch: u32,
    round: u32,
    close_epoch: Option<B256>,
) -> RLPayload {
    let number = parent.number + 1;
    let tag = number as u8;
    RLPayload {
        base_fee_per_gas: parent
            .base_fee_per_gas
            .unwrap_or(rayls_infrastructure_types::MIN_PROTOCOL_BASE_FEE),
        gas_limit: parent.gas_limit,
        parent_header: parent,
        beneficiary: Address::repeat_byte(0xBE),
        nonce: (u64::from(epoch) << 32) | u64::from(round),
        batch_index: 0,
        timestamp: GOLDEN_GENESIS_TIME + number,
        batch_digest: B256::repeat_byte(0xB0 + tag),
        consensus_header_digest: B256::repeat_byte(0xC0 + tag),
        mix_hash: B256::repeat_byte(0xD0 + tag),
        close_epoch,
        worker_id: 0,
    }
}

/// Genesis hash, and hash, state root, receipts root and gas used of four blocks, against fixed
/// values.
///
/// The chain uses the hardfork schedule of the local network. What the blocks cover:
/// - genesis: the pre-genesis EVM simulation that builds the consensus registry state
/// - block 1: the two one-shot migrations of the local schedule, a transfer, a contract creation
/// - block 2: a contract call with a storage write and a log, a transfer through the native ERC-20
///   precompile
/// - block 3: an epoch close, with its system calls
/// - block 4: a transfer in the new epoch
///
/// Not covered: the reward amounts. The test env has no consensus database behind its rewards
/// counter, so the epoch close runs with an empty tally. The close pins the system calls and the
/// committee change, not the reward math.
#[tokio::test]
async fn golden_genesis_and_blocks() -> eyre::Result<()> {
    use crate::NetworkProfile;
    use rayls_infrastructure_types::RaylsNetwork;
    use reth_provider::{ReceiptProvider as _, StateProvider as _};

    // genesis hash, genesis state root
    const EXPECTED_GENESIS: [&str; 2] = [
        "0x343f67c779d2b6631543451f97880881a9c1bcd12072b5181f0bac7040171617",
        "0x2d191fa6b14b09efb7932d2c3acbb25ec78da7eefc7ddc08986df8606260a8c5",
    ];
    // per block: hash, state root, receipts root, gas used, transactions
    const EXPECTED_BLOCKS: [[&str; 5]; 4] = [
        // block 1
        [
            "0x175c2d005d7c455921c7c1e942f4bfcec3f0c2df1246491bd3520f4e6e3b60bf",
            "0xc5cb2555fe8737e8703c545ebe9df8ec150b09f434a29d11236d3ec075be21ca",
            "0x7b63442fc3bb636f45608775a000448294c5f3181db035f76b5c78ccb11e6928",
            "76276",
            "2",
        ],
        // block 2
        [
            "0x62d1d38c68e82370b3d5cff9f6672a845c67049003e2d0f3ec72577499f1365c",
            "0x3f53b07849ca405108735154b807738875be7e80064315a4a57d4fef160c0206",
            "0x7560ccf08f392a89e26049c789995dc15f0a8cec1a966ca58aa04bcabbae0466",
            "74070",
            "2",
        ],
        // block 3
        [
            "0xef141cf0b9171844dc20a0cfda6d023a82ccf9b3eb1647b6624e943ccc2683c9",
            "0x6f024e556afa4b506be7fb8b3cc420685f8954ed5b753257f455529d460db8cd",
            "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "0",
            "0",
        ],
        // block 4
        [
            "0x5dbde113bf3ea0a9bcd5339828bc0ea09ef42c28a302d36297af8a2a0b4de5db",
            "0x32440657f8b0dd5927e51c8f68559146492909d3aaeeeeff8059db44bf749ee7",
            "0xf3e5d7efe2f0cec3853ea3580e632cb94238b6749655d3955fbc81f0c6a8f62a",
            "21000",
            "1",
        ],
    ];

    let chain: Arc<RethChainSpec> = Arc::new(golden_genesis()?.into());
    let profile = NetworkProfile::from_builtin(RaylsNetwork::Local);
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain_with_profile(
        chain.clone(),
        tmp_dir.path(),
        &task_manager,
        None,
        &profile,
    )
    .await?;

    let genesis_header = chain.sealed_genesis_header();
    let actual_genesis =
        [format!("{:?}", genesis_header.hash()), format!("{:?}", genesis_header.state_root)];

    // the default `TransactionFactory` address is funded in `test_genesis` at nonce 0
    let mut factory = TransactionFactory::new();
    let sender = factory.address();
    // The key of `TransactionFactory::new()` comes from a seeded `StdRng`. If a `rand` upgrade
    // changes it, fail here with a clear reason, not with a hash mismatch below.
    assert_eq!(
        sender,
        rayls_infrastructure_types::address!("b14d3c4f5fbfbcfb98af2d330000d49c95b93aa7"),
        "the key of TransactionFactory::new() changed; the golden values depend on it"
    );
    let gas_price = reth_env.get_gas_price()?;
    let mut nonce = 0u64;
    let mut tx = |to: Option<Address>, value: U256, input: Bytes| {
        factory.set_nonce(nonce);
        nonce += 1;
        factory.create_eip1559_encoded(chain.clone(), None, gas_price, to, value, input)
    };

    // init code that deploys the runtime `NUMBER PUSH1 1 SSTORE PUSH1 0 PUSH1 0 LOG0 STOP`:
    // PUSH10 <runtime> PUSH1 0 MSTORE PUSH1 10 PUSH1 22 RETURN
    let init_code = Bytes::from_static(&[
        0x69, 0x43, 0x60, 0x01, 0x55, 0x60, 0x00, 0x60, 0x00, 0xa0, 0x00, 0x60, 0x00, 0x52, 0x60,
        0x0a, 0x60, 0x16, 0xf3,
    ]);
    let contract = sender.create(1);
    let receiver = Address::repeat_byte(0xAA);
    // `transfer(address,uint256)` on the native ERC-20 precompile
    let precompile = crate::native_erc20::ERC20_PRECOMPILE_ADDRESS;
    let mut erc20_transfer = vec![0xa9, 0x05, 0x9c, 0xbb];
    erc20_transfer.extend_from_slice(&[0u8; 12]);
    erc20_transfer.extend_from_slice(receiver.as_slice());
    erc20_transfer.extend_from_slice(&U256::from(12_345u64).to_be_bytes::<32>());

    // (epoch, round, close_epoch, transactions)
    let blocks: Vec<(u32, u32, Option<B256>, Vec<Bytes>)> = vec![
        (
            0,
            2,
            None,
            vec![
                tx(Some(receiver), U256::from(1_000_000u64), Bytes::new()),
                tx(None, U256::ZERO, init_code),
            ],
        ),
        (
            0,
            4,
            None,
            vec![
                tx(Some(contract), U256::ZERO, Bytes::new()),
                tx(Some(precompile), U256::ZERO, erc20_transfer.into()),
            ],
        ),
        (0, 6, Some(B256::repeat_byte(0xEC)), vec![]),
        (1, 2, None, vec![tx(Some(receiver), U256::from(7u64), Bytes::new())]),
    ];

    // The local schedule runs two one-shot migrations at block 1: one gives the native ERC-20
    // precompile account its bytecode, one swaps the consensus registry bytecode. Read both
    // before block 1, to check after it that they really ran.
    let code_of = |address: Address| -> eyre::Result<Option<Bytes>> {
        Ok(reth_env.latest()?.account_code(&address)?.map(|code| code.original_bytes()))
    };
    assert_eq!(code_of(precompile)?, None, "the precompile account has no code at genesis");
    let registry_code_at_genesis = code_of(CONSENSUS_REGISTRY_ADDRESS)?;
    assert!(registry_code_at_genesis.is_some(), "genesis deploys the consensus registry");

    let mut header = genesis_header;
    let mut actual_blocks: Vec<[String; 5]> = Vec::new();
    for (epoch, round, close_epoch, txs) in blocks {
        let sent = txs.len();
        let payload = golden_payload(header, epoch, round, close_epoch);
        let block = execute_payload_and_update_canonical_chain(&reth_env, payload, txs).await?;
        header = block.recovered_block.clone_sealed_header();

        // every transaction must be in the block and must have succeeded: the values below are
        // only worth pinning for work that really ran
        assert_eq!(
            block.recovered_block.body().transactions.len(),
            sent,
            "block {}: all transactions included",
            header.number
        );
        let receipts = reth_env
            .blockchain_provider
            .receipts_by_block(header.number.into())?
            .expect("receipts of a persisted block");
        assert_eq!(receipts.len(), sent, "block {}: one receipt per transaction", header.number);
        assert!(receipts.iter().all(|r| r.success), "block {}: all succeeded", header.number);

        if header.number == 1 {
            assert!(
                code_of(precompile)?.is_some(),
                "block 1 must run the Erc20PrecompileBytecode migration"
            );
            assert_ne!(
                code_of(CONSENSUS_REGISTRY_ADDRESS)?,
                registry_code_at_genesis,
                "block 1 must run the HybridRewards migration"
            );
        }

        actual_blocks.push([
            format!("{:?}", header.hash()),
            format!("{:?}", header.state_root),
            format!("{:?}", header.receipts_root),
            header.gas_used.to_string(),
            block.recovered_block.body().transactions.len().to_string(),
        ]);
    }

    // the contract call in block 2 wrote the block number
    {
        assert_eq!(
            reth_env.latest()?.storage(contract, B256::with_last_byte(1))?,
            Some(U256::from(2u64)),
            "the contract stored the number of block 2"
        );
    }

    let expected_blocks: Vec<[String; 5]> =
        EXPECTED_BLOCKS.iter().map(|b| b.map(String::from)).collect();
    let expected_genesis = EXPECTED_GENESIS.map(String::from);
    assert!(
        actual_genesis == expected_genesis && actual_blocks == expected_blocks,
        "golden values differ.\nactual genesis: {actual_genesis:#?}\nactual blocks: {actual_blocks:#?}"
    );

    Ok(())
}
