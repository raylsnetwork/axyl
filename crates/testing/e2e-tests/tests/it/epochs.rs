//! Test the epoch boundary and validator shuffles.

use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use clap::Parser as _;
use e2e_tests::{create_validator_info, retry_until, IT_TEST_MUTEX};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_execution_evm::{
    system_calls::{ConsensusRegistry, RLSToken, CONSENSUS_REGISTRY_ADDRESS, RLS_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use rayls_infrastructure_types::{
    test_utils::{self, CommandParser},
    Address, EpochCertificate, EpochRecord, Genesis, GenesisAccount, MIN_RAYLS_PROTOCOL_BASE_FEE,
    U256,
};
use rayls_network_cli::genesis::GenesisArgs;
use std::{collections::BTreeMap, panic, path::Path, process::Child, sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::{debug, info};

const NEW_VALIDATOR: &str = "new-validator";
const NODE_PASSWORD: &str = "sup3rsecuur";
const INITIAL_STAKE_AMOUNT: &str = "1_000_000";
/// Number of genuine (post-lookahead) epochs to sample before concluding the
/// new validator was never shuffled in. With a 1/6 per-epoch selection chance,
/// `1 - (5/6)^25 >= 0.99`.
const SELECTABLE_TRIALS_TARGET: usize = 25;
// Bumped from 5s: at short cadences a loaded CI runner can miss the epoch-advance
// window, tripping the `loop_epochs` debounce ("Old and new epoch equal"). The few
// seconds of CI jitter that break 5s (its debounce tolerance is only ~11s) are
// roughly absolute, not proportional — so 30s (tolerance ~61s) gives ample margin
// while keeping test_epoch_boundary's shuffle wait under the 60-min job cap.
// Retry-based record checks stay correct regardless of cadence.
const EPOCH_DURATION: u64 = 30;

async fn test_epoch_boundary_inner(
    genesis: Genesis,
    mut governance_wallet: TransactionFactory,
    temp_path: &Path,
    new_validator: &mut TransactionFactory,
) -> eyre::Result<()> {
    // create transactions to make new validator eligible for future epochs
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let txs = generate_new_validator_txs(temp_path, chain, new_validator, &mut governance_wallet)?;

    // create rpc client for node1 default rpc address
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // submit txs to stake and activate the new validator.
    //
    // A mined tx can still have REVERTED — `watch()` only confirms inclusion, not
    // success. So fetch each receipt and assert it succeeded, naming which tx failed.
    // Without this, a reverted stake/activate silently leaves the validator
    // unregistered and only surfaces much later as a misleading "never shuffled".
    for otx in &txs {
        let pending = provider.send_raw_transaction(&otx.raw).await?;
        debug!(target: "epoch-test", "pending {} tx: {:?}", otx.label, pending.tx_hash());
        let receipt = timeout(Duration::from_secs(15), pending.get_receipt()).await??;
        if !receipt.status() {
            // The receipt only tells us it reverted, not why. Replay as an eth_call
            // from the same sender to surface the actual revert reason.
            let call = serde_json::json!({
                "from": otx.from,
                "to": otx.to,
                "data": format!("0x{}", const_hex::encode(&otx.calldata)),
                "value": "0x0",
            });
            let replay = provider
                .raw_request::<_, serde_json::Value>("eth_call".into(), (call, "latest"))
                .await;
            panic!(
                "{} tx {:?} was included but REVERTED (gas_used={}); eth_call replay: {:?}",
                otx.label, receipt.transaction_hash, receipt.gas_used, replay,
            );
        }
        info!(
            target: "epoch-test",
            "{} tx {:?} succeeded (gas_used={})",
            otx.label, receipt.transaction_hash, receipt.gas_used,
        );
    }

    // retrieve current committee
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_epoch_block_height = current_epoch_info.blockHeight;

    // The new validator can't appear in a *current* committee right away: it goes
    // Active at `activationEpoch` and committees are selected ~2 epochs ahead (see
    // reth_env: "read new committee (always 2 epochs ahead)"). Read its
    // activationEpoch so we only count genuine selection trials — and assert it's
    // actually eligible. If the stake/activate tx was dropped (e.g. submitted right
    // as an epoch switched; see the submit loop above), the validator never enters
    // the candidate set and "never shuffled" would be a false negative, not bad luck.
    let new_val_info = consensus_registry.getValidator(new_validator.address()).call().await?;
    assert_eq!(
        new_val_info.validatorAddress,
        new_validator.address(),
        "new validator not registered in ConsensusRegistry — stake/activate tx was dropped"
    );
    assert!(
        new_val_info.activationEpoch > 0,
        "new validator registered but activationEpoch=0 — activate tx was not applied; \
         it can never be shuffled into a committee"
    );
    // Earliest epoch at which it can show up in the *current* committee.
    let selectable_from = new_val_info.activationEpoch + 2;
    info!(
        target: "epoch-test",
        activation_epoch = new_val_info.activationEpoch,
        selectable_from,
        "new validator eligible; counting selection trials from epoch {selectable_from}"
    );

    // sleep for first epoch with 1s offset and begin assertions loop
    tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;

    let mut last_pause: usize = 100;
    let mut shuffled = false;
    // Count only genuine selection trials: epochs at/after `selectable_from`. Earlier
    // epochs are structural misses (activation + 2-epoch lookahead) and must not be
    // counted, or the 1/6-per-epoch probability model is violated.
    let mut selectable_epochs_seen: usize = 0;
    let mut latest_epoch = 0u32;

    // Budget = lookahead lead-in epochs + the post-lookahead trial target, with
    // slack. Bounded so a stalled chain can't loop forever.
    for i in 0..(SELECTABLE_TRIALS_TARGET + 10) {
        let current_epoch = consensus_registry.getCurrentEpoch().call().await?;
        let new_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;
        if new_epoch_info == current_epoch_info && last_pause != i {
            tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;
            last_pause = i + 1;
            continue;
        }
        last_pause = i;
        assert!(new_epoch_info != current_epoch_info, "Old and new epoch equal on iteration {i}");
        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, EPOCH_DURATION);
        latest_epoch = current_epoch;

        // Only epochs at/after the lookahead window are real 1/6 selection trials.
        if current_epoch >= selectable_from {
            selectable_epochs_seen += 1;
            if new_epoch_info.committee.contains(&new_validator.address()) {
                shuffled = true;
                break;
            }
            // Gave it a full budget of genuine trials without selection.
            if selectable_epochs_seen >= SELECTABLE_TRIALS_TARGET {
                break;
            }
        }

        // store the last seen epoch info that is expected to change every epoch
        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;

        // sleep for epoch duration
        tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION)).await;
    }

    if shuffled {
        // Check that all nodes have valid (certified) Epoch Records. `latest_epoch`
        // is the current, in-progress epoch — its record isn't certified until it
        // concludes — so verify only epochs strictly before it (all concluded).
        for p in 8540..=8545u16 {
            let rpc_url = format!("http://127.0.0.1:{p}");
            let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
            for epoch in 0..latest_epoch {
                wait_for_verified_epoch_record(&provider, p, epoch, Duration::from_secs(60))
                    .await?;
            }
        }

        Ok(())
    } else {
        // return error if loop didn't return
        Err(eyre::eyre!("new validator not shuffled into committee!"))
    }
}

async fn loop_epochs(start: u32, iterations: u32) -> eyre::Result<()> {
    // create rpc client for node1 default rpc address
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    // retrieve current committee
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_pause = 100;
    let mut last_epoch_block_height = current_epoch_info.blockHeight;
    for i in start..start + iterations {
        let new_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;
        if new_epoch_info == current_epoch_info && last_pause != i {
            tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;
            last_pause = i + 1;
            continue;
        }
        last_pause = i;
        assert!(new_epoch_info != current_epoch_info, "Old and new epoch equal on iteration {i}");
        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, EPOCH_DURATION);

        // store the last seen epoch info that is expected to change every epoch
        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;

        // sleep for epoch duration
        tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION)).await;
    }
    Ok(())
}

async fn test_epoch_sync_inner(
    child: Arc<std::sync::Mutex<Child>>,
    nodes_to_start: &[(&str, Address)],
    temp_path: &Path,
) -> eyre::Result<()> {
    // create rpc client for node1 default rpc address
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // sleep for first epoch with 1s offset and begin assertions loop
    tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;

    // Go through at least 5 epochs.
    loop_epochs(0, 5).await?;
    // Kill a node
    send_term(&mut *child.lock().unwrap());
    let _ = child.lock().unwrap().wait();

    // Make sure the node really is down.
    let rpc_url = format!("http://127.0.0.1:8543");
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    assert!(provider.get_chain_id().await.is_err(), "Node not down!");

    loop_epochs(5, 5).await?;
    // Restart the node
    let new_child = start_nodes(temp_path, nodes_to_start)?.pop().expect("child");
    *child.lock().expect("poison") = new_child;
    loop_epochs(10, 5).await?;

    // Do a check to make sure all the nodes have valid (certified) Epoch Records.
    // The node that was down should also have all these records after syncing.
    // Verify only concluded epochs: the current (in-progress) epoch has no certified
    // record yet, so `rayls_epochRecord` would return NotFound (401) for it. The
    // just-restarted node is still catching up, so poll (bounded) rather than
    // requiring every record to be present on the first try.
    let latest_epoch: u32 = 15;
    for p in 8540..=8545u16 {
        let rpc_url = format!("http://127.0.0.1:{p}");
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        for epoch in 0..latest_epoch {
            wait_for_verified_epoch_record(&provider, p, epoch, Duration::from_secs(60)).await?;
        }
    }

    Ok(())
}

/// Poll `rayls_epochRecord(epoch)` on one node until it returns a record, then
/// verify it against its certificate.
///
/// A node that is still catching up returns `NotFound` (401) for a not-yet-synced
/// epoch, so a transient error is retried up to `deadline` — this is what makes
/// the completeness check tolerant of a legitimately-lagging node. A record that
/// IS returned but fails cert verification is a real correctness bug and fails
/// immediately (no retry). A node that never produces the record fails once the
/// deadline is exceeded, with a message naming the node and epoch.
async fn wait_for_verified_epoch_record<P: Provider>(
    provider: &P,
    port: u16,
    epoch: u32,
    deadline: Duration,
) -> eyre::Result<()> {
    let (record, cert): (EpochRecord, EpochCertificate) =
        retry_until(deadline, Duration::from_secs(1), move || async move {
            provider
                .raw_request::<_, (EpochRecord, EpochCertificate)>(
                    "rayls_epochRecord".into(),
                    (epoch,),
                )
                .await
        })
        .await
        .map_err(|e| {
            eyre::eyre!(
                "node on port {port}: epoch {epoch} record unavailable after {deadline:?}: {e}"
            )
        })?;

    eyre::ensure!(
        record.verify_with_cert(&cert),
        "node on port {port}: epoch {epoch} record present but failed cert verification: {}/{} {}",
        record.epoch,
        record.digest(),
        cert.epoch_hash
    );
    Ok(())
}

fn kill_procs(procs: &Vec<Arc<std::sync::Mutex<Child>>>) {
    // We need to capture the result above and then kill all the procs.
    for proc in procs.iter() {
        let _ = proc.lock().unwrap().kill();
    }
    for proc in procs {
        let _ = proc.lock().unwrap().wait();
    }
}

/// Send SIGTERM to child, can use this to pre-send TERM to all children when shutting down.
fn send_term(child: &mut Child) {
    if let Err(e) = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM) {
        tracing::error!(target: "restart-test", ?e, "error killing child");
    }
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test a new node joining the network and being shuffled into the committee.
async fn test_epoch_boundary() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    // create validator and governance wallets for adding new validator later
    let mut new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("epoch_boundary")?;
    let temp_path = temp_dir.path();

    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let genesis = create_genesis_for_test(
        temp_path,
        new_validator.address(),
        governance_wallet.address(),
        &committee,
    )?;

    // start nodes (committee + new validator)
    committee.push((NEW_VALIDATOR, new_validator.address()));
    let procs = start_nodes(temp_path, &committee)?;
    let procs: Vec<Arc<std::sync::Mutex<Child>>> =
        procs.into_iter().map(|c| Arc::new(std::sync::Mutex::new(c))).collect();
    let procs_clone = procs.clone();
    // Use a panic hook to make sure we kill the node procs on a panic (assert failure).
    let org_panic = panic::take_hook();
    panic::set_hook(Box::new(move |a| {
        kill_procs(&procs_clone);
        org_panic(a);
    }));

    let r =
        test_epoch_boundary_inner(genesis, governance_wallet, temp_path, &mut new_validator).await;
    kill_procs(&procs);
    r
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test that sync works to fill in missing epochs.
async fn test_epoch_sync() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    // create validator and governance wallets for adding new validator later
    let new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("epoch_sync")?;
    let temp_path = temp_dir.path();

    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let _genesis = create_genesis_for_test(
        temp_path,
        new_validator.address(),
        governance_wallet.address(),
        &committee,
    )?;

    // start nodes (committee + new validator)
    committee.push((NEW_VALIDATOR, new_validator.address()));
    let procs = start_nodes(temp_path, &committee)?;
    let procs: Vec<Arc<std::sync::Mutex<Child>>> =
        procs.into_iter().map(|c| Arc::new(std::sync::Mutex::new(c))).collect();
    let procs_clone = procs.clone();
    // Use a panic hook to make sure we kill the node procs on a panic (assert failure).
    let org_panic = panic::take_hook();
    panic::set_hook(Box::new(move |a| {
        kill_procs(&procs_clone);
        org_panic(a);
    }));

    let r = test_epoch_sync_inner(
        procs[2].clone(),
        &[("validator-3", Address::from_slice(&[0x33; 20]))],
        temp_path,
    )
    .await;
    kill_procs(&procs);
    r
}

/// Create genesis for this test.
///
/// Funds a new validator and the governance wallet to issue NFTs.
/// This method also configures the initial committee to start the network.
fn create_genesis_for_test(
    temp_path: &Path,
    new_validator: Address,
    governance_wallet: Address,
    committee: &Vec<(&str, Address)>,
) -> eyre::Result<Genesis> {
    // use same passphrase for all nodes
    let passphrase = NODE_PASSWORD.to_string();

    // create validator info for "new" validator to join
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    create_validator_info(&new_validator_path, &new_validator.to_string(), passphrase.clone())?;

    // fund governance to issue NFT and new validator to stake
    let accounts = vec![
        (
            governance_wallet,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), /* 50mil RLS */
        ),
        (
            new_validator,
            GenesisAccount::default().with_balance(U256::from(parse_ether("2_000_000")?)), /* double stake */
        ),
    ];

    let shared_genesis_dir = temp_path.join("shared-genesis");

    // create the initial committee of validators and create genesis
    let genesis = config_committee(
        temp_path,
        &shared_genesis_dir,
        passphrase,
        governance_wallet,
        accounts,
        committee,
    )?;

    // copy genesis for new validator
    std::fs::create_dir_all(new_validator_path.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        new_validator_path.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        new_validator_path.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("parameters.yaml"),
        new_validator_path.join("parameters.yaml"),
    )?;

    Ok(genesis)
}

/// Configure the initial committee and fund accounts for network genesis.
///
/// All data is written to file.
fn config_committee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: String,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &Vec<(&str, Address)>,
) -> eyre::Result<Genesis> {
    // create shared genesis dir
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;
    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info(&dir, &addr.to_string(), passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // configuration for ConsensusRegistry to pass through CLI
    let min_withdrawal = "1_000";

    info!(target: "epoch-test", "creating committee!");

    // Pre-fund these accounts with ERC-20 RLS. This is SEPARATE from the native
    // balance set via `extend_accounts` below (which only covers gas):
    // `ConsensusRegistry.stake()` pulls the stake via `RLS.transferFrom`, so the new
    // validator must actually hold RLS tokens or stake reverts with
    // ERC20InsufficientBalance. `--rls-accounts` mints these RLS balances at genesis.
    let rls_accounts: BTreeMap<Address, GenesisAccount> = accounts.iter().cloned().collect();
    let rls_accounts_path = shared_genesis_dir.join("rls-accounts.yaml");
    Config::write_to_path(&rls_accounts_path, &rls_accounts, ConfigFmt::YAML)?;
    let rls_accounts_str = rls_accounts_path.to_str().expect("rls-accounts path is valid utf8");

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "rl",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        &consensus_registry_owner.to_string(),
        "--initial-stake-per-validator",
        INITIAL_STAKE_AMOUNT,
        "--min-withdraw-amount",
        min_withdrawal,
        "--epoch-duration-in-secs",
        &EPOCH_DURATION.to_string(),
        "--dev-funded-account",
        "test-source",
        "--rls-accounts",
        rls_accounts_str,
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
    create_committee_command.args.execute(shared_genesis_dir.to_path_buf())?;

    // update genesis with funded accounts
    let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
    let genesis = genesis.extend_accounts(accounts);
    Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;

    // distribute updated genesis to all validators
    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    Ok(genesis)
}

/// Start the network using the node cli command.
fn start_nodes(temp_path: &Path, validators: &[(&str, Address)]) -> eyre::Result<Vec<Child>> {
    let bin = e2e_tests::get_rayls_network_binary();

    let mut children = Vec::new();
    for (v, _) in validators.iter() {
        let dir = temp_path.join(v);
        let mut instance = v.chars().last().expect("validator instance").to_string();

        // assign instance for "new-validator"
        if instance == "r" {
            instance = "6".to_string();
            info!(target: "epoch-test", ?v, "starting new validator");
        }

        let mut command = bin.command();

        command
            .env("RL_BLS_PASSPHRASE", NODE_PASSWORD)
            .arg("--bls-passphrase-source")
            .arg("env")
            .arg("node")
            .arg("--datadir")
            .arg(&*dir.to_string_lossy())
            .arg("--instance")
            .arg(&instance)
            .arg("--http")
            // v1 (plain) storage is no longer supported -- the node refuses to start without this
            .arg("--storage.v2");

        #[cfg(feature = "faucet")]
        command
            .arg("--public-key") // If the binary is built with the faucet need this to start...
            .arg("0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99")
            .arg("--google-kms")
            .arg("--faucet-contract")
            .arg("0x0000000000000000000000000000000000000000");

        children.push(command.spawn().expect("failed to execute"));
    }

    Ok(children)
}

/// A pre-signed onboarding transaction plus the pieces needed to replay it as an
/// `eth_call` (to surface a revert reason) if it reverts on-chain.
struct OnboardTx {
    label: &'static str,
    from: Address,
    to: Address,
    calldata: Vec<u8>,
    raw: Vec<u8>,
}

/// Generate all the transactions needed to onboard the new validator so it becomes
/// eligible for committee selection.
///
/// `ConsensusRegistry.stake()` requires the validator to be (1) allowlisted by the
/// registry owner and (2) to have approved the registry to pull its ERC-20 RLS — and
/// it is NOT payable (funds move via `transferFrom`, not `msg.value`). So the full
/// ordered flow is: governance allowlists → validator approves RLS → validator stakes
/// (value 0) → validator activates. The validator is already RLS-funded at genesis,
/// so no separate transfer is needed.
fn generate_new_validator_txs(
    temp_path: &Path,
    chain: Arc<RethChainSpec>,
    new_validator: &mut TransactionFactory,
    governance_wallet: &mut TransactionFactory,
) -> eyre::Result<Vec<OnboardTx>> {
    // read bls public key from fs for new validator
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    let new_validator_info = Config::load_from_path_or_default::<NodeInfo>(
        new_validator_path.join("node-info.yaml").as_path(),
        ConfigFmt::YAML,
    )?;

    let stake_amount = parse_ether(INITIAL_STAKE_AMOUNT)?;
    let new_validator_addr = new_validator.address();
    let governance_addr = governance_wallet.address();

    // 1. Governance (the registry owner) allowlists the new validator. Without this, `stake()`
    //    reverts with NotAllowlisted before doing anything else.
    let allowlist_calldata =
        ConsensusRegistry::allowlistValidatorCall { validatorAddress: new_validator_addr }
            .abi_encode();
    let allowlist_raw = governance_wallet.create_eip1559_encoded(
        chain.clone(),
        None,
        MIN_RAYLS_PROTOCOL_BASE_FEE as u128,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        allowlist_calldata.clone().into(),
    );

    // 2. The new validator approves the ConsensusRegistry to pull its RLS stake. `stake()` does
    //    `RLS.transferFrom(msg.sender, registry, stakeAmount)`.
    let approve_calldata =
        RLSToken::approveCall { spender: CONSENSUS_REGISTRY_ADDRESS, amount: stake_amount }
            .abi_encode();
    let approve_raw = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        MIN_RAYLS_PROTOCOL_BASE_FEE as u128,
        Some(RLS_ADDRESS),
        U256::ZERO,
        approve_calldata.clone().into(),
    );

    // 3. Stake — value 0; the registry pulls RLS via transferFrom (non-payable fn).
    let proof = ConsensusRegistry::ProofOfPossession {
        uncompressedPubkey: new_validator_info.bls_public_key.serialize().into(),
        uncompressedSignature: new_validator_info.proof_of_possession.serialize().into(),
    };
    let stake_calldata = ConsensusRegistry::stakeCall {
        blsPubkey: new_validator_info.bls_public_key.compress().into(),
        proofOfPossession: proof,
    }
    .abi_encode();
    let stake_raw = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        MIN_RAYLS_PROTOCOL_BASE_FEE as u128,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        stake_calldata.clone().into(),
    );

    // 4. Activate — request entry into a future committee.
    let activate_calldata = ConsensusRegistry::activateCall {}.abi_encode();
    let activate_raw = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        MIN_RAYLS_PROTOCOL_BASE_FEE as u128,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        activate_calldata.clone().into(),
    );

    Ok(vec![
        OnboardTx {
            label: "allowlist",
            from: governance_addr,
            to: CONSENSUS_REGISTRY_ADDRESS,
            calldata: allowlist_calldata,
            raw: allowlist_raw,
        },
        OnboardTx {
            label: "approve",
            from: new_validator_addr,
            to: RLS_ADDRESS,
            calldata: approve_calldata,
            raw: approve_raw,
        },
        OnboardTx {
            label: "stake",
            from: new_validator_addr,
            to: CONSENSUS_REGISTRY_ADDRESS,
            calldata: stake_calldata,
            raw: stake_raw,
        },
        OnboardTx {
            label: "activate",
            from: new_validator_addr,
            to: CONSENSUS_REGISTRY_ADDRESS,
            calldata: activate_calldata,
            raw: activate_raw,
        },
    ])
}
