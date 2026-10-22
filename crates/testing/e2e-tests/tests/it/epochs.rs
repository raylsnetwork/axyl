//! Test the epoch boundary and validator shuffles.

use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use clap::Parser as _;
use e2e_tests::{create_validator_info, retry_until, IT_TEST_MUTEX, TEST_NETWORK};
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
    Address, BlsPublicKey, Bytes, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, Genesis,
    GenesisAccount, RecordBinding, RecordKeys, MIN_RAYLS_PROTOCOL_BASE_FEE, U256,
};
use rayls_network_cli::genesis::GenesisArgs;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    panic,
    path::Path,
    process::Child,
    sync::Arc,
    time::Duration,
};
use tokio::time::{timeout, Instant};
use tracing::{debug, info};

pub(crate) const NEW_VALIDATOR: &str = "new-validator";
pub(crate) const NODE_PASSWORD: &str = "sup3rsecuur";
const INITIAL_STAKE_AMOUNT: &str = "1_000_000";
/// Number of genuine (post-lookahead) epochs to sample before concluding the
/// new validator was never shuffled in. With a 1/6 per-epoch selection chance,
/// `1 - (5/6)^25 >= 0.99`.
const SELECTABLE_TRIALS_TARGET: usize = 25;
// Bumped from 5s: at short cadences a loaded CI runner can miss the epoch-advance
// window, tripping the `loop_epochs` debounce ("Old and new epoch equal"). The few
// seconds of CI jitter that break 5s (its debounce tolerance is only ~11s) are
// roughly absolute, not proportional - so 30s (tolerance ~61s) gives ample margin
// while keeping test_epoch_boundary's shuffle wait under the 60-min job cap.
// Retry-based record checks stay correct regardless of cadence.
pub(crate) const EPOCH_DURATION: u64 = 30;
/// RPC ports of every node: instance `i` serves on `8546 - i`, the new validator (instance 6) on
/// 8540.
pub(crate) const RPC_PORTS: RangeInclusive<u16> = 8540..=8545;
/// Bound for sampling threshold certificates of two consecutive epochs on every node.
const THRESHOLD_SAMPLE_WAIT: Duration = Duration::from_secs(4 * EPOCH_DURATION);

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
    // A mined tx can still have REVERTED - `watch()` only confirms inclusion, not
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
    // activationEpoch so we only count genuine selection trials - and assert it's
    // actually eligible. If the stake/activate tx was dropped (e.g. submitted right
    // as an epoch switched; see the submit loop above), the validator never enters
    // the candidate set and "never shuffled" would be a false negative, not bad luck.
    let new_val_info = consensus_registry.getValidator(new_validator.address()).call().await?;
    assert_eq!(
        new_val_info.validatorAddress,
        new_validator.address(),
        "new validator not registered in ConsensusRegistry - stake/activate tx was dropped"
    );
    assert!(
        new_val_info.activationEpoch > 0,
        "new validator registered but activationEpoch=0 - activate tx was not applied; \
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
        // The first epoch the new validator sat in had its key dealt by a committee it was not
        // in. Find it now, while the registry still keeps the epochs around it: the loop above
        // can skip an epoch, and its two registry reads can straddle an epoch change, so step
        // forward to an epoch that holds the new validator, then back to the first one.
        let joiner = new_validator.address();
        let mut e_join = latest_epoch;
        while !consensus_registry.getEpochInfo(e_join).call().await?.committee.contains(&joiner) {
            e_join += 1;
        }
        while consensus_registry.getEpochInfo(e_join - 1).call().await?.committee.contains(&joiner)
        {
            e_join -= 1;
        }

        // Threshold certificates from epoch 2 (Local schedules the fork at block 0): the current
        // epoch and the next one certify with empty signer bitmaps on every node.
        let from = consensus_registry.getCurrentEpoch().call().await?.max(latest_epoch);
        assert_threshold_certificates(RPC_PORTS, from..=from + 1, THRESHOLD_SAMPLE_WAIT).await?;

        // With t = n - 1 the other members certify without the new validator, so check its share
        // itself.
        assert_new_validator_share(temp_path, e_join).await?;

        // Check that all nodes have valid (certified) Epoch Records. `latest_epoch`
        // is the current, in-progress epoch - its record isn't certified until it
        // concludes - so verify only epochs strictly before it (all concluded).
        for p in 8540..=8545u16 {
            let rpc_url = format!("http://127.0.0.1:{p}");
            let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
            for epoch in 0..latest_epoch {
                wait_for_verified_epoch_record(&provider, p, epoch, Duration::from_secs(60))
                    .await?;
            }
            assert_registry_matches_records(&provider, p).await?;
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
    let now = current_epoch(8545).await?;
    assert_threshold_certificates(RPC_PORTS, now..=now, THRESHOLD_SAMPLE_WAIT).await?;
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

    // The restarted validator computed the threshold keys of the epochs it missed: it holds
    // threshold-certified headers of epochs that started after its restart, like every other
    // node, and its consensus height keeps up with the network.
    let now = current_epoch(8545).await?;
    assert_threshold_certificates(RPC_PORTS, now..=now + 1, THRESHOLD_SAMPLE_WAIT).await?;
    assert_consensus_keeps_up(8543, 8545, Duration::from_secs(2 * EPOCH_DURATION)).await?;

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
        assert_registry_matches_records(&provider, p).await?;
    }

    Ok(())
}

/// The current epoch as the registry of the node on `port` states it.
pub(crate) async fn current_epoch(port: u16) -> eyre::Result<Epoch> {
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    Ok(registry.getCurrentEpoch().call().await?)
}

/// The latest consensus header of the node on `port`, from `rayls_latestHeader`.
pub(crate) async fn latest_header(port: u16) -> eyre::Result<ConsensusHeader> {
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    Ok(provider.raw_request::<_, ConsensusHeader>("rayls_latestHeader".into(), ()).await?)
}

/// Poll `rayls_latestHeader` on every node in `ports` every 500 ms until each returned a header
/// of every epoch in `epochs`, and require every certificate of those headers, leader included,
/// to be a threshold certificate: an empty `signed_authorities` bitmap, where an aggregate
/// certificate names a quorum of signers.
///
/// Fails at the first aggregate certificate, when a node reaches a later epoch before one of
/// `epochs` was sampled on it (only the latest header is served), or after `deadline`.
pub(crate) async fn assert_threshold_certificates(
    ports: RangeInclusive<u16>,
    epochs: RangeInclusive<Epoch>,
    deadline: Duration,
) -> eyre::Result<()> {
    let mut unsampled: BTreeMap<u16, BTreeSet<Epoch>> =
        ports.map(|port| (port, epochs.clone().collect())).collect();
    let started = Instant::now();
    loop {
        unsampled.retain(|_, missing| !missing.is_empty());
        if unsampled.is_empty() {
            return Ok(());
        }
        eyre::ensure!(
            started.elapsed() < deadline,
            "epochs {epochs:?}: no header sampled within {deadline:?} (port: epochs) {unsampled:?}"
        );
        for (port, missing) in unsampled.iter_mut() {
            // a node busy or restarting answers on a later poll; the deadline bounds the wait
            let Ok(header) = latest_header(*port).await else { continue };
            let epoch = header.sub_dag.leader_epoch();
            if epochs.contains(&epoch) {
                let certificates =
                    header.sub_dag.certificates.iter().chain([&header.sub_dag.leader]);
                for cert in certificates {
                    eyre::ensure!(
                        cert.signed_authorities().is_empty(),
                        "node on port {port}: consensus header {} of epoch {epoch} carries an \
                         aggregate certificate (round {}, {} signers)",
                        header.number,
                        cert.round(),
                        cert.signed_authorities().len(),
                    );
                }
                missing.remove(&epoch);
            }
            if let Some(first) = missing.first() {
                eyre::ensure!(
                    *first >= epoch,
                    "node on port {port}: reached epoch {epoch} before a header of epoch {first} \
                     was sampled"
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until the node on `port` holds a consensus header at least as high as the one the node on
/// `reference` held when the wait began.
async fn assert_consensus_keeps_up(
    port: u16,
    reference: u16,
    deadline: Duration,
) -> eyre::Result<()> {
    let target = latest_header(reference).await?.number;
    retry_until(deadline, Duration::from_millis(500), move || async move {
        let number = latest_header(port).await?.number;
        eyre::ensure!(number >= target, "at consensus header {number}");
        Ok(())
    })
    .await
    .map_err(|e| {
        eyre::eyre!(
            "node on port {port} did not reach consensus header {target} of port {reference} \
             within {deadline:?}: {e}"
        )
    })
}

/// Poll `rayls_epochRecord(epoch)` on one node until it returns a record, then
/// verify it against its certificate.
///
/// A node that is still catching up returns `NotFound` (401) for a not-yet-synced
/// epoch, so a transient error is retried up to `deadline` - this is what makes
/// the completeness check tolerant of a legitimately-lagging node. A record that
/// IS returned but fails cert verification is a real correctness bug and fails
/// immediately (no retry). A node that never produces the record fails once the
/// deadline is exceeded, with a message naming the node and epoch.
pub(crate) async fn wait_for_verified_epoch_record<P: Provider>(
    provider: &P,
    port: u16,
    epoch: u32,
    deadline: Duration,
) -> eyre::Result<EpochRecord> {
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

    // what the certificate signs besides the record, and the epoch's group key for a threshold
    // certificate, as the node knows them
    let binding: RecordBinding = provider
        .raw_request("rayls_epochRecordBinding".into(), (epoch,))
        .await
        .map_err(|e| eyre::eyre!("node on port {port}: epoch {epoch} record binding: {e}"))?;
    let group_key: Option<BlsPublicKey> = if cert.signed_authorities.is_empty() {
        provider
            .raw_request("rayls_epochGroupKey".into(), (epoch,))
            .await
            .map_err(|e| eyre::eyre!("node on port {port}: epoch {epoch} group key: {e}"))?
    } else {
        None
    };
    eyre::ensure!(
        record.verify_with_cert(&cert, &RecordKeys { binding, group_key }),
        "node on port {port}: epoch {epoch} record present but failed cert verification: {}/{} {}",
        record.epoch,
        record.digest(),
        cert.epoch_hash
    );
    Ok(record)
}

/// The threshold dealing gate reads an epoch's first block from the registry
/// (`getEpochInfo(epoch).blockHeight`), or, once the registry no longer keeps the epoch, from the
/// previous epoch's record (one past its closing block). Check that the two agree on the node on
/// `port`, for its current epoch: the registry keeps it for three more epochs, longer than the
/// record wait, and a later epoch has no start block yet.
async fn assert_registry_matches_records<P: Provider>(provider: &P, port: u16) -> eyre::Result<()> {
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);
    let epoch = registry.getCurrentEpoch().call().await?;
    eyre::ensure!(epoch > 0, "node on port {port} is still in epoch 0");
    let started = registry.getEpochInfo(epoch).call().await?.blockHeight;
    let previous =
        wait_for_verified_epoch_record(provider, port, epoch - 1, Duration::from_secs(60)).await?;
    eyre::ensure!(
        started == previous.parent_state.number + 1,
        "node on port {port}: epoch {epoch} starts at block {started} in the registry, but the \
         record of epoch {} ends at block {}",
        epoch - 1,
        previous.parent_state.number
    );
    Ok(())
}

/// Wait until the new validator logged that `epoch` uses threshold certificates with its share,
/// and fail at once if it logged the epoch without one. The new validator writes its own log
/// files (see `start_nodes`).
async fn assert_new_validator_share(temp_path: &Path, epoch: Epoch) -> eyre::Result<()> {
    let dir = temp_path.join(NEW_VALIDATOR).join("logs").join("rayls-network-logs");
    let line = |has_share: bool| {
        format!("epoch uses threshold certificates epoch={epoch} has_share={has_share}")
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // reth rotates reth.log into reth.log.N; a file renamed mid-read is read next time
        let mut text = String::new();
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            if entry.file_name().to_string_lossy().starts_with("reth.log") {
                text.push_str(&String::from_utf8_lossy(
                    &std::fs::read(entry.path()).unwrap_or_default(),
                ));
            }
        }
        eyre::ensure!(
            !text.contains(&line(false)),
            "the new validator has no share of epoch {epoch}'s threshold key"
        );
        if text.contains(&line(true)) {
            return Ok(());
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "no threshold scheme line for epoch {epoch} in {}",
            dir.display()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
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
        false,
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
        false,
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
pub(crate) fn create_genesis_for_test(
    temp_path: &Path,
    new_validator: Address,
    governance_wallet: Address,
    committee: &Vec<(&str, Address)>,
    genesis_key: bool,
) -> eyre::Result<Genesis> {
    // use same passphrase for all nodes
    let passphrase = NODE_PASSWORD.to_string();

    // create validator info for "new" validator to join
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    create_validator_info(&new_validator_path, &new_validator.to_string(), passphrase.clone())?;

    // the joiner is not a genesis validator, so genesis does not fund its DKG submitter: fund it
    // here, as an operator would
    let new_info: NodeInfo =
        Config::load_from_path(&new_validator_path.join("node-info.yaml"), ConfigFmt::YAML)?;

    // fund governance to issue NFT and new validator to stake
    let accounts = vec![
        (
            new_info.dkg_submitter,
            GenesisAccount::default().with_balance(U256::from(parse_ether("100")?)),
        ),
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
        genesis_key,
    )?;

    // copy genesis for new validator
    std::fs::create_dir_all(new_validator_path.join("genesis"))?;
    if genesis_key {
        std::fs::copy(
            shared_genesis_dir.join("genesis/dkg-dealings.yaml"),
            new_validator_path.join("genesis/dkg-dealings.yaml"),
        )?;
    }
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
/// With `genesis_key`, the validators also run the second ceremony round and genesis gives epoch
/// 0 a threshold key.
pub(crate) fn config_committee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: String,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &Vec<(&str, Address)>,
    genesis_key: bool,
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

    // create committee from shared genesis dir. The chain-id must match the `--network`
    // flag the nodes are started with (see start_nodes / TEST_NETWORK).
    let chain_id = TEST_NETWORK.chain_id().to_string();
    let dealings_dir = shared_genesis_dir.join("dkg-dealings");
    if genesis_key {
        std::fs::create_dir_all(&dealings_dir)?;
        for (v, _) in validators.iter() {
            e2e_tests::create_genesis_dealing(
                &temp_path.join(v),
                &copy_path,
                TEST_NETWORK.chain_id(),
                &dealings_dir.join(format!("{v}.yaml")),
                passphrase.clone(),
            )?;
        }
    }
    let dealings_arg = dealings_dir.to_string_lossy().into_owned();
    let owner_arg = consensus_registry_owner.to_string();
    let duration_arg = EPOCH_DURATION.to_string();
    let mut ceremony_args = vec![
        "rl",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        &owner_arg,
        "--initial-stake-per-validator",
        INITIAL_STAKE_AMOUNT,
        "--min-withdraw-amount",
        min_withdrawal,
        "--epoch-duration-in-secs",
        &duration_arg,
        "--dev-funded-account",
        "test-source",
        "--rls-accounts",
        rls_accounts_str,
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
        "--chain-id",
        &chain_id,
    ];
    if genesis_key {
        ceremony_args.extend(["--dkg-dealings", &dealings_arg]);
    }
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from(ceremony_args);
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
        if genesis_key {
            std::fs::copy(
                shared_genesis_dir.join("genesis/dkg-dealings.yaml"),
                dir.join("genesis/dkg-dealings.yaml"),
            )?;
        }
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
            .arg("--storage.v2")
            // Use the Local hardfork schedule so HybridRewards activates at block 1: genesis
            // deploys the pre-hybrid ConsensusRegistry and the in-place migration swaps it to the
            // hybrid contract at the first block, exercising the full rollout end-to-end.
            // TEST_NETWORK's chain-id must equal the ceremony's `--chain-id` above.
            .arg("--network")
            .arg(TEST_NETWORK.to_string());
        // The new validator keeps its own log files, which `assert_new_validator_share` reads.
        if instance == "6" {
            command.arg("--log.file.directory").arg(dir.join("logs"));
        }

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
pub(crate) struct OnboardTx {
    pub(crate) label: &'static str,
    pub(crate) from: Address,
    pub(crate) to: Address,
    pub(crate) calldata: Vec<u8>,
    pub(crate) raw: Bytes,
}

/// Generate all the transactions needed to onboard the new validator so it becomes
/// eligible for committee selection.
///
/// `ConsensusRegistry.stake()` requires the validator to be (1) allowlisted by the
/// registry owner and (2) to have approved the registry to pull its ERC-20 RLS - and
/// it is NOT payable (funds move via `transferFrom`, not `msg.value`). So the full
/// ordered flow is: governance allowlists → validator approves RLS → validator stakes
/// (value 0) → validator activates. The validator is already RLS-funded at genesis,
/// so no separate transfer is needed.
pub(crate) fn generate_new_validator_txs(
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

    // 3. Stake - value 0; the registry pulls RLS via transferFrom (non-payable fn).
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

    // 4. Activate - request entry into a future committee.
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
