//! Node IT tests

// unused deps lint confusion
#![allow(unused_crate_dependencies)]

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    test_utils::temp_dir,
    ConsensusBus,
};
use rayls_consensus_state_sync::prime_consensus;
use rayls_execution_evm::{test_utils::seeded_genesis_from_random_batches, RethChainSpec};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, testnet_genesis, Batch, CommittedSubDag, ConsensusHeader,
    ConsensusOutput, Database, DbTxMut, ExecHeader, Notifier, RaylsReceiver as _, RaylsSender as _,
    ReputationScores, SealedHeader, TaskManager, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_middleware_orchestrator::epoch_manager::catchup_accumulator;
use rayls_middleware_processor::{batch::BatchOrdering, ExecutorEngine};
use rayls_testing_test_utils::{
    create_signed_certificates_for_rounds, default_test_execution_node, CommitteeFixture,
};
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::debug;

#[tokio::test]
async fn test_catchup_accumulator() -> eyre::Result<()> {
    rayls_infrastructure_types::test_utils::init_test_tracing();
    let tmp = temp_dir();
    // create deterministic committee fixture and use first authority's components
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(8991))
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let consensus_bus = ConsensusBus::new();

    // make certificates for rounds 1 to 7 with batches of txs
    let max_round = 21;
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=max_round, &fixture);

    // fund accounts in genesis so txs execute
    let genesis = testnet_genesis();
    let all_batches: Vec<_> = batches.values().cloned().collect();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution env
    let gas_accumulator = GasAccumulator::new(1);
    gas_accumulator.rewards_counter().set_committee(fixture.committee());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp.path().join("reth"),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    // manually create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(10);
    let max = Some(max_round as u64 - 1); // consensus needs 1 extra round to commit
    let parent = chain.sealed_genesis_header();

    // start engine
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store.clone());
    let engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
    );
    let (tx, mut rx) = oneshot::channel();
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        debug!(target: "gas-test", ?res, "res:");
        let _ = tx.send(res);
    });

    // subscribe to output early
    let mut consensus_output = consensus_bus.consensus_output().subscribe();

    // spawn consensus to send output to engine for full execution
    spawn_consensus(
        &fixture,
        &consensus_bus,
        batches,
        config,
        consensus_store.clone(),
        &task_manager,
        to_engine.clone(),
    );

    // send certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // simulate epoch manager's role:
    // forward consensus output to engine until `max_round`
    let mut rewards = HashMap::new();
    loop {
        tokio::select! {
            // forward output from consensus to engine
            Some(output) = consensus_output.recv() => {
                debug!(target: "gas-test", output=?output.leader(), round=output.leader().round(), "received output");
                let leader = output.leader().origin().clone();
                // manually track values as well
                rewards.entry(leader).and_modify(|count| *count += 1).or_insert(1);
                to_engine.send((rayls_infrastructure_types::CameFrom::Test, output)).await?;
            }
            // wait for engine to reach `max_round` or timeout
            engine_task = timeout(Duration::from_secs(15), &mut rx) => {
                // engine shutdown
                assert!(engine_task.is_ok());
                break;
            }
        }
    }

    // check results
    debug!(target: "gas-test", "gas accumulator:\n{:#?}", gas_accumulator);
    let worker_id = 0;
    // initialize a new gas accumulator to simulate node recovery
    let recovered = GasAccumulator::new(1);
    recovered.rewards_counter().set_committee(fixture.committee());
    catchup_accumulator(&consensus_store, reth_env.clone(), &recovered)?;
    // assert recovered and active track the same expected values
    //      G48pDy85GhyGMp9afPBvWgaNzgPAnvBtMxjReQTe1NiN: 3,
    //      Agv7rsffEbxoa7ybTJj57TiAHchf27ia7ziB5CVrHNTk: 3,
    //      73HL4cMSiCfGthUE7xM1F8JwwYfmM53wQi4r34ECrs3F: 3,
    //      2VDmuopDmr9KZcp4z9q9ne2CAxkaF2ftMt6ejzp42FM7: 1,
    debug!(target: "gas-test", "recovered accumulator:\n{:#?}", recovered);
    assert_eq!(gas_accumulator.get_values(worker_id), (231, 9702000));
    assert_eq!(gas_accumulator.get_values(worker_id), recovered.get_values(worker_id));

    // convert manually calculated rewards for assertion
    let expected: BTreeMap<_, _> = rewards
        .iter()
        .map(|(auth, count)| {
            (fixture.authority_by_id(auth).expect("in committee").execution_address(), *count)
        })
        .collect();

    // assert rewards
    assert_eq!(expected, gas_accumulator.rewards_counter().get_address_counts());
    assert_eq!(expected, recovered.rewards_counter().get_address_counts());

    Ok(())
}

/// Helper to spawn consensus components.
fn spawn_consensus(
    fixture: &CommitteeFixture<MemDatabase>,
    consensus_bus: &ConsensusBus,
    batches: HashMap<B256, Batch>,
    config: ConsensusConfig<MemDatabase>,
    consensus_store: MemDatabase,
    task_manager: &TaskManager,
    to_engine: mpsc::Sender<(rayls_infrastructure_types::CameFrom, ConsensusOutput)>,
) {
    // components for tasks
    let committee = fixture.committee();
    let rx_shutdown = config.shutdown().subscribe();

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // spawn the executor
    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // Set up mock worker.
    let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
    config.local_network().set_primary_to_worker_local_handler(mock_client);

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        Arc::new(Default::default()),
        3,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    // spawn consensus to await certificates
    // genesis-shaped parent: parent_beacon_block_root = Some(ZERO).
    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(B256::ZERO);
    let dummy_parent = SealedHeader::new(exec_header, B256::default());
    consensus_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    Consensus::spawn(config, consensus_bus, bullshark, task_manager);
}

/// Verifies prime_consensus() recovers committed_round from the executed_anchor SSOT after restart.
///
/// The watermark is the consensus header the highest executed block commits to, seeded once at
/// boot. Without proper recovery, max_round = gc_depth causing "TooNew" header rejections.
#[tokio::test]
async fn test_prime_consensus_recovers_committed_round_after_restart() -> eyre::Result<()> {
    rayls_infrastructure_types::test_utils::init_test_tracing();

    // Setup: Create a committee fixture with in-memory database
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(12345))
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let gc_depth = config.parameters().gc_depth;

    // Create certificates for rounds 1 to 100 (simulating a node that ran for a while)
    let target_round = 100u32;
    let (certificates, _next_parents, _batches) =
        create_signed_certificates_for_rounds(1..=target_round, &fixture);

    // Get the last certificate from round 100 to use as the leader certificate
    // In a real scenario, this would be the leader certificate from the committed subdag
    let last_cert = certificates.back().unwrap().clone();
    debug!(
        target: "test",
        "Created certificate at round {} from authority {:?}",
        last_cert.round(),
        last_cert.origin()
    );

    // Create a ConsensusHeader with the certificate at round 100
    // This simulates the state of a node that processed consensus up to round 100
    let sub_dag = CommittedSubDag::new(
        vec![], // certificates in the subdag (simplified for test)
        last_cert.clone(),
        0, // commit_timestamp
        ReputationScores::default(),
        None,
    );

    let consensus_header = ConsensusHeader {
        parent_hash: B256::default(),
        sub_dag,
        number: target_round as u64, // Block number = round (simplified)
        extra: B256::default(),
    };

    // Write the ConsensusHeader to the database (simulating node state before restart)
    let consensus_digest = consensus_header.digest();

    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&consensus_header.number, &consensus_header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&consensus_digest, &consensus_header.number)?;
        Ok(())
    })
    .expect("Failed to write consensus header to DB");

    debug!(
        target: "test",
        "Wrote ConsensusHeader at block number {} with leader round {} and digest {:?}",
        consensus_header.number,
        consensus_header.sub_dag.leader_round(),
        consensus_digest
    );

    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(consensus_digest);
    let sealed_header = SealedHeader::new(exec_header, B256::default());

    // Create a FRESH ConsensusBus (simulating node restart) and seed the executed_anchor SSOT
    // with the recovered header, which is what core.rs does at boot from the highest-nonce block.
    let consensus_bus = ConsensusBus::new();
    consensus_bus
        .recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(sealed_header));
    consensus_bus.executed_anchor().send_replace(consensus_header.clone());

    // Verify initial state: committed_round should be 0 (default)
    let initial_committed_round: u32 = *consensus_bus.committed_round_updates().borrow();
    assert_eq!(
        initial_committed_round, 0,
        "Initial committed_round should be 0 before prime_consensus"
    );

    // Act: Call prime_consensus (this is what happens during node startup)
    prime_consensus(&consensus_bus, &config);

    // Assert: committed_round should be recovered from DB
    let recovered_committed_round: u32 = *consensus_bus.committed_round_updates().borrow();
    let max_round = recovered_committed_round + gc_depth;

    debug!(
        target: "test",
        "After prime_consensus: committed_round={}, gc_depth={}, max_round={}",
        recovered_committed_round, gc_depth, max_round
    );

    assert!(
        recovered_committed_round >= target_round,
        "committed_round ({}) should be >= {} (target round)",
        recovered_committed_round,
        target_round
    );

    let expected_max_round = target_round + gc_depth;
    assert!(
        max_round >= expected_max_round,
        "max_round ({}) should be >= {} (target_round + gc_depth)",
        max_round,
        expected_max_round,
    );

    assert!(recovered_committed_round > 0, "committed_round is 0, recovery failed");

    debug!(target: "test", max_round, "recovery successful, network accepts headers up to max_round");

    Ok(())
}

/// Verifies prime_consensus() recovers committed_round from the executed_anchor SSOT with both
/// consensus DB tables populated (the realistic post-restart state).
#[tokio::test]
async fn test_prime_consensus_recovers_via_primary_path() -> eyre::Result<()> {
    rayls_infrastructure_types::test_utils::init_test_tracing();

    // Setup: Create a committee fixture with in-memory database
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(54321))
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let gc_depth = config.parameters().gc_depth;

    // Create certificates for rounds 1 to 100 (simulating a node that ran for a while)
    let target_round = 100u32;
    let (certificates, _next_parents, _batches) =
        create_signed_certificates_for_rounds(1..=target_round, &fixture);

    // Get the last certificate from round 100 to use as the leader certificate
    let last_cert = certificates.back().unwrap().clone();
    debug!(
        target: "test",
        "Created certificate at round {} from authority {:?}",
        last_cert.round(),
        last_cert.origin()
    );

    // Create a ConsensusHeader with the certificate at round 100
    let sub_dag = CommittedSubDag::new(
        vec![], // certificates in the subdag (simplified for test)
        last_cert.clone(),
        0, // commit_timestamp
        ReputationScores::default(),
        None,
    );

    let consensus_header = ConsensusHeader {
        parent_hash: B256::default(),
        sub_dag,
        number: target_round as u64,
        extra: B256::default(),
    };

    // CRITICAL: Get the digest of the ConsensusHeader - this is the link between EL and CL
    let consensus_digest = consensus_header.digest();
    debug!(
        target: "test",
        "ConsensusHeader digest: {:?}",
        consensus_digest
    );

    // Write to BOTH consensus DB tables (this is what production does)
    // This is the key difference from the other test!
    db.with_write_txn(|txn| {
        // Table 1: ConsensusBlocks - number → header
        txn.insert::<ConsensusBlocks>(&consensus_header.number, &consensus_header)?;
        // Table 2: ConsensusBlockNumbersByDigest - digest → number (REQUIRED for PRIMARY path!)
        txn.insert::<ConsensusBlockNumbersByDigest>(&consensus_digest, &consensus_header.number)?;
        Ok(())
    })
    .expect("Failed to write consensus header to DB");

    debug!(
        target: "test",
        "Wrote ConsensusHeader at block number {} with leader round {} and digest {:?}",
        consensus_header.number,
        consensus_header.sub_dag.leader_round(),
        consensus_digest
    );

    // Create a SealedHeader with parent_beacon_block_root = consensus_digest
    // This simulates what try_restore_state() does: reads from reth DB where
    // each block has parent_beacon_block_root pointing to its ConsensusHeader
    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(consensus_digest);
    let sealed_header = SealedHeader::new(exec_header, B256::default());

    // Create ConsensusBus and populate recently_executed_blocks (simulating try_restore_state)
    let consensus_bus = ConsensusBus::new();

    // Verify recently_executed_blocks is initially empty
    assert!(
        consensus_bus.recently_executed_blocks().borrow().is_empty(),
        "recently_executed_blocks should be empty before population"
    );

    // Push the sealed header to recently_executed_blocks (what try_restore_state does)
    consensus_bus
        .recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(sealed_header.clone()));
    // Seed the executed_anchor SSOT with the recovered header (core.rs does this at boot).
    consensus_bus.executed_anchor().send_replace(consensus_header.clone());

    // Verify recently_executed_blocks is now populated
    assert!(
        !consensus_bus.recently_executed_blocks().borrow().is_empty(),
        "recently_executed_blocks should be populated after push"
    );

    // Verify the parent_beacon_block_root is set correctly
    let latest_block = consensus_bus.recently_executed_blocks().borrow().latest_block().clone();
    assert_eq!(
        latest_block.subdag_consensus_digest().map(|d| d.get()),
        Some(consensus_digest),
        "parent_beacon_block_root should match consensus_digest"
    );

    // Verify initial state: committed_round should be 0 (default)
    let initial_committed_round: u32 = *consensus_bus.committed_round_updates().borrow();
    assert_eq!(
        initial_committed_round, 0,
        "Initial committed_round should be 0 before prime_consensus"
    );

    // Act: Call prime_consensus (this is what happens during node startup)
    // Now it should use the PRIMARY path: recently_executed_blocks → parent_beacon_block_root →
    // get_consensus_by_hash
    prime_consensus(&consensus_bus, &config);

    // Assert: committed_round should be recovered from DB via PRIMARY path
    let recovered_committed_round: u32 = *consensus_bus.committed_round_updates().borrow();
    let max_round = recovered_committed_round + gc_depth;

    debug!(
        target: "test",
        "After prime_consensus: committed_round={}, gc_depth={}, max_round={}",
        recovered_committed_round, gc_depth, max_round
    );

    assert!(
        recovered_committed_round >= target_round,
        "committed_round ({}) should be >= {} via primary path",
        recovered_committed_round,
        target_round
    );

    let expected_max_round = target_round + gc_depth;
    assert!(
        max_round >= expected_max_round,
        "max_round ({}) should be >= {}",
        max_round,
        expected_max_round,
    );

    assert!(recovered_committed_round > 0, "committed_round is 0, primary path recovery failed");

    debug!(target: "test", max_round, "primary path recovery successful");

    Ok(())
}
