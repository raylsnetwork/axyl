//! Subscriber IT tests

#![allow(unused_crate_dependencies)]

use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{
    consensus::{Bullshark, Consensus, LeaderSchedule},
    network::PrimaryNetworkHandle,
    ConsensusBus,
};
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
};
use rayls_infrastructure_types::{
    CameFrom, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, Database, DbTxMut,
    ExecHeader, RaylsReceiver as _, RaylsSender as _, ReputationScores, SealedHeader, TaskManager,
    B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_testing_test_utils::{create_signed_certificates_for_rounds, CommitteeFixture};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

#[tokio::test]
async fn test_output_to_header() -> eyre::Result<()> {
    let num_sub_dags_per_schedule = 3;
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let task_manager = TaskManager::new("subscriber tests");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();

    let mut consensus_output = consensus_bus.consensus_output().subscribe();

    // prime recently_executed_blocks BEFORE spawn so execution-wait paths have a tip; the
    // subscriber's startup anchor now reads the SSOT `executed_anchor` (default number 0,
    // nothing executed).
    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(B256::ZERO);
    let dummy_parent = SealedHeader::new(exec_header, B256::default());
    consensus_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));

    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                reply.send(Ok(MessageId::new(&[0]))).unwrap();
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // spawn the executor
    let (to_engine, _subscriber_to_engine) = mpsc::channel(100);
    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // yield for subscriber to spawn
    tokio::task::yield_now().await;

    // make certificates for rounds 1 to 7 (inclusive)
    let (certificates, _next_parents, batches) =
        create_signed_certificates_for_rounds(1..=7, &fixture);

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
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &consensus_bus, bullshark, &task_manager);

    // forward certificates to trigger subdag commit
    for certificate in certificates.iter() {
        consensus_bus.new_certificates().send(certificate.clone()).await?;
    }

    let expected_num = 3;
    let mut consensus_headers_seen: Vec<_> = Vec::with_capacity(expected_num);
    while let Some(output) = consensus_output.recv().await {
        // assert epoch boundary not reached
        assert!(!output.close_epoch);

        let num = output.number;
        let consensus_header = output.consensus_header();
        consensus_headers_seen.push(consensus_header);
        if num == expected_num as u64 {
            break;
        }

        // yield for other tasks
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let last_header = consensus_headers_seen.last().expect("at least one consensus header seen");
    assert_eq!(last_header.number, expected_num as u64);

    Ok(())
}

/// Verifies the subscriber does not deadlock when replaying a stored consensus
/// header whose `committed_at` is >= `epoch_boundary`.
///
/// Regression test for the guard added at subscriber.rs:641. Without that guard
/// the subscriber would send the output to `to_engine`, then wait forever on
/// `recently_executed_blocks_update.await` because no engine is running to execute the
/// block (it belongs to a past epoch).
#[tokio::test]
async fn test_missing_consensus_beyond_epoch_boundary() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let mut config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let task_manager = TaskManager::new("epoch-boundary-test");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();

    // Subscribe to channels before spawning the subscriber.
    let mut consensus_output_rx = consensus_bus.consensus_output().subscribe();
    let mut replay_complete_rx = consensus_bus.execution_replay_complete().subscribe();

    // Mock network.
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                let _ = reply.send(Ok(MessageId::new(&[0])));
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // Build a leader certificate with a high created_at so that
    // committed_at() (max(0, created_at)) > epoch_boundary.
    let mut leader = Certificate::default();
    leader.header.created_at = 2000;
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);

    // Store the consensus header at number 1 so that
    // get_missing_consensus (last executed = 0) treats it as missing.
    let header = ConsensusHeader {
        parent_hash: B256::default(),
        sub_dag,
        number: 1,
        extra: B256::default(),
    };
    let digest = header.digest();
    consensus_store.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, &header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&digest, &header.number)?;
        Ok(())
    })?;

    // Verify the stored header has committed_at > epoch_boundary.
    let stored_committed = header.sub_dag.commit_timestamp();
    assert!(
        stored_committed >= 1000,
        "expected committed_at ({stored_committed}) >= epoch_boundary (1000)"
    );

    // Set epoch boundary lower than committed_at so the guard triggers.
    config.set_epoch_boundary(1000);

    // Leave the SSOT `executed_anchor` at its default (number 0), so get_missing_consensus
    // treats the stored header at number 1 as missing.

    // Keep the to_engine receiver alive but never read from it.
    // The old (broken) code path sends to_engine then waits on
    // recently_executed_blocks_update.await, which deadlocks because no engine
    // is running to update recently_executed_blocks. The guard must prevent
    // reaching that code path.
    let (to_engine, _rx) = mpsc::channel(100);

    spawn_subscriber(
        config,
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // Wait for replay to complete with a timeout to detect deadlock.
    timeout(Duration::from_secs(20), replay_complete_rx.changed()).await??;

    // Verify the boundary-crossing output was sent to the broadcast channel.
    // If this times out, the guard likely failed to trigger.
    let output = tokio::time::timeout(Duration::from_secs(1), consensus_output_rx.recv())
        .await
        .expect("timed out waiting for consensus_output — guard likely didn't trigger")
        .expect("broadcast channel closed unexpectedly");
    assert_eq!(output.number, 1, "output number should match stored header");
    assert!(output.committed_at() >= 1000, "output committed_at should be >= epoch_boundary");

    Ok(())
}

/// Regression for the epoch-boundary leak fork: the subscriber cuts the epoch deterministically
/// (first output reaching `epoch_boundary` = closing block, saved + broadcast; later outputs
/// dropped), not via the drain signal. With no drain signal sent, a post-boundary output must still
/// not be saved or broadcast.
#[tokio::test]
async fn subscriber_drops_post_boundary_output_without_drain_signal() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let mut config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let task_manager = TaskManager::new("post-boundary-cut-test");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();

    let mut consensus_output_rx = consensus_bus.consensus_output().subscribe();
    let mut replay_complete_rx = consensus_bus.execution_replay_complete().subscribe();

    // Mock network (ack publishes).
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                let _ = reply.send(Ok(MessageId::new(&[0])));
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // Prime recently_executed_blocks so the startup anchor path has a tip (nothing executed: anchor
    // number 0).
    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(B256::ZERO);
    let dummy_parent = SealedHeader::new(exec_header, B256::default());
    consensus_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));

    // Deterministic epoch boundary at timestamp 1000.
    const BOUNDARY: u64 = 1000;
    config.set_epoch_boundary(BOUNDARY);

    let (to_engine, _engine_rx) = mpsc::channel::<(CameFrom, ConsensusOutput)>(100);
    spawn_subscriber(
        config,
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // Wait for catch-up replay to finish; the subscriber is now in the live loop.
    timeout(Duration::from_secs(10), replay_complete_rx.changed()).await??;

    // First commit reaches the boundary (committed_at == BOUNDARY): the epoch-closing block.
    let mut closing_leader = Certificate::default();
    closing_leader.header.created_at = BOUNDARY;
    let closing =
        CommittedSubDag::new(vec![], closing_leader, 0, ReputationScores::default(), None);
    consensus_bus.sequence().send(closing).await?;

    // Second commit is past the boundary (committed_at > BOUNDARY): a next-epoch leak. No drain
    // signal is sent, mirroring the window in which the leak was originally saved.
    let mut leak_leader = Certificate::default();
    leak_leader.header.created_at = BOUNDARY + 10;
    let leak = CommittedSubDag::new(vec![], leak_leader, 0, ReputationScores::default(), None);
    consensus_bus.sequence().send(leak).await?;

    // The closing block is saved and broadcast.
    let closing_output = timeout(Duration::from_secs(10), consensus_output_rx.recv())
        .await
        .expect("timed out waiting for the epoch-closing output")
        .expect("consensus_output channel closed");
    assert!(
        closing_output.committed_at() >= BOUNDARY,
        "first output should be the boundary-crossing closing block"
    );
    let closing_number = closing_output.number;

    // The post-boundary leak must NOT be broadcast: no further output arrives.
    let leaked = timeout(Duration::from_secs(2), consensus_output_rx.recv()).await;
    assert!(
        leaked.is_err(),
        "post-boundary output must not be broadcast, got number {:?}",
        leaked.ok().flatten().map(|o| o.number)
    );

    // The durable consensus tip (which bounds restart replay via get_missing_consensus) must be the
    // epoch-closing block, not the post-boundary leak. get_consensus_by_number is not used here: it
    // falls back to the transient ConsensusBlocksCache, where the header is written at ingest
    // before the save-side cut, and that cache is not the restart-replay vector.
    let (durable_tip, _) = consensus_store
        .last_record::<ConsensusBlocks>()
        .expect("the epoch-closing block must be durably saved");
    assert_eq!(
        durable_tip, closing_number,
        "durable consensus tip must be the epoch-closing block, not the post-boundary leak"
    );

    Ok(())
}

/// A committed consensus output with an empty subdag, chained from `parent_hash`.
fn empty_header(number: u64, parent_hash: B256) -> ConsensusHeader {
    let sub_dag =
        CommittedSubDag::new(vec![], Certificate::default(), 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
}

/// Regression for validator-2's restart fork: when the consensus-chain tip is ahead of the
/// EVM-execution anchor, the subscriber must seed live numbering from the consensus tip, not the
/// lagging anchor. Seeding from the lagging anchor reuses a number the network already consumed,
/// producing a divergent ConsensusHeader digest - which feeds `mix_hash` and
/// `parent_beacon_block_root` and forks the chain even though the executed transactions and state
/// are identical.
///
/// Setup: header 4 is where the EVM anchor points; header 5 is committed after it (in
/// `ConsensusBlocks`) but the anchor has not durably reached it (e.g. lost to a crash/static-file
/// heal). The next live commit must be numbered 6 and chain from header 5 - not 5 chaining from
/// header 4, the off-by-one that forked validator-2.
#[tokio::test]
async fn live_numbering_seeds_from_consensus_tip_across_lagging_anchor_restart() -> eyre::Result<()>
{
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let task_manager = TaskManager::new("lagging-anchor-restart-test");
    let rx_shutdown = config.shutdown().subscribe();
    let consensus_bus = ConsensusBus::new();

    let mut consensus_output_rx = consensus_bus.consensus_output().subscribe();
    let mut replay_complete_rx = consensus_bus.execution_replay_complete().subscribe();

    // Mock network (ack publishes).
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                let _ = reply.send(Ok(MessageId::new(&[0])));
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    // Persisted consensus chain at restart: header 4 then header 5, chained 5 -> 4. Both live in
    // ConsensusBlocks; the execution anchor has only durably reached header 4.
    let header4 = empty_header(4, B256::default());
    let header5 = empty_header(5, header4.digest());
    let digest4 = header4.digest();
    let digest5 = header5.digest();
    consensus_store.with_write_txn(|txn| {
        for header in [&header4, &header5] {
            txn.insert::<ConsensusBlocks>(&header.number, header)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        }
        Ok(())
    })?;

    // EVM-execution anchor = header 4: the executed tip's parent_beacon_block_root points at
    // header 4, NOT header 5. This is the lagging anchor that caused the off-by-one.
    let mut exec_header = ExecHeader::default();
    exec_header.parent_beacon_block_root = Some(digest4);
    let exec_tip = SealedHeader::new(exec_header, B256::from([1u8; 32]));
    consensus_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(exec_tip));

    // Stand in for the engine: each replayed output produces a block, so drain to_engine and tick
    // recently_executed_blocks per output - the signal the replay loop now waits on.
    // Annotated because `engine_rx` is moved into `tokio::spawn` below before `spawn_subscriber`
    // constrains the channel type, so inference can't resolve it from the sender side in time.
    let (to_engine, mut engine_rx) = mpsc::channel::<(CameFrom, ConsensusOutput)>(100);
    let engine_bus = consensus_bus.clone();
    tokio::spawn(async move {
        while let Some((_came_from, output)) = engine_rx.recv().await {
            let mut header = ExecHeader::default();
            header.number = output.number;
            let block = SealedHeader::new(header, B256::from([2u8; 32]));
            engine_bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(block));
        }
    });
    spawn_subscriber(
        config.clone(),
        rx_shutdown,
        consensus_bus.clone(),
        &task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // Wait for replay to finish; the subscriber is now seeding the live loop.
    timeout(Duration::from_secs(10), replay_complete_rx.changed()).await??;

    // Drive one live commit. `sequence` is a buffered mpsc, so this is delivered once the
    // subscriber subscribes to it at the top of the live loop.
    let live_sub_dag =
        CommittedSubDag::new(vec![], Certificate::default(), 0, ReputationScores::default(), None);
    consensus_bus.sequence().send(live_sub_dag.clone()).await?;

    let output = timeout(Duration::from_secs(10), consensus_output_rx.recv())
        .await
        .expect("timed out waiting for live consensus output")
        .expect("consensus_output channel closed");

    // The live commit must continue the chain from the consensus tip (header 5).
    assert_eq!(
        output.number, 6,
        "live commit must be numbered consensus-tip + 1 (6), not reuse the EVM-anchor number (5)"
    );
    assert_eq!(
        output.consensus_header().parent_hash, digest5,
        "live commit must chain from the consensus tip (header 5), not the execution anchor (header 4)"
    );

    // The output digest feeds mix_hash and parent_beacon_block_root, hence the EVM block hash.
    // It must equal the consensus-tip-seeded header, and must NOT equal the off-by-one header the
    // lagging EVM anchor would have produced (number 5 chaining from header 4) - that mismatch is
    // exactly what forked validator-2.
    let correct = ConsensusHeader {
        parent_hash: digest5,
        sub_dag: live_sub_dag.clone(),
        number: 6,
        extra: B256::default(),
    };
    let forked = ConsensusHeader {
        parent_hash: digest4,
        sub_dag: live_sub_dag,
        number: 5,
        extra: B256::default(),
    };
    assert_eq!(
        output.consensus_header().digest(),
        correct.digest(),
        "output_digest must match the consensus-tip-seeded ConsensusHeader"
    );
    assert_ne!(
        output.consensus_header().digest(),
        forked.digest(),
        "output_digest must not reproduce the off-by-one (EVM-anchor) digest that forked validator-2"
    );

    Ok(())
}
