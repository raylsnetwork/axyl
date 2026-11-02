//! A node that syncs across an epoch boundary where a validator joined the committee.
//!
//! The syncing node starts in epoch 0 with the 4-validator committee. The network has moved on to
//! epoch 1, where a fifth validator joined. The node's first epoch-1 commit has a certificate from
//! the new validator. Verification passes (the epoch-0 record names the epoch-1 committee), but the
//! subscriber then looks the new validator up in the epoch-0 committee it started with, does not
//! find it, and fails fatally. That crashes the node, and it crashes the same way on every restart.

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode};
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::EpochRecords};
use rayls_infrastructure_types::{
    now, test_chain_spec_arc, B256Map, CommittedSubDag, ConsensusHeader, Database, DbTxMut,
    EpochRecord, HeaderBuilder, Notifier, RaylsSender as _, ReputationScores, TaskManager, B256,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_testing_test_utils::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

/// Seed shared by both committee fixtures, so the epoch-1 committee keeps the four epoch-0
/// validators (same keys and execution addresses) and adds one.
const SEED: u64 = 0x5eed;

/// How long the subscriber must survive the epoch-1 commit.
const SURVIVE: Duration = Duration::from_secs(10);

async fn sync_across_validator_join(mode: NodeMode) -> eyre::Result<()> {
    let before = CommitteeFixture::builder(MemDatabase::default)
        .epoch(0)
        .with_rng(StdRng::seed_from_u64(SEED))
        .build();
    let after = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(5).expect("non-zero"))
        .epoch(1)
        .with_rng(StdRng::seed_from_u64(SEED))
        .build();
    let old_members: BTreeSet<_> = before.authorities().map(|a| a.id()).collect();
    let kept = after.authorities().filter(|a| old_members.contains(&a.id())).count();
    assert_eq!(kept, 4, "the epoch-1 committee must keep the four epoch-0 validators");
    let joined = after
        .authorities()
        .find(|a| !old_members.contains(&a.id()))
        .expect("one validator joined in epoch 1")
        .id();
    assert!(before.committee().authority(&joined).is_none());

    // The syncing node: an epoch-0 validator, configured with the epoch-0 committee.
    let config = before.first_authority().consensus_config().clone();
    assert_eq!(config.epoch(), 0);

    // The certified epoch-0 record names the epoch-1 committee, as on any node that synced the
    // epoch records. This is what lets the node verify epoch-1 headers.
    let record = EpochRecord {
        epoch: 0,
        committee: before.committee().bls_keys(),
        next_committee: after.committee().bls_keys(),
        ..Default::default()
    };
    config.node_storage().with_write_txn(|txn| txn.insert::<EpochRecords>(&0, &record))?;
    assert_eq!(config.get_committee_keys_for_epoch(1), Some(after.committee().bls_keys()));

    // The first epoch-1 commit: its leader is the new validator's certificate, carrying one batch,
    // signed by the epoch-1 committee.
    let batch = rayls_execution_evm::test_utils::batch(test_chain_spec_arc());
    let mut batches = B256Map::default();
    batches.insert(batch.digest(), batch.clone());
    let leader_header = HeaderBuilder::default()
        .author(joined.clone())
        .round(2)
        .epoch(1)
        .parents(after.genesis().collect())
        .created_at(now())
        .with_payload_batch(batch, 0)
        .build();
    let leader = after.certificate(&leader_header);
    let sub_dag =
        CommittedSubDag::new(vec![leader.clone()], leader, 0, ReputationScores::default(), None);
    let header = ConsensusHeader {
        parent_hash: B256::default(),
        sub_dag,
        number: 1,
        extra: B256::default(),
    };
    assert_eq!(header.sub_dag.leader_epoch(), 1);

    // Peers serve the batch.
    config
        .local_network()
        .set_primary_to_worker_local_handler(Arc::new(MockPrimaryToWorkerClient { batches }));

    // Mock network (ack publishes; header requests go unanswered).
    let (tx, mut rx) = mpsc::channel(5);
    tokio::spawn(async move {
        while let Some(com) = rx.recv().await {
            if let NetworkCommand::Publish { topic: _, msg: _, reply } = com {
                let _ = reply.send(Ok(MessageId::new(&[0])));
            }
        }
    });
    let network = PrimaryNetworkHandle::new_for_test(tx);

    let consensus_bus = ConsensusBus::new();
    consensus_bus.node_mode().send_replace(mode);
    let mut task_manager = TaskManager::new("committee-change-test");
    let (to_engine, _engine_rx) = mpsc::channel(100);
    spawn_subscriber(
        config.clone(),
        config.shutdown().subscribe(),
        consensus_bus.clone(),
        &task_manager,
        network,
        to_engine,
        tokio::sync::watch::channel(()).0,
    );

    // Deliver the epoch-1 header the way the header streamer does, repeatedly: a broadcast send
    // to a channel nobody listens on yet is dropped silently, and the subscriber skips the
    // duplicates once it has taken the header.
    let streamer_bus = consensus_bus.clone();
    let streamer = tokio::spawn(async move {
        loop {
            let _ = streamer_bus.consensus_header().send(header.clone()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // The subscriber is a critical task: the task manager resolves only if it exits.
    let exited = timeout(SURVIVE, task_manager.join(Notifier::new())).await;
    streamer.abort();
    config.shutdown().notify();
    assert!(
        exited.is_err(),
        "{mode:?} subscriber exited on the first epoch-1 commit (authored by validator {joined} \
         who joined in epoch 1): {exited:?}"
    );
    Ok(())
}

/// An observer following the chain across the epoch where a validator joined.
#[tokio::test]
async fn observer_syncs_across_validator_join() -> eyre::Result<()> {
    sync_across_validator_join(NodeMode::Observer).await
}

/// A validator catching up (to rejoin) across the epoch where a validator joined.
#[tokio::test]
async fn rejoining_validator_syncs_across_validator_join() -> eyre::Result<()> {
    sync_across_validator_join(NodeMode::CvvInactive).await
}
