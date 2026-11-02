//! A node that syncs across an epoch boundary where a validator joined the committee.
//!
//! The syncing node starts in epoch 0 with the 4-validator committee. The network has moved on to
//! epoch 1, where a fifth validator joined. The node's first epoch-1 commit has a certificate from
//! the new validator. The subscriber must drop that commit, not look the new validator up in the
//! epoch-0 committee it started with: that lookup fails fatally and crashes the node.

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode};
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    tables::{ConsensusBlocks, EpochRecords},
};
use rayls_infrastructure_types::{
    now, test_chain_spec_arc, AuthorityIdentifier, B256Map, Batch, CommittedSubDag,
    ConsensusHeader, Database, DbTxMut, Epoch, EpochRecord, Hash as _, HeaderBuilder, Notifier,
    RaylsReceiver as _, RaylsSender as _, ReputationScores, Round, TaskManager, B256,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_testing_test_utils::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

/// Seed shared by both fixtures, so the epoch-1 committee keeps the four epoch-0 validators.
const SEED: u64 = 0x5eed;

/// How long the subscriber gets to take a commit or to stop.
const WAIT: Duration = Duration::from_secs(10);

/// A commit at `number` whose leader is `author`'s certificate of `(epoch, round)`, carrying
/// `batch`.
fn commit(
    fixture: &CommitteeFixture<MemDatabase>,
    author: &AuthorityIdentifier,
    epoch: Epoch,
    round: Round,
    number: u64,
    batch: Batch,
) -> ConsensusHeader {
    let header = HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(epoch)
        .parents(fixture.genesis().collect())
        .created_at(now())
        .with_payload_batch(batch, 0)
        .build();
    let leader = fixture.certificate(&header);
    let sub_dag =
        CommittedSubDag::new(vec![leader.clone()], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::default(), sub_dag, number, extra: B256::default() }
}

async fn sync_across_validator_join(mode: NodeMode, next_epoch_keys: bool) -> eyre::Result<()> {
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
    let old_member = before.first_authority().id();

    // The syncing node: an epoch-0 validator, configured with the epoch-0 committee.
    let mut config = before.first_authority().consensus_config().clone();
    assert_eq!(config.epoch(), 0);
    // Boundary unknown, so the epoch cut cannot drop the epoch-1 commit for the subscriber.
    config.set_epoch_boundary(u64::MAX);

    // The certified epoch-0 record names the epoch-1 committee, so epoch-1 commits verify.
    if next_epoch_keys {
        let record = EpochRecord {
            epoch: 0,
            committee: before.committee().bls_keys(),
            next_committee: after.committee().bls_keys(),
            ..Default::default()
        };
        config.node_storage().with_write_txn(|txn| txn.insert::<EpochRecords>(&0, &record))?;
        assert_eq!(config.get_committee_keys_for_epoch(1), Some(after.committee().bls_keys()));
    } else {
        assert_eq!(config.get_committee_keys_for_epoch(1), None);
    }

    // 1: an epoch-0 commit; 2: the new validator's first epoch-1 commit; 2 again: an epoch-0 one.
    let batches: Vec<Batch> =
        (0..3).map(|_| rayls_execution_evm::test_utils::batch(test_chain_spec_arc())).collect();
    let control = commit(&before, &old_member, 0, 2, 1, batches[0].clone());
    let joined_commit = commit(&after, &joined, 1, 2, 2, batches[1].clone());
    let same_number = commit(&before, &old_member, 0, 4, 2, batches[2].clone());
    assert_eq!(joined_commit.sub_dag.leader_epoch(), 1);

    // Peers serve every batch.
    let mut served = B256Map::default();
    for batch in batches {
        served.insert(batch.digest(), batch);
    }
    config.local_network().set_primary_to_worker_local_handler(Arc::new(
        MockPrimaryToWorkerClient { batches: served },
    ));

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
    let mut outputs = consensus_bus.consensus_output().subscribe();
    let mut task_manager = TaskManager::new("committee-change-test");
    // no 2 s straggler grace, so a crash shows up at once
    task_manager.set_join_wait(0);
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

    // Resend the control until it is taken: a broadcast to a channel with no listener is lost.
    let streamer_bus = consensus_bus.clone();
    let streamer = tokio::spawn(async move {
        loop {
            let _ = streamer_bus.consensus_header().send(control.clone()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let taken = timeout(WAIT, outputs.recv()).await;
    streamer.abort();
    assert_eq!(taken?.map(|output| output.number), Some(1), "the control commit must be taken");

    // the epoch-1 commit must be dropped without using its number: commit 2 is the epoch-0 one
    let expected = same_number.sub_dag.leader.digest();
    consensus_bus.consensus_header().send(joined_commit).await?;
    consensus_bus.consensus_header().send(same_number).await?;
    tokio::select! {
        exited = task_manager.join(Notifier::new()) => panic!(
            "{mode:?} subscriber exited on the first epoch-1 commit (authored by validator \
             {joined} who joined in epoch 1): {exited:?}"
        ),
        output = timeout(WAIT, outputs.recv()) => {
            // without the keys, verifying first would wait 30 s, longer than WAIT
            let output = output
                .map_err(|_| eyre::eyre!("{mode:?} subscriber did not take commit 2 in time"))?
                .expect("consensus output channel open");
            assert_eq!(output.number, 2, "{mode:?} subscriber skipped commit 2");
            assert_eq!(output.sub_dag.leader.digest(), expected, "{mode:?} took the epoch-1 commit");
        }
    }

    // stop the subscriber before reading what it stored
    config.shutdown().notify();
    let stopped = timeout(WAIT, task_manager.join(config.shutdown().clone())).await;
    assert!(stopped.is_ok(), "{mode:?} subscriber did not stop");
    let stored = config.node_storage().get::<ConsensusBlocks>(&2)?.expect("commit 2 is stored");
    assert_eq!(
        stored.sub_dag.leader.digest(),
        expected,
        "commit 2 is stored from the wrong commit"
    );
    Ok(())
}

/// An observer following the chain across the epoch where a validator joined.
#[tokio::test]
async fn observer_syncs_across_validator_join() -> eyre::Result<()> {
    sync_across_validator_join(NodeMode::Observer, true).await
}

/// A validator catching up (to rejoin) across the epoch where a validator joined.
#[tokio::test]
async fn rejoining_validator_syncs_across_validator_join() -> eyre::Result<()> {
    sync_across_validator_join(NodeMode::CvvInactive, true).await
}

/// The later-epoch commit is dropped before it is verified, so missing keys do not hold it up.
#[tokio::test]
async fn later_epoch_commit_is_dropped_before_verifying() -> eyre::Result<()> {
    sync_across_validator_join(NodeMode::Observer, false).await
}
