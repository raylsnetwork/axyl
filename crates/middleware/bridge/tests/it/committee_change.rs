//! A node that syncs across an epoch boundary where a validator joined the committee.
//!
//! The syncing node starts in epoch 0 with the 4-validator committee. The network has moved on to
//! epoch 1, where a fifth validator joined. The node's first epoch-1 commit has a certificate from
//! the new validator. The subscriber must drop that commit, not look the new validator up in the
//! epoch-0 committee it started with: that lookup fails fatally and crashes the node.

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    tables::{ConsensusBlocks, EpochRecords},
};
use rayls_infrastructure_types::{
    now, test_chain_spec_arc, AuthorityIdentifier, B256Map, Batch, CertificateDigest,
    CommittedSubDag, ConsensusHeader, ConsensusOutput, Database, DbTxMut, Epoch, EpochRecord,
    Hash as _, HeaderBuilder, Notifier, RaylsReceiver, RaylsSender as _, ReputationScores, Round,
    StreamedConsensusHeader, TaskManager, B256,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_testing_test_utils::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

/// Seed shared by both fixtures, so the epoch-1 committee keeps the four epoch-0 validators.
const SEED: u64 = 0x5eed;

/// How long the subscriber gets to take a commit or to stop.
pub(super) const WAIT: Duration = Duration::from_secs(10);

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

/// The two fixtures: epoch 0 with four validators, and epoch 1 where a fifth one joined.
pub(super) fn committees(
) -> (CommitteeFixture<MemDatabase>, CommitteeFixture<MemDatabase>, AuthorityIdentifier) {
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
    (before, after, joined)
}

/// The certified epoch-0 record: it names the epoch-0 committee and the epoch-1 one.
fn epoch_0_record(
    before: &CommitteeFixture<MemDatabase>,
    after: &CommitteeFixture<MemDatabase>,
) -> EpochRecord {
    EpochRecord {
        epoch: 0,
        committee: before.committee().bls_keys(),
        next_committee: after.committee().bls_keys(),
        ..Default::default()
    }
}

/// Test batches, one per commit.
pub(super) fn batches(count: usize) -> Vec<Batch> {
    (0..count).map(|_| rayls_execution_evm::test_utils::batch(test_chain_spec_arc())).collect()
}

/// Spawns the subscriber of the node with `config` on `consensus_bus`, served `batches` by peers.
pub(super) fn spawn(
    config: &ConsensusConfig<MemDatabase>,
    mode: NodeMode,
    batches: &[Batch],
    consensus_bus: &ConsensusBus,
) -> TaskManager {
    let mut served = B256Map::default();
    for batch in batches {
        served.insert(batch.digest(), batch.clone());
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

    consensus_bus.node_mode().send_replace(mode);
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
    task_manager
}

/// Resends `headers` until the subscriber outputs one: a broadcast with no listener is lost.
pub(super) async fn send_until_taken(
    consensus_bus: &ConsensusBus,
    outputs: &mut impl RaylsReceiver<ConsensusOutput>,
    headers: Vec<ConsensusHeader>,
) -> eyre::Result<ConsensusOutput> {
    let streamer_bus = consensus_bus.clone();
    let streamer = tokio::spawn(async move {
        loop {
            for header in &headers {
                let _ = streamer_bus
                    .consensus_header()
                    .send(StreamedConsensusHeader::unverified(header.clone()))
                    .await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let taken = timeout(WAIT, outputs.recv()).await;
    streamer.abort();
    taken
        .map_err(|_| eyre::eyre!("no commit was taken in time"))?
        .ok_or_else(|| eyre::eyre!("consensus output channel closed"))
}

/// Stops the subscriber and checks that commit `number` is stored from the `expected` leader.
async fn assert_stored(
    mode: NodeMode,
    config: &ConsensusConfig<MemDatabase>,
    mut task_manager: TaskManager,
    number: u64,
    expected: CertificateDigest,
) -> eyre::Result<()> {
    config.shutdown().notify();
    let stopped = timeout(WAIT, task_manager.join(config.shutdown().clone())).await;
    assert!(matches!(stopped, Ok(Ok(()))), "{mode:?} subscriber did not stop cleanly: {stopped:?}");
    let stored = config.node_storage().get::<ConsensusBlocks>(&number)?.expect("commit is stored");
    assert_eq!(
        stored.sub_dag.leader.digest(),
        expected,
        "commit {number} is stored from the wrong commit"
    );
    Ok(())
}

async fn sync_across_validator_join(mode: NodeMode, next_epoch_keys: bool) -> eyre::Result<()> {
    let (before, after, joined) = committees();
    let old_member = before.first_authority().id();

    // The syncing node: an epoch-0 validator, configured with the epoch-0 committee.
    let mut config = before.first_authority().consensus_config().clone();
    assert_eq!(config.epoch(), 0);
    // Boundary unknown, so the epoch cut cannot drop the epoch-1 commit for the subscriber.
    config.set_epoch_boundary(u64::MAX);

    // The certified epoch-0 record names the epoch-1 committee, so epoch-1 commits verify.
    if next_epoch_keys {
        let record = epoch_0_record(&before, &after);
        config.node_storage().with_write_txn(|txn| txn.insert::<EpochRecords>(&0, &record))?;
        assert_eq!(config.get_committee_keys_for_epoch(1), Some(after.committee().bls_keys()));
    } else {
        assert_eq!(config.get_committee_keys_for_epoch(1), None);
    }

    // 1: an epoch-0 commit; 2: the new validator's first epoch-1 commit; 2 again: an epoch-0 one.
    let batches = batches(3);
    let control = commit(&before, &old_member, 0, 2, 1, batches[0].clone());
    let joined_commit = commit(&after, &joined, 1, 2, 2, batches[1].clone());
    let same_number = commit(&before, &old_member, 0, 4, 2, batches[2].clone());
    assert_eq!(joined_commit.sub_dag.leader_epoch(), 1);

    let consensus_bus = ConsensusBus::new();
    let mut outputs = consensus_bus.consensus_output().subscribe();
    let mut task_manager = spawn(&config, mode, &batches, &consensus_bus);
    let taken = send_until_taken(&consensus_bus, &mut outputs, vec![control]).await?;
    assert_eq!(taken.number, 1, "the control commit must be taken");

    // the epoch-1 commit must be dropped without using its number: commit 2 is the epoch-0 one
    let expected = same_number.sub_dag.leader.digest();
    consensus_bus
        .consensus_header()
        .send(StreamedConsensusHeader::unverified(joined_commit))
        .await?;
    consensus_bus.consensus_header().send(StreamedConsensusHeader::unverified(same_number)).await?;
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

    assert_stored(mode, &config, task_manager, 2, expected).await
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

/// A node that has moved to epoch 1 drops an epoch-0 commit past its anchor, even a verified one.
#[tokio::test]
async fn earlier_epoch_commit_is_dropped() -> eyre::Result<()> {
    let mode = NodeMode::Observer;
    let (before, after, _) = committees();
    let old_member = before.first_authority().id();

    // The syncing node: configured with the epoch-1 committee, and it knows the epoch-0 keys.
    let mut config = after.first_authority().consensus_config().clone();
    assert_eq!(config.epoch(), 1);
    config.set_epoch_boundary(u64::MAX);
    let record = epoch_0_record(&before, &after);
    config.node_storage().with_write_txn(|txn| txn.insert::<EpochRecords>(&0, &record))?;
    assert_eq!(config.get_committee_keys_for_epoch(0), Some(before.committee().bls_keys()));

    // 1: a signed epoch-0 straggler past the anchor; 1 again: the epoch-1 commit.
    let batches = batches(2);
    let straggler = commit(&before, &old_member, 0, 10, 1, batches[0].clone());
    let current = commit(&after, &after.first_authority().id(), 1, 2, 1, batches[1].clone());
    let expected = current.sub_dag.leader.digest();

    let consensus_bus = ConsensusBus::new();
    let mut outputs = consensus_bus.consensus_output().subscribe();
    let task_manager = spawn(&config, mode, &batches, &consensus_bus);
    let taken = send_until_taken(&consensus_bus, &mut outputs, vec![straggler, current]).await?;
    assert_eq!(taken.number, 1, "{mode:?} subscriber skipped commit 1");
    assert_eq!(taken.sub_dag.leader.digest(), expected, "{mode:?} took the epoch-0 commit");

    assert_stored(mode, &config, task_manager, 1, expected).await
}
