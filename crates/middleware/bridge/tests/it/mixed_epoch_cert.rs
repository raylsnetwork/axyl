//! A follower that syncs epoch 0 is sent an epoch-0 commit that also carries a certificate from
//! epoch 1, authored by the validator that joined in epoch 1.
//!
//! The subscriber verifies every certificate against the leader's epoch keys, then looks each
//! author up in its own committee to pay block rewards. An author outside that committee is a
//! fatal `UnexpectedAuthority`, which panics the subscriber task.

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_consensus_network::types::{MessageId, NetworkCommand};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus, NodeMode};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    now, test_chain_spec_arc, AuthorityIdentifier, B256Map, Batch, Certificate, CommittedSubDag,
    ConsensusHeader, ConsensusOutput, Epoch, HeaderBuilder, Notifier, RaylsReceiver,
    RaylsSender as _, ReputationScores, Round, TaskManager, B256,
};
use rayls_middleware_bridge::subscriber::spawn_subscriber;
use rayls_testing_test_utils::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

/// Seed shared by both fixtures, so the epoch-1 committee keeps the four epoch-0 validators.
const SEED: u64 = 0x5eed;

/// How long the subscriber gets to take a commit or to stop.
const WAIT: Duration = Duration::from_secs(10);

fn committees(
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

/// An epoch-0 leader certificate by `author` at `round`, carrying `batch`.
fn leader(
    fixture: &CommitteeFixture<MemDatabase>,
    author: &AuthorityIdentifier,
    round: Round,
    batch: Batch,
) -> Certificate {
    let header = HeaderBuilder::default()
        .author(author.clone())
        .round(round)
        .epoch(0)
        .parents(fixture.genesis().collect())
        .created_at(now())
        .with_payload_batch(batch, 0)
        .build();
    fixture.certificate(&header)
}

fn commit(certificates: Vec<Certificate>, leader: Certificate, number: u64) -> ConsensusHeader {
    let sub_dag = CommittedSubDag::new(certificates, leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::default(), sub_dag, number, extra: B256::default() }
}

/// The joined validator's epoch-1 header with no payload.
fn joined_header(
    after: &CommitteeFixture<MemDatabase>,
    joined: &AuthorityIdentifier,
    epoch: Epoch,
    round: Round,
) -> rayls_infrastructure_types::Header {
    HeaderBuilder::default()
        .author(joined.clone())
        .round(round)
        .epoch(epoch)
        .parents(after.genesis().collect())
        .created_at(now())
        .build()
}

/// The certificate an honest joined validator builds: its own vote first, then the others'.
/// This is what `Certifier::propose_header` does in production.
fn honest_joined_cert(
    after: &CommitteeFixture<MemDatabase>,
    joined: &AuthorityIdentifier,
) -> Certificate {
    let header = joined_header(after, joined, 1, 3);
    let own = after.authorities().find(|a| &a.id() == joined).expect("joined authority").vote(&header);
    let mut votes = vec![(own.author().clone(), *own.signature())];
    votes.extend(after.votes(&header).into_iter().map(|v| (v.author().clone(), *v.signature())));
    Certificate::new_unverified(&after.committee(), header, votes).expect("quorum")
}

/// A certificate the joined validator can build only by leaving out its own vote: the four old
/// members' votes, with the signer bitmap laid out over the epoch-0 key list.
fn selfless_joined_cert(
    before: &CommitteeFixture<MemDatabase>,
    after: &CommitteeFixture<MemDatabase>,
    joined: &AuthorityIdentifier,
) -> Certificate {
    let header = joined_header(after, joined, 1, 3);
    let votes: Vec<_> =
        after.votes(&header).into_iter().map(|v| (v.author().clone(), *v.signature())).collect();
    assert_eq!(votes.len(), 4, "only the four old members vote");
    Certificate::new_unverified(&before.committee(), header, votes).expect("quorum")
}

fn batches(count: usize) -> Vec<Batch> {
    (0..count).map(|_| rayls_execution_evm::test_utils::batch(test_chain_spec_arc())).collect()
}

fn spawn(
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
    let mut task_manager = TaskManager::new("mixed-epoch-cert-test");
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

async fn send_until_taken(
    consensus_bus: &ConsensusBus,
    outputs: &mut impl RaylsReceiver<ConsensusOutput>,
    headers: Vec<ConsensusHeader>,
) -> eyre::Result<ConsensusOutput> {
    let streamer_bus = consensus_bus.clone();
    let streamer = tokio::spawn(async move {
        loop {
            for header in &headers {
                let _ = streamer_bus.consensus_header().send(header.clone()).await;
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

/// Sends commit 1 (control) then commit 2 carrying `extra`, and reports whether the subscriber
/// task stopped (crashed) or took a commit 2.
async fn run(mode: NodeMode, extra: Certificate) -> eyre::Result<Result<ConsensusOutput, String>> {
    let (before, _after, _joined) = committees();
    let old_member = before.first_authority().id();
    let mut config = before.first_authority().consensus_config().clone();
    assert_eq!(config.epoch(), 0);
    config.set_epoch_boundary(u64::MAX);

    let batches = batches(3);
    let control_leader = leader(&before, &old_member, 2, batches[0].clone());
    let control = commit(vec![control_leader.clone()], control_leader, 1);
    let mixed_leader = leader(&before, &old_member, 4, batches[1].clone());
    let mixed = commit(vec![extra, mixed_leader.clone()], mixed_leader, 2);
    let clean_leader = leader(&before, &old_member, 4, batches[2].clone());
    let clean = commit(vec![clean_leader.clone()], clean_leader, 2);

    let consensus_bus = ConsensusBus::new();
    let mut outputs = consensus_bus.consensus_output().subscribe();
    let mut task_manager = spawn(&config, mode, &batches, &consensus_bus);
    let taken = send_until_taken(&consensus_bus, &mut outputs, vec![control]).await?;
    assert_eq!(taken.number, 1, "the control commit must be taken");

    consensus_bus.consensus_header().send(mixed).await?;
    consensus_bus.consensus_header().send(clean).await?;
    let result = tokio::select! {
        exited = task_manager.join(Notifier::new()) => Err(format!("{exited:?}")),
        output = timeout(WAIT, outputs.recv()) => {
            Ok(output.map_err(|_| eyre::eyre!("no commit 2 in time"))?.expect("channel open"))
        }
    };
    config.shutdown().notify();
    Ok(result)
}

/// The issue's hypothesis with a production-shaped certificate: it never verifies against the
/// epoch-0 keys, because it carries the joined validator's own signature.
#[tokio::test]
async fn honest_joined_cert_is_rejected_not_fatal() -> eyre::Result<()> {
    for mode in [NodeMode::Observer, NodeMode::CvvInactive] {
        let (before, after, joined) = committees();
        let cert = honest_joined_cert(&after, &joined);
        assert!(
            cert.clone().verify_cert(&before.committee().bls_keys()).is_err(),
            "a self-signed joined-validator certificate must fail the epoch-0 keys"
        );
        let result = run(mode, cert).await?;
        let output = result.unwrap_or_else(|e| panic!("{mode:?} subscriber stopped: {e}"));
        assert_eq!(output.number, 2);
        assert_eq!(output.sub_dag.certificates.len(), 1, "{mode:?} took the mixed commit");
    }
    Ok(())
}

/// The crash: a joined validator that leaves out its own vote gets a certificate signed by a
/// quorum of the epoch-0 keys. Its author is outside that committee, so it must be rejected at
/// verification rather than stop the follower's subscriber fatally on the author lookup.
#[tokio::test]
async fn selfless_joined_cert_crashes_follower() -> eyre::Result<()> {
    for mode in [NodeMode::Observer, NodeMode::CvvInactive] {
        let (before, after, joined) = committees();
        let cert = selfless_joined_cert(&before, &after, &joined);
        assert!(
            cert.clone().verify_cert(&before.committee().bls_keys()).is_err(),
            "a certificate authored outside the epoch-0 committee must fail the epoch-0 keys"
        );
        let result = run(mode, cert).await?;
        assert!(
            result.is_ok(),
            "{mode:?} subscriber stopped on a certificate by validator {joined} from epoch 1: {}",
            result.err().unwrap_or_default()
        );
    }
    Ok(())
}
