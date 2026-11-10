//! A follower that syncs epoch 0 is sent an epoch-0 commit that also carries a certificate from
//! epoch 1, authored by the validator that joined in epoch 1.
//!
//! The subscriber verifies every certificate against the leader's epoch keys, then looks each
//! author up in its own committee to pay block rewards. An author outside that committee is a
//! fatal `UnexpectedAuthority`, which panics the subscriber task.

use super::committee_change::{batches, committees, send_until_taken, spawn, WAIT};
use rayls_consensus_primary::{ConsensusBus, NodeMode};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    now, AuthorityIdentifier, Batch, Certificate, CommittedSubDag, ConsensusHeader,
    ConsensusOutput, Epoch, HeaderBuilder, Notifier, RaylsReceiver, RaylsSender as _,
    ReputationScores, Round, StreamedConsensusHeader, B256,
};
use rayls_testing_test_utils::CommitteeFixture;
use tokio::time::timeout;

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
    let own =
        after.authorities().find(|a| &a.id() == joined).expect("joined authority").vote(&header);
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

    consensus_bus.consensus_header().send(StreamedConsensusHeader::unverified(mixed)).await?;
    consensus_bus.consensus_header().send(StreamedConsensusHeader::unverified(clean)).await?;
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
async fn selfless_joined_cert_does_not_crash_follower() -> eyre::Result<()> {
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
