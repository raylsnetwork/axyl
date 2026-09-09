//! Proposer unit tests.

use std::collections::BTreeSet;

use super::*;
use crate::consensus::LeaderSwapTable;
use indexmap::IndexMap;
use rayls_execution_evm::FixedBytes;
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    BlockNumHash, Certificate, CommittedSubDag, ConsensusHeader, ExecHeader, RaylsReceiver,
    RaylsSender, ReputationScores, SealedHeader, B256,
};
use rayls_testing_test_utils_committee::CommitteeFixture;

#[tokio::test]
async fn test_empty_proposal() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    cb.execution_replay_complete().send_replace(true);

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round(), 1);
    assert!(header.payload().is_empty());
    assert!(header.validate(&committee).is_ok());

    // TODO: assert header el state present
}

/// Build an execution anchor (`ConsensusHeader`) whose leader sits at `round`.
fn anchor_at_round(round: u32) -> ConsensusHeader {
    let mut leader = Certificate::default();
    leader.header.round = round;
    let sub_dag = CommittedSubDag::new(vec![], leader, 1, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::default(), sub_dag, number: 1, extra: B256::default() }
}

/// Build a `recently_executed_blocks` tip whose nonce encodes `round` (the EVM nonce packs `epoch
/// << 32 | round`).
fn tip_at_round(round: u32) -> SealedHeader {
    let exec_header = ExecHeader { nonce: (round as u64).into(), ..Default::default() };
    SealedHeader::new(exec_header, B256::default())
}

/// Regression: the execution-lag throttle reads the monotonic execution anchor, NOT the
/// `recently_executed_blocks` tip. A drained parked batch regresses the tip's round far below the
/// true execution frontier; reading it would compute a huge lag and wedge proposals forever (the
/// halt).
#[tokio::test]
async fn execution_lag_reads_anchor_not_regressed_tip() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    // Seed the proposer's round high enough that the 100-round lag threshold is meaningful.
    cb.committed_round_updates().send_replace(500);

    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        TaskManager::default().get_spawner(),
    );

    // Drained-parked-batch regression: the recently_executed_blocks tip carries a stale, low round
    // (200) while execution has actually reached the frontier (498).
    cb.recently_executed_blocks().send_modify(|b| b.push_latest(tip_at_round(200)));
    cb.executed_anchor().send_replace(anchor_at_round(498));

    // Consensus round 500, anchor 498 -> lag 2 (< 100): the proposer MUST NOT throttle. Reading the
    // regressed tip (200) would compute lag 300 and wedge the proposer - the bug the fix prevents.
    assert_eq!(
        proposer.execution_lag(),
        None,
        "anchor lag 2 must not throttle (tip would lag 300)"
    );

    // Genuine lag: anchor 100, consensus 500 -> lag 400 (> 100): throttle as designed.
    cb.executed_anchor().send_replace(anchor_at_round(100));
    assert_eq!(proposer.execution_lag(), Some(100));
}

#[tokio::test]
async fn test_equivocation_protection_after_restart() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    /* Old comments, note if test gets flakey:
     max_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
     min_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
    */
    // Spawn the proposer.
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let mut task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    cb.execution_replay_complete().send_replace(true);

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send parents
    let parents: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.payload().get(&digest), Some(&worker_id));
    assert!(header.validate(&committee).is_ok());

    // TODO: assert header el state present

    // restart the proposer.
    fixture.notify_shutdown();
    primary.consensus_config().shutdown().notify();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        task_manager.join(primary.consensus_config().shutdown().clone())
    )
    .await
    .is_ok());

    primary.consensus_config().shutdown().reset();

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let task_manager = TaskManager::default();
    let proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    proposer.spawn(&task_manager);

    cb.execution_replay_complete().send_replace(true);

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send a superset parents, same round but different set from before
    let parents: Vec<_> =
        fixture.headers().iter().take(4).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes the same header as before
    let new_header = rx_headers.recv().await.unwrap();
    if new_header.round() == header.round() {
        assert_eq!(header, new_header);
    }
}

#[tokio::test]
async fn test_retransmit_headers_on_gap() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();

    let proposed_headers_cases = [
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        // skip-then-commit: rounds 4 and 8 committed; 6 and 10 are the truly-uncommitted ones
        vec![4u32, 6u32, 8u32, 10u32],
    ];
    let commited_headers_cases = [
        vec![],
        vec![1u32, 3u32],
        vec![1u32, 4u32],
        vec![1u32, 4u32, 7u32],
        vec![5u32, 6u32, 7u32, 8u32],
        vec![10u32],
        vec![11u32],
        // skip-then-commit: 8 was committed but 6 (proposed before 8) was not
        vec![4u32, 8u32],
    ];
    let expected_proposed_headers_cases = [
        vec![4u32, 6u32, 8u32, 10u32],
        vec![4u32, 6u32, 8u32, 10u32],
        vec![6u32, 8u32, 10u32],
        // committed={1,4,7}: rounds at or above the horizon (8, 10) stay proposed - their
        // certificates are still collecting votes and the normal commit path handles them
        vec![8u32, 10u32],
        // committed={5,6,7,8}: 10 sits at the horizon's fresh side and stays proposed
        vec![10u32],
        vec![],
        vec![],
        // skip-then-commit: 4 and 8 removed as committed; 10 is fresh (>= horizon 8) and stays
        vec![10u32],
    ];
    let expected_digests_cases = [
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![FixedBytes::<32>::with_last_byte(6), FixedBytes::<32>::with_last_byte(100)],
        // committed={5,6,7,8}: 6 and 8 are removed as committed, only the straggler 4 (below
        // the horizon) retransmits; 10 stays proposed
        vec![FixedBytes::<32>::with_last_byte(4), FixedBytes::<32>::with_last_byte(100)],
        // committed={10}: 10 removed, 4/6/8 retransmitted
        vec![
            FixedBytes::<32>::with_last_byte(4),
            FixedBytes::<32>::with_last_byte(6),
            FixedBytes::<32>::with_last_byte(8),
            FixedBytes::<32>::with_last_byte(100),
        ],
        // committed={11}: no overlap with proposed, retransmit is triggered and all are re-queued
        vec![
            FixedBytes::<32>::with_last_byte(4),
            FixedBytes::<32>::with_last_byte(6),
            FixedBytes::<32>::with_last_byte(8),
            FixedBytes::<32>::with_last_byte(10),
            FixedBytes::<32>::with_last_byte(100),
        ],
        // skip-then-commit: 4 and 8 removed as committed (NOT re-queued); the superseded
        // round 6 requeues, the fresh round 10 stays proposed.
        vec![FixedBytes::<32>::with_last_byte(6), FixedBytes::<32>::with_last_byte(100)],
    ];

    for i in 0..proposed_headers_cases.len() {
        let proposed_headers = &proposed_headers_cases[i];
        let commited_headers = &commited_headers_cases[i];
        let expected_proposed_headers = &expected_proposed_headers_cases[i];
        let expected_digests = &expected_digests_cases[i];

        let mut proposer = Proposer::new(
            primary.consensus_config(),
            primary.consensus_config().authority_id().expect("authority"),
            ConsensusBus::new(),
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            TaskManager::default().get_spawner(),
        );

        for round in proposed_headers {
            let round_as_byte = *round as u8;
            let round_as_uint = *round;

            let mut payload = IndexMap::new();
            payload.insert(FixedBytes::<32>::with_last_byte(round_as_byte), 1u16);

            let header = Header::new(
                primary.id(),
                round_as_uint,
                1,
                payload,
                BTreeSet::new(),
                BlockNumHash::default(),
            );
            proposer.proposed_headers.insert(round_as_uint, header.clone());
        }
        proposer.digests.push_back(ProposerDigest {
            digest: FixedBytes::<32>::with_last_byte(100),
            worker_id: 1u16,
        });

        proposer.process_committed_headers(1, commited_headers.to_vec());

        let updated_digests =
            proposer.digests.iter().map(|digest| digest.digest).collect::<Vec<_>>();

        assert_eq!(
            proposer.proposed_headers.keys().copied().collect::<Vec<_>>(),
            *expected_proposed_headers
        );
        assert_eq!(updated_digests, expected_digests.clone());
    }
}

/// A header carrying exactly one digest, as the proposer would have built it.
fn header_with(author: AuthorityIdentifier, round: Round, digest: B256) -> Header {
    let mut payload = IndexMap::new();
    payload.insert(digest, 0u16);
    Header::new(author, round, 1, payload, BTreeSet::new(), BlockNumHash::default())
}

/// The digests currently queued in the proposer, front to back.
fn queued_digests<DB: Database>(proposer: &Proposer<DB>) -> Vec<B256> {
    proposer.digests.iter().map(|d| d.digest).collect()
}

/// Builds a proposer holding one proposed-but-uncommitted header at `round` carrying `digest`,
/// with one unrelated digest already queued.
fn proposer_with_pending_header(
    fixture: &CommitteeFixture<MemDatabase>,
    round: Round,
    digest: FixedBytes<32>,
) -> Proposer<MemDatabase> {
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        ConsensusBus::new(),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        TaskManager::default().get_spawner(),
    );

    proposer.proposed_headers.insert(round, header_with(primary.id(), round, digest));
    proposer
        .digests
        .push_back(ProposerDigest { digest: FixedBytes::<32>::with_last_byte(100), worker_id: 1 });
    proposer
}

/// The proposer queue never drops digests: dropping a quorum'd digest would gap the
/// per-authority seq stream and park every later batch from this authority. Growth past the
/// warn threshold only logs.
#[tokio::test]
async fn push_digest_never_drops() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(fixture.committee(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    // fill one past the warn threshold: every digest is retained
    for _ in 0..=DIGEST_QUEUE_WARN_THRESHOLD {
        proposer.push_digest(ProposerDigest { digest: B256::random(), worker_id: 0 });
    }
    assert_eq!(
        proposer.digests.len(),
        DIGEST_QUEUE_WARN_THRESHOLD + 1,
        "growth past the warn threshold is retained, not evicted"
    );
}

/// The fallback horizon (a commit carrying none of our own headers) must forgive ordinary
/// commit lag: a header a round or two behind the commit is routinely still committable, so
/// reproposing it manufactures a duplicate commit and parks its successor seqs. Fails against
/// the ungraced fallback, which requeues at one round of lag.
#[tokio::test]
async fn fallback_requeue_forgives_ordinary_commit_lag() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let header_digest = FixedBytes::<32>::with_last_byte(1);
    let mut proposer = proposer_with_pending_header(&fixture, 1, header_digest);

    // a foreign commit one round above the header: within the grace, nothing moves
    proposer.process_committed_headers(2, vec![]);

    assert_eq!(proposer.proposed_headers.keys().copied().collect::<Vec<_>>(), vec![1]);
    let digests: Vec<_> = proposer.digests.iter().map(|d| d.digest).collect();
    assert_eq!(digests, vec![FixedBytes::<32>::with_last_byte(100)]);
}

/// Beyond the grace the fallback must still rescue: a header this far behind foreign commits
/// is stranded (its votes were rejected), and only the requeue returns its quorum'd,
/// seq-consumed digests to circulation before the GC horizon.
#[tokio::test]
async fn fallback_requeue_still_rescues_beyond_grace() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let header_digest = FixedBytes::<32>::with_last_byte(1);
    let mut proposer = proposer_with_pending_header(&fixture, 1, header_digest);

    // a foreign commit past the grace: the header is stranded and its digests requeue in front
    let beyond_grace = 1 + super::recovery::FALLBACK_REQUEUE_GRACE_ROUNDS + 1;
    proposer.process_committed_headers(beyond_grace, vec![]);

    assert!(proposer.proposed_headers.is_empty());
    let digests: Vec<_> = proposer.digests.iter().map(|d| d.digest).collect();
    assert_eq!(digests, vec![header_digest, FixedBytes::<32>::with_last_byte(100)]);
}

/// The fallback requeue (no own header committed, keyed on the leader's commit round) stops at
/// that round: headers above it are fresh, their certificates are still collecting votes, and
/// the normal commit path handles them. Requeueing them manufactures a duplicate commit
/// (original + re-proposal) for zero rescue value; the execution registry absorbs it, but it is a
/// backstop, not a reason to create duplicates.
#[tokio::test]
async fn fallback_requeue_stops_at_the_commit_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        ConsensusBus::new(),
        LeaderSchedule::new(fixture.committee(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );

    let straggler = B256::random();
    let fresh_low = B256::random();
    let fresh_high = B256::random();
    proposer.proposed_headers.insert(91, header_with(primary.id(), 91, straggler));
    proposer.proposed_headers.insert(103, header_with(primary.id(), 103, fresh_low));
    proposer.proposed_headers.insert(105, header_with(primary.id(), 105, fresh_high));

    // a foreign-only commit at round 99: none of our headers are in it
    proposer.process_committed_headers(99, vec![]);

    assert_eq!(
        queued_digests(&proposer),
        vec![straggler],
        "only the straggler below the commit round is requeued"
    );
    assert_eq!(
        proposer.proposed_headers.keys().copied().collect::<Vec<_>>(),
        vec![103, 105],
        "headers at or above the commit round stay proposed - their certs are in flight"
    );
}

/// A quorum'd digest handed to the proposer is never silently discarded: on every path that
/// could lose one it is either in a proposed header or in the queue. GC eviction requeues
/// just like a superseded commit.
///
/// Load-bearing beyond the proposer: a dropped digest gaps the per-authority seq stream and
/// parks every later batch from this authority, and the in-flight TTL does not heal it - the
/// sweep re-seals those txs under a NEW seq, leaving the original gap.
#[tokio::test]
async fn digests_survive_gc_advance_and_later_round_commit() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let task_manager = TaskManager::default();
    let bus = ConsensusBus::new();
    let mut proposer = Proposer::new(
        primary.consensus_config(),
        primary.consensus_config().authority_id().expect("authority"),
        bus.clone(),
        LeaderSchedule::new(fixture.committee(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    // set explicitly so the test does not ride on fixture parameter defaults
    proposer.gc_depth = 10;
    proposer.round = 100;
    // the eviction horizon keys on the COMMITTED round: rounds <= 85 are evictable
    bus.committed_round_updates().send_replace(95);

    // a quorum'd digest that is queued but not yet proposed
    let queued = B256::random();
    proposer.push_digest(ProposerDigest { digest: queued, worker_id: 0 });

    // two proposed-but-uncommitted headers, both inside the retained window
    let early = B256::random();
    let late = B256::random();
    proposer.proposed_headers.insert(91, header_with(primary.id(), 91, early));
    proposer.proposed_headers.insert(99, header_with(primary.id(), 99, late));

    // GC runs after every successful proposal: it must not evict a header still inside the
    // window, and must never touch the queue of un-proposed digests.
    proposer.evict_old_proposed_headers();
    assert_eq!(
        proposer.proposed_headers.keys().copied().collect::<Vec<_>>(),
        vec![91, 99],
        "GC must retain headers above the gc horizon - their digests are still recoverable"
    );
    assert_eq!(
        queued_digests(&proposer),
        vec![queued],
        "GC must not touch the un-proposed digest queue"
    );

    // the LATER round commits while the earlier one never does: round 91's digest must come
    // back to the queue, FIFO-ahead of the still-queued one, rather than being dropped with
    // its header.
    proposer.process_committed_headers(99, vec![99]);
    assert!(proposer.proposed_headers.is_empty(), "committed and superseded rounds are drained");
    assert_eq!(
        queued_digests(&proposer),
        vec![early, queued],
        "the uncommitted round's digest is requeued (oldest first), never discarded"
    );

    // and a later GC sweep, with the round jumped far past every digest's origin round, still
    // leaves both queued.
    proposer.round = 500;
    proposer.evict_old_proposed_headers();
    assert_eq!(
        queued_digests(&proposer),
        vec![early, queued],
        "a round advance past the digests' rounds must not discard them"
    );

    // Commits trail the frontier: proposer at 500, committed at 460, so the eviction horizon
    // is 450 - NOT 490. Rounds 400/401 sit below it (the leaderless-partition case), 455 sits
    // in the commit-lag window (450, 490]: still committable by a future leader, so evicting
    // it would double-commit its batches once it lands. 495 is above every horizon.
    bus.committed_round_updates().send_replace(460);
    let old_low = B256::random();
    let old_high = B256::random();
    let lag_window = B256::random();
    let survivor = B256::random();
    proposer.proposed_headers.insert(400, header_with(primary.id(), 400, old_low));
    proposer.proposed_headers.insert(401, header_with(primary.id(), 401, old_high));
    proposer.proposed_headers.insert(455, header_with(primary.id(), 455, lag_window));
    proposer.proposed_headers.insert(495, header_with(primary.id(), 495, survivor));

    proposer.evict_old_proposed_headers();
    assert_eq!(
        proposer.proposed_headers.keys().copied().collect::<Vec<_>>(),
        vec![455, 495],
        "eviction keys on the committed round: the commit-lag window stays committable"
    );
    assert_eq!(
        queued_digests(&proposer),
        vec![old_low, old_high, early, queued],
        "gc-evicted headers requeue their digests oldest-round-first, ahead of the queue"
    );
}
