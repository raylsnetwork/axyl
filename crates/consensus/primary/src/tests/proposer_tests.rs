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
        vec![],
        vec![],
        vec![],
        vec![],
        // 4 and 8 removed as committed; 6 and 10 re-queued by the retransmit loop
        vec![],
    ];
    let expected_digests_cases = [
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![FixedBytes::<32>::with_last_byte(100)],
        vec![
            FixedBytes::<32>::with_last_byte(6),
            FixedBytes::<32>::with_last_byte(8),
            FixedBytes::<32>::with_last_byte(10),
            FixedBytes::<32>::with_last_byte(100),
        ],
        // committed={5,6,7,8}: 6 and 8 are removed as committed, only 4 and 10 retransmitted
        vec![
            FixedBytes::<32>::with_last_byte(4),
            FixedBytes::<32>::with_last_byte(10),
            FixedBytes::<32>::with_last_byte(100),
        ],
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
        // skip-then-commit: 4 and 8 removed as committed (NOT re-queued); only the
        // truly-uncommitted rounds 6 and 10 are re-queued by the retransmit loop.
        vec![
            FixedBytes::<32>::with_last_byte(6),
            FixedBytes::<32>::with_last_byte(10),
            FixedBytes::<32>::with_last_byte(100),
        ],
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

        proposer
            .process_committed_headers(1, commited_headers.iter().map(|r| (*r, false)).collect());

        let updated_digests =
            proposer.digests.iter().map(|digest| digest.digest).collect::<Vec<_>>();

        assert_eq!(
            proposer.proposed_headers.keys().copied().collect::<Vec<_>>(),
            *expected_proposed_headers
        );
        assert_eq!(updated_digests, expected_digests.clone());
    }
}
