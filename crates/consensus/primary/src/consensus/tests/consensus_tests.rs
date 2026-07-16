//! Consensus tests

use crate::{
    consensus::{
        Bullshark, Consensus, ConsensusError, ConsensusMetrics, ConsensusState, LeaderSchedule,
        LeaderSwapTable,
    },
    test_utils::make_optimal_certificates,
    ConsensusBus, NodeMode,
};
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore, ConsensusStore};
use rayls_infrastructure_types::{
    Certificate, ExecHeader, Hash as _, RaylsReceiver, RaylsSender, ReputationScores, SealedHeader,
    TaskManager, B256, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{collections::BTreeSet, sync::Arc};

/// This test is trying to compare the output of the Consensus algorithm when:
/// (1) running without any crash for certificates processed from round 1 to 5 (inclusive)
/// (2) when a crash happens with last commit at round 2, and then consensus recovers
///
/// The output of (1) is compared to the output of (2) . The output of (2) is the combination
/// of the output before the crash and after the crash. What we expect to see is the output of
/// (1) & (2) be exactly the same. That will ensure:
/// * no certificates re-commit happens
/// * no certificates are skipped
/// * no forks created
#[tokio::test]
async fn test_consensus_recovery_with_bullshark() {
    // GIVEN
    let num_sub_dags_per_schedule = 3;

    // AND Setup consensus
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config().clone();
    let consensus_store = config.node_storage().clone();
    let certificate_store = config.node_storage().clone();

    // config.set_consensus_bad_nodes_stake_threshold(33);

    // AND make certificates for rounds 1 to 7 (inclusive)
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _next_parents) =
        make_optimal_certificates(&committee, 1..=7, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        leader_schedule.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager);

    // WHEN we feed all certificates to the consensus.
    for certificate in certificates.iter() {
        // we store the certificates so we can enable the recovery
        // mechanism later.
        certificate_store.write(certificate.clone()).unwrap();
        cb.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // THEN we expect to have 2 leader election rounds (round = 2, and round = 4).
    // In total we expect to have the following certificates get committed:
    // * 4 certificates from round 1
    // * 4 certificates from round 2
    // * 4 certificates from round 3
    // * 4 certificates from round 4
    // * 4 certificates from round 5
    // * 1 certificates from round 6 (the leader of last round)
    //
    // In total we should see 21 certificates committed
    let mut consensus_index_counter = 2;

    // hold all the certificates that get committed when consensus runs
    // without any crash.
    let mut committed_output_no_crash: Vec<Certificate> = Vec::new();
    let mut score_no_crash: ReputationScores = ReputationScores::default();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_no_crash = sub_dag.reputation_score.clone();
        assert_eq!(sub_dag.leader.round(), consensus_index_counter);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());
        for output in sub_dag.certificates {
            assert!(output.round() <= 6);

            committed_output_no_crash.push(output.clone());

            // we received the leader of round 6, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 6 {
                break 'main;
            }
        }
        consensus_index_counter += 2;
    }

    // AND the last committed store should be updated correctly
    let last_committed = consensus_store.read_last_committed(config.epoch());

    for id in ids.clone() {
        let last_round = *last_committed.get(&id).unwrap();

        // For the leader of round 6 we expect to have last committed round of 6.
        if id == leader_schedule.leader(6).id() {
            assert_eq!(last_round, 6);
        } else {
            // For the others should be 5.
            assert_eq!(last_round, 5);
        }
    }

    // AND shutdown consensus
    task_manager.abort();
    drop(task_manager);

    certificate_store.clear().unwrap();
    consensus_store.clear_consensus_chain_for_test();

    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        consensus_store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager);

    // WHEN we send same certificates but up to round 3 (inclusive)
    // Then we store all the certificates up to round 6 so we can let the recovery algorithm
    // restore the consensus.
    // We omit round 7 so we can feed those later after "crash" to trigger a new leader
    // election round and commit.
    for certificate in certificates.iter() {
        if certificate.header().round() <= 3 {
            cb.new_certificates().send(certificate.clone()).await.unwrap();
        }
        if certificate.header().round() <= 6 {
            certificate_store.write(certificate.clone()).unwrap();
        }
    }

    // THEN we expect to commit with a leader of round 2.
    // So in total we expect to have committed certificates:
    // * 4 certificates of round 1
    // * 1 certificate of round 2 (the leader)
    let mut consensus_index_counter = 2;
    let mut committed_output_before_crash: Vec<Certificate> = Vec::new();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        assert_eq!(sub_dag.leader.round(), consensus_index_counter);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());
        for output in sub_dag.certificates {
            assert!(output.round() <= 2);

            committed_output_before_crash.push(output.clone());

            // we received the leader of round 2, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 2 {
                break 'main;
            }
        }
        consensus_index_counter += 2;
    }

    // AND shutdown (crash) consensus
    task_manager.abort();
    drop(task_manager);

    let bad_nodes_stake_threshold = 0;
    let bullshark = Bullshark::new(
        committee.clone(),
        metrics.clone(),
        num_sub_dags_per_schedule,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        bad_nodes_stake_threshold,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
    let task_manager = TaskManager::default();
    Consensus::spawn(config, &cb, bullshark, &task_manager);

    // WHEN send the certificates of round >= 5 to trigger a leader election for round 4
    // and start committing.
    for certificate in certificates.iter() {
        if certificate.header().round() >= 5 {
            cb.new_certificates().send(certificate.clone()).await.unwrap();
        }
    }

    // AND capture the committed output
    let mut committed_output_after_crash: Vec<Certificate> = Vec::new();
    let mut score_with_crash: ReputationScores = ReputationScores::default();

    'main: while let Some(sub_dag) = rx_output.recv().await {
        score_with_crash = sub_dag.reputation_score.clone();
        assert_eq!(score_with_crash.total_authorities(), 4);
        consensus_store.write_subdag_for_test(consensus_index_counter as u64, sub_dag.clone());

        for output in sub_dag.certificates {
            assert!(output.round() >= 2);

            committed_output_after_crash.push(output.clone());

            // we received the leader of round 6, now stop as we don't expect to see any other
            // certificate from that or higher round.
            if output.round() == 6 {
                break 'main;
            }
        }
    }

    // THEN compare the output from a non-Crashed consensus to the outputs produced by the
    // crash consensus events. Those two should be exactly the same and will ensure that we see:
    // * no certificate re-commits
    // * no skips
    // * no forks
    committed_output_before_crash.append(&mut committed_output_after_crash);

    let all_output_with_crash = committed_output_before_crash;

    assert_eq!(committed_output_no_crash, all_output_with_crash);

    // AND ensure that scores are exactly the same
    assert_eq!(score_with_crash.scores_per_authority.len(), 4);
    assert_eq!(score_with_crash, score_no_crash);
    assert_eq!(
        score_with_crash.scores_per_authority.into_iter().filter(|(_, score)| *score == 1).count(),
        4
    );
}

/// Dev-mode smoke test: a single-validator committee must still drive Bullshark
/// to commit a sub-dag (a "block").
///
/// This guards the n=1 path enabled by relaxing the committee-size assertion (the
/// foundation of `--dev` single-node mode). The single authority is its own quorum
/// (quorum = validity = 1), so feeding a linear DAG must commit the round-2 leader,
/// exactly as the multi-validator path does. The `timeout` makes a non-committing
/// regression fail fast instead of hanging forever.
#[cfg(feature = "dev-single-node-setup")]
#[tokio::test]
async fn single_validator_commits_a_subdag() {
    use std::{num::NonZeroUsize, time::Duration};
    use tokio::time::timeout;

    let num_sub_dags_per_schedule = 3;

    // A committee of exactly one authority.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(1).unwrap())
        .build();
    let committee = fixture.committee();
    assert_eq!(committee.size(), 1, "fixture must have a single validator");

    let config = fixture.authorities().next().unwrap().consensus_config().clone();
    let store = config.node_storage().clone();

    // Linear DAG of single-authority certificates for rounds 1..=4 (round 3 is
    // enough to commit the round-2 leader).
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _next_parents) =
        make_optimal_certificates(&committee, 1..=4, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let leader_schedule = LeaderSchedule::from_store(
        committee.clone(),
        store.clone(),
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let bullshark = Bullshark::new(
        committee.clone(),
        metrics,
        num_sub_dags_per_schedule,
        leader_schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );

    let cb = ConsensusBus::new();
    let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut rx_output = cb.sequence().subscribe();
    let task_manager = TaskManager::default();
    Consensus::spawn(config.clone(), &cb, bullshark, &task_manager);

    // Feed every certificate to consensus.
    for certificate in certificates.iter() {
        store.write(certificate.clone()).unwrap();
        cb.new_certificates().send(certificate.clone()).await.unwrap();
    }

    // The single validator must commit: the first sub-dag is the round-2 leader
    // and must carry at least one certificate (a block).
    let sub_dag = timeout(Duration::from_secs(10), rx_output.recv())
        .await
        .expect("single-validator consensus did not commit within 10s")
        .expect("consensus output channel closed before committing");

    assert_eq!(sub_dag.leader.round(), 2, "first committed leader should be round 2");
    assert!(
        !sub_dag.certificates.is_empty(),
        "committed sub-dag must contain at least one certificate"
    );

    task_manager.abort();
}

/// MissingParent[Round] raises the promotion barrier, latches CvvInactive,
/// and returns ShuttingDown.
#[tokio::test]
async fn test_missing_parent_demotes_to_cvv_inactive() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config().clone();

    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=4, &genesis, &ids);

    let metrics = Arc::new(ConsensusMetrics::default());
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let mut bullshark = Bullshark::new(
        committee.clone(),
        metrics.clone(),
        3,
        schedule,
        DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    );
    let mut state = ConsensusState::new(metrics.clone(), 50);

    // populate DAG with rounds 1 and 2 only; the gap at round 3 makes any
    // round-4 cert's parent lookup fail with MissingParentRound
    for cert in certificates.iter().filter(|c| c.round() <= 2) {
        bullshark.process_certificate(&mut state, cert.clone()).unwrap();
    }
    let round_4_cert = certificates.iter().find(|c| c.round() == 4).cloned().unwrap();
    let failing_round = round_4_cert.round();

    let cb = ConsensusBus::new();
    cb.node_mode().send_replace(NodeMode::CvvActive);
    let rx_shutdown = config.shutdown().subscribe();
    let shutdown_observer = config.shutdown().subscribe();

    let mut consensus = Consensus {
        committee: committee.clone(),
        consensus_bus: cb.clone(),
        consensus_config: config.clone(),
        rx_shutdown,
        protocol: bullshark,
        metrics,
        state,
        active: true,
    };

    let result = consensus.new_certificate(round_4_cert).await;

    assert!(
        matches!(result, Err(ConsensusError::ShuttingDown)),
        "expected ShuttingDown, got {result:?}",
    );
    let barrier = cb.promotion_barrier().borrow().clone();
    assert_eq!(
        barrier.as_ref().map(|b| b.round),
        Some(failing_round),
        "promotion_barrier round not raised to failing round",
    );
    assert!(
        barrier.as_ref().map_or(true, |b| b.digest.is_none()),
        "digest should be None for MissingParentRound",
    );
    assert_eq!(
        barrier.as_ref().map(|b| b.epoch),
        Some(committee.epoch()),
        "promotion_barrier not tagged with the current epoch",
    );
    assert_eq!(
        *cb.mode_transition().borrow(),
        Some(NodeMode::CvvInactive),
        "mode_transition not latched to CvvInactive",
    );
    assert!(shutdown_observer.noticed(), "consensus_config shutdown not fired");
}

/// Verify construct_dag_from_cert_store drops orphan certs whose parents
/// are absent (models the sparse store left by commit_catchup_output).
#[tokio::test]
async fn test_orphan_certs_pruned_on_reconstruction() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config().clone();
    let cert_store = config.node_storage().clone();

    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=10, &genesis, &ids);

    // Persist rounds 1..=4 whole (fully parent-closed base).
    // At round 5, omit one authority's cert - because make_optimal_certificates
    // wires every round-N+1 cert to ALL round-N certs, this single omission
    // makes every round-6+ cert an orphan once the R5 parent link is severed.
    // Rounds 7..=10 are written in full so the reconstruction faces
    // cascading orphans that need a fix-point pass to prune.
    let target_origin = ids[0].clone();
    let mut written_digests: BTreeSet<_> = BTreeSet::new();
    for cert in certificates.iter() {
        let round = cert.round();
        if round == 5 && cert.origin() == &target_origin {
            continue;
        }
        cert_store.write(cert.clone()).unwrap();
        written_digests.insert((cert.round(), cert.origin().clone()));
    }

    let metrics = Arc::new(ConsensusMetrics::default());
    let gc_depth = 50;
    let state = ConsensusState::new_from_store(
        metrics,
        0,
        gc_depth,
        Default::default(),
        None,
        cert_store,
        committee.epoch(),
    );

    // Every remaining cert above gc_round+1 must have every parent digest
    // present in dag[round-1].
    let gc_round = state.last_round.gc_round;
    for (&round, round_map) in state.dag.iter() {
        if round <= gc_round + 1 {
            continue;
        }
        let parent_digests: BTreeSet<_> = state
            .dag
            .get(&(round - 1))
            .map(|prev| prev.values().map(|(d, _)| *d).collect())
            .unwrap_or_default();
        for (origin, (digest, cert)) in round_map.iter() {
            for parent in cert.header().parents() {
                assert!(
                    parent_digests.contains(parent),
                    "orphan cert survived reconstruction: round={round} origin={origin} \
                     digest={digest:?} missing parent={parent:?}",
                );
            }
        }
    }

    // The omitted round-5 cert must be absent.
    let round5 = state.dag.get(&5).expect("round 5 partially present");
    assert!(
        !round5.contains_key(&target_origin),
        "omitted round-5 cert unexpectedly reconstructed",
    );

    // Round 5 still has quorum (3/4) from the other authorities.
    assert_eq!(round5.len(), ids.len() - 1);

    // Rounds 6..=10 reference all 4 round-5 certs, so each of those certs is
    // missing exactly one parent (the omitted one) and the fix-point pass
    // must drop every single one of them.
    for round in 6..=10 {
        assert!(
            state.dag.get(&round).map(|m| m.is_empty()).unwrap_or(true),
            "round {round} survived but should be fully orphaned: {:?}",
            state.dag.get(&round),
        );
    }
}

/// Verify a DAG reconstructed from a sparse cert store matches the DAG
/// built by the live path (divergence would cause a block-hash fork).
#[tokio::test]
async fn test_reconstructed_dag_matches_live_dag() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let config = fixture.authorities().next().unwrap().consensus_config().clone();
    let cert_store = config.node_storage().clone();

    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (certificates, _) = make_optimal_certificates(&committee, 1..=8, &genesis, &ids);

    // Same omission pattern as the first test: drop one round-3 cert to
    // inject an orphan cascade. Rounds 1-2 are fully present; round 3 has
    // one authority missing; rounds 4+ reference the missing cert via parent.
    let target_origin = ids[0].clone();

    // Build the live DAG: feed certs one-by-one via try_insert with
    // check_parents=true. Orphans error and are silently discarded,
    // mirroring how the live synchronizer rejects certs with missing parents.
    let metrics = Arc::new(ConsensusMetrics::default());
    let gc_depth = 50;
    let mut live_state = ConsensusState::new(metrics.clone(), gc_depth);
    for cert in certificates.iter() {
        let round = cert.round();
        if round == 3 && cert.origin() == &target_origin {
            continue;
        }
        // drop the Err case - a cert with a missing parent is simply not
        // accepted, matching the synchronizer's check_parents behavior.
        let _ = live_state.try_insert(cert);
    }

    // Build the reconstructed DAG: persist the same cert subset to the cert
    // store, then reconstruct via new_from_store.
    for cert in certificates.iter() {
        let round = cert.round();
        if round == 3 && cert.origin() == &target_origin {
            continue;
        }
        cert_store.write(cert.clone()).unwrap();
    }
    let recovered_state = ConsensusState::new_from_store(
        metrics,
        0,
        gc_depth,
        Default::default(),
        None,
        cert_store,
        committee.epoch(),
    );

    // The two dags must have the same round set and the same
    // (authority -> digest) mapping per round.
    let live_rounds: Vec<_> = live_state.dag.keys().copied().collect();
    let recovered_rounds: Vec<_> = recovered_state.dag.keys().copied().collect();
    assert_eq!(
        live_rounds, recovered_rounds,
        "round sets diverge: live={live_rounds:?} recovered={recovered_rounds:?}",
    );

    for (round, live_map) in live_state.dag.iter() {
        let recovered_map = recovered_state
            .dag
            .get(round)
            .unwrap_or_else(|| panic!("round {round} present in live but missing in recovered"));

        let live_keys: BTreeSet<_> = live_map.keys().collect();
        let recovered_keys: BTreeSet<_> = recovered_map.keys().collect();
        assert_eq!(live_keys, recovered_keys, "round {round} authority set diverges",);

        for (origin, (live_digest, _)) in live_map.iter() {
            let (recovered_digest, _) = recovered_map.get(origin).unwrap();
            assert_eq!(
                live_digest, recovered_digest,
                "round {round} origin {origin} digest diverges",
            );
        }
    }
}
