//! Leader schedule tests

use crate::consensus::{Dag, LeaderSchedule, LeaderSwapTable};
use rayls_consensus_primary::test_utils::mock_certificate;
use rayls_infrastructure_storage::{mem_db::MemDatabase, open_db, ConsensusStore};
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, CommittedSubDag, Database as _, ReputationScores, Round,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize};
use tempfile::TempDir;

#[tokio::test]
async fn test_leader_swap_table() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Adding some scores
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // Only one bad authority should be calculated since all have equal stake
    assert_eq!(table.bad_nodes.len(), 1);

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 1 {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        } else {
            assert!(table.swap(id, index as Round).is_none());
        }
    }

    // Now we create a larger committee with more score variation - still all the authorities have
    // equal stake.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Adding some scores
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    // We expect the first 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    assert_eq!(table.bad_nodes.len(), 2);
    assert!(table.bad_nodes.contains_key(&authority_ids[0]));
    assert!(table.bad_nodes.contains_key(&authority_ids[1]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 2 {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        } else {
            assert!(table.swap(id, index as Round).is_none());
        }
    }

    // Adding some scores
    // Add three bad nodes, make sure only bad nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, if i < 7 { 10 } else { 2 });
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes same score so all "good"
    assert_eq!(table.good_nodes.len(), 7);
    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[7]));
    assert!(table.bad_nodes.contains_key(&authority_ids[8]));
    assert!(table.bad_nodes.contains_key(&authority_ids[9]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 7 {
            assert!(table.swap(id, index as Round).is_none());
        } else {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        }
    }

    // Adding some scores
    // Add one bad nodes, make sure only bad nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, if i < 9 { 10 } else { 0 });
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes same score so all "good"
    assert_eq!(table.good_nodes.len(), 9);
    assert_eq!(table.bad_nodes.len(), 1);
    assert!(table.bad_nodes.contains_key(&authority_ids[9]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 9 {
            assert!(table.swap(id, index as Round).is_none());
        } else {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        }
    }

    // Adding some scores
    // Add three bad nodes with varied scores, make sure only bad nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    let mut bad_score = 4;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(
            id,
            if i < 7 {
                10
            } else {
                bad_score -= 1;
                bad_score
            },
        );
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes same score so all "good"
    assert_eq!(table.good_nodes.len(), 7);
    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[7]));
    assert!(table.bad_nodes.contains_key(&authority_ids[8]));
    assert!(table.bad_nodes.contains_key(&authority_ids[9]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 7 {
            assert!(table.swap(id, index as Round).is_none());
        } else {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        }
    }

    // Adding some scores
    // Add three bad nodes with varied scores, varied good nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    let mut bad_score = 4;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(
            id,
            if i < 7 {
                match i {
                    0 => 9,
                    1 => 10,
                    2 => 8,
                    3 => 9,
                    4 => 7,
                    5 => 6,
                    6 => 10,
                    _ => panic!("invalid index"),
                }
            } else {
                bad_score -= 1;
                bad_score
            },
        );
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes excpet one are "good", the score 6 is Ok but not able to swap for a bad.
    assert_eq!(table.good_nodes.len(), 6);
    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[7]));
    assert!(table.bad_nodes.contains_key(&authority_ids[8]));
    assert!(table.bad_nodes.contains_key(&authority_ids[9]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 7 {
            assert!(table.swap(id, index as Round).is_none());
        } else {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        }
    }

    // Adding some scores
    // Add two bad nodes with varied scores, varied good nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    let mut bad_score = 2;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(
            id,
            if i < 8 {
                match i {
                    0 => 9,
                    1 => 6,
                    2 => 8,
                    3 => 9,
                    4 => 5,
                    5 => 6,
                    6 => 10,
                    7 => 9,
                    _ => panic!("invalid index"),
                }
            } else {
                bad_score -= 1;
                bad_score
            },
        );
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes except two are "good", the score 6 or less is Ok but not able to swap for a
    // bad.
    assert_eq!(table.good_nodes.len(), 5);
    assert_eq!(table.bad_nodes.len(), 2);
    assert!(table.bad_nodes.contains_key(&authority_ids[8]));
    assert!(table.bad_nodes.contains_key(&authority_ids[9]));

    // now first three should be swapped, whereas the others should not return anything
    for (index, id) in authority_ids.iter().enumerate() {
        if index < 8 {
            assert!(table.swap(id, index as Round).is_none());
        } else {
            let s = table.swap(id, index as Round).unwrap();

            // make sure that the returned node is amongst the good nodes
            assert!(table.good_nodes.contains(&s));
        }
    }

    // Adding some scores
    // Add two bad nodes with varied scores, varied good nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(
            id,
            match i {
                0 => 1,
                1 => 0,
                2 => 8,
                3 => 9,
                4 => 5,
                5 => 2,
                6 => 10,
                7 => 9,
                8 => 6,
                9 => 6,
                _ => panic!("invalid index"),
            },
        );
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes except two are "good", the score 6 or less is Ok but not able to swap for a
    // bad.
    assert_eq!(table.good_nodes.len(), 4);
    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[0]));
    assert!(table.bad_nodes.contains_key(&authority_ids[1]));
    assert!(table.bad_nodes.contains_key(&authority_ids[5]));
    // Tested the swap function a bunch already.

    // Adding some scores
    // Add two bad nodes with varied scores, varied good nodes.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (i, id) in authority_ids.iter().enumerate() {
        scores.add_score(
            id,
            match i {
                0 => 3,
                1 => 2,
                2 => 8,
                3 => 9,
                4 => 5,
                5 => 2,
                6 => 10,
                7 => 9,
                8 => 6,
                9 => 6,
                _ => panic!("invalid index"),
            },
        );
    }

    // We expect the 3 authorities (f) to be amongst the bad nodes
    let table = LeaderSwapTable::new(&committee, 2, &scores, 33);

    // All other nodes except two are "good", the score 6 or less is Ok but not able to swap for a
    // bad.
    assert_eq!(table.good_nodes.len(), 4);
    assert_eq!(table.bad_nodes.len(), 3);
    assert!(table.bad_nodes.contains_key(&authority_ids[0]));
    assert!(table.bad_nodes.contains_key(&authority_ids[1]));
    assert!(table.bad_nodes.contains_key(&authority_ids[5]));
    // Tested the swap function a bunch already.
}

#[tokio::test]
async fn test_leader_schedule() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    // protocol_config.set_consensus_bad_nodes_stake_threshold(33);

    // the authority ids
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // Create a leader schedule with a default swap table, so no authority will be swapped.
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());

    // Call the leader for round 2. It should give us the validator of position 0
    let original_leader = authority_ids.first().unwrap().clone();
    let leader_2 = schedule.leader(2);

    assert_eq!(leader_2.id(), original_leader);

    // Now update the scores to consider the authority of position 0 as slow
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in authority_ids.iter().enumerate() {
        scores.add_score(id, score as u64);
    }

    // Update the schedule
    let table = LeaderSwapTable::new(
        &committee, 2, &scores, // protocol_config.consensus_bad_nodes_stake_threshold(),
        33,
    );
    schedule.update_leader_swap_table(table.clone());

    // Now call the leader for round 2 again. It should be swapped with another node
    let leader_2 = schedule.leader(2);

    // The returned leader should not be the one of position 0
    assert_ne!(leader_2.id(), original_leader);

    // The returned leader should be the one returned by the swap table when using the updated
    // leader scores.
    let swapped_leader = table.swap(&original_leader, 2).unwrap().id();
    assert_eq!(leader_2.id(), table.swap(&original_leader, 2).unwrap().id());

    // Now create an empty DAG
    let mut dag = Dag::new();

    // Now try to retrieve the leader's certificate
    let (leader_authority, leader_certificate) = schedule.leader_certificate(2, &dag);
    assert_eq!(leader_authority.id(), swapped_leader);
    assert!(leader_certificate.is_none());

    // Populate the leader's certificate and try again
    let (digest, certificate) =
        mock_certificate(&committee, leader_authority.id(), 2, BTreeSet::new());
    dag.entry(2).or_default().insert(leader_authority.id(), (digest, certificate.clone()));

    let (leader_authority, leader_certificate_result) = schedule.leader_certificate(2, &dag);
    assert_eq!(leader_authority.id(), swapped_leader);
    assert_eq!(certificate, leader_certificate_result.unwrap().clone());
}

/// TODO: this test is failing - I think it's due to bad stake threshold
#[tokio::test]
async fn test_leader_schedule_from_store() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let authority_ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Create a leader schedule with a default swap table, so no authority will be swapped and find
    // the leader at position 2. We expect the leader of round 2 to be the authority of position
    // 0 , since round robin is used in tests.
    let schedule = LeaderSchedule::new(committee.clone(), LeaderSwapTable::default());
    let leader_2 = schedule.leader(2);
    assert_eq!(leader_2.id(), authority_ids[0]);

    // AND we add a commit with a final score where the validator 0 is expected to be the
    // lowest score one.
    let mut scores = ReputationScores::new(&committee);
    scores.final_of_schedule = true;
    for (score, id) in fixture.authorities().map(|a| a.id()).enumerate() {
        scores.add_score(&id, score as u64);
    }

    let sub_dag = CommittedSubDag::new(vec![], Certificate::default(), 0, scores, None);

    store.write_subdag_for_test(0, sub_dag);
    store.persist().await.unwrap();

    // WHEN
    let schedule = LeaderSchedule::from_store(committee, store, 33);

    // THEN the stored schedule should be returned and eventually the low score leader should be
    // swapped with a high score one.
    let new_leader_2 = schedule.leader(2);

    assert_ne!(leader_2.id(), new_leader_2.id());
}

/// A consensus header's `reputation_score` is not covered by the header digest
/// (`CommittedSubDag::digest` skips it). A peer that serves a header by number or by hash, or
/// that answers the latest-block probe, can therefore set it to anything while keeping a hash a
/// following node accepts. `LeaderSwapTable::new` divides by `auths_by_score.len()` without
/// guarding zero, so an empty `scores_per_authority` with `final_of_schedule = true`, which no
/// honest node ever produces, panics the node. This is the crash primitive behind the persisted
/// crash-loop in the test below.
#[test]
#[should_panic(expected = "divide by zero")]
fn tampered_final_scores_divide_by_zero_in_leader_swap_table() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // What a peer can serve: marked final (so it is used as the schedule's scores) with no
    // authorities (so the standard-deviation step divides by zero).
    let poisoned =
        ReputationScores { scores_per_authority: Default::default(), final_of_schedule: true };

    let _ = LeaderSwapTable::new(&committee, 2, &poisoned, 33);
}

/// End to end on the stored row: a following node that caches a peer's consensus header keeps the
/// peer's `reputation_score`, and that row is promoted to `ConsensusBlocks`. On the next start,
/// `LeaderSchedule::from_store` reads the newest final-of-schedule row and builds the swap table
/// from it. A poisoned row makes startup panic, and since the row is persisted the node panics on
/// every restart until an operator clears its consensus database.
#[test]
#[should_panic(expected = "divide by zero")]
fn stored_tampered_final_scores_crash_leader_schedule_from_store() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let store = MemDatabase::default();

    // A committed sub-dag at the committee's epoch, marked final, with an empty score map: what a
    // peer serves and a following node stores after caching the header.
    let leader = Certificate::genesis(&committee).into_iter().next().unwrap();
    let poisoned =
        ReputationScores { scores_per_authority: Default::default(), final_of_schedule: true };
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, poisoned, None);
    store.write_subdag_for_test(1, sub_dag);

    // Startup rebuild of the leader schedule reads that row and panics.
    let _ = LeaderSchedule::from_store(committee, store, 33);
}
