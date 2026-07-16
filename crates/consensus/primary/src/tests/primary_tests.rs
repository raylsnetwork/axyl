//! Primary tests

use crate::{
    error::PrimaryNetworkError,
    network::{handler::RequestHandler, MissingCertificatesRequest, PrimaryResponse},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use rayls_consensus_primary::test_utils::make_optimal_signed_certificates;
use rayls_execution_evm::test_utils::fixture_batch_with_transactions;
use rayls_infrastructure_network_types::MockPrimaryToWorkerClient;
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore, PayloadStore};
use rayls_infrastructure_types::{
    encode, error::HeaderError, now, AuthorityIdentifier, BlockNumHash, Certificate, Committee,
    ExecHeader, Hash as _, SealedHeader, SignatureVerificationState, TaskManager,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_too_new() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_parent_num_hash = dummy_parent.num_hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(target.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();

    // Create a test header.
    // Note: gc_depth defaults to 500, so with committed_round=2, max_round=502.
    // We need round > 502 to trigger TooNew error.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(600) // Must be > committed_round (2) + gc_depth (500) = 502
        .latest_execution_block(dummy_parent_num_hash) // Use correct hash from dummy parent
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    cb.committed_round_updates().send_replace(2);
    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(
        matches!(result, Err(PrimaryNetworkError::InvalidHeader(HeaderError::TooNew { .. }))),
        "{result:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(target.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(BlockNumHash::default()) // dummy_hash would be correct here but this is the test...
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(result.is_err(), "{result:?}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_older_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    // This will be an "older" execution block, test this still works.
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let mut dummy = ExecHeader { nonce: 110_u64.into(), ..Default::default() };
    dummy.nonce = 110_u64.into();
    cb.recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(SealedHeader::seal_slow(dummy)));
    dummy = ExecHeader { nonce: 120_u64.into(), ..Default::default() };
    cb.recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(SealedHeader::seal_slow(dummy)));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(target.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    cb.committed_round_updates().send_replace(2);
    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(target.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(2)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    cb.committed_round_updates().send_replace(1);
    // handler should report missing parent certificates to caller.
    let missing = if let PrimaryResponse::MissingParents(missing) =
        handler.vote(author_peer, test_header.clone(), Vec::new()).await.unwrap()
    {
        missing
    } else {
        panic!("Response not missing!");
    };

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // retry with 0 parents re-issues MissingParents (certifier may have lost state)
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header.clone(), Vec::new()))
            .await;
    assert!(
        matches!(result, Ok(Ok(PrimaryResponse::MissingParents(_)))),
        "expected MissingParents re-issue on retry with 0 parents, got: {result:?}"
    );

    // same behavior even with advanced round threshold — TooOld applies after parents supplied
    cb.primary_round_updates().send_replace(100);
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    assert!(
        matches!(result, Ok(Ok(PrimaryResponse::MissingParents(_)))),
        "expected MissingParents re-issue, got: {result:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_accept_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(target.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());

    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_1_certs = all_certificates[..NUM_PARENTS].to_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Populate all round 1 certificates and some round 2 certificates into the storage.
    // The new header will have some round 2 certificates missing as parents, but these parents
    // should be able to get accepted.
    for cert in round_1_certs {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    // Populate new header payload so they don't have to be retrieved.
    for (digest, worker_id) in test_header.payload() {
        payload_store.write_payload(digest, worker_id).unwrap();
    }

    cb.committed_round_updates().send_replace(2);
    // handler should report missing parent certificates to caller.
    let missing = if let PrimaryResponse::MissingParents(missing) =
        handler.vote(author_peer, test_header.clone(), Vec::new()).await.unwrap()
    {
        missing
    } else {
        panic!("Response not missing!");
    };

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // handler should process missing parent certificates and succeed.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, round_2_missing))
            .await
            .unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn test_request_vote_missing_batches() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let authority_id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();
    let consensus_config = primary.consensus_config();
    let certificate_store = consensus_config.node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(primary.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    cb.committed_round_updates().send_replace(1);
    // Verify Handler synchronizes missing batches and generates a Vote.
    let _vote = timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new()))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_request_vote_missing_batches_withheld_payload_does_not_vote() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let authority_id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let consensus_config = primary.consensus_config();
    let certificate_store = consensus_config.node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(primary.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    // Intentionally do NOT register a worker handler that can serve the header payload.
    // This simulates a peer that advertises the header but withholds the referenced batches.
    cb.committed_round_updates().send_replace(1);

    let response =
        timeout(Duration::from_secs(3), handler.vote(author_peer, test_header.clone(), Vec::new()))
            .await;

    match response {
        // The important invariant is that the request must not successfully produce a vote
        // while the payload remains unavailable.
        Ok(Ok(PrimaryResponse::Vote(vote))) => {
            panic!("unexpectedly voted for header with unavailable payload: {:?}", vote.digest())
        }
        Ok(Ok(other)) => {
            panic!("unexpected successful response while payload is withheld: {other:?}")
        }
        Ok(Err(_)) | Err(_) => {}
    }

    // And the payload should still be unavailable locally after the failed/blocked vote attempt.
    for (digest, worker_id) in test_header.payload() {
        assert!(!payload_store.contains_payload(*digest, *worker_id).unwrap());
    }
}

#[tokio::test]
async fn test_request_vote_already_voted() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(primary.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    // Verify Handler generates a Vote.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    cb.committed_round_updates().send_replace(1);
    let vote = if let PrimaryResponse::Vote(vote) = tokio::time::timeout(
        Duration::from_secs(10),
        handler.vote(author_peer, test_header.clone(), Vec::new()),
    )
    .await
    .unwrap()
    .unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };

    // Verify the same request gets the same vote back successfully.
    let vote2 = if let PrimaryResponse::Vote(vote) =
        handler.vote(author_peer, test_header, Vec::new()).await.unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };
    assert_eq!(vote.digest(), vote2.digest());

    // Verify a different request for the same round receives an error.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build();

    let response = handler.vote(author_peer, test_header, Vec::new()).await;
    assert!(response.is_err());
}

// NOTE: this is unit tested in primary::rayls_consensus_state_sync
#[tokio::test]
async fn test_fetch_certificates_handler() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();

    let certificate_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(primary.consensus_config(), cb.clone(), synchronizer.clone());

    let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
        .into_iter()
        .map(|cert| cert.header().clone())
        .collect();
    let mut headers = vec![];
    let total_rounds = 4;
    for i in 0..total_rounds {
        let parents: BTreeSet<_> =
            current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }

    let total_authorities = fixture.authorities().count();
    let total_certificates = total_authorities * total_rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates);
    assert_eq!(16, total_certificates);

    // Populate certificate store such that each authority has the following rounds:
    // Authority 0: 1
    // Authority 1: 1 2
    // Authority 2: 1 2 3
    // Authority 3: 1 2 3 4
    // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
    // already in store. But this does not matter for testing here.
    let mut authorities = Vec::<AuthorityIdentifier>::new();
    for i in 0..total_authorities {
        authorities.push(certificates[i].header().author().clone());
        for j in 0..=i {
            let mut cert = certificates[i + j * total_authorities].clone();
            assert_eq!(&cert.header().author(), &authorities.last().unwrap());
            // Simulating only 1 directly verified certificate (Auth 3 Round 4) being stored.
            cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                cert.aggregated_signature().expect("Invalid Signature"),
            ));

            certificate_store.write(cert).expect("Writing certificate to store failed");
        }
    }

    // Each test case contains (lower bound round, skip rounds, max items, expected output).
    let test_cases = vec![
        (0, vec![vec![], vec![], vec![], vec![]], 20, vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![1u32], vec![1], vec![], vec![]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![], vec![], vec![1], vec![1]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 4, vec![2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2]),
        (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], 2, vec![2, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
        // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
        (1, vec![vec![], vec![], vec![3], vec![3]], 5, vec![2, 2, 2, 4]),
    ];

    let sample_cert = &certificates[0];
    let single_cert_size = encode(sample_cert).len();
    let message_overhead = encode(&MissingCertificatesRequest::default()).len();
    for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in &test_cases {
        // estimate response size based on max_items returned
        let response_size = single_cert_size * max_items + message_overhead;
        let missing_req = MissingCertificatesRequest::default()
            .set_bounds(
                *lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.iter().map(|rounds| rounds.iter().copied().collect()))
                    .collect(),
            )
            .expect("boundary set")
            .set_max_response_size(response_size);
        let resp = handler.retrieve_missing_certs(missing_req).await.unwrap();
        if let PrimaryResponse::RequestedCertificates(certs) = resp {
            assert_eq!(certs.iter().map(|cert| cert.round()).collect::<Vec<_>>(), *expected_rounds);
        } else {
            panic!("did not get certs response!");
        }
    }

    // assert error for invalid requests with min too low
    for (lower_bound_round, skip_rounds_vec, _max_items, _expected_rounds) in test_cases {
        let too_big = MissingCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                    .collect(),
            )
            .expect("boundary set")
            .set_max_response_size(0);
        let resp = handler.retrieve_missing_certs(too_big).await;
        assert!(resp.is_err());
    }
}

#[tokio::test]
async fn test_request_vote_created_at_in_future() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(primary.consensus_config(), cb.clone(), synchronizer.clone());

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    // Verify Handler generates a Vote.

    // Make some mock certificates that are parents of our new header.
    // New certs for a new header
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .round(2)
            .with_payload_batch(fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set the creation time to be deep in the future (an hour)
    let created_at = now() + 60 * 60;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .created_at(created_at)
        .build();

    // For such a future header we get back an error
    assert!(handler.vote(author_peer, test_header, Vec::new()).await.is_err());

    // Verify Handler generates a Vote.

    // Set the creation time to be a bit in the future (1s)
    let created_at = now() + 1;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(3)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .created_at(created_at)
        .build();

    cb.committed_round_updates().send_replace(1);
    let _vote = if let PrimaryResponse::Vote(vote) =
        handler.vote(author_peer, test_header, Vec::new()).await.unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };
    assert!(created_at <= now());
}
