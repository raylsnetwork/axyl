//! Certifier tests

use super::*;
use crate::{
    network::{PrimaryRequest, PrimaryResponse},
    ConsensusBus,
};
use rand::{rngs::StdRng, SeedableRng};
use rayls_consensus_network::types::{NetworkCommand, NetworkHandle};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{BlsKeypair, BlsSigner, RaylsSender, SignatureVerificationState};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::HashMap,
    num::{NonZero, NonZeroUsize},
};
use tokio::sync::mpsc;

#[tokio::test(flavor = "current_thread")]
async fn propose_header_to_form_certificate() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // Set up remote primaries responding with votes.
    let mut peer_votes = HashMap::new();
    for peer in fixture.authorities().filter(|a| a.id() != id) {
        let name = peer.authority().protocol_key();
        let id = peer.authority().id();
        let vote = Vote::new(&proposed_header, id, peer.consensus_config().key_config());
        peer_votes.insert(name, vote);
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());

    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone().into(),
        &task_manager,
    );

    // Propose header and ensure that a certificate is formed by pulling it out of the
    // consensus channel.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();
    // Wait for the vote requests and send the votes back.
    while let Some(req) = network_rx.recv().await {
        if let NetworkCommand::SendRequest {
            peer,
            request: PrimaryRequest::Vote { header: _, parents: _ },
            reply,
        } = req
        {
            if let Some(vote) = peer_votes.remove(&peer) {
                reply.send(Ok(PrimaryResponse::Vote(vote))).unwrap();
            }
        }
        if peer_votes.is_empty() {
            break;
        }
    }
    let certificate = tokio::time::timeout(Duration::from_secs(10), rx_new_certificates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.header().digest(), proposed_digest);
    assert!(matches!(
        certificate.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_failure() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    let task_manager = TaskManager::default();
    // Spawn the core.
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());

    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone().into(),
        &task_manager,
    );

    // Propose header and verify we get no certificate back.
    cb.headers().send(proposed_header).await.unwrap();

    // Wait for the vote requests and send back errors.
    let mut i = 0;
    while let Some(req) = network_rx.recv().await {
        if let NetworkCommand::SendRequest {
            peer: _,
            request: PrimaryRequest::Vote { header: _, parents: _ },
            reply,
        } = req
        {
            reply.send(Err(NetworkError::RPCError("bad vote".to_string()))).unwrap();
        }
        i += 1;
        if i >= 3 {
            break;
        }
    }

    if let Ok(result) =
        tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv()).await
    {
        panic!("expected no certificate to form; got {result:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_scenario_with_bad_sigs() {
    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(6, 0, true).await;
    run_vote_aggregator_with_param(6, 1, true).await;
    run_vote_aggregator_with_param(6, 2, false).await;

    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(4, 0, true).await;
    run_vote_aggregator_with_param(4, 1, true).await;
    run_vote_aggregator_with_param(4, 2, false).await;
}

async fn run_vote_aggregator_with_param(
    committee_size: usize,
    num_byzantine: usize,
    expect_cert: bool,
) {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(committee_size).unwrap())
        .randomize_ports(true)
        .build();

    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id: AuthorityIdentifier = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // Set up remote primaries responding with votes.
    let mut peer_votes = HashMap::new();
    for (i, peer) in fixture.authorities().filter(|a| a.id() != id).enumerate() {
        let name = peer.id();
        // Create bad signature for a number of byzantines.
        let vote = if i < num_byzantine {
            let bad_key = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            Vote::new_with_signer(&proposed_header, name.clone(), &bad_key)
        } else {
            Vote::new(&proposed_header, name.clone(), peer.consensus_config().key_config())
        };
        let id = peer.authority().protocol_key();
        peer_votes.insert(id, vote);
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.into(),
        &task_manager,
    );

    // Send a proposed header.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();
    // Wait for the vote requests and send the votes back.
    while let Some(req) = network_rx.recv().await {
        if let NetworkCommand::SendRequest {
            peer,
            request: PrimaryRequest::Vote { header: _, parents: _ },
            reply,
        } = req
        {
            if let Some(vote) = peer_votes.remove(&peer) {
                reply.send(Ok(PrimaryResponse::Vote(vote))).unwrap();
            }
        }
        if peer_votes.is_empty() {
            break;
        }
    }

    if expect_cert {
        // A cert is expected, checks that the header digest matches.
        let certificate = tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(certificate.header().digest(), proposed_digest);
    } else {
        // A cert is not expected, checks that it times out without forming the cert.
        assert!(tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn test_shutdown_core() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

    let cb = ConsensusBus::new();
    // Spawn the core.
    let mut task_manager = TaskManager::default();
    // Make a synchronizer for the core.
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());

    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        config.clone(),
        cb.clone(),
        synchronizer.clone(),
        NetworkHandle::new_for_test().into(),
        &task_manager,
    );

    // send request to spawn voting sub-tasks
    cb.headers().send(Header::default()).await.expect("send header for proposal");

    // sleep briefly so certifier has time to subscribe then shutdown the core
    tokio::time::sleep(Duration::from_millis(100)).await;
    config.shutdown().notify();
    let _ =
        tokio::time::timeout(Duration::from_secs(3), task_manager.join(config.shutdown().clone()))
            .await
            .expect("timeout");
}

/// One vote request will produce an error, make sure the certificate is still formed with the good
/// votes. I.E. the vote error does not derail the entire process leaving a broken DAG.
#[tokio::test(flavor = "current_thread")]
async fn propose_headers_one_bad() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZero::new(10).unwrap())
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // Set up remote primaries responding with votes.
    let mut peer_votes = HashMap::new();
    for (i, peer) in fixture.authorities().filter(|a| a.id() != id).enumerate() {
        let name = peer.authority().protocol_key();
        let id = peer.authority().id();
        let mut vote = Vote::new(&proposed_header, id, peer.consensus_config().key_config());
        if i < 3 {
            // Break the signature, a lot of errors will be filtered before they get to what we are
            // testing...
            vote.signature =
                primary.consensus_config().key_config().request_signature_direct(&[0_u8, 0_u8]);
        }
        peer_votes.insert(name, vote);
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());

    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone().into(),
        &task_manager,
    );

    // Propose header and ensure that a certificate is formed by pulling it out of the
    // consensus channel.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();
    // Wait for the vote requests and send the votes back.
    while let Some(req) = network_rx.recv().await {
        if let NetworkCommand::SendRequest {
            peer,
            request: PrimaryRequest::Vote { header: _, parents: _ },
            reply,
        } = req
        {
            if let Some(vote) = peer_votes.remove(&peer) {
                reply.send(Ok(PrimaryResponse::Vote(vote))).unwrap();
            }
        }
        if peer_votes.is_empty() {
            break;
        }
    }
    let certificate = tokio::time::timeout(Duration::from_secs(10), rx_new_certificates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.header().digest(), proposed_digest);
    assert!(matches!(
        certificate.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

/// Re-proposing a header that already has a certificate must not certify it again.
///
/// The proposer re-sends its last header whenever the max-delay timer fires and it has not yet
/// observed its own certificate come back round. Under load that window is seconds wide - on
/// 2026-08-16 a ~10s execution stall held a round uncertified until two certification attempts
/// completed in the same flurry. A second attempt aggregates whatever quorum answers that time,
/// producing a certificate with the same digest (it is just the header digest) but a different
/// signer set and so a different aggregate signature. Consensus cannot distinguish them and
/// receivers silently drop the later arrival, so nodes end up holding different bytes for the
/// same certificate - which forks the chain at an epoch close, where that signature is hashed
/// into the block's `extra_data`.
///
/// The in-flight guard does not cover this: an attempt that finished *successfully* reports
/// `is_finished() == true` and falls through it. This asserts the certificate-store guard behind
/// it, so the test must be given time for the first task to actually finish - otherwise it would
/// pass on the in-flight guard instead and prove nothing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn does_not_recertify_an_already_certified_header() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id = primary.id();

    let proposed_header = primary.header(&committee);
    let proposed_digest = proposed_header.digest();

    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    let mut peer_votes = HashMap::new();
    for peer in fixture.authorities().filter(|a| a.id() != id) {
        let name = peer.authority().protocol_key();
        let peer_id = peer.authority().id();
        let vote = Vote::new(&proposed_header, peer_id, peer.consensus_config().key_config());
        peer_votes.insert(name, vote);
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone().into(),
        &task_manager,
    );

    // First attempt: certify the header normally.
    cb.headers().send(proposed_header.clone()).await.unwrap();
    while let Some(req) = network_rx.recv().await {
        if let NetworkCommand::SendRequest {
            peer,
            request: PrimaryRequest::Vote { header: _, parents: _ },
            reply,
        } = req
        {
            if let Some(vote) = peer_votes.remove(&peer) {
                reply.send(Ok(PrimaryResponse::Vote(vote))).unwrap();
            }
        }
        if peer_votes.is_empty() {
            break;
        }
    }

    let certificate = tokio::time::timeout(Duration::from_secs(10), rx_new_certificates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.header().digest(), proposed_digest);

    // The first proposal task has stored the certificate but is now blocked publishing it, and it
    // does not finish until that reply lands. That matters: while it is unfinished the *in-flight*
    // guard would swallow the re-propose and this test would pass without exercising the store
    // guard at all. Answer the publish so the task actually completes.
    //
    // `spawn_header_proposal` skips gossip entirely under `dev-single-node-setup` (no peers to
    // publish to), so there is nothing to answer and the task finishes without blocking. Waiting
    // unconditionally would hang that build.
    #[cfg(not(feature = "dev-single-node-setup"))]
    {
        let published = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(req) = network_rx.recv().await {
                if let NetworkCommand::Publish { reply, .. } = req {
                    let _ = reply.send(Ok(rayls_consensus_network::types::MessageId::new(b"test")));
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert!(published, "certifier never published the certificate, so its task cannot finish");
    }

    // Let the task unwind now that publish has returned. `start_paused` makes this exact rather
    // than hopeful: tokio only auto-advances the clock once every task is blocked on time, so
    // this sleep cannot return while the proposal task is still runnable. A wall-clock sleep
    // would be a guess, and guessing too short is invisible - the re-propose would be swallowed
    // by the in-flight guard and the test would pass while asserting nothing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Re-propose the identical header, exactly as the proposer's max-delay timer would.
    cb.headers().send(proposed_header).await.unwrap();

    // Nothing may ask for votes on it again. Non-Vote traffic (certificate gossip from the first
    // attempt) is expected and ignored.
    let deadline = Duration::from_millis(750);
    let recertified = tokio::time::timeout(deadline, async {
        while let Some(req) = network_rx.recv().await {
            if let NetworkCommand::SendRequest {
                request: PrimaryRequest::Vote { header, parents: _ },
                ..
            } = req
            {
                if header.digest() == proposed_digest {
                    return true;
                }
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    assert!(
        !recertified,
        "certifier re-requested votes for an already-certified header - a second certificate \
         with a different signer set can now exist for digest {proposed_digest:?}",
    );

    // And no second certificate reached the bus.
    assert!(
        tokio::time::timeout(Duration::from_millis(250), rx_new_certificates.recv()).await.is_err(),
        "a second certificate was produced for the same header",
    );
}

/// Pins the ordering the re-propose guard depends on.
///
/// `does_not_recertify_an_already_certified_header` asserts the *effect* - no second vote request
/// - but the guard is only a sufficient check because a finished proposal task has necessarily
/// stored its certificate. That holds because `spawn_header_proposal` awaits
/// `process_own_certificate`, which awaits the certificate manager's reply before returning.
/// Nothing in the type system enforces it: if that await is ever removed or batched, the guard
/// silently stops covering the window and duplicate certificates become possible again, with no
/// test failing. This is the assertion that would fail instead.
#[tokio::test(flavor = "current_thread")]
async fn process_own_certificate_stores_before_returning() {
    use rayls_infrastructure_storage::CertificateStore;
    use rayls_infrastructure_types::Hash as _;

    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let config = primary.consensus_config();

    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    // Mirror what the certifier does to its own certificate before handing it over: the
    // aggregator marks it VerifiedDirectly (votes.rs), and the manager rejects anything still
    // Unverified coming in on the own-certificate path.
    let mut certificate = fixture.certificate(&primary.header(&committee));
    let signature = certificate.aggregated_signature().expect("fixture cert is signed");
    certificate
        .set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(signature));
    let digest = certificate.digest();

    assert!(
        !config.node_storage().contains(&digest).unwrap(),
        "precondition: the certificate must not already be stored",
    );

    synchronizer.process_own_certificate(certificate).await.unwrap();

    assert!(
        config.node_storage().contains(&digest).unwrap(),
        "process_own_certificate returned before storing the certificate - the certifier's \
         re-propose guard reads this store to decide whether a header is already certified, so \
         it no longer covers the window between an attempt finishing and its certificate landing",
    );
}
