//! IT tests for the flow of certificates.
//!
//! Certificates are validated and sent to the [CertificateManager].
//! The [CertificateManager] tracks pending certificates and accepts certificates that are complete.

use super::{cert_manager::CertificateManager, cert_validator::CertificateValidator, AtomicRound};
use crate::{
    certificate_fetcher::CertificateFetcherCommand,
    consensus::{gc_round, ConsensusRound},
    error::CertManagerError,
    state_sync::HeaderValidator,
    ConsensusBus,
};
use assert_matches::assert_matches;
use rayls_consensus_primary::test_utils::{make_optimal_signed_certificates, signed_cert_for_test};
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore};
use rayls_infrastructure_types::{
    Certificate, CertificateDigest, Database, Hash as _, RaylsReceiver as _, RaylsSender, Round,
    TaskManager,
};
use rayls_testing_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use std::{collections::BTreeSet, time::Duration};
use tokio::time::timeout;

struct TestTypes<DB = MemDatabase> {
    /// The CertificateValidator
    validator: CertificateValidator<DB>,
    /// The CertificateManager
    manager: CertificateManager<DB>,
    /// The consensus bus.
    cb: ConsensusBus,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
    /// The task manager.
    task_manager: TaskManager,
}

fn create_all_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().last().unwrap();
    let (manager, validator, cb, _) = create_core_test_types(primary);
    let task_manager = TaskManager::default();

    TestTypes { manager, validator, cb, fixture, task_manager }
}

// reused in other tests
fn create_core_test_types_with_tasks<DB: Database>(
    primary: &AuthorityFixture<DB>,
    task_manager: TaskManager,
) -> (CertificateManager<DB>, CertificateValidator<DB>, ConsensusBus, TaskManager) {
    let cb = ConsensusBus::new();
    let config = primary.consensus_config();
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);

    // manager
    let manager = CertificateManager::new(
        config.clone(),
        cb.clone(),
        gc_round.clone(),
        highest_processed_round.clone(),
    );

    // validator
    let validator = CertificateValidator::new(
        config.clone(),
        cb.clone(),
        gc_round.clone(),
        highest_processed_round,
        highest_received_round,
        task_manager.get_spawner(),
    );

    (manager, validator, cb, task_manager)
}

// reused in other tests
fn create_core_test_types<DB: Database>(
    primary: &AuthorityFixture<DB>,
) -> (CertificateManager<DB>, CertificateValidator<DB>, ConsensusBus, TaskManager) {
    let task_manager = TaskManager::default();
    create_core_test_types_with_tasks(primary, task_manager)
}

/// Helper to sort certificates by digest
fn sort_by_digest(a: &CertificateDigest, b: &CertificateDigest) -> core::cmp::Ordering {
    a.cmp(b)
}

#[tokio::test]
async fn test_accept_valid_certs() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // spawn manager task
    task_manager.spawn_critical_task("manager", manager.run());

    // receive parent updates (proposer)
    let mut rx_parents = cb.parents().subscribe();
    // receive new accepted certs (consensus)
    let mut rx_new_certificates = cb.new_certificates().subscribe();

    // create 3 certs
    // NOTE: test types uses the last authority
    let certs: Vec<_> = fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    // assert unverified certificates and process
    for cert in certs.clone() {
        assert!(!cert.is_verified());
        // try to accept
        validator.process_peer_certificate(cert).await?;
    }

    // recover_state feeds genesis into the parents aggregator; each cert appended
    // after quorum triggers another emission (CertificatesAggregator::append drains
    // on every call past the quorum threshold). Skip all round-0 emissions.
    let received = loop {
        let msg = rx_parents.recv().await.unwrap();
        if msg.1 >= 1 {
            break msg;
        }
    };
    assert_eq!(received, (certs.clone(), 1));

    // assert consensus receives accepted certs
    for cert in &certs {
        let received = rx_new_certificates.recv().await.unwrap();
        assert_eq!(&received, cert);
    }

    // assert certs were stored
    for cert in &certs {
        let stored = certificate_store.read(cert.digest())?;
        assert_eq!(stored, Some(cert.clone()));
    }

    Ok(())
}

#[tokio::test]
async fn test_accept_pending_certs() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();

    // spawn manager task
    task_manager.spawn_critical_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    // try to process certs for rounds 2..5 before round 1
    // assert pending
    for cert in later_rounds {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
    }

    // assert no certs accepted
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    assert!(rx_new_certificates.try_recv().is_err()); // empty channel

    // process round 1
    for cert in first_round {
        let res = validator.process_peer_certificate(cert).await;
        assert_matches!(res, Ok(()));
    }

    // assert all certs accepted in causal order
    let mut causal_round = 0;
    for _ in &all_certificates {
        let received = rx_new_certificates.try_recv().expect("new cert");
        // cert rounds should only accend
        let cert_round = received.round();
        if cert_round > causal_round {
            causal_round = cert_round;
        }
        assert!(cert_round == causal_round);
    }

    // create a certificate far in the future — the acceptance window
    // slides forward and the cert is accepted (parents already in store).
    let far_round = 2000;
    let (_digest, cert) = signed_cert_for_test(
        keys.as_slice(),
        all_certificates.iter().last().cloned().unwrap().origin().clone(),
        far_round,
        next_parents,
        &committee,
    );

    let res = validator.process_peer_certificate(cert).await;
    assert_matches!(res, Ok(()));

    Ok(())
}

#[tokio::test]
async fn test_gc_pending_certs() -> eyre::Result<()> {
    const GC_DEPTH: Round = 5;

    // create test types
    let TestTypes { validator, manager, cb, fixture, task_manager } = create_all_test_types();

    // cert store
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // spawn manager task
    task_manager.spawn_critical_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make 5 rounds of certificates
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    // try to process certs for rounds 2..5 (before round 1)
    // assert pending
    for cert in later_rounds.clone() {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
    }

    // assert no certs accepted
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    assert!(rx_new_certificates.try_recv().is_err()); // empty channel

    // reinsert later rounds as if fetched from peers
    // and assert still pending
    let last_digest = later_rounds.back().expect("last certificate").digest();
    let err = validator.process_fetched_certificates_in_parallel(later_rounds.clone().into()).await;
    assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == last_digest);

    // update consensus rounds
    // commit at round 8, so round 3 becomes the GC round
    let commit_round = 8;
    cb.update_consensus_rounds(ConsensusRound::new(commit_round, gc_round(commit_round, GC_DEPTH)));

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    // assert all certs accepted in causal order; the store write completes on the blocking pool
    // before the manager forwards, so await the forwards rather than expecting them queued.
    let mut causal_round = 0;
    for _ in &later_rounds {
        let received =
            timeout(Duration::from_secs(3), rx_new_certificates.recv()).await?.expect("new cert");
        // cert rounds should only accend
        let cert_round = received.round();
        if cert_round > causal_round {
            causal_round = cert_round;
        }
        assert!(cert_round == causal_round);
    }

    Ok(())
}

#[tokio::test]
async fn test_node_restart_syncs_state() -> eyre::Result<()> {
    let TestTypes { validator, manager, fixture, task_manager, .. } = create_all_test_types();
    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // spawn manager task
    task_manager.spawn_critical_task("manager", manager.run());

    // create 3 certs
    // NOTE: test types uses the last authority
    let mut certs: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let last_cert = certs.last().cloned().expect("last certificate");
    let last_digest = last_cert.digest();

    // process 1 certificate
    validator.process_peer_certificate(last_cert).await?;

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    // crash
    task_manager.abort();

    //
    // recover from crash and submit last two certs
    // this should not forward to the proposer on startup because quorum wasn't reached
    // so the round hasn't advanced
    //

    let (manager_first_recovery, validator_first_recovery, cb_first_recovery, task_manager) =
        create_core_test_types_with_tasks(primary, task_manager);

    task_manager.spawn_critical_task("recovered manager", manager_first_recovery.run());

    // assert proposer receives parents for round after recovery
    let mut rx_parents_first_recovery = cb_first_recovery.parents().subscribe();

    // proposer should not receive parents because quorum wasn't reached
    assert!(rx_parents_first_recovery.try_recv().is_err());

    // send remaining 2 certs to reach quorum
    let mut last_digest = CertificateDigest::default();
    for cert in certs.clone().into_iter().take(2) {
        last_digest = cert.digest();
        validator_first_recovery.process_peer_certificate(cert).await.unwrap();
    }

    // wait for certs to storage
    timeout(Duration::from_secs(3), certificate_store.notify_read(last_digest)).await??;

    //crash
    task_manager.abort();

    //
    // recover - this should forward an update to the proposer because enough certs were
    // reached to advance the round before crash
    //

    let (manager_second_recovery, _validator, cb_second_recovery, task_manager) =
        create_core_test_types_with_tasks(primary, task_manager);

    task_manager.spawn_critical_task("recovered manager", manager_second_recovery.run());

    // assert proposer receives parents for round after recovery
    let mut rx_parents_second_recovery = cb_second_recovery.parents().subscribe();

    let (mut received_certs, round) = rx_parents_second_recovery.recv().await.unwrap();

    // sort certs to ensure consistent order
    received_certs.sort_by(|a, b| {
        let a = a.digest();
        let b = b.digest();
        sort_by_digest(&a, &b)
    });
    certs.sort_by(|a, b| {
        let a = a.digest();
        let b = b.digest();
        sort_by_digest(&a, &b)
    });
    assert_eq!(round, 1);

    Ok(())
}

#[tokio::test]
async fn test_filter_unknown_parents() -> eyre::Result<()> {
    let TestTypes { validator, manager, cb, fixture, task_manager, .. } = create_all_test_types();

    // test types uses last authority for config
    let primary = fixture.authorities().last().unwrap();

    // spawn manager task
    task_manager.spawn_critical_task("manager", manager.run());

    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, _next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // separate first round (4 certs) and later rounds
    let mut first_round = all_certificates.clone(); // rename for readability
    let later_rounds = first_round.split_off(num_authorities);

    let header_validator = HeaderValidator::new(primary.consensus_config(), cb.clone());

    // assert all unknown
    let round5_cert = later_rounds.back().expect("header for round 2");
    // only round 4 should be unknown
    let mut expected: Vec<_> = all_certificates
        .iter()
        .filter_map(|c| if c.header().round() == 4 { Some(c.digest()) } else { None })
        .collect();
    expected.sort_by(sort_by_digest);
    // report unknown
    let mut unknown = header_validator.identify_unknown_parents(round5_cert.header()).await?;
    unknown.sort_by(sort_by_digest);

    assert_eq!(expected, unknown);

    // try to process certs for rounds 2..5 before round 1
    // assert pending
    for cert in later_rounds.clone() {
        let expected = cert.digest();
        let err = validator.process_peer_certificate(cert).await;
        assert_matches!(err, Err(CertManagerError::Pending(digest)) if digest == expected);
    }

    // round 4 should no longer be "missing"
    let unknown = header_validator.identify_unknown_parents(round5_cert.header()).await?;
    assert!(unknown.is_empty());

    // assert pending aren't unknown
    let round2_cert = later_rounds.front().expect("header for round 2");
    let mut unknown = header_validator.identify_unknown_parents(round2_cert.header()).await?;
    unknown.sort_by(sort_by_digest);
    let mut expected: Vec<_> = first_round.iter().map(|c| c.digest()).collect();
    expected.sort_by(sort_by_digest);
    assert_eq!(expected, unknown);

    Ok(())
}

/// Regression test: nodes that fall >936 rounds behind must not deadlock.
///
/// When highest_processed_round + max_diff(1000) < cert.round(), the acceptance
/// window slides forward so the cert enters the normal pipeline. CertificateManager
/// then triggers ancestor fetching for missing parents, allowing the node to heal.
///
/// Mode-independent — the fix applies to CvvActive, CvvInactive, and Observer equally.
#[tokio::test]
async fn test_too_new_window_slides_and_triggers_ancestor_fetch() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().last().unwrap();
    let cb = ConsensusBus::new();
    let config = primary.consensus_config();

    let highest_processed_round = AtomicRound::new(5);
    let gc_round = AtomicRound::new(0);
    let highest_received_round = AtomicRound::new(0);
    let task_manager = TaskManager::default();

    let manager = CertificateManager::new(
        config.clone(),
        cb.clone(),
        gc_round.clone(),
        highest_processed_round.clone(),
    );
    let validator = CertificateValidator::new(
        config.clone(),
        cb.clone(),
        gc_round,
        highest_processed_round.clone(),
        highest_received_round,
        task_manager.get_spawner(),
    );

    task_manager.spawn_critical_task("manager", manager.run());

    let committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (all_certificates, next_parents) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    let max_diff = config
        .network_config()
        .sync_config()
        .max_diff_between_external_cert_round_and_highest_local_round;

    let far_round = 2000;
    let (digest, cert) = signed_cert_for_test(
        keys.as_slice(),
        all_certificates.iter().last().cloned().unwrap().origin().clone(),
        far_round,
        next_parents,
        &committee,
    );

    let mut cert_fetcher_rx = cb.certificate_fetcher().subscribe();

    let result = validator.process_peer_certificate(cert).await;

    // cert must reach CertificateManager and pend on missing parents
    assert_matches!(
        result,
        Err(CertManagerError::Pending(d)) if d == digest,
        "expected Pending (cert entered pipeline), got {result:?}"
    );

    // acceptance window must advance to cert.round() - max_diff
    let expected_floor = far_round - max_diff;
    assert_eq!(
        highest_processed_round.load(),
        expected_floor,
        "highest_processed_round should advance to {expected_floor}"
    );

    // CertificateManager must trigger ancestor fetching for missing parents
    let fetcher_cmd = timeout(Duration::from_secs(3), cert_fetcher_rx.recv())
        .await
        .expect("cert_fetcher should receive command within timeout")
        .expect("cert_fetcher channel should not be closed");
    assert_matches!(
        fetcher_cmd,
        CertificateFetcherCommand::Ancestors(c) if c.digest() == digest,
        "CertificateManager must trigger ancestor fetch for the pending cert"
    );

    Ok(())
}
