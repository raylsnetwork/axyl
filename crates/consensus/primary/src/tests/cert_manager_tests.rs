//! Tests for the cert manager.

use super::{CertificateManager, CertificateManagerCommand};
use crate::{error::CertManagerError, state_sync::AtomicRound, ConsensusBus};
use assert_matches::assert_matches;
use rayls_consensus_primary::test_utils::make_optimal_signed_certificates;
use rayls_infrastructure_storage::{mem_db::MemDatabase, test_utils::TestDb};
use rayls_infrastructure_types::{
    Certificate, Hash as _, Notifier, RaylsSender as _, SignatureVerificationState, TaskKind,
    TaskManager,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{collections::BTreeSet, time::Duration};
use tokio::{sync::oneshot, time::timeout};

struct TestTypes<DB = MemDatabase> {
    /// The CertificateManager
    manager: CertificateManager<DB>,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
    /// The bus the manager publishes on.
    bus: ConsensusBus,
}

fn create_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();

    // for validator
    let config = primary.consensus_config();
    let gc_round = AtomicRound::new(0);
    let highest_processed_round = AtomicRound::new(0);

    let manager = CertificateManager::new(config, cb.clone(), gc_round, highest_processed_round);

    TestTypes { manager, fixture, bus: cb }
}

#[tokio::test]
async fn test_unverified_certificate_fails() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();

    let shutdown = Notifier::new();
    let shutdown_rx = shutdown.subscribe();
    let unverified = fixture.unverified_cert_from_last_authority();
    assert!(manager.process_verified_certificates(vec![unverified], &shutdown_rx).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_accept_pending_certs() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();
    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // all certs
    let certs: Vec<_> = certificates
        .into_iter()
        .map(|mut c| {
            c.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                c.aggregated_signature().expect("signature valid"),
            ));
            c
        })
        .collect();

    // separate first round (4 certs) and later rounds
    let mut first_round = certs; // for readability
    let later_rounds = first_round.split_off(num_authorities);
    let expected_pending_len = later_rounds.len();

    // try to process certs - all should be pending
    let shutdown = Notifier::new();
    let shutdown_rx = shutdown.subscribe();
    let expected_last_digest = later_rounds.last().expect("at least one cert").digest();
    let res = manager.process_verified_certificates(later_rounds, &shutdown_rx).await;

    // expect all certs to process and error to reference last digest processed
    assert_matches!(res, Err(CertManagerError::Pending(digest)) if digest == expected_last_digest);

    // later_rounds should be pending
    assert_eq!(expected_pending_len, manager.pending.num_pending());
    Ok(())
}

/// The proposer's backpressure brake reads this watch, so a count left stale-high after the
/// drain would throttle proposing forever.
#[tokio::test]
async fn suspended_cert_count_follows_the_drain() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, bus } = create_test_types();
    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, keys.as_slice());
    let mut first_round: Vec<_> = certificates
        .into_iter()
        .map(|mut c| {
            c.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                c.aggregated_signature().expect("signature valid"),
            ));
            c
        })
        .collect();
    let later_rounds = first_round.split_off(num_authorities);
    let suspended = later_rounds.len();

    let shutdown = Notifier::new();
    let shutdown_rx = shutdown.subscribe();
    let res = manager.process_verified_certificates(later_rounds, &shutdown_rx).await;
    assert_matches!(res, Err(CertManagerError::Pending(_)));
    assert_eq!(*bus.suspended_cert_count().borrow(), suspended, "suspension publishes the count");

    // the first round unlocks everything above it
    manager.process_verified_certificates(first_round, &shutdown_rx).await?;
    assert_eq!(manager.pending.num_pending(), 0, "every pending cert drained");
    assert_eq!(*bus.suspended_cert_count().borrow(), 0, "the drain publishes the count");
    Ok(())
}

/// Epoch teardown must be able to reap the manager while it is inside a store write. The write is
/// synchronous, so a writer that has stopped applying parks the manager's worker thread with the
/// run future pinned on its stack: `abort` never reaches a task that never yields, the teardown
/// abandons it, and it keeps its channels and the mem-store lock into the next epoch. The write
/// must run off the worker so the future can be dropped at a yield point while the write finishes
/// on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn teardown_reaps_a_manager_blocked_in_a_store_write() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(TestDb::new).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage().clone();
    let manager = CertificateManager::new(
        config.clone(),
        cb.clone(),
        AtomicRound::new(0),
        AtomicRound::new(0),
    );

    let mut task_manager = TaskManager::new("cert-manager-teardown");
    task_manager.set_phase_stall_bound(Duration::from_millis(100));
    task_manager.set_join_wait(100);
    task_manager.spawn_classified_task("certificate-manager", manager.run(), TaskKind::Drainable);

    // Round-1 certificates over genesis parents are accepted outright, so they reach the store.
    let committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _) =
        make_optimal_signed_certificates(1..=1, &genesis, &committee, keys.as_slice());
    let certificates: Vec<_> = certificates
        .into_iter()
        .map(|mut c| {
            c.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                c.aggregated_signature().expect("signature valid"),
            ));
            c
        })
        .collect();

    db.arm();
    let (reply, reply_rx) = oneshot::channel();
    cb.certificate_manager()
        .send(CertificateManagerCommand::ProcessVerifiedCertificates { certificates, reply })
        .await?;
    timeout(Duration::from_secs(5), async {
        while !db.blocked() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the manager must reach the store write");

    let shutdown = config.shutdown().clone();
    shutdown.notify();
    let joined = timeout(Duration::from_secs(10), task_manager.join(shutdown)).await;

    // The command's reply sender lives on the run future's stack: it is dropped only if teardown
    // actually tore the future down while the write was still parked.
    let reaped = timeout(Duration::from_secs(1), reply_rx).await;
    // Let the parked write finish regardless, so a failure cannot wedge the runtime.
    db.release();

    assert!(joined.is_ok(), "join must complete");
    assert!(
        matches!(reaped, Ok(Err(_))),
        "teardown must drop the manager while its write is parked; reply channel state: {reaped:?}"
    );
    Ok(())
}
