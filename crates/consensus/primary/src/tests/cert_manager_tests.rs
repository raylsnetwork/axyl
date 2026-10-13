//! Tests for the cert manager.

use super::CertificateManager;
use crate::{error::CertManagerError, state_sync::AtomicRound, ConsensusBus};
use assert_matches::assert_matches;
use rayls_consensus_primary::test_utils::make_optimal_signed_certificates;
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore as _};
use rayls_infrastructure_types::{Certificate, Hash as _, Notifier, SignatureVerificationState};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::collections::BTreeSet;

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


/// The certificate fetcher's accept path overwrites a stored certificate with a peer-supplied
/// variant: a second valid certificate for the same header, with a different signer subset and a
/// different aggregate signature.
///
/// Fetched certificates reach the store through `process_verified_certificates`, which checks only
/// whether a digest is already *pending* and whether its parents exist. It never checks whether the
/// certificate is already *stored*, so an already-accepted digest is written again and `write_all`
/// (an unconditional keyed insert) replaces the stored copy with the variant. The gossip/single
/// path (`CertificateValidator::process_certificate`) does guard this: it returns early when
/// `node_storage().contains(digest)`, keeping the stored copy. The fetch-accept path has no such
/// guard. The stale-`skip_rounds` fallback in the fetcher makes this reachable in normal catch-up:
/// after repeated failures it clears the skip set so a peer re-sends rounds the node already holds.
#[tokio::test]
async fn fetched_variant_silently_overwrites_stored_certificate() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();
    // MemDatabase clones share one `Arc<RwLock<..>>`, so this handle reads what the manager writes.
    let config = primary.consensus_config();
    let committee = fixture.committee();

    let mut manager =
        CertificateManager::new(config.clone(), cb.clone(), AtomicRound::new(0), AtomicRound::new(0));

    // One round-1 header, two valid 3-of-4 certificates over it: the canonical one the node
    // accepted, and a variant a peer can serve for the same round during catch-up.
    let header = fixture.header_from_last_authority();
    let votes: Vec<_> = fixture
        .authorities()
        .map(|a| {
            let vote = a.vote(&header);
            (vote.author().clone(), *vote.signature())
        })
        .collect();
    let canonical = Certificate::new_unverified(&committee, header.clone(), votes[..3].to_vec())
        .expect("3 of 4 votes reach quorum")
        .validate_and_verify(&committee)
        .expect("canonical verifies");
    let variant = Certificate::new_unverified(&committee, header, votes[1..].to_vec())
        .expect("3 of 4 votes reach quorum")
        .validate_and_verify(&committee)
        .expect("variant verifies");

    let digest = canonical.digest();
    assert_eq!(digest, variant.digest(), "the two certificates share a digest");
    assert_ne!(canonical.signed_authorities(), variant.signed_authorities(), "different signers");

    let shutdown = Notifier::new();
    let shutdown_rx = shutdown.subscribe();

    // Accept the canonical certificate, as the node would in normal operation.
    manager.process_verified_certificates(vec![canonical.clone()], &shutdown_rx).await?;
    let stored = config.node_storage().read(digest).expect("read").expect("canonical stored");
    assert_eq!(stored.signed_authorities(), canonical.signed_authorities());

    // Feed the variant through the exact accept path the certificate fetcher uses.
    manager.process_verified_certificates(vec![variant.clone()], &shutdown_rx).await?;

    // The stored certificate has been silently replaced by the variant...
    let stored = config.node_storage().read(digest).expect("read").expect("still stored");
    assert_eq!(
        stored.signed_authorities(),
        variant.signed_authorities(),
        "the fetched variant overwrote the stored certificate"
    );
    assert_ne!(
        stored.aggregated_signature(),
        canonical.aggregated_signature(),
        "the stored aggregate signature is now the variant's, not the canonical one"
    );

    Ok(())
}
