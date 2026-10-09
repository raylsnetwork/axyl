//! Certificate tests

use rand::{rngs::StdRng, SeedableRng};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlsKeypair, Certificate, CommittedSubDag, ConsensusOutput, Hash as _,
    ReputationScores, SignatureVerificationState, Vote,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{num::NonZeroUsize, sync::Arc};

#[test]
fn test_empty_certificate_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    let votes =
        fixture.authorities().take(3).map(|a| (a.id(), *a.vote(&header).signature())).collect();

    let certificate =
        Certificate::new_unsigned_for_test(&committee, header, votes).expect("new unsigned cert");
    assert!(certificate.validate_and_verify(&committee).is_err());
}

#[test]
fn test_valid_certificate_verification() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    for authority in fixture.authorities().take(3) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let certificate = Certificate::new_unverified(&committee, header, signatures).unwrap();
    let verified_certificate = certificate.validate_and_verify(&committee);

    assert!(verified_certificate.is_ok());
    assert!(matches!(
        verified_certificate.unwrap().signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[test]
fn test_certificate_insufficient_signatures() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 2 Signatures. This is less than 2F + 1 (3).
    for authority in fixture.authorities().take(2) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    assert!(Certificate::new_unverified(&committee, header.clone(), signatures.clone()).is_err());

    let certificate = Certificate::new_unsigned_for_test(&committee, header, signatures).unwrap();

    assert!(certificate.validate_and_verify(&committee).is_err());
}

#[test]
fn test_certificate_validly_repeated_public_keys() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 3 Signers satisfies the 2F + 1 signed stake requirement
    for authority in fixture.authorities().take(3) {
        let vote = authority.vote(&header);
        // We double every (pk, signature) pair - these should be ignored when forming the
        // certificate.
        signatures.push((vote.author().clone(), *vote.signature()));
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let certificate_res = Certificate::new_unverified(&committee, header, signatures);
    assert!(certificate_res.is_ok());
    let certificate = certificate_res.unwrap();

    assert!(certificate.validate_and_verify(&committee).is_ok());
}

#[test]
fn test_unknown_signature_in_certificate() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    let mut signatures = Vec::new();

    // 2 Signatures. This is less than 2F + 1 (3).
    for authority in fixture.authorities().take(2) {
        let vote = authority.vote(&header);
        signatures.push((vote.author().clone(), *vote.signature()));
    }

    let malicious_key = BlsKeypair::generate(&mut StdRng::from_os_rng());
    let malicious_id: AuthorityIdentifier = AuthorityIdentifier::dummy_for_test(50u8);

    let vote = Vote::new_with_signer(&header, malicious_id, &malicious_key);
    signatures.push((vote.author().clone(), *vote.signature()));

    assert!(Certificate::new_unverified(&committee, header, signatures).is_err());
}

proptest::proptest! {
    #[test]
    fn test_certificate_verification(
        committee_size in 4..35_usize
    ) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(committee_size).unwrap())
            .build();
        let committee = fixture.committee();
        let header = fixture.header_from_last_authority();

        let mut signatures = Vec::new();

        let quorum_threshold = committee.quorum_threshold() as usize;

        for authority in fixture.authorities().take(quorum_threshold) {
            let vote = authority.vote(&header);
            signatures.push((vote.author().clone(), *vote.signature()));
        }

        let certificate = Certificate::new_unverified(&committee, header, signatures).unwrap();

        assert!(certificate
            .validate_and_verify(&committee)
            .is_ok());
    }
}

/// Two certificates over the same leader header, built from different 2f+1 vote subsets, must
/// derive the same epoch-close seed (#233).
///
/// The legacy seed hashes the certificate's aggregate BLS signature, which differs between the
/// two subsets, so nodes holding different variants stamped different `extra_data` into the
/// epoch-closing block. The post-fork seed is the consensus header hash, which covers the sub-dag
/// by certificate digests only.
#[test]
fn test_epoch_close_seed_identical_across_certificate_vote_subsets() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let header = fixture.header_from_last_authority();

    // Committee of 4: quorum is 3, so two distinct 3-vote subsets exist.
    let votes: Vec<_> = fixture
        .authorities()
        .map(|authority| {
            let vote = authority.vote(&header);
            (vote.author().clone(), *vote.signature())
        })
        .collect();
    assert_eq!(votes.len(), 4);

    let cert_a = Certificate::new_unverified(&committee, header.clone(), votes[..3].to_vec())
        .expect("3 of 4 votes reach quorum");
    let cert_b = Certificate::new_unverified(&committee, header, votes[1..].to_vec())
        .expect("3 of 4 votes reach quorum");

    // Same header-only digest, so the DAG and the certificate store treat them as one...
    assert_eq!(cert_a.digest(), cert_b.digest());
    // ...but different signer sets and aggregate bytes.
    assert_ne!(cert_a.signed_authorities(), cert_b.signed_authorities());
    assert_ne!(cert_a.aggregated_signature(), cert_b.aggregated_signature());

    let output_for = |leader: Certificate| ConsensusOutput {
        sub_dag: Arc::new(CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
        )),
        close_epoch: true,
        ..Default::default()
    };
    let output_a = output_for(cert_a);
    let output_b = output_for(cert_b);

    // The legacy seed forks on the signer subset.
    assert_ne!(output_a.keccak_leader_sigs(), output_b.keccak_leader_sigs());
    // The post-fork seed does not.
    assert_eq!(output_a.epoch_close_seed(), output_b.epoch_close_seed());
}
