//! Certificate tests

use rand::{rngs::StdRng, SeedableRng};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlsKeypair, Certificate, CommittedSubDag, ConsensusOutput, Hash as _,
    ReputationScores, SignatureVerificationState, Vote, B256,
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

/// A syncing node cannot distinguish a variant leader certificate from the canonical one, so it
/// accepts the variant, and for an epoch-closing block below the `EpochCloseSeedV2` activation
/// number it rebuilds a different block than the rest of the network.
///
/// A node fetching a consensus header (catchup: `schedule_catchup_fetch` -> `fetch_batches`, or
/// the single-header walk `get_consensus_header`) runs exactly two checks on the reply: the header
/// digest must equal the requested hash, and every certificate must verify against the committee
/// keys. The header digest covers the sub-dag by certificate digests only, and a different signer
/// subset is still a valid quorum, so both checks pass for a certificate a peer swapped in.
///
/// The output is then executed through the orchestrator, which for a block below the activation
/// number derives the epoch-close seed from the leader certificate's aggregate signature
/// (`keccak_leader_sigs`). That value is stamped into the block's `extra_data`, so a node served
/// the variant forks on the epoch-closing block hash and every block after it. This holds even
/// once `EpochCloseSeedV2` is active network-wide, because history below the activation number
/// keeps the legacy derivation. No runtime check compares the rebuilt block against the
/// committee-signed `EpochRecord.parent_state`, so the divergence is silent.
///
/// This exercises the accept checks and the legacy derivation directly; it does not run the
/// network.
#[test]
fn test_variant_leader_header_passes_sync_accept_checks_but_forks_pre_activation() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let keys = committee.bls_keys();
    let header = fixture.header_from_last_authority();

    // Committee of 4: quorum is 3, so two distinct 3-vote subsets exist for the same header.
    let votes: Vec<_> = fixture
        .authorities()
        .map(|authority| {
            let vote = authority.vote(&header);
            (vote.author().clone(), *vote.signature())
        })
        .collect();
    assert_eq!(votes.len(), 4);

    // The canonical certificate, and a variant a peer could serve to a syncing node instead.
    let canonical = Certificate::new_unverified(&committee, header.clone(), votes[..3].to_vec())
        .expect("3 of 4 votes reach quorum");
    let variant = Certificate::new_unverified(&committee, header, votes[1..].to_vec())
        .expect("3 of 4 votes reach quorum");

    let output_for = |leader: Certificate| ConsensusOutput {
        sub_dag: Arc::new(CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
        )),
        parent_hash: B256::ZERO,
        number: 1,
        close_epoch: true,
        ..Default::default()
    };
    let canonical_output = output_for(canonical);
    let variant_output = output_for(variant);

    let canonical_header = canonical_output.consensus_header();
    let variant_header = variant_output.consensus_header();

    // Accept check 1 (`request_consensus`): the reply's digest must equal the requested hash.
    // The syncing node asked for the canonical header's hash; the variant matches it, so the hash
    // check cannot reject it.
    assert_eq!(
        variant_header.digest(),
        canonical_header.digest(),
        "the variant header answers the request for the canonical hash"
    );

    // Accept check 2 (`get_consensus_header`): the header must verify against the committee keys.
    // Both are valid quorums, so both pass and the node stores whichever it was served.
    canonical_header.verify_header_with_keys(&keys).expect("canonical header verifies");
    variant_header.verify_header_with_keys(&keys).expect("variant header verifies");

    // Having accepted the variant, the node executes it. Below the activation number the closing
    // seed (stamped into the block's `extra_data`) hashes the leader's aggregate signature, so a
    // node served the variant builds a different epoch-closing block.
    assert_ne!(
        canonical_output.keccak_leader_sigs(),
        variant_output.keccak_leader_sigs(),
        "below activation: a node served the variant builds a different closing block"
    );

    // At or above the activation number the seed is the consensus header hash, identical on both,
    // so the same swap changes nothing. This is why removing the legacy derivation is safe only
    // once every network is past activation.
    assert_eq!(
        canonical_output.epoch_close_seed(),
        variant_output.epoch_close_seed(),
        "at/above activation: the swap no longer forks the closing block"
    );
}
