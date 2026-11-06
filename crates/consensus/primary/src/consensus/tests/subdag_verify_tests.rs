//! Reproduction for the indirectly-verified sub-dag parent trust bug.
//!
//! `CommittedSubDag::verify_certificates_with_keys` verifies the leader against the committee
//! keys, then treats every certificate the leader points to as already verified and pushes it
//! through without checking it. A following node then persists those parents during catch-up
//! (`commit_catchup_output`). So a node accepts and stores a parent certificate that would fail
//! verification on its own, carrying whatever signature and verification state the peer sent.

use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    Certificate, CommittedSubDag, Hash as _, HeaderBuilder, ReputationScores,
    SignatureVerificationState,
};
use rayls_testing_test_utils_committee::CommitteeFixture;

/// An inquorate parent certificate, which `verify_cert` rejects on its own, is accepted by
/// `verify_certificates_with_keys` because the leader lists it as a parent. On current code the
/// call returns Ok and the unverified parent is in the result, ready to be persisted. A fix that
/// re-verifies the parents would reject it.
#[test]
fn inquorate_parent_cert_is_accepted_because_the_leader_lists_it() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let keys = committee.bls_keys();
    let authorities: Vec<_> = fixture.authorities().collect();

    // A parent header by authority 0 at round 1.
    let parent_header = HeaderBuilder::default()
        .author(authorities[0].id())
        .round(1)
        .epoch(0)
        .parents(Certificate::genesis(&committee).iter().map(|c| c.digest()).collect())
        .build();

    // Only one vote, so the certificate is inquorate: `verify_cert` must reject it on its own.
    let lone_vote = authorities[1].vote(&parent_header);
    let bad_parent = Certificate::new_unsigned_for_test(
        &committee,
        parent_header,
        vec![(lone_vote.author().clone(), *lone_vote.signature())],
    )
    .unwrap();
    assert!(
        bad_parent.clone().verify_cert(&keys).is_err(),
        "precondition: the inquorate parent must fail verification on its own"
    );

    // A quorum-signed leader at round 2 that lists the bad parent as one of its parents.
    let leader_header = HeaderBuilder::default()
        .author(authorities[1].id())
        .round(2)
        .epoch(0)
        .parents([bad_parent.digest()].into_iter().collect())
        .build();
    let leader = fixture.certificate(&leader_header);

    let sub_dag = CommittedSubDag::new(
        vec![bad_parent.clone()],
        leader,
        0,
        ReputationScores::default(),
        None,
    );

    // FAILS to reject on current code: the inquorate parent rides through as "indirectly verified".
    let verified = sub_dag
        .verify_certificates_with_keys(&keys)
        .expect("current code accepts the sub-dag even though the parent is inquorate");

    let stored_parent = verified
        .certificates
        .iter()
        .find(|c| c.digest() == bad_parent.digest())
        .expect("the inquorate parent is kept in the sub-dag and would be persisted");
    assert!(
        !matches!(
            stored_parent.signature_verification_state(),
            SignatureVerificationState::VerifiedDirectly(_)
        ),
        "the parent was never actually verified, yet it is accepted and stored"
    );
}
