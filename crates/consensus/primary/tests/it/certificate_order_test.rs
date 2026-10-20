//! Certificate order

use indexmap::IndexMap;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlockNumHash, BlsPublicKey, BlsSignature, Certificate, Committee, Header,
    Vote, VotingPower,
};
use rayls_testing_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

#[tokio::test]
async fn test_certificate_signers_are_ordered() {
    // GIVEN
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .voting_power_distribution(vec![1, 1, 1, 1].into()) // all validators are equal.
        .build();
    let committee: Committee = fixture.committee();

    // Need to sort validator by there BlsPublicKeys.
    let authorities: BTreeMap<BlsPublicKey, &AuthorityFixture<MemDatabase>> =
        fixture.authorities().map(|a| (a.primary_public_key(), a)).collect();
    let authorities: Vec<&AuthorityFixture<MemDatabase>> = authorities.values().copied().collect();
    let total_stake: u64 = authorities.iter().map(|a| a.authority().voting_power()).sum();
    assert_eq!(total_stake, 4);
    // authorities are ordered by keys so the stake may not be 1, 2, 3, 4...
    let last_three_stake: u64 = authorities[1..].iter().map(|a| a.authority().voting_power()).sum();

    // The authority that creates the Header
    let authority = authorities[0];

    let header = Header::new(
        authority.id(),
        1,
        1,
        IndexMap::new(),
        BTreeSet::new(),
        BlockNumHash::default(),
        None,
    );

    // WHEN
    let mut votes: Vec<(AuthorityIdentifier, BlsSignature)> = Vec::new();
    let mut sorted_signers: Vec<BlsPublicKey> = Vec::new();

    // The authorities on position 1, 2, 3 are the ones who would sign
    for authority in &authorities[1..=3] {
        sorted_signers.push(authority.primary_public_key());

        let vote =
            Vote::new(&header.clone(), authority.id(), authority.consensus_config().key_config());
        votes.push((vote.author().clone(), *vote.signature()));
    }

    // Just shuffle to ensure that any underlying sorting will work correctly
    votes.shuffle(&mut StdRng::from_os_rng());

    // Create a certificate
    let certificate = Certificate::new_unverified(&committee, header, votes).unwrap();

    let (stake, signers) = certificate.signed_by(&committee.bls_keys());

    // THEN
    assert_eq!(signers.len(), 3);

    // AND authorities public keys are returned in order
    assert_eq!(signers, sorted_signers);

    assert_eq!(stake, last_three_stake as VotingPower);
}
