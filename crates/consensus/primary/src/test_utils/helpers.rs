//! Helper methods for creating useful structs during tests.

use indexmap::IndexMap;
use rand::{
    distr::{Bernoulli, Distribution as _},
    rngs::StdRng,
    Rng, RngCore, SeedableRng as _,
};
use rayls_execution_evm::{
    test_utils::{batch, TransactionFactory},
    RethChainSpec,
};
use rayls_infrastructure_types::{
    test_chain_spec_arc, test_genesis, to_intent_message, Address, AuthorityIdentifier, Batch,
    BlockHash, BlsKeypair, BlsSignature, Bytes, Certificate, CertificateDigest, Committee, Epoch,
    ExecHeader, Hash as _, HeaderBuilder, ProtocolSignature, Round, VotingPower, WorkerId, U256,
};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    ops::RangeInclusive,
    sync::Arc,
};
use tempfile::TempDir;

/// Create a temporary directory that is removed on drop.
pub fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to open temporary directory")
}

/// Generate a random BLS keypair.
pub fn random_key() -> BlsKeypair {
    BlsKeypair::generate(&mut rand::rngs::StdRng::from_os_rng())
}

/// Create a header payload of `number_of_batches` random batch digests for worker 0.
pub fn fixture_payload(number_of_batches: u8) -> IndexMap<BlockHash, WorkerId> {
    let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::new();

    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    for _ in 0..number_of_batches {
        let batch_digest = batch(chain.clone()).digest();

        payload.insert(batch_digest, 0);
    }

    payload
}

/// Create a header payload of `number_of_batches` batch digests drawn from `rand`.
pub fn fixture_payload_with_rand<R: Rng + ?Sized>(
    number_of_batches: u8,
    rand: &mut R,
) -> IndexMap<BlockHash, WorkerId> {
    let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::new();

    for _ in 0..number_of_batches {
        let batch_digest = batch_with_rand(rand, 0).digest();

        payload.insert(batch_digest, 0);
    }

    payload
}

/// Create a transaction with a randomly generated keypair.
pub fn transaction_with_rand<R: Rng + ?Sized>(rand: &mut R) -> Bytes {
    let mut tx_factory = TransactionFactory::new_random_from_seed(rand);
    let chain = test_chain_spec_arc();
    // TODO: this is excessively high, but very unlikely to ever fail
    let gas_price = 875000000;
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // random transaction
    tx_factory.create_eip1559_encoded(
        chain,
        None,
        gas_price,
        Some(Address::ZERO),
        value,
        Bytes::new(),
    )
}

/// Create a two-transaction batch for `worker_id` with transactions drawn from `rand`.
pub fn batch_with_rand<R: Rng + ?Sized>(rand: &mut R, worker_id: WorkerId) -> Batch {
    Batch::new_for_test(
        vec![transaction_with_rand(rand), transaction_with_rand(rand)],
        ExecHeader::default(),
        worker_id,
        0,
        0,
    )
}

/// Creates one certificate per authority starting and finishing at the specified rounds
/// (inclusive).
///
/// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
/// of digests to be used as parents for the certificates of the next round.
///
/// Note : the certificates are unsigned
pub fn make_optimal_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_certificates(committee, range, initial_parents, ids, 0.0)
}

/// Outputs rounds worth of certificates with optimal parents that are signed.
pub fn make_optimal_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, BlsKeypair)],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_signed_certificates(range, initial_parents, committee, keys, 0.0)
}

/// Bernoulli-samples from a set of ancestors passed as a argument,
fn this_cert_parents(
    ancestors: &BTreeSet<CertificateDigest>,
    failure_prob: f64,
) -> BTreeSet<CertificateDigest> {
    std::iter::from_fn(|| {
        let f: f64 = rand::rng().random();
        Some(f > failure_prob)
    })
    .take(ancestors.len())
    .zip(ancestors)
    .flat_map(|(parenthood, parent)| parenthood.then_some(*parent))
    .collect::<BTreeSet<_>>()
}

/// Utility for making several rounds worth of certificates through iterated parenthood sampling.
/// The making of individual certificates once parents are figured out is delegated to the
/// `make_one_certificate` argument
fn rounds_of_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
    make_one_certificate: impl Fn(
        AuthorityIdentifier,
        Round,
        BTreeSet<CertificateDigest>,
    ) -> (CertificateDigest, Certificate),
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for id in ids {
            let this_cert_parents = this_cert_parents(&parents, failure_probability);

            let (digest, certificate) = make_one_certificate(id.clone(), round, this_cert_parents);
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let generator = |pk, round, parents| mock_certificate(committee, pk, round, parents);

    rounds_of_certificates(range, initial_parents, ids, failure_probability, generator)
}

/// Creates certificates for the provided rounds but also having slow nodes.
///
/// `range`: the rounds for which we intend to create the certificates for
/// `initial_parents`: the parents to use when start creating the certificates
/// `keys`: the authorities for which it will create certificates for
/// `slow_nodes`: the authorities which are considered slow. Being a slow authority means that we
/// will  still create certificates for them on each round, but no other authority from higher round
/// will refer to those certificates. The number (by stake) of slow_nodes can not be > f , as
/// otherwise no valid graph will be produced.
pub fn make_certificates_with_slow_nodes(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: Vec<Certificate>,
    names: &[AuthorityIdentifier],
    slow_nodes: &[(AuthorityIdentifier, f64)],
) -> (VecDeque<Certificate>, Vec<Certificate>) {
    let mut rand = StdRng::seed_from_u64(1);

    // ensure provided slow nodes do not account > f
    let slow_nodes_voting_power: VotingPower =
        slow_nodes.iter().map(|(key, _)| committee.authority(key).unwrap().voting_power()).sum();

    assert!(slow_nodes_voting_power < committee.validity_threshold());

    let mut certificates = VecDeque::new();
    let mut parents = initial_parents;
    let mut next_parents = Vec::new();

    for round in range {
        next_parents.clear();
        for name in names {
            let this_cert_parents = this_cert_parents_with_slow_nodes(
                name,
                parents.clone(),
                slow_nodes,
                &mut rand,
                committee,
            );

            let (_, certificate) =
                mock_certificate(committee, name.clone(), round, this_cert_parents);
            certificates.push_back(certificate.clone());
            next_parents.push(certificate);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

#[derive(Debug, Clone, Copy)]
pub enum TestLeaderSupport {
    /// There will be support for the leader, but less than f+1
    Weak,
    /// There will be strong support for the leader, meaning >= f+1
    Strong,
    /// Leader will be completely ommitted by the voters
    NoSupport,
}

#[derive(Debug)]
pub struct TestLeaderConfiguration {
    /// The round of the leader
    pub round: Round,
    /// The leader id. That allow us to explicitly dictate which we consider the leader to be
    pub authority: AuthorityIdentifier,
    /// If true then the leader for that round will not be created at all
    pub should_omit: bool,
    /// The support that this leader should receive from the voters of next round
    pub support: Option<TestLeaderSupport>,
}

/// Creates fully connected DAG for the dictated rounds but with specific conditions for the
/// leaders.
///
/// By providing the `leader_configuration` we can dictate the setup for specific leaders
/// of specific rounds. For a leader the following can be configured:
/// * whether a leader will exist or not for a round
/// * whether a leader will receive enough support from the next round
pub fn make_certificates_with_leader_configuration(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    names: &[AuthorityIdentifier],
    leader_configurations: HashMap<Round, TestLeaderConfiguration>,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    for round in leader_configurations.keys() {
        assert_eq!(round % 2, 0, "Leaders are elected only on even rounds");
    }

    let mut certificates: VecDeque<Certificate> = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();

        for name in names {
            // should we produce the leader of that round?
            if let Some(leader_config) = leader_configurations.get(&round) {
                if leader_config.should_omit && leader_config.authority == *name {
                    // just skip and don't create the certificate for this authority
                    continue;
                }
            }

            // we now check for the leader of previous round. If should not be omitted we need to
            // check on the support we are supposed to provide
            let cert_parents = if round > 0 {
                if let Some(leader_config) = leader_configurations.get(&(round - 1)) {
                    match leader_config.support {
                        Some(TestLeaderSupport::Weak) => {
                            // find the leader from the previous round
                            let leader_certificate = certificates
                                .iter()
                                .find(|c| {
                                    c.round() == round - 1 && c.origin() == &leader_config.authority
                                })
                                .unwrap();

                            // check whether anyone from the current round already included it
                            // if yes, then we should remove it and not vote again.
                            if certificates.iter().any(|c| {
                                c.round() == round
                                    && c.header().parents().contains(&leader_certificate.digest())
                            }) {
                                let mut p = parents.clone();
                                p.remove(&leader_certificate.digest());
                                p
                            } else {
                                // otherwise return all the parents
                                parents.clone()
                            }
                        }
                        Some(TestLeaderSupport::Strong) => {
                            // just return the whole parent set so we can vote for it
                            parents.clone()
                        }
                        Some(TestLeaderSupport::NoSupport) => {
                            // remove the leader from the set of parents
                            let c = certificates
                                .iter()
                                .find(|c| {
                                    c.round() == round - 1 && c.origin() == &leader_config.authority
                                })
                                .unwrap();
                            let mut p = parents.clone();
                            p.remove(&c.digest());
                            p
                        }
                        None => parents.clone(),
                    }
                } else {
                    parents.clone()
                }
            } else {
                parents.clone()
            };

            // Create the certificates
            let (_, certificate) = mock_certificate(committee, name.clone(), round, cert_parents);
            certificates.push_back(certificate.clone());
            next_parents.insert(certificate.digest());
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// Returns the parents that should be used as part of a newly created certificate.
///
/// The `slow_nodes` parameter is used to dictate which parents to exclude and not use. The slow
/// node will not be used under some probability which is provided as part of the tuple.
/// If probability to use it is 0.0, then the parent node will NEVER be used.
/// If probability to use it is 1.0, then the parent node will ALWAYS be used.
/// We always make sure to include our "own" certificate, thus the `name` property is needed.
pub fn this_cert_parents_with_slow_nodes(
    authority_id: &AuthorityIdentifier,
    ancestors: Vec<Certificate>,
    slow_nodes: &[(AuthorityIdentifier, f64)],
    rand: &mut StdRng,
    committee: &Committee,
) -> BTreeSet<CertificateDigest> {
    let mut parents = BTreeSet::new();
    let mut not_included = Vec::new();
    let mut total_stake = 0;

    for parent in ancestors {
        let authority = committee.authority(parent.origin()).unwrap();

        // Identify if the parent is within the slow nodes - and is not the same author as the
        // one we want to create the certificate for.
        if let Some((_, inclusion_probability)) =
            slow_nodes.iter().find(|(id, _)| id != authority_id && id == parent.header().author())
        {
            let b = Bernoulli::new(*inclusion_probability).unwrap();
            let should_include = b.sample(rand);

            if should_include {
                parents.insert(parent.digest());
                total_stake += authority.voting_power();
            } else {
                not_included.push(parent);
            }
        } else {
            // just add it directly as it is not within the slow nodes or we are the
            // same author.
            parents.insert(parent.digest());
            total_stake += authority.voting_power();
        }
    }

    // ensure we'll have enough parents (2f + 1)
    while total_stake < committee.quorum_threshold() {
        let parent = not_included.pop().unwrap();
        let authority = committee.authority(parent.origin()).unwrap();

        total_stake += authority.voting_power();

        parents.insert(parent.digest());
    }

    assert!(
        committee.reached_quorum(total_stake),
        "Not enough parents by stake provided. Expected at least {} but instead got {}",
        committee.quorum_threshold(),
        total_stake
    );

    parents
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates_with_epoch(
    committee: &Committee,
    range: RangeInclusive<Round>,
    epoch: Epoch,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) =
                mock_certificate_with_epoch(committee, name.clone(), round, epoch, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// make rounds worth of signed certificates with the sampled number of parents
pub fn make_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, BlsKeypair)],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let ids = keys.iter().map(|(authority, _)| authority.clone()).collect::<Vec<_>>();
    let generator = |pk, round, parents| signed_cert_for_test(keys, pk, round, parents, committee);

    rounds_of_certificates(range, initial_parents, &ids[..], failure_probability, generator)
}

/// Create an unsigned certificate whose header payload is drawn from `rand`.
pub fn mock_certificate_with_rand<R: RngCore + ?Sized>(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    rand: &mut R,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(0)
        .parents(parents)
        .payload(fixture_payload_with_rand(1, rand))
        .build();
    let certificate = Certificate::new_unsigned_for_test(committee, header, Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates a badly signed certificate from its given round, origin and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    mock_certificate_with_epoch(committee, origin, round, 0, parents)
}

/// Creates a badly signed certificate from its given round, epoch, origin, and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate_with_epoch(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(epoch)
        .parents(parents)
        .payload(fixture_payload(1))
        .build();
    let certificate = Certificate::new_unsigned_for_test(committee, header, Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates one signed certificate from a set of signers - the signers must include the origin
pub fn signed_cert_for_test(
    signers: &[(AuthorityIdentifier, BlsKeypair)],
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    committee: &Committee,
) -> (CertificateDigest, Certificate) {
    let header = HeaderBuilder::default()
        .author(origin)
        .payload(fixture_payload(1))
        .round(round)
        .epoch(0)
        .parents(parents)
        .build();

    let cert = Certificate::new_unsigned_for_test(committee, header.clone(), Vec::new())
        .expect("new unsigned cert for tests");

    let votes = signers
        .iter()
        .map(|(name, signer)| {
            (
                name.clone(),
                BlsSignature::new_secure(&to_intent_message(cert.header().digest()), signer),
            )
        })
        .collect();

    let cert = Certificate::new_unverified(committee, header, votes).unwrap();
    (cert.digest(), cert)
}
