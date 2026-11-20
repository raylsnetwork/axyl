//! The gate in `vote_inner`: a peer's claims are verified before we sign a vote.

#![allow(unused_crate_dependencies)]

use rayls_consensus_network::{ConsensusNetwork, NetworkMetrics};
use rayls_consensus_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle, RequestVoteResult},
    ConsensusBus, StateSynchronizer,
};
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore, VoteDigestStore};
use rayls_infrastructure_types::{
    encode, header_is_post_activation, init_signer_claims_activation_epoch, now,
    AuthorityIdentifier, BlockHash, BlockNumHash, BlsAggregateSignature, BlsPublicKey,
    BlsSignature, Certificate, CertificateDigest, Committee, Epoch, ExecHeader, Header,
    HeaderBuilder, Round, SealedHeader, SignerClaim, TaskManager, VoteDigest, MAX_SIGNER_CLAIMS,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Once},
    time::Duration,
};
use tokio::sync::mpsc;

const ACTIVATION_EPOCH: Epoch = 5_000;

fn activation() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| init_signer_claims_activation_epoch(ACTIVATION_EPOCH));
}

struct Gate {
    fixture: CommitteeFixture<MemDatabase>,
    handler_bus: ConsensusBus,
    handler_config: ConsensusConfig<MemDatabase>,
    requester: PrimaryNetworkHandle,
    handler_key: BlsPublicKey,
    author: AuthorityIdentifier,
    anchor: BlockNumHash,
    chain: Vec<Certificate>,
    _tasks: TaskManager,
}

async fn gate(epoch: Epoch) -> Gate {
    activation();

    let fixture =
        CommitteeFixture::builder(MemDatabase::default).epoch(epoch).randomize_ports(true).build();

    let handler_auth = fixture.first_authority();
    let author_auth = fixture.authority_fixture_by_idx(1).expect("second authority");
    let handler_config = handler_auth.consensus_config();
    let author_config = author_auth.consensus_config();
    let handler_key = *handler_auth.authority().protocol_key();
    let author = author_auth.id();

    let tasks = TaskManager::default();
    let bus = ConsensusBus::new();
    let parent = SealedHeader::seal_slow(ExecHeader::default());
    let anchor = parent.num_hash();
    bus.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(parent));

    let (tx_events, rx_events) = mpsc::channel(16);
    let handler_swarm = ConsensusNetwork::new_for_primary(
        handler_config.network_config(),
        tx_events,
        handler_config.key_config().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        handler_config.primary_address(),
        Arc::new(NetworkMetrics::default()),
    )
    .expect("handler swarm");
    let handler_handle = PrimaryNetworkHandle::new(handler_swarm.network_handle());
    tokio::spawn(async move {
        handler_swarm.run().await.expect("handler swarm run");
    });

    let synchronizer =
        StateSynchronizer::new(handler_config.clone(), bus.clone(), tasks.get_spawner());
    PrimaryNetwork::new(
        rx_events,
        handler_handle.clone(),
        handler_config.clone(),
        bus.clone(),
        synchronizer,
        tasks.get_spawner(),
    )
    .spawn(&tasks.get_spawner());

    handler_handle
        .inner_handle()
        .start_listening(handler_config.primary_address())
        .await
        .expect("handler listening");

    // The vote path reads the carrier's parents from storage, so the chain is seeded before the
    // first request; nothing in the gate itself writes it.
    let chain = cert_chain(&fixture, epoch, anchor);
    handler_config.node_storage().write_all(chain.iter().cloned()).expect("store the parent chain");

    let (tx_author_events, mut rx_author_events) = mpsc::channel(16);
    let author_swarm = ConsensusNetwork::new_for_primary(
        author_config.network_config(),
        tx_author_events,
        author_config.key_config().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        author_config.primary_address(),
        Arc::new(NetworkMetrics::default()),
    )
    .expect("author swarm");
    let requester = PrimaryNetworkHandle::new(author_swarm.network_handle());
    tokio::spawn(async move {
        author_swarm.run().await.expect("author swarm run");
    });
    tokio::spawn(async move { while rx_author_events.recv().await.is_some() {} });
    requester
        .inner_handle()
        .start_listening(author_config.primary_address())
        .await
        .expect("author listening");
    // A trusted peer binds the peer id to the handler's BLS key on insert, which is what makes the
    // request route; the dial itself can report a timeout while the connection lands afterwards.
    let _ = requester
        .inner_handle()
        .add_trusted_peer_and_dial(
            handler_key,
            handler_config.primary_networkkey(),
            handler_config.primary_address(),
        )
        .await;
    // Requests before the connection is up time out, so wait for it rather than sleep.
    let connected = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if requester.inner_handle().connected_peer_count().await.unwrap_or(0) >= 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(connected, "the requester must reach the handler");

    Gate {
        fixture,
        handler_bus: bus,
        handler_config,
        requester,
        handler_key,
        author,
        anchor,
        chain,
        _tasks: tasks,
    }
}

fn rank_of(committee: &Committee, id: &AuthorityIdentifier) -> u32 {
    let authority = committee.authority(id).expect("authority in the committee");
    committee
        .bls_keys()
        .iter()
        .position(|key| key == authority.protocol_key())
        .expect("authority is in bls_keys()") as u32
}

fn header_at(
    author: AuthorityIdentifier,
    epoch: Epoch,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    claims: Vec<SignerClaim>,
    anchor: BlockNumHash,
) -> Header {
    HeaderBuilder::default()
        .author(author)
        .round(round)
        .epoch(epoch)
        .created_at(now())
        .parents(parents)
        .latest_execution_block(anchor)
        .build()
        .with_signer_claims(claims)
}

fn target_header(fixture: &CommitteeFixture<MemDatabase>, epoch: Epoch, round: Round) -> Header {
    HeaderBuilder::default()
        .author(fixture.last_authority().id())
        .round(round)
        .epoch(epoch)
        .created_at(now())
        .payload(Default::default())
        .parents(Default::default())
        .build()
}

fn claim_over(
    fixture: &CommitteeFixture<MemDatabase>,
    target: &Header,
    signers: usize,
) -> SignerClaim {
    let committee = fixture.committee();
    let mut by_rank: BTreeMap<u32, BlsSignature> = BTreeMap::new();
    for vote in fixture.votes(target).into_iter().take(signers) {
        by_rank.insert(rank_of(&committee, &vote.author), vote.signature);
    }
    SignerClaim {
        target_digest: CertificateDigest::new(target.digest().into()),
        target_epoch: target.epoch(),
        target_round: target.round(),
        ranks: by_rank.keys().copied().collect(),
        signature: BlsAggregateSignature::aggregate(
            &by_rank.values().copied().collect::<Vec<_>>(),
            true,
        )
        .expect("aggregate")
        .to_signature(),
    }
}

fn full_claim(fixture: &CommitteeFixture<MemDatabase>, target: &Header) -> SignerClaim {
    let committee = fixture.committee();
    let mut by_rank: BTreeMap<u32, BlsSignature> = BTreeMap::new();
    for authority in fixture.authorities() {
        by_rank.insert(rank_of(&committee, &authority.id()), authority.vote(target).signature);
    }
    SignerClaim {
        target_digest: CertificateDigest::new(target.digest().into()),
        target_epoch: target.epoch(),
        target_round: target.round(),
        ranks: by_rank.keys().copied().collect(),
        signature: BlsAggregateSignature::aggregate(
            &by_rank.values().copied().collect::<Vec<_>>(),
            true,
        )
        .expect("aggregate")
        .to_signature(),
    }
}

// Certificates for rounds 1..3, from three authorities each: enough stake for a quorum, unique
// authors, and every round pointing at the round before it. The handler's vote path reads the
// carrier's parents from storage, so these are written there up front.
fn cert_chain(
    fixture: &CommitteeFixture<MemDatabase>,
    epoch: Epoch,
    anchor: BlockNumHash,
) -> Vec<Certificate> {
    let mut parents: BTreeSet<CertificateDigest> = fixture.genesis().collect();
    let mut chain = Vec::new();
    for round in 1..=3 {
        let round_certs: Vec<Certificate> = fixture
            .authorities()
            .take(3)
            .map(|authority| {
                let header =
                    header_at(authority.id(), epoch, round, parents.clone(), Vec::new(), anchor);
                fixture.certificate(&header)
            })
            .collect();
        parents = round_certs
            .iter()
            .map(|cert| CertificateDigest::new(cert.header().digest().into()))
            .collect::<BTreeSet<_>>();
        chain.extend(round_certs);
    }
    chain
}

struct Carrier {
    header: Header,
    certificates: Vec<Certificate>,
}

fn carrier(gate: &Gate, epoch: Epoch, claims: Vec<SignerClaim>) -> Carrier {
    // The carrier sits at round 4 and parents the round-3 certificates.
    let certificates: Vec<Certificate> =
        gate.chain.iter().filter(|cert| cert.round() == 3).cloned().collect();
    let parents = certificates
        .iter()
        .map(|cert| CertificateDigest::new(cert.header().digest().into()))
        .collect::<BTreeSet<_>>();
    Carrier {
        header: header_at(gate.author.clone(), epoch, 4, parents, claims, gate.anchor),
        certificates,
    }
}

fn rejected_count(gate: &Gate) -> u64 {
    gate.handler_bus.primary_metrics().node_metrics.claim_headers_rejected.get()
}

async fn vote(gate: &Gate, carrier: Carrier) -> Result<RequestVoteResult, String> {
    gate.requester
        .request_vote(gate.handler_key, carrier.header, carrier.certificates)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn test_gate_runs_when_no_stored_vote_info() {
    let gate = gate(ACTIVATION_EPOCH).await;
    // Nothing is stored for this author, so the `:723` read and the cached early return are
    // skipped.
    assert!(gate
        .handler_config
        .node_storage()
        .read_vote_info(&gate.author)
        .expect("read")
        .is_none());

    let target = target_header(&gate.fixture, ACTIVATION_EPOCH, 1);
    let claim = claim_over(&gate.fixture, &target, 2);
    let res = vote(&gate, carrier(&gate, ACTIVATION_EPOCH, vec![claim])).await;
    assert!(res.is_err(), "a header whose claims do not verify must not be voted for");
    assert_eq!(rejected_count(&gate), 1);
    // With the check placed after the `vote_info` read this path would have signed the vote.
    assert!(gate
        .handler_config
        .node_storage()
        .read_vote_info(&gate.author)
        .expect("read")
        .is_none());
}

#[tokio::test]
async fn test_gate_rejects_duplicate_ranks() {
    let gate = gate(ACTIVATION_EPOCH).await;
    let target = target_header(&gate.fixture, ACTIVATION_EPOCH, 1);
    let claim = claim_over(&gate.fixture, &target, 3);
    let res = vote(
        &gate,
        carrier(&gate, ACTIVATION_EPOCH, vec![SignerClaim { ranks: vec![0, 1, 1], ..claim }]),
    )
    .await;
    assert!(res.is_err());
    assert_eq!(rejected_count(&gate), 1);
}

#[tokio::test]
async fn test_gate_rejects_weight_below_quorum() {
    let gate = gate(ACTIVATION_EPOCH).await;
    let committee = gate.fixture.committee();
    assert!(
        committee.quorum_threshold() > 2,
        "two single-stake signers must weigh less than a quorum"
    );
    let target = target_header(&gate.fixture, ACTIVATION_EPOCH, 1);
    let claim = claim_over(&gate.fixture, &target, 2);
    assert_eq!(claim.ranks.len(), 2, "two signers is 2f+1 by count for this committee");
    let res = vote(&gate, carrier(&gate, ACTIVATION_EPOCH, vec![claim])).await;
    assert!(res.is_err());
    assert_eq!(rejected_count(&gate), 1);
}

#[tokio::test]
async fn test_gate_rejects_bad_aggregate() {
    let gate = gate(ACTIVATION_EPOCH).await;
    let committee = gate.fixture.committee();
    let target = target_header(&gate.fixture, ACTIVATION_EPOCH, 1);
    let claim = claim_over(&gate.fixture, &target, 2);
    let mut ranks = claim.ranks.clone();
    ranks.push(rank_of(&committee, &gate.author));
    assert_eq!(ranks.len(), claim.ranks.len() + 1, "the extra rank adds weight, not signatures");
    let res =
        vote(&gate, carrier(&gate, ACTIVATION_EPOCH, vec![SignerClaim { ranks, ..claim }])).await;
    assert!(res.is_err());
    assert_eq!(rejected_count(&gate), 1);
}

#[tokio::test]
async fn test_gate_accepts_empty_region() {
    let gate = gate(ACTIVATION_EPOCH).await;
    let carrier = carrier(&gate, ACTIVATION_EPOCH, Vec::new());
    let digest = carrier.header.digest();
    let res = vote(&gate, carrier).await;
    assert!(matches!(res, Ok(RequestVoteResult::Vote(_))), "an empty region is a valid region");
    assert_eq!(rejected_count(&gate), 0);
    let info = gate
        .handler_config
        .node_storage()
        .read_vote_info(&gate.author)
        .expect("read")
        .expect("a vote was written");
    assert_eq!(info.vote_digest(), VoteDigest::from(digest));
}

#[tokio::test]
async fn test_gate_ignores_pre_activation_headers() {
    let epoch = ACTIVATION_EPOCH - 1;
    let gate = gate(epoch).await;
    let target = target_header(&gate.fixture, epoch, 1);
    let claims = vec![claim_over(&gate.fixture, &target, 3)];
    let carrier = carrier(&gate, epoch, claims);
    assert!(!carrier.header.signer_claims.is_empty(), "the local header does carry claims");

    let res = vote(&gate, carrier).await;
    assert!(matches!(res, Ok(RequestVoteResult::Vote(_))));
    assert_eq!(rejected_count(&gate), 0);
}

#[tokio::test]
async fn test_vote_never_signs_header_with_unverifiable_claims() {
    let gate = gate(ACTIVATION_EPOCH).await;
    let target = target_header(&gate.fixture, ACTIVATION_EPOCH, 1);
    let claim = claim_over(&gate.fixture, &target, 3);
    // A claim naming a target the carrier cannot yet claim.
    let carrier = carrier(&gate, ACTIVATION_EPOCH, vec![SignerClaim { target_round: 4, ..claim }]);
    let digest = carrier.header.digest();
    let res = vote(&gate, carrier).await;
    assert!(res.is_err());
    assert_eq!(rejected_count(&gate), 1);

    // No vote for this header is stored, so nothing downstream can credit these claims.
    let info = gate.handler_config.node_storage().read_vote_info(&gate.author).expect("read");
    assert!(info.is_none_or(|info| info.vote_digest() != VoteDigest::from(digest)));
}

#[tokio::test]
async fn test_max_certificate_with_max_claims_encodes_under_max_rpc_message_size() {
    activation();
    let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(ACTIVATION_EPOCH).build();
    let committee = fixture.committee();

    let claims: Vec<SignerClaim> = (0..MAX_SIGNER_CLAIMS)
        .map(|i| full_claim(&fixture, &target_header(&fixture, ACTIVATION_EPOCH, 1 + i as Round)))
        .collect();
    assert_eq!(claims.len(), MAX_SIGNER_CLAIMS);
    assert!(claims.iter().all(|claim| claim.ranks.len() == committee.size()));

    let header = HeaderBuilder::default()
        .author(fixture.first_authority().id())
        .round(1 + MAX_SIGNER_CLAIMS as Round)
        .epoch(ACTIVATION_EPOCH)
        .created_at(now())
        .parents(fixture.genesis().collect())
        .latest_execution_block(BlockNumHash::new(0, BlockHash::ZERO))
        .build()
        .with_signer_claims(claims);
    assert!(header_is_post_activation(&header));

    let encoded = encode(&fixture.certificate(&header));
    assert!(encoded.len() < LibP2pConfig::default().max_rpc_message_size);
}
