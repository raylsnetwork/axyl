//! One integration test through the real vote path: the collector seals on its own thread, the
//! proposal carries the claim, and the far node's gate accepts it.

#![allow(unused_crate_dependencies)]

use rayls_consensus_network::{ConsensusNetwork, NetworkMetrics};
use rayls_consensus_primary::{
    consensus::{LeaderSchedule, LeaderSwapTable},
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, Primary, StateSynchronizer,
};
use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_storage::{
    mem_db::MemDatabase, CertificateStore, ProposerStore, VoteDigestStore,
};
use rayls_infrastructure_types::{
    init_signer_claims_activation_epoch, verify_signer_claims_region, AuthorityIdentifier,
    BlsPublicKey, Certificate, CertificateDigest, Committee, Epoch, ExecHeader, Header, Multiaddr,
    NetworkPublicKey, SealedHeader, SignerClaim, TaskManager, MAX_SIGNER_CLAIMS,
    SIGNER_CLAIM_LAG_ROUNDS,
};
use rayls_testing_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, Once},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

const ACTIVATION_EPOCH: Epoch = 5_000;
const CLAIM_LABELS: [&str; 7] = [
    "after_freeze",
    "after_carry",
    "queue_full",
    "handoff_dropped",
    "epoch_end",
    "cap_eviction",
    "rank_miss",
];

fn activation() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| init_signer_claims_activation_epoch(ACTIVATION_EPOCH));
}

struct Node {
    authority_id: AuthorityIdentifier,
    bls_key: BlsPublicKey,
    config: ConsensusConfig<MemDatabase>,
    metrics: Arc<PrimaryMetrics>,
    handle: PrimaryNetworkHandle,
    bus: ConsensusBus,
}

// The proposer waits on replay completion, so the node stays idle until its peer is linked.
// Without this the first certificates are published into a mesh that has no peers in it.
impl Node {
    fn start(&self) {
        self.bus.execution_replay_complete().send_replace(true);
    }
}

async fn node(
    authority: &AuthorityFixture<MemDatabase>,
    committee: &Committee,
    tasks: &TaskManager,
) -> Node {
    let config = authority.consensus_config();
    let bus = ConsensusBus::new();
    let metrics = bus.primary_metrics().node_metrics.clone();
    // There is no engine in this harness: the tip is a dummy, and replay is signalled complete
    // only once the peer is linked (see `Node::start`).
    bus.recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(SealedHeader::seal_slow(ExecHeader::default())));

    let (tx_events, rx_events) = mpsc::channel(32);
    let swarm = ConsensusNetwork::new_for_primary(
        config.network_config(),
        tx_events,
        config.key_config().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        config.primary_address(),
        Arc::new(NetworkMetrics::default()),
    )
    .expect("swarm");
    let handle = PrimaryNetworkHandle::new(swarm.network_handle());
    tokio::spawn(async move {
        swarm.run().await.expect("swarm run");
    });

    let synchronizer = StateSynchronizer::new(config.clone(), bus.clone(), tasks.get_spawner());
    PrimaryNetwork::new(
        rx_events,
        handle.clone(),
        config.clone(),
        bus.clone(),
        synchronizer.clone(),
        tasks.get_spawner(),
    )
    .spawn(&tasks.get_spawner());
    let mut primary = Primary::new(config.clone(), &bus, handle.clone(), synchronizer);
    primary.spawn(
        config.clone(),
        &bus,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        tasks,
    );

    handle.inner_handle().start_listening(config.primary_address()).await.expect("listen");
    // Consensus gossip travels on the primary topic with the committee as the authorized
    // publishers; without the subscription nothing is delivered and every round stalls.
    handle
        .inner_handle()
        .subscribe_with_publishers(
            LibP2pConfig::primary_topic(),
            HashSet::from_iter(committee.bls_keys()),
        )
        .await
        .expect("subscribe");

    Node {
        authority_id: config.authority_id().expect("authority id"),
        bls_key: *authority.authority().protocol_key(),
        config,
        metrics,
        handle,
        bus,
    }
}

async fn connect(
    from: &PrimaryNetworkHandle,
    to: &Node,
    to_addr: Multiaddr,
    to_network_key: NetworkPublicKey,
) {
    from.inner_handle().add_explicit_peer(to.bls_key, to_network_key, to_addr).await.expect("peer");
    // Discovery may have connected already; the peer table is what the request needs.
    let _ = from.inner_handle().dial_by_bls(to.bls_key).await;
}

async fn poll<F>(within: Duration, pred: F) -> bool
where
    F: Fn() -> bool,
{
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn carrier_over_the_wire(far: &Node, author: &AuthorityIdentifier) -> Header {
    let until = Instant::now() + Duration::from_secs(60);
    loop {
        let header =
            far.config.node_storage().get_last_proposed_by_authority(author.clone()).expect("read");
        let info = far.config.node_storage().read_vote_info(author).expect("read");
        // Tied to the vote: this is the exact header the gate ran on.
        if header.as_ref().is_some_and(|header| {
            !header.signer_claims.is_empty()
                && info.as_ref().is_some_and(|info| info.round() == header.round())
        }) {
            return header.expect("a header carrying claims");
        }
        if Instant::now() > until {
            panic!("the far node never voted for a header carrying claims");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_vote_path_carries_a_claim_the_far_node_accepts() {
    activation();

    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(2).unwrap())
        .epoch(ACTIVATION_EPOCH)
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();

    let tasks = TaskManager::default();
    let author =
        node(fixture.authority_fixture_by_idx(0).expect("first"), &committee, &tasks).await;
    let far = node(fixture.authority_fixture_by_idx(1).expect("second"), &committee, &tasks).await;

    connect(&author.handle, &far, far.config.primary_address(), far.config.primary_networkkey())
        .await;
    connect(
        &far.handle,
        &author,
        author.config.primary_address(),
        author.config.primary_networkkey(),
    )
    .await;

    // Nothing proposes until the peer is linked: a certificate published into a mesh with no
    // peers in it is simply lost.
    author.start();
    far.start();

    // Round 0 is genesis, so the first header that may carry anything is round K + 1.
    let first_carrier_round = SIGNER_CLAIM_LAG_ROUNDS + 1;
    let author_id = author.authority_id.clone();
    assert!(
        poll(Duration::from_secs(60), || author.metrics.claims_carried.get() > 0).await,
        "no claim was carried by round {first_carrier_round}"
    );

    let carrier = carrier_over_the_wire(&far, &author_id).await;
    assert!(carrier.signer_claims.len() <= MAX_SIGNER_CLAIMS);
    assert!(carrier.round() >= first_carrier_round);
    assert!(
        verify_signer_claims_region(
            &carrier.signer_claims,
            carrier.epoch(),
            carrier.round(),
            &committee
        )
        .is_ok(),
        "the far node accepted claims it decoded from the wire"
    );
    assert!(
        carrier
            .signer_claims
            .iter()
            .all(|claim: &SignerClaim| claim.target_round + SIGNER_CLAIM_LAG_ROUNDS
                <= carrier.round())
    );

    // Nothing went wrong on either node.
    assert_eq!(far.metrics.claim_headers_rejected.get(), 0, "the gate refused nothing");
    assert_eq!(author.metrics.claim_headers_rejected.get(), 0);
    assert!(author.metrics.claims_carried.get() > 0, "claims_carried must be above zero");
    for label in CLAIM_LABELS {
        assert_eq!(
            author.metrics.claim_events_dropped.with_label_values(&[label]).get(),
            0,
            "nothing should have been dropped ({label})"
        );
    }

    // The far node's vote is for that carrier, so the gate ran on exactly these claims.
    let info = far
        .config
        .node_storage()
        .read_vote_info(&author_id)
        .expect("read")
        .expect("the far node voted");
    assert_eq!(info.round(), carrier.round());

    // The carrier reached quorum, so its claims were committed rather than handed out into nothing.
    let digest = CertificateDigest::new(carrier.digest().into());
    assert!(
        poll(Duration::from_secs(60), || far
            .config
            .node_storage()
            .read(digest)
            .expect("read")
            .is_some())
        .await,
        "no certificate for the carrier"
    );
    let certificate: Certificate = far
        .config
        .node_storage()
        .read(digest)
        .expect("read")
        .expect("a certificate for the carrier");
    assert_eq!(certificate.round(), carrier.round());
    assert_eq!(certificate.header().author(), &author_id);
}
