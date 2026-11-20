//! The collector task: freeze markers, carries, epoch boundaries, caps and counted drops.

#![allow(unused_crate_dependencies)]

use rayls_consensus_network::{ConsensusNetwork, NetworkMetrics};
use rayls_consensus_primary::{
    claim_collector::CollectorHandle,
    consensus::{LeaderSchedule, LeaderSwapTable},
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    test_utils::random_key,
    ConsensusBus, Primary, StateSynchronizer,
};
use rayls_consensus_primary_metrics::{Metrics, PrimaryMetrics};
use rayls_infrastructure_config::{
    Config, ConsensusConfig, KeyConfig, LibP2pConfig, NetworkConfig,
};
use rayls_infrastructure_storage::{mem_db::MemDatabase, ProposerStore};
use rayls_infrastructure_types::{
    init_signer_claims_activation_epoch, now, signer_claims_region_bytes, test_genesis,
    verify_signer_claims_region, Address, Authority, AuthorityIdentifier, BlsPublicKey,
    BlsSignature, BootstrapServer, CertificateDigest, Committee, Epoch, ExecHeader, Header,
    HeaderBuilder, Multiaddr, Round, SealedHeader, TaskManager, VotingPower, MAX_SIGNER_CLAIMS,
    MAX_TRACKED_TARGETS, SIGNER_CLAIM_LAG_ROUNDS,
};
use rayls_testing_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, Once},
    time::Duration,
};
use tokio::{sync::mpsc, task::yield_now, time::Instant};

const ACTIVATION_EPOCH: Epoch = 5_000;

fn activation() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| init_signer_claims_activation_epoch(ACTIVATION_EPOCH));
}

struct Ctl {
    fixture: CommitteeFixture<MemDatabase>,
    collector: CollectorHandle,
    metrics: Arc<PrimaryMetrics>,
    _tasks: TaskManager,
}

fn ctl() -> Ctl {
    activation();
    let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(ACTIVATION_EPOCH).build();
    let tasks = TaskManager::default();
    let metrics = Metrics::default().node_metrics.clone();
    let collector = CollectorHandle::spawn(&tasks, metrics.clone());
    Ctl { fixture, collector, metrics, _tasks: tasks }
}

fn dropped(metrics: &PrimaryMetrics, label: &str) -> u64 {
    metrics.claim_events_dropped.with_label_values(&[label]).get()
}

// The task drains everything already queued; no clock is involved.
async fn settled() {
    yield_now().await;
    yield_now().await;
}

fn target(fixture: &CommitteeFixture<MemDatabase>, round: Round) -> Header {
    HeaderBuilder::default()
        .author(fixture.first_authority().id())
        .round(round)
        .epoch(ACTIVATION_EPOCH)
        .created_at(now())
        .payload(Default::default())
        .parents(Default::default())
        .build()
}

fn digest_of(header: &Header) -> CertificateDigest {
    CertificateDigest::new(header.digest().into())
}

fn digest_at(i: usize) -> CertificateDigest {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
    CertificateDigest::new(bytes)
}

fn rank_of(fixture: &CommitteeFixture<MemDatabase>, id: &AuthorityIdentifier) -> u32 {
    let authority = fixture.committee().authority(id).expect("authority in the committee");
    fixture
        .committee()
        .bls_keys()
        .iter()
        .position(|key| key == authority.protocol_key())
        .expect("authority is in bls_keys()") as u32
}

fn shares(
    fixture: &CommitteeFixture<MemDatabase>,
    header: &Header,
) -> Vec<(u32, VotingPower, BlsSignature)> {
    fixture
        .authorities()
        .map(|authority| {
            (
                rank_of(fixture, &authority.id()),
                authority.authority().voting_power(),
                authority.vote(header).signature,
            )
        })
        .collect()
}

fn register_target(ctl: &Ctl, header: &Header, signers: usize) {
    let digest = digest_of(header);
    let shares = shares(&ctl.fixture, header);
    let (rank, weight, signature) = shares[0];
    ctl.collector.register(ACTIVATION_EPOCH, digest, header.round(), rank, weight, signature);
    for (rank, weight, signature) in shares.into_iter().skip(1).take(signers - 1) {
        ctl.collector.add_share(ACTIVATION_EPOCH, digest, rank, signature, weight);
    }
}

#[tokio::test(start_paused = true)]
async fn test_freeze_happens_at_the_round_marker_not_at_quorum() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    assert_eq!(
        committee.quorum_threshold(),
        3,
        "the fixture's quorum is three single-stake signers"
    );

    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());
    settled().await;

    // Quorum weight is complete, but the target is inside the lag window: nothing is published.
    ctl.collector.freeze_marker(
        ACTIVATION_EPOCH,
        SIGNER_CLAIM_LAG_ROUNDS,
        committee.quorum_threshold(),
    );
    settled().await;
    assert!(ctl.collector.take(ACTIVATION_EPOCH, SIGNER_CLAIM_LAG_ROUNDS).is_empty());

    // The marker that puts the target `K` rounds back is what publishes it.
    ctl.collector.freeze_marker(
        ACTIVATION_EPOCH,
        1 + SIGNER_CLAIM_LAG_ROUNDS,
        committee.quorum_threshold(),
    );
    settled().await;
    let claims = ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].target_digest, digest_of(&header));
    assert_eq!(claims[0].ranks.len(), committee.size());
}

#[tokio::test(start_paused = true)]
async fn test_late_share_does_not_change_frozen_bytes() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());
    ctl.collector.freeze_marker(
        ACTIVATION_EPOCH,
        1 + SIGNER_CLAIM_LAG_ROUNDS,
        committee.quorum_threshold(),
    );
    settled().await;

    // A share that arrives after the freeze is counted and changes nothing.
    let (_, weight, signature) = shares(&ctl.fixture, &header)[0];
    ctl.collector.add_share(
        ACTIVATION_EPOCH,
        digest_of(&header),
        rank_of(&ctl.fixture, &ctl.fixture.last_authority().id()),
        signature,
        weight,
    );
    settled().await;
    assert_eq!(dropped(&ctl.metrics, "after_freeze"), 1);

    let frozen = ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(frozen.len(), 1);
    assert_eq!(frozen[0].ranks.len(), committee.size(), "the late share added no rank");
    let frozen_bytes = signer_claims_region_bytes(&frozen);

    // A share that arrives after the carry is counted too, and the re-offered bytes stay identical.
    ctl.collector.add_share(
        ACTIVATION_EPOCH,
        digest_of(&header),
        rank_of(&ctl.fixture, &ctl.fixture.last_authority().id()),
        signature,
        weight,
    );
    settled().await;
    assert_eq!(dropped(&ctl.metrics, "after_carry"), 1);

    ctl.collector.freeze_marker(
        ACTIVATION_EPOCH,
        2 + SIGNER_CLAIM_LAG_ROUNDS,
        committee.quorum_threshold(),
    );
    settled().await;
    let again = ctl.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(
        signer_claims_region_bytes(&again),
        frozen_bytes,
        "a late share changes no frozen bytes"
    );
}

#[tokio::test(start_paused = true)]
async fn test_backlog_carried_after_a_failed_proposal() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();

    let first = target(&ctl.fixture, 1);
    let second = target(&ctl.fixture, 2);
    register_target(&ctl, &first, committee.size());
    register_target(&ctl, &second, committee.size());
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;

    // The carrier never certifies, so nothing is committed and the backlog stays alive.
    let handed_out = ctl.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(handed_out.len(), 2);

    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 3 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    let again = ctl.collector.take(ACTIVATION_EPOCH, 3 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(again.len(), 2, "the backlog is handed out again after a failed proposal");
    assert_eq!(signer_claims_region_bytes(&again), signer_claims_region_bytes(&handed_out));
    assert_eq!(dropped(&ctl.metrics, "after_carry"), 0);
}

#[tokio::test(start_paused = true)]
async fn test_carried_claim_reappears_in_the_next_take() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();
    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;

    let first = ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(first.len(), 1);
    assert!(
        ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS).is_empty(),
        "a take with no marker cannot hand the same claim out twice"
    );

    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    let second = ctl.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0], first[0], "the re-offered claim is the same frozen claim");
    assert_eq!(
        dropped(&ctl.metrics, "after_carry"),
        0,
        "re-offering a backlog is not a dropped event"
    );
}

#[tokio::test(start_paused = true)]
async fn test_uncarried_target_keeps_collecting_until_epoch_end() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();

    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, 2);
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    assert!(
        ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS).is_empty(),
        "two signers weigh below quorum, so the target stays collecting"
    );

    // It keeps accepting shares while it is still collecting.
    let (_, weight, signature) = shares(&ctl.fixture, &header)[2];
    ctl.collector.add_share(
        ACTIVATION_EPOCH,
        digest_of(&header),
        rank_of(&ctl.fixture, &ctl.fixture.authority_fixture_by_idx(2).expect("third").id()),
        signature,
        weight,
    );
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    assert_eq!(ctl.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS).len(), 1);

    // The epoch boundary drops what is left and counts it.
    let other = target(&ctl.fixture, 1);
    ctl.collector.register(
        ACTIVATION_EPOCH + 1,
        digest_of(&other),
        1,
        0,
        1,
        BlsSignature::default(),
    );
    settled().await;
    assert!(dropped(&ctl.metrics, "epoch_end") > 0);
    assert!(ctl.collector.take(ACTIVATION_EPOCH + 1, 1 + SIGNER_CLAIM_LAG_ROUNDS).is_empty());
}

#[tokio::test(start_paused = true)]
async fn test_claim_survives_a_skipped_proposal_round() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();
    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());

    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 3, threshold);
    settled().await;
    assert!(ctl.collector.take(ACTIVATION_EPOCH, 3).is_empty());

    // No marker arrives at round 4; round 5 still sees the target as old enough.
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 5, threshold);
    settled().await;
    let claims = ctl.collector.take(ACTIVATION_EPOCH, 5);
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].target_round, 1);
}

#[tokio::test(start_paused = true)]
async fn test_take_never_hands_out_a_claim_from_a_previous_epoch() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();
    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    assert_eq!(ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS).len(), 1);

    let next = target(&ctl.fixture, 1);
    ctl.collector.register(
        ACTIVATION_EPOCH + 1,
        digest_of(&next),
        1,
        1,
        1,
        BlsSignature::default(),
    );
    settled().await;

    let claims = ctl.collector.take(ACTIVATION_EPOCH + 1, 1 + SIGNER_CLAIM_LAG_ROUNDS);
    assert!(claims.iter().all(|claim| claim.target_epoch == ACTIVATION_EPOCH + 1));
    assert!(claims.iter().all(|claim| claim.target_digest != digest_of(&header)));
    assert!(dropped(&ctl.metrics, "epoch_end") > 0);
}

#[tokio::test(start_paused = true)]
async fn test_take_never_hands_out_more_than_a_header_can_carry() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();

    for round in 1..=MAX_SIGNER_CLAIMS + 2 {
        let header = target(&ctl.fixture, round as Round);
        register_target(&ctl, &header, committee.size());
    }
    let carrier_round = (MAX_SIGNER_CLAIMS + 2) as Round + SIGNER_CLAIM_LAG_ROUNDS;
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, carrier_round, threshold);
    settled().await;

    let claims = ctl.collector.take(ACTIVATION_EPOCH, carrier_round);
    assert_eq!(claims.len(), MAX_SIGNER_CLAIMS, "the cap is what a header can carry");

    // The claims the cap cut off stay available until their carrier is committed.
    ctl.collector.claims_committed(
        ACTIVATION_EPOCH,
        claims.iter().map(|claim| claim.target_digest).collect(),
    );
    settled().await;
    ctl.collector.freeze_marker(ACTIVATION_EPOCH, carrier_round + 1, threshold);
    settled().await;
    let rest = ctl.collector.take(ACTIVATION_EPOCH, carrier_round + 1);
    assert_eq!(rest.len(), 2, "the cut-off claims stay ready");
    assert!(rest.iter().all(|claim| claim.target_round > MAX_SIGNER_CLAIMS as Round));
}

#[tokio::test(start_paused = true)]
async fn test_committed_header_deletes_only_the_targets_it_carried() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let threshold = committee.quorum_threshold();

    let carried = target(&ctl.fixture, 1);
    let kept = target(&ctl.fixture, 2);
    register_target(&ctl, &carried, committee.size());
    register_target(&ctl, &kept, committee.size());

    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    let claims = ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(claims.len(), 1, "only the target that is old enough is handed out");
    assert_eq!(claims[0].target_digest, digest_of(&carried));

    ctl.collector.claims_committed(
        ACTIVATION_EPOCH,
        claims.iter().map(|claim| claim.target_digest).collect(),
    );
    settled().await;

    ctl.collector.freeze_marker(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    let next = ctl.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].target_digest, digest_of(&kept));
    assert!(next.iter().all(|claim| claim.target_digest != digest_of(&carried)));
}

#[tokio::test(start_paused = true)]
async fn test_cap_evicts_only_collecting() {
    let table = ctl();
    let committee = table.fixture.committee();
    let threshold = committee.quorum_threshold();

    let ready = target(&table.fixture, 1);
    register_target(&table, &ready, committee.size());
    table.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;

    // Fill the table to the cap, draining the inbox as we go so nothing lands on a full queue.
    for i in 0..MAX_TRACKED_TARGETS {
        table.collector.register(ACTIVATION_EPOCH, digest_at(i), 1, 0, 1, BlsSignature::default());
        if i % 256 == 255 {
            settled().await;
        }
    }
    settled().await;

    // Every further registration evicts an oldest still-collecting target and is counted.
    for i in MAX_TRACKED_TARGETS..MAX_TRACKED_TARGETS + 40 {
        table.collector.register(ACTIVATION_EPOCH, digest_at(i), 1, 0, 1, BlsSignature::default());
        settled().await;
    }
    assert!(dropped(&table.metrics, "cap_eviction") > 0, "the table is capped");
    assert_eq!(dropped(&table.metrics, "queue_full"), 0, "a drained inbox never overflows");

    table.collector.freeze_marker(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS, threshold);
    settled().await;
    let claims = table.collector.take(ACTIVATION_EPOCH, 2 + SIGNER_CLAIM_LAG_ROUNDS);
    assert_eq!(claims.len(), 1, "a ready target is never evicted");
    assert_eq!(claims[0].target_digest, digest_of(&ready));

    // A table at the cap with nothing still collecting evicts nothing, so nothing is counted: a
    // refused registration is not an eviction.
    let frozen = ctl();
    let frozen_threshold = frozen.fixture.committee().quorum_threshold();
    for i in 0..MAX_TRACKED_TARGETS {
        frozen.collector.register(
            ACTIVATION_EPOCH,
            digest_at(i),
            1,
            0,
            frozen_threshold,
            BlsSignature::default(),
        );
        if i % 256 == 255 {
            settled().await;
        }
    }
    settled().await;
    frozen.collector.freeze_marker(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS, frozen_threshold);
    settled().await;
    assert_eq!(dropped(&frozen.metrics, "cap_eviction"), 0, "the cap is reached exactly");

    for i in MAX_TRACKED_TARGETS..MAX_TRACKED_TARGETS + 5 {
        frozen.collector.register(
            ACTIVATION_EPOCH,
            digest_at(i),
            1,
            0,
            frozen_threshold,
            BlsSignature::default(),
        );
        settled().await;
    }
    assert_eq!(
        dropped(&frozen.metrics, "cap_eviction"),
        0,
        "nothing collecting means nothing evicted"
    );
}

#[tokio::test]
async fn test_queue_full_and_unset_handle_never_block() {
    activation();
    let metrics = Metrics::default().node_metrics.clone();
    let stalled = CollectorHandle::stalled(metrics.clone());
    let started = Instant::now();

    for i in 0..MAX_TRACKED_TARGETS + 5 {
        stalled.register(ACTIVATION_EPOCH, digest_at(i), 1, 0, 1, BlsSignature::default());
    }
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "a stalled collector must cost coverage, not time"
    );
    assert_eq!(dropped(&metrics, "queue_full"), 5);
    assert!(stalled.take(ACTIVATION_EPOCH, 10).is_empty());

    let bus = ConsensusBus::new();
    assert!(bus.claim_collector().is_none(), "an unset handle is a plain no-op");

    // Through the real proposal path a stalled collector costs coverage, not rounds: the header is
    // proposed with an encoded, empty region. Our own stake reaches the threshold, so no peer is
    // needed to certify.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(2).unwrap())
        .voting_power_distribution(std::collections::VecDeque::from([3, 1]))
        .epoch(ACTIVATION_EPOCH)
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();
    let mut by_power: Vec<(AuthorityIdentifier, VotingPower)> = committee
        .authorities()
        .iter()
        .map(|authority| (authority.id(), authority.voting_power()))
        .collect();
    by_power.sort_by_key(|(_, power)| *power);
    let ours_authority = by_power[1].0.clone();
    let ours_fixture = fixture
        .authorities()
        .find(|authority| authority.authority().id() == ours_authority)
        .expect("fixture for the 3-stake authority");

    let tasks = TaskManager::default();
    let node = primary(ours_fixture, &committee, &tasks).await;
    node.bus.set_claim_collector(CollectorHandle::stalled(
        node.bus.primary_metrics().node_metrics.clone(),
    ));
    node.start();

    let node_metrics = node.bus.primary_metrics().node_metrics.clone();
    let proposed = settled_within(Duration::from_secs(60), &|| {
        node.config.node_storage().get_last_proposed().expect("read").is_some()
    })
    .await;
    assert!(proposed, "a stalled collector must not stop proposals");

    let header =
        node.config.node_storage().get_last_proposed().expect("read").expect("a proposed header");
    assert!(header.signer_claims.is_empty(), "nothing is carried while the collector is stalled");
    assert_eq!(node_metrics.claims_carried.get(), 0, "an empty take carries no claim");
    assert_eq!(
        signer_claims_region_bytes(&header.signer_claims),
        vec![0u8],
        "the region is still encoded"
    );
}

#[tokio::test(start_paused = true)]
async fn test_task_never_reads_a_clock() {
    let ctl = ctl();
    let committee = ctl.fixture.committee();
    let started = Instant::now();

    let header = target(&ctl.fixture, 1);
    register_target(&ctl, &header, committee.size());
    ctl.collector.freeze_marker(
        ACTIVATION_EPOCH,
        1 + SIGNER_CLAIM_LAG_ROUNDS,
        committee.quorum_threshold(),
    );
    settled().await;

    assert_eq!(Instant::now(), started, "nothing in the collector waited on the clock");
    assert_eq!(ctl.collector.take(ACTIVATION_EPOCH, 1 + SIGNER_CLAIM_LAG_ROUNDS).len(), 1);

    let source = include_str!("../src/claim_collector.rs");
    for forbidden in ["now()", "now_in_millis", "SystemTime", "Instant::", "sleep(", "elapsed()"] {
        assert!(!source.contains(forbidden), "the collector reads no clock ({forbidden})");
    }
}

// A node under test: its bus holds the collector the proposer feeds.
struct Run {
    bus: ConsensusBus,
    config: ConsensusConfig<MemDatabase>,
    handle: PrimaryNetworkHandle,
}

// The proposer waits on replay completion, so the node stays idle until its peers are linked.
// Without this the first certificates are published into a mesh that has no peers yet.
impl Run {
    fn start(&self) {
        self.bus.execution_replay_complete().send_replace(true);
    }
}

async fn primary(
    authority: &AuthorityFixture<MemDatabase>,
    committee: &Committee,
    tasks: &TaskManager,
) -> Run {
    let config = authority.consensus_config();
    let bus = ConsensusBus::new();
    // There is no engine in this harness: the tip is a dummy, and replay is signalled complete
    // only once the peers are linked (see `Run::start`).
    bus.recently_executed_blocks()
        .send_modify(|blocks| blocks.push_latest(SealedHeader::seal_slow(ExecHeader::default())));
    let net = Arc::new(NetworkMetrics::default());
    let (tx_events, rx_events) = mpsc::channel(32);
    let swarm = ConsensusNetwork::new_for_primary(
        config.network_config(),
        tx_events,
        config.key_config().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        config.primary_address(),
        net.clone(),
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
    let mut node = Primary::new(config.clone(), &bus, handle.clone(), synchronizer);
    node.spawn(
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
    Run { bus, config, handle }
}

// A node that answers Vote requests with a signature the committee cannot verify: its votes are
// dropped by the aggregator, so they must never reach the task. It listens on the address of the
// committee slot it fills, so discovery never dials a dead address.
async fn rogue_responder(
    fixture: &CommitteeFixture<MemDatabase>,
    tasks: &TaskManager,
    slot_address: Multiaddr,
) -> Run {
    let key_config = KeyConfig::new_with_testing_key(random_key().copy());
    let rogue_key = key_config.primary_public_key();
    let mut node_config = Config::default_for_test_with_genesis(test_genesis());
    node_config
        .update_primary_network_key(key_config.primary_network_public_key())
        .expect("network key");
    node_config.node_info.p2p_info.primary.network_address = slot_address.clone();
    let address = node_config.node_info.p2p_info.primary.network_address.clone();
    let network_key = node_config.node_info.p2p_info.primary.network_key.clone();

    let mut authorities: BTreeMap<BlsPublicKey, Authority> = BTreeMap::new();
    for authority_fixture in fixture.authorities() {
        authorities.insert(
            *authority_fixture.authority().protocol_key(),
            authority_fixture.authority().clone(),
        );
    }
    authorities.insert(rogue_key, Authority::new_for_test(rogue_key, 1, Address::ZERO));
    // The responder knows only itself: it must not answer DHT queries with the addresses of the
    // committee it does not run, or it pollutes the peers' BLS mappings.
    let bootstrap = BTreeMap::from([(
        rogue_key,
        BootstrapServer::new(
            (address.clone(), network_key.clone()).into(),
            (address.clone(), network_key.clone()).into(),
        ),
    )]);
    let committee = Committee::new_for_test(authorities, ACTIVATION_EPOCH, bootstrap);
    let config = ConsensusConfig::new_with_committee_for_test(
        node_config,
        MemDatabase::default(),
        key_config,
        committee,
        NetworkConfig::default(),
    )
    .expect("rogue config");

    let bus = ConsensusBus::new();
    let net = Arc::new(NetworkMetrics::default());
    let (tx_events, rx_events) = mpsc::channel(32);
    let swarm = ConsensusNetwork::new_for_primary(
        config.network_config(),
        tx_events,
        config.key_config().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        config.primary_address(),
        net.clone(),
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
        synchronizer,
        tasks.get_spawner(),
    )
    .spawn(&tasks.get_spawner());
    handle.inner_handle().start_listening(config.primary_address()).await.expect("listen");
    Run { bus, config, handle }
}

async fn link(from: &Run, to: &Run, bls_key: BlsPublicKey) {
    // A trusted peer binds the peer id to the authority's BLS key on insert; gossip delivery
    // needs that binding, so an explicit peer plus a later dial is not enough here. The dial may
    // report a timeout while the connection lands afterwards, so the result is not asserted.
    let _ = from
        .handle
        .inner_handle()
        .add_trusted_peer_and_dial(
            bls_key,
            to.config.primary_networkkey(),
            to.config.primary_address(),
        )
        .await;
}

async fn settled_within(within: Duration, pred: &dyn Fn() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn test_collector_sees_no_share_the_aggregator_rejected() {
    activation();
    // Two slots, stake 3 and 1: the threshold is 3, so the 3-stake member certifies with its own
    // vote plus whatever the aggregator accepts. The 1-stake slot is filled by a responder whose
    // votes never verify, so it contributes nothing - accepted or otherwise.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(2).unwrap())
        .voting_power_distribution(std::collections::VecDeque::from([3, 1]))
        .epoch(ACTIVATION_EPOCH)
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();
    let mut by_power: Vec<(AuthorityIdentifier, VotingPower)> = committee
        .authorities()
        .iter()
        .map(|authority| (authority.id(), authority.voting_power()))
        .collect();
    by_power.sort_by_key(|(_, power)| *power);
    let rogue_authority = by_power[0].0.clone();
    let ours_authority = by_power[1].0.clone();
    let fixture_for = |id: &AuthorityIdentifier| -> &AuthorityFixture<MemDatabase> {
        fixture
            .authorities()
            .find(|authority| authority.authority().id() == *id)
            .expect("fixture for the authority")
    };
    let rogue_rank = rank_of(&fixture, &rogue_authority);
    let ours_rank = rank_of(&fixture, &ours_authority);

    let tasks = TaskManager::default();
    let ours = primary(fixture_for(&ours_authority), &committee, &tasks).await;
    // The responder fills the slot's address so the certifier's dial lands on a live listener.
    let responder = rogue_responder(
        &fixture,
        &tasks,
        fixture_for(&rogue_authority).consensus_config().primary_address(),
    )
    .await;

    let ours_key = ours.config.key_config().primary_public_key();
    let rogue_key = *fixture_for(&rogue_authority).authority().protocol_key();
    link(&ours, &responder, rogue_key).await;
    link(&responder, &ours, ours_key).await;

    ours.start();

    let metrics = ours.bus.primary_metrics().node_metrics.clone();

    // The task's count and the aggregator's accepted count must agree: one vote is accepted each
    // round - our own - so every claim the task hands out names exactly one rank.
    let settled = settled_within(Duration::from_secs(150), &|| {
        metrics.claims_carried.get() > 0 && metrics.votes_received_last_round.get() == 1
    })
    .await;
    assert!(settled, "our own vote must certify our headers and carry claims");
    assert_eq!(metrics.votes_received_last_round.get(), 1, "only the accepted vote is counted");

    // A vote the aggregator rejected never becomes a share, so the rank-miss branch never runs.
    assert_eq!(dropped(&metrics, "rank_miss"), 0, "a rejected share must not reach the task");
    assert_eq!(dropped(&metrics, "queue_full"), 0);
    assert_eq!(dropped(&metrics, "handoff_dropped"), 0);
    assert_eq!(dropped(&metrics, "after_freeze"), 0);
    assert_eq!(dropped(&metrics, "epoch_end"), 0);
    assert_eq!(dropped(&metrics, "cap_eviction"), 0);
    assert_eq!(metrics.claim_headers_rejected.get(), 0, "the gate rejects nothing here");

    // The claims we carried name only the authorities whose votes the aggregator accepted. The
    // header is read from our own proposer store: the wire test covers acceptance on a peer.
    let header =
        ours.config.node_storage().get_last_proposed().expect("read").expect("a proposed header");
    assert!(!header.signer_claims.is_empty(), "the carrier must hold claims");
    for claim in &header.signer_claims {
        assert_eq!(claim.target_epoch, header.epoch(), "a claim never crosses the epoch");
        assert!(claim.target_round < header.round(), "a claim lags the header that carries it");
        assert_eq!(claim.ranks.len(), 1, "the claim holds only the accepted voter");
        assert_eq!(claim.ranks, vec![ours_rank], "our own share is the only rank in the claim");
        assert!(
            claim.ranks.iter().all(|rank| *rank != rogue_rank),
            "the rejected signer's rank appears in a claim: {:?}",
            claim.ranks
        );
    }
    verify_signer_claims_region(&header.signer_claims, header.epoch(), header.round(), &committee)
        .expect("the carried claim verifies");

    // The slot the responder fills is reachable - the two nodes are connected. Whether its vote
    // arrives is timing-dependent: our own stake already reaches the threshold, so the certifier
    // cancels the outstanding request as soon as quorum is reached. What must hold either way is
    // asserted above: a vote the certifier refused never becomes a share.
    let connected = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if responder.handle.inner_handle().connected_peer_count().await.unwrap_or(0) >= 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(connected, "the responder must be reachable");
}
