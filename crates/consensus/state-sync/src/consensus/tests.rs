use super::*;
use rayls_consensus_primary::network::{
    NetworkCommand, NetworkError, PrimaryRequest, PrimaryResponse,
};
use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::ConsensusBlocks};
use rayls_infrastructure_types::{
    encode, to_intent_message, AuthorityIdentifier, BlsAggregateSignature, BlsPublicKey,
    BlsSignature, BlsSigner, Certificate, CertifiedTip, CommittedSubDag, CommitteeLookahead,
    EpochCertificate, EpochRecord, HeaderBuilder, ReputationScores, Signer as _,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

/// A network handle whose receiver is dropped: every fetch fails fast, so a walk can make
/// progress only through the local DB/cache, never a peer.
fn no_peer_network() -> PrimaryNetworkHandle {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    PrimaryNetworkHandle::new_for_test(tx)
}

/// A stale `ConsensusBlockNumbersByDigest` entry (foreign digest mapped to a canonical number)
/// must not false-stop the backwards walk or republish the wrong header.
#[tokio::test(start_paused = true)]
async fn stale_index_entry_does_not_false_stop_the_walk() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    // Canonical block at 100, plus a STALE index entry pointing a foreign digest at 100.
    let canonical = ConsensusHeader { number: 100, ..Default::default() };
    let canonical_digest = canonical.digest();
    let stale_digest = B256::repeat_byte(0xEE);
    assert_ne!(stale_digest, canonical_digest, "precondition: the stale hash is not canonical");
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&canonical_digest, &canonical.number)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&stale_digest, &canonical.number)?;
        Ok(())
    })
    .unwrap();

    let consensus_bus = ConsensusBus::new();
    let before = consensus_bus.last_consensus_header().borrow().number;

    // Network with a dropped receiver: every fetch fails fast, so the ONLY path that can change
    // `last_consensus_header` is the buggy canonical-hit branch.
    let network = no_peer_network();
    let shutdown = config.shutdown().subscribe();

    let parent =
        get_consensus_header(stale_digest, &config, &consensus_bus, &network, &shutdown).await;

    let after = consensus_bus.last_consensus_header().borrow().number;
    assert_eq!(
        after, before,
        "stale index entry false-stopped the walk and republished the wrong block"
    );
    assert!(parent.is_none(), "no reachable peer, so the fetch fails and the walk yields None");
}

/// A header already cached by an earlier leg (fetched but not yet executed/canonical) must be
/// traversed from the DB, not refetched from a peer: the walk reads it, republishes the tip,
/// and returns its parent to keep descending toward the canonical anchor. While execution
/// lags catch-up every not-yet-executed header lives only in the cache, so refetching each
/// one on every gossip update re-walks the whole gap over the network.
#[tokio::test(start_paused = true)]
async fn cached_header_is_traversed_not_refetched() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    // Cache-only header at 100 (no canonical `ConsensusBlocks` row): the steady state while
    // execution lags the backwards walk.
    let parent_hash = B256::repeat_byte(0x77);
    let cached = ConsensusHeader { number: 100, parent_hash, ..Default::default() };
    let cached_digest = cached.digest();
    store_consensus_header_in_cache(db, &cached);
    assert!(
        db.get_canonical_consensus_by_hash(cached_digest).is_none(),
        "precondition: a cache-only header is invisible to the canonical anchor check"
    );

    let consensus_bus = ConsensusBus::new();

    // Dropped-receiver network: every fetch fails fast. So returning the parent (and advancing
    // `last_consensus_header` to 100) is only reachable by traversing the cache; a refetch path
    // would fail and yield `None` with the watermark left at 0.
    let network = no_peer_network();
    let shutdown = config.shutdown().subscribe();

    let next =
        get_consensus_header(cached_digest, &config, &consensus_bus, &network, &shutdown).await;

    assert_eq!(
        next,
        Some((parent_hash, 100)),
        "a cached header is traversed from the DB, returning its parent to keep descending"
    );
    assert_eq!(
        consensus_bus.last_consensus_header().borrow().number,
        100,
        "traversing the cached header republishes it without a peer refetch"
    );
}

/// A walk stops at the session's covered anchor instead of descending below it. The anchor
/// heads a chain a prior walk already grounded, so re-descending it is wasted work; with no
/// reachable peer, only stopping at the anchor lets the walk reach `Covered` rather than
/// `Incomplete`.
#[tokio::test(start_paused = true)]
async fn walk_stops_at_the_covered_anchor() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    // The covered anchor (number 105) is NOT itself in the DB; a cached child at 106 links onto
    // it, so the walk reaches the anchor by traversing the child and must stop there.
    let covered_hash = B256::repeat_byte(0x55);
    let child = ConsensusHeader { number: 106, parent_hash: covered_hash, ..Default::default() };
    store_consensus_header_in_cache(db, &child);

    // Dropped-receiver network: any fetch below the anchor fails, so a walk that does not stop
    // at the anchor falls through to a failing fetch and yields `Incomplete`.
    let network = no_peer_network();

    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);
    session.covered = Some(covered_hash);

    assert_eq!(
        session.walk(child.digest(), None).await,
        WalkOutcome::Covered,
        "the walk must stop at the covered anchor, not fetch below it"
    );
}

/// `derive_covered_anchor` is a pure projection of the persisted cache: on an unchanged cache
/// it returns the same anchor every call. The sweep is offloaded to a blocking thread, so its
/// result must stay deterministic and independent of where it runs.
#[tokio::test]
async fn derive_covered_anchor_is_stable_across_calls() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    // Canonical tip at 100, with a contiguous cached run 101 -> 102 hash-linked above it.
    let canonical = ConsensusHeader { number: 100, ..Default::default() };
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
        Ok(())
    })
    .unwrap();
    let child_101 =
        ConsensusHeader { number: 101, parent_hash: canonical.digest(), ..Default::default() };
    let child_102 =
        ConsensusHeader { number: 102, parent_hash: child_101.digest(), ..Default::default() };
    store_consensus_header_in_cache(db, &child_101);
    store_consensus_header_in_cache(db, &child_102);

    let first = derive_covered_anchor(db);
    let second = derive_covered_anchor(db);
    assert_eq!(first, Some(child_102.digest()), "anchor must be the top of the contiguous run");
    assert_eq!(first, second, "derive must be stable across calls on an unchanged cache");
}

/// The switch is made against the next block to execute, like every other fork check.
#[test]
fn certified_tip_is_used_once_the_fork_is_active_at_the_next_block() {
    assert!(!uses_certified_tip(None, 100), "Never: probe latest headers");
    assert!(!uses_certified_tip(Some(102), 100), "next block 101 is before activation");
    assert!(uses_certified_tip(Some(101), 100), "next block 101 is the activation block");
    assert!(uses_certified_tip(Some(0), 0), "genesis-active");
    assert!(uses_certified_tip(Some(u64::MAX), u64::MAX), "no overflow at the top");
}

fn committee_of_four() -> CommitteeFixture<MemDatabase> {
    CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build()
}

/// A genuine tip one block above genesis, its leader certified by the whole fixture committee.
fn honest_tip(fixture: &CommitteeFixture<MemDatabase>) -> ConsensusHeader {
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: *GENESIS_DIGEST, sub_dag, number: 1, extra: B256::ZERO }
}

/// What one adversarial validator serves as its latest header: the highest number there is,
/// and an unsigned leader claiming an epoch no committee exists for (#370).
fn forged_tip() -> ConsensusHeader {
    let mut leader = Certificate::default();
    leader.header.epoch = Epoch::MAX;
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    ConsensusHeader {
        parent_hash: B256::repeat_byte(0xAB),
        sub_dag,
        number: u64::MAX,
        extra: B256::ZERO,
    }
}

/// A forged copy of `honest`: the same block number and the same committee-signed leader and
/// certificates, with only the commit timestamp raised. The commit timestamp is hashed into the
/// header digest but is not covered by any committee signature, so the copy still verifies yet
/// has a different digest and would execute to a different block (#312).
fn forged_copy(honest: &ConsensusHeader) -> ConsensusHeader {
    let mut higher_leader = Certificate::default();
    higher_leader.header.created_at = honest.sub_dag.commit_timestamp() + 1_000_000;
    let higher = CommittedSubDag::new(vec![], higher_leader, 0, ReputationScores::default(), None);
    let tampered_sub_dag = CommittedSubDag::new(
        honest.sub_dag.certificates.clone(),
        honest.sub_dag.leader.clone(),
        0,
        honest.sub_dag.reputation_score.clone(),
        Some(&higher),
    );
    ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: tampered_sub_dag,
        number: honest.number,
        extra: honest.extra,
    }
}

/// `header` as `signers` of the fixture committee sign it when they commit it.
fn signatures(
    fixture: &CommitteeFixture<MemDatabase>,
    signers: usize,
    header: &ConsensusHeader,
) -> Vec<(BlsPublicKey, BlsSignature)> {
    let (epoch, round) = (header.sub_dag.leader_epoch(), header.sub_dag.leader_round());
    let digest = CertifiedTip::digest_data(epoch, round, header.number, header.digest());
    fixture
        .authorities()
        .take(signers)
        .map(|a| {
            (*a.authority().protocol_key(), a.keypair().sign(&encode(&to_intent_message(digest))))
        })
        .collect()
}

/// The certified tip f+1 of the fixture committee hold for `header`.
fn certified(fixture: &CommitteeFixture<MemDatabase>, header: &ConsensusHeader) -> CertifiedTip {
    let keys: Vec<_> = fixture.authorities().map(|a| *a.authority().protocol_key()).collect();
    CertifiedTip::from_signatures(
        header.sub_dag.leader_epoch(),
        header.sub_dag.leader_round(),
        header.number,
        header.digest(),
        &keys,
        signatures(fixture, CertifiedTip::quorum(keys.len()), header),
    )
    .expect("f+1 signers")
}

/// One connected peer: what it answers when asked for its certified tip (`Err` for a peer on
/// a release without the request) and for its latest header, and whether its certified tip
/// arrives a second late.
struct Peer {
    key: BlsPublicKey,
    certified_tip: Result<Option<CertifiedTip>, ()>,
    latest: ConsensusHeader,
    slow: bool,
}

impl Peer {
    fn new(key: BlsPublicKey, tip: Option<CertifiedTip>, latest: &ConsensusHeader) -> Self {
        Self { key, certified_tip: Ok(tip), latest: latest.clone(), slow: false }
    }

    /// A peer on a release without the certified tip request.
    fn old(key: BlsPublicKey, latest: &ConsensusHeader) -> Self {
        Self { key, certified_tip: Err(()), latest: latest.clone(), slow: false }
    }
}

/// Requests the follower sent, by kind.
#[derive(Default)]
struct Asked {
    latest: AtomicUsize,
    certified_tip: AtomicUsize,
}

/// Connected `peers`, serving every header in `headers` by hash.
fn peer_network(
    peers: Vec<Peer>,
    headers: Vec<ConsensusHeader>,
) -> (PrimaryNetworkHandle, Arc<Asked>) {
    let asked = Arc::new(Asked::default());
    let counts = asked.clone();
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
    tokio::spawn(async move {
        let unknown = || NetworkError::RPCError("unknown".to_string());
        while let Some(cmd) = rx.recv().await {
            match cmd {
                NetworkCommand::ConnectedPeers { reply } => {
                    let _ = reply.send(peers.iter().map(|p| p.key).collect());
                }
                NetworkCommand::SendRequest { peer, request, reply } => {
                    let Some(peer) = peers.iter().find(|p| p.key == peer) else { continue };
                    let response = match request {
                        PrimaryRequest::CertifiedTip => {
                            counts.certified_tip.fetch_add(1, Ordering::SeqCst);
                            let response = peer
                                .certified_tip
                                .clone()
                                .map(PrimaryResponse::CertifiedTip)
                                .map_err(|()| unknown());
                            if peer.slow {
                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    let _ = reply.send(response);
                                });
                                continue;
                            }
                            response
                        }
                        PrimaryRequest::ConsensusHeader { number: None, hash: None } => {
                            counts.latest.fetch_add(1, Ordering::SeqCst);
                            Ok(PrimaryResponse::ConsensusHeader(Arc::new(peer.latest.clone())))
                        }
                        _ => continue,
                    };
                    let _ = reply.send(response);
                }
                NetworkCommand::SendRequestAny {
                    request: PrimaryRequest::ConsensusHeader { hash: Some(hash), .. },
                    reply,
                } => {
                    let found = headers.iter().find(|h| h.digest() == hash).cloned();
                    let _ = reply.send(
                        found
                            .map(|h| PrimaryResponse::ConsensusHeader(Arc::new(h)))
                            .ok_or_else(unknown),
                    );
                }
                _ => {}
            }
        }
    });
    (PrimaryNetworkHandle::new_for_test(tx), asked)
}

/// Wait until the follower's latest header reaches `number`, or `limit` passes.
async fn reaches(consensus_bus: &ConsensusBus, number: u64, limit: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if consensus_bus.last_consensus_header().borrow().number >= number {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// After the fork one valid certified tip from any connected peer is enough, here an
/// observer outside the committee, while the other peers are on an older release or hold
/// none. No peer is asked for its latest header.
#[tokio::test(start_paused = true)]
async fn after_the_fork_one_peer_certified_tip_is_enough() {
    let fixture = committee_of_four();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let config = config.with_certified_tip_v1_block(Some(0));
    let honest = honest_tip(&fixture);
    let members: Vec<_> = config.committee().others_primaries_by_id(config.authority_id().as_ref());
    // a key outside the committee: another fixture's keys are freshly generated
    let observer = *committee_of_four().authorities().next().unwrap().authority().protocol_key();
    let peers = vec![
        Peer::old(members[0].1, &honest),
        Peer::old(members[1].1, &honest),
        Peer::new(members[2].1, None, &honest),
        Peer::new(observer, Some(certified(&fixture, &honest)), &honest),
    ];
    let (network, asked) = peer_network(peers, vec![honest.clone()]);
    let consensus_bus = ConsensusBus::new();
    tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

    assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
    let published = consensus_bus.last_consensus_header().borrow().digest();
    config.shutdown().notify();
    assert_eq!(published, honest.digest());
    assert_eq!(asked.latest.load(Ordering::SeqCst), 0, "no peer is asked for its latest");
    assert_eq!(asked.certified_tip.load(Ordering::SeqCst), 4, "every peer is asked");
}

/// #312 after the fork: one member serves a forged copy of the real tip, as its latest header
/// and as a certified tip it signed alone, its bitmap claiming f+1 signers. The follower walks
/// to the honest tip and never stores or publishes the copy.
#[tokio::test(start_paused = true)]
async fn after_the_fork_a_forged_copy_cannot_fork_the_follower() {
    let fixture = committee_of_four();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let config = config.with_certified_tip_v1_block(Some(0));
    let honest = honest_tip(&fixture);
    let forged = forged_copy(&honest);
    let keys = config.get_committee_keys_for_epoch(0).expect("current committee");
    assert!(forged.clone().verify_header_with_keys(&keys).is_ok(), "the copy still verifies");
    assert_ne!(forged.digest(), honest.digest(), "the bumped timestamp changes the digest");

    // the adversary's own signature, under a bitmap naming f+1 signers
    let mut alone = certified(&fixture, &forged);
    let (_, own) = signatures(&fixture, 1, &forged)[0];
    alone.signature = BlsAggregateSignature::aggregate(&[own], true).unwrap().to_signature();
    let alone = CertifiedTip { number: forged.number, ..alone };
    assert!(!alone.verify(&keys), "precondition: one signer is not f+1");

    let members: Vec<_> = config.committee().others_primaries_by_id(config.authority_id().as_ref());
    let mut peers: Vec<_> = members
        .iter()
        .map(|(_, key)| Peer::new(*key, Some(certified(&fixture, &honest)), &honest))
        .collect();
    peers[0] = Peer::new(members[0].1, Some(alone), &forged);
    let (network, asked) = peer_network(peers, vec![honest.clone(), forged.clone()]);
    let consensus_bus = ConsensusBus::new();
    tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

    assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
    let published = consensus_bus.last_consensus_header().borrow().clone();
    let stored_forged = config.node_storage().get_consensus_by_hash(forged.digest()).is_some();
    config.shutdown().notify();
    assert_eq!(published.digest(), honest.digest(), "the honest tip is the published header");
    assert!(!stored_forged, "the forged copy was stored in the consensus cache");
    assert_eq!(asked.latest.load(Ordering::SeqCst), 0, "no peer is asked for its latest");
}

/// #370 after the fork: one member answers with a tip at the highest number there is, from an
/// epoch no committee exists for. It cannot strand the follower, which walks to the honest
/// tip, nor send the epoch record collector chasing that epoch.
#[tokio::test(start_paused = true)]
async fn after_the_fork_a_forged_epoch_cannot_strand_the_follower() {
    let fixture = committee_of_four();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let config = config.with_certified_tip_v1_block(Some(0));
    let honest = honest_tip(&fixture);
    assert!(config.get_committee_keys_for_epoch(Epoch::MAX).is_none(), "precondition");
    let far = CertifiedTip { epoch: Epoch::MAX, number: u64::MAX, ..certified(&fixture, &honest) };

    let members: Vec<_> = config.committee().others_primaries_by_id(config.authority_id().as_ref());
    let mut peers: Vec<_> = members
        .iter()
        .map(|(_, key)| Peer::new(*key, Some(certified(&fixture, &honest)), &honest))
        .collect();
    peers[0] = Peer::new(members[0].1, Some(far), &forged_tip());
    let (network, _) = peer_network(peers, vec![honest.clone()]);
    let consensus_bus = ConsensusBus::new();
    tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

    assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
    let requested = *consensus_bus.requested_missing_epoch().borrow();
    config.shutdown().notify();
    assert!(requested <= 1, "the collector was sent to epoch {requested}");
}

/// A copy of the honest top tip with a bad signature, answered before the honest ones, does
/// not stand in for them.
#[tokio::test(start_paused = true)]
async fn a_bad_copy_of_the_top_tip_does_not_hide_the_honest_ones() {
    let fixture = committee_of_four();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let config = config.with_certified_tip_v1_block(Some(0));
    let honest = honest_tip(&fixture);
    let valid = certified(&fixture, &honest);
    let bad = CertifiedTip { signature: signatures(&fixture, 1, &honest)[0].1, ..valid.clone() };
    let keys = config.get_committee_keys_for_epoch(0).expect("current committee");
    assert!(!bad.verify(&keys), "precondition: the copy does not verify");

    // the honest members answer late, the bad copy from an observer first
    let observer = *committee_of_four().authorities().next().unwrap().authority().protocol_key();
    let mut peers: Vec<_> = config
        .committee()
        .others_primaries_by_id(config.authority_id().as_ref())
        .into_iter()
        .map(|(_, key)| Peer { slow: true, ..Peer::new(key, Some(valid.clone()), &honest) })
        .collect();
    peers.push(Peer::new(observer, Some(bad), &honest));
    let (network, _) = peer_network(peers, vec![honest.clone()]);
    let consensus_bus = ConsensusBus::new();
    tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

    assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
    config.shutdown().notify();
}

/// Before the fork the follower probes committee members' latest headers as before, and asks
/// nobody for a certified tip.
#[tokio::test(start_paused = true)]
async fn before_the_fork_the_follower_probes_latest_headers() {
    let fixture = committee_of_four();
    let config = fixture.authorities().next().unwrap().consensus_config();
    assert_eq!(config.certified_tip_v1_block(), None, "precondition: the fork is Never");
    let honest = honest_tip(&fixture);
    let peers = config
        .committee()
        .others_primaries_by_id(config.authority_id().as_ref())
        .into_iter()
        .map(|(_, key)| Peer::new(key, Some(certified(&fixture, &honest)), &honest))
        .collect();
    let (network, asked) = peer_network(peers, vec![honest.clone()]);
    let consensus_bus = ConsensusBus::new();
    tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

    assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
    config.shutdown().notify();
    assert_eq!(asked.latest.load(Ordering::SeqCst), 3, "every member is probed");
    assert_eq!(asked.certified_tip.load(Ordering::SeqCst), 0, "no certified tip is asked for");
}

/// Requests a [`walk_network`] received: fetches by hash and by number, the lowest number
/// asked for, and each hash asked for, in order. `numbers` follows the fetches by number, for
/// a test that acts once the walk has asked for some.
struct WalkRequests {
    by_hash: Arc<AtomicUsize>,
    by_number: Arc<AtomicUsize>,
    lowest_number: Arc<AtomicU64>,
    hashes: tokio::sync::mpsc::UnboundedReceiver<B256>,
    numbers: watch::Receiver<usize>,
}

impl WalkRequests {
    /// The hashes asked for so far, in order.
    fn hashes(&mut self) -> Vec<B256> {
        std::iter::from_fn(|| self.hashes.try_recv().ok()).collect()
    }
}

/// A network that answers `ConsensusHeader` requests by number from `by_number` and by hash
/// from `by_hash`, like a peer, and records what it was asked. The first request for a digest
/// in `first_by_hash` gets that header instead, like a bad peer answering first.
fn walk_network(
    by_number: BTreeMap<u64, ConsensusHeader>,
    by_hash: &BTreeMap<u64, ConsensusHeader>,
    mut first_by_hash: HashMap<B256, ConsensusHeader>,
) -> (PrimaryNetworkHandle, WalkRequests) {
    let by_digest: HashMap<B256, ConsensusHeader> =
        by_hash.values().map(|h| (h.digest(), h.clone())).collect();
    let (hash_tx, hashes) = tokio::sync::mpsc::unbounded_channel();
    let (number_tx, numbers) = watch::channel(0usize);
    let requests = WalkRequests {
        by_hash: Arc::new(AtomicUsize::new(0)),
        by_number: Arc::new(AtomicUsize::new(0)),
        lowest_number: Arc::new(AtomicU64::new(u64::MAX)),
        hashes,
        numbers,
    };
    let (hash_count, number_count, lowest) =
        (requests.by_hash.clone(), requests.by_number.clone(), requests.lowest_number.clone());
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
            let PrimaryRequest::ConsensusHeader { number, hash } = request else { continue };
            let header = match (hash, number) {
                (Some(hash), _) => {
                    hash_count.fetch_add(1, Ordering::Relaxed);
                    let _ = hash_tx.send(hash);
                    first_by_hash.remove(&hash).or_else(|| by_digest.get(&hash).cloned())
                }
                (None, Some(number)) => {
                    number_count.fetch_add(1, Ordering::Relaxed);
                    number_tx.send_modify(|count| *count += 1);
                    lowest.fetch_min(number, Ordering::Relaxed);
                    by_number.get(&number).cloned()
                }
                (None, None) => None,
            };
            let response = match header {
                Some(h) => Ok(PrimaryResponse::ConsensusHeader(Arc::new(h))),
                None => Err(NetworkError::RPCError("no such header".to_string())),
            };
            let _ = reply.send(response);
        }
    });
    (PrimaryNetworkHandle::new_for_test(tx), requests)
}

/// A canonical tip at 100 and a real chain 101..=`top` above it, every leader quorum-signed so
/// BLS passes.
fn canonical_tip_and_chain<DB: ReDatabase>(
    db: &DB,
    leader: &Certificate,
    top: u64,
) -> BTreeMap<u64, ConsensusHeader> {
    canonical_chain_above(db, ConsensusHeader { number: 100, ..Default::default() }, leader, top)
}

/// [`canonical_tip_and_chain`] with the given header as the canonical tip at 100.
fn canonical_chain_above<DB: ReDatabase>(
    db: &DB,
    tip: ConsensusHeader,
    leader: &Certificate,
    top: u64,
) -> BTreeMap<u64, ConsensusHeader> {
    assert_eq!(tip.number, 100, "the chain starts at 101");
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&tip.number, &tip)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&tip.digest(), &tip.number)?;
        Ok(())
    })
    .unwrap();
    let mut chain = BTreeMap::new();
    let mut parent_hash = tip.digest();
    for number in 101..=top {
        let sub_dag =
            CommittedSubDag::new(vec![], leader.clone(), 0, ReputationScores::default(), None);
        let header = ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() };
        parent_hash = header.digest();
        chain.insert(number, header);
    }
    chain
}

fn assert_chain_cached<DB: ReDatabase>(db: &DB, chain: &BTreeMap<u64, ConsensusHeader>) {
    for (number, header) in chain {
        let cached = db.get::<ConsensusBlocksCache>(number).unwrap().map(|h| h.digest());
        assert_eq!(cached, Some(header.digest()), "header {number} must be cached");
    }
}

/// The walk fetches only the top header by hash and the rest of the gap by number in rounds
/// below it, never at or below the canonical tip.
#[tokio::test]
async fn walk_prefetches_the_gap_by_number() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);

    let (network, requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert_eq!(session.walk(chain[&140].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(requests.by_hash.load(Ordering::Relaxed), 1, "only the top is fetched by hash");
    assert!(
        requests.lowest_number.load(Ordering::Relaxed) > 100,
        "nothing at or below the canonical tip is requested"
    );
}

/// A reply with the real parent link but changed contents ends prefetching. Only the checked
/// run above it is stored, the walk fetches that header by hash, and prefetching resumes below
/// with the replies it already holds.
#[tokio::test]
async fn walk_prefetch_stops_at_a_mismatched_reply() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);

    let mut served = chain.clone();
    let changed =
        ConsensusHeader { number: 120, parent_hash: chain[&119].digest(), ..Default::default() };
    assert_ne!(changed.digest(), chain[&120].digest(), "precondition: contents differ");
    served.insert(120, changed);
    let (network, requests) = walk_network(served, &chain, HashMap::new());
    let session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert_eq!(session.walk(chain[&140].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(
        requests.by_hash.load(Ordering::Relaxed),
        2,
        "the mismatched header is fetched by hash, then prefetching resumes"
    );
    assert_eq!(
        requests.by_number.load(Ordering::Relaxed),
        39,
        "each of 101..=139 is asked for by number once: the replies below 120 are kept"
    );
}

/// A reply with the right digest but signatures that fail BLS ends prefetching too: the digest
/// does not cover the signatures, so only BLS tells it apart.
#[tokio::test]
async fn walk_prefetch_stops_at_a_bls_invalid_reply() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader_header = fixture.header_from_last_authority();
    let leader = fixture.certificate(&leader_header);
    let chain = canonical_tip_and_chain(db, &leader, 140);

    let one_vote: Vec<_> = fixture
        .votes(&leader_header)
        .into_iter()
        .take(1)
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();
    let under_signed =
        Certificate::new_unsigned_for_test(&fixture.committee(), leader_header, one_vote).unwrap();
    let mut invalid = chain[&120].clone();
    invalid.sub_dag =
        CommittedSubDag::new(vec![], under_signed, 0, ReputationScores::default(), None);
    assert_eq!(invalid.digest(), chain[&120].digest(), "precondition: same digest");

    let mut served = chain.clone();
    served.insert(120, invalid);
    let (network, requests) = walk_network(served, &chain, HashMap::new());
    let session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert_eq!(session.walk(chain[&140].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(requests.by_hash.load(Ordering::Relaxed), 2, "120 is re-fetched by hash");
    let keys = config.get_committee_keys_for_epoch(0).unwrap();
    let stored = db.get::<ConsensusBlocksCache>(&120).unwrap().unwrap();
    assert!(stored.verify_header_with_keys(&keys).is_ok(), "the stored 120 must pass BLS");
    assert_eq!(
        requests.by_number.load(Ordering::Relaxed),
        39,
        "each of 101..=139 is asked for by number once: the replies below 120 are kept"
    );
}

/// A fetch by hash whose reply fails BLS asks another peer instead of ending the walk.
#[tokio::test]
async fn walk_refetches_a_bls_invalid_reply_by_hash() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader_header = fixture.header_from_last_authority();
    let leader = fixture.certificate(&leader_header);
    let chain = canonical_tip_and_chain(db, &leader, 110);

    let one_vote: Vec<_> = fixture
        .votes(&leader_header)
        .into_iter()
        .take(1)
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();
    let under_signed =
        Certificate::new_unsigned_for_test(&fixture.committee(), leader_header, one_vote).unwrap();
    let mut invalid = chain[&110].clone();
    invalid.sub_dag =
        CommittedSubDag::new(vec![], under_signed, 0, ReputationScores::default(), None);
    assert_eq!(invalid.digest(), chain[&110].digest(), "precondition: same digest");

    let first = HashMap::from([(invalid.digest(), invalid)]);
    let (network, requests) = walk_network(chain.clone(), &chain, first);
    let session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert_eq!(session.walk(chain[&110].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(requests.by_hash.load(Ordering::Relaxed), 2, "110 is asked for again by hash");
}

/// A stored row with the real parent link but other contents, under a good stored row, is
/// replaced by the real header on the way down. The forward streamer relies on this and does
/// not clear such a row itself.
#[tokio::test]
async fn walk_replaces_a_stale_stored_row() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 105);

    let stale_103 = ConsensusHeader {
        parent_hash: chain[&102].digest(),
        sub_dag: CommittedSubDag::new(
            vec![],
            Certificate::default(),
            0,
            ReputationScores::default(),
            None,
        ),
        number: 103,
        extra: B256::default(),
    };
    assert_ne!(stale_103.digest(), chain[&103].digest(), "precondition: another fork's row");
    for header in [&chain[&101], &chain[&102], &stale_103, &chain[&104]] {
        store_consensus_header_in_cache(db, header);
    }

    let (network, _requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert_eq!(session.walk(chain[&105].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
}

/// A peer serves, as the walk target, a commit whose leader is valid but which also carries a
/// certificate a quorum of the committee signed for an author outside it, as a validator that
/// joined in the next epoch can build by leaving out its own vote. The walk must refuse it from
/// every peer and store nothing, since the subscriber's block-reward lookup fails fatally on
/// that author. The real commit at that number is still taken.
#[tokio::test(start_paused = true)]
async fn walk_does_not_store_a_commit_with_a_certificate_from_outside_the_committee() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 101);

    // The joined validator's epoch-1 header, signed by all four members of this committee.
    let joined = AuthorityIdentifier::dummy_for_test(0xAA);
    assert!(fixture.committee().authority(&joined).is_none(), "precondition: an outsider");
    let joined_header = HeaderBuilder::default()
        .author(joined)
        .round(3)
        .epoch(1)
        .parents(fixture.genesis().collect())
        .build();
    let votes: Vec<_> = fixture
        .votes(&joined_header)
        .into_iter()
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();
    assert_eq!(votes.len(), 4, "precondition: the whole committee signs");
    let selfless = Certificate::new_unverified(&fixture.committee(), joined_header, votes).unwrap();

    // Not a parent of the leader, so it is checked on its own.
    let mut forged = chain[&101].clone();
    forged.sub_dag =
        CommittedSubDag::new(vec![selfless], leader.clone(), 0, ReputationScores::default(), None);
    assert_ne!(forged.digest(), chain[&101].digest(), "precondition: another commit at 101");

    // Every peer serves the forged commit for its hash.
    let consensus_bus = ConsensusBus::new();
    let tip_before = consensus_bus.last_consensus_header().borrow().number;
    let (network, mut requests) =
        walk_network(BTreeMap::new(), &BTreeMap::from([(101, forged.clone())]), HashMap::new());
    let session = WalkSession::new(config.clone(), consensus_bus.clone(), network);

    assert_eq!(session.walk(forged.digest(), None).await, WalkOutcome::Incomplete);
    assert_eq!(
        requests.hashes(),
        vec![forged.digest(); 3],
        "the forged commit is asked for three times and refused each time"
    );
    assert_eq!(requests.by_number.load(Ordering::Relaxed), 0, "nothing below is prefetched");
    assert!(
        db.get_consensus_by_hash(forged.digest()).is_none(),
        "the forged commit must not be stored"
    );
    assert!(
        db.get::<ConsensusBlocksCache>(&101).unwrap().is_none(),
        "nothing may be stored at 101"
    );
    assert_eq!(
        consensus_bus.last_consensus_header().borrow().number,
        tip_before,
        "the forged commit must not become the tip"
    );

    // The real commit at 101, which differs only by the outsider's certificate, is taken.
    let (network, _) = walk_network(chain.clone(), &chain, HashMap::new());
    let session = WalkSession::new(config.clone(), consensus_bus.clone(), network);
    assert_eq!(session.walk(chain[&101].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(consensus_bus.last_consensus_header().borrow().number, 101);
}

/// Nothing at or below the session's covered anchor is asked for by number: a walk toward a
/// tip just above the anchor asks only for the headers in between.
#[tokio::test]
async fn walk_prefetch_stops_at_the_covered_anchor() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    for header in chain.range(..=130).map(|(_, h)| h) {
        store_consensus_header_in_cache(db, header);
    }

    let (network, requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);
    session.covered = Some(chain[&130].digest());

    assert_eq!(session.walk(chain[&140].digest(), None).await, WalkOutcome::Covered);
    assert_chain_cached(db, &chain);
    assert_eq!(requests.lowest_number.load(Ordering::Relaxed), 131);
    assert_eq!(
        requests.by_number.load(Ordering::Relaxed),
        9,
        "only 131..=139 are asked for by number"
    );
}

/// The fixture's node still running the epoch before the fixture's, as a node that has not
/// closed that epoch yet does: the fixture's committee, which signs the chain, is known from
/// the committee lookahead.
fn one_epoch_behind(config: &ConsensusConfig<MemDatabase>) -> ConsensusConfig<MemDatabase> {
    epochs_behind(config, 1)
}

/// [`one_epoch_behind`], `behind` epochs before the fixture's.
fn epochs_behind(
    config: &ConsensusConfig<MemDatabase>,
    behind: Epoch,
) -> ConsensusConfig<MemDatabase> {
    let committee = config.committee();
    let ahead = committee.epoch();
    ConsensusConfig::new_for_epoch(
        config.config().clone(),
        config.node_storage().clone(),
        config.key_config().clone(),
        committee.advance_epoch_for_test(ahead - behind),
        CommitteeLookahead::from_entries([(ahead, committee.bls_keys())]),
        config.network_config().clone(),
        u64::MAX,
        true,
    )
    .unwrap()
}

/// Store a certified record for `epoch` whose last consensus header is `end`.
fn certify_epoch_end<DB: ReDatabase, S: BlsSigner>(db: &DB, key: &S, epoch: Epoch, end: B256) {
    certify_epoch_end_naming(db, key, epoch, end, vec![]);
}

/// [`certify_epoch_end`] for a record that names `next_committee`, which a node reads the next
/// epoch's committee keys from when it does not run that epoch.
fn certify_epoch_end_naming<DB: ReDatabase, S: BlsSigner>(
    db: &DB,
    key: &S,
    epoch: Epoch,
    end: B256,
    next_committee: Vec<BlsPublicKey>,
) {
    let record = EpochRecord { epoch, parent_consensus: end, next_committee, ..Default::default() };
    let mut signed_authorities = roaring::RoaringBitmap::new();
    signed_authorities.push(0);
    let cert = EpochCertificate {
        epoch_hash: record.digest(),
        signature: record.sign_vote(key).signature,
        signed_authorities,
    };
    db.save_epoch_record_with_cert(&record, &cert).unwrap();
}

/// With certified records for the epochs in the gap, the walk goes to the end of each epoch
/// first, oldest first, and only then to the target. So the gap is stored from the bottom up,
/// one epoch at a time, and each header is still asked for once.
#[tokio::test]
async fn walk_goes_through_epoch_ends_oldest_first() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    let key = config.key_config();
    certify_epoch_end(db, key, 0, chain[&110].digest());
    certify_epoch_end(db, key, 1, chain[&125].digest());

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(
        requests.hashes(),
        vec![chain[&140].digest(), chain[&110].digest(), chain[&125].digest()],
        "the target first, for the records, then each epoch end oldest first"
    );
    assert_eq!(
        requests.by_number.load(Ordering::Relaxed),
        37,
        "every other header in 101..=139 is asked for by number once"
    );
    assert_eq!(session.covered, Some(chain[&140].digest()));
}

/// A record whose epoch end no peer serves does not stop catch-up: the walk to the target
/// covers the gap, as without records.
#[tokio::test]
async fn walk_falls_back_to_the_target_when_an_epoch_end_is_unavailable() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 120);
    certify_epoch_end(db, config.key_config(), 0, B256::repeat_byte(0x77));

    let (network, _requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&120].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(session.covered, Some(chain[&120].digest()));
}

/// A record without a certificate is not trusted as a checkpoint.
#[tokio::test]
async fn walk_ignores_an_uncertified_epoch_end() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 120);
    let record =
        EpochRecord { epoch: 0, parent_consensus: chain[&110].digest(), ..Default::default() };
    db.save_epoch_record(&record).unwrap();

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&120].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(requests.hashes(), vec![chain[&120].digest()], "only the target by hash");
}

/// The hashes asked for after the last request for `target`, with every request for
/// `target` before them.
fn hashes_after_target(requests: &mut WalkRequests, target: B256) -> Vec<B256> {
    let hashes = requests.hashes();
    let last = hashes.iter().rposition(|h| *h == target).expect("the target was asked for");
    hashes[last + 1..].to_vec()
}

/// A node one epoch behind finds the target's committee keys without the record collector,
/// from the committee lookahead. The walk must still ask the collector for the records of its
/// gap, and use them, or it gets no walk to an epoch end in the most common case.
#[tokio::test(start_paused = true)]
async fn walk_requests_the_records_when_the_keys_did_not_need_them() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(2)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = one_epoch_behind(&primary.consensus_config());
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 200);
    assert_eq!(config.epoch(), 1, "precondition: the node runs epoch 1");
    assert_eq!(chain[&200].sub_dag.leader_epoch(), 2, "precondition: the target is in epoch 2");
    assert!(
        config.get_committee_keys_for_epoch(2).is_some(),
        "precondition: the target's keys need no record"
    );

    // Stands in for the record collector: stores the records of the gap once asked.
    let bus = ConsensusBus::new();
    let mut asked = bus.requested_missing_epoch().subscribe();
    let collector_config = config.clone();
    // The node runs epoch 1, so epoch 0 ended at its tip; epoch 1 ends inside the gap.
    let ends = (chain[&101].parent_hash, chain[&125].digest());
    let next_committee = fixture.committee().bls_keys();
    tokio::spawn(async move {
        if asked.changed().await.is_ok() {
            let (db, key) = (collector_config.node_storage(), collector_config.key_config());
            certify_epoch_end(db, key, 0, ends.0);
            certify_epoch_end_naming(db, key, 1, ends.1, next_committee);
        }
    });

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    assert!(session.sync_to(chain[&200].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(*bus.requested_missing_epoch().borrow(), 1, "the records up to epoch 1");
    assert_eq!(
        requests.hashes(),
        vec![chain[&200].digest(), chain[&125].digest()],
        "the target, then the end of the epoch in the gap"
    );
}

/// A failed fetch of the target does not skip the walks to the epoch ends below it, which
/// need only the stored records. The target is not asked for again in the same pass.
#[tokio::test]
async fn walk_goes_through_epoch_ends_when_the_target_fetch_fails() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    let key = config.key_config();
    certify_epoch_end(db, key, 0, chain[&110].digest());
    certify_epoch_end(db, key, 1, chain[&125].digest());

    // No peer serves the target by hash.
    let mut by_hash = chain.clone();
    by_hash.remove(&140);
    let (network, mut requests) = walk_network(chain.clone(), &by_hash, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert_chain_cached(db, &chain.range(..=125).map(|(n, h)| (*n, h.clone())).collect());
    assert_eq!(session.covered, Some(chain[&125].digest()));
    assert_eq!(
        hashes_after_target(&mut requests, chain[&140].digest()),
        vec![chain[&110].digest(), chain[&125].digest()],
        "each epoch end oldest first, and the target is not asked for again"
    );
}

/// A walk to an epoch end that does not complete ends the walks to epoch ends for this pass:
/// the later ones are not tried, and the walk to the target covers the rest of the gap.
#[tokio::test]
async fn walk_stops_at_an_unavailable_epoch_end() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    let key = config.key_config();
    let unavailable = B256::repeat_byte(0x77);
    certify_epoch_end(db, key, 0, unavailable);
    certify_epoch_end(db, key, 1, chain[&125].digest());

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    let after_target = hashes_after_target(&mut requests, chain[&140].digest());
    assert!(!after_target.is_empty(), "the missing epoch end was tried");
    assert!(after_target.iter().all(|h| *h == unavailable), "and no later epoch end");
    assert_eq!(session.next_checkpoint, 0, "no epoch end was covered");
    assert_eq!(session.covered, Some(chain[&140].digest()));
}

/// A target already stored, for example by an earlier pass that did not finish, still gets
/// the walks to the epoch ends below it.
#[tokio::test]
async fn walk_goes_through_epoch_ends_when_the_target_is_stored() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    store_consensus_header_in_cache(db, &chain[&140]);
    let key = config.key_config();
    certify_epoch_end(db, key, 0, chain[&110].digest());
    certify_epoch_end(db, key, 1, chain[&125].digest());

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);

    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(
        requests.hashes(),
        vec![chain[&110].digest(), chain[&125].digest()],
        "each epoch end oldest first"
    );
}

/// A node that follows the tip and still runs epoch 1 gets a target in epoch 2 before epoch
/// 1's last header is stored. Epoch 1's record may not be certified anywhere yet, so the walk
/// asks for it but does not wait: it walks to the target at once. It asks only once per
/// target epoch.
#[tokio::test(start_paused = true)]
async fn walk_does_not_wait_for_epoch_records_that_do_not_arrive() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(2)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = one_epoch_behind(&primary.consensus_config());
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    assert_eq!(config.epoch(), 1, "precondition: the node runs epoch 1");
    assert_eq!(chain[&130].sub_dag.leader_epoch(), 2, "precondition: the target is in epoch 2");

    // No collector answers the request.
    let bus = ConsensusBus::new();
    let mut asked = bus.requested_missing_epoch().subscribe();
    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    let start = tokio::time::Instant::now();
    assert!(session.sync_to(chain[&130].digest()).await.is_continue());
    assert_eq!(start.elapsed(), Duration::ZERO, "the walk did not wait for the records");
    assert!(asked.has_changed().unwrap(), "the records were asked for");
    assert_eq!(*asked.borrow_and_update(), 1, "up to epoch 1");
    assert_chain_cached(db, &chain.range(..=130).map(|(n, h)| (*n, h.clone())).collect());
    assert_eq!(requests.hashes(), vec![chain[&130].digest()], "only the target, by hash");
    assert_eq!(session.covered, Some(chain[&130].digest()));

    // Lowered again, as the collector does after a pass that did not reach the epoch.
    bus.requested_missing_epoch().send_replace(0);
    let _ = asked.borrow_and_update();
    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert!(!asked.has_changed().unwrap(), "not asked again for the same target epoch");
    assert_chain_cached(db, &chain);
}

/// The records of the gap land while the walk to the target is already prefetching. The walk
/// stops before its next round, walks to the epoch end in the gap, then walks to the target
/// again, so the streamer can run the epoch below while the rest is fetched.
#[tokio::test(start_paused = true)]
async fn walk_diverts_to_an_epoch_end_when_its_record_lands_mid_walk() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(2)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = one_epoch_behind(&primary.consensus_config());
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 200);

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    // Stands in for the record collector: stores the records only once the walk has asked
    // for a full round of headers by number, so they land mid-walk.
    let bus = ConsensusBus::new();
    let mut asked = bus.requested_missing_epoch().subscribe();
    let mut numbers = requests.numbers.clone();
    let collector_config = config.clone();
    let ends = (chain[&101].parent_hash, chain[&125].digest());
    let next_committee = fixture.committee().bls_keys();
    tokio::spawn(async move {
        if asked.changed().await.is_err()
            || numbers.wait_for(|count| *count >= WALK_PREFETCH as usize).await.is_err()
        {
            return;
        }
        let (db, key) = (collector_config.node_storage(), collector_config.key_config());
        certify_epoch_end(db, key, 0, ends.0);
        certify_epoch_end_naming(db, key, 1, ends.1, next_committee);
    });
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    assert!(session.sync_to(chain[&200].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(
        requests.hashes(),
        vec![chain[&200].digest(), chain[&125].digest()],
        "the target, then the end of the epoch in the gap, fetched by hash"
    );
    assert_eq!(session.covered, Some(chain[&200].digest()));
}

/// A diverted walk whose epoch end cannot be fetched falls back to the target, as without
/// records, and does not divert again in the same pass.
#[tokio::test(start_paused = true)]
async fn walk_diverts_once_when_the_epoch_end_is_unavailable() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(2)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = one_epoch_behind(&primary.consensus_config());
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);

    // Stands in for the record collector: epoch 1's record names an end no peer serves.
    let bus = ConsensusBus::new();
    let mut asked = bus.requested_missing_epoch().subscribe();
    let collector_config = config.clone();
    let tip = chain[&101].parent_hash;
    let unavailable = B256::repeat_byte(0x77);
    let next_committee = fixture.committee().bls_keys();
    tokio::spawn(async move {
        if asked.changed().await.is_ok() {
            let (db, key) = (collector_config.node_storage(), collector_config.key_config());
            certify_epoch_end(db, key, 0, tip);
            certify_epoch_end_naming(db, key, 1, unavailable, next_committee);
        }
    });
    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    let start = tokio::time::Instant::now();
    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert!(start.elapsed() < Duration::from_secs(5), "only the missing end's retries");
    assert_chain_cached(db, &chain);
    let after_target = hashes_after_target(&mut requests, chain[&140].digest());
    assert!(!after_target.is_empty(), "the walk diverted to the missing epoch end");
    assert!(after_target.iter().all(|h| *h == unavailable), "and no other hash after the target");
    assert_eq!(session.covered, Some(chain[&140].digest()));
}

/// An epoch end that cannot be fetched is tried once per pass. Two epochs behind, the node
/// already holds epoch 1's record, whose end no peer serves, and asks for epoch 2's. The walk
/// to epoch 1's end fails first, and the walk to the target must not divert back to it.
#[tokio::test(start_paused = true)]
async fn walk_does_not_divert_back_to_an_epoch_end_that_failed() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(3)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = epochs_behind(&primary.consensus_config(), 2);
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 140);
    assert_eq!(config.epoch(), 1, "precondition: the node runs epoch 1");
    assert_eq!(chain[&140].sub_dag.leader_epoch(), 3, "precondition: the target is in epoch 3");
    let unavailable = B256::repeat_byte(0x77);
    certify_epoch_end(db, config.key_config(), 1, unavailable);

    // The requests one failed walk to the missing end makes.
    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let probe = WalkSession::new(config.clone(), ConsensusBus::new(), network);
    assert_eq!(probe.walk(unavailable, None).await, WalkOutcome::Incomplete);
    let one_walk = requests.hashes().len();
    assert!(one_walk > 0, "precondition: the walk asked for the missing end");

    // No collector answers the request for epoch 2's record.
    let bus = ConsensusBus::new();
    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    assert!(session.sync_to(chain[&140].digest()).await.is_continue());
    assert_eq!(*bus.requested_missing_epoch().borrow(), 2, "epoch 2's record was asked for");
    assert_chain_cached(db, &chain);
    let after_target = hashes_after_target(&mut requests, chain[&140].digest());
    assert!(after_target.iter().all(|h| *h == unavailable), "only the missing end by hash");
    assert_eq!(after_target.len(), one_walk, "the missing end was walked to once");
    assert_eq!(session.covered, Some(chain[&140].digest()));
}

/// A node still in epoch 0 with the target in epoch 1 needs epoch 0's record. The collector
/// acts only on an epoch above the last record it holds, which counts as 0 when it holds
/// none, so the walk must ask for epoch 1 to get it.
#[tokio::test(start_paused = true)]
async fn walk_gets_epoch_0_record_for_a_target_in_epoch_1() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = one_epoch_behind(&primary.consensus_config());
    let db = config.node_storage();
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_tip_and_chain(db, &leader, 200);
    assert_eq!(chain[&200].sub_dag.leader_epoch(), 1, "precondition: the target is in epoch 1");

    // Stands in for the record collector, which acts only on an epoch above 0 here.
    let bus = ConsensusBus::new();
    let mut asked = bus.requested_missing_epoch().subscribe();
    let collector_config = config.clone();
    let end = chain[&110].digest();
    let next_committee = fixture.committee().bls_keys();
    tokio::spawn(async move {
        while asked.changed().await.is_ok() {
            if *asked.borrow_and_update() > 0 {
                let (db, key) = (collector_config.node_storage(), collector_config.key_config());
                certify_epoch_end_naming(db, key, 0, end, next_committee);
                return;
            }
        }
    });

    let (network, mut requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    assert!(session.sync_to(chain[&200].digest()).await.is_continue());
    assert_chain_cached(db, &chain);
    assert_eq!(
        requests.hashes(),
        vec![chain[&200].digest(), chain[&110].digest()],
        "the target, then epoch 0's end"
    );
}

/// A node that follows the tip and has just moved to epoch 2 has executed epoch 1's last
/// header itself: its tip is that header. Its first target in epoch 2 must not ask for epoch
/// 1's record, which its epoch manager is still certifying and the walk does not need.
#[tokio::test]
async fn walk_does_not_ask_for_the_record_of_an_epoch_the_node_closed() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(2)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let last_of_epoch_1 =
        fixture.certificate(&fixture.header_builder_last_authority().epoch(1).build());
    let tip = ConsensusHeader {
        number: 100,
        sub_dag: CommittedSubDag::new(
            vec![],
            last_of_epoch_1,
            0,
            ReputationScores::default(),
            None,
        ),
        ..Default::default()
    };
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let chain = canonical_chain_above(db, tip, &leader, 101);
    assert_eq!(config.epoch(), 2, "precondition: the node runs epoch 2");
    assert_eq!(chain[&101].sub_dag.leader_epoch(), 2, "precondition: the target is in epoch 2");

    // No collector answers, and epoch 1's record is not stored yet.
    let bus = ConsensusBus::new();
    let (network, _requests) = walk_network(chain.clone(), &chain, HashMap::new());
    let mut session = WalkSession::new(config.clone(), bus.clone(), network);

    assert!(session.sync_to(chain[&101].digest()).await.is_continue());
    assert_eq!(*bus.requested_missing_epoch().borrow(), 0, "no record was asked for");
    assert_chain_cached(db, &chain);
}
