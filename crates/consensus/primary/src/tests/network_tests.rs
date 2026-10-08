//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{message::PrimaryGossip, MissingCertificatesRequest, RequestHandler},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use assert_matches::assert_matches;
use rayls_consensus_network::{GossipMessage, TopicHash};
use rayls_infrastructure_config::LibP2pConfig;
use rayls_infrastructure_storage::mem_db::MemDatabase;
use rayls_infrastructure_types::{
    encode, error::HeaderError, now, AuthorityIdentifier, BlockHash, BlockHeader, BlockNumHash,
    BlsPublicKey, Certificate, CertificateDigest, ExecHeader, Hash as _, RaylsReceiver,
    RaylsSender, SealedHeader, TaskManager,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

#[test]
// for primary::network::message
fn test_missing_certs_request() {
    let max = 10;
    let expected_gc_round = 3;
    let expected_skip_rounds: BTreeMap<_, _> = [
        (AuthorityIdentifier::dummy_for_test(0), BTreeSet::from([4, 5, 6, 7])),
        (AuthorityIdentifier::dummy_for_test(2), BTreeSet::from([6, 7, 8])),
    ]
    .into_iter()
    .collect();
    let missing_req = MissingCertificatesRequest::default()
        .set_bounds(expected_gc_round, expected_skip_rounds.clone())
        .expect("boundary set")
        .set_max_response_size(max);
    let (decoded_gc_round, decoded_skip_rounds) =
        missing_req.get_bounds(max_skip_rounds()).expect("decode missing bounds");
    assert_eq!(expected_gc_round, decoded_gc_round);
    assert_eq!(expected_skip_rounds, decoded_skip_rounds);
}

/// A crafted request whose lower bound plus a skip delta exceeds `Round::MAX` is rejected
/// instead of overflowing. The release profile aborts on overflow, so before this any peer
/// could take a node down with one message.
#[test]
fn test_missing_certs_request_rejects_round_overflow() {
    let mut serialized = Vec::new();
    roaring::RoaringBitmap::from_iter([1u32]).serialize_into(&mut serialized).expect("bitmap");
    let request = MissingCertificatesRequest {
        exclusive_lower_bound: u32::MAX,
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(0), serialized)],
        max_response_size: 10,
        exclusive_upper_bound: None,
    };
    assert_matches!(request.get_bounds(max_skip_rounds()), Err(PrimaryNetworkError::StdIo(_)));

    // the same delta below the edge decodes as before
    let request = MissingCertificatesRequest { exclusive_lower_bound: u32::MAX - 1, ..request };
    let (_, skip) = request.get_bounds(max_skip_rounds()).expect("no overflow");
    assert_eq!(skip[&AuthorityIdentifier::dummy_for_test(0)], BTreeSet::from([u32::MAX]));
}

/// The type for holding testng components.
struct TestTypes<DB = MemDatabase> {
    /// Committee committee with authorities that vote.
    committee: CommitteeFixture<DB>,
    // /// The authority that receives messages.
    // authority: &'a AuthorityFixture<DB>,
    /// The handler for requests.
    handler: RequestHandler<DB>,
    /// The parent execution result for all primary headers.
    ///
    /// num: 0
    /// hash: 0x78dec18c6d7da925bbe773c315653cdc70f6444ed6c1de9ac30bdb36cff74c3b
    parent: SealedHeader,
    /// Task manager the synchronizer (in RequestHandler) is spawned on.
    /// Save it so that task is not dropped early if needed.
    task_manager: TaskManager,
    /// The consensus bus for tests.
    consensus_bus: ConsensusBus,
}

/// Helper function to create an instance of [RequestHandler] for the first authority in the
/// committee.
fn create_test_types() -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let config = authority.consensus_config();
    let cb = ConsensusBus::new();

    // spawn the synchronizer
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    // last execution result
    let parent = SealedHeader::seal_slow(ExecHeader::default());

    // set the latest execution result to genesis - test headers are proposed for round 1
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(parent.clone()));

    let handler = RequestHandler::new(config.clone(), cb.clone(), synchronizer);
    TestTypes { committee, handler, parent, task_manager, consensus_bus: cb }
}

#[tokio::test]
async fn test_vote_succeeds() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert!(res.is_ok());
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_too_many_parents() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    // last authority produced 2 certs for round 1
    let mut too_many_parents: Vec<_> = Certificate::genesis(&committee.committee());
    let extra_parent = too_many_parents.last().expect("last cert").clone();
    too_many_parents.push(extra_parent.clone());

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, too_many_parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::TooManyParents(received, expected))) if received == 5 && expected == 4 );
    Ok(())
}

/// A round-0 header is never a valid proposal. Before the floor in `vote_inner`, a round-0 header
/// with an unknown parent passed `Header::validate` and the too-old gate (the local round is 0 at
/// node start and at every epoch start) and reached `check_for_missing_parents`, which keyed the
/// request by `round - 1`: with overflow checks on and `panic = "abort"` in release, one such
/// request from a committee member aborted the node.
#[tokio::test]
async fn test_vote_fails_round_zero() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let unknown_parent = CertificateDigest::new(BlockHash::random().0);
    let header = committee
        .header_builder_last_authority()
        .round(0)
        .parents([unknown_parent].into_iter().collect())
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    let res = handler.vote(peer, header, Vec::new()).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidRound { header_round: 0, .. }))
    );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_authority_network_key() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let random_key = BlsPublicKey::default();

    // process vote
    let res = handler.vote(random_key, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_genesis_parent() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let parents = Vec::new();

    // start with the expected parents in genesis
    let mut expected_parents: Vec<_> =
        Certificate::genesis(&committee.committee()).iter().map(|x| x.digest()).collect();
    let extra_parent = CertificateDigest::new(BlockHash::random().0);
    expected_parents.pop();
    expected_parents.push(extra_parent);
    let wrong_genesis: BTreeSet<_> = expected_parents.into_iter().collect();

    // create header proposed by last peer in the committee for round 1
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .parents(wrong_genesis)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidGenesisParent(wrong))) if wrong == extra_parent);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_execution_result() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, task_manager: _task_manager, .. } = create_test_types();

    // create header proposed by last peer in the committee for round 1
    let header = committee.header_from_last_authority();
    let parents = Vec::new();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownExecutionResult(wrong_hash))) if wrong_hash.hash == BlockHash::ZERO);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_header_digest() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, task_manager: _task_manager, .. } = create_test_types();

    let parents = Vec::new();

    // create header proposed by last peer in the committee for round 1
    let mut header = committee.header_from_last_authority();
    // change values so digest doesn't match
    header.latest_execution_block = BlockNumHash::new(0, BlockHash::random());
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidHeaderDigest)));
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_invalid_timestamp() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_time = now() + 100000; // too far in the future
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(wrong_time)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidTimestamp{created: wrong, ..})) if wrong == wrong_time);
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_wrong_epoch() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_epoch = 3;
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .epoch(wrong_epoch)
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidEpoch{ theirs: wrong, ours: correct })) if wrong == wrong_epoch && correct == 0 );
    Ok(())
}

#[tokio::test]
async fn test_vote_fails_unknown_authority() -> eyre::Result<()> {
    // common types
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();

    let parents = Vec::new();

    // create valid header proposed by last peer in the committee for round 1
    let wrong_authority = AuthorityIdentifier::dummy_for_test(100);
    let header = committee
        .header_builder_last_authority()
        .author(wrong_authority.clone())
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    // process vote
    let res = handler.vote(peer, header, parents).await;
    debug!(target: "primary::handler_tests", ?res);
    assert_matches!(res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnknownAuthority(wrong))) if wrong == wrong_authority.to_string());
    Ok(())
}

/// Test that primary pub/sub is enforcing topics.
#[tokio::test]
async fn test_primary_batch_gossip_topics() {
    let TestTypes { handler, task_manager, consensus_bus, .. } = create_test_types();

    task_manager.spawn_task("process-gossip-test", async move {
        let mut rx = consensus_bus.new_epoch_votes().subscribe();
        while let Some((_, tx)) = rx.recv().await {
            let _ = tx.send(Ok(()));
        }
    });

    let gossip = PrimaryGossip::Certificate(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::primary_topic());
    let goodish_msg =
        GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&goodish_msg).await;
    // This will be rejected for other reasons, but make sure not for an invalid topic.
    assert!(!matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::consensus_output_topic());
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&good_msg).await.is_ok());

    let gossip = PrimaryGossip::EpochVote(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::epoch_vote_topic());
    let good_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&good_msg).await.is_ok());

    let gossip = PrimaryGossip::Certificate(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::epoch_vote_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    let res = handler.process_gossip(&bad_msg).await;
    // This will be rejected for other reasons, but make sure it is for an invalid topic.
    assert!(matches!(res, Err(PrimaryNetworkError::InvalidTopic)));

    let gossip = PrimaryGossip::Consensus(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::primary_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());

    let gossip = PrimaryGossip::EpochVote(Box::default());
    let data = encode(&gossip);
    let topic = TopicHash::from_raw(LibP2pConfig::consensus_output_topic());
    let bad_msg = GossipMessage { source: None, data: data.clone(), sequence_number: None, topic };
    assert!(handler.process_gossip(&bad_msg).await.is_err());
}

/// Most skip rounds a request may carry per authority, as configured by default.
fn max_skip_rounds() -> usize {
    rayls_infrastructure_config::SyncConfig::default().max_skip_rounds_for_missing_certs
}

/// ULEB128 encoding of `value`, as bcs writes a list length.
fn uleb128(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// Decoding refuses a skip-round list longer than the wire limit.
#[test]
fn test_missing_certs_request_decode_bounds_authority_list() {
    use super::message::MAX_SKIP_ROUND_AUTHORITIES;
    use rayls_infrastructure_types::try_decode;

    // The audit's request named this many invented authorities.
    const AUDIT_AUTHORITIES: usize = 50_000;

    let request = |count: usize| MissingCertificatesRequest {
        skip_rounds: (0..count)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
                (try_decode::<AuthorityIdentifier>(&bytes).expect("id"), vec![])
            })
            .collect(),
        ..Default::default()
    };

    let at_limit = encode(&request(MAX_SKIP_ROUND_AUTHORITIES));
    let decoded: MissingCertificatesRequest = try_decode(&at_limit).expect("at the limit decodes");
    assert_eq!(decoded.skip_rounds.len(), MAX_SKIP_ROUND_AUTHORITIES);

    let over = encode(&request(MAX_SKIP_ROUND_AUTHORITIES + 1));
    assert!(try_decode::<MissingCertificatesRequest>(&over).is_err());

    // The audit's request fits under the message limit and is refused.
    let attack = encode(&request(AUDIT_AUTHORITIES));
    assert!(attack.len() < LibP2pConfig::default().max_rpc_message_size);
    assert!(try_decode::<MissingCertificatesRequest>(&attack).is_err());

    // A list that only declares the largest length bcs accepts is refused.
    let empty = encode(&request(0));
    // the list length follows the lower bound
    let at = encode(&request(0).exclusive_lower_bound).len();
    assert_eq!(empty[at..at + 1], uleb128(0)[..]);
    let mut declared = empty[..at].to_vec();
    declared.extend(uleb128(bcs::MAX_SEQUENCE_LENGTH));
    declared.extend_from_slice(&empty[at + 1..]);
    assert!(try_decode::<MissingCertificatesRequest>(&declared).is_err());
}

/// Serialized roaring bitmap made of `containers` full run containers.
///
/// Each container holds every value of its 16-bit range, using the portable run-container format.
/// The roaring crate never writes run containers, so the bytes are built by hand.
fn full_run_bitmap(containers: u16) -> Vec<u8> {
    assert!(containers > 0, "a bitmap needs at least one container");
    // header cookie of a bitmap with run containers
    const SERIAL_COOKIE: u32 = 12347;
    // from this many containers on, an offset table follows the descriptions
    const NO_OFFSET_THRESHOLD: usize = 4;
    // a container with one run: the run count, the start and the length minus one
    const ONE_RUN_LEN: usize = 3 * size_of::<u16>();

    let n = containers as usize;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(SERIAL_COOKIE | (u32::from(containers - 1) << u16::BITS)).to_le_bytes(),
    );
    // every container is a run container
    bytes.extend(std::iter::repeat_n(u8::MAX, n.div_ceil(u8::BITS as usize)));
    for key in 0..containers {
        // the container key, then its value count minus one
        bytes.extend_from_slice(&key.to_le_bytes());
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
    }
    if n >= NO_OFFSET_THRESHOLD {
        let start = bytes.len() + size_of::<u32>() * n;
        for i in 0..n {
            bytes.extend_from_slice(&((start + ONE_RUN_LEN * i) as u32).to_le_bytes());
        }
    }
    for _ in 0..containers {
        // one run from the first value covering the whole range
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
    }
    bytes
}

/// Serialized roaring bitmap with one run container of `runs` single-value runs in descending
/// order.
///
/// Decoding inserts each run at the front, so the work grows with the square of `runs`.
fn descending_runs_bitmap(runs: u16) -> Vec<u8> {
    // header cookie of a bitmap with run containers, for one container
    const SERIAL_COOKIE: u32 = 12347;
    let mut bytes = SERIAL_COOKIE.to_le_bytes().to_vec();
    // the only container is a run container
    bytes.push(1);
    // the container key, then its value count minus one
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&(runs - 1).to_le_bytes());
    bytes.extend_from_slice(&runs.to_le_bytes());
    for start in (0..runs).rev() {
        // a run of one value
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
    }
    bytes
}

/// A skip bitmap that declares too many containers is refused before it is decoded.
///
/// A few hundred bytes can stand for over a million rounds, so the container count is checked
/// first.
#[test]
fn test_skip_bitmap_is_refused_before_decoding() {
    use super::message::MAX_SKIP_ROUND_CONTAINERS;
    use crate::state_sync::CertificateCollector;

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let max_response_size = LibP2pConfig::default().max_rpc_message_size;
    let request = |bitmap: Vec<u8>| MissingCertificatesRequest {
        skip_rounds: vec![(AuthorityIdentifier::dummy_for_test(1), bitmap)],
        max_response_size,
        ..Default::default()
    };
    let refusal =
        |bitmap: Vec<u8>| CertificateCollector::new(request(bitmap), config.clone()).err();
    let max_containers = u16::try_from(MAX_SKIP_ROUND_CONTAINERS).expect("fits a container key");

    // One container over the limit stands for over a million rounds.
    let over = full_run_bitmap(max_containers + 1);
    let bitmap = roaring::RoaringBitmap::deserialize_from(&over[..]).expect("valid bitmap");
    assert_eq!(bitmap.len(), u64::from(max_containers + 1) << u16::BITS);
    assert_matches!(refusal(over), Some(PrimaryNetworkError::StdIo(_)));

    // A bitmap written by the roaring crate with one container too many is refused too.
    let mut spread = roaring::RoaringBitmap::new();
    for key in 0..=u32::from(max_containers) {
        spread.insert(key << u16::BITS);
    }
    let mut written = Vec::new();
    spread.serialize_into(&mut written).expect("serialize");
    assert_matches!(refusal(written), Some(PrimaryNetworkError::StdIo(_)));

    // A bitmap longer than any honest one is refused before decoding.
    // Each run takes four bytes, twice what an honest round takes.
    let runs = u16::try_from(max_skip_rounds() + 1).expect("fits a container");
    assert_matches!(
        refusal(descending_runs_bitmap(runs)),
        Some(PrimaryNetworkError::InvalidRequest(reason)) if reason.contains("too long")
    );

    // Within the container limit, a bitmap over the round limit is refused.
    assert_matches!(
        refusal(full_run_bitmap(max_containers)),
        Some(PrimaryNetworkError::InvalidRequest(_))
    );

    // An unknown header is refused.
    assert_matches!(refusal(vec![1, 2, 3]), Some(PrimaryNetworkError::StdIo(_)));

    // An honest bitmap at the round limit is accepted.
    let lower_bound = 10;
    let max_rounds = u32::try_from(max_skip_rounds()).expect("fits a round");
    let honest = MissingCertificatesRequest::default()
        .set_bounds(
            lower_bound,
            [(
                AuthorityIdentifier::dummy_for_test(1),
                (lower_bound + 1..=lower_bound + max_rounds).collect(),
            )]
            .into_iter()
            .collect(),
        )
        .expect("bounds")
        .set_max_response_size(max_response_size);
    assert!(CertificateCollector::new(honest, config.clone()).is_ok());
}
