//! Test for Primary <-> Primary handler.

use crate::{
    error::PrimaryNetworkError,
    network::{
        handler::fetch_scope, message::PrimaryGossip, MissingCertificatesRequest, PrimaryRequest,
        RequestHandler,
    },
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use assert_matches::assert_matches;
use rayls_consensus_network::{GossipMessage, TopicHash};
use rayls_infrastructure_config::LibP2pConfig;
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertScope};
use rayls_infrastructure_types::{
    encode, error::HeaderError, now, AuthorityIdentifier, BlockHash, BlockHeader, BlockNumHash,
    BlsPublicKey, BlsSignature, Certificate, CertificateDigest, ExecHeader, Hash as _,
    OwnParentSigners, RaylsReceiver, RaylsSender, SealedHeader, TaskManager,
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
        missing_req.get_bounds(max_skip_rounds(), |_| true).expect("decode missing bounds");
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
        skip_rounds: [(AuthorityIdentifier::dummy_for_test(0), serialized)].into_iter().collect(),
        max_response_size: 10,
        exclusive_upper_bound: None,
    };
    assert_matches!(
        request.get_bounds(max_skip_rounds(), |_| true),
        Err(PrimaryNetworkError::StdIo(_))
    );

    // the same delta below the edge decodes as before
    let request = MissingCertificatesRequest { exclusive_lower_bound: u32::MAX - 1, ..request };
    let (_, skip) = request.get_bounds(max_skip_rounds(), |_| true).expect("no overflow");
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
    create_test_types_with(false)
}

/// Like [`create_test_types`], with the `HeaderParentSigners` fork state set on the handler's
/// config.
fn create_test_types_with(header_parent_signers_active: bool) -> TestTypes {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let authority = committee.first_authority();
    let mut config = authority.consensus_config();
    config.set_header_parent_signers_active(header_parent_signers_active);
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

/// Once the fork is active a vote request must carry the author's own-parent signer claim and it
/// must match the parent certificate this node holds. Round-1 headers build on genesis
/// certificates, whose signer bitmaps are empty, so the only valid claim there is an empty one.
#[tokio::test]
async fn test_vote_own_parent_signers_required_and_checked_once_fork_is_active() -> eyre::Result<()>
{
    // a refused vote request still marks the author's round on this handler, so each case gets
    // a fresh one
    let vote = |claim: Option<OwnParentSigners>| async move {
        let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
            create_test_types_with(true);
        let peer = *committee.last_authority().authority().protocol_key();
        let header = committee
            .header_builder_last_authority()
            .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
            .created_at(1) // parent is 0
            .own_parent_signers(claim)
            .build();
        handler.vote(peer, header, Vec::new()).await
    };

    // no claim at all
    let res = vote(None).await;
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::MissingOwnParentSigners))
    );

    // a claim that disagrees with the (empty) genesis parent and cannot verify: one signer is
    // below quorum, and the signature is not theirs anyway
    let res = vote(Some(OwnParentSigners {
        signers: roaring::RoaringBitmap::from_iter([0u32]),
        signature: Some(BlsSignature::default()),
    }))
    .await;
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::OwnParentSignersInvalid {
            claimed,
            expected,
        })) if claimed == vec![0] && expected.is_empty()
    );

    // signers without a signature: refused as malformed before any comparison
    let res =
        vote(Some(OwnParentSigners::unsigned_for_test(roaring::RoaringBitmap::from_iter([0u32]))))
            .await;
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::OwnParentSignersMalformed))
    );

    // the matching claim
    let res = vote(Some(OwnParentSigners::empty())).await;
    assert!(res.is_ok(), "{res:?}");
    Ok(())
}

/// The property the fork exists for, against real parents: the voter holds the author's
/// round-1 certificate with signer set A. A round-2 header claiming A is voted at once. One
/// claiming another valid quorum B is verified on its own and voted too, with and without the
/// proposer shipping its B copy, and the voter's stored copy converges on B. Claims that cannot
/// verify, an empty one, A's signers under B's signature, or a real aggregate below quorum, are
/// refused and leave the stored copy alone.
#[tokio::test]
async fn test_vote_own_parent_signers_checked_against_the_stored_parent() -> eyre::Result<()> {
    use rayls_infrastructure_storage::CertificateStore as _;

    // the claim is picked from the variants once they exist: A = stored, B = the other quorum,
    // SMALL = a two-vote aggregate below quorum
    type Claim = fn(&Certificate, &Certificate, &Certificate) -> OwnParentSigners;

    // one vote request per fresh handler: a refused request marks the author's round. The claim
    // is picked from the two variants (A = stored, B = the other quorum) once they exist.
    let vote = |claim_of: Claim, ship_variant_b: bool| async move {
        let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
            create_test_types_with(true);
        let author = committee.last_authority();
        let peer = *author.authority().protocol_key();
        let exec = BlockNumHash::new(parent.number(), parent.hash());

        // the author's round-1 header, with every committee vote available
        let own_header = author.header_builder(&committee.committee()).created_at(1).build();
        let mut votes: Vec<_> = committee
            .votes(&own_header)
            .into_iter()
            .map(|v| (v.author().clone(), *v.signature()))
            .collect();
        let own_vote = author.vote(&own_header);
        if !votes.iter().any(|(a, _)| a == own_vote.author()) {
            votes.push((own_vote.author().clone(), *own_vote.signature()));
        }
        votes.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(votes.len(), 4, "fixture must yield a vote from every authority");
        // two distinct, equally valid quorum subsets of the same header
        let variant_a = Certificate::new_unverified(
            &committee.committee(),
            own_header.clone(),
            votes[..3].to_vec(),
        )
        .expect("quorum A");
        let variant_b = Certificate::new_unverified(
            &committee.committee(),
            own_header.clone(),
            votes[1..].to_vec(),
        )
        .expect("quorum B");
        assert_eq!(variant_a.digest(), variant_b.digest());
        assert_ne!(variant_a.signed_authorities(), variant_b.signed_authorities());
        let variant_small = Certificate::new_unsigned_for_test(
            &committee.committee(),
            own_header.clone(),
            votes[..2].to_vec(),
        )
        .expect("two-vote aggregate");

        // the voter already holds variant A, plus two other round-1 parents for quorum
        let others: Vec<Certificate> = committee
            .authorities()
            .filter(|a| a.id() != author.id())
            .take(2)
            .map(|a| {
                committee
                    .certificate(&a.header_builder(&committee.committee()).created_at(1).build())
            })
            .collect();
        let mut stored = others.clone();
        stored.push(variant_a.clone());
        committee
            .first_authority()
            .consensus_config()
            .node_storage()
            .write_all(stored.clone())
            .expect("parents stored");

        let header = author
            .header_builder(&committee.committee())
            .round(2)
            .parents(stored.iter().map(|c| c.digest()).collect())
            .latest_execution_block(exec)
            .created_at(2)
            .own_parent_signers(Some(claim_of(&variant_a, &variant_b, &variant_small)))
            .build();
        let shipped = if ship_variant_b {
            let mut shipped = others;
            shipped.push(variant_b.clone());
            shipped
        } else {
            Vec::new()
        };
        let res = handler.vote(peer, header, shipped).await;
        // what the voter holds for the author's round-1 certificate after the request
        let stored_after = committee
            .first_authority()
            .consensus_config()
            .node_storage()
            .read(variant_a.digest())
            .expect("store read")
            .expect("parent still stored")
            .signed_authorities()
            .clone();
        (
            res,
            stored_after,
            variant_a.signed_authorities().clone(),
            variant_b.signed_authorities().clone(),
        )
    };
    let invalid = |res: &Result<_, PrimaryNetworkError>| {
        matches!(
            res,
            Err(PrimaryNetworkError::InvalidHeader(HeaderError::OwnParentSignersInvalid { .. }))
        )
    };

    // an empty claim while the author's certificate is among the parents: nothing to verify
    let (res, stored, a, _) = vote(|_, _, _| OwnParentSigners::empty(), false).await;
    assert!(invalid(&res), "{res:?}");
    assert_eq!(stored, a, "an invalid claim never touches the stored copy");
    // the exact stored set: a vote, store untouched
    let (res, stored, a, _) = vote(|a, _, _| OwnParentSigners::from_certificate(a), false).await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(stored, a);
    // another valid quorum of the same header: verified and accepted whatever copy is stored,
    // and the stored copy converges on the author's claimed variant
    let (res, stored, _, b) = vote(|_, b, _| OwnParentSigners::from_certificate(b), false).await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(stored, b, "the stored copy adopts the verified claim");
    // the same, with the proposer shipping its B copy alongside: the stored A is compared first,
    // the claim verifies, the copy converges
    let (res, stored, _, b) = vote(|_, b, _| OwnParentSigners::from_certificate(b), true).await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(stored, b);
    // the stored signer set with the other quorum's signature: fails verification
    let (res, stored, a, _) = vote(
        |a, b, _| OwnParentSigners {
            signers: a.signed_authorities().clone(),
            signature: b.aggregated_signature(),
        },
        false,
    )
    .await;
    assert!(invalid(&res), "{res:?}");
    assert_eq!(stored, a);
    // a real aggregate of two votes, below quorum: fails verification
    let (res, stored, a, _) =
        vote(|_, _, small| OwnParentSigners::from_certificate(small), false).await;
    assert!(invalid(&res), "{res:?}");
    assert_eq!(stored, a);
    Ok(())
}

/// The other half of the parent-copy rule: a voter that does not hold the author's round-1
/// certificate asks for the parents, receives them with the retried request, verifies and
/// stores them, and compares the claim against that shipped copy.
#[tokio::test]
async fn test_vote_own_parent_signers_checked_against_the_shipped_parent() -> eyre::Result<()> {
    use crate::network::message::PrimaryResponse;
    type Claim = fn(&Certificate) -> OwnParentSigners;

    let vote = |claim_of: Claim| async move {
        let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
            create_test_types_with(true);
        let author = committee.last_authority();
        let peer = *author.authority().protocol_key();
        let exec = BlockNumHash::new(parent.number(), parent.hash());

        let own_cert = committee
            .certificate(&author.header_builder(&committee.committee()).created_at(1).build());
        let mut parents: Vec<Certificate> = committee
            .authorities()
            .filter(|a| a.id() != author.id())
            .take(2)
            .map(|a| {
                committee
                    .certificate(&a.header_builder(&committee.committee()).created_at(1).build())
            })
            .collect();
        parents.push(own_cert.clone());

        let header = author
            .header_builder(&committee.committee())
            .round(2)
            .parents(parents.iter().map(|c| c.digest()).collect())
            .latest_execution_block(exec)
            .created_at(2)
            .own_parent_signers(Some(claim_of(&own_cert)))
            .build();

        // nothing stored: the first request comes back asking for the parents
        let missing = match handler.vote(peer, header.clone(), Vec::new()).await {
            Ok(PrimaryResponse::MissingParents(missing)) => missing,
            other => panic!("expected MissingParents, got {other:?}"),
        };
        assert_eq!(missing.len(), parents.len());
        // the retry ships them: verified, stored, and the claim is compared with that copy
        handler.vote(peer, header, parents).await
    };

    let res = vote(OwnParentSigners::from_certificate).await;
    assert!(matches!(res, Ok(PrimaryResponse::Vote(_))), "{res:?}");
    // an empty claim while the shipped parent is the author's own: nothing verifies it
    let res = vote(|_| OwnParentSigners::empty()).await;
    assert!(
        matches!(
            res,
            Err(PrimaryNetworkError::InvalidHeader(HeaderError::OwnParentSignersInvalid { .. }))
        ),
        "{res:?}"
    );
    Ok(())
}

/// Before the fork a claim is refused, so no proposer can start the new layout early.
#[tokio::test]
async fn test_vote_fails_own_parent_signers_before_fork() -> eyre::Result<()> {
    let TestTypes { committee, handler, parent, task_manager: _task_manager, .. } =
        create_test_types();
    let header = committee
        .header_builder_last_authority()
        .latest_execution_block(BlockNumHash::new(parent.number(), parent.hash()))
        .created_at(1) // parent is 0
        .own_parent_signers(Some(OwnParentSigners::empty()))
        .build();
    let peer = *committee.last_authority().authority().protocol_key();

    let res = handler.vote(peer, header, Vec::new()).await;
    assert_matches!(
        res,
        Err(PrimaryNetworkError::InvalidHeader(HeaderError::UnexpectedOwnParentSigners))
    );
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

#[test]
fn fetch_scope_serves_current_previous_and_nothing_older() {
    // our epoch, or an older requester that does not say: the live tables
    assert_eq!(fetch_scope(Some(485), 485), Some(CertScope::Current));
    assert_eq!(fetch_scope(None, 485), Some(CertScope::Current));
    // the epoch we just closed: the kept tail
    assert_eq!(fetch_scope(Some(484), 485), Some(CertScope::PrevEpoch));
    // anything older, or ahead of us: nothing to serve
    assert_eq!(fetch_scope(Some(483), 485), None);
    assert_eq!(fetch_scope(Some(486), 485), None);
    // no overflow on a crafted epoch
    assert_eq!(fetch_scope(Some(u32::MAX), 0), None);
}

#[test]
fn missing_certificates_in_epoch_is_the_last_request_variant() {
    // BCS writes the variant index first. The epoch-stamped request must stay appended after every
    // older variant so a node that does not know it still decodes all of them; and an encoded
    // older variant must decode unchanged on a node that does.
    let old = PrimaryRequest::MissingCertificates { inner: MissingCertificatesRequest::default() };
    let new = PrimaryRequest::MissingCertificatesInEpoch {
        inner: MissingCertificatesRequest::default(),
        epoch: 484,
    };
    let old_bytes = encode(&old);
    let new_bytes = encode(&new);
    assert_eq!(old_bytes[0], 1, "MissingCertificates keeps its original variant index");
    assert_eq!(new_bytes[0], 5, "MissingCertificatesInEpoch is appended last");
    assert_eq!(rayls_infrastructure_types::decode::<PrimaryRequest>(&old_bytes), old);
    assert_eq!(rayls_infrastructure_types::decode::<PrimaryRequest>(&new_bytes), new);
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

/// Decoding keeps a bounded number of skip-round entries, and counts every authority named.
///
/// A long list then reaches the handler, which refuses it with the full count.
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
                bytes[..size_of::<u64>()].copy_from_slice(&(i as u64).to_le_bytes());
                (AuthorityIdentifier::from(bytes), vec![])
            })
            .collect(),
        ..Default::default()
    };
    // Decode a request, and return how many entries it kept and how many authorities it named.
    let decode = |bytes: &[u8]| {
        let decoded: MissingCertificatesRequest = try_decode(bytes).expect("request decodes");
        (decoded.skip_rounds.kept(), decoded.skip_rounds.named())
    };

    let at_limit = encode(&request(MAX_SKIP_ROUND_AUTHORITIES));
    assert_eq!(decode(&at_limit), (MAX_SKIP_ROUND_AUTHORITIES, MAX_SKIP_ROUND_AUTHORITIES));

    let over = encode(&request(MAX_SKIP_ROUND_AUTHORITIES + 1));
    assert_eq!(decode(&over), (MAX_SKIP_ROUND_AUTHORITIES, MAX_SKIP_ROUND_AUTHORITIES + 1));

    // The audit's request fits under the message limit, and decoding keeps only the bound.
    let attack = encode(&request(AUDIT_AUTHORITIES));
    assert!(attack.len() < LibP2pConfig::default().max_rpc_message_size);
    assert_eq!(decode(&attack), (MAX_SKIP_ROUND_AUTHORITIES, AUDIT_AUTHORITIES));

    // A list that only declares the largest length bcs accepts is not decoded.
    let empty = encode(&request(0));
    // the list length follows the lower bound
    let at = encode(&request(0).exclusive_lower_bound).len();
    assert_eq!(empty[at..at + 1], uleb128(0)[..]);
    let mut declared = empty[..at].to_vec();
    declared.extend(uleb128(bcs::MAX_SEQUENCE_LENGTH));
    declared.extend_from_slice(&empty[at + 1..]);
    assert!(try_decode::<MissingCertificatesRequest>(&declared).is_err());
}

/// The handler's limit on named authorities never passes what decoding keeps.
///
/// Otherwise the handler could accept a list whose extra entries decoding dropped.
#[test]
fn test_requested_authorities_stay_within_the_decoding_limit() {
    use super::{
        handler::{max_requested_authorities, REQUESTED_AUTHORITIES_PER_MEMBER},
        message::MAX_SKIP_ROUND_AUTHORITIES,
    };

    // A committee of this size reaches the decoding limit through the per-member limit.
    let largest_unclamped = MAX_SKIP_ROUND_AUTHORITIES / REQUESTED_AUTHORITIES_PER_MEMBER;
    assert_eq!(max_requested_authorities(largest_unclamped), MAX_SKIP_ROUND_AUTHORITIES);
    assert_eq!(max_requested_authorities(largest_unclamped + 1), MAX_SKIP_ROUND_AUTHORITIES);
    // An honest request names the whole committee, so committees up to the limit still fit.
    for size in [1, largest_unclamped, MAX_SKIP_ROUND_AUTHORITIES] {
        assert!(max_requested_authorities(size) >= size, "a committee of {size} does not fit");
    }
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
    assert!(runs > 0, "a container needs at least one run");
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
    // The server decodes a skip bitmap only for committee members, so the origin must be one.
    let origin = fixture.authorities().next().unwrap().id();
    let max_response_size = LibP2pConfig::default().max_rpc_message_size;
    let request = |bitmap: Vec<u8>| MissingCertificatesRequest {
        skip_rounds: [(origin.clone(), bitmap)].into_iter().collect(),
        max_response_size,
        ..Default::default()
    };
    let refusal = |bitmap: Vec<u8>| {
        CertificateCollector::new(request(bitmap), config.clone(), CertScope::Current).err()
    };
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
            [(origin.clone(), (lower_bound + 1..=lower_bound + max_rounds).collect())]
                .into_iter()
                .collect(),
        )
        .expect("bounds")
        .set_max_response_size(max_response_size);
    assert!(CertificateCollector::new(honest, config.clone(), CertScope::Current).is_ok());
}

/// The server decodes a skip bitmap only for an origin it serves.
///
/// That is a member of the committee of the epoch the request is served from.
/// Without a record of the previous committee, the previous epoch's tail keeps every origin.
#[test]
fn test_skip_bitmap_decoded_only_for_served_origins() {
    use crate::state_sync::CertificateCollector;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_storage::EpochStore;
    use rayls_infrastructure_types::{BlsKeypair, EpochRecord};

    let max_response_size = LibP2pConfig::default().max_rpc_message_size;
    // An unknown header fails to decode, so it shows whether a decode was attempted at all.
    let bad_bitmap = vec![1u8, 2, 3];
    let request = |origin: &AuthorityIdentifier| MissingCertificatesRequest {
        skip_rounds: [(origin.clone(), bad_bitmap.clone())].into_iter().collect(),
        max_response_size,
        ..Default::default()
    };
    // A decode was attempted when the bad bitmap is refused, and skipped when the request passes.
    let decoded = |origin: &AuthorityIdentifier, fixture: &CommitteeFixture<MemDatabase>, scope| {
        let config = fixture.authorities().next().unwrap().consensus_config();
        match CertificateCollector::new(request(origin), config, scope) {
            Err(PrimaryNetworkError::StdIo(_)) => true,
            Ok(_) => false,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    };

    // Epoch 1, with a record of epoch 0 whose committee had one member that has since left.
    let departed = *BlsKeypair::generate(&mut StdRng::seed_from_u64(7)).public();
    let record = EpochRecord { epoch: 0, committee: vec![departed], ..Default::default() };
    let fixture = CommitteeFixture::builder(move || {
        let db = MemDatabase::default();
        db.save_epoch_record(&record).expect("previous epoch record");
        db
    })
    .epoch(1)
    .build();
    let member = fixture.authorities().next().unwrap().id();
    let departed = AuthorityIdentifier::from(departed);
    let outsider = AuthorityIdentifier::dummy_for_test(200);

    // The current epoch decodes only for current members.
    assert!(decoded(&member, &fixture, CertScope::Current));
    assert!(!decoded(&departed, &fixture, CertScope::Current));
    assert!(!decoded(&outsider, &fixture, CertScope::Current));

    // The previous epoch decodes only for its own members.
    assert!(decoded(&departed, &fixture, CertScope::PrevEpoch));
    assert!(!decoded(&member, &fixture, CertScope::PrevEpoch));
    assert!(!decoded(&outsider, &fixture, CertScope::PrevEpoch));

    // With no record of the previous committee, the previous epoch decodes for every origin.
    let unknown = CommitteeFixture::builder(MemDatabase::default).epoch(1).build();
    assert!(decoded(&outsider, &unknown, CertScope::PrevEpoch));
    assert!(!decoded(&outsider, &unknown, CertScope::Current));
}
