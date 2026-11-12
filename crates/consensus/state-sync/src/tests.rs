use crate::{
    attempt_catch_up, catch_up_consensus_from_to, certified_consensus_checkpoint,
    consensus::WalkSession, consensus_chain_tip, get_missing_consensus, highest_executed_anchor,
    prime_consensus, save_consensus, store_consensus_header_in_cache, stream_missing_consensus,
    verified::VerifiedHeaders,
};
use rayls_consensus_primary::{
    network::{
        NetworkCommand, NetworkError, PrimaryNetworkHandle, PrimaryRequest, PrimaryResponse,
    },
    ConsensusBus, WaitForExecutionError,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache},
    ConsensusStore, EpochStore,
};
use rayls_infrastructure_types::{
    Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, Database, DbTxMut, Epoch,
    EpochCertificate, EpochRecord, ExecHeader, RaylsReceiver as _, RaylsSender, ReputationScores,
    SealedHeader, B256,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// A network handle that answers `ConsensusHeader` requests like a peer: by number from
/// `by_number`, by hash from `honest`. Returns the handle and a count of by-number requests.
fn peer_network(
    by_number: BTreeMap<u64, ConsensusHeader>,
    honest: &[ConsensusHeader],
) -> (PrimaryNetworkHandle, Arc<AtomicUsize>) {
    let by_hash: BTreeMap<B256, ConsensusHeader> =
        honest.iter().map(|h| (h.digest(), h.clone())).collect();
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
            let PrimaryRequest::ConsensusHeader { number, hash } = request else { continue };
            let header = match (hash, number) {
                (Some(hash), _) => by_hash.get(&hash),
                (None, Some(number)) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    by_number.get(&number)
                }
                (None, None) => None,
            };
            let response = match header {
                Some(header) => Ok(PrimaryResponse::ConsensusHeader(Arc::new(header.clone()))),
                None => Err(NetworkError::RPCError("no such header".to_string())),
            };
            let _ = reply.send(response);
        }
    });
    (PrimaryNetworkHandle::new_for_test(tx), requests)
}

/// Runs the backwards walk from `target` down to the stored anchor against `network`.
async fn walk_to(
    config: &ConsensusConfig<MemDatabase>,
    cb: &ConsensusBus,
    network: PrimaryNetworkHandle,
    target: &ConsensusHeader,
) {
    let mut session = WalkSession::new(config.clone(), cb.clone(), network);
    assert!(session.sync_to(target.digest()).await.is_continue(), "the walk must not shut down");
}

/// Asserts `header` was never streamed for execution, draining what was.
fn assert_never_streamed(
    rx: &mut impl rayls_infrastructure_types::RaylsReceiver<
        rayls_infrastructure_types::StreamedConsensusHeader,
    >,
    header: &ConsensusHeader,
) {
    while let Ok(streamed) = rx.try_recv() {
        assert_ne!(
            streamed.digest(),
            header.digest(),
            "a header the walk did not store was streamed"
        );
    }
}

fn create_consensus_header_at_number(
    number: u64,
    committee: &rayls_infrastructure_types::Committee,
) -> ConsensusHeader {
    chained_header(number, B256::default(), committee)
}

fn sealed_header_with(nonce: u64, anchor: B256) -> SealedHeader {
    let exec_header = ExecHeader {
        nonce: nonce.into(),
        parent_beacon_block_root: Some(anchor),
        ..Default::default()
    };
    SealedHeader::new(exec_header, B256::default())
}

#[test]
fn highest_executed_anchor_selects_max_nonce() {
    // empty window
    assert_eq!(highest_executed_anchor(&[]), None);

    // single header
    let only = B256::repeat_byte(0x11);
    assert_eq!(highest_executed_anchor(&[sealed_header_with(5, only)]), Some(only));

    // max nonce is NOT the last element: mirrors a drained parked batch making the tip
    // (last, lower nonce) anchor to an older output than an earlier higher-nonce block.
    let highest = B256::repeat_byte(0xaa);
    let tip = B256::repeat_byte(0xbb);
    let window = [sealed_header_with(10, highest), sealed_header_with(7, tip)];
    assert_eq!(highest_executed_anchor(&window), Some(highest));

    // tie on max nonce: equal nonces share the same output, hence the same anchor.
    let shared = B256::repeat_byte(0xcc);
    let tied = [sealed_header_with(9, shared), sealed_header_with(9, shared)];
    assert_eq!(highest_executed_anchor(&tied), Some(shared));
}

#[tokio::test]
async fn test_prime_consensus_recovers_committed_round_via_primary_path() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();
    let header = create_consensus_header_at_number(1000, &committee);
    let header_digest = header.digest();

    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, &header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header_digest, &header.number)?;
        Ok(())
    })
    .unwrap();

    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(header.clone());

    prime_consensus(&cb, &config);

    let committed_round: u32 = *cb.committed_round_updates().borrow();
    assert!(committed_round > 0, "committed_round not recovered via primary path");
}

/// `prime_consensus` derives its watermark from the SSOT `executed_anchor` channel alone,
/// independent of `recently_executed_blocks` and the producer/engine. Seeding the anchor at number
/// N must drive committed_round off N (block-number fallback for the genesis-subdag header).
#[tokio::test]
async fn test_prime_consensus_reads_executed_anchor_ssot() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let committee = fixture.committee();

    let anchor_number = 4242u64;
    let header = create_consensus_header_at_number(anchor_number, &committee);

    // Seed the SSOT only - no recently_executed_blocks, no DB rows, no engine.
    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(header);

    prime_consensus(&cb, &config);

    let committed_round: u32 = *cb.committed_round_updates().borrow();
    assert_eq!(
        committed_round, anchor_number as u32,
        "prime_consensus must derive committed_round from the SSOT anchor number"
    );
    assert_eq!(*cb.primary_round_updates().borrow(), anchor_number as u32);
}

#[tokio::test]
async fn test_max_round_not_equal_to_gc_depth_after_restart() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let gc_depth = config.parameters().gc_depth;
    let committee = fixture.committee();
    let header = create_consensus_header_at_number(10000, &committee);
    let header_digest = header.digest();

    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, &header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header_digest, &header.number)?;
        Ok(())
    })
    .unwrap();

    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(header.clone());

    prime_consensus(&cb, &config);

    let committed_round: u32 = *cb.committed_round_updates().borrow();
    let max_round: u32 = committed_round + gc_depth;

    assert!(max_round > gc_depth * 2, "max_round too close to gc_depth");
}

/// Verify that `stream_missing_consensus` returns the correct handoff point so
/// `spawn_stream_consensus_headers` starts without overlap.
///
/// Setup: DB has headers 1..=10, execution is at block 8 (via the SSOT `executed_anchor`).
/// Expected: stream_missing sends headers [9, 10] and returns 10.
#[tokio::test]
async fn test_stream_missing_consensus_returns_correct_handoff() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Pre-populate ConsensusBlocks table with headers 1..=10
    let mut headers = Vec::new();
    for i in 1..=10u64 {
        let header = create_consensus_header_at_number(i, &committee);
        headers.push(header);
    }
    db.with_write_txn(|txn| {
        for header in &headers {
            txn.insert::<ConsensusBlocks>(&header.number, header)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        }
        Ok(())
    })
    .unwrap();

    // Seed the SSOT `executed_anchor` to reflect execution at block 8.
    let header_8 = headers[7].clone(); // index 7 = number 8

    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(header_8);

    // Subscribe to consensus_header broadcast BEFORE calling the function
    let mut rx = cb.consensus_header().subscribe();

    // Call stream_missing_consensus
    let last_streamed = stream_missing_consensus(&config, &cb).await.unwrap();

    // Assert: return value is 10 (the last DB block) - the handoff point
    assert_eq!(last_streamed, 10, "handoff should be last DB block number");

    // Collect all headers sent through the broadcast channel
    let mut received_numbers = Vec::new();
    while let Ok(header) = rx.try_recv() {
        received_numbers.push(header.number);
    }

    // Assert: received exactly [9, 10] - the gap between executed (8) and last DB (10)
    assert_eq!(
        received_numbers,
        vec![9, 10],
        "should stream only the missing headers (9 and 10), no duplicates"
    );
}

/// Build a saved consensus header at `number` whose subdag was committed by `leader_epoch`,
/// with a leader commit timestamp of `committed_at`.
fn header_for_epoch(number: u64, leader_epoch: Epoch, committed_at: u64) -> ConsensusHeader {
    let mut leader = Certificate::default();
    leader.header.epoch = leader_epoch;
    leader.header.created_at = committed_at;
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::default(), sub_dag, number, extra: B256::default() }
}

/// Regression for the epoch-boundary leak fork: a consensus output committed by the *previous*
/// epoch but saved above the execution anchor (the drain-race leak) must not be replayed on the
/// next epoch's startup, or the node re-executes past the block it signed in the EpochRecord.
#[tokio::test]
async fn test_get_missing_consensus_skips_prior_epoch_leak() {
    // Current epoch is 1; the closed epoch is 0.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // The leak: an output committed by epoch 0 (leader_epoch 0 < current epoch 1), saved at
    // consensus block 101 - one past the boundary the node executed and signed.
    let leak = header_for_epoch(101, 0, 5_000);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&leak.number, &leak)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&leak.digest(), &leak.number)?;
        Ok(())
    })
    .unwrap();

    // Execution anchor at consensus block 100 (the epoch-0 boundary, already executed).
    let anchor = create_consensus_header_at_number(100, &committee);
    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(anchor);

    let missing = get_missing_consensus(&config, &cb).await.unwrap();
    let numbers: Vec<u64> = missing.iter().map(|h| h.number).collect();
    assert!(
        !numbers.contains(&101),
        "post-boundary leak from a closed epoch must not be replayed, got {numbers:?}"
    );
}

/// Defense-in-depth cross-check for the replay guard: the certified-checkpoint resolver
/// requires a certificate (an uncertified local record is not trusted) and resolves the
/// closed epoch's `parent_consensus` to its consensus block number.
#[tokio::test]
async fn test_certified_consensus_checkpoint_requires_cert() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Fresh node: no prior epoch record -> guard inert, so first-epoch catch-up is unaffected.
    assert_eq!(certified_consensus_checkpoint(db, 1), None);

    // The signed last consensus block of epoch 0 lives at consensus number 100.
    let boundary = create_consensus_header_at_number(100, &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&boundary.number, &boundary)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&boundary.digest(), &boundary.number)?;
        Ok(())
    })
    .unwrap();

    let record =
        EpochRecord { epoch: 0, parent_consensus: boundary.digest(), ..Default::default() };

    // An uncertified local record must not anchor the guard.
    db.save_epoch_record(&record).unwrap();
    assert_eq!(
        certified_consensus_checkpoint(db, 1),
        None,
        "uncertified record must not be trusted as a checkpoint"
    );

    // With a certificate, parent_consensus resolves to the signed checkpoint number.
    let vote = record.sign_vote(config.key_config());
    let mut signed_authorities = roaring::RoaringBitmap::new();
    signed_authorities.push(0);
    let cert = EpochCertificate {
        epoch_hash: record.digest(),
        signature: vote.signature,
        signed_authorities,
    };
    db.save_epoch_record_with_cert(&record, &cert).unwrap();
    assert_eq!(certified_consensus_checkpoint(db, 1), Some(100));
}

/// Regression for the epoch-boundary numbering reseed fork.
///
/// A post-boundary leak (a prior-epoch output saved above the certified checkpoint, committed
/// but never executed) must not seed the next epoch's header numbering.
/// `consensus_chain_tip` feeds the live subscriber's number counter (`number = last_number
/// + 1`); if it returns the leaked tip, the first output of the new epoch is numbered
/// `leak_tip + 1` instead of `certified_checkpoint + 1`, so every header diverges from the
/// network by the leak count and the chain forks via a divergent `ConsensusHeader` digest
/// (which feeds `mix_hash` / `parent_beacon_block_root`).
#[tokio::test]
async fn test_numbering_seed_excludes_post_boundary_leak() {
    // Current epoch is 1; the closed, certified epoch is 0.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // The certified epoch-0 boundary: last consensus block of epoch 0 at number 100.
    let boundary = create_consensus_header_at_number(100, &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&boundary.number, &boundary)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&boundary.digest(), &boundary.number)?;
        Ok(())
    })
    .unwrap();

    // Certify it: a signed EpochRecord whose parent_consensus resolves to number 100.
    let record =
        EpochRecord { epoch: 0, parent_consensus: boundary.digest(), ..Default::default() };
    let vote = record.sign_vote(config.key_config());
    let mut signed_authorities = roaring::RoaringBitmap::new();
    signed_authorities.push(0);
    let cert = EpochCertificate {
        epoch_hash: record.digest(),
        signature: vote.signature,
        signed_authorities,
    };
    db.save_epoch_record_with_cert(&record, &cert).unwrap();
    assert_eq!(
        certified_consensus_checkpoint(db, 1),
        Some(100),
        "precondition: epoch-0 checkpoint must certify at number 100"
    );

    // The leak: three epoch-0 outputs (leader_epoch 0 < current epoch 1) saved past the
    // boundary at 101/102/103, committed but never executed (the drain-race leak).
    for number in 101..=103u64 {
        let leak = header_for_epoch(number, 0, 5_000 + number);
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&leak.number, &leak)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&leak.digest(), &leak.number)?;
            Ok(())
        })
        .unwrap();
    }

    // The numbering seed must anchor to the certified checkpoint (100), so the new epoch's
    // first output is numbered 101 - matching the network. Before the fix it returned the
    // leaked tip (103), so the first output was numbered 104: a +3 numbering fork.
    let seed = consensus_chain_tip(&config)
        .expect("consensus_chain_tip must return a seed when the DB is non-empty");
    assert_eq!(
        seed.number, 100,
        "numbering seed must exclude the post-boundary leak and anchor to the certified \
             checkpoint (100), got {} (the leaked tip)",
        seed.number
    );
    assert_eq!(
        seed.number + 1,
        101,
        "first output of the new epoch must be certified_checkpoint + 1 (101), not leak_tip + 1"
    );
}

/// Verify that when executed == last_db, nothing is streamed and the handoff is correct.
#[tokio::test]
async fn test_stream_missing_consensus_no_gap() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();

    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Pre-populate ConsensusBlocks table with headers 1..=10
    let mut headers = Vec::new();
    for i in 1..=10u64 {
        let header = create_consensus_header_at_number(i, &committee);
        headers.push(header);
    }
    db.with_write_txn(|txn| {
        for header in &headers {
            txn.insert::<ConsensusBlocks>(&header.number, header)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        }
        Ok(())
    })
    .unwrap();

    // Seed the SSOT `executed_anchor` reflecting execution at block 10 (fully caught up)
    let header_10 = headers[9].clone(); // index 9 = number 10

    let cb = ConsensusBus::new();
    cb.executed_anchor().send_replace(header_10);

    // Subscribe to consensus_header broadcast BEFORE calling the function
    let mut rx = cb.consensus_header().subscribe();

    let last_streamed = stream_missing_consensus(&config, &cb).await.unwrap();

    // Assert: return value is 10 and nothing was streamed
    assert_eq!(last_streamed, 10, "handoff should be last executed block number");

    // Nothing should have been sent
    let result = rx.try_recv();
    assert!(result.is_err(), "no headers should be streamed when execution is caught up");
}

/// Build a consensus header at `number` chained onto `parent_hash`. The digest commits the
/// parent link, so two headers at the same number with different parents have distinct digests.
fn chained_header(
    number: u64,
    parent_hash: B256,
    committee: &rayls_infrastructure_types::Committee,
) -> ConsensusHeader {
    let genesis_certs = Certificate::genesis(committee);
    let leader_cert = genesis_certs.first().unwrap().clone();
    let sub_dag = CommittedSubDag::new(vec![], leader_cert, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
}

/// Like [`chained_header`] but with a real quorum-signed `leader`, so the by-number BLS check in
/// catch-up accepts it. One signed leader is reused across a chain: only `parent_hash` and `number`
/// vary the digest, which is all the digest-chain guard checks.
fn signed_chained_header(number: u64, parent_hash: B256, leader: &Certificate) -> ConsensusHeader {
    let sub_dag =
        CommittedSubDag::new(vec![], leader.clone(), 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
}

/// Push one recent execution block above genesis so `wait_for_execution` treats the genesis-
/// subdag base block (number 0) as already executed and returns immediately.
fn seed_recently_executed_blocks(cb: &ConsensusBus) {
    let exec =
        SealedHeader::new(ExecHeader { number: 1, ..Default::default() }, B256::repeat_byte(0x01));
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(exec));
}

/// The streamer never asks a peer for a header by number: such a reply proves only its number, and
/// any peer can make one up. With the gap not stored yet it returns at the anchor without streaming
/// anything, and once the backwards walk has stored the gap it streams all of it.
#[tokio::test]
async fn test_catch_up_streams_only_stored_rows() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let (n, m) = (100u64, 105u64);
    let anchor = chained_header(n, B256::default(), &committee);
    store_executed(db, &anchor);
    let mut chain = Vec::new();
    let mut parent = anchor.digest();
    for number in n + 1..=m {
        let header = chained_header(number, parent, &committee);
        parent = header.digest();
        chain.push(header);
    }
    let target = chain.last().unwrap().clone();

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let first = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor.clone(),
        target.clone(),
        &mut VerifiedHeaders::default(),
    )
    .await
    .expect("a gap the walk has not stored yet is not an error");
    assert_eq!(first.number, n, "catch-up waits at the anchor for the walk");
    assert!(rx.try_recv().is_err(), "nothing may stream before the walk stores the gap");

    // The walk stores the gap below the target.
    for header in &chain[..chain.len() - 1] {
        store_consensus_header_in_cache(db, header);
    }
    let second =
        catch_up_consensus_from_to(&config, &cb, anchor, target, &mut VerifiedHeaders::default())
            .await
            .expect("catch-up streams the gap once the walk has stored it");
    assert_eq!(second.number, m);
    let mut streamed = Vec::new();
    while let Ok(header) = rx.try_recv() {
        streamed.push(header.number);
    }
    assert_eq!(streamed, (n + 1..=m).collect::<Vec<_>>(), "every gap header must be streamed");
}

/// Regression: a digest mismatch during catch-up must NOT delete a canonical `ConsensusBlocks`
/// entry. A mismatch on a committed row means a fork (needs a loud error and an operator restore),
/// not silently un-committing executed history the backwards walk cannot rebuild.
#[tokio::test]
async fn test_catch_up_mismatch_preserves_canonical_block() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Correct canonical anchor at 100.
    let anchor = chained_header(100, B256::default(), &committee);
    // A canonical row at 101 that does NOT chain onto the anchor (parent != anchor digest):
    // mirrors a regressed anchor re-examining an already-committed row on a divergent chain.
    let forked_canonical = chained_header(101, B256::repeat_byte(0xCC), &committee);
    assert_ne!(forked_canonical.parent_hash, anchor.digest(), "precondition: 101 does not link");
    let target = chained_header(102, forked_canonical.digest(), &committee);

    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&anchor.number, &anchor)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&anchor.digest(), &anchor.number)?;
        txn.insert::<ConsensusBlocks>(&forked_canonical.number, &forked_canonical)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(
            &forked_canonical.digest(),
            &forked_canonical.number,
        )?;
        Ok(())
    })
    .unwrap();

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);

    let result = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor.clone(),
        target.clone(),
        &mut VerifiedHeaders::default(),
    )
    .await;
    assert!(result.is_err(), "a digest mismatch must surface an error");

    // The committed block at 101 must still be there: catch-up must not delete canonical state.
    assert!(
        db.get::<ConsensusBlocks>(&forked_canonical.number).unwrap().is_some(),
        "catch-up deleted a canonical ConsensusBlocks entry on a digest mismatch"
    );
}

/// Regression: a stale cache row (another fork's header at that number, with the right parent
/// link) that does not link to the next header must be cleared on the mismatch. Otherwise every
/// retry re-reads it and stalls forward catch-up. Once the walk stores the real row, catch-up
/// reaches the target.
#[tokio::test]
async fn test_catch_up_drops_poisoned_cache_row_and_recovers() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let real_101 = chained_header(101, anchor.digest(), &committee);
    let target = chained_header(102, real_101.digest(), &committee);
    let stale_101 = fabricated_header(101, anchor.digest());
    assert_ne!(stale_101.digest(), real_101.digest(), "precondition: another fork's row");
    store_consensus_header_in_cache(db, &stale_101);

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let _rx = cb.consensus_header().subscribe();

    let first = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor.clone(),
        target.clone(),
        &mut VerifiedHeaders::default(),
    )
    .await;
    assert!(first.is_err(), "the target does not link onto the stale row");
    assert!(
        db.get_consensus_by_number(101).is_none(),
        "the stale row must be cleared, or every retry re-reads it and stalls"
    );

    // The walk stores the real 101; the next pass reaches the target.
    store_consensus_header_in_cache(db, &real_101);
    let second =
        catch_up_consensus_from_to(&config, &cb, anchor, target, &mut VerifiedHeaders::default())
            .await
            .expect("catch-up recovers once the walk stores the real row");
    assert_eq!(second.number, 102, "catch-up must reach the target after recovering");
}

/// Regression for the epoch-boundary leak fork: the forward-streamer catch-up
/// (`catch_up_consensus_from_to`) must HALT when it reaches an output committed by a *closed* epoch
/// (`leader_epoch < current epoch`), not forward it for execution. Without this a lagging node
/// replays an epoch-tail straggler into the new epoch and forks (an observer executes a subdag the
/// committee dropped). The DB-replay path `collect_replayable_headers` already guards this; the
/// forward streamer did not, so a bombarded observer perpetually catching up hit the unguarded
/// path.
#[tokio::test]
async fn test_catch_up_from_to_halts_on_prior_epoch_leak() {
    // Current epoch is 1; the leak was committed by the closed epoch 0.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

    // Anchor at consensus number 100 (current epoch); the next output (101) is a closed-epoch leak
    // whose subdag was committed by epoch 0 (leader_epoch 0 < current epoch 1). Chain it onto the
    // anchor so it clears the digest-chain guard and would reach execution WITHOUT the epoch guard
    // -- the leak must be stopped by the epoch check, not incidentally by a chain mismatch.
    let anchor = header_for_epoch(100, 1, 5_000);
    let mut leak = header_for_epoch(101, 0, 6_000);
    leak.parent_hash = anchor.digest();
    // The epoch guard runs before the execution wait, so a leak halted correctly never waits; but
    // seed an executed base block so that WITHOUT the guard the leak would proceed to be streamed
    // (proving the test isolates the epoch check).
    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let result = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor.clone(),
        leak.clone(),
        &mut VerifiedHeaders::default(),
    )
    .await
    .expect("catch-up must return Ok (halt), not error, on a closed-epoch leak");

    // Halted at the leak: the anchor did not advance, and the leak was never streamed for
    // execution.
    assert_eq!(
        result.number, anchor.number,
        "catch-up must halt at the closed-epoch leak without advancing past the anchor"
    );
    assert!(
        rx.try_recv().is_err(),
        "a closed-epoch leak (leader_epoch < epoch) must not be streamed for execution"
    );
}

/// A closed-epoch leak stalled in the DB blocks the *legitimate* current-epoch headers stacked
/// above it: catch-up halts at the leak and refuses to stream 102..=105 even though those are valid
/// epoch-1 outputs. This is deliberate -- executing past an unresolved leak forks the chain -- and
/// the node self-heals once the backwards walk overwrites the leaked row. Documents that the guard
/// gates on the *first* leak in the range, not only on a leak that is itself the catch-up target.
#[tokio::test]
async fn test_catch_up_from_to_leak_blocks_later_valid_headers() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    // Anchor 100 (epoch 1); a closed-epoch leak at 101 (epoch 0); then legitimate epoch-1 headers
    // 102..=105 chained above it. All of 101..=105 are DB-resident, so catch-up reads them locally.
    let anchor = header_for_epoch(100, 1, 5_000);
    let mut leak = header_for_epoch(101, 0, 6_000);
    leak.parent_hash = anchor.digest();
    let mut chain = vec![leak.clone()];
    let mut parent = leak.digest();
    for number in 102..=105 {
        let mut h = header_for_epoch(number, 1, 6_000 + number);
        h.parent_hash = parent;
        parent = h.digest();
        chain.push(h);
    }
    let target = chain.last().unwrap().clone();
    db.with_write_txn(|txn| {
        for h in &chain {
            txn.insert::<ConsensusBlocks>(&h.number, h)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&h.digest(), &h.number)?;
        }
        Ok(())
    })
    .unwrap();

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let result = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor.clone(),
        target,
        &mut VerifiedHeaders::default(),
    )
    .await
    .expect("catch-up must return Ok (halt), not error, when a leak blocks later headers");

    assert_eq!(
        result.number, anchor.number,
        "catch-up must halt at the leak (101) and not advance to the valid tail 102..=105"
    );
    assert!(
        rx.try_recv().is_err(),
        "no header may be streamed: the leak at 101 blocks the valid 102..=105 above it"
    );
}

fn sealed_at(number: u64) -> SealedHeader {
    SealedHeader::new(ExecHeader { number, ..Default::default() }, B256::repeat_byte(number as u8))
}

/// Regression: `save_consensus` prunes the `ByDigest` entries it supersedes at a number, so the
/// append-only index cannot accumulate stale mappings that false-stop the walk. Runs on the
/// `LayeredDatabase`, whose `get` panics inside a write txn, so only it catches the read-in-write
/// panic class the separate `with_read_txn` guards against.
#[tokio::test]
async fn test_save_consensus_prunes_superseded_bydigest_on_layered_db() {
    use rayls_consensus_primary::test_utils::temp_dir;
    use rayls_infrastructure_storage::open_db;

    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();

    let dir = temp_dir();
    let db = open_db(dir.path());

    // A stale canonical and a stale cache header share `number`, each indexed in ByDigest.
    let number = 100u64;
    let stale_canonical = chained_header(number, B256::repeat_byte(0xAA), &committee);
    let stale_cache = chained_header(number, B256::repeat_byte(0xCC), &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&number, &stale_canonical)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&stale_canonical.digest(), &number)?;
        txn.insert::<ConsensusBlocksCache>(&number, &stale_cache)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&stale_cache.digest(), &number)?;
        Ok(())
    })
    .unwrap();

    // Commit a new header (distinct digest) at the same number. On the broken code this panics
    // inside save_consensus (LayeredDbTxMut::get); with the fix it completes and prunes.
    let new_header = chained_header(number, B256::repeat_byte(0xBB), &committee);
    let new_digest = new_header.digest();
    let output = ConsensusOutput {
        sub_dag: std::sync::Arc::new(new_header.sub_dag.clone()),
        parent_hash: new_header.parent_hash,
        number,
        extra: new_header.extra,
        ..Default::default()
    };
    save_consensus(&db, output, &None).unwrap();

    // The new header is canonical, and both superseded digests were pruned from the index.
    assert_eq!(db.get::<ConsensusBlocks>(&number).unwrap().map(|h| h.digest()), Some(new_digest));
    assert_eq!(
        db.get::<ConsensusBlockNumbersByDigest>(&stale_canonical.digest()).unwrap(),
        None,
        "superseded canonical digest must be pruned"
    );
    assert_eq!(
        db.get::<ConsensusBlockNumbersByDigest>(&stale_cache.digest()).unwrap(),
        None,
        "superseded cache digest must be pruned"
    );
}

/// Regression: a target that never executes trips the bounded wait instead of hanging forever,
/// returning `Stalled`. The outer guard timeout is far longer than the idle bound, so
/// `Ok(Err(Stalled))` proves the method itself returned (a stall), not the guard.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_wait_for_execution_bounded_trips_on_stall() {
    let cb = ConsensusBus::new();
    cb.recently_executed_blocks().send_modify(|b| b.push_latest(sealed_at(0)));

    let idle = std::time::Duration::from_secs(5);
    let target = sealed_at(5).num_hash(); // never produced

    let result = tokio::time::timeout(idle * 4, cb.wait_for_execution_bounded(target, idle)).await;

    assert!(
        matches!(result, Ok(Err(WaitForExecutionError::Stalled))),
        "stalled target must return Stalled after the idle bound, not hang (got {result:?})"
    );
}

/// Regression: a target whose number is reached but whose hash differs returns `Forked`,
/// distinct from a `Stalled` timeout, so catch-up can tell a real divergence from a slow chain.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_wait_for_execution_reports_fork_on_hash_mismatch() {
    let cb = ConsensusBus::new();
    // Execution produced block 5, but with a different hash than the target references.
    cb.recently_executed_blocks().send_modify(|b| b.push_latest(sealed_at(5)));

    let mut target = sealed_at(5).num_hash();
    target.hash = B256::repeat_byte(0xFF); // same number, divergent hash

    assert!(
        matches!(
            cb.wait_for_execution_bounded(target, std::time::Duration::from_secs(5)).await,
            Err(WaitForExecutionError::Forked)
        ),
        "a reached-but-divergent target must report Forked, not Stalled"
    );
}

/// Regression: a slow-but-progressing chain reaches the target and returns Ok, never tripping
/// the bound (each block resets the per-block window), so the bound does not kill a
/// correct-but-slow catch-up.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_wait_for_execution_bounded_tolerates_slow_progress() {
    let cb = ConsensusBus::new();
    cb.recently_executed_blocks().send_modify(|b| b.push_latest(sealed_at(0)));

    let idle = std::time::Duration::from_secs(5);
    let target = sealed_at(5).num_hash();

    // push one block per half-window: always inside the idle bound, so it never trips.
    let tx = cb.recently_executed_blocks().clone();
    tokio::spawn(async move {
        for number in 1..=5 {
            tokio::time::sleep(idle / 2).await;
            tx.send_modify(|b| b.push_latest(sealed_at(number)));
        }
    });

    assert!(
        cb.wait_for_execution_bounded(target, idle).await.is_ok(),
        "steady progress under the idle bound must return Ok"
    );
}

/// The four-validator fixture the catch-up tests below run on.
fn catch_up_fixture() -> CommitteeFixture<MemDatabase> {
    CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build()
}

/// Stores `header` as an executed consensus block, like the streamer's anchor.
fn store_executed<DB: Database>(db: &DB, header: &ConsensusHeader) {
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        Ok(())
    })
    .unwrap();
}

/// A header at `number` with a real parent link and a made-up sub-dag, as a bad peer serves it.
/// Its leader's `latest_execution_block` is the default, so the execution gate passes.
fn fabricated_header(number: u64, parent_hash: B256) -> ConsensusHeader {
    let mut fake_leader = Certificate::default();
    fake_leader.header.created_at = 123_456;
    let sub_dag = CommittedSubDag::new(vec![], fake_leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
}

/// Retries catch-up up to five times, as the streamer does, and returns the
/// target if it is still pending.
async fn retry_catch_up<DB: Database>(
    config: &ConsensusConfig<DB>,
    cb: &ConsensusBus,
    last_header: &mut ConsensusHeader,
    last_height: &mut u64,
    target: ConsensusHeader,
) -> Option<ConsensusHeader> {
    let mut pending = Some(target);
    for _ in 0..5 {
        let Some(t) = pending.clone() else { break };
        pending = attempt_catch_up(
            config,
            cb,
            last_header,
            last_height,
            t,
            "test retry",
            &mut VerifiedHeaders::default(),
        )
        .await;
    }
    pending
}

/// A peer can answer a by-number request with a header that has the real parent link and a made-up
/// sub_dag. On main the streamer fetched gaps by number, made such a reply its anchor, and then
/// failed the digest check on every retry. Now only the backwards walk asks by number, and it
/// stores a reply only once it hash-links to a trusted header and passes BLS. So the fake never
/// reaches the streamer, and the node reaches the target.
#[tokio::test]
async fn test_forward_streamer_does_not_stall_on_gap_fetch_fake() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let real_101 = signed_chained_header(101, anchor.digest(), &leader);
    let real_102 = signed_chained_header(102, real_101.digest(), &leader);
    let target = signed_chained_header(103, real_102.digest(), &leader);
    let fake_101 = fabricated_header(101, anchor.digest());
    assert_ne!(fake_101.digest(), real_101.digest(), "precondition: fake diverges");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    // The byzantine peer answers 101 by number with the fake and nothing else by number.
    let (byzantine, by_number) = peer_network(
        BTreeMap::from([(101u64, fake_101.clone())]),
        &[real_101, real_102, target.clone()],
    );
    walk_to(&config, &cb, byzantine, &target).await;
    assert!(by_number.load(Ordering::Relaxed) > 0, "precondition: the peer was asked by number");

    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;
    let pending =
        retry_catch_up(&config, &cb, &mut last_header, &mut last_height, target.clone()).await;
    assert_eq!(last_height, 103, "the node must reach the target");
    assert_eq!(last_header.digest(), target.digest());
    assert!(pending.is_none());
    assert_never_streamed(&mut rx, &fake_101);
}

/// Same attack, with the byzantine peer answering both numbers by number: N with a parent-linked
/// fake and N+1 with a header of a closed epoch. On main the epoch halt ran before the digest check
/// and made the fake the anchor. Now neither reply is stored, and the node reaches the target.
#[tokio::test]
async fn test_forward_streamer_does_not_stall_via_closed_epoch_reply_after_fake() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();

    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let anchor = header_for_epoch(100, 1, 5_000);
    store_executed(db, &anchor);
    let real_101 = signed_chained_header(101, anchor.digest(), &leader);
    let real_102 = signed_chained_header(102, real_101.digest(), &leader);
    let target = signed_chained_header(103, real_102.digest(), &leader);

    let mut fake_101 = header_for_epoch(101, 1, 999_999);
    fake_101.parent_hash = anchor.digest();
    let closed_epoch_102 = header_for_epoch(102, 0, 1);

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let (byzantine, by_number) = peer_network(
        BTreeMap::from([(101u64, fake_101.clone()), (102u64, closed_epoch_102.clone())]),
        &[real_101, real_102, target.clone()],
    );
    walk_to(&config, &cb, byzantine, &target).await;
    assert!(by_number.load(Ordering::Relaxed) > 0, "precondition: the peer was asked by number");

    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;
    let _ = retry_catch_up(&config, &cb, &mut last_header, &mut last_height, target).await;
    assert_eq!(last_height, 103, "the node must not stall on a fake by-number reply");
    assert_never_streamed(&mut rx, &fake_101);
    assert_never_streamed(&mut rx, &closed_epoch_102);
}

/// A by-number reply can carry a REAL, quorum-signed leader with a tampered field no signature
/// covers (commit_timestamp; the certificate list works too). It passes BLS, yet its digest differs
/// from the canonical header. On main the streamer forwarded it for execution. The walk stores it
/// only if it hash-links to the trusted header above, which it does not, so it is never forwarded.
#[tokio::test]
async fn test_gap_fetch_does_not_forward_bls_valid_tampered_subdag() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);

    // Real, quorum-signed leader certificate (3 of 4 votes).
    let signed_leader = fixture.certificate(&fixture.header_from_last_authority());
    let real_101 = signed_chained_header(101, anchor.digest(), &signed_leader);
    let target = signed_chained_header(102, real_101.digest(), &signed_leader);

    // Tamper only commit_timestamp (private, set via `previous_sub_dag`: max(prev, leader)).
    let later = header_for_epoch(0, 0, *signed_leader.header().created_at() + 1_000_000);
    let tampered_sub_dag = CommittedSubDag::new(
        vec![],
        signed_leader,
        0,
        ReputationScores::default(),
        Some(&later.sub_dag),
    );
    let tampered_101 = ConsensusHeader { sub_dag: tampered_sub_dag, ..real_101.clone() };
    assert_ne!(tampered_101.digest(), real_101.digest(), "precondition: non-canonical");
    // Precondition: the subscriber's own check accepts it.
    tampered_101
        .clone()
        .verify_header_with_keys(&committee.bls_keys())
        .expect("tampered header carries a valid quorum-signed leader");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let (byzantine, by_number) =
        peer_network(BTreeMap::from([(101u64, tampered_101.clone())]), &[real_101, target.clone()]);
    walk_to(&config, &cb, byzantine, &target).await;
    assert!(by_number.load(Ordering::Relaxed) > 0, "precondition: the peer was asked by number");

    let result = catch_up_consensus_from_to(
        &config,
        &cb,
        anchor,
        target.clone(),
        &mut VerifiedHeaders::default(),
    )
    .await
    .expect("catch-up reaches the target on the walk's rows");
    assert_eq!(result.digest(), target.digest());
    assert_never_streamed(&mut rx, &tampered_101);
}

/// The subscriber skips BLS only for a header flagged verified, so the flag must mean the walk in
/// this process verified exactly that header, with the current committee's keys. A stored row the
/// walk did not record, or with other signatures than the walk verified, stays unflagged.
#[tokio::test]
async fn test_catch_up_flags_only_headers_verified_in_this_process() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let leader_header = fixture.header_from_last_authority();
    let honest_leader = fixture.certificate(&leader_header);
    let one_vote: Vec<_> = fixture
        .votes(&leader_header)
        .into_iter()
        .take(1)
        .map(|v| (v.author().clone(), *v.signature()))
        .collect();
    let under_signed_leader =
        Certificate::new_unsigned_for_test(&committee, leader_header, one_vote).unwrap();
    let honest_101 = signed_chained_header(101, anchor.digest(), &honest_leader);
    let under_signed_101 = signed_chained_header(101, anchor.digest(), &under_signed_leader);
    let target = chained_header(102, honest_101.digest(), &committee);
    store_consensus_header_in_cache(db, &honest_101);
    let stored_101 = db.get_consensus_by_number(101).expect("101 is stored");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();
    let mut streamed = || {
        let mut flags = Vec::new();
        while let Ok(header) = rx.try_recv() {
            flags.push((header.number, header.is_verified()));
        }
        flags
    };
    let (walk, mut verified) = VerifiedHeaders::channel();

    // a stored row the walk did not record in this process, such as one from before a restart
    catch_up_consensus_from_to(&config, &cb, anchor.clone(), target.clone(), &mut verified)
        .await
        .unwrap();
    assert_eq!(streamed(), vec![(101, false), (102, false)]);

    // the walk verified the same header with other signatures
    walk.record(&under_signed_101);
    catch_up_consensus_from_to(&config, &cb, anchor.clone(), target.clone(), &mut verified)
        .await
        .unwrap();
    assert_eq!(streamed(), vec![(101, false), (102, false)], "other signatures than verified");

    // the walk verified exactly the stored row, and the target
    walk.record(&stored_101);
    walk.record(&target);
    catch_up_consensus_from_to(&config, &cb, anchor.clone(), target.clone(), &mut verified)
        .await
        .unwrap();
    assert_eq!(streamed(), vec![(101, true), (102, true)]);

    // a header of another epoch was checked with record or lookahead keys, not the ones the
    // subscriber uses, so it stays unflagged even when the walk verified those exact bytes
    let mut next_epoch = target;
    next_epoch.sub_dag.leader.header.epoch = config.epoch() + 1;
    walk.record(&stored_101);
    walk.record(&next_epoch);
    catch_up_consensus_from_to(&config, &cb, anchor.clone(), next_epoch, &mut verified)
        .await
        .unwrap();
    assert_eq!(streamed(), vec![(101, true), (102, false)], "next epoch's keys are not ours");
}

/// When both sides of a failed link are stored rows, the row below the link may be the bad one
/// (a stale row from another fork). The anchor must not advance onto it, so the row is cleared and
/// a later pass can replace it, even when headers below it were already streamed.
#[tokio::test]
async fn test_catch_up_does_not_anchor_on_stale_stored_row() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let mut chain: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    let mut parent = anchor.digest();
    for number in 101..=105u64 {
        let header = chained_header(number, parent, &committee);
        parent = header.digest();
        chain.insert(number, header);
    }
    let target = chain[&105].clone();

    // Stored rows 101..=104, with a stale row at 103: real parent link, different sub_dag.
    let stale_103 = fabricated_header(103, chain[&102].digest());
    for header in [&chain[&101], &chain[&102], &stale_103, &chain[&104]] {
        store_consensus_header_in_cache(db, header);
    }

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    let pending = attempt_catch_up(
        &config,
        &cb,
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test",
        &mut VerifiedHeaders::default(),
    )
    .await;
    assert!(pending.is_some());
    assert_eq!(
        last_height, 100,
        "the anchor must not advance onto a row the failed link implicates"
    );
    assert!(db.get_consensus_by_number(103).is_none(), "the stale row must be cleared");

    // A stored cache row is trusted transitively, so 101, 102 and the stale 103 are forwarded
    // before the mismatch at 104 is found. Pin that the stale row still reaches the consumer here,
    // where the real subscriber then rejects it on BLS. Holding cache rows until linked would close
    // this, tracked in #380.
    let mut forwarded = Vec::new();
    while let Ok(h) = rx.try_recv() {
        forwarded.push(h.digest());
    }
    assert!(
        forwarded.contains(&stale_103.digest()),
        "a stored cache row is forwarded transitively before the mismatch is found"
    );

    // The walk refills 103; the next pass reaches the target.
    store_consensus_header_in_cache(db, &chain[&103]);
    let pending = attempt_catch_up(
        &config,
        &cb,
        &mut last_header,
        &mut last_height,
        target,
        "test retry",
        &mut VerifiedHeaders::default(),
    )
    .await;
    assert!(pending.is_none());
    assert_eq!(last_height, 105);
}

/// A pass that stops at a header the walk has not stored yet leaves the anchor on the last row it
/// streamed, and that row can be stale (another fork's row with the real parent link). Once the
/// walk stores the real child, the next pass fails on the anchor's own stored child and must rewind
/// to the executed anchor, so the pass after the walk repairs the row reaches the target instead of
/// staying stuck until a restart.
#[tokio::test]
async fn test_catch_up_rewinds_stale_anchor() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let mut chain: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    let mut parent = anchor.digest();
    for number in 101..=105u64 {
        let header = chained_header(number, parent, &committee);
        parent = header.digest();
        chain.insert(number, header);
    }
    let target = chain[&105].clone();

    // Stored rows 101, 102 and a stale 103 with the real parent link; 104 is not stored yet.
    let stale_103 = fabricated_header(103, chain[&102].digest());
    for header in [&chain[&101], &chain[&102], &stale_103] {
        store_consensus_header_in_cache(db, header);
    }

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    cb.executed_anchor().send_replace(anchor.clone());
    let _rx = cb.consensus_header().subscribe();
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    assert!(
        attempt_catch_up(
            &config,
            &cb,
            &mut last_header,
            &mut last_height,
            target.clone(),
            "test",
            &mut VerifiedHeaders::default(),
        )
        .await
        .is_some(),
        "104 is not stored yet"
    );
    assert_eq!(last_height, 103, "the pass stops at 104 with the anchor on the stale 103");

    // The walk stores the real 104. The next pass fails on the anchor's own stored child and must
    // rewind.
    store_consensus_header_in_cache(db, &chain[&104]);
    assert!(attempt_catch_up(
        &config,
        &cb,
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test walk stored 104",
        &mut VerifiedHeaders::default(),
    )
    .await
    .is_some());
    assert_eq!(last_height, 100, "the stale anchor must rewind to the executed anchor");
    assert_eq!(last_header.digest(), anchor.digest());

    // The walk repairs 103; the next pass reaches the target.
    store_consensus_header_in_cache(db, &chain[&103]);
    assert!(
        attempt_catch_up(
            &config,
            &cb,
            &mut last_header,
            &mut last_height,
            target.clone(),
            "test repaired",
            &mut VerifiedHeaders::default(),
        )
        .await
        .is_none(),
        "the repaired row lets the pass finish"
    );
    assert_eq!(last_height, 105);
}
