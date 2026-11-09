use crate::{
    attempt_catch_up, catch_up_consensus_from_to, certified_consensus_checkpoint,
    consensus_chain_tip, get_missing_consensus, highest_executed_anchor, prime_consensus,
    save_consensus, store_consensus_header_in_cache, stream_missing_consensus, MAX_UNPROVEN_GAP,
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

/// A network handle with a dropped receiver: every fetch fails fast. Used where catch-up
/// must resolve everything from the local DB and must never block on a peer.
fn no_peer_network() -> PrimaryNetworkHandle {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    PrimaryNetworkHandle::new_for_test(tx)
}

/// A network handle backed by an in-memory `number -> header` map that answers
/// `SendRequestAny` `ConsensusHeader { number, .. }` requests like a peer's
/// `get_header_by_number`. A number absent from the map yields an `RPCError`.
fn serving_network(headers: BTreeMap<u64, ConsensusHeader>) -> PrimaryNetworkHandle {
    counting_serving_network(headers).0
}

/// [`serving_network`] that also counts the consensus-header requests it receives.
fn counting_serving_network(
    headers: BTreeMap<u64, ConsensusHeader>,
) -> (PrimaryNetworkHandle, Arc<AtomicUsize>) {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
            let PrimaryRequest::ConsensusHeader { number, .. } = request else { continue };
            counter.fetch_add(1, Ordering::Relaxed);
            let response = match number.and_then(|n| headers.get(&n)) {
                Some(header) => Ok(PrimaryResponse::ConsensusHeader(Arc::new(header.clone()))),
                None => Err(NetworkError::RPCError("no such header".to_string())),
            };
            let _ = reply.send(response);
        }
    });
    (PrimaryNetworkHandle::new_for_test(tx), requests)
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

/// Regression: a lagging node whose DB holds only header N must fill the gap `N+1..=M` by fetching
/// each missing header by number from a peer. The backwards walk is not exercised, so the only way
/// to pass is the by-number gap fill (an early return at the first DB miss would loop forever).
#[tokio::test]
async fn test_catch_up_fills_gap_by_number_when_db_is_empty() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Last executed at N=100, the only header committed locally.
    let n = 100u64;
    let m = 105u64;
    let anchor = chained_header(n, B256::default(), &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&anchor.number, &anchor)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&anchor.digest(), &anchor.number)?;
        Ok(())
    })
    .unwrap();

    // Each header links onto the prior digest, so the catch-up digest guard accepts them, and
    // carries a real quorum-signed leader so the by-number BLS check accepts them too.
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let mut served: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    let mut parent = anchor.digest();
    let mut target = anchor.clone();
    for number in n + 1..=m {
        let header = signed_chained_header(number, parent, &leader);
        parent = header.digest();
        served.insert(number, header.clone());
        target = header;
    }
    // The target M is passed directly to catch-up; the peer only serves the strictly
    // intermediate headers N+1..=M-1, the ones absent from the DB.
    let net = serving_network(served.clone());

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let result = catch_up_consensus_from_to(&config, &cb, &net, anchor.clone(), target.clone())
        .await
        .expect("catch-up must succeed by filling the gap from peers");

    // The full gap was streamed and the target reached.
    assert_eq!(result.number, m, "catch-up must reach the target M by filling the gap");

    let mut streamed_numbers = Vec::new();
    while let Ok(header) = rx.try_recv() {
        streamed_numbers.push(header.number);
    }
    assert_eq!(
        streamed_numbers,
        (n + 1..=m).collect::<Vec<_>>(),
        "every gap header N+1..=M must be streamed for execution"
    );

    // The intermediate gap headers were cached into the DB by the by-number fetch.
    for number in n + 1..m {
        assert!(
            db.get_consensus_by_number(number).is_some(),
            "gap header {number} must be cached after a by-number fetch"
        );
    }
}

/// Regression: a mis-chained by-number gap fetch must NOT be cached before the digest-chain guard
/// runs (the fetch only checks the number). Caching earlier poisons the row, so every retry
/// re-reads it and re-fails the guard, stalling catch-up at that number.
#[tokio::test]
async fn test_gap_fetch_does_not_cache_mischained_header() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let n = 100u64;
    let anchor = chained_header(n, B256::default(), &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&anchor.number, &anchor)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&anchor.digest(), &anchor.number)?;
        Ok(())
    })
    .unwrap();

    // The peer serves a header at N+1 that does NOT chain onto the anchor (its parent_hash is
    // unrelated), with a target at N+2 so catch-up has a gap to fill.
    let bad = chained_header(n + 1, B256::repeat_byte(0xAB), &committee);
    let target = chained_header(n + 2, bad.digest(), &committee);
    let mut served: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    served.insert(n + 1, bad.clone());
    let net = serving_network(served);

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);

    let result = catch_up_consensus_from_to(&config, &cb, &net, anchor.clone(), target).await;
    assert!(result.is_err(), "a mis-chained gap header must fail the digest-chain guard");

    // The cache row and its digest index are written together, so a clean cache row also
    // means a clean digest index.
    assert!(
        db.get_consensus_by_number(n + 1).is_none(),
        "a mis-chained gap header must not be cached before the guard validates it"
    );
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

    let net = no_peer_network();
    let result =
        catch_up_consensus_from_to(&config, &cb, &net, anchor.clone(), target.clone()).await;
    assert!(result.is_err(), "a digest mismatch must surface an error");

    // The committed block at 101 must still be there: catch-up must not delete canonical state.
    assert!(
        db.get::<ConsensusBlocks>(&forked_canonical.number).unwrap().is_some(),
        "catch-up deleted a canonical ConsensusBlocks entry on a digest mismatch"
    );
}

/// Regression: a byzantine gap fetch can return a header with correct parent_hash/number but a
/// fabricated sub_dag, which caches then fails to link to the real successor. Without dropping the
/// poisoned non-canonical row on mismatch, every retry re-reads it and stalls forward catch-up.
#[tokio::test]
async fn test_catch_up_drops_poisoned_gap_row_and_recovers() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Canonical anchor at 100 (the executed-anchor seed `from`).
    let anchor = chained_header(100, B256::default(), &committee);
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&anchor.number, &anchor)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&anchor.digest(), &anchor.number)?;
        Ok(())
    })
    .unwrap();

    // The honest header at 101 chains onto the anchor and carries a real quorum-signed leader; the
    // target at 102 chains onto it.
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let real_101 = signed_chained_header(101, anchor.digest(), &leader);
    let target = chained_header(102, real_101.digest(), &committee);

    // The byzantine header at 101 carries the correct parent_hash and number but a fabricated
    // sub_dag, so it passes its own guard yet its digest differs from `real_101`.
    let mut fake_leader = Certificate::default();
    fake_leader.header.created_at = 123_456;
    let fake_sub_dag =
        CommittedSubDag::new(vec![], fake_leader, 0, ReputationScores::default(), None);
    let bogus_101 = ConsensusHeader {
        parent_hash: anchor.digest(),
        sub_dag: fake_sub_dag,
        number: 101,
        extra: B256::default(),
    };
    assert_eq!(bogus_101.parent_hash, real_101.parent_hash, "precondition: same parent link");
    assert_ne!(bogus_101.digest(), real_101.digest(), "precondition: fabricated sub_dag diverges");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    // A live subscriber keeps the broadcast `send` from erroring while headers stream.
    let _rx = cb.consensus_header().subscribe();

    // Pass 1: the byzantine peer serves a fabricated sub_dag at 101. Its leader is not quorum-
    // signed, so the by-number BLS check rejects the reply and catch-up returns at the anchor
    // without caching it. The digest excludes signatures, so the reply still links by hash: only
    // the BLS check stops it.
    let byzantine = serving_network(BTreeMap::from([(101u64, bogus_101.clone())]));
    let first =
        catch_up_consensus_from_to(&config, &cb, &byzantine, anchor.clone(), target.clone())
            .await
            .expect("a BLS-invalid reply is dropped, not a hard error: catch-up awaits the walk");
    assert_eq!(
        first.number, 100,
        "catch-up must not advance past the anchor on a BLS-invalid reply"
    );

    // The rejected reply must never be cached, so the next retry re-fetches it; on the broken code
    // it stays cached and every retry re-reads it.
    assert!(
        db.get_consensus_by_number(101).is_none(),
        "a BLS-invalid gap reply must not be cached, or every retry re-reads it and stalls"
    );

    // Pass 2: an honest peer now serves the real 101; catch-up must reach the target. On the broken
    // code the stale bogus row is read from the DB instead, re-failing at 102 forever.
    let honest = serving_network(BTreeMap::from([(101u64, real_101.clone())]));
    let second = catch_up_consensus_from_to(&config, &cb, &honest, anchor, target.clone())
        .await
        .expect("catch-up must recover once the poisoned row is cleared and an honest peer serves");
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

    let result =
        catch_up_consensus_from_to(&config, &cb, &no_peer_network(), anchor.clone(), leak.clone())
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

    let result =
        catch_up_consensus_from_to(&config, &cb, &no_peer_network(), anchor.clone(), target)
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

/// Retries catch-up against `network` up to five times, as the streamer does, and returns the
/// target if it is still pending.
async fn retry_catch_up<DB: Database>(
    config: &ConsensusConfig<DB>,
    cb: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    last_header: &mut ConsensusHeader,
    last_height: &mut u64,
    target: ConsensusHeader,
) -> Option<ConsensusHeader> {
    let mut pending = Some(target);
    for _ in 0..5 {
        let Some(t) = pending.clone() else { break };
        pending =
            attempt_catch_up(config, cb, network, last_header, last_height, t, "test retry").await;
    }
    pending
}

/// A by-number gap fetch is checked only on number, so a byzantine peer can serve a header at N
/// with the real parent_hash and a fabricated sub_dag. If the next number then cannot be fetched,
/// the call returns Ok(fake) and `attempt_catch_up` promotes the fake to the in-memory anchor.
/// From then on every retry fails the digest guard at N+1, and the poison-clear guard skips N
/// because N is the anchor (`poisoned_parent_number > last_consensus_height` is false). The
/// streamer must recover once honest peers serve the real chain; on main it stays stalled at N.
#[tokio::test]
async fn test_forward_streamer_does_not_stall_on_gap_fetch_fake() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    // Canonical executed anchor at 100 and the honest chain 101 -> 102 -> 103, each with a real
    // quorum-signed leader so the by-number BLS check accepts the honest replies.
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    let real_101 = signed_chained_header(101, anchor.digest(), &leader);
    let real_102 = signed_chained_header(102, real_101.digest(), &leader);
    let target = signed_chained_header(103, real_102.digest(), &leader);

    // Fake 101: real parent link and number, fabricated sub_dag. Its leader's
    // latest_execution_block is the default (0, 0x0), below the seeded window, so the execution
    // gate passes.
    let fake_101 = fabricated_header(101, anchor.digest());
    assert_ne!(fake_101.digest(), real_101.digest(), "precondition: fake diverges");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    // Streamer state as `spawn_stream_consensus_headers` seeds it (lib.rs:507-508).
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    // Pass 1: the byzantine peer answers 101; nobody answers 102 (all 3 attempts fail).
    let byzantine = serving_network(BTreeMap::from([(101u64, fake_101.clone())]));
    let pending = attempt_catch_up(
        &config,
        &cb,
        &byzantine,
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test",
    )
    .await;
    assert!(pending.is_some(), "incomplete catch-up keeps the target pending");
    // On main the fake became the in-memory anchor here. It must stay held: never promoted to the
    // anchor and never forwarded, since nothing trusted has linked onto it.
    assert_eq!(last_height, 100, "an unproven by-number reply must not become the anchor");
    assert_eq!(last_header.digest(), anchor.digest());
    assert!(rx.try_recv().is_err(), "an unproven by-number reply must not be forwarded");

    // Passes 2..: honest peers now serve the real 101 and 102. A correct streamer recovers.
    let honest =
        serving_network(BTreeMap::from([(101u64, real_101.clone()), (102u64, real_102.clone())]));
    let pending =
        retry_catch_up(&config, &cb, &honest, &mut last_header, &mut last_height, target.clone())
            .await;
    // FAILS on main: last_height stays 101, last_header stays fake_101, pending stays Some.
    assert_eq!(
        last_height, 103,
        "forward streamer must recover from a gap-fetched fake once honest peers serve the chain"
    );
    assert_eq!(last_header.digest(), target.digest());
    assert!(pending.is_none());
}

/// Same stall, but the byzantine peer controls both gap fetches and no honest fetch has to fail:
/// it answers N with a parent-linked fake and N+1 with any header whose leader_epoch < epoch.
/// The epoch halt (lib.rs:703) runs before the digest guard and returns Ok(fake N).
#[tokio::test]
async fn test_forward_streamer_does_not_stall_via_closed_epoch_reply_after_fake() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .epoch(1)
        .build();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();

    // The honest 101..=103 carry a real quorum-signed epoch-1 leader so the by-number BLS check
    // accepts them on the recovery pass.
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let anchor = header_for_epoch(100, 1, 5_000);
    let real_101 = signed_chained_header(101, anchor.digest(), &leader);
    let real_102 = signed_chained_header(102, real_101.digest(), &leader);
    let target = signed_chained_header(103, real_102.digest(), &leader);

    let mut fake_101 = header_for_epoch(101, 1, 999_999);
    fake_101.parent_hash = anchor.digest();
    let closed_epoch_102 = header_for_epoch(102, 0, 1); // unchained; never digest-checked

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let _rx = cb.consensus_header().subscribe();
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    let byzantine =
        serving_network(BTreeMap::from([(101u64, fake_101.clone()), (102u64, closed_epoch_102)]));
    let _ = attempt_catch_up(
        &config,
        &cb,
        &byzantine,
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test",
    )
    .await;
    // On main the epoch halt promoted the fake here; it must stay held instead.
    assert_eq!(
        last_header.digest(),
        anchor.digest(),
        "fake must not be promoted (the by-number BLS check rejects it before the epoch halt)"
    );

    let honest = serving_network(BTreeMap::from([(101u64, real_101), (102u64, real_102)]));
    let _ = retry_catch_up(&config, &cb, &honest, &mut last_header, &mut last_height, target).await;
    // FAILS on main: stalled at 101.
    assert_eq!(last_height, 103, "streamer must not stay anchored on a fake gap-fetched header");
}

/// A gap-fetched header can carry a REAL, quorum-signed leader with a tampered, unsigned field
/// (commit_timestamp; the certificate list works too). It passes the streamer's digest guard,
/// the execution gate, and the subscriber's BLS re-verify (`verify_header_with_keys`), yet its
/// digest differs from the canonical header. The streamer forwards it before it ever checks N+1,
/// so the subscriber would execute it. A non-canonical header must never be forwarded.
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
    let real_sub_dag =
        CommittedSubDag::new(vec![], signed_leader.clone(), 0, ReputationScores::default(), None);
    let real_101 = ConsensusHeader {
        parent_hash: anchor.digest(),
        sub_dag: real_sub_dag,
        number: 101,
        extra: B256::default(),
    };
    let target = chained_header(102, real_101.digest(), &committee);

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
    // Precondition (holds on main): the subscriber's re-verify accepts it.
    tampered_101
        .clone()
        .verify_header_with_keys(&committee.bls_keys())
        .expect("tampered header carries a valid quorum-signed leader");

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    let byzantine = serving_network(BTreeMap::from([(101u64, tampered_101.clone())]));
    let _ = catch_up_consensus_from_to(&config, &cb, &byzantine, anchor, target).await;

    let mut forwarded = Vec::new();
    while let Ok(h) = rx.try_recv() {
        forwarded.push(h.digest());
    }
    // FAILS on main: tampered_101 was forwarded before the mismatch at 102 was seen.
    assert!(
        !forwarded.contains(&tampered_101.digest()),
        "a gap-fetched header not proven canonical must not be forwarded for execution"
    );
}

/// A by-number reply links by digest, and the digest excludes the certificate signatures. So a peer
/// can serve the real header at N with an under-signed leader: it has the same digest as the honest
/// header and links onto the anchor, yet fails BLS. It must be rejected before it is cached, or the
/// subscriber discards it downstream and every retry re-reads the stored row and stalls, across
/// restarts too.
#[tokio::test]
async fn test_gap_fetch_rejects_bls_invalid_reply() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);

    // One leader header, signed two ways: a full quorum (honest) and a single vote (under quorum).
    // Both carry the same leader header, so their consensus-header digests are identical; only BLS
    // verification tells them apart.
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
    let invalid_101 = signed_chained_header(101, anchor.digest(), &under_signed_leader);
    assert_eq!(
        invalid_101.digest(),
        honest_101.digest(),
        "precondition: same digest, differ only in signatures"
    );
    let target = chained_header(102, honest_101.digest(), &committee);

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    // Pass 1: the BLS-invalid reply must not be cached or forwarded, and catch-up must not pass it.
    let byzantine = serving_network(BTreeMap::from([(101u64, invalid_101.clone())]));
    let first =
        catch_up_consensus_from_to(&config, &cb, &byzantine, anchor.clone(), target.clone())
            .await
            .expect("a BLS-invalid reply is dropped, not a hard error");
    assert_eq!(
        first.number, 100,
        "catch-up must not advance past the anchor on a BLS-invalid reply"
    );
    assert!(db.get_consensus_by_number(101).is_none(), "a BLS-invalid reply must not be cached");
    assert!(rx.try_recv().is_err(), "a BLS-invalid reply must not be forwarded for execution");

    // Pass 2: an honest peer serves the quorum-signed 101, and catch-up reaches the target.
    let honest = serving_network(BTreeMap::from([(101u64, honest_101.clone())]));
    let second = catch_up_consensus_from_to(&config, &cb, &honest, anchor, target)
        .await
        .expect("catch-up recovers once an honest peer serves a BLS-valid reply");
    assert_eq!(second.number, 102, "catch-up must reach the target after recovery");
}

/// A by-number reply can only be proven by a trusted header linking onto it, so the streamer must
/// not fetch a gap with no trusted header within reach above it (it would hold replies it cannot
/// prove). Once the backwards walk caches a header within reach, the gap fills and streams.
#[tokio::test]
async fn test_catch_up_waits_for_walk_when_gap_out_of_reach() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let n = 100u64;
    let m = n + MAX_UNPROVEN_GAP + 2;
    let anchor = chained_header(n, B256::default(), &committee);
    store_executed(db, &anchor);
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let mut chain: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    let mut parent = anchor.digest();
    for number in n + 1..=m {
        let header = signed_chained_header(number, parent, &leader);
        parent = header.digest();
        chain.insert(number, header);
    }
    let target = chain[&m].clone();
    let (net, requests) = counting_serving_network(chain.clone());

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let mut rx = cb.consensus_header().subscribe();

    // Nothing trusted within reach of 101: return at the anchor without streaming anything.
    let first = catch_up_consensus_from_to(&config, &cb, &net, anchor.clone(), target.clone())
        .await
        .expect("an out-of-reach gap waits for the walk, it is not an error");
    assert_eq!(first.number, n);
    assert!(rx.try_recv().is_err(), "nothing may stream before a trusted header is in reach");
    assert!(db.get_consensus_by_number(n + 1).is_none(), "no unproven reply may be cached");
    assert_eq!(requests.load(Ordering::Relaxed), 0, "no by-number fetch for a gap it cannot prove");

    // The walk caches a header within reach; the gap below it is proven and the rest follows.
    store_consensus_header_in_cache(db, &chain[&(n + MAX_UNPROVEN_GAP / 2)]);
    let second = catch_up_consensus_from_to(&config, &cb, &net, anchor, target)
        .await
        .expect("catch-up must fill the gap once a trusted header is in reach");
    assert_eq!(second.number, m);
    let mut streamed = Vec::new();
    while let Ok(header) = rx.try_recv() {
        streamed.push(header.number);
    }
    assert_eq!(streamed, (n + 1..=m).collect::<Vec<_>>());
}

/// A bad by-number reply fails a link: either the trusted header above it does not link onto it,
/// or it does not link onto the stored row below it. The headers this call already proved and
/// streamed must stay as the new anchor, so a peer that answers some numbers with bad headers
/// cannot send every retry back to the same anchor. The stored row below a bad reply is good and
/// must not be cleared.
#[tokio::test]
async fn test_catch_up_keeps_proven_progress_on_mismatch() {
    let fixture = catch_up_fixture();
    let primary = fixture.authorities().next().unwrap();
    let config = primary.consensus_config();
    let db = config.node_storage();
    let committee = fixture.committee();

    let anchor = chained_header(100, B256::default(), &committee);
    store_executed(db, &anchor);
    // Real chain 101..=110, each with a real quorum-signed leader so the by-number BLS check
    // accepts it. The walk has cached the even numbers; the odd ones are fetched by number.
    let leader = fixture.certificate(&fixture.header_from_last_authority());
    let mut chain: BTreeMap<u64, ConsensusHeader> = BTreeMap::new();
    let mut parent = anchor.digest();
    for number in 101..=110u64 {
        let header = signed_chained_header(number, parent, &leader);
        parent = header.digest();
        if number % 2 == 0 {
            store_consensus_header_in_cache(db, &header);
        }
        chain.insert(number, header);
    }
    let target = chain[&110].clone();

    let cb = ConsensusBus::new();
    seed_recently_executed_blocks(&cb);
    let _rx = cb.consensus_header().subscribe();
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    // Two bad replies, each with a made-up sub_dag. The one at 105 has the real parent link, so
    // the stored row 106 fails to link onto it. The one at 107 does not link onto the stored row
    // 106 streamed in the same pass. Either way the headers proven before the reply become the
    // anchor, and the stored row below the reply is good and must stay.
    let bad_replies = [
        (fabricated_header(105, chain[&104].digest()), 104u64),
        (fabricated_header(107, B256::repeat_byte(0xab)), 106u64),
    ];
    for (bad, proven) in bad_replies {
        let mut served = chain.clone();
        served.insert(bad.number, bad);
        let pending = attempt_catch_up(
            &config,
            &cb,
            &serving_network(served),
            &mut last_header,
            &mut last_height,
            target.clone(),
            "test",
        )
        .await;
        assert!(pending.is_some(), "the bad reply keeps the target pending");
        assert_eq!(last_height, proven, "headers proven before the bad reply become the anchor");
        assert_eq!(last_header.digest(), chain[&proven].digest());
        assert!(db.get::<ConsensusBlocksCache>(&proven).unwrap().is_some(), "good row must stay");
        assert_eq!(
            db.get::<ConsensusBlockNumbersByDigest>(&chain[&proven].digest()).unwrap(),
            Some(proven),
            "the digest index of the good row must stay"
        );
    }

    let pending = attempt_catch_up(
        &config,
        &cb,
        &serving_network(chain),
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test retry",
    )
    .await;
    assert!(pending.is_none());
    assert_eq!(last_height, 110);
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
    let _rx = cb.consensus_header().subscribe();
    let mut last_header = anchor.clone();
    let mut last_height = anchor.number;

    let pending = attempt_catch_up(
        &config,
        &cb,
        &no_peer_network(),
        &mut last_header,
        &mut last_height,
        target.clone(),
        "test",
    )
    .await;
    assert!(pending.is_some());
    assert_eq!(
        last_height, 100,
        "the anchor must not advance onto a row the failed link implicates"
    );
    assert!(db.get_consensus_by_number(103).is_none(), "the stale row must be cleared");

    // The walk refills 103; the next pass reaches the target.
    store_consensus_header_in_cache(db, &chain[&103]);
    let pending = attempt_catch_up(
        &config,
        &cb,
        &no_peer_network(),
        &mut last_header,
        &mut last_height,
        target,
        "test retry",
    )
    .await;
    assert!(pending.is_none());
    assert_eq!(last_height, 105);
}
