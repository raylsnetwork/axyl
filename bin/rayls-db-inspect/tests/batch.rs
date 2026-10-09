// SPDX-License-Identifier: BUSL-1.1
//! `get-batch <DIGEST>` and `get-tx <TX_HASH>`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::Tier,
    report::{
        batch::{get_batch, get_tx, Commit},
        header::header,
        Lookup,
    },
    view::b256,
};
use rayls_infrastructure_storage::{tables::Batches, DatabaseType};
use rayls_infrastructure_types::{
    keccak256, BlockHash, Bytes, ConsensusHeader, Database as _, DbTxMut as _, B256,
};

/// The healthy chain (headers 0..=3) plus header 4, which commits one epoch-0 batch of `txs`.
/// Returns the batch digest and header 4.
fn seed_with_batch(fx: &Fixture, db: &DatabaseType, txs: &[Bytes]) -> (BlockHash, ConsensusHeader) {
    let (_, headers) = seed_healthy(fx, db);
    let digest = write_batch(db, &fx.batch(0, 1, txs.to_vec()));
    let h4 = fx.header_with_batches(4, headers[3].digest(), &[digest]);
    write_header(db, &h4);
    (digest, h4)
}

/// Header 4 commits an epoch-0 batch, header 5 opens epoch 1, and epoch 0 is archived: the batch
/// and headers 0..=4 live in the cold tier.
fn seed_with_cold_batch(fx0: &Fixture, fx1: &Fixture, db: &DatabaseType, txs: &[Bytes]) {
    let (_, h4) = seed_with_batch(fx0, db, txs);
    write_header(db, &fx1.header(5, h4.digest()));
    archive_below(db, 1);
}

#[test]
fn batch_found_hot_on_every_node_with_its_transactions() {
    let fx = Fixture::new();
    let txs = signed_transactions(3);
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let b = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let digest = fx.batch(0, 1, txs.clone()).digest();

    let report = get_batch(&[a.open("a"), b.open("b")], digest).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    assert!(report.verdict.healthy);
    assert_eq!(report.committed_at, Some(4));
    for n in &report.nodes {
        assert_eq!(n.lookup, Lookup::Found);
        assert_eq!(n.tier, Some(Tier::Hot));
        let v = n.batch.as_ref().unwrap();
        assert!(v.digest_ok);
        assert_eq!((v.epoch, v.worker_id, v.seq, v.transaction_count), (0, 0, 1, 3));
        assert_eq!(
            v.authority,
            rayls_infrastructure_types::Address::repeat_byte(0xbe).to_string(),
            "the sealing authority's execution address, as stored in the batch"
        );
        assert_eq!(v.committed_in, Commit::Committed { number: 4, tier: Tier::Hot });
        let list = &v.transactions;
        assert_eq!(list.len(), 3);
        let second = &list[1];
        assert_eq!(second.hash, b256(&keccak256(&txs[1])));
        assert_eq!(second.index, 1);
        assert_eq!(second.tx_type, Some(2), "EIP-1559");
        assert_eq!(second.nonce, Some(1));
        assert!(second.from.is_some(), "signer recovers");
        assert_eq!(
            second.to.as_deref(),
            Some(rayls_infrastructure_types::Address::repeat_byte(0x11).to_string().as_str())
        );
        assert_eq!(second.value.as_deref(), Some("1001"));
        assert!(second.error.is_none());
    }
}

/// The path from a batch to its commit is read from the stored header: the sub-dag certificate
/// whose payload lists the batch, and the header's own fields.
#[test]
fn batch_reports_its_carrying_certificate_and_committing_header() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let digest = fx.batch(0, 1, txs).digest();
    let (h4, _) = {
        let node = a.open("a");
        node.header(4).unwrap().unwrap()
    };

    let report = get_batch(&[a.open("a")], digest).unwrap();
    let v = report.nodes[0].batch.as_ref().unwrap();
    assert_eq!(v.committed_in, Commit::Committed { number: 4, tier: Tier::Hot });
    let path = v.path.as_ref().expect("committed: path present");
    assert_eq!(path.header.number, 4);
    assert_eq!(path.header.digest, b256(&h4.digest()));
    assert_eq!(path.header.parent_hash, b256(&h4.parent_hash));
    assert_eq!(path.header.leader.round, h4.sub_dag.leader.round());
    assert_eq!((path.header.certificate_count, path.header.batch_count), (1, 1));
    assert_eq!(path.header.commit_timestamp, h4.sub_dag.commit_timestamp());
    let carrier = path.certificate.as_ref().expect("the leader's payload lists the batch");
    assert_eq!(carrier.worker_id, 0);
    assert_eq!(carrier.certificate.summary, path.header.leader, "the fixture's leader carries it");
    assert_eq!(
        carrier.certificate.signers.len() as u64,
        h4.sub_dag.leader.signed_authorities().len(),
        "the signer set as stored"
    );
    assert!(carrier.certificate.raw.is_none(), "the stored certificate is not echoed");
}

/// A batch nothing committed has no path and nothing is invented for it.
#[test]
fn uncommitted_batch_has_no_path() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let sealed = fx.batch(0, 9, txs);
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_batch(db, &sealed);
    });
    let report = get_batch(&[a.open("a")], sealed.digest()).unwrap();
    let v = report.nodes[0].batch.as_ref().unwrap();
    assert_eq!(v.committed_in, Commit::NotCommitted);
    assert!(v.path.is_none());
    assert_eq!(v.transactions.len(), 1, "transactions are always listed");
}

/// Absence is judged against the header that committed the batch, not the epoch: a node whose tip
/// is past that header must hold it, one below has not reached it.
#[test]
fn absent_batch_is_not_reached_below_the_committing_header_and_missing_above_it() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let sealed = fx.batch(0, 1, txs.clone());
    // a: holds the batch, committed by its header 4
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    // b: tip 4 (a header 4 that commits nothing) and no batch: missing
    let b = SeededNode::new(|db| {
        let (_, h) = seed_healthy(&fx, db);
        write_header(db, &fx.header(4, h[3].digest()));
    });
    // c: tip 3, in the same epoch, no batch: not reached
    let c = SeededNode::new(|db| drop(seed_healthy(&fx, db)));

    let report = get_batch(&[a.open("a"), b.open("b"), c.open("c")], sealed.digest()).unwrap();
    assert_eq!(report.committed_at, Some(4));
    assert_eq!(report.nodes[0].lookup, Lookup::Found);
    assert_eq!(report.nodes[1].lookup, Lookup::Missing);
    assert_eq!(report.nodes[1].tip, Some(4));
    assert_eq!(report.nodes[2].lookup, Lookup::NotReached);
    assert_eq!(report.nodes[2].tip, Some(3));
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=3 found=1 missing=1 not_reached=1");
}

#[test]
fn batch_committed_nowhere_is_not_expected_anywhere() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let sealed = fx.batch(0, 9, txs.clone());
    // a sealed the batch but no header commits it; b (same tip) never received it: normal
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_batch(db, &sealed);
    });
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = get_batch(&[a.open("a"), b.open("b")], sealed.digest()).unwrap();
    assert_eq!(report.committed_at, None);
    assert_eq!(report.nodes[1].lookup, Lookup::NotReached);
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 found=1 not_reached=1");

    // a digest nobody holds says nothing about any node
    let nowhere = get_batch(&[b.open("b")], B256::repeat_byte(0x42)).unwrap();
    assert_eq!(nowhere.nodes[0].lookup, Lookup::NotFound);
    assert_eq!(nowhere.verdict.to_string(), "EMPTY nodes=1 not_found=1");
    assert!(!nowhere.verdict.healthy);
}

#[test]
fn batch_stored_under_the_wrong_digest_is_broken_even_when_others_lack_it() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let wrong = B256::repeat_byte(0x77);
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        let sealed = fx.batch(0, 1, txs.clone());
        db.with_write_txn(|txn| txn.insert::<Batches>(&wrong, &sealed)).unwrap();
    });
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));

    let report = get_batch(&[a.open("a")], wrong).unwrap();
    let v = report.nodes[0].batch.as_ref().unwrap();
    assert!(!v.digest_ok);
    assert_eq!(v.computed_digest, b256(&fx.batch(0, 1, txs).digest()));
    assert_eq!(report.verdict.to_string(), "BROKEN nodes=1 bad_digest=1");
    assert!(!report.verdict.healthy);

    // corruption outranks partial absence
    let report = get_batch(&[a.open("a"), b.open("b")], wrong).unwrap();
    assert_eq!(report.verdict.to_string(), "BROKEN nodes=2 found=1 not_reached=1 bad_digest=1");
}

#[test]
fn archived_batch_is_found_in_the_cold_tier_with_its_cold_header() {
    let fx0 = Fixture::new();
    let fx1 = Fixture::with_epoch(1);
    let txs = signed_transactions(2);
    let a = SeededNode::new(|db| seed_with_cold_batch(&fx0, &fx1, db, &txs));
    let digest = fx0.batch(0, 1, txs.clone()).digest();

    let node = a.open("a");
    assert_eq!(node.header(4).unwrap().map(|(_, t)| t), Some(Tier::Cold), "epoch 0 archived");
    assert_eq!(node.header(5).unwrap().map(|(_, t)| t), Some(Tier::Hot), "epoch 1 stays hot");
    let report = get_batch(&[node], digest).unwrap();
    assert_eq!(report.nodes[0].tier, Some(Tier::Cold));
    let v = report.nodes[0].batch.as_ref().unwrap();
    assert!(v.digest_ok);
    assert_eq!(v.transaction_count, 2);
    assert_eq!(v.committed_in, Commit::Committed { number: 4, tier: Tier::Cold });
    let path = v.path.as_ref().expect("the cold header is read for the path too");
    assert_eq!(path.header.number, 4);
    assert!(path.certificate.is_some());
    assert_eq!(report.verdict.to_string(), "OK nodes=1");

    // and the transaction scan reaches the cold jar too, with or without the epoch filter
    for epoch in [None, Some(0)] {
        let found = get_tx(&[a.open("a")], keccak256(&txs[1]), epoch).unwrap();
        let n = &found.nodes[0];
        assert_eq!(n.lookup, Lookup::Found, "epoch filter {epoch:?}");
        assert_eq!(n.matches.len(), 1);
        let m = &n.matches[0];
        assert_eq!((m.tier, m.index, m.epoch), (Tier::Cold, 1, 0));
        assert_eq!(m.committed_in, Commit::Committed { number: 4, tier: Tier::Cold });
        assert_eq!(m.path.as_ref().map(|p| p.header.number), Some(4));
        assert!(m.digest_ok);
        assert_eq!(
            (n.scanned.hot_batches, n.scanned.cold_batches, n.scanned.cold_epochs),
            (0, 1, 1)
        );
        assert_eq!(found.verdict.to_string(), "OK nodes=1");
    }
}

/// A cold index entry whose jar is gone is corruption, and `header -v` must agree with `get-batch`.
#[test]
fn dangling_cold_index_is_broken_not_absent() {
    let fx0 = Fixture::new();
    let fx1 = Fixture::with_epoch(1);
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| seed_with_cold_batch(&fx0, &fx1, db, &txs));
    let digest = fx0.batch(0, 1, txs).digest();
    // remove the batches jar but keep the location index
    let jars = std::path::Path::new(&a.consensus_db()).join("cold/batches");
    let mut removed = 0;
    for entry in std::fs::read_dir(&jars).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
        removed += 1;
    }
    assert!(removed > 0, "epoch 0 jar files existed");

    let report = get_batch(&[a.open("a")], digest).unwrap();
    let n = &report.nodes[0];
    assert_eq!(n.lookup, Lookup::Missing);
    assert_eq!(n.dangling.map(|l| l.epoch), Some(0));
    assert!(n.batch.is_none());
    assert_eq!(report.verdict.to_string(), "BROKEN nodes=1 missing=1 dangling=1");

    let h = header(&[(a.open("a"))], 4, true).unwrap();
    let presence = &h.nodes[0].header.as_ref().unwrap().batches.as_ref().unwrap()[0];
    assert!(presence.dangling && presence.tier.is_none());
}

#[test]
fn tx_is_found_in_its_batch_with_the_committing_header() {
    let fx = Fixture::new();
    let txs = signed_transactions(3);
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let b = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let digest = fx.batch(0, 1, txs.clone()).digest();
    let (h4, _) = a.open("a").header(4).unwrap().unwrap();

    let report = get_tx(&[a.open("a"), b.open("b")], keccak256(&txs[2]), None).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    assert!(report.verdict.healthy);
    assert_eq!(report.committed_at, Some(4));
    for n in &report.nodes {
        assert_eq!(n.lookup, Lookup::Found);
        assert_eq!(n.matches.len(), 1);
        let m = &n.matches[0];
        assert_eq!(m.digest, b256(&digest));
        assert_eq!((m.tier, m.index, m.epoch, m.transaction_count), (Tier::Hot, 2, 0, 3));
        assert_eq!(m.authority, rayls_infrastructure_types::Address::repeat_byte(0xbe).to_string());
        assert_eq!(m.committed_in, Commit::Committed { number: 4, tier: Tier::Hot });
        // the whole stored path: batch -> carrying certificate -> committing header
        let path = m.path.as_ref().expect("committed: path present");
        assert_eq!(path.header.number, 4);
        assert_eq!(path.header.digest, b256(&h4.digest()));
        let carrier = path.certificate.as_ref().expect("a sub-dag certificate lists the batch");
        assert_eq!(carrier.certificate.summary, path.header.leader);
        assert_eq!(carrier.worker_id, 0);
        let t = n.transaction.as_ref().unwrap();
        assert_eq!(t.nonce, Some(2));
        assert_eq!(t.hash, b256(&keccak256(&txs[2])));
        assert!(t.error.is_none());
        assert_eq!((n.scanned.hot_batches, n.scanned.hot_table_rows), (1, 1));
        assert!(!n.skipped);
    }
}

#[test]
fn tx_not_found_reports_what_was_scanned() {
    let fx = Fixture::new();
    let txs = signed_transactions(2);
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });

    let report = get_tx(&[a.open("a")], B256::repeat_byte(0x99), None).unwrap();
    assert_eq!(report.nodes[0].lookup, Lookup::NotFound);
    // a node skipped for being in an earlier epoch is behind even when nobody holds the hash
    let behind = get_tx(&[a.open("a")], B256::repeat_byte(0x99), Some(5)).unwrap();
    assert!(behind.nodes[0].skipped);
    assert_eq!(behind.verdict.to_string(), "NOT_REACHED nodes=1 not_reached=1");
    assert_eq!(report.nodes[0].scanned.hot_batches, 1);
    assert!(report.nodes[0].matches.is_empty());
    assert_eq!(report.verdict.to_string(), "EMPTY nodes=1 not_found=1");

    // an epoch the node has not entered is not scanned at all and is not a gap
    let ahead = get_tx(&[a.open("a")], keccak256(&txs[0]), Some(5)).unwrap();
    assert_eq!(ahead.nodes[0].lookup, Lookup::NotReached);
    assert!(ahead.nodes[0].skipped);
    assert_eq!(ahead.nodes[0].scanned, Default::default());
    assert_eq!(ahead.verdict.to_string(), "NOT_REACHED nodes=1 not_reached=1");

    // the filter keeps batches of the epoch and still reads the whole hot table
    let other = get_tx(&[a.open("a")], keccak256(&txs[0]), Some(0)).unwrap();
    assert_eq!(other.nodes[0].lookup, Lookup::Found);
    assert_eq!(other.nodes[0].scanned.hot_batches, 1);
}

/// A transaction sealed twice (duplicate submission) is not a disagreement between nodes.
#[test]
fn tx_in_two_batches_counts_copies_and_uncommitted_but_is_not_divergent() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    // a: the tx in a batch committed by header 4 and again in a later, uncommitted batch
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
        write_batch(db, &fx.batch(0, 9, txs.clone()));
    });
    // b: only the uncommitted copy, tip 3 (has not reached header 4)
    let b = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_batch(db, &fx.batch(0, 9, txs.clone()));
    });
    let report = get_tx(&[a.open("a"), b.open("b")], keccak256(&txs[0]), None).unwrap();
    assert_eq!(report.nodes[0].matches.len(), 2);
    assert_eq!(report.nodes[1].matches.len(), 1);
    assert_eq!(report.nodes[1].matches[0].committed_in, Commit::NotCommitted);
    assert_eq!(report.verdict.to_string(), "OK nodes=2 copies=2 uncommitted=1");
    assert!(report.verdict.healthy);
}

/// Nodes naming different batches for the same committing header do disagree.
#[test]
fn tx_committed_in_different_batches_at_the_same_header_is_divergent() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| {
        seed_with_batch(&fx, db, &txs);
    });
    let b = SeededNode::new(|db| {
        let (_, h) = seed_healthy(&fx, db);
        let other = write_batch(db, &fx.batch(0, 9, txs.clone()));
        write_header(db, &fx.header_with_batches(4, h[3].digest(), &[other]));
    });
    let report = get_tx(&[a.open("a"), b.open("b")], keccak256(&txs[0]), None).unwrap();
    assert_eq!(report.verdict.to_string(), "DIVERGENT nodes=2 what=batch variants=2");
    assert!(!report.verdict.healthy);
}

/// A header in the verified-but-unprocessed cache is a consensus commit this node has not
/// executed yet: reported as such, and not counted as uncommitted.
#[test]
fn tx_committed_by_a_cached_header_is_verified_not_processed() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| {
        let (_, h) = seed_healthy(&fx, db);
        let digest = write_batch(db, &fx.batch(0, 1, txs.clone()));
        write_cached_header(db, &fx.header_with_batches(4, h[3].digest(), &[digest]));
    });
    let report = get_tx(&[a.open("a")], keccak256(&txs[0]), None).unwrap();
    assert_eq!(report.nodes[0].matches[0].committed_in, Commit::Verified { number: 4 });
    // the cached header is read for the path like a canonical one
    let path = report.nodes[0].matches[0].path.as_ref().unwrap();
    assert_eq!(path.header.number, 4);
    assert!(path.certificate.is_some());
    assert_eq!(report.committed_at, Some(4));
    assert_eq!(report.verdict.to_string(), "OK nodes=1");
}

/// A node with no consensus headers at all has an unknown position: it is scanned, never skipped.
#[test]
fn tx_on_a_node_without_headers_is_still_scanned() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| {
        write_batch(db, &fx.batch(0, 1, txs.clone()));
    });
    let report = get_tx(&[a.open("a")], keccak256(&txs[0]), Some(0)).unwrap();
    let n = &report.nodes[0];
    assert!(!n.skipped);
    assert_eq!(n.current_epoch, None);
    assert_eq!(n.lookup, Lookup::Found);
    assert_eq!(n.matches[0].committed_in, Commit::NotCommitted);
    assert!(n.matches[0].path.is_none(), "nothing committed it: no path");
    assert_eq!(report.verdict.to_string(), "OK nodes=1 uncommitted=1");
}

/// `--epoch` for an epoch with no cold jar reads nothing cold and is not an error.
#[test]
fn tx_epoch_filter_without_a_cold_jar_scans_nothing_cold() {
    let fx0 = Fixture::new();
    let fx1 = Fixture::with_epoch(1);
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| seed_with_cold_batch(&fx0, &fx1, db, &txs));
    // epoch 1 is current: no jar for it, and the hot table holds no batches after the archive
    let report = get_tx(&[a.open("a")], keccak256(&txs[0]), Some(1)).unwrap();
    let n = &report.nodes[0];
    assert!(!n.skipped);
    assert_eq!((n.scanned.hot_batches, n.scanned.cold_batches, n.scanned.cold_epochs), (0, 0, 0));
    assert_eq!(n.lookup, Lookup::NotFound);
    assert_eq!(report.verdict.to_string(), "EMPTY nodes=1 not_found=1");
}
