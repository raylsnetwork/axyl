// SPDX-License-Identifier: BUSL-1.1
//! `header <N>`, `cert <N>` and `walk header <N>`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::Tier,
    report::header::{cert, header, walk, Link, Lookup},
};
use rayls_infrastructure_types::{Database as _, B256};

#[test]
fn header_agrees_across_nodes() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = header(&[a.open("a"), b.open("b")], 2, true).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    for n in &report.nodes {
        assert_eq!(n.tier, Some(Tier::Hot));
        let h = n.header.as_ref().unwrap();
        assert_eq!(h.number, 2);
        assert_eq!(h.certificate_count, 1);
        assert_eq!(h.leader.round, 3);
        assert!(h.certificates.as_ref().is_some_and(|c| c.len() == 1));
        assert!(h.reputation.is_some());
    }
    assert_eq!(
        report.nodes[0].header.as_ref().unwrap().digest,
        report.nodes[1].header.as_ref().unwrap().digest
    );
}

#[test]
fn header_from_cache_tier_and_missing_node() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // node b has header 4 only as a verified-but-unprocessed cache row
        write_cached_header(db, &fx.header(4, headers[3].digest()));
    });
    let report = header(&[a.open("a"), b.open("b")], 4, false).unwrap();
    // a's canonical tip is 3, so 4 is not reached there rather than missing
    assert_eq!(report.nodes[0].tier, None);
    assert_eq!(report.nodes[0].lookup, Lookup::NotReached);
    assert_eq!(report.nodes[0].tip, Some(3));
    assert_eq!(report.nodes[1].tier, Some(Tier::Cache));
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 found=1 not_reached=1");
}

#[test]
fn header_gap_below_the_tip_is_missing() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::ConsensusBlocks,
            >(txn, &2)
        })
        .unwrap();
    });
    let report = header(&[a.open("a"), b.open("b")], 2, false).unwrap();
    assert_eq!(report.nodes[1].lookup, Lookup::Missing);
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 found=1 missing=1");
}

#[test]
fn header_missing_everywhere_and_divergent() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // b disagrees at number 3: a header with a different parent
        write_header(db, &fx.header(3, headers[1].digest()));
    });
    let nodes = [a.open("a"), b.open("b")];
    let beyond = header(&nodes, 99, false).unwrap();
    assert_eq!(beyond.verdict.to_string(), "NOT_REACHED nodes=2 not_reached=2");
    assert!(beyond.nodes.iter().all(|n| n.tip == Some(3)));
    assert_eq!(
        header(&nodes, 3, false).unwrap().verdict.to_string(),
        "DIVERGENT nodes=2 what=header variants=2"
    );

    // a real gap on every node
    let gap_a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::ConsensusBlocks,
            >(txn, &1)
        })
        .unwrap();
    });
    let gap_b = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::ConsensusBlocks,
            >(txn, &1)
        })
        .unwrap();
    });
    let gap = header(&[gap_a.open("a"), gap_b.open("b")], 1, false).unwrap();
    assert_eq!(gap.verdict.to_string(), "MISSING nodes=2 missing=2");
}

#[test]
fn cert_agrees_signers_included() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = cert(&[a.open("a"), b.open("b")], 1, true).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    let leader = report.nodes[0].leader.as_ref().unwrap();
    assert_eq!(leader.signer_count, 3, "fixture certificates carry the three non-author votes");
    assert!(leader.signature.is_some());
    assert!(leader.parents.is_some() && leader.payload.is_some());
}

#[test]
fn cert_same_digest_different_signers_is_a_fork_signal() {
    let fx = Fixture::new();
    let (leader_a, leader_b) = fx.forked_leaders(7);
    let a = SeededNode::new(|db| {
        write_header(db, &fx.header_with_leader(5, B256::default(), leader_a.clone()))
    });
    let b = SeededNode::new(|db| {
        write_header(db, &fx.header_with_leader(5, B256::default(), leader_b.clone()))
    });
    let report = cert(&[a.open("a"), b.open("b")], 5, false).unwrap();
    let la = report.nodes[0].leader.as_ref().unwrap();
    let lb = report.nodes[1].leader.as_ref().unwrap();
    assert_eq!(la.summary.digest, lb.summary.digest, "consensus digest ignores signatures");
    assert_ne!(la.signers, lb.signers);
    assert_eq!(report.verdict.to_string(), "DIVERGENT nodes=2 what=signers variants=2");
    assert!(!report.verdict.healthy);
}

#[test]
fn cert_beyond_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = cert(&[a.open("a")], 42, false).unwrap();
    assert_eq!(report.nodes[0].lookup, Lookup::NotReached);
    assert_eq!(report.verdict.code, "NOT_REACHED");
    assert_eq!(report.nodes[0].tip, Some(3));
}

#[test]
fn walk_intact_chain_reaches_genesis() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = walk(&[a.open("a")], 3, 10).unwrap();
    let n = &report.nodes[0];
    assert!(n.ok, "{}", n.stopped);
    assert_eq!(n.hops.len(), 4);
    assert!(n.hops[..3].iter().all(|h| h.link == Link::Ok));
    assert_eq!(n.hops[3].link, Link::Genesis);
    assert_eq!(report.verdict.to_string(), "OK nodes=1 hops=3");
}

#[test]
fn walk_stops_after_back_hops() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = walk(&[a.open("a")], 3, 1).unwrap();
    let n = &report.nodes[0];
    assert!(n.ok);
    assert_eq!(n.hops.iter().map(|h| h.number).collect::<Vec<_>>(), vec![3, 2]);
}

#[test]
fn walk_detects_missing_parent_and_digest_mismatch() {
    let fx = Fixture::new();
    let missing = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // header 5 whose parent (4) does not exist
        write_header(db, &fx.header(5, headers[3].digest()));
    });
    let mismatch = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // header 4 claims a parent hash that is not header 3's digest
        let _ = headers;
        write_header(db, &fx.header(4, B256::repeat_byte(0x44)));
    });
    let r = walk(&[missing.open("m")], 5, 3).unwrap();
    assert_eq!(r.nodes[0].hops[0].link, Link::ParentMissing);
    assert!(!r.nodes[0].ok);
    assert_eq!(r.verdict.to_string(), "BROKEN nodes=1 hops=0 broken=1 first=5");

    let r = walk(&[mismatch.open("d")], 4, 3).unwrap();
    assert!(matches!(r.nodes[0].hops[0].link, Link::ParentDigestMismatch { .. }));
    assert!(!r.nodes[0].ok);
}

#[test]
fn walk_start_beyond_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let r = walk(&[a.open("a")], 50, 3).unwrap();
    assert!(r.nodes[0].hops.is_empty());
    assert!(r.nodes[0].start_not_reached);
    assert_eq!(r.nodes[0].stopped, "start header 50 not reached (tip 3)");
    assert_eq!(r.verdict.to_string(), "NOT_REACHED nodes=1 hops=0 not_reached=1");
}

#[test]
fn walk_start_missing_below_tip() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::ConsensusBlocks,
            >(txn, &2)
        })
        .unwrap();
    });
    let r = walk(&[a.open("a")], 2, 3).unwrap();
    assert!(!r.nodes[0].start_not_reached);
    assert_eq!(r.nodes[0].stopped, "start header 2 missing");
    assert_eq!(r.verdict.to_string(), "BROKEN nodes=1 hops=0 broken=1");
}
