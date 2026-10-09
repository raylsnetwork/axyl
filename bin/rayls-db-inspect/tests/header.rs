// SPDX-License-Identifier: BUSL-1.1
//! `header <N>`, `cert <N>` and `header-check <N>`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::Tier,
    report::header::{cert, header, header_check, Link, Lookup, Unverifiable},
};
use rayls_infrastructure_types::{Database as _, B256};

#[test]
fn header_agrees_across_nodes() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = header(&[(a.open("a")), (b.open("b"))], 2, true).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    for n in &report.nodes {
        assert_eq!(n.tier, Some(Tier::Hot));
        let h = n.header.as_ref().unwrap();
        assert_eq!(h.number, 2);
        assert_eq!(h.certificate_count, 1);
        assert_eq!(h.leader.round, 3);
        let raw = h.raw.as_ref().expect("-v embeds the stored header");
        assert_eq!(raw.number, 2);
        assert_eq!(raw.sub_dag.certificates.len(), 1);
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
    let report = header(&[(a.open("a")), (b.open("b"))], 4, false).unwrap();
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
    let report = header(&[(a.open("a")), (b.open("b"))], 2, false).unwrap();
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
    let nodes = [(a.open("a")), (b.open("b"))];
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
    let gap = header(&[(gap_a.open("a")), (gap_b.open("b"))], 1, false).unwrap();
    assert_eq!(gap.verdict.to_string(), "MISSING nodes=2 missing=2");
}

#[test]
fn cert_agrees_signers_included() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = cert(&[(a.open("a")), (b.open("b"))], 1, true).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2");
    let leader = report.nodes[0].leader.as_ref().unwrap();
    assert_eq!(leader.signer_count, 3, "fixture certificates carry the three non-author votes");
    assert!(leader.signature.is_some());
    assert!(leader.raw.is_some(), "-v embeds the stored certificate");
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
    let report = cert(&[(a.open("a")), (b.open("b"))], 5, false).unwrap();
    let la = report.nodes[0].leader.as_ref().unwrap();
    let lb = report.nodes[1].leader.as_ref().unwrap();
    assert_eq!(la.summary.digest, lb.summary.digest, "consensus digest ignores signatures");
    assert_ne!(la.signers, lb.signers);
    // these nodes hold no epoch records, so the signatures cannot be re-checked
    assert_eq!(
        report.verdict.to_string(),
        "DIVERGENT nodes=2 what=signers variants=2 unverifiable=2"
    );
    assert!(!report.verdict.healthy);
}

#[test]
fn cert_beyond_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = cert(&[(a.open("a"))], 42, false).unwrap();
    assert_eq!(report.nodes[0].lookup, Lookup::NotReached);
    assert_eq!(report.verdict.code, "NOT_REACHED");
    assert_eq!(report.nodes[0].tip, Some(3));
}

#[test]
fn header_check_intact_chain_reaches_genesis() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = header_check(&[(a.open("a"))], 3, 10).unwrap();
    let n = &report.nodes[0];
    assert!(n.ok, "{}", n.stopped);
    assert_eq!(n.hops.len(), 4);
    assert!(n.hops[..3].iter().all(|h| h.link == Link::Ok));
    assert_eq!(n.hops[3].link, Link::Genesis);
    assert_eq!(report.verdict.to_string(), "OK nodes=1 hops=3");
}

#[test]
fn header_check_stops_after_back_hops() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = header_check(&[(a.open("a"))], 3, 1).unwrap();
    let n = &report.nodes[0];
    assert!(n.ok);
    assert_eq!(n.hops.iter().map(|h| h.number).collect::<Vec<_>>(), vec![3, 2]);
    assert_eq!(n.hops[0].link, Link::Ok);
    assert_eq!(n.hops[1].link, Link::End, "the last row's own link is not followed");
    assert_eq!(report.verdict.to_string(), "OK nodes=1 hops=1");
}

#[test]
fn header_check_with_one_node_behind_the_start_is_partial() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let behind = SeededNode::new(|db| write_header(db, &fx.header(0, B256::default())));
    let report = header_check(&[(a.open("a")), (behind.open("b"))], 3, 2).unwrap();
    assert!(report.nodes[0].ok);
    assert!(report.nodes[1].start_not_reached);
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 hops=2 not_reached=1");
}

#[test]
fn header_check_detects_missing_parent_and_digest_mismatch() {
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
    let r = header_check(&[(missing.open("m"))], 5, 3).unwrap();
    assert_eq!(r.nodes[0].hops[0].link, Link::ParentMissing);
    assert!(!r.nodes[0].ok);
    assert_eq!(r.verdict.to_string(), "BROKEN nodes=1 hops=0 broken=1 first=5");

    let r = header_check(&[(mismatch.open("d"))], 4, 3).unwrap();
    assert!(matches!(r.nodes[0].hops[0].link, Link::ParentDigestMismatch { .. }));
    assert!(!r.nodes[0].ok);
}

#[test]
fn header_check_start_beyond_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let r = header_check(&[(a.open("a"))], 50, 3).unwrap();
    assert!(r.nodes[0].hops.is_empty());
    assert!(r.nodes[0].start_not_reached);
    assert_eq!(r.nodes[0].stopped, "start header 50 not reached (tip 3)");
    assert_eq!(r.verdict.to_string(), "NOT_REACHED nodes=1 hops=0 not_reached=1");
}

#[test]
fn header_check_start_missing_below_tip() {
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
    let r = header_check(&[(a.open("a"))], 2, 3).unwrap();
    assert!(!r.nodes[0].start_not_reached);
    assert_eq!(r.nodes[0].stopped, "start header 2 missing");
    assert!(r.nodes[0].start_missing, "a missing start is a missing header, not a broken link");
    assert_eq!(r.verdict.to_string(), "MISSING nodes=1 hops=0 missing=1");
}

/// A digest index that maps a parent elsewhere is reported, and the check goes on.
#[test]
fn header_check_reports_an_index_mismatch_and_continues() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // the digest index maps header 2's digest to number 7
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::insert::<
                rayls_infrastructure_storage::tables::ConsensusBlockNumbersByDigest,
            >(txn, &headers[2].digest(), &7)
        })
        .unwrap();
    });
    let r = header_check(&[(a.open("a"))], 3, 2).unwrap();
    let n = &r.nodes[0];
    assert_eq!(n.hops[0].link, Link::IndexMismatch { indexed: Some(7) });
    assert_eq!(n.hops.len(), 3, "the check continues past an index mismatch");
    assert_eq!(n.hops[1].link, Link::Ok);
    assert!(!n.ok);
    assert_eq!(r.verdict.to_string(), "BROKEN nodes=1 hops=2 broken=1 first=3");
}

/// A real chain starts at header 1, whose parent is the digest of the default header; nothing
/// stores a header 0. The check reaches genesis there, and `header 0` is not found, not missing.
#[test]
fn a_chain_anchored_on_the_genesis_digest_reaches_genesis_at_header_one() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        // record 0 holds the committee that verifies the certificates
        write_epoch(db, &fx.record(0, None, B256::default()), None);
        let mut parent = rayls_db_inspect::report::header::genesis_anchor();
        for n in 1..=3u64 {
            let h = fx.header(n, parent);
            parent = h.digest();
            write_header(db, &h);
        }
    });
    let r = header_check(&[(a.open("a"))], 3, 10).unwrap();
    let n = &r.nodes[0];
    assert!(n.ok, "{}", n.stopped);
    assert_eq!(n.stopped, "reached genesis");
    assert_eq!(n.hops.iter().map(|h| h.number).collect::<Vec<_>>(), vec![3, 2, 1]);
    assert_eq!(n.hops[2].link, Link::Genesis);
    assert_eq!(r.verdict.to_string(), "OK nodes=1 hops=2");

    let zero = header(&[(a.open("a"))], 0, false).unwrap();
    assert_eq!(zero.nodes[0].lookup, Lookup::NotFound);
    assert_eq!(zero.verdict.to_string(), "EMPTY nodes=1 not_found=1");
}

/// Epoch 0 archived and the chain continued: headers and their certificates are served and
/// verified from the cold tier, and the check walks from the hot tip through the cold span to
/// genesis, reporting each hop's tier.
#[test]
fn archived_headers_are_served_and_verified_from_the_cold_tier() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        let h4 = fx.header(4, headers[3].digest());
        write_header(db, &h4);
        // everything so far is epoch 0: sealing below epoch 1 archives headers 0..=4
        archive_below(db, 1);
        write_header(db, &fx.header(5, h4.digest()));
    });
    let nodes = [(a.open("a"))];

    let r = header(&nodes, 2, false).unwrap();
    assert_eq!(r.nodes[0].tier, Some(Tier::Cold));
    assert_eq!(r.verdict.to_string(), "OK nodes=1", "verified with record 0's committee");
    assert_eq!(header(&nodes, 5, false).unwrap().nodes[0].tier, Some(Tier::Hot));

    let c = cert(&nodes, 2, false).unwrap();
    assert_eq!((c.nodes[0].tier, c.verdict.to_string()), (Some(Tier::Cold), "OK nodes=1".into()));

    let hc = header_check(&nodes, 5, 10).unwrap();
    let n = &hc.nodes[0];
    assert!(n.ok, "{}", n.stopped);
    assert_eq!(n.stopped, "reached genesis");
    let tiers: Vec<Tier> = n.hops.iter().map(|h| h.tier).collect();
    assert_eq!(tiers, [Tier::Hot, Tier::Cold, Tier::Cold, Tier::Cold, Tier::Cold, Tier::Cold]);
    assert_eq!(hc.verdict.to_string(), "OK nodes=1 hops=5");
}

/// A cache row can outlive its header's promotion (a late gossip copy). It must not make an
/// archived header look unprocessed: the cold tier is consulted before the cache, and the cache
/// only answers for a header no canonical tier holds.
#[test]
fn a_stale_cache_row_does_not_shadow_an_archived_header() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        write_cached_header(db, &headers[2]);
        archive_below(db, 1);
        // a header above the tip, verified but not processed: genuinely cached
        write_cached_header(db, &fx.header(4, headers[3].digest()));
    });
    let nodes = [(a.open("a"))];
    assert_eq!(header(&nodes, 2, false).unwrap().nodes[0].tier, Some(Tier::Cold));
    assert_eq!(header(&nodes, 4, false).unwrap().nodes[0].tier, Some(Tier::Cache));
}

/// A node that lost an epoch's records cannot prove the certificates of the next epoch: the
/// check says so, names the records, and the verdict is PARTIAL rather than OK.
#[test]
fn missing_epoch_records_make_the_next_epochs_headers_unverifiable() {
    // headers of epoch 2 on a node holding record 0 only: neither record 2 nor record 1
    let fx = Fixture::with_epoch(2);
    let a = SeededNode::new(|db| {
        let (_, headers) = healthy_chain(&fx);
        for h in &headers {
            write_header(db, h);
        }
        write_epoch(db, &fx.record(0, None, B256::default()), None);
    });
    let nodes = [(a.open("a"))];

    let r = header_check(&nodes, 3, 10).unwrap();
    let n = &r.nodes[0];
    assert!(n.ok, "the links are intact: {}", n.stopped);
    assert_eq!(
        n.unverifiable,
        vec![Unverifiable { epoch: 2, hops: 4, missing_records: vec![1, 2] }]
    );
    assert_eq!(r.verdict.to_string(), "PARTIAL nodes=1 hops=3 unverifiable=4");

    let h = header(&nodes, 3, false).unwrap();
    assert_eq!(h.verdict.to_string(), "PARTIAL nodes=1 unverifiable=1");
    assert_eq!(
        h.nodes[0].signature_check.as_ref().unwrap().to_string(),
        "no committee keys for epoch 2: the source holds no record for epoch 2 or 1"
    );
}
