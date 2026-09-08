// SPDX-License-Identifier: BUSL-1.1
//! `epochs <FROM> <TO>` and `chain-check`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::report::epoch::{chain_check, epochs, Cell};
use rayls_infrastructure_types::B256;

#[test]
fn matrix_cells_and_row_verdicts() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    // b is missing epoch 2 entirely
    let b = SeededNode::new(|db| {
        let h0 = fx.header(0, B256::default());
        let h1 = fx.header(1, h0.digest());
        let r0 = fx.record(0, None, h0.digest());
        write_epoch(db, &r0, None);
        let r1 = fx.record(1, Some(&r0), h1.digest());
        write_epoch(db, &r1, Some(&fx.certify(&r1, &[0, 1, 2])));
    });
    let nodes = [a.open("a"), b.open("b")];

    let report = epochs(&nodes, Some((0, 3))).unwrap();
    assert_eq!(report.nodes, vec!["a", "b"]);
    assert_eq!(report.rows.len(), 4);
    assert_eq!(report.rows[0].cells, vec![Cell::RecordOnly, Cell::RecordOnly]);
    assert_eq!(report.rows[0].status, "ok");
    assert_eq!(report.rows[1].cells, vec![Cell::RecordAndCert, Cell::RecordAndCert]);
    assert_eq!(report.rows[1].status, "ok");
    // b never closed epoch 2 (no headers, latest record 1): behind, not a gap
    assert_eq!(report.rows[2].cells, vec![Cell::RecordAndCert, Cell::NotReached]);
    assert_eq!(report.rows[2].status, "partial");
    assert_eq!(report.rows[3].cells, vec![Cell::NotReached, Cell::NotReached]);
    assert_eq!(report.rows[3].status, "not-reached");
    assert_eq!(
        report.verdict.to_string(),
        "PARTIAL epochs=4 ok=2 partial=1 not_reached=3..=3 first=2"
    );
}

#[test]
fn range_beyond_the_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = epochs(&[a.open("a")], Some((10, 12))).unwrap();
    assert!(report.rows.iter().all(|r| r.status == "not-reached"));
    assert_eq!(report.verdict.to_string(), "NOT_REACHED epochs=3 not_reached=10..=12");

    // a range that extends past the tip is healthy for the part that exists
    let report = epochs(&[a.open("a")], Some((0, 5))).unwrap();
    assert_eq!(report.verdict.to_string(), "OK epochs=6 ok=3 not_reached=3..=5");
}

#[test]
fn true_gap_in_the_matrix_is_missing() {
    let fx = Fixture::with_epoch(5);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = epochs(&[a.open("a")], Some((0, 4))).unwrap();
    assert_eq!(report.rows[3].cells, vec![Cell::Missing]);
    assert_eq!(report.rows[3].status, "missing");
    assert_eq!(report.verdict.to_string(), "MISSING epochs=5 ok=3 missing=2 first=3");
}

#[test]
fn all_range_is_the_union_of_nodes() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = epochs(&[a.open("a")], None).unwrap();
    assert_eq!((report.from, report.to), (0, 2));
    assert_eq!(report.verdict.to_string(), "OK epochs=3 ok=3");
    assert!(report.verdict.healthy);
}

#[test]
fn all_on_empty_nodes() {
    let a = SeededNode::new(|_| {});
    let report = epochs(&[a.open("a")], None).unwrap();
    assert!(report.rows.is_empty());
    assert_eq!(report.verdict.to_string(), "EMPTY epochs=0");
}

#[test]
fn run_rejects_epochs_without_bounds_or_all() {
    use rayls_db_inspect::cli::{Cli, Command, NodeArgs};
    let a = SeededNode::new(|_| {});
    let cli = Cli {
        json: false,
        verbose: false,
        exclusive: false,
        require_stopped: false,
        recover: false,
        command: Command::Epochs {
            from: None,
            to: None,
            all: false,
            nodes: NodeArgs { dbs: vec![a.datadir()] },
        },
    };
    let err = rayls_db_inspect::run(&cli).expect_err("no bounds and no --all");
    assert!(err.to_string().contains("FROM_EPOCH and TO_EPOCH"), "{err}");
}

#[test]
fn inverted_range_is_an_error() {
    let a = SeededNode::new(|_| {});
    assert!(epochs(&[a.open("a")], Some((5, 1))).is_err());
}

#[test]
fn chain_check_healthy() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = chain_check(&[a.open("a"), b.open("b")], None, None).unwrap();
    assert_eq!(report.verdict.to_string(), "OK checked=3");
    for n in &report.nodes {
        assert_eq!((n.from, n.to), (Some(0), Some(2)));
        assert_eq!(n.checked, 3);
        assert_eq!(n.certified, 2, "epoch 0 is unsigned by design");
        assert!(n.ok);
    }
}

#[test]
fn chain_check_finds_broken_link_gap_and_uncertified() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, headers) = seed_healthy(&fx, db);
        // epoch 3 is uncertified; epoch 5 links to nothing valid; epoch 4 is a gap
        let r3 = fx.record(3, Some(&records[2]), headers[3].digest());
        write_epoch(db, &r3, None);
        let mut r5 = fx.record(5, None, B256::default());
        r5.parent_hash = B256::repeat_byte(0x55);
        write_epoch(db, &r5, Some(&fx.certify(&r5, &[0, 1, 2])));
    });
    let report = chain_check(&[a.open("a")], None, None).unwrap();
    let n = &report.nodes[0];
    assert_eq!((n.from, n.to), (Some(0), Some(5)));
    assert_eq!(n.gaps, vec![4]);
    assert_eq!(n.uncertified, vec![3]);
    // the record after a gap has no previous record to link against, so no broken-link entry
    assert!(n.broken_links.is_empty());
    assert!(!n.ok);
    assert_eq!(report.verdict.to_string(), "BROKEN checked=5 gaps=1 uncertified=1 first=3");
}

#[test]
fn chain_check_range_beyond_the_latest_record() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    // --to past the last record is clamped and noted, not reported as gaps
    let report = chain_check(&[a.open("a")], None, Some(50)).unwrap();
    let n = &report.nodes[0];
    assert_eq!(n.to, Some(2));
    assert!(n.gaps.is_empty());
    assert!(n.ok);
    assert!(n.note.as_deref().unwrap().contains("--to 50 > latest record 2"), "{:?}", n.note);
    assert_eq!(report.verdict.to_string(), "OK checked=3");
    // --from past the last record: nothing to check
    let report = chain_check(&[a.open("a")], Some(50), None).unwrap();
    assert_eq!(report.nodes[0].checked, 0);
    assert_eq!(report.verdict.to_string(), "EMPTY checked=0");
    assert!(report.nodes[0].note.as_deref().unwrap().contains("--from 50 > latest record 2"));
}

#[test]
fn chain_check_broken_parent_hash() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        let mut r2 = records[2].clone();
        r2.parent_hash = B256::repeat_byte(0x22);
        write_epoch(db, &r2, Some(&fx.certify(&r2, &[0, 1, 2])));
    });
    let report = chain_check(&[a.open("a")], Some(1), Some(2)).unwrap();
    let n = &report.nodes[0];
    assert_eq!(n.broken_links.len(), 1);
    assert_eq!(n.broken_links[0].epoch, 2);
    assert_eq!(report.verdict.to_string(), "BROKEN checked=2 broken=1 first=2");
}

#[test]
fn chain_check_divergent_nodes() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        let other = fx.record(2, Some(&records[1]), B256::repeat_byte(0xab));
        write_epoch(db, &other, Some(&fx.certify(&other, &[0, 1, 2])));
    });
    let report = chain_check(&[a.open("a"), b.open("b")], None, None).unwrap();
    assert_eq!(report.verdict.to_string(), "DIVERGENT checked=3 divergent=1 first=2");
}
