// SPDX-License-Identifier: BUSL-1.1
//! `epoch <N>` against seeded nodes.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::report::epoch::{epoch, EpochStatus, LinkCheck};
use rayls_infrastructure_types::{Database as _, B256};

#[test]
fn certified_everywhere() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let nodes = [(a.open("a")), (b.open("b"))];

    let report = epoch(&nodes, 2, true).unwrap();
    assert_eq!(report.verdict.code, "OK");
    assert!(report.verdict.healthy);
    for n in &report.nodes {
        assert_eq!(n.status, EpochStatus::Certified);
        let rec = n.record.as_ref().unwrap();
        assert!(rec.index_ok);
        assert_eq!(rec.parent_link, LinkCheck::Ok);
        assert_eq!(rec.committee_handoff_ok, Some(true));
        assert_eq!(rec.parent_consensus_number, Some(2));
        assert_eq!(rec.committee.as_ref().map(Vec::len), Some(4));
        let cert = n.cert.as_ref().unwrap();
        assert!(cert.digest_match && cert.quorum_ok && cert.signature_ok && cert.valid);
        assert_eq!(cert.signers, vec![0, 1, 2, 3]);
        assert!(n.checkpoint.is_none());
    }
}

#[test]
fn genesis_record_is_expected_to_be_unsigned() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = epoch(&[(a.open("a"))], 0, false).unwrap();
    assert_eq!(report.nodes[0].status, EpochStatus::RecordOnly);
    assert_eq!(report.nodes[0].record.as_ref().unwrap().parent_link, LinkCheck::Genesis);
    assert_eq!(report.verdict.to_string(), "OK nodes=1 genesis=1");
    assert!(report.verdict.healthy);
}

#[test]
fn record_only_on_one_node_is_partial() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    // node b wrote epoch 2's record without its certificate
    let b = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::EpochCerts,
            >(txn, &records[2].digest())
        })
        .unwrap();
    });
    let report = epoch(&[(a.open("a")), (b.open("b"))], 2, false).unwrap();
    assert_eq!(report.nodes[0].status, EpochStatus::Certified);
    assert_eq!(report.nodes[1].status, EpochStatus::RecordOnly);
    assert!(report.nodes[1].cert.is_none());
    assert_eq!(report.verdict.code, "PARTIAL");
    assert!(!report.verdict.healthy);
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 certified=1 record_only=1");
}

#[test]
fn missing_everywhere_is_the_incident_case() {
    // consensus headers carry epoch 5, so epoch 3 has closed everywhere and its record must exist
    let fx = Fixture::with_epoch(5);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = epoch(&[(a.open("a")), (b.open("b"))], 3, false).unwrap();
    assert!(report.nodes.iter().all(|n| n.status == EpochStatus::Missing));
    assert_eq!(report.nodes[0].position.current_epoch, Some(5));
    assert_eq!(report.verdict.to_string(), "MISSING nodes=2 missing=2");
    assert!(!report.verdict.healthy);
}

#[test]
fn epoch_beyond_the_tip_is_not_reached_not_missing() {
    // records 0..=2 and headers in epoch 0: epoch 3 (and 900) simply have not happened yet
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let nodes = [(a.open("a")), (b.open("b"))];
    for e in [3, 900] {
        let report = epoch(&nodes, e, false).unwrap();
        assert!(report.nodes.iter().all(|n| n.status == EpochStatus::NotReached), "epoch {e}");
        assert_eq!(report.verdict.code, "NOT_REACHED");
        assert!(!report.verdict.healthy);
        assert_eq!(report.verdict.to_string(), "NOT_REACHED nodes=2 not_reached=2");
    }
    let p = report_position(&nodes);
    assert_eq!(
        (p.latest_epoch_record, p.current_epoch, p.consensus_tip),
        (Some(2), Some(0), Some(3))
    );
}

fn report_position(
    nodes: &[rayls_db_inspect::node_db::NodeDb],
) -> rayls_db_inspect::node_db::Position {
    nodes[0].position().unwrap()
}

#[test]
fn missing_on_one_node_names_it() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (records, headers) = seed_healthy(&fx, db);
        let _ = (records, headers);
    });
    // node c never got past epoch 1
    let c = SeededNode::new(|db| {
        let r0 = fx.record(0, None, B256::default());
        write_epoch(db, &r0, None);
        let r1 = fx.record(1, Some(&r0), B256::default());
        write_epoch(db, &r1, Some(&fx.certify(&r1, &[0, 1, 2])));
    });
    let report = epoch(&[(a.open("a")), (b.open("b")), (c.open("c"))], 2, false).unwrap();
    // c has no headers past epoch 1 and no record 2: it is behind, not holding a gap
    assert_eq!(report.nodes[2].status, EpochStatus::NotReached);
    assert_eq!(report.verdict.code, "PARTIAL");
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=3 certified=2 not_reached=1");
    assert_eq!(report.nodes[2].position.latest_epoch_record, Some(1));
}

#[test]
fn node_that_closed_the_epoch_but_lacks_the_record_is_missing() {
    let fx = Fixture::with_epoch(5);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let c = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        let _ = headers;
        // drop epoch 2's record: the node is in epoch 5, so this is a real gap
        db.with_write_txn(|txn| {
            rayls_infrastructure_types::DbTxMut::remove::<
                rayls_infrastructure_storage::tables::EpochRecords,
            >(txn, &2)
        })
        .unwrap();
    });
    let report = epoch(&[(a.open("a")), (c.open("c"))], 2, false).unwrap();
    assert_eq!(report.nodes[1].status, EpochStatus::Missing);
    assert_eq!(report.verdict.code, "PARTIAL");
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 certified=1 missing=1");
}

#[test]
fn divergent_records_are_detected() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        // overwrite epoch 2 with a record pointing at a different boundary header
        let other = fx.record(2, Some(&records[1]), B256::repeat_byte(0xab));
        write_epoch(db, &other, Some(&fx.certify(&other, &[0, 1, 2])));
    });
    let report = epoch(&[(a.open("a")), (b.open("b"))], 2, false).unwrap();
    assert_eq!(report.verdict.to_string(), "DIVERGENT nodes=2 variants=2 certified=2");
    assert_ne!(
        report.nodes[0].record.as_ref().unwrap().digest,
        report.nodes[1].record.as_ref().unwrap().digest
    );
    // the divergent node's boundary header is unknown to it
    assert_eq!(report.nodes[1].record.as_ref().unwrap().parent_consensus_number, None);
}

#[test]
fn cert_below_quorum_is_record_only_with_reasons() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        // re-certify epoch 2 with only two signers (super quorum of 4 is 3)
        let weak = fx.certify(&records[2], &[0, 1]);
        write_epoch(db, &records[2], Some(&weak));
    });
    let report = epoch(&[(a.open("a"))], 2, false).unwrap();
    let n = &report.nodes[0];
    assert_eq!(n.status, EpochStatus::RecordOnly);
    let cert = n.cert.as_ref().unwrap();
    assert!(cert.digest_match);
    assert!(!cert.quorum_ok);
    assert!(cert.signature_ok, "two valid signatures still verify as signatures");
    assert!(!cert.valid);
    assert_eq!(report.verdict.code, "PARTIAL");
}

#[test]
fn cert_with_wrong_signature_is_flagged() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        // signers claim 0..3 but the aggregate only carries 0..2's signatures
        let mut bad = fx.certify(&records[2], &[0, 1, 2]);
        bad.signed_authorities.push(3);
        write_epoch(db, &records[2], Some(&bad));
    });
    let report = epoch(&[(a.open("a"))], 2, false).unwrap();
    let cert = report.nodes[0].cert.as_ref().unwrap();
    assert!(cert.quorum_ok);
    assert!(!cert.signature_ok);
    assert!(!cert.valid);
    assert_eq!(report.nodes[0].status, EpochStatus::RecordOnly);
}

#[test]
fn leftover_checkpoint_is_reported() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_checkpoint(db, 2);
    });
    let report = epoch(&[(a.open("a"))], 2, false).unwrap();
    let cp = report.nodes[0].checkpoint.as_ref().expect("checkpoint reported");
    assert_eq!(cp.epoch, 2);
    assert_eq!(cp.completed_phase, "Draining");
    // a leftover checkpoint does not by itself change the certification verdict
    assert_eq!(report.verdict.code, "OK");
}

#[test]
fn broken_parent_link_is_reported() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, headers) = seed_healthy(&fx, db);
        let mut r2 = records[2].clone();
        r2.parent_hash = B256::repeat_byte(0x11);
        let _ = headers;
        write_epoch(db, &r2, Some(&fx.certify(&r2, &[0, 1, 2])));
    });
    let report = epoch(&[(a.open("a"))], 2, false).unwrap();
    match &report.nodes[0].record.as_ref().unwrap().parent_link {
        LinkCheck::Broken { expected } => assert!(expected.starts_with("0x")),
        other => panic!("expected broken link, got {other:?}"),
    }
}

#[test]
fn empty_database_reports_table_absent() {
    let fx = Fixture::new();
    let _ = &fx;
    let a = SeededNode::new(|_db| {});
    // open_db creates every table, so an empty node is "not reached", not "table absent"
    let report = epoch(&[(a.open("a"))], 1, false).unwrap();
    assert_eq!(report.nodes[0].status, EpochStatus::NotReached);
    assert_eq!(report.verdict.code, "NOT_REACHED");
    assert_eq!(report.nodes[0].position.current_epoch, None);
}

/// A node that closed the epoch but has not certified it yet holds the record it built in
/// `pending_epoch_record`: reported as `pending`, never as missing or not reached.
#[test]
fn pending_record_is_closed_but_uncertified() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        let (records, headers) = healthy_chain(&fx);
        for h in &headers {
            write_header(db, h);
        }
        for (record, cert) in &records[..2] {
            write_epoch(db, record, cert.as_ref());
        }
        write_pending(db, &records[2].0);
    });
    let expected = healthy_chain(&fx).0[2].0.digest();

    let report = epoch(&[a.open("a"), b.open("b")], 2, false).unwrap();
    assert_eq!(report.verdict.to_string(), "PARTIAL nodes=2 certified=1 pending=1");
    assert!(!report.verdict.healthy);
    assert_eq!(report.nodes[0].status, EpochStatus::Certified);
    assert!(report.nodes[0].pending.is_none());
    let b_view = &report.nodes[1];
    assert_eq!(b_view.status, EpochStatus::Pending);
    assert!(b_view.record.is_none() && b_view.cert.is_none());
    let pending = b_view.pending.as_ref().unwrap();
    assert_eq!(
        pending.digest,
        rayls_db_inspect::view::b256(&expected),
        "the same record a certified"
    );
    assert!(!pending.stale);
    assert_eq!(pending.matches_record, None);
    assert_eq!(pending.committee_size, fx.keys().len());
    // a pending record proves the node closed the epoch
    assert_eq!(b_view.position.latest_epoch_record, Some(1));
    assert_eq!(b_view.position.latest_pending_record, Some(2));
    assert!(b_view.position.has_closed_epoch(2));
    assert!(!b_view.position.has_closed_epoch(3));

    // the epoch after it is not reached, not missing
    let next = epoch(&[b.open("b")], 3, false).unwrap();
    assert_eq!(next.nodes[0].status, EpochStatus::NotReached);
    assert_eq!(next.verdict.to_string(), "NOT_REACHED nodes=1 not_reached=1");
}

/// The certificate's write removes the pending row in the same transaction, so a pending row
/// next to a certified record is a leftover and is flagged as such.
#[test]
fn stale_pending_row_next_to_a_certified_record_is_flagged() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (records, _) = seed_healthy(&fx, db);
        write_pending(db, &records[2]);
    });
    let report = epoch(&[a.open("a")], 2, false).unwrap();
    assert_eq!(report.nodes[0].status, EpochStatus::Certified);
    let pending = report.nodes[0].pending.as_ref().unwrap();
    assert!(pending.stale);
    assert_eq!(pending.matches_record, Some(true));
    assert_eq!(report.verdict.to_string(), "OK nodes=1 certified=1 stale_pending=1");
}
