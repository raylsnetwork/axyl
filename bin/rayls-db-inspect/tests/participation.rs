// SPDX-License-Identifier: BUSL-1.1
//! `participation <EPOCH>`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::report::participation::{
    participation, AuthorityRow, EpochState, ParticipationNodeView,
};
use rayls_infrastructure_storage::{tables::EpochRecords, DatabaseType};
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, Database as _, DbTxMut as _, EpochRecord, B256,
};

/// The row of fixture authority `i` in a node's view.
fn row<'v>(view: &'v ParticipationNodeView, fx: &Fixture, i: usize) -> &'v AuthorityRow {
    let hex = fx.authority_hex(i);
    view.authorities.iter().find(|r| r.authority == hex).expect("authority row")
}

/// Rewrites record 0 so that `last` is the epoch's boundary header, where the node's tally
/// stopped. `seed_healthy`'s record 0 names header 0, which would bound the tally there.
fn close_epoch0_at(fx: &Fixture, db: &DatabaseType, last: &ConsensusHeader) {
    write_epoch(db, &fx.record(0, None, last.digest()), None);
}

/// `seed_healthy` (headers 0..=3, each a one-certificate sub-dag led by authority 0 at round
/// n+1) plus header 4: authorities 1, 2 and 3 contribute a certificate at round 4, authority 0
/// leads at round 5 and, as in a real commit, its certificate is in the sub-dag too. Header 4
/// is the epoch's boundary.
fn seed_with_subdag(fx: &Fixture, db: &DatabaseType) {
    let (_, headers) = seed_healthy(fx, db);
    let batch = BlockHash::repeat_byte(0xb1);
    let certs = vec![
        fx.certificate_by(1, 4, &[batch], &[0, 2, 3]),
        fx.certificate_by(2, 4, &[], &[0, 1, 3]),
        fx.certificate_by(3, 4, &[batch, BlockHash::repeat_byte(0xb2)], &[0, 1, 2]),
    ];
    let leader = fx.certificate_by(0, 5, &[], &[1, 2, 3]);
    let mut all = certs;
    all.push(leader.clone());
    let h4 = fx.header_with_subdag(4, headers[3].digest(), all, leader);
    write_header(db, &h4);
    close_epoch0_at(fx, db, &h4);
}

#[test]
fn agreeing_nodes_are_tallied_in_committee_order() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| seed_with_subdag(&fx, db));
    let b = SeededNode::new(|db| seed_with_subdag(&fx, db));
    let report = participation(&[a.open("a"), b.open("b")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=2 closed=2 headers=5 certs=8");
    for n in &report.nodes {
        assert_eq!(n.state, EpochState::Closed);
        assert_eq!((n.headers, n.hot, n.cold), (5, 5, 0));
        assert_eq!((n.first_header, n.last_header), (Some(0), Some(4)));
        assert_eq!((n.first_round, n.last_round), (Some(1), Some(5)));
        assert_eq!((n.boundary_header, n.boundary_round), (Some(4), Some(5)));
        assert!(n.after_boundary.is_empty());
        assert_eq!(n.committee_size, Some(4));
        assert_eq!(n.committee_from, Some("its record"));
        assert_eq!((n.totals.certs, n.totals.batches, n.totals.authors), (8, 3, 4));
        // every certificate carries three votes
        assert_eq!(n.totals.signatures, 24);
        assert_eq!(n.unknown_signers, 0);
        assert!(n.genesis_headers.is_empty() && n.cached_not_tallied.is_empty());

        // rows follow the committee's (sorted-key) order, which is not the fixture's
        let indices: Vec<Option<usize>> = n.authorities.iter().map(|r| r.committee_index).collect();
        assert_eq!(indices, vec![Some(0), Some(1), Some(2), Some(3)]);
        for i in 0..4 {
            assert_eq!(row(n, &fx, i).committee_index, Some(fx.committee_index(i)));
        }

        let leader = row(n, &fx, 0).tally;
        assert_eq!((leader.participation_rounds, leader.anchor_rounds), (5, 5));
        assert_eq!(leader.participation_bps, Some(10_000));
        assert_eq!((leader.certs, leader.batches), (5, 0));
        // authority 0 signed the three round-4 certificates and nothing else
        assert_eq!(leader.signed, Some(3));
        for i in 1..4 {
            let t = row(n, &fx, i).tally;
            assert_eq!((t.participation_rounds, t.anchor_rounds), (1, 0), "authority {i}");
            assert_eq!(t.participation_bps, Some(2_000));
            assert_eq!(t.certs, 1);
            // four seeded headers plus two of the three round-4 certificates plus the leader
            assert_eq!(t.signed, Some(4 + 2 + 1), "authority {i}");
        }
        assert_eq!(row(n, &fx, 1).tally.batches, 1);
        assert_eq!(row(n, &fx, 3).tally.batches, 2);
    }
}

#[test]
fn an_extra_certificate_on_one_node_is_divergent() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| seed_with_subdag(&fx, db));
    let b = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // header 3 on b also holds a certificate by authority 2
        let leader = headers[3].sub_dag.leader.clone();
        let extra = fx.certificate_by(2, 3, &[], &[0, 1, 3]);
        let h3 = fx.header_with_subdag(3, headers[2].digest(), vec![leader.clone(), extra], leader);
        write_header(db, &h3);
        close_epoch0_at(&fx, db, &h3);
    });
    let report = participation(&[a.open("a"), b.open("b")], 0).unwrap();
    assert_eq!(
        report.verdict.to_string(),
        "DIVERGENT nodes=2 closed=2 what=tally variants=2 headers=5 certs=8"
    );
}

#[test]
fn an_epoch_beyond_every_tip_is_not_reached() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = participation(&[a.open("a"), b.open("b")], 99).unwrap();
    assert_eq!(report.verdict.to_string(), "NOT_REACHED nodes=2 not_reached=2");
    for n in &report.nodes {
        assert_eq!(n.state, EpochState::NotReached);
        assert!(n.authorities.is_empty());
        assert_eq!(n.headers, 0);
    }
}

#[test]
fn headers_past_the_record_boundary_are_listed_and_not_tallied() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // the close block was built at header 2 (round 3); header 3 was committed after it
        close_epoch0_at(&fx, db, &headers[2]);
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(
        report.verdict.to_string(),
        "OK nodes=1 closed=1 headers=3 certs=3 after_boundary=1"
    );
    let n = &report.nodes[0];
    assert_eq!((n.boundary_header, n.boundary_round), (Some(2), Some(3)));
    assert_eq!(n.after_boundary, vec![3]);
    assert_eq!((n.last_header, n.last_round), (Some(2), Some(3)));
    assert_eq!(row(n, &fx, 0).tally.anchor_rounds, 3);
}

#[test]
fn an_open_epoch_is_printed_from_the_previous_record_and_not_compared() {
    let fx = Fixture::with_epoch(1);
    // a has closed epoch 1 (records up to 2, record 1 bounding the tally at header 1); b is
    // still in it, with record 0 only, so its running tally covers every header
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        // a node still in epoch 1 would not hold records 1 and 2 yet
        db.with_write_txn(|txn| {
            txn.remove::<EpochRecords>(&1)?;
            txn.remove::<EpochRecords>(&2)
        })
        .unwrap();
    });
    let report = participation(&[a.open("a"), b.open("b")], 1).unwrap();
    assert_eq!(
        report.verdict.to_string(),
        "PARTIAL nodes=2 closed=1 open=1 headers=2 certs=2 after_boundary=2"
    );
    let (a, b) = (&report.nodes[0], &report.nodes[1]);
    assert_eq!(a.state, EpochState::Closed);
    assert_eq!(a.committee_from, Some("its record"));
    assert_eq!((a.boundary_header, a.boundary_round), (Some(1), Some(2)));
    assert_eq!((a.headers, a.after_boundary.clone()), (2, vec![2, 3]));
    assert_eq!(b.state, EpochState::Open);
    assert_eq!(b.committee_from, Some("the previous record"));
    assert_eq!((b.boundary_header, b.headers), (None, 4));
    assert!(b.after_boundary.is_empty());
}

#[test]
fn archived_headers_are_tallied_from_the_cold_tier() {
    let fx0 = Fixture::new();
    let fx1 = Fixture::with_epoch(1);
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx0, db);
        close_epoch0_at(&fx0, db, &headers[3]);
        // header 4 opens epoch 1, so epoch 0 (headers 0..=3) can be archived
        write_header(db, &fx1.header(4, headers[3].digest()));
        archive_below(db, 1);
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=1 closed=1 headers=4 certs=4");
    let n = &report.nodes[0];
    assert_eq!((n.hot, n.cold), (0, 4));
    assert_eq!((n.first_header, n.last_header), (Some(0), Some(3)));
    // the boundary header is resolved from the cold tier too
    assert_eq!((n.boundary_header, n.boundary_round), (Some(3), Some(4)));
    assert_eq!(row(n, &fx0, 0).tally.anchor_rounds, 4);
}

#[test]
fn a_cached_header_is_listed_and_not_tallied() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        close_epoch0_at(&fx, db, &headers[3]);
        write_cached_header(db, &fx.header(4, headers[3].digest()));
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=1 closed=1 headers=4 certs=4 cached=1");
    let n = &report.nodes[0];
    assert_eq!(n.cached_not_tallied, vec![4]);
    assert_eq!(n.headers, 4);
}

#[test]
fn a_genesis_header_is_listed_and_not_tallied() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        close_epoch0_at(&fx, db, &headers[3]);
        // a header whose leader is the unsigned round-0 genesis certificate
        let genesis = fx.genesis_certificate(0);
        write_header(
            db,
            &fx.header_with_subdag(4, headers[3].digest(), vec![genesis.clone()], genesis),
        );
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=1 closed=1 headers=4 certs=4");
    let n = &report.nodes[0];
    assert_eq!(n.genesis_headers, vec![4]);
    assert!(n.after_boundary.is_empty(), "genesis is skipped before the boundary check");
    assert_eq!((n.headers, n.last_header, n.last_round), (4, Some(3), Some(4)));
}

#[test]
fn a_repeated_author_counts_once_per_header() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // one commit flattening two DAG rounds of authority 1
        let leader = fx.certificate_by(0, 6, &[], &[1, 2, 3]);
        let certs = vec![
            fx.certificate_by(1, 4, &[], &[0, 2, 3]),
            fx.certificate_by(1, 5, &[], &[0, 2, 3]),
            leader.clone(),
        ];
        let h4 = fx.header_with_subdag(4, headers[3].digest(), certs, leader);
        write_header(db, &h4);
        close_epoch0_at(&fx, db, &h4);
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=1 closed=1 headers=5 certs=7");
    let t = row(&report.nodes[0], &fx, 1).tally;
    assert_eq!((t.participation_rounds, t.certs), (1, 2));
}

#[test]
fn without_a_committee_the_signed_column_is_unavailable() {
    let fx0 = Fixture::new();
    let fx1 = Fixture::with_epoch(1);
    let a = SeededNode::new(|db| {
        let (_, headers) = healthy_chain(&fx0);
        for h in &headers {
            write_header(db, h);
        }
        // the tip is in epoch 1, so epoch 0 has closed, but no record was ever written
        write_header(db, &fx1.header(4, headers[3].digest()));
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "OK nodes=1 closed=1 headers=4 certs=4 no_committee=1");
    let n = &report.nodes[0];
    assert_eq!(n.committee_size, None);
    // no record: the bound is unknown, every stored header is tallied
    assert_eq!((n.boundary_header, n.headers), (None, 4));
    assert_eq!(n.authorities.len(), 1, "only the author seen, no committee rows");
    assert!(n.authorities.iter().all(|r| r.committee_index.is_none() && r.tally.signed.is_none()));
    // signer bits cannot be resolved without a committee: counted in the total, never as
    // "beyond the committee"
    assert_eq!(n.totals.signatures, 12);
    assert_eq!(n.unknown_signers, 0);
}

#[test]
fn an_author_outside_the_committee_is_flagged() {
    let fx = Fixture::new();
    // the fixture authority whose key sorts last is dropped from record 0's committee
    let dropped = (0..4).find(|i| fx.committee_index(*i) == 3).unwrap();
    let others = [(dropped + 1) % 4, (dropped + 2) % 4, (dropped + 3) % 4];
    let a = SeededNode::new(|db| {
        // headers 0..=3: led by one of the others, voted by the rest including the dropped one
        let mut parent = B256::default();
        for n in 0..4u64 {
            let lead = others[0];
            let signers: Vec<usize> = (0..4).filter(|i| *i != lead).collect();
            let leader = fx.certificate_by(lead, n as u32 + 1, &[], &signers);
            let h = fx.header_with_subdag(n, parent, vec![leader.clone()], leader);
            parent = h.digest();
            write_header(db, &h);
        }
        // header 4, the boundary, is led by the dropped authority and voted by the others only
        let leader = fx.certificate_by(dropped, 5, &[], &others);
        let h4 = fx.header_with_subdag(4, parent, vec![leader.clone()], leader);
        write_header(db, &h4);
        let mut keys = fx.keys();
        keys.truncate(3);
        let record = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys,
            parent_hash: B256::default(),
            parent_state: Default::default(),
            parent_consensus: h4.digest(),
        };
        write_epoch(db, &record, None);
    });
    let report = participation(&[a.open("a")], 0).unwrap();
    // headers 0..=3 each carry one vote from the dropped authority (bit 3); header 4 none
    assert_eq!(
        report.verdict.to_string(),
        "OK nodes=1 closed=1 headers=5 certs=5 outsiders=1 unknown_signers=4"
    );
    let n = &report.nodes[0];
    assert_eq!(n.committee_size, Some(3));
    assert_eq!(n.authorities.len(), 4);
    let outsider = row(n, &fx, dropped);
    assert_eq!(outsider.committee_index, None);
    assert_eq!((outsider.tally.anchor_rounds, outsider.tally.participation_rounds), (1, 1));
    assert_eq!(outsider.tally.signed, None, "signer bits beyond the committee resolve to nobody");
    assert_eq!(
        n.authorities.last().unwrap().authority,
        fx.authority_hex(dropped),
        "outsiders come last"
    );
}

#[test]
fn a_closed_epoch_with_no_headers_is_empty() {
    let fx = Fixture::with_epoch(1);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let report = participation(&[a.open("a"), b.open("b")], 0).unwrap();
    assert_eq!(report.verdict.to_string(), "EMPTY nodes=2 closed=2");
    for n in &report.nodes {
        assert_eq!((n.headers, n.authorities.len()), (0, 4));
        // record 0 names an epoch-1 header as its boundary: not a bound for epoch 0
        assert_eq!(n.boundary_header, None);
    }
}
