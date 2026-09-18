// SPDX-License-Identifier: BUSL-1.1
//! `--rpc` nodes beside `--db` nodes, and the implicit signature re-check.

#![allow(unused_crate_dependencies)]

mod common;

use clap::Parser as _;
use common::{rpc::MockRpc, *};
use rayls_db_inspect::{
    cli::Cli,
    node_db::{LiveStatus, OpenOptions, Tier},
    report::{
        epoch::{epoch, epoch_check, EpochStatus, LinkCheck},
        header::{cert, header, header_check, SignatureCheck},
    },
    run,
    source::Source,
};
use rayls_infrastructure_types::{Certificate, Hash as _, SignatureVerificationState, B256};

fn db(node: &SeededNode, label: &str) -> Source {
    Source::open_db(&format!("{label}={}", node.datadir()), &OpenOptions::default()).unwrap()
}

fn rpc(mock: &MockRpc, label: &str) -> Source {
    Source::open_rpc(&format!("{label}={}", mock.url), 0).unwrap()
}

#[test]
fn rpc_node_agrees_with_the_database_it_mirrors() {
    // headers in epoch 3: records 0..=2 are closed epochs, as on a real node
    let fx = Fixture::with_epoch(3);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers.clone(), epochs, true);
    let nodes = [db(&a, "a"), rpc(&mock, "r")];
    assert_eq!(nodes[1].live(), LiveStatus::Rpc);

    // the same positions; the RPC node's records are probed from rayls_epochRecord
    let (pa, pr) = (nodes[0].position().unwrap(), nodes[1].position().unwrap());
    assert_eq!(
        (pr.consensus_tip, pr.current_epoch, pr.latest_epoch_record),
        (Some(3), Some(3), Some(2))
    );
    assert_eq!((pa.consensus_tip, pa.current_epoch), (pr.consensus_tip, pr.current_epoch));

    // certified epochs match record for record
    let r = epoch(&nodes, 2, false).unwrap();
    assert_eq!(r.verdict.to_string(), "OK nodes=2 certified=2");
    assert_eq!(r.nodes[1].status, EpochStatus::Certified);
    assert_eq!(
        r.nodes[0].record.as_ref().unwrap().digest,
        r.nodes[1].record.as_ref().unwrap().digest
    );
    // the RPC node checks its record's parent link through rayls_epochRecordByHash too
    assert!(r.nodes[1].record.as_ref().unwrap().index_ok);

    // headers: the tip from rayls_latestHeader, older ones from rayls_consensusHeaderByNumber
    for number in [3, 1] {
        let h = header(&nodes, number, false).unwrap();
        assert_eq!(h.verdict.to_string(), "OK nodes=2", "header {number}");
        assert_eq!(h.nodes[1].tier, Some(Tier::Rpc));
        assert_eq!(
            h.nodes[0].header.as_ref().unwrap().digest,
            h.nodes[1].header.as_ref().unwrap().digest
        );
    }
    let beyond = header(&nodes, 9, false).unwrap();
    assert_eq!(beyond.verdict.to_string(), "NOT_REACHED nodes=2 not_reached=2");
    assert_eq!(beyond.nodes[1].tip, Some(3));

    let c = cert(&nodes, 2, true).unwrap();
    assert_eq!(c.verdict.to_string(), "OK nodes=2");
    assert!(c.nodes[1].leader.as_ref().unwrap().raw.is_some());

    let w = header_check(&nodes, 3, 10).unwrap();
    assert_eq!(w.verdict.to_string(), "OK nodes=2 hops=3");
    assert_eq!(w.nodes[1].hops.len(), 4);
    assert!(w.nodes[1].hops.iter().all(|h| matches!(h.verify, SignatureCheck::Verified { .. })));

    let cc = epoch_check(&nodes, None, None, false).unwrap();
    assert_eq!(cc.verdict.to_string(), "OK nodes=2 checked=3");
    assert_eq!(
        (cc.nodes[1].from, cc.nodes[1].to),
        (Some(1), Some(2)),
        "RPC serves certified records"
    );
}

/// The RPC only serves certified records, so the unsigned genesis record (and any record stored
/// without its certificate) is absent from an RPC node. The tool reports that plainly.
#[test]
fn rpc_node_does_not_serve_uncertified_records() {
    let fx = Fixture::with_epoch(3);
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, true);
    let nodes = [db(&a, "a"), rpc(&mock, "r")];

    let r = epoch(&nodes, 0, false).unwrap();
    assert_eq!(r.nodes[0].status, EpochStatus::RecordOnly);
    assert_eq!(r.nodes[1].status, EpochStatus::Missing);
    assert_eq!(r.verdict.to_string(), "PARTIAL nodes=2 genesis=1 missing=1");

    // epoch-check therefore starts at record 1 on the RPC node and cannot check its parent link
    let cc = epoch_check(&nodes, None, None, true).unwrap();
    assert_eq!((cc.nodes[1].from, cc.nodes[1].to), (Some(1), Some(2)));
    assert_eq!(cc.nodes[1].records.as_ref().unwrap()[0].link, LinkCheck::PrevMissing);
    assert!(cc.nodes[1].ok, "an uncheckable link is not a broken one");
}

#[test]
fn older_node_without_header_methods_still_serves_its_tip() {
    let fx = Fixture::new();
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, false);
    let nodes = [rpc(&mock, "old")];
    // epoch 0's record is unsigned, so this node serves no committee: unverifiable, not failed
    assert_eq!(
        header(&nodes, 3, false).unwrap().verdict.to_string(),
        "PARTIAL nodes=1 unverifiable=1"
    );
    let err = header(&nodes, 1, false).expect_err("no rayls_consensusHeaderByNumber");
    assert!(err.to_string().contains("does not serve rayls_consensusHeaderByNumber"), "{err}");
}

#[test]
fn commands_that_need_a_database_refuse_rpc_nodes() {
    let fx = Fixture::new();
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, true);
    let zero = format!("0x{}", "00".repeat(32));
    for args in [vec!["summary"], vec!["get-batch", zero.as_str()], vec!["get-tx", zero.as_str()]] {
        let cli =
            Cli::try_parse_from([&["x"], args.as_slice(), &["--rpc", &mock.url]].concat()).unwrap();
        let err = run(&cli).expect_err("database-only command");
        assert!(err.to_string().contains("needs database nodes"), "{args:?}: {err}");
    }
    // and the epoch commands work end to end through `run`
    let cli =
        Cli::try_parse_from(["x", "epoch", "2", "--rpc", &format!("r={}", mock.url)]).unwrap();
    assert!(run(&cli).unwrap().healthy());
}

#[test]
fn rpc_flags_parse_like_db_flags() {
    let cli = Cli::try_parse_from([
        "x",
        "--rpc",
        "a=http://h:1",
        "epoch",
        "2",
        "--rpc",
        "http://h:2,c=http://h:3",
    ])
    .unwrap();
    assert_eq!(cli.rpcs(), vec!["a=http://h:1", "http://h:2", "c=http://h:3"]);
    assert!(cli.dbs().is_empty());
    assert!(
        Cli::try_parse_from(["x", "epoch", "2"]).is_ok_and(|c| run(&c).is_err()),
        "no node given"
    );
}

#[test]
fn signatures_are_rechecked_against_the_epoch_committee() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let nodes = [db(&a, "a")];

    let h = header(&nodes, 2, false).unwrap();
    assert_eq!(
        h.nodes[0].signature_check,
        Some(SignatureCheck::Verified { epoch: 0, keys: 4, keys_from: "its record" })
    );
    assert_eq!(h.verdict.to_string(), "OK nodes=1");
    let c = cert(&nodes, 2, false).unwrap();
    assert!(matches!(c.nodes[0].signature_check, Some(SignatureCheck::Verified { .. })));
    assert_eq!(c.verdict.to_string(), "OK nodes=1");

    // a leader certificate signed by another committee fails against the recorded one
    let other = Fixture::new();
    let b = SeededNode::new(|db| {
        let (_, h) = seed_healthy(&fx, db);
        write_header(db, &other.header(4, h[3].digest()));
    });
    let c = cert(&[db(&b, "b")], 4, false).unwrap();
    assert!(matches!(
        c.nodes[0].signature_check,
        Some(SignatureCheck::Failed { epoch: 0, keys: 4, .. })
    ));
    assert_eq!(c.verdict.to_string(), "BROKEN nodes=1 sig_failed=1");
    assert!(!c.verdict.healthy);
    let h = header(&[db(&b, "b")], 4, false).unwrap();
    assert_eq!(h.verdict.to_string(), "BROKEN nodes=1 sig_failed=1");
    let w = header_check(&[db(&b, "b")], 4, 1).unwrap();
    assert!(w.nodes[0].ok, "the links are intact");
    assert!(w.nodes[0].hops[0].verify.failed());
    assert_eq!(w.verdict.to_string(), "BROKEN nodes=1 hops=1 sig_failed=1");

    // no epoch records at all: nothing to check against
    let c_node = SeededNode::new(|db| write_header(db, &fx.header(0, B256::default())));
    let c = cert(&[db(&c_node, "c")], 0, false).unwrap();
    assert_eq!(c.nodes[0].signature_check, Some(SignatureCheck::NoKeys { epoch: 0 }));
    assert_eq!(c.verdict.to_string(), "PARTIAL nodes=1 unverifiable=1");

    // a certificate carrying the `Genesis` state is unsigned by design (the type has the state;
    // `Certificate::genesis` itself leaves the default `Unsigned`)
    let committee = fx.committee.committee();
    let mut genesis = Certificate::genesis(&committee).remove(0);
    genesis.set_signature_verification_state(SignatureVerificationState::Genesis);
    let g = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_header(db, &fx.header_with_leader(4, B256::repeat_byte(0x33), genesis.clone()));
    });
    let c = cert(&[db(&g, "g")], 4, false).unwrap();
    assert_eq!(c.nodes[0].signature_check, Some(SignatureCheck::Genesis));
    assert_eq!(c.verdict.to_string(), "OK nodes=1");
    assert_eq!(
        c.nodes[0].leader.as_ref().unwrap().summary.digest,
        rayls_db_inspect::view::cert_digest(genesis.digest())
    );
}

/// Verification works the same over RPC: the keys come from the records the node serves.
#[test]
fn signatures_are_rechecked_over_rpc() {
    let fx = Fixture::new();
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, true);
    let c = cert(&[rpc(&mock, "r")], 3, false).unwrap();
    // epoch 0's record is not served (unsigned), so the keys come from nowhere: unverifiable
    assert_eq!(c.nodes[0].signature_check, Some(SignatureCheck::NoKeys { epoch: 0 }));
    assert_eq!(c.verdict.to_string(), "PARTIAL nodes=1 unverifiable=1");
}

/// A certificate that fails against the committee the RPC node serves is broken there too.
#[test]
fn a_failing_signature_over_rpc_is_broken() {
    let fx = Fixture::with_epoch(3);
    let other = Fixture::with_epoch(3);
    let (epochs, mut headers) = healthy_chain(&fx);
    // header 4 is signed by another committee and served beside the certified records
    let h4 = other.header(4, headers[3].digest());
    headers.push(h4);
    let mock = MockRpc::start(headers, epochs, true);
    let c = cert(&[rpc(&mock, "r")], 4, false).unwrap();
    assert!(
        matches!(
            c.nodes[0].signature_check,
            Some(SignatureCheck::Failed { epoch: 3, keys: 4, .. })
        ),
        "{:?}",
        c.nodes[0].signature_check
    );
    assert_eq!(c.verdict.to_string(), "BROKEN nodes=1 sig_failed=1");
}

/// Mixed sources: database-only commands name the RPC nodes they refuse, and one node cannot be
/// given twice under two labels or two labels cannot name two nodes.
#[test]
fn mixed_sources_are_checked_up_front() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, true);
    let rpc_spec = format!("r={}", mock.url);
    let cli =
        Cli::try_parse_from(["x", "summary", "--db", &a.datadir(), "--rpc", &rpc_spec]).unwrap();
    let err = run(&cli).expect_err("summary needs databases").to_string();
    assert!(err.contains("needs database nodes") && err.contains("r is an RPC node"), "{err}");

    let same_label = format!("a={}", mock.url);
    let cli = Cli::try_parse_from([
        "x",
        "epoch",
        "2",
        "--db",
        &format!("a={}", a.datadir()),
        "--rpc",
        &same_label,
    ])
    .unwrap();
    assert!(run(&cli).expect_err("label twice").to_string().contains("used twice"));
    let cli = Cli::try_parse_from([
        "x",
        "epoch",
        "2",
        "--rpc",
        &rpc_spec,
        "--rpc",
        &format!("s={}", mock.url),
    ])
    .unwrap();
    assert!(run(&cli).expect_err("endpoint twice").to_string().contains("given twice"));
}

/// Validators do not keep the header behind `rayls_latestHeader` current (it can lag or read 0),
/// so the tip is found by probing `rayls_consensusHeaderByNumber`.
#[test]
fn rpc_tip_is_probed_not_trusted_from_latest_header() {
    let fx = Fixture::with_epoch(3);
    let (epochs, headers) = healthy_chain(&fx);
    // latestHeader answers the default header (number 0) although headers 0..=3 are served
    let stale = MockRpc::start_with_latest(
        headers.clone(),
        epochs.clone(),
        true,
        Some(rayls_infrastructure_types::ConsensusHeader::default()),
    );
    let node = rpc(&stale, "stale");
    let p = node.position().unwrap();
    assert_eq!(
        (p.consensus_tip, p.current_epoch, p.latest_epoch_record),
        (Some(3), Some(3), Some(2))
    );
    assert_eq!(header(&[node], 3, false).unwrap().verdict.to_string(), "OK nodes=1");
    // the watch's default header is not a stored header 0: header 0 is asked of the node, which
    // here serves the fixture's own header 0
    let zero = header(&[rpc(&stale, "stale")], 0, false).unwrap();
    let h = zero.nodes[0].header.as_ref().expect("the mock serves header 0");
    assert_eq!(h.digest, rayls_db_inspect::view::b256(&headers[0].digest()));
    assert_eq!(zero.verdict.to_string(), "OK nodes=1");
    // a lagging latestHeader is only the floor of the search
    let lagging =
        MockRpc::start_with_latest(headers.clone(), epochs, true, Some(headers[1].clone()));
    assert_eq!(rpc(&lagging, "lag").position().unwrap().consensus_tip, Some(3));
}

/// `--rpc-rate` spaces the requests to one node: at 20 per second the calls behind a position
/// and a record listing (latest header, tip probes, one call per epoch) take at least the
/// spacing times their count, where the unlimited client is done in a few milliseconds.
#[test]
fn rpc_requests_are_paced_per_node() {
    let fx = Fixture::with_epoch(3);
    let (epochs, headers) = healthy_chain(&fx);
    let mock = MockRpc::start(headers, epochs, true);
    let spec = format!("paced={}", mock.url);
    let paced = Source::open_rpc(&spec, 20).unwrap();
    let started = std::time::Instant::now();
    let found = paced.epoch_numbers().unwrap();
    assert_eq!(found, vec![1, 2], "the certified records");
    let elapsed = started.elapsed();
    // six calls: latestHeader, one tip probe, records 3 and 2 for the position, records 0 and
    // 1 for the listing (2 and 3 are memoized); five gaps of 50 ms between them
    assert!(elapsed >= std::time::Duration::from_millis(200), "{elapsed:?} for six paced calls");
    let unlimited = Source::open_rpc(&format!("free={}", mock.url), 0).unwrap();
    let started = std::time::Instant::now();
    unlimited.epoch_numbers().unwrap();
    assert!(started.elapsed() < std::time::Duration::from_millis(200));
}
