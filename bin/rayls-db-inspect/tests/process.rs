// SPDX-License-Identifier: BUSL-1.1
//! The built binary end to end: JSON output, text output, exit codes.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rayls-db-inspect"))
}

#[test]
fn json_epoch_report_and_exit_codes() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let b = SeededNode::new(|db| drop(seed_healthy(&fx, db)));

    let out = bin()
        .args([
            "--json",
            "epoch",
            "2",
            "--db",
            &format!("a={}", a.datadir()),
            "--db",
            &format!("b={}", b.consensus_db()),
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["command"], "epoch");
    assert_eq!(json["epoch"], 2);
    assert_eq!(json["verdict"]["code"], "OK");
    assert_eq!(json["verdict"]["fields"]["certified"], 2);
    assert_eq!(json["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(json["nodes"][1]["node"], "b");
    assert!(json["nodes"][0]["record"]["digest"].as_str().unwrap().starts_with("0x"));

    // unhealthy verdict -> exit 1, still valid JSON
    let out = bin().args(["--json", "epoch", "9", "-d", &a.datadir()]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["verdict"]["code"], "NOT_REACHED");
    assert_eq!(json["nodes"][0]["status"], "not-reached");
    assert_eq!(json["nodes"][0]["position"]["latest_epoch_record"], 2);

    // bad path -> exit 2 with a message on stderr
    let out = bin().args(["summary", "-d", "/nonexistent/path"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("mdbx.dat"));
}

#[test]
fn header_verbose_json_embeds_the_wire_header() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let out = bin().args(["--json", "-v", "header", "2", "-d", &a.datadir()]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let raw = &json["nodes"][0]["header"]["raw"];
    assert_eq!(raw["number"], 2, "{raw}");
    assert_eq!(raw["sub_dag"]["leader"]["header"]["round"], 3);
    assert_eq!(raw["sub_dag"]["certificates"].as_array().map(Vec::len), Some(1));
    assert!(raw["sub_dag"]["reputation_score"]["scores_per_authority"].is_object());
}

/// The JSON `command` tag is the subcommand name, for every subcommand that changed or is new.
#[test]
fn json_command_tags() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let zero = format!("0x{}", "00".repeat(32));
    for (args, tag) in [
        (vec!["header-check", "3"], "header-check"),
        (vec!["epoch-check"], "epoch-check"),
        (vec!["get-batch", zero.as_str()], "get-batch"),
        (vec!["get-tx", zero.as_str()], "get-tx"),
    ] {
        let out = bin().arg("--json").args(&args).args(["-d", &a.datadir()]).output().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(json["command"], tag, "{args:?}");
        assert!(json["nodes"].is_array() && json["verdict"]["code"].is_string(), "{args:?}");
    }
}

#[test]
fn text_output_for_each_subcommand() {
    let fx = Fixture::new();
    let txs = signed_transactions(1);
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        let digest = write_batch(db, &fx.batch(0, 1, txs.clone()));
        write_header(db, &fx.header_with_batches(4, headers[3].digest(), &[digest]));
    });
    let db = a.datadir();
    let digest = rayls_db_inspect::view::b256(&fx.batch(0, 1, txs.clone()).digest());
    let tx_hash = rayls_db_inspect::view::b256(&rayls_infrastructure_types::keccak256(&txs[0]));
    for (args, needle) in [
        (vec!["epoch", "1"], "verdict: OK nodes=1 certified=1"),
        (vec!["epochs", "--all"], "\n1      RC"),
        (vec!["epoch-check"], "0..=2  3        2"),
        (vec!["header", "2"], "consensus header 2"),
        (vec!["cert", "2"], "leader cert of header 2"),
        (vec!["get-batch", digest.as_str()], "verdict: OK nodes=1"),
        (vec!["get-batch", digest.as_str(), "-v"], "committed in  header 4 (hot)"),
        (vec!["get-tx", tx_hash.as_str()], "header 4 (hot)  1/1 hot, 0 cold (0 epochs)"),
        (vec!["get-tx", tx_hash.as_str(), "--epoch", "0"], "(batches of epoch 0)"),
        (vec!["header", "4", "-v"], "  batches:\n    0x"),
        (vec!["header", "4", "-v"], "r5 e0 by 0x"),
        (vec!["header", "2", "-v"], "  batches       none"),
        (vec!["cert", "2"], "verify verified (epoch 0, 4 keys from its record)"),
        (vec!["header", "2"], "  verify        verified (epoch 0, 4 keys from its record)"),
        (vec!["header-check", "3", "--back", "1"], "end of range  ok"),
        (vec!["header-check", "4"], "ok (reached genesis)"),
        (vec!["epoch-check", "-v"], "  0      0x"),
        (vec!["summary"], "0..=2   3        2"),
    ] {
        let out = bin().args(&args).args(["-d", &db]).output().unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "{args:?}: {}\n{stdout}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(stdout.contains(needle), "{args:?} output lacks {needle:?}:\n{stdout}");
        let expected = usize::from(args[0] != "summary");
        assert_eq!(
            stdout.lines().filter(|l| l.starts_with("verdict: ")).count(),
            expected,
            "{args:?}"
        );
    }
    // requests beyond the tip say so and show where the node is
    let unknown = format!("0x{}", "42".repeat(32));
    for (args, needle) in [
        (vec!["epoch", "900"], "not reached (epoch 0, record 2)"),
        (vec!["header", "900"], "not reached (tip 4)"),
        (vec!["cert", "900"], "not reached (tip 4)"),
        (vec!["header-check", "900"], "[a] not reached (tip 4)"),
        (vec!["get-batch", unknown.as_str()], "not found"),
        (vec!["get-batch", unknown.as_str()], "verdict: EMPTY nodes=1 not_found=1"),
        (vec!["get-tx", unknown.as_str()], "not found; scanned 1/1 hot, 0 cold (0 epochs)"),
        (
            vec!["get-tx", unknown.as_str(), "--epoch", "3"],
            "not reached (node in epoch 0), not scanned",
        ),
        (vec!["epochs", "5", "6"], "verdict: NOT_REACHED epochs=2 not_reached=5..=6"),
        (vec!["epoch-check", "--from", "9"], "verdict: EMPTY nodes=1 checked=0"),
    ] {
        let out = bin().args(&args).args(["-d", &format!("a={db}")]).output().unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        assert!(stdout.contains(needle), "{args:?} output lacks {needle:?}:\n{stdout}");
    }
}

#[test]
fn every_help_renders() {
    for args in [
        vec!["--help"],
        vec!["epoch", "--help"],
        vec!["epochs", "--help"],
        vec!["epoch-check", "--help"],
        vec!["header", "--help"],
        vec!["cert", "--help"],
        vec!["get-batch", "--help"],
        vec!["get-tx", "--help"],
        vec!["header-check", "--help"],
        vec!["summary", "--help"],
    ] {
        let out = bin().args(&args).output().unwrap();
        assert!(out.status.success(), "{args:?}");
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage"), "{args:?}");
    }
}

/// The JSON shapes scripts key on: tagged link and verification states, per-record rows.
#[test]
fn json_shapes_of_check_reports() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let out = bin().args(["--json", "-v", "epoch-check", "-d", &a.datadir()]).output().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let records = json["nodes"][0]["records"].as_array().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["link"]["state"], "genesis");
    assert_eq!(records[0]["cert"], "genesis");
    assert_eq!(records[1]["link"]["state"], "ok");
    assert_eq!(records[1]["index_ok"], true);
    assert_eq!(json["verdict"]["fields"]["nodes"], 1);

    let out = bin()
        .args(["--json", "header-check", "3", "--back", "1", "-d", &a.datadir()])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let hops = json["nodes"][0]["hops"].as_array().unwrap();
    assert_eq!(hops.len(), 2);
    assert_eq!(hops[0]["link"]["state"], "ok");
    assert_eq!(hops[1]["link"]["state"], "end");
    assert_eq!(hops[0]["verify"]["state"], "verified");
    assert_eq!(hops[0]["verify"]["keys"], 4);
    assert_eq!(json["nodes"][0]["start_missing"], false);
}
