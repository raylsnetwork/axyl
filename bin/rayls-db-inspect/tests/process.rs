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
fn text_output_for_each_subcommand() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let db = a.datadir();
    for (args, needle) in [
        (vec!["epoch", "1"], "verdict: OK nodes=1 certified=1"),
        (vec!["epochs", "--all"], "RC"),
        (vec!["chain-check"], "0..=2  3        2"),
        (vec!["header", "2"], "consensus header 2"),
        (vec!["cert", "2"], "leader cert of header 2"),
        (vec!["walk", "header", "3"], "live=no ok (reached genesis)"),
        (vec!["summary"], "epoch_record_by_number"),
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
    for (args, needle) in [
        (vec!["epoch", "900"], "not reached (epoch 0, record 2)"),
        (vec!["header", "900"], "not reached (tip 3)"),
        (vec!["cert", "900"], "not reached (tip 3)"),
        (vec!["walk", "header", "900"], "[a] live=no not reached (tip 3)"),
        (vec!["epochs", "5", "6"], "verdict: NOT_REACHED epochs=2 not_reached=5..=6"),
        (vec!["chain-check", "--from", "9"], "verdict: EMPTY checked=0"),
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
        vec!["chain-check", "--help"],
        vec!["header", "--help"],
        vec!["cert", "--help"],
        vec!["walk", "--help"],
        vec!["walk", "header", "--help"],
        vec!["summary", "--help"],
    ] {
        let out = bin().args(&args).output().unwrap();
        assert!(out.status.success(), "{args:?}");
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage"), "{args:?}");
    }
}
