// SPDX-License-Identifier: BUSL-1.1
//! Opening behaviour: read-only and exclusive, held databases refused, copies, bad paths,
//! `summary`.

#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::{NodeDb, OpenOptions},
    report::summary::summary,
};
use rayls_infrastructure_config::RaylsDirs as _;
use rayls_infrastructure_storage::{open_db, EpochStore as _};
use rayls_infrastructure_types::{Database as _, B256};
use std::io::{BufRead as _, BufReader};

#[test]
fn opens_by_datadir_and_by_consensus_db_dir() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    // one handle per environment per process: open them one after the other
    let by_datadir = NodeDb::open(&a.datadir(), &OpenOptions::default()).unwrap();
    assert!(!by_datadir.recovered);
    assert!(by_datadir.epoch(1).unwrap().is_some());
    assert!(by_datadir.has_cold(), "open_db creates the cold/ directory");
    let path = by_datadir.path.clone();
    drop(by_datadir);
    let by_db = NodeDb::open(&a.consensus_db(), &OpenOptions::default()).unwrap();
    assert_eq!(by_db.path, path);
    assert!(by_db.epoch(1).unwrap().is_some());
}

#[test]
fn same_database_twice_is_rejected_up_front() {
    use clap::Parser as _;
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let cli = rayls_db_inspect::cli::Cli::try_parse_from([
        "x",
        "epoch",
        "1",
        "-d",
        &format!("x={}", a.datadir()),
        "-d",
        &format!("y={}", a.consensus_db()),
    ])
    .unwrap();
    let err = rayls_db_inspect::run(&cli).expect_err("duplicate must be rejected");
    assert!(format!("{err}").contains("given twice"), "{err}");
}

#[test]
fn wrong_path_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let err = NodeDb::open(&dir.path().display().to_string(), &OpenOptions::default())
        .expect_err("must fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("mdbx.dat"), "{msg}");
}

#[test]
fn read_only_open_does_not_write() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let dat = a.dirs.consensus_db_path().join("mdbx.dat");
    let before = std::fs::read(&dat).unwrap();
    {
        let node = a.open("a");
        assert!(node.epoch(2).unwrap().is_some());
        assert!(node.header(3).unwrap().is_some());
        drop(summary(&[node]).unwrap());
    }
    assert_eq!(std::fs::read(&dat).unwrap(), before, "datafile changed under a read-only open");
}

/// Environment variable that turns [`hold_db_helper`] into a database-holding child process.
const HOLD_ENV: &str = "RAYLS_DB_INSPECT_TEST_HOLD_DB";

/// Not a test on its own: when `HOLD_ENV` names a consensus-db directory, this re-executed test
/// binary opens it read-write the way a node does, prints `ready`, and exits when stdin closes.
/// Used to stand in for a running node, because MDBX only allows one handle per environment
/// within a process.
#[test]
fn hold_db_helper() {
    let Ok(path) = std::env::var(HOLD_ENV) else { return };
    use std::io::{BufRead as _, Write as _};
    let db = open_db(std::path::PathBuf::from(path));
    println!("ready");
    std::io::stdout().flush().unwrap();
    for line in std::io::stdin().lock().lines() {
        if line.is_err() {
            break;
        }
        let record = rayls_infrastructure_types::EpochRecord { epoch: 7, ..Default::default() };
        db.save_epoch_record(&record).unwrap();
        db.sync_persist().unwrap();
        println!("written");
        std::io::stdout().flush().unwrap();
    }
    drop(db);
}

/// Spawns this test binary as a database-holding process (see [`hold_db_helper`]) and waits until
/// it has the environment open. Returns the child and its stdin (close it to make the child exit).
fn spawn_holder(
    consensus_db: &str,
) -> (std::process::Child, std::process::ChildStdin, BufReader<std::process::ChildStdout>) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "hold_db_helper", "--nocapture", "--test-threads=1"])
        .env(HOLD_ENV, consensus_db)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    // libtest prints "test hold_db_helper ... " on the same line as the helper's first output
    let mut line = String::new();
    while !line.contains("ready") {
        line.clear();
        assert!(stdout.read_line(&mut line).unwrap() > 0, "helper exited before ready");
    }
    (child, stdin, stdout)
}

/// The tool never reads a running node: a database another process holds open is refused, with
/// and without `--recover`, and the message says what to do instead.
#[test]
fn refuses_a_database_another_process_holds() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));

    // A second process opens the database read-write and keeps it open, like a running node.
    let (mut child, stdin, _stdout) = spawn_holder(&a.consensus_db());

    for opts in [OpenOptions::default(), OpenOptions { recover: true }] {
        let err = NodeDb::open(&a.datadir(), &opts).expect_err("a held database must be refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("open in another process"), "{opts:?}: {msg}");
        assert!(msg.contains("stop it first"), "{opts:?}: {msg}");
    }

    drop(stdin);
    assert!(child.wait().unwrap().success());
    // once the holder is gone the same directory opens
    assert!(a.open("a").epoch(2).unwrap().is_some());
}

#[test]
fn recover_is_a_noop_on_a_clean_database_and_refused_while_held() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let opts = OpenOptions { recover: true };
    let node = NodeDb::open(&a.datadir(), &opts).expect("recover then open");
    assert!(node.recovered, "a recovered copy says so in summary");
    assert!(node.epoch(2).unwrap().is_some());
    drop(node);
    // while another handle holds the environment, the exclusive recovery open must fail
    let held = open_db(a.dirs.consensus_db_path());
    let err = NodeDb::open(&a.datadir(), &opts).expect_err("recover must refuse a held db");
    assert!(format!("{err:#}").contains("another process"), "{err:#}");
    drop(held);
}

#[test]
fn read_only_copy_opens_without_flags() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    // a copy made without write permission: MDBX cannot register a reader in mdbx.lck, which the
    // exclusive open never needs
    let copy = tempfile::tempdir().unwrap();
    let dst = copy.path().join("consensus-db");
    std::fs::create_dir(&dst).unwrap();
    for f in ["mdbx.dat", "mdbx.lck"] {
        std::fs::copy(a.dirs.consensus_db_path().join(f), dst.join(f)).unwrap();
    }
    let readonly = |p: &std::path::Path| {
        let mut perm = std::fs::metadata(p).unwrap().permissions();
        perm.set_readonly(true);
        std::fs::set_permissions(p, perm).unwrap();
    };
    readonly(&dst.join("mdbx.dat"));
    readonly(&dst.join("mdbx.lck"));

    let node = NodeDb::open(&copy.path().display().to_string(), &OpenOptions::default())
        .expect("read-only copy opens with no flags");
    assert!(node.epoch(2).unwrap().is_some());
    assert!(!node.recovered);
}

/// Files copied from a database while another process held it are a separate environment:
/// nobody holds the copy, so it opens (recovered when its head was unsynced).
#[test]
fn copy_of_a_held_database_opens_on_its_own() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| drop(seed_healthy(&fx, db)));
    let (mut child, stdin, _stdout) = spawn_holder(&a.consensus_db());

    let copy = tempfile::tempdir().unwrap();
    let dst = copy.path().join("consensus-db");
    std::fs::create_dir(&dst).unwrap();
    for f in ["mdbx.dat", "mdbx.lck", "lock"] {
        let src = a.dirs.consensus_db_path().join(f);
        if src.exists() {
            std::fs::copy(src, dst.join(f)).unwrap();
        }
    }
    let copied = NodeDb::open(&copy.path().display().to_string(), &OpenOptions { recover: true })
        .expect("the copy opens while the original is held");
    assert!(copied.recovered);
    assert!(copied.epoch(2).unwrap().is_some());

    drop(stdin);
    assert!(child.wait().unwrap().success());
}

#[test]
fn summary_reports_tips_tables_and_checkpoints() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        drop(seed_healthy(&fx, db));
        write_cached_header(db, &fx.header(9, B256::default()));
        write_checkpoint(db, 2);
    });
    let report = summary(&[a.open("a")]).unwrap();
    let n = &report.nodes[0];
    assert_eq!(n.node, "a");
    assert!(!n.recovered);
    assert_eq!((n.first_epoch, n.last_epoch), (Some(0), Some(2)));
    assert_eq!(n.epoch_records, 3);
    assert_eq!(n.epoch_certs, 2);
    assert_eq!(n.latest_consensus_number, Some(3));
    assert_eq!(n.latest_cached_consensus_number, Some(9));
    assert!(n.cold_tier);
    assert_eq!(n.cold_high_water_mark, None);
    assert_eq!(n.leftover_checkpoints.len(), 1);
    assert!(n.datafile_bytes > 0);
    assert_eq!(n.tables.get("consensus_block"), Some(&4));
    assert!(n.tables.contains_key("epoch_record_by_number"));
}
