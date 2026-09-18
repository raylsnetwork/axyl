// SPDX-License-Identifier: BUSL-1.1
//! `snapshot --to DIR`.
#![allow(unused_crate_dependencies)]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::{BatchLookup, LiveStatus, NodeDb, OpenOptions, Tier},
    report::snapshot::snapshot,
};
use rayls_infrastructure_types::Database as _;
use std::path::PathBuf;

/// A node with epoch 0 archived and the chain continued; its snapshot opens read-only with no
/// recovery, holds the same tiers, carries the marker, and never copies the node's lock files.
#[test]
fn snapshot_copies_a_consistent_database_with_its_sealed_jars() {
    let fx = Fixture::new();
    let batch = fx.batch(0, 1, signed_transactions(1));
    let digest = batch.digest();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        // header 4 commits an epoch-0 batch, so both segments get a jar when epoch 0 is sealed
        write_batch(db, &batch);
        let h4 = fx.header_with_batches(4, headers[3].digest(), &[digest]);
        write_header(db, &h4);
        archive_below(db, 1);
        write_header(db, &fx.header(5, h4.digest()));
    });
    let source = a.open("a");
    let dest = tempfile::tempdir().unwrap();
    let to = dest.path().join("snap");

    let r = snapshot(&source, &to).unwrap();
    assert!(!r.recovered_copy, "a readable source is copied by MDBX itself");
    assert_eq!(r.copy.latest_consensus_number, Some(5));
    assert_eq!(r.copy.live, LiveStatus::Stopped);
    assert_eq!(r.cold_epochs, vec![0]);
    assert_eq!(r.cold_files, 6, "data, offsets and config of both segments' jars");
    assert!(r.mdbx_bytes > 0 && r.cold_bytes > 0);
    assert!(!to.join("lock").exists(), "the node's lock file is never copied");

    let copy = NodeDb::open(
        &format!("c={}", to.display()),
        &OpenOptions { exclusive: true, ..Default::default() },
    )
    .expect("a snapshot opens without --recover");
    assert_eq!(copy.header(2).unwrap().map(|(_, t)| t), Some(Tier::Cold));
    assert_eq!(copy.header(5).unwrap().map(|(_, t)| t), Some(Tier::Hot));
    assert_eq!(copy.latest_consensus_number().unwrap(), Some(5));
    assert!(
        matches!(copy.batch(digest).unwrap(), BatchLookup::Found(_, Tier::Cold)),
        "the archived batch is readable from the copy's jar"
    );
    drop(copy);

    // a destination beside the node's datadir (same parent) is fine: only nesting is refused
    let sibling = PathBuf::from(a.datadir()).parent().unwrap().join("snap-sibling");
    snapshot(&source, &sibling).expect("a sibling of the datadir is outside the source");
    std::fs::remove_dir_all(&sibling).unwrap();

    // refusals: a non-empty destination, one inside the source, and a plain file
    assert!(snapshot(&source, &to).is_err(), "destination is not empty");
    let inside = PathBuf::from(a.consensus_db()).join("snap");
    assert!(snapshot(&source, &inside).is_err(), "inside the source");
    assert!(!inside.exists(), "nothing is created on a refusal");
    let file = dest.path().join("a-file");
    std::fs::write(&file, b"x").unwrap();
    assert!(snapshot(&source, &file).is_err(), "not a directory");
}

/// The summary of any directory says when its data ends: the tip header's commit time.
#[test]
fn summary_dates_the_tip() {
    let fx = Fixture::new();
    let a = SeededNode::new(|db| {
        seed_healthy(&fx, db);
    });
    let db = a.open("a");
    let tip = db.latest_consensus_header().unwrap().expect("a tip");
    let s = rayls_db_inspect::report::summary::summary(std::slice::from_ref(&db)).unwrap();
    assert_eq!(s.nodes[0].latest_consensus_timestamp, Some(tip.sub_dag.commit_timestamp()));
}
