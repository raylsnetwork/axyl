// SPDX-License-Identifier: BUSL-1.1
//! Damaged databases: the tool must never panic and must read whatever is still readable.
//!
//! Every variant is a sparse copy of one seeded node (headers 0..=5 with epoch 0 archived, one
//! batch) damaged in a specific way: truncated, zeroed or bit-flipped pages, missing or garbled
//! cold-jar files, garbled lock file. Each command is run under `catch_unwind`; a panic fails the
//! test, an error or a report is acceptable. Where MDBX itself tolerates the damage (e.g. a single
//! zeroed or bit-flipped data page), the report must still be right.
#![allow(unused_crate_dependencies)]
#![cfg(target_os = "linux")]

mod common;

use common::*;
use rayls_db_inspect::{
    node_db::{NodeDb, OpenOptions},
    report::{
        batch::{get_batch, get_tx},
        epoch::{epoch, epoch_check},
        header::{header, header_check},
        summary::summary,
        Lookup,
    },
};
use rayls_infrastructure_types::{keccak256, Bytes};
use std::{
    fs::{self, OpenOptions as FileOptions},
    io::{Seek as _, SeekFrom, Write as _},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::Command,
};

/// Deterministic pseudo-random bytes (LCG), so a failing variant reproduces.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    let mut f = FileOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(bytes).unwrap();
}

fn zero_range(path: &Path, offset: u64, len: usize) {
    write_at(path, offset, &vec![0u8; len]);
}

fn flip_bytes(path: &Path, seed: u64, count: usize, lo: u64, hi: u64) {
    let mut rng = Lcg(seed);
    for _ in 0..count {
        let off = lo + rng.next() % (hi - lo);
        let byte = (rng.next() & 0xff) as u8;
        write_at(path, off, &[byte]);
    }
}

fn truncate(path: &Path, len: u64) {
    FileOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

/// Sparse copy of a consensus-db directory (the MDBX file is a 1 GiB sparse file).
fn sparse_copy(src: &Path, dst: &Path) {
    let status =
        Command::new("cp").args(["-a", "--sparse=always"]).arg(src).arg(dst).status().expect("cp");
    assert!(status.success(), "cp --sparse failed");
    let _ = fs::remove_file(dst.join("lock"));
}

/// The datafile's page size, from the spacing of its three meta pages (each starts with the same
/// magic bytes); 4096 if they cannot be found.
fn page_size(dat: &Path) -> u64 {
    const MAGIC: [u8; 7] = [0x11, 0x4C, 0xEF, 0xBD, 0x9D, 0x65, 0x59];
    let head = fs::read(dat).map(|b| b[..b.len().min(1 << 17)].to_vec()).unwrap_or_default();
    let hits: Vec<usize> = head
        .windows(MAGIC.len())
        .enumerate()
        .filter(|(_, w)| *w == MAGIC)
        .map(|(i, _)| i)
        .collect();
    match hits.as_slice() {
        [a, b, ..] => (b - a) as u64,
        _ => 4096,
    }
}

/// Allocated extent of the datafile: MDBX pages live in the first used bytes of the sparse file.
fn used_bytes(dat: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    fs::metadata(dat).unwrap().blocks() * 512
}

fn first_cold_jar(consensus_db: &Path, segment: &str) -> Option<PathBuf> {
    let dir = consensus_db.join("cold").join(segment);
    let mut names: Vec<PathBuf> = fs::read_dir(&dir)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_none())
        .collect();
    names.sort();
    names.into_iter().next()
}

/// One damage case: name, how to damage the copy, whether it must still open.
type Variant<'a> = (&'a str, Box<dyn Fn(&Path)>, bool);

#[derive(Debug)]
#[allow(dead_code)] // the payloads are read through Debug when a variant fails
enum Outcome {
    Report,
    Error(String),
    Panic(String),
}

/// Runs every command against the damaged copy; each under `catch_unwind`.
fn exercise(
    copy: &Path,
    tx_hash: rayls_infrastructure_types::B256,
) -> Vec<(&'static str, Outcome)> {
    let spec = format!("d={}", copy.display());
    let run = |name: &'static str, f: &dyn Fn() -> eyre::Result<()>| -> (&'static str, Outcome) {
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(())) => (name, Outcome::Report),
            Ok(Err(e)) => (name, Outcome::Error(format!("{e:#}").chars().take(160).collect())),
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "non-string panic".to_owned());
                (name, Outcome::Panic(msg))
            }
        }
    };
    let open = || NodeDb::open(&spec, &OpenOptions::default());
    let mut out = Vec::new();
    out.push(run("open", &|| open().map(|_| ())));
    if matches!(out[0].1, Outcome::Error(_)) {
        // an unopenable database is an error, not a crash; nothing more to check
        return out;
    }
    out.push(run("summary", &|| summary(&[open()?]).map(|_| ())));
    out.push(run("epoch 0", &|| epoch(&[open()?], 0, true).map(|_| ())));
    out.push(run("epoch-check", &|| epoch_check(&[open()?], None, None, true).map(|_| ())));
    out.push(run("header 5", &|| header(&[open()?], 5, true).map(|_| ())));
    out.push(run("header 2 (cold)", &|| header(&[open()?], 2, true).map(|_| ())));
    out.push(run("header-check", &|| header_check(&[open()?], 5, 100).map(|_| ())));
    out.push(run("get-tx", &|| get_tx(&[open()?], tx_hash, None).map(|_| ())));
    out.push(run("get-batch (absent)", &|| {
        get_batch(&[open()?], rayls_infrastructure_types::B256::ZERO).map(|_| ())
    }));
    out
}

#[test]
fn damaged_databases_never_panic_and_stay_readable_where_mdbx_allows() {
    let fx = Fixture::new();
    let txs = signed_transactions(2);
    let tx_hash = keccak256(&txs[0]);
    let batch = fx.batch(0, 1, txs.clone());
    let digest = batch.digest();
    let a = SeededNode::new(|db| {
        let (_, headers) = seed_healthy(&fx, db);
        write_batch(db, &batch);
        let h4 = fx.header_with_batches(4, headers[3].digest(), &[digest]);
        write_header(db, &h4);
        archive_below(db, 1);
        write_header(db, &fx.header(5, h4.digest()));
    });
    let source = PathBuf::from(a.consensus_db());
    let root = tempfile::tempdir().unwrap();
    let dat_used = used_bytes(&source.join("mdbx.dat"));
    let page = page_size(&source.join("mdbx.dat"));
    let _ = Bytes::new();

    // (name, damage, must the database still open?)
    let variants: Vec<Variant<'_>> = vec![
        ("intact copy", Box::new(|_| {}), true),
        // db-inspect does not detect the page size, so a zeroed meta page 0 won't open at 16 KiB.
        (
            "meta page 0 zeroed",
            Box::new(move |d| zero_range(&d.join("mdbx.dat"), 0, page as usize)),
            false,
        ),
        (
            "meta pages 0 and 1 zeroed",
            Box::new(move |d| zero_range(&d.join("mdbx.dat"), 0, 2 * page as usize)),
            false,
        ),
        (
            "all three meta pages zeroed",
            Box::new(move |d| zero_range(&d.join("mdbx.dat"), 0, 3 * page as usize)),
            false,
        ),
        (
            "datafile truncated to half its used bytes",
            Box::new(move |d| truncate(&d.join("mdbx.dat"), dat_used / 2)),
            false,
        ),
        ("datafile truncated to zero", Box::new(|d| truncate(&d.join("mdbx.dat"), 0)), false),
        ("datafile deleted", Box::new(|d| fs::remove_file(d.join("mdbx.dat")).unwrap()), false),
        (
            "one data page zeroed",
            Box::new(move |d| zero_range(&d.join("mdbx.dat"), 4 * page, page as usize)),
            true,
        ),
        (
            "40 random bytes flipped in data pages",
            Box::new(move |d| {
                flip_bytes(&d.join("mdbx.dat"), 1, 40, 3 * page, dat_used.max(4 * page))
            }),
            true,
        ),
        (
            "400 random bytes flipped in data pages",
            Box::new(move |d| {
                flip_bytes(&d.join("mdbx.dat"), 2, 400, 3 * page, dat_used.max(4 * page))
            }),
            true,
        ),
        (
            "4000 random bytes flipped anywhere used",
            Box::new(move |d| flip_bytes(&d.join("mdbx.dat"), 3, 4000, 0, dat_used.max(4 * page))),
            false,
        ),
        (
            "datafile replaced by random bytes",
            Box::new(move |d| {
                let mut rng = Lcg(4);
                let bytes: Vec<u8> = (0..dat_used).map(|_| (rng.next() & 0xff) as u8).collect();
                fs::write(d.join("mdbx.dat"), bytes).unwrap();
            }),
            false,
        ),
        (
            "mdbx.lck garbled",
            Box::new(|d| fs::write(d.join("mdbx.lck"), b"garbage").unwrap()),
            true,
        ),
        (
            "mdbx.lck deleted",
            Box::new(|d| {
                let _ = fs::remove_file(d.join("mdbx.lck"));
            }),
            true,
        ),
        (
            "cold consensus jar data truncated",
            Box::new(|d| {
                if let Some(jar) = first_cold_jar(d, "consensus_blocks") {
                    truncate(&jar, 100);
                }
            }),
            true,
        ),
        (
            "cold consensus jar offsets deleted",
            Box::new(|d| {
                if let Some(jar) = first_cold_jar(d, "consensus_blocks") {
                    let _ = fs::remove_file(jar.with_extension("off"));
                }
            }),
            true,
        ),
        (
            "cold consensus jar config garbled",
            Box::new(|d| {
                if let Some(jar) = first_cold_jar(d, "consensus_blocks") {
                    fs::write(jar.with_extension("conf"), b"garbage").unwrap();
                }
            }),
            true,
        ),
        (
            "cold consensus jar config deleted",
            Box::new(|d| {
                if let Some(jar) = first_cold_jar(d, "consensus_blocks") {
                    let _ = fs::remove_file(jar.with_extension("conf"));
                }
            }),
            true,
        ),
        (
            "cold batches jar data zeroed",
            Box::new(|d| {
                if let Some(jar) = first_cold_jar(d, "batches") {
                    let len = fs::metadata(&jar).unwrap().len();
                    zero_range(&jar, 0, len as usize);
                }
            }),
            true,
        ),
        (
            "cold directory deleted",
            Box::new(|d| {
                let _ = fs::remove_dir_all(d.join("cold"));
            }),
            true,
        ),
        (
            "cold directory replaced by a file",
            Box::new(|d| {
                let _ = fs::remove_dir_all(d.join("cold"));
                fs::write(d.join("cold"), b"x").unwrap();
            }),
            true,
        ),
    ];

    let mut panics = Vec::new();
    let mut unexpected_unopenable = Vec::new();
    let mut bad_meta_errors = Vec::new();
    let mut table = String::new();
    for (i, (name, damage, must_open)) in variants.iter().enumerate() {
        let copy = root.path().join(format!("v{i:02}"));
        sparse_copy(&source, &copy);
        damage(&copy);
        let outcomes = exercise(&copy, tx_hash);
        for (cmd, outcome) in &outcomes {
            table.push_str(&format!("{name:45} {cmd:20} {outcome:?}\n"));
            if let Outcome::Panic(msg) = outcome {
                panics.push(format!("{name} / {cmd}: {msg}"));
            }
        }
        if *must_open && matches!(outcomes[0].1, Outcome::Error(_)) {
            unexpected_unopenable.push(format!("{name}: {:?}", outcomes[0].1));
        }
        // Meta-page damage must fail to open with a clean, specific error, never a panic.
        if name.contains("meta page") {
            let clean =
                matches!(&outcomes[0].1, Outcome::Error(e) if e.contains("not an MDBX file"));
            if !clean {
                bad_meta_errors.push(format!("{name}: {:?}", outcomes[0].1));
            }
        }
    }
    eprintln!("{table}");
    assert!(panics.is_empty(), "panics on damaged databases:\n{}", panics.join("\n"));

    // one unreadable node does not abort a check over several: it is reported as such
    let good = root.path().join("good");
    let bad = root.path().join("bad");
    sparse_copy(&source, &good);
    sparse_copy(&source, &bad);
    if let Some(jar) = first_cold_jar(&bad, "consensus_blocks") {
        truncate(&jar, 100);
    }
    let open = |p: &Path| {
        NodeDb::open(
            &format!("{}={}", p.file_name().unwrap().to_string_lossy(), p.display()),
            &OpenOptions::default(),
        )
        .unwrap()
    };
    let r = header_check(&[open(&good), open(&bad)], 5, 100).unwrap();
    assert!(r.nodes[0].ok, "the intact node is checked: {}", r.nodes[0].stopped);
    assert!(r.nodes[1].error.is_some(), "the damaged node is reported unreadable");
    assert_eq!(r.verdict.code, "BROKEN");
    assert!(r.verdict.to_string().contains("unreadable=1"), "{}", r.verdict);
    assert!(
        unexpected_unopenable.is_empty(),
        "databases MDBX should still open:\n{}",
        unexpected_unopenable.join("\n")
    );
    assert!(
        bad_meta_errors.is_empty(),
        "meta-page damage should report a clean 'not an MDBX file' error:\n{}",
        bad_meta_errors.join("\n")
    );
}

/// db-inspect reads a healthy DB across the common page sizes (4 to 64 KiB).
/// The corruption test above fuzzes bytes so it stays at one size; this one only reads intact DBs.
#[test]
fn reads_healthy_databases_at_every_page_size() {
    let fx = Fixture::new();
    let txs = signed_transactions(2);
    let tx_hash = keccak256(&txs[0]);
    let batch = fx.batch(0, 1, txs.clone());
    let digest = batch.digest();
    for page_bytes in [4096usize, 8192, 16384, 32768, 65536] {
        let node = SeededNode::new_with_page_size(page_bytes, |db| {
            let (_, headers) = seed_healthy(&fx, db);
            write_batch(db, &batch);
            let h4 = fx.header_with_batches(4, headers[3].digest(), &[digest]);
            write_header(db, &h4);
            archive_below(db, 1);
            write_header(db, &fx.header(5, h4.digest()));
        });

        // Open a fresh handle per command, like the CLI does.
        let db = || [node.open("d")];
        summary(&db()).unwrap_or_else(|e| panic!("summary at {page_bytes}B page: {e:#}"));
        epoch(&db(), 0, true).unwrap_or_else(|e| panic!("epoch 0 at {page_bytes}B page: {e:#}"));
        epoch_check(&db(), None, None, true)
            .unwrap_or_else(|e| panic!("epoch-check at {page_bytes}B page: {e:#}"));
        header_check(&db(), 5, 100)
            .unwrap_or_else(|e| panic!("header-check at {page_bytes}B page: {e:#}"));

        // The reads must find the seeded data, not just run without error.
        let h2 = header(&db(), 2, true)
            .unwrap_or_else(|e| panic!("header 2 (cold) at {page_bytes}B page: {e:#}"));
        assert_eq!(
            h2.nodes[0].lookup,
            Lookup::Found,
            "cold header 2 missing at {page_bytes}B page"
        );
        let h5 = header(&db(), 5, true)
            .unwrap_or_else(|e| panic!("header 5 at {page_bytes}B page: {e:#}"));
        assert_eq!(h5.nodes[0].lookup, Lookup::Found, "header 5 missing at {page_bytes}B page");
        let tx = get_tx(&db(), tx_hash, None)
            .unwrap_or_else(|e| panic!("get-tx at {page_bytes}B page: {e:#}"));
        assert_eq!(tx.nodes[0].lookup, Lookup::Found, "tx missing at {page_bytes}B page");
        let seen = get_batch(&db(), digest)
            .unwrap_or_else(|e| panic!("get-batch at {page_bytes}B page: {e:#}"));
        assert_eq!(seen.nodes[0].lookup, Lookup::Found, "batch missing at {page_bytes}B page");
    }
}
