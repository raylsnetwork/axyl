// SPDX-License-Identifier: BUSL-1.1
//! `state-root-check`: a datadir whose sealed root matches its leaves, and one whose root
//! was sealed wrong.

#![allow(unused_crate_dependencies)]

use alloy::{
    consensus::Header,
    primitives::{B256, U256},
};
use rayls_db_inspect::report::{code, state_root::state_root_check};
use reth_db::{
    init_db,
    mdbx::DatabaseArguments,
    table::Table,
    tables,
    transaction::{DbTx as _, DbTxMut as _},
    Database as _,
};
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives_traits::{Account, StorageEntry};
use reth_provider::{providers::StaticFileProvider, StaticFileSegment, StaticFileWriter as _};
use reth_trie::{trie_cursor::noop::NoopTrieCursorFactory, StateRoot};
use reth_trie_db::DatabaseHashedCursorFactory;
use std::path::Path;

/// Writes two accounts and two storage slots as hashed leaves, then seals block 1 with either
/// the root those leaves hash to or a wrong one. Block 0 is a placeholder genesis header, since
/// a static-file segment must start at block 0.
fn build_datadir(dir: &Path, seal_wrong_root: bool, with_checkpoint: bool) -> B256 {
    let db = init_db(dir.join("db"), DatabaseArguments::default()).unwrap();
    let tx = db.tx_mut().unwrap();
    let a1 = B256::repeat_byte(0x11);
    let a2 = B256::repeat_byte(0x22);
    tx.put::<tables::HashedAccounts>(
        a1,
        Account { nonce: 1, balance: U256::from(10), bytecode_hash: None },
    )
    .unwrap();
    tx.put::<tables::HashedAccounts>(a2, Account { nonce: 2, ..Default::default() }).unwrap();
    tx.put::<tables::HashedStorages>(
        a1,
        StorageEntry { key: B256::repeat_byte(0x01), value: U256::from(7) },
    )
    .unwrap();
    tx.put::<tables::HashedStorages>(
        a1,
        StorageEntry { key: B256::repeat_byte(0x02), value: U256::from(9) },
    )
    .unwrap();
    if with_checkpoint {
        type Checkpoint = <tables::StageCheckpoints as Table>::Value;
        let checkpoint = Checkpoint { block_number: 1, ..Default::default() };
        tx.put::<tables::StageCheckpoints>("Finish".to_owned(), checkpoint).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    let root =
        StateRoot::new(NoopTrieCursorFactory::default(), DatabaseHashedCursorFactory::new(&tx))
            .root()
            .unwrap();
    drop(tx);

    let static_files =
        StaticFileProvider::<EthPrimitives>::read_write(dir.join("static_files")).unwrap();
    let mut writer = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
    let genesis = Header { number: 0, ..Default::default() };
    writer.append_header(&genesis, &genesis.hash_slow()).unwrap();
    let sealed = if seal_wrong_root { B256::repeat_byte(0xee) } else { root };
    let block1 = Header {
        number: 1,
        parent_hash: genesis.hash_slow(),
        state_root: sealed,
        ..Default::default()
    };
    writer.append_header(&block1, &block1.hash_slow()).unwrap();
    writer.commit().unwrap();
    root
}

#[test]
fn sealed_root_matching_leaves_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let root = build_datadir(dir.path(), false, true);
    let spec = format!("good={}", dir.path().display());
    let report = state_root_check(&[spec]).unwrap();
    assert_eq!(report.verdict.code, code::OK);
    assert!(report.verdict.healthy);
    let node = &report.nodes[0];
    assert_eq!(node.node, "good");
    assert_eq!(node.block_number, 1);
    assert_eq!(node.header_root, root);
    assert_eq!(node.leaves_root, root);
    assert_eq!(node.accounts, 2);
    assert_eq!(node.storage_slots, 2);
    assert!(node.matches);
}

#[test]
fn sealed_root_disagreeing_with_leaves_is_broken() {
    let dir = tempfile::tempdir().unwrap();
    let root = build_datadir(dir.path(), true, true);
    let spec = dir.path().display().to_string();
    let report = state_root_check(&[spec]).unwrap();
    assert_eq!(report.verdict.code, code::BROKEN);
    assert!(!report.verdict.healthy);
    assert_eq!(report.verdict.to_string(), "BROKEN nodes=1 mismatched=1 first=1");
    let node = &report.nodes[0];
    assert_eq!(node.header_root, B256::repeat_byte(0xee));
    assert_eq!(node.leaves_root, root);
    assert!(!node.matches);
}

/// The highest static-file header can be ahead of the hashed state, so without the `Finish`
/// checkpoint there is no safe block to compare: an error, never a guessed verdict.
#[test]
fn missing_finish_checkpoint_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    build_datadir(dir.path(), false, false);
    let err = state_root_check(&[dir.path().display().to_string()]).unwrap_err();
    assert!(format!("{err:#}").contains("no Finish stage checkpoint"), "{err:#}");
}

/// A database another handle holds (a running node) is refused, as by every other subcommand.
#[test]
fn held_database_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    build_datadir(dir.path(), false, true);
    let _held = init_db(dir.path().join("db"), DatabaseArguments::default()).unwrap();
    let err = state_root_check(&[dir.path().display().to_string()]).unwrap_err();
    assert!(format!("{err:#}").contains("stop it or use a copy"), "{err:#}");
}

#[test]
fn consensus_db_path_resolves_to_its_datadir() {
    let dir = tempfile::tempdir().unwrap();
    build_datadir(dir.path(), false, true);
    let consensus_db = dir.path().join("consensus-db");
    std::fs::create_dir_all(&consensus_db).unwrap();
    let report = state_root_check(&[consensus_db.display().to_string()]).unwrap();
    assert_eq!(report.verdict.code, code::OK);
    assert_eq!(report.nodes[0].path, dir.path().display().to_string());
}

#[test]
fn wrong_path_and_duplicate_datadir_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let err = state_root_check(&[dir.path().display().to_string()]).unwrap_err();
    assert!(err.to_string().contains("no db/mdbx.dat"), "{err}");

    build_datadir(dir.path(), false, true);
    let spec = dir.path().display().to_string();
    let err = state_root_check(&[format!("a={spec}"), format!("b={spec}")]).unwrap_err();
    assert!(err.to_string().contains("given twice"), "{err}");
}

#[test]
fn duplicate_label_is_an_error() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    build_datadir(a.path(), false, true);
    build_datadir(b.path(), false, true);
    let specs = [format!("n1={}", a.path().display()), format!("n1={}", b.path().display())];
    let err = state_root_check(&specs).unwrap_err();
    assert!(err.to_string().contains("node label n1 used twice"), "{err}");
}
