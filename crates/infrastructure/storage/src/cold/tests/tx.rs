//! The cold tier as a transactional backend: [`ColdTx`] and [`ColdTxMut`] speak the standard
//! `DbTx`/`DbTxMut` contracts over the jars.

use super::*;
use crate::cold::{ColdTx, ColdTxMut};

/// Opens a bare cold store rooted under `tmp`.
fn bare_cold(tmp: &TempDir) -> ColdStore {
    ColdStore::open(&ColdConfig { dir: tmp.path().join("cold") }).expect("open cold store")
}

/// A cold read transaction serves point reads, dense scans, and derived reads through the
/// standard contract; `Batches` scans stay empty by contract.
#[test]
fn cold_tx_speaks_dbtx() {
    let tmp = TempDir::new().expect("tempdir");
    let cold = bare_cold(&tmp);

    // Seal epoch 0: three blocks and one batch.
    let digest = BlockHash::repeat_byte(0x21);
    let headers: Vec<_> = (0..3).map(|n| header_for(n, 0, digest)).collect();
    cold.consensus_blocks().begin_epoch(0, 0).expect("begin");
    for header in &headers {
        cold.consensus_blocks().append_row(&[&encode(header)]).expect("append");
    }
    cold.consensus_blocks().commit().expect("commit");
    cold.batches().begin_epoch(0, 0).expect("begin");
    let batch = batch_for(0, 0);
    cold.batches().append_row(&[digest.as_slice(), &encode(&batch)]).expect("append");
    cold.batches().commit().expect("commit");

    let location = ColdLocation { epoch: 0, row: 0 };
    let tx = ColdTx::new(&cold, move |d| Ok((*d == digest).then_some(location)));

    // Point reads across both cold kinds.
    let block = tx.get::<ConsensusBlocks>(&1).expect("get").expect("block must serve");
    assert_eq!(encode(&block), encode(&headers[1]), "block round-trips byte-identically");
    assert!(tx.contains_key::<ConsensusBlocks>(&2).expect("contains"));
    let served = tx.get::<Batches>(&digest).expect("get").expect("batch must serve");
    assert_eq!(encode(&served), encode(&batch), "batch round-trips byte-identically");
    let raw = tx.raw_get::<Batches>(&digest).expect("raw get").expect("raw bytes");
    assert_eq!(raw.as_ref(), encode(&batch).as_slice(), "raw read serves the jar value bytes");

    // Scans cover the dense span; `Batches` scans stay empty by contract.
    let scanned: Vec<u64> = tx.iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(scanned, vec![0, 1, 2], "forward scan covers the sealed span");
    let sought: Vec<u64> =
        tx.skip_to::<ConsensusBlocks>(&1).expect("skip").map(|(n, _)| n).collect();
    assert_eq!(sought, vec![1, 2], "seek floors the scan");
    let descending: Vec<u64> = tx.reverse_iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(descending, vec![2, 1, 0], "reverse scan descends the span");
    assert!(tx.iter::<Batches>().next().is_none(), "batches scans stay empty by contract");

    // Derived reads.
    assert_eq!(tx.last_record::<ConsensusBlocks>().map(|(n, _)| n), Some(2));
    assert_eq!(tx.record_prior_to::<ConsensusBlocks>(&2).map(|(n, _)| n), Some(1));
    assert_eq!(tx.record_prior_to::<ConsensusBlocks>(&0), None, "nothing precedes the span");
}

/// A cold write transaction appends within the open epoch, enforces dense numbering, refuses
/// removal, and seals both segments on commit.
#[test]
fn cold_tx_mut_appends_and_seals() {
    let tmp = TempDir::new().expect("tempdir");
    let cold = bare_cold(&tmp);
    let digest = BlockHash::repeat_byte(0x31);

    let mut tx = ColdTxMut::begin(&cold, 0, 0).expect("begin epoch");
    tx.insert::<ConsensusBlocks>(&0, &header_for(0, 0, digest)).expect("insert block 0");
    tx.insert::<Batches>(&digest, &batch_for(0, 0)).expect("insert batch");
    // Dense numbering is enforced at the write boundary.
    assert!(
        tx.insert::<ConsensusBlocks>(&5, &header_for(5, 0, digest)).is_err(),
        "a numbering gap must fail closed"
    );
    tx.insert::<ConsensusBlocks>(&1, &header_for(1, 0, digest)).expect("insert block 1");
    // The cold tier is append-only, and only archived tables have a cold representation.
    assert!(tx.remove::<Batches>(&digest).is_err(), "remove has no jar representation");
    assert!(tx.clear_table::<Batches>().is_err(), "clear has no jar representation");
    assert!(
        tx.evict_persistent_batch::<Batches>(&[digest]).is_err(),
        "eviction has no jar representation"
    );
    assert!(
        tx.insert::<ColdBatchLocations>(&digest, &ColdLocation { epoch: 0, row: 0 }).is_err(),
        "a table with no cold representation must fail closed"
    );
    tx.commit().expect("commit seals both segments");

    // Sealed rows read back through a cold read transaction.
    let location = ColdLocation { epoch: 0, row: 0 };
    let read = ColdTx::new(&cold, move |d| Ok((*d == digest).then_some(location)));
    let scanned: Vec<u64> = read.iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(scanned, vec![0, 1], "committed blocks serve");
    assert!(read.get::<Batches>(&digest).expect("get").is_some(), "committed batch serves");
}

/// Dropping a cold write transaction without commit abandons its appends: nothing seals, and the
/// next begin re-seals the epoch whole.
#[test]
fn cold_tx_mut_abandon_heals_on_next_begin() {
    let tmp = TempDir::new().expect("tempdir");
    let cold = bare_cold(&tmp);

    {
        let mut tx = ColdTxMut::begin(&cold, 0, 0).expect("begin");
        tx.insert::<ConsensusBlocks>(&0, &header_for(0, 0, BlockHash::repeat_byte(0x41)))
            .expect("insert");
    }
    assert!(!cold.consensus_blocks().is_epoch_sealed(0), "abandoned appends must not seal");

    let mut tx = ColdTxMut::begin(&cold, 0, 0).expect("re-begin heals the leftovers");
    let header = header_for(0, 0, BlockHash::repeat_byte(0x42));
    tx.insert::<ConsensusBlocks>(&0, &header).expect("insert");
    tx.commit().expect("commit");
    assert!(cold.consensus_blocks().is_epoch_sealed(0), "the re-sealed epoch commits whole");
    let read = ColdTx::new(&cold, |_| Ok(None));
    let served = read.get::<ConsensusBlocks>(&0).expect("get").expect("block serves");
    assert_eq!(encode(&served), encode(&header), "the re-sealed row is the second attempt's");
}
