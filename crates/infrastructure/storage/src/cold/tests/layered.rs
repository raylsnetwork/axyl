//! The cold layer inside [`LayeredDatabase`]: point reads fall through mem -> db -> cold on the
//! layered handle itself, with no outer wrapper.

use std::sync::Arc;

use super::*;

/// Opens a layered DB rooted under `tmp` with the cold layer attached, alongside the shared cold
/// store for direct jar writes.
fn open_with_cold(tmp: &TempDir) -> (LayeredDatabase<MdbxDatabase>, Arc<ColdStore>) {
    let mdbx = MdbxDatabase::open(tmp.path().join("hot")).expect("open mdbx");
    let cold = Arc::new(
        ColdStore::open(&ColdConfig { dir: tmp.path().join("cold") }).expect("open cold store"),
    );
    let mut db = LayeredDatabase::open(mdbx).with_cold(Arc::clone(&cold));
    open_default_tables(&mut db).expect("open tables");
    (db, cold)
}

/// A hot miss on an archived table resolves from the cold tier; hot rows and tables with no cold
/// tier never consult it.
#[test]
fn layered_get_falls_through_to_cold_on_hot_miss() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // A batch resident in hot serves from hot.
    let hot_digest = BlockHash::repeat_byte(0x11);
    let hot_batch = batch_for(1, 0);
    db.insert::<Batches>(&hot_digest, &hot_batch).expect("insert hot batch");
    let served = db.get::<Batches>(&hot_digest).expect("get").expect("hot batch must serve");
    assert_eq!(encode(&served), encode(&hot_batch), "hot row must round-trip byte-identically");

    // A batch present only in a sealed cold jar serves through the fall-through.
    let cold_digest = BlockHash::repeat_byte(0x22);
    let cold_batch = batch_for(2, 1);
    cold.batches().begin_epoch(1, 0).expect("begin epoch");
    cold.batches().append_row(&[cold_digest.as_slice(), &encode(&cold_batch)]).expect("append row");
    cold.batches().commit().expect("commit");
    db.insert::<ColdBatchLocations>(&cold_digest, &ColdLocation { epoch: 1, row: 0 })
        .expect("insert cold location");

    let served = db.get::<Batches>(&cold_digest).expect("get").expect("cold batch must serve");
    assert_eq!(encode(&served), encode(&cold_batch), "cold row must round-trip byte-identically");

    // A miss on a table with no cold tier stays a plain miss.
    let absent = BlockHash::repeat_byte(0x99);
    assert!(db.get::<ColdBatchLocations>(&absent).expect("get").is_none());
}

/// After a hot miss, `contains_key` consults the cold tier and never promises a row `get` cannot
/// produce: an auxiliary-index entry naming an unsealed epoch answers false on both reads.
#[test]
fn layered_contains_key_falls_through_and_agrees_with_get() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // A sealed cold row answers true.
    let sealed = BlockHash::repeat_byte(0x44);
    let batch = batch_for(3, 1);
    cold.batches().begin_epoch(1, 0).expect("begin epoch");
    cold.batches().append_row(&[sealed.as_slice(), &encode(&batch)]).expect("append row");
    cold.batches().commit().expect("commit");
    db.insert::<ColdBatchLocations>(&sealed, &ColdLocation { epoch: 1, row: 0 })
        .expect("insert cold location");
    assert!(
        db.contains_key::<Batches>(&sealed).expect("contains"),
        "a sealed cold row must answer true after a hot miss"
    );

    // An index entry naming an epoch with no sealed jar row is absent on both reads.
    let unsealed = BlockHash::repeat_byte(0x55);
    db.insert::<ColdBatchLocations>(&unsealed, &ColdLocation { epoch: 7, row: 0 })
        .expect("insert cold location");
    assert!(db.get::<Batches>(&unsealed).expect("get").is_none(), "no jar holds the row");
    assert!(
        !db.contains_key::<Batches>(&unsealed).expect("contains"),
        "contains_key must not promise a batch the serve path cannot produce"
    );
}

/// A held read transaction resolves cold rows on a hot miss, for typed, containment and raw reads
/// alike, so a multi-read serve path stays on one snapshot.
#[test]
fn layered_read_txn_falls_through_to_cold() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    let digest = BlockHash::repeat_byte(0x66);
    let batch = batch_for(4, 1);
    cold.batches().begin_epoch(1, 0).expect("begin epoch");
    cold.batches().append_row(&[digest.as_slice(), &encode(&batch)]).expect("append row");
    cold.batches().commit().expect("commit");
    db.insert::<ColdBatchLocations>(&digest, &ColdLocation { epoch: 1, row: 0 })
        .expect("insert cold location");

    let tx = db.read_txn().expect("read txn");
    let served = tx.get::<Batches>(&digest).expect("get").expect("cold batch must serve");
    assert_eq!(encode(&served), encode(&batch), "cold row must round-trip byte-identically");
    assert!(tx.contains_key::<Batches>(&digest).expect("contains"), "contains_key must agree");
    let raw = tx.raw_get::<Batches>(&digest).expect("raw get").expect("raw cold bytes must serve");
    assert_eq!(
        raw.as_ref(),
        encode(&batch).as_slice(),
        "raw read must serve the jar's value bytes"
    );
}

/// `without_cold` yields a hot-only view of the same database: reads never fall through, while
/// the mem cache and writer stay shared with the tiered handle.
#[test]
fn without_cold_view_never_falls_through() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // A batch present only in cold serves through the tiered handle...
    let digest = BlockHash::repeat_byte(0x77);
    let batch = batch_for(5, 1);
    cold.batches().begin_epoch(1, 0).expect("begin epoch");
    cold.batches().append_row(&[digest.as_slice(), &encode(&batch)]).expect("append row");
    cold.batches().commit().expect("commit");
    db.insert::<ColdBatchLocations>(&digest, &ColdLocation { epoch: 1, row: 0 })
        .expect("insert cold location");
    assert!(db.get::<Batches>(&digest).expect("get").is_some(), "tiered handle must serve");

    // ...and is absent through the hot-only view, on the database and its read txn alike.
    let hot_only = db.without_cold();
    assert!(hot_only.get::<Batches>(&digest).expect("get").is_none(), "hot-only view must miss");
    assert!(!hot_only.contains_key::<Batches>(&digest).expect("contains"));
    let tx = hot_only.read_txn().expect("read txn");
    assert!(tx.get::<Batches>(&digest).expect("get").is_none(), "hot-only txn must miss");
    drop(tx);

    // The view shares the hot tiers: a write through it is visible on the tiered handle.
    let shared = BlockHash::repeat_byte(0x88);
    hot_only.insert::<Batches>(&shared, &batch_for(6, 2)).expect("insert via hot-only view");
    assert!(db.get::<Batches>(&shared).expect("get").is_some(), "mem cache and writer are shared");
}

/// A row `remove`d from hot (a mem tombstone) still serves its archived copy: hot GC of an
/// archived table must never mask cold history.
#[test]
fn removed_hot_row_still_serves_from_cold() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // The row lives in both tiers, then hot GC removes it.
    let digest = BlockHash::repeat_byte(0xAA);
    let batch = batch_for(7, 1);
    db.insert::<Batches>(&digest, &batch).expect("insert hot batch");
    cold.batches().begin_epoch(1, 0).expect("begin epoch");
    cold.batches().append_row(&[digest.as_slice(), &encode(&batch)]).expect("append row");
    cold.batches().commit().expect("commit");
    db.insert::<ColdBatchLocations>(&digest, &ColdLocation { epoch: 1, row: 0 })
        .expect("insert cold location");

    db.remove::<Batches>(&digest).expect("remove hot row");

    let served = db.get::<Batches>(&digest).expect("get").expect("archived copy must still serve");
    assert_eq!(encode(&served), encode(&batch), "cold row must round-trip byte-identically");
}

/// Archived consensus blocks resolve by block number through the layered handle, cross-checked
/// against the stored header's own number.
#[test]
fn layered_get_serves_archived_consensus_blocks() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // Seal blocks 10 and 11 of epoch 2 into the consensus jar.
    let digest = BlockHash::repeat_byte(0xBB);
    let headers = [header_for(10, 2, digest), header_for(11, 2, digest)];
    cold.consensus_blocks().begin_epoch(2, 10).expect("begin epoch");
    for header in &headers {
        cold.consensus_blocks().append_row(&[&encode(header)]).expect("append row");
    }
    cold.consensus_blocks().commit().expect("commit");

    for header in &headers {
        let served = db
            .get::<ConsensusBlocks>(&header.number)
            .expect("get")
            .expect("archived block must serve");
        assert_eq!(encode(&served), encode(header), "cold block must round-trip byte-identically");
        assert!(db.contains_key::<ConsensusBlocks>(&header.number).expect("contains"));
    }
    // A number the jar does not cover is a plain miss.
    assert!(db.get::<ConsensusBlocks>(&12).expect("get").is_none());
}
