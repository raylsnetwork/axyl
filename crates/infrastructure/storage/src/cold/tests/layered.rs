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

/// A full `iter` over an archived dense table is one ordered stream across the cold jars and the
/// hot tail: every row exactly once, byte-identical, jar boundaries invisible.
#[test]
fn layered_iter_spans_cold_jars_and_hot_tail() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);

    // Three epochs archive into three jars; the recent epoch stays hot.
    let cutoff: Epoch = EPOCHS - 1;
    archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None).expect("archive");
    hot.sync_persist().expect("persist");

    let scanned: Vec<(u64, Vec<u8>)> =
        db.iter::<ConsensusBlocks>().map(|(n, h)| (n, encode(&h))).collect();
    let expected: Vec<(u64, Vec<u8>)> =
        fixtures.iter().map(|f| (f.number, encode(&f.header))).collect();
    assert_eq!(scanned, expected, "the scan must span cold and hot, in order, each row once");
}

/// Archives the standard fixtures and returns the tiered handle plus its hot view.
///
/// Three epochs seal into three jars; the recent epoch stays hot.
fn archived_db(tmp: &TempDir, fixtures: &[Fixture]) -> (TestDb, HotDb) {
    let (db, hot) = open_test_db(tmp);
    seed_hot(&hot, fixtures);
    let cutoff: Epoch = EPOCHS - 1;
    archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None).expect("archive");
    hot.sync_persist().expect("persist");
    (db, hot)
}

/// A held read transaction's scans merge cold and hot like the database-level scans: forward,
/// seek, raw seek and reverse all span the tiers on one snapshot, and the derived
/// `last_record`/`record_prior_to` agree.
#[test]
fn layered_txn_scans_span_tiers() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;

    let tx = db.read_txn().expect("read txn");

    let scanned: Vec<u64> = tx.iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(scanned, (0..total).collect::<Vec<_>>(), "forward txn scan spans the tiers");

    let floor = 3u64;
    let sought: Vec<u64> =
        tx.skip_to::<ConsensusBlocks>(&floor).expect("skip").map(|(n, _)| n).collect();
    assert_eq!(sought, (floor..total).collect::<Vec<_>>(), "txn seek starts inside cold");

    let raw_keys: Vec<Vec<u8>> = tx
        .raw_skip_to::<ConsensusBlocks>(&floor)
        .expect("raw skip")
        .map(|(k, _)| k.into_owned())
        .collect();
    let expected_keys: Vec<Vec<u8>> =
        (floor..total).map(|n| rayls_infrastructure_types::encode_key(&n)).collect();
    assert_eq!(raw_keys, expected_keys, "raw txn seek agrees with the typed one");

    let descending: Vec<u64> = tx.reverse_iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(descending, (0..total).rev().collect::<Vec<_>>(), "reverse txn scan descends");

    assert_eq!(tx.last_record::<ConsensusBlocks>().map(|(n, _)| n), Some(total - 1));
    let first_hot = (EPOCHS as u64 - 1) * BLOCKS_PER_EPOCH;
    assert_eq!(
        tx.record_prior_to::<ConsensusBlocks>(&first_hot).map(|(n, _)| n),
        Some(first_hot - 1),
        "the prior of the first hot row crosses into cold"
    );
}

/// A scan created before an archival pass still drains every number exactly once: the cold span
/// is captured at creation and the hot side reads its own snapshot, so a seal-and-prune landing
/// mid-scan can neither drop nor duplicate the rows it moves between tiers.
#[test]
fn scan_created_before_archival_drains_complete() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);

    // First pass archives epoch 0 only, so the scan starts over two populated tiers.
    archive_below_epoch(&hot, db.cold().expect("cold attached"), 1, None).expect("first pass");
    hot.sync_persist().expect("persist");

    let mut scan = db.iter::<ConsensusBlocks>();
    // Drain a prefix, leaving the scan parked before rows the next pass moves to cold.
    let head: Vec<u64> = scan.by_ref().take(3).map(|(n, _)| n).collect();
    assert_eq!(head, vec![0, 1, 2], "the prefix reads from the first jar");

    // Mid-scan, the second pass archives epochs 1 and 2 and prunes them from hot.
    archive_below_epoch(&hot, db.cold().expect("cold attached"), EPOCHS - 1, None)
        .expect("second pass");
    hot.sync_persist().expect("persist");

    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;
    let tail: Vec<u64> = scan.map(|(n, _)| n).collect();
    assert_eq!(
        tail,
        (3..total).collect::<Vec<_>>(),
        "every remaining number exactly once, none lost to the mid-scan prune"
    );
}

/// `skip_to` seeks into the cold span, exactly onto a jar boundary, and into the hot tail alike;
/// a floor past the tip yields nothing.
#[test]
fn layered_skip_to_seeks_across_tiers() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;

    let floors = [0u64, 2, BLOCKS_PER_EPOCH, (EPOCHS as u64 - 1) * BLOCKS_PER_EPOCH + 1, total - 1];
    for floor in floors {
        let scanned: Vec<u64> =
            db.skip_to::<ConsensusBlocks>(&floor).expect("skip").map(|(n, _)| n).collect();
        assert_eq!(scanned, (floor..total).collect::<Vec<_>>(), "seek from {floor}");
    }
    assert!(
        db.skip_to::<ConsensusBlocks>(&total).expect("skip").next().is_none(),
        "a floor past the tip yields nothing"
    );
}

/// `reverse_iter` descends from the hot tip through every jar, and `last_record` and
/// `record_prior_to` agree with the merged order, including the prior that crosses the
/// hot-to-cold boundary.
#[test]
fn layered_reverse_and_prior_cross_tiers() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;

    let descending: Vec<u64> = db.reverse_iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert_eq!(descending, (0..total).rev().collect::<Vec<_>>(), "full descending span");

    let (last, _) = db.last_record::<ConsensusBlocks>().expect("last record");
    assert_eq!(last, total - 1, "the hot tip is the last record");

    let first_hot = (EPOCHS as u64 - 1) * BLOCKS_PER_EPOCH;
    let (prior, _) = db.record_prior_to::<ConsensusBlocks>(&first_hot).expect("prior record");
    assert_eq!(prior, first_hot - 1, "the prior of the first hot row is the last archived row");
}

/// A number present in both tiers surfaces once, with the hot copy winning, so a
/// sealed-but-unpruned window never duplicates rows.
#[test]
fn layered_iter_yields_overlapping_row_once_hot_wins() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;

    // Re-insert an archived number hot with a distinguishable payload.
    let overlapping = 1u64;
    let marker = header_for(overlapping, 0, BlockHash::repeat_byte(0xEE));
    db.insert::<ConsensusBlocks>(&overlapping, &marker).expect("insert hot copy");

    let scanned: Vec<(u64, Vec<u8>)> =
        db.iter::<ConsensusBlocks>().map(|(n, h)| (n, encode(&h))).collect();
    let numbers: Vec<u64> = scanned.iter().map(|(n, _)| *n).collect();
    assert_eq!(numbers, (0..total).collect::<Vec<_>>(), "each number exactly once");
    assert_eq!(scanned[1].1, encode(&marker), "the hot copy wins on an equal key");
}

/// `reverse_skip_to` walks backwards from an interior key across the tiers: every key at or
/// below the floor, descending, whether the floor lands in hot, on the boundary, or in cold; a
/// floor past the tip walks the whole table.
#[test]
fn layered_reverse_skip_to_walks_back_across_tiers() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;
    let first_hot = (EPOCHS as u64 - 1) * BLOCKS_PER_EPOCH;

    let floors = [total + 5, total - 1, first_hot, first_hot - 1, BLOCKS_PER_EPOCH, 3, 0];
    for floor in floors {
        let walked: Vec<u64> = db
            .reverse_skip_to::<ConsensusBlocks>(&floor)
            .expect("reverse skip")
            .map(|(n, _)| n)
            .collect();
        let expected: Vec<u64> = (0..=floor.min(total - 1)).rev().collect();
        assert_eq!(walked, expected, "walk back from {floor}");
    }

    // The held txn walks back the same way, crossing into cold.
    let tx = db.read_txn().expect("read txn");
    let walked: Vec<u64> =
        tx.reverse_skip_to::<ConsensusBlocks>(&3).expect("reverse skip").map(|(n, _)| n).collect();
    assert_eq!(walked, vec![3, 2, 1, 0], "txn walk-back crosses into cold");
}

/// Removal interacts with the walk-back per the fall-through rules: a removed hot-only row
/// vanishes from the stream, while a removed row with an archived copy still serves from cold.
#[test]
fn reverse_skip_to_respects_tombstones_and_cold_copies() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);
    let total = EPOCHS as u64 * BLOCKS_PER_EPOCH;

    // A hot-only row (no cold copy) is removed: it must vanish from the walk-back.
    let hot_only = total - 4;
    db.remove::<ConsensusBlocks>(&hot_only).expect("remove hot-only row");
    // An archived row is removed hot: its cold copy must still serve.
    let archived = 2u64;
    db.remove::<ConsensusBlocks>(&archived).expect("remove archived row");

    let walked: Vec<u64> = db
        .reverse_skip_to::<ConsensusBlocks>(&(total - 1))
        .expect("walk back")
        .map(|(n, _)| n)
        .collect();
    let expected: Vec<u64> = (0..total).rev().filter(|n| *n != hot_only).collect();
    assert_eq!(walked, expected, "hot-only removal vanishes; the archived copy still serves");
}

/// The walk-back positions each layer instead of filtering a full reverse scan: the persistent
/// tier is stepped by positioned lookups only, never drained from the tip.
#[test]
fn reverse_skip_to_steps_instead_of_scanning() {
    let mut db = LayeredDatabase::open(crate::test_utils::TestDb::new());
    open_default_tables(&mut db).expect("open tables");
    // Sparse keys prove the stepping follows the backend's order, not arithmetic.
    for n in [1u64, 3, 5, 7] {
        db.insert::<ConsensusBlocks>(&n, &header_for(n, 0, BlockHash::repeat_byte(0xDD)))
            .expect("insert");
    }
    db.sync_persist().expect("persist");

    let walked: Vec<u64> =
        db.reverse_skip_to::<ConsensusBlocks>(&5).expect("walk back").map(|(n, _)| n).collect();
    assert_eq!(walked, vec![5, 3, 1], "walk back from an interior key over sparse keys");
    assert_eq!(
        db.inner().reverse_iters(),
        0,
        "the persistent tier must be stepped by positioned lookups, never drained from the tip"
    );
}

/// `raw_iter` yields the same merged stream as `iter`, as encoded key and value bytes.
#[test]
fn layered_raw_iter_matches_typed_iter() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);

    let typed: Vec<(Vec<u8>, Vec<u8>)> = db
        .iter::<ConsensusBlocks>()
        .map(|(n, h)| (rayls_infrastructure_types::encode_key(&n), encode(&h)))
        .collect();
    let raw: Vec<(Vec<u8>, Vec<u8>)> =
        db.raw_iter::<ConsensusBlocks>().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
    assert_eq!(raw, typed, "raw and typed scans must agree byte for byte");

    let raw_reversed: Vec<(Vec<u8>, Vec<u8>)> = db
        .reverse_raw_iter::<ConsensusBlocks>()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let mut typed_reversed = typed;
    typed_reversed.reverse();
    assert_eq!(raw_reversed, typed_reversed, "the reverse raw scan is the exact mirror");
}

/// `is_empty` reflects the merged view: a table whose only rows live in cold jars reports
/// non-empty, because that history is still readable through the handle.
#[test]
fn is_empty_sees_cold_history() {
    let tmp = TempDir::new().expect("tempdir");
    let (db, cold) = open_with_cold(&tmp);

    // Nothing anywhere: empty.
    assert!(db.is_empty::<ConsensusBlocks>(), "no rows in any tier");

    // One block sealed into a jar, no hot rows: non-empty.
    cold.consensus_blocks().begin_epoch(0, 0).expect("begin epoch");
    cold.consensus_blocks()
        .append_row(&[&encode(&header_for(0, 0, BlockHash::repeat_byte(0xCC)))])
        .expect("append row");
    cold.consensus_blocks().commit().expect("commit");
    assert!(!db.is_empty::<ConsensusBlocks>(), "cold-only history must count as data");
}

/// `Batches` has no cold key order (digest-keyed, append-ordered jars), so its iteration stays
/// hot-only by contract; archived batches remain reachable by point read.
#[test]
fn batches_iteration_stays_hot_only() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);

    let archived = fixtures.iter().find(|f| f.epoch < EPOCHS - 1).expect("an archived fixture");
    assert!(
        db.get::<Batches>(&archived.digest).expect("get").is_some(),
        "the point read serves the archived batch"
    );
    assert!(
        !db.iter::<Batches>().any(|(digest, _)| digest == archived.digest),
        "a batches scan must stay hot-only"
    );
}

/// A corrupted later jar ends the scan at its boundary: rows before the fault serve intact and
/// none is repeated or skipped, and point reads on healthy jars keep working.
#[test]
fn corrupt_jar_ends_scan_at_its_boundary() {
    let tmp = TempDir::new().expect("tempdir");
    let fixtures = build_fixtures();
    let (db, _hot) = archived_db(&tmp, &fixtures);

    // Flip bytes in the middle of the second jar's data file (epoch 1 of the three archived).
    let data = tmp.path().join("cold").join("consensus_blocks").join("epoch-0000000001");
    let mut bytes = std::fs::read(&data).expect("read jar data file");
    let mid = bytes.len() / 2;
    for byte in &mut bytes[mid / 2..mid] {
        *byte ^= 0xFF;
    }
    std::fs::write(&data, &bytes).expect("write corrupted jar data file");

    let scanned: Vec<u64> = db.iter::<ConsensusBlocks>().map(|(n, _)| n).collect();
    assert!(
        scanned.len() < (EPOCHS as u64 * BLOCKS_PER_EPOCH) as usize,
        "the scan must not serve rows past the corrupt jar"
    );
    assert_eq!(
        scanned,
        (0..scanned.len() as u64).collect::<Vec<_>>(),
        "rows before the fault serve in order, none repeated or skipped"
    );
    let healthy = db.get::<ConsensusBlocks>(&0).expect("get").expect("healthy jar row");
    assert_eq!(encode(&healthy), encode(&fixtures[0].header), "healthy jars keep serving");
}
