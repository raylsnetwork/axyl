//! Chunked-seal shapes, cancel and retry, and jar corruption guards.

use super::*;

/// Collects every file under `dir` into a relative-path -> bytes map, recursing into
/// subdirectories, so two cold directories can be compared byte-for-byte.
fn dir_files(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    fn walk(
        root: &std::path::Path,
        dir: &std::path::Path,
        out: &mut std::collections::BTreeMap<String, Vec<u8>>,
    ) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).expect("under root").display().to_string();
                out.insert(rel, std::fs::read(&path).expect("read jar file"));
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// Seals one epoch of single-digest `layout` blocks through [`seal_epoch`] with the given chunk
/// budget in a fresh DB, seeding one batch per unique digest (numbered by its first block).
fn seal_layout(layout: &[(u64, BlockHash)], chunk_bytes: usize) -> (TempDir, TestDb) {
    let blocks: Vec<(u64, Vec<BlockHash>)> = layout.iter().map(|(n, d)| (*n, vec![*d])).collect();
    let mut batches: Vec<(BlockHash, u64)> = Vec::new();
    for (number, digest) in layout {
        if !batches.iter().any(|(seen, _)| seen == digest) {
            batches.push((*digest, *number));
        }
    }

    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    seed_chunk_blocks(&hot, &blocks, &batches);
    crate::cold::producer::seal_next_epoch(&hot, db.cold(), 1, chunk_bytes)
        .expect("seal epoch")
        .expect("one epoch below the cutoff");
    hot.sync_persist();
    (tmp, db)
}

/// A chunked seal must be indistinguishable from a single-chunk seal across digest-sharing
/// layouts and chunk budgets: identical jar files on disk, identical auxiliary-index locations,
/// and byte-identical serves. Only peak memory may differ.
///
/// The layouts cover no sharing, full sharing, sharing across chunk boundaries (a zero budget
/// puts every block in its own chunk), and a single block; the mid-size budget splits chunks at
/// layout-dependent points.
#[test]
fn chunked_seal_matches_single_chunk_across_layouts_and_budgets() {
    let layouts: &[&[(u64, BlockHash)]] = &[
        &[(0, DIGEST_A)],
        &[(0, DIGEST_A), (1, DIGEST_B), (2, DIGEST_C)],
        &[(0, DIGEST_A), (1, DIGEST_A), (2, DIGEST_A)],
        &[(0, DIGEST_A), (1, DIGEST_B), (2, DIGEST_A), (3, DIGEST_C), (4, DIGEST_B)],
    ];
    for layout in layouts {
        let (tmp_single, db_single) = seal_layout(layout, usize::MAX);
        let single_files = dir_files(&tmp_single.path().join("cold"));

        for chunk_bytes in [0, 1, 600] {
            let (tmp_chunked, db_chunked) = seal_layout(layout, chunk_bytes);
            let chunked_files = dir_files(&tmp_chunked.path().join("cold"));
            assert_eq!(
                chunked_files.keys().collect::<Vec<_>>(),
                single_files.keys().collect::<Vec<_>>(),
                "both seals must produce the same jar files (layout {layout:?}, {chunk_bytes}B)"
            );
            for (name, bytes) in &chunked_files {
                assert_eq!(
                    bytes, &single_files[name],
                    "jar file {name} must be byte-identical (layout {layout:?}, {chunk_bytes}B)"
                );
            }

            for (number, digest) in layout.iter() {
                let loc_chunked: ColdLocation =
                    db_chunked.get::<ColdBatchLocations>(digest).unwrap().expect("indexed");
                let loc_single: ColdLocation =
                    db_single.get::<ColdBatchLocations>(digest).unwrap().expect("indexed");
                assert_eq!(loc_chunked, loc_single, "digest {digest} must share a jar row");

                let batch_chunked =
                    db_chunked.get::<Batches>(digest).expect("read batch").expect("served");
                let batch_single =
                    db_single.get::<Batches>(digest).expect("read batch").expect("served");
                assert_eq!(encode(&batch_chunked), encode(&batch_single), "digest {digest} bytes");
                let block_chunked =
                    db_chunked.get::<ConsensusBlocks>(number).expect("read block").expect("served");
                let block_single =
                    db_single.get::<ConsensusBlocks>(number).expect("read block").expect("served");
                assert_eq!(encode(&block_chunked), encode(&block_single), "block {number} bytes");
            }
        }
    }
}

/// A seal that fails in a later chunk, after earlier chunks already appended to the jars, must
/// retry cleanly: the retry's `begin_epoch` heals the uncommitted appends and reseals whole.
#[test]
fn chunked_seal_retry_after_late_chunk_failure_reseals_cleanly() {
    let blocks: Vec<(u64, Vec<BlockHash>)> =
        vec![(0, vec![DIGEST_A]), (1, vec![DIGEST_B]), (2, vec![DIGEST_C])];

    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    // Seed batches A and B but not C, so a zero-budget seal appends two chunks before the third
    // chunk finds C absent from both tiers and aborts with the jars uncommitted.
    seed_chunk_blocks(&hot, &blocks, &[(DIGEST_A, 0), (DIGEST_B, 1)]);

    let result = crate::cold::producer::seal_next_epoch(&hot, db.cold(), 1, 0);
    assert!(
        matches!(result, Err(ColdError::Corruption(_))),
        "missing batch must abort the seal, got {result:?}"
    );
    assert!(!db.cold().consensus_blocks().is_epoch_sealed(0), "aborted seal must not be indexed");

    // Heal the cause and retry: begin_epoch must recover the partially-appended jars.
    hot.with_write_txn(|txn| txn.insert::<Batches>(&DIGEST_C, &batch_for(2, 0)))
        .expect("insert missing batch");
    hot.sync_persist();
    let stats = crate::cold::producer::seal_next_epoch(&hot, db.cold(), 1, 0)
        .expect("retried seal")
        .expect("epoch sealed on retry");
    hot.sync_persist();
    assert_eq!((stats.blocks_archived, stats.batches_archived, stats.epochs_sealed), (3, 3, 1));

    // The resealed epoch serves every row and the hot rows are pruned.
    for (number, digests) in &blocks {
        let block = db
            .get::<ConsensusBlocks>(number)
            .expect("read block")
            .expect("resealed block must serve through cold");
        assert_eq!(encode(&block), encode(&header_for(*number, 0, digests[0])));
        assert!(db.get::<Batches>(&digests[0]).expect("read batch").is_some());
    }
    let mdbx = hot.inner();
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| *n <= 2), 0, "hot rows pruned");
}

/// A cancelled seal pass must leave the epoch unsealed (jars uncommitted, hot rows intact), and a
/// retry must re-seal it whole, byte-identical to a never-cancelled seal.
#[test]
fn cancelled_seal_leaves_jars_uncommitted_and_reseals_whole() {
    let blocks: Vec<(u64, Vec<BlockHash>)> =
        vec![(0, vec![DIGEST_A]), (1, vec![DIGEST_B]), (2, vec![DIGEST_C])];
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    seed_chunk_blocks(&hot, &blocks, &[(DIGEST_A, 0), (DIGEST_B, 1), (DIGEST_C, 2)]);

    // Zero budget = one block per chunk; cancel at the second chunk seam, after the first chunk's
    // rows were already appended (the leftover state the retry's `begin_epoch` must heal).
    let seams = std::cell::Cell::new(0u32);
    let outcome = crate::cold::producer::seal_next_epoch_jars(&hot, db.cold(), 1, 0, &|| {
        seams.set(seams.get() + 1);
        seams.get() > 1
    })
    .expect("a cancelled pass is not an error");
    assert!(
        matches!(outcome, crate::cold::producer::JarSeal::Cancelled),
        "flag must cancel the pass"
    );
    assert!(db.cold().consensus_blocks().sealed_epochs().is_empty(), "nothing may be sealed");
    let mdbx = hot.inner();
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(mdbx, |_| true), 3, "hot rows intact");
    assert_eq!(count_hot_rows::<Batches, _>(mdbx, |_| true), 3, "hot batches intact");

    // Retry without cancelling: the epoch seals whole...
    let stats = crate::cold::producer::seal_next_epoch(&hot, db.cold(), 1, 0)
        .expect("retried seal")
        .expect("epoch sealed on retry");
    hot.sync_persist();
    assert_eq!((stats.blocks_archived, stats.batches_archived, stats.epochs_sealed), (3, 3, 1));

    // ...and its jars are byte-identical to a never-cancelled seal of the same layout.
    let (tmp_clean, _db_clean) = seal_layout(&[(0, DIGEST_A), (1, DIGEST_B), (2, DIGEST_C)], 0);
    assert_eq!(
        dir_files(&tmp.path().join("cold")),
        dir_files(&tmp_clean.path().join("cold")),
        "cancelled-then-retried jars must match a clean seal byte-for-byte"
    );
}

/// The background `seal_due` (seal + finalize + yielding prune, in one pass) must produce exactly
/// the fused `archive_due` outcome: byte-identical jars, equal auxiliary index and high-water, and
/// equally pruned hot tiers. Proves the yielding prune does not change the archived state.
#[test]
fn seal_due_fully_archives_and_matches_fused() {
    let fixtures = build_fixtures();

    let tmp_bg = TempDir::new().unwrap();
    let (db_bg, hot_bg) = open_test_db(&tmp_bg);
    seed_hot(&hot_bg, &fixtures);
    let background = ColdArchiver::new(hot_bg.clone(), db_bg.cold().clone());
    while matches!(
        background.seal_due(Epoch::MAX, || false).expect("archive pass"),
        SealOutcome::Sealed(_)
    ) {}
    hot_bg.sync_persist();

    let tmp_fused = TempDir::new().unwrap();
    let (db_fused, hot_fused) = open_test_db(&tmp_fused);
    seed_hot(&hot_fused, &fixtures);
    let fused = ColdArchiver::new(hot_fused.clone(), db_fused.cold().clone());
    let stats = fused.archive_due(Epoch::MAX, None).expect("fused archive");
    assert_eq!(stats.epochs_sealed, 3);
    hot_fused.sync_persist();

    assert_eq!(
        dir_files(&tmp_bg.path().join("cold")),
        dir_files(&tmp_fused.path().join("cold")),
        "background jars must match fused jars byte-for-byte"
    );
    let high_water = |hot: &HotDb| {
        hot.with_read_txn(|tx| tx.get::<ColdArchiveHighWater>(&ARCHIVE_HIGH_WATER_KEY))
            .expect("read high-water")
    };
    assert_eq!(high_water(&hot_bg), high_water(&hot_fused), "high-water must agree");
    for f in &fixtures {
        assert_eq!(
            db_bg.get::<ColdBatchLocations>(&f.digest).expect("bg index"),
            db_fused.get::<ColdBatchLocations>(&f.digest).expect("fused index"),
            "auxiliary index must agree for {}",
            f.digest
        );
        // Archived rows (epochs 0-2) are pruned from hot yet still serve through the fall-through.
        assert!(db_bg.get::<ConsensusBlocks>(&f.number).expect("read").is_some());
    }
    // All archived epochs' hot rows are gone on the background path exactly as on the fused path.
    let below_cutoff = |n: &u64| *n < 3 * BLOCKS_PER_EPOCH;
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(hot_bg.inner(), below_cutoff), 0, "bg pruned");
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(hot_fused.inner(), below_cutoff),
        0,
        "fused pruned"
    );
}

/// A numbering gap within an epoch must be caught at archival (rows still hot and recoverable), not
/// sealed into a misaligned jar that the arithmetic row addressing would later mis-serve.
#[test]
fn archive_rejects_non_contiguous_consensus_blocks() {
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);

    // Epoch 0 with a gap: blocks 0, 1, 3 (missing 2).
    let gapped = [
        (0u64, header_for(0, 0, BlockHash::repeat_byte(1))),
        (1u64, header_for(1, 0, BlockHash::repeat_byte(2))),
        (3u64, header_for(3, 0, BlockHash::repeat_byte(3))),
    ];
    hot.with_write_txn(|txn| {
        for (number, header) in &gapped {
            txn.insert::<ConsensusBlocks>(number, header)?;
        }
        Ok(())
    })
    .expect("seed hot");
    hot.sync_persist();

    // Archiving epoch 0 (cutoff 1) must reject the gap rather than seal a misaligned jar.
    let result = archive_below_epoch(&hot, db.cold(), 1, None);
    assert!(
        matches!(result, Err(ColdError::Corruption(_))),
        "non-contiguous epoch must surface as corruption, got {result:?}"
    );
}

/// A cold consensus_blocks read whose stored header number disagrees with the arithmetic addressing
/// (a wrong start_key, or a numbering regression) must surface as corruption, never a silent
/// wrong-header serve. The batches path already cross-checks its digest column; this is the mirror.
#[test]
fn cold_consensus_block_read_rejects_misaligned_row() {
    let tmp = TempDir::new().unwrap();
    let header = header_for(0, 0, BlockHash::repeat_byte(1));
    let header_bytes = encode(&header);

    // Root the jar at start_key 100 but append a header whose own number is 0, so reading number
    // 100 maps to row 0 (100 - 100), which holds block 0.
    let store = ColdStore::open(&ColdConfig { dir: tmp.path().join("misaligned") }).unwrap();
    store.consensus_blocks().begin_epoch(1, 100).unwrap();
    store.consensus_blocks().append_row(&[header_bytes.as_slice()]).unwrap();
    store.consensus_blocks().commit().unwrap();

    // The checked read catches the misalignment instead of serving the wrong header.
    assert!(matches!(store.read_consensus_block_checked(100), Err(ColdError::Corruption(_))));

    // A correctly-aligned jar still serves the header byte-identically.
    let aligned = ColdStore::open(&ColdConfig { dir: tmp.path().join("aligned") }).unwrap();
    aligned.consensus_blocks().begin_epoch(1, 0).unwrap();
    aligned.consensus_blocks().append_row(&[header_bytes.as_slice()]).unwrap();
    aligned.consensus_blocks().commit().unwrap();
    assert_eq!(
        aligned.read_consensus_block_checked(0).unwrap().as_deref(),
        Some(header_bytes.as_slice())
    );
}

/// A jar data file corrupted after sealing (truncated or bit-flipped by a torn write or disk
/// fault) must surface as an error or serve the original bytes on read, never panic: the
/// workspace aborts on panic, so a read-path panic is a node abort at boot or on a serve.
#[test]
fn corrupt_jar_data_file_errors_instead_of_panicking() {
    let originals: Vec<Vec<u8>> =
        (0..4u64).map(|n| encode(&header_for(n, 1, BlockHash::repeat_byte(n as u8 + 1)))).collect();
    let seal = |dir: &std::path::Path| {
        let store = ColdStore::open(&ColdConfig { dir: dir.to_path_buf() }).expect("open cold");
        store.consensus_blocks().begin_epoch(1, 0).expect("begin");
        for bytes in &originals {
            store.consensus_blocks().append_row(&[bytes.as_slice()]).expect("append");
        }
        store.consensus_blocks().commit().expect("commit");
    };
    // Reads over a corrupt jar must never panic and never serve wrong bytes; `Err`/`None` are the
    // acceptable outcomes, a byte-identical serve the only acceptable `Some`.
    let assert_reads = |store: &ColdStore| {
        for (n, want) in originals.iter().enumerate() {
            if let Ok(Some(got)) = store.read_consensus_block_checked(n as u64) {
                assert_eq!(&got, want, "corrupt jar must never silently serve wrong bytes");
            }
        }
    };
    let data_file = |dir: &std::path::Path| dir.join("consensus_blocks").join("epoch-0000000001");

    // Truncation: half the data file vanishes.
    let tmp = TempDir::new().unwrap();
    seal(tmp.path());
    let path = data_file(tmp.path());
    let len = std::fs::metadata(&path).expect("stat").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open data file")
        .set_len(len / 2)
        .expect("truncate");
    assert_reads(&ColdStore::open(&ColdConfig { dir: tmp.path().to_path_buf() }).expect("reopen"));

    // Bit flips at several offsets within the compressed payload region.
    let tmp = TempDir::new().unwrap();
    seal(tmp.path());
    let path = data_file(tmp.path());
    let mut bytes = std::fs::read(&path).expect("read data file");
    for pos in [bytes.len() / 4, bytes.len() / 2, bytes.len() * 3 / 4] {
        bytes[pos] ^= 0xFF;
    }
    std::fs::write(&path, &bytes).expect("write corrupted");
    assert_reads(&ColdStore::open(&ColdConfig { dir: tmp.path().to_path_buf() }).expect("reopen"));
}
