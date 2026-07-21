//! Archive boundedness and serve, cutoff and pacing policy, and manual real-db drills.

use super::*;

#[test]
fn test_archive_keeps_hot_bounded_and_serves_cold() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);

    // The last epoch must stay hot; everything below the cutoff is archived.
    let cutoff: Epoch = EPOCHS - 1;
    let archived = |epoch: Epoch| epoch < cutoff;

    let numbers_below: BTreeSet<u64> =
        fixtures.iter().filter(|f| archived(f.epoch)).map(|f| f.number).collect();
    let digests_below: BTreeSet<BlockHash> =
        fixtures.iter().filter(|f| archived(f.epoch)).map(|f| f.digest).collect();

    // Baseline: every archivable row is hot before the pass (probe the bare MDBX after flush).
    let mdbx = hot.inner();
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| numbers_below.contains(n)),
        numbers_below.len()
    );
    assert_eq!(
        count_hot_rows::<Batches, _>(mdbx, |d| digests_below.contains(d)),
        digests_below.len()
    );

    // The producer runs against the layered hot DB (beneath the cold fall-through), so its reads
    // and deletes stay on the hot tier and route through the single db_run writer. Serving reads go
    // through the full stack (cold outermost).
    archive_below_epoch(&hot, db.cold(), cutoff, None).expect("archive");
    // Flush the layered write queue so the archive's deletes land in the bare MDBX we probe.
    hot.sync_persist();

    // (a) Boundedness: archived epochs leave the hot tables, the recent epoch stays.
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| numbers_below.contains(n)),
        0,
        "archived consensus blocks must be removed from hot"
    );
    assert_eq!(
        count_hot_rows::<Batches, _>(mdbx, |d| digests_below.contains(d)),
        0,
        "archived batches must be removed from hot"
    );
    let recent_blocks = (EPOCHS as u64 - 1) * BLOCKS_PER_EPOCH..EPOCHS as u64 * BLOCKS_PER_EPOCH;
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| recent_blocks.contains(n)),
        BLOCKS_PER_EPOCH as usize,
        "recent (un-archived) epoch must remain hot"
    );

    // (b) Serve not regressed: every archived row reads back byte-identically through the full
    // stack (cold outermost), proving the cold fall-through is reachable past the hot tier.
    for f in fixtures.iter().filter(|f| archived(f.epoch)) {
        let block = db
            .get::<ConsensusBlocks>(&f.number)
            .expect("read block")
            .expect("archived block must read through cold fall-through");
        assert_eq!(
            encode(&block),
            encode(&f.header),
            "cold consensus block {} must be byte-identical",
            f.number
        );

        let batch = db
            .get::<Batches>(&f.digest)
            .expect("read batch")
            .expect("archived batch must read through cold fall-through");
        assert_eq!(
            encode(&batch),
            encode(&f.batch),
            "cold batch {} must be byte-identical",
            f.number
        );
    }

    // (c) The auxiliary index and high-water reflect the archive.
    let high_water = db
        .get::<ColdArchiveHighWater>(&ARCHIVE_HIGH_WATER_KEY)
        .expect("read high water")
        .expect("high water must advance after archive");
    assert_eq!(high_water, cutoff - 1, "high water is the last fully-archived epoch");

    for f in fixtures.iter().filter(|f| archived(f.epoch)) {
        let loc: ColdLocation = db
            .get::<ColdBatchLocations>(&f.digest)
            .expect("read auxiliary index")
            .expect("auxiliary index entry must exist for archived batch");
        assert_eq!(loc.epoch, f.epoch, "auxiliary index epoch must match the batch epoch");
        // The located batch reads byte-identically straight from the jar.
        let raw =
            db.cold().read_batch_checked(f.digest, loc).expect("cold read").expect("jar present");
        assert_eq!(raw, encode(&f.batch), "jar batch bytes must match the inserted batch");
    }
}

/// A batch archived to cold and pruned from hot is served by both a `Database`-level `get` and a
/// held read txn, because `ColdTx` makes the transaction cold-aware (mem -> hot -> cold).
///
/// Pins the invariant the batch responder relies on: a single `with_read_txn(|tx| tx.get())`
/// resolves the historical batches a lagging peer requests, even though after archival those rows
/// live only in cold.
#[test]
fn archived_batch_serves_via_read_txn_and_get() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);

    let cutoff: Epoch = EPOCHS - 1;
    archive_below_epoch(&hot, db.cold(), cutoff, None).expect("archive");
    // Flush the layered write queue so the archive's deletes land in the bare hot tier.
    hot.sync_persist();

    // An archived (below-cutoff) batch: pruned from hot, present only in cold.
    let archived = fixtures.iter().find(|f| f.epoch < cutoff).expect("an archived fixture");

    // The cold-aware read txn serves it: `ColdTx` falls through to cold within the txn.
    let via_txn = db
        .with_read_txn(|tx| tx.get::<Batches>(&archived.digest))
        .expect("read txn succeeds")
        .expect("a cold-aware read txn must serve an archived batch");
    assert_eq!(encode(&via_txn), encode(&archived.batch), "txn-served bytes");

    // Database-level `get` serves it identically (it now reads through the cold-aware txn).
    let via_get = db
        .get::<Batches>(&archived.digest)
        .expect("database read succeeds")
        .expect("Database::get must serve the archived batch via cold fall-through");
    assert_eq!(encode(&via_get), encode(&archived.batch), "get-served bytes");
}

/// The EL-anchor floor bites: an anchor behind the consensus tip keeps otherwise-eligible epochs
/// hot, and a genesis anchor archives nothing (the saturating cutoff edge).
#[test]
fn archive_due_floors_cutoff_by_el_anchor() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);
    let archiver = ColdArchiver::new(hot.clone(), db.cold().clone());

    // An EL still at genesis floors the cutoff to zero: nothing seals.
    let stats = archiver.archive_due(0, None).expect("floored pass");
    assert_eq!(stats, ArchiveStats::default(), "anchor 0 must archive nothing");

    // An anchor inside epoch 1 proves epoch 0 is fully executed: exactly epoch 0 seals while
    // consensus alone (current epoch 3) would allow more.
    let stats = archiver.archive_due(1, None).expect("bitten pass");
    hot.sync_persist();
    assert_eq!(stats.epochs_sealed, 1, "only the epoch below the floored cutoff seals");

    let mdbx = hot.inner();
    let epoch0 = 0..BLOCKS_PER_EPOCH;
    let epoch1 = BLOCKS_PER_EPOCH..2 * BLOCKS_PER_EPOCH;
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| epoch0.contains(n)),
        0,
        "the epoch below the floor must seal"
    );
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| epoch1.contains(n)),
        BLOCKS_PER_EPOCH as usize,
        "the floor must keep an epoch hot though consensus has committed past it"
    );
    assert!(
        db.get::<ConsensusBlocks>(&0).expect("read sealed block").is_some(),
        "the sealed epoch still serves through cold"
    );

    // Advancing the anchor into epoch 2 unlocks exactly the next epoch.
    let stats = archiver.archive_due(2, None).expect("advanced pass");
    hot.sync_persist();
    assert_eq!(stats.epochs_sealed, 1, "the anchor advance unlocks one more epoch");
    let epoch2 = 2 * BLOCKS_PER_EPOCH..3 * BLOCKS_PER_EPOCH;
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| epoch1.contains(n)), 0);
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| epoch2.contains(n)),
        BLOCKS_PER_EPOCH as usize,
        "the floor must keep epoch 2 hot while the anchor is inside it"
    );
}

/// The just-closed epoch is due as soon as the next epoch commits and executes its first output:
/// with only epoch 0 plus the first block of epoch 1 hot (anchor inside epoch 1), epoch 0 seals
/// and the live epoch stays hot. Pins the one-epoch retention: the prior policy kept the closed
/// epoch hot for a whole extra epoch, so its rows only left hot two epochs later.
#[test]
fn seal_due_seals_prior_epoch_during_live_epoch() {
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    // Epoch 0 in full plus the first block of epoch 1: the smallest state where 0 is due.
    let seeded: Vec<Fixture> = build_fixtures()
        .into_iter()
        .filter(|f| f.epoch == 0 || (f.epoch == 1 && f.number == BLOCKS_PER_EPOCH))
        .collect();
    seed_hot(&hot, &seeded);

    let archiver = ColdArchiver::new(hot.clone(), db.cold().clone());
    let outcome = archiver.seal_due(1, || false).expect("seal pass");
    assert_eq!(outcome, SealOutcome::Sealed(0), "epoch 0 must seal during epoch 1");

    // A second pass finds nothing further due: the live epoch never seals.
    let outcome = archiver.seal_due(1, || false).expect("drained pass");
    assert_eq!(outcome, SealOutcome::Drained, "the live epoch must stay hot");

    hot.sync_persist();
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n < BLOCKS_PER_EPOCH),
        0,
        "epoch 0 rows must leave hot"
    );
    for f in seeded.iter().filter(|f| f.epoch == 0) {
        assert!(
            db.get::<Batches>(&f.digest).expect("read batch").is_some(),
            "sealed batch must keep serving through cold"
        );
    }
}

/// `max_epochs` caps how many epochs one pass seals; the resumable high-water drains the rest over
/// later passes, converging to one uncapped pass. This bounds the boundary archival pause.
#[test]
fn archive_below_epoch_caps_epochs_per_pass() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let (db, hot) = open_test_db(&tmp);
    seed_hot(&hot, &fixtures);

    // Epochs 0 and 1 are eligible under the explicit cutoff; the committed epoch stays hot.
    let cutoff = EPOCHS - 2;

    // A cap of one seals exactly one epoch; without the cap this single pass would seal both.
    let first = archive_below_epoch(&hot, db.cold(), cutoff, Some(1)).expect("first capped pass");
    hot.sync_persist();
    assert_eq!(first.epochs_sealed, 1, "a cap of one seals exactly one epoch");

    // The next pass resumes past the high-water and seals the next eligible epoch.
    let second = archive_below_epoch(&hot, db.cold(), cutoff, Some(1)).expect("second capped pass");
    hot.sync_persist();
    assert_eq!(second.epochs_sealed, 1, "the resumable high-water advances the capped passes");

    // The backlog is now drained; a further (uncapped) pass seals nothing.
    let third = archive_below_epoch(&hot, db.cold(), cutoff, None).expect("drained pass");
    hot.sync_persist();
    assert_eq!(third.epochs_sealed, 0, "nothing remains after the backlog drained");

    // The capped passes together moved the whole eligible backlog out of hot, served via cold.
    let mdbx = hot.inner();
    let below: BTreeSet<u64> =
        fixtures.iter().filter(|f| f.epoch < cutoff).map(|f| f.number).collect();
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| below.contains(n)),
        0,
        "capped passes together drain the whole backlog out of hot"
    );
    for f in fixtures.iter().filter(|f| f.epoch < cutoff) {
        assert!(
            db.get::<ConsensusBlocks>(&f.number).expect("read block").is_some(),
            "every drained block still serves through cold"
        );
    }
}

/// Sums the byte size of every file under `dir`, recursing into subdirectories.
#[allow(dead_code)]
fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_size(&path);
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
}

/// Times each epoch's archival as the node splits it: background jar seal, boundary finalize.
///
/// Manual only: point `RAYLS_COLD_TIME_DB` at a COPY of a consensus mdbx directory (opened
/// ReadWrite; every pass prunes hot rows). Each pass runs the production pair - the actor's
/// [`ColdArchiver::seal_due`] then the boundary's [`ColdArchiver::reconcile`] finalize - so one
/// printed line is one epoch, with the finalize column being the boundary pause's archival cost;
/// `RAYLS_COLD_TIME_EPOCHS` caps the pass count. The first pass includes the from-genesis resume
/// scan (the first-boot shape); later passes resume past the sealed-jar tip, the steady-state
/// shape.
#[test]
#[ignore = "manual: needs RAYLS_COLD_TIME_DB pointing at a COPY of a consensus mdbx dir"]
fn time_per_epoch_archive() {
    let Ok(hot_dir) = std::env::var("RAYLS_COLD_TIME_DB").map(PathBuf::from) else {
        eprintln!("skip: set RAYLS_COLD_TIME_DB to a copy of a consensus mdbx dir");
        return;
    };
    let passes_cap: u64 = std::env::var("RAYLS_COLD_TIME_EPOCHS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX);

    let mdbx = MdbxDatabase::open(&hot_dir).expect("open hot copy");
    let layered = LayeredDatabase::open(mdbx);
    let cfg = ColdConfig { dir: hot_dir.join("cold") };
    let mut db = ColdDatabase::open(layered, &cfg).expect("open cold");
    open_default_tables(&mut db).expect("open tables");
    let hot = db.inner().clone();

    let archiver = ColdArchiver::new(hot.clone(), db.cold().clone());
    println!("pass,epoch,blocks,archive_seconds");
    for pass in 0..passes_cap {
        let started = Instant::now();
        // Epoch::MAX anchor makes the EL floor a no-op; the cutoff is the consensus one. One
        // seal_due fully archives the epoch (seal + finalize + yielding prune).
        let outcome = archiver.seal_due(Epoch::MAX, || false).expect("archive pass");
        let archive_secs = started.elapsed().as_secs_f64();
        let SealOutcome::Sealed(epoch) = outcome else {
            println!("done: backlog drained after {pass} passes");
            break;
        };
        let blocks = db
            .cold()
            .consensus_blocks()
            .key_range_for_epoch(epoch)
            .map(|range| range.end() - range.start() + 1)
            .unwrap_or(0);
        println!("{pass},{epoch},{blocks},{archive_secs:.2}");
    }
}

/// Drives the archiver against a real consensus DB to measure compression and prove round-trips.
///
/// Manual only: point `RAYLS_COLD_VALIDATE_DB` at a COPY of a consensus mdbx directory (the
/// directory containing `mdbx.dat`; the file is opened ReadWrite, so never use the original).
/// `RAYLS_COLD_VALIDATE_COLD` overrides the cold-jar dir (default `<db>/../cold`) and
/// `RAYLS_COLD_VALIDATE_EPOCHS` caps how many epochs to archive (default 50) to bound runtime.
#[test]
#[ignore = "manual: needs RAYLS_COLD_VALIDATE_DB pointing at a COPY of a consensus mdbx dir"]
fn validate_real_db_archive() {
    let Ok(hot_dir) = std::env::var("RAYLS_COLD_VALIDATE_DB").map(PathBuf::from) else {
        eprintln!("skip: set RAYLS_COLD_VALIDATE_DB to a copy of a consensus mdbx dir");
        return;
    };
    let cold_dir = std::env::var("RAYLS_COLD_VALIDATE_COLD")
        .map(PathBuf::from)
        .unwrap_or_else(|_| hot_dir.parent().unwrap_or(&hot_dir).join("cold"));
    let archive_epochs: Epoch =
        std::env::var("RAYLS_COLD_VALIDATE_EPOCHS").ok().and_then(|s| s.parse().ok()).unwrap_or(50);

    let mdbx = MdbxDatabase::open(&hot_dir).expect("open hot copy");
    let layered = LayeredDatabase::open(mdbx);
    let cfg = ColdConfig { dir: cold_dir.clone() };
    let mut db = ColdDatabase::open(layered, &cfg).expect("open cold");
    open_default_tables(&mut db).expect("open tables");
    let hot = db.inner().clone();

    // Survey via the ConsensusBlocks table only. It is small enough to scan inside the MDBX
    // read-transaction window; the multi-GB Batches table is not, so a full scan there trips the
    // read-txn timeout and silently truncates, skewing the survey.
    let (min_epoch, max_epoch) = hot
        .with_read_txn(|tx| {
            let mut min_epoch = Epoch::MAX;
            let mut max_epoch = 0;
            for (_, header) in tx.iter::<ConsensusBlocks>() {
                let epoch = header.sub_dag.leader_epoch();
                min_epoch = min_epoch.min(epoch);
                max_epoch = max_epoch.max(epoch);
            }
            Ok((min_epoch, max_epoch))
        })
        .expect("survey");
    assert!(min_epoch <= max_epoch, "no ConsensusBlocks in the DB at {hot_dir:?}");

    // Keep the most recent epochs hot; cap the span so a quick run stays bounded.
    let cutoff = (min_epoch + archive_epochs).min(max_epoch);

    // Collect the headers below the cutoff in one header-only read txn, then derive their unique
    // batch digests. Reading batches under the same txn would hold it too long, so they come after.
    let headers_below: Vec<(u64, ConsensusHeader)> = hot
        .with_read_txn(|tx| {
            Ok(tx
                .iter::<ConsensusBlocks>()
                .filter(|(_, h)| h.sub_dag.leader_epoch() < cutoff)
                .collect())
        })
        .expect("collect headers");
    let hot_blocks_below_before = headers_below.len() as u64;
    let mut unique_digests: BTreeSet<BlockHash> = BTreeSet::new();
    for (_, header) in &headers_below {
        for cert in &header.sub_dag.certificates {
            for digest in cert.header().payload().keys() {
                unique_digests.insert(*digest);
            }
        }
    }

    // Sum the uncompressed size of each unique batch (short read txn per batch) and stash samples.
    let mut uncompressed_batch_bytes = 0u64;
    let mut present_unique = 0u64;
    let mut batch_samples: Vec<(BlockHash, Vec<u8>)> = Vec::new();
    for digest in &unique_digests {
        let batch = hot.with_read_txn(|tx| tx.get::<Batches>(digest)).expect("read batch");
        if let Some(batch) = batch {
            let bytes = encode(&batch);
            uncompressed_batch_bytes += bytes.len() as u64;
            present_unique += 1;
            if batch_samples.len() < 8 {
                batch_samples.push((*digest, bytes));
            }
        }
    }
    let block_samples: Vec<(u64, Vec<u8>)> =
        headers_below.iter().take(8).map(|(n, h)| (*n, encode(h))).collect();

    // Run the archiver against the layered hot DB and time it.
    let started = Instant::now();
    let stats = match archive_below_epoch(&hot, db.cold(), cutoff, None) {
        Ok(stats) => stats,
        Err(e) => panic!("archive failed (finding, not a test bug): {e}"),
    };
    let elapsed = started.elapsed();
    hot.sync_persist();

    // Post-archive: count hot blocks still below the cutoff (should reach 0) and size each jar dir.
    let hot_blocks_below_after = hot
        .with_read_txn(|tx| {
            Ok(tx
                .iter::<ConsensusBlocks>()
                .filter(|(_, h)| h.sub_dag.leader_epoch() < cutoff)
                .count() as u64)
        })
        .expect("post survey");
    let batches_jar_bytes = dir_size(&cold_dir.join("batches"));
    let consensus_blocks_jar_bytes = dir_size(&cold_dir.join("consensus_blocks"));

    // Prove every captured sample still reads byte-identically through the cold fall-through.
    for (number, want) in &block_samples {
        let got = db
            .get::<ConsensusBlocks>(number)
            .expect("read cold block")
            .expect("archived block must read through cold");
        assert_eq!(&encode(&got), want, "cold consensus block {number} mismatch");
    }
    for (digest, want) in &batch_samples {
        let got = db
            .get::<Batches>(digest)
            .expect("read cold batch")
            .expect("archived batch must read through cold");
        assert_eq!(&encode(&got), want, "cold batch {digest} mismatch");
    }

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let ratio = |num: u64, den: u64| if den > 0 { num as f64 / den as f64 } else { 0.0 };
    println!("\n=== real-DB cold-archive validation ===");
    println!("hot dir:              {hot_dir:?}");
    println!("epoch span:           {min_epoch}..={max_epoch}  (archived epoch < {cutoff})");
    println!("blocks below cutoff:  {hot_blocks_below_before} hot -> {hot_blocks_below_after} hot");
    println!(
        "archived:             {} epochs, {} blocks, {} unique batches",
        stats.epochs_sealed, stats.blocks_archived, stats.batches_archived
    );
    println!("unique batch digests: {} ({present_unique} present in hot)", unique_digests.len());
    println!("batches uncompressed: {:.1} MiB", mib(uncompressed_batch_bytes));
    println!(
        "batches jar (lz4):    {:.1} MiB  ({:.2}x)",
        mib(batches_jar_bytes),
        ratio(uncompressed_batch_bytes, batches_jar_bytes)
    );
    println!("consensus_blocks jar (lz4): {:.1} MiB", mib(consensus_blocks_jar_bytes));
    println!("archive wall time:    {:.1}s", elapsed.as_secs_f64());
    println!(
        "samples round-tripped: {} blocks, {} batches",
        block_samples.len(),
        batch_samples.len()
    );
    println!("=======================================\n");

    // Each unique payload is archived exactly once: no duplicate jar rows.
    assert_eq!(
        stats.batches_archived, present_unique,
        "producer must archive exactly the unique batches present in hot, with no duplicates"
    );
    assert_eq!(hot_blocks_below_after, 0, "every block below the cutoff must leave the hot tier");
}
