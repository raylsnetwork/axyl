//! Crash-window heals and paced-prune/finalize interplay.

use super::*;

#[test]
fn test_reconcile_heals_interrupted_archive() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let cutoff: Epoch = EPOCHS - 1;
    let archived = |epoch: Epoch| epoch < cutoff;

    let numbers_below: BTreeSet<u64> =
        fixtures.iter().filter(|f| archived(f.epoch)).map(|f| f.number).collect();
    let digests_below: BTreeSet<BlockHash> =
        fixtures.iter().filter(|f| archived(f.epoch)).map(|f| f.digest).collect();

    // Phase 1: seed hot, archive into durable jars, then simulate a crash AFTER the jar `.conf` is
    // durable but BEFORE the hot delete + auxiliary-index/high-water mark commit. The dangerous
    // ordering (hot delete before jar durable) is impossible by construction; this is the
    // surviving crash window.
    {
        let (db, hot) = open_test_db(&tmp);
        seed_hot(&hot, &fixtures);

        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");

        // Re-create the partial state: jars sealed (kept on disk), hot rows still present,
        // auxiliary index and high-water mark rolled back as if the post-jar hot txn never
        // committed.
        hot.with_write_txn(|txn| {
            for f in fixtures.iter().filter(|f| archived(f.epoch)) {
                txn.insert::<Batches>(&f.digest, &f.batch)?;
                txn.insert::<ConsensusBlocks>(&f.number, &f.header)?;
                txn.remove::<ColdBatchLocations>(&f.digest)?;
            }
            txn.remove::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)?;
            Ok(())
        })
        .expect("simulate crash window");
        hot.sync_persist().expect("persist");

        // Sanity: the jars are durable, so no row is absent from both tiers even pre-reconcile.
        let sealed: BTreeSet<Epoch> =
            db.cold().expect("cold attached").consensus_blocks().sealed_epochs();
        for epoch in 0..cutoff {
            assert!(sealed.contains(&epoch), "epoch {epoch} jar must be durable after commit");
        }
        // Drop the process state; phase 2 reopens from disk to model a real reboot.
    }

    // Phase 2: reopen (boot), rebuilding the cold index from the on-disk `.conf` headers, then run
    // reconciliation and assert the tiers are healed.
    let (db, hot) = open_test_db(&tmp);
    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    // Flush so any reconcile hot deletes land in the bare MDBX we probe for boundedness.
    hot.sync_persist().expect("persist");

    // The auxiliary index and high-water mark are rebuilt from the self-describing jars.
    let high_water_mark = db
        .get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)
        .expect("read high-water mark")
        .expect("reconcile must restore the high-water mark from the durable jars");
    assert_eq!(high_water_mark, cutoff - 1);

    // The redundant hot rows are re-deleted so the table stays bounded.
    let mdbx = hot.inner();
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| numbers_below.contains(n)),
        0,
        "reconcile must re-delete archived consensus blocks left hot by the crash"
    );
    assert_eq!(
        count_hot_rows::<Batches, _>(mdbx, |d| digests_below.contains(d)),
        0,
        "reconcile must re-delete archived batches left hot by the crash"
    );

    // No row is ever absent from both tiers: every archived row still reads through fall-through.
    for f in fixtures.iter().filter(|f| archived(f.epoch)) {
        let block = db
            .get::<ConsensusBlocks>(&f.number)
            .expect("read block")
            .expect("archived block must read after reconcile");
        assert_eq!(encode(&block), encode(&f.header));

        let batch = db
            .get::<Batches>(&f.digest)
            .expect("read batch")
            .expect("archived batch must read after reconcile");
        assert_eq!(encode(&batch), encode(&f.batch));
    }
}

/// Deletes every on-disk file for `epoch`'s jar in the named cold segment, modelling a jar whose
/// commit never reached its durable `.conf` so it drops from the boot-rebuilt index.
fn delete_segment_epoch_files(tmp: &TempDir, segment: &str, epoch: Epoch) {
    let dir = tmp.path().join("cold").join(segment);
    let stem = format!("epoch-{epoch:010}");
    for entry in std::fs::read_dir(&dir).expect("read segment dir").flatten() {
        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&stem)) {
            std::fs::remove_file(&path).expect("remove jar file");
        }
    }
}

/// A crash between the two jar commits must leave the torn epoch re-sealable.
///
/// Batches commits before consensus_blocks, so that window leaves a durable batches jar, no
/// consensus_blocks jar, and the hot rows present. Reconcile must gate on consensus_blocks, the
/// last commit: gating on the union instead advances the high-water mark over a missing jar and
/// evicts rows that are then in neither tier.
#[test]
fn reconcile_preserves_epoch_torn_between_jar_commits() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let cutoff: Epoch = EPOCHS - 1;
    // The highest archived epoch is the realistic torn one: its seal is the most recent, so its
    // consensus_blocks commit is the one a crash interrupts.
    let torn_epoch: Epoch = cutoff - 1;

    // Phase 1: full archive, then carve the torn-epoch crash window into the on-disk state.
    {
        let (db, hot) = open_test_db(&tmp);
        seed_hot(&hot, &fixtures);
        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");

        // Model the crash between `cold.batches().commit()` and `cold.consensus_blocks().commit()`
        // for `torn_epoch`: the consensus_blocks jar never reached its durable `.conf`, so delete
        // its files (it drops from the boot-rebuilt index); the batches jar stays sealed.
        delete_segment_epoch_files(&tmp, "consensus_blocks", torn_epoch);

        // Roll the rest back to that instant: the post-jar hot txn never ran, so the torn epoch's
        // hot rows are still present, its auxiliary-index entries absent, and the high-water mark
        // still points at the last fully sealed epoch below it.
        hot.with_write_txn(|txn| {
            for f in fixtures.iter().filter(|f| f.epoch == torn_epoch) {
                txn.insert::<Batches>(&f.digest, &f.batch)?;
                txn.insert::<ConsensusBlocks>(&f.number, &f.header)?;
                txn.remove::<ColdBatchLocations>(&f.digest)?;
            }
            txn.insert::<ColdArchiveHighWaterMark>(
                &ARCHIVE_HIGH_WATER_MARK_KEY,
                &(torn_epoch - 1),
            )?;
            Ok(())
        })
        .expect("carve crash window");
        hot.sync_persist().expect("persist");
        // Drop the process state; phase 2 reopens from disk to model a real reboot.
    }

    // Phase 2: reboot (rebuild the index from the surviving `.conf` files) and reconcile.
    let (db, hot) = open_test_db(&tmp);

    // The boot scan rebuilt the index from disk: the batches jar survived but the consensus_blocks
    // jar did not, so only the union of both segments still claims the torn epoch as sealed (the
    // trap reconcile must not fall into).
    assert!(
        db.cold().expect("cold attached").batches().is_epoch_sealed(torn_epoch),
        "batches jar must survive reboot"
    );
    assert!(
        !db.cold().expect("cold attached").consensus_blocks().is_epoch_sealed(torn_epoch),
        "consensus_blocks jar must be absent after reboot"
    );

    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    // The torn epoch's rows must still resolve. With the fix they stay hot (reconcile skips the
    // epoch); against the bug both reads are None because reconcile evicted the hot rows with no
    // cold copy to fall through to.
    for f in fixtures.iter().filter(|f| f.epoch == torn_epoch) {
        let block = db
            .get::<ConsensusBlocks>(&f.number)
            .expect("read block")
            .expect("torn epoch consensus block must survive reconcile");
        assert_eq!(encode(&block), encode(&f.header));

        let batch = db
            .get::<Batches>(&f.digest)
            .expect("read batch")
            .expect("torn epoch batch must survive reconcile");
        assert_eq!(encode(&batch), encode(&f.batch));
    }
}

/// A digest several blocks in one epoch share is stored once, and reconcile must rebuild its
/// index row to match that single jar row.
///
/// The seal appends it once, so a rebuild that does not dedup identically drifts the row counter
/// and mis-maps that digest and every later one.
#[test]
fn reconcile_rebuilds_shared_digest_to_correct_row() {
    let tmp = TempDir::new().unwrap();

    // One archived epoch (0, under cutoff 1) with three blocks: two share batch digest `shared`,
    // the third holds a distinct `other`, so a one-row drift mis-serves both.
    let shared = BlockHash::repeat_byte(0xD1);
    let other = BlockHash::repeat_byte(0xE2);
    let batch_shared = batch_for(100, 0);
    let batch_other = batch_for(200, 0);
    let blocks = [
        (0u64, header_for(0, 0, shared)),
        (1u64, header_for(1, 0, shared)),
        (2u64, header_for(2, 0, other)),
    ];
    let cutoff: Epoch = 1;

    let seed = |hot: &HotDb| {
        hot.with_write_txn(|txn| {
            txn.insert::<Batches>(&shared, &batch_shared)?;
            txn.insert::<Batches>(&other, &batch_other)?;
            for (number, header) in &blocks {
                txn.insert::<ConsensusBlocks>(number, header)?;
            }
            Ok(())
        })
        .expect("seed hot");
        hot.sync_persist().expect("persist");
    };

    // Phase 1: archive into durable jars, then roll back the post-jar hot txn to model a crash
    // after the jars are durable but before the index/high-water mark commit and hot delete.
    {
        let (db, hot) = open_test_db(&tmp);
        seed(&hot);
        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");
        hot.with_write_txn(|txn| {
            txn.insert::<Batches>(&shared, &batch_shared)?;
            txn.insert::<Batches>(&other, &batch_other)?;
            for (number, header) in &blocks {
                txn.insert::<ConsensusBlocks>(number, header)?;
            }
            txn.remove::<ColdBatchLocations>(&shared)?;
            txn.remove::<ColdBatchLocations>(&other)?;
            txn.remove::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)?;
            Ok(())
        })
        .expect("simulate crash window");
        hot.sync_persist().expect("persist");
    }

    // Phase 2: reopen and reconcile, then both batches must read back through cold.
    let (db, hot) = open_test_db(&tmp);
    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    let got_shared = db
        .get::<Batches>(&shared)
        .expect("read shared")
        .expect("shared digest must resolve after reconcile");
    assert_eq!(encode(&got_shared), encode(&batch_shared), "shared digest maps to its own batch");

    let got_other = db
        .get::<Batches>(&other)
        .expect("read other")
        .expect("distinct digest must resolve after reconcile");
    assert_eq!(encode(&got_other), encode(&batch_other), "row drift must not mis-serve later rows");
}

/// A digest an earlier epoch already archived gets NO new jar row when a later epoch's blocks
/// reference it again, so replaying the block projection to rebuild the index numbers one row too
/// many and shifts that digest and every later one in the epoch. Walking the batches jar cannot
/// drift: row `i` stores the digest the seal appended at row `i`.
#[test]
fn reconcile_rebuilds_cross_epoch_shared_digest_from_the_jar() {
    let tmp = TempDir::new().unwrap();

    // Epoch 0 archives `shared`; epoch 1 references it again (already in cold, so no new row) and
    // adds `only_later` at its jar's row 0. Block 3 keeps epoch 2 as the live epoch.
    let shared = BlockHash::repeat_byte(0xD1);
    let only_later = BlockHash::repeat_byte(0xE2);
    let live = BlockHash::repeat_byte(0xF3);
    let batch_shared = batch_for(100, 0);
    let batch_only_later = batch_for(200, 1);
    let blocks = [
        (0u64, header_for(0, 0, shared)),
        (1u64, header_for(1, 1, shared)),
        (2u64, header_for(2, 1, only_later)),
        (3u64, header_for(3, 2, live)),
    ];

    {
        let (db, hot) = open_test_db(&tmp);
        hot.with_write_txn(|txn| {
            txn.insert::<Batches>(&shared, &batch_shared)?;
            txn.insert::<Batches>(&only_later, &batch_only_later)?;
            txn.insert::<Batches>(&live, &batch_for(300, 2))?;
            for (number, header) in &blocks {
                txn.insert::<ConsensusBlocks>(number, header)?;
            }
            Ok(())
        })
        .expect("seed hot");
        hot.sync_persist().expect("persist");

        // Archive one epoch at a time, flushing between: epoch 1's seal must observe `shared`
        // already gone from hot, which is what makes it skip the row.
        for cutoff in 1..=2 {
            crate::cold::producer::seal_next_epoch(
                &hot,
                db.cold().expect("cold attached"),
                cutoff,
                usize::MAX,
            )
            .expect("seal")
            .expect("one epoch below the cutoff");
            hot.sync_persist().expect("persist");
        }

        // Roll back the index and high-water mark, the crash window boot reconcile rebuilds from.
        hot.with_write_txn(|txn| {
            txn.remove::<ColdBatchLocations>(&shared)?;
            txn.remove::<ColdBatchLocations>(&only_later)?;
            txn.remove::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)?;
            Ok(())
        })
        .expect("simulate crash window");
        hot.sync_persist().expect("persist");
    }

    let (db, hot) = open_test_db(&tmp);
    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    // Against the drift, `shared` is re-pointed at epoch 1 row 0 (which holds `only_later`) and
    // `only_later` at a row epoch 1's jar does not have.
    assert_eq!(
        db.get::<ColdBatchLocations>(&shared).expect("read index"),
        Some(ColdLocation { epoch: 0, row: 0 }),
        "a digest archived by an earlier epoch keeps that epoch's row"
    );
    let got_shared = db
        .get::<Batches>(&shared)
        .expect("read shared")
        .expect("shared digest must resolve after reconcile");
    assert_eq!(encode(&got_shared), encode(&batch_shared));
    let got_only_later = db
        .get::<Batches>(&only_later)
        .expect("read only_later")
        .expect("later digest must resolve after reconcile");
    assert_eq!(encode(&got_only_later), encode(&batch_only_later));
}

/// A batches jar's start key is an unused sentinel, so the rebuilt [`ColdLocation::row`] must be
/// the row index alone. Sealing one at a non-zero start key pins that: any `start_key + row`
/// mapping leaking into the rebuild is invisible while the seal happens to root batches jars at 0.
#[test]
fn reconcile_rebuilds_rows_independently_of_the_batches_start_key() {
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);

    let digests = [BlockHash::repeat_byte(0xA7), BlockHash::repeat_byte(0xB8)];
    let batches: Vec<Batch> = (0..2).map(|n| batch_for(100 + n, 0)).collect();
    hot.with_write_txn(|txn| {
        for (digest, batch) in digests.iter().zip(&batches) {
            txn.insert::<Batches>(digest, batch)?;
        }
        txn.insert::<ConsensusBlocks>(&0, &header_for(0, 0, digests[0]))?;
        Ok(())
    })
    .expect("seed hot");
    hot.sync_persist().expect("persist");

    // Seal epoch 0 by hand so the batches jar is rooted at a non-zero sentinel; the seal path
    // always passes 0, which is exactly what would hide a start-key-based row mapping.
    db.cold().expect("cold attached").batches().begin_epoch(0, 500).expect("begin batches");
    for (digest, batch) in digests.iter().zip(&batches) {
        db.cold()
            .expect("cold attached")
            .batches()
            .append_row(&[digest.as_slice(), &encode(batch)])
            .expect("append batch");
    }
    db.cold().expect("cold attached").batches().commit().expect("commit batches");
    db.cold().expect("cold attached").consensus_blocks().begin_epoch(0, 0).expect("begin blocks");
    db.cold()
        .expect("cold attached")
        .consensus_blocks()
        .append_row(&[&encode(&header_for(0, 0, digests[0]))])
        .expect("append block");
    db.cold().expect("cold attached").consensus_blocks().commit().expect("commit blocks");

    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    for (row, (digest, batch)) in digests.iter().zip(&batches).enumerate() {
        assert_eq!(
            db.get::<ColdBatchLocations>(digest).expect("read index"),
            Some(ColdLocation { epoch: 0, row: row as u64 }),
            "row must be the jar row index, not the jar start key plus it"
        );
        let served = db.get::<Batches>(digest).expect("read batch").expect("batch must serve");
        assert_eq!(encode(&served), encode(batch));
    }
}

/// A consensus DB restored without its `cold/` directory keeps a high-water mark and a fully
/// populated auxiliary index while every jar is gone, so each archived read resolves to neither
/// tier. Boot reconcile drives off the sealed jars, so it finds nothing to do; it must assert
/// coverage and refuse to boot rather than serve the hole.
#[test]
fn reconcile_rejects_a_high_water_mark_the_jars_do_not_cover() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let cutoff: Epoch = EPOCHS - 1;

    {
        let (db, hot) = open_test_db(&tmp);
        seed_hot(&hot, &fixtures);
        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");
        hot.sync_persist().expect("persist");
    }
    std::fs::remove_dir_all(tmp.path().join("cold")).expect("restore without the cold directory");

    let (db, hot) = open_test_db(&tmp);
    assert!(
        db.cold().expect("cold attached").consensus_blocks().sealed_epochs().is_empty(),
        "no jar may survive"
    );
    assert!(
        db.get::<ConsensusBlocks>(&0).expect("read block").is_none(),
        "the archived rows really are absent from both tiers"
    );

    let healed = reconcile(&hot, db.cold().expect("cold attached"));
    assert!(
        matches!(healed, Err(ColdError::Corruption(_))),
        "reconcile must fail closed on a high-water mark no jar covers, got {healed:?}"
    );
}

/// The durable residue of a seal whose index txn was lost while its hot prune landed (the layered
/// writer surfaces a failed commit only through `sync_persist`; callers must treat it as fatal):
/// jars durable, hot rows pruned, auxiliary index and high-water mark absent. Boot reconcile must
/// rebuild the index from the jars so every archived row still serves.
#[test]
fn reconcile_restores_pruned_but_unindexed_epochs() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let cutoff: Epoch = EPOCHS - 1;
    let archived = |epoch: Epoch| epoch < cutoff;

    {
        let (db, hot) = open_test_db(&tmp);
        seed_hot(&hot, &fixtures);
        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");
        // Carve the residue: the index/high-water mark txn rolls back while the (later) prune
        // stays.
        hot.with_write_txn(|txn| {
            for f in fixtures.iter().filter(|f| archived(f.epoch)) {
                txn.remove::<ColdBatchLocations>(&f.digest)?;
            }
            txn.remove::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)?;
            Ok(())
        })
        .expect("carve residue");
        hot.sync_persist().expect("persist");
    }

    let (db, hot) = open_test_db(&tmp);
    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    let high_water_mark = db
        .get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)
        .expect("read high-water mark")
        .expect("reconcile must restore the high-water mark");
    assert_eq!(high_water_mark, cutoff - 1);
    for f in fixtures.iter().filter(|f| archived(f.epoch)) {
        let batch = db
            .get::<Batches>(&f.digest)
            .expect("read batch")
            .expect("batch must serve with the index rebuilt from the jars");
        assert_eq!(encode(&batch), encode(&f.batch));
        assert!(db.get::<ConsensusBlocks>(&f.number).expect("read block").is_some());
    }
}

/// An epoch at or below the high-water mark is skipped outright, with no probe and no work.
///
/// That skip is what keeps boot O(sealed epochs) instead of a hot read per archived epoch, and it
/// is sound only because the high-water mark advances after a completed prune. Pinning it also pins
/// the trade: rows reappearing below the high-water mark are never swept, which no production path
/// does.
#[test]
fn reconcile_skips_epochs_at_or_below_the_high_water_mark() {
    let tmp = TempDir::new().unwrap();
    let fixtures = build_fixtures();
    let cutoff: Epoch = EPOCHS - 1;
    // The last archived epoch is the persisted high-water mark.
    let high_water_mark_epoch = cutoff - 1;

    {
        let (db, hot) = open_test_db(&tmp);
        seed_hot(&hot, &fixtures);
        archive_below_epoch(&hot, db.cold().expect("cold attached"), cutoff, None)
            .expect("archive");
        // Re-insert only the high-water mark epoch's rows: the index and high-water mark stay
        // committed, so the state is exactly the crash window between the high-water mark
        // txn and the delete.
        hot.with_write_txn(|txn| {
            for f in fixtures.iter().filter(|f| f.epoch == high_water_mark_epoch) {
                txn.insert::<Batches>(&f.digest, &f.batch)?;
                txn.insert::<ConsensusBlocks>(&f.number, &f.header)?;
            }
            Ok(())
        })
        .expect("carve crash window");
        hot.sync_persist().expect("persist");
    }

    let (db, hot) = open_test_db(&tmp);
    reconcile(&hot, db.cold().expect("cold attached")).expect("reconcile");
    hot.sync_persist().expect("persist");

    let mdbx = hot.inner();
    let numbers: BTreeSet<u64> =
        fixtures.iter().filter(|f| f.epoch == high_water_mark_epoch).map(|f| f.number).collect();
    let digests: BTreeSet<BlockHash> =
        fixtures.iter().filter(|f| f.epoch == high_water_mark_epoch).map(|f| f.digest).collect();
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(mdbx, |n| numbers.contains(n)),
        numbers.len(),
        "an epoch at the high-water mark is skipped, so reconcile leaves its rows untouched"
    );
    assert_eq!(
        count_hot_rows::<Batches, _>(mdbx, |d| digests.contains(d)),
        digests.len(),
        "an epoch at the high-water mark is skipped, so reconcile leaves its batches untouched"
    );
    for f in fixtures.iter().filter(|f| f.epoch == high_water_mark_epoch) {
        let batch = db.get::<Batches>(&f.digest).expect("read batch").expect("row must serve");
        assert_eq!(encode(&batch), encode(&f.batch));
        assert!(db.get::<ConsensusBlocks>(&f.number).expect("read block").is_some());
    }
}

/// The boundary finalize (reconcile) must never open a serve gap: while it migrates a sealed
/// epoch from hot rows to the cold index, a reader hammering the fall-through finds every
/// archived row in at least one tier (the index+high-water mark commit lands strictly before the
/// hot prune).
#[test]
fn finalize_never_gaps_reads_through_fall_through() {
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);

    // One sealed-but-unfinalized epoch (the seal actor's steady state entering a boundary),
    // sized so the finalize's rebuild+prune spans a window the reader thread can actually race.
    let blocks: Vec<(u64, Vec<BlockHash>)> =
        (0..200).map(|n| (n, vec![BlockHash::from(digest_seed(n, 0))])).collect();
    let batches: Vec<(BlockHash, u64)> = blocks.iter().map(|(n, d)| (d[0], *n)).collect();
    seed_chunk_blocks(&hot, &blocks, &batches);
    let outcome = crate::cold::producer::seal_next_epoch_jars(
        &hot,
        db.cold().expect("cold attached"),
        1,
        usize::MAX,
        &|| false,
    )
    .expect("seal jars");
    assert!(matches!(outcome, crate::cold::producer::JarSeal::Sealed { .. }), "epoch must seal");

    // Reader thread: every row must resolve through mem -> hot -> cold at every instant.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let db = db.clone();
        let stop = std::sync::Arc::clone(&stop);
        let blocks = blocks.clone();
        std::thread::spawn(move || {
            let mut gaps = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for (number, digests) in &blocks {
                    if db.get::<ConsensusBlocks>(number).expect("read block").is_none() {
                        gaps += 1;
                    }
                    if db.get::<Batches>(&digests[0]).expect("read batch").is_none() {
                        gaps += 1;
                    }
                }
            }
            gaps
        })
    };

    reconcile(&hot, db.cold().expect("cold attached")).expect("finalize");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let gaps = reader.join().expect("reader thread");
    assert_eq!(gaps, 0, "finalize must never let an archived row vanish from both tiers");

    // The finalize completed the migration: hot rows pruned, every row serves from cold. A
    // zero-yield finalize queues its deletes without draining, so flush before the raw probe.
    hot.sync_persist().expect("persist");
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |_| true), 0, "hot pruned");
    for (number, _) in &blocks {
        assert!(db.get::<ConsensusBlocks>(number).expect("read").is_some(), "cold serves");
    }
}

/// A prune cancelled after the high-water mark commit leaves every row in cold, and a later
/// reconcile must sweep the hot leftovers.
///
/// The pass must also report cancelled rather than archived: a `Sealed` outcome counts as
/// progress and schedules the next pass, which is the teardown the cancel is stopping.
#[test]
fn paced_prune_cancelled_after_seal_heals_on_reconcile() {
    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    seed_chunk_blocks(
        &hot,
        &[(0, vec![DIGEST_A]), (1, vec![DIGEST_B]), (2, vec![DIGEST_C])],
        &[(DIGEST_A, 0), (DIGEST_B, 1), (DIGEST_C, 2)],
    );
    // Marker epochs 1 and 2 make epoch 0 due; the cancel below fires before epoch 1 seals.
    hot.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&3, &header_for(3, 1, DIGEST_A))?;
        txn.insert::<ConsensusBlocks>(&4, &header_for(4, 2, DIGEST_A))?;
        Ok(())
    })
    .expect("seed markers");
    hot.sync_persist().expect("persist");

    let archiver = ColdArchiver::new(hot.clone(), db.cold().expect("cold attached").clone());
    // Cancel the instant epoch 0's jar is committed: the seal completes and the high-water mark
    // commits, but the prune is skipped at its first batch - the partial-archive state a
    // shutdown leaves.
    let cold = db.cold().expect("cold attached").clone();
    let cancel_once_sealed = move || cold.consensus_blocks().sealed_epochs().contains(&0);
    assert_eq!(
        archiver.seal_due(Epoch::MAX, cancel_once_sealed).expect("archive"),
        SealOutcome::Cancelled,
        "a pass whose prune was cancelled has not archived the epoch"
    );
    hot.sync_persist().expect("persist");

    // The high-water mark did NOT advance, so the epoch stays above it and reconcile revisits it;
    // its rows are in cold either way, so serving is unaffected.
    let high_water_mark = hot
        .with_read_txn(|tx| tx.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY))
        .expect("read high-water mark");
    assert_eq!(high_water_mark, None, "a cancelled prune must not mark the epoch archived");
    assert!(
        count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n <= 2) > 0,
        "the cancelled prune left hot rows (they are in both tiers, which is safe)"
    );
    // Every archived row still serves (from hot or cold).
    assert!(db.get::<Batches>(&DIGEST_A).expect("read").is_some());

    // Reconcile sweeps the leftover hot rows, completing the prune idempotently.
    archiver.reconcile().expect("heal");
    hot.sync_persist().expect("persist");
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n <= 2),
        0,
        "reconcile swept the leftover hot rows"
    );
    assert!(db.get::<Batches>(&DIGEST_A).expect("read").is_some(), "cold serves after the sweep");
}

/// A prune cancelled MID-delete (some block chunks committed, the tail not) must still be swept
/// by reconcile: the epoch stays above the high-water mark, so the whole finalize re-runs and the
/// re-prune is addressed from the jar rather than from what the hot tier still happens to hold.
#[test]
fn reconcile_sweeps_partially_paced_pruned_epoch() {
    use crate::cold::producer::{
        finalize_sealed, seal_next_epoch_jars, Finalized, JarSeal, SEAL_CHUNK_BYTES,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    let tmp = TempDir::new().unwrap();
    let (db, hot) = open_test_db(&tmp);
    // One epoch spanning two prune chunks: 4097 blocks (and 4097 unique digests) split the paced
    // delete into two digest chunks followed by two block chunks.
    const BLOCKS: u64 = 4097;
    hot.with_write_txn(|txn| {
        for number in 0..BLOCKS {
            let digest = BlockHash::from(digest_seed(number, 0));
            txn.insert::<Batches>(&digest, &batch_for(number, 0))?;
            txn.insert::<ConsensusBlocks>(&number, &header_for(number, 0, digest))?;
        }
        Ok(())
    })
    .expect("seed");
    hot.sync_persist().expect("persist");

    // Seal + index, then cancel the paced prune right before its SECOND block chunk: the digests
    // and blocks 0..=4095 are deleted and committed, block 4096 (the epoch's last) stays hot.
    let seal =
        seal_next_epoch_jars(&hot, db.cold().expect("cold attached"), 1, SEAL_CHUNK_BYTES, &|| {
            false
        })
        .expect("seal");
    let JarSeal::Sealed(sealed) = seal else { panic!("epoch 0 must seal") };
    let polls = AtomicUsize::new(0);
    let finalized = finalize_sealed(
        &hot,
        &sealed,
        &|| polls.fetch_add(1, Ordering::Relaxed) + 1 >= 4,
        Duration::ZERO,
    )
    .expect("paced finalize");
    assert!(
        matches!(finalized, Finalized::Cancelled),
        "a prune stopped before its last batch has not finished archiving the epoch"
    );
    hot.sync_persist().expect("persist");

    // The partial-archive state a shutdown leaves: index committed, first block gone, last block
    // still hot, and the high-water mark un-advanced so the epoch is still due.
    let high_water_mark = hot
        .with_read_txn(|tx| tx.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY))
        .expect("read high-water mark");
    assert_eq!(high_water_mark, None, "a cancelled prune must not mark the epoch archived");
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n == 0), 0);
    assert_eq!(count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n == BLOCKS - 1), 1);

    // Reconcile must sweep the leftover tail; probing only the first block would skip it.
    reconcile(&hot, db.cold().expect("cold attached")).expect("heal");
    hot.sync_persist().expect("persist");
    assert_eq!(
        count_hot_rows::<ConsensusBlocks, _>(hot.inner(), |n| *n < BLOCKS),
        0,
        "reconcile must sweep a partially paced-pruned epoch"
    );
    // The invariant that makes the partial state safe: every row still serves through cold.
    assert!(db.get::<ConsensusBlocks>(&(BLOCKS - 1)).expect("read").is_some());
    assert!(
        db.get::<Batches>(&BlockHash::from(digest_seed(0, 0))).expect("read").is_some(),
        "cold serves after the sweep"
    );
}
