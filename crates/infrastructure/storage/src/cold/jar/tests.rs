use rayls_infrastructure_types::BlockHash;
use tempfile::TempDir;

use super::*;

fn open_consensus_blocks(dir: &Path) -> ColdSegment {
    ColdSegment::open(dir, ColdSegmentKind::ConsensusBlocks).unwrap()
}

fn open_batches(dir: &Path) -> ColdSegment {
    ColdSegment::open(dir, ColdSegmentKind::Batches).unwrap()
}

#[test]
fn consensus_blocks_round_trip_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let start: u64 = 100;
    let rows: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 1 + i as usize * 3]).collect();

    {
        let segment = open_consensus_blocks(tmp.path());
        segment.begin_epoch(7, start).unwrap();
        for row in &rows {
            segment.append_row(&[row.as_slice()]).unwrap();
        }
        segment.commit().unwrap();
    }

    // Fresh open rebuilds the index purely from the on-disk `.conf` header.
    let segment = open_consensus_blocks(tmp.path());
    assert!(segment.is_epoch_sealed(7));
    assert_eq!(segment.sealed_epochs(), BTreeSet::from([7]));
    // The epoch's archived tip is the jar end_key, recovered from the index without a row scan.
    let tip = segment.key_range_for_epoch(7).map(|range| *range.end());
    assert_eq!(tip, Some(start + rows.len() as u64 - 1));
    assert_eq!(segment.key_range_for_epoch(999), None);

    for (i, row) in rows.iter().enumerate() {
        let number = start + i as u64;
        assert!(segment.contains_number(number));
        assert_eq!(segment.read_by_number(number).unwrap().as_deref(), Some(row.as_slice()));
    }

    // Out-of-range numbers fall through to None.
    assert_eq!(segment.read_by_number(start - 1).unwrap(), None);
    assert_eq!(segment.read_by_number(start + rows.len() as u64).unwrap(), None);
    assert!(!segment.contains_number(start - 1));
}

/// A reopen must rebuild the exact live index across a multi-jar layout: every sealed epoch
/// present with its range, reads correct across the jar boundary, a zero-row jar excluded,
/// and the resume anchor (`last_sealed` tip) identical to the pre-restart value.
#[test]
fn reopen_rebuilds_multi_epoch_index_and_excludes_zero_row_jar() {
    let tmp = TempDir::new().unwrap();
    {
        let segment = open_consensus_blocks(tmp.path());
        segment.begin_epoch(7, 100).unwrap();
        for i in 0..5u8 {
            segment.append_row(&[&[i]]).unwrap();
        }
        segment.commit().unwrap();
        segment.begin_epoch(8, 105).unwrap();
        for i in 5..8u8 {
            segment.append_row(&[&[i]]).unwrap();
        }
        segment.commit().unwrap();
        // A zero-row seal leaves only the creation-time `.conf` and must stay out of the index.
        segment.begin_epoch(9, 108).unwrap();
        segment.commit().unwrap();
    }

    let segment = open_consensus_blocks(tmp.path());
    assert_eq!(segment.sealed_epochs(), BTreeSet::from([7, 8]));
    assert!(!segment.is_epoch_sealed(9), "zero-row jar must not enter the boot index");
    assert_eq!(segment.key_range_for_epoch(7), Some(100..=104));
    assert_eq!(segment.key_range_for_epoch(8), Some(105..=107));
    // The resume anchor the producer seeds `start_key` from survives the reboot.
    let tip = segment.last_sealed().expect("two sealed jars");
    assert_eq!((tip.epoch, tip.end_key()), (8, 107));
    // Reads resolve to the correct jar on both sides of the 104/105 boundary.
    assert_eq!(segment.read_by_number(104).unwrap().as_deref(), Some([4u8].as_slice()));
    assert_eq!(segment.read_by_number(105).unwrap().as_deref(), Some([5u8].as_slice()));
    assert_eq!(segment.read_by_number(108).unwrap(), None);
}

#[test]
fn batches_round_trip_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let digests: Vec<BlockHash> = (0..4u8).map(|i| BlockHash::repeat_byte(i + 1)).collect();
    let payloads: Vec<Vec<u8>> = (0..4).map(|i| vec![0xAA ^ i as u8; 4 + i as usize]).collect();

    {
        let segment = open_batches(tmp.path());
        segment.begin_epoch(3, 0).unwrap();
        for (digest, payload) in digests.iter().zip(&payloads) {
            segment.append_row(&[digest.as_slice(), payload.as_slice()]).unwrap();
        }
        segment.commit().unwrap();
    }

    let segment = open_batches(tmp.path());
    assert!(segment.is_epoch_sealed(3));

    // Row reads return the payload column byte-identically.
    for (row, payload) in payloads.iter().enumerate() {
        let columns = segment.read_row(3, row).unwrap().expect("row present");
        assert_eq!(columns[BATCH_PAYLOAD_COLUMN], *payload);
    }

    // An unsealed epoch is absent from the index; production gates reads on this before
    // touching a jar (see `read_batch_checked`).
    assert!(!segment.is_epoch_sealed(99));
}

#[test]
fn read_batch_checked_verifies_digest_column() {
    let tmp = TempDir::new().unwrap();
    let store = ColdStore::open(&ColdConfig { dir: tmp.path().to_path_buf() }).unwrap();

    let digest = BlockHash::repeat_byte(0x42);
    let payload = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
    store.batches().begin_epoch(5, 0).unwrap();
    store.batches().append_row(&[digest.as_slice(), payload.as_slice()]).unwrap();
    store.batches().commit().unwrap();

    let loc = ColdLocation { epoch: 5, row: 0 };

    // The matching digest serves the payload byte-identically.
    assert_eq!(store.read_batch_checked(digest, loc).unwrap().as_deref(), Some(payload.as_slice()));

    // A wrong digest at a real row is surfaced as corruption, never a silent mis-serve.
    let wrong = BlockHash::repeat_byte(0x99);
    assert!(matches!(store.read_batch_checked(wrong, loc), Err(ColdError::Corruption(_))));

    // An unsealed epoch falls through to None.
    assert_eq!(store.read_batch_checked(digest, ColdLocation { epoch: 99, row: 0 }).unwrap(), None);
}

#[test]
fn multiple_epochs_read_gaplessly_across_jar_boundary() {
    let tmp = TempDir::new().unwrap();
    {
        let segment = open_consensus_blocks(tmp.path());
        // Epoch 1 covers [0, 2], epoch 2 covers [3, 5]: contiguous across the jar boundary.
        segment.begin_epoch(1, 0).unwrap();
        for n in 0..3u64 {
            segment.append_row(&[&[n as u8]]).unwrap();
        }
        segment.commit().unwrap();

        segment.begin_epoch(2, 3).unwrap();
        for n in 3..6u64 {
            segment.append_row(&[&[n as u8]]).unwrap();
        }
        segment.commit().unwrap();
    }

    let segment = open_consensus_blocks(tmp.path());
    assert_eq!(segment.sealed_epochs(), BTreeSet::from([1, 2]));
    // Every number across the two jars resolves by point read, gaplessly over the boundary.
    for n in 0..6u64 {
        assert_eq!(segment.read_by_number(n).unwrap().as_deref(), Some([n as u8].as_slice()));
    }
}

#[test]
fn reopen_after_partial_write_self_heals() {
    let tmp = TempDir::new().unwrap();

    // Seal a first epoch fully so a committed baseline exists on disk.
    {
        let segment = open_consensus_blocks(tmp.path());
        segment.begin_epoch(1, 0).unwrap();
        segment.append_row(&[&[10u8]]).unwrap();
        segment.append_row(&[&[11u8]]).unwrap();
        segment.commit().unwrap();
    }

    // Begin a second epoch, append, then drop without committing: the `.conf` for epoch 2 is
    // never frozen, so the writer's consistency check must heal it on the next open.
    {
        let segment = open_consensus_blocks(tmp.path());
        segment.begin_epoch(2, 2).unwrap();
        segment.append_row(&[&[99u8]]).unwrap();
        // Drop the segment (and its open writer) without commit.
    }

    // Reopen and drive the self-heal by reopening the uncommitted epoch-2 jar via begin_epoch,
    // which constructs a NippyJarWriter that runs ensure_consistency on the existing files.
    let segment = open_consensus_blocks(tmp.path());
    // Epoch 1 stayed intact and readable across the crash.
    assert!(segment.is_epoch_sealed(1));
    assert_eq!(segment.read_by_number(0).unwrap().as_deref(), Some([10u8].as_slice()));
    assert_eq!(segment.read_by_number(1).unwrap().as_deref(), Some([11u8].as_slice()));
    // Epoch 2 never sealed its `.conf`, so it is absent from the boot index.
    assert!(!segment.is_epoch_sealed(2));

    // Re-running the epoch-2 archive (idempotent producer retry) succeeds and seals it. The
    // NippyJarWriter::new path here exercises ensure_consistency against the half-written jar.
    segment.begin_epoch(2, 2).unwrap();
    segment.append_row(&[&[99u8]]).unwrap();
    segment.commit().unwrap();

    let segment = open_consensus_blocks(tmp.path());
    assert!(segment.is_epoch_sealed(2));
    assert_eq!(segment.read_by_number(2).unwrap().as_deref(), Some([99u8].as_slice()));
}

#[test]
fn begin_epoch_retry_over_live_open_jar_reseals_cleanly() {
    let tmp = TempDir::new().unwrap();
    let segment = open_consensus_blocks(tmp.path());

    // Begin and append to epoch 1 but never commit, leaving a live writer in `self.open`
    // (models a seal that failed after `begin_epoch`, e.g. a read-phase error in the
    // producer).
    segment.begin_epoch(1, 0).unwrap();
    segment.append_row(&[&[7u8]]).unwrap();

    // Retry the same epoch: `begin_epoch` must release the prior live writer before opening a
    // new one on the same file (never two writers at once), then seal cleanly to the
    // retried value.
    segment.begin_epoch(1, 0).unwrap();
    segment.append_row(&[&[8u8]]).unwrap();
    segment.commit().unwrap();

    let segment = open_consensus_blocks(tmp.path());
    assert!(segment.is_epoch_sealed(1));
    assert_eq!(segment.read_by_number(0).unwrap().as_deref(), Some([8u8].as_slice()));
}

#[test]
fn empty_dir_opens_as_empty_segment() {
    let tmp = TempDir::new().unwrap();
    let segment = open_consensus_blocks(tmp.path());
    assert!(segment.sealed_epochs().is_empty());
    assert_eq!(segment.read_by_number(0).unwrap(), None);
}

/// A zero-row seal must not enter the index, live or across a reopen: the boot scan skips
/// `rows == 0` headers, and the live commit must agree or the two indexes diverge.
#[test]
fn zero_row_commit_is_not_indexed() {
    let tmp = TempDir::new().unwrap();
    let segment = open_consensus_blocks(tmp.path());
    segment.begin_epoch(1, 0).unwrap();
    segment.commit().unwrap();
    assert!(!segment.is_epoch_sealed(1), "empty seal must not index live");

    let segment = open_consensus_blocks(tmp.path());
    assert!(!segment.is_epoch_sealed(1), "empty seal must not index across a reopen");
    assert!(segment.sealed_epochs().is_empty());
}

/// Concurrent point reads must stay correct while later epochs seal: the index, cache, and
/// writer locks are independent and a sealed row is immutable, so readers never deadlock,
/// panic, or observe a torn serve.
#[test]
fn concurrent_reads_while_sealing_later_epochs() {
    const ROWS: u64 = 16;
    let tmp = TempDir::new().unwrap();
    let segment = open_consensus_blocks(tmp.path());
    segment.begin_epoch(1, 0).unwrap();
    for n in 0..ROWS {
        segment.append_row(&[&[n as u8]]).unwrap();
    }
    segment.commit().unwrap();

    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                for i in 0..400u64 {
                    let n = i % ROWS;
                    assert_eq!(
                        segment.read_by_number(n).unwrap().as_deref(),
                        Some([n as u8].as_slice())
                    );
                }
            });
        }
        for epoch in 2..=5u32 {
            let start = (u64::from(epoch) - 1) * ROWS;
            segment.begin_epoch(epoch, start).unwrap();
            for n in start..start + ROWS {
                segment.append_row(&[&[n as u8]]).unwrap();
            }
            segment.commit().unwrap();
        }
    });

    // Every epoch sealed during the contention serves correctly afterwards.
    for n in 0..5 * ROWS {
        assert_eq!(segment.read_by_number(n).unwrap().as_deref(), Some([n as u8].as_slice()));
    }
}

/// Deletes a sealed jar's data file but keeps its `.conf`, so the index still lists the epoch
/// while any read of it fails. Lets the laziness tests prove a jar opens only when reached.
fn remove_jar_data_file(dir: &Path, epoch: Epoch) {
    fs::remove_file(dir.join(format!("epoch-{epoch:010}"))).unwrap();
}

fn seal_two_epochs(dir: &Path) {
    let segment = open_consensus_blocks(dir);
    // Epoch 1 covers [0, 2], epoch 2 covers [3, 5].
    segment.begin_epoch(1, 0).unwrap();
    for n in 0..3u64 {
        segment.append_row(&[&[n as u8]]).unwrap();
    }
    segment.commit().unwrap();

    segment.begin_epoch(2, 3).unwrap();
    for n in 3..6u64 {
        segment.append_row(&[&[n as u8]]).unwrap();
    }
    segment.commit().unwrap();
}

/// The amortized reader yields an epoch's rows in ascending order and reads only the target
/// jar (reusing one cursor across the scan). Guards the reconcile-rebuild path.
#[test]
fn for_each_row_in_epoch_reads_only_the_target_jar() {
    let tmp = TempDir::new().unwrap();
    seal_two_epochs(tmp.path());

    // Reopen, then delete epoch 1's data file: a scan of epoch 2 must succeed without touching
    // epoch 1, and yield exactly epoch 2's rows in ascending order.
    let segment = open_consensus_blocks(tmp.path());
    remove_jar_data_file(tmp.path(), 1);

    let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
    segment
        .for_each_row_in_epoch(2, |number, value| {
            rows.push((number, value.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, vec![(0, vec![3u8]), (1, vec![4u8]), (2, vec![5u8])]);

    // The deleted epoch-1 jar errors only when its own rows are read.
    assert!(segment.for_each_row_in_epoch(1, |_, _| Ok(())).is_err());

    // A never-sealed epoch is an empty scan, not an error.
    let mut visited = false;
    segment
        .for_each_row_in_epoch(9, |_, _| {
            visited = true;
            Ok(())
        })
        .unwrap();
    assert!(!visited, "an absent epoch must visit no rows");
}

/// The batches walk yields every archived row as `(row, digest)` in ascending row order, with the
/// row equal to the [`ColdLocation`] a serve resolves through, so the auxiliary digest index can be
/// rebuilt from the jar alone.
#[test]
fn batch_digest_walk_yields_serveable_locations() {
    let tmp = TempDir::new().unwrap();
    let store = ColdStore::open(&ColdConfig { dir: tmp.path().to_path_buf() }).unwrap();
    let digests: Vec<BlockHash> = (0..4u8).map(|i| BlockHash::repeat_byte(i + 1)).collect();

    store.batches().begin_epoch(6, 0).unwrap();
    for (i, digest) in digests.iter().enumerate() {
        store.batches().append_row(&[digest.as_slice(), &[i as u8; 3]]).unwrap();
    }
    store.batches().commit().unwrap();

    let mut walked = Vec::new();
    store
        .for_each_batch_digest_in_epoch(6, |row, digest| {
            walked.push((digest, ColdLocation { epoch: 6, row }));
            Ok(())
        })
        .unwrap();
    let expected: Vec<_> = digests
        .iter()
        .enumerate()
        .map(|(row, digest)| (*digest, ColdLocation { epoch: 6, row: row as u64 }))
        .collect();
    assert_eq!(walked, expected);

    // Each walked pair addresses the row it was read from, so an index rebuilt from the walk
    // serves the right payload rather than tripping the digest cross-check.
    for (digest, loc) in walked {
        assert_eq!(store.read_batch_checked(digest, loc).unwrap().unwrap().len(), 3);
    }

    // An epoch with no sealed batches jar is an empty walk, not an error.
    let mut visited = false;
    store
        .for_each_batch_digest_in_epoch(9, |_, _| {
            visited = true;
            Ok(())
        })
        .unwrap();
    assert!(!visited, "an absent epoch must visit no rows");
}

/// A `.conf` start key that cannot address the jar's own rows must surface as corruption on both
/// the live seal and the boot rebuild. The header is deserialized from disk without validation, so
/// the range arithmetic must never be what notices: it aborts the process under overflow-checks,
/// on every restart, before the node serves anything.
#[test]
fn incoherent_start_key_is_rejected_not_aborted() {
    let tmp = TempDir::new().unwrap();
    let segment = open_consensus_blocks(tmp.path());
    // Two rows rooted at the last addressable key: the jar's end key would wrap.
    segment.begin_epoch(1, u64::MAX).unwrap();
    segment.append_row(&[&[1u8]]).unwrap();
    segment.append_row(&[&[2u8]]).unwrap();
    assert!(
        matches!(segment.commit(), Err(ColdError::Corruption(_))),
        "live seal must fail closed"
    );

    // The jar is durable by now (nippy commits before the index insert), so the boot rebuild has
    // to reject it too rather than fault inside `ColdSegment::open`.
    let reopened = ColdSegment::open(tmp.path(), ColdSegmentKind::ConsensusBlocks);
    assert!(matches!(reopened, Err(ColdError::Corruption(_))), "boot rebuild must fail closed");
}

/// A jar whose own `kind` disagrees with the segment holding it must fail the boot rebuild. The
/// two kinds carry different column counts and different addressing, so indexing a foreign jar
/// would serve batch rows as `ConsensusHeader` bytes at arithmetic block numbers.
#[test]
fn foreign_kind_jar_is_rejected_at_boot() {
    let tmp = TempDir::new().unwrap();
    {
        let segment = open_batches(tmp.path());
        segment.begin_epoch(3, 0).unwrap();
        segment.append_row(&[BlockHash::repeat_byte(1).as_slice(), &[0xAAu8]]).unwrap();
        segment.commit().unwrap();
    }

    let opened = ColdSegment::open(tmp.path(), ColdSegmentKind::ConsensusBlocks);
    assert!(matches!(opened, Err(ColdError::Corruption(_))), "a foreign-kind jar must not index");

    // The jar is still coherent for the segment it was written for.
    assert!(open_batches(tmp.path()).is_epoch_sealed(3));
}

#[test]
fn cached_jar_serves_reads_after_file_deletion() {
    let tmp = TempDir::new().unwrap();
    {
        let segment = open_consensus_blocks(tmp.path());
        segment.begin_epoch(1, 0).unwrap();
        for n in 0..3u64 {
            segment.append_row(&[&[n as u8]]).unwrap();
        }
        segment.commit().unwrap();
    }

    let segment = open_consensus_blocks(tmp.path());
    // Prime the cache: this read mmaps and caches epoch 1's jar.
    assert_eq!(segment.read_by_number(0).unwrap().as_deref(), Some([0u8].as_slice()));

    // Delete the data file. The cached mmap pins the inode, so a later read of that epoch is
    // served from the open handle without touching the file. A segment that re-opened the file
    // per read would fail here, so this pins the cache reuse.
    remove_jar_data_file(tmp.path(), 1);
    assert_eq!(segment.read_by_number(1).unwrap().as_deref(), Some([1u8].as_slice()));
}
