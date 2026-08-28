//! Transaction shape and high-water mark ordering of the finalize tail.

use std::time::Duration;

use super::*;
use crate::{
    cold::producer::{
        advance_high_water_mark, commit_index, delete_archived_rows, finalize_sealed, Finalized,
        SealedJars, PRUNE_YIELD,
    },
    test_utils::TestDb,
};

/// Builds `count` staged locations for `epoch`, in the row order a seal would append them.
fn staged_locations(epoch: Epoch, count: u64) -> Vec<(BlockHash, ColdLocation)> {
    (0..count)
        .map(|row| (BlockHash::from(digest_seed(row, epoch)), ColdLocation { epoch, row }))
        .collect()
}

/// The index commit must split an epoch's locations across bounded transactions, and must not
/// touch the high-water mark.
///
/// A whole epoch's digests in one transaction holds the writer that live consensus shares.
#[test]
fn index_commit_chunks_locations_and_leaves_the_high_water_mark_alone() {
    // One digest past two full chunks, so the split is observable and the boundary is exercised.
    const EPOCH: Epoch = 3;
    let db = TestDb::new();
    let locations = staged_locations(EPOCH, 4097);

    commit_index(&db, &locations).expect("commit index");

    let starts = db.write_starts();
    assert!(starts.len() > 1, "a whole epoch's locations must not commit in one transaction");
    assert_eq!(db.iter::<ColdBatchLocations>().count(), locations.len());
    for (digest, loc) in &locations {
        assert_eq!(db.get::<ColdBatchLocations>(digest).unwrap().as_ref(), Some(loc));
    }
    assert_eq!(
        db.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY).unwrap(),
        None,
        "indexing alone must not mark the epoch archived: its rows are still hot"
    );
}

/// The high-water mark advances only once the prune has run to completion.
///
/// Its sole meaning is "archived through here", which is what lets reconcile skip an epoch without
/// probing it. Advancing it on a cancelled prune would mark an epoch done while its rows were
/// still hot, and no later pass revisits an epoch at or below the high-water mark.
#[test]
fn high_water_mark_advances_only_after_a_completed_prune() {
    const EPOCH: Epoch = 3;
    let sealed =
        SealedJars { epoch: EPOCH, numbers: 0..=9, locations: staged_locations(EPOCH, 4097) };

    let cancelled = TestDb::new();
    let outcome = finalize_sealed(&cancelled, &sealed, &|| true, Duration::ZERO).expect("finalize");
    assert!(matches!(outcome, Finalized::Cancelled), "a cancelled prune is not a complete archive");
    assert_eq!(
        cancelled.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY).unwrap(),
        None,
        "a cancelled prune must leave the epoch above the high-water mark so reconcile revisits it"
    );

    let completed = TestDb::new();
    let outcome =
        finalize_sealed(&completed, &sealed, &|| false, Duration::ZERO).expect("finalize");
    assert!(matches!(outcome, Finalized::Complete(_)), "an uncancelled prune completes");
    assert_eq!(
        completed.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY).unwrap(),
        Some(EPOCH),
        "a completed finalize marks the epoch archived"
    );
}

/// A hot tier rejecting the write txn outright (e.g. its map is full) must surface as the fatal
/// `WriteFailed` variant, not as retryable `Corruption`: the producer's txn-closure failures are
/// hot-tier faults, and callers shut the node down gracefully on them rather than retrying forever.
#[test]
fn rejected_hot_tier_writes_surface_as_write_failed() {
    let locations = vec![(BlockHash::repeat_byte(0xAA), ColdLocation { epoch: 0, row: 0 })];

    let err = commit_index(&TestDb::failing_writes(), &locations)
        .expect_err("a rejected hot-tier txn must surface");
    assert!(matches!(err, ColdError::WriteFailed(_)), "commit_index: got {err:?}");

    let err = advance_high_water_mark(&TestDb::failing_writes(), 0)
        .expect_err("a rejected hot-tier txn must surface");
    assert!(matches!(err, ColdError::WriteFailed(_)), "advance_high_water_mark: got {err:?}");

    let err = delete_archived_rows(
        &TestDb::failing_writes(),
        0..=0,
        &locations,
        &|| false,
        Duration::ZERO,
    )
    .expect_err("a rejected hot-tier txn must surface");
    assert!(matches!(err, ColdError::WriteFailed(_)), "delete_archived_rows: got {err:?}");
}

/// A live finalize must not wait on the writer: `sync_persist` is a FIFO barrier that re-waits the
/// whole backlog queued ahead of it, and the FIFO queue already applies index, prune and high-water
/// mark in enqueue order, so nothing needs to wait.
#[test]
fn live_finalize_issues_no_writer_barrier() {
    const EPOCH: Epoch = 3;
    let db = TestDb::new();
    let sealed =
        SealedJars { epoch: EPOCH, numbers: 0..=9, locations: staged_locations(EPOCH, 4097) };

    let outcome = finalize_sealed(&db, &sealed, &|| false, PRUNE_YIELD).expect("finalize");

    assert!(matches!(outcome, Finalized::Complete(_)), "an uncancelled prune completes");
    assert_eq!(
        db.barriers(),
        0,
        "a live finalize must never block on a writer barrier: each one re-waits the whole backlog"
    );
}
