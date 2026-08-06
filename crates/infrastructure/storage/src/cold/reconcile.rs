//! Boot reconciliation of the cold tier with the hot auxiliary index.
//!
//! [`reconcile`] heals a crash or cancel that interrupted an archive pass: sealed jars whose
//! index/high-water mark/prune tail never landed. It shares the finalize tail with the seal path in
//! [`producer`](super::producer), so heal and archive apply the same crash-atomic ordering.

use std::time::Duration;

use rayls_infrastructure_types::{BlockHash, Database, DbTx, Epoch};
use tracing::info;

use super::{
    producer::{advance_high_water_mark, commit_index, delete_archived_rows, to_cold},
    ColdError, ColdLocation, ColdResult, ColdStore, ARCHIVE_HIGH_WATER_MARK_KEY,
};
use crate::tables::ColdArchiveHighWaterMark;

/// Reconciles the cold tier with the hot index: the boot heal for an archive a crash or cancel
/// interrupted (sealed jars whose index/prune never landed). Mirrors reth `ensure_invariants`.
///
/// `db` must be the RAW hot database. Idempotent, and safe beside an in-flight seal: its jar is
/// uncommitted, so the epochs touched are disjoint.
pub fn reconcile<DB: Database>(db: &DB, cold: &ColdStore) -> ColdResult<()> {
    let high_water_mark = db
        .with_read_txn(|tx| tx.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY))
        .map_err(to_cold)?;

    // The loop below only visits epochs the jars already claim, so a cold directory that is
    // missing entirely reads as "nothing to do" while the surviving index and high-water mark still
    // route every archived read to it. Coverage is an O(1) assertion, so it is checked up front:
    // the high-water mark only ever advances behind that epoch's own committed consensus_blocks
    // jar.
    if let Some(high_water_mark) = high_water_mark {
        if !cold.consensus_blocks().is_epoch_sealed(high_water_mark) {
            return Err(ColdError::Corruption(format!(
                "cold high-water mark is epoch {high_water_mark} but no consensus_blocks jar covers it: \
                 the cold directory is missing or truncated"
            )));
        }
    }

    // Drive off the consensus_blocks segment, the seal source of truth (it commits last). A torn
    // epoch with only its batches jar durable is absent from it, so its hot rows are left for the
    // next pass to re-seal rather than evicted with no durable cold copy.
    for epoch in cold.consensus_blocks().sealed_epochs() {
        if high_water_mark.is_some_and(|high_water_mark| epoch <= high_water_mark) {
            // The high-water mark is only advanced once an epoch's index AND its whole prune
            // landed, so at or below it there is provably nothing left to do.
            continue;
        }
        // The sealed jar fixes what to delete (dense range + jar-walk digests); the hot tier is
        // never scanned.
        let numbers = cold.consensus_blocks().key_range_for_epoch(epoch).ok_or_else(|| {
            ColdError::Corruption(format!("sealed epoch {epoch} missing from the jar index"))
        })?;

        // The jar is durable but the finalize did not run to completion, so re-run all of it. Both
        // halves are idempotent: the index rebuild rewrites the same rows the jar dictates, and the
        // prune is addressed from that jar rather than from what the hot tier still holds.
        info!(target: "cold-archive", epoch, "reconcile: finalizing a sealed epoch (index + prune + high-water mark)");
        let locations = batch_locations_from_jar(cold, epoch)?;
        commit_index(db, &locations)?;
        // A boot heal shares the hot writer with nobody, so it neither cancels nor yields and the
        // prune always reports a full sweep.
        let _ = delete_archived_rows(db, numbers, &locations, &|| false, Duration::ZERO)?;
        advance_high_water_mark(db, epoch)?;
    }
    Ok(())
}

/// Rebuilds the `(digest -> ColdLocation)` entries for `epoch` from that epoch's batches jar,
/// whose column 0 is the digest stored at that row.
///
/// The jar is the layout, so the rebuild cannot drift from it. Re-deriving it from the archived
/// consensus blocks can: the seal appends no row for a digest an earlier epoch already archived,
/// which a projection replay would still number, shifting every later row in the epoch.
fn batch_locations_from_jar(
    cold: &ColdStore,
    epoch: Epoch,
) -> ColdResult<Vec<(BlockHash, ColdLocation)>> {
    let mut locations = Vec::new();
    cold.for_each_batch_digest_in_epoch(epoch, |row, digest| {
        locations.push((digest, ColdLocation { epoch, row }));
        Ok(())
    })?;
    Ok(locations)
}
