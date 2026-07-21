//! Boot reconciliation of the cold tier with the hot auxiliary index.
//!
//! [`reconcile`] heals a crash or cancel that interrupted an archive pass: sealed jars whose
//! index/high-water/prune tail never landed. It shares the finalize tail with the seal path in
//! [`producer`](super::producer), so heal and archive apply the same crash-atomic ordering.

use std::time::Duration;

use rayls_infrastructure_types::{
    leader_epoch_and_batch_digests, B256Set, BlockHash, Database, DbTx, Epoch,
};
use tracing::info;

use super::{
    producer::{commit_index_and_high_water, delete_archived_rows, to_cold},
    ColdError, ColdLocation, ColdResult, ColdStore, ARCHIVE_HIGH_WATER_KEY,
};
use crate::tables::{ColdArchiveHighWater, ConsensusBlocks};

/// Reconciles the cold tier with the hot index: the boot heal for an archive a crash or cancel
/// interrupted (sealed jars whose index/prune never landed). Mirrors reth `ensure_invariants`.
///
/// `db` must be the RAW hot database. Idempotent, and safe beside an in-flight seal: its jar is
/// uncommitted, so the epochs touched are disjoint.
pub fn reconcile<DB: Database>(db: &DB, cold: &ColdStore) -> ColdResult<()> {
    let high_water = db
        .with_read_txn(|tx| tx.get::<ColdArchiveHighWater>(&ARCHIVE_HIGH_WATER_KEY))
        .map_err(to_cold)?;

    // Drive off the consensus_blocks segment, the seal source of truth (it commits last). A torn
    // epoch with only its batches jar durable is absent from it, so its hot rows are left for the
    // next pass to re-seal rather than evicted with no durable cold copy.
    for epoch in cold.consensus_blocks().sealed_epochs() {
        if high_water.is_some_and(|high_water| epoch < high_water) {
            // Below the high-water the seal already removed these hot rows (it deletes before
            // sealing the next epoch), so nothing remains.
            continue;
        }
        // The sealed jar fixes what to delete (dense range + jar-walk digests); the hot tier is
        // never scanned.
        let numbers = cold.consensus_blocks().key_range_for_epoch(epoch).ok_or_else(|| {
            ColdError::Corruption(format!("sealed epoch {epoch} missing from the jar index"))
        })?;
        if high_water == Some(epoch) {
            // Only the high-water epoch can hold hot rows a crash left between advancing it and
            // the last delete commit. Both delete paths remove the epoch's LAST block last, so
            // probing it alone decides for the whole epoch; probing the FIRST would misread a
            // mid-prune cancel as clean and strand the tail hot forever.
            let torn = db
                .with_read_txn(|tx| tx.contains_key::<ConsensusBlocks>(numbers.end()))
                .map_err(to_cold)?;
            if torn {
                info!(target: "cold-archive", epoch, "reconcile: re-deleting a torn epoch's leftover hot rows");
                let locations = batch_locations_from_jar(cold, epoch)?;
                delete_archived_rows(db, numbers, &locations, &|| false, Duration::ZERO)?;
            }
            continue;
        }

        // Jar durable but the auxiliary index and high-water never landed: rebuild from the jar,
        // advance the high-water in one synced txn, then drop the hot rows.
        info!(target: "cold-archive", epoch, "reconcile: finalizing a sealed epoch (index + high-water + prune)");
        let locations = batch_locations_from_jar(cold, epoch)?;
        commit_index_and_high_water(db, epoch, &locations)?;
        delete_archived_rows(db, numbers, &locations, &|| false, Duration::ZERO)?;
    }
    Ok(())
}

/// Rebuilds the `(digest -> ColdLocation)` entries for `epoch` by replaying the seal's projection
/// over the archived consensus blocks: the append and rebuild walks share one source of truth, so
/// each digest resolves to the exact row it landed in. Reads only `epoch`'s jar.
fn batch_locations_from_jar(
    cold: &ColdStore,
    epoch: Epoch,
) -> ColdResult<Vec<(BlockHash, ColdLocation)>> {
    let mut locations = Vec::new();
    // Never iterated, so the fixed-bytes hasher cannot affect row order.
    let mut seen = B256Set::default();
    let mut row: u64 = 0;
    cold.for_each_consensus_block_in_epoch(epoch, |number, raw| {
        let (block_epoch, digests) = leader_epoch_and_batch_digests(raw)
            .map_err(|e| ColdError::Codec(format!("project cold consensus block {number}: {e}")))?;
        // The jar is epoch-scoped, so a foreign committing epoch here means a corrupt or mis-keyed
        // jar, not a row to skip.
        if block_epoch != epoch {
            return Err(ColdError::Corruption(format!(
                "epoch {epoch} jar holds block {number} committed by epoch {block_epoch}"
            )));
        }
        for digest in digests {
            // Mirror the seal's dedup or the row drifts from the jar layout; a batch digest is
            // epoch-specific, so per-epoch dedup reproduces the seal's pass-wide dedup exactly.
            if !seen.insert(digest) {
                continue;
            }
            locations.push((digest, ColdLocation { epoch, row }));
            row += 1;
        }
        Ok(())
    })?;
    Ok(locations)
}
