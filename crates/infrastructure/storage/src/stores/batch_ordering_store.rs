use crate::{
    tables::{BatchOrderingState as TableBatchOrderingState, Batches},
    StoreResult,
};
use rayls_infrastructure_types::{
    batch_ordering::{AuthoritySeqState, BatchOrderingState, StoredBatchOrderingState},
    decode, try_decode, B256Map, Batch, Database, DbTx, B256,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::warn;

/// Key of the single ordering-state row; only the current epoch's state is kept.
pub const ORDERING_KEY: u8 = 0;

/// Persistence of the per-epoch batch ordering state.
pub trait BatchOrderingStore {
    /// Writes the entire batch ordering state for the current epoch.
    fn write_batch_ordering_state(&self, ordering: &BatchOrderingState) -> StoreResult<()>;

    /// Reads the batch ordering state, reloading parked batch bodies from the `Batches` table.
    fn read_batch_ordering_state(&self) -> StoreResult<Option<BatchOrderingState>>;
}

impl<DB: Database> BatchOrderingStore for DB {
    fn write_batch_ordering_state(&self, ordering: &BatchOrderingState) -> StoreResult<()> {
        // Persist parked batches by digest, not by value: a committed batch's `Batches` row
        // survives the reboot, so the transaction bytes would only be duplicated in the blob.
        let stored = StoredBatchOrderingState::from(ordering);
        self.insert::<TableBatchOrderingState>(&ORDERING_KEY, &stored)
    }

    fn read_batch_ordering_state(&self) -> StoreResult<Option<BatchOrderingState>> {
        // Every read txn here does raw reads only; decoding happens after it closes. Holding a
        // read txn across the deserialization of a parking storm's worth of transaction bytes
        // would trip the MDBX read-txn timeout mid-recovery.
        let Some(raw) = self.with_read_txn(|txn| {
            Ok(txn
                .raw_get::<TableBatchOrderingState>(&ORDERING_KEY)?
                .map(|bytes| bytes.into_owned()))
        })?
        else {
            return Ok(None);
        };

        // A `ParkedRef` is fixed-size, so the compact decode succeeds only on the current format: a
        // legacy blob with any parked entry leaves trailing bytes and fails it, while an all-empty
        // blob is byte-identical and decodes either way harmlessly.
        let Ok(stored) = try_decode::<StoredBatchOrderingState>(&raw) else {
            // Backwards compatibility: an older binary wrote the parked batches by value, so the
            // blob carries the bodies. Decode it directly; the next persist rewrites the compact
            // format.
            let legacy = try_decode(&raw).map_err(|e| {
                eyre::eyre!("undecodable batch ordering state (neither format): {e}")
            })?;
            return Ok(Some(legacy));
        };

        // Collect the parked bodies in one more short txn (raw reads only), then decode and
        // assemble once it has closed.
        let digests: Vec<B256> = stored
            .authorities
            .values()
            .flat_map(|auth| auth.parked.values().map(|reference| reference.batch_digest))
            .collect();
        let bodies = self.with_read_txn(|txn| {
            let mut bodies = B256Map::default();
            for digest in &digests {
                if let Some(bytes) = txn.raw_get::<Batches>(digest)? {
                    bodies.insert(*digest, bytes.into_owned());
                }
            }
            Ok(bodies)
        })?;

        Ok(Some(reconstruct_parked(stored, &bodies)))
    }
}

/// Rebuilds each authority's parked map by pairing every reference with its reloaded body,
/// dropping any entry whose body is absent from `Batches`.
///
/// A dropped entry leaves a seq gap the ordering waits to refill; a committed parked batch's row
/// always survives the reboot, so the drop is defensive.
fn reconstruct_parked(
    stored: StoredBatchOrderingState,
    bodies: &B256Map<Vec<u8>>,
) -> BatchOrderingState {
    let mut authorities = BTreeMap::new();
    for (addr, auth) in stored.authorities {
        let mut parked = BTreeMap::new();
        for (seq, reference) in auth.parked {
            match bodies.get(&reference.batch_digest) {
                Some(bytes) => {
                    let batch: Batch = decode(bytes);
                    parked.insert(seq, reference.into_prepared(Arc::new(batch)));
                }
                None => warn!(
                    target: "engine",
                    ?addr,
                    seq,
                    batch_digest = ?reference.batch_digest,
                    "dropping parked batch on restart: no Batches row for its digest"
                ),
            }
        }
        authorities
            .insert(addr, AuthoritySeqState { last_executed_seq: auth.last_executed_seq, parked });
    }
    BatchOrderingState { epoch: stored.epoch, authorities }
}
