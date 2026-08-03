//! The cold read seam: hot-miss fall-through resolution shared by the layered database.
//!
//! Reads resolve hot-first, then fall through by `T::NAME`: `ConsensusBlocks` by arithmetic block
//! number, `Batches` through the hot `ColdBatchLocations` index; every other table is hot-only.
//! [`LayeredDatabase`](crate::layered_db::LayeredDatabase) and its read transaction call
//! [`cold_get`]/[`cold_has`]/[`cold_raw`] after a hot miss, injecting the auxiliary-index lookup
//! so a cold serve stays consistent with the hot view around it. Iteration stays hot-tail only
//! (the infallible `DBIter` contract cannot surface a cold read error); cold history is reached
//! by point reads or the per-epoch [`ColdStore`] iterators.

use rayls_infrastructure_types::{encode_key, try_decode, try_decode_key, BlockHash, Table};

use super::{ColdError, ColdLocation, ColdResult, ColdStore};
use crate::tables::{Batches, ConsensusBlocks};

/// Returns `key`'s raw cold-jar bytes, or `None` if the cold tier does not hold it.
///
/// `lookup` resolves the hot `ColdBatchLocations` auxiliary index on the caller's own hot view (a
/// held snapshot, or the layered read path), so a cold serve stays consistent with the hot reads
/// around it. Non-archived tables are `None`.
pub(crate) fn cold_raw<T: Table>(
    cold: &ColdStore,
    key: &T::Key,
    lookup: impl FnOnce(&BlockHash) -> eyre::Result<Option<ColdLocation>>,
) -> ColdResult<Option<Vec<u8>>> {
    match cold_kind::<T>() {
        Some(ColdKind::ConsensusBlocks) => {
            // The stored header's own number is cross-checked against the arithmetic addressing,
            // so a misaligned jar cannot silently serve the wrong block's header.
            cold.read_consensus_block_checked(archived_number::<T>(key)?)
        }
        Some(ColdKind::Batches) => {
            let digest = archived_digest::<T>(key)?;
            match lookup(&digest).map_err(|e| {
                ColdError::Corruption(format!("cold auxiliary-index read failed: {e}"))
            })? {
                // The digest column in the jar is verified against the index-resolved digest, so
                // a stale or mis-pointing auxiliary index cannot serve a row from the wrong batch.
                Some(loc) => cold.read_batch_checked(digest, loc),
                None => Ok(None),
            }
        }
        None => Ok(None),
    }
}

/// Returns `key`'s decoded cold value, or `None` if the cold tier does not hold it.
pub(crate) fn cold_get<T: Table>(
    cold: &ColdStore,
    key: &T::Key,
    lookup: impl FnOnce(&BlockHash) -> eyre::Result<Option<ColdLocation>>,
) -> ColdResult<Option<T::Value>> {
    cold_raw::<T>(cold, key, lookup)?.map(|bytes| decode_cold::<T>(&bytes)).transpose()
}

/// Returns true if the cold tier holds `key`.
pub(crate) fn cold_has<T: Table>(
    cold: &ColdStore,
    key: &T::Key,
    lookup: impl FnOnce(&BlockHash) -> eyre::Result<Option<ColdLocation>>,
) -> ColdResult<bool> {
    match cold_kind::<T>() {
        Some(ColdKind::ConsensusBlocks) => {
            Ok(cold.consensus_blocks().contains_number(archived_number::<T>(key)?))
        }
        // Answered from the jar, not the auxiliary index alone: an index entry can name a row
        // that is not readable (an epoch whose jar is not sealed yet, a digest the jar does not
        // hold), and a `contains_key` that promises a value `get` cannot produce turns any
        // availability check that gates on it into a lie. The price is a full row read (mmap
        // plus decompress, sized to the widest archived batch), so this is the exact answer and
        // not a cheap one: resolve a set of digests with `get`, never with this in a loop.
        Some(ColdKind::Batches) => Ok(cold_raw::<T>(cold, key, lookup)?.is_some()),
        None => Ok(false),
    }
}

/// Decodes the generic `Batches` key as the digest that addresses its cold row.
///
/// Routing through the key codec keeps the cold seam free of `Any` while staying an identity
/// round-trip for the real key type.
fn archived_digest<T: Table>(key: &T::Key) -> ColdResult<BlockHash> {
    try_decode_key(&encode_key(key))
        .map_err(|e| ColdError::Codec(format!("batches key not a digest: {e}")))
}

/// Which cold-archived segment a generic table `T` resolves to.
///
/// Returned by [`cold_kind`] to centralize the `T::NAME` dispatch; a hot-only table yields `None`.
enum ColdKind {
    /// `ConsensusBlocks`: resolved by block number against the consensus_blocks jars.
    ConsensusBlocks,
    /// `Batches`: resolved by digest through the hot `ColdBatchLocations` auxiliary index.
    Batches,
}

/// Classifies `T` as a cold-archived segment, or `None` for a hot-only table.
///
/// The single dispatch point for the cold seam: adding a cold-archived table is one new arm here,
/// so no read path can silently fall through to `None` (a fatal missing row) for a new table.
fn cold_kind<T: Table>() -> Option<ColdKind> {
    if T::NAME == ConsensusBlocks::NAME {
        Some(ColdKind::ConsensusBlocks)
    } else if T::NAME == Batches::NAME {
        Some(ColdKind::Batches)
    } else {
        None
    }
}

/// Decodes the generic `ConsensusBlocks` key as the block number that addresses its cold jar.
///
/// `ConsensusBlocks` is keyed by `u64`; routing through the key codec keeps the cold seam free of
/// `Any` while staying an identity round-trip for the real key type.
fn archived_number<T: Table>(key: &T::Key) -> ColdResult<u64> {
    try_decode_key::<u64>(&encode_key(key))
        .map_err(|e| ColdError::Codec(format!("consensus_blocks key not a u64: {e}")))
}

/// Decodes raw cold-jar bytes into the table's value type via the bcs value codec.
///
/// Jars store the same bcs-encoded value bytes the hot tier wrote, so the cold value round-trips
/// through the exact decode path the hot tier uses.
fn decode_cold<T: Table>(bytes: &[u8]) -> ColdResult<T::Value> {
    try_decode::<T::Value>(bytes).map_err(|e| ColdError::Codec(e.to_string()))
}

/// Converts a cold-tier error to the `eyre` form the `Database` trait surface returns.
pub(crate) fn cold_to_eyre(e: ColdError) -> eyre::Report {
    eyre::eyre!("cold tier: {e}")
}
