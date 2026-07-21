//! The cold read seam: a [`Database`] newtype routing hot misses to the cold tier.
//!
//! Reads resolve hot-first, then fall through by `T::NAME`: `ConsensusBlocks` by arithmetic block
//! number, `Batches` through the hot `ColdBatchLocations` index; every other table is hot-only.
//! The fall-through lives in [`ColdTx`], so held read txns see cold rows too. Iteration stays
//! hot-tail only (the infallible `DBIter` contract cannot surface a cold read error); cold
//! history is reached by point reads or the per-epoch [`ColdStore`] iterators.

use std::{future::Future, sync::Arc};

use rayls_infrastructure_types::{
    encode_key, try_decode, try_decode_key, BlockHash, DBIter, DBRawIter, Database, DbTx, Table,
};

use super::{ColdConfig, ColdError, ColdLocation, ColdResult, ColdStore};
use crate::tables::{Batches, ColdBatchLocations, ConsensusBlocks};

/// A [`Database`] wrapper that falls through to the cold tier on a hot miss.
///
/// Composed as `ColdDatabase<LayeredDatabase<MdbxDatabase>>` so reads resolve mem -> hot -> cold.
/// The cold store is shared (`Arc`) because [`Database`] requires `Clone`.
#[derive(Debug, Clone)]
pub struct ColdDatabase<DB: Database> {
    /// The hot database this wraps.
    inner: DB,
    /// The shared cold tier consulted on a hot miss.
    cold: Arc<ColdStore>,
}

impl<DB: Database> ColdDatabase<DB> {
    /// Opens the cold tier under `cfg` and wraps `inner`.
    pub fn open(inner: DB, cfg: &ColdConfig) -> eyre::Result<Self> {
        let cold =
            ColdStore::open(cfg).map_err(|e| eyre::eyre!("failed to open cold store: {e}"))?;
        Ok(Self { inner, cold: Arc::new(cold) })
    }

    /// Returns the hot database this wraps.
    pub fn inner(&self) -> &DB {
        &self.inner
    }

    /// Returns the shared cold tier.
    pub fn cold(&self) -> &Arc<ColdStore> {
        &self.cold
    }
}

/// A cold-aware read transaction: the inner hot snapshot plus the cold store it falls through to.
///
/// Implements [`DbTx`] so `with_read_txn(|tx| tx.get(...))` resolves mem -> hot -> cold like the
/// [`Database`]-level reads. `get`/`raw_get`/`contains_key` consult the cold jars on a hot miss;
/// iteration delegates to the hot snapshot (use `ColdDatabase::iter` for the full history).
pub struct ColdTx<'a, DB: Database> {
    /// The hot MDBX snapshot.
    inner: DB::TX<'a>,
    /// The cold tier, borrowed from the [`ColdDatabase`] for the transaction's life.
    cold: &'a ColdStore,
}

impl<DB: Database> std::fmt::Debug for ColdTx<'_, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-rolled so the transaction is `Debug` without `DB::TX: Debug` derive plumbing; the
        // inner snapshot and cold store are not usefully printable here.
        f.debug_struct("ColdTx").finish_non_exhaustive()
    }
}

impl<DB: Database> ColdTx<'_, DB> {
    /// Returns `key`'s raw cold-jar bytes, or `None` if the cold tier does not hold it.
    ///
    /// `Batches` resolves its jar location through the hot `ColdBatchLocations` index read on this
    /// same transaction, so a cold serve stays a single read txn. Non-archived tables are `None`.
    fn cold_raw<T: Table>(&self, key: &T::Key) -> ColdResult<Option<Vec<u8>>> {
        match cold_kind::<T>() {
            Some(ColdKind::ConsensusBlocks) => {
                // The stored header's own number is cross-checked against the arithmetic
                // addressing, so a misaligned jar cannot silently serve the wrong
                // block's header.
                self.cold.read_consensus_block_checked(archived_number::<T>(key)?)
            }
            Some(ColdKind::Batches) => match self.archived_location::<T>(key)? {
                // The digest column in the jar is verified against the index-resolved digest, so a
                // stale or mis-pointing auxiliary index cannot serve a row from the wrong batch.
                Some((digest, loc)) => self.cold.read_batch_checked(digest, loc),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Returns `key`'s decoded cold value, or `None` if the cold tier does not hold it.
    fn cold_get<T: Table>(&self, key: &T::Key) -> ColdResult<Option<T::Value>> {
        self.cold_raw::<T>(key)?.map(|bytes| decode_cold::<T>(&bytes)).transpose()
    }

    /// Returns true if the cold tier holds `key`, without reading the payload.
    fn cold_has<T: Table>(&self, key: &T::Key) -> ColdResult<bool> {
        match cold_kind::<T>() {
            Some(ColdKind::ConsensusBlocks) => {
                Ok(self.cold.consensus_blocks().contains_number(archived_number::<T>(key)?))
            }
            Some(ColdKind::Batches) => Ok(self.archived_location::<T>(key)?.is_some()),
            None => Ok(false),
        }
    }

    /// Resolves an archived `Batches` digest to its cold location via the hot index on this txn.
    ///
    /// Returns the resolved digest alongside the location so the caller can verify it against the
    /// jar's stored digest column on read.
    fn archived_location<T: Table>(
        &self,
        key: &T::Key,
    ) -> ColdResult<Option<(BlockHash, ColdLocation)>> {
        let digest: BlockHash = try_decode_key(&encode_key(key))
            .map_err(|e| ColdError::Codec(format!("batches key not a digest: {e}")))?;
        let loc = self
            .inner
            .get::<ColdBatchLocations>(&digest)
            .map_err(|e| ColdError::Corruption(format!("cold auxiliary-index read failed: {e}")))?;
        Ok(loc.map(|loc| (digest, loc)))
    }
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

impl<DB: Database> DbTx for ColdTx<'_, DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if let Some(value) = self.inner.get::<T>(key)? {
            return Ok(Some(value));
        }
        self.cold_get::<T>(key).map_err(cold_to_eyre)
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if self.inner.contains_key::<T>(key)? {
            return Ok(true);
        }
        self.cold_has::<T>(key).map_err(cold_to_eyre)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        self.inner.iter::<T>()
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.inner.raw_iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        self.inner.skip_to::<T>(key)
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        self.inner.raw_skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.inner.reverse_iter::<T>()
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.inner.reverse_raw_iter::<T>()
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.inner.last_record::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.inner.record_prior_to::<T>(key)
    }

    fn disable_long_read_safety(&self) {
        self.inner.disable_long_read_safety()
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
fn cold_to_eyre(e: ColdError) -> eyre::Report {
    eyre::eyre!("cold tier: {e}")
}

impl<DB: Database> Database for ColdDatabase<DB> {
    type TX<'txn>
        = ColdTx<'txn, DB>
    where
        Self: 'txn;

    type TXMut<'txn>
        = DB::TXMut<'txn>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.inner.open_table::<T>()
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(ColdTx { inner: self.inner.read_txn()?, cold: self.cold.as_ref() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.inner.write_txn()
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.with_read_txn(|tx| tx.contains_key::<T>(key))
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.with_read_txn(|tx| tx.get::<T>(key))
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.inner.insert::<T>(key, value)
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.inner.remove::<T>(key)
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.inner.clear_table::<T>()
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.inner.is_empty::<T>()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        // Iteration is hot-tail only: the infallible `DBIter` contract cannot surface a cold read
        // error, so cold history is served by point reads (`get`) and the per-epoch rebuild
        // iterators on `ColdStore`, never force-merged into one forward stream here.
        self.inner.iter::<T>()
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.inner.raw_iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        self.inner.skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.inner.reverse_iter::<T>()
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.inner.reverse_raw_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.inner.record_prior_to::<T>(key)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.inner.last_record::<T>()
    }

    fn compact(&self) -> eyre::Result<()> {
        self.inner.compact()
    }

    fn persist(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        self.inner.persist()
    }

    fn sync_persist(&self) {
        self.inner.sync_persist()
    }
}
