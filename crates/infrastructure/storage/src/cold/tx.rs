//! The cold tier's transactions: [`ColdTx`] and [`ColdTxMut`] speak the standard
//! `DbTx`/`DbTxMut` contracts over the jars.
//!
//! Reads dispatch by `T::NAME`: `ConsensusBlocks` by arithmetic block number, `Batches` through
//! the injected `ColdBatchLocations` auxiliary-index lookup; every other table has no cold
//! representation. Scans cover the dense `ConsensusBlocks` span, seeded arithmetically like the
//! mdbx seeked cursors; `Batches` scans stay empty by contract, since append-ordered jars carry
//! no digest order. The layered database composes these transactions beneath its hot tiers for
//! the mem -> db -> cold fall-through and the merged scans.

use std::{borrow::Cow, cell::Cell, rc::Rc};

use rayls_infrastructure_types::{
    encode, encode_key, try_decode, try_decode_key, BlockHash, DBIter, DBRawIter, DbTx, DbTxMut,
    Epoch, Table,
};
use tracing::error;

use super::{ColdError, ColdLocation, ColdResult, ColdStore};
use crate::tables::{Batches, ConsensusBlocks};

/// A read transaction over the cold tier alone, speaking the standard [`DbTx`] contract.
///
/// `Batches` point reads resolve through the injected auxiliary-index lookup: the index is
/// materialized hot (rebuildable from the jars), so the caller supplies it from its own hot view
/// and a cold serve stays consistent with the hot reads around it. A scan fault raises the
/// shared flag ([`ColdTx::faulted`]) so a merging caller can fuse its whole stream.
pub struct ColdTx<'c> {
    /// The cold store served.
    cold: &'c ColdStore,
    /// Resolves a `Batches` digest to its jar location on the caller's hot view.
    index: Box<dyn Fn(&BlockHash) -> eyre::Result<Option<ColdLocation>> + 'c>,
    /// Raised when a scan died on a fault instead of exhausting.
    faulted: Rc<Cell<bool>>,
}

impl std::fmt::Debug for ColdTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-rolled because the index closure is not printable.
        f.debug_struct("ColdTx").finish_non_exhaustive()
    }
}

impl<'c> ColdTx<'c> {
    /// Opens a cold read transaction resolving `Batches` digests through `index`.
    pub fn new(
        cold: &'c ColdStore,
        index: impl Fn(&BlockHash) -> eyre::Result<Option<ColdLocation>> + 'c,
    ) -> Self {
        Self { cold, index: Box::new(index), faulted: Rc::new(Cell::new(false)) }
    }

    /// Returns the shared fault flag, raised when a scan died on a fault instead of exhausting.
    pub fn faulted(&self) -> Rc<Cell<bool>> {
        Rc::clone(&self.faulted)
    }

    /// The `'c`-bound typed scan, outliving the handle itself so a merging caller can own it.
    pub(crate) fn scan<T: Table>(
        &self,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> Option<DBIter<'c, T>> {
        typed_stream::<T>(self.cold, from, reverse, Rc::clone(&self.faulted))
    }

    /// The `'c`-bound raw scan, outliving the handle itself so a merging caller can own it.
    pub(crate) fn raw_scan<T: Table>(
        &self,
        from: Option<&T::Key>,
        reverse: bool,
    ) -> Option<DBRawIter<'c>> {
        raw_stream::<T>(self.cold, from, reverse, Rc::clone(&self.faulted))
    }
}

impl DbTx for ColdTx<'_> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        cold_get::<T>(self.cold, key, |digest| (self.index)(digest)).map_err(cold_to_eyre)
    }

    fn raw_get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<Cow<'_, [u8]>>> {
        Ok(cold_raw::<T>(self.cold, key, |digest| (self.index)(digest))
            .map_err(cold_to_eyre)?
            .map(Cow::Owned))
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        cold_has::<T>(self.cold, key, |digest| (self.index)(digest)).map_err(cold_to_eyre)
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        self.scan::<T>(None, false).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.raw_scan::<T>(None, false).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        Ok(self.scan::<T>(Some(key), false).unwrap_or_else(|| Box::new(std::iter::empty())))
    }

    fn raw_skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBRawIter<'_>> {
        Ok(self.raw_scan::<T>(Some(key), false).unwrap_or_else(|| Box::new(std::iter::empty())))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.scan::<T>(None, true).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        self.raw_scan::<T>(None, true).unwrap_or_else(|| Box::new(std::iter::empty()))
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.reverse_iter::<T>().next()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        // A reverse scan capped one key below the target: dense numbering makes its first row
        // the prior record, one positioned read instead of a walk.
        let number = archived_number::<T>(key).ok()?;
        let prior = number.checked_sub(1)?;
        let prior_key = try_decode_key::<T::Key>(&encode_key(&prior)).ok()?;
        self.scan::<T>(Some(&prior_key), true)?.next()
    }

    fn disable_long_read_safety(&self) {
        // Jar reads run on mmapped files with no read-txn window to exempt.
    }
}

/// A write transaction over the cold tier: the open epoch's jars across both segments.
///
/// The cold tier is append-only. `insert` appends into the open epoch jar, with
/// `ConsensusBlocks` numbering enforced dense from the epoch's start key; `commit` seals both
/// jars; dropping without commit abandons the appends for the next [`ColdTxMut::begin`] to heal.
/// Removal has no jar representation, so `remove`, `clear_table` and `evict_persistent_batch`
/// fail closed.
pub struct ColdTxMut<'c> {
    /// The cold store written.
    cold: &'c ColdStore,
    /// Next expected `ConsensusBlocks` number, advancing densely from the epoch's start key.
    next_number: u64,
}

impl std::fmt::Debug for ColdTxMut<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdTxMut").field("next_number", &self.next_number).finish_non_exhaustive()
    }
}

impl<'c> ColdTxMut<'c> {
    /// Begins `epoch`'s jars on both segments, with consensus blocks numbered from
    /// `start_number`; leftovers of an abandoned earlier attempt are healed here.
    pub fn begin(cold: &'c ColdStore, epoch: Epoch, start_number: u64) -> ColdResult<Self> {
        cold.consensus_blocks().begin_epoch(epoch, start_number)?;
        cold.batches().begin_epoch(epoch, 0)?;
        Ok(Self { cold, next_number: start_number })
    }

    /// Appends an already-encoded row for `T`, the zero-decode path archival writes through.
    pub(crate) fn append_raw<T: Table>(&mut self, key: &T::Key, value: &[u8]) -> ColdResult<()> {
        match cold_kind::<T>() {
            Some(ColdKind::ConsensusBlocks) => {
                let number = archived_number::<T>(key)?;
                if number != self.next_number {
                    return Err(ColdError::Corruption(format!(
                        "cold append out of order: block {number} where {expected} was next",
                        expected = self.next_number
                    )));
                }
                self.cold.consensus_blocks().append_row(&[value])?;
                self.next_number += 1;
                Ok(())
            }
            Some(ColdKind::Batches) => {
                let digest = archived_digest::<T>(key)?;
                self.cold.batches().append_row(&[digest.as_slice(), value])
            }
            None => {
                Err(ColdError::Corruption(format!("table {} has no cold representation", T::NAME)))
            }
        }
    }

    /// Seals both segments with typed errors, the form archival consumes.
    ///
    /// Batches commits before consensus_blocks, and reconcile/archive gate "sealed" on the
    /// consensus_blocks jar alone: a crash between the commits leaves the epoch un-sealed
    /// (batches jar durable but orphaned) and the next pass re-archives it whole, rather than
    /// stranding a half-sealed epoch whose hot rows a later prune would evict with no cold copy.
    pub(crate) fn seal(self) -> ColdResult<()> {
        self.cold.batches().commit()?;
        self.cold.consensus_blocks().commit()
    }
}

impl DbTx for ColdTxMut<'_> {
    fn get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
        panic!("DbTx get() should not be called on a DbTxMut!");
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx iter() should not be called on a DbTxMut!");
    }

    fn raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx raw_iter() should not be called on a DbTxMut!");
    }

    fn skip_to<T: Table>(&self, _key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        panic!("DbTx skip_to() should not be called on a DbTxMut!");
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        panic!("DbTx reverse_iter() should not be called on a DbTxMut!");
    }

    fn reverse_raw_iter<T: Table>(&self) -> DBRawIter<'_> {
        panic!("DbTx reverse_raw_iter() should not be called on a DbTxMut!");
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        panic!("DbTx last_record() should not be called on a DbTxMut!");
    }

    fn record_prior_to<T: Table>(&self, _key: &T::Key) -> Option<(T::Key, T::Value)> {
        panic!("DbTx record_prior_to() should not be called on a DbTxMut!");
    }

    fn disable_long_read_safety(&self) {
        // Jar writes run outside any read-txn window; nothing to exempt.
    }
}

impl DbTxMut for ColdTxMut<'_> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.append_raw::<T>(key, &encode(value)).map_err(cold_to_eyre)
    }

    fn remove<T: Table>(&mut self, _key: &T::Key) -> eyre::Result<()> {
        Err(eyre::eyre!("cold tier is append-only: remove has no jar representation"))
    }

    fn evict_persistent_batch<T: Table>(&mut self, _keys: &[T::Key]) -> eyre::Result<()> {
        Err(eyre::eyre!("cold tier is append-only: eviction has no jar representation"))
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        Err(eyre::eyre!("cold tier is append-only: clear has no jar representation"))
    }

    fn commit(self) -> eyre::Result<()> {
        self.seal().map_err(cold_to_eyre)
    }
}

/// Returns `key`'s raw cold-jar bytes, or `None` if the cold tier does not hold it.
///
/// `lookup` resolves the hot `ColdBatchLocations` auxiliary index on the caller's own hot view (a
/// held snapshot, or the layered read path), so a cold serve stays consistent with the hot reads
/// around it. Non-archived tables are `None`.
fn cold_raw<T: Table>(
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
fn cold_get<T: Table>(
    cold: &ColdStore,
    key: &T::Key,
    lookup: impl FnOnce(&BlockHash) -> eyre::Result<Option<ColdLocation>>,
) -> ColdResult<Option<T::Value>> {
    cold_raw::<T>(cold, key, lookup)?.map(|bytes| decode_cold::<T>(&bytes)).transpose()
}

/// Returns true if the cold tier holds `key`.
fn cold_has<T: Table>(
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

/// Builds the typed cold stream behind [`ColdTx::scan`], or `None` when `T` has no cold key
/// order.
///
/// Only `ConsensusBlocks` qualifies: dense numeric keys give the jars a total order, so the
/// stream composes from the checked point read (each row verified and owned). `from` floors a
/// forward scan and caps a reverse one. `DBIter` is infallible, so a read error, in-span gap, or
/// codec failure ends the stream after an `error!` log, raising `flag` (never raised on clean
/// exhaustion) so a merging caller can end its whole merged stream: a corrupt jar must read as
/// truncation at the fault, never as a gap the hot tail continues past.
fn typed_stream<'c, T: Table>(
    cold: &'c ColdStore,
    from: Option<&T::Key>,
    reverse: bool,
    flag: Rc<Cell<bool>>,
) -> Option<DBIter<'c, T>> {
    let numbers = cold_scan_numbers::<T>(cold, from, reverse)?;
    Some(Box::new(numbers.map_while(move |n| {
        let bytes = read_cold_block(cold, n, &flag)?;
        let key = match try_decode_key::<T::Key>(&encode_key(&n)) {
            Ok(key) => key,
            Err(e) => {
                flag.set(true);
                error!(target: "cold_store", number = n, "cold scan key decode failed, ending iteration: {e}");
                return None;
            }
        };
        match decode_cold::<T>(&bytes) {
            Ok(value) => Some((key, value)),
            Err(e) => {
                flag.set(true);
                error!(target: "cold_store", number = n, "cold scan value decode failed, ending iteration: {e}");
                None
            }
        }
    })))
}

/// Raw-bytes twin of [`typed_stream`], behind [`ColdTx::raw_scan`]: yields the encoded key and
/// the jar's bcs value bytes.
fn raw_stream<'c, T: Table>(
    cold: &'c ColdStore,
    from: Option<&T::Key>,
    reverse: bool,
    flag: Rc<Cell<bool>>,
) -> Option<DBRawIter<'c>> {
    let numbers = cold_scan_numbers::<T>(cold, from, reverse)?;
    Some(Box::new(numbers.map_while(move |n| {
        let bytes = read_cold_block(cold, n, &flag)?;
        Some((Cow::Owned(encode_key(&n)), Cow::Owned(bytes)))
    })))
}

/// Yields the block numbers a cold scan visits, or `None` when `T` has no cold key order, the
/// segment is empty, or `from` starts past the sealed span.
fn cold_scan_numbers<'c, T: Table>(
    cold: &ColdStore,
    from: Option<&T::Key>,
    reverse: bool,
) -> Option<Box<dyn Iterator<Item = u64> + 'c>> {
    if !matches!(cold_kind::<T>()?, ColdKind::ConsensusBlocks) {
        return None;
    }
    let span = cold.consensus_blocks().key_span()?;
    let (start, end) = match from {
        None => (*span.start(), *span.end()),
        Some(key) => match archived_number::<T>(key) {
            // Forward, `from` floors the scan; reverse, it caps it (walk-back ceiling).
            Ok(n) if reverse => (*span.start(), n.min(*span.end())),
            Ok(n) => (n.max(*span.start()), *span.end()),
            Err(e) => {
                error!(target: "cold_store", "cold scan floor decode failed, skipping cold: {e}");
                return None;
            }
        },
    };
    (start <= end).then(|| -> Box<dyn Iterator<Item = u64> + 'c> {
        if reverse {
            Box::new((start..=end).rev())
        } else {
            Box::new(start..=end)
        }
    })
}

/// Reads one verified cold block for a scan, mapping every failure to stream end with a log and
/// the fault flag raised.
///
/// An `Ok(None)` inside the sealed span is a gap the contiguous-epoch invariant forbids, so it is
/// reported as corruption rather than skipped: skipping would silently misrepresent history.
fn read_cold_block(cold: &ColdStore, n: u64, faulted: &Cell<bool>) -> Option<Vec<u8>> {
    match cold.read_consensus_block_checked(n) {
        Ok(Some(bytes)) => Some(bytes),
        Ok(None) => {
            faulted.set(true);
            error!(target: "cold_store", number = n, "cold scan hit a gap inside the sealed span, ending iteration");
            None
        }
        Err(e) => {
            faulted.set(true);
            error!(target: "cold_store", number = n, "cold scan read failed, ending iteration: {e}");
            None
        }
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
