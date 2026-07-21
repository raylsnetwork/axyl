//! The cold archiver: chunked seal pass plus the index-and-prune finalize tail.
//!
//! [`archive_below_epoch`] migrates whole epochs of `Batches` and `ConsensusBlocks` out of the
//! hot MDBX into per-epoch nippy jars; [`reconcile`](super::reconcile()) heals an interrupted
//! migration. The ordering is crash-atomic so a row is never absent from both tiers (the serve
//! floor is genesis, so a missing row is a fatal protocol violation): jars are made durable, then
//! the hot auxiliary index and high-water are synced, and only then are the hot rows removed.
//!
//! Every MDBX access here is scoped in an explicit `with_read_txn` / `with_write_txn` closure; jar
//! file I/O happens outside those closures so a read txn never spans disk writes.

use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::OnceLock,
    time::{Duration, Instant},
};

use prometheus::{register_int_gauge_with_registry, IntGauge};
use rayls_infrastructure_types::{
    decode_key, leader_epoch_and_batch_digests, B256Set, BlockHash, Database, DbTx, DbTxMut, Epoch,
};
use tracing::info;

use super::{ColdError, ColdLocation, ColdResult, ColdStore, ARCHIVE_HIGH_WATER_KEY};
use crate::tables::{Batches, ColdArchiveHighWater, ColdBatchLocations, ConsensusBlocks};

/// A header plus its archivable batch rows, captured under a read txn for the later jar phase.
type PendingBlock = (Vec<u8>, Vec<(BlockHash, Vec<u8>)>);

/// Header-byte budget per seal chunk, bounding peak seal memory to one chunk (plus the block that
/// crosses it) rather than a whole multi-gigabyte epoch.
pub(super) const SEAL_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// Fresh-digest cap per seal chunk: batch sizes are unknown until read, so the batch side is
/// bounded by count (wire-capped near 2MiB each) instead of bytes.
const MAX_CHUNK_DIGESTS: usize = 64;

/// Blocks appended between seal-progress log lines, keeping a minutes-long pass observably alive
/// at a handful of lines per epoch.
const SEAL_PROGRESS_LOG_BLOCKS: u64 = 50_000;

/// Returns the process-wide high-water gauge (last fully-archived epoch), registered on first use.
fn high_water_gauge() -> &'static IntGauge {
    static GAUGE: OnceLock<IntGauge> = OnceLock::new();
    GAUGE.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            register_int_gauge_with_registry!(
                "cold_archive_high_water_epoch",
                "Last fully-archived (sealed and indexed) cold epoch.",
                registry,
            )
        })
    })
}

/// Outcome of a jar-only seal pass (no index or hot-prune tail).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealOutcome {
    /// One epoch's jars were sealed (committed durable); its hot rows and index remain untouched
    /// until a finalize ([`reconcile`](super::reconcile())) runs.
    Sealed(Epoch),
    /// Every epoch below the cutoff is already sealed; nothing to do.
    Drained,
    /// The cancel flag tripped at a chunk seam; the jars are uncommitted and the next
    /// `begin_epoch` heals them, so the epoch re-seals whole on retry.
    Cancelled,
}

/// Counts of rows moved to cold by a single [`archive_below_epoch`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveStats {
    /// Number of `Batches` rows archived.
    pub batches_archived: u64,
    /// Number of `ConsensusBlocks` rows archived.
    pub blocks_archived: u64,
    /// Number of epochs newly sealed in cold by this pass.
    pub epochs_sealed: u64,
}

impl ArchiveStats {
    /// Returns the field-wise sum of two passes' stats.
    fn merge(self, other: Self) -> Self {
        Self {
            batches_archived: self.batches_archived + other.batches_archived,
            blocks_archived: self.blocks_archived + other.blocks_archived,
            epochs_sealed: self.epochs_sealed + other.epochs_sealed,
        }
    }
}

/// Archives epochs below `cutoff_epoch` into cold, at most `max_epochs` per pass (`None` is
/// unbounded); a larger backlog drains over later resumable passes.
///
/// `db` must be the RAW hot database: reading through the cold fall-through would re-archive
/// just-archived payloads. Errors if a batch digest is absent from both tiers.
pub fn archive_below_epoch<DB: Database>(
    db: &DB,
    cold: &ColdStore,
    cutoff_epoch: Epoch,
    max_epochs: Option<usize>,
) -> ColdResult<ArchiveStats> {
    let cap = max_epochs.unwrap_or(usize::MAX);
    let mut stats = ArchiveStats::default();

    // Each seal resumes past the sealed-jar tip, so the scans are O(history) in total rather
    // than O(epochs * history), and the resumable tip drains any remaining backlog over later
    // passes.
    while (stats.epochs_sealed as usize) < cap {
        let Some(sealed) = seal_next_epoch(db, cold, cutoff_epoch, SEAL_CHUNK_BYTES)? else {
            break; // backlog below the cutoff is drained
        };
        stats = stats.merge(sealed);
    }
    Ok(stats)
}

/// Seals and immediately finalizes the lowest unarchived epoch below `cutoff_epoch`, or returns
/// `None` once that backlog is drained: the fused shape used by boot and offline migration; the
/// live actor splits the phases ([`ColdArchiver::seal_due`](super::ColdArchiver::seal_due)).
pub(super) fn seal_next_epoch<DB: Database>(
    db: &DB,
    cold: &ColdStore,
    cutoff_epoch: Epoch,
    chunk_bytes: usize,
) -> ColdResult<Option<ArchiveStats>> {
    let seal_started = Instant::now();
    match seal_next_epoch_jars(db, cold, cutoff_epoch, chunk_bytes, &|| false)? {
        JarSeal::Sealed(sealed) => {
            let seal = seal_started.elapsed();
            let finalize_started = Instant::now();
            let stats = finalize_sealed(db, &sealed, &|| false, Duration::ZERO)?;
            // The fused path's per-epoch progress, mirroring the live actor's "cold pass phases";
            // `remaining` counts the epochs still due below the cutoff, so a long boot or
            // migration drain shows its distance to done.
            info!(
                target: "cold-archive",
                epoch = sealed.epoch,
                remaining = cutoff_epoch - sealed.epoch - 1,
                seal = ?seal,
                finalize = ?finalize_started.elapsed(),
                "cold backlog epoch archived",
            );
            Ok(Some(stats))
        }
        // The flag above never trips, so Cancelled is unreachable and folds into "nothing sealed".
        JarSeal::Drained | JarSeal::Cancelled => Ok(None),
    }
}

/// A sealed epoch's jar metadata, carried from the seal to its index+prune tail so the finalize
/// never re-walks the jar.
pub(super) struct SealedJars {
    /// The sealed epoch.
    pub(super) epoch: Epoch,
    /// The epoch's dense consensus-block range.
    pub(super) numbers: RangeInclusive<u64>,
    /// The epoch's deduped batch digests with their jar rows, in append order.
    pub(super) locations: Vec<(BlockHash, ColdLocation)>,
}

/// Outcome of [`seal_next_epoch_jars`].
pub(super) enum JarSeal {
    /// The epoch's jars are committed durable; the payload feeds [`finalize_sealed`].
    Sealed(SealedJars),
    /// Every epoch below the cutoff is already sealed.
    Drained,
    /// `should_cancel` tripped at a chunk seam; the jars are uncommitted, hot rows intact.
    Cancelled,
}

/// Finalizes one sealed epoch from its carried metadata: index + high-water in one synced txn,
/// then the hot prune in bounded batches. The tail of the crash-atomic ordering; the jars are
/// already durable. The live archiver passes its cancel flag and [`PRUNE_YIELD`]; the fused
/// boot/offline paths pass no-cancel and zero yield (nothing shares the hot writer yet).
pub(super) fn finalize_sealed<DB: Database>(
    db: &DB,
    sealed: &SealedJars,
    should_cancel: &(impl Fn() -> bool + ?Sized),
    yield_between: Duration,
) -> ColdResult<ArchiveStats> {
    commit_index_and_high_water(db, sealed.epoch, &sealed.locations)?;
    let blocks_archived = delete_archived_rows(
        db,
        sealed.numbers.clone(),
        &sealed.locations,
        should_cancel,
        yield_between,
    )?;
    Ok(ArchiveStats {
        batches_archived: sealed.locations.len() as u64,
        blocks_archived,
        epochs_sealed: 1,
    })
}

/// Seals the lowest unsealed epoch below `cutoff_epoch` into committed jars, without the
/// index+prune tail (a later [`reconcile`](super::reconcile()) finalizes, or
/// [`seal_next_epoch`] fuses both).
///
/// `should_cancel` is polled at each chunk seam; a cancelled pass leaves the jars uncommitted and
/// hot rows intact, so the next `begin_epoch` heals and re-seals whole.
pub(super) fn seal_next_epoch_jars<DB: Database>(
    db: &DB,
    cold: &ColdStore,
    cutoff_epoch: Epoch,
    chunk_bytes: usize,
    should_cancel: &(impl Fn() -> bool + ?Sized),
) -> ColdResult<JarSeal> {
    // Resume past the last SEALED jar's tip, not the high-water: a sealed epoch can still await
    // its finalize prune, and a high-water resume would re-open (truncate) a committed jar.
    let resume_number =
        cold.consensus_blocks().last_sealed().map(|jar| jar.end_key() + 1).unwrap_or(0);

    // A torn epoch (jar durable, high-water not advanced) still owns its hot rows, so it is
    // rediscovered and re-sealed whole.
    let Some((start_number, epoch)) = first_unsealed_block(db, resume_number)? else {
        return Ok(JarSeal::Drained);
    };
    if epoch >= cutoff_epoch {
        // A partially-filled current epoch is never sealed.
        return Ok(JarSeal::Drained);
    }

    cold.consensus_blocks().begin_epoch(epoch, start_number)?;
    cold.batches().begin_epoch(epoch, 0)?;
    info!(target: "cold-archive", epoch, start_number, "cold seal started");
    let pass_started = Instant::now();
    let mut next_progress = SEAL_PROGRESS_LOG_BLOCKS;

    // Digests written for THIS epoch, carried across chunks so a shared digest still dedups;
    // never iterated, so the fixed-bytes hasher has no ordering effect.
    let mut archived = B256Set::default();

    // Alternate read and append in bounded chunks; the rows are immutable history, so per-chunk
    // snapshots read what an epoch-wide snapshot would. A failed chunk aborts with the jars
    // uncommitted; the retry's `begin_epoch` heals the leftovers.
    let mut locations: Vec<(BlockHash, ColdLocation)> = Vec::new();
    let mut next_batch_row: u64 = 0;
    let mut next_number = start_number;
    let mut epoch_open = true;
    while epoch_open {
        // The chunk seam is the cancellation point: between chunks nothing is committed, so
        // stopping here leaves only uncommitted appends for the next `begin_epoch` to heal.
        if should_cancel() {
            return Ok(JarSeal::Cancelled);
        }
        let (pending, closed) = db
            .with_read_txn(|tx| read_seal_chunk(tx, epoch, next_number, chunk_bytes, &mut archived))
            .map_err(to_cold)?;

        // Jar phase (no MDBX): append rows in the captured order; the batches jar row index is the
        // append position, captured per digest so the auxiliary index points at the exact row.
        for (header, rows) in &pending {
            for (digest, batch_bytes) in rows {
                cold.batches().append_row(&[digest.as_slice(), batch_bytes])?;
                locations.push((*digest, ColdLocation { epoch, row: next_batch_row }));
                next_batch_row += 1;
            }
            cold.consensus_blocks().append_row(&[header])?;
        }
        // One `pending` entry per captured block, so its length advances the walk.
        next_number += pending.len() as u64;
        let blocks_done = next_number - start_number;
        if blocks_done >= next_progress {
            next_progress = (blocks_done / SEAL_PROGRESS_LOG_BLOCKS + 1) * SEAL_PROGRESS_LOG_BLOCKS;
            info!(
                target: "cold-archive",
                epoch,
                blocks = blocks_done,
                batch_rows = next_batch_row,
                elapsed = ?pass_started.elapsed(),
                "cold seal progress",
            );
        }
        epoch_open = !closed;
    }

    // Commit both jars so the `.conf` durability boundary is on disk before any hot delete.
    // NOTE: batches commits before consensus_blocks, and reconcile/archive gate "sealed" on the
    // consensus_blocks jar alone. So a crash between these commits leaves the epoch un-sealed
    // (batches jar durable but orphaned), and the next pass re-archives it whole rather than
    // stranding a half-sealed epoch whose hot rows reconcile would evict with no cold copy.
    cold.batches().commit()?;
    cold.consensus_blocks().commit()?;

    // The peek found at least one block, so the walk captured at least one and the range is
    // never empty.
    Ok(JarSeal::Sealed(SealedJars { epoch, numbers: start_number..=next_number - 1, locations }))
}

/// Reads one seal chunk under `tx`: whole blocks from `start_from` until the byte budget or
/// digest cap is crossed, plus their fresh batch payloads, assembled in append order; returns
/// whether the walk closed the epoch.
///
/// Batches are read in sorted-digest order (b-tree key order, not random probes); `archived`
/// carries the epoch's captured digests across chunks so a shared digest still dedups.
fn read_seal_chunk<TX: DbTx>(
    tx: &TX,
    epoch: Epoch,
    start_from: u64,
    chunk_bytes: usize,
    archived: &mut B256Set,
) -> eyre::Result<(Vec<PendingBlock>, bool)> {
    // The reads are immutable history, so a snapshot pinned past the mdbx read-txn timeout under
    // IO pressure must not be force-reset: a silently truncated walk would look like the epoch's
    // end and seal a short jar.
    tx.disable_long_read_safety();
    // Walk whole blocks until the header-byte budget or the fresh-digest cap is crossed; checking
    // after the push keeps at least one block per chunk (progress even with a zero budget).
    let mut headers = tx.raw_skip_to::<ConsensusBlocks>(&start_from)?;
    let mut selected: Vec<(Vec<u8>, Vec<BlockHash>)> = Vec::new();
    let mut header_bytes = 0usize;
    let mut fresh_count = 0usize;
    let mut expected = start_from;
    let mut closed = false;
    loop {
        // Table end closes the epoch like a boundary; only reachable when sealing up to the tip.
        let Some((key, value)) = headers.next() else {
            closed = true;
            break;
        };
        let number = decode_key::<u64>(&key);
        // Arithmetic jar addressing needs dense numbers: a gap aborts the pass (hot rows intact)
        // rather than sealing a misaligned jar.
        if number != expected {
            return Err(eyre::eyre!(
                "epoch {epoch} consensus blocks not contiguous: expected block {expected}, found \
                 {number}"
            ));
        }
        let (block_epoch, digests) = leader_epoch_and_batch_digests(&value)
            .map_err(|e| eyre::eyre!("project consensus block {number}: {e}"))?;
        if block_epoch != epoch {
            // The next epoch begins; leave its first block unconsumed.
            closed = true;
            break;
        }
        let header = value.into_owned();
        header_bytes += header.len();
        let fresh: Vec<BlockHash> =
            digests.into_iter().filter(|digest| archived.insert(*digest)).collect();
        fresh_count += fresh.len();
        selected.push((header, fresh));
        expected += 1;
        if header_bytes >= chunk_bytes || fresh_count >= MAX_CHUNK_DIGESTS {
            break;
        }
    }

    // `fresh_count` was tallied during selection, so the buffer is sized exactly once.
    let mut wanted: Vec<BlockHash> = Vec::with_capacity(fresh_count);
    wanted.extend(selected.iter().flat_map(|(_, fresh)| fresh.iter().copied()));
    wanted.sort_unstable();
    let mut payloads = BTreeMap::new();
    for digest in &wanted {
        match tx.raw_get::<Batches>(digest)? {
            Some(batch_bytes) => {
                payloads.insert(*digest, batch_bytes.into_owned());
            }
            // Absent from hot is tolerated only when a prior pass already archived it (then it is
            // in cold); otherwise it is in neither tier.
            None if tx.contains_key::<ColdBatchLocations>(digest)? => {}
            None => {
                return Err(eyre::eyre!("batch {digest} for epoch {epoch} missing from both tiers"))
            }
        }
    }

    // Assemble in append order: blocks ascending, each block's fresh digests in payload order. A
    // fresh digest with no payload was already archived to cold, so it gets no new row.
    let pending: Vec<PendingBlock> = selected
        .into_iter()
        .map(|(header, fresh)| {
            let rows = fresh
                .iter()
                .filter_map(|digest| payloads.remove(digest).map(|bytes| (*digest, bytes)))
                .collect();
            (header, rows)
        })
        .collect();
    Ok((pending, closed))
}

/// Returns the number and committing epoch of the first hot consensus block at or above
/// `resume_number`, or `None` when the hot tier holds nothing past the sealed tip.
fn first_unsealed_block<DB: Database>(
    db: &DB,
    resume_number: u64,
) -> ColdResult<Option<(u64, Epoch)>> {
    db.with_read_txn(|tx| match tx.raw_skip_to::<ConsensusBlocks>(&resume_number)?.next() {
        Some((key, value)) => {
            let number = decode_key::<u64>(&key);
            let (epoch, _) = leader_epoch_and_batch_digests(&value)
                .map_err(|e| eyre::eyre!("project consensus block {number}: {e}"))?;
            Ok(Some((number, epoch)))
        }
        None => Ok(None),
    })
    .map_err(to_cold)
}

/// Inserts the staged batch locations and advances the high-water to `epoch` in one hot txn,
/// drained via `sync_persist` so it is applied before any hot row is removed.
///
/// `sync_persist` orders, it does not fsync (SafeNoSync env). That suffices: MDBX commits
/// monotonically, so the later delete commit can never be durable while this one is not, and the
/// independently fsynced jars let boot [`reconcile`](super::reconcile()) rebuild the rest.
pub(super) fn commit_index_and_high_water<DB: Database>(
    db: &DB,
    epoch: Epoch,
    locations: &[(BlockHash, ColdLocation)],
) -> ColdResult<()> {
    db.with_write_txn(|txn| {
        for (digest, loc) in locations {
            txn.insert::<ColdBatchLocations>(digest, loc)?;
        }
        txn.insert::<ColdArchiveHighWater>(&ARCHIVE_HIGH_WATER_KEY, &epoch)?;
        Ok(())
    })
    .map_err(to_cold)?;
    // Drain the write queue so the high-water commit is applied before the hot delete (ordering,
    // not an fsync; see the doc above).
    db.sync_persist();
    high_water_gauge().set(epoch as i64);
    Ok(())
}

/// Rows evicted per prune commit: a whole-epoch prune in one txn would hold the single hot
/// writer (shared with live consensus) for tens of seconds; bounded batches with a yield hand it
/// back, so consensus writes never wait more than one batch.
const PRUNE_BATCH_ROWS: usize = 4096;

/// A yield between live prune batches: long enough for consensus writes queued during the batch
/// to reach the writer before the next one, and well under a consensus round.
pub(super) const PRUNE_YIELD: Duration = Duration::from_millis(5);

/// Removes an archived epoch's hot rows in bounded per-txn batches (addressed from the sealed
/// jar's metadata, no hot scan), returning the epoch's block count.
///
/// Only invoked after the jars are durable and the high-water synced. A cancelled or crashed
/// partial delete is safe: every row is already in cold, the epoch's LAST block is deleted last,
/// and [`reconcile`](super::reconcile()) probes it to sweep leftovers.
pub(super) fn delete_archived_rows<DB: Database>(
    db: &DB,
    numbers: RangeInclusive<u64>,
    locations: &[(BlockHash, ColdLocation)],
    should_cancel: &(impl Fn() -> bool + ?Sized),
    yield_between: Duration,
) -> ColdResult<u64> {
    let count = numbers.end() - numbers.start() + 1;
    let digests: Vec<BlockHash> = locations.iter().map(|(digest, _)| *digest).collect();
    let block_numbers: Vec<u64> = numbers.collect();
    // The between-chunk drain and yield hand the shared hot writer back to live consensus; with
    // zero yield (boot, migration, reconcile) nothing shares the writer, and every `sync_persist`
    // would cost its full polling quantum per chunk for nothing. Deletes queued at a crash just
    // leave more hot leftovers for reconcile's last-block probe to sweep.
    let pace = |db: &DB| {
        if !yield_between.is_zero() {
            db.sync_persist();
            std::thread::sleep(yield_between);
        }
    };
    for chunk in digests.chunks(PRUNE_BATCH_ROWS) {
        if should_cancel() {
            return Ok(count);
        }
        db.with_write_txn(|txn| txn.evict_persistent_batch::<Batches>(chunk)).map_err(to_cold)?;
        pace(db);
    }
    for chunk in block_numbers.chunks(PRUNE_BATCH_ROWS) {
        if should_cancel() {
            return Ok(count);
        }
        db.with_write_txn(|txn| txn.evict_persistent_batch::<ConsensusBlocks>(chunk))
            .map_err(to_cold)?;
        pace(db);
    }
    Ok(count)
}

/// Converts an `eyre` error from the `Database`/`DbTx` trait boundary into a [`ColdError`].
pub(super) fn to_cold(err: eyre::Report) -> ColdError {
    ColdError::Corruption(err.to_string())
}
