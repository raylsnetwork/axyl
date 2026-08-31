//! The cold archiver: chunked seal pass plus the index-and-prune finalize tail.
//!
//! Ordering is crash-atomic so a row is never absent from both tiers: jars are made durable, then
//! the hot auxiliary index and high-water mark are synced, and only then are the hot rows removed.
//! Every MDBX access is scoped in an explicit txn closure, so a read txn never spans jar writes.
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

use super::{
    ColdError, ColdLocation, ColdResult, ColdStore, ColdTxMut, ARCHIVE_HIGH_WATER_MARK_KEY,
};
use crate::tables::{Batches, ColdArchiveHighWaterMark, ColdBatchLocations, ConsensusBlocks};

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

/// Returns the process-wide high-water mark gauge (last fully-archived epoch), registered on first
/// use.
fn high_water_mark_gauge() -> &'static IntGauge {
    static GAUGE: OnceLock<IntGauge> = OnceLock::new();
    GAUGE.get_or_init(|| {
        crate::layered_db::register_metric_or_unscraped(|registry| {
            register_int_gauge_with_registry!(
                "cold_archive_high_water_mark_epoch",
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
    /// The cancel flag tripped, leaving the epoch NOT archived. At a seal chunk seam the jars are
    /// uncommitted and the next `begin_epoch` heals them, so the epoch re-seals whole; at a prune
    /// batch seam the jars and index are durable and only leftover hot rows remain, which the
    /// next [`reconcile`](super::reconcile()) sweeps. Either way the caller must retry the epoch.
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
            match finalize_sealed(db, &sealed, &|| false, Duration::ZERO)? {
                Finalized::Complete(stats) => {
                    // The fused path's per-epoch progress, mirroring the live actor's "cold pass
                    // phases"; `remaining` counts the epochs still due below the cutoff, so a long
                    // boot or migration drain shows its distance to done.
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
                // The flag above never trips, so this is unreachable alongside JarSeal::Cancelled.
                Finalized::Cancelled => Ok(None),
            }
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

/// Outcome of [`finalize_sealed`].
///
/// `#[must_use]` because dropping it silently reports a cancelled pass as a completed archive.
#[must_use]
pub(super) enum Finalized {
    /// The index, the high-water mark and the whole hot prune landed; carries the pass counts.
    Complete(ArchiveStats),
    /// `should_cancel` tripped mid-prune: the jars and the index are durable, so every row is in
    /// cold, and the leftover hot rows are swept by the next
    /// [`reconcile`](super::reconcile())'s last-block probe. The epoch is NOT fully archived.
    Cancelled,
}

/// Finalizes one sealed epoch from its carried metadata: index + high-water mark in bounded synced
/// txns, then the hot prune in bounded batches. The tail of the crash-atomic ordering; the jars
/// are already durable. The live archiver passes its cancel flag and [`PRUNE_YIELD`]; the fused
/// boot/offline paths pass no-cancel and zero yield (nothing shares the hot writer yet).
pub(super) fn finalize_sealed<DB: Database>(
    db: &DB,
    sealed: &SealedJars,
    should_cancel: &(impl Fn() -> bool + ?Sized),
    yield_between: Duration,
) -> ColdResult<Finalized> {
    commit_index(db, &sealed.locations)?;
    let Some(blocks_archived) = delete_archived_rows(
        db,
        sealed.numbers.clone(),
        &sealed.locations,
        should_cancel,
        yield_between,
    )?
    else {
        return Ok(Finalized::Cancelled);
    };
    advance_high_water_mark(db, sealed.epoch)?;
    Ok(Finalized::Complete(ArchiveStats {
        batches_archived: sealed.locations.len() as u64,
        blocks_archived,
        epochs_sealed: 1,
    }))
}

/// Seals the lowest unsealed epoch below `cutoff_epoch` into committed jars, without the
/// index+prune tail that [`reconcile`](super::reconcile()) or [`seal_next_epoch`] adds.
///
/// A cancel at a chunk seam leaves the jars uncommitted and hot rows intact, so the next
/// `begin_epoch` re-seals the epoch whole.
pub(super) fn seal_next_epoch_jars<DB: Database>(
    db: &DB,
    cold: &ColdStore,
    cutoff_epoch: Epoch,
    chunk_bytes: usize,
    should_cancel: &(impl Fn() -> bool + ?Sized),
) -> ColdResult<JarSeal> {
    // Resume past the last SEALED jar's tip, not the high-water mark: a sealed epoch can still
    // await its finalize prune, and a high-water mark resume would re-open (truncate) a
    // committed jar.
    let resume_number =
        cold.consensus_blocks().last_sealed().map(|jar| jar.end_key() + 1).unwrap_or(0);

    // An epoch whose consensus_blocks jar never committed is absent from the index, so the resume
    // above does not skip it and it is re-sealed whole. One whose jar did commit is past the
    // resume point and is finished by reconcile instead.
    let Some((start_number, epoch)) = first_unsealed_block(db, resume_number)? else {
        return Ok(JarSeal::Drained);
    };
    if epoch >= cutoff_epoch {
        // A partially-filled current epoch is never sealed.
        return Ok(JarSeal::Drained);
    }
    // `begin_epoch` hands nippy a fresh zero-row config for the epoch's file, whose consistency
    // heal truncates whatever that file already holds. An epoch that is already sealed yet still
    // owns hot rows past its jar was sealed short by an earlier pass, so re-opening it would
    // destroy the only copy of the rows that pass archived and pruned.
    if cold.consensus_blocks().is_epoch_sealed(epoch) {
        return Err(ColdError::Corruption(format!(
            "epoch {epoch} is already sealed yet still holds hot block {start_number}: its jar \
             covers only part of the epoch"
        )));
    }

    let mut jar_txn = ColdTxMut::begin(cold, epoch, start_number)?;
    info!(target: "cold-archive", epoch, start_number, "cold seal started");
    let pass_started = Instant::now();
    let mut next_progress = SEAL_PROGRESS_LOG_BLOCKS;

    // Digests written for THIS epoch, carried across chunks so a shared digest still dedups;
    // never iterated, so the fixed-bytes hasher has no ordering effect.
    let mut archived = B256Set::default();

    // Alternate read and append in bounded chunks; the rows are immutable history, so per-chunk
    // snapshots read what an epoch-wide snapshot would. A failed chunk aborts with the jar txn
    // uncommitted; the retry's begin heals the leftovers.
    let mut locations: Vec<(BlockHash, ColdLocation)> = Vec::new();
    let mut next_batch_row: u64 = 0;
    let mut next_number = start_number;
    let mut epoch_open = true;
    while epoch_open {
        // The chunk seam is the cancellation point: between chunks nothing is committed, so
        // stopping here drops the jar txn and leaves only uncommitted appends for the next begin
        // to heal.
        if should_cancel() {
            return Ok(JarSeal::Cancelled);
        }
        let (pending, closed) = db
            .with_read_txn(|tx| read_seal_chunk(tx, epoch, next_number, chunk_bytes, &mut archived))
            .map_err(to_cold)?;

        // Jar phase (no MDBX): append rows in the captured order; the batches jar row index is the
        // append position, captured per digest so the auxiliary index points at the exact row.
        for (offset, (header, rows)) in pending.iter().enumerate() {
            for (digest, batch_bytes) in rows {
                jar_txn.append_raw::<Batches>(digest, batch_bytes)?;
                locations.push((*digest, ColdLocation { epoch, row: next_batch_row }));
                next_batch_row += 1;
            }
            jar_txn.append_raw::<ConsensusBlocks>(&(next_number + offset as u64), header)?;
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

    // The peek and the walk run in separate read txns, so a row the peek saw can be gone or carry
    // another epoch by the time the walk reads it; an empty capture would build an inverted range.
    if next_number == start_number {
        return Err(ColdError::Corruption(format!(
            "epoch {epoch} seal captured no block at {start_number}"
        )));
    }
    // A mid-walk cursor error is indistinguishable from end-of-table (the raw iterator yields
    // `None` for both), and committing a short jar is unrecoverable: the finalize prunes the rows
    // that jar holds, and the epoch's remaining hot rows then look like a fresh epoch to seal.
    // Nothing is committed yet, so confirming the walk really reached the epoch end is free.
    if let Some((number, block_epoch)) = first_unsealed_block(db, next_number)? {
        if block_epoch == epoch {
            return Err(ColdError::Corruption(format!(
                "epoch {epoch} seal walk stopped at block {next_number} but block {number} still \
                 belongs to it"
            )));
        }
    }

    // Commit both jars so the `.conf` durability boundary is on disk before any hot delete; the
    // crash-window ordering argument lives on `ColdTxMut::seal`.
    jar_txn.seal()?;

    // The empty-capture check above makes the range non-empty by construction.
    Ok(JarSeal::Sealed(SealedJars { epoch, numbers: start_number..=next_number - 1, locations }))
}

/// Reads one seal chunk under `tx`: whole blocks from `start_from` until the byte budget or digest
/// cap is crossed, plus their fresh batch payloads in append order, and whether the epoch closed.
///
/// `archived` carries the epoch's captured digests across chunks so a shared digest still dedups.
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

    // `fresh_count` was tallied during selection, so the buffer is sized exactly once. Sorted
    // because the fetch below walks `Batches` in b-tree key order rather than probing at random,
    // which is what keeps a chunk's payload reads sequential across a multi-gigabyte table.
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

/// Inserts the staged batch locations in bounded hot txns and drains them.
///
/// The drain orders, it does not fsync (SafeNoSync env): MDBX commits monotonically, so a later
/// delete can never be durable while this is not, and the fsynced jars let boot reconcile rebuild.
pub(super) fn commit_index<DB: Database>(
    db: &DB,
    locations: &[(BlockHash, ColdLocation)],
) -> ColdResult<()> {
    for chunk in locations.chunks(WRITE_BATCH_ROWS) {
        db.with_write_txn(|txn| {
            for (digest, loc) in chunk {
                txn.insert::<ColdBatchLocations>(digest, loc)?;
            }
            Ok(())
        })
        // A txn-closure failure is a hot-tier fault (mem-cache mutation or a dead writer thread),
        // not corruption: it must take the fatal path, not retry forever. The underlying MDBX
        // write failure of these ops surfaces via the sync_persist below instead.
        .map_err(to_write_failed)?;
    }
    // Every location must be applied before the prune removes the rows they address.
    db.sync_persist().map_err(to_write_failed)?;
    Ok(())
}

/// Marks `epoch` fully archived, after its index and its whole hot prune have landed.
///
/// NOTE: this is the LAST step of a finalize, never an earlier one. The high-water mark's only
/// meaning is "archived through here", which is what lets reconcile skip an epoch without probing
/// it. An interruption anywhere before this leaves it un-advanced, so reconcile re-runs the whole
/// finalize for that epoch, which is idempotent. Advancing it earlier would mark an epoch done
/// while its rows were still hot, and nothing would ever sweep them.
pub(super) fn advance_high_water_mark<DB: Database>(db: &DB, epoch: Epoch) -> ColdResult<()> {
    db.with_write_txn(|txn| {
        txn.insert::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY, &epoch)
    })
    // Same hot-tier fault classification as `commit_index`; see the comment there.
    .map_err(to_write_failed)?;
    db.sync_persist().map_err(to_write_failed)?;
    high_water_mark_gauge().set(epoch as i64);
    Ok(())
}

/// Rows written per hot commit, indexing and pruning alike: a whole epoch in one txn holds the
/// single hot writer (shared with live consensus) for tens of seconds, and the writer queue's
/// over-mark pacing sleeps under it.
///
/// NOTE: this bounds the transaction, and the mem-write-lock serialization makes each
/// `with_write_txn` a real commit point. A consensus write that needs the hot writer blocks until
/// this transaction's `CommitTxn` is enqueued, rather than merging into the archiver's transaction.
const WRITE_BATCH_ROWS: usize = 4096;

/// A yield between live prune batches: long enough for consensus writes queued during the batch
/// to reach the writer before the next one, and well under a consensus round.
pub(super) const PRUNE_YIELD: Duration = Duration::from_millis(5);

/// Removes an archived epoch's hot rows in bounded per-txn batches addressed from the sealed jar,
/// returning the block count, or `None` if `should_cancel` tripped mid-prune.
///
/// Only safe after the jars are durable and the high-water mark synced. A partial delete strands
/// nothing: the epoch's LAST block is deleted last, which is what reconcile probes to sweep it.
pub(super) fn delete_archived_rows<DB: Database>(
    db: &DB,
    numbers: RangeInclusive<u64>,
    locations: &[(BlockHash, ColdLocation)],
    should_cancel: &(impl Fn() -> bool + ?Sized),
    yield_between: Duration,
) -> ColdResult<Option<u64>> {
    let count = numbers.end() - numbers.start() + 1;
    let digests: Vec<BlockHash> = locations.iter().map(|(digest, _)| *digest).collect();
    // The between-chunk drain and yield hand the shared hot writer back to live consensus; with
    // zero yield (boot, migration, reconcile) nothing shares the writer, and every `sync_persist`
    // would cost its full polling quantum per chunk for nothing. Deletes queued at a crash just
    // leave more hot leftovers for reconcile's last-block probe to sweep.
    let pace = |db: &DB| -> ColdResult<()> {
        if !yield_between.is_zero() {
            db.sync_persist().map_err(to_write_failed)?;
            std::thread::sleep(yield_between);
        }
        Ok(())
    };
    for chunk in digests.chunks(WRITE_BATCH_ROWS) {
        if should_cancel() {
            return Ok(None);
        }
        db.with_write_txn(|txn| txn.evict_persistent_batch::<Batches>(chunk))
            // Hot-tier fault, not corruption; see `commit_index`.
            .map_err(to_write_failed)?;
        pace(db)?;
    }
    // Chunk the dense range directly instead of materializing every block number up front; only
    // one chunk's numbers are ever held at a time.
    let mut chunk_start = *numbers.start();
    loop {
        if should_cancel() {
            return Ok(None);
        }
        let chunk_end = chunk_start.saturating_add(WRITE_BATCH_ROWS as u64 - 1).min(*numbers.end());
        let chunk: Vec<u64> = (chunk_start..=chunk_end).collect();
        db.with_write_txn(|txn| txn.evict_persistent_batch::<ConsensusBlocks>(&chunk))
            // Hot-tier fault, not corruption; see `commit_index`.
            .map_err(to_write_failed)?;
        pace(db)?;
        match chunk_end.checked_add(1) {
            Some(next) if next <= *numbers.end() => chunk_start = next,
            _ => break,
        }
    }
    Ok(Some(count))
}

/// Converts an `eyre` error from the `Database`/`DbTx` trait boundary into a [`ColdError`].
pub(super) fn to_cold(err: eyre::Report) -> ColdError {
    ColdError::Corruption(err.to_string())
}

/// Converts a hot-tier durability-barrier failure into a [`ColdError::WriteFailed`], the variant
/// the seal actor treats as fatal for the node (unlike corruption, which is retried).
pub(super) fn to_write_failed(err: eyre::Report) -> ColdError {
    ColdError::WriteFailed(err.to_string())
}
