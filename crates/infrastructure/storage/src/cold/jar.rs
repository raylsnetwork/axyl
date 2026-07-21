//! Per-segment nippy-jar handles and the two-segment cold store.
//!
//! A [`ColdSegment`] owns one nippy jar per archived epoch plus an in-memory index rebuilt at boot
//! from the jars' `.conf` headers. The consensus_blocks segment is addressed arithmetically by
//! block number minus the jar start key; the batches segment is addressed by an explicit
//! [`ColdLocation`] resolved from the hot auxiliary index. The digest column of a batches jar keeps
//! it self-describing, so the auxiliary index is never the sole source of truth (reconcile rebuilds
//! it from the consensus_blocks projection).

use std::{
    collections::BTreeSet,
    fs,
    num::NonZeroUsize,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
};

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use rayls_infrastructure_types::{BlockHash, ConsensusHeaderChainMeta, Epoch};
use reth_nippy_jar::{
    compression::{Compressors, Zstd},
    DataReader, NippyJar, NippyJarCursor, NippyJarWriter, CONFIG_FILE_EXTENSION,
};

use super::{
    ColdConfig, ColdError, ColdJarHeader, ColdLocation, ColdResult, ColdSegmentKind, JarIndex,
    SealedJar,
};

/// Zstd level for cold jars, set to the measured knee of the speed/ratio curve on real batch data.
///
/// Batch payloads are RLP-encoded txs (signatures + calldata), so they are near-incompressible: on
/// an 8 GB sample level 3 reaches 1.58x at near-lz4 speed, while level 19 spends ~16x the CPU for
/// only 1.61x. Decompression speed is level-independent, so serving reads are unaffected.
const COLD_ZSTD_LEVEL: i32 = 3;

/// Number of columns in the consensus_blocks segment: `[bcs(ConsensusHeader)]`.
pub(crate) const CONSENSUS_BLOCKS_COLUMNS: usize = 1;

/// Number of columns in the batches segment: `[digest(32B), bcs(Batch)]`.
pub(crate) const BATCHES_COLUMNS: usize = 2;

/// Column index of the batch payload within a batches jar row.
const BATCH_PAYLOAD_COLUMN: usize = 1;

/// Number of open jars a segment keeps mmapped for reuse across reads.
///
/// Each cached [`LoadedJar`] pins two file descriptors and two mmaps (see [`DataReader`]), so the
/// cache is bounded: reth's static-file cache is unbounded, but cold epochs grow without limit, so
/// a small LRU caps the descriptor and address-space cost while still serving read bursts that
/// cluster on a few recent epochs from the open handles.
const JAR_CACHE_CAPACITY: usize = 16;

/// An open writer plus the header it is accumulating for the in-progress epoch jar.
///
/// Held behind a [`Mutex`] so the producer can drive `append_row`/`commit` through `&self` without
/// promoting the receiver to `&mut self`, which would break sharing the store via `Arc`.
#[derive(Debug)]
struct OpenJar {
    /// Active nippy-jar writer for the epoch being sealed.
    writer: NippyJarWriter<ColdJarHeader>,
    /// Identity header frozen into the jar's `.conf` at [`ColdSegment::begin_epoch`].
    header: ColdJarHeader,
}

/// A jar opened for reading: its config plus an `Arc` data reader so cursors share one open mmap
/// instead of re-opening the file per row (mirrors reth-provider's `LoadedJar`).
struct LoadedJar {
    /// Jar config and header, the borrow target for a cursor.
    jar: NippyJar<ColdJarHeader>,
    /// Shared handle to the jar's data and offset mmaps, cloned into each cursor.
    reader: Arc<DataReader>,
}

impl LoadedJar {
    /// Opens the jar at `path`, mmapping its data and offset files once.
    ///
    /// Validates the offsets' claimed data end against the mmapped file: a truncated data file
    /// would otherwise panic the unchecked slice on first read, and panic=abort makes that a node
    /// abort rather than a surfaced corruption.
    fn load(path: &Path) -> ColdResult<Self> {
        let jar = NippyJar::<ColdJarHeader>::load(path)?;
        let reader = Arc::new(jar.open_data_reader()?);
        let claimed = reader.reverse_offset(0)? as usize;
        if claimed > reader.size() {
            return Err(ColdError::Corruption(format!(
                "jar {path:?} data file truncated: offsets end at {claimed}, file holds {}",
                reader.size()
            )));
        }
        Ok(Self { jar, reader })
    }

    /// Returns a fresh cursor over this jar that shares the already-open mmap.
    fn cursor(&self) -> ColdResult<NippyJarCursor<'_, ColdJarHeader>> {
        Ok(NippyJarCursor::with_reader(&self.jar, Arc::clone(&self.reader))?)
    }
}

/// One nippy-jar-backed cold segment (a kind, its directory, and an in-memory jar index).
///
/// A segment owns the single writer for its kind and serves arithmetic (consensus_blocks) or
/// location-addressed (batches) row reads. The index maps each sealed jar's end key to its
/// [`SealedJar`] entry and is rebuilt from the on-disk jars at [`ColdSegment::open`].
#[derive(Debug)]
pub struct ColdSegment {
    /// Segment kind, fixing column layout and addressing scheme.
    kind: ColdSegmentKind,
    /// Directory holding this segment's jars and satellite files.
    dir: PathBuf,
    /// End-key -> sealed-jar index, rebuilt at boot from the on-disk jars.
    index: RwLock<JarIndex>,
    /// Writer for the epoch currently being sealed, if any.
    open: Mutex<Option<OpenJar>>,
    /// Bounded LRU of open jars, so read bursts on the same epoch reuse one mmap.
    ///
    /// `Mutex` not `RwLock` because an LRU `get` reorders (mirrors reth's jar-provider cache).
    cache: Mutex<LruCache<Epoch, Arc<LoadedJar>>>,
}

impl ColdSegment {
    /// Opens the segment at `dir`, rebuilding the in-memory index from each jar's `.conf` header.
    ///
    /// A missing directory is created and treated as an empty segment, mirroring reth's
    /// `iter_static_files` -> `initialize_index` boot scan.
    pub fn open(dir: impl AsRef<Path>, kind: ColdSegmentKind) -> ColdResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let index = Self::rebuild_index(&dir, kind)?;

        let capacity =
            NonZeroUsize::new(JAR_CACHE_CAPACITY).expect("jar cache capacity is nonzero");
        Ok(Self {
            kind,
            dir,
            index: RwLock::new(index),
            open: Mutex::new(None),
            cache: Mutex::new(LruCache::new(capacity)),
        })
    }

    /// Rebuilds the in-memory index by loading every sealed jar `.conf` under `dir`.
    fn rebuild_index(dir: &Path, kind: ColdSegmentKind) -> ColdResult<JarIndex> {
        let mut index = JarIndex::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            // The `.conf` is the durable commit boundary; a jar without one was never sealed and is
            // healed by the writer's consistency check, so it must not enter the read index.
            if path.extension().and_then(|e| e.to_str()) != Some(CONFIG_FILE_EXTENSION) {
                continue;
            }
            // nippy addresses satellites off the data-file stem (no extension), so strip `.conf`.
            let data_path = path.with_extension("");
            let jar = NippyJar::<ColdJarHeader>::load(&data_path)?;
            // The row count comes from the jar itself, never a persisted copy (see
            // [`SealedJar`]). The writer freezes a zero-row `.conf` at jar creation, before any
            // row is committed; such a jar marks an aborted or in-progress seal, not a durable
            // one, so it must not enter the read index; the writer's consistency check heals it
            // on the next reopen.
            let rows = jar.rows() as u64;
            if rows > 0 {
                let header = jar.user_header();
                let sealed = SealedJar { epoch: header.epoch, start_key: header.start_key, rows };
                index.insert(Self::index_key(kind, &sealed), sealed);
            }
        }
        Ok(index)
    }

    /// Returns true if the given epoch already has a sealed jar in the index (idempotent re-run).
    pub fn is_epoch_sealed(&self, epoch: Epoch) -> bool {
        match self.kind {
            // Batches jars are epoch-keyed, so presence is an O(log n) lookup. This is on the
            // served-batch read path (`read_batch_checked`), so it must not scan every jar.
            ColdSegmentKind::Batches => {
                self.index.read().contains_key(&Self::index_key_for_epoch(epoch))
            }
            // Consensus-blocks jars are end-key-keyed, so presence requires a scan by epoch.
            ColdSegmentKind::ConsensusBlocks => {
                self.index.read().values().any(|h| h.epoch == epoch)
            }
        }
    }

    /// Opens a fresh jar for `epoch` rooted at `start_key`, making it the active append target.
    ///
    /// `start_key` is the first block number for consensus_blocks and an unused sentinel for
    /// batches. Public so out-of-crate harnesses (benches) can author fixture jars.
    pub fn begin_epoch(&self, epoch: Epoch, start_key: u64) -> ColdResult<()> {
        // Release any half-built writer from a failed seal first: two live writers over the same
        // jar file would collide, and the fresh writer's consistency check heals the leftovers.
        *self.open.lock() = None;

        let header = ColdJarHeader { epoch, start_key, kind: self.kind };
        let writer = NippyJarWriter::new(self.cold_zstd_jar(header.clone()))?;
        *self.open.lock() = Some(OpenJar { writer, header });
        Ok(())
    }

    /// Creates `header`'s epoch jar with the [`COLD_ZSTD_LEVEL`] compressor swapped in.
    ///
    /// Cold jars are written once by a background task (off the consensus path) and read for the
    /// chain's lifetime. nippy's `with_zstd` builds the compressor at zstd's default level and
    /// its level field is private, so the level-set compressor goes in through the public handle.
    fn cold_zstd_jar(&self, header: ColdJarHeader) -> NippyJar<ColdJarHeader> {
        let path = self.jar_data_path(header.epoch);
        let mut jar = NippyJar::new(self.column_count(), &path, header).with_zstd(false, 0);
        if let Some(slot @ Compressors::Zstd(_)) = jar.compressor_mut() {
            *slot = Compressors::Zstd(
                Zstd::new(false, 0, self.column_count()).with_level(COLD_ZSTD_LEVEL),
            );
        }
        jar
    }

    /// Appends one row (all columns, left to right) to the open jar for the current epoch.
    ///
    /// Columns must be supplied in segment order: consensus_blocks `[header]`, batches `[digest,
    /// bcs]`. The last `append_column` auto-finalizes the row, so the column count must match
    /// the segment.
    pub fn append_row(&self, columns: &[&[u8]]) -> ColdResult<()> {
        let expected = self.column_count();
        if columns.len() != expected {
            return Err(ColdError::Corruption(format!(
                "row has {} columns, segment expects {expected}",
                columns.len()
            )));
        }

        let mut guard = self.open.lock();
        let open = guard
            .as_mut()
            .ok_or_else(|| ColdError::Corruption("append_row without an open epoch jar".into()))?;

        for column in columns {
            open.writer.append_column(Some(Ok(*column)))?;
        }
        Ok(())
    }

    /// Commits the open jar: fsyncs data + offsets, freezes the `.conf` boundary, indexes it.
    ///
    /// The `.conf` write is nippy's atomic durability boundary; only after it returns is the epoch
    /// safe to remove from the hot tier. Sealing the same epoch twice without an intervening
    /// `begin_epoch` is a no-op.
    pub fn commit(&self) -> ColdResult<()> {
        let mut guard = self.open.lock();
        let Some(mut open) = guard.take() else {
            return Ok(());
        };

        // The writer owns the authoritative row count (it survives its own consistency heal); the
        // index derives the count and covered range from it at commit, and the boot scan
        // re-derives them from the jar, so no persisted copy exists to disagree. consensus_blocks
        // rows arrive in ascending number order, so the count fixes the range.
        let rows = open.writer.rows() as u64;
        open.writer.commit()?;

        // An idempotent re-seal rewrites the epoch's file; drop any cached handle so later reads
        // mmap the fresh file rather than the stale one (mirrors reth's remove_cached_provider).
        self.cache.lock().pop(&open.header.epoch);

        // A zero-row seal leaves only the creation-time `.conf`; keep the "indexed => rows > 0"
        // invariant so the boot scan and the live index agree.
        if rows > 0 {
            let sealed =
                SealedJar { epoch: open.header.epoch, start_key: open.header.start_key, rows };
            self.index.write().insert(Self::index_key(self.kind, &sealed), sealed);
        }
        Ok(())
    }

    /// Reads a raw row by arithmetic block number (consensus_blocks segment).
    ///
    /// Returns the single bcs-encoded column value, or `None` if no jar covers `number`.
    pub fn read_by_number(&self, number: u64) -> ColdResult<Option<Vec<u8>>> {
        let Some(jar) = self.covering_jar(number) else {
            return Ok(None);
        };
        let row = number - jar.start_key;
        let loaded = self.load_jar(jar.epoch)?;
        let mut cursor = loaded.cursor()?;
        Ok(cursor.row_by_number(row as usize)?.and_then(|cols| cols.first().map(|c| c.to_vec())))
    }

    /// Returns true if a jar covers the given block number (consensus_blocks addressing).
    pub fn contains_number(&self, number: u64) -> bool {
        self.covering_jar(number).is_some()
    }

    /// Visits every row of `epoch`'s jar in ascending order, reusing one cursor for the whole
    /// scan: peak memory stays one borrowed row and a rebuild costs O(that epoch).
    pub(crate) fn for_each_row_in_epoch(
        &self,
        epoch: Epoch,
        mut visit: impl FnMut(u64, &[u8]) -> ColdResult<()>,
    ) -> ColdResult<()> {
        let Some(jar) = self.index.read().values().find(|j| j.epoch == epoch).cloned() else {
            return Ok(());
        };
        let loaded = self.load_jar(epoch)?;
        let mut cursor = loaded.cursor()?;
        for row in 0..jar.rows {
            // consensus_blocks numbers are arithmetic: jar start key plus the row offset.
            let number = jar.start_key + row;
            let columns = cursor.row_by_number(row as usize)?.ok_or_else(|| {
                ColdError::Corruption(format!("cold row {row} missing from epoch {epoch} jar"))
            })?;
            let value = columns.first().copied().ok_or_else(|| {
                ColdError::Corruption(format!("consensus_blocks row {number} has no columns"))
            })?;
            visit(number, value)?;
        }
        Ok(())
    }

    /// Returns the set of epochs that have a sealed jar in this segment.
    pub fn sealed_epochs(&self) -> BTreeSet<Epoch> {
        self.index.read().values().map(|h| h.epoch).collect()
    }

    /// Returns the newest sealed jar's index entry, or `None` if the segment has no jar (the
    /// index is keyed by end key and epochs are monotonic, so the last entry is the newest).
    pub fn last_sealed(&self) -> Option<SealedJar> {
        self.index.read().values().next_back().cloned()
    }

    /// Returns the dense `[start_key, end_key]` range archived for `epoch`, or `None` if it has
    /// no jar; reads the in-memory index only.
    pub fn key_range_for_epoch(&self, epoch: Epoch) -> Option<RangeInclusive<u64>> {
        self.index.read().values().find(|j| j.epoch == epoch).map(|j| j.start_key..=j.end_key())
    }

    /// Returns the entry of the jar whose `[start_key, end_key]` range covers `number`.
    fn covering_jar(&self, number: u64) -> Option<SealedJar> {
        self.index
            .read()
            .range(number..)
            .next()
            .map(|(_, j)| j.clone())
            .filter(|j| number >= j.start_key)
    }

    /// Returns the sealed jar for `epoch`, reusing the cached open mmap or loading and caching it.
    ///
    /// Loads outside the cache lock so a slow mmap never blocks other readers; a concurrent miss
    /// may load the same (immutable) jar twice, which is harmless and self-heals on insert.
    fn load_jar(&self, epoch: Epoch) -> ColdResult<Arc<LoadedJar>> {
        if let Some(jar) = self.cache.lock().get(&epoch).cloned() {
            return Ok(jar);
        }
        let loaded = Arc::new(LoadedJar::load(&self.jar_data_path(epoch))?);
        self.cache.lock().put(epoch, Arc::clone(&loaded));
        Ok(loaded)
    }

    /// Reads every column of one row from the `epoch` jar with a single cursor, returning them in
    /// segment order, or `None` if the row is absent.
    fn read_row(&self, epoch: Epoch, row: usize) -> ColdResult<Option<Vec<Vec<u8>>>> {
        let loaded = self.load_jar(epoch)?;
        let mut cursor = loaded.cursor()?;
        Ok(cursor.row_by_number(row)?.map(|cols| cols.iter().map(|c| c.to_vec()).collect()))
    }

    /// Returns the data-file path for `epoch`'s jar (satellites share this stem).
    ///
    /// The epoch is zero-padded so the on-disk listing sorts in archival order.
    fn jar_data_path(&self, epoch: Epoch) -> PathBuf {
        self.dir.join(format!("epoch-{epoch:010}"))
    }

    /// Returns the number of columns a jar of this segment's kind holds.
    fn column_count(&self) -> usize {
        match self.kind {
            ColdSegmentKind::ConsensusBlocks => CONSENSUS_BLOCKS_COLUMNS,
            ColdSegmentKind::Batches => BATCHES_COLUMNS,
        }
    }

    /// Returns the [`JarIndex`] key for a sealed jar of `kind`: end key for range-addressed
    /// consensus_blocks, epoch for batches (whose sentinel start key would collide end keys
    /// across epochs and drop all but the last jar).
    fn index_key(kind: ColdSegmentKind, jar: &SealedJar) -> u64 {
        match kind {
            ColdSegmentKind::ConsensusBlocks => jar.end_key(),
            ColdSegmentKind::Batches => Self::index_key_for_epoch(jar.epoch),
        }
    }

    /// Returns the [`JarIndex`] key a batches jar for `epoch` is stored under; kept in lockstep
    /// with the `Batches` arm of [`index_key`](Self::index_key).
    fn index_key_for_epoch(epoch: Epoch) -> u64 {
        u64::from(epoch)
    }
}

/// The two-segment cold store: a consensus_blocks segment plus a batches segment.
///
/// Wraps both [`ColdSegment`]s behind the read APIs [`ColdDatabase`](super::ColdDatabase) routes to
/// on a hot miss.
#[derive(Debug)]
pub struct ColdStore {
    /// Consensus-blocks segment (block number -> ConsensusHeader bytes).
    consensus_blocks: ColdSegment,
    /// Batches segment (digest -> Batch bytes), addressed via the hot auxiliary index.
    batches: ColdSegment,
}

impl ColdStore {
    /// Opens both cold segments under `cfg.dir`, rebuilding their indexes from `.conf` headers.
    pub fn open(cfg: &ColdConfig) -> ColdResult<Self> {
        let consensus_blocks =
            ColdSegment::open(cfg.dir.join("consensus_blocks"), ColdSegmentKind::ConsensusBlocks)?;
        let batches = ColdSegment::open(cfg.dir.join("batches"), ColdSegmentKind::Batches)?;
        Ok(Self { consensus_blocks, batches })
    }

    /// Returns the consensus_blocks segment.
    pub fn consensus_blocks(&self) -> &ColdSegment {
        &self.consensus_blocks
    }

    /// Returns the batches segment.
    pub fn batches(&self) -> &ColdSegment {
        &self.batches
    }

    /// Reads the raw `ConsensusHeader` bytes for `number`, cross-checking the stored header's own
    /// number so a misaligned jar surfaces as corruption instead of a silent wrong-header serve
    /// (which would feed a forked `mix_hash` / `parent_beacon_block_root`).
    ///
    /// # Errors
    ///
    /// Returns [`ColdError::Corruption`] if the stored header's number differs from `number`.
    pub fn read_consensus_block_checked(&self, number: u64) -> ColdResult<Option<Vec<u8>>> {
        let Some(bytes) = self.consensus_blocks.read_by_number(number)? else {
            return Ok(None);
        };
        let stored = ConsensusHeaderChainMeta::from_bytes(&bytes)
            .map_err(|e| ColdError::Codec(format!("project cold consensus block {number}: {e}")))?
            .number;
        if stored != number {
            return Err(ColdError::Corruption(format!(
                "consensus block {number} cold row holds block {stored} (misaligned jar)"
            )));
        }
        Ok(Some(bytes))
    }

    /// Reads the raw `Batch` bytes at `loc`, cross-checking the stored digest column so a
    /// mis-pointing auxiliary index surfaces as corruption instead of a silent mis-serve.
    /// Returns `None` if the epoch jar or row is absent.
    ///
    /// # Errors
    ///
    /// Returns [`ColdError::Corruption`] if the stored digest at `loc` differs from `digest`.
    pub fn read_batch_checked(
        &self,
        digest: BlockHash,
        loc: ColdLocation,
    ) -> ColdResult<Option<Vec<u8>>> {
        // An unsealed epoch has no jar to open; treat it as absent rather than an open error.
        if !self.batches.is_epoch_sealed(loc.epoch) {
            return Ok(None);
        }
        let Some(mut columns) = self.batches.read_row(loc.epoch, loc.row as usize)? else {
            return Ok(None);
        };
        let stored = columns.first().map(|c| c.as_slice()).unwrap_or_default();
        if stored != digest.as_slice() {
            return Err(ColdError::Corruption(format!(
                "batches epoch {} row {} digest mismatch: stored {} != requested {digest}",
                loc.epoch,
                loc.row,
                BlockHash::try_from(stored)
                    .map(|h| h.to_string())
                    .unwrap_or_else(|_| format!("{stored:x?}")),
            )));
        }
        // Move the (large) payload column out of the row instead of cloning it; the digest column
        // is the only other column, so `swap_remove` does not shuffle real work.
        Ok((columns.len() > BATCH_PAYLOAD_COLUMN)
            .then(|| columns.swap_remove(BATCH_PAYLOAD_COLUMN)))
    }

    /// Visits a single epoch's archived consensus blocks in ascending order, reusing one cursor
    /// (see [`ColdSegment::for_each_row_in_epoch`]).
    pub(crate) fn for_each_consensus_block_in_epoch(
        &self,
        epoch: Epoch,
        visit: impl FnMut(u64, &[u8]) -> ColdResult<()>,
    ) -> ColdResult<()> {
        self.consensus_blocks.for_each_row_in_epoch(epoch, visit)
    }
}

#[cfg(test)]
mod tests;
