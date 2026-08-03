//! Per-segment nippy-jar handles and the two-segment cold store.
//!
//! A [`ColdSegment`] owns one jar per archived epoch plus an index rebuilt at boot from the jars'
//! `.conf` headers. consensus_blocks is addressed arithmetically by block number minus the jar
//! start key; batches is addressed by a [`ColdLocation`] resolved from the hot auxiliary index.
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

/// Zstd level for cold jars: the measured knee, past which CPU climbs and the ratio does not.
const COLD_ZSTD_LEVEL: i32 = 3;

/// Number of columns in the consensus_blocks segment: `[bcs(ConsensusHeader)]`.
pub(crate) const CONSENSUS_BLOCKS_COLUMNS: usize = 1;

/// Number of columns in the batches segment: `[digest(32B), bcs(Batch)]`.
pub(crate) const BATCHES_COLUMNS: usize = 2;

/// Column index of the batch payload within a batches jar row.
const BATCH_PAYLOAD_COLUMN: usize = 1;

/// Number of open jars a segment keeps mmapped.
///
/// Each entry pins two file descriptors and two mmaps, and cold epochs grow without limit, so the
/// cache is bounded rather than unbounded like reth's.
const JAR_CACHE_CAPACITY: usize = 16;

/// An open writer plus the header it is accumulating for the in-progress epoch jar.
///
/// Behind a [`Mutex`] so appends drive through `&self`, keeping the store shareable via `Arc`.
#[derive(Debug)]
struct OpenJar {
    /// Active nippy-jar writer for the epoch being sealed.
    writer: NippyJarWriter<ColdJarHeader>,
    /// Identity header frozen into the jar's `.conf` at [`ColdSegment::begin_epoch`].
    header: ColdJarHeader,
}

/// A jar opened for reading, whose cursors share one open mmap rather than reopening per row.
struct LoadedJar {
    /// Jar config and header, the borrow target for a cursor.
    jar: NippyJar<ColdJarHeader>,
    /// Shared handle to the jar's data and offset mmaps, cloned into each cursor.
    reader: Arc<DataReader>,
}

impl LoadedJar {
    /// Opens the jar at `path`, mmapping its data and offset files once.
    ///
    /// Checks the offsets' claimed data end against the file: a truncated jar would otherwise
    /// panic an unchecked slice, which `panic=abort` turns into a node abort. The `.conf`'s
    /// declared max row size stays unchecked because only `NippyJarWriter` exposes it, and
    /// opening a writer here would run nippy's consistency heal.
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

/// One nippy-jar-backed cold segment: a kind, its directory, and an in-memory jar index.
///
/// Owns the single writer for its kind. The index is rebuilt from the on-disk jars at
/// [`ColdSegment::open`].
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
    /// Opens the segment at `dir`, rebuilding the index from each jar's `.conf` header.
    ///
    /// A missing directory is created and treated as an empty segment.
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
            // A jar records its own kind so that a restored, renamed, or mis-filed file cannot be
            // served under the other segment's column count and addressing scheme.
            let header = jar.user_header();
            if header.kind != kind {
                return Err(ColdError::Corruption(format!(
                    "jar {data_path:?} is a {:?} jar in a {kind:?} segment",
                    header.kind
                )));
            }
            // The row count comes from the jar itself, never a persisted copy (see
            // [`SealedJar`]). The writer freezes a zero-row `.conf` at jar creation, before any
            // row is committed; such a jar marks an aborted or in-progress seal, not a durable
            // one, so it must not enter the read index; the writer's consistency check heals it
            // on the next reopen.
            let rows = jar.rows() as u64;
            if rows > 0 {
                let sealed = SealedJar { epoch: header.epoch, start_key: header.start_key, rows };
                index.insert(Self::index_key(kind, &sealed)?, sealed);
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
    /// Truncates any existing jar for `epoch` back to empty rather than appending, which is how a
    /// half-written or orphaned jar is re-archived whole. Callers must not begin an epoch whose
    /// jar is the only remaining copy of its rows. `start_key` is a sentinel for batches.
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
    /// nippy leaves the level at zstd's default, which equals [`COLD_ZSTD_LEVEL`] today, so the
    /// swap pins it: a zstd default change would otherwise re-tune every cold jar silently.
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
    /// The `.conf` write is the durability boundary, so only after it returns is the epoch safe to
    /// remove from the hot tier. A second commit without an intervening `begin_epoch` is a no-op.
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
            self.index.write().insert(Self::index_key(self.kind, &sealed)?, sealed);
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

    /// Visits every row of `epoch`'s jar in ascending row order as `(row index, first column)`,
    /// reusing one cursor so peak memory stays one borrowed row.
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
            let columns = cursor.row_by_number(row as usize)?.ok_or_else(|| {
                ColdError::Corruption(format!("cold row {row} missing from epoch {epoch} jar"))
            })?;
            let value = columns.first().copied().ok_or_else(|| {
                ColdError::Corruption(format!("epoch {epoch} jar row {row} has no columns"))
            })?;
            visit(row, value)?;
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

    /// Returns the dense `[first, last]` key range the sealed jars cover as one span, or `None`
    /// for an empty segment; reads the in-memory index only (epochs seal contiguously, so the
    /// span has no interior gaps).
    pub fn key_span(&self) -> Option<RangeInclusive<u64>> {
        let index = self.index.read();
        let first = index.values().next()?.start_key;
        let last = index.values().next_back()?.end_key();
        Some(first..=last)
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

    /// Returns the [`JarIndex`] key for a sealed jar: end key for consensus_blocks, epoch for
    /// batches, whose sentinel start key would otherwise collide every epoch onto one key.
    ///
    /// # Errors
    ///
    /// [`ColdError::Corruption`] if a start key cannot address its own rows. This is the only
    /// place that bound is applied, so it keeps every later `start_key + row` in range.
    fn index_key(kind: ColdSegmentKind, jar: &SealedJar) -> ColdResult<u64> {
        match kind {
            ColdSegmentKind::ConsensusBlocks => jar.checked_end_key().ok_or_else(|| {
                ColdError::Corruption(format!(
                    "jar epoch-{:010} start key {} cannot address {} rows",
                    jar.epoch, jar.start_key, jar.rows
                ))
            }),
            ColdSegmentKind::Batches => Ok(Self::index_key_for_epoch(jar.epoch)),
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
/// Wraps both [`ColdSegment`]s behind the read APIs the layered database's cold fall-through
/// routes to on a hot miss.
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

    /// Reads the raw `ConsensusHeader` bytes for `number`.
    ///
    /// # Errors
    ///
    /// [`ColdError::Corruption`] if the stored header's own number differs, since a misaligned jar
    /// would otherwise serve a wrong header into `mix_hash` / `parent_beacon_block_root`.
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

    /// Reads the raw `Batch` bytes at `loc`, or `None` if the epoch jar or row is absent.
    ///
    /// # Errors
    ///
    /// [`ColdError::Corruption`] if the stored digest column differs, since a mis-pointing
    /// auxiliary index would otherwise serve the wrong batch.
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

    /// Visits a single epoch's archived batches as `(row, digest)`, `row` being exactly the
    /// [`ColdLocation::row`] the batch is served from, so the auxiliary index can be rebuilt from
    /// the jar itself rather than re-derived from another segment.
    ///
    /// # Errors
    ///
    /// [`ColdError::Corruption`] if a row's digest column is not a 32-byte hash.
    pub(crate) fn for_each_batch_digest_in_epoch(
        &self,
        epoch: Epoch,
        mut visit: impl FnMut(u64, BlockHash) -> ColdResult<()>,
    ) -> ColdResult<()> {
        self.batches.for_each_row_in_epoch(epoch, |row, column| {
            let digest = BlockHash::try_from(column).map_err(|_| {
                ColdError::Corruption(format!(
                    "batches epoch {epoch} row {row} digest column is {} bytes",
                    column.len()
                ))
            })?;
            visit(row, digest)
        })
    }
}

#[cfg(test)]
mod tests;
