// SPDX-License-Identifier: BUSL-1.1
//! Read-only access to one node's consensus database.
//!
//! [`NodeDb`] opens the MDBX environment read-only and exclusively (never creating tables, and
//! refusing a database another process holds open), attaches the cold tier when its directory
//! exists, and answers point queries through short read transactions.

use eyre::{eyre, WrapErr};
use rayls_infrastructure_storage::{
    cold::{ColdLocation, ColdResult, ARCHIVE_HIGH_WATER_MARK_KEY},
    mdbx::MdbxDatabase,
    tables::{
        Batches, Certificates, ColdArchiveHighWaterMark, ColdBatchLocations,
        ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache, EpochCerts,
        EpochRecords, EpochRecordsIndex, EpochTransitionCheckpoints, NodeIdentity,
    },
    ColdConfig, ColdStore,
};
use rayls_infrastructure_types::{
    leader_epoch_and_batch_digests, try_decode, try_decode_key, AuthorityIdentifier, Batch,
    BlockHash, CertificateDigest, ConsensusHeader, ConsensusHeaderMeta, Database, DbTx as _, Epoch,
    EpochCertificate, EpochRecord, EpochTransitionCheckpoint, Table, B256,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

/// A raw key/value iterator over one table, as the storage layer hands it out.
type DBRawIterBox<'i> = rayls_infrastructure_types::DBRawIter<'i>;

/// Options shared by every database open. Every open is read-only and exclusive.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    /// Open read-write once first so MDBX can recover a copy whose last commit was never synced.
    pub recover: bool,
}

/// Which storage tier answered a consensus-header or batch lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tier {
    /// The canonical hot table.
    Hot,
    /// The verified-but-unprocessed cache table.
    Cache,
    /// The append-only cold archive under `cold/`.
    Cold,
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Hot => "hot",
            Self::Cache => "cache",
            Self::Cold => "cold",
        })
    }
}

/// Whether a table exists in this database at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TableStatus {
    Present,
    Absent,
}

/// Where a node stands: its latest epoch record, the epoch its consensus tip belongs to, and
/// the tip's number. Used to tell "not reached yet" apart from "should be there but is not".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Position {
    /// Highest epoch with a record on disk.
    pub latest_epoch_record: Option<Epoch>,
    /// Epoch of the latest canonical consensus header.
    pub current_epoch: Option<Epoch>,
    /// Number of the latest canonical consensus header.
    pub consensus_tip: Option<u64>,
}

impl Position {
    /// Whether the node has closed epoch `epoch`, so its record is expected on disk.
    ///
    /// An epoch's record is written when the epoch ends, so it is expected once the node either
    /// holds that record (or a later one) or its consensus tip is already in a later epoch.
    pub fn has_closed_epoch(&self, epoch: Epoch) -> bool {
        self.latest_epoch_record.is_some_and(|latest| latest >= epoch)
            || self.current_epoch.is_some_and(|current| current > epoch)
    }

    /// Whether the node's consensus tip is at or past `number`.
    pub fn has_reached_header(&self, number: u64) -> bool {
        self.consensus_tip.is_some_and(|tip| tip >= number)
    }

    /// Whether the node's latest consensus header is in `epoch` or a later one, so batches sealed
    /// in `epoch` can exist on it (workers reject batches from another epoch).
    pub fn has_reached_epoch(&self, epoch: Epoch) -> bool {
        self.current_epoch.is_some_and(|current| current >= epoch)
    }
}

/// How much a batch scan covered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ScanStats {
    /// Hot rows read (the whole table; an epoch filter is applied after reading).
    pub hot_batches: usize,
    /// Rows the hot table reports holding, so a scan cut short by a read error is visible.
    pub hot_table_rows: usize,
    /// Batches read from cold jars.
    pub cold_batches: usize,
    /// Cold epochs whose jar was read.
    pub cold_epochs: usize,
}

impl std::fmt::Display for ScanStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} hot, {} cold ({} {})",
            self.hot_batches,
            self.hot_table_rows,
            self.cold_batches,
            self.cold_epochs,
            if self.cold_epochs == 1 { "epoch" } else { "epochs" }
        )
    }
}

/// Outcome of looking a batch up by digest.
#[derive(Debug)]
pub enum BatchLookup {
    /// Stored, in this tier.
    Found(Batch, Tier),
    /// The cold location index names a jar row that does not exist: a corrupt index.
    Dangling(ColdLocation),
    /// Neither the hot table nor the cold index knows the digest.
    Absent,
}

/// One node's consensus database, opened read-only and exclusively.
#[derive(Debug)]
pub struct NodeDb {
    /// Display label (user-supplied or derived from the path).
    pub label: String,
    /// Resolved `consensus-db` directory.
    pub path: PathBuf,
    /// `--recover` opened this copy read-write once this run; its newest commit may have been
    /// rolled back (see the README).
    pub recovered: bool,
    db: MdbxDatabase,
    /// The cold archive under `cold/`, when the directory existed at the open.
    cold: Option<ColdStore>,
    /// Table presence, probed once per table: a read-only environment cannot gain tables.
    tables: std::sync::Mutex<HashMap<&'static str, bool>>,
}

impl NodeDb {
    /// Resolves `spec` (`[LABEL=]PATH`, PATH a node datadir or a `consensus-db` directory) to a
    /// label and the directory holding `mdbx.dat`, without opening anything.
    pub fn resolve(spec: &str) -> eyre::Result<(String, PathBuf)> {
        let (label, raw_path) = split_label(spec);
        let path = resolve_consensus_db(Path::new(raw_path))?;
        let label = label.map(str::to_owned).unwrap_or_else(|| default_label(&path));
        Ok((label, path))
    }

    /// Opens `spec`, which is `[LABEL=]PATH` where PATH is a node datadir or a `consensus-db`
    /// directory.
    pub fn open(spec: &str, opts: &OpenOptions) -> eyre::Result<Self> {
        let (label, path) = Self::resolve(spec)?;

        let dat_len = std::fs::metadata(path.join("mdbx.dat")).map(|m| m.len()).unwrap_or(0);
        if dat_len < 4096 {
            return Err(eyre!(
                "{label}: {} is {dat_len} bytes long: not even one page; the file is empty or \
                 truncated beyond what MDBX can read",
                path.join("mdbx.dat").display()
            ));
        }
        // Held by another process: MDBX_EXCLUSIVE fails, and the recovery open (read-write,
        // exclusive) fails the same way, so a running node is refused on every path.
        let in_use = |e: &eyre::Report| -> Option<eyre::Report> {
            // MDBX_BUSY, which reth-libmdbx renders as "another write transaction is running",
            // or EAGAIN from the file lock when the holder is in this process
            let text = format!("{e:#}");
            (text.contains("another write transaction is running")
                || text.contains("Resource temporarily unavailable")
                || text.contains("Busy")
                || text.contains("MDBX_BUSY"))
            .then(|| {
                eyre!(
                    "{label}: {} is open in another process (a running node?); this tool never \
                     reads a running node: stop it first, or copy its files and inspect the copy",
                    path.display()
                )
            })
        };
        if opts.recover {
            recover(&path).map_err(|e| {
                in_use(&e)
                    .unwrap_or_else(|| e.wrap_err(format!("{label}: recover {}", path.display())))
            })?;
            eprintln!(
                "{label}: recovered {}: its meta pages were rewritten, and if the copy was taken \
                 on another host or before a reboot MDBX rolled it back to the last steady \
                 commit, losing up to a few seconds of writes",
                path.display()
            );
        }
        // Exclusive: refuses a database another process holds and skips MDBX's reader table, so
        // a copy whose mdbx.lck is not writable opens too.
        let db = MdbxDatabase::open_read_only(&path, true).map_err(|e| {
            if let Some(in_use) = in_use(&e) {
                return in_use;
            }
            let text = format!("{e:#}");
            if text.contains("should be recovered") {
                eyre::Report::new(NeedsRecovery(format!(
                    "{label}: {}: MDBX refuses to read it until it is recovered: its last commit \
                     was never synced (the node was killed or crashed, or the files were copied \
                     from a running node), or the file is damaged (for example truncated). \
                     Copy the directory (`cp --sparse=always`) and run `--recover` on the copy, \
                     which is the recovery the node itself performs when it next starts (on this \
                     boot it keeps the last commit; after a reboot or on another host it drops up \
                     to a few seconds of unsynced writes). Recovery never repairs damage",
                    path.display()
                )))
            } else {
                eyre!("{label}: open {} read-only: {text}", path.display())
            }
        })?;

        let cold = Self::open_cold(&label, &path)?;
        Ok(Self {
            label,
            path,
            recovered: opts.recover,
            db,
            cold,
            tables: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Opens the cold tier under `path` if it exists. Opening a cold store creates its
    /// directories, so a missing one is never opened.
    fn open_cold(label: &str, path: &Path) -> eyre::Result<Option<ColdStore>> {
        let cold_dir = path.join("cold");
        // opening a store creates any missing segment directory, so attach only a complete one:
        // the tool must not create directories inside a node's datadir
        if !(cold_dir.join("consensus_blocks").is_dir() && cold_dir.join("batches").is_dir()) {
            return Ok(None);
        }
        match ColdStore::open(&ColdConfig { dir: cold_dir.clone() }) {
            Ok(store) => Ok(Some(store)),
            // a damaged jar index must not hide the hot tables: go on without the cold tier
            Err(e) => {
                eprintln!(
                    "{label}: cold tier at {} cannot be opened ({e}); reading the hot tables \
                     only, so archived headers and batches will read as missing",
                    cold_dir.display()
                );
                Ok(None)
            }
        }
    }

    /// The cold tier, when one was attached at the open.
    fn cold(&self) -> Option<&ColdStore> {
        self.cold.as_ref()
    }

    /// Whether the cold tier exists.
    pub fn has_cold(&self) -> bool {
        self.cold().is_some()
    }

    /// Reads from the cold tier; without one, `None`.
    fn cold_read<T>(
        &self,
        what: &str,
        read: impl Fn(&ColdStore) -> ColdResult<Option<T>>,
    ) -> eyre::Result<Option<T>> {
        let Some(cold) = self.cold() else { return Ok(None) };
        read(cold).map_err(|e| eyre!("{}: cold {what}: {e}", self.label))
    }

    /// Whether the epoch-record table was never created.
    pub fn epoch_table_absent(&self) -> eyre::Result<bool> {
        Ok(self.table_status::<EpochRecords>()? == TableStatus::Absent)
    }

    pub fn table_status<T: Table>(&self) -> eyre::Result<TableStatus> {
        Ok(if self.table_present::<T>()? { TableStatus::Present } else { TableStatus::Absent })
    }

    /// Whether table `T` exists, probing MDBX once per table and caching the answer.
    fn table_present<T: Table>(&self) -> eyre::Result<bool> {
        if let Some(present) = self.tables.lock().unwrap_or_else(|e| e.into_inner()).get(T::NAME) {
            return Ok(*present);
        }
        let present = self.db.has_table::<T>()?;
        self.tables.lock().unwrap_or_else(|e| e.into_inner()).insert(T::NAME, present);
        Ok(present)
    }

    /// Point read that treats a never-created table as an empty one. Decodes fallibly: a corrupt
    /// row is reported, not a panic, on the databases this tool exists to inspect.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if !self.table_present::<T>()? {
            return Ok(None);
        }
        let bytes = self
            .db
            .with_read_txn(|tx| tx.raw_get::<T>(key).map(|b| b.map(|b| b.into_owned())))
            .wrap_err_with(|| format!("{}: read {}", self.label, T::NAME))?;
        match bytes {
            Some(bytes) => Ok(Some(
                try_decode::<T::Value>(&bytes)
                    .map_err(|e| eyre!("{}: decode a {} row: {e}", self.label, T::NAME))?,
            )),
            None => Ok(None),
        }
    }

    /// The epoch record for `epoch` and, if present, its certificate (keyed by record digest).
    pub fn epoch(
        &self,
        epoch: Epoch,
    ) -> eyre::Result<Option<(EpochRecord, Option<EpochCertificate>)>> {
        let Some(record) = self.get::<EpochRecords>(&epoch)? else { return Ok(None) };
        let cert = self.get::<EpochCerts>(&record.digest())?;
        Ok(Some((record, cert)))
    }

    /// The epoch number the digest index maps `digest` to, if any.
    pub fn epoch_by_digest(&self, digest: B256) -> eyre::Result<Option<Epoch>> {
        self.get::<EpochRecordsIndex>(&digest)
    }

    /// Leftover transition checkpoint for `epoch` (present only after an interrupted transition).
    pub fn checkpoint(&self, epoch: Epoch) -> eyre::Result<Option<EpochTransitionCheckpoint>> {
        self.get::<EpochTransitionCheckpoints>(&epoch)
    }

    /// Every leftover transition checkpoint (there is at most one per interrupted transition).
    pub fn checkpoints(&self) -> eyre::Result<Vec<EpochTransitionCheckpoint>> {
        if !self.table_present::<EpochTransitionCheckpoints>()? {
            return Ok(Vec::new());
        }
        Ok(self.db.iter::<EpochTransitionCheckpoints>().map(|(_, cp)| cp).collect())
    }

    /// Every epoch number with a record, in ascending order. Keys only; values are not decoded.
    pub fn epoch_numbers(&self) -> eyre::Result<Vec<Epoch>> {
        if !self.table_present::<EpochRecords>()? {
            return Ok(Vec::new());
        }
        self.db
            .raw_iter::<EpochRecords>()
            .map(|(k, _)| {
                try_decode_key::<Epoch>(&k)
                    .map_err(|e| eyre!("{}: decode an epoch record key: {e}", self.label))
            })
            .collect()
    }

    /// The consensus header at `number`, with the tier that held it: the hot table, then the
    /// cold archive, then the verified-but-unprocessed cache. The canonical tiers come first
    /// because a cache row can outlive its header's promotion (a late gossip copy); such a row
    /// must not make an archived header look unprocessed.
    pub fn header(&self, number: u64) -> eyre::Result<Option<(ConsensusHeader, Tier)>> {
        if let Some(h) = self.get::<ConsensusBlocks>(&number)? {
            return Ok(Some((h, Tier::Hot)));
        }
        let bytes = self.cold_read(&format!("read of consensus block {number}"), |cold| {
            cold.read_consensus_block_checked(number)
        })?;
        if let Some(b) = bytes {
            let header = try_decode::<ConsensusHeader>(&b)
                .map_err(|e| eyre!("{}: decode cold consensus block {number}: {e}", self.label))?;
            return Ok(Some((header, Tier::Cold)));
        }
        Ok(self.get::<ConsensusBlocksCache>(&number)?.map(|h| (h, Tier::Cache)))
    }

    /// The consensus number the digest index maps `digest` to, if any.
    pub fn header_number_by_digest(&self, digest: BlockHash) -> eyre::Result<Option<u64>> {
        self.get::<ConsensusBlockNumbersByDigest>(&digest)
    }

    /// Highest key in a `u64`-keyed table, without decoding values.
    fn last_u64_key<T: Table<Key = u64>>(&self) -> eyre::Result<Option<u64>> {
        if !self.table_present::<T>()? {
            return Ok(None);
        }
        self.db
            .reverse_raw_iter::<T>()
            .next()
            .map(|(k, _)| {
                try_decode_key::<u64>(&k)
                    .map_err(|e| eyre!("{}: decode a {} key: {e}", self.label, T::NAME))
            })
            .transpose()
    }

    /// Highest epoch with a record on disk. Keys only; values are not decoded.
    pub fn latest_epoch_record(&self) -> eyre::Result<Option<Epoch>> {
        if !self.table_present::<EpochRecords>()? {
            return Ok(None);
        }
        self.db
            .reverse_raw_iter::<EpochRecords>()
            .next()
            .map(|(k, _)| {
                try_decode_key::<Epoch>(&k)
                    .map_err(|e| eyre!("{}: decode an epoch record key: {e}", self.label))
            })
            .transpose()
    }

    /// Epoch of the latest canonical consensus header, read from a projection of its raw bytes.
    /// Falls back to the newest archived header when the hot table is empty.
    pub fn current_epoch(&self) -> eyre::Result<Option<Epoch>> {
        let bytes = if self.table_present::<ConsensusBlocks>()? {
            self.db.reverse_raw_iter::<ConsensusBlocks>().next().map(|(_, v)| v.into_owned())
        } else {
            None
        };
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => {
                let Some((cold, number)) = self.cold_tip()? else { return Ok(None) };
                let Some(bytes) = cold.read_consensus_block_checked(number).map_err(|e| {
                    eyre!("{}: cold read of consensus block {number}: {e}", self.label)
                })?
                else {
                    return Ok(None);
                };
                bytes
            }
        };
        let meta = ConsensusHeaderMeta::from_bytes(&bytes)
            .map_err(|e| eyre!("{}: project latest consensus header: {e}", self.label))?;
        Ok(Some(meta.leader_epoch))
    }

    /// The cold tier and the number of its newest archived consensus header, if any.
    fn cold_tip(&self) -> eyre::Result<Option<(&ColdStore, u64)>> {
        let Some(cold) = self.cold() else { return Ok(None) };
        Ok(cold.consensus_blocks().key_span().map(|span| (cold, *span.end())))
    }

    /// The node's [`Position`].
    pub fn position(&self) -> eyre::Result<Position> {
        Ok(Position {
            latest_epoch_record: self.latest_epoch_record()?,
            current_epoch: self.current_epoch()?,
            consensus_tip: self.latest_consensus_number()?,
        })
    }

    /// The canonical tip header itself, from whichever tier holds it.
    pub fn latest_consensus_header(&self) -> eyre::Result<Option<ConsensusHeader>> {
        let Some(number) = self.latest_consensus_number()? else { return Ok(None) };
        Ok(self.header(number)?.map(|(h, _)| h))
    }

    /// Highest canonical consensus header number on disk: the hot table's, or the cold tier's
    /// when nothing is hot.
    pub fn latest_consensus_number(&self) -> eyre::Result<Option<u64>> {
        if let Some(number) = self.last_u64_key::<ConsensusBlocks>()? {
            return Ok(Some(number));
        }
        Ok(self.cold_tip()?.map(|(_, number)| number))
    }

    /// Highest consensus header number in the verified-but-unprocessed cache.
    pub fn latest_cached_consensus_number(&self) -> eyre::Result<Option<u64>> {
        self.last_u64_key::<ConsensusBlocksCache>()
    }

    /// Whether a DAG certificate is in the (current-epoch) certificate table.
    pub fn has_certificate(&self, digest: CertificateDigest) -> eyre::Result<bool> {
        Ok(self.get::<Certificates>(&digest)?.is_some())
    }

    /// The batch with `digest`: the hot table, then the cold archive through its location index.
    /// An index entry whose jar row cannot be read (or with no cold tier attached) is
    /// [`BatchLookup::Dangling`].
    pub fn batch(&self, digest: BlockHash) -> eyre::Result<BatchLookup> {
        if let Some(batch) = self.get::<Batches>(&digest)? {
            return Ok(BatchLookup::Found(batch, Tier::Hot));
        }
        let Some(location) = self.get::<ColdBatchLocations>(&digest)? else {
            return Ok(BatchLookup::Absent);
        };
        let bytes = self.cold_read(&format!("read of batch {digest}"), |cold| {
            cold.read_batch_checked(digest, location)
        })?;
        Ok(match bytes {
            Some(bytes) => BatchLookup::Found(
                try_decode(&bytes)
                    .map_err(|e| eyre!("{}: decode cold batch {digest}: {e}", self.label))?,
                Tier::Cold,
            ),
            None => BatchLookup::Dangling(location),
        })
    }

    /// Visits every stored batch: the hot table first, then each sealed cold epoch in ascending
    /// order. With `epoch`, hot batches sealed in other epochs are skipped after being read (the
    /// table is keyed by digest, so it is always read end to end) and only that epoch's cold jar
    /// is opened. `visit` returns `Ok(false)` to stop. The hot pass holds one read transaction
    /// for the whole table. Fails when the hot pass ends short of the rows the table reports:
    /// the cursor swallows read errors, and a short scan must not read as absence.
    pub fn scan_batches(
        &self,
        epoch: Option<Epoch>,
        mut visit: impl FnMut(BlockHash, &Batch, Tier) -> eyre::Result<bool>,
    ) -> eyre::Result<ScanStats> {
        let mut stats = ScanStats::default();
        if self.table_present::<Batches>()? {
            stats.hot_table_rows =
                self.table_counts()?.get(<Batches as Table>::NAME).copied().unwrap_or(0);
            for (key, value) in self.db.raw_iter::<Batches>() {
                stats.hot_batches += 1;
                let digest = try_decode_key::<BlockHash>(&key)
                    .map_err(|e| eyre!("{}: decode a batch key: {e}", self.label))?;
                let batch: Batch = try_decode(&value)
                    .map_err(|e| eyre!("{}: decode hot batch {digest}: {e}", self.label))?;
                if epoch.is_some_and(|e| batch.epoch != e) {
                    continue;
                }
                if !visit(digest, &batch, Tier::Hot)? {
                    return Ok(stats);
                }
            }
            // nothing else holds the database, so it cannot change under the scan: a shortfall
            // is a read error
            if stats.hot_batches < stats.hot_table_rows {
                return Err(eyre!(
                    "{}: hot batch scan ended after {} of {} rows; the table could not be read \
                     to the end",
                    self.label,
                    stats.hot_batches,
                    stats.hot_table_rows
                ));
            }
        }
        let Some(cold) = self.cold() else { return Ok(stats) };
        let epochs: Vec<Epoch> = match epoch {
            Some(e) => vec![e],
            None => cold.batches().sealed_epochs().into_iter().collect(),
        };
        for e in epochs {
            // the jar's (row, digest) pairs first, then each payload through the checked read
            // that serves the node's own lookups (one cursor per batch: fine for an offline scan)
            let mut rows: Vec<(u64, BlockHash)> = Vec::new();
            cold.for_each_batch_digest_in_epoch(e, |row, digest| {
                rows.push((row, digest));
                Ok(())
            })
            .map_err(|err| eyre!("{}: scan cold batches of epoch {e}: {err}", self.label))?;
            if !rows.is_empty() {
                stats.cold_epochs += 1;
            }
            for (row, digest) in rows {
                let location = ColdLocation { epoch: e, row };
                let bytes = cold.read_batch_checked(digest, location).map_err(|err| {
                    eyre!(
                        "{}: cold read of batch {digest} (epoch {e} row {row}): {err}",
                        self.label
                    )
                })?;
                let Some(bytes) = bytes else {
                    return Err(eyre!(
                        "{}: cold batch {digest} listed at epoch {e} row {row} has no payload",
                        self.label
                    ));
                };
                let batch: Batch = try_decode(&bytes).map_err(|err| {
                    eyre!("{}: decode cold batch {digest} (epoch {e} row {row}): {err}", self.label)
                })?;
                stats.cold_batches += 1;
                if !visit(digest, &batch, Tier::Cold)? {
                    return Ok(stats);
                }
            }
        }
        Ok(stats)
    }

    /// The consensus header of `epoch` whose sub-dag commits `batch`, as (number, tier), if one
    /// is stored. Hot and cache rows are projected (leader epoch and payload digests) without
    /// decoding the rest of the header; the cold tier is read for that epoch's jar only.
    pub fn header_committing_batch(
        &self,
        batch: BlockHash,
        epoch: Epoch,
    ) -> eyre::Result<Option<(u64, Tier)>> {
        let commits = |number: u64, bytes: &[u8]| -> eyre::Result<bool> {
            let (leader_epoch, digests) = leader_epoch_and_batch_digests(bytes)
                .map_err(|e| eyre!("{}: project consensus header {number}: {e}", self.label))?;
            Ok(leader_epoch == epoch && digests.contains(&batch))
        };
        let scan = |iter: DBRawIterBox<'_>, stop_below_epoch: bool| -> eyre::Result<Option<u64>> {
            for (key, value) in iter {
                let number = try_decode_key::<u64>(&key)
                    .map_err(|e| eyre!("{}: decode a consensus header key: {e}", self.label))?;
                if stop_below_epoch {
                    // newest first: once the leaders are from an earlier epoch, nothing older can
                    // commit a batch of this one
                    let meta = ConsensusHeaderMeta::from_bytes(&value).map_err(|e| {
                        eyre!("{}: project consensus header {number}: {e}", self.label)
                    })?;
                    if meta.leader_epoch < epoch {
                        return Ok(None);
                    }
                }
                if commits(number, &value)? {
                    return Ok(Some(number));
                }
            }
            Ok(None)
        };
        if self.table_present::<ConsensusBlocks>()? {
            if let Some(n) = scan(self.db.reverse_raw_iter::<ConsensusBlocks>(), true)? {
                return Ok(Some((n, Tier::Hot)));
            }
        }
        // the cold archive before the cache, for the same reason as in `header`
        if let Some(cold) = self.cold() {
            if let Some(range) = cold.consensus_blocks().key_range_for_epoch(epoch) {
                for number in range {
                    let bytes = cold.read_consensus_block_checked(number).map_err(|e| {
                        eyre!("{}: cold read of consensus block {number}: {e}", self.label)
                    })?;
                    if let Some(bytes) = bytes {
                        if commits(number, &bytes)? {
                            return Ok(Some((number, Tier::Cold)));
                        }
                    }
                }
            }
        }
        if self.table_present::<ConsensusBlocksCache>()? {
            if let Some(n) = scan(self.db.raw_iter::<ConsensusBlocksCache>(), false)? {
                return Ok(Some((n, Tier::Cache)));
            }
        }
        Ok(None)
    }

    /// Last fully archived epoch, if the cold tier has committed one.
    pub fn cold_high_water_mark(&self) -> eyre::Result<Option<Epoch>> {
        self.get::<ColdArchiveHighWaterMark>(&ARCHIVE_HIGH_WATER_MARK_KEY)
    }

    /// The authority identifier this database belongs to, if recorded.
    pub fn node_identity(&self) -> eyre::Result<Option<AuthorityIdentifier>> {
        self.get::<NodeIdentity>(&0)
    }

    /// Entry counts of every table on disk, including ones this build does not know.
    pub fn table_counts(&self) -> eyre::Result<BTreeMap<String, usize>> {
        self.db.table_entry_counts().wrap_err_with(|| format!("{}: table stats", self.label))
    }

    /// Size of `mdbx.dat` in bytes.
    pub fn datafile_size(&self) -> eyre::Result<u64> {
        std::fs::metadata(self.path.join("mdbx.dat"))
            .map(|m| m.len())
            .wrap_err_with(|| format!("{}: stat mdbx.dat", self.label))
    }
}

/// Makes a copy whose last commit was never synced openable: one read-write, exclusive open,
/// during which MDBX settles the head meta page, then closed without touching any table. Fails
/// if any other process has the database open. The same recovery the node performs on start.
pub fn recover(consensus_db: &Path) -> eyre::Result<()> {
    use reth_libmdbx::{Environment, EnvironmentFlags, Mode, SyncMode};
    if !consensus_db.join("mdbx.dat").is_file() {
        return Err(eyre!("no MDBX database at {} (expected mdbx.dat)", consensus_db.display()));
    }
    let flags = EnvironmentFlags {
        mode: Mode::ReadWrite { sync_mode: SyncMode::Durable },
        exclusive: true,
        ..Default::default()
    };
    let env = Environment::builder().set_max_dbs(32).set_flags(flags).open(consensus_db)?;
    env.stat().map_err(|e| {
        eyre!("MDBX database at {} failed its integrity check: {e}", consensus_db.display())
    })?;
    drop(env);
    Ok(())
}

/// Splits `LABEL=PATH` into its parts; a bare path has no label.
///
/// Only splits on a `=` that comes before any path separator, so paths containing `=` still work.
fn split_label(spec: &str) -> (Option<&str>, &str) {
    match spec.split_once('=') {
        Some((label, path)) if !label.is_empty() && !label.contains(['/', '\\']) => {
            (Some(label), path)
        }
        _ => (None, spec),
    }
}

/// Resolves a datadir or a consensus-db directory to the directory holding `mdbx.dat`.
fn resolve_consensus_db(path: &Path) -> eyre::Result<PathBuf> {
    let nested = path.join("consensus-db");
    if nested.join("mdbx.dat").is_file() {
        return Ok(nested);
    }
    if path.join("mdbx.dat").is_file() {
        return Ok(path.to_path_buf());
    }
    Err(eyre!("no mdbx.dat at {0} or {0}/consensus-db", path.display()))
}

/// `<parent>/<dir>` of the resolved path, enough to tell nodes apart in a table.
fn default_label(path: &Path) -> String {
    let mut parts = path.components().rev().filter_map(|c| match c {
        std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
        _ => None,
    });
    let last = parts.next().unwrap_or_default();
    match parts.next() {
        Some(parent) if last == "consensus-db" => parent,
        Some(parent) => format!("{parent}/{last}"),
        None => last,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_splitting() {
        assert_eq!(split_label("v1=/data/n1"), (Some("v1"), "/data/n1"));
        assert_eq!(split_label("/data/n1"), (None, "/data/n1"));
        assert_eq!(split_label("/data/a=b/n1"), (None, "/data/a=b/n1"));
        assert_eq!(split_label("=/data/n1"), (None, "=/data/n1"));
    }

    #[test]
    fn default_labels() {
        assert_eq!(default_label(Path::new("/data/node1/consensus-db")), "node1");
        assert_eq!(default_label(Path::new("/data/node1/copy")), "node1/copy");
        assert_eq!(default_label(Path::new("consensus-db")), "consensus-db");
    }

    #[test]
    fn resolve_requires_mdbx_dat() {
        let dir = tempfile::tempdir().unwrap();
        assert!(resolve_consensus_db(dir.path()).is_err());
        std::fs::create_dir(dir.path().join("consensus-db")).unwrap();
        std::fs::write(dir.path().join("consensus-db/mdbx.dat"), b"").unwrap();
        assert_eq!(resolve_consensus_db(dir.path()).unwrap(), dir.path().join("consensus-db"));
        assert_eq!(
            resolve_consensus_db(&dir.path().join("consensus-db")).unwrap(),
            dir.path().join("consensus-db")
        );
    }
}

/// Typed marker on the open error when the datafile needs recovery; detected by type, not message.
#[derive(Debug)]
pub struct NeedsRecovery(String);

impl std::fmt::Display for NeedsRecovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NeedsRecovery {}
