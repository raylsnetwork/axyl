// SPDX-License-Identifier: BUSL-1.1
//! Read-only access to one node's consensus database.
//!
//! [`NodeDb`] opens the MDBX environment read-only (never creating tables), attaches the cold
//! tier when its directory exists, and answers point queries through short read transactions so
//! a running node's page reclamation is never pinned for long.

use eyre::{eyre, WrapErr};
use rayls_infrastructure_storage::{
    cold::{ColdLocation, ARCHIVE_HIGH_WATER_MARK_KEY},
    mdbx::MdbxDatabase,
    tables::{
        Batches, Certificates, ColdArchiveHighWaterMark, ColdBatchLocations,
        ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache, EpochCerts,
        EpochRecords, EpochRecordsIndex, EpochTransitionCheckpoints, NodeIdentity,
    },
    ColdConfig, ColdStore,
};
use rayls_infrastructure_types::{
    decode, decode_key, AuthorityIdentifier, BlockHash, CertificateDigest, ConsensusHeader,
    ConsensusHeaderMeta, Database, Epoch, EpochCertificate, EpochRecord, EpochTransitionCheckpoint,
    Table, B256,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Options shared by every database open.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    /// Open with `MDBX_EXCLUSIVE` (copies only).
    pub exclusive: bool,
    /// Refuse a database whose lock file names a live process.
    pub require_stopped: bool,
    /// Open read-write once first so MDBX can recover a copy taken from a running node.
    pub recover: bool,
}

/// Whether a process currently holds the consensus-db directory open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum LiveStatus {
    /// No process holds an OS lock on the directory's `mdbx.lck`.
    Stopped,
    /// A process holds `mdbx.lck`; `pid` when the lock table reveals it.
    Live { pid: Option<u32> },
    /// Cannot be determined on this platform.
    Unknown,
}

impl std::fmt::Display for LiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Stopped => "no",
            Self::Live { .. } => "yes",
            Self::Unknown => "?",
        })
    }
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
}

/// One node's consensus database, opened read-only.
#[derive(Debug)]
pub struct NodeDb {
    /// Display label (user-supplied or derived from the path).
    pub label: String,
    /// Resolved `consensus-db` directory.
    pub path: PathBuf,
    /// Whether a node process holds this directory.
    pub live: LiveStatus,
    db: MdbxDatabase,
    cold: Option<ColdStore>,
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

        let live = probe_live(&path);
        if opts.require_stopped {
            if let LiveStatus::Live { pid } = live {
                let holder = pid.map(|p| format!(" by pid {p}")).unwrap_or_default();
                return Err(eyre!(
                    "{label}: consensus-db at {} is in use{holder} (--require-stopped)",
                    path.display()
                ));
            }
        }

        if opts.recover {
            MdbxDatabase::recover(&path)
                .wrap_err_with(|| format!("{label}: recover {}", path.display()))?;
        }
        // MDBX registers every reader in mdbx.lck, so a read-only open still needs to write that
        // file. A lock file nobody can write cannot be held by a running node either, so open it
        // exclusively, which skips the reader table.
        let exclusive = opts.exclusive || !lock_file_writable(&path);
        let db = MdbxDatabase::open_read_only(&path, exclusive).map_err(|e| {
            let text = format!("{e:#}");
            if text.contains("should be recovered") {
                eyre!(
                    "{label}: {} needs recovery, it was copied from a running node or left by a \
                     killed one; rerun with --recover, or copy from a stopped node",
                    path.display()
                )
            } else if text.contains("opened in read-only") {
                eyre!(
                    "{label}: cannot register as a reader of {}: mdbx.lck is not writable; \
                     rerun with --exclusive",
                    path.display()
                )
            } else {
                eyre!("{label}: open {} read-only: {text}", path.display())
            }
        })?;

        // Opening the cold store creates its directories, so only attach one that already exists.
        let cold_dir = path.join("cold");
        let cold = if cold_dir.is_dir() {
            Some(
                ColdStore::open(&ColdConfig { dir: cold_dir.clone() })
                    .map_err(|e| eyre!("{label}: open cold tier {}: {e}", cold_dir.display()))?,
            )
        } else {
            None
        };

        Ok(Self { label, path, live, db, cold })
    }

    /// Whether the cold tier is attached.
    pub fn has_cold(&self) -> bool {
        self.cold.is_some()
    }

    /// Whether table `T` exists on disk.
    pub fn table_status<T: Table>(&self) -> eyre::Result<TableStatus> {
        Ok(if self.db.has_table::<T>()? { TableStatus::Present } else { TableStatus::Absent })
    }

    /// Point read that treats a never-created table as an empty one.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if !self.db.has_table::<T>()? {
            return Ok(None);
        }
        self.db.get::<T>(key).wrap_err_with(|| format!("{}: read {}", self.label, T::NAME))
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
        if !self.db.has_table::<EpochTransitionCheckpoints>()? {
            return Ok(Vec::new());
        }
        Ok(self.db.iter::<EpochTransitionCheckpoints>().map(|(_, cp)| cp).collect())
    }

    /// Every epoch number with a record, in ascending order. Keys only; values are not decoded.
    pub fn epoch_numbers(&self) -> eyre::Result<Vec<Epoch>> {
        if !self.db.has_table::<EpochRecords>()? {
            return Ok(Vec::new());
        }
        Ok(self.db.raw_iter::<EpochRecords>().map(|(k, _)| decode_key::<Epoch>(&k)).collect())
    }

    /// The consensus header at `number`, with the tier that held it: hot table, then the cache
    /// table, then the cold archive.
    pub fn header(&self, number: u64) -> eyre::Result<Option<(ConsensusHeader, Tier)>> {
        if let Some(h) = self.get::<ConsensusBlocks>(&number)? {
            return Ok(Some((h, Tier::Hot)));
        }
        if let Some(h) = self.get::<ConsensusBlocksCache>(&number)? {
            return Ok(Some((h, Tier::Cache)));
        }
        let Some(cold) = &self.cold else { return Ok(None) };
        let bytes = cold
            .read_consensus_block_checked(number)
            .map_err(|e| eyre!("{}: cold read of consensus block {number}: {e}", self.label))?;
        Ok(bytes.map(|b| (decode::<ConsensusHeader>(&b), Tier::Cold)))
    }

    /// The consensus number the digest index maps `digest` to, if any.
    pub fn header_number_by_digest(&self, digest: BlockHash) -> eyre::Result<Option<u64>> {
        self.get::<ConsensusBlockNumbersByDigest>(&digest)
    }

    /// Highest key in a `u64`-keyed table, without decoding values.
    fn last_u64_key<T: Table<Key = u64>>(&self) -> eyre::Result<Option<u64>> {
        if !self.db.has_table::<T>()? {
            return Ok(None);
        }
        Ok(self.db.reverse_raw_iter::<T>().next().map(|(k, _)| decode_key::<u64>(&k)))
    }

    /// Highest epoch with a record on disk. Keys only; values are not decoded.
    pub fn latest_epoch_record(&self) -> eyre::Result<Option<Epoch>> {
        if !self.db.has_table::<EpochRecords>()? {
            return Ok(None);
        }
        Ok(self.db.reverse_raw_iter::<EpochRecords>().next().map(|(k, _)| decode_key::<Epoch>(&k)))
    }

    /// Epoch of the latest canonical consensus header, read from a projection of its raw bytes.
    pub fn current_epoch(&self) -> eyre::Result<Option<Epoch>> {
        if !self.db.has_table::<ConsensusBlocks>()? {
            return Ok(None);
        }
        let Some((_, bytes)) = self.db.reverse_raw_iter::<ConsensusBlocks>().next() else {
            return Ok(None);
        };
        let meta = ConsensusHeaderMeta::from_bytes(&bytes)
            .map_err(|e| eyre!("{}: project latest consensus header: {e}", self.label))?;
        Ok(Some(meta.leader_epoch))
    }

    /// The node's [`Position`].
    pub fn position(&self) -> eyre::Result<Position> {
        Ok(Position {
            latest_epoch_record: self.latest_epoch_record()?,
            current_epoch: self.current_epoch()?,
            consensus_tip: self.latest_consensus_number()?,
        })
    }

    /// Highest canonical consensus header number on disk.
    pub fn latest_consensus_number(&self) -> eyre::Result<Option<u64>> {
        self.last_u64_key::<ConsensusBlocks>()
    }

    /// Highest consensus header number in the verified-but-unprocessed cache.
    pub fn latest_cached_consensus_number(&self) -> eyre::Result<Option<u64>> {
        self.last_u64_key::<ConsensusBlocksCache>()
    }

    /// Whether a DAG certificate is in the (current-epoch) certificate table.
    pub fn has_certificate(&self, digest: CertificateDigest) -> eyre::Result<bool> {
        Ok(self.get::<Certificates>(&digest)?.is_some())
    }

    /// Where a batch lives, if anywhere: hot table or the cold tier's location index.
    pub fn batch_tier(&self, digest: BlockHash) -> eyre::Result<Option<Tier>> {
        if self.get::<Batches>(&digest)?.is_some() {
            return Ok(Some(Tier::Hot));
        }
        Ok(self.get::<ColdBatchLocations>(&digest)?.map(|_: ColdLocation| Tier::Cold))
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
        MdbxDatabase::datafile_size(&self.path)
            .wrap_err_with(|| format!("{}: stat mdbx.dat", self.label))
    }
}

/// Whether `mdbx.lck` in `consensus_db` can be opened for writing. A missing file counts as
/// writable: MDBX creates it.
fn lock_file_writable(consensus_db: &Path) -> bool {
    let lck = consensus_db.join("mdbx.lck");
    !lck.exists() || std::fs::OpenOptions::new().write(true).open(&lck).is_ok()
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

/// Whether some process holds an OS lock on the directory's `mdbx.lck`, which every MDBX handle
/// (reader or writer) takes while the environment is open. Read from `/proc/locks` by inode, so a
/// datadir copied from elsewhere reads as stopped even if its files came from a running node.
/// MDBX places its writer lock at byte offset `pid`, which is how the PID is recovered when the
/// lock table itself does not name one (OFD locks report -1).
fn probe_live(consensus_db: &Path) -> LiveStatus {
    let Ok(meta) = std::fs::metadata(consensus_db.join("mdbx.lck")) else {
        return LiveStatus::Stopped;
    };
    probe_live_linux(&meta).unwrap_or(LiveStatus::Unknown)
}

#[cfg(target_os = "linux")]
fn probe_live_linux(meta: &std::fs::Metadata) -> Option<LiveStatus> {
    use std::os::unix::fs::MetadataExt as _;
    let (major, minor) = split_dev(meta.dev());
    let target = format!("{major:02x}:{minor:02x}:{}", meta.ino());
    let locks = std::fs::read_to_string("/proc/locks").ok()?;
    let mut held = false;
    let mut pid = None;
    for line in locks.lines() {
        // "<id>: <OFDLCK|POSIX|FLOCK> <ADVISORY|MANDATORY> <READ|WRITE> <pid> MAJ:MIN:INO <start>
        // <end>"
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 8 || f[5] != target {
            continue;
        }
        held = true;
        if f[3] == "WRITE" {
            pid = f[4].parse::<u32>().ok().filter(|p| *p > 0).or_else(|| f[6].parse().ok());
        }
    }
    Some(if held { LiveStatus::Live { pid } } else { LiveStatus::Stopped })
}

#[cfg(not(target_os = "linux"))]
fn probe_live_linux(_meta: &std::fs::Metadata) -> Option<LiveStatus> {
    None
}

/// Linux `dev_t` -> (major, minor), matching the `MAJ:MIN` columns of `/proc/locks`.
#[cfg(target_os = "linux")]
fn split_dev(dev: u64) -> (u64, u64) {
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    (major, minor)
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

    #[test]
    fn live_probe_needs_a_held_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(probe_live(dir.path()), LiveStatus::Stopped);
        // an mdbx.lck nobody holds (a copy) is not live
        std::fs::write(dir.path().join("mdbx.lck"), [0u8; 64]).unwrap();
        if cfg!(target_os = "linux") {
            assert_eq!(probe_live(dir.path()), LiveStatus::Stopped);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dev_t_splits_into_major_minor() {
        assert_eq!(split_dev(0xfc01), (0xfc, 0x01));
        assert_eq!(split_dev(0x0000_0103_0000_1201), (0x12, 0x1030_0001));
    }
}
