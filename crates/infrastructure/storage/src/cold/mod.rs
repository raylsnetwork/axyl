//! Append-only compressed cold tier for the consensus DB, backed by the nippy-jar file format.
//!
//! Whole epochs of `Batches` and `ConsensusBlocks` move into per-epoch jars, one jar per segment.
//! Nothing here drops data: all history stays queryable through the layered database's cold
//! fall-through, and the batches jar carries its digest column so the auxiliary index that
//! addresses it can be rebuilt from the jars alone.

mod archiver;
mod fallthrough;
mod jar;
mod producer;
mod reconcile;

#[cfg(test)]
mod tests;

pub use archiver::ColdArchiver;
pub(crate) use fallthrough::{cold_get, cold_has, cold_raw, cold_to_eyre};
pub use jar::{ColdSegment, ColdStore};
pub use producer::{archive_below_epoch, ArchiveStats, SealOutcome};
pub use reconcile::reconcile;

use std::{collections::BTreeMap, path::PathBuf};

use rayls_infrastructure_types::Epoch;
use reth_nippy_jar::NippyJarError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors raised by the cold tier.
///
/// Module-internal APIs return this enum; the `Database`/`DbTx` trait boundary converts it to
/// `eyre` since those traits are fixed.
#[derive(Debug, Error)]
pub enum ColdError {
    /// An I/O operation against a jar or its satellite files failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The underlying nippy-jar format reported an error.
    #[error(transparent)]
    Nippy(#[from] NippyJarError),

    /// A jar row could not be decoded as the expected typed value.
    #[error("failed to decode cold value: {0}")]
    Codec(String),

    /// A jar or index is internally inconsistent and cannot be reconciled.
    #[error("cold store corruption: {0}")]
    Corruption(String),
}

/// Location of an archived batch within the cold tier.
///
/// Stored as the value of the hot `ColdBatchLocations` auxiliary index, mapping a batch digest to
/// the epoch jar and row that hold it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdLocation {
    /// Epoch whose jar holds the row.
    pub epoch: Epoch,
    /// Zero-based row index within that jar.
    pub row: u64,
}

/// Which cold segment a jar belongs to.
///
/// Persisted inside [`ColdJarHeader`] so a jar is self-describing across the two segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColdSegmentKind {
    /// `ConsensusBlocks`: one column `[bcs(ConsensusHeader)]`, keyed by block number.
    ConsensusBlocks,
    /// `Batches`: two columns `[digest, bcs(Batch)]`, keyed by batch digest.
    Batches,
}

/// User header serialized into each jar's `.conf`: the jar's identity, fixed at creation.
///
/// Row count and covered range are deliberately not persisted: nippy's own row count in the same
/// `.conf` is their single source of truth (it survives the writer's consistency heal), so boot
/// derives them into a [`SealedJar`] instead of trusting a second copy that could disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdJarHeader {
    /// Epoch this jar archives.
    pub epoch: Epoch,
    /// First key in the jar (block number for consensus_blocks; unused sentinel for batches).
    pub start_key: u64,
    /// Segment the jar belongs to.
    pub kind: ColdSegmentKind,
}

/// Index entry for a sealed jar: its identity plus the row count read from the jar itself, never
/// deserialized, so the indexed count cannot disagree with the jar. Only `rows > 0` jars are
/// indexed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedJar {
    /// Epoch the jar archives.
    pub epoch: Epoch,
    /// First key in the jar (block number for consensus_blocks; unused sentinel for batches).
    pub start_key: u64,
    /// Number of rows the jar holds (nonzero for an indexed jar).
    pub rows: u64,
}

impl SealedJar {
    /// Returns the last key the jar covers, or `None` if `start_key` cannot address its rows.
    ///
    /// `start_key` comes from the jar's `.conf` unvalidated, so the addition is checked: an
    /// overflow would abort the process under `overflow-checks`.
    pub fn checked_end_key(&self) -> Option<u64> {
        self.start_key.checked_add(self.rows.saturating_sub(1))
    }

    /// Returns the last key the jar covers, saturating on a start key that cannot address its rows.
    ///
    /// Index admission refuses such a jar, so a saturated value never describes an indexed one.
    pub fn end_key(&self) -> u64 {
        self.checked_end_key().unwrap_or(u64::MAX)
    }
}

/// Configuration for opening the cold tier.
#[derive(Debug, Clone)]
pub struct ColdConfig {
    /// Directory holding the cold jars and their satellite files.
    pub dir: PathBuf,
}

/// In-memory index from a jar's end key to its [`SealedJar`] entry, rebuilt at boot from the jars.
///
/// A `BTreeMap` keyed by end key gives an O(log n) range lookup of "which jar holds key K",
/// matching reth's static-file segment index.
pub(crate) type JarIndex = BTreeMap<u64, SealedJar>;

/// Convenience result alias for cold-tier internal APIs.
pub type ColdResult<T> = Result<T, ColdError>;

/// Sentinel key for the single `ColdArchiveHighWaterMark` row (the table holds at most one entry).
pub const ARCHIVE_HIGH_WATER_MARK_KEY: u8 = 0;

#[cfg(test)]
pub(crate) mod probe;
