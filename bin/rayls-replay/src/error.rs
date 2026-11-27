//! Replay error surface.

use rayls_infrastructure_types::B256;
use thiserror::Error;

/// Errors emitted by the scripted-replay pipeline.
#[derive(Debug, Error)]
pub enum ReplayError {
    /// Snapshot's EVM chain is missing a header we expected to read.
    #[error("snapshot missing header at block {block_number}")]
    MissingHeader { block_number: u64 },

    /// Consensus DB is missing the `Batch` referenced by a header's mix_hash XOR.
    #[error("consensus DB missing Batch for digest {batch_digest:?} (block {block_number})")]
    MissingBatch { block_number: u64, batch_digest: B256 },

    /// Replay produced a state root that disagrees with the snapshot's.
    #[error("replay diverged at block {block_number}: archive {ours:?} != snapshot {expected:?}")]
    StateRootMismatch { block_number: u64, ours: B256, expected: B256 },

    /// Replay produced a block hash that disagrees with the snapshot's.
    #[error(
        "block hash diverged at block {block_number}: archive {ours:?} != snapshot {expected:?}"
    )]
    BlockHashMismatch { block_number: u64, ours: B256, expected: B256 },

    /// Snapshot and archive disagree on the genesis block hash, meaning their
    /// chainspecs differ (chain id, hardfork schedule, or genesis state).
    #[error("genesis hash mismatch: archive {ours:?} != snapshot {expected:?}")]
    GenesisHashMismatch { ours: B256, expected: B256 },

    /// A read from the snapshot failed.
    #[error("snapshot read error: {0}")]
    SnapshotEnv(String),

    /// Archive's `RethEnv` returned an error.
    #[error("archive env error: {0}")]
    ArchiveEnv(String),

    /// Consensus DB read failed.
    #[error("consensus DB error: {0}")]
    ConsensusDb(String),

    /// Reading the committee from the on-chain `ConsensusRegistry` failed.
    #[error("committee load error: {0}")]
    Committee(String),
}

/// Result alias for [`ReplayError`].
pub type ReplayResult<T> = Result<T, ReplayError>;
