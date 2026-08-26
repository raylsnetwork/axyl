//! Types for the phase-based epoch transition state machine.

use crate::{engine::ExecutionNode, primary::PrimaryNode};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, CameFrom, ConsensusOutput, Database, NodeMode, Notifier,
    TaskManager, B256,
};
use tokio::sync::mpsc;

// Re-export the checkpoint types from the types crate for use by other
// orchestrator modules and consumers.
pub use rayls_infrastructure_types::{EpochTransitionCheckpoint, EpochTransitionPhase};

/// Outcome of the `run_epoch()` select loop.
///
/// Classifies how the running phase ended so the sequential
/// handler can take the appropriate action without races.
pub(crate) enum RunningOutcome {
    /// The node-level shutdown signal was received.
    NodeShutdown,
    /// The epoch boundary was detected. Contains the target hash and the
    /// boundary output that must be sent to the engine during the
    /// EXECUTION_COMPLETE phase.
    EpochBoundary(B256, Box<ConsensusOutput>),
    /// A mode transition was requested (e.g., behind_consensus → CvvInactive,
    /// or catch-up complete → CvvActive).
    ModeTransition(NodeMode),
    /// A consensus task crashed.
    TaskCrash(eyre::Error),
}

/// Outcome of the shared shutdown helper (`controlled_shutdown`).
pub(crate) struct ShutdownOutcome {
    /// Whether the subscriber drain was confirmed within the timeout.
    pub drain_confirmed: bool,
}

/// Outcome of resolving the initial batch sequence for a worker's batch builder.
pub(crate) enum InitialBatchSeq {
    /// Pass this seq to `start_batch_builder`.
    Use(u64),
    /// Skip `start_batch_builder`; the outer select will route the pending
    /// mode transition or keep running in CvvInactive. The walk will run on
    /// the next post-catchup epoch when `ConsensusBlocks` is fully populated.
    Defer,
    /// Node shutdown was signaled while waiting for replay.
    Shutdown,
}

/// Context for a single transition, grouping resources needed
/// across the sequential phases.
pub(crate) struct TransitionCtx<'a, DB: Database> {
    pub engine: &'a ExecutionNode,
    pub to_engine: &'a mpsc::Sender<(CameFrom, ConsensusOutput)>,
    pub primary: &'a PrimaryNode<DB>,
    pub consensus_shutdown: Notifier,
    pub epoch_task_manager: &'a mut TaskManager,
    pub gas_accumulator: GasAccumulator,
}
