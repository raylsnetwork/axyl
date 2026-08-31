// SPDX-License-Identifier: BUSL-1.1
//! Typestate machine for the batch builder, making an illegal build/seal ordering unrepresentable.
//!
//! Each phase is a distinct type, so a transition that does not exist (say, sealing without first
//! accumulating candidate transactions) cannot be written. [`PipelineState`] wraps the active phase
//! so the `select!` loop can hold it across iterations.

use crate::{
    batch::SelectedForSeal, error::BatchBuilderResult, BOUNDARY_QUIESCE_WINDOW_SECS, MAX_SEAL_AHEAD,
};
use rayls_execution_evm::in_flight::SealMarks;
use std::fmt;
use tokio::sync::oneshot;

/// Phase with no candidate transactions pending a seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clean;

/// Phase with candidates waiting for the next build attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accumulating;

/// Phase draining a batch that filled to capacity, so more candidates certainly remain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogDraining;

/// Terminal phase once the epoch boundary is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// Phase with a build task in flight, awaiting the worker's quorum resolution.
pub struct AwaitingQuorum {
    /// The in-flight build task's outcome.
    pub(crate) rx: oneshot::Receiver<BatchBuilderResult<TaskOutcome>>,
    /// Whether a candidate event arrived mid-build, so the seal must re-accumulate rather than
    /// return to clean and lose the wake.
    pub(crate) event_arrived_while_waiting: bool,
}

impl fmt::Debug for AwaitingQuorum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwaitingQuorum")
            .field("rx", &"<oneshot::Receiver>")
            .field("event_arrived_while_waiting", &self.event_arrived_while_waiting)
            .finish()
    }
}

/// How close the canonical tip is to the epoch boundary, which throttles the seal-ahead budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochPhase {
    /// Well before the boundary: seal up to [`MAX_SEAL_AHEAD`] batches ahead of execution.
    Active,
    /// Within the quiesce window: allow at most one outstanding batch.
    Quiescing,
    /// At or past the boundary: seal nothing more this epoch.
    Closed,
}

impl EpochPhase {
    /// Classifies the phase from the boundary and the canonical tip timestamp.
    pub fn evaluate(epoch_boundary: u64, canonical_tip: u64) -> Self {
        if canonical_tip >= epoch_boundary {
            Self::Closed
        } else if epoch_boundary.saturating_sub(canonical_tip) <= BOUNDARY_QUIESCE_WINDOW_SECS {
            Self::Quiescing
        } else {
            Self::Active
        }
    }

    /// Returns the most batches that may be outstanding (sealed but unexecuted) in this phase.
    #[inline]
    pub fn max_allowed_ahead(&self) -> u64 {
        match self {
            Self::Active => MAX_SEAL_AHEAD,
            Self::Quiescing => 1,
            Self::Closed => 0,
        }
    }
}

/// Why the budget predicate refused to start a new build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateRejectionReason {
    /// The epoch boundary has been reached; no further batches seal this epoch.
    EpochBoundaryReached,
    /// The seal-ahead budget is already full; wait for execution to catch up.
    BudgetExhausted,
}

/// The resolution of a spawned build-and-seal task.
#[derive(Debug)]
pub enum TaskOutcome {
    /// Every candidate was already in flight, so nothing sealed.
    NothingToSeal,
    /// The worker failed to reach quorum; the same sequence retries next window.
    QuorumFailed,
    /// The batch reached quorum; `selected` must be marked in flight.
    QuorumSucceeded {
        /// The transactions sealed into the batch, to mark in flight.
        selected: SelectedForSeal,
        /// Whether the batch filled to capacity, implying more candidates remain.
        at_capacity: bool,
    },
}

/// Context carried unchanged across every phase transition.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PipelineData {
    /// The sequence the builder resumed from; `start_seq - 1` stands in for the executed watermark
    /// until execution reports one.
    pub(crate) start_seq: u64,
    /// The highest sequence sealed to quorum so far, if any; never moves backward.
    pub(crate) highest_durable_sealed_seq: Option<u64>,
    /// The sequence the next build uses; advances only on a successful seal.
    pub(crate) current_seq: u64,
    /// The latest canonical tip timestamp, which the boundary and quiesce checks read.
    pub(crate) last_canonical_timestamp: u64,
}

/// The builder pipeline in a statically known phase `S`.
///
/// Legal transitions, each a consuming method on the matching `impl` block:
/// - `Clean` -> `Accumulating` on a candidate event;
/// - `Accumulating` | `BacklogDraining` -> `AwaitingQuorum` when a build spawns;
/// - `AwaitingQuorum` -> `Clean` | `Accumulating` | `BacklogDraining` on the quorum outcome;
/// - any phase -> `Closed` once the canonical tip reaches the epoch boundary; `Closed` has no
///   outgoing transition.
#[derive(Debug)]
pub struct BatchPipeline<S> {
    /// The phase marker, carrying per-phase payload where one exists.
    pub(crate) state: S,
    /// The phase-independent context.
    pub(crate) data: PipelineData,
}

impl<S> BatchPipeline<S> {
    /// Returns the sequence number the next build will use.
    #[inline]
    pub fn current_seq(&self) -> u64 {
        self.data.current_seq
    }

    /// Returns the highest sequence sealed so far, if any.
    #[inline]
    pub fn highest_durable_sealed_seq(&self) -> Option<u64> {
        self.data.highest_durable_sealed_seq
    }

    /// Records the latest canonical tip timestamp, which gates the boundary and quiesce checks.
    #[inline]
    pub fn on_canonical_update(&mut self, timestamp: u64) {
        self.data.last_canonical_timestamp = timestamp;
    }
}

impl BatchPipeline<Clean> {
    /// Starts a fresh pipeline resuming from `start_seq` with the given durable seal high-water
    /// mark.
    pub fn new(
        persisted_highest_sealed_seq: Option<u64>,
        start_seq: u64,
        canonical_timestamp: u64,
    ) -> Self {
        Self {
            state: Clean,
            data: PipelineData {
                start_seq,
                highest_durable_sealed_seq: persisted_highest_sealed_seq,
                current_seq: start_seq,
                last_canonical_timestamp: canonical_timestamp,
            },
        }
    }

    /// Moves to `Accumulating` once a candidate transaction event arrives.
    pub fn on_event(self) -> BatchPipeline<Accumulating> {
        BatchPipeline { state: Accumulating, data: self.data }
    }

    /// Closes the pipeline when the canonical tip has reached the epoch boundary.
    pub fn check_boundary(self, epoch_boundary: u64) -> Result<Self, BatchPipeline<Closed>> {
        if self.data.last_canonical_timestamp >= epoch_boundary {
            Err(BatchPipeline { state: Closed, data: self.data })
        } else {
            Ok(self)
        }
    }
}

impl BatchPipeline<Accumulating> {
    /// Returns whether a build may start now, given execution progress and the boundary.
    pub fn can_start_build(
        &self,
        own_executed_seq: Option<u64>,
        epoch_boundary: u64,
    ) -> Result<(), GateRejectionReason> {
        check_build_budget(&self.data, own_executed_seq, epoch_boundary)
    }

    /// Moves to `AwaitingQuorum` once the build task is spawned.
    pub fn start_building(
        self,
        rx: oneshot::Receiver<BatchBuilderResult<TaskOutcome>>,
    ) -> BatchPipeline<AwaitingQuorum> {
        BatchPipeline {
            state: AwaitingQuorum { rx, event_arrived_while_waiting: false },
            data: self.data,
        }
    }

    /// Closes the pipeline when the canonical tip has reached the epoch boundary.
    pub fn check_boundary(self, epoch_boundary: u64) -> Result<Self, BatchPipeline<Closed>> {
        if self.data.last_canonical_timestamp >= epoch_boundary {
            Err(BatchPipeline { state: Closed, data: self.data })
        } else {
            Ok(self)
        }
    }
}

impl BatchPipeline<BacklogDraining> {
    /// Returns whether a build may start now, given execution progress and the boundary.
    pub fn can_start_build(
        &self,
        own_executed_seq: Option<u64>,
        epoch_boundary: u64,
    ) -> Result<(), GateRejectionReason> {
        check_build_budget(&self.data, own_executed_seq, epoch_boundary)
    }

    /// Moves to `AwaitingQuorum` once the build task is spawned.
    pub fn start_building(
        self,
        rx: oneshot::Receiver<BatchBuilderResult<TaskOutcome>>,
    ) -> BatchPipeline<AwaitingQuorum> {
        BatchPipeline {
            state: AwaitingQuorum { rx, event_arrived_while_waiting: false },
            data: self.data,
        }
    }

    /// Closes the pipeline when the canonical tip has reached the epoch boundary.
    pub fn check_boundary(self, epoch_boundary: u64) -> Result<Self, BatchPipeline<Closed>> {
        if self.data.last_canonical_timestamp >= epoch_boundary {
            Err(BatchPipeline { state: Closed, data: self.data })
        } else {
            Ok(self)
        }
    }
}

impl BatchPipeline<AwaitingQuorum> {
    /// Records that a candidate event arrived while a build was already in flight.
    #[inline]
    pub fn on_event(&mut self) {
        self.state.event_arrived_while_waiting = true;
    }

    /// Returns the receiver the loop awaits for the quorum resolution.
    #[inline]
    pub fn rx_mut(&mut self) -> &mut oneshot::Receiver<BatchBuilderResult<TaskOutcome>> {
        &mut self.state.rx
    }

    /// Returns to `Clean` after a build that sealed nothing or found the worker gone.
    pub fn into_clean(self) -> BatchPipeline<Clean> {
        BatchPipeline { state: Clean, data: self.data }
    }

    /// Returns to `Accumulating` to retry after a failed quorum.
    pub fn into_accumulating(self) -> BatchPipeline<Accumulating> {
        BatchPipeline { state: Accumulating, data: self.data }
    }

    /// Marks the sealed batch in flight, advances the sequence, and picks the next phase.
    ///
    /// A full batch drains a backlog, a candidate that arrived mid-build re-accumulates, and
    /// otherwise the pipeline returns to clean.
    pub fn mark_in_flight_and_advance(
        mut self,
        selected: SelectedForSeal,
        at_capacity: bool,
        in_flight: &SealMarks,
    ) -> SealedTransition {
        in_flight.mark(selected.into_marks());

        let sealed_seq = self.data.current_seq;
        self.data.highest_durable_sealed_seq = Some(
            self.data.highest_durable_sealed_seq.map_or(sealed_seq, |prev| prev.max(sealed_seq)),
        );
        self.data.current_seq += 1;

        let event_arrived = self.state.event_arrived_while_waiting;

        if at_capacity {
            SealedTransition::BacklogDraining(BatchPipeline {
                state: BacklogDraining,
                data: self.data,
            })
        } else if event_arrived {
            SealedTransition::Accumulating(BatchPipeline { state: Accumulating, data: self.data })
        } else {
            SealedTransition::Clean(BatchPipeline { state: Clean, data: self.data })
        }
    }

    /// Closes the pipeline when the canonical tip has reached the epoch boundary.
    pub fn check_boundary(self, epoch_boundary: u64) -> Result<Self, BatchPipeline<Closed>> {
        if self.data.last_canonical_timestamp >= epoch_boundary {
            Err(BatchPipeline { state: Closed, data: self.data })
        } else {
            Ok(self)
        }
    }
}

impl BatchPipeline<Closed> {
    /// Returns true; the terminal phase has no outgoing transitions.
    #[inline]
    pub fn is_closed(&self) -> bool {
        true
    }
}

/// The non-terminal phase a completed seal transitions into.
#[derive(Debug)]
pub enum SealedTransition {
    /// No further candidates; back to clean.
    Clean(BatchPipeline<Clean>),
    /// A candidate arrived mid-build; keep accumulating.
    Accumulating(BatchPipeline<Accumulating>),
    /// The batch filled to capacity; drain the remaining backlog.
    BacklogDraining(BatchPipeline<BacklogDraining>),
}

/// The active pipeline phase, held by the `select!` loop across iterations.
#[derive(Debug)]
pub enum PipelineState {
    /// No candidates pending.
    Clean(BatchPipeline<Clean>),
    /// Candidates pending a build.
    Accumulating(BatchPipeline<Accumulating>),
    /// Draining a capacity-filled batch's backlog.
    BacklogDraining(BatchPipeline<BacklogDraining>),
    /// A build is in flight awaiting quorum.
    AwaitingQuorum(BatchPipeline<AwaitingQuorum>),
}

impl From<SealedTransition> for PipelineState {
    fn from(transition: SealedTransition) -> Self {
        match transition {
            SealedTransition::Clean(p) => Self::Clean(p),
            SealedTransition::Accumulating(p) => Self::Accumulating(p),
            SealedTransition::BacklogDraining(p) => Self::BacklogDraining(p),
        }
    }
}

impl PipelineState {
    /// Returns whether a build task is currently in flight.
    #[inline]
    pub fn is_awaiting_quorum(&self) -> bool {
        matches!(self, Self::AwaitingQuorum(_))
    }

    /// Returns the sequence number the next build will use.
    #[inline]
    pub fn current_seq(&self) -> u64 {
        match self {
            Self::Clean(p) => p.current_seq(),
            Self::Accumulating(p) => p.current_seq(),
            Self::BacklogDraining(p) => p.current_seq(),
            Self::AwaitingQuorum(p) => p.current_seq(),
        }
    }

    /// Awaits the quorum resolution when a build is in flight, otherwise parks forever so the
    /// `select!` branch is inert in every other phase.
    pub async fn await_quorum(
        &mut self,
    ) -> Result<BatchBuilderResult<TaskOutcome>, oneshot::error::RecvError> {
        match self {
            Self::AwaitingQuorum(p) => p.rx_mut().await,
            _ => std::future::pending().await,
        }
    }

    /// Registers a candidate event, moving `Clean` to `Accumulating` and flagging an in-flight
    /// build.
    pub fn on_event(self) -> Self {
        match self {
            Self::Clean(p) => Self::Accumulating(p.on_event()),
            Self::Accumulating(p) => Self::Accumulating(p),
            Self::BacklogDraining(p) => Self::BacklogDraining(p),
            Self::AwaitingQuorum(mut p) => {
                p.on_event();
                Self::AwaitingQuorum(p)
            }
        }
    }

    /// Records the latest canonical tip timestamp in whichever phase is active.
    pub fn on_canonical_update(&mut self, timestamp: u64) {
        match self {
            Self::Clean(p) => p.on_canonical_update(timestamp),
            Self::Accumulating(p) => p.on_canonical_update(timestamp),
            Self::BacklogDraining(p) => p.on_canonical_update(timestamp),
            Self::AwaitingQuorum(p) => p.on_canonical_update(timestamp),
        }
    }

    /// Closes the pipeline when the canonical tip has reached the epoch boundary.
    pub fn check_boundary(self, epoch_boundary: u64) -> Result<Self, BatchPipeline<Closed>> {
        match self {
            Self::Clean(p) => p.check_boundary(epoch_boundary).map(Self::Clean),
            Self::Accumulating(p) => p.check_boundary(epoch_boundary).map(Self::Accumulating),
            Self::BacklogDraining(p) => p.check_boundary(epoch_boundary).map(Self::BacklogDraining),
            Self::AwaitingQuorum(p) => p.check_boundary(epoch_boundary).map(Self::AwaitingQuorum),
        }
    }
}

/// Refuses a build past the boundary or once the phase's seal-ahead budget is full.
#[inline]
fn check_build_budget(
    data: &PipelineData,
    own_executed_seq: Option<u64>,
    epoch_boundary: u64,
) -> Result<(), GateRejectionReason> {
    let phase = EpochPhase::evaluate(epoch_boundary, data.last_canonical_timestamp);
    if phase == EpochPhase::Closed {
        return Err(GateRejectionReason::EpochBoundaryReached);
    }

    let in_flight = calculate_in_flight_depth(
        data.highest_durable_sealed_seq,
        data.start_seq,
        own_executed_seq,
    );

    if in_flight >= phase.max_allowed_ahead() {
        return Err(GateRejectionReason::BudgetExhausted);
    }

    Ok(())
}

/// Returns the number of sealed-but-unexecuted batches: the seal high-water mark minus the executed
/// watermark, floored at the pre-resume sequence when execution has not reported yet.
#[inline]
pub fn calculate_in_flight_depth(
    highest_sealed_seq: Option<u64>,
    start_seq: u64,
    own_executed_seq: Option<u64>,
) -> u64 {
    let sealed = match highest_sealed_seq {
        Some(s) => s,
        None => return 0,
    };
    let executed = own_executed_seq.unwrap_or_else(|| start_seq.saturating_sub(1));
    sealed.saturating_sub(executed)
}
