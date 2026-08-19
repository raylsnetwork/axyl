// SPDX-License-Identifier: BUSL-1.1
//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

// silence unused lib deps that are used in IT tests
#![allow(unused_crate_dependencies)]

pub mod batch;
mod error;
mod execution;
mod gas;

use error::EngineResult;
pub use error::RLEngineError;
pub use execution::{execute_consensus_output, Processor};
use futures::{Future, StreamExt};
use futures_util::FutureExt;
use rayls_execution_evm::{payload::BuildArguments, reth_env::RethEnv};
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{
    batch_tracker::BatchTracker, executed_batch_registry::ExecutedBatchRegistry,
    gas_accumulator::GasAccumulator, CameFrom, ConsensusHeader, ConsensusOutput, Database, Epoch,
    Hash as _, Noticer, Round, SealedHeader, TaskSpawner, B256,
};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use crate::batch::BatchOrdering;

/// Admission ceiling for the local execution queue.
///
/// At the ceiling the engine stops draining its inbound channel instead of growing the queue, so
/// execution lag backs up the channel and reaches consensus as backpressure rather than being
/// absorbed as memory (each queued output owns its transaction payloads). Nothing is dropped:
/// unadmitted outputs wait in the channel until the queue drains.
pub const MAX_QUEUED_OUTPUTS: usize = 100;

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// [`SealedHeader`].
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

/// Identity of the last consensus output admitted to the execution queue: the engine's dedup
/// and ordering anchor. Keyed on the deterministic `(epoch, leader_round)`, not the node-local
/// `number`, which can drift across catch-up handoffs.
#[derive(Clone, Copy)]
struct LastAdmitted {
    /// Deterministic `(epoch, leader_round)`: the dedup and ordering key.
    position: (Epoch, Round),
    /// Subdag content digest, telling a benign re-delivery from a fork at the same position.
    subdag_digest: B256,
    /// Node-local output number. Diagnostics only (numbering-drift warning).
    number: u64,
}

/// Execution engine that receives consensus output and produces EVM blocks.
///
/// The engine makes no attempt to track consensus. Its only purpose is to receive output from
/// consensus then try to execute it.
///
/// The engine runs until either the maximum round of consensus is reached OR the sending broadcast
/// channel is dropped. If the sending channel is dropped, the engine attempts to execute any
/// remaining output that is queued up before shutting itself down gracefully. If the maximum round
/// is reached, the engine shuts down immediately.
pub struct ExecutorEngine<DB: Database> {
    /// Backlog of output from consensus ready to be executed, tagged with the path that
    /// delivered it (for tracing only).
    queued: VecDeque<(CameFrom, ConsensusOutput)>,
    /// Single active future that executes consensus output on a blocking thread.
    /// Paired with the output number for signaling completion and the executed output's
    /// [`ConsensusHeader`] anchor to publish once a block is produced.
    pending_task: Option<(PendingExecutionTask, u64, ConsensusHeader)>,
    /// Optional max round for testing/debugging.
    max_round: Option<u64>,
    /// Receiving end of the consensus output channel.
    consensus_output_stream: ReceiverStream<(CameFrom, ConsensusOutput)>,
    /// The [`SealedHeader`] of the last fully-executed block.
    parent_header: SealedHeader,
    /// Shutdown notification receiver.
    rx_shutdown: Noticer,
    /// Task spawner for blocking execution tasks.
    task_spawner: TaskSpawner,
    /// Identity of the last consensus output admitted to the queue: the dedup and ordering anchor.
    last_admitted: Option<LastAdmitted>,
    /// Shared execution services (reth env, gas, tracking, dedup, batch ordering).
    processor: Processor<DB>,
    /// Optional sender publishing the [`ConsensusHeader`] the highest executed block commits to.
    /// Every output produces a block, so this advances on every successfully executed output.
    executed_anchor_tx: Option<watch::Sender<ConsensusHeader>>,
    /// Optional sender publishing whether the engine is idle: no pending execution task and an
    /// empty queue, i.e. it has executed everything it admitted. A mode transition waits on this
    /// to drain the admitted backlog before the next epoch starts.
    engine_idle_tx: Option<watch::Sender<bool>>,
    /// Set once `rx_shutdown` has fired. A channel closing after this is a normal
    /// teardown signal (the producer/executor tasks are going away), not a fatal
    /// fault, so the engine exits cleanly with `Ok` instead of erroring.
    shutdown_requested: bool,
}

/// Number of recent blocks to scan for batch digest reconstruction on startup.
const DIGEST_RECONSTRUCTION_DEPTH: u64 = 2000;

/// Reconstruct executed batch digests from recent canonical blocks.
///
/// The batch_digest is recovered as `mix_hash ^ parent_beacon_block_root` since
/// `mix_hash = output_digest ^ batch_digest` and `parent_beacon_block_root = output_digest`.
///
/// An empty EVM block can mean two different things, and `consensus_db` (the batch's transactions)
/// plus current EVM state are consulted to tell them apart: a batch whose txns are still PENDING
/// (nonce-too-high) must be re-enabled for retry (`drop_digest`), but a batch whose txns are
/// already MINED (nonce-too-low - it was only a stale reproposal) must stay registered, otherwise
/// a restart re-executes it and forks the chain.
pub fn reconstruct_batch_digests<CDB: Database>(
    reth_env: &RethEnv,
    tip: u64,
    consensus_db: &CDB,
) -> ExecutedBatchRegistry {
    let executed_batch_registry = ExecutedBatchRegistry::default();
    let start = tip.saturating_sub(DIGEST_RECONSTRUCTION_DEPTH).max(1);
    for num in start..=tip {
        match reth_env.header_by_number(num) {
            Ok(Some(header)) => {
                let output_digest = header.parent_beacon_block_root.unwrap_or_default();
                // empty rounds have mix_hash == output_digest (batch_digest is zero)
                if header.mix_hash != output_digest && !output_digest.is_zero() {
                    let batch_digest = header.mix_hash ^ output_digest;
                    executed_batch_registry.try_register(batch_digest, output_digest);
                    // An empty block means the batch's txns were excluded. Only re-enable retry
                    // when those txns are STILL PENDING (nonce-too-high) against current state; if
                    // they're already mined (nonce-too-low, a stale reproposal) the batch is done
                    // and must stay registered so a restart cannot re-execute it and fork.
                    if header.transaction_root_is_empty() {
                        let retryable = match consensus_db.get::<Batches>(&batch_digest) {
                            Ok(Some(b)) => reth_env.batch_txns_all_pending(&b.transactions),
                            // Missing batch is expected for older blocks outside the write window;
                            // stay deduped (conservative).
                            Ok(None) => false,
                            // A DB error is NOT expected: log it, but still stay deduped.
                            Err(e) => {
                                warn!(
                                    target: "engine",
                                    ?batch_digest,
                                    block_number = num,
                                    ?e,
                                    "consensus_db read failed during dedup reconstruction; keeping registered"
                                );
                                false
                            }
                        };
                        if retryable {
                            executed_batch_registry.drop_digest(batch_digest);
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    target: "engine",
                    block_number = num,
                    ?e,
                    "failed to read header for batch digest reconstruction"
                );
                break;
            }
        }
    }
    executed_batch_registry
}

impl<DB: Database> ExecutorEngine<DB> {
    /// Create a new [`ExecutorEngine`].
    ///
    /// The engine waits for CL to broadcast output then tries to execute.
    pub fn new(
        reth_env: RethEnv,
        max_round: Option<u64>,
        rx_consensus_output: mpsc::Receiver<(CameFrom, ConsensusOutput)>,
        parent_header: SealedHeader,
        rx_shutdown: Noticer,
        task_spawner: TaskSpawner,
        gas_accumulator: GasAccumulator,
        batch_tracker_arg: Option<Arc<BatchTracker>>,
        gas_limit: u64,
        batch_ordering: BatchOrdering<DB>,
        executed_anchor_tx: Option<watch::Sender<ConsensusHeader>>,
        engine_idle_tx: Option<watch::Sender<bool>>,
        last_consensus_header: ConsensusHeader,
        executed_batch_registry: ExecutedBatchRegistry,
        in_flight_tracker: rayls_execution_evm::in_flight::InFlightTracker,
    ) -> Self {
        let consensus_output_stream = ReceiverStream::new(rx_consensus_output);

        let processor = Processor::new(
            reth_env,
            gas_accumulator,
            batch_tracker_arg,
            executed_batch_registry,
            batch_ordering,
            gas_limit,
            in_flight_tracker,
        );

        // Seed the dedup anchor from the last executed consensus header. A genesis/default
        // header (number 0) means no prior execution, so the first output is always admitted.
        let last_admitted = (last_consensus_header.number > 0).then(|| {
            let sub_dag = &last_consensus_header.sub_dag;
            LastAdmitted {
                position: (sub_dag.leader_epoch(), sub_dag.leader_round()),
                subdag_digest: sub_dag.digest().into(),
                number: last_consensus_header.number,
            }
        });

        Self {
            queued: Default::default(),
            pending_task: None,
            max_round,
            consensus_output_stream,
            parent_header,
            rx_shutdown,
            task_spawner,
            last_admitted,
            processor,
            executed_anchor_tx,
            engine_idle_tx,
            shutdown_requested: false,
        }
    }

    /// Test-only [`Self::new`] that seeds a genesis (`ConsensusHeader::default`) dedup anchor.
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_test(
        reth_env: RethEnv,
        max_round: Option<u64>,
        rx_consensus_output: mpsc::Receiver<(CameFrom, ConsensusOutput)>,
        parent_header: SealedHeader,
        rx_shutdown: Noticer,
        task_spawner: TaskSpawner,
        gas_accumulator: GasAccumulator,
        batch_tracker_arg: Option<Arc<BatchTracker>>,
        gas_limit: u64,
        batch_ordering: BatchOrdering<DB>,
        in_flight_tracker: rayls_execution_evm::in_flight::InFlightTracker,
    ) -> Self {
        Self::new(
            reth_env,
            max_round,
            rx_consensus_output,
            parent_header,
            rx_shutdown,
            task_spawner,
            gas_accumulator,
            batch_tracker_arg,
            gas_limit,
            batch_ordering,
            None,
            None,
            ConsensusHeader::default(),
            ExecutedBatchRegistry::default(),
            in_flight_tracker,
        )
    }

    /// Set the batch lifecycle tracker.
    pub fn set_batch_tracker(&mut self, tracker: Arc<BatchTracker>) {
        self.processor.set_batch_tracker(tracker);
    }

    /// Spawn a blocking task to execute the next queued consensus output.
    ///
    /// Executing blocks is cpu intensive, so a blocking task is used to yield back to the runtime.
    /// Returns the output number and the executed output's [`ConsensusHeader`] anchor alongside
    /// the oneshot receiver so the caller can signal completion and advance the anchor.
    fn spawn_execution_task(&mut self) -> (PendingExecutionTask, u64, ConsensusHeader) {
        let (tx, rx) = oneshot::channel();
        let output_number;
        let anchor;

        // pop next output in queue and execute
        if let Some((came_from, output)) = self.queued.pop_front() {
            output_number = output.number;
            // capture the anchor (clones the subdag certs, not the batch payloads) before the
            // output is moved into the blocking task.
            anchor = output.consensus_header();
            let reth_env = self.processor.reth_env().clone();
            let parent = self.parent_header.clone();
            let task_name = format!("execution-output-{}", output.consensus_header_hash());
            let build_args = BuildArguments::new(reth_env, output, parent);

            let processor = self.processor.clone();
            // spawn blocking task and return future
            self.task_spawner.spawn_blocking_task(task_name, move || {
                // safe to call on blocking thread without a semaphore because it's held in
                // Self::pending_task as a single `Option`
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    processor.execute_consensus_output(build_args, came_from).inspect_err(|e| {
                        error!(target: "engine", ?e, "error executing consensus output");
                    })
                }))
                .unwrap_or_else(|panic_val| {
                    let msg = panic_val
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic_val.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic payload".to_string());
                    error!(target: "engine", panic = %msg, "PANIC in execute_consensus_output - set RUST_BACKTRACE=1 for details");
                    Err(RLEngineError::ExecutionPanic(msg))
                });
                if let Err(e) = tx.send(result) {
                    warn!(target: "engine", ?e, "error sending result from execute_consensus_output")
                }
            });
        } else {
            // unreachable: spawn_execution_task is only called when the queue is non-empty
            output_number = 0;
            anchor = ConsensusHeader::default();
            let _ = tx.send(Err(RLEngineError::EmptyQueue));
        }

        // oneshot receiver for execution result
        (rx, output_number, anchor)
    }

    /// Check if the engine has reached the maximum round of consensus.
    #[cfg(any(test, feature = "test-utils"))]
    fn has_reached_max_round(&self, progress: u64) -> bool {
        let (_, round) = RethEnv::deconstruct_nonce(progress);
        let has_reached_max_round =
            self.max_round.map(|target| round as u64 >= target).unwrap_or_default();
        if has_reached_max_round {
            tracing::trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_round,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_round
    }

    /// TESTING ONLY - push a consensus output to the back of the queue.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn push_back_queued_for_test(&mut self, output: ConsensusOutput) {
        self.queued.push_back((CameFrom::Test, output))
    }
}

/// The [`ExecutorEngine`] is a future that loops through:
/// - receive messages from consensus
/// - add these messages to a queue
/// - pull from queue to start next execution task if idle
/// - poll any pending tasks being executed
///
/// If a task completes, the loop continues to poll for new output then begins the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks
/// and any queued output before shutting down.
impl<DB: Database> Future for ExecutorEngine<DB> {
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check for shutdown signal
        if pin!(&this.rx_shutdown).poll(cx).is_ready() {
            info!(target: "engine", "received shutdown signal...");
            this.shutdown_requested = true;
            // only return if there are no current tasks and the queue is empty
            // otherwise, let the loop continue so any remaining tasks and queued output is
            // executed. rx_shutdown continues to poll ready so once the queue clears, we shut down.
            if this.pending_task.is_none() && this.queued.is_empty() {
                return Poll::Ready(Ok(()));
            }
        }

        loop {
            // At capacity, leave the output in the inbound channel: that is what carries the
            // backpressure to consensus (see [`MAX_QUEUED_OUTPUTS`]). No waker is registered on
            // the stream here; the pending execution task wakes the engine when it completes, and
            // admission resumes once the queue has room.
            let poll = if this.queued.len() >= MAX_QUEUED_OUTPUTS {
                Poll::Pending
            } else {
                this.consensus_output_stream.poll_next_unpin(cx)
            };
            match poll {
                Poll::Ready(Some((came_from, output))) => {
                    // Dedup and order on the deterministic `(epoch, leader_round)` + subdag
                    // digest, not the node-local `number` (which can drift across handoffs).
                    let output_epoch = output.leader().epoch();
                    let output_round = output.leader_round();
                    let position = (output_epoch, output_round);

                    if let Some(last) = this.last_admitted {
                        match position.cmp(&last.position) {
                            Ordering::Less => {
                                // Behind the last admitted position: a stale re-delivery or an
                                // unfillable late gap. Cannot extend the chain, so drop.
                                warn!(
                                    target: "engine",
                                    came_from = %came_from,
                                    output_number = output.number,
                                    output_epoch,
                                    output_round,
                                    last_epoch = last.position.0,
                                    last_round = last.position.1,
                                    "dropping stale/out-of-order consensus output (behind last position)"
                                );
                                if let Some(tracker) = this.processor.batch_tracker() {
                                    tracker.output_duplicate_dropped(output.number);
                                }
                                continue;
                            }
                            Ordering::Equal => {
                                let output_digest: B256 = output.sub_dag.digest().into();
                                if last.subdag_digest == output_digest {
                                    // Same position and content: benign re-delivery (dual feed).
                                    debug!(
                                        target: "engine",
                                        came_from = %came_from,
                                        output_number = output.number,
                                        output_epoch,
                                        output_round,
                                        "dropping duplicate consensus output (same position, identical content)"
                                    );
                                    if let Some(tracker) = this.processor.batch_tracker() {
                                        tracker.output_duplicate_dropped(output.number);
                                    }
                                    continue;
                                }
                                // Divergent content at an already-admitted position: a fork. Halt
                                // so the node resyncs instead of silently extending it.
                                error!(
                                    target: "engine",
                                    output_number = output.number,
                                    output_epoch,
                                    output_round,
                                    expected_subdag = ?last.subdag_digest,
                                    got_subdag = ?output_digest,
                                    "consensus fork detected: divergent content at an already-executed \
                                     (epoch, round); halting engine to force resync"
                                );
                                return Poll::Ready(Err(RLEngineError::ConsensusFork {
                                    epoch: output_epoch,
                                    round: output_round,
                                }));
                            }
                            Ordering::Greater => {
                                // Newer commit. Warn when the round advanced but the local number
                                // did not, i.e. numbering drift from a catch-up handoff.
                                if output.number <= last.number {
                                    warn!(
                                        target: "engine",
                                        output_number = output.number,
                                        last_number = last.number,
                                        output_epoch,
                                        output_round,
                                        "leader round advanced but local output number did not (numbering drift)"
                                    );
                                }
                                if output_epoch > last.position.0 + 1 {
                                    warn!(
                                        target: "engine",
                                        output_number = output.number,
                                        output_epoch,
                                        last_epoch = last.position.0,
                                        "epoch jumped by more than 1 - may indicate catch-up replay"
                                    );
                                }
                            }
                        }
                    }

                    // Admit. The anchor advances at admit time (before execution): safe with the
                    // deterministic key, since a same-position re-delivery is dropped or forked
                    // above and a newer commit always has a higher position.
                    let output_digest: B256 = output.sub_dag.digest().into();
                    this.last_admitted = Some(LastAdmitted {
                        position,
                        subdag_digest: output_digest,
                        number: output.number,
                    });
                    if let Some(tracker) = this.processor.batch_tracker() {
                        tracker.output_received(output.number);
                    }

                    // Queue the output for local execution.
                    this.queued.push_back((came_from, output))
                }
                Poll::Ready(None) => {
                    // the stream has ended
                    error!(target: "engine", "ConsensusOutput channel closed. Shutting down...");

                    // only return if there are no current tasks and the queue is empty
                    // otherwise, let the loop continue so any remaining tasks and queued output is
                    // executed
                    if this.pending_task.is_none() && this.queued.is_empty() {
                        // During shutdown the producers dropping their senders is the normal
                        // teardown path, so exit cleanly rather than as a fault.
                        if this.shutdown_requested {
                            return Poll::Ready(Ok(()));
                        }
                        return Poll::Ready(Err(RLEngineError::ConsensusOutputStreamClosed));
                    }
                }

                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if this.pending_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to begin executing next round of consensus
                this.pending_task = Some(this.spawn_execution_task());
            }

            // poll receiver that returns output execution result
            if let Some((mut receiver, output_number, anchor)) = this.pending_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        debug!(target: "engine", ?res, "receiver for execution result polled ready");
                        let finalized_header = match res.map_err(Into::into).and_then(|res| res) {
                            Ok(header) => header,
                            // ONLY a closed result channel during shutdown is benign: the blocking
                            // execution task was torn down before sending, so no block was
                            // finalized. Every other error - including ConsensusFork - propagates
                            // even during shutdown, and a closed channel outside shutdown (the
                            // execution task panicked) is a real fault.
                            Err(RLEngineError::ChannelClosed) if this.shutdown_requested => {
                                info!(
                                    target: "engine",
                                    "execution result channel closed during shutdown; exiting cleanly"
                                );
                                return Poll::Ready(Ok(()));
                            }
                            Err(e) => return Poll::Ready(Err(e)),
                        };
                        // store last executed header in memory
                        this.parent_header = finalized_header;

                        // Every output produces at least one block, so advance the EVM-execution
                        // anchor to the consensus header the just-executed block commits to.
                        if let Some(tx) = &this.executed_anchor_tx {
                            tx.send_replace(anchor);
                        }

                        // check max_round to auto shutdown
                        #[cfg(any(test, feature = "test-utils"))]
                        if this.max_round.is_some()
                            && this.has_reached_max_round(this.parent_header.nonce.into())
                        {
                            // immediately terminate if the specified max consensus round is reached
                            return Poll::Ready(Ok(()));
                        }

                        // allow loop to continue: poll broadcast stream for next output
                    }
                    Poll::Pending => {
                        this.pending_task = Some((receiver, output_number, anchor));

                        // break loop and return Poll::Pending
                        break;
                    }
                }
            }
        }

        // Publish whether the engine has gone idle: no pending task and an empty queue means it has
        // executed everything it admitted. A mode transition waits on this to drain the admitted
        // backlog before the next epoch starts. `send_if_modified` avoids spurious wakeups; during
        // a transition the producers are stopped, so this only flips false→true.
        if let Some(tx) = &this.engine_idle_tx {
            let idle = this.pending_task.is_none() && this.queued.is_empty();
            tx.send_if_modified(|cur| {
                if *cur != idle {
                    *cur = idle;
                    true
                } else {
                    false
                }
            });
        }

        // all output executed, yield back to runtime
        Poll::Pending
    }
}

impl<DB: Database> std::fmt::Debug for ExecutorEngine<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorEngine")
            .field("queued", &self.queued.len())
            .field("pending_task", &self.pending_task.is_some())
            .field("max_round", &self.max_round)
            .field("parent_header", &self.parent_header)
            .finish_non_exhaustive()
    }
}
