// SPDX-License-Identifier: BUSL-1.1
//! The batch builder, which seals pending-pool transactions into batches for the worker to propose.
//!
//! Only transactions in the pending sub-pool are candidates; the engine's canonical updates alone
//! move the pool's tracked tip, basefee, and blob fees, so every worker validates a peer's batch
//! off the same canonical state. A sealed batch goes to the worker, which publishes it and seeks
//! quorum within a time limit. On quorum failure the candidates are left untouched for the next
//! attempt. On quorum the sealed transactions are marked in flight so the next build skips them;
//! they stay in the pending pool until execution mines them, which releases the marks.

// it tests
#![allow(unused_crate_dependencies)]

pub use batch::{build_batch, BatchBuilderOutput, SelectedForSeal};
pub use watermark::OwnWatermarkReceiver;

use error::{BatchBuilderError, BatchBuilderResult};
use futures_util::{FutureExt, StreamExt};
use pipeline::{BatchPipeline, PipelineState, TaskOutcome};
use rayls_execution_evm::{
    in_flight::{DuePolicy, SealMarks},
    reth_env::RethEnv,
    CanonStateNotificationStream, WorkerTxPool,
};
use rayls_infrastructure_types::{
    batch_ordering::MAX_PARKED_PER_AUTHORITY, error::BlockSealError,
    gas_accumulator::BaseFeeContainer, Address, BatchBuilderArgs, BatchSender, Epoch, SealedBatch,
    SenderNonceRanges, TaskSpawner, TxHash, WorkerId,
};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Interval, MissedTickBehavior},
};
use tracing::{debug, error, warn};

mod batch;
mod error;
pub mod pipeline;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod watermark;

/// How long a sealed transaction stays marked in flight before a sweep may presume its batch lost
/// and release it for resealing. Inert unless a sweep runs; reconcile releases marks on execution
/// well before this.
const IN_FLIGHT_TTL: Duration = Duration::from_secs(60);

/// The most batches the builder may seal ahead of its own execution watermark while the epoch is
/// active. Keeps the in-flight prefix within the per-authority parking budget, so a lagging batch
/// cannot strand its successors.
pub const MAX_SEAL_AHEAD: u64 = 4;
const _: () = assert!(MAX_SEAL_AHEAD * 4 <= MAX_PARKED_PER_AUTHORITY as u64);

/// Seconds before the epoch boundary within which the seal-ahead budget collapses to one batch, so
/// a batch sealed near the cut has no unexecuted predecessor of its own to cascade skips from once
/// the epoch severs per-authority sequence ordering.
pub const BOUNDARY_QUIESCE_WINDOW_SECS: u64 = 5;

/// Builds batches for a worker to propose, sealing ahead of its own execution watermark.
///
/// Driven by [`BatchBuilder::run`], a `select!` loop that reacts to canonical updates, candidate
/// transaction ingress, in-flight mark releases, its own execution watermark, and a batching-window
/// tick. A [`pipeline::BatchPipeline`] typestate tracks whether a build may start and the next
/// phase after each seal, so the seal-ahead budget and epoch-boundary cutoff live in the types
/// rather than in ad hoc flags.
#[derive(Debug)]
pub struct BatchBuilder {
    /// Static per-epoch configuration.
    config: BatchBuilderConfig,
    /// The transaction pool with pending transactions.
    pool: WorkerTxPool,
    /// Sealing capability over the pool's in-flight tracker: marks a batch's hashes on quorum so
    /// the next round skips them until execution releases them. Armed once for this builder's
    /// (epoch-scoped) life.
    in_flight: SealMarks,
    /// The sending side to the worker's batch maker; a send has the worker publish the batch to
    /// all peers.
    to_worker: BatchSender,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The current base fee for this worker.
    base_fee: BaseFeeContainer,
    /// This authority's highest executed batch sequence, bounding how far ahead the builder seals.
    own_executed_watermark: OwnWatermarkReceiver,
    /// Canonical updates from the engine; the tip timestamp gates the epoch boundary and quiesce.
    state_changed: CanonStateNotificationStream,
    /// Wakes the builder when a new candidate enters the pending sub-pool.
    pending_tx_events: mpsc::Receiver<TxHash>,
    /// Wakes the builder when in-flight marks release, reopening candidates for the next seal.
    release_events: watch::Receiver<u64>,
    /// The last canonical tip timestamp seen, seeded from the tip at construction.
    last_canonical_timestamp: u64,
    /// The highest sequence already sealed and durable before this builder started, if any.
    persisted_highest_sealed_seq: Option<u64>,
}

/// Static configuration for a [`BatchBuilder`], fixed for its (epoch-scoped) life.
#[derive(Debug, Clone, Copy)]
pub struct BatchBuilderConfig {
    /// The beneficiary address for this worker's batches.
    pub address: Address,
    /// The worker id this builder belongs to.
    pub worker_id: WorkerId,
    /// The epoch this builder seals batches for.
    pub epoch: Epoch,
    /// Epoch boundary timestamp (seconds); once the canonical tip reaches it the builder stops.
    pub epoch_boundary: u64,
    /// Maximum time to wait before the batching-window tick attempts a build.
    pub max_delay: Duration,
    /// The sequence number the first batch uses, before the execution watermark resumes it.
    pub next_batch_seq: u64,
    /// Block gas limit for batches.
    pub gas_limit: u64,
}

/// The result of a single build attempt on the blocking pool, before quorum.
enum Built {
    /// Every candidate was already in flight, so nothing was sealed.
    NothingToSeal,
    /// A batch was sealed and is ready to send to the worker.
    Sealed {
        /// The sealed batch to send.
        sealed_batch: SealedBatch,
        /// The transactions to mark in flight once quorum is reached.
        selected: SelectedForSeal,
        /// Per-sender nonce ranges carried to the worker.
        sender_nonce_ranges: SenderNonceRanges,
        /// Whether the batch filled to capacity, implying more candidates remain.
        at_capacity: bool,
    },
}

impl BatchBuilder {
    /// Creates a batch builder for one epoch, resuming its sequence from the execution watermark.
    pub fn new(
        reth_env: &RethEnv,
        pool: WorkerTxPool,
        to_worker: BatchSender,
        task_spawner: TaskSpawner,
        base_fee: BaseFeeContainer,
        own_executed_watermark: OwnWatermarkReceiver,
        config: BatchBuilderConfig,
    ) -> Self {
        let tracker = pool.in_flight();
        let release_events = tracker.release_events();
        let in_flight = tracker.arm_sealing(DuePolicy::ttl(IN_FLIGHT_TTL));
        let last_canonical_timestamp = Self::latest_canon_timestamp(reth_env);

        let start_seq = own_executed_watermark.resume_seq(config.next_batch_seq);
        let persisted_highest_sealed_seq = start_seq.checked_sub(1);

        Self {
            config: BatchBuilderConfig { next_batch_seq: start_seq, ..config },
            pool: pool.clone(),
            in_flight,
            to_worker,
            task_spawner,
            base_fee,
            own_executed_watermark,
            state_changed: reth_env.canonical_block_stream(),
            pending_tx_events: pool.pending_transactions_listener(),
            release_events,
            last_canonical_timestamp,
            persisted_highest_sealed_seq,
        }
    }

    /// Runs the builder until the epoch boundary is reached or the worker seal loop disconnects.
    pub async fn run(mut self) -> BatchBuilderResult<()> {
        let mut interval = tokio::time::interval(self.config.max_delay);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.reset();

        let initial_clean = BatchPipeline::new(
            self.persisted_highest_sealed_seq,
            self.config.next_batch_seq,
            self.last_canonical_timestamp,
        );
        let mut pipeline = PipelineState::Clean(initial_clean);
        pipeline = self.try_start_build(pipeline.on_event(), &mut interval);

        loop {
            pipeline = match pipeline.check_boundary(self.config.epoch_boundary) {
                Ok(p) => p,
                Err(_closed) => {
                    debug!(
                        target: "worker::batch_builder",
                        epoch = %self.config.epoch,
                        "epoch boundary reached; terminating batch builder"
                    );
                    return Ok(());
                }
            };

            // Drain backlog while the budget allows before parking on select!, so a full batch does
            // not wait for the next tick.
            if matches!(
                pipeline,
                PipelineState::BacklogDraining(_) | PipelineState::Accumulating(_)
            ) {
                pipeline = self.try_start_build(pipeline, &mut interval);
            }

            let is_awaiting = pipeline.is_awaiting_quorum();

            tokio::select! {
                // Shutdown and boundary detection must win, so keep the branch order semantic.
                biased;

                // Canonical tip updates: burst-drain to the latest tip.
                Some(latest) = self.state_changed.next() => {
                    let tip_ts = latest.tip().sealed_block().timestamp;
                    pipeline.on_canonical_update(tip_ts);
                    while let Some(Some(more)) = self.state_changed.next().now_or_never() {
                        pipeline.on_canonical_update(more.tip().sealed_block().timestamp);
                    }

                    if !is_awaiting && tip_ts >= self.config.epoch_boundary {
                        debug!(
                            target: "worker::batch_builder",
                            epoch = %self.config.epoch,
                            "epoch boundary crossed by canonical tip; terminating builder"
                        );
                        return Ok(());
                    }

                    if !is_awaiting {
                        pipeline = self.try_start_build(pipeline.on_event(), &mut interval);
                    }
                }

                // Quorum resolution: unblock the next sequence promptly.
                rx_res = pipeline.await_quorum(), if is_awaiting => {
                    match self.handle_quorum_resolution(rx_res, pipeline, &mut interval)? {
                        Some(next) => pipeline = next,
                        // The worker seal loop is gone; end the builder rather than spin resealing.
                        None => return Ok(()),
                    }
                }

                // Own watermark advance: wake as soon as one of this authority's batches executes.
                Ok(()) = self.own_executed_watermark.inner_mut().changed(), if !is_awaiting => {
                    let _ = self.own_executed_watermark.inner_mut().borrow_and_update();
                    pipeline = self.try_start_build(pipeline, &mut interval);
                }

                // Candidate transaction ingress: burst-drain.
                Some(_) = self.pending_tx_events.recv() => {
                    pipeline = pipeline.on_event();
                    while self.pending_tx_events.try_recv().is_ok() {}

                    if !is_awaiting {
                        pipeline = self.try_start_build(pipeline, &mut interval);
                    }
                }

                // In-flight mark releases reopen candidates for the next seal.
                Ok(()) = self.release_events.changed() => {
                    let _ = self.release_events.borrow_and_update();
                    pipeline = pipeline.on_event();

                    if !is_awaiting {
                        pipeline = self.try_start_build(pipeline, &mut interval);
                    }
                }

                // Batching window tick: the only wake that re-arms a failed seal's retry.
                _ = interval.tick(), if !is_awaiting => {
                    pipeline = self.try_start_build(pipeline.on_window(), &mut interval);
                }
            }
        }
    }

    /// Starts a build when the active phase allows it, transitioning to `AwaitingQuorum` on spawn.
    fn try_start_build(
        &mut self,
        pipeline: PipelineState,
        interval: &mut Interval,
    ) -> PipelineState {
        let executed_seq = self.own_executed_watermark.get();
        let seq = pipeline.current_seq();

        match pipeline {
            PipelineState::Clean(clean) => PipelineState::Clean(clean),
            PipelineState::Accumulating(acc) => {
                if acc.can_start_build(executed_seq, self.config.epoch_boundary).is_err() {
                    return PipelineState::Accumulating(acc);
                }
                interval.reset();
                let rx = self.spawn_build_task(seq);
                PipelineState::AwaitingQuorum(acc.start_building(rx))
            }
            PipelineState::BacklogDraining(backlog) => {
                if backlog.can_start_build(executed_seq, self.config.epoch_boundary).is_err() {
                    return PipelineState::BacklogDraining(backlog);
                }
                interval.reset();
                let rx = self.spawn_build_task(seq);
                PipelineState::AwaitingQuorum(backlog.start_building(rx))
            }
            PipelineState::AwaitingQuorum(p) => PipelineState::AwaitingQuorum(p),
            PipelineState::QuorumRetry(p) => PipelineState::QuorumRetry(p),
        }
    }

    /// Folds a quorum resolution into the next pipeline phase, advancing the sequence on success.
    ///
    /// Returns `None` when the worker seal loop has disconnected, signalling [`Self::run`] to end.
    fn handle_quorum_resolution(
        &mut self,
        res: Result<BatchBuilderResult<TaskOutcome>, oneshot::error::RecvError>,
        pipeline: PipelineState,
        interval: &mut Interval,
    ) -> BatchBuilderResult<Option<PipelineState>> {
        let awaiting = match pipeline {
            PipelineState::AwaitingQuorum(a) => a,
            other => return Ok(Some(other)),
        };

        let current_seq = awaiting.current_seq();
        let start_time = awaiting.state.started;

        let outcome = match res.map_err(BatchBuilderError::from).and_then(|r| r) {
            Ok(out) => out,
            Err(e) if e.is_worker_gone() => {
                warn!(target: "worker::batch_builder", %e, "worker seal loop disconnected; ending builder");
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        interval.reset();

        match outcome {
            TaskOutcome::NothingToSeal => {
                debug!(target: "worker::batch_builder", "all candidate txs in-flight; nothing to seal");
                Ok(Some(PipelineState::Clean(awaiting.into_clean())))
            }
            TaskOutcome::QuorumFailed => {
                warn!(
                    target: "worker::batch_builder",
                    elapsed_ms = start_time.elapsed().as_millis(),
                    "batch quorum failed; re-armed retry for next window"
                );
                Ok(Some(PipelineState::QuorumRetry(awaiting.into_retry())))
            }
            TaskOutcome::QuorumSucceeded { selected, at_capacity } => {
                debug!(
                    target: "worker::batch_builder",
                    seq = current_seq,
                    elapsed_ms = start_time.elapsed().as_millis(),
                    "batch reached quorum and sealed successfully"
                );

                let transition =
                    awaiting.mark_in_flight_and_advance(selected, at_capacity, &self.in_flight);
                Ok(Some(transition.into()))
            }
        }
    }

    /// Spawns the blocking build, sends the batch to the worker, and reports the quorum outcome.
    fn spawn_build_task(&mut self, seq: u64) -> oneshot::Receiver<BatchBuilderResult<TaskOutcome>> {
        let pool = self.pool.clone();
        let to_worker = self.to_worker.clone();
        let address = self.config.address;
        let epoch = self.config.epoch;
        let worker_id = self.config.worker_id;
        let base_fee = self.base_fee.base_fee();
        let gas_limit = self.config.gas_limit;

        let (outcome_tx, outcome_rx) = oneshot::channel();
        self.task_spawner.spawn_task("build-seal-and-submit-batch", async move {
            let built = match tokio::task::spawn_blocking(move || {
                let build_args = BatchBuilderArgs::new(pool, address, epoch);
                let BatchBuilderOutput { batch, selected, sender_nonce_ranges, at_capacity } =
                    build_batch(build_args, worker_id, base_fee, seq, gas_limit);

                if selected.is_empty() {
                    Built::NothingToSeal
                } else {
                    Built::Sealed {
                        sealed_batch: batch.seal_slow(),
                        selected,
                        sender_nonce_ranges,
                        at_capacity,
                    }
                }
            })
            .await
            {
                Ok(built) => built,
                Err(err) => {
                    let _ = outcome_tx.send(Err(BatchBuilderError::from(err)));
                    return;
                }
            };

            match built {
                Built::NothingToSeal => {
                    let _ = outcome_tx.send(Ok(TaskOutcome::NothingToSeal));
                }
                Built::Sealed { sealed_batch, selected, sender_nonce_ranges, at_capacity } => {
                    if outcome_tx.is_closed() {
                        debug!(target: "worker::batch_builder", "builder cancelled task prior to broadcast");
                        return;
                    }

                    let (ack, ack_rx) = oneshot::channel();

                    if let Err(send_err) =
                        to_worker.send((sealed_batch, sender_nonce_ranges, ack)).await
                    {
                        let err = BatchBuilderError::from(send_err);
                        if !err.is_worker_gone() {
                            error!(target: "worker::batch_builder", ?err, "failed to send next batch to worker");
                        }
                        let _ = outcome_tx.send(Err(err));
                        return;
                    }

                    match ack_rx.await {
                        Ok(Ok(_)) => {
                            let _ = outcome_tx
                                .send(Ok(TaskOutcome::QuorumSucceeded { selected, at_capacity }));
                        }
                        Ok(Err(BlockSealError::FatalDBFailure)) => {
                            let _ = outcome_tx.send(Err(BatchBuilderError::FatalDBFailure));
                        }
                        Ok(Err(_)) => {
                            let _ = outcome_tx.send(Ok(TaskOutcome::QuorumFailed));
                        }
                        Err(recv_err) => {
                            let _ = outcome_tx.send(Err(BatchBuilderError::from(recv_err)));
                        }
                    }
                }
            }
        });

        outcome_rx
    }

    /// Returns the timestamp of the latest canonical block, or genesis before any block.
    fn latest_canon_timestamp(reth_env: &RethEnv) -> u64 {
        let num = reth_env.last_block_number().unwrap_or_default();
        if let Ok(Some(block)) = reth_env.sealed_block_by_number(num) {
            block.timestamp
        } else {
            reth_env.chainspec().sealed_genesis_block().timestamp
        }
    }
}
