//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for publishing to peers.

use crate::{
    batch_fetcher::BatchFetcher,
    metrics::{Metrics, WorkerMetrics},
    network::PrimaryReceiverHandler,
    quorum_waiter::{QuorumWaiter, QuorumWaiterTrait},
    WorkerNetworkHandle,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_network_types::{
    local::LocalNetwork, WorkerOwnBatchMessage, WorkerToPrimaryClient,
};
use rayls_infrastructure_storage::tables::{BatchSeqCounter, Batches, ConsensusBlocks};
use rayls_infrastructure_types::{
    batch_tracker::{BatchTracker, SealFailureReason},
    error::BlockSealError,
    AuthorityIdentifier, BatchReceiver, BatchSender, BatchValidation, Bytes, Database, Epoch,
    SealedBatch, SenderNonceRanges, TaskKind, TaskManager, WorkerId,
};
use std::{sync::Arc, time::Duration};
use tracing::{error, info, warn};

#[cfg(test)]
#[path = "tests/batch_sequence.rs"]
mod batch_sequence_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Spawn the worker.
///
/// Create an instance of `Self` and start all tasks to participate in consensus.
pub fn new_worker<DB: Database>(
    id: WorkerId,
    validator: Arc<dyn BatchValidation>,
    metrics: Metrics,
    consensus_config: ConsensusConfig<DB>,
    network_handle: WorkerNetworkHandle,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Boot worker node with id {} key {:?}", id, consensus_config.key_config().primary_public_key());

    let node_metrics = metrics.worker_metrics.clone();

    let batch_fetcher = BatchFetcher::new(
        network_handle.clone(),
        consensus_config.node_storage().clone(),
        node_metrics.clone(),
    );
    consensus_config.local_network().set_primary_to_worker_local_handler(Arc::new(
        PrimaryReceiverHandler {
            store: consensus_config.node_storage().clone(),
            request_batches_timeout: consensus_config.parameters().sync_retry_delay,
            network: Some(network_handle.clone()),
            batch_fetcher: Some(batch_fetcher),
            validator,
        },
    ));
    let batch_provider = new_worker_internal(
        id,
        &consensus_config,
        node_metrics,
        consensus_config.local_network().clone(),
        network_handle.clone(),
    );

    // NOTE: This log entry is used to compute performance.
    info!(target: "worker::worker",
        "Worker {} successfully booted on {}",
        id,
        consensus_config.config().node_info.p2p_info.worker.network_address
    );

    batch_provider
}

/// Builds a new batch provider responsible for handling client transactions.
fn new_worker_internal<DB: Database>(
    id: WorkerId,
    consensus_config: &ConsensusConfig<DB>,
    node_metrics: Arc<WorkerMetrics>,
    client: LocalNetwork,
    network_handle: WorkerNetworkHandle,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Starting handler for transactions");

    // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the batch
    // before forwarding the batch to the `Processor`
    // Only have a quorum waiter if we are an authority (validator).
    let quorum_waiter = consensus_config.authority().clone().map(|authority| {
        QuorumWaiter::new(
            authority,
            consensus_config.committee().clone(),
            network_handle.clone(),
            node_metrics.clone(),
        )
    });

    Worker::new(
        id,
        quorum_waiter,
        node_metrics,
        client,
        consensus_config.node_storage().clone(),
        consensus_config.parameters().batch_vote_timeout,
        network_handle,
    )
}

/// Return the highest seq `authority_id` authored on `worker_id` within
/// `current_epoch`. `Some(0)` vs `None` matters: `Some(0)` means we authored
/// seq=0 (next=1); `None` means nothing observed (next=0).
pub(crate) fn walk_consensus_blocks_for_max_seq<DB: Database>(
    store: &DB,
    worker_id: WorkerId,
    authority_id: AuthorityIdentifier,
    current_epoch: Epoch,
) -> Option<u64> {
    let mut max_seq: Option<u64> = None;
    for (_block_num, consensus_block) in store.reverse_iter::<ConsensusBlocks>() {
        if consensus_block.sub_dag.leader_epoch() < current_epoch {
            break;
        }
        let mut found_in_block = false;
        for cert in &consensus_block.sub_dag.certificates {
            if cert.header.author != authority_id {
                continue;
            }
            for (batch_digest, wid) in cert.header.payload() {
                if *wid != worker_id {
                    continue;
                }
                if let Ok(Some(batch)) = store.get::<Batches>(batch_digest) {
                    max_seq = Some(max_seq.map_or(batch.seq, |m| m.max(batch.seq)));
                    found_in_block = true;
                }
            }
        }
        if found_in_block {
            break;
        }
    }
    max_seq
}

/// Process batch from EL into sealed batches for CL.
pub struct Worker<DB, QW> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to batches.
    quorum_waiter: Option<QW>,
    /// Metrics handler.
    node_metrics: Arc<WorkerMetrics>,
    /// The network client to send our batches to the primary.
    client: LocalNetwork,
    /// The batch store to store our own batches.
    store: DB,
    /// Channel sender for alternate batch submission if not calling seal directly.
    tx_batches: BatchSender,
    /// Channel receiver for alternate batch submission if not calling seal directly.
    /// This will be "taken" on batch spawn and become None.
    rx_batches: Option<BatchReceiver>,
    /// The amount of time to wait on a reply from peer before timing out.
    timeout: Duration,
    /// Worker network handle.
    network_handle: WorkerNetworkHandle,
    /// Optional batch lifecycle tracker.
    batch_tracker: Option<Arc<BatchTracker>>,
}

// Manual impl: `rx_batches` is consumed once by `spawn_batch_builder`, so a clone starts without
// it; a clone that tries to spawn panics immediately on the `take`.
impl<DB: Clone, QW: Clone> Clone for Worker<DB, QW> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            quorum_waiter: self.quorum_waiter.clone(),
            node_metrics: self.node_metrics.clone(),
            client: self.client.clone(),
            store: self.store.clone(),
            tx_batches: self.tx_batches.clone(),
            rx_batches: None,
            timeout: self.timeout,
            network_handle: self.network_handle.clone(),
            batch_tracker: self.batch_tracker.clone(),
        }
    }
}

impl<DB, QW> std::fmt::Debug for Worker<DB, QW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> Worker<DB, QW> {
    #[allow(clippy::too_many_arguments)]
    /// Create an instance of `Self`.
    pub fn new(
        id: WorkerId,
        quorum_waiter: Option<QW>,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
        network_handle: WorkerNetworkHandle,
    ) -> Self {
        let (tx_batches, rx_batches) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        Self {
            id,
            quorum_waiter,
            node_metrics,
            client,
            store,
            tx_batches,
            rx_batches: Some(rx_batches),
            timeout,
            network_handle,
            batch_tracker: None,
        }
    }

    /// Spawns the task that seals batches arriving on the submit channel, keeping the engine
    /// decoupled from the worker.
    pub fn spawn_batch_builder(&mut self, prefix: &str, task_manager: &TaskManager) {
        let this_clone = self.clone();
        let mut rx_batches = self.rx_batches.take().expect("have batch receive");
        let rx_shutdown = task_manager.shutdown_subscriber();
        task_manager.spawn_classified_task(
            &format!("{prefix} batch-builder"),
            async move {
                loop {
                    tokio::select! {
                        // shutdown wins: a drained batch-builder must stop sealing promptly so
                        // the epoch transition is not held by another seal round trip
                        biased;

                        _ = &rx_shutdown => {
                            info!(target: "worker::batch_provider", "shutdown received, exiting batch-builder loop");
                            break;
                        }
                        batch = rx_batches.recv() => {
                            let Some((batch, sender_nonce_ranges, tx)) = batch else {
                                break;
                            };
                            let next_seq = batch.batch.seq + 1;
                            let res = this_clone.seal(batch, sender_nonce_ranges).await;
                            if res.is_ok() {
                                this_clone.persist_batch_seq_counter(next_seq);
                            }
                            if tx.send(res).is_err() {
                                error!(target: "worker::batch_provider", "Error sending result to channel caller!  Channel closed.");
                            }
                        }
                    }
                }
            },
            TaskKind::Drainable,
        );
    }

    /// Return worker's ID.
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// Return the network handle for this worker.
    pub fn network_handle(&self) -> WorkerNetworkHandle {
        self.network_handle.clone()
    }

    /// Set the batch lifecycle tracker.
    pub fn set_batch_tracker(&mut self, tracker: Arc<BatchTracker>) {
        self.batch_tracker = Some(tracker);
    }

    /// Return the persisted next-seq counter, or `None` if unset or the read
    /// errored. `None` triggers the history-walk fallback in the caller.
    pub fn get_persisted_batch_seq(&self) -> Option<u64> {
        match self.store.get::<BatchSeqCounter>(&self.id) {
            Ok(Some(seq)) => Some(seq),
            Ok(None) => None,
            Err(e) => {
                warn!(target: "worker::batch_provider",
                    "BatchSeqCounter read error, treating as missing: {e:?}");
                None
            }
        }
    }

    /// Recover the next seq by walking `ConsensusBlocks`. Call only when
    /// `get_persisted_batch_seq()` returned `None` and replay has completed.
    /// Returns 0 when `authority_id` is `None` (observer / non-committee).
    pub fn recover_batch_seq_from_history(
        &self,
        authority_id: Option<AuthorityIdentifier>,
        current_epoch: Epoch,
    ) -> u64 {
        let Some(authority_id) = authority_id else {
            return 0;
        };

        let walk_observed =
            walk_consensus_blocks_for_max_seq(&self.store, self.id, authority_id, current_epoch);
        // saturating_add guards against a u64::MAX observation
        let resolved = walk_observed.map(|s| s.saturating_add(1)).unwrap_or(0);
        info!(
            target: "worker::batch_provider",
            worker_id = self.id,
            walk_observed_max = ?walk_observed,
            resolved,
            "recovered batch sequence from ConsensusBlocks (counter was stale or missing)"
        );
        resolved
    }

    /// Persist the batch sequence counter for this worker to DB.
    fn persist_batch_seq_counter(&self, next_seq: u64) {
        if let Err(e) = self.store.insert::<BatchSeqCounter>(&self.id, &next_seq) {
            error!(target: "worker::batch_provider", "Failed to persist batch seq counter: {:?}", e);
        }
    }

    /// The sender end of the batch submit channel.
    pub fn batches_tx(&self) -> BatchSender {
        self.tx_batches.clone()
    }

    /// Send all the txns in sealed_batch to CVVs so they can be included in blocks.
    /// Use this when not a CVV so that transactions you accept can be included in a block.
    pub async fn disburse_txns(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let payloads = sealed_batch.batch.transactions.into_iter().map(Bytes::from).collect();
        if let Err(err) = self.network_handle.publish_txn(payloads).await {
            error!(target: "worker::batch_provider", "Error publishing transaction: {err}");
        }
        Ok(())
    }

    /// Seals and publishes the current batch.
    pub async fn seal(
        &self,
        sealed_batch: SealedBatch,
        sender_nonce_ranges: SenderNonceRanges,
    ) -> Result<(), BlockSealError> {
        let Some(quorum_waiter) = &self.quorum_waiter else {
            // We are not a validator so need to send any transactions out for a CVV to pickup.
            return self.disburse_txns(sealed_batch).await;
        };
        let size = sealed_batch.size();

        self.node_metrics
            .created_batch_size
            .with_label_values(&["latest batch size"])
            .observe(size as f64);

        let batch_attest_handle = quorum_waiter.verify_batch(
            sealed_batch.clone(),
            self.timeout,
            self.network_handle.get_task_spawner(),
        );

        let (batch, digest) = sealed_batch.split();
        if let Some(tracker) = &self.batch_tracker {
            tracker.batch_sealed(digest, batch.transactions.len(), &sender_nonce_ranges);
        }

        // Wait for our batch to reach quorum or fail to do so.
        match batch_attest_handle.await {
            Ok(res) => {
                match res {
                    Ok(()) => {
                        // batch reached quorum!
                        if let Some(tracker) = &self.batch_tracker {
                            tracker.batch_quorum_reached(digest);
                        }

                        // Now save it to permanent storage
                        if let Err(e) = self.store.insert::<Batches>(&digest, &batch) {
                            error!(target: "worker::batch_provider", "Store failed with error: {:?}", e);
                            return Err(BlockSealError::FatalDBFailure);
                        }

                        // Publish the digest for non-committee listeners. A publish error is
                        // ignored: it only signals a p2p problem that surfaces elsewhere quickly.
                        let _ = self.network_handle.publish_batch(digest).await;
                    }
                    Err(e) => {
                        if let Some(tracker) = &self.batch_tracker {
                            tracker.batch_seal_failed(digest, SealFailureReason::Quorum);
                        }
                        return Err(match e {
                            crate::quorum_waiter::QuorumWaiterError::QuorumRejected => {
                                BlockSealError::QuorumRejected
                            }
                            crate::quorum_waiter::QuorumWaiterError::AntiQuorum => {
                                BlockSealError::AntiQuorum
                            }
                            crate::quorum_waiter::QuorumWaiterError::Timeout => {
                                BlockSealError::Timeout
                            }
                            crate::quorum_waiter::QuorumWaiterError::Network
                            | crate::quorum_waiter::QuorumWaiterError::DroppedReceiver
                            | crate::quorum_waiter::QuorumWaiterError::Rpc(_) => {
                                BlockSealError::FailedQuorum
                            }
                        });
                    }
                }
            }
            Err(e) => {
                error!(target: "worker::batch_provider", "Join error attempting batch quorum! {e}");
                if let Some(tracker) = &self.batch_tracker {
                    tracker.batch_seal_failed(digest, SealFailureReason::QuorumJoin);
                }
                return Err(BlockSealError::FailedQuorum);
            }
        }

        // Report the batch to the primary. An unacknowledged report means the digest never
        // entered the proposer's queue (the epoch-scoped proposer stops draining at teardown), so
        // recording the batch as sealed would consume a seq no header ever carries: a permanent
        // per-authority hole whose committed successors park until the boundary drain. Surface it
        // as a seal failure instead, so the builder reuses the seq and the txs stay selectable.
        // Reported as FailedQuorum: the batch reached availability but cannot be placed.
        let message = WorkerOwnBatchMessage { worker_id: self.id, digest };
        if let Err(err) = self.client.report_own_batch(message).await {
            error!(target: "worker::batch_provider", "Failed to report our batch: {err:?}");
            if let Some(tracker) = &self.batch_tracker {
                tracker.batch_seal_failed(digest, SealFailureReason::ReportUnacknowledged);
            }
            return Err(BlockSealError::FailedQuorum);
        }
        if let Some(tracker) = &self.batch_tracker {
            tracker.batch_reported_to_primary(digest);
        }

        Ok(())
    }
}
