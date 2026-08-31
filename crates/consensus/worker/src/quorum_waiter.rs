//! Wait for a quorum of acks from workers before sharing with the primary.

use crate::{metrics::WorkerMetrics, network::WorkerNetworkHandle};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use rayls_consensus_network::error::NetworkError;
use rayls_infrastructure_types::{
    Authority, BlsPublicKey, Committee, SealedBatch, TaskSpawner, VotingPower,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::oneshot;

use tracing::debug;

/// Interface to the quorum waiter, indirected so tests can substitute it.
pub trait QuorumWaiterTrait: Send + Sync + Clone + Unpin + 'static {
    /// Sends a batch to committee peers and resolves once its quorum outcome is known.
    ///
    /// The receiver yields `Ok` once a quorum of stake has acked the batch, within `timeout`.
    /// Each peer can accept (adding its stake), reject explicitly (its stake is lost for good and
    /// may make quorum impossible), or fail transiently (its stake is lost for this attempt).
    /// [`QuorumWaiterError::QuorumRejected`] is permanent; the other errors may clear if the
    /// network recovers.
    fn verify_batch(
        &self,
        batch: SealedBatch,
        timeout: Duration,
        task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>>;
}

#[derive(Debug)]
struct QuorumWaiterInner {
    /// This authority.
    authority: Authority,
    /// The committee information.
    committee: Committee,
    /// A network sender to publish the batches to the other workers.
    network: WorkerNetworkHandle,
    /// Record metrics for quorum waiter.
    metrics: Arc<WorkerMetrics>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
#[derive(Clone, Debug)]
pub struct QuorumWaiter {
    inner: Arc<QuorumWaiterInner>,
}

impl QuorumWaiter {
    /// Create a new QuorumWaiter.
    pub fn new(
        authority: Authority,
        committee: Committee,
        network: WorkerNetworkHandle,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        Self { inner: Arc::new(QuorumWaiterInner { authority, committee, network, metrics }) }
    }

    /// Awaits one peer's ack and maps it to that peer's stake, or to an error carrying it.
    async fn waiter(
        bls: BlsPublicKey,
        wait_for: oneshot::Receiver<Result<(), NetworkError>>,
        deliver: VotingPower,
    ) -> Result<VotingPower, WaiterError> {
        match wait_for.await {
            Ok(r) => match r {
                Ok(_) => Ok(deliver),
                Err(NetworkError::RPCError(msg)) => {
                    tracing::error!(
                        target = "worker::quorum_waiter",
                        "RPCError, peer {bls}: {msg}"
                    );
                    Err(WaiterError::Rejected(deliver))
                }
                Err(err) => {
                    tracing::error!(
                        target = "worker::quorum_waiter",
                        "Network error, peer {bls}: {err:?}"
                    );
                    Err(WaiterError::Network(deliver))
                }
            },
            Err(_) => Err(WaiterError::Network(deliver)),
        }
    }
}

impl QuorumWaiterTrait for QuorumWaiter {
    fn verify_batch(
        &self,
        sealed_batch: SealedBatch,
        timeout: Duration,
        task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
        let inner = self.inner.clone();
        let task_name = format!("verifying-batch-{}", sealed_batch.digest());
        let (tx, rx) = oneshot::channel();
        let spawner_clone = task_spawner.clone();
        task_spawner.spawn_task(task_name, async move {
            let timeout_res = tokio::time::timeout(timeout, async move {
                let start_time = Instant::now();
                // Publish the batch to the other workers.
                let peers: Vec<_> =
                    inner.committee.others_keys_except(inner.authority.protocol_key());
                let handlers = inner.network.report_batch_to_peers(&peers, sealed_batch);
                let _timer = inner.metrics.batch_broadcast_quorum_latency.start_timer();

                // Collect all the handlers to receive acknowledgements.
                let mut wait_for_quorum = FuturesUnordered::new();
                // Total stake available for the entire committee.
                // Can use this to determine anti-quorum more quickly.
                let mut available_stake: u64 = 0;
                // Stake from a committee member that has rejected this batch.
                let mut rejected_stake: u64 = 0;
                // Each waiter is only an await on a peer ack, so it runs inline in the
                // FuturesUnordered; a task per peer would cost a task and a channel per peer per
                // batch for no concurrency gain.
                peers
                    .into_iter()
                    .zip(handlers.into_iter())
                    .map(|(name, handler)| {
                        let stake = inner.committee.voting_power(&name);
                        available_stake = available_stake.saturating_add(stake);
                        Self::waiter(name, handler, stake)
                    })
                    .for_each(|f| wait_for_quorum.push(f));

                // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
                // delivered and we send its digest to the primary (that will include it into
                // the dag). This should reduce the amount of syncing.
                let threshold = inner.committee.quorum_threshold();
                let mut total_stake = inner.authority.voting_power();
                // If more stake than this is rejected then the batch will never be accepted,
                // and account for this node's vote.
                let max_rejected_stake =
                    available_stake.saturating_add(total_stake).saturating_sub(threshold);

                debug!(
                    target: "quorum-waiter",
                    ?available_stake,
                    ?rejected_stake,
                    ?threshold,
                    ?total_stake,
                    ?max_rejected_stake,
                    "begin loop"
                );

                // Wait on the peer responses and produce an Ok(()) for quorum (2/3 stake confirmed
                // batch) or Error if quorum not reached.
                loop {
                    // Dev-only early exit: on a single-validator committee the node's own
                    // stake (seeded into total_stake above) already meets quorum_threshold()==1.
                    // Without this check the loop calls wait_for_quorum.next(), gets None
                    // (no peers), and wrongly returns AntiQuorum.
                    #[cfg(feature = "dev-single-node-setup")]
                    if total_stake >= threshold {
                        break Ok(());
                    }
                    if let Some(res) = wait_for_quorum.next().await {
                        match res {
                            Ok(stake) => {
                                total_stake = total_stake.saturating_add(stake);
                                available_stake = available_stake.saturating_sub(stake);
                                if total_stake >= threshold {
                                    let remaining_time =
                                        timeout.saturating_sub(start_time.elapsed());
                                    if !wait_for_quorum.is_empty() && !remaining_time.is_zero() {
                                        // Let the remaining waiters have a chance for the remaining
                                        // time.
                                        // These are fire and forget, they will timeout soon so no
                                        // big deal.
                                        spawner_clone.spawn_task("quorum-remainder", async move {
                                            let _ =
                                                tokio::time::timeout(remaining_time, async move {
                                                    while (wait_for_quorum.next().await).is_some() {
                                                        // do nothing
                                                    }
                                                })
                                                .await;
                                        });
                                    }
                                    break Ok(());
                                }
                            }
                            Err(WaiterError::Rejected(stake)) => {
                                rejected_stake = rejected_stake.saturating_add(stake);
                                available_stake = available_stake.saturating_sub(stake);
                            }
                            Err(WaiterError::Network(stake)) => {
                                available_stake = available_stake.saturating_sub(stake);
                            }
                        }
                    } else {
                        // Ran out of Peers and did not reach quorum...
                        break Err(QuorumWaiterError::AntiQuorum);
                    }

                    debug!(
                        target: "quorum-waiter",
                        ?total_stake,
                        ?available_stake,
                        ?threshold,
                        ?rejected_stake,
                        ?max_rejected_stake,
                        "begin loop"
                    );

                    // check if quorum is impossible
                    if rejected_stake > max_rejected_stake {
                        // Can no longer reach quorum because our batch was explicitly rejected by
                        // to much stake.
                        break Err(QuorumWaiterError::QuorumRejected);
                    }
                    if total_stake.saturating_add(available_stake) < threshold {
                        // It is no longer possible to reach quorum...
                        // This is likely because of network/rpc errors and may not be permanent.
                        break Err(QuorumWaiterError::AntiQuorum);
                    }
                }
            })
            .await;

            let res = match timeout_res {
                Ok(res) => match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(e),
                },
                Err(_elapsed) => Err(QuorumWaiterError::Timeout),
            };

            // forward result
            tx.send(res)
        });
        rx
    }
}

/// Reasons a batch failed to reach quorum.
#[derive(Clone, Debug, Error)]
pub enum QuorumWaiterError {
    /// Enough stake rejected the batch that quorum can never be reached.
    #[error("Block was rejected by enough peers to never reach quorum")]
    QuorumRejected,
    /// Too much stake is unreachable for quorum this attempt; may clear if the network recovers.
    #[error("Anti quorum reached for batch (note this may not be permanent)")]
    AntiQuorum,
    /// The quorum deadline elapsed.
    #[error("Timed out waiting for quorum")]
    Timeout,
    /// A network failure prevented the attempt.
    #[error("Network Error")]
    Network,
    /// A peer returned an RPC status error.
    #[error("RPC Status Error {0}")]
    Rpc(String),
    /// The waiter task dropped its result channel.
    #[error("Oneshot receiver dropped.")]
    DroppedReceiver,
}

impl From<oneshot::error::RecvError> for QuorumWaiterError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::DroppedReceiver
    }
}

/// Per-peer failure, carrying the stake that outcome removes from the running tally.
#[derive(Clone, Debug, Error)]
enum WaiterError {
    #[error("Block was rejected by peer")]
    Rejected(VotingPower),
    #[error("Network Error")]
    Network(VotingPower),
}
