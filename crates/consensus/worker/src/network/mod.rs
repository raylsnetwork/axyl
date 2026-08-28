//! Worker network implementation.

use crate::batch_fetcher::BatchFetcher;
use error::WorkerNetworkError;
use futures::{stream::FuturesUnordered, StreamExt};
use handler::RequestHandler;
use message::{WorkerGossip, WorkerRPCError};
pub use message::{WorkerRequest, WorkerResponse};
use rayls_consensus_network::{
    error::NetworkError,
    types::{NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, Penalty, ResponseChannel,
};
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig, GOSSIP_TOPIC_TXN};
use rayls_infrastructure_network_types::{
    FetchBatchResponse, PrimaryToWorkerClient, WorkerSynchronizeMessage,
};
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{
    encode, now, B256Set, Batch, BatchValidation, BlockHash, BlsPublicKey, Bytes, Database,
    DbTxMut, NodeMode, RaylsReceiver, SealedBatch, TaskKind, TaskSpawner, TxHash, WorkerId,
};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, trace, warn};

pub(crate) mod error;
pub(crate) mod handler;
pub(crate) mod message;

/// Request type of the worker network.
pub(crate) type Req = WorkerRequest;
/// Response type of the worker network.
pub(crate) type Res = WorkerResponse;

/// Soft cap on this handle's concurrently in-flight outbound batch requests.
///
/// Each holds one substream against the per-connection request-response limit of 100; staying well
/// under it leaves headroom for inbound, `report_batch`, and the primary's reqres on the same
/// connections. A frequency reducer, not a hard bound (see `outbound_failure_penalty`).
const MAX_CONCURRENT_BATCH_REQUESTS: usize = 32;

/// Cap on concurrently admitted inbound transaction submits.
///
/// Each submit recovers signers on the shared rayon pool that batch validation also uses, so an
/// unbounded burst would starve consensus; excess submits are shed back to the sender, which
/// retries the next validator.
const MAX_CONCURRENT_SUBMIT_TXNS: usize = 16;

/// Encoded size of a [`WorkerRequest::SubmitTxns`] carrying nothing: the envelope every direct
/// submit pays on top of its transaction bytes.
static TXN_SUBMIT_OVERHEAD: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| encode(&WorkerRequest::SubmitTxns { transactions: vec![] }).len());

/// Why an inbound transaction submit was refused before any admission work was spawned.
///
/// The reply reuses the existing error variant (no wire change) carrying the `Display` string,
/// which a sender parses back with [`Self::parse`]: the strings are a wire contract and stay stable
/// across upgrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitRejection {
    /// This node cannot seal batches, so pooling forwarded transactions would strand them.
    #[error("not batch-producing")]
    NotBatchProducing,
    /// The inbound submit fan-out is at capacity.
    #[error("submit backlog")]
    Backlog,
}

impl SubmitRejection {
    /// Every rejection a receiver can reply with.
    const ALL: [Self; 2] = [Self::NotBatchProducing, Self::Backlog];

    /// Parse a receiver's error reply back into the rejection it displayed; any other reply is
    /// not a rejection.
    fn parse(reply: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|rejection| rejection.to_string() == reply)
    }
}

/// Why a direct submit to a validator failed.
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// The receiver's gate refused the submit before admission: its explicit self-report.
    #[error(transparent)]
    Rejected(#[from] SubmitRejection),
    /// The request produced no reply this node can act on: transport failure, timeout, or an
    /// error reply that is not a gate rejection.
    #[error(transparent)]
    Network(#[from] NetworkError),
}

/// Encoded size of a [`WorkerGossip::Txn`] carrying nothing: the frame every published payload
/// pays on top of its transaction bytes.
static TXN_GOSSIP_OVERHEAD: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| encode(&WorkerGossip::Txn(vec![])).len());

/// The wrapper around worker-specific network calls.
#[derive(Clone, Debug)]
pub struct WorkerNetworkHandle {
    /// The handle to the node's network.
    handle: NetworkHandle<Req, Res>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The max rpc message size (in bytes).
    max_rpc_message_size: usize,
    /// Bounds concurrent outbound batch-request substreams under the request-response stream cap.
    batch_request_permits: Arc<Semaphore>,
}

impl WorkerNetworkHandle {
    /// Create a new instance of [Self].
    pub fn new(
        handle: NetworkHandle<Req, Res>,
        task_spawner: TaskSpawner,
        max_rpc_message_size: usize,
    ) -> Self {
        Self {
            handle,
            task_spawner,
            max_rpc_message_size,
            batch_request_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_REQUESTS)),
        }
    }

    /// Return a reference to the task spawner.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.task_spawner
    }

    /// Create a handle for tests whose commands go nowhere.
    pub fn new_for_test(task_spawner: TaskSpawner) -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(5);
        Self {
            handle: NetworkHandle::new(tx),
            task_spawner,
            max_rpc_message_size: 1024 * 1024,
            batch_request_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_REQUESTS)),
        }
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<Req, Res> {
        &self.handle
    }

    /// Forward transactions directly to `peer_bls`, returning the hashes it rejected as stale.
    pub async fn submit_txns(
        &self,
        peer_bls: BlsPublicKey,
        transactions: Vec<Bytes>,
        timeout: Duration,
    ) -> Result<Vec<TxHash>, SubmitError> {
        let request = WorkerRequest::SubmitTxns { transactions };
        match self.request_within(request, peer_bls, timeout).await? {
            WorkerResponse::SubmitTxns { stale } => Ok(stale),
            WorkerResponse::ReportBatch
            | WorkerResponse::RequestBatches { .. }
            | WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a submit txns response!".to_string(),
            )
            .into()),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(match SubmitRejection::parse(&s) {
                Some(rejection) => rejection.into(),
                None => NetworkError::RPCError(s).into(),
            }),
        }
    }

    /// Send `request` to `peer_bls` and await its reply within `timeout`.
    async fn request_within(
        &self,
        request: WorkerRequest,
        peer_bls: BlsPublicKey,
        timeout: Duration,
    ) -> NetworkResult<WorkerResponse> {
        let res = self.handle.send_request(request, peer_bls).await?;
        tokio::time::timeout(timeout, res).await.map_err(|_| NetworkError::Timeout)??
    }

    /// The most transaction bytes one direct submit may carry, leaving room for the request
    /// envelope under the RPC message-size cap.
    pub fn direct_txn_payload_budget(&self) -> usize {
        self.max_rpc_message_size.saturating_sub(*TXN_SUBMIT_OVERHEAD)
    }

    /// Transaction bytes that fit in one [`Self::publish_txn`] under `max_gossip_message_size`.
    ///
    /// Peers reject an over-budget message outright, so a publisher subtracts the encoding frame
    /// itself rather than discover the cap on the receiving side.
    pub fn txn_payload_budget(max_gossip_message_size: usize) -> usize {
        max_gossip_message_size.saturating_sub(*TXN_GOSSIP_OVERHEAD)
    }

    /// Publish a batch digest to the worker network.
    pub(crate) async fn publish_batch(&self, batch_digest: BlockHash) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Batch(batch_digest));
        self.handle.publish(LibP2pConfig::worker_batch_topic(), data).await?;
        Ok(())
    }

    /// Publish encoded transactions on the worker transaction topic so a committee member can
    /// include them in a batch; the pre-fork path for a node that cannot seal batches.
    pub async fn publish_txn(&self, txn: Vec<Bytes>) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Txn(txn));
        self.handle.publish(GOSSIP_TOPIC_TXN.into(), data).await?;
        Ok(())
    }

    /// Report a new batch to a peer.
    async fn report_batch(
        &self,
        peer_bls: BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> NetworkResult<()> {
        // TODO- issue 237- should we sign these batches and check the sig before accepting any
        // batches during consensus?
        let request = WorkerRequest::ReportBatch { sealed_batch };
        let res = self.handle.send_request(request, peer_bls).await?;
        let res = res.await??;
        match res {
            WorkerResponse::ReportBatch => Ok(()),
            WorkerResponse::RequestBatches { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is request batches!".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is peer exchange!".to_string(),
            )),
            WorkerResponse::SubmitTxns { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is submit txns!".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Report a new batch to peers.
    pub(crate) fn report_batch_to_peers(
        &self,
        peers: &[BlsPublicKey],
        sealed_batch: SealedBatch,
    ) -> Vec<oneshot::Receiver<NetworkResult<()>>> {
        let mut result = vec![];
        for peer in peers {
            let handle = self.clone();
            let batch = sealed_batch.clone();
            let task_name = format!("ReportBatchToPeer-{peer}");
            let (tx, rx) = oneshot::channel();
            let peer = *peer;
            self.task_spawner.spawn_task(task_name, async move {
                let res = handle.report_batch(peer, batch).await;
                // ignore error bc quorum waiter will move on once quorum is reached
                let _ = tx.send(res);
            });

            result.push(rx);
        }
        result
    }

    /// Request a group of batches by hashes.
    async fn request_batches_from_peer(
        &self,
        peer: BlsPublicKey,
        batch_digests: Vec<BlockHash>,
        timeout: Duration,
    ) -> NetworkResult<Vec<Batch>> {
        // Hold a permit for the whole in-flight window (send + await response) so total outbound
        // substreams stay under the request-response stream cap. The semaphore is never closed, so
        // acquire cannot fail.
        let _permit = self
            .batch_request_permits
            .acquire()
            .await
            .expect("batch request semaphore never closed");
        let request = WorkerRequest::RequestBatches {
            batch_digests: batch_digests.clone(),
            max_response_size: self.max_rpc_message_size,
        };
        let res = self.handle.send_request(request, peer).await?;
        let res =
            tokio::time::timeout(timeout, res).await.map_err(|_| NetworkError::Timeout)???;
        match res {
            WorkerResponse::ReportBatch => Err(NetworkError::RPCError(
                "Got wrong response, not a request batches is report batch!".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a request batches is peer exchange!".to_string(),
            )),
            WorkerResponse::RequestBatches(batches) => {
                for batch in &batches {
                    let batch_digest = batch.digest();
                    if !batch_digests.contains(&batch_digest) {
                        let msg = format!(
                            "Peer {peer} returned batch with digest \
                            {batch_digest} which is not part of the requested digests: {batch_digests:?}"
                        );
                        return Err(NetworkError::ProtocolError(msg));
                    }
                }
                Ok(batches)
            }
            WorkerResponse::SubmitTxns { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a request batches is submit txns!".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Request a group of batches by hashes.
    /// Sends request to all our connected peers at once and returns Ok when we
    /// get a valid response or Err if no one responds with the batches.
    pub(crate) async fn request_batches(
        &self,
        requested_digests: Vec<BlockHash>,
    ) -> NetworkResult<Vec<Batch>> {
        let mut peers = self.handle.connected_peers().await?;
        if requested_digests.is_empty() || peers.is_empty() {
            // nothing requested or no one to ask
            return Ok(vec![]);
        }
        let mut remaining_digests = requested_digests.clone();
        let num_peers = peers.len();
        let mut all_batches = Vec::new();
        // Spread digests across peers and rotate the assignment each round so load is shared and
        // a digest one peer lacks is asked of another. The rotation is approximate: a digest may
        // land on the same peer twice.
        for _ in 0..num_peers {
            let mut batch_of_batches = Vec::with_capacity(num_peers);
            (0..num_peers).for_each(|_| batch_of_batches.push(vec![]));
            peers.rotate_left(1); // Change which peers we ask for which batches.
            for (i, batch) in remaining_digests.iter().enumerate() {
                batch_of_batches
                    .get_mut(i % num_peers)
                    .expect("missing index we just created!")
                    .push(*batch);
            }
            let mut futures = FuturesUnordered::new();
            for (peer, batch_digests) in peers.iter().zip(batch_of_batches.into_iter()) {
                if !batch_digests.is_empty() {
                    futures.push(self.request_batches_from_peer(
                        *peer,
                        batch_digests,
                        Duration::from_secs(6),
                    ));
                }
            }
            while let Some(res) = futures.next().await {
                match res {
                    Ok(batches) => {
                        for batch in batches {
                            let batch_digest = batch.digest();
                            if requested_digests.contains(&batch_digest) {
                                if !all_batches.contains(&batch) {
                                    remaining_digests.retain(|d| *d != batch_digest);
                                    all_batches.push(batch);
                                }
                            } else {
                                warn!(target: "worker::network", "recieved a batch not requested {batch_digest}");
                            }
                        }
                        if remaining_digests.is_empty() {
                            return Ok(all_batches);
                        }
                    }
                    Err(e) => {
                        // Another worker might succeed so just log this.
                        warn!(target: "worker::network", ?e, "error requesting batches");
                    }
                }
            }
        }
        if all_batches.is_empty() {
            Err(NetworkError::RPCError("Unable to get batches from any peers!".to_string()))
        } else {
            Ok(all_batches)
        }
    }

    /// Report penalty to peer manager.
    async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }

    /// Update the task spawner at the epoch boundary.
    pub fn update_task_spawner(&mut self, task_spawner: TaskSpawner) {
        self.task_spawner = task_spawner
    }
}

/// Event loop that handles inbound worker requests and gossip for one epoch.
#[derive(Debug)]
pub struct WorkerNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: WorkerNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// Bounds concurrent inbound submit fan-out onto the shared rayon pool.
    submit_permits: Arc<Semaphore>,
    /// The node's mode, gating inbound transaction submits to batch-producing nodes; app-scoped on
    /// the consensus bus, so the watch survives epoch transitions.
    node_mode: watch::Receiver<NodeMode>,
}

impl<DB, Events> WorkerNetwork<DB, Events>
where
    DB: Database,
    Events: RaylsReceiver<NetworkEvent<Req, Res>> + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: Events,
        network_handle: WorkerNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        node_mode: watch::Receiver<NodeMode>,
    ) -> Self {
        let request_handler =
            RequestHandler::new(id, validator, consensus_config, network_handle.clone());
        let submit_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_SUBMIT_TXNS));
        Self { network_events, network_handle, request_handler, submit_permits, node_mode }
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_classified_task(
            "worker network events",
            async move {
                while let Some(event) = self.network_events.recv().await {
                    self.process_network_event(event);
                }
            },
            TaskKind::Cancel,
        );
    }

    /// Dispatch one network event to a task so the event loop never blocks on handling.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::RequestBatches { batch_digests, max_response_size } => {
                    self.process_request_batches(
                        peer,
                        batch_digests,
                        max_response_size,
                        channel,
                        cancel,
                    );
                }
                WorkerRequest::PeerExchange { .. } => {
                    // expect this is intercepted by network layer
                    warn!(target: "worker::network", "worker application received unexpected peer exchange message");
                }
                WorkerRequest::SubmitTxns { transactions } => {
                    self.process_submit_txns(transactions, channel, cancel);
                }
            },
            NetworkEvent::Gossip(msg, gossip_source) => {
                self.process_gossip(msg, gossip_source);
            }
            NetworkEvent::Error(msg, channel) => {
                let err = WorkerResponse::Error(message::WorkerRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.network_handle.get_task_spawner().spawn_task(
                    "report request error",
                    async move {
                        let _ = network_handle.handle.send_response(err, channel).await;
                    },
                );
            }
        }
    }

    /// Process directly-submitted transactions from a forwarding peer.
    ///
    /// Spawns a task that admits them to the pool and replies with the stale hashes, so the sender
    /// can prune what has already executed instead of re-forwarding it. Any connected peer may
    /// submit (observers are not committee members, so there is no membership gate to apply) and
    /// pool validation still rejects anything invalid; signer recovery hops to the blocking pool
    /// inside `submit_forwarded_txns`, so a flood cannot stall the network task.
    fn process_submit_txns(
        &self,
        transactions: Vec<Bytes>,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // Refuse the submit before spawning any admission work: replying with the existing error
        // variant (no wire change) makes the sender retry the next live validator immediately
        // instead of queueing behind a rayon backlog that would also delay consensus validation.
        let admitted = admit_submit(&self.node_mode, &self.submit_permits);
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        self.network_handle.get_task_spawner().spawn_task("process-submit-txns", async move {
            let response = match admitted {
                Err(rejection) => WorkerResponse::Error(WorkerRPCError(rejection.to_string())),
                // hold the permit across the rayon fan-out so the cap bounds concurrent CPU work
                Ok(_permit) => tokio::select! {
                    stale = request_handler.submit_forwarded_txns(transactions) => {
                        WorkerResponse::SubmitTxns { stale }
                    }
                    // cancel notification from the network layer
                    _ = cancel => return,
                },
            };
            let _ = network_handle.handle.send_response(response, channel).await;
        });
    }

    /// Process a new reported batch.
    ///
    /// Spawn a task to validate the peer's batch and return a response.
    fn process_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-report-batch-{}", sealed_batch.digest());
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_report_batch(&peer, sealed_batch) => {
                    let response = match res {
                        Ok(()) => WorkerResponse::ReportBatch,
                        Err(err) => {
                            let error = err.to_string();
                            if let Some(penalty) = err.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                            WorkerResponse::Error(message::WorkerRPCError(error))
                        }
                    };
                    let _ = network_handle.handle.send_response(response, channel).await;
                },
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to return requested batches.
    fn process_request_batches(
        &self,
        peer: BlsPublicKey,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-request-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_request_batches(batch_digests, max_response_size) => {
                    let response = match res {
                        Ok(r) => WorkerResponse::RequestBatches(r),
                        Err(err) => {
                            let error = err.to_string();
                            if let Some(penalty) = err.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                             WorkerResponse::Error(message::WorkerRPCError(error))
                        }
                    };

                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from a worker.
    fn process_gossip(&self, msg: GossipMessage, gossip_source: BlsPublicKey) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-gossip-{gossip_source}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                // convert error into penalty to lower peer score
                if let Some(penalty) = e.into() {
                    network_handle.report_penalty(gossip_source, penalty).await;
                }
            }
        });
    }
}

/// Admit one inbound transaction submit, holding a fan-out permit, or name the refusal.
///
/// The mode is checked first so a node that cannot seal sheds without consuming admission capacity.
fn admit_submit(
    node_mode: &watch::Receiver<NodeMode>,
    permits: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, SubmitRejection> {
    if !node_mode.borrow().is_batch_producing() {
        return Err(SubmitRejection::NotBatchProducing);
    }
    permits.clone().try_acquire_owned().map_err(|_| SubmitRejection::Backlog)
}

/// Handler for the local primary's requests to this worker.
#[derive(Debug)]
pub(super) struct PrimaryReceiverHandler<DB> {
    /// The batch store.
    pub store: DB,
    /// Timeout on RequestBatches RPC.
    pub request_batches_timeout: Duration,
    /// Synchronize header payloads from other workers.
    pub network: Option<WorkerNetworkHandle>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher<DB>>,
    /// Validates incoming batches.
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait::async_trait]
impl<DB: Database> PrimaryToWorkerClient for PrimaryReceiverHandler<DB> {
    async fn synchronize(&self, message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        let Some(network) = self.network.as_ref() else {
            return Err(eyre::eyre!(
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.contains_key::<Batches>(digest) {
                Ok(false) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(true) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(eyre::eyre!("failed to read from batch store: {e:?}"));
                }
            };
        }
        if missing.is_empty() {
            return Ok(());
        }

        let response = tokio::time::timeout(
            self.request_batches_timeout,
            network.request_batches(missing.iter().cloned().collect()),
        )
        .await??;

        let sealed_batches_from_response: Vec<SealedBatch> =
            response.into_iter().map(|b| b.seal_slow()).collect();

        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(&sealed_batch).await {
                    return Err(eyre::eyre!("Invalid batch: {err}"));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());
                self.store
                    .with_write_txn(|tx| {
                        tx.insert::<Batches>(&digest, &batch)?;
                        Ok(())
                    })
                    .map_err(|e| {
                        WorkerNetworkError::Internal(format!("failed to commit batch: {e:?}"))
                    })?;
            } else {
                return Err(eyre::eyre!(format!(
                    "failed to synchronize batches- received a batch {digest} we did not request!"
                )));
            }
        }

        if missing.is_empty() {
            return Ok(());
        }
        Err(eyre::eyre!("failed to synchronize batches!".to_string()))
    }

    async fn fetch_batches(&self, digests: B256Set) -> eyre::Result<FetchBatchResponse> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(eyre::eyre!(
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let batches = batch_fetcher.fetch(digests).await;
        Ok(FetchBatchResponse { batches })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rayls_consensus_network::types::NetworkCommand;
    use rayls_infrastructure_types::{BlsPublicKey, TaskManager, B256};
    use tokio::sync::mpsc::error::TryRecvError;

    /// The gate refuses a non-batch-producing node before capacity without consuming a permit,
    /// sheds at capacity, and follows the live watch across demotion and re-promotion.
    #[test]
    fn admit_submit_gates_on_mode_then_capacity() {
        let permits = Arc::new(Semaphore::new(1));
        let (tx, node_mode) = watch::channel(NodeMode::CvvActive);
        let admit = || admit_submit(&node_mode, &permits);

        for mode in [NodeMode::CvvInactive, NodeMode::Observer] {
            tx.send_replace(mode);
            assert_eq!(admit().err(), Some(SubmitRejection::NotBatchProducing));
            assert_eq!(permits.available_permits(), 1, "a mode reject must not consume a permit");
        }
        tx.send_replace(NodeMode::CvvActive);
        let permit = admit().expect("re-promotion admits again");
        assert_eq!(admit().err(), Some(SubmitRejection::Backlog), "at capacity: shed");
        drop(permit);
        assert!(admit().is_ok());
    }

    /// The reply strings are a wire contract: a sender parses back exactly what a receiver
    /// displayed, the two stay distinct, and anything else is not a rejection.
    #[test]
    fn submit_rejection_replies_round_trip_and_stay_distinct() {
        for rejection in SubmitRejection::ALL {
            assert_eq!(SubmitRejection::parse(&rejection.to_string()), Some(rejection));
        }
        assert_eq!(SubmitRejection::NotBatchProducing.to_string(), "not batch-producing");
        assert_eq!(SubmitRejection::Backlog.to_string(), "submit backlog");
        assert!(SubmitRejection::parse("boom").is_none());
    }

    /// A receiver's gate reply comes back as the typed rejection it displayed; any other error
    /// reply stays a network error, so the caller matches an enum instead of parsing strings.
    #[tokio::test]
    async fn submit_txns_types_the_gate_replies() {
        let (tx, mut commands) = tokio::sync::mpsc::channel::<NetworkCommand<Req, Res>>(4);
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new(
            NetworkHandle::new(tx),
            task_manager.get_spawner(),
            1024 * 1024,
        );
        let mut replies = ["not batch-producing", "submit backlog", "boom"].into_iter();
        tokio::spawn(async move {
            while let Some(NetworkCommand::SendRequest { reply, .. }) = commands.recv().await {
                let text = replies.next().expect("one reply per submit").to_string();
                let _ = reply.send(Ok(WorkerResponse::Error(WorkerRPCError(text))));
            }
        });

        let peer = BlsPublicKey::default();
        let submit = || handle.submit_txns(peer, Vec::new(), Duration::from_secs(1));
        assert_matches!(
            submit().await,
            Err(SubmitError::Rejected(SubmitRejection::NotBatchProducing))
        );
        assert_matches!(submit().await, Err(SubmitError::Rejected(SubmitRejection::Backlog)));
        assert_matches!(submit().await, Err(SubmitError::Network(NetworkError::RPCError(s))) if s == "boom");
    }

    /// Asserts concurrently in-flight outbound batch requests never exceed
    /// [`MAX_CONCURRENT_BATCH_REQUESTS`]. The mock buffers every pending request before answering
    /// any, so the buffered count is the true peak overlap.
    #[tokio::test]
    async fn request_batches_caps_concurrent_outbound_streams() {
        // Choose peers/digests well above the cap so an unbounded fan-out is observable.
        let num_peers = MAX_CONCURRENT_BATCH_REQUESTS * 2;
        let (tx, mut network_commands_rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<Req, Res>>(num_peers * 4);

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new(
            NetworkHandle::new(tx),
            task_manager.get_spawner(),
            1024 * 1024,
        );

        // Answer ConnectedPeers, then collect all queued SendRequests before replying to any: the
        // number held at once is exactly the live outbound overlap.
        let mock = tokio::spawn(async move {
            let mut peak = 0usize;
            while let Some(cmd) = network_commands_rx.recv().await {
                match cmd {
                    NetworkCommand::ConnectedPeers { reply } => {
                        let _ = reply.send(vec![BlsPublicKey::default(); num_peers]);
                    }
                    NetworkCommand::SendRequest { reply, .. } => {
                        // Drain the rest of the queued burst without replying so they overlap.
                        let mut pending = vec![reply];
                        loop {
                            match network_commands_rx.try_recv() {
                                Ok(NetworkCommand::SendRequest { reply, .. }) => {
                                    pending.push(reply)
                                }
                                Ok(_) => {}
                                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                            }
                        }
                        peak = peak.max(pending.len());
                        // Empty response is valid: peer simply does not have the batches.
                        for reply in pending {
                            let _ = reply.send(Ok(WorkerResponse::RequestBatches(vec![])));
                        }
                    }
                    _ => {}
                }
            }
            peak
        });

        // Request more distinct digests than the cap so the first round alone would open
        // `num_peers` substreams without the permit gate.
        let digests: Vec<B256> = (0..num_peers).map(|_| B256::random()).collect();
        let _ = handle.request_batches(digests).await;

        // Close the command channel so the mock loop terminates and returns its observed peak.
        drop(handle);
        let peak = mock.await.expect("mock task");
        assert!(
            peak <= MAX_CONCURRENT_BATCH_REQUESTS,
            "peak concurrent outbound batch requests {peak} exceeded cap {MAX_CONCURRENT_BATCH_REQUESTS}"
        );
    }
}
