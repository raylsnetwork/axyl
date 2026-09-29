//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use crate::{
    error::{CertManagerError, PrimaryNetworkError},
    proposer::OurDigestMessage,
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
// Re-exported so downstream crates (state-sync) can build a `PrimaryNetworkHandle` test mock that
// answers `NetworkCommand`s without taking a direct dependency on the network crate.
use message::{PrimaryGossip, PrimaryRPCError};
#[doc(hidden)]
pub use rayls_consensus_network::{error::NetworkError, types::NetworkCommand};
use rayls_consensus_network::{
    types::{IntoResponse as _, NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, Penalty, ResponseChannel,
};
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_network_types::{
    WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient,
};
use rayls_infrastructure_storage::PayloadStore;
use rayls_infrastructure_types::{
    encode, AuthorityIdentifier, BlockHash, BlsPublicKey, BlsSignature, Certificate,
    CertificateDigest, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord, EpochVote,
    Header, RaylsReceiver, RaylsSender, Round, TaskKind, TaskSpawner, Vote,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
pub mod handler;
mod message;
use crate::network::state::AuthVoteState;
pub use message::ConsensusResult;

pub mod state;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
pub(crate) type Req = PrimaryRequest;
/// Convenience type for Primary network.
pub(crate) type Res = PrimaryResponse;

/// Per-authority vote state for equivocation detection and caching.
pub(crate) type AuthEquivocationMap = BTreeMap<AuthorityIdentifier, AuthVoteState>;

/// Primary network specific handle.
#[derive(Clone, Debug)]
pub struct PrimaryNetworkHandle {
    handle: NetworkHandle<Req, Res>,
}

impl From<NetworkHandle<Req, Res>> for PrimaryNetworkHandle {
    fn from(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle }
    }
}

impl PrimaryNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle }
    }

    //// Convenience method for creating a new Self for tests.
    pub fn new_for_test(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self { handle: NetworkHandle::new(sender) }
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<PrimaryRequest, PrimaryResponse> {
        &self.handle
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(Box::new(certificate)));
        self.handle.publish(LibP2pConfig::primary_topic(), data).await?;
        Ok(())
    }

    /// Publish a consensus block number and hash of the header.
    pub async fn publish_consensus(
        &self,
        epoch: Epoch,
        round: Round,
        consensus_block_num: u64,
        consensus_header_hash: BlockHash,
        key: BlsPublicKey,
        signature: BlsSignature,
    ) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Consensus(Box::new(ConsensusResult {
            epoch,
            round,
            number: consensus_block_num,
            hash: consensus_header_hash,
            validator: key,
            signature,
        })));
        self.handle.publish(LibP2pConfig::consensus_output_topic(), data).await?;
        Ok(())
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_epoch_vote(&self, vote: EpochVote) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
        self.handle.publish(LibP2pConfig::epoch_vote_topic(), data).await?;
        Ok(())
    }

    /// Request a vote for header from the peer.
    /// Can return a response of Vote or MissingParents, other responses will be an error.
    pub async fn request_vote(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
    ) -> NetworkResult<RequestVoteResult> {
        let header = Arc::new(header);
        let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
        let res = self.handle.send_request(request, peer).await?;
        let mut res = res.await??;
        let mut tries = 0;
        while let PrimaryResponse::RecoverableError(PrimaryRPCError(s)) = res {
            warn!(target: "primary::network", "Got recoverable error {s}, retrying");
            tokio::time::sleep(Duration::from_millis(250)).await;
            let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
            let res_raw = self.handle.send_request(request, peer).await?;
            res = res_raw.await??;
            tries += 1;
            if tries > 5 {
                break;
            }
        }
        match res {
            PrimaryResponse::Vote(vote) => Ok(RequestVoteResult::Vote(vote)),
            PrimaryResponse::TooOld { header_round, limit_round } => {
                Ok(RequestVoteResult::TooOld { header_round, limit_round })
            }
            PrimaryResponse::EpochMismatch { expected, received } => {
                Ok(RequestVoteResult::EpochMismatch { expected, received })
            }
            PrimaryResponse::RecoverableError(PrimaryRPCError(s))
            | PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            PrimaryResponse::RequestedCertificates(_vec) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is requested certificates!".to_string(),
            )),
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::ConsensusHeader(_consensus_header) => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is consensus header!".to_string(),
            )),
            PrimaryResponse::EpochRecord { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is epoch record!".to_string(),
            )),
            PrimaryResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is peer exchange!".to_string(),
            )),
        }
    }

    pub async fn fetch_certificates(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        let request = PrimaryRequest::MissingCertificates { inner: request };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::RequestedCertificates(certs) => Ok(certs),
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError("Got wrong response, not a certificate!".to_string())),
        }
    }

    /// Request consensus header from specific peer.
    /// Will verify the returned header matches hash if provided (strong) or number if not (weak).
    pub async fn request_consensus_from_peer(
        &self,
        peer: BlsPublicKey,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        let res = self.handle.send_request(request, peer).await?;
        let res = res.await??;
        match res {
            PrimaryResponse::ConsensusHeader(header) => match (hash, number) {
                (Some(hash), _) => {
                    if header.digest() == hash {
                        Ok(Arc::unwrap_or_clone(header))
                    } else {
                        Err(NetworkError::RPCError(
                            "Got wrong response, header does not match hash!".to_string(),
                        ))
                    }
                }
                (_, Some(number)) => {
                    if header.number == number {
                        Ok(Arc::unwrap_or_clone(header))
                    } else {
                        Err(NetworkError::RPCError(
                            "Got wrong response, number does not match header!".to_string(),
                        ))
                    }
                }
                _ => Ok(Arc::unwrap_or_clone(header)),
            },
            PrimaryResponse::Error(PrimaryRPCError(s)) => Err(NetworkError::RPCError(s)),
            _ => Err(NetworkError::RPCError(
                "Got wrong response, not a consensus header!".to_string(),
            )),
        }
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    /// Will verify the returned header matches hash if provided (strong) or number if not (weak).
    pub async fn request_consensus(
        &self,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<ConsensusHeader> {
        let request = PrimaryRequest::ConsensusHeader { number, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            let res = res.await?;
            if let Ok(PrimaryResponse::ConsensusHeader(header)) = res {
                match (hash, number) {
                    (Some(hash), _) => {
                        if header.digest() == hash {
                            return Ok(Arc::unwrap_or_clone(header));
                        }
                    }
                    (_, Some(number)) => {
                        if header.number == number {
                            return Ok(Arc::unwrap_or_clone(header));
                        }
                    }
                    _ => {
                        return Err(NetworkError::RPCError(
                            "Must provide hash or number!".to_string(),
                        ));
                    }
                }
            }
        }
        Err(NetworkError::RPCError("Could not get the consensus header!".to_string()))
    }

    /// Request the certified epoch record for `epoch` from one specific peer.
    ///
    /// Unlike [`Self::request_epoch_cert`] this names the authority, so the answer is that
    /// validator's own view: what an attestation across a committee needs, where any peer
    /// answering would let one node vote several times.
    pub async fn request_epoch_cert_from_peer(
        &self,
        peer: BlsPublicKey,
        epoch: Epoch,
    ) -> NetworkResult<(EpochRecord, EpochCertificate)> {
        let request = PrimaryRequest::EpochRecord { epoch: Some(epoch), hash: None };
        let res = self.handle.send_request(request, peer).await?;
        match res.await?? {
            PrimaryResponse::EpochRecord { record, certificate } => Ok((record, certificate)),
            _ => Err(NetworkError::RPCError("Peer did not return an epoch record!".to_string())),
        }
    }

    /// Request consensus header from a random peer up to three times from three different peers.
    pub async fn request_epoch_cert(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> NetworkResult<(EpochRecord, EpochCertificate)> {
        let request = PrimaryRequest::EpochRecord { epoch, hash };
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            if let Ok(Ok(PrimaryResponse::EpochRecord { record, certificate })) = res.await {
                return Ok((record, certificate));
            }
        }
        Err(NetworkError::RPCError("Could not get the epoch record!".to_string()))
    }

    /// Report a penalty to the network's peer manager.
    async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }
}

/// Handle inter-node communication between primaries.
#[derive(Debug)]
pub struct PrimaryNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: PrimaryNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
}

impl<DB, Events> PrimaryNetwork<DB, Events>
where
    DB: Database,
    Events: RaylsReceiver<NetworkEvent<Req, Res>> + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: Events,
        network_handle: PrimaryNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        rayls_consensus_state_sync: StateSynchronizer<DB>,
        task_spawner: TaskSpawner,
    ) -> Self {
        let request_handler = RequestHandler::new(
            consensus_config,
            consensus_bus,
            rayls_consensus_state_sync.clone(),
        );
        Self { network_events, network_handle, request_handler, task_spawner }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_classified_task(
            "primary network events",
            async move {
                while let Some(event) = self.network_events.recv().await {
                    self.process_network_event(event)
                }
            },
            TaskKind::Cancel,
        );
    }

    /// Handle events concurrently.
    fn process_network_event(&mut self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                PrimaryRequest::Vote { header, parents } => {
                    self.process_vote_request(
                        peer,
                        Arc::unwrap_or_clone(header),
                        parents,
                        channel,
                        cancel,
                    );
                }
                PrimaryRequest::MissingCertificates { inner } => {
                    self.process_request_for_missing_certs(peer, inner, channel, cancel)
                }
                PrimaryRequest::ConsensusHeader { number, hash } => {
                    self.process_consensus_output_request(peer, number, hash, channel, cancel)
                }
                PrimaryRequest::PeerExchange { .. } => {
                    warn!(target: "primary::network", "primary application received unexpected peer exchange message");
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
            },
            NetworkEvent::Gossip(msg, gossip_source) => {
                self.process_gossip(msg, gossip_source);
            }
            NetworkEvent::Error(msg, channel) => {
                let err = PrimaryResponse::Error(PrimaryRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.task_spawner.spawn_task("report request error", async move {
                    let _ = network_handle.handle.send_response(err, channel).await;
                });
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("VoteRequest-{}", header.digest());

        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                vote = request_handler.vote(peer, header, parents) => {
                    let response = vote.into_response();
                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel from network layer - InFlightGuard's Drop handles cleanup
                _ = cancel => {}
            };
        });
    }

    /// Attempt to retrieve certificates for a peer that's missing them.
    fn process_request_for_missing_certs(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("MissingCertsReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                result = request_handler.retrieve_missing_certs(request) => {
                    // report penalty if any
                    if let Err(ref e) = result {
                        if let Some(penalty) = e.into() {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                    }

                    let response = result.into_response();
                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_consensus_output_request(
        &self,
        peer: BlsPublicKey,
        number: Option<u64>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ConsensusOutputReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                header =
                    request_handler.retrieve_consensus_header(number, hash) => {
                        let response = header.into_response();
                        // TODO: penalize peer's reputation for bad request
                        // if response.is_err() { }
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_epoch_record_request(
        &self,
        peer: BlsPublicKey,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("EpochRecordReq-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            tokio::select! {
                header =
                    request_handler.retrieve_epoch_record(epoch, hash) => {
                        let response = header.into_response();
                        // TODO: penalize peer's reputation for bad request
                        // if response.is_err() { }
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from committee.
    fn process_gossip(&self, msg: GossipMessage, gossip_source: BlsPublicKey) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("ProcessGossip-{}-{gossip_source}", msg.topic);
        // spawn task to process gossip
        self.task_spawner.spawn_task(task_name, async move {
            if let Err(ref e) = request_handler.process_gossip(&msg).await {
                // pending certificates are expected when they arrive before parents
                if matches!(e, PrimaryNetworkError::Certificate(CertManagerError::Pending(_))) {
                    debug!(target: "primary::network", ?e, "process_gossip");
                } else {
                    warn!(target: "primary::network", ?e, "process_gossip");
                }
                // convert error into penalty to lower peer score
                if let Some(penalty) = e.into() {
                    network_handle.report_penalty(gossip_source, penalty).await;
                }
            }
        });
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    consensus_bus: ConsensusBus,
    payload_store: DB,
}

impl<DB: PayloadStore> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self.
    pub(crate) fn new(consensus_bus: ConsensusBus, payload_store: DB) -> Self {
        Self { consensus_bus, payload_store }
    }
}

#[async_trait::async_trait]
impl<DB: Database> WorkerToPrimaryClient for WorkerReceiverHandler<DB> {
    async fn report_own_batch(&self, message: WorkerOwnBatchMessage) -> eyre::Result<()> {
        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                ack_channel: tx_ack,
            })
            .await?;

        // If we are ok, then wait for the ack
        rx_ack.await?;

        Ok(response)
    }

    async fn report_others_batch(&self, message: WorkerOthersBatchMessage) -> eyre::Result<()> {
        self.payload_store.write_payload(&message.digest, &message.worker_id)?;
        Ok(())
    }
}

/// Responses to a vote request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestVoteResult {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<CertificateDigest>),
    /// The proposed header is too old for the peer's current round.
    TooOld {
        /// The round of the header that was rejected.
        header_round: Round,
        /// The peer's limit round (below which headers are rejected).
        limit_round: Round,
    },
    /// The proposed header belongs to a different epoch than the peer.
    EpochMismatch {
        /// The epoch the peer expected.
        expected: Epoch,
        /// The epoch of the proposed header.
        received: Epoch,
    },
}
