//! Constants and trait implementations for network compatibility.

use crate::{
    codec::RLMessage, error::NetworkError, peers::Penalty, GossipMessage, PeerExchangeMap,
};
pub use libp2p::gossipsub::MessageId;
use libp2p::{
    core::transport::ListenerId,
    gossipsub::{PublishError, SubscriptionError, TopicHash},
    multiaddr::Protocol,
    request_response::ResponseChannel,
    Multiaddr, PeerId, TransportError,
};
use rayls_infrastructure_types::{
    encode, now, BlsPublicKey, BlsSignature, NetworkPublicKey, P2pNode, TimestampSec,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

#[cfg(test)]
#[path = "tests/types.rs"]
mod network_types;

/// The result for network operations.
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Returns true when the address contains a `/dnsaddr` component.
///
/// Such an address is advertise-only: it names relay circuits via `_dnsaddr` DNS TXT records,
/// so it can neither be listened on nor dialed raw (the relay client selects its connection
/// handler from the literal address shape) - it must be resolved to concrete circuits first.
pub fn is_dnsaddr(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| matches!(p, Protocol::Dnsaddr(_)))
}

/// Extracts the relay server's [`PeerId`] from a circuit address of the form
/// `<relay-addr>/p2p/<relay-id>/p2p-circuit/p2p/<dst-id>`: the `P2p` component immediately
/// preceding the `P2pCircuit` protocol. Returns `None` for non-relayed addresses.
pub fn circuit_relay_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    let mut last_p2p = None;
    for proto in addr.iter() {
        match proto {
            Protocol::P2p(peer) => last_p2p = Some(peer),
            Protocol::P2pCircuit => return last_p2p,
            _ => {}
        }
    }
    None
}

/// Helper trait to cast lib-specific results into RPC messages.
pub trait IntoResponse<M> {
    /// Convert a [Result] into a [RLMessage] type.
    fn into_response(self) -> M;
}

impl<M, E> IntoResponse<M> for Result<M, E>
where
    M: RLMessage + IntoRpcError<E>,
{
    fn into_response(self) -> M {
        self.unwrap_or_else(|e| M::into_error(e))
    }
}

/// Convenience trait for casting lib-specific error types to RPC application-layer error messages.
pub trait IntoRpcError<E> {
    /// Convert application-layer error into message.
    fn into_error(error: E) -> Self;
}

/// The topic for NVVs to subscribe to for published worker batches.
pub const WORKER_BATCH_TOPIC: &str = "rayls_batches";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "rayls_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "rayls_consensus_headers";

/// Events created from network activity.
#[derive(Debug)]
pub enum NetworkEvent<Req, Res> {
    /// Direct request from peer.
    Request {
        /// The peer that made the request.
        peer: BlsPublicKey,
        /// The network request type.
        request: Req,
        /// The network response channel.
        channel: ResponseChannel<Res>,
        /// The oneshot channel if the request gets cancelled at the network level.
        cancel: oneshot::Receiver<()>,
    },
    /// Gossip message received and propagation source.
    Gossip(GossipMessage, BlsPublicKey),
    /// Send an error back the requester.
    Error(String, ResponseChannel<Res>),
}

/// Commands for the swarm.
#[derive(Debug)]
pub enum NetworkCommand<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Update the list of authorized publishers.
    ///
    /// This list is used to verify messages came from an authorized source.
    /// Only valid for Subscriber implementations.
    UpdateAuthorizedPublishers {
        /// The unique set of authorized peers by topic.
        authorities: HashMap<String, Option<HashSet<BlsPublicKey>>>,
        /// The acknowledgement that the set was updated.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Start listening on the provided multiaddr.
    ///
    /// Return the result to caller.
    StartListening {
        /// The [Multiaddr] for the swarm to connect.
        multiaddr: Multiaddr,
        /// Oneshot channel for reply.
        reply: oneshot::Sender<Result<ListenerId, TransportError<std::io::Error>>>,
    },
    /// Listeners
    GetListener {
        /// The reply to caller.
        reply: oneshot::Sender<Vec<Multiaddr>>,
    },
    /// Add explicit peer and then dial it.
    ///
    /// This adds to the swarm's peers and the gossipsub's peers.
    AddTrustedPeerAndDial {
        /// The Bls public key for this record.
        bls_pubkey: BlsPublicKey,
        /// The peer's id.
        network_pubkey: NetworkPublicKey,
        /// The peer's address.
        addr: Multiaddr,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Add explicit peer to internal bls to peer cache.
    AddExplicitPeer {
        /// The Bls public key for this record.
        bls_pubkey: BlsPublicKey,
        /// The peer's id.
        network_pubkey: NetworkPublicKey,
        /// The peer's address.
        addr: Multiaddr,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Add explicit peers to internal bls to peer cache.
    /// Don't overwrite existing records.
    AddBootstrapPeers {
        /// The Bls public key for t.
        peers: BTreeMap<BlsPublicKey, P2pNode>,
        /// Reply for connection outcome.
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Register relays discovered from `/dnsaddr` resolution as protected.
    ///
    /// Sent by an off-loop discovery task (see `AddBootstrapPeers`) so the blocking DNS lookup
    /// never runs on the swarm event loop -- blocking the loop stops the swarm from servicing
    /// relayed (yamux-over-circuit) connections, which have no transport-level keep-alive and get
    /// reset by peers. Registration itself is cheap and runs on the loop. Fire-and-forget.
    RegisterRelays {
        /// Circuit multiaddrs whose `/p2p/<relay>` peer ids should be exempted from banning.
        circuits: Vec<Multiaddr>,
    },
    /// Dial a peer to establish a connection.
    Dial {
        /// The peer's id.
        peer_id: PeerId,
        /// The peer's address.
        peer_addr: Multiaddr,
        /// Oneshot for reply
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Dial a peer by bls key to establish a connection.
    DialBls {
        /// The peer's bls public key.
        bls_key: BlsPublicKey,
        /// Oneshot for reply
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Dial a peer with a set of already-resolved addresses.
    ///
    /// Used by `DialBls` after resolving a committee peer's `/dnsaddr` to concrete
    /// `/p2p-circuit` addresses off the swarm loop. The dialed address MUST be the concrete
    /// circuit (not `/dnsaddr`): the relay client behaviour selects its connection handler by
    /// multiaddr shape (`is_relayed`), so it has to see the `/p2p-circuit` to treat the
    /// connection as relayed rather than as a direct link to a relay.
    DialResolved {
        /// The peer's id.
        peer_id: PeerId,
        /// Concrete addresses to dial (e.g. circuits via each advertised relay).
        addrs: Vec<Multiaddr>,
        /// Oneshot for reply
        reply: oneshot::Sender<NetworkResult<()>>,
    },
    /// Return an owned copy of this node's [PeerId].
    LocalPeerId {
        /// Reply to caller.
        reply: oneshot::Sender<PeerId>,
    },
    /// Send a request to a peer.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequest {
        /// The destination peer.
        peer: BlsPublicKey,
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: oneshot::Sender<NetworkResult<Res>>,
    },
    /// Send a request to a peer by PeerId.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequestDirect {
        /// The destination peer.
        peer: PeerId,
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: oneshot::Sender<NetworkResult<Res>>,
    },
    /// Send a request to any connected peer.
    ///
    /// The caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    SendRequestAny {
        /// The request to send.
        request: Req,
        /// Channel for forwarding any responses.
        reply: oneshot::Sender<NetworkResult<Res>>,
    },
    /// Send response to a peer's request.
    SendResponse {
        /// The encoded message data.
        response: Res,
        /// The libp2p response channel.
        channel: ResponseChannel<Res>,
        /// Oneshot channel for returning result.
        reply: oneshot::Sender<Result<(), Res>>,
    },
    /// Subscribe to a topic.
    Subscribe {
        /// The topic to subscribe to.
        topic: String,
        /// Authorized publishers.
        publishers: Option<HashSet<BlsPublicKey>>,
        /// The reply to caller.
        reply: oneshot::Sender<Result<bool, SubscriptionError>>,
    },
    /// Publish a message to topic subscribers.
    Publish {
        /// The topic to publish the message on.
        topic: String,
        /// The encoded message to publish.
        msg: Vec<u8>,
        /// The reply to caller.
        reply: oneshot::Sender<Result<MessageId, PublishError>>,
    },
    /// Map of all known peers and their associated subscribed topics.
    AllPeers {
        /// Reply to caller.
        reply: oneshot::Sender<HashMap<PeerId, Vec<TopicHash>>>,
    },
    /// Collection of this node's connected peers.
    ConnectedPeerIds {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// Collection of this node's connected peers.
    ConnectedPeers {
        /// Reply to caller.
        reply: oneshot::Sender<Vec<BlsPublicKey>>,
    },
    /// Collection of all mesh peers by a certain topic hash.
    MeshPeers {
        /// The topic to filter peers.
        topic: String,
        /// Reply to caller.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// The peer's score, if it exists.
    PeerScore {
        /// The peer's id.
        peer_id: PeerId,
        /// Reply to caller.
        reply: oneshot::Sender<Option<f64>>,
    },
    /// Report penalty for peer.
    ReportPenalty {
        /// The peer's id.
        peer: BlsPublicKey,
        /// The penalty to apply to the peer.
        penalty: Penalty,
    },
    /// Return the number of pending outbound requests.
    PendingRequestCount {
        /// Reply to caller.
        reply: oneshot::Sender<usize>,
    },
    /// Disconnect a peer by [PeerId]. The oneshot returns a result if the peer
    /// was connected or not.
    DisconnectPeer {
        /// The peer's id.
        peer_id: PeerId,
        /// Reply to caller.
        reply: oneshot::Sender<Result<(), ()>>,
    },
    /// Retrieve peers from peer manager to share with a requesting peer.
    PeersForExchange {
        /// The reply to caller.
        reply: oneshot::Sender<PeerExchangeMap>,
    },
    /// Start a new epoch.
    NewEpoch {
        /// The epoch committee.
        committee: HashSet<BlsPublicKey>,
    },
    /// Find authorities for a future committee by bls key and return to sender.
    FindAuthorities {
        /// The collection of bls public keys associated with authorities to find.
        bls_keys: Vec<BlsPublicKey>,
    },
}

/// Network handle.
///
/// The type that sends commands to the running network (swarm) task.
#[derive(Clone, Debug)]
pub struct NetworkHandle<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Sending channel to the network to process commands.
    sender: mpsc::Sender<NetworkCommand<Req, Res>>,
}

impl<Req, Res> NetworkHandle<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Create a new instance of Self.
    pub fn new(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self { sender }
    }

    /// Create a handle to no where for test setup.
    pub fn new_for_test() -> Self {
        let (sender, _) = mpsc::channel(100);
        Self { sender }
    }

    /// Update the list of authorized publishers.
    pub async fn update_authorized_publishers(
        &self,
        authorities: HashMap<String, Option<HashSet<BlsPublicKey>>>,
    ) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::UpdateAuthorizedPublishers { authorities, reply }).await?;
        ack.await?
    }

    /// Start swarm listening on the given address. Returns an error if the address is not
    /// supported.
    ///
    /// Return swarm error to caller.
    pub async fn start_listening(&self, multiaddr: Multiaddr) -> NetworkResult<ListenerId> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::StartListening { multiaddr, reply }).await?;
        let res = ack.await?;
        res.map_err(Into::into)
    }

    /// Request listeners from the swarm.
    pub async fn listeners(&self) -> NetworkResult<Vec<Multiaddr>> {
        let (reply, listeners) = oneshot::channel();
        self.sender.send(NetworkCommand::GetListener { reply }).await?;
        listeners.await.map_err(Into::into)
    }

    /// Add explicit "trusted" peer.
    ///
    /// These peers are considered "trusted" and do not receive penalties.
    /// This does not unban ips and should only be called during initialization.
    pub async fn add_trusted_peer_and_dial(
        &self,
        bls_pubkey: BlsPublicKey,
        network_pubkey: NetworkPublicKey,
        addr: Multiaddr,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender
            .send(NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, network_pubkey, addr, reply })
            .await?;
        rx.await?
    }

    /// Add explicit peer.
    pub async fn add_explicit_peer(
        &self,
        bls_pubkey: BlsPublicKey,
        network_pubkey: NetworkPublicKey,
        addr: Multiaddr,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender
            .send(NetworkCommand::AddExplicitPeer { bls_pubkey, network_pubkey, addr, reply })
            .await?;
        rx.await?
    }

    /// Add explicit bootstrap peers.
    pub async fn add_bootstrap_peers(
        &self,
        peers: BTreeMap<BlsPublicKey, P2pNode>,
    ) -> NetworkResult<()> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::AddBootstrapPeers { peers, reply }).await?;
        rx.await?
    }

    /// Dial a peer by Bls public key.
    ///
    /// Return swarm error to caller.
    pub async fn dial_by_bls(&self, bls_key: BlsPublicKey) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::DialBls { bls_key, reply }).await?;
        ack.await?
    }

    /// Subscribe to a topic with valid publishers.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe_with_publishers(
        &self,
        topic: String,
        publishers: HashSet<BlsPublicKey>,
    ) -> NetworkResult<bool> {
        self.subscribe_inner(topic, Some(publishers)).await
    }

    /// Subscribe to a topic, any publisher valid.
    ///
    /// Return swarm error to caller.
    pub async fn subscribe(&self, topic: String) -> NetworkResult<bool> {
        self.subscribe_inner(topic, None).await
    }

    async fn subscribe_inner(
        &self,
        topic: String,
        publishers: Option<HashSet<BlsPublicKey>>,
    ) -> NetworkResult<bool> {
        let has_publishers = publishers.is_some();
        let (reply, already_subscribed) = oneshot::channel();
        self.sender.send(NetworkCommand::Subscribe { topic, publishers, reply }).await?;
        let res = already_subscribed.await?;
        debug!(target: "network", already_subscribed=?res, with_publishers=has_publishers, "Subscribe");
        res.map_err(Into::into)
    }

    /// Publish a message on a certain topic.
    pub async fn publish(&self, topic: String, msg: Vec<u8>) -> NetworkResult<MessageId> {
        let (reply, published) = oneshot::channel();
        self.sender.send(NetworkCommand::Publish { topic, msg, reply }).await?;
        published.await?.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peer_count(&self) -> NetworkResult<usize> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeerIds { reply }).await?;
        Ok(peers.await?.len())
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> NetworkResult<Vec<BlsPublicKey>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeers { reply }).await?;
        peers.await.map_err(Into::into)
    }

    /// Send a request to a peer.
    ///
    /// Returns a handle for the caller to await the peer's response.
    pub async fn send_request(
        &self,
        request: Req,
        peer: BlsPublicKey,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<Res>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequest { peer, request, reply }).await?;
        Ok(to_caller)
    }

    /// Send a request to a peer- any peer will do.
    ///
    /// Returns a handle for the caller to await the peer's response.
    pub async fn send_request_any(
        &self,
        request: Req,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<Res>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequestAny { request, reply }).await?;
        Ok(to_caller)
    }

    /// Respond to a peer's request.
    pub async fn send_response(
        &self,
        response: Res,
        channel: ResponseChannel<Res>,
    ) -> NetworkResult<()> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::SendResponse { response, channel, reply }).await?;
        res.await?.map_err(|_| NetworkError::SendResponse)
    }

    /// Return the number of pending requests.
    ///
    /// Mostly helpful for testing, but could be useful for managing outbound requests.
    pub async fn get_pending_request_count(&self) -> NetworkResult<usize> {
        let (reply, count) = oneshot::channel();
        self.sender.send(NetworkCommand::PendingRequestCount { reply }).await?;
        count.await.map_err(Into::into)
    }

    /// Disconnect from the peer.
    ///
    /// This method closes all connections to the peer without waiting for handlers
    /// to complete.
    pub(crate) async fn disconnect_peer(&self, peer_id: PeerId) -> NetworkResult<()> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::DisconnectPeer { peer_id, reply }).await?;
        res.await?.map_err(|_| NetworkError::DisconnectPeer)
    }

    /// Report a penalty to the peer manager.
    pub async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        let _ = self.sender.send(NetworkCommand::ReportPenalty { peer, penalty }).await;
    }

    /// Create a [PeerExchangeMap] for exchanging peers.
    pub async fn peers_for_exchange(&self) -> NetworkResult<PeerExchangeMap> {
        let (reply, res) = oneshot::channel();
        self.sender.send(NetworkCommand::PeersForExchange { reply }).await?;
        res.await.map_err(Into::into)
    }

    /// Create a [PeerExchangeMap] for exchanging peers.
    pub async fn new_epoch(&self, committee: HashSet<BlsPublicKey>) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::NewEpoch { committee }).await?;
        Ok(())
    }

    /// Return network information for authorities by bls pubkey on kad.
    pub async fn find_authorities(&self, bls_keys: Vec<BlsPublicKey>) -> NetworkResult<()> {
        self.sender.send(NetworkCommand::FindAuthorities { bls_keys }).await?;
        Ok(())
    }

    /// Map of all known peers and their associated subscribed topics.
    pub async fn all_peers(&self) -> NetworkResult<HashMap<PeerId, Vec<TopicHash>>> {
        let (reply, all_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::AllPeers { reply }).await?;
        all_peers.await.map_err(Into::into)
    }

    /// Collection of all mesh peers by a certain topic hash.
    pub async fn mesh_peers(&self, topic: String) -> NetworkResult<Vec<PeerId>> {
        let (reply, mesh_peers) = oneshot::channel();
        self.sender.send(NetworkCommand::MeshPeers { topic, reply }).await?;
        mesh_peers.await.map_err(Into::into)
    }
}

/// List of addresses for a node, signature will be the nodes BLS signature
/// over the addresses to verify they are from the node in question.
/// Used to publish this to kademlia.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The network information contained within the record.
    pub info: NetworkInfo,
    /// Signature of the info field with the node's BLS key.
    /// This is part of a kademlia record keyed on a BLS public key
    /// that can be used for verifiction.  Intended to stop malicious
    /// nodes from poisoning the routing table.
    pub signature: BlsSignature,
}

impl NodeRecord {
    /// Helper method to build a signed node record.
    pub fn build<F>(pubkey: NetworkPublicKey, multiaddr: Multiaddr, signer: F) -> NodeRecord
    where
        F: FnOnce(&[u8]) -> BlsSignature,
    {
        let info = NetworkInfo { pubkey, multiaddrs: vec![multiaddr], timestamp: now() };
        let data = encode(&info);
        let signature = signer(&data);
        Self { info, signature }
    }

    /// Verify if a signature matches the record.
    pub fn verify(&self, pubkey: &BlsPublicKey) -> Result<(), String> {
        let data = encode(&self.info);
        if self.signature.verify_raw(&data, pubkey) {
            Ok(())
        } else {
            Err("Invalid signature for NodeRecord".to_string())
        }
    }

    /// Return a reference to the record's [NetworkInfo].
    pub fn info(&self) -> &NetworkInfo {
        &self.info
    }
}

/// The network information needed for consensus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInfo {
    /// The node's [NetworkPublicKey].
    pub pubkey: NetworkPublicKey,
    /// Network address for node.
    pub multiaddrs: Vec<Multiaddr>,
    /// The timestamps when this was published.
    /// Useful for nodes to compare latest records.
    pub timestamp: TimestampSec,
}

/// Outbound kad query from this node.
#[derive(Debug)]
pub struct KadQuery {
    /// The [BlsPublicKey] for the requested authority record.
    pub request: BlsPublicKey,
    /// The best result so far.
    pub result: Option<NodeRecord>,
}

impl From<BlsPublicKey> for KadQuery {
    fn from(request: BlsPublicKey) -> Self {
        Self { request, result: None }
    }
}

/// Helper macro for sending oneshot replies and logging errors.
///
/// The arguments are:
/// 1) oneshot::Sender
/// 2) value to send through oneshot channel
/// 3) string error message
/// 4) `key = value` for additional logging (Optional)
#[macro_export]
macro_rules! send_or_log_error {
    // basic case: Takes a result expression and an error message string
    ($reply:expr, $result:expr, $error_msg:expr) => {
        if let Err(e) = $reply.send($result) {
            error!(target: "network", ?e, $error_msg);
        }
    };

    // optional case that allows specifying additional error context
    ($reply:expr, $result:expr, $error_msg:expr, $($field:ident = $value:expr),+ $(,)?) => {
        if let Err(e) = $reply.send($result) {
            error!(target: "network", ?e, $($field = ?$value,)+ $error_msg);
        }
    };
}

/// Some PeerId specific code only used for in-crate testing.
#[cfg(test)]
impl<Req, Res> NetworkHandle<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub(crate) async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        let (reply, ack) = oneshot::channel();
        self.sender.send(NetworkCommand::Dial { peer_id, peer_addr, reply }).await?;
        ack.await?
    }

    /// Retrieve a specific peer's score, if it exists.
    pub(crate) async fn peer_score(&self, peer_id: PeerId) -> NetworkResult<Option<f64>> {
        let (reply, score) = oneshot::channel();
        self.sender.send(NetworkCommand::PeerScore { peer_id, reply }).await?;
        score.await.map_err(Into::into)
    }

    /// Get local peer id.
    pub(crate) async fn local_peer_id(&self) -> NetworkResult<PeerId> {
        let (reply, peer_id) = oneshot::channel();
        self.sender.send(NetworkCommand::LocalPeerId { reply }).await?;
        peer_id.await.map_err(Into::into)
    }

    /// Retrieve a collection of connected peers.
    pub(crate) async fn connected_peer_ids(&self) -> NetworkResult<Vec<PeerId>> {
        let (reply, peers) = oneshot::channel();
        self.sender.send(NetworkCommand::ConnectedPeerIds { reply }).await?;
        peers.await.map_err(Into::into)
    }

    /// Send a request to a peer by peer id.
    ///
    /// Returns a handle for the caller to await the peer's response.
    /// For internal network use.
    pub(crate) async fn send_request_direct(
        &self,
        request: Req,
        peer: PeerId,
    ) -> NetworkResult<oneshot::Receiver<NetworkResult<Res>>> {
        let (reply, to_caller) = oneshot::channel();
        self.sender.send(NetworkCommand::SendRequestDirect { peer, request, reply }).await?;
        Ok(to_caller)
    }
}
