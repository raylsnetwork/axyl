//! Error types for RL network.

use libp2p::{
    gossipsub::{ConfigBuilderError, PublishError, SubscriptionError},
    kad::GetRecordError,
    request_response::OutboundFailure,
    swarm::DialError,
    TransportError,
};
use std::io;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

/// Networking error type.
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Swarm error dialing a peer.
    #[error("{0}")]
    Dial(String),
    /// Redial attempt.
    #[error("Peer already dialed")]
    RedialAttempt,
    /// Dialing an banned peer.
    #[error("{0}")]
    DialBannedPeer(String),
    /// Dialing an already connected peer.
    #[error("{0}")]
    AlreadyConnected(String),
    /// The peer is already being dialed.
    #[error("{0}")]
    AlreadyDialing(String),
    /// Gossipsub error publishing message.
    #[error(transparent)]
    Publish(#[from] PublishError),
    /// Gossipsub error subscribing to topic.
    #[error(transparent)]
    Subscription(#[from] SubscriptionError),
    /// mpsc try send
    #[error("mpsc try send error: {0}")]
    MpscTrySend(String),
    /// mpsc receiver dropped.
    #[error("mpsc error: {0}")]
    ChannelSender(String),
    /// oneshot sender dropped.
    #[error("oneshot error: {0}")]
    AckChannelClosed(String),
    /// Swarm failed to connect on listen address.
    #[error(transparent)]
    Listen(#[from] TransportError<io::Error>),
    /// Failed to build gossipsub config.
    #[error(transparent)]
    GossipsubConfig(#[from] ConfigBuilderError),
    /// Failed to build swarm with peer scoring enabled.
    #[error("{0}")]
    EnablePeerScoreBehavior(String),
    /// Error conversion from [std::io::Error]
    #[error(transparent)]
    StdIo(#[from] std::io::Error),
    /// Error converted from [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromIntError(#[from] std::num::TryFromIntError),
    /// Libp2p `ResponseChannel` already closed due to timeout or loss of connection.
    #[error("Response channel closed.")]
    SendResponse,
    /// Failed to send request/response outbound to peer.
    #[error("Outbound failure: {0}")]
    Outbound(#[from] OutboundFailure),
    /// Failed to create gossipsub behavior.
    #[error("{0}")]
    GossipBehavior(&'static str),
    /// Failed to build swarm with behavior.
    #[error("SwarmBuilder::with_behaviour failed somehow.")]
    BuildSwarm,
    /// Failed to build the DNS resolver configuration.
    #[error("DNS resolver config error: {0}")]
    DnsConfig(String),
    /// Request/response RPC Error
    #[error("{0}")]
    RPCError(String),
    /// If a request is made to "any" peer and no peers are currently connected.
    #[error("No connected peers")]
    NoPeers,
    /// Response violated the protocol.
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    /// A network operation timed out.
    #[error("Timed Out")]
    Timeout,
    /// This node disconnected from the peer.
    #[error("Disconnected from peer")]
    Disconnected,
    /// The peer was already disconnected.
    #[error("Peer already disconnected")]
    DisconnectPeer,
    /// Fatal error - the swarm is not connected to any listeners.
    #[error("All swarm listeners closed. Network shutting down...")]
    AllListenersClosed,
    /// The retrieved peer record is invalid.
    #[error("Invalid peer record: {0}")]
    InvalidPeerRecord(String),
    /// The requested peer is not on our local store.
    #[error("Requested peer is not in our local store.")]
    PeerMissing,
    /// Rayls: The peer is known but not yet connected.
    #[error("Peer is known but not yet connected")]
    PeerNotConnected,
    /// Kademlia error.
    #[error("Failed to get kad record: {0}")]
    GetKademliaRecord(#[from] GetRecordError),
    /// Kademlia store write error.
    #[error("Failed to store kad record: {0}")]
    StoreKademliaRecord(String),
    /// Request queue overflow - too many pending requests.
    #[error("Request queue overflow - too many pending requests")]
    RequestQueueOverflow,
}

impl From<oneshot::error::RecvError> for NetworkError {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::AckChannelClosed(e.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for NetworkError {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        Self::ChannelSender(e.to_string())
    }
}

impl<T> From<broadcast::error::SendError<T>> for NetworkError {
    fn from(e: broadcast::error::SendError<T>) -> Self {
        Self::ChannelSender(e.to_string())
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for NetworkError {
    fn from(e: mpsc::error::TrySendError<T>) -> Self {
        Self::MpscTrySend(e.to_string())
    }
}

impl From<&DialError> for NetworkError {
    fn from(e: &DialError) -> Self {
        Self::Dial(e.to_string())
    }
}
