//! Types for managing peers.

use crate::types::NetworkResult;
use libp2p::{Multiaddr, PeerId};
use rayls_infrastructure_types::{BlsPublicKey, NetworkPublicKey};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::IntoIter, HashMap, HashSet},
    net::IpAddr,
};
use tokio::sync::oneshot;

/// Events for the `PeerManager`.
#[derive(Debug)]
pub(crate) enum PeerEvent {
    /// Connected with peer.
    PeerConnected(PeerId, Multiaddr),
    /// Peer was disconnected.
    PeerDisconnected(PeerId),
    /// Disconnect from the peer without exchanging peer information.
    /// This is the event for disconnecting from penalized peers.
    DisconnectPeer(PeerId),
    /// Disconnect from the peer and share peer information for discovery.
    /// This is the event for disconnecting from excess peers with otherwise trusted reputations.
    DisconnectPeerX(PeerId, PeerExchangeMap),
    /// Peer manager has identified a peer and associated ip addresses to ban.
    Banned(PeerId),
    /// Peer manager has unbanned a peer and associated ip addresses.
    Unbanned(PeerId),
    /// Authorities are missing from the peer map. This triggers kad queries.
    MissingAuthorities(Vec<BlsPublicKey>),
    /// A known committee member is neither connected nor dialing and should be re-dialed.
    ///
    /// Carries the BLS key (not addresses) so the network layer routes the dial through the
    /// resolving `DialBls` path: a `/dnsaddr`-advertised member must be resolved to concrete
    /// `/p2p-circuit` addresses off the swarm loop before dialing, or the relay client selects
    /// its connection handler from the unresolved address shape and mishandles the connection.
    RedialCommittee(BlsPublicKey),
    /// Initiate a discovery attempt because discovery peer counts are low.
    Discovery,
}

/// The action to take after a peer's reputation or connection status changes.
///
/// Both reputation and connection status changes may require the manager to take
/// action to update the peer.
#[derive(Debug, PartialEq)]
pub(super) enum PeerAction {
    /// Ban the peer and the associated IP addresses.
    Ban(Vec<IpAddr>),
    /// No action needed.
    NoAction,
    /// Disconnect from peer.
    Disconnect,
    /// Disconnect a peer with peer exchange information to support discovery.
    /// This results in a temporary ban to prevent immediate reconnection attempts.
    DisconnectWithPX,
    /// Unban the peer and its known IP addresses.
    Unban(Vec<IpAddr>),
}

impl PeerAction {
    /// Helper method if the action is to ban the peer.
    pub(super) fn is_ban(&self) -> bool {
        matches!(self, PeerAction::Ban(_))
    }
}

/// Penalties applied to peers based on the significance of their actions.
///
/// Each variant has an associated score change.
///
/// NOTE: the number of variations is intentionally low.
/// Too many variations or specific penalties would result in more complexity.
#[derive(Debug, Clone, Copy)]
pub enum Penalty {
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a high tolerance for this type of error and will be banned ~50 occurances.
    Mild,
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a medium tolerance for this type of error and will be banned ~10 occurances.
    Medium,
    /// The penalty assessed for actions that are likely not malicious, but will not be tolerated.
    ///
    /// The peer will be banned after ~5 occurances (based on -100).
    Severe,
    /// The penalty assessed for unforgiveable actions.
    ///
    /// This type of action results in disconnecting from a peer and banning them.
    Fatal,
}

/// Request for dialing peers.
#[derive(Debug)]
pub(crate) struct DialRequest {
    /// The peer's network id.
    pub(crate) peer_id: PeerId,
    /// The multiaddr to dial.
    pub(crate) multiaddrs: Vec<Multiaddr>,
    /// The channel to forward results and errors.
    /// Optional in case dial is the result of a peer-exchange.
    pub(crate) reply: Option<oneshot::Sender<NetworkResult<()>>>,
}

/// Types of connections between peers.
#[derive(Debug)]
pub(super) enum ConnectionType {
    /// A peer has successfully dialed this node.
    IncomingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
    /// This node has successfully dialed a peer.
    OutgoingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
}

/// Direction of connection between peers from the local node's perspective.
#[derive(Debug, Clone, Serialize)]
pub(super) enum ConnectionDirection {
    /// The connection was established by a peer dialing this node.
    Incoming,
    /// The connection was established by this node dialing a peer.
    Outgoing,
}

/// Wrapper for a map of [PeerId] to a collection of [Multiaddr].
///
/// This is a convenience wrapper because PeerId doesn't implement `Deserialize`.
/// Peers exchange information to facilitate discovery.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct PeerExchangeMap(pub HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>);

impl IntoIterator for PeerExchangeMap {
    type Item = (BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>));
    type IntoIter = IntoIter<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>> for PeerExchangeMap {
    fn from(value: HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>) -> Self {
        Self(value)
    }
}
