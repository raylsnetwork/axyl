use crate::{
    kad::KadStore,
    peers::{self, PeerManager},
};
use libp2p::{
    gossipsub::{self},
    kad::{self},
    relay::{self},
    request_response::{self, Codec},
    swarm::NetworkBehaviour,
    PeerId,
};
use rayls_infrastructure_config::PeerConfig;
use rayls_infrastructure_types::Database;

/// Custom network libp2p behaviour type for Rayls Network.
///
/// The behavior includes gossipsub and request-response.
#[derive(NetworkBehaviour)]
pub(crate) struct RLBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
{
    /// The peer manager.
    ///
    /// MUST be the first field. The derived `NetworkBehaviour` invokes each field's
    /// `handle_established_{inbound,outbound}_connection` in declaration order and does not roll
    /// back earlier fields when a later one denies. `PeerManager` is the only behaviour that
    /// denies connections (banned peer / peer limit reached / bad IP); every other behaviour
    /// unconditionally registers the connection in its own state during `handle_established_*`.
    /// If those ran first, a subsequent `PeerManager` denial would leave a phantom connection in
    /// e.g. `request_response`'s `connected` map -- the swarm then dispatches
    /// `ListenFailure`/`DialFailure` (which `request_response` does not use to clean up) rather
    /// than `ConnectionClosed`, and the stale entry later trips its `debug_assert_eq!` in
    /// `on_connection_closed`. Ordering `PeerManager` first makes its denial short-circuit before
    /// any sibling registers the connection. The relay setup surfaced this by inflating the
    /// startup connection/dial count past the peer limit.
    pub(crate) peer_manager: peers::PeerManager,
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// Used for peer discovery.
    pub(crate) kademlia: kad::Behaviour<KadStore<DB>>,
    /// Circuit-relay-v2 client.
    ///
    /// Enables this node to reserve a slot on a relay server (by listening on a `/p2p-circuit`
    /// address) and to dial other peers through that relay. Peers are reached over the relay
    /// whenever their advertised address is a `/…/p2p-circuit/…` multiaddr.
    pub(crate) relay_client: relay::client::Behaviour,
}

impl<C, DB> RLBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        gossipsub: gossipsub::Behaviour,
        req_res: request_response::Behaviour<C>,
        kademlia: kad::Behaviour<KadStore<DB>>,
        peer_config: &PeerConfig,
        relay_client: relay::client::Behaviour,
        local_peer_id: PeerId,
    ) -> Self {
        let peer_manager = PeerManager::new(peer_config, local_peer_id);
        Self { peer_manager, gossipsub, req_res, kademlia, relay_client }
    }
}
