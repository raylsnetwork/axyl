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
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// The peer manager.
    pub(crate) peer_manager: peers::PeerManager,
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
    ) -> Self {
        let peer_manager = PeerManager::new(peer_config);
        Self { gossipsub, req_res, peer_manager, kademlia, relay_client }
    }
}
