//! Unit tests for the `NetworkBehaviour` connection hooks.

use super::*;
use crate::{common::create_multiaddr, Penalty};
use rayls_infrastructure_config::PeerConfig;

/// Build a `PeerManager` with the default operator config.
fn peer_manager() -> PeerManager {
    PeerManager::new(&PeerConfig::default(), PeerId::random())
}

/// An inbound connection endpoint from `addr`.
fn inbound(addr: &Multiaddr) -> ConnectedPoint {
    ConnectedPoint::Listener { local_addr: create_multiaddr(None), send_back_addr: addr.clone() }
}

/// A connection the manager refuses to track must not reach the application as connected, or the
/// application routes requests to a peer that has no state in `AllPeers`.
#[tokio::test]
async fn refused_registration_is_not_announced_as_connected() {
    let mut manager = peer_manager();
    let addr = create_multiaddr(None);
    let peer_id = PeerId::random();

    // establish, ban, and disconnect so the peer is genuinely banned
    manager.on_connection_established(peer_id, &inbound(&addr), 0);
    manager.process_penalty(peer_id, Penalty::Fatal);
    manager.register_disconnected(&peer_id);
    while manager.poll_events().is_some() {}

    // the ban landed mid-handshake, so the admission gate passed but registration refuses
    manager.on_connection_established(peer_id, &inbound(&addr), 0);

    let mut events = Vec::new();
    while let Some(event) = manager.poll_events() {
        events.push(event);
    }
    assert!(
        !events.iter().any(|e| matches!(e, PeerEvent::PeerConnected(id, _) if *id == peer_id)),
        "announced a peer the manager refused to track: {events:?}"
    );
}
