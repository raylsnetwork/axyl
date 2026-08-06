//! Implement the libp2p network behavior to manage peers in the swarm.

use super::{manager::PeerManager, types::DialRequest, PeerEvent};
use crate::peers::types::ConnectionType;
use libp2p::{
    core::{transport::PortUse, ConnectedPoint, Endpoint},
    swarm::{
        behaviour::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        dummy::ConnectionHandler,
        ConnectionClosed, ConnectionDenied, ConnectionId, DialError, DialFailure, FromSwarm,
        NetworkBehaviour, THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use std::task::{Context, Poll};
use tracing::{debug, error, info, trace};

#[cfg(test)]
#[path = "../tests/behavior.rs"]
mod behavior_tests;

impl NetworkBehaviour for PeerManager {
    type ConnectionHandler = ConnectionHandler;
    type ToSwarm = PeerEvent;

    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr], // kad may dial by PeerId only
        _effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        // kademlia can initiate dial attempts
        //
        // ensure PeerId isn't banned if known and register dial attempt
        if let Some(peer_id) = maybe_peer {
            // PeerManager and Kad may initiate dials
            // intercept kad dial attempts, sanitize, and register
            if self.dial_attempt_already_registered(&peer_id) {
                // peer manager has already approved this dial attempt
                return Ok(vec![]);
            }

            if self.peer_banned(&peer_id) {
                debug!(target: "peer-manager", ?peer_id, "outbound dial denied: peer is banned");
                return Err(ConnectionDenied::new("peer is banned"));
            }

            debug!(target: "peer-manager", ?peer_id, ?addresses, "kad initiated dial attempt for peer");
            // Register the attempt so we track it
            self.register_dial_attempt(peer_id, None);
        }

        // do not check peer connection limits since kad may try to find better peers for routing
        // excess peers are pruned next heartbeat
        //
        // NOTE: kademlia extends addresses by default
        // See swarm `WithPeerId::build` -> DialOpts
        Ok(vec![])
    }

    // filter connections
    fn handle_pending_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        debug!(target: "network", ?remote_addr, "handle pending inbound connection");
        self.sanitize_ip_addr(remote_addr)
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        trace!(target: "peer-manager", ?peer, ?remote_addr, "inbound connection established");
        // ensure banned peers are not accepted
        if self.peer_banned(&peer) {
            return Err(ConnectionDenied::new("peer is banned"));
        }

        // check connection limits
        if self.peer_inbound_limit_reached() && !self.peer_is_important(&peer) {
            return Err(ConnectionDenied::new("peer limit reached"));
        }

        Ok(ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        _role_override: Endpoint,
        _port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        trace!(target: "peer-manager", ?peer, ?addr, "outbound connection established");
        if self.peer_banned(&peer) {
            error!(target: "peer-manager", ?peer, ?addr, "established outbound connection with banned peer - disconnecting...");
            return Err(ConnectionDenied::new("peer is banned"));
        }

        // kad may dial peers by PeerId only, so always santize ban IPs after connection established
        self.sanitize_ip_addr(addr)?;

        Ok(ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                endpoint,
                other_established,
                ..
            }) => {
                // NOTE: The ConnectionEstablished event must be handled because
                // NetworkBehaviour::handle_established_inbound_connection and
                // NetworkBehaviour::handle_established_outbound_connection are fallible.
                //
                // Another behaviour can terminate the connection early, making it unsafe to
                // assume a peer is connected until this event is received.
                self.on_connection_established(peer_id, endpoint, other_established)
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                endpoint,
                remaining_established,
                ..
            }) => self.on_connection_closed(peer_id, endpoint, remaining_established),
            FromSwarm::DialFailure(DialFailure { peer_id, error, connection_id: _ }) => {
                debug!(target: "peer-manager", ?peer_id, ?error, "failed to dial peer");
                self.on_dial_failure(peer_id, error);
            }
            FromSwarm::ExternalAddrConfirmed(_) => {
                // The external address was confirmed: possible to support NAT traversal
                //
                // TODO: Issue #254 update metrics here
            }
            _ => {
                // `FromSwarm` is non-exhaustive
                //
                // remaining events are handled by `SwarmEvent`s
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        // "dummy handler" - no events
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // poll heartbeat
        while self.heartbeat_ready(cx) {
            self.heartbeat();
        }

        // pass the next event to the swarm if the manager's events aren't empty
        if let Some(next_event) = self.poll_events() {
            return Poll::Ready(ToSwarm::GenerateEvent(next_event));
        }

        // process dial requests after all events drained
        if let Some(request) = self.next_dial_request() {
            let DialRequest { peer_id, multiaddrs, reply } = request;

            debug!(target: "network", ?peer_id, "network behavior processing next dial request");

            // register to send result back to caller
            self.register_dial_attempt(peer_id, reply);

            // swarm to dial peer
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer_id)
                    .condition(PeerCondition::NotDialing)
                    .addresses(multiaddrs)
                    .build(),
            });
        }

        Poll::Pending
    }
}

impl PeerManager {
    /// Logic to ensure a pending connection supports ipv4 or ipv6, and that the ip address isn't
    /// banned.
    fn sanitize_ip_addr(&self, remote_addr: &Multiaddr) -> Result<(), ConnectionDenied> {
        // only support ipv4 and ipv6
        if !self.has_valid_unbanned_ips(std::slice::from_ref(remote_addr)) {
            return Err(ConnectionDenied::new(
                "Connection denied: peer has no valid unbanned IP addresses".to_string(),
            ));
        }
        Ok(())
    }

    /// Handle on connection established event from the swarm.
    ///
    /// The ConnectionEstablished event must be handled separately because
    /// NetworkBehaviour::handle_established_inbound_connection and
    /// NetworkBehaviour::handle_established_outbound_connection are fallible.
    ///
    /// Another behavior can terminate the connection early, making it unsafe to
    /// assume a peer is connected until this event is received.
    fn on_connection_established(
        &mut self,
        peer_id: PeerId,
        endpoint: &ConnectedPoint,
        other_established: usize,
    ) {
        debug!(
            target: "peer-manager",
            ?peer_id,
            multiaddr = ?endpoint.get_remote_address(),
            other_established,
            "connection established"
        );

        // register peers as connected by this point
        // even if the peer is to be immediately disconnected with peer-exchange (PX)
        let (multiaddr, registered) = match endpoint {
            ConnectedPoint::Listener { send_back_addr, .. } => (
                send_back_addr.clone(),
                self.register_peer_connection(
                    &peer_id,
                    ConnectionType::IncomingConnection { multiaddr: send_back_addr.clone() },
                ),
            ),
            ConnectedPoint::Dialer { address, .. } => (
                address.clone(),
                self.register_peer_connection(
                    &peer_id,
                    ConnectionType::OutgoingConnection { multiaddr: address.clone() },
                ),
            ),
        };

        // TODO: Issue #254 update metrics

        // a peer this node refused to track must not be announced as connected, or the
        // application routes requests to a peer the manager holds no state for
        if !registered {
            return;
        }

        self.push_event(PeerEvent::PeerConnected(peer_id, multiaddr));

        // log successful connection establishment
        info!(
            target: "network",
            ?endpoint,
            other_established,
            "new connection established",
        );
    }

    /// Handle the connection closed event.
    fn on_connection_closed(
        &mut self,
        peer_id: PeerId,
        _endpoint: &ConnectedPoint,
        remaining_established: usize,
    ) {
        if remaining_established > 0 {
            return;
        }

        // there are no more connections
        if self.is_peer_connected_or_disconnecting(&peer_id) {
            // if the peer's connection status is either `Connected` or `Disconnecting`,
            // ensure the application layer is notified the peer has disconnected
            self.push_event(PeerEvent::PeerDisconnected(peer_id));
            debug!(target: "peer-manager", ?peer_id, "peer disconnected");
        }

        // if this node has too many peers, disconnect from the peer.
        // when this happens, the peer manager still needs to register this peer
        self.register_disconnected(&peer_id);

        // TODO: Issue #254 update metrics
    }

    /// Dial attempt failed.
    ///
    /// NOTE: `AllPeers` is only updated if the peer is _not_ already connected. It's possible that
    /// an outgoing dial attempt fails because the peer connected during the dial.
    fn on_dial_failure(&mut self, peer_id: Option<PeerId>, error: &DialError) {
        if let Some(peer_id) = peer_id {
            if !self.is_connected(&peer_id) {
                self.register_disconnected(&peer_id);
            }

            // return error to dialer
            self.notify_dial_result(&peer_id, Err(error.into()));
        }
    }
}
