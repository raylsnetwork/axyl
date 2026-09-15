//! Manage peer connection status and reputation.

use super::{
    all_peers::AllPeers,
    cache::BannedPeerCache,
    status::{DisconnectReason, NewConnectionStatus},
    types::{ConnectionDirection, ConnectionType, DialRequest, PeerAction},
    PeerEvent, PeerExchangeMap, Penalty,
};
use crate::{
    error::NetworkError,
    peers::status::ConnectionStatus,
    send_or_log_error,
    types::{NetworkInfo, NetworkResult},
};
use libp2p::{kad::PeerInfo, multiaddr::Protocol, Multiaddr, PeerId};
use rand::seq::{IteratorRandom as _, SliceRandom as _};
use rayls_infrastructure_config::PeerConfig;
use rayls_infrastructure_types::{now, BlsPublicKey};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    net::IpAddr,
    task::Context,
};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

#[cfg(test)]
use libp2p::core::ConnectedPoint;

/// Rayls: Maximum known peers to track.
const MAX_KNOWN_PEERS: usize = 2000;

#[cfg(test)]
#[path = "../tests/peer_manager.rs"]
mod peer_manager;

/// The type to manage peers.
pub(crate) struct PeerManager {
    /// Config
    config: PeerConfig,
    /// The interval to perform maintenance.
    heartbeat: tokio::time::Interval,
    /// All peers for the manager.
    peers: AllPeers,
    /// The collection of bls public keys to known peers.
    /// This should incude the current and next couple of committee members network info.
    /// This is used for bootstrapping and to make sure we know the network settings of committee
    /// members.
    known_peers: HashMap<BlsPublicKey, NetworkInfo>,

    /// The time a peer was added to the known peers list. Used for pruning old entries when the
    /// list
    known_peers_time_added: HashMap<BlsPublicKey, u64>,

    /// PeerId -> BlsPublicKey for know peers.
    known_peerids: HashMap<PeerId, BlsPublicKey>,
    /// A queue of events that the `PeerManager` is waiting to produce.
    events: VecDeque<PeerEvent>,
    /// A queue of peers to dial.
    dial_requests: VecDeque<DialRequest>,
    /// Tracks temporarily banned peers to prevent immediate reconnection attempts.
    ///
    /// This LRU cache manages a time-based ban list that operates independently
    /// from the peer's state. Characteristics:
    ///
    /// - Prevents reconnection attempts at the network layer without affecting the peer's stored
    ///   state
    /// - Peers appear to be banned for connection purposes while still having a non-banned state
    ///   in the database
    /// - Ban records persist even after a peer is removed from the database, allowing rejection of
    ///   unknown peers based on previous temporary bans
    /// - Control the time-based LRU cache mechanism by leveraging the PeerManager's heartbeat
    ///   cycle for maintenance instead of requiring separate polling
    /// - The actual ban duration has a resolution limited by the heartbeat interval, as cache
    ///   cleanup occurs during heartbeat events
    ///
    /// The implementation uses `FnvHashSet` instead of the default Rust hasher `SipHash`
    /// for improved performance for short keys.
    temporarily_banned: BannedPeerCache<PeerId>,
    /// Potential peers discovered through kad.
    ///
    /// These peers are not connected and reserved for dial attempts at heartbeat intervals if
    /// connections drop.
    discovery_peers: HashMap<PeerId, Vec<Multiaddr>>,
    /// Consecutive heartbeats with zero connected peers for dial backoff.
    isolation_streak: u32,
    /// Circuit-relay-v2 servers referenced by peers' `/p2p-circuit` addresses.
    ///
    /// Relays only speak the circuit protocol, not the consensus protocols (gossipsub, kad,
    /// req/res), so ordinary peer scoring would immediately ban them and tear down the reservation
    /// and every circuit routed through them. These peer ids are therefore exempt from penalties
    /// and pruning.
    relay_peers: HashSet<PeerId>,
    /// This node's own peer id.
    ///
    /// Used to skip self when re-dialing missing committee members (this node appears in its own
    /// committee and known-peers maps) and to detect whether this node is itself a committee
    /// member.
    local_peer_id: PeerId,
}

impl PeerManager {
    /// Create a new instance of Self.
    pub(crate) fn new(config: &PeerConfig, local_peer_id: PeerId) -> Self {
        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(config.heartbeat_interval));

        let peers = AllPeers::new(
            config.dial_timeout,
            config.max_banned_peers,
            config.max_disconnected_peers,
            config.score_config,
        );
        let temporarily_banned = BannedPeerCache::new(config.excess_peers_reconnection_timeout);

        Self {
            config: *config,
            heartbeat,
            peers,
            known_peers: Default::default(),
            known_peers_time_added: Default::default(),
            known_peerids: Default::default(),
            events: Default::default(),
            dial_requests: Default::default(),
            isolation_streak: 0,
            temporarily_banned,
            discovery_peers: Default::default(),
            relay_peers: Default::default(),
            local_peer_id,
        }
    }

    /// Explicitly add a "trusted" peer and dial it.
    ///
    /// These peers are considered "trusted" and do not receive penalties.
    /// This does not unban ips and should only be called during initialization.
    pub(crate) fn add_trusted_peer_and_dial(
        &mut self,
        bls_key: BlsPublicKey,
        info: NetworkInfo,
        reply: oneshot::Sender<NetworkResult<()>>,
    ) {
        let peer_id: PeerId = info.pubkey.clone().into();
        let multiaddr = info.multiaddrs.clone();
        self.peers.add_trusted_peer(bls_key, info.pubkey.clone(), multiaddr.clone());

        // remove from temporary banned and warn if peer was banned
        if self.temporarily_banned.remove(&peer_id) {
            warn!(target: "peer-manager", ?peer_id, "removed trusted peer from temporarily banned list");
        }
        let peer_id: PeerId = info.pubkey.clone().into();
        debug!(target: "peer-manager", ?peer_id, "Inserting trusted peer into known_peerids");
        self.known_peerids.insert(peer_id, bls_key);
        debug!(target: "peer-manager", ?peer_id, "Inserting trusted peer into known_peers");
        self.known_peers.insert(bls_key, info);
        self.known_peers_time_added.insert(bls_key, now());

        debug!(target: "peer-manager", ?peer_id, "Dialing trusted peer");
        self.dial_peer(peer_id, multiaddr, Some(reply));
    }

    /// Process the request to dial a peer.
    pub(crate) fn dial_peer(
        &mut self,
        peer_id: PeerId,
        multiaddrs: Vec<Multiaddr>,
        reply: Option<oneshot::Sender<NetworkResult<()>>>,
    ) {
        // A circuit dial rides on a direct leg to the relay named in the address; learn that
        // relay before the leg comes up so it is penalty-exempt and classified as a relay
        // connection. Dials resolved at dial time (e.g. `/dnsaddr` failover) may name a relay no
        // earlier ingestion path has seen. No-op for non-circuit addresses.
        self.register_relays_from_addrs(&multiaddrs);
        // return early if peer is banned, connected, or currently being dialed
        if let Some(peer) = self.peers.get_peer(&peer_id) {
            match peer.connection_status() {
                ConnectionStatus::Banned { .. } => {
                    // report error - dial banned peer
                    let error = NetworkError::DialBannedPeer(format!("Peer {peer_id} is banned"));
                    warn!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Peer Banned",
                            peer = peer_id
                        );
                    }
                    return;
                }
                ConnectionStatus::Dialing { .. } => {
                    // report error - dialing already in progress
                    let error = NetworkError::AlreadyDialing(format!("Already dialing {peer_id}"));
                    debug!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Already dialing",
                            peer = peer_id
                        );
                    }
                    return;
                }
                ConnectionStatus::Connected { .. } => {
                    // report error - dialing already connected
                    let error =
                        NetworkError::AlreadyConnected(format!("Already connected {peer_id}"));
                    debug!(target: "peer-manager", ?error, "invalid dial request");
                    if let Some(reply) = reply {
                        send_or_log_error!(
                            reply,
                            Err(error),
                            "DialPeer- Already connected",
                            peer = peer_id
                        );
                    }
                    return;
                }
                _ => { /* ignore */ }
            }
        }
        // schedule swarm to dial peer
        debug!(target: "peer-manager", ?peer_id, "sending dial request to swarm");
        let request = DialRequest { peer_id, multiaddrs, reply };
        self.dial_requests.push_back(request);
    }

    /// Check if this peer is already registered as dialing.
    ///
    /// Self and kad behaviors can initiate dial attempts. This is used to filter pending outbound
    /// connections.
    pub(super) fn dial_attempt_already_registered(&self, peer_id: &PeerId) -> bool {
        self.peers.get_peer(peer_id).is_some_and(|peer| peer.connection_status().is_dialing())
    }

    /// Push a [PeerEvent].
    pub(super) fn push_event(&mut self, event: PeerEvent) {
        self.events.push_back(event);
    }

    /// Register a dial attempt to return the result to caller.
    ///
    /// This method initializes the peer and sets the connection to `Dialing`.
    /// If a dial attempt was already registered, the reply channel is updated.
    pub(super) fn register_dial_attempt(
        &mut self,
        peer_id: PeerId,
        reply: Option<oneshot::Sender<NetworkResult<()>>>,
    ) {
        self.peers.register_dial_attempt(peer_id, reply);
    }

    /// Return the next dial request if it exists.
    pub(super) fn next_dial_request(&mut self) -> Option<DialRequest> {
        self.dial_requests.pop_front()
    }

    /// Notify the caller that a dial attempt was successful.
    pub(super) fn notify_dial_result(&mut self, peer_id: &PeerId, result: NetworkResult<()>) {
        self.peers.notify_dial_result(peer_id, result);
    }

    /// Poll events.
    ///
    /// This method is called when the peer manager is `poll`ed by the swarm.
    /// The next event is returned, unless there are no events to pass to the swarm.
    /// When events are empty, the capacity of the vector is shrunk as much as possible.
    pub(super) fn poll_events(&mut self) -> Option<PeerEvent> {
        if self.events.is_empty() {
            // expect ~32 events
            if self.events.capacity() > 64 {
                self.events.shrink_to(32);
            }
            None
        } else {
            self.events.pop_front()
        }
    }

    /// Returns a boolean indicating if the next instant in the heartbeat interval was reached.
    pub(super) fn heartbeat_ready(&mut self, cx: &mut Context<'_>) -> bool {
        self.heartbeat.poll_tick(cx).is_ready()
    }

    /// Heartbeat maintenance.
    ///
    /// The manager runs routine maintenance to decay penalties for peers. This method
    /// is routine and can not further penalize peers.
    pub(super) fn heartbeat(&mut self) {
        // update peers
        let actions = self.peers.heartbeat_maintenance();
        for (peer_id, action) in actions {
            self.apply_peer_action(peer_id, action);
        }

        // TODO: Issue #254 update metrics

        // enforce connection limits
        self.prune_connected_peers();

        // update timestamps
        self.unban_temp_banned_peers();

        // heal committee connectivity without waiting for the epoch boundary
        self.redial_missing_committee();

        // manage discovery peers
        self.discovery_heartbeat();
    }

    /// Requests a re-dial, via [`PeerEvent::RedialCommittee`], of every current-committee member
    /// that is neither connected nor dialing.
    ///
    /// Committee connectivity must not wait for the epoch boundary: a member whose relay or host
    /// recovers mid-epoch becomes reachable again immediately, while the only other committee
    /// dial paths run at `NewEpoch` or when this node is fully isolated (discovery seeding). One
    /// attempt per member per heartbeat bounds the dial rate; committee members are ban-exempt,
    /// so failed attempts cannot escalate. Only committee members re-dial: an observer with
    /// peers must not pester the committee, matching the epoch-boundary dial policy.
    fn redial_missing_committee(&mut self) {
        if !self.is_peer_validator(&self.local_peer_id) {
            return;
        }
        let connected_or_dialing: HashSet<PeerId> =
            self.connected_or_dialing_peers().into_iter().collect();
        let missing: Vec<BlsPublicKey> = self
            .known_peers
            .iter()
            .filter_map(|(bls_key, info)| {
                let peer_id: PeerId = info.pubkey.clone().into();
                (peer_id != self.local_peer_id
                    && self.is_peer_validator(&peer_id)
                    && !connected_or_dialing.contains(&peer_id))
                .then_some(*bls_key)
            })
            .collect();
        for bls_key in missing {
            debug!(target: "peer-manager", ?bls_key, "requesting re-dial of missing committee member");
            self.events.push_back(PeerEvent::RedialCommittee(bls_key));
        }
    }

    /// Apply a [PeerAction].
    ///
    /// Actions on peers happen when their reputation or connection status changes.
    fn apply_peer_action(&mut self, peer_id: PeerId, action: PeerAction) {
        // this is the single point every reputation-driven action passes through, and each arm
        // fires only on an actual change, so the ban lifecycle is legible at warn/info without the
        // per-penalty decay noise that sits at debug
        match action {
            PeerAction::Ban(ip_addrs) => {
                warn!(target: "peer-manager", ?peer_id, score = ?self.peer_score(&peer_id), ?ip_addrs, "peer banned");
                self.process_ban(&peer_id);
            }
            PeerAction::Disconnect => {
                info!(target: "peer-manager", ?peer_id, score = ?self.peer_score(&peer_id), "peer disconnected for reputation");
                self.temporarily_banned.insert(peer_id);
                self.push_event(PeerEvent::DisconnectPeer(peer_id));
            }
            PeerAction::DisconnectWithPX => {
                debug!(target: "peer-manager", ?peer_id, "reputation update results in temp ban with PX");
                // prevent immediate reconnection attempts
                self.temporarily_banned.insert(peer_id);
                let exchange = self.peers.peer_exchange();
                self.events.push_back(PeerEvent::DisconnectPeerX(peer_id, exchange));
            }
            PeerAction::Unban(ip_addrs) => {
                info!(target: "peer-manager", ?peer_id, ?ip_addrs, "peer unbanned");
                self.push_event(PeerEvent::Unbanned(peer_id));
            }

            PeerAction::NoAction => { /* nothing to do */ }
        }
    }

    /// Returns a boolean indicating if a peer is already connected or disconnecting.
    ///
    /// Used when handling connection closed events from the swarm.
    pub(super) fn is_peer_connected_or_disconnecting(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_connected_or_disconnecting(peer_id)
    }

    /// Returns boolean if the ip address is banned.
    pub(super) fn is_ip_banned(&self, ip: &IpAddr) -> bool {
        self.peers.ip_banned(ip)
    }

    /// Returns a boolean if the peer is a known validator.
    ///
    /// `AllPeers` only tracks CVVs for now. (current voting validators)
    ///
    /// This method will be extended to support any staked validator.
    pub(super) fn is_peer_validator(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_validator(peer_id)
    }

    /// Returns a boolean if the peer is connected.
    pub(crate) fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.peers.get_peer(peer_id).is_some_and(|peer| {
            matches!(peer.connection_status(), ConnectionStatus::Connected { .. })
        })
    }

    /// Check if the peer id is banned or associated with any banned ip addresses.
    ///
    /// This is called before accepting new connections. Also checks that the peer
    /// wasn't temporarily banned due to excess peers connections.
    ///
    /// Rayls: Committee members are exempt from ban checks to allow immediate reconnection
    /// after restart, even if they accumulated penalties before shutdown.
    pub(crate) fn peer_banned(&self, peer_id: &PeerId) -> bool {
        // committee members are never considered banned - allows immediate reconnection
        // after restart even if they accumulated penalties before shutdown
        if self.is_peer_validator(peer_id) {
            trace!(
                target: "peer-manager",
                ?peer_id,
                "committee member exempted from ban check"
            );
            return false;
        }

        // known peers are exempt from temp-ban cache (score-based bans still apply)
        let temp_banned = if self.known_peerids.contains_key(peer_id) {
            false
        } else {
            self.temporarily_banned.contains(peer_id)
        };

        trace!(
            target: "peer-manager",
            ?peer_id,
            "checking if peer banned"
        );
        temp_banned || self.peers.score_or_ip_banned(peer_id)
    }

    #[cfg(test)]
    /// Process new connection and return boolean indicating if the peer limit was reached.
    pub(super) fn peer_limit_reached(&self, endpoint: &ConnectedPoint) -> bool {
        debug!(target: "peer-manager", connected_peers=?self.peers.connected_peer_ids().count(), "checking peer limits");
        if endpoint.is_dialer() {
            // this node dialed peer
            self.peers.connected_peer_ids().count() >= self.config.max_outbound_dialing_peers()
        } else {
            // peer dialed this node
            self.connected_or_dialing_peers().len() >= self.config.max_peers()
        }
    }

    /// Check if the inbound peer limit was reached.
    pub(super) fn peer_inbound_limit_reached(&self) -> bool {
        debug!(target: "peer-manager", connected_peers=?self.peers.connected_peer_ids().count(), "checking peer limits");
        self.connected_or_dialing_peers().len() >= self.config.max_peers()
    }

    /// Return an iterator of peers that are connected or dialed.
    pub(crate) fn connected_or_dialing_peers(&self) -> Vec<PeerId> {
        trace!(target: "peer-manager", "all peers:\n{:?}", self.peers);
        self.peers.connected_or_dialing_peers()
    }

    /// Rayls: Returns only peers with fully established connections.
    pub(crate) fn connected_peers_only(&self) -> Vec<PeerId> {
        self.peers.connected_peers_only()
    }

    /// Process a penalty from the application layer.
    ///
    /// The application layer reports issues from peers that are processed here.
    /// Some reports are propagated to libp2p network layer. Caller is responsible
    /// for specifying the severity of the penalty to apply.
    pub(crate) fn process_penalty(&mut self, peer_id: PeerId, penalty: Penalty) {
        // Relays only speak the circuit protocol, so consensus-layer penalties (e.g. kad/gossip
        // "unsupported protocol") must never ban them - that would drop the reservation and all
        // circuits routed through the relay.
        if self.relay_peers.contains(&peer_id) {
            trace!(target: "peer-manager", ?peer_id, ?penalty, "ignoring penalty for relay peer");
            return;
        }

        let action = self.peers.process_penalty(&peer_id, penalty);

        // Surface the penalty that tipped a peer into a ban. Emitted at warn (not trace, like the
        // step below) because a ban severs connectivity and the bare "peer banned" event otherwise
        // records no cause -- leaving the trigger to be guessed from surrounding logs.
        if matches!(action, PeerAction::Ban(_)) {
            warn!(target: "peer-manager", ?peer_id, ?penalty, "penalty resulted in ban");
        }
        trace!(target: "peer-manager", ?peer_id, ?action, "processed penalty");
        self.apply_peer_action(peer_id, action);
    }

    /// Whether `peer_id` is a known relay server. Relays are exempt from penalties/pruning and are
    /// kept out of the kademlia DHT (they only speak the circuit protocol).
    pub(crate) fn is_relay(&self, peer_id: &PeerId) -> bool {
        self.relay_peers.contains(peer_id)
    }

    /// Record a peer that completed connection setup but does not speak a consensus protocol
    /// (learned authoritatively, e.g. gossipsub's `GossipsubNotSupported`) as protected
    /// infrastructure. In this network a peer that runs none of our consensus protocols is a
    /// circuit relay, so record it the same way as a relay named in a `/p2p-circuit` address:
    /// exempt from penalties and kept out of the kademlia DHT. This is the discovery-independent
    /// counterpart to [`Self::register_relays_from_addrs`]; it catches relays we only ever saw via
    /// a bare direct address (a kad/identify leak), which the address-based path cannot.
    ///
    /// Returns `false` without recording when `peer_id` is a known committee validator: a
    /// validator that fails consensus-protocol negotiation is a real protocol/version fault the
    /// caller must surface, never silently exempt.
    pub(crate) fn mark_relay_peer(&mut self, peer_id: PeerId) -> bool {
        if self.is_peer_validator(&peer_id) {
            return false;
        }
        if self.relay_peers.insert(peer_id) {
            debug!(target: "peer-manager", ?peer_id, "recorded non-consensus peer as relay (exempt from penalties)");
        }
        true
    }

    /// Record the relay servers referenced by any `/p2p-circuit` addresses so they are treated as
    /// protected infrastructure (never penalized or pruned).
    pub(crate) fn register_relays_from_addrs(&mut self, addrs: &[Multiaddr]) {
        for addr in addrs {
            if let Some(relay_id) = crate::types::circuit_relay_peer_id(addr) {
                if self.relay_peers.insert(relay_id) {
                    debug!(target: "peer-manager", ?relay_id, "registered relay peer (exempt from penalties)");
                }
            }
        }
    }

    /// Process newly banned IP addresses.
    ///
    /// The peer is disconnected and is banned from network layer.
    fn process_ban(&mut self, peer_id: &PeerId) {
        // ensure unbanned events are removed for this peer
        self.events.retain(|event| {
            if let PeerEvent::Unbanned(unbanned_peer_id) = event {
                unbanned_peer_id != peer_id
            } else {
                true
            }
        });

        // push banned event
        self.events.push_back(PeerEvent::Banned(*peer_id));
    }

    /// Disconnect from a peer.
    ///
    /// This is the recommended graceful disconnect method and is called when peers
    /// are penalized or if connecting with a dialing peer would result in excess peer
    /// count.
    ///
    /// The argument `support_discovery` indicates if the disconnect message should
    /// include additional connected peers to help the peer discovery other nodes.
    /// Peers that are disconnected because of excess peer limits support discovery.
    pub(crate) fn disconnect_peer(&mut self, peer_id: PeerId, support_discovery: bool) {
        // include peer exchange or not
        let event = if support_discovery {
            let exchange = self.peers.peer_exchange();
            PeerEvent::DisconnectPeerX(peer_id, exchange)
        } else {
            PeerEvent::DisconnectPeer(peer_id)
        };

        self.events.push_back(event);
        let action = self.peers.update_connection_status(
            &peer_id,
            NewConnectionStatus::Disconnecting { reason: DisconnectReason::ExcessPeers },
        );

        debug!(target: "peer-manager", ?action, "disconnect peer results in:");
        self.apply_peer_action(peer_id, action);
    }

    /// Register a connected peer if their reputation is sufficient.
    ///
    /// Returns a boolean if the peer was successfully registered. This is the initial
    /// method to call for registering a new peer through dialing or incoming connections.
    ///
    /// A refusal closes the connection: the admission gates share this method's predicate, so a
    /// peer reaching here banned means the ban landed mid-handshake, and leaving that connection
    /// open would route application traffic to a peer this manager does not track.
    pub(super) fn register_peer_connection(
        &mut self,
        peer_id: &PeerId,
        connection: ConnectionType,
    ) -> bool {
        if self.peer_banned(peer_id) {
            error!(target: "peer-manager", ?peer_id, "connected with banned peer");
            self.push_event(PeerEvent::DisconnectPeer(*peer_id));
            return false;
        }

        let (multiaddr, con_type) = match connection {
            ConnectionType::IncomingConnection { multiaddr } => {
                (multiaddr, ConnectionDirection::Incoming)
            }
            ConnectionType::OutgoingConnection { multiaddr } => {
                // this node dials for outgoing connections
                self.notify_dial_result(peer_id, Ok(()));

                (multiaddr, ConnectionDirection::Outgoing)
            }
        };

        self.peers.update_connection_status(
            peer_id,
            NewConnectionStatus::Connected { multiaddr, direction: con_type },
        );

        // self.add_peer_metrics(peer_id, self.peers.get_peer(peer_id));

        true
    }

    /// Register disconnected peers.
    ///
    /// Some peers are disconnected with the intention to ban that peer.
    /// This method registers the peer as disconnected and ensures the list of banned/disconnected
    /// peers doesn't grow infinitely large. Peers may become "unbanned" if the limit for banned
    /// peers is reached.
    pub(super) fn register_disconnected(&mut self, peer_id: &PeerId) {
        let (action, pruned_peers) = self.peers.register_disconnected(peer_id);

        debug!(target: "peer-manager", ?action, ?pruned_peers, ?peer_id, "register disconnected");

        // banning is the only action that happens after disconnect
        // if the peer is banned then manager needs to apply the ban still
        // otherwise, there is no other action to take
        if action.is_ban() {
            debug!(target: "peer-manager", ?peer_id, "processing ban");
            self.apply_peer_action(*peer_id, action);
        }

        // process pruned peers
        self.events
            .extend(pruned_peers.into_iter().map(|(peer_id, _)| PeerEvent::Unbanned(peer_id)));
    }

    /// Prune peers to reach target peer counts.
    ///
    /// Trusted peers and validators are ignored. Peers are sorted from lowest to highest score and
    /// removed until excess peer count reaches target.
    fn prune_connected_peers(&mut self) {
        // connected peers sorted from lowest to highest aggregate score
        // peers that do not participate in the kad routing table are prioritized for disconnect
        let connected_peers = self.peers.connected_peers_by_score_and_routability();
        let mut excess_peer_count =
            connected_peers.len().saturating_sub(self.config.target_num_peers);
        if excess_peer_count == 0 {
            // no excess peers
            return;
        }

        // filter peers that are validators
        let ready_to_prune = connected_peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                if !self.is_peer_validator(peer_id)
                    && !peer.is_trusted()
                    && !self.relay_peers.contains(peer_id)
                {
                    Some(**peer_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // disconnect peers until excess_peer_count is 0 or no more peers
        for peer_id in ready_to_prune {
            if excess_peer_count > 0 {
                self.disconnect_peer(peer_id, true);
                excess_peer_count = excess_peer_count.saturating_sub(1);
                continue;
            }

            // excess peers 0 - finish pruning
            break;
        }
    }

    /// Unban temporarily banned peers.
    ///
    /// Peers are temporarily "banned" when trying to connect while this node has excess peers.
    fn unban_temp_banned_peers(&mut self) {
        for peer_id in self.temporarily_banned.heartbeat() {
            self.push_event(PeerEvent::Unbanned(peer_id));
        }
    }

    /// Process peer exchange for peer discovery.
    ///
    /// This method is called when a peer disconnects immediately from this node due to having too
    /// many peers. The disconnecting peer shares information about other known peers to
    /// facilitate discovery.
    ///
    /// Peers should be wary of these reported peers (eclipse attacks). Peers discovered through
    /// kademlia are prioritized over peer exchange by only processing up to the missing target
    /// number of discovery peers from exchange map.
    pub(crate) fn process_peer_exchange(&mut self, peers: PeerExchangeMap) {
        // check if discovery peers needed
        let max_discovery_peers = self.config.max_discovery_peers();
        let current_count = self.discovery_peers.len();

        // seed discovery peers from peer exchange
        if current_count < max_discovery_peers {
            // convert eligible peers to `PeerInfo` for processing
            let mut peers: Vec<_> = peers
                .into_iter()
                .filter_map(|(_, (net_key, addrs))| {
                    let info =
                        PeerInfo { peer_id: net_key.into(), addrs: addrs.into_iter().collect() };

                    // filter out ineligible peers
                    if self.eligible_for_discovery(&info) {
                        debug!(target: "peer-manager", ?info, "peer exchange eligible");
                        Some(info)
                    } else {
                        debug!(target: "peer-manager", peer=?self.peers.get_peer(&info.peer_id), ?info, "peer exchange ineligible");
                        None
                    }
                })
                .collect();

            debug!(target: "peer-manager", eligible=?peers, "processing peer exchange");

            // shuffle all peers
            let mut rng = rand::rng();
            peers.shuffle(&mut rng);

            // add target number of peers for discovery
            let peers_to_take = max_discovery_peers - current_count;
            for peer in peers.into_iter().take(peers_to_take) {
                debug!(target: "peer-manager", peer=?peer.peer_id, "added peer to discovery peers");
                self.discovery_peers.insert(peer.peer_id, peer.addrs);
            }
        }
    }

    /// Create [PeerExchangeMap] for exchange with peers.
    pub(crate) fn peers_for_exchange(&self) -> PeerExchangeMap {
        self.peers.peer_exchange()
    }

    /// Return the score for a peer if they exist.
    pub(crate) fn peer_score(&self, peer_id: &PeerId) -> Option<f64> {
        self.peers.get_peer(peer_id).map(|peer| peer.score().aggregate_score())
    }

    /// Bool indicating if the peer is trusted or a validator.
    pub(crate) fn peer_is_important(&self, peer_id: &PeerId) -> bool {
        self.peers.is_peer_recently_validator(peer_id)
            || self.peers.get_peer(peer_id).map(|p| p.is_trusted()).unwrap_or_default()
    }

    /// Update the committee for the new epoch.
    pub(crate) fn new_epoch(&mut self, committee: HashSet<BlsPublicKey>) {
        // remove from temporary banned and warn if validator was banned
        //
        // every committee key is forwarded, resolved or not: membership is decided by stake, so a
        // member this node has not discovered yet must still be recognized the moment its record
        // arrives rather than at the next boundary
        let mut exp_committee = Vec::with_capacity(committee.len());
        for bls_key in &committee {
            let Some(NetworkInfo { pubkey, multiaddrs: multiaddr, timestamp }) =
                self.known_peers.get(bls_key)
            else {
                warn!(target: "peer-manager", "unknown committee member with key {bls_key}");
                exp_committee.push((*bls_key, None));
                continue;
            };

            let peer_id: PeerId = pubkey.clone().into();
            info!(target: "peer-manager", "adding committee member {bls_key}/{peer_id}");
            if self.temporarily_banned.remove(&peer_id) {
                warn!(target: "peer-manager", ?peer_id, "removed committee member from temporarily banned list");
            }
            exp_committee.push((
                *bls_key,
                Some(NetworkInfo {
                    pubkey: pubkey.clone(),
                    multiaddrs: multiaddr.clone(),
                    timestamp: *timestamp,
                }),
            ));
        }

        // add trusted peer record
        let unban_actions = self.peers.new_epoch(exp_committee);

        // apply unban for any banned validators
        for (peer_id, action) in unban_actions {
            self.apply_peer_action(peer_id, action);
        }
    }

    /// Add a known peer to the known list.
    /// Used for bootstrap servers or possibly committee members.
    pub(crate) fn add_known_peer(&mut self, bls_key: BlsPublicKey, info: NetworkInfo) {
        let peer_id: PeerId = info.pubkey.clone().into();
        trace!(
            target: "peer-manager",
            ?bls_key,
            ?peer_id,
            known_peerids_len = self.known_peerids.len(),
            "add_known_peer",
        );
        self.peers.upsert_peer(bls_key, info.pubkey.clone(), info.multiaddrs.clone());

        // A member banned before discovery resolved it still carries its ban after `upsert_peer`
        // trusts it; release it so the unban action repairs the gossipsub blacklist and kad routing
        // entry. Not in `upsert_peer`: `new_epoch` also calls it and does its own boundary unban.
        if self.peers.is_peer_validator(&peer_id)
            && self.peers.get_peer(&peer_id).is_some_and(|p| p.connection_status().is_banned())
        {
            let action =
                self.peers.update_connection_status(&peer_id, NewConnectionStatus::Unbanned);
            self.apply_peer_action(peer_id, action);
        }

        self.known_peers.insert(bls_key, info.clone());
        self.known_peers_time_added.insert(bls_key, now());
        self.known_peerids.insert(peer_id, bls_key);

        // Learn the relay servers this peer is reached through so they are exempt from banning.
        self.register_relays_from_addrs(&info.multiaddrs);

        // Cleanup if we've exceeded the maximum known peers limit
        self.cleanup_known_peers();
    }

    /// Snapshot the dial targets in `known_peers` as `(peer_id, multiaddr)` pairs (one per
    /// advertised address). Used to expose what this node will redial -- including peers it has
    /// not connected to -- for the `advertised_peer_addr_*` metric.
    ///
    /// This node is in its own committee, so `known_peers` contains itself; skip it (as `redial`
    /// does) so the node's own address is not reported as a dial target -- otherwise it would show
    /// as a phantom "never connected" entry in the diff against the routing table.
    pub(crate) fn known_peer_addrs(&self) -> Vec<(PeerId, Multiaddr)> {
        self.known_peers
            .values()
            .flat_map(|info| {
                let peer_id: PeerId = info.pubkey.clone().into();
                info.multiaddrs.iter().map(move |addr| (peer_id, addr.clone()))
            })
            .filter(|(peer_id, _)| *peer_id != self.local_peer_id)
            .collect()
    }

    /// Snapshot the kad-discovery dial candidates in `discovery_peers` as `(peer_id, multiaddr)`
    /// pairs. These are peers learned from other nodes' routing tables via `get_closest_peers` and
    /// dialed on the heartbeat -- the path by which a cross-host-unreachable address (e.g. a
    /// co-located peer's `127.0.0.1`) becomes dial churn. Exposed for the `discovery_peer_addr_*`
    /// metric.
    pub(crate) fn discovery_peer_addrs(&self) -> Vec<(PeerId, Multiaddr)> {
        self.discovery_peers
            .iter()
            .flat_map(|(peer_id, addrs)| addrs.iter().map(move |addr| (*peer_id, addr.clone())))
            .collect()
    }

    /// Rayls: Remove oldest known peers when exceeding maximum size.
    fn cleanup_known_peers(&mut self) {
        if self.known_peers.len() <= MAX_KNOWN_PEERS {
            return;
        }

        // Find the oldest entries to remove based on timestamp
        let entries_to_remove = self.known_peers.len() - MAX_KNOWN_PEERS;

        let mut entries = self
            .known_peers_time_added
            .iter()
            .map(|(k, timestamp)| (*k, *timestamp))
            .collect::<Vec<_>>();

        entries.sort_by_key(|(_, timestamp)| *timestamp);

        // Remove the oldest entries (but never remove validators)
        let mut removed = 0;
        for (bls_key, _) in entries.iter().take(entries_to_remove * 2) {
            // both maps should match so entry should exist in known_peers
            let pubkey = self.known_peers.get(bls_key).unwrap().pubkey.clone();
            // Don't remove if this is a validator in the current committee
            if self.is_peer_validator(&pubkey.clone().into()) {
                continue;
            }

            if let Some(info) = self.known_peers.remove(bls_key) {
                self.known_peers_time_added.remove(bls_key);
                let peer_id: PeerId = info.pubkey.into();
                self.known_peerids.remove(&peer_id);
                removed += 1;

                if removed >= entries_to_remove {
                    break;
                }
            }
        }

        if removed > 0 {
            trace!(
                target: "peer-manager",
                removed,
                remaining = self.known_peers.len(),
                "cleaned up known peers"
            );
        }
    }

    /// Find authorities for the epoch manager.
    pub(crate) fn find_authorities(&mut self, authorities: Vec<BlsPublicKey>) {
        let mut missing = Vec::new();

        // check all peers for authority and track missing
        for bls_key in authorities {
            // identify missing authorities
            if !self.known_peers.contains_key(&bls_key) {
                missing.push(bls_key);
            }
        }

        // emit event for kad to try to discover
        trace!(target: "peer-manager", ?missing, "requesting kad records");
        self.events.push_back(PeerEvent::MissingAuthorities(missing));
    }

    /// Find the peer id for an authority.
    pub(crate) fn auth_to_peer(&self, bls_key: BlsPublicKey) -> Option<(PeerId, Vec<Multiaddr>)> {
        if let Some(NetworkInfo { pubkey, multiaddrs, .. }) = self.known_peers.get(&bls_key) {
            Some((pubkey.clone().into(), multiaddrs.clone()))
        } else {
            debug!(target: "peer-manager", ?bls_key, "unknown peer for bls key");
            None
        }
    }

    /// Find the BlsPublicKey for a known PeerId.
    pub(crate) fn peer_to_bls(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.known_peerids.get(peer_id).copied()
    }

    /// Return the number of PeerId → BLS mappings currently held.
    pub(crate) fn known_peerids_len(&self) -> usize {
        self.known_peerids.len()
    }

    /// Extract IP addresses from multiaddrs and check if any are banned.
    ///
    /// Returns `true` if the peer has valid IP addresses and NONE are banned.
    /// Returns `false` if no valid IPs found OR any IP is banned.
    pub(super) fn has_valid_unbanned_ips(&self, multiaddrs: &[Multiaddr]) -> bool {
        let mut found_valid_ip = false;

        for addr in multiaddrs {
            if let Some(ip) = Self::extract_ip_from_multiaddr(addr) {
                found_valid_ip = true;
                if self.is_ip_banned(&ip) {
                    return false; // Early return on first banned IP
                }
            }
        }

        found_valid_ip
    }

    /// Extract IP address from a single multiaddr.
    ///
    /// Only supports IPv4 and IPv6.
    fn extract_ip_from_multiaddr(addr: &Multiaddr) -> Option<IpAddr> {
        addr.iter().find_map(|protocol| match protocol {
            Protocol::Ip4(ip) => Some(IpAddr::V4(ip)),
            Protocol::Ip6(ip) => Some(IpAddr::V6(ip)),
            _ => None,
        })
    }

    /// Check if peer is eligible for discovery.
    ///
    /// A peer is eligible if:
    /// - it has at least one valid ip address (ipv4/ipv6)
    /// - none of its ip addresses are banned
    /// - it can be dialed (not connected/dialing/banned)
    fn eligible_for_discovery(&self, info: &PeerInfo) -> bool {
        self.has_valid_unbanned_ips(&info.addrs) && self.peers.can_dial(&info.peer_id)
    }

    /// Process newly discovered peers for potential dial attempts.
    ///
    /// Only eligible peers are stored for dialing during heartbeat.
    /// Enforces size limits to prevent unbounded growth between heartbeats.
    pub(crate) fn process_peers_for_discovery(&mut self, mut peers: Vec<PeerInfo>) {
        peers.retain(|peer| self.eligible_for_discovery(peer));

        // Only add peers not already in the discovery map to prevent duplicates
        let max_discovery = self.config.max_discovery_peers();
        for info in peers {
            // Skip if already at max capacity - enforce strict limit to prevent
            // unbounded growth between heartbeat cleanup cycles
            if self.discovery_peers.len() >= max_discovery {
                break;
            }
            // Skip duplicates
            if let Entry::Vacant(e) = self.discovery_peers.entry(info.peer_id) {
                e.insert(info.addrs);
            }
        }
        trace!(target: "peer-manager", count = self.discovery_peers.len(), "discovery peers after processing");
    }

    /// Check peer counts and initiate dial attempts to maintain connection targets.
    fn discovery_heartbeat(&mut self) {
        // take discovery peers and filter ineligble peers
        let mut discovery_peers = std::mem::take(&mut self.discovery_peers);
        discovery_peers.retain(|peer_id, addrs| {
            let peer_info = PeerInfo { peer_id: *peer_id, addrs: addrs.clone() };
            self.eligible_for_discovery(&peer_info)
        });

        // calculate dial attempts needed for target connection limits
        let connected_or_dialing = self.connected_or_dialing_peers().len();

        // track isolation for backoff: when completely disconnected, avoid
        // dial storms that accumulate penalties and trigger permanent bans.
        let mut skip_dial = false;
        if connected_or_dialing == 0 {
            self.isolation_streak = self.isolation_streak.saturating_add(1);

            // exponential backoff: skip increasingly many heartbeats (2, 4, 8)
            if self.isolation_streak > 1 {
                let backoff = 1u32 << (self.isolation_streak - 1).min(3);
                if self.isolation_streak % backoff != 1 {
                    skip_dial = true;
                }
            }

            // seed from known_peers when discovery is empty
            if !skip_dial && discovery_peers.is_empty() {
                for (_bls, info) in &self.known_peers {
                    let peer_id: PeerId = info.pubkey.clone().into();
                    if self.peer_banned(&peer_id) {
                        continue;
                    }
                    // A `/dnsaddr` member MUST be reached through the resolving `DialBls` path,
                    // which resolves it to a concrete `/p2p-circuit` before dialing. Raw-dialing
                    // the unresolved `/dnsaddr` here opens a non-circuit connection that
                    // `sanitize_ip_addr` denies ("no valid unbanned IP"); the resulting
                    // `on_dial_failure` then `register_disconnected`s the peer, racing with and
                    // tearing down the circuit the proper path just established. Committee members
                    // are re-dialed via `redial_missing_committee`, so seed only concrete
                    // (already-dialable) addresses and drop `/dnsaddr` ones.
                    let dialable: Vec<Multiaddr> = info
                        .multiaddrs
                        .iter()
                        .filter(|a| !crate::types::is_dnsaddr(a))
                        .cloned()
                        .collect();
                    if !dialable.is_empty() {
                        discovery_peers.insert(peer_id, dialable);
                    }
                }
                if !discovery_peers.is_empty() {
                    warn!(
                        target: "peer-manager",
                        seeded = discovery_peers.len(),
                        streak = self.isolation_streak,
                        "node isolated - seeding discovery from known peers"
                    );
                }
            }
        } else {
            self.isolation_streak = 0;
        }
        let peers_needed = self.config.target_num_peers.saturating_sub(connected_or_dialing);

        // used for random selections
        let mut rng = rand::rng();

        // initiate dial attempts (skip during isolation backoff)
        if peers_needed > 0 && !skip_dial {
            // randomly select peers to dial
            let to_dial: Vec<(PeerId, Vec<Multiaddr>)> = discovery_peers
                .iter()
                .map(|(id, addrs)| (*id, addrs.clone()))
                .choose_multiple(&mut rng, peers_needed);

            // remove from discovery and dial discovery candidate
            for (peer, addrs) in to_dial {
                debug!(target: "peer-manager", ?peer, "dialing peer for discovery");
                discovery_peers.remove(&peer);
                self.dial_peer(peer, addrs, None);
            }
        }

        // manage target discovery peer counts
        let max_discovery_peers = self.config.max_discovery_peers();
        let current_count = discovery_peers.len();
        if current_count > max_discovery_peers {
            debug!(target: "peer-manager", "pruning discovery peers");
            // prune excess
            let excess = current_count - max_discovery_peers;
            let to_remove: Vec<PeerId> =
                discovery_peers.keys().copied().choose_multiple(&mut rng, excess);
            for peer in to_remove {
                discovery_peers.remove(&peer);
            }

            debug!(
                target: "peer-manager",
                pruned = excess,
                remaining = discovery_peers.len(),
                "pruned excess discovery peers"
            );
        } else if current_count < max_discovery_peers {
            // emit discovery event to find closest peers
            debug!(target: "peer-manager", "discovery peers low");
            self.events.push_back(PeerEvent::Discovery);
        }

        // store discovery peers
        self.discovery_peers = discovery_peers;
    }

    /// Update a peer's status in the routing table.
    pub(crate) fn update_routing_for_peer(&mut self, peer_id: &PeerId, routable: bool) {
        self.peers.update_routing_for_peer(peer_id, routable);
    }
}
