use crate::{
    codec::RLMessage,
    error::NetworkError,
    send_or_log_error,
    types::{NetworkCommand, NetworkEvent, NetworkInfo, NetworkResult},
    ConsensusNetwork,
};
use libp2p::{
    gossipsub::{IdentTopic, Topic, TopicHash},
    multiaddr::Protocol,
    Multiaddr,
};
use rayls_infrastructure_types::{now, Database, RaylsSender};
use tracing::{debug, error, info, warn};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Process commands for the network.
    pub(super) fn process_command(
        &mut self,
        command: NetworkCommand<Req, Res>,
    ) -> NetworkResult<()> {
        match command {
            NetworkCommand::UpdateAuthorizedPublishers { authorities, reply } => {
                // this value should be updated at the start of each epoch
                self.authorized_publishers = authorities;
                send_or_log_error!(reply, Ok(()), "UpdateAuthorizedPublishers");
            }
            NetworkCommand::StartListening { multiaddr, reply } => {
                // When listening on a relay circuit (a node may reserve on several relays for
                // failover), protect that relay from banning/pruning so we don't tear down our own
                // reservation. No-op for direct (non-circuit) listen addresses.
                self.swarm
                    .behaviour_mut()
                    .peer_manager
                    .register_relays_from_addrs(std::slice::from_ref(&multiaddr));
                let is_relayed = multiaddr.iter().any(|p| matches!(p, Protocol::P2pCircuit));
                let res = self.swarm.listen_on(multiaddr.clone());
                // Track relay reservations so we can re-establish them if the relay drops and
                // later comes back (see `retry_relay_reservations`).
                if is_relayed {
                    self.relay_reservations.insert(multiaddr, res.as_ref().ok().copied());
                }
                send_or_log_error!(reply, res, "StartListening");
            }
            NetworkCommand::GetListener { reply } => {
                let addrs = self.swarm.listeners().cloned().collect();
                send_or_log_error!(reply, addrs, "GetListeners");
            }
            NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, network_pubkey, addr, reply } => {
                // update peer manager
                self.swarm.behaviour_mut().peer_manager.add_trusted_peer_and_dial(
                    bls_pubkey,
                    NetworkInfo {
                        pubkey: network_pubkey,
                        multiaddrs: vec![addr],
                        timestamp: now(),
                    },
                    reply,
                );
            }
            NetworkCommand::AddExplicitPeer { bls_pubkey, network_pubkey, addr, reply } => {
                // update peer manager
                self.swarm.behaviour_mut().peer_manager.add_known_peer(
                    bls_pubkey,
                    NetworkInfo {
                        pubkey: network_pubkey,
                        multiaddrs: vec![addr],
                        timestamp: now(),
                    },
                );
                let _ = reply.send(Ok(()));
            }
            NetworkCommand::AddBootstrapPeers { peers, reply } => {
                // update peer manager
                let mut dnsaddrs = Vec::new();
                let pm = &mut self.swarm.behaviour_mut().peer_manager;
                for (bls, info) in peers {
                    // Collect /dnsaddr advertise addresses so we can resolve them below and exempt
                    // the relays they're reached through (their committee address carries no
                    // circuit, so `add_known_peer` alone can't learn those relays).
                    if crate::types::is_dnsaddr(&info.network_address) {
                        dnsaddrs.push(info.network_address.clone());
                    }
                    pm.add_known_peer(
                        bls,
                        NetworkInfo {
                            pubkey: info.network_key,
                            multiaddrs: vec![info.network_address],
                            timestamp: now(),
                        },
                    );
                }
                // Resolve /dnsaddr peers and register the relays we dial through as protected.
                //
                // This MUST NOT run on the swarm loop: `txt_lookup().await` blocks the loop for as
                // long as the resolver takes, and while blocked the swarm cannot service relayed
                // (yamux-over-circuit) connections -- which, unlike direct QUIC, have no
                // transport-level keep-alive -- so peers reset them and consensus connectivity
                // churns. Instead resolve in a detached task and hand the results back via
                // `RegisterRelays`, which registers cheaply on the loop. The exemption may land
                // slightly after the first dials; that's fine (relays aren't banned instantly and
                // penalties decay), and it's far better than stalling the loop.
                if !dnsaddrs.is_empty() {
                    let resolver = self.relay_resolver.clone();
                    let handle = self.handle.clone();
                    self.task_spawner.spawn_task("dnsaddr-relay-discovery", async move {
                        let circuits = resolve_relay_circuits(&resolver, &dnsaddrs).await;
                        if !circuits.is_empty() {
                            let _ = handle.send(NetworkCommand::RegisterRelays { circuits }).await;
                        }
                    });
                }
                let _ = reply.send(Ok(()));
            }
            NetworkCommand::RegisterRelays { circuits } => {
                // Cheap, non-blocking: just records relay peer ids as protected. Sent by the
                // off-loop `/dnsaddr` discovery task spawned in `AddBootstrapPeers`.
                self.swarm.behaviour_mut().peer_manager.register_relays_from_addrs(&circuits);
            }
            NetworkCommand::Dial { peer_id, peer_addr, reply } => {
                self.swarm.behaviour_mut().peer_manager.dial_peer(
                    peer_id,
                    vec![peer_addr],
                    Some(reply),
                );
            }
            NetworkCommand::DialBls { bls_key, reply } => {
                debug!(target: "network", "command for dial bls {bls_key}");
                let Some((peer_id, addrs)) =
                    self.swarm.behaviour().peer_manager.auth_to_peer(bls_key)
                else {
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                    return Ok(());
                };
                // Split committee `/dnsaddr` addresses from any already-concrete ones.
                let (dnsaddrs, concrete): (Vec<_>, Vec<_>) =
                    addrs.into_iter().partition(crate::types::is_dnsaddr);
                if dnsaddrs.is_empty() {
                    // Nothing to resolve; dial the concrete addresses directly.
                    self.swarm.behaviour_mut().peer_manager.dial_peer(
                        peer_id,
                        concrete,
                        Some(reply),
                    );
                } else {
                    // Resolve `/dnsaddr` -> concrete `/p2p-circuit` addresses at dial time, OFF the
                    // swarm loop. This must NOT be dialed as `/dnsaddr`: the relay client behaviour
                    // picks its connection handler from the multiaddr shape (`is_relayed`), so it
                    // has to see the `/p2p-circuit`; dialing `/dnsaddr` (resolved only inside the
                    // DNS transport) makes it treat the relayed connection as a direct link to a
                    // relay and reset it. Resolving here (rather than caching) keeps it always
                    // fresh -- a reconnect picks up whatever relays DNS currently advertises, which
                    // is how failover works. The DNS lookup is async, hence the detached task.
                    let resolver = self.relay_resolver.clone();
                    let handle = self.handle.clone();
                    self.task_spawner.spawn_task("dial-resolve-dnsaddr", async move {
                        let mut resolved = resolve_relay_circuits(&resolver, &dnsaddrs).await;
                        // A `/dnsaddr` host advertises circuits for several peer ids (e.g. a
                        // validator's primary and worker); keep only those that terminate at the
                        // peer we're dialing.
                        resolved.retain(
                            |c| matches!(c.iter().last(), Some(Protocol::P2p(id)) if id == peer_id),
                        );
                        let mut all = concrete;
                        all.extend(resolved);
                        if all.is_empty() {
                            let _ = reply.send(Err(NetworkError::Dial(
                                "no circuit addresses resolved from /dnsaddr".to_string(),
                            )));
                        } else {
                            let _ = handle
                                .send(NetworkCommand::DialResolved { peer_id, addrs: all, reply })
                                .await;
                        }
                    });
                }
            }
            NetworkCommand::DialResolved { peer_id, addrs, reply } => {
                self.swarm.behaviour_mut().peer_manager.dial_peer(peer_id, addrs, Some(reply));
            }
            NetworkCommand::LocalPeerId { reply } => {
                let peer_id = *self.swarm.local_peer_id();
                send_or_log_error!(reply, peer_id, "LocalPeerId");
            }
            NetworkCommand::Publish { topic, msg, reply } => {
                let topic_hash = TopicHash::from_raw(topic.clone());
                let mesh_peers: Vec<_> =
                    self.swarm.behaviour_mut().gossipsub.mesh_peers(&topic_hash).cloned().collect();
                let all_topic_peers: Vec<_> = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .all_peers()
                    .filter(|(_, topics)| topics.contains(&&topic_hash))
                    .map(|(peer_id, _)| *peer_id)
                    .collect();

                let res = self.swarm.behaviour_mut().gossipsub.publish(topic_hash, msg);

                if res.is_err() {
                    warn!(
                        target: "network::gossipsub",
                        ?topic,
                        ?res,
                        mesh_peer_count = mesh_peers.len(),
                        ?mesh_peers,
                        all_topic_peer_count = all_topic_peers.len(),
                        ?all_topic_peers,
                        connected_peers = self.connected_peers.len(),
                        "gossipsub PUBLISH FAILED"
                    );
                } else {
                    debug!(
                        target: "network::gossipsub",
                        ?topic,
                        mesh_peer_count = mesh_peers.len(),
                        all_topic_peer_count = all_topic_peers.len(),
                        connected_peers = self.connected_peers.len(),
                        "gossipsub publish OK"
                    );
                }

                send_or_log_error!(reply, res, "Publish");
            }
            NetworkCommand::Subscribe { topic, publishers, reply } => {
                let sub: IdentTopic = Topic::new(&topic);
                let res = self.swarm.behaviour_mut().gossipsub.subscribe(&sub);
                self.authorized_publishers.insert(topic.clone(), publishers);

                let topic_hash: TopicHash = sub.into();
                let mesh_peers: Vec<_> =
                    self.swarm.behaviour_mut().gossipsub.mesh_peers(&topic_hash).cloned().collect();
                let all_topic_peers: Vec<_> = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .all_peers()
                    .filter(|(_, topics)| topics.contains(&&topic_hash))
                    .map(|(peer_id, _)| *peer_id)
                    .collect();

                info!(
                    target: "network::gossipsub",
                    ?topic,
                    ?res,
                    mesh_peer_count = mesh_peers.len(),
                    ?mesh_peers,
                    all_topic_peer_count = all_topic_peers.len(),
                    ?all_topic_peers,
                    connected_peers = self.connected_peers.len(),
                    "SUBSCRIBE to topic"
                );

                send_or_log_error!(reply, res, "Subscribe");
            }
            NetworkCommand::ConnectedPeerIds { reply } => {
                let res = self.swarm.behaviour().peer_manager.connected_peers_only();
                debug!(target: "network", ?res, "peer manager connected peers:");
                send_or_log_error!(reply, res, "ConnectedPeers");
            }
            NetworkCommand::ConnectedPeers { reply } => {
                let peers = self
                    .swarm
                    .behaviour()
                    .peer_manager
                    .connected_or_dialing_peers()
                    .iter()
                    .flat_map(|id| self.swarm.behaviour().peer_manager.peer_to_bls(id))
                    .collect();
                debug!(target: "network", ?peers, "peer manager connected peers:");
                send_or_log_error!(reply, peers, "ConnectedPeers");
            }
            NetworkCommand::PeerScore { peer_id, reply } => {
                let opt_score = self.swarm.behaviour().peer_manager.peer_score(&peer_id);
                send_or_log_error!(reply, opt_score, "PeerScore");
            }
            NetworkCommand::AllPeers { reply } => {
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .all_peers()
                    .map(|(peer_id, vec)| (*peer_id, vec.into_iter().cloned().collect()))
                    .collect();

                send_or_log_error!(reply, collection, "AllPeers");
            }
            NetworkCommand::MeshPeers { topic, reply } => {
                let topic: IdentTopic = Topic::new(&topic);
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .mesh_peers(&topic.into())
                    .cloned()
                    .collect();
                send_or_log_error!(reply, collection, "MeshPeers");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                debug!(target: "network", "send request for bls {peer}");
                if let Some((peer_id, addr)) =
                    self.swarm.behaviour().peer_manager.auth_to_peer(peer)
                {
                    // Rayls: Check if peer is actually connected before sending to avoid
                    // OutboundFailure spam from sending to known but not-yet-connected peers
                    if !self.swarm.behaviour().peer_manager.is_connected(&peer_id) {
                        debug!(
                            target: "network",
                            ?peer_id,
                            "request delayed - peer not yet connected"
                        );
                        let _ = reply.send(Err(NetworkError::PeerNotConnected));
                        return Ok(());
                    }

                    debug!(target: "network", "trying to send to {peer_id} at {addr:?}");
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .req_res
                        .send_request_with_addresses(&peer_id, request, addr);
                    self.outbound_requests.insert((peer_id, request_id), reply);
                } else {
                    // Best effort to return an error to caller.
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::SendRequestDirect { peer, request, reply } => {
                let request_id = self.swarm.behaviour_mut().req_res.send_request(&peer, request);
                self.outbound_requests.insert((peer, request_id), reply);
            }
            NetworkCommand::SendRequestAny { request, reply } => {
                // Rotating an empty list will panic...
                if !self.connected_peers.is_empty() {
                    self.connected_peers.rotate_left(1);
                }

                // find first non-banned peer that can actually serve a request. Relays are in
                // `connected_peers` (the direct leg we route circuits over) but only speak the
                // circuit protocol, so picking one as a request target always fails with
                // `UnsupportedProtocols` -- skip them.
                if let Some(peer) = self.connected_peers.iter().find(|p| {
                    let pm = &self.swarm.behaviour().peer_manager;
                    !pm.peer_banned(p) && !pm.is_relay(p)
                }) {
                    let request_id = self.swarm.behaviour_mut().req_res.send_request(peer, request);
                    self.outbound_requests.insert((*peer, request_id), reply);
                } else {
                    let _ = reply.send(Err(NetworkError::NoPeers));
                }
            }
            NetworkCommand::SendResponse { response, channel, reply } => {
                let res = self.swarm.behaviour_mut().req_res.send_response(channel, response);
                send_or_log_error!(reply, res, "SendResponse");
            }
            NetworkCommand::PendingRequestCount { reply } => {
                let count = self.outbound_requests.len();
                send_or_log_error!(reply, count, "SendResponse");
            }
            NetworkCommand::ReportPenalty { peer, penalty } => {
                debug!(target: "network", "penalty reported for peer {peer}");
                if let Some((peer, _)) = self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, penalty);
                } else {
                    warn!(target: "peer-manager", ?peer, "unable to assess penalty for peer");
                }
            }
            NetworkCommand::DisconnectPeer { peer_id, reply } => {
                // this is called after timeout for disconnected peer exchanges
                let res = self.swarm.disconnect_peer_id(peer_id);
                send_or_log_error!(reply, res, "DisconnectPeer");
            }
            NetworkCommand::PeersForExchange { reply } => {
                let peers = self.swarm.behaviour_mut().peer_manager.peers_for_exchange();
                send_or_log_error!(reply, peers, "PeersForExchange");
            }
            NetworkCommand::NewEpoch { committee } => {
                // at the start of a new epoch, each node needs to know:
                // - the current committee
                // - all staked nodes who will vote at the end of the epoch
                //      - only synced nodes can vote
                //
                // once a node stakes and tries to sync, it would be nice
                // if it could receive priority on the network for syncing
                // state
                //
                // for now, this only supports the current committee for the epoch

                info!(target: "network", this_node=?self.swarm.local_peer_id(), "network update for next committee - ensuring no committee members are banned");
                // ensure that the next committee isn't banned
                self.swarm.behaviour_mut().peer_manager.new_epoch(committee);
            }
            NetworkCommand::FindAuthorities { bls_keys } => {
                // this will trigger a PeerEvent to fetch records through kad if not in the peer map
                self.swarm.behaviour_mut().peer_manager.find_authorities(bls_keys);
            }
        }

        Ok(())
    }
}

/// Resolve each `/dnsaddr/<host>/...` address's `_dnsaddr.<host>` TXT records into the circuit
/// multiaddrs they advertise (`dnsaddr=<multiaddr>` entries). This is how a node learns -- from
/// DNS, configlessly -- which relays it dials through when peers are advertised via `/dnsaddr`, so
/// it can register them as protected and not ban them (which would trigger an IP-level cascade).
///
/// Free function taking a cloned resolver so it can run in a detached task off the swarm event
/// loop -- the DNS lookup must never block the loop (see `AddBootstrapPeers`). The returned
/// circuits are handed back via `NetworkCommand::RegisterRelays` for on-loop registration.
async fn resolve_relay_circuits(
    resolver: &hickory_resolver::TokioResolver,
    dnsaddrs: &[Multiaddr],
) -> Vec<Multiaddr> {
    let mut circuits = Vec::new();
    for addr in dnsaddrs {
        let Some(host) = addr.iter().find_map(|p| match p {
            Protocol::Dnsaddr(h) => Some(h.to_string()),
            _ => None,
        }) else {
            continue;
        };
        let name = format!("_dnsaddr.{host}");
        match resolver.txt_lookup(name.clone()).await {
            Ok(txts) => {
                for record in txts.iter() {
                    for data in record.txt_data() {
                        if let Ok(s) = std::str::from_utf8(data) {
                            if let Some(rest) = s.strip_prefix("dnsaddr=") {
                                if let Ok(ma) = rest.parse::<Multiaddr>() {
                                    circuits.push(ma);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(target: "network", %name, ?e, "failed to resolve /dnsaddr for relay discovery");
            }
        }
    }
    circuits
}
