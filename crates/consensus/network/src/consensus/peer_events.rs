use crate::{
    codec::RLMessage,
    error::NetworkError,
    peers::PeerEvent,
    types::{NetworkEvent, NetworkResult},
    ConsensusNetwork,
};
use libp2p::{kad, PeerId};
use rayls_infrastructure_types::{Database, RaylsSender};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Process an event from the peer manager.
    pub(super) fn process_peer_manager_event(&mut self, event: PeerEvent) -> NetworkResult<()> {
        match event {
            PeerEvent::DisconnectPeer(peer_id) => {
                debug!(target: "network", ?peer_id, "peer manager: disconnect peer");
                // remove from request-response
                // NOTE: gossipsub handle `FromSwarm::ConnectionClosed`
                let _ = self.swarm.disconnect_peer_id(peer_id);

                //update metrics
                if let Err(e) = self
                    .network_metrics
                    .connected_peers
                    .remove_label_values(&[peer_id.to_string().as_str(), self.network_label])
                {
                    warn!(target: "network::metrics", ?e, "failed to remove connected peer from metrics");
                }

                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
            }
            PeerEvent::PeerDisconnected(peer_id) => {
                let bls = self.swarm.behaviour().peer_manager.peer_to_bls(&peer_id);
                info!(
                    target: "network::gossipsub",
                    ?peer_id,
                    ?bls,
                    connected_peers = self.connected_peers.len(),
                    "peer DISCONNECTED"
                );

                // Check if there are any connections still in the pool
                if self.swarm.is_connected(&peer_id) {
                    warn!(
                        target: "network",
                        ?peer_id,
                        "PeerDisconnected event but swarm still has connections - forcing disconnect"
                    );
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);

                //update metrics
                if let Err(e) = self
                    .network_metrics
                    .connected_peers
                    .remove_label_values(&[peer_id.to_string().as_str(), self.network_label])
                {
                    warn!(target: "network::metrics", ?e, "failed to remove connected peer from metrics");
                }
                self.network_metrics
                    .connected_peers_count
                    .with_label_values(&[self.network_label])
                    .dec();

                let keys = self
                    .outbound_requests
                    .iter()
                    .filter_map(
                        |((p_id, req_id), _)| {
                            if *p_id == peer_id {
                                Some((*p_id, *req_id))
                            } else {
                                None
                            }
                        },
                    )
                    .collect::<Vec<_>>();

                // remove from outbound_requests and send error
                for k in keys {
                    let _ = self
                        .outbound_requests
                        .remove(&k)
                        .map(|ack| ack.send(Err(NetworkError::Disconnected)));
                }
            }
            PeerEvent::DisconnectPeerX(peer_id, peer_exchange) => {
                debug!(target: "peer-manager", this_node=?self.swarm.local_peer_id(), ?peer_id, "disconnecting from peer with exchange info");
                // attempt to exchange peer information if limits allow
                if self.pending_px_disconnects.len() < self.config.max_px_disconnects {
                    let (reply, done) = oneshot::channel();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .req_res
                        .send_request(&peer_id, peer_exchange.into());
                    self.outbound_requests.insert((peer_id, request_id), reply);

                    let timeout = self.config.px_disconnect_timeout;
                    let handle = self.network_handle();

                    // spawn task
                    let task_name = format!("peer-exchange-{peer_id}");
                    self.task_spawner.spawn_task(task_name, async move {
                        // ignore errors and disconnect after px attempt
                        let _res = tokio::time::timeout(timeout, done).await;
                        let _ = handle.disconnect_peer(peer_id).await;
                    });

                    // insert to pending px disconnects
                    self.pending_px_disconnects.insert(request_id, peer_id);
                } else {
                    // too many px disconnects pending so disconnect without px
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }
                // remove peer from kad - will redial if necessary
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);

                // update metrics
                if let Err(e) = self
                    .network_metrics
                    .connected_peers
                    .remove_label_values(&[peer_id.to_string().as_str(), self.network_label])
                {
                    warn!(target: "network::metrics", ?e, "failed to remove connected peer from metrics");
                }
            }
            PeerEvent::PeerConnected(peer_id, addr) => {
                let bls = self.swarm.behaviour().peer_manager.peer_to_bls(&peer_id);
                let is_important = self.swarm.behaviour().peer_manager.peer_is_important(&peer_id);
                info!(
                    target: "network::gossipsub",
                    ?peer_id,
                    ?bls,
                    ?addr,
                    is_important,
                    connected_peers = self.connected_peers.len(),
                    "peer CONNECTED"
                );

                // register peer for request-response behaviour
                // NOTE: gossipsub handles `FromSwarm::ConnectionEstablished`
                self.swarm.add_peer_address(peer_id, addr.clone());
                // Do NOT add relays to kademlia or share our record with them: relays only speak
                // the circuit protocol, so putting them in the DHT makes other nodes discover and
                // dial them as if they were peers. Those nodes then penalize/ban the relay for not
                // speaking consensus protocols -- and on a shared IP (local testnet, everything on
                // 127.0.0.1) an IP-level ban then knocks out every real peer behind that IP.
                if self.swarm.behaviour().peer_manager.is_relay(&peer_id) {
                    debug!(target: "network-kad", ?peer_id, "skipping kad add/publish for relay peer");
                } else {
                    // add as a kademlia peer
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    self.publish_our_data_to_peer(peer_id);
                }

                // manage connected peers - avoid duplicates from rapid reconnects
                if !self.connected_peers.contains(&peer_id) {
                    self.connected_peers.push_back(peer_id);
                }

                // update metrics
                let peer_id_str = peer_id.to_string();
                let labels = [peer_id_str.as_str(), self.network_label];
                if self.network_metrics.connected_peers.with_label_values(&labels).get() == 0 {
                    self.network_metrics.connected_peers.with_label_values(&labels).inc();
                    self.network_metrics
                        .connected_peers_count
                        .with_label_values(&[self.network_label])
                        .inc();
                }
            }
            PeerEvent::Banned(peer_id) => {
                warn!(target: "network", ?peer_id, "peer banned");
                // blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);

                // update metrics
                self.network_metrics
                    .banned_peers
                    .with_label_values(&[peer_id.to_string().as_str(), self.network_label])
                    .inc();
                self.network_metrics
                    .banned_peers_count
                    .with_label_values(&[self.network_label])
                    .inc();
                //update metrics
                if let Err(e) = self
                    .network_metrics
                    .connected_peers
                    .remove_label_values(&[peer_id.to_string().as_str(), self.network_label])
                {
                    warn!(target: "network::metrics", ?e, "failed to remove connected peer from metrics");
                }
            }
            PeerEvent::Unbanned(peer_id) => {
                debug!(target: "network", ?peer_id, "peer unbanned");
                // remove blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&peer_id);

                // update metrics
                if let Err(e) = self
                    .network_metrics
                    .banned_peers
                    .remove_label_values(&[peer_id.to_string().as_str(), self.network_label])
                {
                    warn!(target: "network::metrics", ?e, "failed to remove banned peer from metrics");
                }
                self.network_metrics
                    .banned_peers_count
                    .with_label_values(&[self.network_label])
                    .dec();
            }
            PeerEvent::MissingAuthorities(missing) => {
                for bls_key in missing {
                    let key = kad::RecordKey::new(&bls_key);
                    let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
                    self.kad_record_queries.insert(query_id, bls_key.into());
                }
            }
            PeerEvent::Discovery => {
                let peer_id = PeerId::random();
                self.swarm.behaviour_mut().kademlia.get_closest_peers(peer_id);
            }
        }

        Ok(())
    }
}
