use crate::{
    codec::RLMessage,
    consensus::types::GossipAcceptance,
    peers::Penalty,
    types::{NetworkEvent, NetworkResult},
    ConsensusNetwork,
};
use libp2p::gossipsub::{Event as GossipEvent, Message as GossipMessage};
use rayls_infrastructure_config::GOSSIP_TOPIC_TXN;
use rayls_infrastructure_types::{Database, RaylsSender};
use tracing::{debug, error, info, trace, warn};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Process gossip events.
    pub(super) fn process_gossip_event(&mut self, event: GossipEvent) -> NetworkResult<()> {
        match event {
            GossipEvent::Message { propagation_source, message_id, message } => {
                trace!(target: "network", topic=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?message, "message received from publisher");
                // verify message was published by authorized node
                let msg_acceptance = self.verify_gossip(&message);
                trace!(target: "network", ?msg_acceptance, "gossip message verification status");

                // report message validation results to propagate valid messages
                if !self.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    msg_acceptance.into(),
                ) {
                    error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, "error reporting message validation result");
                }

                // process gossip in application layer
                match msg_acceptance {
                    GossipAcceptance::Accept => {
                        // We should not be able to receive a message from an unknown peer so this
                        // should always work.
                        let gossip_source = message.source.unwrap_or(propagation_source);

                        if let Some(bls) =
                            self.swarm.behaviour().peer_manager.peer_to_bls(&gossip_source)
                        {
                            // forward gossip to handler
                            if let Err(e) =
                                self.event_stream.try_send(NetworkEvent::Gossip(message, bls))
                            {
                                error!(target: "network", topics=?self.authorized_publishers.keys(), ?gossip_source, ?message_id, ?e, "failed to forward gossip!");
                                // ignore failures at the epoch boundary
                                // During epoch change the event_stream receiver can be closed.
                                return Ok(());
                            }
                        } else {
                            let known_peerids_len =
                                self.swarm.behaviour().peer_manager.known_peerids_len();
                            // debug, not warn: reachable per message through the known-validator
                            // fallback and on a topic that is still open, so a peer must not be
                            // able to fill the log through it
                            debug!(
                                target: "network::gossip",
                                ?gossip_source,
                                ?propagation_source,
                                topic = %message.topic,
                                known_peerids_len,
                                "dropping valid gossip, no BLS mapping for propagation source"
                            );
                        }
                    }
                    GossipAcceptance::Ignore => {
                        let GossipMessage { source: gossip_source, topic, .. } = message;
                        // the topic label is bounded because gossipsub delivers only the topics
                        // this node subscribed to; keep this counter behind that filter
                        self.network_metrics
                            .gossip_ignored
                            .with_label_values(&[topic.as_str(), self.network_label])
                            .inc();
                        // trace, not debug: nothing stops a peer from streaming these, and the
                        // counter above is the signal
                        trace!(
                            target: "network",
                            author = ?gossip_source,
                            ?topic,
                            ?propagation_source,
                            "dropping gossip from an unauthorized or unbound publisher"
                        );
                    }
                    GossipAcceptance::Reject => {
                        let GossipMessage { source: gossip_source, topic, .. } = message;
                        warn!(
                            target: "network",
                            author = ?gossip_source,
                            ?topic,
                            ?propagation_source,
                            "applying fatal penalty to message author"
                        );
                        // Penalize the message author only, never the forwarding propagation
                        // source; a relayer must not be banned for an invalid message it merely
                        // gossiped on.
                        if let Some(peer_id) = gossip_source {
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer_id, Penalty::Fatal);
                        }
                    }
                }
            }
            GossipEvent::Subscribed { peer_id, topic } => {
                let bls = self.swarm.behaviour().peer_manager.peer_to_bls(&peer_id);
                info!(
                    target: "network::gossipsub",
                    ?peer_id,
                    ?bls,
                    ?topic,
                    connected_peers = self.connected_peers.len(),
                    "peer SUBSCRIBED to topic"
                );
            }
            GossipEvent::Unsubscribed { peer_id, topic } => {
                let bls = self.swarm.behaviour().peer_manager.peer_to_bls(&peer_id);
                info!(
                    target: "network::gossipsub",
                    ?peer_id,
                    ?bls,
                    ?topic,
                    connected_peers = self.connected_peers.len(),
                    "peer UNSUBSCRIBED from topic"
                );
            }
            GossipEvent::GossipsubNotSupported { peer_id } => {
                // `GossipsubNotSupported` is authoritative: gossipsub negotiated with this peer and
                // it runs none of our gossip protocols. Fatally banning it (the original behavior)
                // severed every circuit routed through a relay, stranding relayed nodes -- so we
                // must not ban. This arm MUST stay penalty-free: relays are not exempt from
                // penalties (scoring is behaviour-based, see `PeerManager::process_penalty`), and
                // "does not speak gossipsub" is the one signal an honest relay emits by nature --
                // the only one it must never be scored for. Conversely, "no gossipsub" is NOT proof
                // of being a relay: from a bare peer id a relay is indistinguishable from a
                // misconfigured or hostile peer, so this signal confers no privilege either. The
                // peer remains subject to every other penalty and to pruning. Keep it out of the
                // DHT so it is never propagated as a peer. A committee validator reaching here is a
                // real protocol/version fault -- surface it loudly rather than reclassifying it.
                if self.swarm.behaviour().peer_manager.should_skip_gossip_penalty(&peer_id) {
                    // stop treating it as a DHT peer (mirrors the kad cleanup the ban path did)
                    self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
                    info!(target: "network", ?peer_id, "peer does not support gossipsub - skipping gossip penalty and keeping it out of kad (other penalties and pruning still apply)");
                } else {
                    warn!(target: "network", ?peer_id, "committee member does not support gossipsub - possible protocol/version mismatch");
                }
            }
            GossipEvent::SlowPeer { peer_id, failed_messages } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?failed_messages, "gossipsub event - slow peer");

                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Mild);
            }
        }

        Ok(())
    }

    /// Specific logic to accept gossip messages.
    ///
    /// Messages must come from a publisher on the topic's list (the committee, or the registered
    /// validators for consensus output and epoch votes) and must be within max size.
    ///
    /// Rayls: During startup, there's a race condition where `authorized_publishers` may be empty
    /// or `peer_to_bls()` mappings may not be established yet. So that committee traffic is
    /// still forwarded and processed, messages from known validators (peers marked as important,
    /// the current and departing committee) are accepted when the list check would otherwise
    /// fail.
    ///
    /// The rule: an oversize message is rejected (a fault of the message itself, so its author is
    /// penalized); a publisher that is unlisted or unbound is ignored, neither forwarded nor
    /// penalized. The lists are this node's own view, so an unlisted publisher may be an honest
    /// validator the node is behind on, and gossipsub has already done the frame work that an
    /// ignored message costs. Nothing else here penalizes a publisher.
    pub(super) fn verify_gossip(&self, gossip: &GossipMessage) -> GossipAcceptance {
        // verify message size
        // do not punish if gossip is tx batch
        if gossip.data.len() > self.config.max_gossip_message_size
            && gossip.topic.as_str() != GOSSIP_TOPIC_TXN
        {
            return GossipAcceptance::Reject;
        }

        let GossipMessage { topic, .. } = gossip;

        // Rayls: Startup grace period: If no authorization config for this topic,
        // accept messages from known validators so committee traffic is still
        // delivered during initialization race conditions
        let auth_config = self.authorized_publishers.get(topic.as_str());
        if auth_config.is_none() {
            if let Some(source_id) = gossip.source {
                if self.swarm.behaviour().peer_manager.peer_is_important(&source_id) {
                    trace!(
                        target: "network",
                        ?topic,
                        ?source_id,
                        "accepting gossip from validator during startup grace period"
                    );
                    return GossipAcceptance::Accept;
                }
            }
        }

        // ensure publisher is authorized
        if gossip.source.is_some_and(|id| {
            let bls_key = self.swarm.behaviour().peer_manager.peer_to_bls(&id);
            auth_config.is_some_and(|auth| {
                auth.is_none()
                    || (bls_key.is_some()
                        && auth.as_ref().expect("is some").contains(&bls_key.expect("is some")))
            })
        }) {
            GossipAcceptance::Accept
        } else {
            // Rayls: Final fallback: if BLS mapping unavailable but source is known validator,
            // accept
            if let Some(source_id) = gossip.source {
                if self.swarm.behaviour().peer_manager.peer_is_important(&source_id) {
                    trace!(
                        target: "network",
                        ?topic,
                        ?source_id,
                        "accepting gossip from validator (BLS mapping unavailable)"
                    );
                    return GossipAcceptance::Accept;
                }
            }
            GossipAcceptance::Ignore
        }
    }
}
