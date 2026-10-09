use crate::{
    codec::RLMessage,
    peers::Penalty,
    types::{NetworkEvent, NetworkResult, NexthopFmt},
    ConsensusNetwork, PeerExchangeMap,
};
use libp2p::request_response::{
    self, Event as ReqResEvent, InboundFailure as ReqResInboundFailure, OutboundFailure,
};
use rayls_infrastructure_types::{Database, RaylsSender};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Process req/res events.
    pub(super) fn process_reqres_event(
        &mut self,
        event: ReqResEvent<Req, Res>,
    ) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id } => {
                // the transport path this message actually traveled, for the relayed-topology
                // audit trail. Cheap Copy wrapper; formatted lazily by the log macro only when the
                // event is enabled ("closed" if the connection already closed).
                let via = NexthopFmt(self.connection_paths.get(&connection_id).copied());
                match message {
                    request_response::Message::Request { request_id, request, channel } => {
                        debug!(target: "network", peer_id = %peer, ?via, ?request, "request received");
                        // intercept peer exchange messages
                        if let Some(peers) = request.peer_exchange_msg() {
                            debug!(target: "network", ?peers, "processing peer exchange");
                            self.swarm.behaviour_mut().peer_manager.process_peer_exchange(peers);
                            // send empty ack and ignore errors
                            let ack = PeerExchangeMap::default().into();
                            let _ = self.swarm.behaviour_mut().req_res.send_response(channel, ack);

                            // initiate disconnect from this peer to prevent redial attempts
                            debug!(target: "peer-manager", ?peer, "initiating reciprocal disconnect after px");
                            self.swarm.behaviour_mut().peer_manager.disconnect_peer(peer, false);

                            return Ok(());
                        }

                        // We should not be able to receive a message from an unknown peer so this
                        // should always work. It is possible (mostly in
                        // testing) to have a race where we don't know the requester YET.
                        // If so send an error back but this should be so infrequent on a real
                        // network that we can ignore and it should not
                        // cause any lasting damage if triggered.
                        if let Some(bls) = self.swarm.behaviour().peer_manager.peer_to_bls(&peer) {
                            let (notify, cancel) = oneshot::channel();
                            // forward request to handler without blocking other events
                            if let Err(e) = self.event_stream.try_send(NetworkEvent::Request {
                                peer: bls,
                                request,
                                channel,
                                cancel,
                            }) {
                                error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                                // ignore failures at the epoch boundary
                                // During epoch change the event_stream receiver can be closed.
                                return Ok(());
                            }

                            // store the request and cancel duplicate requests
                            //
                            // NOTE: the request id is internally generated, so this should not
                            // happen
                            if let Some(channel) = self.inbound_requests.insert(request_id, notify)
                            {
                                // cancel if this is a duplicate request
                                warn!(target: "network", ?peer, "duplicate request id from peer");
                                let _ = channel.send(());
                            }
                        } else if let Err(e) = self.event_stream.try_send(NetworkEvent::Error(
                            format!("requesting peer unknown: {peer:?}"),
                            channel,
                        )) {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                            // ignore failures at the epoch boundary
                            // During epoch change the event_stream receiver can be closed.
                            return Ok(());
                        }
                    }
                    request_response::Message::Response { request_id, response } => {
                        debug!(target: "network", peer_id = %peer, ?via, ?request_id, "response received");
                        // check if response associated with PX disconnect
                        if self.pending_px_disconnects.remove(&request_id).is_some() {
                            let _ = self.swarm.disconnect_peer_id(peer);
                        }

                        // try to forward response to original caller
                        let _ = self
                            .outbound_requests
                            .remove(&(peer, request_id))
                            .map(|ack| ack.send(Ok(response)));
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, "Outbound failure for req/res");
                // handle px disconnects
                //
                // px attempts to support peer discovery, but failures are okay
                // this node disconnects after a px timeout
                if self.pending_px_disconnects.remove(&request_id).is_some() {
                    debug!(target: "network", "outbound failure expected because of px disconnect");
                    // a px request is tracked in outbound_requests too; the common cleanup below is
                    // skipped by this early return, so drop the ack here. Dropping the sender also
                    // resolves the px task's wait immediately instead of holding it to the timeout.
                    self.outbound_requests.remove(&(peer, request_id));
                    return Ok(());
                }

                // apply differentiated penalty based on failure type
                if let Some(penalty) = outbound_failure_penalty(&error) {
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, penalty);
                }

                // try to forward error to original caller
                let _ = self
                    .outbound_requests
                    .remove(&(peer, request_id))
                    .map(|ack| ack.send(Err(error.into())));
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, pending=?self.inbound_requests, "Inbound failure for req/res");
                if let Some(penalty) = inbound_failure_penalty(&error) {
                    warn!(target: "network", ?peer, ?request_id, ?error, ?penalty, "penalizing inbound failure");
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, penalty);
                }

                // forward cancelation to handler and ignore errors
                if let Some(channel) = self.inbound_requests.remove(&request_id) {
                    let _ = channel.send(());
                }
            }

            ReqResEvent::ResponseSent { request_id, .. } => {
                if let Some(channel) = self.inbound_requests.remove(&request_id) {
                    let _ = channel.send(());
                }
            }
        }

        Ok(())
    }
}

/// Classifies an outbound request-response failure into the penalty owed to the target peer.
///
/// Penalties are behaviour-based: a peer is scored only for what *it* did to us. An outbound
/// request is an operation *we* initiated against a target *we* chose, so none of its failure
/// modes is the peer's provable fault, and every one returns `None`.
///
/// `Io` used to cost the target a Mild. The variant is ambiguous: an undecodable response and a
/// stream reset or read error mid-response surface the same way, and resets are routine on
/// relayed circuits, which have no transport keep-alive. The arm could not tell a peer's fault
/// from link weather, and since committee members are penalty-immune, every point it produced
/// landed on non-committee peers only. A validator one epoch behind, retrying its epoch-record
/// fetch once a second across every connected peer, disconnected both observers of a testnet
/// for reputation on nothing but failed streams. A peer that really sends garbage is still
/// caught where the garbage is inspected: the application handlers decode responses and report
/// their own penalties for malformed content.
fn outbound_failure_penalty(error: &OutboundFailure) -> Option<Penalty> {
    match error {
        OutboundFailure::ConnectionClosed
        | OutboundFailure::Timeout
        | OutboundFailure::DialFailure => None,
        // The target does not speak the protocol we asked for. That is our targeting mistake, not
        // the peer's behaviour: `SendRequestAny` round-robins over every connected peer, which can
        // include a relay, a worker-side identity that landed on the primary port, or a node on a
        // different protocol version. Scoring it here banned exactly those peers (two hits reach
        // the disconnect threshold, five the ban) for requests they never asked to receive. The
        // rotation already moves on to the next peer, so the cost of not scoring is one wasted
        // request per rotation.
        OutboundFailure::UnsupportedProtocols => None,
        // Covers local substream exhaustion ("max sub-streams reached"), resets, read errors and
        // undecodable responses alike; see the function doc for why none of them is scored here.
        OutboundFailure::Io(_) => None,
    }
}

/// Classifies an inbound request-response failure into the penalty owed the requesting peer.
///
/// Returns `None` for failures not the requester's fault. libp2p reports an unreadable request
/// and a failed response write as the same `Io` variant, so charging it would ban an innocent
/// requester for this node's own write failure.
fn inbound_failure_penalty(error: &ReqResInboundFailure) -> Option<Penalty> {
    match error {
        // The requester opened a stream on a protocol set this node does not accept. That is the
        // peer's own action, so it is scored -- but as Severe, not Fatal: a single stray
        // negotiation is far more often a misconfigured swarm (a worker dialing the primary port)
        // or a version skew during a rolling upgrade than an attack. Severe still disconnects
        // after two and bans after five sustained attempts, and the ban decays once the peer is
        // fixed (`banned_before_decay_secs`, then the score half-life) with no restart on our side;
        // Fatal banned the id on the first stream for the full freeze period.
        ReqResInboundFailure::UnsupportedProtocols => Some(Penalty::Severe),
        ReqResInboundFailure::Io(_)
        | ReqResInboundFailure::Timeout
        | ReqResInboundFailure::ConnectionClosed
        | ReqResInboundFailure::ResponseOmission => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    /// An inbound `Io` failure is ambiguous between an unreadable request and this node's own
    /// failure to write the response, so it must never charge the requester.
    #[test]
    fn inbound_io_failure_is_not_penalized() {
        let error = ReqResInboundFailure::Io(io::Error::other("failed to write response"));
        assert!(
            inbound_failure_penalty(&error).is_none(),
            "an inbound Io failure may be our own write failure and must not ban the requester"
        );
    }

    /// Connection-level and local inbound failures earn nothing; only a protocol mismatch does,
    /// and it accumulates (Severe) rather than banning on the first stray stream (Fatal), so a
    /// misconfigured or version-skewed peer is disconnected/banned only while the fault persists.
    #[test]
    fn inbound_failure_penalties_match_fault() {
        assert!(inbound_failure_penalty(&ReqResInboundFailure::Timeout).is_none());
        assert!(inbound_failure_penalty(&ReqResInboundFailure::ConnectionClosed).is_none());
        assert!(inbound_failure_penalty(&ReqResInboundFailure::ResponseOmission).is_none());
        assert!(matches!(
            inbound_failure_penalty(&ReqResInboundFailure::UnsupportedProtocols),
            Some(Penalty::Severe)
        ));
    }

    /// A local outbound substream exhaustion must not penalize the (innocent) target peer.
    #[test]
    fn max_substreams_reached_is_not_penalized() {
        let error = OutboundFailure::Io(io::Error::other("max sub-streams reached"));
        assert!(
            outbound_failure_penalty(&error).is_none(),
            "local substream exhaustion must not move a peer toward ban"
        );
    }

    /// Outbound failures are operations we initiated and are never scored. An `Io` failure is
    /// ambiguous between a garbage response and a reset link, and scoring it disconnected the
    /// observers of a testnet for reputation on failed streams alone; a target that does not speak
    /// the requested protocol is our targeting mistake -- that path banned relays and worker
    /// identities picked up by `SendRequestAny`.
    #[test]
    fn outbound_failures_are_not_the_targets_fault() {
        let decode_failure = OutboundFailure::Io(io::Error::other("invalid value"));
        assert!(
            outbound_failure_penalty(&decode_failure).is_none(),
            "an Io failure cannot be told apart from link weather and must not be scored"
        );
        let reset = OutboundFailure::Io(io::Error::from(io::ErrorKind::ConnectionReset));
        assert!(outbound_failure_penalty(&reset).is_none());

        assert!(
            outbound_failure_penalty(&OutboundFailure::UnsupportedProtocols).is_none(),
            "a target we picked that lacks the protocol did nothing to us"
        );

        assert!(outbound_failure_penalty(&OutboundFailure::Timeout).is_none());
        assert!(outbound_failure_penalty(&OutboundFailure::ConnectionClosed).is_none());
        assert!(outbound_failure_penalty(&OutboundFailure::DialFailure).is_none());
    }
}
