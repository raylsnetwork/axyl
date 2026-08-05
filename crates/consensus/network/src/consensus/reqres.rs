use crate::{
    codec::RLMessage,
    peers::Penalty,
    types::{NetworkEvent, NetworkResult},
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
            ReqResEvent::Message { peer, message, connection_id: _ } => {
                match message {
                    request_response::Message::Request { request_id, request, channel } => {
                        debug!(target: "network", ?peer, ?request, "request received");
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
                debug!(target: "network", my_id=?self.swarm.local_peer_id(), "this node");
                match error {
                    ReqResInboundFailure::Io(e) => {
                        // penalize peer since this is an attack surface
                        warn!(target: "network", ?e, ?peer, ?request_id, "inbound IO failure");
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Medium);
                    }
                    ReqResInboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, ?error, "inbound failure: unsupported protocol");

                        // the local peer supports none of the protocols requested by the remote
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Fatal);
                    }
                    ReqResInboundFailure::Timeout | ReqResInboundFailure::ConnectionClosed => {
                        // no penalty for connection-level failures
                    }
                    ReqResInboundFailure::ResponseOmission => { /* ignore local error */ }
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
/// Returns `None` for failures not the peer's fault. "max sub-streams reached" is local outbound
/// exhaustion, not the target, so penalizing there would let a self-inflicted flood ban innocents.
fn outbound_failure_penalty(error: &OutboundFailure) -> Option<Penalty> {
    match error {
        OutboundFailure::ConnectionClosed
        | OutboundFailure::Timeout
        | OutboundFailure::DialFailure => None,
        // brittle string match: the libp2p handler exposes local exhaustion only as an opaque
        // `io::Error::other("max sub-streams reached")`, so an SDK bump changing this literal must
        // re-check the arm (a miss only over-penalizes, never under-penalizes a real fault).
        OutboundFailure::Io(e) if e.to_string().contains("max sub-streams reached") => None,
        OutboundFailure::Io(_) => Some(Penalty::Medium),
        OutboundFailure::UnsupportedProtocols => Some(Penalty::Severe),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    /// A local outbound substream exhaustion must not penalize the (innocent) target peer.
    #[test]
    fn max_substreams_reached_is_not_penalized() {
        let error = OutboundFailure::Io(io::Error::other("max sub-streams reached"));
        assert!(
            outbound_failure_penalty(&error).is_none(),
            "local substream exhaustion must not move a peer toward ban"
        );
    }

    /// Other outbound failure classes keep their existing penalties.
    #[test]
    fn other_outbound_failures_keep_penalties() {
        let decode_failure = OutboundFailure::Io(io::Error::other("invalid value"));
        assert!(matches!(outbound_failure_penalty(&decode_failure), Some(Penalty::Medium)));

        assert!(matches!(
            outbound_failure_penalty(&OutboundFailure::UnsupportedProtocols),
            Some(Penalty::Severe)
        ));

        assert!(outbound_failure_penalty(&OutboundFailure::Timeout).is_none());
        assert!(outbound_failure_penalty(&OutboundFailure::ConnectionClosed).is_none());
        assert!(outbound_failure_penalty(&OutboundFailure::DialFailure).is_none());
    }
}
