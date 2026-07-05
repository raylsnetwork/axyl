use crate::{
    codec::{RLCodec, RLMessage},
    consensus::behaviour::RLBehaviorEvent,
    error::NetworkError,
    types::{NetworkEvent, NetworkResult},
    ConsensusNetwork,
};
use futures::StreamExt as _;
use libp2p::{kad::Mode, swarm::SwarmEvent};
use rayls_infrastructure_types::{Database, RaylsSender};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        // add peer record if address confirmed
        self.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
        self.load_known_peers_from_kad_store();
        self.provide_our_data();

        let local_peer_id = *self.swarm.local_peer_id();
        let known_peers = self.swarm.behaviour().peer_manager.connected_peers_only().len();
        info!(
            target: "network::gossipsub",
            ?local_peer_id,
            known_peers,
            subscribed_topics = ?self.authorized_publishers.keys().collect::<Vec<_>>(),
            "gossipsub network loop STARTING"
        );

        // Counter for periodic cleanup
        let mut event_counter: u64 = 0;
        // Time-based cleanup interval (10 seconds)
        const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

        // Periodically re-attempt any relay reservation whose relay went away, so a relay coming
        // back restores the node's reachability without a restart.
        let mut relay_retry = tokio::time::interval(Duration::from_secs(15));

        loop {
            tokio::select! {
                _ = relay_retry.tick() => {
                    self.retry_relay_reservations();
                }
                event = self.swarm.select_next_some() => {
                    self.process_event(event).await.inspect_err(|e| {
                        error!(target: "network", ?e, "network event error")
                    })?;

                    // Periodic cleanup: event-based (every 1000 events) OR time-based (every 10 seconds)
                    event_counter = event_counter.wrapping_add(1);
                    let time_for_cleanup = self.last_cleanup.elapsed() >= CLEANUP_INTERVAL;
                    if event_counter.is_multiple_of(1000) || time_for_cleanup {
                        self.cleanup_request_maps();
                        self.last_cleanup = Instant::now();
                    }
                },
                command = self.commands.recv() => match command {
                    Some(c) => self.process_command(c).inspect_err(|e| {
                        error!(target: "network", ?e, "network command error")
                    })?,
                    None => {
                        info!(target: "network", "network shutting down...");
                        return Ok(())
                    }
                },
            }
        }
    }

    /// Re-issue `listen_on` for any desired relay reservation that currently has no active
    /// listener (its relay went away). Safe to call repeatedly: a still-down relay just closes
    /// again and is retried on the next tick, while a recovered relay re-establishes the
    /// reservation. Once re-reserved on the node's committee-advertised relay, peers reconnect on
    /// their own without a restart.
    fn retry_relay_reservations(&mut self) {
        if self.relay_listen_addrs.is_empty() {
            return;
        }
        let missing: Vec<_> = self
            .relay_listen_addrs
            .iter()
            .filter(|addr| !self.relay_listeners.values().any(|active| active == *addr))
            .cloned()
            .collect();
        for addr in missing {
            match self.swarm.listen_on(addr.clone()) {
                Ok(id) => {
                    info!(target: "network", ?addr, "re-attempting relay reservation");
                    self.relay_listeners.insert(id, addr);
                }
                Err(e) => {
                    warn!(target: "network", ?addr, ?e, "failed to re-attempt relay reservation");
                }
            }
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.authorized_publishers.keys()))]
    async fn process_event(
        &mut self,
        event: SwarmEvent<RLBehaviorEvent<RLCodec<Req, Res>, DB>>,
    ) -> NetworkResult<()> {
        match event {
            SwarmEvent::Behaviour(behavior) => match behavior {
                RLBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                RLBehaviorEvent::ReqRes(event) => self.process_reqres_event(event)?,
                RLBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
                RLBehaviorEvent::Kademlia(event) => self.process_kad_event(event)?,
                RLBehaviorEvent::RelayClient(event) => {
                    // Relay reservation / circuit lifecycle events. Connectivity is driven by the
                    // swarm + peer manager; we only trace these for observability.
                    trace!(target: "network", ?event, "relay client event");
                }
            },
            SwarmEvent::ExternalAddrConfirmed { address: _ } => {
                // New confirmed address so lets publish/update or kademlia address rocord.
                self.provide_our_data();
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                debug!(
                    target: "network",
                    ?address,
                    "listener address expired"
                );
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                // log listener errors
                error!(
                    target: "network",
                    ?listener_id,
                    ?error,
                    "listener error"
                );
            }
            SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                // log errors
                if let Err(e) = reason {
                    error!(target: "network", ?e, "listener unexpectedly closed");
                }

                // If this was a relay reservation, drop it from the active set but keep it in the
                // desired set so `retry_relay_reservations` re-establishes it when the relay
                // returns (the address stays reachable via committee, so peers reconnect on their
                // own once the reservation is back).
                if let Some(addr) = self.relay_listeners.remove(&listener_id) {
                    warn!(target: "network", ?addr, "relay reservation lost; will retry to re-reserve");
                }

                // critical failure
                if self.swarm.listeners().count() == 0 {
                    error!(target: "network", ?addresses, "no listeners for swarm - network shutting down");
                    return Err(NetworkError::AllListenersClosed);
                }
            }
            // other events handled by peer manager and other behaviors
            _ => {}
        }
        Ok(())
    }
}
