use crate::{
    codec::{RLCodec, RLMessage},
    consensus::behaviour::RLBehaviorEvent,
    error::NetworkError,
    types::{NetworkEvent, NetworkResult},
    ConsensusNetwork,
};
use futures::StreamExt as _;
use libp2p::{gossipsub, kad::Mode, swarm::SwarmEvent};
use rayls_infrastructure_types::{Database, RaylsSender};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, instrument, warn};

/// CHAOS: how long `chaos_seen_gossip` remembers a message id's first acceptance.
const CHAOS_SEEN_GOSSIP_TTL: Duration = Duration::from_secs(15 * 60);

/// CHAOS (local experiments only): the gossip read delay toggle file, if enabled.
///
/// `RAYLS_CHAOS_GOSSIP_DELAY_FILE` names the file directly. Otherwise
/// `RAYLS_CHAOS_GOSSIP_DELAY_DIR` gives a directory and the file is `<dir>/<pid>`, so one
/// variable exported for a whole local network still gives every node its own toggle.
pub(super) fn chaos_gossip_delay_path() -> Option<String> {
    std::env::var("RAYLS_CHAOS_GOSSIP_DELAY_FILE").ok().or_else(|| {
        std::env::var("RAYLS_CHAOS_GOSSIP_DELAY_DIR")
            .ok()
            .map(|dir| format!("{dir}/{}", std::process::id()))
    })
}

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

        // CHAOS (local experiments only, not for real networks): slow socket reading on the
        // primary network. The toggle file (see `chaos_gossip_delay_path`) is re-read every
        // second and its value (ms, 0 or missing = off) is slept before each gossip message is
        // handled. The sleep stalls the whole swarm loop, so connection tasks stop reading their
        // streams, data waits in quinn's receive buffers, and Accept (and with it the forward) is
        // reported late, while quinn keeps the connections alive.
        let gossip_delay_ms = Arc::new(AtomicU64::new(0));
        if self.network_label == "primary" {
            if let Some(path) = chaos_gossip_delay_path() {
                warn!(target: "network::chaos", %path, "gossip read delay toggle ENABLED");
                let delay = gossip_delay_ms.clone();
                tokio::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(1));
                    // Exit once the network loop (the other Arc holder) is gone.
                    while Arc::strong_count(&delay) > 1 {
                        tick.tick().await;
                        let ms = std::fs::read_to_string(&path)
                            .ok()
                            .and_then(|s| s.trim().parse::<u64>().ok())
                            .unwrap_or(0);
                        let prev = delay.swap(ms, Ordering::Relaxed);
                        if prev != ms {
                            warn!(target: "network::chaos", prev, ms, "gossip read delay changed");
                        }
                    }
                });
            }
        }

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    // CHAOS: see `gossip_delay_ms` above.
                    if matches!(
                        event,
                        SwarmEvent::Behaviour(RLBehaviorEvent::Gossipsub(
                            gossipsub::Event::Message { .. }
                        ))
                    ) {
                        let ms = gossip_delay_ms.load(Ordering::Relaxed);
                        if ms > 0 {
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                        }
                    }
                    self.process_event(event).await.inspect_err(|e| {
                        error!(target: "network", ?e, "network event error")
                    })?;

                    // Periodic cleanup: event-based (every 1000 events) OR time-based (every 10 seconds)
                    event_counter = event_counter.wrapping_add(1);
                    let time_for_cleanup = self.last_cleanup.elapsed() >= CLEANUP_INTERVAL;
                    if event_counter.is_multiple_of(1000) || time_for_cleanup {
                        self.cleanup_request_maps();
                        if let Some(seen) = self.chaos_seen_gossip.as_mut() {
                            seen.retain(|_, first| first.elapsed() < CHAOS_SEEN_GOSSIP_TTL);
                        }
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
            SwarmEvent::ListenerClosed { addresses, reason, .. } => {
                // log errors
                if let Err(e) = reason {
                    error!(target: "network", ?e, "listener unexpectedly closed");
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
