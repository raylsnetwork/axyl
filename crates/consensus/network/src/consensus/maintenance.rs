use crate::{
    codec::RLMessage,
    consensus::types::{
        MAX_PENDING_INBOUND_REQUESTS, MAX_PENDING_KAD_QUERIES, MAX_PENDING_OUTBOUND_REQUESTS,
    },
    error::NetworkError,
    types::NetworkEvent,
    ConsensusNetwork,
};
use rayls_infrastructure_types::{Database, RaylsSender};
use tracing::warn;

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Rayls: Remove stale entries from request tracking maps to prevent unbounded growth.
    pub(super) fn cleanup_request_maps(&mut self) {
        // Clean up outbound requests if too many are pending
        let outbound_len = self.outbound_requests.len();
        if outbound_len > MAX_PENDING_OUTBOUND_REQUESTS {
            warn!(
                target: "network",
                count = outbound_len,
                "too many pending outbound requests, clearing oldest"
            );
            // Clear half and notify callers of error
            let to_remove = outbound_len / 2;
            let mut removed = 0;
            for (_, sender) in self.outbound_requests.extract_if(|_, _| {
                removed += 1;
                removed <= to_remove
            }) {
                let _ = sender.send(Err(NetworkError::RequestQueueOverflow));
            }
        }

        // Clean up inbound requests if too many are pending
        let inbound_len = self.inbound_requests.len();
        if inbound_len > MAX_PENDING_INBOUND_REQUESTS {
            warn!(
                target: "network",
                count = inbound_len,
                "too many pending inbound requests, clearing oldest"
            );
            // Clear half and notify handlers of cancellation
            let to_remove = inbound_len / 2;
            let mut removed = 0;
            for (_, sender) in self.inbound_requests.extract_if(|_, _| {
                removed += 1;
                removed <= to_remove
            }) {
                let _ = sender.send(());
            }
        }

        // Clean up kad queries if too many are pending
        let kad_len = self.kad_record_queries.len();
        if kad_len > MAX_PENDING_KAD_QUERIES {
            warn!(
                target: "network-kad",
                count = kad_len,
                "too many pending kad queries, clearing oldest"
            );
            // Just clear without notifying - these are internal queries
            let to_remove = kad_len - MAX_PENDING_KAD_QUERIES / 2;
            let keys: Vec<_> = self.kad_record_queries.keys().take(to_remove).copied().collect();
            for key in keys {
                self.kad_record_queries.remove(&key);
            }
        }
    }

    /// Republish this swarm's peer-address view as three gauges:
    /// - `kad_known_peer_addr_{primary,worker}` -- the kademlia routing table (`kbuckets`), i.e.
    ///   peers actually *connected* and the working address they connected on.
    /// - `advertised_peer_addr_{primary,worker}` -- the peer-manager's `known_peers` dial targets
    ///   (addresses peers advertised that it will redial), including peers not yet connected.
    /// - `discovery_peer_addr_{primary,worker}` -- the peer-manager's `discovery_peers` candidates
    ///   learned via `get_closest_peers` and dialed on the heartbeat (drained as it dials, so a
    ///   fast-churning entry is often absent at snapshot time).
    ///
    /// (The `dial_peer_addr_failures` counter is updated separately, on each dial-error event.)
    ///
    /// Rebuilt from scratch each call: this swarm's own vecs are reset first so a peer or address
    /// that has left stops being reported (an info-style gauge otherwise lingers forever). Primary
    /// and worker use separate vecs, so `reset()` only clears this swarm's rows. All three tables
    /// are snapshotted into owned strings before touching the metrics so the swarm borrow is
    /// released first.
    pub(super) fn refresh_peer_addr_metrics(&mut self) {
        // kad routing table (connected peers, working addresses)
        let mut kad_entries: Vec<(String, String)> = Vec::new();
        for bucket in self.swarm.behaviour_mut().kademlia.kbuckets() {
            for entry in bucket.iter() {
                let peer_id = entry.node.key.preimage().to_string();
                for addr in entry.node.value.iter() {
                    kad_entries.push((peer_id.clone(), addr.to_string()));
                }
            }
        }

        // known_peers dial targets (advertised addresses, incl. peers not connected)
        let known_entries: Vec<(String, String)> = self
            .swarm
            .behaviour()
            .peer_manager
            .known_peer_addrs()
            .into_iter()
            .map(|(peer_id, addr)| (peer_id.to_string(), addr.to_string()))
            .collect();

        // discovery_peers dial candidates (learned via get_closest_peers, dialed on heartbeat)
        let discovery_entries: Vec<(String, String)> = self
            .swarm
            .behaviour()
            .peer_manager
            .discovery_peer_addrs()
            .into_iter()
            .map(|(peer_id, addr)| (peer_id.to_string(), addr.to_string()))
            .collect();

        let (kad_gauge, known_gauge, discovery_gauge) = match self.network_label {
            "worker" => (
                &self.network_metrics.kad_known_peer_addr_worker,
                &self.network_metrics.advertised_peer_addr_worker,
                &self.network_metrics.discovery_peer_addr_worker,
            ),
            _ => (
                &self.network_metrics.kad_known_peer_addr_primary,
                &self.network_metrics.advertised_peer_addr_primary,
                &self.network_metrics.discovery_peer_addr_primary,
            ),
        };

        kad_gauge.reset();
        for (peer_id, addr) in kad_entries {
            kad_gauge.with_label_values(&[peer_id.as_str(), addr.as_str()]).set(1);
        }

        known_gauge.reset();
        for (peer_id, addr) in known_entries {
            known_gauge.with_label_values(&[peer_id.as_str(), addr.as_str()]).set(1);
        }

        discovery_gauge.reset();
        for (peer_id, addr) in discovery_entries {
            discovery_gauge.with_label_values(&[peer_id.as_str(), addr.as_str()]).set(1);
        }
    }
}
