//! Metrics for the network.

use prometheus::{
    default_registry, register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    IntCounterVec, IntGaugeVec, Registry,
};

#[derive(Clone, Debug)]
pub struct NetworkMetrics {
    // total number of connected peers.
    pub connected_peers_count: IntGaugeVec,
    // total number of banned peers.
    pub banned_peers_count: IntGaugeVec,
    // connected peers by peer id.
    pub connected_peers: IntGaugeVec,
    // banned peers by peer id.
    pub banned_peers: IntGaugeVec,
    /// peer scores by peer id.
    pub peer_scores: IntGaugeVec,
    /// Established connections by transport path ("circuit", "relay_direct", "direct_nonrelay").
    ///
    /// In a relayed-only topology "direct_nonrelay" must stay 0: every connection is either a leg
    /// to a relay server or a `/p2p-circuit` through one. Non-zero means the node opened a direct
    /// connection to a peer, bypassing the relays.
    pub connections_by_path: IntCounterVec,
    /// primary swarm's kademlia routing table (`kbuckets`), as an info-style gauge (like
    /// `connected_peers`): one series per `(peer_id, multiaddr)` of a peer this node is CONNECTED
    /// to, using the resolved address the connection runs on. Populated only on a successful
    /// connect, so unreachable peers never appear here. Contrast `advertised_peer_addr_primary`,
    /// which lists dial *intent*. Rebuilt each refresh (departed peers drop out); primary and
    /// worker use separate vecs so each can `reset()` its own without touching the other.
    pub kad_known_peer_primary: IntGaugeVec,
    /// worker swarm's kademlia routing table; see [`Self::kad_known_peer_primary`].
    pub kad_known_peer_worker: IntGaugeVec,
    /// primary swarm's dial targets: one series per `(peer_id, multiaddr)` a peer ADVERTISED (via
    /// its DHT `NodeRecord` or committee bootstrap) that this node will redial -- including peers
    /// it has not connected to, so undialable/churn addresses show up here. Diff against
    /// `kad_known_peer_primary` by `peer_id` (`advertised unless on(peer_id) kad_known`) to find
    /// targets that never connect. Same separate-vec / reset scheme.
    pub advertised_peer_addr_primary: IntGaugeVec,
    /// worker swarm's dial targets; see [`Self::advertised_peer_addr_primary`].
    pub advertised_peer_addr_worker: IntGaugeVec,
}

impl NetworkMetrics {
    pub fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            connected_peers_count: register_int_gauge_vec_with_registry!(
                "connected_peers_count",
                "Total number of connected peers",
                &["kad_type"],
                registry
            )?,
            banned_peers_count: register_int_gauge_vec_with_registry!(
                "banned_peers_count",
                "Total number of banned peers",
                &["kad_type"],
                registry
            )?,
            connected_peers: register_int_gauge_vec_with_registry!(
                "connected_peers",
                "Connected peers by peer id",
                &["peer_id", "kad_type"],
                registry
            )?,
            banned_peers: register_int_gauge_vec_with_registry!(
                "banned_peers",
                "Banned peers by peer id",
                &["peer_id", "kad_type"],
                registry
            )?,
            peer_scores: register_int_gauge_vec_with_registry!(
                "peer_scores",
                "Peer scores by peer id",
                &["peer_id", "kad_type"],
                registry
            )?,
            connections_by_path: register_int_counter_vec_with_registry!(
                "connections_by_path",
                "Established connections classified by transport path",
                &["path", "kad_type"],
                registry
            )?,
            kad_known_peer_primary: register_int_gauge_vec_with_registry!(
                "kad_known_peer_primary",
                "Primary kademlia routing table (kbuckets): connected peers and the resolved address in use",
                &["peer_id", "multiaddr"],
                registry
            )?,
            kad_known_peer_worker: register_int_gauge_vec_with_registry!(
                "kad_known_peer_worker",
                "Worker kademlia routing table (kbuckets): connected peers and the resolved address in use",
                &["peer_id", "multiaddr"],
                registry
            )?,
            advertised_peer_addr_primary: register_int_gauge_vec_with_registry!(
                "advertised_peer_addr_primary",
                "Primary dial targets peers advertised (DHT record / committee); includes not-yet-connected peers",
                &["peer_id", "multiaddr"],
                registry
            )?,
            advertised_peer_addr_worker: register_int_gauge_vec_with_registry!(
                "advertised_peer_addr_worker",
                "Worker dial targets peers advertised (DHT record / committee); includes not-yet-connected peers",
                &["peer_id", "multiaddr"],
                registry
            )?,
        })
    }
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(e) => {
                tracing::warn!(target: "rayls::metrics", ?e, "Network::try_new metrics error");
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests do to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}
