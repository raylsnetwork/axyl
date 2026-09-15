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
    pub kad_known_peer_addr_primary: IntGaugeVec,
    /// worker swarm's kademlia routing table; see [`Self::kad_known_peer_addr_primary`].
    pub kad_known_peer_addr_worker: IntGaugeVec,
    /// primary swarm's dial targets: one series per `(peer_id, multiaddr)` a peer ADVERTISED (via
    /// its DHT `NodeRecord` or committee bootstrap) that this node will redial -- including peers
    /// it has not connected to, so undialable/churn addresses show up here. Diff against
    /// `kad_known_peer_addr_primary` by `peer_id` (`advertised unless on(peer_id) kad_known`) to
    /// find targets that never connect. Same separate-vec / reset scheme.
    pub advertised_peer_addr_primary: IntGaugeVec,
    /// worker swarm's dial targets; see [`Self::advertised_peer_addr_primary`].
    pub advertised_peer_addr_worker: IntGaugeVec,
    /// primary swarm's kad-discovery dial candidates: one series per `(peer_id, multiaddr)` in the
    /// peer-manager's `discovery_peers` -- peers learned from OTHER nodes' routing tables via
    /// `get_closest_peers` and dialed on the heartbeat. Distinct from `advertised_peer_addr_*`
    /// (record/committee) and `kad_known_peer_addr_*` (connected): this is where a
    /// cross-host-unreachable address a co-located peer advertised (e.g. a `127.0.0.1`)
    /// surfaces as churn. Same separate-vec / reset scheme.
    pub discovery_peer_addr_primary: IntGaugeVec,
    /// worker swarm's kad-discovery dial candidates; see [`Self::discovery_peer_addr_primary`].
    pub discovery_peer_addr_worker: IntGaugeVec,
    /// Failed outbound dials by target `(peer_id, multiaddr, swarm)` (`swarm` = primary/worker).
    /// Each dial error
    /// increments once per attempted address. A climbing count for an unreachable address (e.g. a
    /// cross-host `127.0.0.1`) is the dial-churn signal -- and unlike the `*_peer_addr` gauges it
    /// captures every dial path (kad iterative query, discovery heartbeat, committee redial),
    /// including kad-internal dials that never land in an app-side map.
    pub dial_peer_addr_failures: IntCounterVec,
    /// This node's own identity, per swarm: `node_peer_addr_self{peer_id, authority, swarm} = 1`.
    /// `authority` is the node's BLS committee key (same across primary+worker); `peer_id` is the
    /// per-swarm libp2p id. Set once at startup, so `grep peer_addr` maps any peer id seen
    /// elsewhere back to a node and its authority.
    pub node_peer_addr_self: IntGaugeVec,
    /// The address(es) this node publishes (its `NodeRecord` / `external_addr`), per swarm. In
    /// practice `NodeRecord::build` uses exactly one, but the record holds a `Vec`. Set once at
    /// startup.
    pub node_peer_addr_external: IntGaugeVec,
    /// Addresses the primary swarm is currently listening on (`swarm.listeners()`). Refreshed each
    /// tick (listeners come and go); separate primary/worker vecs so each can `reset()` its own.
    pub node_peer_addr_listen_primary: IntGaugeVec,
    /// Addresses the worker swarm is currently listening on; see
    /// [`Self::node_peer_addr_listen_primary`].
    pub node_peer_addr_listen_worker: IntGaugeVec,
    /// Primary swarm's desired relay reservations: value `1` = active, `0` = desired but currently
    /// down (relay gone). Refreshed each tick. Surfaces a flapping relay that
    /// `node_peer_addr_listen_*` cannot (a down reservation is not a live listener).
    pub node_peer_addr_reservation_primary: IntGaugeVec,
    /// Worker swarm's desired relay reservations; see
    /// [`Self::node_peer_addr_reservation_primary`].
    pub node_peer_addr_reservation_worker: IntGaugeVec,
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
            kad_known_peer_addr_primary: register_int_gauge_vec_with_registry!(
                "kad_known_peer_addr_primary",
                "Primary kademlia routing table (kbuckets): connected peers and the resolved address in use",
                &["peer_id", "multiaddr"],
                registry
            )?,
            kad_known_peer_addr_worker: register_int_gauge_vec_with_registry!(
                "kad_known_peer_addr_worker",
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
            discovery_peer_addr_primary: register_int_gauge_vec_with_registry!(
                "discovery_peer_addr_primary",
                "Primary kad-discovery dial candidates (discovery_peers: peers learned via get_closest_peers, dialed on heartbeat)",
                &["peer_id", "multiaddr"],
                registry
            )?,
            discovery_peer_addr_worker: register_int_gauge_vec_with_registry!(
                "discovery_peer_addr_worker",
                "Worker kad-discovery dial candidates (discovery_peers: peers learned via get_closest_peers, dialed on heartbeat)",
                &["peer_id", "multiaddr"],
                registry
            )?,
            dial_peer_addr_failures: register_int_counter_vec_with_registry!(
                "dial_peer_addr_failures",
                "Failed outbound dials by target address (increments per attempted multiaddr on each dial error)",
                &["peer_id", "multiaddr", "swarm"],
                registry
            )?,
            node_peer_addr_self: register_int_gauge_vec_with_registry!(
                "node_peer_addr_self",
                "This node's own identity per swarm (peer_id + BLS authority)",
                &["peer_id", "authority", "swarm"],
                registry
            )?,
            node_peer_addr_external: register_int_gauge_vec_with_registry!(
                "node_peer_addr_external",
                "The address(es) this node publishes (its NodeRecord external_addr) per swarm",
                &["multiaddr", "swarm"],
                registry
            )?,
            node_peer_addr_listen_primary: register_int_gauge_vec_with_registry!(
                "node_peer_addr_listen_primary",
                "Addresses the primary swarm is currently listening on",
                &["multiaddr"],
                registry
            )?,
            node_peer_addr_listen_worker: register_int_gauge_vec_with_registry!(
                "node_peer_addr_listen_worker",
                "Addresses the worker swarm is currently listening on",
                &["multiaddr"],
                registry
            )?,
            node_peer_addr_reservation_primary: register_int_gauge_vec_with_registry!(
                "node_peer_addr_reservation_primary",
                "Primary swarm desired relay reservations (1 = active, 0 = desired but currently down)",
                &["multiaddr"],
                registry
            )?,
            node_peer_addr_reservation_worker: register_int_gauge_vec_with_registry!(
                "node_peer_addr_reservation_worker",
                "Worker swarm desired relay reservations (1 = active, 0 = desired but currently down)",
                &["multiaddr"],
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
