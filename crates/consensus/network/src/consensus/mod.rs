//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{RLCodec, RLMessage},
    consensus::behaviour::RLBehavior,
    types::{ConnectionPath, KadQuery, NetworkCommand, NetworkEvent, NetworkResult, NodeRecord},
    NetworkMetrics,
};
use libp2p::{
    core::transport::ListenerId,
    kad::QueryId,
    request_response::{InboundRequestId, OutboundRequestId},
    swarm::ConnectionId,
    Multiaddr, PeerId, Swarm,
};
use rayls_infrastructure_config::{KeyConfig, LibP2pConfig};
use rayls_infrastructure_types::{BlsPublicKey, Database, RaylsSender, TaskSpawner};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Instant,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};

mod behaviour;
mod command;
mod constructor;
mod debug;
mod gossip;
mod kad;
mod maintenance;
mod peer_events;
mod reqres;
mod runtime;
mod types;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
pub struct ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>>,
{
    /// The gossip network for flood publishing sealed batches.
    swarm: Swarm<RLBehavior<RLCodec<Req, Res>, DB>>,
    /// The stream for forwarding network events.
    event_stream: Events,
    /// The sender for network handles.
    handle: Sender<NetworkCommand<Req, Res>>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand<Req, Res>>,
    /// The collection of authorized publishers per topic.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify messages
    /// published on certain topics. These are updated when the caller subscribes to a topic.
    authorized_publishers: HashMap<String, Option<HashSet<BlsPublicKey>>>,
    /// The collection of pending _graceful_ disconnects.
    ///
    /// This node disconnects from new peers if it already has the target number of peers.
    /// For these types of "peer exchange / discovery disconnects", the node shares peer records
    /// before disconnecting. This keeps track of the number of disconnects to ensure resources
    /// aren't starved while waiting for the peer's ack.
    pending_px_disconnects: HashMap<OutboundRequestId, PeerId>,
    /// The collection of pending outbound requests.
    ///
    /// Callers include a oneshot channel for the network to return response. The caller is
    /// responsible for decoding message bytes and reporting peers who return bad data. Peers that
    /// send messages that fail to decode must receive an application score penalty.
    outbound_requests: HashMap<(PeerId, OutboundRequestId), oneshot::Sender<NetworkResult<Res>>>,
    /// The collection of pending inbound requests.
    ///
    /// Callers include a oneshot channel for the network to return a cancellation notice. The
    /// caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    inbound_requests: HashMap<InboundRequestId, oneshot::Sender<()>>,
    /// The collection of kademlia record requests.
    ///
    /// When the application layer makes a request, the swarm stores the kad::QueryId and the
    /// the bls key associated with the desired authority's [NodeRecord]. The query runs until
    /// the last step. During this time, results are tracked and compared to one another to
    /// ensure the latest valid record is used for the peer's info.
    kad_record_queries: HashMap<QueryId, KadQuery>,
    /// Kad queries that are expected to fail because of having 0 peers
    kad_expecting_to_fail_query_ids: HashSet<QueryId>,
    /// The configurables for the libp2p consensus network implementation.
    config: LibP2pConfig,
    /// Track peers we have a connection with.
    ///
    /// This explicitly tracked and is a VecDeque so we can use to round robin requests without an
    /// explicit peer.
    connected_peers: VecDeque<PeerId>,
    /// Key manager, provide the BLS public key and sign peer records published to kademlia.
    key_config: KeyConfig,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The signed [NodeRecord].
    ///
    /// The external address is self-reported and unconfirmed.
    node_record: NodeRecord,
    /// Last time cleanup was performed for time-based cleanup.
    last_cleanup: Instant,
    ///Network metrics for the peer manager
    network_metrics: Arc<NetworkMetrics>,
    /// A label for the network to use in metrics.
    network_label: &'static str,
    /// Desired relay reservations (the `/p2p-circuit` addresses passed to `StartListening`), each
    /// mapped to its listener id while the reservation is established. `None` marks a reservation
    /// whose relay went away; `retry_relay_reservations` re-issues it so a returning relay
    /// restores reachability without a restart.
    relay_reservations: HashMap<Multiaddr, Option<ListenerId>>,
    /// The transport path each live connection was established over, classified once at
    /// `ConnectionEstablished` and removed on `ConnectionClosed`.
    ///
    /// Lets every request/response be attributed to the path it traveled (circuit vs direct), the
    /// audit trail proving a relayed-only node exchanges consensus messages exclusively through
    /// its relays.
    connection_paths: HashMap<ConnectionId, ConnectionPath>,
    /// Resolver for `/dnsaddr` committee peers, used to discover (and exempt) the relays a node
    /// dials through -- so the relay set is learned from DNS rather than configured.
    relay_resolver: hickory_resolver::TokioResolver,
}
