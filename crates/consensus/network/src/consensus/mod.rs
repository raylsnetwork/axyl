//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{RLCodec, RLMessage},
    consensus::behaviour::RLBehavior,
    types::{KadQuery, NetworkCommand, NetworkEvent, NetworkResult, NodeRecord},
    NetworkMetrics,
};
use libp2p::{
    kad::QueryId,
    request_response::{InboundRequestId, OutboundRequestId},
    PeerId, Swarm,
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
pub(crate) mod kad;
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
    /// bls key associated with the desired authority's [NodeRecord]. The query runs until
    /// the last step. During this time, results are tracked and compared to one another to
    /// ensure the latest valid record is used for the peer's info.
    kad_record_queries: HashMap<QueryId, KadQuery>,
    /// Kad queries that are expected to fail because of having 0 peers.
    kad_expecting_to_fail_query_ids: HashSet<QueryId>,
    /// Fixed-window count of inbound kad PutRecord requests per peer, as `(window start in
    /// seconds, puts seen)`.
    ///
    /// Every put reaching the verify path costs a BLS verification on the swarm event loop and a
    /// validly signed replay earns no penalty, so without a budget one peer can convert puts into
    /// event-loop CPU at will. Honest nodes republish their record on the order of minutes, so the
    /// budget only bites floods.
    pub(super) kad_put_budget: HashMap<PeerId, (u64, u32)>,
    /// The configurables for the libp2p consensus network implementation.
    config: LibP2pConfig,
    /// Track peers we have a connection with.
    ///
    /// Tracked explicitly as a VecDeque so requests can round-robin over peers when the caller
    /// names none.
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
    /// Network metrics for the peer manager.
    network_metrics: Arc<NetworkMetrics>,
    /// A label for the network to use in metrics.
    network_label: &'static str,
}
