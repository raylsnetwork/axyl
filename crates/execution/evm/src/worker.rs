//! Worker-side stand-in for the reth network traits its RPC namespaces require.
//!
//! Workers have no devp2p network, but reth's `net`, `web3`, and `eth` RPC modules are generic
//! over one; this answers their queries from the worker's libp2p peer count and leaves the
//! `admin`-only methods as no-ops.

use crate::{reth_env::ChainSpec, WorkerTxPool};
use rayls_consensus_worker::WorkerNetworkHandle;
use reth::{network::config::SecretKey, rpc::builder::RpcServerHandle};
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_discv4::DEFAULT_DISCOVERY_PORT;
use reth_eth_wire::DisconnectReason;
use reth_network_api::{
    EthProtocolInfo, NetworkError, NetworkInfo, NetworkStatus, PeerInfo, PeerKind, Peers,
    PeersInfo, Reputation, ReputationChangeKind,
};
use reth_network_peers::{Enr, NodeRecord, PeerId as RethPeerId};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

/// Execution components on a per-worker basis.
#[derive(Debug)]
pub struct WorkerComponents {
    /// The RPC handle.
    rpc_handle: RpcServerHandle,
    /// The worker's transaction pool.
    pool: WorkerTxPool,
    /// Network stand-in, kept so its peer-count task can be respawned each epoch.
    network: WorkerNetwork,
}

impl WorkerComponents {
    /// Create a new instance of [Self].
    pub fn new(rpc_handle: RpcServerHandle, pool: WorkerTxPool, network: WorkerNetwork) -> Self {
        Self { rpc_handle, pool, network }
    }

    /// Return a reference to the rpc handle.
    pub fn rpc_handle(&self) -> &RpcServerHandle {
        &self.rpc_handle
    }

    /// Return a reference to the worker's transaction pool.
    pub fn pool(&self) -> WorkerTxPool {
        self.pool.clone()
    }

    /// Return the worker network interface (RPC helper) for this worker.
    pub fn worker_network(&self) -> &WorkerNetwork {
        &self.network
    }
}

/// Implementation of the reth network traits behind the `net`, `web3`, and `eth` RPC namespaces.
///
/// Most methods are no-ops: they back the `admin` namespace, which Rayls does not serve.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerNetwork {
    /// Chain spec.
    chain_spec: RethChainSpec,
    /// Connected peer count, refreshed by the polling task and served to `net_peerCount`.
    peer_count: Arc<AtomicUsize>,
    /// App version.
    version: &'static str,
}

impl WorkerNetwork {
    /// Create a new instance of self.
    pub fn new(
        chain_spec: ChainSpec,
        worker_network: WorkerNetworkHandle,
        version: &'static str,
    ) -> Self {
        let peer_count = Arc::new(AtomicUsize::new(0));
        Self::spawn_peer_count_task(&peer_count, worker_network);
        Self { chain_spec: chain_spec.reth_chain_spec(), peer_count, version }
    }

    /// Spawn a new task to keep up with peer counts.
    /// Use this when the epoch rolls over and the worker_network gets a new task manager.
    pub fn respawn_peer_count(&self, worker_network: WorkerNetworkHandle) {
        Self::spawn_peer_count_task(&self.peer_count, worker_network);
    }

    /// Spawn the peer-count polling task on the handle's task spawner.
    fn spawn_peer_count_task(peer_count: &Arc<AtomicUsize>, worker_network: WorkerNetworkHandle) {
        let peer_count = peer_count.clone();
        let spawner = worker_network.get_task_spawner().clone();
        spawner.spawn_task("Worker Network Peers", async move {
            // Bounded so a task that outlives its abort cannot poll forever; the epoch rollover
            // respawns it.
            const MAX_ITERATIONS: u32 = 10_000; // ~41 hours at 15 sec intervals
            for _ in 0..MAX_ITERATIONS {
                if let Ok(peers) = worker_network.connected_peers_count().await {
                    peer_count.store(peers, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
            tracing::debug!(target: "worker", "Peer count task reached max iterations, exiting");
        });
    }
}

impl NetworkInfo for WorkerNetwork {
    // Rayls Unused
    fn local_addr(&self) -> SocketAddr {
        (IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED), DEFAULT_DISCOVERY_PORT).into()
    }

    #[allow(deprecated, reason = "EthProtocolInfo::difficulty is deprecated")]
    async fn network_status(&self) -> Result<NetworkStatus, NetworkError> {
        Ok(NetworkStatus {
            client_version: self.version.to_string(), // web3_clientVersion
            protocol_version: 1,                      // eth_protocolVersion
            eth_protocol_info: EthProtocolInfo {
                difficulty: None,
                network: self.chain_id(),
                genesis: self.chain_spec.genesis_hash(),
                head: Default::default(),
                config: self.chain_spec.genesis().config.clone(),
            },
            capabilities: vec![],
        })
    }

    // eth_chainId AND net_version
    fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    fn is_syncing(&self) -> bool {
        false
    }

    fn is_initially_syncing(&self) -> bool {
        false
    }
}

impl PeersInfo for WorkerNetwork {
    // net_peerCount
    fn num_connected_peers(&self) -> usize {
        self.peer_count.load(Ordering::Relaxed)
    }

    // Rayls Unused
    fn local_node_record(&self) -> NodeRecord {
        NodeRecord::new(self.local_addr(), RethPeerId::random())
    }

    // Rayls Unused
    fn local_enr(&self) -> Enr<SecretKey> {
        let sk = SecretKey::from_slice(&[0xcd; 32]).expect("secret key derived from static slice");
        Enr::builder().build(&sk).expect("ENR builds from key")
    }
}

// These appear to support Reth's admin namespace- Rayls does not use this.
impl Peers for WorkerNetwork {
    fn add_trusted_peer_id(&self, _peer: RethPeerId) {}

    fn add_peer_kind(
        &self,
        _peer: RethPeerId,
        _kind: PeerKind,
        _tcp_addr: SocketAddr,
        _udp_addr: Option<SocketAddr>,
    ) {
    }

    async fn get_peers_by_kind(&self, _kind: PeerKind) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    async fn get_all_peers(&self) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    async fn get_peer_by_id(&self, _peer_id: RethPeerId) -> Result<Option<PeerInfo>, NetworkError> {
        Ok(None)
    }

    async fn get_peers_by_id(
        &self,
        _peer_id: Vec<RethPeerId>,
    ) -> Result<Vec<PeerInfo>, NetworkError> {
        Ok(vec![])
    }

    fn remove_peer(&self, _peer: RethPeerId, _kind: PeerKind) {}

    fn disconnect_peer(&self, _peer: RethPeerId) {}

    fn disconnect_peer_with_reason(&self, _peer: RethPeerId, _reason: DisconnectReason) {}

    fn reputation_change(&self, _peer_id: RethPeerId, _kind: ReputationChangeKind) {}

    async fn reputation_by_id(
        &self,
        _peer_id: RethPeerId,
    ) -> Result<Option<Reputation>, NetworkError> {
        Ok(None)
    }

    fn connect_peer_kind(
        &self,
        _peer: RethPeerId,
        _kind: PeerKind,
        _tcp_addr: SocketAddr,
        _udp_addr: Option<SocketAddr>,
    ) {
        // unimplemented!
    }
}
