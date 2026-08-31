use crate::{
    codec::{RLCodec, RLMessage},
    consensus::{
        behaviour::RLBehavior,
        types::{PRIMARY_KAD_PROTO_NAME, WORKER_KAD_PROTO_NAME},
    },
    error::NetworkError,
    kad::{KadStore, KadStoreType},
    types::{NetworkEvent, NetworkHandle, NetworkResult, NodeRecord},
    ConsensusNetwork, NetworkMetrics,
};
use libp2p::{gossipsub, kad, request_response, Multiaddr, PeerId, SwarmBuilder};
use rayls_infrastructure_config::{KeyConfig, NetworkConfig};
use rayls_infrastructure_types::{
    BlsSigner, Database, NetworkKeypair, NetworkPublicKey, RaylsSender, TaskSpawner,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Convenience method for spawning a primary network instance.
    pub fn new_for_primary(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
        network_metrics: Arc<NetworkMetrics>,
    ) -> NetworkResult<Self> {
        let network_key = key_config.primary_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            KadStoreType::Primary,
            external_addr,
            network_metrics,
        )
    }

    /// Convenience method for spawning a worker network instance.
    pub fn new_for_worker(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
        network_metrics: Arc<NetworkMetrics>,
    ) -> NetworkResult<Self> {
        let network_key = key_config.worker_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            KadStoreType::Worker,
            external_addr,
            network_metrics,
        )
    }

    /// Create a new instance of Self.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        keypair: NetworkKeypair,
        db: DB,
        task_spawner: TaskSpawner,
        kad_type: KadStoreType,
        external_addr: Multiaddr,
        network_metrics: Arc<NetworkMetrics>,
    ) -> NetworkResult<Self> {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .max_transmit_size(2 * 1024 * 1024) // 2 MiB
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // RL specific: filter against authorized_publishers for certain topics
            .validate_messages()
            .build()?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(NetworkError::GossipBehavior)?;

        let rayls_codec =
            RLCodec::<Req, Res>::new(network_config.libp2p_config().max_rpc_message_size);

        let req_res = request_response::Behaviour::with_codec(
            rayls_codec,
            network_config.libp2p_config().supported_req_res_protocols.clone(),
            request_response::Config::default(),
        );
        let peer_id: PeerId = keypair.public().into();
        let kad_proto = match kad_type {
            KadStoreType::Primary => PRIMARY_KAD_PROTO_NAME,
            KadStoreType::Worker => WORKER_KAD_PROTO_NAME,
        };
        let mut kad_config = libp2p::kad::Config::new(kad_proto);
        // manually add peers
        kad_config.set_kbucket_inserts(kad::BucketInserts::Manual);
        kad_config.set_kbucket_size(network_config.libp2p_config().k_bucket_size);
        let two_days = Some(Duration::from_secs(48 * 60 * 60));
        let twelve_hours = Some(Duration::from_secs(12 * 60 * 60));
        kad_config
            .set_record_ttl(two_days)
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            .set_publication_interval(twelve_hours)
            .set_query_timeout(Duration::from_secs(60))
            .set_provider_record_ttl(two_days);
        let kad_store = KadStore::new(db.clone(), &key_config, kad_type);
        let kademlia = kad::Behaviour::with_config(peer_id, kad_store.clone(), kad_config);

        // create custom behavior
        let behavior = RLBehavior::new(gossipsub, req_res, kademlia, network_config.peer_config());

        let network_pubkey = keypair.public().into();

        // create swarm
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic_config(|mut config| {
                config.handshake_timeout = network_config.quic_config().handshake_timeout;
                config.max_idle_timeout = network_config.quic_config().max_idle_timeout;
                config.keep_alive_interval = network_config.quic_config().keep_alive_interval;
                config.max_concurrent_stream_limit =
                    network_config.quic_config().max_concurrent_stream_limit;
                config.max_stream_data = network_config.quic_config().max_stream_data;
                config.max_connection_data = network_config.quic_config().max_connection_data;
                config
            })
            .with_behaviour(|_| behavior)
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(
                    network_config.libp2p_config().max_idle_connection_timeout,
                )
            })
            .build();

        // set external address
        swarm.add_external_address(external_addr.clone());

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let config = network_config.libp2p_config().clone();
        let pending_px_disconnects = HashMap::with_capacity(config.max_px_disconnects);
        let node_record = Self::create_node_record(external_addr, &key_config, network_pubkey);
        let network_label = match kad_type {
            KadStoreType::Primary => "primary",
            KadStoreType::Worker => "worker",
        };
        Ok(Self {
            swarm,
            handle,
            commands,
            event_stream,
            authorized_publishers: Default::default(),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            kad_record_queries: Default::default(),
            kad_expecting_to_fail_query_ids: Default::default(),
            kad_put_budget: Default::default(),
            config,
            connected_peers: VecDeque::new(),
            pending_px_disconnects,
            key_config,
            task_spawner,
            node_record,
            last_cleanup: Instant::now(),
            network_metrics,
            network_label,
        })
    }

    /// Return a [NetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Create and sign this node's [NodeRecord].
    fn create_node_record(
        external_addr: Multiaddr,
        key_config: &KeyConfig,
        network_pubkey: NetworkPublicKey,
    ) -> NodeRecord {
        NodeRecord::build(network_pubkey, external_addr, |data| {
            key_config.request_signature_direct(data)
        })
    }
}
