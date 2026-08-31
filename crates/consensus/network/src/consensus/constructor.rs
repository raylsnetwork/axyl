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
use libp2p::{
    core::upgrade::Version, gossipsub, kad, relay, request_response, Multiaddr, PeerId,
    SwarmBuilder, Transport,
};
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
            // Announce IDONTWANT to mesh peers as soon as we publish, not only on receive:
            // published batches approach max_transmit_size, and each duplicate a mesh peer sends
            // back costs a full path transit (doubly so when links hairpin through relay
            // circuits). Peers negotiate gossipsub v1.2 per connection, so this is a no-op toward
            // peers that don't support it.
            .idontwant_on_publish(true)
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

        let network_pubkey = keypair.public().into();
        let peer_config = network_config.peer_config();

        // DNS resolver used by the transport for /dns4 & /dnsaddr resolution.
        let (resolver_cfg, resolver_opts) = dns_resolver_config()?;
        // A standalone resolver with the same config, used to resolve committee `/dnsaddr` peers at
        // ingest so we can learn (and exempt) the relays we dial through -- configless: the relay
        // set is discovered from DNS, not passed in. See `discover_and_register_relays`. Idle
        // unless a `/dnsaddr` address is actually ingested, so direct/`--relay` setups are
        // unaffected.
        let relay_resolver = hickory_resolver::TokioResolver::builder_with_config(
            resolver_cfg.clone(),
            hickory_resolver::name_server::TokioConnectionProvider::default(),
        )
        .with_options(resolver_opts.clone())
        .build();

        // Relay client transport + behaviour. Built manually (rather than `.with_relay_client`) so
        // we control transport ordering below.
        let (relay_transport, relay_behaviour) = relay::client::new(peer_id);

        // create swarm
        //
        // Transport ordering matters: we add the relay client transport as an "other transport"
        // (OR'd with QUIC) BEFORE `.with_dns_config`, so the DNS transport ends up on the OUTSIDE:
        // `dns(or(quic, relay))`. That way a `/dnsaddr/.../p2p/<peer>` address is first resolved by
        // DNS into a `<relay>/p2p-circuit/p2p/<peer>` address and THEN dialed through the relay
        // transport. The builder's `.with_relay_client` puts relay outside DNS, which would resolve
        // the circuit and then (wrongly) dial it on QUIC -- so /dnsaddr failover would never open a
        // circuit. The relayed connection is upgraded with noise + yamux; the hop to the relay is
        // QUIC. Relay behaviour is only exercised for circuit addresses, so direct nets are
        // unaffected.
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic_config(|config| network_config.quic_config().apply(config))
            .with_other_transport(|keypair| {
                // `with_other_transport` boxes the muxer itself, so stop at `.multiplex`.
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    relay_transport
                        .upgrade(Version::V1Lazy)
                        .authenticate(libp2p::noise::Config::new(keypair)?)
                        .multiplex(libp2p::yamux::Config::default()),
                )
            })
            .map_err(|_| NetworkError::BuildSwarm)?
            // Resolve /dns4, /dns6 and /dnsaddr multiaddrs. Placed AFTER the relay transport so DNS
            // wraps it (see note above). Uses RAYLS_DNS_SERVER when set, else the system resolver.
            .with_dns_config(resolver_cfg, resolver_opts)
            .with_behaviour(|_| {
                RLBehavior::new(gossipsub, req_res, kademlia, peer_config, relay_behaviour, peer_id)
            })
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(
                    network_config.libp2p_config().max_idle_connection_timeout,
                )
            })
            .build();

        // set external address
        swarm.add_external_address(external_addr.clone());

        // If this node is reached via a relay, its own external address is a `/p2p-circuit` on that
        // relay. Register that relay as protected so the node doesn't ban its own relay (which
        // would drop the reservation and close its only listener). Committee peers' relays
        // are registered via `add_known_peer`, but a node's own relay -- especially for
        // non-committee nodes like observers -- is only visible here.
        swarm
            .behaviour_mut()
            .peer_manager
            .register_relays_from_addrs(std::slice::from_ref(&external_addr));

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
            config,
            connected_peers: VecDeque::new(),
            pending_px_disconnects,
            key_config,
            task_spawner,
            node_record,
            last_cleanup: Instant::now(),
            network_metrics,
            network_label,
            relay_reservations: Default::default(),
            relay_resolver,
            connection_paths: Default::default(),
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

/// Build the DNS resolver config for the transport.
///
/// If `RAYLS_DNS_SERVER` is set (e.g. `127.0.0.1:5353` for a local dnsmasq serving `/dnsaddr`
/// records), resolve exclusively against that server -- avoids touching the system resolver /
/// systemd-resolved. Otherwise fall back to the system configuration (`/etc/resolv.conf`), i.e.
/// the same behaviour as plain `.with_dns()`.
fn dns_resolver_config(
) -> NetworkResult<(hickory_resolver::config::ResolverConfig, hickory_resolver::config::ResolverOpts)>
{
    use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};

    let (cfg, mut opts) = match std::env::var("RAYLS_DNS_SERVER") {
        Ok(server) => {
            let addr: std::net::SocketAddr = server
                .parse()
                .map_err(|e| NetworkError::DnsConfig(format!("invalid RAYLS_DNS_SERVER: {e}")))?;
            // trust_negative_responses=false: don't cache an early NXDOMAIN/empty (e.g. a query
            // racing dnsmasq startup) as authoritative -- retry instead, so a transient miss
            // doesn't lock a name out for the resolver's cache lifetime.
            let name_servers =
                NameServerConfigGroup::from_ips_clear(&[addr.ip()], addr.port(), false);
            (ResolverConfig::from_parts(None, vec![], name_servers), ResolverOpts::default())
        }
        Err(_) => hickory_resolver::system_conf::read_system_conf()
            .map_err(|e| NetworkError::DnsConfig(format!("failed to read system resolver: {e}")))?,
    };
    // Enable EDNS0: a `/dnsaddr` fan-out returns several long TXT records (each a full circuit
    // multiaddr), easily exceeding the 512-byte classic UDP limit. Without EDNS0 the response is
    // truncated (TC=1), forcing a TCP fallback that stalls -- the resolver then retries one name
    // forever and never resolves the rest. EDNS0 advertises a large UDP buffer so the whole answer
    // arrives in one datagram.
    opts.edns0 = true;
    Ok((cfg, opts))
}
