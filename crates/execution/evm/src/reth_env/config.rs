use crate::{dirs::path_to_datadir, rpc_server_args::RpcServerArgs};
use clap::Parser;
use rayls_infrastructure_types::ETHEREUM_BLOCK_GAS_LIMIT_56BITS;
use reth::{
    args::{
        DatabaseArgs, DebugArgs, DevArgs, DiscoveryArgs, EngineArgs, EraArgs, EraSourceArgs,
        MetricArgs, NetworkArgs, PayloadBuilderArgs, PruningArgs, StorageArgs, TxPoolArgs,
    },
    builder::NodeConfig,
    network::transactions::{config::TransactionPropagationKind, TransactionPropagationMode},
    rpc::builder::{RethRpcModule, RpcModuleSelection},
};
use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
use reth_discv4::NatResolver;
use reth_node_builder::{
    DEFAULT_MEMORY_BLOCK_BUFFER_TARGET, DEFAULT_PERSISTENCE_THRESHOLD, DEFAULT_RESERVED_CPU_CORES,
};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tracing::warn;

/// Reth [`MetricArgs`] with `--reth-metrics` prefix to avoid collision with consensus `--metrics`.
#[derive(Debug, Clone, Default, Parser)]
pub struct RethMetricArgs {
    /// Enable Prometheus metrics for reth execution-layer components.
    #[arg(long = "reth-metrics", value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Reth Metrics"
    )]
    pub prometheus: Option<SocketAddr>,

    /// Push gateway URL for reth metrics.
    #[arg(
        long = "reth-metrics.push.url",
        value_name = "PUSH_GATEWAY_URL",
        help_heading = "Reth Metrics"
    )]
    pub push_gateway_url: Option<String>,

    /// Push interval in seconds (default: 5).
    #[arg(long = "reth-metrics.push.interval", default_value = "5", value_parser = parse_duration_from_secs, value_name = "SECONDS", help_heading = "Reth Metrics"
    )]
    pub push_gateway_interval: Duration,
}

impl From<RethMetricArgs> for MetricArgs {
    fn from(args: RethMetricArgs) -> Self {
        MetricArgs {
            prometheus: args.prometheus,
            push_gateway_url: args.push_gateway_url,
            push_gateway_interval: args.push_gateway_interval,
        }
    }
}

/// Reth specific command line args.
#[derive(Debug, Parser, Clone)]
pub struct RethCommand {
    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,

    /// All storage related arguments
    #[clap(flatten)]
    pub storage: StorageArgs,

    /// All pruning related arguments (--full, --minimal, etc.)
    #[clap(flatten)]
    pub pruning: PruningArgs,

    /// All reth metrics arguments (--reth-metrics prefix)
    #[clap(flatten)]
    pub reth_metrics: RethMetricArgs,
}

/// A wrapper abstraction around a Reth node config.
#[derive(Clone, Debug)]
pub struct RethConfig(pub(crate) NodeConfig<RethChainSpec>);

const DEFAULT_UNUSED_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

/// RPC modules the node exposes; `admin` is deliberately excluded.
pub(super) const ALL_MODULES: [RethRpcModule; 7] = [
    RethRpcModule::Eth,
    RethRpcModule::Net,
    RethRpcModule::Web3,
    RethRpcModule::Debug,
    RethRpcModule::Trace,
    RethRpcModule::Rpc,
    RethRpcModule::Txpool,
];

impl RethConfig {
    /// Make sure that some modules are not selected, primarily they won't work as expected with RL
    /// (or at all).
    pub(super) fn validate_rpc_modules(mods: &mut Option<RpcModuleSelection>) {
        match &mods {
            Some(RpcModuleSelection::All) => {
                *mods = Some(RpcModuleSelection::Selection(HashSet::from(ALL_MODULES)));
            }
            Some(RpcModuleSelection::Standard) => {}
            Some(RpcModuleSelection::Selection(hash_set)) => {
                let mut new_set = HashSet::new();
                for r in ALL_MODULES {
                    if hash_set.contains(&r) {
                        new_set.insert(r);
                    }
                }
                if hash_set.contains(&RethRpcModule::Admin) {
                    warn!(target: "rayls::reth", "Attempted to configure unsupported admin RPC module!");
                }
                *mods = Some(RpcModuleSelection::Selection(new_set));
            }
            None => {}
        }
    }

    /// Create a new RethConfig wrapper.
    pub fn new<P: AsRef<Path>>(
        reth_config: RethCommand,
        instance: Option<u16>,
        datadir: P,
        with_unused_ports: bool,
        chain: Arc<RethChainSpec>,
    ) -> Self {
        // create a reth DatadirArgs from rayls datadir
        let datadir = path_to_datadir(datadir.as_ref());

        let RethCommand { mut rpc, txpool, db, storage, pruning, reth_metrics } = reth_config;
        Self::validate_rpc_modules(&mut rpc.http_api);
        Self::validate_rpc_modules(&mut rpc.ws_api);
        // We don't just use Default for these Reth args.
        // This will force us to look at new options and make sure they are good for our use.
        // We DO NOT use the Reth networking so these settings should reflect that.
        let network = NetworkArgs {
            discovery: DiscoveryArgs {
                disable_discovery: true,
                disable_nat: true,
                disable_dns_discovery: true,
                disable_discv4_discovery: true,
                enable_discv5_discovery: false,
                addr: DEFAULT_UNUSED_ADDR,
                port: 0,
                ..Default::default()
            },
            trusted_only: false,
            trusted_peers: vec![],
            bootnodes: None,
            dns_retries: 0,
            peers_file: None,
            identity: "Reth Null Network".to_string(),
            p2p_secret_key: None,
            no_persist_peers: true,
            nat: NatResolver::None,
            addr: DEFAULT_UNUSED_ADDR,
            port: 0,
            max_outbound_peers: None,
            max_inbound_peers: None,
            max_concurrent_tx_requests: 0,
            max_concurrent_tx_requests_per_peer: 0,
            max_seen_tx_history: 0,
            max_pending_pool_imports: 0,
            soft_limit_byte_size_pooled_transactions_response: 0,
            soft_limit_byte_size_pooled_transactions_response_on_pack_request: 0,
            max_capacity_cache_txns_pending_fetch: 0,
            net_if: None,
            tx_propagation_policy: TransactionPropagationKind::Trusted,
            disable_tx_gossip: true,
            propagation_mode: TransactionPropagationMode::Max(0),
            required_block_hashes: vec![],
            ..Default::default()
        };

        // Not using the Reth payload builder.
        let builder = PayloadBuilderArgs {
            extra_data: "rayls-execution-evm-na".to_string(),
            gas_limit: Some(ETHEREUM_BLOCK_GAS_LIMIT_56BITS),
            interval: Duration::from_secs(1),
            deadline: Duration::from_secs(1),
            max_payload_tasks: 0,
            ..Default::default()
        };
        let debug = DebugArgs::default();
        // No Reth dev options.
        let dev = DevArgs::default();
        // Parameters for configuring the engine driver.
        #[allow(deprecated)]
        let engine = EngineArgs {
            persistence_threshold: DEFAULT_PERSISTENCE_THRESHOLD,
            memory_block_buffer_target: DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
            legacy_state_root_task_enabled: false,
            reserved_cpu_cores: DEFAULT_RESERVED_CPU_CORES,
            cross_block_cache_size: 256,
            ..Default::default()
        };

        // Parameters to configure block history syncing.
        let era = EraArgs { enabled: false, source: EraSourceArgs { path: None, url: None } };

        let mut this = NodeConfig {
            config: None,
            chain,
            metrics: reth_metrics.into(),
            instance,
            datadir,
            network,
            rpc: rpc.into(),
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            engine,
            era,
            storage,
            static_files: Default::default(),
        };
        if with_unused_ports {
            this = this.with_unused_ports();
        }
        // adjust rpc instance ports
        this.adjust_instance_ports();

        Self(this)
    }
}
