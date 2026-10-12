use rayls_execution_evm::{reth_env::RethConfig, NetworkProfile};
use rayls_execution_faucet::FaucetArgs;
use rayls_infrastructure_config::Config;
use rayls_infrastructure_storage::mdbx::MdbxConfig;
use rayls_infrastructure_types::BuildMetadata;
use std::net::SocketAddr;

/// The struct used to build the execution nodes.
///
/// Used to build the node until upstream reth supports
/// broader node customization.
#[derive(Clone, Debug)]
pub struct RaylsBuilder {
    /// The node configuration.
    pub node_config: RethConfig,
    /// Rayls Network config.
    pub rayls_infrastructure_config: Config,
    /// The hardfork schedule the CLI boot gate selected (a `--config-file`
    /// subnet or the `--network` built-in). Every execution-layer constructor
    /// takes it from here; nothing about the schedule is process-global.
    pub profile: NetworkProfile,
    /// TODO: temporary solution until upstream reth
    /// rpc hooks are publicly available.
    pub opt_faucet_args: Option<FaucetArgs>,
    /// Enable Prometheus consensus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    pub metrics: Option<SocketAddr>,
    /// Optional TCP port to start healthcheck service.
    /// If a port is provided, the healthcheck service will spawn. Otherwise, no healthcheck
    /// service starts.
    ///
    /// IMPORTANT: only enable healthcheck if the endpoint is protected by a firewall. The
    /// healthcheck service responds unconditionally. This reads from `HEALTHCHECK_TCP_PORT` env
    /// var.
    pub healthcheck: Option<u16>,

    pub consensus_db_config: MdbxConfig,

    /// Compile-time build metadata for metrics reporting.
    pub build_metadata: BuildMetadata,
}

impl RaylsBuilder {
    pub fn new(
        node_config: RethConfig,
        rayls_infrastructure_config: Config,
        profile: NetworkProfile,
        opt_faucet_args: Option<FaucetArgs>,
        metrics: Option<SocketAddr>,
        healthcheck: Option<u16>,
        build_metadata: BuildMetadata,
    ) -> Self {
        RaylsBuilder::new_with_consensus_db_config(
            node_config,
            rayls_infrastructure_config,
            profile,
            opt_faucet_args,
            metrics,
            healthcheck,
            MdbxConfig::default(),
            build_metadata,
        )
    }

    pub fn new_with_consensus_db_config(
        node_config: RethConfig,
        rayls_infrastructure_config: Config,
        profile: NetworkProfile,
        opt_faucet_args: Option<FaucetArgs>,
        metrics: Option<SocketAddr>,
        healthcheck: Option<u16>,
        consensus_db_config: MdbxConfig,
        build_metadata: BuildMetadata,
    ) -> Self {
        RaylsBuilder {
            node_config,
            rayls_infrastructure_config,
            profile,
            opt_faucet_args,
            metrics,
            healthcheck,
            consensus_db_config,
            build_metadata,
        }
    }
}
