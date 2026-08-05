//! Main node command
//!
//! Starts the client
use crate::{args::ConsensusDatabaseArgs, version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Parser};
use core::fmt;
use fdlimit::raise_fd_limit;
use rayls_execution_evm::{
    parse_socket_address,
    reth_env::{RethCommand, RethConfig},
};
use rayls_infrastructure_config::Config;
// dev-only: reading the committee file for the single-validator gating check
#[cfg(feature = "dev-single-node-setup")]
use rayls_infrastructure_config::{ConfigFmt, ConfigTrait as _, RaylsDirs};
#[cfg(feature = "dev-single-node-setup")]
use rayls_infrastructure_types::Committee;
use rayls_infrastructure_types::{BuildMetadata, RaylsNetwork};
use rayls_middleware_orchestrator::engine::RaylsBuilder;
use rayon::ThreadPoolBuilder;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, thread::available_parallelism};
use tracing::*;

/// Chain-ids that must never be paired with `--dev`. Mainnet only — testnet and
/// the devnet default both use chain-id 2017, so this list cannot exclude testnet
/// without also blocking valid local devnet use.
#[cfg(feature = "dev-single-node-setup")]
const PROD_CHAIN_IDS: &[u64] = &[487];

/// Enforce the single-node-only invariant for `dev` feature builds, before the node boots.
///
/// The `dev-single-node-setup` feature is for local single-validator dev chains only, so a
/// dev build refuses to run a multi-validator committee — build without the feature (a
/// production build) for real networks. This is the runtime enforcement point: the
/// `Committee` constructor is deliberately left permissive under the feature (`>= 1`) because
/// it is shared by the multi-validator consensus test suite, so the invariant is checked here
/// against the committee the node actually loads.
///
/// A committee of size 0 (missing/default committee file) is left alone — the real
/// "no committee" error surfaces later when consensus loads it. `--dev` additionally may
/// never target a production chain-id (mainnet).
#[cfg(feature = "dev-single-node-setup")]
fn check_dev_mode(dev: bool, committee_size: usize, chain_id: u64) -> eyre::Result<()> {
    if committee_size > 1 {
        eyre::bail!(
            "dev builds are single-node only: refusing to start with a {committee_size}-validator \
             committee. Rebuild without `--features dev-single-node-setup` (a production build) to \
             run a multi-validator network."
        );
    }
    if dev && PROD_CHAIN_IDS.contains(&chain_id) {
        eyre::bail!(
            "--dev cannot be used with production chain-id {chain_id} \
             (production chain-ids: {PROD_CHAIN_IDS:?})"
        );
    }
    Ok(())
}

/// Avaliable "named" chains.
/// These will have embedded config files and can be joined after gereating keys.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum NamedChain {
    /// Testnet
    Testnet,
    /// Mainnet
    Mainnet,
}

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Join a named rayls network (for instance test or main net).
    #[arg(long, value_name = "NAMED_RL_NETWORK", verbatim_doc_comment)]
    pub chain: Option<NamedChain>,

    /// Enable Prometheus consensus metrics, served at the given interface and port.
    ///
    /// Overrides `metrics_address` in parameters.yaml when passed. If neither is set,
    /// consensus metrics stay off.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Consensus Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - `HTTP_RPC_PORT`: default - `instance` + 1
    /// - `WS_RPC_PORT`: default + `instance` * 2 - 2
    /// - `IPC_PATH`: default + `-instance`
    #[arg(long, value_name = "INSTANCE", global = true,  value_parser = value_parser!(u16).range(..=200))]
    pub instance: Option<u16>,

    /// Is this an observer node?  True if set, an observer will never be in the committee
    /// but will follow consensus and provide node RPC access.
    #[arg(long, value_name = "OBSERVER", global = true, default_value_t = false)]
    pub observer: bool,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    /// Additional reth arguments
    #[clap(flatten)]
    pub reth: RethCommand,

    /// Consensus db arguments
    #[clap(flatten)]
    pub consensus_db: ConsensusDatabaseArgs,

    /// TCP health check endpoint port.
    ///
    /// When a port is specified, the node will spawn a TCP health check service
    /// on that port. The health check endpoint is useful for load balancers and
    /// monitoring systems to verify that the node process is running.
    ///
    /// If not specified, the health check service will not be started.
    ///
    /// WARNING: ensure the health endpoint is behind a firewall.
    /// Each connection is handled synchronously in the main accept loop.
    /// No connection limits or rate limiting are implemented.
    /// Connections are immediately closed after sending response.
    #[arg(long, value_name = "HEALTHCHECK_TCP_PORT", global = true, env = "HEALTHCHECK_TCP_PORT")]
    pub healthcheck: Option<u16>,

    /// Override the Rayls network hardfork profile from parameters.yaml.
    ///
    /// Selects which baked-in hardfork schedule to use (devnet, testnet, mainnet).
    /// When set, overrides the `network` field in parameters.yaml without requiring
    /// a re-genesis. Useful for activating hardforks on existing networks.
    #[arg(long, value_name = "RAYLS_NETWORK", global = true, env = "RAYLS_NETWORK")]
    pub network: Option<RaylsNetwork>,

    /// Run as a single-node developer network.
    ///
    /// Redundant in a `dev-single-node-setup` build (always in dev mode); accepted
    /// for compatibility.
    ///
    /// WARNING: for local development and demos only — NOT FOR PRODUCTION USE.
    /// Refuses to start if the configured chain-id matches a known production
    /// network (mainnet = 487). Pair with a single-validator genesis generated
    /// locally via `keytool generate validator` and `genesis`.
    #[cfg(feature = "dev-single-node-setup")]
    #[arg(long, default_value_t = false, conflicts_with = "chain")]
    pub dev: bool,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext,
}

impl<Ext: clap::Args + fmt::Debug> NodeCommand<Ext> {
    /// Execute `node` command
    #[instrument(level = "info", skip_all)]
    pub fn execute<L>(
        #[cfg_attr(not(feature = "dev-single-node-setup"), allow(unused_mut))] mut self,
        rl_datadir: PathBuf,
        passphrase: String,
        launcher: L,
    ) -> eyre::Result<()>
    where
        L: FnOnce(RaylsBuilder, Ext, PathBuf, String) -> eyre::Result<()>,
    {
        // NOTE: operator forensics count this exact line as a node (re)start; only paths that
        // actually start a node may emit it (see `execute_maintenance`).
        info!(target: "rl::cli", "rayls-network {} starting", SHORT_VERSION);

        // A `dev-single-node-setup` build is single-node-only, so it is always in dev
        // mode: imply `--dev`. The flag is still accepted (scripts/docs that pass it
        // keep working) but is now redundant in a dev build.
        #[cfg(feature = "dev-single-node-setup")]
        {
            self.dev = true;
        }

        // Dev auto-bootstrap: on an empty datadir, generate the validator key +
        // single-validator genesis + committee in-process, so the manual
        // `keytool generate` / `genesis` steps aren't required (#590).
        // Idempotent — a no-op once the datadir is initialized.
        #[cfg(feature = "dev-single-node-setup")]
        if self.dev {
            crate::dev::bootstrap_dev_datadir_if_empty(&rl_datadir, &passphrase)?;
        }

        self.build_and_launch(rl_datadir, passphrase, launcher, true)
    }

    /// Executes a maintenance command over an existing datadir: the node's config/builder
    /// wiring, but no start banner (the operator restart marker), no dev bootstrap of an
    /// uninitialized datadir, and no dev single-validator gate.
    #[cfg(feature = "cold-storage")]
    #[instrument(level = "info", skip_all)]
    pub fn execute_maintenance<L>(
        self,
        rl_datadir: PathBuf,
        passphrase: String,
        launcher: L,
    ) -> eyre::Result<()>
    where
        L: FnOnce(RaylsBuilder, Ext, PathBuf, String) -> eyre::Result<()>,
    {
        info!(target: "rl::cli", "rayls-network {} maintenance run", SHORT_VERSION);
        self.build_and_launch(rl_datadir, passphrase, launcher, false)
    }

    /// Loads config, builds the [`RaylsBuilder`], and hands off to `launcher`.
    ///
    /// The shared tail of [`execute`](Self::execute) and
    /// [`execute_maintenance`](Self::execute_maintenance); `enforce_dev_gate` applies the dev
    /// single-validator gating only on the node path (unused in non-dev builds).
    fn build_and_launch<L>(
        mut self,
        rl_datadir: PathBuf,
        passphrase: String,
        launcher: L,
        #[cfg_attr(not(feature = "dev-single-node-setup"), allow(unused_variables))]
        enforce_dev_gate: bool,
    ) -> eyre::Result<()>
    where
        L: FnOnce(RaylsBuilder, Ext, PathBuf, String) -> eyre::Result<()>,
    {
        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit()?;

        // limit global rayon thread pool for batch validator
        //
        // ensure 2 cores are reserved unless the system only has 1 core
        let num_parallel_threads =
            available_parallelism().map_or(0, |num| num.get().saturating_sub(2).max(1));
        if let Err(err) = ThreadPoolBuilder::new()
            .num_threads(num_parallel_threads)
            .thread_name(|i| format!("rl-rayon-{i}"))
            .build_global()
        {
            error!("Failed to initialize global thread pool for rayon: {}", err)
        }

        // overwrite all genesis if `genesis` was passed to CLI
        let mut rayls_infrastructure_config = if let Some(chain) = self.chain.take() {
            info!(target: "cli", "Overwriting RL config with named chain: {chain:?}");
            match chain {
                NamedChain::Testnet => {
                    Config::load_testnet(&rl_datadir, self.observer, SHORT_VERSION)?
                }
                NamedChain::Mainnet => {
                    Config::load_mainnet(&rl_datadir, self.observer, SHORT_VERSION)?
                }
            }
        } else {
            Config::load(&rl_datadir, self.observer, SHORT_VERSION)?
        };

        // override network hardfork profile if specified via CLI / env
        if let Some(network) = self.network {
            info!(target: "cli", %network, "overriding network hardfork profile from CLI");
            rayls_infrastructure_config.parameters.network = network;
        }

        debug!(target: "cli", validator = ?rayls_infrastructure_config.node_info.name, "rl datadir for node command: {rl_datadir:?}");
        info!(target: "cli", validator = ?rayls_infrastructure_config.node_info.name, "config loaded");

        // Single-validator gating (dev builds only): `--dev` is the explicit opt-in for a
        // single-validator network (the committee-size assert allows n=1) and may never
        // target a production chain-id. Production builds have no such escape hatch — a
        // 1-of-1 committee is refused by the committee-size assert, exactly as before dev mode.
        // A maintenance run skips the gate: it starts no node, so any committee size is fine.
        #[cfg(feature = "dev-single-node-setup")]
        if enforce_dev_gate {
            // Read the committee from the same file `ConsensusConfig` will later load,
            // so the count reflects what consensus runs.
            let committee: Committee =
                Config::load_from_path_or_default(rl_datadir.committee_path(), ConfigFmt::YAML)?;
            let committee_size = committee.size();
            let chain_id = rayls_infrastructure_config.genesis().config.chain_id;
            check_dev_mode(self.dev, committee_size, chain_id)?;
            if self.dev {
                warn!(
                    target: "rl::cli",
                    "DEV MODE ({committee_size}-validator network), chain-id {chain_id} — NOT FOR PRODUCTION."
                );
            }
        }

        // get the worker's transaction address from the config
        let Self {
            chain: _,    // Used above
            observer: _, // Used above
            network: _,  // Used above
            #[cfg(feature = "dev-single-node-setup")]
                dev: _, // Used above
            metrics,
            instance,
            with_unused_ports,
            reth,
            healthcheck,
            ext,
            consensus_db,
        } = self;

        // Both metrics endpoints can also be enabled from parameters.yaml — `metrics_address` for
        // the consensus/Narwhal suite and `reth_metrics_address` for the execution layer. The
        // `--metrics` / `--reth-metrics` CLI flags override the config values when passed.
        let metrics = metrics.or(rayls_infrastructure_config.parameters.metrics_address);
        if let Some(addr) = metrics {
            info!(target: "cli", %addr, "consensus Prometheus metrics enabled");
        }
        let mut reth = reth;
        reth.reth_metrics.prometheus = reth
            .reth_metrics
            .prometheus
            .or(rayls_infrastructure_config.parameters.reth_metrics_address);
        if let Some(addr) = reth.reth_metrics.prometheus {
            info!(target: "cli", %addr, "reth execution-layer Prometheus metrics enabled");
        }

        debug!(target: "cli", "node command genesis: {:#?}", rayls_infrastructure_config.genesis());

        // set up reth node config for engine components
        let node_config = RethConfig::new(
            reth,
            instance,
            &rl_datadir,
            with_unused_ports,
            Arc::new(rayls_infrastructure_config.chain_spec()),
        );

        let build_metadata = BuildMetadata {
            version: env!("CARGO_PKG_VERSION"),
            build_timestamp: env!("VERGEN_BUILD_TIMESTAMP"),
            cargo_features: env!("VERGEN_CARGO_FEATURES"),
            git_sha: env!("VERGEN_GIT_SHA"),
            target_triple: env!("VERGEN_CARGO_TARGET_TRIPLE"),
            build_profile: crate::version::build_profile(),
        };

        let builder = RaylsBuilder::new_with_consensus_db_config(
            node_config,
            rayls_infrastructure_config,
            None,
            metrics,
            healthcheck,
            consensus_db.database_args(),
            build_metadata,
        );

        launcher(builder, ext, rl_datadir, passphrase)
    }
}

#[cfg(all(test, feature = "dev-single-node-setup"))]
mod tests {
    use super::check_dev_mode;

    // Mainnet chain-id; must be one of `PROD_CHAIN_IDS`.
    const MAINNET_CHAIN_ID: u64 = 487;
    // Testnet / devnet default chain-id; not a production chain-id.
    const DEV_CHAIN_ID: u64 = 2017;

    #[test]
    fn single_validator_allowed() {
        // A dev build is single-node: a 1-of-1 committee is the expected case,
        // with or without the --dev auto-bootstrap flag.
        assert!(check_dev_mode(true, 1, DEV_CHAIN_ID).is_ok());
        assert!(check_dev_mode(false, 1, DEV_CHAIN_ID).is_ok());
    }

    #[test]
    fn multi_validator_rejected() {
        // Single-node only: a dev build refuses a multi-validator committee,
        // regardless of the --dev flag.
        let err = check_dev_mode(true, 4, DEV_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("single-node only"), "{err}");
        let err = check_dev_mode(false, 4, DEV_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("single-node only"), "{err}");
    }

    #[test]
    fn dev_rejects_production_chain_id() {
        let err = check_dev_mode(true, 1, MAINNET_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("production chain-id"), "{err}");
    }

    #[test]
    fn dev_allows_non_production_chain_id() {
        assert!(check_dev_mode(true, 1, DEV_CHAIN_ID).is_ok());
    }

    #[test]
    fn empty_committee_is_left_alone() {
        // A missing/default committee deserializes to size 0; the single-node gate
        // targets `> 1`, so it must not fire here — the real "no committee" error
        // surfaces later when consensus loads it.
        assert!(check_dev_mode(false, 0, DEV_CHAIN_ID).is_ok());
        assert!(check_dev_mode(true, 0, DEV_CHAIN_ID).is_ok());
    }
}
