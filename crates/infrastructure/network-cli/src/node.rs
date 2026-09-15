//! Main node command
//!
//! Starts the client
use crate::{args::ConsensusDatabaseArgs, version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Parser, ValueHint};
use core::fmt;
use eyre::Context;
use fdlimit::raise_fd_limit;
use rayls_execution_evm::{
    parse_socket_address,
    reth_env::{RethCommand, RethConfig, RethEnv},
    set_active_profile, verify_schedule, NetworkConfigFile, NetworkProfile, RaylsHardFork,
    ScheduleRecord,
};
use rayls_infrastructure_config::{Config, RaylsDirs};
// dev-only: reading the committee file for the single-validator gating check
#[cfg(feature = "dev-single-node-setup")]
use rayls_infrastructure_config::{ConfigFmt, ConfigTrait as _};
#[cfg(feature = "dev-single-node-setup")]
use rayls_infrastructure_types::Committee;
use rayls_infrastructure_types::{BuildMetadata, RaylsNetwork};
use rayls_middleware_orchestrator::engine::RaylsBuilder;
use rayon::ThreadPoolBuilder;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    thread::available_parallelism,
};
use tracing::*;

/// Chain-ids that must never be paired with `--dev`. Mainnet only (`72957`) —
/// testnet, devnet and local have distinct, non-production chain-ids.
#[cfg(feature = "dev-single-node-setup")]
const PROD_CHAIN_IDS: &[u64] = &[72957];

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

/// Load the client's network config file and select the requested subnet.
///
/// Validates the profile (known hardforks, non-empty schedule) before the node
/// starts, so a broken file fails fast with an actionable message.
fn load_subnet_profile(config_file: &Path, subnet: &str) -> eyre::Result<NetworkProfile> {
    let yaml = std::fs::read_to_string(config_file)
        .wrap_err_with(|| format!("failed to read network config file {config_file:?}"))?;
    let file: NetworkConfigFile = serde_yaml::from_str(&yaml)
        .wrap_err_with(|| format!("failed to parse network config file {config_file:?}"))?;
    let profile = file.subnet(subnet).cloned().ok_or_else(|| {
        let known = file.networks.keys().cloned().collect::<Vec<_>>().join(", ");
        eyre::eyre!("subnet '{subnet}' not found in {config_file:?}; available subnets: {known}")
    })?;
    profile.validate_hardforks()?;
    if profile.hardforks.is_empty() {
        eyre::bail!(
            "subnet '{subnet}' in {config_file:?} defines no `hardforks`; every subnet must \
             define its hardfork schedule (a block number or \"never\" per fork)"
        );
    }
    Ok(profile)
}

/// Verify that the datadir's genesis chain-id matches the chain-id of the
/// selected schedule source (a config-file subnet, or the baked-in network
/// profile). A mismatch means the datadir belongs to a different network or
/// client, and running it would apply the wrong hardfork schedule — refuse to
/// boot.
fn verify_datadir_chain_id(actual: u64, expected: u64, source: &str) -> eyre::Result<()> {
    if actual != expected {
        eyre::bail!(
            "datadir chain-id {actual} does not match the expected chain-id {expected} \
             from {source}. The datadir appears to belong to a different network or client. \
             Use a datadir whose genesis chain-id is {expected}, or select a schedule source \
             whose chain-id is {actual}."
        );
    }
    Ok(())
}

/// Resolve the chain-id the datadir must carry, given the schedule source selected
/// for this boot.
///
/// Precedence: a `--config-file`/`--subnet` profile (already loaded and validated)
/// wins, then the effective built-in `network` (the `--network` CLI/env override
/// already merged over `parameters.yaml`). Bails when the datadir is "external"
/// (`network: null`) and no file schedule was provided — such a datadir has no
/// baked-in profile, so a schedule must be supplied explicitly.
fn resolve_expected_chain_id(
    file_schedule: Option<(u64, String)>,
    network: Option<RaylsNetwork>,
) -> eyre::Result<(u64, String)> {
    if let Some((chain_id, source)) = file_schedule {
        return Ok((chain_id, source));
    }
    match network {
        Some(network) => Ok((network.chain_id(), format!("network '{network}'"))),
        None => eyre::bail!(
            "datadir has no built-in network profile (external): start with \
             `--network <devnet|testnet|mainnet|local>` (the chain-id must match the \
             genesis) or `--config-file <path> --subnet <name>`"
        ),
    }
}

/// The hardfork schedule selected from a `--config-file`: the file path, the
/// subnet chosen with `--subnet`, and the subnet's resolved profile.
struct FileSchedule {
    /// Path of the network config file.
    path: PathBuf,
    /// The subnet selected from it.
    subnet: String,
    /// The subnet's resolved profile.
    profile: NetworkProfile,
}

/// Verify the hardfork schedule selected for this boot against the datadir's
/// schedule record, then update the record for this boot.
///
/// The record (see [`ScheduleRecord`]) pins the schedule this chain's blocks
/// were produced under: a selected schedule that disagrees with the record on
/// an already-executed fork is refused (it would re-interpret the chain's
/// history), while a differing fork boundary still in the future is allowed —
/// that is how agreed schedule updates ship — but warned about. A datadir
/// without a record (a fresh chain) gets one written for the selected schedule.
fn verify_schedule_record<P: RaylsDirs>(
    datadir: &P,
    node_config: &RethConfig,
    file_schedule: Option<&FileSchedule>,
    network: Option<RaylsNetwork>,
) -> eyre::Result<()> {
    // The schedule selected for this boot: the file profile wins, else the
    // built-in profile (an "external" datadir without either is already
    // refused by `resolve_expected_chain_id`).
    let (schedule, chain_id) = match file_schedule {
        Some(file_schedule) => (file_schedule.profile.schedule(), file_schedule.profile.chain_id),
        None => {
            let network =
                network.expect("an external datadir without a file schedule is refused at boot");
            (RaylsHardFork::for_network(network).to_vec(), network.chain_id())
        }
    };

    // The chain's highest executed block: reth's `Finish` stage checkpoint
    // (the node re-opens the DB right after).
    let head = RethEnv::best_block_number(node_config, datadir.reth_db_path())?;

    let path = datadir.schedule_record_path();
    let existing: Option<ScheduleRecord> = if path.exists() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read schedule record {path:?}"))?;
        Some(
            serde_yaml::from_str::<ScheduleRecord>(&raw)
                .with_context(|| format!("failed to parse schedule record {path:?}"))?,
        )
    } else {
        if head > 0 {
            warn!(
                target: "cli",
                ?path,
                %head,
                "datadir has no schedule record; trusting the selected schedule and \
                 recording it (the executed-history check starts from now)"
            );
        }
        None
    };

    let moves = match &existing {
        Some(record) => verify_schedule(record, &schedule, chain_id, head)?,
        None => Vec::new(),
    };
    for move_ in &moves {
        warn!(
            target: "cli",
            fork = move_.fork.name(),
            ?move_.recorded,
            ?move_.selected,
            %head,
            "future hardfork boundary changed; verify this matches the network-agreed schedule"
        );
    }

    let record = ScheduleRecord::from_schedule(chain_id, head, &schedule);
    let yaml =
        serde_yaml::to_string(&record).wrap_err("failed to serialize the schedule record")?;
    // Write to a sibling temp file and rename into place (atomic on POSIX): a
    // crash mid-write must leave the previous, still-parseable record — never
    // a torn file that bricks the next boot. A leftover temp file after a
    // crash is harmless; the next write overwrites it.
    let tmp = path.with_file_name("schedule-record.yaml.tmp");
    std::fs::write(&tmp, yaml)
        .with_context(|| format!("failed to write schedule record temp file {tmp:?}"))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("failed to move schedule record into place at {path:?}"))?;
    info!(
        target: "cli",
        ?path,
        %chain_id,
        %head,
        "hardfork schedule verified against the chain's executed history"
    );
    Ok(())
}

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
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
    /// Selects which baked-in hardfork schedule to use (devnet, testnet, mainnet,
    /// local). When set, overrides the `network` field in parameters.yaml without
    /// requiring a re-genesis. Useful for activating hardforks on existing networks.
    /// Required (or a `--config-file`/`--subnet` pair) when the datadir's `network`
    /// is unset ("external"), since such a datadir carries no baked-in schedule.
    #[arg(
        long,
        value_name = "RAYLS_NETWORK",
        global = true,
        env = "RAYLS_NETWORK",
        conflicts_with = "config_file"
    )]
    pub network: Option<RaylsNetwork>,

    /// The client's network config file (YAML).
    ///
    /// The file holds any number of named subnets; each subnet carries the
    /// network's hardfork schedule. When set, `--subnet` selects which subnet
    /// this node runs, and its `hardforks` section replaces the schedule baked
    /// into the binary. Everything else (genesis, parameters, committee, node
    /// identity) still comes from the datadir, exactly as without the file.
    /// Cannot be combined with `--network`.
    #[arg(long, value_name = "PATH", value_hint = ValueHint::FilePath, requires = "subnet", conflicts_with = "network")]
    pub config_file: Option<PathBuf>,

    /// The subnet to run, as named in `--config-file`.
    #[arg(long, value_name = "SUBNET", requires = "config_file")]
    pub subnet: Option<String>,

    /// Run as a single-node developer network.
    ///
    /// Redundant in a `dev-single-node-setup` build (always in dev mode); accepted
    /// for compatibility.
    ///
    /// WARNING: for local development and demos only — NOT FOR PRODUCTION USE.
    /// Refuses to start if the configured chain-id matches a known production
    /// network (mainnet = 72957). Pair with a single-validator genesis generated
    /// locally via `keytool generate validator` and `genesis`.
    #[cfg(feature = "dev-single-node-setup")]
    #[arg(long, default_value_t = false)]
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
        self,
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

        // Resolve the hardfork schedule from the client's network config file, if
        // given — before touching the datadir, so a flag conflict or a broken
        // file fails fast with an actionable message.
        let file_schedule = if let Some(config_file) = &self.config_file {
            let subnet = self.subnet.as_deref().expect("clap requires --subnet with --config-file");
            Some(FileSchedule {
                path: config_file.clone(),
                subnet: subnet.to_string(),
                profile: load_subnet_profile(config_file, subnet)?,
            })
        } else {
            None
        };

        // Load the node config from the datadir (genesis, parameters, committee,
        // node identity), as before. The hardfork schedule then either comes
        // from the config file resolved above or is selected by
        // `parameters.network` / `--network` as before.
        let mut rayls_infrastructure_config =
            Config::load(&rl_datadir, self.observer, SHORT_VERSION)?;

        // Apply the `--network` CLI/env override to the config so the execution layer
        // picks up the same schedule source we validate below.
        if let Some(network) = self.network {
            info!(target: "cli", %network, "overriding network hardfork profile from CLI");
            rayls_infrastructure_config.parameters.network = Some(network);
        }

        // The datadir must carry the chain-id of the schedule source selected
        // for this boot, otherwise it belongs to a different network or client
        // and would run the wrong hardfork schedule.
        let file_schedule_desc = file_schedule.as_ref().map(|file_schedule| {
            (
                file_schedule.profile.chain_id,
                format!("subnet '{}' of {:?}", file_schedule.subnet, file_schedule.path),
            )
        });
        let (expected_chain_id, chain_id_source) = resolve_expected_chain_id(
            file_schedule_desc,
            rayls_infrastructure_config.parameters.network,
        )?;
        let actual_chain_id = rayls_infrastructure_config.genesis().config.chain_id;
        verify_datadir_chain_id(actual_chain_id, expected_chain_id, &chain_id_source)?;

        // Borrowed: the schedule record gate below still needs the selected profile.
        if let Some(file_schedule) = &file_schedule {
            info!(
                target: "cli",
                config_file = ?file_schedule.path,
                subnet = %file_schedule.subnet,
                chain_id = actual_chain_id,
                "loading hardfork schedule from file"
            );
            set_active_profile(file_schedule.profile.clone())?;
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
            observer: _,    // Used above
            network: _,     // Used above
            config_file: _, // Used above
            subnet: _,      // Used above
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

        // The datadir's genesis chain-id must match the schedule source (checked above); so
        // must the hardfork schedule itself: a schedule that moves an already-activated fork
        // (or back-dates a new one into the executed history) would re-interpret the blocks
        // this chain has already run. The datadir's schedule record pins what was executed.
        verify_schedule_record(
            &rl_datadir,
            &node_config,
            file_schedule.as_ref(),
            rayls_infrastructure_config.parameters.network,
        )?;

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
    const MAINNET_CHAIN_ID: u64 = 72957;
    // Local chain-id; not a production chain-id.
    const LOCAL_CHAIN_ID: u64 = 487;

    #[test]
    fn single_validator_allowed() {
        // A dev build is single-node: a 1-of-1 committee is the expected case,
        // with or without the --dev auto-bootstrap flag.
        assert!(check_dev_mode(true, 1, LOCAL_CHAIN_ID).is_ok());
        assert!(check_dev_mode(false, 1, LOCAL_CHAIN_ID).is_ok());
    }

    #[test]
    fn multi_validator_rejected() {
        // Single-node only: a dev build refuses a multi-validator committee,
        // regardless of the --dev flag.
        let err = check_dev_mode(true, 4, LOCAL_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("single-node only"), "{err}");
        let err = check_dev_mode(false, 4, LOCAL_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("single-node only"), "{err}");
    }

    #[test]
    fn dev_rejects_production_chain_id() {
        let err = check_dev_mode(true, 1, MAINNET_CHAIN_ID).unwrap_err();
        assert!(err.to_string().contains("production chain-id"), "{err}");
    }

    #[test]
    fn dev_allows_non_production_chain_id() {
        assert!(check_dev_mode(true, 1, LOCAL_CHAIN_ID).is_ok());
    }

    #[test]
    fn empty_committee_is_left_alone() {
        // A missing/default committee deserializes to size 0; the single-node gate
        // targets `> 1`, so it must not fire here — the real "no committee" error
        // surfaces later when consensus loads it.
        assert!(check_dev_mode(false, 0, LOCAL_CHAIN_ID).is_ok());
        assert!(check_dev_mode(true, 0, LOCAL_CHAIN_ID).is_ok());
    }
}

#[cfg(test)]
mod chain_id_tests {
    use super::{resolve_expected_chain_id, verify_datadir_chain_id};
    use rayls_infrastructure_types::RaylsNetwork;

    #[test]
    fn matching_chain_id_passes() {
        assert!(verify_datadir_chain_id(7295799, 7295799, "network 'testnet'").is_ok());
        assert!(verify_datadir_chain_id(72957, 72957, "subnet 'mainnet' of \"/x/y.yaml\"").is_ok());
    }

    #[test]
    fn mismatched_chain_id_is_refused() {
        let err = verify_datadir_chain_id(487, 72957, "network 'mainnet'").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("487"), "{msg}");
        assert!(msg.contains("72957"), "{msg}");
        assert!(msg.contains("network 'mainnet'"), "{msg}");
    }

    #[test]
    fn file_schedule_wins_over_network() {
        let file = (72957, "subnet 'mainnet' of \"/x/y.yaml\"".to_string());
        let resolved = resolve_expected_chain_id(Some(file), Some(RaylsNetwork::Testnet)).unwrap();
        assert_eq!(resolved, (72957, "subnet 'mainnet' of \"/x/y.yaml\"".to_string()));
    }

    #[test]
    fn network_flag_resolves_to_its_chain_id() {
        let resolved = resolve_expected_chain_id(None, Some(RaylsNetwork::Local)).unwrap();
        assert_eq!(resolved, (487, "network 'local'".to_string()));
    }

    #[test]
    fn external_without_schedule_is_refused() {
        let err = resolve_expected_chain_id(None, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("external"), "{msg}");
        assert!(msg.contains("--network"), "{msg}");
        assert!(msg.contains("--config-file"), "{msg}");
    }
}

#[cfg(test)]
mod schedule_record_tests {
    use super::{verify_schedule_record, FileSchedule};
    use clap::Parser;
    use rayls_execution_evm::{
        network_profile::{ForkActivation, NetworkProfile},
        reth_env::{RethCommand, RethConfig, RethEnv},
        ForkCondition, RaylsHardFork, RethChainSpec, ScheduleRecord,
    };
    use rayls_infrastructure_types::RaylsNetwork;
    use reth_db::{tables::StageCheckpoints, transaction::DbTxMut, Database};
    use reth_stages::{StageCheckpoint, StageId};
    use std::{collections::BTreeMap, path::Path, sync::Arc};

    fn node_config(datadir: &Path) -> RethConfig {
        let reth = RethCommand::parse_from(["rayls-test"]);
        RethConfig::new(reth, None, datadir, false, Arc::new(RethChainSpec::default()))
    }

    /// Fake the chain head the way a real node records it: commit the `Finish`
    /// stage checkpoint at `block` (reth tracks the executed head there, not in
    /// `CanonicalHeaders`).
    fn set_head(node_config: &RethConfig, datadir: &Path, block: u64) {
        let db = RethEnv::new_database(node_config, datadir.join("db")).expect("db opens");
        db.update(|tx| {
            tx.put::<StageCheckpoints>(
                StageId::Finish.as_str().to_string(),
                StageCheckpoint::new(block),
            )
            .expect("head checkpoint written");
        })
        .expect("db update");
    }

    /// The full local schedule with one fork's activation replaced.
    fn local_profile_moving(fork: &str, to: u64) -> NetworkProfile {
        let mut hardforks = BTreeMap::new();
        for (fork, condition) in RaylsHardFork::for_network(RaylsNetwork::Local) {
            if let ForkCondition::Block(block) = condition {
                hardforks.insert(fork.name().to_string(), ForkActivation::Block(block));
            }
        }
        hardforks.insert(fork.to_string(), ForkActivation::Block(to));
        NetworkProfile { chain_id: 487, hardforks }
    }

    fn boot(dir: &Path, config: &RethConfig, network: Option<RaylsNetwork>) -> eyre::Result<()> {
        let dir = dir.to_path_buf();
        verify_schedule_record(&dir, config, None, network)
    }

    #[test]
    fn fresh_datadir_records_selected_schedule() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.chain_id, 487);
        assert_eq!(record.as_of_block, 0);
        assert_eq!(record.hardforks.get("Eip1559"), Some(&ForkActivation::Block(0)));
        assert_eq!(record.hardforks.get("UsdrSupplyCorrection"), Some(&ForkActivation::Block(100)));
    }

    #[test]
    fn second_boot_records_the_chain_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("second boot passes");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record re-written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.as_of_block, 10);
    }

    #[test]
    fn executed_fork_move_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("Eip1559", 20);
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, Some(&file_schedule), None)
        }
        .expect_err("moving an executed fork is refused");
        let msg = err.to_string();
        assert!(msg.contains("Eip1559"), "{msg}");
        assert!(msg.contains("0"), "{msg}");
        assert!(msg.contains("20"), "{msg}");
    }

    #[test]
    fn future_fork_move_is_allowed_and_re_recorded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("UsdrSupplyCorrection", 200);
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, Some(&file_schedule), None)
        }
        .expect("moving a future fork is allowed");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record re-written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.hardforks.get("UsdrSupplyCorrection"), Some(&ForkActivation::Block(200)));
    }

    #[test]
    fn deleted_record_is_rewritten() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        std::fs::remove_file(dir.path().join("schedule-record.yaml")).expect("record removed");
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("boot passes");
        let record: ScheduleRecord = serde_yaml::from_str(
            &std::fs::read_to_string(dir.path().join("schedule-record.yaml")).expect("record"),
        )
        .expect("record parses");
        assert_eq!(record.as_of_block, 10);
    }

    #[test]
    fn chain_id_mismatch_in_record_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, Some(RaylsNetwork::Local)).expect("first boot passes");
        let mut profile = local_profile_moving("Eip1559", 0);
        profile.chain_id = 99999;
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, Some(&file_schedule), None)
        }
        .expect_err("chain-id mismatch is refused");
        let msg = err.to_string();
        assert!(msg.contains("chain-id"), "{msg}");
    }

    #[test]
    fn unparseable_record_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        std::fs::write(dir.path().join("schedule-record.yaml"), "not: [yaml").expect("garbage");
        let err = boot(dir.path(), &config, Some(RaylsNetwork::Local))
            .expect_err("unparseable record is refused");
        let msg = err.to_string();
        assert!(msg.contains("schedule-record.yaml"), "{msg}");
    }
}
