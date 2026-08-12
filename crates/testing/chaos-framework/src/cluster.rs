//! TestCluster: spawn and manage a local 4-validator testnet.

use crate::node::NodeHandle;
use escargot::CargoRun;
use std::{
    net::{TcpListener, UdpSocket},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant},
};
use tracing::info;

/// Default number of validators in a test cluster.
pub const DEFAULT_VALIDATOR_COUNT: usize = 4;

/// Default passphrase for test nodes.
pub const TEST_PASSPHRASE: &str = "chaos_test";

/// How long `spawn` waits for every validator to come up and start advancing.
const STARTUP_HEALTH_TIMEOUT: Duration = Duration::from_secs(60);

/// Only compile the main binary once across all tests.
static CHAOS_BINARY: OnceLock<CargoRun> = OnceLock::new();

/// Build or retrieve the cached `rayls-network` binary.
pub fn get_binary() -> &'static CargoRun {
    CHAOS_BINARY.get_or_init(|| {
        info!(target: "chaos", "building rayls-network binary for chaos tests");
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let path = PathBuf::from(manifest_dir);
        let workspace_root = path
            .ancestors()
            .find(|p| p.join("Cargo.toml").exists() && p.join("crates").exists())
            .expect("Cannot find workspace root");

        escargot::CargoBuild::new()
            .bin("rayls-network")
            .manifest_path(workspace_root.join("Cargo.toml"))
            .target_dir(workspace_root.join("target"))
            .current_target()
            .run()
            .expect("Failed to build rayls-network binary")
    })
}

/// A managed cluster of validator (and optionally observer) nodes.
pub struct TestCluster {
    /// The validator node handles.
    pub validators: Vec<NodeHandle>,
    /// Temp directory holding all node data (dropped = cleaned up).
    _tmp_dir: tempfile::TempDir,
    /// Base path for node data directories.
    base_dir: PathBuf,
    /// Reference to the compiled binary.
    bin: &'static CargoRun,
    /// Passphrase used for BLS keys.
    passphrase: String,
}

impl TestCluster {
    /// Spawn a new test cluster with `count` validators.
    ///
    /// Runs the genesis ceremony, starts all validator processes, and returns only
    /// once every one of them is alive, serving RPC, and advancing.
    pub fn spawn(count: usize) -> eyre::Result<Self> {
        // The genesis ceremony below is hardcoded to build exactly
        // DEFAULT_VALIDATOR_COUNT validator directories. A different count would
        // either point processes at non-existent datadirs (count > 4) or leave
        // genesis validators with no running process (count < 4), silently
        // invalidating the test. Fail fast instead.
        eyre::ensure!(
            count == DEFAULT_VALIDATOR_COUNT,
            "TestCluster::spawn supports exactly {DEFAULT_VALIDATOR_COUNT} validators (got {count})"
        );

        let tmp_dir = tempfile::TempDir::new()?;
        let base_dir = tmp_dir.path().to_path_buf();
        let passphrase = TEST_PASSPHRASE.to_string();

        // Reserve the consensus p2p ports before the ceremony and hold them until the
        // nodes are about to start. Left to itself the keytool picks each primary/worker
        // address with a bind-then-free helper, one address at a time, so two nodes can
        // be handed the SAME udp port — the loser dies at startup with "Address already
        // in use", the cluster runs at 3/4 (exactly quorum), and the first kill silently
        // drops consensus below 2f+1 and halts the chain.
        // Two ports per node (primary + worker), plus two for the observer.
        let p2p_reservation = reserve_distinct_udp_ports((count + 1) * 2)?;

        // Run genesis ceremony.
        e2e_tests_config_local_testnet(&base_dir, passphrase.clone(), &p2p_reservation.ports)?;

        let bin = get_binary();
        let mut validators = Vec::with_capacity(count);

        // Reserve distinct ports up front. Calling an ephemeral-port helper once
        // per validator hands back the SAME port each time (it binds then frees
        // immediately), so the second node failed with "address already in use".
        let rpc_ports = reserve_distinct_ports(count)?;

        // Release the p2p ports so the nodes themselves can bind them.
        drop(p2p_reservation);

        for (i, &rpc_port) in rpc_ports.iter().enumerate() {
            let node = NodeHandle::spawn_validator(i, bin, &base_dir, rpc_port, &passphrase);
            validators.push(node);
        }

        let mut cluster = Self { validators, _tmp_dir: tmp_dir, base_dir, bin, passphrase };
        cluster.wait_until_healthy(STARTUP_HEALTH_TIMEOUT)?;

        info!(target: "chaos", count, "test cluster spawned");
        Ok(cluster)
    }

    /// Wait until every validator is alive, serving RPC, and producing blocks.
    ///
    /// A cluster that comes up degraded otherwise looks healthy: the survivors keep
    /// producing blocks at exactly quorum, and the first injected fault pushes
    /// consensus below 2f+1. Gating here names the validator that never joined
    /// instead of surfacing 60s into a scenario as "chain did not advance".
    pub fn wait_until_healthy(&mut self, timeout: Duration) -> eyre::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut first_seen: Vec<Option<u64>> = vec![None; self.validators.len()];
        let mut advanced = vec![false; self.validators.len()];
        let mut status: Vec<String> =
            vec!["no RPC response yet".to_string(); self.validators.len()];

        loop {
            for i in 0..self.validators.len() {
                if advanced[i] {
                    continue;
                }
                if !self.validators[i].is_alive() {
                    eyre::bail!(
                        "validator {i} exited during startup — check its log for a bind \
                         (\"Address already in use\") or genesis error"
                    );
                }
                match crate::rpc::get_block_number(self.validators[i].rpc_url()) {
                    Ok(height) => match first_seen[i] {
                        Some(start) if height > start => advanced[i] = true,
                        Some(start) => status[i] = format!("stuck at block {start}"),
                        None => {
                            first_seen[i] = Some(height);
                            status[i] = format!("at block {height}, not advancing yet");
                        }
                    },
                    Err(e) => status[i] = format!("RPC error: {e}"),
                }
            }

            if advanced.iter().all(|&ok| ok) {
                return Ok(());
            }

            if Instant::now() >= deadline {
                let stalled: Vec<String> = advanced
                    .iter()
                    .enumerate()
                    .filter(|(_, &ok)| !ok)
                    .map(|(i, _)| format!("validator {i} ({})", status[i]))
                    .collect();
                eyre::bail!(
                    "cluster unhealthy after {timeout:?}: {} — refusing to inject faults \
                     into a cluster that is already below full participation",
                    stalled.join(", ")
                );
            }

            std::thread::sleep(Duration::from_secs(1));
        }
    }

    /// Spawn a default 4-validator cluster.
    pub fn spawn_default() -> eyre::Result<Self> {
        Self::spawn(DEFAULT_VALIDATOR_COUNT)
    }

    /// Get the RPC URLs of all currently alive validators.
    pub fn live_rpc_urls(&mut self) -> Vec<&str> {
        let mut urls = Vec::new();
        for node in &mut self.validators {
            if node.is_alive() {
                urls.push(node.rpc_url());
            }
        }
        urls
    }

    /// Get the RPC URLs of all validators (alive or dead).
    pub fn all_rpc_urls(&self) -> Vec<&str> {
        self.validators.iter().map(|n| n.rpc_url()).collect()
    }

    /// Kill validator at the given index.
    pub fn kill_validator(&mut self, index: usize) {
        self.validators[index].kill();
    }

    /// Hard-kill (SIGKILL) validator at the given index.
    pub fn hard_kill_validator(&mut self, index: usize) {
        self.validators[index].hard_kill();
    }

    /// Restart a previously killed validator.
    pub fn restart_validator(&mut self, index: usize) {
        self.validators[index].restart(self.bin, &self.passphrase);
    }

    /// Get the compiled binary reference.
    pub fn bin(&self) -> &'static CargoRun {
        self.bin
    }

    /// Get the base directory for node data.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Get the passphrase.
    pub fn passphrase(&self) -> &str {
        &self.passphrase
    }

    /// Shut down all validators gracefully.
    pub fn shutdown(&mut self) {
        // Send SIGTERM to all first for parallel shutdown.
        for node in &mut self.validators {
            node.graceful_stop();
        }
        // Then wait/kill each.
        for node in &mut self.validators {
            node.kill();
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for TestCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestCluster")
            .field("validators", &self.validators.len())
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

/// Reserve `n` distinct ephemeral TCP ports on localhost.
///
/// Binds all listeners simultaneously so each gets a unique port, then releases
/// them so the spawned nodes can bind. There is a small TOCTOU window between
/// release and the node binding, which is acceptable for local tests and far
/// better than the guaranteed collision of requesting one port at a time.
fn reserve_distinct_ports(n: usize) -> eyre::Result<Vec<u16>> {
    let listeners: Vec<TcpListener> =
        (0..n).map(|_| TcpListener::bind("127.0.0.1:0")).collect::<Result<_, _>>()?;
    let ports =
        listeners.iter().map(|l| Ok(l.local_addr()?.port())).collect::<eyre::Result<Vec<u16>>>()?;
    drop(listeners);
    Ok(ports)
}

/// `n` distinct ephemeral UDP ports, held open until the reservation is dropped.
///
/// Unlike [`reserve_distinct_ports`], the sockets stay bound: the genesis ceremony
/// bakes these ports into each `node-info.yaml` and the nodes only bind them later,
/// so holding them keeps anything else on the host from taking one in between.
struct UdpPortReservation {
    /// Kept alive purely to hold the ports; dropping this frees them.
    _sockets: Vec<UdpSocket>,
    ports: Vec<u16>,
}

/// Reserve `n` distinct ephemeral UDP ports on localhost simultaneously.
fn reserve_distinct_udp_ports(n: usize) -> eyre::Result<UdpPortReservation> {
    let sockets: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0")).collect::<Result<_, _>>()?;
    let ports =
        sockets.iter().map(|s| Ok(s.local_addr()?.port())).collect::<eyre::Result<Vec<u16>>>()?;
    Ok(UdpPortReservation { _sockets: sockets, ports })
}

/// Run the genesis ceremony to configure a local testnet.
///
/// This replicates the logic from `e2e_tests::config_local_testnet` but is
/// self-contained to avoid a direct dependency on the e2e-tests crate.
///
/// `p2p_ports` holds one primary and one worker port per node, in order: validator
/// 1's primary and worker, validator 2's, ..., then the observer's. They are passed
/// explicitly so no two nodes can be assigned the same port (see `spawn`).
fn e2e_tests_config_local_testnet(
    temp_path: &Path,
    passphrase: String,
    p2p_ports: &[u16],
) -> eyre::Result<()> {
    use clap::Parser as _;
    use rayls_infrastructure_types::test_utils::CommandParser;
    use rayls_network_cli::{genesis::GenesisArgs, keytool::KeyArgs};

    let validators = [
        ("validator-1", "0x1111111111111111111111111111111111111111"),
        ("validator-2", "0x2222222222222222222222222222222222222222"),
        ("validator-3", "0x3333333333333333333333333333333333333333"),
        ("validator-4", "0x4444444444444444444444444444444444444444"),
    ];

    // Validators plus the observer, two ports each.
    let expected_ports = (validators.len() + 1) * 2;
    eyre::ensure!(
        p2p_ports.len() == expected_ports,
        "expected {expected_ports} p2p ports, got {}",
        p2p_ports.len()
    );
    let multiaddr = |port: u16| format!("/ip4/127.0.0.1/udp/{port}/quic-v1");

    // Create shared genesis directory.
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    for (i, (v, addr)) in validators.iter().enumerate() {
        let dir = temp_path.join(v);
        let primary_addr = multiaddr(p2p_ports[i * 2]);
        let worker_addr = multiaddr(p2p_ports[i * 2 + 1]);
        let keys_command = CommandParser::<KeyArgs>::parse_from([
            "rl",
            "generate",
            "validator",
            "--address",
            addr,
            "--external-primary-addr",
            primary_addr.as_str(),
            "--external-worker-addrs",
            worker_addr.as_str(),
        ]);
        keys_command.args.execute(dir.clone(), passphrase.clone())?;
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // Create observer config.
    let dir = temp_path.join("observer");
    let observer_primary_addr = multiaddr(p2p_ports[validators.len() * 2]);
    let observer_worker_addr = multiaddr(p2p_ports[validators.len() * 2 + 1]);
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "rl",
        "generate",
        "observer",
        "--address",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
        "--external-primary-addr",
        observer_primary_addr.as_str(),
        "--external-worker-addrs",
        observer_worker_addr.as_str(),
    ]);
    keys_command.args.execute(dir, passphrase)?;

    // Create committee from shared genesis.
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "rl",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        "0x00000000000000000000000000000000000007a0",
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
    create_committee_command.args.execute(shared_genesis_dir.clone())?;

    // Copy genesis files to each validator and observer.
    for (v, _) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    let dir = temp_path.join("observer");
    std::fs::create_dir_all(dir.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        dir.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        dir.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;

    Ok(())
}
