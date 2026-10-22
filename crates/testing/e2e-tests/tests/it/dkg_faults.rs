//! Faults during the threshold-certificate DKG on a five-validator local network: validators
//! crash at each DKG step, stay offline for one to five epochs, lose their databases, or the
//! whole committee holds its dealings past the epoch boundary. After each fault the test checks
//! that the faulty validator takes part in consensus again, not just that the chain moves: with
//! five validators the quorum is four, so the test stops another validator and requires the
//! chain to keep committing and to cross an epoch boundary, which it can only do if the
//! recovered validator votes with its share of the epoch key. Every node must also agree on how
//! each epoch closed.
//!
//! Needs the node binary built with the `dkg-test-hooks` feature (`RL_TEST_DKG_*` environment
//! variables): `cargo test -p e2e-tests --features dkg-test-hooks --test it dkg_faults --
//! --ignored --test-threads 1`.

use crate::epochs::{
    assert_threshold_certificates, config_committee, create_genesis_for_test, current_epoch,
    generate_new_validator_txs, latest_header, wait_for_verified_epoch_record, EPOCH_DURATION,
    NEW_VALIDATOR, NODE_PASSWORD,
};
use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
};
use e2e_tests::{IT_TEST_MUTEX, TEST_NETWORK};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_execution_evm::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait as _};
use rayls_infrastructure_types::{
    test_utils, Address, Epoch, EpochCertificate, EpochRecord, Genesis, GenesisAccount, U256,
};
use std::{
    collections::BTreeMap,
    os::unix::process::ExitStatusExt as _,
    path::PathBuf,
    process::{Child, ExitStatus},
    sync::Arc,
    time::Duration,
};
use tokio::time::{sleep, Instant};
use tracing::info;

const EPOCH: Duration = Duration::from_secs(EPOCH_DURATION);
/// Environment variable that names the hold file: while it exists, a node submits no dealing.
const HOLD: &str = "RL_TEST_DKG_HOLD_DEALINGS";
/// Environment variables that crash a node at a DKG step.
const CRASH_AT: &str = "RL_TEST_DKG_CRASH_AT";
const CRASH_EPOCH: &str = "RL_TEST_DKG_CRASH_EPOCH";
/// Environment variable that sets the blocks per receipts static file.
const RECEIPTS_PER_FILE: &str = "RL_TEST_RECEIPTS_PER_FILE";
/// Environment variable that sets how long past its boundary an epoch waits for the next key.
const KEY_WAIT: &str = "RL_TEST_DKG_KEY_WAIT";

/// One node process of the test network.
struct Node {
    name: String,
    instance: u8,
    dir: PathBuf,
    child: Option<Child>,
    /// Arguments the node starts with besides the usual ones.
    extra_args: Vec<String>,
}

impl Node {
    /// RPC port: instance `i` serves on `8546 - i`.
    fn port(&self) -> u16 {
        8546 - u16::from(self.instance)
    }

    fn start(&mut self, env: &[(&str, String)]) {
        assert!(self.child.is_none(), "{} already running", self.name);
        let mut command = e2e_tests::get_rayls_network_binary().command();
        command
            .env("RL_BLS_PASSPHRASE", NODE_PASSWORD)
            .arg("--bls-passphrase-source")
            .arg("env")
            .arg("node")
            .arg("--datadir")
            .arg(&*self.dir.to_string_lossy())
            .arg("--instance")
            .arg(self.instance.to_string())
            .arg("--http")
            .arg("--storage.v2")
            .arg("--network")
            .arg(TEST_NETWORK.to_string())
            .arg("--log.file.directory")
            .arg(self.dir.join("logs"))
            .args(&self.extra_args);
        for (key, value) in env {
            command.env(key, value);
        }
        info!(target: "dkg-faults", node = %self.name, ?env, "starting");
        self.child = Some(command.spawn().expect("node starts"));
    }

    /// Kill the process with no shutdown, as a crash or power loss would.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "dkg-faults", node = %self.name, "killing");
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Stop the process gracefully.
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "dkg-faults", node = %self.name, "stopping");
            let _ = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
            let _ = child.wait();
        }
    }

    /// Wait for the process to exit on its own (a test hook's crash) and return how it ended.
    async fn exited(&mut self, deadline: Duration) -> eyre::Result<ExitStatus> {
        let started = Instant::now();
        loop {
            let child =
                self.child.as_mut().ok_or_else(|| eyre::eyre!("{} not running", self.name))?;
            if let Some(status) = child.try_wait()? {
                self.child = None;
                return Ok(status);
            }
            eyre::ensure!(
                started.elapsed() < deadline,
                "{} did not crash within {deadline:?}",
                self.name
            );
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Delete the node's execution and consensus databases, keeping its keys and genesis.
    fn wipe_databases(&self) -> eyre::Result<()> {
        assert!(self.child.is_none(), "stop {} first", self.name);
        for db in ["db", "consensus-db", "static_files", "rocksdb", "blobstore"] {
            let path = self.dir.join(db);
            if path.exists() {
                std::fs::remove_dir_all(&path)?;
            }
        }
        Ok(())
    }

    /// Everything the node logged so far, rotated files included.
    fn log_text(&self) -> String {
        let dir = self.dir.join("logs").join("rayls-network-logs");
        let mut text = String::new();
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            if entry.file_name().to_string_lossy().starts_with("reth.log") {
                text.push_str(&String::from_utf8_lossy(
                    &std::fs::read(entry.path()).unwrap_or_default(),
                ));
            }
        }
        text
    }
}

/// The test network: five genesis validators, and optionally a joiner.
struct Net {
    nodes: BTreeMap<String, Node>,
    /// The file whose existence holds back the dealings of every node started with [`HOLD`].
    hold_file: PathBuf,
    /// Variables every node of the network starts with, besides the hold variable.
    shared_env: Vec<(&'static str, String)>,
    temp_path: PathBuf,
    temp: Option<tempfile::TempDir>,
}

impl Drop for Net {
    fn drop(&mut self) {
        for node in self.nodes.values_mut() {
            node.kill();
        }
        // `DKG_FAULTS_KEEP=1` keeps the nodes' data and logs for a look after the test
        if std::env::var_os("DKG_FAULTS_KEEP").is_some() {
            if let Some(temp) = self.temp.take() {
                let path = temp.keep();
                eprintln!("dkg-faults: kept {}", path.display());
            }
        }
    }
}

const VALIDATORS: [(&str, u8); 5] = [
    ("validator-1", 0x11),
    ("validator-2", 0x22),
    ("validator-3", 0x33),
    ("validator-4", 0x44),
    ("validator-5", 0x55),
];

impl Net {
    /// Genesis for the five validators, every one of them with its encryption key at genesis,
    /// and, with `joiner`, a funded new validator that can stake later. Nothing is started.
    fn create(prefix: &str, joiner: Option<&TransactionFactory>) -> eyre::Result<Self> {
        let temp = tempfile::TempDir::with_prefix(prefix)?;
        let temp_path = temp.path().to_path_buf();
        let committee: Vec<(&str, Address)> = VALIDATORS
            .iter()
            .map(|(name, byte)| (*name, Address::from_slice(&[*byte; 20])))
            .collect();
        let governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        match joiner {
            Some(joiner) => {
                create_genesis_for_test(
                    &temp_path,
                    joiner.address(),
                    governance.address(),
                    &committee,
                    true,
                )?;
            }
            None => {
                let accounts = vec![(
                    governance.address(),
                    GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
                )];
                config_committee(
                    &temp_path,
                    &temp_path.join("shared-genesis"),
                    NODE_PASSWORD.to_string(),
                    governance.address(),
                    accounts,
                    &committee,
                    true,
                )?;
            }
        }
        let mut nodes = BTreeMap::new();
        for (index, (name, _)) in VALIDATORS.iter().enumerate() {
            nodes.insert(
                name.to_string(),
                Node {
                    name: name.to_string(),
                    instance: index as u8 + 1,
                    dir: temp_path.join(name),
                    child: None,
                    extra_args: Vec::new(),
                },
            );
        }
        if joiner.is_some() {
            nodes.insert(
                NEW_VALIDATOR.to_string(),
                Node {
                    name: NEW_VALIDATOR.to_string(),
                    instance: 6,
                    dir: temp_path.join(NEW_VALIDATOR),
                    child: None,
                    extra_args: Vec::new(),
                },
            );
        }
        Ok(Self {
            nodes,
            hold_file: temp_path.join("hold-dealings"),
            shared_env: Vec::new(),
            temp_path,
            temp: Some(temp),
        })
    }

    /// The genesis timestamp: when epoch 0 started.
    fn genesis_timestamp(&self) -> eyre::Result<u64> {
        let path = self.temp_path.join("shared-genesis/genesis/genesis.yaml");
        let genesis: Genesis = Config::load_from_path(&path, ConfigFmt::YAML)?;
        Ok(genesis.timestamp)
    }

    fn node(&mut self, name: &str) -> &mut Node {
        self.nodes.get_mut(name).expect("known node")
    }

    /// The hold variable every node gets, so the test can hold the whole committee's dealings.
    fn hold_env(&self) -> (&'static str, String) {
        (HOLD, self.hold_file.to_string_lossy().into_owned())
    }

    /// The hold variable and the shared ones: what every node starts with.
    fn env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![self.hold_env()];
        env.extend(self.shared_env.iter().cloned());
        env
    }

    /// Start every node not running, with [`Self::env`], and wait for their RPC (not for nodes
    /// already started, which a test may have set to crash).
    async fn start_all(&mut self) -> eyre::Result<()> {
        let env = self.env();
        let mut started = Vec::new();
        for node in self.nodes.values_mut().filter(|node| node.child.is_none()) {
            node.start(&env);
            started.push(node.port());
        }
        for port in started {
            wait_rpc(port).await?;
        }
        Ok(())
    }

    /// Restart `name` with [`Self::env`] and `extra` variables, and wait for its RPC.
    async fn restart(&mut self, name: &str, extra: &[(&str, String)]) -> eyre::Result<()> {
        let mut env = self.env();
        env.extend(extra.iter().cloned());
        let node = self.node(name);
        node.kill();
        node.start(&env);
        wait_rpc(node.port()).await
    }

    fn running_ports(&self) -> Vec<u16> {
        self.nodes.values().filter(|node| node.child.is_some()).map(Node::port).collect()
    }

    /// The port of a running node other than `except`.
    fn reference_port(&self, except: &[&str]) -> u16 {
        self.nodes
            .values()
            .find(|node| node.child.is_some() && !except.contains(&node.name.as_str()))
            .expect("a running node")
            .port()
    }

    fn hold_dealings(&self) -> eyre::Result<()> {
        std::fs::write(&self.hold_file, b"hold")?;
        Ok(())
    }

    fn release_dealings(&self) -> eyre::Result<()> {
        if self.hold_file.exists() {
            std::fs::remove_file(&self.hold_file)?;
        }
        Ok(())
    }

    /// `name` takes part in consensus: stop another genesis validator, leaving exactly a quorum
    /// that includes `name`, and require the chain to commit and to cross an epoch boundary, the
    /// new epoch's key dealt and used, before the stopped validator comes back.
    async fn assert_votes(&mut self, name: &str) -> eyre::Result<()> {
        // a vote request that reaches a validator before it is an active voter again is not
        // retried, and with a quorum of exactly four the round then never certifies (a consensus
        // issue of its own, not the DKG's): stop the other validator only once this one votes
        let port = self.node(name).port();
        wait_until(2 * EPOCH, || async move {
            let provider =
                ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
            let status: serde_json::Value =
                provider.raw_request("rayls_nodeStatus".into(), Vec::<()>::new()).await?;
            eyre::ensure!(status["role"] == "active_cvv", "{name} is {}", status["role"]);
            Ok(())
        })
        .await?;
        let other = VALIDATORS
            .iter()
            .map(|(validator, _)| *validator)
            .find(|validator| *validator != name && self.nodes[*validator].child.is_some())
            .expect("another running validator");
        self.node(other).stop();
        let reference = self.reference_port(&[name, other]);
        let result = async {
            let epoch = current_epoch(reference).await?;
            let header = latest_header(reference).await?.number;
            wait_until(4 * EPOCH, || async {
                let now = current_epoch(reference).await?;
                let number = latest_header(reference).await?.number;
                eyre::ensure!(
                    now > epoch && number >= header + 20,
                    "at epoch {now}, header {number} (from epoch {epoch}, header {header})"
                );
                Ok(())
            })
            .await
            .map_err(|e| {
                eyre::eyre!("with {other} stopped the chain needs {name} to vote, and stalled: {e}")
            })?;
            let now = current_epoch(reference).await?;
            assert_threshold_certificates_on(&[reference], now, 2 * EPOCH).await
        }
        .await;
        let env = self.env();
        let node = self.node(other);
        node.start(&env);
        let port = node.port();
        wait_rpc(port).await?;
        // back to full strength before the next fault: the next one must not overlap this one
        wait_caught_up(port, reference, 3 * EPOCH).await?;
        result
    }

    /// Every running node holds the same certified record for every closed epoch, a threshold
    /// certificate wherever the epoch has a group key, with the same bytes on every node (an
    /// aggregate one depends on whose votes arrived); and the registry agrees on where each epoch
    /// the registry still keeps began.
    async fn assert_agreement(&self) -> eyre::Result<()> {
        let ports = self.running_ports();
        let reference = ports[0];
        let current = current_epoch(reference).await?;
        let mut records: BTreeMap<Epoch, EpochRecord> = BTreeMap::new();
        let mut threshold_certs: BTreeMap<Epoch, EpochCertificate> = BTreeMap::new();
        for port in &ports {
            let provider =
                ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
            for epoch in 0..current {
                let record =
                    wait_for_verified_epoch_record(&provider, *port, epoch, 2 * EPOCH).await?;
                match records.get(&epoch) {
                    Some(expected) => eyre::ensure!(
                        expected.digest() == record.digest(),
                        "node on port {port} holds another record of epoch {epoch}"
                    ),
                    None => {
                        records.insert(epoch, record);
                    }
                }
                let (_, cert): (EpochRecord, EpochCertificate) =
                    provider.raw_request("rayls_epochRecord".into(), (epoch,)).await?;
                // an epoch with a group key has a threshold certificate, no signer listed
                let group_key: Option<Option<rayls_infrastructure_types::BlsPublicKey>> =
                    provider.raw_request("rayls_epochGroupKey".into(), (epoch,)).await.ok();
                if let Some(Some(_)) = group_key {
                    eyre::ensure!(
                        cert.signed_authorities.is_empty(),
                        "node on port {port}: epoch {epoch} has a group key but an aggregate \
                         certificate"
                    );
                }
                if cert.signed_authorities.is_empty() {
                    match threshold_certs.get(&epoch) {
                        Some(expected) => eyre::ensure!(
                            *expected == cert,
                            "node on port {port} holds other certificate bytes for epoch {epoch}"
                        ),
                        None => {
                            threshold_certs.insert(epoch, cert);
                        }
                    }
                }
            }
            let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
            // the registry keeps the epochs from its own current one back three
            let own = registry.getCurrentEpoch().call().await?;
            for epoch in own.saturating_sub(3).max(1)..current.min(own) {
                let started = registry.getEpochInfo(epoch).call().await?.blockHeight;
                let previous = &records[&(epoch - 1)];
                eyre::ensure!(
                    started == previous.parent_state.number + 1,
                    "node on port {port}: epoch {epoch} starts at block {started}, its \
                     predecessor's record ends at {}",
                    previous.parent_state.number
                );
            }
        }
        info!(target: "dkg-faults", epochs = current, nodes = ports.len(), threshold = threshold_certs.len(), "every node agrees on every closed epoch");
        Ok(())
    }
}

async fn wait_until<F, Fut>(deadline: Duration, mut check: F) -> eyre::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = eyre::Result<()>>,
{
    let started = Instant::now();
    loop {
        let error = match check().await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        if started.elapsed() > deadline {
            return Err(error.wrap_err(format!("not reached within {deadline:?}")));
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_rpc(port: u16) -> eyre::Result<()> {
    wait_until(Duration::from_secs(60), || async move {
        let provider =
            ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
        provider.get_chain_id().await?;
        Ok(())
    })
    .await
}

async fn block_number(port: u16) -> eyre::Result<u64> {
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    Ok(provider.get_block_number().await?)
}

async fn wait_for_epoch(port: u16, epoch: Epoch, deadline: Duration) -> eyre::Result<()> {
    wait_until(deadline, || async move {
        let now = current_epoch(port).await?;
        eyre::ensure!(now >= epoch, "node on port {port} at epoch {now}, waiting for {epoch}");
        Ok(())
    })
    .await
}

/// `port`'s consensus reaches the height `reference` held when the wait began.
async fn wait_caught_up(port: u16, reference: u16, deadline: Duration) -> eyre::Result<()> {
    let target = latest_header(reference).await?.number;
    wait_until(deadline, || async move {
        let number = latest_header(port).await?.number;
        eyre::ensure!(number >= target, "node on port {port} at header {number} of {target}");
        Ok(())
    })
    .await
}

alloy::sol! {
    /// The `DkgManager` reads the tests use.
    interface IDkgManager {
        function dealers(uint32 epoch) external view returns (address[] memory);
        function dealingBlock(uint32 epoch, address dealer) external view returns (uint64);
    }
}

/// Whether the node on `port` lacks the receipts of every block that accepted a dealing of the
/// key of `epoch`: a pruned block's receipts read back empty or not at all.
async fn dealing_receipts_pruned(port: u16, epoch: Epoch) -> eyre::Result<bool> {
    use alloy::{rpc::types::TransactionRequest, sol_types::SolCall as _};
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    let call = |input: Vec<u8>| {
        TransactionRequest::default()
            .to(rayls_infrastructure_config::DKG_MANAGER_ADDRESS)
            .input(input.into())
    };
    let output = provider.call(call(IDkgManager::dealersCall { epoch }.abi_encode())).await?;
    let dealers = IDkgManager::dealersCall::abi_decode_returns(&output)?;
    eyre::ensure!(!dealers.is_empty(), "no dealings for the key of epoch {epoch}");
    for dealer in dealers {
        let output = provider
            .call(call(IDkgManager::dealingBlockCall { epoch, dealer }.abi_encode()))
            .await?;
        let block = IDkgManager::dealingBlockCall::abi_decode_returns(&output)?;
        let receipts =
            provider.get_block_receipts(alloy::eips::BlockId::number(block)).await.ok().flatten();
        if receipts.is_some_and(|receipts| !receipts.is_empty()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A header of `epoch` sampled on each of `ports` carries aggregate certificates only.
async fn assert_aggregate_certificates_on(
    ports: &[u16],
    epoch: Epoch,
    deadline: Duration,
) -> eyre::Result<()> {
    for port in ports {
        let started = Instant::now();
        let header = loop {
            eyre::ensure!(
                started.elapsed() < deadline,
                "node on port {port}: no header of epoch {epoch} sampled within {deadline:?}"
            );
            if let Ok(header) = latest_header(*port).await {
                let now = header.sub_dag.leader_epoch();
                eyre::ensure!(now <= epoch, "node on port {port} passed epoch {epoch} unsampled");
                if now == epoch {
                    break header;
                }
            }
            sleep(Duration::from_millis(500)).await;
        };
        for cert in header.sub_dag.certificates.iter().chain([&header.sub_dag.leader]) {
            eyre::ensure!(
                !cert.signed_authorities().is_empty(),
                "node on port {port}: consensus header {} of epoch {epoch} carries a threshold \
                 certificate (round {})",
                header.number,
                cert.round(),
            );
        }
    }
    Ok(())
}

async fn assert_threshold_certificates_on(
    ports: &[u16],
    epoch: Epoch,
    deadline: Duration,
) -> eyre::Result<()> {
    for port in ports {
        assert_threshold_certificates(*port..=*port, epoch..=epoch, deadline).await?;
    }
    Ok(())
}

/// Every validator prunes receipts to the last 64 blocks, as `--full` does to the last 10,064,
/// with 16-block receipt files (`RL_TEST_RECEIPTS_PER_FILE`; pruning deletes whole files, 500,000
/// blocks each by default), so the receipts of the blocks that accepted an epoch's dealings are
/// gone long before the epoch ends. A validator restarted late in an epoch derives its share from
/// the dealings it kept while executing them, and votes.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_restart_with_pruned_receipts() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_pruned", None)?;
    for node in net.nodes.values_mut() {
        node.extra_args = ["--prune.receipts.distance", "64", "--prune.block-interval", "1"]
            .map(String::from)
            .to_vec();
    }
    net.shared_env.push((RECEIPTS_PER_FILE, "16".to_string()));
    net.start_all().await?;
    let victim = "validator-3";
    let port = net.reference_port(&[victim]);

    // epoch 2's key was dealt at the start of epoch 1: late in epoch 2 those blocks are well over
    // 64 behind the tip
    wait_for_epoch(port, 2, 4 * EPOCH).await?;
    sleep(EPOCH * 2 / 3).await;
    net.restart(victim, &[]).await?;
    wait_caught_up(net.node(victim).port(), port, 2 * EPOCH).await?;
    // the share came from the kept dealings: the victim no longer has the dealing blocks'
    // receipts
    let victim_port = net.node(victim).port();
    let pruned = dealing_receipts_pruned(victim_port, 2).await?;
    eyre::ensure!(
        pruned,
        "{victim} still has the receipts of epoch 2's dealings: the test proves nothing"
    );
    net.assert_votes(victim).await?;
    net.assert_agreement().await?;
    Ok(())
}

/// How long after the nodes start epoch 0's key-wait deadline falls in [`dkg_key_deadline`].
const DEADLINE_AFTER_START: Duration = Duration::from_secs(60);

/// The committee never deals the key of epoch 1: epoch 0 waits for it past its boundary, then
/// closes without it at the key-wait deadline, at one output every node agrees on. Epoch 1 runs
/// on aggregate certificates, and once the dealings resume epoch 2 gets a threshold key again.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_key_deadline() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_deadline", None)?;
    net.hold_dealings()?;
    // the deadline is the boundary (genesis + EPOCH) plus the wait: place it a minute after the
    // nodes start, however old the genesis timestamp is by then
    // the first start builds or finds the node binary, which can take a minute: do it first
    let _ = e2e_tests::get_rayls_network_binary();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let boundary = net.genesis_timestamp()? + EPOCH.as_secs();
    let wait = (now + DEADLINE_AFTER_START.as_secs()).saturating_sub(boundary);
    info!(target: "dkg-faults", now, boundary, wait, "key-wait deadline set");
    net.shared_env.push((KEY_WAIT, wait.to_string()));
    net.start_all().await?;
    let port = net.reference_port(&[]);

    // past the boundary, before the deadline: still waiting
    sleep(DEADLINE_AFTER_START / 2).await;
    eyre::ensure!(current_epoch(port).await? == 0, "epoch 0 closed before the key-wait deadline");

    for node_port in net.running_ports() {
        wait_for_epoch(node_port, 1, DEADLINE_AFTER_START + EPOCH).await?;
    }
    eyre::ensure!(
        net.nodes.values().any(|node| node.log_text().contains("closing the epoch without it")),
        "no node logged the close without the key"
    );
    assert_aggregate_certificates_on(&net.running_ports(), 1, EPOCH).await?;

    net.release_dealings()?;
    assert_threshold_certificates_on(&net.running_ports(), 2, 3 * EPOCH).await?;
    net.assert_agreement().await?;
    net.assert_votes("validator-3").await?;
    Ok(())
}

/// The committee holds every dealing for the key of epoch 1 past epoch 0's boundary: epoch 0
/// stays open while blocks keep coming, a validator killed and restarted during the extension
/// decides the close the same way, and once the dealings are released the epoch closes at one
/// output that every node agrees on, and epoch 1 runs on threshold certificates.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_forced_extension() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_extension", None)?;
    net.hold_dealings()?;
    net.start_all().await?;
    let port = net.reference_port(&[]);
    // the genesis ceremony gave epoch 0 its own threshold key
    assert_threshold_certificates_on(&net.running_ports(), 0, EPOCH).await?;

    // past the boundary: still epoch 0, and execution moves on
    sleep(EPOCH + Duration::from_secs(10)).await;
    let blocks = block_number(port).await?;
    sleep(EPOCH / 2).await;
    eyre::ensure!(current_epoch(port).await? == 0, "epoch 0 closed without its successor's key");
    eyre::ensure!(block_number(port).await? > blocks, "no blocks while the epoch was held open");

    // a validator crashes and comes back during the extension
    net.restart("validator-3", &[]).await?;
    wait_caught_up(net.node("validator-3").port(), port, 2 * EPOCH).await?;
    sleep(EPOCH / 2).await;
    eyre::ensure!(current_epoch(port).await? == 0, "epoch 0 closed while still held");

    net.release_dealings()?;
    for node_port in net.running_ports() {
        wait_for_epoch(node_port, 1, 3 * EPOCH).await?;
    }
    eyre::ensure!(
        net.nodes.values().any(|node| node.log_text().contains("epoch extended")),
        "no node logged the extension"
    );
    assert_threshold_certificates_on(&net.running_ports(), 1, 3 * EPOCH).await?;
    wait_for_epoch(port, 2, 3 * EPOCH).await?;
    net.assert_agreement().await?;
    net.assert_votes("validator-3").await?;
    Ok(())
}

/// A validator crashes at each step of its DKG work, one step after the other: with its dealing
/// built, with it in its pool, after it is accepted, with the next epoch's outcome saved, with
/// its share derived, and while its epoch is held open for the next key. Each time it restarts
/// and carries on; at the end it votes, and every node agrees on every epoch.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_crash_at_each_step() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_crash_steps", None)?;
    let victim = "validator-3";
    let crash = |point: &str, epoch: Epoch| {
        vec![(CRASH_AT, point.to_string()), (CRASH_EPOCH, epoch.to_string())]
    };
    let hold = net.hold_env();
    net.node(victim).start(&[hold, (CRASH_AT, "dealing-built".into()), (CRASH_EPOCH, "0".into())]);
    net.start_all().await?;
    let port = net.reference_port(&[victim]);

    for point in
        ["dealing-built", "dealing-submitted", "dealing-accepted", "outcome-saved", "share-derived"]
    {
        let status = net.node(victim).exited(4 * EPOCH).await?;
        eyre::ensure!(
            status.signal() == Some(6),
            "{victim} ended with {status} at {point}, not an abort"
        );
        info!(target: "dkg-faults", point, "crashed as asked");
        let next = match point {
            "dealing-built" => Some("dealing-submitted"),
            "dealing-submitted" => Some("dealing-accepted"),
            "dealing-accepted" => Some("outcome-saved"),
            "outcome-saved" => Some("share-derived"),
            _ => None,
        };
        // the next crash is for a later epoch, so the node runs a while in between
        let epoch = current_epoch(port).await? + 1;
        match next {
            Some(next) => net.restart(victim, &crash(next, epoch)).await?,
            None => net.restart(victim, &[]).await?,
        }
    }

    // crash while the epoch is held open for the next key: hold the dealings once this epoch's
    // are in, so the next epoch starts and then cannot close
    let epoch = current_epoch(port).await? + 1;
    wait_for_epoch(port, epoch, 3 * EPOCH).await?;
    sleep(Duration::from_secs(15)).await;
    net.hold_dealings()?;
    wait_for_epoch(port, epoch + 1, 3 * EPOCH).await?;
    let epoch = epoch + 1;
    net.restart(victim, &crash("epoch-held-open", epoch)).await?;
    let status = net.node(victim).exited(4 * EPOCH).await?;
    eyre::ensure!(status.signal() == Some(6), "{victim} ended with {status}, not an abort");
    net.restart(victim, &[]).await?;
    sleep(EPOCH / 2).await;
    net.release_dealings()?;
    wait_for_epoch(port, epoch + 1, 3 * EPOCH).await?;

    wait_caught_up(net.node(victim).port(), port, 2 * EPOCH).await?;
    net.assert_votes(victim).await?;
    net.assert_agreement().await?;
    Ok(())
}

/// A validator goes offline for one, two, three, four and five epochs in turn, missing its
/// dealings and its shares of the keys dealt while it was away. Each time it comes back, catches
/// up, derives its share of the current key from the chain, and votes.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_offline_for_epochs() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_offline", None)?;
    net.start_all().await?;
    let victim = "validator-3";
    let port = net.reference_port(&[victim]);
    wait_for_epoch(port, 2, 4 * EPOCH).await?;

    for epochs in 1..=5u32 {
        net.node(victim).kill();
        let from = current_epoch(port).await?;
        wait_for_epoch(port, from + epochs, (epochs + 2) * EPOCH).await?;
        info!(target: "dkg-faults", epochs, "bringing the validator back");
        net.restart(victim, &[]).await?;
        wait_caught_up(net.node(victim).port(), port, 3 * EPOCH).await?;
        net.assert_votes(victim).await?;
    }
    net.assert_agreement().await?;
    Ok(())
}

/// A validator loses its execution and consensus databases and syncs from genesis, its
/// decryption key derived again from its BLS key, then votes.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_resync_from_scratch() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_resync", None)?;
    net.start_all().await?;
    let victim = "validator-3";
    let port = net.reference_port(&[victim]);
    wait_for_epoch(port, 3, 5 * EPOCH).await?;

    net.node(victim).stop();
    net.node(victim).wipe_databases()?;
    wait_for_epoch(port, current_epoch(port).await? + 1, 3 * EPOCH).await?;
    net.restart(victim, &[]).await?;
    wait_caught_up(net.node(victim).port(), port, 4 * EPOCH).await?;
    net.assert_votes(victim).await?;
    net.assert_agreement().await?;
    Ok(())
}

/// Two validators stop at once, more than the network tolerates: consensus halts, with dealing
/// and the epoch boundary stuck behind it. Both come back, the chain resumes, closes its epochs
/// and deals its keys again, and each of them votes.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_quorum_loss_and_recovery() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut net = Net::create("dkg_quorum_loss", None)?;
    net.start_all().await?;
    let port = net.reference_port(&["validator-2", "validator-3"]);
    wait_for_epoch(port, 2, 4 * EPOCH).await?;

    net.node("validator-2").kill();
    net.node("validator-3").kill();
    let header = latest_header(port).await?.number;
    sleep(EPOCH + EPOCH / 2).await;
    let halted = latest_header(port).await?.number;
    eyre::ensure!(halted <= header + 2, "consensus went on without a quorum: {header} to {halted}");

    net.restart("validator-2", &[]).await?;
    net.restart("validator-3", &[]).await?;
    let epoch = current_epoch(port).await?;
    wait_for_epoch(port, epoch + 2, 5 * EPOCH).await?;
    net.assert_votes("validator-2").await?;
    net.assert_votes("validator-3").await?;
    net.assert_agreement().await?;
    Ok(())
}

/// A new validator stakes, and crashes right after it submits its encryption-key registration.
/// It restarts, registers, is selected into the committee only once its key is registered, and
/// votes with its share.
#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn dkg_joiner_crashes_while_registering() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let mut joiner = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut net = Net::create("dkg_joiner", Some(&joiner))?;
    let hold = net.hold_env();
    net.node(NEW_VALIDATOR).start(&[
        hold,
        (CRASH_AT, "registration-submitted".into()),
        (CRASH_EPOCH, "0".into()),
    ]);
    net.start_all().await?;
    let port = net.reference_port(&[NEW_VALIDATOR]);

    // stake and activate the joiner
    let genesis: rayls_infrastructure_types::Genesis = Config::load_from_path(
        &net.temp_path.join("shared-genesis/genesis/genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let mut governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let txs = generate_new_validator_txs(&net.temp_path, chain, &mut joiner, &mut governance)?;
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    for tx in &txs {
        let pending = provider.send_raw_transaction(&tx.raw).await?;
        let receipt =
            tokio::time::timeout(Duration::from_secs(30), pending.get_receipt()).await??;
        eyre::ensure!(receipt.status(), "{} reverted", tx.label);
    }

    let status = net.node(NEW_VALIDATOR).exited(4 * EPOCH).await?;
    eyre::ensure!(status.signal() == Some(6), "the joiner ended with {status}, not an abort");
    net.restart(NEW_VALIDATOR, &[]).await?;

    // selected only once registered, then a share of every key dealt to it
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let joiner_address = joiner.address();
    let joined = Arc::new(std::sync::Mutex::new(0u32));
    let joined_in = joined.clone();
    wait_until(12 * EPOCH, || {
        let registry = &registry;
        let joined_in = joined_in.clone();
        async move {
            let epoch = registry.getCurrentEpoch().call().await?;
            let committee = registry.getEpochInfo(epoch).call().await?.committee;
            eyre::ensure!(committee.contains(&joiner_address), "not in the committee of {epoch}");
            *joined_in.lock().expect("lock") = epoch;
            Ok(())
        }
    })
    .await?;
    let first = *joined.lock().expect("lock");
    let log = net.node(NEW_VALIDATOR).log_text();
    eyre::ensure!(
        log.contains("dkg encryption key registration submitted"),
        "the joiner never registered"
    );
    let share_line = format!("epoch uses threshold certificates epoch={first} has_share=true");
    wait_until(2 * EPOCH, || {
        let text = net.nodes[NEW_VALIDATOR].log_text();
        let line = share_line.clone();
        async move {
            eyre::ensure!(text.contains(&line), "no share line for epoch {first}");
            Ok(())
        }
    })
    .await?;
    net.assert_votes(NEW_VALIDATOR).await?;
    net.assert_agreement().await?;
    Ok(())
}
