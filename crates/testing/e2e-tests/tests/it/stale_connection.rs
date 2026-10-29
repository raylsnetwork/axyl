//! A validator is killed and restarted within the 30 s QUIC idle timeout, so every peer still
//! holds its dead connection when the new process connects. The restarted validator must be
//! able to gossip again (peers resend their topic subscriptions) and, with another validator
//! stopped so exactly a quorum (4 of 5) is left, the chain must keep committing.
//!
//! Tunables (environment, read at run time so variants need no rebuild):
//! - `STALE_ROUNDS`: how many restart rounds run on the same network (default 1).
//! - `STALE_KEEP`: keep the nodes' data and logs (`<datadir>/node.log`) after the test.

use crate::epochs::{config_committee, NODE_PASSWORD};
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
use rayls_execution_evm::test_utils::TransactionFactory;
use rayls_infrastructure_types::{test_utils, Address, GenesisAccount, U256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    path::PathBuf,
    process::{Child, Stdio},
    time::Duration,
};
use tokio::time::{sleep, Instant};
use tracing::info;

const VALIDATORS: [(&str, u8); 5] = [
    ("validator-1", 0x11),
    ("validator-2", 0x22),
    ("validator-3", 0x33),
    ("validator-4", 0x44),
    ("validator-5", 0x55),
];
/// The validator that is killed and restarted.
const VICTIM: &str = "validator-3";
/// The validator stopped afterwards, leaving exactly a quorum.
const OTHER: &str = "validator-5";
/// The node the test reads the chain from (never stopped).
const REFERENCE: &str = "validator-1";
/// Time the restarted validator gets to reconnect before its gossip is checked. Well under the
/// 30 s idle timeout would hide nothing: the breakage on main outlasts the timeout.
const SETTLE: Duration = Duration::from_secs(20);
/// Window over which the restarted validator's publishes are checked.
const OBSERVE: Duration = Duration::from_secs(30);
/// Headers the chain must commit with the other validator stopped, within the deadline.
const HEADERS: u64 = 20;
const PROGRESS_DEADLINE: Duration = Duration::from_secs(60);

struct Node {
    name: String,
    instance: u8,
    dir: PathBuf,
    child: Option<Child>,
}

impl Node {
    fn port(&self) -> u16 {
        8546 - u16::from(self.instance)
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("node.log")
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "{} already running", self.name);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .expect("node log opens");
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
            .stdout(Stdio::from(log.try_clone().expect("log handle")))
            .stderr(Stdio::from(log));
        info!(target: "stale-connection", node = %self.name, "starting");
        self.child = Some(command.spawn().expect("node starts"));
    }

    /// Kill with no shutdown, as a crash would: no QUIC close reaches the peers.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "stale-connection", node = %self.name, "killing");
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Graceful stop (SIGTERM).
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "stale-connection", node = %self.name, "stopping");
            let _ = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
            let _ = child.wait();
        }
    }

    /// Current length of the node's log, to read only what is written afterwards.
    fn log_len(&self) -> u64 {
        std::fs::metadata(self.log_path()).map(|m| m.len()).unwrap_or(0)
    }

    /// The node's log written since byte `from`.
    fn log_since(&self, from: u64) -> String {
        let bytes = std::fs::read(self.log_path()).unwrap_or_default();
        String::from_utf8_lossy(&bytes[(from as usize).min(bytes.len())..]).into_owned()
    }
}

struct Net {
    nodes: BTreeMap<String, Node>,
    temp: Option<tempfile::TempDir>,
}

impl Drop for Net {
    fn drop(&mut self) {
        for node in self.nodes.values_mut() {
            node.kill();
        }
        if std::env::var_os("STALE_KEEP").is_some() {
            if let Some(temp) = self.temp.take() {
                eprintln!("stale-connection: kept {}", temp.keep().display());
            }
        }
    }
}

impl Net {
    fn create() -> eyre::Result<Self> {
        let temp = tempfile::TempDir::with_prefix("stale_connection")?;
        let temp_path = temp.path().to_path_buf();
        let committee: Vec<(&str, Address)> = VALIDATORS
            .iter()
            .map(|(name, byte)| (*name, Address::from_slice(&[*byte; 20])))
            .collect();
        let governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
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
        )?;
        let nodes = VALIDATORS
            .iter()
            .enumerate()
            .map(|(index, (name, _))| {
                (
                    name.to_string(),
                    Node {
                        name: name.to_string(),
                        instance: index as u8 + 1,
                        dir: temp_path.join(name),
                        child: None,
                    },
                )
            })
            .collect();
        Ok(Self { nodes, temp: Some(temp) })
    }

    fn node(&mut self, name: &str) -> &mut Node {
        self.nodes.get_mut(name).expect("known node")
    }

    fn port(&self, name: &str) -> u16 {
        self.nodes[name].port()
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
        sleep(Duration::from_millis(200)).await;
    }
}

fn provider(port: u16) -> eyre::Result<impl Provider> {
    Ok(ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?))
}

async fn wait_rpc(port: u16) -> eyre::Result<()> {
    wait_until(Duration::from_secs(60), || async move {
        provider(port)?.get_chain_id().await?;
        Ok(())
    })
    .await
}

/// Number of the latest consensus header of the node on `port`.
async fn header_number(port: u16) -> eyre::Result<u64> {
    let header: serde_json::Value =
        provider(port)?.raw_request("rayls_latestHeader".into(), ()).await?;
    header["number"].as_u64().ok_or_else(|| eyre::eyre!("no header number in {header}"))
}

/// `port`'s consensus reaches the height `reference` held when the wait began.
async fn wait_caught_up(port: u16, reference: u16, deadline: Duration) -> eyre::Result<()> {
    let target = header_number(reference).await?;
    wait_until(deadline, || async move {
        let number = header_number(port).await?;
        eyre::ensure!(number >= target, "node on port {port} at header {number} of {target}");
        Ok(())
    })
    .await
}

/// Outcome of one restart round.
#[derive(Debug)]
struct Round {
    /// Failed gossip publishes the restarted validator logged after [`SETTLE`].
    publish_failures: usize,
    /// Whether [`HEADERS`] headers committed with exactly a quorum left.
    progressed: bool,
}

/// Kill and immediately restart the victim, let it reconnect, then stop the other validator
/// and watch both the victim's publishes and the chain. The other validator is brought back and
/// every node caught up before returning.
async fn one_round(net: &mut Net) -> eyre::Result<Round> {
    let reference = net.port(REFERENCE);
    let victim = net.port(VICTIM);
    let killed = Instant::now();
    net.node(VICTIM).kill();
    net.node(VICTIM).start();
    wait_rpc(victim).await?;
    info!(target: "stale-connection", down = ?killed.elapsed(), "victim serves RPC again");
    sleep(SETTLE).await;
    let mark = net.node(VICTIM).log_len();

    net.node(OTHER).stop();
    let start = header_number(reference).await?;
    let started = Instant::now();
    let progressed = wait_until(PROGRESS_DEADLINE, || async move {
        let now = header_number(reference).await?;
        eyre::ensure!(now >= start + HEADERS, "at header {now} (from {start})");
        Ok(())
    })
    .await
    .is_ok();
    let elapsed = started.elapsed();
    if let Some(rest) = OBSERVE.checked_sub(started.elapsed()) {
        sleep(rest).await;
    }
    let log = net.node(VICTIM).log_since(mark);
    let publish_failures = log.lines().filter(|l| l.contains("gossipsub PUBLISH FAILED")).count();
    let round = Round { publish_failures, progressed };
    eprintln!("stale-connection: {round:?}, {HEADERS} headers in {elapsed:?} (or the deadline)");

    net.node(OTHER).start();
    let other = net.port(OTHER);
    wait_rpc(other).await?;
    wait_caught_up(other, reference, Duration::from_secs(180)).await?;
    wait_caught_up(victim, reference, Duration::from_secs(180)).await?;
    Ok(round)
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn restart_within_idle_timeout_regossips() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let rounds: u32 = std::env::var("STALE_ROUNDS").map(|r| r.parse()).unwrap_or(Ok(1))?;

    let mut net = Net::create()?;
    for node in net.nodes.values_mut() {
        node.start();
    }
    for (name, _) in VALIDATORS {
        wait_rpc(net.port(name)).await?;
    }
    let reference = net.port(REFERENCE);
    wait_until(Duration::from_secs(120), || async move {
        let number = header_number(reference).await?;
        eyre::ensure!(number >= 10, "at header {number}");
        Ok(())
    })
    .await?;

    let mut results = Vec::new();
    for _ in 0..rounds {
        results.push(one_round(&mut net).await?);
    }
    eprintln!("stale-connection: results {results:?}");
    let failed: Vec<_> =
        results.iter().filter(|r| r.publish_failures > 0 || !r.progressed).collect();
    eyre::ensure!(
        failed.is_empty(),
        "restarted validator could not gossip or the quorum stalled: {failed:?}"
    );
    Ok(())
}
