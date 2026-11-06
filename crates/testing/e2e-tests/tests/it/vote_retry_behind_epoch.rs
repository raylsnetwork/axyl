//! One validator closes an epoch late while another is stopped, leaving exactly a quorum (4 of
//! 5) that includes the late one. The chain must keep committing once the late validator enters
//! the new epoch.
//!
//! The late validator runs with the `RL_TEST_LATE_EPOCH_CLOSE_MS` test hook (the node binary is
//! built with `--features test-hooks`): it commits every boundary like its peers but closes the
//! epoch [`LATE_CLOSE`] later, so in the meantime it still runs the old epoch and refuses the new
//! epoch's vote requests as a peer one epoch behind. This is how a validator behaves that
//! commits the boundary seconds after the others, for one because it was missing a certificate
//! it had to fetch. With all five up the others certify without it. Each trial kills the other
//! validator before a boundary, so the four left need the late one's vote for every header of
//! the new epoch.
//!
//! No validator is restarted while a trial runs, and the killed one is restarted only past the
//! 30 s connection idle timeout, so a stale connection cannot be what stalls the chain.
//!
//! Tunables (environment, read at run time):
//! - `LATE_TRIALS`: how many boundaries to cross this way (default 3).
//! - `LATE_CLOSE_MS`: how late the late validator closes each epoch (default 5000).
//! - `LATE_KEEP`: keep the nodes' data and logs (`<datadir>/node.log`) after the test.
//! - `LATE_NODE_LOG`: the nodes' `RUST_LOG` (default: theirs, not the test runner's).

use crate::epochs::{config_committee, EPOCH_DURATION, NODE_PASSWORD};
use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
};
use e2e_tests::{IT_TEST_MUTEX, TEST_NETWORK};
use rand::{rngs::StdRng, SeedableRng as _};
use rayls_execution_evm::test_utils::TransactionFactory;
use rayls_infrastructure_types::{test_utils, Address, GenesisAccount, U256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    path::PathBuf,
    process::{Child, Stdio},
    sync::{Arc, Mutex},
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
/// The validator that closes every epoch late.
const LATE: &str = "validator-3";
/// The validator killed for the trial.
const OTHER: &str = "validator-5";
/// The node the test reads the chain from (never stopped).
const REFERENCE: &str = "validator-1";
/// How late the late validator closes each epoch, by default: well past the ~1.5 s the network
/// layer resends a refused vote request, well short of the minutes a stall lasts.
const LATE_CLOSE: Duration = Duration::from_secs(5);
/// Headers the chain must commit past the boundary with the other validator stopped.
const HEADERS: u64 = 10;
/// Far below the ~4 minutes a vote task to the stopped validator keeps retrying.
const PROGRESS_DEADLINE: Duration = Duration::from_secs(90);
/// How much longer a stalled trial watches the chain with the other validator still down.
const STALL_WATCH: Duration = Duration::from_secs(420);
/// How far into an epoch the other validator is killed.
const KILL_AFTER: Duration = Duration::from_secs(10);

struct Node {
    name: String,
    instance: u8,
    dir: PathBuf,
    /// Extra environment for the node process.
    env: Vec<(&'static str, String)>,
    child: Option<Child>,
}

impl Node {
    fn port(&self) -> u16 {
        8546 - u16::from(self.instance)
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "{} already running", self.name);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("node.log"))
            .expect("node log opens");
        let mut command = e2e_tests::get_rayls_network_binary().command();
        match std::env::var("LATE_NODE_LOG") {
            Ok(filter) => command.env("RUST_LOG", filter),
            Err(_) => command.env_remove("RUST_LOG"),
        };
        command.envs(self.env.iter().map(|(key, value)| (key, value)));
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
        info!(target: "late-validator", node = %self.name, "starting");
        self.child = Some(command.spawn().expect("node starts"));
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Occurrences of `needle` in this node's log.
    fn log_count(&self, needle: &str) -> usize {
        std::fs::read_to_string(self.dir.join("node.log"))
            .map(|log| log.matches(needle).count())
            .unwrap_or(0)
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
        if std::env::var_os("LATE_KEEP").is_some() {
            if let Some(temp) = self.temp.take() {
                eprintln!("late-validator: kept {}", temp.keep().display());
            }
        }
    }
}

impl Net {
    fn create(late_close: Duration) -> eyre::Result<Self> {
        let temp = tempfile::TempDir::with_prefix("vote_retry_behind_epoch")?;
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
                        env: if *name == LATE {
                            vec![(
                                "RL_TEST_LATE_EPOCH_CLOSE_MS",
                                late_close.as_millis().to_string(),
                            )]
                        } else {
                            vec![]
                        },
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
        sleep(Duration::from_millis(100)).await;
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

async fn node_status(port: u16) -> eyre::Result<serde_json::Value> {
    Ok(provider(port)?.raw_request("rayls_nodeStatus".into(), Vec::<()>::new()).await?)
}

/// The `rayls_nodeStatus` of the node on `port`, or the error as text.
async fn status(port: u16) -> String {
    match node_status(port).await {
        Ok(status) => status.to_string(),
        Err(e) => format!("error: {e}"),
    }
}

async fn epoch(port: u16) -> eyre::Result<u64> {
    let status = node_status(port).await?;
    status["epoch"].as_u64().ok_or_else(|| eyre::eyre!("no epoch in {status}"))
}

/// Wait for the node on `port` to enter a new epoch; returns it.
async fn next_epoch(port: u16, deadline: Duration) -> eyre::Result<u64> {
    let from = epoch(port).await?;
    wait_until(deadline, || async move {
        let now = epoch(port).await?;
        eyre::ensure!(now > from, "still at epoch {now}");
        Ok(())
    })
    .await?;
    epoch(port).await
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

/// The committed tip of the node on `port`: its epoch and consensus header number.
async fn tip(port: u16) -> eyre::Result<(u64, u64)> {
    Ok((epoch(port).await?, header_number(port).await?))
}

/// Report every stretch of at least ten seconds in which the node on `port` committed nothing,
/// for `duration`, with times since `since`.
async fn watch_stalls(port: u16, duration: Duration, since: Instant) {
    let watched = Instant::now();
    let mut last = tip(port).await.ok();
    let mut last_moved = Instant::now();
    let stalled_for = |moved: Instant| moved.elapsed() >= Duration::from_secs(10);
    while watched.elapsed() < duration {
        sleep(Duration::from_secs(1)).await;
        let now = tip(port).await.ok();
        if now.is_some() && now != last {
            if let (true, Some((epoch, number))) = (stalled_for(last_moved), last) {
                eprintln!(
                    "late-validator: stalled at header {number} (epoch {epoch}) for at least {:?}, moving \
                     again {:?} after the kill",
                    last_moved.elapsed(),
                    since.elapsed()
                );
            }
            last = now;
            last_moved = Instant::now();
        }
    }
    if let (true, Some((epoch, number))) = (stalled_for(last_moved), last) {
        eprintln!(
            "late-validator: still stalled at header {number} (epoch {epoch}) for at least {:?}, {:?} \
             after the kill",
            last_moved.elapsed(),
            since.elapsed()
        );
    }
}

/// One boundary: [`KILL_AFTER`] into an epoch, kill the other validator and require the chain to
/// commit [`HEADERS`] headers of the next epoch within [`PROGRESS_DEADLINE`] of that epoch's
/// start. Returns whether it progressed. If it did not, keeps the other validator down for
/// [`STALL_WATCH`] more and reports how long each stall lasts. The other validator is then
/// brought back and every node caught up before returning.
async fn one_late_boundary(net: &mut Net, trial: u32) -> eyre::Result<bool> {
    let reference = net.port(REFERENCE);
    let late = net.port(LATE);
    let epoch_secs = Duration::from_secs(EPOCH_DURATION);

    let current = next_epoch(reference, epoch_secs * 3).await?;
    // the late validator has long entered this epoch
    sleep(KILL_AFTER).await;
    net.node(OTHER).kill();
    let killed = Instant::now();
    info!(target: "late-validator", trial, current, "epoch started, other validator killed");

    // the first header of the next epoch, then [`HEADERS`] more
    let next_started = Arc::new(Mutex::new(None::<u64>));
    let progress = || {
        let next_started = next_started.clone();
        async move {
            let (epoch, number) = tip(reference).await?;
            eyre::ensure!(epoch > current, "at header {number} of epoch {epoch}");
            let first = *next_started.lock().unwrap().get_or_insert(number);
            eyre::ensure!(
                number >= first + HEADERS,
                "at header {number}, epoch {epoch} began at {first}"
            );
            Ok(())
        }
    };
    let progressed =
        wait_until(epoch_secs.saturating_sub(KILL_AFTER) + PROGRESS_DEADLINE, progress).await;
    let ok = progressed.is_ok();
    let elapsed = killed.elapsed();
    let mut statuses = String::new();
    for name in [REFERENCE, "validator-2", LATE, "validator-4"] {
        statuses.push_str(&format!("\n  {name}: {}", status(net.port(name)).await));
    }
    eprintln!(
        "late-validator: trial {trial} (epoch {current} to {}): {} {elapsed:?} after the kill{statuses}",
        current + 1,
        if ok { "PROGRESSED" } else { "STALLED" }
    );
    if !ok {
        watch_stalls(reference, STALL_WATCH, killed).await;
    }

    // restarted only past the 30 s idle timeout, so its peers hold no stale connection to it
    sleep(Duration::from_secs(35).saturating_sub(killed.elapsed())).await;
    net.node(OTHER).start();
    let other = net.port(OTHER);
    wait_rpc(other).await?;
    wait_caught_up(other, reference, Duration::from_secs(400)).await?;
    wait_caught_up(late, reference, Duration::from_secs(180)).await?;
    Ok(ok)
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn late_validator_across_an_epoch_boundary_with_one_stopped() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let trials: u32 = std::env::var("LATE_TRIALS").map(|r| r.parse()).unwrap_or(Ok(3))?;
    let late_close = std::env::var("LATE_CLOSE_MS")
        .map(|ms| ms.parse().map(Duration::from_millis))
        .unwrap_or(Ok(LATE_CLOSE))?;

    let mut net = Net::create(late_close)?;
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
    for trial in 1..=trials {
        results.push((trial, one_late_boundary(&mut net, trial).await?));
    }

    // what the nodes logged: the refusals the proposers gave up on (main), the refusals they
    // retried (fixed), and the stale-connection symptom (absent: nothing restarted early)
    for (name, node) in &net.nodes {
        eprintln!(
            "late-validator: {name}: gave up on a refusal {}, retried a refusal {}, \
             NoPeersSubscribedToTopic {}",
            node.log_count("stopping vote requests for stale header"),
            node.log_count("retrying vote request"),
            node.log_count("NoPeersSubscribedToTopic"),
        );
    }
    let stalled: Vec<_> = results.iter().filter(|(_, ok)| !ok).collect();
    eprintln!("late-validator: results {results:?}");
    eyre::ensure!(stalled.is_empty(), "the chain stalled in trials {stalled:?} (of {results:?})");
    Ok(())
}
