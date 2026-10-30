//! A validator stays down across an epoch boundary and comes back while another validator
//! stops, leaving exactly a quorum (4 of 5) that includes the returning one. The returning
//! validator must catch up, become active, and the chain must keep committing.
//!
//! The failure this guards against: the returning validator's catch-up crosses the epoch
//! boundary, the epoch transition resets its `last_consensus_header` watch to the default, and
//! the next state-sync walk starts at the covered anchor (the network tip it already cached).
//! That walk publishes nothing, so the node never learns the head, stays `inactive_cvv`, and the
//! three active validators cannot reach a quorum without it: the network halts until another
//! validator returns.
//!
//! The victim stays down for [`OFFLINE`] (past the 30 s idle connection timeout, so no stale
//! connection survives, and at least one epoch boundary, which the test waits for). The other
//! validator stops as soon as the victim serves RPC, so no new header is produced to hide the
//! defect.
//!
//! The test fails only on that signature (victim `inactive_cvv`, its last walk started at the
//! covered anchor and nothing was published). A stall without it belongs to another defect: it
//! is reported, the network is recovered, and the next fault runs.
//!
//! Tunables (environment, read at run time):
//! - `ANCHOR_FAULTS`: how many faults to inject (default 6); the test stops at the first
//!   anchor-republish stall.
//! - `ANCHOR_OFFLINE_SECS`: how long the victim stays down (default 40).
//! - `ANCHOR_KEEP`: keep the nodes' data and logs (`<datadir>/node.log`) after the test.

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
/// The validator that goes down across an epoch boundary.
const VICTIM: &str = "validator-3";
/// The validator stopped while the victim comes back.
const OTHER: &str = "validator-5";
/// The node the test reads the chain from (never stopped).
const REFERENCE: &str = "validator-1";
/// Headers the chain must commit with the other validator stopped.
const HEADERS: u64 = 20;
const PROGRESS_DEADLINE: Duration = Duration::from_secs(120);
/// Default time the victim stays down.
const OFFLINE: Duration = Duration::from_secs(40);

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
        info!(target: "anchor-republish", node = %self.name, "starting");
        self.child = Some(command.spawn().expect("node starts"));
    }

    /// Kill with no shutdown, as a crash would.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "anchor-republish", node = %self.name, "killing");
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Graceful stop (SIGTERM).
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            info!(target: "anchor-republish", node = %self.name, "stopping");
            let _ = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
            let _ = child.wait();
        }
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
        if std::env::var_os("ANCHOR_KEEP").is_some() {
            if let Some(temp) = self.temp.take() {
                eprintln!("anchor-republish: kept {}", temp.keep().display());
            }
        }
    }
}

impl Net {
    fn create() -> eyre::Result<Self> {
        let temp = tempfile::TempDir::with_prefix("anchor_republish")?;
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

/// The anchor-republish signature in the victim's log since its restart: the last walk derived
/// its coverage from cached headers at anchor `N`, a later probe (with the watch at 0) reported
/// the same `N` as the network tip, and nothing was published to the watch after that walk.
/// Returns the anchor when found.
fn anchor_signature(log: &str) -> Option<u64> {
    let clean = strip_ansi(log);
    let lines: Vec<&str> = clean.lines().collect();
    let (at, anchor) = lines.iter().enumerate().rev().find_map(|(i, line)| {
        let rest = line.split("derived walk coverage from cached headers anchor=").nth(1)?;
        Some((i, rest.split_whitespace().next()?.parse::<u64>().ok()?))
    })?;
    let after = &lines[at + 1..];
    let probe = format!("current=0 peer_latest={anchor}");
    let probed =
        after.iter().any(|line| line.contains("probe discovered") && line.contains(&probe));
    let published = after.iter().any(|line| line.contains("notifying watchers"));
    (probed && !published).then_some(anchor)
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

enum Outcome {
    Progressed,
    /// Stalled with the victim inactive and the anchor-republish signature in its log.
    AnchorStall(String),
    /// Stalled for some other reason (reported, not this test's defect).
    OtherStall(String),
}

/// One fault: take the victim down across an epoch boundary, bring it back, stop the other
/// validator at once, and require the chain to commit [`HEADERS`] headers within
/// [`PROGRESS_DEADLINE`]. Unless it was an anchor-republish stall, the other validator is brought
/// back and every node caught up before returning.
async fn one_fault(net: &mut Net, offline: Duration) -> eyre::Result<Outcome> {
    let reference = net.port(REFERENCE);
    let victim = net.port(VICTIM);
    let down_epoch = epoch(reference).await?;
    net.node(VICTIM).kill();
    sleep(offline).await;
    // The victim's catch-up must cross at least one epoch boundary.
    wait_until(Duration::from_secs(120), || async move {
        let now = epoch(reference).await?;
        eyre::ensure!(now > down_epoch, "reference still in epoch {now}");
        Ok(())
    })
    .await?;
    let log_offset = std::fs::metadata(net.node(VICTIM).log_path())?.len() as usize;
    net.node(VICTIM).start();
    wait_rpc(victim).await?;
    net.node(OTHER).stop();
    let start = header_number(reference).await?;
    info!(target: "anchor-republish", down_epoch, start, "victim back, other validator stopped");
    let started = Instant::now();
    let progressed = wait_until(PROGRESS_DEADLINE, || async move {
        let now = header_number(reference).await?;
        eyre::ensure!(now >= start + HEADERS, "at header {now} (from {start})");
        Ok(())
    })
    .await;
    let elapsed = started.elapsed();
    let mut statuses = String::new();
    for name in [REFERENCE, "validator-2", VICTIM, "validator-4"] {
        statuses.push_str(&format!("\n  {name}: {}", status(net.port(name)).await));
    }
    let ok = progressed.is_ok();
    eprintln!(
        "anchor-republish: offline {offline:?} from epoch {down_epoch}: {} after {elapsed:?}{statuses}",
        if ok { "PROGRESSED" } else { "STALLED" }
    );
    let mut outcome = Outcome::Progressed;
    if !ok {
        let log = std::fs::read(net.node(VICTIM).log_path())?;
        let log = String::from_utf8_lossy(&log[log_offset.min(log.len())..]).into_owned();
        let victim_role =
            node_status(victim).await.ok().and_then(|s| s["role"].as_str().map(str::to_owned));
        let detail = format!("after {elapsed:?}{statuses}");
        outcome = match (anchor_signature(&log), victim_role.as_deref()) {
            (Some(anchor), Some("inactive_cvv")) => {
                return Ok(Outcome::AnchorStall(format!(
                    "victim stayed inactive_cvv; its walk started at the covered anchor {anchor} \
                     and published nothing {detail}"
                )))
            }
            _ => Outcome::OtherStall(format!("victim role {victim_role:?} {detail}")),
        };
    }
    net.node(OTHER).start();
    let other = net.port(OTHER);
    wait_rpc(other).await?;
    wait_caught_up(other, reference, Duration::from_secs(180)).await?;
    wait_caught_up(victim, reference, Duration::from_secs(180)).await?;
    Ok(outcome)
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn validator_back_across_epoch_boundary_with_exact_quorum() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let faults: u32 = std::env::var("ANCHOR_FAULTS").map(|r| r.parse()).unwrap_or(Ok(6))?;
    let offline = std::env::var("ANCHOR_OFFLINE_SECS")
        .map(|s| s.parse().map(Duration::from_secs))
        .unwrap_or(Ok(OFFLINE))?;

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

    let mut other_stalls = 0;
    for fault in 1..=faults {
        match one_fault(&mut net, offline).await? {
            Outcome::Progressed => {}
            Outcome::AnchorStall(detail) => {
                eyre::bail!("fault {fault} of {faults}: the network halted: {detail}")
            }
            Outcome::OtherStall(detail) => {
                other_stalls += 1;
                eprintln!(
                    "anchor-republish: fault {fault} of {faults} stalled without the \
                     anchor-republish signature (another defect, not counted): {detail}"
                );
            }
        }
    }
    eprintln!(
        "anchor-republish: {faults} faults, no anchor-republish stall, {other_stalls} other stalls"
    );
    Ok(())
}
