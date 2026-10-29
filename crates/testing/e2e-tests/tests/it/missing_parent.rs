//! With exactly a quorum of validators up (4 of 5), one validator loses the certificate gossip
//! of one round in both directions: its own certificate reaches nobody and it receives no one
//! else's. No live node then holds a quorum of that round's certificates, so nobody proposes a
//! header that would carry the missing ones as vote parents, and only the certificate fetcher
//! can get them. The chain must resume within a few max header delays.
//!
//! The loss comes from a test-only hook in the node (feature `cert-gossip-drop-hook`): creating
//! the file named by `RAYLS_TEST_CERT_GOSSIP_DROP_FILE` makes it drop every certificate gossip,
//! sent or received, of the second round after the one it is handling.
//!
//! Tunables (environment): `MISSING_PARENT_FAULTS` (losses to inject, default 3),
//! `MISSING_PARENT_KEEP` (keep the nodes' data and logs, `<datadir>/node.log`).

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
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::{Child, Stdio},
    time::Duration,
};
use tokio::time::{sleep, Instant};

const VALIDATORS: [(&str, u8); 5] = [
    ("validator-1", 0x11),
    ("validator-2", 0x22),
    ("validator-3", 0x33),
    ("validator-4", 0x44),
    ("validator-5", 0x55),
];
/// The validator that loses one round of certificate gossip.
const VICTIM: usize = 2;
/// The validator kept stopped, so exactly a quorum is up.
const STOPPED: usize = 4;
/// The node the test reads the chain from.
const REFERENCE: usize = 0;
/// Headers the chain must commit after each loss.
const HEADERS: u64 = 10;
/// The longest the chain may go without a new header after a loss. The test network's max
/// header delay is 1 s, a fetch asks one more peer every 5 s, and the fetcher's idle probe runs
/// once a minute.
const MAX_GAP: Duration = Duration::from_secs(30);
/// Test hook log line, one per dropped certificate.
const DROP_LINE: &str = "test hook: dropping certificate gossip";

struct Node {
    instance: u8,
    dir: PathBuf,
    child: Option<Child>,
}

impl Node {
    fn port(&self) -> u16 {
        8546 - u16::from(self.instance)
    }

    fn trigger(&self) -> PathBuf {
        self.dir.join("drop-cert-gossip")
    }

    fn start(&mut self, hook: bool) {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("node.log"))
            .expect("node log opens");
        let mut command = e2e_tests::get_rayls_network_binary().command();
        if hook {
            command.env("RAYLS_TEST_CERT_GOSSIP_DROP_FILE", self.trigger());
        }
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
        self.child = Some(command.spawn().expect("node starts"));
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
            let _ = child.wait();
        }
    }

    fn drops(&self) -> usize {
        std::fs::read_to_string(self.dir.join("node.log"))
            .map(|log| log.matches(DROP_LINE).count())
            .unwrap_or(0)
    }
}

struct Net {
    nodes: Vec<Node>,
    temp: Option<tempfile::TempDir>,
}

impl Drop for Net {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if std::env::var_os("MISSING_PARENT_KEEP").is_some() {
            if let Some(temp) = self.temp.take() {
                eprintln!("missing-parent: kept {}", temp.keep().display());
            }
        }
    }
}

fn create(temp_path: &Path) -> eyre::Result<Vec<Node>> {
    let committee: Vec<(&str, Address)> =
        VALIDATORS.iter().map(|(name, byte)| (*name, Address::from_slice(&[*byte; 20]))).collect();
    let governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let accounts = vec![(
        governance.address(),
        GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
    )];
    config_committee(
        temp_path,
        &temp_path.join("shared-genesis"),
        NODE_PASSWORD.to_string(),
        governance.address(),
        accounts,
        &committee,
    )?;
    Ok(VALIDATORS
        .iter()
        .enumerate()
        .map(|(index, (name, _))| Node {
            instance: index as u8 + 1,
            dir: temp_path.join(name),
            child: None,
        })
        .collect())
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

/// Number of the latest consensus header of the node on `port`.
async fn header_number(port: u16) -> eyre::Result<u64> {
    let provider = ProviderBuilder::new().connect_http(format!("http://127.0.0.1:{port}").parse()?);
    let header: serde_json::Value = provider.raw_request("rayls_latestHeader".into(), ()).await?;
    header["number"].as_u64().ok_or_else(|| eyre::eyre!("no header number in {header}"))
}

async fn wait_headers(port: u16, target: u64, deadline: Duration) -> eyre::Result<()> {
    wait_until(deadline, || async move {
        let number = header_number(port).await?;
        eyre::ensure!(number >= target, "at header {number} of {target}");
        Ok(())
    })
    .await
}

/// Inject one loss, then follow the chain until it commits [`HEADERS`] more headers. Returns the
/// longest time without a new header and the number of certificates the hook dropped.
async fn one_loss(net: &Net) -> eyre::Result<(Duration, usize)> {
    let reference = net.nodes[REFERENCE].port();
    let victim = &net.nodes[VICTIM];
    let drops_before = victim.drops();
    std::fs::write(victim.trigger(), b"")?;
    let trigger = victim.trigger();
    wait_until(Duration::from_secs(30), || {
        let trigger = trigger.clone();
        async move {
            eyre::ensure!(!trigger.exists(), "hook has not armed");
            Ok(())
        }
    })
    .await?;

    let start = header_number(reference).await?;
    let mut last = start;
    let mut last_change = Instant::now();
    let mut longest = Duration::ZERO;
    let deadline = Instant::now() + Duration::from_secs(180);
    while last < start + HEADERS {
        eyre::ensure!(
            Instant::now() < deadline,
            "chain stuck at header {last} (from {start}) after the loss"
        );
        sleep(Duration::from_millis(200)).await;
        let now = header_number(reference).await?;
        longest = longest.max(last_change.elapsed());
        if now > last {
            last = now;
            last_change = Instant::now();
        }
    }
    Ok((longest, victim.drops() - drops_before))
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
async fn lost_parent_certificate_is_fetched_within_a_few_max_header_delays() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    test_utils::init_test_tracing();
    let faults: usize =
        std::env::var("MISSING_PARENT_FAULTS").map(|f| f.parse()).unwrap_or(Ok(3))?;

    let temp = tempfile::TempDir::with_prefix("missing_parent")?;
    let mut net = Net { nodes: create(temp.path())?, temp: Some(temp) };
    for (index, node) in net.nodes.iter_mut().enumerate() {
        node.start(index == VICTIM);
    }
    let reference = net.nodes[REFERENCE].port();
    wait_headers(reference, 10, Duration::from_secs(120)).await?;

    // exactly a quorum left: every round needs the certificate of every live validator
    net.nodes[STOPPED].stop();
    let now = header_number(reference).await?;
    wait_headers(reference, now + 5, Duration::from_secs(60)).await?;

    let mut results = Vec::new();
    for _ in 0..faults {
        let (gap, drops) = one_loss(&net).await?;
        eprintln!("missing-parent: dropped {drops} certificates, longest gap {gap:?}");
        eyre::ensure!(drops > 0, "the hook dropped no certificate");
        results.push(gap);
    }
    eprintln!("missing-parent: longest gaps {results:?}");
    eyre::ensure!(
        results.iter().all(|gap| *gap <= MAX_GAP),
        "the chain went more than {MAX_GAP:?} without a header after a lost certificate: {results:?}"
    );
    Ok(())
}
