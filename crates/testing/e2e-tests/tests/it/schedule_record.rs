//! End-to-end coverage for the schedule-record boot gate, through real
//! `rayls-network` process boots on a dev single-validator datadir.
//!
//! A dev node bootstraps a local single-validator chain; once the chain has
//! executed, the datadir carries a `schedule-record.yaml` pinning the schedule
//! those blocks ran under. This test verifies the gate end-to-end: moving an
//! already-activated fork in a `--config-file` schedule is refused at boot,
//! activating a fork that was absent from the record (still in the future) is
//! allowed and re-recorded, and a deleted record is re-established on the next
//! boot (trust-on-first-use).
//!
//! Needs the `dev-single-node-setup` feature: the `dev` subcommand and the
//! single-validator `--dev` gate only exist in dev builds.

use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::get_rayls_network_binary;
use rayls_execution_evm::{
    network_profile::{ForkActivation, NetworkConfigFile, NetworkProfile, ScheduleRecord},
    ForkCondition, RaylsHardFork,
};
use rayls_infrastructure_types::{
    get_available_tcp_port, test_utils::init_test_tracing, RaylsNetwork,
};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, ChildStderr, Stdio},
    time::{Duration, Instant},
};
use tokio::time::timeout;

const DEV_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

/// The full built-in local schedule as a config-file profile, with one fork's
/// activation replaced (a `Never` fork becomes `Block(to)`).
fn local_profile_moving(fork: &str, to: u64) -> NetworkProfile {
    let mut hardforks = BTreeMap::new();
    for (fork, condition) in RaylsHardFork::for_network(RaylsNetwork::Local) {
        if let ForkCondition::Block(block) = condition {
            hardforks.insert(fork.name().to_string(), ForkActivation::Block(block));
        }
    }
    hardforks.insert(fork.to_string(), ForkActivation::Block(to));
    NetworkProfile { chain_id: DEV_CHAIN_ID, hardforks }
}

fn write_config_file(base: &Path, name: &str, profile: &NetworkProfile) -> PathBuf {
    let file =
        NetworkConfigFile { networks: BTreeMap::from([("local".to_string(), profile.clone())]) };
    let path = base.join(name);
    std::fs::write(&path, serde_yaml::to_string(&file).expect("config serializes"))
        .expect("config file written");
    path
}

fn read_record(datadir: &Path) -> ScheduleRecord {
    let raw =
        std::fs::read_to_string(datadir.join("schedule-record.yaml")).expect("record readable");
    serde_yaml::from_str(&raw).expect("record parses")
}

/// Boot `rayls-network node` against a dev-bootstrapped datadir. Instance 1
/// shifts reth's ports by zero, so the RPC listens on `port` exactly.
fn start_node(datadir: &Path, port: u16, config_file: Option<&Path>) -> (Child, ChildStderr) {
    let mut command = get_rayls_network_binary().command();
    command
        .env("RL_BLS_PASSPHRASE", rayls_network_cli::dev::DEV_PASSPHRASE)
        .arg("node")
        .arg("--datadir")
        .arg(&*datadir.to_string_lossy())
        .arg("--instance")
        .arg("1")
        .arg("--http")
        .arg("--http.port")
        .arg(port.to_string())
        .arg("--storage.v2");
    if let Some(file) = config_file {
        command.arg("--config-file").arg(&*file.to_string_lossy()).arg("--subnet").arg("local");
    } else {
        command.arg("--network").arg(RaylsNetwork::Local.to_string());
    }
    let mut child =
        command.stdout(Stdio::null()).stderr(Stdio::piped()).spawn().expect("node spawns");
    let stderr = child.stderr.take().expect("stderr piped");
    (child, stderr)
}

async fn wait_for_rpc(url: &str) -> eyre::Result<()> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    timeout(Duration::from_secs(120), async {
        loop {
            match provider.get_chain_id().await {
                Ok(id) => {
                    assert_eq!(id, DEV_CHAIN_ID, "unexpected chain-id from RPC");
                    return;
                }
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    })
    .await
    .map_err(|_| eyre::eyre!("RPC did not come up at {url} within 120s"))?;
    Ok(())
}

/// Wait for the boot to be refused: the process must exit non-zero on its own
/// (a refusal happens pre-launch, so the node never serves RPC). Returns the
/// captured stderr.
async fn wait_for_refusal(
    mut child: Child,
    mut stderr: ChildStderr,
) -> eyre::Result<(std::process::ExitStatus, String)> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    return Err(eyre::eyre!(
                        "node was still running 120s after a boot under a tampered schedule \
                         (the gate should have refused it pre-launch)"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => return Err(e.into()),
        }
    };
    let mut output = String::new();
    stderr.read_to_string(&mut output).ok();
    Ok((status, output))
}

fn kill_quietly(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[ignore = "boots full dev nodes; run independently from other it tests"]
#[tokio::test]
async fn schedule_record_gate_on_real_boots() -> eyre::Result<()> {
    let _guard = e2e_tests::IT_TEST_MUTEX.lock();
    init_test_tracing();

    let temp = tempfile::TempDir::with_prefix("sched_rec_e2e")?;
    let datadir = temp.path().join("datadir");
    std::fs::create_dir_all(&datadir)?;

    // 1. Boot the dev node on an empty datadir and let the chain execute. The RPC port comes from a
    //    free port (and the dashboard is off, freeing its fixed port too), so a stale dev node
    //    holding the defaults cannot break this step. The dev node's other ports (consensus, P2P,
    //    metrics) remain fixed — only the HTTP surface is made collision-proof here.
    let port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no free tcp port for the dev node"))?;
    let mut command = get_rayls_network_binary().command();
    command
        .arg("dev")
        .arg("--datadir")
        .arg(&*datadir.to_string_lossy())
        .arg("--no-dashboard")
        .arg("--http.port")
        .arg(port.to_string());
    let mut dev_node = command.spawn().expect("dev node spawns");
    let rpc_url = format!("http://127.0.0.1:{port}");
    wait_for_rpc(&rpc_url).await?;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    timeout(Duration::from_secs(60), async {
        loop {
            if provider.get_block_number().await.unwrap_or(0) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("dev chain did not produce any blocks"))?;
    kill_quietly(&mut dev_node);

    // 2. The datadir now carries a schedule record pinning the local schedule. The datadir was
    //    fresh at that boot, so the pinned head is 0 — the gate compares the record against the
    //    LIVE head at each subsequent boot.
    let record = read_record(&datadir);
    assert_eq!(record.chain_id, DEV_CHAIN_ID);
    assert_eq!(record.as_of_block, 0, "fresh datadir records its boot head (0)");
    assert_eq!(
        record.hardforks.get("Eip1559"),
        Some(&ForkActivation::Block(0)),
        "local Eip1559 is active from block 0"
    );

    // 3. Tamper: move an ALREADY-ACTIVATED fork (Eip1559, active since block 0) to the future. The
    //    gate must refuse the boot pre-launch.
    let tampered = write_config_file(
        temp.path(),
        "tampered.yaml",
        &local_profile_moving("Eip1559", 1_000_000),
    );
    let port = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no free tcp port for port"))?;
    let (child, stderr) = start_node(&datadir, port, Some(&tampered));
    let (status, output) = wait_for_refusal(child, stderr).await?;
    assert!(!status.success(), "boot under a tampered schedule must be refused");
    assert!(output.contains("Eip1559"), "the refusal should name the moved fork: {output}");
    assert!(
        output.contains("schedule"),
        "the refusal should mention the schedule record: {output}"
    );

    // 4. Activate a fork that the record says was absent (Uups is Never on local, so this boundary
    //    is in the future for any realistic head): the boot is allowed and the record is re-written
    //    with the new boundary.
    let future =
        write_config_file(temp.path(), "future.yaml", &local_profile_moving("Uups", 2_000_000));
    let port2 = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no free tcp port for port2"))?;
    let (mut child, mut stderr) = start_node(&datadir, port2, Some(&future));
    wait_for_rpc(&format!("http://127.0.0.1:{port2}")).await.inspect_err(|e| {
        kill_quietly(&mut child);
        let mut out = String::new();
        stderr.read_to_string(&mut out).ok();
        tracing::error!(target: "schedule-record-test", ?e, node_output = %out, "future-move boot failed");
    })?;
    kill_quietly(&mut child);
    let record = read_record(&datadir);
    assert_eq!(
        record.hardforks.get("Uups"),
        Some(&ForkActivation::Block(2_000_000)),
        "the allowed future activation must be re-recorded"
    );

    // 5. Delete the record: the next boot trusts the selected schedule, warns, and re-establishes
    //    the record at the current head.
    std::fs::remove_file(datadir.join("schedule-record.yaml")).expect("record removed");
    let port3 = get_available_tcp_port("127.0.0.1")
        .ok_or_else(|| eyre::eyre!("no free tcp port for port3"))?;
    let (mut child, _stderr) = start_node(&datadir, port3, None);
    wait_for_rpc(&format!("http://127.0.0.1:{port3}")).await.inspect_err(|e| {
        kill_quietly(&mut child);
        tracing::error!(target: "schedule-record-test", ?e, "re-record boot failed");
    })?;
    kill_quietly(&mut child);
    let record = read_record(&datadir);
    assert_eq!(record.chain_id, DEV_CHAIN_ID);
    assert!(record.as_of_block > 0, "re-established record must pin the head");

    Ok(())
}
