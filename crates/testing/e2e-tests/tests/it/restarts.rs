use alloy::primitives::address;
use e2e_tests::{config_local_testnet, IT_TEST_MUTEX};
use escargot::CargoRun;
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use gcloud_sdk::google::cloud::kms::v1::key_operation_attestation;
use jsonrpsee::{
    core::{client::ClientT, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use rayls_infrastructure_types::{
    get_available_tcp_port, keccak256, test_utils::init_test_tracing, Address,
    MIN_RAYLS_PROTOCOL_BASE_FEE,
};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::Value;
use std::{collections::HashMap, fmt::Debug, path::Path, process::Child, time::Duration};
use tokio::runtime::Builder;
use tracing::{error, info};

/// One unit of RLS (10^18) measured in wei.
const WEI_PER_RLS: u128 = 1_000_000_000_000_000_000;

/// Send SIGTERM to child, can use this to pre-send TERM to all children when shutting down.
fn send_term(child: &mut Child) {
    if let Err(e) = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM) {
        error!(target: "restart-test", ?e, "error killing child");
    }
}

/// Helper function to shutdown child processes and log errors.
fn kill_child(child: &mut Child) {
    send_term(child);

    for _ in 0..5 {
        match child.try_wait() {
            Ok(Some(_)) => {
                info!(target: "restart-test", "child exited");
                return;
            }
            Ok(None) => {}
            Err(e) => error!(target: "restart-test", "error waiting on child to exit: {e}"),
        }
        std::thread::sleep(Duration::from_millis(1200));
    }
    // The child is not exiting...
    // The code below will send SIGKILL without the use of nix.
    if let Err(e) = child.kill() {
        error!(target: "restart-test", ?e, "error killing child");
    }
    // Hopefully it will exit now...
    if let Err(e) = child.wait() {
        error!(target: "restart-test", ?e, "error waiting for child to die");
    }
}

/// Send 10 RLS from `key` to `to_account` and confirm it lands on `node_test`.
///
/// `fee_index` is the 0-based ordinal of this transfer within the test (0 for the
/// first, 1 for the second, ...) and is used only for the cumulative basefee
/// assertion -- it is NOT the on-chain nonce. The on-chain nonce is queried live
/// via `get_transaction_count`, because the dev-funded `test-source` account
/// starts at a genesis nonce of 5+N (the pre-genesis ceremony deploys contracts
/// from it), so a hardcoded nonce is rejected as "nonce too low".
fn send_and_confirm(
    node: &str,
    node_test: &str,
    key: &str,
    to_account: Address,
    fee_index: u128,
) -> eyre::Result<()> {
    let basefee_address = address!("0x9999999999999999999999999999999999999999");
    let current = get_balance(node, &to_account.to_string(), 1)?;
    let current_basefee = get_balance(node, &basefee_address.to_string(), 1)?;
    let amount = 10 * WEI_PER_RLS; // 10 RLS
    let expected = current + amount;
    let (from_account, _, _) = decode_key(key)?;
    let nonce = get_transaction_count(node, &from_account)?;
    send_rls(node, key, to_account, amount, MIN_RAYLS_PROTOCOL_BASE_FEE as u128, 21000, nonce)?;

    // sleep
    std::thread::sleep(Duration::from_millis(1000));
    info!(target: "restart-test", "calling get_positive_balance_with_retry...");

    // get positive bal and kill child2 if error
    let bal = get_balance_above_with_retry(node_test, &to_account.to_string(), expected - 1)?;

    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }
    let new_basefee =
        get_balance_above_with_retry(node_test, &basefee_address.to_string(), current_basefee)?;
    // Assert only that the basefee collector STRICTLY INCREASED (the transfer paid a
    // base fee into it) — not that it grew by an exact per-tx amount. Under EIP-1559
    // the base fee drifts block-to-block, so requiring a byte-identical increment on
    // every tx is flaky and was the source of the intermittent
    // "Expected a basefee increment!" failures.
    if fee_index > 0 && new_basefee <= current_basefee {
        error!(target: "restart-test", "basefee collector did not increase: {current_basefee} -> {new_basefee}");
        return Err(Report::msg("Expected a basefee increment!".to_string()));
    }
    Ok(())
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests1(
    client_urls: &[String; 4],
    child2: &mut Child,
    bin: &'static CargoRun,
    temp_path: &Path,
    rpc_port2: u16,
    delay_secs: u64,
) -> eyre::Result<Child> {
    network_advancing(client_urls).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to advance network in restart_tests1");
    })?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    info!(target: "restart-test", "testing blocks same first time in restart_tests1");
    test_blocks_same(client_urls)?;
    // Try once more then fail test.
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to send and confirm in restart_tests1");
    })?;

    info!(target: "restart-test", "killing child2...");
    kill_child(child2);
    info!(target: "restart-test", "child2 dead :D sleeping...");
    std::thread::sleep(Duration::from_secs(delay_secs));

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 5).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    info!(target: "restart-test", "restarting child2...");
    // Restart
    let mut child2 = start_validator(2, bin, temp_path, rpc_port2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "failed to get positive balance with retry in restart_tests1");
        })?;
    if 10 * WEI_PER_RLS != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_RLS != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_RLS)));
    }
    // Try once more then fail test.
    send_and_confirm(&client_urls[0], &client_urls[2], &key, to_account, 1).inspect_err(|e| {
        error!(target: "restart-test", ?e, "send and confirm nonce 1 failed - killing child2...");
        kill_child(&mut child2);
    })?;

    info!(target: "restart-test", "testing blocks same again in restart_tests1");

    test_blocks_same(client_urls).inspect_err(|e| {
        error!(target: "restart-test", ?e, "test blocks same failed - killing child2...");
        kill_child(&mut child2);
    })?;

    // verify nonce monotonicity on the restarted validator
    let latest = get_block_number(&client_urls[2]).inspect_err(|e| {
        error!(target: "restart-test", ?e, "get_block_number failed for nonce check - killing child2...");
        kill_child(&mut child2);
    })?;
    assert_nonce_monotonicity(&client_urls[2], latest).inspect_err(|e| {
        error!(target: "restart-test", ?e, "nonce monotonicity failed - killing child2...");
        kill_child(&mut child2);
    })?;

    Ok(child2)
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
/// This versoin is intended to leave the restarted node in a lagged (not caught up state)
/// in order to exercise more restart code.
fn run_restart_tests_lagged1(
    client_urls: &[String; 4],
    child2: &mut Child,
    bin: &'static CargoRun,
    temp_path: &Path,
    rpc_port2: u16,
    delay_secs: u64,
) -> eyre::Result<Child> {
    network_advancing(client_urls).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to advance network in restart_tests1");
    })?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    info!(target: "restart-test", "testing blocks same first time in restart_tests1");
    test_blocks_same(client_urls)?;
    // Try once more then fail test.
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to send and confirm in restart_tests1");
    })?;

    info!(target: "restart-test", "killing child2...");
    kill_child(child2);
    info!(target: "restart-test", "child2 dead :D sleeping...");
    std::thread::sleep(Duration::from_secs(delay_secs));

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 5).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    let current = get_balance(&client_urls[0], &to_account.to_string(), 1)?;
    let amount = 10 * WEI_PER_RLS; // 10 RLS
    let expected = current + amount;
    // Query the live nonce (the dev-funded account starts at genesis nonce 5+N).
    let (from_account, _, _) = decode_key(&key)?;
    let nonce = get_transaction_count(&client_urls[0], &from_account)?;
    send_rls(
        &client_urls[0],
        &key,
        to_account,
        amount,
        MIN_RAYLS_PROTOCOL_BASE_FEE as u128,
        21000,
        nonce,
    )?;
    std::thread::sleep(Duration::from_millis(5000));

    info!(target: "restart-test", "restarting child2...");
    // Restart
    let mut child2 = start_validator(2, bin, temp_path, rpc_port2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "failed to get positive balance with retry in restart_tests1");
        })?;
    if 10 * WEI_PER_RLS != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_RLS != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_RLS)));
    }
    let bal = get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), expected - 1)?;
    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }

    info!(target: "restart-test", "testing blocks same again in restart_tests1");

    // verify nonce monotonicity on the restarted validator
    let latest = get_block_number(&client_urls[2]).inspect_err(|e| {
        error!(target: "restart-test", ?e, "get_block_number failed for nonce check - killing child2...");
        kill_child(&mut child2);
    })?;
    assert_nonce_monotonicity(&client_urls[2], latest).inspect_err(|e| {
        error!(target: "restart-test", ?e, "nonce monotonicity failed - killing child2...");
        kill_child(&mut child2);
    })?;

    Ok(child2)
}

/// Run the second part of tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests2(client_urls: &[String; 4]) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.
    test_blocks_same(client_urls)?; // Starting from a solid position after a restart?
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    for (i, uri) in client_urls.iter().enumerate().take(4) {
        let bal = get_positive_balance_with_retry(uri, &to_account.to_string())?;
        if 20 * WEI_PER_RLS != bal {
            return Err(Report::msg(format!(
                "Expected a balance of {} got {bal} for node {i}!",
                20 * WEI_PER_RLS
            )));
        }
    }
    let number_start = get_block_number(&client_urls[3])?;
    if let Err(e) = send_and_confirm(&client_urls[0], &client_urls[3], &key, to_account, 2) {
        let number_0 = get_block_number(&client_urls[0])?;
        let number_1 = get_block_number(&client_urls[1])?;
        let number_2 = get_block_number(&client_urls[2])?;
        let number_3 = get_block_number(&client_urls[3])?;
        if number_start == number_3 {
            return Err(eyre::eyre!(
                "Stuck on block {number_3}, other nodes {number_0}, {number_1}, {number_2}, error: {e}"
            ));
        }
        return Err(e);
    }
    test_blocks_same(client_urls)?;

    // verify nonce monotonicity on the restarted validator after full network restart
    let latest = get_block_number(&client_urls[2])?;
    assert_nonce_monotonicity(&client_urls[2], latest)?;

    Ok(())
}

fn network_advancing(client_urls: &[String; 4]) -> eyre::Result<()> {
    fn max_start(client_urls: &[String; 4]) -> eyre::Result<u64> {
        let mut start_num = get_block_number(&client_urls[0])?;
        start_num = start_num.max(get_block_number(&client_urls[1])?);
        start_num = start_num.max(get_block_number(&client_urls[2])?);
        start_num = start_num.max(get_block_number(&client_urls[3])?);
        Ok(start_num)
    }
    let start_num = max_start(client_urls)?;
    let mut next_num = start_num;
    let mut i = 0;
    // Wait until a node is advancing agian, network should be back now.
    while next_num <= start_num {
        std::thread::sleep(Duration::from_secs(1));
        next_num = max_start(client_urls)?;
        i += 1;
        if i > 45 {
            return Err(eyre::eyre!(
                "Network not advancing past {next_num} within 45 seconds after restart!"
            ));
        }
    }
    Ok(())
}

fn do_restarts(delay: u64, lagged: bool) -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();
    info!(target: "restart-test", "do_restarts, delay: {delay}");
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, "restart_test".to_string(), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_rayls_network_binary();
    let mut children: [Option<Child>; 4] = [None, None, None, None];
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0, 0, 0, 0];
    for (i, child) in children.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        *child = Some(start_validator(i, &bin, &temp_path, rpc_port));
    }

    // pass &mut to `run_restart_tests1` to shutdown child in case of error
    let mut child2 = children[2].take().expect("missing child 2");

    info!(target: "restart-test", "Running restart tests 1");
    // run restart tests1
    let res1 = if lagged {
        run_restart_tests_lagged1(&client_urls, &mut child2, &bin, &temp_path, rpc_ports[2], delay)
    } else {
        run_restart_tests1(&client_urls, &mut child2, &bin, &temp_path, rpc_ports[2], delay)
    };
    info!(target: "restart-test", "Ran restart tests 1: {res1:?}");
    let is_ok = res1.is_ok();

    // kill new child2 if successfully restarted
    let assert_str = match res1 {
        Ok(mut child2_restarted) => {
            kill_child(&mut child2_restarted);
            "".to_string()
        }
        Err(err) => {
            // run_restart_tests1 shutsdown child2 on error
            tracing::error!(target: "restart-test", "Got error: {err}");
            err.to_string()
        }
    };

    // send SIGTERM to all children (child2 should already be dead)
    // This lets them start shutting down in parrallel.
    for (i, child) in children.iter_mut().enumerate() {
        if i != 2 {
            let child = child.as_mut().expect("missing a child");
            send_term(child);
        }
    }

    // kill all children (child2 should already be dead)
    for (i, child) in children.iter_mut().enumerate() {
        // Best effort to kill all the other nodes.
        if i != 2 {
            let child = child.as_mut().expect("missing a child");
            kill_child(child);
            info!(target: "restart-test", "kill and wait on child{i} complete");
        }
    }

    // Make sure we shutdown nodes even if an error in first testing.
    assert!(is_ok, "{}", assert_str);
    let to_account = address_from_word("testing");
    // The validators should be down now, confirm.
    assert!(get_balance(&client_urls[0], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[1], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[3], &to_account.to_string(), 5).is_err());

    info!(target: "restart-test", "all nodes shutdown...restarting network");
    // Restart network
    for (i, child) in children.iter_mut().enumerate() {
        *child = Some(start_validator(i, &bin, &temp_path, rpc_ports[i]));
    }

    info!(target: "restart-test", "Running restart tests 2");
    let res2 = run_restart_tests2(&client_urls);
    info!(target: "restart-test", "Ran restart tests 2: {res2:?}");

    // SIGTERM children so they can shutdown in parrellel.
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        send_term(child);
    }

    // kill children before returning final_result
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        kill_child(child);
        info!(target: "restart-test", "kill and wait on child complete for final result");
    }
    res2
}

/// Test a restart case with a short delay, the stopped node should rejoin consensus.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as separate step"]
fn test_restartstt() -> eyre::Result<()> {
    do_restarts(2, false)
}

/// Run some test to make sure an observer is participating in the network.
fn run_observer_tests(client_urls: &[String; 4], obs_url: &str) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    test_blocks_same(client_urls)?;
    // Send to observer, validator confirms.
    send_and_confirm(obs_url, &client_urls[2], &key, to_account, 0)?;
    // Send to observer, validator confirms- second time.
    send_and_confirm(obs_url, &client_urls[3], &key, to_account, 1)?;

    // Send to a validator, observer sees transfer.
    send_and_confirm(&client_urls[0], obs_url, &key, to_account, 2)?;

    test_blocks_same(client_urls)?;
    Ok(())
}

/// Test an observer node can submit txns.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as separate step"]
fn test_restarts_observer() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();
    info!(target: "restart-test", "do_restarts_observer");
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, "restart_test".to_owned(), None)
            .expect("failed to config");
    }
    let bin = e2e_tests::get_rayls_network_binary();
    let mut children: [Option<Child>; 4] = [None, None, None, None];
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0, 0, 0, 0];
    for (i, child) in children.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        *child = Some(start_validator(i, &bin, &temp_path, rpc_port));
    }
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for child!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let mut obs_child = start_observer(4, &bin, &temp_path, obs_rpc_port);
    let res = run_observer_tests(&client_urls, &obs_url);

    // SIGTERM children so they can shutdown in parrellel.
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        send_term(child);
    }
    send_term(&mut obs_child);

    // kill children before returning final_result
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        kill_child(child);
        info!(target: "restart-test", "kill and wait on child complete for final result");
    }
    kill_child(&mut obs_child);
    res
}

/// Test that an observer started AFTER consensus has advanced can catch up.
///
/// This is the regression test for the observer catch-up stall caused by
/// MAX_WALK_DEPTH=500 in commit 892a2ba. The test starts 4 validators, lets
/// consensus advance for ~30 seconds, then starts an observer that must
/// catch up from genesis to the current tip.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as separate step"]
fn test_observer_late_start_catchup() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();
    info!(target: "restart-test", "test_observer_late_start_catchup");
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    config_local_testnet(&temp_path, "restart_test".to_owned(), None).expect("failed to config");

    let bin = e2e_tests::get_rayls_network_binary();

    // start 4 validators only - no observer yet
    let mut children: [Option<Child>; 4] = [None, None, None, None];
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for (i, child) in children.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        client_urls[i].push_str(&format!(":{rpc_port}"));
        *child = Some(start_validator(i, &bin, &temp_path, rpc_port));
    }

    // wait for network to advance and produce blocks
    network_advancing(&client_urls).inspect_err(|e| {
        for child in children.iter_mut() {
            kill_child(child.as_mut().unwrap());
        }
        error!(target: "restart-test", ?e, "network failed to advance before observer start");
    })?;

    // send a transaction so the observer has state to verify after catch-up
    let key = get_key("test-source");
    let to_account = address_from_word("observer-catchup-test");
    send_and_confirm(&client_urls[0], &client_urls[1], &key, to_account, 0).inspect_err(|e| {
        for child in children.iter_mut() {
            kill_child(child.as_mut().unwrap());
        }
        error!(target: "restart-test", ?e, "failed to send pre-observer transaction");
    })?;

    // let consensus advance further to create a meaningful gap
    info!(target: "restart-test", "waiting 30s for consensus to advance before starting observer");
    std::thread::sleep(Duration::from_secs(30));

    let validator_height = get_block_number(&client_urls[0])?;
    info!(target: "restart-test", validator_height, "validator block height before observer start");

    // NOW start the observer - it must catch up from genesis
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let mut obs_child = start_observer(4, &bin, &temp_path, obs_rpc_port);
    info!(target: "restart-test", obs_url, "observer started, waiting for catch-up");

    // wait for observer to catch up to the validator's block height
    let catchup_result = (|| -> eyre::Result<()> {
        for attempt in 0..120 {
            std::thread::sleep(Duration::from_secs(2));
            match get_block_number(&obs_url) {
                Ok(obs_height) => {
                    info!(
                        target: "restart-test",
                        obs_height,
                        validator_height,
                        attempt,
                        "observer catch-up progress"
                    );
                    if obs_height >= validator_height {
                        info!(target: "restart-test", obs_height, "observer caught up");
                        return Ok(());
                    }
                }
                Err(_) if attempt < 30 => {
                    // observer might not have RPC ready yet
                    continue;
                }
                Err(e) => {
                    return Err(eyre::eyre!("observer RPC failed after 60s: {e}"));
                }
            }
        }
        Err(eyre::eyre!(
            "observer did not catch up within 240s (validator was at block {validator_height})"
        ))
    })();

    if let Err(ref e) = catchup_result {
        error!(target: "restart-test", ?e, "observer catch-up FAILED");
    }

    // verify the observer has the same state as the validator
    if catchup_result.is_ok() {
        let obs_balance = get_balance(&obs_url, &to_account.to_string(), 10)?;
        let val_balance = get_balance(&client_urls[0], &to_account.to_string(), 1)?;
        if obs_balance != val_balance {
            error!(
                target: "restart-test",
                obs_balance,
                val_balance,
                "observer balance mismatch after catch-up"
            );
            return Err(Report::msg(format!(
                "Observer balance {obs_balance} != validator balance {val_balance}"
            )));
        }
        info!(target: "restart-test", obs_balance, "observer state verified - balances match");

        // verify nonce monotonicity on the observer
        let obs_height = get_block_number(&obs_url)?;
        assert_nonce_monotonicity(&obs_url, obs_height)?;
    }

    // cleanup
    for child in children.iter_mut() {
        send_term(child.as_mut().unwrap());
    }
    send_term(&mut obs_child);
    for child in children.iter_mut() {
        kill_child(child.as_mut().unwrap());
    }
    kill_child(&mut obs_child);

    catchup_result
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as separate step"]
fn test_restarts_delayed() -> eyre::Result<()> {
    do_restarts(70, false)
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.  Lag the restarted validator.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as separate step"]
fn test_restarts_lagged_delayed() -> eyre::Result<()> {
    do_restarts(70, true)
}

/// Start a process running a validator node.
fn start_validator(
    instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    mut rpc_port: u16,
) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    // The instance option will still change a set port so account for that.
    rpc_port += instance as u16;
    let mut command = bin.command();

    command
        .env("RL_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--instance")
        .arg(format!("{}", instance + 1))
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"));

    #[cfg(feature = "faucet")]
    command
        .arg("--public-key") // If the binary is built with the faucet need this to start...
        .arg("0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99");

    command.spawn().expect("failed to execute")
}

/// Start a process running an observer node.
fn start_observer(
    instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    mut rpc_port: u16,
) -> Child {
    let data_dir = base_dir.join("observer");
    // The instance option will still change a set port so account for that.
    rpc_port += instance as u16;
    let mut command = bin.command();
    command
        .env("RL_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--observer")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--instance")
        .arg(format!("{}", instance + 1))
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"));
    command.spawn().expect("failed to execute")
}

/// Assert that block nonces are monotonically non-decreasing for a given node.
///
/// The EVM block header nonce encodes `(epoch << 32) | round`. After a restart,
/// nonces must never go backwards -- a decrease would indicate a fork where
/// duplicate consensus outputs produced blocks out of order.
fn assert_nonce_monotonicity(node: &str, latest_block: u64) -> eyre::Result<()> {
    let mut prev_nonce: u64 = 0;
    for block_num in 1..=latest_block {
        let block = get_block(node, Some(block_num))?;
        let nonce_str = block["nonce"]
            .as_str()
            .ok_or_else(|| Report::msg(format!("missing nonce at block {block_num}")))?;
        let nonce = u64::from_str_radix(&nonce_str[2..], 16)?;
        if nonce < prev_nonce {
            return Err(Report::msg(format!(
                "Fork detected: nonce went backwards at block {block_num}: {nonce:#x} < {prev_nonce:#x}"
            )));
        }
        prev_nonce = nonce;
    }
    info!(target: "restart-test", "nonce monotonicity OK for {node} up to block {latest_block}");
    Ok(())
}

fn test_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
    info!(target: "restart-test", "calling get_block for {:?}", &client_urls[0]);
    let block0 = get_block(&client_urls[0], None)?;
    let number = u64::from_str_radix(&block0["number"].as_str().unwrap_or("0x100_000")[2..], 16)?;
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[1]);
    let block = get_block(&client_urls[1], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[2]);
    let block = get_block(&client_urls[2], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same! block0: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[3]);
    let block = get_block(&client_urls[3], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    info!(target: "restart-test", "all rpcs returned same block hash");
    Ok(())
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the RLS and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let res_str: String =
        call_rpc(node, "eth_getBalance", rpc_params!(address, "latest"), retries)?;
    info!(target: "restart-test", "get_balance for {node}: parsing string {res_str}");
    let rls = u128::from_str_radix(&res_str[2..], 16)?;
    info!(target: "restart-test", "get_balance for {node}: {rls:?}");
    Ok(rls)
}

/// Retry up to 10 times to retrieve an account balance > 0.
fn get_positive_balance_with_retry(node: &str, address: &str) -> eyre::Result<u128> {
    get_balance_above_with_retry(node, address, 0)
}

/// Retry up to 30 times to retrieve an account balance > above.
///
/// Max time to get balance is 1min.
fn get_balance_above_with_retry(node: &str, address: &str, above: u128) -> eyre::Result<u128> {
    let mut bal = get_balance(node, address, 5)?;
    let mut i = 0;
    while i < 45 && bal <= above {
        std::thread::sleep(Duration::from_millis(1200));
        i += 1;
        bal = get_balance(node, address, 5)?;
    }
    if i == 45 && bal <= above {
        error!(target:"restart-test", "get_balance_above_with_retry i == 30 - returning error!!");
        Err(Report::msg(format!("Failed to get a balance {bal} for {address} above {above}")))
    } else {
        Ok(bal)
    }
}

/// If key starts with 0x then return it otherwise generate the key from the key string.
fn get_key(key: &str) -> String {
    if key.starts_with("0x") {
        key.to_string()
    } else {
        let (_, _, key) = account_from_word(key);
        key
    }
}

fn get_block(node: &str, block_number: Option<u64>) -> eyre::Result<HashMap<String, Value>> {
    let params = if let Some(block_number) = block_number {
        rpc_params!(format!("0x{block_number:x}"), true)
    } else {
        rpc_params!("latest", true)
    };
    call_rpc(node, "eth_getBlockByNumber", params.clone(), 10)
}

fn get_block_number(node: &str) -> eyre::Result<u64> {
    let block = get_block(node, None)?;
    Ok(u64::from_str_radix(&block["number"].as_str().unwrap_or("0x100_000")[2..], 16)?)
}

/// Get the next (pending) nonce for an address via `eth_getTransactionCount`.
///
/// Use this instead of assuming a starting nonce of 0: the dev-funded ceremony
/// account (`test-source`) begins at a genesis nonce of 5+N, so hardcoded nonces
/// are rejected as "nonce too low".
fn get_transaction_count(node: &str, address: &str) -> eyre::Result<u128> {
    let res_str: String =
        call_rpc(node, "eth_getTransactionCount", rpc_params!(address, "pending"), 5)?;
    Ok(u128::from_str_radix(res_str.strip_prefix("0x").unwrap_or(&res_str), 16)?)
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
fn address_from_word(key_word: &str) -> Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (_, public_key) = secp.generate_keypair(&mut rand);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    Address::from_slice(&hash[12..])
}

/// Return the (account, public key, secret key) generated from key_word.
fn account_from_word(key_word: &str) -> (String, String, String) {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand);
    let keypair = Keypair::from_secret_key(&secp, &secret_key);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    let address = Address::from_slice(&hash[12..]);
    let pubkey = keypair.public_key().serialize();
    let secret = keypair.secret_bytes();
    (address.to_string(), const_hex::encode(pubkey), const_hex::encode(secret))
}

/// Create, sign and submit a TXN to transfer RLS from key's account to to_account.
fn send_rls(
    node: &str,
    key: &str,
    to_account: Address,
    amount: u128,
    gas_price: u128,
    gas: u128,
    nonce: u128,
) -> eyre::Result<()> {
    let mut to_addr = [0_u8; 20];
    //const_hex::decode_to_slice(to_account, &mut to_addr[..])?;
    to_addr.copy_from_slice(to_account.as_slice());
    let (from_account, _, _) = decode_key(key)?;
    let new_transaction = LegacyTransaction {
        chain: 0x7e1,
        nonce,
        to: Some(to_addr),
        value: amount,
        gas_price,
        gas,
        data: vec![/* contract code or other data */],
    };
    let key_bytes: [u8; 32] = const_hex::decode(key)?
        .try_into()
        .map_err(|_| Report::msg("Invalid secret key length, expected 32 bytes"))?;
    let secret_key = SecretKey::from_byte_array(key_bytes)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let transaction_bytes = new_transaction.sign(&ecdsa);
    let res_str: String = call_rpc(
        node,
        "eth_sendRawTransaction",
        rpc_params!(const_hex::encode(transaction_bytes)),
        1,
    )?;
    info!(target: "restart-test", "Submitted RLS transfer from {from_account} to {to_account} for {amount}: {res_str}");
    Ok(())
}

/// Decode a secret key into it's public key and account.
/// Returns a tuple of (account, public_key, public_key_long) as hex encoded strings.
fn decode_key(key: &str) -> eyre::Result<(String, String, String)> {
    match const_hex::decode(key) {
        Ok(key) => {
            let key = key
                .try_into()
                .map_err(|_| Report::msg("Invalid secret key length, expected 32 bytes"))?;
            match SecretKey::from_byte_array(key) {
                Ok(secret_key) => {
                    let secp = Secp256k1::new();
                    let keypair = Keypair::from_secret_key(&secp, &secret_key);
                    let public_key = keypair.public_key();
                    // strip out the first byte because that should be the
                    // SECP256K1_TAG_PUBKEY_UNCOMPRESSED tag returned by
                    // libsecp's uncompressed pubkey serialization
                    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
                    let address = Address::from_slice(&hash[12..]);
                    Ok((
                        address.to_string(),
                        const_hex::encode(public_key.serialize()),
                        const_hex::encode(public_key.serialize_uncompressed()),
                    ))
                }
                Err(err) => Err(Report::msg(err.to_string())),
            }
        }
        Err(err) => Err(Report::msg(err.to_string())),
    }
}

/// Make an RPC call to node with command and params.
/// Wraps any Eyre otherwise returns the result as a String.
/// This is for testing and will try up to retries times at one second intervals to send the
/// request.
fn call_rpc<R, Params>(node: &str, command: &str, params: Params, retries: usize) -> eyre::Result<R>
where
    R: DeserializeOwned + Debug,
    Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone + Debug,
{
    // jsonrpsee is async AND tokio specific so give it a runtime (and can't use a crate like
    // pollster)...
    let runtime = Builder::new_current_thread().enable_io().enable_time().build()?;

    let resp = runtime.block_on(async move {
        let client = HttpClientBuilder::default().build(node).expect("couldn't build rpc client");
        let mut resp = client.request(command, params.clone()).await;
        let mut i = 0;
        while i < retries && resp.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let client =
                HttpClientBuilder::default().build(node).expect("couldn't build rpc client");
            resp = client.request(command, params.clone()).await;
            i += 1;
        }
        resp.inspect_err(|_| {
            error!(target: "restart-tests", ?command, ?node, ?params, "rpc call failed");
        })
    });

    Ok(resp?)
}
