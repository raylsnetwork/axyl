//! Crash durability of the header the proposer sends out.
//!
//! A child process runs the real proposer on the real MDBX stack and is then killed.
//! The parent reopens the same directory and checks that the sent header survived.

use super::*;
use crate::{
    consensus::LeaderSwapTable,
    crash_test_utils::{
        child_dir, committee, field, report, test_name, wait_for_kill, Child, WriteGate,
        CHILD_START_TIMEOUT, GATE_HOLD,
    },
};
use rayls_infrastructure_storage::{open_db, DatabaseType};
use rayls_infrastructure_types::{RaylsReceiver as _, RaylsSender as _};
use std::{path::Path, time::Duration};

/// Tells the child process which directory holds the proposer's store.
const DIR_ENV: &str = "PROPOSER_DURABILITY_CHILD_DIR";

/// Opens the first authority's store at `dir` with the same committee in every process.
fn open_node(dir: &Path) -> ConsensusConfig<DatabaseType> {
    let fixture = committee();
    let me = fixture.first_authority().consensus_config();
    // The production open path: MDBX, the layered writer and every default table.
    let db = open_db(dir);
    ConsensusConfig::new_with_committee_for_test(
        me.config().clone(),
        db,
        me.key_config().clone(),
        me.committee().clone(),
        me.network_config().clone(),
    )
    .expect("consensus config")
}

/// How long to wait for the proposer to send a header.
/// It sends at least once per `max_header_delay`, so allow one more for a slow machine.
fn header_wait(config: &ConsensusConfig<DatabaseType>) -> Duration {
    config.parameters().max_header_delay * 2
}

/// Starts the proposer on `cb`.
/// Subscribe to `cb.headers()` before calling this so no header is missed.
fn start_proposer(
    config: &ConsensusConfig<DatabaseType>,
    cb: &ConsensusBus,
    task_manager: &TaskManager,
) {
    let proposer = Proposer::new(
        config.clone(),
        config.authority_id().expect("authority"),
        cb.clone(),
        LeaderSchedule::new(config.committee().clone(), LeaderSwapTable::default()),
        task_manager.get_spawner(),
    );
    proposer.spawn(task_manager);
    cb.execution_replay_complete().send_replace(true);
}

/// Formats a header the way the child reports it.
fn sent_line(header: &Header) -> String {
    format!("SENT {} {:?}", header.round(), header.digest())
}

/// Child side of `sent_header_survives_crash`.
/// It does nothing unless that test started it.
#[tokio::test]
async fn proposer_durability_child() {
    let Some(dir) = child_dir(DIR_ENV) else {
        return;
    };
    let config = open_node(&dir);
    let db = config.node_storage().clone();
    // Let the writer finish its startup work so it is idle.
    db.persist().await.expect("startup persist");
    let gate = WriteGate::close(&db);

    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    start_proposer(&config, &cb, &task_manager);

    // A proposer that waits for the disk only sends once the gate opens.
    let header = match tokio::time::timeout(GATE_HOLD, rx_headers.recv()).await {
        Ok(header) => header,
        Err(_) => {
            gate.open();
            tokio::time::timeout(header_wait(&config), rx_headers.recv())
                .await
                .expect("proposer never sent a header")
        }
    }
    .expect("headers channel closed");

    // Stand-in for the broadcast to peers: the parent learns of the header from this line.
    // The header is read straight from MDBX to see whether it reached disk before the send.
    let on_disk = db.inner().get_last_proposed().expect("mdbx read").map(|h| h.digest())
        == Some(header.digest());
    report(&format!("{} on_disk={on_disk}", sent_line(&header)));
    wait_for_kill();
}

/// A header the proposer has sent to peers must survive a crash.
/// Otherwise the restarted proposer builds a different header for the same round.
/// Peers that voted for the first one refuse the second, so the round never certifies.
#[tokio::test]
async fn sent_header_survives_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let child = Child::spawn(test_name!(proposer_durability_child), DIR_ENV, dir.path());
    let line = child.wait_for(&["SENT "], CHILD_START_TIMEOUT);
    let stderr = child.stderr();
    child.kill();
    let line = line.unwrap_or_else(|| panic!("child never sent a header: {stderr}"));

    // Restart on the same directory.
    let config = open_node(dir.path());
    let saved = config.node_storage().get_last_proposed().expect("read last proposed");
    let saved_line = saved.as_ref().map(sent_line);
    assert!(
        saved_line.as_ref().is_some_and(|s| line.starts_with(&format!("{s} "))),
        "the header was sent but was not on disk after the crash: sent [{line}], saved \
         [{saved_line:?}]"
    );
    assert_eq!(
        field(&line, "on_disk"),
        Some("true"),
        "the proposer sent a header that was not on disk: {line}"
    );

    // The restarted proposer must send the same header again for that round.
    let task_manager = TaskManager::default();
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    start_proposer(&config, &cb, &task_manager);
    let again = tokio::time::timeout(header_wait(&config), rx_headers.recv())
        .await
        .expect("restarted proposer never sent a header")
        .expect("headers channel closed");
    assert_eq!(
        Some(sent_line(&again)),
        saved_line,
        "the restarted proposer sent a different header for the same round"
    );
}
