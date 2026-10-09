//! A certificate the node has acted on must survive a crash.
//!
//! When a proposer assembles its own certificate, it stores the certificate and then broadcasts it.
//! The store only queues the write, the same as a vote or a header, so a crash in that window loses
//! the certificate. After a restart the node no longer knows its round was certified, so it can
//! build a different header for that round. A second certificate then forms, which two honest nodes
//! see as equivocation.
//!
//! A child process runs the real own-certificate handler on the real MDBX store while the write is
//! blocked, then is killed. The parent reopens the store and checks that the certificate survived.

use crate::{
    crash_test_utils::{
        child_dir, committee, field, report, test_name, wait_for_kill, Child, WriteGate,
        CHILD_START_TIMEOUT, GATE_HOLD,
    },
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{open_db, CertificateStore as _, DatabaseType};
use rayls_infrastructure_types::{
    Certificate, Database as _, Hash as _, SignatureVerificationState, TaskManager,
};
use std::{path::Path, time::Duration};

/// Tells the child process which directory holds the store.
const DIR_ENV: &str = "CERT_DURABILITY_CHILD_DIR";
/// How long the own-certificate handler may take once the database can write.
const STORE_TIMEOUT: Duration = Duration::from_secs(10);

/// The node (first authority) on a real MDBX store, plus one certified header.
struct Node {
    synchronizer: StateSynchronizer<DatabaseType>,
    db: DatabaseType,
    cert: Certificate,
    _task_manager: TaskManager,
}

/// Builds the same committee in every process and opens the store at `dir`.
fn node(dir: &Path) -> Node {
    let fixture = committee();
    let me = fixture.first_authority().consensus_config();
    // The production open path: MDBX, the layered writer and every default table.
    let db = open_db(dir);
    let config = ConsensusConfig::new_with_committee_for_test(
        me.config().clone(),
        db.clone(),
        me.key_config().clone(),
        me.committee().clone(),
        me.network_config().clone(),
    )
    .expect("consensus config");

    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);

    // The node's own certificate, with a quorum of votes, marked verified as the aggregator does
    // before handing it to the own-certificate path (the manager rejects an unverified own cert).
    let mut cert = fixture.certificate(&fixture.first_authority().header(&fixture.committee()));
    let signature = cert.aggregated_signature().expect("fixture cert is signed");
    cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(signature));
    Node { synchronizer, db, cert, _task_manager: task_manager }
}

/// Child side of `sent_certificate_survives_crash`.
/// It does nothing unless that test started it.
#[tokio::test]
async fn cert_durability_child() {
    let Some(dir) = child_dir(DIR_ENV) else {
        return;
    };
    let node = node(&dir);
    // Let the writer finish its startup work so it is idle.
    node.db.persist().await.expect("startup persist");
    let gate = WriteGate::close(&node.db);
    report("READY");

    // Process the own certificate, which stores it right before the proposer broadcasts it.
    // A node that saves it before broadcasting blocks here until the gate opens.
    let pending = node.synchronizer.process_own_certificate(node.cert.clone());
    tokio::pin!(pending);
    match tokio::time::timeout(GATE_HOLD, &mut pending).await {
        Ok(res) => res.expect("process own certificate"),
        Err(_) => {
            gate.open();
            tokio::time::timeout(STORE_TIMEOUT, pending)
                .await
                .expect("own certificate after the gate opened")
                .expect("process own certificate");
        }
    }

    // Read straight from MDBX to see whether the certificate reached disk before the broadcast.
    let on_disk = node.db.inner().contains(&node.cert.digest()).expect("mdbx read");
    report(&format!("STORED {} on_disk={on_disk}", node.cert.digest()));
    wait_for_kill();
}

/// A certificate the proposer has acted on must be on disk before it is sent.
/// Otherwise a crash loses it, the node forgets its round was certified, and it can certify a
/// second, different header for the same round.
#[tokio::test]
async fn sent_certificate_survives_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let child = Child::spawn(test_name!(cert_durability_child), DIR_ENV, dir.path());
    let ready = child.wait_for(&["READY"], CHILD_START_TIMEOUT);
    assert!(ready.is_some(), "child did not start: {}", child.stderr());
    let stored = child.wait_for(&["STORED "], GATE_HOLD + STORE_TIMEOUT);
    let stderr = child.stderr();
    child.kill();
    let stored = stored.unwrap_or_else(|| panic!("child never stored a certificate: {stderr}"));

    // The certificate was stored and the proposer would now broadcast it, so it must be on disk.
    assert_eq!(
        field(&stored, "on_disk"),
        Some("true"),
        "the certificate was stored but was not on disk before the broadcast: {stored}"
    );

    // Reopen the store. The certificate must still be there after the crash.
    let node = node(dir.path());
    assert!(
        node.db.contains(&node.cert.digest()).expect("read after restart"),
        "the certificate was stored but was not on disk after the crash"
    );
}
