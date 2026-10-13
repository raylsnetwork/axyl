//! A vote that left the node must survive a crash.
//!
//! A child process runs the real vote handler on the real MDBX store.
//! The parent kills it with SIGKILL and reopens the same directory.
//! A new handler is then asked to vote for a conflicting header.

use crate::{
    crash_test_utils::{
        child_dir, committee, field, report, test_name, wait_for_kill, Child, WriteGate,
        CHILD_START_TIMEOUT, GATE_HOLD,
    },
    error::PrimaryNetworkError,
    network::{PrimaryResponse, RequestHandler},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{open_db, DatabaseType, VoteDigestStore as _};
use rayls_infrastructure_types::{
    error::HeaderError, BlockHeader, BlockNumHash, BlsPublicKey, Database as _, ExecHeader,
    Hash as _, Header, SealedHeader, TaskManager, Vote,
};
use std::{path::Path, time::Duration};

/// Tells the child process which directory holds the voter's store.
const DIR_ENV: &str = "VOTE_DURABILITY_CHILD_DIR";
/// How long one vote may take once the database can write.
const VOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// The voter (first authority) on a real MDBX store, plus two conflicting headers.
struct Node {
    handler: RequestHandler<DatabaseType>,
    db: DatabaseType,
    h1: Header,
    h2: Header,
    peer: BlsPublicKey,
    _task_manager: TaskManager,
}

/// Builds the same committee in every process and opens the voter's store at `dir`.
fn node(dir: &Path) -> Node {
    let fixture = committee();
    let voter = fixture.first_authority().consensus_config();
    // The production open path: MDBX, the layered writer and every default table.
    let db = open_db(dir);
    let config = ConsensusConfig::new_with_committee_for_test(
        voter.config().clone(),
        db.clone(),
        voter.key_config().clone(),
        voter.committee().clone(),
        voter.network_config().clone(),
    )
    .expect("consensus config");

    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(config.clone(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let parent = SealedHeader::seal_slow(ExecHeader::default());
    cb.recently_executed_blocks().send_modify(|blocks| blocks.push_latest(parent.clone()));
    let handler = RequestHandler::new(config, cb, synchronizer);

    // Two headers from the same author for the same round.
    // They differ only in their timestamp.
    let exec = BlockNumHash::new(parent.number(), parent.hash());
    let h1 = fixture.header_builder_last_authority().latest_execution_block(exec).build();
    let h2 = fixture
        .header_builder_last_authority()
        .latest_execution_block(exec)
        .created_at(h1.created_at() + 1)
        .build();
    assert_eq!((h1.author(), h1.round()), (h2.author(), h2.round()));
    assert_ne!(h1.digest(), h2.digest());
    let peer = *fixture.last_authority().authority().protocol_key();
    Node { handler, db, h1, h2, peer, _task_manager: task_manager }
}

/// Asks the handler to vote for `header`.
async fn vote(node: &Node, header: &Header) -> Result<Vote, PrimaryNetworkError> {
    match node.handler.vote(node.peer, header.clone(), Vec::new()).await? {
        PrimaryResponse::Vote(v) => Ok(v),
        other => panic!("unexpected response {other:?}"),
    }
}

/// Child side of `voter_refuses_conflicting_header_after_crash`.
/// It does nothing unless that test started it.
#[tokio::test]
async fn vote_durability_child() {
    let Some(dir) = child_dir(DIR_ENV) else {
        return;
    };
    let node = node(&dir);
    // Let the writer finish its startup work so it is idle.
    node.db.persist().await.expect("startup persist");
    let gate = WriteGate::close(&node.db);
    report("READY");

    // A node that saves the vote before replying blocks until the gate opens.
    let pending = vote(&node, &node.h1);
    tokio::pin!(pending);
    let res = match tokio::time::timeout(GATE_HOLD, &mut pending).await {
        Ok(res) => res,
        Err(_) => {
            gate.open();
            tokio::time::timeout(VOTE_TIMEOUT, pending).await.expect("vote after the gate opened")
        }
    };

    // Stand-in for the network reply: the parent learns of the vote from this line.
    // The vote is read straight from MDBX to see whether it reached disk before the reply.
    match res {
        Ok(v) => {
            let on_disk = node
                .db
                .inner()
                .read_vote_info(node.h1.author())
                .expect("mdbx read")
                .is_some_and(|info| info.vote_digest() == v.digest());
            report(&format!("VOTED {} {} on_disk={on_disk}", v.round(), v.header_digest()));
        }
        Err(e) => report(&format!("REFUSED {e}")),
    }
    wait_for_kill();
}

/// Once the node has replied with a vote for H1, it must refuse H2 after a crash and restart.
/// H2 has the same author and round as H1 but a different digest.
#[tokio::test]
async fn voter_refuses_conflicting_header_after_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let child = Child::spawn(test_name!(vote_durability_child), DIR_ENV, dir.path());
    let ready = child.wait_for(&["READY"], CHILD_START_TIMEOUT);
    assert!(ready.is_some(), "child did not start: {}", child.stderr());
    let reply = child.wait_for(&["VOTED ", "REFUSED "], GATE_HOLD + VOTE_TIMEOUT);
    let stderr = child.stderr();
    child.kill();
    let reply = reply.unwrap_or_else(|| panic!("child never replied: {stderr}"));

    // Restart on the same directory.
    let node = node(dir.path());
    let res = tokio::time::timeout(VOTE_TIMEOUT, vote(&node, &node.h2))
        .await
        .expect("restarted vote handler hung");

    // H1 is a valid header, so refusing it means the setup is broken.
    let expected = format!("VOTED {} {} ", node.h1.round(), node.h1.digest());
    assert!(reply.starts_with(&expected), "child did not vote for H1: {reply}");
    assert!(
        matches!(&res, Err(PrimaryNetworkError::InvalidHeader(HeaderError::AlreadyVoted(d, r)))
            if *d == node.h2.digest() && *r == node.h2.round()),
        "node replied with a vote for H1 {} before the crash, then voted for H2 after restart: \
         {res:?}",
        node.h1.digest(),
    );
    assert_eq!(
        field(&reply, "on_disk"),
        Some("true"),
        "node replied with a vote that was not on disk: {reply}"
    );
}
