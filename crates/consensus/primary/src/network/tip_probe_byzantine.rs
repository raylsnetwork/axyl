//! Test-only Byzantine override for the latest-consensus-header probe.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets a
//! committee member answer a follower's tip probe with a fake header, which is the reproduction
//! lever for the tip-probe-number issue. It is armed per process by environment, so a single node
//! in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_FAKE_TIP=1   answer a request with no number and no hash with the fake header, and a
//!                       request by hash for the fake header's digest with the same header
//!
//! It also carries the fork variant on the same probe path, more severe than the strand above: a
//! committee member answers the probe with a copy of its own real latest block with the commit
//! timestamp raised. The commit timestamp is hashed into the header digest but not covered by any
//! committee signature, so the copy still passes the follower's signature re-verify yet has a
//! different digest and executes to a different block. The follower takes it as its catch-up
//! target, executes it, and forks. Armed per process by environment:
//!
//!   RL_BYZ_FORK_TIP=1        answer the probe (no number, no hash), and a by-hash request for the
//!                            forged copy's own digest, with the forged copy
//!   RL_BYZ_FORK_NUMBER=<N>   the block number to wrap: this node's first real block at or above N
//!                            that carries batches (so its bumped timestamp executes to a distinct
//!                            EVM block) is copied and tampered
//!   RL_BYZ_FORK_BUMP=<secs>  commit-timestamp increase (default 1000000)

use rayls_infrastructure_storage::ConsensusStore;
use rayls_infrastructure_types::{
    BlockHash, Certificate, CommittedSubDag, ConsensusHeader, Database, Epoch, ReputationScores,
    B256,
};
use std::sync::{LazyLock, OnceLock};
use tracing::warn;

/// The fake tip: the highest block number there is, and an unsigned leader claiming an epoch no
/// committee exists for. Built once so its digest is stable across requests.
static FAKE_TIP: LazyLock<ConsensusHeader> = LazyLock::new(|| {
    let mut leader = Certificate::default();
    leader.header.epoch = Epoch::MAX;
    let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
    ConsensusHeader { parent_hash: B256::repeat_byte(0xAB), sub_dag, number: u64::MAX, extra: B256::ZERO }
});

/// Returns the fake header to serve for a latest-header request, or for a by-hash request for the
/// fake header itself, or None to serve the honest reply.
pub(super) fn override_tip_probe(
    number: Option<u64>,
    hash: Option<B256>,
) -> Option<ConsensusHeader> {
    if std::env::var("RL_BYZ_FAKE_TIP").ok().as_deref() != Some("1") {
        return None;
    }
    let fake = &*FAKE_TIP;
    let kind = match (number, hash) {
        (None, None) => "latest",
        (_, Some(h)) if h == fake.digest() => "by-hash",
        _ => return None,
    };
    warn!(
        target: "primary::byzantine",
        kind,
        fake_number = fake.number,
        fake_digest = ?fake.digest(),
        "BYZANTINE TIP-PROBE: serving fake consensus header"
    );
    Some(fake.clone())
}

/// How many blocks from `RL_BYZ_FORK_NUMBER` upwards to search for a block that carries batches.
const FORK_SEARCH_BLOCKS: u64 = 64;

/// True when this node is armed for the fork-probe route.
fn fork_route_armed() -> bool {
    std::env::var("RL_BYZ_FORK_TIP").ok().as_deref() == Some("1")
}

/// Keep the real quorum-signed leader and certificates, only raise the commit timestamp. The commit
/// timestamp is hashed into the header digest but is not covered by any committee signature, so the
/// result still passes the follower's signature re-verify yet executes to a different block.
fn fork_header(honest: &ConsensusHeader) -> ConsensusHeader {
    let bump: u64 =
        std::env::var("RL_BYZ_FORK_BUMP").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    // The commit timestamp is max(previous sub-dag timestamp, leader created_at). Raise it by
    // building the sub-dag with a throwaway previous sub-dag whose timestamp is higher.
    let mut higher_leader = Certificate::default();
    higher_leader.header.created_at = honest.sub_dag.commit_timestamp() + bump;
    let higher = CommittedSubDag::new(vec![], higher_leader, 0, ReputationScores::default(), None);
    // sub_dag_index is not stored on CommittedSubDag and is not part of its digest, so pass zero.
    let tampered_sub_dag = CommittedSubDag::new(
        honest.sub_dag.certificates.clone(),
        honest.sub_dag.leader.clone(),
        0,
        honest.sub_dag.reputation_score.clone(),
        Some(&higher),
    );
    ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: tampered_sub_dag,
        number: honest.number,
        extra: honest.extra,
    }
}

/// Build the forged copy once and keep it: this node's first real block at or above
/// `RL_BYZ_FORK_NUMBER` that carries batches, with the commit timestamp raised. Served on a
/// follower's "latest" probe and on a by-hash request for its own digest.
fn fork_poison<DB: Database>(db: &DB) -> Option<&'static ConsensusHeader> {
    static POISON: OnceLock<ConsensusHeader> = OnceLock::new();
    if let Some(poison) = POISON.get() {
        return Some(poison);
    }
    let first: u64 = std::env::var("RL_BYZ_FORK_NUMBER").ok()?.parse().ok()?;
    let Some(honest) = (first..first + FORK_SEARCH_BLOCKS)
        .map_while(|number| db.get_consensus_by_number(number))
        .find(|block| block.sub_dag.num_primary_blocks() > 0)
    else {
        warn!(
            target: "primary::byzantine",
            first,
            searched = FORK_SEARCH_BLOCKS,
            "BYZANTINE FORK-TIP: no block carrying batches to wrap"
        );
        return None;
    };
    let crafted = fork_header(&honest);
    let _ = POISON.set(crafted);
    let poison = POISON.get()?;
    warn!(
        target: "primary::byzantine",
        number = poison.number,
        honest_digest = ?honest.digest(),
        poison_digest = ?poison.digest(),
        "BYZANTINE FORK-TIP: built forged copy (served on latest-probe and by-hash)"
    );
    Some(poison)
}

/// Returns the forged copy to serve for a follower's "latest" probe (number and hash both absent),
/// or None to serve the honest latest. Armed by `RL_BYZ_FORK_TIP`.
pub(super) fn override_latest_fork<DB: Database>(db: &DB) -> Option<ConsensusHeader> {
    if !fork_route_armed() {
        return None;
    }
    let poison = fork_poison(db)?;
    warn!(
        target: "primary::byzantine",
        number = poison.number,
        "BYZANTINE FORK-TIP: serving forged copy on the latest probe"
    );
    Some(poison.clone())
}

/// Returns the forged copy to serve for a by-hash request whose hash is the forged copy's own
/// digest, or None to serve the honest header for that hash. Armed by `RL_BYZ_FORK_TIP`.
pub(super) fn override_by_hash_fork<DB: Database>(
    db: &DB,
    hash: BlockHash,
) -> Option<ConsensusHeader> {
    if !fork_route_armed() {
        return None;
    }
    let poison = fork_poison(db)?;
    if poison.digest() != hash {
        return None;
    }
    warn!(
        target: "primary::byzantine",
        number = poison.number,
        "BYZANTINE FORK-TIP: serving forged copy on a by-hash request"
    );
    Some(poison.clone())
}
