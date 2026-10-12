//! Test-only Byzantine override for by-number consensus-header gap fetches.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets a
//! node answer a follower's by-number consensus-header request with a crafted header, which is the
//! reproduction lever for the gap-fetch-stall issue. It is armed per process by environment, so a
//! single node in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_GAP_TARGET=<N> or <A-B>  block number, or inclusive range, to lie about (arms the hook)
//!   RL_BYZ_GAP_MODE=fork            serve the real header for the number with its commit timestamp
//!                                   raised, keeping the real quorum-signed leader and certificates
//!                                   so the follower's signature re-verify still passes while the
//!                                   header digest and the executed block diverge (default)
//!   RL_BYZ_GAP_MODE=stall           serve a fabricated header with the real parent link and an
//!                                   unsigned sub-dag, to stall the follower's streamer anchor
//!   RL_BYZ_GAP_BUMP=<secs>          commit-timestamp increase for fork mode (default 1000000)

use rayls_infrastructure_types::{
    Certificate, CommittedSubDag, ConsensusHeader, Hash as _, ReputationScores,
};
use tracing::warn;

/// Returns a crafted header to serve back for a by-number gap fetch of `number`, or None to serve
/// the honest stored header. `honest` is the real header for `number` read from local storage.
pub(super) fn override_gap_fetch(number: u64, honest: &ConsensusHeader) -> Option<ConsensusHeader> {
    let spec = std::env::var("RL_BYZ_GAP_TARGET").ok()?;
    let (lo, hi) = match spec.split_once('-') {
        Some((a, b)) => (a.trim().parse().ok()?, b.trim().parse().ok()?),
        None => {
            let n: u64 = spec.trim().parse().ok()?;
            (n, n)
        }
    };
    if number < lo || number > hi {
        return None;
    }
    let mode = std::env::var("RL_BYZ_GAP_MODE").unwrap_or_else(|_| "fork".to_string());
    match mode.as_str() {
        "fork" => Some(fork_header(number, honest)),
        "stall" => Some(stall_header(number, honest)),
        other => {
            warn!(target: "primary::byzantine", mode = other, number, "unknown RL_BYZ_GAP_MODE, serving honest header");
            None
        }
    }
}

/// Keep the real quorum-signed leader and certificates, only raise the commit timestamp. The
/// commit timestamp is hashed into the header digest but is not covered by any committee signature,
/// so the result still passes the follower's signature re-verify yet executes to a different block.
fn fork_header(number: u64, honest: &ConsensusHeader) -> ConsensusHeader {
    let bump: u64 =
        std::env::var("RL_BYZ_GAP_BUMP").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
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
    let tampered = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: tampered_sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        tampered_digest = ?tampered.digest(),
        "BYZANTINE GAP-FETCH: serving fork header (real leader, bumped commit timestamp)"
    );
    tampered
}

/// Fabricate a header with the real parent link for the previous block and an unsigned sub-dag.
fn stall_header(number: u64, honest: &ConsensusHeader) -> ConsensusHeader {
    let mut fake_leader = Certificate::default();
    fake_leader.header.created_at = honest.sub_dag.commit_timestamp();
    let fake_sub_dag =
        CommittedSubDag::new(vec![], fake_leader, 0, ReputationScores::default(), None);
    let fake = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag: fake_sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        fake_digest = ?fake.digest(),
        "BYZANTINE GAP-FETCH: serving stall header (fabricated sub-dag, real parent link)"
    );
    fake
}
