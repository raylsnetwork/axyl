//! Test-only Byzantine override for consensus-header requests (finding N1).
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets a
//! node answer a follower's consensus-header request with a crafted header, armed per process by
//! environment, so a single node in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_GAP_TARGET=<N> or <A-B>   block number, or inclusive range, to poison (arms the hook)
//!   RL_BYZ_GAP_MODE=rep-poison       serve the real header for the number with its sub-dag
//!                                    reputation_score replaced by an empty, final-of-schedule
//!                                    score map
//!
//! reputation_score is excluded from the consensus block digest on purpose (see output.rs, "skip
//! reputation for stable hashes"), so the reply keeps the honest digest and parent link and passes
//! every hash and parent check a follower makes. The override keys on the real block's number and
//! applies to every serving route (by number, by hash, and the latest probe), so no honest reply
//! for an in-range number can overwrite the poison at the same block number. The honest and
//! poisoned headers share a digest, so a by-hash reply would otherwise supersede a by-number one.
//!
//! A follower that caches the poison promotes it to its canonical ConsensusBlocks table with the
//! empty scores. On its next start the follower rebuilds its leader schedule from the newest stored
//! final-of-schedule row (LeaderSchedule::from_store), and LeaderSwapTable::new divides by the
//! number of scored authorities, which is zero, and panics. The row persists, so every restart
//! repeats the panic.

use rayls_infrastructure_types::{ConsensusHeader, ReputationScores};
use tracing::warn;

/// Returns a crafted header to serve back for any consensus-header request, or None to serve the
/// honest header. `honest` is the real header read from local storage, whatever route asked for it.
/// The decision is made on the honest header's own block number against the armed range.
pub(super) fn override_header(honest: &ConsensusHeader) -> Option<ConsensusHeader> {
    let spec = std::env::var("RL_BYZ_GAP_TARGET").ok()?;
    let (lo, hi) = match spec.split_once('-') {
        Some((a, b)) => (a.trim().parse().ok()?, b.trim().parse().ok()?),
        None => {
            let n: u64 = spec.trim().parse().ok()?;
            (n, n)
        }
    };
    let number = honest.number;
    if number < lo || number > hi {
        return None;
    }
    let mode = std::env::var("RL_BYZ_GAP_MODE").unwrap_or_default();
    if mode != "rep-poison" {
        warn!(target: "primary::byzantine", mode, number, "unknown RL_BYZ_GAP_MODE, serving honest header");
        return None;
    }
    Some(rep_poison_header(number, honest))
}

/// Keep the honest header byte for byte, only replace the sub-dag reputation scores with an empty
/// final-of-schedule map. Scores are not part of the header digest, so the digest is unchanged and
/// the reply links exactly like the honest block.
fn rep_poison_header(number: u64, honest: &ConsensusHeader) -> ConsensusHeader {
    let mut poison_scores = ReputationScores::default();
    poison_scores.final_of_schedule = true;
    let mut sub_dag = honest.sub_dag.clone();
    sub_dag.reputation_score = poison_scores;
    let poison = ConsensusHeader {
        parent_hash: honest.parent_hash,
        sub_dag,
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        poison_digest = ?poison.digest(),
        "BYZANTINE GAP-FETCH: serving rep-poison header (empty final-of-schedule reputation scores)"
    );
    poison
}
