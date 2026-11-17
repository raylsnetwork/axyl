//! Test-only Byzantine override for by-number consensus-header requests.
//!
//! Compiled only with the `byzantine-test-hooks` feature, never in a production build. It lets one
//! node answer a follower's by-number consensus-header request with a header that does not link to
//! the real header below it. That sends the follower's forward streamer into its mismatch branch,
//! which deletes the row below from the consensus cache together with its by-hash index entry.
//! Armed per process by environment, so one node in a test network lies while the rest stay honest:
//!
//!   RL_BYZ_UNLINK_TARGET=<N> or <A-B>   consensus number, or inclusive range, to lie about
//!
//! The reply is the real header for the number with its `parent_hash` replaced, so its number is
//! right (the follower's by-number fetch accepts it) and its digest differs from the one the next
//! real header links to.

use rayls_infrastructure_types::{ConsensusHeader, B256};
use tracing::warn;

/// Returns a header to serve back for a by-number request of `number`, or None to serve the honest
/// stored header. `honest` is the real header for `number` read from local storage.
pub(super) fn override_by_number(number: u64, honest: &ConsensusHeader) -> Option<ConsensusHeader> {
    let spec = std::env::var("RL_BYZ_UNLINK_TARGET").ok()?;
    let (lo, hi): (u64, u64) = match spec.split_once('-') {
        Some((a, b)) => (a.trim().parse().ok()?, b.trim().parse().ok()?),
        None => {
            let n = spec.trim().parse().ok()?;
            (n, n)
        }
    };
    if number < lo || number > hi {
        return None;
    }
    let unlinked = ConsensusHeader {
        parent_hash: B256::repeat_byte(0xBD),
        sub_dag: honest.sub_dag.clone(),
        number: honest.number,
        extra: honest.extra,
    };
    warn!(
        target: "primary::byzantine",
        number,
        honest_digest = ?honest.digest(),
        unlinked_digest = ?unlinked.digest(),
        "BYZANTINE BY-NUMBER: serving a header that does not link to the real one below"
    );
    Some(unlinked)
}
