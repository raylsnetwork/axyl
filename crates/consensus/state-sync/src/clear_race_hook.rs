//! Test-only hooks around the forward streamer's mismatch delete branch.
//!
//! Compiled only for unit tests and with the `fault-injection-test-hooks` feature, never in a
//! production build. When a header does not link to the row below it, the streamer checks that the
//! row below is not a saved (canonical) block, then deletes it and its by-hash index entry in a
//! separate write. [`in_window`] runs between the two:
//!
//! - in unit tests it runs a one-shot callback registered with [`set_in_window`], which saves the
//!   row the way the subscriber does;
//! - with the feature it sleeps `RL_FAULT_CLEAR_RACE_DELAY_MS` (default 0), so on a live network
//!   the subscriber's save of the row it was just sent can land in the window. The sleep only
//!   widens the window; the check and the delete are unchanged.
//!
//! The feature also adds two switches that steer the streamer onto this branch on a live network
//! without changing what the branch does:
//!
//!   RL_FAULT_GAP_FETCH=odd          the first time the streamer reaches an odd number, ignore the
//!                                   stored row and fetch it by number, as when the streamer
//!                                   outruns the backwards walk; later passes read it as usual
//!   RL_FAULT_GAP_FETCH=after:<K>    ignore stored rows K or more above the first number of each
//!                                   pass, so each pass streams K stored rows and then fetches
//!   RL_FAULT_GAP_FETCH_PEER=<key>   send every by-number fetch to the committee peer with this BLS
//!                                   public key (base58, as in node-info.yaml), not to any peer

use rayls_infrastructure_types::B256;
#[cfg(feature = "fault-injection-test-hooks")]
use tracing::warn;

#[cfg(test)]
thread_local! {
    static IN_WINDOW: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

/// Register a callback that runs once, in the next delete window on this thread.
#[cfg(test)]
pub(crate) fn set_in_window(callback: impl FnOnce() + 'static) {
    IN_WINDOW.with(|cell| *cell.borrow_mut() = Some(Box::new(callback)));
}

/// Runs after the streamer found that row `number` (digest `digest`) is not canonical, and before
/// it deletes that row and its by-hash index entry.
pub(crate) async fn in_window(number: u64, digest: B256) {
    #[cfg(test)]
    if let Some(callback) = IN_WINDOW.with(|cell| cell.borrow_mut().take()) {
        callback();
    }
    #[cfg(feature = "fault-injection-test-hooks")]
    {
        let delay_ms: u64 = std::env::var("RL_FAULT_CLEAR_RACE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        warn!(
            target: "rayls-consensus-state-sync",
            number,
            ?digest,
            delay_ms,
            "FAULT HOOK: mismatch branch checked row {number} is not canonical, deleting it after {delay_ms} ms"
        );
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }
    #[cfg(not(feature = "fault-injection-test-hooks"))]
    let _ = (number, digest);
}

/// Report what the delete left behind: a canonical row at `number` with digest `digest` and no
/// by-hash index entry is the lost-index state this race produces.
#[cfg(feature = "fault-injection-test-hooks")]
pub(crate) fn after_clear<DB: rayls_infrastructure_types::Database>(
    db: &DB,
    number: u64,
    digest: B256,
) {
    use rayls_infrastructure_storage::tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks};
    let canonical =
        db.get::<ConsensusBlocks>(&number).ok().flatten().map(|h| h.digest()) == Some(digest);
    let indexed = db.get::<ConsensusBlockNumbersByDigest>(&digest).ok().flatten().is_some();
    if canonical && !indexed {
        warn!(
            target: "rayls-consensus-state-sync",
            number,
            ?digest,
            "RACE HIT: canonical block {number} was saved inside the delete window and lost its by-hash index entry"
        );
    } else {
        warn!(
            target: "rayls-consensus-state-sync",
            number,
            canonical,
            indexed,
            "FAULT HOOK: mismatch branch deleted row {number}; no canonical block lost its index entry"
        );
    }
}

/// Whether the streamer should ignore the stored row at `number` and fetch it by number instead.
/// `first` is the first number of the current pass.
#[cfg(feature = "fault-injection-test-hooks")]
pub(crate) fn skip_stored_row(number: u64, first: u64) -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    // The highest odd number skipped so far: each odd number is skipped once.
    static ODD_SKIPPED: AtomicU64 = AtomicU64::new(0);
    match std::env::var("RL_FAULT_GAP_FETCH").as_deref() {
        Ok("odd") => number % 2 == 1 && ODD_SKIPPED.fetch_max(number, Ordering::Relaxed) < number,
        Ok(spec) => spec
            .strip_prefix("after:")
            .and_then(|k| k.parse::<u64>().ok())
            .is_some_and(|k| number >= first.saturating_add(k)),
        _ => false,
    }
}

/// Fetch consensus header `number` by number, from the one peer named by `RL_FAULT_GAP_FETCH_PEER`
/// when it is set and in the committee, or from any peer otherwise.
#[cfg(feature = "fault-injection-test-hooks")]
pub(crate) async fn request_by_number<DB: rayls_infrastructure_types::Database>(
    config: &rayls_infrastructure_config::ConsensusConfig<DB>,
    network: &rayls_consensus_primary::network::PrimaryNetworkHandle,
    number: u64,
) -> Result<
    rayls_infrastructure_types::ConsensusHeader,
    rayls_consensus_primary::network::NetworkError,
> {
    if let Ok(wanted) = std::env::var("RL_FAULT_GAP_FETCH_PEER") {
        let wanted = wanted.trim();
        let peers = config.committee().others_primaries_by_id(config.authority_id().as_ref());
        if let Some((_, peer)) = peers.iter().find(|(_, peer)| peer.to_string() == wanted) {
            return network.request_consensus_from_peer(*peer, Some(number), None).await;
        }
        let known: Vec<String> = peers.iter().map(|(_, peer)| peer.to_string()).collect();
        warn!(
            target: "rayls-consensus-state-sync",
            wanted,
            ?known,
            "FAULT HOOK: RL_FAULT_GAP_FETCH_PEER is not a committee peer, fetching from any peer"
        );
    }
    network.request_consensus(Some(number), None).await
}
