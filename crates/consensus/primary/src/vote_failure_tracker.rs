//! Tracks peer vote rejections to detect when this node should transition
//! to CvvInactive.
//!
//! Rejections are counted per distinct peer so a single peer rejecting
//! repeatedly cannot demote the honest majority. Epoch-mismatch rejections
//! only count when the peer is at a newer epoch than ours (otherwise the
//! peer itself is stale and its rejection carries no information about our
//! liveness).

use rayls_infrastructure_types::{AuthorityIdentifier, Epoch};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Outcome of recording a vote rejection.
pub(crate) enum RejectionOutcome {
    /// Below threshold - keep trying.
    BelowThreshold,
    /// Threshold reached - transition to CvvInactive.
    TransitionToInactive,
}

/// Shared state for tracking peer rejections across `Certifier` clones.
struct Inner {
    too_old: BTreeSet<AuthorityIdentifier>,
    epoch_mismatch: BTreeSet<AuthorityIdentifier>,
    /// When the current run of rejections began: the first rejection recorded since the last
    /// certified header. `None` while the node is certifying normally. Lets
    /// `skip_with_rejection_grace` tell a transient lag (the node certifies again within
    /// seconds, which clears this) from a node that has stopped contributing.
    rejected_since: Option<Instant>,
}

/// Peer rejection tracker - counts distinct rejecting peers.
#[derive(Clone)]
pub(crate) struct VoteFailureTracker {
    inner: Arc<Mutex<Inner>>,
    threshold: usize,
}

impl VoteFailureTracker {
    /// Create a tracker. Rejections are skipped for `REJECTION_GRACE` after the first one that
    /// follows a certification (see `skip_with_rejection_grace`), which also covers the start of
    /// an epoch; there is no separate startup grace.
    pub(crate) fn new(committee_size: usize) -> Self {
        // n/2: f+1 distinct peers for a 3f+1 committee (e.g. 2 of 4).
        Self {
            inner: Arc::new(Mutex::new(Inner {
                too_old: BTreeSet::new(),
                epoch_mismatch: BTreeSet::new(),
                rejected_since: None,
            })),
            threshold: committee_size / 2,
        }
    }

    /// Decide whether to SKIP a rejection (not count it toward demotion).
    ///
    /// A rejection is skipped while the node has been rejected for less than `grace`, counted
    /// from the first rejection after the last certified header; certifying again
    /// (`clear_counters`) resets it. A transient lag, a lost gossip
    /// certificate being fetched or a boundary the node is about to cross, lasts seconds and
    /// ends with a certification, so it never outlasts the grace. A node that has stopped
    /// contributing, wedged or limping behind the committee, never certifies, so its rejections
    /// count once the grace is over and it demotes at the usual threshold.
    ///
    /// No peer rounds and no local progress are consulted: a node that is rejected for the whole
    /// grace without certifying is out of consensus whatever its rounds are doing.
    pub(crate) fn skip_with_rejection_grace(&self, grace: Duration) -> bool {
        let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
        let since = *guard.rejected_since.get_or_insert_with(Instant::now);
        since.elapsed() < grace
    }

    /// Seconds the node has been rejected without certifying, for logs. Zero when certifying.
    pub(crate) fn rejected_for_secs(&self) -> u64 {
        let guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
        guard.rejected_since.map_or(0, |since| since.elapsed().as_secs())
    }

    /// Clear counters after successful certification.
    pub(crate) fn clear_counters(&self) {
        let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
        guard.too_old.clear();
        guard.epoch_mismatch.clear();
        guard.rejected_since = None;
    }

    /// Record a `TooOld` rejection from the given peer.
    pub(crate) fn record_too_old(&self, peer: AuthorityIdentifier) -> RejectionOutcome {
        let count = {
            let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
            guard.too_old.insert(peer);
            guard.too_old.len()
        };
        self.evaluate(count)
    }

    /// Record an epoch-mismatch rejection from a peer that is at our epoch
    /// or a newer one. Rejections from stale peers never reach here: the
    /// certifier keeps asking a peer behind us instead.
    pub(crate) fn record_epoch_mismatch(&self, peer: AuthorityIdentifier) -> RejectionOutcome {
        let count = {
            let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
            guard.epoch_mismatch.insert(peer);
            guard.epoch_mismatch.len()
        };
        self.evaluate(count)
    }

    /// Decide whether an epoch-mismatch rejection should count. A peer at
    /// an older epoch than ours is itself stale - its rejection is not
    /// evidence that we are behind.
    pub(crate) fn should_count_epoch_rejection(peer_epoch: Epoch, our_epoch: Epoch) -> bool {
        peer_epoch >= our_epoch
    }

    /// True when we are exactly one epoch behind the peer: we are at the epoch boundary and our
    /// own epoch manager will transition us as soon as the boundary output commits. A rejection
    /// in that state is gated on the rejection grace like a too-old one (see
    /// `skip_with_rejection_grace`).
    /// Two or more epochs behind means we are genuinely far behind, and the rejection counts
    /// immediately.
    pub(crate) fn is_one_epoch_behind(peer_epoch: Epoch, our_epoch: Epoch) -> bool {
        peer_epoch.checked_sub(our_epoch) == Some(1)
    }

    pub(crate) fn too_old_count(&self) -> usize {
        self.inner.lock().expect("vote failure tracker mutex poisoned").too_old.len()
    }

    pub(crate) fn epoch_mismatch_count(&self) -> usize {
        self.inner.lock().expect("vote failure tracker mutex poisoned").epoch_mismatch.len()
    }

    pub(crate) fn threshold(&self) -> usize {
        self.threshold
    }

    fn evaluate(&self, count: usize) -> RejectionOutcome {
        if count < self.threshold {
            RejectionOutcome::BelowThreshold
        } else {
            RejectionOutcome::TransitionToInactive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> AuthorityIdentifier {
        AuthorityIdentifier::dummy_for_test(byte)
    }

    #[test]
    fn same_peer_rejecting_repeatedly_does_not_demote() {
        let tracker = VoteFailureTracker::new(4);
        // threshold = 4/2 = 2

        let peer_a = peer(1);
        let first = tracker.record_epoch_mismatch(peer_a.clone());
        assert!(matches!(first, RejectionOutcome::BelowThreshold));
        let second = tracker.record_epoch_mismatch(peer_a.clone());
        assert!(matches!(second, RejectionOutcome::BelowThreshold));
        let third = tracker.record_epoch_mismatch(peer_a);
        assert!(matches!(third, RejectionOutcome::BelowThreshold));
        assert_eq!(tracker.epoch_mismatch_count(), 1);
    }

    #[test]
    fn distinct_peers_at_threshold_demote() {
        let tracker = VoteFailureTracker::new(4);
        let first = tracker.record_epoch_mismatch(peer(1));
        assert!(matches!(first, RejectionOutcome::BelowThreshold));
        let second = tracker.record_epoch_mismatch(peer(2));
        assert!(matches!(second, RejectionOutcome::TransitionToInactive));
    }

    #[test]
    fn stale_peer_rejection_is_ignored() {
        // peer at epoch 10 should not count against a node at epoch 15.
        assert!(!VoteFailureTracker::should_count_epoch_rejection(10, 15));
        // peer at same epoch should count (normal case).
        assert!(VoteFailureTracker::should_count_epoch_rejection(15, 15));
        // peer at newer epoch should count (we may be the stale one).
        assert!(VoteFailureTracker::should_count_epoch_rejection(20, 15));
    }

    #[test]
    fn one_epoch_behind_is_exactly_one() {
        // peer just closed the epoch we are still finishing
        assert!(VoteFailureTracker::is_one_epoch_behind(771, 770));
        // same epoch (a wrong-epoch rejection from a peer at our epoch): not behind
        assert!(!VoteFailureTracker::is_one_epoch_behind(770, 770));
        // two or more epochs: genuinely behind, counts immediately
        assert!(!VoteFailureTracker::is_one_epoch_behind(772, 770));
        // peer behind us: filtered earlier as stale anyway
        assert!(!VoteFailureTracker::is_one_epoch_behind(769, 770));
    }

    #[test]
    fn too_old_uses_distinct_peer_tracking() {
        let tracker = VoteFailureTracker::new(4);
        let first = tracker.record_too_old(peer(1));
        assert!(matches!(first, RejectionOutcome::BelowThreshold));
        let repeat = tracker.record_too_old(peer(1));
        assert!(matches!(repeat, RejectionOutcome::BelowThreshold));
        let distinct = tracker.record_too_old(peer(2));
        assert!(matches!(distinct, RejectionOutcome::TransitionToInactive));
    }

    #[test]
    fn clear_counters_resets_peer_sets() {
        let tracker = VoteFailureTracker::new(4);
        tracker.record_epoch_mismatch(peer(1));
        tracker.record_too_old(peer(2));
        tracker.clear_counters();
        assert_eq!(tracker.epoch_mismatch_count(), 0);
        assert_eq!(tracker.too_old_count(), 0);
    }

    #[test]
    fn first_rejection_starts_the_grace_and_is_skipped() {
        let tracker = VoteFailureTracker::new(4);
        assert_eq!(tracker.rejected_for_secs(), 0);
        // the first rejection after a certification starts the grace and is always skipped
        assert!(tracker.skip_with_rejection_grace(std::time::Duration::from_secs(30)));
        // so is every rejection while the grace is not over
        assert!(tracker.skip_with_rejection_grace(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn rejections_count_once_the_grace_is_over() {
        let tracker = VoteFailureTracker::new(4);
        let grace = std::time::Duration::from_millis(5);
        assert!(tracker.skip_with_rejection_grace(grace));
        std::thread::sleep(grace * 2);
        // the grace is over: stop skipping, and keep not skipping
        assert!(!tracker.skip_with_rejection_grace(grace));
        assert!(!tracker.skip_with_rejection_grace(grace));
    }

    #[test]
    fn certification_resets_the_grace() {
        let tracker = VoteFailureTracker::new(4);
        let grace = std::time::Duration::from_millis(5);
        assert!(tracker.skip_with_rejection_grace(grace));
        std::thread::sleep(grace * 2);
        assert!(!tracker.skip_with_rejection_grace(grace));
        // a certified header clears the counters and the grace: the next rejection starts over
        tracker.clear_counters();
        assert_eq!(tracker.rejected_for_secs(), 0);
        assert!(tracker.skip_with_rejection_grace(grace));
    }

    #[test]
    fn wedged_node_still_demotes_at_threshold() {
        let tracker = VoteFailureTracker::new(4);
        // a zero grace never skips: every rejection counts
        assert!(!tracker.skip_with_rejection_grace(std::time::Duration::ZERO));
        // the caller now records instead of skipping: threshold still works as before
        assert!(matches!(tracker.record_too_old(peer(1)), RejectionOutcome::BelowThreshold));
        assert!(matches!(tracker.record_too_old(peer(2)), RejectionOutcome::TransitionToInactive));
    }
}
