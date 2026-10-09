//! Tracks peer vote rejections to detect when this node should transition
//! to CvvInactive.
//!
//! Rejections are counted per distinct peer so a single peer rejecting
//! repeatedly cannot demote the honest majority. Epoch-mismatch rejections
//! only count when the peer is at a newer epoch than ours (otherwise the
//! peer itself is stale and its rejection carries no information about our
//! liveness).

use rayls_infrastructure_types::{AuthorityIdentifier, Epoch, Round};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Outcome of recording a vote rejection.
pub(crate) enum RejectionOutcome {
    /// Below threshold - keep trying.
    BelowThreshold,
    /// Threshold reached but grace period still active.
    GracePeriod,
    /// Threshold reached and grace expired - transition to CvvInactive.
    TransitionToInactive,
}

/// Shared state for tracking peer rejections across `Certifier` clones.
struct Inner {
    too_old: BTreeSet<AuthorityIdentifier>,
    epoch_mismatch: BTreeSet<AuthorityIdentifier>,
    /// Highest cert-store and committed rounds seen while evaluating a too-old rejection, and
    /// when either last advanced. Lets `skip_while_progressing` tell a transient lag (certs
    /// still arriving, DAG still committing) from a genuine wedge (nothing moves).
    last_cert_store_round: Round,
    last_committed_round: Round,
    last_progress_at: Instant,
}

/// Peer rejection tracker - counts distinct rejecting peers.
#[derive(Clone)]
pub(crate) struct VoteFailureTracker {
    inner: Arc<Mutex<Inner>>,
    threshold: usize,
    grace_deadline: Option<Instant>,
}

impl VoteFailureTracker {
    /// Create a tracker. `None` grace skips the grace period.
    pub(crate) fn new(committee_size: usize, grace_deadline: Option<Instant>) -> Self {
        // n/2: f+1 distinct peers for a 3f+1 committee (e.g. 2 of 4).
        Self {
            inner: Arc::new(Mutex::new(Inner {
                too_old: BTreeSet::new(),
                epoch_mismatch: BTreeSet::new(),
                last_cert_store_round: Round::default(),
                last_committed_round: Round::default(),
                // Deliberate: the first too-old rejection after construction is skipped for a
                // full wedge window even if no round has been observed yet, which acts as a
                // startup grace. It stacks with the explicit `grace_deadline` on non-initial
                // epochs; both are set at the same moment.
                last_progress_at: Instant::now(),
            })),
            threshold: committee_size / 2,
            grace_deadline,
        }
    }

    /// For a too-old rejection, decide whether to SKIP it (not count it toward demotion).
    ///
    /// We skip while our own DAG is still making progress: certificates are being written
    /// (`cert_store_round` advancing) or committed (`committed_round` advancing). A rejection
    /// then reflects a transient lag — a lost gossip message being fetched, parents arriving but
    /// not yet usable — and demoting would only tear down the epoch tasks for something that
    /// resolves within seconds. Only if neither round has advanced for `wedge_window` is the
    /// node genuinely wedged, so we stop skipping and let the rejection accumulate toward a
    /// demote (whose rejoin re-primes consensus).
    ///
    /// The check is on local progress, not on a timer: a wedged node falls through as soon as
    /// the window expires, while a recovering node is never counted.
    pub(crate) fn skip_while_progressing(
        &self,
        cert_store_round: Round,
        committed_round: Round,
        wedge_window: Duration,
    ) -> bool {
        let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
        let mut progressed = false;
        if cert_store_round > guard.last_cert_store_round {
            guard.last_cert_store_round = cert_store_round;
            progressed = true;
        }
        if committed_round > guard.last_committed_round {
            guard.last_committed_round = committed_round;
            progressed = true;
        }
        if progressed {
            guard.last_progress_at = Instant::now();
            true
        } else {
            Instant::now().duration_since(guard.last_progress_at) < wedge_window
        }
    }

    /// Clear counters after successful certification.
    pub(crate) fn clear_counters(&self) {
        let mut guard = self.inner.lock().expect("vote failure tracker mutex poisoned");
        guard.too_old.clear();
        guard.epoch_mismatch.clear();
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

    /// Record an epoch-mismatch rejection from a peer that is at a newer
    /// epoch than ours. Rejections from stale peers must be filtered out
    /// by the caller before calling this method.
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
    /// in that state is gated on DAG progress like a too-old one (see `skip_while_progressing`).
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
        } else if self.grace_deadline.is_some_and(|d| Instant::now() < d) {
            RejectionOutcome::GracePeriod
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
        let tracker = VoteFailureTracker::new(4, None);
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
        let tracker = VoteFailureTracker::new(4, None);
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
        let tracker = VoteFailureTracker::new(4, None);
        let first = tracker.record_too_old(peer(1));
        assert!(matches!(first, RejectionOutcome::BelowThreshold));
        let repeat = tracker.record_too_old(peer(1));
        assert!(matches!(repeat, RejectionOutcome::BelowThreshold));
        let distinct = tracker.record_too_old(peer(2));
        assert!(matches!(distinct, RejectionOutcome::TransitionToInactive));
    }

    #[test]
    fn grace_period_delays_demotion() {
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        let tracker = VoteFailureTracker::new(4, Some(deadline));
        tracker.record_epoch_mismatch(peer(1));
        let at_threshold = tracker.record_epoch_mismatch(peer(2));
        assert!(matches!(at_threshold, RejectionOutcome::GracePeriod));
    }

    #[test]
    fn clear_counters_resets_peer_sets() {
        let tracker = VoteFailureTracker::new(4, None);
        tracker.record_epoch_mismatch(peer(1));
        tracker.record_too_old(peer(2));
        tracker.clear_counters();
        assert_eq!(tracker.epoch_mismatch_count(), 0);
        assert_eq!(tracker.too_old_count(), 0);
    }

    #[test]
    fn skip_while_committed_round_advances() {
        let tracker = VoteFailureTracker::new(4, None);
        let window = std::time::Duration::from_secs(30);
        // committed round advancing => transient proposer lag => skip (don't demote)
        assert!(tracker.skip_while_progressing(20, 10, window));
        assert!(tracker.skip_while_progressing(20, 11, window));
        assert!(tracker.skip_while_progressing(20, 12, window));
    }

    #[test]
    fn skip_while_cert_store_round_advances_alone() {
        let tracker = VoteFailureTracker::new(4, None);
        // setup only: seed both baselines so the asserts below exercise store-only progress
        let _ = tracker.skip_while_progressing(20, 10, std::time::Duration::ZERO);
        // certs are being written (a fetch is landing) even though nothing commits yet:
        // that is progress, never count the rejection
        assert!(tracker.skip_while_progressing(21, 10, std::time::Duration::ZERO));
        assert!(tracker.skip_while_progressing(22, 10, std::time::Duration::ZERO));
        assert!(tracker.skip_while_progressing(23, 10, std::time::Duration::ZERO));
    }

    #[test]
    fn skip_tolerates_brief_stall_within_window() {
        let tracker = VoteFailureTracker::new(4, None);
        let window = std::time::Duration::from_secs(30);
        assert!(tracker.skip_while_progressing(20, 10, window));
        // nothing advanced, but well within the wedge window => still skip
        assert!(tracker.skip_while_progressing(20, 10, window));
    }

    #[test]
    fn skip_stops_when_wedged() {
        let tracker = VoteFailureTracker::new(4, None);
        // first call establishes progress
        assert!(tracker.skip_while_progressing(20, 10, std::time::Duration::ZERO));
        // neither round advanced and the (zero) wedge window has elapsed =>
        // genuinely wedged => stop skipping so the rejection counts toward demotion
        assert!(!tracker.skip_while_progressing(20, 10, std::time::Duration::ZERO));
        // a lower value is not progress either
        assert!(!tracker.skip_while_progressing(19, 9, std::time::Duration::ZERO));
    }

    #[test]
    fn wedged_node_still_demotes_at_threshold() {
        let tracker = VoteFailureTracker::new(4, None);
        assert!(tracker.skip_while_progressing(20, 10, std::time::Duration::ZERO));
        assert!(!tracker.skip_while_progressing(20, 10, std::time::Duration::ZERO));
        // the caller now records instead of skipping: threshold still works as before
        assert!(matches!(tracker.record_too_old(peer(1)), RejectionOutcome::BelowThreshold));
        assert!(matches!(tracker.record_too_old(peer(2)), RejectionOutcome::TransitionToInactive));
    }
}
