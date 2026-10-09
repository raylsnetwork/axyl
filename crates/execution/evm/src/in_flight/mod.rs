// allow: long-doc module overview of the in-flight tracker feature (roles, marks, release, restart)
//! Tracking of transactions sent to the mempool but not yet observed mined, so a sealing or
//! forwarding round can skip the hashes it already has outstanding instead of resending them.
//!
//! # Why it exists
//!
//! Both the block builder and the transaction forwarder repeatedly scan the pending sub-pool and
//! push its transactions onward: the builder seals them into batches, the forwarder gossips them
//! to the current leader. With no memory of what is already outstanding, every scan re-sends the
//! same hashes, so a transaction stuck behind a deep inclusion backlog is sealed into batch after
//! batch (wasted bytes) or re-gossiped every round (network amplification). [`InFlightTracker`] is
//! that memory: a hash is marked when sent and released when the pool proves it is no longer
//! pending, and each round consults the tracker to skip what is still in flight.
//!
//! # Roles
//!
//! The two consumers have different release semantics, so the tracker is armed for exactly one
//! [`MarkRole`] at a time and hands back a capability handle that scopes the writes it allows:
//!
//! - **Sealing** ([`SealMarks`]) is the block builder. Arming installs a policy-driven sweep
//!   ([`InFlightTracker::sweep_due`]) that releases marks once their batch is old enough to be
//!   presumed lost, and each arm starts from an empty set because a new sealing round has no
//!   relation to the prior one.
//! - **Forwarding** ([`ForwardMarks`]) is the gossip forwarder. It installs no sweep: the forwarder
//!   re-drives its own resends through a due check with exponential backoff, so a flat sweep would
//!   erase that backoff and reintroduce the amplification it exists to stop. Forwarding marks
//!   survive an epoch boundary ([`InFlightTracker::clear`] keeps them) because validator pools
//!   persist across it, and re-publishing the whole pool with the backoff erased would flood the
//!   leader.
//!
//! # Marks and due-ness
//!
//! A tracked hash holds one `Mark`: `Sent`, carrying the send time, the execution anchor at send,
//! and a resend-attempt count; or `AckedStale`, the terminal state the forwarder mints once it has
//! confirmed a forwarded transaction is no longer live but still wants further resends suppressed.
//! A `Sent` mark comes due under a [`DuePolicy`] combining a base wait, a per-attempt exponential
//! backoff (capped), and a minimum anchor advance, so a mark cannot release on wall-clock time
//! alone before the node has seen at least one new block. An `AckedStale` mark is never due; only
//! an explicit reconcile or clear releases it.
//!
//! # Release paths and the release watch
//!
//! A mark leaves the set through one of several paths, each counted under its own cause so TTL
//! churn stays distinguishable from healthy execution-driven release: reconcile against the live
//! pending set ([`InFlightTracker::release_mined`]), the release of hashes execution dropped as
//! nonce-too-high once their sender's state nonce advances ([`InFlightTracker::hold_dropped`] then
//! [`InFlightTracker::release_advanced`]), the sealing TTL sweep, and the epoch-transition clear.
//! Every release that frees at least one mark ticks the release watch
//! ([`InFlightTracker::release_events`]) exactly once - once per release *call*, not per hash -
//! which is the builder's only wake-up signal that transactions are re-selectable again; a no-op
//! release must not tick it, or the builder spins on wakes that select nothing.
//!
//! # Restart survival
//!
//! In-flight marks are not persisted across a restart. The mempool itself is ephemeral (standard
//! semantics: a client resubmits any transaction accepted but not yet mined), so a fresh boot
//! starts with no marks and re-tracks transactions as they are re-forwarded or re-sealed.
//!
//! # Concurrency
//!
//! The tracker is a cheaply cloned handle over one shared `parking_lot::RwLock`, and in production
//! four writers race it: the builder marks, the canonical-chain task releases mined hashes, the
//! engine tick sweeps, and the epoch transition clears. Correctness does not depend on their
//! interleaving. Every mutation computes its metric delta under the same write lock that applies
//! it, so the deltas telescope and the conservation identity
//! `marked - reconcile - ttl - clear = live set size` (and the gauge) holds under any
//! linearization.

use crate::in_flight::metrics::{InFlightMetrics, IN_FLIGHT_METRICS};
use alloy::primitives::{
    map::{AddressSet, B256Map, B256Set},
    Address, TxHash,
};
use parking_lot::RwLock;
#[cfg(test)]
use std::time::Duration;
use std::{sync::Arc, time::Instant};

mod marks;
mod metrics;

pub use marks::*;

#[derive(Default, Debug)]
struct Inner {
    marks: B256Map<Mark>,
    armed: Option<Armed>,
}

/// Shared, cheaply cloned handle over the in-flight mark set; see the module header for the
/// roles, release paths, and restart behavior.
#[derive(Clone, Debug)]
pub struct InFlightTracker {
    inner: Arc<RwLock<Inner>>,
    metrics: InFlightMetrics,
    release_epoch: tokio::sync::watch::Sender<u64>,
}

impl Default for InFlightTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl InFlightTracker {
    /// Creates a tracker backed by the process-wide in-flight metrics.
    pub fn new() -> Self {
        Self::with_metrics(IN_FLIGHT_METRICS.clone())
    }

    fn with_metrics(metrics: InFlightMetrics) -> Self {
        Self { inner: Arc::default(), metrics, release_epoch: tokio::sync::watch::Sender::new(0) }
    }

    /// Arms the tracker for a sealing round under the given expiry policy, discarding any
    /// marks left from the prior round.
    pub fn arm_sealing(&self, policy: DuePolicy) -> SealMarks {
        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        // Wipe unconditionally: a new sealing round has no relation to whatever the prior
        // arm tracked.
        guard.marks.clear();
        guard.armed = Some(Armed::Sealing(policy));
        drop(guard);
        self.on_released(previous_marks_len, 0, &self.metrics.released_clear);
        SealMarks::new(self.clone())
    }

    /// Arms the tracker for a forwarding round under the given expiry policy.
    pub fn arm_forwarding(&self, policy: DuePolicy) -> ForwardMarks {
        let mut guard = self.inner.write();
        guard.armed = Some(Armed::Forwarding);
        drop(guard);
        ForwardMarks::new(self.clone(), policy)
    }
}

impl InFlightTracker {
    /// Returns whether no hashes are tracked, including `AckedStale` marks a sweep never releases.
    pub fn is_empty(&self) -> bool {
        self.inner.read().marks.is_empty()
    }

    /// Returns the tracked hash count, counting both outstanding sends and marks stranded
    /// past their TTL (`Mark::AckedStale`) that a sweep will never release.
    pub fn len(&self) -> usize {
        self.inner.read().marks.len()
    }

    /// Returns whether the given hash is tracked, including a `Mark::AckedStale` hash a
    /// sweep will never release on its own.
    pub fn is_in_flight(&self, hash: &TxHash) -> bool {
        self.inner.read().marks.contains_key(hash)
    }

    fn is_due(&self, hash: &TxHash, now: Instant, anchor: u64, policy: &DuePolicy) -> bool {
        self.inner.read().marks.get(hash).is_none_or(|mark| mark.is_due(now, anchor, policy))
    }

    /// Subscribes to the release epoch, which ticks each time one or more marks are released.
    pub fn release_events(&self) -> tokio::sync::watch::Receiver<u64> {
        self.release_epoch.subscribe()
    }
}

impl InFlightTracker {
    fn track_all(&self, hashes: impl IntoIterator<Item = TxHash>, mark: Mark) {
        let mut hashes = hashes.into_iter().peekable();
        if hashes.peek().is_none() {
            return;
        }

        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        for hash in hashes {
            guard.marks.insert(hash, mark);
        }
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_marked(previous_marks_len, current_marks_len)
    }

    /// Releases marks due under the active sealing policy at the given execution anchor;
    /// a no-op unless the tracker is armed for sealing.
    pub fn sweep_due(&self, anchor: u64) -> usize {
        let Some(Armed::Sealing(policy)) = self.inner.read().armed else {
            return 0;
        };
        self.release_due(anchor, &policy)
    }

    fn release_due(&self, anchor: u64, policy: &DuePolicy) -> usize {
        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        let now = Instant::now();
        guard.marks.retain(|_, mark| !mark.is_due(now, anchor, policy));
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_released(previous_marks_len, current_marks_len, &self.metrics.released_ttl);
        previous_marks_len - current_marks_len
    }

    /// Holds tracked hashes execution dropped as nonce-too-high: each stays in flight (neither
    /// re-sealed nor re-forwarded) until [`Self::release_advanced`] sees its sender's state nonce
    /// move or [`DROPPED_HOLD`] lapses. Untracked hashes are ignored, and a hash already held keeps
    /// its stamp, so a peer re-sealing it cannot extend the hold.
    pub fn hold_dropped(&self, dropped: impl IntoIterator<Item = (Address, TxHash)>) {
        let now = Instant::now();
        let mut guard = self.inner.write();
        for (sender, hash) in dropped {
            if let Some(mark @ Mark::Sent { .. }) = guard.marks.get_mut(&hash) {
                *mark = Mark::Held { at: now, sender };
            }
        }
    }

    /// Releases every held hash of the given senders, whose state nonce a block just advanced:
    /// their dropped successors may now execute, so they are re-sealable and re-forwardable.
    pub fn release_advanced(&self, senders: impl IntoIterator<Item = Address>) {
        let advanced: AddressSet = senders.into_iter().collect();
        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        guard.marks.retain(
            |_, mark| !matches!(mark, Mark::Held { sender, .. } if advanced.contains(sender)),
        );
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_released(previous_marks_len, current_marks_len, &self.metrics.released_dropped);
    }

    /// Reconciles against the given live set, releasing every tracked hash absent from it.
    pub fn release_mined(&self, live: &B256Set) -> usize {
        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        guard.marks.retain(|hash, _| live.contains(hash));
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_released(previous_marks_len, current_marks_len, &self.metrics.released_reconcile)
    }

    /// Releases marks for exactly the given hashes - the txns a block just mined, taken straight
    /// from the canonical notification. Touches only this tracker's own lock and is O(mined): no
    /// pending snapshot and no pool lock, so it cannot tax execution or gap the execution
    /// heartbeat. Complements [`Self::release_mined`], the periodic full-scan backstop that also
    /// reaps marks for txns that left the pending sub-pool WITHOUT mining
    /// (dropped/replaced/evicted).
    pub fn release_hashes(&self, mined: impl IntoIterator<Item = TxHash>) -> usize {
        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        for hash in mined {
            guard.marks.remove(&hash);
        }
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_released(previous_marks_len, current_marks_len, &self.metrics.released_reconcile)
    }

    /// Disarms the tracker, releasing all tracked marks except forwarding marks (which persist
    /// for the next arm to inherit or explicitly release).
    pub fn clear(&self) {
        let released = {
            let mut guard = self.inner.write();
            let released = match guard.armed {
                Some(Armed::Sealing(_)) => {
                    let previous_marks_len = guard.marks.len();
                    guard.marks.clear();
                    previous_marks_len
                }
                // Forwarding marks (including AckedStale) outlive an epoch boundary by
                // design: `clear` only disarms, leaving the marks for the next forwarding
                // arm to inherit or explicitly release rather than re-forwarding them.
                Some(Armed::Forwarding) => 0,
                None => {
                    let previous_marks_len = guard.marks.len();
                    guard.marks.clear();
                    previous_marks_len
                }
            };

            guard.armed = None;
            released
        };

        if released > 0 {
            self.on_released(released, 0, &self.metrics.released_clear);
        }
    }
}

impl InFlightTracker {
    fn on_marked(&self, previous_marks_len: usize, current_marks_len: usize) {
        let delta = current_marks_len.saturating_sub(previous_marks_len);
        self.metrics.marked.inc_by(delta as u64);
        self.metrics.gauge.add(delta as i64);
    }

    fn on_forwarded(&self, previous_marks_len: usize, current_marks_len: usize) {
        let delta = current_marks_len.saturating_sub(previous_marks_len);
        self.metrics.marked_forward.inc_by(delta as u64);
    }

    fn on_released(
        &self,
        previous_marks_len: usize,
        current_marks_len: usize,
        counter: &prometheus::IntCounter,
    ) -> usize {
        let delta = previous_marks_len.saturating_sub(current_marks_len);
        if delta > 0 {
            counter.inc_by(delta as u64);
            self.release_epoch.send_modify(|epoch| *epoch += 1);
        }
        self.metrics.gauge.sub(delta as i64);
        delta
    }
}

#[cfg(test)]
impl InFlightTracker {
    fn with_fresh_metrics() -> Self {
        Self::with_metrics(InFlightMetrics::register_fresh())
    }

    fn metrics(&self) -> &InFlightMetrics {
        &self.metrics
    }
}

/// Test levers that bypass the arm flow; production marks and sweeps only through the
/// [`SealMarks`]/[`ForwardMarks`] handles.
#[cfg(any(test, feature = "test-utils"))]
impl InFlightTracker {
    /// Marks the given hashes in-flight directly.
    pub fn mark_in_flight(&self, hashes: impl IntoIterator<Item = TxHash>) {
        self.track_all(hashes, Mark::Sent { at: Instant::now(), anchor: 0, attempts: 0 });
    }

    /// Returns every tracked hash, including any `Mark::AckedStale` a sweep will never
    /// release on its own.
    pub fn tracked_hashes(&self) -> Vec<TxHash> {
        let guard = self.inner.read();
        guard.marks.keys().cloned().collect()
    }

    /// Releases the given hashes directly.
    pub fn release_in_flight(&self, hashes: impl IntoIterator<Item = TxHash>) {
        let mut guard = self.inner.write();
        let before = guard.marks.len();
        for hash in hashes {
            guard.marks.remove(&hash);
        }
        let len = guard.marks.len();
        drop(guard);
        self.on_released(before, len, &self.metrics.released_reconcile);
    }

    /// Releases marks older than the given TTL, independent of any armed policy.
    pub fn sweep_expired(&self, ttl: std::time::Duration) -> usize {
        self.release_due(0, &DuePolicy::ttl(ttl))
    }
}

#[cfg(test)]
mod tests;
