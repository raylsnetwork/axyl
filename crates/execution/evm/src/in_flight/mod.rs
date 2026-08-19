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
//! pending set ([`InFlightTracker::release_mined`]), an explicit drop of pool-rejected hashes
//! ([`InFlightTracker::release_dropped`]), the sealing TTL sweep, and the epoch-transition clear.
//! Every release that frees at least one mark ticks the release watch
//! ([`InFlightTracker::release_events`]) exactly once - once per release *call*, not per hash -
//! which is the builder's only wake-up signal that transactions are re-selectable again; a no-op
//! release must not tick it, or the builder spins on wakes that select nothing.
//!
//! # Restart survival
//!
//! Marks are captured to a versioned, serializable [`MarkBackup`] ([`InFlightTracker::snapshot`])
//! and staged for the next boot ([`InFlightTracker::stash_restore`]). The stash is applied lazily
//! at the next arm, and only when its role matches: a restore for the wrong role is discarded
//! rather than corrupt the freshly armed state. Wall-clock fields are re-stamped fresh on restore,
//! so only the resend and backoff state carries across, never a stale clock.
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
    map::{B256Map, B256Set},
    TxHash,
};
use parking_lot::RwLock;
#[cfg(test)]
use std::time::Duration;
use std::{sync::Arc, time::Instant};
use tracing::info;

mod marks;
mod metrics;

pub use marks::*;

#[derive(Default, Debug)]
struct Inner {
    marks: B256Map<Mark>,
    armed: Option<Armed>,
    // Staged by `stash_restore` ahead of the arm that will use it, since the caller
    // (e.g. crash recovery) knows the saved role before the tracker is re-armed. Applied
    // lazily in `consume_stash` so a restore for the wrong role is dropped instead of
    // corrupting the freshly armed state.
    pending_restore: Option<MarkBackup>,
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
        // arm tracked. Only `pending_restore`, staged separately, survives into the new round.
        guard.marks.clear();
        guard.armed = Some(Armed::Sealing(policy));
        let restored = Self::consume_stash(&mut guard, MarkRole::Sealing);
        drop(guard);
        self.on_released(previous_marks_len, 0, &self.metrics.released_clear);
        self.on_restored(restored, MarkRole::Sealing);
        SealMarks::new(self.clone())
    }

    /// Arms the tracker for a forwarding round under the given expiry policy, restoring any
    /// marks stashed for the forwarding role.
    pub fn arm_forwarding(&self, policy: DuePolicy) -> ForwardMarks {
        let mut guard = self.inner.write();
        guard.armed = Some(Armed::Forwarding);
        let restored = Self::consume_stash(&mut guard, MarkRole::Forwarding);
        drop(guard);
        self.on_restored(restored, MarkRole::Forwarding);
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
        guard.marks.retain(|_, mark| !mark.is_due(Instant::now(), anchor, policy));
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.on_released(previous_marks_len, current_marks_len, &self.metrics.released_ttl);
        previous_marks_len - current_marks_len
    }

    /// Releases the given hashes, e.g. transactions the pool dropped before mining.
    pub fn release_dropped(&self, hashes: impl IntoIterator<Item = TxHash>) {
        let mut hashes = hashes.into_iter().peekable();
        if hashes.peek().is_none() {
            return;
        }

        let mut guard = self.inner.write();
        let previous_marks_len = guard.marks.len();
        for hash in hashes {
            guard.marks.remove(&hash);
        }
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
                // design: the forwarder tracks acknowledgement across restarts via
                // stash/restore, so `clear` only disarms and leaves the marks for the
                // next arm to inherit or explicitly release.
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

impl InFlightTracker {
    /// Captures the current marks for the armed role, sorted for deterministic serialization.
    /// Returns `None` if the tracker is unarmed.
    pub fn snapshot(&self) -> Option<MarkBackup> {
        let guard = self.inner.read();
        let role = match guard.armed? {
            Armed::Sealing(_) => MarkRole::Sealing,
            Armed::Forwarding => MarkRole::Forwarding,
        };
        let mut marks: Vec<SavedMark> = guard
            .marks
            .iter()
            .map(|(hash, mark)| SavedMark {
                hash: *hash,
                kind: match *mark {
                    Mark::Sent { attempts, .. } => SavedMarkKind::Sent { attempts },
                    Mark::AckedStale => SavedMarkKind::AckedStale,
                },
            })
            .collect();
        drop(guard);
        marks.sort_unstable_by_key(|mark| mark.hash);
        Some(MarkBackup { version: MARK_BACKUP_VERSION, role, marks })
    }

    /// Stages a previously captured snapshot to be restored on the next matching-role arm.
    pub fn stash_restore(&self, backup: MarkBackup) {
        self.inner.write().pending_restore = Some(backup);
    }

    fn consume_stash(guard: &mut Inner, role: MarkRole) -> usize {
        let Some(backup) = guard.pending_restore.take() else { return 0 };
        if backup.role != role {
            info!(
                target: "rayls::txpool",
                discarded = backup.marks.len(),
                saved_role = ?backup.role,
                armed_role = ?role,
                "discarded restored in-flight marks on role change"
            );
            return 0;
        }
        let now = Instant::now();
        let before = guard.marks.len();
        for mark in backup.marks {
            let restored = match mark.kind {
                SavedMarkKind::Sent { attempts } => Mark::Sent { at: now, anchor: 0, attempts },
                // only the forwarder mints acked-stale marks; under a sealing arm one would
                // strand its tx past the TTL (the sweep never releases acked-stale)
                SavedMarkKind::AckedStale if role == MarkRole::Sealing => continue,
                SavedMarkKind::AckedStale => Mark::AckedStale,
            };
            guard.marks.insert(mark.hash, restored);
        }
        guard.marks.len() - before
    }

    fn on_restored(&self, n: usize, role: MarkRole) {
        if n > 0 {
            self.metrics.marked.inc_by(n as u64);
            self.metrics.restored.inc_by(n as u64);
            info!(target: "rayls::txpool", restored = n, ?role, "restored in-flight marks");
        }
        // Add the restored count directly rather than re-reading `self.len()`, which would take
        // the read lock a second time and could observe a length a concurrent writer already moved.
        self.metrics.gauge.add(n as i64);
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
