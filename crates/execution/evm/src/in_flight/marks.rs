use crate::in_flight::InFlightTracker;
use alloy::primitives::{Address, TxHash};
use std::time::{Duration, Instant};

/// Governs when a mark becomes eligible for release: a base wait, an exponential backoff cap
/// on repeated resends, and a minimum execution-anchor advance since the mark was last set.
#[derive(Copy, Clone, Debug)]
pub struct DuePolicy {
    /// Base wait before a fresh mark is due.
    pub after: Duration,
    /// Upper bound on the backoff shift applied per resend attempt, so wait time plateaus
    /// instead of growing unbounded under a stuck transaction.
    pub backoff_shift_cap: u32,
    /// Minimum execution-anchor advance required since the mark was set, in addition to the
    /// time-based wait, so a mark cannot come due before at least one new block was seen.
    pub min_anchor_advance: u64,
}

impl DuePolicy {
    /// Builds a pure time-based policy with no backoff or anchor requirement.
    pub const fn ttl(after: Duration) -> Self {
        Self { after, backoff_shift_cap: 0, min_anchor_advance: 0 }
    }
}

/// Upper bound on how long a dropped hash stays held when its sender never advances, so a
/// predecessor lost for good is still retried, just not every round.
pub const DROPPED_HOLD: Duration = Duration::from_secs(60);

/// The state of one tracked hash: sent and awaiting a due check, acknowledged stale by the
/// forwarder, or held after execution dropped it as nonce-too-high. `AckedStale` is a terminal
/// state outside the resend/expiry cycle - see `is_due`.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Mark {
    /// Sent at `at` while the execution anchor was `anchor`, after `attempts` prior resends.
    Sent { at: Instant, anchor: u64, attempts: u32 },
    /// Confirmed no longer live by the forwarder; never due, released only by reconcile or clear.
    AckedStale,
    /// Executed as nonce-too-high at `at`: pooled and unexecutable until `sender`'s state nonce
    /// advances, which releases it ([`InFlightTracker::release_advanced`]); due only once
    /// [`DROPPED_HOLD`] lapses, since a re-seal or re-send before that drops it again.
    Held { at: Instant, sender: Address },
}

impl Mark {
    /// Returns whether this mark is eligible for release under the given policy at `now`/`anchor`.
    ///
    /// An `AckedStale` mark is never due: it is released only by an explicit reconcile or clear,
    /// never by a TTL sweep, since the forwarder already knows the transaction is stale.
    pub(crate) fn is_due(&self, now: Instant, anchor: u64, policy: &DuePolicy) -> bool {
        match *self {
            Mark::Sent { at, anchor: mark_anchor, attempts } => {
                let shift = attempts.min(policy.backoff_shift_cap).min(31);
                let wait = policy.after.saturating_mul(1u32 << shift);
                now.duration_since(at) >= wait
                    && anchor >= mark_anchor.saturating_add(policy.min_anchor_advance)
            }
            Mark::Held { at, .. } => now.duration_since(at) >= DROPPED_HOLD,
            Mark::AckedStale => false,
        }
    }

    /// Refreshes a `Sent` or `Held` mark to the given time/anchor and bumps its attempt count; a
    /// no-op on an `AckedStale` mark, which never resends.
    pub(crate) fn resend(&mut self, now: Instant, anchor: u64) {
        let attempts = match *self {
            Self::Sent { attempts, .. } => attempts,
            Self::Held { .. } => 0,
            Self::AckedStale => return,
        };
        *self = Self::Sent { at: now, anchor, attempts: attempts.saturating_add(1) };
    }
}

/// The role the tracker is currently armed for, carrying the policy sealing needs to decide
/// when a mark is due (forwarding decides due-ness per call via its own stored policy instead).
#[derive(Debug, Copy, Clone)]
pub(crate) enum Armed {
    /// The block builder, with the policy its TTL sweep releases under.
    Sealing(DuePolicy),
    /// The gossip forwarder, which decides due-ness per call through its own handle.
    Forwarding,
}

/// A capability handle scoping mark writes to the sealing role, returned by
/// `InFlightTracker::arm_sealing` so a caller cannot mark hashes without first arming.
#[must_use = "dropping the handle discards the sealing capability the arm just minted"]
#[derive(Debug)]
pub struct SealMarks {
    tracker: InFlightTracker,
}

impl SealMarks {
    /// Wraps a tracker already armed for sealing.
    pub fn new(tracker: InFlightTracker) -> Self {
        Self { tracker }
    }

    /// Marks the given hashes as freshly sent, resetting their due clock.
    pub fn mark(&self, hashes: impl IntoIterator<Item = TxHash>) {
        self.tracker.track_all(hashes, Mark::Sent { at: Instant::now(), anchor: 0, attempts: 0 });
    }
}

/// A point-in-time read of one hash's forwarding state, combining presence and due-ness so a
/// caller need not take the tracker lock twice to answer both questions consistently.
#[derive(Debug, Copy, Clone)]
pub struct ForwardProbe {
    /// Whether the hash is currently tracked as forwarded.
    pub forwarded: bool,
    /// Whether the hash is untracked or its mark is due for resend/release.
    pub due: bool,
    /// Re-send attempts recorded for this hash (0 if untracked or not a `Sent` mark). Used to
    /// decide when a repeatedly-forwarded transaction should be pruned as unmineable.
    pub attempts: u32,
}

/// A capability handle scoping mark writes to the forwarding role, returned by
/// `InFlightTracker::arm_forwarding` so a caller cannot mark hashes without first arming.
#[must_use = "dropping the handle discards the forwarding capability the arm just minted"]
#[derive(Debug, Clone)]
pub struct ForwardMarks {
    tracker: InFlightTracker,
    policy: DuePolicy,
}

impl ForwardMarks {
    /// Wraps a tracker already armed for forwarding, under the given expiry policy.
    pub fn new(tracker: InFlightTracker, policy: DuePolicy) -> Self {
        Self { tracker, policy }
    }

    /// Returns whether the given hash is untracked or due under this round's policy.
    pub fn is_due(&self, hash: &TxHash, now: Instant, anchor: u64) -> bool {
        self.tracker.is_due(hash, now, anchor, &self.policy)
    }

    /// Returns whether the given hash is currently tracked as forwarded.
    pub fn is_forwarded(&self, hash: &TxHash) -> bool {
        self.tracker.is_in_flight(hash)
    }

    /// Reads presence and due-ness for the given hash under one lock acquisition, so the two
    /// facts reflect the same instant instead of two independently-locked reads racing a
    /// concurrent writer.
    pub fn probe(&self, hash: &TxHash, now: Instant, anchor: u64) -> ForwardProbe {
        let guard = self.tracker.inner.read();
        let mark = guard.marks.get(hash);
        ForwardProbe {
            forwarded: mark.is_some(),
            due: mark.is_none_or(|m| m.is_due(now, anchor, &self.policy)),
            attempts: match mark {
                Some(Mark::Sent { attempts, .. }) => *attempts,
                _ => 0,
            },
        }
    }

    /// Records a forward attempt for the given hashes: resends an existing mark (bumping its
    /// backoff) or inserts a fresh one, so a first forward and a retry share one code path.
    pub fn mark_forwarded(
        &self,
        hashes: impl IntoIterator<Item = TxHash>,
        now: Instant,
        anchor: u64,
    ) {
        let mut hashes = hashes.into_iter().peekable();
        if hashes.peek().is_none() {
            return;
        }

        let mut guard = self.tracker.inner.write();
        let previous_marks_len = guard.marks.len();
        for hash in hashes {
            guard
                .marks
                .entry(hash)
                .and_modify(|mark| mark.resend(now, anchor))
                .or_insert(Mark::Sent { at: now, anchor, attempts: 0 });
        }
        let current_marks_len = guard.marks.len();
        drop(guard);
        self.tracker.on_marked(previous_marks_len, current_marks_len);
        self.tracker.on_forwarded(previous_marks_len, current_marks_len);
    }

    /// Marks the given hashes acknowledged-stale: forwarded once, confirmed no longer live, and
    /// exempt from further resend or TTL release until explicitly reconciled or cleared.
    pub fn mark_acked_stale(&self, hashes: impl IntoIterator<Item = TxHash>) {
        self.tracker.track_all(hashes, Mark::AckedStale);
    }
}
