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
    /// Minimum advance of the local execution anchor past the head the mark was stamped with, in
    /// addition to the time-based wait, so a mark cannot come due before execution has passed
    /// where the send could first have landed.
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

/// The state of one tracked hash: sent and awaiting a due check, or held after execution dropped
/// it as nonce-too-high.
///
/// A validator's stale ack is stamped as a send too: an honest one is followed by the pool pruning
/// the hash within a few blocks, so a hash still pending after the window re-sends, and an ack
/// cannot silence a transaction.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Mark {
    /// Sent at `at` against head `anchor` (the caller's head at send time, which the due check
    /// measures local execution against), after `attempts` prior resends.
    Sent { at: Instant, anchor: u64, attempts: u32 },
    /// Executed as nonce-too-high at `at`: pooled and unexecutable until `sender`'s state nonce
    /// advances, which releases it ([`InFlightTracker::release_advanced`]); due only once
    /// [`DROPPED_HOLD`] lapses, since a re-seal or re-send before that drops it again.
    Held { at: Instant, sender: Address },
}

impl Mark {
    /// Returns whether this mark is eligible for release under the given policy at `now`/`anchor`.
    pub(crate) fn is_due(&self, now: Instant, anchor: u64, policy: &DuePolicy) -> bool {
        match *self {
            Mark::Sent { at, anchor: mark_anchor, attempts } => {
                let shift = attempts.min(policy.backoff_shift_cap).min(31);
                let wait = policy.after.saturating_mul(1u32 << shift);
                now.duration_since(at) >= wait
                    && anchor >= mark_anchor.saturating_add(policy.min_anchor_advance)
            }
            Mark::Held { at, .. } => now.duration_since(at) >= DROPPED_HOLD,
        }
    }

    /// Refreshes the mark to the given time/anchor and bumps its attempt count.
    pub(crate) fn resend(&mut self, now: Instant, anchor: u64) {
        let attempts = match *self {
            Self::Sent { attempts, .. } => attempts,
            Self::Held { .. } => 0,
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
}

/// Schema version of `MarkBackup`, bumped whenever its serialized shape changes so a backup
/// written by a prior version is rejected instead of misread.
pub const MARK_BACKUP_VERSION: u32 = 1;

/// A serializable snapshot of the marks tracked for one role, persisted across a restart and
/// restored via `InFlightTracker::stash_restore` once re-armed for the matching role.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MarkBackup {
    /// Schema version this backup was written under; see `MARK_BACKUP_VERSION`.
    pub version: u32,
    /// The role the marks were captured under; a restore for a different role is discarded.
    pub role: MarkRole,
    /// The captured marks, sorted by hash for deterministic serialization.
    pub marks: Vec<SavedMark>,
}

/// The role a captured `MarkBackup` belongs to, mirroring `Armed` in a serializable form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MarkRole {
    /// Marks owned by the block builder.
    Sealing,
    /// Marks owned by the gossip forwarder.
    Forwarding,
}

/// One tracked hash's mark, in the serializable form used by `MarkBackup`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SavedMark {
    /// The tracked transaction hash.
    pub hash: TxHash,
    /// The mark state to restore for this hash.
    pub kind: SavedMarkKind,
}

/// The serializable form of `Mark`, carrying only the state needed to resume tracking after a
/// restart (the wall-clock `at`/`anchor` fields are re-stamped fresh on restore instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SavedMarkKind {
    /// A `Mark::Sent` reduced to its resend count; the clock and anchor are re-stamped on restore.
    Sent { attempts: u32 },
}
