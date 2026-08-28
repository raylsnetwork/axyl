//! Sender-side health of each committee member, ranking the forwarder's ring-walk.

use alloy::primitives::map::AddressMap;
use breaker::Breaker;
use rayls_consensus_worker::SubmitError;
use rayls_infrastructure_types::{ring_walk, Address, BlsPublicKey, TxHash};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// How long a first trip holds a validator out of the walk; each failed probe doubles it, up to
/// [`BREAKER_RETRIP_CAP`] doublings.
///
/// Wall-clock is fine here: the breaker is a node-local heuristic, not consensus state.
pub(super) const BREAKER_COOLDOWN: Duration = Duration::from_secs(10);

/// Doublings of the hold a breaker accrues across failed probes, so a validator that never
/// includes is probed ever more rarely (at most once per 16 cooldowns) while an honest one closes
/// on its first probe's inclusion.
const BREAKER_RETRIP_CAP: u32 = 4;

/// How long an unresolved probe stays outstanding before another is admitted.
///
/// A probe's verdict is its own re-send, which arrives one resend window after the probe, so this
/// must exceed the base window ([`super::FORWARD_POLICY`]: 3s plus 2 commits) or every probe is
/// replaced
/// before it can fail and the hold never grows. Past this, the probe's group was pruned or re-sends
/// are gated, and another probe is the only way to learn anything.
pub(super) const PROBE_EXPIRY: Duration = Duration::from_secs(40);

/// Consecutive failed sends (transport failures, timeouts, backlog sheds) that open a validator's
/// breaker.
///
/// A mode reject trips immediately: it is the peer's explicit self-report. Transport failures
/// tolerate transient noise before tripping, and a shed tolerates a burst: only a validator that
/// keeps refusing is saturated, and holding it out routes its senders to their rendezvous
/// fallbacks for a cooldown instead of retrying it every tick.
const BREAKER_TRANSPORT_TRIP: u32 = 3;

/// Re-sent sender groups blamed on a validator that open its breaker.
///
/// A validator that acks and drops never fails a send, so non-inclusion is counted on its own and
/// an ack neither resets the count nor closes the trip ([`breaker::Cause::NotIncluded`]); only an
/// inclusion does either. The count carries the same tolerance for an honest validator that is
/// merely slow as transport failures do; a spurious trip costs one cooldown plus the time its next
/// probe takes to land.
pub(super) const BREAKER_NOT_INCLUDED_TRIP: u32 = 3;

/// The forwarder's breaker on one validator: closed (counting failures) or open (tripped).
///
/// Only a trip constructs [`Open`] and the module boundary keeps its fields private, so a hold can
/// never be forged; half-open is not stored but read off the clock ([`Breaker::holds`]).
mod breaker {
    use super::{
        BREAKER_COOLDOWN, BREAKER_NOT_INCLUDED_TRIP, BREAKER_RETRIP_CAP, BREAKER_TRANSPORT_TRIP,
        PROBE_EXPIRY,
    };
    use rayls_consensus_worker::{SubmitError, SubmitRejection};
    use rayls_infrastructure_types::Address;
    use std::time::{Duration, Instant};

    /// In the walk, counting consecutive transport failures toward the trip threshold.
    #[derive(Debug, Clone, Copy, Default)]
    pub(super) struct Closed {
        /// Consecutive transport failures since the last success.
        transport_failures: u32,
        /// Re-sent groups blamed on this validator since its record was last clean.
        not_included: u32,
    }

    /// What tripped a breaker, which decides the evidence that closes it.
    #[derive(Debug, Clone, Copy)]
    pub(super) enum Cause {
        /// Failed sends; the next ack proves the validator reachable and admitting again.
        Send,
        /// Acked sends that came back as re-sends. A censor acks everything, so an ack proves
        /// nothing here; only a delivered group leaving the pool (an inclusion) does.
        NotIncluded,
    }

    /// The one group admitted to a lapsed breaker; the rest of the slot's traffic walks on until
    /// it resolves or [`PROBE_EXPIRY`] passes.
    #[derive(Debug, Clone, Copy)]
    pub(super) struct Probe {
        /// When the probe was admitted.
        at: Instant,
        /// The sender whose group is the probe; a blame for it is the probe failing.
        sender: Address,
    }

    /// Tripped: held out of the walk until its hold lapses, then admitted one probe group at a
    /// time (half-open), whose outcome re-trips with a doubled hold or closes it.
    #[derive(Debug, Clone, Copy)]
    pub(super) struct Open {
        /// When the breaker tripped or last re-tripped.
        since: Instant,
        /// What tripped it.
        cause: Cause,
        /// The outstanding probe, if one is.
        probe: Option<Probe>,
        /// Failed probes since the trip; each doubles the hold.
        retrips: u32,
    }

    impl Open {
        const fn trip(since: Instant, cause: Cause) -> Self {
            Self { since, cause, probe: None, retrips: 0 }
        }

        /// A re-trip at `now` after a failed probe.
        fn retrip(self, now: Instant) -> Self {
            Self { since: now, probe: None, retrips: self.retrips.saturating_add(1), ..self }
        }

        /// How long this trip holds.
        fn hold(self) -> Duration {
            BREAKER_COOLDOWN.saturating_mul(1 << self.retrips.min(BREAKER_RETRIP_CAP))
        }
    }

    /// Either breaker state; every validator starts [`Closed`] with a clean record.
    #[derive(Debug, Clone, Copy)]
    pub(super) enum Breaker {
        /// See [`Closed`].
        Closed(Closed),
        /// See [`Open`].
        Open(Open),
    }

    impl Default for Breaker {
        fn default() -> Self {
            Self::Closed(Closed::default())
        }
    }

    impl Breaker {
        /// Returns whether the breaker holds its validator out of the walk at `now`.
        pub(super) fn holds(self, now: Instant) -> bool {
            matches!(self, Self::Open(open) if now.duration_since(open.since) < open.hold())
        }

        /// Returns whether a group may be delivered to this validator at `now`. A lapsed breaker
        /// with no probe outstanding admits the group as its probe when `probe` names the group's
        /// sender, which only a first send does: its verdict (its re-send) arrives within the base
        /// resend window, inside [`PROBE_EXPIRY`], where a backed-off re-send's would not.
        pub(super) fn admit(&mut self, now: Instant, probe: Option<Address>) -> bool {
            let holds = self.holds(now);
            let Self::Open(open) = self else { return true };
            if holds || open.probe.is_some_and(|probe| now.duration_since(probe.at) < PROBE_EXPIRY)
            {
                return false;
            }
            let Some(sender) = probe else { return false };
            open.probe = Some(Probe { at: now, sender });
            true
        }

        /// Returns the breaker after a failed submit at `now`.
        ///
        /// A mode reject trips at once (the peer's explicit self-report). A transport failure or
        /// a backlog shed counts toward the threshold while closed and re-trips an open breaker
        /// outright.
        pub(super) fn failed(self, error: &SubmitError, now: Instant) -> Self {
            match (self, error) {
                (
                    Self::Closed(Closed { transport_failures, not_included }),
                    SubmitError::Network(_) | SubmitError::Rejected(SubmitRejection::Backlog),
                ) => {
                    let transport_failures = transport_failures.saturating_add(1);
                    if transport_failures < BREAKER_TRANSPORT_TRIP {
                        Self::Closed(Closed { transport_failures, not_included })
                    } else {
                        Self::Open(Open::trip(now, Cause::Send))
                    }
                }
                (Self::Closed(_), _) => Self::Open(Open::trip(now, Cause::Send)),
                // A lapsed breaker's send is its probe: a doubled hold. A last-resort send to a
                // holding breaker restarts the same hold.
                (Self::Open(open), _) if !self.holds(now) => Self::Open(open.retrip(now)),
                (Self::Open(open), _) => Self::Open(Open { since: now, ..open }),
            }
        }

        /// Returns the breaker after `sender`'s re-sent group was blamed on its validator at
        /// `now`: counts toward the trip while closed; re-trips an open breaker when the group was
        /// its probe ([`Open::retrip`]), and leaves it as it is otherwise, since that send predates
        /// the trip.
        pub(super) fn not_included(self, now: Instant, sender: Address) -> Self {
            match self {
                Self::Closed(Closed { transport_failures, not_included }) => {
                    let not_included = not_included.saturating_add(1);
                    if not_included < BREAKER_NOT_INCLUDED_TRIP {
                        Self::Closed(Closed { transport_failures, not_included })
                    } else {
                        Self::Open(Open::trip(now, Cause::NotIncluded))
                    }
                }
                Self::Open(open) if open.probe.is_some_and(|probe| probe.sender == sender) => {
                    Self::Open(open.retrip(now))
                }
                Self::Open(_) => self,
            }
        }

        /// Records an acked submit; returns whether the ack closes an open breaker, which only a
        /// send-failure trip allows ([`Cause`]). A closed breaker keeps its non-inclusion count.
        pub(super) fn acked(&mut self) -> bool {
            match self {
                Self::Closed(closed) => {
                    closed.transport_failures = 0;
                    false
                }
                Self::Open(open) => matches!(open.cause, Cause::Send),
            }
        }
    }
}

/// The last acked delivery of one sender's group.
#[derive(Debug, Clone, Copy)]
struct Delivery {
    /// The validator that acked the group.
    peer: BlsPublicKey,
    /// The highest-nonce transaction delivered; nonces are contiguous, so its leaving the pool is
    /// the inclusion of the whole delivered run.
    last: TxHash,
}

/// Per-validator breaker state for one epoch's committee, ranking the forwarder's ring-walk.
///
/// A node-local heuristic, never consensus state. A breaker holds a validator out of the walk after
/// local send failures (transport errors, or the peer's own mode reject) or after enough sends it
/// acked came back as re-sends. It never holds work: a held validator only walks last, and a
/// lapsed breaker admits one probe group, whose ack or inclusion ([`breaker::Cause`]) closes it.
/// Inclusion is read off the forwarder's own pool via each sender's last acked delivery.
#[derive(Debug, Default)]
pub(super) struct ValidatorHealth {
    /// Breakers of validators with a send failure on record; absent means closed with a clean
    /// record.
    breakers: HashMap<BlsPublicKey, Breaker>,
    /// Each sender's last acked delivery, until it leaves the pool.
    deliveries: AddressMap<Delivery>,
}

impl ValidatorHealth {
    /// Health with every validator's breaker closed and a clean record.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Record an acknowledged submit of `delivered` (sender and hash, nonce-ordered per sender) to
    /// `peer`. Returns whether the ack closed an open breaker ([`Breaker::acked`]), reported for a
    /// lapsed breaker too, since the probe's ack is the recovery worth reporting.
    pub(super) fn on_success(
        &mut self,
        peer: &BlsPublicKey,
        delivered: impl IntoIterator<Item = (Address, TxHash)>,
    ) -> bool {
        self.deliveries.extend(
            delivered.into_iter().map(|(sender, last)| (sender, Delivery { peer: *peer, last })),
        );
        let recovered = self.breakers.get_mut(peer).is_some_and(Breaker::acked);
        if recovered {
            self.breakers.remove(peer);
        }
        recovered
    }

    /// Credit every validator whose last acked delivery for a sender has left the pool, per
    /// `pending`; returns the validators whose open breaker that inclusion closed.
    pub(super) fn credit_included(
        &mut self,
        pending: impl Fn(&TxHash) -> bool,
    ) -> Vec<BlsPublicKey> {
        let Self { breakers, deliveries } = self;
        let mut recovered = Vec::new();
        deliveries.retain(|_, delivery| {
            if pending(&delivery.last) {
                return true;
            }
            // An inclusion closes an open breaker whatever tripped it and clears a closed one's
            // record: it is the evidence a non-inclusion count is missing.
            if matches!(breakers.remove(&delivery.peer), Some(Breaker::Open(_))) {
                recovered.push(delivery.peer);
            }
            false
        });
        recovered
    }

    /// Blame each re-sent group on the validator that acked its prior send and return the
    /// validators whose breaker that opened. `groups` yields one `(sender, owner, resent)` per
    /// selected group; a first send (`resent` false) is never blamed for non-inclusion.
    ///
    /// The prior target is the recorded delivery when one is still held (the marks are node-scoped
    /// and carry no target, the committee is epoch-scoped), else the group's owner, which is wrong
    /// only when that send failed over along the ring or the owner rotated since; failover already
    /// counts against the validator that failed.
    pub(super) fn blame_non_inclusion(
        &mut self,
        committee: &[BlsPublicKey],
        groups: impl IntoIterator<Item = (Address, u64, bool)>,
        now: Instant,
    ) -> Vec<BlsPublicKey> {
        let mut tripped = Vec::new();
        for (sender, owner, resent) in groups {
            if !resent {
                continue;
            }
            let peer = self.last_acked_by(sender).unwrap_or_else(|| committee[owner as usize]);
            if self.on_not_included(&peer, sender, now) {
                tripped.push(peer);
            }
        }
        tripped
    }

    /// The validator that acked `sender`'s last delivery, if it is still pending.
    fn last_acked_by(&self, sender: Address) -> Option<BlsPublicKey> {
        self.deliveries.get(&sender).map(|delivery| delivery.peer)
    }

    /// Whether a group may be delivered to `peer` at `now`, `probe` naming its sender when it may
    /// serve as a probe ([`Breaker::admit`]).
    pub(super) fn admit(
        &mut self,
        peer: &BlsPublicKey,
        probe: Option<Address>,
        now: Instant,
    ) -> bool {
        self.breakers.get_mut(peer).is_none_or(|breaker| breaker.admit(now, probe))
    }

    /// Record a re-sent group blamed on `peer` at `now`; returns whether this opened the breaker
    /// ([`Breaker::not_included`]).
    pub(super) fn on_not_included(
        &mut self,
        peer: &BlsPublicKey,
        sender: Address,
        now: Instant,
    ) -> bool {
        let breaker = self.breakers.entry(*peer).or_default();
        let was_holding = breaker.holds(now);
        *breaker = breaker.not_included(now, sender);
        !was_holding && breaker.holds(now)
    }

    /// Record a failed submit at `now`; returns whether this failure opened the breaker
    /// ([`Breaker::failed`]).
    pub(super) fn on_failure(
        &mut self,
        peer: &BlsPublicKey,
        error: &SubmitError,
        now: Instant,
    ) -> bool {
        let breaker = self.breakers.entry(*peer).or_default();
        let was_holding = breaker.holds(now);
        *breaker = breaker.failed(error, now);
        !was_holding && breaker.holds(now)
    }

    /// Whether `peer`'s breaker holds it out at `now`.
    pub(super) fn holds(&self, peer: &BlsPublicKey, now: Instant) -> bool {
        self.breakers.get(peer).is_some_and(|breaker| breaker.holds(now))
    }

    /// Every connected validator to try for one message, in ring order from the owner's slot, with
    /// any whose breaker holds moved to the back as a last resort.
    pub(super) fn candidates(
        &self,
        owner: u64,
        committee: &[BlsPublicKey],
        connected: &[BlsPublicKey],
        now: Instant,
    ) -> Vec<BlsPublicKey> {
        let mut ranked: Vec<BlsPublicKey> = ring_walk(owner, committee.len() as u64)
            .map(|slot| committee[slot as usize])
            .filter(|peer| connected.contains(peer))
            .collect();
        // Stable: ring order survives among validators sharing a breaker state. A held breaker
        // sorts last, so it is only a last resort where a real send is its probe.
        ranked.sort_by_key(|peer| self.holds(peer, now));
        ranked
    }
}
