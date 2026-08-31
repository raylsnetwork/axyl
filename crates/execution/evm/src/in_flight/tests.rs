use super::*;
use proptest::prelude::*;

fn hash(n: u64) -> TxHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    TxHash::from(bytes)
}

/// A stale ack is terminal: never due again however far time and the anchor advance, not
/// revived by a racing forward mark, released only by the membership reconcile. Fails
/// against release-on-ack semantics, where the hash reads unmarked and due.
#[test]
fn acked_stale_suppresses_resends_until_released_by_membership() {
    let tracker = InFlightTracker::with_fresh_metrics();
    let marks = tracker.arm_forwarding(DuePolicy {
        after: Duration::from_secs(10),
        backoff_shift_cap: 4,
        min_anchor_advance: 20,
    });
    let now = Instant::now();
    marks.mark_forwarded([hash(1)], now, 0);
    marks.mark_acked_stale([hash(1)]);

    let late = now + Duration::from_secs(3600);
    assert!(marks.is_forwarded(&hash(1)));
    assert!(!marks.is_due(&hash(1), late, u64::MAX));
    // a racing forward mark does not downgrade the ack
    marks.mark_forwarded([hash(1)], late, u64::MAX);
    assert!(!marks.is_due(&hash(1), late + Duration::from_secs(3600), u64::MAX));
    // execution pruning the transaction from the pool is the release path
    tracker.release_in_flight([hash(1)]);
    assert!(marks.is_due(&hash(1), late, 0));
}

#[test]
fn mark_and_query_roundtrip() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight([hash(1), hash(2)]);
    assert!(tracker.is_in_flight(&hash(1)) && tracker.is_in_flight(&hash(2)));
    assert!(!tracker.is_in_flight(&hash(3)));
    let tracked = tracker.tracked_hashes();
    assert_eq!(tracked.len(), 2);
    // the copy is point-in-time: later mutations do not affect it
    tracker.release_in_flight([hash(1)]);
    assert!(tracked.contains(&hash(1)));
    assert!(!tracker.is_in_flight(&hash(1)));
    assert_eq!(tracker.len(), 1);
}

#[test]
fn release_ignores_unknown_hashes() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight([hash(1)]);
    tracker.release_in_flight([hash(1), hash(99)]);
    assert!(tracker.is_empty());
}

#[test]
fn clones_share_one_set() {
    let tracker = InFlightTracker::new();
    let clone = tracker.clone();
    tracker.mark_in_flight([hash(7)]);
    assert!(clone.is_in_flight(&hash(7)));
    clone.clear();
    assert!(tracker.is_empty());
}

#[test]
fn sweep_respects_ttl() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight([hash(1), hash(2)]);
    // a generous TTL keeps fresh entries
    assert_eq!(tracker.sweep_expired(Duration::from_secs(60)), 0);
    assert_eq!(tracker.len(), 2);
    // a zero TTL expires everything
    assert_eq!(tracker.sweep_expired(Duration::ZERO), 2);
    assert!(tracker.is_empty());
}

#[test]
fn clear_empties_the_set() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight((0..10).map(hash));
    tracker.clear();
    assert!(tracker.is_empty());
    assert!(tracker.tracked_hashes().is_empty());
}

/// The sweep is a strict age-vs-TTL comparison: an entry younger than the TTL survives the
/// sweep, an entry older is dropped. The TTL is a floor, never a target - shrinking it only
/// trades batch bytes (duplicates skip deterministically at execution), but a sweep that
/// fires early would release txs whose batch is still in flight.
#[test]
fn sweep_drops_only_entries_older_than_ttl() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight([hash(1)]);
    std::thread::sleep(Duration::from_millis(120));
    tracker.mark_in_flight([hash(2)]);

    // hash(1) is ~120ms old, hash(2) fresh: a 80ms TTL sweeps exactly the aged entry
    assert_eq!(tracker.sweep_expired(Duration::from_millis(80)), 1);
    assert!(!tracker.is_in_flight(&hash(1)));
    assert!(tracker.is_in_flight(&hash(2)));

    // a TTL larger than every entry's age sweeps nothing
    assert_eq!(tracker.sweep_expired(Duration::from_secs(60)), 0);
    assert!(tracker.is_in_flight(&hash(2)));
}

/// Every release path (targeted release, TTL sweep, epoch clear) must bump the release
/// watch: it is the builder's only wake-up signal for re-selectable txs. No-op releases
/// must not bump it, or the builder spins on wakes that select nothing.
#[test]
fn release_paths_bump_the_release_watch() {
    let tracker = InFlightTracker::new();
    let events = tracker.release_events();
    let epoch = || *events.borrow();
    assert_eq!(epoch(), 0);

    // marking is not a release
    tracker.mark_in_flight([hash(1), hash(2), hash(3)]);
    assert_eq!(epoch(), 0);

    tracker.release_in_flight([hash(1)]);
    assert_eq!(epoch(), 1);

    // releasing only unknown hashes is a no-op
    tracker.release_in_flight([hash(99)]);
    assert_eq!(epoch(), 1);

    assert_eq!(tracker.sweep_expired(Duration::ZERO), 2);
    assert_eq!(epoch(), 2);

    // sweeping an empty set is a no-op
    assert_eq!(tracker.sweep_expired(Duration::ZERO), 0);
    assert_eq!(epoch(), 2);

    tracker.mark_in_flight([hash(4)]);
    tracker.clear();
    assert_eq!(epoch(), 3);

    // clearing an empty set is a no-op
    tracker.clear();
    assert_eq!(epoch(), 3);
}

/// The counters are deltas of the set, not of the caller's input, so marked minus the three
/// release counters always equals the live set size - and the gauge. A re-mark of a tracked
/// hash and a release of an untracked one must both count as nothing, or the identity drifts
/// and TTL churn can no longer be separated from healthy execution-driven releases.
#[test]
fn counter_deltas_reconcile_with_the_gauge() {
    let tracker = InFlightTracker::with_fresh_metrics();
    tracker.mark_in_flight((0..5).map(hash));
    // hash(0) and hash(1) are already tracked: only hash(5) is a new mark
    tracker.mark_in_flight([hash(0), hash(1), hash(5)]);
    // hash(99) was never marked: only hash(0) is a real release
    tracker.release_in_flight([hash(0), hash(99)]);

    std::thread::sleep(Duration::from_millis(80));
    tracker.mark_in_flight([hash(6)]);
    // everything but the fresh hash(6) is older than the TTL
    assert_eq!(tracker.sweep_expired(Duration::from_millis(40)), 5);

    tracker.mark_in_flight([hash(7), hash(8)]);
    tracker.clear();
    tracker.mark_in_flight([hash(9)]);

    assert_eq!(tracker.metrics().counts(), (10, 1, 5, 3));
    assert_eq!(tracker.len(), 1);
    assert_eq!(tracker.metrics().outstanding(), 1);
    assert_eq!(tracker.metrics().gauge(), 1);
}

/// An unarmed tracker must not sweep: between roles (CvvInactive epochs, pre-first-epoch)
/// no policy is installed, and a sweep with no owner would release marks on a schedule
/// nobody chose.
#[test]
fn sweep_due_is_a_no_op_unarmed() {
    let tracker = InFlightTracker::new();
    tracker.mark_in_flight([hash(1), hash(2)]);
    assert_eq!(tracker.sweep_due(u64::MAX), 0);
    assert_eq!(tracker.len(), 2);
}

/// Arming for sealing installs the flat sweep; `clear` disarms it again, so a policy can
/// never outlive the epoch-scoped owner that chose it.
#[test]
fn arm_sealing_installs_the_sweep_and_clear_disarms_it() {
    let tracker = InFlightTracker::new();
    let marks = tracker.arm_sealing(DuePolicy::ttl(Duration::ZERO));
    marks.mark([hash(1)]);
    assert_eq!(tracker.sweep_due(0), 1);
    assert!(tracker.is_empty());

    tracker.clear();
    tracker.mark_in_flight([hash(2)]);
    assert_eq!(tracker.sweep_due(u64::MAX), 0, "clear must disarm the sweep");
    assert!(tracker.is_in_flight(&hash(2)));
}

/// Arming for forwarding installs NO sweep: forward marks are re-driven by the forwarder's
/// own due check, and a flat sweep releasing them would erase the backoff state and bring
/// back the re-gossip amplification the backoff exists to stop.
#[test]
fn arm_forwarding_never_installs_a_sweep() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(DuePolicy {
        after: Duration::ZERO,
        backoff_shift_cap: 4,
        min_anchor_advance: 0,
    });
    forward.mark_forwarded([hash(1)], Instant::now(), 0);
    assert_eq!(tracker.sweep_due(u64::MAX), 0);
    assert!(tracker.is_in_flight(&hash(1)));
}

/// The forward policy the due tests exercise: 10s base, x16 backoff cap, 20-block anchor
/// margin (the production shape).
fn forward_policy() -> DuePolicy {
    DuePolicy { after: Duration::from_secs(10), backoff_shift_cap: 4, min_anchor_advance: 20 }
}

/// A forwarded hash is not due again inside the base window, while an unmarked hash is
/// always due.
#[test]
fn published_hash_is_not_due_within_the_window() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    let now = Instant::now();

    assert!(forward.is_due(&hash(1), now, 0), "an unmarked hash is always due");
    forward.mark_forwarded([hash(1)], now, 0);
    assert!(!forward.is_due(&hash(1), now + Duration::from_secs(1), u64::MAX));
}

/// Wall clock alone is not loss: past the window with a stalled anchor nothing is due,
/// because the node cannot yet know whether the transaction landed. The margin must be
/// fully cleared, not merely started.
#[test]
fn stalled_anchor_holds_a_due_hash_back() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    let now = Instant::now();
    forward.mark_forwarded([hash(1)], now, 100);

    let late = now + Duration::from_secs(100);
    assert!(!forward.is_due(&hash(1), late, 100), "a stalled anchor must hold the hash");
    assert!(!forward.is_due(&hash(1), late, 119), "the margin must be fully cleared");
    assert!(forward.is_due(&hash(1), late, 120));
}

/// Each resend doubles the wait before the next one, so a transaction stuck behind a deep
/// inclusion backlog is not re-gossiped every base window.
#[test]
fn restamp_backs_off_exponentially() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    let now = Instant::now();
    forward.mark_forwarded([hash(1)], now, 0);

    let t1 = now + Duration::from_secs(10);
    assert!(forward.is_due(&hash(1), t1, 20));
    forward.mark_forwarded([hash(1)], t1, 20);

    // the base window is no longer enough; the second re-send needs double
    assert!(
        !forward.is_due(&hash(1), t1 + Duration::from_secs(10), 40),
        "second re-send fired at the base window instead of backing off"
    );
    assert!(forward.is_due(&hash(1), t1 + Duration::from_secs(20), 40));
}

/// A release (the tx left the pool) erases the mark entirely: re-entering the pool is a
/// fresh mark with fresh backoff, so the tracker follows pool membership.
#[test]
fn release_resets_forward_state() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    let now = Instant::now();
    forward.mark_forwarded([hash(1)], now, 0);

    tracker.release_in_flight([hash(1)]);
    assert!(forward.is_due(&hash(1), now, 0), "a released hash is a fresh claim");
}

/// An epoch exit under a forwarding arm keeps the marks: a forwarded transaction's
/// delivery is not invalidated by an epoch boundary (validator pools persist across it),
/// and clearing would schedule a full-pool re-publish with the backoff state erased. The
/// arm itself still drops, so the next epoch re-arms cleanly.
#[test]
fn clear_keeps_forward_marks_and_disarms() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    forward.mark_forwarded([hash(1), hash(2)], Instant::now(), 0);

    tracker.clear();
    assert_eq!(tracker.len(), 2, "forward marks must survive the epoch exit");
    assert!(!forward.is_due(&hash(1), Instant::now(), 0), "the backoff state must survive too");

    // a sealing clear is unchanged: marks go, sweep disarms
    let marks = tracker.arm_sealing(DuePolicy::ttl(Duration::ZERO));
    marks.mark([hash(3)]);
    tracker.clear();
    assert!(tracker.is_empty(), "a sealing-armed clear drops everything");
    assert_eq!(tracker.sweep_due(u64::MAX), 0, "and disarms the sweep");
}

/// A promotion carries an Observer epoch's forward marks across the boundary (clear() keeps
/// them under Armed::Forwarding), but arm_sealing must start the sealing set empty: a leftover
/// mark - especially AckedStale, which the TTL sweep never releases - would make the newly
/// promoted validator's builder skip a transaction it now solely owns. Fails against an
/// arm_sealing that only sets the sweep policy.
#[test]
fn arm_sealing_clears_forward_marks_left_by_a_promotion() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    forward.mark_forwarded([hash(1)], Instant::now(), 0);
    forward.mark_acked_stale([hash(2)]);
    tracker.clear(); // epoch boundary: Armed::Forwarding keeps the forward marks
    assert_eq!(tracker.len(), 2, "clear keeps forward marks across the boundary");

    // Observer -> CvvActive promotion: the builder arms for sealing the next epoch
    let _seal = tracker.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    assert!(!tracker.is_in_flight(&hash(1)), "a Sent mark must not survive into sealing");
    assert!(!tracker.is_in_flight(&hash(2)), "an AckedStale mark must not survive into sealing");
}

/// `is_forwarded` distinguishes a first send from a re-send: the catch-up gate blocks only
/// re-sends, so new transactions flow even while the node is behind.
#[test]
fn is_forwarded_tracks_first_sends() {
    let tracker = InFlightTracker::new();
    let forward = tracker.arm_forwarding(forward_policy());
    assert!(!forward.is_forwarded(&hash(1)));
    forward.mark_forwarded([hash(1)], Instant::now(), 0);
    assert!(forward.is_forwarded(&hash(1)));
    tracker.release_in_flight([hash(1)]);
    assert!(!forward.is_forwarded(&hash(1)), "a released hash is a fresh first send");
}

/// The batched membership release keeps only the live set, counts as a reconcile release,
/// and bumps the release watch exactly once; releasing nothing bumps nothing.
#[test]
fn release_mined_retains_only_the_live_set() {
    let tracker = InFlightTracker::new();
    let events = tracker.release_events();
    tracker.mark_in_flight([hash(1), hash(2), hash(3)]);

    let live = [hash(2)].into_iter().collect();
    assert_eq!(tracker.release_mined(&live), 2);
    assert!(tracker.is_in_flight(&hash(2)));
    assert_eq!(tracker.len(), 1);
    assert_eq!(*events.borrow(), 1);

    // everything tracked is live: a no-op, and no spurious wake
    assert_eq!(tracker.release_mined(&live), 0);
    assert_eq!(*events.borrow(), 1);
}

/// `mark_forwarded` participates in the conservation law: a fresh mark counts as marked,
/// a resend counts as nothing, so `marked - releases = len` holds across both roles.
#[test]
fn mark_forwarded_counts_set_deltas_only() {
    let tracker = InFlightTracker::with_fresh_metrics();
    let forward = tracker.arm_forwarding(forward_policy());
    let now = Instant::now();

    forward.mark_forwarded([hash(1), hash(2)], now, 0);
    // hash(1) is a resend: only hash(3) is a new mark
    forward.mark_forwarded([hash(1), hash(3)], now, 0);

    let (marked, _, _, _) = tracker.metrics().counts();
    assert_eq!(marked, 3);
    assert_eq!(tracker.metrics().outstanding(), 3);
    assert_eq!(tracker.metrics().gauge(), 3);
}

/// Hashes the concurrent property draws from; small enough that the threads collide.
const UNIVERSE: u8 = 12;

#[derive(Clone, Debug)]
enum Op {
    Mark(Vec<u8>),
    Release(Vec<u8>),
    /// `sweep_expired(ZERO)`: expires everything currently tracked.
    SweepAll,
    /// `sweep_expired(1h)`: expires nothing, so it must be a pure no-op.
    SweepNone,
    Clear,
}

impl Op {
    fn apply(&self, tracker: &InFlightTracker) {
        match self {
            Op::Mark(ids) => tracker.mark_in_flight(ids.iter().map(|id| hash(*id as u64))),
            Op::Release(ids) => tracker.release_in_flight(ids.iter().map(|id| hash(*id as u64))),
            Op::SweepAll => drop(tracker.sweep_expired(Duration::ZERO)),
            Op::SweepNone => drop(tracker.sweep_expired(Duration::from_secs(3600))),
            Op::Clear => tracker.clear(),
        }
    }
}

fn any_op() -> impl Strategy<Value = Op> {
    let ids = || prop::collection::vec(0..UNIVERSE, 0..4);
    prop_oneof![
        4 => ids().prop_map(Op::Mark),
        3 => ids().prop_map(Op::Release),
        1 => Just(Op::SweepAll),
        1 => Just(Op::SweepNone),
        1 => Just(Op::Clear),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    /// Up to four writers race one tracker (builder marks, canonical task releases, sweep
    /// expires, epoch transition clears); the per-method tests never interleave them.
    ///
    /// The final membership is not interleaving-independent, so the oracle is the one thing
    /// that is: every mutation derives its counter delta from set sizes read under the one
    /// write lock, so the deltas telescope and `marked - reconcile - ttl - clear` equals the
    /// live set size under *every* linearization. That is the shadow model, checked exactly,
    /// alongside the interleaving-free structural invariants (membership agrees with
    /// `is_in_flight`, the set never exceeds the universe, the release watch never goes
    /// backwards, and a final `clear` is absorbing and leaves a working tracker).
    #[test]
    fn concurrent_writers_preserve_the_release_conservation_law(
        per_thread in prop::collection::vec(
            prop::collection::vec(any_op(), 0..12),
            2..=3usize,
        ),
    ) {
        let tracker = InFlightTracker::with_fresh_metrics();
        std::thread::scope(|scope| {
            for ops in &per_thread {
                let tracker = tracker.clone();
                scope.spawn(move || {
                    let events = tracker.release_events();
                    let mut last = *events.borrow();
                    for op in ops {
                        op.apply(&tracker);
                        let seen = *events.borrow();
                        assert!(seen >= last, "release watch fell {last} -> {seen}");
                        last = seen;
                    }
                });
            }
        });

        let len = tracker.len();
        prop_assert!(len <= UNIVERSE as usize);
        prop_assert_eq!(tracker.metrics().outstanding(), len as i64);
        // the gauge telescopes from the same per-op deltas as the counters, so it tracks the
        // live length exactly even after concurrent churn
        prop_assert_eq!(tracker.metrics().gauge(), len as i64);

        let tracked = tracker.tracked_hashes();
        prop_assert_eq!(tracked.len(), len);
        for id in 0..UNIVERSE {
            let hash = hash(id as u64);
            prop_assert_eq!(tracker.is_in_flight(&hash), tracked.contains(&hash));
        }
        // a final clear empties the set and stays empty under re-reads
        tracker.clear();
        prop_assert!(tracker.is_empty());
        prop_assert!(tracker.tracked_hashes().is_empty());
        prop_assert_eq!(tracker.len(), 0);
        prop_assert_eq!(tracker.metrics().outstanding(), 0);
        prop_assert_eq!(tracker.metrics().gauge(), 0);

        // the tracker still behaves after the churn, and the release watch is one bump per
        // release *call*, not per released hash: it wakes the builder, it does not count txs
        tracker.mark_in_flight([hash(0), hash(1), hash(2)]);
        let before = *tracker.release_events().borrow();
        tracker.release_in_flight([hash(0), hash(1)]);
        prop_assert_eq!(*tracker.release_events().borrow(), before + 1);
        prop_assert_eq!(tracker.len(), 1);
        prop_assert!(tracker.is_in_flight(&hash(2)));
        prop_assert!(!tracker.is_in_flight(&hash(0)));
        prop_assert_eq!(tracker.metrics().outstanding(), 1);
        prop_assert_eq!(tracker.metrics().gauge(), 1);
    }
}

/// `release_dropped` frees exactly the given hashes and counts them under its own release
/// cause, leaving unrelated marks in place.
#[test]
fn release_dropped_frees_only_the_given_hashes() {
    let tracker = InFlightTracker::new();
    let kept = TxHash::random();
    let dropped = TxHash::random();
    tracker.mark_in_flight([kept, dropped]);
    tracker.release_dropped([dropped]);
    assert_eq!(tracker.len(), 1, "only the dropped hash is released");
    tracker.release_dropped([dropped]);
    assert_eq!(tracker.len(), 1, "an already-released hash is a no-op");
}

/// A sealing snapshot round-trips through the next boot's matching arm: marks reinstalled
/// with rebased clocks, sorted persisted output, and nothing live until the arm.
#[test]
fn snapshot_round_trips_through_a_matching_sealing_arm() {
    let source = InFlightTracker::with_fresh_metrics();
    let seal = source.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    seal.mark([hash(2), hash(1)]);
    let backup = source.snapshot().expect("an armed tracker snapshots");
    assert_eq!(backup.role, MarkRole::Sealing);
    assert_eq!(backup.marks.len(), 2);
    assert!(backup.marks.windows(2).all(|w| w[0].hash < w[1].hash), "sorted by hash");

    let restored = InFlightTracker::with_fresh_metrics();
    restored.stash_restore(backup);
    assert!(restored.is_empty(), "the stash must not touch the live set");
    let _seal = restored.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    assert_eq!(restored.len(), 2, "the matching arm installs the stash");
    assert!(restored.is_in_flight(&hash(1)) && restored.is_in_flight(&hash(2)));
}

/// An arming of the other role discards the stash: it cannot honor marks whose semantics
/// belong to a role the node no longer runs.
#[test]
fn restored_marks_are_discarded_on_a_role_change() {
    let source = InFlightTracker::with_fresh_metrics();
    let seal = source.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    seal.mark([hash(1)]);
    let backup = source.snapshot().expect("armed");

    let restored = InFlightTracker::with_fresh_metrics();
    restored.stash_restore(backup);
    let _fwd = restored.arm_forwarding(DuePolicy::ttl(Duration::from_secs(10)));
    assert!(restored.is_empty(), "a sealing stash must not survive a forwarding arm");
}

/// The promotion-safety clear still wipes cross-role residue while the matching stash
/// installs: an Observer -> CvvActive boot keeps only what the sealing role owns.
#[test]
fn arm_sealing_clears_residue_and_installs_the_matching_stash() {
    let tracker = InFlightTracker::with_fresh_metrics();
    tracker.mark_in_flight([hash(9)]);
    tracker.stash_restore(MarkBackup {
        version: MARK_BACKUP_VERSION,
        role: MarkRole::Sealing,
        marks: vec![SavedMark { hash: hash(1), kind: SavedMarkKind::Sent { attempts: 0 } }],
    });
    let _seal = tracker.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    assert!(!tracker.is_in_flight(&hash(9)), "cross-role residue is cleared");
    assert!(tracker.is_in_flight(&hash(1)), "the matching stash installs");
}

/// Forward backoff survives the round trip: a restored mark reads as forwarded (no
/// first-send flood), its attempts keep the backoff window, and acked-stale stays
/// never-due; an acked-stale mark also snapshots back out.
#[test]
fn forward_backoff_survives_the_round_trip() {
    let policy =
        DuePolicy { after: Duration::from_secs(10), backoff_shift_cap: 4, min_anchor_advance: 0 };
    let tracker = InFlightTracker::with_fresh_metrics();
    tracker.stash_restore(MarkBackup {
        version: MARK_BACKUP_VERSION,
        role: MarkRole::Forwarding,
        marks: vec![
            SavedMark { hash: hash(1), kind: SavedMarkKind::Sent { attempts: 3 } },
            SavedMark { hash: hash(2), kind: SavedMarkKind::AckedStale },
        ],
    });
    let fwd = tracker.arm_forwarding(policy);
    assert!(fwd.is_forwarded(&hash(1)), "a restored mark is a re-send, not a first send");
    assert!(
        !fwd.is_due(&hash(1), Instant::now(), u64::MAX),
        "the restored backoff window holds at the rebased stamp"
    );
    assert!(
        !fwd.is_due(&hash(2), Instant::now() + Duration::from_secs(3600), u64::MAX),
        "acked-stale is never due"
    );
    let backup = tracker.snapshot().expect("armed");
    assert_eq!(backup.role, MarkRole::Forwarding);
    assert!(backup.marks.iter().any(|m| m.kind == SavedMarkKind::AckedStale));
}

/// An unarmed tracker has no role whose marks mean anything: no snapshot.
#[test]
fn snapshot_is_none_when_unarmed() {
    let tracker = InFlightTracker::with_fresh_metrics();
    tracker.mark_in_flight([hash(1)]);
    assert!(tracker.snapshot().is_none());
}

/// A clear with nothing armed keeps the stash: the epoch that ended had no role armed, so the
/// marks belong to the next role to arm. A validator boots CvvInactive and promotes to
/// CvvActive through exactly this clear before its builder ever arms - dropping the stash
/// there silently discards every restored seal mark.
#[test]
fn clear_with_nothing_armed_keeps_the_stash() {
    let tracker = InFlightTracker::with_fresh_metrics();
    tracker.stash_restore(MarkBackup {
        version: MARK_BACKUP_VERSION,
        role: MarkRole::Sealing,
        marks: vec![SavedMark { hash: hash(1), kind: SavedMarkKind::Sent { attempts: 0 } }],
    });
    tracker.clear();
    let _seal = tracker.arm_sealing(DuePolicy::ttl(Duration::from_secs(60)));
    assert!(
        tracker.is_in_flight(&hash(1)),
        "an actor-less clear must not eat the next actor's stash"
    );
}
