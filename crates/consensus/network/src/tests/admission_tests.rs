//! Tests for request admission: the pools of one class, and the table of every class.

use crate::{
    admission::{
        AdmissionClass, BusyLimit, ClassLimits, ClassValues, RequestLimits, JOBS_COMMITTEE_FLOOR,
        JOBS_PER_OUTSIDER,
    },
    NetworkMetrics,
};
use rand::{rngs::StdRng, SeedableRng};
use rayls_infrastructure_config::SyncConfig;
use rayls_infrastructure_types::{BlsKeypair, BlsPublicKey};
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

/// The key of the peer `seed` stands for.
fn key(seed: u64) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
}

/// A committee of `size` members, distinct from any other `first`.
fn committee(first: u64, size: usize) -> HashSet<BlsPublicKey> {
    (first..first + size as u64).map(key).collect()
}

/// The default values of the missing-certificates class.
fn default_values() -> ClassValues {
    let sync_config = SyncConfig::default();
    ClassValues {
        jobs_per_peer: sync_config.max_missing_cert_jobs_per_peer,
        jobs_others: sync_config.max_missing_cert_jobs_others,
    }
}

/// Slots follow the committee: a member that leaves keeps a member's
/// share for one epoch, while it may still be finishing its last one.
///
/// The committee pool is shared across epochs, so jobs from an old epoch still count.
#[test]
fn slots_follow_the_epoch_committee() {
    let values = default_values();
    let jobs_per_peer = values.jobs_per_peer;
    let (old, new, later) = (committee(0, 4), committee(100, 4), committee(200, 4));
    let limits = ClassLimits::new(values.jobs_others);
    let free_at_start = limits.free_committee_slots();
    let old_slots = limits.for_members(&old, jobs_per_peer);

    // A member of the old committee uses all of its slots.
    let former = key(0);
    let old_jobs: Vec<_> = (0..jobs_per_peer.get())
        .map(|_| old_slots.acquire(former).expect("slot in the old epoch"))
        .collect();
    let next = old_slots.acquire(former);
    assert!(matches!(next, Err(BusyLimit::Member)), "got {next:?}");

    // The next epoch's committee pool still counts the old jobs.
    let new_slots = limits.for_members(&new.union(&old).copied().collect(), jobs_per_peer);
    assert_eq!(limits.free_committee_slots(), free_at_start - jobs_per_peer.get());

    // For one epoch the former member keeps a member's share.
    let former_jobs: Vec<_> = (0..jobs_per_peer.get())
        .map(|_| new_slots.acquire(former).expect("slot for a former member"))
        .collect();
    let next = new_slots.acquire(former);
    assert!(matches!(next, Err(BusyLimit::Member)), "got {next:?}");

    // New members have their own slots.
    let member_job = new_slots.acquire(key(100)).expect("slot for a new member");

    // Two epochs on, the former member has an outsider's limit.
    let later_slots = limits.for_members(&later.union(&new).copied().collect(), jobs_per_peer);
    let outsider_jobs: Vec<_> = (0..JOBS_PER_OUTSIDER)
        .map(|_| later_slots.acquire(former).expect("slot in the shared pool"))
        .collect();
    let next = later_slots.acquire(former);
    assert!(matches!(next, Err(BusyLimit::Outsider)), "got {next:?}");

    // Slots come back once the jobs end.
    drop((old_jobs, former_jobs, member_job, outsider_jobs));
    assert_eq!(limits.free_committee_slots(), free_at_start);
}

/// One outside peer cannot fill the shared outsider pool.
#[test]
fn slots_limit_each_outsider() {
    let values = default_values();
    let limits = ClassLimits::new(values.jobs_others);
    let slots = limits.for_members(&committee(0, 4), values.jobs_per_peer);
    let outsider = |seed: u64| key(1000 + seed);

    // One outsider at its limit is refused while the pool has room.
    let greedy = outsider(0);
    let greedy_jobs: Vec<_> = (0..JOBS_PER_OUTSIDER)
        .map(|_| slots.acquire(greedy).expect("slot for an outsider"))
        .collect();
    let next = slots.acquire(greedy);
    assert!(matches!(next, Err(BusyLimit::Outsider)), "got {next:?}");

    // Other outsiders get the rest of the pool, until it is full.
    let other_jobs: Vec<_> = (1..)
        .take(values.jobs_others.get() - JOBS_PER_OUTSIDER)
        .map(|seed| slots.acquire(outsider(seed)).expect("slot in the shared pool"))
        .collect();
    let next = slots.acquire(outsider(u64::MAX / 2));
    assert!(matches!(next, Err(BusyLimit::Others)), "got {next:?}");

    // Only outsiders with a running job are tracked.
    assert_eq!(limits.tracked_outsiders(), 1 + other_jobs.len());
    drop(greedy_jobs);
    assert_eq!(limits.tracked_outsiders(), other_jobs.len());
    slots.acquire(greedy).expect("slot once the outsider's job ended");
    drop(other_jobs);
    assert_eq!(limits.tracked_outsiders(), 0);
}

/// Racing requests from one outside peer never run more jobs than its limit.
#[test]
fn slots_limit_each_outsider_under_races() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier,
    };

    const THREADS: usize = 8;
    const ROUNDS: usize = 20_000;

    let values = default_values();
    let limits = ClassLimits::new(values.jobs_others);
    let slots = limits.for_members(&committee(0, 4), values.jobs_per_peer);
    let greedy = key(1000);
    let running = AtomicUsize::new(0);
    let most_running = AtomicUsize::new(0);
    let served = AtomicUsize::new(0);
    let start = Barrier::new(THREADS);

    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                start.wait();
                for _ in 0..ROUNDS {
                    let Ok(job) = slots.acquire(greedy) else { continue };
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    most_running.fetch_max(now, Ordering::SeqCst);
                    // Hold the job a moment, so racing jobs overlap.
                    std::thread::yield_now();
                    running.fetch_sub(1, Ordering::SeqCst);
                    served.fetch_add(1, Ordering::SeqCst);
                    drop(job);
                }
            });
        }
    });

    assert!(served.load(Ordering::SeqCst) > 0, "no request was served");
    assert_eq!(most_running.load(Ordering::SeqCst), JOBS_PER_OUTSIDER);
    assert_eq!(limits.tracked_outsiders(), 0);
}

/// An outside peer at its limit takes no shared slot from other outside peers.
#[test]
fn at_limit_outsider_leaves_the_pool_alone() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const SPAMMERS: usize = 7;
    const HONEST_REQUESTS: usize = 20_000;

    let values = default_values();
    let limits = ClassLimits::new(values.jobs_others);
    let slots = limits.for_members(&committee(0, 4), values.jobs_per_peer);
    let (greedy, honest) = (key(1000), key(1001));
    let greedy_jobs: Vec<_> = (0..JOBS_PER_OUTSIDER)
        .map(|_| slots.acquire(greedy).expect("slot for an outsider"))
        .collect();
    let done = AtomicBool::new(false);
    let greedy_served = AtomicUsize::new(0);

    let honest_refused = std::thread::scope(|scope| {
        for _ in 0..SPAMMERS {
            scope.spawn(|| {
                while !done.load(Ordering::SeqCst) {
                    if slots.acquire(greedy).is_ok() {
                        greedy_served.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
        }
        let refused = (0..HONEST_REQUESTS).filter(|_| slots.acquire(honest).is_err()).count();
        done.store(true, Ordering::SeqCst);
        refused
    });

    assert_eq!(greedy_served.load(Ordering::SeqCst), 0, "an outsider went over its limit");
    assert_eq!(honest_refused, 0, "requests from an outsider at its limit took the pool");
    drop(greedy_jobs);
}

/// Members of a large committee and the previous one each get a share.
///
/// Otherwise faulty members could hold the whole committee pool.
#[test]
fn slots_grow_with_the_committee() {
    let values = default_values();
    let jobs_per_peer = values.jobs_per_peer;
    let committee_size = JOBS_COMMITTEE_FLOOR / jobs_per_peer.get() + 1;
    let previous = committee(0, committee_size);
    let current = committee(1000, committee_size);
    let next_committee = committee(2000, committee_size);
    let limits = ClassLimits::new(values.jobs_others);
    let slots = limits.for_members(&current.union(&previous).copied().collect(), jobs_per_peer);

    let jobs: Vec<_> = current
        .iter()
        .chain(previous.iter())
        .flat_map(|key| (0..jobs_per_peer.get()).map(move |_| *key))
        .map(|key| slots.acquire(key).expect("slot for every member"))
        .collect();
    assert_eq!(jobs.len(), 2 * committee_size * jobs_per_peer.get());
    assert_eq!(limits.free_committee_slots(), 0);

    // While those jobs run, a member of the next committee waits for the pool.
    let next_slots = limits.for_members(&next_committee, jobs_per_peer);
    let next = next_slots.acquire(key(2000));
    assert!(matches!(next, Err(BusyLimit::Committee)), "got {next:?}");

    // A member of both committees counts once.
    let overlap = ClassLimits::new(values.jobs_others);
    let _overlap_slots = overlap.for_members(&current, jobs_per_peer);
    assert_eq!(overlap.free_committee_slots(), committee_size * jobs_per_peer.get());
}

/// A member limit set above the default gives each member more jobs,
/// and the committee pool grows to match.
#[test]
fn slots_follow_the_configured_member_limit() {
    let values = default_values();
    let jobs_per_peer = values.jobs_per_peer.saturating_add(1);
    // Large enough that the pool must grow past its minimum size.
    let committee_size = JOBS_COMMITTEE_FLOOR / jobs_per_peer.get() + 1;
    let limits = ClassLimits::new(values.jobs_others);
    let slots = limits.for_members(&committee(0, committee_size), jobs_per_peer);
    assert_eq!(limits.free_committee_slots(), committee_size * jobs_per_peer.get());

    let member = key(0);
    let jobs: Vec<_> = (0..jobs_per_peer.get())
        .map(|_| slots.acquire(member).expect("slot within the configured limit"))
        .collect();
    let next = slots.acquire(member);
    assert!(matches!(next, Err(BusyLimit::Member)), "got {next:?}");
    drop(jobs);
}

/// A larger outsider pool admits that many outside peers at once.
#[test]
fn slots_follow_the_configured_outsider_pool() {
    let values = default_values();
    let jobs_others = values.jobs_others.saturating_add(2);
    let limits = ClassLimits::new(jobs_others);
    let slots = limits.for_members(&committee(0, 4), values.jobs_per_peer);

    let jobs: Vec<_> = (0..jobs_others.get() as u64)
        .map(|seed| slots.acquire(key(1000 + seed)).expect("slot in the configured pool"))
        .collect();
    let next = slots.acquire(key(u64::MAX / 2));
    assert!(matches!(next, Err(BusyLimit::Others)), "got {next:?}");
    drop(jobs);
}

/// A class draws on its own pools, a refusal is counted under its class, and a class the
/// swarm was not given is refused.
#[test]
fn classes_draw_on_their_own_pools_and_count_refusals() {
    const CERTS: AdmissionClass = AdmissionClass::MissingCertificates;

    let one = NonZeroUsize::MIN;
    let values = ClassValues { jobs_per_peer: one, jobs_others: one };
    let metrics = Arc::new(NetworkMetrics::default());
    let limits = RequestLimits::new([(CERTS, values)], metrics.clone(), "test");
    let slots = limits.for_members(&committee(0, 2));
    let (member, outsider) = (key(0), key(1000));

    // A member's and an outsider's jobs draw on the class's own pools.
    let cert_job = slots.acquire(CERTS, member).expect("a member's cert job");
    let next = slots.acquire(CERTS, member);
    assert!(matches!(next, Err(BusyLimit::Member)), "got {next:?}");
    let outsider_job = slots.acquire(CERTS, outsider).expect("an outsider's cert job");
    let next = slots.acquire(CERTS, outsider);
    assert!(matches!(next, Err(BusyLimit::Outsider)), "got {next:?}");

    // Each refusal is counted under its class, the limit that refused it, and the swarm.
    let refused = |limit: &str| {
        metrics
            .request_admission_busy
            .with_label_values(&["missing_certificates", limit, "test"])
            .get()
    };
    assert_eq!(refused("member"), 1);
    assert_eq!(refused("outsider"), 1);
    assert!(limits.class(CERTS).is_some());
    drop((cert_job, outsider_job));

    // A swarm given no values for a class has no pools for it, whatever the peer.
    let undeclared = RequestLimits::new([], metrics.clone(), "test");
    let slots = undeclared.for_members(&committee(0, 2));
    let next = slots.acquire(CERTS, member);
    assert!(matches!(next, Err(BusyLimit::Undeclared)), "got {next:?}");
    assert_eq!(refused("undeclared"), 1);
    assert!(undeclared.class(CERTS).is_none());
}
