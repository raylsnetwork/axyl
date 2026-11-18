//! Admission of inbound requests that cost the node real work.
//!
//! A request type the application declares as an admission class takes a permit in the swarm
//! loop before it is forwarded, and the application keeps the permit until the work behind the
//! request ends. A refused request is answered with the response type's busy answer and never
//! reaches the application.
//!
//! Each class has its own pools: a share per committee member, a pool for the committee as a
//! whole, and a small pool shared by every other peer. The pools live for the node's life, so
//! jobs left over from an earlier epoch still count; the per-member shares follow the committee
//! and are rebuilt at every epoch from the current and the previous committee.
//!
//! The application declares which of its requests belong to which [`AdmissionClass`] through
//! [`RLMessage::admission_class`], its busy answer through [`RLMessage::busy`], and each
//! class's values through the [`ClassValues`] it hands the network when the swarm is built.
//!
//! [`RLMessage::admission_class`]: crate::RLMessage::admission_class
//! [`RLMessage::busy`]: crate::RLMessage::busy

use crate::NetworkMetrics;
use rayls_infrastructure_types::BlsPublicKey;
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, RandomState},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

/// Smallest committee pool, grown so every member can use its share.
/// It only refuses while jobs from an earlier epoch are still running.
pub const JOBS_COMMITTEE_FLOOR: usize = 64;
/// Jobs one outside peer may have running at once, in any class.
/// Honest nodes send each peer one request at a time.
pub const JOBS_PER_OUTSIDER: usize = 1;

/// The classes of request the network admits per peer, across both networks.
///
/// A class indexes its pools by its discriminant, so admitting a request costs no hashing.
/// A new class adds a variant and its name here, the requests it covers in the application's
/// [`RLMessage::admission_class`], and its values in the list the application hands the swarm.
///
/// [`RLMessage::admission_class`]: crate::RLMessage::admission_class
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClass {
    /// Missing-certificates requests on the primary: each one is a storage scan on a blocking
    /// thread.
    MissingCertificates,
}

impl AdmissionClass {
    /// The name of this class in the refusal metric.
    pub fn name(self) -> &'static str {
        match self {
            Self::MissingCertificates => "missing_certificates",
        }
    }

    /// The index of this class's pools.
    fn index(self) -> usize {
        self as usize
    }
}

/// The values of one class.
#[derive(Debug, Clone, Copy)]
pub struct ClassValues {
    /// Jobs one committee member, or last epoch's member, may have running at once.
    pub jobs_per_peer: NonZeroUsize,
    /// Jobs every peer outside both committees may have running at once, in total.
    pub jobs_others: NonZeroUsize,
}

/// The limits of every class, shared by every epoch of one swarm.
#[derive(Debug)]
pub struct RequestLimits {
    /// Each class's node-wide pools and its member share, at the class's index.
    classes: Vec<Option<(ClassLimits, NonZeroUsize)>>,
    /// Counts refusals.
    metrics: Arc<NetworkMetrics>,
    /// The swarm these limits belong to, for the refusal metric.
    swarm: &'static str,
}

impl RequestLimits {
    /// Limits for `classes`, counted on `metrics` under `swarm`.
    pub fn new(
        classes: impl IntoIterator<Item = (AdmissionClass, ClassValues)>,
        metrics: Arc<NetworkMetrics>,
        swarm: &'static str,
    ) -> Self {
        let mut table: Vec<Option<(ClassLimits, NonZeroUsize)>> = Vec::new();
        for (class, values) in classes {
            if table.len() <= class.index() {
                table.resize_with(class.index() + 1, || None);
            }
            table[class.index()] =
                Some((ClassLimits::new(values.jobs_others), values.jobs_per_peer));
        }
        Self { classes: table, metrics, swarm }
    }

    /// The slots of one epoch, with a member share for each of `members`.
    ///
    /// Members are the current committee and the previous one, whose members may still be
    /// finishing their last epoch. Every other peer draws on the shared pool.
    pub fn for_members(&self, members: &HashSet<BlsPublicKey>) -> RequestSlots {
        let classes = self
            .classes
            .iter()
            .map(|class| {
                class
                    .as_ref()
                    .map(|(limits, jobs_per_peer)| limits.for_members(members, *jobs_per_peer))
            })
            .collect();
        RequestSlots { classes, metrics: self.metrics.clone(), swarm: self.swarm }
    }

    /// The limits of one class, if declared.
    pub fn class(&self, class: AdmissionClass) -> Option<&ClassLimits> {
        self.classes.get(class.index()).and_then(Option::as_ref).map(|(limits, _)| limits)
    }
}

/// The slots of every class for one epoch of one swarm.
#[derive(Debug)]
pub struct RequestSlots {
    /// Each class's slots, at the class's index.
    classes: Vec<Option<ClassSlots>>,
    /// Counts refusals.
    metrics: Arc<NetworkMetrics>,
    /// The swarm these slots belong to, for the refusal metric.
    swarm: &'static str,
}

impl RequestSlots {
    /// Take the permits for one job of `class` from `peer`, or name the full limit.
    ///
    /// Never waits. A refusal is counted under the class and the limit that refused it.
    /// A class this swarm was not given values for is refused with [BusyLimit::Undeclared].
    pub fn acquire(
        &self,
        class: AdmissionClass,
        peer: BlsPublicKey,
    ) -> Result<RequestPermits, BusyLimit> {
        self.classes
            .get(class.index())
            .and_then(Option::as_ref)
            .ok_or(BusyLimit::Undeclared)
            .and_then(|slots| slots.acquire(peer))
            .inspect_err(|limit| {
                self.metrics
                    .request_admission_busy
                    .with_label_values(&[class.name(), limit.label(), self.swarm])
                    .inc();
                debug!(
                    target: "network::admission",
                    %peer,
                    class = class.name(),
                    ?limit,
                    swarm = self.swarm,
                    "too many requests in progress"
                );
            })
    }
}

/// Concurrency limits of one request class, shared by every epoch.
#[derive(Debug)]
pub struct ClassLimits {
    /// Pool for committee peers.
    committee: Arc<Semaphore>,
    /// Permits the committee pool was made with, including any growth.
    committee_capacity: AtomicUsize,
    /// Smaller pool for every other peer, so observers cannot starve committee catch-up.
    outsiders: Arc<OutsiderSlots>,
}

impl ClassLimits {
    /// Peers outside both committees share `jobs_others` jobs at once.
    /// A full pool delays every one of them.
    pub fn new(jobs_others: NonZeroUsize) -> Self {
        Self {
            committee: Arc::new(Semaphore::new(JOBS_COMMITTEE_FLOOR)),
            committee_capacity: AtomicUsize::new(JOBS_COMMITTEE_FLOOR),
            outsiders: Arc::new(OutsiderSlots::new(jobs_others)),
        }
    }

    /// Job slots for one epoch: a pool per member.
    ///
    /// Each member may run `jobs_per_peer` jobs at once. Pools are rebuilt each
    /// epoch, so a member may briefly run twice that, still within the committee pool.
    pub fn for_members(
        &self,
        members: &HashSet<BlsPublicKey>,
        jobs_per_peer: NonZeroUsize,
    ) -> ClassSlots {
        let jobs_per_peer = jobs_per_peer.get();
        // Faulty members must never hold the whole pool.
        // It only grows, so jobs from an earlier epoch keep their permits.
        let needed = jobs_per_peer * members.len();
        // Racing callers each add only the part they grew.
        let before = self.committee_capacity.fetch_max(needed, Ordering::SeqCst);
        if needed > before {
            self.committee.add_permits(needed - before);
        }
        let per_peer =
            members.iter().map(|key| (*key, Arc::new(Semaphore::new(jobs_per_peer)))).collect();
        ClassSlots {
            committee: self.committee.clone(),
            outsiders: self.outsiders.clone(),
            per_peer,
        }
    }

    /// Free slots in the committee pool.
    pub fn free_committee_slots(&self) -> usize {
        self.committee.available_permits()
    }

    /// Slots held by outside peers' running jobs.
    pub fn tracked_outsiders(&self) -> usize {
        self.outsiders.held()
    }
}

/// Job slots of one request class for one epoch.
///
/// The members are fixed for the epoch, so the pools are made once and read without a lock.
#[derive(Debug)]
pub struct ClassSlots {
    /// Pool for committee peers, shared with every epoch.
    committee: Arc<Semaphore>,
    /// Pool for every other peer, shared with every epoch.
    outsiders: Arc<OutsiderSlots>,
    /// A pool per member of this and the previous committee.
    per_peer: HashMap<BlsPublicKey, Arc<Semaphore>>,
}

/// Marks a free slot in [OutsiderSlots].
const FREE_SLOT: u64 = 0;

/// The pool shared by outside peers, one slot per running job.
///
/// A slot holds a hash of the peer's key, salted per node.
/// A collision only refuses a request, which the requester retries.
#[derive(Debug)]
struct OutsiderSlots {
    /// The slots, claimed with compare-and-swap.
    slots: Box<[AtomicU64]>,
    /// Hashes peer keys with a salt random to this node.
    hasher: RandomState,
}

impl OutsiderSlots {
    fn new(size: NonZeroUsize) -> Self {
        Self {
            slots: (0..size.get()).map(|_| AtomicU64::new(FREE_SLOT)).collect(),
            hasher: RandomState::new(),
        }
    }

    /// The tag a peer's slots hold, never [FREE_SLOT].
    fn tag(&self, peer: &BlsPublicKey) -> u64 {
        self.hasher.hash_one(peer).max(FREE_SLOT + 1)
    }

    /// Slots held by the peer with `tag`.
    fn running(&self, tag: u64) -> usize {
        self.slots.iter().filter(|slot| slot.load(Ordering::SeqCst) == tag).count()
    }

    /// Claim a slot for the peer with `tag`, unless the pool or the peer is full.
    /// A slot freed behind the scan is missed, as if the request came earlier.
    fn claim(self: &Arc<Self>, tag: u64) -> Result<OutsiderJob, BusyLimit> {
        let index = self
            .slots
            .iter()
            .position(|slot| {
                slot.compare_exchange(FREE_SLOT, tag, Ordering::SeqCst, Ordering::SeqCst).is_ok()
            })
            .ok_or(BusyLimit::Others)?;
        let job = OutsiderJob { index, outsiders: self.clone() };
        // Claim first and count after, so racing claims cannot both pass.
        // If both see each other, both are refused and retry later.
        (self.running(tag) <= JOBS_PER_OUTSIDER).then_some(job).ok_or(BusyLimit::Outsider)
    }

    /// Slots in use.
    fn held(&self) -> usize {
        self.slots.iter().filter(|slot| slot.load(Ordering::SeqCst) != FREE_SLOT).count()
    }
}

/// The limit that refused a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusyLimit {
    /// A member, or last epoch's member, already runs its share of jobs.
    Member,
    /// The committee pool is full, with jobs from an earlier epoch.
    Committee,
    /// The outside peer already runs its share of jobs.
    Outsider,
    /// The pool shared by outside peers is full.
    Others,
    /// The class was never declared to this swarm, so it has no pools. The swarm loop
    /// refuses such a request like any other, so a code mismatch never runs an unlimited job.
    Undeclared,
}

impl BusyLimit {
    /// The label of this limit in the refusal metric.
    fn label(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Committee => "committee",
            Self::Outsider => "outsider",
            Self::Others => "others",
            Self::Undeclared => "undeclared",
        }
    }
}

/// The permits one job holds until it ends.
#[derive(Debug)]
pub enum RequestPermits {
    /// A member's own permit and its permit from the committee pool.
    Member { _peer: OwnedSemaphorePermit, _pool: OwnedSemaphorePermit },
    /// An outside peer's slot in the shared pool.
    Outsider { _job: OutsiderJob },
}

impl RequestPermits {
    /// Permits from a pool nothing else draws on, for tests of the work behind them.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn unbounded_for_tests() -> Self {
        let outsiders = Arc::new(OutsiderSlots::new(NonZeroUsize::MIN));
        let job = outsiders.claim(FREE_SLOT + 1).expect("a fresh pool has a free slot");
        Self::Outsider { _job: job }
    }
}

/// One running job of an outside peer, whose slot is freed on drop.
#[derive(Debug)]
pub struct OutsiderJob {
    /// The slot the job holds.
    index: usize,
    /// The slots it was claimed from.
    outsiders: Arc<OutsiderSlots>,
}

impl Drop for OutsiderJob {
    fn drop(&mut self) {
        self.outsiders.slots[self.index].store(FREE_SLOT, Ordering::SeqCst);
    }
}

impl ClassSlots {
    /// Take the permits for one job, or name the full limit without waiting.
    ///
    /// A committee peer takes a permit from its own pool and from the committee pool.
    /// Any other peer takes a slot in the shared pool, within its own limit.
    pub fn acquire(&self, peer: BlsPublicKey) -> Result<RequestPermits, BusyLimit> {
        let Some(own) = self.per_peer.get(&peer) else {
            return self.acquire_outsider(peer);
        };
        // A member at its limit is refused before touching the pool.
        // If the pool is full, the member's permit is dropped and so given back.
        let peer = own.clone().try_acquire_owned().map_err(|_| BusyLimit::Member)?;
        let pool = self.committee.clone().try_acquire_owned().map_err(|_| BusyLimit::Committee)?;
        Ok(RequestPermits::Member { _peer: peer, _pool: pool })
    }

    /// Take a slot in the shared pool for one job of an outside peer.
    fn acquire_outsider(&self, peer: BlsPublicKey) -> Result<RequestPermits, BusyLimit> {
        let tag = self.outsiders.tag(&peer);
        // A peer at its limit is refused before it takes a shared slot.
        if self.outsiders.running(tag) >= JOBS_PER_OUTSIDER {
            return Err(BusyLimit::Outsider);
        }
        let job = self.outsiders.claim(tag)?;
        Ok(RequestPermits::Outsider { _job: job })
    }
}

#[cfg(test)]
#[path = "tests/admission_tests.rs"]
mod admission_tests;
