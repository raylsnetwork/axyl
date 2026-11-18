//! Admission of peer requests that cost this node real work.
//!
//! Each request class has its own pools: a share per committee member, a pool
//! for the committee as a whole, and a small pool shared by every other peer.
//! The dispatch takes a permit before it hands a request to its handler, and the
//! handler keeps the permit until its work ends, so a cancelled request cannot
//! free a slot its job still uses.
//!
//! Only missing-certificate requests are admitted this way so far. A new class
//! adds a variant, a field in [RequestLimits] and [RequestAdmission], and its
//! own values; the pools and the refusal path are shared.

use rayls_consensus_primary_metrics::Metrics;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_types::{BlsPublicKey, Committee, Database};
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

use crate::ConsensusBus;

/// Smallest committee pool, grown so every member can use its share.
/// It only refuses while jobs from an earlier epoch are still running.
pub(crate) const JOBS_COMMITTEE_FLOOR: usize = 64;
/// Jobs one outside peer may have running at once, in any class.
/// Honest nodes send each peer one request at a time.
pub(crate) const JOBS_PER_OUTSIDER: usize = 1;

/// The classes of request this node admits through [RequestAdmission].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestClass {
    /// A missing-certificates request, served by a storage scan on a blocking thread.
    MissingCerts,
}

impl RequestClass {
    /// The label of this class in the refusal metric.
    fn label(self) -> &'static str {
        match self {
            Self::MissingCerts => "missing_certs",
        }
    }
}

/// Concurrency limits for every request class, shared by every epoch.
///
/// The node keeps one for its whole life, so jobs left over from an earlier epoch still count.
#[derive(Debug)]
pub(crate) struct RequestLimits {
    /// Limits on the missing-certificate jobs this node runs for its peers.
    missing_certs: ClassLimits,
}

impl RequestLimits {
    /// Peers outside both committees share `missing_cert_jobs_others` missing-certificate
    /// jobs at once.
    pub(crate) fn new(missing_cert_jobs_others: NonZeroUsize) -> Self {
        Self { missing_certs: ClassLimits::new(missing_cert_jobs_others) }
    }

    /// The limits of one class.
    pub(crate) fn class(&self, class: RequestClass) -> &ClassLimits {
        match class {
            RequestClass::MissingCerts => &self.missing_certs,
        }
    }
}

/// Permits for one epoch's requests, taken by the dispatch before a handler runs.
#[derive(Debug)]
pub(crate) struct RequestAdmission {
    /// Slots for missing-certificate jobs.
    missing_certs: ClassSlots,
    /// Counts refusals, by class and limit.
    metrics: Arc<Metrics>,
}

impl RequestAdmission {
    /// Slots for the epoch of `consensus_config`, drawn from the bus's node-wide limits.
    pub(crate) fn new<DB: Database>(
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
    ) -> Self {
        // Without a previous committee, former members are outsiders.
        let previous = consensus_config.previous_committee().map_or(&[][..], |prev| prev.keys());
        debug!(
            target: "primary::admission",
            epoch = consensus_config.committee().epoch(),
            committee = consensus_config.committee().size(),
            previous = previous.len(),
            "request slots for this epoch"
        );
        let sync_config = consensus_config.network_config().sync_config();
        let limits = consensus_bus.request_limits();
        let missing_certs = limits.class(RequestClass::MissingCerts).for_committees(
            consensus_config.committee(),
            previous,
            sync_config.max_missing_cert_jobs_per_peer,
        );
        Self { missing_certs, metrics: consensus_bus.primary_metrics() }
    }

    /// Take the permits for one job of `class` from `peer`, or name the full limit.
    ///
    /// Never waits. A refusal is counted under the class and the limit that refused it.
    pub(crate) fn acquire(
        &self,
        class: RequestClass,
        peer: BlsPublicKey,
    ) -> Result<RequestPermits, BusyLimit> {
        let slots = match class {
            RequestClass::MissingCerts => &self.missing_certs,
        };
        slots.acquire(peer).inspect_err(|limit| {
            self.metrics
                .node_metrics
                .request_admission_busy
                .with_label_values(&[class.label(), limit.label()])
                .inc();
            debug!(
                target: "primary::admission",
                %peer,
                ?class,
                ?limit,
                "too many requests in progress"
            );
        })
    }
}

/// Concurrency limits of one request class, shared by every epoch.
#[derive(Debug)]
pub(crate) struct ClassLimits {
    /// Pool for committee peers.
    committee: Arc<Semaphore>,
    /// Permits the committee pool was made with, including any growth.
    committee_capacity: AtomicUsize,
    /// Smaller pool for every other peer, so observers cannot starve committee catch-up.
    outsiders: Arc<OutsiderSlots>,
}

impl ClassLimits {
    /// Peers outside both committees share `jobs_others` jobs at once.
    /// A full pool delays every one of them (#357).
    pub(crate) fn new(jobs_others: NonZeroUsize) -> Self {
        Self {
            committee: Arc::new(Semaphore::new(JOBS_COMMITTEE_FLOOR)),
            committee_capacity: AtomicUsize::new(JOBS_COMMITTEE_FLOOR),
            outsiders: Arc::new(OutsiderSlots::new(jobs_others)),
        }
    }

    /// Job slots for one epoch: a pool per member of its committee and of
    /// `previous`, whose members may still be finishing their last epoch.
    ///
    /// Each member may run `jobs_per_peer` jobs at once. Pools are rebuilt each
    /// epoch, so a member may briefly run twice that, still within the committee pool.
    pub(crate) fn for_committees(
        &self,
        committee: &Committee,
        previous: &[BlsPublicKey],
        jobs_per_peer: NonZeroUsize,
    ) -> ClassSlots {
        let jobs_per_peer = jobs_per_peer.get();
        let members: HashSet<BlsPublicKey> = committee
            .authorities()
            .iter()
            .map(|authority| *authority.protocol_key())
            .chain(previous.iter().copied())
            .collect();
        // Faulty members must never hold the whole pool.
        // It only grows, so jobs from an earlier epoch keep their permits.
        let needed = jobs_per_peer * members.len();
        // Racing callers each add only the part they grew.
        let before = self.committee_capacity.fetch_max(needed, Ordering::SeqCst);
        if needed > before {
            self.committee.add_permits(needed - before);
        }
        let per_peer =
            members.into_iter().map(|key| (key, Arc::new(Semaphore::new(jobs_per_peer)))).collect();
        ClassSlots {
            committee: self.committee.clone(),
            outsiders: self.outsiders.clone(),
            per_peer,
        }
    }

    /// Free slots in the committee pool.
    #[cfg(test)]
    pub(crate) fn free_committee_slots(&self) -> usize {
        self.committee.available_permits()
    }

    /// Slots held by outside peers' running jobs.
    #[cfg(test)]
    pub(crate) fn tracked_outsiders(&self) -> usize {
        self.outsiders.held()
    }
}

/// Job slots of one request class for one epoch.
///
/// The committee is fixed for the epoch, so the pools are made once and read without a lock.
#[derive(Debug)]
pub(crate) struct ClassSlots {
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
    #[cfg(test)]
    fn held(&self) -> usize {
        self.slots.iter().filter(|slot| slot.load(Ordering::SeqCst) != FREE_SLOT).count()
    }
}

/// The limit that refused a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BusyLimit {
    /// A member, or last epoch's member, already runs its share of jobs.
    Member,
    /// The committee pool is full, with jobs from an earlier epoch.
    Committee,
    /// The outside peer already runs its share of jobs.
    Outsider,
    /// The pool shared by outside peers is full (#357).
    Others,
}

impl BusyLimit {
    /// The label of this limit in the refusal metric.
    fn label(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Committee => "committee",
            Self::Outsider => "outsider",
            Self::Others => "others",
        }
    }
}

/// The permits one job holds until it ends.
#[derive(Debug)]
pub(crate) enum RequestPermits {
    /// A member's own permit and its permit from the committee pool.
    Member { _peer: OwnedSemaphorePermit, _pool: OwnedSemaphorePermit },
    /// An outside peer's slot in the shared pool.
    Outsider { _job: OutsiderJob },
}

impl RequestPermits {
    /// Permits from a pool nothing else draws on, for tests of the work behind them.
    #[cfg(test)]
    pub(crate) fn unbounded() -> Self {
        let outsiders = Arc::new(OutsiderSlots::new(NonZeroUsize::MIN));
        let job = outsiders.claim(FREE_SLOT + 1).expect("a fresh pool has a free slot");
        Self::Outsider { _job: job }
    }
}

/// One running job of an outside peer, whose slot is freed on drop.
#[derive(Debug)]
pub(crate) struct OutsiderJob {
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
    pub(crate) fn acquire(&self, peer: BlsPublicKey) -> Result<RequestPermits, BusyLimit> {
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
