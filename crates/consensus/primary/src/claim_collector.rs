//! Collector for signer claims: accumulates vote signatures for targets we authored and
//! publishes frozen claims for the proposal path to carry.

use parking_lot::Mutex;
use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_types::{
    BlsAggregateSignature, BlsSignature, CertificateDigest, Epoch, Round, SignerClaim, TaskManager,
    VotingPower, MAX_SIGNER_CLAIMS, MAX_TRACKED_TARGETS, SIGNER_CLAIM_LAG_ROUNDS,
};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum CollectorMsg {
    Register {
        epoch: Epoch,
        target_digest: CertificateDigest,
        target_round: Round,
        rank: u32,
        weight: VotingPower,
        signature: BlsSignature,
    },
    AddShare {
        epoch: Epoch,
        target_digest: CertificateDigest,
        rank: u32,
        signature: BlsSignature,
        weight: VotingPower,
    },
    FreezeMarker {
        epoch: Epoch,
        round: Round,
        quorum_threshold: VotingPower,
    },
    Carried {
        epoch: Epoch,
        target_digests: Vec<CertificateDigest>,
    },
    ClaimsCommitted {
        epoch: Epoch,
        target_digests: Vec<CertificateDigest>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryState {
    Collecting,
    Ready,
    Carried,
}

#[derive(Debug)]
struct Entry {
    state: EntryState,
    target_round: Round,
    weight: VotingPower,
    sigs: BTreeMap<u32, BlsSignature>,
    frozen: Option<SignerClaim>,
}

#[derive(Clone)]
pub struct CollectorHandle {
    tx: mpsc::Sender<CollectorMsg>,
    metrics: Arc<PrimaryMetrics>,
    published: Arc<Mutex<VecDeque<SignerClaim>>>,
    #[cfg(feature = "test-utils")]
    idle_rx: Arc<Mutex<Option<mpsc::Receiver<CollectorMsg>>>>,
}

impl fmt::Debug for CollectorHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectorHandle")
            .field("published", &self.published.lock().len())
            .field("capacity", &self.tx.capacity())
            .finish()
    }
}

impl CollectorHandle {
    // Spawns the one collector task; losing it costs recorded signers, not uptime.
    pub fn spawn(task_manager: &TaskManager, metrics: Arc<PrimaryMetrics>) -> Self {
        let (handle, inbox) = handle_with_inbox(metrics);
        task_manager.spawn_task(
            "signer claim collector",
            run(inbox, handle.published.clone(), handle.metrics.clone()),
        );
        handle
    }

    // The inbox is not read: a stalled collector must cost coverage, never time.
    #[cfg(feature = "test-utils")]
    pub fn stalled(metrics: Arc<PrimaryMetrics>) -> Self {
        let (handle, inbox) = handle_with_inbox(metrics);
        *handle.idle_rx.lock() = Some(inbox);
        handle
    }

    pub fn register(
        &self,
        epoch: Epoch,
        target_digest: CertificateDigest,
        target_round: Round,
        rank: u32,
        weight: VotingPower,
        signature: BlsSignature,
    ) {
        self.send(
            CollectorMsg::Register { epoch, target_digest, target_round, rank, weight, signature },
            "queue_full",
        );
    }

    pub fn add_share(
        &self,
        epoch: Epoch,
        target_digest: CertificateDigest,
        rank: u32,
        signature: BlsSignature,
        weight: VotingPower,
    ) {
        self.send(
            CollectorMsg::AddShare { epoch, target_digest, rank, signature, weight },
            "queue_full",
        );
    }

    pub fn freeze_marker(&self, epoch: Epoch, round: Round, quorum_threshold: VotingPower) {
        self.send(CollectorMsg::FreezeMarker { epoch, round, quorum_threshold }, "queue_full");
    }

    pub fn claims_committed(&self, epoch: Epoch, target_digests: Vec<CertificateDigest>) {
        self.send(CollectorMsg::ClaimsCommitted { epoch, target_digests }, "queue_full");
    }

    pub fn take(&self, epoch: Epoch, round: Round) -> Vec<SignerClaim> {
        let mut claims = Vec::new();
        let mut stale = 0usize;
        {
            let mut published = self.published.lock();
            while claims.len() < MAX_SIGNER_CLAIMS {
                let Some(head) = published.front() else { break };
                let oldest_allowed = round.checked_sub(SIGNER_CLAIM_LAG_ROUNDS);
                if head.target_epoch != epoch {
                    stale += 1;
                    published.pop_front();
                    continue;
                }
                if oldest_allowed.is_none_or(|oldest| head.target_round > oldest) {
                    break;
                }
                claims.push(published.pop_front().expect("queue has a front"));
            }
        }
        if stale > 0 {
            self.count("epoch_end", stale as u64);
        }
        if !claims.is_empty() {
            let target_digests = claims.iter().map(|claim| claim.target_digest).collect();
            self.send(CollectorMsg::Carried { epoch, target_digests }, "handoff_dropped");
        }
        claims
    }

    fn send(&self, msg: CollectorMsg, reason: &str) {
        match self.tx.try_send(msg) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => self.count(reason, 1),
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    fn count(&self, reason: &str, delta: u64) {
        self.metrics.claim_events_dropped.with_label_values(&[reason]).inc_by(delta);
    }
}

fn handle_with_inbox(
    metrics: Arc<PrimaryMetrics>,
) -> (CollectorHandle, mpsc::Receiver<CollectorMsg>) {
    let (tx, inbox) = mpsc::channel(MAX_TRACKED_TARGETS);
    let handle = CollectorHandle {
        tx,
        metrics,
        published: Arc::new(Mutex::new(VecDeque::new())),
        #[cfg(feature = "test-utils")]
        idle_rx: Arc::new(Mutex::new(None)),
    };
    (handle, inbox)
}

async fn run(
    mut inbox: mpsc::Receiver<CollectorMsg>,
    published: Arc<Mutex<VecDeque<SignerClaim>>>,
    metrics: Arc<PrimaryMetrics>,
) {
    let mut targets: BTreeMap<CertificateDigest, Entry> = BTreeMap::new();
    let mut epoch: Option<Epoch> = None;

    while let Some(msg) = inbox.recv().await {
        match msg {
            CollectorMsg::Register {
                epoch: msg_epoch,
                target_digest,
                target_round,
                rank,
                weight,
                signature,
            } => {
                align(msg_epoch, &mut epoch, &mut targets, &published, &metrics);
                if targets.contains_key(&target_digest) {
                    continue;
                }
                if targets.len() >= MAX_TRACKED_TARGETS {
                    // Only a real eviction is counted; a target that never entered the table was
                    // never evicted. Below the cap nothing is dropped, so nothing is counted.
                    let Some(evicted) = evict_oldest_collecting(&targets) else { continue };
                    targets.remove(&evicted);
                    metrics.claim_events_dropped.with_label_values(&["cap_eviction"]).inc();
                }
                let mut sigs = BTreeMap::new();
                sigs.insert(rank, signature);
                targets.insert(
                    target_digest,
                    Entry {
                        state: EntryState::Collecting,
                        target_round,
                        weight,
                        sigs,
                        frozen: None,
                    },
                );
            }

            CollectorMsg::AddShare { epoch: msg_epoch, target_digest, rank, signature, weight } => {
                align(msg_epoch, &mut epoch, &mut targets, &published, &metrics);
                let Some(entry) = targets.get_mut(&target_digest) else { continue };
                match entry.state {
                    EntryState::Collecting => {
                        if entry.sigs.insert(rank, signature).is_none() {
                            entry.weight += weight;
                        }
                    }
                    EntryState::Ready => {
                        metrics.claim_events_dropped.with_label_values(&["after_freeze"]).inc();
                    }
                    EntryState::Carried => {
                        metrics.claim_events_dropped.with_label_values(&["after_carry"]).inc();
                    }
                }
            }

            CollectorMsg::FreezeMarker { epoch: msg_epoch, round, quorum_threshold } => {
                align(msg_epoch, &mut epoch, &mut targets, &published, &metrics);
                let oldest_allowed = round.checked_sub(SIGNER_CLAIM_LAG_ROUNDS);

                for entry in targets.values_mut() {
                    if entry.state != EntryState::Carried {
                        continue;
                    }
                    entry.state = EntryState::Ready;
                    if let Some(claim) = entry.frozen.as_ref() {
                        publish(&published, claim.clone());
                    }
                }

                let Some(oldest_allowed) = oldest_allowed else { continue };
                let sealed: Vec<(CertificateDigest, SignerClaim)> = targets
                    .iter()
                    .filter(|(_, entry)| {
                        entry.state == EntryState::Collecting
                            && entry.target_round <= oldest_allowed
                            && entry.weight >= quorum_threshold
                    })
                    .filter_map(|(digest, entry)| {
                        seal(digest, entry, msg_epoch).map(|claim| (*digest, claim))
                    })
                    .collect();

                for (digest, claim) in sealed {
                    if let Some(entry) = targets.get_mut(&digest) {
                        entry.state = EntryState::Ready;
                        entry.frozen = Some(claim.clone());
                        publish(&published, claim);
                    }
                }
            }

            CollectorMsg::Carried { epoch: msg_epoch, target_digests } => {
                align(msg_epoch, &mut epoch, &mut targets, &published, &metrics);
                for digest in target_digests {
                    if let Some(entry) = targets.get_mut(&digest) {
                        entry.state = EntryState::Carried;
                    }
                }
            }

            CollectorMsg::ClaimsCommitted { epoch: msg_epoch, target_digests } => {
                align(msg_epoch, &mut epoch, &mut targets, &published, &metrics);
                for digest in target_digests {
                    targets.remove(&digest);
                }
            }
        }
    }
}

// Rounds are epoch-relative, so a message from another epoch means the table is stale.
fn align(
    msg_epoch: Epoch,
    epoch: &mut Option<Epoch>,
    targets: &mut BTreeMap<CertificateDigest, Entry>,
    published: &Mutex<VecDeque<SignerClaim>>,
    metrics: &PrimaryMetrics,
) {
    if *epoch == Some(msg_epoch) {
        return;
    }
    if epoch.is_some() {
        let dropped = targets.len() + published.lock().len();
        metrics.claim_events_dropped.with_label_values(&["epoch_end"]).inc_by(dropped as u64);
    }
    targets.clear();
    published.lock().clear();
    *epoch = Some(msg_epoch);
}

fn evict_oldest_collecting(
    targets: &BTreeMap<CertificateDigest, Entry>,
) -> Option<CertificateDigest> {
    targets
        .iter()
        .filter(|(_, entry)| entry.state == EntryState::Collecting)
        .min_by_key(|(digest, entry)| (entry.target_round, *digest))
        .map(|(digest, _)| *digest)
}

fn seal(target_digest: &CertificateDigest, entry: &Entry, epoch: Epoch) -> Option<SignerClaim> {
    let ranks = entry.sigs.keys().copied().collect();
    let signature =
        BlsAggregateSignature::aggregate(&entry.sigs.values().copied().collect::<Vec<_>>(), true)
            .ok()?
            .to_signature();
    Some(SignerClaim {
        target_digest: *target_digest,
        target_epoch: epoch,
        target_round: entry.target_round,
        ranks,
        signature,
    })
}

fn publish(published: &Mutex<VecDeque<SignerClaim>>, claim: SignerClaim) {
    let mut queue = published.lock();
    let position = queue
        .binary_search_by(|entry| {
            (entry.target_round, entry.target_digest)
                .cmp(&(claim.target_round, claim.target_digest))
        })
        .unwrap_or_else(|index| index);
    queue.insert(position, claim);
}
