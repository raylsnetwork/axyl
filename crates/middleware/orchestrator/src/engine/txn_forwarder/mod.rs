//! Forwards a non-committee node's pending transactions to the committee.
//!
//! A node that cannot seal a batch still has to get its RPC-accepted transactions into consensus.
//! Under the sender-affinity fork it submits them by request-response to the validator owning each
//! sender's committee slot; pre-fork it publishes on the worker transaction topic. The ack lists
//! hashes the validator rejected as stale (already executed), which the acked-stale mark suppresses
//! from resends until local execution prunes them. Delivery is never assumed: transactions stay in
//! the local pool, and the shared in-flight tracker re-marks anything still pending once its mark
//! is due ([`FORWARD_POLICY`]) but only while this node is caught up, since a lagging node cannot
//! tell a lost send from its own lag.

use alloy::primitives::map::AddressMap;
use futures::{stream::FuturesUnordered, StreamExt as _};
use health::{ValidatorHealth, BREAKER_COOLDOWN};
use metrics::{ForwardMetrics, FORWARD_METRICS};
use parking_lot::Mutex;
use rayls_consensus_worker::{SubmitError, SubmitRejection, WorkerNetworkHandle};
use rayls_execution_evm::{
    in_flight::{DuePolicy, ForwardMarks, ForwardProbe},
    PoolTxn, TxPool, WorkerTxPool,
};
use rayls_infrastructure_types::{
    fxhash_slot_digest, Address, B256Set, BlsPublicKey, Bytes, ConsensusHeader, Encodable2718 as _,
    TaskKind, TaskSpawner, TxHash,
};
use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, time::MissedTickBehavior};
use tracing::{debug, info, warn};

mod health;
mod metrics;
#[cfg(test)]
mod tests;

/// Bytes charged to each transaction on top of its own length, covering the bcs length prefix that
/// precedes it in the published vector.
const TXN_FRAME_BYTES: usize = 5;

/// Maximum transactions carried in one forward message.
///
/// The byte budget alone lets a ~2 MiB message hold ~19k transactions, which the receiver admits in
/// a single call. If that admission outruns the submit timeout the sender re-sends the identical
/// payload to the next validator, fanning the same work across the committee and multiplying
/// duplicate seals. Capping the count bounds per-message receiver work independently of wire size.
const MAX_TXNS_PER_MESSAGE: usize = 2_000;

/// How long one direct submit waits for the validator's ack before trying the next live one.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum blocks local execution may trail the latest seen header before re-sends stop.
///
/// A node that is behind advances its anchor on schedule while re-sending transactions the network
/// executed long ago, flooding the committee with rejections. While the lag exceeds this bound loss
/// is indistinguishable from lag, so only first sends flow; those are stamped with the head at send
/// time, so they come due only once execution has passed it.
const RESEND_MAX_LOCAL_LAG: u64 = 20;

/// Consensus outputs per owner assignment: the locally executed consensus number (monotonic per
/// committed output, not the EVM block number) `>> OWNER_ROTATION_SHIFT` salts the rendezvous
/// score, so ownership re-shuffles every 1024 outputs.
///
/// A censoring owner (acks, never includes) holds a sender for at most one window; the breaker
/// escapes sooner on evidence, the rotation is the bound that needs none. Each rotation splits
/// every active sender's chain across two owners (in-flight sends stay with the old one), so the
/// window is long against inclusion latency; 256 outputs split chains too often on the local rig.
const OWNER_ROTATION_SHIFT: u32 = 10;

/// When a forwarded transaction is due for another send.
///
/// `after` is the WAN latency/jitter buffer, above worst-case inclusion latency so a healthy geo
/// path never double-sends. The small anchor margin makes a re-send mean "local execution passed
/// the send head and it is still pending"; a couple of blocks suffices, where the old wide margin
/// waited out 20-plus blocks of unrelated traffic and stranded the frontier chunk into the
/// validators' queued subpool. Backoff here counts re-sends of a frontier the owner acked but has
/// not included, so one doubling is the whole dampener: it caps the tail at 2x without throttling
/// the transaction that most needs re-sending.
pub(super) const FORWARD_POLICY: DuePolicy =
    DuePolicy { after: Duration::from_secs(3), backoff_shift_cap: 1, min_anchor_advance: 2 };

/// One owner's in-flight delivery: its chunks in order, resolving to the owner's slot once the last
/// is acked or has failed over.
type OwnerStream<'a> = Pin<Box<dyn Future<Output = u64> + Send + 'a>>;

/// One sender's forward-pending transactions, kept whole so its nonce chain stays contiguous.
struct SenderGroup {
    /// The committee slot the group is delivered to.
    owner: u64,
    /// Whether the head of the chain has been forwarded before, so the group is a re-send.
    resent: bool,
    /// Each transaction as `(nonce, hash, txn)`.
    txns: Vec<(u64, TxHash, Arc<PoolTxn>)>,
}

/// Epoch-scoped application actor that forwards an observer's pending transactions.
///
/// The pool and its forwarding marks remain node-scoped; this actor only coordinates one epoch's
/// committee, progress signals, and transport mode.
pub(super) struct TxnForwarder {
    /// The shared worker transaction pool holding the pending transactions to forward.
    pool: WorkerTxPool,
    /// The worker network handle for direct submit and gossip publish.
    network_handle: WorkerNetworkHandle,
    /// The node-scoped forwarding marks driving the resend policy.
    marks: ForwardMarks,
    /// The highest consensus header executed locally: the anchor the resend policy advances on,
    /// and (bucketed by [`OWNER_ROTATION_SHIFT`]) the number that keys the owner window.
    executed_anchor: watch::Receiver<ConsensusHeader>,
    /// The latest consensus header this node has seen, used to gate re-sends when lagging.
    last_seen_header: watch::Receiver<ConsensusHeader>,
    /// The committee, slot-ordered (authorities sorted by id), matching receiver-side dispatch.
    committee: Vec<BlsPublicKey>,
    /// Per-validator health ranking the ring-walk, epoch-scoped with this actor; the lock is never
    /// held across an await.
    health: Mutex<ValidatorHealth>,
    /// Re-evaluated each tick so the sender-affinity fork can activate mid-epoch.
    direct_submit: Box<dyn Fn() -> bool + Send + Sync>,
    /// Transaction-byte budget for one gossip publish.
    gossip_budget: usize,
    /// Transaction-byte budget for one direct submit.
    direct_budget: usize,
    /// Scan and delivery instrumentation, a clone of the process-wide family.
    metrics: ForwardMetrics,
}

impl TxnForwarder {
    /// Create a forwarder for one epoch.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        pool: WorkerTxPool,
        network_handle: WorkerNetworkHandle,
        executed_anchor: watch::Receiver<ConsensusHeader>,
        last_seen_header: watch::Receiver<ConsensusHeader>,
        committee: Vec<BlsPublicKey>,
        direct_submit: Box<dyn Fn() -> bool + Send + Sync>,
        max_gossip_message_size: usize,
        policy: DuePolicy,
    ) -> Self {
        // Arming keeps the sweep uninstalled: these marks are re-driven by the due check below, and
        // a node-scoped flat sweep releasing them would erase the backoff state.
        let marks = pool.in_flight().arm_forwarding(policy);
        let gossip_budget = WorkerNetworkHandle::txn_payload_budget(max_gossip_message_size);
        let direct_budget = network_handle.direct_txn_payload_budget();
        let health = Mutex::new(ValidatorHealth::new());

        Self {
            pool,
            network_handle,
            marks,
            executed_anchor,
            last_seen_header,
            committee,
            health,
            direct_submit,
            gossip_budget,
            direct_budget,
            metrics: FORWARD_METRICS.clone(),
        }
    }

    /// Spawn this actor for the epoch.
    ///
    /// [`TaskKind::Cancel`] is correct because the actor owns no epoch-tied state: forwarding marks
    /// remain node-scoped, so an epoch transition does not republish transactions.
    pub(super) fn spawn(self, forward_interval: Duration, task_spawner: &TaskSpawner) {
        task_spawner.spawn_classified_task(
            "txn forwarder",
            async move { self.run(forward_interval).await },
            TaskKind::Cancel,
        );
    }

    /// Run until the epoch task manager cancels the forwarder.
    async fn run(self, forward_interval: Duration) {
        // Drain at the batch cadence so a forwarded transaction reaches the committee no later than
        // the batch path it replaces.
        let mut interval = tokio::time::interval(forward_interval);
        // Delay, not Burst: a stalled tick has nothing to catch up on, since the next drain sees
        // the same pool the skipped ones would have.
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Owner streams outlive the tick that started them: a slow ack must not hold the next tick
        // for every other owner. An owner with a stream in flight is skipped until it returns, so
        // its still-unmarked groups are re-selected then rather than sent twice.
        let mut in_flight = FuturesUnordered::new();
        let mut busy = BTreeSet::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for (owner, stream) in self.forward_once(&busy).await {
                        busy.insert(owner);
                        in_flight.push(stream);
                    }
                }
                Some(owner) = in_flight.next(), if !in_flight.is_empty() => {
                    busy.remove(&owner);
                }
            }
        }
    }

    /// Apply the forwarding policy and start delivering the resulting sender runs, returning the
    /// direct owner streams left in flight (gossip publishes complete inline).
    async fn forward_once(&self, busy: &BTreeSet<u64>) -> Vec<(u64, OwnerStream<'_>)> {
        let now = Instant::now();
        let anchor = self.executed_anchor.borrow().number;
        let seen = self.last_seen_header.borrow().number;
        // The owner window is the executed consensus height bucketed by the rotation shift: a
        // deterministic, cross-observer-agreed number, the same one the resend policy anchors on.
        let window = anchor >> OWNER_ROTATION_SHIFT;
        // First sends of new transactions always flow; re-sends are gated while this node lags,
        // because a node behind the network cannot tell a lost send from its own lag and must not
        // re-flood transactions the network may already have executed.
        let allow_resend = Self::is_caught_up(seen, anchor);
        if !allow_resend {
            self.metrics.on_resend_gated();
            debug!(target: "worker::txn_forwarder", anchor, "local execution behind the seen header; re-sends gated, first sends still flow");
        }

        // Timed region: the O(pending) CPU work (snapshot, send-gate probe, grouping), isolated
        // from the network sends below. `select_pending` records the scan metrics itself.
        let pool = self.pool.clone();
        let committee = self.committee.clone();
        let marks = self.marks.clone();
        let metrics = self.metrics.clone();
        let by_sender = match tokio::task::spawn_blocking(move || {
            Self::select_pending(
                &pool,
                &marks,
                &committee,
                &metrics,
                now,
                anchor,
                window,
                allow_resend,
            )
        })
        .await
        {
            Ok(by_sender) => by_sender,
            Err(err) => {
                warn!(target: "worker::txn_forwarder", ?err, "forward-plan preparation task failed");
                return Vec::new();
            }
        };
        if by_sender.is_empty() {
            return Vec::new();
        }

        // Stamp with the head at send time, not the local anchor: a first send made while lagging
        // would otherwise be due as soon as the lag closes, before the block that included it is
        // executed locally, and its re-send blamed on a validator that did include it. `max`
        // covers `last_consensus_header` resetting at an epoch transition.
        let send_head = seen.max(anchor);
        if !self.committee.is_empty() && (self.direct_submit)() {
            self.submit_to_committee(by_sender, send_head, window, now, busy).await
        } else {
            self.publish_to_gossip(by_sender, send_head).await;
            Vec::new()
        }
    }

    /// Select transactions due for delivery, grouped into nonce-contiguous sender runs; `window`
    /// is the owner window of the executed EVM tip ([`OWNER_ROTATION_SHIFT`]).
    #[allow(clippy::too_many_arguments)]
    fn select_pending(
        pool: &WorkerTxPool,
        marks: &ForwardMarks,
        committee: &[BlsPublicKey],
        metrics: &ForwardMetrics,
        now: Instant,
        anchor: u64,
        window: u64,
        allow_resend: bool,
    ) -> AddressMap<SenderGroup> {
        let scan_start = Instant::now();
        let mut by_sender = AddressMap::default();
        let mut forwarded = 0u64;
        let mut resent = 0u64;

        let best_transactions = {
            let mut best_transactions = pool.best_transactions();
            best_transactions.no_updates();
            best_transactions
        };

        for txn in best_transactions {
            // EIP-4844 transactions are excluded for the same reason the batch builder skips them:
            // the blob sidecar does not travel with the encoded transaction, so the receiver cannot
            // pool it.
            if txn.is_eip4844() {
                continue;
            }
            // One tracker probe provides both the due decision and re-send classification.
            let probe = marks.probe(txn.hash(), now, anchor);
            if !Self::should_send(probe, allow_resend) {
                continue;
            }
            resent += u64::from(probe.forwarded);

            let sender = txn.sender();
            let group = by_sender.entry(sender).or_insert_with(|| SenderGroup {
                // The head of the nonce chain sets the step: it gates inclusion of the rest.
                owner: Self::rendezvous_owner(sender, window, committee, |_| true).unwrap_or(0),
                resent: probe.forwarded,
                txns: Vec::new(),
            });
            group.txns.push((txn.nonce(), *txn.hash(), txn.clone()));
            forwarded += 1;
        }

        metrics.on_scan(scan_start.elapsed(), pool.pool_size().pending as u64, forwarded, resent);
        by_sender
    }

    /// Start direct, sender-affinity streams, one per owner not already `busy`, and return them.
    /// Owner streams proceed concurrently, but chunks within a stream remain sequential to preserve
    /// nonce ordering across message boundaries.
    async fn submit_to_committee(
        &self,
        mut by_sender: AddressMap<SenderGroup>,
        anchor: u64,
        window: u64,
        now: Instant,
        busy: &BTreeSet<u64>,
    ) -> Vec<(u64, OwnerStream<'_>)> {
        // A busy owner's groups wait for its stream, unmarked, so the next free tick re-selects
        // them; dropping them before the walk keeps their blame and probes for that send.
        by_sender.retain(|_, group| !busy.contains(&group.owner));
        let connected = Arc::new(
            self.network_handle.inner_handle().connected_peers().await.unwrap_or_default(),
        );
        let (recovered, tripped) = {
            let mut health = self.health.lock();
            let recovered = health.credit_included(|hash| self.pool.get(hash).is_some());
            let tripped = health.blame_non_inclusion(
                &self.committee,
                by_sender.iter().map(|(s, g)| (*s, g.owner, g.resent)),
                now,
            );
            // Rendezvous over the connected validators whose breaker is closed. A held or probing
            // owner takes no group but the one probe, admitted only on the sender's own owner slot
            // so a lapsed breaker exposes one group, not every sender it used to own; with no
            // eligible slot the group keeps its owner and the send fails over on the ring.
            for (sender, group) in &mut by_sender {
                let probe = (!group.resent).then_some(*sender);
                let affinity = group.owner;
                let owner = Self::rendezvous_owner(*sender, window, &self.committee, |slot| {
                    let peer = &self.committee[slot as usize];
                    connected.contains(peer)
                        && if slot == affinity {
                            health.admit(peer, probe, now)
                        } else {
                            !health.holds(peer, now)
                        }
                });
                group.owner = owner.unwrap_or(affinity);
            }
            (recovered, tripped)
        };
        for peer in recovered {
            info!(target: "worker::txn_forwarder", ?peer, evidence = "inclusion", "validator breaker closed");
        }
        for peer in tripped {
            self.metrics.on_breaker_tripped();
            warn!(
                target: "worker::txn_forwarder",
                ?peer,
                "validator breaker opened: sends it acked keep coming back as re-sends"
            );
        }
        let by_owner = Self::aggregate_by_owner(by_sender, self.committee.len());

        by_owner
            .into_iter()
            .enumerate()
            .map(|(owner, txns)| (owner as u64, txns))
            // A walk can land on a busy slot too; that group waits like the rest of the slot's.
            .filter(|(owner, txns)| !txns.is_empty() && !busy.contains(owner))
            .map(|(owner, txns)| {
                let connected = Arc::clone(&connected);
                let stream: OwnerStream<'_> = Box::pin(async move {
                    // The validator that acked the last chunk leads the next one, so a stream
                    // that failed over stays whole on one pool instead of splitting a sender's
                    // nonce chain between a flapping owner and its successor.
                    let mut acked_by = None;
                    for message in
                        Self::chunk_under_budget(txns, self.direct_budget, MAX_TXNS_PER_MESSAGE)
                    {
                        acked_by = self
                            .submit_message(owner, &connected, message, anchor, now, acked_by)
                            .await;
                    }
                    owner
                });
                (owner, stream)
            })
            .collect()
    }

    /// Deliver legacy gossip streams. A sender run must remain separate because the receiver
    /// derives a whole message's owner from its first transaction.
    async fn publish_to_gossip(&self, by_sender: AddressMap<SenderGroup>, anchor: u64) {
        for group in by_sender.into_values() {
            for message in Self::chunk_under_budget(
                Self::nonce_sorted(group.txns),
                self.gossip_budget,
                MAX_TXNS_PER_MESSAGE,
            ) {
                self.publish_message(message, anchor).await;
            }
        }
    }

    /// Submits a message to the first candidate on the ring walk ([`ValidatorHealth::candidates`]),
    /// or to `lead` first when the stream's previous chunk landed there; returns the acking peer.
    ///
    /// A failure feeds the breaker and walks on; if every candidate fails the message stays
    /// unstamped and the next tick retries it. Acked hashes are stamped with the current anchor.
    async fn submit_message(
        &self,
        owner: u64,
        connected: &[BlsPublicKey],
        message: Vec<(TxHash, Arc<PoolTxn>)>,
        anchor: u64,
        now: Instant,
        lead: Option<BlsPublicKey>,
    ) -> Option<BlsPublicKey> {
        // Encode once outside the failover loop: a retry to the next validator re-uses the same
        // hashes and clones the `Bytes` payloads (refcount bumps, not re-serialization).
        let hashes = message.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
        let payloads: Vec<Bytes> = message.iter().map(|(_, txn)| encode_txn(txn)).collect();

        let mut candidates = self.health.lock().candidates(owner, &self.committee, connected, now);
        if let Some(index) = lead.and_then(|lead| candidates.iter().position(|peer| *peer == lead))
        {
            candidates[..=index].rotate_right(1);
        }
        if candidates.is_empty() {
            // No committee member is connected, not a walk exhausting: the network layer already
            // reports the disconnect, so a warn here would repeat it once per pending message.
            debug!(target: "worker::txn_forwarder", txns = message.len(), "no connected validator; retrying next tick");
            return None;
        }
        for peer in candidates {
            match self.network_handle.submit_txns(peer, payloads.clone(), SUBMIT_TIMEOUT).await {
                Ok(stale) => {
                    debug!(
                        target: "worker::txn_forwarder",
                        txns = message.len(),
                        stale = stale.len(),
                        ?peer,
                        "submitted transactions"
                    );
                    let delivered = message.iter().map(|(hash, txn)| (txn.sender(), *hash));
                    if self.health.lock().on_success(&peer, delivered) {
                        info!(target: "worker::txn_forwarder", ?peer, evidence = "ack", "validator breaker closed");
                    }
                    let stale = Self::validate_stale(&hashes, stale);
                    self.metrics.on_acked_stale(stale.len() as u64);
                    // A stale ack is a claim, not a verdict: an honest one is followed by the
                    // pool pruning the hash within a few blocks, so it is stamped like an
                    // accepted send and, if still pending after the window, re-sends one slot
                    // on. A validator cannot silence a hash by calling it stale.
                    self.marks.mark_forwarded(hashes, Instant::now(), anchor);
                    return Some(peer);
                }
                Err(e) => {
                    if matches!(e, SubmitError::Rejected(SubmitRejection::NotBatchProducing)) {
                        self.metrics.on_mode_rejected();
                    }
                    if self.health.lock().on_failure(&peer, &e, Instant::now()) {
                        self.metrics.on_breaker_tripped();
                        // Edge-triggered by the trip, so one line per exclusion, not per failed
                        // send: the validator now walks last until its breaker cooldown lapses.
                        warn!(
                            target: "worker::txn_forwarder",
                            ?e,
                            ?peer,
                            cooldown = ?BREAKER_COOLDOWN,
                            "validator breaker tripped; held out of forwarding until cooldown lapses"
                        );
                    }
                    debug!(target: "worker::txn_forwarder", ?e, ?peer, "submit failed, trying next live validator")
                }
            }
        }
        warn!(
            target: "worker::txn_forwarder",
            txns = message.len(),
            "no live validator accepted the message; retrying next tick"
        );
        None
    }

    /// Publish one message on the worker transaction topic (the pre-fork path).
    async fn publish_message(&self, message: Vec<(TxHash, Arc<PoolTxn>)>, anchor: u64) {
        let (hashes, payloads): (Vec<TxHash>, Vec<Bytes>) =
            message.into_iter().map(|(hash, txn)| (hash, encode_txn(&txn))).unzip();

        match self.network_handle.publish_txn(payloads).await {
            Ok(()) => {
                debug!(target: "worker::txn_forwarder", txns = hashes.len(), "forwarded transactions");
                // Stamp only what is on the wire: a failed publish stays due for the next tick.
                self.marks.mark_forwarded(hashes, Instant::now(), anchor);
            }
            Err(e) => warn!(target: "worker::txn_forwarder", ?e, "failed to publish transactions"),
        }
    }

    /// Whether local execution is close enough to the peer-latest header to allow re-sends.
    ///
    /// The `seen >= anchor` guard rejects an understated `seen`, which happens when
    /// `last_consensus_header` resets at an epoch transition.
    fn is_caught_up(seen: u64, anchor: u64) -> bool {
        seen >= anchor && seen - anchor <= RESEND_MAX_LOCAL_LAG
    }

    /// A first send flows while syncing; a re-send additionally needs the caught-up gate.
    fn should_send(probe: ForwardProbe, allow_resend: bool) -> bool {
        (allow_resend || !probe.forwarded) && probe.due
    }

    /// Restrict a stale reply to hashes this node actually sent, preventing a peer from suppressing
    /// an unrelated pending transaction.
    fn validate_stale(hashes: &[TxHash], mut stale: Vec<TxHash>) -> B256Set {
        if stale.is_empty() {
            return B256Set::default();
        }
        if stale.len() <= 4 {
            stale.retain(|hash| hashes.iter().any(|sent_hash| sent_hash == hash));
            return stale.into_iter().collect();
        }
        let sent: B256Set = hashes.iter().copied().collect();
        stale.into_iter().filter(|hash| sent.contains(hash)).collect()
    }

    /// Split a nonce-ordered stream by wire budget and receiver admission count.
    fn chunk_under_budget(
        txns: Vec<(TxHash, Arc<PoolTxn>)>,
        budget: usize,
        max_count: usize,
    ) -> Vec<Vec<(TxHash, Arc<PoolTxn>)>> {
        let mut messages = Vec::new();
        let mut message = Vec::with_capacity(max_count.min(128));
        let mut size = 0;
        for (hash, txn) in txns {
            let cost = txn.encoded_length() + TXN_FRAME_BYTES;
            // A single oversized transaction still goes out alone; dropping it would lose it.
            if (size + cost > budget || message.len() >= max_count) && !message.is_empty() {
                messages.push(std::mem::take(&mut message));
                size = 0;
            }
            size += cost;
            message.push((hash, txn));
        }
        if !message.is_empty() {
            messages.push(message);
        }
        messages
    }

    /// Overrides the direct-submit byte budget, so a test forces multi-chunk streams with a
    /// handful of transactions.
    #[cfg(test)]
    fn with_direct_budget(mut self, direct_budget: usize) -> Self {
        self.direct_budget = direct_budget;
        self
    }

    /// A sender's owner in EVM block `block`'s window ([`OWNER_ROTATION_SHIFT`]) with every
    /// committee slot eligible ([`Self::rendezvous_owner`]); zero for an empty committee.
    #[cfg(test)]
    fn owner_slot(sender: Address, block: u64, committee: &[BlsPublicKey]) -> u64 {
        Self::rendezvous_owner(sender, block >> OWNER_ROTATION_SHIFT, committee, |_| true)
            .unwrap_or(0)
    }

    /// The eligible committee slot with the highest rendezvous score for `sender` in `window`:
    /// `fxhash(sender ++ window ++ validator key)`, ties to the lower slot.
    ///
    /// A pure function of its inputs, so every observer with the same eligibility view routes a
    /// sender identically (a load balancer can then split a sender's chain across observers but
    /// not across validators), and a validator leaving the eligible set moves only the senders it
    /// owned, spread over the rest by their own scores. `eligible` is asked once per slot, in
    /// slot order. Balance is by expectation: exact only in the limit of many senders.
    fn rendezvous_owner(
        sender: Address,
        window: u64,
        committee: &[BlsPublicKey],
        mut eligible: impl FnMut(u64) -> bool,
    ) -> Option<u64> {
        let mut key = Vec::with_capacity(Address::len_bytes() + 8 + 96);
        key.extend_from_slice(sender.as_slice());
        key.extend_from_slice(&window.to_le_bytes());
        let prefix = key.len();
        (0..committee.len() as u64).filter(|&slot| eligible(slot)).max_by_key(|&slot| {
            key.truncate(prefix);
            key.extend_from_slice(committee[slot as usize].as_ref());
            (fxhash_slot_digest(&key), std::cmp::Reverse(slot))
        })
    }

    /// Concatenate each owner slot's sender runs into one nonce-ordered stream.
    fn aggregate_by_owner(
        by_sender: AddressMap<SenderGroup>,
        committee_size: usize,
    ) -> Vec<Vec<(TxHash, Arc<PoolTxn>)>> {
        let mut by_owner = vec![Vec::new(); committee_size.max(1)];
        for group in by_sender.into_values() {
            by_owner[group.owner as usize].extend(Self::nonce_sorted(group.txns));
        }
        by_owner
    }

    /// Sort one sender's transactions by nonce and remove the nonce from the wire payload.
    fn nonce_sorted(mut group: Vec<(u64, TxHash, Arc<PoolTxn>)>) -> Vec<(TxHash, Arc<PoolTxn>)> {
        group.sort_unstable_by_key(|(nonce, _, _)| *nonce);
        group.into_iter().map(|(_, hash, txn)| (hash, txn)).collect()
    }
}

/// Encode a pooled transaction to its EIP-2718 wire bytes.
fn encode_txn(txn: &Arc<PoolTxn>) -> Bytes {
    let mut buf = Vec::with_capacity(txn.encoded_length());
    txn.transaction.transaction().encode_2718(&mut buf);
    Bytes::from(buf)
}
