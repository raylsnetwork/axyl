//! Forwards a non-committee node's pending transactions to the committee.
//!
//! A node that cannot seal a batch still has to get its RPC-accepted transactions into consensus.
//! Under the sender-affinity fork it submits them by request-response to the validator owning each
//! sender's committee slot; pre-fork it publishes on the worker transaction topic. The ack lists
//! hashes the validator rejected as stale (already executed), which the acked-stale mark suppresses
//! from resends until local execution prunes them. Delivery is never assumed: transactions stay in
//! the local pool, and the shared in-flight tracker re-marks anything still pending once its mark
//! is due ([`FORWARD_POLICY`]) but only while this node is caught up, since a lagging node cannot
//! tell a lost send from its own lag. A transaction still pending after [`FORWARD_PRUNE_ATTEMPTS`]
//! re-sends is dropped as unmineable rather than forwarded forever.

use alloy::primitives::map::AddressMap;
use prometheus::{
    default_registry, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, IntCounter, Registry,
};
use rayls_consensus_worker::WorkerNetworkHandle;
use rayls_execution_evm::{
    in_flight::{DuePolicy, ForwardMarks, ForwardProbe},
    PoolTxn, TxPool, WorkerTxPool,
};
use rayls_infrastructure_types::{
    fxhash_slot_digest, ring_walk, B256Set, BlsPublicKey, Bytes, ConsensusHeader,
    Encodable2718 as _, TaskKind, TaskSpawner, TxHash,
};
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::{sync::watch, time::MissedTickBehavior};
use tracing::{debug, warn};

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
/// is indistinguishable from lag, so only first sends flow; by catch-up, reconcile has pruned what
/// executed and almost nothing is left to re-send.
const RESEND_MAX_LOCAL_LAG: u64 = 20;

/// When a forwarded transaction is due for another send.
///
/// The 10s base sits well above worst-case inclusion latency (~20 rounds at a 500ms round), so a
/// healthy path never double-sends. The 20-block anchor margin makes a re-send mean "execution
/// passed where this should have landed and it is still pending". The doubling cap bounds re-send
/// amplification at 16x the base window. A re-send is cheap: the target pool rejects a hash it
/// holds.
const FORWARD_POLICY: DuePolicy =
    DuePolicy { after: Duration::from_secs(10), backoff_shift_cap: 4, min_anchor_advance: 20 };

/// The observer shifts each sender's owner validator one slot along the ring every this many
/// executed anchors, so no sender is pinned to a single validator: a dishonest owner can withhold a
/// sender's transactions for at most one window before the next validator takes over. Only the
/// observer reads this (the receiver accepts unconditionally), so it needs no cross-node agreement.
const OWNER_ROTATION_BLOCKS: u64 = 2048;

/// Drop a forwarded transaction still sitting in the pending pool after this many re-send attempts.
/// Each attempt already required the executed anchor to advance `min_anchor_advance` blocks, so a
/// transaction that has survived this many re-sends is not going to be mined (the chain moved on
/// without it); forwarding it forever only wastes submits. The client resubmits if it is still
/// wanted, so dropping it is safe.
///
/// The check runs before the re-send gate, so a transaction that reached the cap while caught up is
/// still dropped even if the node has since fallen behind. But `attempts` only climbs while caught
/// up (re-sends are gated by lag), so a node that has been lagging throughout - holding, not
/// re-sending, while it catches up - never reaches the cap here: its pending transactions are stuck
/// because it is behind, not because they are dead, and most are mined once it catches up. A node
/// that can never catch up is a separate problem, not papered over here.
const FORWARD_PRUNE_ATTEMPTS: u32 = 5;

/// Instrumentation for the per-tick pool scan, settling whether re-examining the whole pending set
/// each tick is a real cost or noise absorbed by the interval.
#[derive(Clone)]
struct ForwardMetrics {
    /// Seconds spent scanning the pending pool and grouping due transactions per tick.
    scan_duration: Histogram,
    /// Total pending transactions inspected across ticks (the scan's input size).
    pending_examined: IntCounter,
    /// Total transactions that passed the send gate across ticks (the scan's useful output).
    forwarded: IntCounter,
    /// Subset of `forwarded` that re-sent an already-published hash; a sustained rate is a flood.
    resent: IntCounter,
    /// Hashes a validator acked as already-executed (stale) on a direct submit.
    acked_stale: IntCounter,
    /// Ticks where re-sends were gated because local execution trailed the seen header.
    resend_gated: IntCounter,
    /// Transactions dropped from the pool after repeated forwards without being mined.
    pruned: IntCounter,
}

impl ForwardMetrics {
    /// Register the family on `registry`, failing if a name is already registered there.
    fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            scan_duration: register_histogram_with_registry!(
                "rayls_txn_forwarder_scan_duration_seconds",
                "Seconds spent scanning the pending pool and grouping due transactions per tick",
                vec![
                    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1
                ],
                registry
            )?,
            pending_examined: register_int_counter_with_registry!(
                "rayls_txn_forwarder_pending_examined_total",
                "Total pending transactions inspected across forward ticks",
                registry
            )?,
            forwarded: register_int_counter_with_registry!(
                "rayls_txn_forwarder_forwarded_total",
                "Total transactions that passed the send gate across forward ticks",
                registry
            )?,
            resent: register_int_counter_with_registry!(
                "rayls_txn_forwarder_resent_total",
                "Sends that re-forwarded an already-published hash (a sustained rate is a flood)",
                registry
            )?,
            acked_stale: register_int_counter_with_registry!(
                "rayls_txn_forwarder_acked_stale_total",
                "Hashes a validator acked as already-executed on a direct submit",
                registry
            )?,
            resend_gated: register_int_counter_with_registry!(
                "rayls_txn_forwarder_resend_gated_total",
                "Ticks where re-sends were withheld because this node trailed the seen header",
                registry
            )?,
            pruned: register_int_counter_with_registry!(
                "rayls_txn_forwarder_pruned_total",
                "Transactions dropped from the pool after repeated forwards without being mined",
                registry
            )?,
        })
    }

    /// Register against a private registry, for the fallback when the default already holds the
    /// family (a second process-wide registration).
    fn register_fresh() -> Self {
        Self::register(&Registry::new()).expect("a fresh registry should always succeed")
    }

    /// Record one scan tick: its duration and the pending, forwarded, and re-sent counts.
    fn on_scan(&self, duration: Duration, pending: u64, forwarded: u64, resent: u64) {
        self.scan_duration.observe(duration.as_secs_f64());
        self.pending_examined.inc_by(pending);
        self.forwarded.inc_by(forwarded);
        self.resent.inc_by(resent);
    }

    /// Record a tick whose re-sends were withheld because this node trailed the seen header.
    fn on_resend_gated(&self) {
        self.resend_gated.inc();
    }

    /// Record hashes a validator acked as already-executed on a direct submit.
    fn on_acked_stale(&self, count: u64) {
        self.acked_stale.inc_by(count);
    }

    /// Record transactions dropped from the pool as unmineable after repeated forwards.
    fn on_pruned(&self, count: u64) {
        self.pruned.inc_by(count);
    }
}

/// Registered once per process; the forwarder re-spawns each epoch but these outlive it. Falls back
/// to a private registry if the default already holds the family, so a second registration degrades
/// to unscraped instead of aborting.
static FORWARD_METRICS: LazyLock<ForwardMetrics> = LazyLock::new(|| {
    ForwardMetrics::register(default_registry())
        .unwrap_or_else(|_| ForwardMetrics::register_fresh())
});

/// One sender's forward-pending transactions, kept whole so its nonce chain stays contiguous.
///
/// The slot owner (computed once when the sender is first seen) plus each transaction as
/// `(nonce, hash, txn)`; the nonce is kept only to sort the run before it goes on the wire.
type SenderGroup = (u64, Vec<(u64, TxHash, Arc<PoolTxn>)>);

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
    /// The highest consensus header executed locally, the anchor the resend policy advances on.
    executed_anchor: watch::Receiver<ConsensusHeader>,
    /// The latest consensus header this node has seen, used to gate re-sends when lagging.
    last_seen_header: watch::Receiver<ConsensusHeader>,
    /// The committee, slot-ordered (authorities sorted by id), matching receiver-side dispatch.
    committee: Vec<BlsPublicKey>,
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
    ///
    /// `committee` is slot-ordered (authorities sorted by id), matching receiver-side dispatch;
    /// `direct_submit` is evaluated on each tick so the sender-affinity fork can activate during
    /// the epoch.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        pool: WorkerTxPool,
        network_handle: WorkerNetworkHandle,
        executed_anchor: watch::Receiver<ConsensusHeader>,
        last_seen_header: watch::Receiver<ConsensusHeader>,
        committee: Vec<BlsPublicKey>,
        direct_submit: Box<dyn Fn() -> bool + Send + Sync>,
        max_gossip_message_size: usize,
    ) -> Self {
        // Arming keeps the sweep uninstalled: these marks are re-driven by the due check below, and
        // a node-scoped flat sweep releasing them would erase the backoff state.
        let marks = pool.in_flight().arm_forwarding(FORWARD_POLICY);
        let gossip_budget = WorkerNetworkHandle::txn_payload_budget(max_gossip_message_size);
        let direct_budget = network_handle.direct_txn_payload_budget();

        Self {
            pool,
            network_handle,
            marks,
            executed_anchor,
            last_seen_header,
            committee,
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
        loop {
            interval.tick().await;
            self.forward_once().await;
        }
    }

    /// Apply the forwarding policy and deliver the resulting sender runs once.
    async fn forward_once(&self) {
        let now = Instant::now();
        let anchor = self.executed_anchor.borrow().number;
        // First sends of new transactions always flow; re-sends are gated while this node lags,
        // because a node behind the network cannot tell a lost send from its own lag and must not
        // re-flood transactions the network may already have executed.
        let allow_resend = Self::is_caught_up(self.last_seen_header.borrow().number, anchor);
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
            Self::select_pending(&pool, &marks, &committee, &metrics, now, anchor, allow_resend)
        })
        .await
        {
            Ok(by_sender) => by_sender,
            Err(err) => {
                warn!(target: "worker::txn_forwarder", ?err, "forward-plan preparation task failed");
                return;
            }
        };
        if by_sender.is_empty() {
            return;
        }

        if !self.committee.is_empty() && (self.direct_submit)() {
            self.submit_to_committee(by_sender, anchor).await;
        } else {
            self.publish_to_gossip(by_sender, anchor).await;
        }
    }

    /// Select transactions due for delivery, grouped into nonce-contiguous sender runs.
    fn select_pending(
        pool: &WorkerTxPool,
        marks: &ForwardMarks,
        committee: &[BlsPublicKey],
        metrics: &ForwardMetrics,
        now: Instant,
        anchor: u64,
        allow_resend: bool,
    ) -> AddressMap<SenderGroup> {
        let scan_start = Instant::now();
        let mut by_sender = AddressMap::default();
        let mut forwarded = 0u64;
        let mut resent = 0u64;
        let mut prune_hashes: Vec<TxHash> = Vec::new();
        let committee_size = committee.len() as u64;

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
            // One tracker probe provides the due decision, re-send classification, and attempt
            // count.
            let probe = marks.probe(txn.hash(), now, anchor);
            // A transaction still pending after FORWARD_PRUNE_ATTEMPTS re-sends is not going to be
            // mined (each attempt already required the anchor to advance, so the chain moved on
            // without it). Drop it instead of forwarding it forever; the client resubmits if still
            // wanted. Removing it clears its in-flight mark on the next reconcile (it leaves
            // pending). Checked before the send gate so a transaction that reached the cap while
            // caught up is still pruned if the node has since fallen behind (`attempts` only climbs
            // while caught up, so this cannot fire for one that has been lagging throughout).
            if probe.attempts >= FORWARD_PRUNE_ATTEMPTS {
                prune_hashes.push(*txn.hash());
                continue;
            }
            if !Self::should_send(probe, allow_resend) {
                continue;
            }
            resent += u64::from(probe.forwarded);

            let sender = txn.sender();
            let (_, group) = by_sender.entry(sender).or_insert_with(|| {
                // An empty committee has no owner; slot zero is only used by the gossip fallback.
                // Rotate the owner one slot every OWNER_ROTATION_BLOCKS so a sender is never pinned
                // to a single validator: a persistently dishonest owner can withhold it for at most
                // one window, after which the next validator on the ring takes over. Keyed on the
                // observer's own executed anchor - the receiver accepts unconditionally, so no
                // cross-node agreement on the window is required. `submit_message` still fails over
                // from the rotated slot to the next live validator on the ring.
                let window = anchor / OWNER_ROTATION_BLOCKS;
                let owner = fxhash_slot_digest(sender.as_slice())
                    .wrapping_add(window)
                    .checked_rem(committee_size)
                    .unwrap_or(0);
                (owner, Vec::new())
            });
            group.push((txn.nonce(), *txn.hash(), txn.clone()));
            forwarded += 1;
        }

        if !prune_hashes.is_empty() {
            metrics.on_pruned(prune_hashes.len() as u64);
            // Their in-flight marks are released by the next reconcile (they leave the pending
            // pool).
            pool.remove_transactions(prune_hashes);
        }

        metrics.on_scan(scan_start.elapsed(), pool.pool_size().pending as u64, forwarded, resent);
        by_sender
    }

    /// Deliver direct, sender-affinity streams. Owner streams proceed concurrently, but chunks
    /// within a stream remain sequential to preserve nonce ordering across message boundaries.
    async fn submit_to_committee(&self, by_sender: AddressMap<SenderGroup>, anchor: u64) {
        let connected =
            self.network_handle.inner_handle().connected_peers().await.unwrap_or_default();
        let by_owner = Self::aggregate_by_owner(by_sender, self.committee.len());
        let connected = &connected;

        futures::future::join_all(
            by_owner.into_iter().enumerate().filter(|(_, txns)| !txns.is_empty()).map(
                |(owner, txns)| async move {
                    for message in
                        Self::chunk_under_budget(txns, self.direct_budget, MAX_TXNS_PER_MESSAGE)
                    {
                        self.submit_message(owner as u64, connected, message, anchor).await;
                    }
                },
            ),
        )
        .await;
    }

    /// Deliver legacy gossip streams. A sender run must remain separate because the receiver
    /// derives a whole message's owner from its first transaction.
    async fn publish_to_gossip(&self, by_sender: AddressMap<SenderGroup>, anchor: u64) {
        for (_, group) in by_sender.into_values() {
            for message in Self::chunk_under_budget(
                Self::nonce_sorted(group),
                self.gossip_budget,
                MAX_TXNS_PER_MESSAGE,
            ) {
                self.publish_message(message, anchor).await;
            }
        }
    }

    /// Submit a message to the first live validator on the ring from the owner's slot.
    ///
    /// A failure or timeout walks to the next connected validator; if all fail, the message stays
    /// unstamped and the next tick retries it. An acknowledged stale hash is never re-sent while it
    /// stays pending, whereas every accepted hash is stamped with the current retry anchor.
    async fn submit_message(
        &self,
        owner: u64,
        connected: &[BlsPublicKey],
        message: Vec<(TxHash, Arc<PoolTxn>)>,
        anchor: u64,
    ) {
        // Encode once outside the failover loop: a retry to the next validator re-uses the same
        // hashes and clones the `Bytes` payloads (refcount bumps, not re-serialization).
        let hashes = message.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
        let payloads: Vec<Bytes> = message.iter().map(|(_, txn)| encode_txn(txn)).collect();

        for slot in ring_walk(owner, self.committee.len() as u64) {
            let peer = self.committee[slot as usize];
            if !connected.contains(&peer) {
                continue;
            }
            match self.network_handle.submit_txns(peer, payloads.clone(), SUBMIT_TIMEOUT).await {
                Ok(stale) => {
                    debug!(
                        target: "worker::txn_forwarder",
                        txns = message.len(),
                        stale = stale.len(),
                        slot,
                        "submitted transactions"
                    );
                    let stale = Self::validate_stale(&hashes, stale);
                    if stale.is_empty() {
                        self.marks.mark_forwarded(hashes.iter().copied(), Instant::now(), anchor);
                    } else {
                        let accepted = hashes.iter().copied().filter(|hash| !stale.contains(hash));
                        self.marks.mark_forwarded(accepted, Instant::now(), anchor);
                        self.metrics.on_acked_stale(stale.len() as u64);
                        self.marks.mark_acked_stale(stale);
                    }
                    return;
                }
                Err(e) => {
                    debug!(target: "worker::txn_forwarder", ?e, slot, "submit failed, trying next live validator")
                }
            }
        }
        warn!(
            target: "worker::txn_forwarder",
            txns = message.len(),
            "no live validator accepted the message; retrying next tick"
        );
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

    /// Concatenate each owner slot's sender runs into one nonce-ordered stream.
    fn aggregate_by_owner(
        by_sender: AddressMap<SenderGroup>,
        committee_size: usize,
    ) -> Vec<Vec<(TxHash, Arc<PoolTxn>)>> {
        let mut by_owner = vec![Vec::new(); committee_size.max(1)];
        for (owner, group) in by_sender.into_values() {
            by_owner[owner as usize].extend(Self::nonce_sorted(group));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_caught_up_gates_on_local_lag() {
        assert!(TxnForwarder::is_caught_up(100, 100), "level with the anchor is caught up");
        assert!(TxnForwarder::is_caught_up(120, 100), "20 behind is at the bound, still caught up");
        assert!(!TxnForwarder::is_caught_up(121, 100), "21 behind exceeds the bound");
        assert!(
            !TxnForwarder::is_caught_up(99, 100),
            "seen below anchor (epoch reset) is not caught up"
        );
    }

    #[test]
    fn should_send_flows_first_sends_but_gates_resends() {
        let first_due = ForwardProbe { forwarded: false, due: true, attempts: 0 };
        let resend_due = ForwardProbe { forwarded: true, due: true, attempts: 1 };
        let not_due = ForwardProbe { forwarded: true, due: false, attempts: 1 };

        // a first send flows whether or not re-sends are currently allowed
        assert!(TxnForwarder::should_send(first_due, false));
        assert!(TxnForwarder::should_send(first_due, true));
        // a re-send additionally needs the caught-up gate
        assert!(!TxnForwarder::should_send(resend_due, false));
        assert!(TxnForwarder::should_send(resend_due, true));
        // nothing sends when the mark is not due
        assert!(!TxnForwarder::should_send(not_due, true));
    }

    #[test]
    fn validate_stale_keeps_only_hashes_this_node_sent() {
        let sent = [TxHash::repeat_byte(1), TxHash::repeat_byte(2)];
        // the peer claims a hash we sent plus one we never sent
        let claimed = vec![TxHash::repeat_byte(2), TxHash::repeat_byte(9)];
        let stale = TxnForwarder::validate_stale(&sent, claimed);
        assert_eq!(stale.len(), 1);
        assert!(stale.contains(&TxHash::repeat_byte(2)));
        assert!(!stale.contains(&TxHash::repeat_byte(9)), "a peer cannot suppress an unsent hash");
    }

    #[test]
    fn chunk_under_budget_splits_by_count() {
        // only the empty case is covered: a count split needs a real pooled transaction fixture
        let txns: Vec<(TxHash, std::sync::Arc<PoolTxn>)> = Vec::new();
        let chunks = TxnForwarder::chunk_under_budget(txns, usize::MAX, 2);
        assert!(chunks.is_empty(), "no transactions produce no messages");
    }
}
