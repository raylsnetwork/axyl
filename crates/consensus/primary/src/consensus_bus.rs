//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, consensus::ConsensusRound,
    proposer::OurDigestMessage, state_sync::CertificateManagerCommand, RecentlyExecutedBlocks,
};
use consensus_metrics::metered_channel::{self, channel_with_total_sender, MeteredMpscChannel};
use parking_lot::Mutex;
use rayls_consensus_network::{types::NetworkEvent, NetworkMetrics};
use rayls_consensus_primary_metrics::{ChannelMetrics, ConsensusMetrics, ExecutorMetrics, Metrics};
use rayls_infrastructure_config::Parameters;
use rayls_infrastructure_types::{
    batch_tracker::BatchTracker, error::HeaderError, BlockHash, BlockNumHash, Certificate,
    CertificateDigest, CommittedSubDag, ConsensusHeader, ConsensusOutput, Epoch, EpochVote, Header,
    RaylsReceiver, RaylsSender, Round, SendError, TryRecvError, TrySendError, CHANNEL_CAPACITY,
};

/// Gate for re-promotion to CvvActive after a DAG-behind demotion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromotionBarrier {
    /// Epoch the demotion occurred in; rounds are epoch-relative, so a barrier from a
    /// prior epoch is stale and must not gate the current one.
    pub epoch: Epoch,
    /// Highest round from the MissingParent[Round] cert.
    pub round: Round,
    /// Specific missing parent digest, if the error was MissingParent (not MissingParentRound).
    pub digest: Option<CertificateDigest>,
}

impl PromotionBarrier {
    /// Returns whether re-promotion is cleared.
    ///
    /// A prior-epoch barrier never blocks. `MissingParent` clears once its digest is persisted;
    /// `MissingParentRound` clears once `committed_round` reaches the failing round (a max-round
    /// cert-store watermark does not prove the gap filled).
    pub fn is_cleared(
        &self,
        current_epoch: Epoch,
        committed_round: Round,
        contains: impl FnOnce(&CertificateDigest) -> bool,
    ) -> bool {
        if self.epoch != current_epoch {
            return true;
        }
        match self.digest {
            Some(digest) => contains(&digest),
            None => committed_round >= self.round,
        }
    }
}
use std::sync::Arc;
use tokio::{
    sync::{
        broadcast, mpsc, oneshot,
        watch::{self, error::RecvError},
    },
    time::{error::Elapsed, Duration},
};

/// Wrapper around a receiver and a subs count to make sure only one of these exists at a time.
/// Note this does NOT implement Clone on purpose, do not implement it else managing subscriptions
/// will break.
#[derive(Debug)]
struct QueChanReceiver<T> {
    receiver: Option<mpsc::Receiver<T>>,
    container: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
}

/// Use the Drop to decrement subs.
impl<T> Drop for QueChanReceiver<T> {
    fn drop(&mut self) {
        (*self.container.lock()) = self.receiver.take();
    }
}

/// Wrapper around an mpsc channel.  It allows a channel to exist for application lifetime
/// even if used for epoch messages.  It tracks subscibers so that each epoch will be able to
/// "subscribe" to the channel (after the last epoch has dropped it's subscription).
#[derive(Debug)]
pub struct QueChannel<T> {
    channel: mpsc::Sender<T>,
    // Putting this in a lock is unfortunate but if want an mpsc under the hood is needed.
    receiver: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
}

impl<T> QueChannel<T> {
    /// Create a new QueChannel.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        Self { channel: tx, receiver }
    }
}

impl<T> Default for QueChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for QueChannel<T> {
    fn clone(&self) -> Self {
        Self { channel: self.channel.clone(), receiver: self.receiver.clone() }
    }
}

impl<T: Send + 'static> RaylsSender<T> for QueChannel<T> {
    async fn send(&self, value: T) -> Result<(), SendError<T>> {
        Ok(self.channel.send(value).await?)
    }

    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        Ok(self.channel.try_send(value)?)
    }

    fn subscribe(&self) -> impl RaylsReceiver<T> + 'static {
        let receiver = self.receiver.lock().take();
        if receiver.is_none() {
            panic!("Another subscription is already in use!")
        }
        QueChanReceiver { receiver, container: self.receiver.clone() }
    }
}

impl<T: Send + 'static> RaylsReceiver<T> for QueChanReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        self.receiver.as_mut().expect("receiver").recv().await
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        Ok(self.receiver.as_mut().expect("receiver").try_recv()?)
    }

    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<T>> {
        self.receiver.as_mut().expect("receiver").poll_recv(cx)
    }
}

/// Node mode, seeded at boot from `Config::observer` and updated by `identify_node_mode`.
///
/// No `Default`: an unseeded `CvvActive` snapshot would leak voting behavior to
/// any task that subscribes before `identify_node_mode` runs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NodeMode {
    /// Full CVV actively voting in the current committee.
    CvvActive,
    /// Staked CVV catching up, allowed to sync past the GC window and rejoin.
    CvvInactive,
    /// Follower not in the committee (staked or unstaked).
    Observer,
}

impl NodeMode {
    /// True if this node is an active CVV.
    pub fn is_active_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive)
    }

    /// True if this node is a CVV (i.e. staked and able to participate in a committee).
    pub fn is_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive | NodeMode::CvvInactive)
    }

    /// True if this node is only an obsever and will never participate in an committee.
    pub fn is_observer(&self) -> bool {
        matches!(self, NodeMode::Observer)
    }

    /// True if this node should run a batch builder (active CVV sequences, observer disburses).
    ///
    /// A catching-up `CvvInactive` node must not: with no proposer draining `our_digests`, a
    /// sealed batch wedges the worker batch-builder on `report_own_batch`, and that Drainable task
    /// never observes shutdown, stalling the epoch-transition drain.
    pub fn is_batch_producing(&self) -> bool {
        matches!(self, NodeMode::CvvActive | NodeMode::Observer)
    }
}

/// The thread-safe inner type that holds all the channels for inner-consensus
/// communication between different tasks.
/// This contains things that exist for the app lifetime.
#[derive(Clone, Debug)]
struct ConsensusBusAppInner {
    /// Outputs the highest committed round & corresponding gc_round in the consensus.
    tx_committed_round_updates: watch::Sender<Round>,

    /// Outputs the highest gc_round from the consensus.
    tx_gc_round_updates: watch::Sender<Round>,

    /// An epoch we need an epoch record for.
    tx_requested_missing_epoch: watch::Sender<Epoch>,

    /// Signals a new round
    tx_primary_round_updates: watch::Sender<Round>,

    /// Watch tracking the most recently executed blocks
    tx_recently_executed_blocks: watch::Sender<RecentlyExecutedBlocks>,

    /// The EVM-execution anchor: the consensus header the highest executed block commits to.
    /// Single source of truth for the restart/replay watermark, seeded once at boot from the
    /// chain and advanced live by the engine, so consumers never re-derive it from
    /// recently_executed_blocks.
    tx_executed_anchor: watch::Sender<ConsensusHeader>,

    /// True when the node-scoped engine has executed everything it admitted (its queue is empty
    /// and no execution is in flight). A mode transition waits on this to drain the engine's
    /// admitted backlog before the next epoch starts, so `get_missing_consensus` does not race
    /// it.
    tx_engine_idle: watch::Sender<bool>,

    /// Watch tracking most recently seen consensus header.
    tx_last_consensus_header: watch::Sender<ConsensusHeader>,
    /// Watch tracking the last gossipped consensus block number and hash.
    tx_last_published_consensus_num_hash: watch::Sender<(u64, BlockHash)>,

    /// Consensus output with a consensus header.
    consensus_output: broadcast::Sender<ConsensusOutput>,
    /// Consensus header.  Note this can be used to create consensus output to execute for non
    /// validators.
    consensus_header: broadcast::Sender<ConsensusHeader>,
    /// Status of sync?
    tx_sync_status: watch::Sender<NodeMode>,
    /// Produce new epoch certs as they are recieved.
    new_epoch_votes: QueChannel<(EpochVote, oneshot::Sender<Result<(), HeaderError>>)>,
    /// The que channel for primary network events.
    primary_network_events: QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>>,

    /// Hold onto the network metrics (mostly for testing)
    network_metrics: Arc<NetworkMetrics>,
    /// Hold onto the consensus_metrics (mostly for testing)
    consensus_metrics: Arc<ConsensusMetrics>,
    /// Hold onto the primary metrics (allow early creation)
    primary_metrics: Arc<Metrics>,
    /// Hold onto the channel metrics.
    channel_metrics: Arc<ChannelMetrics>,
    /// Hold onto the executor metrics.
    executor_metrics: Arc<ExecutorMetrics>,
    /// Signal that the subscriber has finished replaying missed consensus outputs on startup.
    /// The proposer waits for this before creating headers, preventing stale exec_digest
    /// from recently_executed_blocks that was populated from MDBX before execution replay
    /// completed.
    tx_execution_replay_complete: watch::Sender<bool>,
    /// Batch lifecycle tracker (app-lifetime, survives epoch transitions).
    batch_tracker: Arc<BatchTracker>,
    /// Mode transition request. The handler or subscriber writes the target
    /// `NodeMode` via `send_replace` and the manager detects it via `changed()`.
    /// Uses `watch` so the latest desired state always wins (no queuing).
    mode_transition: watch::Sender<Option<NodeMode>>,
    /// Highest round written to the cert store by the cert manager.
    cert_store_round: watch::Sender<Round>,
    /// Blocks re-promotion until the cert store covers the barrier round.
    /// `None` means no barrier active. Set atomically via `send_modify`.
    promotion_barrier: watch::Sender<Option<PromotionBarrier>>,
}

impl ConsensusBusAppInner {
    fn new(initial_mode: NodeMode, recently_executed_blocks: u32) -> Self {
        let network_metrics = Arc::new(NetworkMetrics::default());
        let consensus_metrics = Arc::new(ConsensusMetrics::default());
        let primary_metrics = Arc::new(Metrics::default()); // Initialize the metrics
        let channel_metrics = Arc::new(ChannelMetrics::default());
        let executor_metrics = Arc::new(ExecutorMetrics::default());
        let (tx_committed_round_updates, _) = watch::channel(Round::default());
        let (tx_gc_round_updates, _) = watch::channel(Round::default());
        let (tx_requested_missing_epoch, _) = watch::channel(Epoch::default());
        let (tx_primary_round_updates, _) = watch::channel(0u32);
        let (tx_last_consensus_header, _) = watch::channel(ConsensusHeader::default());
        let (tx_last_published_consensus_num_hash, _) = watch::channel((0, BlockHash::default()));
        let (tx_recently_executed_blocks, _) =
            watch::channel(RecentlyExecutedBlocks::new(recently_executed_blocks as usize));
        let (tx_executed_anchor, _) = watch::channel(ConsensusHeader::default());
        let (tx_engine_idle, _) = watch::channel(false);
        let (tx_execution_replay_complete, _) = watch::channel(false);

        let (tx_sync_status, _) = watch::channel(initial_mode);
        let (mode_transition, _) = watch::channel(None);

        let (consensus_header, _) = broadcast::channel(CHANNEL_CAPACITY);
        // Use CHANNEL_CAPACITY to match upstream MPSC capacity and prevent message accumulation
        // when any subscriber lags (broadcast channels keep ALL messages until ALL subscribers
        // read)
        let (consensus_output, _) = broadcast::channel(CHANNEL_CAPACITY);

        Self {
            tx_committed_round_updates,
            tx_gc_round_updates,
            tx_requested_missing_epoch,
            tx_primary_round_updates,
            tx_recently_executed_blocks,
            tx_executed_anchor,
            tx_engine_idle,
            tx_last_consensus_header,
            tx_last_published_consensus_num_hash,
            consensus_output,
            consensus_header,
            tx_sync_status,
            new_epoch_votes: QueChannel::new(),
            primary_network_events: QueChannel::new(),
            network_metrics,
            consensus_metrics,
            primary_metrics,
            channel_metrics,
            executor_metrics,
            tx_execution_replay_complete,
            batch_tracker: Arc::new(BatchTracker::new()),
            mode_transition,
            cert_store_round: watch::channel(0).0,
            promotion_barrier: watch::channel(None).0,
        }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    fn reset_for_epoch(&self) {
        self.cert_store_round.send_replace(0);
        self.tx_committed_round_updates.send_replace(Round::default());
        self.tx_gc_round_updates.send_replace(Round::default());
        self.tx_primary_round_updates.send_replace(0u32);
        self.tx_execution_replay_complete.send_replace(false);
        // NOTE: promotion_barrier is intentionally NOT reset here. It is node-lifetime intent
        // that must survive the same-epoch mode-change restart the demotion itself triggers; it
        // self-invalidates via its epoch tag and is cleared lazily in try_rejoin_consensus.
        let recently_executed_blocks = self.tx_recently_executed_blocks.borrow().block_capacity();
        // Hang onto the last block of the previous epoch, clear the rest.
        let latest = self.tx_recently_executed_blocks.borrow().latest_block();
        let mut recently_executed_blocks =
            RecentlyExecutedBlocks::new(recently_executed_blocks as usize);
        recently_executed_blocks.push_latest(latest.into_header());
        self.tx_recently_executed_blocks.send_replace(recently_executed_blocks);
    }
}

/// The thread-safe inner type that holds all the channels for inner-consensus
/// communication between different tasks.
/// These are things that are refreshed each Epoch.
#[derive(Clone, Debug)]
struct ConsensusBusEpochInner {
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    new_certificates: MeteredMpscChannel<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    /// Each cert's `bool` is `true` when its committed subdag reaches the epoch boundary, so its
    /// output is dropped by the subscriber cut and its batches must not be cleaned up.
    committed_certificates: MeteredMpscChannel<(Round, Vec<(Certificate, bool)>)>,

    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    certificate_fetcher: MeteredMpscChannel<CertificateFetcherCommand>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    parents: MeteredMpscChannel<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    our_digests: MeteredMpscChannel<OurDigestMessage>,
    /// Sends newly created headers to the `Certifier`.
    headers: MeteredMpscChannel<Header>,
    /// Updates when headers were committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    /// Each round's `bool` is `true` when its commit reaches the epoch boundary (output dropped by
    /// the subscriber cut), so the proposer must skip `NodeBatchesCache` cleanup for that header.
    committed_own_headers: MeteredMpscChannel<(Round, Vec<(Round, bool)>)>,

    /// Outputs the sequence of ordered certificates to the application layer.
    sequence: MeteredMpscChannel<CommittedSubDag>,

    /// Messages to the Certificate Manager.
    certificate_manager: MeteredMpscChannel<CertificateManagerCommand>,

    /// Drain signal from manager to subscriber. Manager sends `Some(boundary_round)` to
    /// initiate drain with the deterministic epoch boundary round. `None` means no drain.
    drain_signal: watch::Sender<Option<Round>>,

    /// Subscriber sends drain acknowledgment when all in-flight work is complete.
    /// Wrapped in Arc<Mutex<Option>> because oneshot::Sender is consumed on use and is not Clone.
    drain_ack_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,

    /// Manager receives drain acknowledgment from subscriber.
    /// Wrapped in Arc<Mutex<Option>> because oneshot::Receiver is consumed on use and is not
    /// Clone.
    drain_ack_rx: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl ConsensusBusEpochInner {
    fn new(app_inner: &ConsensusBusAppInner) -> Self {
        let new_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_new_certificates,
        );

        let committed_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_committed_certificates,
        );

        let our_digests = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_our_digests,
            &app_inner.primary_metrics.primary_channel_metrics.tx_our_digests_total,
        );
        let parents = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_parents,
            &app_inner.primary_metrics.primary_channel_metrics.tx_parents_total,
        );
        let headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_headers,
            &app_inner.primary_metrics.primary_channel_metrics.tx_headers_total,
        );
        let certificate_fetcher = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_certificate_fetcher,
            &app_inner.primary_metrics.primary_channel_metrics.tx_certificate_fetcher_total,
        );
        let committed_own_headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_committed_own_headers,
            &app_inner.primary_metrics.primary_channel_metrics.tx_committed_own_headers_total,
        );

        let certificate_manager = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &app_inner.primary_metrics.primary_channel_metrics.tx_certificate_acceptor,
            &app_inner.primary_metrics.primary_channel_metrics.tx_certificate_acceptor_total,
        );

        let sequence = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &app_inner.channel_metrics.tx_sequence,
        );

        let (drain_signal, _) = watch::channel(None);
        let (drain_ack_tx, drain_ack_rx) = oneshot::channel();

        Self {
            new_certificates,
            committed_certificates,
            certificate_fetcher,
            parents,
            our_digests,
            headers,
            committed_own_headers,
            sequence,
            certificate_manager,
            drain_signal,
            drain_ack_tx: Arc::new(Mutex::new(Some(drain_ack_tx))),
            drain_ack_rx: Arc::new(Mutex::new(Some(drain_ack_rx))),
        }
    }
}

/// The type that holds the collection of send/sync channels for
/// inter-task communication during consensus.
#[derive(Clone, Debug)]
pub struct ConsensusBus {
    /// The inner type to make this thread-safe and cheap to own.
    /// This is for stuff that lasts the app lifetime.
    inner_app: Arc<ConsensusBusAppInner>,
    /// The inner type to make this thread-safe and cheap to own.
    /// This is for stuff that lasts an epoch lifetime.
    inner_epoch: Arc<ConsensusBusEpochInner>,
}

impl Default for ConsensusBus {
    fn default() -> Self {
        Self::new()
    }
}

/// This contains the shared consensus channels and the prometheus metrics
/// containers (used mostly to track consensus messages).
/// A new bus can be created with new() but there should only ever be one created (except for
/// tests). This allows us to not create and pass channels all over the place add-hoc.
/// It also allows makes it much easier to find where channels are fed and consumed.
impl ConsensusBus {
    /// Create a test-only bus seeded as `CvvActive` to preserve prior test behavior.
    pub fn new() -> Self {
        Self::new_with_args(NodeMode::CvvActive, Parameters::default_gc_depth())
    }

    /// Create a bus with an explicit initial mode.
    pub fn new_with_args(initial_mode: NodeMode, recently_executed_blocks: u32) -> Self {
        let inner_app = Arc::new(ConsensusBusAppInner::new(initial_mode, recently_executed_blocks));
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new(&inner_app));
        Self { inner_app, inner_epoch }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    pub fn reset_for_epoch(&mut self) {
        self.inner_app.reset_for_epoch();
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new(&self.inner_app));
        self.inner_epoch = inner_epoch;
    }

    /// New certificates.
    ///
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    /// Can only be subscribed to once.
    pub fn new_certificates(&self) -> &impl RaylsSender<Certificate> {
        &self.inner_epoch.new_certificates
    }

    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    /// Can only be subscribed to once.
    pub fn committed_certificates(&self) -> &impl RaylsSender<(Round, Vec<(Certificate, bool)>)> {
        &self.inner_epoch.committed_certificates
    }

    /// Missing certificates.
    ///
    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn certificate_fetcher(&self) -> &impl RaylsSender<CertificateFetcherCommand> {
        &self.inner_epoch.certificate_fetcher
    }

    /// Valid quorum of certificates' ids.
    ///
    /// Sends a valid quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn parents(&self) -> &impl RaylsSender<(Vec<Certificate>, Round)> {
        &self.inner_epoch.parents
    }

    /// Contains the highest committed round & corresponding gc_round for consensus.
    pub fn committed_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner_app.tx_committed_round_updates
    }

    /// Contains the highest gc_round for consensus.
    pub fn gc_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner_app.tx_gc_round_updates
    }

    /// Contains the last requested epoch to retrieve a record.
    pub fn requested_missing_epoch(&self) -> &watch::Sender<Epoch> {
        &self.inner_app.tx_requested_missing_epoch
    }

    /// Signals a new round
    pub fn primary_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner_app.tx_primary_round_updates
    }

    /// Batches' digests from our workers.
    /// Can only be subscribed to once.
    pub fn our_digests(&self) -> &impl RaylsSender<OurDigestMessage> {
        &self.inner_epoch.our_digests
    }

    /// Sends newly created headers to the `Certifier`.
    /// Can only be subscribed to once.
    pub fn headers(&self) -> &impl RaylsSender<Header> {
        &self.inner_epoch.headers
    }

    /// Updates when headers are committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    /// Can only be subscribed to once.
    pub fn committed_own_headers(&self) -> &impl RaylsSender<(Round, Vec<(Round, bool)>)> {
        &self.inner_epoch.committed_own_headers
    }

    /// Outputs the sequence of ordered certificates from consensus.
    /// Can only be subscribed to once.
    pub fn sequence(&self) -> &impl RaylsSender<CommittedSubDag> {
        &self.inner_epoch.sequence
    }

    /// Channel for forwarding newly received certificates for verification.
    ///
    /// These channels are used to communicate with the long-running CertificateManager task.
    /// Can only be subscribed to once.
    pub(crate) fn certificate_manager(&self) -> &impl RaylsSender<CertificateManagerCommand> {
        &self.inner_epoch.certificate_manager
    }

    /// Drain signal sender for the manager to initiate subscriber drain.
    /// Send `Some(boundary_round)` to signal the subscriber with the deterministic cutoff round.
    pub fn drain_signal(&self) -> &watch::Sender<Option<Round>> {
        &self.inner_epoch.drain_signal
    }

    /// Take the drain acknowledgment sender for the subscriber.
    /// Returns `None` if already taken (can only be taken once per epoch).
    pub fn take_drain_ack_tx(&self) -> Option<oneshot::Sender<()>> {
        self.inner_epoch.drain_ack_tx.lock().take()
    }

    /// Take the drain acknowledgment receiver for the manager.
    /// Returns `None` if already taken (can only be taken once per epoch).
    pub fn take_drain_ack_rx(&self) -> Option<oneshot::Receiver<()>> {
        let rx = self.inner_epoch.drain_ack_rx.lock().take();
        if rx.is_none() {
            tracing::warn!(target: "consensus-bus", "drain_ack_rx already taken or never created");
        }
        rx
    }

    /// Track the most recently executed blocks (a bounded window, newest at the tip).
    ///
    /// Safe to read for block *numbers* and *hashes* - those are monotonic. But the tip's nonce
    /// (`epoch << 32 | round`) is NOT monotonic - neither half: draining a parked (out-of-order
    /// seq) batch executes a block belonging to an OLDER output that still lands as the newest
    /// height, so the tip's round (and, for a batch carried over from a previous epoch, its epoch)
    /// can regress far below the true execution frontier.
    ///
    /// Example: execution has genuinely reached round 498. A batch for an earlier seq, mapping to
    /// round 200, was parked; the gap then fills and it is drained and executed now. That fresh
    /// block gets the next (highest) block number and becomes the tip, but its nonce encodes round
    /// 200. A caller reading the round off the tip sees 200, not 498. The proposer throttle did
    /// exactly this: with consensus at round 500 it computed lag `500 - 200 = 300 > threshold` and
    /// throttled forever, wedging proposals - when the real lag was `500 - 498 = 2`.
    ///
    /// For the frontier epoch/round read the monotonic [`Self::executed_anchor`] instead, or scan
    /// this window for the max-nonce block.
    pub fn recently_executed_blocks(&self) -> &watch::Sender<RecentlyExecutedBlocks> {
        &self.inner_app.tx_recently_executed_blocks
    }

    /// The EVM-execution anchor: the consensus header the highest executed block commits to.
    ///
    /// Single source of truth for the restart/replay watermark. Seeded once at boot from the
    /// chain (highest-nonce recent block) and advanced live by the engine on each executed output;
    /// replay/catch-up consumers read this instead of re-deriving it from
    /// `recently_executed_blocks` (whose tip can lag behind after a drained parked batch).
    /// Distinct from the peer-derived [`Self::last_consensus_header`], which is for header
    /// numbering only.
    pub fn executed_anchor(&self) -> &watch::Sender<ConsensusHeader> {
        &self.inner_app.tx_executed_anchor
    }

    /// True when the node-scoped engine has executed everything it admitted (queue empty, nothing
    /// in flight). A mode transition waits on this so the engine's admitted backlog finishes before
    /// the next epoch's `get_missing_consensus` snapshot — otherwise a concurrently-finishing
    /// output is dropped as stale (the demote→rejoin flap race).
    pub fn engine_idle(&self) -> &watch::Sender<bool> {
        &self.inner_app.tx_engine_idle
    }

    /// Signal that execution replay of missed consensus outputs is complete.
    /// Set by the subscriber after replaying, read by the proposer before creating headers.
    pub fn execution_replay_complete(&self) -> &watch::Sender<bool> {
        &self.inner_app.tx_execution_replay_complete
    }

    /// Track the latest consensus header we have seen.
    /// Note, this should be a valid header (authenticated by it's epoch's committee).
    pub fn last_consensus_header(&self) -> &watch::Sender<ConsensusHeader> {
        &self.inner_app.tx_last_consensus_header
    }

    /// Track the latest published consensus header block number and hash seen on the gossip
    /// network. This value will have been verified and can be trusted to be the correct hash
    /// for block number.  DO NOT send unverified values to this watch.
    pub fn last_published_consensus_num_hash(&self) -> &watch::Sender<(u64, BlockHash)> {
        &self.inner_app.tx_last_published_consensus_num_hash
    }

    /// Broadcast channel with consensus output (includes the consensus chain block).
    /// This also provides the ConsesusHeader, use this for block execution.
    pub fn consensus_output(&self) -> &impl RaylsSender<ConsensusOutput> {
        &self.inner_app.consensus_output
    }

    /// Broadcast subscriber with consensus output.
    /// This breaks the trait pattern in order to return a concrete receiver to pass to the
    /// execution module.
    pub fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        self.inner_app.consensus_output.subscribe()
    }

    /// Broadcast channel with consensus header.
    /// This is useful pre-consensus output when not participating in consensus.
    pub fn consensus_header(&self) -> &impl RaylsSender<ConsensusHeader> {
        &self.inner_app.consensus_header
    }

    /// Status of initial sync operation.
    pub fn node_mode(&self) -> &watch::Sender<NodeMode> {
        &self.inner_app.tx_sync_status
    }

    /// Highest round written to the cert store by the cert manager.
    pub fn cert_store_round(&self) -> &watch::Sender<Round> {
        &self.inner_app.cert_store_round
    }

    /// Promotion barrier raised on MissingParent[Round] demote.
    /// `None` = no barrier. Cleared by subscriber when condition is met.
    pub fn promotion_barrier(&self) -> &watch::Sender<Option<PromotionBarrier>> {
        &self.inner_app.promotion_barrier
    }

    /// Batch lifecycle tracker (app-lifetime).
    pub fn batch_tracker(&self) -> &Arc<BatchTracker> {
        &self.inner_app.batch_tracker
    }

    /// Request a mode transition. Callers use `send_replace(Some(target_mode))`
    /// to signal the manager. The manager subscribes and detects changes via
    /// `changed()`, then performs a controlled shutdown with drain.
    pub fn mode_transition(&self) -> &watch::Sender<Option<NodeMode>> {
        &self.inner_app.mode_transition
    }

    /// Request a transition to `target`, idempotent. Observer is sticky.
    pub fn request_mode_transition(&self, target: NodeMode) -> bool {
        let current = *self.node_mode().borrow();
        if current == NodeMode::Observer || current == target {
            return false;
        }
        self.inner_app.mode_transition.send_if_modified(|current| {
            if *current == Some(target) {
                false
            } else {
                *current = Some(target);
                true
            }
        })
    }

    /// Return the channel for primary network events.
    pub fn primary_network_events(
        &self,
    ) -> &impl RaylsSender<NetworkEvent<crate::network::Req, crate::network::Res>> {
        &self.inner_app.primary_network_events
    }

    /// Return the channel for primary network events.  Returns a concrete clone.
    pub fn primary_network_events_cloned(
        &self,
    ) -> QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner_app.primary_network_events.clone()
    }

    /// Hold onto the consensus_metrics (mostly for testing)
    pub fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.inner_app.consensus_metrics.clone()
    }

    pub fn network_metrics(&self) -> Arc<NetworkMetrics> {
        self.inner_app.network_metrics.clone()
    }

    /// Hold onto the primary metrics (allow early creation)
    pub fn primary_metrics(&self) -> Arc<Metrics> {
        self.inner_app.primary_metrics.clone()
    }

    /// Hold onto the channel metrics (metrics for the sequence channel).
    pub fn channel_metrics(&self) -> Arc<ChannelMetrics> {
        self.inner_app.channel_metrics.clone()
    }

    /// Hold onto the executor metrics
    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.inner_app.executor_metrics
    }

    /// New epoch certs as they are recieved.
    pub fn new_epoch_votes(
        &self,
    ) -> &impl RaylsSender<(EpochVote, oneshot::Sender<Result<(), HeaderError>>)> {
        &self.inner_app.new_epoch_votes
    }

    /// Update consensus round watch channels.
    ///
    /// This sends both the gc round and the committed round to the respective watch channels after
    /// consensus updates.
    pub fn update_consensus_rounds(&self, update: ConsensusRound) {
        let ConsensusRound { committed_round, gc_round } = update;
        self.gc_round_updates().send_replace(gc_round);
        self.committed_round_updates().send_replace(committed_round);
    }

    /// Resolves once the target block has executed, or returns a [`WaitForExecutionError`].
    ///
    /// The unbounded form returns `Forked` on a divergent hash at the target number or `Closed` if
    /// `recently_executed_blocks` closes; never `Stalled` (use [`Self::wait_for_execution_bounded`]
    /// for that).
    pub async fn wait_for_execution(
        &self,
        block: BlockNumHash,
    ) -> Result<(), WaitForExecutionError> {
        self.wait_for_execution_inner(block, None).await
    }

    /// Like [`Self::wait_for_execution`], but bounds the idle gap between ticks by `idle_timeout`.
    ///
    /// A stalled or forked target errors after `idle_timeout` with no new block instead of parking
    /// forever; a progressing chain resets the window on every tick.
    pub async fn wait_for_execution_bounded(
        &self,
        block: BlockNumHash,
        idle_timeout: Duration,
    ) -> Result<(), WaitForExecutionError> {
        self.wait_for_execution_inner(block, Some(idle_timeout)).await
    }

    async fn wait_for_execution_inner(
        &self,
        block: BlockNumHash,
        idle_timeout: Option<Duration>,
    ) -> Result<(), WaitForExecutionError> {
        let mut watch_execution_result = self.recently_executed_blocks().subscribe();
        let target_number = block.number;
        let target_hash = block.hash;

        // Make sure that our recently-executed window is not empty.  If it is we can have a race
        // around block 0.
        while self.recently_executed_blocks().borrow().is_empty() {
            await_execution_tick(&mut watch_execution_result, idle_timeout).await?;
        }
        let mut current_number =
            self.recently_executed_blocks().borrow().latest_block_num_hash().number;
        while current_number < target_number {
            await_execution_tick(&mut watch_execution_result, idle_timeout).await?;
            current_number =
                self.recently_executed_blocks().borrow().latest_block_num_hash().number;
        }

        let recent = self.recently_executed_blocks().borrow();
        let oldest_number = recent.oldest_block_number();
        let latest_number = recent.latest_block_num_hash().number;

        // block older than recently_executed_blocks window, assume already executed
        if target_number < oldest_number {
            tracing::debug!(
                target: "consensus-bus",
                target_number,
                oldest_number,
                latest_number,
                ?target_hash,
                "wait_for_execution: block older than recently_executed_blocks window, assuming already executed"
            );
            return Ok(());
        }

        if recent.contains_hash(block.hash) {
            // Once we see our hash, should happen when current_number == target_number- trust
            // digesting for this, we are done.
            Ok(())
        } else {
            // hash mismatch - potential fork
            let block_at_target = recent.block_at_number(target_number);
            tracing::error!(
                target: "consensus-bus",
                target_number,
                ?target_hash,
                oldest_number,
                latest_number,
                block_at_target_hash = ?block_at_target.map(|b| b.hash()),
                "wait_for_execution: HASH MISMATCH - potential fork detected! \
                 Expected hash not found in recently_executed_blocks at target number."
            );
            Err(WaitForExecutionError::Forked)
        }
    }
}

/// Maximum idle gap `wait_for_execution_bounded` tolerates before treating the target as stalled;
/// a progressing chain resets it every block.
pub const EXECUTION_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Await the next `recently_executed_blocks` change, optionally bounded by `idle_timeout`.
///
/// Returns `Closed` if the receiver closes, or `Stalled` when `idle_timeout` is set and no change
/// arrives within that window.
async fn await_execution_tick(
    rx: &mut watch::Receiver<RecentlyExecutedBlocks>,
    idle_timeout: Option<Duration>,
) -> Result<(), WaitForExecutionError> {
    match idle_timeout {
        Some(timeout) => tokio::time::timeout(timeout, rx.changed()).await??,
        None => rx.changed().await?,
    }
    Ok(())
}

/// Why [`ConsensusBus::wait_for_execution`] stopped before confirming the target executed.
///
/// Surfaces a precise cause in the caller's logs and lets a test tell a stalled chain apart from a
/// divergent one; callers do not branch on it today (both reachable causes are logged and retried).
/// The unbounded wait yields only `Forked`; the bounded wait can also yield `Stalled`.
#[derive(Copy, Clone, Debug, thiserror::Error)]
pub enum WaitForExecutionError {
    /// Execution made no progress within the bounded idle window (a stalled chain).
    #[error("execution made no progress within the idle timeout")]
    Stalled,
    /// `recently_executed_blocks` reported its sender closed. Defensive only: the sole sender is
    /// owned for the app lifetime by the same `ConsensusBus` the wait borrows, so this is not
    /// expected to fire.
    #[error("recently_executed_blocks closed before the target executed")]
    Closed,
    /// Execution reached the target number but the block hash there differs: a fork.
    #[error("execution reached the target number with a divergent block hash")]
    Forked,
}

impl From<Elapsed> for WaitForExecutionError {
    fn from(_: Elapsed) -> Self {
        Self::Stalled
    }
}

impl From<RecvError> for WaitForExecutionError {
    fn from(_: RecvError) -> Self {
        Self::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsensusBus, PromotionBarrier};
    use rayls_infrastructure_types::CertificateDigest;

    // MissingParentRound (digest=None) must gate on committed_round, not a max-round cert-store
    // watermark: a far-behind node fetches high-round certs out of order, so the max is already
    // past the gap. It clears only when committed_round reaches the failing round, since
    // committed subdags backfill contiguous causal history.
    #[test]
    fn missing_parent_round_clears_only_when_committed_reaches_round() {
        let barrier = PromotionBarrier { epoch: 5, round: 100, digest: None };
        assert!(!barrier.is_cleared(5, 99, |_| false), "must block until committed reaches round");
        assert!(barrier.is_cleared(5, 100, |_| false), "clears once committed reaches round");
    }

    // Rounds are epoch-relative, so a barrier raised in a prior epoch is stale and never blocks.
    #[test]
    fn prior_epoch_barrier_is_stale() {
        let barrier = PromotionBarrier { epoch: 5, round: 100, digest: None };
        assert!(barrier.is_cleared(6, 0, |_| false), "a prior-epoch barrier must not gate");
    }

    // MissingParent (digest=Some) clears exactly when the specific parent is persisted.
    #[test]
    fn missing_parent_clears_on_digest_present() {
        let digest = CertificateDigest::new([7u8; 32]);
        let barrier = PromotionBarrier { epoch: 5, round: 100, digest: Some(digest) };
        assert!(barrier.is_cleared(5, 0, |d| *d == digest), "clears when parent digest present");
        assert!(!barrier.is_cleared(5, 0, |_| false), "blocks while parent digest absent");
    }

    // The barrier is node-lifetime intent: it must survive the same-epoch mode-change restart the
    // demotion itself triggers, so reset_for_epoch must not clear it.
    #[tokio::test]
    async fn reset_for_epoch_preserves_promotion_barrier() {
        let mut bus = ConsensusBus::new();
        let barrier = PromotionBarrier { epoch: 5, round: 100, digest: None };
        bus.promotion_barrier().send_replace(Some(barrier.clone()));
        bus.reset_for_epoch();
        assert_eq!(
            *bus.promotion_barrier().borrow(),
            Some(barrier),
            "promotion barrier must survive a same-epoch mode-change restart"
        );
    }
}
