//! Tasks and helpers for collecting consensus headers trustlessly.
use futures::{stream::FuturesUnordered, StreamExt};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use rayls_infrastructure_config::{ConsensusConfig, EpochKeysStatus};
use rayls_infrastructure_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache},
    ConsensusStore, EpochStore as _,
};
use rayls_infrastructure_types::{
    ConsensusHeader, ConsensusHeaderChainMeta, Database as ReDatabase, DbTxMut as _, Epoch,
    EpochKeys, Noticer, B256,
};
use std::{ops::ControlFlow, sync::LazyLock, time::Duration};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

const COMMITTEE_KEYS_TIMEOUT: Duration = Duration::from_secs(30);
const COMMITTEE_KEYS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How long to wait for a gossip tip before actively probing peers for the network head.
const PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Cache a consensus header in persistent storage.
pub fn store_consensus_header_in_cache<DB: ReDatabase>(db: &DB, header: &ConsensusHeader) {
    let result = db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocksCache>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        Ok(())
    });
    if let Err(e) = result {
        error!(target: "state-sync", ?e, "error storing consensus header in cache");
    }
}

/// Ask the epoch record collector for records up to `epoch`.
fn request_missing_epoch(consensus_bus: &ConsensusBus, epoch: Epoch) {
    // the collector iterates upward from its last_epoch, so requesting
    // the highest unknown epoch bridges all intermediate gaps
    consensus_bus.requested_missing_epoch().send_if_modified(|current| {
        if epoch > *current {
            *current = epoch;
            true
        } else {
            false
        }
    });
}

/// Trigger the epoch record collector and poll until committee keys arrive. They come with the
/// epoch's certificate scheme.
///
/// Return `None` on shutdown or timeout, and at once for an epoch whose threshold key this node
/// has not computed yet: it comes from the previous epoch's headers, which a backwards walk
/// reaches only after this one.
pub(crate) async fn wait_for_committee_keys<DB: ReDatabase>(
    epoch: Epoch,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    shutdown: &Noticer,
) -> Option<EpochKeys> {
    match config.epoch_keys_status(epoch) {
        EpochKeysStatus::Ready(keys) => return Some(keys),
        EpochKeysStatus::Pending => {
            debug!(target: "state-sync", epoch, "backwards walk: threshold key pending");
            return None;
        }
        EpochKeysStatus::NoCommittee => {}
    }

    request_missing_epoch(consensus_bus, epoch);

    info!(
        target: "state-sync",
        epoch,
        "backwards walk: waiting for committee keys (triggered epoch record collector)"
    );

    let deadline = tokio::time::Instant::now() + COMMITTEE_KEYS_TIMEOUT;
    loop {
        if shutdown.noticed() {
            return None;
        }
        match config.epoch_keys_status(epoch) {
            EpochKeysStatus::Ready(keys) => {
                info!(target: "state-sync", epoch, "backwards walk: committee keys arrived");
                return Some(keys);
            }
            EpochKeysStatus::Pending => {
                debug!(target: "state-sync", epoch, "backwards walk: threshold key pending");
                return None;
            }
            EpochKeysStatus::NoCommittee => {}
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(target: "state-sync", epoch, "backwards walk: timed out waiting for committee keys");
            return None;
        }
        tokio::time::sleep(COMMITTEE_KEYS_POLL_INTERVAL).await;
    }
}

/// Republish `header` as the latest consensus tip when it advances the current watermark.
///
/// The compare-and-set is atomic under the watch lock, so it never overwrites a higher tip with a
/// lower one.
fn republish_if_higher(consensus_bus: &ConsensusBus, header: &ConsensusHeader) {
    let advanced = consensus_bus.last_consensus_header().send_if_modified(|current| {
        let advance = header.number > current.number;
        if advance {
            *current = header.clone();
        }
        advance
    });
    if advanced {
        info!(target: "state-sync", header_number = header.number, "notifying watchers");
    }
}

/// One step of the backwards walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Keep descending from this parent hash.
    Parent(B256),
    /// The canonical anchor was reached, or the header could not be fetched or verified.
    Stop,
    /// The header leads in this later epoch, whose threshold key this node has not computed yet.
    /// That key comes from this epoch's headers, so the walk must cover this epoch first.
    PendingAbove(Epoch),
}

/// Descend one step of the backwards walk: stop at the canonical anchor, traverse a cached header,
/// or fetch and BLS-verify from a peer.
async fn get_consensus_header<DB: ReDatabase>(
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Step {
    let db = config.node_storage();
    // Stop only at the canonical anchor (never an unverified cache row); its per-row digest==hash
    // check means a stale, never-pruned ByDigest index entry cannot false-stop the walk.
    if let Some(block) = db.get_canonical_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &block);
        return Step::Stop;
    }
    // Traverse a cached header from the DB instead of refetching it, and keep descending.
    if let Some(cached) = db.get_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &cached);
        return Step::Parent(cached.parent_hash);
    }
    for attempt in 0u32..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
        }
        if shutdown.noticed() {
            return Step::Stop;
        }
        match network.request_consensus(None, Some(hash)).await {
            Ok(header) => {
                let leader_epoch = header.sub_dag.leader_epoch();
                let Some(keys) =
                    wait_for_committee_keys(leader_epoch, config, consensus_bus, shutdown).await
                else {
                    if leader_epoch > config.epoch()
                        && config.epoch_keys_status(leader_epoch) == EpochKeysStatus::Pending
                    {
                        return Step::PendingAbove(leader_epoch);
                    }
                    warn!(
                        target: "state-sync",
                        ?hash,
                        epoch = leader_epoch,
                        "committee keys unavailable, stopping walk"
                    );
                    return Step::Stop;
                };
                let scheme = keys.scheme();
                let Ok(verified) = header.verify_header_with_scheme(&scheme).inspect_err(|e| {
                    warn!(target: "state-sync", ?hash, "BLS verification failed, discarding header: {e}");
                }) else {
                    return Step::Stop;
                };
                let parent = verified.parent_hash;
                store_consensus_header_in_cache(db, &verified);
                republish_if_higher(consensus_bus, &verified);
                return Step::Parent(parent);
            }
            Err(e) => {
                warn!(target: "state-sync", ?hash, ?e, attempt, "failed to fetch consensus header from peers");
            }
        }
    }
    Step::Stop
}

/// Digest of the genesis consensus header (number 0, the default header).
///
/// A genuine number-1 header carries this as its parent hash, so a walk that descends to it has
/// reached the chain's base and is grounded.
static GENESIS_DIGEST: LazyLock<B256> = LazyLock::new(|| ConsensusHeader::default().digest());

/// Terminal state of a backwards walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkOutcome {
    /// Linked `head` to local coverage (canonical, genesis, or the session anchor). `boundary`:
    /// the start led in a later, pending epoch, so `head` is this epoch's certified boundary.
    Covered { head: B256, boundary: bool },
    /// The start led in a later, pending epoch, and this epoch's certified record is not held yet.
    AwaitingBoundary,
    /// Stopped before reaching coverage (a failed fetch).
    Incomplete,
    /// Shutdown was noticed mid-walk.
    Shutdown,
}

/// Recovers the covered anchor: the highest cached header that hash-links, number by number, down
/// to the canonical tip (or genesis).
///
/// Re-verifies each parent link rather than trusting a row's presence, since an interrupted descent
/// can leave another fork's rows behind. [`ConsensusHeaderChainMeta`] projects each link from the
/// raw bytes without deserializing.
fn derive_covered_anchor<DB: ReDatabase>(db: &DB) -> Option<B256> {
    let (mut expected_parent, mut expected_number) = match db
        .reverse_raw_iter::<ConsensusBlocks>()
        .next()
        .map(|(_, value)| ConsensusHeaderChainMeta::from_bytes(&value))
    {
        Some(Ok(meta)) => (meta.digest, meta.number + 1),
        // an unreadable tip row proves nothing; let the walk re-fetch
        Some(Err(_)) => return None,
        None => (*GENESIS_DIGEST, 1),
    };
    let mut frontier = None;
    for (_, value) in db.raw_iter::<ConsensusBlocksCache>() {
        // an unreadable row ends the chain at the row below it
        let Ok(meta) = ConsensusHeaderChainMeta::from_bytes(&value) else { break };
        if meta.number < expected_number {
            // a stale row below the canonical tip
            continue;
        }
        // a gap or a broken/fork-overwritten link ends the proven run here
        if meta.number != expected_number || meta.parent_hash != expected_parent {
            break;
        }
        expected_parent = meta.digest;
        frontier = Some((meta.digest, meta.number));
        expected_number += 1;
    }
    frontier.map(|(digest, number)| {
        info!(target: "state-sync", anchor = number, "derived walk coverage from cached headers");
        digest
    })
}

/// How often a session waiting for its epoch's certified record asks the collector again.
const BOUNDARY_RECORD_REQUEST_INTERVAL: Duration = Duration::from_secs(30);

/// One tracker session of backwards walks, owning the coverage anchor that outlives a single walk.
///
/// In-memory on purpose: teardown drops it, and it is re-derived from the persisted cache when
/// unset, so coverage survives a restart.
pub(crate) struct WalkSession<DB> {
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    shutdown: Noticer,
    /// Digest where the last successful walk reached coverage; a later walk stops here instead of
    /// re-descending. Cleared on an incomplete walk (stale rows may lie below it), then
    /// re-derived.
    covered: Option<B256>,
    /// This epoch's certified boundary is covered. Every later epoch is pending until the
    /// transition, which starts the next session, so later tips are not walked.
    boundary_covered: bool,
    /// When this session last asked the collector for its epoch's record.
    boundary_requested_at: Option<tokio::time::Instant>,
    /// The skip for a pending epoch key was logged at info.
    pending_key_logged: bool,
}

impl<DB: ReDatabase> WalkSession<DB> {
    /// Create a session for one tracker task lifetime.
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        network: PrimaryNetworkHandle,
    ) -> Self {
        let shutdown = config.shutdown().subscribe();
        Self {
            config,
            consensus_bus,
            network,
            shutdown,
            covered: None,
            boundary_covered: false,
            boundary_requested_at: None,
            pending_key_logged: false,
        }
    }

    /// Walk backwards from `start_hash` to local coverage, recording the result for the next call.
    /// Returns `Break` when shutdown was noticed and the tracker must exit.
    pub(crate) async fn sync_to(&mut self, start_hash: B256) -> ControlFlow<()> {
        // Nothing a tip offers verifies before this epoch's key lands or the transition starts the
        // next session: past a covered boundary every epoch is pending, and so is this one while
        // its own key is.
        if self.config.scheme_pending() {
            if !std::mem::replace(&mut self.pending_key_logged, true) {
                info!(
                    target: "state-sync",
                    epoch = self.config.epoch(),
                    "backwards walk paused: this epoch's threshold key is not computed yet"
                );
            }
            return ControlFlow::Continue(());
        }
        if self.boundary_covered {
            debug!(target: "state-sync", "backwards walk: boundary covered, waiting for the transition");
            return ControlFlow::Continue(());
        }
        // Recover coverage from the persisted cache when the session has none yet (fresh task, or a
        // prior walk cleared it), so this walk stops at the top of the cached run rather than
        // re-descending it. Derivation sweeps and hashes every cached row, so it runs on a blocking
        // thread to keep the async worker responsive through a long catch-up stall; a join failure
        // just leaves the anchor unset and re-derives next call.
        if self.covered.is_none() {
            let db = self.config.node_storage().clone();
            self.covered = tokio::task::spawn_blocking(move || derive_covered_anchor(&db))
                .await
                .unwrap_or(None);
        }
        match self.walk(start_hash).await {
            // `head` now heads a verified chain to canonical; stop here next time.
            WalkOutcome::Covered { head, boundary } => {
                self.covered = Some(head);
                self.boundary_covered = boundary;
                ControlFlow::Continue(())
            }
            // Nothing was descended, so the coverage still holds.
            WalkOutcome::AwaitingBoundary => ControlFlow::Continue(()),
            // Drop the anchor so the next walk re-derives and re-descends to self-heal the gap.
            WalkOutcome::Incomplete => {
                self.covered = None;
                ControlFlow::Continue(())
            }
            WalkOutcome::Shutdown => ControlFlow::Break(()),
        }
    }

    /// Descend parent hashes from `start_hash` until reaching local coverage.
    ///
    /// A start in a later epoch whose threshold key is pending cannot verify until this epoch's
    /// headers execute, so the walk descends from this epoch's certified boundary instead. The
    /// streamer then executes up to it, the transition computes the next key, and the next epoch's
    /// walk repeats the step.
    async fn walk(&mut self, start_hash: B256) -> WalkOutcome {
        let covered_hash = self.covered;
        let mut current = start_hash;
        let mut head = start_hash;
        let mut anchored = false;
        let mut depth = 0u64;
        let outcome = loop {
            if self.shutdown.noticed() {
                return WalkOutcome::Shutdown;
            }
            // Stop at proven coverage without re-descending it: the session anchor or genesis.
            if covered_hash == Some(current) || current == *GENESIS_DIGEST {
                // A walk that starts at the anchor traverses nothing, so nothing published the
                // tip a peer just reported. Publish it here: the epoch transition resets this
                // watch, and the forward streamer and the rejoin check learn the head only from
                // it, so a cached tip would otherwise stay unexecuted until a newer header comes.
                if depth == 0 {
                    if let Some(tip) = self.config.node_storage().get_consensus_by_hash(current) {
                        republish_if_higher(&self.consensus_bus, &tip);
                    }
                }
                break WalkOutcome::Covered { head, boundary: anchored };
            }
            match get_consensus_header(
                current,
                &self.config,
                &self.consensus_bus,
                &self.network,
                &self.shutdown,
            )
            .await
            {
                Step::Parent(parent) => current = parent,
                // Below this epoch's boundary every header leads in this epoch or an earlier one,
                // so a second pending header means the chain does not hold together.
                Step::PendingAbove(tip_epoch) if !anchored => {
                    let Some(boundary) = self.certified_boundary(tip_epoch) else {
                        break WalkOutcome::AwaitingBoundary;
                    };
                    // A new session derives its coverage from the cache, which can already reach
                    // the boundary: republish it so the streamer gets its target either way.
                    if let Some(header) = self.config.node_storage().get_consensus_by_hash(boundary)
                    {
                        republish_if_higher(&self.consensus_bus, &header);
                    }
                    debug!(
                        target: "state-sync",
                        epoch = self.config.epoch(),
                        ?boundary,
                        "backwards walk: tip leads in a later epoch whose key is pending, \
                         walking from this epoch's boundary"
                    );
                    anchored = true;
                    head = boundary;
                    current = boundary;
                    continue;
                }
                Step::PendingAbove(_) => break WalkOutcome::Incomplete,
                // `get_consensus_header` stopped at the canonical anchor (Covered) or could not
                // fetch (Incomplete: coverage below `current` is unknown).
                Step::Stop => {
                    break if self
                        .config
                        .node_storage()
                        .get_canonical_consensus_by_hash(current)
                        .is_some()
                    {
                        WalkOutcome::Covered { head, boundary: anchored }
                    } else {
                        WalkOutcome::Incomplete
                    };
                }
            }
            depth += 1;
            if depth.is_multiple_of(100) {
                info!(target: "state-sync", depth, "backwards walk: progress");
            }
            tokio::task::yield_now().await;
        };
        if depth > 0 {
            info!(target: "state-sync", depth, anchored, ?outcome, "backwards walk: completed");
        }
        outcome
    }

    /// The last header of this node's epoch, from the epoch's certified record.
    ///
    /// Without that record, ask the collector for records up to the last epoch closed before the
    /// tip's, at most once per [`BOUNDARY_RECORD_REQUEST_INTERVAL`], and return `None`. The
    /// collector's pass starts at the last record it holds and refetches it when uncertified, so
    /// this also replaces the epoch-0 placeholder.
    fn certified_boundary(&mut self, tip_epoch: Epoch) -> Option<B256> {
        let epoch = self.config.epoch();
        if let Some((record, Some(_))) = self.config.node_storage().get_epoch_by_number(epoch) {
            return Some(record.parent_consensus);
        }
        let now = tokio::time::Instant::now();
        if self
            .boundary_requested_at
            .is_none_or(|at| now.duration_since(at) >= BOUNDARY_RECORD_REQUEST_INTERVAL)
        {
            self.boundary_requested_at = Some(now);
            request_missing_epoch(&self.consensus_bus, tip_epoch.saturating_sub(1).max(epoch));
        }
        None
    }
}

/// Probe all committee peers concurrently, return the highest header above `current`.
/// BLS is re-verified downstream in `get_consensus_header`.
async fn probe_peers_for_latest<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    network: &PrimaryNetworkHandle,
) -> Option<ConsensusHeader> {
    let mut inflight: FuturesUnordered<_> = config
        .committee()
        .others_primaries_by_id(config.authority_id().as_ref())
        .into_iter()
        .map(|(_, peer)| network.request_consensus_from_peer(peer, None, None))
        .collect();
    let mut best: Option<ConsensusHeader> = None;
    while let Some(result) = inflight.next().await {
        if let Ok(h) = result {
            if h.number > current && best.as_ref().map(|b| h.number > b.number).unwrap_or(true) {
                best = Some(h);
            }
        }
    }
    best
}

/// Blocks until a backwards walk is warranted, returning the tip hash to walk toward.
///
/// Resolves at once if gossip holds an unobserved tip, else waits for gossip, falling back to a
/// periodic peer probe. Returns `None` on shutdown or a closed gossip channel.
async fn next_walk_target<DB: ReDatabase>(
    rx_gossip: &mut watch::Receiver<(u64, B256)>,
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Option<B256> {
    // Counts consecutive idle probe ticks for the log; reset implicitly by returning a target.
    let mut idle_intervals = 0u64;
    loop {
        tokio::select! {
            // biased: shutdown must win; gossip and the probe timer otherwise commute.
            biased;
            _ = shutdown => return None,
            result = rx_gossip.changed() => {
                result.ok()?;
                let (_, hash) = *rx_gossip.borrow_and_update();
                return Some(hash);
            }
            _ = tokio::time::sleep(PROBE_INTERVAL) => {
                idle_intervals += 1;
                let current = consensus_bus.last_consensus_header().borrow().number;
                info!(
                    target: "state-sync",
                    current,
                    idle_secs = idle_intervals * PROBE_INTERVAL.as_secs(),
                    "track_recent_consensus: no gossip received, probing peers"
                );
                if let Some(header) = probe_peers_for_latest(current, config, network).await {
                    info!(
                        target: "state-sync",
                        current,
                        peer_latest = header.number,
                        "probe discovered newer consensus header from peer"
                    );
                    return Some(header.digest());
                }
                info!(target: "state-sync", "probe: no peer returned a newer header");
            }
        }
    }
}

/// Walks parent hashes backwards from gossip tips until reaching local state.
///
/// Runs one [`WalkSession`] walk at a time; mid-walk tips are coalesced by the gossip watch and
/// re-trigger the next iteration, which resumes from the session's covered anchor.
pub(crate) async fn spawn_track_recent_consensus<DB: ReDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), network.clone());

    // Probe at startup so the first walk begins without waiting for a gossip tip.
    let current = consensus_bus.last_consensus_header().borrow().number;
    if let Some(header) = probe_peers_for_latest(current, &config, &network).await {
        info!(
            target: "state-sync",
            current,
            peer_latest = header.number,
            "startup probe discovered newer consensus header from peer"
        );
        if session.sync_to(header.digest()).await.is_break() {
            return Ok(());
        }
    }

    loop {
        let Some(hash) = next_walk_target(
            &mut rx_gossip_update,
            &consensus_bus,
            &config,
            &network,
            &rx_shutdown,
        )
        .await
        else {
            return Ok(());
        };

        let current_latest = consensus_bus.last_consensus_header().borrow().number;
        info!(target: "state-sync", current_latest, "starting backwards walk");

        if session.sync_to(hash).await.is_break() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_consensus_primary::network::{
        NetworkCommand, NetworkError, PrimaryRequest, PrimaryResponse,
    };
    use rayls_infrastructure_config::DealtEpochs;
    use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::ConsensusBlocks};
    use rayls_infrastructure_types::{
        dkg::EpochScheme, Certificate, CommittedSubDag, CommitteeLookahead, EpochCertificate,
        EpochRecord, RecordBinding, ReputationScores,
    };
    use rayls_testing_test_utils_committee::CommitteeFixture;
    use std::{
        collections::{BTreeMap, HashMap},
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    /// A network handle whose receiver is dropped: every fetch fails fast, so a walk can make
    /// progress only through the local DB/cache, never a peer.
    fn no_peer_network() -> PrimaryNetworkHandle {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        PrimaryNetworkHandle::new_for_test(tx)
    }

    /// A stale `ConsensusBlockNumbersByDigest` entry (foreign digest mapped to a canonical number)
    /// must not false-stop the backwards walk or republish the wrong header.
    #[tokio::test(start_paused = true)]
    async fn stale_index_entry_does_not_false_stop_the_walk() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Canonical block at 100, plus a STALE index entry pointing a foreign digest at 100.
        let canonical = ConsensusHeader { number: 100, ..Default::default() };
        let canonical_digest = canonical.digest();
        let stale_digest = B256::repeat_byte(0xEE);
        assert_ne!(stale_digest, canonical_digest, "precondition: the stale hash is not canonical");
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&canonical_digest, &canonical.number)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&stale_digest, &canonical.number)?;
            Ok(())
        })
        .unwrap();

        let consensus_bus = ConsensusBus::new();
        let before = consensus_bus.last_consensus_header().borrow().number;

        // Network with a dropped receiver: every fetch fails fast, so the ONLY path that can change
        // `last_consensus_header` is the buggy canonical-hit branch.
        let network = no_peer_network();
        let shutdown = config.shutdown().subscribe();

        let parent =
            get_consensus_header(stale_digest, &config, &consensus_bus, &network, &shutdown).await;

        let after = consensus_bus.last_consensus_header().borrow().number;
        assert_eq!(
            after, before,
            "stale index entry false-stopped the walk and republished the wrong block"
        );
        assert_eq!(parent, Step::Stop, "no reachable peer, so the fetch fails and the walk stops");
    }

    /// A header already cached by an earlier leg (fetched but not yet executed/canonical) must be
    /// traversed from the DB, not refetched from a peer: the walk reads it, republishes the tip,
    /// and returns its parent to keep descending toward the canonical anchor. While execution
    /// lags catch-up every not-yet-executed header lives only in the cache, so refetching each
    /// one on every gossip update re-walks the whole gap over the network.
    #[tokio::test(start_paused = true)]
    async fn cached_header_is_traversed_not_refetched() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Cache-only header at 100 (no canonical `ConsensusBlocks` row): the steady state while
        // execution lags the backwards walk.
        let parent_hash = B256::repeat_byte(0x77);
        let cached = ConsensusHeader { number: 100, parent_hash, ..Default::default() };
        let cached_digest = cached.digest();
        store_consensus_header_in_cache(db, &cached);
        assert!(
            db.get_canonical_consensus_by_hash(cached_digest).is_none(),
            "precondition: a cache-only header is invisible to the canonical anchor check"
        );

        let consensus_bus = ConsensusBus::new();

        // Dropped-receiver network: every fetch fails fast. So returning the parent (and advancing
        // `last_consensus_header` to 100) is only reachable by traversing the cache; a refetch path
        // would fail and yield `None` with the watermark left at 0.
        let network = no_peer_network();
        let shutdown = config.shutdown().subscribe();

        let next =
            get_consensus_header(cached_digest, &config, &consensus_bus, &network, &shutdown).await;

        assert_eq!(
            next,
            Step::Parent(parent_hash),
            "a cached header is traversed from the DB, returning its parent to keep descending"
        );
        assert_eq!(
            consensus_bus.last_consensus_header().borrow().number,
            100,
            "traversing the cached header republishes it without a peer refetch"
        );
    }

    /// A walk stops at the session's covered anchor instead of descending below it. The anchor
    /// heads a chain a prior walk already grounded, so re-descending it is wasted work; with no
    /// reachable peer, only stopping at the anchor lets the walk reach `Covered` rather than
    /// `Incomplete`.
    #[tokio::test(start_paused = true)]
    async fn walk_stops_at_the_covered_anchor() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // The covered anchor (number 105) is NOT itself in the DB; a cached child at 106 links onto
        // it, so the walk reaches the anchor by traversing the child and must stop there.
        let covered_hash = B256::repeat_byte(0x55);
        let child =
            ConsensusHeader { number: 106, parent_hash: covered_hash, ..Default::default() };
        store_consensus_header_in_cache(db, &child);

        // Dropped-receiver network: any fetch below the anchor fails, so a walk that does not stop
        // at the anchor falls through to a failing fetch and yields `Incomplete`.
        let network = no_peer_network();

        let mut session = WalkSession::new(config.clone(), ConsensusBus::new(), network);
        session.covered = Some(covered_hash);

        assert_eq!(
            session.walk(child.digest()).await,
            WalkOutcome::Covered { head: child.digest(), boundary: false },
            "the walk must stop at the covered anchor, not fetch below it"
        );
    }

    /// `derive_covered_anchor` is a pure projection of the persisted cache: on an unchanged cache
    /// it returns the same anchor every call. The sweep is offloaded to a blocking thread, so its
    /// result must stay deterministic and independent of where it runs.
    #[tokio::test]
    async fn derive_covered_anchor_is_stable_across_calls() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Canonical tip at 100, with a contiguous cached run 101 -> 102 hash-linked above it.
        let canonical = ConsensusHeader { number: 100, ..Default::default() };
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            Ok(())
        })
        .unwrap();
        let child_101 =
            ConsensusHeader { number: 101, parent_hash: canonical.digest(), ..Default::default() };
        let child_102 =
            ConsensusHeader { number: 102, parent_hash: child_101.digest(), ..Default::default() };
        store_consensus_header_in_cache(db, &child_101);
        store_consensus_header_in_cache(db, &child_102);

        let first = derive_covered_anchor(db);
        let second = derive_covered_anchor(db);
        assert_eq!(first, Some(child_102.digest()), "anchor must be the top of the contiguous run");
        assert_eq!(first, second, "derive must be stable across calls on an unchanged cache");
    }

    /// The epoch of the pending-key walk tests: it dealt the next epoch's threshold key.
    const DEALING_EPOCH: Epoch = 3;

    /// A node in `DEALING_EPOCH` whose next-epoch key is not computed yet: the epoch dealt, and no
    /// DKG outcome is stored for the next one.
    fn dealing_epoch_node() -> (CommitteeFixture<MemDatabase>, ConsensusConfig<MemDatabase>) {
        dealing_epoch_node_with(EpochScheme::Aggregate)
    }

    /// [`dealing_epoch_node`] with `scheme` as the epoch's own certificate scheme.
    fn dealing_epoch_node_with(
        scheme: EpochScheme,
    ) -> (CommitteeFixture<MemDatabase>, ConsensusConfig<MemDatabase>) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .epoch(DEALING_EPOCH)
            .build();
        let base = fixture.authorities().next().unwrap().consensus_config();
        let config = ConsensusConfig::new_for_epoch(
            base.config().clone(),
            base.node_storage().clone(),
            base.key_config().clone(),
            base.committee().clone(),
            base.lookahead().clone(),
            base.network_config().clone(),
            u64::MAX,
            false,
            scheme,
            DealtEpochs::from_known(BTreeMap::from([(DEALING_EPOCH, true)])),
        )
        .unwrap();
        (fixture, config)
    }

    /// A consensus header at `number` on `parent_hash` whose leader certificate the fixture's
    /// committee signed in its epoch.
    fn signed_header(
        fixture: &CommitteeFixture<MemDatabase>,
        number: u64,
        parent_hash: B256,
    ) -> ConsensusHeader {
        let leader = fixture.certificate(&fixture.header_from_last_authority());
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
    }

    /// A consensus header at `number` led in `epoch`. Its certificate is unsigned, so it verifies
    /// under no scheme.
    fn header_led_in(epoch: Epoch, number: u64) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header.epoch = epoch;
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader {
            parent_hash: B256::repeat_byte(0x99),
            sub_dag,
            number,
            extra: B256::default(),
        }
    }

    /// The record of `DEALING_EPOCH` with `boundary` as its last header. `certified` adds a
    /// certificate; the walk trusts the collector, which saves one only after verifying it.
    fn save_dealing_epoch_record(
        config: &ConsensusConfig<MemDatabase>,
        boundary: B256,
        certified: bool,
    ) {
        let keys = config.committee().bls_keys();
        let record = EpochRecord {
            epoch: DEALING_EPOCH,
            committee: keys.clone(),
            next_committee: keys,
            parent_consensus: boundary,
            ..Default::default()
        };
        let db = config.node_storage();
        if !certified {
            db.save_epoch_record(&record).unwrap();
            return;
        }
        let vote = record.sign_vote(config.key_config());
        let mut signed_authorities = roaring::RoaringBitmap::new();
        signed_authorities.push(0);
        let cert = EpochCertificate {
            epoch_hash: record.digest(),
            signature: vote.signature,
            signed_authorities,
        };
        db.save_epoch_record_with_cert(&record, &cert, &RecordBinding::Legacy).unwrap();
    }

    /// A network handle that answers by-hash header requests from `headers`, like a peer; any
    /// other hash yields an `RPCError`.
    fn serving_by_hash(headers: Vec<ConsensusHeader>) -> PrimaryNetworkHandle {
        serving_by_hash_counted(headers).0
    }

    /// [`serving_by_hash`], with the number of header requests it received.
    fn serving_by_hash_counted(
        headers: Vec<ConsensusHeader>,
    ) -> (PrimaryNetworkHandle, Arc<AtomicUsize>) {
        let headers: HashMap<B256, ConsensusHeader> =
            headers.into_iter().map(|header| (header.digest(), header)).collect();
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                let NetworkCommand::SendRequestAny { request, reply } = cmd else { continue };
                let PrimaryRequest::ConsensusHeader { hash, .. } = request else { continue };
                counter.fetch_add(1, Ordering::SeqCst);
                let response = match hash.and_then(|hash| headers.get(&hash)) {
                    Some(header) => Ok(PrimaryResponse::ConsensusHeader(Arc::new(header.clone()))),
                    None => Err(NetworkError::RPCError("no such header".to_string())),
                };
                let _ = reply.send(response);
            }
        });
        (PrimaryNetworkHandle::new_for_test(tx), requests)
    }

    /// Canonical headers up to `anchor`'s number, then the rest of the epoch to its boundary:
    /// `(anchor, middle, boundary)`, numbered 10, 11 and 12.
    fn dealing_epoch_chain(
        fixture: &CommitteeFixture<MemDatabase>,
        config: &ConsensusConfig<MemDatabase>,
    ) -> (ConsensusHeader, ConsensusHeader, ConsensusHeader) {
        let anchor = signed_header(fixture, 10, B256::default());
        config
            .node_storage()
            .with_write_txn(|txn| {
                txn.insert::<ConsensusBlocks>(&anchor.number, &anchor)?;
                txn.insert::<ConsensusBlockNumbersByDigest>(&anchor.digest(), &anchor.number)?;
                Ok(())
            })
            .unwrap();
        let middle = signed_header(fixture, 11, anchor.digest());
        let boundary = signed_header(fixture, 12, middle.digest());
        (anchor, middle, boundary)
    }

    /// A node behind a threshold boundary must not stall. The tip leads in the next epoch, whose
    /// key comes from this epoch's headers, so the walk descends from this epoch's certified
    /// boundary: it verifies and caches the rest of this epoch, and republishes the boundary as
    /// the streamer's target.
    #[tokio::test(start_paused = true)]
    async fn walk_descends_from_the_certified_boundary_when_the_tip_epoch_is_pending() {
        let (fixture, config) = dealing_epoch_node();
        let db = config.node_storage();

        // Canonical up to 10; the rest of the epoch, 11 and the boundary 12, is only on peers.
        let (_, middle, boundary) = dealing_epoch_chain(&fixture, &config);
        save_dealing_epoch_record(&config, boundary.digest(), true);
        let tip = header_led_in(DEALING_EPOCH + 1, 20);
        assert_eq!(
            config.epoch_keys_status(DEALING_EPOCH + 1),
            EpochKeysStatus::Pending,
            "precondition: the tip's epoch key is pending"
        );

        let consensus_bus = ConsensusBus::new();
        let (network, requests) =
            serving_by_hash_counted(vec![tip.clone(), boundary.clone(), middle.clone()]);
        let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), network);
        assert!(session.sync_to(tip.digest()).await.is_continue());
        assert!(session.boundary_covered);

        assert_eq!(
            consensus_bus.last_consensus_header().borrow().number,
            boundary.number,
            "the boundary is the streamer's target"
        );
        assert_eq!(
            session.covered,
            Some(boundary.digest()),
            "coverage starts at the verified boundary, not the unverified tip"
        );
        assert!(db.get_consensus_by_hash(boundary.digest()).is_some());
        assert!(db.get_consensus_by_hash(middle.digest()).is_some());
        assert!(db.get_consensus_by_hash(tip.digest()).is_none(), "the tip is never cached");

        // Nothing above the covered boundary verifies before the transition: later tips are not
        // even fetched.
        let fetched = requests.load(Ordering::SeqCst);
        let later = header_led_in(DEALING_EPOCH + 1, 21);
        assert!(session.sync_to(later.digest()).await.is_continue());
        assert_eq!(requests.load(Ordering::SeqCst), fetched, "a later tip is not walked");
        assert_eq!(session.covered, Some(boundary.digest()));
    }

    /// A node restarted while executing up to its epoch's boundary finds the rest of the epoch in
    /// its cache, and derives its coverage right up to the boundary. The walk must still publish
    /// the boundary, or the new streamer never gets a target and the node never transitions.
    #[tokio::test(start_paused = true)]
    async fn a_new_session_republishes_a_boundary_its_cache_already_reaches() {
        let (fixture, config) = dealing_epoch_node();
        let (_, middle, boundary) = dealing_epoch_chain(&fixture, &config);
        store_consensus_header_in_cache(config.node_storage(), &middle);
        store_consensus_header_in_cache(config.node_storage(), &boundary);
        save_dealing_epoch_record(&config, boundary.digest(), true);
        let tip = header_led_in(DEALING_EPOCH + 1, 20);

        // A fresh bus and session, as after a restart.
        let consensus_bus = ConsensusBus::new();
        let network = serving_by_hash(vec![tip.clone()]);
        let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), network);
        assert!(session.sync_to(tip.digest()).await.is_continue());

        assert_eq!(session.covered, Some(boundary.digest()), "coverage derived from the cache");
        assert_eq!(
            consensus_bus.last_consensus_header().borrow().number,
            boundary.number,
            "the boundary is the streamer's target"
        );
    }

    /// While this epoch's own key is pending nothing of it verifies, so tips are not walked.
    #[tokio::test(start_paused = true)]
    async fn a_pending_epoch_key_skips_walks() {
        let (_, config) = dealing_epoch_node_with(EpochScheme::Pending);
        let tip = header_led_in(DEALING_EPOCH, 20);
        let (network, requests) = serving_by_hash_counted(vec![tip.clone()]);
        let mut session = WalkSession::new(config, ConsensusBus::new(), network);
        assert!(session.sync_to(tip.digest()).await.is_continue());
        assert_eq!(requests.load(Ordering::SeqCst), 0);
    }

    /// A node still in epoch 0 holds only the uncertified epoch-0 placeholder. Asking the
    /// collector for epoch 0 would change nothing (the request watch starts at 0), so the walk asks
    /// for the last epoch closed before the tip's; the collector's pass then refetches epoch 0.
    #[tokio::test(start_paused = true)]
    async fn a_node_in_epoch_0_asks_for_records_up_to_the_tips_epoch() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let base = fixture.authorities().next().unwrap().consensus_config();
        let keys = base.committee().bls_keys();
        // What the dealing gate gives a node in epoch 0 of a network whose fork is active from
        // genesis: epoch 0 deals, and later epochs are unknown.
        let config = ConsensusConfig::new_for_epoch(
            base.config().clone(),
            base.node_storage().clone(),
            base.key_config().clone(),
            base.committee().clone(),
            CommitteeLookahead::from_entries(vec![(1, keys.clone()), (2, keys)]),
            base.network_config().clone(),
            u64::MAX,
            false,
            EpochScheme::Aggregate,
            DealtEpochs::from_known(BTreeMap::from([(0, true)])),
        )
        .unwrap();
        config
            .node_storage()
            .save_epoch_record(&EpochRecord { epoch: 0, ..Default::default() })
            .unwrap();
        assert_eq!(config.epoch_keys_status(2), EpochKeysStatus::Pending, "precondition");
        let tip = header_led_in(2, 20);

        let consensus_bus = ConsensusBus::new();
        let mut session =
            WalkSession::new(config, consensus_bus.clone(), serving_by_hash(vec![tip.clone()]));
        assert_eq!(session.walk(tip.digest()).await, WalkOutcome::AwaitingBoundary);
        assert_eq!(*consensus_bus.requested_missing_epoch().borrow(), 1);
    }

    /// Without this epoch's certified record the walk cannot know the boundary: it asks the
    /// collector for the record and waits, publishing nothing.
    #[tokio::test(start_paused = true)]
    async fn walk_requests_this_epochs_record_when_the_boundary_is_not_certified() {
        let (fixture, config) = dealing_epoch_node();
        let boundary = signed_header(&fixture, 12, B256::default());
        // A local record without a certificate proves nothing.
        save_dealing_epoch_record(&config, boundary.digest(), false);
        let tip = header_led_in(DEALING_EPOCH + 1, 20);

        let consensus_bus = ConsensusBus::new();
        let network = serving_by_hash(vec![tip.clone(), boundary]);
        let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), network);
        assert_eq!(session.walk(tip.digest()).await, WalkOutcome::AwaitingBoundary);

        assert_eq!(*consensus_bus.requested_missing_epoch().borrow(), DEALING_EPOCH);
        assert_eq!(consensus_bus.last_consensus_header().borrow().number, 0, "nothing published");
    }

    /// After an epoch transition resets `last_consensus_header` to the default, a probe can find
    /// the network tip that an earlier leg already cached. The walk then starts at the covered
    /// anchor and descends nothing, but it must still publish that tip: the forward streamer and
    /// the rejoin check only learn the target through this watch, and with a stalled network no
    /// newer header ever arrives to publish it instead.
    #[tokio::test(start_paused = true)]
    async fn walk_starting_at_the_covered_anchor_publishes_it() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config();
        let db = config.node_storage();

        // Executed up to 100 (the previous epoch's last output); 101 and 102 were fetched into the
        // cache before the transition and not executed yet.
        let canonical = ConsensusHeader { number: 100, ..Default::default() };
        db.with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            Ok(())
        })
        .unwrap();
        let child_101 =
            ConsensusHeader { number: 101, parent_hash: canonical.digest(), ..Default::default() };
        let child_102 =
            ConsensusHeader { number: 102, parent_hash: child_101.digest(), ..Default::default() };
        store_consensus_header_in_cache(db, &child_101);
        store_consensus_header_in_cache(db, &child_102);

        // A fresh bus: the watch holds the default header, as after the transition's reset.
        let consensus_bus = ConsensusBus::new();
        assert_eq!(consensus_bus.last_consensus_header().borrow().number, 0);

        // The probe found 102, the top of the cached run (the session derives it as its anchor).
        let mut session = WalkSession::new(config.clone(), consensus_bus.clone(), no_peer_network());
        assert!(session.sync_to(child_102.digest()).await.is_continue());

        assert_eq!(
            consensus_bus.last_consensus_header().borrow().number,
            102,
            "the probed tip is the covered anchor, so the walk descends nothing, but it must still \
             publish the tip"
        );
    }
}
