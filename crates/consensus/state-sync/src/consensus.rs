//! Tasks and helpers for collecting consensus headers trustlessly.
use futures::{stream::FuturesUnordered, StreamExt};
use rayls_consensus_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache, EpochRecords},
    ConsensusStore,
};
use rayls_infrastructure_types::{
    BlsPublicKey, ConsensusHeader, ConsensusHeaderChainMeta, Database as ReDatabase, DbTxMut as _,
    Epoch, Noticer, B256,
};
use std::{ops::ControlFlow, sync::LazyLock, time::Duration};
use tokio::sync::watch;
use tracing::{error, info, warn};

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

/// Trigger the epoch record collector and poll until committee keys arrive.
///
/// Return `None` on shutdown or timeout.
pub(crate) async fn wait_for_committee_keys<DB: ReDatabase>(
    epoch: Epoch,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    shutdown: &Noticer,
) -> Option<Vec<BlsPublicKey>> {
    if let Some(keys) = config.get_committee_keys_for_epoch(epoch) {
        return Some(keys);
    }

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
        if let Some(keys) = config.get_committee_keys_for_epoch(epoch) {
            info!(target: "state-sync", epoch, "backwards walk: committee keys arrived");
            return Some(keys);
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

/// Descend one step of the backwards walk: stop at the canonical anchor, traverse a cached header,
/// or fetch and BLS-verify from a peer.
///
/// Returns `None` at the canonical anchor (the walk is grounded) or on failure; otherwise returns
/// the parent hash to continue descending.
async fn get_consensus_header<DB: ReDatabase>(
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
    shutdown: &Noticer,
) -> Option<B256> {
    let db = config.node_storage();
    // Stop only at the canonical anchor (never an unverified cache row); its per-row digest==hash
    // check means a stale, never-pruned ByDigest index entry cannot false-stop the walk.
    if let Some(block) = db.get_canonical_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &block);
        return None;
    }
    // Traverse a cached header from the DB instead of refetching it, and keep descending.
    if let Some(cached) = db.get_consensus_by_hash(hash) {
        republish_if_higher(consensus_bus, &cached);
        return Some(cached.parent_hash);
    }
    for attempt in 0u32..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
        }
        if shutdown.noticed() {
            return None;
        }
        match network.request_consensus(None, Some(hash)).await {
            Ok(header) => {
                let leader_epoch = header.sub_dag.leader_epoch();
                let Some(keys) =
                    wait_for_committee_keys(leader_epoch, config, consensus_bus, shutdown).await
                else {
                    warn!(
                        target: "state-sync",
                        ?hash,
                        epoch = leader_epoch,
                        "committee keys unavailable, stopping walk"
                    );
                    return None;
                };
                let Ok(verified) = header.verify_header_with_keys(&keys).inspect_err(|e| {
                    warn!(target: "state-sync", ?hash, "BLS verification failed, discarding header: {e}");
                }) else {
                    return None;
                };
                let parent = verified.parent_hash;
                store_consensus_header_in_cache(db, &verified);
                republish_if_higher(consensus_bus, &verified);
                return Some(parent);
            }
            Err(e) => {
                warn!(target: "state-sync", ?hash, ?e, attempt, "failed to fetch consensus header from peers");
            }
        }
    }
    None
}

/// Digest of the genesis consensus header (number 0, the default header).
///
/// A genuine number-1 header carries this as its parent hash, so a walk that descends to it has
/// reached the chain's base and is grounded.
static GENESIS_DIGEST: LazyLock<B256> = LazyLock::new(|| ConsensusHeader::default().digest());

/// Terminal state of a backwards walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkOutcome {
    /// Linked the start anchor to local coverage (canonical, genesis, or the session anchor).
    Covered,
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
}

impl<DB: ReDatabase> WalkSession<DB> {
    /// Create a session for one tracker task lifetime.
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        network: PrimaryNetworkHandle,
    ) -> Self {
        let shutdown = config.shutdown().subscribe();
        Self { config, consensus_bus, network, shutdown, covered: None }
    }

    /// Walk backwards from `start_hash` to local coverage, recording the result for the next call.
    /// Returns `Break` when shutdown was noticed and the tracker must exit.
    pub(crate) async fn sync_to(&mut self, start_hash: B256) -> ControlFlow<()> {
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
            // `start_hash` now heads a verified chain to canonical; stop here next time.
            WalkOutcome::Covered => {
                self.covered = Some(start_hash);
                ControlFlow::Continue(())
            }
            // Drop the anchor so the next walk re-derives and re-descends to self-heal the gap.
            WalkOutcome::Incomplete => {
                self.covered = None;
                ControlFlow::Continue(())
            }
            WalkOutcome::Shutdown => ControlFlow::Break(()),
        }
    }

    /// Descend parent hashes from `start_hash` until reaching local coverage.
    async fn walk(&self, start_hash: B256) -> WalkOutcome {
        let db = self.config.node_storage();
        let covered_hash = self.covered;
        let mut current = start_hash;
        let mut depth = 0u64;
        let outcome = loop {
            if self.shutdown.noticed() {
                return WalkOutcome::Shutdown;
            }
            // Stop at proven coverage without re-descending it: the session anchor or genesis.
            if covered_hash == Some(current) || current == *GENESIS_DIGEST {
                break WalkOutcome::Covered;
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
                Some(parent) => current = parent,
                // `get_consensus_header` stopped at the canonical anchor (Covered) or could not
                // fetch (Incomplete: coverage below `current` is unknown).
                None => {
                    break if db.get_canonical_consensus_by_hash(current).is_some() {
                        WalkOutcome::Covered
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
            info!(target: "state-sync", depth, ?outcome, "backwards walk: completed");
        }
        outcome
    }
}

/// Whether this node finds the network head from peers' certified tips: once the `CertifiedTipV1`
/// fork is active at the next block to execute, the point every other fork check uses.
/// `activation` is the fork's block, `None` while it is `Never`. Not before: a peer on an older
/// release cannot decode the request.
fn uses_certified_tip(activation: Option<u64>, executed_tip: u64) -> bool {
    activation.is_some_and(|block| executed_tip.saturating_add(1) >= block)
}

/// Find a network head above `current` to walk to, as its number and hash. Before the
/// `CertifiedTipV1` fork, the single highest latest header any committee member reports; after,
/// a certified tip, which one member alone cannot forge (#370, #312).
async fn probe_for_head<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<(u64, B256)> {
    let executed_tip =
        consensus_bus.recently_executed_blocks().borrow().latest_block_num_hash().number;
    if uses_certified_tip(config.certified_tip_v1_block(), executed_tip) {
        probe_peers_for_certified_tip(current, config, consensus_bus, network).await
    } else {
        probe_peers_for_latest(current, config, network).await.map(|h| (h.number, h.digest()))
    }
}

/// Ask every connected peer, observers included, for its certified tip and return the highest
/// one above `current` that verifies against its epoch's committee. One valid answer is enough;
/// asking every peer only guards against peers that are down, on an older release, or behind.
/// A tip from before this node's previous epoch is dropped: f+1 keys of a retired committee
/// could sign any number, and the previous epoch counts so a chain halted on its last header can
/// still be found.
/// A tip from an epoch whose committee is not known yet is skipped, and the epoch record
/// collector is asked for the record after the newest one held: it walks upward as far as peers
/// serve, so a later probe can verify the tip. Neither waiting here nor requesting the tip's own
/// epoch, which is unverified: a peer naming a far epoch would hold up the probe or send the
/// collector chasing it.
async fn probe_peers_for_certified_tip<DB: ReDatabase>(
    current: u64,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<(u64, B256)> {
    let peers = network.inner_handle().connected_peers().await.ok()?;
    let mut inflight: FuturesUnordered<_> =
        peers.into_iter().map(|peer| network.request_certified_tip_from_peer(peer)).collect();
    let floor = config.epoch().saturating_sub(1);
    let mut candidates = Vec::new();
    while let Some(result) = inflight.next().await {
        if let Ok(Some(tip)) = result {
            if tip.number > current && tip.epoch >= floor {
                candidates.push(tip);
            }
        }
    }
    // epoch first: a high number signed by f+1 retired keys of an earlier epoch must not rank
    // above an honest tip from a later one
    candidates.sort_by(|a, b| (b.epoch, b.number).cmp(&(a.epoch, a.number)));
    // whole-struct: a copy with the honest number and hash but a bad signature must not stand
    // in for the honest ones
    candidates.dedup();
    for tip in candidates {
        let Some(committee) = config.get_committee_keys_for_epoch(tip.epoch) else {
            let next = config
                .node_storage()
                .last_record::<EpochRecords>()
                .map_or(0, |(epoch, _)| epoch)
                .saturating_add(1);
            consensus_bus.requested_missing_epoch().send_if_modified(|requested| {
                let higher = next > *requested;
                if higher {
                    *requested = next;
                }
                higher
            });
            continue;
        };
        if tip.verify(&committee) {
            return Some((tip.number, tip.hash));
        }
        warn!(
            target: "state-sync",
            number = tip.number,
            epoch = tip.epoch,
            "probe: certified tip did not verify against its committee, skipping"
        );
    }
    None
}

/// Probe all committee peers concurrently, return the highest header above `current`.
/// BLS is re-verified downstream in `get_consensus_header`.
///
/// Never call this once `CertifiedTipV1` is active (see [`uses_certified_tip`]): the answer of one
/// member is taken on its own, and a header carries no signature of its own, so one member can
/// serve a forged copy of a real header and fork or strand the node (#370, #312). It stays only
/// for nodes before the fork, which still talk to peers that cannot serve a certified tip.
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
                if let Some((number, hash)) =
                    probe_for_head(current, config, consensus_bus, network).await
                {
                    info!(
                        target: "state-sync",
                        current,
                        peer_latest = number,
                        "probe discovered newer consensus header from peer"
                    );
                    return Some(hash);
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
    if let Some((number, hash)) = probe_for_head(current, &config, &consensus_bus, &network).await {
        info!(
            target: "state-sync",
            current,
            peer_latest = number,
            "startup probe discovered newer consensus header from peer"
        );
        if session.sync_to(hash).await.is_break() {
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
    use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::ConsensusBlocks};
    use rayls_infrastructure_types::{
        encode, to_intent_message, BlsAggregateSignature, BlsSignature, Certificate, CertifiedTip,
        CommittedSubDag, ReputationScores, Signer as _,
    };
    use rayls_testing_test_utils_committee::CommitteeFixture;
    use std::{
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
        assert!(parent.is_none(), "no reachable peer, so the fetch fails and the walk yields None");
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
            Some(parent_hash),
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
            WalkOutcome::Covered,
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

    /// The switch is made against the next block to execute, like every other fork check.
    #[test]
    fn certified_tip_is_used_once_the_fork_is_active_at_the_next_block() {
        assert!(!uses_certified_tip(None, 100), "Never: probe latest headers");
        assert!(!uses_certified_tip(Some(102), 100), "next block 101 is before activation");
        assert!(uses_certified_tip(Some(101), 100), "next block 101 is the activation block");
        assert!(uses_certified_tip(Some(0), 0), "genesis-active");
        assert!(uses_certified_tip(Some(u64::MAX), u64::MAX), "no overflow at the top");
    }

    fn committee_of_four() -> CommitteeFixture<MemDatabase> {
        CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build()
    }

    /// A genuine tip one block above genesis, its leader certified by the whole fixture committee.
    fn honest_tip(fixture: &CommitteeFixture<MemDatabase>) -> ConsensusHeader {
        let leader = fixture.certificate(&fixture.header_from_last_authority());
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { parent_hash: *GENESIS_DIGEST, sub_dag, number: 1, extra: B256::ZERO }
    }

    /// What one adversarial validator serves as its latest header: the highest number there is,
    /// and an unsigned leader claiming an epoch no committee exists for (#370).
    fn forged_tip() -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header.epoch = Epoch::MAX;
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader {
            parent_hash: B256::repeat_byte(0xAB),
            sub_dag,
            number: u64::MAX,
            extra: B256::ZERO,
        }
    }

    /// A forged copy of `honest`: the same block number and the same committee-signed leader and
    /// certificates, with only the commit timestamp raised. The commit timestamp is hashed into the
    /// header digest but is not covered by any committee signature, so the copy still verifies yet
    /// has a different digest and would execute to a different block (#312).
    fn forged_copy(honest: &ConsensusHeader) -> ConsensusHeader {
        let mut higher_leader = Certificate::default();
        higher_leader.header.created_at = honest.sub_dag.commit_timestamp() + 1_000_000;
        let higher =
            CommittedSubDag::new(vec![], higher_leader, 0, ReputationScores::default(), None);
        let tampered_sub_dag = CommittedSubDag::new(
            honest.sub_dag.certificates.clone(),
            honest.sub_dag.leader.clone(),
            0,
            honest.sub_dag.reputation_score.clone(),
            Some(&higher),
        );
        ConsensusHeader {
            parent_hash: honest.parent_hash,
            sub_dag: tampered_sub_dag,
            number: honest.number,
            extra: honest.extra,
        }
    }

    /// `header` as `signers` of the fixture committee sign it when they commit it.
    fn signatures(
        fixture: &CommitteeFixture<MemDatabase>,
        signers: usize,
        header: &ConsensusHeader,
    ) -> Vec<(BlsPublicKey, BlsSignature)> {
        let (epoch, round) = (header.sub_dag.leader_epoch(), header.sub_dag.leader_round());
        let digest = CertifiedTip::digest_data(epoch, round, header.number, header.digest());
        fixture
            .authorities()
            .take(signers)
            .map(|a| {
                (
                    *a.authority().protocol_key(),
                    a.keypair().sign(&encode(&to_intent_message(digest))),
                )
            })
            .collect()
    }

    /// The certified tip f+1 of the fixture committee hold for `header`.
    fn certified(
        fixture: &CommitteeFixture<MemDatabase>,
        header: &ConsensusHeader,
    ) -> CertifiedTip {
        let keys: Vec<_> = fixture.authorities().map(|a| *a.authority().protocol_key()).collect();
        CertifiedTip::from_signatures(
            header.sub_dag.leader_epoch(),
            header.sub_dag.leader_round(),
            header.number,
            header.digest(),
            &keys,
            signatures(fixture, CertifiedTip::quorum(keys.len()), header),
        )
        .expect("f+1 signers")
    }

    /// One connected peer: what it answers when asked for its certified tip (`Err` for a peer on
    /// a release without the request) and for its latest header, and whether its certified tip
    /// arrives a second late.
    struct Peer {
        key: BlsPublicKey,
        certified_tip: Result<Option<CertifiedTip>, ()>,
        latest: ConsensusHeader,
        slow: bool,
    }

    impl Peer {
        fn new(key: BlsPublicKey, tip: Option<CertifiedTip>, latest: &ConsensusHeader) -> Self {
            Self { key, certified_tip: Ok(tip), latest: latest.clone(), slow: false }
        }

        /// A peer on a release without the certified tip request.
        fn old(key: BlsPublicKey, latest: &ConsensusHeader) -> Self {
            Self { key, certified_tip: Err(()), latest: latest.clone(), slow: false }
        }
    }

    /// Requests the follower sent, by kind.
    #[derive(Default)]
    struct Asked {
        latest: AtomicUsize,
        certified_tip: AtomicUsize,
    }

    /// Connected `peers`, serving every header in `headers` by hash.
    fn peer_network(
        peers: Vec<Peer>,
        headers: Vec<ConsensusHeader>,
    ) -> (PrimaryNetworkHandle, Arc<Asked>) {
        let asked = Arc::new(Asked::default());
        let counts = asked.clone();
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<NetworkCommand<PrimaryRequest, PrimaryResponse>>(64);
        tokio::spawn(async move {
            let unknown = || NetworkError::RPCError("unknown".to_string());
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    NetworkCommand::ConnectedPeers { reply } => {
                        let _ = reply.send(peers.iter().map(|p| p.key).collect());
                    }
                    NetworkCommand::SendRequest { peer, request, reply } => {
                        let Some(peer) = peers.iter().find(|p| p.key == peer) else { continue };
                        let response = match request {
                            PrimaryRequest::CertifiedTip => {
                                counts.certified_tip.fetch_add(1, Ordering::SeqCst);
                                let response = peer
                                    .certified_tip
                                    .clone()
                                    .map(PrimaryResponse::CertifiedTip)
                                    .map_err(|()| unknown());
                                if peer.slow {
                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                        let _ = reply.send(response);
                                    });
                                    continue;
                                }
                                response
                            }
                            PrimaryRequest::ConsensusHeader { number: None, hash: None } => {
                                counts.latest.fetch_add(1, Ordering::SeqCst);
                                Ok(PrimaryResponse::ConsensusHeader(Arc::new(peer.latest.clone())))
                            }
                            _ => continue,
                        };
                        let _ = reply.send(response);
                    }
                    NetworkCommand::SendRequestAny {
                        request: PrimaryRequest::ConsensusHeader { hash: Some(hash), .. },
                        reply,
                    } => {
                        let found = headers.iter().find(|h| h.digest() == hash).cloned();
                        let _ = reply.send(
                            found
                                .map(|h| PrimaryResponse::ConsensusHeader(Arc::new(h)))
                                .ok_or_else(unknown),
                        );
                    }
                    _ => {}
                }
            }
        });
        (PrimaryNetworkHandle::new_for_test(tx), asked)
    }

    /// Wait until the follower's latest header reaches `number`, or `limit` passes.
    async fn reaches(consensus_bus: &ConsensusBus, number: u64, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while tokio::time::Instant::now() < deadline {
            if consensus_bus.last_consensus_header().borrow().number >= number {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// After the fork one valid certified tip from any connected peer is enough, here an
    /// observer outside the committee, while the other peers are on an older release or hold
    /// none. No peer is asked for its latest header.
    #[tokio::test(start_paused = true)]
    async fn after_the_fork_one_peer_certified_tip_is_enough() {
        let fixture = committee_of_four();
        let config = fixture.authorities().next().unwrap().consensus_config();
        let config = config.with_certified_tip_v1_block(Some(0));
        let honest = honest_tip(&fixture);
        let members: Vec<_> =
            config.committee().others_primaries_by_id(config.authority_id().as_ref());
        // a key outside the committee: another fixture's keys are freshly generated
        let observer =
            *committee_of_four().authorities().next().unwrap().authority().protocol_key();
        let peers = vec![
            Peer::old(members[0].1, &honest),
            Peer::old(members[1].1, &honest),
            Peer::new(members[2].1, None, &honest),
            Peer::new(observer, Some(certified(&fixture, &honest)), &honest),
        ];
        let (network, asked) = peer_network(peers, vec![honest.clone()]);
        let consensus_bus = ConsensusBus::new();
        tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

        assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
        let published = consensus_bus.last_consensus_header().borrow().digest();
        config.shutdown().notify();
        assert_eq!(published, honest.digest());
        assert_eq!(asked.latest.load(Ordering::SeqCst), 0, "no peer is asked for its latest");
        assert_eq!(asked.certified_tip.load(Ordering::SeqCst), 4, "every peer is asked");
    }

    /// #312 after the fork: one member serves a forged copy of the real tip, as its latest header
    /// and as a certified tip it signed alone, its bitmap claiming f+1 signers. The follower walks
    /// to the honest tip and never stores or publishes the copy.
    #[tokio::test(start_paused = true)]
    async fn after_the_fork_a_forged_copy_cannot_fork_the_follower() {
        let fixture = committee_of_four();
        let config = fixture.authorities().next().unwrap().consensus_config();
        let config = config.with_certified_tip_v1_block(Some(0));
        let honest = honest_tip(&fixture);
        let forged = forged_copy(&honest);
        let keys = config.get_committee_keys_for_epoch(0).expect("current committee");
        assert!(forged.clone().verify_header_with_keys(&keys).is_ok(), "the copy still verifies");
        assert_ne!(forged.digest(), honest.digest(), "the bumped timestamp changes the digest");

        // the adversary's own signature, under a bitmap naming f+1 signers
        let mut alone = certified(&fixture, &forged);
        let (_, own) = signatures(&fixture, 1, &forged)[0];
        alone.signature = BlsAggregateSignature::aggregate(&[own], true).unwrap().to_signature();
        assert!(!alone.verify(&keys), "precondition: one signer is not f+1");

        let members: Vec<_> =
            config.committee().others_primaries_by_id(config.authority_id().as_ref());
        let mut peers: Vec<_> = members
            .iter()
            .map(|(_, key)| Peer::new(*key, Some(certified(&fixture, &honest)), &honest))
            .collect();
        peers[0] = Peer::new(members[0].1, Some(alone), &forged);
        let (network, asked) = peer_network(peers, vec![honest.clone(), forged.clone()]);
        let consensus_bus = ConsensusBus::new();
        tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

        assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
        let published = consensus_bus.last_consensus_header().borrow().clone();
        let stored_forged = config.node_storage().get_consensus_by_hash(forged.digest()).is_some();
        config.shutdown().notify();
        assert_eq!(published.digest(), honest.digest(), "the honest tip is the published header");
        assert!(!stored_forged, "the forged copy was stored in the consensus cache");
        assert_eq!(asked.latest.load(Ordering::SeqCst), 0, "no peer is asked for its latest");
    }

    /// #370 after the fork: one member answers with a tip at the highest number there is, from an
    /// epoch no committee exists for. It cannot strand the follower, which walks to the honest
    /// tip, nor send the epoch record collector chasing that epoch.
    #[tokio::test(start_paused = true)]
    async fn after_the_fork_a_forged_epoch_cannot_strand_the_follower() {
        let fixture = committee_of_four();
        let config = fixture.authorities().next().unwrap().consensus_config();
        let config = config.with_certified_tip_v1_block(Some(0));
        let honest = honest_tip(&fixture);
        assert!(config.get_committee_keys_for_epoch(Epoch::MAX).is_none(), "precondition");
        let far =
            CertifiedTip { epoch: Epoch::MAX, number: u64::MAX, ..certified(&fixture, &honest) };

        let members: Vec<_> =
            config.committee().others_primaries_by_id(config.authority_id().as_ref());
        let mut peers: Vec<_> = members
            .iter()
            .map(|(_, key)| Peer::new(*key, Some(certified(&fixture, &honest)), &honest))
            .collect();
        peers[0] = Peer::new(members[0].1, Some(far), &forged_tip());
        let (network, _) = peer_network(peers, vec![honest.clone()]);
        let consensus_bus = ConsensusBus::new();
        tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

        assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
        let requested = *consensus_bus.requested_missing_epoch().borrow();
        config.shutdown().notify();
        assert!(requested <= 1, "the collector was sent to epoch {requested}");
    }

    /// A copy of the honest top tip with a bad signature, answered before the honest ones, does
    /// not stand in for them.
    #[tokio::test(start_paused = true)]
    async fn a_bad_copy_of_the_top_tip_does_not_hide_the_honest_ones() {
        let fixture = committee_of_four();
        let config = fixture.authorities().next().unwrap().consensus_config();
        let config = config.with_certified_tip_v1_block(Some(0));
        let honest = honest_tip(&fixture);
        let valid = certified(&fixture, &honest);
        let bad =
            CertifiedTip { signature: signatures(&fixture, 1, &honest)[0].1, ..valid.clone() };
        let keys = config.get_committee_keys_for_epoch(0).expect("current committee");
        assert!(!bad.verify(&keys), "precondition: the copy does not verify");

        // the honest members answer late, the bad copy from an observer first
        let observer =
            *committee_of_four().authorities().next().unwrap().authority().protocol_key();
        let mut peers: Vec<_> = config
            .committee()
            .others_primaries_by_id(config.authority_id().as_ref())
            .into_iter()
            .map(|(_, key)| Peer { slow: true, ..Peer::new(key, Some(valid.clone()), &honest) })
            .collect();
        peers.push(Peer::new(observer, Some(bad), &honest));
        let (network, _) = peer_network(peers, vec![honest.clone()]);
        let consensus_bus = ConsensusBus::new();
        tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

        assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
        config.shutdown().notify();
    }

    /// Before the fork the follower probes committee members' latest headers as before, and asks
    /// nobody for a certified tip.
    #[tokio::test(start_paused = true)]
    async fn before_the_fork_the_follower_probes_latest_headers() {
        let fixture = committee_of_four();
        let config = fixture.authorities().next().unwrap().consensus_config();
        assert_eq!(config.certified_tip_v1_block(), None, "precondition: the fork is Never");
        let honest = honest_tip(&fixture);
        let peers = config
            .committee()
            .others_primaries_by_id(config.authority_id().as_ref())
            .into_iter()
            .map(|(_, key)| Peer::new(key, Some(certified(&fixture, &honest)), &honest))
            .collect();
        let (network, asked) = peer_network(peers, vec![honest.clone()]);
        let consensus_bus = ConsensusBus::new();
        tokio::spawn(spawn_track_recent_consensus(config.clone(), consensus_bus.clone(), network));

        assert!(reaches(&consensus_bus, 1, Duration::from_secs(5)).await, "walks to the tip");
        config.shutdown().notify();
        assert_eq!(asked.latest.load(Ordering::SeqCst), 3, "every member is probed");
        assert_eq!(asked.certified_tip.load(Ordering::SeqCst), 0, "no certified tip is asked for");
    }
}
